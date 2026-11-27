package qoder

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	protocolqoder "github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// payload_converted.go executes Qoder attempts through protocol/convert and
// codec/qoderchat while preserving Qoder's desktop payload envelope,
// conversation fingerprinting, model capability gating, and local request error
// classification. Client requests from Anthropic Messages, OpenAI Responses, and
// OpenAI Chat Completions (including input-shaped Cursor Chat bodies) decode
// through a single convert.Exchange into protocol/qoder's normalized
// PayloadRequest before withEffectiveControls and conversation planning run.
//
// Translating protocol/qoder RequestError and APIError values into upstream
// qoder error types keeps local HTTP 400 capability rejections free of provider
// failover, health penalties, and Ops upstream error attribution while allowing
// retryable upstream failures to switch accounts before semantic output begins.
// Tool-name reverse mapping binds after PrepareConversation resolves the
// outbound tool list so response streaming and buffered folding restore Qoder
// upstream aliases before convert raises lowered custom, namespaced, and search
// tool identities.

// newQoderExchangeRuntime wraps convert.DefaultRuntime so OpenAI Responses
// conversions emit the exact pre-allocated resp_ identifier recorded on
// AttemptResult.RequestID. Empty responseID inputs return DefaultRuntime
// directly, and subsequent or non-"resp_" prefixes delegate to the base source.
func newQoderExchangeRuntime(responseID string) convert.Runtime {
	base := convert.DefaultRuntime()
	if responseID == "" {
		return base
	}
	var usedRespID atomic.Bool
	return convert.Runtime{
		Now: base.Now,
		NewID: func(prefix string) string {
			if prefix == "resp_" && usedRespID.CompareAndSwap(false, true) {
				return responseID
			}
			return base.NewID(prefix)
		},
	}
}

// PayloadRequestFromIR converts a decoded protocol/qoder.PayloadRequest into a
// QoderPayloadRequest ready for withEffectiveControls, conversation
// fingerprinting, and desktop payload serialization. Nil inputs, invalid
// allowed_tools restrictions, or blank model names return a *RequestError.
//
// System instructions normalize volatile x-anthropic-billing-header cch
// checksums to "cch=<cch>;" so consecutive Claude Code turns retain stable
// conversation fingerprints. User turns preserve discrete text blocks and
// cache_control breakpoints, assistant and tool turns keep Qoder's scalar
// content convention unless tool blocks carry explicit cache markers, and tool
// JSON schemas stay as raw JSON bytes without float64 re-decoding.
func PayloadRequestFromIR(protoReq *protocolqoder.PayloadRequest) (QoderPayloadRequest, error) {
	if protoReq == nil {
		return QoderPayloadRequest{}, &RequestError{
			Code:    protocolqoder.CodeInvalidRequest,
			Message: "qoder payload request is nil",
		}
	}
	restricted, err := protoReq.RestrictAllowedTools()
	if err != nil {
		return QoderPayloadRequest{}, translateClientRequestError(err)
	}
	if strings.TrimSpace(restricted.Model) == "" {
		return QoderPayloadRequest{}, &RequestError{
			Code:    protocolqoder.CodeInvalidRequest,
			Message: "model is required",
		}
	}

	messages := qoderMessagesFromIRPayload(restricted.Messages)
	messages = EnrichQoderToolResultMessages(messages)

	maxTokens := QoderDefaultMaxTokens
	if restricted.MaxTokens > 0 {
		maxTokens = restricted.MaxTokens
	}

	return QoderPayloadRequest{
		Model:      restricted.Model,
		System:     restricted.NormalizedSystem(),
		Messages:   messages,
		Tools:      qoderToolsFromIRPayload(restricted.Tools),
		ToolChoice: restricted.ToolChoice.WireValue(),
		MaxTokens:  maxTokens,
		Thinking: QoderThinkingDirective{
			Specified: restricted.Thinking.Specified,
			Enabled:   restricted.Thinking.Enabled,
			Effort:    restricted.Thinking.Effort,
		},
		ExplicitSession: restricted.ExplicitSession(),
		PromptCacheKey:  restricted.CacheKey,
		MetadataUserID:  restricted.MetadataUserID,
	}, nil
}

// ExtractTopLevelModelJSON scans a raw JSON request object via jsontext and
// returns its trimmed top-level "model" string alongside whether body begins as
// a syntactically valid JSON object up through that member. Non-object bodies
// or syntax failures return ("", false) without decoding into map[string]any.
func ExtractTopLevelModelJSON(body []byte) (string, bool) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return "", false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return "", false
	}
	for dec.PeekKind() != '}' {
		keyTok, keyErr := dec.ReadToken()
		if keyErr != nil {
			return "", false
		}
		if keyTok.String() == "model" {
			valTok, valErr := dec.ReadToken()
			if valErr != nil {
				return "", false
			}
			if valTok.Kind() == '"' {
				return strings.TrimSpace(valTok.String()), true
			}
			return "", true
		}
		if skipErr := dec.SkipValue(); skipErr != nil {
			return "", false
		}
	}
	return "", true
}

// ExtractTopLevelStreamJSON scans a raw JSON request object via jsontext and
// reports whether its top-level "stream" member is set to boolean true.
// Malformed JSON, non-object payloads, or non-true values return false.
func ExtractTopLevelStreamJSON(body []byte) bool {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return false
	}
	for dec.PeekKind() != '}' {
		keyTok, keyErr := dec.ReadToken()
		if keyErr != nil {
			return false
		}
		if keyTok.String() == "stream" {
			valTok, valErr := dec.ReadToken()
			if valErr != nil {
				return false
			}
			return valTok.Kind() == 't'
		}
		if skipErr := dec.SkipValue(); skipErr != nil {
			return false
		}
	}
	return false
}

// executeConverted runs merged IR request conversion, model capability gating,
// conversation planning, upstream dispatch, and streaming or buffered response
// reading for one Qoder attempt. Client decode, IR validation, and stateless
// Responses rejections return a local *RequestError before credential waits or
// upstream transport calls begin.
//
// Transport acceptance publishes a versioned conversation snapshot ahead of
// response reading and binds QoderDeclaredToolNameMapper from the effective
// outbound tool slice so Stream and Buffer reverse upstream tool aliases before
// raising lowered client tools. Stream or buffer failures roll back that
// accepted snapshot, returning partial completion evidence on streaming paths
// only when both semantic output and token usage were observed.
func (e *Executor) executeConverted(
	ctx context.Context,
	executionCtx context.Context,
	start time.Time,
	target *Target,
	input upstream.AttemptInput,
	sink upstream.OutputSink,
) (upstream.AttemptResult, error) {
	switch input.Protocol {
	case protocol.ProtocolOpenAIChatCompletions,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolAnthropicMessages:
	default:
		return upstream.AttemptResult{}, errors.New("unsupported qoder client protocol")
	}

	if target.IngressError != nil {
		return upstream.AttemptResult{}, translateClientRequestError(target.IngressError)
	}

	dialect := convert.Dialect{AllowBillingHeader: true}
	if target.Dialect != nil {
		if rawModel, validObj := ExtractTopLevelModelJSON(input.Body); validObj && rawModel != "" {
			resolvedDialect, dialectErr := target.Dialect(ctx, rawModel, input.Protocol)
			if dialectErr != nil {
				return upstream.AttemptResult{}, translateProtocolQoderError(dialectErr)
			}
			dialect = resolvedDialect
		}
	}

	responseID := ""
	if input.Protocol == protocol.ProtocolOpenAIResponses {
		responseID = QoderResponsesID()
	}

	var toolNameMapper QoderToolNameMapper
	exchange, err := convert.NewExchange(input.Body, convert.Options{
		Route: convert.Route{
			Client:   input.Protocol,
			Upstream: protocol.ProtocolQoderChat,
			Dialect:  dialect,
		},
		ClientModel: strings.TrimSpace(input.ResponseModel),
		Runtime:     newQoderExchangeRuntime(responseID),
		MapUpstreamToolName: func(name string) string {
			if toolNameMapper == nil {
				return name
			}
			return toolNameMapper(name)
		},
	})
	if err != nil {
		return upstream.AttemptResult{}, translateClientRequestError(err)
	}

	upBytes, err := exchange.UpstreamRequest()
	if err != nil {
		return upstream.AttemptResult{}, translateClientRequestError(err)
	}
	protoReq, err := protocolqoder.DecodePayload(upBytes)
	if err != nil {
		return upstream.AttemptResult{}, translateClientRequestError(err)
	}
	request, err := PayloadRequestFromIR(protoReq)
	if err != nil {
		return upstream.AttemptResult{}, translateClientRequestError(err)
	}
	request.ResponseID = responseID

	responseModel := strings.TrimSpace(input.ResponseModel)
	if responseModel == "" {
		responseModel = request.Model
	}

	if target.Attributes != nil {
		attributes, resolveErr := target.Attributes(ctx, request.Model, input.Protocol)
		if resolveErr != nil {
			return upstream.AttemptResult{}, translateProtocolQoderError(resolveErr)
		}
		request.Capabilities = &attributes
		if attributes.Target.Model != "" {
			request.Model = attributes.Target.Model
		}
	}

	payload, model, plan, err := e.PrepareConversation(target, input.Protocol, request)
	if err != nil {
		return upstream.AttemptResult{}, translateProtocolQoderError(err)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return upstream.AttemptResult{}, fmt.Errorf("marshal qoder payload: %w", err)
	}

	// Recheck the caller's request context before credential waits and upstream
	// dispatch so requests cancelled during preparation never start inference.
	if err := ctx.Err(); err != nil {
		return upstream.AttemptResult{}, err
	}
	session, err := target.Session(ctx)
	if err != nil {
		return upstream.AttemptResult{}, fmt.Errorf("get qoder session: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return upstream.AttemptResult{}, err
	}
	client, err := target.Client()
	if err != nil {
		return upstream.AttemptResult{}, err
	}
	headers := map[string]string{
		"x-model-key":    model,
		"x-model-source": "system",
	}
	if err := ctx.Err(); err != nil {
		return upstream.AttemptResult{}, err
	}

	var response *http.Response
	if doerClient, ok := client.(streamClientWithDoer); ok && target.Doer != nil {
		response, err = doerClient.StreamRequestContextWithDoer(executionCtx, session, "", body, headers, target.Doer)
	} else {
		response, err = client.StreamRequestContext(executionCtx, session, "", body, headers)
	}
	if err != nil {
		return upstream.AttemptResult{}, translateProtocolQoderError(err)
	}

	// Publish versioned transport acceptance before reading SSE frames so stream
	// or buffer conversion failures can roll back this attempt cleanly.
	plan.CommitAccepted()
	toolNameMapper = QoderDeclaredToolNameMapper(QoderAnySlice(payload["tools"]))

	result := upstream.AttemptResult{
		Model:           responseModel,
		UpstreamModel:   model,
		Stream:          input.Stream,
		UpstreamHeaders: response.Header.Clone(),
		RequestID:       responseID,
	}

	convOpts := ConvertedResponseOptions{
		Exchange:                     exchange,
		RequestID:                    responseID,
		CommittedSSE:                 target.CommittedSSE,
		DeferRetryableCommittedError: target.DeferRetryableCommittedError,
		WriteLocalTerminalError:      target.WriteLocalTerminalError,
		WriteRawTerminalSSE:          target.WriteRawTerminalSSE,
	}

	var nativeUsage upstream.TokenUsage
	commitComplete := true

	if input.Stream {
		deferredOut := upstream.NewDeferredOutputContext(sink)
		streamRes, streamErr := ReadConvertedStreaming(executionCtx, deferredOut, response, convOpts)
		if streamRes != nil {
			result.UpstreamResponseModel = streamRes.UpstreamResponseModel
			nativeUsage = streamRes.Usage
			result.Usage = plan.RecordUsage(nativeUsage)
			result.HasUsage = streamRes.HasUsage
			result.Served = streamRes.Served
			result.ClientDisconnect = result.ClientDisconnect || streamRes.ClientDisconnect
			commitComplete = streamRes.Served
		}
		if streamErr != nil {
			// Revert accepted conversation history on stream failure while retaining
			// partial completion evidence only when both output and usage were observed.
			plan.RollbackAccepted()
			result.Duration = time.Since(start)
			if result.Served && result.HasUsage {
				return result, streamErr
			}
			return upstream.AttemptResult{
				UpstreamResponseModel: result.UpstreamResponseModel,
				Stream:                true,
				Duration:              result.Duration,
			}, streamErr
		}
	} else {
		bufRes, bufErr := ReadConvertedBuffered(executionCtx, sink, response, convOpts)
		if bufRes != nil {
			result.UpstreamResponseModel = bufRes.UpstreamResponseModel
			nativeUsage = bufRes.Usage
			result.Usage = plan.RecordUsage(nativeUsage)
			result.HasUsage = bufRes.HasUsage
			result.Served = bufRes.Served
			result.ClientDisconnect = result.ClientDisconnect || bufRes.ClientDisconnect
		}
		if bufErr != nil {
			// Roll back accepted conversation state whenever buffered SSE collection or
			// response encoding fails before a complete JSON body can be built.
			plan.RollbackAccepted()
			return upstream.AttemptResult{
				UpstreamResponseModel: result.UpstreamResponseModel,
			}, bufErr
		}
	}

	plan.LogUsage(target.Metadata, target.ProviderID, nativeUsage, result.Usage)
	// Commit completed conversation fingerprints and native usage after response
	// conversion succeeds for non-streaming or content-bearing streaming work.
	if commitComplete {
		plan.Commit(nativeUsage)
	}
	result.Duration = time.Since(start)
	return result, nil
}

// qoderMessagesFromIRPayload converts normalized protocol/qoder messages into
// QoderMessage values matching Qoder's desktop wire and fingerprinting shapes.
// Empty input slices return a non-nil empty slice, user turns retain discrete
// text blocks when multi-block, cache-marked, or explicitly blank, and
// assistant or tool turns keep scalar content unless tool blocks carry
// cache_control markers.
func qoderMessagesFromIRPayload(messages []protocolqoder.Message) []QoderMessage {
	if len(messages) == 0 {
		return []QoderMessage{}
	}
	out := make([]QoderMessage, 0, len(messages))
	for _, m := range messages {
		role := strings.TrimSpace(m.Role)
		switch role {
		case "user":
			var textContent []map[string]any
			if len(m.TextBlocks) > 1 || hasIRTextBlockCacheControl(m.TextBlocks) || (len(m.TextBlocks) == 1 && m.Text == "") {
				textContent = qoderTextBlocksToMaps(m.TextBlocks)
			}
			out = append(out, QoderMessage{
				Role: "user",
				Text: m.Text,
				Raw: map[string]any{
					"role":    "user",
					"content": m.Text,
				},
				TextContent: textContent,
			})

		case "assistant":
			raw := map[string]any{
				"role":    "assistant",
				"content": m.Text,
			}
			if len(m.ToolCalls) > 0 {
				calls := make([]any, 0, len(m.ToolCalls))
				for _, tc := range m.ToolCalls {
					args := tc.Function.Arguments
					if strings.TrimSpace(args) == "" {
						args = "{}"
					}
					callType := strings.TrimSpace(tc.Type)
					if callType == "" {
						callType = "function"
					}
					calls = append(calls, map[string]any{
						"id":   tc.ID,
						"type": callType,
						"function": map[string]any{
							"name":      tc.Function.Name,
							"arguments": args,
						},
					})
				}
				raw["tool_calls"] = calls
			}
			out = append(out, QoderMessage{
				Role: "assistant",
				Text: m.Text,
				Raw:  raw,
			})

		case "tool":
			raw := map[string]any{
				"role":         "tool",
				"tool_call_id": m.ToolCallID,
				"content":      m.Text,
			}
			if strings.TrimSpace(m.Name) != "" {
				raw["name"] = strings.TrimSpace(m.Name)
			}
			var textContent []map[string]any
			if hasIRTextBlockCacheControl(m.TextBlocks) {
				textContent = qoderTextBlocksToMaps(m.TextBlocks)
			}
			out = append(out, QoderMessage{
				Role:        "tool",
				Text:        m.Text,
				ToolCallID:  m.ToolCallID,
				Raw:         raw,
				TextContent: textContent,
			})
		}
	}
	return out
}

// hasIRTextBlockCacheControl reports whether any block in blocks carries a
// non-nil CacheControl with a non-blank Type string. Empty slices or blocks
// whose CacheControl is nil or blank return false.
func hasIRTextBlockCacheControl(blocks []protocolqoder.TextBlock) bool {
	for _, b := range blocks {
		if b.CacheControl != nil && strings.TrimSpace(b.CacheControl.Type) != "" {
			return true
		}
	}
	return false
}

// qoderTextBlocksToMaps converts discrete protocol/qoder TextBlock values into
// freshly allocated Qoder content block maps. Blocks carrying a non-blank
// CacheControl.Type attach a copied cache_control map so downstream mutation
// never alters caller-owned slices.
func qoderTextBlocksToMaps(blocks []protocolqoder.TextBlock) []map[string]any {
	out := make([]map[string]any, 0, len(blocks))
	for _, b := range blocks {
		block := map[string]any{
			"type": "text",
			"text": b.Text,
		}
		if b.CacheControl != nil && strings.TrimSpace(b.CacheControl.Type) != "" {
			block["cache_control"] = map[string]any{
				"type": strings.TrimSpace(b.CacheControl.Type),
			}
		}
		out = append(out, block)
	}
	return out
}

// qoderToolsFromIRPayload converts normalized protocol/qoder function tool
// definitions into Qoder's outbound tool slice while preserving raw JSON Schema
// parameter bytes without float64 unmarshaling. Empty tool lists return a
// non-nil empty slice, blank function names are skipped, empty descriptions are
// omitted for fingerprint parity, and missing or null schemas default to an
// empty JSON object schema.
func qoderToolsFromIRPayload(tools []protocolqoder.Tool) []any {
	if len(tools) == 0 {
		return []any{}
	}
	out := make([]any, 0, len(tools))
	for _, t := range tools {
		name := strings.TrimSpace(t.Function.Name)
		if name == "" {
			continue
		}
		params := bytes.TrimSpace(t.Function.Parameters)
		if len(params) == 0 || bytes.Equal(params, []byte("null")) {
			// Supply an empty JSON Schema object when tool parameters are omitted or null
			// so Qoder's desktop gateway accepts parameterless function declarations.
			params = []byte(`{"type":"object","properties":{}}`)
		} else {
			params = bytes.Clone(params)
		}
		fn := map[string]any{
			"name":       name,
			"parameters": json.RawMessage(params),
		}
		if t.Function.Description != "" {
			fn["description"] = t.Function.Description
		}
		if t.Function.Strict != nil {
			fn["strict"] = *t.Function.Strict
		}
		toolType := strings.TrimSpace(t.Type)
		if toolType == "" {
			toolType = "function"
		}
		out = append(out, map[string]any{
			"type":     toolType,
			"function": fn,
		})
	}
	return out
}

// translateProtocolQoderError converts protocol/qoder RequestError and APIError
// values into upstream/qoder's concrete error types for unified gateway
// classification. Nil inputs and errors outside those protocol types return
// unchanged so context cancellation and transport errors stay intact.
func translateProtocolQoderError(err error) error {
	if err == nil {
		return nil
	}
	if reqErr, ok := errors.AsType[*RequestError](err); ok {
		return reqErr
	}
	if apiErr, ok := errors.AsType[*APIError](err); ok {
		return apiErr
	}
	if protoReqErr, ok := errors.AsType[*protocolqoder.RequestError](err); ok {
		return &RequestError{
			Code:    FirstNonEmptyQoder(protoReqErr.Code, protocolqoder.CodeInvalidRequest),
			Message: protoReqErr.Message,
		}
	}
	if protoAPIErr, ok := errors.AsType[*protocolqoder.APIError](err); ok {
		return &APIError{
			StatusCode:          protoAPIErr.StatusCode,
			Body:                protoAPIErr.Body,
			Code:                protoAPIErr.Code,
			Message:             protoAPIErr.Message,
			AgentLimitResetTime: protoAPIErr.AgentLimitResetTime,
		}
	}
	return err
}

// translateClientRequestError converts any client request decoding, IR
// validation, or stateless Qoder preflight failure into a *RequestError before
// credential acquisition or upstream network dispatch begins. Nil inputs return
// nil, and existing *RequestError codes pass through unchanged.
func translateClientRequestError(err error) error {
	if err == nil {
		return nil
	}
	translated := translateProtocolQoderError(err)
	if reqErr, ok := errors.AsType[*RequestError](translated); ok {
		return reqErr
	}
	return &RequestError{
		Code:    protocolqoder.CodeInvalidRequest,
		Message: translated.Error(),
	}
}
