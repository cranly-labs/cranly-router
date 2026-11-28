package forward

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	protocolcore "github.com/TokenFlux/TokenRouter/internal/protocol"
	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// conversion.go forwards OpenAI Responses and Chat Completions requests through
// Anthropic, Vertex, and Bedrock Messages targets via IR exchanges and extracts
// normalized client reasoning effort. Conversion preserves the client's logical
// model and stream mode while patching the encoded Messages body to stream
// upstream under the mapped provider model, running OAuth mimicry and cache
// limits before Build and Send, and stripping rejected thinking signatures once
// on HTTP 400 before applying provider health and failover policy.
//
// Recorded effort combines client syntax, mapped model precedence, converted
// Anthropic output_config.effort, pre-stripping IR effort when StripEffort is
// active, and model-specific thinking fallbacks. Selected wire speed fills
// missing result speed metadata after response reading without overriding any
// speed reported by the upstream response stream.

// rawForwardJSONMember holds one JSON object key and its cloned raw value slice
// in wire encounter order during lossless converted Messages patching.
type rawForwardJSONMember struct {
	name  string         // decoded JSON object key
	value jsontext.Value // verbatim raw JSON value bytes
}

// AsConverted executes one IR-based OpenAI Responses or Chat attempt against an
// Anthropic, Vertex, or Bedrock Messages provider and delegates response reading
// to the merged output reader. Legacy Responses ingress normalization runs only
// on Responses input, and ResolveModel errors (such as Bedrock regional routing
// rejections) stop execution before credentials or upstream exchanges run.
//
// Post-encoding preparation preserves Mimic -> CacheLimit -> Build ordering,
// where Build applies model attributes and 1h cache TTL injection. Rejected
// cross-model thinking signatures on HTTP 400 trigger one lossless jsontext
// stripping retry on the same provider before health policy and failover run.
func AsConverted(
	ctx context.Context,
	source protocolcore.ProtocolID,
	p ConvertedMessagesPorts,
	in ConversionInput,
	body []byte,
) (*Result, error) {
	if source != protocolcore.ProtocolOpenAIResponses && source != protocolcore.ProtocolOpenAIChatCompletions {
		return nil, fmt.Errorf("unsupported converted messages source protocol %q", source)
	}
	startTime := time.Now()

	if source == protocolcore.ProtocolOpenAIResponses {
		normalizedBody, normalized, err := p.NormalizeResponses(body)
		if err != nil {
			p.WriteClientError(400, "invalid_request_error", "Failed to parse request body")
			return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(err)
		}
		if normalized {
			body = normalizedBody
		}
	}

	originalModel, ok := extractConvertedClientModelBytes(body)
	if !ok {
		p.WriteClientError(400, "invalid_request_error", "Failed to parse request body")
		parseErr := fmt.Errorf("parse %s request: invalid JSON object", source)
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(parseErr)
	}
	if strings.TrimSpace(originalModel) == "" {
		p.WriteClientError(400, "invalid_request_error", "model is required")
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "model is required")
	}

	mappedModel, err := p.ResolveModel(ctx, originalModel)
	if err != nil {
		return nil, err
	}
	if mappedModel == "" {
		mappedModel = originalModel
	}

	dialect, passback, err := p.ResolveDialect(ctx, mappedModel, body)
	if err != nil {
		return nil, fmt.Errorf("resolve anthropic conversion dialect: %w", err)
	}

	exchange, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   source,
			Upstream: protocolcore.ProtocolAnthropicMessages,
			Dialect:  dialect,
		},
		ClientModel: originalModel,
		Runtime:     p.ConversionRuntime(),
	})
	if err != nil {
		p.WriteClientError(400, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("convert %s to anthropic: %w", source, err))
	}

	baseAnthropicBody, err := exchange.UpstreamRequest()
	if err != nil {
		p.WriteClientError(400, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("encode converted anthropic request: %w", err))
	}
	upstream.ObserveExchangeConversion(ctx, exchange)

	baseAnthropicBody, err = patchConvertedMessagesBodyBytes(baseAnthropicBody, mappedModel)
	if err != nil {
		return nil, fmt.Errorf("patch converted anthropic request: %w", err)
	}

	clientStream := exchange.Info().Stream
	noticeLabel := "gateway forward_as_chat_completions: model mapping applied"
	if source == protocolcore.ProtocolOpenAIResponses {
		noticeLabel = "gateway forward_as_responses: model mapping applied"
	}
	p.ModelNotice(noticeLabel, originalModel, mappedModel, clientStream)

	if err := p.Credential(ctx); err != nil {
		return nil, fmt.Errorf("get access token: %w", err)
	}

	// OAuth mimicry applies only to direct Anthropic OAuth exchanges; Bedrock
	// Invoke requests never carry Claude Code OAuth headers or mimic markers.
	shouldMimicClaudeCode := in.OAuth && !in.Bedrock
	dispatchOnce := func(candidateBase []byte, losses protocolanthropic.SignatureRetryLosses) (Response, []byte, error) {
		prepared := candidateBase
		if shouldMimicClaudeCode {
			systemRaw := extractConvertedMessagesSystemBytes(candidateBase)
			prepared = p.Mimic(ctx, prepared, systemRaw, mappedModel)
		}
		prepared = p.CacheLimit(prepared)
		wire, buildErr := p.Build(ctx, prepared, mappedModel, true, shouldMimicClaudeCode)
		if buildErr != nil {
			return Response{}, nil, fmt.Errorf("build upstream request: %w", buildErr)
		}
		if losses.Tokens > 0 || losses.Reasoning > 0 {
			// Prepare the diagnostic snapshot only after the replacement payload
			// builds successfully and before the transport assigns its dispatch ordinal.
			upstream.StageSignatureRetry(ctx, exchange, losses.Tokens, losses.Reasoning)
		}
		sentResp, sendErr := p.Send(ctx)
		if sendErr != nil {
			return Response{}, nil, sendErr
		}
		return sentResp, wire, nil
	}

	resp, wireBody, err := dispatchOnce(baseAnthropicBody, protocolanthropic.SignatureRetryLosses{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if resp.Close != nil {
			resp.Close()
		}
	}()

	var preReadErrBody []byte
	preReadDone := false
	if resp.StatusCode == 400 {
		respBody, readErr := p.ReadErrorBody()
		preReadErrBody = respBody
		preReadDone = true
		if readErr == nil && p.IsSignatureError(ctx, respBody) {
			// Cross-model Claude attempts can reject replayed trc1:anthropic
			// signatures with HTTP 400; strip historical reasoning signatures
			// losslessly via jsontext and retry once on the same provider.
			if strippedBase, stripped, losses := protocolanthropic.StripMessagesSignatureBlocksWithLosses(baseAnthropicBody, passback); stripped {
				p.ObserveSignatureRetry(resp.StatusCode, respBody, p.ErrorMessage(respBody))
				if resp.Close != nil {
					prevClose := resp.Close
					resp.Close = nil
					prevClose()
				}
				retryResp, retryWire, retryErr := dispatchOnce(strippedBase, losses)
				if retryErr != nil {
					return nil, retryErr
				}
				resp = retryResp
				wireBody = retryWire
				baseAnthropicBody = strippedBase
				preReadErrBody = nil
				preReadDone = false
			}
		}
	}

	if resp.StatusCode >= 400 {
		return nil, handleConvertedHTTPError(ctx, p, resp.StatusCode, mappedModel, preReadErrBody, preReadDone)
	}

	isChat := source == protocolcore.ProtocolOpenAIChatCompletions
	reasoningEffort := p.Effort(body, isChat, mappedModel, originalModel)
	if convertedEffort := extractConvertedMessagesEffortBytes(baseAnthropicBody); convertedEffort != nil {
		reasoningEffort = convertedEffort
	}
	// Retain pre-stripping client effort for billing when StripEffort cleared
	// outgoing wire effort fields during IR request encoding.
	if reasoningEffort == nil && dialect.StripEffort {
		if preStrip := protocolcore.NormalizeClaudeOutputEffort(exchange.Info().Effort); preStrip != nil {
			reasoningEffort = preStrip
		}
	}
	reasoningEffort = p.ThinkingFallback(reasoningEffort, body, mappedModel)

	result, handleErr := p.ReadConverted(ctx, exchange, clientStream, originalModel, mappedModel, reasoningEffort, startTime)
	// Fill missing response speed from the outgoing wire hint without overriding
	// any explicit speed reported by the upstream stream.
	if result != nil && strings.TrimSpace(result.Usage.Speed) == "" &&
		strings.EqualFold(extractConvertedMessagesSpeedBytes(wireBody), "fast") {
		result.Usage.Speed = "fast"
	}
	return result, handleErr
}

// ExtractEffort returns normalized recorded effort or nil when no usable value
// exists. Nested reasoning.effort wins; Chat input alone can use reasoning_effort
// when the nested value is blank. A nonnil normalizer is required once a raw
// effort is available.
//
// The first trimmed nonempty model candidate wins over the request's model,
// allowing mapped identity to govern capability-sensitive normalization. Blank
// normalized output becomes nil even when input effort was present. The result
// points to a local string value, and gjson extraction does not validate the
// entire request body.
func ExtractEffort(body []byte, chat bool, normalize func(string, string) string, models ...string) *string {
	raw := strings.TrimSpace(gjson.GetBytes(body, "reasoning.effort").String())
	// A nonempty nested value blocks the Chat alias even if normalization later
	// rejects it. This makes field precedence a syntax decision before model
	// capability filtering decides whether the selected effort is usable.
	if raw == "" && chat {
		raw = strings.TrimSpace(gjson.GetBytes(body, "reasoning_effort").String())
	}
	if raw == "" {
		return nil
	}
	model := ""
	for _, candidate := range models {
		if value := strings.TrimSpace(candidate); value != "" {
			model = value
			break
		}
	}
	// Supplied candidates establish execution identity; request model is the
	// final default when none is usable. The normalizer can therefore interpret
	// client effort using a mapped model instead of an unresolved input alias.
	if model == "" {
		model = strings.TrimSpace(gjson.GetBytes(body, "model").String())
	}
	value := normalize(raw, model)
	if value == "" {
		return nil
	}
	return &value
}

// handleConvertedHTTPError classifies an upstream HTTP >= 400 failure on
// AsConverted using already-captured bytes when preReadDone is true or reading
// error bytes once otherwise. Generic provider decisions emit HTTP 500 before
// failover checks, and non-failover statuses map >= 500 responses to 502.
func handleConvertedHTTPError(
	ctx context.Context,
	p ConvertedMessagesPorts,
	status int,
	mappedModel string,
	preReadBody []byte,
	preReadDone bool,
) error {
	respBody := preReadBody
	if !preReadDone {
		// Retain available partial diagnostic bytes even if reading reports an
		// error so health classification and failover still inspect captured text.
		respBody, _ = p.ReadErrorBody()
	}
	upstreamMsg := p.ErrorMessage(respBody)
	decision := p.Health(ctx, status, respBody, mappedModel)

	if decision.Generic {
		p.WriteClientError(500, "server_error", "Upstream gateway error")
		return fmt.Errorf("upstream error: %d (not in custom error codes)", status)
	}

	if decision.Failover {
		p.FailoverNotice(status, upstreamMsg)
		return p.FailoverError(status, respBody, decision.RetrySameProvider)
	}

	p.WriteClientError(MapStatus(status), "server_error", upstreamMsg)
	return fmt.Errorf("upstream error: %d %s", status, upstreamMsg)
}

// extractConvertedClientModelBytes scans a top-level client JSON object via
// jsontext with invalid UTF-8 tolerated and returns its string model field. The
// boolean reports whether body is a syntactically valid top-level JSON object.
func extractConvertedClientModelBytes(body []byte) (string, bool) {
	members, err := parseForwardJSONObject(body)
	if err != nil {
		return "", false
	}
	for _, m := range slices.Backward(members) {
		if m.name != "model" {
			continue
		}
		var model string
		if jsonv2.Unmarshal(m.value, &model, jsontext.AllowInvalidUTF8(true)) == nil {
			return model, true
		}
		return "", true
	}
	return "", true
}

// patchConvertedMessagesBodyBytes sets top-level model to mappedModel and
// stream to true on an encoded Anthropic Messages JSON object via ordered
// jsontext members. Unknown Extra fields and 64-bit integer literals survive
// verbatim, while malformed JSON objects return a decoding error.
func patchConvertedMessagesBodyBytes(body []byte, mappedModel string) ([]byte, error) {
	members, err := parseForwardJSONObject(body)
	if err != nil {
		return nil, err
	}
	rawModel, err := jsonv2.Marshal(mappedModel, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return nil, err
	}
	members = upsertForwardJSONMember(members, "model", rawModel)
	members = upsertForwardJSONMember(members, "stream", jsontext.Value("true"))
	return encodeForwardJSONObject(members)
}

// extractConvertedMessagesSystemBytes extracts a cloned raw JSON value of
// top-level "system" from an Anthropic Messages body via jsontext. Absent
// system fields or malformed JSON objects return nil.
func extractConvertedMessagesSystemBytes(body []byte) json.RawMessage {
	members, err := parseForwardJSONObject(body)
	if err != nil {
		return nil
	}
	for _, m := range slices.Backward(members) {
		if m.name == "system" {
			return json.RawMessage(bytes.Clone(m.value))
		}
	}
	return nil
}

// extractConvertedMessagesEffortBytes extracts and normalizes top-level
// output_config.effort from an Anthropic Messages body via jsontext. Missing
// output_config objects, non-string effort fields, or unrecognized effort
// levels return nil so earlier caller observations remain intact.
func extractConvertedMessagesEffortBytes(body []byte) *string {
	members, err := parseForwardJSONObject(body)
	if err != nil {
		return nil
	}
	for _, m := range slices.Backward(members) {
		if m.name != "output_config" || len(m.value) == 0 || m.value[0] != '{' {
			continue
		}
		subMembers, subErr := parseForwardJSONObject(m.value)
		if subErr != nil {
			return nil
		}
		for _, sub := range slices.Backward(subMembers) {
			if sub.name != "effort" {
				continue
			}
			var effort string
			if jsonv2.Unmarshal(sub.value, &effort, jsontext.AllowInvalidUTF8(true)) != nil {
				return nil
			}
			return protocolcore.NormalizeClaudeOutputEffort(effort)
		}
	}
	return nil
}

// extractConvertedMessagesSpeedBytes reads top-level "speed" from wireBody via
// jsontext and returns its trimmed string value. Missing fields, non-string
// values, or non-object payloads return an empty string.
func extractConvertedMessagesSpeedBytes(wireBody []byte) string {
	members, err := parseForwardJSONObject(wireBody)
	if err != nil {
		return ""
	}
	for _, m := range slices.Backward(members) {
		if m.name != "speed" {
			continue
		}
		var speed string
		if jsonv2.Unmarshal(m.value, &speed, jsontext.AllowInvalidUTF8(true)) == nil {
			return strings.TrimSpace(speed)
		}
	}
	return ""
}

// parseForwardJSONObject decodes one top-level JSON object into ordered raw
// members with invalid UTF-8 permitted. Empty slices, non-object roots, or
// trailing tokens after the closing brace return an error.
func parseForwardJSONObject(raw []byte) ([]rawForwardJSONMember, error) {
	if len(raw) == 0 {
		return nil, io.ErrUnexpectedEOF
	}
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '{' {
		return nil, io.ErrUnexpectedEOF
	}
	var members []rawForwardJSONMember
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return nil, kErr
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return nil, vErr
		}
		members = append(members, rawForwardJSONMember{
			name:  key,
			value: val.Clone(),
		})
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return nil, io.ErrUnexpectedEOF
	}
	return members, nil
}

// upsertForwardJSONMember replaces the first existing member named name in
// place while dropping duplicate keys, or appends it at the end of members when
// absent. Unrelated member slices remain shared with the input slice.
func upsertForwardJSONMember(members []rawForwardJSONMember, name string, val jsontext.Value) []rawForwardJSONMember {
	out := make([]rawForwardJSONMember, 0, len(members)+1)
	replaced := false
	for _, m := range members {
		if m.name == name {
			if !replaced {
				out = append(out, rawForwardJSONMember{name: name, value: val})
				replaced = true
			}
			continue
		}
		out = append(out, m)
	}
	if !replaced {
		out = append(out, rawForwardJSONMember{name: name, value: val})
	}
	return out
}

// encodeForwardJSONObject serializes ordered raw JSON object members into a
// single compact JSON object slice with invalid UTF-8 preserved.
func encodeForwardJSONObject(members []rawForwardJSONMember) ([]byte, error) {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return nil, err
	}
	for _, m := range members {
		if err := enc.WriteToken(jsontext.String(m.name)); err != nil {
			return nil, err
		}
		if err := enc.WriteValue(m.value); err != nil {
			return nil, err
		}
	}
	if err := enc.WriteToken(jsontext.EndObject); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
