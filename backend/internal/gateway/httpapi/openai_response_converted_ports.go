package httpapi

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	openaiexecution "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// openai_response_converted_ports.go adapts OpenAITextExecutor attempt state to
// openaiforward.ConvertedResponsesPorts for merged IR Responses execution.
// Sharing openAIChatExecutionAdapter's transport, credential, session, and
// policy methods keeps provider selection and error handling identical while
// resolving model rules directly from raw request bytes for convert.Exchange.

// openAIConvertedResponsesAdapter implements ConvertedResponsesPorts for one
// text execution attempt.
type openAIConvertedResponsesAdapter struct {
	*openAIChatExecutionAdapter // shared Messages and Chat attempt services
}

// newConvertedResponsesAdapter constructs an attempt-scoped adapter for
// converted Messages and Chat execution through RunConvertedResponses.
func (s *OpenAITextExecutor) newConvertedResponsesAdapter(
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	tls []egress.TLSFingerprintRouterMatchResult,
) *openAIConvertedResponsesAdapter {
	return &openAIConvertedResponsesAdapter{
		openAIChatExecutionAdapter: &openAIChatExecutionAdapter{
			openAIMessagesExecutionAdapter: &openAIMessagesExecutionAdapter{
				s:        s,
				c:        c,
				provider: provider,
				tls:      tls,
			},
		},
	}
}

// PrepareMessages resolves the selected attempt provider, resets request
// observations, and returns its Messages capability profile and dispatch target.
// Identity or route lookup errors abort execution before parsing client bytes.
func (p *openAIConvertedResponsesAdapter) PrepareMessages(ctx context.Context) (openaiexecution.MessagesProfile, openaiexecution.Dispatch, error) {
	return p.Prepare(ctx)
}

// DispatchMessages delegates non-Responses Messages routes to NativeMessages or
// MessagesViaRawChat using the active attempt state and TLS routing matches.
func (p *openAIConvertedResponsesAdapter) DispatchMessages(ctx context.Context, route openaiexecution.Dispatch, body []byte, key, model string) (*openaiexecution.Result, error) {
	return p.Dispatch(ctx, route, body, key, model)
}

// SourceError marks response commitment on p.c and writes a local error in
// source's client protocol envelope. Anthropic Messages sources emit Anthropic
// JSON or SSE errors, while other sources emit OpenAI rejections.
func (p *openAIConvertedResponsesAdapter) SourceError(source protocol.ProtocolID, status int, kind, message string) {
	if source == protocol.ProtocolAnthropicMessages {
		p.Error(status, kind, message)
		return
	}
	p.ChatError(status, kind, message)
}

// ResolveResponsesDialect resolves model catalog attributes from raw request
// bytes and builds the target convert.Dialect for OpenAI Responses execution.
// Successful attribute lookup updates p.provider.Capabilities on the attempt,
// and missing HTTP requests fall back to context.Background().
func (p *openAIConvertedResponsesAdapter) ResolveResponsesDialect(model string, body []byte) (convert.Dialect, error) {
	ctx := context.Background()
	if p.c != nil && p.c.Request != nil {
		ctx = p.c.Request.Context()
	}
	res, err := p.modelResolutionFromBytes(ctx, model, body, "openai_responses")
	if err != nil {
		return convert.Dialect{}, err
	}
	return gatewayprovider.ConversionDialect(ctx, p.provider, res, protocol.ProtocolOpenAIResponses), nil
}

// ConversionRuntime returns the default wall-clock and crypto/rand ID runtime
// used by convert.Exchange.
func (p *openAIConvertedResponsesAdapter) ConversionRuntime() convert.Runtime {
	return convert.DefaultRuntime()
}

// ConvertCursorResponsesToChat resolves target Chat Completions model rules for
// body's mapped model and converts an input-shaped Cursor Chat request into
// Chat Completions JSON bytes via convert.NewExchange. Capability or syntax
// errors return before delegating to the target route.
func (p *openAIConvertedResponsesAdapter) ConvertCursorResponsesToChat(body []byte) ([]byte, error) {
	ctx := context.Background()
	if p.c != nil && p.c.Request != nil {
		ctx = p.c.Request.Context()
	}
	reqModel := extractCursorRequestModelBytes(body)
	targetModel := reqModel
	if p.provider != nil && reqModel != "" {
		if mapped := p.UpstreamModel(p.BillingModel(reqModel, "")); mapped != "" {
			targetModel = mapped
		}
	}
	res, err := p.modelResolutionFromBytes(ctx, targetModel, body, "openai_chat_completions")
	if err != nil {
		return nil, err
	}
	dialect := gatewayprovider.ConversionDialect(ctx, p.provider, res, protocol.ProtocolOpenAIChatCompletions)
	ex, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolOpenAIChatCompletions,
			Dialect:  dialect,
		},
		Runtime: p.ConversionRuntime(),
	})
	if err != nil {
		return nil, err
	}
	return ex.UpstreamRequest()
}

// MessagesEffortValue reconciles explicit Anthropic max effort against the
// resolved catalog attributes on p.provider.
func (p *openAIConvertedResponsesAdapter) MessagesEffortValue(clientEffort, model, convertedEffort string) string {
	return gatewayprovider.OpenAICompatAnthropicReasoningEffortValue(
		clientEffort,
		model,
		convertedEffort,
		gatewayprovider.ExecutionModelAttributes(p.provider, model),
	)
}

// SourceErrorResponse formats upstream HTTP >= 400 rejections in source's
// client envelope using the replayable error body installed on r. Callers
// retain ownership of closing r.Body after handling returns.
func (p *openAIConvertedResponsesAdapter) SourceErrorResponse(source protocol.ProtocolID, r *http.Response, model string) (*openaiexecution.Result, error) {
	if source == protocol.ProtocolAnthropicMessages {
		return p.ErrorResponse(r, model)
	}
	return p.ChatErrorResponse(r, model)
}

// ConvertedResponseOptions builds merged reader callbacks for
// ReadConvertedStreaming and ReadConvertedBuffered on the active HTTP attempt.
// Callers keep the HTTP context and response alive until reading finishes.
func (p *openAIConvertedResponsesAdapter) ConvertedResponseOptions(
	r *http.Response,
	source protocol.ProtocolID,
	original, billing, model string,
	requestBodyLen int,
) openai.ConvertedResponseOptions {
	return p.s.Output.ConvertedOptions(p.c, p.provider, r, source, original, billing, model, requestBodyLen)
}

// UpdateGrokUsage records parsed Grok OAuth quota-header snapshots on HTTP 2xx
// Responses exchanges and clears recoverable rate-limit state when quota
// headers are absent. Nil adapters, executors, providers, or Grok health
// services return safely without mutating provider state.
func (p *openAIConvertedResponsesAdapter) UpdateGrokUsage(ctx context.Context, model string, headers http.Header, status int) {
	if p == nil || p.openAIChatExecutionAdapter == nil || p.openAIMessagesExecutionAdapter == nil ||
		p.s == nil || p.s.Grok == nil || p.s.Grok.Health == nil || p.provider == nil {
		return
	}
	p.s.Grok.Health.ObserveResponse(ctx, p.provider.View(), headers, status, model)
}

// modelResolutionFromBytes resolves model catalog attributes for model and
// adjusts them directly against raw request bytes without struct re-marshaling.
// When p.provider is non-nil, successful base resolution publishes the base
// Resolution pointer onto p.provider.Capabilities before request-state adjustment.
func (p *openAIMessagesExecutionAdapter) modelResolutionFromBytes(
	ctx context.Context,
	model string,
	body []byte,
	targetProtocol string,
) (modelrules.Resolution, error) {
	result, err := gatewayprovider.ResolveModelAttributes(ctx, p.provider, model, p.s.modelAttributes(), targetProtocol)
	if err != nil {
		return modelrules.Resolution{}, err
	}
	if p.provider != nil {
		p.provider.Capabilities = &result
	}
	return result.ForRequest(gatewayprovider.ModelRequestState(body)), nil
}

// extractCursorRequestModelBytes reads the top-level string model field from
// raw JSON bytes via jsontext while permitting invalid UTF-8 in sibling fields.
// Empty slices, non-object payloads, or malformed tokens return an empty string.
func extractCursorRequestModelBytes(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return ""
	}
	var model string
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil {
			return ""
		}
		k := kTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return ""
		}
		if k == "model" {
			_ = jsonv2.Unmarshal(val, &model, jsontext.AllowInvalidUTF8(true))
		}
	}
	return model
}
