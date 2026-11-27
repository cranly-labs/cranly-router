package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	openaiexecution "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/tierpolicy"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// openai_raw_fallback_ports.go binds converted Messages and Responses requests
// to Chat-only upstream execution.
// Messages and Responses inputs can target a Chat-only upstream with their own
// client error envelopes, fast-tier rejections, and terminal SSE framing while
// keeping credentials, TLS profiles, and model attribution local to one attempt.
//
// openAIRawFallbackAdapter supplies shared Chat-only transport and model
// preparation services (also embedded by openAIRawChatAdapter), while
// openAIConvertedRawChatAdapter resolves target Chat model rules directly from
// raw request bytes, constructs pure convert.Dialect values, and binds
// ConvertedRawOptions for merged IR readers.

// openAIRawFallbackAdapter binds Chat-only exchanges to shared provider,
// credential, and error services.
type openAIRawFallbackAdapter struct {
	// openAIMessagesExecutionAdapter retains the selected provider and request
	// services shared with the fallback runner until synchronous output finishes.
	*openAIMessagesExecutionAdapter
	// kind selects Messages rejection for NativeMessages; other fallback kinds
	// use Responses or Chat error formatting.
	kind openaiexecution.NativeAnthropicKind
}

// openAIConvertedRawChatAdapter implements ConvertedRawChatPorts for one Chat
// fallback execution attempt.
type openAIConvertedRawChatAdapter struct {
	*openAIRawFallbackAdapter // shared Chat fallback transport and error services
}

// PrepareModel applies selected-provider model attributes for Chat Completions
// request bytes. The request helper extracts the body model and delegates
// preparation; errors return before the runner obtains its target or sends the
// fallback request.
func (p *openAIRawFallbackAdapter) PrepareModel(ctx context.Context, body []byte) ([]byte, error) {
	return p.s.Requests.applyModelAttributes(ctx, p.provider, body, "openai_chat_completions")
}

// FastFallback applies the current provider fast-tier policy to the outgoing
// Chat body. A BlockedError writes the dedicated heartbeat-aware policy
// rejection; callers terminate forwarding on the returned error rather than
// sending the rejected tier.
func (p *openAIRawFallbackAdapter) FastFallback(ctx context.Context, m string, b []byte) ([]byte, error) {
	updated, err := tierpolicy.ApplyBody(b, p.s.FastPolicy.Input(ctx, p.provider, m))
	if blocked, ok := errors.AsType[*tierpolicy.BlockedError](err); ok {
		WriteFastPolicyBlockedResponse(p.c, blocked)
	}
	return updated, err
}

// ObserveModel records the chosen upstream model in the request's Ops context.
func (p *openAIRawFallbackAdapter) ObserveModel(m string) { SetOpsUpstreamModel(p.c, m) }

// Target delegates Chat fallback endpoint and credential resolution for the
// selected provider.
func (p *openAIRawFallbackAdapter) Target(ctx context.Context) (string, string, error) {
	return p.s.Requests.ChatFallbackTarget(ctx, p.provider)
}

// Endpoint publishes the actual Chat fallback endpoint for request
// observation.
func (p *openAIRawFallbackAdapter) Endpoint(v string) {
	SetActualOpenAIUpstreamEndpoint(p.c, v)
}

// SendCC clears model observation and sends the prepared fallback Chat body
// with the provider's User-Agent and TLS matches. The runner owns a successful
// response body and closes it after output or failure handling.
func (p *openAIRawFallbackAdapter) SendCC(ctx context.Context, url string, b []byte, stream bool, key string) (*http.Response, error) {
	resetResponseModel(p.c)
	return p.s.Requests.SendChat(ctx, p.c, p.provider, url, b, stream, key, p.provider.View().GetOpenAIUserAgent(), "", p.tls...)
}

// AnthropicError delegates captured HTTP failure handling to the
// Messages-compatible writer. It converts any returned result for the fallback
// runner; the caller retains body cleanup and honors a returned failover
// signal before another provider attempt.
func (p *openAIRawFallbackAdapter) AnthropicError(r *http.Response, m string) (*openaiexecution.Result, error) {
	v, e := p.s.messagesError(r, p.c, p.provider, m)
	return openaiexecution.FromForwardResult(v), e
}

// ResponsesError delegates HTTP failure handling to Responses output using the
// captured request bytes. It converts the returned observation for the runner;
// response cleanup and actual provider-loop retry decisions remain with the
// execution caller.
func (p *openAIRawFallbackAdapter) ResponsesError(ctx context.Context, r *http.Response, b []byte, m string) (*openaiexecution.Result, error) {
	v, e := p.s.Output.ResponseError(ctx, r, p.c, p.provider, b, m)
	return openaiexecution.FromForwardResult(v), e
}

// newConvertedRawChatAdapter constructs an attempt-scoped adapter for converted
// Messages and Responses execution through RunConvertedRawChat.
func (s *OpenAITextExecutor) newConvertedRawChatAdapter(
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	kind openaiexecution.NativeAnthropicKind,
	tls []egress.TLSFingerprintRouterMatchResult,
) *openAIConvertedRawChatAdapter {
	return &openAIConvertedRawChatAdapter{
		openAIRawFallbackAdapter: &openAIRawFallbackAdapter{
			openAIMessagesExecutionAdapter: &openAIMessagesExecutionAdapter{
				s:        s,
				c:        c,
				provider: provider,
				tls:      tls,
			},
			kind: kind,
		},
	}
}

// Profile describes the selected provider, Grok OAuth mode, and shadow flag for
// the merged Chat conversion runner.
func (p *openAIConvertedRawChatAdapter) Profile() openaiexecution.MessagesProfile {
	return openaiexecution.MessagesProfile{
		Profile:   openAIForwardProfile(p.provider),
		ID:        p.provider.Record.ID,
		GrokOAuth: p.provider.View().IsGrokOAuth(),
		Shadow:    p.provider.View().IsShadow(),
	}
}

// SourceError marks response commitment on p.c and writes a local error in
// source's client protocol envelope. Already committed SSE streams receive an
// untimed terminal SSE error frame (synthetic response.failed for Responses
// clients, event: error for Messages clients) instead of raw JSON.
func (p *openAIConvertedRawChatAdapter) SourceError(source protocol.ProtocolID, status int, kind, message string) {
	MarkResponseCommitted(p.c)
	switch source {
	case protocol.ProtocolAnthropicMessages:
		WriteForwardAnthropicError(p.c, status, kind, message)
	case protocol.ProtocolOpenAIResponses:
		if CommittedSSE(p.c) {
			if WriteResponsesFailedSSE(p.c, kind, "", message, ErrorRequestID(p.c), ErrorRequestModel(p.c)) {
				return
			}
		}
		WriteForwardResponsesFallbackError(p.c, status, kind, message)
	default:
		WriteForwardChatError(p.c, status, kind, message)
	}
}

// ResolveChatDialect resolves target Chat Completions model rules from raw
// request bytes and constructs the target convert.Dialect for the selected
// provider attempt. Successful lookup publishes base capabilities onto
// p.provider.Capabilities, and missing HTTP requests fall back to Background.
func (p *openAIConvertedRawChatAdapter) ResolveChatDialect(model string, body []byte) (convert.Dialect, error) {
	ctx := context.Background()
	if p.c != nil && p.c.Request != nil {
		ctx = p.c.Request.Context()
	}
	res, err := p.modelResolutionFromBytes(ctx, model, body, "openai_chat_completions")
	if err != nil {
		return convert.Dialect{}, err
	}
	return gatewayprovider.ConversionDialect(ctx, p.provider, res, protocol.ProtocolOpenAIChatCompletions), nil
}

// ConversionRuntime returns the default wall-clock and crypto/rand ID runtime
// used by convert.Exchange.
func (p *openAIConvertedRawChatAdapter) ConversionRuntime() convert.Runtime {
	return convert.DefaultRuntime()
}

// MessagesEffortValue reconciles explicit Anthropic max client effort against
// the resolved catalog attributes on p.provider, keeping "max" when supported
// or unknown and downgrading converted "max" to "xhigh" when unsupported.
// Other client effort values return convertedEffort unchanged without reading
// catalog attributes.
func (p *openAIConvertedRawChatAdapter) MessagesEffortValue(clientEffort, model, convertedEffort string) string {
	return gatewayprovider.OpenAICompatAnthropicReasoningEffortValue(
		clientEffort,
		model,
		convertedEffort,
		gatewayprovider.ExecutionModelAttributes(p.provider, model),
	)
}

// ApplySourceFast applies provider fast-tier policy to outgoing Chat request
// bytes and formats any BlockedError in source's client protocol envelope.
// Callers abort request forwarding whenever a non-nil policy error is returned
// so rejected service tiers never reach the upstream endpoint.
func (p *openAIConvertedRawChatAdapter) ApplySourceFast(ctx context.Context, source protocol.ProtocolID, model string, body []byte) ([]byte, error) {
	if source == protocol.ProtocolAnthropicMessages {
		return p.ApplyFast(ctx, model, body)
	}
	return p.FastFallback(ctx, model, body)
}

// SourceErrorResponse formats terminal HTTP >= 400 upstream rejections in
// source's client error envelope using the replayable body installed on resp.
// Callers retain ownership of closing resp.Body after error presentation and
// observation capture complete.
func (p *openAIConvertedRawChatAdapter) SourceErrorResponse(ctx context.Context, source protocol.ProtocolID, resp *http.Response, requestBody []byte, model string) (*openaiexecution.Result, error) {
	switch source {
	case protocol.ProtocolAnthropicMessages:
		return p.AnthropicError(resp, model)
	case protocol.ProtocolOpenAIResponses:
		return p.ResponsesError(ctx, resp, requestBody, model)
	default:
		v, e := p.s.chatError(resp, p.c, p.provider, model)
		return openaiexecution.FromForwardResult(v), e
	}
}

// UpdateGrokUsage records parsed Grok OAuth quota-header snapshots on HTTP 2xx
// Chat responses and clears recoverable OAuth rate-limit state when quota
// headers are absent. Nil adapters, executors, providers, or Grok health
// services return safely without mutating provider state.
func (p *openAIConvertedRawChatAdapter) UpdateGrokUsage(ctx context.Context, model string, headers http.Header, status int) {
	if p == nil || p.openAIRawFallbackAdapter == nil || p.openAIMessagesExecutionAdapter == nil ||
		p.s == nil || p.s.Grok == nil || p.s.Grok.Health == nil || p.provider == nil {
		return
	}
	p.s.Grok.Health.ObserveResponse(ctx, p.provider.View(), headers, status, model)
}

// ConvertedRawOptions builds client-protocol-aware reader, observation, and
// terminal error callbacks for ReadRawChatConvertedStreaming and
// ReadRawChatConvertedBuffered on the active HTTP attempt and selected
// provider. Callers keep p.c and resp live until reading returns and close
// resp.Body afterward.
func (p *openAIConvertedRawChatAdapter) ConvertedRawOptions(
	resp *http.Response,
	source protocol.ProtocolID,
	original, billing, model string,
	serviceTier *string,
	requestBodyLen int,
) openai.ConvertedRawChatOptions {
	return p.s.Output.ConvertedRawOptions(p.c, resp, p.provider, source, original, billing, model, serviceTier, requestBodyLen)
}
