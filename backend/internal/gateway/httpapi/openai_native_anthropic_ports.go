package httpapi

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"

	protocolforward "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/modelidentity"
	forward "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/searchtools"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
)

// openai_native_anthropic_ports.go binds compatible Anthropic execution to
// native Messages forwarding and IR-converted OpenAI protocols.
// Native Anthropic execution uses the selected provider's protocol key, model
// policy, and proxy while the client may speak Messages, Chat, or Responses.
// Shared attempt state supplies credentials, continuity carrier unpacking,
// dialect resolution, request observations, and synchronous output until the
// runner closes the upstream response.

// Interface assertions guarantee openAINativeAnthropicAdapter satisfies both
// NativeAnthropicPorts and ConvertedAnthropicPorts contracts.
var (
	_ forward.NativeAnthropicPorts    = (*openAINativeAnthropicAdapter)(nil)
	_ forward.ConvertedAnthropicPorts = (*openAINativeAnthropicAdapter)(nil)
)

// openAINativeAnthropicAdapter binds compatible Anthropic exchange to shared
// text attempt services.
type openAINativeAnthropicAdapter struct {
	// openAIMessagesExecutionAdapter shares this attempt's provider, services and
	// HTTP state; the native runner borrows them until response handling ends.
	*openAIMessagesExecutionAdapter
	// kind selects the client error format. The native entry constructor supplies
	// Messages, Chat, or Responses for the current call.
	kind forward.NativeAnthropicKind
}

// errorWriter selects the client rejection writer from the native execution
// kind. NativeResponses uses Responses terminal handling, NativeChat uses Chat
// JSON, and other kinds use Anthropic output; the caller supplies the current
// live context.
func (p *openAINativeAnthropicAdapter) errorWriter() func(*gin.Context, int, string, string) {
	switch p.kind {
	case forward.NativeResponses:
		return writeTextResponsesError
	case forward.NativeChat:
		return WriteForwardChatError
	default:
		return WriteForwardAnthropicError
	}
}

// Profile describes the selected provider for the native Anthropic runner.
func (p *openAINativeAnthropicAdapter) Profile() forward.MessagesProfile {
	return forward.MessagesProfile{Profile: openAIForwardProfile(p.provider), ID: p.provider.Record.ID}
}

// Error writes the rejection through the current native client protocol.
func (p *openAINativeAnthropicAdapter) Error(status int, kind, message string) {
	p.errorWriter()(p.c, status, kind, message)
}

// PrepareModel delegates attribute-sensitive body preparation for the
// Anthropic Messages protocol. The selected model and provider guide the
// shared attribute service; preparation errors return before the native runner
// constructs or sends its request.
func (p *openAINativeAnthropicAdapter) PrepareModel(ctx context.Context, body []byte, model string) ([]byte, error) {
	return gatewayprovider.PrepareModelAttributeParameters(ctx, p.provider, model, body, p.s.modelAttributes(), "anthropic_messages")
}

// ThinkingFallback returns supplied effort or the model's enabled-thinking
// display default. The provider helper retains a present pointer, including
// empty text; a derived value affects usage attribution rather than rewriting
// this body.
func (p *openAINativeAnthropicAdapter) ThinkingFallback(effort *string, body []byte, model string) *string {
	return gatewayprovider.ApplyThinkingEnabledFallback(effort, body, model)
}

// StripEmpty delegates removal of empty Anthropic text blocks from request
// bytes.
func (p *openAINativeAnthropicAdapter) StripEmpty(body []byte) []byte {
	return protocolanthropic.StripEmptyTextBlocks(body)
}

// FilterSearch delegates search-history cleanup using the mapped model's
// passback requirement. Passback-required models select stripAll for supported
// upstream and emulated search blocks; other models select the filter's
// synthetic-block-only rule, with failures handled by that helper.
func (p *openAINativeAnthropicAdapter) FilterSearch(body []byte, model string) []byte {
	return searchtools.FilterWebSearchHistoryBlocks(body, modelidentity.ResolveThinkingProtocol(model) == modelidentity.ThinkingProtocolPassbackRequired)
}

// CacheLimit delegates Anthropic cache-control cleanup and breakpoint limiting
// for the body. The upstream helper removes invalid thinking breakpoints and
// excess tool, message and system anchors in its defined order before native
// execution.
func (p *openAINativeAnthropicAdapter) CacheLimit(body []byte) []byte {
	return anthropic.EnforceCacheControlLimit(body)
}

// CacheTTL rewrites existing ephemeral cache markers to a 1h lifetime when the
// authenticated API Key enables forced Claude 1h caching for the selected
// upstream model. Ineligible platforms and passback models keep their input bytes.
func (p *openAINativeAnthropicAdapter) CacheTTL(ctx context.Context, body []byte, model string) []byte {
	if !gatewayprovider.ShouldForceClaude1hCache(ctx, p.provider, model) {
		return body
	}
	return anthropic.InjectAnthropicCacheControlTTL1h(body)
}

// Log writes native forwarding diagnostics under the gateway logging
// component.
func (p *openAINativeAnthropicAdapter) Log(format string, args ...any) {
	logging.LegacyPrintf("service.gateway", format, args...)
}

// ProtocolAPIKey reads the provider's configured Anthropic-compatible protocol
// credential for execution.
func (p *openAINativeAnthropicAdapter) ProtocolAPIKey() string {
	return p.provider.View().GetOpenAIProtocolAPIKey()
}

// TargetURL resolves the native Anthropic endpoint through the configured
// request builder.
func (p *openAINativeAnthropicAdapter) TargetURL() (string, error) {
	return p.s.Requests.AnthropicURL(p.provider)
}

// StreamContext removes client cancellation and deadlines for streaming
// requests while retaining values and any server-abort signal. Nonstreaming
// input stays attached, and nil input becomes Background. The returned
// callback is a no-op that the runner invokes after request construction.
func (p *openAINativeAnthropicAdapter) StreamContext(ctx context.Context, stream bool) (context.Context, context.CancelFunc) {
	return gatewayprovider.DetachStreamUpstreamContext(ctx, stream)
}

// BuildNative constructs the native Anthropic HTTP request using this
// attempt's request headers and provider. It returns the request or
// construction error; the runner supplies the derived upstream context and
// retains response cleanup after sending.
func (p *openAINativeAnthropicAdapter) BuildNative(ctx context.Context, body []byte, key, url string) (*http.Request, error) {
	r, _, err := p.s.Requests.BuildAnthropic(ctx, p.c, p.provider, body, key, url)
	return r, err
}

// SendNative clears request model observation before sending the prepared
// native request through the selected proxy. A successful response belongs to
// the runner, which closes its body after native reading or HTTP-error
// handling finishes.
func (p *openAINativeAnthropicAdapter) SendNative(r *http.Request) (*http.Response, error) {
	resetResponseModel(p.c)
	return p.s.Requests.Transport.Do(r, p.proxyURL, p.provider.Record.ID, p.provider.Record.Concurrency)
}

// TransportErrorNative delegates native exchange failure observation with the
// passthrough Ops marker. The shared handler preserves client cancellation and
// otherwise returns a sanitized provider failover signal; the outer text loop
// decides whether another attempt is allowed.
func (p *openAINativeAnthropicAdapter) TransportErrorNative(ctx context.Context, err error) error {
	return p.s.Requests.Failure.Handle(ctx, p.c, p.provider, err, true)
}

// DirectOptions binds native Messages output to this request and selected
// provider.
func (p *openAINativeAnthropicAdapter) DirectOptions() forward.NativeAnthropicOptions {
	return p.s.Output.AnthropicDirectOptions(p.c, p.provider)
}

// ResponsesEffort extracts and normalizes recorded effort from a
// Responses-shaped body.
func (p *openAINativeAnthropicAdapter) ResponsesEffort(body []byte, models ...string) *string {
	return protocolforward.ExtractEffort(body, false, capability.NormalizeRecordedOpenAIEffortForModel, models...)
}

// ChatEffort extracts and normalizes recorded effort from a Chat-shaped body.
func (p *openAINativeAnthropicAdapter) ChatEffort(body []byte, models ...string) *string {
	return protocolforward.ExtractEffort(body, true, capability.NormalizeRecordedOpenAIEffortForModel, models...)
}

// MapStatus delegates upstream-to-client status mapping for native protocol
// conversion output.
func (p *openAINativeAnthropicAdapter) MapStatus(status int) int {
	return protocolforward.MapStatus(status)
}

// PrepareNativeMessagesReplay unpacks matching trc1 continuity envelopes or
// strips incompatible blocks on compatible Anthropic endpoints. Nil adapters
// and carrier-free payloads return body unchanged without resolving model
// catalog attributes.
func (p *openAINativeAnthropicAdapter) PrepareNativeMessagesReplay(ctx context.Context, model string, body []byte) ([]byte, error) {
	if p == nil {
		return body, nil
	}
	if ctx == nil {
		ctx = context.Background()
		if p.c != nil && p.c.Request != nil {
			ctx = p.c.Request.Context()
		}
	}
	rewritten, _, err := gatewayprovider.PrepareNativeReplay(
		ctx,
		p.provider,
		model,
		body,
		p.s.modelAttributes(),
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolAnthropicMessages,
	)
	return rewritten, err
}

// ResolveAnthropicDialect resolves catalog model attributes and execution rules
// from raw client bytes and builds the target Anthropic Messages convert.Dialect
// alongside passback mode. Nil contexts fall back to p.c.Request.Context() or
// context.Background(), and successful lookups publish p.provider.Capabilities.
func (p *openAINativeAnthropicAdapter) ResolveAnthropicDialect(ctx context.Context, model string, body []byte) (convert.Dialect, bool, error) {
	if ctx == nil {
		ctx = context.Background()
		if p.c != nil && p.c.Request != nil {
			ctx = p.c.Request.Context()
		}
	}
	res, err := p.modelResolutionFromBytes(ctx, model, body, "anthropic_messages")
	if err != nil {
		return convert.Dialect{}, false, err
	}
	return gatewayprovider.ConversionDialect(ctx, p.provider, res, protocol.ProtocolAnthropicMessages), res.Rules.SignatureMode == "passback", nil
}

// ConversionRuntime returns the default wall-clock and crypto/rand ID runtime
// used by convert.NewExchange.
func (*openAINativeAnthropicAdapter) ConversionRuntime() convert.Runtime {
	return convert.DefaultRuntime()
}

// SourceError writes a client-protocol rejection on p.c, promoting to an
// untimed terminal SSE error when waiting heartbeats already committed 200 SSE.
// Uncommitted responses mark semantic commitment before writing JSON, and nil
// Gin contexts return immediately without side effects.
func (p *openAINativeAnthropicAdapter) SourceError(source protocol.ProtocolID, status int, kind, message string) {
	if p.c == nil {
		return
	}
	if CommittedSSE(p.c) {
		switch {
		case source == protocol.ProtocolAnthropicMessages:
			DefaultOpenAIErrorOutput().WriteAnthropicStreamingError(p.c, status, kind, message, true)
		case source == protocol.ProtocolOpenAIResponses || InboundIsResponses(p.c):
			if WriteResponsesFailedSSE(p.c, kind, "", message, ErrorRequestID(p.c), ErrorRequestModel(p.c)) {
				return
			}
			DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(p.c, status, kind, "", message, true, false)
		default:
			DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(p.c, status, kind, "", message, true, false)
		}
		return
	}
	MarkResponseCommitted(p.c)
	switch source {
	case protocol.ProtocolOpenAIResponses:
		writeTextResponsesError(p.c, status, kind, message)
	case protocol.ProtocolAnthropicMessages:
		WriteForwardAnthropicError(p.c, status, kind, message)
	default:
		WriteForwardChatError(p.c, status, kind, message)
	}
}

// Mimic returns body unchanged on OpenAI-compatible Anthropic endpoints because
// compatible targets authenticate via protocol API keys rather than Claude Code
// OAuth sessions. Callers still pass the returned slice into CacheLimit.
func (*openAINativeAnthropicAdapter) Mimic(_ context.Context, body []byte, _ json.RawMessage, _ string) []byte {
	return body
}

// BuildConvertedNative constructs an authenticated Anthropic Messages HTTP
// request and returns both the request and its adopted sanitized wire body.
// Attribute or header-policy errors return before SendNative is invoked.
func (p *openAINativeAnthropicAdapter) BuildConvertedNative(ctx context.Context, body []byte, key, url string) (*http.Request, []byte, error) {
	return p.s.Requests.BuildAnthropic(ctx, p.c, p.provider, body, key, url)
}

// IsSignatureError reports whether captured HTTP 400 bytes match an Anthropic
// thinking-block signature validation failure eligible for lossless stripping
// retry. Non-empty matcher diagnostics are logged under service.gateway.
func (*openAINativeAnthropicAdapter) IsSignatureError(_ context.Context, body []byte) bool {
	matched, diagnostic := anthropic.IsThinkingBlockSignatureError(upstream.ExtractErrorMessage(body))
	if diagnostic != "" {
		logging.LegacyPrintf("service.gateway", "%s", diagnostic)
	}
	return matched
}

// ObserveSignatureRetry records an Ops signature_error event on p.c before
// ForwardConvertedAnthropic re-dispatches the stripped-signature retry. Nil Gin
// contexts or providers return without recording telemetry, and body excerpts
// obey LogUpstreamErrorBodyMaxBytes when body logging is enabled.
func (p *openAINativeAnthropicAdapter) ObserveSignatureRetry(resp *http.Response, body []byte, msg string) {
	if p.c == nil || p.provider == nil {
		return
	}
	var status int
	var reqID string
	if resp != nil {
		status = resp.StatusCode
		reqID = resp.Header.Get("x-request-id")
	}
	detail := ""
	if p.s != nil && p.s.Output != nil && p.s.Output.Options.LogUpstreamErrorBody {
		maxBytes := p.s.Output.Options.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		detail = logredact.TruncateUTF8(string(body), maxBytes)
	}
	AppendOpsUpstreamError(p.c, ops.OpsUpstreamErrorEvent{
		Platform:           p.provider.Record.Platform,
		ProviderID:         p.provider.Record.ID,
		ProviderName:       p.provider.Record.Name,
		UpstreamStatusCode: status,
		UpstreamRequestID:  reqID,
		Kind:               "signature_error",
		Message:            msg,
		Detail:             detail,
	})
}

// ConvertedOutputOptions binds merged Anthropic conversion readers and
// single-pass stream failure classification to this attempt's HTTP context.
// Callers keep p.c, p.provider, and resp alive until reading completes.
func (p *openAINativeAnthropicAdapter) ConvertedOutputOptions(
	resp *http.Response,
	source protocol.ProtocolID,
	original, billing, model string,
	requestBodyLen int,
) forward.ConvertedAnthropicOutputOptions {
	return p.s.Output.ConvertedAnthropicOptions(p.c, p.provider, resp, source, original, billing, model, requestBodyLen)
}

// writeTextResponsesError marks terminal output committed and writes a
// Responses error as JSON, or emits an untimed streaming error frame when SSE
// headers are already committed. Nil Gin contexts are ignored.
func writeTextResponsesError(c *gin.Context, status int, code, message string) {
	if c == nil {
		return
	}
	MarkResponseCommitted(c)
	if CommittedSSE(c) {
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, code, code, message, true, false)
		return
	}
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": message}})
}
