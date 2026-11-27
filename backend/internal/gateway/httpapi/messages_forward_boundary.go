package httpapi

import (
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	egressprovider "github.com/TokenFlux/TokenRouter/internal/egress/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/errorpolicy"
	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/messageforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/searchtools"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// messages_forward_boundary.go supplies HTTP output, headers, and diagnostics
// to native and converted Messages forwarding.
// Messages forwarding can expose native Anthropic output or convert it into
// Chat and Responses envelopes. The HTTP boundary supplies the current writer,
// request headers and diagnostic state to the shared runtime, while retaining
// distinct header defaults for native passthrough and converted output.

// Interface assertions keep the request-local adapter compatible with both
// native and converted HTTP operations required by Messages runtime forwarding.
var (
	_ messageforward.HTTPBoundary          = (*MessageForwardBoundary)(nil)
	_ messageforward.ConvertedHTTPBoundary = (*MessageForwardBoundary)(nil)
)

// MessageForwardBoundary adapts one Gin request to Messages forwarding output.
type MessageForwardBoundary struct {
	// context retains the active request and writer; callers keep it usable until
	// synchronous forwarding and any returned conversion output have finished.
	context *gin.Context
	// filter is shared across outputs. Nil uses native passthrough restrictions
	// in WriteAnthropicPassthroughHeaders and shared defaults for converted headers.
	filter *egress.CompiledHeaderFilter
}

// WriteAnthropicPassthroughHeaders copies allowed upstream response headers
// into dst. Nil source or destination does nothing; a supplied filter owns
// selection, while nil copies only trimmed Content-Type and x-request-id
// values.
func WriteAnthropicPassthroughHeaders(dst, src http.Header, filter *egress.CompiledHeaderFilter) {
	if dst == nil || src == nil {
		return
	}
	if filter != nil {
		egressprovider.WriteFilteredHeaders(dst, src, filter)
		return
	}
	// Native passthrough without configured filtering retains only protocol type
	// and upstream correlation identity, rather than applying the converted-output defaults.
	if value := strings.TrimSpace(src.Get("Content-Type")); value != "" {
		dst.Set("Content-Type", value)
	}
	if value := strings.TrimSpace(src.Get("x-request-id")); value != "" {
		dst.Set("x-request-id", value)
	}
}

// NewMessageForwardBoundary retains the active Gin context and optional shared
// header filter. The context and writer must remain usable throughout
// synchronous runtime forwarding; a nil filter selects the boundary’s
// protocol-specific defaults.
func NewMessageForwardBoundary(c *gin.Context, filter *egress.CompiledHeaderFilter) *MessageForwardBoundary {
	return &MessageForwardBoundary{context: c, filter: filter}
}

// Present reports whether the boundary retains a Gin context. A nil receiver
// returns false, allowing callers to test the optional HTTP adapter before
// using methods that require request output.
func (b *MessageForwardBoundary) Present() bool { return b != nil && b.context != nil }

// RequestPresent reports whether a boundary and its HTTP request both exist. A
// missing boundary or request returns false; methods needing request headers
// can use this guard before accessing the live request.
func (b *MessageForwardBoundary) RequestPresent() bool {
	return b.Present() && b.context.Request != nil
}

// HasHeaderFilter reports whether this boundary has a configured header
// filter. The receiver must exist; false selects the distinct passthrough or
// converted defaults applied by the boundary’s header-writing methods.
func (b *MessageForwardBoundary) HasHeaderFilter() bool { return b.filter != nil }

// RequestHeaders returns the live request header map when a request exists.
// Otherwise it returns a new empty map; callers mutating a live result also
// mutate headers seen by subsequent request consumers.
func (b *MessageForwardBoundary) RequestHeaders() http.Header {
	if !b.RequestPresent() {
		return http.Header{}
	}
	return b.context.Request.Header
}

// Sink returns an output sink bound to the current HTTP writer. Callers
// require a usable context and writer, and retain this request-local sink only
// for the forwarding operation that owns them.
func (b *MessageForwardBoundary) Sink() upstream.OutputSink {
	return NewResponseSink(b.context)
}

// SearchOutput returns search-tool output bound to the current Gin request.
// Its consumers use the same context and writer as forwarding, so callers keep
// that request usable during synchronous search output.
func (b *MessageForwardBoundary) SearchOutput() searchtools.Output {
	return SearchOutput{Context: b.context}
}

// ReadResponseBody delegates bounded upstream reading with an error writer
// selected by body kind. CountBody uses a Count Tokens envelope; other kinds
// use Anthropic output, and the caller retains ownership of closing the
// reader.
func (b *MessageForwardBoundary) ReadResponseBody(reader io.Reader, limit int64, kind messageforward.BodyKind) ([]byte, error) {
	tooLarge := AnthropicResponseTooLarge
	if kind == messageforward.CountBody {
		tooLarge = func(c *gin.Context) {
			WriteForwardCountError(c, http.StatusBadGateway, "upstream_error", "Upstream response too large")
		}
	}
	return ReadUpstreamResponseBody(reader, limit, b.context, tooLarge)
}

// Size returns response bytes after excluding all recorded heartbeat output.
func (b *MessageForwardBoundary) Size() int { return HeartbeatAdjustedWrittenSize(b.context) }

// Written reports whether the current HTTP writer has written headers.
func (b *MessageForwardBoundary) Written() bool { return b.context.Writer.Written() }

// Commit marks request output as semantically committed for outer fallback
// handling. The marker is independent of physical bytes and is set when the
// invoking output path decides another response should be suppressed.
func (b *MessageForwardBoundary) Commit() { MarkResponseCommitted(b.context) }

// GenericError delegates the generic Messages failure to the shared writer.
// That writer marks semantic commitment before writing its HTTP 500 envelope,
// allowing outer fallback handling to recognize this response.
func (b *MessageForwardBoundary) GenericError() {
	WriteForwardMessageGenericError(b.context, b.Commit)
}

// MessageError writes the supplied Anthropic error through this request, using
// JSON before stream commitment or an Anthropic SSE error frame when
// event-stream framing is already committed. It does not mark semantic
// commitment; runtime callers coordinate that marker separately.
func (b *MessageForwardBoundary) MessageError(status int, kind, message string) {
	(AnthropicForwardErrorOutput{Context: b.context}).Message(status, kind, message)
}

// RawError writes supplied bytes as application/json with the selected status,
// or extracts safe error fields into an Anthropic SSE error frame when
// event-stream framing is already committed. Uncommitted bytes pass through
// unvalidated, and runtime callers own the semantic commit marker.
func (b *MessageForwardBoundary) RawError(status int, body []byte) {
	(AnthropicForwardErrorOutput{Context: b.context}).Raw(status, body)
}

// CountError writes the supplied Count Tokens error through shared
// presentation. It leaves semantic commitment to runtime orchestration, which
// decides whether another response or fallback can still be presented.
func (b *MessageForwardBoundary) CountError(status int, kind, message string) {
	WriteForwardCountError(b.context, status, kind, message)
}

// CountSuccess writes Count Tokens bytes with the upstream status and headers.
// Passthrough first applies this boundary’s header policy and retains the
// upstream content type when available; ordinary output uses the shared JSON
// response path.
func (b *MessageForwardBoundary) CountSuccess(status int, headers map[string][]string, body []byte, passthrough bool) {
	if passthrough {
		WriteAnthropicPassthroughHeaders(b.context.Writer.Header(), headers, b.filter)
	}
	WriteForwardCountSuccess(b.context, status, headers, body, passthrough)
}

// MarkPassthrough records native Anthropic passthrough on the current request.
// A missing context does nothing; later observation reads this flag to
// distinguish native response handling from converted output.
func (b *MessageForwardBoundary) MarkPassthrough() {
	if b.Present() {
		b.context.Set("anthropic_passthrough", true)
	}
}

// ServiceTier returns the upstream service tier selected by this request's
// response observer. Terminal declarations take precedence; missing evidence
// or conflicting nonterminal tiers return empty so completion can interpret
// the absent observation.
func (b *MessageForwardBoundary) ServiceTier() string {
	return ObservedUpstreamResponseServiceTier(b.context)
}

// SetError stores the upstream status, message and detail for Ops.
func (b *MessageForwardBoundary) SetError(status int, message, detail string) {
	SetOpsUpstreamError(b.context, status, message, detail)
}

// Observe appends one supplied upstream-error notice to request-local Ops
// history.
func (b *MessageForwardBoundary) Observe(notice forward.Notice) {
	AppendOpsUpstreamError(b.context, ops.OpsUpstreamErrorEvent{
		UpstreamURL: notice.UpstreamURL, Passthrough: notice.Passthrough,
		Platform: notice.Platform, ProviderID: notice.ProviderID, ProviderName: notice.ProviderName,
		UpstreamStatusCode: notice.UpstreamStatusCode, UpstreamRequestID: notice.UpstreamRequestID,
		Kind: notice.Kind, Message: notice.Message, Detail: notice.Detail,
	})
}

// MatchRule queries the error rule service bound to this request. An absent
// service returns nil, letting runtime forwarding continue its default error
// classification and presentation for the supplied platform and body.
func (b *MessageForwardBoundary) MatchRule(platform string, status int, body []byte) *errorpolicy.ErrorPassthroughRule {
	rules := BoundErrorPassthroughService(b.context)
	if rules == nil {
		return nil
	}
	return rules.MatchRule(platform, status, body)
}

// SkipMonitoring marks this request to suppress passthrough error monitoring.
func (b *MessageForwardBoundary) SkipMonitoring() {
	MarkOpsSkipMonitoring(b.context)
}

// WriteHeaders copies upstream response headers under the selected output
// policy. Native passthrough uses its restricted nil-filter defaults;
// converted output delegates to shared filtering, where nil has the shared
// policy meaning.
func (b *MessageForwardBoundary) WriteHeaders(dst, src http.Header, passthrough bool) {
	if passthrough {
		WriteAnthropicPassthroughHeaders(dst, src, b.filter)
		return
	}
	egressprovider.WriteFilteredHeaders(dst, src, b.filter)
}

// CommittedSSE reports whether downstream HTTP 200 text/event-stream headers
// have already been committed on b.context, including by waiting heartbeats.
// Nil boundaries or absent Gin contexts return false.
func (b *MessageForwardBoundary) CommittedSSE() bool {
	return b.Present() && CommittedSSE(b.context)
}

// WriteConvertedError writes an uncommitted client-protocol HTTP JSON error on
// b.context or promotes to an untimed terminal SSE error when CommittedSSE is
// true. Uncommitted responses mark semantic commitment before writing JSON, and
// nil boundaries return without side effects.
func (b *MessageForwardBoundary) WriteConvertedError(source protocol.ProtocolID, originalModel string, status int, errType, code, message string) {
	if !b.Present() {
		return
	}
	if CommittedSSE(b.context) {
		b.WriteLocalTerminalError(source, originalModel, status, errType, code, message)
		return
	}
	MarkResponseCommitted(b.context)
	if source == protocol.ProtocolAnthropicMessages {
		WriteForwardAnthropicError(b.context, status, errType, message)
		return
	}
	DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(b.context, status, errType, code, message, false, false)
}

// WriteLocalTerminalError writes an untimed client-protocol terminal SSE error
// onto b.context, emitting synthetic response.failed frames on Responses
// streams. Nil boundaries return without side effects, and failed synthetic
// Responses writes fall back to generic OpenAI SSE error frames.
func (b *MessageForwardBoundary) WriteLocalTerminalError(source protocol.ProtocolID, originalModel string, status int, errType, code, message string) {
	if !b.Present() {
		return
	}
	switch {
	case source == protocol.ProtocolAnthropicMessages:
		DefaultOpenAIErrorOutput().WriteAnthropicStreamingError(b.context, status, errType, message, true)
	case source == protocol.ProtocolOpenAIResponses || InboundIsResponses(b.context):
		if WriteResponsesFailedSSE(b.context, errType, code, message, ErrorRequestID(b.context), firstNonBlankModel(originalModel, ErrorRequestModel(b.context))) {
			return
		}
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(b.context, status, errType, code, message, true, false)
	default:
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(b.context, status, errType, code, message, true, false)
	}
}

// WriteRawTerminalSSE delivers pre-encoded terminal SSE frame bytes through the
// untimed terminal barrier on b.context and reports whether delivery succeeded.
// Nil boundaries return false so callers fall back to WriteLocalTerminalError.
func (b *MessageForwardBoundary) WriteRawTerminalSSE(data []byte) bool {
	if !b.Present() {
		return false
	}
	return emitLocalTerminalSSE(b.context, data)
}

// WriteForwardMessageGenericError marks a handled Messages failure before
// writing generic 500 output, switching to a terminal SSE error frame when
// event-stream framing is already committed. The callback and Gin context are
// required; marking precedes delivery so outer error handling can see that this
// path has attempted its response.
func WriteForwardMessageGenericError(c *gin.Context, commit func()) {
	commit()
	WriteAnthropicStreamError(c, http.StatusInternalServerError, "upstream_error", "", "Upstream gateway error", CommittedSSE(c), MarkOpsStreamError)
}

// WriteForwardCountSuccess writes counted output with the supplied status and
// body. Passthrough uses a nonblank trimmed upstream Content-Type; other cases
// select application/json, allowing ordinary count results to use their
// expected decoder.
func WriteForwardCountSuccess(c *gin.Context, status int, headers map[string][]string, body []byte, passthrough bool) {
	contentType := "application/json"
	if passthrough {
		if value := strings.TrimSpace(http.Header(headers).Get("Content-Type")); value != "" {
			contentType = value
		}
	}
	c.Data(status, contentType, body)
}

// WriteForwardCountError writes an Anthropic count failure with the supplied
// status. The caller owns separate handled-output marking; this helper emits
// the protocol envelope using the provided error kind and message.
func WriteForwardCountError(c *gin.Context, status int, kind, message string) {
	c.JSON(status, gin.H{"type": "error", "error": gin.H{"type": kind, "message": message}})
}
