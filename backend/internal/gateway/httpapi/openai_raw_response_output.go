package httpapi

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	egressadapter "github.com/TokenFlux/TokenRouter/internal/egress/provider"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/moderationflow"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// openai_raw_response_output.go configures native and IR-converted raw Chat
// response readers with platform repairs, failure policies, and usage callbacks.
// Raw Chat output keeps native Chat payloads while applying provider-specific
// response repairs and request observations, invoking bound callbacks for
// missing Grok usage, truncation, and silent refusal.
//
// ConvertedRawOptions binds merged IR Chat readers (ReadRawChatConvertedStreaming
// and ReadRawChatConvertedBuffered) to client-protocol JSON and untimed terminal
// SSE error writers (emitting synthetic response.failed on Responses clients and
// event: error on Messages clients) so early keepalive-committed streams and
// mid-stream failures never record upstream TTFT or append duplicate envelopes.

// RawOptions binds raw Chat reading to the active HTTP request and response.
// Callers retain c, resp and the selected tier until reading finishes and own
// response closure; a nil provider leaves ProviderID zero while retaining the
// bound callbacks.
func (p *OpenAIResponseOutput) RawOptions(c *gin.Context, resp *http.Response, provider *gatewayprovider.ExecutionProvider, billingModel, upstreamModel string, serviceTier *string, writeError func(*gin.Context, int, string, string)) openai.RawResponseOptions {
	options := openai.RawResponseOptions{
		Scanner: p.Scanner,
		ReadBody: func(r io.Reader) ([]byte, error) {
			return ReadUpstreamResponseBody(r, p.Options.ReadLimit, c, OpenAIResponseTooLarge)
		},
		BodyLimitError: httpclient.ErrResponseBodyTooLarge,
		Headers: func(dst, src http.Header) {
			if p.Headers != nil {
				egressadapter.WriteFilteredHeaders(dst, src, p.Headers)
			}
		},
		WriteError: func(status int, kind, message string) { writeError(c, status, kind, message) },
		Observe:    func(body []byte, event string) { ObserveOpenAIServiceTierInContext(c, body, event) },
		ObserveSSE: func(body string) { ObserveOpenAISSEBody(c, body) },
		TransformLine: func(line string) string {
			// The provider gate restricts Ollama repairs to recognized API-key Chat
			// targets; other providers retain the framing supplied by their upstream.
			return gatewayprovider.ApplyOllamaCloudRawChatCompletionsSSELine(provider, line)
		},
		TransformBody: func(body []byte) []byte {
			// Native JSON repairs use the same Ollama provider gate as line repairs,
			// after the reader has already observed usage from the upstream bytes.
			return gatewayprovider.ApplyOllamaCloudRawChatCompletionsResponse(provider, body)
		},
		MissingUsage: func(model string, usage wire.ForwardUsage) error {
			// The reader supplies its observed model and usage; eligible Grok billing
			// models turn missing usage into a failure even when payload forwarding succeeds.
			if gatewayprovider.RequiresBillableGrokChatUsage(provider, billingModel, upstreamModel, model) && !gatewayprovider.HasBillableGrokChatUsage(usage) {
				return NewGrokMissingUsageFailure(c, provider, requeststate.FirstNonEmpty(resp.Header.Get("x-request-id"), resp.Header.Get("xai-request-id")))
			}
			return nil
		},
		ServiceTier:         func() string { return ObservedUpstreamResponseServiceTier(c) },
		ResolvedServiceTier: func() *string { return ResolvedOpenAIUpstreamServiceTier(c, serviceTier) },
		TruncatedFailover: func(err error) error {
			return NewOpenAIRawTruncationFailure(c, provider, resp.Header.Get("x-request-id"), err)
		},
		RecordTruncation: func(err error) {
			RecordOpenAIRawTruncation(c, provider, resp.Header.Get("x-request-id"), err, "http_error")
		},
		SilentRefusal: func() error {
			return NewOpenAISilentRefusalFailoverError(c, ExecutionErrorProvider(provider), resp.Header.Get("x-request-id"))
		},
	}
	if provider != nil {
		options.ProviderID = provider.Record.ID
	}
	return options
}

// ConvertedRawOptions binds client-protocol-aware failure, truncation, silent-
// refusal, and untimed terminal SSE callbacks to one live HTTP attempt for
// ReadRawChatConvertedStreaming and ReadRawChatConvertedBuffered. Nil providers
// leave ProviderID zero while retaining bound request observations.
//
// Pre-output rejections route through client-protocol JSON writers when
// uncommitted or promote to untimed terminal SSE frames when waiting heartbeats
// already committed HTTP 200 SSE. Callers keep c, provider, and resp alive
// until reading finishes and close resp.Body afterward.
func (p *OpenAIResponseOutput) ConvertedRawOptions(
	c *gin.Context,
	resp *http.Response,
	provider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	originalModel, billingModel, upstreamModel string,
	serviceTier *string,
	requestBodyLen int,
) openai.ConvertedRawChatOptions {
	requestID := ""
	var respHeaders http.Header
	if resp != nil {
		respHeaders = resp.Header
		requestID = resp.Header.Get("x-request-id")
	}

	defaultErrType := "upstream_error"
	if source == protocol.ProtocolAnthropicMessages {
		defaultErrType = "api_error"
	}

	writeLocalTerminal := func(status int, errType, code, message string) {
		switch {
		case source == protocol.ProtocolAnthropicMessages:
			DefaultOpenAIErrorOutput().WriteAnthropicStreamingError(c, status, errType, message, true)
		case source == protocol.ProtocolOpenAIResponses || InboundIsResponses(c):
			if WriteResponsesFailedSSE(c, errType, code, message, ErrorRequestID(c), firstNonBlankModel(originalModel, ErrorRequestModel(c))) {
				return
			}
			DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, errType, code, message, true, false)
		default:
			DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, errType, code, message, true, false)
		}
	}

	writeUnstarted := func(status int, errType, code, message string) {
		if CommittedSSE(c) {
			writeLocalTerminal(status, errType, code, message)
			return
		}
		MarkResponseCommitted(c)
		if source == protocol.ProtocolAnthropicMessages {
			WriteForwardAnthropicError(c, status, errType, message)
			return
		}
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, errType, code, message, false, false)
	}

	maxFrameSize := OpenAIResponseDefaultMaxLineSize
	if p != nil && p.Options.Configured && p.Options.MaxLineSize > 0 {
		maxFrameSize = p.Options.MaxLineSize
	}

	reqCtx := context.Background()
	if c != nil && c.Request != nil {
		reqCtx = c.Request.Context()
	}

	opts := openai.ConvertedRawChatOptions{
		RequestContext:    reqCtx,
		RequestBodyLen:    requestBodyLen,
		MaxFrameBytes:     maxFrameSize,
		AppendDoneOnCyber: source == protocol.ProtocolOpenAIChatCompletions,
		DefaultErrorType:  defaultErrType,
		StreamInterval: func() time.Duration {
			if p != nil && p.Options.Configured && p.Options.StreamDataIntervalTimeout > 0 {
				// Configured stream idle intervals are stored in whole seconds, so
				// StreamInterval converts positive settings into time.Duration.
				return time.Duration(p.Options.StreamDataIntervalTimeout) * time.Second
			}
			return 0
		},
		ReadBody: func(r io.Reader) ([]byte, error) {
			return ReadUpstreamResponseBody(r, p.Options.ReadLimit, c, OpenAIResponseTooLarge)
		},
		BodyLimitError:       httpclient.ErrResponseBodyTooLarge,
		Observe:              func(body []byte, event string) { ObserveOpenAIServiceTierInContext(c, body, event) },
		ServiceTier:          func() string { return ObservedUpstreamResponseServiceTier(c) },
		ResolvedServiceTier:  func() *string { return ResolvedOpenAIUpstreamServiceTier(c, serviceTier) },
		NormalizeServiceTier: forwardcore.NormalizeObservedOpenAIServiceTier,
		Headers: func(dst, src http.Header) {
			if p != nil && p.Headers != nil {
				egressadapter.WriteFilteredHeaders(dst, src, p.Headers)
			}
		},
		MissingUsage: func(model string, usage wire.ForwardUsage) error {
			// Enforce billable Grok Chat usage presence before delivering converted
			// buffered replies so zero-usage responses fail before HTTP 200 commits.
			if gatewayprovider.RequiresBillableGrokChatUsage(provider, billingModel, upstreamModel, model) && !gatewayprovider.HasBillableGrokChatUsage(usage) {
				var xaiReqID string
				if respHeaders != nil {
					xaiReqID = respHeaders.Get("xai-request-id")
				}
				return NewGrokMissingUsageFailure(c, provider, requeststate.FirstNonEmpty(requestID, xaiReqID))
			}
			return nil
		},
		CommittedSSE:            func() bool { return CommittedSSE(c) },
		WriteError:              writeUnstarted,
		WriteLocalTerminalError: writeLocalTerminal,
		WriteRawTerminalSSE:     func(data []byte) bool { return emitLocalTerminalSSE(c, data) },
		StreamFailure: func(body []byte, message string, started bool) openai.ConvertedFailure {
			return p.convertedStreamFailure(c, provider, resp, source, requestID, upstreamModel, body, message, started, false)
		},
		TruncatedFailover: func(err error) error {
			return NewOpenAIRawTruncationFailure(c, provider, requestID, err)
		},
		RecordTruncation: func(err error) {
			RecordOpenAIRawTruncation(c, provider, requestID, err, "http_error")
		},
		SilentRefusal: func() error {
			return NewOpenAISilentRefusalFailoverError(c, ExecutionErrorProvider(provider), requestID)
		},
		ReadFailure:    openai.NewUpstreamStreamReadError,
		CyberForwarded: forwardcore.ErrCyberPolicyForwarded,
		MarkCyber: func(value openai.CyberObservation) {
			MarkOpsCyberPolicy(c, moderationflow.Mark{
				Code:           value.Code,
				Message:        value.Message,
				Body:           value.Body,
				UpstreamStatus: value.UpstreamStatus,
				UpstreamInTok:  value.UpstreamInTok,
				UpstreamOutTok: value.UpstreamOutTok,
			})
		},
		Truncate: logredact.TruncateUTF8,
	}
	if provider != nil {
		opts.ProviderID = provider.Record.ID
	}
	return opts
}
