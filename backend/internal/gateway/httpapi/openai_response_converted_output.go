package httpapi

import (
	"bufio"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	egressadapter "github.com/TokenFlux/TokenRouter/internal/egress/provider"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/moderationflow"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	gatewaytelemetry "github.com/TokenFlux/TokenRouter/internal/gateway/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/grok"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// openai_response_converted_output.go binds merged IR Responses readers
// (ReadConvertedStreaming and ReadConvertedBuffered) and shared converted
// stream failure classification to HTTP failure policy, untimed terminal SSE
// error delivery, and request-local telemetry. Converted streams can commit
// HTTP 200 text/event-stream via waiting keepalive heartbeats before upstream
// semantic tokens arrive, or fail mid-stream after client blocks open.
//
// ConvertedOptions routes pre-output errors through client-protocol JSON or
// untimed terminal SSE writers (emitting synthetic response.failed on Responses
// clients, event: error on Messages clients, and OpenAI SSE error frames on
// Chat clients) while routing post-output stream.Close terminal frames through
// emitLocalTerminalSSE so gateway errors never record upstream TTFT or trigger
// duplicate fallback error envelopes.

// convertedStreamFailure classifies a converted Responses or Chat stream
// failure event for any inbound client protocol by combining provider health
// policy with event-level failover predicates. Provider switching is permitted
// only before client content starts so partial downstream streams never
// interleave two providers, and nil providers default error-rule matching to
// the OpenAI platform.
//
// Once failover is declined or output has begun, Ops records the sanitized
// failure and custom error presentation rules apply unless health policy
// selected a generic internal error. Matching rules mark the response committed
// and return the client status, type, code, and message for terminal delivery.
func (p *OpenAIResponseOutput) convertedStreamFailure(
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	resp *http.Response,
	source protocol.ProtocolID,
	requestID, upstreamModel string,
	payloadBytes []byte,
	message string,
	clientOutputStarted, isBareErrorEvent bool,
) openai.ConvertedFailure {
	policyStatus, decision := p.ApplyStreamFailurePolicy(
		c.Request.Context(), provider, upstreamModel, resp.Header, payloadBytes, message,
	)
	policyGeneric := decision.ShouldReturnGenericError()
	// Select the bare-error or response.failed failover predicate before combining
	// it with provider error policy and downstream output state.
	shouldFailoverSignal := openai.OpenAIStreamFailedEventShouldFailover(payloadBytes, message)
	if isBareErrorEvent {
		shouldFailoverSignal = openai.OpenAIStreamErrorEventShouldFailover(payloadBytes, message)
	}
	if !clientOutputStarted && decision.ShouldFailover(gatewayprovider.ExecutionErrorPolicy(provider), policyStatus, shouldFailoverSignal) {
		failure := p.NewStreamPolicyFailureWithModel(
			c, provider, false, requestID, resp.Header, payloadBytes, message,
			&AppliedOpenAIResponseEffects{StatusCode: policyStatus, StopScheduling: decision.StopScheduling}, upstreamModel,
		)
		return openai.ConvertedFailure{Failover: failure}
	}

	message = p.RecordStreamError(c, provider, false, requestID, "http_error", payloadBytes, message)
	defaultErrType := "upstream_error"
	if source == protocol.ProtocolAnthropicMessages {
		defaultErrType = "api_error"
	}
	errStatus, errType, errMsg := http.StatusBadGateway, defaultErrType, message
	if policyGeneric {
		errStatus, errMsg = http.StatusInternalServerError, "Upstream gateway error"
	}
	errCode := strings.TrimSpace(upstream.ExtractErrorCode(payloadBytes))
	platform := capability.PlatformOpenAI
	if provider != nil {
		platform = provider.Record.Platform
	}
	if status, ruleType, ruleMsg, matched := ApplyOpenAIStreamFailedErrorRule(
		c, platform, payloadBytes, message,
	); matched && !policyGeneric {
		if ruleMsg == "" {
			ruleMsg = errMsg
		}
		errStatus, errType, errMsg = status, ruleType, ruleMsg
		MarkResponseCommitted(c)
	}
	return openai.ConvertedFailure{
		Status:  errStatus,
		Type:    errType,
		Code:    errCode,
		Message: errMsg,
	}
}

// ConvertedOptions binds client-protocol-aware reader, observation, failure,
// and untimed terminal SSE callbacks to one live HTTP attempt for
// ReadConvertedStreaming and ReadConvertedBuffered. Callers keep c, provider,
// and resp alive until the reader returns and retain ownership of closing
// resp.Body afterward.
func (p *OpenAIResponseOutput) ConvertedOptions(
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	resp *http.Response,
	source protocol.ProtocolID,
	originalModel, billingModel, upstreamModel string,
	requestBodyLen int,
) openai.ConvertedResponseOptions {
	requestID := ""
	if resp != nil {
		requestID = requeststate.FirstNonEmpty(resp.Header.Get("x-request-id"), resp.Header.Get("xai-request-id"))
	}
	defaultErrType := "upstream_error"
	writeBufferedErr := WriteForwardChatError
	if source == protocol.ProtocolAnthropicMessages {
		defaultErrType = "api_error"
		writeBufferedErr = WriteForwardAnthropicError
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

	opts := openai.ConvertedResponseOptions{
		RequestContext:       c.Request.Context(),
		RequestBodyLen:       requestBodyLen,
		CodexFailureTerminal: provider != nil && provider.View().IsOpenAIOAuthLike(),
		NativeOpenAI:         provider != nil && provider.Record.Platform == capability.PlatformOpenAI && source != protocol.ProtocolOpenAIChatCompletions,
		AppendDoneOnCyber:    source == protocol.ProtocolOpenAIChatCompletions,
		DefaultErrorType:     defaultErrType,
		Scanner:              func(r io.Reader) *bufio.Scanner { return p.Scanner(r) },
		StreamInterval: func() time.Duration {
			if p.Options.Configured && p.Options.StreamDataIntervalTimeout > 0 {
				// Configured stream idle intervals are stored in whole seconds.
				return time.Duration(p.Options.StreamDataIntervalTimeout) * time.Second
			}
			return 0
		},
		RestoreToolNames: func(body []byte) []byte { return RestoreCodexToolNamesFromContext(c, body) },
		Observe:          func(body []byte, event string) { ObserveOpenAIServiceTierInContext(c, body, event) },
		ObserveFinal: func(final *wire.ResponsesResponse) {
			if final == nil {
				return
			}
			observer := UpstreamResponseModelObserverFromContext(c)
			if observer == nil {
				observer = BeginUpstreamResponseModelObservation(c)
			}
			observer.Observe(final.Model, true)
			observer.ObserveServiceTier(final.ServiceTier, true)
		},
		ServiceTier: func() string { return ObservedUpstreamResponseServiceTier(c) },
		Headers: func(dst, src http.Header) {
			if p.Headers != nil {
				egressadapter.WriteFilteredHeaders(dst, src, p.Headers)
			}
		},
		CountSearch:             provider != nil && provider.View().IsGrok(),
		JSONSearch:              grok.CountGrokNativeSearchCallsFromJSONBytes,
		StreamSearch:            grok.CountGrokNativeSearchCallsInSSEDataDedup,
		CommittedSSE:            func() bool { return CommittedSSE(c) },
		WriteError:              writeUnstarted,
		WriteLocalTerminalError: writeLocalTerminal,
		WriteRawTerminalSSE:     func(data []byte) bool { return emitLocalTerminalSSE(c, data) },
		StreamFailure: func(body []byte, message string, started, bare bool) openai.ConvertedFailure {
			return p.convertedStreamFailure(c, provider, resp, source, requestID, upstreamModel, body, message, started, bare)
		},
		TerminalSideEffects: func(body []byte, message string, headers http.Header, model string) {
			p.TerminalProviderEffects(c, provider, body, message, headers, model)
		},
		BufferedFailure: func(final *wire.ResponsesResponse, usage wire.ForwardUsage) error {
			return p.bufferedResponseFailure(
				c, provider, resp, requestID, upstreamModel, final, usage,
				defaultErrType, writeBufferedErr,
			)
		},
		BufferedReadFailure: func(err error) error {
			if source == protocol.ProtocolAnthropicMessages {
				if readErr, ok := err.(*openai.CompatBufferedReadError); ok && readErr != nil {
					return readErr.Unwrap()
				}
				return err
			}
			return p.BufferedReadFailure(c, provider, resp, requestID, err)
		},
		ObserveTerminalUsage: func(final *wire.ResponsesResponse, usage *wire.ForwardUsage, terminalEvent string, clientDisconnected bool) error {
			if source == protocol.ProtocolAnthropicMessages {
				if resp != nil && usage != nil {
					var id int64
					if provider != nil {
						id = provider.Record.ID
					}
					gatewaytelemetry.SuccessMissingUsage(c.Request.Context(), id, resp.StatusCode, usage, terminalEvent, clientDisconnected)
				}
				return nil
			}
			if source == protocol.ProtocolOpenAIChatCompletions && final != nil && usage != nil {
				// Enforce billable Grok Chat usage presence on buffered Chat conversions.
				if !c.Writer.Written() && gatewayprovider.RequiresBillableGrokChatUsage(provider, billingModel, upstreamModel, final.Model) && !gatewayprovider.HasBillableGrokChatUsage(*usage) {
					return NewGrokMissingUsageFailure(c, provider, requeststate.FirstNonEmpty(requestID, resp.Header.Get("xai-request-id")))
				}
			}
			return nil
		},
		EmptyCompleted: func(reqID string) error {
			return NewOpenAIResponsesEmptyCompletedFailoverError(c, ExecutionErrorProvider(provider), reqID)
		},
		RecordMissingTerminal: func(message string) {
			if source == protocol.ProtocolAnthropicMessages {
				p.RecordMessagesStreamError(c, provider, requestID, "stream_missing_terminal", message)
			}
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

	if source == protocol.ProtocolAnthropicMessages {
		opts.MissingTerminal = func(message string) error {
			return p.NewStreamFailure(c, provider, false, requestID, nil, message)
		}
	}
	if source == protocol.ProtocolOpenAIChatCompletions {
		opts.SilentRefusal = func() error {
			return NewOpenAISilentRefusalFailoverError(c, ExecutionErrorProvider(provider), requestID)
		}
	}
	return opts
}

// firstNonBlankModel returns trimmed primary when non-blank and falls back to
// fallback otherwise. Blank primary strings therefore reuse request-context
// model labels when synthesizing Responses failure events.
func firstNonBlankModel(primary, fallback string) string {
	if strings.TrimSpace(primary) != "" {
		return strings.TrimSpace(primary)
	}
	return strings.TrimSpace(fallback)
}

// RecordMessagesStreamError stores sanitized Messages stream failure evidence
// in request Ops. Nil context skips observation; a selected provider supplies
// its platform and identity, while missing provider metadata uses the OpenAI
// platform default.
func (p *OpenAIResponseOutput) RecordMessagesStreamError(c *gin.Context, provider *gatewayprovider.ExecutionProvider, upstreamRequestID, kind, message string) {
	if c == nil {
		return
	}
	message = logredact.SanitizeUpstreamQueries(message)
	SetOpsUpstreamError(c, http.StatusBadGateway, message, "")
	event := ops.OpsUpstreamErrorEvent{
		Platform:           capability.PlatformOpenAI,
		UpstreamStatusCode: http.StatusBadGateway,
		UpstreamRequestID:  strings.TrimSpace(upstreamRequestID),
		Kind:               kind,
		Message:            message,
	}
	if provider != nil {
		event.Platform = provider.Record.Platform
		event.ProviderID = provider.Record.ID
		event.ProviderName = provider.Record.Name
	}
	AppendOpsUpstreamError(c, event)
}
