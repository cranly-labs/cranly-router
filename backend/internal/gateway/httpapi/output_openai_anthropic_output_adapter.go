package httpapi

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	egressadapter "github.com/TokenFlux/TokenRouter/internal/egress/provider"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	forward "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
)

// output_openai_anthropic_output_adapter.go binds Anthropic response readers
// to HTTP headers, tool restoration and observations.
// Native Anthropic and converted OpenAI output share request-level header
// filtering and tool restoration. The option adapters supply synchronous HTTP
// callbacks while their platform readers own buffering, terminal detection and
// cancellation; selected provider observations remain tied to the retained
// execution target.

// AnthropicDirectOptions binds native Anthropic reading to the selected
// provider and request. ProviderID is captured immediately, while observer
// callbacks inspect the retained provider at invocation. Bounded-body reads
// leave cleanup with their caller; interval callbacks interpret configured
// seconds, and header/tool callbacks use current bound output and request
// state.
func (p *OpenAIResponseOutput) AnthropicDirectOptions(c *gin.Context, provider *gatewayprovider.ExecutionProvider) forward.NativeAnthropicOptions {
	return forward.NativeAnthropicOptions{
		ProviderID: provider.Record.ID,
		UpdateWindow: func(ctx context.Context, h http.Header) {
			if p.Observer != nil {
				gatewayprovider.ObserveExecutionSessionWindow(ctx, p.Observer, provider, h)
			}
		},
		ReadBody: func(r io.Reader) ([]byte, error) {
			return ReadUpstreamResponseBody(r, p.Options.ReadLimit, c, AnthropicResponseTooLarge)
		},
		InvalidJSON: func(ctx context.Context, r *http.Response, body []byte, err error, model string) error {
			return gatewayprovider.NonJSONUpstreamFailure(ctx, p.Observer, r, provider, body, err, model)
		},
		ForceCache: requeststate.IsForceCacheBilling, ClassifyCache: anthropic.ClassifyResponseInputAsCacheRead,
		CopyHeaders:  func(dst, src http.Header) { WriteAnthropicPassthroughHeaders(dst, src, p.Headers) },
		ReverseTools: func(body []byte) []byte { return ReverseToolNamesIfPresent(c, body) },
		MaxLineSize: func() int {
			if p.Options.Configured && p.Options.MaxLineSize > 0 {
				return p.Options.MaxLineSize
			}
			return OpenAIResponseDefaultMaxLineSize
		},
		StreamInterval: p.TextStreamInterval,
		ExtractData:    anthropic.ExtractSSEDataLine, IsTerminal: anthropic.StreamEventIsTerminal,
		Log: func(format string, args ...any) { logging.LegacyPrintf("service.gateway", format, args...) },
		HandleTimeout: func(ctx context.Context, model string) {
			if p.Observer != nil {
				p.Observer.Core.HandleStreamTimeout(ctx, gatewayprovider.ExecutionRecord(provider), model)
			}
		},
	}
}

// ConvertedAnthropicOptions binds merged IR Anthropic readers
// (ReadConvertedAnthropicStreaming and ReadConvertedAnthropicBuffered) to
// single-pass stream failure classification, OAuth tool-name restoration, and
// untimed terminal SSE error writers on one HTTP attempt. Callers keep c,
// provider, and resp alive until reading completes and close resp.Body afterward.
func (p *OpenAIResponseOutput) ConvertedAnthropicOptions(
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	resp *http.Response,
	source protocol.ProtocolID,
	originalModel, _, upstreamModel string,
	_ int,
) forward.ConvertedAnthropicOutputOptions {
	requestID := ""
	if resp != nil {
		requestID = resp.Header.Get("x-request-id")
		if requestID == "" {
			requestID = resp.Header.Get("request-id")
		}
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

	maxFrameBytes := OpenAIResponseDefaultMaxLineSize
	if p != nil && p.Options.Configured && p.Options.MaxLineSize > 0 {
		maxFrameBytes = p.Options.MaxLineSize
	}

	var providerID int64
	if provider != nil {
		providerID = provider.Record.ID
	}

	opts := forward.ConvertedAnthropicOutputOptions{
		ProviderID:       providerID,
		MaxFrameBytes:    maxFrameBytes,
		DefaultErrorType: defaultErrType,
		StreamInterval:   p.TextStreamInterval,
		UpdateWindow: func(ctx context.Context, h http.Header) {
			if p.Observer != nil {
				gatewayprovider.ObserveExecutionSessionWindow(ctx, p.Observer, provider, h)
			}
		},
		CopyHeaders: func(dst, src http.Header) {
			if p.Headers != nil {
				egressadapter.WriteFilteredHeaders(dst, src, p.Headers)
			}
		},
		RestoreToolNames:        func(body []byte) []byte { return ReverseToolNamesIfPresent(c, body) },
		CommittedSSE:            func() bool { return CommittedSSE(c) },
		WriteError:              writeUnstarted,
		WriteLocalTerminalError: writeLocalTerminal,
		WriteRawTerminalSSE:     func(data []byte) bool { return emitLocalTerminalSSE(c, data) },
		StreamFailure: func(ctx context.Context, body []byte, message string, started bool) forward.ConvertedAnthropicFailure {
			return p.convertedAnthropicStreamFailure(ctx, c, provider, resp, defaultErrType, requestID, upstreamModel, body, message, started)
		},
		PreOutputReadFailure: func(err error) error {
			return NewOpenAIRawTruncationFailure(c, provider, requestID, err)
		},
		Warn: func(msg string, fields ...zap.Field) { logging.L().Warn(msg, fields...) },
	}
	if p.Observer != nil && provider != nil {
		opts.HandleTimeout = func(ctx context.Context, model string) {
			p.Observer.Core.HandleStreamTimeout(ctx, gatewayprovider.ExecutionRecord(provider), model)
		}
	}
	return opts
}

// convertedAnthropicStreamFailure classifies an upstream Anthropic SSE error
// event on a compatible Messages endpoint by mapping nested error.type to its
// semantic HTTP status rather than using OpenAI response.failed rules. Provider
// health persistence runs only when clientOutputStarted is false so post-output
// stream errors never apply duplicate or post-commitment health penalties.
//
// Pre-semantic retryable statuses return an UpstreamFailoverError carrying the
// mapped status (including 529 for overloaded_error and 403 for permission_error),
// whereas non-failover statuses such as 400 and 404 preserve their status and
// error.type unless an administrator error passthrough rule overrides them.
func (p *OpenAIResponseOutput) convertedAnthropicStreamFailure(
	ctx context.Context,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	resp *http.Response,
	defaultErrType, requestID, upstreamModel string,
	payload []byte,
	message string,
	clientOutputStarted bool,
) forward.ConvertedAnthropicFailure {
	if ctx == nil {
		ctx = context.Background()
		if c != nil && c.Request != nil {
			ctx = c.Request.Context()
		}
	}
	rawErrType := extractAnthropicStreamErrorTypeBytes(payload)
	semanticStatus := mapAnthropicStreamErrorStatusCode(rawErrType)
	cleanMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(message))
	if cleanMsg == "" {
		cleanMsg = "Upstream stream error"
	}

	var respHeaders http.Header
	if resp != nil {
		respHeaders = resp.Header
	}

	decision := providercore.ErrorDecisionWithoutPersistence(gatewayprovider.ExecutionErrorPolicy(provider), semanticStatus)
	if !clientOutputStarted && p != nil {
		upHealth := p.Observer
		if upHealth == nil && p.Health != nil {
			upHealth = p.Health.Health
		}
		if upHealth != nil {
			decision = gatewayprovider.ApplyExecutionHealth(
				ctx,
				upHealth,
				provider,
				gatewayprovider.HealthObservationFromContext(ctx, semanticStatus, respHeaders, payload, []string{upstreamModel}),
			)
		}
	}

	detail := ""
	if len(payload) > 0 && p != nil && p.Options.LogUpstreamErrorBody {
		maxBytes := p.Options.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		// Bound logged Ops diagnostic excerpts to complete UTF-8 sequences while
		// health and error-rule matchers inspect the full captured payload slice.
		detail = logredact.TruncateUTF8(string(payload), maxBytes)
	}

	platform := capability.PlatformOpenAI
	var providerID int64
	var providerName string
	if provider != nil {
		if provider.Record.Platform != "" {
			platform = provider.Record.Platform
		}
		providerID = provider.Record.ID
		providerName = provider.Record.Name
	}

	if !clientOutputStarted && !decision.ShouldReturnGenericError() &&
		decision.ShouldFailover(gatewayprovider.ExecutionErrorPolicy(provider), semanticStatus, forwardcore.ShouldFailover(semanticStatus)) {
		if c != nil {
			AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
				Platform:           platform,
				ProviderID:         providerID,
				ProviderName:       providerName,
				UpstreamStatusCode: semanticStatus,
				UpstreamRequestID:  strings.TrimSpace(requestID),
				Kind:               "failover",
				Message:            cleanMsg,
				Detail:             detail,
			})
		}
		return forward.ConvertedAnthropicFailure{
			Failover: gatewayprovider.NewOpenAIUpstreamFailure(
				semanticStatus,
				respHeaders,
				payload,
				cleanMsg,
				decision.RetryableOnSameProvider(gatewayprovider.ExecutionErrorPolicy(provider), semanticStatus),
			),
		}
	}

	if c != nil {
		SetOpsUpstreamError(c, semanticStatus, cleanMsg, detail)
		AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
			Platform:           platform,
			ProviderID:         providerID,
			ProviderName:       providerName,
			UpstreamStatusCode: semanticStatus,
			UpstreamRequestID:  strings.TrimSpace(requestID),
			Kind:               "stream_error",
			Message:            cleanMsg,
			Detail:             detail,
		})
	}

	if decision.ShouldReturnGenericError() {
		return forward.ConvertedAnthropicFailure{
			Status:  http.StatusInternalServerError,
			Type:    "server_error",
			Code:    "server_error",
			Message: "Upstream gateway error",
		}
	}

	outStatus := forwardcore.MapStatus(semanticStatus)
	outType := rawErrType
	if outType == "" {
		outType = defaultErrType
	}
	outCode := rawErrType
	if c != nil && len(payload) > 0 {
		if ruleStatus, ruleType, ruleMsg, matched := ApplyErrorPassthroughRule(
			c, platform, semanticStatus, payload, outStatus, outType, cleanMsg,
		); matched {
			if ruleMsg != "" {
				cleanMsg = ruleMsg
			}
			outStatus = ruleStatus
			outType = ruleType
			MarkResponseCommitted(c)
		}
	}

	return forward.ConvertedAnthropicFailure{
		Status:  outStatus,
		Type:    outType,
		Code:    outCode,
		Message: cleanMsg,
	}
}

// mapAnthropicStreamErrorStatusCode maps an Anthropic SSE error.type string to
// its semantic HTTP status code for health, failover, and client presentation.
// Unknown or blank types default to HTTP 403 Forbidden, matching messageforward.
func mapAnthropicStreamErrorStatusCode(errType string) int {
	switch strings.ToLower(strings.TrimSpace(errType)) {
	case "overloaded_error":
		return 529
	case "rate_limit_error":
		return http.StatusTooManyRequests
	case "authentication_error":
		return http.StatusUnauthorized
	case "permission_error":
		return http.StatusForbidden
	case "invalid_request_error":
		return http.StatusBadRequest
	case "not_found_error":
		return http.StatusNotFound
	case "timeout_error", "gateway_timeout":
		return http.StatusGatewayTimeout
	case "api_error", "internal_server_error", "server_error":
		return http.StatusInternalServerError
	default:
		return http.StatusForbidden
	}
}

// extractAnthropicStreamErrorTypeBytes extracts nested error.type from an
// Anthropic SSE error JSON object via jsontext without map or gjson allocation.
// Malformed JSON, non-object values, or missing fields return an empty string.
func extractAnthropicStreamErrorTypeBytes(payload []byte) string {
	dec := jsontext.NewDecoder(bytes.NewReader(payload), jsontext.AllowInvalidUTF8(true))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return ""
	}
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return ""
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return ""
		}
		if key != "error" || len(val) == 0 || val.Kind() != '{' {
			continue
		}
		subDec := jsontext.NewDecoder(bytes.NewReader(val), jsontext.AllowInvalidUTF8(true))
		if subTok, subErr := subDec.ReadToken(); subErr != nil || subTok.Kind() != '{' {
			return ""
		}
		for subDec.PeekKind() != '}' {
			skTok, skErr := subDec.ReadToken()
			if skErr != nil {
				return ""
			}
			sKey := skTok.String()
			sVal, svErr := subDec.ReadValue()
			if svErr != nil {
				return ""
			}
			if sKey == "type" {
				var typ string
				if jsonv2.Unmarshal(sVal, &typ, jsontext.AllowInvalidUTF8(true)) == nil {
					return strings.TrimSpace(typ)
				}
				return ""
			}
		}
	}
	return ""
}
