package googleforward

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/media"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/gemini"
)

// gemini_converted.go executes protocol-agnostic IR conversion from Anthropic
// Messages, OpenAI Responses, and OpenAI Chat Completions (including Cursor
// input-shaped Chat requests) onto Gemini GenerateContent upstreams. A single
// convert.Exchange translates the inbound client request directly to Gemini
// wire JSON without chaining intermediate Anthropic or Responses subset structs,
// binding session.GeminiToolCalls.CallMemory per (APIKeyID, ProviderID,
// mappedModel) scope while preserving AI Studio REST normalization, Code Assist
// OAuth wrapping, Vertex routing, and OpenCode bearer/session header adaptation.
//
// Upstream HTTP 400 signature rejections rebuild a compatibility Exchange with
// Dialect.ReplayScheme cleared and CallMemory omitted while retaining
// Dialect.StripEffort and PlaceholderToken so stale tokens drop and each step's
// leading tool call receives skip_thought_signature_validator. Response reading
// delegates to upstream/gemini's merged ReadConvertedStreaming and
// ReadConvertedBuffered functions, evaluating provider error policy at most
// once across HTTP and stream failures while keeping logical client models,
// mapped wire models, and raw UpstreamResponseModel observations distinct.

// ForwardConverted converts one Anthropic Messages, OpenAI Responses, or OpenAI
// Chat Completions request into a selected-provider Gemini attempt via
// convert.Exchange and reads streaming or buffered client output through
// ReadConvertedStreaming or ReadConvertedBuffered.
//
// Client syntax, missing-model, and unsupported capability failures write a
// client-protocol HTTP 400 response (promoting to an untimed terminal SSE error
// when CommittedSSE is true) and return MODEL_CAPABILITY_UNSUPPORTED so
// scheduler health scores stay unaffected. Project-bearing OAuth targets can
// stream upstream SSE into a buffered client response, signature-related HTTP
// 400 errors retry once in compatibility format while preserving response token
// sealing on the original Exchange, and provider health policy runs at most
// once per failed exchange.
// @project-doc docs/interfaces/gemini_upstream.md#gemini_native_execution
func (s *Gemini) ForwardConverted(
	ctx context.Context,
	output Output,
	targetProvider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	body []byte,
) (*forward.MessagesResult, error) {
	switch source {
	case protocol.ProtocolAnthropicMessages, protocol.ProtocolOpenAIResponses, protocol.ProtocolOpenAIChatCompletions:
	default:
		return nil, fmt.Errorf("unsupported gemini converted source protocol %q", source)
	}
	if targetProvider == nil {
		return nil, errors.New("provider is nil")
	}

	c := &attempt{Output: output}
	c.Images = 0
	startTime := time.Now()

	if source == protocol.ProtocolOpenAIResponses {
		normalizedBody, normalized, err := gatewayprovider.NormalizeOpenAIResponsesLegacyIngress(body)
		if err != nil {
			s.writeConvertedClientError(c, source, "", http.StatusBadRequest, "invalid_request_error", "", "Failed to parse request body")
			return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(err)
		}
		if normalized {
			body = normalizedBody
		}
	}

	originalModel, ok := inspectConvertedGeminiClientModel(body)
	if !ok {
		s.writeConvertedClientError(c, source, "", http.StatusBadRequest, "invalid_request_error", "", "Failed to parse request body")
		parseErr := fmt.Errorf("parse %s request: invalid JSON object", source)
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(parseErr)
	}
	if strings.TrimSpace(originalModel) == "" {
		s.writeConvertedClientError(c, source, "", http.StatusBadRequest, "invalid_request_error", "", "model is required")
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "model is required")
	}

	if err := requeststate.ValidateOpenAIReasoningEffort(body, originalModel); err != nil {
		s.writeConvertedClientError(c, source, originalModel, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(err)
	}

	switch targetProvider.Record.Type {
	case capability.ProviderTypeAPIKey, capability.ProviderTypeOAuth, capability.ProviderTypeServiceAccount:
	default:
		return nil, fmt.Errorf("unsupported provider type: %s", targetProvider.Record.Type)
	}

	// Resolve provider model mapping ahead of attribute lookup and CallMemory
	// scoping so OAuth, API key, Vertex, and OpenCode routes share wire identity.
	mappedModel := gatewayprovider.ExecutionModelPolicy(targetProvider).UpstreamModel(ctx, originalModel)
	toolScope := session.GeminiToolScope{
		APIKeyID:   c.APIKeyID(),
		ProviderID: targetProvider.Record.ID,
		Model:      mappedModel,
	}

	attributes, err := gatewayprovider.ResolveModelAttributes(ctx, targetProvider, mappedModel, s.Attributes, "gemini_generate_content")
	if err != nil {
		return nil, err
	}
	baseAttrs := attributes
	targetProvider.Capabilities = &baseAttrs

	adjusted := attributes.ForRequest(gatewayprovider.ModelRequestState(body))
	dialect := gatewayprovider.ConversionDialect(ctx, targetProvider, adjusted, protocol.ProtocolGeminiGenerateContent)
	if !dialect.StripEffort && gatewayprovider.ExecutionModelPolicy(targetProvider).ShouldStripWireEffort(ctx, originalModel) {
		dialect.StripEffort = true
	}

	exchange, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   source,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect:  dialect,
		},
		ClientModel: originalModel,
		Runtime:     convert.DefaultRuntime(),
		Calls:       s.CallMemory(toolScope),
	})
	if err != nil {
		s.writeConvertedClientError(c, source, originalModel, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("convert %s to gemini: %w", source, err))
	}

	geminiReq, err := exchange.UpstreamRequest()
	if err != nil {
		s.writeConvertedClientError(c, source, originalModel, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("encode converted gemini request: %w", err))
	}
	upstream.ObserveExchangeConversion(ctx, exchange)

	clientStream := exchange.Info().Stream
	useUpstreamStream := clientStream
	// Project-bearing Code Assist OAuth streams SSE upstream even for buffered
	// clients; keep clientStream separate so ReadConvertedBuffered folds the stream.
	if targetProvider.Record.Type == capability.ProviderTypeOAuth &&
		!clientStream &&
		strings.TrimSpace(targetProvider.View().GetCredential("project_id")) != "" {
		useUpstreamStream = true
	}

	proxyURL := ""
	if targetProvider.Record.ProxyID != nil && targetProvider.Record.Proxy != nil {
		proxyURL = targetProvider.Record.Proxy.URL()
	}

	plan := s.geminiRequestPlan(targetProvider, mappedModel, "", false, clientStream, useUpstreamStream, false, c.GetHeader)
	exchOpts := s.geminiConvertedExchangeOptions(c, ctx, targetProvider, mappedModel, source, originalModel)
	exchOpts.Build = func(buildCtx context.Context) (*http.Request, string, error) {
		return gemini.BuildRequest(buildCtx, geminiReq, plan)
	}
	exchOpts.RequestIDHeader = "x-request-id"
	exchOpts.Do = func(req *http.Request) (*http.Response, error) {
		return s.Transport.Do(req, proxyURL, targetProvider.Record.ID, targetProvider.Record.Concurrency)
	}
	exchOpts.RepairToolReplay = func() bool {
		// Rebuild request bytes with ReplayScheme cleared and Calls omitted while
		// preserving StripEffort and PlaceholderToken so rejected signatures drop
		// and each step's first call receives skip_thought_signature_validator,
		// keeping the original Exchange intact for sealing new response tokens.
		retryDialect := dialect
		retryDialect.ReplayScheme = ""
		retryEx, retryErr := convert.NewExchange(body, convert.Options{
			Route: convert.Route{
				Client:   source,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  retryDialect,
			},
			ClientModel: originalModel,
			Runtime:     convert.DefaultRuntime(),
		})
		if retryErr != nil {
			return false
		}
		fallback, encErr := retryEx.UpstreamRequest()
		if encErr != nil || bytes.Equal(fallback, geminiReq) {
			return false
		}
		// Replace initial request losses with the signature-free retry exchange's
		// losses so retry attempts and final usage reflect the accepted request.
		exchange.ReplaceRequestLosses(retryEx.Losses())
		telemetry.ReplaceProtocolConversion(ctx, upstream.ExchangeConversion(exchange))
		geminiReq = fallback
		return true
	}

	var (
		requestID                  string
		upstreamHeaders            http.Header
		reasoningEffort            *string
		compatibilityResult        *forward.MessagesResult
		stopped                    bool
		alreadyPresentedInClassify bool
	)

	convOut, hasConvOut := c.Output.(ConvertedOutput)
	defaultErrType := "upstream_error"
	if source == protocol.ProtocolAnthropicMessages {
		defaultErrType = "api_error"
	}

	convReaderOpts := gemini.ConvertedOptions{
		Exchange:         exchange,
		DefaultErrorType: defaultErrType,
		CommittedSSE: func() bool {
			return hasConvOut && convOut.CommittedSSE()
		},
		WriteError: func(status int, errType, code, message string) {
			if alreadyPresentedInClassify {
				return
			}
			s.writeConvertedClientError(c, source, originalModel, status, errType, code, message)
		},
		WriteLocalTerminalError: func(status int, errType, code, message string) {
			if alreadyPresentedInClassify {
				return
			}
			if hasConvOut {
				convOut.WriteLocalTerminalError(source, originalModel, status, errType, code, message)
				return
			}
			s.writeConvertedClientError(c, source, originalModel, status, errType, code, message)
		},
		WriteRawTerminalSSE: func(status int, errType, code, message string, data []byte) bool {
			if hasConvOut {
				return convOut.WriteRawTerminalSSE(source, status, errType, code, message, data)
			}
			return false
		},
		ReadBody: func(r io.Reader) ([]byte, bool, error) {
			if hasConvOut {
				return convOut.ReadConvertedBody(r, s.Options.ResponseReadLimit, source)
			}
			raw, readErr := c.ReadBody(r, s.Options.ResponseReadLimit)
			return raw, false, readErr
		},
		ClassifyStreamFailure: func(streamCtx context.Context, streamErr *upstream.GeminiStreamError, clientOutputStarted bool) gemini.ConvertedStreamFailure {
			res, presented := s.classifyConvertedStreamFailure(
				streamCtx,
				c,
				targetProvider,
				source,
				originalModel,
				mappedModel,
				requestID,
				upstreamHeaders,
				streamErr,
				clientOutputStarted,
			)
			if presented {
				alreadyPresentedInClassify = true
			}
			return res
		},
	}

	target := &gemini.Target{
		ProviderID:     targetProvider.Record.ID,
		Model:          mappedModel,
		Mode:           gemini.ConvertedResponse,
		Exchange:       exchOpts,
		Response:       s.geminiResponseAdapter(c).Options,
		Converted:      convReaderOpts,
		StartedAt:      startTime,
		UpstreamStream: useUpstreamStream,
		OAuth:          targetProvider.Record.Type == capability.ProviderTypeOAuth,
		Enter:          s.Enter,
	}

	target.BeforeResponse = func(respCtx context.Context, resp *http.Response, requestIDHeader string) (bool, error) {
		var callbackErr error
		compatibilityResult, callbackErr = func() (*forward.MessagesResult, error) {
			requestID = resp.Header.Get(requestIDHeader)
			if requestID == "" {
				requestID = resp.Header.Get("x-goog-request-id")
			}
			upstreamHeaders = resp.Header.Clone()

			reasoningEffort = extractConvertedGeminiEffort(source, dialect.StripEffort, exchange.Info().Effort, body, mappedModel, originalModel)

			if resp.StatusCode >= http.StatusBadRequest {
				return nil, s.handleConvertedHTTPFailure(respCtx, c, targetProvider, source, originalModel, mappedModel, requestID, resp)
			}
			return nil, nil
		}()
		stopped = resp.StatusCode >= http.StatusBadRequest || callbackErr != nil || compatibilityResult != nil
		return stopped, callbackErr
	}

	result, executeErr := (gemini.Executor{}).Execute(ctx, upstream.AttemptInput{
		Protocol:      source,
		Body:          geminiReq,
		Stream:        clientStream,
		ResponseModel: originalModel,
		Target:        target,
	}, c.Sink())
	if stopped {
		return compatibilityResult, executeErr
	}
	if executeErr != nil {
		// Pre-semantic failover errors never carry partial completion results so
		// outer failover settles usage once on the winning provider attempt.
		if _, isFailover := errors.AsType[*forward.UpstreamFailoverError](executeErr); isFailover {
			return nil, executeErr
		}
		if !result.HasUsage && !result.Served {
			return nil, executeErr
		}
	}

	requestID = result.RequestID
	usage := &result.Usage
	imageInputSize := s.extractImageInputSize(geminiReq)
	imageSize := media.NormalizeImageSizeTier(imageInputSize)
	imageCount := max(c.imageCount(originalModel, mappedModel), result.ObservedImages)

	return &forward.MessagesResult{
		RequestID:             requestID,
		UpstreamHeaders:       result.UpstreamHeaders,
		UpstreamResponseModel: result.UpstreamResponseModel,
		Usage:                 *usage,
		Model:                 originalModel,
		UpstreamModel:         mappedModel,
		Stream:                clientStream,
		Duration:              result.Duration,
		Timings:               result.Timings,
		ProtocolConversion:    result.ProtocolConversion,
		ReasoningEffort:       reasoningEffort,
		ImageCount:            imageCount,
		ImageSize:             imageSize,
		ImageInputSize:        imageInputSize,
		ClientDisconnect:      result.ClientDisconnect,
	}, executeErr
}

// geminiConvertedExchangeOptions binds retry policy, health observation, and
// protocol-aware BuildError and FinalError callbacks for one converted Gemini
// exchange. Compatible relays return UpstreamFailoverError on transport
// exhaustion, whereas Google accounts render source's client error format.
func (s *Gemini) geminiConvertedExchangeOptions(
	c *attempt,
	_ context.Context,
	targetProvider *gatewayprovider.ExecutionProvider,
	model string,
	source protocol.ProtocolID,
	originalModel string,
) gemini.ExchangeOptions {
	options := s.geminiExchangeOptions(c, targetProvider, model)
	options.BuildError = func(err error) error {
		status := http.StatusBadGateway
		kind := "upstream_error"
		if source == protocol.ProtocolAnthropicMessages && strings.Contains(err.Error(), "missing project_id") {
			status = http.StatusBadRequest
			kind = "invalid_request_error"
		}
		s.writeConvertedClientError(c, source, originalModel, status, kind, "", err.Error())
		return fmt.Errorf("%s", err.Error())
	}
	options.FinalError = func(message string) error {
		// Compatible relays return an UpstreamFailoverError on transport exhaustion
		// so the outer gateway loop can switch providers within its budget.
		if !isGoogleAccount(targetProvider) {
			return &forward.UpstreamFailoverError{
				Platform:                targetProvider.Record.Platform,
				StatusCode:              http.StatusBadGateway,
				RetryableOnSameProvider: targetProvider.View().IsPoolMode() && targetProvider.View().IsPoolModeRetryableStatus(http.StatusBadGateway),
			}
		}
		s.writeConvertedClientError(c, source, originalModel, http.StatusBadGateway, "upstream_error", "", message)
		return fmt.Errorf("%s", message)
	}
	return options
}

// handleConvertedHTTPFailure evaluates provider error policy once on an HTTP >=
// 400 response before body conversion starts. Retryable outcomes return an
// UpstreamFailoverError, pre-committed SSE streams across all client protocols
// emit an untimed local terminal SSE error frame, and uncommitted terminal
// failures render source's client JSON error format.
func (s *Gemini) handleConvertedHTTPFailure(
	ctx context.Context,
	c *attempt,
	targetProvider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	originalModel, mappedModel, requestID string,
	resp *http.Response,
) error {
	respBody := s.readUpstreamErrorBody(resp)
	outcome := s.buildGeminiFailureOutcome(ctx, targetProvider, resp, respBody, mappedModel)

	isOAuth := targetProvider.Record.Type == capability.ProviderTypeOAuth
	evBody := respBody
	if source != protocol.ProtocolAnthropicMessages {
		evBody = gemini.UnwrapIfNeeded(isOAuth, respBody)
	}

	if outcome.failover != nil {
		if source == protocol.ProtocolAnthropicMessages {
			upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(respBody)))
			c.Observe(ops.OpsUpstreamErrorEvent{
				Platform:           targetProvider.Record.Platform,
				ProviderID:         targetProvider.Record.ID,
				ProviderName:       targetProvider.Record.Name,
				UpstreamStatusCode: resp.StatusCode,
				UpstreamRequestID:  requestID,
				Kind:               "failover",
				Message:            upstreamMsg,
				Detail:             s.upstreamErrorDetail(respBody),
			})
			if outcome.googleConfigError {
				logging.LegacyPrintf("gateway.gemini", "[Gemini] status=400 google_config_error failover=true upstream_message=%q provider=%d", upstreamMsg, targetProvider.Record.ID)
			}
			return outcome.failover
		}

		// OpenAI-compatible ordinary failures expose the unwrapped OAuth error
		// without body detail, whereas skipped/pool policies retain raw bytes.
		upstreamDetail := ""
		if outcome.policy == provider.ErrorPolicyCustomSkipped || outcome.policy == provider.ErrorPolicyPoolBypassed {
			upstreamDetail = s.upstreamErrorDetail(respBody)
		} else {
			outcome.failover.ResponseBody = evBody
		}
		upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(outcome.failover.ResponseBody)))
		c.Observe(ops.OpsUpstreamErrorEvent{
			Platform:           targetProvider.Record.Platform,
			ProviderID:         targetProvider.Record.ID,
			ProviderName:       targetProvider.Record.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  requestID,
			Kind:               "failover",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		return outcome.failover
	}

	// Route pre-committed HTTP 200 SSE responses through the untimed local
	// terminal SSE writer for all client protocols (including Anthropic Messages
	// event: error framing) and deduplicate via beginGoogleError.
	convOut, hasConvOut := c.Output.(ConvertedOutput)
	if hasConvOut && convOut.CommittedSSE() {
		customSkipped := outcome.policy == provider.ErrorPolicyCustomSkipped
		mapBody := evBody
		if customSkipped {
			mapBody = respBody
		}
		st, tp, cd, msg, mErr := convOut.MapGeminiStreamFailure(targetProvider, source, resp.StatusCode, requestID, mapBody, customSkipped)
		convOut.WriteLocalTerminalError(source, originalModel, st, tp, cd, msg)
		return mErr
	}

	if source == protocol.ProtocolAnthropicMessages {
		if outcome.policy == provider.ErrorPolicyCustomSkipped {
			return c.GeminiCustomCodeSkippedError(targetProvider, resp.StatusCode, requestID, respBody, func() {
				s.writeConvertedClientError(c, source, originalModel, http.StatusInternalServerError, "api_error", "", geminiCustomCodeSkippedClientMessage)
			})
		}
		return c.GeminiMappedError(targetProvider, resp.StatusCode, requestID, respBody)
	}

	compatProto := geminiCompatProtocolForSource(source)
	if outcome.policy == provider.ErrorPolicyCustomSkipped {
		return c.GeminiCustomCodeSkippedError(targetProvider, resp.StatusCode, requestID, respBody, func() {
			// Discard the string error returned by GeminiOpenAICompatError because
			// GeminiCustomCodeSkippedError constructs the canonical skipped-code error.
			_ = c.GeminiOpenAICompatError(compatProto, http.StatusInternalServerError, "api_error", geminiCustomCodeSkippedClientMessage)
		})
	}
	return c.GeminiOpenAICompatMappedError(targetProvider, resp.StatusCode, requestID, evBody, compatProto)
}

// classifyConvertedStreamFailure evaluates provider error policy once for an
// HTTP 200 Gemini stream or inline error. Before real protocol content has been
// committed to the sink, retryable outcomes return Failover without presenting
// client bytes; otherwise client error metadata is resolved through
// ConvertedOutput or fallback Output methods, reporting whether fallback HTTP
// presentation already ran so callers never write duplicate error envelopes.
func (s *Gemini) classifyConvertedStreamFailure(
	ctx context.Context,
	c *attempt,
	targetProvider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	_, mappedModel, requestID string,
	upstreamHeaders http.Header,
	streamErr *upstream.GeminiStreamError,
	clientOutputStarted bool,
) (gemini.ConvertedStreamFailure, bool) {
	body := streamErr.Body
	if len(body) == 0 {
		// Synthesize a typed Google error envelope from scalar fields when an
		// upstream stream failure carried no raw JSON payload.
		body = marshalSyntheticGoogleErrorBytes(streamErr.Status, streamErr.Message)
	}
	synthResp := &http.Response{
		StatusCode: streamErr.Status,
		Header:     upstreamHeaders.Clone(),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	defer func() { _ = synthResp.Body.Close() }()

	outcome := s.buildGeminiFailureOutcome(ctx, targetProvider, synthResp, body, mappedModel)
	committed := clientOutputStarted
	if !committed {
		if state, ok := c.Sink().(interface {
			// OutputState reports content delivery without initializing headers.
			OutputState() upstream.OutputHead
		}); ok {
			committed = state.OutputState().ContentCommitted
		}
	}

	if outcome.failover != nil && !committed {
		// Allow configured failover while only waiting heartbeats or staged
		// non-semantic preamble frames have occurred on this attempt.
		c.Observe(ops.OpsUpstreamErrorEvent{
			Platform:           targetProvider.Record.Platform,
			ProviderID:         targetProvider.Record.ID,
			ProviderName:       targetProvider.Record.Name,
			UpstreamStatusCode: streamErr.Status,
			UpstreamRequestID:  requestID,
			Kind:               "failover",
			Message:            logredact.SanitizeUpstreamQueries(streamErr.Message),
		})
		return gemini.ConvertedStreamFailure{Failover: outcome.failover}, false
	}

	customSkipped := outcome.policy == provider.ErrorPolicyCustomSkipped
	if convOut, hasConvOut := c.Output.(ConvertedOutput); hasConvOut {
		mapBody := body
		if source != protocol.ProtocolAnthropicMessages && !customSkipped {
			mapBody = gemini.UnwrapIfNeeded(targetProvider.Record.Type == capability.ProviderTypeOAuth, body)
		}
		st, tp, cd, msg, mErr := convOut.MapGeminiStreamFailure(targetProvider, source, streamErr.Status, requestID, mapBody, customSkipped)
		return gemini.ConvertedStreamFailure{
			Status:  st,
			Type:    tp,
			Code:    cd,
			Message: msg,
			Err:     mErr,
		}, false
	}

	// Fall back to direct Output error methods when custom test wrappers omit
	// ConvertedOutput; pre-output failures present once and report presented=true.
	compatProto := geminiCompatProtocolForSource(source)
	present := func(st int, errBody []byte) error {
		if source == protocol.ProtocolAnthropicMessages {
			return c.GeminiMappedError(targetProvider, st, requestID, errBody)
		}
		return c.GeminiOpenAICompatMappedError(targetProvider, st, requestID, errBody, compatProto)
	}
	if !clientOutputStarted {
		var presented error
		if customSkipped {
			presented = c.GeminiCustomCodeSkippedError(targetProvider, streamErr.Status, requestID, body, func() {
				fallback := marshalSyntheticGoogleErrorBytes(http.StatusInternalServerError, geminiCustomCodeSkippedClientMessage)
				// Discard the companion error because GeminiCustomCodeSkippedError returns
				// the canonical custom-code miss error to the outer caller.
				_ = present(http.StatusInternalServerError, fallback)
			})
		} else {
			presented = present(streamErr.Status, body)
		}
		return gemini.ConvertedStreamFailure{
			Status:  streamErr.Status,
			Type:    "upstream_error",
			Message: streamErr.Message,
			Err:     presented,
		}, true
	}
	if customSkipped {
		err := c.GeminiCustomCodeSkippedError(targetProvider, streamErr.Status, requestID, body, func() {})
		return gemini.ConvertedStreamFailure{
			Status:  http.StatusInternalServerError,
			Type:    "api_error",
			Message: geminiCustomCodeSkippedClientMessage,
			Err:     err,
		}, false
	}
	cleanMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(streamErr.Message))
	if cleanMsg == "" {
		cleanMsg = "Upstream stream failed"
	}
	return gemini.ConvertedStreamFailure{
		Status:  streamErr.Status,
		Type:    "upstream_error",
		Message: cleanMsg,
	}, false
}

// marshalSyntheticGoogleErrorBytes encodes a minimal Google JSON error object
// carrying code and message via encoding/json/v2 using private typed structs.
// Encoding failures fall back to a static HTTP 502 error literal so policy
// classifiers always receive valid JSON bytes.
func marshalSyntheticGoogleErrorBytes(code int, message string) []byte {
	// synthGoogleErrorBody carries numeric status and message inside a synthetic Google error object.
	type synthGoogleErrorBody struct {
		Code    int    `json:"code"`    // HTTP or Google status code
		Message string `json:"message"` // sanitized diagnostic message
	}
	// synthGoogleErrorEnvelope wraps synthGoogleErrorBody under a top-level "error" member.
	type synthGoogleErrorEnvelope struct {
		Error synthGoogleErrorBody `json:"error"` // nested Google error payload
	}
	encoded, err := json.Marshal(synthGoogleErrorEnvelope{
		Error: synthGoogleErrorBody{
			Code:    code,
			Message: message,
		},
	})
	if err != nil {
		return []byte(`{"error":{"code":502,"message":"Upstream stream failed"}}`)
	}
	return encoded
}

// writeConvertedClientError writes a client-protocol error response via
// ConvertedOutput when available, promoting to an untimed terminal SSE error
// when CommittedSSE is true. Callers supplying custom Output mocks without
// ConvertedOutput fall back to c's base Output error methods.
func (s *Gemini) writeConvertedClientError(
	c *attempt,
	source protocol.ProtocolID,
	originalModel string,
	status int,
	errType, code, message string,
) {
	if convOut, ok := c.Output.(ConvertedOutput); ok {
		convOut.WriteConvertedError(source, originalModel, status, errType, code, message)
		return
	}
	// Discard companion presentation errors on custom Output test doubles because
	// callers construct and return their own typed capability or upstream error.
	switch source {
	case protocol.ProtocolAnthropicMessages:
		_ = c.ClaudeError(status, errType, message)
	case protocol.ProtocolOpenAIResponses:
		_ = c.GeminiOpenAICompatError(gemini.OpenAICompatResponses, status, errType, message)
	default:
		_ = c.ChatError(status, errType, message)
	}
}

// extractConvertedGeminiEffort derives the recorded reasoning effort attached
// to MessagesResult for OpenAI Responses, Chat Completions (including Cursor
// input-shaped Chat requests carrying top-level reasoning_effort), or
// StripEffort Anthropic Messages requests. Non-stripped Anthropic Messages
// requests return nil so outer completion fills effort rules.
func extractConvertedGeminiEffort(
	source protocol.ProtocolID,
	stripEffort bool,
	preStripEffort string,
	body []byte,
	mappedModel, originalModel string,
) *string {
	switch source {
	case protocol.ProtocolOpenAIResponses:
		eff := forward.ExtractEffort(body, false, capability.NormalizeRecordedOpenAIEffortForModel, mappedModel, originalModel)
		return gatewayprovider.ApplyThinkingEnabledFallback(eff, body, mappedModel)
	case protocol.ProtocolOpenAIChatCompletions:
		eff := forward.ExtractEffort(body, true, capability.NormalizeRecordedOpenAIEffortForModel, mappedModel, originalModel)
		return gatewayprovider.ApplyThinkingEnabledFallback(eff, body, mappedModel)
	default:
		if stripEffort && preStripEffort != "" {
			return protocol.NormalizeClaudeOutputEffort(preStripEffort)
		}
		return nil
	}
}

// geminiCompatProtocolForSource maps source onto OpenAICompatResponses for
// OpenAI Responses requests and OpenAICompatChatCompletions for every other
// client protocol.
func geminiCompatProtocolForSource(source protocol.ProtocolID) gemini.OpenAICompatProtocol {
	if source == protocol.ProtocolOpenAIResponses {
		return gemini.OpenAICompatResponses
	}
	return gemini.OpenAICompatChatCompletions
}

// inspectConvertedGeminiClientModel scans top-level JSON object members from
// body via jsontext, allowing invalid UTF-8 in strings, to extract the first
// string model field without map decoding. Non-object inputs, malformed tokens,
// or trailing data return false.
func inspectConvertedGeminiClientModel(body []byte) (string, bool) {
	var model string
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return "", false
	}
	sawModel := false
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return "", false
		}
		if keyTok.String() == "model" && !sawModel && dec.PeekKind() == '"' {
			valTok, vErr := dec.ReadToken()
			if vErr != nil {
				return "", false
			}
			model = valTok.String()
			sawModel = true
			continue
		}
		if err := dec.SkipValue(); err != nil {
			return "", false
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return "", false
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return "", false
	}
	return model, true
}
