package googleforward

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/media"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/protocol/google"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/upstream/antigravity"
)

// antigravity_converted.go executes protocol-agnostic IR request and response
// conversion from Anthropic Messages, OpenAI Responses, and OpenAI Chat
// Completions (including Cursor input-shaped Chat requests) onto Antigravity
// OAuth v1internal upstreams. One convert.Exchange translates the inbound
// payload into Gemini GenerateContent JSON parameterized by
// gatewayprovider.ConversionDialect and scoped session.GeminiToolCalls
// CallMemory, applies Antigravity's wire identity patch, CleanGeminiRequest
// schema pass, and WrapV1InternalRequest envelope, and reads responses through
// upstream/antigravity's merged ReadConvertedStreaming and ReadConvertedBuffered.
//
// Client validation, unsupported reasoning effort, non-OAuth provider types,
// and model-whitelist rejections commit a client-protocol error response
// (promoting to an untimed terminal SSE error when CommittedSSE is true) and
// return MODEL_CAPABILITY_UNSUPPORTED so outer attempt loops skip scheduler
// penalties. Rejected claude-* exchanges run RecoverClaudeIR while gemini-*
// exchanges run RecoverGemini, preserving the initial Exchange across same-model
// recovery while switching onto the fallback model's Exchange after successful
// 404 model fallback, and provider health policy evaluates at most once across
// HTTP and HTTP 200 SSE stream failures.

// ForwardConverted converts one Anthropic Messages, OpenAI Responses, or OpenAI
// Chat Completions request into a selected Antigravity OAuth v1internal attempt
// via convert.Exchange, runs family-matched recovery on rejected exchanges, and
// reads streaming or buffered client output through ModeConvertedResponse.
//
// Inbound JSON syntax errors, blank model names, "ultra" reasoning effort,
// non-OAuth targets, whitelist denials, and IR capability rejections write a
// client-protocol HTTP 400 or 403 error (or an untimed terminal SSE frame when
// CommittedSSE is true) and return MODEL_CAPABILITY_UNSUPPORTED so provider
// health scores stay unaffected. Thinking-qualified final model selection
// precedes whitelist and attribute lookup, CallMemory binds per
// (APIKeyID, ProviderID, mappedModel), and HTTP or stream failures evaluate
// health policy once while preserving sticky failover and prompt-length fallback.
// @project-doc docs/interfaces/antigravity_upstream.md#antigravity_native_execution
func (s *Antigravity) ForwardConverted(
	ctx context.Context,
	output Output,
	targetProvider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	body []byte,
	isStickySession bool,
) (*forwardcore.MessagesResult, error) {
	switch source {
	case protocol.ProtocolAnthropicMessages, protocol.ProtocolOpenAIResponses, protocol.ProtocolOpenAIChatCompletions:
	default:
		return nil, fmt.Errorf("unsupported antigravity converted source protocol %q", source)
	}

	ctx = requeststate.WithClientProtocol(ctx, source)
	c := &attempt{Output: output}
	c.Images = 0
	startTime := time.Now()

	if targetProvider == nil ||
		targetProvider.Record.Platform != capability.PlatformAntigravity ||
		targetProvider.Record.Type != capability.ProviderTypeOAuth {
		const msg = "native OAuth provider required for antigravity compatibility mode"
		s.writeAntigravityConvertedClientError(c, source, "", http.StatusBadRequest, "invalid_request_error", "", msg)
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", msg)
	}

	if source == protocol.ProtocolOpenAIResponses {
		// Normalize legacy Responses messages payloads via jsontext so mixed client
		// inputs keep 64-bit integer literals and unknown extensions intact through
		// ingress normalization and convert.NewExchange IR decoding.
		normalizedBody, normalized, err := gatewayprovider.NormalizeOpenAIResponsesLegacyIngress(body)
		if err != nil {
			const msg = "Failed to parse request body"
			s.writeAntigravityConvertedClientError(c, source, "", http.StatusBadRequest, "invalid_request_error", "", msg)
			return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", msg).WithCause(err)
		}
		if normalized {
			body = normalizedBody
		}
	}

	originalModel, ok := inspectConvertedGeminiClientModel(body)
	if !ok {
		const msg = "Failed to parse request body"
		s.writeAntigravityConvertedClientError(c, source, "", http.StatusBadRequest, "invalid_request_error", "", msg)
		parseErr := fmt.Errorf("parse %s request: invalid JSON object", source)
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", msg).WithCause(parseErr)
	}
	if strings.TrimSpace(originalModel) == "" {
		const msg = "model is required"
		s.writeAntigravityConvertedClientError(c, source, "", http.StatusBadRequest, "invalid_request_error", "", msg)
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", msg)
	}

	if err := requeststate.ValidateOpenAIReasoningEffort(body, originalModel); err != nil {
		s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(err)
	}

	// Derive thinking and routing effort from the inbound request before
	// FinalAntigravityModel so -thinking suffixes, effort_model_mapping, and
	// whitelist checks evaluate the same final model variant.
	reqState := gatewayprovider.ModelRequestState(body)
	thinkingEnabled := reqState.Thinking != nil && *reqState.Thinking
	modelCtx := requeststate.WithThinkingEnabled(ctx, thinkingEnabled)
	if requeststate.RoutingReasoningEffortFromContext(modelCtx) == "" {
		if extractedEffort := requeststate.ExtractRoutingReasoningEffort(body, source); extractedEffort != "" {
			modelCtx = requeststate.WithRoutingReasoningEffort(modelCtx, extractedEffort)
		}
	}

	mappedModel := gatewayprovider.ExecutionModelPolicy(targetProvider).FinalAntigravityModel(modelCtx, originalModel)
	if mappedModel == "" {
		c.FeatureDenied()
		msg := fmt.Sprintf("model %s not in whitelist", originalModel)
		s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusForbidden, "permission_error", "", msg)
		return nil, apperror.Forbidden("MODEL_CAPABILITY_UNSUPPORTED", msg)
	}

	if s.Tokens == nil {
		const msg = "Antigravity token provider not configured"
		s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusBadGateway, "api_error", "", msg)
		return nil, errors.New("antigravity token provider not configured")
	}
	accessToken, err := gatewayprovider.ExecutionToken(modelCtx, s.Tokens, targetProvider)
	if err != nil {
		return nil, &forwardcore.UpstreamFailoverError{
			StatusCode:   http.StatusBadGateway,
			ResponseBody: []byte(`{"error":{"type":"authentication_error","message":"Failed to get upstream access token"},"type":"error"}`),
		}
	}

	projectID, err := resolveAntigravityProjectID(targetProvider)
	if err != nil {
		s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		return nil, err
	}

	attributes, err := gatewayprovider.ResolveModelAttributes(modelCtx, targetProvider, mappedModel, s.Attributes, "gemini_generate_content")
	if err != nil {
		return nil, err
	}
	baseAttrs := attributes
	targetProvider.Capabilities = &baseAttrs

	adjusted := attributes.ForRequest(reqState)
	dialect := gatewayprovider.ConversionDialect(modelCtx, targetProvider, adjusted, protocol.ProtocolGeminiGenerateContent)
	if !dialect.StripEffort && gatewayprovider.ExecutionModelPolicy(targetProvider).ShouldStripWireEffort(modelCtx, originalModel) {
		dialect.StripEffort = true
	}

	toolScope := session.GeminiToolScope{
		APIKeyID:   c.APIKeyID(),
		ProviderID: targetProvider.Record.ID,
		Model:      mappedModel,
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
		s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("convert %s to antigravity gemini: %w", source, err))
	}

	rawGeminiBody, err := exchange.UpstreamRequest()
	if err != nil {
		s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("encode converted antigravity request: %w", err))
	}

	customIdentityPrompt := ""
	if s.Gateway != nil {
		customIdentityPrompt = s.Gateway.GetIdentityPatchPrompt(modelCtx)
	}
	geminiBody, wrappedBody, err := prepareAntigravityWireBody(projectID, mappedModel, customIdentityPrompt, rawGeminiBody)
	if err != nil {
		s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		if errors.Is(err, antigravity.ErrProjectIDRequired) {
			return nil, err
		}
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(err)
	}

	info := exchange.Info()
	clientStream := info.Stream
	prefix := logPrefix(c.GetHeader("session_id"), targetProvider.Record.Name)
	proxyURL := antigravityCompatProxyURL(targetProvider)

	retry, params := s.antigravityRetryAdapter(antigravityRetryLoopParams{
		ctx:             modelCtx,
		prefix:          prefix,
		provider:        targetProvider,
		proxyURL:        proxyURL,
		accessToken:     accessToken,
		action:          "streamGenerateContent",
		body:            wrappedBody,
		c:               c,
		httpUpstream:    s.Transport,
		providerRepo:    s.Store,
		handleError:     s.handleUpstreamError,
		requestedModel:  originalModel,
		isStickySession: isStickySession,
		groupID:         0,
		sessionHash:     "",
	})

	var (
		requestID                  string
		upstreamHeaders            http.Header
		alreadyPresentedInClassify bool
	)

	convOut, hasConvOut := c.Output.(ConvertedOutput)
	defaultErrType := "upstream_error"
	if source == protocol.ProtocolAnthropicMessages {
		defaultErrType = "api_error"
	}

	respOpts := s.antigravityResponseAdapter(c).Options
	// Leave ReverseTools unset on converted paths because ForwardConverted sends
	// raw tool names without Anthropic OAuth cc_sess_* obfuscation, and convert's
	// ToolMap restores lowered custom, namespace, and search tool identities.
	respOpts.ReverseTools = nil

	convReaderOpts := antigravity.ConvertedOptions{
		Exchange:         exchange,
		DefaultErrorType: defaultErrType,
		CommittedSSE: func() bool {
			return hasConvOut && convOut.CommittedSSE()
		},
		WriteError: func(status int, errType, code, message string) {
			if alreadyPresentedInClassify {
				return
			}
			s.writeAntigravityConvertedClientError(c, source, originalModel, status, errType, code, message)
		},
		WriteLocalTerminalError: func(status int, errType, code, message string) {
			if alreadyPresentedInClassify {
				return
			}
			if hasConvOut {
				convOut.WriteLocalTerminalError(source, originalModel, status, errType, code, message)
				return
			}
			s.writeAntigravityConvertedClientError(c, source, originalModel, status, errType, code, message)
		},
		WriteRawTerminalSSE: func(status int, errType, code, message string, data []byte) bool {
			if hasConvOut {
				return convOut.WriteRawTerminalSSE(source, status, errType, code, message, data)
			}
			return false
		},
		ClassifyStreamFailure: func(streamCtx context.Context, streamErr *upstream.GeminiStreamError, clientOutputStarted bool) antigravity.ConvertedStreamFailure {
			classified, presented := s.classifyAntigravityConvertedStreamFailure(
				streamCtx,
				c,
				targetProvider,
				source,
				originalModel,
				prefix,
				isStickySession,
				requestID,
				upstreamHeaders,
				streamErr,
				clientOutputStarted,
				clientStream,
			)
			if presented {
				alreadyPresentedInClassify = true
			}
			return classified
		},
	}

	var target *antigravity.Target
	target = &antigravity.Target{
		ProviderID: targetProvider.Record.ID,
		Model:      mappedModel,
		Mode:       antigravity.ModeConvertedResponse,
		StartedAt:  startTime,
		Response:   respOpts,
		Converted:  convReaderOpts,
		Enter:      s.Enter,
		Exchange: func(context.Context) (*http.Response, error) {
			res, loopErr := retry.AntigravityRetryLoop(params)
			if loopErr != nil {
				return nil, s.handleAntigravityConvertedTransportError(c, source, originalModel, loopErr)
			}
			if res.Resp == nil || res.Resp.StatusCode < http.StatusBadRequest {
				return res.Resp, nil
			}
			if dialect.ReplayScheme == ir.SchemeAntigravityClaude {
				return s.recoverAntigravityConvertedClaude(
					modelCtx,
					targetProvider,
					prefix,
					projectID,
					mappedModel,
					customIdentityPrompt,
					exchange,
					rawGeminiBody,
					retry,
					params,
					res.Resp,
				), nil
			}
			recoveredResp, recoveredEx, recoverErr := s.recoverAntigravityConvertedGemini(
				modelCtx,
				c,
				targetProvider,
				source,
				originalModel,
				mappedModel,
				projectID,
				accessToken,
				customIdentityPrompt,
				body,
				reqState,
				geminiBody,
				retry,
				params,
				res.Resp,
			)
			if recoverErr != nil {
				return nil, s.handleAntigravityConvertedTransportError(c, source, originalModel, recoverErr)
			}
			if recoveredEx != nil {
				target.Converted.Exchange = recoveredEx
			}
			return recoveredResp, nil
		},
		BeforeResponse: func(respCtx context.Context, resp *http.Response) (bool, error) {
			requestID = resp.Header.Get("x-request-id")
			upstreamHeaders = resp.Header.Clone()
			if resp.StatusCode < http.StatusBadRequest {
				return false, nil
			}
			return true, s.handleAntigravityConvertedHTTPError(
				respCtx,
				c,
				targetProvider,
				source,
				originalModel,
				prefix,
				isStickySession,
				resp,
			)
		},
		OutputError: func(outErr error) {
			kind := "stream_collect_error"
			if clientStream {
				kind = "stream_error"
			}
			logging.LegacyPrintf("service.antigravity_gateway", "%s status=%s error=%v", prefix, kind, outErr)
		},
	}

	result, executeErr := (antigravity.Executor{}).Execute(modelCtx, upstream.AttemptInput{
		Protocol:      source,
		Body:          wrappedBody,
		ResponseModel: originalModel,
		Stream:        clientStream,
		Target:        target,
	}, c.Sink())
	if executeErr != nil {
		// Pre-semantic failover errors never return partial completion metadata so
		// outer failover settles usage once on the winning provider attempt.
		if _, isFailover := errors.AsType[*forwardcore.UpstreamFailoverError](executeErr); isFailover {
			return nil, executeErr
		}
		if !result.HasUsage && !result.Served {
			return nil, executeErr
		}
	}

	reasoningEffort := extractConvertedGeminiEffort(source, dialect.StripEffort, info.Effort, body, mappedModel, originalModel)
	imageInputSize := s.extractImageInputSize(geminiBody)
	imageSize := media.NormalizeImageSizeTier(imageInputSize)
	imageCount := max(c.imageCount(originalModel, mappedModel), result.ObservedImages)

	return &forwardcore.MessagesResult{
		RequestID:             result.RequestID,
		UpstreamHeaders:       result.UpstreamHeaders,
		UpstreamResponseModel: result.UpstreamResponseModel,
		Usage:                 result.Usage,
		Model:                 originalModel,
		UpstreamModel:         mappedModel,
		Stream:                clientStream,
		Duration:              result.Duration,
		Timings:               result.Timings,
		ReasoningEffort:       reasoningEffort,
		ImageCount:            imageCount,
		ImageSize:             imageSize,
		ImageInputSize:        imageInputSize,
		ClientDisconnect:      result.ClientDisconnect,
	}, executeErr
}

// recoverAntigravityConvertedClaude invokes RecoverClaudeIR for rejected
// claude-* Antigravity exchanges and returns the selected HTTP response.
// Callers keep exchange's original ToolMap and ReplayScheme active so downstream
// response conversion restores lowered client tools and seals continuity carriers.
func (s *Antigravity) recoverAntigravityConvertedClaude(
	ctx context.Context,
	targetProvider *gatewayprovider.ExecutionProvider,
	prefix, projectID, mappedModel, customIdentityPrompt string,
	exchange *convert.Exchange,
	initialGeminiBody []byte,
	retry *antigravity.RetryAdapter,
	params antigravity.RetryInput,
	resp *http.Response,
) *http.Response {
	return antigravity.RecoverClaudeIR(ctx, antigravity.ClaudeIRRecoveryInput{
		ProviderID:          targetProvider.Record.ID,
		ProviderName:        targetProvider.Record.Name,
		Prefix:              prefix,
		ProjectID:           projectID,
		Model:               mappedModel,
		IdentityPatchPrompt: customIdentityPrompt,
		Exchange:            exchange,
		InitialGeminiBody:   initialGeminiBody,
	}, resp, antigravity.ClaudeIRRecoveryOptions{
		Retry: func(retryBody []byte) (*http.Response, error) {
			next := params
			next.Body = retryBody
			value, err := retry.AntigravityRetryLoop(next)
			if err != nil {
				return nil, err
			}
			return value.Resp, nil
		},
		SignatureEnabled: func(rCtx context.Context) bool {
			return s.Gateway != nil && s.Gateway.IsSignatureRectifierEnabled(rCtx)
		},
		BudgetEnabled: func(rCtx context.Context) bool {
			return s.Gateway != nil && s.Gateway.IsBudgetRectifierEnabled(rCtx)
		},
		LogConfig:          s.getLogConfig,
		ErrorDetail:        s.getUpstreamErrorDetail,
		ReadErrorBody:      s.readUpstreamErrorBody,
		Observe:            retry.Options.Observe,
		IsBudgetConstraint: anthropic.IsThinkingBudgetConstraintError,
		BudgetTokens:       anthropic.BudgetRectifyBudgetTokens,
		MinMaxTokens:       anthropic.BudgetRectifyMinMaxTokens,
		MaxTokens:          anthropic.BudgetRectifyMaxTokens,
		TruncateForLog:     logredact.TruncateLine,
		TruncateString:     logredact.TruncateUTF8,
	})
}

// recoverAntigravityConvertedGemini delegates missing-model fallback and
// thoughtSignature cleanup on gemini-* Antigravity exchanges to RecoverGemini.
// Successful 404 fallback attempts return the fallback model's convert.Exchange
// so response conversion seals carriers and stores CallMemory under the fallback
// model's scope, whereas same-model signature retries return a nil exchange and
// rewrite inner Gemini thoughtSignature fields via jsontext before envelope wrapping.
func (s *Antigravity) recoverAntigravityConvertedGemini(
	ctx context.Context,
	c *attempt,
	targetProvider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	originalModel, mappedModel, projectID, accessToken, customIdentityPrompt string,
	clientBody []byte,
	reqState modelrules.RequestState,
	injectedGeminiBody []byte,
	retry *antigravity.RetryAdapter,
	params antigravity.RetryInput,
	resp *http.Response,
) (*http.Response, *convert.Exchange, error) {
	var fallbackExchange *convert.Exchange
	recovered, err := antigravity.RecoverGemini(ctx, antigravity.GeminiRecoveryInput{
		ProviderID:   targetProvider.Record.ID,
		ProviderName: targetProvider.Record.Name,
		ProjectID:    projectID,
		Model:        mappedModel,
		Action:       "streamGenerateContent",
		AccessToken:  accessToken,
		Body:         injectedGeminiBody,
	}, resp, antigravity.GeminiRecoveryOptions{
		Prepare: func(candidateModel string, candidateBody []byte) ([]byte, error) {
			if candidateModel == "" || candidateModel == mappedModel {
				fallbackExchange = nil
				return candidateBody, nil
			}
			fbAttrs, attrErr := gatewayprovider.ResolveModelAttributes(ctx, targetProvider, candidateModel, s.Attributes, "gemini_generate_content")
			if attrErr != nil {
				return nil, attrErr
			}
			fbDialect := gatewayprovider.ConversionDialect(ctx, targetProvider, fbAttrs.ForRequest(reqState), protocol.ProtocolGeminiGenerateContent)
			if !fbDialect.StripEffort && gatewayprovider.ExecutionModelPolicy(targetProvider).ShouldStripWireEffort(ctx, originalModel) {
				fbDialect.StripEffort = true
			}
			fbScope := session.GeminiToolScope{
				APIKeyID:   c.APIKeyID(),
				ProviderID: targetProvider.Record.ID,
				Model:      candidateModel,
			}
			fbEx, exErr := convert.NewExchange(clientBody, convert.Options{
				Route: convert.Route{
					Client:   source,
					Upstream: protocol.ProtocolGeminiGenerateContent,
					Dialect:  fbDialect,
				},
				ClientModel: originalModel,
				Runtime:     convert.DefaultRuntime(),
				Calls:       s.CallMemory(fbScope),
			})
			if exErr != nil {
				return nil, exErr
			}
			fbRaw, encErr := fbEx.UpstreamRequest()
			if encErr != nil {
				return nil, encErr
			}
			fbPatched, patchErr := antigravity.InjectIdentityPatchToGeminiRequestWithPrompt(fbRaw, customIdentityPrompt)
			if patchErr != nil {
				return nil, patchErr
			}
			// Keep the identity-patched fallback Gemini bytes emitted by
			// codec/generatecontent when CleanGeminiRequest fails so 404 fallback
			// wrapping still proceeds.
			if fbCleaned, cleanErr := antigravity.CleanGeminiRequest(fbPatched); cleanErr == nil {
				fbPatched = fbCleaned
			}
			fallbackExchange = fbEx
			return fbPatched, nil
		},
		Retry: func(retryBody []byte) (*http.Response, error) {
			next := params
			next.Body = retryBody
			value, rErr := retry.AntigravityRetryLoop(next)
			if rErr != nil {
				return nil, rErr
			}
			return value.Resp, nil
		},
		Do: retry.Options.Do,
		FallbackEnabled: func(rCtx context.Context) bool {
			return s.Routing != nil && s.Routing.IsModelFallbackEnabled(rCtx)
		},
		SignatureEnabled: func(rCtx context.Context) bool {
			return s.Gateway != nil && s.Gateway.IsSignatureRectifierEnabled(rCtx)
		},
		FallbackModel: func(rCtx context.Context) string {
			if s.Routing == nil {
				return ""
			}
			return s.Routing.GetFallbackModel(rCtx, capability.PlatformAntigravity)
		},
		IsModelNotFound: upstream.IsModelNotFoundOrBare404,
		CleanSignatures: func(rawBody []byte) []byte {
			return replaceGeminiThoughtSignaturesLossless(rawBody, gemini.DummyThoughtSignature)
		},
		ReadErrorBody: s.readUpstreamErrorBody,
		ErrorDetail:   s.getUpstreamErrorDetail,
		Observe:       retry.Options.Observe,
	})
	if err != nil {
		return nil, nil, err
	}
	if recovered.Response != nil &&
		recovered.Response != resp &&
		recovered.Response.StatusCode < http.StatusBadRequest &&
		fallbackExchange != nil {
		return recovered.Response, fallbackExchange, nil
	}
	return recovered.Response, nil, nil
}

// classifyAntigravityConvertedStreamFailure evaluates provider health and
// failover rules at most once for an HTTP 200 SSE inline error or stream reader
// failure. Pre-semantic retryable errors return Failover without touching the
// client writer, whereas post-semantic or terminal failures resolve client
// error metadata and report whether fallback presentation already ran.
//
// Explicit inline Google errors run handleUpstreamError once before selecting
// failover or MapAntigravityFailure, while oversize or wirejson-colliding frames
// skip retryable empty-stream failover and render terminal client errors immediately.
func (s *Antigravity) classifyAntigravityConvertedStreamFailure(
	ctx context.Context,
	c *attempt,
	targetProvider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	originalModel, prefix string,
	isStickySession bool,
	requestID string,
	upstreamHeaders http.Header,
	streamErr *upstream.GeminiStreamError,
	clientOutputStarted bool,
	isClientStream bool,
) (antigravity.ConvertedStreamFailure, bool) {
	committed := clientOutputStarted
	if !committed {
		if state, ok := c.Sink().(interface {
			// OutputState reports current sink content commitment without initializing deferred response headers.
			OutputState() upstream.OutputHead
		}); ok {
			committed = state.OutputState().ContentCommitted
		}
	}

	if streamErr.Kind == "upstream_error" {
		body := streamErr.Body
		if len(body) == 0 {
			body = marshalSyntheticGoogleErrorBytes(streamErr.Status, streamErr.Message)
		}
		upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(google.ExtractPlatformMessage(body)))
		if upstreamMsg == "" {
			upstreamMsg = logredact.SanitizeUpstreamQueries(strings.TrimSpace(streamErr.Message))
		}
		upstreamDetail := s.getUpstreamErrorDetail(body)

		// Preserve typed prompt-too-long fallback ahead of health penalties when an
		// inline HTTP 200 error arrives before semantic content on Messages routes.
		if !committed &&
			source == protocol.ProtocolAnthropicMessages &&
			streamErr.Status == http.StatusBadRequest &&
			antigravity.IsPromptTooLongError(body) {
			logBody, maxBytes := s.getLogConfig()
			if logBody {
				logging.LegacyPrintf(
					"service.antigravity_gateway",
					"%s status=400 prompt_too_long=true upstream_message=%q request_id=%s body=%s",
					prefix,
					upstreamMsg,
					requestID,
					logredact.TruncateLine(body, maxBytes),
				)
			}
			c.Observe(ops.OpsUpstreamErrorEvent{
				Platform:           targetProvider.Record.Platform,
				ProviderID:         targetProvider.Record.ID,
				ProviderName:       targetProvider.Record.Name,
				UpstreamStatusCode: streamErr.Status,
				UpstreamRequestID:  requestID,
				Kind:               "prompt_too_long",
				Message:            upstreamMsg,
				Detail:             upstreamDetail,
			})
			return antigravity.ConvertedStreamFailure{
				Failover: &antigravity.PromptTooLongError{
					StatusCode: streamErr.Status,
					RequestID:  requestID,
					Body:       body,
				},
			}, false
		}

		s.handleUpstreamError(ctx, prefix, targetProvider, streamErr.Status, upstreamHeaders, body, originalModel, 0, "", isStickySession)

		if !committed {
			if streamErr.Status == http.StatusBadRequest && upstream.IsGoogleProjectConfigError(strings.ToLower(upstreamMsg)) {
				logging.LegacyPrintf(
					"gateway.antigravity",
					"%s status=400 google_config_error failover=true upstream_message=%q provider=%d",
					prefix,
					upstreamMsg,
					targetProvider.Record.ID,
				)
				c.Observe(ops.OpsUpstreamErrorEvent{
					Platform:           targetProvider.Record.Platform,
					ProviderID:         targetProvider.Record.ID,
					ProviderName:       targetProvider.Record.Name,
					UpstreamStatusCode: streamErr.Status,
					UpstreamRequestID:  requestID,
					Kind:               "failover",
					Message:            upstreamMsg,
					Detail:             upstreamDetail,
				})
				return antigravity.ConvertedStreamFailure{
					Failover: &forwardcore.UpstreamFailoverError{
						StatusCode:              streamErr.Status,
						ResponseBody:            body,
						ResponseHeaders:         upstreamHeaders.Clone(),
						RetryableOnSameProvider: true,
					},
				}, false
			}

			if s.shouldFailoverUpstreamError(streamErr.Status) {
				event := ops.OpsUpstreamErrorEvent{
					Platform:           targetProvider.Record.Platform,
					ProviderID:         targetProvider.Record.ID,
					ProviderName:       targetProvider.Record.Name,
					UpstreamStatusCode: streamErr.Status,
					UpstreamRequestID:  requestID,
					Kind:               "failover",
					Message:            upstreamMsg,
					Detail:             upstreamDetail,
				}
				if streamErr.Status == http.StatusUnauthorized && source != protocol.ProtocolAnthropicMessages {
					event.Stage = string(forwardcore.GatewayFailureStageProviderAuth)
					event.Scope = string(forwardcore.GatewayFailureScopeProvider)
					event.Reason = string(forwardcore.AntigravityCredentialRejectedReason)
					c.Observe(event)
					synthResp := &http.Response{
						StatusCode: streamErr.Status,
						Header:     upstreamHeaders.Clone(),
					}
					return antigravity.ConvertedStreamFailure{
						Failover: antigravityCredentialRejectedError(synthResp, body),
					}, false
				}
				c.Observe(event)
				return antigravity.ConvertedStreamFailure{
					Failover: &forwardcore.UpstreamFailoverError{
						StatusCode:      streamErr.Status,
						ResponseBody:    body,
						ResponseHeaders: upstreamHeaders.Clone(),
					},
				}, false
			}
		}

		if convOut, hasConvOut := c.Output.(ConvertedOutput); hasConvOut {
			st, tp, cd, clientMsg, mErr := convOut.MapAntigravityFailure(targetProvider, source, streamErr.Status, requestID, body)
			return antigravity.ConvertedStreamFailure{
				Status:  st,
				Type:    tp,
				Code:    cd,
				Message: clientMsg,
				Err:     mErr,
			}, false
		}
		if !clientOutputStarted {
			var mErr error
			if source == protocol.ProtocolAnthropicMessages {
				mErr = c.MappedClaudeError(targetProvider, streamErr.Status, requestID, body)
			} else {
				mErr = c.MappedAntigravityCompatError(targetProvider, streamErr.Status, requestID, body)
			}
			return antigravity.ConvertedStreamFailure{
				Status:  streamErr.Status,
				Type:    "upstream_error",
				Message: upstreamMsg,
				Err:     mErr,
			}, true
		}
		return antigravity.ConvertedStreamFailure{
			Status:  http.StatusBadGateway,
			Type:    "upstream_error",
			Message: antigravity.GetPassthroughOrDefault(upstreamMsg, "Upstream request failed"),
		}, false
	}

	if !committed && streamErr.Kind == "stream_empty" {
		return antigravity.ConvertedStreamFailure{
			Failover: &forwardcore.UpstreamFailoverError{
				StatusCode:              http.StatusBadGateway,
				ResponseBody:            []byte(`{"error":"empty stream response from upstream"}`),
				RetryableOnSameProvider: true,
			},
		}, false
	}

	// Keep pre-semantic streaming transport, truncation, and non-collision JSON
	// failures eligible for outer provider failover, matching Antigravity stream
	// recovery while rejecting oversize or wirejson-colliding frames immediately.
	if isClientStream &&
		!committed &&
		streamErr.Kind != "stream_frame_too_large" &&
		!errors.Is(streamErr, antigravity.ErrAmbiguousUsagePayload) {
		return antigravity.ConvertedStreamFailure{
			Failover: &forwardcore.UpstreamFailoverError{
				StatusCode:              http.StatusBadGateway,
				ResponseBody:            []byte(`{"error":"empty stream response from upstream"}`),
				RetryableOnSameProvider: true,
			},
		}, false
	}

	errType := "upstream_error"
	if source == protocol.ProtocolAnthropicMessages {
		errType = "api_error"
	}
	clientMsg := "Failed to parse upstream response"
	if clientOutputStarted {
		clientMsg = "Upstream stream failed"
	}
	switch streamErr.Kind {
	case "stream_frame_too_large":
		errType = "response_too_large"
		clientMsg = "Upstream response line too long"
	case "stream_timeout":
		errType = "upstream_timeout"
		clientMsg = "Upstream stream data interval timeout"
	}
	code := ""
	if source == protocol.ProtocolOpenAIResponses {
		code = streamErr.Kind
		if code == "" {
			code = errType
		}
	}

	diagMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(streamErr.Message))
	if diagMsg == "" {
		diagMsg = clientMsg
	}
	c.SetError(http.StatusBadGateway, diagMsg, "")
	c.Observe(ops.OpsUpstreamErrorEvent{
		Platform:           targetProvider.Record.Platform,
		ProviderID:         targetProvider.Record.ID,
		ProviderName:       targetProvider.Record.Name,
		UpstreamStatusCode: http.StatusBadGateway,
		UpstreamRequestID:  requestID,
		Kind:               "stream_error",
		Message:            diagMsg,
	})
	return antigravity.ConvertedStreamFailure{
		Status:  http.StatusBadGateway,
		Type:    errType,
		Code:    code,
		Message: clientMsg,
	}, false
}

// replaceGeminiThoughtSignaturesLossless rewrites every exact "thoughtSignature"
// object member value in body to placeholder via jsontext token streaming,
// preserving 64-bit integer literals and member ordering in the inner Gemini
// payload before WrapV1InternalRequest envelope serialization. Empty or
// syntactically invalid inputs return body unchanged.
func replaceGeminiThoughtSignaturesLossless(body []byte, placeholder string) []byte {
	if len(body) == 0 {
		return body
	}
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	var out bytes.Buffer
	enc := jsontext.NewEncoder(&out, jsontext.AllowInvalidUTF8(true))
	if err := rewriteThoughtSignaturesTokens(dec, enc, placeholder); err != nil {
		return body
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return body
	}
	return bytes.TrimRight(out.Bytes(), "\n")
}

// rewriteThoughtSignaturesTokens copies one complete JSON value from dec to enc
// while replacing exact "thoughtSignature" object member values with placeholder.
// Syntax errors or writer failures propagate to the caller so malformed inputs abort cleanly.
func rewriteThoughtSignaturesTokens(dec *jsontext.Decoder, enc *jsontext.Encoder, placeholder string) error {
	switch dec.PeekKind() {
	case '{':
		tok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		if err := enc.WriteToken(tok); err != nil {
			return err
		}
		for dec.PeekKind() != '}' {
			kTok, kErr := dec.ReadToken()
			if kErr != nil {
				return kErr
			}
			key := kTok.String()
			if err := enc.WriteToken(jsontext.String(key)); err != nil {
				return err
			}
			if key == "thoughtSignature" {
				if err := dec.SkipValue(); err != nil {
					return err
				}
				if err := enc.WriteToken(jsontext.String(placeholder)); err != nil {
					return err
				}
				continue
			}
			if err := rewriteThoughtSignaturesTokens(dec, enc, placeholder); err != nil {
				return err
			}
		}
		endTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		return enc.WriteToken(endTok)

	case '[':
		tok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		if err := enc.WriteToken(tok); err != nil {
			return err
		}
		for dec.PeekKind() != ']' {
			if err := rewriteThoughtSignaturesTokens(dec, enc, placeholder); err != nil {
				return err
			}
		}
		endTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		return enc.WriteToken(endTok)

	default:
		val, err := dec.ReadValue()
		if err != nil {
			return err
		}
		return enc.WriteValue(val)
	}
}

// prepareAntigravityWireBody runs Antigravity's wire-level identity injection
// (using customIdentityPrompt when non-blank or the built-in default prompt),
// best-effort schema cleanup, and v1internal project/model wrapping on
// codec/generatecontent output bytes. Returning both the unwrapped Gemini body
// and wrapped envelope preserves image-input size inspection before transport.
func prepareAntigravityWireBody(projectID, mappedModel, customIdentityPrompt string, rawGeminiBody []byte) ([]byte, []byte, error) {
	geminiBody, err := antigravity.InjectIdentityPatchToGeminiRequestWithPrompt(rawGeminiBody, customIdentityPrompt)
	if err != nil {
		return nil, nil, fmt.Errorf("inject antigravity identity patch: %w", err)
	}
	// Accept wire schema cleanup when it succeeds; a cleanup error leaves the
	// already-cleaned codec/generatecontent schema intact for wrapping.
	if cleaned, cleanErr := antigravity.CleanGeminiRequest(geminiBody); cleanErr == nil {
		geminiBody = cleaned
	}
	wrappedBody, err := antigravity.WrapV1InternalRequest(projectID, mappedModel, geminiBody)
	if err != nil {
		return nil, nil, err
	}
	return geminiBody, wrappedBody, nil
}

// handleAntigravityConvertedTransportError translates retry-loop provider-switch
// signals into UpstreamFailoverError with sticky cache-billing propagation.
// Non-switch failures inspect c.RequestContext() for inbound cancellation,
// commit a client-protocol 502 error response, and return the terminal error.
func (s *Antigravity) handleAntigravityConvertedTransportError(
	c *attempt,
	source protocol.ProtocolID,
	originalModel string,
	err error,
) error {
	// Translate the platform provider-switch signal into UpstreamFailoverError so
	// the outer selection loop receives the sticky cache-billing hint.
	if switchErr, ok := antigravity.IsAntigravityProviderSwitchError(err); ok {
		return &forwardcore.UpstreamFailoverError{
			StatusCode:        http.StatusServiceUnavailable,
			ForceCacheBilling: switchErr.IsStickySession,
		}
	}
	if c.RequestContext().Err() != nil {
		const msg = "Client disconnected before upstream response"
		s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusBadGateway, "client_disconnected", "", msg)
		return errors.New("client disconnected before upstream response")
	}
	const msg = "Upstream request failed after retries"
	s.writeAntigravityConvertedClientError(c, source, originalModel, http.StatusBadGateway, "upstream_error", "", msg)
	return errors.New("upstream request failed after retries")
}

// handleAntigravityConvertedHTTPError classifies an HTTP >= 400 Antigravity
// response across Anthropic Messages, OpenAI Responses, and OpenAI Chat
// Completions. Messages prompt-length HTTP 400 returns a typed
// PromptTooLongError before health observation, Google project-configuration
// HTTP 400 requests retryable failover, OpenAI HTTP 401 attaches credential
// rejection metadata, and terminal errors render in source's client format.
func (s *Antigravity) handleAntigravityConvertedHTTPError(
	ctx context.Context,
	c *attempt,
	targetProvider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	originalModel, prefix string,
	isStickySession bool,
	resp *http.Response,
) error {
	respBody := s.readUpstreamErrorBody(resp)
	requestID := resp.Header.Get("x-request-id")
	upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(google.ExtractPlatformMessage(respBody)))
	upstreamDetail := s.getUpstreamErrorDetail(respBody)

	// Return typed prompt-length evidence ahead of ordinary health handling on
	// Messages attempts so textattempt can switch to a configured fallback group.
	if source == protocol.ProtocolAnthropicMessages &&
		resp.StatusCode == http.StatusBadRequest &&
		antigravity.IsPromptTooLongError(respBody) {
		logBody, maxBytes := s.getLogConfig()
		if logBody {
			logging.LegacyPrintf(
				"service.antigravity_gateway",
				"%s status=400 prompt_too_long=true upstream_message=%q request_id=%s body=%s",
				prefix,
				upstreamMsg,
				requestID,
				logredact.TruncateLine(respBody, maxBytes),
			)
		}
		c.Observe(ops.OpsUpstreamErrorEvent{
			Platform:           targetProvider.Record.Platform,
			ProviderID:         targetProvider.Record.ID,
			ProviderName:       targetProvider.Record.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  requestID,
			Kind:               "prompt_too_long",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		return &antigravity.PromptTooLongError{
			StatusCode: resp.StatusCode,
			RequestID:  requestID,
			Body:       respBody,
		}
	}

	s.handleUpstreamError(ctx, prefix, targetProvider, resp.StatusCode, resp.Header, respBody, originalModel, 0, "", isStickySession)

	// Recognized Google project-configuration HTTP 400 errors request replayable
	// failover after health observation so the outer loop can retry or switch.
	if resp.StatusCode == http.StatusBadRequest && upstream.IsGoogleProjectConfigError(strings.ToLower(upstreamMsg)) {
		logging.LegacyPrintf(
			"gateway.antigravity",
			"%s status=400 google_config_error failover=true upstream_message=%q provider=%d",
			prefix,
			upstreamMsg,
			targetProvider.Record.ID,
		)
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
		return &forwardcore.UpstreamFailoverError{
			StatusCode:              resp.StatusCode,
			ResponseBody:            respBody,
			ResponseHeaders:         resp.Header.Clone(),
			RetryableOnSameProvider: true,
		}
	}

	if s.shouldFailoverUpstreamError(resp.StatusCode) {
		event := ops.OpsUpstreamErrorEvent{
			Platform:           targetProvider.Record.Platform,
			ProviderID:         targetProvider.Record.ID,
			ProviderName:       targetProvider.Record.Name,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  requestID,
			Kind:               "failover",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		}
		if resp.StatusCode == http.StatusUnauthorized && source != protocol.ProtocolAnthropicMessages {
			event.Stage = string(forwardcore.GatewayFailureStageProviderAuth)
			event.Scope = string(forwardcore.GatewayFailureScopeProvider)
			event.Reason = string(forwardcore.AntigravityCredentialRejectedReason)
			c.Observe(event)
			return antigravityCredentialRejectedError(resp, respBody)
		}
		c.Observe(event)
		return &forwardcore.UpstreamFailoverError{
			StatusCode:      resp.StatusCode,
			ResponseBody:    respBody,
			ResponseHeaders: resp.Header.Clone(),
		}
	}

	// Deliver untimed protocol-specific terminal SSE frames (including Messages
	// event: error and Responses response.failed) when keepalive already committed
	// SSE, evaluating MapAntigravityFailure first so logBody diagnostics and
	// Messages ApplyErrorPassthroughRule overrides match uncommitted responses.
	if convOut, ok := c.Output.(ConvertedOutput); ok && convOut.CommittedSSE() {
		st, tp, cd, clientMsg, mErr := convOut.MapAntigravityFailure(targetProvider, source, resp.StatusCode, requestID, respBody)
		convOut.WriteLocalTerminalError(source, originalModel, st, tp, cd, clientMsg)
		return mErr
	}

	if source == protocol.ProtocolAnthropicMessages {
		return c.MappedClaudeError(targetProvider, resp.StatusCode, requestID, respBody)
	}
	return c.MappedAntigravityCompatError(targetProvider, resp.StatusCode, requestID, respBody)
}

// writeAntigravityConvertedClientError presents a client-protocol error via
// ConvertedOutput when available, promoting to an untimed terminal SSE error
// when CommittedSSE is true. Custom test Output doubles lacking ConvertedOutput
// fall back to c's base Output error methods while discarding their companion errors.
func (s *Antigravity) writeAntigravityConvertedClientError(
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
	if source == protocol.ProtocolAnthropicMessages {
		_ = c.ClaudeError(status, errType, message)
		return
	}
	_ = c.AntigravityCompatError(status, errType, message)
}

// antigravityCompatProxyURL returns the attached provider proxy URL when both
// references exist. Otherwise it returns empty for direct transport; callers
// provide a selected provider before preparing the Antigravity call.
func antigravityCompatProxyURL(provider *gatewayprovider.ExecutionProvider) string {
	if provider.Record.ProxyID == nil || provider.Record.Proxy == nil {
		return ""
	}
	return provider.Record.Proxy.URL()
}

// antigravityCredentialRejectedError returns provider-auth failure metadata
// with cloned headers and shared diagnostic bytes. Outer handling receives
// next-provider replay, safe status 502, and the credential-rejection client
// message instead of raw authorization text.
func antigravityCredentialRejectedError(resp *http.Response, body []byte) *forwardcore.UpstreamFailoverError {
	return &forwardcore.UpstreamFailoverError{
		StatusCode:         resp.StatusCode,
		ResponseBody:       body,
		ResponseHeaders:    resp.Header.Clone(),
		Stage:              forwardcore.GatewayFailureStageProviderAuth,
		Scope:              forwardcore.GatewayFailureScopeProvider,
		Reason:             forwardcore.AntigravityCredentialRejectedReason,
		NextProviderAction: forwardcore.NextProviderRetry,
		ClientStatusCode:   http.StatusBadGateway,
		ClientMessage:      forwardcore.AntigravityCredentialRejectedClientMessage,
	}
}
