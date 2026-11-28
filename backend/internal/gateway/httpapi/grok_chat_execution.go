package httpapi

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/moderationflow"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/tierpolicy"
	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	protocolopenai "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/grok"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// grok_chat_execution.go executes eligible Grok Chat requests through
// Responses transport and converts output for Chat clients.
// A Grok Chat request can use Responses transport while returning Chat JSON or
// SSE to its client. The bridge selects that path from the provider's resolved
// model, cache identity and image input, returning handled=false when native
// Chat should perform this attempt instead.
//
// ChatResponses inspects and patches JSON losslessly via jsontext, converts
// standard Chat and Cursor input-shaped payloads through convert.Exchange with
// OmitStoreAndInclude enabled, retries rejected xAI encrypted reasoning once
// without rounding large integers, and reads output through
// ReadConvertedStreaming and ReadConvertedBuffered.

// grokChatClientBodyInfo summarizes top-level scalar, shape, and media signals
// scanned from an incoming Chat Completions or Cursor input-shaped JSON body.
type grokChatClientBodyInfo struct {
	model         string // decoded top-level model identifier
	stream        bool   // decoded top-level stream boolean
	instructions  string // trimmed top-level instructions compatibility string
	hasMessages   bool   // whether top-level "messages" member is present
	hasInput      bool   // whether top-level "input" member is present
	hasImageInput bool   // whether messages or input contains image media parts
}

// rawGrokJSONMember pairs one JSON object key with its cloned raw value bytes
// during lossless Grok Responses request patching.
type rawGrokJSONMember struct {
	name  string         // decoded JSON object key
	value jsontext.Value // cloned raw JSON value slice
}

// ChatResponses executes a Chat Completions or Cursor input-shaped Chat
// request against Grok Responses using protocol/convert and the merged
// ReadConvertedStreaming and ReadConvertedBuffered readers, returning whether
// it handled the attempt.
//
// Malformed JSON, unsupported reasoning efforts such as "ultra", image-only
// model selections, and IR validation rejections write an OpenAI 400 error (or
// untimed terminal SSE frame when CommittedSSE is true) and return
// MODEL_CAPABILITY_UNSUPPORTED so provider scheduling scores stay unaffected.
// Unplanned adaptive OAuth attempts that fail runtime bridge eligibility return
// handled=false without writing output so the caller can fall back to native
// Chat; accepted attempts omit default store and include controls, retry once
// after lossless jsontext stripping when replayed xAI ciphertext is rejected
// with HTTP 400, and evaluate Grok health at most once per response.
func (s *GrokExecutor) ChatResponses(
	ctx context.Context,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	body []byte,
	promptCacheKey string,
	defaultMappedModel string,
	tlsRouterMatch ...egress.TLSFingerprintRouterMatchResult,
) (*forward.OpenAIResult, bool, error) {
	startTime := time.Now()

	clientInfo, parseErr := inspectGrokChatClientBody(body)
	if parseErr != nil {
		WriteGrokForwardInvalidRequest(c, "Failed to parse request body", "")
		return nil, true, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(fmt.Errorf("parse grok chat completions request: %w", parseErr))
	}

	originalModel := clientInfo.model
	clientStream := clientInfo.stream
	isResponsesShape := !clientInfo.hasMessages && clientInfo.hasInput

	billingModel := gatewayprovider.ExecutionModelPolicy(provider).WithContext(ctx).ForwardModel(originalModel, defaultMappedModel)
	upstreamModel := gatewayprovider.ExecutionModelPolicy(provider).NormalizeOpenAI(billingModel)
	attributes, attributesErr := gatewayprovider.ResolveModelAttributes(ctx, provider, upstreamModel, s.Attributes, "openai_responses")
	if attributesErr != nil {
		if routing.IsModelAttributeError(attributesErr) && c != nil {
			WriteOpenAIModelAttributeError(c, attributesErr, c.Writer != nil && c.Writer.Written())
		}
		return nil, true, attributesErr
	}
	if provider != nil {
		provider.Capabilities = &attributes
	}
	if attributes.Attributes.OutputOnly("image") {
		msg := fmt.Sprintf("model %s is an image model and is not available on the Responses endpoint; use /v1/images/generations instead", upstreamModel)
		WriteGrokForwardInvalidRequest(c, msg, "model")
		return nil, true, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", msg)
	}

	cacheIdentity := ResolveGrokCacheIdentity(c, body, promptCacheKey, upstreamModel)
	// Unplanned adaptive Grok OAuth routes bridge only eligible 4.5/4.6 cached or
	// image Chat turns, leaving other models and uncached text turns to native Chat.
	if (provider == nil || provider.Route.Protocol() == "") &&
		!gatewayprovider.GrokBodyCodec().GrokChatResponsesRuntimeEligible(upstreamModel, cacheIdentity) &&
		(!clientInfo.hasImageInput || !gatewayprovider.GrokBodyCodec().GrokChatResponsesBridgeModel(upstreamModel)) {
		return nil, false, nil
	}

	reqResolution := attributes.ForRequest(gatewayprovider.ModelRequestState(body))
	dialect := gatewayprovider.ConversionDialect(ctx, provider, reqResolution, protocol.ProtocolOpenAIResponses)
	dialect.OmitStoreAndInclude = true
	if dialect.ReplayScheme == "" && reqResolution.Rules.SignatureMode != "passback" {
		dialect.ReplayScheme = "xai"
	}

	if !dialect.StripEffort {
		if err := requeststate.ValidateOpenAIReasoningEffort(body, originalModel); err != nil {
			WriteGrokForwardInvalidRequest(c, err.Error(), "")
			return nil, true, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(err)
		}
	}

	exchange, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  dialect,
		},
		ClientModel: originalModel,
		Runtime:     convert.DefaultRuntime(),
	})
	if err != nil {
		WriteGrokForwardInvalidRequest(c, err.Error(), "")
		return nil, true, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("convert grok chat completions to responses: %w", err))
	}
	responsesBody, err := exchange.UpstreamRequest()
	if err != nil {
		WriteGrokForwardInvalidRequest(c, err.Error(), "")
		return nil, true, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("encode grok responses bridge request: %w", err))
	}
	upstream.ObserveExchangeConversion(ctx, exchange)

	var deleteFields []string
	if isResponsesShape {
		deleteFields = openaiforward.CursorResponsesUnsupportedFields
	}
	injectInstructions := ""
	if !isResponsesShape {
		injectInstructions = clientInfo.instructions
	}
	responsesBody, err = patchGrokConvertedResponsesBody(responsesBody, upstreamModel, injectInstructions, deleteFields)
	if err != nil {
		return nil, true, fmt.Errorf("patch grok responses bridge request: %w", err)
	}

	// Capture tool intent before PatchGrokResponsesBody drops orphan tool controls
	// so Free-tier cache routing distinguishes tool-free turns from explicit tools.
	intentBody, err := grokChatCacheIntentBodyBytes(responsesBody)
	if err != nil {
		return nil, true, fmt.Errorf("normalize grok responses bridge cache intent: %w", err)
	}
	codec := gatewayprovider.GrokBodyCodec()
	codec.Attributes = func(string) modelcatalog.Attributes { return attributes.Attributes }
	responsesBody, err = codec.PatchGrokResponsesBody(responsesBody, upstreamModel)
	if err != nil {
		return nil, true, fmt.Errorf("patch grok responses bridge request: %w", err)
	}
	isGrokOAuth := provider != nil && provider.View().IsGrokOAuth()
	responsesBody, err = grok.ApplyGrokResponsesCacheIdentity(responsesBody, intentBody, cacheIdentity, isGrokOAuth)
	if err != nil {
		return nil, true, fmt.Errorf("apply grok responses bridge cache identity: %w", err)
	}
	responsesBody, err = ApplyGrokFreeRequestToolCacheRoute(c, responsesBody, intentBody, provider, cacheIdentity)
	if err != nil {
		return nil, true, fmt.Errorf("apply grok responses bridge function-tool cache route: %w", err)
	}

	if s.FastPolicy != nil {
		updatedBody, policyErr := tierpolicy.ApplyBody(responsesBody, s.FastPolicy.Input(ctx, provider, upstreamModel))
		if policyErr != nil {
			if blocked, ok := errors.AsType[*tierpolicy.BlockedError](policyErr); ok {
				MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
				WriteForwardChatError(c, http.StatusForbidden, "permission_error", blocked.Message)
			}
			return nil, true, policyErr
		}
		responsesBody = updatedBody
	}

	token, _, err := s.Credentials.Resolve(ctx, RequestCredentialBudget(c), CredentialObserver{Context: c}, provider)
	if err != nil {
		return nil, true, fmt.Errorf("get grok access token: %w", err)
	}
	// Detach client cancellation deadlines while keeping request values and
	// shutdown barriers alive until upstream exchange and response reading end.
	upstreamCtx, releaseUpstreamCtx := gatewayprovider.DetachUpstreamContext(ctx)
	defer releaseUpstreamCtx()

	initialReq, err := s.BuildResponsesRequest(upstreamCtx, c, provider, responsesBody, token, cacheIdentity, true)
	if err != nil {
		return nil, true, fmt.Errorf("build grok responses bridge request: %w", err)
	}
	SetActualOpenAIUpstreamEndpoint(c, grok.GrokChatResponsesEndpoint)

	proxyURL := ""
	var providerID int64
	var concurrency int
	if provider != nil {
		providerID = provider.Record.ID
		concurrency = provider.Record.Concurrency
		if provider.Record.ProxyID != nil && provider.Record.Proxy != nil {
			proxyURL = provider.Record.Proxy.URL()
		}
	}

	effectiveBody := responsesBody
	var result *forward.OpenAIResult
	health := s.grokHealth()
	target := &grok.ResponsesTarget{
		ProviderID:    providerID,
		Model:         upstreamModel,
		Enter:         s.Enter,
		PassRawStream: true,
		Exchange: grok.ResponsesExchange{
			SingleExchange: true,
			Build:          func([]byte) (*http.Request, error) { return initialReq, nil },
			Do: func(req *http.Request) (*http.Response, error) {
				resp, doErr := s.Transport.DoWithTLS(req, proxyURL, providerID, concurrency, s.TLSProfile(provider, tlsRouterMatch...))
				if doErr != nil {
					return nil, doErr
				}
				if resp == nil {
					return nil, errors.New("grok upstream returned nil response")
				}
				if resp.StatusCode != http.StatusBadRequest {
					return resp, nil
				}
				// Ordinary Chat payloads carry no encrypted_content and exit after one
				// exchange; Cursor input-shaped turns carrying unpacked xai ciphertext
				// strip rejected encrypted_content losslessly via jsontext and retry once.
				rawErr := s.Output.ReadErrorBody(resp)
				if resp.Body != nil {
					_ = resp.Body.Close()
				}
				shouldStrip := gatewayprovider.GrokBodyCodec().IsGrokInvalidEncryptedContentResponse(resp.StatusCode, rawErr) ||
					gatewayprovider.GrokBodyCodec().RequestHasGrokEncryptedReasoning(effectiveBody)
				if !shouldStrip {
					resp.Body = io.NopCloser(bytes.NewReader(rawErr))
					return resp, nil
				}
				retryBody, changed, dropped := protocolopenai.StripResponsesEncryptedReasoningWithCount(effectiveBody)
				if !changed {
					resp.Body = io.NopCloser(bytes.NewReader(rawErr))
					return resp, nil
				}
				effectiveBody = retryBody
				retryReq, buildErr := s.BuildResponsesRequest(upstreamCtx, c, provider, retryBody, token, cacheIdentity, true)
				if buildErr != nil {
					return nil, buildErr
				}
				upstream.StageSignatureRetry(ctx, exchange, dropped, 0)
				return s.Transport.DoWithTLS(retryReq, proxyURL, providerID, concurrency, s.TLSProfile(provider, tlsRouterMatch...))
			},
			ReadError: s.Output.ReadErrorBody,
			AfterExchange: func(exErr error) error {
				if exErr != nil {
					return s.Failure.Handle(ctx, c, provider, exErr, false)
				}
				return nil
			},
		},
		BeforeResponse: func(resp *http.Response, _ []byte) (bool, error) {
			if resp.StatusCode >= http.StatusBadRequest {
				return true, s.handleConvertedChatHTTPError(ctx, c, provider, resp, health, billingModel, upstreamModel)
			}
			if health != nil && provider != nil {
				health.ObserveResponse(ctx, provider.View(), resp.Header, resp.StatusCode, upstreamModel)
			}
			return false, nil
		},
		ReadResponse: func(resp *http.Response, _ upstream.AttemptInput, sink upstream.OutputSink) (upstream.ResponsesObservation, error) {
			if sink == nil && c != nil {
				sink = NewResponseSink(c)
			}
			output := upstream.NewDeferredOutputContext(sink)
			convOpts := s.Output.ConvertedOptions(c, provider, resp, protocol.ProtocolOpenAIChatCompletions, originalModel, billingModel, upstreamModel, len(body))
			var compatResult *openai.CompatResponseResult
			var readErr error
			if clientStream {
				compatResult, readErr = openai.ReadConvertedStreaming(resp, output, exchange, convOpts, originalModel, upstreamModel, startTime)
			} else {
				compatResult, readErr = openai.ReadConvertedBuffered(resp, output, exchange, convOpts, originalModel, upstreamModel, startTime)
			}
			result = gatewayprovider.ChatForwardResult(compatResult, billingModel)
			if result == nil {
				return upstream.ResponsesObservation{}, readErr
			}
			result.UpstreamResponseModel = compatResult.UpstreamResponseModel
			httpCommitted := c != nil && c.Writer != nil && c.Writer.Written()
			return upstream.ResponsesObservation{
				UpstreamResponseModel: result.UpstreamResponseModel,
				Usage:                 &result.Usage,
				HasUsage:              protocolopenai.OpenAIUsageHasTokens(&result.Usage),
				Served:                httpCommitted || result.ClientDisconnect,
				HTTPCommitted:         httpCommitted,
				RetryCommitted:        IsResponseCommitted(c),
				ClientDisconnected:    result.ClientDisconnect,
				ResponseID:            result.ResponseID,
				SearchCount:           result.SearchCount,
				ImageCount:            result.ImageCount,
				ImageFrameCount:       result.ImageFrameCount,
				ImageOutputSizes:      result.ImageOutputSizes,
			}, readErr
		},
	}

	var sink upstream.OutputSink
	if c != nil {
		sink = NewResponseSink(c)
	}
	nativeResult, err := (grok.ResponsesExecutor{}).Execute(upstreamCtx, upstream.AttemptInput{
		Protocol:      protocol.ProtocolOpenAIResponses,
		Body:          effectiveBody,
		ResponseModel: originalModel,
		Stream:        clientStream,
		Target:        target,
	}, sink)

	// Suppress partial usage settlement when upstream cyber-policy moderation
	// rejected the response mid-stream or inside an HTTP 200 failure frame.
	if GetOpsCyberPolicy(c) != nil {
		if err == nil {
			err = forward.ErrCyberPolicyForwarded
		}
		return nil, true, err
	}

	if result != nil {
		result.UpstreamEndpoint = grok.GrokChatResponsesEndpoint
		result.ResponseHeaders = nativeResult.UpstreamHeaders.Clone()
		if result.RequestID == "" {
			result.RequestID = requeststate.FirstNonEmpty(nativeResult.UpstreamHeaders.Get("x-request-id"), nativeResult.UpstreamHeaders.Get("xai-request-id"))
		}
		if result.UpstreamResponseModel == "" {
			result.UpstreamResponseModel = nativeResult.UpstreamResponseModel
		}
		if err == nil {
			if tier := ResolvedOpenAIUpstreamServiceTier(c, requeststate.ExtractOpenAIServiceTierFromBody(effectiveBody)); tier != nil {
				result.ServiceTier = tier
			}
			eff := requeststate.ExtractOpenAIReasoningEffortFromBody(effectiveBody)
			// Preserve pre-stripping client effort for billing when StripEffort
			// removed outgoing wire effort controls during conversion.
			if eff == nil && dialect.StripEffort {
				if norm := protocolopenai.NormalizeRecordedReasoningEffort(exchange.Info().Effort); norm != "" {
					eff = &norm
				}
			}
			if eff == nil {
				eff = requeststate.ExtractOpenAIReasoningEffortFromBody(body)
			}
			result.ReasoningEffort = eff
		}
	}
	return result, true, err
}

// grokHealth returns the Grok health observer configured on s, falling back to
// s.Output.GrokHealth when s.Health is nil. Nil executors or unconfigured
// observers return nil so callers skip health mutations safely.
func (s *GrokExecutor) grokHealth() *provideradapter.GrokHealth {
	if s == nil {
		return nil
	}
	if s.Health != nil {
		return s.Health
	}
	if s.Output != nil {
		return s.Output.GrokHealth
	}
	return nil
}

// handleConvertedChatHTTPError classifies an upstream HTTP >= 400 response for
// ChatResponses and applies Grok provider health policy at most once per
// attempt. Request-scoped cyber-policy, content-policy, invalid-parameter, and
// oversize rejections write terminal Chat errors directly without penalizing
// provider health or scheduling cooldowns.
//
// Remaining upstream statuses evaluate ApplyGrokExecutionHealth once, append a
// single OpsUpstreamErrorEvent, and either return an UpstreamFailoverError for
// outer provider switching or present a mapped Chat JSON error envelope.
func (s *GrokExecutor) handleConvertedChatHTTPError(
	ctx context.Context,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	resp *http.Response,
	health *provideradapter.GrokHealth,
	billingModel, upstreamModel string,
) error {
	rawBody := s.Output.ReadErrorBody(resp)
	respBody := s.Output.redact(ctx, provider, rawBody)
	upstreamReqID := requeststate.FirstNonEmpty(resp.Header.Get("x-request-id"), resp.Header.Get("xai-request-id"))

	// Request-scoped cyber-policy rejections record moderation telemetry and write
	// Chat rejections before provider health can penalize the account.
	if hit, code, cyberMsg := openai.DetectOpenAICyberPolicy(respBody); hit {
		MarkOpsCyberPolicy(c, moderationflow.Mark{
			Code:           code,
			Message:        cyberMsg,
			Body:           logredact.TruncateUTF8(string(respBody), 4096),
			UpstreamStatus: resp.StatusCode,
		})
		SetOpsUpstreamError(c, resp.StatusCode, cyberMsg, logredact.TruncateUTF8(string(respBody), 2048))
		clientMsg := cyberMsg
		if clientMsg == "" {
			clientMsg = "Request blocked by upstream cyber-security policy"
		}
		WriteForwardChatError(c, resp.StatusCode, "invalid_request_error", clientMsg)
		if cyberMsg == "" {
			return fmt.Errorf("openai cyber_policy: %d", resp.StatusCode)
		}
		return fmt.Errorf("openai cyber_policy: %s", cyberMsg)
	}

	// Grok content-policy refusals belong to the caller's prompt rather than the
	// account, so rewrite status to 403 and skip provider health penalties.
	if grok.IsGrokContentPolicyRejection(resp.StatusCode, respBody) {
		clientMsg := gatewayprovider.GrokContentPolicyClientMessage(respBody)
		SetOpsUpstreamError(c, resp.StatusCode, clientMsg, logredact.TruncateUTF8(string(respBody), 2048))
		WriteForwardChatError(c, http.StatusForbidden, "invalid_request_error", clientMsg)
		return fmt.Errorf("grok content policy rejection: %s", clientMsg)
	}

	upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(respBody)))
	if upstreamMsg == "" {
		upstreamMsg = fmt.Sprintf("xAI upstream returned status %d", resp.StatusCode)
	}

	upstreamDetail := ""
	if s.Output != nil && s.Output.Options.Configured && s.Output.Options.LogUpstreamErrorBody {
		maxBytes := s.Output.Options.LogUpstreamErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		// Bound optional Ops diagnostic body detail on a UTF-8 character boundary
		// while retaining the full redacted respBody for error classification below.
		upstreamDetail = logredact.TruncateUTF8(string(respBody), maxBytes)
	}
	SetOpsUpstreamError(c, resp.StatusCode, upstreamMsg, upstreamDetail)

	var platform, providerName string
	var providerID int64
	if provider != nil {
		platform = provider.Record.Platform
		providerID = provider.Record.ID
		providerName = provider.Record.Name
	}

	// Client parameter rejections other than Grok decoder-compatibility failures
	// return HTTP 400 directly so malformed caller inputs never penalize accounts.
	if !grok.IsGrokDecoderCompatibilityError(resp.StatusCode, respBody) &&
		openai.IsOpenAIClientInvalidRequestError(resp.StatusCode, upstreamMsg, respBody) {
		AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
			Platform:           platform,
			ProviderID:         providerID,
			ProviderName:       providerName,
			UpstreamStatusCode: resp.StatusCode,
			UpstreamRequestID:  upstreamReqID,
			Kind:               "http_error",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
		WriteForwardChatErrorBody(c, http.StatusBadRequest, respBody)
		return fmt.Errorf("upstream invalid request: %d message=%s", resp.StatusCode, upstreamMsg)
	}
	// Oversized request rejections originate from the client payload and return
	// immediately without marking the selected Grok provider unhealthy.
	if gatewayprovider.IsOpenAIRequestBodyTooLargeError(resp.StatusCode, upstreamMsg, respBody) {
		WriteForwardChatErrorBody(c, resp.StatusCode, respBody)
		return fmt.Errorf("upstream request body too large: %d message=%s", resp.StatusCode, upstreamMsg)
	}

	cooldownModel := upstreamModel
	if strings.TrimSpace(cooldownModel) == "" {
		cooldownModel = billingModel
	}
	decision := gatewayprovider.ApplyGrokExecutionHealth(ctx, health, provider, resp.StatusCode, resp.Header, respBody, "", cooldownModel)
	kind := "http_error"
	if decision.ShouldFailover(gatewayprovider.ExecutionErrorPolicy(provider), resp.StatusCode, gatewayprovider.ShouldFailoverGrokResponse(resp.StatusCode, respBody)) {
		kind = "failover"
	}
	AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
		Platform:           platform,
		ProviderID:         providerID,
		ProviderName:       providerName,
		UpstreamStatusCode: resp.StatusCode,
		UpstreamRequestID:  upstreamReqID,
		Kind:               kind,
		Message:            upstreamMsg,
		Detail:             upstreamDetail,
	})

	if decision.ShouldReturnGenericError() {
		WriteForwardChatError(c, http.StatusInternalServerError, "api_error", "Upstream gateway error")
		return fmt.Errorf("upstream error: %d (not in custom error codes) message=%s", resp.StatusCode, upstreamMsg)
	}

	// Mark team-model quota cooldowns on quota-exhaustion evidence while leaving
	// transient model-capacity responses eligible for ordinary provider selection.
	if grok.ShouldMarkGrokTeamModelRateLimit(resp.StatusCode, respBody) {
		providercore.MarkGrokTeamModelRateLimit(
			gatewayprovider.ExecutionRecord(provider),
			upstreamModel,
			providercore.ResolveGrokTeamRateLimitUntil(time.Now().Add(10*time.Minute), time.Now()),
		)
	}

	if kind == "failover" {
		retryable, retryDelay, retryDeadline, retryMax := gatewayprovider.GrokSameProviderRetryMetadata(provider, resp.StatusCode, respBody)
		return &forward.UpstreamFailoverError{
			StatusCode:                resp.StatusCode,
			ResponseBody:              respBody,
			ResponseHeaders:           resp.Header.Clone(),
			RetryableOnSameProvider:   retryable || decision.RetryableOnSameProvider(gatewayprovider.ExecutionErrorPolicy(provider), resp.StatusCode),
			RequestScopedTransient:    retryable && resp.StatusCode == http.StatusTooManyRequests,
			SameProviderRetryDelay:    retryDelay,
			SameProviderRetryDeadline: retryDeadline,
			SameProviderRetryMax:      retryMax,
		}
	}

	if status, errType, errMsg, matched := ApplyErrorPassthroughRule(
		c, platform, resp.StatusCode, respBody,
		http.StatusBadGateway, "api_error", "Upstream request failed",
	); matched {
		if errMsg == "" {
			errMsg = upstreamMsg
		}
		WriteForwardChatError(c, status, errType, errMsg)
		return fmt.Errorf("upstream error: %d (passthrough rule matched) message=%s", resp.StatusCode, errMsg)
	}

	errType := "api_error"
	switch resp.StatusCode {
	case http.StatusBadRequest:
		errType = "invalid_request_error"
	case http.StatusNotFound:
		errType = "not_found_error"
	case http.StatusTooManyRequests:
		errType = "rate_limit_error"
	}
	WriteForwardChatError(c, resp.StatusCode, errType, upstreamMsg)
	return fmt.Errorf("upstream error: %d %s", resp.StatusCode, upstreamMsg)
}

// inspectGrokChatClientBody scans top-level model, stream, instructions,
// messages, input, and image-input flags from raw JSON bytes via jsontext
// while tolerating invalid UTF-8 in strings. Non-object roots, syntax errors,
// or trailing tokens return an error without mutating body.
func inspectGrokChatClientBody(body []byte) (grokChatClientBodyInfo, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return grokChatClientBodyInfo{}, err
	}
	if tok.Kind() != '{' {
		return grokChatClientBodyInfo{}, errors.New("JSON payload must be an object")
	}

	var info grokChatClientBodyInfo
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return grokChatClientBodyInfo{}, kErr
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return grokChatClientBodyInfo{}, vErr
		}
		switch key {
		case "model":
			_ = jsonv2.Unmarshal(val, &info.model, jsontext.AllowInvalidUTF8(true))
		case "stream":
			_ = jsonv2.Unmarshal(val, &info.stream, jsontext.AllowInvalidUTF8(true))
		case "instructions":
			var instr string
			if jsonv2.Unmarshal(val, &instr, jsontext.AllowInvalidUTF8(true)) == nil {
				info.instructions = strings.TrimSpace(instr)
			}
		case "messages":
			info.hasMessages = true
			if jsonBytesMayContainImageInput(val) {
				info.hasImageInput = true
			}
		case "input":
			info.hasInput = true
			if jsonBytesMayContainImageInput(val) {
				info.hasImageInput = true
			}
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return grokChatClientBodyInfo{}, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return grokChatClientBodyInfo{}, errors.New("unexpected trailing JSON token")
	}
	return info, nil
}

// jsonBytesMayContainImageInput scans raw JSON tokens via jsontext and reports
// whether any nested object declares type "image_url" or "input_image" or a
// non-null "image_url" property. Malformed JSON fragments return false.
func jsonBytesMayContainImageInput(raw jsontext.Value) bool {
	if len(raw) == 0 || (!bytes.Contains(raw, []byte("image")) && !bytes.Contains(raw, []byte(`\u`))) {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	return scanDecoderForImageInput(dec)
}

// scanDecoderForImageInput recursively consumes one JSON value from dec and
// reports whether any nested object carries a Chat or Responses image-input
// marker. Malformed tokens terminate scanning early and return false.
func scanDecoderForImageInput(dec *jsontext.Decoder) bool {
	switch dec.PeekKind() {
	case '{':
		if _, err := dec.ReadToken(); err != nil {
			return false
		}
		found := false
		for dec.PeekKind() != '}' {
			kTok, err := dec.ReadToken()
			if err != nil {
				return false
			}
			key := kTok.String()
			switch key {
			case "type":
				val, vErr := dec.ReadValue()
				if vErr != nil {
					return false
				}
				var typ string
				if jsonv2.Unmarshal(val, &typ, jsontext.AllowInvalidUTF8(true)) == nil {
					switch strings.ToLower(strings.TrimSpace(typ)) {
					case "image_url", "input_image":
						found = true
					}
				}
			case "image_url":
				val, vErr := dec.ReadValue()
				if vErr != nil {
					return false
				}
				trimmed := bytes.TrimSpace(val)
				if len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null")) && !bytes.Equal(trimmed, []byte(`""`)) {
					found = true
				}
			default:
				if found {
					if err := dec.SkipValue(); err != nil {
						return false
					}
				} else if scanDecoderForImageInput(dec) {
					found = true
				}
			}
		}
		_, _ = dec.ReadToken()
		return found

	case '[':
		if _, err := dec.ReadToken(); err != nil {
			return false
		}
		found := false
		for dec.PeekKind() != ']' {
			if found {
				if err := dec.SkipValue(); err != nil {
					return false
				}
			} else if scanDecoderForImageInput(dec) {
				found = true
			}
		}
		_, _ = dec.ReadToken()
		return found

	default:
		_ = dec.SkipValue()
		return false
	}
}

// patchGrokConvertedResponsesBody applies mapped model, forced upstream
// streaming, canonical service_tier normalization, optional Trae instructions,
// and Cursor extension deletions onto ordered raw JSON object members. Untouched
// fields and 64-bit integer literals survive verbatim, while malformed JSON
// inputs return an error without mutating body.
func patchGrokConvertedResponsesBody(
	body []byte,
	upstreamModel string,
	injectInstructions string,
	deleteFields []string,
) ([]byte, error) {
	members, err := readGrokJSONObjectMembers(body)
	if err != nil {
		return nil, err
	}
	for _, delKey := range deleteFields {
		members = deleteGrokJSONMember(members, delKey)
	}
	if upstreamModel != "" {
		rawModel, mErr := jsonv2.Marshal(upstreamModel, jsontext.AllowInvalidUTF8(true))
		if mErr != nil {
			return nil, mErr
		}
		members = upsertGrokJSONMember(members, "model", rawModel)
	}
	members = upsertGrokJSONMember(members, "stream", jsontext.Value("true"))

	if injectInstructions != "" {
		existingInstr := ""
		if rawExisting, ok := findGrokJSONMember(members, "instructions"); ok {
			_ = jsonv2.Unmarshal(rawExisting, &existingInstr, jsontext.AllowInvalidUTF8(true))
		}
		if strings.TrimSpace(existingInstr) == "" {
			rawInstr, iErr := jsonv2.Marshal(injectInstructions, jsontext.AllowInvalidUTF8(true))
			if iErr != nil {
				return nil, iErr
			}
			members = upsertGrokJSONMember(members, "instructions", rawInstr)
		}
	}

	if rawTier, hasTier := findGrokJSONMember(members, "service_tier"); hasTier {
		var tierStr string
		if jsonv2.Unmarshal(rawTier, &tierStr, jsontext.AllowInvalidUTF8(true)) == nil && tierStr != "" {
			norm := protocolopenai.ServiceTierValue(tierStr)
			if norm == "" {
				members = deleteGrokJSONMember(members, "service_tier")
			} else if norm != tierStr {
				rawNorm, tErr := jsonv2.Marshal(norm, jsontext.AllowInvalidUTF8(true))
				if tErr != nil {
					return nil, tErr
				}
				members = upsertGrokJSONMember(members, "service_tier", rawNorm)
			}
		}
	}

	return encodeGrokJSONObjectMembers(members)
}

// grokChatCacheIntentBodyBytes returns an independent copy of body with
// ineffective "auto" or "none" tool_choice strings removed when tools is absent.
// Member order and 64-bit integer literals remain intact via jsontext, and
// malformed root objects return an error.
func grokChatCacheIntentBodyBytes(body []byte) ([]byte, error) {
	members, err := readGrokJSONObjectMembers(body)
	if err != nil {
		return nil, err
	}
	if _, hasTools := findGrokJSONMember(members, "tools"); hasTools {
		return bytes.Clone(body), nil
	}
	rawChoice, hasChoice := findGrokJSONMember(members, "tool_choice")
	if !hasChoice || len(rawChoice) == 0 || rawChoice.Kind() != '"' {
		return bytes.Clone(body), nil
	}
	var choice string
	if jsonv2.Unmarshal(rawChoice, &choice, jsontext.AllowInvalidUTF8(true)) != nil ||
		(choice != "auto" && choice != "none") {
		return bytes.Clone(body), nil
	}
	members = deleteGrokJSONMember(members, "tool_choice")
	return encodeGrokJSONObjectMembers(members)
}

// readGrokJSONObjectMembers parses a JSON object into ordered raw members with
// cloned value slices and invalid UTF-8 tolerated. Non-object roots or syntax
// errors return a nil slice and the underlying jsontext error.
func readGrokJSONObjectMembers(raw []byte) ([]rawGrokJSONMember, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '{' {
		return nil, errors.New("expected JSON object")
	}
	var members []rawGrokJSONMember
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return nil, kErr
		}
		name := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return nil, vErr
		}
		members = append(members, rawGrokJSONMember{
			name:  name,
			value: bytes.Clone(val),
		})
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	return members, nil
}

// encodeGrokJSONObjectMembers serializes ordered raw JSON object members into
// compact object bytes while permitting invalid UTF-8 strings without error.
// Member write failures abort serialization and return the encoder error.
func encodeGrokJSONObjectMembers(members []rawGrokJSONMember) ([]byte, error) {
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

// findGrokJSONMember scans members from back to front and returns the last raw
// value slice matching name under last-wins semantics. Absent keys return nil and false.
func findGrokJSONMember(members []rawGrokJSONMember, name string) (jsontext.Value, bool) {
	for _, m := range slices.Backward(members) {
		if m.name == name {
			return m.value, true
		}
	}
	return nil, false
}

// upsertGrokJSONMember compacts members in place, replacing the first entry
// matching name with a cloned val slice and dropping later duplicates. When
// name is absent, a clipped append adds the new member at the end.
func upsertGrokJSONMember(members []rawGrokJSONMember, name string, val jsontext.Value) []rawGrokJSONMember {
	replaced := false
	out := members[:0]
	for _, m := range members {
		if m.name == name {
			if !replaced {
				out = append(out, rawGrokJSONMember{name: name, value: bytes.Clone(val)})
				replaced = true
			}
			continue
		}
		out = append(out, m)
	}
	if !replaced {
		out = append(slices.Clip(out), rawGrokJSONMember{name: name, value: bytes.Clone(val)})
	}
	return out
}

// deleteGrokJSONMember removes all members matching name in place via
// slices.DeleteFunc. Remaining entries keep their original relative order.
func deleteGrokJSONMember(members []rawGrokJSONMember, name string) []rawGrokJSONMember {
	return slices.DeleteFunc(members, func(m rawGrokJSONMember) bool {
		return m.name == name
	})
}
