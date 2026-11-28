package openaiforward

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

	"go.uber.org/zap"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	protocolopenai "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/grok"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// responses_converted.go executes merged IR protocol conversion onto OpenAI-
// compatible Responses upstreams for both Anthropic Messages and OpenAI Chat
// Completions clients.
// Cross-protocol Responses execution requires coordinated model mapping,
// lossless prompt-cache seeding, Codex OAuth adaptation, continuation recovery,
// and deferred stream commitment without round-tripping client JSON through
// subset structs that discard unmodeled Extra fields.
//
// RunConvertedResponses parameterizes client-specific admission, continuation
// trimming, and Grok cache routing by source protocol while sharing one
// convert.Exchange request pipeline and the merged ReadConvertedStreaming and
// ReadConvertedBuffered response readers. Cursor Chat requests carrying top-
// level input without messages preserve native Responses payloads on Responses
// upstreams and convert through convert.NewExchange when routed elsewhere.

// CursorResponsesUnsupportedFields identifies extension fields removed from raw
// Responses-shaped Chat input before tier and OAuth compatibility preparation.
var CursorResponsesUnsupportedFields = []string{"prompt_cache_retention", "safety_identifier", "metadata", "stream_options"}

// convertedClientBodyInfo summarizes top-level scalar and shape observations
// scanned from an incoming client JSON body via jsontext.
type convertedClientBodyInfo struct {
	model        string // decoded top-level model string
	stream       bool   // decoded top-level stream boolean
	hasMessages  bool   // whether top-level "messages" member is present
	hasInput     bool   // whether top-level "input" member is present
	clientEffort string // decoded Anthropic output_config.effort when present
	// thinkingDisabled reports whether Anthropic thinking explicitly disables
	// reasoning; when true, post-conversion effort reconciliation is skipped so
	// clientEffort cannot overwrite capability-guarded disabled-thinking fallbacks.
	thinkingDisabled bool
}

// rawConvertedMember holds one ordered JSON object member's decoded name and
// cloned raw value slice during lossless request-body patching.
type rawConvertedMember struct {
	name  string         // decoded JSON object key
	value jsontext.Value // cloned raw JSON value bytes
}

// convertedResponsesBodyPatch specifies lossless top-level JSON edits applied
// after convert.Exchange.UpstreamRequest or on raw Cursor Responses bodies.
type convertedResponsesBodyPatch struct {
	Model                string                                  // target upstream model when non-empty
	ForceStream          bool                                    // sets top-level stream=true when enabled
	DeleteFields         []string                                // top-level member names to remove
	OnDeleteField        func(key string, rawVal jsontext.Value) // optional observer invoked before removing an existing DeleteFields member
	SetServiceTier       string                                  // explicit service_tier value to set when non-empty
	NormalizeServiceTier bool                                    // canonicalizes existing service_tier via ServiceTierValue
	PreviousResponseID   string                                  // previous_response_id value to attach when non-empty
	InjectPromptCacheKey string                                  // prompt_cache_key injected when absent or blank
	ReconcileEffort      func(converted string) string           // optional callback rewriting reasoning.effort
}

// RunConvertedResponses dispatches or converts a Messages or Chat request onto
// an OpenAI-compatible Responses upstream through convert.Exchange and reads
// streaming or buffered client output via ReadConvertedStreaming or ReadConvertedBuffered.
//
// Alternate routes (Anthropic native, raw Chat, or Grok Chat) delegate before
// Responses conversion, converting Cursor input-shaped Chat bodies into Chat
// Completions bytes when targeting non-Responses endpoints. Provider-local
// recoveries rebuild from the original client body with context guards, while
// continuation IDs, digest prefixes, billing controls, and Codex quota headers
// commit only after response reading succeeds without cyber-policy rejection.
func RunConvertedResponses(
	ctx context.Context,
	source protocol.ProtocolID,
	body []byte,
	promptCacheKey string,
	defaultMappedModel string,
	compatPromptCacheTenantIsolated bool,
	p ConvertedResponsesPorts,
) (*Result, error) {
	switch source {
	case protocol.ProtocolAnthropicMessages:
		return runConvertedMessagesToResponses(ctx, body, promptCacheKey, defaultMappedModel, protocolanthropic.SignatureRetryLosses{}, p)
	case protocol.ProtocolOpenAIChatCompletions:
		return runConvertedChatToResponses(ctx, body, promptCacheKey, defaultMappedModel, compatPromptCacheTenantIsolated, p)
	default:
		return nil, fmt.Errorf("unsupported converted responses source protocol %q", source)
	}
}

// runConvertedMessagesToResponses executes the Messages-source branch of
// RunConvertedResponses using lossless jsontext cache seeds, replay guards, and
// continuation input trimming. Non-Responses routes delegate before parsing,
// while invalid incoming effort strings write a client 400 rejection.
//
// Outgoing requests always stream SSE from the Responses upstream while
// honoring clientStream for downstream Messages SSE or buffered JSON delivery.
// Provider-local recoveries retry Grok invalid encrypted_content once in place
// and recursively rebuild from callerPromptCacheKey on Agent Identity task
// expiration, unavailable previous_response_id, or Grok thinking-signature
// rejection before binding response IDs and digest chains on success.
func runConvertedMessagesToResponses(
	ctx context.Context,
	body []byte,
	promptCacheKey string,
	defaultMappedModel string,
	sourceLosses protocolanthropic.SignatureRetryLosses,
	p ConvertedResponsesPorts,
) (*Result, error) {
	profile, route, err := p.PrepareMessages(ctx)
	if err != nil {
		return nil, err
	}
	if route != DispatchResponses {
		return p.DispatchMessages(ctx, route, body, promptCacheKey, defaultMappedModel)
	}
	startTime := time.Now()

	clientInfo, ok := inspectConvertedClientBody(body)
	if !ok {
		p.SourceError(protocol.ProtocolAnthropicMessages, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		parseErr := errors.New("parse anthropic request: invalid JSON object")
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(parseErr)
	}
	if err := requeststate.ValidateOpenAIReasoningEffort(body, clientInfo.model); err != nil {
		p.SourceError(protocol.ProtocolAnthropicMessages, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(err)
	}

	originalModel := clientInfo.model
	normalizedModel := clientInfo.model
	clientStream := clientInfo.stream
	billingModel := p.BillingModel(normalizedModel, defaultMappedModel)
	upstreamModel := p.UpstreamModel(billingModel)

	promptCacheKey = strings.TrimSpace(promptCacheKey)
	callerPromptCacheKey := promptCacheKey
	apiKeyID := p.APIKeyID()
	anthropicDigestChain := ""
	anthropicMatchedDigestChain := ""
	compatPromptCacheInjected := false

	// Reuse Claude Code's session seed for Grok prompt caching when no explicit
	// prompt_cache_key was supplied by the caller.
	if promptCacheKey == "" && profile.Platform == "grok" {
		if sessionSeed := p.ClaudeSession(body); sessionSeed != "" {
			promptCacheKey = sessionSeed
			compatPromptCacheInjected = true
		} else if sessionSeed := session.AnthropicMetadataPromptCacheKeyBytes(body); sessionSeed != "" {
			promptCacheKey = sessionSeed
			compatPromptCacheInjected = true
		}
	}
	// Derive automatic compatibility cache keys from lossless raw-byte metadata,
	// ephemeral cache_control anchors, or digest-prefix chains before conversion.
	if promptCacheKey == "" && gatewayprovider.ShouldAutoInjectPromptCacheKeyForCompat(upstreamModel) {
		promptCacheKey = session.AnthropicMetadataPromptCacheKeyBytes(body)
		if promptCacheKey == "" {
			promptCacheKey = gatewayprovider.DeriveAnthropicCacheControlPromptCacheKeyBytes(body)
		}
		if promptCacheKey == "" {
			anthropicDigestChain = session.BuildAnthropicDigestChainBytes(body)
			if reusedKey, matchedChain := p.FindDigestKey(apiKeyID, anthropicDigestChain); reusedKey != "" {
				promptCacheKey = reusedKey
				anthropicMatchedDigestChain = matchedChain
			} else {
				promptCacheKey = session.AnthropicDigestPromptCacheKey(anthropicDigestChain)
			}
		}
		compatPromptCacheInjected = promptCacheKey != ""
	}

	compatReplayTrimmed := false
	compatMessagesAfterTrim := 0
	compatReplayGuardEnabled := gatewayprovider.ShouldAutoInjectPromptCacheKeyForCompat(upstreamModel)
	compatContinuationEnabled := p.ContinuationEnabled(upstreamModel)
	previousResponseID := ""
	if compatContinuationEnabled {
		previousResponseID = p.ResponseID(ctx, promptCacheKey)
	}
	compatContinuationDisabled := compatContinuationEnabled &&
		p.ContinuationDisabled(ctx, promptCacheKey)
	compatTurnState := ""

	effectiveBody := body
	// Preserve full history on Codex OAuth so session_id and turn-state caching
	// grow across turns; trim stateless non-Codex replays losslessly via jsontext.
	if compatReplayGuardEnabled && !profile.UsesCodex && previousResponseID == "" && !compatContinuationDisabled {
		effectiveBody, compatMessagesAfterTrim, compatReplayTrimmed = gatewayprovider.ApplyAnthropicCompatFullReplayGuardBytes(body)
	}

	dialect, err := p.ResolveResponsesDialect(upstreamModel, effectiveBody)
	if err != nil {
		return nil, fmt.Errorf("resolve responses dialect: %w", err)
	}
	exchange, err := convert.NewExchange(effectiveBody, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  dialect,
		},
		ClientModel: originalModel,
		Runtime:     p.ConversionRuntime(),
	})
	if err != nil {
		p.SourceError(protocol.ProtocolAnthropicMessages, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("convert anthropic to responses: %w", err))
	}
	responsesBody, err := exchange.UpstreamRequest()
	if err != nil {
		p.SourceError(protocol.ProtocolAnthropicMessages, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("encode converted responses request: %w", err))
	}
	// A recursive Grok recovery may already have stripped the client Messages
	// body. Those losses belong to this rebuilt request across continuation retries.
	if sourceLosses.Tokens > 0 || sourceLosses.Reasoning > 0 {
		exchange.SetSignatureRetryLosses(sourceLosses.Tokens, sourceLosses.Reasoning)
	}
	upstream.ObserveExchangeConversion(ctx, exchange)

	tierOverride := ""
	if p.BetaFast() {
		tierOverride = "priority"
	}
	var reconcileEffort func(string) string
	// Skip MessagesEffortValue reconciliation when thinking is explicitly disabled
	// so an accompanying output_config.effort cannot override the dialect fallback.
	if !clientInfo.thinkingDisabled && !dialect.StripEffort {
		reconcileEffort = func(converted string) string {
			return p.MessagesEffortValue(clientInfo.clientEffort, upstreamModel, converted)
		}
	}
	responsesBody, _, err = patchConvertedResponsesBody(responsesBody, convertedResponsesBodyPatch{
		Model:              upstreamModel,
		ForceStream:        true,
		SetServiceTier:     tierOverride,
		PreviousResponseID: previousResponseID,
		ReconcileEffort:    reconcileEffort,
	})
	if err != nil {
		return nil, fmt.Errorf("patch converted responses request: %w", err)
	}
	isStream := true

	// Continuation attaches previous_response_id and trims input[] losslessly to
	// the newest turn and its paired function calls.
	if previousResponseID != "" {
		responsesBody, _ = gatewayprovider.TrimCompatResponsesInputToLatestTurnBytes(responsesBody)
	}
	if compatReplayGuardEnabled && !profile.UsesCodex {
		responsesBody, _ = gatewayprovider.AppendOpenAICompatClaudeCodeTodoGuardBytes(responsesBody)
	}

	if profile.UsesCodex && profile.Platform != "grok" {
		var codexErr error
		responsesBody, upstreamModel, promptCacheKey, codexErr = applyConvertedCodexTransform(responsesBody, convertedCodexTransformInput{
			ForMessages:      true,
			OriginalModel:    originalModel,
			NormalizedModel:  normalizedModel,
			BillingModel:     billingModel,
			UpstreamModel:    upstreamModel,
			PromptCacheKey:   promptCacheKey,
			APIKeyID:         apiKeyID,
			ForcedTemplate:   p.ForcedTemplate(),
			ToolNameReverse:  p.ToolNameReverse,
			RenderTemplate:   p.ForcedInstructions,
			ProviderIdentity: p.ProviderIdentity,
		})
		if codexErr != nil {
			if valErr, ok := errors.AsType[*convertedCodexValidationError](codexErr); ok {
				p.SourceError(protocol.ProtocolAnthropicMessages, http.StatusBadRequest, "invalid_request_error", valErr.Error())
				return nil, valErr.Unwrap()
			}
			return nil, codexErr
		}
		if gatewayprovider.ShouldAutoInjectPromptCacheKeyForCompat(upstreamModel) {
			compatTurnState = p.TurnState(ctx, promptCacheKey)
		}
		isStream = true
	}

	logFields := []zap.Field{
		zap.Int64("provider_id", profile.ID),
		zap.String("original_model", originalModel),
		zap.String("normalized_model", normalizedModel),
		zap.String("billing_model", billingModel),
		zap.String("upstream_model", upstreamModel),
		zap.Bool("stream", isStream),
		zap.Bool("compat_continuation_supported", profile.ContinuationSupported),
	}
	if compatPromptCacheInjected {
		logFields = append(logFields,
			zap.Bool("compat_prompt_cache_key_injected", true),
			zap.String("compat_prompt_cache_key_sha256", upstream.HashSensitiveValueForLog(promptCacheKey)),
		)
	}
	if compatReplayTrimmed {
		logFields = append(logFields,
			zap.Bool("compat_full_replay_trimmed", true),
			zap.Int("compat_messages_after_trim", compatMessagesAfterTrim),
		)
	}
	if previousResponseID != "" {
		logFields = append(logFields,
			zap.Bool("compat_previous_response_id_attached", true),
			zap.String("compat_previous_response_id", gatewayprovider.TruncateOpenAIWSLogValue(previousResponseID, gatewayprovider.OpenAIWSIDValueMaxLen)),
		)
	}
	if compatTurnState != "" {
		logFields = append(logFields, zap.Bool("compat_turn_state_attached", true))
	}
	logging.L().Debug("openai messages (ir): model mapping applied", logFields...)

	if profile.Type == "apikey" {
		if trimmedKey := strings.TrimSpace(promptCacheKey); trimmedKey != "" {
			responsesBody, _, err = patchConvertedResponsesBody(responsesBody, convertedResponsesBodyPatch{
				InjectPromptCacheKey: trimmedKey,
			})
			if err != nil {
				return nil, fmt.Errorf("inject prompt_cache_key: %w", err)
			}
		}
	}

	if profile.Platform == "openai" {
		policyBody, changed, policyErr := p.ApplyEffort(ctx, responsesBody)
		if policyErr != nil {
			return nil, policyErr
		}
		if changed {
			responsesBody = policyBody
		}
	}

	updatedBody, policyErr := p.ApplyFast(ctx, upstreamModel, responsesBody)
	if policyErr != nil {
		return nil, policyErr
	}
	responsesBody = updatedBody

	grokCacheIdentity := ""
	if profile.Platform == "grok" {
		grokIntentBody := responsesBody
		grokCacheIdentity = p.GrokCacheIdentity(grokIntentBody, promptCacheKey, upstreamModel)
		patchedBody, patchErr := p.PatchGrokBody(grokIntentBody, upstreamModel)
		if patchErr != nil {
			return nil, patchErr
		}
		responsesBody, patchErr = grok.ApplyGrokResponsesCacheIdentity(patchedBody, grokIntentBody, grokCacheIdentity, profile.GrokOAuth)
		if patchErr != nil {
			return nil, fmt.Errorf("apply grok prompt cache identity: %w", patchErr)
		}
		responsesBody, patchErr = p.GrokFreeToolRoute(responsesBody, grokIntentBody, grokCacheIdentity)
		if patchErr != nil {
			return nil, fmt.Errorf("apply grok Free function-tool cache route: %w", patchErr)
		}
	}

	token, err := p.Credential(ctx)
	if err != nil {
		return nil, fmt.Errorf("get access token: %w", err)
	}

	if profile.UsesCodex && profile.Platform != "grok" {
		p.BindMessagesBridge(true)
	}
	upstreamCtx, releaseUpstreamCtx := gatewayprovider.DetachUpstreamContext(ctx)
	upstreamReq, err := p.Build(ctx, upstreamCtx, responsesBody, token, isStream, promptCacheKey, grokCacheIdentity)
	releaseUpstreamCtx()
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}

	if profile.Platform != "grok" && promptCacheKey != "" {
		isolatedSessionID := p.IsolatedSessionID(apiKeyID, promptCacheKey)
		upstreamReq.Header.Set("session_id", isolatedSessionID)
		if upstreamReq.Header.Get("conversation_id") != "" {
			upstreamReq.Header.Set("conversation_id", isolatedSessionID)
		}
	}
	if profile.UsesCodex && profile.Platform != "grok" {
		openai.EnsureCodexIdentityHeaders(upstreamReq.Header)
		openai.EnforceCodexIdentityHeaders(upstreamReq.Header)
	}
	if profile.UsesCodex && promptCacheKey != "" && strings.TrimSpace(p.Header("conversation_id")) == "" {
		upstreamReq.Header.Del("conversation_id")
	}
	if compatTurnState != "" && upstreamReq.Header.Get("x-codex-turn-state") == "" {
		upstreamReq.Header.Set("x-codex-turn-state", compatTurnState)
	}

	p.PrepareTransport()
	// Retry once on Grok HTTP 400 after stripping invalid replayed encrypted
	// reasoning content before treating the error as terminal or switching providers.
	var resp *http.Response
	var droppedCiphertexts int
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if profile.Platform != "grok" {
				break
			}
			upstreamCtxRetry, releaseRetry := gatewayprovider.DetachUpstreamContext(ctx)
			upstreamReq, err = p.Build(ctx, upstreamCtxRetry, responsesBody, token, isStream, promptCacheKey, grokCacheIdentity)
			releaseRetry()
			if err != nil {
				return nil, fmt.Errorf("build grok retry request: %w", err)
			}
			upstream.StageSignatureRetry(ctx, exchange, sourceLosses.Tokens+droppedCiphertexts, sourceLosses.Reasoning)
		}
		resp, err = p.Send(upstreamReq)
		if err != nil {
			return nil, p.TransportError(ctx, err)
		}
		if profile.Platform != "grok" || attempt > 0 || resp.StatusCode != http.StatusBadRequest {
			break
		}
		respBody := p.ReadErrorBody(resp)
		if resp.Body != nil {
			_ = resp.Body.Close()
		}
		shouldStrip := gatewayprovider.GrokBodyCodec().IsGrokInvalidEncryptedContentResponse(resp.StatusCode, respBody) ||
			gatewayprovider.GrokBodyCodec().RequestHasGrokEncryptedReasoning(responsesBody)
		if !shouldStrip {
			resp.Body = io.NopCloser(bytes.NewReader(respBody))
			break
		}
		retryBody, changed, dropped := protocolopenai.StripResponsesEncryptedReasoningWithCount(responsesBody)
		if !changed {
			resp.Body = io.NopCloser(bytes.NewReader(respBody))
			break
		}
		responsesBody = retryBody
		droppedCiphertexts = dropped
		logging.L().Info("openai messages (ir): retrying after stripping invalid Grok encrypted_content",
			zap.Int64("provider_id", profile.ID),
			zap.Bool("cache_identity_present", strings.TrimSpace(grokCacheIdentity) != ""),
			zap.String("upstream_error_preview", gatewayprovider.TruncateOpenAIWSLogValue(string(respBody), 240)),
		)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, _ := p.ReadUpstreamError(resp)
		// Spend the single per-request Agent Identity task recovery before rebuilding
		// from client bytes in a fresh conversion scope so repeated task errors
		// proceed to standard error handling without summing prior attempt losses.
		if !requeststate.AgentTaskRecoveryTried(ctx) && p.IsAgentIdentity(ctx) && openai.IsAgentTaskInvalidHTTPResponse(resp.StatusCode, respBody) {
			if err := p.RecoverAgentTask(ctx); err != nil {
				return nil, fmt.Errorf("agent identity task recovery failed: %w", err)
			}
			retryCtx := telemetry.WithProtocolConversionScope(requeststate.WithAgentTaskRecovery(ctx))
			return runConvertedMessagesToResponses(retryCtx, body, callerPromptCacheKey, defaultMappedModel, sourceLosses, p)
		}
		respBody = p.RedactErrorBody(ctx, respBody)
		resp.Body = io.NopCloser(bytes.NewReader(respBody))
		upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(respBody)))
		// Clear or disable rejected previous_response_id state before re-running
		// full history conversion in an isolated conversion scope so attempt two
		// records only its own request losses instead of summing with attempt one.
		if previousResponseID != "" && (gatewayprovider.CompatPreviousResponseNotFound(resp.StatusCode, upstreamMsg, respBody) || gatewayprovider.CompatPreviousResponseUnsupported(resp.StatusCode, upstreamMsg, respBody)) {
			if gatewayprovider.CompatPreviousResponseUnsupported(resp.StatusCode, upstreamMsg, respBody) {
				p.DisableContinuation(ctx, promptCacheKey)
			} else {
				p.DeleteResponseID(ctx, promptCacheKey)
			}
			logging.L().Info("openai messages (ir): previous_response_id unavailable, retrying without continuation",
				zap.Int64("provider_id", profile.ID),
				zap.String("previous_response_id", gatewayprovider.TruncateOpenAIWSLogValue(previousResponseID, gatewayprovider.OpenAIWSIDValueMaxLen)),
				zap.String("upstream_model", upstreamModel),
				zap.Bool("compat_continuation_supported", profile.ContinuationSupported),
			)
			retryCtx := telemetry.WithProtocolConversionScope(ctx)
			return runConvertedMessagesToResponses(retryCtx, body, callerPromptCacheKey, defaultMappedModel, sourceLosses, p)
		}
		if profile.Platform == "grok" &&
			gatewayprovider.GrokBodyCodec().IsGrokInvalidEncryptedContentResponse(resp.StatusCode, respBody) &&
			!gatewayprovider.GrokBodyCodec().GrokEncryptedContentStripRetried(ctx) {
			// Strip rejected thinking signatures and empty redacted blocks losslessly via
			// jsontext in an isolated conversion scope so a subsequent Grok replay
			// retains readable thinking text without summing prior attempt losses.
			if strippedBody, strippedOK, losses := protocolanthropic.StripMessagesSignatureBlocksWithLosses(body, true); strippedOK {
				logging.L().Info("openai messages (ir): stripping thinking signatures for Grok failover retry",
					zap.Int64("provider_id", profile.ID),
				)
				retryCtx := telemetry.WithProtocolConversionScope(gatewayprovider.GrokBodyCodec().MarkGrokEncryptedContentStripRetried(ctx))
				return runConvertedMessagesToResponses(retryCtx, strippedBody, callerPromptCacheKey, defaultMappedModel, losses, p)
			}
		}
		if foErr := p.FailoverHTTP(ctx, resp, respBody, upstreamMsg, upstreamModel); foErr != nil {
			return nil, foErr
		}
		return p.SourceErrorResponse(protocol.ProtocolAnthropicMessages, resp, billingModel)
	}

	if profile.Platform == "grok" && profile.Type == "oauth" && !profile.Shadow {
		p.UpdateGrokUsage(ctx, upstreamModel, resp.Header, resp.StatusCode)
	}
	if profile.UsesCodex && promptCacheKey != "" {
		if turnState := strings.TrimSpace(resp.Header.Get("x-codex-turn-state")); turnState != "" {
			p.BindTurnState(ctx, promptCacheKey, turnState)
		}
	}

	output := upstream.NewDeferredOutputContext(p.Sink())
	convOpts := p.ConvertedResponseOptions(resp, protocol.ProtocolAnthropicMessages, originalModel, billingModel, upstreamModel, len(body))
	if ctx != nil {
		// Pass the active retry context so FinalizeExchangeConversion updates this
		// attempt's isolated scope even when resp.Request is synthetic in tests.
		convOpts.RequestContext = ctx
	}
	var nativeResult *openai.CompatResponseResult
	var handleErr error
	if clientStream {
		nativeResult, handleErr = openai.ReadConvertedStreaming(resp, output, exchange, convOpts, originalModel, upstreamModel, startTime)
	} else {
		nativeResult, handleErr = openai.ReadConvertedBuffered(resp, output, exchange, convOpts, originalModel, upstreamModel, startTime)
	}
	result := FromCompatResult(nativeResult, billingModel)
	if p.CyberPolicy() {
		if handleErr == nil {
			handleErr = forwardcore.ErrCyberPolicyForwarded
		}
		return nil, handleErr
	}

	if handleErr == nil && result != nil {
		if compatContinuationEnabled && promptCacheKey != "" && result.ResponseID != "" {
			p.BindResponseID(ctx, promptCacheKey, result.ResponseID)
		}
		if promptCacheKey != "" && anthropicDigestChain != "" {
			p.BindDigestKey(apiKeyID, anthropicDigestChain, promptCacheKey, anthropicMatchedDigestChain)
		}
		if tier := p.ResolvedServiceTier(requeststate.ExtractOpenAIServiceTierFromBody(responsesBody)); tier != nil {
			result.ServiceTier = tier
		}
		if eff := requeststate.ExtractOpenAIReasoningEffortFromBody(responsesBody); eff != nil && *eff != "" {
			result.ReasoningEffort = eff
		}
	}
	if handleErr == nil && profile.Type == "oauth" && !profile.Shadow && profile.Platform != "grok" {
		p.UpdateCodexUsage(ctx, resp.Header)
	}
	return result, handleErr
}

// runConvertedChatToResponses executes the Chat-source branch of
// RunConvertedResponses across Responses and non-Responses provider profiles.
// Client admission and reasoning-effort validation run before conversion, and
// API Key compatibility cache seeds isolate by downstream key once.
//
// Cursor requests arriving on /v1/chat/completions with top-level input and no
// messages convert to Chat Completions bytes via ConvertCursorResponsesToChat
// when targeting raw Chat or Anthropic routes, or patch raw Responses bytes
// with stream=true when targeting Responses upstreams while converting SSE
// output back to Chat Completions.
func runConvertedChatToResponses(
	ctx context.Context,
	body []byte,
	promptCacheKey string,
	defaultMappedModel string,
	compatPromptCacheTenantIsolated bool,
	p ConvertedResponsesPorts,
) (*Result, error) {
	profile, err := p.PrepareChat(ctx)
	if err != nil {
		return nil, err
	}
	if !p.ClientAllowed(ctx, body) {
		p.PolicyDenied()
		p.Reject(http.StatusForbidden, "forbidden_error", "This provider only allows Codex official clients")
		return nil, errors.New("codex_cli_only restriction: only codex official clients are allowed")
	}

	clientInfo, ok := inspectConvertedClientBody(body)
	if !ok {
		p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		parseErr := errors.New("parse chat completions request: invalid JSON object")
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(parseErr)
	}
	isResponsesShape := !clientInfo.hasMessages && clientInfo.hasInput

	// When a fixed non-Responses, non-Anthropic upstream protocol receives a
	// Cursor input-shaped Chat request, decode as Responses and encode as Chat
	// Completions via Exchange before delegating to the target route. Anthropic
	// Messages routes skip this hop so ForwardConvertedAnthropic converts input
	// directly in one Exchange without losing trc1:anthropic signatures or ToolMap.
	if profile.Protocol != "" &&
		profile.Protocol != protocol.ProtocolOpenAIResponses &&
		profile.Protocol != protocol.ProtocolAnthropicMessages &&
		!profile.Anthropic &&
		isResponsesShape {
		convertedChatBody, convErr := p.ConvertCursorResponsesToChat(body)
		if convErr != nil {
			p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", convErr.Error())
			return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", convErr.Error()).WithCause(convErr)
		}
		body = convertedChatBody
		isResponsesShape = false
	}

	if profile.Platform == "grok" {
		if profile.Protocol == protocol.ProtocolOpenAIChatCompletions {
			return p.DispatchChat(ctx, DispatchRawChat, body, promptCacheKey, defaultMappedModel)
		}
		if profile.Protocol == protocol.ProtocolOpenAIResponses {
			// Explicit Grok Responses routes convert all Chat Completions and Cursor
			// input-shaped payloads directly through GrokExecutor.ChatResponses (IR).
			return p.DispatchChat(ctx, DispatchGrok, body, promptCacheKey, defaultMappedModel)
		}
		if profile.GrokOAuth {
			eligible, reason := gatewayprovider.GrokBodyCodec().GrokChatResponsesBridgeEligibility(body)
			if eligible {
				return p.DispatchChat(ctx, DispatchGrok, body, promptCacheKey, defaultMappedModel)
			}
			logging.L().Debug("grok chat_completions (ir): using raw fallback",
				zap.Int64("provider_id", profile.ID),
				zap.String("reason", reason),
			)
		}
		if isResponsesShape {
			convertedChatBody, convErr := p.ConvertCursorResponsesToChat(body)
			if convErr != nil {
				p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", convErr.Error())
				return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", convErr.Error()).WithCause(convErr)
			}
			body = convertedChatBody
		}
		return p.DispatchChat(ctx, DispatchRawChat, body, promptCacheKey, defaultMappedModel)
	}

	if err := requeststate.ValidateOpenAIReasoningEffort(body, clientInfo.model); err != nil {
		p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(err)
	}

	if profile.Adaptive {
		if !isResponsesShape {
			return p.DispatchChat(ctx, DispatchRawChat, body, promptCacheKey, defaultMappedModel)
		}
		if !profile.NativeResponsesSupported {
			chatBody, convErr := p.ConvertCursorResponsesToChat(body)
			if convErr != nil {
				p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", convErr.Error())
				return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", convErr.Error()).WithCause(fmt.Errorf("convert responses-shaped chat completions request: %w", convErr))
			}
			return p.DispatchChat(ctx, DispatchRawChat, chatBody, promptCacheKey, defaultMappedModel)
		}
	}

	if profile.Anthropic {
		return p.DispatchChat(ctx, DispatchAnthropic, body, promptCacheKey, defaultMappedModel)
	}
	if profile.RawChat || (!profile.ProtocolSlotDefaults && p.DefaultChat()) {
		if isResponsesShape {
			chatBody, convErr := p.ConvertCursorResponsesToChat(body)
			if convErr != nil {
				p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", convErr.Error())
				return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", convErr.Error()).WithCause(fmt.Errorf("convert responses-shaped chat completions request: %w", convErr))
			}
			body = chatBody
		}
		return p.DispatchChat(ctx, DispatchRawChat, body, promptCacheKey, defaultMappedModel)
	}

	startTime := time.Now()
	originalModel := clientInfo.model
	clientStream := clientInfo.stream
	billingModel := p.BillingModel(originalModel, defaultMappedModel)
	upstreamModel := p.UpstreamModel(billingModel)

	promptCacheKey = strings.TrimSpace(promptCacheKey)
	compatPromptCacheInjected := false
	if promptCacheKey == "" && !isResponsesShape && (profile.UsesCodex || profile.OpenAIAPIKey) && gatewayprovider.ShouldAutoInjectPromptCacheKeyForCompat(upstreamModel) {
		promptCacheKey = gatewayprovider.DeriveCompatPromptCacheKeyBytes(body, upstreamModel)
		compatPromptCacheInjected = promptCacheKey != ""
		if compatPromptCacheInjected && profile.OpenAIAPIKey {
			promptCacheKey = p.IsolateCacheKey(promptCacheKey)
			compatPromptCacheTenantIsolated = true
		}
	}

	dialect, err := p.ResolveResponsesDialect(upstreamModel, body)
	if err != nil {
		return nil, fmt.Errorf("resolve responses dialect: %w", err)
	}
	exchange, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  dialect,
		},
		ClientModel: originalModel,
		Runtime:     p.ConversionRuntime(),
	})
	if err != nil {
		p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("convert chat completions to responses: %w", err))
	}

	var responsesBody []byte
	if isResponsesShape {
		// Cursor input-shaped requests targeting Responses upstream preserve raw
		// Responses fields, force stream=true for SSE response readers, strip
		// incompatible Cursor extensions, normalize service_tier via jsontext, and
		// collect deleted-field losses during the same single top-level parse.
		var cursorLosses []ir.Loss
		responsesBody, _, err = patchConvertedResponsesBody(body, convertedResponsesBodyPatch{
			Model:                upstreamModel,
			ForceStream:          true,
			DeleteFields:         CursorResponsesUnsupportedFields,
			NormalizeServiceTier: true,
			OnDeleteField: func(key string, rawVal jsontext.Value) {
				if loss, ok := cursorDeletedFieldLoss(key, rawVal); ok {
					cursorLosses = append(cursorLosses, loss)
				}
			},
		})
		if err != nil {
			p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", err.Error())
			return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("patch responses-shape chat body: %w", err))
		}
		// Replace Exchange's unused re-encoded request losses with losses from the
		// raw Cursor body actually sent upstream while preserving response losses.
		exchange.ReplaceRequestLosses(cursorLosses)
	} else {
		responsesBody, err = exchange.UpstreamRequest()
		if err != nil {
			p.SourceError(protocol.ProtocolOpenAIChatCompletions, http.StatusBadRequest, "invalid_request_error", err.Error())
			return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("encode converted responses request: %w", err))
		}
		responsesBody, _, err = patchConvertedResponsesBody(responsesBody, convertedResponsesBodyPatch{
			Model:                upstreamModel,
			ForceStream:          true,
			NormalizeServiceTier: true,
		})
		if err != nil {
			return nil, fmt.Errorf("patch converted responses request: %w", err)
		}
	}
	upstream.ObserveExchangeConversion(ctx, exchange)

	logFields := []zap.Field{
		zap.Int64("provider_id", profile.ID),
		zap.String("original_model", originalModel),
		zap.String("billing_model", billingModel),
		zap.String("upstream_model", upstreamModel),
		zap.Bool("stream", clientStream),
		zap.Bool("responses_shape", isResponsesShape),
	}
	if compatPromptCacheInjected {
		logFields = append(logFields,
			zap.Bool("compat_prompt_cache_key_injected", true),
			zap.String("compat_prompt_cache_key_sha256", upstream.HashSensitiveValueForLog(promptCacheKey)),
		)
	}
	logging.L().Debug("openai chat_completions (ir): model mapping applied", logFields...)

	if profile.UsesCodex {
		isJSONObject := strings.EqualFold(strings.TrimSpace(extractResponsesBodyTextFormatType(responsesBody)), "json_object")
		var codexErr error
		responsesBody, upstreamModel, promptCacheKey, codexErr = applyConvertedCodexTransform(responsesBody, convertedCodexTransformInput{
			ForMessages:      false,
			IsResponsesShape: isResponsesShape,
			IsJSONObject:     isJSONObject,
			OriginalModel:    originalModel,
			BillingModel:     billingModel,
			UpstreamModel:    upstreamModel,
			PromptCacheKey:   promptCacheKey,
			APIKeyID:         p.APIKeyID(),
			ToolNameReverse:  p.ToolNameReverse,
			ProviderIdentity: p.ProviderIdentity,
		})
		if codexErr != nil {
			if valErr, ok := errors.AsType[*convertedCodexValidationError](codexErr); ok {
				p.Reject(http.StatusBadRequest, "invalid_request_error", valErr.Error())
				return nil, valErr.Unwrap()
			}
			return nil, codexErr
		}
	}
	if profile.Type == "apikey" {
		if trimmedKey := strings.TrimSpace(promptCacheKey); trimmedKey != "" {
			responsesBody, _, err = patchConvertedResponsesBody(responsesBody, convertedResponsesBodyPatch{
				InjectPromptCacheKey: trimmedKey,
			})
			if err != nil {
				return nil, fmt.Errorf("inject prompt_cache_key: %w", err)
			}
		}
	}

	updatedBody, policyErr := p.ApplyChatFast(ctx, upstreamModel, responsesBody)
	if policyErr != nil {
		return nil, policyErr
	}
	responsesBody = updatedBody
	reasoningEffort := requeststate.ExtractOpenAIReasoningEffortFromBody(responsesBody)
	reasoningEffort = gatewayprovider.ApplyThinkingEnabledFallback(reasoningEffort, responsesBody, billingModel)

	token, err := p.AccessToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("get access token: %w", err)
	}

	upstreamCtx, releaseUpstreamCtx := gatewayprovider.DetachUpstreamContext(ctx)
	upstreamReq, err := p.BuildChat(upstreamCtx, responsesBody, token, promptCacheKey)
	releaseUpstreamCtx()
	if err != nil {
		return nil, fmt.Errorf("build upstream request: %w", err)
	}

	if promptCacheKey != "" {
		apiKeyID := p.APIKeyID()
		sessionKey := promptCacheKey
		if !compatPromptCacheTenantIsolated {
			sessionKey = p.UpstreamSessionKey(apiKeyID, promptCacheKey)
		}
		upstreamReq.Header.Set("session_id", upstream.GenerateSessionUUID(sessionKey))
	}

	p.PrepareTransport()
	resp, err := p.Send(upstreamReq)
	if err != nil {
		return nil, p.TransportError(ctx, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, _ := p.ReadUpstreamError(resp)
		// Spend the single Agent Identity recovery guard before recursively
		// rebuilding the converted Chat attempt from client bytes in a fresh
		// conversion scope so retry losses never sum with the rejected attempt.
		if !requeststate.AgentTaskRecoveryTried(ctx) && p.IsAgentIdentity(ctx) && openai.IsAgentTaskInvalidHTTPResponse(resp.StatusCode, respBody) {
			if err := p.RecoverAgentTask(ctx); err != nil {
				return nil, fmt.Errorf("agent identity task recovery failed: %w", err)
			}
			retryCtx := telemetry.WithProtocolConversionScope(requeststate.WithAgentTaskRecovery(ctx))
			return RunConvertedResponses(retryCtx, protocol.ProtocolOpenAIChatCompletions, body, promptCacheKey, defaultMappedModel, compatPromptCacheTenantIsolated, p)
		}
		respBody = p.RedactErrorBody(ctx, respBody)
		resp.Body = io.NopCloser(bytes.NewReader(respBody))
		upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(respBody)))
		if foErr := p.FailoverHTTP(ctx, resp, respBody, upstreamMsg, upstreamModel); foErr != nil {
			return nil, foErr
		}
		return p.SourceErrorResponse(protocol.ProtocolOpenAIChatCompletions, resp, billingModel)
	}

	output := upstream.NewDeferredOutputContext(p.Sink())
	convOpts := p.ConvertedResponseOptions(resp, protocol.ProtocolOpenAIChatCompletions, originalModel, billingModel, upstreamModel, len(body))
	if ctx != nil {
		// Pass the active retry context so FinalizeExchangeConversion updates this
		// attempt's isolated scope even when resp.Request is synthetic in tests.
		convOpts.RequestContext = ctx
	}
	var nativeResult *openai.CompatResponseResult
	var handleErr error
	if clientStream {
		nativeResult, handleErr = openai.ReadConvertedStreaming(resp, output, exchange, convOpts, originalModel, upstreamModel, startTime)
	} else {
		nativeResult, handleErr = openai.ReadConvertedBuffered(resp, output, exchange, convOpts, originalModel, upstreamModel, startTime)
	}
	result := FromCompatResult(nativeResult, billingModel)
	if p.CyberPolicy() {
		if handleErr == nil {
			handleErr = forwardcore.ErrCyberPolicyForwarded
		}
		return nil, handleErr
	}

	if handleErr == nil && result != nil {
		if tier := p.ResolvedServiceTier(requeststate.ExtractOpenAIServiceTierFromBody(responsesBody)); tier != nil {
			result.ServiceTier = tier
		}
		result.ReasoningEffort = reasoningEffort
	}
	if handleErr == nil && profile.UsesCodex && !profile.Shadow {
		p.UpdateCodexUsage(ctx, resp.Header)
	}
	return result, handleErr
}

// inspectConvertedClientBody scans top-level model, stream, messages, input,
// output_config.effort, and disabled thinking flags from raw client JSON bytes
// via jsontext. Non-object payloads, syntax errors, or trailing tokens after
// the root object return a zero-valued summary and false.
func inspectConvertedClientBody(body []byte) (convertedClientBodyInfo, bool) {
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return convertedClientBodyInfo{}, false
	}

	var info convertedClientBodyInfo
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil || keyTok.Kind() != '"' {
			return convertedClientBodyInfo{}, false
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return convertedClientBodyInfo{}, false
		}
		switch key {
		case "model":
			_ = jsonv2.Unmarshal(val, &info.model, jsontext.AllowInvalidUTF8(true))
		case "stream":
			_ = jsonv2.Unmarshal(val, &info.stream, jsontext.AllowInvalidUTF8(true))
		case "messages":
			info.hasMessages = true
		case "input":
			info.hasInput = true
		case "output_config":
			info.clientEffort = extractSubObjectStringMember(val, "effort")
		case "thinking":
			info.thinkingDisabled = isConvertedThinkingDisabledBytes(val)
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return convertedClientBodyInfo{}, false
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return convertedClientBodyInfo{}, false
	}
	return info, true
}

// isConvertedThinkingDisabledBytes reports whether rawObj is a JSON thinking
// object that explicitly disables reasoning via type="disabled"/"off"/"none" or
// an integer zero budget_tokens field. Non-object values or malformed JSON
// tokens return false so callers fall back to active-effort inspection.
func isConvertedThinkingDisabledBytes(rawObj jsontext.Value) bool {
	dec := jsontext.NewDecoder(bytes.NewReader(rawObj), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return false
	}
	disabled := false
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil {
			return false
		}
		k := kTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return false
		}
		switch k {
		case "type":
			var typ string
			if jsonv2.Unmarshal(val, &typ, jsontext.AllowInvalidUTF8(true)) == nil {
				switch strings.ToLower(strings.TrimSpace(typ)) {
				case "disabled", "off", "none":
					disabled = true
				}
			}
		case "budget_tokens":
			var budget int64
			if jsonv2.Unmarshal(val, &budget) == nil && budget == 0 {
				disabled = true
			}
		}
	}
	return disabled
}

// cursorDeletedFieldLoss classifies one stripped Cursor extension member into
// an extra_dropped ir.Loss record. Consumed stream_options.include_usage
// toggles and null stream_options values return false because Exchange applies
// include_usage during response encoding without losing request semantics.
func cursorDeletedFieldLoss(key string, rawVal jsontext.Value) (ir.Loss, bool) {
	if key == "stream_options" && !cursorStreamOptionsHasDroppedKeys(rawVal) {
		return ir.Loss{}, false
	}
	return ir.Loss{
		Kind:   ir.LossExtraDropped,
		Path:   key,
		Detail: key,
	}, true
}

// cursorStreamOptionsHasDroppedKeys reports whether rawVal carries any
// stream_options content other than null or the consumed include_usage toggle
// without copying nested values.
func cursorStreamOptionsHasDroppedKeys(rawVal jsontext.Value) bool {
	trimmed := bytes.TrimSpace(rawVal)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	if trimmed[0] != '{' {
		return true
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return true
	}
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil || kTok.Kind() != '"' {
			return true
		}
		if kTok.String() != "include_usage" {
			return true
		}
		if err := dec.SkipValue(); err != nil {
			return true
		}
	}
	return false
}

// patchConvertedResponsesBody applies model, stream, service_tier, effort,
// continuation, and prompt_cache_key edits onto ordered raw JSON object members
// without decoding unmodeled fields or rounding large integers. When
// patch.OnDeleteField is set, it observes each matched DeleteFields member
// before removal during the same parse. It returns the updated JSON bytes
// alongside any resolved canonical service_tier string, or an error when body
// is not a valid JSON object.
func patchConvertedResponsesBody(body []byte, patch convertedResponsesBodyPatch) ([]byte, string, error) {
	members, err := readConvertedObjectMembers(body)
	if err != nil {
		return nil, "", err
	}

	for _, delKey := range patch.DeleteFields {
		rawVal, ok := findConvertedMember(members, delKey)
		if !ok {
			continue
		}
		if patch.OnDeleteField != nil {
			patch.OnDeleteField(delKey, rawVal)
		}
		members = deleteConvertedMember(members, delKey)
	}

	if patch.Model != "" {
		rawModel, err := jsonv2.Marshal(patch.Model, jsontext.AllowInvalidUTF8(true))
		if err != nil {
			return nil, "", err
		}
		members = upsertConvertedMember(members, "model", rawModel)
	}
	if patch.ForceStream {
		members = upsertConvertedMember(members, "stream", jsontext.Value("true"))
	}

	resolvedTier := ""
	if patch.SetServiceTier != "" {
		rawTier, err := jsonv2.Marshal(patch.SetServiceTier, jsontext.AllowInvalidUTF8(true))
		if err != nil {
			return nil, "", err
		}
		members = upsertConvertedMember(members, "service_tier", rawTier)
		resolvedTier = patch.SetServiceTier
	} else if patch.NormalizeServiceTier {
		if rawExisting, ok := findConvertedMember(members, "service_tier"); ok {
			var tierStr string
			if jsonv2.Unmarshal(rawExisting, &tierStr, jsontext.AllowInvalidUTF8(true)) == nil && tierStr != "" {
				norm := protocolopenai.ServiceTierValue(tierStr)
				if norm == "" {
					members = deleteConvertedMember(members, "service_tier")
				} else {
					resolvedTier = norm
					if norm != tierStr {
						rawNorm, err := jsonv2.Marshal(norm, jsontext.AllowInvalidUTF8(true))
						if err != nil {
							return nil, "", err
						}
						members = upsertConvertedMember(members, "service_tier", rawNorm)
					}
				}
			}
		}
	}

	if patch.ReconcileEffort != nil {
		if rawReasoning, ok := findConvertedMember(members, "reasoning"); ok {
			if subMembers, subErr := readConvertedObjectMembers(rawReasoning); subErr == nil {
				if rawEff, hasEff := findConvertedMember(subMembers, "effort"); hasEff {
					var curEffort string
					if jsonv2.Unmarshal(rawEff, &curEffort, jsontext.AllowInvalidUTF8(true)) == nil {
						nextEffort := patch.ReconcileEffort(curEffort)
						if nextEffort != curEffort {
							if nextEffort == "" {
								subMembers = deleteConvertedMember(subMembers, "effort")
							} else {
								rawNext, err := jsonv2.Marshal(nextEffort, jsontext.AllowInvalidUTF8(true))
								if err != nil {
									return nil, "", err
								}
								subMembers = upsertConvertedMember(subMembers, "effort", rawNext)
							}
							rebuiltReasoning, err := encodeConvertedObjectMembers(subMembers)
							if err != nil {
								return nil, "", err
							}
							members = upsertConvertedMember(members, "reasoning", rebuiltReasoning)
						}
					}
				}
			}
		}
	}

	if patch.PreviousResponseID != "" {
		rawPrev, err := jsonv2.Marshal(patch.PreviousResponseID, jsontext.AllowInvalidUTF8(true))
		if err != nil {
			return nil, "", err
		}
		members = upsertConvertedMember(members, "previous_response_id", rawPrev)
	}

	if patch.InjectPromptCacheKey != "" {
		existingKey := ""
		if rawExisting, ok := findConvertedMember(members, "prompt_cache_key"); ok {
			_ = jsonv2.Unmarshal(rawExisting, &existingKey, jsontext.AllowInvalidUTF8(true))
		}
		if strings.TrimSpace(existingKey) == "" {
			rawKey, err := jsonv2.Marshal(patch.InjectPromptCacheKey, jsontext.AllowInvalidUTF8(true))
			if err != nil {
				return nil, "", err
			}
			members = upsertConvertedMember(members, "prompt_cache_key", rawKey)
		}
	}

	encoded, err := encodeConvertedObjectMembers(members)
	if err != nil {
		return nil, "", err
	}
	return encoded, resolvedTier, nil
}

// extractResponsesBodyTextFormatType reads text.format.type from raw Responses
// JSON bytes using jsontext without decoding unrelated request fields. Missing,
// non-object, malformed, or non-string format members return an empty string.
func extractResponsesBodyTextFormatType(body []byte) string {
	members, err := readConvertedObjectMembers(body)
	if err != nil {
		return ""
	}
	rawText, ok := findConvertedMember(members, "text")
	if !ok {
		return ""
	}
	textMembers, err := readConvertedObjectMembers(rawText)
	if err != nil {
		return ""
	}
	rawFmt, ok := findConvertedMember(textMembers, "format")
	if !ok {
		return ""
	}
	return extractSubObjectStringMember(rawFmt, "type")
}

// extractSubObjectStringMember reads one string member named targetKey from
// rawObj using permissive UTF-8 decoding. Non-object inputs, malformed tokens,
// or non-string member values return an empty string.
func extractSubObjectStringMember(rawObj jsontext.Value, targetKey string) string {
	dec := jsontext.NewDecoder(bytes.NewReader(rawObj), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return ""
	}
	var out string
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
		if k == targetKey {
			_ = jsonv2.Unmarshal(val, &out, jsontext.AllowInvalidUTF8(true))
		}
	}
	return out
}

// readConvertedObjectMembers parses a JSON object into ordered raw members with
// independently cloned value slices. Non-object inputs or syntax errors return
// nil alongside the underlying jsontext parse error.
func readConvertedObjectMembers(raw []byte) ([]rawConvertedMember, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '{' {
		return nil, errors.New("expected JSON object")
	}
	var members []rawConvertedMember
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		name := kTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return nil, err
		}
		members = append(members, rawConvertedMember{
			name:  name,
			value: bytes.Clone(val),
		})
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	return members, nil
}

// encodeConvertedObjectMembers serializes ordered raw JSON object members into
// compact object bytes while preserving invalid UTF-8 sequences in member
// values. Write errors return nil alongside the failing jsontext error.
func encodeConvertedObjectMembers(members []rawConvertedMember) ([]byte, error) {
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

// findConvertedMember scans members from back to front and returns the raw
// value slice of the last entry matching name. Absent keys return nil and
// false without allocating slices.
func findConvertedMember(members []rawConvertedMember, name string) (jsontext.Value, bool) {
	for _, m := range slices.Backward(members) {
		if m.name == name {
			return m.value, true
		}
	}
	return nil, false
}

// upsertConvertedMember replaces the first member matching name in place with a
// cloned val slice and removes later duplicate keys. When name is absent, a new
// member is appended at the end of the returned slice.
func upsertConvertedMember(members []rawConvertedMember, name string, val jsontext.Value) []rawConvertedMember {
	replaced := false
	out := members[:0]
	for _, m := range members {
		if m.name == name {
			if !replaced {
				out = append(out, rawConvertedMember{name: name, value: bytes.Clone(val)})
				replaced = true
			}
			continue
		}
		out = append(out, m)
	}
	if !replaced {
		out = append(slices.Clip(out), rawConvertedMember{name: name, value: bytes.Clone(val)})
	}
	return out
}

// deleteConvertedMember removes all members matching name from members while
// preserving the relative order of remaining entries.
func deleteConvertedMember(members []rawConvertedMember, name string) []rawConvertedMember {
	return slices.DeleteFunc(members, func(m rawConvertedMember) bool {
		return m.name == name
	})
}
