package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	"github.com/TokenFlux/TokenRouter/internal/gateway/admission"
	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayadapter "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/timing"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	openaiwire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/upstream/qoder"
)

// unified_text_executor.go dispatches compatible text requests to platform
// executors and converts native results for completion.
// Compatible text entry points can execute providers whose native token
// accounting differs from OpenAI total input. Shared preparation checks pricing
// and reasoning before dispatching the selected platform; conversion retains
// native usage alongside the common result so completion can apply the
// appropriate cache interpretation.
//
// Native Messages also needs local interception and serial message ownership
// before platform forwarding. Qoder credential recovery stays within the same
// target when its exchange returns nil result and leaves writer size unchanged.
// Header-only commitment without recorded heartbeats blocks recovery. Native result
// conversion copies token buckets but retains mutable metadata references, so
// completion capture creates its own recording input before later request state
// can change.

// UnifiedTextExecutor executes one exchange on the selected provider; the caller owns selection and failover.
type UnifiedTextExecutor struct {
	// Pricing optionally checks the effective billing model for every selected
	// text attempt. Nil skips this preflight, allowing execution to use the
	// configured platform binding directly.
	Pricing *admission.ModelPricing
	// OpenAI-compatible single exchange executor
	OpenAI      *OpenAIResponsesExecutor
	Anthropic   *MessagesExecutor            // native Messages and conversion executor
	Gemini      *GeminiExecutor              // native Gemini and conversion executor
	Antigravity *AntigravityExecutor         // Antigravity text conversion executor
	Qoder       *gatewayadapter.QoderRuntime // native Qoder exchange runtime
	// QoderRefresh optionally recovers eligible credential failures when the first
	// exchange returns nil result and leaves writer size unchanged. Nil disables
	// recovery and retains the first forwarding error.
	QoderRefresh *provideradapter.QoderRequestRefresh
	// MessageQueue optionally serializes or spaces real user messages on eligible
	// Anthropic targets. Nil omits queue handling while native client detection
	// and compatibility still run.
	MessageQueue NativeMessageQueue
	// MessageQueueMode is the default when an eligible provider has no explicit
	// mode. A provider mode takes precedence, and unsupported modes perform no
	// queue action.
	MessageQueueMode string
	MessageQueueWait time.Duration // native message queue wait budget
}

// EnforceClient installs the supplied execution context and checks
// OpenAI-compatible client policy. Other provider kinds proceed with no
// client-policy call here; callers provide the compatible request binding
// before eligible execution begins.
func (e *UnifiedTextExecutor) EnforceClient(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, body []byte, match egress.TLSFingerprintRouterMatchResult) error {
	c.Request = c.Request.WithContext(ctx)
	if target.View().IsOpenAICompatible() {
		return e.OpenAI.Requests.EnforceClient(ctx, c, target, body, match)
	}
	return nil
}

// Responses executes one selected exchange after pricing and reasoning
// preparation. It installs the supplied context in Gin, then uses any
// preparation-updated context for compatible or native dispatch. Failed
// attempts restore the incoming routing context while retaining any captured
// client-requested effort; provider selection, switching and completion remain
// with its caller.
func (e *UnifiedTextExecutor) Responses(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, body []byte) (result *forward.OpenAIResult, failure error) {
	baseCtx := ctx
	defer func() {
		captureTextPerformance(c.Request.Context(), result, failure)
		if failure != nil {
			restoreFailedAttemptContext(c, baseCtx)
		}
	}()
	c.Request = c.Request.WithContext(ctx)
	var err error
	target, body, err = e.prepare(c, target, body, protocol.ProtocolOpenAIResponses)
	if err != nil {
		return nil, err
	}
	ctx = c.Request.Context()
	if usesOpenAIExecutor(target) {
		return e.OpenAI.Forward(ctx, c, target, body)
	}
	return e.native(ctx, c, target, body, protocol.ProtocolOpenAIResponses)
}

// Messages clears the current interception marker and executes one selected
// exchange after common preparation. Compatible targets receive cache/model and
// optional TLS inputs; native targets use native preparation and return nil
// result for handled local probes. Failed attempts restore the caller's
// baseline routing context while retaining any captured client-requested
// effort.
func (e *UnifiedTextExecutor) Messages(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, body []byte, cacheKey, model string, tls ...egress.TLSFingerprintRouterMatchResult) (result *forward.OpenAIResult, failure error) {
	baseCtx := ctx
	defer func() {
		captureTextPerformance(c.Request.Context(), result, failure)
		if failure != nil {
			restoreFailedAttemptContext(c, baseCtx)
		}
	}()
	c.Request = c.Request.WithContext(ctx)
	c.Set(nativeMessageInterceptedKey, false)
	var err error
	target, body, err = e.prepare(c, target, body, protocol.ProtocolAnthropicMessages)
	if err != nil {
		return nil, err
	}
	ctx = c.Request.Context()
	if usesOpenAIExecutor(target) {
		return e.OpenAI.Text.Messages(ctx, c, target, body, cacheKey, model, tls...)
	}
	return e.native(ctx, c, target, body, protocol.ProtocolAnthropicMessages)
}

// Chat executes one selected exchange after pricing and reasoning preparation
// in the supplied context. Compatible targets receive cache/model and optional
// TLS inputs, while native targets use their platform bindings; failed attempts
// restore the caller's baseline routing context while retaining any captured
// client-requested effort, and callers own selection and completion.
func (e *UnifiedTextExecutor) Chat(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, body []byte, cacheKey, model string, tls ...egress.TLSFingerprintRouterMatchResult) (result *forward.OpenAIResult, failure error) {
	baseCtx := ctx
	defer func() {
		captureTextPerformance(c.Request.Context(), result, failure)
		if failure != nil {
			restoreFailedAttemptContext(c, baseCtx)
		}
	}()
	c.Request = c.Request.WithContext(ctx)
	var err error
	target, body, err = e.prepare(c, target, body, protocol.ProtocolOpenAIChatCompletions)
	if err != nil {
		return nil, err
	}
	ctx = c.Request.Context()
	if usesOpenAIExecutor(target) {
		return e.OpenAI.Text.Chat(ctx, c, target, body, cacheKey, model, tls...)
	}
	return e.native(ctx, c, target, body, protocol.ProtocolOpenAIChatCompletions)
}

// prepare records actual selected-provider attribution, applies platform
// reasoning policy, resolves the attempt route, and checks price before
// dispatch. Applying platform effort policy ahead of attempt-route resolution
// ensures group downgrades or rewrites update the routing effort seen by
// provider effort_model_mapping and pricing preflight. Anthropic and OpenAI
// policy failures write local permission output in the inbound format; caller
// context changes remain in Gin.
func (e *UnifiedTextExecutor) prepare(c *gin.Context, target *gatewayadapter.ExecutionProvider, body []byte, source protocol.ProtocolID) (*gatewayadapter.ExecutionProvider, []byte, error) {
	if target == nil {
		return nil, nil, fmt.Errorf("provider is nil")
	}
	// Failover can move to a provider served by the other executor family.
	// Clear both observation slots so a failed attempt cannot attribute its
	// predecessor's endpoint to this provider.
	ClearActualOpenAIUpstreamEndpoint(c)
	SetActualUpstreamEndpoint(c, "")
	SetOpsSelectedProvider(c, target.Record.RoutingSnapshot())
	key, _ := EffectiveAPIKey(c)
	result := body
	var policyChanged bool
	var policyErr error
	switch target.Record.Platform {
	case provider.PlatformAnthropic:
		result, policyChanged, policyErr = ApplyAnthropicReasoningEffortPolicyForRequest(c, key, body)
	case provider.PlatformOpenAI:
		result, policyChanged, policyErr = ApplyOpenAIReasoningEffortPolicyForRequest(c, key, body)
	}
	if policyErr != nil {
		MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
		MarkResponseCommitted(c)
		if source == protocol.ProtocolAnthropicMessages {
			DefaultOpenAIErrorOutput().WriteAnthropicStreamingError(c, http.StatusForbidden, "permission_error", policyErr.Error(), CommittedSSE(c))
		} else {
			DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, http.StatusForbidden, "permission_error", "reasoning_effort_limit", policyErr.Error(), CommittedSSE(c), false)
		}
		return nil, nil, policyErr
	}
	// Synchronize routing effort and route plan after a platform effort policy
	// rewrite so attempt resolution and pricing evaluate the effective effort.
	if policyChanged {
		effectiveEffort := requeststate.ExtractRoutingReasoningEffort(result, source)
		updatedCtx := requeststate.WithRoutingReasoningEffort(c.Request.Context(), effectiveEffort)
		if plan, ok := requeststate.RoutePlanFromContext(updatedCtx); ok {
			updatedCtx = requeststate.WithRoutePlan(updatedCtx, plan.WithReasoningEffort(effectiveEffort))
		}
		c.Request = c.Request.WithContext(updatedCtx)
	}
	target, err := gatewayadapter.ProviderForProtocolAttempt(c.Request.Context(), target)
	if err != nil {
		return nil, nil, err
	}
	if err := e.checkPricing(c, target, result, source); err != nil {
		return nil, nil, err
	}
	return target, result, nil
}

// usesOpenAIExecutor reports whether the OpenAI-compatible executor serves the
// target's attempt. Compatible platforms use it for every text protocol except
// a Gemini route, whose request encoding belongs to the Gemini executor.
func usesOpenAIExecutor(target *gatewayadapter.ExecutionProvider) bool {
	return target.View().IsOpenAICompatible() && target.Route.Protocol() != protocol.ProtocolGeminiGenerateContent
}

// native dispatches one native-platform exchange and converts its retained
// result for common completion. Native Messages first applies client detection,
// local interception, queue ownership and Bedrock compatibility, with returned
// cleanup deferred around execution.
//
// Platform adapters receive their specific protocol and session inputs; partial
// results remain available alongside errors. Qoder can refresh credentials and
// retry that same target once when the result is nil, writer size is unchanged
// and the error allows refresh. Header-only commitment without recorded
// heartbeats changes -1 to zero and prevents this retry. A successful refresh replaces the target record before
// retry; failed refresh leaves the first execution error. No provider selection
// is repeated by this function.
func (e *UnifiedTextExecutor) native(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, body []byte, source protocol.ProtocolID) (*forward.OpenAIResult, error) {
	var result *forward.MessagesResult
	var err error
	// Clear the previous native endpoint before dispatch so a failed attempt
	// cannot retain attribution from a different platform or earlier exchange.
	SetActualUpstreamEndpoint(c, "")
	var parsed *requeststate.ParsedRequest
	if source == protocol.ProtocolAnthropicMessages {
		var release func()
		var intercepted bool
		parsed, release, intercepted, err = e.prepareNativeMessages(c, target, body)
		if release != nil {
			defer release()
		}
		if err != nil || intercepted {
			return nil, err
		}
		body = parsed.Body.Bytes()
		ctx = c.Request.Context()
	}
	switch target.Record.Platform {
	case provider.PlatformAnthropic:
		if source == protocol.ProtocolAnthropicMessages {
			result, err = e.Anthropic.Forward(ctx, c, target, parsed)
		} else {
			result, err = e.Anthropic.ForwardConverted(ctx, c, target, source, body)
		}
	case provider.PlatformGemini:
		result, err = e.forwardGemini(ctx, c, target, body, source)
	case provider.PlatformAntigravity:
		result, err = e.forwardAntigravity(ctx, c, target, body, source)
	case provider.PlatformQoder:
		result, err = e.forwardQoderConverted(ctx, c, target, body, source)
	default:
		// A compatible platform can route a model through Gemini encoding; that
		// route uses the Gemini executor regardless of the provider platform.
		if target.Route.Protocol() != protocol.ProtocolGeminiGenerateContent {
			return nil, fmt.Errorf("unsupported provider platform %q", target.Record.Platform)
		}
		result, err = e.forwardGemini(ctx, c, target, body, source)
	}
	return nativeTextResult(result, GetUpstreamEndpoint(c, target.Record.Platform)), err
}

// forwardGemini runs one Gemini-encoded exchange for the client source and
// records the Gemini endpoint before dispatching through ForwardConverted;
// partial results remain available alongside errors for caller conversion.
func (e *UnifiedTextExecutor) forwardGemini(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, body []byte, source protocol.ProtocolID) (*forward.MessagesResult, error) {
	SetActualUpstreamEndpoint(c, EndpointGeminiModels)
	return e.Gemini.ForwardConverted(ctx, c, target, source, body)
}

// forwardAntigravity runs one Antigravity OAuth exchange for source and records
// the Antigravity GenerateContent endpoint before dispatching through
// ForwardConverted with sticky-session state.
func (e *UnifiedTextExecutor) forwardAntigravity(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, body []byte, source protocol.ProtocolID) (*forward.MessagesResult, error) {
	SetActualUpstreamEndpoint(c, EndpointAntigravityGenerateContent)
	sticky, ok := requeststate.PrefetchedStickyProviderIDFromContext(ctx)
	isSticky := ok && sticky == target.Record.ID
	return e.Antigravity.ForwardConverted(ctx, c, target, source, body, isSticky)
}

// forwardQoderConverted executes one Qoder exchange through protocol/convert
// while preserving same-target credential refresh, local RequestError 400
// rejection, and pre-semantic provider failover. Switchable upstream failures
// observed before any non-heartbeat client bytes are written return an
// UpstreamFailoverError so outer attempt loops advance to the next provider.
//
// When a waiting keepalive heartbeat has already committed HTTP 200 SSE headers,
// unrecoverable local or upstream failures emit an untimed client-protocol
// terminal SSE error frame instead of trailing JSON. Retryable pre-output
// errors remain unwritten during that initial attempt so credential refresh or
// provider failover can still take over the open event stream.
func (e *UnifiedTextExecutor) forwardQoderConverted(
	ctx context.Context,
	c *gin.Context,
	target *gatewayadapter.ExecutionProvider,
	body []byte,
	source protocol.ProtocolID,
) (*forward.MessagesResult, error) {
	before := HeartbeatAdjustedWrittenSize(c)
	result, err := forwardQoderAttempt(ctx, c, e.Qoder, &target.Record, body, source, true)
	// Retry once on the same Qoder provider when credentials can refresh and no
	// non-heartbeat client bytes were written during the initial attempt.
	if err != nil && result == nil && HeartbeatAdjustedWrittenSize(c) == before && qoder.MayRefreshAttempt(err) && e.QoderRefresh != nil {
		fresh, refreshErr := e.QoderRefresh.RefreshProviderSession(ctx, &target.Record)
		if refreshErr == nil && fresh != nil {
			target.Record = *fresh
			result, err = forwardQoderAttempt(ctx, c, e.Qoder, &target.Record, body, source, true)
		}
	}
	if local, ok := errors.AsType[*qoder.RequestError](err); ok {
		// Map client request validation and model capability rejections to local
		// HTTP 400 MODEL_CAPABILITY_UNSUPPORTED so outer scheduling skips failover.
		failure := apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", local.Message).WithCause(err)
		if CommittedSSE(c) {
			model, _ := qoder.ExtractTopLevelModelJSON(body)
			writeQoderCommittedTerminalError(c, source, http.StatusBadRequest, "invalid_request_error", local.Code, failure.Message, model)
			MarkOpsClientBusinessLimited(c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
		} else {
			writeLocalTextError(c, source, failure, local.Code)
		}
		return nil, failure
	}
	if err == nil || result != nil {
		return result, err
	}
	// Promote switchable pre-output upstream errors into UpstreamFailoverError so
	// compatible text attempt loops fail over to the next candidate provider.
	if HeartbeatAdjustedWrittenSize(c) == before && ctx.Err() == nil && qoder.MaySwitchAttempt(err) {
		return nil, buildQoderFailoverError(err)
	}
	if !IsResponseCommitted(c) && !TerminalSSEDelivered(c) {
		model, _ := qoder.ExtractTopLevelModelJSON(body)
		if view := gatewayadapter.DescribeQoderError(err); view.Recognized {
			SetOpsUpstreamError(c, view.SourceStatus, view.Message, "")
			if CommittedSSE(c) || c.Writer.Written() {
				writeQoderCommittedTerminalError(c, source, view.Status, view.Kind, "", view.Message, model)
			} else if source == protocol.ProtocolAnthropicMessages {
				WriteAnthropicError(c, view.Status, view.Kind, "", view.Message)
				MarkResponseCommitted(c)
			} else {
				DefaultOpenAIErrorOutput().WriteError(c, view.Status, view.Kind, view.Message)
				MarkResponseCommitted(c)
			}
		} else if CommittedSSE(c) && ctx.Err() == nil {
			writeQoderCommittedTerminalError(c, source, http.StatusBadGateway, "upstream_error", "", "Upstream request failed", model)
		}
	}
	return nil, err
}

// buildQoderFailoverError wraps a pre-semantic switchable Qoder error into an
// UpstreamFailoverError carrying classified source and client HTTP metadata.
// Unrecognized transport failures default to HTTP 502 Bad Gateway under the
// provider inference scope.
func buildQoderFailoverError(err error) *forward.UpstreamFailoverError {
	view := gatewayadapter.DescribeQoderError(err)
	statusCode := http.StatusBadGateway
	clientStatus := 0
	clientMessage := ""
	var respBody []byte
	if apiErr, ok := errors.AsType[*qoder.APIError](err); ok {
		if apiErr.StatusCode > 0 {
			statusCode = apiErr.StatusCode
		}
		if apiErr.Body != "" {
			respBody = []byte(apiErr.Body)
		}
	}
	if view.Recognized {
		if view.SourceStatus > 0 {
			statusCode = view.SourceStatus
		}
		clientStatus = view.Status
		clientMessage = view.Message
	}
	return &forward.UpstreamFailoverError{
		Platform:         provider.PlatformQoder,
		StatusCode:       statusCode,
		ResponseBody:     respBody,
		Stage:            forward.GatewayFailureStageInference,
		Scope:            forward.GatewayFailureScopeProvider,
		ClientStatusCode: clientStatus,
		ClientMessage:    clientMessage,
	}
}

// nativeTextResult converts retained native observations into the common text
// result while copying its token usage value. Nil input returns nil. Headers,
// image collections and optional timing, effort, tier and audio pointers remain
// shared with the supplied result.
//
// NativeUsage retains the complete native buckets and processing speed
// separately from the OpenAI-shaped numeric view. Completion uses that native
// flag to interpret input and cache tokens according to their source protocol,
// preventing subtraction intended for OpenAI total-input counts from changing
// native billed input. Callers capture the result before mutating shared
// metadata or handing work to an asynchronous recorder.
func nativeTextResult(value *forward.MessagesResult, endpoint string) *forward.OpenAIResult {
	if value == nil {
		return nil
	}
	// Copy the native token buckets before constructing the common view, retaining
	// their source interpretation for completion while other metadata references
	// remain shared.
	usage := value.Usage
	return &forward.OpenAIResult{
		RequestID: value.RequestID, UpstreamHeaders: value.UpstreamHeaders,
		Model: value.Model, UpstreamModel: value.UpstreamModel, UpstreamEndpoint: endpoint,
		NativeUsage: &usage,
		Usage:       openaiwire.ForwardUsage{InputTokens: usage.InputTokens, OutputTokens: usage.OutputTokens, CacheCreationInputTokens: usage.CacheCreationInputTokens, CacheReadInputTokens: usage.CacheReadInputTokens, ImageOutputTokens: usage.ImageOutputTokens},
		Stream:      value.Stream, Duration: value.Duration, Timings: value.Timings, ClientDisconnect: value.ClientDisconnect,
		ReasoningEffort: value.ReasoningEffort, RequestedReasoningEffort: value.RequestedReasoningEffort,
		ServiceTier:                 value.ServiceTier,
		UpstreamResponseServiceTier: value.UpstreamResponseServiceTier,
		UpstreamResponseModel:       value.UpstreamResponseModel,
		ImageCount:                  value.ImageCount,
		ImageFrameCount:             value.ImageFrameCount, ImageSize: value.ImageSize, ImageInputSize: value.ImageInputSize, ImageOutputSize: value.ImageOutputSize,
		ImageOutputSizes: value.ImageOutputSizes, ImageSizeSource: value.ImageSizeSource, ImageSizeBreakdown: value.ImageSizeBreakdown,
		SearchCount: value.SearchCount, AudioUsage: value.AudioUsage,
	}
}

// captureTextPerformance freezes transport and downstream observations after forwarding.
// Platform executors can additionally confirm buffered usage; that evidence survives
// the final snapshot refresh. A conversion failure invalidates throughput eligibility
// while retaining the upstream observations available for diagnostics.
//
// The refresh runs after the adapter's final writes so delivery observations are
// present before asynchronous capture. A completed disconnect drain keeps its
// upstream completeness evidence; ordinary execution errors can retain partial
// usage for accounting while leaving throughput unavailable.
func captureTextPerformance(ctx context.Context, result *forward.OpenAIResult, failure error) {
	if result == nil {
		return
	}
	p := timing.PerformanceFromContext(ctx)
	if p == nil {
		return
	}
	if !p.Stream {
		p.Complete = failure == nil
		p.UsageComplete = result.Timings != nil && result.Timings.UsageComplete
	}
	if failure != nil && !result.ClientDisconnect {
		p.UsageComplete = false
	}
	result.Timings = p
}

// restoreFailedAttemptContext restores baseCtx onto c.Request after a failed
// attempt so platform effort rewrites do not leak into subsequent failover
// candidates. When baseCtx had no requested-effort observation, any client
// choice captured from the inbound payload before group policy evaluation is
// carried forward for local-denial and failure attribution.
func restoreFailedAttemptContext(c *gin.Context, baseCtx context.Context) {
	if c == nil || c.Request == nil {
		return
	}
	restored := baseCtx
	// Carry forward the immutable client effort captured before group policy
	// evaluation so local 403 denials and Ops diagnostics retain the raw choice.
	if requeststate.RequestedReasoningEffortFromContext(restored) == nil {
		if requested := requeststate.RequestedReasoningEffortFromContext(c.Request.Context()); requested != nil {
			restored = requeststate.WithRequestedReasoningEffort(restored, *requested)
		}
	}
	c.Request = c.Request.WithContext(restored)
}
