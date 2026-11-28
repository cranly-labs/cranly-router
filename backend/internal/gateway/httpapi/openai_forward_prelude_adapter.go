package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	"github.com/TokenFlux/TokenRouter/internal/gateway/media"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	forward "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// openai_forward_prelude_adapter.go binds Responses preparation to HTTP
// admission, identity and tool-restoration state.
// Responses preparation clears request-local response mappings before deriving
// this attempt's provider identity and compatibility policy. The prelude
// adapter supplies group image controls, client admission and protocol
// transformations to the shared runner, so rejected preparation uses the
// active HTTP context before any upstream response is opened.

// Interface assertion confirms openAIForwardPreludeAdapter satisfies PreludePorts.
var _ forward.PreludePorts = openAIForwardPreludeAdapter{}

// openAIForwardPreludeAdapter binds one Responses preparation to its provider
// and HTTP request.
type openAIForwardPreludeAdapter struct {
	// s, c, provider and ctx are borrowed across one prelude and transformation
	// call. A copied adapter retains these same references; callers keep their
	// selected attempt and live HTTP state available until those phases finish.
	s *OpenAIResponsesExecutor // shared Responses execution services
	c *gin.Context             // active HTTP request context
	// selected protocol attempt provider
	provider *gatewayprovider.ExecutionProvider
	ctx      context.Context // admitted execution policy context
}

// openAIForwardProfile describes the selected provider's credential, platform
// and resolved protocol flags for forwarding. Callers supply a nonnil
// execution provider whose route is resolved; the result captures the policy
// values used by prelude dispatch and the provider-specific image text driver.
func openAIForwardProfile(provider *gatewayprovider.ExecutionProvider) forward.Profile {
	imageModel := openai.ImagesResponsesMainModel
	if provider.View().IsOpenAIOAuthLike() {
		imageModel = gatewayprovider.ExecutionProtocolRecord(provider).CodexImageResponsesModel()
	}
	return forward.Profile{
		ImageResponsesModel:        imageModel,
		Platform:                   provider.Record.Platform,
		Name:                       provider.Record.Name,
		Type:                       provider.Record.Type,
		HTTPContinuation:           gatewayprovider.ExecutionProtocolTarget(provider).SupportsHTTPResponsesContinuation(),
		UsesCodex:                  provider.View().UsesOpenAICodexProtocol(),
		OpenAI:                     provider.View().IsOpenAI(),
		OAuth:                      provider.View().IsOAuth(),
		OAuthLike:                  provider.View().IsOpenAIOAuthLike(),
		APIKey:                     provider.Record.Type == capability.ProviderTypeAPIKey,
		Grok:                       provider.Record.Platform == capability.PlatformGrok,
		EncodeResponsesClientTools: gatewayprovider.ExecutionProtocolTarget(provider).RequiresResponsesClientToolEncoding(),
		StatelessResponses:         gatewayprovider.ExecutionProtocolTarget(provider).UsesStatelessResponses(),
		Anthropic:                  gatewayprovider.ExecutionProtocolTarget(provider).IsAnthropicProtocol(),
		RawChat:                    gatewayprovider.ExecutionModelPolicy(provider).RawChat(),
		ResolvedChat:               provider.Route.Protocol() == protocol.ProtocolOpenAIChatCompletions,
		Passthrough:                provider.View().IsOpenAIPassthroughEnabled(),
	}
}

// BlockGroupImages reports whether the admitted Responses group selects image
// blocking. It considers the execution context's declared client protocol and
// current effective API-key group; other protocols or missing group return
// false.
func (p openAIForwardPreludeAdapter) BlockGroupImages() bool {
	if source, _ := requeststate.ClientProtocolFromContext(p.ctx); source == protocol.ProtocolOpenAIResponses {
		if key := GetExecutionAPIKey(p.c); key != nil && key.Group != nil {
			return key.Group.ResponsesImagePolicy == "block"
		}
	}
	return false
}

// Begin resets model observation and actual-endpoint metadata for this
// attempt. A provider resolved to raw Chat records that endpoint immediately,
// so later transport or conversion errors report the route chosen before
// sending.
func (p openAIForwardPreludeAdapter) Begin() {
	BeginUpstreamResponseModelObservation(p.c)
	ClearActualOpenAIUpstreamEndpoint(p.c)
	if shouldForwardOpenAIResponsesViaRawChatCompletions(p.provider) {
		SetActualOpenAIUpstreamEndpoint(p.c, "/v1/chat/completions")
	}
}

// ClearMappings removes request-local Grok, OpenAI, namespace and Codex
// reverse-tool state before preparation, including IR ToolMaps. Callers run it
// at attempt start so another provider's restoration data cannot be reused.
func (p openAIForwardPreludeAdapter) ClearMappings() {
	ClearGrokResponsesClientToolMap(p.c)
	ClearOpenAIResponsesClientToolMap(p.c)
	ClearOpenAIResponsesNamespaceNames(p.c)
	SetCodexToolNameReverse(p.c, nil)
}

// PrepareIdentity prepares Codex identity for this selected attempt provider
// and request. The configured provider reader supplies any parent identity
// information; errors return to PreparePrelude before client compatibility
// transformations proceed.
func (p openAIForwardPreludeAdapter) PrepareIdentity(ctx context.Context) error {
	_, err := PrepareCodexIdentity(ctx, p.c, p.s.Requests.Providers, p.provider)
	return err
}

// MatchTLS resolves this provider's TLS routing match from the inbound
// request.
func (p openAIForwardPreludeAdapter) MatchTLS() egress.TLSFingerprintRouterMatchResult {
	return p.s.Requests.MatchTLS(p.c, p.provider)
}

// ClientAllowed evaluates and logs client admission with the supplied TLS
// match. An enabled unmatched restriction returns its configured forbidden
// message; disabled or matched admission returns true with an empty rejection
// message.
func (p openAIForwardPreludeAdapter) ClientAllowed(ctx context.Context, tls egress.TLSFingerprintRouterMatchResult, body []byte) (bool, string) {
	result := p.s.Requests.DetectClient(p.c, p.provider, tls)
	LogCodexCLIOnlyDetection(ctx, p.c, p.provider, APIKeyIDFromContext(p.c), result, body)
	if result.Enabled && !result.Matched {
		return false, OpenAIClientPolicyForbiddenMessage(result)
	}
	return true, ""
}

// Reject publishes requested local-policy, feature or upstream observations
// before writing its JSON rejection. Each flag controls its corresponding
// request marker, and the supplied parameter is passed to the error writer for
// client diagnostics.
func (p openAIForwardPreludeAdapter) Reject(v forward.Rejection) {
	if v.PolicyDenied {
		MarkOpsClientBusinessLimited(p.c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
	}
	if v.FeatureDenied {
		MarkOpsClientBusinessLimited(p.c, OpsClientBusinessLimitedReasonLocalFeatureGate)
	}
	if v.ObserveUpstream {
		SetOpsUpstreamError(p.c, v.Status, v.Message, "")
	}
	WriteOpenAIForwardRejection(p.c, v.Status, v.Type, v.Message, v.Param)
}

// LiteHeader reports whether the inbound header declares Responses Lite
// execution.
func (p openAIForwardPreludeAdapter) LiteHeader() bool {
	return gatewayprovider.ImageIntent().IsOpenAIResponsesLiteHeader(p.c.GetHeader(media.ResponsesLiteHeader))
}

// LitePayload delegates credential-specific Responses Lite cleanup and returns
// its rejection parameter. The default parameter is tools; a
// ResponsesLiteValidationError supplies its own parameter so the prelude
// rejection identifies the invalid field.
func (p openAIForwardPreludeAdapter) LitePayload(body []byte) ([]byte, bool, string, error) {
	updated, changed, err := gatewayprovider.NormalizeResponsesLiteForProvider(p.provider.View(), body)
	param := "tools"
	if validation, ok := errors.AsType[*openai.ResponsesLiteValidationError](err); ok {
		param = validation.Parameter()
	}
	return updated, changed, param, err
}

// Transport selects HTTP SSE with the HTTP client-protocol decision reason.
func (p openAIForwardPreludeAdapter) Transport() forward.TransportDecision {
	return forward.TransportDecision{Transport: "http_sse", Reason: "client_protocol_http"}
}

// CompactPath reports whether the current HTTP request targets legacy
// Responses Compact.
func (p openAIForwardPreludeAdapter) CompactPath() bool {
	return IsOpenAIResponsesCompactPath(p.c)
}

// FlattenRequired delegates namespace-declaration flattening policy for this
// provider, transport and Compact state.
func (p openAIForwardPreludeAdapter) FlattenRequired(v forward.TransportDecision, passthrough, compact bool) bool {
	return gatewayprovider.ShouldFlattenOpenAIResponsesNamespaces(p.provider, egress.OpenAIUpstreamTransport(v.Transport), passthrough, compact)
}

// Flatten lowers namespace declarations and binds their restoration names in
// the active request. The shared helper controls body rewriting and errors;
// callers clear prior mappings before invoking it for another provider
// attempt.
func (p openAIForwardPreludeAdapter) Flatten(ctx context.Context, body []byte) ([]byte, error) {
	rewritten, report, err := FlattenOpenAIResponsesNamespaces(p.c, body)
	if err != nil || len(report.Names) == 0 {
		return rewritten, err
	}
	reqCtx := p.resolveContext(ctx)
	model := requeststate.NewOpenAIRequestView(body).Model
	if mapped := p.MappedModel(model); mapped != "" {
		model = mapped
	}
	dialect := p.resolveClientToolDialect(reqCtx, model, body)
	counts := map[string]int{string(ir.LossToolLowered): report.Lowered}
	if report.ApproximatedChoice {
		counts[string(ir.LossToolChoiceApproximated)] = 1
	}
	upstream.ObserveToolConversionCounts(reqCtx, protocol.ProtocolOpenAIResponses, protocol.ProtocolOpenAIResponses, dialect, counts)
	return rewritten, nil
}

// StripNamespacesRequired delegates historical namespace cleanup policy for
// the selected provider and transport.
func (p openAIForwardPreludeAdapter) StripNamespacesRequired(v forward.TransportDecision, passthrough bool) bool {
	return gatewayprovider.ShouldStripOpenAIResponsesInputNamespaces(p.provider, egress.OpenAIUpstreamTransport(v.Transport), passthrough)
}

// KeepNamespaces delegates whether historical tool calls retain namespaces
// under the selected protocol policy. Provider, transport, passthrough and
// Compact policy are supplied together; the prelude uses this result when
// applying input namespace cleanup.
func (p openAIForwardPreludeAdapter) KeepNamespaces(v forward.TransportDecision, passthrough, compact bool) bool {
	return gatewayprovider.ShouldKeepOpenAIResponsesToolCallNamespaces(p.provider, egress.OpenAIUpstreamTransport(v.Transport), passthrough, compact)
}

// BindMessagesBridge delegates the request's Messages-compatibility marker
// publication for this preparation. SetOpenAICompatMessagesBridgeContext
// records true values and ignores false, so this call does not clear a marker
// already present on the request.
func (p openAIForwardPreludeAdapter) BindMessagesBridge(v bool) {
	SetOpenAICompatMessagesBridgeContext(p.c, v)
}

// CodexClient recognizes official request identity or the configured force-CLI
// setting.
func (p openAIForwardPreludeAdapter) CodexClient() bool {
	return openai.IsCodexOfficialClientByHeaders(p.c.GetHeader("User-Agent"), p.c.GetHeader("originator")) || p.s.Requests.Options.ForceCLI
}

// ImageToolPolicy reads explicit image-tool handling from the selected
// provider record.
func (p openAIForwardPreludeAdapter) ImageToolPolicy() string {
	return gatewayprovider.ExecutionProtocolRecord(p.provider).CodexImageGenerationExplicitToolPolicy()
}

// ObserveTransport records the chosen transport and reason in the live HTTP
// context. A WSv2 decision also emits normalized debug fields; a missing Gin
// context skips request publication while the decision can still be logged.
func (p openAIForwardPreludeAdapter) ObserveTransport(v forward.TransportDecision, model string, stream bool) {
	if p.c != nil {
		p.c.Set("openai_ws_transport_decision", v.Transport)
		p.c.Set("openai_ws_transport_reason", v.Reason)
	}
	if v.Transport == string(egress.OpenAIUpstreamTransportResponsesWebsocketV2) {
		gatewayprovider.LogOpenAIWSModeDebug("selected provider_id=%d provider_type=%s transport=%s reason=%s model=%s stream=%v", p.provider.Record.ID, p.provider.Record.Type, gatewayprovider.NormalizeOpenAIWSLogValue(v.Transport), gatewayprovider.NormalizeOpenAIWSLogValue(v.Reason), model, stream)
	}
}

// MappedModel applies this selected provider's effort-aware single-hop model
// mapping for preparation.
func (p openAIForwardPreludeAdapter) MappedModel(model string) string {
	policy := gatewayprovider.ExecutionModelPolicy(p.provider)
	if p.c != nil && p.c.Request != nil {
		policy = policy.WithContext(p.c.Request.Context())
	}
	return policy.Mapped(model)
}

// NeedsClientToolAdaptation resolves model's target Dialect and evaluates body
// without mutating request state. It reports whether compatible API Key
// Responses preparation must lower client tools before TransformRequest runs.
func (p openAIForwardPreludeAdapter) NeedsClientToolAdaptation(ctx context.Context, model string, body []byte) bool {
	dialect := p.resolveClientToolDialect(ctx, model, body)
	return convert.NeedsResponsesToolAdaptation(dialect, body, nil)
}

// AdaptClientTools lowers unsupported client tools via convert.AdaptResponsesTools
// and publishes the resulting ToolMap on success. Validation or collision errors
// write a local HTTP 400 invalid_request_error on tools and return a
// MODEL_CAPABILITY_UNSUPPORTED error so failover and health penalties are skipped.
func (p openAIForwardPreludeAdapter) AdaptClientTools(ctx context.Context, model string, body []byte) ([]byte, error) {
	dialect := p.resolveClientToolDialect(ctx, model, body)
	updated, toolMap, losses, err := convert.AdaptResponsesTools(body, dialect, nil)
	if err != nil {
		p.Reject(forward.Rejection{
			Status:          http.StatusBadRequest,
			Type:            "invalid_request_error",
			Message:         err.Error(),
			Param:           "tools",
			ObserveUpstream: true,
		})
		MarkResponseCommitted(p.c)
		wrapped := fmt.Errorf("adapt DeepSeek Responses client tools: %w", err)
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", wrapped.Error()).WithCause(wrapped)
	}
	reqCtx := p.resolveContext(ctx)
	if len(losses) > 0 {
		logging.FromContext(reqCtx).Debug(
			"gateway.prelude_responses_tool_adaptation_losses",
			zap.Int64("provider_id", p.provider.Record.ID),
			zap.String("model", model),
			zap.Int("loss_count", len(losses)),
		)
	}
	upstream.ObserveToolAdaptedConversion(
		reqCtx,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIResponses,
		dialect,
		toolMap,
		losses,
	)
	// Publish the active ToolMap only after lowering succeeds so response
	// restoration matches the outgoing request bytes adopted by PreparePrelude.
	SetOpenAIResponsesClientToolMap(p.c, toolMap)
	return updated, nil
}

// resolveClientToolDialect constructs the target convert.Dialect for prelude
// client-tool lowering from provider and model attributes. Catalog lookup
// failures fall back to a zero resolution so downstream TransformRequest keeps
// single ownership of model-attribute admission.
func (p openAIForwardPreludeAdapter) resolveClientToolDialect(ctx context.Context, model string, body []byte) convert.Dialect {
	reqCtx := p.resolveContext(ctx)
	var attrs modelrules.Resolution
	if p.s != nil {
		// Suppress catalog lookup errors during prelude tool lowering so downstream
		// TransformRequest retains single ownership of model-attribute admission.
		if resolved, err := gatewayprovider.ResolveModelAttributes(reqCtx, p.provider, model, p.s.modelAttributes(), "openai_responses"); err == nil {
			attrs = resolved.ForRequest(gatewayprovider.ModelRequestState(body))
		}
	}
	return gatewayprovider.ConversionDialect(reqCtx, p.provider, attrs, protocol.ProtocolOpenAIResponses)
}

// resolveContext returns ctx when non-nil for prelude attribute resolution and
// diagnostics. When ctx is nil, it falls back to the adapter's stored execution
// context, inbound Gin request context, or context.Background.
func (p openAIForwardPreludeAdapter) resolveContext(ctx context.Context) context.Context {
	if ctx != nil {
		return ctx
	}
	if p.ctx != nil {
		return p.ctx
	}
	if p.c != nil && p.c.Request != nil {
		return p.c.Request.Context()
	}
	return context.Background()
}
