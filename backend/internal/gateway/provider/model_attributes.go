package provider

import (
	"bytes"
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/gateway/media"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/antigravity"
	"github.com/TokenFlux/TokenRouter/internal/upstream/ollama"
)

// model_attributes.go resolves final-route capabilities, builds IR conversion
// dialects, prepares native continuity-carrier replay, and applies request
// parameter policy.
// Final-route attributes decide request sampling, reasoning controls, output
// limits, tool constraints, and image-tool options after model mapping.
// Resolution reuses a request catalogue session and group context, keeping each
// attempt on the intended catalogue and configuration view while protocol
// selection and model identity are supplied separately.
//
// ConversionDialect translates provider authentication, canonical model author
// identity, normalized Antigravity model families, and resolved execution rules
// into a pure convert.Dialect value so protocol codecs and native carrier
// replay never branch on platform names. PrepareNativeReplay short-circuits
// when no trc1 prefix is present and otherwise resolves route rules to unpack
// matching carriers or strip foreign carriers losslessly. Wire parameter policy
// separately interprets client controls to activate conditional attributes,
// applies clamping and effort aliases, validates named image-tool models before
// removing input_fidelity, and binds Grok attributes to body patching.

const (
	// Tool name byte caps bound converted function declarations and historical calls per upstream wire specification.
	openAIToolNameMaxBytes   = 64  // OpenAI, Gemini, Antigravity, and Qoder function name byte limit
	messagesToolNameMaxBytes = 128 // Anthropic Messages tool name byte limit

	// antigravityNonClaudeOutputLimit caps converted non-Claude Messages and
	// Responses output tokens on Antigravity routes, matching modelrules.antigravityPatch.
	antigravityNonClaudeOutputLimit = 65000 // tokens
)

// ResolveModelAttributes resolves attributes for the final model, admitted
// group, and current client/upstream protocols. The first optional protocol
// overrides the attempt route; request sessions are reused, otherwise
// service.NewSession supplies one for the lookup.
func ResolveModelAttributes(ctx context.Context, value *ExecutionProvider, model string, service *routing.ModelAttributeService, protocols ...string) (modelrules.Resolution, error) {
	upstreamProtocol := ""
	if value != nil {
		upstreamProtocol = string(value.Route.Protocol())
	}
	if len(protocols) > 0 {
		upstreamProtocol = protocols[0]
	}
	target := ExecutionProtocolRecord(value).ModelAttributeTarget(model, upstreamProtocol)
	if source, ok := requeststate.ClientProtocolFromContext(ctx); ok {
		target.ClientProtocol = string(source)
	}

	var groupID int64
	if group, ok := requeststate.GroupFromContext(ctx); ok && group != nil {
		groupID = group.ID
	}
	session := routing.ModelAttributesFromContext(ctx)
	// An entry can reach this adapter without an existing request session. Create
	// one from the bound service before resolving this route and its group attributes.
	if session == nil {
		session = service.NewSession()
	}
	return session.Resolve(ctx, groupID, target)
}

// ExecutionModelAttributes returns the attached attribute resolution when its
// target model exactly matches model. Nil targets, missing capabilities, and
// mismatched names return unknown attributes, so callers resolve the intended
// route before conversion.
func ExecutionModelAttributes(value *ExecutionProvider, model string) modelcatalog.Attributes {
	if value != nil && value.Capabilities != nil && value.Capabilities.Target.Model == model {
		return value.Capabilities.Attributes
	}
	return modelcatalog.Attributes{}
}

// ConversionDialect constructs the target convert.Dialect for one upstream
// attempt from provider configuration, resolved model rules, and the selected
// upstream protocol. Callers evaluate conditional request attributes through
// result.ForRequest before calling ConversionDialect when request reasoning
// controls are present; nil contexts and nil execution targets fall back to
// result.Target metadata without panicking.
//
// Replay schemes derive from result.Rules.SignatureMode, canonical model author
// identity on result.Target, normalized Antigravity claude-* and gemini-*
// families (including models/, publisher, and author-qualified prefixes), and
// upstream protocol rather than platform names alone, isolating Antigravity
// Claude signatures from Gemini models and omitting tokens on passback and
// count_tokens routes. StripEffort consults the pre-mapped route model so
// convert preserves Info().Effort for billing before clearing outgoing wire effort.
func ConversionDialect(ctx context.Context, target *ExecutionProvider, result modelrules.Resolution, upstream protocol.ProtocolID) convert.Dialect {
	if ctx == nil {
		ctx = context.Background()
	}
	if upstream == "" && target != nil && target.Route.Protocol() != "" {
		upstream = target.Route.Protocol()
	}
	if upstream == "" && result.Target.UpstreamProtocol != "" {
		upstream = protocol.ProtocolID(result.Target.UpstreamProtocol)
	}

	platform := strings.ToLower(strings.TrimSpace(result.Target.Platform))
	providerType := strings.ToLower(strings.TrimSpace(result.Target.ProviderType))
	var providerID int64
	if target != nil {
		if target.Record.ID > 0 {
			providerID = target.Record.ID
		}
		if recPlatform := strings.ToLower(strings.TrimSpace(target.Record.Platform)); recPlatform != "" {
			platform = recPlatform
		}
		if recType := strings.ToLower(strings.TrimSpace(target.Record.Type)); recType != "" {
			providerType = recType
		}
	}
	if upstream == "" && platform == "antigravity" {
		// Antigravity forwards exclusively over v1internal Gemini GenerateContent, so
		// infer that wire protocol when callers omit UpstreamProtocol on Target.
		upstream = protocol.ProtocolGeminiGenerateContent
	}

	routeTarget := result.Target
	if routeTarget.Platform == "" {
		routeTarget.Platform = platform
	}
	if routeTarget.ProviderType == "" {
		routeTarget.ProviderType = providerType
	}
	if routeTarget.UpstreamProtocol == "" {
		routeTarget.UpstreamProtocol = string(upstream)
	}
	if routeTarget.BaseURL == "" && routeTarget.CatalogProvider == "" && target != nil {
		routeTarget.BaseURL = target.Record.GetCredential("base_url")
	}

	name := platform
	if name == "" {
		name = routeTarget.ServiceID()
	}
	if name == "" {
		name = string(upstream)
	}

	shortModel := shortGoogleModel(routeTarget.Model)
	antigravityModel := shortModel
	if platform == "antigravity" {
		antigravityModel = normalizeAntigravityModel(routeTarget.Model)
	}
	countTokens := isCountTokensEndpoint(routeTarget.Endpoint)
	convertedAntigravity := platform == "antigravity" &&
		routeTarget.ClientProtocol != string(protocol.ProtocolGeminiGenerateContent)

	toolNameMaxBytes := openAIToolNameMaxBytes
	if upstream == protocol.ProtocolAnthropicMessages {
		toolNameMaxBytes = messagesToolNameMaxBytes
	}

	reasoningHistory := convert.HistoryNone
	if upstream == protocol.ProtocolOpenAIChatCompletions ||
		(upstream == protocol.ProtocolAnthropicMessages && result.Rules.SignatureMode == "passback") {
		reasoningHistory = convert.HistoryToolTurns
	}

	schemaDialect := convert.SchemaJSONSchema
	switch {
	case platform == "antigravity":
		schemaDialect = convert.SchemaAntigravity
	case upstream == protocol.ProtocolGeminiGenerateContent || platform == "gemini":
		schemaDialect = convert.SchemaGemini
	}

	attrs := result.Attributes
	minimum, maximum := attrs.ReasoningBudget()
	budgetMin, budgetMax := 0, 0
	if minimum != nil {
		budgetMin = *minimum
	}
	if maximum != nil {
		budgetMax = *maximum
	}

	adaptiveBudget := result.Rules.AdaptiveThinkingBudget
	budgetPadding := result.Rules.BudgetPadding
	outputLimit := attrs.OutputLimit
	if convertedAntigravity {
		// Normalize prefixed Antigravity Claude and Gemini variants so resource or
		// author prefixes receive the same adaptive budget, padding, and output caps
		// as bare model names resolved through modelrules.antigravityPatch.
		if adaptiveBudget == 0 && isAntigravityAdaptiveClaudeModel(antigravityModel) {
			adaptiveBudget = modelrules.AntigravityAdaptiveBudget
		}
		if budgetPadding == 0 {
			budgetPadding = antigravity.MaxTokensBudgetPadding
		}
		if !isExplicitAttributeSource(result.Sources["output_limit"]) {
			agLimit := antigravityNonClaudeOutputLimit
			if strings.HasPrefix(antigravityModel, "claude-") ||
				routeTarget.ClientProtocol == string(protocol.ProtocolOpenAIChatCompletions) {
				agLimit = modelrules.AntigravityDefaultOutputLimit
			}
			if outputLimit != nil || routeTarget.ClientProtocol != "" || result.Sources != nil {
				outputLimit = modelcatalog.MinimumLimit(outputLimit, &agLimit)
			}
		}
	}

	reasoning := (convert.ReasoningCapabilities{
		ControlsKnown:  attrs.ReasoningOptions != nil,
		Supported:      attrs.Reasoning,
		Toggle:         attrs.HasReasoningOption("toggle"),
		Efforts:        attrs.Efforts(),
		Budget:         attrs.HasReasoningOption("budget_tokens"),
		BudgetMin:      budgetMin,
		BudgetMax:      budgetMax,
		ForceBudget:    platform == "antigravity" && strings.HasPrefix(antigravityModel, "claude-"),
		AdaptiveBudget: adaptiveBudget,
		OutputLimit:    outputLimit,
		BudgetPadding:  budgetPadding,
	}).Clone()

	// Resolve effort_model_mapping against the pre-provider-mapped request or group
	// model when unplanned, because routeTarget.Model already holds the mapped target.
	sourceModel := routeTarget.Model
	if plan, ok := requeststate.RoutePlanFromContext(ctx); ok {
		if planned := plan.Models().GroupMappedModel; planned != "" {
			sourceModel = planned
		} else if requested := plan.Models().RequestedModel; requested != "" {
			sourceModel = requested
		}
	}
	policy := ExecutionModelPolicy(target)
	stripEffort := policy.ShouldStripWireEffort(ctx, sourceModel)
	if !stripEffort && platform == "antigravity" {
		// Antigravity model resolution strips a leading models/ prefix before matching
		// effort_model_mapping, so recheck the unprefixed request spelling here.
		if trimmedSource, hadModelsPrefix := strings.CutPrefix(strings.TrimSpace(sourceModel), "models/"); hadModelsPrefix && trimmedSource != "" {
			stripEffort = policy.ShouldStripWireEffort(ctx, strings.TrimSpace(trimmedSource))
		}
	}

	dropFields := result.Rules.DropFields
	dropTemperature := (attrs.Temperature != nil && !*attrs.Temperature) ||
		slices.Contains(dropFields, "temperature")
	dropTopP := (attrs.TopP != nil && !*attrs.TopP) ||
		slices.Contains(dropFields, "top_p") ||
		slices.Contains(dropFields, "topP")
	dropTopK := slices.Contains(dropFields, "top_k") ||
		slices.Contains(dropFields, "topK")
	omitStop := result.Rules.OmitStopSequences ||
		slices.Contains(dropFields, "stop") ||
		slices.Contains(dropFields, "stop_sequences") ||
		slices.Contains(dropFields, "stopSequences")
	if convertedAntigravity && isAntigravityGeminiThinkingModel(antigravityModel) {
		// Prefixed Antigravity Gemini reasoning variants miss antigravityPatch's raw
		// name check; suppress non-overridden sampling controls and stop sequences.
		if !isExplicitAttributeSource(result.Sources["temperature"]) {
			dropTemperature = true
		}
		if !isExplicitAttributeSource(result.Sources["top_p"]) {
			dropTopP = true
		}
		dropTopK = true
		omitStop = true
	}

	placeholderToken := ""
	if upstream == protocol.ProtocolGeminiGenerateContent && !countTokens {
		// Antigravity claude-* and unrecognized families reject Gemini dummy
		// thoughtSignature placeholders, whereas Gemini and Antigravity gemini-*
		// routes need them on unsigned step-leading calls.
		if platform != "antigravity" || strings.HasPrefix(antigravityModel, "gemini-") {
			placeholderToken = gemini.DummyThoughtSignature
		}
	}

	return convert.Dialect{
		Name:                name,
		ProviderID:          providerID,
		Tools:               conversionTools(target, routeTarget.ServiceID(), platform, providerType, upstream),
		ToolNameMaxBytes:    toolNameMaxBytes,
		ReplayScheme:        conversionReplayScheme(routeTarget, result.Rules.SignatureMode, platform, shortModel, antigravityModel, upstream),
		ReasoningHistory:    reasoningHistory,
		SchemaDialect:       schemaDialect,
		Reasoning:           reasoning,
		StripEffort:         stripEffort,
		DropTemperature:     dropTemperature,
		DropTopP:            dropTopP,
		DropTopK:            dropTopK,
		OmitStopSequences:   omitStop,
		OmitStoreAndInclude: platform == "grok" && upstream == protocol.ProtocolOpenAIResponses,
		AllowBillingHeader:  upstream == protocol.ProtocolQoderChat,
		PlaceholderToken:    placeholderToken,
	}
}

// PrepareNativeReplay unpacks matching trc1 continuity envelopes or strips
// incompatible envelopes inside a native Anthropic Messages, OpenAI Responses
// HTTP, or Responses WebSocket request body for target and model. Payloads
// without the "trc1:" prefix return body immediately without resolving catalog
// attributes or allocating; when "trc1:" is present, route attributes and
// conditional request rules are resolved to construct ConversionDialect before
// delegating surgical jsontext rewriting to convert.PrepareNativeReplay.
//
// Responses WebSocket routes resolve model catalog attributes under the
// "openai_responses" wire family while retaining ProtocolResponsesWebSocket on
// convert.Route so session.update and non-token frames stay byte-for-byte
// untouched. Reported replay losses are logged at debug level without blocking
// request forwarding.
func PrepareNativeReplay(
	ctx context.Context,
	target *ExecutionProvider,
	model string,
	body []byte,
	service *routing.ModelAttributeService,
	clientProto protocol.ProtocolID,
	upstreamProto protocol.ProtocolID,
) ([]byte, []ir.Loss, error) {
	if !bytes.Contains(body, []byte(ir.CarrierPrefix)) {
		return body, nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if upstreamProto == "" {
		upstreamProto = clientProto
	}
	if clientProto == "" {
		clientProto = upstreamProto
	}
	// Resolve catalog attributes under openai_responses for WebSocket turns so
	// protocol-scoped endpoint rules select the Responses slot rather than Chat.
	attrProto := string(upstreamProto)
	if upstreamProto == protocol.ProtocolResponsesWebSocket {
		attrProto = string(protocol.ProtocolOpenAIResponses)
	}
	result, err := ResolveModelAttributes(ctx, target, model, service, attrProto)
	if err != nil {
		return nil, nil, err
	}
	return PrepareNativeReplayWithResolution(ctx, target, result, body, clientProto, upstreamProto)
}

// PrepareNativeReplayWithResolution unpacks matching trc1 continuity envelopes
// or strips incompatible envelopes using an already resolved base
// modelrules.Resolution. Payloads lacking the "trc1:" prefix return body
// untouched; otherwise baseResult is specialized with ModelRequestState(body)
// to derive ConversionDialect and run convert.PrepareNativeReplay, logging any
// recorded losses at debug level.
func PrepareNativeReplayWithResolution(
	ctx context.Context,
	target *ExecutionProvider,
	baseResult modelrules.Resolution,
	body []byte,
	clientProto protocol.ProtocolID,
	upstreamProto protocol.ProtocolID,
) ([]byte, []ir.Loss, error) {
	if !bytes.Contains(body, []byte(ir.CarrierPrefix)) {
		return body, nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if upstreamProto == "" {
		upstreamProto = clientProto
	}
	if clientProto == "" {
		clientProto = upstreamProto
	}
	resolved := baseResult.ForRequest(ModelRequestState(body))
	dialect := ConversionDialect(ctx, target, resolved, upstreamProto)
	route := convert.Route{
		Client:   clientProto,
		Upstream: upstreamProto,
		Dialect:  dialect,
	}
	rewritten, losses, applied, err := convert.PrepareNativeReplayWithStatus(route, body)
	if err == nil {
		if applied {
			upstream.ObserveNativeReplayConversion(ctx, route, losses, applied)
		}
		if len(losses) > 0 {
			logging.L().Debug(
				"gateway.native_replay_losses",
				zap.String("client_protocol", string(clientProto)),
				zap.String("upstream_protocol", string(upstreamProto)),
				zap.String("dialect", dialect.Name),
				zap.Int64("provider_id", dialect.ProviderID),
				zap.Int("loss_count", len(losses)),
			)
		}
	}
	return rewritten, losses, err
}

// conversionTools selects the native ToolSupport bitmask for upstream from
// provider authentication, service host identity, and platform client-tool rules.
// OpenAI OAuth-like Responses endpoints and compatible endpoints without
// client-tool lowering retain full Responses tools; OpenAI API Key and Grok
// Responses retain hosted web_search alongside functions while lowering custom,
// namespace, and tool_search items; DeepSeek lowers to functions only.
func conversionTools(
	target *ExecutionProvider,
	serviceID string,
	platform string,
	providerType string,
	upstream protocol.ProtocolID,
) convert.ToolSupport {
	switch upstream {
	case protocol.ProtocolOpenAIResponses, protocol.ProtocolResponsesWebSocket:
		if platform == "openai" && (providerType == "oauth" || providerType == "setup-token") {
			return convert.ToolSupportAllResponses
		}
		if platform == "grok" {
			return convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch
		}
		// Official OpenAI API Key, xAI, and generic OpenAI relays accept hosted
		// {"type":"web_search"} while lowering custom, namespace, and tool_search items;
		// DeepSeek hosts accept standard function declarations only.
		if platform == "openai" {
			if serviceID == "deepseek" {
				return convert.ToolSupportFunction
			}
			return convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch
		}
		protoRecord := ExecutionProtocolRecord(target)
		if protoRecord == nil && (platform != "" || providerType != "") {
			protoRecord = &providercore.Record{Platform: platform, Type: providerType}
		}
		if (providercore.ProtocolTarget{
			Record:   protoRecord,
			Protocol: protocol.ProtocolOpenAIResponses,
		}).RequiresResponsesClientToolEncoding() {
			return convert.ToolSupportFunction
		}
		return convert.ToolSupportAllResponses

	case protocol.ProtocolAnthropicMessages, protocol.ProtocolGeminiGenerateContent:
		return convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch

	default:
		return convert.ToolSupportFunction
	}
}

// conversionReplayScheme derives the continuity token scheme issued and
// accepted by the selected upstream route from signature rules, canonical
// model author metadata, and target protocol.
//
// Passback models, Chat Completions, Qoder, and count_tokens preflight requests
// return an empty scheme so convert neither emits nor forwards opaque tokens.
// Antigravity splits claude-* and gemini-* families into distinct schemes so a
// group mixing both families never replays Gemini thought signatures onto Claude,
// and OpenCode routes require a verified canonical author reference in
// AttributeModel rather than guessing an issuer from outbound alias names.
func conversionReplayScheme(
	route modelrules.Target,
	signatureMode string,
	platform string,
	shortModel string,
	antigravityModel string,
	upstream protocol.ProtocolID,
) string {
	if isCountTokensEndpoint(route.Endpoint) || signatureMode == "passback" {
		return ""
	}
	canonicalAuthor, canonicalName := canonicalAuthorModel(route.AttributeModel)
	isOpenCode := platform == "opencode"
	serviceID := route.ServiceID()

	switch upstream {
	case protocol.ProtocolAnthropicMessages:
		if signatureMode == "anthropic" {
			return ir.SchemeAnthropic
		}
		if isOpenCode {
			if canonicalAuthor == "anthropic" && isClaudeFamilyModel(canonicalName) {
				return ir.SchemeAnthropic
			}
			return ""
		}
		switch serviceID {
		case "anthropic", "amazon-bedrock", "google-vertex-anthropic":
			return ir.SchemeAnthropic
		default:
			return ""
		}

	case protocol.ProtocolOpenAIResponses, protocol.ProtocolResponsesWebSocket:
		if isOpenCode {
			switch canonicalAuthor {
			case "openai":
				return ir.SchemeOpenAI
			case "xai":
				return ir.SchemeXAI
			default:
				return ""
			}
		}
		if platform == "grok" || serviceID == "xai" {
			return ir.SchemeXAI
		}
		if serviceID == "openai" {
			return ir.SchemeOpenAI
		}
		// Custom-URL OpenAI API Key relays without a recognized third-party host retain
		// OpenAI continuity only when the outbound model is not a known non-OpenAI family.
		if platform == "openai" && serviceID == "" && !isForeignOpenAIModel(shortModel) {
			return ir.SchemeOpenAI
		}
		return ""

	case protocol.ProtocolGeminiGenerateContent:
		if platform == "antigravity" {
			// Separate Antigravity Claude signatures from Gemini signatures so mixed-model
			// groups drop incompatible tokens locally instead of triggering upstream 400s.
			switch {
			case strings.HasPrefix(antigravityModel, "claude-"):
				return ir.SchemeAntigravityClaude
			case strings.HasPrefix(antigravityModel, "gemini-"):
				return ir.SchemeGemini
			default:
				return ""
			}
		}
		if isOpenCode {
			if canonicalAuthor == "google" {
				return ir.SchemeGemini
			}
			return ""
		}
		if platform == "gemini" || serviceID == "google" || serviceID == "google-vertex" {
			return ir.SchemeGemini
		}
		return ""

	default:
		return ""
	}
}

// canonicalAuthorModel splits a qualified "author/model" canonical reference
// into trimmed lowercase author and model components. Unqualified, incomplete,
// or multi-slash strings return empty strings so intermediary routes never
// guess an author from outbound model aliases.
func canonicalAuthorModel(attributeModel string) (string, string) {
	author, name, qualified := strings.Cut(strings.ToLower(strings.TrimSpace(attributeModel)), "/")
	if !qualified || author == "" || name == "" || strings.Contains(name, "/") {
		return "", ""
	}
	return author, name
}

// shortGoogleModel strips recognized Google models, publisher, and regional
// Vertex resource prefixes from a trimmed lowercase model identifier. Custom
// relay paths with other segment counts return the normalized input unchanged.
func shortGoogleModel(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	parts := strings.Split(normalized, "/")
	if len(parts) == 2 && parts[0] == "models" {
		return parts[1]
	}
	if len(parts) == 4 && parts[0] == "publishers" && parts[1] == "google" && parts[2] == "models" {
		return parts[3]
	}
	if len(parts) == 8 && parts[0] == "projects" && parts[1] != "" && parts[2] == "locations" && parts[3] != "" && parts[4] == "publishers" && parts[5] == "google" && parts[6] == "models" {
		return parts[7]
	}
	return normalized
}

// normalizeAntigravityModel reduces recognized Google and Anthropic resource
// paths, leading models/ prefixes, and single-segment Google or Anthropic
// author qualifiers to a lowercase Antigravity model name. Unrecognized vendor
// qualifiers, stacked prefixes, empty path segments, and nested custom relay
// paths return normalized unchanged so unknown families do not match claude-*
// or gemini-* rules.
func normalizeAntigravityModel(model string) string {
	normalized := strings.ToLower(strings.TrimSpace(model))
	// Match complete path shapes in a single pass so custom relay paths,
	// foreign publishers, and stacked qualifiers cannot collapse to a bare family name.
	parts := strings.Split(normalized, "/")
	switch len(parts) {
	case 2:
		if parts[1] != "" {
			switch parts[0] {
			case "models",
				"anthropic",
				"google",
				"gemini",
				"google-vertex",
				"google-vertex-anthropic",
				"antigravity":
				return parts[1]
			}
		}
	case 4:
		if parts[0] == "publishers" &&
			(parts[1] == "google" || parts[1] == "anthropic") &&
			parts[2] == "models" &&
			parts[3] != "" {
			return parts[3]
		}
	case 8:
		if parts[0] == "projects" &&
			parts[1] != "" &&
			parts[2] == "locations" &&
			parts[3] != "" &&
			parts[4] == "publishers" &&
			(parts[5] == "google" || parts[5] == "anthropic") &&
			parts[6] == "models" &&
			parts[7] != "" {
			return parts[7]
		}
	}
	return normalized
}

// isAntigravityAdaptiveClaudeModel reports whether normalizedModel names one of
// the registered Antigravity Claude Opus variants that encode adaptive thinking
// with AntigravityAdaptiveBudget (24,576 tokens). Trailing @version suffixes
// are stripped before matching so Vertex-style Claude identifiers resolve identically.
func isAntigravityAdaptiveClaudeModel(normalizedModel string) bool {
	base, _, _ := strings.Cut(normalizedModel, "@")
	switch base {
	case "claude-opus-4-6",
		"claude-opus-4-6-thinking",
		"claude-opus-4-7",
		"claude-opus-4-7-thinking",
		"claude-opus-4-8",
		"claude-opus-4-8-thinking":
		return true
	default:
		return false
	}
}

// isAntigravityGeminiThinkingModel reports whether normalizedModel names one of
// the registered Antigravity Gemini reasoning variants that suppress sampling
// controls and stop sequences on converted requests. Trailing @version suffixes
// are stripped before matching so versioned resource names follow the same rules.
func isAntigravityGeminiThinkingModel(normalizedModel string) bool {
	base, _, _ := strings.Cut(normalizedModel, "@")
	switch base {
	case "gemini-2.5-flash-thinking",
		"gemini-3-pro-high",
		"gemini-3.1-pro-high",
		"gemini-3.6-flash-high",
		"gemini-3.6-flash-low",
		"gemini-3.6-flash-medium",
		"gemini-3.6-flash-tiered":
		return true
	default:
		return false
	}
}

// isExplicitAttributeSource reports whether source identifies an administrator
// profile override ("config") or immutable provider publication ("provider")
// that takes precedence over platform defaults.
func isExplicitAttributeSource(source string) bool {
	return source == "config" || source == "provider"
}

// isCountTokensEndpoint reports whether endpoint identifies a token-counting
// preflight operation under snake_case or camelCase spelling. Surrounding
// whitespace and letter case are ignored so Messages and Gemini count targets match.
func isCountTokensEndpoint(endpoint string) bool {
	trimmed := strings.TrimSpace(endpoint)
	return strings.EqualFold(trimmed, "count_tokens") || strings.EqualFold(trimmed, "counttokens")
}

// isClaudeFamilyModel reports whether model starts with a registered Anthropic Claude family prefix.
func isClaudeFamilyModel(model string) bool {
	return strings.HasPrefix(model, "claude-") ||
		strings.HasPrefix(model, "opus-") ||
		strings.HasPrefix(model, "sonnet-") ||
		strings.HasPrefix(model, "haiku-")
}

// isForeignOpenAIModel reports whether a normalized model name belongs to a
// known passback or non-OpenAI vendor family. Unrecognized custom deployment
// names return false so OpenAI API-key relays retain OpenAI encrypted continuity.
func isForeignOpenAIModel(model string) bool {
	switch {
	case isClaudeFamilyModel(model),
		strings.HasPrefix(model, "gemini-"),
		strings.HasPrefix(model, "grok-"),
		strings.HasPrefix(model, "deepseek-"),
		strings.HasPrefix(model, "kimi-"),
		strings.HasPrefix(model, "moonshot-"),
		strings.HasPrefix(model, "glm-"),
		strings.HasPrefix(model, "minimax-m"),
		model == "k3",
		model == "k3-256k":
		return true
	case strings.Contains(model, "-thinking") &&
		(strings.HasPrefix(model, "qwen-") ||
			strings.HasPrefix(model, "qwen2-") ||
			strings.HasPrefix(model, "qwen3-") ||
			strings.HasPrefix(model, "qwen4-")):
		return true
	default:
		return false
	}
}

// ApplyModelAttributeParameters applies route attributes after resolving their
// request-state conditions. It removes prohibited fields, applies output
// clamping and effort aliases, and adapts platform-specific thinking and
// reasoning controls.
//
// Known unsupported OpenAI none effort is removed when the route has no toggle
// option; GLM conversion uses the resolved allowed effort set, distinguishing
// unknown options from an explicitly empty set. Adaptive Anthropic modes
// remove explicit budgets after encoding adaptive. Grok conversion runs last
// with these same resolved attributes. Field-edit errors can return nil, while
// final adaptive edits can return partial bytes with an error; callers adopt
// the result only when successful.
func ApplyModelAttributeParameters(body []byte, result modelrules.Resolution) ([]byte, error) {
	result = result.ForRequest(ModelRequestState(body))
	remove := append([]string(nil), result.Rules.DropFields...)
	if value := result.Attributes.Temperature; value != nil && !*value {
		remove = append(remove, "temperature")
	}
	if value := result.Attributes.TopP; value != nil && !*value {
		remove = append(remove, "top_p")
	}
	// Output clamping belongs to the final resolved route, after conditional
	// attributes have selected their restrictions; the Ollama helper retains bytes on declined edits.
	if result.Rules.ClampOutput && result.Attributes.OutputLimit != nil {
		body, _ = ollama.ClampMaxTokens(body, int64(*result.Attributes.OutputLimit))
	}
	var err error
	for _, field := range remove {
		if gjson.GetBytes(body, field).Exists() {
			body, err = sjson.DeleteBytes(body, field)
			if err != nil {
				return nil, err
			}
		}
	}
	// Apply route-owned effort aliases to supported protocol spellings so an
	// administrator or platform correction reaches the field actually used by this body.
	for _, path := range []string{"reasoning.effort", "reasoning_effort", "output_config.effort"} {
		effort := gjson.GetBytes(body, path)
		if mapped := result.Rules.EffortAliases[effort.String()]; effort.Exists() && mapped != "" {
			body, err = sjson.SetBytes(body, path, mapped)
			if err != nil {
				return nil, err
			}
		}
	}
	// A route with known unsupported none and no toggle cannot express disabled
	// reasoning through these effort fields; remove that spelling before OpenAI forwarding.
	if result.Target.Platform == "openai" && result.Target.ServiceID() == "" && !result.Attributes.HasReasoningOption("toggle") {
		if supported := result.Attributes.EffortSupport("none"); supported != nil && !*supported {
			for _, path := range []string{"reasoning.effort", "reasoning_effort"} {
				if gjson.GetBytes(body, path).String() == "none" {
					body, err = sjson.DeleteBytes(body, path)
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}
	// Final GLM options can differ from family defaults. Pass a nonnil empty
	// effort set when controls are known but absent, preserving that configured distinction.
	if strings.HasPrefix(strings.ToLower(result.Target.Model), "glm-") {
		options := thinkingRequestOptions(result.Target.Model)
		if result.Attributes.ReasoningOptions != nil {
			options.Efforts = append([]string{}, result.Attributes.Efforts()...)
		}
		if result.Target.UpstreamProtocol == "anthropic_messages" && options.GLM53 {
			body, _ = requeststate.NormalizeGLM53AnthropicThinking(body, options)
		} else if result.Attributes.HasReasoningOption("effort") {
			body, _ = requeststate.NormalizeGLMOpenAIReasoningEffort(body, options)
		}
	}
	// Adaptive mode encodes control through thinking.type, so an enabled request
	// loses its explicit budget after adaptive is set; propagate either failed byte edit.
	if (result.Rules.ThinkingMode == "adaptive" || result.Rules.ThinkingMode == "adaptive_between_tools") && gjson.GetBytes(body, "thinking.type").String() == "enabled" {
		body, err = sjson.SetBytes(body, "thinking.type", "adaptive")
		if err == nil {
			body, err = sjson.DeleteBytes(body, "thinking.budget_tokens")
		}
	}
	if err == nil && (result.Target.Platform == "grok" || result.Target.ServiceID() == "xai") {
		codec := GrokBodyCodec()
		codec.Attributes = func(string) modelcatalog.Attributes { return result.Attributes }
		if result.Target.UpstreamProtocol == "openai_chat_completions" {
			return codec.NormalizeGrokChatReasoningEffort(body, result.Target.Model)
		}
		return codec.NormalizeGrokResponsesReasoningEffort(body, result.Target.Model)
	}
	return body, err
}

// ModelRequestState reads reasoning controls used to evaluate conditional
// attributes after group policy. Each top-level path retains its first
// observed value, and the first nonblank string among nested reasoning, flat
// effort, and output effort supplies initial state.
//
// Explicit recognized thinking.type then replaces the inferred toggle. Gemini
// camel and snake config forms are visited afterward in that order; later
// level or numeric budget observations override prior state, with a zero
// budget meaning disabled. Effort strings are copied away from parsed body
// storage. Parsing is lenient and does not validate the full request, so
// ingress validation remains the caller's responsibility.
func ModelRequestState(body []byte) modelrules.RequestState {
	var reasoning, effort, output, thinking, camelConfig, snakeConfig gjson.Result
	// Read the controls in one pass so large inputs are skipped once. Each
	// path keeps its first match, including null, as gjson.GetBytes does.
	wirejson.ParseView(body).ForEach(func(key, value gjson.Result) bool {
		var field *gjson.Result
		switch key.Str {
		case "reasoning":
			field, value = &reasoning, value.Get("effort")
		case "reasoning_effort":
			field = &effort
		case "output_config":
			field, value = &output, value.Get("effort")
		case "thinking":
			field, value = &thinking, value.Get("type")
		case "generationConfig":
			field = &camelConfig
		case "generation_config":
			field = &snakeConfig
		}
		if field != nil && !field.Exists() {
			*field = value
		}
		return true
	})
	state := modelrules.RequestState{}
	for _, value := range []gjson.Result{reasoning, effort, output} {
		if value.Type == gjson.String && strings.TrimSpace(value.String()) != "" {
			effort := strings.Clone(strings.ToLower(strings.TrimSpace(value.String())))
			state.Effort = &effort
			thinking := effort != "none"
			state.Thinking = &thinking
			break
		}
	}
	if thinking.Type == gjson.String {
		switch thinking.String() {
		case "enabled", "adaptive", "between_tools":
			thinking := true
			state.Thinking = &thinking
		case "disabled", "off", "none":
			thinking := false
			state.Thinking = &thinking
		}
	}
	// Gemini aliases are read after ordinary controls. Their loop order supplies
	// later-observation precedence when both camel and snake spellings occur in one body.
	for _, config := range []gjson.Result{camelConfig, snakeConfig} {
		for _, thinkingName := range []string{"thinkingConfig", "thinking_config"} {
			thinkingConfig := config.Get(thinkingName)
			for _, key := range []string{"thinkingLevel", "thinking_level"} {
				if level := thinkingConfig.Get(key); level.Type == gjson.String {
					effort := strings.Clone(strings.ToLower(level.String()))
					state.Effort = &effort
					thinking := effort != "none"
					state.Thinking = &thinking
				}
			}
			for _, key := range []string{"thinkingBudget", "thinking_budget"} {
				if budget := thinkingConfig.Get(key); budget.Type == gjson.Number {
					// Zero budget expresses disabled thinking in this state.
					thinking := budget.Int() != 0
					state.Thinking = &thinking
				}
			}
		}
	}
	return state
}

// PrepareModelAttributeParameters applies request attributes using one session
// for the final model and named image-tool models. Main parameter rewriting
// runs first; its error can return partially rewritten bytes, which callers
// adopt only on success.
//
// An image tool can name a different model from its text driver. Resolve that
// tool's own media attributes before removing input_fidelity, so the driver's
// restrictions cannot be mistaken for the image model's capabilities. Image
// lookup or field-edit failures return nil and stop preparation. The provider
// retains the main model's resolution; named and default image tool models are
// validated independently against the same request attribute session.
func PrepareModelAttributeParameters(ctx context.Context, value *ExecutionProvider, model string, body []byte, service *routing.ModelAttributeService, protocols ...string) ([]byte, error) {
	if err := upstream.ValidateGIFJSONControls(body); err != nil {
		return nil, apperror.BadRequest("IMAGE_GIF_PARAMS_INVALID", err.Error())
	}
	if routing.ModelAttributesFromContext(ctx) == nil {
		ctx = routing.WithModelAttributes(ctx, service)
	}
	result, err := ResolveModelAttributes(ctx, value, model, service, protocols...)
	if err != nil {
		return nil, err
	}
	if value != nil {
		value.Capabilities = &result
	}
	body, err = ApplyModelAttributeParameters(body, result)
	if err != nil {
		return body, err
	}
	// Hosted image tools can target a different model from the text driver.
	// Validate each tool's selected or default image model before removing a
	// fidelity field rejected by that tool route's media declaration.
	for index, tool := range gjson.GetBytes(body, "tools").Array() {
		if tool.Get("type").String() != "image_generation" {
			continue
		}
		toolModel := strings.TrimSpace(tool.Get("model").String())
		if toolModel == "" {
			toolModel = "gpt-image-2"
		}
		image, err := ResolveModelAttributes(ctx, value, toolModel, service, protocols...)
		if err != nil {
			return nil, err
		}
		if err := routing.RequireImageOutput(image.Attributes); err != nil {
			return nil, err
		}
		// Lifecycle streaming belongs to Responses, while frame controls belong
		// to its image tool. Normalize only the tool so OAuth event delivery remains intact.
		if strings.EqualFold(tool.Get("output_format").String(), "gif") || tool.Get("frame_count").Exists() || tool.Get("frame_duration_ms").Exists() {
			parsed := &upstream.ImageRequest{OutputFormat: tool.Get("output_format").String(), Size: tool.Get("size").String(), HasMask: tool.Get("input_image_mask").Exists()}
			for _, field := range []struct {
				name   string // numeric image control name
				target **int  // parsed optional integer destination
			}{{"frame_count", &parsed.FrameCount}, {"frame_duration_ms", &parsed.FrameDurationMS}, {"output_compression", &parsed.OutputCompression}} {
				raw := tool.Get(field.name)
				if raw.Exists() {
					number, parseErr := strconv.Atoi(raw.Raw)
					if raw.Type != gjson.Number || parseErr != nil {
						return nil, apperror.BadRequest("IMAGE_GIF_PARAMS_INVALID", field.name+" must be an integer")
					}
					*field.target = &number
				}
			}
			if raw := tool.Get("partial_images"); raw.Exists() && raw.Raw != "0" {
				return nil, apperror.BadRequest("IMAGE_GIF_PARAMS_INVALID", "GIF output does not support partial images")
			}
			if err := parsed.NormalizeGIF(); err != nil {
				return nil, apperror.BadRequest("IMAGE_GIF_PARAMS_INVALID", err.Error())
			}
			if err := RequireImageGIF(value, protocol.ProtocolOpenAIResponses, image.Attributes); err != nil {
				return nil, err
			}
			prefix := "tools." + strconv.Itoa(index) + "."
			body, err = sjson.SetBytes(body, prefix+"frame_count", *parsed.FrameCount)
			if err != nil {
				return nil, err
			}
			body, err = sjson.SetBytes(body, prefix+"frame_duration_ms", *parsed.FrameDurationMS)
			if err != nil {
				return nil, err
			}
		}
		media := image.Attributes.Media
		if media != nil && media.InputFidelity != nil && !*media.InputFidelity {
			body, err = sjson.DeleteBytes(body, "tools."+strconv.Itoa(index)+".input_fidelity")
			if err != nil {
				return nil, err
			}
		}
	}
	return body, nil
}

// PrepareGrokResponses resolves Responses-route attributes, applies parameter
// policy, then patches the body through a codec bound to those same
// attributes. Errors stop preparation, allowing cache identity and forwarding
// to share the returned encoded body.
func PrepareGrokResponses(ctx context.Context, provider *ExecutionProvider, model string, body []byte, service *routing.ModelAttributeService) ([]byte, error) {
	resolution, err := ResolveModelAttributes(ctx, provider, model, service, "openai_responses")
	if err != nil {
		return nil, err
	}
	body, err = ApplyModelAttributeParameters(body, resolution)
	if err != nil {
		return nil, err
	}
	codec := GrokBodyCodec()
	codec.Attributes = func(string) modelcatalog.Attributes { return resolution.Attributes }
	return codec.PatchGrokResponsesBody(body, model)
}

// ValidateResponsesImageModels checks every selected hosted image model on one route.
// Empty identities select the hosted default. Callers retain a request
// attribute session so candidate filtering and outgoing preparation share overrides.
func ValidateResponsesImageModels(ctx context.Context, value *ExecutionProvider, models []string, service *routing.ModelAttributeService, protocols ...string) error {
	if routing.ModelAttributesFromContext(ctx) == nil {
		ctx = routing.WithModelAttributes(ctx, service)
	}
	for _, model := range models {
		if model == "" {
			model = "gpt-image-2"
		}
		resolution, err := ResolveModelAttributes(ctx, value, model, service, protocols...)
		if err != nil {
			return err
		}
		if err := routing.RequireImageOutput(resolution.Attributes); err != nil {
			return err
		}
	}
	return nil
}

// ResolveResponsesImageBilling selects the hosted tool or confirmed converted
// image identity and validates its output capability. Text-driver names never
// supply a billing hint; a native tool without a model uses its registered
// default, which undergoes the same route attribute validation.
func ResolveResponsesImageBilling(ctx context.Context, value *ExecutionProvider, body []byte, service *routing.ModelAttributeService) (media.OpenAIResponsesImageBillingConfig, error) {
	if routing.ModelAttributesFromContext(ctx) == nil {
		ctx = routing.WithModelAttributes(ctx, service)
	}
	protocol := "openai_responses"
	if value != nil && value.Route.Protocol() != "" {
		protocol = string(value.Route.Protocol())
	}
	if err := ValidateResponsesImageModels(ctx, value, media.ResponsesImageModels(body), service, protocol); err != nil {
		return media.OpenAIResponsesImageBillingConfig{}, err
	}
	confirmed := ""
	if value != nil && value.Capabilities != nil && value.Capabilities.Attributes.OutputOnly("image") {
		confirmed = value.Capabilities.Target.Model
	}
	config, err := ImageIntent().ResolveOpenAIResponsesImageBillingConfigDetailedFromBody(body, confirmed)
	if err != nil || config.Model == "" {
		return config, err
	}
	resolution, err := ResolveModelAttributes(ctx, value, config.Model, service, protocol)
	if err != nil {
		return config, err
	}
	return config, routing.RequireImageOutput(resolution.Attributes)
}

// ResolvedImageOutput supplies image classification for cooldown observations.
// It uses the retained request session and final provider identity. Failed
// metadata reads provide no classification evidence; request admission separately
// propagates those errors before any upstream image operation.
func ResolvedImageOutput(ctx context.Context, record *providercore.Record, model string) bool {
	if record == nil {
		return false
	}
	target, err := ProviderForProtocolAttempt(ctx, &ExecutionProvider{Record: *record})
	if err != nil {
		// Cooldown classification has no output evidence for an unresolved route.
		// The selector and forwarding preparation report that route error independently.
		return false
	}
	resolution, err := ResolveModelAttributes(ctx, target, model, nil)
	if err != nil {
		// Failed metadata cannot classify a diagnostic as an image failure. Request
		// admission separately returns the retained profile error before upstream execution.
		return false
	}
	supported := resolution.Attributes.OutputSupport("image")
	return supported != nil && *supported
}

// RequireImageGIF checks provider transport policy and final model capabilities.
// Administrators can declare formats for aliases through model attributes, but
// those declarations cannot override a provider's disabled GIF switch or native
// streaming-only endpoint. Unknown output formats do not establish GIF support.
func RequireImageGIF(value *ExecutionProvider, operation protocol.ProtocolID, attrs modelcatalog.Attributes) error {
	if value == nil || !ExecutionProtocolRecord(value).SupportsImageGIF(operation) || attrs.Media == nil || !slices.Contains(attrs.Media.OutputFormats, "gif") {
		return apperror.BadRequest("IMAGE_GIF_UNSUPPORTED", "the selected provider or model does not support GIF output")
	}
	return nil
}
