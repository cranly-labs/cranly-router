package convert

import (
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// dialect.go re-exports target endpoint capability descriptors from ir and
// applies cross-protocol parameter filters and default request settings.
// Upstream protocols differ in supported tool categories, identifier byte
// limits, reasoning history retention, and default transport flags even when
// driven through one canonical IR pipeline.
//
// Separating cross-protocol default injection from codec-level EncodeRequest
// preserves single-protocol round-trip fixed points under Law Q2 while ensuring
// converted requests carry target-required output floors, encrypted continuity
// flags, medium-and-above Anthropic thinking activation, capability-guarded
// disabled-thinking effort fallbacks, and model-attribute sampling or effort
// restrictions. Every dropped sampling parameter or custom stop sequence
// appends an original-coordinate ir.Loss record so essence verification
// accounts for target dialect pruning.

const (
	// ToolSupport constants re-export bitmask flags describing which tool
	// declaration and invocation categories an upstream endpoint accepts natively.
	ToolSupportNone            = ir.ToolSupportNone            // no native tool support configured
	ToolSupportFunction        = ir.ToolSupportFunction        // standard JSON-schema function tools
	ToolSupportCustom          = ir.ToolSupportCustom          // free-form custom grammar tools
	ToolSupportNamespace       = ir.ToolSupportNamespace       // scoped Responses tool namespaces
	ToolSupportClientSearch    = ir.ToolSupportClientSearch    // client-executed tool_search discovery
	ToolSupportHostedWebSearch = ir.ToolSupportHostedWebSearch // provider-hosted web search declarations
	ToolSupportAllResponses    = ir.ToolSupportAllResponses    // full Responses native tool support

	// HistoryMode constants re-export rules governing when unsigned plaintext
	// reasoning segments return on historical assistant turns.
	HistoryNone      = ir.HistoryNone      // omit unsigned plaintext reasoning on all turns
	HistoryToolTurns = ir.HistoryToolTurns // retain plaintext reasoning on tool-calling turns
	HistoryAll       = ir.HistoryAll       // retain plaintext reasoning on every assistant turn

	// SchemaDialect constants re-export JSON Schema cleaning dialects enforced
	// by target upstream tool and structured-output encoders.
	SchemaJSONSchema  = ir.SchemaJSONSchema  // standard JSON Schema pass-through
	SchemaGemini      = ir.SchemaGemini      // Google Gemini uppercase schema subset
	SchemaAntigravity = ir.SchemaAntigravity // Antigravity lowercase seven-key cleaned schema subset with constraint hints

	// Default tool name byte caps and cross-protocol Responses output floors
	// align converted requests with upstream endpoint validation rules.
	defaultOpenAIToolNameMaxBytes   = 64  // default function name byte limit for OpenAI, Gemini, Antigravity, Qoder, and fallback upstreams
	defaultMessagesToolNameMaxBytes = 128 // maximum byte length for Anthropic Messages tool names
	minResponsesMaxOutputTokens     = 128 // minimum positive max_output_tokens sent to Responses
)

// canonicalReasoningEfforts lists recognized reasoning effort tiers from
// lowest to highest intensity for capability-guarded fallback selection.
var canonicalReasoningEfforts = []string{"none", "minimal", "low", "medium", "high", "xhigh", "max"}

// Dialect aliases ir.Dialect so callers of convert configure endpoint capabilities without extra imports.
type Dialect = ir.Dialect

// ToolSupport aliases ir.ToolSupport for upstream tool capability bitmasks.
type ToolSupport = ir.ToolSupport

// HistoryMode aliases ir.HistoryMode for plaintext reasoning history retention policies.
type HistoryMode = ir.HistoryMode

// SchemaDialect aliases ir.SchemaDialect for target JSON Schema normalization modes.
type SchemaDialect = ir.SchemaDialect

// ReasoningCapabilities aliases ir.ReasoningCapabilities for model thinking effort and budget controls.
type ReasoningCapabilities = ir.ReasoningCapabilities

// ClientView aliases ir.ClientView for client-facing response model naming and request echoing.
type ClientView = ir.ClientView

// Runtime aliases ir.Runtime for deterministic clock and response-side ID injection.
type Runtime = ir.Runtime

// DefaultRuntime returns a production Runtime backed by time.Now and crypto/rand hex identifiers.
func DefaultRuntime() Runtime {
	return ir.DefaultRuntime()
}

// resolveEffectiveDialect returns a detached copy of route.Dialect populated
// with protocol-natural defaults for unset Tools, ToolNameMaxBytes,
// ReasoningHistory, and cross-protocol Gemini SchemaDialect fields. Explicit
// caller capability settings remain intact, and same-protocol Chat, Messages,
// Responses, and Gemini routes preserve empty ReasoningHistory and
// SchemaDialect so single-protocol Law Q2 round-trips hold.
func resolveEffectiveDialect(route Route) Dialect {
	out := route.Dialect.Clone()
	switch route.Upstream {
	case protocol.ProtocolOpenAIChatCompletions:
		if out.Tools == 0 {
			out.Tools = ToolSupportFunction
		}
		if out.ToolNameMaxBytes <= 0 {
			out.ToolNameMaxBytes = defaultOpenAIToolNameMaxBytes
		}
		if out.ReasoningHistory == "" && route.Client != route.Upstream {
			out.ReasoningHistory = HistoryToolTurns
		}

	case protocol.ProtocolAnthropicMessages:
		if out.Tools == 0 {
			out.Tools = ToolSupportFunction | ToolSupportHostedWebSearch
		}
		if out.ToolNameMaxBytes <= 0 {
			out.ToolNameMaxBytes = defaultMessagesToolNameMaxBytes
		}
		if out.ReasoningHistory == "" && route.Client != route.Upstream {
			out.ReasoningHistory = HistoryNone
		}

	case protocol.ProtocolOpenAIResponses, protocol.ProtocolResponsesWebSocket:
		if out.Tools == 0 {
			out.Tools = ToolSupportAllResponses
		}
		if out.ToolNameMaxBytes <= 0 {
			out.ToolNameMaxBytes = defaultOpenAIToolNameMaxBytes
		}
		if out.ReasoningHistory == "" && route.Client != route.Upstream {
			out.ReasoningHistory = HistoryNone
		}

	case protocol.ProtocolGeminiGenerateContent:
		if out.Tools == 0 {
			out.Tools = ToolSupportFunction | ToolSupportHostedWebSearch
		}
		if out.ToolNameMaxBytes <= 0 {
			out.ToolNameMaxBytes = defaultOpenAIToolNameMaxBytes
		}
		if out.ReasoningHistory == "" && route.Client != route.Upstream {
			out.ReasoningHistory = HistoryNone
		}
		if out.SchemaDialect == "" && route.Client != route.Upstream {
			out.SchemaDialect = SchemaGemini
		}

	case protocol.ProtocolQoderChat:
		if out.Tools == 0 {
			out.Tools = ToolSupportFunction
		}
		if out.ToolNameMaxBytes <= 0 {
			out.ToolNameMaxBytes = defaultOpenAIToolNameMaxBytes
		}
		if out.ReasoningHistory == "" {
			out.ReasoningHistory = HistoryNone
		}

	default:
		if out.Tools == 0 {
			out.Tools = ToolSupportFunction
		}
		if out.ToolNameMaxBytes <= 0 {
			out.ToolNameMaxBytes = defaultOpenAIToolNameMaxBytes
		}
		if out.ReasoningHistory == "" && route.Client != route.Upstream {
			out.ReasoningHistory = HistoryNone
		}
	}
	return out
}

// applyDialectRequestAdjustments enforces StripEffort, sampling drops, stop
// sequence omissions, and cross-protocol transport, reasoning, and tool
// defaults onto req in place. Rejected reasoning effort tiers such as "ultra"
// return an error, while dropped sampling, stop, or relocated system parameters
// return original-coordinate ir.Loss records.
//
// Same-protocol routes return immediately after explicit dialect pruning so
// single-protocol round-trips satisfy Law Q2 without synthetic defaults.
// Cross-protocol routes normalize non-strict tool defaults, strip incompatible
// Anthropic user fingerprints, and apply target-specific Responses, Messages,
// Chat, and Qoder request rules.
func applyDialectRequestAdjustments(req *ir.Request, coords *sourceCoords, route Route) ([]ir.Loss, error) {
	var losses []ir.Loss
	dialect := route.Dialect

	if dialect.StripEffort && req.Reasoning != nil {
		req.Reasoning.Effort = ""
	}
	if dialect.DropTemperature && req.Sampling.Temperature != nil {
		req.Sampling.Temperature = nil
		losses = append(losses, ir.Loss{
			Kind:   ir.LossSamplingDropped,
			Path:   "sampling.temperature",
			Detail: "temperature",
		})
	}
	if dialect.DropTopP && req.Sampling.TopP != nil {
		req.Sampling.TopP = nil
		losses = append(losses, ir.Loss{
			Kind:   ir.LossSamplingDropped,
			Path:   "sampling.top_p",
			Detail: "top_p",
		})
	}
	if dialect.DropTopK && req.Sampling.TopK != nil {
		req.Sampling.TopK = nil
		losses = append(losses, ir.Loss{
			Kind:   ir.LossSamplingDropped,
			Path:   "sampling.top_k",
			Detail: "top_k",
		})
	}
	if dialect.OmitStopSequences && len(req.Stop) > 0 {
		req.Stop = nil
		losses = append(losses, ir.Loss{
			Kind: ir.LossStopDropped,
			Path: "stop",
		})
	}

	if route.Client == route.Upstream {
		return losses, nil
	}

	losses = append(losses, normalizeCrossProtocolToolStrict(req, coords, route)...)
	if route.Client == protocol.ProtocolAnthropicMessages &&
		(route.Upstream == protocol.ProtocolOpenAIResponses || route.Upstream == protocol.ProtocolOpenAIChatCompletions) {
		// Anthropic metadata.user_id carries long JSON client fingerprints (over 64
		// bytes in Claude Code) that violate OpenAI user length caps and Chat store:false rules.
		req.UserID = ""
		req.MetadataUserID = ""
	}

	switch route.Upstream {
	case protocol.ProtocolOpenAIResponses:
		req.Stream = true
		if route.Client == protocol.ProtocolAnthropicMessages && prependSystemAsDeveloper(req, coords) {
			losses = append(losses, ir.Loss{
				Kind: ir.LossSystemRelocated,
				Path: "system",
			})
		}
		if err := injectCrossProtocolResponsesDefaults(req, route.Client, dialect); err != nil {
			return nil, err
		}

	case protocol.ProtocolAnthropicMessages:
		req.Stream = true
		if req.ParallelToolCalls != nil && *req.ParallelToolCalls {
			req.ParallelToolCalls = nil
		}
		applyCrossProtocolMessagesReasoningDefaults(req, dialect)

	case protocol.ProtocolOpenAIChatCompletions:
		if req.Stream && req.IncludeUsage == nil {
			inc := true
			req.IncludeUsage = &inc
		}
		if route.Client == protocol.ProtocolAnthropicMessages {
			if err := injectMessagesToChatDefaults(req, dialect); err != nil {
				return nil, err
			}
		}

	case protocol.ProtocolQoderChat:
		if !dialect.StatelessResponses {
			// Non-Responses clients such as Chat Completions may supply "store": true as an
			// ordinary request flag without requesting stateful Responses continuation.
			req.Store = nil
		}
	}
	return losses, nil
}

// normalizeCrossProtocolToolStrict aligns non-strict function tool defaults
// across protocols so OpenAI Responses' server-side strict=true default never
// rejects standard schemas, Messages-to-Chat and Messages-to-Qoder retain
// explicit strict=false parity, and Anthropic endpoints omit redundant
// strict=false. Every rewritten declaration with a non-negative original index
// records a LossSchemaApproximated("strict") entry so VerifyEssence reconciles the toggle.
func normalizeCrossProtocolToolStrict(req *ir.Request, coords *sourceCoords, route Route) []ir.Loss {
	var losses []ir.Loss
	switch {
	case route.Upstream == protocol.ProtocolOpenAIResponses ||
		(route.Client == protocol.ProtocolAnthropicMessages &&
			(route.Upstream == protocol.ProtocolOpenAIChatCompletions || route.Upstream == protocol.ProtocolQoderChat)):
		for i := range req.Tools {
			if req.Tools[i].Kind == ir.ToolFunction && req.Tools[i].Strict == nil {
				strictFalse := false
				req.Tools[i].Strict = &strictFalse
				if origIdx := coords.origToolIndex(i); origIdx >= 0 {
					losses = append(losses, ir.Loss{
						Kind:   ir.LossSchemaApproximated,
						Path:   fmt.Sprintf("tools[%d]", origIdx),
						Detail: "strict",
					})
				}
			}
		}

	case route.Upstream == protocol.ProtocolAnthropicMessages:
		for i := range req.Tools {
			if req.Tools[i].Kind == ir.ToolFunction && req.Tools[i].Strict != nil && !*req.Tools[i].Strict {
				req.Tools[i].Strict = nil
				if origIdx := coords.origToolIndex(i); origIdx >= 0 {
					losses = append(losses, ir.Loss{
						Kind:   ir.LossSchemaApproximated,
						Path:   fmt.Sprintf("tools[%d]", origIdx),
						Detail: "strict",
					})
				}
			}
		}
	}
	return losses
}

// applyCrossProtocolMessagesReasoningDefaults enables Anthropic extended
// thinking on cross-protocol requests carrying medium, high, xhigh, or max
// reasoning effort without an explicit thinking mode or token budget.
//
// Nil requests, StripEffort routes, explicit Mode or BudgetTokens inputs, low
// or disabled effort tiers, and targets declaring Supported=false leave
// req.Reasoning.Mode unchanged. Keeping cross-protocol activation here
// preserves single-protocol Law Q2 round-trips and low-effort output_config
// mappings inside codec/messages.
func applyCrossProtocolMessagesReasoningDefaults(req *ir.Request, dialect Dialect) {
	if req == nil || req.Reasoning == nil || dialect.StripEffort {
		return
	}
	if req.Reasoning.Mode != "" || req.Reasoning.BudgetTokens != nil {
		return
	}
	if dialect.Reasoning.Supported != nil && !*dialect.Reasoning.Supported {
		return
	}
	switch protocol.NormalizeReasoningEffort(req.Reasoning.Effort) {
	case "medium", "high", "xhigh", "max":
		req.Reasoning.Mode = "enabled"
	}
}

// injectMessagesToChatDefaults populates cross-protocol Chat Completions
// defaults for Anthropic Messages requests, enabling parallel_tool_calls when
// unspecified, enforcing the 128-token positive completion floor, and
// resolving default or disabled reasoning_effort tiers. Unsupported "ultra"
// effort values return an error unless dialect.StripEffort suppresses effort emission.
func injectMessagesToChatDefaults(req *ir.Request, dialect Dialect) error {
	if req.Reasoning != nil && !dialect.StripEffort {
		trimmedEffort := strings.TrimSpace(req.Reasoning.Effort)
		if strings.EqualFold(trimmedEffort, "ultra") {
			return fmt.Errorf("reasoning effort %q is not supported", trimmedEffort)
		}
	}
	if req.ParallelToolCalls == nil {
		enabled := true
		req.ParallelToolCalls = &enabled
	}
	if req.MaxOutputTokens != nil && *req.MaxOutputTokens > 0 && *req.MaxOutputTokens < minResponsesMaxOutputTokens {
		floor := int64(minResponsesMaxOutputTokens)
		req.MaxOutputTokens = &floor
	}
	applyMessagesToChatReasoningDefaults(req, dialect)
	return nil
}

// applyMessagesToChatReasoningDefaults maps Anthropic Messages thinking and
// effort settings onto Chat Completions reasoning_effort defaults. Explicitly
// disabled thinking consults dialect.Reasoning so "none" is emitted only when
// the target model declares support for it, whereas active turns default to
// "medium" and downgrade "max" to "xhigh" when omitted by the catalog.
func applyMessagesToChatReasoningDefaults(req *ir.Request, dialect Dialect) {
	if dialect.StripEffort {
		return
	}
	if isMessagesThinkingDisabled(req.Reasoning) {
		// Guard disabled-thinking Chat conversions with target model capabilities so
		// models lacking "none" support fall back safely or omit reasoning_effort.
		req.Reasoning.Effort = resolveDisabledThinkingEffort(dialect.Reasoning)
		return
	}
	if req.Reasoning == nil {
		req.Reasoning = &ir.ReasoningConfig{}
	}
	effort := strings.ToLower(strings.TrimSpace(req.Reasoning.Effort))
	if effort == "" {
		effort = "medium"
	} else if effort == "max" && dialect.Reasoning.ControlsKnown && !containsEffortFold(dialect.Reasoning.Efforts, "max") {
		// Downgrade max effort to xhigh when catalog controls are known and omit max,
		// while preserving max for unlisted models whose ControlsKnown is false.
		effort = "xhigh"
	}
	req.Reasoning.Effort = effort
}

// injectCrossProtocolResponsesDefaults populates section 4.2 cross-protocol
// OpenAI Responses defaults according to clientProto and dialect without
// affecting native Responses requests. Unsupported "ultra" effort values return
// a descriptive error before encoding.
func injectCrossProtocolResponsesDefaults(req *ir.Request, clientProto protocol.ProtocolID, dialect Dialect) error {
	if req.Reasoning != nil && !dialect.StripEffort {
		trimmedEffort := strings.TrimSpace(req.Reasoning.Effort)
		if strings.EqualFold(trimmedEffort, "ultra") {
			return fmt.Errorf("reasoning effort %q is not supported", trimmedEffort)
		}
	}

	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		if req.ParallelToolCalls == nil {
			enabled := true
			req.ParallelToolCalls = &enabled
		}
		if req.Output == nil {
			req.Output = &ir.OutputFormat{Verbosity: "medium"}
		} else if req.Output.Verbosity == "" {
			req.Output.Verbosity = "medium"
		}
		// Enforce the 128-token Responses output floor on positive limits.
		if req.MaxOutputTokens != nil && *req.MaxOutputTokens > 0 && *req.MaxOutputTokens < minResponsesMaxOutputTokens {
			floor := int64(minResponsesMaxOutputTokens)
			req.MaxOutputTokens = &floor
		}
		applyMessagesToResponsesReasoningDefaults(req, dialect)

	case protocol.ProtocolOpenAIChatCompletions:
		req.Stream = true
		// Enforce the 128-token Responses output floor on positive limits.
		if req.MaxOutputTokens != nil && *req.MaxOutputTokens > 0 && *req.MaxOutputTokens < minResponsesMaxOutputTokens {
			floor := int64(minResponsesMaxOutputTokens)
			req.MaxOutputTokens = &floor
		}
		if req.Reasoning != nil && strings.TrimSpace(req.Reasoning.Effort) != "" && req.Reasoning.Summary == "" {
			req.Reasoning.Summary = "auto"
		}
	}

	if !dialect.OmitStoreAndInclude {
		if req.Reasoning == nil {
			req.Reasoning = &ir.ReasoningConfig{}
		}
		req.Reasoning.IncludeEncrypted = true
		if req.Store == nil {
			storeFalse := false
			req.Store = &storeFalse
		}
	}
	return nil
}

// applyMessagesToResponsesReasoningDefaults maps Anthropic Messages thinking
// and effort controls onto OpenAI Responses effort and summary defaults.
// Explicitly disabled thinking omits summary and resolves effort through
// dialect.Reasoning so endpoints rejecting "none" receive a supported fallback
// or omit effort, while active "max" downgrades to "xhigh" when omitted by the catalog.
func applyMessagesToResponsesReasoningDefaults(req *ir.Request, dialect Dialect) {
	if isMessagesThinkingDisabled(req.Reasoning) {
		// Guard disabled-thinking Responses conversions with dialect.Reasoning and
		// suppress summary="auto" so models lacking "none" never fail with HTTP 400.
		if dialect.StripEffort {
			req.Reasoning.Effort = ""
		} else {
			req.Reasoning.Effort = resolveDisabledThinkingEffort(dialect.Reasoning)
		}
		req.Reasoning.Summary = ""
		return
	}

	if req.Reasoning == nil {
		req.Reasoning = &ir.ReasoningConfig{}
	}
	if !dialect.StripEffort {
		effort := strings.ToLower(strings.TrimSpace(req.Reasoning.Effort))
		if effort == "" {
			effort = "medium"
		} else if effort == "max" && dialect.Reasoning.ControlsKnown && !containsEffortFold(dialect.Reasoning.Efforts, "max") {
			// Downgrade max effort to xhigh when catalog controls are known and omit max,
			// while preserving max for unlisted models whose ControlsKnown is false.
			effort = "xhigh"
		}
		req.Reasoning.Effort = effort
	}
	if req.Reasoning.Summary == "" {
		req.Reasoning.Summary = "auto"
	}
}

// isMessagesThinkingDisabled reports whether rc explicitly disables Anthropic
// Messages thinking via mode="disabled", budget_tokens=0, or effort="none".
func isMessagesThinkingDisabled(rc *ir.ReasoningConfig) bool {
	if rc == nil {
		return false
	}
	return rc.Mode == "disabled" ||
		(rc.BudgetTokens != nil && *rc.BudgetTokens == 0) ||
		strings.EqualFold(strings.TrimSpace(rc.Effort), "none")
}

// resolveDisabledThinkingEffort selects a target-compatible effort label from
// rc when an Anthropic Messages request explicitly disables thinking.
//
// Upstream endpoints reject unrecognized effort values with HTTP 400: GPT-5.4
// and Kimi Coding expose "none", GPT-5 exposes "minimal", pure reasoning models
// such as o3 expose only active tiers, and non-reasoning, toggle-only, or
// uncatalogued endpoints accept no "none" effort string. Returning "none" only
// when rc.ControlsKnown and rc.Efforts explicitly list "none", falling back to
// "minimal" or the lowest declared rank on non-toggle reasoning models, and
// returning "" otherwise prevents invalid effort emissions across all families.
func resolveDisabledThinkingEffort(rc ReasoningCapabilities) string {
	if !rc.ControlsKnown {
		return ""
	}
	if rc.Supported != nil && !*rc.Supported {
		return ""
	}
	if containsEffortFold(rc.Efforts, "none") {
		return "none"
	}
	if containsEffortFold(rc.Efforts, "minimal") {
		return "minimal"
	}
	if !rc.Toggle && len(rc.Efforts) > 0 {
		// Pure reasoning models without a disable toggle cannot turn thinking off;
		// select their lowest declared canonical tier to minimize reasoning budget.
		return lowestSupportedEffort(rc.Efforts)
	}
	// Toggleable models without "none" or "minimal" (such as DeepSeek and Kimi
	// dual-protocol entries) enable thinking when given active effort tiers, so
	// omit effort completely to preserve disabled reasoning.
	return ""
}

// lowestSupportedEffort returns the lowest canonical effort tier from
// canonicalReasoningEfforts present in efforts, or "" when none match.
func lowestSupportedEffort(efforts []string) string {
	for _, rank := range canonicalReasoningEfforts {
		if containsEffortFold(efforts, rank) {
			return rank
		}
	}
	return ""
}

// containsEffortFold reports whether efforts contains target under trimmed
// ASCII case-insensitive comparison.
func containsEffortFold(efforts []string, target string) bool {
	return slices.ContainsFunc(efforts, func(v string) bool {
		return strings.EqualFold(strings.TrimSpace(v), target)
	})
}
