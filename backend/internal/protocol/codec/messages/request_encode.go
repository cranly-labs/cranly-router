package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// request_encode.go serializes normalized ir.Request values into deterministic
// Anthropic Messages JSON request bytes under target Dialect capabilities.
// Outgoing requests supply positive max_tokens defaults, raise max_tokens above
// enabled thinking.budget_tokens, resolve reasoning effort and budget controls
// against model capabilities, and map priority service tiers onto speed="fast".
//
// Target endpoints vary in hosted search support, tool-name length bounds,
// reasoning modes, and sampling restrictions, so request_encode.go lowers
// custom, client_search, and namespaced tools into Messages tool declarations,
// omits top-level thinking when an active non-passback tool loop lacks a
// leading assistant thinking block, and reports every dropped, lowered, or
// approximated field as an ir.Loss entry for Law Q4 verification.

const (
	// defaultMessagesMaxTokens and defaultThinkingHeadroomTokens supply the
	// fallback max_tokens cap and minimum output token headroom above enabled
	// thinking.budget_tokens on outgoing Anthropic Messages requests.
	defaultMessagesMaxTokens      int64 = 8192
	defaultThinkingHeadroomTokens int64 = 4096

	// emptyObjectSchemaJSON provides the default JSON Schema object emitted when
	// a cross-protocol function tool omits input_schema.
	emptyObjectSchemaJSON = `{"type":"object","properties":{}}`
)

// EncodeRequest serializes req into deterministic Anthropic Messages JSON bytes
// according to dialect and returns all incurred information-loss records. Nil
// requests, unsupported "ultra" reasoning efforts, or incompatible model
// thinking capability bounds return an error.
//
// Message legalization, tool lowering, tool_choice reconciliation, thinking
// capability resolution, and sampling or stop-sequence drops record original-path
// ir.Loss entries while leaving req unmutated for safe retry re-encoding.
func EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	if req == nil {
		return nil, nil, errors.New("encode messages request: nil request")
	}

	var losses []ir.Loss
	thinking, outEffort, err := resolveMessagesThinkingAndEffort(req.Reasoning, dialect)
	if err != nil {
		return nil, nil, err
	}

	maxTokens := defaultMessagesMaxTokens
	if req.MaxOutputTokens != nil && *req.MaxOutputTokens > 0 {
		maxTokens = *req.MaxOutputTokens
	}
	if thinking != nil && thinking.Type == "enabled" && thinking.BudgetTokens != nil {
		maxTokens = ensureMaxTokensAboveBudget(maxTokens, thinking.BudgetTokens, dialect.Reasoning)
	}

	maxNameBytes := defaultMessagesToolNameMaxBytes
	if dialect.ToolNameMaxBytes > 0 {
		maxNameBytes = dialect.ToolNameMaxBytes
	}

	sysBlocks, outMessages, err := legalizeRequestMessages(req, dialect, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}
	if outMessages == nil {
		outMessages = []wireOutMessage{}
	}

	// Official Anthropic Claude endpoints reject enabled/adaptive thinking when
	// an active tool-use loop's latest assistant turn lacks a leading thinking block.
	if thinking != nil && (thinking.Type == "enabled" || thinking.Type == "adaptive") &&
		dialect.ReasoningHistory != ir.HistoryToolTurns &&
		needsActiveToolLoopThinkingDowngrade(outMessages) {
		thinking = nil
	}

	tools, declaredNames, err := encodeMessagesTools(req.Tools, dialect, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}

	toolChoice, err := encodeMessagesToolChoice(
		req.ToolChoice,
		req.ParallelToolCalls,
		len(req.Tools) > 0,
		declaredNames,
		maxNameBytes,
		&losses,
	)
	if err != nil {
		return nil, nil, err
	}

	var outFormat *wireOutputFormat
	if req.Output != nil {
		outFormat = encodeMessagesOutputFormat(req.Output, &losses)
	}

	var outputConfig *wireOutputConfig
	if outEffort != "" || outFormat != nil {
		outputConfig = &wireOutputConfig{
			Effort: outEffort,
			Format: outFormat,
		}
	}

	var metadata *wireMetadata
	userID := req.MetadataUserID
	if userID == "" {
		userID = req.UserID
	}
	if userID != "" {
		metadata = &wireMetadata{UserID: userID}
	}

	var speed string
	switch strings.ToLower(strings.TrimSpace(req.ServiceTier)) {
	case "priority", "fast", "ultrafast":
		speed = "fast"
	}

	// Merge same-origin sub-object extensions back into thinking, output_config,
	// metadata, and tool_choice while removing their keys from topUnknown so
	// json:",embed" never duplicates modeled top-level request members.
	hadSameOriginExtra := req.Extra.Origin == protocol.ProtocolAnthropicMessages && !req.Extra.IsZero()
	topUnknown := restoreOrDropExtra(req.Extra, "", &losses)
	emittedSubExtra := mergeTopSubObjectExtras(
		topUnknown,
		thinking,
		&outputConfig,
		outFormat,
		&metadata,
		toolChoice,
		speed,
	)
	if hadSameOriginExtra && !emittedSubExtra && len(topUnknown) == 0 {
		losses = append(losses, ir.Loss{
			Kind: ir.LossExtraDropped,
			Path: "",
		})
	}

	out := wireOutRequest{
		Model:        req.Model,
		MaxTokens:    maxTokens,
		System:       sysBlocks,
		Messages:     outMessages,
		Tools:        tools,
		ToolChoice:   toolChoice,
		Thinking:     thinking,
		OutputConfig: outputConfig,
		Metadata:     metadata,
		Stream:       req.Stream,
		Speed:        speed,
		Unknown:      topUnknown,
	}

	encodeMessagesSamplingAndStop(req, dialect, &out, &losses)

	encoded, err := marshalDeterministic(out)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal messages request: %w", err)
	}
	return encoded, losses, nil
}

// EncodeRequest serializes req into deterministic Anthropic Messages JSON bytes
// under dialect by delegating to the package-level EncodeRequest function.
// Nil requests or unsupported reasoning capability settings return an error.
func (Codec) EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	return EncodeRequest(req, dialect)
}

// mergeTopSubObjectExtras restores unmodeled fields decoded from thinking,
// output_config, output_config.format, metadata, and tool_choice onto their
// outgoing wire structs and removes colliding keys from topUnknown. It reports
// whether at least one sub-object retained same-origin extra members.
func mergeTopSubObjectExtras(
	topUnknown map[string]jsontext.Value,
	thinking *wireThinking,
	outputConfig **wireOutputConfig,
	outFormat *wireOutputFormat,
	metadata **wireMetadata,
	toolChoice *wireToolChoice,
	speed string,
) bool {
	if len(topUnknown) == 0 {
		return false
	}
	emitted := false

	if _, hasThinking := topUnknown["thinking"]; hasThinking {
		thinkUnknown := extractSubObjectUnknown(topUnknown, "thinking", "type", "budget_tokens")
		delete(topUnknown, "thinking")
		if thinking != nil && len(thinkUnknown) > 0 {
			thinking.Unknown = thinkUnknown
			emitted = true
		}
	}

	if rawOC, hasOC := topUnknown["output_config"]; hasOC {
		if bytes.Equal(bytes.TrimSpace(rawOC), []byte("{}")) {
			if *outputConfig != nil {
				delete(topUnknown, "output_config")
			}
		} else {
			ocUnknown := extractSubObjectUnknown(topUnknown, "output_config", "effort")
			delete(topUnknown, "output_config")
			var fmtUnknown map[string]jsontext.Value
			if _, hasFmt := ocUnknown["format"]; hasFmt {
				fmtUnknown = extractSubObjectUnknown(ocUnknown, "format", "type", "schema")
				delete(ocUnknown, "format")
			}
			if outFormat != nil && len(fmtUnknown) > 0 {
				outFormat.Unknown = fmtUnknown
				emitted = true
			}
			if len(ocUnknown) > 0 {
				if *outputConfig == nil {
					*outputConfig = &wireOutputConfig{}
				}
				(*outputConfig).Unknown = ocUnknown
				emitted = true
			}
		}
	}

	if rawMeta, hasMeta := topUnknown["metadata"]; hasMeta {
		if bytes.Equal(bytes.TrimSpace(rawMeta), []byte("{}")) {
			if *metadata != nil {
				delete(topUnknown, "metadata")
			}
		} else {
			metaUnknown := extractSubObjectUnknown(topUnknown, "metadata", "user_id")
			delete(topUnknown, "metadata")
			if len(metaUnknown) > 0 {
				if *metadata == nil {
					*metadata = &wireMetadata{}
				}
				(*metadata).Unknown = metaUnknown
				emitted = true
			}
		}
	}

	if _, hasTC := topUnknown["tool_choice"]; hasTC {
		tcUnknown := extractSubObjectUnknown(topUnknown, "tool_choice", "type", "name", "disable_parallel_tool_use")
		delete(topUnknown, "tool_choice")
		if toolChoice != nil && len(tcUnknown) > 0 {
			toolChoice.Unknown = tcUnknown
			emitted = true
		}
	}

	if speed != "" {
		delete(topUnknown, "speed")
	}
	return emitted
}

// encodeMessagesSamplingAndStop applies Dialect sampling and stop-sequence
// filters onto out so unsupported parameters never reach Anthropic upstreams.
// Dropped temperature, top_p, top_k, seed, penalty, candidate_count, or stop
// fields append their corresponding ir.Loss entries onto losses.
func encodeMessagesSamplingAndStop(req *ir.Request, dialect ir.Dialect, out *wireOutRequest, losses *[]ir.Loss) {
	if req.Sampling.Temperature != nil {
		if dialect.DropTemperature {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossSamplingDropped,
				Path:   "sampling.temperature",
				Detail: "temperature",
			})
		} else {
			out.Temperature = req.Sampling.Temperature
		}
	}
	if req.Sampling.TopP != nil {
		if dialect.DropTopP {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossSamplingDropped,
				Path:   "sampling.top_p",
				Detail: "top_p",
			})
		} else {
			out.TopP = req.Sampling.TopP
		}
	}
	if req.Sampling.TopK != nil {
		if dialect.DropTopK {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossSamplingDropped,
				Path:   "sampling.top_k",
				Detail: "top_k",
			})
		} else {
			out.TopK = req.Sampling.TopK
		}
	}
	if req.Sampling.Seed != nil {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossSamplingDropped,
			Path:   "sampling.seed",
			Detail: "seed",
		})
	}
	if req.Sampling.FrequencyPenalty != nil {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossSamplingDropped,
			Path:   "sampling.frequency_penalty",
			Detail: "frequency_penalty",
		})
	}
	if req.Sampling.PresencePenalty != nil {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossSamplingDropped,
			Path:   "sampling.presence_penalty",
			Detail: "presence_penalty",
		})
	}
	if req.Sampling.CandidateCount != nil {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossCandidateCountIgnored,
			Path: "sampling.candidate_count",
		})
	}

	if len(req.Stop) > 0 {
		if dialect.OmitStopSequences {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossStopDropped,
				Path: "stop",
			})
		} else {
			out.StopSequences = slices.Clone(req.Stop)
		}
	}
}

// resolveMessagesThinkingAndEffort resolves rc against dialect.Reasoning into an
// optional Messages thinking object and an optional output_config.effort string.
// Nil configs return (nil, "", nil), and StripEffort clears incoming effort
// labels before validation.
//
// Explicit "ultra" efforts always return an error, whereas "max" survives when
// ControlsKnown is false. Disabled modes and effort="none" resolve to
// thinking.type="disabled" when toggling is permitted. Budget-capable or
// unknown-control models emit bounded thinking.budget_tokens on enabled or
// effort-lowered requests, while effort-only adaptive models emit
// thinking.type="adaptive" alongside output_config.effort without inflating max_tokens.
func resolveMessagesThinkingAndEffort(rc *ir.ReasoningConfig, dialect ir.Dialect) (*wireThinking, string, error) {
	if rc == nil {
		return nil, "", nil
	}
	caps := dialect.Reasoning

	rawEffort := strings.TrimSpace(rc.Effort)
	if dialect.StripEffort {
		rawEffort = ""
	}
	if strings.EqualFold(rawEffort, "ultra") {
		return nil, "", fmt.Errorf("reasoning effort %q is not supported", rawEffort)
	}

	effort := ""
	if rawEffort != "" {
		if norm := protocol.NormalizeReasoningEffort(rawEffort); norm != "" {
			effort = norm
		} else {
			return nil, "", fmt.Errorf("unsupported reasoning effort %q", rawEffort)
		}
	}

	disabled := rc.Mode == "disabled" || (rc.Mode == "" && effort == "none") || (rc.BudgetTokens != nil && *rc.BudgetTokens == 0)
	if disabled {
		outEffort, err := resolveMessagesOutputEffort(effort, caps)
		if err != nil {
			return nil, "", err
		}
		if caps.Supported != nil && !*caps.Supported {
			return nil, outEffort, nil
		}
		if caps.ControlsKnown && !caps.Toggle {
			if rc.Mode == "disabled" || (rc.BudgetTokens != nil && *rc.BudgetTokens == 0) {
				return nil, "", errors.New("selected model does not support disabling thinking")
			}
			// OpenAI effort="none" omits the thinking object on non-toggle models so
			// cross-protocol requests succeed without sending an unsupported disable toggle.
			return nil, "", nil
		}
		return &wireThinking{Type: "disabled"}, outEffort, nil
	}

	hasExplicitThinking := rc.Mode == "enabled" || rc.Mode == "adaptive" || rc.BudgetTokens != nil
	if !hasExplicitThinking && effort == "" {
		return nil, "", nil
	}
	if caps.Supported != nil && !*caps.Supported {
		return nil, "", errors.New("selected model does not support thinking")
	}
	if caps.ControlsKnown && !caps.ForceBudget && !caps.Budget && len(caps.Efforts) == 0 && !caps.Toggle {
		return nil, "", errors.New("selected model does not expose adjustable thinking")
	}

	var outThinking *wireThinking
	useBudget := caps.ForceBudget ||
		(caps.Budget && (rc.Mode == "enabled" || rc.BudgetTokens != nil || len(caps.Efforts) == 0)) ||
		(!caps.ControlsKnown && (rc.Mode == "enabled" || rc.BudgetTokens != nil))

	switch {
	case rc.Mode == "adaptive" && !caps.ForceBudget:
		outThinking = &wireThinking{Type: "adaptive"}

	case useBudget:
		budget := selectThinkingBudgetTokens(rc, effort, caps)
		bounded, err := clampThinkingBudget(budget, caps)
		if err != nil {
			return nil, "", err
		}
		outThinking = &wireThinking{
			Type:         "enabled",
			BudgetTokens: &bounded,
		}

	case rc.Mode == "enabled":
		// Effort-only adaptive models omit budget_tokens support in catalog controls,
		// so activate extended thinking via type="adaptive" without inflating max_tokens.
		outThinking = &wireThinking{Type: "adaptive"}
	}

	outEffort, err := resolveMessagesOutputEffort(effort, caps)
	if err != nil {
		return nil, "", err
	}
	if outEffort == "" && (outThinking == nil || outThinking.BudgetTokens == nil) && rc.BudgetTokens != nil && *rc.BudgetTokens > 0 && caps.ControlsKnown && len(caps.Efforts) > 0 {
		// Effort-only models omit budget_tokens in catalog controls, so map positive
		// client token budgets onto the closest supported effort rank.
		mapped := budgetToEffortTier(*rc.BudgetTokens)
		selected, err := selectSupportedClaudeEffort(mapped, caps.Efforts)
		if err != nil {
			return nil, "", err
		}
		outEffort = selected
	}

	return outThinking, outEffort, nil
}

// resolveMessagesOutputEffort resolves a normalized non-"none" effort string
// against caps into an outgoing output_config.effort label. Unknown capability
// sets pass normalized Claude effort labels through unchanged, whereas budget-forced
// or effort-less known models return "" without error.
func resolveMessagesOutputEffort(effort string, caps ir.ReasoningCapabilities) (string, error) {
	if effort == "" || effort == "none" {
		return "", nil
	}
	if !caps.ControlsKnown {
		return normalizeMessagesEffortLabel(effort), nil
	}
	if len(caps.Efforts) > 0 && !caps.ForceBudget {
		return selectSupportedClaudeEffort(effort, caps.Efforts)
	}
	return "", nil
}

// selectThinkingBudgetTokens chooses the positive initial token budget before
// capability clamping for enabled, forced-budget, or effort-lowered requests.
// When neither explicit budget_tokens nor effort is set, it defaults to high effort.
func selectThinkingBudgetTokens(rc *ir.ReasoningConfig, effort string, caps ir.ReasoningCapabilities) int64 {
	if rc.Mode == "adaptive" && caps.AdaptiveBudget > 0 && effort == "" {
		return int64(caps.AdaptiveBudget)
	}
	if rc.BudgetTokens != nil && *rc.BudgetTokens > 0 {
		return *rc.BudgetTokens
	}
	if effort != "" {
		return defaultEffortThinkingBudget(effort)
	}
	if rc.Mode == "adaptive" && caps.AdaptiveBudget > 0 {
		return int64(caps.AdaptiveBudget)
	}
	return defaultEffortThinkingBudget("high")
}

// defaultEffortThinkingBudget maps canonical reasoning effort labels onto
// concrete Anthropic thinking.budget_tokens counts. Effort "minimal" folds into
// "low" at 1024 tokens, while unrecognized labels fall back to 10240 tokens.
func defaultEffortThinkingBudget(effort string) int64 {
	switch effort {
	case "minimal", "low":
		return 1024
	case "medium":
		return 4096
	case "high":
		return 10240
	case "xhigh", "max":
		return 32768
	default:
		return 10240
	}
}

// budgetToEffortTier maps a positive thinking token budget onto "low", "medium",
// or "high" for level-only Claude models. Budgets up to 1024 select "low",
// budgets up to 4096 select "medium", and larger budgets select "high".
func budgetToEffortTier(budget int64) string {
	switch {
	case budget <= 1024:
		return "low"
	case budget <= 4096:
		return "medium"
	default:
		return "high"
	}
}

// clampThinkingBudget enforces BudgetMin, BudgetMax, and OutputLimit headroom
// bounds on an initial thinking budget. Non-positive inputs default to 1024,
// and conflicting capability bounds that leave no valid budget return an error.
func clampThinkingBudget(budget int64, caps ir.ReasoningCapabilities) (int64, error) {
	if budget <= 0 {
		budget = 1024
	}
	if caps.BudgetMin > 0 {
		budget = max(budget, int64(caps.BudgetMin))
	}
	hasMax := caps.BudgetMax > 0
	maxBound := int64(caps.BudgetMax)
	if caps.BudgetPadding > 0 && caps.OutputLimit != nil {
		limit := int64(*caps.OutputLimit - caps.BudgetPadding)
		if !hasMax || limit < maxBound {
			hasMax = true
			maxBound = limit
		}
	}
	if hasMax {
		if maxBound <= 0 || (caps.BudgetMin > 0 && maxBound < int64(caps.BudgetMin)) {
			return 0, errors.New("output limit leaves no valid thinking budget")
		}
		budget = min(budget, maxBound)
	}
	return budget, nil
}

// ensureMaxTokensAboveBudget raises maxTokens above *budgetPtr so Anthropic's
// strict budget_tokens < max_tokens invariant always holds. When OutputLimit
// caps maxTokens at or below *budgetPtr, *budgetPtr is clamped below OutputLimit.
func ensureMaxTokensAboveBudget(maxTokens int64, budgetPtr *int64, caps ir.ReasoningCapabilities) int64 {
	if budgetPtr == nil || *budgetPtr < maxTokens {
		return maxTokens
	}
	headroom := defaultThinkingHeadroomTokens
	if caps.BudgetPadding > 0 {
		headroom = int64(caps.BudgetPadding)
	}
	desired := *budgetPtr + headroom
	if caps.OutputLimit != nil && *caps.OutputLimit > 0 {
		limit := int64(*caps.OutputLimit)
		if desired > limit {
			desired = limit
		}
		if *budgetPtr >= desired && desired > 1 {
			*budgetPtr = desired - 1
		}
	}
	return desired
}

// normalizeMessagesEffortLabel maps OpenAI's "minimal" effort tier onto
// Anthropic's lowest supported effort "low". Supported Claude labels ("low",
// "medium", "high", "xhigh", and "max") are returned unchanged.
func normalizeMessagesEffortLabel(effort string) string {
	if effort == "minimal" {
		return "low"
	}
	return effort
}

// selectSupportedClaudeEffort chooses the highest allowed Claude effort rank not
// exceeding effort. When every allowed rank exceeds effort, the lowest allowed
// rank is selected, and empty allowed sets return an error.
func selectSupportedClaudeEffort(effort string, allowed []string) (string, error) {
	order := []string{"minimal", "low", "medium", "high", "xhigh", "max"}
	rank := slices.Index(order, effort)
	if rank < 0 {
		return "", fmt.Errorf("unsupported reasoning effort %q", effort)
	}
	selected := ""
	for idx, candidate := range order {
		if !slices.Contains(allowed, candidate) {
			continue
		}
		if selected == "" || idx <= rank {
			selected = candidate
		}
	}
	if selected == "" {
		return "", errors.New("selected model has no compatible reasoning effort")
	}
	return normalizeMessagesEffortLabel(selected), nil
}

// encodeMessagesOutputFormat maps out onto Anthropic's output_config.format
// object when Kind is "json_schema" with a valid object schema. Unsupported
// formats or dropped name, description, strict, and verbosity subfields record LossOutputFormatApproximated.
func encodeMessagesOutputFormat(out *ir.OutputFormat, losses *[]ir.Loss) *wireOutputFormat {
	if out.Verbosity != "" {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output.verbosity",
			Detail: "verbosity",
		})
	}
	switch out.Kind {
	case "":
		return nil
	case "json_schema":
		if !isValidSingleJSONObject(out.Schema) {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossOutputFormatApproximated,
				Path:   "output",
				Detail: "json_schema",
			})
			return nil
		}
		if out.Description != "" {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossOutputFormatApproximated,
				Path:   "output.description",
				Detail: "description",
			})
		}
		if out.Strict != nil {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossOutputFormatApproximated,
				Path:   "output.strict",
				Detail: "strict",
			})
		}
		if out.Name != "" {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossOutputFormatApproximated,
				Path:   "output.name",
				Detail: "name",
			})
		}
		return &wireOutputFormat{
			Type:   "json_schema",
			Schema: jsontext.Value(bytes.Clone(out.Schema)),
		}
	default:
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output",
			Detail: out.Kind,
		})
		return nil
	}
}

// encodeMessagesTools serializes req.Tools into Messages tools[] JSON objects
// and returns the set of declared wire tool names alongside lowering or drop losses.
//
// Same-origin hosted server tools pass through when permitted by dialect.Tools,
// cross-protocol hosted web search maps onto web_search_20250305 when enabled,
// and unsupported hosted tools emit LossHostedToolDropped. Function tools
// missing an object input_schema receive emptyObjectSchemaJSON with LossSchemaApproximated.
func encodeMessagesTools(
	tools []ir.Tool,
	dialect ir.Dialect,
	maxNameBytes int,
	losses *[]ir.Loss,
) ([]jsontext.Value, map[string]bool, error) {
	if len(tools) == 0 {
		return nil, nil, nil
	}
	var out []jsontext.Value
	declared := make(map[string]bool, len(tools))
	searchEmitted := false

	for i, t := range tools {
		tPath := fmt.Sprintf("tools[%d]", i)
		switch t.Kind {
		case ir.ToolHosted:
			if t.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tPath})
			}
			_ = restoreOrDropExtra(t.Extra, tPath, losses)
			allowWebSearch := dialect.Tools == 0 || dialect.Tools.Has(ir.ToolSupportHostedWebSearch)
			allowSameOrigin := allowWebSearch || (t.Hosted != nil && !isMessagesWebSearchType(t.Hosted.Type))
			switch {
			case t.Hosted != nil && t.Hosted.Origin == protocol.ProtocolAnthropicMessages && allowSameOrigin:
				out = append(out, jsontext.Value(bytes.Clone(t.Hosted.Raw)))
				if t.Name != "" {
					declared[t.Name] = true
				}
				if t.Hosted.Type != "" {
					declared[t.Hosted.Type] = true
				}
			case t.Hosted != nil && allowWebSearch && isMessagesWebSearchType(t.Hosted.Type):
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossToolLowered,
					Path:   tPath,
					Detail: "hosted_web_search",
				})
				out = append(out, jsontext.Value(`{"name":"web_search","type":"web_search_20250305"}`))
				declared["web_search"] = true
				declared["web_search_20250305"] = true
			default:
				detail := t.Name
				if t.Hosted != nil && t.Hosted.Type != "" {
					detail = t.Hosted.Type
				}
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossHostedToolDropped,
					Path:   tPath,
					Detail: detail,
				})
			}

		case ir.ToolFunction:
			cache := encodeCacheControl(t.Cache, tPath+".cache", losses)
			unknown := restoreOrDropExtra(t.Extra, tPath, losses)
			name := t.Name
			lowered := false
			if t.Namespace != "" {
				name = flattenMessagesToolName(t.Namespace, name, maxNameBytes)
				if t.Namespace != "functions" {
					lowered = true
				}
			}
			if lowered {
				*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			}
			var schema jsontext.Value
			if isValidSingleJSONObject(t.InputSchema) {
				schema = jsontext.Value(bytes.Clone(t.InputSchema))
			} else {
				schema = jsontext.Value(emptyObjectSchemaJSON)
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossSchemaApproximated,
					Path: tPath + ".input_schema",
				})
			}
			rawTool, err := marshalDeterministic(wireToolWrite{
				Name:         name,
				Description:  t.Description,
				InputSchema:  schema,
				Strict:       t.Strict,
				DeferLoading: t.DeferLoading,
				CacheControl: cache,
				Unknown:      unknown,
			})
			if err != nil {
				return nil, nil, err
			}
			declared[name] = true
			out = append(out, jsontext.Value(rawTool))

		case ir.ToolCustom:
			cache := encodeCacheControl(t.Cache, tPath+".cache", losses)
			unknown := restoreOrDropExtra(t.Extra, tPath, losses)
			name := t.Name
			if t.Namespace != "" {
				name = flattenMessagesToolName(t.Namespace, name, maxNameBytes)
			}
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			rawTool, err := marshalDeterministic(wireToolWrite{
				Name:         name,
				Description:  t.Description,
				InputSchema:  jsontext.Value(customToolInputSchemaJSON),
				DeferLoading: t.DeferLoading,
				CacheControl: cache,
				Unknown:      unknown,
			})
			if err != nil {
				return nil, nil, err
			}
			declared[name] = true
			out = append(out, jsontext.Value(rawTool))

		case ir.ToolClientSearch:
			if t.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tPath})
			}
			_ = restoreOrDropExtra(t.Extra, tPath, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			if searchEmitted {
				continue
			}
			searchEmitted = true
			rawTool, err := marshalDeterministic(wireToolWrite{
				Name:        toolSearchProxyName,
				Description: "Search and load tools, plugins, connectors, and MCP namespaces for the current task.",
				InputSchema: jsontext.Value(toolSearchProxySchemaJSON),
			})
			if err != nil {
				return nil, nil, err
			}
			declared[toolSearchProxyName] = true
			out = append(out, jsontext.Value(rawTool))
		}
	}

	if len(out) == 0 {
		return nil, declared, nil
	}
	return out, declared, nil
}

// encodeMessagesToolChoice maps tc and parallelCalls onto an Anthropic
// wireToolChoice object while reconciling named selections against surviving tools.
// Hosted selections, AllowedTools subsets, or pruned targets record LossToolChoiceApproximated.
func encodeMessagesToolChoice(
	tc *ir.ToolChoice,
	parallelCalls *bool,
	hadSourceTools bool,
	declared map[string]bool,
	maxNameBytes int,
	losses *[]ir.Loss,
) (*wireToolChoice, error) {
	if tc == nil {
		return nil, nil
	}
	if hadSourceTools && len(declared) == 0 && tc.Mode != ir.ToolChoiceNone {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolChoiceApproximated,
			Path: "tool_choice",
		})
		return nil, nil
	}

	var disableParallel *bool
	if parallelCalls != nil && tc.Mode != ir.ToolChoiceNone {
		d := !*parallelCalls
		disableParallel = &d
	}

	if len(tc.AllowedTools) > 0 {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolChoiceApproximated,
			Path: "tool_choice",
		})
	}

	switch tc.Mode {
	case ir.ToolChoiceAuto:
		return &wireToolChoice{
			Type:                   "auto",
			DisableParallelToolUse: disableParallel,
		}, nil

	case ir.ToolChoiceRequired:
		return &wireToolChoice{
			Type:                   "any",
			DisableParallelToolUse: disableParallel,
		}, nil

	case ir.ToolChoiceNone:
		return &wireToolChoice{
			Type: "none",
		}, nil

	case ir.ToolChoiceTool:
		if tc.Tool.Kind == ir.ToolHosted {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossToolChoiceApproximated,
				Path: "tool_choice",
			})
			return nil, nil
		}
		name := resolveMessagesChoiceName(tc.Tool, maxNameBytes)
		if isLoweredToolRef(tc.Tool) {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossToolChoiceApproximated,
				Path: "tool_choice",
			})
		}
		if len(declared) > 0 && !declared[name] {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossToolChoiceApproximated,
				Path: "tool_choice",
			})
			return nil, nil
		}
		return &wireToolChoice{
			Type:                   "tool",
			Name:                   name,
			DisableParallelToolUse: disableParallel,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported tool_choice mode %q", tc.Mode)
	}
}

// resolveMessagesChoiceName maps a ToolChoice ToolRef onto its outgoing
// Anthropic Messages tool identifier bounded by maxNameBytes. Client search
// references resolve to toolSearchProxyName and namespaced tools flatten with "__".
func resolveMessagesChoiceName(ref ir.ToolRef, maxNameBytes int) string {
	if ref.Kind == ir.ToolClientSearch {
		return toolSearchProxyName
	}
	if ref.Namespace != "" {
		return flattenMessagesToolName(ref.Namespace, ref.Name, maxNameBytes)
	}
	return ref.Name
}

// isMessagesWebSearchType reports whether typ names a hosted web search tool
// across Anthropic, OpenAI Responses, or Gemini declarations.
func isMessagesWebSearchType(typ string) bool {
	trimmed := strings.TrimSpace(typ)
	return strings.HasPrefix(trimmed, "web_search") || trimmed == "google_search"
}
