package generatecontent

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

// request_encode.go serializes validated IR Requests into deterministic Google
// Gemini GenerateContent JSON payloads under a target ir.Dialect. It coordinates
// conversation legalization, tool declaration and schema cleaning, mixed
// client/server tool invocation flags, and thinkingConfig budget or level
// resolution without mutating caller-owned IR structures.
//
// Schema cleaning selects CleanGeminiSchema (parameters with uppercase types) or
// CleanAntigravitySchema (seven-key whitelist with constraint hints and empty-object
// reason injection) from Dialect.SchemaDialect while keeping same-protocol
// JSON Schema round trips lossless under parametersJsonSchema and responseJsonSchema.
// Generation settings resolve thinking budgets or levels from Dialect.Reasoning,
// pad maxOutputTokens above positive thinking budgets, and apply per-parameter
// sampling or stop-sequence omission rules.

const (
	// defaultBudgetHeadroomTokens provides the default output token margin added
	// above a positive thinkingBudget when maxOutputTokens <= thinkingBudget and
	// Dialect.Reasoning.BudgetPadding is unset.
	defaultBudgetHeadroomTokens int64 = 4096
)

// EncodeRequest serializes req into deterministic Gemini GenerateContent JSON
// bytes under dialect and returns all incurred information-loss records. Nil
// requests, unsupported thinking controls, or unencodable schemas return an error.
func EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	if req == nil {
		return nil, nil, errors.New("encode generatecontent request: nil request")
	}

	var losses []ir.Loss
	maxNameBytes := defaultGeminiToolNameMaxBytes
	if dialect.ToolNameMaxBytes > 0 && dialect.ToolNameMaxBytes < maxNameBytes {
		maxNameBytes = dialect.ToolNameMaxBytes
	}

	if len(req.Namespaces) > 0 {
		for nsIdx, ns := range req.Namespaces {
			nsPath := fmt.Sprintf("namespaces[%d]", nsIdx)
			if ns.Cache != nil {
				losses = append(losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: nsPath})
			}
			_ = restoreOrDropExtra(ns.Extra, nsPath, &losses)
		}
	}
	if req.ServiceTier != "" {
		losses = append(losses, ir.Loss{
			Kind:   ir.LossExtraDropped,
			Path:   "service_tier",
			Detail: req.ServiceTier,
		})
	}

	topUnknown := restoreOrDropExtra(req.Extra, "", &losses)
	sysUnknown, _ := extractSubObjectUnknown(topUnknown, "systemInstruction", "parts")
	tcUnknown, tcWasPresent := extractSubObjectUnknown(topUnknown, "toolConfig", "functionCallingConfig", "includeServerSideToolInvocations")
	fccUnknown, fccWasPresent := extractNestedSubObjectUnknown(topUnknown, "toolConfig", "functionCallingConfig", "mode", "allowedFunctionNames")
	explicitIncludeServerSide := extractNestedBoolMember(topUnknown, "toolConfig", "includeServerSideToolInvocations")
	gcUnknown, gcWasPresent := extractSubObjectUnknown(
		topUnknown,
		"generationConfig",
		"temperature",
		"topP",
		"topK",
		"candidateCount",
		"maxOutputTokens",
		"stopSequences",
		"presencePenalty",
		"frequencyPenalty",
		"seed",
		"responseMimeType",
		"responseSchema",
		"responseJsonSchema",
		"thinkingConfig",
	)
	thUnknown, thWasPresent := extractNestedSubObjectUnknown(
		topUnknown,
		"generationConfig",
		"thinkingConfig",
		"thinkingBudget",
		"thinkingLevel",
	)
	explicitCustomMime := extractNestedStringMember(topUnknown, "generationConfig", "responseMimeType")
	validatedModeOverride := extractNestedStringMember(topUnknown, "toolConfig", "functionCallingConfig", "mode")
	delete(fccUnknown, "mode")

	for _, modeledTop := range []string{
		"systemInstruction",
		"contents",
		"tools",
		"toolConfig",
		"generationConfig",
	} {
		delete(topUnknown, modeledTop)
	}

	legalized, err := legalizeRequestConversation(req, dialect, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}

	var sysInstruction *wireOutSystemInstruction
	if len(legalized.systemParts) > 0 {
		sysInstruction = &wireOutSystemInstruction{
			Parts:   legalized.systemParts,
			Unknown: sysUnknown,
		}
	}

	toolsOut, hasFunctions, hasBuiltinTools, err := encodeRequestTools(req.Tools, dialect, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}

	toolCfg, err := encodeToolConfig(
		req.ToolChoice,
		dialect,
		maxNameBytes,
		hasFunctions && hasBuiltinTools,
		explicitIncludeServerSide,
		validatedModeOverride,
		tcUnknown,
		tcWasPresent,
		fccUnknown,
		fccWasPresent,
		&losses,
	)
	if err != nil {
		return nil, nil, err
	}

	genCfg, err := encodeGenerationConfig(
		req,
		dialect,
		legalized.strippedThinking,
		explicitCustomMime,
		gcUnknown,
		gcWasPresent,
		thUnknown,
		thWasPresent,
		&losses,
	)
	if err != nil {
		return nil, nil, err
	}

	out := wireOutRequest{
		SystemInstruction: sysInstruction,
		Contents:          legalized.contents,
		Tools:             toolsOut,
		ToolConfig:        toolCfg,
		GenerationConfig:  genCfg,
		Unknown:           topUnknown,
	}
	encoded, err := marshalDeterministic(out)
	if err != nil {
		return nil, nil, err
	}
	return encoded, losses, nil
}

// encodeRequestTools serializes req.Tools into Gemini tools entries and reports
// whether client function declarations and built-in hosted tools coexist.
// Contiguous function declarations merge into one functionDeclarations entry,
// whereas empty tool slices return (nil, false, false, nil) without allocating.
func encodeRequestTools(
	tools []ir.Tool,
	dialect ir.Dialect,
	maxNameBytes int,
	losses *[]ir.Loss,
) ([]jsontext.Value, bool, bool, error) {
	if len(tools) == 0 {
		return nil, false, false, nil
	}

	var entries []jsontext.Value
	var pendingFuncs []jsontext.Value
	hasFunctions := false
	hasBuiltin := false
	emittedHostedSearch := false
	emittedCodeExec := false

	flushPendingFuncs := func() error {
		if len(pendingFuncs) == 0 {
			return nil
		}
		decls := slices.Clone(pendingFuncs)
		rawEntry, err := marshalDeterministic(wireToolEntry{
			FunctionDeclarations: &decls,
		})
		if err != nil {
			return err
		}
		entries = append(entries, jsontext.Value(rawEntry))
		pendingFuncs = nil
		return nil
	}

	for idx, t := range tools {
		tPath := fmt.Sprintf("tools[%d]", idx)
		if t.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tPath})
		}

		switch t.Kind {
		case ir.ToolFunction, ir.ToolCustom, ir.ToolClientSearch:
			// Antigravity internal Gemini promotes legacy client functions named
			// web_search/google_search/web_search_20250305 into googleSearch when
			// Dialect advertises ToolSupportHostedWebSearch.
			if dialect.SchemaDialect == ir.SchemaAntigravity &&
				dialect.Tools.Has(ir.ToolSupportHostedWebSearch) &&
				isLegacyPromotedWebSearchName(t.Name) {
				_ = restoreOrDropExtra(t.Extra, tPath, losses)
				if emittedHostedSearch {
					*losses = append(*losses, ir.Loss{
						Kind:   ir.LossToolLowered,
						Path:   tPath,
						Detail: "duplicate_web_search",
					})
					continue
				}
				if err := flushPendingFuncs(); err != nil {
					return nil, false, false, err
				}
				emittedHostedSearch = true
				hasBuiltin = true
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossToolLowered,
					Path:   tPath,
					Detail: "promoted_google_search",
				})
				entries = append(entries, jsontext.Value(`{"googleSearch":{}}`))
				continue
			}

			declRaw, err := encodeSingleFunctionTool(t, tPath, dialect, maxNameBytes, losses)
			if err != nil {
				return nil, false, false, err
			}
			hasFunctions = true
			pendingFuncs = append(pendingFuncs, declRaw)

		case ir.ToolHosted:
			_ = restoreOrDropExtra(t.Extra, tPath, losses)
			if t.Hosted == nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossHostedToolDropped, Path: tPath})
				continue
			}
			if t.Hosted.Origin == protocol.ProtocolGeminiGenerateContent {
				if err := flushPendingFuncs(); err != nil {
					return nil, false, false, err
				}
				switch t.Hosted.Type {
				case "googleSearch", "google_search":
					emittedHostedSearch = true
					hasBuiltin = true
				case "codeExecution":
					emittedCodeExec = true
					hasBuiltin = true
				}
				entries = append(entries, compactRawJSON(t.Hosted.Raw))
				continue
			}

			// Foreign hosted web_search* and code_execution tools promote to Gemini
			// built-in declarations when Dialect.ToolSupport enables hosted search;
			// otherwise they drop with LossHostedToolDropped.
			if (strings.HasPrefix(t.Hosted.Type, "web_search") || t.Hosted.Type == "google_search") &&
				dialect.Tools.Has(ir.ToolSupportHostedWebSearch) {
				if emittedHostedSearch {
					*losses = append(*losses, ir.Loss{Kind: ir.LossHostedToolDropped, Path: tPath})
					continue
				}
				if err := flushPendingFuncs(); err != nil {
					return nil, false, false, err
				}
				emittedHostedSearch = true
				hasBuiltin = true
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossToolLowered,
					Path:   tPath,
					Detail: "google_search",
				})
				entries = append(entries, jsontext.Value(`{"googleSearch":{}}`))
				continue
			}
			if t.Hosted.Type == "code_execution" && dialect.SchemaDialect == ir.SchemaAntigravity {
				if emittedCodeExec {
					*losses = append(*losses, ir.Loss{Kind: ir.LossHostedToolDropped, Path: tPath})
					continue
				}
				if err := flushPendingFuncs(); err != nil {
					return nil, false, false, err
				}
				emittedCodeExec = true
				hasBuiltin = true
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossToolLowered,
					Path:   tPath,
					Detail: "code_execution",
				})
				entries = append(entries, jsontext.Value(`{"codeExecution":{}}`))
				continue
			}

			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossHostedToolDropped,
				Path:   tPath,
				Detail: t.Hosted.Type,
			})
		}
	}

	if err := flushPendingFuncs(); err != nil {
		return nil, false, false, err
	}
	return entries, hasFunctions, hasBuiltin, nil
}

// isLegacyPromotedWebSearchName reports whether name matches one of the three
// Claude search function names that encodeRequestTools promotes into googleSearch.
func isLegacyPromotedWebSearchName(name string) bool {
	switch strings.TrimSpace(name) {
	case "web_search", "google_search", "web_search_20250305":
		return true
	default:
		return false
	}
}

// encodeSingleFunctionTool serializes one ToolFunction, ToolCustom, or
// ToolClientSearch declaration into a Gemini FunctionDeclaration JSON object.
// Custom and client_search tools lower into wrapper schemas with LossToolLowered,
// and schema cleaning follows dialect.SchemaDialect while reporting approximations.
func encodeSingleFunctionTool(
	t ir.Tool,
	tPath string,
	dialect ir.Dialect,
	maxNameBytes int,
	losses *[]ir.Loss,
) (jsontext.Value, error) {
	rawName := t.Name
	desc := t.Description
	schema := t.InputSchema
	lowered := false

	switch t.Kind {
	case ir.ToolCustom:
		lowered = true
		schema = jsontext.Value(customToolWrapperSchemaJSON)
	case ir.ToolClientSearch:
		lowered = true
		if strings.TrimSpace(rawName) == "" {
			rawName = clientSearchProxyName
		}
		if desc == "" {
			desc = "Search deferred tools and namespaces."
		}
		schema = jsontext.Value(clientSearchProxySchemaJSON)
	}

	if t.Namespace != "" {
		lowered = true
		rawName = t.Namespace + "__" + rawName
	}
	fnName := sanitizeGeminiToolName(rawName, maxNameBytes)
	if fnName != rawName {
		lowered = true
	}

	if lowered {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolLowered,
			Path:   tPath,
			Detail: string(t.Kind),
		})
	} else if t.Strict != nil {
		// Gemini FunctionDeclaration omits OpenAI/Anthropic strict flags, so record
		// strict_dropped when a non-lowered function declaration carried Strict.
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossSchemaApproximated,
			Path:   tPath,
			Detail: "strict_dropped",
		})
	}

	declUnknown := restoreOrDropExtra(t.Extra, tPath, losses)
	for _, k := range []string{"name", "description", "parameters", "parametersJsonSchema"} {
		delete(declUnknown, k)
	}

	decl := wireFunctionDeclaration{
		Name:        fnName,
		Description: desc,
		Unknown:     declUnknown,
	}

	switch dialect.SchemaDialect {
	case ir.SchemaGemini:
		cleaned, changed, err := CleanGeminiSchema(schema)
		if err != nil {
			return nil, fmt.Errorf("%s: clean Gemini schema: %w", tPath, err)
		}
		decl.Parameters = cleaned
		if changed && !lowered {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossSchemaApproximated,
				Path:   tPath,
				Detail: "gemini",
			})
		}

	case ir.SchemaAntigravity:
		cleaned, changed, err := CleanAntigravitySchema(schema)
		if err != nil {
			return nil, fmt.Errorf("%s: clean Antigravity schema: %w", tPath, err)
		}
		decl.Parameters = cleaned
		if changed && !lowered {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossSchemaApproximated,
				Path:   tPath,
				Detail: "antigravity",
			})
		}

	default:
		// Same-protocol round trips (SchemaDialect == "" or SchemaJSONSchema) keep
		// Gemini-subset schemas in parameters and full JSON Schemas in
		// parametersJsonSchema so ValidateGeminiRequest and Law Q2 both hold.
		if len(bytes.TrimSpace(schema)) > 0 && !bytes.Equal(bytes.TrimSpace(schema), []byte("null")) {
			compacted := compactRawJSON(schema)
			if isStrictGeminiParametersSchema(compacted) {
				decl.Parameters = compacted
			} else {
				decl.ParametersJSONSchema = compacted
			}
		}
	}

	raw, err := marshalDeterministic(decl)
	if err != nil {
		return nil, err
	}
	return jsontext.Value(raw), nil
}

// isStrictGeminiParametersSchema reports whether rawSchema already satisfies
// Gemini FunctionDeclaration.parameters syntax with uppercase type names and no
// forbidden JSON Schema keywords. Malformed or non-conforming schemas return
// false so callers emit parametersJsonSchema instead.
func isStrictGeminiParametersSchema(rawSchema jsontext.Value) bool {
	parsed, err := decodeSchemaMap(bytes.TrimSpace(rawSchema))
	if err != nil {
		return false
	}
	return checkStrictGeminiSchemaMap(parsed)
}

// checkStrictGeminiSchemaMap recursively checks that m contains no forbidden
// Gemini parameters keywords, no type arrays, and uppercase type strings when
// present. Non-conforming subtrees return false so callers fall back to
// parametersJsonSchema or responseJsonSchema.
func checkStrictGeminiSchemaMap(m map[string]any) bool {
	if typeVal, hasType := m["type"]; hasType {
		ts, ok := typeVal.(string)
		if !ok || ts != strings.ToUpper(ts) {
			return false
		}
	}
	for k, v := range m {
		if geminiUnsupportedSchemaKeys[k] {
			return false
		}
		if k == "properties" {
			if props, ok := v.(map[string]any); ok {
				for _, pv := range props {
					if pm, isMap := pv.(map[string]any); isMap && !checkStrictGeminiSchemaMap(pm) {
						return false
					}
				}
				continue
			}
		}
		if subMap, ok := v.(map[string]any); ok {
			if !checkStrictGeminiSchemaMap(subMap) {
				return false
			}
		} else if subArr, ok := v.([]any); ok {
			for _, elem := range subArr {
				if em, isMap := elem.(map[string]any); isMap && !checkStrictGeminiSchemaMap(em) {
					return false
				}
			}
		}
	}
	return true
}

// encodeToolConfig builds the top-level Gemini toolConfig object from tc, mixed
// client/server tool presence, and preserved same-origin Extra fields. When no
// mode, server-side flag, or Extra member survives, it returns (nil, nil) so
// outgoing requests omit empty toolConfig wrappers.
func encodeToolConfig(
	tc *ir.ToolChoice,
	dialect ir.Dialect,
	maxNameBytes int,
	hasMixedTools bool,
	explicitIncludeServerSide *bool,
	validatedModeOverride string,
	tcUnknown map[string]jsontext.Value,
	tcWasPresent bool,
	fccUnknown map[string]jsontext.Value,
	fccWasPresent bool,
	losses *[]ir.Loss,
) (*wireToolConfig, error) {
	var fccBytes jsontext.Value
	if tc != nil {
		fcc := wireFunctionCallingConfig{
			Unknown: fccUnknown,
		}
		approximated := false
		switch tc.Mode {
		case ir.ToolChoiceAuto:
			fcc.Mode = "AUTO"
			if strings.EqualFold(validatedModeOverride, "VALIDATED") {
				fcc.Mode = "VALIDATED"
			}
			fcc.AllowedFunctionNames, approximated = encodeAllowedToolNames(tc.AllowedTools, maxNameBytes)
		case ir.ToolChoiceNone:
			fcc.Mode = "NONE"
			fcc.AllowedFunctionNames, approximated = encodeAllowedToolNames(tc.AllowedTools, maxNameBytes)
		case ir.ToolChoiceRequired:
			fcc.Mode = "ANY"
			fcc.AllowedFunctionNames, approximated = encodeAllowedToolNames(tc.AllowedTools, maxNameBytes)
		case ir.ToolChoiceTool:
			fcc.Mode = "ANY"
			fcc.AllowedFunctionNames, approximated = encodeAllowedToolNames([]ir.ToolRef{tc.Tool}, maxNameBytes)
		}
		if approximated {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossToolChoiceApproximated,
				Path: "tool_choice",
			})
		}
		raw, err := marshalDeterministic(fcc)
		if err != nil {
			return nil, err
		}
		fccBytes = jsontext.Value(raw)
	} else if len(fccUnknown) > 0 || fccWasPresent {
		raw, err := marshalDeterministic(wireFunctionCallingConfig{Unknown: fccUnknown})
		if err != nil {
			return nil, err
		}
		fccBytes = jsontext.Value(raw)
	}

	var includeServerSide *bool
	if explicitIncludeServerSide != nil {
		v := *explicitIncludeServerSide
		includeServerSide = &v
	} else if hasMixedTools {
		// Gemini requires includeServerSideToolInvocations=true whenever client
		// functionDeclarations coexist with built-in googleSearch or codeExecution.
		enabled := true
		includeServerSide = &enabled
	}

	if len(fccBytes) == 0 && includeServerSide == nil && len(tcUnknown) == 0 && !tcWasPresent {
		return nil, nil
	}
	return &wireToolConfig{
		FunctionCallingConfig:            fccBytes,
		IncludeServerSideToolInvocations: includeServerSide,
		Unknown:                          tcUnknown,
	}, nil
}

// encodeAllowedToolNames converts AllowedTools references into sanitized Gemini
// function names and reports whether any reference was lowered or renamed.
// Empty input slices return (nil, false) without allocating.
func encodeAllowedToolNames(refs []ir.ToolRef, maxNameBytes int) ([]string, bool) {
	if len(refs) == 0 {
		return nil, false
	}
	names := make([]string, 0, len(refs))
	approximated := false
	for _, r := range refs {
		name := r.Name
		if r.Namespace != "" {
			approximated = true
			name = r.Namespace + "__" + name
		}
		if r.Kind == ir.ToolClientSearch && strings.TrimSpace(name) == "" {
			approximated = true
			name = clientSearchProxyName
		}
		sanitized := sanitizeGeminiToolName(name, maxNameBytes)
		if sanitized != r.Name || r.Kind != ir.ToolFunction {
			approximated = true
		}
		names = append(names, sanitized)
	}
	return names, approximated
}

// encodeGenerationConfig builds the outgoing Gemini generationConfig object
// from Sampling, MaxOutputTokens, Stop, Reasoning, OutputFormat, and preserved
// same-origin Extra fields. It returns (nil, nil) when no generation settings
// survive dialect filtering and the incoming request carried no generationConfig.
//
// Positive thinking budgets pad maxOutputTokens upward by BudgetPadding (or
// 4096 tokens when unset) before OutputLimit clamping so maxOutputTokens stays
// strictly above thinkingBudget, clamping thinkingBudget to maxOutputTokens-1
// if OutputLimit leaves less headroom. When strippedThinking is true under
// antigravity_claude, thinkingConfig is omitted completely for the request.
func encodeGenerationConfig(
	req *ir.Request,
	dialect ir.Dialect,
	strippedThinking bool,
	explicitCustomMime string,
	gcUnknown map[string]jsontext.Value,
	gcWasPresent bool,
	thUnknown map[string]jsontext.Value,
	thWasPresent bool,
	losses *[]ir.Loss,
) (*wireGenerationConfig, error) {
	gc := wireGenerationConfig{
		Unknown: gcUnknown,
	}

	var maxOut *int64
	if req.MaxOutputTokens != nil && *req.MaxOutputTokens > 0 {
		v := *req.MaxOutputTokens
		maxOut = &v
	}

	var effectiveRC *ir.ReasoningConfig
	if req.Reasoning != nil {
		rcCopy := *req.Reasoning
		if dialect.StripEffort {
			rcCopy.Effort = ""
			if rcCopy == (ir.ReasoningConfig{}) {
				effectiveRC = nil
			} else {
				effectiveRC = &rcCopy
			}
		} else {
			effectiveRC = &rcCopy
		}
	}

	// Resolve thinkingConfig unless antigravity_claude downgraded an unsigned
	// historical thinking block and requires suppressing thinkingConfig this turn.
	var thBytes jsontext.Value
	var resolvedBudget *int64
	if !strippedThinking {
		var err error
		thBytes, resolvedBudget, err = resolveAndEncodeThinkingConfig(
			effectiveRC,
			dialect.Reasoning,
			dialect.SchemaDialect,
			thUnknown,
			thWasPresent,
		)
		if err != nil {
			return nil, err
		}
	}
	gc.ThinkingConfig = thBytes

	// Ensure maxOutputTokens strictly exceeds any positive thinkingBudget before
	// applying the model ceiling so ValidateGeminiRequest GEM-THINKING-BUDGET holds.
	if maxOut != nil && resolvedBudget != nil && *resolvedBudget > 0 && *maxOut <= *resolvedBudget {
		padding := defaultBudgetHeadroomTokens
		if dialect.Reasoning.BudgetPadding > 0 {
			padding = int64(dialect.Reasoning.BudgetPadding)
		}
		adjusted := *resolvedBudget + padding
		maxOut = &adjusted
	}
	if maxOut != nil && dialect.Reasoning.OutputLimit != nil && *dialect.Reasoning.OutputLimit > 0 {
		capped := int64(*dialect.Reasoning.OutputLimit)
		if *maxOut > capped {
			maxOut = &capped
		}
		if resolvedBudget != nil && *resolvedBudget >= *maxOut && *maxOut > 1 {
			clampedBudget := *maxOut - 1
			rewritten, err := rewriteThinkingBudget(gc.ThinkingConfig, clampedBudget, thUnknown)
			if err != nil {
				return nil, err
			}
			gc.ThinkingConfig = rewritten
		}
	}
	gc.MaxOutputTokens = maxOut

	encodeSamplingAndStops(req, dialect, &gc, losses)

	if err := encodeOutputFormatIntoConfig(req.Output, dialect, explicitCustomMime, &gc, losses); err != nil {
		return nil, err
	}

	hasAnyField := gc.Temperature != nil ||
		gc.TopP != nil ||
		gc.TopK != nil ||
		gc.CandidateCount != nil ||
		gc.MaxOutputTokens != nil ||
		len(gc.StopSequences) > 0 ||
		gc.PresencePenalty != nil ||
		gc.FrequencyPenalty != nil ||
		gc.Seed != nil ||
		gc.ResponseMimeType != "" ||
		len(gc.ResponseSchema) > 0 ||
		len(gc.ResponseJSONSchema) > 0 ||
		len(gc.ThinkingConfig) > 0 ||
		len(gc.Unknown) > 0
	if !hasAnyField && !gcWasPresent {
		return nil, nil
	}
	return &gc, nil
}

// encodeSamplingAndStops copies supported Sampling scalars and StopSequences
// from req onto gc according to dialect flags. Suppressed sampling parameters
// and Antigravity-unsupported penalties record LossSamplingDropped,
// LossCandidateCountIgnored, or LossStopDropped in losses.
func encodeSamplingAndStops(
	req *ir.Request,
	dialect ir.Dialect,
	gc *wireGenerationConfig,
	losses *[]ir.Loss,
) {
	s := req.Sampling
	if s.Temperature != nil {
		if dialect.DropTemperature {
			*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.temperature", Detail: "temperature"})
		} else {
			gc.Temperature = cloneFloat64Ptr(s.Temperature)
		}
	}
	if s.TopP != nil {
		if dialect.DropTopP {
			*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.top_p", Detail: "top_p"})
		} else {
			gc.TopP = cloneFloat64Ptr(s.TopP)
		}
	}
	if s.TopK != nil {
		if dialect.DropTopK {
			*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.top_k", Detail: "top_k"})
		} else {
			gc.TopK = cloneInt64Ptr(s.TopK)
		}
	}
	if s.Seed != nil {
		if dialect.SchemaDialect == ir.SchemaAntigravity {
			*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.seed", Detail: "seed"})
		} else {
			gc.Seed = cloneInt64Ptr(s.Seed)
		}
	}
	if s.FrequencyPenalty != nil {
		if dialect.SchemaDialect == ir.SchemaAntigravity {
			*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.frequency_penalty", Detail: "frequency_penalty"})
		} else {
			gc.FrequencyPenalty = cloneFloat64Ptr(s.FrequencyPenalty)
		}
	}
	if s.PresencePenalty != nil {
		if dialect.SchemaDialect == ir.SchemaAntigravity {
			*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.presence_penalty", Detail: "presence_penalty"})
		} else {
			gc.PresencePenalty = cloneFloat64Ptr(s.PresencePenalty)
		}
	}
	if s.CandidateCount != nil {
		if dialect.SchemaDialect == ir.SchemaAntigravity {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCandidateCountIgnored, Path: "sampling.candidate_count"})
		} else {
			gc.CandidateCount = cloneInt64Ptr(s.CandidateCount)
		}
	}

	if len(req.Stop) > 0 {
		if dialect.OmitStopSequences {
			*losses = append(*losses, ir.Loss{Kind: ir.LossStopDropped, Path: "stop"})
		} else {
			gc.StopSequences = slices.Clone(req.Stop)
		}
	}
}

// encodeOutputFormatIntoConfig maps outFmt onto gc.ResponseMimeType and
// gc.ResponseSchema or gc.ResponseJSONSchema while recording incurred losses.
// Nil outFmt values restore explicitCustomMime when present on same-origin
// requests and leave gc unchanged otherwise.
func encodeOutputFormatIntoConfig(
	outFmt *ir.OutputFormat,
	dialect ir.Dialect,
	explicitCustomMime string,
	gc *wireGenerationConfig,
	losses *[]ir.Loss,
) error {
	if outFmt == nil {
		if explicitCustomMime != "" {
			gc.ResponseMimeType = explicitCustomMime
			delete(gc.Unknown, "responseMimeType")
		}
		return nil
	}

	if outFmt.Verbosity != "" {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output.verbosity",
			Detail: "verbosity",
		})
	}
	if outFmt.Description != "" {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output.description",
			Detail: "description",
		})
	}
	if outFmt.Strict != nil {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output.strict",
			Detail: "strict",
		})
	}
	if outFmt.Name != "" {
		// Gemini responseSchema carries only the schema object without json_schema.name,
		// so record whole-output approximation when a cross-protocol Name is dropped.
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output",
			Detail: "name_dropped",
		})
	}

	switch outFmt.Kind {
	case "text":
		gc.ResponseMimeType = "text/plain"
	case "json_object":
		gc.ResponseMimeType = "application/json"
	case "json_schema":
		gc.ResponseMimeType = "application/json"
		switch dialect.SchemaDialect {
		case ir.SchemaGemini:
			cleaned, changed, err := CleanGeminiSchema(outFmt.Schema)
			if err != nil {
				return fmt.Errorf("output.schema: clean Gemini schema: %w", err)
			}
			gc.ResponseSchema = cleaned
			if changed && outFmt.Name == "" {
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossSchemaApproximated,
					Path:   "output.schema",
					Detail: "gemini",
				})
			}
		case ir.SchemaAntigravity:
			cleaned, changed, err := CleanAntigravitySchema(outFmt.Schema)
			if err != nil {
				return fmt.Errorf("output.schema: clean Antigravity schema: %w", err)
			}
			gc.ResponseSchema = cleaned
			if changed && outFmt.Name == "" {
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossSchemaApproximated,
					Path:   "output.schema",
					Detail: "antigravity",
				})
			}
		default:
			if len(bytes.TrimSpace(outFmt.Schema)) > 0 {
				compacted := compactRawJSON(outFmt.Schema)
				if isStrictGeminiParametersSchema(compacted) {
					gc.ResponseSchema = compacted
				} else {
					gc.ResponseJSONSchema = compacted
				}
			}
		}
	}
	return nil
}

// resolveAndEncodeThinkingConfig translates rc and caps into serialized
// generationConfig.thinkingConfig JSON bytes and returns any resolved
// thinkingBudget pointer (-1, 0, or positive). Unsupported thinking controls
// or incompatible effort levels return a descriptive error.
//
// Configs carrying only IncludeEncrypted or Summary metadata without an
// explicit mode, budget, or effort return no thinkingConfig unless same-origin
// Extra fields must be restored. Disabled thinking emits thinkingBudget=0 with
// includeThoughts=false on upstream dialects, whereas enabled thinking sets
// includeThoughts=true on upstream dialects (and when Summary is non-empty on
// empty-dialect round trips) while choosing mutually exclusive thinkingBudget
// or thinkingLevel from caps.
func resolveAndEncodeThinkingConfig(
	rc *ir.ReasoningConfig,
	caps ir.ReasoningCapabilities,
	schemaDialect ir.SchemaDialect,
	thUnknown map[string]jsontext.Value,
	thWasPresent bool,
) (jsontext.Value, *int64, error) {
	explicitIncludeFalse := false
	if rawInc, ok := thUnknown["includeThoughts"]; ok && bytes.Equal(bytes.TrimSpace(rawInc), []byte("false")) {
		explicitIncludeFalse = true
		delete(thUnknown, "includeThoughts")
	}

	if rc == nil {
		if explicitIncludeFalse {
			f := false
			raw, err := marshalDeterministic(wireThinkingConfig{
				IncludeThoughts: &f,
				Unknown:         thUnknown,
			})
			return jsontext.Value(raw), nil, err
		}
		if len(thUnknown) > 0 || thWasPresent {
			raw, err := marshalDeterministic(wireThinkingConfig{Unknown: thUnknown})
			return jsontext.Value(raw), nil, err
		}
		return nil, nil, nil
	}

	effort := strings.ToLower(strings.TrimSpace(rc.Effort))
	disabled := rc.Mode == "disabled" || effort == "none" || (rc.BudgetTokens != nil && *rc.BudgetTokens == 0)

	if disabled {
		if caps.Supported != nil && !*caps.Supported {
			return nil, nil, nil
		}
		if caps.ControlsKnown && !caps.Toggle {
			return nil, nil, errors.New("selected model does not support disabling thinking")
		}
		zero := int64(0)
		var incPtr *bool
		if explicitIncludeFalse || ((schemaDialect != "" || caps.ControlsKnown || caps.ForceBudget) && rc.Summary == "") {
			f := false
			incPtr = &f
		} else if rc.Summary != "" {
			t := true
			incPtr = &t
		}
		raw, err := marshalDeterministic(wireThinkingConfig{
			ThinkingBudget:  &zero,
			IncludeThoughts: incPtr,
			Unknown:         thUnknown,
		})
		if err != nil {
			return nil, nil, err
		}
		return jsontext.Value(raw), &zero, nil
	}

	// Ignore metadata-only configs (such as Responses include=["reasoning.encrypted_content"]
	// without effort or budget) so non-reasoning models are not rejected and
	// budget models do not receive an unrequested thinkingBudget=-1.
	hasExplicitThinking := rc.Mode == "enabled" || rc.Mode == "adaptive" || rc.BudgetTokens != nil
	if !hasExplicitThinking && effort == "" {
		if explicitIncludeFalse {
			f := false
			raw, err := marshalDeterministic(wireThinkingConfig{
				IncludeThoughts: &f,
				Unknown:         thUnknown,
			})
			return jsontext.Value(raw), nil, err
		}
		if len(thUnknown) > 0 || thWasPresent {
			raw, err := marshalDeterministic(wireThinkingConfig{Unknown: thUnknown})
			return jsontext.Value(raw), nil, err
		}
		return nil, nil, nil
	}

	if caps.Supported != nil && !*caps.Supported {
		return nil, nil, errors.New("selected model does not support thinking")
	}
	if caps.ControlsKnown && !caps.ForceBudget && !caps.Budget && len(caps.Efforts) == 0 && !caps.Toggle {
		return nil, nil, errors.New("selected model does not expose adjustable thinking")
	}

	var incPtr *bool
	if explicitIncludeFalse {
		f := false
		incPtr = &f
	} else if schemaDialect != "" || caps.ControlsKnown || caps.ForceBudget || rc.Summary != "" {
		t := true
		incPtr = &t
	}

	useBudget := caps.ForceBudget ||
		(caps.Budget && (rc.BudgetTokens != nil || len(caps.Efforts) == 0)) ||
		(!caps.ControlsKnown && rc.BudgetTokens != nil)

	if useBudget {
		budget, err := selectAndClampGeminiBudget(rc, effort, caps)
		if err != nil {
			return nil, nil, err
		}
		raw, err := marshalDeterministic(wireThinkingConfig{
			ThinkingBudget:  &budget,
			IncludeThoughts: incPtr,
			Unknown:         thUnknown,
		})
		if err != nil {
			return nil, nil, err
		}
		return jsontext.Value(raw), &budget, nil
	}

	chosenEffort := effort
	if chosenEffort == "" && rc.BudgetTokens != nil && *rc.BudgetTokens > 0 {
		chosenEffort = geminiBudgetToEffort(*rc.BudgetTokens)
	}
	if chosenEffort == "" || chosenEffort == "default" {
		raw, err := marshalDeterministic(wireThinkingConfig{
			IncludeThoughts: incPtr,
			Unknown:         thUnknown,
		})
		if err != nil {
			return nil, nil, err
		}
		return jsontext.Value(raw), nil, nil
	}

	if caps.ControlsKnown {
		var err error
		chosenEffort, err = selectCompatibleGeminiEffort(chosenEffort, caps.Efforts)
		if err != nil {
			return nil, nil, err
		}
	}

	raw, err := marshalDeterministic(wireThinkingConfig{
		ThinkingLevel:   chosenEffort,
		IncludeThoughts: incPtr,
		Unknown:         thUnknown,
	})
	if err != nil {
		return nil, nil, err
	}
	return jsontext.Value(raw), nil, nil
}

// selectAndClampGeminiBudget resolves the target Gemini thinkingBudget (-1 or
// positive) from rc, effort, and caps. Positive budgets clamp into [BudgetMin,
// BudgetMax] and OutputLimit-BudgetPadding, returning an error when bounds leave
// no positive budget or when budget falls below -1.
func selectAndClampGeminiBudget(rc *ir.ReasoningConfig, effort string, caps ir.ReasoningCapabilities) (int64, error) {
	adaptive := rc.Mode == "adaptive"
	budget := int64(-1)
	switch {
	case adaptive && caps.AdaptiveBudget > 0 && effort == "":
		budget = int64(caps.AdaptiveBudget)
	case rc.BudgetTokens != nil:
		budget = *rc.BudgetTokens
	case effort != "" && effort != "default":
		norm, err := selectCompatibleGeminiEffort(effort, nil)
		if err != nil {
			return 0, err
		}
		if norm == "minimal" {
			norm = "low"
		}
		budget = defaultGeminiEffortBudget(norm)
	case adaptive && caps.AdaptiveBudget > 0:
		budget = int64(caps.AdaptiveBudget)
	}
	if budget < -1 {
		return 0, errors.New("thinking budget must be -1 or nonnegative")
	}
	if budget > 0 {
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
	}
	return budget, nil
}

// selectCompatibleGeminiEffort normalizes "ultra" and "extrahigh" aliases onto
// canonical Gemini effort tiers and matches against allowed. When allowed omits
// effort, it picks the highest supported tier at or below effort (or the lowest
// supported tier otherwise), returning an error on unrecognized effort names.
func selectCompatibleGeminiEffort(effort string, allowed []string) (string, error) {
	order := []string{"minimal", "low", "medium", "high", "xhigh", "max"}
	switch effort {
	case "ultra":
		effort = "max"
	case "extrahigh":
		effort = "xhigh"
	}
	rank := slices.Index(order, effort)
	if rank < 0 {
		return "", fmt.Errorf("unsupported reasoning effort %q", effort)
	}
	if len(allowed) == 0 || slices.Contains(allowed, effort) {
		return effort, nil
	}
	selected := ""
	for idx, cand := range order {
		if !slices.Contains(allowed, cand) {
			continue
		}
		if selected == "" || idx <= rank {
			selected = cand
		}
	}
	if selected == "" {
		return "", errors.New("selected model has no compatible reasoning effort")
	}
	return selected, nil
}

// defaultGeminiEffortBudget maps canonical effort labels onto fixed Gemini
// thinkingBudget token allowances. Unrecognized or empty effort labels default
// to 10240 tokens when resolving a positive token budget for budget-controlled
// models.
func defaultGeminiEffortBudget(effort string) int64 {
	switch effort {
	case "low":
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

// geminiBudgetToEffort maps a positive token budget onto "low" (<=1024),
// "medium" (<=4096), or "high" (>4096) for level-only Gemini models. Callers
// pass only positive budgets after handling disabled zero and dynamic -1.
func geminiBudgetToEffort(budget int64) string {
	switch {
	case budget <= 1024:
		return "low"
	case budget <= 4096:
		return "medium"
	default:
		return "high"
	}
}

// rewriteThinkingBudget updates thinkingBudget inside an already-marshaled
// thinkingConfig JSON object when Dialect.Reasoning.OutputLimit clamps the
// positive thinking budget below the model output token cap. Calling this
// helper preserves any unmodeled thinkingConfig Unknown members unchanged.
func rewriteThinkingBudget(raw jsontext.Value, clamped int64, thUnknown map[string]jsontext.Value) (jsontext.Value, error) {
	var tc wireThinkingConfig
	if err := unmarshalJSONObject(raw, &tc); err != nil {
		return nil, err
	}
	tc.ThinkingBudget = &clamped
	tc.Unknown = thUnknown
	updated, err := marshalDeterministic(tc)
	if err != nil {
		return nil, err
	}
	return jsontext.Value(updated), nil
}

// extractNestedSubObjectUnknown extracts an unknown map from
// topUnknown[parentKey][childKey] while stripping modeled stripKeys and
// reporting whether childKey originally held a JSON object. Missing or
// non-object parent and child entries return (nil, false).
func extractNestedSubObjectUnknown(
	topUnknown map[string]jsontext.Value,
	parentKey string,
	childKey string,
	stripKeys ...string,
) (map[string]jsontext.Value, bool) {
	parentMap, ok := extractSubObjectUnknown(topUnknown, parentKey)
	if !ok || len(parentMap) == 0 {
		return nil, false
	}
	return extractSubObjectUnknown(parentMap, childKey, stripKeys...)
}

// extractNestedBoolMember reads an optional boolean stored under childKey inside
// topUnknown[parentKey]. Missing keys, non-object parent containers, or
// non-boolean JSON tokens return nil without failing request encoding.
func extractNestedBoolMember(topUnknown map[string]jsontext.Value, parentKey, childKey string) *bool {
	parentMap, ok := extractSubObjectUnknown(topUnknown, parentKey)
	if !ok || len(parentMap) == 0 {
		return nil
	}
	raw, exists := parentMap[childKey]
	if !exists {
		return nil
	}
	var b bool
	if unmarshalJSONValue(bytes.TrimSpace(raw), &b) != nil {
		return nil
	}
	return &b
}

// extractNestedStringMember walks keys inside topUnknown and returns the JSON
// string value stored at the leaf key. Missing intermediate maps, non-object
// containers, or non-string leaf values return "".
func extractNestedStringMember(topUnknown map[string]jsontext.Value, keys ...string) string {
	cur := topUnknown
	for i, k := range keys {
		if len(cur) == 0 {
			return ""
		}
		raw, exists := cur[k]
		if !exists {
			return ""
		}
		if i == len(keys)-1 {
			s, ok := tryDecodeJSONString(raw)
			if !ok {
				return ""
			}
			return s
		}
		var next map[string]jsontext.Value
		if unmarshalJSONObject(raw, &next) != nil {
			return ""
		}
		cur = next
	}
	return ""
}
