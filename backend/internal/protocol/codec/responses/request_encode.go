package responses

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
)

// request_encode.go serializes normalized ir.Request values into deterministic
// OpenAI Responses JSON request bytes under target Dialect capabilities.
// Outgoing requests encode System into top-level instructions, flatten Messages
// into input[] items, reconstruct namespace wrappers when supported, and preserve
// same-origin top-level and sub-object Extra fields without key-specific lookups.
//
// Dialect flags control whether custom, namespace, client_search, and hosted
// tools remain native or lower into 64-byte function tools, whether forbidden
// sampling and stop controls drop with ir.Loss entries, and whether effort
// strings strip or fail validation. Cross-protocol defaults (such as forcing
// store=false or verbosity="medium" for Messages/Chat clients) stay outside this
// encoder so same-protocol Law Q2 round trips incur zero Loss.

// wireAllowedToolsChoiceWrite serializes an outgoing OpenAI Responses allowed_tools selector object.
type wireAllowedToolsChoiceWrite struct {
	Type    string                    `json:"type"`          // literal "allowed_tools" discriminator
	Mode    string                    `json:"mode,omitzero"` // "auto", "required", or "none"
	Tools   []wireAllowedToolEntry    `json:"tools"`         // allowed tool subset
	Unknown map[string]jsontext.Value `json:",embed"`        // preserved same-origin Extra members
}

// wireSpecificToolChoiceWrite serializes an outgoing function, custom, tool_search, or hosted tool_choice object.
type wireSpecificToolChoiceWrite struct {
	Type      string                    `json:"type"`               // function, custom, tool_search, or hosted type
	Name      string                    `json:"name,omitzero"`      // target tool name when applicable
	Namespace string                    `json:"namespace,omitzero"` // optional namespace scope
	Unknown   map[string]jsontext.Value `json:",embed"`             // preserved same-origin Extra members
}

// EncodeRequest serializes req into deterministic OpenAI Responses JSON bytes
// according to dialect and returns all incurred information-loss records. Nil
// requests or unsupported "ultra" reasoning efforts return an error without
// mutating caller-owned slices or maps.
//
// System parts join into top-level instructions, conversation turns legalize
// into flat input[] items, and same-origin sub-object extensions merge back onto
// reasoning, include, text, metadata, conversation, and tool_choice. Every
// dialect-lowered tool, omitted sampling or stop parameter, or dropped foreign
// Extra map records an original-coordinate ir.Loss entry for Law Q4 verification.
func EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	if req == nil {
		return nil, nil, errors.New("encode responses request: nil request")
	}

	var losses []ir.Loss
	reasoningOut, includeList, err := encodeResponsesReasoningAndInclude(req.Reasoning, dialect)
	if err != nil {
		return nil, nil, err
	}

	maxNameBytes := defaultResponsesToolNameMaxBytes
	if dialect.ToolNameMaxBytes > 0 {
		maxNameBytes = dialect.ToolNameMaxBytes
	}

	instructions := legalizeSystemInstructions(req.System, &losses)
	inputItems, err := legalizeInputItems(req, dialect, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}
	if inputItems == nil {
		inputItems = []jsontext.Value{}
	}

	toolsOut, err := encodeResponsesTools(req.Tools, req.Namespaces, dialect, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}

	var textCfg *wireTextConfig
	var textFormat *wireTextFormat
	if req.Output != nil {
		textCfg, textFormat = encodeResponsesTextConfig(req.Output, &losses)
	}

	var metadata *wireMetadata
	if req.MetadataUserID != "" {
		metadata = &wireMetadata{UserID: req.MetadataUserID}
	}

	serviceTier := openai.ServiceTierValue(req.ServiceTier)
	if serviceTier == "" && strings.TrimSpace(req.ServiceTier) != "" {
		serviceTier = strings.TrimSpace(req.ServiceTier)
	}

	hadSameOriginExtra := req.Extra.Origin == protocol.ProtocolOpenAIResponses && !req.Extra.IsZero()
	topUnknown := restoreOrDropExtra(req.Extra, "", &losses)

	var tcUnknown map[string]jsontext.Value
	if _, hasTCExtra := topUnknown["tool_choice"]; hasTCExtra {
		tcUnknown = extractSubObjectUnknown(topUnknown, "tool_choice", "type", "name", "namespace", "mode", "tools", "function", "allowed_tools")
		delete(topUnknown, "tool_choice")
	}

	lossCountBeforeSub := len(losses)
	toolChoiceOut, emittedTCExtra, err := encodeResponsesToolChoice(req.ToolChoice, dialect, maxNameBytes, tcUnknown, &losses)
	if err != nil {
		return nil, nil, err
	}

	convOut, convIDOut, emittedConvExtra, err := encodeConversationFields(req.ConversationID, req.ConversationSet, topUnknown)
	if err != nil {
		return nil, nil, err
	}

	emittedSubExtra := mergeResponsesSubObjectExtras(
		topUnknown,
		&reasoningOut,
		&includeList,
		&textCfg,
		textFormat,
		&metadata,
		&losses,
	)
	droppedSubExtra := false
	for _, l := range losses[lossCountBeforeSub:] {
		if l.Kind == ir.LossExtraDropped {
			droppedSubExtra = true
			break
		}
	}
	emittedAnySub := emittedSubExtra || emittedTCExtra || emittedConvExtra
	if hadSameOriginExtra && !emittedAnySub && !droppedSubExtra && len(topUnknown) == 0 {
		losses = append(losses, ir.Loss{
			Kind: ir.LossExtraDropped,
			Path: "",
		})
	}

	out := wireOutRequest{
		Model:              req.Model,
		Instructions:       instructions,
		Input:              inputItems,
		Tools:              toolsOut,
		ToolChoice:         toolChoiceOut,
		ParallelToolCalls:  req.ParallelToolCalls,
		MaxOutputTokens:    req.MaxOutputTokens,
		Reasoning:          reasoningOut,
		Include:            includeList,
		Text:               textCfg,
		Stream:             req.Stream,
		ServiceTier:        serviceTier,
		PromptCacheKey:     req.CacheKey,
		PreviousResponseID: req.PreviousResponseID,
		Store:              req.Store,
		Conversation:       convOut,
		ConversationID:     convIDOut,
		SessionID:          req.SessionID,
		User:               req.UserID,
		Metadata:           metadata,
		Unknown:            topUnknown,
	}

	encodeResponsesSamplingAndStop(req, dialect, &out, &losses)

	encoded, err := marshalDeterministic(out)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal responses request: %w", err)
	}
	return encoded, losses, nil
}

// EncodeRequest serializes req into deterministic OpenAI Responses JSON bytes by
// delegating to the package-level EncodeRequest function with dialect. Nil
// requests or unsupported reasoning efforts return an error without mutating req.
func (Codec) EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	return EncodeRequest(req, dialect)
}

// encodeResponsesReasoningAndInclude resolves req.Reasoning under dialect into
// outgoing reasoning and include fields. Nil configs return nil values, while
// unsupported "ultra" effort strings fail unless dialect.StripEffort is enabled.
func encodeResponsesReasoningAndInclude(rc *ir.ReasoningConfig, dialect ir.Dialect) (*wireReasoningConfig, []string, error) {
	if rc == nil {
		return nil, nil, nil
	}
	var effort string
	if !dialect.StripEffort {
		effort = strings.TrimSpace(rc.Effort)
		if strings.EqualFold(effort, "ultra") {
			return nil, nil, fmt.Errorf("reasoning effort %q is not supported", effort)
		}
		if effort != "" {
			if norm := protocol.NormalizeReasoningEffort(effort); norm != "" {
				effort = norm
			}
		}
	}
	summary := strings.TrimSpace(rc.Summary)

	var outCfg *wireReasoningConfig
	if effort != "" || summary != "" {
		outCfg = &wireReasoningConfig{
			Effort:  effort,
			Summary: summary,
		}
	}

	var includes []string
	if rc.IncludeEncrypted && !dialect.OmitStoreAndInclude {
		includes = []string{"reasoning.encrypted_content"}
	}
	return outCfg, includes, nil
}

// encodeResponsesTextConfig converts req.Output into outgoing text and
// text.format structs. Missing json_schema names default to defaultResponseSchemaName
// with LossOutputFormatApproximated, while empty text configs return (nil, nil).
func encodeResponsesTextConfig(out *ir.OutputFormat, losses *[]ir.Loss) (*wireTextConfig, *wireTextFormat) {
	var fmtOut *wireTextFormat
	switch out.Kind {
	case "":
	case "text":
		fmtOut = &wireTextFormat{Type: "text"}
	case "json_object":
		fmtOut = &wireTextFormat{Type: "json_object"}
	case "json_schema":
		name := out.Name
		if strings.TrimSpace(name) == "" {
			name = defaultResponseSchemaName
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossOutputFormatApproximated,
				Path:   "output",
				Detail: "name",
			})
		}
		var schema jsontext.Value
		if len(out.Schema) > 0 {
			schema = jsontext.Value(bytes.Clone(out.Schema))
		}
		fmtOut = &wireTextFormat{
			Type:        "json_schema",
			Name:        name,
			Description: out.Description,
			Schema:      schema,
			Strict:      out.Strict,
		}
	default:
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output",
			Detail: out.Kind,
		})
	}

	if fmtOut == nil && out.Verbosity == "" {
		return nil, nil
	}
	return &wireTextConfig{
		Format:    fmtOut,
		Verbosity: out.Verbosity,
	}, fmtOut
}

// encodeResponsesTools serializes req.Tools and req.Namespaces into tools[] JSON
// objects under dialect.
//
// When dialect supports ToolSupportNamespace, contiguous runs of tools sharing a
// non-empty Namespace re-wrap inside their corresponding namespace declaration
// object while flushing any preceding empty namespaces in declaration order for
// lossless Law Q2 round-tripping. Restricted dialects flatten namespaced
// identifiers into 64-byte function names, wrap custom tools in single-field
// string schemas, lower tool_search declarations into functions, and omit
// unsupported hosted tools with explicit ir.Loss entries.
func encodeResponsesTools(
	tools []ir.Tool,
	namespaces []ir.Namespace,
	dialect ir.Dialect,
	maxNameBytes int,
	losses *[]ir.Loss,
) ([]jsontext.Value, error) {
	support := effectiveToolSupport(dialect)
	if len(tools) == 0 && (!support.Has(ir.ToolSupportNamespace) || len(namespaces) == 0) {
		return nil, nil
	}

	var out []jsontext.Value
	usedNS := make([]bool, len(namespaces))
	emitEmptyNamespace := func(idx int, ns ir.Namespace) error {
		nsPath := fmt.Sprintf("namespaces[%d]", idx)
		if ns.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: nsPath})
		}
		nsUnk := restoreOrDropExtra(ns.Extra, nsPath, losses)
		wrapped, err := marshalDeterministic(wireNamespaceTool{
			Type:         "namespace",
			Name:         ns.Name,
			Description:  ns.Description,
			DeferLoading: ns.DeferLoading,
			Tools:        []jsontext.Value{},
			Unknown:      nsUnk,
		})
		if err != nil {
			return err
		}
		out = append(out, jsontext.Value(wrapped))
		return nil
	}
	claimNamespace := func(name string) (ir.Namespace, int, bool, error) {
		for idx, ns := range namespaces {
			if !usedNS[idx] && ns.Name == name {
				// Emit preceding unclaimed empty namespaces before ns so decoded
				// Namespaces slice order stays invariant under Law Q2 round trips.
				for prev := range idx {
					if !usedNS[prev] {
						usedNS[prev] = true
						if err := emitEmptyNamespace(prev, namespaces[prev]); err != nil {
							return ir.Namespace{}, 0, false, err
						}
					}
				}
				usedNS[idx] = true
				return ns, idx, true, nil
			}
		}
		return ir.Namespace{Name: name}, -1, false, nil
	}

	for i := 0; i < len(tools); {
		t := tools[i]
		if t.Namespace != "" && support.Has(ir.ToolSupportNamespace) &&
			(t.Kind == ir.ToolFunction || (t.Kind == ir.ToolCustom && support.Has(ir.ToolSupportCustom))) {
			nsName := t.Namespace
			nsMeta, nsIdx, matchedNS, err := claimNamespace(nsName)
			if err != nil {
				return nil, err
			}
			nsPath := fmt.Sprintf("tools[%d]", i)
			if matchedNS {
				nsPath = fmt.Sprintf("namespaces[%d]", nsIdx)
			}
			if nsMeta.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: nsPath})
			}
			nsUnk := restoreOrDropExtra(nsMeta.Extra, nsPath, losses)

			var childBytes []jsontext.Value
			j := i
			for j < len(tools) {
				cand := tools[j]
				if cand.Namespace != nsName {
					break
				}
				if cand.Kind != ir.ToolFunction && (cand.Kind != ir.ToolCustom || !support.Has(ir.ToolSupportCustom)) {
					break
				}
				cRaw, ok, err := encodeSingleResponsesTool(cand, j, true, support, maxNameBytes, losses)
				if err != nil {
					return nil, err
				}
				if ok {
					childBytes = append(childBytes, cRaw)
				}
				j++
			}
			wrapped, err := marshalDeterministic(wireNamespaceTool{
				Type:         "namespace",
				Name:         nsName,
				Description:  nsMeta.Description,
				DeferLoading: nsMeta.DeferLoading,
				Tools:        childBytes,
				Unknown:      nsUnk,
			})
			if err != nil {
				return nil, err
			}
			out = append(out, jsontext.Value(wrapped))
			i = j
			continue
		}

		rawTool, ok, err := encodeSingleResponsesTool(t, i, false, support, maxNameBytes, losses)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, rawTool)
		}
		i++
	}

	if support.Has(ir.ToolSupportNamespace) {
		for idx, ns := range namespaces {
			if usedNS[idx] {
				continue
			}
			usedNS[idx] = true
			if err := emitEmptyNamespace(idx, ns); err != nil {
				return nil, err
			}
		}
	}

	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// encodeSingleResponsesTool serializes one ir.Tool declaration into JSON bytes
// under support and maxNameBytes. Unsupported hosted tools return ok=false
// after recording LossHostedToolDropped.
func encodeSingleResponsesTool(
	t ir.Tool,
	idx int,
	insideNamespace bool,
	support ir.ToolSupport,
	maxNameBytes int,
	losses *[]ir.Loss,
) (jsontext.Value, bool, error) {
	tPath := fmt.Sprintf("tools[%d]", idx)
	if t.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tPath})
	}

	lowered := false
	name := t.Name
	if !insideNamespace && t.Namespace != "" {
		name = flattenNamespaceToolName(t.Namespace, t.Name, maxNameBytes)
		lowered = true
	}

	switch t.Kind {
	case ir.ToolFunction:
		sanitized, changed := sanitizeResponsesToolName(name, maxNameBytes)
		if changed {
			lowered = true
		}
		if lowered {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
		}
		tUnk := restoreOrDropExtra(t.Extra, tPath, losses)
		var params jsontext.Value
		if len(t.InputSchema) > 0 {
			params = jsontext.Value(bytes.Clone(t.InputSchema))
		}
		raw, err := marshalDeterministic(wireFunctionTool{
			Type:         "function",
			Name:         sanitized,
			Description:  t.Description,
			Parameters:   params,
			Strict:       t.Strict,
			DeferLoading: t.DeferLoading,
			Unknown:      tUnk,
		})
		return jsontext.Value(raw), err == nil, err

	case ir.ToolCustom:
		sanitized, changed := sanitizeResponsesToolName(name, maxNameBytes)
		if changed {
			lowered = true
		}
		if support.Has(ir.ToolSupportCustom) {
			if lowered {
				*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			}
			tUnk := restoreOrDropExtra(t.Extra, tPath, losses)
			var format jsontext.Value
			if len(t.Format) > 0 {
				format = jsontext.Value(bytes.Clone(t.Format))
			}
			raw, err := marshalDeterministic(wireCustomTool{
				Type:         "custom",
				Name:         sanitized,
				Description:  t.Description,
				Format:       format,
				DeferLoading: t.DeferLoading,
				Unknown:      tUnk,
			})
			return jsontext.Value(raw), err == nil, err
		}
		_ = restoreOrDropExtra(t.Extra, tPath, losses)
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
		raw, err := marshalDeterministic(wireFunctionTool{
			Type:         "function",
			Name:         sanitized,
			Description:  t.Description,
			Parameters:   jsontext.Value(customToolInputSchemaJSON),
			DeferLoading: t.DeferLoading,
		})
		return jsontext.Value(raw), err == nil, err

	case ir.ToolClientSearch:
		if support.Has(ir.ToolSupportClientSearch) {
			tUnk := restoreOrDropExtra(t.Extra, tPath, losses)
			raw, err := marshalDeterministic(wireToolSearchTool{
				Type:         "tool_search",
				Name:         t.Name,
				Description:  t.Description,
				Strict:       t.Strict,
				DeferLoading: t.DeferLoading,
				Unknown:      tUnk,
			})
			return jsontext.Value(raw), err == nil, err
		}
		_ = restoreOrDropExtra(t.Extra, tPath, losses)
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
		fnName := "tool_search"
		if strings.TrimSpace(t.Name) != "" {
			fnName, _ = sanitizeResponsesToolName(t.Name, maxNameBytes)
		}
		raw, err := marshalDeterministic(wireFunctionTool{
			Type:         "function",
			Name:         fnName,
			Description:  t.Description,
			Parameters:   jsontext.Value(toolSearchInputSchemaJSON),
			Strict:       t.Strict,
			DeferLoading: t.DeferLoading,
		})
		return jsontext.Value(raw), err == nil, err

	case ir.ToolHosted:
		_ = restoreOrDropExtra(t.Extra, tPath, losses)
		if support.Has(ir.ToolSupportHostedWebSearch) && t.Hosted != nil {
			if t.Hosted.Origin == protocol.ProtocolOpenAIResponses {
				return jsontext.Value(bytes.Clone(t.Hosted.Raw)), true, nil
			}
			if isResponsesWebSearchType(t.Hosted.Type) {
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossToolLowered,
					Path:   tPath,
					Detail: "hosted_web_search",
				})
				return jsontext.Value(`{"type":"web_search"}`), true, nil
			}
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossHostedToolDropped, Path: tPath})
		return nil, false, nil

	default:
		return nil, false, nil
	}
}

// isResponsesWebSearchType reports whether typ names a provider-hosted web
// search declaration convertible to OpenAI Responses' {"type":"web_search"}.
func isResponsesWebSearchType(typ string) bool {
	trimmed := strings.TrimSpace(typ)
	return strings.HasPrefix(trimmed, "web_search") || trimmed == "google_search"
}

// encodeResponsesToolChoice serializes tc into string or object tool_choice JSON
// under dialect, lowering restricted tool references when needed. Nil selectors
// return (nil, false, nil), and emittedTCExtra reports whether tcUnknown survived.
func encodeResponsesToolChoice(
	tc *ir.ToolChoice,
	dialect ir.Dialect,
	maxNameBytes int,
	tcUnknown map[string]jsontext.Value,
	losses *[]ir.Loss,
) (jsontext.Value, bool, error) {
	if tc == nil {
		return nil, false, nil
	}
	support := effectiveToolSupport(dialect)

	if len(tc.AllowedTools) > 0 {
		approximated := false
		entries := make([]wireAllowedToolEntry, 0, len(tc.AllowedTools))
		for _, ref := range tc.AllowedTools {
			ent, keep, approx := encodeAllowedToolRef(ref, support, maxNameBytes)
			if approx {
				approximated = true
			}
			if keep {
				entries = append(entries, ent)
			}
		}
		if len(entries) == 0 && tc.Mode != ir.ToolChoiceNone {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
			if len(tcUnknown) > 0 {
				*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: "tool_choice"})
			}
			return jsontext.Value(`"auto"`), false, nil
		}
		if approximated {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
		}
		raw, err := marshalDeterministic(wireAllowedToolsChoiceWrite{
			Type:    "allowed_tools",
			Mode:    string(tc.Mode),
			Tools:   entries,
			Unknown: tcUnknown,
		})
		return jsontext.Value(raw), len(tcUnknown) > 0, err
	}

	switch tc.Mode {
	case ir.ToolChoiceAuto, ir.ToolChoiceNone, ir.ToolChoiceRequired:
		if len(tcUnknown) > 0 {
			raw, err := marshalDeterministic(wireAllowedToolsChoiceWrite{
				Type:    "allowed_tools",
				Mode:    string(tc.Mode),
				Tools:   []wireAllowedToolEntry{},
				Unknown: tcUnknown,
			})
			return jsontext.Value(raw), true, err
		}
		switch tc.Mode {
		case ir.ToolChoiceAuto:
			return jsontext.Value(`"auto"`), false, nil
		case ir.ToolChoiceNone:
			return jsontext.Value(`"none"`), false, nil
		default:
			return jsontext.Value(`"required"`), false, nil
		}
	case ir.ToolChoiceTool:
		return encodeSpecificToolChoice(tc.Tool, support, maxNameBytes, tcUnknown, losses)
	default:
		return nil, false, fmt.Errorf("unsupported tool_choice mode %q", tc.Mode)
	}
}

// encodeSpecificToolChoice serializes a ToolChoiceTool target reference into JSON under support.
func encodeSpecificToolChoice(
	ref ir.ToolRef,
	support ir.ToolSupport,
	maxNameBytes int,
	tcUnknown map[string]jsontext.Value,
	losses *[]ir.Loss,
) (jsontext.Value, bool, error) {
	ns := ref.Namespace
	name := ref.Name
	approximated := false
	if ns != "" && !support.Has(ir.ToolSupportNamespace) {
		name = flattenNamespaceToolName(ns, name, maxNameBytes)
		ns = ""
		approximated = true
	}

	switch ref.Kind {
	case ir.ToolCustom:
		sanitized, changed := sanitizeResponsesToolName(name, maxNameBytes)
		if changed {
			approximated = true
		}
		outType := "custom"
		if !support.Has(ir.ToolSupportCustom) {
			outType = "function"
			approximated = true
		}
		if approximated {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
		}
		raw, err := marshalDeterministic(wireSpecificToolChoiceWrite{
			Type:      outType,
			Name:      sanitized,
			Namespace: ns,
			Unknown:   tcUnknown,
		})
		return jsontext.Value(raw), len(tcUnknown) > 0, err

	case ir.ToolClientSearch:
		if support.Has(ir.ToolSupportClientSearch) {
			outName := ""
			if name != "" && name != "tool_search" {
				outName = name
			}
			raw, err := marshalDeterministic(wireSpecificToolChoiceWrite{
				Type:    "tool_search",
				Name:    outName,
				Unknown: tcUnknown,
			})
			return jsontext.Value(raw), len(tcUnknown) > 0, err
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
		fnName := "tool_search"
		if name != "" {
			fnName, _ = sanitizeResponsesToolName(name, maxNameBytes)
		}
		raw, err := marshalDeterministic(wireSpecificToolChoiceWrite{
			Type:    "function",
			Name:    fnName,
			Unknown: tcUnknown,
		})
		return jsontext.Value(raw), len(tcUnknown) > 0, err

	case ir.ToolHosted:
		if !support.Has(ir.ToolSupportHostedWebSearch) {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
			if len(tcUnknown) > 0 {
				*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: "tool_choice"})
			}
			return jsontext.Value(`"auto"`), false, nil
		}
		raw, err := marshalDeterministic(wireSpecificToolChoiceWrite{
			Type:    name,
			Unknown: tcUnknown,
		})
		return jsontext.Value(raw), len(tcUnknown) > 0, err

	default:
		sanitized, changed := sanitizeResponsesToolName(name, maxNameBytes)
		if changed {
			approximated = true
		}
		if approximated {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
		}
		raw, err := marshalDeterministic(wireSpecificToolChoiceWrite{
			Type:      "function",
			Name:      sanitized,
			Namespace: ns,
			Unknown:   tcUnknown,
		})
		return jsontext.Value(raw), len(tcUnknown) > 0, err
	}
}

// encodeAllowedToolRef converts one allowed_tools ToolRef into a wireAllowedToolEntry.
func encodeAllowedToolRef(ref ir.ToolRef, support ir.ToolSupport, maxNameBytes int) (wireAllowedToolEntry, bool, bool) {
	ns := ref.Namespace
	name := ref.Name
	approx := false
	if ns != "" && !support.Has(ir.ToolSupportNamespace) {
		name = flattenNamespaceToolName(ns, name, maxNameBytes)
		ns = ""
		approx = true
	}
	switch ref.Kind {
	case ir.ToolCustom:
		sanitized, changed := sanitizeResponsesToolName(name, maxNameBytes)
		if changed {
			approx = true
		}
		if support.Has(ir.ToolSupportCustom) {
			return wireAllowedToolEntry{Type: "custom", Namespace: ns, Name: sanitized}, true, approx
		}
		return wireAllowedToolEntry{Type: "function", Namespace: ns, Name: sanitized}, true, true
	case ir.ToolClientSearch:
		if support.Has(ir.ToolSupportClientSearch) {
			return wireAllowedToolEntry{Type: "tool_search", Name: name}, true, false
		}
		fnName := "tool_search"
		if name != "" {
			fnName, _ = sanitizeResponsesToolName(name, maxNameBytes)
		}
		return wireAllowedToolEntry{Type: "function", Name: fnName}, true, true
	case ir.ToolHosted:
		if !support.Has(ir.ToolSupportHostedWebSearch) {
			return wireAllowedToolEntry{}, false, true
		}
		return wireAllowedToolEntry{Type: name}, true, false
	default:
		sanitized, changed := sanitizeResponsesToolName(name, maxNameBytes)
		if changed {
			approx = true
		}
		return wireAllowedToolEntry{Type: "function", Namespace: ns, Name: sanitized}, true, approx
	}
}

// encodeConversationFields serializes ConversationID and ConversationSet onto
// either top-level conversation (string or object) or flat conversation_id.
// It removes any "conversation" entry from topUnknown and reports whether sub-object extras survived.
func encodeConversationFields(
	convID string,
	convSet bool,
	topUnknown map[string]jsontext.Value,
) (jsontext.Value, string, bool, error) {
	rawExtra, hasConvExtra := topUnknown["conversation"]
	if hasConvExtra {
		delete(topUnknown, "conversation")
	}
	if !convSet {
		return nil, convID, false, nil
	}
	if hasConvExtra {
		if bytes.Equal(bytes.TrimSpace(rawExtra), []byte("{}")) && convID == "" {
			return jsontext.Value("{}"), "", true, nil
		}
		var unk map[string]jsontext.Value
		// Non-object conversation Extra tokens fall back to string serialization of
		// convID so synthetic Extra values cannot corrupt the conversation field shape.
		if unmarshalJSONObject(rawExtra, &unk) == nil && len(unk) > 0 {
			delete(unk, "id")
			encoded, err := marshalDeterministic(wireConversationObject{
				ID:      convID,
				Unknown: unk,
			})
			return jsontext.Value(encoded), "", len(unk) > 0, err
		}
	}
	encoded, err := marshalDeterministic(convID)
	return jsontext.Value(encoded), "", false, err
}

// mergeResponsesSubObjectExtras restores unmodeled fields decoded from reasoning,
// include, text, text.format, and metadata onto their outgoing wire structs and
// removes those keys from topUnknown so json:",embed" never collides with
// modeled top-level request fields.
//
// Explicit empty sub-objects ("{}" or "[]") materialize empty wire containers
// when no modeled sibling fields were populated, preserving zero-Loss Law Q2
// fixed points. It returns true when at least one sub-object retained same-origin
// extension data.
func mergeResponsesSubObjectExtras(
	topUnknown map[string]jsontext.Value,
	reasoning **wireReasoningConfig,
	includes *[]string,
	textCfg **wireTextConfig,
	textFormat *wireTextFormat,
	metadata **wireMetadata,
	losses *[]ir.Loss,
) bool {
	if len(topUnknown) == 0 {
		return false
	}
	emitted := false

	if rawR, hasR := topUnknown["reasoning"]; hasR {
		delete(topUnknown, "reasoning")
		if bytes.Equal(bytes.TrimSpace(rawR), []byte("{}")) {
			if *reasoning == nil {
				*reasoning = &wireReasoningConfig{}
				emitted = true
			}
		} else {
			var rUnk map[string]jsontext.Value
			// Non-object reasoning Extra values are skipped after deleting the key so
			// embedded Unknown cannot duplicate or overwrite the modeled reasoning object.
			if unmarshalJSONObject(rawR, &rUnk) == nil {
				delete(rUnk, "effort")
				delete(rUnk, "summary")
				if len(rUnk) > 0 {
					if *reasoning == nil {
						*reasoning = &wireReasoningConfig{}
					}
					(*reasoning).Unknown = rUnk
					emitted = true
				}
			}
		}
	}

	if rawInc, hasInc := topUnknown["include"]; hasInc {
		delete(topUnknown, "include")
		if bytes.Equal(bytes.TrimSpace(rawInc), []byte("[]")) {
			if len(*includes) == 0 {
				*includes = []string{}
				emitted = true
			}
		} else {
			var extraItems []string
			// Non-array or non-string include Extra values are ignored so modeled
			// reasoning.encrypted_content entries remain a valid string array.
			if unmarshalJSONValue(rawInc, &extraItems) == nil && len(extraItems) > 0 {
				combined := make([]string, 0, len(*includes)+len(extraItems))
				combined = append(combined, *includes...)
				for _, item := range extraItems {
					if !slices.Contains(combined, item) {
						combined = append(combined, item)
					}
				}
				*includes = combined
				emitted = true
			}
		}
	}

	if rawText, hasText := topUnknown["text"]; hasText {
		delete(topUnknown, "text")
		if bytes.Equal(bytes.TrimSpace(rawText), []byte("{}")) {
			if *textCfg == nil {
				*textCfg = &wireTextConfig{}
				emitted = true
			}
		} else {
			var tUnk map[string]jsontext.Value
			// Non-object text Extra values are skipped after key removal so embedded
			// Unknown cannot collide with the modeled text configuration object.
			if unmarshalJSONObject(rawText, &tUnk) == nil {
				delete(tUnk, "verbosity")
				if rawFmt, hasFmt := tUnk["format"]; hasFmt {
					delete(tUnk, "format")
					var fUnk map[string]jsontext.Value
					// Non-object text.format Extra values are ignored so modeled output
					// schema fields serialize without duplicate or malformed format keys.
					if unmarshalJSONObject(rawFmt, &fUnk) == nil {
						for _, k := range []string{"type", "name", "description", "schema", "strict"} {
							delete(fUnk, k)
						}
						if len(fUnk) > 0 {
							if textFormat != nil {
								textFormat.Unknown = fUnk
								emitted = true
							} else {
								*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: "output"})
							}
						}
					}
				}
				if len(tUnk) > 0 {
					if *textCfg == nil {
						*textCfg = &wireTextConfig{}
					}
					(*textCfg).Unknown = tUnk
					emitted = true
				}
			}
		}
	}

	if rawMeta, hasMeta := topUnknown["metadata"]; hasMeta {
		delete(topUnknown, "metadata")
		if bytes.Equal(bytes.TrimSpace(rawMeta), []byte("{}")) {
			if *metadata == nil {
				*metadata = &wireMetadata{}
				emitted = true
			}
		} else {
			var mUnk map[string]jsontext.Value
			// Non-object metadata Extra values are skipped after key removal so modeled
			// metadata.user_id serializes without conflicting top-level metadata keys.
			if unmarshalJSONObject(rawMeta, &mUnk) == nil {
				delete(mUnk, "user_id")
				if len(mUnk) > 0 {
					if *metadata == nil {
						*metadata = &wireMetadata{}
					}
					(*metadata).Unknown = mUnk
					emitted = true
				}
			}
		}
	}

	return emitted
}

// encodeResponsesSamplingAndStop populates temperature and top_p on out from req.
// Dialect-stripped temperature or top_p values and Responses-unsupported top_k,
// penalties, seed, candidate_count, and stop fields append ir.Loss entries.
func encodeResponsesSamplingAndStop(req *ir.Request, dialect ir.Dialect, out *wireOutRequest, losses *[]ir.Loss) {
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
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossSamplingDropped,
			Path:   "sampling.top_k",
			Detail: "top_k",
		})
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
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossStopDropped,
			Path: "stop",
		})
	}
}
