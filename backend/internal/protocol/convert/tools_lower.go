package convert

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// tools_lower.go lowers unsupported custom, namespace, and client tool_search
// declarations, historical tool calls, and tool_choice selectors on an
// ir.Request into function-compatible representations while sanitizing
// cross-protocol identifiers and constructing a reversible ToolMap.
//
// Tool lowering runs as a protocol-neutral IR pass before target request
// encoding: discovered declarations inside completed tool_search_output turns
// promote into the active tool list with conflict detection, scoped and
// cross-protocol names flatten and sanitize within Dialect.ToolNameMaxBytes
// (enforcing leading [a-zA-Z_] characters on Gemini targets), historical calls
// whose declarations disappeared receive deterministic collision-free replay
// names, custom tool inputs wrap inside {"input":"..."}, and every lowered
// declaration, call, or correlated result records an original-coordinate
// ir.Loss entry.

// discoveredToolCandidate pairs a comparison key and client identity dimensions
// with a promoted ir.Tool declaration extracted from tool_search_output history.
type discoveredToolCandidate struct {
	key     string  // admission lookup key (bare or flattened name)
	kind    string  // "function", "custom", or "namespace"
	ns      string  // declared namespace or empty string
	name    string  // bare tool name
	encoded string  // canonical shape fingerprint for equality checks
	tool    ir.Tool // normalized IR tool declaration to append when admitted
}

// existingToolSlot tracks static tool identity fingerprints for discovery conflict detection.
type existingToolSlot struct {
	kind      string // "function", "custom", or "namespace"
	ns        string // declared namespace or empty string
	name      string // bare tool name
	encoded   string // canonical shape fingerprint
	ambiguous bool   // true when static declarations collided at the same key
}

// wireDiscoveredTool decodes one tool declaration inside a historical tool_search_output JSON array.
type wireDiscoveredTool struct {
	Type         string                    `json:"type"`                   // "function", "custom", or "namespace"
	Name         string                    `json:"name,omitzero"`          // tool or namespace group name
	Description  string                    `json:"description,omitzero"`   // declared tool description text
	Parameters   jsontext.Value            `json:"parameters,omitzero"`    // function JSON Schema
	Strict       *bool                     `json:"strict,omitzero"`        // strict schema flag
	Format       jsontext.Value            `json:"format,omitzero"`        // custom tool grammar
	DeferLoading *bool                     `json:"defer_loading,omitzero"` // deferred loading flag
	Tools        []wireDiscoveredTool      `json:"tools,omitzero"`         // namespace child tools
	Children     []wireDiscoveredTool      `json:"children,omitzero"`      // alternate namespace child tools
	Unknown      map[string]jsontext.Value `json:",embed"`                 // preserved vendor extension fields
}

// lowerTools lowers custom, namespace, and client_search tools unsupported by
// route.Dialect and sanitizes cross-protocol function identifiers on req in
// place, updating coords and returning the resulting ToolMap alongside
// original-coordinate ir.Loss entries. Nil requests or same-protocol routes
// whose Dialect natively supports custom, namespace, and client_search tools
// return nil without mutating req.
//
// Discovered declarations inside historical tool_search_output items promote
// into req.Tools before identity allocation, scoped and cross-protocol names
// flatten and sanitize within Dialect.ToolNameMaxBytes with collision
// detection, orphaned defer_loading flags strip when no client_search tool
// survives, and inherited WebSocket mappings carry forward when a continuation
// turn omits tools declarations. Source-kind extension fields on lowered custom
// and search declarations clear with LossExtraDropped so same-origin encoders
// never emit stale attributes.
func lowerTools(
	req *ir.Request,
	coords *sourceCoords,
	route Route,
	inherited *ToolMap,
) (*ToolMap, []ir.Loss, error) {
	if req == nil {
		return nil, nil, nil
	}
	support := route.Dialect.Tools
	needCustom := !support.Has(ToolSupportCustom)
	needNamespace := !support.Has(ToolSupportNamespace)
	needSearch := !support.Has(ToolSupportClientSearch)
	needSanitize := route.Client != route.Upstream
	if !needCustom && !needNamespace && !needSearch && !needSanitize {
		return nil, nil, nil
	}

	maxBytes := route.Dialect.ToolNameMaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultOpenAIToolNameMaxBytes
	}
	alphaStart := route.Upstream == protocol.ProtocolGeminiGenerateContent

	ir.Link(req)

	var losses []ir.Loss
	if len(req.Tools) == 0 && len(req.Namespaces) == 0 {
		return lowerToollessHistory(req, coords, route, maxBytes, needCustom, needNamespace, needSearch, needSanitize, alphaStart, inherited, &losses)
	}

	var discoveredFunctions map[string]bool
	if needSearch && hasClientSearchTool(req.Tools) {
		var err error
		discoveredFunctions, err = promoteIRToolSearchDiscoveries(req, coords, maxBytes)
		if err != nil {
			return nil, nil, err
		}
	}

	tm, err := buildIRToolIdentityMap(
		req,
		maxBytes,
		needCustom,
		needNamespace,
		needSearch,
		needSanitize,
		alphaStart,
		discoveredFunctions,
		inherited,
	)
	if err != nil {
		return nil, nil, err
	}

	if err := lowerIRToolChoice(req, tm, maxBytes, needCustom, needNamespace, needSearch, needSanitize, alphaStart, &losses); err != nil {
		return nil, nil, err
	}

	if needNamespace && len(req.Namespaces) > 0 {
		req.Namespaces = nil
	}

	loweredSlot := make([]bool, len(req.Tools))
	keptTools := make([]ir.Tool, 0, len(req.Tools))
	var keptToolIdx []int
	seenSearch := false

	for i, t := range req.Tools {
		origIdx := coords.origToolIndex(i)
		tLowered := false
		origNS := t.Namespace
		origName := t.Name

		if needNamespace && t.Namespace != "" {
			flat := flattenNamespaceToolName(t.Namespace, t.Name, maxBytes)
			t.Name = flat
			t.Namespace = ""
			tLowered = true
		}

		switch t.Kind {
		case ir.ToolCustom:
			if needCustom && t.Name != "" {
				sanitized, _ := sanitizeTargetToolName(t.Name, maxBytes, alphaStart)
				if !needNamespace && origNS != "" {
					if tm.ScopedCustom == nil {
						tm.ScopedCustom = make(map[NamespacedName]NamespacedName)
					}
					tm.ScopedCustom[NamespacedName{Namespace: origNS, Name: sanitized}] = NamespacedName{
						Namespace: origNS,
						Name:      origName,
					}
				} else {
					tm.CustomTools[sanitized] = true
					canon := NamespacedName{Namespace: origNS, Name: origName}
					if canon.Namespace == defaultFunctionsNamespace {
						canon.Namespace = ""
					}
					if canon.Namespace != "" || canon.Name != sanitized {
						if tm.CustomNames == nil {
							tm.CustomNames = make(map[string]NamespacedName)
						}
						tm.CustomNames[sanitized] = canon
					}
				}
				hadExtra := !t.Extra.IsZero()
				t.Kind = ir.ToolFunction
				t.Name = sanitized
				t.InputSchema = jsontext.Value(customToolInputSchema)
				t.Format = nil
				t.Strict = nil
				t.Extra = ir.Extra{}
				tLowered = true
				if hadExtra && origIdx >= 0 {
					losses = append(losses, ir.Loss{
						Kind: ir.LossExtraDropped,
						Path: fmt.Sprintf("tools[%d]", origIdx),
					})
				}
			}

		case ir.ToolClientSearch:
			if needSearch {
				if seenSearch {
					if origIdx >= 0 {
						losses = append(losses, ir.Loss{
							Kind:   ir.LossOpaqueItemDropped,
							Path:   fmt.Sprintf("tools[%d]", origIdx),
							Detail: "duplicate_tool_search",
						})
					}
					continue
				}
				seenSearch = true
				hadExtra := !t.Extra.IsZero()
				t.Kind = ir.ToolFunction
				t.Name = toolSearchProxyName
				t.Description = toolSearchProxyDescription
				t.InputSchema = jsontext.Value(toolSearchProxySchema)
				t.Format = nil
				t.Extra = ir.Extra{}
				tLowered = true
				if hadExtra && origIdx >= 0 {
					losses = append(losses, ir.Loss{
						Kind: ir.LossExtraDropped,
						Path: fmt.Sprintf("tools[%d]", origIdx),
					})
				}
			}

		case ir.ToolFunction:
			if needSanitize {
				sanitized, changed := sanitizeTargetToolName(t.Name, maxBytes, alphaStart)
				if changed {
					canon := NamespacedName{Namespace: origNS, Name: origName}
					if canon.Namespace == defaultFunctionsNamespace {
						canon.Namespace = ""
					}
					tm.NamespaceTools[sanitized] = canon
					t.Name = sanitized
					tLowered = true
				}
			}
		}

		if tLowered && origIdx >= 0 {
			losses = append(losses, ir.Loss{
				Kind: ir.LossToolLowered,
				Path: fmt.Sprintf("tools[%d]", origIdx),
			})
		}
		loweredSlot = append(loweredSlot[:len(keptTools)], tLowered)
		keptTools = append(keptTools, t)
		keptToolIdx = append(keptToolIdx, origIdx)
	}

	// Strip defer_loading whenever no client_search declaration survives on a
	// lowered or cross-protocol request so upstreams do not reject or hide
	// orphaned deferred tools lacking a discovery mechanism.
	if (needSearch || needSanitize) && !hasClientSearchTool(keptTools) {
		for i := range keptTools {
			if keptTools[i].DeferLoading != nil {
				keptTools[i].DeferLoading = nil
				origIdx := keptToolIdx[i]
				if !loweredSlot[i] && origIdx >= 0 {
					losses = append(losses, ir.Loss{
						Kind: ir.LossToolLowered,
						Path: fmt.Sprintf("tools[%d]", origIdx),
					})
				}
			}
		}
	}

	req.Tools = keptTools
	if coords != nil {
		coords.toolIdx = keptToolIdx
	}

	if err := rewriteIRToolHistory(req, coords, route, tm, maxBytes, needCustom, needNamespace, needSearch, needSanitize, alphaStart, &losses); err != nil {
		return nil, nil, err
	}

	if len(tm.CustomTools) == 0 {
		tm.CustomTools = nil
	}
	if len(tm.CustomNames) == 0 {
		tm.CustomNames = nil
	}
	if len(tm.ScopedCustom) == 0 {
		tm.ScopedCustom = nil
	}
	if len(tm.NamespaceTools) == 0 {
		tm.NamespaceTools = nil
	}
	if !needCustom && !needNamespace && !needSearch && !tm.HasRestorations() && (inherited == nil || inherited.IsEmpty()) {
		return nil, losses, nil
	}
	if tm.IsEmpty() {
		return nil, losses, nil
	}
	return tm, losses, nil
}

// lowerToollessHistory rewrites req.ToolChoice and historical calls in
// req.Messages in place when the current turn declares no tools, appending
// original-coordinate entries via coords onto losses. Non-empty inherited
// session mappings are cloned before rewriting so WebSocket continuation turns
// resolve prior tool identities, while nil or empty inherited maps return a nil
// ToolMap alongside *losses; alphaStart enforces leading [a-zA-Z_] characters
// on Gemini targets.
func lowerToollessHistory(
	req *ir.Request,
	coords *sourceCoords,
	route Route,
	maxBytes int,
	needCustom, needNamespace, needSearch, needSanitize, alphaStart bool,
	inherited *ToolMap,
	losses *[]ir.Loss,
) (*ToolMap, []ir.Loss, error) {
	var tm *ToolMap
	if inherited != nil && !inherited.IsEmpty() {
		tm = inherited.Clone()
	} else {
		tm = &ToolMap{
			CustomTools:        make(map[string]bool),
			NamespaceTools:     make(map[string]NamespacedName),
			CallNames:          make(map[NamespacedName]string),
			FunctionTools:      make(map[string]bool),
			DeclaredNamespaces: make(map[string]bool),
		}
	}

	if err := lowerIRToolChoice(req, tm, maxBytes, needCustom, needNamespace, needSearch, needSanitize, alphaStart, losses); err != nil {
		return nil, nil, err
	}
	if err := rewriteIRToolHistory(req, coords, route, tm, maxBytes, needCustom, needNamespace, needSearch, needSanitize, alphaStart, losses); err != nil {
		return nil, nil, err
	}
	if inherited == nil || inherited.IsEmpty() {
		return nil, *losses, nil
	}
	return tm, *losses, nil
}

// hasClientSearchTool reports whether tools contains at least one ToolClientSearch declaration.
func hasClientSearchTool(tools []ir.Tool) bool {
	for _, t := range tools {
		if t.Kind == ir.ToolClientSearch {
			return true
		}
	}
	return false
}

// promoteIRToolSearchDiscoveries scans non-error tool_search_output turns in
// req.Messages, validates discovered tool declarations against static tools,
// and appends newly admitted tools onto req.Tools with coords.toolIdx = -1.
// Exact duplicate discoveries are deduplicated silently, whereas conflicting
// names, namespaces, or schemas return an error.
func promoteIRToolSearchDiscoveries(
	req *ir.Request,
	coords *sourceCoords,
	maxBytes int,
) (map[string]bool, error) {
	known := make(map[string]existingToolSlot, len(req.Tools))
	for _, t := range req.Tools {
		registerStaticIRToolSlot(known, t, maxBytes)
	}

	discoveredPlainFunctions := make(map[string]bool)
	for _, msg := range req.Messages {
		if msg.Role != ir.RoleTool {
			continue
		}
		for _, part := range msg.Parts {
			if part.Kind != ir.PartToolResult || part.Result == nil || part.Result.IsError {
				continue
			}
			if part.Result.Tool.Kind != ir.ToolClientSearch && part.Result.Tool.Kind != "" {
				continue
			}
			if len(part.Result.Content) != 1 || part.Result.Content[0].Kind != ir.PartText {
				continue
			}
			rawTxt := strings.TrimSpace(part.Result.Content[0].Text)
			if !strings.HasPrefix(rawTxt, "[") {
				continue
			}
			var rawTools []wireDiscoveredTool
			if err := unmarshalPermissive([]byte(rawTxt), &rawTools); err != nil {
				continue
			}
			for _, dt := range rawTools {
				cands := expandDiscoveredCandidates(dt, maxBytes)
				for _, cand := range cands {
					if cand.kind == "function" && cand.ns == "" && cand.name != "" {
						discoveredPlainFunctions[cand.name] = true
					}
					admit, err := admitDiscoveredSlot(known, cand)
					if err != nil {
						return nil, err
					}
					if admit {
						req.Tools = append(req.Tools, cand.tool)
						if coords != nil {
							coords.toolIdx = append(coords.toolIdx, -1)
						}
					}
				}
			}
		}
	}
	return discoveredPlainFunctions, nil
}

// registerStaticIRToolSlot records t's comparison fingerprint in known so
// subsequent tool_search_output discoveries can detect collisions within
// maxBytes. Empty names or hosted tools are ignored, while conflicting static
// declarations sharing the same key mark the slot ambiguous.
func registerStaticIRToolSlot(known map[string]existingToolSlot, t ir.Tool, maxBytes int) {
	var slot existingToolSlot
	var key string
	switch t.Kind {
	case ir.ToolFunction:
		if t.Name == "" {
			return
		}
		if t.Namespace != "" {
			key = flattenNamespaceToolName(t.Namespace, t.Name, maxBytes)
			slot = existingToolSlot{
				kind:    "namespace",
				ns:      t.Namespace,
				name:    t.Name,
				encoded: fingerprintIRToolShape(ir.ToolFunction, t.Name, t.Description, t.InputSchema, t.Strict),
			}
		} else {
			key = t.Name
			slot = existingToolSlot{
				kind:    "function",
				name:    t.Name,
				encoded: fingerprintIRToolShape(ir.ToolFunction, t.Name, t.Description, t.InputSchema, t.Strict),
			}
		}
	case ir.ToolCustom:
		if t.Name == "" {
			return
		}
		key = flattenNamespaceToolName(t.Namespace, t.Name, maxBytes)
		slot = existingToolSlot{
			kind:    "custom",
			ns:      t.Namespace,
			name:    t.Name,
			encoded: fingerprintIRToolShape(ir.ToolFunction, t.Name, t.Description, jsontext.Value(customToolInputSchema), nil),
		}
	default:
		return
	}

	prev, exists := known[key]
	if !exists {
		known[key] = slot
		return
	}
	if prev.kind != slot.kind || prev.ns != slot.ns || prev.name != slot.name || prev.encoded != slot.encoded {
		prev.ambiguous = true
		known[key] = prev
	}
}

// expandDiscoveredCandidates converts one wireDiscoveredTool into candidate IR
// tools and admission keys within maxBytes while cloning unknown JSON extension
// members onto promoted declarations. Empty names or unrecognized tool types
// return nil without failing discovery scanning.
func expandDiscoveredCandidates(dt wireDiscoveredTool, maxBytes int) []discoveredToolCandidate {
	typ := strings.TrimSpace(dt.Type)
	name := strings.TrimSpace(dt.Name)
	switch typ {
	case "function":
		if name == "" {
			return nil
		}
		t := ir.Tool{
			Kind:         ir.ToolFunction,
			Name:         name,
			Description:  dt.Description,
			InputSchema:  jsontext.Value(bytes.Clone(dt.Parameters)),
			Strict:       cloneBoolPtrLocal(dt.Strict),
			DeferLoading: cloneBoolPtrLocal(dt.DeferLoading),
			Extra:        cloneDiscoveredUnknownExtra(dt.Unknown),
		}
		return []discoveredToolCandidate{{
			key:     name,
			kind:    "function",
			name:    name,
			encoded: fingerprintIRToolShape(ir.ToolFunction, name, dt.Description, dt.Parameters, dt.Strict),
			tool:    t,
		}}

	case "custom":
		if name == "" {
			return nil
		}
		t := ir.Tool{
			Kind:         ir.ToolCustom,
			Name:         name,
			Description:  dt.Description,
			Format:       jsontext.Value(bytes.Clone(dt.Format)),
			DeferLoading: cloneBoolPtrLocal(dt.DeferLoading),
			Extra:        cloneDiscoveredUnknownExtra(dt.Unknown),
		}
		return []discoveredToolCandidate{{
			key:     name,
			kind:    "custom",
			name:    name,
			encoded: fingerprintIRToolShape(ir.ToolFunction, name, dt.Description, jsontext.Value(customToolInputSchema), nil),
			tool:    t,
		}}

	case "namespace":
		if name == "" {
			return nil
		}
		children := dt.Tools
		if len(children) == 0 {
			children = dt.Children
		}
		var out []discoveredToolCandidate
		for _, ch := range children {
			cType := strings.TrimSpace(ch.Type)
			cName := strings.TrimSpace(ch.Name)
			if cName == "" {
				continue
			}
			flat := flattenNamespaceToolName(name, cName, maxBytes)
			switch cType {
			case "function":
				t := ir.Tool{
					Kind:         ir.ToolFunction,
					Namespace:    name,
					Name:         cName,
					Description:  ch.Description,
					InputSchema:  jsontext.Value(bytes.Clone(ch.Parameters)),
					Strict:       cloneBoolPtrLocal(ch.Strict),
					DeferLoading: cloneBoolPtrLocal(ch.DeferLoading),
					Extra:        cloneDiscoveredUnknownExtra(ch.Unknown),
				}
				out = append(out, discoveredToolCandidate{
					key:     flat,
					kind:    "namespace",
					ns:      name,
					name:    cName,
					encoded: fingerprintIRToolShape(ir.ToolFunction, cName, ch.Description, ch.Parameters, ch.Strict),
					tool:    t,
				})
			case "custom":
				t := ir.Tool{
					Kind:         ir.ToolCustom,
					Namespace:    name,
					Name:         cName,
					Description:  ch.Description,
					Format:       jsontext.Value(bytes.Clone(ch.Format)),
					DeferLoading: cloneBoolPtrLocal(ch.DeferLoading),
					Extra:        cloneDiscoveredUnknownExtra(ch.Unknown),
				}
				out = append(out, discoveredToolCandidate{
					key:     flat,
					kind:    "custom",
					ns:      name,
					name:    cName,
					encoded: fingerprintIRToolShape(ir.ToolFunction, cName, ch.Description, jsontext.Value(customToolInputSchema), nil),
					tool:    t,
				})
			}
		}
		return out
	}
	return nil
}

// cloneDiscoveredUnknownExtra copies non-empty unknown JSON fields from a
// discovered Responses tool declaration into a detached ir.Extra value. Nil or
// empty maps return a zero ir.Extra without allocating a map.
func cloneDiscoveredUnknownExtra(unknown map[string]jsontext.Value) ir.Extra {
	if len(unknown) == 0 {
		return ir.Extra{}
	}
	copied := make(map[string]jsontext.Value, len(unknown))
	for k, v := range unknown {
		copied[k] = jsontext.Value(bytes.Clone(v))
	}
	return ir.Extra{
		Origin:  protocol.ProtocolOpenAIResponses,
		Members: copied,
	}
}

// admitDiscoveredSlot checks cand against known static and previously admitted
// discovery tools. Unseen keys record into known and return true, exact
// duplicates return false without re-appending, and conflicting shapes or
// namespaces return a descriptive error.
func admitDiscoveredSlot(known map[string]existingToolSlot, cand discoveredToolCandidate) (bool, error) {
	prev, exists := known[cand.key]
	if !exists {
		known[cand.key] = existingToolSlot{
			kind:    cand.kind,
			ns:      cand.ns,
			name:    cand.name,
			encoded: cand.encoded,
		}
		return true, nil
	}
	if !prev.ambiguous && prev.kind == cand.kind && prev.ns == cand.ns && prev.name == cand.name && prev.encoded == cand.encoded {
		return false, nil
	}
	return false, fmt.Errorf("discovered tool %q conflicts with an existing declaration; cannot disambiguate names, namespaces, or schemas", cand.key)
}

// fingerprintIRToolShape computes a deterministic string key over a declaration's normalized shape.
func fingerprintIRToolShape(kind ir.ToolKind, name, desc string, schema jsontext.Value, strict *bool) string {
	// fpWire captures normalized tool declaration fields serialized for equality fingerprinting.
	type fpWire struct {
		Kind   ir.ToolKind    `json:"kind"`            // normalized tool kind
		Name   string         `json:"name"`            // bare tool name
		Desc   string         `json:"desc,omitzero"`   // normalized tool description
		Schema jsontext.Value `json:"schema,omitzero"` // raw JSON Schema
		Strict *bool          `json:"strict,omitzero"` // strict schema flag
	}
	raw, _ := json.Marshal(fpWire{
		Kind:   kind,
		Name:   name,
		Desc:   desc,
		Schema: schema,
		Strict: strict,
	}, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
	return string(raw)
}

// buildIRToolIdentityMap validates declaration name uniqueness across
// req.Tools, req.Namespaces, and promoted discoveredFunctions within maxBytes,
// returning a bidirectional ToolMap for request lowering and response
// restoration. When alphaStart is true, sanitized names also prepend '_' ahead
// of leading digits or hyphens so Gemini FunctionDeclaration identifiers start
// with an ASCII letter or underscore.
//
// Scoped declarations skip flattening when needNamespace is false, whereas
// distinct client tools or built-in tool_search proxies that map onto the same
// upstream identifier return a collision error. Inherited namespace group names,
// default-scope function entries, and discovered function bindings from
// inherited carry forward across omitted-tools WS turns unless they conflict
// with an active namespace declaration.
func buildIRToolIdentityMap(
	req *ir.Request,
	maxBytes int,
	needCustom, needNamespace, needSearch, needSanitize, alphaStart bool,
	discoveredFunctions map[string]bool,
	inherited *ToolMap,
) (*ToolMap, error) {
	tm := &ToolMap{
		CustomTools:        make(map[string]bool),
		CustomNames:        make(map[string]NamespacedName),
		ScopedCustom:       make(map[NamespacedName]NamespacedName),
		NamespaceTools:     make(map[string]NamespacedName),
		CallNames:          make(map[NamespacedName]string),
		FunctionTools:      make(map[string]bool),
		DeclaredNamespaces: make(map[string]bool),
	}
	for _, ns := range req.Namespaces {
		if ns.Name != "" {
			tm.DeclaredNamespaces[ns.Name] = true
		}
	}

	owners := make(map[string]string)
	directFunctions := make(map[string]bool)

	for _, t := range req.Tools {
		switch t.Kind {
		case ir.ToolClientSearch:
			if needSearch {
				tm.ToolSearch = true
			}

		case ir.ToolFunction, ir.ToolCustom:
			if t.Name == "" {
				continue
			}
			if t.Namespace != "" {
				tm.DeclaredNamespaces[t.Namespace] = true
				if !needNamespace {
					upstream := t.Name
					if t.Kind == ir.ToolCustom && needCustom {
						upstream, _ = sanitizeTargetToolName(t.Name, maxBytes, alphaStart)
					} else if t.Kind == ir.ToolFunction && needSanitize {
						upstream, _ = sanitizeTargetToolName(t.Name, maxBytes, alphaStart)
					}
					kindTag := string(t.Kind)
					if t.Kind == ir.ToolCustom && needCustom {
						kindTag = "custom"
					}
					scopedOwnerKey := fmt.Sprintf("ns:%q:%q", t.Namespace, upstream)
					owner := fmt.Sprintf("%s:%q:%q", kindTag, t.Namespace, t.Name)
					if prev, exists := owners[scopedOwnerKey]; exists && prev != owner {
						return nil, fmt.Errorf("namespace tool %q/%q conflicts with scoped function name %q", t.Namespace, t.Name, upstream)
					}
					owners[scopedOwnerKey] = owner
					tm.CallNames[NamespacedName{Namespace: t.Namespace, Name: t.Name}] = upstream
					if t.Kind == ir.ToolCustom && needCustom {
						tm.ScopedCustom[NamespacedName{Namespace: t.Namespace, Name: upstream}] = NamespacedName{
							Namespace: t.Namespace,
							Name:      t.Name,
						}
					}
					continue
				}
				if t.Kind != ir.ToolFunction && !needCustom {
					continue
				}
				upstream := flattenNamespaceToolName(t.Namespace, t.Name, maxBytes)
				if t.Kind == ir.ToolFunction && needSanitize {
					upstream, _ = sanitizeTargetToolName(upstream, maxBytes, alphaStart)
				} else if t.Kind == ir.ToolCustom && needCustom {
					upstream, _ = sanitizeTargetToolName(upstream, maxBytes, alphaStart)
				}
				canon := NamespacedName{Namespace: t.Namespace, Name: t.Name}
				if canon.Namespace == defaultFunctionsNamespace {
					canon.Namespace = ""
				}
				kindTag := string(t.Kind)
				if t.Kind == ir.ToolCustom && needCustom {
					kindTag = "custom"
				}
				owner := fmt.Sprintf("%s:%q:%q", kindTag, canon.Namespace, canon.Name)
				if prev, exists := owners[upstream]; exists && prev != owner {
					return nil, fmt.Errorf("namespace tool %q/%q conflicts with upstream function name %q", t.Namespace, t.Name, upstream)
				}
				owners[upstream] = owner
				tm.CallNames[NamespacedName{Namespace: t.Namespace, Name: t.Name}] = upstream
				if t.Kind == ir.ToolFunction {
					tm.NamespaceTools[upstream] = canon
					if canon.Namespace == "" {
						tm.CallNames[canon] = upstream
						tm.FunctionTools[upstream] = true
					}
				} else if needCustom {
					tm.CustomTools[upstream] = true
					tm.CustomNames[upstream] = canon
					tm.CallNames[NamespacedName{Namespace: t.Namespace, Name: t.Name}] = upstream
				}
				continue
			}

			upstream := t.Name
			sanitizedFn := false
			if t.Kind == ir.ToolCustom && needCustom {
				upstream, _ = sanitizeTargetToolName(t.Name, maxBytes, alphaStart)
			} else if t.Kind == ir.ToolFunction && needSanitize {
				upstream, sanitizedFn = sanitizeTargetToolName(t.Name, maxBytes, alphaStart)
			}
			owner := fmt.Sprintf("%s:%q:%q", t.Kind, "", t.Name)
			if prev, exists := owners[upstream]; exists && prev != owner {
				return nil, fmt.Errorf("tool %q conflicts with an existing converted tool identity", t.Name)
			}
			owners[upstream] = owner
			tm.CallNames[NamespacedName{Name: t.Name}] = upstream
			tm.CallNames[NamespacedName{Namespace: defaultFunctionsNamespace, Name: t.Name}] = upstream
			if t.Kind == ir.ToolFunction {
				tm.FunctionTools[upstream] = true
				directFunctions[upstream] = true
				if sanitizedFn {
					tm.NamespaceTools[upstream] = NamespacedName{Name: t.Name}
				}
			} else if needCustom {
				tm.CustomTools[upstream] = true
				if upstream != t.Name {
					tm.CustomNames[upstream] = NamespacedName{Name: t.Name}
				}
			}
		}
	}

	if tm.ToolSearch {
		if _, collision := owners[toolSearchProxyName]; collision {
			return nil, fmt.Errorf("built-in tool_search conflicts with a declared tool named %q", toolSearchProxyName)
		}
	}

	for fnName := range discoveredFunctions {
		if !directFunctions[fnName] || fnName == defaultFunctionsNamespace {
			continue
		}
		ident := NamespacedName{Namespace: fnName, Name: fnName}
		if prev, exists := tm.CallNames[ident]; exists && prev != fnName {
			return nil, fmt.Errorf("discovered function %q conflicts with a namespace tool of the same identity", fnName)
		}
		tm.CallNames[ident] = fnName
		tm.NamespaceTools[fnName] = ident
	}

	if inherited != nil {
		maps.Copy(tm.DeclaredNamespaces, inherited.DeclaredNamespaces)
		for upstream, ident := range inherited.NamespaceTools {
			if inherited.FunctionTools[upstream] && tm.FunctionTools[upstream] &&
				ident.Name == upstream && (ident.Namespace == ident.Name || ident.Namespace == "") {
				if cur, exists := tm.CallNames[ident]; exists && cur != upstream {
					return nil, fmt.Errorf("inherited discovered function %q conflicts with current namespace tool", upstream)
				}
				tm.NamespaceTools[upstream] = ident
				tm.CallNames[ident] = upstream
			}
		}
	}
	return tm, nil
}

// lowerIRToolChoice rewrites req.ToolChoice in place when its target tool or
// AllowedTools subset references lowered custom, search, namespaced, or
// sanitized function tools, enforcing leading [a-zA-Z_] characters when
// alphaStart is true. Nil req.ToolChoice returns nil immediately, while any
// rewritten reference appends a single "tool_choice" LossToolChoiceApproximated
// entry onto losses or propagates unresolved namespace errors.
func lowerIRToolChoice(
	req *ir.Request,
	tm *ToolMap,
	maxBytes int,
	needCustom, needNamespace, needSearch, needSanitize, alphaStart bool,
	losses *[]ir.Loss,
) error {
	if req.ToolChoice == nil {
		return nil
	}
	tc := req.ToolChoice
	approx := false

	if len(tc.AllowedTools) > 0 {
		for i := range tc.AllowedTools {
			ref, changed, err := lowerSingleToolChoiceRef(tc.AllowedTools[i], tm, maxBytes, needCustom, needNamespace, needSearch, needSanitize, alphaStart)
			if err != nil {
				return err
			}
			if changed {
				tc.AllowedTools[i] = ref
				approx = true
			}
		}
	}

	if tc.Mode == ir.ToolChoiceTool {
		ref, changed, err := lowerSingleToolChoiceRef(tc.Tool, tm, maxBytes, needCustom, needNamespace, needSearch, needSanitize, alphaStart)
		if err != nil {
			return err
		}
		if changed {
			tc.Tool = ref
			approx = true
		}
	}

	if approx {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolChoiceApproximated,
			Path: "tool_choice",
		})
	}
	return nil
}

// lowerSingleToolChoiceRef maps ref to its lowered executable ir.ToolRef within
// maxBytes and reports whether the kind, namespace, or name changed. Scoped
// references under needNamespace require a matching tm entry and return an
// error when undeclared, while alphaStart enforces leading [a-zA-Z_]
// sanitization on custom and cross-protocol function names.
func lowerSingleToolChoiceRef(
	ref ir.ToolRef,
	tm *ToolMap,
	maxBytes int,
	needCustom, needNamespace, needSearch, needSanitize, alphaStart bool,
) (ir.ToolRef, bool, error) {
	changed := false
	if needNamespace && ref.Namespace != "" {
		resolved, err := tm.ResolveCallName(ref.Namespace, ref.Name)
		if err != nil {
			return ir.ToolRef{}, false, err
		}
		ref.Name = resolved
		ref.Namespace = ""
		changed = true
	}

	switch ref.Kind {
	case ir.ToolCustom:
		if needCustom {
			sanitized, _ := sanitizeTargetToolName(ref.Name, maxBytes, alphaStart)
			ref.Kind = ir.ToolFunction
			ref.Name = sanitized
			changed = true
		}
	case ir.ToolClientSearch:
		if needSearch {
			ref.Kind = ir.ToolFunction
			ref.Name = toolSearchProxyName
			ref.Namespace = ""
			changed = true
		}
	case ir.ToolFunction:
		if needSanitize && ref.Namespace == "" {
			if resolved, err := tm.ResolveCallName("", ref.Name); err == nil && resolved != ref.Name {
				ref.Name = resolved
				changed = true
			} else if sanitized, didChange := sanitizeTargetToolName(ref.Name, maxBytes, alphaStart); didChange {
				ref.Name = sanitized
				changed = true
			}
		}
	}
	return ref, changed, nil
}

// rewriteIRToolHistory rewrites historical custom, search, namespaced, and
// sanitized cross-protocol PartToolCall and correlated PartToolResult items
// across req.Messages in place, appending original-coordinate ir.Loss entries
// via coords onto losses. Disappeared namespace declarations receive
// deterministic replay names from allocateIRHistoryNames, custom inputs wrap
// inside {"input":...}, empty tool_search arguments normalize to "{}", and
// alphaStart enforces leading [a-zA-Z_] characters on Gemini function names.
//
// Lowered item IDs retype to fc_ or fco_ prefixes with LossIDRewritten only
// when targeting OpenAI Responses or Responses WebSocket upstreams. Correlated
// PartToolResult parts inherit lowering state by CallID until a later
// non-lowered call reuses that CallID, falling back to part.Result.Tool when
// CallID is unmatched.
func rewriteIRToolHistory(
	req *ir.Request,
	coords *sourceCoords,
	route Route,
	tm *ToolMap,
	maxBytes int,
	needCustom, needNamespace, needSearch, needSanitize, alphaStart bool,
	losses *[]ir.Loss,
) error {
	historyNames, err := allocateIRHistoryNames(req.Messages, tm, maxBytes, needNamespace, alphaStart)
	if err != nil {
		return err
	}

	loweredCallIDs := make(map[string]bool)
	retypeResponsesIDs := route.Upstream == protocol.ProtocolOpenAIResponses ||
		route.Upstream == protocol.ProtocolResponsesWebSocket

	for m := range req.Messages {
		for p := range req.Messages[m].Parts {
			part := &req.Messages[m].Parts[p]
			origPath := coords.origPartPath(m, p)

			switch part.Kind {
			case ir.PartToolCall:
				if part.Call == nil {
					continue
				}
				callLowered := false
				switch part.Call.Tool.Kind {
				case ir.ToolCustom:
					if needCustom {
						name := part.Call.Tool.Name
						keptNS := ""
						if needNamespace && part.Call.Tool.Namespace != "" {
							name = flattenNamespaceToolName(part.Call.Tool.Namespace, name, maxBytes)
						} else if !needNamespace {
							keptNS = part.Call.Tool.Namespace
						}
						sanitized, _ := sanitizeTargetToolName(name, maxBytes, alphaStart)
						part.Call.Arguments = wrapCustomToolInput(part.Call.Arguments)
						part.Call.Tool = ir.ToolRef{Kind: ir.ToolFunction, Namespace: keptNS, Name: sanitized}
						if retypeResponsesIDs {
							retypedID, idChanged := normalizeLoweredItemID(part.ID)
							if idChanged {
								part.ID = retypedID
								*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: origPath + ".id"})
							}
						}
						callLowered = true
					} else if needNamespace && part.Call.Tool.Namespace != "" {
						flat := flattenNamespaceToolName(part.Call.Tool.Namespace, part.Call.Tool.Name, maxBytes)
						part.Call.Tool.Name = flat
						part.Call.Tool.Namespace = ""
						callLowered = true
					}

				case ir.ToolClientSearch:
					if needSearch {
						if strings.TrimSpace(part.Call.Arguments) == "" {
							part.Call.Arguments = "{}"
						}
						part.Call.Tool = ir.ToolRef{Kind: ir.ToolFunction, Name: toolSearchProxyName}
						if retypeResponsesIDs {
							retypedID, idChanged := normalizeLoweredItemID(part.ID)
							if idChanged {
								part.ID = retypedID
								*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: origPath + ".id"})
							}
						}
						callLowered = true
					}

				case ir.ToolFunction:
					if needNamespace && part.Call.Tool.Namespace != "" {
						origNS := part.Call.Tool.Namespace
						origName := part.Call.Tool.Name
						resolved, ok := historyNames[NamespacedName{Namespace: origNS, Name: origName}]
						if !ok {
							if origNS == defaultFunctionsNamespace {
								resolved = origName
							} else {
								return fmt.Errorf("cannot convert historical tool call %q in namespace %q", origName, origNS)
							}
						}
						if needSanitize {
							resolved, _ = sanitizeTargetToolName(resolved, maxBytes, alphaStart)
						}
						part.Call.Tool.Name = resolved
						part.Call.Tool.Namespace = ""
						callLowered = true
					} else if needSanitize {
						if resolved, resolveErr := tm.ResolveCallName("", part.Call.Tool.Name); resolveErr == nil && resolved != part.Call.Tool.Name {
							part.Call.Tool.Name = resolved
							callLowered = true
						} else if sanitized, changed := sanitizeTargetToolName(part.Call.Tool.Name, maxBytes, alphaStart); changed {
							part.Call.Tool.Name = sanitized
							callLowered = true
						}
					}
				}

				if callLowered {
					if part.Call.CallID != "" {
						loweredCallIDs[part.Call.CallID] = true
					}
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossToolLowered,
						Path: origPath,
					})
				} else if part.Call.CallID != "" {
					delete(loweredCallIDs, part.Call.CallID)
				}

			case ir.PartToolResult:
				if part.Result == nil {
					continue
				}
				resLowered := loweredCallIDs[part.Result.CallID]
				if !resLowered {
					switch part.Result.Tool.Kind {
					case ir.ToolCustom:
						resLowered = needCustom || (needNamespace && part.Result.Tool.Namespace != "")
					case ir.ToolClientSearch:
						resLowered = needSearch
					case ir.ToolFunction:
						if needNamespace && part.Result.Tool.Namespace != "" {
							resLowered = true
						} else if needSanitize && part.Result.Tool.Name != "" {
							_, resLowered = sanitizeTargetToolName(part.Result.Tool.Name, maxBytes, alphaStart)
						}
					}
				}
				if resLowered {
					if retypeResponsesIDs {
						retypedID, idChanged := normalizeLoweredItemID(part.ID)
						if idChanged {
							part.ID = retypedID
							*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: origPath + ".id"})
						}
					}
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossToolLowered,
						Path: origPath,
					})
				}
			}
		}
	}
	return nil
}

// allocateIRHistoryNames assigns deterministic, collision-free upstream
// function names to historical scoped ToolFunction calls in msgs whose
// declarations disappeared from req.Tools. When needNamespace is false or no
// unresolved scoped calls exist, it returns immediately without allocating
// synthetic digest suffixes.
//
// Active declarations in tm, built-in tool_search, historical custom tool
// names, and namespace-prefixed custom aliases reserve their upstream
// identifiers first. Unresolved (Namespace, Name) pairs sort lexically,
// sanitize via alphaStart within maxBytes, and append SHA-256 hex suffixes on
// collision, returning an error if every candidate slot remains occupied.
func allocateIRHistoryNames(
	msgs []ir.Message,
	tm *ToolMap,
	maxBytes int,
	needNamespace, alphaStart bool,
) (map[NamespacedName]string, error) {
	names := make(map[NamespacedName]string)
	if !needNamespace {
		return names, nil
	}

	reserved := map[string]bool{toolSearchProxyName: true}
	if tm != nil {
		for _, up := range tm.CallNames {
			reserved[up] = true
		}
		for up := range tm.NamespaceTools {
			reserved[up] = true
		}
		for up := range tm.FunctionTools {
			reserved[up] = true
		}
		for up := range tm.CustomTools {
			reserved[up] = true
		}
	}

	unresolved := make(map[NamespacedName]bool)
	for _, msg := range msgs {
		for _, part := range msg.Parts {
			if part.Kind != ir.PartToolCall || part.Call == nil {
				continue
			}
			if part.Call.Tool.Kind == ir.ToolCustom && part.Call.Tool.Name != "" {
				reserved[part.Call.Tool.Name] = true
				targetName := part.Call.Tool.Name
				if needNamespace && part.Call.Tool.Namespace != "" {
					targetName = flattenNamespaceToolName(part.Call.Tool.Namespace, targetName, maxBytes)
				}
				sanitized, _ := sanitizeTargetToolName(targetName, maxBytes, alphaStart)
				reserved[sanitized] = true
				continue
			}
			if part.Call.Tool.Kind != ir.ToolFunction || part.Call.Tool.Namespace == "" {
				continue
			}
			ident := NamespacedName{Namespace: part.Call.Tool.Namespace, Name: part.Call.Tool.Name}
			if upstream, err := tm.ResolveCallName(ident.Namespace, ident.Name); err == nil {
				names[ident] = upstream
				reserved[upstream] = true
			} else if strings.TrimSpace(ident.Name) != "" {
				unresolved[ident] = true
			}
		}
	}
	if len(unresolved) == 0 {
		return names, nil
	}

	if tm != nil {
		for customName := range tm.CustomTools {
			for _, nsIdent := range tm.NamespaceTools {
				reserved[customToolAlias(nsIdent.Namespace, customName, maxBytes)] = true
			}
		}
	}

	sorted := make([]NamespacedName, 0, len(unresolved))
	for ident := range unresolved {
		sorted = append(sorted, ident)
	}
	slices.SortFunc(sorted, func(a, b NamespacedName) int {
		if c := strings.Compare(a.Namespace, b.Namespace); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})

	for _, ident := range sorted {
		base, _ := sanitizeTargetToolName(flattenNamespaceToolName(ident.Namespace, ident.Name, maxBytes), maxBytes, alphaStart)
		candidate := base
		limit := len(reserved) + 1
		for attempt := 0; reserved[candidate] && attempt < limit; attempt++ {
			// Append a sixteen-character SHA-256 component digest when two historical
			// namespaces flatten onto the same base identifier so replay calls stay distinct.
			digest := sha256.Sum256(fmt.Appendf(nil, "%q/%q/%d", ident.Namespace, ident.Name, attempt))
			suffix := "__" + hex.EncodeToString(digest[:8])
			if maxBytes <= len(suffix) {
				candidate = suffix[len(suffix)-maxBytes:]
			} else {
				candidate = base[:min(len(base), maxBytes-len(suffix))] + suffix
			}
		}
		if reserved[candidate] {
			return nil, fmt.Errorf("cannot allocate distinct historical tool name for %q/%q", ident.Namespace, ident.Name)
		}
		names[ident] = candidate
		reserved[candidate] = true
	}
	return names, nil
}

// cloneBoolPtrLocal allocates an independent boolean copy of *p on the heap so
// lowered IR tool declarations never alias caller-owned boolean pointers. Nil
// pointer inputs return nil without allocating storage.
func cloneBoolPtrLocal(p *bool) *bool {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
