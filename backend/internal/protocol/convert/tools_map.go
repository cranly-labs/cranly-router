package convert

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"maps"
	"slices"
	"strings"
)

// tools_map.go defines the serializable ToolMap state and deterministic naming,
// ID-retyping, and custom-input wrapper helpers shared by IR tool lowering,
// response stream raising, and same-protocol Responses tool adaptation.
//
// Function-only upstreams accept flat ASCII function identifiers with JSON
// object parameters rather than scoped namespaces, freeform custom grammars, or
// client-executed tool_search declarations. Recording forward call aliases,
// reverse identity maps, and lowered Responses tool JSON bytes inside a
// JSON-serializable ToolMap lets single-turn Exchanges and multi-turn Responses
// WebSocket sessions lower declarations and restore upstream calls without
// losing client tool identity.

const (
	// defaultFunctionsNamespace names Codex's default function scope whose
	// children keep bare upstream identifiers and omit namespace on restoration.
	defaultFunctionsNamespace = "functions" // implicit Codex bare-function namespace

	// toolSearchProxyName and toolSearchProxyDescription define the synthetic
	// function declaration emitted when lowering client tool_search declarations.
	toolSearchProxyName        = "tool_search"                                                                                // reserved proxy function name for client search
	toolSearchProxyDescription = "Search and load Codex tools, plugins, connectors, and MCP namespaces for the current task." // proxy function description

	// customToolInputSchema and toolSearchProxySchema supply the JSON Schema
	// parameter objects attached to lowered custom and tool_search functions.
	customToolInputSchema = `{"type":"object","properties":{"input":{"type":"string","description":"The raw input for this tool, passed through verbatim."}},"required":["input"]}`
	toolSearchProxySchema = `{"type":"object","properties":{"query":{"type":"string","description":"Search query for tools or connectors to load."},"limit":{"type":"integer","description":"Maximum number of tool groups to return."}},"required":["query"]}`
)

// toolCallItemPrefixes lists the reversible Responses output-item ID prefixes
// across function_call, custom_tool_call, and tool_search_call items.
var toolCallItemPrefixes = []string{"fc_", "ctc_", "tsc_"}

// NamespacedName pairs an optional Responses tool namespace with its bare client tool name.
type NamespacedName struct {
	Namespace string `json:"namespace,omitzero"` // owning namespace, or empty for bare/default scope
	Name      string `json:"name"`               // bare client tool name
}

// ToolMap records lowered client tool identities, forward call aliases, and
// lowered Responses tool JSON declarations for response restoration and WS inheritance.
type ToolMap struct {
	CustomTools        map[string]bool                   `json:"custom_tools,omitzero"`        // upstream function names lowered from custom tools
	CustomNames        map[string]NamespacedName         `json:"custom_names,omitzero"`        // sanitized or flattened custom upstream name -> client identity
	ScopedCustom       map[NamespacedName]NamespacedName `json:"scoped_custom,omitzero"`       // (namespace, sanitized name) -> client identity when namespaces stay unflattened
	ToolSearch         bool                              `json:"tool_search,omitzero"`         // whether client tool_search proxy was enabled
	NamespaceTools     map[string]NamespacedName         `json:"namespace_tools,omitzero"`     // upstream flat name -> original client identity
	CallNames          map[NamespacedName]string         `json:"call_names,omitzero"`          // client (namespace, name) -> upstream function name
	FunctionTools      map[string]bool                   `json:"function_tools,omitzero"`      // declared bare function tool names
	DeclaredNamespaces map[string]bool                   `json:"declared_namespaces,omitzero"` // declared namespace group names
	LoweredTools       []jsontext.Value                  `json:"lowered_tools,omitzero"`       // lowered Responses tools[] JSON elements for WS turns
}

// customWrapperWrite serializes {"input": ...} deterministically for lowered custom calls.
type customWrapperWrite struct {
	Input string `json:"input"` // verbatim freeform custom tool input
}

// MarshalText encodes n as a two-element JSON string array [Namespace, Name] so
// map[NamespacedName]string fields inside ToolMap marshal deterministically.
// Any underlying JSON encoding failure returns a non-nil error.
func (n NamespacedName) MarshalText() ([]byte, error) {
	raw, err := json.Marshal([2]string{n.Namespace, n.Name})
	if err != nil {
		return nil, err
	}
	return raw, nil
}

// UnmarshalText decodes a two-element JSON string array produced by MarshalText
// back into n.Namespace and n.Name. Malformed JSON syntax or non-array text
// inputs return a descriptive unmarshaling error.
func (n *NamespacedName) UnmarshalText(text []byte) error {
	var pair [2]string
	if err := json.Unmarshal(text, &pair); err != nil {
		return fmt.Errorf("unmarshal NamespacedName %q: %w", string(text), err)
	}
	n.Namespace = pair[0]
	n.Name = pair[1]
	return nil
}

// IsEmpty reports whether m holds zero lowered tool identities, forward call
// aliases, declared namespaces, or serialized tool declarations. Nil receivers
// return true without dereferencing map fields.
func (m *ToolMap) IsEmpty() bool {
	return m == nil ||
		(len(m.CustomTools) == 0 &&
			len(m.CustomNames) == 0 &&
			len(m.ScopedCustom) == 0 &&
			!m.ToolSearch &&
			len(m.NamespaceTools) == 0 &&
			len(m.CallNames) == 0 &&
			len(m.FunctionTools) == 0 &&
			len(m.DeclaredNamespaces) == 0 &&
			len(m.LoweredTools) == 0)
}

// HasRestorations reports whether m carries any lowered custom, scoped custom,
// client search, or flattened namespace identity requiring response-side
// restoration. Nil receivers or declaration-only maps return false.
func (m *ToolMap) HasRestorations() bool {
	return m != nil && (len(m.CustomTools) > 0 || len(m.ScopedCustom) > 0 || m.ToolSearch || len(m.NamespaceTools) > 0)
}

// Clone returns an independent deep copy of m with detached maps and byte slices.
// Nil receivers return nil so absent tool maps remain nil across Exchange options.
func (m *ToolMap) Clone() *ToolMap {
	if m == nil {
		return nil
	}
	out := &ToolMap{
		ToolSearch: m.ToolSearch,
	}
	if len(m.CustomTools) > 0 {
		out.CustomTools = maps.Clone(m.CustomTools)
	}
	if len(m.CustomNames) > 0 {
		out.CustomNames = maps.Clone(m.CustomNames)
	}
	if len(m.ScopedCustom) > 0 {
		out.ScopedCustom = maps.Clone(m.ScopedCustom)
	}
	if len(m.NamespaceTools) > 0 {
		out.NamespaceTools = maps.Clone(m.NamespaceTools)
	}
	if len(m.CallNames) > 0 {
		out.CallNames = maps.Clone(m.CallNames)
	}
	if len(m.FunctionTools) > 0 {
		out.FunctionTools = maps.Clone(m.FunctionTools)
	}
	if len(m.DeclaredNamespaces) > 0 {
		out.DeclaredNamespaces = maps.Clone(m.DeclaredNamespaces)
	}
	if len(m.LoweredTools) > 0 {
		out.LoweredTools = make([]jsontext.Value, len(m.LoweredTools))
		for i, raw := range m.LoweredTools {
			out.LoweredTools[i] = slices.Clone(raw)
		}
	}
	return out
}

// IsCustomTool reports whether upstreamName was lowered from a client custom tool.
func (m *ToolMap) IsCustomTool(upstreamName string) bool {
	return m != nil && m.CustomTools[strings.TrimSpace(upstreamName)]
}

// LookupCustomTool returns the original client (Namespace, Name) identity for a
// lowered bare or flattened custom tool name, falling back to bare upstreamName
// when unsanitized. Nil maps or unrecognized upstream names return (NamespacedName{}, false).
func (m *ToolMap) LookupCustomTool(upstreamName string) (NamespacedName, bool) {
	trimmed := strings.TrimSpace(upstreamName)
	if m == nil || !m.CustomTools[trimmed] {
		return NamespacedName{}, false
	}
	if orig, ok := m.CustomNames[trimmed]; ok {
		return orig, true
	}
	return NamespacedName{Name: trimmed}, true
}

// LookupCustomCall resolves an upstream function call's (namespace, name) back
// to its lowered client custom tool identity across both unflattened namespace
// scopes and flattened bare upstream names. Non-default upstream namespaces only
// match entries declared in that scope so unrelated top-level custom tools never
// shadow scoped functions, while nil maps or unmapped names return false.
func (m *ToolMap) LookupCustomCall(namespace, upstreamName string) (NamespacedName, bool) {
	trimmedNS := strings.TrimSpace(namespace)
	trimmedName := strings.TrimSpace(upstreamName)
	if m == nil || trimmedName == "" {
		return NamespacedName{}, false
	}
	if trimmedNS != "" {
		if orig, ok := m.ScopedCustom[NamespacedName{Namespace: trimmedNS, Name: trimmedName}]; ok {
			return orig, true
		}
		if trimmedNS == defaultFunctionsNamespace {
			if orig, ok := m.ScopedCustom[NamespacedName{Namespace: "", Name: trimmedName}]; ok {
				return orig, true
			}
			return m.LookupCustomTool(trimmedName)
		}
		return NamespacedName{}, false
	}
	if orig, ok := m.ScopedCustom[NamespacedName{Namespace: "", Name: trimmedName}]; ok {
		return orig, true
	}
	return m.LookupCustomTool(trimmedName)
}

// IsToolSearchProxy reports whether upstreamName invokes the lowered tool_search proxy.
func (m *ToolMap) IsToolSearchProxy(upstreamName string) bool {
	if m == nil || !m.ToolSearch {
		return false
	}
	trimmed := strings.TrimSpace(upstreamName)
	return trimmed == toolSearchProxyName && !m.CustomTools[trimmed]
}

// IsToolSearchProxyCall reports whether a function call in namespace invokes the
// lowered client tool_search proxy. Explicit non-default namespaces return false
// so scoped functions named tool_search keep their namespace function identity.
func (m *ToolMap) IsToolSearchProxyCall(namespace, upstreamName string) bool {
	trimmedNS := strings.TrimSpace(namespace)
	if trimmedNS != "" && trimmedNS != defaultFunctionsNamespace {
		return false
	}
	return m.IsToolSearchProxy(upstreamName)
}

// LookupNamespaceTool returns the original (Namespace, Name) pair for a
// flattened function name when present in m.NamespaceTools. Nil maps or
// unmapped bare function names return (NamespacedName{}, false).
func (m *ToolMap) LookupNamespaceTool(upstreamName string) (NamespacedName, bool) {
	if m == nil || len(m.NamespaceTools) == 0 {
		return NamespacedName{}, false
	}
	pair, ok := m.NamespaceTools[strings.TrimSpace(upstreamName)]
	return pair, ok
}

// ResolveCallName resolves (namespace, name) to its upstream executable function
// name using declared or inherited mappings in m. Bare or default "functions"
// scope names fall back to name directly, whereas an explicit non-default
// namespace without declaration or inherited evidence returns an error.
func (m *ToolMap) ResolveCallName(namespace, name string) (string, error) {
	if m != nil {
		if upstream, ok := m.CallNames[NamespacedName{Namespace: namespace, Name: name}]; ok {
			return upstream, nil
		}
		for upstream, orig := range m.NamespaceTools {
			if orig.Namespace == namespace && orig.Name == name {
				return upstream, nil
			}
		}
	}
	if namespace == "" || namespace == defaultFunctionsNamespace {
		return name, nil
	}
	return "", fmt.Errorf("cannot convert tool call %q in namespace %q without declaration or inherited mapping", name, namespace)
}

// flattenNamespaceToolName joins namespace and name with "__" within a 64-byte
// function identifier cap (or a stricter caller maxBytes), keeping empty and
// default "functions" scopes bare and shortening oversized names at rune
// boundaries with an eight-hex SHA-256 suffix.
func flattenNamespaceToolName(namespace, name string, maxBytes int) string {
	if maxBytes <= 0 || maxBytes > defaultOpenAIToolNameMaxBytes {
		maxBytes = defaultOpenAIToolNameMaxBytes
	}
	if namespace == "" || namespace == defaultFunctionsNamespace {
		return name
	}
	full := namespace + "__" + name
	if len(full) <= maxBytes {
		return full
	}
	// Reserve ten bytes for "__" plus eight SHA-256 hex digits so shortened
	// names stay deterministic and rune-aligned within maxBytes.
	sum := sha256.Sum256([]byte(full))
	suffix := "__" + hex.EncodeToString(sum[:4])
	if maxBytes <= len(suffix) {
		return suffix[len(suffix)-maxBytes:]
	}
	prefixBudget := maxBytes - len(suffix)
	var b strings.Builder
	for _, r := range full {
		if b.Len()+len(string(r)) > prefixBudget {
			break
		}
		_, _ = b.WriteRune(r)
	}
	return b.String() + suffix
}

// sanitizeExecutableToolName replaces characters outside [a-zA-Z0-9_-] with '_'
// and bounds the resulting identifier within maxBytes using a SHA-256 suffix.
func sanitizeExecutableToolName(name string, maxBytes int) (string, bool) {
	return sanitizeTargetToolName(name, maxBytes, false)
}

// sanitizeTargetToolName normalizes name into a target-legal ASCII tool
// identifier bounded to maxBytes bytes using a deterministic SHA-256 suffix.
// When requireAlphaUnderscoreStart is true (such as Google Gemini upstreams),
// leading digits or hyphens receive a leading '_' so FunctionDeclaration names
// start with an ASCII letter or underscore and register inside ToolMap.
func sanitizeTargetToolName(name string, maxBytes int, requireAlphaUnderscoreStart bool) (string, bool) {
	if maxBytes <= 0 {
		maxBytes = defaultOpenAIToolNameMaxBytes
	}
	changed := false
	mapped := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		changed = true
		return '_'
	}, name)
	if mapped == "" {
		mapped = "tool"
		changed = true
	} else if requireAlphaUnderscoreStart {
		c0 := mapped[0]
		validStart := (c0 >= 'a' && c0 <= 'z') || (c0 >= 'A' && c0 <= 'Z') || c0 == '_'
		if !validStart {
			mapped = "_" + mapped
			changed = true
		}
	}
	if len(mapped) <= maxBytes {
		return mapped, changed
	}
	sum := sha256.Sum256([]byte(name))
	suffix := "__" + hex.EncodeToString(sum[:4])
	if maxBytes <= len(suffix) {
		return suffix[len(suffix)-maxBytes:], true
	}
	return mapped[:maxBytes-len(suffix)] + suffix, true
}

// customToolAlias returns the namespace-prefixed custom spelling reserved when
// allocating replay names for historical undeclared calls.
func customToolAlias(namespace, name string, maxBytes int) string {
	return flattenNamespaceToolName(namespace, name, maxBytes)
}

// retypeToolCallItemID swaps known fc_, ctc_, and tsc_ prefixes on id to match
// targetItemType while leaving empty or vendor-specific prefixes untouched.
func retypeToolCallItemID(id, targetItemType string) string {
	want := toolCallItemPrefix(targetItemType)
	if want == "" || id == "" || strings.HasPrefix(id, want) {
		return id
	}
	for _, known := range toolCallItemPrefixes {
		if known != want && strings.HasPrefix(id, known) {
			return want + strings.TrimPrefix(id, known)
		}
	}
	return id
}

// normalizeLoweredItemID converts ctc_/tsc_ prefixes back to fc_ and returns
// empty string for non-empty IDs lacking a compatible fc_ prefix.
func normalizeLoweredItemID(id string) (string, bool) {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" || strings.HasPrefix(trimmed, "fc_") {
		return id, false
	}
	if retyped := retypeToolCallItemID(trimmed, "function_call"); retyped != trimmed {
		return retyped, true
	}
	return "", true
}

// toolCallItemPrefix returns the canonical item ID prefix for itemType.
func toolCallItemPrefix(itemType string) string {
	switch itemType {
	case "custom_tool_call":
		return "ctc_"
	case "tool_search_call":
		return "tsc_"
	case "function_call":
		return "fc_"
	default:
		return ""
	}
}

// wrapCustomToolInput encodes rawInput into the {"input":"..."} function argument JSON object.
func wrapCustomToolInput(rawInput string) string {
	encoded, _ := json.Marshal(customWrapperWrite{Input: rawInput}, jsontext.AllowInvalidUTF8(true))
	return string(encoded)
}

// unwrapCustomToolInput extracts the string "input" property from a lowered
// custom tool JSON wrapper, returning "" for empty/null/empty-object inputs and
// returning trimmed source text when arguments is non-wrapper or malformed JSON.
func unwrapCustomToolInput(arguments string) string {
	trimmed := strings.TrimSpace(arguments)
	if trimmed == "" {
		return ""
	}
	members, err := parseOrderedObject([]byte(trimmed))
	if err != nil {
		return trimmed
	}
	if rawInput, ok := findRawMember(members, "input"); ok {
		if rawInput.Kind() == 'n' {
			return ""
		}
		if rawInput.Kind() == '"' {
			var s string
			if err := unmarshalPermissive(rawInput, &s); err == nil {
				return s
			}
		}
		return trimmed
	}
	if len(members) == 0 {
		return ""
	}
	return trimmed
}
