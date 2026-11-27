package openai

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// responses_namespace.go flattens namespaced Responses tool declarations,
// removes disallowed namespace keys from direct history items, and restores
// original namespace identities on response calls.
// Responses namespaces identify tools with both a group name and a child tool
// name, whereas flattened native forwarding and Compact routes require single
// function names and stripped history items. Default "functions" declarations
// use bare tool names and omit namespace on restoration, while other explicit
// namespaces join with double underscores or shorten oversized names with a
// deterministic SHA-256 suffix.
//
// Flattening checks name collisions against top-level tools and sibling
// namespaces before mutating request declarations, input calls, or tool_choice.
// Direct input-array namespace stripping patches only selected raw item objects
// so untouched items retain exact 64-bit integer literals, whereas JSON call
// restoration decodes and re-encodes matched payloads with HTML escaping
// disabled.

const (
	// responsesDefaultToolNamespace and namespaceToolNameMaxLen govern bare
	// default-scope naming and the 64-byte flattened function-name ceiling.
	responsesDefaultToolNamespace = "functions" // Codex default function scope omitted on restoration
	namespaceToolNameMaxLen       = 64          // maximum byte length for flattened namespace tool names
)

// openAIResponsesToolCallItemTypes lists the four lowercase call-item types
// that retain namespace fields when StripOpenAIResponsesInputNamespaces runs
// with keepToolCallNamespaces enabled.
var openAIResponsesToolCallItemTypes = map[string]bool{
	"function_call":    true,
	"tool_call":        true,
	"custom_tool_call": true,
	"mcp_tool_call":    true,
}

// NamespacedToolName identifies a Responses child tool by its original
// namespace and unqualified tool name.
type NamespacedToolName struct {
	// Namespace and Name preserve the client's original two-part tool identity
	// independently of its flattened upstream name so restoration never splits
	// underscores or hash suffixes heuristically.
	Namespace string // original declared tool namespace, empty for default "functions" scope
	Name      string // original unqualified child tool name
}

// ResponsesNamespaceName aliases NamespacedToolName for Responses namespace
// flattening and call restoration maps.
type ResponsesNamespaceName = NamespacedToolName

// FlattenResponsesNamespaces flattens all namespace function declarations in
// req without preserving any exempt namespace groups. The request map and its
// nested history maps may be mutated in place; callers retain the returned map
// to restore response function calls and treat conflicts as 400-class errors.
func FlattenResponsesNamespaces(req map[string]any) (map[string]ResponsesNamespaceName, bool, error) {
	return FlattenResponsesNamespacesExcept(req, nil)
}

// FlattenResponsesNamespacesExcept replaces namespace function declarations in
// req with flat top-level function declarations except for namespace names
// enabled in preserved. Nil requests, absent tools, or requests without
// eligible namespace functions return nil, false, and nil without mutation.
//
// Every candidate flat name is checked for collisions against top-level
// function or custom declarations and other flattened namespace children before
// req is modified. Emitted child tool maps are shallow copies, while matching
// input function_call items and tool_choice maps are rewritten in place, and a
// flattened namespace-level tool_choice falls back to "auto".
func FlattenResponsesNamespacesExcept(req map[string]any, preserved map[string]bool) (map[string]ResponsesNamespaceName, bool, error) {
	if req == nil {
		return nil, false, nil
	}
	tools, ok := req["tools"].([]any)
	if !ok || len(tools) == 0 {
		return nil, false, nil
	}

	topLevel := make(map[string]bool)
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		typ := strings.TrimSpace(stringValue(tool["type"]))
		name := strings.TrimSpace(stringValue(tool["name"]))
		if (typ == "function" || typ == "custom") && name != "" {
			topLevel[name] = true
		}
	}

	names := make(map[string]ResponsesNamespaceName)
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if !ok || strings.TrimSpace(stringValue(tool["type"])) != "namespace" {
			continue
		}
		namespace := strings.TrimSpace(stringValue(tool["name"]))
		if namespace == "" || preserved[namespace] {
			continue
		}
		for _, rawChild := range namespaceChildren(tool) {
			child, ok := rawChild.(map[string]any)
			if !ok || strings.TrimSpace(stringValue(child["type"])) != "function" {
				continue
			}
			name := strings.TrimSpace(stringValue(child["name"]))
			if name == "" {
				continue
			}
			flat := flattenNamespaceToolName(namespace, name)
			entry := ResponsesNamespaceName{Namespace: namespace, Name: name}
			if entry.Namespace == responsesDefaultToolNamespace {
				entry.Namespace = ""
			}
			// Reject collisions before mutating declarations or history so every
			// flattened upstream function name maps back to a unique client tool.
			if topLevel[flat] {
				return nil, false, fmt.Errorf("namespace tool %q/%q flattens to %q which conflicts with a top-level tool of the same name; this upstream cannot disambiguate them, rename one of the tools", namespace, name, flat)
			}
			if prev, exists := names[flat]; exists && prev != entry {
				return nil, false, fmt.Errorf("namespace tools %q/%q and %q/%q both flatten to %q; this upstream cannot disambiguate them, rename one of the tools", prev.Namespace, prev.Name, namespace, name, flat)
			}
			names[flat] = entry
		}
	}
	if len(names) == 0 {
		return nil, false, nil
	}

	flattened := make([]any, 0, len(tools)+len(names))
	seen := make(map[string]bool)
	for _, raw := range tools {
		tool, ok := raw.(map[string]any)
		if !ok || strings.TrimSpace(stringValue(tool["type"])) != "namespace" {
			flattened = append(flattened, raw)
			continue
		}
		namespace := strings.TrimSpace(stringValue(tool["name"]))
		if preserved[namespace] {
			flattened = append(flattened, raw)
			continue
		}
		for _, rawChild := range namespaceChildren(tool) {
			child, ok := rawChild.(map[string]any)
			if !ok || strings.TrimSpace(stringValue(child["type"])) != "function" {
				continue
			}
			name := strings.TrimSpace(stringValue(child["name"]))
			flat := flattenNamespaceToolName(namespace, name)
			if name == "" || seen[flat] {
				continue
			}
			seen[flat] = true
			// Clone each child declaration map at the top level before replacing its
			// name so caller child maps keep their original tool names while sharing
			// nested parameter schemas.
			flatChild := make(map[string]any, len(child))
			maps.Copy(flatChild, child)
			flatChild["name"] = flat
			flattened = append(flattened, flatChild)
		}
	}
	req["tools"] = flattened
	rewriteNamespaceQualifiedCalls(req["input"], names)
	if choice, ok := req["tool_choice"].(map[string]any); ok {
		choiceNamespace := strings.TrimSpace(stringValue(choice["name"]))
		if strings.TrimSpace(stringValue(choice["type"])) == "namespace" && !preserved[choiceNamespace] {
			// Replace a flattened group selector with "auto" because flat function
			// declarations can no longer be targeted as a single namespace group.
			req["tool_choice"] = "auto"
		} else {
			rewriteNamespaceQualifiedCall(choice, names)
		}
	}
	return names, true, nil
}

// RestoreResponsesNamespaceCalls rewrites flattened function_call names across
// payload back to their original child Name and Namespace fields. Default-scope
// entries omit the namespace field; empty payloads, empty mappings, or payloads
// with no matching function_call items return payload unchanged with false.
//
// Changed payloads are re-encoded with HTML escaping disabled. Because generic
// encoding/json decoding uses float64 numbers, numeric literals in rewritten
// payloads follow standard JSON float64 formatting; decode or encode errors
// return payload alongside false and the error.
func RestoreResponsesNamespaceCalls(payload []byte, names map[string]ResponsesNamespaceName) ([]byte, bool, error) {
	if len(payload) == 0 || len(names) == 0 {
		return payload, false, nil
	}
	var value any
	if err := json.Unmarshal(payload, &value); err != nil {
		return payload, false, err
	}
	changed := restoreResponsesNamespaceValue(value, names)
	if !changed {
		return payload, false, nil
	}
	var rebuilt bytes.Buffer
	encoder := json.NewEncoder(&rebuilt)
	// Disable HTML escaping so literal angle brackets and ampersands in message
	// or argument strings are not rewritten when restoring call identities.
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return payload, false, err
	}
	return bytes.TrimSuffix(rebuilt.Bytes(), []byte("\n")), true, nil
}

// StripOpenAIResponsesInputNamespaces deletes direct "namespace" fields from
// object elements of the top-level input array in body. When
// keepToolCallNamespaces is true, recognized tool-call item types keep their
// namespace keys while messages, outputs, reasoning, and other items are
// cleaned; tool declarations and nested content fields stay untouched.
//
// Only items carrying a removable namespace key are patched via sjson, and the
// input array is reassembled from raw item slices so untouched objects keep
// their exact numeric precision and key spelling. Unchanged bodies and patch
// failures return body, with patch failures also returning a descriptive error.
func StripOpenAIResponsesInputNamespaces(body []byte, keepToolCallNamespaces bool) ([]byte, error) {
	input := gjson.GetBytes(body, "input")
	if !input.IsArray() {
		return body, nil
	}
	needsCleanup := false
	input.ForEach(func(_, item gjson.Result) bool {
		needsCleanup = item.IsObject() && item.Get("namespace").Exists() &&
			(!keepToolCallNamespaces || !IsOpenAIResponsesToolCallItemType(item.Get("type").String()))
		return !needsCleanup
	})
	if !needsCleanup {
		return body, nil
	}

	var rebuilt bytes.Buffer
	rebuilt.Grow(len(input.Raw))
	// Rebuild array delimiters once around raw item slices so unmodified items
	// keep exact JSON bytes without full-document sjson rewrites per element.
	_ = rebuilt.WriteByte('[')
	changed := false
	first := true
	var stripErr error
	input.ForEach(func(_, item gjson.Result) bool {
		if !first {
			_ = rebuilt.WriteByte(',')
		}
		first = false
		itemBody := []byte(item.Raw)
		if item.IsObject() && item.Get("namespace").Exists() &&
			(!keepToolCallNamespaces || !IsOpenAIResponsesToolCallItemType(item.Get("type").String())) {
			// Delete only the item's direct namespace member so nested content or
			// retained tool-call namespaces remain intact.
			itemBody, stripErr = sjson.DeleteBytes(itemBody, "namespace")
			if stripErr != nil {
				return false
			}
			changed = true
		}
		_, _ = rebuilt.Write(itemBody)
		return true
	})
	_ = rebuilt.WriteByte(']')
	if stripErr != nil {
		return body, fmt.Errorf("delete OpenAI input namespace: %w", stripErr)
	}
	if !changed {
		return body, nil
	}
	stripped, err := sjson.SetRawBytes(body, "input", rebuilt.Bytes())
	if err != nil {
		return body, fmt.Errorf("replace OpenAI input after namespace deletion: %w", err)
	}
	return stripped, nil
}

// IsOpenAIResponsesToolCallItemType reports whether itemType names one of the
// four namespace-capable Responses call item types after trimming surrounding
// whitespace and folding ASCII case. Blank strings and non-call item types
// return false.
func IsOpenAIResponsesToolCallItemType(itemType string) bool {
	return openAIResponsesToolCallItemTypes[strings.ToLower(strings.TrimSpace(itemType))]
}

// namespaceChildren returns a non-empty "tools" slice from tool when present,
// falling back to the legacy "children" slice alias when "tools" is absent or
// empty. Returned slices share backing storage with tool, and missing or
// non-slice fields return nil.
func namespaceChildren(tool map[string]any) []any {
	if children, ok := tool["tools"].([]any); ok && len(children) > 0 {
		return children
	}
	children, _ := tool["children"].([]any)
	return children
}

// rewriteNamespaceQualifiedCalls recursively visits decoded slices and maps,
// rewriting matching function_call objects in place against names. Callers
// pass acyclic JSON-decoded values and the mapping produced by flattening.
func rewriteNamespaceQualifiedCalls(value any, names map[string]ResponsesNamespaceName) {
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			rewriteNamespaceQualifiedCalls(item, names)
		}
	case map[string]any:
		if strings.TrimSpace(stringValue(typed["type"])) == "function_call" {
			rewriteNamespaceQualifiedCall(typed, names)
		}
		for _, child := range typed {
			rewriteNamespaceQualifiedCalls(child, names)
		}
	}
}

// rewriteNamespaceQualifiedCall replaces a matched (namespace, name) pair on
// item with its flattened function name and deletes the "namespace" key. Blank
// components or pairs missing from names leave item untouched and return false.
func rewriteNamespaceQualifiedCall(item map[string]any, names map[string]ResponsesNamespaceName) bool {
	namespace := strings.TrimSpace(stringValue(item["namespace"]))
	name := strings.TrimSpace(stringValue(item["name"]))
	if namespace == "" || name == "" {
		return false
	}
	flat := flattenNamespaceToolName(namespace, name)
	entry, ok := names[flat]
	canonicalNamespace := namespace
	if canonicalNamespace == responsesDefaultToolNamespace {
		canonicalNamespace = ""
	}
	if !ok || entry.Namespace != canonicalNamespace || entry.Name != name {
		return false
	}
	item["name"] = flat
	delete(item, "namespace")
	return true
}

// flattenNamespaceToolName returns name unchanged for empty or default
// "functions" namespaces and otherwise joins namespace and name with "__"
// within the 64-byte limit. Joined names exceeding 64 bytes keep a UTF-8
// rune-aligned prefix followed by "__" and 8 hex digits of SHA-256(full).
func flattenNamespaceToolName(namespace, name string) string {
	if namespace == "" || namespace == responsesDefaultToolNamespace {
		// Keep default "functions" scope declarations and historical calls on bare
		// names so restored responses omit redundant default namespaces.
		return name
	}
	full := namespace + "__" + name
	if len(full) <= namespaceToolNameMaxLen {
		return full
	}
	// Reserve 10 bytes for "__" plus 8 hex digits of SHA-256(full) and clip the
	// prefix on a rune boundary so multi-byte UTF-8 sequences stay valid.
	sum := sha256.Sum256([]byte(full))
	suffix := "__" + hex.EncodeToString(sum[:4])
	prefixLen := namespaceToolNameMaxLen - len(suffix)
	var prefix strings.Builder
	for _, ch := range full {
		if prefix.Len()+len(string(ch)) > prefixLen {
			break
		}
		_, _ = prefix.WriteRune(ch)
	}
	return prefix.String() + suffix
}

// restoreResponsesNamespaceValue recursively mutates decoded JSON maps and
// slices in place to restore mapped function_call names and namespaces.
// Default-scope entries delete any existing "namespace" key rather than writing
// an empty string, and the returned bool reports whether any call map changed.
func restoreResponsesNamespaceValue(value any, names map[string]ResponsesNamespaceName) bool {
	changed := false
	switch typed := value.(type) {
	case []any:
		for _, item := range typed {
			changed = restoreResponsesNamespaceValue(item, names) || changed
		}
	case map[string]any:
		if strings.TrimSpace(stringValue(typed["type"])) == "function_call" {
			if entry, ok := names[strings.TrimSpace(stringValue(typed["name"]))]; ok {
				if typed["name"] != entry.Name {
					typed["name"] = entry.Name
					changed = true
				}
				if entry.Namespace == "" {
					if _, present := typed["namespace"]; present {
						delete(typed, "namespace")
						changed = true
					}
				} else if typed["namespace"] != entry.Namespace {
					typed["namespace"] = entry.Namespace
					changed = true
				}
			}
		}
		for _, child := range typed {
			changed = restoreResponsesNamespaceValue(child, names) || changed
		}
	}
	return changed
}

// stringValue extracts a string from value when value holds a dynamic Go
// string. Nil interfaces and all non-string dynamic types return an empty
// string so numeric, boolean, or null JSON fields never coerce to tool names.
func stringValue(value any) string {
	text, _ := value.(string)
	return text
}
