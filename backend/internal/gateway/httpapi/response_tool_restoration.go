package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	openaicore "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// response_tool_restoration.go associates tool mappings with HTTP requests and
// WebSocket turns and restores client tool names in response payloads.
// Tool lowering can rename reserved functions or flatten namespace declarations
// before upstream execution. Output restoration needs the mapping that belongs
// to that request or turn, while later WebSocket session updates can carry a
// different name set. Active and future-session names therefore occupy separate
// slots.
//
// The request state pointer is initialized through a shared mutex, and
// ResponseTools synchronizes publication of its individual slots. Name
// publication copies string maps, IR ToolMap publication and retrieval return
// detached deep copies, and native namespace mappings retain shared internal
// maps that consumers keep immutable. Buffered restoration uses the stored IR
// ToolMap and re-applies Codex alias reversal when un-flattening exposes an
// aliased function name. SSE restoration can temporarily borrow its event-line
// type for missing payload metadata, removing that added field after a
// successful name rewrite.

// responseToolsContextKey names shared request restoration state read by HTTP
// and turn output adapters before restoring client-visible tool metadata.
const responseToolsContextKey = "gateway_response_tools"

// responseToolsInit serializes publication of per-request state pointers; each request owns its tool data.
var responseToolsInit sync.Mutex

// SetCodexToolNameReverse replaces active-turn names and clears future-session
// names for this request. Nil context is ignored. Each supplied string map is
// copied by ResponseTools, while the two slot publications occur separately and
// require stable input during copying.
func SetCodexToolNameReverse(c *gin.Context, reverse map[string]string) {
	if c == nil {
		return
	}
	responseTools(c, true).SetCodexNames(false, reverse)
	responseTools(c, true).SetCodexNames(true, nil)
}

// MergeCodexToolNameReverse merges supplied names over the current active-turn
// map. Nil context or empty input skips publication; matching keys prefer
// reverse. Retrieved mapping storage stays read-only, and the newly prepared
// map is copied when published.
func MergeCodexToolNameReverse(c *gin.Context, reverse map[string]string) {
	if c == nil || len(reverse) == 0 {
		return
	}
	merged := make(map[string]string, len(reverse)+len(CodexToolNameReverseFromContext(c)))
	maps.Copy(merged, CodexToolNameReverseFromContext(c))
	// Apply newly supplied names after the active snapshot so this update can
	// replace a prior alias while the published map remains read-only.
	maps.Copy(merged, reverse)
	responseTools(c, true).SetCodexNames(false, merged)
}

// CodexToolNameReverseFromContext returns the current active-turn name map or
// nil for missing state. The map is shared after retrieval; callers treat it as
// immutable and publish replacement mappings through setters.
func CodexToolNameReverseFromContext(c *gin.Context) map[string]string {
	return responseTools(c, false).CodexNames(false)
}

// UpdateCodexToolNameReverseForWSFrame updates future-session or active-turn
// restoration according to the root frame type. Nil context skips updates, and
// tools are expected in the normalized root/input forms examined by the
// explicit-declaration helper.
//
// A session.update with a present session.tools field replaces session names
// while leaving active output unchanged. A response.create or untyped frame
// replaces active names; absent explicit declarations inherit session names
// before the supplied reverse map is merged. Explicit tools, including present
// empty or null declarations, suppress that inheritance so client literal names
// do not acquire stale session aliases.
func UpdateCodexToolNameReverseForWSFrame(c *gin.Context, frame []byte, reverse map[string]string) {
	if c == nil {
		return
	}
	eventType := strings.TrimSpace(gjson.GetBytes(frame, "type").String())
	switch eventType {
	case "session.update":
		if gjson.GetBytes(frame, "session.tools").Exists() {
			responseTools(c, true).SetCodexNames(true, reverse)
		}
	case "response.create", "":
		active := reverse
		// Inherit session aliases only for undeclared tools; explicit client
		// declarations can intentionally use literal reserved-looking names and must
		// avoid stale alias restoration.
		if !OpenAIWSFrameHasExplicitToolDeclarations(frame) {
			active = openai.MergeCodexToolNameReverseMaps(
				responseTools(c, false).CodexNames(true),
				reverse,
			)
		}
		responseTools(c, true).SetCodexNames(false, active)
	}
}

// OpenAIWSFrameHasExplicitToolDeclarations reports present root tools or input
// additional_tools declarations. Presence includes empty or null tools fields,
// and additional_tools labels are trimmed and compared case-insensitively;
// callers normalize any nested response envelope before this check.
func OpenAIWSFrameHasExplicitToolDeclarations(frame []byte) bool {
	if gjson.GetBytes(frame, "tools").Exists() {
		return true
	}
	for _, item := range gjson.GetBytes(frame, "input").Array() {
		if strings.EqualFold(strings.TrimSpace(item.Get("type").String()), "additional_tools") && item.Get("tools").Exists() {
			return true
		}
	}
	return false
}

// RestoreCodexToolNamesFromContext restores supported response tool nodes using
// current request state. Session-created or session-updated payload types
// select future-session names; other payloads select active-turn names. The
// shared JSON restorer decides whether bytes are reused and retains unrelated
// fields.
func RestoreCodexToolNamesFromContext(c *gin.Context, data []byte) []byte {
	reverse := CodexToolNameReverseFromContext(c)
	switch strings.TrimSpace(gjson.GetBytes(data, "type").String()) {
	case "session.created", "session.updated":
		reverse = responseTools(c, false).CodexNames(true)
	}
	return openai.RestoreCodexToolNamesInJSON(data, reverse)
}

// RestoreCodexToolNamesFromSSEContext restores names when an SSE event line
// supplies a missing payload type. A nonblank payload type or blank event type
// delegates directly, keeping payload classification authoritative.
//
// Otherwise a temporary type guides the shared tool-node restorer. If
// restoration made no change, original bytes return. Changed payloads have that
// synthetic field removed before output, retaining the caller's type-free
// shape; deletion failure returns the restored bytes with the temporary type
// still present. This exception keeps successful restoration available while
// leaving actual removal failure visible in the resulting representation.
func RestoreCodexToolNamesFromSSEContext(c *gin.Context, data []byte, eventType string) []byte {
	if strings.TrimSpace(gjson.GetBytes(data, "type").String()) != "" || strings.TrimSpace(eventType) == "" {
		return RestoreCodexToolNamesFromContext(c, data)
	}
	// Borrow event-line classification while rewriting recognized tool nodes, then
	// remove the synthetic field so a type-free payload retains its original
	// protocol shape.
	compat := []byte(openaicore.OpenAICompatPayloadWithEventType(string(data), eventType))
	restored := RestoreCodexToolNamesFromContext(c, compat)
	if string(restored) == string(compat) {
		return data
	}
	withoutSyntheticType, err := sjson.DeleteBytes(restored, "type")
	// Keep the restored payload if synthetic-type removal fails, retaining the
	// completed name rewrite even though the temporary classification field
	// remains in output.
	if err != nil {
		return restored
	}
	return withoutSyntheticType
}

// RestoreOpenAIResponsesClientToolPayload restores lowered client tool
// representations using the stored OpenAI IR ToolMap. Missing mappings,
// non-object JSON, or invalid JSON return supplied bytes with no error;
// restoration preserves untouched numeric precision and re-applies Codex alias
// reversal when un-flattening exposes an aliased function name.
func RestoreOpenAIResponsesClientToolPayload(c *gin.Context, payload []byte) ([]byte, error) {
	toolMap, ok := OpenAIResponsesClientToolMap(c)
	if !ok || !isJSONObjectBytes(payload) {
		return payload, nil
	}
	restored, err := convert.RestoreResponsesTools(payload, toolMap)
	if err != nil {
		return payload, err
	}
	// Buffered passthrough runs Codex alias restoration before client-tool
	// un-flattening, so re-apply alias reversal once namespace names emerge.
	if !bytes.Equal(restored, payload) && len(CodexToolNameReverseFromContext(c)) > 0 {
		restored = RestoreCodexToolNamesFromContext(c, restored)
	}
	return restored, nil
}

// RestoreGrokResponsesClientToolPayload restores lowered tool representations
// using the stored Grok IR ToolMap. Missing mappings, absent raw function_call
// markers, non-object JSON, or invalid JSON return supplied bytes with no
// error; otherwise convert.RestoreResponsesTools rewrites output.
func RestoreGrokResponsesClientToolPayload(c *gin.Context, payload []byte) ([]byte, error) {
	toolMap, ok := GrokResponsesClientToolMap(c)
	if !ok || !bytes.Contains(payload, []byte(`"function_call"`)) || !isJSONObjectBytes(payload) {
		return payload, nil
	}
	restored, err := convert.RestoreResponsesTools(payload, toolMap)
	if err != nil {
		return payload, err
	}
	return restored, nil
}

// isJSONObjectBytes reports whether payload is a syntactically valid top-level
// JSON object after trimming outer whitespace. Non-object JSON scalars or
// arrays return false so envelope restorers skip non-response payloads cleanly.
func isJSONObjectBytes(payload []byte) bool {
	trimmed := bytes.TrimSpace(payload)
	return len(trimmed) > 0 && trimmed[0] == '{' && json.Valid(payload)
}

// FlattenOpenAIResponsesNamespaces lowers supported namespace declarations
// while exempting image_gen and stores their reverse names after successful
// encoding. The report contains the counts of committed edits. An unchanged
// payload returns its original bytes and an empty report.
//
// Decoding or transformation failure returns the body with an error, and
// namespace state is published after replacement bytes exist. Number-preserving
// decoding retains exact integer arguments and schema bounds. Callers retain
// source bytes separately when another attempt needs its original namespace
// declarations.
func FlattenOpenAIResponsesNamespaces(c *gin.Context, body []byte) ([]byte, openaicore.NamespaceFlattening, error) {
	if !bytes.Contains(body, []byte(`"namespace"`)) && !bytes.Contains(body, []byte(`\u`)) {
		return body, openaicore.NamespaceFlattening{}, nil
	}
	var requestBody map[string]any
	if err := wirejson.DecodeUseNumber(body, &requestBody); err != nil {
		return body, openaicore.NamespaceFlattening{}, fmt.Errorf("decode OpenAI namespace body: %w", err)
	}
	report, err := openaicore.FlattenResponsesNamespacesWithReport(requestBody, map[string]bool{"image_gen": true})
	if err != nil {
		return body, openaicore.NamespaceFlattening{}, err
	}
	if len(report.Names) == 0 {
		return body, openaicore.NamespaceFlattening{}, nil
	}
	rebuilt, err := wirejson.Marshal(requestBody)
	if err != nil {
		return body, openaicore.NamespaceFlattening{}, fmt.Errorf("encode OpenAI namespace body: %w", err)
	}
	// Publish reverse names after encoding succeeds, so a failed replacement
	// cannot install restoration state for bytes the caller never received.
	SetOpenAIResponsesNamespaceNames(c, report.Names)
	return rebuilt, report, nil
}

// RestoreOpenAIResponsesNamespacePayload restores flattened call names from
// shared namespace state. Missing names or invalid JSON keeps supplied bytes,
// and a restoration error returns those bytes with the cause; only a changed
// successful payload replaces them.
func RestoreOpenAIResponsesNamespacePayload(c *gin.Context, payload []byte) ([]byte, error) {
	names := OpenAIResponsesNamespaceNames(c)
	if len(names) == 0 || !json.Valid(payload) {
		return payload, nil
	}
	restored, changed, err := openaicore.RestoreResponsesNamespaceCalls(payload, names)
	if err != nil {
		return payload, err
	}
	if changed {
		return restored, nil
	}
	return payload, nil
}

// responseTools reads the request restoration state and optionally publishes a
// new instance. Nil contexts return nil, reads without create do not allocate,
// and a stored typed nil remains nil.
//
// Creation takes the shared initialization mutex and rechecks the binding
// before publishing, so callers using this helper can reuse an already
// initialized pointer. The mutex protects pointer creation, while ResponseTools
// owns slot publication. Retrieved maps and bytes remain shared, requiring
// immutable consumer access and setter-based replacement; another owner
// replacing the Gin binding is outside this helper's initialization
// coordination.
func responseTools(c *gin.Context, create bool) *requeststate.ResponseTools {
	if c == nil {
		return nil
	}
	if value, ok := c.Get(responseToolsContextKey); ok {
		if state, ok := value.(*requeststate.ResponseTools); ok {
			return state
		}
	}
	if !create {
		return nil
	}
	// Recheck under the shared initialization lock before publishing to the Gin
	// slot, so concurrent helper creation cannot replace another helper's newly
	// initialized request state.
	responseToolsInit.Lock()
	defer responseToolsInit.Unlock()
	if value, ok := c.Get(responseToolsContextKey); ok {
		if state, ok := value.(*requeststate.ResponseTools); ok {
			return state
		}
	}
	state := &requeststate.ResponseTools{}
	c.Set(responseToolsContextKey, state)
	return state
}

// SetOpenAIResponsesNamespaceNames publishes a nonempty namespace reverse map
// for the request. Empty input retains existing names; callers use
// ClearOpenAIResponsesNamespaceNames to remove them. Published map storage is
// shared and remains immutable while restoration reads it.
func SetOpenAIResponsesNamespaceNames(c *gin.Context, names map[string]openaicore.ResponsesNamespaceName) {
	if len(names) > 0 {
		responseTools(c, true).SetNamespaces(names)
	}
}

// ClearOpenAIResponsesNamespaceNames clears existing namespace reverse-name
// storage without creating request state. The nil-safe setter leaves absent
// state unchanged, while output consumers of an earlier retrieved map keep
// their captured reference.
func ClearOpenAIResponsesNamespaceNames(c *gin.Context) { responseTools(c, false).SetNamespaces(nil) }

// OpenAIResponsesNamespaceNames returns the shared namespace reverse-name map
// from request state. Missing state yields nil; callers read the map immutably
// and use setters to publish a replacement for later restoration.
func OpenAIResponsesNamespaceNames(c *gin.Context) map[string]openaicore.ResponsesNamespaceName {
	return responseTools(c, false).Namespaces()
}

// SetOpenAIResponsesClientToolMap publishes an OpenAI IR ToolMap with active
// call restorations or clears existing state when tools has none. ResponseTools
// stores a detached deep copy so callers may reuse or discard tools safely.
func SetOpenAIResponsesClientToolMap(c *gin.Context, tools *convert.ToolMap) {
	if !tools.HasRestorations() {
		ClearOpenAIResponsesClientToolMap(c)
		return
	}
	responseTools(c, true).SetClientToolMap(false, tools)
}

// ClearOpenAIResponsesClientToolMap clears the OpenAI IR ToolMap from existing
// request state without allocating a new ResponseTools instance when absent.
// Callers invoke this before a new attempt so stale lowered state never leaks.
func ClearOpenAIResponsesClientToolMap(c *gin.Context) {
	responseTools(c, false).SetClientToolMap(false, nil)
}

// OpenAIResponsesClientToolMap returns a detached copy of the stored OpenAI IR
// ToolMap and reports whether active call restorations exist. Missing state or
// maps without call restorations return nil and false.
func OpenAIResponsesClientToolMap(c *gin.Context) (*convert.ToolMap, bool) {
	tools := responseTools(c, false).ClientToolMap(false)
	return tools, tools.HasRestorations()
}

// SetGrokResponsesClientToolMap publishes a Grok IR ToolMap with active call
// restorations or clears existing state when tools has none. ResponseTools
// stores a detached deep copy so output readers observe stable snapshot state.
func SetGrokResponsesClientToolMap(c *gin.Context, tools *convert.ToolMap) {
	if !tools.HasRestorations() {
		ClearGrokResponsesClientToolMap(c)
		return
	}
	responseTools(c, true).SetClientToolMap(true, tools)
}

// ClearGrokResponsesClientToolMap clears the Grok IR ToolMap from existing
// request state without allocating a new ResponseTools instance when absent.
// Callers invoke this before another attempt discards prior lowered evidence.
func ClearGrokResponsesClientToolMap(c *gin.Context) {
	responseTools(c, false).SetClientToolMap(true, nil)
}

// GrokResponsesClientToolMap returns a detached copy of the stored Grok IR
// ToolMap and reports whether active call restorations exist. Missing state or
// maps without call restorations return nil and false.
func GrokResponsesClientToolMap(c *gin.Context) (*convert.ToolMap, bool) {
	tools := responseTools(c, false).ClientToolMap(true)
	return tools, tools.HasRestorations()
}

// OpenAIWSHTTPBridgeToolMapFromContext returns a detached copy of the retained
// HTTP-bridge IR ToolMap and its publication-presence flag. Missing state
// returns (nil, false), while an explicit tools reset returns (nil, true).
func OpenAIWSHTTPBridgeToolMapFromContext(c *gin.Context) (*convert.ToolMap, bool) {
	return responseTools(c, false).BridgeToolMap()
}

// SetOpenAIWSHTTPBridgeToolMap publishes a detached copy of tools into the
// HTTP-bridge session slot and marks it present, including a nil reset value.
// Nil context is ignored by the underlying nil-safe ResponseTools setter.
func SetOpenAIWSHTTPBridgeToolMap(c *gin.Context, tools *convert.ToolMap) {
	responseTools(c, true).SetBridgeToolMap(tools)
}
