package qoder

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
)

// payload.go builds Qoder desktop inference payloads, conversation
// fingerprints, and declared tool-name alias mappers from normalized
// QoderPayloadRequest values.
//
// Route capability projection runs before conversation fingerprinting so reused
// sessions only hash tool declarations that reach the wire. Payload
// construction retains shared tool definitions, copies outgoing text-block maps
// before attaching automatic cache markers, and mirrors observed context and
// thinking controls into Qoder's nested parameters, model_config, and
// chat_context envelopes.

// FallbackMaxInputTokens supplies the native wire limit when no group, account
// or catalog source provided an input limit for the selected model.
const FallbackMaxInputTokens = 200000

// Billing checksum normalization keeps volatile client billing metadata from
// changing the stable conversation prefix used for session prefix comparison.
var (
	// QoderClaudeBillingCCHRe recognizes five-hex billing checksum text ending in a
	// semicolon. System normalization replaces that volatile value before message
	// payload or conversation fingerprint construction.
	QoderClaudeBillingCCHRe = regexp.MustCompile(`(x-anthropic-billing-header:[^\n\r;]*?(?:;[^\n\r;]*?)*\bcch=)[0-9a-fA-F]{5}(;)`)
)

// QoderThinkingDirective carries normalized client intent before
// route-capability effort mapping.
type QoderThinkingDirective struct {
	// Specified distinguishes explicit disabling from an absent client control.
	// Missing controls inherit the selected account model's current defaults.
	Specified bool // explicit client control
	// Enabled selects whether adjustable routes send thinking controls. Effort is
	// normalized client intent; effort-capable routes translate it only while
	// Enabled is true. Disabled thinking sends none on those adjustable routes.
	Enabled bool   // requested thinking switch
	Effort  string // normalized requested effort
}

// QoderPayloadRequest retains normalized protocol input and session sources for
// Gateway preparation.
type QoderPayloadRequest struct {
	// Capabilities supplies per-attempt resolved attributes and execution rules
	// when present. Nil leaves only execution defaults for an explicitly
	// configured custom route; no site-wide model table is consulted.
	Capabilities *modelrules.Resolution // resolved route attributes
	Model        string                 // client model name
	Site         Site                   // selected credential site
	System       string                 // normalized system text
	// Messages retains mutable raw and text-block maps used by conversion.
	// Enrichment and cache-control insertion can mutate those maps before request
	// dispatch.
	Messages []QoderMessage // ordered normalized history
	// Tools is shared with payloads when tools are included. Callers keep its
	// nested declarations stable while planning fingerprints and preparing request
	// bytes.
	Tools      []any                  // declared function tools
	ToolChoice any                    // normalized per-request tool policy
	MaxTokens  int                    // output tokens
	Thinking   QoderThinkingDirective // normalized thinking intent
	// ExplicitSession, PromptCacheKey, and MetadataUserID retain distinct client
	// session sources. QoderConversationKey selects precedence and applies API-key
	// plus provider isolation after parsing.
	ExplicitSession string // body session identifier
	PromptCacheKey  string // stable prompt-cache identifier
	MetadataUserID  string // Anthropic metadata identity
	ResponseID      string // generated Responses identifier
}

// QoderClaudeCodeStablePrefixKey joins model, normalized system, tool hash, and
// first user text. The conversation caller applies API-key and provider
// isolation to the result; a missing first user text leaves the other stable
// inputs present.
func QoderClaudeCodeStablePrefixKey(request QoderPayloadRequest) string {
	parts := []string{
		"model=" + strings.TrimSpace(request.Model),
		"system=" + QoderNormalizeSystemForFingerprint(request.System),
		"tools=" + QoderFingerprintAny(request.Tools),
	}
	if firstUser := QoderFirstUserMessageText(request.Messages); firstUser != "" {
		parts = append(parts, "first_user="+firstUser)
	}
	return strings.Join(parts, "|")
}

// QoderFirstUserMessageText returns the first nonblank user text after
// trimming it. Roles are trimmed before matching user; absent qualifying text
// returns empty so stable-prefix construction can use its other inputs.
func QoderFirstUserMessageText(messages []QoderMessage) string {
	for _, message := range messages {
		if strings.TrimSpace(message.Role) != "user" {
			continue
		}
		if text := strings.TrimSpace(message.Text); text != "" {
			return text
		}
	}
	return ""
}

// QoderMessageFingerprints hashes each message's outgoing payload representation
// in order. Outgoing text-block maps are copied, while raw tool data is read
// synchronously. Callers keep the input stable until all history fingerprints
// have been computed.
func QoderMessageFingerprints(messages []QoderMessage) []string {
	fingerprints := make([]string, 0, len(messages))
	for _, message := range messages {
		fingerprints = append(fingerprints, QoderFingerprintAny(QoderPayloadMessageFromMessage(message)))
	}
	return fingerprints
}

// QoderFingerprintString returns the payload fingerprint of the supplied string
// bytes.
func QoderFingerprintString(value string) string {
	return qoderPayloadFingerprint([]byte(value))
}

// QoderSystemFingerprint hashes system text after normalizing recognized
// volatile billing checksums.
func QoderSystemFingerprint(system string) string {
	return QoderFingerprintString(QoderNormalizeSystemForFingerprint(system))
}

// QoderNormalizeSystemForFingerprint replaces recognized five-hex cch values
// before system hashing. Other system text remains supplied, allowing stable
// conversation comparison across checksum variants while retaining the rest of
// the billing header.
func QoderNormalizeSystemForFingerprint(system string) string {
	if !strings.Contains(system, "x-anthropic-billing-header") || !strings.Contains(system, "cch=") {
		return system
	}
	return QoderClaudeBillingCCHRe.ReplaceAllString(system, "${1}<cch>${2}")
}

// QoderFingerprintAny hashes compact JSON, using formatted text when JSON
// marshaling fails. Empty serialized input follows qoderPayloadFingerprint's
// empty result; callers use the returned text for conversation comparison.
func QoderFingerprintAny(value any) string {
	body, err := json.Marshal(value)
	if err != nil {
		// Unsupported Go values cannot produce JSON fingerprints. Hash their formatted
		// text so the helper still returns a fingerprint when marshaling fails.
		return qoderPayloadFingerprint([]byte(fmt.Sprint(value)))
	}
	return qoderPayloadFingerprint(body)
}

// BuildQoderPayloadWithOptions builds a complete Qoder request using selected
// messages and inclusion flags. The captured account model supplies the service
// key and execution controls. A blank session ID generates a UUID. The
// supplied MaxTokens is written as provided, and callers pass a supported site
// for business-profile lookup.
//
// Request and chat-record IDs share one UUID while request-set and business IDs
// are separate. Prompt selection considers current messages before the request's
// full history. System/tool inclusion and mirrored context/thinking settings
// shape the outgoing request. Automatic cache markers modify only outgoing
// text-block maps; tool definitions remain shared references in the returned
// payload and must stay stable until serialization finishes.
func BuildQoderPayloadWithOptions(request QoderPayloadRequest, sessionID string, messages []QoderMessage, includeSystem bool, includeTools bool) (map[string]any, string, error) {
	request, err := request.withEffectiveControls()
	if err != nil {
		return nil, "", err
	}
	payload, model := buildQoderPayload(request, sessionID, messages, includeSystem, includeTools)
	return payload, model, nil
}

// withEffectiveControls projects client intent onto the selected route before
// fingerprinting tools or preparing a request. The final thinking switch and
// effort determine conditional tool support, including for clients that rely
// on account defaults. A forced or named tool conflicts with disabled tools.
//
// Optional declarations are omitted from the outbound request when the route
// disables calls. Prior tool calls and results remain part of the conversation
// history. The returned request owns its control fields and leaves the supplied
// draft available for other route attempts.
func (request QoderPayloadRequest) withEffectiveControls() (QoderPayloadRequest, error) {
	thinking, err := ResolveObservedThinking(request.Thinking, request.Capabilities)
	if err != nil {
		return request, err
	}
	request.Thinking = thinking
	if choice, ok := request.ToolChoice.(map[string]any); ok && QoderStringField(choice, "type") == "allowed_tools" {
		request.Tools, request.ToolChoice, err = restrictQoderFunctions(request.Tools, choice)
		if err != nil {
			return request, err
		}
	}
	if request.Capabilities == nil || len(request.Tools) == 0 && request.ToolChoice == nil {
		return request, nil
	}
	attrs := request.Capabilities.Attributes
	if attrs.Conditions != nil {
		state := modelrules.RequestState{Thinking: &thinking.Enabled}
		if thinking.Effort != "" {
			state.Effort = &thinking.Effort
		}
		attrs = request.Capabilities.ForRequest(state).Attributes
	}
	if attrs.ToolCall == nil || *attrs.ToolCall {
		return request, nil
	}
	switch choice := request.ToolChoice.(type) {
	case map[string]any:
		return request, &RequestError{Code: "unsupported_tool_choice", Message: "This Qoder route does not support the selected tool."}
	case string:
		if choice == "required" {
			return request, &RequestError{Code: "unsupported_tool_choice", Message: "This Qoder route does not support required tool calls."}
		}
	}
	request.Tools = []any{}
	request.ToolChoice = nil
	return request, nil
}

// buildQoderPayload writes already validated controls and the planned transcript.
// Conversation planning and this encoder receive the same effective tool list
// so reused sessions cannot fingerprint tools that did not reach the wire.
func buildQoderPayload(request QoderPayloadRequest, sessionID string, messages []QoderMessage, includeSystem, includeTools bool) (map[string]any, string) {
	modelInfo := QoderModelInfo{Key: strings.TrimSpace(request.Model), Source: "system"}
	observed, found := observedModel(request.Capabilities)
	if found {
		modelInfo.Key = observed.Key
	}
	requestID := uuid.NewString()
	prompt := LatestQoderPayloadPromptText(messages, includeTools)
	if prompt == "" {
		prompt = LatestQoderPayloadPromptText(request.Messages, includeTools)
	}
	if strings.TrimSpace(sessionID) == "" {
		sessionID = uuid.NewString()
	}
	payload := QoderBasePayload()
	payload["request_id"] = requestID
	payload["chat_record_id"] = requestID
	payload["request_set_id"] = uuid.NewString()
	payload["session_id"] = sessionID
	// The current desktop runtime leaves this legacy hint empty. A saved plan
	// label must not replace that protocol value when constructing a new request.
	payload["aliyun_user_type"] = ""
	parameters, _ := payload["parameters"].(map[string]any)
	modelConfig, _ := payload["model_config"].(map[string]any)
	parameters["max_tokens"] = request.MaxTokens
	if request.ToolChoice != nil {
		parameters["tool_choice"] = request.ToolChoice
	}
	modelConfig["key"] = modelInfo.Key
	modelConfig["source"] = modelInfo.Source
	modelConfig["display_name"] = modelInfo.Key
	if found {
		if observed.Name != "" {
			modelConfig["display_name"] = observed.Name
		}
		if observed.Vision != nil {
			modelConfig["is_vl"] = *observed.Vision
		}
	}
	chatContext, _ := payload["chat_context"].(map[string]any)
	extra, _ := chatContext["extra"].(map[string]any)
	extraModelConfig, _ := extra["modelConfig"].(map[string]any)
	chatContext["text"] = prompt
	extraModelConfig["key"] = modelInfo.Key
	extraModelConfig["source"] = modelInfo.Source
	applyContextCapability(payload, request.Capabilities, len(observed.ContextWindows) > 0)
	applyObservedThinking(payload, request.Thinking)
	extra["originalContent"] = prompt
	// Business name is limited to thirty runes while full prompt text remains in
	// chat context and message history. Truncation affects this descriptive title
	// alone.
	payload["business"] = map[string]any{
		"product":  DesktopProduct,
		"version":  MustProfileForSite(request.Site).ClientVersion,
		"type":     "agent",
		"stage":    "init",
		"id":       uuid.NewString(),
		"name":     TruncateRunes(prompt, 30),
		"begin_at": time.Now().UnixMilli(),
	}

	outMessages := make([]any, 0, len(messages)+1)
	if includeSystem && strings.TrimSpace(request.System) != "" {
		outMessages = append(outMessages, QoderPayloadMessage("system", request.System))
	}
	for _, msg := range messages {
		outMessages = append(outMessages, QoderPayloadMessageFromMessage(msg))
	}
	// The last eligible text block defines the outgoing ephemeral cache marker.
	// Apply it after message conversion so included system and history share one
	// selection order.
	AddQoderEphemeralCacheControl(outMessages)
	payload["messages"] = outMessages
	if includeTools {
		payload["tools"] = request.Tools
	} else {
		payload["tools"] = []any{}
	}
	return payload, modelInfo.Key
}

// applyContextCapability encodes the effective input limit in Qoder's nested fields.
// The wire fallback applies only when every attribute source omitted the limit.
// Selectable windows additionally require the runtime override and context length.
func applyContextCapability(payload map[string]any, resolution *modelrules.Resolution, selectable bool) {
	maxInput := FallbackMaxInputTokens
	if resolution != nil && resolution.Attributes.InputLimit != nil {
		maxInput = *resolution.Attributes.InputLimit
	}
	modelConfig, _ := payload["model_config"].(map[string]any)
	modelConfig["max_input_tokens"] = maxInput
	if !selectable {
		return
	}

	parameters, _ := payload["parameters"].(map[string]any)
	parameters["context_length"] = maxInput
	chatContext, _ := payload["chat_context"].(map[string]any)
	extra, _ := chatContext["extra"].(map[string]any)
	runtimeOverride, _ := extra["ideModelConfigOverride"].(map[string]any)
	if runtimeOverride == nil {
		runtimeOverride = map[string]any{}
		extra["ideModelConfigOverride"] = runtimeOverride
	}
	runtimeOverride["max_input_tokens"] = maxInput
}

// AddQoderEphemeralCacheControl marks the final nonblank message content for
// caching. Text arrays carry the marker on their last text block, and scalar
// content carries it on the message. Existing markers remain authoritative;
// callers supply payload-owned message and block maps before this mutation.
func AddQoderEphemeralCacheControl(messages []any) {
	for _, message := range slices.Backward(messages) {
		message, ok := message.(map[string]any)
		if !ok {
			continue
		}
		if text, ok := message["content"].(string); ok && strings.TrimSpace(text) != "" {
			// Scalar tool or assistant content carries cache control on the message,
			// matching the desktop representation without duplicating text in another field.
			if _, exists := message["cache_control"]; !exists {
				message["cache_control"] = map[string]any{"type": "ephemeral"}
			}
			return
		}
		contents, _ := message["content"].([]any)
		for _, content := range slices.Backward(contents) {
			block, ok := content.(map[string]any)
			if !ok || block["type"] != "text" {
				continue
			}
			text, _ := block["text"].(string)
			if strings.TrimSpace(text) == "" {
				continue
			}
			if _, exists := block["cache_control"]; exists {
				return
			}
			block["cache_control"] = map[string]any{"type": "ephemeral"}
			return
		}
	}
}

// QoderBasePayload returns newly allocated Qoder request maps with execution
// defaults and empty history. Nested model and chat-context maps are ready for
// capability writers, while messages and tools start as empty slices.
func QoderBasePayload() map[string]any {
	return map[string]any{
		"request_id":       "",
		"request_set_id":   "",
		"chat_record_id":   "",
		"stream":           true,
		"chat_task":        "FREE_INPUT",
		"image_urls":       nil,
		"is_reply":         true,
		"is_retry":         false,
		"session_id":       "",
		"code_language":    "",
		"source":           1,
		"version":          "3",
		"chat_prompt":      "",
		"parameters":       map[string]any{"max_tokens": QoderDefaultMaxTokens},
		"aliyun_user_type": "",
		"session_type":     DesktopProduct,
		"agent_id":         "agent_common",
		"task_id":          "common",
		"chat_context": map[string]any{
			"chatPrompt": "",
			"features":   []any{},
			"imageUrls":  nil,
			"text":       "",
			"extra": map[string]any{
				"context":         []any{},
				"modelConfig":     map[string]any{"is_reasoning": false, "key": "auto"},
				"originalContent": "",
			},
		},
		"model_config": map[string]any{
			"key":              "auto",
			"format":           "openai",
			"model":            "",
			"api_key":          "",
			"url":              "",
			"is_vl":            false,
			"is_reasoning":     false,
			"source":           "system",
			"max_input_tokens": FallbackMaxInputTokens,
		},
		"messages": []any{},
		"tools":    []any{},
	}
}

// QoderPayloadMessage converts a role and text through the standard Qoder
// message builder.
func QoderPayloadMessage(role, text string) map[string]any {
	return QoderPayloadMessageFromMessage(QoderMessage{Role: role, Text: text})
}

// restrictQoderFunctions translates an explicit allowed-functions subset into
// native tool declarations and the corresponding auto or required choice.
// Chat nests the policy under allowed_tools; Responses places the same fields
// beside type. Every selected name must match a declared function in this turn.
//
// Filtering before fingerprinting keeps the session's effective tool set equal
// to the outgoing definitions. Preserve their original order and reject choices
// that the function-only native adapter cannot encode rather than expanding an
// unknown subset to every declared tool.
func restrictQoderFunctions(tools []any, choice map[string]any) ([]any, any, error) {
	invalid := &RequestError{Code: "unsupported_tool_choice", Message: "Select declared Qoder functions and an auto or required allowed_tools mode."}
	if nested, present := choice["allowed_tools"]; present {
		var ok bool
		choice, ok = nested.(map[string]any)
		if !ok {
			return nil, nil, invalid
		}
	}
	mode := QoderStringField(choice, "mode")
	if mode != "auto" && mode != "required" {
		return nil, nil, invalid
	}
	allowed := map[string]bool{}
	for _, item := range QoderAnySlice(choice["tools"]) {
		entry, ok := item.(map[string]any)
		name := QoderDeclaredToolName(entry)
		if !ok || name == "" || QoderStringField(entry, "type") != "function" {
			return nil, nil, invalid
		}
		allowed[name] = true
	}
	if len(allowed) == 0 {
		return nil, nil, invalid
	}
	selected := make([]any, 0, len(allowed))
	for _, tool := range tools {
		name := QoderDeclaredToolName(tool)
		if _, ok := allowed[name]; ok {
			selected = append(selected, tool)
			allowed[name] = false
		}
	}
	for _, missing := range allowed {
		if missing {
			return nil, nil, invalid
		}
	}
	return selected, mode, nil
}

// QoderDeclaredToolNameMapper returns a mapper from normalized aliases to
// client-declared tool names. No usable declarations returns nil. Alias
// construction resolves collisions in declaration order, with the last name
// recorded for a key.
//
// The returned callback trims and normalizes each lookup key, then returns the
// declared spelling when present. Unknown or blank incoming names retain their
// supplied text. The alias map belongs to this callback after construction, so
// subsequent changes to the input declaration maps do not rebuild its
// selections.
func QoderDeclaredToolNameMapper(tools []any) QoderToolNameMapper {
	aliases := QoderDeclaredToolNameAliases(tools)
	if len(aliases) == 0 {
		return nil
	}
	return func(name string) string {
		key := QoderToolNameKey(name)
		if key == "" {
			return name
		}
		if mapped := aliases[key]; mapped != "" {
			return mapped
		}
		return name
	}
}

// QoderDeclaredToolNameAliases builds normalized alias keys for the supplied
// tool definitions. Unnamed declarations are skipped and later declarations
// replace colliding keys; no usable alias returns nil for optional mapping.
func QoderDeclaredToolNameAliases(tools []any) map[string]string {
	aliases := map[string]string{}
	for _, raw := range tools {
		name := QoderDeclaredToolName(raw)
		if name == "" {
			continue
		}
		for _, alias := range QoderToolNameAliases(name) {
			key := QoderToolNameKey(alias)
			if key != "" {
				aliases[key] = name
			}
		}
	}
	if len(aliases) == 0 {
		return nil
	}
	return aliases
}

// QoderDeclaredToolName reads a trimmed top-level tool name before nested
// function.name. Non-object or unnamed declarations produce empty text, allowing
// alias construction and selection to skip unusable entries.
func QoderDeclaredToolName(raw any) string {
	tool, ok := raw.(map[string]any)
	if !ok {
		return ""
	}
	if name := strings.TrimSpace(QoderStringField(tool, "name")); name != "" {
		return name
	}
	function, _ := tool["function"].(map[string]any)
	return strings.TrimSpace(QoderStringField(function, "name"))
}

// QoderToolNameAliases returns the declared spelling and recognized Qoder
// compatibility names. Blank input returns nil; known shell/file/search/todo
// tools add the supported aliases, while other names keep their trimmed spelling
// alone.
func QoderToolNameAliases(name string) []string {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return nil
	}
	aliases := []string{trimmed}
	switch strings.ToLower(trimmed) {
	case "bash":
		aliases = append(aliases, "Bash", "shell", "execute_bash")
	case "read":
		aliases = append(aliases, "Read")
	case "write":
		aliases = append(aliases, "Write")
	case "edit":
		aliases = append(aliases, "Edit")
	case "multiedit", "multi_edit":
		aliases = append(aliases, "MultiEdit", "multi_edit")
	case "grep":
		aliases = append(aliases, "Grep")
	case "glob":
		aliases = append(aliases, "Glob")
	case "ls":
		aliases = append(aliases, "LS", "Ls")
	case "todowrite", "todo_write":
		aliases = append(aliases, "TodoWrite", "todo_write")
	}
	return aliases
}

// QoderToolNameKey lowercases trimmed tool names and removes underscores for
// alias matching.
func QoderToolNameKey(name string) string {
	return strings.ToLower(strings.ReplaceAll(strings.TrimSpace(name), "_", ""))
}

// QoderCopyRawMap returns a new top-level map containing the supplied raw
// entries. Nested values remain shared, and nil or empty input produces an
// editable empty map for message normalization.
func QoderCopyRawMap(raw map[string]any) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	copied := make(map[string]any, len(raw))
	maps.Copy(copied, raw)
	return copied
}

// QoderPayloadMessageFromMessage builds canonical desktop message content and
// retains tool identity. User text uses content blocks so cache annotations
// remain attached to their text; other roles retain scalar content unless
// explicit text blocks were supplied. The obsolete text-only contents field
// is not emitted because current routes consume the canonical content field.
//
// Tool history is normalized from raw calls or Anthropic tool-use blocks.
// Explicit ToolCallID wins over raw aliases so call/result association remains
// coherent. Each emitted text block receives its own map before cache markers
// are added, keeping payload construction from mutating request-owned blocks
// retained by conversation fingerprinting and retry preparation.
func QoderPayloadMessageFromMessage(message QoderMessage) map[string]any {
	msg := map[string]any{"role": message.Role, "content": message.Text}
	if len(message.TextContent) > 0 {
		content := make([]any, 0, len(message.TextContent))
		for _, block := range message.TextContent {
			content = append(content, QoderCopyRawMap(block))
		}
		msg["content"] = content
	} else if message.Role == "user" && message.Text != "" {
		msg["content"] = []any{map[string]any{"type": "text", "text": message.Text}}
	}
	CopyQoderToolFields(msg, message.Raw)
	if strings.TrimSpace(message.ToolCallID) != "" {
		toolCallID := strings.TrimSpace(message.ToolCallID)
		msg["tool_call_id"] = toolCallID
		msg["tool_call_call_id"] = toolCallID
	}
	return msg
}

// CopyQoderToolFields writes normalized call history and result identity into
// a usable outgoing message map. Empty raw input leaves the map as supplied.
// Modern tool_calls take precedence when usable; otherwise Anthropic tool-use
// blocks supply function history.
//
// Result identity prefers tool_call_id, tool_call_call_id, then call_id after
// trimming. The selected value fills both Gateway result fields so they refer
// to one invocation. A nonblank string name retains its supplied spelling.
// Normalized call maps are allocated, while this operation mutates the
// caller's outgoing map.
func CopyQoderToolFields(out map[string]any, raw map[string]any) {
	if len(raw) == 0 {
		return
	}
	if toolCalls := NormalizeQoderToolCalls(raw["tool_calls"]); len(toolCalls) > 0 {
		out["tool_calls"] = toolCalls
	} else if toolCalls := AnthropicToolUseBlocksToQoderToolCalls(raw["content"]); len(toolCalls) > 0 {
		out["tool_calls"] = toolCalls
	}
	// Raw result histories use several ID spellings. Resolve them once and mirror
	// that choice into both Gateway result fields so they identify one invocation.
	if toolCallID := FirstNonEmptyQoder(
		QoderStringField(raw, "tool_call_id"),
		QoderStringField(raw, "tool_call_call_id"),
		QoderStringField(raw, "call_id"),
	); toolCallID != "" {
		out["tool_call_id"] = toolCallID
		out["tool_call_call_id"] = toolCallID
	}
	if name, ok := raw["name"].(string); ok && strings.TrimSpace(name) != "" {
		out["name"] = name
	}
}

// EnrichQoderToolResultMessages mutates result messages with call IDs and names
// learned while walking history. It indexes each raw message before enriching
// that message, retaining the latest name per call ID. Existing result name and
// explicit ToolCallID remain authoritative.
//
// A tool result can therefore gain the owning assistant call's name even when
// the client omitted it. The message slice and raw maps are modified in place,
// creating a raw map when necessary; callers finish enrichment before sharing
// the request with conversation planning.
func EnrichQoderToolResultMessages(messages []QoderMessage) []QoderMessage {
	toolNamesByID := map[string]string{}
	for i := range messages {
		for id, name := range QoderToolCallNamesFromRaw(messages[i].Raw) {
			if id != "" && name != "" {
				toolNamesByID[id] = name
			}
		}
		if messages[i].Role != "tool" {
			continue
		}
		toolCallID := QoderMessageToolCallID(messages[i])
		if toolCallID == "" {
			continue
		}
		if strings.TrimSpace(messages[i].ToolCallID) == "" {
			messages[i].ToolCallID = toolCallID
		}
		if QoderStringField(messages[i].Raw, "name") != "" {
			continue
		}
		name := toolNamesByID[toolCallID]
		if name == "" {
			continue
		}
		if messages[i].Raw == nil {
			messages[i].Raw = map[string]any{}
		}
		messages[i].Raw["name"] = name
	}
	return messages
}

// NormalizeQoderToolCalls converts usable nested function calls into newly
// allocated wire maps. Missing name and arguments drops an entry, absent type
// becomes function, and argument values are serialized through
// QoderToolArgumentsString.
func NormalizeQoderToolCalls(raw any) []any {
	tools := QoderAnySlice(raw)
	if len(tools) == 0 {
		return []any{}
	}
	normalized := make([]any, 0, len(tools))
	for _, rawTool := range tools {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			continue
		}
		function, _ := tool["function"].(map[string]any)
		name := QoderStringField(function, "name")
		arguments := QoderToolArgumentsString(function["arguments"])
		if name == "" && strings.TrimSpace(arguments) == "" {
			continue
		}
		id := QoderStringField(tool, "id")
		callType := QoderStringField(tool, "type")
		if callType == "" {
			// Call type is optional in imported history. Supply function for downstream
			// output while retaining the actual ID and name fields used to associate
			// results.
			callType = "function"
		}
		normalized = append(normalized, map[string]any{
			"id":   id,
			"type": callType,
			"function": map[string]any{
				"name":      name,
				"arguments": arguments,
			},
		})
	}
	return normalized
}

// AnthropicToolUseBlocksToQoderToolCalls converts named tool_use blocks into
// function-call maps. Other content and blank names are skipped; IDs remain with
// their serialized inputs for later tool-result association.
func AnthropicToolUseBlocksToQoderToolCalls(raw any) []any {
	blocks := QoderAnySlice(raw)
	if len(blocks) == 0 {
		return []any{}
	}
	toolCalls := make([]any, 0, len(blocks))
	for _, rawBlock := range blocks {
		block, ok := rawBlock.(map[string]any)
		if !ok || block["type"] != "tool_use" {
			continue
		}
		name := QoderStringField(block, "name")
		if name == "" {
			continue
		}
		toolCalls = append(toolCalls, map[string]any{
			"id":   QoderStringField(block, "id"),
			"type": "function",
			"function": map[string]any{
				"name":      name,
				"arguments": QoderToolArgumentsString(block["input"]),
			},
		})
	}
	return toolCalls
}

// qoderPayloadFingerprint returns lowercase SHA-256 text, with empty text for
// zero-length payloads. Callers use that empty result to distinguish absent
// serialized data while comparing conversation fingerprints.
func qoderPayloadFingerprint(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
