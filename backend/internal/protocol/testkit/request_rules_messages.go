package testkit

import (
	"fmt"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// request_rules_messages.go validates Anthropic Messages request JSON payloads
// against tool_use/tool_result pairing, leading tool_result placement, thinking
// signature and ordering rules, positive max_tokens > budget_tokens bounds,
// non-empty text blocks, tool naming, and optional strict top-level member checks.
//
// Upstream Messages endpoints enforce turn-adjacent tool pairing with all
// tool_result blocks placed ahead of user text or media, and require signed
// thinking blocks ahead of tool_use blocks whenever extended thinking is
// active, whereas passback compatible endpoints accept unsigned thinking blocks
// on tool-calling assistant turns without stripping thinking history.

// messagesAllowedTopLevelKeys lists documented Anthropic Messages request
// fields accepted when StrictTopLevel validation is enabled.
var messagesAllowedTopLevelKeys = map[string]bool{
	"model":              true,
	"messages":           true,
	"max_tokens":         true,
	"system":             true,
	"cache_control":      true,
	"metadata":           true,
	"stop_sequences":     true,
	"stream":             true,
	"temperature":        true,
	"top_p":              true,
	"top_k":              true,
	"tools":              true,
	"tool_choice":        true,
	"thinking":           true,
	"output_config":      true,
	"container":          true,
	"mcp_servers":        true,
	"service_tier":       true,
	"speed":              true,
	"context_management": true,
	"betas":              true,
}

// ValidateMessagesRequest checks body against strict Anthropic Messages request
// rules under opts and returns all detected violations in scan order.
//
// Every assistant tool_use block must match a leading tool_result block in the
// immediately following user message, roles must alternate between user and
// assistant without empty text blocks, and thinking.budget_tokens must stay
// strictly below max_tokens.
//
// Setting opts.StrictTopLevel rejects unknown root fields, and including
// "passback" in opts.DialectRules permits unsigned historical thinking blocks.
func ValidateMessagesRequest(body []byte, opts RequestValidationOptions) []RuleViolation {
	var violations []RuleViolation
	add := func(ruleID, path, msg string) {
		violations = append(violations, RuleViolation{
			RuleID:    ruleID,
			Protocol:  protocol.ProtocolAnthropicMessages,
			Path:      path,
			Message:   msg,
			Reference: ruleReference(ruleID),
		})
	}

	rootNode, err := parseRequestJSONRoot(body)
	if err != nil {
		add("MSG-JSON", "$", err.Error())
		return violations
	}
	root := rootNode.objVal

	if opts.StrictTopLevel {
		for _, k := range orderedNodeKeys(rootNode) {
			if !messagesAllowedTopLevelKeys[k] {
				add("MSG-STRICT-TOP-LEVEL", appendJSONMemberPath("$", k), fmt.Sprintf("unknown top-level Messages field %q", k))
			}
		}
	}

	if model, _, _ := nodeStringField(root, "model"); model == "" {
		add("MSG-JSON", "$.model", "model must be a non-empty string")
	}

	maxTokens, hasMax, okMax := nodeIntField(root, "max_tokens")
	if !hasMax || !okMax || maxTokens <= 0 {
		add("MSG-MAX-TOKENS", "$.max_tokens", "max_tokens must be a positive integer")
	}

	thinkingEnabled := false
	if thObj, hasTh, okTh := nodeObjectField(root, "thinking"); hasTh {
		if !okTh {
			add("MSG-THINKING", "$.thinking", "thinking must be a JSON object")
		} else {
			thType, _, _ := nodeStringField(thObj, "type")
			switch thType {
			case "enabled":
				thinkingEnabled = true
				budget, hasB, okB := nodeIntField(thObj, "budget_tokens")
				if !hasB || !okB || budget <= 0 {
					add("MSG-MAX-TOKENS", "$.thinking.budget_tokens", "thinking.budget_tokens must be a positive integer when thinking.type is \"enabled\"")
				} else if hasMax && okMax && maxTokens > 0 && budget >= maxTokens {
					add("MSG-MAX-TOKENS", "$.thinking.budget_tokens", fmt.Sprintf("thinking.budget_tokens (%d) must be strictly less than max_tokens (%d)", budget, maxTokens))
				}
			case "adaptive":
				thinkingEnabled = true
			case "disabled", "":
			default:
				add("MSG-THINKING", "$.thinking.type", fmt.Sprintf("unrecognized thinking.type %q", thType))
			}
		}
	}

	if ocObj, hasOC, okOC := nodeObjectField(root, "output_config"); hasOC {
		if !okOC {
			add("MSG-JSON", "$.output_config", "output_config must be a JSON object")
		} else if effort, hasEff, okEff := nodeStringField(ocObj, "effort"); hasEff {
			if !okEff {
				add("MSG-THINKING", "$.output_config.effort", "output_config.effort must be a string")
			} else {
				switch effort {
				case "low", "medium", "high", "xhigh", "max":
				default:
					add("MSG-THINKING", "$.output_config.effort", fmt.Sprintf("unsupported output_config.effort %q (want low, medium, high, xhigh, or max)", effort))
				}
			}
		}
	}

	validateMessagesSystem(root, add)
	validateMessagesTools(root, opts, add)

	msgs, hasMsgs, okMsgs := nodeArrayField(root, "messages")
	if !hasMsgs || !okMsgs || len(msgs) == 0 {
		add("MSG-JSON", "$.messages", "messages must be a non-empty array")
		return violations
	}

	passback := opts.HasDialect("passback")
	prevRole := ""
	var pendingToolUseIDs []string
	pendingFromMsg := -1
	lastAssistantToolUseMsg := -1
	lastAssistantStartsWithThinking := false

	for i, mNode := range msgs {
		mPath := fmt.Sprintf("$.messages[%d]", i)
		if mNode.kind != nodeObject {
			add("MSG-JSON", mPath, "message must be a JSON object")
			continue
		}
		mObj := mNode.objVal
		role, _, _ := nodeStringField(mObj, "role")
		if role != "user" && role != "assistant" {
			add("MSG-ROLES", mPath+".role", fmt.Sprintf("messages[%d].role = %q, want \"user\" or \"assistant\"", i, role))
		} else if role == prevRole {
			add("MSG-ROLES", mPath+".role", fmt.Sprintf("consecutive %q messages at %d and %d must be merged", role, i-1, i))
		}
		prevRole = role

		toolUses, toolResults, startsWithThinking := inspectMessagesContent(i, role, mObj, opts, passback, thinkingEnabled, add)
		if role == "assistant" && len(toolUses) > 0 {
			lastAssistantToolUseMsg = i
			lastAssistantStartsWithThinking = startsWithThinking
		}

		if len(pendingToolUseIDs) > 0 {
			if role != "user" {
				add("MSG-TOOL-PAIRING", mPath, fmt.Sprintf("assistant tool_use in messages[%d] must be immediately followed by a user message with tool_result blocks", pendingFromMsg))
			} else {
				verifyMessagesToolPairing(pendingFromMsg, i, pendingToolUseIDs, toolResults, add)
			}
			pendingToolUseIDs = nil
			pendingFromMsg = -1
		} else if len(toolResults) > 0 {
			add("MSG-TOOL-PAIRING", mPath+".content", fmt.Sprintf("user messages[%d] contains tool_result %v with no preceding assistant tool_use", i, toolResults))
		}

		if role == "assistant" && len(toolUses) > 0 {
			pendingToolUseIDs = toolUses
			pendingFromMsg = i
		}
	}

	if len(pendingToolUseIDs) > 0 {
		add("MSG-TOOL-PAIRING", fmt.Sprintf("$.messages[%d].content", pendingFromMsg), fmt.Sprintf("assistant tool_use %v at end of messages has no following user tool_result", pendingToolUseIDs))
	}

	// When extended thinking is enabled and the final assistant message in an
	// active tool-use loop invokes tools (followed only by user tool_result),
	// Anthropic requires that assistant message to begin with thinking or redacted_thinking.
	if thinkingEnabled && !passback && lastAssistantToolUseMsg >= 0 && lastAssistantToolUseMsg == len(msgs)-2 && !lastAssistantStartsWithThinking {
		add("MSG-THINKING", fmt.Sprintf("$.messages[%d].content[0]", lastAssistantToolUseMsg), "when thinking is enabled, the latest assistant tool_use turn must start with a thinking or redacted_thinking block")
	}

	return violations
}

// validateMessagesSystem checks top-level system string or block array for empty text blocks.
func validateMessagesSystem(root map[string]jsonNode, add func(string, string, string)) {
	sysVal, hasSys := root["system"]
	if !hasSys || sysVal.kind == nodeNull {
		return
	}
	if sysVal.kind == nodeString {
		if sysVal.strVal == "" {
			add("MSG-EMPTY-TEXT", "$.system", "system string must not be empty when present")
		}
		return
	}
	if sysVal.kind != nodeArray {
		add("MSG-JSON", "$.system", "system must be a string or array of text blocks")
		return
	}
	for i, blk := range sysVal.arrVal {
		bPath := fmt.Sprintf("$.system[%d]", i)
		if blk.kind != nodeObject {
			add("MSG-JSON", bPath, "system block must be an object")
			continue
		}
		bType, _, _ := nodeStringField(blk.objVal, "type")
		if bType == "text" {
			if txt, _, _ := nodeStringField(blk.objVal, "text"); txt == "" {
				add("MSG-EMPTY-TEXT", bPath+".text", "system text block must not be empty")
			}
		}
	}
}

// validateMessagesTools checks tool names and input_schema objects on tools[].
func validateMessagesTools(root map[string]jsonNode, opts RequestValidationOptions, add func(string, string, string)) {
	toolsArr, hasTools, okTools := nodeArrayField(root, "tools")
	if !hasTools {
		return
	}
	if !okTools {
		add("MSG-JSON", "$.tools", "tools must be an array")
		return
	}
	maxLen := 128
	if opts.ToolNameMaxBytes > 0 {
		maxLen = opts.ToolNameMaxBytes
	}
	for i, tNode := range toolsArr {
		tPath := fmt.Sprintf("$.tools[%d]", i)
		if tNode.kind != nodeObject {
			add("MSG-JSON", tPath, "tool declaration must be an object")
			continue
		}
		tObj := tNode.objVal
		tType, _, _ := nodeStringField(tObj, "type")
		if tType != "" && tType != "custom" {
			// Anthropic server tools such as web_search_20250305 omit user-defined
			// input_schema objects and carry provider-versioned type tags instead.
			continue
		}
		name, _, _ := nodeStringField(tObj, "name")
		if !messagesToolNamePattern.MatchString(name) || len(name) > maxLen {
			add("MSG-TOOL-NAME", tPath+".name", fmt.Sprintf("tool name %q must match ^[a-zA-Z0-9_-]{1,%d}$", name, maxLen))
		}
		if _, hasSchema, okSchema := nodeObjectField(tObj, "input_schema"); !hasSchema || !okSchema {
			add("MSG-TOOL-NAME", tPath+".input_schema", "function tool requires an object input_schema")
		}
	}
}

// inspectMessagesContent validates blocks inside messages[msgIdx] and returns
// ordered tool_use IDs, tool_result IDs, and whether block 0 is thinking.
func inspectMessagesContent(msgIdx int, role string, mObj map[string]jsonNode, opts RequestValidationOptions, passback, _ bool, add func(string, string, string)) ([]string, []string, bool) {
	cPath := fmt.Sprintf("$.messages[%d].content", msgIdx)
	cVal, hasC := mObj["content"]
	if !hasC {
		add("MSG-EMPTY-TEXT", cPath, "message content is required")
		return nil, nil, false
	}
	if cVal.kind == nodeString {
		if cVal.strVal == "" {
			add("MSG-EMPTY-TEXT", cPath, "message string content must not be empty")
		}
		return nil, nil, false
	}
	if cVal.kind != nodeArray || len(cVal.arrVal) == 0 {
		add("MSG-EMPTY-TEXT", cPath, "message content array must be non-empty")
		return nil, nil, false
	}

	maxNameLen := 128
	if opts.ToolNameMaxBytes > 0 {
		maxNameLen = opts.ToolNameMaxBytes
	}

	var toolUses []string
	var toolResults []string
	seenToolUse := make(map[string]bool)
	sawNonToolResultInUser := false
	sawToolUseInAssistant := false
	startsWithThinking := false

	for bIdx, bNode := range cVal.arrVal {
		bPath := fmt.Sprintf("%s[%d]", cPath, bIdx)
		if bNode.kind != nodeObject {
			add("MSG-JSON", bPath, "content block must be an object")
			continue
		}
		bObj := bNode.objVal
		bType, _, _ := nodeStringField(bObj, "type")
		if bIdx == 0 && (bType == "thinking" || bType == "redacted_thinking") {
			startsWithThinking = true
		}

		switch bType {
		case "text":
			if role == "user" {
				sawNonToolResultInUser = true
			}
			if txt, _, _ := nodeStringField(bObj, "text"); txt == "" {
				add("MSG-EMPTY-TEXT", bPath+".text", "text block must not have empty text")
			}

		case "thinking":
			if role != "assistant" {
				add("MSG-THINKING", bPath, "thinking block may only appear in assistant messages")
			}
			if sawToolUseInAssistant {
				add("MSG-THINKING", bPath, "thinking block must precede tool_use blocks in the same assistant message")
			}
			sig, _, _ := nodeStringField(bObj, "signature")
			if sig == "" && !passback {
				add("MSG-THINKING", bPath+".signature", "thinking block requires a non-empty signature")
			}

		case "redacted_thinking":
			if role != "assistant" {
				add("MSG-THINKING", bPath, "redacted_thinking block may only appear in assistant messages")
			}
			if sawToolUseInAssistant {
				add("MSG-THINKING", bPath, "redacted_thinking block must precede tool_use blocks in the same assistant message")
			}
			if dataStr, _, _ := nodeStringField(bObj, "data"); dataStr == "" {
				add("MSG-THINKING", bPath+".data", "redacted_thinking block requires non-empty data")
			}

		case "tool_use":
			if role != "assistant" {
				add("MSG-TOOL-PAIRING", bPath, "tool_use block may only appear in assistant messages")
			}
			sawToolUseInAssistant = true
			id, _, _ := nodeStringField(bObj, "id")
			if id == "" {
				add("MSG-TOOL-PAIRING", bPath+".id", "tool_use block requires non-empty id")
			} else if seenToolUse[id] {
				add("MSG-TOOL-PAIRING", bPath+".id", fmt.Sprintf("duplicate tool_use id %q", id))
			} else {
				seenToolUse[id] = true
				toolUses = append(toolUses, id)
			}
			name, _, _ := nodeStringField(bObj, "name")
			if !messagesToolNamePattern.MatchString(name) || len(name) > maxNameLen {
				add("MSG-TOOL-NAME", bPath+".name", fmt.Sprintf("tool_use name %q must match ^[a-zA-Z0-9_-]{1,%d}$", name, maxNameLen))
			}
			if _, hasIn, okIn := nodeObjectField(bObj, "input"); !hasIn || !okIn {
				add("MSG-TOOL-PAIRING", bPath+".input", "tool_use.input must be a JSON object")
			}

		case "tool_result":
			if role != "user" {
				add("MSG-TOOL-PAIRING", bPath, "tool_result block may only appear in user messages")
			}
			if sawNonToolResultInUser {
				add("MSG-TOOL-RESULT-ORDER", bPath, "tool_result block must precede all non-tool_result blocks in user content")
			}
			useID, _, _ := nodeStringField(bObj, "tool_use_id")
			if useID == "" {
				add("MSG-TOOL-PAIRING", bPath+".tool_use_id", "tool_result requires non-empty tool_use_id")
			} else {
				toolResults = append(toolResults, useID)
			}
			validateMessagesToolResultInner(bPath, bObj, add)

		default:
			if role == "user" {
				sawNonToolResultInUser = true
			}
		}
	}
	return toolUses, toolResults, startsWithThinking
}

// validateMessagesToolResultInner rejects empty text blocks nested inside a tool_result content array.
func validateMessagesToolResultInner(basePath string, bObj map[string]jsonNode, add func(string, string, string)) {
	inner, hasInner := bObj["content"]
	if !hasInner || inner.kind != nodeArray {
		return
	}
	for i, item := range inner.arrVal {
		if item.kind != nodeObject {
			continue
		}
		if iType, _, _ := nodeStringField(item.objVal, "type"); iType == "text" {
			if txt, _, _ := nodeStringField(item.objVal, "text"); txt == "" {
				add("MSG-EMPTY-TEXT", fmt.Sprintf("%s.content[%d].text", basePath, i), "nested tool_result text block must not be empty")
			}
		}
	}
}

// verifyMessagesToolPairing checks exact bijection between assistant tool_use IDs
// and the immediately following user message's tool_result IDs.
func verifyMessagesToolPairing(asstIdx, userIdx int, calls, results []string, add func(string, string, string)) {
	want := make(map[string]bool, len(calls))
	for _, id := range calls {
		want[id] = true
	}
	seenResult := make(map[string]bool, len(results))
	userPath := fmt.Sprintf("$.messages[%d].content", userIdx)
	for _, resID := range results {
		if !want[resID] {
			add("MSG-TOOL-PAIRING", userPath, fmt.Sprintf("tool_result tool_use_id %q does not match any tool_use in messages[%d]", resID, asstIdx))
			continue
		}
		if seenResult[resID] {
			add("MSG-TOOL-PAIRING", userPath, fmt.Sprintf("duplicate tool_result for tool_use_id %q", resID))
			continue
		}
		seenResult[resID] = true
	}
	for _, id := range calls {
		if !seenResult[id] {
			add("MSG-TOOL-PAIRING", userPath, fmt.Sprintf("missing tool_result in messages[%d] for tool_use id %q from messages[%d]", userIdx, id, asstIdx))
		}
	}
}
