package testkit

import (
	"fmt"
	"slices"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// request_rules_chat.go validates OpenAI Chat Completions request JSON payloads
// against contiguous assistant-to-tool pairing, non-empty assistant turns,
// JSON-valid tool call arguments, function name syntax, and the deepseek-thinking
// reasoning_content requirement on tool-calling assistant messages.
//
// Chat Completions requires every tool_calls[].id on an assistant turn to be
// answered by contiguous role="tool" messages before the next user, assistant,
// or system turn, rejects orphan tool results without an active assistant call
// group, and under deepseek-thinking rejects active-turn unmerged commentary or
// tool-call history turns that omit string reasoning_content fields on the
// assistant message (b567a28f9).

// ValidateChatRequest checks body against strict OpenAI Chat Completions request
// rules under opts and returns all detected violations in scan order.
//
// Assistant messages must carry non-empty content, refusal, tool_calls, or
// function_call fields, every tool call's arguments string must parse as JSON,
// and contiguous role="tool" messages must satisfy all pending tool_calls IDs.
//
// Including "deepseek-thinking" in opts.DialectRules requires string
// reasoning_content on every assistant message that contains tool_calls, forbids
// reasoning_content on completed non-tool assistant turns, and rejects split
// active-turn assistant commentary messages ahead of tool_calls.
func ValidateChatRequest(body []byte, opts RequestValidationOptions) []RuleViolation {
	var violations []RuleViolation
	add := func(ruleID, path, msg string) {
		violations = append(violations, RuleViolation{
			RuleID:    ruleID,
			Protocol:  protocol.ProtocolOpenAIChatCompletions,
			Path:      path,
			Message:   msg,
			Reference: ruleReference(ruleID),
		})
	}

	root, err := parseRequestJSONObject(body)
	if err != nil {
		add("CHAT-JSON", "$", err.Error())
		return violations
	}

	if model, _, _ := nodeStringField(root, "model"); model == "" {
		add("CHAT-JSON", "$.model", "model must be a non-empty string")
	}

	maxNameLen := 64
	if opts.ToolNameMaxBytes > 0 && opts.ToolNameMaxBytes < maxNameLen {
		maxNameLen = opts.ToolNameMaxBytes
	}
	validateChatDeclaredTools(root, maxNameLen, add)

	msgs, hasMsgs, okMsgs := nodeArrayField(root, "messages")
	if !hasMsgs || !okMsgs || len(msgs) == 0 {
		add("CHAT-JSON", "$.messages", "messages must be a non-empty array")
		return violations
	}

	deepseekThinking := opts.HasDialect("deepseek-thinking")
	lastUserIdx := -1
	for i, mNode := range msgs {
		if mNode.kind == nodeObject {
			if role, _, _ := nodeStringField(mNode.objVal, "role"); role == "user" {
				lastUserIdx = i
			}
		}
	}
	var pendingCalls []string
	satisfiedCalls := make(map[string]bool)
	pendingAssistantIdx := -1

	flushPendingCalls := func(boundaryPath string) {
		if len(pendingCalls) == 0 {
			return
		}
		for _, id := range pendingCalls {
			if !satisfiedCalls[id] {
				add("CHAT-TOOL-PAIRING", boundaryPath, fmt.Sprintf("missing role=\"tool\" result for tool_call id %q from messages[%d] before next turn", id, pendingAssistantIdx))
			}
		}
		pendingCalls = nil
		satisfiedCalls = make(map[string]bool)
		pendingAssistantIdx = -1
	}

	for i, mNode := range msgs {
		mPath := fmt.Sprintf("$.messages[%d]", i)
		if mNode.kind != nodeObject {
			add("CHAT-JSON", mPath, "message must be a JSON object")
			continue
		}
		mObj := mNode.objVal
		role, _, _ := nodeStringField(mObj, "role")

		if role == "tool" {
			callID, _, _ := nodeStringField(mObj, "tool_call_id")
			if len(pendingCalls) == 0 {
				add("CHAT-TOOL-PAIRING", mPath, fmt.Sprintf("orphan tool message (tool_call_id %q) has no preceding assistant tool_calls turn", callID))
				continue
			}
			if callID == "" {
				add("CHAT-TOOL-PAIRING", mPath+".tool_call_id", "tool message requires non-empty tool_call_id")
				continue
			}
			if !slices.Contains(pendingCalls, callID) {
				add("CHAT-TOOL-PAIRING", mPath+".tool_call_id", fmt.Sprintf("tool_call_id %q does not match any tool_call in messages[%d]", callID, pendingAssistantIdx))
			} else if satisfiedCalls[callID] {
				add("CHAT-TOOL-PAIRING", mPath+".tool_call_id", fmt.Sprintf("duplicate tool message for tool_call_id %q", callID))
			} else {
				satisfiedCalls[callID] = true
			}
			continue
		}

		flushPendingCalls(mPath)

		if role == "assistant" {
			callIDs := validateChatAssistantMessage(i, mObj, maxNameLen, deepseekThinking, add)
			if len(callIDs) > 0 {
				pendingCalls = callIDs
				pendingAssistantIdx = i
			} else if deepseekThinking && i > lastUserIdx && chatHasLaterActiveToolStep(msgs[i+1:]) {
				if _, hasFC, okFC := nodeObjectField(mObj, "function_call"); !hasFC || !okFC {
					add("CHAT-DEEPSEEK-THINKING", mPath, "deepseek-thinking requires active-turn assistant commentary preceding tool_calls to coalesce into the tool-calling assistant message with reasoning_content")
				}
			}
		}
	}

	flushPendingCalls(fmt.Sprintf("$.messages[%d]", len(msgs)-1))
	return violations
}

// validateChatDeclaredTools checks function name syntax across tools[] and legacy functions[] arrays.
func validateChatDeclaredTools(root map[string]jsonNode, maxNameLen int, add func(string, string, string)) {
	if toolsArr, hasTools, okTools := nodeArrayField(root, "tools"); hasTools {
		if !okTools {
			add("CHAT-JSON", "$.tools", "tools must be an array")
		} else {
			for i, tNode := range toolsArr {
				tPath := fmt.Sprintf("$.tools[%d]", i)
				if tNode.kind != nodeObject {
					add("CHAT-JSON", tPath, "tool must be an object")
					continue
				}
				fnObj, hasFn, okFn := nodeObjectField(tNode.objVal, "function")
				if !hasFn || !okFn {
					continue
				}
				name, _, _ := nodeStringField(fnObj, "name")
				if !openAIToolNamePattern.MatchString(name) || len(name) > maxNameLen {
					add("CHAT-TOOL-NAME", tPath+".function.name", fmt.Sprintf("function name %q must match ^[a-zA-Z0-9_-]{1,%d}$", name, maxNameLen))
				}
			}
		}
	}

	if fnArr, hasFns, okFns := nodeArrayField(root, "functions"); hasFns && okFns {
		for i, fNode := range fnArr {
			if fNode.kind != nodeObject {
				continue
			}
			name, _, _ := nodeStringField(fNode.objVal, "name")
			if !openAIToolNamePattern.MatchString(name) || len(name) > maxNameLen {
				add("CHAT-TOOL-NAME", fmt.Sprintf("$.functions[%d].name", i), fmt.Sprintf("function name %q must match ^[a-zA-Z0-9_-]{1,%d}$", name, maxNameLen))
			}
		}
	}
}

// validateChatAssistantMessage checks body presence, tool_calls names/arguments,
// and deepseek-thinking reasoning_content on an assistant message, returning its tool_call IDs.
func validateChatAssistantMessage(msgIdx int, mObj map[string]jsonNode, maxNameLen int, deepseekThinking bool, add func(string, string, string)) []string {
	mPath := fmt.Sprintf("$.messages[%d]", msgIdx)

	hasBody := false
	if cVal, exists := mObj["content"]; exists {
		if cVal.kind == nodeString && cVal.strVal != "" {
			hasBody = true
		} else if cVal.kind == nodeArray && len(cVal.arrVal) > 0 {
			hasBody = true
		}
	}
	if ref, hasRef, okRef := nodeStringField(mObj, "refusal"); hasRef && okRef && ref != "" {
		hasBody = true
	}

	hasLegacyFC := false
	if fcObj, hasFC, okFC := nodeObjectField(mObj, "function_call"); hasFC && okFC {
		hasLegacyFC = true
		hasBody = true
		if argsStr, hasArgs, okArgs := nodeStringField(fcObj, "arguments"); !hasArgs || !okArgs {
			add("CHAT-TOOL-ARGS-JSON", mPath+".function_call.arguments", "function_call.arguments must be a JSON string")
		} else if _, err := parseJSONNode([]byte(argsStr), true); err != nil {
			add("CHAT-TOOL-ARGS-JSON", mPath+".function_call.arguments", fmt.Sprintf("function_call.arguments is not valid JSON: %v", err))
		}
	}

	var callIDs []string
	seenID := make(map[string]bool)
	hasToolCalls := false
	if tcArr, hasTC, okTC := nodeArrayField(mObj, "tool_calls"); hasTC {
		if !okTC {
			add("CHAT-JSON", mPath+".tool_calls", "tool_calls must be an array")
		} else if len(tcArr) > 0 {
			hasToolCalls = true
			hasBody = true
			if deepseekThinking {
				if _, hasRC, okRC := nodeStringField(mObj, "reasoning_content"); !hasRC || !okRC {
					add("CHAT-DEEPSEEK-THINKING", mPath+".reasoning_content", "deepseek-thinking requires assistant messages with tool_calls to include a string reasoning_content field")
				}
			}
			for j, tcNode := range tcArr {
				tcPath := fmt.Sprintf("%s.tool_calls[%d]", mPath, j)
				if tcNode.kind != nodeObject {
					add("CHAT-JSON", tcPath, "tool_call must be an object")
					continue
				}
				tcObj := tcNode.objVal
				id, _, _ := nodeStringField(tcObj, "id")
				if id == "" {
					add("CHAT-TOOL-PAIRING", tcPath+".id", "tool_call requires non-empty id")
				} else if seenID[id] {
					add("CHAT-TOOL-PAIRING", tcPath+".id", fmt.Sprintf("duplicate tool_call id %q", id))
				} else {
					seenID[id] = true
					callIDs = append(callIDs, id)
				}
				if typ, _, _ := nodeStringField(tcObj, "type"); typ != "function" {
					add("CHAT-TOOL-PAIRING", tcPath+".type", fmt.Sprintf("tool_call.type = %q, want \"function\"", typ))
				}
				fnObj, hasFn, okFn := nodeObjectField(tcObj, "function")
				if !hasFn || !okFn {
					add("CHAT-TOOL-ARGS-JSON", tcPath+".function", "tool_call requires a function object")
					continue
				}
				name, _, _ := nodeStringField(fnObj, "name")
				if !openAIToolNamePattern.MatchString(name) || len(name) > maxNameLen {
					add("CHAT-TOOL-NAME", tcPath+".function.name", fmt.Sprintf("tool_call function.name %q must match ^[a-zA-Z0-9_-]{1,%d}$", name, maxNameLen))
				}
				argsStr, hasArgs, okArgs := nodeStringField(fnObj, "arguments")
				if !hasArgs || !okArgs {
					add("CHAT-TOOL-ARGS-JSON", tcPath+".function.arguments", "function.arguments must be a string")
				} else if _, err := parseJSONNode([]byte(argsStr), true); err != nil {
					add("CHAT-TOOL-ARGS-JSON", tcPath+".function.arguments", fmt.Sprintf("function.arguments is not valid JSON: %v", err))
				}
			}
		}
	}

	if deepseekThinking && !hasToolCalls && !hasLegacyFC {
		if rcNode, hasRC := mObj["reasoning_content"]; hasRC && rcNode.kind != nodeNull {
			add("CHAT-DEEPSEEK-THINKING", mPath+".reasoning_content", "deepseek-thinking forbids reasoning_content on completed assistant messages without tool_calls")
		}
	}

	if !hasBody {
		add("CHAT-ASSISTANT-BODY", mPath, "assistant message must have non-empty content, refusal, tool_calls, or function_call")
	}
	return callIDs
}

// chatHasLaterActiveToolStep reports whether any subsequent message in tail
// carries assistant tool_calls, legacy function_call, or role="tool" within the
// current user turn. Scanning stops at the next role="user" turn boundary.
func chatHasLaterActiveToolStep(tail []jsonNode) bool {
	for _, mNode := range tail {
		if mNode.kind != nodeObject {
			continue
		}
		mObj := mNode.objVal
		role, _, _ := nodeStringField(mObj, "role")
		if role == "user" {
			return false
		}
		if role == "tool" {
			return true
		}
		if role == "assistant" {
			if tcArr, hasTC, okTC := nodeArrayField(mObj, "tool_calls"); hasTC && okTC && len(tcArr) > 0 {
				return true
			}
			if _, hasFC, okFC := nodeObjectField(mObj, "function_call"); hasFC && okFC {
				return true
			}
		}
	}
	return false
}
