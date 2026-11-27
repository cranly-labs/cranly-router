package testkit

import (
	"fmt"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// request_rules_responses.go validates OpenAI Responses request JSON payloads
// against call/output pairing invariants, reasoning follower invariants, tool
// name character and length limits, JSON-encoded function_call arguments, and
// function-only dialect restrictions.
//
// Responses encodes conversation history as a flat input item list where every
// call item must pair by call_id and kind with a later output item and every
// reasoning item must precede its associated assistant message or tool call
// rather than dangling before a user or tool output item. Function-only
// dialects additionally require custom, namespace, and tool_search items to be
// lowered into standard function declarations and calls.

// responsesCallRecord tracks one call item encountered in input[] for pairing checks.
type responsesCallRecord struct {
	kind    string // function_call, custom_tool_call, or tool_search_call
	index   int    // position inside input[]
	matched bool   // whether a matching *_output item appeared
}

// ValidateResponsesRequest checks body against strict OpenAI Responses request
// rules under opts and returns all detected violations in scan order.
//
// Array inputs require exact bijective call_id pairing across function_call,
// custom_tool_call, and tool_search_call items, valid JSON strings on
// function_call.arguments, and an assistant follower after every reasoning item.
//
// Including "function-only" in opts.DialectRules rejects unlowered custom,
// namespace, and tool_search declarations and history items.
func ValidateResponsesRequest(body []byte, opts RequestValidationOptions) []RuleViolation {
	var violations []RuleViolation
	add := func(ruleID, path, msg string) {
		violations = append(violations, RuleViolation{
			RuleID:    ruleID,
			Protocol:  protocol.ProtocolOpenAIResponses,
			Path:      path,
			Message:   msg,
			Reference: ruleReference(ruleID),
		})
	}

	root, err := parseRequestJSONObject(body)
	if err != nil {
		add("RSP-JSON", "$", err.Error())
		return violations
	}

	if model, _, _ := nodeStringField(root, "model"); model == "" {
		add("RSP-JSON", "$.model", "model must be a non-empty string")
	}

	functionOnly := opts.HasDialect("function-only")
	maxNameLen := 64
	if opts.ToolNameMaxBytes > 0 && opts.ToolNameMaxBytes < maxNameLen {
		maxNameLen = opts.ToolNameMaxBytes
	}

	validateResponsesTools(root, functionOnly, maxNameLen, add)

	inVal, hasIn := root["input"]
	if !hasIn || inVal.kind == nodeNull {
		add("RSP-JSON", "$.input", "input must be a string or array")
		return violations
	}
	if inVal.kind == nodeString {
		return violations
	}
	if inVal.kind != nodeArray {
		add("RSP-JSON", "$.input", "input must be a string or array of items")
		return violations
	}

	validateResponsesInputItems(inVal.arrVal, functionOnly, maxNameLen, add)
	return violations
}

// validateResponsesTools validates tools[] declarations and nested namespace child tools.
func validateResponsesTools(root map[string]jsonNode, functionOnly bool, maxNameLen int, add func(string, string, string)) {
	toolsArr, hasTools, okTools := nodeArrayField(root, "tools")
	if !hasTools {
		return
	}
	if !okTools {
		add("RSP-JSON", "$.tools", "tools must be an array")
		return
	}
	for i, tNode := range toolsArr {
		tPath := fmt.Sprintf("$.tools[%d]", i)
		if tNode.kind != nodeObject {
			add("RSP-JSON", tPath, "tool declaration must be an object")
			continue
		}
		tObj := tNode.objVal
		tType, _, _ := nodeStringField(tObj, "type")
		switch tType {
		case "function":
			name, _, _ := nodeStringField(tObj, "name")
			if !openAIToolNamePattern.MatchString(name) || len(name) > maxNameLen {
				add("RSP-TOOL-NAME", tPath+".name", fmt.Sprintf("function tool name %q must match ^[a-zA-Z0-9_-]{1,%d}$", name, maxNameLen))
			}
		case "custom":
			if functionOnly {
				add("RSP-DIALECT-FUNCTION-ONLY", tPath+".type", "custom tool declaration must be lowered for function-only target")
			}
			name, _, _ := nodeStringField(tObj, "name")
			if !openAIToolNamePattern.MatchString(name) || len(name) > maxNameLen {
				add("RSP-TOOL-NAME", tPath+".name", fmt.Sprintf("custom tool name %q must match ^[a-zA-Z0-9_-]{1,%d}$", name, maxNameLen))
			}
		case "namespace":
			if functionOnly {
				add("RSP-DIALECT-FUNCTION-ONLY", tPath+".type", "namespace tool declaration must be flattened for function-only target")
			}
			children, hasCh, okCh := nodeArrayField(tObj, "tools")
			if !hasCh {
				children, hasCh, okCh = nodeArrayField(tObj, "children")
			}
			if hasCh && okCh {
				for cIdx, chNode := range children {
					if chNode.kind != nodeObject {
						continue
					}
					cName, _, _ := nodeStringField(chNode.objVal, "name")
					if !openAIToolNamePattern.MatchString(cName) || len(cName) > maxNameLen {
						add("RSP-TOOL-NAME", fmt.Sprintf("%s.tools[%d].name", tPath, cIdx), fmt.Sprintf("namespaced tool name %q must match ^[a-zA-Z0-9_-]{1,%d}$", cName, maxNameLen))
					}
				}
			}
		case "tool_search":
			if functionOnly {
				add("RSP-DIALECT-FUNCTION-ONLY", tPath+".type", "tool_search declaration must be lowered for function-only target")
			}
		}
	}
}

// validateResponsesInputItems checks call/output pairing, reasoning follower
// invariants, tool call names/arguments, and function-only dialect items.
func validateResponsesInputItems(items []jsonNode, functionOnly bool, maxNameLen int, add func(string, string, string)) {
	calls := make(map[string]*responsesCallRecord)
	var callOrder []string
	seenOutputs := make(map[string]int)

	for i, itemNode := range items {
		iPath := fmt.Sprintf("$.input[%d]", i)
		if itemNode.kind != nodeObject {
			add("RSP-JSON", iPath, "input item must be a JSON object")
			continue
		}
		iObj := itemNode.objVal
		iType, _, _ := nodeStringField(iObj, "type")
		role, _, _ := nodeStringField(iObj, "role")
		if iType == "" && role != "" {
			iType = "message"
		}

		switch iType {
		case "reasoning":
			if !hasAssociatedAssistantFollower(items, i) {
				add("RSP-REASONING-FOLLOWER", iPath, "reasoning item must be followed by an associated assistant output or tool call item")
			}

		case "function_call", "custom_tool_call", "tool_search_call":
			if functionOnly && iType != "function_call" {
				add("RSP-DIALECT-FUNCTION-ONLY", iPath+".type", fmt.Sprintf("%s item must be lowered for function-only target", iType))
			}
			if functionOnly && iType == "function_call" {
				if ns, _, _ := nodeStringField(iObj, "namespace"); ns != "" {
					add("RSP-DIALECT-FUNCTION-ONLY", iPath+".namespace", "function_call.namespace must be flattened for function-only target")
				}
			}
			callID, _, _ := nodeStringField(iObj, "call_id")
			if callID == "" {
				add("RSP-CALL-PAIRING", iPath+".call_id", fmt.Sprintf("%s requires non-empty call_id", iType))
			} else if prev, dup := calls[callID]; dup {
				add("RSP-CALL-PAIRING", iPath+".call_id", fmt.Sprintf("duplicate call_id %q (already used at input[%d])", callID, prev.index))
			} else {
				calls[callID] = &responsesCallRecord{kind: iType, index: i}
				callOrder = append(callOrder, callID)
			}

			if iType == "function_call" || iType == "custom_tool_call" {
				name, _, _ := nodeStringField(iObj, "name")
				if !openAIToolNamePattern.MatchString(name) || len(name) > maxNameLen {
					add("RSP-TOOL-NAME", iPath+".name", fmt.Sprintf("%s name %q must match ^[a-zA-Z0-9_-]{1,%d}$", iType, name, maxNameLen))
				}
			}
			if iType == "function_call" {
				argsStr, hasArgs, okArgs := nodeStringField(iObj, "arguments")
				if !hasArgs || !okArgs {
					add("RSP-TOOL-NAME", iPath+".arguments", "function_call.arguments must be a JSON string")
				} else if _, err := parseJSONNode([]byte(argsStr), true); err != nil {
					add("RSP-TOOL-NAME", iPath+".arguments", fmt.Sprintf("function_call.arguments is not valid JSON: %v", err))
				}
			}

		case "function_call_output", "custom_tool_call_output", "tool_search_output":
			if functionOnly && iType != "function_call_output" {
				add("RSP-DIALECT-FUNCTION-ONLY", iPath+".type", fmt.Sprintf("%s item must be lowered for function-only target", iType))
			}
			callID, _, _ := nodeStringField(iObj, "call_id")
			if callID == "" {
				add("RSP-CALL-PAIRING", iPath+".call_id", fmt.Sprintf("%s requires non-empty call_id", iType))
				continue
			}
			if prevIdx, dup := seenOutputs[callID]; dup {
				add("RSP-CALL-PAIRING", iPath+".call_id", fmt.Sprintf("duplicate output for call_id %q (already seen at input[%d])", callID, prevIdx))
				continue
			}
			seenOutputs[callID] = i
			rec, exists := calls[callID]
			if !exists {
				add("RSP-CALL-PAIRING", iPath+".call_id", fmt.Sprintf("%s references call_id %q with no preceding call item", iType, callID))
				continue
			}
			wantCallKind := expectedCallKindForOutput(iType)
			if rec.kind != wantCallKind {
				add("RSP-CALL-PAIRING", iPath+".type", fmt.Sprintf("%s cannot pair with %s at input[%d]", iType, rec.kind, rec.index))
			}
			rec.matched = true
		}
	}

	for _, callID := range callOrder {
		rec := calls[callID]
		if !rec.matched {
			add("RSP-CALL-PAIRING", fmt.Sprintf("$.input[%d].call_id", rec.index), fmt.Sprintf("%s with call_id %q has no matching output item in input", rec.kind, callID))
		}
	}
}

// hasAssociatedAssistantFollower reports whether input[start+1:] reaches an
// assistant output item or tool call before hitting end-of-input or a user/tool turn.
func hasAssociatedAssistantFollower(items []jsonNode, start int) bool {
	for j := start + 1; j < len(items); j++ {
		if items[j].kind != nodeObject {
			return false
		}
		obj := items[j].objVal
		typ, _, _ := nodeStringField(obj, "type")
		role, _, _ := nodeStringField(obj, "role")
		if typ == "" && role != "" {
			typ = "message"
		}
		if typ == "reasoning" {
			continue
		}
		switch typ {
		case "message":
			return role == "assistant"
		case "function_call", "custom_tool_call", "tool_search_call",
			"web_search_call", "mcp_call", "image_generation_call", "local_shell_call":
			return true
		default:
			return false
		}
	}
	return false
}

// expectedCallKindForOutput maps a Responses *_output item type to its call item type.
func expectedCallKindForOutput(outputType string) string {
	switch outputType {
	case "function_call_output":
		return "function_call"
	case "custom_tool_call_output":
		return "custom_tool_call"
	case "tool_search_output":
		return "tool_search_call"
	default:
		return ""
	}
}
