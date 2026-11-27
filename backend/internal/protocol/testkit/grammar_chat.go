package testkit

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// grammar_chat.go validates OpenAI Chat Completions SSE streams against chunk
// envelope consistency, contiguous tool_calls index initialization, argument
// JSON validity, finish_reason ordering, and mandatory [DONE] termination.
// Client accumulators merge tool_calls chunks by numeric index and corrupt tool
// invocations when follow-up chunks overwrite function.name with empty strings.
//
// Historical regressions caught here include empty string tool names on
// follow-up argument chunks (091ec1f41), content deltas arriving after
// finish_reason, and truncated streams ending silently without a finish_reason
// chunk or terminal error frame (bd39eb589) while still allowing trailing
// choices-empty usage chunks before the final [DONE] marker.

// chatToolAcc accumulates streamed arguments for one tool_calls[index] slot.
type chatToolAcc struct {
	id   string          // initial tool call ID
	name string          // initial function name
	args strings.Builder // concatenated function.arguments chunks
}

// ValidateChatStream checks frames against the OpenAI Chat Completions SSE
// grammar and returns all detected violations in frame order.
//
// Normal completion requires consistent chunk IDs, chat.completion.chunk object
// tags, contiguous tool_calls indices with valid accumulated JSON arguments,
// exactly one finish_reason before optional usage tail chunks, and a final
// data: [DONE] sentinel.
//
// When opts.AllowAbnormal is true, a JSON error frame (optionally followed by
// [DONE]) satisfies termination and forgives truncated tool_calls JSON, whereas
// streams claiming finish_reason must still carry valid JSON arguments and
// silent EOF cuts still report missing_terminal_or_error.
func ValidateChatStream(frames []sse.Frame, opts GrammarOptions) []Violation {
	var violations []Violation
	add := func(idx int, event, rule, path, msg string) {
		violations = append(violations, Violation{
			Protocol:   protocol.ProtocolOpenAIChatCompletions,
			Rule:       rule,
			FrameIndex: idx,
			Event:      event,
			Path:       path,
			Message:    msg,
		})
	}

	if len(frames) == 0 {
		add(-1, "", "empty_stream", "", "Chat Completions stream ended without emitting any frames")
		return violations
	}

	streamID := ""
	sawFinish := false
	sawDone := false
	sawError := false
	var tools []*chatToolAcc

	for i, frame := range frames {
		trimmed := bytes.TrimSpace(frame.Data)
		if sawDone {
			add(i, frame.Event, "frame_after_done", "", "no frames may follow [DONE]")
			continue
		}
		if bytes.Equal(trimmed, []byte("[DONE]")) {
			if !sawFinish && !sawError {
				add(i, frame.Event, "done_without_finish_reason", "", "[DONE] arrived before any finish_reason chunk")
			}
			sawDone = true
			continue
		}
		if sawError {
			add(i, frame.Event, "frame_after_error", "", "only optional [DONE] may follow a Chat error frame")
			continue
		}

		obj, err := parseFrameJSONObject(trimmed)
		if err != nil {
			add(i, frame.Event, "invalid_json", "$", err.Error())
			continue
		}

		if errObj, hasErr, okErr := nodeObjectField(obj, "error"); hasErr {
			if !opts.AllowAbnormal {
				add(i, frame.Event, "unexpected_error_frame", "$.error", "error frame is invalid in normal completion mode")
			}
			if !okErr {
				add(i, frame.Event, "invalid_error_object", "$.error", "error field must be a JSON object")
			} else if msg, _, _ := nodeStringField(errObj, "message"); msg == "" {
				add(i, frame.Event, "missing_error_message", "$.error.message", "error.message must be a non-empty string")
			}
			sawError = true
			continue
		}

		id, _, _ := nodeStringField(obj, "id")
		if id == "" {
			add(i, frame.Event, "missing_chunk_id", "$.id", "chat.completion.chunk requires a non-empty id")
		} else if streamID == "" {
			streamID = id
		} else if id != streamID {
			add(i, frame.Event, "inconsistent_chunk_id", "$.id", fmt.Sprintf("chunk id %q differs from initial stream id %q", id, streamID))
		}
		if objTag, _, _ := nodeStringField(obj, "object"); objTag != "chat.completion.chunk" {
			add(i, frame.Event, "invalid_chunk_object", "$.object", fmt.Sprintf("object = %q, want \"chat.completion.chunk\"", objTag))
		}
		if _, hasCreated, okCreated := nodeIntField(obj, "created"); !hasCreated || !okCreated {
			add(i, frame.Event, "missing_chunk_created", "$.created", "chat.completion.chunk requires integer created")
		}
		if model, _, _ := nodeStringField(obj, "model"); model == "" {
			add(i, frame.Event, "missing_chunk_model", "$.model", "chat.completion.chunk requires non-empty model")
		}

		choices, hasChoices, okChoices := nodeArrayField(obj, "choices")
		if !hasChoices || !okChoices {
			add(i, frame.Event, "missing_choices", "$.choices", "chat.completion.chunk requires choices array")
			continue
		}

		if usageVal, hasUsage := obj["usage"]; hasUsage && usageVal.kind != nodeNull {
			if usageVal.kind != nodeObject {
				add(i, frame.Event, "invalid_usage_object", "$.usage", "usage must be an object or null")
			} else {
				for _, uKey := range []string{"prompt_tokens", "completion_tokens", "total_tokens"} {
					if v, has, ok := nodeIntField(usageVal.objVal, uKey); !has || !ok || v < 0 {
						add(i, frame.Event, "invalid_usage_counter", "$.usage."+uKey, fmt.Sprintf("usage.%s must be a non-negative integer", uKey))
					}
				}
			}
		}

		for cPos, cNode := range choices {
			if cNode.kind != nodeObject {
				add(i, frame.Event, "invalid_choice_element", fmt.Sprintf("$.choices[%d]", cPos), "choice must be an object")
				continue
			}
			cObj := cNode.objVal
			deltaObj, hasDelta, okDelta := nodeObjectField(cObj, "delta")
			if !hasDelta || !okDelta {
				add(i, frame.Event, "missing_choice_delta", fmt.Sprintf("$.choices[%d].delta", cPos), "choice requires a delta object")
				continue
			}

			hasDeltaPayload := hasNonEmptyDeltaContent(deltaObj)
			if sawFinish && hasDeltaPayload {
				add(i, frame.Event, "delta_after_finish_reason", fmt.Sprintf("$.choices[%d].delta", cPos), "content or tool_calls delta emitted after finish_reason")
			}

			if tcArr, hasTC, okTC := nodeArrayField(deltaObj, "tool_calls"); hasTC {
				if !okTC {
					add(i, frame.Event, "invalid_tool_calls_delta", fmt.Sprintf("$.choices[%d].delta.tool_calls", cPos), "delta.tool_calls must be an array")
				} else {
					validateChatToolCallDeltas(i, frame.Event, cPos, tcArr, &tools, add)
				}
			}

			if frVal, hasFR := cObj["finish_reason"]; hasFR && frVal.kind != nodeNull {
				if frVal.kind != nodeString || !isValidChatFinishReason(frVal.strVal) {
					add(i, frame.Event, "invalid_finish_reason", fmt.Sprintf("$.choices[%d].finish_reason", cPos), fmt.Sprintf("unrecognized finish_reason %v", renderNodeSummary(frVal)))
				}
				if sawFinish {
					add(i, frame.Event, "duplicate_finish_reason", fmt.Sprintf("$.choices[%d].finish_reason", cPos), "finish_reason must be emitted at most once")
				}
				sawFinish = true
			}
		}
	}

	// Completed Chat streams claiming a normal finish_reason must always carry
	// valid JSON tool arguments, whereas AllowAbnormal error terminations or
	// silent EOF cuts skip argument parsing (silent EOF fails missing_terminal_or_error).
	if !opts.AllowAbnormal || (!sawError && sawFinish) {
		for idx, tc := range tools {
			rawArgs := strings.TrimSpace(tc.args.String())
			if rawArgs != "" {
				if _, err := parseJSONNode([]byte(rawArgs), true); err != nil {
					add(-1, "", "invalid_tool_arguments_json", fmt.Sprintf("$.tool_calls[%d].function.arguments", idx), fmt.Sprintf("accumulated arguments for %q are not valid JSON: %v", tc.name, err))
				}
			}
		}
	}
	if !opts.AllowAbnormal {
		if !sawFinish {
			add(-1, "", "missing_finish_reason", "", "Chat stream ended without finish_reason")
		}
		if !sawDone {
			add(-1, "", "missing_done_sentinel", "", "Chat stream ended without data: [DONE]")
		}
	} else if !sawError && (!sawFinish || !sawDone) {
		add(-1, "", "missing_terminal_or_error", "", "truncated Chat stream ended silently without finish_reason+[DONE] or an error frame")
	}

	return violations
}

// validateChatToolCallDeltas checks contiguous tool_call index creation and
// non-empty initial IDs and names. Follow-up deltas for an open index must not
// overwrite function.name with an empty string.
func validateChatToolCallDeltas(frameIdx int, event string, choicePos int, tcArr []jsonNode, tools *[]*chatToolAcc, add func(int, string, string, string, string)) {
	for tPos, tcNode := range tcArr {
		basePath := fmt.Sprintf("$.choices[%d].delta.tool_calls[%d]", choicePos, tPos)
		if tcNode.kind != nodeObject {
			add(frameIdx, event, "invalid_tool_call_delta", basePath, "tool_call delta must be an object")
			continue
		}
		tcObj := tcNode.objVal
		idxVal, hasIdx, okIdx := nodeIntField(tcObj, "index")
		if !hasIdx || !okIdx || idxVal < 0 {
			add(frameIdx, event, "missing_tool_call_index", basePath+".index", "tool_call delta requires non-negative integer index")
			continue
		}

		fnObj, hasFn, okFn := nodeObjectField(tcObj, "function")
		if idxVal == int64(len(*tools)) {
			id, _, _ := nodeStringField(tcObj, "id")
			typ, _, _ := nodeStringField(tcObj, "type")
			var fnName, fnArgs string
			if hasFn && okFn {
				fnName, _, _ = nodeStringField(fnObj, "name")
				fnArgs, _, _ = nodeStringField(fnObj, "arguments")
			}
			if id == "" {
				add(frameIdx, event, "missing_tool_call_id", basePath+".id", "opening tool_call delta requires non-empty id")
			}
			if typ != "function" {
				add(frameIdx, event, "invalid_tool_call_type", basePath+".type", fmt.Sprintf("opening tool_call type = %q, want \"function\"", typ))
			}
			if fnName == "" {
				add(frameIdx, event, "missing_tool_call_name", basePath+".function.name", "opening tool_call delta requires non-empty function.name")
			}
			acc := &chatToolAcc{id: id, name: fnName}
			_, _ = acc.args.WriteString(fnArgs)
			*tools = append(*tools, acc)
			continue
		}

		if idxVal > int64(len(*tools)) {
			add(frameIdx, event, "non_contiguous_tool_call_index", basePath+".index", fmt.Sprintf("tool_call index %d skipped expected index %d", idxVal, len(*tools)))
			continue
		}

		existing := (*tools)[idxVal]
		if hasFn && okFn {
			if nameVal, hasName, okName := nodeStringField(fnObj, "name"); hasName {
				if !okName || nameVal == "" {
					add(frameIdx, event, "empty_tool_name_delta", basePath+".function.name", "follow-up tool_call delta must not carry empty function.name (091ec1f41)")
				} else if nameVal != existing.name {
					add(frameIdx, event, "conflicting_tool_name_delta", basePath+".function.name", fmt.Sprintf("follow-up function.name %q conflicts with %q", nameVal, existing.name))
				}
			}
			if fnArgs, hasArgs, okArgs := nodeStringField(fnObj, "arguments"); hasArgs && okArgs {
				_, _ = existing.args.WriteString(fnArgs)
			}
		}
	}
}

// hasNonEmptyDeltaContent reports whether deltaObj carries content, reasoning,
// refusal, or tool_calls fragments.
func hasNonEmptyDeltaContent(deltaObj map[string]jsonNode) bool {
	for _, k := range []string{"content", "reasoning_content", "reasoning", "refusal"} {
		if s, has, ok := nodeStringField(deltaObj, k); has && ok && s != "" {
			return true
		}
	}
	if arr, has, ok := nodeArrayField(deltaObj, "tool_calls"); has && ok && len(arr) > 0 {
		return true
	}
	return false
}

// isValidChatFinishReason reports whether reason is an accepted OpenAI finish_reason string.
func isValidChatFinishReason(reason string) bool {
	switch reason {
	case "stop", "length", "tool_calls", "content_filter", "function_call":
		return true
	default:
		return false
	}
}
