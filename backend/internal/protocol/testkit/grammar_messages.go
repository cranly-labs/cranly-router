package testkit

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// grammar_messages.go validates Anthropic Messages SSE streams against event
// ordering, block index contiguity, delta type compatibility, tool input JSON
// completeness, and normal or error termination rules. Client SDKs assemble
// assistant blocks by numeric index and reject out-of-order block starts or
// deltas emitted after content_block_stop (7b15de584).
//
// Messages requires a single message_start followed by non-overlapping content
// blocks numbered 0..N-1, then message_delta and message_stop, with no [DONE]
// sentinel. Under AllowAbnormal mode (law S7), an error event may terminate the
// stream once any currently open content block has been closed cleanly.

// ValidateMessagesStream checks frames against the Anthropic Messages streaming
// state machine and returns all detected grammar violations in frame order.
//
// Normal completion requires message_start, zero or more closed content blocks
// with contiguous zero-based indices, message_delta with a non-empty
// stop_reason, and message_stop, while accumulated tool_use input_json_delta
// fragments must form a valid JSON object.
//
// When opts.AllowAbnormal is true, a terminal error event is permitted provided
// no content block remains open and no further frames follow the error event.
// Incomplete tool_use JSON closed before an error event is allowed in that
// abnormal mode, whereas streams ending with message_stop must still carry
// valid JSON tool inputs.
func ValidateMessagesStream(frames []sse.Frame, opts GrammarOptions) []Violation {
	var violations []Violation
	add := func(idx int, event, rule, path, msg string) {
		violations = append(violations, Violation{
			Protocol:   protocol.ProtocolAnthropicMessages,
			Rule:       rule,
			FrameIndex: idx,
			Event:      event,
			Path:       path,
			Message:    msg,
		})
	}

	if len(frames) == 0 {
		add(-1, "", "empty_stream", "", "Messages stream ended without emitting any frames")
		return violations
	}

	started := false
	openIndex := -1
	openType := ""
	var openToolPartial strings.Builder
	var pendingToolJSONViolations []Violation
	nextIndex := 0
	sawMessageDelta := false
	terminated := false
	terminatedByError := false

	for i, frame := range frames {
		trimmed := bytes.TrimSpace(frame.Data)
		if bytes.Equal(trimmed, []byte("[DONE]")) {
			add(i, frame.Event, "unexpected_done_sentinel", "", "Anthropic Messages SSE must not emit [DONE]")
			continue
		}
		if terminated {
			add(i, frame.Event, "frame_after_terminal", "", "no frames may follow message_stop or error")
			continue
		}

		obj, err := parseFrameJSONObject(trimmed)
		if err != nil {
			add(i, frame.Event, "invalid_json", "$", err.Error())
			continue
		}
		evType, hasType, okType := nodeStringField(obj, "type")
		if !hasType || !okType || evType == "" {
			add(i, frame.Event, "missing_event_type", "$.type", "frame payload must carry a non-empty string type")
			continue
		}
		if frame.Event != "" && frame.Event != evType {
			add(i, frame.Event, "event_header_mismatch", "$.type", fmt.Sprintf("SSE event header %q disagrees with payload type %q", frame.Event, evType))
		}

		switch evType {
		case "ping":
			continue

		case "message_start":
			if started {
				add(i, evType, "duplicate_message_start", "$", "message_start must appear only once")
				continue
			}
			started = true
			validateMessagesStartPayload(i, obj, add)

		case "content_block_start":
			if !started {
				add(i, evType, "block_before_message_start", "$", "content_block_start appeared before message_start")
			}
			if sawMessageDelta {
				add(i, evType, "block_after_message_delta", "$", "content_block_start cannot appear after message_delta")
			}
			if openIndex >= 0 {
				add(i, evType, "overlapping_content_block", "$.index", fmt.Sprintf("content_block_start arrived while block %d (%s) is still open", openIndex, openType))
			}
			idxVal, hasIdx, okIdx := nodeIntField(obj, "index")
			if !hasIdx || !okIdx || idxVal != int64(nextIndex) {
				add(i, evType, "non_contiguous_block_index", "$.index", fmt.Sprintf("got content_block_start index %d, want %d", idxVal, nextIndex))
			}
			blockObj, hasBlock, okBlock := nodeObjectField(obj, "content_block")
			if !hasBlock || !okBlock {
				add(i, evType, "missing_content_block", "$.content_block", "content_block_start requires a content_block object")
				openIndex = nextIndex
				openType = "unknown"
				continue
			}
			bType, _, _ := nodeStringField(blockObj, "type")
			if bType == "" {
				add(i, evType, "missing_block_type", "$.content_block.type", "content_block.type must be a non-empty string")
			}
			openToolPartial.Reset()
			if bType == "tool_use" || bType == "server_tool_use" {
				idStr, _, _ := nodeStringField(blockObj, "id")
				nameStr, _, _ := nodeStringField(blockObj, "name")
				if idStr == "" {
					add(i, evType, "missing_tool_use_id", "$.content_block.id", "tool_use content_block requires non-empty id")
				}
				if nameStr == "" {
					add(i, evType, "missing_tool_use_name", "$.content_block.name", "tool_use content_block requires non-empty name")
				}
				if _, hasIn, okIn := nodeObjectField(blockObj, "input"); !hasIn || !okIn {
					add(i, evType, "missing_tool_use_input", "$.content_block.input", fmt.Sprintf("%s content_block requires an object input field", bType))
				}
			}
			openIndex = nextIndex
			openType = bType

		case "content_block_delta":
			if openIndex < 0 {
				add(i, evType, "delta_without_open_block", "$", "content_block_delta arrived with no open content block")
				continue
			}
			idxVal, hasIdx, okIdx := nodeIntField(obj, "index")
			if !hasIdx || !okIdx || idxVal != int64(openIndex) {
				add(i, evType, "delta_index_mismatch", "$.index", fmt.Sprintf("content_block_delta index %d does not match open block %d", idxVal, openIndex))
			}
			deltaObj, hasDelta, okDelta := nodeObjectField(obj, "delta")
			if !hasDelta || !okDelta {
				add(i, evType, "missing_delta_object", "$.delta", "content_block_delta requires a delta object")
				continue
			}
			validateMessagesDeltaPayload(i, openType, deltaObj, &openToolPartial, add)

		case "content_block_stop":
			if openIndex < 0 {
				add(i, evType, "stop_without_open_block", "$", "content_block_stop arrived with no open content block")
				continue
			}
			idxVal, hasIdx, okIdx := nodeIntField(obj, "index")
			if !hasIdx || !okIdx || idxVal != int64(openIndex) {
				add(i, evType, "stop_index_mismatch", "$.index", fmt.Sprintf("content_block_stop index %d does not match open block %d", idxVal, openIndex))
			}
			if openType == "tool_use" || openType == "server_tool_use" {
				if rawArgs := strings.TrimSpace(openToolPartial.String()); rawArgs != "" {
					if _, err := parseFrameJSONObject([]byte(rawArgs)); err != nil {
						msg := fmt.Sprintf("accumulated tool_use input_json_delta is not a valid JSON object: %v", err)
						if !opts.AllowAbnormal {
							add(i, evType, "invalid_tool_input_json", "$.partial_json", msg)
						} else {
							// Hold malformed tool JSON until stream termination is known so
							// aborted blocks closed before an error event pass S7 while fake
							// normal message_stop completions still report invalid_tool_input_json.
							pendingToolJSONViolations = append(pendingToolJSONViolations, Violation{
								Protocol:   protocol.ProtocolAnthropicMessages,
								Rule:       "invalid_tool_input_json",
								FrameIndex: i,
								Event:      evType,
								Path:       "$.partial_json",
								Message:    msg,
							})
						}
					}
				}
			}
			openIndex = -1
			openType = ""
			nextIndex++

		case "message_delta":
			if !started {
				add(i, evType, "message_delta_before_start", "$", "message_delta arrived before message_start")
			}
			if openIndex >= 0 {
				add(i, evType, "unclosed_block_at_message_delta", "$", fmt.Sprintf("block %d is still open when message_delta arrived", openIndex))
			}
			if sawMessageDelta {
				add(i, evType, "duplicate_message_delta", "$", "message_delta must appear only once")
			}
			sawMessageDelta = true
			deltaObj, hasDelta, okDelta := nodeObjectField(obj, "delta")
			if !hasDelta || !okDelta {
				add(i, evType, "missing_message_delta_body", "$.delta", "message_delta requires a delta object")
			} else if stopReason, _, _ := nodeStringField(deltaObj, "stop_reason"); stopReason == "" {
				add(i, evType, "missing_stop_reason", "$.delta.stop_reason", "message_delta.delta.stop_reason must be a non-empty string")
			}
			usageObj, hasUsage, okUsage := nodeObjectField(obj, "usage")
			if !hasUsage || !okUsage {
				add(i, evType, "missing_message_delta_usage", "$.usage", "message_delta requires a usage object")
			} else if outTok, hasOut, okOut := nodeIntField(usageObj, "output_tokens"); !hasOut || !okOut || outTok < 0 {
				add(i, evType, "invalid_output_tokens", "$.usage.output_tokens", "message_delta.usage.output_tokens must be a non-negative integer")
			}

		case "message_stop":
			if !started {
				add(i, evType, "message_stop_before_start", "$", "message_stop arrived before message_start")
			}
			if openIndex >= 0 {
				add(i, evType, "unclosed_block_at_message_stop", "$", fmt.Sprintf("block %d is still open at message_stop", openIndex))
			}
			if !sawMessageDelta {
				add(i, evType, "missing_message_delta", "$", "message_stop arrived without preceding message_delta")
			}
			terminated = true

		case "error":
			if !opts.AllowAbnormal {
				add(i, evType, "unexpected_error_event", "$", "error event is invalid in normal completion mode")
			}
			if openIndex >= 0 {
				add(i, evType, "dangling_block_before_error", "$", fmt.Sprintf("block %d was left open before terminal error event", openIndex))
			}
			errObj, hasErr, okErr := nodeObjectField(obj, "error")
			if !hasErr || !okErr {
				add(i, evType, "missing_error_object", "$.error", "error event requires an error object")
			} else {
				errMsg, _, _ := nodeStringField(errObj, "message")
				if errMsg == "" {
					add(i, evType, "missing_error_message", "$.error.message", "error.message must be a non-empty string")
				}
			}
			terminated = true
			terminatedByError = true

		default:
			add(i, evType, "unknown_event_type", "$.type", fmt.Sprintf("unrecognized Messages SSE event type %q", evType))
		}
	}

	// A stream that completed normally via message_stop rather than terminating
	// with an error event must still form valid JSON objects on every tool_use block.
	if terminated && !terminatedByError && len(pendingToolJSONViolations) > 0 {
		violations = append(violations, pendingToolJSONViolations...)
		slices.SortStableFunc(violations, func(a, b Violation) int {
			return cmp.Compare(a.FrameIndex, b.FrameIndex)
		})
	}
	if openIndex >= 0 && !terminatedByError {
		add(-1, "", "unclosed_content_block", "", fmt.Sprintf("stream ended while content block %d (%s) remained open", openIndex, openType))
	}
	if !terminated {
		add(-1, "", "missing_terminal_event", "", "stream ended without message_stop (or error in abnormal mode)")
	}
	return violations
}

// validateMessagesStartPayload checks mandatory envelope and usage fields on message_start.message.
func validateMessagesStartPayload(idx int, obj map[string]jsonNode, add func(int, string, string, string, string)) {
	msgObj, hasMsg, okMsg := nodeObjectField(obj, "message")
	if !hasMsg || !okMsg {
		add(idx, "message_start", "missing_message_envelope", "$.message", "message_start requires a message object")
		return
	}
	if id, _, _ := nodeStringField(msgObj, "id"); id == "" {
		add(idx, "message_start", "missing_message_id", "$.message.id", "message.id must be a non-empty string")
	}
	if mType, _, _ := nodeStringField(msgObj, "type"); mType != "message" {
		add(idx, "message_start", "invalid_message_type", "$.message.type", fmt.Sprintf("message.type = %q, want \"message\"", mType))
	}
	if role, _, _ := nodeStringField(msgObj, "role"); role != "assistant" {
		add(idx, "message_start", "invalid_message_role", "$.message.role", fmt.Sprintf("message.role = %q, want \"assistant\"", role))
	}
	if _, hasModel, okModel := nodeStringField(msgObj, "model"); !hasModel || !okModel {
		add(idx, "message_start", "missing_message_model", "$.message.model", "message.model must be a string")
	}
	if contentArr, hasContent, okContent := nodeArrayField(msgObj, "content"); !hasContent || !okContent || len(contentArr) != 0 {
		add(idx, "message_start", "non_empty_initial_content", "$.message.content", "message_start.message.content must be an empty array")
	}
	usageObj, hasUsage, okUsage := nodeObjectField(msgObj, "usage")
	if !hasUsage || !okUsage {
		add(idx, "message_start", "missing_initial_usage", "$.message.usage", "message_start.message requires a usage object")
		return
	}
	if inTok, hasIn, okIn := nodeIntField(usageObj, "input_tokens"); !hasIn || !okIn || inTok < 0 {
		add(idx, "message_start", "invalid_input_tokens", "$.message.usage.input_tokens", "message.usage.input_tokens must be a non-negative integer")
	}
	if outTok, hasOut, okOut := nodeIntField(usageObj, "output_tokens"); !hasOut || !okOut || outTok < 0 {
		add(idx, "message_start", "invalid_output_tokens", "$.message.usage.output_tokens", "message.usage.output_tokens must be a non-negative integer")
	}
}

// validateMessagesDeltaPayload checks delta.type compatibility with the currently
// open content block and accumulates partial_json fragments for tool_use blocks.
func validateMessagesDeltaPayload(idx int, blockType string, deltaObj map[string]jsonNode, toolBuf *strings.Builder, add func(int, string, string, string, string)) {
	dType, _, _ := nodeStringField(deltaObj, "type")
	switch dType {
	case "text_delta":
		if blockType != "text" {
			add(idx, "content_block_delta", "incompatible_delta_type", "$.delta.type", fmt.Sprintf("text_delta arrived inside %q block", blockType))
		}
		if _, hasTxt, okTxt := nodeStringField(deltaObj, "text"); !hasTxt || !okTxt {
			add(idx, "content_block_delta", "missing_text_delta", "$.delta.text", "text_delta requires a string text field")
		}
	case "citations_delta":
		if blockType != "text" {
			add(idx, "content_block_delta", "incompatible_delta_type", "$.delta.type", fmt.Sprintf("citations_delta arrived inside %q block", blockType))
		}
	case "thinking_delta":
		if blockType != "thinking" {
			add(idx, "content_block_delta", "incompatible_delta_type", "$.delta.type", fmt.Sprintf("thinking_delta arrived inside %q block", blockType))
		}
		if _, hasTh, okTh := nodeStringField(deltaObj, "thinking"); !hasTh || !okTh {
			add(idx, "content_block_delta", "missing_thinking_delta", "$.delta.thinking", "thinking_delta requires a string thinking field")
		}
	case "signature_delta":
		if blockType != "thinking" {
			add(idx, "content_block_delta", "incompatible_delta_type", "$.delta.type", fmt.Sprintf("signature_delta arrived inside %q block", blockType))
		}
		if sig, hasSig, okSig := nodeStringField(deltaObj, "signature"); !hasSig || !okSig || sig == "" {
			add(idx, "content_block_delta", "invalid_signature_delta", "$.delta.signature", "signature_delta requires a non-empty string signature")
		}
	case "input_json_delta":
		if blockType != "tool_use" && blockType != "server_tool_use" {
			add(idx, "content_block_delta", "incompatible_delta_type", "$.delta.type", fmt.Sprintf("input_json_delta arrived inside %q block", blockType))
		}
		partJSON, hasPJ, okPJ := nodeStringField(deltaObj, "partial_json")
		if !hasPJ || !okPJ {
			add(idx, "content_block_delta", "missing_partial_json", "$.delta.partial_json", "input_json_delta requires a string partial_json field")
		} else {
			_, _ = toolBuf.WriteString(partJSON)
		}
	default:
		add(idx, "content_block_delta", "unknown_delta_type", "$.delta.type", fmt.Sprintf("unrecognized content_block_delta type %q", dType))
	}
}
