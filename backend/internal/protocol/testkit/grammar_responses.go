package testkit

import (
	"bytes"
	"fmt"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// grammar_responses.go validates OpenAI Responses SSE streams against response
// envelope completeness, sequence_number monotonicity, contiguous output_index
// and content_index progression, argument delta purity, and terminal usage
// details required by strict Rust and TypeScript SDK decoders.
//
// Historical regressions caught here include missing created_at on response
// envelopes (6b6987483), non-advancing content_index inside multi-part messages
// (ac38d88f0), stray empty name fields on argument deltas (091ec1f41), and
// missing cached_tokens or reasoning_tokens usage detail subobjects that cause
// Codex CLI deserialization failures on completed responses. Under abnormal
// stream cuts, streams past response.created must close open items and emit
// response.failed rather than a bare error event.

// responsesOpenItem tracks per-output_index lifecycle state while scanning a
// Responses SSE stream.
type responsesOpenItem struct {
	itemType        string // item.type announced on output_item.added
	closed          bool   // whether output_item.done arrived
	openContentIdx  int    // currently open content_index, or -1
	openContentType string // output_text or refusal
	nextContentIdx  int    // next expected contiguous content_index
	openSummaryIdx  int    // currently open summary_index for reasoning, or -1
	nextSummaryIdx  int    // next expected contiguous summary_index
}

// ValidateResponsesStream checks frames against the OpenAI Responses SSE
// grammar and returns all detected violations in frame order.
//
// Normal completion requires response.created, contiguous output_index and
// nested content_index/summary_index lifecycles with every added item closed,
// and a terminal response.completed or response.incomplete carrying non-null
// created_at and nested usage token detail objects.
//
// When opts.AllowAbnormal is true, pre-creation streams may end with error,
// whereas streams that already emitted response.created must close open items
// and terminate with response.failed rather than a bare error frame.
func ValidateResponsesStream(frames []sse.Frame, opts GrammarOptions) []Violation {
	var violations []Violation
	add := func(idx int, event, rule, path, msg string) {
		violations = append(violations, Violation{
			Protocol:   protocol.ProtocolOpenAIResponses,
			Rule:       rule,
			FrameIndex: idx,
			Event:      event,
			Path:       path,
			Message:    msg,
		})
	}

	if len(frames) == 0 {
		add(-1, "", "empty_stream", "", "Responses stream ended without emitting any frames")
		return violations
	}

	created := false
	terminated := false
	sawDone := false
	hasSeqMode := false
	expectedSeq := int64(0)
	var items []*responsesOpenItem

	for i, frame := range frames {
		trimmed := bytes.TrimSpace(frame.Data)
		if sawDone {
			add(i, frame.Event, "frame_after_done", "", "no frames may follow [DONE]")
			continue
		}
		if bytes.Equal(trimmed, []byte("[DONE]")) {
			if !terminated {
				add(i, frame.Event, "done_before_terminal", "", "[DONE] arrived before terminal response event")
			}
			sawDone = true
			continue
		}
		if terminated {
			add(i, frame.Event, "frame_after_terminal", "", "only optional [DONE] may follow terminal response event")
			continue
		}

		obj, err := parseFrameJSONObject(trimmed)
		if err != nil {
			add(i, frame.Event, "invalid_json", "$", err.Error())
			continue
		}
		evType, hasType, okType := nodeStringField(obj, "type")
		if !hasType || !okType || evType == "" {
			add(i, frame.Event, "missing_event_type", "$.type", "Responses event payload must have a non-empty string type")
			continue
		}
		if frame.Event != "" && frame.Event != evType {
			add(i, frame.Event, "event_header_mismatch", "$.type", fmt.Sprintf("SSE event header %q disagrees with payload type %q", frame.Event, evType))
		}

		if seq, hasSeq, okSeq := nodeIntField(obj, "sequence_number"); hasSeq {
			if !okSeq || seq < 0 {
				add(i, evType, "invalid_sequence_number", "$.sequence_number", "sequence_number must be a non-negative integer")
			} else if !hasSeqMode {
				hasSeqMode = true
				expectedSeq = seq + 1
			} else {
				if seq != expectedSeq {
					add(i, evType, "non_contiguous_sequence_number", "$.sequence_number", fmt.Sprintf("sequence_number = %d, want %d", seq, expectedSeq))
				}
				expectedSeq = seq + 1
			}
		} else if hasSeqMode {
			add(i, evType, "missing_sequence_number", "$.sequence_number", "sequence_number omitted after earlier numbered events")
		}

		switch evType {
		case "response.created":
			if created {
				add(i, evType, "duplicate_response_created", "$", "response.created must appear only once")
				continue
			}
			created = true
			validateResponsesEnvelope(i, evType, obj, false, add)

		case "response.in_progress":
			if !created {
				add(i, evType, "in_progress_before_created", "$", "response.in_progress arrived before response.created")
			}
			validateResponsesEnvelope(i, evType, obj, false, add)

		case "response.output_item.added":
			if !created {
				add(i, evType, "item_before_created", "$", "response.output_item.added arrived before response.created")
			}
			outIdx, hasIdx, okIdx := nodeIntField(obj, "output_index")
			if !hasIdx || !okIdx || outIdx != int64(len(items)) {
				add(i, evType, "non_contiguous_output_index", "$.output_index", fmt.Sprintf("output_index = %d, want %d", outIdx, len(items)))
			}
			itemObj, hasItem, okItem := nodeObjectField(obj, "item")
			iType := "unknown"
			if !hasItem || !okItem {
				add(i, evType, "missing_item_object", "$.item", "response.output_item.added requires an item object")
			} else {
				iType, _, _ = nodeStringField(itemObj, "type")
				if iType == "" {
					add(i, evType, "missing_item_type", "$.item.type", "item.type must be a non-empty string")
				}
				if iType == "function_call" || iType == "custom_tool_call" {
					callID, _, _ := nodeStringField(itemObj, "call_id")
					name, _, _ := nodeStringField(itemObj, "name")
					if callID == "" {
						add(i, evType, "missing_call_id", "$.item.call_id", fmt.Sprintf("%s item requires non-empty call_id", iType))
					}
					if name == "" {
						add(i, evType, "missing_tool_name", "$.item.name", fmt.Sprintf("%s item requires non-empty name", iType))
					}
				}
			}
			items = append(items, &responsesOpenItem{
				itemType:       iType,
				openContentIdx: -1,
				openSummaryIdx: -1,
			})

		case "response.content_part.added":
			it := lookupOpenResponsesItem(i, evType, obj, items, "message", add)
			if it == nil {
				continue
			}
			if it.openContentIdx >= 0 {
				add(i, evType, "overlapping_content_part", "$.content_index", fmt.Sprintf("content_part %d is still open", it.openContentIdx))
			}
			cIdx, hasC, okC := nodeIntField(obj, "content_index")
			if !hasC || !okC || cIdx != int64(it.nextContentIdx) {
				add(i, evType, "non_contiguous_content_index", "$.content_index", fmt.Sprintf("content_index = %d, want %d", cIdx, it.nextContentIdx))
			}
			partObj, hasP, okP := nodeObjectField(obj, "part")
			pType := ""
			if !hasP || !okP {
				add(i, evType, "missing_part_object", "$.part", "content_part.added requires a part object")
			} else {
				pType, _, _ = nodeStringField(partObj, "type")
				if pType != "output_text" && pType != "refusal" {
					add(i, evType, "invalid_content_part_type", "$.part.type", fmt.Sprintf("unexpected message part.type %q", pType))
				}
			}
			it.openContentIdx = it.nextContentIdx
			it.openContentType = pType

		case "response.output_text.delta", "response.refusal.delta":
			wantPart := "output_text"
			if evType == "response.refusal.delta" {
				wantPart = "refusal"
			}
			it := lookupOpenResponsesItem(i, evType, obj, items, "message", add)
			if it == nil {
				continue
			}
			cIdx, hasC, okC := nodeIntField(obj, "content_index")
			if !hasC || !okC || cIdx != int64(it.openContentIdx) {
				add(i, evType, "content_delta_index_mismatch", "$.content_index", fmt.Sprintf("content_index = %d, want open %d", cIdx, it.openContentIdx))
			}
			if it.openContentType != "" && it.openContentType != wantPart {
				add(i, evType, "content_delta_type_mismatch", "$.type", fmt.Sprintf("%s arrived while open part is %q", evType, it.openContentType))
			}
			if _, hasD, okD := nodeStringField(obj, "delta"); !hasD || !okD {
				add(i, evType, "missing_delta_string", "$.delta", "text/refusal delta requires a string delta field")
			}

		case "response.output_text.done", "response.refusal.done":
			it := lookupOpenResponsesItem(i, evType, obj, items, "message", add)
			if it == nil {
				continue
			}
			if cIdx, hasC, okC := nodeIntField(obj, "content_index"); !hasC || !okC || cIdx != int64(it.openContentIdx) {
				add(i, evType, "content_done_index_mismatch", "$.content_index", fmt.Sprintf("content_index = %d, want open %d", cIdx, it.openContentIdx))
			}

		case "response.content_part.done":
			it := lookupOpenResponsesItem(i, evType, obj, items, "message", add)
			if it == nil {
				continue
			}
			cIdx, hasC, okC := nodeIntField(obj, "content_index")
			if it.openContentIdx < 0 || !hasC || !okC || cIdx != int64(it.openContentIdx) {
				add(i, evType, "content_part_done_mismatch", "$.content_index", fmt.Sprintf("content_part.done index %d does not match open %d", cIdx, it.openContentIdx))
			}
			it.openContentIdx = -1
			it.openContentType = ""
			it.nextContentIdx++

		case "response.reasoning_summary_part.added":
			it := lookupOpenResponsesItem(i, evType, obj, items, "reasoning", add)
			if it == nil {
				continue
			}
			if it.openSummaryIdx >= 0 {
				add(i, evType, "overlapping_summary_part", "$.summary_index", fmt.Sprintf("summary_part %d is still open", it.openSummaryIdx))
			}
			sIdx, hasS, okS := nodeIntField(obj, "summary_index")
			if !hasS || !okS || sIdx != int64(it.nextSummaryIdx) {
				add(i, evType, "non_contiguous_summary_index", "$.summary_index", fmt.Sprintf("summary_index = %d, want %d", sIdx, it.nextSummaryIdx))
			}
			it.openSummaryIdx = it.nextSummaryIdx

		case "response.reasoning_summary_text.delta", "response.reasoning_summary_text.done":
			it := lookupOpenResponsesItem(i, evType, obj, items, "reasoning", add)
			if it == nil {
				continue
			}
			if sIdx, hasS, okS := nodeIntField(obj, "summary_index"); !hasS || !okS || sIdx != int64(it.openSummaryIdx) {
				add(i, evType, "summary_index_mismatch", "$.summary_index", fmt.Sprintf("summary_index = %d, want open %d", sIdx, it.openSummaryIdx))
			}
			if evType == "response.reasoning_summary_text.delta" {
				if _, hasD, okD := nodeStringField(obj, "delta"); !hasD || !okD {
					add(i, evType, "missing_delta_string", "$.delta", "reasoning_summary_text.delta requires a string delta")
				}
			}

		case "response.reasoning_summary_part.done":
			it := lookupOpenResponsesItem(i, evType, obj, items, "reasoning", add)
			if it == nil {
				continue
			}
			sIdx, hasS, okS := nodeIntField(obj, "summary_index")
			if it.openSummaryIdx < 0 || !hasS || !okS || sIdx != int64(it.openSummaryIdx) {
				add(i, evType, "summary_part_done_mismatch", "$.summary_index", fmt.Sprintf("summary_part.done index %d does not match open %d", sIdx, it.openSummaryIdx))
			}
			it.openSummaryIdx = -1
			it.nextSummaryIdx++

		case "response.reasoning_text.delta", "response.reasoning_text.done":
			_ = lookupOpenResponsesItem(i, evType, obj, items, "reasoning", add)

		case "response.function_call_arguments.delta":
			it := lookupOpenResponsesItem(i, evType, obj, items, "function_call", add)
			if it == nil {
				continue
			}
			if _, hasName := obj["name"]; hasName {
				add(i, evType, "unexpected_name_on_arguments_delta", "$.name", "function_call_arguments.delta must not carry a name field (091ec1f41)")
			}
			if _, hasD, okD := nodeStringField(obj, "delta"); !hasD || !okD {
				add(i, evType, "missing_delta_string", "$.delta", "function_call_arguments.delta requires a string delta")
			}

		case "response.function_call_arguments.done":
			_ = lookupOpenResponsesItem(i, evType, obj, items, "function_call", add)

		case "response.custom_tool_call_input.delta":
			it := lookupOpenResponsesItem(i, evType, obj, items, "custom_tool_call", add)
			if it == nil {
				continue
			}
			if _, hasName := obj["name"]; hasName {
				add(i, evType, "unexpected_name_on_custom_delta", "$.name", "custom_tool_call_input.delta must not carry a name field")
			}
			if _, hasD, okD := nodeStringField(obj, "delta"); !hasD || !okD {
				add(i, evType, "missing_delta_string", "$.delta", "custom_tool_call_input.delta requires a string delta")
			}

		case "response.custom_tool_call_input.done":
			_ = lookupOpenResponsesItem(i, evType, obj, items, "custom_tool_call", add)

		case "response.output_item.done":
			it := lookupOpenResponsesItem(i, evType, obj, items, "", add)
			if it == nil {
				continue
			}
			if it.openContentIdx >= 0 {
				add(i, evType, "unclosed_content_part", "$", fmt.Sprintf("output_item.done arrived while content_part %d is open", it.openContentIdx))
			}
			if it.openSummaryIdx >= 0 {
				add(i, evType, "unclosed_summary_part", "$", fmt.Sprintf("output_item.done arrived while summary_part %d is open", it.openSummaryIdx))
			}
			if _, hasItem, okItem := nodeObjectField(obj, "item"); !hasItem || !okItem {
				add(i, evType, "missing_item_object", "$.item", "response.output_item.done requires an item object")
			}
			it.closed = true

		case "response.completed", "response.incomplete":
			if !created {
				add(i, evType, "terminal_before_created", "$", fmt.Sprintf("%s arrived before response.created", evType))
			}
			for idx, it := range items {
				if !it.closed {
					add(i, evType, "unclosed_output_item", "$", fmt.Sprintf("output_item %d (%s) was not closed before %s", idx, it.itemType, evType))
				}
			}
			validateResponsesEnvelope(i, evType, obj, true, add)
			terminated = true

		case "response.failed":
			if !opts.AllowAbnormal {
				add(i, evType, "unexpected_response_failed", "$", "response.failed is invalid in normal completion mode")
			}
			for idx, it := range items {
				if !it.closed {
					add(i, evType, "dangling_item_before_failed", "$", fmt.Sprintf("output_item %d (%s) remained open before response.failed", idx, it.itemType))
				}
			}
			validateResponsesEnvelope(i, evType, obj, false, add)
			terminated = true

		case "error":
			if !opts.AllowAbnormal {
				add(i, evType, "unexpected_error_event", "$", "error event is invalid in normal completion mode")
			}
			if created {
				add(i, evType, "bare_error_after_response_created", "$", "after response.created, abnormal termination must emit response.failed rather than bare error")
			}
			terminated = true

		default:
			// Hosted tool progress notifications (e.g. response.web_search_call.searching)
			// remain valid while an associated output item is open before termination.
			if !created {
				add(i, evType, "event_before_created", "$", fmt.Sprintf("%s arrived before response.created", evType))
			}
		}
	}

	if !terminated {
		add(-1, "", "missing_terminal_event", "", "Responses stream ended without response.completed, response.incomplete, or abnormal response.failed")
	}
	return violations
}

// lookupOpenResponsesItem resolves output_index against items and checks that
// the target item is still open. When wantType is non-empty, it also verifies
// that the open item matches wantType.
func lookupOpenResponsesItem(frameIdx int, evType string, obj map[string]jsonNode, items []*responsesOpenItem, wantType string, add func(int, string, string, string, string)) *responsesOpenItem {
	outIdx, hasIdx, okIdx := nodeIntField(obj, "output_index")
	if !hasIdx || !okIdx || outIdx < 0 || outIdx >= int64(len(items)) {
		add(frameIdx, evType, "invalid_output_index", "$.output_index", fmt.Sprintf("output_index %d does not reference an added item", outIdx))
		return nil
	}
	it := items[outIdx]
	if it.closed {
		add(frameIdx, evType, "event_after_item_done", "$.output_index", fmt.Sprintf("%s arrived after output_item %d was already closed", evType, outIdx))
		return nil
	}
	if wantType != "" && it.itemType != wantType {
		add(frameIdx, evType, "item_type_mismatch", "$.output_index", fmt.Sprintf("%s targets output_item %d of type %q, want %q", evType, outIdx, it.itemType, wantType))
	}
	return it
}

// validateResponsesEnvelope checks mandatory fields on response.created,
// response.in_progress, response.completed, response.incomplete, and response.failed.
func validateResponsesEnvelope(frameIdx int, evType string, obj map[string]jsonNode, checkUsageDetails bool, add func(int, string, string, string, string)) {
	respObj, hasResp, okResp := nodeObjectField(obj, "response")
	if !hasResp || !okResp {
		add(frameIdx, evType, "missing_response_object", "$.response", fmt.Sprintf("%s requires a response object", evType))
		return
	}
	if id, _, _ := nodeStringField(respObj, "id"); id == "" {
		add(frameIdx, evType, "missing_response_id", "$.response.id", "response.id must be a non-empty string")
	}
	if rObj, _, _ := nodeStringField(respObj, "object"); rObj != "response" {
		add(frameIdx, evType, "invalid_response_object_tag", "$.response.object", fmt.Sprintf("response.object = %q, want \"response\"", rObj))
	}
	if createdAt, hasCA, okCA := nodeIntField(respObj, "created_at"); !hasCA || !okCA || createdAt < 0 {
		add(frameIdx, evType, "missing_created_at", "$.response.created_at", "response.created_at must be a non-negative integer (6b6987483)")
	}
	if status, _, _ := nodeStringField(respObj, "status"); status == "" {
		add(frameIdx, evType, "missing_response_status", "$.response.status", "response.status must be a non-empty string")
	}
	if _, hasModel, okModel := nodeStringField(respObj, "model"); !hasModel || !okModel {
		add(frameIdx, evType, "missing_response_model", "$.response.model", "response.model must be a string")
	}
	if _, hasOut, okOut := nodeArrayField(respObj, "output"); !hasOut || !okOut {
		add(frameIdx, evType, "missing_response_output", "$.response.output", "response.output must be an array")
	}

	if !checkUsageDetails {
		return
	}
	usageVal, hasUsage := respObj["usage"]
	if !hasUsage || usageVal.kind == nodeNull {
		return
	}
	if usageVal.kind != nodeObject {
		add(frameIdx, evType, "invalid_usage_object", "$.response.usage", "response.usage must be an object or null")
		return
	}
	uObj := usageVal.objVal
	for _, field := range []string{"input_tokens", "output_tokens", "total_tokens"} {
		if v, has, ok := nodeIntField(uObj, field); !has || !ok || v < 0 {
			add(frameIdx, evType, "invalid_usage_counter", "$.response.usage."+field, fmt.Sprintf("usage.%s must be a non-negative integer", field))
		}
	}
	inDetails, hasInD, okInD := nodeObjectField(uObj, "input_tokens_details")
	if !hasInD || !okInD {
		add(frameIdx, evType, "missing_input_tokens_details", "$.response.usage.input_tokens_details", "usage.input_tokens_details with cached_tokens is required")
	} else if v, has, ok := nodeIntField(inDetails, "cached_tokens"); !has || !ok || v < 0 {
		add(frameIdx, evType, "missing_cached_tokens_detail", "$.response.usage.input_tokens_details.cached_tokens", "input_tokens_details.cached_tokens must be a non-negative integer")
	}
	outDetails, hasOutD, okOutD := nodeObjectField(uObj, "output_tokens_details")
	if !hasOutD || !okOutD {
		add(frameIdx, evType, "missing_output_tokens_details", "$.response.usage.output_tokens_details", "usage.output_tokens_details with reasoning_tokens is required")
	} else if v, has, ok := nodeIntField(outDetails, "reasoning_tokens"); !has || !ok || v < 0 {
		add(frameIdx, evType, "missing_reasoning_tokens_detail", "$.response.usage.output_tokens_details.reasoning_tokens", "output_tokens_details.reasoning_tokens must be a non-negative integer")
	}
}
