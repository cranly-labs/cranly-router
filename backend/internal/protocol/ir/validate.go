package ir

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"strings"
)

// validate.go enforces structural invariants on IR Requests, buffered Responses,
// and sequential Event streams. Codecs and IR transformations rely on tagged
// union exclusivity, role-specific part placement, non-overlapping usage
// counters, and sequential single-open-part stream ordering rather than
// repeating defensive structural checks inside every encoder.
//
// Validate and ValidateResponse check union branches, role rules, tool-result
// nesting, raw JSON fragments, and extension metadata on decoded requests and
// replies. ValidateStream additionally enforces the seven canonical stream
// invariants: a unique leading response_start, sequential 0..N-1 part indices
// with at most one open part, kind-compatible deltas, complete part_end
// snapshots, non-erasing sparse usage updates, and a single terminal event
// preceded by closed parts.

const (
	// partLocation values identify where a Part appears so validatePart can
	// enforce location-specific PartKind restrictions.
	locSystem      partLocation = iota // Request.System instruction slice
	locMessage                         // conversation Message.Parts slice
	locToolResult                      // nested ToolResult.Content slice
	locResponse                        // buffered Response.Parts slice
	locStreamStart                     // EventPartStart skeleton payload
	locStreamEnd                       // EventPartEnd complete snapshot
)

// partLocation classifies the structural container owning a validated Part.
type partLocation uint8

// openStreamPart tracks an open part between EventPartStart and EventPartEnd in ValidateStream.
type openStreamPart struct {
	index     int             // expected sequential part index
	startPart Part            // skeleton Part captured at EventPartStart
	textBuf   strings.Builder // concatenated Delta.Text fragments
	argsBuf   strings.Builder // concatenated Delta.Arguments fragments
}

// Validate checks structural invariants on req, including Part union branch
// exclusivity, message roles, dedicated tool-turn placement, ToolResult.Content
// text/media restrictions, tool declarations, and raw JSON extension values.
// Nil requests or any violated invariant return a path-qualified error.
func Validate(req *Request) error {
	if req == nil {
		return errors.New("nil request")
	}
	for i, part := range req.System {
		path := fmt.Sprintf("system[%d]", i)
		if err := validatePart(part, path, locSystem, ""); err != nil {
			return err
		}
	}
	for mIdx, msg := range req.Messages {
		mPath := fmt.Sprintf("messages[%d]", mIdx)
		if !isValidRole(msg.Role) {
			return fmt.Errorf("%s: invalid role %q", mPath, msg.Role)
		}
		if msg.Role == RoleTool && len(msg.Parts) == 0 {
			return fmt.Errorf("%s: tool message must contain at least one tool_result part", mPath)
		}
		for pIdx, part := range msg.Parts {
			pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
			if err := validatePart(part, pPath, locMessage, msg.Role); err != nil {
				return err
			}
		}
		if err := validateExtra(msg.Extra, mPath+".extra"); err != nil {
			return err
		}
	}
	for i, ns := range req.Namespaces {
		nsPath := fmt.Sprintf("namespaces[%d]", i)
		if strings.TrimSpace(ns.Name) == "" {
			return fmt.Errorf("%s: empty namespace name", nsPath)
		}
		if err := validateCacheMark(ns.Cache, nsPath+".cache"); err != nil {
			return err
		}
		if err := validateExtra(ns.Extra, nsPath+".extra"); err != nil {
			return err
		}
	}
	for i, tool := range req.Tools {
		if err := validateTool(tool, fmt.Sprintf("tools[%d]", i)); err != nil {
			return err
		}
	}
	if req.ToolChoice != nil {
		if err := validateToolChoice(*req.ToolChoice, "tool_choice"); err != nil {
			return err
		}
	}
	if req.MaxOutputTokens != nil && *req.MaxOutputTokens < 0 {
		return fmt.Errorf("max_output_tokens: negative value %d", *req.MaxOutputTokens)
	}
	if req.Sampling.CandidateCount != nil && *req.Sampling.CandidateCount <= 0 {
		return fmt.Errorf("sampling.candidate_count: nonpositive value %d", *req.Sampling.CandidateCount)
	}
	if req.Sampling.TopK != nil && *req.Sampling.TopK < 0 {
		return fmt.Errorf("sampling.top_k: negative value %d", *req.Sampling.TopK)
	}
	if req.Reasoning != nil {
		if err := validateReasoningConfig(*req.Reasoning, "reasoning"); err != nil {
			return err
		}
	}
	if req.Output != nil {
		if err := validateOutputFormat(*req.Output, "output"); err != nil {
			return err
		}
	}
	return validateExtra(req.Extra, "extra")
}

// ValidateResponse checks structural invariants on a buffered Response,
// verifying FinishReason validity, Part union branches, absence of tool_result
// parts in assistant output, nonnegative Usage buckets, and Extra payloads.
// Nil responses or structural violations return a path-qualified error.
func ValidateResponse(resp *Response) error {
	if resp == nil {
		return errors.New("nil response")
	}
	if !resp.Finish.Reason.IsValid() {
		return fmt.Errorf("finish.reason: invalid finish reason %q", resp.Finish.Reason)
	}
	for i, part := range resp.Parts {
		pPath := fmt.Sprintf("parts[%d]", i)
		if err := validatePart(part, pPath, locResponse, RoleAssistant); err != nil {
			return err
		}
		if resp.Finish.Reason != FinishError && part.Kind == PartToolCall && part.Call != nil && part.Call.Incomplete {
			return fmt.Errorf("%s: incomplete tool_call is only valid with error finish reason", pPath)
		}
	}
	if resp.Usage != nil {
		if err := validateUsage(resp.Usage, "usage"); err != nil {
			return err
		}
	}
	return validateExtra(resp.Extra, "extra")
}

// ValidateStream checks all seven canonical stream invariants from section 3.5
// across events. Empty slices, missing or duplicate response_start events,
// interleaved open parts, non-contiguous part indices, mismatched deltas,
// incomplete part_end snapshots, sparse usage resets, or missing/duplicate
// terminal events return descriptive errors.
//
// Part snapshots at EventPartEnd must retain every text or argument fragment
// delivered through preceding EventPartDelta steps on the same part index.
// Normal EventResponseEnd completions reject tool calls marked Incomplete,
// whereas EventError terminations allow closed Incomplete calls emitted when an
// upstream stream aborts mid-call.
func ValidateStream(events []Event) error {
	if len(events) == 0 {
		return errors.New("stream is empty: missing response_start")
	}

	var open *openStreamPart
	var prevUsage *Usage
	var sawIncompleteCall bool
	nextPartIndex := 0
	terminated := false

	for i, ev := range events {
		ePath := fmt.Sprintf("events[%d]", i)
		if terminated {
			return fmt.Errorf("%s: event %q appeared after terminal event", ePath, ev.Kind)
		}
		if !ev.Kind.IsValid() {
			return fmt.Errorf("%s: invalid event kind %q", ePath, ev.Kind)
		}
		if i == 0 && ev.Kind != EventResponseStart {
			return fmt.Errorf("%s: first event must be response_start, got %q", ePath, ev.Kind)
		}
		if i > 0 && ev.Kind == EventResponseStart {
			return fmt.Errorf("%s: duplicate response_start event", ePath)
		}

		switch ev.Kind {
		case EventResponseStart:
			if ev.Response == nil {
				return fmt.Errorf("%s: response_start requires non-nil response metadata", ePath)
			}
			if ev.Index != 0 || ev.Part != nil || ev.Delta != nil || ev.Usage != nil || ev.Finish != nil || ev.ServiceTier != "" || ev.Err != nil {
				return fmt.Errorf("%s: response_start carries unexpected non-response fields", ePath)
			}

		case EventPartStart:
			if open != nil {
				return fmt.Errorf("%s: part_start at index %d while part %d is still open", ePath, ev.Index, open.index)
			}
			if ev.Index != nextPartIndex {
				return fmt.Errorf("%s: part_start index = %d, want %d", ePath, ev.Index, nextPartIndex)
			}
			if ev.Part == nil {
				return fmt.Errorf("%s: part_start requires non-nil part", ePath)
			}
			if ev.Response != nil || ev.Delta != nil || ev.Usage != nil || ev.Finish != nil || ev.ServiceTier != "" || ev.Err != nil {
				return fmt.Errorf("%s: part_start carries unexpected fields", ePath)
			}
			if err := validatePart(*ev.Part, ePath+".part", locStreamStart, RoleAssistant); err != nil {
				return err
			}
			open = &openStreamPart{
				index:     ev.Index,
				startPart: ev.Part.Clone(),
			}

		case EventPartDelta:
			if open == nil {
				return fmt.Errorf("%s: part_delta at index %d with no open part", ePath, ev.Index)
			}
			if ev.Index != open.index {
				return fmt.Errorf("%s: part_delta index = %d, want open part index %d", ePath, ev.Index, open.index)
			}
			if ev.Delta == nil {
				return fmt.Errorf("%s: part_delta requires non-nil delta", ePath)
			}
			if ev.Response != nil || ev.Part != nil || ev.Usage != nil || ev.Finish != nil || ev.ServiceTier != "" || ev.Err != nil {
				return fmt.Errorf("%s: part_delta carries unexpected fields", ePath)
			}
			if err := validateStreamDelta(open, *ev.Delta, ePath); err != nil {
				return err
			}

		case EventPartEnd:
			if open == nil {
				return fmt.Errorf("%s: part_end at index %d with no open part", ePath, ev.Index)
			}
			if ev.Index != open.index {
				return fmt.Errorf("%s: part_end index = %d, want open part index %d", ePath, ev.Index, open.index)
			}
			if ev.Part == nil {
				return fmt.Errorf("%s: part_end requires non-nil complete part snapshot", ePath)
			}
			if ev.Response != nil || ev.Delta != nil || ev.Usage != nil || ev.Finish != nil || ev.ServiceTier != "" || ev.Err != nil {
				return fmt.Errorf("%s: part_end carries unexpected fields", ePath)
			}
			if err := validatePart(*ev.Part, ePath+".part", locStreamEnd, RoleAssistant); err != nil {
				return err
			}
			if err := validatePartEndSnapshot(open, *ev.Part, ePath); err != nil {
				return err
			}
			if ev.Part.Kind == PartToolCall && ev.Part.Call != nil && ev.Part.Call.Incomplete {
				sawIncompleteCall = true
			}
			open = nil
			nextPartIndex++

		case EventUsage:
			if ev.Usage == nil {
				return fmt.Errorf("%s: usage event requires non-nil usage", ePath)
			}
			if ev.Index != 0 || ev.Response != nil || ev.Part != nil || ev.Delta != nil || ev.Finish != nil || ev.ServiceTier != "" || ev.Err != nil {
				return fmt.Errorf("%s: usage event carries unexpected fields", ePath)
			}
			if err := validateUsage(ev.Usage, ePath+".usage"); err != nil {
				return err
			}
			if err := validateMergedUsageSnapshot(prevUsage, ev.Usage, ePath+".usage"); err != nil {
				return err
			}
			prevUsage = ev.Usage.Clone()

		case EventResponseEnd:
			if open != nil {
				return fmt.Errorf("%s: response_end while part %d is still open", ePath, open.index)
			}
			if ev.Finish == nil || !ev.Finish.Reason.IsValid() {
				return fmt.Errorf("%s: response_end requires a valid finish reason", ePath)
			}
			if ev.Index != 0 || ev.Response != nil || ev.Part != nil || ev.Delta != nil || ev.Usage != nil || ev.Err != nil {
				return fmt.Errorf("%s: response_end carries unexpected fields", ePath)
			}
			if sawIncompleteCall && ev.Finish.Reason != FinishError {
				return fmt.Errorf("%s: stream containing an incomplete tool_call cannot end with normal finish %q", ePath, ev.Finish.Reason)
			}
			terminated = true

		case EventError:
			if open != nil {
				return fmt.Errorf("%s: error event while part %d is still open", ePath, open.index)
			}
			if ev.Err == nil || strings.TrimSpace(ev.Err.Message) == "" {
				return fmt.Errorf("%s: error event requires non-empty error message", ePath)
			}
			if ev.Index != 0 || ev.Response != nil || ev.Part != nil || ev.Delta != nil || ev.Usage != nil || ev.Finish != nil || ev.ServiceTier != "" {
				return fmt.Errorf("%s: error event carries unexpected fields", ePath)
			}
			terminated = true
		}
	}

	if !terminated {
		return errors.New("stream ended without terminal response_end or error event")
	}
	return nil
}

// validatePart checks PartKind placement, union branch exclusivity, and nested
// payloads at path. Stream-start skeletons allow empty media data or token
// strings that complete at part_end, whereas completed parts require them.
func validatePart(p Part, path string, loc partLocation, msgRole Role) error {
	if err := validatePartLocation(p.Kind, path, loc, msgRole); err != nil {
		return err
	}
	if err := validateCacheMark(p.Cache, path+".cache"); err != nil {
		return err
	}
	if err := validateExtra(p.Extra, path+".extra"); err != nil {
		return err
	}
	if p.Kind != PartText && len(p.Annotations) > 0 {
		return fmt.Errorf("%s: annotations are only allowed on text parts, got %q", path, p.Kind)
	}
	if p.Kind != PartText && p.Token != nil {
		return fmt.Errorf("%s: token is only allowed on text parts, got %q", path, p.Kind)
	}

	switch p.Kind {
	case PartText:
		if p.Reasoning != nil || p.Call != nil || p.Result != nil || p.Media != nil || p.Opaque != nil {
			return fmt.Errorf("%s: text part must not populate non-text union branches", path)
		}
		if p.Token != nil && (loc == locSystem || loc == locToolResult || (loc == locMessage && msgRole != RoleAssistant)) {
			return fmt.Errorf("%s: text part token is only allowed in assistant turns or response output", path)
		}
		for i, ann := range p.Annotations {
			aPath := fmt.Sprintf("%s.annotations[%d]", path, i)
			if ann.StartIndex != nil && *ann.StartIndex < 0 {
				return fmt.Errorf("%s.start_index: negative offset %d", aPath, *ann.StartIndex)
			}
			if ann.EndIndex != nil && *ann.EndIndex < 0 {
				return fmt.Errorf("%s.end_index: negative offset %d", aPath, *ann.EndIndex)
			}
			if ann.StartIndex != nil && ann.EndIndex != nil && *ann.EndIndex < *ann.StartIndex {
				return fmt.Errorf("%s: end_index %d precedes start_index %d", aPath, *ann.EndIndex, *ann.StartIndex)
			}
			if err := validateExtra(ann.Extra, aPath+".extra"); err != nil {
				return err
			}
		}
		return validateReplayToken(p.Token, path+".token", loc == locStreamStart)

	case PartRefusal:
		if p.Reasoning != nil || p.Call != nil || p.Result != nil || p.Media != nil || p.Opaque != nil {
			return fmt.Errorf("%s: refusal part must not populate non-refusal union branches", path)
		}
		return nil

	case PartReasoning:
		if p.Reasoning == nil {
			return fmt.Errorf("%s: reasoning part requires non-nil reasoning branch", path)
		}
		if p.Text != "" || p.Call != nil || p.Result != nil || p.Media != nil || p.Opaque != nil {
			return fmt.Errorf("%s: reasoning part must not populate other union branches", path)
		}
		return validateReplayToken(p.Reasoning.Token, path+".reasoning.token", loc == locStreamStart)

	case PartToolCall:
		if p.Call == nil {
			return fmt.Errorf("%s: tool_call part requires non-nil call branch", path)
		}
		if p.Text != "" || p.Reasoning != nil || p.Result != nil || p.Media != nil || p.Opaque != nil {
			return fmt.Errorf("%s: tool_call part must not populate other union branches", path)
		}
		if loc == locMessage && p.Call.Incomplete {
			return fmt.Errorf("%s: request tool_call cannot set incomplete", path)
		}
		if err := validateToolRef(p.Call.Tool, path+".call.tool"); err != nil {
			return err
		}
		return validateReplayToken(p.Call.Token, path+".call.token", loc == locStreamStart)

	case PartToolResult:
		if p.Result == nil {
			return fmt.Errorf("%s: tool_result part requires non-nil result branch", path)
		}
		if p.Text != "" || p.Reasoning != nil || p.Call != nil || p.Media != nil || p.Opaque != nil {
			return fmt.Errorf("%s: tool_result part must not populate other union branches", path)
		}
		if isNonZeroToolRef(p.Result.Tool) {
			if err := validateToolRef(p.Result.Tool, path+".result.tool"); err != nil {
				return err
			}
		}
		for i, child := range p.Result.Content {
			cPath := fmt.Sprintf("%s.result.content[%d]", path, i)
			if child.Kind != PartText && child.Kind != PartMedia {
				return fmt.Errorf("%s: tool_result content allows only text and media parts, got %q", cPath, child.Kind)
			}
			if err := validatePart(child, cPath, locToolResult, ""); err != nil {
				return err
			}
		}
		return nil

	case PartMedia:
		if p.Media == nil {
			return fmt.Errorf("%s: media part requires non-nil media branch", path)
		}
		if p.Text != "" || p.Reasoning != nil || p.Call != nil || p.Result != nil || p.Opaque != nil {
			return fmt.Errorf("%s: media part must not populate other union branches", path)
		}
		return validateMedia(*p.Media, path+".media", loc == locStreamStart)

	case PartOpaque:
		if p.Opaque == nil {
			return fmt.Errorf("%s: opaque part requires non-nil opaque branch", path)
		}
		if p.Text != "" || p.Reasoning != nil || p.Call != nil || p.Result != nil || p.Media != nil {
			return fmt.Errorf("%s: opaque part must not populate other union branches", path)
		}
		return validateOpaqueItem(*p.Opaque, path+".opaque")

	default:
		return fmt.Errorf("%s: invalid part kind %q", path, p.Kind)
	}
}

// validatePartLocation enforces container and speaker-role placement rules for
// kind. Tool turns accept only tool_result parts, tool calls require assistant
// turns or response output, and system or nested tool_result slices reject calls.
func validatePartLocation(kind PartKind, path string, loc partLocation, msgRole Role) error {
	switch kind {
	case PartText, PartReasoning, PartToolCall, PartToolResult, PartMedia, PartRefusal, PartOpaque:
	default:
		return fmt.Errorf("%s: invalid part kind %q", path, kind)
	}

	switch loc {
	case locSystem:
		if kind != PartText && kind != PartMedia && kind != PartOpaque {
			return fmt.Errorf("%s: %s part is not allowed in system instructions", path, kind)
		}
	case locMessage:
		if msgRole == RoleTool && kind != PartToolResult {
			return fmt.Errorf("%s: tool message allows only tool_result parts, got %q", path, kind)
		}
		if msgRole != RoleTool && kind == PartToolResult {
			return fmt.Errorf("%s: tool_result part is only allowed in tool messages, got role %q", path, msgRole)
		}
		if msgRole != RoleAssistant && kind == PartToolCall {
			return fmt.Errorf("%s: tool_call part is only allowed in assistant messages, got role %q", path, msgRole)
		}
	case locToolResult:
		if kind != PartText && kind != PartMedia {
			return fmt.Errorf("%s: tool_result content allows only text and media parts, got %q", path, kind)
		}
	case locResponse, locStreamStart, locStreamEnd:
		if kind == PartToolResult {
			return fmt.Errorf("%s: tool_result part is not allowed in assistant response output", path)
		}
	}
	return nil
}

// validateStreamDelta verifies that d matches open.startPart.Kind and appends
// streamed fragments into open. Deltas carrying both Text and Arguments or
// targeting media or opaque parts return an error.
func validateStreamDelta(open *openStreamPart, d Delta, path string) error {
	if d.Text != "" && d.Arguments != "" {
		return fmt.Errorf("%s: delta cannot carry both text and arguments simultaneously", path)
	}
	switch open.startPart.Kind {
	case PartText, PartRefusal, PartReasoning:
		if d.Arguments != "" {
			return fmt.Errorf("%s: %s part cannot receive arguments delta", path, open.startPart.Kind)
		}
		_, _ = open.textBuf.WriteString(d.Text)
		return nil

	case PartToolCall:
		if d.Text != "" {
			return fmt.Errorf("%s: tool_call part cannot receive text delta", path)
		}
		_, _ = open.argsBuf.WriteString(d.Arguments)
		return nil

	default:
		return fmt.Errorf("%s: %s part does not accept part_delta events", path, open.startPart.Kind)
	}
}

// validatePartEndSnapshot verifies that endPart preserves open.startPart's Kind,
// tool CallID, and all text or argument fragments accumulated during the part's
// open window. Missing accumulated text or dropped tool arguments return an error.
func validatePartEndSnapshot(open *openStreamPart, endPart Part, path string) error {
	if endPart.Kind != open.startPart.Kind {
		return fmt.Errorf("%s: part_end kind %q does not match part_start kind %q", path, endPart.Kind, open.startPart.Kind)
	}
	switch endPart.Kind {
	case PartText, PartRefusal:
		want := open.startPart.Text + open.textBuf.String()
		if want != "" && endPart.Text != want {
			return fmt.Errorf("%s: %s part_end text %q does not match accumulated snapshot %q", path, endPart.Kind, endPart.Text, want)
		}

	case PartReasoning:
		var startText string
		if open.startPart.Reasoning != nil && len(open.startPart.Reasoning.Segments) > 0 {
			startText = strings.Join(open.startPart.Reasoning.Segments, "")
		}
		want := startText + open.textBuf.String()
		got := strings.Join(endPart.Reasoning.Segments, "")
		if want != "" && got != want {
			return fmt.Errorf("%s: reasoning part_end segments %q do not match accumulated text %q", path, got, want)
		}

	case PartToolCall:
		if open.startPart.Call != nil && open.startPart.Call.CallID != "" && endPart.Call.CallID != open.startPart.Call.CallID {
			return fmt.Errorf("%s: tool_call part_end call_id %q does not match part_start call_id %q", path, endPart.Call.CallID, open.startPart.Call.CallID)
		}
		if open.argsBuf.Len() > 0 && endPart.Call.Arguments == "" {
			return fmt.Errorf("%s: tool_call part_end dropped accumulated arguments", path)
		}
	}
	return nil
}

// validateMergedUsageSnapshot rejects sparse tail usage events that zero out
// previously observed input or cache buckets while still reporting token work.
// Complete all-zero resets and tail frames that reclassify uncached input into
// an increased cache_read or cache_write bucket are permitted.
func validateMergedUsageSnapshot(prev, cur *Usage, path string) error {
	if prev == nil || !cur.HasObservedTokens() {
		return nil
	}
	// Permit Input dropping to zero when a later frame supplies a 100% cache-hit
	// or cache-write breakdown that absorbs the earlier unsplit prompt count.
	if prev.Input > 0 && cur.Input == 0 &&
		cur.CacheRead <= prev.CacheRead &&
		cur.EffectiveCacheWrite() <= prev.EffectiveCacheWrite() {
		return fmt.Errorf("%s: sparse usage update cleared prior input count %d", path, prev.Input)
	}
	if prev.CacheRead > 0 && cur.CacheRead == 0 {
		return fmt.Errorf("%s: sparse usage update cleared prior cache_read count %d", path, prev.CacheRead)
	}
	if prev.EffectiveCacheWrite() > 0 && cur.EffectiveCacheWrite() == 0 {
		return fmt.Errorf("%s: sparse usage update cleared prior cache_write count %d", path, prev.EffectiveCacheWrite())
	}
	return nil
}

// validateTool checks declaration fields on t according to t.Kind, verifying
// required names, mutually exclusive schema/format/hosted branches, and JSON
// syntax. Invalid declarations return a path-qualified error.
func validateTool(t Tool, path string) error {
	switch t.Kind {
	case ToolFunction:
		if strings.TrimSpace(t.Name) == "" {
			return fmt.Errorf("%s: function tool requires non-empty name", path)
		}
		if len(t.Format) > 0 || t.Hosted != nil {
			return fmt.Errorf("%s: function tool cannot set custom format or hosted payload", path)
		}
		if len(t.InputSchema) > 0 {
			if err := validateRawJSON(t.InputSchema, path+".input_schema"); err != nil {
				return err
			}
		}

	case ToolCustom:
		if strings.TrimSpace(t.Name) == "" {
			return fmt.Errorf("%s: custom tool requires non-empty name", path)
		}
		if len(t.InputSchema) > 0 || t.Hosted != nil {
			return fmt.Errorf("%s: custom tool cannot set input_schema or hosted payload", path)
		}
		if len(t.Format) > 0 {
			if err := validateRawJSON(t.Format, path+".format"); err != nil {
				return err
			}
		}

	case ToolClientSearch:
		if len(t.InputSchema) > 0 || len(t.Format) > 0 || t.Hosted != nil {
			return fmt.Errorf("%s: client_search tool cannot set schema, format, or hosted payload", path)
		}

	case ToolHosted:
		if t.Hosted == nil {
			return fmt.Errorf("%s: hosted tool requires non-nil hosted payload", path)
		}
		if len(t.InputSchema) > 0 || len(t.Format) > 0 {
			return fmt.Errorf("%s: hosted tool cannot set input_schema or format", path)
		}
		if err := validateOpaqueItem(*t.Hosted, path+".hosted"); err != nil {
			return err
		}

	default:
		return fmt.Errorf("%s: invalid tool kind %q", path, t.Kind)
	}

	if err := validateCacheMark(t.Cache, path+".cache"); err != nil {
		return err
	}
	return validateExtra(t.Extra, path+".extra")
}

// validateToolChoice checks Mode, required Tool target exclusivity, and each
// AllowedTools entry on tc. Populated Tool targets outside ToolChoiceTool mode
// or missing names in ToolChoiceTool mode return an error.
func validateToolChoice(tc ToolChoice, path string) error {
	switch tc.Mode {
	case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
		if isNonZeroToolRef(tc.Tool) {
			return fmt.Errorf("%s: mode %q must not populate tool target", path, tc.Mode)
		}
	case ToolChoiceTool:
		if err := validateToolRef(tc.Tool, path+".tool"); err != nil {
			return err
		}
		if strings.TrimSpace(tc.Tool.Name) == "" {
			return fmt.Errorf("%s.tool: tool mode requires non-empty tool name", path)
		}
	default:
		return fmt.Errorf("%s: invalid tool_choice mode %q", path, tc.Mode)
	}
	for i, allowed := range tc.AllowedTools {
		aPath := fmt.Sprintf("%s.allowed_tools[%d]", path, i)
		if err := validateToolRef(allowed, aPath); err != nil {
			return err
		}
	}
	return nil
}

// validateToolRef checks ref.Kind and requires a non-empty Name on function and
// custom tool references. Client search and hosted references allow empty names
// because wire items such as tool_search omit bare function identifiers.
func validateToolRef(ref ToolRef, path string) error {
	switch ref.Kind {
	case ToolFunction, ToolCustom:
		if strings.TrimSpace(ref.Name) == "" {
			return fmt.Errorf("%s: %s tool reference requires non-empty name", path, ref.Kind)
		}
		return nil
	case ToolClientSearch, ToolHosted:
		return nil
	default:
		return fmt.Errorf("%s: invalid tool reference kind %q", path, ref.Kind)
	}
}

// validateMedia checks modality, source encoding tag, and payload presence on m.
// Stream-start skeletons allow empty Data when allowEmptySkeleton is true, while
// request, response, and part_end media items require non-empty Data.
func validateMedia(m Media, path string, allowEmptySkeleton bool) error {
	switch m.Type {
	case MediaImage, MediaAudio, MediaVideo, MediaDocument:
	default:
		return fmt.Errorf("%s: invalid media type %q", path, m.Type)
	}
	switch m.Source {
	case MediaSourceBase64, MediaSourceURL, MediaSourceFileID:
	default:
		return fmt.Errorf("%s: invalid media source %q", path, m.Source)
	}
	if !allowEmptySkeleton && m.Data == "" {
		return fmt.Errorf("%s: empty media data", path)
	}
	return nil
}

// validateOpaqueItem verifies non-empty Origin, Type, and syntactically valid
// single-value Raw JSON on item. Missing discriminators, empty byte buffers, or
// malformed JSON payloads return a path-qualified validation error.
func validateOpaqueItem(item OpaqueItem, path string) error {
	if strings.TrimSpace(string(item.Origin)) == "" {
		return fmt.Errorf("%s: empty opaque origin", path)
	}
	if strings.TrimSpace(item.Type) == "" {
		return fmt.Errorf("%s: empty opaque item type", path)
	}
	if len(item.Raw) == 0 {
		return fmt.Errorf("%s: empty opaque raw JSON", path)
	}
	return validateRawJSON(item.Raw, path+".raw")
}

// validateReplayToken checks optional scheme labels, nonnegative provider IDs,
// and non-empty Data on tok. Nil tokens succeed, and stream-start skeletons
// allow empty Data until signature deltas complete at part_end.
func validateReplayToken(tok *ReplayToken, path string, allowEmptySkeleton bool) error {
	if tok == nil {
		return nil
	}
	if tok.Scheme != "" && !IsCarrierScheme(tok.Scheme) && tok.Scheme != SchemeUnknown {
		return fmt.Errorf("%s: invalid replay token scheme %q", path, tok.Scheme)
	}
	if tok.ProviderID < 0 {
		return fmt.Errorf("%s: negative replay token provider_id %d", path, tok.ProviderID)
	}
	if !allowEmptySkeleton && tok.Data == "" {
		return fmt.Errorf("%s: empty replay token data", path)
	}
	return nil
}

// validateCacheMark checks cache_control TTL strings ("", "5m", or "1h") and
// nested Extra members on mark. Nil cache marks return nil immediately, while
// unrecognized TTL strings return a path-qualified error.
func validateCacheMark(mark *CacheMark, path string) error {
	if mark == nil {
		return nil
	}
	switch mark.TTL {
	case "", "5m", "1h":
	default:
		return fmt.Errorf("%s: invalid cache_control ttl %q", path, mark.TTL)
	}
	return validateExtra(mark.Extra, path+".extra")
}

// validateReasoningConfig checks Mode, Summary, and BudgetTokens bounds on rc.
// Effort strings stay unconstrained during request validation so target encoders
// enforce dialect-specific effort rules and HTTP 400 status mappings.
func validateReasoningConfig(rc ReasoningConfig, path string) error {
	switch rc.Mode {
	case "", "enabled", "adaptive", "disabled":
	default:
		return fmt.Errorf("%s.mode: invalid reasoning mode %q", path, rc.Mode)
	}
	switch rc.Summary {
	case "", "auto", "concise", "detailed":
	default:
		return fmt.Errorf("%s.summary: invalid reasoning summary %q", path, rc.Summary)
	}
	// Gemini thinkingConfig represents dynamic thinking with thinkingBudget -1,
	// so decoded budgets down to -1 remain valid while values below -1 fail.
	if rc.BudgetTokens != nil && *rc.BudgetTokens < -1 {
		return fmt.Errorf("%s.budget_tokens: invalid budget %d", path, *rc.BudgetTokens)
	}
	return nil
}

// validateOutputFormat checks Kind and raw Schema JSON syntax on out. Non-empty
// Schema bytes require Kind to equal "json_schema" and must decode as one
// complete JSON value without duplicate keys.
func validateOutputFormat(out OutputFormat, path string) error {
	switch out.Kind {
	case "", "text", "json_object", "json_schema":
	default:
		return fmt.Errorf("%s.kind: invalid output format kind %q", path, out.Kind)
	}
	if len(out.Schema) > 0 {
		if out.Kind != "json_schema" {
			return fmt.Errorf("%s: schema requires kind \"json_schema\", got %q", path, out.Kind)
		}
		if err := validateRawJSON(out.Schema, path+".schema"); err != nil {
			return err
		}
	}
	return nil
}

// validateUsage checks that all scalar token counters and populated per-TTL
// cache-write pointers on u are nonnegative. Any negative scalar or TTL bucket
// returns a path-qualified validation error.
func validateUsage(u *Usage, path string) error {
	if u.Input < 0 || u.CacheRead < 0 || u.CacheWrite < 0 ||
		u.Output < 0 || u.Reasoning < 0 ||
		u.ImageInput < 0 || u.ImageOutput < 0 ||
		u.AudioInput < 0 || u.AudioOutput < 0 ||
		u.AcceptedPrediction < 0 || u.RejectedPrediction < 0 {
		return fmt.Errorf("%s: usage token counters must be nonnegative", path)
	}
	for _, entry := range []struct {
		name string // JSON field label for error reporting
		val  *int64 // optional TTL cache-write bucket pointer
	}{
		{"cache_write_5m", u.CacheWrite5m},
		{"cache_write_30m", u.CacheWrite30m},
		{"cache_write_1h", u.CacheWrite1h},
		{"cache_write_12h", u.CacheWrite12h},
	} {
		if entry.val != nil && *entry.val < 0 {
			return fmt.Errorf("%s.%s: negative cache-write count %d", path, entry.name, *entry.val)
		}
	}
	return nil
}

// validateExtra verifies that non-zero Extra maps specify a non-empty Origin
// protocol and syntactically valid JSON member values. Zero Extra values return
// nil immediately even when Origin was pre-populated.
func validateExtra(e Extra, path string) error {
	if e.IsZero() {
		return nil
	}
	if strings.TrimSpace(string(e.Origin)) == "" {
		return fmt.Errorf("%s: non-empty extra members require origin protocol", path)
	}
	for k, raw := range e.Members {
		if err := validateRawJSON(raw, fmt.Sprintf("%s.members[%q]", path, k)); err != nil {
			return err
		}
	}
	return nil
}

// validateRawJSON checks that raw encodes a single complete JSON value without
// duplicate object keys while tolerating invalid UTF-8 inside JSON strings.
// Empty buffers, syntax errors, or trailing tokens return a path-qualified error.
func validateRawJSON(raw jsontext.Value, path string) error {
	if len(bytes.TrimSpace(raw)) == 0 {
		return fmt.Errorf("%s: empty raw JSON value", path)
	}
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	if _, err := dec.ReadValue(); err != nil {
		return fmt.Errorf("%s: invalid raw JSON: %w", path, err)
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%s: trailing JSON token after value", path)
		}
		return fmt.Errorf("%s: trailing JSON syntax error: %w", path, err)
	}
	return nil
}

// isValidRole reports whether r is one of the five canonical conversation Role values.
func isValidRole(r Role) bool {
	switch r {
	case RoleSystem, RoleDeveloper, RoleUser, RoleAssistant, RoleTool:
		return true
	default:
		return false
	}
}
