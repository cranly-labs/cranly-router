package openai

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// response_reconstruction.go reconstructs Responses terminal output arrays from
// completed stream items or accumulated delta fragments.
// A Responses terminal event may carry fewer output items than the preceding
// response.output_item.done events or arrive with an empty output array. Keeping
// each raw completed item preserves its reported identity, content structure,
// and unmodeled vendor fields when rebuilding that terminal array. Streaming
// reconstruction orders stored items by output_index and replaces repeated
// observations at the same index, whereas buffered SSE reconstruction retains
// arrival order and deduplicates by item identity.
//
// When completed items are absent, delta accumulation supplies modeled
// reasoning, message text, and function calls followed by separately observed
// image-generation items. Selecting completed items before delta accumulation
// prevents combining two representations of the same turn, while added
// compaction items supplement buffered reconstruction only when no completed
// compaction item was recorded.

// bufferedFuncCall stores added-item call identity and accumulated raw argument
// fragments for one delta-reconstructed tool invocation.
type bufferedFuncCall struct {
	// CallID and Name record initial tool metadata announced on
	// response.output_item.added. Subsequent argument delta events append raw
	// fragments to Args through outputIndexToFuncIdx without overwriting either
	// identity string.
	CallID string // added call correlation identifier
	Name   string // added tool name
	// Args accumulates raw argument fragments in arrival order. Growing funcCalls
	// after writing Args can relocate a nonzero strings.Builder and trigger copy
	// detection on a later write to that earlier call index.
	Args strings.Builder // accumulated raw argument text
}

// BufferedResponseAccumulator collects modeled text, reasoning, and tool-call
// deltas for empty terminal output reconstruction. Construct it with
// NewBufferedResponseAccumulator and serialize mutation on the returned pointer,
// because used strings.Builder fields reject writes after a value copy.
type BufferedResponseAccumulator struct {
	// text and reasoning retain independent delta streams. BuildOutput emits
	// reasoning before message text regardless of arrival order, and reading
	// neither buffer clears accumulated state.
	text      strings.Builder // accumulated visible output text
	reasoning strings.Builder // accumulated reasoning summary text
	// funcCalls and outputIndexToFuncIdx pair call addition order with stream
	// output_index positions. Reused output indexes bind subsequent deltas to
	// the newly appended call record.
	funcCalls            []bufferedFuncCall // tool calls in addition order
	outputIndexToFuncIdx map[int]int        // stream output_index to funcCalls index
}

// ResponsesStreamOutputItems retains copied completed-item JSON keyed by stream
// output_index for terminal output reconstruction. Construct it with
// NewResponsesStreamOutputItems before observing events, and serialize access
// because the underlying map has no internal synchronization.
type ResponsesStreamOutputItems struct {
	// items owns copied object JSON keyed by output_index; later events at the
	// same index overwrite earlier snapshots before BuildOutput sorts keys.
	items map[int]json.RawMessage
}

// NewBufferedResponseAccumulator allocates an empty delta accumulator with an
// initialized output-index lookup map. Callers serialize mutation and keep the
// returned accumulator referenced by pointer so its internal strings.Builder
// values are never copied after writes.
func NewBufferedResponseAccumulator() *BufferedResponseAccumulator {
	return &BufferedResponseAccumulator{
		outputIndexToFuncIdx: make(map[int]int),
	}
}

// ProcessEvent records output-text deltas, reasoning deltas, and function or
// custom tool additions plus their indexed argument fragments. Callers pass a
// non-nil event to an accumulator created by NewBufferedResponseAccumulator;
// unrelated event types and completed item snapshots are ignored.
//
// Call correlation and name come from response.output_item.added, while custom
// tool input deltas append raw text into Args. Appending a new call after
// writing arguments to an earlier call can grow funcCalls and relocate a used
// strings.Builder, which panics if that earlier call receives another delta.
func (a *BufferedResponseAccumulator) ProcessEvent(event *ResponsesStreamEvent) {
	switch event.Type {
	case "response.output_text.delta":
		if event.Delta != "" {
			// Retain visible text fragments in one builder until terminal output
			// reconstruction; strings.Builder.WriteString always returns a nil error.
			_, _ = a.text.WriteString(event.Delta)
		}
	case "response.output_item.added":
		if event.Item != nil && (event.Item.Type == "function_call" || event.Item.Type == "custom_tool_call") {
			// Bind output_index at addition time so subsequent argument deltas
			// append to this call's builder without repeating call metadata.
			idx := len(a.funcCalls)
			a.outputIndexToFuncIdx[event.OutputIndex] = idx
			a.funcCalls = append(a.funcCalls, bufferedFuncCall{
				CallID: event.Item.CallID,
				Name:   event.Item.Name,
			})
		}
	case "response.function_call_arguments.delta", "response.custom_tool_call_input.delta":
		if event.Delta != "" {
			// Drop orphan argument deltas whose output_index was never announced
			// rather than synthesizing a tool call with empty identity fields.
			if idx, ok := a.outputIndexToFuncIdx[event.OutputIndex]; ok {
				_, _ = a.funcCalls[idx].Args.WriteString(event.Delta)
			}
		}
	case "response.reasoning_summary_text.delta", "response.reasoning_text.delta":
		if event.Delta != "" {
			// Combine summary and raw reasoning deltas into one summary buffer for
			// fallback output reconstruction when completed items are absent.
			_, _ = a.reasoning.WriteString(event.Delta)
		}
	}
}

// HasContent reports whether the accumulator holds any reasoning bytes, visible
// text bytes, or announced tool calls. Callers pass a non-nil receiver; true
// does not imply that accumulated tool arguments form complete JSON.
func (a *BufferedResponseAccumulator) HasContent() bool {
	return a.text.Len() > 0 || len(a.funcCalls) > 0 || a.reasoning.Len() > 0
}

// BuildOutput returns reconstructed reasoning, assistant message text, and
// function_call items in that fixed group order. Empty reasoning or text groups
// are omitted, and an accumulator without content returns nil; custom tool
// additions are emitted as function_call items carrying raw input in Arguments.
//
// Because delta accumulation does not track cross-kind arrival order, item IDs,
// encrypted tokens, or vendor extension fields, callers prefer raw completed
// items whenever available. Reading the builders leaves accumulated state
// intact for repeated inspection.
func (a *BufferedResponseAccumulator) BuildOutput() []ResponsesOutput {
	var out []ResponsesOutput

	// Emit accumulated reasoning ahead of assistant text and tool calls because
	// separate builders do not record cross-kind interleaving positions.
	if a.reasoning.Len() > 0 {
		out = append(out, ResponsesOutput{
			Type: "reasoning",
			Summary: []ResponsesSummary{{
				Type: "summary_text",
				Text: a.reasoning.String(),
			}},
		})
	}

	if a.text.Len() > 0 {
		out = append(out, ResponsesOutput{
			Type: "message",
			Role: "assistant",
			Content: []ResponsesContentPart{{
				Type: "output_text",
				Text: a.text.String(),
			}},
		})
	}

	for i := range a.funcCalls {
		out = append(out, ResponsesOutput{
			Type:      "function_call",
			CallID:    a.funcCalls[i].CallID,
			Name:      a.funcCalls[i].Name,
			Arguments: a.funcCalls[i].Args.String(),
		})
	}

	return out
}

// SupplementResponseOutput populates resp.Output from accumulated deltas when
// resp is non-nil, resp.Output is empty, and HasContent is true. Existing
// terminal output items always take precedence, and usage or status fields on
// resp remain untouched.
func (a *BufferedResponseAccumulator) SupplementResponseOutput(resp *ResponsesResponse) {
	if resp == nil || len(resp.Output) > 0 {
		return
	}
	if !a.HasContent() {
		return
	}
	// Populate empty terminal output arrays once from accumulated deltas while
	// leaving non-empty upstream terminal output items authoritative.
	resp.Output = a.BuildOutput()
}

// NewResponsesStreamOutputItems allocates an empty completed-item recorder for
// one Responses stream. Its initialized map accepts Observe calls; a zero-value
// receiver supports read queries but cannot store items.
func NewResponsesStreamOutputItems() *ResponsesStreamOutputItems {
	return &ResponsesStreamOutputItems{items: make(map[int]json.RawMessage)}
}

// Observe records the raw item object from a valid response.output_item.done
// JSON payload under its integer output_index. Nil receivers, invalid JSON,
// other event types, and non-object item fields are ignored, while missing or
// non-numeric output_index values default to zero via gjson.Int.
//
// Accepted item bytes are cloned before storage so caller buffer reuse cannot
// mutate retained items, and later done events at the same output_index replace
// earlier snapshots while keeping unmodeled fields intact.
func (r *ResponsesStreamOutputItems) Observe(data []byte) {
	if r == nil || len(data) == 0 || !gjson.ValidBytes(data) {
		return
	}
	if strings.TrimSpace(gjson.GetBytes(data, "type").String()) != "response.output_item.done" {
		return
	}
	item := gjson.GetBytes(data, "item")
	if !item.Exists() || !item.IsObject() {
		return
	}
	index := int(gjson.GetBytes(data, "output_index").Int())
	r.items[index] = json.RawMessage(append([]byte(nil), item.Raw...))
}

// HasItems reports whether r holds at least one recorded completed output item.
func (r *ResponsesStreamOutputItems) HasItems() bool {
	return r != nil && len(r.items) > 0
}

// Count returns the number of distinct output_index entries stored in r, or
// zero when r is nil.
func (r *ResponsesStreamOutputItems) Count() int {
	if r == nil {
		return 0
	}
	return len(r.items)
}

// BuildOutput marshals stored raw items in ascending output_index order and
// returns a newly allocated JSON array slice. A nil or empty recorder or a
// JSON marshaling failure returns nil and false so callers can fall back to
// delta-based output reconstruction.
//
// Sorting recorded map keys restores canonical stream positions when
// response.output_item.done events arrive out of order, while preserving raw
// unmodeled item fields and omitting placeholder entries for skipped indexes.
func (r *ResponsesStreamOutputItems) BuildOutput() ([]byte, bool) {
	if !r.HasItems() {
		return nil, false
	}
	indexes := make([]int, 0, len(r.items))
	for index := range r.items {
		indexes = append(indexes, index)
	}
	slices.Sort(indexes)
	ordered := make([]json.RawMessage, 0, len(indexes))
	for _, index := range indexes {
		ordered = append(ordered, r.items[index])
	}
	encoded, err := json.Marshal(ordered)
	// Return false on marshaling failure so the caller can fall back to delta
	// reconstruction instead of writing a malformed output array.
	if err != nil {
		return nil, false
	}
	return encoded, true
}

// NormalizeResponsesStreamingTerminalOutput repairs missing or truncated
// response.output arrays on completed, done, incomplete, or cancelled terminal
// events. Non-empty terminal arrays whose length is at least doneItems.Count()
// and empty terminal arrays with no recorded items or deltas return data
// unchanged with false.
//
// Repair prefers doneItems.BuildOutput(), then BuildResponsesOutputJSON(acc,
// imageOutputs), and otherwise writes an empty JSON array. Non-terminal event
// types and sjson patch failures return data unchanged with false.
func NormalizeResponsesStreamingTerminalOutput(
	data []byte,
	acc *BufferedResponseAccumulator,
	doneItems *ResponsesStreamOutputItems,
	imageOutputs []json.RawMessage,
) ([]byte, bool) {
	eventType := strings.TrimSpace(gjson.GetBytes(data, "type").String())
	switch eventType {
	case "response.completed", "response.done", "response.incomplete", "response.cancelled", "response.canceled":
	default:
		return data, false
	}

	output := gjson.GetBytes(data, "response.output")
	hasAccumulatedOutput := (acc != nil && acc.HasContent()) || len(imageOutputs) > 0 || doneItems.HasItems()
	if output.Exists() && output.IsArray() {
		terminalCount := len(output.Array())
		// Keep populated upstream terminal arrays whose length covers all
		// observed done indexes even when upstream item IDs differ.
		if terminalCount > 0 && terminalCount >= doneItems.Count() {
			return data, false
		}
		if terminalCount == 0 && !hasAccumulatedOutput {
			return data, false
		}
	}

	outputJSON := []byte("[]")
	// Prefer complete done-item JSON over modeled delta reconstruction so raw
	// IDs, encrypted_content, and image items are not duplicated or stripped.
	if reconstructed, ok := doneItems.BuildOutput(); ok {
		outputJSON = reconstructed
	} else if reconstructed, ok := BuildResponsesOutputJSON(acc, imageOutputs); ok {
		outputJSON = reconstructed
	}
	updated, err := sjson.SetRawBytes(data, "response.output", outputJSON)
	// Preserve the original terminal event payload when sjson patching fails so
	// callers still observe upstream completion status and usage metadata.
	if err != nil {
		return data, false
	}
	return updated, true
}

// ReconstructResponseOutputFromSSE builds a JSON output array from a buffered
// SSE stream body when a terminal response arrives with an empty output array.
// It checks CollectRawResponsesOutputItemsFromSSE first so completed raw item
// IDs, compaction payloads, and unmodeled vendor fields survive verbatim
// without appending duplicate image-generation items.
//
// When raw item collection yields nothing, recognized delta events feed a fresh
// BufferedResponseAccumulator alongside deduplicated image-generation items
// extracted from SSE payloads. Empty streams or JSON marshaling failures return
// nil and false.
func ReconstructResponseOutputFromSSE(bodyText string) ([]byte, bool) {
	// Use raw completed items first because typed delta accumulation cannot
	// preserve compaction items or unmodeled vendor fields.
	if outputJSON, ok := CollectRawResponsesOutputItemsFromSSE(bodyText); ok {
		return outputJSON, true
	}
	acc := NewBufferedResponseAccumulator()
	imageOutputs := make([]json.RawMessage, 0, 1)
	seenImages := make(map[string]struct{})
	ForEachSSEDataPayload(bodyText, func(data []byte) {
		if imageOutput, ok := ExtractImageGenerationOutputFromSSEData(data, seenImages); ok {
			imageOutputs = append(imageOutputs, imageOutput)
		}
		eventType := strings.TrimSpace(gjson.GetBytes(data, "type").String())
		if ResponsesStreamEventMayContributeToOutput(eventType) {
			var event ResponsesStreamEvent
			// Skip malformed individual delta frames so later valid SSE frames
			// in the same buffered body still contribute to output recovery.
			if err := json.Unmarshal(data, &event); err == nil {
				acc.ProcessEvent(&event)
			}
		}
	})
	return BuildResponsesOutputJSON(acc, imageOutputs)
}

// CollectRawResponsesOutputItemsFromSSE collects object items from
// response.output_item.done SSE payloads in arrival order, deduplicating by
// trimmed non-empty item.id or full raw item JSON when id is blank. Completed
// image-generation status fields are normalized before inspection.
//
// If no done compaction item was collected, a second pass appends compaction
// items from response.output_item.added payloads using the same deduplication
// set. Empty results or JSON encoding errors return nil and false.
func CollectRawResponsesOutputItemsFromSSE(bodyText string) ([]byte, bool) {
	var items []json.RawMessage
	seen := make(map[string]struct{})
	hasCompactionItem := false
	appendItem := func(item gjson.Result) {
		if !item.Exists() || !item.IsObject() {
			return
		}
		key := strings.TrimSpace(item.Get("id").String())
		if key == "" {
			key = item.Raw
		}
		// Keep the first completed representation of each item key so repeated
		// done frames do not duplicate output entries in buffered responses.
		if _, dup := seen[key]; dup {
			return
		}
		seen[key] = struct{}{}
		if IsResponsesCompactionItemType(item.Get("type").String()) {
			hasCompactionItem = true
		}
		items = append(items, json.RawMessage(item.Raw))
	}
	ForEachSSEDataPayload(bodyText, func(data []byte) {
		// Normalize in-progress status on completed image items before storing
		// raw item bytes for buffered terminal output.
		if normalized, changed := NormalizeCompletedImageGenerationStatus(data); changed {
			data = normalized
		}
		if strings.TrimSpace(gjson.GetBytes(data, "type").String()) != "response.output_item.done" {
			return
		}
		appendItem(gjson.GetBytes(data, "item"))
	})
	// Fall back to response.output_item.added for compaction items only when no
	// completed compaction item appeared in the done pass.
	if !hasCompactionItem {
		ForEachSSEDataPayload(bodyText, func(data []byte) {
			if strings.TrimSpace(gjson.GetBytes(data, "type").String()) != "response.output_item.added" {
				return
			}
			item := gjson.GetBytes(data, "item")
			if !IsResponsesCompactionItemType(item.Get("type").String()) {
				return
			}
			appendItem(item)
		})
	}
	if len(items) == 0 {
		return nil, false
	}
	// Return false if a raw item slice fails JSON marshaling so callers can try
	// typed delta reconstruction instead.
	outputJSON, err := json.Marshal(items)
	if err != nil {
		return nil, false
	}
	return outputJSON, true
}

// BuildResponsesOutputJSON encodes accumulated delta output items followed by
// supplied raw imageOutputs into a single JSON array slice. When both sources
// have no content or JSON marshaling or unmarshaling fails, it returns nil and
// false without partial output.
//
// Modeled accumulator items are marshaled and re-sliced into json.RawMessage
// elements before appending imageOutputs so raw image-generation objects join
// the same array without losing unmodeled vendor fields.
func BuildResponsesOutputJSON(acc *BufferedResponseAccumulator, imageOutputs []json.RawMessage) ([]byte, bool) {
	if (acc == nil || !acc.HasContent()) && len(imageOutputs) == 0 {
		return nil, false
	}

	var output []json.RawMessage
	if acc != nil && acc.HasContent() {
		// Re-slice modeled items as json.RawMessage entries before appending raw
		// image items; reject the combined result if either conversion fails.
		outputJSON, err := json.Marshal(acc.BuildOutput())
		if err != nil {
			return nil, false
		}
		if err := json.Unmarshal(outputJSON, &output); err != nil {
			return nil, false
		}
	}
	output = append(output, imageOutputs...)

	// Reject the entire output array if any supplied raw image entry is invalid
	// JSON rather than emitting a partially constructed array.
	outputJSON, err := json.Marshal(output)
	if err != nil {
		return nil, false
	}
	return outputJSON, true
}
