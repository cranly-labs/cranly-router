package openai

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// TestNormalizeResponsesStreamingTerminalOutputPreservesReportedItems repairs
// an empty terminal array from two completed items. It checks their selected
// identities, encrypted reasoning, message status/phase, and text in the
// rebuilt output.
func TestNormalizeResponsesStreamingTerminalOutputPreservesReportedItems(t *testing.T) {
	doneItems := NewResponsesStreamOutputItems()

	doneItems.Observe([]byte(`{
		"type":"response.output_item.done",
		"output_index":0,
		"item":{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"thinking"}],"encrypted_content":"opaque"}
	}`))
	doneItems.Observe([]byte(`{
		"type":"response.output_item.done",
		"output_index":1,
		"item":{"id":"msg_1","type":"message","status":"completed","phase":"final_answer","role":"assistant","content":[{"type":"output_text","text":"shipped","annotations":[],"logprobs":[]}]}
	}`))

	normalized, changed := NormalizeResponsesStreamingTerminalOutput(
		[]byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`),
		nil,
		doneItems,
		nil,
	)
	require.True(t, changed)

	output := gjson.GetBytes(normalized, "response.output")
	require.True(t, output.IsArray())
	require.Len(t, output.Array(), 2, "both reported items must survive")

	require.Equal(t, "reasoning", gjson.GetBytes(normalized, "response.output.0.type").String())
	require.Equal(t, "rs_1", gjson.GetBytes(normalized, "response.output.0.id").String())
	require.Equal(t, "opaque", gjson.GetBytes(normalized, "response.output.0.encrypted_content").String(),
		"fields the gateway does not model must survive verbatim")

	require.Equal(t, "message", gjson.GetBytes(normalized, "response.output.1.type").String())
	require.Equal(t, "msg_1", gjson.GetBytes(normalized, "response.output.1.id").String(),
		"the reported id must be reused, not regenerated")
	require.Equal(t, "completed", gjson.GetBytes(normalized, "response.output.1.status").String())
	require.Equal(t, "final_answer", gjson.GetBytes(normalized, "response.output.1.phase").String())
	require.Equal(t, "shipped", gjson.GetBytes(normalized, "response.output.1.content.0.text").String())
}

// TestResponsesStreamOutputItemsOrderByOutputIndex observes two completed
// items whose indexes arrive in reverse order. The encoded output must put the
// lower-index identity before the higher-index identity.
func TestResponsesStreamOutputItemsOrderByOutputIndex(t *testing.T) {
	doneItems := NewResponsesStreamOutputItems()
	doneItems.Observe([]byte(`{"type":"response.output_item.done","output_index":2,"item":{"id":"c","type":"message"}}`))
	doneItems.Observe([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"id":"a","type":"reasoning"}}`))

	built, ok := doneItems.BuildOutput()
	require.True(t, ok)
	require.Equal(t, "a", gjson.GetBytes(built, "0.id").String())
	require.Equal(t, "c", gjson.GetBytes(built, "1.id").String())
}

// TestNormalizeResponsesStreamingTerminalOutputIgnoresNonDoneEvents supplies
// an added event and a text delta to the completed-item recorder. No items are
// stored, and an empty terminal remains unchanged.
func TestNormalizeResponsesStreamingTerminalOutputIgnoresNonDoneEvents(t *testing.T) {
	doneItems := NewResponsesStreamOutputItems()
	doneItems.Observe([]byte(`{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message"}}`))
	doneItems.Observe([]byte(`{"type":"response.output_text.delta","output_index":0,"delta":"hi"}`))
	require.False(t, doneItems.HasItems())

	raw := []byte(`{"type":"response.completed","response":{"status":"completed","output":[]}}`)
	normalized, changed := NormalizeResponsesStreamingTerminalOutput(raw, nil, doneItems, nil)
	require.False(t, changed)
	require.Equal(t, string(raw), string(normalized))
}

// TestNormalizeResponsesStreamingTerminalOutputRepairsTruncatedOutput combines
// two completed records with a one-item terminal array. Repair must restore
// two items and replace the synthetic identity with the reported identities.
func TestNormalizeResponsesStreamingTerminalOutputRepairsTruncatedOutput(t *testing.T) {
	doneItems := NewResponsesStreamOutputItems()
	doneItems.Observe([]byte(`{
		"type":"response.output_item.done","output_index":0,
		"item":{"id":"rs_real","type":"reasoning","status":"in_progress","summary":[]}
	}`))
	doneItems.Observe([]byte(`{
		"type":"response.output_item.done","output_index":1,
		"item":{"id":"msg_real","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","text":"shipped","annotations":[],"logprobs":[]}]}
	}`))

	normalized, changed := NormalizeResponsesStreamingTerminalOutput([]byte(`{
		"type":"response.completed",
		"response":{"status":"completed","output":[{"type":"message","role":"assistant","id":"msg_fabricated","status":"completed","content":[{"type":"output_text","text":"shipped","annotations":[],"logprobs":[]}]}]}
	}`), nil, doneItems, nil)
	require.True(t, changed)

	require.Len(t, gjson.GetBytes(normalized, "response.output").Array(), 2)
	require.Equal(t, "reasoning", gjson.GetBytes(normalized, "response.output.0.type").String())
	require.Equal(t, "rs_real", gjson.GetBytes(normalized, "response.output.0.id").String())
	require.Equal(t, "msg_real", gjson.GetBytes(normalized, "response.output.1.id").String(),
		"the id the stream reported must replace the fabricated one")
}

// TestNormalizeResponsesStreamingTerminalOutputLeavesCompleteOutputAlone
// compares a one-item terminal array with one stored done index. Equal counts
// keep the complete terminal bytes despite different item identities.
func TestNormalizeResponsesStreamingTerminalOutputLeavesCompleteOutputAlone(t *testing.T) {
	doneItems := NewResponsesStreamOutputItems()
	doneItems.Observe([]byte(`{
		"type":"response.output_item.done","output_index":0,
		"item":{"id":"msg_real","type":"message","status":"completed"}
	}`))

	raw := []byte(`{"type":"response.completed","response":{"status":"completed","output":[{"type":"message","id":"msg_upstream","status":"completed","vendor":"keep"}]}}`)
	normalized, changed := NormalizeResponsesStreamingTerminalOutput(raw, nil, doneItems, nil)
	require.False(t, changed)
	require.Equal(t, string(raw), string(normalized))
}

// TestBufferedResponseAccumulator_TextOnly appends two visible text fragments.
// HasContent must become true, and reconstruction must produce one assistant
// message with one output-text part containing their concatenation.
func TestBufferedResponseAccumulator_TextOnly(t *testing.T) {
	acc := NewBufferedResponseAccumulator()

	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.output_text.delta", Delta: "Hello"})
	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.output_text.delta", Delta: ", world!"})

	assert.True(t, acc.HasContent())

	output := acc.BuildOutput()
	require.Len(t, output, 1)
	assert.Equal(t, "message", output[0].Type)
	assert.Equal(t, "assistant", output[0].Role)
	require.Len(t, output[0].Content, 1)
	assert.Equal(t, "output_text", output[0].Content[0].Type)
	assert.Equal(t, "Hello, world!", output[0].Content[0].Text)
}

// TestBufferedResponseAccumulator_ToolCalls announces one function and
// supplies two argument fragments. It checks content presence and one
// reconstructed call retaining correlation/name and complete concatenated
// argument text.
func TestBufferedResponseAccumulator_ToolCalls(t *testing.T) {
	acc := NewBufferedResponseAccumulator()

	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:        "response.output_item.added",
		OutputIndex: 1,
		Item: &ResponsesOutput{
			Type:   "function_call",
			CallID: "call_abc",
			Name:   "get_weather",
		},
	})
	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:        "response.function_call_arguments.delta",
		OutputIndex: 1,
		Delta:       `{"city":`,
	})
	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:        "response.function_call_arguments.delta",
		OutputIndex: 1,
		Delta:       `"NYC"}`,
	})

	assert.True(t, acc.HasContent())

	output := acc.BuildOutput()
	require.Len(t, output, 1)
	assert.Equal(t, "function_call", output[0].Type)
	assert.Equal(t, "call_abc", output[0].CallID)
	assert.Equal(t, "get_weather", output[0].Name)
	assert.Equal(t, `{"city":"NYC"}`, output[0].Arguments)
}

// TestBufferedResponseAccumulator_Reasoning appends two reasoning-summary
// fragments. Reconstruction must return one reasoning item with one summary-
// text member containing the complete concatenation, after HasContent becomes
// true.
func TestBufferedResponseAccumulator_Reasoning(t *testing.T) {
	acc := NewBufferedResponseAccumulator()

	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.reasoning_summary_text.delta", Delta: "Step 1: "})
	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.reasoning_summary_text.delta", Delta: "think about it"})

	assert.True(t, acc.HasContent())

	output := acc.BuildOutput()
	require.Len(t, output, 1)
	assert.Equal(t, "reasoning", output[0].Type)
	require.Len(t, output[0].Summary, 1)
	assert.Equal(t, "summary_text", output[0].Summary[0].Type)
	assert.Equal(t, "Step 1: think about it", output[0].Summary[0].Text)
}

// TestBufferedResponseAccumulator_Mixed supplies reasoning, visible text, then
// one call with arguments. It checks three ordered reconstructed kinds,
// retained message text, and the selected function name.
func TestBufferedResponseAccumulator_Mixed(t *testing.T) {
	acc := NewBufferedResponseAccumulator()

	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.reasoning_summary_text.delta", Delta: "I thought about it."})
	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.output_text.delta", Delta: "The answer is 42."})
	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:        "response.output_item.added",
		OutputIndex: 2,
		Item: &ResponsesOutput{
			Type:   "function_call",
			CallID: "call_1",
			Name:   "verify",
		},
	})
	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:        "response.function_call_arguments.delta",
		OutputIndex: 2,
		Delta:       `{}`,
	})

	assert.True(t, acc.HasContent())

	output := acc.BuildOutput()
	require.Len(t, output, 3)
	assert.Equal(t, "reasoning", output[0].Type)
	assert.Equal(t, "message", output[1].Type)
	assert.Equal(t, "function_call", output[2].Type)
	assert.Equal(t, "The answer is 42.", output[1].Content[0].Text)
	assert.Equal(t, "verify", output[2].Name)
}

// TestBufferedResponseAccumulator_SupplementEmptyOutput supplies visible
// deltas beside a terminal response with nil output. Supplementation must
// create the inspected message/text while retaining the asserted input usage
// count.
func TestBufferedResponseAccumulator_SupplementEmptyOutput(t *testing.T) {
	acc := NewBufferedResponseAccumulator()
	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.output_text.delta", Delta: "Hello"})

	resp := &ResponsesResponse{
		ID:     "resp_1",
		Status: "completed",
		Output: nil,
		Usage:  &ResponsesUsage{InputTokens: 10, OutputTokens: 5},
	}

	acc.SupplementResponseOutput(resp)

	require.Len(t, resp.Output, 1)
	assert.Equal(t, "message", resp.Output[0].Type)
	assert.Equal(t, "Hello", resp.Output[0].Content[0].Text)
	assert.Equal(t, 10, resp.Usage.InputTokens)
}

// TestBufferedResponseAccumulator_NoSupplementWhenOutputExists supplies delta
// text beside populated terminal output. The terminal's single message and
// text must retain precedence after the supplement helper is called.
func TestBufferedResponseAccumulator_NoSupplementWhenOutputExists(t *testing.T) {
	acc := NewBufferedResponseAccumulator()
	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.output_text.delta", Delta: "from deltas"})

	resp := &ResponsesResponse{
		ID:     "resp_2",
		Status: "completed",
		Output: []ResponsesOutput{
			{
				Type: "message",
				Content: []ResponsesContentPart{
					{Type: "output_text", Text: "from terminal event"},
				},
			},
		},
	}

	acc.SupplementResponseOutput(resp)

	require.Len(t, resp.Output, 1)
	assert.Equal(t, "from terminal event", resp.Output[0].Content[0].Text)
}

// TestBufferedResponseAccumulator_EmptyDeltas supplies empty text and an
// unrelated creation event. HasContent remains false, and supplementing an
// empty terminal leaves its output slice nil.
func TestBufferedResponseAccumulator_EmptyDeltas(t *testing.T) {
	acc := NewBufferedResponseAccumulator()

	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.output_text.delta", Delta: ""})
	acc.ProcessEvent(&ResponsesStreamEvent{Type: "response.created"})

	assert.False(t, acc.HasContent())

	resp := &ResponsesResponse{ID: "resp_3", Status: "completed"}
	acc.SupplementResponseOutput(resp)
	assert.Nil(t, resp.Output)
}

// TestBufferedResponseAccumulator_IgnoresNonFunctionCallItems announces a
// message item without text deltas. HasContent must remain false, protecting
// the no-recorded-content result for that unsupported addition kind.
func TestBufferedResponseAccumulator_IgnoresNonFunctionCallItems(t *testing.T) {
	acc := NewBufferedResponseAccumulator()

	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:        "response.output_item.added",
		OutputIndex: 0,
		Item:        &ResponsesOutput{Type: "message"},
	})

	assert.False(t, acc.HasContent())
}

// TestBufferedResponseAccumulator_CodexEvents supplies a custom addition/input
// fragment and raw reasoning text. HasContent must report true and BuildOutput
// must retain both the reasoning item and raw custom patch arguments.
func TestBufferedResponseAccumulator_CodexEvents(t *testing.T) {
	acc := NewBufferedResponseAccumulator()
	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:        "response.output_item.added",
		OutputIndex: 0,
		Item:        &ResponsesOutput{Type: "custom_tool_call", CallID: "c1", Name: "apply_patch"},
	})
	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:        "response.custom_tool_call_input.delta",
		OutputIndex: 0,
		Delta:       "patch-body",
	})
	acc.ProcessEvent(&ResponsesStreamEvent{
		Type:  "response.reasoning_text.delta",
		Delta: "raw-reasoning",
	})
	require.True(t, acc.HasContent())
	out := acc.BuildOutput()
	require.Len(t, out, 2)
	require.Equal(t, "raw-reasoning", out[0].Summary[0].Text)
	require.Equal(t, "patch-body", out[1].Arguments)
}

// TestReconstructResponseOutputFromSSE_RawDoneCompactionAndDeltaFallback feeds
// buffered SSE bodies covering completed items, added-only compaction items,
// and delta-only message text. Reconstruction must prefer raw done items over
// deltas, retain added compaction items, and recover delta text when no done
// items exist.
func TestReconstructResponseOutputFromSSE_RawDoneCompactionAndDeltaFallback(t *testing.T) {
	withDone := strings.Join([]string{
		`data: {"type":"response.output_text.delta","delta":"hel"}`,
		`data: {"type":"response.output_text.delta","delta":"lo"}`,
		`data: {"type":"response.output_item.done","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}}`,
		`data: {"type":"response.completed","response":{"id":"resp_1","output":[]}}`,
	}, "\n")
	outJSON, ok := ReconstructResponseOutputFromSSE(withDone)
	require.True(t, ok)
	items := gjson.ParseBytes(outJSON).Array()
	require.Len(t, items, 1)
	require.Equal(t, "msg_1", items[0].Get("id").String())

	compactionOnly := `data: {"type":"response.output_item.added","output_index":0,"item":{"id":"cmp_1","type":"compaction","encrypted_content":"enc"}}`
	outJSON, ok = ReconstructResponseOutputFromSSE(compactionOnly)
	require.True(t, ok)
	items = gjson.ParseBytes(outJSON).Array()
	require.Len(t, items, 1)
	require.Equal(t, "compaction", items[0].Get("type").String())
	require.Equal(t, "enc", items[0].Get("encrypted_content").String())

	deltaOnly := `data: {"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message","content":[]}}` + "\n" +
		`data: {"type":"response.output_text.delta","delta":"from-delta"}`
	outJSON, ok = ReconstructResponseOutputFromSSE(deltaOnly)
	require.True(t, ok)
	items = gjson.ParseBytes(outJSON).Array()
	require.Len(t, items, 1)
	require.Equal(t, "from-delta", items[0].Get("content.0.text").String())
}
