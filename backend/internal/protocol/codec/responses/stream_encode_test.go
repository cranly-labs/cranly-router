package responses

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// stream_encode_test.go verifies incremental OpenAI Responses SSE encoding
// across prefix-guarded argument flushes, Semantic and Terminal frame flags,
// echoed client request fields, and punctuated client tool names. Every
// emitted event sequence must pass strict Responses stream grammar validation
// and round-trip cleanly through StreamDecoder without unexpected losses.

// TestStreamEncoderHasPrefixSuffixGuardAndSemanticFlags exercises StreamEncoder
// on mismatched versus extending EventPartEnd tool-argument snapshots. Non-prefix
// final snapshots must not emit corrupted suffix deltas, argument deltas must
// omit name fields, and Semantic/Terminal flags must match frame roles.
func TestStreamEncoderHasPrefixSuffixGuardAndSemanticFlags(t *testing.T) {
	events := []ir.Event{
		{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "resp_guard", Model: "gpt-5", CreatedAt: 1760000000}},
		{
			Kind:  ir.EventPartStart,
			Index: 0,
			Part:  &ir.Part{Kind: ir.PartToolCall, ID: "fc_1", Call: &ir.ToolCall{CallID: "call_1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "lookup"}}},
		},
		{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Arguments: `{ "a" : 1 }`}},
		{
			Kind:  ir.EventPartEnd,
			Index: 0,
			// Compacted final snapshot has a different prefix than the spaced delta;
			// strings.HasPrefix must prevent appending a corrupted suffix delta.
			Part: &ir.Part{Kind: ir.PartToolCall, ID: "fc_1", Call: &ir.ToolCall{CallID: "call_1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "lookup"}, Arguments: `{"a":1}`}},
		},
		{
			Kind:  ir.EventPartStart,
			Index: 1,
			Part:  &ir.Part{Kind: ir.PartToolCall, ID: "fc_2", Call: &ir.ToolCall{CallID: "call_2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "one_shot"}}},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 1,
			// One-shot final arguments extend the empty emitted prefix and must flush as a delta.
			Part: &ir.Part{Kind: ir.PartToolCall, ID: "fc_2", Call: &ir.ToolCall{CallID: "call_2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "one_shot"}, Arguments: `{"ok":true}`}},
		},
		{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishToolCalls}},
	}

	frames, losses := encodeFullEventStream(t, ir.ClientView{
		Model:   "gpt-5",
		Runtime: newDeterministicRuntime(),
	}, events)
	require.Empty(t, losses)
	require.Empty(t, testkit.ValidateResponsesStream(frames, testkit.GrammarOptions{}))

	deltaCountFC1 := 0
	deltaCountFC2 := 0
	terminalCount := 0
	for i, f := range frames {
		if f.Terminal {
			terminalCount++
			require.Equal(t, len(frames)-1, i, "only final lifecycle frame may be Terminal")
		}
		if f.Event == "response.created" || f.Event == "response.in_progress" {
			require.False(t, f.Semantic, "initial lifecycle frames must not set Semantic=true")
		}
		if f.Event == "response.function_call_arguments.delta" {
			require.True(t, f.Semantic)
			require.False(t, strings.Contains(string(f.Data), `"name"`), "argument delta must omit name field")
			if bytes.Contains(f.Data, []byte(`"fc_1"`)) {
				deltaCountFC1++
			}
			if bytes.Contains(f.Data, []byte(`"fc_2"`)) {
				deltaCountFC2++
			}
		}
	}
	require.Equal(t, 1, deltaCountFC1, "non-prefix finalArgs on fc_1 must not emit a second suffix delta")
	require.Equal(t, 1, deltaCountFC2, "one-shot finalArgs on fc_2 must flush one suffix delta")
	require.Equal(t, 1, terminalCount)
}

// TestStreamEncoderEchoesAllNineClientRequestFields streams a text reply through
// StreamEncoder with all nine section 3.6 ClientView.Request fields populated.
// Every response.created, response.in_progress, and response.completed envelope
// must carry all nine echoed fields and pass strict grammar validation.
func TestStreamEncoderEchoesAllNineClientRequestFields(t *testing.T) {
	parallel := false
	store := true
	maxOut := int64(512)
	clientReq := &ir.Request{
		System:             []ir.Part{{Kind: ir.PartText, Text: "System echo."}},
		MaxOutputTokens:    &maxOut,
		ParallelToolCalls:  &parallel,
		PreviousResponseID: "resp_prev_stream",
		Store:              &store,
		Reasoning:          &ir.ReasoningConfig{Effort: "medium", Summary: "auto"},
		Output:             &ir.OutputFormat{Kind: "text", Verbosity: "high"},
		Tools: []ir.Tool{{
			Kind:        ir.ToolFunction,
			Name:        "ping_host",
			InputSchema: jsontext.Value(`{"type":"object"}`),
		}},
		ToolChoice: &ir.ToolChoice{Mode: ir.ToolChoiceAuto},
	}

	events := []ir.Event{
		{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "resp_stream_echo", Model: "gpt-5", CreatedAt: 1760000000}},
		{Kind: ir.EventPartStart, Index: 0, Part: &ir.Part{Kind: ir.PartText, ID: "msg_1"}},
		{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Text: "ok"}},
		{Kind: ir.EventPartEnd, Index: 0, Part: &ir.Part{Kind: ir.PartText, ID: "msg_1", Text: "ok"}},
		{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishStop}},
	}

	frames, losses := encodeFullEventStream(t, ir.ClientView{
		Model:   "gpt-5",
		Request: clientReq,
		Runtime: newDeterministicRuntime(),
	}, events)
	require.Empty(t, losses)
	require.Empty(t, testkit.ValidateResponsesStream(frames, testkit.GrammarOptions{}))

	checkedLifecycles := 0
	for _, f := range frames {
		if f.Event != "response.created" && f.Event != "response.in_progress" && f.Event != "response.completed" {
			continue
		}
		checkedLifecycles++
		var outer struct {
			Response map[string]jsontext.Value `json:"response"` // nested response envelope map
		}
		require.NoError(t, json.Unmarshal(f.Data, &outer))
		for _, key := range []string{
			"tools",
			"tool_choice",
			"parallel_tool_calls",
			"reasoning",
			"text",
			"store",
			"instructions",
			"max_output_tokens",
			"previous_response_id",
		} {
			require.Contains(t, outer.Response, key, "%s envelope must echo %s", f.Event, key)
		}
	}
	require.Equal(t, 3, checkedLifecycles)
}

// TestStreamEncoderPreservesPunctuatedClientToolNames streams a restored
// custom_tool_call ("code.exec") and namespaced function_call ("search.docs")
// through StreamEncoder and decodes the emitted SSE frames via StreamDecoder.
// Both response.output_item.added and response.output_item.done frames must
// preserve exact client tool names with zero losses and strict grammar validity.
func TestStreamEncoderPreservesPunctuatedClientToolNames(t *testing.T) {
	events := []ir.Event{
		{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "resp_stream_punct", Model: "gpt-5", CreatedAt: 1760000000}},
		{
			Kind:  ir.EventPartStart,
			Index: 0,
			Part:  &ir.Part{Kind: ir.PartToolCall, ID: "ctc_1", Call: &ir.ToolCall{CallID: "call_exec_1", Tool: ir.ToolRef{Kind: ir.ToolCustom, Name: "code.exec"}}},
		},
		{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Arguments: "pwd"}},
		{
			Kind:  ir.EventPartEnd,
			Index: 0,
			Part:  &ir.Part{Kind: ir.PartToolCall, ID: "ctc_1", Call: &ir.ToolCall{CallID: "call_exec_1", Tool: ir.ToolRef{Kind: ir.ToolCustom, Name: "code.exec"}, Arguments: "pwd"}},
		},
		{
			Kind:  ir.EventPartStart,
			Index: 1,
			Part:  &ir.Part{Kind: ir.PartToolCall, ID: "fc_2", Call: &ir.ToolCall{CallID: "call_mcp_2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Namespace: "mcp.srv", Name: "search.docs"}}},
		},
		{Kind: ir.EventPartDelta, Index: 1, Delta: &ir.Delta{Arguments: `{"q":"ir"}`}},
		{
			Kind:  ir.EventPartEnd,
			Index: 1,
			Part:  &ir.Part{Kind: ir.PartToolCall, ID: "fc_2", Call: &ir.ToolCall{CallID: "call_mcp_2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Namespace: "mcp.srv", Name: "search.docs"}, Arguments: `{"q":"ir"}`}},
		},
		{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishToolCalls}},
	}

	frames, losses := encodeFullEventStream(t, ir.ClientView{
		Model:   "gpt-5",
		Runtime: newDeterministicRuntime(),
	}, events)
	require.Empty(t, losses)
	require.Empty(t, testkit.ValidateResponsesStream(frames, testkit.GrammarOptions{}))

	dec := NewStreamDecoder()
	var decodedEvents []ir.Event
	for _, f := range frames {
		require.NotContains(t, string(f.Data), "code_exec_")
		batch, err := dec.Decode(f)
		require.NoError(t, err)
		decodedEvents = append(decodedEvents, batch...)
	}
	decodedEvents = append(decodedEvents, dec.Close(nil)...)
	resp, err := ir.Accumulate(decodedEvents)
	require.NoError(t, err)
	require.Len(t, resp.Parts, 2)
	require.Equal(t, ir.ToolRef{Kind: ir.ToolCustom, Name: "code.exec"}, resp.Parts[0].Call.Tool)
	require.Equal(t, ir.ToolRef{Kind: ir.ToolFunction, Namespace: "mcp.srv", Name: "search.docs"}, resp.Parts[1].Call.Tool)
}
