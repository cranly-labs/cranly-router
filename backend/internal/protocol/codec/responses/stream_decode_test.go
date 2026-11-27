package responses

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestStreamDecoderCodexBareErrorRecoveryAndEOF reproduces a standalone Codex
// error frame followed either by a recovering response.completed or by EOF.
// Recovered streams must complete normally with EventResponseEnd, whereas
// unrecovered streams must flush the buffered error as terminal EventError.
func TestStreamDecoderCodexBareErrorRecoveryAndEOF(t *testing.T) {
	recoveredSSE := []byte(
		"event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_rec\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5\",\"output\":[]}}\n\n" +
			"event: error\n" +
			"data: {\"type\":\"error\",\"error\":{\"message\":\"transient upstream warning\"}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_rec\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Recovered answer.\",\"annotations\":[]}]}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":3,\"response\":{\"id\":\"resp_rec\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[]}}\n\n",
	)
	recEvents := decodeFullSSEStream(t, recoveredSSE)
	require.NoError(t, ir.ValidateStream(recEvents))
	require.Equal(t, ir.EventResponseEnd, recEvents[len(recEvents)-1].Kind)

	folded, err := ir.Accumulate(recEvents)
	require.NoError(t, err)
	require.Len(t, folded.Parts, 1)
	require.Equal(t, "Recovered answer.", folded.Parts[0].Text)

	unrecoveredSSE := []byte(
		"event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_fail\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5\",\"output\":[]}}\n\n" +
			"event: error\n" +
			"data: {\"type\":\"error\",\"error\":{\"code\":\"rate_limit\",\"message\":\"fatal quota error\"}}\n\n",
	)
	errEvents := decodeFullSSEStream(t, unrecoveredSSE)
	require.NoError(t, ir.ValidateStream(errEvents))
	last := errEvents[len(errEvents)-1]
	require.Equal(t, ir.EventError, last.Kind)
	require.Equal(t, "fatal quota error", last.Err.Message)
}

// TestStreamDecoderInvalidFunctionArgsVsFreeTextCustomCall reproduces normal
// completion streams carrying duplicate-key function_call arguments versus
// free-text custom_tool_call inputs. Malformed function arguments must mark the
// call Incomplete and terminate with EventError, while custom free-text inputs complete cleanly.
func TestStreamDecoderInvalidFunctionArgsVsFreeTextCustomCall(t *testing.T) {
	badFnSSE := []byte(
		"event: response.created\n" +
			"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_bad\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5\",\"output\":[]}}\n\n" +
			"event: response.output_item.added\n" +
			"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"c_1\",\"name\":\"fn\",\"arguments\":\"\"}}\n\n" +
			"event: response.function_call_arguments.delta\n" +
			"data: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"delta\":\"{\\\"dup\\\":1,\\\"dup\\\":2}\"}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"c_1\",\"name\":\"fn\",\"arguments\":\"{\\\"dup\\\":1,\\\"dup\\\":2}\"}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_bad\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[]}}\n\n",
	)
	badEvents := decodeFullSSEStream(t, badFnSSE)
	require.NoError(t, ir.ValidateStream(badEvents))
	require.Equal(t, ir.EventError, badEvents[len(badEvents)-1].Kind)
	require.Equal(t, "invalid_tool_arguments", badEvents[len(badEvents)-1].Err.Code)

	sawIncomplete := false
	for _, ev := range badEvents {
		if ev.Kind == ir.EventPartEnd && ev.Part != nil && ev.Part.Call != nil && ev.Part.Call.Incomplete {
			sawIncomplete = true
		}
	}
	require.True(t, sawIncomplete)

	customOkSSE := []byte(
		"event: response.created\n" +
			"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_cust\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5\",\"output\":[]}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"ctc_1\",\"type\":\"custom_tool_call\",\"call_id\":\"c_patch\",\"name\":\"apply_patch\",\"input\":\"*** Begin Patch\\n+raw text\\n*** End Patch\"}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_cust\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[]}}\n\n",
	)
	okEvents := decodeFullSSEStream(t, customOkSSE)
	require.NoError(t, ir.ValidateStream(okEvents))
	require.Equal(t, ir.EventResponseEnd, okEvents[len(okEvents)-1].Kind)
}

// TestStreamDecoderWhitespaceTypeClosedSlotAndDistinctErrorMessages reproduces
// whitespace-only item.type frames, duplicate content_part.added after
// content_part.done, and non-message slot type mismatches. Whitespace item types
// must fail cleanly, closed content slots must ignore late duplicates, and each
// content handler must report its own event label on slot mismatches.
func TestStreamDecoderWhitespaceTypeClosedSlotAndDistinctErrorMessages(t *testing.T) {
	wsTypeEvents := decodeFullSSEStream(t, []byte(`data: {"type":"response.output_item.done","item":{"type":" "}}`+"\n\n"))
	require.NoError(t, ir.ValidateStream(wsTypeEvents))
	require.Equal(t, ir.EventError, wsTypeEvents[len(wsTypeEvents)-1].Kind)

	dupAddedSSE := []byte(
		"data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"hi\"}}\n\n" +
			"data: {\"type\":\"response.content_part.done\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"hi\"}}\n\n" +
			"data: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"late\"}}\n\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"object\":\"response\",\"created_at\":1,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[]}}\n\n",
	)
	dupEvents := decodeFullSSEStream(t, dupAddedSSE)
	require.NoError(t, ir.ValidateStream(dupEvents))
	folded, err := ir.Accumulate(dupEvents)
	require.NoError(t, err)
	require.Len(t, folded.Parts, 1)
	require.Equal(t, "hi", folded.Parts[0].Text)

	// Verify distinct error labels across content delta, content text done, and content_part.done.
	checks := []struct {
		frame       string // incoming SSE JSON payload targeting non-message slot 0
		errContains string // expected event-specific error substring
	}{
		{
			frame:       `{"type":"response.output_text.delta","output_index":0,"delta":"x"}`,
			errContains: "content delta targets non-message",
		},
		{
			frame:       `{"type":"response.output_text.done","output_index":0,"text":"x"}`,
			errContains: "content text done targets non-message",
		},
		{
			frame:       `{"type":"response.content_part.done","output_index":0,"part":{"type":"output_text","text":"x"}}`,
			errContains: "content_part.done targets non-message",
		},
	}
	for _, tc := range checks {
		dec := NewStreamDecoder()
		_, err := dec.Decode(sse.Frame{Data: []byte(`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning"}}`)})
		require.NoError(t, err)
		_, err = dec.Decode(sse.Frame{Data: []byte(tc.frame)})
		require.Error(t, err)
		require.Contains(t, err.Error(), tc.errContains)
	}
}

// TestStreamDecoderNonObjectToolSearchArguments reproduces completed and
// dangling tool_search_call items carrying scalar JSON string arguments. Normal
// stream completions must mark both calls Incomplete and emit terminal
// EventError events citing the JSON object requirement.
func TestStreamDecoderNonObjectToolSearchArguments(t *testing.T) {
	doneScalarSSE := []byte(
		"event: response.created\n" +
			"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ts1\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5\",\"output\":[]}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"tsc_1\",\"type\":\"tool_search_call\",\"call_id\":\"c_ts1\",\"arguments\":\"\\\"\\\"\"}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ts1\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[]}}\n\n",
	)
	doneEvents := decodeFullSSEStream(t, doneScalarSSE)
	require.NoError(t, ir.ValidateStream(doneEvents))
	lastDone := doneEvents[len(doneEvents)-1]
	require.Equal(t, ir.EventError, lastDone.Kind)
	require.Equal(t, "invalid_tool_arguments", lastDone.Err.Code)
	require.Equal(t, "upstream tool_search_call arguments must be a valid JSON object", lastDone.Err.Message)

	danglingScalarSSE := []byte(
		"event: response.created\n" +
			"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_ts2\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5\",\"output\":[]}}\n\n" +
			"event: response.output_item.added\n" +
			"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"tsc_2\",\"type\":\"tool_search_call\",\"call_id\":\"c_ts2\",\"arguments\":\"\\\"\\\"\"}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_ts2\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[]}}\n\n",
	)
	danglingEvents := decodeFullSSEStream(t, danglingScalarSSE)
	require.NoError(t, ir.ValidateStream(danglingEvents))
	lastDangling := danglingEvents[len(danglingEvents)-1]
	require.Equal(t, ir.EventError, lastDangling.Kind)
	require.Equal(t, "invalid_tool_arguments", lastDangling.Err.Code)
	require.Equal(t, "upstream tool_search_call arguments must be a valid JSON object", lastDangling.Err.Message)

	// Malformed string escapes in RawArguments must fail in handleOutputItemAdded before creating slots or starting Sequencer.
	dec := NewStreamDecoder()
	_, err := dec.handleOutputItemAdded(wireStreamEvent{
		Item: jsontext.Value(`{"id":"tsc_bad","type":"tool_search_call","arguments":"\uZZZZ"}`),
	})
	require.Error(t, err)
	require.False(t, dec.seq.Started())
	require.Empty(t, dec.items)
}

// FuzzStreamDecode feeds arbitrary and fixture-seeded SSE byte streams through
// sse.NewReader and StreamDecoder without panicking. Every resulting event slice
// must satisfy ir.ValidateStream and re-encode into valid Responses SSE frames.
func FuzzStreamDecode(f *testing.F) {
	wireStreams, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureStream)
	require.NoError(f, err)
	for _, fix := range wireStreams {
		f.Add(fix.Data)
	}
	f.Add([]byte(`data:{"type":"response.output_item.done","item":{"type":"function_call"}}`))
	f.Add([]byte("data:{\"type\":\"response.output_item.added\",\"item\":{\"type\":\"0\"}}\n\ndata:{\"type\":\"response.reasoning_text.done\",\"text\":\"0\"}"))
	f.Add([]byte("data:{\"type\":\"response.output_item.done\",\"item\":{\"type\":\" \"}}\n\n"))
	f.Add([]byte("data:{\"type\":\"response.content_part.done\",\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\ndata:{\"type\":\"response.content_part.added\",\"part\":{\"type\":\"output_text\",\"text\":\"x\"}}\n\n"))
	f.Add([]byte("data:{\"type\":\"response.output_item.done\",\"item\":{\"type\":\"tool_search_call\",\"arguments\":\"\\\"\\\"\"}}\n\ndata:{\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"object\":\"response\",\"created_at\":1,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[]}}\n\n"))
	f.Add([]byte("data:{\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"tool_search_call\",\"arguments\":\"\\\"\\\"\"}}\n\ndata:{\"type\":\"response.completed\",\"response\":{\"id\":\"r\",\"object\":\"response\",\"created_at\":1,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[]}}\n\n"))

	f.Fuzz(func(t *testing.T, data []byte) {
		events := decodeFullSSEStream(t, data)
		require.NoError(t, ir.ValidateStream(events))

		frames, _ := encodeFullEventStream(t, ir.ClientView{
			Model:   "gpt-5",
			Runtime: newDeterministicRuntime(),
		}, events)
		violations := testkit.ValidateResponsesStream(frames, testkit.GrammarOptions{AllowAbnormal: true})
		require.Empty(t, violations, "ValidateResponsesStream violations: %v", violations)
	})
}
