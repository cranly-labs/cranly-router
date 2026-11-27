package messages

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestStreamDecoderHeaderlessEventsServerToolUseAndTruncation verifies SSE
// frames missing event headers (22235a63c), server_tool_use input_json_delta
// patching, citations_delta accumulation, and clean versus truncated EOF handling.
// Silent EOF mid-block without message_stop must mark open tool calls Incomplete
// and emit a terminal EventError (bd39eb589).
func TestStreamDecoderHeaderlessEventsServerToolUseAndTruncation(t *testing.T) {
	// Headerless SSE frames carrying message_start, ping, thinking + signature,
	// server_tool_use with input_json_delta, text + citations_delta, and
	// message_delta with stop_reason followed by clean EOF without message_stop.
	rawSSE := []byte(
		`data: {"type":"message_start","message":{"id":"msg_hdrless","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":25,"output_tokens":1}}}` + "\n\n" +
			`data: {"type":"ping"}` + "\n\n" +
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}` + "\n\n" +
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Check web."}}` + "\n\n" +
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"EqQSigStream"}}` + "\n\n" +
			`data: {"type":"content_block_stop","index":0}` + "\n\n" +
			`data: {"type":"content_block_start","index":1,"content_block":{"type":"server_tool_use","id":"srvtoolu_1","name":"web_search","input":{}}}` + "\n\n" +
			`data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"query\":\"golang\"}"}}` + "\n\n" +
			`data: {"type":"content_block_stop","index":1}` + "\n\n" +
			`data: {"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}` + "\n\n" +
			`data: {"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"Go 1.27 released."}}` + "\n\n" +
			`data: {"type":"content_block_delta","index":2,"delta":{"type":"citations_delta","citation":{"type":"web_search_result_location","url":"https://go.dev/doc/go1.27","title":"Go 1.27"}}}` + "\n\n" +
			`data: {"type":"content_block_stop","index":2}` + "\n\n" +
			`data: {"type":"message_delta","delta":{"stop_reason":"stop_sequence","stop_sequence":"</ans>"},"usage":{"output_tokens":18}}` + "\n\n",
	)

	events := decodeFullSSEStream(t, rawSSE)
	require.NoError(t, ir.ValidateStream(events))

	resp, err := ir.Accumulate(events)
	require.NoError(t, err)
	require.Equal(t, "msg_hdrless", resp.ID)
	require.Len(t, resp.Parts, 3)
	require.Equal(t, ir.PartReasoning, resp.Parts[0].Kind)
	require.Equal(t, []string{"Check web."}, resp.Parts[0].Reasoning.Segments)
	require.Equal(t, "EqQSigStream", resp.Parts[0].Reasoning.Token.Data)
	require.Equal(t, ir.PartOpaque, resp.Parts[1].Kind)
	require.JSONEq(t, `{"type":"server_tool_use","id":"srvtoolu_1","name":"web_search","input":{"query":"golang"}}`, string(resp.Parts[1].Opaque.Raw))
	require.Equal(t, ir.PartText, resp.Parts[2].Kind)
	require.Equal(t, "Go 1.27 released.", resp.Parts[2].Text)
	require.Len(t, resp.Parts[2].Annotations, 1)
	require.Equal(t, "https://go.dev/doc/go1.27", resp.Parts[2].Annotations[0].URL)
	require.Equal(t, ir.FinishStopSequence, resp.Finish.Reason)
	require.Equal(t, "</ans>", resp.Finish.StopSequence)
	require.Equal(t, int64(25), resp.Usage.Input)
	require.Equal(t, int64(18), resp.Usage.Output)

	// Silent EOF with an open tool_use block and no stop_reason emits EventError + Incomplete=true.
	truncatedSSE := []byte(
		"event: message_start\n" +
			`data: {"type":"message_start","message":{"id":"msg_cut","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":10,"output_tokens":1}}}` + "\n\n" +
			"event: content_block_start\n" +
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_cut","name":"query","input":{}}}` + "\n\n" +
			"event: content_block_delta\n" +
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"q\":"}}` + "\n\n",
	)
	truncEvents := decodeFullSSEStream(t, truncatedSSE)
	require.NoError(t, ir.ValidateStream(truncEvents))
	require.Equal(t, ir.EventError, truncEvents[len(truncEvents)-1].Kind)
	require.Equal(t, "stream_truncated", truncEvents[len(truncEvents)-1].Err.Code)

	truncResp, err := ir.Accumulate(truncEvents)
	require.NoError(t, err)
	require.Len(t, truncResp.Parts, 1)
	require.True(t, truncResp.Parts[0].Call.Incomplete)
	require.Equal(t, ir.FinishError, truncResp.Finish.Reason)

	// Transport interruption via Close(cause) closes open blocks and emits stream_interrupted.
	dec := NewStreamDecoder()
	evBatch, err := dec.Decode(sse.Frame{
		Event: "message_start",
		Data:  []byte(`{"type":"message_start","message":{"id":"msg_net","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[]}}`),
	})
	require.NoError(t, err)
	stepBatch, err := dec.Decode(sse.Frame{
		Event: "content_block_start",
		Data:  []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"partial"}}`),
	})
	require.NoError(t, err)
	evBatch = append(evBatch, stepBatch...)
	evBatch = append(evBatch, dec.Close(errors.New("connection reset by peer"))...)
	require.NoError(t, ir.ValidateStream(evBatch))
	require.Equal(t, ir.EventError, evBatch[len(evBatch)-1].Kind)
	require.Equal(t, "stream_interrupted", evBatch[len(evBatch)-1].Err.Code)
	require.Equal(t, "connection reset by peer", evBatch[len(evBatch)-1].Err.Message)
}

// TestStreamDecoderRejectsInvalidOrDuplicateKeyToolArgumentsOnNormalFinish
// verifies that streams ending with message_stop after truncated or duplicate-key
// input_json_delta fragments mark tool_use calls Incomplete and emit EventError.
// Re-encoding the resulting abnormal stream must satisfy AllowAbnormal grammar rules.
func TestStreamDecoderRejectsInvalidOrDuplicateKeyToolArgumentsOnNormalFinish(t *testing.T) {
	cases := []struct {
		name           string
		rawSSE         []byte
		expectToolCall bool
	}{
		{
			name: "truncated_tool_use_args_with_max_tokens_and_message_stop",
			rawSSE: []byte(
				`data: {"type":"message_start","message":{"id":"m_len","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"usage":{"input_tokens":5,"output_tokens":1}}}` + "\n\n" +
					`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_trunc","name":"exec","input":{}}}` + "\n\n" +
					`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"cmd\":\"ssh"}}` + "\n\n" +
					`data: {"type":"content_block_stop","index":0}` + "\n\n" +
					`data: {"type":"message_delta","delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":10}}` + "\n\n" +
					`data: {"type":"message_stop"}` + "\n\n",
			),
			expectToolCall: true,
		},
		{
			name: "duplicate_key_tool_use_args_with_message_stop",
			rawSSE: []byte(
				`data: {"type":"message_start","message":{"id":"m_dup","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"usage":{"input_tokens":5,"output_tokens":1}}}` + "\n\n" +
					`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_dup","name":"exec","input":{}}}` + "\n\n" +
					`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"a\":1,\"a\":2}"}}` + "\n\n" +
					`data: {"type":"content_block_stop","index":0}` + "\n\n" +
					`data: {"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":10}}` + "\n\n" +
					`data: {"type":"message_stop"}` + "\n\n",
			),
			expectToolCall: true,
		},
		{
			name: "truncated_server_tool_use_args_with_message_stop",
			rawSSE: []byte(
				`data: {"type":"message_start","message":{"id":"m_srv","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"usage":{"input_tokens":5,"output_tokens":1}}}` + "\n\n" +
					`data: {"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"srvtoolu_bad","name":"web_search","input":{}}}` + "\n\n" +
					`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"query\":"}}` + "\n\n" +
					`data: {"type":"content_block_stop","index":0}` + "\n\n" +
					`data: {"type":"message_delta","delta":{"stop_reason":"pause_turn"},"usage":{"output_tokens":4}}` + "\n\n" +
					`data: {"type":"message_stop"}` + "\n\n",
			),
			expectToolCall: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			events := decodeFullSSEStream(t, tc.rawSSE)
			require.NoError(t, ir.ValidateStream(events))
			require.Equal(t, ir.EventError, events[len(events)-1].Kind)
			require.Equal(t, "invalid_tool_arguments", events[len(events)-1].Err.Code)

			folded, err := ir.Accumulate(events)
			require.NoError(t, err)
			require.Len(t, folded.Parts, 1)
			if tc.expectToolCall {
				require.True(t, folded.Parts[0].Call.Incomplete)
			}
			require.Equal(t, ir.FinishError, folded.Finish.Reason)

			frames, _ := encodeFullEventStream(t, ir.ClientView{
				Model:   "claude-sonnet-4-5",
				Runtime: newDeterministicRuntime(),
			}, events)
			violations := testkit.ValidateMessagesStream(frames, testkit.GrammarOptions{AllowAbnormal: true})
			require.Empty(t, violations, "AllowAbnormal grammar violations: %v", violations)
		})
	}
}

// TestStreamDecoderEmptyTextBlockMatchesBufferedResponseLawS2 compares buffered
// and streaming Messages replies that carry an empty unannotated text block
// alongside a populated text block. StreamDecoder must omit unpopulated empty
// text blocks so ir.Accumulate matches DecodeResponse under Law S2 while still
// opening deferred text blocks lazily when citations_delta arrives.
func TestStreamDecoderEmptyTextBlockMatchesBufferedResponseLawS2(t *testing.T) {
	bufferedJSON := []byte(`{"id":"msg_s2_emp","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":""},{"type":"text","text":"Visible reply."}],"stop_reason":"end_turn","stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":4}}`)
	streamSSE := []byte(
		`data: {"type":"message_start","message":{"id":"msg_s2_emp","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":1}}}` + "\n\n" +
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
			`data: {"type":"content_block_stop","index":0}` + "\n\n" +
			`data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}` + "\n\n" +
			`data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Visible reply."}}` + "\n\n" +
			`data: {"type":"content_block_stop","index":1}` + "\n\n" +
			`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":4}}` + "\n\n" +
			`data: {"type":"message_stop"}` + "\n\n",
	)

	bufferedResp, err := DecodeResponse(bufferedJSON)
	require.NoError(t, err)

	events := decodeFullSSEStream(t, streamSSE)
	require.NoError(t, ir.ValidateStream(events))
	foldedResp, err := ir.Accumulate(events)
	require.NoError(t, err)

	require.Equal(t, bufferedResp, foldedResp)
	require.Len(t, foldedResp.Parts, 1)
	require.Equal(t, "Visible reply.", foldedResp.Parts[0].Text)

	// Deferred empty text block receiving citations_delta opens lazily and preserves the citation.
	citationOnlySSE := []byte(
		`data: {"type":"message_start","message":{"id":"msg_cit_only","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[]}}` + "\n\n" +
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
			`data: {"type":"content_block_delta","index":0,"delta":{"type":"citations_delta","citation":{"type":"web_search_result_location","url":"https://go.dev"}}}` + "\n\n" +
			`data: {"type":"content_block_stop","index":0}` + "\n\n" +
			`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":5,"output_tokens":1}}` + "\n\n" +
			`data: {"type":"message_stop"}` + "\n\n",
	)
	citEvents := decodeFullSSEStream(t, citationOnlySSE)
	require.NoError(t, ir.ValidateStream(citEvents))
	citFolded, err := ir.Accumulate(citEvents)
	require.NoError(t, err)
	require.Len(t, citFolded.Parts, 1)
	require.Len(t, citFolded.Parts[0].Annotations, 1)
}

// FuzzStreamDecode feeds arbitrary SSE byte streams through sse.NewReader and
// StreamDecoder across Messages wire stream seeds. Whenever every frame decodes
// without a JSON, block-state, or usage validation error, the resulting event
// sequence must pass ir.ValidateStream without panicking.
func FuzzStreamDecode(f *testing.F) {
	streamFixes, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureStream)
	require.NoError(f, err)
	for _, fix := range streamFixes {
		f.Add(fix.Data)
	}
	f.Add([]byte("data: [DONE]\n\n"))
	f.Add([]byte("data: {\"type\":\"content_block_start\",\"index\":-1,\"content_block\":{\"type\":\"text\",\"text\":\"x\"}}\n\n"))
	f.Add([]byte("data: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[]}}\n\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t1\",\"name\":\"fn\",\"input\":{}}}\n\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"a\\\":1,\\\"a\\\":2}\"}}\n\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\ndata: {\"type\":\"message_stop\"}\n\n"))

	f.Fuzz(func(t *testing.T, raw []byte) {
		rd := sse.NewReader(bytes.NewReader(raw))
		dec := NewStreamDecoder()
		var events []ir.Event
		decodeFailed := false

		for rd.Scan() {
			batch, err := dec.Decode(rd.Frame())
			if err != nil {
				decodeFailed = true
				break
			}
			events = append(events, batch...)
		}
		if decodeFailed {
			_ = dec.Close(errors.New("decode aborted"))
			return
		}
		events = append(events, dec.Close(rd.Err())...)
		if err := ir.ValidateStream(events); err != nil {
			t.Fatalf("StreamDecoder produced invalid canonical event stream: %v", err)
		}
	})
}
