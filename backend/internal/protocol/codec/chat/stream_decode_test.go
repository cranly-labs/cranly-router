package chat

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

// TestStreamDecoderQuirksPreNameArgsAndReasoningPrecedence verifies explicit
// empty reasoning_content precedence over reasoning, pre-name tool argument
// buffering, and follow-up empty id/name preservation (826f45a2c). Clean EOF
// after finish_reason must succeed without [DONE], whereas silent EOF without
// finish_reason must emit EventError and Incomplete=true (bd39eb589).
func TestStreamDecoderQuirksPreNameArgsAndReasoningPrecedence(t *testing.T) {
	// Chunk 1 has explicit reasoning_content:"" alongside reasoning:"ignored",
	// plus pre-name tool arguments; Chunk 2 supplies function.name and empty id;
	// Chunk 3 supplies finish_reason without trailing [DONE].
	rawSSE := []byte(
		`data: {"id":"c1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"","reasoning":"ignored","tool_calls":[{"index":0,"id":"call_late_name","function":{"arguments":"{\"host\":"}}]},"finish_reason":null}]}` + "\n\n" +
			`data: {"id":"c1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"","function":{"name":"ping_host","arguments":"\"db\"}"}}]},"finish_reason":null}]}` + "\n\n" +
			`data: {"id":"c1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}` + "\n\n",
	)

	events := decodeFullSSEStream(t, rawSSE)
	require.NoError(t, ir.ValidateStream(events))

	resp, err := ir.Accumulate(events)
	require.NoError(t, err)
	require.Len(t, resp.Parts, 1, "explicit empty reasoning_content must block reasoning fallback")
	require.Equal(t, ir.PartToolCall, resp.Parts[0].Kind)
	require.Equal(t, "call_late_name", resp.Parts[0].Call.CallID)
	require.Equal(t, "ping_host", resp.Parts[0].Call.Tool.Name)
	require.Equal(t, `{"host":"db"}`, resp.Parts[0].Call.Arguments)
	require.Equal(t, ir.FinishToolCalls, resp.Finish.Reason)

	// Silent EOF with no finish_reason and no [DONE] must emit EventError and Incomplete=true.
	truncatedSSE := []byte(
		`data: {"id":"c2","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_cut","type":"function","function":{"name":"query","arguments":"{\"q\":"}}]},"finish_reason":null}]}` + "\n\n",
	)
	truncEvents := decodeFullSSEStream(t, truncatedSSE)
	require.NoError(t, ir.ValidateStream(truncEvents))
	require.Equal(t, ir.EventError, truncEvents[len(truncEvents)-1].Kind)

	truncResp, err := ir.Accumulate(truncEvents)
	require.NoError(t, err)
	require.Len(t, truncResp.Parts, 1)
	require.True(t, truncResp.Parts[0].Call.Incomplete)
	require.Equal(t, ir.FinishError, truncResp.Finish.Reason)

	// Transport interruption via Close(cause) also marks open calls Incomplete and emits EventError.
	dec := NewStreamDecoder()
	evBatch, err := dec.Decode(sse.Frame{
		Data: []byte(`{"id":"c3","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`),
	})
	require.NoError(t, err)
	evBatch = append(evBatch, dec.Close(errors.New("connection reset by peer"))...)
	require.NoError(t, ir.ValidateStream(evBatch))
	require.Equal(t, ir.EventError, evBatch[len(evBatch)-1].Kind)
	require.Equal(t, "connection reset by peer", evBatch[len(evBatch)-1].Err.Message)
}

// TestStreamDecoderRejectsInvalidOrDuplicateKeyToolArgumentsOnNormalFinish
// verifies that streams ending with finish_reason and [DONE] after truncated or
// duplicate-key tool_call arguments mark the call Incomplete and emit EventError.
// Re-encoding the resulting abnormal stream must satisfy AllowAbnormal grammar rules.
func TestStreamDecoderRejectsInvalidOrDuplicateKeyToolArgumentsOnNormalFinish(t *testing.T) {
	cases := []struct {
		name   string
		rawSSE []byte
	}{
		{
			name: "truncated_args_with_length_finish_and_done",
			rawSSE: []byte(
				`data: {"id":"c_len","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_trunc","type":"function","function":{"name":"exec","arguments":"{\"cmd\":\"ssh"}}]},"finish_reason":"length"}]}` + "\n\n" +
					"data: [DONE]\n\n",
			),
		},
		{
			name: "duplicate_key_args_with_tool_calls_finish_and_done",
			rawSSE: []byte(
				`data: {"id":"c_dup","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_dup","type":"function","function":{"name":"exec","arguments":"{\"a\":1,\"a\":2}"}}]},"finish_reason":"tool_calls"}]}` + "\n\n" +
					"data: [DONE]\n\n",
			),
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
			require.True(t, folded.Parts[0].Call.Incomplete)
			require.Equal(t, ir.FinishError, folded.Finish.Reason)

			frames, _ := encodeFullEventStream(t, ir.ClientView{
				Model:   "gpt-4o",
				Runtime: newDeterministicRuntime(),
			}, events)
			violations := testkit.ValidateChatStream(frames, testkit.GrammarOptions{AllowAbnormal: true})
			require.Empty(t, violations, "AllowAbnormal grammar violations: %v", violations)
		})
	}
}

// FuzzStreamDecode feeds arbitrary SSE byte streams through sse.NewReader and
// StreamDecoder across wire stream seeds. Whenever every frame decodes without
// a JSON or usage validation error, the resulting event sequence must pass
// ir.ValidateStream without panicking.
func FuzzStreamDecode(f *testing.F) {
	streamFixes, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureStream)
	require.NoError(f, err)
	for _, fix := range streamFixes {
		f.Add(fix.Data)
	}
	f.Add([]byte("data: [DONE]\n\n"))
	f.Add([]byte("data: {\"id\":\"x\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":-1,\"id\":\"c\",\"function\":{\"arguments\":\"{}\"}}]}}]}\n\ndata: [DONE]\n\n"))

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
