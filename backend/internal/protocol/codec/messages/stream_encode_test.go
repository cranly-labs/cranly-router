package messages

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestStreamEncoderSemanticTerminalFlagsContiguousIndicesAndAbort verifies
// Semantic and Terminal SSE flags, deferred message_start usage embedding,
// contiguous zero-based block numbering across dropped parts, and mid-block error closure.
// Truncated streams ending with EventError must close any active block before
// emitting event: error so ValidateMessagesStream AllowAbnormal checks pass.
func TestStreamEncoderSemanticTerminalFlagsContiguousIndicesAndAbort(t *testing.T) {
	fiveMin := int64(30)
	oneHour := int64(20)
	events := []ir.Event{
		{
			Kind: ir.EventResponseStart,
			Response: &ir.ResponseMeta{
				ID:    "msg_stream_flags",
				Model: "gemini-2.5-pro",
			},
		},
		{
			Kind: ir.EventUsage,
			Usage: &ir.Usage{
				Input:        40,
				CacheRead:    10,
				CacheWrite:   50,
				CacheWrite5m: &fiveMin,
				CacheWrite1h: &oneHour,
				Output:       1,
				Speed:        "fast",
			},
		},
		// Empty PartText opens no wire block and consumes no index.
		{
			Kind:  ir.EventPartStart,
			Index: 0,
			Part:  &ir.Part{Kind: ir.PartText},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 0,
			Part:  &ir.Part{Kind: ir.PartText},
		},
		// One-shot PartReasoning opens wire index 0 and flushes signature_delta before stop.
		{
			Kind:  ir.EventPartStart,
			Index: 1,
			Part: &ir.Part{
				Kind:      ir.PartReasoning,
				Reasoning: &ir.Reasoning{},
			},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 1,
			Part: &ir.Part{
				Kind: ir.PartReasoning,
				Reasoning: &ir.Reasoning{
					Segments: []string{"Verify policy."},
					Token:    &ir.ReplayToken{Data: "EqQPolicySig"},
				},
			},
		},
		// Hosted PartToolCall is skipped without advancing wire block index.
		{
			Kind:  ir.EventPartStart,
			Index: 2,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID: "ws_1",
					Tool:   ir.ToolRef{Kind: ir.ToolHosted, Name: "web_search"},
				},
			},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 2,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID: "ws_1",
					Tool:   ir.ToolRef{Kind: ir.ToolHosted, Name: "web_search"},
				},
			},
		},
		// Custom PartToolCall opens wire index 1 and wraps free-form input as {"input":...}.
		{
			Kind:  ir.EventPartStart,
			Index: 3,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID: "",
					Tool:   ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"},
				},
			},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 3,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID:    "",
					Tool:      ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"},
					Arguments: "*** Begin Patch",
				},
			},
		},
		// PartRefusal opens wire index 2 lazily on first delta.
		{
			Kind:  ir.EventPartStart,
			Index: 4,
			Part:  &ir.Part{Kind: ir.PartRefusal},
		},
		{
			Kind:  ir.EventPartDelta,
			Index: 4,
			Delta: &ir.Delta{Text: "Cannot apply destructive patch."},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 4,
			Part:  &ir.Part{Kind: ir.PartRefusal, Text: "Cannot apply destructive patch."},
		},
		{
			Kind: ir.EventUsage,
			Usage: &ir.Usage{
				Input:        40,
				CacheRead:    10,
				CacheWrite:   50,
				CacheWrite5m: &fiveMin,
				CacheWrite1h: &oneHour,
				Output:       24,
				Speed:        "fast",
			},
		},
		{
			Kind:   ir.EventResponseEnd,
			Finish: &ir.Finish{Reason: ir.FinishContentFilter},
		},
	}

	view := ir.ClientView{
		Model:   "claude-sonnet-4-5",
		Runtime: newDeterministicRuntime(),
	}
	enc := NewStreamEncoder(view)
	var frames []sse.Frame
	var losses []ir.Loss
	for _, ev := range events {
		fBatch, lBatch, err := enc.Encode(ev)
		require.NoError(t, err)
		frames = append(frames, fBatch...)
		losses = append(losses, lBatch...)
	}

	// Trailing events after message_stop must be ignored cleanly.
	extraFrames, extraLosses, err := enc.Encode(ir.Event{
		Kind:  ir.EventPartDelta,
		Index: 4,
		Delta: &ir.Delta{Text: "late"},
	})
	require.NoError(t, err)
	require.Empty(t, extraFrames)
	require.Empty(t, extraLosses)

	require.Empty(t, testkit.ValidateMessagesStream(frames, testkit.GrammarOptions{}))
	require.Equal(t, "message_start", frames[0].Event)
	require.False(t, frames[0].Semantic)
	require.False(t, frames[0].Terminal)
	require.Contains(t, string(frames[0].Data), `"speed":"fast"`)
	require.Contains(t, string(frames[0].Data), `"ephemeral_5m_input_tokens":30`)

	// Check contiguous wire block indices 0 (thinking), 1 (tool_use), 2 (text refusal).
	rawSSE := string(formatSSEFrames(frames))
	require.Contains(t, rawSSE, `"index":0,"content_block":{"type":"thinking"`)
	require.Contains(t, rawSSE, `"index":1,"content_block":{"type":"tool_use"`)
	require.Contains(t, rawSSE, `"index":2,"content_block":{"type":"text"`)
	require.NotContains(t, rawSSE, `"index":3`)
	require.Contains(t, rawSSE, `"stop_reason":"refusal"`)
	require.Equal(t, "message_stop", frames[len(frames)-1].Event)
	require.False(t, frames[len(frames)-1].Semantic)
	require.True(t, frames[len(frames)-1].Terminal)
	require.Equal(t, []ir.Loss{
		{Kind: ir.LossClientMarkerDropped, Path: "parts[0]", Detail: "empty_text"},
		{Kind: ir.LossOpaqueItemDropped, Path: "parts[2]"},
		{Kind: ir.LossIDRewritten, Path: "parts[3].call_id"},
		{Kind: ir.LossToolLowered, Path: "parts[3]"},
		{Kind: ir.LossRefusalInlined, Path: "parts[4]"},
	}, losses)

	// Mid-block EventError closes open text block before emitting terminal event: error.
	errFrames, _ := encodeFullEventStream(t, view, []ir.Event{
		{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "msg_abort", Model: "claude-sonnet-4-5"}},
		{Kind: ir.EventPartStart, Index: 0, Part: &ir.Part{Kind: ir.PartText}},
		{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Text: "Streaming partial..."}},
		{Kind: ir.EventError, Err: &ir.StreamError{Type: "overloaded_error", Code: "overloaded", Message: "upstream overloaded"}},
	})
	require.Empty(t, testkit.ValidateMessagesStream(errFrames, testkit.GrammarOptions{AllowAbnormal: true}))
	require.Equal(t, "content_block_stop", errFrames[len(errFrames)-2].Event)
	require.Equal(t, "error", errFrames[len(errFrames)-1].Event)
	require.False(t, errFrames[len(errFrames)-1].Semantic)
	require.True(t, errFrames[len(errFrames)-1].Terminal)
}

// TestStreamEncoderToolArgsPrefixMismatchInvalidUTF8AndNonObjectFallback checks
// StreamEncoder when EventPartEnd tool arguments differ in byte length from
// streamed deltas (due to invalid UTF-8 replacement or whitespace) or contain
// non-object JSON values such as "[]" and "null". Streamed and buffered outputs
// must both satisfy ValidateMessagesStream and produce identical decoded responses.
func TestStreamEncoderToolArgsPrefixMismatchInvalidUTF8AndNonObjectFallback(t *testing.T) {
	rawInvalidUTF8 := string(append(append([]byte(`{"a":"`), 0xff), []byte(`"}`)...))
	compactedUTF8 := string(compactRawJSON([]byte(rawInvalidUTF8)))
	require.Greater(t, len(compactedUTF8), len(rawInvalidUTF8))

	events := []ir.Event{
		{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "msg_tool_edge", Model: "claude-sonnet-4-5"}},
		// 1. Delta carries 1-byte 0xff while EventPartEnd carries 3-byte U+FFFD replacement.
		{
			Kind:  ir.EventPartStart,
			Index: 0,
			Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_utf8", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_utf8"}}},
		},
		{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Arguments: rawInvalidUTF8}},
		{
			Kind:  ir.EventPartEnd,
			Index: 0,
			Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_utf8", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_utf8"}, Arguments: compactedUTF8}},
		},
		// 2. Delta streams compact JSON {"k":1} while EventPartEnd carries spaced {"k": 1}.
		{
			Kind:  ir.EventPartStart,
			Index: 1,
			Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_ws", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_ws"}}},
		},
		{Kind: ir.EventPartDelta, Index: 1, Delta: &ir.Delta{Arguments: `{"k":1}`}},
		{
			Kind:  ir.EventPartEnd,
			Index: 1,
			Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_ws", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_ws"}, Arguments: `{"k": 1}`}},
		},
		// 3. Cross-protocol Chat non-object JSON arguments "[]" and "null" withhold deltas so input:{} stands.
		{
			Kind:  ir.EventPartStart,
			Index: 2,
			Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_arr", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_arr"}}},
		},
		{Kind: ir.EventPartDelta, Index: 2, Delta: &ir.Delta{Arguments: `[`}},
		{Kind: ir.EventPartDelta, Index: 2, Delta: &ir.Delta{Arguments: `]`}},
		{
			Kind:  ir.EventPartEnd,
			Index: 2,
			Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_arr", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_arr"}, Arguments: `[]`}},
		},
		{
			Kind:  ir.EventPartStart,
			Index: 3,
			Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_null", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_null"}}},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 3,
			Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_null", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_null"}, Arguments: `null`}},
		},
		{Kind: ir.EventUsage, Usage: &ir.Usage{Input: 10, Output: 12}},
		{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishToolCalls}},
	}

	require.NoError(t, ir.ValidateStream(events))
	accumResp, err := ir.Accumulate(events)
	require.NoError(t, err)

	view := ir.ClientView{Model: "claude-sonnet-4-5", Runtime: newDeterministicRuntime()}
	frames, streamLosses := encodeFullEventStream(t, view, events)
	require.Empty(t, testkit.ValidateMessagesStream(frames, testkit.GrammarOptions{}))

	bufBytes, bufLosses, err := EncodeResponse(accumResp, ir.ClientView{Model: "claude-sonnet-4-5", Runtime: newDeterministicRuntime()})
	require.NoError(t, err)
	require.Equal(t, bufLosses, streamLosses)

	streamDecoded, err := ir.Accumulate(decodeFullSSEStream(t, formatSSEFrames(frames)))
	require.NoError(t, err)
	bufDecoded, err := DecodeResponse(bufBytes)
	require.NoError(t, err)
	require.Equal(t, bufDecoded, streamDecoded)
	require.Equal(t, `{}`, streamDecoded.Parts[2].Call.Arguments)
	require.Equal(t, `{}`, streamDecoded.Parts[3].Call.Arguments)
}
