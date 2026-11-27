package ir

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// TestSequencerSerializesInterleavedParallelToolCalls feeds leading text
// chunks followed by interleaved parallel tool-call deltas across slots 10 and
// 11 into Sequencer. Active text must emit immediately without buffering while
// interleaved tool calls drain in sequential canonical order with complete
// argument snapshots that pass ValidateStream and Accumulate.
func TestSequencerSerializesInterleavedParallelToolCalls(t *testing.T) {
	seq := NewSequencer()
	var canonical []Event

	// Leading text streams out immediately chunk by chunk.
	step1 := seq.Push(
		Event{Kind: EventResponseStart, Response: &ResponseMeta{ID: "chatcmpl_1", Model: "gpt-4o"}},
		Event{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartText}},
		Event{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "Checking "}},
	)
	require.Len(t, step1, 3)
	require.True(t, seq.Started())
	require.True(t, seq.HasOpenParts())
	require.False(t, seq.Terminated())
	canonical = append(canonical, step1...)

	step2 := seq.Push(
		Event{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "both:"}},
		Event{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartText}},
	)
	require.Len(t, step2, 2)
	require.Equal(t, "Checking both:", step2[1].Part.Text)
	canonical = append(canonical, step2...)

	// Interleave parallel tool calls at upstream slots 10 and 11 (and start 12 out of order before 11 finishes).
	step3 := seq.Push(
		Event{
			Kind:  EventPartStart,
			Index: 10,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "call_1", Tool: ToolRef{Kind: ToolFunction, Name: "get_weather"}},
			},
		},
		Event{Kind: EventPartDelta, Index: 10, Delta: &Delta{Arguments: `{"ci`}},
		Event{
			Kind:  EventPartStart,
			Index: 12,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "call_3", Tool: ToolRef{Kind: ToolFunction, Name: "get_alerts"}},
			},
		},
		Event{
			Kind:  EventPartStart,
			Index: 11,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "call_2", Tool: ToolRef{Kind: ToolFunction, Name: "get_time"}},
			},
		},
		Event{Kind: EventPartDelta, Index: 11, Delta: &Delta{Arguments: `{"tz":`}},
		Event{Kind: EventPartDelta, Index: 12, Delta: &Delta{Arguments: `{}`}},
		Event{Kind: EventPartDelta, Index: 10, Delta: &Delta{Arguments: `ty":"Tokyo"}`}},
		Event{Kind: EventPartDelta, Index: 11, Delta: &Delta{Arguments: `"Asia/Tokyo"}`}},
	)
	// While slot 10 is active (canonical index 1), only slot 10's start and two deltas emit.
	require.Len(t, step3, 3)
	require.Equal(t, 1, step3[0].Index)
	canonical = append(canonical, step3...)

	// FlushOpenParts closes slot 10 and drains buffered slots 11 and 12 in ascending key order (canonical 2 and 3).
	flushed := seq.FlushOpenParts()
	require.False(t, seq.HasOpenParts())
	canonical = append(canonical, flushed...)

	// Trailing usage and terminal response_end follow after all parts closed.
	tail := seq.Push(
		Event{Kind: EventUsage, Usage: &Usage{Input: 52, Output: 28}},
		Event{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishToolCalls}},
		// Post-terminal events must be ignored.
		Event{Kind: EventUsage, Usage: &Usage{Input: 999}},
	)
	require.True(t, seq.Terminated())
	canonical = append(canonical, tail...)

	require.NoError(t, ValidateStream(canonical))
	resp, err := Accumulate(canonical)
	require.NoError(t, err)
	require.Len(t, resp.Parts, 4)
	require.Equal(t, "Checking both:", resp.Parts[0].Text)
	require.Equal(t, "call_1", resp.Parts[1].Call.CallID)
	require.Equal(t, `{"city":"Tokyo"}`, resp.Parts[1].Call.Arguments)
	require.False(t, resp.Parts[1].Call.Incomplete)
	require.Equal(t, "call_2", resp.Parts[2].Call.CallID)
	require.Equal(t, `{"tz":"Asia/Tokyo"}`, resp.Parts[2].Call.Arguments)
	require.Equal(t, "call_3", resp.Parts[3].Call.CallID)
	require.Equal(t, `{}`, resp.Parts[3].Call.Arguments)
}

// TestSequencerAbnormalErrorMarksUnfinishedToolCallsIncomplete aborts a stream
// while slot 0 and slot 2 remain unfinished and buffered slot 1 already closed.
// Unfinished tool calls must set Incomplete=true while finished slot 1 retains
// Incomplete=false, and the resulting stream must satisfy ValidateStream.
func TestSequencerAbnormalErrorMarksUnfinishedToolCallsIncomplete(t *testing.T) {
	var seq Sequencer
	events := seq.Push(
		Event{Kind: EventResponseStart, Response: &ResponseMeta{ID: "err_interleaved"}},
		Event{
			Kind:  EventPartStart,
			Index: 0,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "c_open_0", Tool: ToolRef{Kind: ToolFunction, Name: "fn0"}},
			},
		},
		Event{Kind: EventPartDelta, Index: 0, Delta: &Delta{Arguments: `{"partial":`}},
		Event{
			Kind:  EventPartStart,
			Index: 1,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "c_done_1", Tool: ToolRef{Kind: ToolFunction, Name: "fn1"}},
			},
		},
		Event{Kind: EventPartDelta, Index: 1, Delta: &Delta{Arguments: `{"ok":true}`}},
		Event{Kind: EventPartEnd, Index: 1},
		Event{
			Kind:  EventPartStart,
			Index: 2,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "c_open_2", Tool: ToolRef{Kind: ToolFunction, Name: "fn2"}},
			},
		},
		Event{Kind: EventError, Err: &StreamError{Message: "connection reset"}},
	)

	require.NoError(t, ValidateStream(events))
	resp, err := Accumulate(events)
	require.NoError(t, err)
	require.Len(t, resp.Parts, 3)
	require.True(t, resp.Parts[0].Call.Incomplete)
	require.Equal(t, `{"partial":`, resp.Parts[0].Call.Arguments)
	require.False(t, resp.Parts[1].Call.Incomplete)
	require.Equal(t, `{"ok":true}`, resp.Parts[1].Call.Arguments)
	require.True(t, resp.Parts[2].Call.Incomplete)
}

// TestSequencerSynthesizesStartAndOneShotParts exercises frame-zero errors
// without an explicit response_start, one-shot EventPartEnd items without prior
// part_start, and upstream slot key reuse after closure. Synthesized streams
// must pass ValidateStream and preserve reasoning tokens and opaque items.
func TestSequencerSynthesizesStartAndOneShotParts(t *testing.T) {
	// Frame-zero error synthesizes leading response_start automatically.
	var errSeq Sequencer
	frameZeroErr := errSeq.Push(Event{Kind: EventError, Err: &StreamError{Message: "upstream 502"}})
	require.NoError(t, ValidateStream(frameZeroErr))
	require.Equal(t, EventResponseStart, frameZeroErr[0].Kind)
	require.Equal(t, EventError, frameZeroErr[1].Kind)

	// One-shot complete parts and reused slot index 0 produce sequential canonical indices.
	var seq Sequencer
	got := seq.Push(
		Event{
			Kind:  EventPartStart,
			Index: 0,
			Part:  &Part{Kind: PartReasoning, Reasoning: &Reasoning{Summary: true}},
		},
		Event{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "thought text"}},
		Event{
			Kind:  EventPartEnd,
			Index: 0,
			Part: &Part{
				Kind:      PartReasoning,
				Reasoning: &Reasoning{Token: &ReplayToken{Scheme: SchemeGemini, Data: "gem_sig"}},
			},
		},
		// Reuse slot 0 for a one-shot opaque item without prior part_start.
		Event{
			Kind:  EventPartEnd,
			Index: 0,
			Part: &Part{
				Kind: PartOpaque,
				Opaque: &OpaqueItem{
					Origin: protocol.ProtocolOpenAIResponses,
					Type:   "web_search_call",
					Raw:    []byte(`{"type":"web_search_call"}`),
				},
			},
		},
		Event{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
	)

	require.NoError(t, ValidateStream(got))
	resp, err := Accumulate(got)
	require.NoError(t, err)
	require.Len(t, resp.Parts, 2)
	require.Equal(t, []string{"thought text"}, resp.Parts[0].Reasoning.Segments)
	require.Equal(t, "gem_sig", resp.Parts[0].Reasoning.Token.Data)
	require.Equal(t, PartOpaque, resp.Parts[1].Kind)
}

// TestSequencerPreservesBufferedEndMetadataAcrossLateDeltas sends early
// EventPartEnd snapshots carrying IDs, replay tokens, Extra members, and
// initial payloads to buffered slots 1, 2, and 3, followed by late interleaved
// EventPartDelta chunks before active slot 0 closes. All endPart metadata,
// tokens, initial content, and late fragments must survive and pass ValidateStream.
func TestSequencerPreservesBufferedEndMetadataAcrossLateDeltas(t *testing.T) {
	extra := Extra{
		Origin:  protocol.ProtocolAnthropicMessages,
		Members: map[string]jsontext.Value{"custom_tag": jsontext.Value(`"kept"`)},
	}

	var seq Sequencer
	events := seq.Push(
		Event{Kind: EventResponseStart, Response: &ResponseMeta{ID: "late_meta"}},
		Event{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartText}},

		// Slot 1: one-shot text EventPartEnd (no prior part_start) with ID, Cache, Extra, and initial Text, then late delta.
		Event{
			Kind:  EventPartEnd,
			Index: 1,
			Part: &Part{
				Kind:  PartText,
				ID:    "txt_1",
				Text:  "hello",
				Cache: &CacheMark{TTL: "5m"},
				Extra: extra,
			},
		},
		Event{Kind: EventPartDelta, Index: 1, Delta: &Delta{Text: " world"}},

		// Slot 2: buffered reasoning part with signature token on EventPartEnd, then late thinking delta.
		Event{Kind: EventPartStart, Index: 2, Part: &Part{Kind: PartReasoning, Reasoning: &Reasoning{}}},
		Event{Kind: EventPartDelta, Index: 2, Delta: &Delta{Text: "step 1"}},
		Event{
			Kind:  EventPartEnd,
			Index: 2,
			Part: &Part{
				Kind: PartReasoning,
				ID:   "rs_2",
				Reasoning: &Reasoning{
					Segments: []string{"step 1"},
					Summary:  true,
					Token:    &ReplayToken{Scheme: SchemeAnthropic, Data: "sig_kept"},
				},
				Extra: extra,
			},
		},
		Event{Kind: EventPartDelta, Index: 2, Delta: &Delta{Text: " + step 2"}},

		// Slot 3: buffered tool_call with CallID, Token, Unsigned, Extra, and non-empty Arguments on EventPartEnd, then late delta.
		Event{
			Kind:  EventPartStart,
			Index: 3,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{Tool: ToolRef{Kind: ToolFunction, Name: "lookup"}},
			},
		},
		Event{Kind: EventPartDelta, Index: 3, Delta: &Delta{Arguments: `{"a":`}},
		Event{
			Kind:  EventPartEnd,
			Index: 3,
			Part: &Part{
				Kind: PartToolCall,
				ID:   "tc_3",
				Call: &ToolCall{
					CallID:    "call_kept_3",
					Tool:      ToolRef{Kind: ToolFunction, Name: "lookup"},
					Arguments: `{"a":`,
					Token:     &ReplayToken{Scheme: SchemeGemini, Data: "gem_call_sig"},
					Unsigned:  true,
				},
				Extra: extra,
			},
		},
		Event{Kind: EventPartDelta, Index: 3, Delta: &Delta{Arguments: `1}`}},

		// Close active slot 0 to release slots 1, 2, and 3 in canonical order.
		Event{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "lead"}},
		Event{Kind: EventPartEnd, Index: 0},
		Event{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishToolCalls}},
	)

	require.NoError(t, ValidateStream(events))
	resp, err := Accumulate(events)
	require.NoError(t, err)
	require.Len(t, resp.Parts, 4)

	require.Equal(t, "lead", resp.Parts[0].Text)

	require.Equal(t, "txt_1", resp.Parts[1].ID)
	require.Equal(t, "hello world", resp.Parts[1].Text)
	require.NotNil(t, resp.Parts[1].Cache)
	require.Equal(t, "5m", resp.Parts[1].Cache.TTL)
	require.Equal(t, `"kept"`, string(resp.Parts[1].Extra.Members["custom_tag"]))

	require.Equal(t, "rs_2", resp.Parts[2].ID)
	require.Equal(t, []string{"step 1 + step 2"}, resp.Parts[2].Reasoning.Segments)
	require.True(t, resp.Parts[2].Reasoning.Summary)
	require.NotNil(t, resp.Parts[2].Reasoning.Token)
	require.Equal(t, "sig_kept", resp.Parts[2].Reasoning.Token.Data)
	require.Equal(t, `"kept"`, string(resp.Parts[2].Extra.Members["custom_tag"]))

	require.Equal(t, "tc_3", resp.Parts[3].ID)
	require.Equal(t, "call_kept_3", resp.Parts[3].Call.CallID)
	require.Equal(t, `{"a":1}`, resp.Parts[3].Call.Arguments)
	require.True(t, resp.Parts[3].Call.Unsigned)
	require.NotNil(t, resp.Parts[3].Call.Token)
	require.Equal(t, "gem_call_sig", resp.Parts[3].Call.Token.Data)
	require.Equal(t, `"kept"`, string(resp.Parts[3].Extra.Members["custom_tag"]))
}
