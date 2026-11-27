package ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAccumulatorFoldsCanonicalAndErrorStreams feeds normal multi-part, empty,
// mid-stream error, and malformed event sequences through Accumulate and
// Accumulator. It verifies snapshot isolation, terminal ServiceTier precedence,
// Incomplete tool-call preservation on error streams, and rejection of invalid streams.
func TestAccumulatorFoldsCanonicalAndErrorStreams(t *testing.T) {
	w5m := int64(40)
	events := []Event{
		{
			Kind: EventResponseStart,
			Response: &ResponseMeta{
				ID:          "resp_stream_1",
				Model:       "claude-sonnet-4-5",
				CreatedAt:   1760000500,
				ServiceTier: "standard",
			},
		},
		{
			Kind:  EventUsage,
			Usage: &Usage{Input: 100, CacheWrite: 40, CacheWrite5m: &w5m, Output: 0},
		},
		{
			Kind:  EventPartStart,
			Index: 0,
			Part:  &Part{Kind: PartReasoning, Reasoning: &Reasoning{Summary: true}},
		},
		{
			Kind:  EventPartDelta,
			Index: 0,
			Delta: &Delta{Text: "Step A"},
		},
		{
			Kind:  EventPartEnd,
			Index: 0,
			Part: &Part{
				Kind: PartReasoning,
				Reasoning: &Reasoning{
					Segments: []string{"Step A"},
					Summary:  true,
					Token:    &ReplayToken{Scheme: SchemeAnthropic, Data: "sig_a"},
				},
			},
		},
		{
			Kind:  EventPartStart,
			Index: 1,
			Part:  &Part{Kind: PartText},
		},
		{
			Kind:  EventPartDelta,
			Index: 1,
			Delta: &Delta{Text: "Final "},
		},
		{
			Kind:  EventPartDelta,
			Index: 1,
			Delta: &Delta{Text: "answer"},
		},
		{
			Kind:  EventPartEnd,
			Index: 1,
			Part:  &Part{Kind: PartText, Text: "Final answer"},
		},
		{
			Kind:  EventUsage,
			Usage: &Usage{Input: 100, CacheWrite: 40, CacheWrite5m: &w5m, Output: 22, Reasoning: 8, Speed: "fast"},
		},
		{
			Kind:        EventResponseEnd,
			Finish:      &Finish{Reason: FinishStopSequence, StopSequence: "</done>"},
			ServiceTier: "priority",
		},
	}

	resp, err := Accumulate(events)
	require.NoError(t, err)
	require.NoError(t, ValidateResponse(resp))
	require.Equal(t, "resp_stream_1", resp.ID)
	require.Equal(t, "claude-sonnet-4-5", resp.Model)
	require.Equal(t, int64(1760000500), resp.CreatedAt)
	require.Equal(t, "priority", resp.ServiceTier)
	require.Equal(t, Finish{Reason: FinishStopSequence, StopSequence: "</done>"}, resp.Finish)
	require.Len(t, resp.Parts, 2)
	require.Equal(t, "Final answer", resp.Parts[1].Text)
	require.Equal(t, int64(22), resp.Usage.Output)

	// Verify incremental folding, post-terminal ignore, and detached snapshots.
	acc := NewAccumulator()
	acc.Add(events...)
	require.True(t, acc.Finished())
	require.Nil(t, acc.StreamError())
	snap1 := acc.Response()
	snap1.Parts[1].Text = "mutated"
	acc.Add(Event{Kind: EventPartEnd, Index: 2, Part: &Part{Kind: PartText, Text: "ignored"}})
	snap2 := acc.Response()
	require.Len(t, snap2.Parts, 2)
	require.Equal(t, "Final answer", snap2.Parts[1].Text)

	// Empty stream response still produces a non-nil empty Parts slice.
	emptyResp, err := Accumulate([]Event{
		{Kind: EventResponseStart, Response: &ResponseMeta{ID: "empty_1"}},
		{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
	})
	require.NoError(t, err)
	require.NotNil(t, emptyResp.Parts)
	require.Empty(t, emptyResp.Parts)

	// Abnormal error stream preserves Incomplete tool calls and records StreamError.
	errEvents := []Event{
		{Kind: EventResponseStart, Response: &ResponseMeta{ID: "err_1"}},
		{
			Kind:  EventPartStart,
			Index: 0,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}},
			},
		},
		{Kind: EventPartDelta, Index: 0, Delta: &Delta{Arguments: `{"x":`}},
		{
			Kind:  EventPartEnd,
			Index: 0,
			Part: &Part{
				Kind: PartToolCall,
				Call: &ToolCall{
					CallID:     "c1",
					Tool:       ToolRef{Kind: ToolFunction, Name: "fn"},
					Arguments:  `{"x":`,
					Incomplete: true,
				},
			},
		},
		{Kind: EventError, Err: &StreamError{Code: "eof", Message: "unexpected EOF", Status: 502}},
	}
	var errAcc Accumulator
	errAcc.Add(errEvents...)
	require.True(t, errAcc.Finished())
	require.NotNil(t, errAcc.StreamError())
	require.Equal(t, "unexpected EOF", errAcc.StreamError().Message)
	errResp := errAcc.Response()
	require.NoError(t, ValidateResponse(errResp))
	require.Equal(t, FinishError, errResp.Finish.Reason)
	require.True(t, errResp.Parts[0].Call.Incomplete)

	// Accumulate rejects malformed streams.
	_, err = Accumulate([]Event{{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}}})
	require.Error(t, err)
}
