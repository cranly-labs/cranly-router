package ir

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEventJSONVariants round-trips every canonical EventKind through JSON v2.
// It verifies that start metadata, part indices, deltas, usage snapshots,
// terminal finishes, and stream errors preserve their values without leaking
// unrelated union branches.
func TestEventJSONVariants(t *testing.T) {
	events := []Event{
		{
			Kind: EventResponseStart,
			Response: &ResponseMeta{
				ID:          "msg_1",
				Model:       "claude-sonnet-4-5",
				CreatedAt:   1760000100,
				ServiceTier: "standard",
			},
		},
		{
			Kind:  EventPartStart,
			Index: 0,
			Part:  &Part{Kind: PartText},
		},
		{
			Kind:  EventPartDelta,
			Index: 0,
			Delta: &Delta{Text: "hello"},
		},
		{
			Kind:  EventPartEnd,
			Index: 0,
			Part:  &Part{Kind: PartText, Text: "hello"},
		},
		{
			Kind:  EventUsage,
			Usage: &Usage{Input: 10, Output: 5},
		},
		{
			Kind:        EventResponseEnd,
			Finish:      &Finish{Reason: FinishStop},
			ServiceTier: "priority",
		},
		{
			Kind: EventError,
			Err: &StreamError{
				Type:    "overloaded_error",
				Code:    "overloaded",
				Message: "upstream overloaded",
				Status:  529,
			},
		},
	}

	raw, err := json.Marshal(events, json.Deterministic(true))
	require.NoError(t, err)

	var decoded []Event
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.Equal(t, events, decoded)
}

// TestEventKindPredicatesAndClone checks EventKind.IsValid and IsTerminal on
// valid and unknown event tags, then mutates every pointer branch on a cloned
// Event. Original Part, Usage, Delta, Finish, and StreamError fields must stay unchanged.
func TestEventKindPredicatesAndClone(t *testing.T) {
	for _, k := range []EventKind{
		EventResponseStart, EventPartStart, EventPartDelta, EventPartEnd,
		EventUsage, EventResponseEnd, EventError,
	} {
		require.True(t, k.IsValid())
	}
	require.False(t, EventKind("bogus").IsValid())
	require.True(t, EventResponseEnd.IsTerminal())
	require.True(t, EventError.IsTerminal())
	require.False(t, EventPartEnd.IsTerminal())

	var nilDelta *Delta
	require.Nil(t, nilDelta.Clone())

	w5m := int64(10)
	ev := Event{
		Kind:     EventPartEnd,
		Index:    1,
		Response: &ResponseMeta{ID: "r1"},
		Part:     &Part{Kind: PartReasoning, Reasoning: &Reasoning{Segments: []string{"s1"}}},
		Delta:    &Delta{Text: "d1"},
		Usage:    &Usage{Input: 5, CacheWrite5m: &w5m},
		Finish:   &Finish{Reason: FinishStop},
		Err:      &StreamError{Message: "boom"},
	}

	cp := ev.Clone()
	cp.Response.ID = "r2"
	cp.Part.Reasoning.Segments[0] = "s2"
	cp.Delta.Text = "d2"
	*cp.Usage.CacheWrite5m = 99
	cp.Finish.Reason = FinishError
	cp.Err.Message = "changed"

	require.Equal(t, "r1", ev.Response.ID)
	require.Equal(t, "s1", ev.Part.Reasoning.Segments[0])
	require.Equal(t, "d1", ev.Delta.Text)
	require.Equal(t, int64(10), *ev.Usage.CacheWrite5m)
	require.Equal(t, FinishStop, ev.Finish.Reason)
	require.Equal(t, "boom", ev.Err.Message)
	require.Equal(t, "boom", ev.Err.Error())
	var nilStreamErr *StreamError
	require.Empty(t, nilStreamErr.Error())
}
