package ir

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// TestResponseJSONRoundTrip marshals a buffered Response with stop-sequence
// finish metadata and token usage counters. It checks deterministic JSON
// round-trip preservation across all response envelope fields.
func TestResponseJSONRoundTrip(t *testing.T) {
	write5m := int64(120)
	resp := Response{
		ID:        "resp_01",
		Model:     "gpt-5",
		CreatedAt: 1760000000,
		Parts: []Part{
			{Kind: PartText, Text: "Completed answer."},
		},
		Finish: Finish{
			Reason:       FinishStopSequence,
			StopSequence: "END",
		},
		Usage: &Usage{
			Input:        300,
			CacheRead:    100,
			CacheWrite:   120,
			CacheWrite5m: &write5m,
			Output:       80,
		},
		ServiceTier: "priority",
	}

	data, err := json.Marshal(resp, json.Deterministic(true))
	require.NoError(t, err)

	var got Response
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, resp, got)
}

// TestFinishReasonIsValidAndResponseClone checks FinishReason.IsValid on all
// canonical and unknown reasons and mutates nested slices and maps on a cloned
// Response. Nil receivers must return nil and empty non-nil Parts slices must stay non-nil.
func TestFinishReasonIsValidAndResponseClone(t *testing.T) {
	for _, valid := range []FinishReason{
		FinishStop, FinishLength, FinishToolCalls, FinishContentFilter,
		FinishStopSequence, FinishPause, FinishError, FinishOther,
	} {
		require.True(t, valid.IsValid(), "expected %q to be valid", valid)
	}
	require.False(t, FinishReason("").IsValid())
	require.False(t, FinishReason("unknown_reason").IsValid())

	var nilResp *Response
	require.Nil(t, nilResp.Clone())

	w1h := int64(50)
	orig := &Response{
		ID:    "resp_clone",
		Parts: []Part{{Kind: PartText, Text: "original"}},
		Finish: Finish{
			Reason: FinishStop,
		},
		Usage: &Usage{
			Input:        10,
			CacheWrite:   50,
			CacheWrite1h: &w1h,
		},
		Extra: Extra{
			Origin:  protocol.ProtocolOpenAIResponses,
			Members: map[string]jsontext.Value{"custom": jsontext.Value(`"v1"`)},
		},
	}

	cp := orig.Clone()
	cp.Parts[0].Text = "mutated"
	*cp.Usage.CacheWrite1h = 999
	cp.Extra.Members["custom"] = jsontext.Value(`"v2"`)

	require.Equal(t, "original", orig.Parts[0].Text)
	require.Equal(t, int64(50), *orig.Usage.CacheWrite1h)
	require.Equal(t, `"v1"`, string(orig.Extra.Members["custom"]))

	emptyPartsResp := &Response{Parts: []Part{}, Finish: Finish{Reason: FinishStop}}
	clonedEmpty := emptyPartsResp.Clone()
	require.NotNil(t, clonedEmpty.Parts)
	require.Empty(t, clonedEmpty.Parts)
}
