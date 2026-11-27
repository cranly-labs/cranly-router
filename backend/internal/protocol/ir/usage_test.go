package ir

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// TestUsageJSONRoundTrip marshals a Usage snapshot containing explicit zero and
// positive TTL cache-write pointers alongside nil TTL pointers. Nil TTL buckets
// must stay omitted under omitzero while explicit zero pointers round-trip intact.
func TestUsageJSONRoundTrip(t *testing.T) {
	zero := int64(0)
	write5m := int64(120)
	write30m := int64(45)

	u := Usage{
		Input:              300,
		CacheRead:          100,
		CacheWrite:         165,
		CacheWrite5m:       &write5m,
		CacheWrite30m:      &write30m,
		CacheWrite1h:       &zero,
		Output:             80,
		Reasoning:          30,
		ImageInput:         12,
		ImageOutput:        24,
		AudioInput:         5,
		AudioOutput:        9,
		AcceptedPrediction: 4,
		RejectedPrediction: 2,
		Speed:              "fast",
	}

	data, err := json.Marshal(u, json.Deterministic(true))
	require.NoError(t, err)
	require.Contains(t, string(data), `"cache_write_1h":0`)
	require.NotContains(t, string(data), `"cache_write_12h"`)

	var got Usage
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, u, got)
}

// TestUsageTokenUsageConversionAndTotals exercises UsageFromTokenUsage,
// ToTokenUsage, EffectiveCacheWrite, InclusiveInputTokens, TotalTokens, and
// Clone across nil, speed-only, negative, and multi-TTL inputs. Cached input
// buckets must sum once and detail-only cache writes must backfill missing totals.
func TestUsageTokenUsageConversionAndTotals(t *testing.T) {
	var nilUsage *Usage
	require.Nil(t, nilUsage.Clone())
	require.Equal(t, protocol.TokenUsage{}, nilUsage.ToTokenUsage())
	require.Zero(t, nilUsage.EffectiveCacheWrite())
	require.Zero(t, nilUsage.InclusiveInputTokens())
	require.Zero(t, nilUsage.TotalTokens())
	require.False(t, nilUsage.HasObservedTokens())

	speedOnly := &Usage{Speed: "fast"}
	require.False(t, speedOnly.HasObservedTokens())

	srcTokens := protocol.TokenUsage{
		InputTokens:              120,
		OutputTokens:             28,
		CacheReadInputTokens:     640,
		CacheCreationInputTokens: 300,
		CacheCreation5mTokens:    180,
		CacheCreation1hTokens:    120,
		ImageOutputTokens:        16,
		Speed:                    "fast",
	}
	converted := UsageFromTokenUsage(srcTokens)
	require.Equal(t, int64(120), converted.Input)
	require.Equal(t, int64(640), converted.CacheRead)
	require.Equal(t, int64(300), converted.CacheWrite)
	require.NotNil(t, converted.CacheWrite5m)
	require.Equal(t, int64(180), *converted.CacheWrite5m)
	require.NotNil(t, converted.CacheWrite1h)
	require.Equal(t, int64(120), *converted.CacheWrite1h)
	require.Equal(t, int64(1060), converted.InclusiveInputTokens())
	require.Equal(t, int64(1088), converted.TotalTokens())
	require.True(t, converted.HasObservedTokens())
	require.Equal(t, srcTokens, converted.ToTokenUsage())

	// Detail-only cache creation counts backfill a missing outer aggregate once.
	detailOnly := UsageFromTokenUsage(protocol.TokenUsage{
		InputTokens:           10,
		CacheCreation5mTokens: 40,
		CacheCreation1hTokens: 60,
	})
	require.Equal(t, int64(100), detailOnly.CacheWrite)
	require.Equal(t, int64(110), detailOnly.InclusiveInputTokens())

	w30m := int64(70)
	w12h := int64(30)
	openAISplit := &Usage{
		Input:         50,
		CacheRead:     20,
		CacheWrite30m: &w30m,
		CacheWrite12h: &w12h,
		Output:        15,
	}
	require.Equal(t, int64(100), openAISplit.EffectiveCacheWrite())
	require.Equal(t, int64(170), openAISplit.InclusiveInputTokens())
	require.Equal(t, 100, openAISplit.ToTokenUsage().CacheCreationInputTokens)

	cloned := openAISplit.Clone()
	*cloned.CacheWrite30m = 999
	require.Equal(t, int64(70), *openAISplit.CacheWrite30m)

	negative := &Usage{Input: -5, Output: -2}
	require.Equal(t, protocol.TokenUsage{}, negative.ToTokenUsage())
}
