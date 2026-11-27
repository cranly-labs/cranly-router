package anthropic

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// TestNormalizeStreamUsageSharesCacheOnlyTailWithClient starts with empty usage
// and supplies cache counts in a later event. The canonical client event and
// internal accounting must retain identical reads and lifetime write totals.
func TestNormalizeStreamUsageSharesCacheOnlyTailWithClient(t *testing.T) {
	var usage protocol.TokenUsage
	frames := []string{
		`{"type":"message_start","message":{"usage":{"input_tokens":0,"output_tokens":0}}}`,
		`{"type":"message_delta","usage":{"prompt_cache_hit_tokens":60,"prompt_cache_miss_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":12,"ephemeral_1h_input_tokens":8}}}`,
		`{"type":"message_delta","usage":{"output_tokens":10}}`,
	}
	for _, raw := range frames {
		var event AnthropicStreamEvent
		require.NoError(t, json.Unmarshal([]byte(raw), &event))
		require.True(t, NormalizeStreamUsage([]byte(raw), &event, &usage))
		if event.Usage != nil {
			require.Equal(t, usage.InputTokens, event.Usage.InputTokens)
			require.Equal(t, usage.CacheReadInputTokens, event.Usage.CacheReadInputTokens)
			require.Equal(t, usage.CacheCreationInputTokens, event.Usage.CacheCreationInputTokens)
		}
	}
	require.Zero(t, usage.InputTokens)
	require.Equal(t, 60, usage.CacheReadInputTokens)
	require.Equal(t, 20, usage.CacheCreationInputTokens)
	require.Equal(t, 10, usage.OutputTokens)
}
