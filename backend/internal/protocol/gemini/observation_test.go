package gemini

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// TestObservePayload_SeparatesUsageSemanticAndTerminalEvidence feeds zero-count
// usageMetadata, functionCall candidate parts, and finishReason-only frames to
// ObservePayload. Each payload must set only its corresponding HasUsage,
// Semantic, or Terminal flag without implying the other two states.
func TestObservePayload_SeparatesUsageSemanticAndTerminalEvidence(t *testing.T) {
	usageOnly := ObservePayload([]byte(`{"usageMetadata":{"promptTokenCount":0}}`))
	require.True(t, usageOnly.HasUsage)
	require.False(t, usageOnly.Semantic)
	require.False(t, usageOnly.Terminal)

	contentOnly := ObservePayload([]byte(`{"candidates":[{"content":{"parts":[{"functionCall":{"name":"lookup"}}]}}]} `))
	require.False(t, contentOnly.HasUsage)
	require.True(t, contentOnly.Semantic)
	require.False(t, contentOnly.Terminal)

	terminal := ObservePayload([]byte(`{"candidates":[{"finishReason":"STOP"}]}`))
	require.False(t, terminal.HasUsage)
	require.False(t, terminal.Semantic)
	require.True(t, terminal.Terminal)
}

// TestNativeExtractGeminiUsage_PartitionsCachedPromptThoughtsAndImageDetails
// supplies cached prompt counts, thinking tokens, and duplicate IMAGE modality
// entries. Extraction and projection must separate uncached input from cache
// reads, include thoughts in output, and pick the first IMAGE modality count.
func TestNativeExtractGeminiUsage_PartitionsCachedPromptThoughtsAndImageDetails(t *testing.T) {
	require.Nil(t, NativeExtractGeminiUsage([]byte(`{"candidates":[]}`)))
	require.Nil(t, NativeUsageProjection(nil))

	for _, cached := range []int{0, 60, 100} {
		raw, err := json.Marshal(map[string]any{
			"usageMetadata": map[string]any{
				"promptTokenCount":        100,
				"cachedContentTokenCount": cached,
				"candidatesTokenCount":    15,
				"thoughtsTokenCount":      7,
				"candidatesTokensDetails": []any{
					map[string]any{"modality": "TEXT", "tokenCount": 5},
					map[string]any{"modality": "IMAGE", "tokenCount": 10},
					map[string]any{"modality": "IMAGE", "tokenCount": 99},
				},
			},
		})
		require.NoError(t, err)

		got := NativeExtractGeminiUsage(raw)
		require.NotNil(t, got)
		require.Equal(t, 100-cached, got.InputTokens)
		require.Equal(t, cached, got.CacheReadInputTokens)
		require.Equal(t, 22, got.OutputTokens)
		require.Equal(t, 10, got.ImageOutputTokens)

		projected := NativeUsageProjection(got)
		require.Equal(t, &protocol.TokenUsage{
			InputTokens:          100 - cached,
			OutputTokens:         22,
			CacheReadInputTokens: cached,
			ImageOutputTokens:    10,
		}, projected)
	}
}
