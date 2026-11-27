package generatecontent

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestGeminiUsageDisjointProjectionSparseMergeAndValidation feeds sparse multi-frame
// Gemini usageMetadata payloads through streamUsageTracker and round-trips the
// merged counters through encodeGeminiUsage. Disjoint input and output buckets
// must preserve modality counts while rejecting duplicate, case-colliding, or
// negative token fields.
func TestGeminiUsageDisjointProjectionSparseMergeAndValidation(t *testing.T) {
	var tracker streamUsageTracker
	f1 := []byte(`{"usageMetadata":{"promptTokenCount":500,"cachedContentTokenCount":300,"promptTokensDetails":[{"modality":"IMAGE","tokenCount":40},{"modality":"AUDIO","tokenCount":10}]}}`)
	var env1 wireResponse
	require.NoError(t, unmarshalJSONObject(f1, &env1))
	u1, ok, err := tracker.observe(f1, env1.UsageMetadata)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, &ir.Usage{
		Input:      200,
		CacheRead:  300,
		ImageInput: 40,
		AudioInput: 10,
	}, u1)

	f2 := []byte(`{"usageMetadata":{"candidatesTokenCount":35,"thoughtsTokenCount":15,"candidatesTokensDetails":[{"modality":"IMAGE","tokenCount":12},{"modality":"AUDIO","tokenCount":5}]}}`)
	var env2 wireResponse
	require.NoError(t, unmarshalJSONObject(f2, &env2))
	u2, ok, err := tracker.observe(f2, env2.UsageMetadata)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, &ir.Usage{
		Input:       200,
		CacheRead:   300,
		Output:      50,
		Reasoning:   15,
		ImageInput:  40,
		ImageOutput: 12,
		AudioInput:  10,
		AudioOutput: 5,
	}, u2)

	// Encode back to wireUsageMetadataWrite and re-decode to verify round-trip accounting.
	written := encodeGeminiUsage(u2)
	writtenEnv, err := marshalDeterministic(wireResponseWrite{UsageMetadata: written})
	require.NoError(t, err)
	var reEnv wireResponse
	require.NoError(t, unmarshalJSONObject(writtenEnv, &reEnv))
	reDecoded, err := decodeGeminiUsage(writtenEnv, reEnv.UsageMetadata)
	require.NoError(t, err)
	require.Equal(t, u2, reDecoded)

	// Reject duplicate keys, case-colliding keys, and negative token counts.
	dupEnv := []byte(`{"usageMetadata":{"promptTokenCount":10,"promptTokenCount":20}}`)
	_, err = decodeGeminiUsage(dupEnv, jsontext.Value(`{"promptTokenCount":10,"promptTokenCount":20}`))
	require.Error(t, err)

	caseEnv := []byte(`{"usageMetadata":{"promptTokenCount":10,"PromptTokenCount":20}}`)
	_, err = decodeGeminiUsage(caseEnv, jsontext.Value(`{"promptTokenCount":10,"PromptTokenCount":20}`))
	require.Error(t, err)

	outerCaseEnv := []byte(`{"usageMetadata":{"promptTokenCount":10},"usagemetadata":{"promptTokenCount":20}}`)
	_, err = decodeGeminiUsage(outerCaseEnv, jsontext.Value(`{"promptTokenCount":10}`))
	require.Error(t, err)

	negEnv := []byte(`{"usageMetadata":{"promptTokenCount":-1}}`)
	_, err = decodeGeminiUsage(negEnv, jsontext.Value(`{"promptTokenCount":-1}`))
	require.Error(t, err)
}
