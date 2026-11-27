package chat

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
)

// TestChatUsageDisjointBucketsAndAliases verifies inclusive prompt subtraction,
// DeepSeek hit/miss aliases, and xAI independent reasoning gap reconciliation.
// Re-encoding disjoint IR buckets must reconstruct inclusive prompt_tokens
// without double-counting cached reads or writes.
func TestChatUsageDisjointBucketsAndAliases(t *testing.T) {
	// Inclusive prompt_tokens (1060) = Input (120) + CacheRead (640) + CacheWrite (300).
	env := []byte(`{"usage":{"prompt_tokens":1060,"completion_tokens":28,"total_tokens":1088,"prompt_tokens_details":{"cached_tokens":640,"cache_write_tokens":300,"audio_tokens":5},"completion_tokens_details":{"reasoning_tokens":10,"audio_tokens":3,"accepted_prediction_tokens":4,"rejected_prediction_tokens":2}}}`)
	var holder struct {
		Usage jsontext.Value `json:"usage"`
	}
	require.NoError(t, unmarshalJSONObject(env, &holder))

	u, err := decodeChatUsage(env, holder.Usage)
	require.NoError(t, err)
	require.NotNil(t, u)
	require.Equal(t, int64(120), u.Input)
	require.Equal(t, int64(640), u.CacheRead)
	require.Equal(t, int64(300), u.CacheWrite)
	require.Equal(t, int64(28), u.Output)
	require.Equal(t, int64(10), u.Reasoning)
	require.Equal(t, int64(5), u.AudioInput)
	require.Equal(t, int64(3), u.AudioOutput)
	require.Equal(t, int64(4), u.AcceptedPrediction)
	require.Equal(t, int64(2), u.RejectedPrediction)

	// Re-encoding derives inclusive prompt_tokens = 1060 without double-counting.
	wireOut := encodeChatUsage(u)
	require.Equal(t, int64(1060), wireOut.PromptTokens)
	require.Equal(t, int64(28), wireOut.CompletionTokens)
	require.Equal(t, int64(1088), wireOut.TotalTokens)
	require.Equal(t, int64(640), wireOut.PromptTokensDetails.CachedTokens)
	require.Equal(t, int64(300), wireOut.PromptTokensDetails.CacheWriteTokens)

	// xAI/Grok independent reasoning gap: prompt=32, completion=9, reasoning=94, total=135 -> Output=103.
	grokEnv := []byte(`{"usage":{"prompt_tokens":32,"completion_tokens":9,"total_tokens":135,"completion_tokens_details":{"reasoning_tokens":94}}}`)
	require.NoError(t, unmarshalJSONObject(grokEnv, &holder))
	grokUsage, err := decodeChatUsage(grokEnv, holder.Usage)
	require.NoError(t, err)
	require.Equal(t, int64(32), grokUsage.Input)
	require.Equal(t, int64(103), grokUsage.Output)
	require.Equal(t, int64(94), grokUsage.Reasoning)

	// DeepSeek hit/miss partition + 30m/12h TTL details.
	dsEnv := []byte(`{"usage":{"prompt_cache_hit_tokens":200,"prompt_cache_miss_tokens":80,"completion_tokens":15,"input_tokens_details":{"cache_write_tokens":50,"cache_write_tokens_details":{"ttl_30m_tokens":30,"ttl_12h_tokens":20}}}}`)
	require.NoError(t, unmarshalJSONObject(dsEnv, &holder))
	dsUsage, err := decodeChatUsage(dsEnv, holder.Usage)
	require.NoError(t, err)
	require.Equal(t, int64(30), dsUsage.Input)
	require.Equal(t, int64(200), dsUsage.CacheRead)
	require.Equal(t, int64(50), dsUsage.CacheWrite)
	require.NotNil(t, dsUsage.CacheWrite30m)
	require.Equal(t, int64(30), *dsUsage.CacheWrite30m)
	require.NotNil(t, dsUsage.CacheWrite12h)
	require.Equal(t, int64(20), *dsUsage.CacheWrite12h)
}

// TestChatUsageWireJSONGuardAndSparseAccumulation verifies that case-ambiguous
// usage keys fail wirejson validation before reaching UsageAccumulator. Sparse
// non-zero tail updates must preserve prior prompt and cache counts unless an
// explicit all-zero snapshot resets the stream.
func TestChatUsageWireJSONGuardAndSparseAccumulation(t *testing.T) {
	badCase := []byte(`{"usage":{"Prompt_Tokens":10,"completion_tokens":5,"total_tokens":15}}`)
	_, err := decodeChatUsage(badCase, jsontext.Value(`{"Prompt_Tokens":10,"completion_tokens":5,"total_tokens":15}`))
	require.Error(t, err)

	badEnvelope := []byte(`{"USAGE":{"prompt_tokens":10,"completion_tokens":5}}`)
	_, err = decodeChatUsage(badEnvelope, jsontext.Value(`{"prompt_tokens":10,"completion_tokens":5}`))
	require.Error(t, err)

	var acc openai.UsageAccumulator
	chunk1 := []byte(`{"usage":{"prompt_tokens":150,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":50}}}`)
	u1, ok, err := observeStreamUsage(&acc, nil, chunk1, jsontext.Value(`{"prompt_tokens":150,"completion_tokens":0,"prompt_tokens_details":{"cached_tokens":50}}`))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(100), u1.Input)
	require.Equal(t, int64(50), u1.CacheRead)

	// Output-only tail chunk keeps prior Input=100 and CacheRead=50.
	chunk2 := []byte(`{"usage":{"completion_tokens":25}}`)
	u2, ok, err := observeStreamUsage(&acc, u1, chunk2, jsontext.Value(`{"completion_tokens":25}`))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(100), u2.Input)
	require.Equal(t, int64(50), u2.CacheRead)
	require.Equal(t, int64(25), u2.Output)

	// Partial zeroed prompt_tokens on non-zero output chunk also preserves prior Input/CacheRead.
	chunk3 := []byte(`{"usage":{"prompt_tokens":0,"completion_tokens":30}}`)
	u3, ok, err := observeStreamUsage(&acc, u2, chunk3, jsontext.Value(`{"prompt_tokens":0,"completion_tokens":30}`))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(100), u3.Input)
	require.Equal(t, int64(50), u3.CacheRead)
	require.Equal(t, int64(30), u3.Output)

	// Complete explicit all-zero snapshot resets every bucket to zero.
	chunkReset := []byte(`{"usage":{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}}`)
	uReset, ok, err := observeStreamUsage(&acc, u3, chunkReset, jsontext.Value(`{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, ir.Usage{}, *uReset)
}
