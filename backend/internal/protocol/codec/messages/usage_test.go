package messages

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
)

// TestMessagesUsageDisjointBucketsTTLAndCompatibleAliases verifies disjoint
// Anthropic prompt buckets, 5m/1h cache_creation TTL pointers, speed preservation,
// and compatible prompt_tokens or cache hit/miss alias normalization. Re-encoding
// disjoint IR buckets must keep input_tokens separate from cache reads and writes.
func TestMessagesUsageDisjointBucketsTTLAndCompatibleAliases(t *testing.T) {
	env := []byte(`{"usage":{"input_tokens":120,"cache_read_input_tokens":640,"cache_creation_input_tokens":300,"cache_creation":{"ephemeral_5m_input_tokens":180,"ephemeral_1h_input_tokens":120},"output_tokens":28,"speed":"fast"}}`)
	var holder struct {
		Usage jsontext.Value `json:"usage"`
	}
	require.NoError(t, unmarshalJSONObject(env, &holder))

	u, err := decodeMessagesUsage(env, holder.Usage)
	require.NoError(t, err)
	require.NotNil(t, u)
	require.Equal(t, int64(120), u.Input)
	require.Equal(t, int64(640), u.CacheRead)
	require.Equal(t, int64(300), u.CacheWrite)
	require.NotNil(t, u.CacheWrite5m)
	require.Equal(t, int64(180), *u.CacheWrite5m)
	require.NotNil(t, u.CacheWrite1h)
	require.Equal(t, int64(120), *u.CacheWrite1h)
	require.Equal(t, int64(28), u.Output)
	require.Equal(t, "fast", u.Speed)

	wireOut := encodeMessagesUsage(u)
	require.Equal(t, int64(120), wireOut.InputTokens)
	require.Equal(t, int64(640), wireOut.CacheReadInputTokens)
	require.Equal(t, int64(300), wireOut.CacheCreationInputTokens)
	require.Equal(t, int64(28), wireOut.OutputTokens)
	require.Equal(t, "fast", wireOut.Speed)
	require.NotNil(t, wireOut.CacheCreation)
	require.Equal(t, int64(180), *wireOut.CacheCreation.Ephemeral5mInputTokens)
	require.Equal(t, int64(120), *wireOut.CacheCreation.Ephemeral1hInputTokens)

	// Compatible inclusive prompt_tokens + prompt_tokens_details.cached_tokens.
	compatEnv := []byte(`{"usage":{"prompt_tokens":500,"output_tokens":15,"prompt_tokens_details":{"cached_tokens":200}}}`)
	require.NoError(t, unmarshalJSONObject(compatEnv, &holder))
	compatUsage, err := decodeMessagesUsage(compatEnv, holder.Usage)
	require.NoError(t, err)
	require.Equal(t, int64(300), compatUsage.Input)
	require.Equal(t, int64(200), compatUsage.CacheRead)
	require.Equal(t, int64(15), compatUsage.Output)

	// Compatible DeepSeek-style prompt_cache_hit_tokens + prompt_cache_miss_tokens.
	dsEnv := []byte(`{"usage":{"prompt_cache_hit_tokens":150,"prompt_cache_miss_tokens":50,"output_tokens":12}}`)
	require.NoError(t, unmarshalJSONObject(dsEnv, &holder))
	dsUsage, err := decodeMessagesUsage(dsEnv, holder.Usage)
	require.NoError(t, err)
	require.Equal(t, int64(50), dsUsage.Input)
	require.Equal(t, int64(150), dsUsage.CacheRead)
	require.Equal(t, int64(12), dsUsage.Output)
}

// TestMessagesUsageWireJSONGuardNegativeCountersAndSparseDelta verifies that
// duplicate or case-ambiguous usage keys and negative token counters fail decoding.
// Sparse message_delta updates must retain prior positive counters and subtract
// inherited cache buckets from compatible prompt_tokens identically to NormalizeStreamUsage.
func TestMessagesUsageWireJSONGuardNegativeCountersAndSparseDelta(t *testing.T) {
	badEnvelope := []byte(`{"USAGE":{"input_tokens":10,"output_tokens":5}}`)
	_, err := decodeMessagesUsage(badEnvelope, jsontext.Value(`{"input_tokens":10,"output_tokens":5}`))
	require.Error(t, err)

	badCase := []byte(`{"usage":{"Input_Tokens":10,"output_tokens":5}}`)
	_, err = decodeMessagesUsage(badCase, jsontext.Value(`{"Input_Tokens":10,"output_tokens":5}`))
	require.Error(t, err)

	dupKey := []byte(`{"usage":{"input_tokens":10,"output_tokens":5,"output_tokens":9}}`)
	_, err = decodeMessagesUsage(dupKey, jsontext.Value(`{"input_tokens":10,"output_tokens":5,"output_tokens":9}`))
	require.Error(t, err)

	dupNestedTTL := []byte(`{"usage":{"input_tokens":10,"output_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":4,"ephemeral_5m_input_tokens":8}}}`)
	_, err = decodeMessagesUsage(dupNestedTTL, jsontext.Value(`{"input_tokens":10,"output_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":4,"ephemeral_5m_input_tokens":8}}`))
	require.Error(t, err)

	negInput := []byte(`{"usage":{"input_tokens":-1,"output_tokens":5}}`)
	_, err = decodeMessagesUsage(negInput, jsontext.Value(`{"input_tokens":-1,"output_tokens":5}`))
	require.Error(t, err)

	negTTL := []byte(`{"usage":{"input_tokens":1,"output_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":-5}}}`)
	_, err = decodeMessagesUsage(negTTL, jsontext.Value(`{"input_tokens":1,"output_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":-5}}`))
	require.Error(t, err)

	var tracker streamUsageTracker
	var gatewayUsage protocol.TokenUsage

	startFrame := []byte(`{"type":"message_start","message":{"usage":{"input_tokens":100,"cache_read_input_tokens":40,"cache_creation_input_tokens":60,"cache_creation":{"ephemeral_5m_input_tokens":60},"output_tokens":3,"speed":"fast"}}}`)
	startRaw := jsontext.Value(`{"input_tokens":100,"cache_read_input_tokens":40,"cache_creation_input_tokens":60,"cache_creation":{"ephemeral_5m_input_tokens":60},"output_tokens":3,"speed":"fast"}`)
	var startEv anthropic.AnthropicStreamEvent
	require.NoError(t, json.Unmarshal(startFrame, &startEv))
	require.True(t, anthropic.NormalizeStreamUsage(startFrame, &startEv, &gatewayUsage))

	u1, ok, err := tracker.observe(startFrame, startRaw, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(gatewayUsage.InputTokens), u1.Input)
	require.Equal(t, int64(gatewayUsage.CacheReadInputTokens), u1.CacheRead)
	require.Equal(t, int64(gatewayUsage.CacheCreationInputTokens), u1.CacheWrite)
	require.Equal(t, int64(gatewayUsage.OutputTokens), u1.Output)
	require.Equal(t, int64(60), *u1.CacheWrite5m)
	require.Equal(t, int64(0), *u1.CacheWrite1h)
	require.Equal(t, "fast", u1.Speed)

	// Sparse message_delta with output_tokens:0 retains positive Output=3, matching NormalizeStreamUsage.
	deltaZeroOut := []byte(`{"type":"message_delta","usage":{"output_tokens":0}}`)
	var deltaZeroEv anthropic.AnthropicStreamEvent
	require.NoError(t, json.Unmarshal(deltaZeroOut, &deltaZeroEv))
	require.True(t, anthropic.NormalizeStreamUsage(deltaZeroOut, &deltaZeroEv, &gatewayUsage))

	u2, ok, err := tracker.observe(deltaZeroOut, jsontext.Value(`{"output_tokens":0}`), false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(3), u2.Output)
	require.Equal(t, int64(gatewayUsage.OutputTokens), u2.Output)
	require.Equal(t, int64(gatewayUsage.InputTokens), u2.Input)
	require.Equal(t, int64(gatewayUsage.CacheReadInputTokens), u2.CacheRead)
	require.Equal(t, int64(gatewayUsage.CacheCreationInputTokens), u2.CacheWrite)

	// Compatible prompt_tokens delta omitting cache details subtracts prior CacheRead (40)
	// and CacheWrite (60) without double-counting, matching ParseSSEUsagePassthrough (120 -> 20 + 40 + 60).
	var passthroughUsage protocol.TokenUsage
	anthropic.ParseSSEUsagePassthrough(string(startFrame), &passthroughUsage)
	compatDelta := []byte(`{"type":"message_delta","usage":{"prompt_tokens":120,"output_tokens":15}}`)
	anthropic.ParseSSEUsagePassthrough(string(compatDelta), &passthroughUsage)

	u3, ok, err := tracker.observe(compatDelta, jsontext.Value(`{"prompt_tokens":120,"output_tokens":15}`), false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(20), u3.Input)
	require.Equal(t, int64(40), u3.CacheRead)
	require.Equal(t, int64(60), u3.CacheWrite)
	require.Equal(t, int64(15), u3.Output)
	require.Equal(t, int64(120), u3.Input+u3.CacheRead+u3.CacheWrite)
	require.Equal(t, int64(passthroughUsage.InputTokens), u3.Input)
	require.Equal(t, int64(passthroughUsage.CacheReadInputTokens), u3.CacheRead)
	require.Equal(t, int64(passthroughUsage.CacheCreationInputTokens), u3.CacheWrite)
	require.Equal(t, int64(passthroughUsage.OutputTokens), u3.Output)

	// Sparse 1h TTL update on message_delta updates 1h bucket while keeping 5m=60.
	delta1h := []byte(`{"type":"message_delta","usage":{"output_tokens":35,"cache_creation_input_tokens":80,"cache_creation":{"ephemeral_1h_input_tokens":20}}}`)
	u4, ok, err := tracker.observe(delta1h, jsontext.Value(`{"output_tokens":35,"cache_creation_input_tokens":80,"cache_creation":{"ephemeral_1h_input_tokens":20}}`), false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(20), u4.Input)
	require.Equal(t, int64(40), u4.CacheRead)
	require.Equal(t, int64(80), u4.CacheWrite)
	require.Equal(t, int64(60), *u4.CacheWrite5m)
	require.Equal(t, int64(20), *u4.CacheWrite1h)
	require.Equal(t, int64(35), u4.Output)
	require.Equal(t, "fast", u4.Speed)
}
