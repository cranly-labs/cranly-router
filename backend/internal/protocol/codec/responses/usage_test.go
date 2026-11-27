package responses

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
)

// TestDecodeResponsesUsageDisjointBucketsAndImageGen checks inclusive prompt
// subtraction, 30m/12h and 5m/1h cache-write TTL extraction, outer-zero write
// backfill, and paired hosted tool_usage.image_gen accounting parity. Duplicate
// or case-ambiguous usage keys must fail wirejson validation.
func TestDecodeResponsesUsageDisjointBucketsAndImageGen(t *testing.T) {
	envelope := []byte(`{
		"id": "resp_u1",
		"object": "response",
		"tool_usage": {"image_gen": {"input_tokens_details": {"image_tokens": 12}, "output_tokens_details": {"image_tokens": 90}}},
		"usage": {
			"input_tokens": 1000,
			"output_tokens": 150,
			"total_tokens": 1150,
			"input_tokens_details": {
				"cached_tokens": 600,
				"audio_tokens": 15,
				"cache_write_tokens_details": {
					"ttl_30m_tokens": 180,
					"ttl_12h_tokens": 120
				}
			},
			"output_tokens_details": {
				"reasoning_tokens": 40,
				"audio_tokens": 10,
				"accepted_prediction_tokens": 6,
				"rejected_prediction_tokens": 2
			},
			"cache_creation": {
				"ephemeral_5m_input_tokens": 50,
				"ephemeral_1h_input_tokens": 250
			}
		}
	}`)
	rawUsage := jsontext.Value(`{
		"input_tokens": 1000,
		"output_tokens": 150,
		"total_tokens": 1150,
		"input_tokens_details": {
			"cached_tokens": 600,
			"audio_tokens": 15,
			"cache_write_tokens_details": {
				"ttl_30m_tokens": 180,
				"ttl_12h_tokens": 120
			}
		},
		"output_tokens_details": {
			"reasoning_tokens": 40,
			"audio_tokens": 10,
			"accepted_prediction_tokens": 6,
			"rejected_prediction_tokens": 2
		},
		"cache_creation": {
			"ephemeral_5m_input_tokens": 50,
			"ephemeral_1h_input_tokens": 250
		}
	}`)

	u, err := decodeResponsesUsage(envelope, rawUsage)
	require.NoError(t, err)
	require.NotNil(t, u)
	require.Equal(t, int64(100), u.Input, "1000 inclusive - 600 read - 300 backfilled write = 100 fresh")
	require.Equal(t, int64(600), u.CacheRead)
	require.Equal(t, int64(300), u.CacheWrite)
	require.NotNil(t, u.CacheWrite30m)
	require.Equal(t, int64(180), *u.CacheWrite30m)
	require.NotNil(t, u.CacheWrite12h)
	require.Equal(t, int64(120), *u.CacheWrite12h)
	require.NotNil(t, u.CacheWrite5m)
	require.Equal(t, int64(50), *u.CacheWrite5m)
	require.NotNil(t, u.CacheWrite1h)
	require.Equal(t, int64(250), *u.CacheWrite1h)
	require.Equal(t, int64(150), u.Output)
	require.Equal(t, int64(40), u.Reasoning)
	require.Equal(t, int64(12), u.ImageInput)
	require.Equal(t, int64(90), u.ImageOutput)
	require.Equal(t, int64(15), u.AudioInput)
	require.Equal(t, int64(10), u.AudioOutput)
	require.Equal(t, int64(6), u.AcceptedPrediction)
	require.Equal(t, int64(2), u.RejectedPrediction)

	// Duplicate or case-variant usage keys must be rejected by wirejson.
	dupEnv := []byte(`{"id":"r","usage":{"input_tokens":10,"INPUT_TOKENS":20,"output_tokens":5}}`)
	_, err = decodeResponsesUsage(dupEnv, jsontext.Value(`{"input_tokens":10,"INPUT_TOKENS":20,"output_tokens":5}`))
	require.Error(t, err)
}

// TestObserveResponsesStreamUsageSparseMonotonicAndEncode checks sparse stream
// tail merging across UsageAccumulator and strict Codex detail serialization in
// encodeResponsesUsage. Non-matching 30m/12h splits must be omitted on wire output.
func TestObserveResponsesStreamUsageSparseMonotonicAndEncode(t *testing.T) {
	var acc openai.UsageAccumulator
	frame1 := []byte(`{"type":"response.in_progress","response":{"usage":{"input_tokens":500,"output_tokens":0,"input_tokens_details":{"cached_tokens":300,"cache_write_tokens":50,"cache_write_tokens_details":{"ttl_30m_tokens":30,"ttl_12h_tokens":20}}}}}`)
	u1, ok, err := observeResponsesStreamUsage(&acc, nil, frame1, frame1, false)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(150), u1.Input)
	require.Equal(t, int64(300), u1.CacheRead)
	require.Equal(t, int64(50), u1.CacheWrite)

	// Sparse output-only terminal event without prompt counts retains prior prompt/cache buckets.
	frame2 := []byte(`{"type":"response.completed","response":{"usage":{"output_tokens":42,"output_tokens_details":{"reasoning_tokens":12}}}}`)
	u2, ok, err := observeResponsesStreamUsage(&acc, u1, frame2, frame2, true)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, int64(150), u2.Input)
	require.Equal(t, int64(300), u2.CacheRead)
	require.Equal(t, int64(50), u2.CacheWrite)
	require.Equal(t, int64(42), u2.Output)
	require.Equal(t, int64(12), u2.Reasoning)

	encoded := encodeResponsesUsage(u2)
	require.NotNil(t, encoded)
	require.Equal(t, int64(500), encoded.InputTokens)
	require.Equal(t, int64(542), encoded.TotalTokens)
	require.Equal(t, int64(300), encoded.InputTokensDetails.CachedTokens)
	require.Equal(t, int64(12), encoded.OutputTokensDetails.ReasoningTokens)
	require.NotNil(t, encoded.InputTokensDetails.CacheWriteDetails)
	require.Equal(t, int64(30), encoded.InputTokensDetails.CacheWriteDetails.TTL30mTokens)
	require.Equal(t, int64(20), encoded.InputTokensDetails.CacheWriteDetails.TTL12hTokens)

	// Mismatched 30m + 12h != CacheWrite omits cache_write_tokens_details while keeping cached_tokens.
	badSplit := int64(10)
	mismatchEncoded := encodeResponsesUsage(&ir.Usage{
		Input:         20,
		CacheWrite:    50,
		CacheWrite30m: &badSplit,
		Output:        5,
	})
	require.Nil(t, mismatchEncoded.InputTokensDetails.CacheWriteDetails)
	require.Equal(t, int64(0), mismatchEncoded.InputTokensDetails.CachedTokens)
	require.Equal(t, int64(0), mismatchEncoded.OutputTokensDetails.ReasoningTokens)
}
