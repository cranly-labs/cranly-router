package openai

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// directSHA256Hex computes the raw lowercase SHA-256 hex digest of s without
// carrier unwrapping so tests can verify byte-identical legacy digest outputs.
func directSHA256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// TestOpenAIEncryptedContentDigest_NonCarrierInputsMatchDirectSHA256 feeds bare
// ciphertexts, random strings, malformed trc1 prefixes, empty or whitespace
// inputs, and tokenless plain envelopes into OpenAIEncryptedContentDigest. It
// verifies that every input without an issuer-token carrier preserves the exact
// pre-change SHA-256 hex digest without collisions.
func TestOpenAIEncryptedContentDigest_NonCarrierInputsMatchDirectSHA256(t *testing.T) {
	t.Parallel()

	dupKeyPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"d":"a","d":"b"}`))
	zeroProviderPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"d":"a","p":0}`))
	negProviderPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"d":"a","p":-1}`))
	unknownKeyPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"d":"a","x":1}`))
	missingDataPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"t":"thought"}`))
	arrayPayload := base64.RawURLEncoding.EncodeToString([]byte(`["d"]`))
	validOpenAIPayload := base64.RawURLEncoding.EncodeToString([]byte(`{"d":"gAAAAABnCipher"}`))

	plainA, err := ir.FormatCarrier(ir.Carrier{Scheme: ir.SchemePlain, Text: "first reasoning turn"})
	require.NoError(t, err)
	plainB, err := ir.FormatCarrier(ir.Carrier{Scheme: ir.SchemePlain, Text: "second reasoning turn"})
	require.NoError(t, err)

	cases := []struct {
		name  string // input category label
		input string // raw encrypted_content string
	}{
		{"empty_string", ""},
		{"spaces_only", "   "},
		{"tabs_and_newlines", "\t\r\n "},
		{"bare_fernet_ciphertext", "gAAAAABn9X2mQp7R0sK8vLwYz1234567890_ciphertext-payload"},
		{"bare_compaction_token", "cmp_enc_9f8e7d6c5b4a3210"},
		{"ordinary_random_text", "random-opaque-state-value-42"},
		{"invalid_utf8_raw_bytes", "cipher-\xff\xfe-raw"},
		{"leading_space_before_prefix", " trc1:openai:" + validOpenAIPayload},
		{"bare_trc1_prefix", "trc1:"},
		{"trc1_missing_payload_separator", "trc1:openai"},
		{"trc1_empty_payload", "trc1:openai:"},
		{"trc1_extra_colon", "trc1:openai:" + validOpenAIPayload + ":extra"},
		{"trc1_unknown_scheme", "trc1:custom_vendor:" + validOpenAIPayload},
		{"trc1_padded_base64", "trc1:openai:" + validOpenAIPayload + "="},
		{"trc1_invalid_base64_chars", "trc1:openai:not!valid@base64"},
		{"trc1_duplicate_json_key", "trc1:openai:" + dupKeyPayload},
		{"trc1_zero_provider_id", "trc1:openai:" + zeroProviderPayload},
		{"trc1_negative_provider_id", "trc1:openai:" + negProviderPayload},
		{"trc1_unknown_json_member", "trc1:openai:" + unknownKeyPayload},
		{"trc1_openai_missing_data", "trc1:openai:" + missingDataPayload},
		{"trc1_json_array_payload", "trc1:openai:" + arrayPayload},
		{"trc1_plain_tokenless_a", plainA},
		{"trc1_plain_tokenless_b", plainB},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, directSHA256Hex(tc.input), OpenAIEncryptedContentDigest(tc.input))
		})
	}

	require.NotEqual(t, OpenAIEncryptedContentDigest(plainA), OpenAIEncryptedContentDigest(plainB))
	require.NotEqual(t, OpenAIEncryptedContentDigest(""), OpenAIEncryptedContentDigest(plainA))
}

// TestOpenAIEncryptedContentDigest_ValidCarriersUnwrapIssuerData wraps bare
// ciphertexts and signatures inside all five issuer-token trc1 schemes. It
// verifies that every valid envelope hashes to the exact SHA-256 digest of its
// inner issuer token d regardless of provider ID, text, or redaction metadata.
func TestOpenAIEncryptedContentDigest_ValidCarriersUnwrapIssuerData(t *testing.T) {
	t.Parallel()

	bareOpenAI := "gAAAAABnOpenAICiphertextPayload_001"
	bareXAI := "xai_encrypted_reasoning_blob_77"
	bareAnthropic := "EqQBCgIYAhATGhR_anthropic_sig"
	bareAntigravity := "ag_claude_sig_payload_99"
	bareGemini := "skip_or_real_gemini_thought_sig"

	cases := []struct {
		name    string     // carrier scenario label
		carrier ir.Carrier // envelope definition to format and hash
		wantRaw string     // inner issuer token d expected to govern the digest
	}{
		{
			name:    "openai_without_provider_id",
			carrier: ir.Carrier{Scheme: ir.SchemeOpenAI, Data: bareOpenAI},
			wantRaw: bareOpenAI,
		},
		{
			name:    "openai_provider_101",
			carrier: ir.Carrier{Scheme: ir.SchemeOpenAI, Data: bareOpenAI, ProviderID: 101},
			wantRaw: bareOpenAI,
		},
		{
			name:    "openai_provider_202_matches_same_d",
			carrier: ir.Carrier{Scheme: ir.SchemeOpenAI, Data: bareOpenAI, ProviderID: 202},
			wantRaw: bareOpenAI,
		},
		{
			name:    "xai_with_provider_id",
			carrier: ir.Carrier{Scheme: ir.SchemeXAI, Data: bareXAI, ProviderID: 303},
			wantRaw: bareXAI,
		},
		{
			name:    "anthropic_with_verbatim_text",
			carrier: ir.Carrier{Scheme: ir.SchemeAnthropic, Data: bareAnthropic, Text: "step-by-step chain"},
			wantRaw: bareAnthropic,
		},
		{
			name:    "anthropic_redacted",
			carrier: ir.Carrier{Scheme: ir.SchemeAnthropic, Data: bareAnthropic, Redacted: true},
			wantRaw: bareAnthropic,
		},
		{
			name:    "antigravity_claude_with_text",
			carrier: ir.Carrier{Scheme: ir.SchemeAntigravityClaude, Data: bareAntigravity, Text: "claude thought"},
			wantRaw: bareAntigravity,
		},
		{
			name:    "gemini_thought_signature",
			carrier: ir.Carrier{Scheme: ir.SchemeGemini, Data: bareGemini},
			wantRaw: bareGemini,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			envelope, err := ir.FormatCarrier(tc.carrier)
			require.NoError(t, err)
			require.NotEqual(t, tc.wantRaw, envelope)

			got := OpenAIEncryptedContentDigest(envelope)
			require.Equal(t, directSHA256Hex(tc.wantRaw), got)
			require.Equal(t, OpenAIEncryptedContentDigest(tc.wantRaw), got)
		})
	}
}

// TestStripOpenAIInvalidEncryptedContentMatchesAcrossCarrierAndBareStates
// records invalidation digests from bare upstream ciphertexts and client trc1
// envelopes across HTTP and WS replay helpers. It verifies that invalidating
// one representation strips both representations while retaining fresh items.
func TestStripOpenAIInvalidEncryptedContentMatchesAcrossCarrierAndBareStates(t *testing.T) {
	t.Parallel()

	staleBare := "gAAAAABnStaleSessionCiphertext"
	freshBare := "gAAAAABnFreshSessionCiphertext"
	staleEnvelope, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       staleBare,
		ProviderID: 42,
	})
	require.NoError(t, err)
	freshEnvelope, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       freshBare,
		ProviderID: 42,
	})
	require.NoError(t, err)

	clientPayload, err := json.Marshal(map[string]any{
		"model": "gpt-5.4",
		"input": []any{
			map[string]any{"type": "reasoning", "id": "rs_stale_env", "encrypted_content": staleEnvelope, "summary": []any{}},
			map[string]any{"type": "reasoning", "id": "rs_fresh_env", "encrypted_content": freshEnvelope},
		},
	})
	require.NoError(t, err)

	collected := CollectOpenAIEncryptedContentDigestsRaw(clientPayload)
	require.Equal(t, []string{
		OpenAIEncryptedContentDigest(staleBare),
		OpenAIEncryptedContentDigest(freshBare),
	}, collected)

	invalidFromBare := map[string]struct{}{
		OpenAIEncryptedContentDigest(staleBare): {},
	}
	require.True(t, OpenAIRawPayloadHasInvalidEncryptedContent(clientPayload, invalidFromBare))

	sanitizedClient, strippedCount, err := StripOpenAIInvalidEncryptedContentRaw(clientPayload, invalidFromBare)
	require.NoError(t, err)
	require.Equal(t, 1, strippedCount)

	var decodedClient map[string]any
	require.NoError(t, json.Unmarshal(sanitizedClient, &decodedClient))
	clientItems, ok := decodedClient["input"].([]any)
	require.True(t, ok)
	require.Len(t, clientItems, 2)
	firstClient, ok := clientItems[0].(map[string]any)
	require.True(t, ok)
	_, hasStaleEnc := firstClient["encrypted_content"]
	require.False(t, hasStaleEnc)
	secondClient, ok := clientItems[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, freshEnvelope, secondClient["encrypted_content"])

	invalidFromEnvelope := map[string]struct{}{
		OpenAIEncryptedContentDigest(staleEnvelope): {},
	}
	replayItems := []json.RawMessage{
		json.RawMessage(`{"type":"reasoning","id":"rs_bare","encrypted_content":"` + staleBare + `"}`),
		json.RawMessage(`{"type":"reasoning","id":"rs_keep","encrypted_content":"` + freshBare + `"}`),
	}
	nextReplay, replayStripped := StripOpenAIInvalidEncryptedContentFromReplayItems(replayItems, invalidFromEnvelope)
	require.Equal(t, 1, replayStripped)
	require.Len(t, nextReplay, 2)
	require.NotContains(t, string(nextReplay[0]), "encrypted_content")
	require.Contains(t, string(nextReplay[1]), freshBare)
}

// TestCollectOpenAIEncryptedContentDigestsRaw mixes supported encrypted items,
// plaintext input, missing ciphertext, and an unsupported message type. It
// checks digest order and that unsupported or absent input contributes no
// tracked ciphertext.
func TestCollectOpenAIEncryptedContentDigestsRaw(t *testing.T) {
	t.Parallel()

	digests := CollectOpenAIEncryptedContentDigestsRaw([]byte(`{"input":[
		{"type":"reasoning","encrypted_content":"cipher-a"},
		{"type":"compaction","encrypted_content":"cipher-b"},
		{"type":"input_text","text":"hello"},
		{"type":"reasoning"},
		{"type":"message","encrypted_content":"cipher-alien"}
	]}`))
	// Unsupported message items must not contribute digests that the sanitizer cannot remove.
	require.Equal(t, []string{
		OpenAIEncryptedContentDigest("cipher-a"),
		OpenAIEncryptedContentDigest("cipher-b"),
	}, digests)

	require.Nil(t, CollectOpenAIEncryptedContentDigestsRaw([]byte(`{"model":"gpt-5"}`)))
}

// TestStripOpenAIInvalidEncryptedContentItems supplies stale and fresh
// ciphertext in decoded histories. It protects reasoning-shell retention,
// encrypted compaction removal, fresh-item survival, and zero changes when no
// digest matches.
func TestStripOpenAIInvalidEncryptedContentItems(t *testing.T) {
	t.Parallel()

	invalid := map[string]struct{}{
		OpenAIEncryptedContentDigest("stale-cipher"): {},
	}

	t.Run("only_strips_matching_items", func(t *testing.T) {
		t.Parallel()
		reqBody := map[string]any{
			"input": []any{
				map[string]any{"type": "reasoning", "id": "rs_1", "encrypted_content": "stale-cipher", "summary": []any{}},
				map[string]any{"type": "reasoning", "id": "rs_2", "encrypted_content": "fresh-cipher"},
				map[string]any{"type": "input_text", "text": "hello"},
			},
		}
		stripped := StripOpenAIInvalidEncryptedContentItems(reqBody, invalid)
		require.Equal(t, 1, stripped)
		input, ok := reqBody["input"].([]any)
		require.True(t, ok)
		require.Len(t, input, 3)
		first, ok := input[0].(map[string]any)
		require.True(t, ok)
		_, hasEncrypted := first["encrypted_content"]
		require.False(t, hasEncrypted, "命中项应剥离 encrypted_content")
		require.Equal(t, "rs_1", first["id"], "reasoning 骨架应保留")
		second, ok := input[1].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "fresh-cipher", second["encrypted_content"], "新密文不得误删")
	})

	t.Run("compaction_removed_entirely", func(t *testing.T) {
		t.Parallel()
		reqBody := map[string]any{
			"input": []any{
				map[string]any{"type": "compaction", "encrypted_content": "stale-cipher"},
				map[string]any{"type": "input_text", "text": "hello"},
			},
		}
		stripped := StripOpenAIInvalidEncryptedContentItems(reqBody, invalid)
		require.Equal(t, 1, stripped)
		input, ok := reqBody["input"].([]any)
		require.True(t, ok)
		require.Len(t, input, 1)
		survivor, ok := input[0].(map[string]any)
		require.True(t, ok)
		require.Equal(t, "input_text", survivor["type"])
	})

	t.Run("no_hit_no_change", func(t *testing.T) {
		t.Parallel()
		reqBody := map[string]any{
			"input": []any{
				map[string]any{"type": "reasoning", "encrypted_content": "fresh-cipher"},
			},
		}
		require.Zero(t, StripOpenAIInvalidEncryptedContentItems(reqBody, invalid))
	})
}

// TestStripOpenAIInvalidEncryptedContentFromReplayItems sanitizes stale
// reasoning and compaction alongside unchanged raw input. It checks replacement
// content, removal counts, shared unmatched byte storage, immutable original
// bytes, and no-hit storage reuse.
func TestStripOpenAIInvalidEncryptedContentFromReplayItems(t *testing.T) {
	t.Parallel()

	invalid := map[string]struct{}{
		OpenAIEncryptedContentDigest("stale-cipher"): {},
	}

	t.Run("rewrites_hit_items_and_shares_the_rest", func(t *testing.T) {
		t.Parallel()
		original := json.RawMessage(`{"type":"reasoning","id":"rs_1","encrypted_content":"stale-cipher"}`)
		clean := json.RawMessage(`{"type":"input_text","text":"hello"}`)
		compaction := json.RawMessage(`{"type":"compaction","encrypted_content":"stale-cipher"}`)
		items := []json.RawMessage{original, clean, compaction}

		next, stripped := StripOpenAIInvalidEncryptedContentFromReplayItems(items, invalid)
		require.Equal(t, 2, stripped)
		require.Len(t, next, 2, "compaction 命中项应整项删除")
		var rewritten map[string]any
		require.NoError(t, json.Unmarshal(next[0], &rewritten))
		require.Equal(t, "rs_1", rewritten["id"])
		_, hasEncrypted := rewritten["encrypted_content"]
		require.False(t, hasEncrypted)
		// Unmatched items share their bodies; changed items receive new bodies.
		require.Same(t, &clean[0], &next[1][0])
		require.Contains(t, string(original), "stale-cipher")
	})

	t.Run("miss_returns_original_header", func(t *testing.T) {
		t.Parallel()
		items := []json.RawMessage{json.RawMessage(`{"type":"reasoning","encrypted_content":"fresh-cipher"}`)}
		next, stripped := StripOpenAIInvalidEncryptedContentFromReplayItems(items, invalid)
		require.Zero(t, stripped)
		require.Same(t, &items[0][0], &next[0][0])
	})
}

// TestStripOpenAIInvalidEncryptedContentRaw rewrites one stale item in a
// complete raw request with fresh history. It checks preserved model and input
// content plus reuse of the original payload storage on a no-hit path.
func TestStripOpenAIInvalidEncryptedContentRaw(t *testing.T) {
	t.Parallel()

	invalid := map[string]struct{}{
		OpenAIEncryptedContentDigest("stale-cipher"): {},
	}
	payload := []byte(`{"model":"gpt-5","input":[
		{"type":"reasoning","id":"rs_1","encrypted_content":"stale-cipher"},
		{"type":"reasoning","id":"rs_2","encrypted_content":"fresh-cipher"},
		{"type":"input_text","text":"hello"}
	],"stream":true}`)

	stripped, count, err := StripOpenAIInvalidEncryptedContentRaw(payload, invalid)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(stripped, &decoded))
	require.Equal(t, "gpt-5", decoded["model"])
	input, ok := decoded["input"].([]any)
	require.True(t, ok)
	require.Len(t, input, 3)
	first, ok := input[0].(map[string]any)
	require.True(t, ok)
	_, hasEncrypted := first["encrypted_content"]
	require.False(t, hasEncrypted)
	second, ok := input[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "fresh-cipher", second["encrypted_content"])

	t.Run("miss_fast_path_returns_original", func(t *testing.T) {
		t.Parallel()
		cleanPayload := []byte(`{"input":[{"type":"reasoning","encrypted_content":"fresh-cipher"}]}`)
		unchanged, count, err := StripOpenAIInvalidEncryptedContentRaw(cleanPayload, invalid)
		require.NoError(t, err)
		require.Zero(t, count)
		require.Same(t, &cleanPayload[0], &unchanged[0], "未命中时应原样返回，不重编码")
	})
}
