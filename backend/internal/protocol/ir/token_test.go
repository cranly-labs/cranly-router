package ir

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFormatAndParseCarrierRoundTripAllSchemes formats and parses valid trc1
// carrier envelopes across all six supported continuity schemes, including
// optional account-bound ProviderID p, redacted flags, and invalid UTF-8 text.
// It verifies unpadded base64url encoding, exact d/t/r/p field order, and
// scheme classification predicates.
func TestFormatAndParseCarrierRoundTripAllSchemes(t *testing.T) {
	for _, s := range []string{
		SchemeAnthropic, SchemeAntigravityClaude, SchemeOpenAI,
		SchemeXAI, SchemeGemini, SchemePlain,
	} {
		require.True(t, IsCarrierScheme(s))
	}
	require.False(t, IsCarrierScheme(SchemeUnknown))
	require.False(t, IsCarrierScheme(""))

	require.True(t, IsAccountBoundScheme(SchemeOpenAI))
	require.True(t, IsAccountBoundScheme(SchemeXAI))
	require.False(t, IsAccountBoundScheme(SchemeAnthropic))

	require.True(t, IsTextCarryingScheme(SchemeAnthropic))
	require.True(t, IsTextCarryingScheme(SchemeAntigravityClaude))
	require.True(t, IsTextCarryingScheme(SchemePlain))
	require.False(t, IsTextCarryingScheme(SchemeOpenAI))

	cases := []struct {
		name        string   // test case label
		carrier     Carrier  // input Carrier value
		wantJSON    string   // expected inner JSON field ordering
		wantDecoded *Carrier // expected parsed Carrier when invalid UTF-8 is normalized
	}{
		{
			name: "anthropic thinking with text",
			carrier: Carrier{
				Scheme: SchemeAnthropic,
				Data:   "EqQBCgIYAiJA_sig_01",
				Text:   "First check replica lag.",
			},
			wantJSON: `{"d":"EqQBCgIYAiJA_sig_01","t":"First check replica lag."}`,
		},
		{
			name: "anthropic redacted thinking",
			carrier: Carrier{
				Scheme:   SchemeAnthropic,
				Data:     "redacted_blob_01",
				Redacted: true,
			},
			wantJSON: `{"d":"redacted_blob_01","r":true}`,
		},
		{
			name: "antigravity_claude thinking",
			carrier: Carrier{
				Scheme: SchemeAntigravityClaude,
				Data:   "ag_claude_sig_02",
				Text:   "Verify quorum health.",
			},
			wantJSON: `{"d":"ag_claude_sig_02","t":"Verify quorum health."}`,
		},
		{
			name: "openai without provider_id",
			carrier: Carrier{
				Scheme: SchemeOpenAI,
				Data:   "gAAAAABnOpenAIUnbound",
			},
			wantJSON: `{"d":"gAAAAABnOpenAIUnbound"}`,
		},
		{
			name: "openai with positive provider_id",
			carrier: Carrier{
				Scheme:     SchemeOpenAI,
				Data:       "gAAAAABnRoundtripOpenAI02",
				ProviderID: 101,
			},
			wantJSON: `{"d":"gAAAAABnRoundtripOpenAI02","p":101}`,
		},
		{
			name: "xai redacted with positive provider_id",
			carrier: Carrier{
				Scheme:     SchemeXAI,
				Data:       "enc_xai_native_redacted_01",
				Redacted:   true,
				ProviderID: 202,
			},
			wantJSON: `{"d":"enc_xai_native_redacted_01","r":true,"p":202}`,
		},
		{
			name: "gemini thought signature",
			carrier: Carrier{
				Scheme: SchemeGemini,
				Data:   "CpEBCocB_gem_roundtrip_sig_02",
			},
			wantJSON: `{"d":"CpEBCocB_gem_roundtrip_sig_02"}`,
		},
		{
			name: "plain tokenless reasoning",
			carrier: Carrier{
				Scheme: SchemePlain,
				Text:   "Tokenless reasoning content.",
			},
			wantJSON: `{"t":"Tokenless reasoning content."}`,
		},
		{
			name: "plain carrier with invalid UTF-8 bytes",
			carrier: Carrier{
				Scheme: SchemePlain,
				Text:   "prefix-\xff\xfe-suffix",
			},
			wantDecoded: &Carrier{
				Scheme: SchemePlain,
				Text:   "prefix-\ufffd\ufffd-suffix",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := FormatCarrier(tc.carrier)
			require.NoError(t, err)
			require.True(t, strings.HasPrefix(encoded, CarrierPrefix+tc.carrier.Scheme+":"))
			require.NotContains(t, encoded, "=")

			if tc.wantJSON != "" {
				parts := strings.Split(encoded, ":")
				require.Len(t, parts, 3)
				rawJSON, decErr := base64.RawURLEncoding.DecodeString(parts[2])
				require.NoError(t, decErr)
				require.Equal(t, tc.wantJSON, string(rawJSON))
			}

			got, ok := ParseCarrier(encoded)
			require.True(t, ok)
			want := tc.carrier
			if tc.wantDecoded != nil {
				want = *tc.wantDecoded
			}
			require.Equal(t, want, got)
		})
	}
}

// TestFormatCarrierRejectsInvalidCombinations passes unsupported schemes and
// forbidden combinations of Data, Text, Redacted, and ProviderID to FormatCarrier.
// Every invalid combination must return an error instead of producing a malformed envelope.
func TestFormatCarrierRejectsInvalidCombinations(t *testing.T) {
	bad := []struct {
		name    string  // invalid case description
		carrier Carrier // rejected input Carrier
	}{
		{"empty scheme", Carrier{Data: "x"}},
		{"unknown scheme", Carrier{Scheme: SchemeUnknown, Data: "x"}},
		{"plain with data", Carrier{Scheme: SchemePlain, Data: "x", Text: "t"}},
		{"plain with empty text", Carrier{Scheme: SchemePlain}},
		{"plain with redacted", Carrier{Scheme: SchemePlain, Text: "t", Redacted: true}},
		{"plain with provider_id", Carrier{Scheme: SchemePlain, Text: "t", ProviderID: 1}},
		{"anthropic empty data", Carrier{Scheme: SchemeAnthropic, Text: "t"}},
		{"anthropic provider_id", Carrier{Scheme: SchemeAnthropic, Data: "d", Text: "t", ProviderID: 1}},
		{"anthropic redacted with text", Carrier{Scheme: SchemeAnthropic, Data: "d", Text: "t", Redacted: true}},
		{"antigravity_claude provider_id", Carrier{Scheme: SchemeAntigravityClaude, Data: "d", ProviderID: 5}},
		{"openai empty data", Carrier{Scheme: SchemeOpenAI}},
		{"openai with text", Carrier{Scheme: SchemeOpenAI, Data: "d", Text: "t"}},
		{"openai negative provider_id", Carrier{Scheme: SchemeOpenAI, Data: "d", ProviderID: -1}},
		{"xai with text", Carrier{Scheme: SchemeXAI, Data: "d", Text: "t"}},
		{"gemini empty data", Carrier{Scheme: SchemeGemini}},
		{"gemini with text", Carrier{Scheme: SchemeGemini, Data: "d", Text: "t"}},
		{"gemini redacted", Carrier{Scheme: SchemeGemini, Data: "d", Redacted: true}},
		{"gemini provider_id", Carrier{Scheme: SchemeGemini, Data: "d", ProviderID: 3}},
	}

	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := FormatCarrier(tc.carrier)
			require.Error(t, err)
		})
	}
}

// TestParseCarrierRejectsBareTokensAndMalformedPayloads feeds bare provider
// tokens (including "gAAAA..." ciphertexts), padded or non-canonical base64url
// strings, duplicate or unknown JSON keys, and nonpositive p fields to ParseCarrier.
// Every malformed or bare input must return false and a zero Carrier value.
func TestParseCarrierRejectsBareTokensAndMalformedPayloads(t *testing.T) {
	encodeRaw := func(scheme, rawJSON string) string {
		return CarrierPrefix + scheme + ":" + base64.RawURLEncoding.EncodeToString([]byte(rawJSON))
	}

	// Build a non-canonical 2-char base64url payload with non-zero trailing pad bits ("AB" vs "AA").
	nonCanonical := "trc1:openai:eyJkIjoiYSJ9"
	validJSONBytes, err := base64.RawURLEncoding.DecodeString("eyJkIjoiYSJ9")
	require.NoError(t, err)
	require.Equal(t, `{"d":"a"}`, string(validJSONBytes))
	// 10-byte JSON {"d":"aa"} encodes to 14 chars (4 trailing pad bits); flip last char '0' -> '1'.
	tenByteB64 := base64.RawURLEncoding.EncodeToString([]byte(`{"d":"aa"}`))
	nonCanonicalPadBits := CarrierPrefix + "openai:" + tenByteB64[:len(tenByteB64)-1] + "B"
	_ = nonCanonical

	inputs := []struct {
		name string // malformed or bare token case label
		raw  string // candidate wire string
	}{
		{"empty string", ""},
		{"bare gAAAA OpenAI ciphertext", "gAAAAABnLegacyOpenAICiphertextToken0001"},
		{"bare Anthropic signature", "EqQBCgIYAiJAbGVnYWN5X3NpZ25hdHVyZQ=="},
		{"bare Gemini thoughtSignature", "CpEBCocB_gemini_bare_signature"},
		{"plain prose mentioning trc1", "note: trc1:plain:example"},
		{"wrong version prefix", "trc2:openai:eyJkIjoiYSJ9"},
		{"missing payload separator", "trc1:openai"},
		{"empty payload", "trc1:openai:"},
		{"extra colon segment", "trc1:openai:eyJkIjoiYSJ9:extra"},
		{"unknown scheme", encodeRaw("unknown", `{"d":"a"}`)},
		{"unrecognized vendor scheme", encodeRaw("bedrock", `{"d":"a"}`)},
		{"padded base64url", "trc1:plain:" + base64.URLEncoding.EncodeToString([]byte(`{"t":"hi"}`))},
		{"standard base64 alphabet", "trc1:plain:eyJ0Ijoi+/8ifQ"},
		{"non-canonical base64url pad bits", nonCanonicalPadBits},
		{"non-JSON payload", encodeRaw("openai", "not-json")},
		{"JSON array payload", encodeRaw("openai", `["a"]`)},
		{"JSON null payload", encodeRaw("openai", `null`)},
		{"unknown JSON member", encodeRaw("openai", `{"d":"a","extra":1}`)},
		{"duplicate JSON key", encodeRaw("openai", `{"d":"a","d":"b"}`)},
		{"trailing JSON token", encodeRaw("openai", `{"d":"a"}{}`)},
		{"non-string d field", encodeRaw("openai", `{"d":123}`)},
		{"non-string t field", encodeRaw("plain", `{"t":true}`)},
		{"non-bool r field", encodeRaw("anthropic", `{"d":"a","r":"true"}`)},
		{"string p field", encodeRaw("openai", `{"d":"a","p":"10"}`)},
		{"fractional p field", encodeRaw("openai", `{"d":"a","p":1.5}`)},
		{"zero p field", encodeRaw("openai", `{"d":"a","p":0}`)},
		{"negative p field", encodeRaw("openai", `{"d":"a","p":-7}`)},
		{"null p field", encodeRaw("openai", `{"d":"a","p":null}`)},
		{"empty plain object", encodeRaw("plain", `{}`)},
		{"plain carrying d", encodeRaw("plain", `{"d":"sig","t":"hi"}`)},
		{"openai carrying t", encodeRaw("openai", `{"d":"enc","t":"hi"}`)},
		{"gemini carrying p", encodeRaw("gemini", `{"d":"sig","p":9}`)},
	}

	for _, tc := range inputs {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := ParseCarrier(tc.raw)
			require.False(t, ok, "expected %q to be rejected, got %+v", tc.raw, got)
			require.Equal(t, Carrier{}, got)
		})
	}
}

// FuzzParseCarrier feeds arbitrary strings into ParseCarrier across seed and
// mutated inputs. Any string accepted by ParseCarrier must round-trip through
// FormatCarrier and ParseCarrier to an identical Carrier value without panicking.
func FuzzParseCarrier(f *testing.F) {
	seeds := []string{
		"",
		"gAAAAABnOpenAIBareToken",
		"trc1:plain:eyJ0IjoiaGVsbG8ifQ",
		"trc1:openai:eyJkIjoiZ0FBQUEiLCJwIjoxMDF9",
		"trc1:anthropic:eyJkIjoic2lnIiwidCI6InRoaW5rIn0",
		"trc1:xai:eyJkIjoiZW5jIiwiciI6dHJ1ZSwicCI6MjAyfQ",
		"trc1:unknown:eyJkIjoiYSJ9",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		parsed, ok := ParseCarrier(raw)
		if !ok {
			return
		}
		reformatted, err := FormatCarrier(parsed)
		require.NoError(t, err)
		reparsed, ok2 := ParseCarrier(reformatted)
		require.True(t, ok2)
		require.Equal(t, parsed, reparsed)
	})
}
