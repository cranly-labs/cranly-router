package gemini

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// TestReplaceThoughtSignaturesRecursive_OnlyReplacesTargetField supplies
// replay and generic signature fields at the root and in nested array objects.
// Replacement must target thoughtSignature at both levels while retaining
// the neighboring signature values.
func TestReplaceThoughtSignaturesRecursive_OnlyReplacesTargetField(t *testing.T) {
	input := map[string]any{
		"thoughtSignature": "sig_root",
		"signature":        "keep_signature",
		"nested": []any{
			map[string]any{
				"thoughtSignature": "sig_nested",
				"signature":        "keep_nested_signature",
			},
		},
	}

	got, ok := replaceThoughtSignaturesRecursive(input, DummyThoughtSignature).(map[string]any)
	require.True(t, ok)
	require.Equal(t, DummyThoughtSignature, got["thoughtSignature"])
	require.Equal(t, "keep_signature", got["signature"])

	nested, ok := got["nested"].([]any)
	require.True(t, ok)
	nestedMap, ok := nested[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, DummyThoughtSignature, nestedMap["thoughtSignature"])
	require.Equal(t, "keep_nested_signature", nestedMap["signature"])
}

// TestNativeEnsureGeminiFunctionCallThoughtSignatures_FillsBlankCallPartsOnly
// supplies missing, whitespace-only, and populated functionCall signatures plus
// plain text parts and non-call payloads. Only blank functionCall parts gain
// DummyThoughtSignature while signed calls and non-call bodies stay untouched.
func TestNativeEnsureGeminiFunctionCallThoughtSignatures_FillsBlankCallPartsOnly(t *testing.T) {
	noCall := []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"id":9007199254740993}`)
	require.Equal(t, noCall, NativeEnsureGeminiFunctionCallThoughtSignatures(DummyThoughtSignature, noCall))

	malformed := []byte(`{"contents":[{"parts":[{"functionCall":`)
	require.Equal(t, malformed, NativeEnsureGeminiFunctionCallThoughtSignatures(DummyThoughtSignature, malformed))

	alreadySigned := []byte(`{"contents":[{"role":"model","parts":[{"functionCall":{"name":"read"},"thoughtSignature":"keep_sig"}]}]}`)
	require.Equal(t, alreadySigned, NativeEnsureGeminiFunctionCallThoughtSignatures(DummyThoughtSignature, alreadySigned))

	mixed := []byte(`{"contents":[{"role":"model","parts":[` +
		`{"text":"thinking"},` +
		`{"functionCall":{"name":"first","args":{}}},` +
		`{"functionCall":{"name":"second","args":{}},"thoughtSignature":"   "},` +
		`{"functionCall":{"name":"third","args":{}},"thoughtSignature":"valid_sig"}` +
		`]}]}`)
	patched := NativeEnsureGeminiFunctionCallThoughtSignatures(DummyThoughtSignature, mixed)
	require.False(t, gjson.GetBytes(patched, "contents.0.parts.0.thoughtSignature").Exists())
	require.Equal(t, DummyThoughtSignature, gjson.GetBytes(patched, "contents.0.parts.1.thoughtSignature").String())
	require.Equal(t, DummyThoughtSignature, gjson.GetBytes(patched, "contents.0.parts.2.thoughtSignature").String())
	require.Equal(t, "valid_sig", gjson.GetBytes(patched, "contents.0.parts.3.thoughtSignature").String())
}
