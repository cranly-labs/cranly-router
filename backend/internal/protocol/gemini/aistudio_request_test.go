package gemini

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// TestNativeNormalizeGeminiRequestForAIStudio_RenamesGoogleSearchAndPreservesUntouched
// passes malformed JSON, function-only declarations, dual-key search entries,
// and camelCase googleSearch tools to AI Studio request normalization. Only
// standalone googleSearch entries must rename to google_search while untouched
// inputs keep their original byte slices.
func TestNativeNormalizeGeminiRequestForAIStudio_RenamesGoogleSearchAndPreservesUntouched(t *testing.T) {
	malformed := []byte(`{"tools":[`)
	require.Equal(t, malformed, NativeNormalizeGeminiRequestForAIStudio(malformed))

	functionsOnly := []byte(`{"tools":[{"functionDeclarations":[{"name":"read"}]}],"extra":9007199254740993}`)
	require.Equal(t, functionsOnly, NativeNormalizeGeminiRequestForAIStudio(functionsOnly))

	bothPresent := []byte(`{"tools":[{"googleSearch":{"a":1},"google_search":{"b":2}}]}`)
	require.Equal(t, bothPresent, NativeNormalizeGeminiRequestForAIStudio(bothPresent))

	camelOnly := []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}],"tools":[{"functionDeclarations":[{"name":"read"}]},{"googleSearch":{}}]}`)
	normalized := NativeNormalizeGeminiRequestForAIStudio(camelOnly)
	require.False(t, gjson.GetBytes(normalized, "tools.1.googleSearch").Exists())
	require.True(t, gjson.GetBytes(normalized, "tools.1.google_search").IsObject())
	require.Equal(t, "read", gjson.GetBytes(normalized, "tools.0.functionDeclarations.0.name").String())
}
