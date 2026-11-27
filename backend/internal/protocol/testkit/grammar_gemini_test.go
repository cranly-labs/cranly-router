package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestValidateGeminiStream feeds valid Gemini GenerateContent SSE frames with a
// trailing usageMetadata tail alongside string-typed functionCall.args and
// error frames. Valid streams and AllowAbnormal error frames must pass, while
// non-object functionCall.args must report non_object_function_call_args.
func TestValidateGeminiStream(t *testing.T) {
	valid := []sse.Frame{
		{Data: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"thinking...","thought":true,"thoughtSignature":"sig_1"}]}}]}`)},
		{Data: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"get_weather","args":{"city":"Tokyo"}},"thoughtSignature":"sig_call"}]},"finishReason":"STOP"}]}`)},
		{Data: []byte(`{"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":6}}`)},
	}
	require.Empty(t, ValidateGeminiStream(valid, GrammarOptions{}))

	// String-encoded functionCall.args instead of object must fail.
	badArgs := []sse.Frame{
		{Data: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"get_weather","args":"{}"}}]},"finishReason":"STOP"}]}`)},
	}
	v := ValidateGeminiStream(badArgs, GrammarOptions{})
	require.NotEmpty(t, v)
	require.Equal(t, "non_object_function_call_args", v[0].Rule)

	// Error frame passes under AllowAbnormal and fails normal mode.
	errFrames := []sse.Frame{
		valid[0],
		{Data: []byte(`{"error":{"code":503,"message":"unavailable"}}`)},
	}
	require.NotEmpty(t, ValidateGeminiStream(errFrames, GrammarOptions{AllowAbnormal: false}))
	require.Empty(t, ValidateGeminiStream(errFrames, GrammarOptions{AllowAbnormal: true}))
}
