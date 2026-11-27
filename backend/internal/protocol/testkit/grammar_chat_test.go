package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestValidateChatStream feeds valid Chat Completions SSE chunks alongside
// empty follow-up tool names (091ec1f41) and truncated streams (bd39eb589).
// Valid streams must pass with zero violations, while empty tool names and
// silent EOF cuts must fail validation.
func TestValidateChatStream(t *testing.T) {
	valid := []sse.Frame{
		{Data: []byte(`{"id":"chatcmpl_1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"lookup","arguments":""}}]}}]}`)},
		{Data: []byte(`{"id":"chatcmpl_1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"q\":\"go\"}"}}]}}]}`)},
		{Data: []byte(`{"id":"chatcmpl_1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`)},
		{Data: []byte(`{"id":"chatcmpl_1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[],"usage":{"prompt_tokens":10,"completion_tokens":4,"total_tokens":14}}`)},
		{Data: []byte("[DONE]")},
	}
	require.Empty(t, ValidateChatStream(valid, GrammarOptions{}))

	// Follow-up delta with empty function.name (091ec1f41) must fail.
	emptyName := append([]sse.Frame(nil), valid...)
	emptyName[1] = sse.Frame{Data: []byte(`{"id":"chatcmpl_1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"name":"","arguments":"{}"}}]}}]}`)}
	v := ValidateChatStream(emptyName, GrammarOptions{})
	require.NotEmpty(t, v)
	require.Equal(t, "empty_tool_name_delta", v[0].Rule)

	// Silent truncation without finish_reason or [DONE] (bd39eb589) must fail in both modes.
	require.NotEmpty(t, ValidateChatStream(valid[:2], GrammarOptions{AllowAbnormal: false}))
	require.NotEmpty(t, ValidateChatStream(valid[:2], GrammarOptions{AllowAbnormal: true}))

	// Truncated tool arguments followed by an error frame pass AllowAbnormal,
	// whereas claiming normal finish_reason+[DONE] with truncated JSON fails even under AllowAbnormal.
	truncatedDelta := sse.Frame{Data: []byte(`{"id":"chatcmpl_1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"q\":\"go"}}]}}]}`)}
	abortedWithErr := []sse.Frame{
		valid[0],
		truncatedDelta,
		{Data: []byte(`{"error":{"type":"upstream_error","message":"stream cut"}}`)},
	}
	require.Empty(t, ValidateChatStream(abortedWithErr, GrammarOptions{AllowAbnormal: true}))

	fakeCompleted := []sse.Frame{
		valid[0],
		truncatedDelta,
		valid[2],
		valid[4],
	}
	v = ValidateChatStream(fakeCompleted, GrammarOptions{AllowAbnormal: true})
	require.Len(t, v, 1)
	require.Equal(t, "invalid_tool_arguments_json", v[0].Rule)
}
