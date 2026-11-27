package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateMessagesRequest feeds valid Anthropic Messages requests alongside
// misordered tool_result blocks, unsigned thinking blocks, and unknown root
// fields. Valid requests and passback unsigned thinking must pass, while
// ordering and strict top-level violations must report their rule IDs.
func TestValidateMessagesRequest(t *testing.T) {
	require.NotEmpty(t, RegisteredRequestRules())

	valid := []byte(`{
		"model": "claude-sonnet-4-5",
		"max_tokens": 4096,
		"thinking": {"type": "enabled", "budget_tokens": 1024},
		"tools": [{"name": "read_file", "input_schema": {"type": "object"}}],
		"messages": [
			{"role": "user", "content": "Read go.mod"},
			{"role": "assistant", "content": [
				{"type": "thinking", "thinking": "Checking file", "signature": "sig_valid"},
				{"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {"path": "go.mod"}}
			]},
			{"role": "user", "content": [
				{"type": "tool_result", "tool_use_id": "toolu_1", "content": "module example"},
				{"type": "text", "text": "Now summarize."}
			]}
		]
	}`)
	require.Empty(t, ValidateMessagesRequest(valid, RequestValidationOptions{StrictTopLevel: true}))

	// tool_result placed after text in user message violates MSG-TOOL-RESULT-ORDER.
	badOrder := []byte(`{
		"model": "claude-sonnet-4-5",
		"max_tokens": 1024,
		"messages": [
			{"role": "user", "content": "Read"},
			{"role": "assistant", "content": [{"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {}}]},
			{"role": "user", "content": [
				{"type": "text", "text": "First text"},
				{"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"}
			]}
		]
	}`)
	v := ValidateMessagesRequest(badOrder, RequestValidationOptions{})
	require.NotEmpty(t, v)
	require.Equal(t, "MSG-TOOL-RESULT-ORDER", v[0].RuleID)

	// Unsigned thinking block fails standard Messages but passes passback dialect.
	unsignedThinking := []byte(`{
		"model": "deepseek-reasoner",
		"max_tokens": 1024,
		"messages": [
			{"role": "user", "content": "Read"},
			{"role": "assistant", "content": [
				{"type": "thinking", "thinking": "step 1"},
				{"type": "tool_use", "id": "toolu_1", "name": "read_file", "input": {}}
			]},
			{"role": "user", "content": [{"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"}]}
		]
	}`)
	require.NotEmpty(t, ValidateMessagesRequest(unsignedThinking, RequestValidationOptions{}))
	require.Empty(t, ValidateMessagesRequest(unsignedThinking, RequestValidationOptions{DialectRules: []string{"passback"}}))

	// Top-level cache_control is a valid Anthropic Messages field under StrictTopLevel.
	withTopCacheControl := []byte(`{"model":"claude-sonnet-4-5","max_tokens":100,"cache_control":{"type":"ephemeral"},"messages":[{"role":"user","content":"hi"}]}`)
	require.Empty(t, ValidateMessagesRequest(withTopCacheControl, RequestValidationOptions{StrictTopLevel: true}))

	// Unknown top-level fields are rejected in deterministic wire scan order under StrictTopLevel.
	extraTop := []byte(`{"zeta_extra":1,"model":"claude-sonnet-4-5","max_tokens":100,"alpha_extra":true,"messages":[{"role":"user","content":"hi"}]}`)
	v = ValidateMessagesRequest(extraTop, RequestValidationOptions{StrictTopLevel: true})
	require.Len(t, v, 2)
	require.Equal(t, "MSG-STRICT-TOP-LEVEL", v[0].RuleID)
	require.Equal(t, "$.zeta_extra", v[0].Path)
	require.Equal(t, "$.alpha_extra", v[1].Path)

	// output_config.effort accepts documented Anthropic tiers and rejects "none".
	validEffort := []byte(`{"model":"claude-sonnet-4-5","max_tokens":100,"output_config":{"effort":"high"},"messages":[{"role":"user","content":"hi"}]}`)
	require.Empty(t, ValidateMessagesRequest(validEffort, RequestValidationOptions{StrictTopLevel: true}))

	noneEffort := []byte(`{"model":"claude-sonnet-4-5","max_tokens":100,"output_config":{"effort":"none"},"messages":[{"role":"user","content":"hi"}]}`)
	v = ValidateMessagesRequest(noneEffort, RequestValidationOptions{StrictTopLevel: true})
	require.Len(t, v, 1)
	require.Equal(t, "MSG-THINKING", v[0].RuleID)
	require.Equal(t, "$.output_config.effort", v[0].Path)
}
