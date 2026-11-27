package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateResponsesRequest feeds valid OpenAI Responses requests alongside
// unlowered custom tools, dangling reasoning items, and unpaired function_call
// items. Standard requests must pass, while function-only dialect, follower,
// and call-pairing violations must report their respective rule IDs.
func TestValidateResponsesRequest(t *testing.T) {
	valid := []byte(`{
		"model": "gpt-5",
		"tools": [
			{"type": "function", "name": "run_shell", "parameters": {"type": "object"}},
			{"type": "custom", "name": "apply_patch"}
		],
		"input": [
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "patch"}]},
			{"type": "reasoning", "id": "rs_1", "summary": [], "encrypted_content": "enc_1"},
			{"type": "custom_tool_call", "call_id": "call_1", "name": "apply_patch", "input": "*** Begin Patch"},
			{"type": "custom_tool_call_output", "call_id": "call_1", "output": "applied"}
		]
	}`)
	require.Empty(t, ValidateResponsesRequest(valid, RequestValidationOptions{}))

	// Function-only dialect rejects unlowered custom tools and custom_tool_call items.
	v := ValidateResponsesRequest(valid, RequestValidationOptions{DialectRules: []string{"function-only"}})
	require.NotEmpty(t, v)
	require.Equal(t, "RSP-DIALECT-FUNCTION-ONLY", v[0].RuleID)

	// Dangling reasoning item followed directly by user message violates RSP-REASONING-FOLLOWER.
	danglingReasoning := []byte(`{
		"model": "gpt-5",
		"input": [
			{"type": "reasoning", "id": "rs_1", "summary": [{"type": "summary_text", "text": "orphan"}]},
			{"type": "message", "role": "user", "content": "next"}
		]
	}`)
	v = ValidateResponsesRequest(danglingReasoning, RequestValidationOptions{})
	require.Len(t, v, 1)
	require.Equal(t, "RSP-REASONING-FOLLOWER", v[0].RuleID)

	// Unmatched function_call without function_call_output violates RSP-CALL-PAIRING.
	unpaired := []byte(`{
		"model": "gpt-5",
		"input": [
			{"type": "function_call", "call_id": "call_orphan", "name": "run_shell", "arguments": "{}"}
		]
	}`)
	v = ValidateResponsesRequest(unpaired, RequestValidationOptions{})
	require.Len(t, v, 1)
	require.Equal(t, "RSP-CALL-PAIRING", v[0].RuleID)
}
