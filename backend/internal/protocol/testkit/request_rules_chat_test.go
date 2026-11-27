package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateChatRequest feeds Chat Completions tool-call conversations with
// and without assistant reasoning_content and with missing tool results.
// Standard Chat accepts omitted reasoning_content, whereas deepseek-thinking
// and missing tool results must report CHAT-DEEPSEEK-THINKING and CHAT-TOOL-PAIRING.
func TestValidateChatRequest(t *testing.T) {
	withoutReasoning := []byte(`{
		"model": "deepseek-reasoner",
		"tools": [{"type": "function", "function": {"name": "get_time"}}],
		"messages": [
			{"role": "user", "content": "What time is it?"},
			{"role": "assistant", "content": null, "tool_calls": [
				{"id": "call_1", "type": "function", "function": {"name": "get_time", "arguments": "{}"}}
			]},
			{"role": "tool", "tool_call_id": "call_1", "content": "12:00"}
		]
	}`)
	require.Empty(t, ValidateChatRequest(withoutReasoning, RequestValidationOptions{}))

	// deepseek-thinking dialect rejects assistant tool_calls lacking reasoning_content.
	v := ValidateChatRequest(withoutReasoning, RequestValidationOptions{DialectRules: []string{"deepseek-thinking"}})
	require.Len(t, v, 1)
	require.Equal(t, "CHAT-DEEPSEEK-THINKING", v[0].RuleID)

	withReasoning := []byte(`{
		"model": "deepseek-reasoner",
		"messages": [
			{"role": "user", "content": "What time is it?"},
			{"role": "assistant", "content": "", "reasoning_content": "Need clock", "tool_calls": [
				{"id": "call_1", "type": "function", "function": {"name": "get_time", "arguments": "{}"}}
			]},
			{"role": "tool", "tool_call_id": "call_1", "content": "12:00"}
		]
	}`)
	require.Empty(t, ValidateChatRequest(withReasoning, RequestValidationOptions{DialectRules: []string{"deepseek-thinking"}}))

	// Missing tool result before next user message violates CHAT-TOOL-PAIRING.
	missingResult := []byte(`{
		"model": "gpt-4o",
		"messages": [
			{"role": "user", "content": "Run"},
			{"role": "assistant", "tool_calls": [
				{"id": "call_1", "type": "function", "function": {"name": "fn", "arguments": "{}"}},
				{"id": "call_2", "type": "function", "function": {"name": "fn", "arguments": "{}"}}
			]},
			{"role": "tool", "tool_call_id": "call_1", "content": "ok"},
			{"role": "user", "content": "continue"}
		]
	}`)
	v = ValidateChatRequest(missingResult, RequestValidationOptions{})
	require.Len(t, v, 1)
	require.Equal(t, "CHAT-TOOL-PAIRING", v[0].RuleID)

	// deepseek-thinking dialect rejects reasoning_content on non-tool assistant turns.
	nonToolReasoning := []byte(`{
		"model": "deepseek-reasoner",
		"messages": [
			{"role": "user", "content": "Hello"},
			{"role": "assistant", "content": "Hi there", "reasoning_content": "Prior completed turn reasoning"},
			{"role": "user", "content": "Follow up"}
		]
	}`)
	require.Empty(t, ValidateChatRequest(nonToolReasoning, RequestValidationOptions{}))
	v = ValidateChatRequest(nonToolReasoning, RequestValidationOptions{DialectRules: []string{"deepseek-thinking"}})
	require.Len(t, v, 1)
	require.Equal(t, "CHAT-DEEPSEEK-THINKING", v[0].RuleID)

	// deepseek-thinking dialect rejects split active-turn assistant commentary ahead of tool_calls.
	splitActiveAssistant := []byte(`{
		"model": "deepseek-reasoner",
		"messages": [
			{"role": "user", "content": "Write result.txt"},
			{"role": "assistant", "content": "I will run the shell command now."},
			{"role": "assistant", "content": null, "reasoning_content": "", "tool_calls": [
				{"id": "call_1", "type": "function", "function": {"name": "exec_command", "arguments": "{\"cmd\":\"echo ok\"}"}}
			]},
			{"role": "tool", "tool_call_id": "call_1", "content": "ok"}
		]
	}`)
	require.Empty(t, ValidateChatRequest(splitActiveAssistant, RequestValidationOptions{}))
	v = ValidateChatRequest(splitActiveAssistant, RequestValidationOptions{DialectRules: []string{"deepseek-thinking"}})
	require.Len(t, v, 1)
	require.Equal(t, "CHAT-DEEPSEEK-THINKING", v[0].RuleID)
}
