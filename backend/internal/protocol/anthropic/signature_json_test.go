package anthropic

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// TestStripMessagesSignatureBlocksForRetryBytes_StandardClaudeToolLoopAndPlaceholders
// supplies an active tool-use loop with signed thinking, redacted_thinking, an
// emptied assistant turn, clear_thinking_20251015, and 64-bit integer extras.
// Standard retry stripping must drop historical reasoning blocks, fill emptied
// turns, remove top-level thinking and clear-thinking edits, and preserve large
// integer literals and unknown members verbatim.
func TestStripMessagesSignatureBlocksForRetryBytes_StandardClaudeToolLoopAndPlaceholders(t *testing.T) {
	body := []byte(`{"model":"claude-opus-4-6","max_tokens":4096,"thinking":{"type":"enabled","budget_tokens":2048},"context_management":{"edits":[{"type":"clear_thinking_20251015","keep":"all"}]},"custom_extra":{"seq":9007199254740993},"messages":[{"role":"user","content":"step 1"},{"role":"assistant","content":[{"type":"thinking","thinking":"old sonnet thought","signature":"EqQBSonnetSig"}]},{"role":"user","content":"step 2"},{"role":"assistant","content":[{"type":"thinking","thinking":"tool plan","signature":"EqQBSonnetSig2"},{"type":"redacted_thinking","data":"EmwKRedacted"},{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"id":9007199254740995}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"done"}]}]}`)

	patched, changed, losses := StripMessagesSignatureBlocksWithLosses(body, false)
	require.Equal(t, SignatureRetryLosses{Tokens: 3, Reasoning: 2}, losses)
	require.True(t, changed)
	require.False(t, gjson.GetBytes(patched, "thinking").Exists(), "active tool loop without leading thinking must omit top-level thinking")
	require.False(t, gjson.GetBytes(patched, "context_management").Exists(), "dependent clear_thinking_20251015 must be removed with thinking")
	require.Equal(t, "9007199254740993", gjson.GetBytes(patched, "custom_extra.seq").Raw)
	require.Equal(t, "(assistant content removed)", gjson.GetBytes(patched, "messages.1.content.0.text").String())
	require.Equal(t, "tool_use", gjson.GetBytes(patched, "messages.3.content.0.type").String())
	require.Equal(t, "9007199254740995", gjson.GetBytes(patched, "messages.3.content.0.input.id").Raw)

	// Fresh trailing user text turns retain top-level thinking on the new turn
	// while stripping rejected historical signatures from prior assistant turns.
	freshTurn := []byte(`{"model":"claude-opus-4-6","max_tokens":4096,"thinking":{"type":"adaptive"},"messages":[{"role":"user","content":"q1"},{"role":"assistant","content":[{"type":"thinking","thinking":"prior","signature":"EqQBOld"},{"type":"text","text":"a1"}]},{"role":"user","content":"q2"}]}`)
	patchedFresh, changedFresh := StripMessagesSignatureBlocksForRetryBytes(freshTurn, false)
	require.True(t, changedFresh)
	require.Equal(t, "adaptive", gjson.GetBytes(patchedFresh, "thinking.type").String())
	require.Equal(t, 1, len(gjson.GetBytes(patchedFresh, "messages.1.content").Array()))
	require.Equal(t, "text", gjson.GetBytes(patchedFresh, "messages.1.content.0.type").String())
}

// TestStripMessagesSignatureBlocksForRetryBytes_PassbackAndNoop exercises
// passback signature removal alongside unchanged no-op payloads. Passback mode
// must retain non-empty unsigned thinking text while dropping signatures and
// redacted blocks, and signature-free payloads must return false untouched.
func TestStripMessagesSignatureBlocksForRetryBytes_PassbackAndNoop(t *testing.T) {
	passbackBody := []byte(`{"model":"deepseek-reasoner","thinking":{"type":"enabled","budget_tokens":2048},"messages":[{"role":"user","content":"run"},{"role":"assistant","content":[{"type":"thinking","thinking":"keep me","signature":"EqQBSig"},{"type":"thinking","thinking":"","signature":"EqQBEmpty"},{"type":"redacted_thinking","data":"EmwK"},{"type":"tool_use","id":"call_1","name":"ping","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"pong"}]}]}`)

	patched, changed, losses := StripMessagesSignatureBlocksWithLosses(passbackBody, true)
	require.Equal(t, SignatureRetryLosses{Tokens: 3}, losses)
	require.True(t, changed)
	require.Equal(t, "enabled", gjson.GetBytes(patched, "thinking.type").String())
	require.Equal(t, 2, len(gjson.GetBytes(patched, "messages.1.content").Array()))
	require.Equal(t, "thinking", gjson.GetBytes(patched, "messages.1.content.0.type").String())
	require.Equal(t, "keep me", gjson.GetBytes(patched, "messages.1.content.0.thinking").String())
	require.False(t, gjson.GetBytes(patched, "messages.1.content.0.signature").Exists())
	require.Equal(t, "tool_use", gjson.GetBytes(patched, "messages.1.content.1.type").String())

	cleanBody := []byte(`{"model":"claude-opus-4-6","max_tokens":1024,"messages":[{"role":"user","content":"hello"}]}`)
	unchanged, didChange := StripMessagesSignatureBlocksForRetryBytes(cleanBody, false)
	require.False(t, didChange)
	require.Equal(t, cleanBody, unchanged)
}
