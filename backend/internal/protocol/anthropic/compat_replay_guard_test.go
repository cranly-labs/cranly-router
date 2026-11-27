package anthropic

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestApplyAnthropicCompatFullReplayGuardBytes_TrimsAndPreservesExtras builds a
// fifteen-message request carrying unknown top-level and message fields plus a
// 64-bit integer above float64 precision. Trimming retains the final twelve
// messages and keeps every unknown field and exact integer literal intact.
func TestApplyAnthropicCompatFullReplayGuardBytes_TrimsAndPreservesExtras(t *testing.T) {
	var msgs []string
	for i := range 15 {
		msgs = append(msgs, fmt.Sprintf(
			`{"role":"user","content":[{"type":"text","text":"m-%d","extra_block":9007199254740993}],"extra_msg":"keep-%d"}`,
			i, i,
		))
	}
	raw := fmt.Sprintf(
		`{"model":"claude-sonnet-4-5","custom_top":{"id":9007199254740993},"messages":[%s],"stream":true}`,
		strings.Join(msgs, ","),
	)

	got, count, trimmed := ApplyAnthropicCompatFullReplayGuardBytes([]byte(raw))
	require.True(t, trimmed)
	require.Equal(t, 12, count)
	gotStr := string(got)
	require.Contains(t, gotStr, `"custom_top":{"id":9007199254740993}`)
	require.Contains(t, gotStr, `"extra_block":9007199254740993`)
	require.Contains(t, gotStr, `"extra_msg":"keep-3"`)
	require.NotContains(t, gotStr, `"m-2"`)
	require.Contains(t, gotStr, `"m-3"`)
	require.Contains(t, gotStr, `"m-14"`)
}

// TestApplyAnthropicCompatFullReplayGuardBytes_ExpandsAcrossToolPair places an
// assistant tool_use immediately before the nominal twelve-message cut and its
// matching tool_result inside the retained tail. Expansion pulls the assistant
// message into the suffix so thirteen messages remain paired.
func TestApplyAnthropicCompatFullReplayGuardBytes_ExpandsAcrossToolPair(t *testing.T) {
	msgs := []string{
		`{"role":"user","content":"m-0"}`,
		`{"role":"assistant","content":"m-1"}`,
		`{"role":"assistant","content":[{"type":"tool_use","id":"call_boundary","name":"bash","input":{"cmd":"ls"}}]}`,
		`{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_boundary","content":"ok"}]}`,
	}
	for i := 4; i < 15; i++ {
		msgs = append(msgs, fmt.Sprintf(`{"role":"user","content":"m-%d"}`, i))
	}
	raw := fmt.Sprintf(`{"model":"claude-sonnet-4-5","messages":[%s]}`, strings.Join(msgs, ","))

	got, count, trimmed := ApplyAnthropicCompatFullReplayGuardBytes([]byte(raw))
	require.True(t, trimmed)
	require.Equal(t, 13, count)
	require.NotContains(t, string(got), `"m-1"`)
	require.Contains(t, string(got), `"call_boundary"`)
}

// TestApplyAnthropicCompatFullReplayGuardBytes_LeavesShortOrZeroStartUntouched
// checks a short history and a tool pair anchored at message zero. Both cases
// return false and preserve the exact input byte slice without reformatting.
func TestApplyAnthropicCompatFullReplayGuardBytes_LeavesShortOrZeroStartUntouched(t *testing.T) {
	shortRaw := []byte("{\n  \"model\": \"claude-sonnet-4-5\",\n  \"messages\": [{\"role\": \"user\", \"content\": \"hi\"}]\n}")
	gotShort, shortCount, shortTrimmed := ApplyAnthropicCompatFullReplayGuardBytes(shortRaw)
	require.False(t, shortTrimmed)
	require.Equal(t, 1, shortCount)
	require.Equal(t, shortRaw, gotShort)

	msgs := []string{
		`{"role":"assistant","content":[{"type":"tool_use","id":"call_root","name":"bash","input":{}}]}`,
	}
	for i := 1; i < 14; i++ {
		msgs = append(msgs, fmt.Sprintf(`{"role":"user","content":"m-%d"}`, i))
	}
	msgs = append(msgs, `{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_root","content":"done"}]}`)
	anchoredRaw := []byte(fmt.Sprintf("{\n  \"messages\": [%s]\n}", strings.Join(msgs, ",")))

	gotAnchored, anchoredCount, anchoredTrimmed := ApplyAnthropicCompatFullReplayGuardBytes(anchoredRaw)
	require.False(t, anchoredTrimmed)
	require.Equal(t, 15, anchoredCount)
	require.Equal(t, anchoredRaw, gotAnchored)
}
