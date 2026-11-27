package provider

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestApplyAnthropicCompatFullReplayGuardBytes_DelegatesLosslessTrimming trims a
// fifteen-message raw Anthropic payload through the gateway provider wrapper.
// The wrapper returns twelve retained messages and keeps unknown fields intact.
func TestApplyAnthropicCompatFullReplayGuardBytes_DelegatesLosslessTrimming(t *testing.T) {
	var msgs []string
	for i := range 15 {
		msgs = append(msgs, fmt.Sprintf(`{"role":"user","content":"msg-%d","extra":9007199254740993}`, i))
	}
	raw := []byte(fmt.Sprintf(`{"model":"claude-sonnet-4-5","messages":[%s]}`, strings.Join(msgs, ",")))

	got, count, trimmed := ApplyAnthropicCompatFullReplayGuardBytes(raw)
	require.True(t, trimmed)
	require.Equal(t, 12, count)
	require.NotContains(t, string(got), `"msg-2"`)
	require.Contains(t, string(got), `"msg-3"`)
	require.Contains(t, string(got), `9007199254740993`)
}

// TestTrimCompatResponsesInputToLatestTurnBytes_DelegatesLosslessTrimming trims
// a raw Responses continuation request through the gateway provider wrapper. The
// paired function call and structured output survive while older turns drop.
func TestTrimCompatResponsesInputToLatestTurnBytes_DelegatesLosslessTrimming(t *testing.T) {
	raw := []byte(`{"input":[` +
		`{"type":"message","role":"user","content":"old"},` +
		`{"type":"function_call","call_id":"c1","name":"tool","arguments":"{}"},` +
		`{"type":"function_call_output","call_id":"c1","output":[{"type":"input_text","text":"done"}]}` +
		`]}`)

	got, changed := TrimCompatResponsesInputToLatestTurnBytes(raw)
	require.True(t, changed)
	require.NotContains(t, string(got), `"old"`)
	require.Contains(t, string(got), `"output":[{"type":"input_text","text":"done"}]`)
}
