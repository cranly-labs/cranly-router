package openai

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTrimCompatResponsesInputToLatestTurnBytes_PreservesStructuredToolOutputs
// trims a multi-turn Responses input ending in a function_call_output with an
// array output value and unknown fields. Earlier unrelated turns disappear while
// the paired function_call, array output, and 64-bit integer remain intact.
func TestTrimCompatResponsesInputToLatestTurnBytes_PreservesStructuredToolOutputs(t *testing.T) {
	raw := []byte(`{` +
		`"model":"gpt-5.4",` +
		`"previous_response_id":"resp_prev",` +
		`"extra_top":9007199254740993,` +
		`"input":[` +
		`{"type":"message","role":"user","content":"turn-1"},` +
		`{"type":"message","role":"assistant","content":"reply-1"},` +
		`{"type":"function_call","call_id":"call_keep","name":"read_img","arguments":"{}","extra_call":9007199254740993},` +
		`{"type":"function_call_output","call_id":"call_keep","output":[{"type":"input_text","text":"seen"}],"extra_out":true}` +
		`]` +
		`}`)

	got, changed := TrimCompatResponsesInputToLatestTurnBytes(raw)
	require.True(t, changed)
	gotStr := string(got)
	require.NotContains(t, gotStr, `"turn-1"`)
	require.NotContains(t, gotStr, `"reply-1"`)
	require.Contains(t, gotStr, `"call_keep"`)
	require.Contains(t, gotStr, `"output":[{"type":"input_text","text":"seen"}]`)
	require.Contains(t, gotStr, `"extra_call":9007199254740993`)
	require.Contains(t, gotStr, `"extra_top":9007199254740993`)
}

// TestTrimCompatResponsesInputToLatestTurnBytes_ExpandsBeforeTrailingUserMessage
// places a function_call, its output, and a trailing user message after an
// older user turn. Trimming drops the older turn and keeps the paired call,
// output, and final user message together.
func TestTrimCompatResponsesInputToLatestTurnBytes_ExpandsBeforeTrailingUserMessage(t *testing.T) {
	raw := []byte(`{` +
		`"input":[` +
		`{"type":"message","role":"user","content":"old"},` +
		`{"type":"function_call","call_id":"call_1","name":"sh","arguments":"{}"},` +
		`{"type":"function_call_output","call_id":"call_1","output":"ok"},` +
		`{"type":"message","role":"user","content":"follow-up"}` +
		`]` +
		`}`)

	got, changed := TrimCompatResponsesInputToLatestTurnBytes(raw)
	require.True(t, changed)
	gotStr := string(got)
	require.NotContains(t, gotStr, `"old"`)
	require.Contains(t, gotStr, `"call_1"`)
	require.Contains(t, gotStr, `"follow-up"`)
}

// TestInsertResponsesLeadingDeveloperGuardBytes_InsertsAfterLeadingDevelopers
// injects a guard message into an input array starting with a developer message
// followed by a user message carrying an unmodeled field. A second call detects
// the marker and returns the first output byte-for-byte unchanged.
func TestInsertResponsesLeadingDeveloperGuardBytes_InsertsAfterLeadingDevelopers(t *testing.T) {
	raw := []byte(`{` +
		`"model":"gpt-5.4",` +
		`"input":[` +
		`{"type":"message","role":"developer","content":"lead-dev","custom_dev":1},` +
		`{"type":"message","role":"user","content":"hello","custom_id":9007199254740993}` +
		`]` +
		`}`)

	inserted, ok := InsertResponsesLeadingDeveloperGuardBytes(raw, "<guard>\nrules\n</guard>", "<guard>", "<legacy-guard>")
	require.True(t, ok)
	insertedStr := string(inserted)
	require.Regexp(t, `"lead-dev".*"<guard>\\nrules\\n</guard>".*"hello"`, insertedStr)
	require.Contains(t, insertedStr, `"custom_id":9007199254740993`)

	again, okAgain := InsertResponsesLeadingDeveloperGuardBytes(inserted, "<guard>\nrules\n</guard>", "<guard>", "<legacy-guard>")
	require.False(t, okAgain)
	require.Equal(t, inserted, again)
}
