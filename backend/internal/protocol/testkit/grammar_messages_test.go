package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestValidateMessagesStream feeds valid multi-block Anthropic Messages SSE
// streams alongside post-stop deltas (7b15de584) and error terminations. Valid
// streams and closed-block abnormal errors must pass, while post-stop deltas
// and dangling open blocks must report violations.
func TestValidateMessagesStream(t *testing.T) {
	validFrames := []sse.Frame{
		{Event: "message_start", Data: []byte(`{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"usage":{"input_tokens":12,"output_tokens":1}}}`)},
		{Event: "ping", Data: []byte(`{"type":"ping"}`)},
		{Event: "content_block_start", Data: []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`)},
		{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"step 1"}}`)},
		{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig_abc"}}`)},
		{Event: "content_block_stop", Data: []byte(`{"type":"content_block_stop","index":0}`)},
		{Event: "content_block_start", Data: []byte(`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"run_cmd","input":{}}}`)},
		{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"cmd\":\"ls\"}"}}`)},
		{Event: "content_block_stop", Data: []byte(`{"type":"content_block_stop","index":1}`)},
		{Event: "message_delta", Data: []byte(`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":18}}`)},
		{Event: "message_stop", Data: []byte(`{"type":"message_stop"}`)},
	}
	require.Empty(t, ValidateStreamGrammar(protocol.ProtocolAnthropicMessages, validFrames, GrammarOptions{}))

	// Delta arriving after content_block_stop (7b15de584) must fail.
	afterStop := append([]sse.Frame(nil), validFrames[:6]...)
	afterStop = append(afterStop,
		sse.Frame{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"late"}}`)},
		validFrames[9],
		validFrames[10],
	)
	v := ValidateMessagesStream(afterStop, GrammarOptions{})
	require.NotEmpty(t, v)
	require.Equal(t, "delta_without_open_block", v[0].Rule)

	// Abnormal termination after closing open block passes AllowAbnormal and fails normal mode.
	cleanErr := append([]sse.Frame(nil), validFrames[:6]...)
	cleanErr = append(cleanErr, sse.Frame{Event: "error", Data: []byte(`{"type":"error","error":{"type":"overloaded_error","message":"upstream cut"}}`)})
	require.NotEmpty(t, ValidateMessagesStream(cleanErr, GrammarOptions{AllowAbnormal: false}))
	require.Empty(t, ValidateMessagesStream(cleanErr, GrammarOptions{AllowAbnormal: true}))

	// Dangling open block before error must fail even in AllowAbnormal mode.
	danglingErr := append([]sse.Frame(nil), validFrames[:4]...)
	danglingErr = append(danglingErr, sse.Frame{Event: "error", Data: []byte(`{"type":"error","error":{"type":"api_error","message":"cut"}}`)})
	v = ValidateMessagesStream(danglingErr, GrammarOptions{AllowAbnormal: true})
	require.NotEmpty(t, v)
	require.Equal(t, "dangling_block_before_error", v[0].Rule)

	// tool_use content_block_start missing an object input field must fail.
	missingToolInput := append([]sse.Frame(nil), validFrames...)
	missingToolInput[6] = sse.Frame{
		Event: "content_block_start",
		Data:  []byte(`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"run_cmd"}}`),
	}
	v = ValidateMessagesStream(missingToolInput, GrammarOptions{})
	require.NotEmpty(t, v)
	require.Equal(t, "missing_tool_use_input", v[0].Rule)

	// Truncated tool_use partial_json closed before event: error passes AllowAbnormal,
	// whereas claiming normal message_stop completion with truncated JSON fails even under AllowAbnormal.
	abortedToolErr := []sse.Frame{
		validFrames[0],
		validFrames[6],
		{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"cmd\":\"ls"}}`)},
		{Event: "content_block_stop", Data: []byte(`{"type":"content_block_stop","index":0}`)},
		{Event: "error", Data: []byte(`{"type":"error","error":{"type":"api_error","message":"cut"}}`)},
	}
	abortedToolErr[1] = sse.Frame{
		Event: "content_block_start",
		Data:  []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"run_cmd","input":{}}}`),
	}
	require.Empty(t, ValidateMessagesStream(abortedToolErr, GrammarOptions{AllowAbnormal: true}))

	fakeCompletedTruncatedTool := []sse.Frame{
		validFrames[0],
		abortedToolErr[1],
		abortedToolErr[2],
		abortedToolErr[3],
		validFrames[9],
		validFrames[10],
	}
	v = ValidateMessagesStream(fakeCompletedTruncatedTool, GrammarOptions{AllowAbnormal: true})
	require.Len(t, v, 1)
	require.Equal(t, "invalid_tool_input_json", v[0].Rule)
}
