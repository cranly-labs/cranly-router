package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestSimulateMessagesNextTurn folds streamed Messages thinking and tool_use
// frames into a second-turn request under default and drop_thinking variants.
// Both simulated requests must pass ValidateMessagesRequest, and only the
// default variant may retain historical thinking signatures.
func TestSimulateMessagesNextTurn(t *testing.T) {
	prevReq := []byte(`{
		"model": "claude-sonnet-4-5",
		"max_tokens": 2048,
		"tools": [{"name": "run_cmd", "input_schema": {"type": "object"}}],
		"messages": [{"role": "user", "content": "list files"}]
	}`)
	frames := []sse.Frame{
		{Event: "message_start", Data: []byte(`{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"usage":{"input_tokens":5,"output_tokens":1}}}`)},
		{Event: "content_block_start", Data: []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`)},
		{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"plan"}}`)},
		{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig_123"}}`)},
		{Event: "content_block_stop", Data: []byte(`{"type":"content_block_stop","index":0}`)},
		{Event: "content_block_start", Data: []byte(`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_1","name":"run_cmd","input":{}}}`)},
		{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"cmd\":\"ls\"}"}}`)},
		{Event: "content_block_stop", Data: []byte(`{"type":"content_block_stop","index":1}`)},
		{Event: "message_delta", Data: []byte(`{"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":10}}`)},
		{Event: "message_stop", Data: []byte(`{"type":"message_stop"}`)},
	}

	for _, variant := range ClientSimVariants(protocol.ProtocolAnthropicMessages) {
		nextReq, err := SimulateNextTurn(protocol.ProtocolAnthropicMessages, TurnInput{
			PrevRequest:      prevReq,
			ResponseFrames:   frames,
			ToolOutputs:      []SimulatedToolOutput{{CallID: "toolu_1", Output: "go.mod\nREADME.md"}},
			FollowUpUserText: "Summarize",
			Variant:          variant,
		})
		require.NoError(t, err)
		require.Empty(t, ValidateMessagesRequest(nextReq, RequestValidationOptions{StrictTopLevel: true}))
		if variant == SimVariantMessagesDropThinking {
			require.NotContains(t, string(nextReq), "sig_123")
		} else {
			require.Contains(t, string(nextReq), "sig_123")
		}
	}
}
