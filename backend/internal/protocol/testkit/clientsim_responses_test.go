package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestSimulateResponsesNextTurn folds streamed Responses reasoning and
// custom_tool_call items across preserve_all, strip_encrypted, and
// encrypted_only variants. Every resulting second-turn request must pass
// ValidateResponsesRequest while matching its variant's summary and token retention.
func TestSimulateResponsesNextTurn(t *testing.T) {
	prevReq := []byte(`{
		"model": "gpt-5",
		"tools": [{"type": "custom", "name": "apply_patch"}],
		"input": "Update README"
	}`)
	frames := []sse.Frame{
		{Event: "response.created", Data: []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","object":"response","created_at":1,"status":"in_progress","model":"gpt-5","output":[]}}`)},
		{Event: "response.completed", Data: []byte(`{"type":"response.completed","sequence_number":1,"response":{"id":"resp_1","object":"response","created_at":1,"status":"completed","model":"gpt-5","output":[{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"need patch"}],"encrypted_content":"enc_token"},{"id":"ct_1","type":"custom_tool_call","call_id":"call_9","name":"apply_patch","input":"*** Begin Patch"}]}}`)},
	}

	for _, variant := range ClientSimVariants(protocol.ProtocolOpenAIResponses) {
		nextReq, err := SimulateNextTurn(protocol.ProtocolOpenAIResponses, TurnInput{
			PrevRequest:    prevReq,
			ResponseFrames: frames,
			ToolOutputs:    []SimulatedToolOutput{{CallID: "call_9", Output: "patched"}},
			Variant:        variant,
		})
		require.NoError(t, err)
		require.Empty(t, ValidateResponsesRequest(nextReq, RequestValidationOptions{}))

		switch variant {
		case SimVariantResponsesPreserveAll:
			require.Contains(t, string(nextReq), `"encrypted_content":"enc_token"`)
			require.Contains(t, string(nextReq), `"need patch"`)
		case SimVariantResponsesStripEncrypted:
			require.NotContains(t, string(nextReq), `"encrypted_content"`)
			require.Contains(t, string(nextReq), `"need patch"`)
		case SimVariantResponsesEncryptedOnly:
			require.Contains(t, string(nextReq), `"encrypted_content":"enc_token"`)
			require.NotContains(t, string(nextReq), `"need patch"`)
		}
	}
}
