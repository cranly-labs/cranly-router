package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestSimulateGeminiNextTurn folds streamed Gemini GenerateContent thought and
// functionCall chunks across preserve_thoughts and drop_thoughts variants. Both
// simulated requests must pass ValidateGeminiRequest under gemini-signature,
// while drop_thoughts removes pure thought text parts without dropping call signatures.
func TestSimulateGeminiNextTurn(t *testing.T) {
	prevReq := []byte(`{
		"tools": [{"functionDeclarations": [{"name": "fetch_status", "parameters": {"type": "OBJECT"}}]}],
		"contents": [{"role": "user", "parts": [{"text": "Status?"}]}]
	}`)
	frames := []sse.Frame{
		{Data: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"Checking ","thought":true}]}}]}`)},
		{Data: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"service","thought":true,"thoughtSignature":"sig_thought"}]}}]}`)},
		{Data: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"fetch_status","args":{}},"thoughtSignature":"sig_call"}]},"finishReason":"STOP"}]}`)},
	}

	for _, variant := range ClientSimVariants(protocol.ProtocolGeminiGenerateContent) {
		nextReq, err := SimulateNextTurn(protocol.ProtocolGeminiGenerateContent, TurnInput{
			PrevRequest:    prevReq,
			ResponseFrames: frames,
			ToolOutputs:    []SimulatedToolOutput{{Name: "fetch_status", Output: `{"healthy":true}`}},
			Variant:        variant,
		})
		require.NoError(t, err)
		require.Empty(t, ValidateGeminiRequest(nextReq, RequestValidationOptions{
			DialectRules: []string{"gemini-signature"},
		}))
		if variant == SimVariantGeminiDropThoughts {
			require.NotContains(t, string(nextReq), "sig_thought")
			require.Contains(t, string(nextReq), "sig_call")
		} else {
			require.Contains(t, string(nextReq), "Checking service")
			require.Contains(t, string(nextReq), "sig_thought")
		}
	}
}
