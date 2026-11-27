package chat

import (
	"encoding/json/jsontext"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestResponseDecodeAndEncodeRoundTrip verifies buffered response decoding and
// encoding across reasoning, refusal, missing tool_call IDs, and replay tokens.
// Interleaved source parts must record LossPartsReordered while preserving
// same-origin Chat Extra members across round trips.
func TestResponseDecodeAndEncodeRoundTrip(t *testing.T) {
	_, _, err := EncodeResponse(nil, ir.ClientView{})
	require.Error(t, err)

	_, err = DecodeResponse([]byte(`{"error":{"message":"bad gateway"}}`))
	require.Error(t, err)

	resp := &ir.Response{
		ServiceTier: "priority",
		Parts: []ir.Part{
			{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID:    "",
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "lookup"},
					Arguments: `{"id":1}`,
					Token:     &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig_1"},
				},
			},
			{
				Kind: ir.PartReasoning,
				Reasoning: &ir.Reasoning{
					Segments: []string{"Step 1. ", "Step 2."},
					Token:    &ir.ReplayToken{Scheme: ir.SchemeOpenAI, Data: "enc_1"},
				},
			},
			{
				Kind: ir.PartText,
				ID:   "msg_part_1",
				Text: "Lookup scheduled.",
				Annotations: []ir.Annotation{{
					Kind: "url_citation",
					URL:  "https://example.com",
				}},
			},
		},
		Finish: ir.Finish{Reason: ir.FinishToolCalls},
		Usage:  &ir.Usage{Input: 40, CacheRead: 10, Output: 15, Reasoning: 5},
		Extra: ir.Extra{
			Origin: protocol.ProtocolOpenAIChatCompletions,
			Members: map[string]jsontext.Value{
				"system_fingerprint": jsontext.Value(`"fp_abc123"`),
			},
		},
	}

	view := ir.ClientView{
		Model:   "gpt-4o-client",
		Runtime: newDeterministicRuntime(),
	}
	encoded, losses, err := EncodeResponse(resp, view)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"system_fingerprint":"fp_abc123"`)
	require.Contains(t, string(encoded), `"id":"chatcmpl-1"`)
	require.Contains(t, string(encoded), `"call_2"`)

	var lossKinds []ir.LossKind
	for _, l := range losses {
		lossKinds = append(lossKinds, l.Kind)
	}
	require.Contains(t, lossKinds, ir.LossIDRewritten)
	require.Contains(t, lossKinds, ir.LossReplayTokenDropped)
	require.Contains(t, lossKinds, ir.LossAnnotationDropped)
	require.Contains(t, lossKinds, ir.LossPartsReordered)

	decoded, err := DecodeResponse(encoded)
	require.NoError(t, err)
	require.Equal(t, "chatcmpl-1", decoded.ID)
	require.Equal(t, "gpt-4o-client", decoded.Model)
	require.Equal(t, int64(1760000000), decoded.CreatedAt)
	require.Equal(t, "priority", decoded.ServiceTier)
	require.Len(t, decoded.Parts, 3)
	require.Equal(t, ir.PartReasoning, decoded.Parts[0].Kind)
	require.Equal(t, []string{"Step 1. Step 2."}, decoded.Parts[0].Reasoning.Segments)
	require.Equal(t, ir.PartText, decoded.Parts[1].Kind)
	require.Equal(t, "Lookup scheduled.", decoded.Parts[1].Text)
	require.Equal(t, ir.PartToolCall, decoded.Parts[2].Kind)
	require.Equal(t, "call_2", decoded.Parts[2].Call.CallID)
	require.Equal(t, int64(40), decoded.Usage.Input)
	require.Equal(t, int64(10), decoded.Usage.CacheRead)
}

// TestDecodeResponseRejectsInvalidOrDuplicateKeyToolArguments checks buffered
// responses carrying truncated JSON or duplicate object keys inside
// string-encoded function.arguments. Both modern tool_calls and legacy
// function_call fields must fail decoding before reaching downstream encoders.
func TestDecodeResponseRejectsInvalidOrDuplicateKeyToolArguments(t *testing.T) {
	truncated := []byte(`{"id":"c1","object":"chat.completion","created":1,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"exec","arguments":"{\"cmd\":\"ssh"}}]},"finish_reason":"length"}]}`)
	_, err := DecodeResponse(truncated)
	require.Error(t, err)

	dupKey := []byte(`{"id":"c2","object":"chat.completion","created":1,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"exec","arguments":"{\"a\":1,\"a\":2}"}}]},"finish_reason":"tool_calls"}]}`)
	_, err = DecodeResponse(dupKey)
	require.Error(t, err)

	legacyDupKey := []byte(`{"id":"c3","object":"chat.completion","created":1,"model":"gpt-4o","choices":[{"index":0,"message":{"role":"assistant","content":null,"function_call":{"name":"exec","arguments":"{\"a\":1,\"a\":2}"}},"finish_reason":"function_call"}]}`)
	_, err = DecodeResponse(legacyDupKey)
	require.Error(t, err)
}

// FuzzResponseDecode feeds arbitrary byte slices into DecodeResponse across
// wire response seeds and boundary cases. Every successfully decoded response
// must pass ir.ValidateResponse, carry valid JSON tool arguments, and re-encode deterministically.
func FuzzResponseDecode(f *testing.F) {
	respFixes, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureResponse)
	require.NoError(f, err)
	for _, fix := range respFixes {
		f.Add(fix.Data)
	}
	f.Add([]byte(`{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","function_call":{"name":"f","arguments":"{\"x\":1}"}},"finish_reason":"function_call"}]}`))
	f.Add([]byte(`{"id":"c","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","tool_calls":[{"id":"1","type":"function","function":{"name":"f","arguments":"{\"a\":1,\"a\":2}"}}]},"finish_reason":"tool_calls"}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		resp, err := DecodeResponse(data)
		if err != nil {
			return
		}
		if err := ir.ValidateResponse(resp); err != nil {
			t.Fatalf("DecodeResponse succeeded but ir.ValidateResponse failed: %v", err)
		}
		for _, p := range resp.Parts {
			if p.Kind == ir.PartToolCall && p.Call != nil && strings.TrimSpace(p.Call.Arguments) != "" {
				if !isValidSingleJSON([]byte(p.Call.Arguments)) {
					t.Fatalf("DecodeResponse accepted invalid tool arguments %q", p.Call.Arguments)
				}
			}
		}
		enc1, _, err := EncodeResponse(resp, ir.ClientView{Model: "gpt-4o", Runtime: newDeterministicRuntime()})
		if err != nil {
			t.Fatalf("EncodeResponse failed: %v", err)
		}
		enc2, _, err := EncodeResponse(resp, ir.ClientView{Model: "gpt-4o", Runtime: newDeterministicRuntime()})
		if err != nil {
			t.Fatalf("second EncodeResponse failed: %v", err)
		}
		if !bytesEqual(enc1, enc2) {
			t.Fatalf("non-deterministic EncodeResponse output")
		}
	})
}
