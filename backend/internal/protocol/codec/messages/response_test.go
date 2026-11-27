package messages

import (
	"encoding/json/jsontext"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestResponseDecodeAndEncodeRoundTrip verifies buffered Messages response
// decoding and encoding across thinking, redacted_thinking, server_tool_use
// OpaqueItems, citations, missing call IDs, and same-origin Extra fields.
// Encoded output must retain client model overrides and pass VerifyEssence.
func TestResponseDecodeAndEncodeRoundTrip(t *testing.T) {
	_, _, err := EncodeResponse(nil, ir.ClientView{})
	require.Error(t, err)

	_, err = DecodeResponse([]byte(`{"type":"error","error":{"type":"overloaded_error","message":"overloaded"}}`))
	require.Error(t, err)

	fiveMin := int64(60)
	oneHour := int64(40)
	resp := &ir.Response{
		Parts: []ir.Part{
			{
				Kind: ir.PartReasoning,
				Reasoning: &ir.Reasoning{
					Segments: []string{"Step 1. ", "Step 2."},
					Token:    &ir.ReplayToken{Data: "EqQSig1"},
				},
			},
			{
				Kind: ir.PartReasoning,
				Reasoning: &ir.Reasoning{
					Redacted: true,
					Token:    &ir.ReplayToken{Data: "EmwKRedacted1"},
				},
			},
			{
				Kind: ir.PartOpaque,
				Opaque: &ir.OpaqueItem{
					Origin: protocol.ProtocolAnthropicMessages,
					Type:   "server_tool_use",
					Raw:    jsontext.Value(`{"id":"srvtoolu_1","input":{"query":"go"},"name":"web_search","type":"server_tool_use"}`),
				},
			},
			{
				Kind: ir.PartText,
				ID:   "part_txt_1",
				Text: "Lookup scheduled.",
				Annotations: []ir.Annotation{{
					Kind: "web_search_result_location",
					URL:  "https://go.dev",
				}},
			},
			{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID:    "",
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "lookup"},
					Arguments: `{"id":1}`,
					Token:     &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "gem_sig"},
				},
			},
		},
		Finish: ir.Finish{Reason: ir.FinishStopSequence, StopSequence: "</done>"},
		Usage: &ir.Usage{
			Input:        40,
			CacheRead:    10,
			CacheWrite:   100,
			CacheWrite5m: &fiveMin,
			CacheWrite1h: &oneHour,
			Output:       15,
			Speed:        "fast",
		},
		Extra: ir.Extra{
			Origin: protocol.ProtocolAnthropicMessages,
			Members: map[string]jsontext.Value{
				"container": jsontext.Value(`{"id":"cntr_1"}`),
			},
		},
	}

	view := ir.ClientView{
		Model:   "claude-sonnet-4-5-client",
		Runtime: newDeterministicRuntime(),
	}
	encoded, losses, err := EncodeResponse(resp, view)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"container":{"id":"cntr_1"}`)
	require.Contains(t, string(encoded), `"id":"msg_1"`)
	require.Contains(t, string(encoded), `"toolu_2"`)
	require.Contains(t, string(encoded), `"stop_sequence":"</done>"`)

	decoded, err := DecodeResponse(encoded)
	require.NoError(t, err)
	require.Equal(t, "msg_1", decoded.ID)
	require.Equal(t, "claude-sonnet-4-5-client", decoded.Model)
	require.Equal(t, ir.FinishStopSequence, decoded.Finish.Reason)
	require.Equal(t, "</done>", decoded.Finish.StopSequence)
	require.Equal(t, int64(40), decoded.Usage.Input)
	require.Equal(t, int64(10), decoded.Usage.CacheRead)
	require.Equal(t, int64(100), decoded.Usage.CacheWrite)
	require.Equal(t, "fast", decoded.Usage.Speed)

	essDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(resp), ir.ResponseEssence(decoded), losses)
	require.NoError(t, err)
	require.Empty(t, essDiffs, testkit.FormatJSONDiff(essDiffs))
}

// TestDecodeResponseRejectsInvalidOrDuplicateKeyToolInput checks buffered
// Messages responses carrying non-object values or duplicate keys inside
// tool_use.input. Section 3.6 requires rejecting ambiguous or malformed tool inputs.
func TestDecodeResponseRejectsInvalidOrDuplicateKeyToolInput(t *testing.T) {
	dupKey := []byte(`{"id":"m1","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"tool_use","id":"t1","name":"exec","input":{"a":1,"a":2}}],"stop_reason":"tool_use","usage":{"input_tokens":1,"output_tokens":1}}`)
	_, err := DecodeResponse(dupKey)
	require.Error(t, err)

	nonObj := []byte(`{"id":"m2","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"tool_use","id":"t1","name":"exec","input":[1,2]}],"stop_reason":"tool_use","usage":{"input_tokens":1,"output_tokens":1}}`)
	_, err = DecodeResponse(nonObj)
	require.Error(t, err)
}

// FuzzResponseDecode feeds arbitrary byte slices into DecodeResponse across all
// Messages wire response seeds and boundary cases. Every successfully decoded
// response must pass ir.ValidateResponse, carry valid JSON object tool arguments,
// and re-encode deterministically.
func FuzzResponseDecode(f *testing.F) {
	respFixes, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureResponse)
	require.NoError(f, err)
	for _, fix := range respFixes {
		f.Add(fix.Data)
	}
	f.Add([]byte(`{"id":"m","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"tool_use","id":"t1","name":"fn","input":{"a":1,"a":2}}],"stop_reason":"tool_use","usage":{"input_tokens":1,"output_tokens":1}}`))

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
				if !isValidSingleJSONObject([]byte(p.Call.Arguments)) {
					t.Fatalf("DecodeResponse accepted invalid tool_use.input %q", p.Call.Arguments)
				}
			}
		}
		enc1, _, err := EncodeResponse(resp, ir.ClientView{Model: "claude-sonnet-4-5", Runtime: newDeterministicRuntime()})
		if err != nil {
			t.Fatalf("EncodeResponse failed: %v", err)
		}
		enc2, _, err := EncodeResponse(resp, ir.ClientView{Model: "claude-sonnet-4-5", Runtime: newDeterministicRuntime()})
		if err != nil {
			t.Fatalf("second EncodeResponse failed: %v", err)
		}
		if !bytesEqual(enc1, enc2) {
			t.Fatalf("non-deterministic EncodeResponse output")
		}
	})
}
