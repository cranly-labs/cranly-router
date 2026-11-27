package responses

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// response_test.go verifies buffered OpenAI Responses decoding and encoding
// across malformed function arguments, free-text custom tool calls, echoed
// client request metadata, and punctuated client tool identifiers. Fuzz targets
// protect round-trip determinism and Law S4 Essence reconciliation.

// TestDecodeResponseToolArgValidationAndEmptyBlockParity reproduces buffered
// Responses payloads containing truncated or duplicate-key function_call
// arguments, free-text custom_tool_call inputs, and empty assistant text blocks.
// Invalid function arguments must fail decoding, while custom free-text calls
// succeed and empty metadata-free blocks are omitted for Law S2 parity.
func TestDecodeResponseToolArgValidationAndEmptyBlockParity(t *testing.T) {
	truncFn := []byte(`{
		"id": "resp_bad_fn",
		"object": "response",
		"created_at": 1760000000,
		"status": "completed",
		"model": "gpt-5",
		"output": [{"id": "fc_1", "type": "function_call", "call_id": "c_1", "name": "fn", "arguments": "{\"unterminated":1}]
	}`)
	_, err := DecodeResponse(truncFn)
	require.Error(t, err)

	dupKeyFn := []byte(`{
		"id": "resp_dup_fn",
		"object": "response",
		"created_at": 1760000000,
		"status": "completed",
		"model": "gpt-5",
		"output": [{"id": "fc_1", "type": "function_call", "call_id": "c_1", "name": "fn", "arguments": "{\"a\":1,\"a\":2}"}]
	}`)
	_, err = DecodeResponse(dupKeyFn)
	require.Error(t, err)

	// Custom tool free-form input is not JSON and must succeed without JSON validation errors.
	customFreeText := []byte(`{
		"id": "resp_custom_ok",
		"object": "response",
		"created_at": 1760000000,
		"status": "completed",
		"model": "gpt-5",
		"output": [
			{"id": "msg_empty", "type": "message", "status": "completed", "role": "assistant", "content": [{"type": "output_text", "text": "", "annotations": []}]},
			{"id": "ctc_1", "type": "custom_tool_call", "status": "completed", "call_id": "c_patch", "name": "apply_patch", "input": "*** Begin Patch\n+not json{\n*** End Patch"}
		]
	}`)
	resp, err := DecodeResponse(customFreeText)
	require.NoError(t, err)
	require.Len(t, resp.Parts, 1, "empty metadata-free output_text must be omitted")
	require.Equal(t, ir.PartToolCall, resp.Parts[0].Kind)
	require.Equal(t, ir.ToolCustom, resp.Parts[0].Call.Tool.Kind)
	require.Equal(t, ir.FinishToolCalls, resp.Finish.Reason)

	// Missing arguments on tool_search_call must normalize to "{}" for Law S4 round-tripping.
	tscResp, err := DecodeResponse([]byte(`{"output":[{"type":"tool_search_call"}]}`))
	require.NoError(t, err)
	require.Len(t, tscResp.Parts, 1)
	require.Equal(t, "{}", tscResp.Parts[0].Call.Arguments)
}

// TestEncodeResponseEchoesAllNineClientRequestFields encodes a buffered reply
// with a populated ClientView.Request and a bare reasoning continuity token.
// All nine section 3.6 echo fields must appear on the serialized envelope, and
// non-empty Token.Data must write into encrypted_content verbatim.
func TestEncodeResponseEchoesAllNineClientRequestFields(t *testing.T) {
	parallel := true
	store := false
	maxOut := int64(2048)
	clientReq := &ir.Request{
		System:             []ir.Part{{Kind: ir.PartText, Text: "Follow rules."}},
		MaxOutputTokens:    &maxOut,
		ParallelToolCalls:  &parallel,
		PreviousResponseID: "resp_prev_99",
		Store:              &store,
		Reasoning:          &ir.ReasoningConfig{Effort: "high", Summary: "detailed"},
		Output:             &ir.OutputFormat{Kind: "json_object", Verbosity: "low"},
		Tools: []ir.Tool{{
			Kind:        ir.ToolFunction,
			Name:        "lookup_user",
			InputSchema: jsontext.Value(`{"type":"object"}`),
		}},
		ToolChoice: &ir.ToolChoice{Mode: ir.ToolChoiceRequired},
	}

	resp := &ir.Response{
		ID:        "resp_echo_1",
		Model:     "upstream-gpt",
		CreatedAt: 1760000000,
		Parts: []ir.Part{
			{
				Kind: ir.PartReasoning,
				ID:   "rs_bare",
				Reasoning: &ir.Reasoning{
					Segments: []string{"Summary thought."},
					Summary:  true,
					Token:    &ir.ReplayToken{Scheme: ir.SchemeAnthropic, ProviderID: 7, Data: "raw_upstream_tok"},
				},
			},
			{Kind: ir.PartText, ID: "msg_echo", Text: "Done."},
		},
		Finish: ir.Finish{Reason: ir.FinishStop},
	}

	encoded, losses, err := EncodeResponse(resp, ir.ClientView{
		Model:   "gpt-5",
		Request: clientReq,
		Runtime: newDeterministicRuntime(),
	})
	require.NoError(t, err)
	require.Empty(t, losses)

	var wireMap map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(encoded, &wireMap))
	for _, key := range []string{
		"tools",
		"tool_choice",
		"parallel_tool_calls",
		"reasoning",
		"text",
		"store",
		"instructions",
		"max_output_tokens",
		"previous_response_id",
	} {
		require.Contains(t, wireMap, key, "EncodeResponse must echo %s from ClientView.Request", key)
	}
	require.Contains(t, string(encoded), `"encrypted_content":"raw_upstream_tok"`)
}

// TestEncodeResponsePreservesPunctuatedClientToolNames encodes a buffered
// response containing restored custom and function calls with dots and slashes
// in their client-visible names. Both output items must retain exact client
// names without hash suffixes, record zero losses, and round-trip through
// DecodeResponse with balanced ResponseEssence.
func TestEncodeResponsePreservesPunctuatedClientToolNames(t *testing.T) {
	resp := &ir.Response{
		ID:        "resp_punct_1",
		Model:     "gpt-5",
		CreatedAt: 1760000000,
		Parts: []ir.Part{
			{
				Kind: ir.PartToolCall,
				ID:   "ctc_1",
				Call: &ir.ToolCall{
					CallID:    "call_exec_1",
					Tool:      ir.ToolRef{Kind: ir.ToolCustom, Name: "code.exec"},
					Arguments: "ls -la",
				},
			},
			{
				Kind: ir.PartToolCall,
				ID:   "fc_2",
				Call: &ir.ToolCall{
					CallID:    "call_mcp_2",
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Namespace: "mcp.srv", Name: "search.docs"},
					Arguments: `{"q":"ir"}`,
				},
			},
		},
		Finish: ir.Finish{Reason: ir.FinishToolCalls},
	}

	encoded, losses, err := EncodeResponse(resp, ir.ClientView{
		Model:   "gpt-5",
		Runtime: newDeterministicRuntime(),
	})
	require.NoError(t, err)
	require.Empty(t, losses)
	require.Contains(t, string(encoded), `"name":"code.exec"`)
	require.Contains(t, string(encoded), `"name":"search.docs"`)
	require.NotContains(t, string(encoded), "code_exec_")

	redecoded, err := DecodeResponse(encoded)
	require.NoError(t, err)
	require.Equal(t, ir.ResponseEssence(resp), ir.ResponseEssence(redecoded))
}

// FuzzResponseDecode feeds arbitrary and fixture-seeded JSON bytes into
// DecodeResponse without panicking. Every accepted response must pass
// ir.ValidateResponse, re-encode deterministically, and reconcile under VerifyEssence.
func FuzzResponseDecode(f *testing.F) {
	wireResps, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureResponse)
	require.NoError(f, err)
	for _, fix := range wireResps {
		f.Add(fix.Data)
	}
	f.Add([]byte(`{"output":[{"id":"0","type":"message","content":[{"type":"0"}]}]}`))
	f.Add([]byte(`{"output":[{"type":"tool_search_call"}]}`))
	f.Add([]byte(`{"output":[{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"s"}],"content":[{"type":"reasoning_text","text":"c"}]}]}`))
	f.Add([]byte(`{"output":[{"id":"m1","type":"message","text":"msg_lvl","content":[{"type":"output_text","text":"hi","refusal":"extra_ref"}]}]}`))
	f.Add([]byte(`{"CreAteD_At":[],"usage":{}}`))
	f.Add([]byte(`{"0000000000000":"","output":[{"type":"tool_search_call","arguments":"\"\""}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		resp, err := DecodeResponse(data)
		if err != nil {
			return
		}
		require.NoError(t, ir.ValidateResponse(resp))

		encoded, losses, err := EncodeResponse(resp, ir.ClientView{
			Model:   "gpt-5",
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)

		redecoded, err := DecodeResponse(encoded)
		require.NoError(t, err)
		require.NoError(t, ir.ValidateResponse(redecoded))

		diffs, vErr := testkit.VerifyEssence(ir.ResponseEssence(resp), ir.ResponseEssence(redecoded), losses)
		require.NoError(t, vErr)
		require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
	})
}
