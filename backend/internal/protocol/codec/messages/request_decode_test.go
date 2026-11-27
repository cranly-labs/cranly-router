package messages

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestDecodeRequestMultimodalMixedUserAndExtensions verifies decoding of scalar
// system strings, mixed user messages alternating tool_result and media/text
// runs, file_id attachments, plaintext document opaque fallbacks, tool_choice
// disable_parallel_tool_use inversion, and vendor 64-bit Extra fields. Re-encoding
// the decoded request must satisfy Law Q2 without dropping unmodeled fields.
func TestDecodeRequestMultimodalMixedUserAndExtensions(t *testing.T) {
	body := []byte(`{
		"model": "claude-opus-4-6",
		"max_tokens": 1024,
		"system": "Follow strict deployment rules.",
		"temperature": 0.4,
		"top_p": 0.9,
		"top_k": 32,
		"stop_sequences": ["</plan>"],
		"speed": "standard",
		"metadata": {"team": "platform"},
		"output_config": {
			"effort": "max",
			"format": {
				"type": "json_schema",
				"schema": {"type": "object", "properties": {"ok": {"type": "boolean"}}}
			}
		},
		"thinking": {"type": "adaptive"},
		"prompt_cache_key": "pck_99",
		"session_id": "sess_99",
		"conversation_id": "conv_99",
		"custom_vendor_int": 9007199254740993,
		"tools": [
			{
				"type": "custom",
				"name": "inspect_file",
				"description": "Inspect file metadata",
				"strict": true,
				"defer_loading": true,
				"input_schema": {"type": "object", "properties": {"id": {"type": "string"}}},
				"cache_control": {"type": "ephemeral", "ttl": "1h", "x_cc_meta": true},
				"x_tool_vendor": "v1"
			},
			{
				"type": "bash_20250124",
				"name": "bash"
			}
		],
		"tool_choice": {
			"type": "tool",
			"name": "inspect_file",
			"disable_parallel_tool_use": true
		},
		"messages": [
			{
				"role": "user",
				"x_msg_vendor": 7,
				"content": [
					{"type": "image", "source": {"type": "file", "file_id": "file_img_1"}},
					{"type": "document", "source": {"type": "file", "file_id": "file_doc_1"}, "title": "spec.pdf"},
					{"type": "document", "source": {"type": "text", "media_type": "text/plain", "data": "notes"}},
					{"type": "text", "text": "Inspect file_doc_1."}
				]
			},
			{
				"role": "assistant",
				"content": [
					{"type": "tool_use", "id": "toolu_1", "name": "inspect_file", "input": {"id": "file_doc_1"}}
				]
			},
			{
				"role": "user",
				"content": [
					{
						"type": "tool_result",
						"tool_use_id": "toolu_1",
						"is_error": true,
						"cache_control": {"type": "ephemeral", "ttl": "5m"},
						"content": [
							{"type": "text", "text": "File locked"},
							{"type": "document", "source": {"type": "url", "url": "https://example.com/err.pdf"}}
						]
					},
					{"type": "text", "text": "Retry with fallback."}
				]
			}
		]
	}`)

	req, err := DecodeRequest(body)
	require.NoError(t, err)
	require.Equal(t, []ir.Part{{Kind: ir.PartText, Text: "Follow strict deployment rules."}}, req.System)
	require.NotNil(t, req.Reasoning)
	require.Equal(t, "adaptive", req.Reasoning.Mode)
	require.Equal(t, "max", req.Reasoning.Effort)
	require.NotNil(t, req.Output)
	require.Equal(t, "json_schema", req.Output.Kind)
	require.Empty(t, req.ServiceTier, "non-fast speed remains in Request.Extra")
	require.Equal(t, "pck_99", req.CacheKey)
	require.Equal(t, "sess_99", req.SessionID)
	require.Equal(t, "conv_99", req.ConversationID)
	require.NotNil(t, req.ParallelToolCalls)
	require.False(t, *req.ParallelToolCalls)
	require.Equal(t, protocol.ProtocolAnthropicMessages, req.Extra.Origin)
	require.False(t, req.Extra.IsZero())

	require.Len(t, req.Tools, 2)
	require.Equal(t, ir.ToolFunction, req.Tools[0].Kind)
	require.NotNil(t, req.Tools[0].Strict)
	require.True(t, *req.Tools[0].Strict)
	require.NotNil(t, req.Tools[0].DeferLoading)
	require.True(t, *req.Tools[0].DeferLoading)
	require.Equal(t, "1h", req.Tools[0].Cache.TTL)
	require.False(t, req.Tools[0].Cache.Extra.IsZero())
	require.Equal(t, ir.ToolHosted, req.Tools[1].Kind)

	// Mixed user message at index 2 split into RoleTool followed by RoleUser.
	require.Len(t, req.Messages, 4)
	require.Equal(t, ir.RoleUser, req.Messages[0].Role)
	require.False(t, req.Messages[0].Extra.IsZero())
	require.Equal(t, ir.MediaSourceFileID, req.Messages[0].Parts[0].Media.Source)
	require.Equal(t, ir.MediaDocument, req.Messages[0].Parts[1].Media.Type)
	require.Equal(t, ir.PartOpaque, req.Messages[0].Parts[2].Kind)
	require.Equal(t, ir.RoleAssistant, req.Messages[1].Role)
	require.Equal(t, ir.RoleTool, req.Messages[2].Role)
	require.True(t, req.Messages[2].Parts[0].Result.IsError)
	require.Equal(t, "5m", req.Messages[2].Parts[0].Cache.TTL)
	require.Len(t, req.Messages[2].Parts[0].Result.Content, 2)
	require.Equal(t, ir.RoleUser, req.Messages[3].Role)
	require.Equal(t, "Retry with fallback.", req.Messages[3].Parts[0].Text)

	// Verify byte-identical fixed point after stripping compatible non-Messages top-level hints.
	enc1, losses1, err := EncodeRequest(req, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses1)
	require.Contains(t, string(enc1), `9007199254740993`)
	require.Contains(t, string(enc1), `"speed":"standard"`)
	require.Contains(t, string(enc1), `"metadata":{"team":"platform"}`)

	dec2, err := DecodeRequest(enc1)
	require.NoError(t, err)
	enc2, losses2, err := EncodeRequest(dec2, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses2)
	require.Equal(t, string(enc1), string(enc2))
}

// TestDecodeRequestValidationErrorsAndInvalidUTF8Tolerance checks that
// DecodeRequest tolerates invalid UTF-8 sequences inside scalar and block-array
// JSON strings across user, assistant, system, and tool_result turns. Duplicate
// object keys, non-object tool schemas or inputs, empty redacted_thinking data,
// invalid thinking modes, and illegal cache_control TTLs must return errors.
func TestDecodeRequestValidationErrorsAndInvalidUTF8Tolerance(t *testing.T) {
	// Invalid UTF-8 byte 0xff inside a scalar user string is tolerated.
	invalidUTF8 := append([]byte(`{"model":"claude-sonnet-4-5","max_tokens":64,"messages":[{"role":"user","content":"hello `), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(` world"}]}`)...)
	req, err := DecodeRequest(invalidUTF8)
	require.NoError(t, err)
	require.NoError(t, ir.Validate(req))

	// Invalid UTF-8 byte 0xff inside block-array system, user, assistant, and
	// tool_result.content arrays must survive pooled array-span scanning.
	var blockArrayUTF8 []byte
	blockArrayUTF8 = append(blockArrayUTF8, []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"messages":[{"role":"system","content":[{"type":"text","text":"sys `)...)
	blockArrayUTF8 = append(blockArrayUTF8, 0xff)
	blockArrayUTF8 = append(blockArrayUTF8, []byte(`"}]},{"role":"user","content":[{"type":"text","text":"ask `)...)
	blockArrayUTF8 = append(blockArrayUTF8, 0xff)
	blockArrayUTF8 = append(blockArrayUTF8, []byte(`"}]},{"role":"assistant","content":[{"type":"thinking","thinking":"plan `)...)
	blockArrayUTF8 = append(blockArrayUTF8, 0xff)
	blockArrayUTF8 = append(blockArrayUTF8, []byte(`","signature":"sig_utf8"},{"type":"text","text":"ans `)...)
	blockArrayUTF8 = append(blockArrayUTF8, 0xff)
	blockArrayUTF8 = append(blockArrayUTF8, []byte(`"},{"type":"tool_use","id":"toolu_utf8","name":"fn","input":{"q":"arg `)...)
	blockArrayUTF8 = append(blockArrayUTF8, 0xff)
	blockArrayUTF8 = append(blockArrayUTF8, []byte(`"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_utf8","content":[{"type":"text","text":"out `)...)
	blockArrayUTF8 = append(blockArrayUTF8, 0xff)
	blockArrayUTF8 = append(blockArrayUTF8, []byte(`"}]}]}]}`)...)

	arrReq, err := DecodeRequest(blockArrayUTF8)
	require.NoError(t, err)
	require.NoError(t, ir.Validate(arrReq))
	require.Len(t, arrReq.Messages, 4)
	require.Equal(t, "sys \ufffd", arrReq.Messages[0].Parts[0].Text)
	require.Equal(t, "ask \ufffd", arrReq.Messages[1].Parts[0].Text)
	require.Equal(t, []string{"plan \ufffd"}, arrReq.Messages[2].Parts[0].Reasoning.Segments)
	require.Equal(t, "ans \ufffd", arrReq.Messages[2].Parts[1].Text)
	require.Contains(t, arrReq.Messages[2].Parts[2].Call.Arguments, "arg \ufffd")
	require.Equal(t, "out \ufffd", arrReq.Messages[3].Parts[0].Result.Content[0].Text)

	badCases := []struct {
		name string
		body string
	}{
		{
			name: "duplicate_top_level_key",
			body: `{"model":"a","model":"b","max_tokens":10,"messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "duplicate_nested_tool_input_key",
			body: `{"model":"a","max_tokens":10,"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"fn","input":{"k":1,"k":2}}]}]}`,
		},
		{
			name: "enabled_thinking_missing_budget",
			body: `{"model":"a","max_tokens":100,"thinking":{"type":"enabled"},"messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "disabled_thinking_with_budget",
			body: `{"model":"a","max_tokens":100,"thinking":{"type":"disabled","budget_tokens":1024},"messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "invalid_cache_control_ttl",
			body: `{"model":"a","max_tokens":100,"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral","ttl":"24h"}}]}]}`,
		},
		{
			name: "non_object_tool_use_input",
			body: `{"model":"a","max_tokens":100,"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"fn","input":"not_an_object"}]}]}`,
		},
		{
			name: "non_object_function_tool_schema",
			body: `{"model":"a","max_tokens":100,"tools":[{"name":"fn","input_schema":"bad"}],"messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "empty_redacted_thinking_data",
			body: `{"model":"a","max_tokens":100,"messages":[{"role":"assistant","content":[{"type":"redacted_thinking","data":""}]}]}`,
		},
		{
			name: "tool_use_inside_user_message",
			body: `{"model":"a","max_tokens":100,"messages":[{"role":"user","content":[{"type":"tool_use","id":"t1","name":"fn","input":{}}]}]}`,
		},
		{
			name: "tool_result_inside_assistant_message",
			body: `{"model":"a","max_tokens":100,"messages":[{"role":"assistant","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}]}`,
		},
	}

	for _, tc := range badCases {
		t.Run(tc.name, func(t *testing.T) {
			_, decErr := DecodeRequest([]byte(tc.body))
			require.Error(t, decErr)
		})
	}
}

// TestDecodeAndEncodeSubObjectUnknownFieldsAndDisabledThinkingEffort verifies
// round-trip preservation of unmodeled fields inside thinking, metadata,
// output_config, output_config.format, and tool_choice alongside disabled
// thinking paired with output_config.effort. Strict-valid requests carrying
// these combinations must satisfy Law Q2 with zero Loss.
func TestDecodeAndEncodeSubObjectUnknownFieldsAndDisabledThinkingEffort(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{
			name: "disabled_thinking_with_output_config_effort",
			body: `{"model":"claude-opus-4-6","max_tokens":100,"thinking":{"type":"disabled"},"output_config":{"effort":"low"},"messages":[{"role":"user","content":"hi"}]}`,
		},
		{
			name: "sub_object_unknown_fields_across_thinking_metadata_output_config_and_tool_choice",
			body: `{"model":"claude-sonnet-4-5","max_tokens":4096,"thinking":{"type":"enabled","budget_tokens":1024,"display":"omitted"},"metadata":{"user_id":"u_42","team":"infra"},"output_config":{"effort":"high","format":{"type":"json_schema","schema":{"type":"object"},"x_fmt_ext":true},"x_oc_ext":9},"tools":[{"name":"fn_check","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto","x_tc_ext":"v1"},"messages":[{"role":"user","content":"check"}]}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(tc.body)
			require.Empty(t, testkit.ValidateMessagesRequest(raw, testkit.RequestValidationOptions{StrictTopLevel: true}))

			dec1, err := DecodeRequest(raw)
			require.NoError(t, err)

			enc1, losses1, err := EncodeRequest(dec1, ir.Dialect{})
			require.NoError(t, err)
			require.Empty(t, losses1)
			require.Empty(t, testkit.ValidateMessagesRequest(enc1, testkit.RequestValidationOptions{StrictTopLevel: true}))

			dec2, err := DecodeRequest(enc1)
			require.NoError(t, err)
			require.Equal(t, dec1, dec2, "Dec(Enc(Dec(x))) must equal Dec(x)")

			enc2, losses2, err := EncodeRequest(dec2, ir.Dialect{})
			require.NoError(t, err)
			require.Empty(t, losses2)
			require.Equal(t, string(enc1), string(enc2), "Enc(Dec(enc1)) must equal enc1")
		})
	}
}

// FuzzRequestDecode feeds arbitrary byte slices into DecodeRequest seeded with
// all wire and recorded corpus Messages request fixtures. Every decoded request
// must pass ir.Validate, encode deterministically, and satisfy Law Q2 when
// encoding incurs zero Loss on strict-valid Messages inputs.
func FuzzRequestDecode(f *testing.F) {
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureRequest)
	require.NoError(f, err)
	for _, fix := range wireReqs {
		f.Add(fix.Data)
	}
	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolAnthropicMessages)
	require.NoError(f, err)
	for _, fix := range corpusReqs {
		f.Add(fix.Data)
	}
	f.Add([]byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"system":"sys","output_config":{"effort":"high"},"messages":[{"role":"user","content":"hello"}]}`))
	f.Add([]byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"output_config":{},"metadata":{},"messages":[{"role":"user","content":[{"type":"text","text":"hi"}]}]}`))
	f.Add([]byte(`{"model":"claude-opus-4-6","max_tokens":100,"thinking":{"type":"disabled"},"output_config":{"effort":"low"},"messages":[{"role":"user","content":"hi"}]}`))
	f.Add([]byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"tools":[{"name":"fn_a","input_schema":{"type":"object"}},{"name":"fn_b","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"run"},{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"fn_a","input":{}},{"type":"tool_use","id":"t2","name":"fn_b","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"t2","content":"r2"},{"type":"tool_result","tool_use_id":"t1","content":"r1"}]}]}`))
	f.Add([]byte(`{"model":"claude-sonnet-4-5","max_tokens":4096,"thinking":{"type":"enabled","budget_tokens":1024,"display":"omitted"},"metadata":{"user_id":"u1","team":"core"},"output_config":{"effort":"medium","format":{"type":"json_schema","schema":{"type":"object"},"x_fmt":1},"x_oc":2},"tools":[{"name":"fn","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto","x_tc":3},"messages":[{"role":"user","content":"hi"}]}`))
	f.Add([]byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"messages":[{"role":"user","content":[{"type":"document","source":{"type":"base64","media_type":" application/pdf ","data":"AAAA"},"x_doc":true},{"type":"0000","0000":"x"}]}]}`))
	f.Add(append(append([]byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"messages":[{"role":"user","content":[{"type":"text","text":"u`), 0xff), []byte(`"}]},{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"fn","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"r"}]}`+`]}`+`]}`)...))

	f.Fuzz(func(t *testing.T, data []byte) {
		req, err := DecodeRequest(data)
		if err != nil {
			return
		}
		if err := ir.Validate(req); err != nil {
			t.Fatalf("DecodeRequest succeeded but ir.Validate failed: %v", err)
		}
		enc1, losses1, err := EncodeRequest(req, ir.Dialect{})
		if err != nil {
			return
		}
		enc2, _, err := EncodeRequest(req, ir.Dialect{})
		if err != nil {
			t.Fatalf("second EncodeRequest failed: %v", err)
		}
		if !bytesEqual(enc1, enc2) {
			t.Fatalf("non-deterministic EncodeRequest output")
		}
		if len(losses1) == 0 {
			decAfter, err := DecodeRequest(enc1)
			if err != nil {
				t.Fatalf("DecodeRequest(enc1) failed on zero-loss encode: %v", err)
			}
			encAfter, lossesAfter, err := EncodeRequest(decAfter, ir.Dialect{})
			if err != nil {
				t.Fatalf("EncodeRequest(DecodeRequest(enc1)) failed: %v", err)
			}
			if len(lossesAfter) != 0 || !bytesEqual(enc1, encAfter) {
				t.Fatalf("Enc(Dec(enc1)) differed from enc1")
			}
			if len(testkit.ValidateMessagesRequest(data, testkit.RequestValidationOptions{StrictTopLevel: true})) == 0 {
				if !reflect.DeepEqual(req, decAfter) {
					t.Fatalf("Law Q2 violated on valid input: Dec(Enc(Dec(data))) != Dec(data)")
				}
			}
		}
	})
}
