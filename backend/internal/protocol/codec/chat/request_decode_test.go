package chat

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestDecodeRequestLegacyAndMultimodalShapes verifies Chat request decoding
// across legacy functions, function_call pairing, allowed_tools selectors, and
// nested reasoning.effort precedence. Participant message names, unmodeled
// metadata or stream_options maps, json_schema descriptions, and 64-bit vendor
// Extra members must all survive Law Q2 round trips without drift.
func TestDecodeRequestLegacyAndMultimodalShapes(t *testing.T) {
	body := []byte(`{
		"model": "gpt-4o",
		"max_tokens": 100,
		"max_completion_tokens": 512,
		"stop": "<END>",
		"reasoning_effort": "low",
		"reasoning": {"effort": "high", "summary": "concise"},
		"response_format": {
			"type": "json_schema",
			"json_schema": {
				"name": "eval_out",
				"description": "Evaluation result wrapper",
				"schema": {"type": "object", "properties": {"ans": {"type": "number"}}},
				"strict": true
			}
		},
		"user": "u_top",
		"metadata": {"team": "infra"},
		"stream_options": {"continuous_usage_stats": true},
		"session_id": "sess_1",
		"conversation_id": "conv_1",
		"custom_vendor_flag": 9007199254740993,
		"functions": [
			{
				"name": "legacy_calc",
				"description": "Evaluate an expression",
				"parameters": {"type": "object", "properties": {"expr": {"type": "string"}}},
				"type": "shadowed_legacy_type",
				"function": {"ignored": true},
				"x_fn_vendor": true
			}
		],
		"tools": [
			{"type": "web_search", "search_context_size": "medium"}
		],
		"tool_choice": {
			"type": "allowed_tools",
			"allowed_tools": {
				"mode": "required",
				"tools": [{"type": "function", "function": {"name": "legacy_calc"}}]
			}
		},
		"messages": [
			{
				"role": "user",
				"name": "alice",
				"content": [
					{"type": "input_audio", "input_audio": {"data": "UklGRg==", "format": "wav"}},
					{"type": "file", "file": {"file_id": "file_abc", "filename": "doc.pdf"}},
					{"type": "vendor_widget", "widget_id": "w_1"}
				]
			},
			{
				"role": "assistant",
				"reasoning": "Fallback reasoning text.",
				"function_call": {"name": "legacy_calc", "arguments": "{\"expr\":\"2+2\"}"}
			},
			{
				"role": "function",
				"name": "legacy_calc",
				"content": "4"
			}
		]
	}`)

	req, err := DecodeRequest(body)
	require.NoError(t, err)
	require.NotNil(t, req.MaxOutputTokens)
	require.Equal(t, int64(512), *req.MaxOutputTokens)
	require.Equal(t, []string{"<END>"}, req.Stop)
	require.NotNil(t, req.Reasoning)
	require.Equal(t, "high", req.Reasoning.Effort)
	require.Empty(t, req.Reasoning.Summary)
	require.NotNil(t, req.Output)
	require.Equal(t, "json_schema", req.Output.Kind)
	require.Equal(t, "eval_out", req.Output.Name)
	require.Equal(t, "Evaluation result wrapper", req.Output.Description)
	require.Equal(t, "u_top", req.UserID)
	require.Empty(t, req.MetadataUserID)
	require.Nil(t, req.IncludeUsage)
	require.Equal(t, "sess_1", req.SessionID)
	require.Equal(t, "conv_1", req.ConversationID)
	require.Equal(t, protocol.ProtocolOpenAIChatCompletions, req.Extra.Origin)
	require.False(t, req.Extra.IsZero())

	require.Len(t, req.Tools, 2)
	require.Equal(t, ir.ToolHosted, req.Tools[0].Kind)
	require.Equal(t, "web_search", req.Tools[0].Hosted.Type)
	require.Equal(t, ir.ToolFunction, req.Tools[1].Kind)
	require.Equal(t, "legacy_calc", req.Tools[1].Name)
	require.False(t, req.Tools[1].Extra.IsZero())

	require.NotNil(t, req.ToolChoice)
	require.Equal(t, ir.ToolChoiceRequired, req.ToolChoice.Mode)
	require.Equal(t, []ir.ToolRef{{Kind: ir.ToolFunction, Name: "legacy_calc"}}, req.ToolChoice.AllowedTools)

	// Audio, file_id document, unknown content block -> OpaqueItem, and user name -> Message.Extra.
	require.Len(t, req.Messages[0].Parts, 3)
	require.False(t, req.Messages[0].Extra.IsZero())
	require.Equal(t, ir.MediaAudio, req.Messages[0].Parts[0].Media.Type)
	require.Equal(t, "audio/wav", req.Messages[0].Parts[0].Media.MIME)
	require.Equal(t, ir.MediaDocument, req.Messages[0].Parts[1].Media.Type)
	require.Equal(t, ir.MediaSourceFileID, req.Messages[0].Parts[1].Media.Source)
	require.Equal(t, ir.PartOpaque, req.Messages[0].Parts[2].Kind)
	require.Equal(t, "vendor_widget", req.Messages[0].Parts[2].Opaque.Type)

	// Legacy function_call and role="function" share a deterministic CallID.
	callID := req.Messages[1].Parts[1].Call.CallID
	require.NotEmpty(t, callID)
	require.Equal(t, callID, req.Messages[2].Parts[0].Result.CallID)
	require.Equal(t, []string{"Fallback reasoning text."}, req.Messages[1].Parts[0].Reasoning.Segments)

	// Full Law Q2 fixed-point check on this legacy + vendor extension request!
	y, losses, err := EncodeRequest(req, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses)
	require.Contains(t, string(y), `"name":"alice"`)
	require.Contains(t, string(y), `"metadata":{"team":"infra"}`)
	require.Contains(t, string(y), `"stream_options":{"continuous_usage_stats":true}`)
	require.Contains(t, string(y), `"description":"Evaluation result wrapper"`)
	require.Contains(t, string(y), `"x_fn_vendor":true`)

	dec2, err := DecodeRequest(y)
	require.NoError(t, err)
	require.Equal(t, req, dec2)

	y2, losses2, err := EncodeRequest(dec2, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses2)
	require.Equal(t, y, y2)

	// Also verify metadata.user_id extraction when user_id is populated.
	withMetaUser, err := DecodeRequest([]byte(`{"model":"gpt-4o","metadata":{"user_id":"u_meta"},"messages":[{"role":"user","content":"hi"}]}`))
	require.NoError(t, err)
	require.Equal(t, "u_meta", withMetaUser.MetadataUserID)
}

// TestDecodeRequestUTF8ToleranceAndRejections checks invalid-UTF-8 string
// acceptance on incoming JSON payloads. Duplicate object keys, non-object roots,
// non-positive candidate counts, and negative token caps must fail decoding.
func TestDecodeRequestUTF8ToleranceAndRejections(t *testing.T) {
	invalidUTF8 := append([]byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"bad-byte-`), 0xff)
	invalidUTF8 = append(invalidUTF8, []byte(`"}]}`)...)
	req, err := DecodeRequest(invalidUTF8)
	require.NoError(t, err)
	require.NoError(t, ir.Validate(req))

	// Duplicate keys must fail decoding.
	_, err = DecodeRequest([]byte(`{"model":"gpt-4o","model":"gpt-4o-mini","messages":[{"role":"user","content":"hi"}]}`))
	require.Error(t, err)

	// Non-object JSON values including literal null must fail decoding.
	_, err = DecodeRequest([]byte(`null`))
	require.Error(t, err)
	_, err = DecodeRequest([]byte(`[]`))
	require.Error(t, err)

	// Invalid candidate count and negative max_tokens must fail decoding.
	_, err = DecodeRequest([]byte(`{"model":"gpt-4o","n":0,"messages":[{"role":"user","content":"hi"}]}`))
	require.Error(t, err)
	_, err = DecodeRequest([]byte(`{"model":"gpt-4o","max_tokens":-5,"messages":[{"role":"user","content":"hi"}]}`))
	require.Error(t, err)
	_, err = DecodeRequest([]byte(`{"model":"gpt-4o","messages":[{"role":"bogus","content":"hi"}]}`))
	require.Error(t, err)
}

// FuzzRequestDecode feeds arbitrary byte slices into DecodeRequest across wire
// and corpus seeds. Every successfully decoded request must pass ir.Validate,
// re-encode deterministically, and satisfy Law Q2 fixed-point invariants when
// lossless.
func FuzzRequestDecode(f *testing.F) {
	wireFixes, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureRequest)
	require.NoError(f, err)
	for _, fix := range wireFixes {
		f.Add(fix.Data)
	}
	corpusFixes, err := testkit.CorpusRequests(protocol.ProtocolOpenAIChatCompletions)
	require.NoError(f, err)
	for _, fix := range corpusFixes {
		f.Add(fix.Data)
	}
	f.Add([]byte(`{"model":"gpt-4o","messages":[{"role":"assistant","function_call":{"name":"f","arguments":{"a":1}}},{"role":"function","name":"f","content":"ok"}]}`))
	f.Add([]byte(`{"model":"gpt-4o","tool_choice":{"name":"f"},"functions":[{"name":"f","type":"x"}],"messages":[{"role":"user","name":"bob","content":"hi"}]}`))
	f.Add([]byte("{\"model\":\"0\",\"messages\":[{\"role\":\"user\",\"\":\"\xd7\"}]}"))

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
			if len(testkit.ValidateChatRequest(data, testkit.RequestValidationOptions{})) == 0 {
				if !reflect.DeepEqual(req, decAfter) {
					t.Fatalf("Law Q2 violated on valid input: Dec(Enc(Dec(data))) != Dec(data)")
				}
			}
		}
	})
}

// TestDecodeRequestCompatibleThinkingPreservesSameOriginRoundTrip decodes a
// Chat Completions request carrying a compatible vendor thinking object and
// re-encodes it on the same protocol. It verifies that ir.ReasoningConfig is
// populated for cross-protocol consumers while raw thinking JSON survives
// without loss across same-origin Chat round-trips.
func TestDecodeRequestCompatibleThinkingPreservesSameOriginRoundTrip(t *testing.T) {
	body := []byte(`{"model":"qwen4-max","thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hello"}]}`)
	req, err := DecodeRequest(body)
	if err != nil {
		t.Fatalf("DecodeRequest failed: %v", err)
	}
	if req.Reasoning == nil || req.Reasoning.Mode != "disabled" {
		t.Fatalf("Reasoning = %+v, want Mode=disabled", req.Reasoning)
	}
	enc, losses, err := EncodeRequest(req, ir.Dialect{})
	if err != nil {
		t.Fatalf("EncodeRequest failed: %v", err)
	}
	if len(losses) != 0 {
		t.Fatalf("unexpected losses: %v", losses)
	}
	roundTripped, err := DecodeRequest(enc)
	if err != nil {
		t.Fatalf("DecodeRequest(enc) failed: %v", err)
	}
	if !reflect.DeepEqual(req, roundTripped) {
		t.Fatalf("round-trip mismatch:\n got: %+v\nwant: %+v", roundTripped, req)
	}
}

// bytesEqual compares two byte slices without allocating diagnostic diffs on the hot fuzz path.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
