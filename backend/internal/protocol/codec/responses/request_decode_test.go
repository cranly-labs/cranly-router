package responses

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestDecodeRequestShorthandsAdditionalToolsAndSelectors exercises string and
// role-only message shorthands, additional_tools lifting, conversation bindings,
// fast service_tier normalization, and allowed_tools selectors. Malformed
// service_tier labels, duplicate object keys, and missing input fields must return errors.
func TestDecodeRequestShorthandsAdditionalToolsAndSelectors(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-5",
		"instructions": "System header",
		"service_tier": "fast",
		"previous_response_id": "resp_prev_1",
		"store": true,
		"conversation": "conv_abc",
		"prompt_cache_key": "pck_1",
		"session_id": "sess_1",
		"user": "u_direct",
		"tool_choice": {
			"type": "allowed_tools",
			"mode": "required",
			"tools": [
				{"type": "function", "namespace": "ns1", "name": "fn1"},
				{"type": "custom", "name": "apply_patch"},
				{"type": "tool_search"}
			]
		},
		"input": [
			{"role": "user", "content": "shorthand user text"},
			{
				"type": "additional_tools",
				"tools": [
					{
						"type": "namespace",
						"name": "ns1",
						"tools": [{"type": "function", "name": "fn1", "parameters": {"type": "object"}}]
					}
				]
			}
		]
	}`)

	req, err := DecodeRequest(raw)
	require.NoError(t, err)
	require.NoError(t, ir.Validate(req))
	require.Equal(t, "priority", req.ServiceTier)
	require.Equal(t, "resp_prev_1", req.PreviousResponseID)
	require.NotNil(t, req.Store)
	require.True(t, *req.Store)
	require.True(t, req.ConversationSet)
	require.Equal(t, "conv_abc", req.ConversationID)
	require.Equal(t, "pck_1", req.CacheKey)
	require.Equal(t, "sess_1", req.SessionID)
	require.Equal(t, "u_direct", req.UserID)
	require.Len(t, req.Namespaces, 1)
	require.Equal(t, "ns1", req.Namespaces[0].Name)
	require.Len(t, req.Tools, 1)
	require.Equal(t, "ns1", req.Tools[0].Namespace)
	require.Equal(t, "fn1", req.Tools[0].Name)
	require.NotNil(t, req.ToolChoice)
	require.Equal(t, ir.ToolChoiceRequired, req.ToolChoice.Mode)
	require.Len(t, req.ToolChoice.AllowedTools, 3)

	// Invalid service_tier values, duplicate keys, or missing input must fail DecodeRequest.
	_, err = DecodeRequest([]byte(`{"model":"gpt-5","service_tier":"bogus","input":"hi"}`))
	require.Error(t, err)
	_, err = DecodeRequest([]byte(`{"model":"gpt-5","model":"gpt-4","input":"hi"}`))
	require.Error(t, err)
	_, err = DecodeRequest([]byte(`{"model":"gpt-5"}`))
	require.Error(t, err)
}

// TestDecodeRequestInvalidUTF8RoundTrip feeds raw 0xff invalid UTF-8 bytes
// inside instructions and user input_text JSON strings into DecodeRequest.
// AllowInvalidUTF8 normalizes invalid bytes to U+FFFD without failing decoding,
// and lossless EncodeRequest round-tripping preserves the decoded IR identically.
func TestDecodeRequestInvalidUTF8RoundTrip(t *testing.T) {
	body := []byte("{\"model\":\"gpt-5\",\"instructions\":\"sys-\xff\",\"input\":[{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"hi-\xff-end\"}]}]}")
	dec1, err := DecodeRequest(body)
	require.NoError(t, err)
	require.NoError(t, ir.Validate(dec1))
	require.Equal(t, "sys-\ufffd", dec1.System[0].Text)
	require.Equal(t, "hi-\ufffd-end", dec1.Messages[0].Parts[0].Text)

	enc, losses, err := EncodeRequest(dec1, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses)

	dec2, err := DecodeRequest(enc)
	require.NoError(t, err)
	require.Equal(t, dec1, dec2)
}

// TestDecodeRequestEdgeCasesAndRoundTripInvariants exercises followerless empty
// reasoning items, empty assistant turns, additional_tools-only inputs, dual
// summary/content reasoning items, empty allowed_tools selectors, and non-compact
// tool_search_output strings. Every valid case must reconcile under VerifyEssence
// and satisfy Law Q2 when lossless, while blank annotation types return an error.
func TestDecodeRequestEdgeCasesAndRoundTripInvariants(t *testing.T) {
	// Followerless empty reasoning must record LossReasoningTextDropped and reconcile cleanly.
	emptyRs := []byte(`{"model":"0","input":[{"type":"reasoning"}]}`)
	decRs, err := DecodeRequest(emptyRs)
	require.NoError(t, err)
	encRs, lossesRs, err := EncodeRequest(decRs, ir.Dialect{})
	require.NoError(t, err)
	require.Contains(t, collectLossKindsSlice(lossesRs), ir.LossReasoningTextDropped)
	decRs2, err := DecodeRequest(encRs)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(decRs), ir.Essence(decRs2), lossesRs)
	require.NoError(t, err)
	require.Empty(t, diffs)

	// Zero-Loss Q2 round-trip cases.
	q2Inputs := [][]byte{
		[]byte(`{"model":"0","input":[{"type":"message","role":"assistant"}]}`),
		[]byte(`{"model":"0","input":[{"type":"additional_tools","tools":[{"type":"function","name":"f"}]}]}`),
		[]byte(`{"model":"0","input":[{"type":"message","role":"assistant","extra_m":1,"content":[{"type":"output_text","id":"blk_1","text":"hi"}]}]}`),
		[]byte(`{"model":"0","input":[{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"s1"}],"content":[{"type":"reasoning_text","text":"c1"}]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`),
		[]byte(`{"model":"0","tool_choice":{"type":"allowed_tools","mode":"auto","tools":[],"extra_tc":1},"extra_top":2,"input":"hi"}`),
		[]byte(`{"model":"0","input":[{"type":"tool_search_call","call_id":"c1"},{"type":"tool_search_output","call_id":"c1","output":"[1, 2]"}]}`),
	}
	for _, raw := range q2Inputs {
		d1, decErr := DecodeRequest(raw)
		require.NoError(t, decErr, string(raw))
		e1, ls1, encErr := EncodeRequest(d1, ir.Dialect{})
		require.NoError(t, encErr, string(raw))
		require.Empty(t, ls1, string(raw))
		d2, decErr2 := DecodeRequest(e1)
		require.NoError(t, decErr2, string(raw))
		require.Equal(t, d1, d2, string(raw))
		e2, ls2, encErr2 := EncodeRequest(d2, ir.Dialect{})
		require.NoError(t, encErr2, string(raw))
		require.Empty(t, ls2)
		require.Equal(t, e1, e2, string(raw))
	}

	// ToolResult.Tool must remain zero before ir.Link runs per section 3.3.
	customOutReq, err := DecodeRequest([]byte(`{"model":"0","input":[{"type":"custom_tool_call","call_id":"c1","name":"p","input":"x"},{"type":"custom_tool_call_output","call_id":"c1","output":"ok"}]}`))
	require.NoError(t, err)
	require.Equal(t, ir.ToolRef{}, customOutReq.Messages[1].Parts[0].Result.Tool)

	// Blank annotation.type must fail DecodeRequest.
	_, err = DecodeRequest([]byte(`{"model":"0","input":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi","annotations":[{"type":""}]}]}]}`))
	require.Error(t, err)
}

// collectLossKindsSlice extracts LossKind values from losses in order.
func collectLossKindsSlice(losses []ir.Loss) []ir.LossKind {
	out := make([]ir.LossKind, len(losses))
	for i, l := range losses {
		out[i] = l.Kind
	}
	return out
}

// FuzzRequestDecode feeds arbitrary and corpus-seeded JSON bytes into
// DecodeRequest without panicking. Accepted requests must pass ir.Validate,
// encode into valid Responses JSON, reconcile under VerifyEssence, and satisfy
// Law Q2 fixed-point equality whenever Loss is empty.
func FuzzRequestDecode(f *testing.F) {
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureRequest)
	require.NoError(f, err)
	for _, fix := range wireReqs {
		f.Add(fix.Data)
	}
	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolOpenAIResponses)
	require.NoError(f, err)
	for _, fix := range corpusReqs {
		f.Add(fix.Data)
	}
	f.Add([]byte(`{"model":"gpt-5","input":"short string input"}`))
	f.Add([]byte(`{"model":"gpt-5","conversation":{},"reasoning":{},"include":[],"text":{},"metadata":{},"input":[]}`))
	f.Add([]byte(`{"model":"0","input":[{"type":"\b"},{"type":"function_call_output"},{"type":"0"}]}`))
	f.Add([]byte(`{"model":"0","input":[{"type":"reasoning"}]}`))
	f.Add([]byte(`{"model":"0","input":[{"type":"message","role":"assistant"}]}`))
	f.Add([]byte(`{"model":"0","input":[{"type":"additional_tools","tools":[{"type":"function","name":"f"}]}]}`))
	f.Add([]byte(`{"model":"0","input":[{"type":"message","role":"assistant","extra_m":1,"content":[{"type":"output_text","id":"blk_1","text":"hi"}]}]}`))
	f.Add([]byte(`{"model":"0","tool_choice":{"type":"allowed_tools","mode":"auto","tools":[],"extra_tc":1},"extra_top":2,"input":"hi"}`))
	f.Add([]byte(`{"model":"0","input":[{"type":"reasoning","encrypted_content":"trc1:plain:eyJ0Ijoi0CJ9"}]}`))
	f.Add([]byte(`{"model":"0","input":[{"type":"function_call","name":"0"},{"type":"function_call_output"}]}`))
	f.Add([]byte("{\"model\":\"0\",\"input\":\"\xff\"}"))
	f.Add([]byte(`{"model":"0","tools":[{"type":"namespace","name":"0"},{"type":"namespace","name":"1","tools":[{"name":"0"}]}],"input":[]}`))
	f.Add([]byte(`{"model":"0","input":[{"type":"tool_search_call","call_id":"c1","arguments":"\"\""},{"type":"tool_search_output","call_id":"c1","output":"[]"}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		dec1, err := DecodeRequest(data)
		if err != nil {
			return
		}
		require.NoError(t, ir.Validate(dec1))

		enc1, losses, encErr := EncodeRequest(dec1, ir.Dialect{})
		if encErr != nil {
			return
		}
		violations := testkit.ValidateResponsesRequest(enc1, testkit.RequestValidationOptions{})
		require.Empty(t, violations, "ValidateResponsesRequest failed: %v", violations)

		dec2, err := DecodeRequest(enc1)
		require.NoError(t, err)
		require.NoError(t, ir.Validate(dec2))

		diffs, vErr := testkit.VerifyEssence(ir.Essence(dec1), ir.Essence(dec2), losses)
		require.NoError(t, vErr)
		require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

		if len(losses) == 0 {
			require.Equal(t, dec1, dec2, "zero-Loss request must satisfy Dec(Enc(Dec(x))) == Dec(x)")
			enc2, losses2, err := EncodeRequest(dec2, ir.Dialect{})
			require.NoError(t, err)
			require.Empty(t, losses2)
			require.True(t, bytes.Equal(enc1, enc2), "zero-Loss request must satisfy Enc(Dec(enc1)) == enc1")
		}
	})
}
