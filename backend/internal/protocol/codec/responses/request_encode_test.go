package responses

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestEncodeRequestFunctionOnlyDialectLowering encodes every Responses wire
// request fixture (including custom, namespace, tool_search, and hosted tools)
// under ToolSupportFunction. Every encoded payload must pass strict function-only
// request validation and reconcile cleanly with its source EssenceDoc via VerifyEssence.
func TestEncodeRequestFunctionOnlyDialectLowering(t *testing.T) {
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureRequest)
	require.NoError(t, err)

	fnOnlyDialect := ir.Dialect{
		Tools:            ir.ToolSupportFunction,
		ToolNameMaxBytes: 64,
	}

	for _, fix := range wireReqs {
		t.Run(fix.Name, func(t *testing.T) {
			src, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)

			encoded, losses, encErr := EncodeRequest(src, fnOnlyDialect)
			require.NoError(t, encErr)

			violations := testkit.ValidateResponsesRequest(encoded, testkit.RequestValidationOptions{
				DialectRules:     []string{"function-only"},
				ToolNameMaxBytes: 64,
			})
			require.Empty(t, violations, "function-only violations on %s: %v", fix.Name, violations)

			dst, redecErr := DecodeRequest(encoded)
			require.NoError(t, redecErr)

			diffs, vErr := testkit.VerifyEssence(ir.Essence(src), ir.Essence(dst), losses)
			require.NoError(t, vErr)
			require.Empty(t, diffs, "VerifyEssence diff on %s:\n%s", fix.Name, testkit.FormatJSONDiff(diffs))
		})
	}
}

// TestEncodeRequestSamplingStopEffortAndSubObjectExtras exercises sub-object
// vendor fields on reasoning, include, text, metadata, and conversation alongside
// dialect sampling/stop drops and unsupported "ultra" effort values. Same-origin
// sub-object extras must round-trip with zero Loss, while unsupported controls fail or emit Loss.
func TestEncodeRequestSamplingStopEffortAndSubObjectExtras(t *testing.T) {
	rawWithSubExtras := []byte(`{
		"model": "gpt-5",
		"service_tier": "ultrafast",
		"conversation": {"id": "conv_99", "custom_tag": "alpha"},
		"reasoning": {"effort": "high", "summary": "detailed", "vendor_hint": 1},
		"include": ["reasoning.encrypted_content", "file_search_call.results"],
		"text": {"verbosity": "low", "format": {"type": "json_object", "fmt_ext": true}, "txt_ext": "v"},
		"metadata": {"user_id": "u_1", "trace_id": "t_2"},
		"input": "hello"
	}`)

	dec1, err := DecodeRequest(rawWithSubExtras)
	require.NoError(t, err)
	require.Equal(t, "ultrafast", dec1.ServiceTier)
	require.True(t, dec1.ConversationSet)
	require.Equal(t, "conv_99", dec1.ConversationID)

	enc1, losses, err := EncodeRequest(dec1, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses)

	dec2, err := DecodeRequest(enc1)
	require.NoError(t, err)
	require.Equal(t, dec1, dec2)

	// Unsupported "ultra" effort must fail request encoding.
	ultraReq := dec1.Clone()
	ultraReq.Reasoning.Effort = "ultra"
	_, _, err = EncodeRequest(ultraReq, ir.Dialect{})
	require.Error(t, err)

	// Sampling and stop fields unsupported by Responses or stripped by Dialect must emit Losses.
	temp := 0.5
	topK := int64(20)
	seed := int64(42)
	cand := int64(2)
	dropReq := &ir.Request{
		Model:    "gpt-5",
		Messages: []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "hi"}}}},
		Sampling: ir.Sampling{Temperature: &temp, TopK: &topK, Seed: &seed, CandidateCount: &cand},
		Stop:     []string{"END"},
	}
	encDrop, dropLosses, err := EncodeRequest(dropReq, ir.Dialect{DropTemperature: true})
	require.NoError(t, err)
	dstDrop, err := DecodeRequest(encDrop)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(dropReq), ir.Essence(dstDrop), dropLosses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}

// TestEncodeRequestNamespaceCacheAndForeignExtraLosses encodes empty and
// populated ir.Namespace entries carrying Cache breakpoints and foreign Extra
// members alongside root Request.Extra. Emitted losses must address exact
// namespaces[i] paths and reconcile cleanly under VerifyEssence without
// clobbering root or tool Extra flags.
func TestEncodeRequestNamespaceCacheAndForeignExtraLosses(t *testing.T) {
	req := &ir.Request{
		Model:    "gpt-5",
		Messages: []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "ping"}}}},
		Tools: []ir.Tool{
			{
				Kind:        ir.ToolFunction,
				Namespace:   "ns_pop",
				Name:        "lookup",
				InputSchema: jsontext.Value(`{"type":"object"}`),
				Extra: ir.Extra{
					Origin:  protocol.ProtocolOpenAIResponses,
					Members: map[string]jsontext.Value{"x_tool": jsontext.Value("1")},
				},
			},
		},
		Namespaces: []ir.Namespace{
			{
				Name:  "ns_empty_lead",
				Cache: &ir.CacheMark{TTL: "5m"},
				Extra: ir.Extra{
					Origin:  protocol.ProtocolAnthropicMessages,
					Members: map[string]jsontext.Value{"x_lead": jsontext.Value("true")},
				},
			},
			{
				Name:  "ns_pop",
				Cache: &ir.CacheMark{TTL: "1h"},
				Extra: ir.Extra{
					Origin:  protocol.ProtocolAnthropicMessages,
					Members: map[string]jsontext.Value{"x_pop": jsontext.Value("true")},
				},
			},
			{
				Name:  "ns_empty_tail",
				Cache: &ir.CacheMark{TTL: "5m"},
				Extra: ir.Extra{
					Origin:  protocol.ProtocolAnthropicMessages,
					Members: map[string]jsontext.Value{"x_tail": jsontext.Value("true")},
				},
			},
		},
		Extra: ir.Extra{
			Origin:  protocol.ProtocolOpenAIResponses,
			Members: map[string]jsontext.Value{"x_root": jsontext.Value(`"kept"`)},
		},
	}

	encoded, losses, err := EncodeRequest(req, ir.Dialect{})
	require.NoError(t, err)
	require.Equal(t, []ir.Loss{
		{Kind: ir.LossCacheMarkDropped, Path: "namespaces[0]"},
		{Kind: ir.LossExtraDropped, Path: "namespaces[0]"},
		{Kind: ir.LossCacheMarkDropped, Path: "namespaces[1]"},
		{Kind: ir.LossExtraDropped, Path: "namespaces[1]"},
		{Kind: ir.LossCacheMarkDropped, Path: "namespaces[2]"},
		{Kind: ir.LossExtraDropped, Path: "namespaces[2]"},
	}, losses)

	redecoded, err := DecodeRequest(encoded)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(redecoded), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}
