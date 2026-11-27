package responses

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestLegalizePreservesOutOfOrderFunctionCallOutputs reproduces a RoleTool turn
// whose function_call_output items appear in reverse call_id order relative to
// the preceding assistant calls. EncodeRequest must keep the original output
// order intact with zero Loss while passing strict Responses request validation.
func TestLegalizePreservesOutOfOrderFunctionCallOutputs(t *testing.T) {
	raw := []byte(`{
		"model": "gpt-5",
		"input": [
			{"type": "message", "role": "user", "content": [{"type": "input_text", "text": "run a and b"}]},
			{"type": "function_call", "id": "fc_a", "call_id": "call_a", "name": "fn_a", "arguments": "{}"},
			{"type": "function_call", "id": "fc_b", "call_id": "call_b", "name": "fn_b", "arguments": "{}"},
			{"type": "function_call_output", "id": "fco_b", "call_id": "call_b", "output": "res_b"},
			{"type": "function_call_output", "id": "fco_a", "call_id": "call_a", "output": "res_a"}
		]
	}`)

	req, err := DecodeRequest(raw)
	require.NoError(t, err)
	require.Len(t, req.Messages[2].Parts, 2)
	require.Equal(t, "call_b", req.Messages[2].Parts[0].Result.CallID)
	require.Equal(t, "call_a", req.Messages[2].Parts[1].Result.CallID)

	encoded, losses, err := EncodeRequest(req, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses)
	require.Empty(t, testkit.ValidateResponsesRequest(encoded, testkit.RequestValidationOptions{}))

	redecoded, err := DecodeRequest(encoded)
	require.NoError(t, err)
	require.Equal(t, req, redecoded)
}

// TestLegalizeRepairsOrphansRewritesDuplicateCallIDsAndMovesReasoning exercises
// orphan calls and outputs, multi-turn duplicate call_ids, multi-block system
// instructions, and trailing or followerless reasoning items. EncodeRequest must
// produce a valid Responses payload whose reported Losses reconcile every change under VerifyEssence.
func TestLegalizeRepairsOrphansRewritesDuplicateCallIDsAndMovesReasoning(t *testing.T) {
	src := &ir.Request{
		Model: "gpt-5",
		System: []ir.Part{
			{Kind: ir.PartText, Text: ""},
			{Kind: ir.PartText, Text: "Rule 1", Cache: &ir.CacheMark{TTL: "5m"}},
			{Kind: ir.PartText, Text: "Rule 2"},
		},
		Messages: []ir.Message{
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "turn 1"}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolCall,
						ID:   "fc_1",
						Call: &ir.ToolCall{CallID: "dup_call", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "ping"}, Arguments: `{}`},
					},
					{
						Kind: ir.PartToolCall,
						ID:   "fc_orphan",
						Call: &ir.ToolCall{CallID: "unmatched_call", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "skip"}, Arguments: `{}`},
					},
					{
						Kind:      ir.PartReasoning,
						ID:        "rs_trailing",
						Reasoning: &ir.Reasoning{Segments: []string{"thought after call"}, Summary: true},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "dup_call", Content: []ir.Part{{Kind: ir.PartText, Text: "ok1"}}}},
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "orphan_out", Content: []ir.Part{{Kind: ir.PartText, Text: "stale"}}}},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolCall,
						ID:   "fc_2",
						Call: &ir.ToolCall{CallID: "dup_call", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "ping"}, Arguments: `{}`},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "dup_call", Content: []ir.Part{{Kind: ir.PartText, Text: "ok2"}}}},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{Kind: ir.PartReasoning, ID: "rs_dangling", Reasoning: &ir.Reasoning{Segments: []string{"no follower"}}},
				},
			},
		},
	}

	encoded, losses, err := EncodeRequest(src, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateResponsesRequest(encoded, testkit.RequestValidationOptions{}))
	require.Contains(t, losses, ir.Loss{Kind: ir.LossPartsReordered, Path: "messages[1]"})
	require.Contains(t, losses, ir.Loss{Kind: ir.LossSystemRelocated, Path: "system"})
	require.Contains(t, losses, ir.Loss{Kind: ir.LossClientMarkerDropped, Path: "system[0]"})

	dst, err := DecodeRequest(encoded)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(src), ir.Essence(dst), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}
