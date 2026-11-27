package messages

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestLegalizeRequestMessagesPairingSystemAndThinkingOrdering verifies mid-turn
// developer relocation, consecutive turn merging, orphan and unanswered tool
// pairing repair, thinking-before-tool_use reordering, and tool_result image retention.
// Active unsigned tool loops must omit top-level thinking on strict dialects and
// reconcile every semantic difference under testkit.VerifyEssence.
func TestLegalizeRequestMessagesPairingSystemAndThinkingOrdering(t *testing.T) {
	budget := int64(2048)
	maxOut := int64(8192)
	req := &ir.Request{
		Model:           "claude-sonnet-4-5",
		MaxOutputTokens: &maxOut,
		Reasoning:       &ir.ReasoningConfig{Mode: "enabled", BudgetTokens: &budget},
		System:          []ir.Part{{Kind: ir.PartText, Text: "Lead rule."}},
		Tools: []ir.Tool{{
			Kind:        ir.ToolFunction,
			Name:        "fetch_chart",
			InputSchema: jsontext.Value(`{"type":"object","properties":{"id":{"type":"string"}}}`),
		}},
		Messages: []ir.Message{
			{
				Role:  ir.RoleUser,
				ID:    "msg_u_0",
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Start check."}},
			},
			{
				Role:  ir.RoleDeveloper,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Mid-turn guardrail."}},
			},
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaAudio, Source: ir.MediaSourceBase64, MIME: "audio/wav", Data: "UklGRg=="}}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_good",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fetch_chart"},
							Arguments: `{"id":"c1"}`,
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_unanswered",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fetch_chart"},
							Arguments: `{"id":"c2"}`,
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_bad_json",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fetch_chart"},
							Arguments: `not_json`,
						},
					},
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Segments: []string{"Late signed thought moved ahead of tool_use."},
							Token:    &ir.ReplayToken{Data: "EqQSignedTurn1"},
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolResult,
						Result: &ir.ToolResult{
							CallID: "call_good",
							Content: []ir.Part{
								{Kind: ir.PartText, Text: "final chart ready"},
								{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, Source: ir.MediaSourceBase64, MIME: "image/png", Data: testkit.SyntheticPNGDataBase64}},
							},
						},
					},
					{
						Kind:   ir.PartToolResult,
						Result: &ir.ToolResult{CallID: "call_bad_json", Content: []ir.Part{{Kind: ir.PartText, Text: "dropped with bad call"}}},
					},
					{
						Kind:   ir.PartToolResult,
						Result: &ir.ToolResult{CallID: "call_orphan", Content: []ir.Part{{Kind: ir.PartText, Text: "orphan result"}}},
					},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Segments: []string{"Unsigned active tool-loop thought."},
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_loop_final",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fetch_chart"},
							Arguments: `{"id":"c3"}`,
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind:   ir.PartToolResult,
					Result: &ir.ToolResult{CallID: "call_loop_final", Content: []ir.Part{{Kind: ir.PartText, Text: "done"}}},
				}},
			},
		},
		Extra: ir.Extra{
			Origin:  protocol.ProtocolOpenAIResponses,
			Members: map[string]jsontext.Value{"store": jsontext.Value("false")},
		},
	}

	// 1. Strict Anthropic dialect drops unsigned thinking, omits top-level thinking
	// for the active unsigned tool loop, passes strict validation, and passes VerifyEssence.
	strictBytes, strictLosses, err := EncodeRequest(req, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(strictBytes, testkit.RequestValidationOptions{StrictTopLevel: true}))
	require.NotContains(t, string(strictBytes), `"thinking":{"type":"enabled"`)
	require.Contains(t, string(strictBytes), `"final chart ready"`)

	strictDecoded, err := DecodeRequest(strictBytes)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(strictDecoded), strictLosses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

	// 2. Passback dialect retains unsigned thinking on the final tool-calling assistant turn.
	passBytes, passLosses, err := EncodeRequest(req, ir.Dialect{ReasoningHistory: ir.HistoryToolTurns})
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(passBytes, testkit.RequestValidationOptions{
		StrictTopLevel: true,
		DialectRules:   []string{"passback"},
	}))
	require.Contains(t, string(passBytes), `"Unsigned active tool-loop thought."`)

	passDecoded, err := DecodeRequest(passBytes)
	require.NoError(t, err)
	passDiffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(passDecoded), passLosses)
	require.NoError(t, err)
	require.Empty(t, passDiffs, testkit.FormatJSONDiff(passDiffs))
}

// TestLegalizeDuplicateToolResultLastWins reproduces a tool turn carrying two
// tool_result items correlated to the same assistant CallID. Legalization must
// keep the final result, prune the earlier duplicate with LossToolPairingRepaired,
// and pass strict ValidateMessagesRequest checks.
func TestLegalizeDuplicateToolResultLastWins(t *testing.T) {
	req := &ir.Request{
		Model: "claude-sonnet-4-5",
		Tools: []ir.Tool{{
			Kind:        ir.ToolFunction,
			Name:        "fetch_chart",
			InputSchema: jsontext.Value(`{"type":"object"}`),
		}},
		Messages: []ir.Message{
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Fetch chart."}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{
						CallID:    "call_dup_res",
						Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fetch_chart"},
						Arguments: `{}`,
					},
				}},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{
					{
						Kind:   ir.PartToolResult,
						Result: &ir.ToolResult{CallID: "call_dup_res", Content: []ir.Part{{Kind: ir.PartText, Text: "stale first duplicate"}}},
					},
					{
						Kind:   ir.PartToolResult,
						Result: &ir.ToolResult{CallID: "call_dup_res", Content: []ir.Part{{Kind: ir.PartText, Text: "authoritative final result"}}},
					},
				},
			},
		},
	}

	encoded, losses, err := EncodeRequest(req, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(encoded, testkit.RequestValidationOptions{StrictTopLevel: true}))
	require.Contains(t, string(encoded), `"authoritative final result"`)
	require.NotContains(t, string(encoded), `"stale first duplicate"`)
	require.Equal(t, []ir.Loss{{
		Kind:   ir.LossToolPairingRepaired,
		Path:   "messages[2].parts[0]",
		Detail: "duplicate_tool_result",
	}}, losses)
}

// TestLegalizeParallelOutOfOrderToolResultsCrossTurnAndEmptyTextExtra checks
// out-of-order parallel tool_result retention, cross-turn tool_result relocation
// reporting, and empty text blocks carrying Extra metadata. Valid out-of-order
// parallel tool_result items must round-trip with zero Loss under Law Q2 and Q4,
// while cross-user relocation and dropped empty text blocks must report exact Losses.
func TestLegalizeParallelOutOfOrderToolResultsCrossTurnAndEmptyTextExtra(t *testing.T) {
	// 1. Parallel tool_result items in completion order [t2, t1] must keep [t2, t1] with zero Loss.
	parallelBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"tools":[{"name":"fn_a","input_schema":{"type":"object"}},{"name":"fn_b","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"run"},{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"fn_a","input":{}},{"type":"tool_use","id":"t2","name":"fn_b","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"t2","content":"r2"},{"type":"tool_result","tool_use_id":"t1","content":"r1"}]}]}`)
	require.Empty(t, testkit.ValidateMessagesRequest(parallelBody, testkit.RequestValidationOptions{StrictTopLevel: true}))

	dec1, err := DecodeRequest(parallelBody)
	require.NoError(t, err)
	require.Equal(t, "t2", dec1.Messages[2].Parts[0].Result.CallID)
	require.Equal(t, "t1", dec1.Messages[2].Parts[1].Result.CallID)

	enc1, losses1, err := EncodeRequest(dec1, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses1, "out-of-order parallel tool_result must incur zero Loss")
	require.Empty(t, testkit.ValidateMessagesRequest(enc1, testkit.RequestValidationOptions{StrictTopLevel: true}))

	dec2, err := DecodeRequest(enc1)
	require.NoError(t, err)
	require.Equal(t, dec1, dec2, "Dec(Enc(Dec(x))) must preserve [t2, t1] order")

	enc2, losses2, err := EncodeRequest(dec2, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, losses2)
	require.Equal(t, string(enc1), string(enc2))

	essDiffs, err := testkit.VerifyEssence(ir.Essence(dec1), ir.Essence(dec2), losses1)
	require.NoError(t, err)
	require.Empty(t, essDiffs, testkit.FormatJSONDiff(essDiffs))

	// 2. [RoleAssistant(tool_call t1), RoleUser("interjection"), RoleTool(tool_result t1)]
	// moves tool_result ahead of the intervening user turn and reports LossPartsReordered.
	crossTurnReq := &ir.Request{
		Model: "claude-sonnet-4-5",
		Tools: []ir.Tool{{Kind: ir.ToolFunction, Name: "fn_a", InputSchema: jsontext.Value(`{"type":"object"}`)}},
		Messages: []ir.Message{
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{CallID: "t1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_a"}, Arguments: `{}`},
				}},
			},
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "interjection"}},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind:   ir.PartToolResult,
					Result: &ir.ToolResult{CallID: "t1", Content: []ir.Part{{Kind: ir.PartText, Text: "r1"}}},
				}},
			},
		},
	}
	crossBytes, crossLosses, err := EncodeRequest(crossTurnReq, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(crossBytes, testkit.RequestValidationOptions{StrictTopLevel: true}))
	require.Equal(t, []ir.Loss{{
		Kind: ir.LossPartsReordered,
		Path: "messages[2]",
	}}, crossLosses)

	// 3. Empty text blocks carrying same-origin Extra across system, messages, and
	// tool_result.content drop cleanly with LossExtraDropped + LossClientMarkerDropped.
	sameOriginExtra := ir.Extra{
		Origin:  protocol.ProtocolAnthropicMessages,
		Members: map[string]jsontext.Value{"x_note": jsontext.Value(`"meta"`)},
	}
	emptyExtraReq := &ir.Request{
		Model: "claude-sonnet-4-5",
		System: []ir.Part{
			{Kind: ir.PartText, Text: "", Cache: &ir.CacheMark{TTL: "5m"}, Extra: sameOriginExtra.Clone()},
			{Kind: ir.PartText, Text: "Keep system."},
		},
		Tools: []ir.Tool{{Kind: ir.ToolFunction, Name: "fn_a", InputSchema: jsontext.Value(`{"type":"object"}`)}},
		Messages: []ir.Message{
			{
				Role: ir.RoleUser,
				Parts: []ir.Part{
					{Kind: ir.PartText, Text: "", Extra: sameOriginExtra.Clone()},
					{Kind: ir.PartText, Text: "Hello."},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{Kind: ir.PartText, Text: "", Extra: sameOriginExtra.Clone()},
					{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "t_emp", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_a"}, Arguments: `{}`}},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID: "t_emp",
						Content: []ir.Part{
							{Kind: ir.PartText, Text: "", Extra: sameOriginExtra.Clone()},
							{Kind: ir.PartText, Text: "Result text."},
						},
					},
				}},
			},
		},
	}

	empEncoded, empLosses, err := EncodeRequest(emptyExtraReq, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(empEncoded, testkit.RequestValidationOptions{StrictTopLevel: true}))

	empDecoded, err := DecodeRequest(empEncoded)
	require.NoError(t, err)
	empDiffs, err := testkit.VerifyEssence(ir.Essence(emptyExtraReq), ir.Essence(empDecoded), empLosses)
	require.NoError(t, err)
	require.Empty(t, empDiffs, testkit.FormatJSONDiff(empDiffs))
}
