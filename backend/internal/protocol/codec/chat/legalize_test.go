package chat

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestLegalizeToolPairingMediaRelocationAndEssence verifies System relocation,
// malformed JSON tool call pruning, orphan removal, reused CallIDs across turns,
// and tool_result image relocation. Encoded output must pass ValidateChatRequest
// and reconcile every semantic difference under Law Q4 VerifyEssence.
func TestLegalizeToolPairingMediaRelocationAndEssence(t *testing.T) {
	src := &ir.Request{
		Model: "gpt-4o",
		System: []ir.Part{
			{Kind: ir.PartText, Text: "Follow safety invariants.", Cache: &ir.CacheMark{TTL: "5m"}},
		},
		Messages: []ir.Message{
			{
				Role:  ir.RoleUser,
				ID:    "msg_u1",
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Inspect charts and logs."}},
			},
			// Turn 1: reused short CallID "c1" with IsError=true result, plus an invalid-JSON call "c_bad".
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "c1",
							Tool:      ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"},
							Arguments: "*** Begin Patch\n*** End Patch",
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "c_bad",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "broken"},
							Arguments: "{not valid json",
						},
					},
					{
						Kind: ir.PartText,
						Text: "Running first batch.",
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolResult,
						Result: &ir.ToolResult{
							CallID:  "c1",
							Tool:    ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"},
							IsError: true,
							Content: []ir.Part{{Kind: ir.PartText, Text: "patch target missing"}},
						},
					},
					{
						Kind: ir.PartToolResult,
						Result: &ir.ToolResult{
							CallID:  "c_bad",
							Content: []ir.Part{{Kind: ir.PartText, Text: "ignored"}},
						},
					},
					{
						Kind: ir.PartToolResult,
						Result: &ir.ToolResult{
							CallID:  "c_orphan",
							Content: []ir.Part{{Kind: ir.PartText, Text: "stale orphan"}},
						},
					},
				},
			},
			// Turn 2: reuses short CallID "c1" and returns an image inside tool_result!
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{
						CallID:    "c1",
						Tool:      ir.ToolRef{Kind: ir.ToolFunction, Namespace: "enterprise_observability_telemetry_connector", Name: "capture_dashboard_panel_high_resolution_png"},
						Arguments: `{"panel":"p99"}`,
					},
				}},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID: "c1",
						Tool:   ir.ToolRef{Kind: ir.ToolFunction, Namespace: "enterprise_observability_telemetry_connector", Name: "capture_dashboard_panel_high_resolution_png"},
						Content: []ir.Part{
							{Kind: ir.PartText, Text: "Captured p99."},
							{
								Kind: ir.PartMedia,
								Media: &ir.Media{
									Type:   ir.MediaImage,
									MIME:   "image/png",
									Source: ir.MediaSourceBase64,
									Data:   testkit.SyntheticPNGDataBase64,
								},
							},
						},
					},
				}},
			},
			// Assistant turn containing only a foreign OpaqueItem -> pruned cleanly!
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartOpaque,
					Opaque: &ir.OpaqueItem{
						Origin: protocol.ProtocolOpenAIResponses,
						Type:   "compaction",
						Raw:    jsontext.Value(`{"type":"compaction"}`),
					},
				}},
			},
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Summarize both results."}},
			},
			// Turn 3: tool_result containing ONLY an image without accompanying text!
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{
						CallID:    "c_img_only",
						Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "snap_raw"},
						Arguments: `{}`,
					},
				}},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID: "c_img_only",
						Tool:   ir.ToolRef{Kind: ir.ToolFunction, Name: "snap_raw"},
						Content: []ir.Part{{
							Kind: ir.PartMedia,
							Media: &ir.Media{
								Type:   ir.MediaImage,
								MIME:   "image/png",
								Source: ir.MediaSourceBase64,
								Data:   testkit.SyntheticPNGDataBase64,
							},
						}},
					},
				}},
			},
		},
		Tools: []ir.Tool{
			{
				Kind:   ir.ToolHosted,
				Name:   "web_search",
				Hosted: &ir.OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: "web_search", Raw: jsontext.Value(`{"type":"web_search"}`)},
			},
			{
				Kind:        ir.ToolCustom,
				Name:        "apply_patch",
				Description: "Patch workspace files.",
				Format:      jsontext.Value(`{"type":"grammar"}`),
			},
			{
				Kind:        ir.ToolFunction,
				Namespace:   "enterprise_observability_telemetry_connector",
				Name:        "capture_dashboard_panel_high_resolution_png",
				Description: "Capture panel.",
				InputSchema: jsontext.Value(`{"type":"object"}`),
			},
			{
				Kind:        ir.ToolFunction,
				Name:        "snap_raw",
				Description: "Capture raw image.",
				InputSchema: jsontext.Value(`{"type":"object"}`),
			},
		},
	}

	encoded, losses, err := EncodeRequest(src, ir.Dialect{})
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"Captured p99.\n[Tool output media moved to the following user message]"`)
	require.Contains(t, string(encoded), `"content":"[Tool output media moved to the following user message]"`)

	// Encoded output must pass strict Chat request rules with zero violations.
	violations := testkit.ValidateChatRequest(encoded, testkit.RequestValidationOptions{})
	require.Empty(t, violations, "ValidateChatRequest violations: %v", violations)

	// Decoding back and running VerifyEssence must reconcile every change!
	dst, err := DecodeRequest(encoded)
	require.NoError(t, err)
	require.True(t, dst.Messages[3].Parts[0].Result.IsError, "IsError envelope must survive round trip")
	require.Equal(t, "patch target missing", dst.Messages[3].Parts[0].Result.Content[0].Text)

	diffs, err := testkit.VerifyEssence(ir.Essence(src), ir.Essence(dst), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}

// TestLegalizeCoalescesSplitAssistantReasoningCommentaryAndToolCall reproduces
// a Turn 2 OpenAI Responses replay where reasoning, status-annotated assistant
// commentary, and a function_call decoded into consecutive RoleAssistant turns.
// Under HistoryToolTurns, EncodeRequest must coalesce all three into one
// DeepSeek-valid assistant message retaining reasoning_content and reconciling
// every reported ir.Loss under VerifyEssence.
func TestLegalizeCoalescesSplitAssistantReasoningCommentaryAndToolCall(t *testing.T) {
	src := &ir.Request{
		Model: "deepseek-flash",
		Messages: []ir.Message{
			{
				Role:  ir.RoleUser,
				ID:    "msg_user_1",
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Write marker into result.txt and confirm."}},
			},
			{
				Role: ir.RoleAssistant,
				ID:   "rs_turn_1",
				Parts: []ir.Part{{
					Kind: ir.PartReasoning,
					Reasoning: &ir.Reasoning{
						Segments: []string{"Plan shell write to result.txt."},
						Token: &ir.ReplayToken{
							Scheme: ir.SchemePlain,
							Data:   "Plan shell write to result.txt.",
						},
					},
				}},
			},
			{
				Role: ir.RoleAssistant,
				ID:   "msg_commentary_1",
				Extra: ir.Extra{
					Origin: protocol.ProtocolOpenAIResponses,
					Members: map[string]jsontext.Value{
						"phase":  jsontext.Value(`"commentary"`),
						"status": jsontext.Value(`"completed"`),
					},
				},
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Writing result.txt now."}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{
						CallID:    "call_shell_1",
						Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "exec_command"},
						Arguments: `{"cmd":"printf marker > result.txt"}`,
					},
				}},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID:  "call_shell_1",
						Tool:    ir.ToolRef{Kind: ir.ToolFunction, Name: "exec_command"},
						Content: []ir.Part{{Kind: ir.PartText, Text: "Process exited with code 0"}},
					},
				}},
			},
		},
		Tools: []ir.Tool{{
			Kind:        ir.ToolFunction,
			Name:        "exec_command",
			Description: "Execute a command.",
			InputSchema: jsontext.Value(`{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}`),
		}},
	}

	encoded, losses, err := EncodeRequest(src, ir.Dialect{
		ReasoningHistory: ir.HistoryToolTurns,
	})
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"reasoning_content":"Plan shell write to result.txt."`)
	require.Contains(t, string(encoded), `"content":"Writing result.txt now."`)

	violations := testkit.ValidateChatRequest(encoded, testkit.RequestValidationOptions{
		DialectRules: []string{"deepseek-thinking"},
	})
	require.Empty(t, violations, "ValidateChatRequest(deepseek-thinking) violations: %v\nencoded=%s", violations, string(encoded))

	dst, err := DecodeRequest(encoded)
	require.NoError(t, err)
	require.Len(t, dst.Messages, 3, "split assistant items must coalesce into one assistant message")

	diffs, err := testkit.VerifyEssence(ir.Essence(src), ir.Essence(dst), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}

// TestLegalizePreservesConsecutiveChatAssistantLawQ2AndSameOriginExtra feeds a
// Chat Completions request containing consecutive role="assistant" messages
// with same-origin Extra fields ("name") through single-protocol Law Q2
// round-trips and explicit HistoryToolTurns coalescing. Empty ReasoningHistory
// must preserve all four messages and zero losses, whereas HistoryToolTurns
// must coalesce both assistant turns while retaining same-origin Extra fields.
func TestLegalizePreservesConsecutiveChatAssistantLawQ2AndSameOriginExtra(t *testing.T) {
	rawChat := []byte(`{
		"model": "gpt-4o",
		"messages": [
			{"role": "user", "content": "Write result.txt"},
			{"role": "assistant", "name": "planner_step", "content": "I will run the shell command now."},
			{"role": "assistant", "content": null, "tool_calls": [
				{"id": "call_1", "type": "function", "function": {"name": "exec_command", "arguments": "{\"cmd\":\"echo ok\"}"}}
			]},
			{"role": "tool", "tool_call_id": "call_1", "content": "ok"}
		]
	}`)

	dec1, err := DecodeRequest(rawChat)
	require.NoError(t, err)
	require.Len(t, dec1.Messages, 4)

	encSingle, lossesSingle, err := EncodeRequest(dec1, ir.Dialect{})
	require.NoError(t, err)
	require.Empty(t, lossesSingle, "single-protocol Chat round-trip must incur zero losses")
	require.Empty(t, testkit.ValidateChatRequest(encSingle, testkit.RequestValidationOptions{}))

	dec2, err := DecodeRequest(encSingle)
	require.NoError(t, err)
	require.Equal(t, dec1, dec2, "Law Q2 fixed point Dec_Chat(Enc_Chat(Dec_Chat(x))) == Dec_Chat(x) violated")

	encMerged, lossesMerged, err := EncodeRequest(dec1, ir.Dialect{
		ReasoningHistory: ir.HistoryToolTurns,
	})
	require.NoError(t, err)
	require.Empty(t, lossesMerged, "same-origin Extra on coalesced Chat assistant turn must not report extra_dropped")
	require.Contains(t, string(encMerged), `"name":"planner_step"`)
	require.Empty(t, testkit.ValidateChatRequest(encMerged, testkit.RequestValidationOptions{
		DialectRules: []string{"deepseek-thinking"},
	}))

	decMerged, err := DecodeRequest(encMerged)
	require.NoError(t, err)
	require.Len(t, decMerged.Messages, 3)

	diffs, err := testkit.VerifyEssence(ir.Essence(dec1), ir.Essence(decMerged), lossesMerged)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}
