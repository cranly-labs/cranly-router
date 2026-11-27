package qoderchat_test

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/qoderchat"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestEncodeRequestAllStatelessAndToolRejections feeds invalid models, stateful
// Responses continuation flags, orphan tool outputs, and undeclared allowed
// tool subsets into EncodeRequest. It verifies that each rejected branch
// returns a typed *qoder.RequestError carrying the expected rejection Code.
func TestEncodeRequestAllStatelessAndToolRejections(t *testing.T) {
	storeTrue := true
	cases := []struct {
		name               string      // subtest scenario label
		req                *ir.Request // input canonical IR request
		statelessResponses bool        // whether Responses stateless gates are active
		wantCode           string      // expected *qoder.RequestError Code
	}{
		{
			name:     "blank model",
			req:      &ir.Request{Model: "   "},
			wantCode: qoder.CodeInvalidRequest,
		},
		{
			name: "store true rejected in Responses mode",
			req: &ir.Request{
				Model:    "qwen3.8-max",
				Store:    &storeTrue,
				Messages: []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "hi"}}}},
			},
			statelessResponses: true,
			wantCode:           qoder.CodeUnsupportedResponseState,
		},
		{
			name: "previous_response_id rejected",
			req: &ir.Request{
				Model:              "qwen3.8-max",
				PreviousResponseID: "resp_prev_123",
				Messages:           []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "hi"}}}},
			},
			wantCode: qoder.CodeUnsupportedResponseState,
		},
		{
			name: "conversation_set rejected",
			req: &ir.Request{
				Model:           "qwen3.8-max",
				ConversationID:  "conv_123",
				ConversationSet: true,
				Messages:        []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "hi"}}}},
			},
			wantCode: qoder.CodeUnsupportedResponseState,
		},
		{
			name: "item_reference in messages rejected",
			req: &ir.Request{
				Model: "qwen3.8-max",
				Messages: []ir.Message{{
					Role: ir.RoleUser,
					Parts: []ir.Part{{
						Kind: ir.PartOpaque,
						Opaque: &ir.OpaqueItem{
							Origin: protocol.ProtocolOpenAIResponses,
							Type:   "item_reference",
							Raw:    jsontext.Value(`{"type":"item_reference","id":"msg_1"}`),
						},
					}},
				}},
			},
			wantCode: qoder.CodeUnsupportedResponseState,
		},
		{
			name: "orphan tool_result rejected in Responses mode",
			req: &ir.Request{
				Model: "qwen3.8-max",
				Messages: []ir.Message{{
					Role: ir.RoleTool,
					Parts: []ir.Part{{
						Kind: ir.PartToolResult,
						Result: &ir.ToolResult{
							CallID:  "call_missing",
							Content: []ir.Part{{Kind: ir.PartText, Text: "out"}},
						},
					}},
				}},
			},
			statelessResponses: true,
			wantCode:           qoder.CodeMissingToolCall,
		},
		{
			name: "blank call_id tool_result rejected in Responses mode",
			req: &ir.Request{
				Model: "qwen3.8-max",
				Messages: []ir.Message{{
					Role: ir.RoleTool,
					Parts: []ir.Part{{
						Kind: ir.PartToolResult,
						Result: &ir.ToolResult{
							CallID:  "   ",
							Content: []ir.Part{{Kind: ir.PartText, Text: "out"}},
						},
					}},
				}},
			},
			statelessResponses: true,
			wantCode:           qoder.CodeMissingToolCall,
		},
		{
			name: "allowed_tools undeclared function rejected",
			req: &ir.Request{
				Model: "qwen3.8-max",
				Tools: []ir.Tool{{
					Kind:        ir.ToolFunction,
					Name:        "declared_fn",
					InputSchema: jsontext.Value(`{"type":"object"}`),
				}},
				ToolChoice: &ir.ToolChoice{
					Mode:         ir.ToolChoiceAuto,
					AllowedTools: []ir.ToolRef{{Kind: ir.ToolFunction, Name: "undeclared_fn"}},
				},
			},
			wantCode: qoder.CodeUnsupportedToolChoice,
		},
		{
			name: "allowed_tools invalid mode rejected",
			req: &ir.Request{
				Model: "qwen3.8-max",
				Tools: []ir.Tool{{
					Kind:        ir.ToolFunction,
					Name:        "declared_fn",
					InputSchema: jsontext.Value(`{"type":"object"}`),
				}},
				ToolChoice: &ir.ToolChoice{
					Mode:         ir.ToolChoiceNone,
					AllowedTools: []ir.ToolRef{{Kind: ir.ToolFunction, Name: "declared_fn"}},
				},
			},
			wantCode: qoder.CodeUnsupportedToolChoice,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := qoderchat.EncodeRequest(tc.req, ir.Dialect{
				StatelessResponses: tc.statelessResponses,
			})
			require.Error(t, err)
			reqErr, ok := qoder.AsRequestError(err)
			require.True(t, ok, "expected *qoder.RequestError, got %T: %v", err, err)
			require.Equal(t, tc.wantCode, reqErr.Code)
		})
	}
}

// TestEncodeRequestMetadataControlsPairingAndEssence encodes a multi-turn
// Responses-mode IR request with session affinity hints, billing headers,
// unanswered parallel calls, and allowed_tools subsets. It verifies
// byte-identical determinism, non-mutation of the input tree, and Law Q4
// VerifyEssence equivalence against DecodePayloadRequest.
func TestEncodeRequestMetadataControlsPairingAndEssence(t *testing.T) {
	storeFalse := false
	temp := 0.5
	budget := int64(4096)
	src := &ir.Request{
		Model:          "claude-opus-4-6",
		Store:          &storeFalse,
		SessionID:      "sess_abc",
		ConversationID: "conv_xyz",
		CacheKey:       "pck_99",
		MetadataUserID: "uid_77",
		Sampling:       ir.Sampling{Temperature: &temp},
		Stop:           []string{"STOP"},
		Reasoning: &ir.ReasoningConfig{
			Mode:         "enabled",
			Effort:       "high",
			BudgetTokens: &budget,
		},
		System: []ir.Part{
			{Kind: ir.PartText, Text: "x-anthropic-billing-header: cc_version=2.1.330; cch=12abF;"},
			{Kind: ir.PartText, Text: "Follow repository rules."},
		},
		Messages: []ir.Message{
			{
				Role:  ir.RoleDeveloper,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Use concise Go."}},
			},
			{
				Role: ir.RoleUser,
				Parts: []ir.Part{
					{Kind: ir.PartText, Text: "Check workspace", Cache: &ir.CacheMark{TTL: "5m"}},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Segments: []string{"plan"}}},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_kept",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "Read"},
							Arguments: `{"path":"README.md"}`,
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_unanswered",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "Bash"},
							Arguments: `{"command":"ls"}`,
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID:  "call_kept",
						Content: []ir.Part{},
					},
				}},
			},
		},
		Tools: []ir.Tool{
			{Kind: ir.ToolFunction, Name: "Read", Description: "Read file", InputSchema: jsontext.Value(`{"type":"object"}`)},
			{Kind: ir.ToolFunction, Name: "Bash", Description: "Run bash"},
			{
				Kind: ir.ToolHosted,
				Name: "web_search",
				Hosted: &ir.OpaqueItem{
					Origin: protocol.ProtocolOpenAIResponses,
					Type:   "web_search",
					Raw:    jsontext.Value(`{"type":"web_search"}`),
				},
			},
		},
		ToolChoice: &ir.ToolChoice{
			Mode:         ir.ToolChoiceRequired,
			AllowedTools: []ir.ToolRef{{Kind: ir.ToolFunction, Name: "Read"}},
		},
	}

	beforeClone := src.Clone()
	dialect := ir.Dialect{AllowBillingHeader: true, StatelessResponses: true}
	encoded1, losses1, err := qoderchat.EncodeRequest(src, dialect)
	require.NoError(t, err)
	encoded2, _, err := qoderchat.EncodeRequest(src, dialect)
	require.NoError(t, err)
	require.Equal(t, encoded1, encoded2, "EncodeRequest must be byte-deterministic")
	require.Equal(t, beforeClone, src, "EncodeRequest must not mutate input ir.Request")

	payload, err := qoder.DecodePayload(encoded1)
	require.NoError(t, err)
	require.Equal(t, qoder.DefaultMaxTokens, payload.MaxTokens)
	require.Equal(t, "sess_abc", payload.SessionID)
	require.Equal(t, "conv_xyz", payload.ConversationID)
	require.Equal(t, "pck_99", payload.CacheKey)
	require.Equal(t, "uid_77", payload.MetadataUserID)
	require.Equal(t, qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "high"}, payload.Thinking)
	require.Contains(t, payload.System, "cch=12abF;")
	require.Contains(t, payload.NormalizedSystem(), "cch=<cch>;")
	require.Contains(t, payload.System, "Use concise Go.")
	require.Len(t, payload.Messages, 3)
	require.Len(t, payload.Messages[1].ToolCalls, 1)
	require.Equal(t, "call_kept", payload.Messages[1].ToolCalls[0].ID)
	require.Equal(t, "(empty)", payload.Messages[2].Text)
	require.True(t, payload.Messages[2].EmptyContent)
	require.Equal(t, "Read", payload.Messages[2].Name)
	require.NotNil(t, payload.ToolChoice)
	require.Equal(t, []string{"Read"}, payload.ToolChoice.AllowedTools)

	dstIR, err := qoderchat.DecodePayloadRequest(encoded1)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(src), ir.Essence(dstIR), losses1)
	require.NoError(t, err)
	require.Empty(t, diffs)
}

// TestEncodeRequestChatAndMessagesStatelessExemptions encodes non-Responses IR
// requests carrying Store=true, standalone tool_result turns without preceding
// calls, unanswered assistant calls, and ID-less Messages tool_result blocks.
// It verifies that Store is ignored, identified tool turns survive in order,
// and missing-ID tool results fold readable text into user turns with losses.
func TestEncodeRequestChatAndMessagesStatelessExemptions(t *testing.T) {
	storeTrue := true
	chatAndMessagesReq := &ir.Request{
		Model: "auto",
		Store: &storeTrue,
		Messages: []ir.Message{
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "hello"}},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID:  "standalone_t1",
						Content: []ir.Part{{Kind: ir.PartText, Text: "tool output"}},
					},
				}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{
						CallID:    "unanswered_c1",
						Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "Read"},
						Arguments: `{"path":"a.go"}`,
					},
				}},
			},
		},
	}

	encoded, losses, err := qoderchat.EncodeRequest(chatAndMessagesReq, ir.Dialect{StatelessResponses: false})
	require.NoError(t, err)
	payload, err := qoder.DecodePayload(encoded)
	require.NoError(t, err)
	require.Len(t, payload.Messages, 3)
	require.Equal(t, "tool", payload.Messages[1].Role)
	require.Equal(t, "standalone_t1", payload.Messages[1].ToolCallID)
	require.Equal(t, "tool output", payload.Messages[1].Text)
	require.Equal(t, "assistant", payload.Messages[2].Role)
	require.Len(t, payload.Messages[2].ToolCalls, 1)
	require.Equal(t, "unanswered_c1", payload.Messages[2].ToolCalls[0].ID)

	dstIR, err := qoderchat.DecodePayloadRequest(encoded)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(chatAndMessagesReq), ir.Essence(dstIR), losses)
	require.NoError(t, err)
	require.Empty(t, diffs)

	missingIDReq := &ir.Request{
		Model: "auto",
		Messages: []ir.Message{{
			Role: ir.RoleTool,
			Parts: []ir.Part{{
				Kind: ir.PartToolResult,
				Result: &ir.ToolResult{
					CallID:  "",
					Content: []ir.Part{{Kind: ir.PartText, Text: "file.txt"}},
				},
			}},
		}},
	}
	encodedMissing, missingLosses, err := qoderchat.EncodeRequest(missingIDReq, ir.Dialect{StatelessResponses: false})
	require.NoError(t, err)
	parsedMissing, err := qoder.DecodePayload(encodedMissing)
	require.NoError(t, err)
	require.Len(t, parsedMissing.Messages, 1)
	require.Equal(t, "user", parsedMissing.Messages[0].Role)
	require.Equal(t, "file.txt", parsedMissing.Messages[0].Text)
	require.Empty(t, parsedMissing.Messages[0].ToolCallID)
	require.Empty(t, parsedMissing.Messages[0].ToolCalls)
	require.Contains(t, missingLosses, ir.Loss{
		Kind:   ir.LossToolPairingRepaired,
		Path:   "messages[0].parts[0]",
		Detail: "missing_tool_call_id",
	})
	dstMissingIR, err := qoderchat.DecodePayloadRequest(encodedMissing)
	require.NoError(t, err)
	diffsMissing, err := testkit.VerifyEssence(ir.Essence(missingIDReq), ir.Essence(dstMissingIR), missingLosses)
	require.NoError(t, err)
	require.Empty(t, diffsMissing, testkit.FormatJSONDiff(diffsMissing))

	bracketedReq := &ir.Request{
		Model: "auto",
		Messages: []ir.Message{
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "before"}}},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID:  "",
						Content: []ir.Part{{Kind: ir.PartText, Text: "file.txt"}},
					},
				}},
			},
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "after"}}},
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "separate_turn"}}},
		},
	}
	encBracketed, bracketedLosses, err := qoderchat.EncodeRequest(bracketedReq, ir.Dialect{StatelessResponses: false})
	require.NoError(t, err)
	parsedBracketed, err := qoder.DecodePayload(encBracketed)
	require.NoError(t, err)
	require.Len(t, parsedBracketed.Messages, 2)
	require.Equal(t, "before\nfile.txt\nafter", parsedBracketed.Messages[0].Text)
	require.Equal(t, "separate_turn", parsedBracketed.Messages[1].Text)
	dstBracketedIR, err := qoderchat.DecodePayloadRequest(encBracketed)
	require.NoError(t, err)
	diffsBracketed, err := testkit.VerifyEssence(ir.Essence(bracketedReq), ir.Essence(dstBracketedIR), bracketedLosses)
	require.NoError(t, err)
	require.Empty(t, diffsBracketed, testkit.FormatJSONDiff(diffsBracketed))
}
