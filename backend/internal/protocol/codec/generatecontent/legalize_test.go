package generatecontent

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestLegalizeStepSignaturesReorderingMediaRelocationAndOrphans encodes a
// multi-turn conversation mixing unknown-unsigned parallel calls, out-of-order
// tool results, nested result media, relocated developer instructions, and
// orphan tool items. Every unknown-unsigned call must receive PlaceholderToken,
// pass strict gemini-signature validation, and reconcile under VerifyEssence.
func TestLegalizeStepSignaturesReorderingMediaRelocationAndOrphans(t *testing.T) {
	req := &ir.Request{
		System: []ir.Part{{Kind: ir.PartText, Text: "Root system"}},
		Messages: []ir.Message{
			{
				Role:  ir.RoleDeveloper,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Developer addendum"}},
			},
			{
				Role:  ir.RoleUser,
				ID:    "msg_u_1",
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Run checks"}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Segments: []string{"Unsigned thought dropped on standard Gemini"},
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_a",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_a"},
							Arguments: `{"step":1}`,
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_b",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_b"},
							Arguments: `{"step":2}`,
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_orphan",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fn_orphan"},
							Arguments: `{}`,
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
							CallID: "call_b",
							Content: []ir.Part{
								{Kind: ir.PartText, Text: "second completed first"},
								{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, MIME: "image/png", Source: ir.MediaSourceBase64, Data: "AAAA"}},
							},
						},
					},
					{
						Kind: ir.PartToolResult,
						Result: &ir.ToolResult{
							CallID:  "call_a",
							Content: []ir.Part{{Kind: ir.PartText, Text: "first completed second"}},
						},
					},
					{
						Kind: ir.PartToolResult,
						Result: &ir.ToolResult{
							CallID:  "call_stray",
							Content: []ir.Part{{Kind: ir.PartText, Text: "stray"}},
						},
					},
				},
			},
		},
		Tools: []ir.Tool{
			{Kind: ir.ToolFunction, Name: "fn_a"},
			{Kind: ir.ToolFunction, Name: "fn_b"},
		},
	}

	dialect := ir.Dialect{
		SchemaDialect:    ir.SchemaGemini,
		PlaceholderToken: "skip_thought_signature_validator",
	}
	encoded, losses, err := EncodeRequest(req, dialect)
	require.NoError(t, err)

	violations := testkit.ValidateGeminiRequest(encoded, testkit.RequestValidationOptions{
		DialectRules: []string{"gemini-signature"},
	})
	require.Empty(t, violations, "gemini-signature violations: %v\nencoded=%s", violations, string(encoded))

	// Unknown-unsigned parallel calls (Call.Unsigned==false) both receive PlaceholderToken.
	s := string(encoded)
	require.Contains(t, s, `"id":"call_a"`)
	require.Contains(t, s, `"thoughtSignature":"skip_thought_signature_validator"`)
	require.Equal(t, 2, countSubstring(s, "skip_thought_signature_validator"))

	redecoded, err := DecodeRequest(encoded)
	require.NoError(t, err)
	// Strip the injected placeholder tokens from redecoded calls before Essence comparison
	// because convert's ReplayScheme layer handles placeholder injection outside Loss accounting.
	redecoded.Messages[1].Parts[0].Call.Token = nil
	redecoded.Messages[1].Parts[1].Call.Token = nil

	diffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(redecoded), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}

// TestLegalizeKnownUnsignedStepInitialAndParallelPlaceholderFallback reproduces
// multi-step tool histories where CallMemory marked upstream calls Unsigned=true
// across both parallel invocations and consecutive steps (4b05d79a5). Every
// step-initial call must still receive PlaceholderToken to satisfy GEM-SIGNATURE,
// whereas subsequent known-unsigned parallel calls in the same step stay unsigned.
func TestLegalizeKnownUnsignedStepInitialAndParallelPlaceholderFallback(t *testing.T) {
	req := &ir.Request{
		Messages: []ir.Message{
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Compare cities"}}},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "step1_first",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "get_weather"},
							Arguments: `{"city":"Paris"}`,
							Unsigned:  true,
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "step1_second",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "get_weather"},
							Arguments: `{"city":"London"}`,
							Unsigned:  true,
						},
					},
					{Kind: ir.PartText, Text: "Now checking Tokyo and Berlin."},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "step2_first",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "get_weather"},
							Arguments: `{"city":"Tokyo"}`,
							Unsigned:  true,
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "step2_second",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "get_weather"},
							Arguments: `{"city":"Berlin"}`,
							Unsigned:  true,
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "step1_first", Content: []ir.Part{{Kind: ir.PartText, Text: "sunny"}}}},
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "step1_second", Content: []ir.Part{{Kind: ir.PartText, Text: "rain"}}}},
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "step2_first", Content: []ir.Part{{Kind: ir.PartText, Text: "clear"}}}},
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "step2_second", Content: []ir.Part{{Kind: ir.PartText, Text: "cloudy"}}}},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "turn2_first",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "get_weather"},
							Arguments: `{"city":"Rome"}`,
							Unsigned:  true,
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "turn2_first", Content: []ir.Part{{Kind: ir.PartText, Text: "warm"}}}},
				},
			},
		},
		Tools: []ir.Tool{{
			Kind:        ir.ToolFunction,
			Name:        "get_weather",
			InputSchema: jsontext.Value(`{"properties":{"city":{"type":"STRING"}},"type":"OBJECT"}`),
		}},
	}

	encoded, losses, err := EncodeRequest(req, ir.Dialect{
		SchemaDialect:    ir.SchemaGemini,
		PlaceholderToken: "skip_thought_signature_validator",
	})
	require.NoError(t, err)
	require.Empty(t, losses)

	violations := testkit.ValidateGeminiRequest(encoded, testkit.RequestValidationOptions{
		DialectRules: []string{"gemini-signature"},
	})
	require.Empty(t, violations, "gemini-signature violations: %v\nencoded=%s", violations, string(encoded))

	redecoded, err := DecodeRequest(encoded)
	require.NoError(t, err)
	// Step 1: step1_first signed with placeholder, step1_second stays unsigned.
	require.NotNil(t, redecoded.Messages[1].Parts[0].Call.Token)
	require.Equal(t, "skip_thought_signature_validator", redecoded.Messages[1].Parts[0].Call.Token.Data)
	require.Nil(t, redecoded.Messages[1].Parts[1].Call.Token)
	// Step 2 (after intervening text): step2_first signed with placeholder, step2_second stays unsigned.
	require.NotNil(t, redecoded.Messages[1].Parts[3].Call.Token)
	require.Equal(t, "skip_thought_signature_validator", redecoded.Messages[1].Parts[3].Call.Token.Data)
	require.Nil(t, redecoded.Messages[1].Parts[4].Call.Token)
	// Next assistant turn: turn2_first signed with placeholder.
	require.NotNil(t, redecoded.Messages[3].Parts[0].Call.Token)
	require.Equal(t, "skip_thought_signature_validator", redecoded.Messages[3].Parts[0].Call.Token.Data)
}

// TestLegalizeSchemeAgnosticReplayTokensAndRedactedSignature encodes assistant
// reasoning and tool-call parts carrying SchemeAntigravityClaude bare tokens and
// Redacted flags. EncodeRequest must write Token.Data verbatim to thoughtSignature
// without recording LossReplayTokenDropped, dropping only extra Redacted segments.
func TestLegalizeSchemeAgnosticReplayTokensAndRedactedSignature(t *testing.T) {
	req := &ir.Request{
		Messages: []ir.Message{
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Audit service"}}},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Redacted: true,
							Token: &ir.ReplayToken{
								Scheme: ir.SchemeAntigravityClaude,
								Data:   "ErYBAntigravityBareRedacted",
							},
						},
					},
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Redacted: true,
							Segments: []string{"discarded visible segment"},
							Token: &ir.ReplayToken{
								Scheme: ir.SchemeAntigravityClaude,
								Data:   "ErYBRedactedWithSegments",
							},
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_ag_1",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "read_logs"},
							Arguments: `{"limit":5}`,
							Token: &ir.ReplayToken{
								Scheme: ir.SchemeAntigravityClaude,
								Data:   "ErYBAntigravityCallSignature",
							},
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{
					{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "call_ag_1", Content: []ir.Part{{Kind: ir.PartText, Text: "ok"}}}},
				},
			},
		},
		Tools: []ir.Tool{{
			Kind:        ir.ToolFunction,
			Name:        "read_logs",
			InputSchema: jsontext.Value(`{"properties":{"limit":{"type":"integer"}},"type":"object"}`),
		}},
	}

	encoded, losses, err := EncodeRequest(req, ir.Dialect{
		ReplayScheme:  ir.SchemeAntigravityClaude,
		SchemaDialect: ir.SchemaAntigravity,
	})
	require.NoError(t, err)
	require.Equal(t, []ir.Loss{{
		Kind: ir.LossReasoningTextDropped,
		Path: "messages[1].parts[1]",
	}}, losses)

	s := string(encoded)
	require.Contains(t, s, `{"text":"","thought":true,"thoughtSignature":"ErYBAntigravityBareRedacted"}`)
	require.Contains(t, s, `{"text":"","thought":true,"thoughtSignature":"ErYBRedactedWithSegments"}`)
	require.Contains(t, s, `"thoughtSignature":"ErYBAntigravityCallSignature"`)

	redecoded, err := DecodeRequest(encoded)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(redecoded), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}

// TestAntigravityClaudeUnsignedThinkingInlinesAndStripsThinkingConfig encodes a
// conversation containing unsigned historical reasoning under SchemeAntigravityClaude.
// EncodeRequest must downgrade the reasoning block to plain text with
// LossReasoningTextInlined and suppress generationConfig.thinkingConfig completely.
func TestAntigravityClaudeUnsignedThinkingInlinesAndStripsThinkingConfig(t *testing.T) {
	budget := int64(2048)
	maxOut := int64(8192)
	req := &ir.Request{
		MaxOutputTokens: &maxOut,
		Reasoning: &ir.ReasoningConfig{
			Mode:         "enabled",
			BudgetTokens: &budget,
			Summary:      "auto",
		},
		Messages: []ir.Message{
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Question 1"}}},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Segments: []string{"Step 1 analysis", "Step 2 conclusion"},
						},
					},
					{Kind: ir.PartText, Text: "Answer 1"},
				},
			},
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Question 2"}}},
		},
		Extra: ir.Extra{
			Origin: protocol.ProtocolAnthropicMessages,
		},
	}

	encoded, losses, err := EncodeRequest(req, ir.Dialect{
		ReplayScheme:  ir.SchemeAntigravityClaude,
		SchemaDialect: ir.SchemaAntigravity,
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Budget:        true,
			ForceBudget:   true,
		},
	})
	require.NoError(t, err)
	require.Equal(t, []ir.Loss{{
		Kind: ir.LossReasoningTextInlined,
		Path: "messages[1].parts[0]",
	}}, losses)

	s := string(encoded)
	require.NotContains(t, s, `"thinkingConfig"`)
	require.Contains(t, s, `"Step 1 analysisStep 2 conclusion"`)

	redecoded, err := DecodeRequest(encoded)
	require.NoError(t, err)
	// Restore Reasoning on redecoded only to verify message/part Essence reconciliation.
	redecoded.Reasoning = req.Reasoning
	diffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(redecoded), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}

// countSubstring counts non-overlapping occurrences of sub inside s for encoded JSON assertions.
func countSubstring(s, sub string) int {
	n := 0
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			n++
			i += len(sub) - 1
		}
	}
	return n
}
