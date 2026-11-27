package chat

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestEncodeRequestDialectControlsAndExtensions checks Dialect parameter drops,
// ultra effort rejection, OutputFormat schema naming, and Extra origin rules.
// Encoded DeepSeek tool-turn history must pass strict deepseek-thinking
// validation and reconcile every semantic difference under VerifyEssence.
func TestEncodeRequestDialectControlsAndExtensions(t *testing.T) {
	_, _, err := EncodeRequest(nil, ir.Dialect{})
	require.Error(t, err)

	// Reject "ultra" reasoning effort.
	_, _, err = EncodeRequest(&ir.Request{
		Model:     "gpt-5",
		Reasoning: &ir.ReasoningConfig{Effort: "ultra"},
		Messages:  []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "hi"}}}},
	}, ir.Dialect{})
	require.Error(t, err)

	temp := 0.5
	topP := 0.9
	topK := int64(32)
	req := &ir.Request{
		Model: "deepseek-reasoner",
		Sampling: ir.Sampling{
			Temperature: &temp,
			TopP:        &topP,
			TopK:        &topK,
		},
		Stop:      []string{"HALT"},
		Reasoning: &ir.ReasoningConfig{Effort: "high"},
		Output: &ir.OutputFormat{
			Kind:      "json_schema",
			Schema:    jsontext.Value(`{"type":"object","properties":{"ok":{"type":"boolean"}}}`),
			Verbosity: "medium",
		},
		Messages: []ir.Message{
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Run check."}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_ds_1",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "ping"},
							Arguments: `{}`,
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID:  "call_ds_1",
						Content: []ir.Part{{Kind: ir.PartText, Text: "pong"}},
					},
				}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind:      ir.PartReasoning,
						Reasoning: &ir.Reasoning{Segments: []string{"Non-tool turn thought."}},
					},
					{
						Kind: ir.PartText,
						Text: "All checks passed.",
					},
				},
			},
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Continue."}},
			},
		},
		Tools: []ir.Tool{{
			Kind:        ir.ToolFunction,
			Name:        "ping",
			InputSchema: jsontext.Value(`{"type":"object"}`),
		}},
		Extra: ir.Extra{
			Origin: protocol.ProtocolAnthropicMessages,
			Members: map[string]jsontext.Value{
				"anthropic_beta": jsontext.Value(`"test"`),
			},
		},
	}

	encoded, losses, err := EncodeRequest(req, ir.Dialect{
		ReasoningHistory:  ir.HistoryToolTurns,
		StripEffort:       true,
		DropTemperature:   true,
		DropTopP:          true,
		DropTopK:          true,
		OmitStopSequences: true,
	})
	require.NoError(t, err)
	require.NotContains(t, string(encoded), `"temperature"`)
	require.NotContains(t, string(encoded), `"top_p"`)
	require.NotContains(t, string(encoded), `"stop"`)
	require.NotContains(t, string(encoded), `"reasoning_effort"`)
	require.NotContains(t, string(encoded), `"anthropic_beta"`)
	require.Contains(t, string(encoded), `"name":"response_schema"`)

	// Validate against strict Chat + deepseek-thinking dialect rules!
	violations := testkit.ValidateChatRequest(encoded, testkit.RequestValidationOptions{
		DialectRules: []string{"deepseek-thinking"},
	})
	require.Empty(t, violations, "strict deepseek-thinking violations: %v", violations)

	// Reconcile semantic Essence under VerifyEssence!
	decodedBack, err := DecodeRequest(encoded)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(decodedBack), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}
