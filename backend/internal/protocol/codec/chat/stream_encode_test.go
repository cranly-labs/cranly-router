package chat

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestStreamEncoderSemanticTerminalFlagsAndOneShotParts verifies Semantic and
// Terminal frame flags alongside follow-up tool_call delta hygiene (091ec1f41).
// One-shot EventPartEnd suffixes must flush completely, and usage tail chunks
// must appear only when IncludeUsage is true.
func TestStreamEncoderSemanticTerminalFlagsAndOneShotParts(t *testing.T) {
	events := []ir.Event{
		{
			Kind: ir.EventResponseStart,
			Response: &ir.ResponseMeta{
				ID:          "chatcmpl_one_shot",
				Model:       "gemini-2.5-pro",
				CreatedAt:   1760000500,
				ServiceTier: "priority",
			},
		},
		{
			Kind:  ir.EventPartStart,
			Index: 0,
			Part: &ir.Part{
				Kind:      ir.PartReasoning,
				Reasoning: &ir.Reasoning{},
			},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 0,
			Part: &ir.Part{
				Kind: ir.PartReasoning,
				Reasoning: &ir.Reasoning{
					Segments: []string{"One-shot thought."},
					Token:    &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig_r"},
				},
			},
		},
		{
			Kind:  ir.EventPartStart,
			Index: 1,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID: "",
					Tool:   ir.ToolRef{Kind: ir.ToolFunction, Name: "fetch_metrics"},
				},
			},
		},
		{
			Kind:  ir.EventPartEnd,
			Index: 1,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID:    "",
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "fetch_metrics"},
					Arguments: `{"window":"5m"}`,
					Token:     &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig_c"},
				},
			},
		},
		{
			Kind:  ir.EventUsage,
			Usage: &ir.Usage{Input: 20, Output: 10},
		},
		{
			Kind:   ir.EventResponseEnd,
			Finish: &ir.Finish{Reason: ir.FinishToolCalls},
		},
	}

	// IncludeUsage omitted -> no usage chunk before [DONE].
	framesNoUsage, losses := encodeFullEventStream(t, ir.ClientView{
		Model:   "gpt-4o",
		Runtime: newDeterministicRuntime(),
	}, events)
	require.Empty(t, testkit.ValidateChatStream(framesNoUsage, testkit.GrammarOptions{}))
	require.False(t, framesNoUsage[0].Semantic, "initial role chunk must not be marked Semantic")
	require.False(t, framesNoUsage[0].Terminal)
	require.True(t, framesNoUsage[1].Semantic, "reasoning delta must be marked Semantic")
	require.True(t, framesNoUsage[2].Semantic, "tool_call opening chunk must be marked Semantic")
	require.True(t, framesNoUsage[3].Semantic, "tool_call args chunk must be marked Semantic")
	require.NotContains(t, string(framesNoUsage[3].Data), `"name"`, "follow-up tool_call delta must omit function.name")
	require.False(t, framesNoUsage[4].Semantic, "finish_reason chunk must not be marked Semantic")
	require.True(t, framesNoUsage[len(framesNoUsage)-1].Terminal, "[DONE] must be marked Terminal")
	for _, f := range framesNoUsage {
		require.NotContains(t, string(f.Data), `"prompt_tokens"`, "IncludeUsage=false must omit usage chunk")
	}
	require.Len(t, losses, 3)

	// IncludeUsage=true -> emits usage tail chunk right before [DONE].
	incTrue := true
	framesWithUsage, _ := encodeFullEventStream(t, ir.ClientView{
		Model:   "gpt-4o",
		Request: &ir.Request{IncludeUsage: &incTrue},
		Runtime: newDeterministicRuntime(),
	}, events)
	require.Empty(t, testkit.ValidateChatStream(framesWithUsage, testkit.GrammarOptions{}))
	require.Contains(t, string(framesWithUsage[len(framesWithUsage)-2].Data), `"prompt_tokens":20`)
	require.Equal(t, "[DONE]", string(framesWithUsage[len(framesWithUsage)-1].Data))

	// Error event emits terminal error frame accepted by AllowAbnormal grammar.
	errFrames, _ := encodeFullEventStream(t, ir.ClientView{
		Model:   "gpt-4o",
		Runtime: newDeterministicRuntime(),
	}, []ir.Event{
		{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "chatcmpl_err", Model: "gpt-4o", CreatedAt: 1760000000}},
		{Kind: ir.EventError, Err: &ir.StreamError{Type: "server_error", Code: "overloaded", Message: "capacity exhausted"}},
	})
	require.Empty(t, testkit.ValidateChatStream(errFrames, testkit.GrammarOptions{AllowAbnormal: true}))
	require.True(t, errFrames[len(errFrames)-1].Terminal)
}
