package generatecontent

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestStreamEncoderTrailingSignaturesErrorFramesAndPostTerminalSilence streams
// incremental text parts carrying a Part.Token thoughtSignature followed by a
// terminal response_end event and a post-terminal event. StreamEncoder must emit
// the trailing empty-text signature frame, pass ValidateGeminiStream, and ignore
// events supplied after stream termination.
func TestStreamEncoderTrailingSignaturesErrorFramesAndPostTerminalSilence(t *testing.T) {
	reqWithSig, err := DecodeRequest([]byte(`{"contents":[{"role":"model","parts":[{"text":"Hello world","thoughtSignature":"sig_txt_99"}]},{"role":"user","parts":[{"text":"ok"}]}]}`))
	require.NoError(t, err)
	txtToken := reqWithSig.Messages[0].Parts[0].Token
	require.NotNil(t, txtToken)

	events := []ir.Event{
		{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "gem_se_1", Model: "gemini-2.5-pro"}},
		{Kind: ir.EventPartStart, Index: 0, Part: &ir.Part{Kind: ir.PartText}},
		{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Text: "Hello "}},
		{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Text: "world"}},
		{Kind: ir.EventPartEnd, Index: 0, Part: &ir.Part{Kind: ir.PartText, Text: "Hello world", Token: txtToken}},
		{Kind: ir.EventUsage, Usage: &ir.Usage{Input: 10, Output: 2}},
		{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishStop}, ServiceTier: "priority"},
		{Kind: ir.EventPartStart, Index: 1, Part: &ir.Part{Kind: ir.PartText}},
	}

	frames, losses := encodeFullEventStream(t, ir.ClientView{
		Model:   "gemini-2.5-pro",
		Runtime: newDeterministicRuntime(),
	}, events)
	require.Empty(t, testkit.ValidateGeminiStream(frames, testkit.GrammarOptions{}))
	require.Equal(t, []ir.Loss{{Kind: ir.LossExtraDropped, Path: "service_tier", Detail: "priority"}}, losses)

	redecoded, err := ir.Accumulate(decodeFullSSEStream(t, formatSSEFrames(frames)))
	require.NoError(t, err)
	require.Len(t, redecoded.Parts, 1)
	require.Equal(t, "Hello world", redecoded.Parts[0].Text)
	require.Equal(t, txtToken, redecoded.Parts[0].Token)
}
