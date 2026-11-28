package completion

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
)

// TestResponseModelCompletionSnapshot captures outbound and raw response model
// data before mutating the source result. It checks that the response declaration
// in the copied completion input retains its submitted value.
func TestResponseModelCompletionSnapshot(t *testing.T) {
	conv := telemetry.NewProtocolConversion(
		telemetry.ConversionModeIRConverted,
		"anthropic_messages",
		"gemini_generate_content",
		"gemini",
		"gemini",
		map[string]int{"cache_mark_dropped": 2},
	)
	source := &Input{Result: &Result{
		UpstreamModel:         "sent",
		UpstreamResponseModel: "first",
		ProtocolConversion:    conv,
	}}
	captured := Snapshot(source)
	source.Result.UpstreamResponseModel = "next-turn"
	source.Result.ProtocolConversion.LossCounts["cache_mark_dropped"] = 99
	require.Equal(t, "first", captured.Result.UpstreamResponseModel)
	require.NotNil(t, captured.Result.ProtocolConversion)
	require.Equal(t, 2, captured.Result.ProtocolConversion.LossCounts["cache_mark_dropped"])
}
