package upstream

import (
	"context"
	"encoding/json/jsontext"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/timing"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestFinishRetainsCompletedDisconnectDrain distinguishes a client write
// failure from an incomplete upstream read. Complete drained usage keeps throughput,
// whereas conversion failures and streams without terminal evidence cannot estimate it.
func TestFinishRetainsCompletedDisconnectDrain(t *testing.T) {
	for _, tc := range []struct {
		name         string // completion fixture identity
		disconnected bool   // client write failure observation
		complete     bool   // successful upstream terminal observation
		expectTPS    bool   // expected throughput eligibility
	}{{"drained", true, true, true}, {"partial", true, false, false}, {"conversion_failed", false, true, false}} {
		t.Run(tc.name, func(t *testing.T) {
			p := timing.NewPerformance(timing.FirstOutput, 0)
			p.Stream, p.Complete, p.UsageComplete = true, tc.complete, true
			p.Values["upstream_first_output_ms"] = 100
			p.Values["upstream_last_output_ms"] = 600
			p.Values["upstream_output_event_count"] = 2
			ctx := timing.WithPerformance(t.Context(), p)
			result := AttemptResult{ClientDisconnect: tc.disconnected}
			result.Finish(ctx, context.Canceled)
			require.Equal(t, tc.expectTPS, result.Timings.TPS(10) != nil)
			require.True(t, p.UsageComplete)
		})
	}
}

// TestFinishRecordsUntimedMediaOutcome completes an older media result after a
// new dispatch starts. Its terminal outcome and selected ID remain isolated,
// while partial usage and the absence of performance observations stay unchanged.
func TestFinishRecordsUntimedMediaOutcome(t *testing.T) {
	for _, failure := range []error{nil, io.ErrUnexpectedEOF, context.Canceled} {
		var saved telemetry.RequestRecord
		ctx := telemetry.WithRequestCapture(t.Context(), telemetry.RequestRecord{RequestID: "request"}, func(r telemetry.RequestRecord) { saved = r })
		ctx = telemetry.WithRequestProvider(ctx, 7, "X-Custom")
		first := telemetry.BeginRequestAttempt(ctx, 0)
		ref := telemetry.RequestAttemptFromContext(first)
		telemetry.ObserveRequestHeaders(ctx, ref, http.Header{"X-Custom": {"known"}}, 200)
		result := AttemptResult{RequestAttempt: ref, HasUsage: true, Usage: TokenUsage{InputTokens: 12, OutputTokens: 3}, Served: true}
		telemetry.BeginRequestAttempt(ctx, 0)
		result.Finish(ctx, failure)
		expected := "completed"
		if failure != nil {
			expected = "failed"
		}
		require.Equal(t, expected, saved.Attempts[0].Outcome)
		require.Equal(t, "known", saved.Attempts[0].RequestID)
		require.Equal(t, "running", saved.Attempts[1].Outcome)
		require.Nil(t, result.Timings)
		require.True(t, result.HasUsage)
		require.Equal(t, 12, result.Usage.InputTokens)
		require.Equal(t, 3, result.Usage.OutputTokens)
	}
}

// TestProtocolConversionHelpers_ObserveFinalizeAndZeroFalsePositives verifies
// that ExchangeConversion, FinalizeExchangeConversion, ObserveToolAdaptedConversion,
// ObserveNativeReplayConversion, and AttemptResult.Finish propagate bounded
// diagnostics while suppressing plain-function inheritance and unapplied replays.
func TestProtocolConversionHelpers_ObserveFinalizeAndZeroFalsePositives(t *testing.T) {
	ctx := telemetry.WithRequestProvider(context.Background(), 42, "X-Request-ID")

	// Plain-function LoweredTools without restorations, namespaces, or losses must return nil.
	plainMap := &convert.ToolMap{
		LoweredTools: []jsontext.Value{jsontext.Value(`{"type":"function","name":"read_file"}`)},
	}
	require.Nil(t, ObserveToolAdaptedConversion(
		ctx,
		protocol.ProtocolResponsesWebSocket,
		protocol.ProtocolOpenAIResponses,
		convert.Dialect{Name: "openai", ReplayScheme: ir.SchemeOpenAI},
		plainMap,
		nil,
	))
	require.Nil(t, ObserveNativeReplayConversion(
		ctx,
		convert.Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  convert.Dialect{Name: "openai", ReplayScheme: ir.SchemeOpenAI},
		},
		nil,
		false,
	))
	require.Nil(t, telemetry.ProtocolConversionFromContext(ctx))

	// Native replay + tool adaptation on the same attempt merges into tool_adapted.
	ObserveNativeReplayConversion(
		ctx,
		convert.Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  convert.Dialect{Name: "openai", ReplayScheme: ir.SchemeOpenAI},
		},
		[]ir.Loss{{Kind: ir.LossReplayTokenDropped}},
		true,
	)
	_, customMap, adaptLosses, err := convert.AdaptResponsesTools(
		[]byte(`{"model":"gpt-5.4","tools":[{"type":"custom","name":"apply_patch","format":{"type":"text"}}],"input":"hi"}`),
		convert.Dialect{Name: "openai", ReplayScheme: ir.SchemeOpenAI, Tools: ir.ToolSupportFunction},
		nil,
	)
	require.NoError(t, err)
	ObserveToolAdaptedConversion(
		ctx,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIResponses,
		convert.Dialect{Name: "openai", ReplayScheme: ir.SchemeOpenAI},
		customMap,
		adaptLosses,
	)

	var res AttemptResult
	res.Finish(ctx, nil)
	require.NotNil(t, res.ProtocolConversion)
	require.Equal(t, telemetry.ConversionModeToolAdapted, res.ProtocolConversion.Mode)
	require.Equal(t, 1, res.ProtocolConversion.LossCounts[string(ir.LossReplayTokenDropped)])
	require.Equal(t, 1, res.ProtocolConversion.LossCounts[string(ir.LossToolLowered)])
}
