package telemetry

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// protocol_conversion_test.go verifies bounded field normalization, mode
// precedence merging, failover scope isolation, and HTTP >= 400 retry snapshot
// preservation for ProtocolConversion values and attempt-scoped contexts.

// TestNormalizeAndMergeProtocolConversion_EnforcesBoundsAndPrecedence verifies
// that invalid modes, blank protocols, control characters, nonpositive counts,
// and excess loss keys are pruned while valid observations merge by mode depth.
func TestNormalizeAndMergeProtocolConversion_EnforcesBoundsAndPrecedence(t *testing.T) {
	require.Nil(t, NormalizeProtocolConversion(nil))
	require.Nil(t, NewProtocolConversion("unknown_mode", "anthropic_messages", "openai_responses", "openai", "openai", nil))
	require.Nil(t, NewProtocolConversion(ConversionModeIRConverted, "", "openai_responses", "openai", "", nil))

	longDialect := strings.Repeat("d", 100) + "\x00\r\n"
	rawCounts := map[string]int{
		"extra_dropped":         2,
		"zero_count":            0,
		"negative_count":        -3,
		"Invalid-Prompt-String": 5,
	}
	for i := range 40 {
		rawCounts[fmt.Sprintf("synthetic_loss_%02d", i)] = 1
	}

	got := NewProtocolConversion(
		"  ir_converted  ",
		"anthropic_messages\n",
		"gemini_generate_content",
		longDialect,
		"antigravity_claude",
		rawCounts,
	)
	require.NotNil(t, got)
	require.Equal(t, ConversionModeIRConverted, got.Mode)
	require.Equal(t, "anthropic_messages", got.ClientProtocol)
	require.Equal(t, "gemini_generate_content", got.UpstreamProtocol)
	require.Len(t, got.Dialect, maxConversionFieldBytes)
	require.Equal(t, "antigravity_claude", got.ReplayScheme)
	require.Equal(t, 2, got.LossCounts["extra_dropped"])
	require.NotContains(t, got.LossCounts, "zero_count")
	require.NotContains(t, got.LossCounts, "negative_count")
	require.NotContains(t, got.LossCounts, "Invalid-Prompt-String")
	require.LessOrEqual(t, len(got.LossCounts), maxConversionLossKinds)

	// Merge native_replay + tool_adapted: tool_adapted wins Mode while summing LossCounts
	// and retaining ReplayScheme.
	replay := NewProtocolConversion(
		ConversionModeNativeReplay,
		"openai_responses",
		"openai_responses",
		"openai",
		"openai",
		map[string]int{"replay_token_dropped": 1},
	)
	adapted := NewProtocolConversion(
		ConversionModeToolAdapted,
		"openai_responses",
		"openai_responses",
		"openai",
		"",
		map[string]int{"tool_lowered": 3, "replay_token_dropped": 2},
	)
	merged := MergeProtocolConversion(replay, adapted)
	require.NotNil(t, merged)
	require.Equal(t, ConversionModeToolAdapted, merged.Mode)
	require.Equal(t, "openai", merged.ReplayScheme)
	require.Equal(t, map[string]int{
		"replay_token_dropped": 3,
		"tool_lowered":         3,
	}, merged.LossCounts)

	// Clone detaches LossCounts backing map.
	cloned := merged.Clone()
	cloned.LossCounts["tool_lowered"] = 99
	require.Equal(t, 3, merged.LossCounts["tool_lowered"])
}

// TestProtocolConversionContext_IsolatesFailoverAndPreservesRejectedAttempt
// reproduces a failed converted first attempt followed by a same-provider retry
// and a cross-provider native failover, verifying that each RequestAttempt
// retains its own diagnostics without leaking onto native passthrough attempts.
func TestProtocolConversionContext_IsolatesFailoverAndPreservesRejectedAttempt(t *testing.T) {
	var latest RequestRecord
	base := WithRequestCapture(context.Background(), RequestRecord{
		RequestID: "req-conv-1",
		State:     "running",
	}, func(rec RequestRecord) {
		latest = rec
	})

	// Provider 10 attempt 1: stage ir_converted before BeginRequestAttempt, then fail with HTTP 400.
	ctxP1 := WithRequestProvider(base, 10, "X-Request-ID")
	ObserveProtocolConversion(ctxP1, NewProtocolConversion(
		ConversionModeIRConverted,
		"anthropic_messages",
		"gemini_generate_content",
		"gemini",
		"gemini",
		map[string]int{"cache_mark_dropped": 1},
	))
	dispatch1Ctx := BeginRequestAttempt(ctxP1, 1)
	ref1 := RequestAttemptFromContext(dispatch1Ctx)
	ObserveRequestHeaders(dispatch1Ctx, ref1, http.Header{"X-Request-Id": []string{"up-1"}}, http.StatusBadRequest)

	// Same-provider retry on Provider 10 stages replacement request losses after 400.
	// Attempt 1 (already status 400) must keep its original snapshot while Attempt 2 receives the replacement.
	ReplaceProtocolConversion(ctxP1, NewProtocolConversion(
		ConversionModeIRConverted,
		"anthropic_messages",
		"gemini_generate_content",
		"gemini",
		"gemini",
		map[string]int{"cache_mark_dropped": 1, "replay_token_dropped": 2},
	))
	dispatch2Ctx := BeginRequestAttempt(ctxP1, 2)
	ref2 := RequestAttemptFromContext(dispatch2Ctx)
	ObserveRequestHeaders(dispatch2Ctx, ref2, http.Header{"X-Request-Id": []string{"up-2"}}, http.StatusServiceUnavailable)

	// Failover to Provider 20 running lossless native passthrough: fresh scope must start nil.
	ctxP2 := WithRequestProvider(base, 20, "X-Request-ID")
	require.Nil(t, ProtocolConversionFromContext(ctxP2))
	dispatch3Ctx := BeginRequestAttempt(ctxP2, 3)
	ref3 := RequestAttemptFromContext(dispatch3Ctx)
	ObserveRequestHeaders(dispatch3Ctx, ref3, http.Header{"X-Request-Id": []string{"up-3"}}, http.StatusOK)
	ObserveRequestResult(dispatch3Ctx, ref3, nil)

	require.Len(t, latest.Attempts, 3)
	require.NotNil(t, latest.Attempts[0].ProtocolConversion)
	require.Equal(t, map[string]int{"cache_mark_dropped": 1}, latest.Attempts[0].ProtocolConversion.LossCounts)

	require.NotNil(t, latest.Attempts[1].ProtocolConversion)
	require.Equal(t, map[string]int{"cache_mark_dropped": 1, "replay_token_dropped": 2}, latest.Attempts[1].ProtocolConversion.LossCounts)

	require.Nil(t, latest.Attempts[2].ProtocolConversion)
}

// TestCopyProtocolConversionScopeAndWithoutRequestCapture verifies that
// CopyProtocolConversionScope shares one mutable conversion slot across derived
// WebSocket turn contexts while WithoutRequestCapture masks parent scopes.
func TestCopyProtocolConversionScopeAndWithoutRequestCapture(t *testing.T) {
	turnCtx := WithProtocolConversionScope(context.Background())
	callbackCtx := CopyProtocolConversionScope(context.Background(), turnCtx)
	ObserveProtocolConversion(callbackCtx, NewProtocolConversion(
		ConversionModeNativeReplay,
		"openai_responses_websocket",
		"openai_responses_websocket",
		"openai",
		"openai",
		nil,
	))

	got := ProtocolConversionFromContext(turnCtx)
	require.NotNil(t, got)
	require.Equal(t, ConversionModeNativeReplay, got.Mode)

	masked := WithoutRequestCapture(turnCtx)
	require.Nil(t, ProtocolConversionFromContext(masked))
}

// TestWithRequestProvider_SameProviderRetryIsolatesScopeAndPreservesFailedAttempt
// reproduces a pool-mode retry that selects the same provider ID after an HTTP
// 502 failure. Re-invoking WithRequestProvider on the previous attempt's context
// must start an empty conversion scope so the retry records single loss counts
// without double-counting while leaving Attempt 1's failed snapshot intact.
func TestWithRequestProvider_SameProviderRetryIsolatesScopeAndPreservesFailedAttempt(t *testing.T) {
	var latest RequestRecord
	base := WithRequestCapture(context.Background(), RequestRecord{
		RequestID: "req-same-provider-retry",
		State:     "running",
	}, func(rec RequestRecord) {
		latest = rec
	})

	// Attempt 1 on provider 10 observes both native_replay and tool_adapted, then fails with 502.
	ctxAttempt1 := WithRequestProvider(base, 10, "X-Request-ID")
	ObserveProtocolConversion(ctxAttempt1, NewProtocolConversion(
		ConversionModeNativeReplay,
		"openai_responses",
		"openai_responses",
		"openai",
		"openai",
		map[string]int{"replay_token_dropped": 1},
	))
	ObserveProtocolConversion(ctxAttempt1, NewProtocolConversion(
		ConversionModeToolAdapted,
		"openai_responses",
		"openai_responses",
		"openai",
		"",
		map[string]int{"tool_lowered": 1},
	))
	dispatch1 := BeginRequestAttempt(ctxAttempt1, 1)
	ref1 := RequestAttemptFromContext(dispatch1)
	ObserveRequestHeaders(dispatch1, ref1, http.Header{"X-Request-Id": []string{"up-502"}}, http.StatusBadGateway)

	// Pool retry selects provider 10 again on the existing request context.
	ctxAttempt2 := WithRequestProvider(ctxAttempt1, 10, "X-Request-ID")
	require.Nil(t, ProtocolConversionFromContext(ctxAttempt2), "same-provider retry must start with an empty scope")
	ObserveProtocolConversion(ctxAttempt2, NewProtocolConversion(
		ConversionModeNativeReplay,
		"openai_responses",
		"openai_responses",
		"openai",
		"openai",
		map[string]int{"replay_token_dropped": 1},
	))
	ObserveProtocolConversion(ctxAttempt2, NewProtocolConversion(
		ConversionModeToolAdapted,
		"openai_responses",
		"openai_responses",
		"openai",
		"",
		map[string]int{"tool_lowered": 1},
	))
	dispatch2 := BeginRequestAttempt(ctxAttempt2, 2)
	ref2 := RequestAttemptFromContext(dispatch2)
	ObserveRequestHeaders(dispatch2, ref2, http.Header{"X-Request-Id": []string{"up-200"}}, http.StatusOK)
	ObserveRequestResult(dispatch2, ref2, nil)

	wantLosses := map[string]int{
		"replay_token_dropped": 1,
		"tool_lowered":         1,
	}
	gotFinal := ProtocolConversionFromContext(ctxAttempt2)
	require.NotNil(t, gotFinal)
	require.Equal(t, ConversionModeToolAdapted, gotFinal.Mode)
	require.Equal(t, wantLosses, gotFinal.LossCounts)

	require.Len(t, latest.Attempts, 2)
	require.NotNil(t, latest.Attempts[0].ProtocolConversion)
	require.Equal(t, wantLosses, latest.Attempts[0].ProtocolConversion.LossCounts)
	require.NotNil(t, latest.Attempts[1].ProtocolConversion)
	require.Equal(t, wantLosses, latest.Attempts[1].ProtocolConversion.LossCounts)
}
