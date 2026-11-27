package qoder

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Stream fixtures check Qoder usage merging across cached and uncached prompt
// buckets alongside wrapped SSE scanning, cancellation wakeup, and upstream
// error decoding.

// qoderCachedUsageEventForTest retains a shared cache-bearing usage fixture.
// Tests copy nested details before changing values so other cases keep the same
// observation.
var qoderCachedUsageEventForTest = SSEEvent{
	Type:             "usage",
	PromptTokens:     66637,
	CompletionTokens: 6,
	TotalTokens:      66643,
	UsageDetails: UsageDetails{
		PromptTokensDetails:     &PromptTokensDetails{CachedTokens: 66612, CacheableTokens: 19},
		CompletionTokensDetails: &CompletionTokensDetails{ReasoningTokens: 0},
	},
	HasUsage: true,
}

// TestQoderGatewayUsageSplitsCachedPromptTokens supplies prompt totals with
// cached-token detail and completion usage. It checks uncached input, cache
// reads, output, and zero cache creation independently.
func TestQoderGatewayUsageSplitsCachedPromptTokens(t *testing.T) {
	usage := QoderUsageFromEvents([]SSEEvent{qoderCachedUsageEventForTest})
	require.Equal(t, 25, usage.InputTokens)
	require.Equal(t, 66612, usage.CacheReadInputTokens)
	require.Equal(t, 6, usage.OutputTokens)
	require.Equal(t, 0, usage.CacheCreationInputTokens)
}

// TestQoderGatewayUsageClampsCachedPromptTokens supplies cache reads larger than
// the prompt count. It checks uncached input clamps to zero while cache-read
// evidence retains the larger reported amount.
func TestQoderGatewayUsageClampsCachedPromptTokens(t *testing.T) {
	event := qoderCachedUsageEventForTest
	// The usage fixture retains a shared detail pointer. Copy that object before
	// changing cached tokens so this clamp scenario cannot alter observations used
	// by other tests.
	promptDetails := *event.UsageDetails.PromptTokensDetails
	event.UsageDetails.PromptTokensDetails = &promptDetails
	event.UsageDetails.PromptTokensDetails.CachedTokens = 70000
	usage := QoderUsageFromEvents([]SSEEvent{event})
	require.Equal(t, 0, usage.InputTokens)
	require.Equal(t, 70000, usage.CacheReadInputTokens)
}

// TestQoderGatewayUsageKeepsOldBehaviorWhenDetailsMissing supplies primary token
// counts with no prompt details. It checks full prompt input, zero cache reads,
// and retained output usage.
func TestQoderGatewayUsageKeepsOldBehaviorWhenDetailsMissing(t *testing.T) {
	event := SSEEvent{Type: "usage", PromptTokens: 12, CompletionTokens: 34, TotalTokens: 46, HasUsage: true}
	usage := QoderUsageFromEvents([]SSEEvent{event})
	require.Equal(t, 12, usage.InputTokens)
	require.Equal(t, 0, usage.CacheReadInputTokens)
	require.Equal(t, 34, usage.OutputTokens)
}

// TestQoderGatewayReadsWrappedSSE reads wrapped reasoning, visible text, usage,
// and terminal sentinel frames through ReadQoderSSEEventsContext. It checks
// event types, extracted text, usage counts, and terminal observation.
func TestQoderGatewayReadsWrappedSSE(t *testing.T) {
	resp := &http.Response{
		Body: io.NopCloser(bytes.NewBufferString(
			"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"reasoning_content\\\":\\\"hidden thought\\\"}}]}\"}\n\n" +
				"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"Hi\\\"}}]}\"}\n\n" +
				"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":3,\\\"completion_tokens\\\":4,\\\"total_tokens\\\":7}}\"}\n\n" +
				"data: {\"body\":\"[DONE]\"}\n\n",
		)),
	}

	events, err := ReadQoderSSEEventsContext(context.Background(), resp)
	require.NoError(t, err)
	require.Len(t, events, 4)
	require.Equal(t, "reasoning_delta", events[0].Type)
	require.Equal(t, "hidden thought", events[0].Text)
	require.Equal(t, "text_delta", events[1].Type)
	require.Equal(t, "Hi", events[1].Text)
	require.True(t, events[2].HasUsage)
	require.Equal(t, 3, events[2].PromptTokens)
	require.Equal(t, 4, events[2].CompletionTokens)
	require.True(t, events[3].IsDone)
}

// TestQoderGatewayScannerStopsWhenResultSendIsCanceled stops consumption after
// the first scanner batch and cancels its context. It checks the producer
// goroutine exits while later results cannot drain normally.
func TestQoderGatewayScannerStopsWhenResultSendIsCanceled(t *testing.T) {
	line := "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"Hi\\\"}}]}\"}\n\n"
	resp := &http.Response{Body: io.NopCloser(strings.NewReader(strings.Repeat(line, 10)))}
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan QoderEventResult, 1)
	done := make(chan struct{})

	// Leave later scanner batches buffered after observing the first result.
	// Cancellation then needs to release the producer's send independently of
	// ordinary channel draining.
	go func() {
		ScanQoderEvents(ctx, resp, results)
		close(done)
	}()

	select {
	case <-results:
	case <-time.After(time.Second):
		t.Fatal("scanner did not emit first event")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("scanner goroutine did not exit after context cancellation")
	}
}

// TestQoderGatewayReadsWrappedSSEUpstreamError reads a wrapped signature
// rejection from a response body. It checks empty collected events and typed
// status/code/message information in the returned error.
func TestQoderGatewayReadsWrappedSSEUpstreamError(t *testing.T) {
	resp := &http.Response{
		Body: io.NopCloser(bytes.NewBufferString(
			"data: {\"headers\":{\"Content-Type\":[\"application/json\"]},\"body\":\"{\\\"code\\\":\\\"101\\\",\\\"message\\\":\\\"Signature invalid\\\"}\",\"statusCodeValue\":403,\"statusCode\":\"FORBIDDEN\"}\n\n",
		)),
	}

	events, err := ReadQoderSSEEventsContext(context.Background(), resp)
	require.Error(t, err)
	require.Empty(t, events)
	var apiErr *APIError
	require.True(t, errors.As(err, &apiErr))
	require.Equal(t, 403, apiErr.StatusCode)
	require.Equal(t, "101", apiErr.Code)
	require.Equal(t, "Signature invalid", apiErr.Message)
	require.Equal(t, "Qoder upstream error 101: Signature invalid", apiErr.Error())
}
