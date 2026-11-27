package antigravity

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// One Antigravity upstream stream feeds four client representations with
// streamed and buffered output. Fixtures record body/lifecycle release and raw
// usage observations, including a failing client sink and an upstream read
// failure after partial usage payloads.

// executionSink records synchronous events and output bytes for Antigravity
// execution fixtures.
type executionSink struct {
	body strings.Builder // successfully written fixture output bytes
	// events records supplied values without cloning their data; fixtures inspect
	// retained slices after Execute completes and keep source storage stable.
	events []upstream.OutputEvent
	// fail rejects semantic output after retaining its event, allowing fixtures
	// to model a client disconnect during useful content.
	fail bool
}

// executionBody counts response-body closes before delegating cleanup to its
// wrapped reader.
type executionBody struct {
	io.ReadCloser // wrapped fixture response body reader
	// closes is the nonnil atomic counter shared with the assertion. Each Close
	// updates it before delegating to the wrapped body.
	closes *atomic.Int32
}

// failedAfterPayload replaces underlying EOF with a read failure after
// delivering available fixture bytes.
type failedAfterPayload struct{ io.Reader } // wrapped payload reader

// Begin accepts the fixture output head without recording any header state.
func (s *executionSink) Begin(upstream.OutputHead) error { return nil }

// Emit retains the supplied event and writes bytes unless semantic output is
// configured to fail. Event data stays shared; callers inspect recorded data
// after synchronous execution and keep its storage stable.
func (s *executionSink) Emit(e upstream.OutputEvent) error {
	s.events = append(s.events, e)
	if s.fail && e.Semantic {
		return io.ErrClosedPipe
	}
	_, _ = s.body.Write(e.Data)
	return nil
}

// Close increments the shared close counter before closing the wrapped body.
// The counter records every invocation, including repeated cleanup, while the
// delegated closer determines the returned error.
func (b *executionBody) Close() error { b.closes.Add(1); return b.ReadCloser.Close() }

// executionResponseOptions builds identity tool restoration for execution tests.
func executionResponseOptions() ResponseOptions {
	return ResponseOptions{ReverseTools: func(b []byte) []byte { return b }}
}

// TestExecuteProtocolOutputsAndRelease runs all four client representations in
// streamed and buffered modes. Each attempt must retain output,
// request/model/usage observations, semantic output, and one body close and
// lifecycle release.
func TestExecuteProtocolOutputsAndRelease(t *testing.T) {
	cases := []struct {
		name string              // human-readable client representation name
		mode ResponseMode        // selected output adapter mode
		wire protocol.ProtocolID // admitted client protocol identity
	}{
		{"claude", ModeConvertedResponse, protocol.ProtocolAnthropicMessages},
		{"gemini", ModeGeminiResponse, protocol.ProtocolGeminiGenerateContent},
		{"chat", ModeConvertedResponse, protocol.ProtocolOpenAIChatCompletions},
		{"responses", ModeConvertedResponse, protocol.ProtocolOpenAIResponses},
	}
	for _, tc := range cases {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%v", tc.name, stream), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					w.Header().Set("x-request-id", "fixture-request")
					_, _ = io.WriteString(w, "data: {\"response\":{\"modelVersion\":\"gemini-runtime\",\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"hello\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":3,\"candidatesTokenCount\":1,\"totalTokenCount\":4}}}\n\n")
				}))
				defer server.Close()
				var closes, releases atomic.Int32
				sink := &executionSink{}
				target := &Target{
					ProviderID: 12,
					Model:      "gemini-fixture",
					Mode:       tc.mode,
					Response:   executionResponseOptions(),
					Enter:      func() (func(), error) { return func() { releases.Add(1) }, nil },
					Exchange: func(ctx context.Context) (*http.Response, error) {
						req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, nil)
						if err != nil {
							return nil, err
						}
						resp, err := server.Client().Do(req)
						if resp != nil {
							resp.Body = &executionBody{ReadCloser: resp.Body, closes: &closes}
						}
						return resp, err
					},
				}
				if tc.mode == ModeConvertedResponse {
					reqBytes := buildTestClientRequest(tc.wire, "public-model", stream)
					target.Converted = ConvertedOptions{
						Exchange: newTestAntigravityExchange(t, tc.wire, reqBytes, "gemini-fixture", false, nil),
					}
				}
				result, err := (Executor{}).Execute(context.Background(), upstream.AttemptInput{Protocol: tc.wire, ResponseModel: "public-model", Stream: stream, Target: target}, sink)
				require.NoError(t, err)
				require.Contains(t, sink.body.String(), "hello")
				require.Equal(t, "fixture-request", result.RequestID)
				require.Equal(t, "gemini-runtime", result.UpstreamResponseModel)
				require.Equal(t, 3, result.Usage.InputTokens)
				require.Equal(t, 1, result.Usage.OutputTokens)
				require.True(t, result.HasUsage)
				require.True(t, result.Served)
				require.EqualValues(t, 1, closes.Load())
				require.EqualValues(t, 1, releases.Load())
				var semantic bool
				for _, event := range sink.events {
					semantic = semantic || event.Semantic
				}
				require.True(t, semantic, "emitted output must report semantic content")
			})
		}
	}
}

// TestExecuteDisconnectedStreamStillObservesTailUsage fails the sink when
// semantic stream output arrives before a later usage event. Execution must
// report client disconnect while retaining tail token observations and
// releasing the owned body and lifecycle entry through their close counters.
func TestExecuteDisconnectedStreamStillObservesTailUsage(t *testing.T) {
	body := "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"hello\"}]}}]}}\n\ndata: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":9,\"candidatesTokenCount\":4}}}\n\n"
	var closes, releases atomic.Int32
	reqBytes := buildTestClientRequest(protocol.ProtocolAnthropicMessages, "fixture", true)
	ex := newTestAntigravityExchange(t, protocol.ProtocolAnthropicMessages, reqBytes, "claude-sonnet-4-5", true, nil)
	target := &Target{
		Mode:      ModeConvertedResponse,
		Response:  executionResponseOptions(),
		Converted: ConvertedOptions{Exchange: ex},
		Enter:     func() (func(), error) { return func() { releases.Add(1) }, nil },
		Exchange: func(context.Context) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: &executionBody{ReadCloser: io.NopCloser(strings.NewReader(body)), closes: &closes}}, nil
		},
	}
	result, err := (Executor{}).Execute(context.Background(), upstream.AttemptInput{Protocol: protocol.ProtocolAnthropicMessages, ResponseModel: "fixture", Stream: true, Target: target}, &executionSink{fail: true})
	require.NoError(t, err)
	require.True(t, result.ClientDisconnect)
	require.True(t, result.HasUsage)
	require.Equal(t, 9, result.Usage.InputTokens)
	require.Equal(t, 4, result.Usage.OutputTokens)
	require.EqualValues(t, 1, closes.Load())
	require.EqualValues(t, 1, releases.Load())
}

// TestExecuteFailedBeforeResponseReleasesOwnedBody returns a policy failure
// before response adaptation. Execution must retain unserved/no-usage status
// and close the owned body and lifecycle registration once.
func TestExecuteFailedBeforeResponseReleasesOwnedBody(t *testing.T) {
	var closes, releases atomic.Int32
	want := errors.New("policy rejected")
	target := &Target{Response: executionResponseOptions(), Enter: func() (func(), error) { return func() { releases.Add(1) }, nil }, Exchange: func(context.Context) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusForbidden, Header: http.Header{}, Body: &executionBody{ReadCloser: io.NopCloser(strings.NewReader("denied")), closes: &closes}}, nil
	}, BeforeResponse: func(context.Context, *http.Response) (bool, error) { return true, want }}
	result, err := (Executor{}).Execute(context.Background(), upstream.AttemptInput{Protocol: protocol.ProtocolAnthropicMessages, Target: target}, &executionSink{})
	require.ErrorIs(t, err, want)
	require.False(t, result.Served)
	require.False(t, result.HasUsage)
	require.EqualValues(t, 1, closes.Load())
	require.EqualValues(t, 1, releases.Load())
}

// Read delegates payload delivery and replaces EOF with io.ErrUnexpectedEOF.
// Available bytes and other errors pass through, allowing the executor fixture
// to observe usage before an upstream stream failure.
func (r failedAfterPayload) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// TestExecutePartialUsageSurvivesReadFailure splits input/output usage across
// payloads before an unexpected EOF. The failed attempt must retain both
// observed token counts and report served content and usage.
func TestExecutePartialUsageSurvivesReadFailure(t *testing.T) {
	body := "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"hello\"}]}}],\"usageMetadata\":{\"promptTokenCount\":9}}}\n\ndata: {\"response\":{\"usageMetadata\":{\"candidatesTokenCount\":4}}}\n\n"
	reqBytes := buildTestClientRequest(protocol.ProtocolAnthropicMessages, "fixture", true)
	ex := newTestAntigravityExchange(t, protocol.ProtocolAnthropicMessages, reqBytes, "claude-sonnet-4-5", true, nil)
	target := &Target{
		Mode:      ModeConvertedResponse,
		Response:  executionResponseOptions(),
		Converted: ConvertedOptions{Exchange: ex},
		Exchange: func(context.Context) (*http.Response, error) {
			// Replace only EOF after fixture payload delivery so the failure follows
			// separately observed usage fields rather than preventing their initial
			// reads.
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(failedAfterPayload{Reader: strings.NewReader(body)})}, nil
		},
	}
	result, err := (Executor{}).Execute(context.Background(), upstream.AttemptInput{Protocol: protocol.ProtocolAnthropicMessages, Stream: true, ResponseModel: "fixture", Target: target}, &executionSink{})
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
	require.True(t, result.Served)
	require.True(t, result.HasUsage)
	require.Equal(t, 9, result.Usage.InputTokens)
	require.Equal(t, 4, result.Usage.OutputTokens)
}

// TestExecuteConvertedModeStreamingBufferedAndCollisionGuard runs
// ModeConvertedResponse across Anthropic Messages, OpenAI Responses, and OpenAI
// Chat Completions in streaming and buffered modes, then checks colliding usage
// rejection. Executor must record UpstreamResponseModel, retain sparse usage
// across read failures, and reject colliding usageMetadata keys via wirejson.
func TestExecuteConvertedModeStreamingBufferedAndCollisionGuard(t *testing.T) {
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%v", proto, stream), func(t *testing.T) {
				reqBytes := buildTestClientRequest(proto, "claude-opus-4-6-thinking", stream)
				ex := newTestAntigravityExchange(t, proto, reqBytes, "claude-opus-4-6-thinking", true, nil)

				sseBody := "data: {\"responseId\":\"exec_conv_1\",\"modelVersion\":\"claude-opus-4-6-runtime\",\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"converted ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":14,\"cachedContentTokenCount\":4,\"candidatesTokenCount\":6,\"thoughtsTokenCount\":2}}}\n\n"
				var closes, releases atomic.Int32
				sink := &convertedTestSink{}
				target := &Target{
					ProviderID: 21,
					Model:      "claude-opus-4-6-thinking",
					Mode:       ModeConvertedResponse,
					Converted:  ConvertedOptions{Exchange: ex},
					Enter:      func() (func(), error) { return func() { releases.Add(1) }, nil },
					Exchange: func(context.Context) (*http.Response, error) {
						return &http.Response{
							StatusCode: http.StatusOK,
							Header:     http.Header{"X-Request-Id": []string{"exec-conv-req"}},
							Body:       &executionBody{ReadCloser: io.NopCloser(strings.NewReader(sseBody)), closes: &closes},
						}, nil
					},
				}

				result, err := (Executor{}).Execute(
					t.Context(),
					upstream.AttemptInput{
						Protocol:      proto,
						ResponseModel: "claude-opus-4-6-thinking",
						Stream:        stream,
						Target:        target,
					},
					sink,
				)
				require.NoError(t, err)
				require.Contains(t, sink.body.String(), "converted ok")
				require.Equal(t, "exec-conv-req", result.RequestID)
				require.Equal(t, "claude-opus-4-6-runtime", result.UpstreamResponseModel)
				require.Equal(t, 10, result.Usage.InputTokens)
				require.Equal(t, 4, result.Usage.CacheReadInputTokens)
				require.Equal(t, 8, result.Usage.OutputTokens)
				require.True(t, result.HasUsage)
				require.True(t, result.Served)
				require.EqualValues(t, 1, closes.Load())
				require.EqualValues(t, 1, releases.Load())
			})
		}
	}

	t.Run("colliding_usage_never_marks_has_usage", func(t *testing.T) {
		reqBytes := buildTestClientRequest(protocol.ProtocolOpenAIResponses, "gemini-3.1-pro-high", true)
		ex := newTestAntigravityExchange(t, protocol.ProtocolOpenAIResponses, reqBytes, "gemini-3.1-pro-high", false, nil)
		colliding := "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"x\"}]}}],\"usageMetadata\":{\"promptTokenCount\":1,\"promptTokenCount\":99}}}\n\n"
		target := &Target{
			Mode:      ModeConvertedResponse,
			Converted: ConvertedOptions{Exchange: ex},
			Exchange: func(context.Context) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader(colliding)),
				}, nil
			},
		}
		res, err := (Executor{}).Execute(
			t.Context(),
			upstream.AttemptInput{
				Protocol:      protocol.ProtocolOpenAIResponses,
				ResponseModel: "gemini-3.1-pro-high",
				Stream:        true,
				Target:        target,
			},
			&convertedTestSink{},
		)
		require.Error(t, err)
		require.False(t, res.HasUsage)
		require.Zero(t, res.Usage.InputTokens)
	})
}
