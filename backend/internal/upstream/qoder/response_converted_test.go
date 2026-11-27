package qoder

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// response_converted_test.go verifies IR-backed Qoder streaming and buffered
// response readers across Messages, Responses, and Chat protocols, covering
// QoderToolNameMapper restoration, trc1:plain carriers, pre-semantic staging,
// keepalive-committed terminal SSE errors, and post-disconnect usage draining.

// recordingOutputSink captures downstream OutputHead and OutputEvent writes,
// exposes pre-initialization OutputState metadata, and simulates client socket
// disconnection after failAfterEmits writes.
type recordingOutputSink struct {
	begun     bool                // whether Begin was invoked
	head      upstream.OutputHead // captured response head from Begin
	liveState upstream.OutputHead // live commitment state returned by OutputState
	// emits and failAfterEmits coordinate simulated downstream write disconnects:
	// Emit increments emits on every call and returns io.ErrClosedPipe once
	// failAfterEmits is positive and emits reaches that threshold.
	emits          int          // 1-based count of Emit calls attempted
	failAfterEmits int          // Emit call threshold triggering io.ErrClosedPipe, or 0
	buf            bytes.Buffer // concatenated emitted payload bytes
}

// TestReadConvertedStreamingAndBuffered_AllProtocolsToolMapperAndCarriers
// exercises Executor across Messages, Responses, and Chat in streaming and
// buffered modes. It verifies QoderToolNameMapper + raiseTools restoration,
// trc1:plain carrier sealing, billing usage parity, and pre-conversion
// UpstreamResponseModel observation.
func TestReadConvertedStreamingAndBuffered_AllProtocolsToolMapperAndCarriers(t *testing.T) {
	t.Run("responses custom and namespaced tools round-trip with QoderToolNameMapper and plain reasoning carrier", func(t *testing.T) {
		for _, streamMode := range []bool{true, false} {
			t.Run(map[bool]string{true: "stream", false: "buffered"}[streamMode], func(t *testing.T) {
				qoderSSE := strings.Join([]string{
					`data: {"body":"{\"model\":\"qwen-vendor-internal\",\"choices\":[{\"delta\":{\"reasoning_content\":\"step one \"}}]}"}`,
					`data: {"body":"{\"choices\":[{\"delta\":{\"reasoning_content\":\"step two\"}}]}"}`,
					`data: {"body":"{\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_custom_1\",\"type\":\"function\",\"function\":{\"name\":\"shell\",\"arguments\":\"{\\\"input\\\":\\\"ls -la\\\"}\"}},{\"index\":1,\"id\":\"call_ns_1\",\"type\":\"function\",\"function\":{\"name\":\"OPS__READ_LOG\",\"arguments\":\"{\\\"path\\\":\\\"/tmp/a.log\\\"}\"}}]}}]}"}`,
					`data: {"body":"{\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":25,\"prompt_tokens_details\":{\"cached_tokens\":40,\"cacheable_tokens\":80}}}"}`,
					`data: {"body":"{\"event\":\"message_delta\",\"data\":{\"usage\":{\"output_tokens\":30}}}"}`,
					`data: {"body":"[DONE]"}`,
					"",
				}, "\n\n")

				executor := NewExecutor(ExecuteOptions{})
				target := &Target{
					ProviderID: 11,
					Site:       SiteGlobal,
					Metadata:   RequestMetadata{APIKeyID: 99},
					Session:    func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
					Client: func() (StreamClient, error) {
						return qoderCaptureClientFunc(func(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     http.Header{"X-Upstream-Trace": []string{"trace-1"}},
								Body:       io.NopCloser(strings.NewReader(qoderSSE)),
							}, nil
						}), nil
					},
				}

				reqBody := []byte(`{
					"model":"auto",
					"session_id":"sess-resp-1",
					"stream":` + map[bool]string{true: "true", false: "false"}[streamMode] + `,
					"tools":[
						{"type":"custom","name":"bash","description":"Run shell command"},
						{"type":"namespace","name":"ops","description":"Ops tools","tools":[
							{"type":"function","name":"read_log","description":"Read log","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}
						]}
					],
					"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Check logs"}]}]
				}`)

				sink := &modelTestSink{}
				res, err := executor.Execute(context.Background(), upstream.AttemptInput{
					Protocol:      protocol.ProtocolOpenAIResponses,
					Target:        target,
					Body:          reqBody,
					ResponseModel: "gpt-5-alias",
					Stream:        streamMode,
				}, sink)
				require.NoError(t, err)
				require.True(t, res.Served)
				require.True(t, res.HasUsage)
				require.NotEmpty(t, res.RequestID)
				require.Equal(t, "qwen-vendor-internal", res.UpstreamResponseModel)
				// prompt_tokens=100 with cached_tokens=40 splits to Input=60, CacheRead=40,
				// CacheWrite=0, and trailing message_delta updates Output to 30 while preserving Input/CacheRead.
				require.Equal(t, upstream.TokenUsage{
					InputTokens:              60,
					OutputTokens:             30,
					CacheCreationInputTokens: 0,
					CacheReadInputTokens:     40,
				}, res.Usage)

				out := sink.String()
				require.Contains(t, out, res.RequestID)
				require.Contains(t, out, `"model":"gpt-5-alias"`)
				require.Contains(t, out, `"store":false`)
				require.Contains(t, out, `"type":"custom_tool_call"`)
				require.Contains(t, out, `"name":"bash"`)
				require.Contains(t, out, `"input":"ls -la"`)
				require.Contains(t, out, `"namespace":"ops"`)
				require.Contains(t, out, `"name":"read_log"`)
				require.Contains(t, out, `"input_tokens":100`)
				require.Contains(t, out, `"cached_tokens":40`)
				require.Contains(t, out, `"output_tokens":30`)
				require.Contains(t, out, "trc1:plain:")
			})
		}
	})

	t.Run("messages seals plain reasoning signature and maps XML text tool call back to declared lowercase name", func(t *testing.T) {
		for _, streamMode := range []bool{true, false} {
			t.Run(map[bool]string{true: "stream", false: "buffered"}[streamMode], func(t *testing.T) {
				qoderSSE := strings.Join([]string{
					`data: {"body":"{\"choices\":[{\"delta\":{\"reasoning_content\":\"plan todo\"}}]}"}`,
					`data: {"body":"{\"choices\":[{\"delta\":{\"content\":\"<tool_call>{\\\"name\\\":\\\"TodoWrite\\\",\\\"arguments\\\":{\\\"todos\\\":[\\\"ship\\\"]}}</tool_call>\"}}]}"}`,
					`data: {"body":"{\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":8}}"}`,
					`data: {"body":"[DONE]"}`,
					"",
				}, "\n\n")

				executor := NewExecutor(ExecuteOptions{})
				target := &Target{
					ProviderID: 12,
					Site:       SiteGlobal,
					Session:    func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
					Client: func() (StreamClient, error) {
						return qoderCaptureClientFunc(func(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     http.Header{},
								Body:       io.NopCloser(strings.NewReader(qoderSSE)),
							}, nil
						}), nil
					},
				}

				reqBody := []byte(`{
					"model":"ultimate",
					"max_tokens":128,
					"stream":` + map[bool]string{true: "true", false: "false"}[streamMode] + `,
					"tools":[{"name":"todo_write","description":"Save todos","input_schema":{"type":"object","properties":{"todos":{"type":"array"}}}}],
					"messages":[{"role":"user","content":"track tasks"}]
				}`)
				sink := &modelTestSink{}
				res, err := executor.Execute(context.Background(), upstream.AttemptInput{
					Protocol:      protocol.ProtocolAnthropicMessages,
					Target:        target,
					Body:          reqBody,
					ResponseModel: "claude-sonnet-4-5",
					Stream:        streamMode,
				}, sink)
				require.NoError(t, err)
				require.True(t, res.Served)
				require.Empty(t, res.UpstreamResponseModel)
				require.Equal(t, upstream.TokenUsage{InputTokens: 20, OutputTokens: 8}, res.Usage)

				out := sink.String()
				require.Contains(t, out, `"name":"todo_write"`)
				require.NotContains(t, out, `"name":"TodoWrite"`)
				require.Contains(t, out, "trc1:plain:")

				// Verify the emitted trc1:plain carrier parses back cleanly to "plan todo".
				idx := strings.Index(out, "trc1:plain:")
				require.GreaterOrEqual(t, idx, 0)
				rest := out[idx:]
				endQuote := strings.IndexByte(rest, '"')
				require.Greater(t, endQuote, 0)
				parsedCarrier, ok := ir.ParseCarrier(rest[:endQuote])
				require.True(t, ok)
				require.Equal(t, ir.SchemePlain, parsedCarrier.Scheme)
				require.Equal(t, "plan todo", parsedCarrier.Text)
			})
		}
	})
}

// TestReadConvertedStreaming_StagingWindowCommittedSSEAndMidStreamFailure
// exercises streaming failure and disconnect paths. It verifies pre-semantic
// frame staging for failover, CommittedSSE and OutputState terminal error
// emission, mid-stream block closure with partial usage, and post-disconnect
// upstream usage draining.
func TestReadConvertedStreaming_StagingWindowCommittedSSEAndMidStreamFailure(t *testing.T) {
	t.Run("pre-semantic wrapped 115 agent limit stages preamble and leaves sink uncommitted for failover", func(t *testing.T) {
		// Emit a usage-only non-semantic frame (which opens response_start / message_start
		// inside StreamEncoder with Semantic=false) followed by a wrapped 502 code 115 error.
		qoderSSE := strings.Join([]string{
			`data: {"body":"{\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":0}}"}`,
			`data: {"statusCodeValue":502,"body":"{\"code\":\"115\",\"message\":\"Agent quota reached bearer secret-tok-999\",\"agentLimitResetTime\":1893456000}"}`,
			"",
		}, "\n\n")

		store := NewQoderConversationStore(QoderConversationTTL)
		executor := NewExecutor(ExecuteOptions{Conversations: store})
		sink := &recordingOutputSink{}
		target := &Target{
			ProviderID: 21,
			Site:       SiteGlobal,
			Metadata:   RequestMetadata{APIKeyID: 5},
			Session:    func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
			Client: func() (StreamClient, error) {
				return qoderCaptureClientFunc(func(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{},
						Body:       io.NopCloser(strings.NewReader(qoderSSE)),
					}, nil
				}), nil
			},
		}

		res, err := executor.Execute(context.Background(), upstream.AttemptInput{
			Protocol: protocol.ProtocolAnthropicMessages,
			Target:   target,
			Body:     []byte(`{"model":"auto","max_tokens":64,"session_id":"s-pre","messages":[{"role":"user","content":"hi"}]}`),
			Stream:   true,
		}, sink)
		var apiErr *APIError
		require.ErrorAs(t, err, &apiErr)
		require.True(t, apiErr.IsAgentLimit())
		require.True(t, MaySwitchAttempt(err))
		require.NotContains(t, apiErr.Message, "secret-tok-999")
		require.False(t, sink.begun, "staged non-semantic preamble must not commit HTTP 200 before semantic output")
		require.Zero(t, sink.buf.Len())
		require.False(t, res.Served)
		require.Empty(t, store.Items, "RollbackAccepted must remove uncommitted conversation state")
	})

	t.Run("keepalive-committed SSE stream emits untimed local terminal error unless retry deferral is active", func(t *testing.T) {
		errSSE := "data: {\"statusCodeValue\":429,\"body\":\"{\\\"code\\\":\\\"429\\\",\\\"message\\\":\\\"Too many requests\\\"}\"}\n\n"

		for _, deferRetry := range []bool{false, true} {
			t.Run(map[bool]string{false: "standalone_emits", true: "deferred_skips_retryable"}[deferRetry], func(t *testing.T) {
				localTerminalCalls := 0
				var gotStatus int
				var gotType string
				executor := NewExecutor(ExecuteOptions{})
				target := &Target{
					ProviderID:                   22,
					Site:                         SiteGlobal,
					CommittedSSE:                 func() bool { return true },
					DeferRetryableCommittedError: deferRetry,
					WriteLocalTerminalError: func(status int, errType, _, _ string) {
						localTerminalCalls++
						gotStatus = status
						gotType = errType
					},
					Session: func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
					Client: func() (StreamClient, error) {
						return qoderCaptureClientFunc(func(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error) {
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     http.Header{},
								Body:       io.NopCloser(strings.NewReader(errSSE)),
							}, nil
						}), nil
					},
				}
				_, err := executor.Execute(context.Background(), upstream.AttemptInput{
					Protocol: protocol.ProtocolOpenAIResponses,
					Target:   target,
					Body:     []byte(`{"model":"auto","stream":true,"input":"hi"}`),
					Stream:   true,
				}, &recordingOutputSink{})
				require.Error(t, err)
				if deferRetry {
					require.Zero(t, localTerminalCalls)
				} else {
					require.Equal(t, 1, localTerminalCalls)
					require.Equal(t, http.StatusTooManyRequests, gotStatus)
					require.Equal(t, "rate_limit_error", gotType)
				}
			})
		}
	})

	t.Run("pre-committed sink OutputState triggers local terminal error through executionOutput when Target.CommittedSSE is nil", func(t *testing.T) {
		errSSE := "data: {\"statusCodeValue\":502,\"body\":\"{\\\"code\\\":\\\"500\\\",\\\"message\\\":\\\"Gateway upstream error\\\"}\"}\n\n"
		localTerminalCalls := 0
		var gotStatus int
		var gotType string
		executor := NewExecutor(ExecuteOptions{})
		target := &Target{
			ProviderID: 25,
			Site:       SiteGlobal,
			WriteLocalTerminalError: func(status int, errType, _, _ string) {
				localTerminalCalls++
				gotStatus = status
				gotType = errType
			},
			Session: func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
			Client: func() (StreamClient, error) {
				return qoderCaptureClientFunc(func(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{},
						Body:       io.NopCloser(strings.NewReader(errSSE)),
					}, nil
				}), nil
			},
		}
		sink := &recordingOutputSink{
			liveState: upstream.OutputHead{
				Status:    http.StatusOK,
				Committed: true,
			},
		}
		_, err := executor.Execute(context.Background(), upstream.AttemptInput{
			Protocol: protocol.ProtocolAnthropicMessages,
			Target:   target,
			Body:     []byte(`{"model":"auto","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`),
			Stream:   true,
		}, sink)
		require.Error(t, err)
		require.Equal(t, 1, localTerminalCalls)
		require.Equal(t, http.StatusBadGateway, gotStatus)
		require.Equal(t, "upstream_error", gotType)
	})

	t.Run("mid-stream failure closes open blocks via WriteRawTerminalSSE and returns partial usage", func(t *testing.T) {
		qoderSSE := strings.Join([]string{
			`data: {"body":"{\"choices\":[{\"delta\":{\"content\":\"partial text\"}}]}"}`,
			`data: {"body":"{\"usage\":{\"prompt_tokens\":14,\"completion_tokens\":4}}"}`,
			`data: {"statusCodeValue":502,"body":"{\"code\":\"500\",\"message\":\"upstream exploded\"}"}`,
			"",
		}, "\n\n")

		var rawTerminal []byte
		store := NewQoderConversationStore(QoderConversationTTL)
		executor := NewExecutor(ExecuteOptions{Conversations: store})
		sink := &recordingOutputSink{}
		target := &Target{
			ProviderID: 23,
			Site:       SiteGlobal,
			Metadata:   RequestMetadata{APIKeyID: 8},
			WriteRawTerminalSSE: func(data []byte) bool {
				rawTerminal = append([]byte(nil), data...)
				return true
			},
			Session: func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
			Client: func() (StreamClient, error) {
				return qoderCaptureClientFunc(func(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{},
						Body:       io.NopCloser(strings.NewReader(qoderSSE)),
					}, nil
				}), nil
			},
		}

		res, err := executor.Execute(context.Background(), upstream.AttemptInput{
			Protocol: protocol.ProtocolOpenAIResponses,
			Target:   target,
			Body:     []byte(`{"model":"auto","session_id":"s-mid","stream":true,"input":"hi"}`),
			Stream:   true,
		}, sink)
		require.Error(t, err)
		require.True(t, res.Served)
		require.True(t, res.HasUsage)
		require.Equal(t, 14, res.Usage.InputTokens)
		require.Equal(t, 4, res.Usage.OutputTokens)
		require.Contains(t, sink.buf.String(), "partial text")
		require.Contains(t, sink.buf.String(), "response.output_item.done")
		require.Contains(t, string(rawTerminal), "event: response.failed")
		require.Contains(t, string(rawTerminal), res.RequestID)
		require.Empty(t, store.Items)
	})

	t.Run("client write disconnect stops downstream writes and drains trailing upstream usage", func(t *testing.T) {
		qoderSSE := strings.Join([]string{
			`data: {"body":"{\"choices\":[{\"delta\":{\"content\":\"chunk-1\"}}]}"}`,
			`data: {"body":"{\"choices\":[{\"delta\":{\"content\":\"chunk-2\"}}]}"}`,
			`data: {"body":"{\"usage\":{\"prompt_tokens\":30,\"completion_tokens\":9,\"prompt_tokens_details\":{\"cached_tokens\":10}}}"}`,
			`data: {"body":"[DONE]"}`,
			"",
		}, "\n\n")

		sink := &recordingOutputSink{failAfterEmits: 1}
		executor := NewExecutor(ExecuteOptions{})
		target := &Target{
			ProviderID: 24,
			Site:       SiteGlobal,
			Session:    func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
			Client: func() (StreamClient, error) {
				return qoderCaptureClientFunc(func(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error) {
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{},
						Body:       io.NopCloser(strings.NewReader(qoderSSE)),
					}, nil
				}), nil
			},
		}

		res, err := executor.Execute(context.Background(), upstream.AttemptInput{
			Protocol: protocol.ProtocolOpenAIChatCompletions,
			Target:   target,
			Body:     []byte(`{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
			Stream:   true,
		}, sink)
		require.NoError(t, err)
		require.True(t, res.Served)
		require.True(t, res.HasUsage)
		require.True(t, res.ClientDisconnect)
		require.Equal(t, 20, res.Usage.InputTokens)
		require.Equal(t, 10, res.Usage.CacheReadInputTokens)
		require.Equal(t, 9, res.Usage.OutputTokens)
		require.NotContains(t, sink.buf.String(), "chunk-2")
	})
}

// Begin records h onto s.head and marks s.begun true for output header assertions.
func (s *recordingOutputSink) Begin(h upstream.OutputHead) error {
	s.begun = true
	s.head = h
	return nil
}

// OutputState returns s.liveState so NewDeferredOutputContext observes pre-committed SSE state through executionOutput.
func (s *recordingOutputSink) OutputState() upstream.OutputHead {
	return s.liveState
}

// Emit appends event.Data into s.buf or returns io.ErrClosedPipe once failAfterEmits is reached.
func (s *recordingOutputSink) Emit(event upstream.OutputEvent) error {
	s.emits++
	if s.failAfterEmits > 0 && s.emits >= s.failAfterEmits {
		return io.ErrClosedPipe
	}
	_, _ = s.buf.Write(event.Data)
	return nil
}
