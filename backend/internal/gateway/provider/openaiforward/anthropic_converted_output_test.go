package openaiforward

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
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// trailingErrReader delivers data completely before returning trailingErr on
// the next Read so tests can simulate post-message_stop socket resets.
type trailingErrReader struct {
	data        *strings.Reader // initial valid SSE payload bytes
	trailingErr error           // non-EOF error returned after data drains
}

// anthropicConvertedTestSink captures committed headers, status, regular event
// writes, untimed raw terminal writes, and optional simulated disconnect errors.
type anthropicConvertedTestSink struct {
	failAfterWrites int          // positive write index at which Emit returns io.ErrClosedPipe
	writeCount      int          // cumulative non-empty Emit invocations
	status          int          // HTTP status passed to Begin
	headers         http.Header  // cloned HTTP headers passed to Begin
	buf             bytes.Buffer // concatenated output bytes accepted by Emit
	rawTerminals    [][]byte     // terminal SSE frames delivered via WriteRawTerminalSSE
}

// Read serves remaining bytes from r.data and returns r.trailingErr once empty.
func (r *trailingErrReader) Read(p []byte) (int, error) {
	n, err := r.data.Read(p)
	if n > 0 {
		return n, nil
	}
	if errors.Is(err, io.EOF) && r.trailingErr != nil {
		return 0, r.trailingErr
	}
	return 0, err
}

// Close satisfies io.ReadCloser as a no-op for in-memory test readers.
func (*trailingErrReader) Close() error { return nil }

// Begin records the committed HTTP status and clones response headers onto s.
func (s *anthropicConvertedTestSink) Begin(head upstream.OutputHead) error {
	s.status = head.Status
	s.headers = head.Header.Clone()
	return nil
}

// Emit appends payload bytes onto s.buf or simulates a downstream disconnect
// once writeCount reaches positive failAfterWrites.
func (s *anthropicConvertedTestSink) Emit(ev upstream.OutputEvent) error {
	if len(ev.Data) == 0 {
		return nil
	}
	s.writeCount++
	if s.failAfterWrites > 0 && s.writeCount >= s.failAfterWrites {
		return io.ErrClosedPipe
	}
	_, _ = s.buf.Write(ev.Data)
	return nil
}

// newTestResponsesToAnthropicExchange creates a convert.Exchange from clientBody
// targeting ProtocolAnthropicMessages for reader unit tests.
func newTestResponsesToAnthropicExchange(t *testing.T, clientBody []byte) *convert.Exchange {
	t.Helper()
	ex, err := convert.NewExchange(clientBody, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect: convert.Dialect{
				Tools:            convert.ToolSupportFunction,
				ToolNameMaxBytes: 128,
				ReplayScheme:     "anthropic",
			},
		},
		ClientModel: "gpt-5",
		Runtime:     convert.DefaultRuntime(),
	})
	require.NoError(t, err)
	_, err = ex.UpstreamRequest()
	require.NoError(t, err)
	return ex
}

// TestReadConvertedAnthropicStreaming_LeadingFrameStagingAndSingleHealthEvaluation
// verifies that non-semantic leading frames stay staged on pre-semantic SSE
// error events so provider failover proceeds with zero flushed client bytes.
// Post-semantic error events must evaluate StreamFailure once and close open
// items with matching sequence_number and response.id via WriteRawTerminalSSE.
func TestReadConvertedAnthropicStreaming_LeadingFrameStagingAndSingleHealthEvaluation(t *testing.T) {
	t.Run("pre_semantic_overloaded_error_fails_over_cleanly", func(t *testing.T) {
		sseStream := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_p1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5-20250929\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":30,\"output_tokens\":1}}}\n\n" +
			"event: error\n" +
			"data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\n"

		sink := &anthropicConvertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestResponsesToAnthropicExchange(t, []byte(`{"model":"gpt-5","stream":true,"input":"hello"}`))
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"X-Request-Id": []string{"req_pre_1"}},
			Body:       io.NopCloser(strings.NewReader(sseStream)),
		}

		failoverSentinel := errors.New("switch-provider")
		healthCalls := 0
		res, err := ReadConvertedAnthropicStreaming(
			context.Background(),
			resp,
			outCtx,
			ex,
			ConvertedAnthropicOutputOptions{
				DefaultErrorType: "server_error",
				StreamFailure: func(_ context.Context, _ []byte, _ string, started bool) ConvertedAnthropicFailure {
					healthCalls++
					require.False(t, started, "leading message_start must stay staged before semantic content")
					return ConvertedAnthropicFailure{Failover: failoverSentinel}
				},
			},
			"gpt-5",
			"claude-sonnet-4-5",
			"claude-sonnet-4-5-20250929",
			nil,
			time.Now(),
		)
		require.ErrorIs(t, err, failoverSentinel)
		require.Nil(t, res, "failover before semantic output must return nil result to avoid double billing")
		require.Equal(t, 1, healthCalls, "provider health policy must run exactly once")
		require.Zero(t, sink.buf.Len(), "staged non-semantic frames must not flush on failover")
	})

	t.Run("post_semantic_error_evaluates_health_once_and_preserves_sequence_number", func(t *testing.T) {
		sseStream := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_post_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5-20250929\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":25,\"output_tokens\":1}}}\n\n" +
			"event: content_block_start\n" +
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\n" +
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial chunk\"}}\n\n" +
			"event: error\n" +
			"data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Mid-stream overload\"}}\n\n"

		sink := &anthropicConvertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestResponsesToAnthropicExchange(t, []byte(`{"model":"gpt-5","stream":true,"input":"hello"}`))
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"X-Request-Id": []string{"req_post_1"}},
			Body:       io.NopCloser(strings.NewReader(sseStream)),
		}

		healthCalls := 0
		res, err := ReadConvertedAnthropicStreaming(
			context.Background(),
			resp,
			outCtx,
			ex,
			ConvertedAnthropicOutputOptions{
				DefaultErrorType: "server_error",
				WriteRawTerminalSSE: func(data []byte) bool {
					sink.rawTerminals = append(sink.rawTerminals, bytes.Clone(data))
					_, _ = sink.buf.Write(data)
					return true
				},
				StreamFailure: func(_ context.Context, _ []byte, msg string, started bool) ConvertedAnthropicFailure {
					healthCalls++
					require.True(t, started, "semantic delta already flushed downstream")
					return ConvertedAnthropicFailure{
						Status:  http.StatusServiceUnavailable,
						Type:    "overloaded_error",
						Code:    "overloaded_error",
						Message: msg,
					}
				},
			},
			"gpt-5",
			"claude-sonnet-4-5",
			"claude-sonnet-4-5-20250929",
			nil,
			time.Now(),
		)
		require.Error(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, healthCalls, "mid-stream error event must evaluate health exactly once")
		require.Equal(t, "claude-sonnet-4-5-20250929", res.UpstreamResponseModel)
		require.Equal(t, 25, res.NativeUsage.InputTokens)
		require.Len(t, sink.rawTerminals, 1)
		require.Contains(t, string(sink.rawTerminals[0]), `"type":"response.failed"`)
		require.Contains(t, string(sink.rawTerminals[0]), `"sequence_number":`)
		require.Contains(t, string(sink.rawTerminals[0]), `"id":"msg_post_1"`)
	})
}

// TestReadConvertedAnthropic_DisconnectDrainUsageValidationAndOAuthToolRestore
// exercises post-disconnect trailing message_delta draining and CommittedSSE
// terminal error promotion before semantic output starts. It also confirms
// wirejson usage collision guards and pre-decoding OAuth tool-name restoration
// across both streaming and buffered readers.
func TestReadConvertedAnthropic_DisconnectDrainUsageValidationAndOAuthToolRestore(t *testing.T) {
	t.Run("client_disconnect_continues_draining_trailing_usage", func(t *testing.T) {
		sseStream := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_disc\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-opus-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":40,\"cache_read_input_tokens\":10,\"output_tokens\":1}}}\n\n" +
			"event: content_block_start\n" +
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\n" +
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"first chunk\"}}\n\n" +
			"event: content_block_stop\n" +
			"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\n" +
			"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":19}}\n\n" +
			"event: message_stop\n" +
			"data: {\"type\":\"message_stop\"}\n\n"

		sink := &anthropicConvertedTestSink{failAfterWrites: 2}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestResponsesToAnthropicExchange(t, []byte(`{"model":"gpt-5","stream":true,"input":"hello"}`))
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"X-Amzn-Requestid": []string{"amzn_req_7"}},
			Body:       io.NopCloser(strings.NewReader(sseStream)),
		}

		res, err := ReadConvertedAnthropicStreaming(
			context.Background(),
			resp,
			outCtx,
			ex,
			ConvertedAnthropicOutputOptions{DefaultErrorType: "server_error"},
			"gpt-5",
			"claude-opus-4-6",
			"us.anthropic.claude-opus-4-6-v1",
			nil,
			time.Now(),
		)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.ClientDisconnect)
		require.Equal(t, "amzn_req_7", res.RequestID)
		require.Equal(t, "claude-opus-4-6", res.UpstreamResponseModel)
		require.Equal(t, 40, res.NativeUsage.InputTokens)
		require.Equal(t, 10, res.NativeUsage.CacheReadInputTokens)
		require.Equal(t, 19, res.NativeUsage.OutputTokens, "trailing message_delta output_tokens must be drained after disconnect")
		require.Equal(t, 50, res.Usage.InputTokens)
	})

	t.Run("committed_sse_promotes_unstarted_error_and_rejects_colliding_usage", func(t *testing.T) {
		collidingSSE := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_col\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"Usage\":{\"input_tokens\":999,\"output_tokens\":999},\"usage\":{\"input_tokens\":50,\"output_tokens\":5}}}\n\n" +
			"event: error\n" +
			"data: {\"type\":\"error\",\"error\":{\"type\":\"invalid_request_error\",\"message\":\"bad param\"}}\n\n"

		sink := &anthropicConvertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestResponsesToAnthropicExchange(t, []byte(`{"model":"gpt-5","stream":true,"input":"hello"}`))
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(collidingSSE)),
		}

		localTermCalled := false
		res, err := ReadConvertedAnthropicStreaming(
			context.Background(),
			resp,
			outCtx,
			ex,
			ConvertedAnthropicOutputOptions{
				DefaultErrorType: "server_error",
				CommittedSSE:     func() bool { return true },
				WriteLocalTerminalError: func(status int, errType, _, msg string) {
					localTermCalled = true
					require.Equal(t, http.StatusBadGateway, status)
					require.Equal(t, "invalid_request_error", errType)
					require.Equal(t, "bad param", msg)
				},
			},
			"gpt-5",
			"claude-sonnet-4-5",
			"claude-sonnet-4-5",
			nil,
			time.Now(),
		)
		require.Error(t, err)
		require.NotNil(t, res)
		require.True(t, localTermCalled, "early CommittedSSE must route pre-output errors through WriteLocalTerminalError")
		require.Zero(t, res.NativeUsage.InputTokens, "colliding Usage/usage keys must be rejected by wirejson.ValidateUsageEnvelope")
	})

	t.Run("buffered_restores_oauth_tool_alias_before_custom_tool_raising", func(t *testing.T) {
		clientBody := []byte(`{"model":"gpt-5","stream":false,"input":"patch file","tools":[{"type":"custom","name":"apply_patch","description":"patch"}]}`)
		ex := newTestResponsesToAnthropicExchange(t, clientBody)

		sseStream := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_buf_tool\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":15,\"cache_creation_input_tokens\":20,\"cache_creation\":{\"ephemeral_1h_input_tokens\":20},\"output_tokens\":1}}}\n\n" +
			"event: content_block_start\n" +
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_p1\",\"name\":\"oauth_prefixed_apply_patch\",\"input\":{}}}\n\n" +
			"event: content_block_delta\n" +
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"input\\\":\\\"*** Begin Patch\\\"}\"}}\n\n" +
			"event: content_block_stop\n" +
			"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\n" +
			"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":8}}\n\n" +
			"event: message_stop\n" +
			"data: {\"type\":\"message_stop\"}\n\n"

		sink := &anthropicConvertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(sseStream)),
		}

		res, err := ReadConvertedAnthropicBuffered(
			context.Background(),
			resp,
			outCtx,
			ex,
			ConvertedAnthropicOutputOptions{
				RestoreToolNames: func(b []byte) []byte {
					return bytes.ReplaceAll(b, []byte("oauth_prefixed_apply_patch"), []byte("apply_patch"))
				},
				OverrideCache: func(context.Context) (string, bool) {
					return "5m", true
				},
			},
			"gpt-5",
			"claude-sonnet-4-5",
			"claude-sonnet-4-5",
			nil,
			time.Now(),
		)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, http.StatusOK, sink.status)
		require.Equal(t, "application/json; charset=utf-8", sink.headers.Get("Content-Type"))
		require.Equal(t, "custom_tool_call", gjson.GetBytes(sink.buf.Bytes(), "output.0.type").String())
		require.Equal(t, "apply_patch", gjson.GetBytes(sink.buf.Bytes(), "output.0.name").String())
		require.Equal(t, "*** Begin Patch", gjson.GetBytes(sink.buf.Bytes(), "output.0.input").String())
		require.Equal(t, 20, res.NativeUsage.CacheCreation5mTokens, "OverrideCache 5m must reclassify 1h billing buckets")
		require.Zero(t, res.NativeUsage.CacheCreation1hTokens)
	})

	t.Run("trailing_reset_after_message_stop_or_stop_reason_omits_duplicate_error_frame", func(t *testing.T) {
		completeSSE := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_tail_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":14,\"output_tokens\":0}}}\n\n" +
			"event: content_block_start\n" +
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\n" +
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"finished\"}}\n\n" +
			"event: content_block_stop\n" +
			"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\n" +
			"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n" +
			"event: message_stop\n" +
			"data: {\"type\":\"message_stop\"}\n\n"

		sink := &anthropicConvertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestResponsesToAnthropicExchange(t, []byte(`{"model":"gpt-5","stream":true,"input":"hi"}`))
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: &trailingErrReader{
				data:        strings.NewReader(completeSSE),
				trailingErr: io.ErrUnexpectedEOF,
			},
		}

		localTerminals := 0
		res, err := ReadConvertedAnthropicStreaming(
			context.Background(),
			resp,
			outCtx,
			ex,
			ConvertedAnthropicOutputOptions{
				DefaultErrorType: "server_error",
				WriteLocalTerminalError: func(int, string, string, string) {
					localTerminals++
				},
				WriteRawTerminalSSE: func(data []byte) bool {
					sink.rawTerminals = append(sink.rawTerminals, bytes.Clone(data))
					return true
				},
			},
			"gpt-5",
			"claude-sonnet-4-6",
			"claude-sonnet-4-6",
			nil,
			time.Now(),
		)
		require.ErrorIs(t, err, io.ErrUnexpectedEOF)
		require.NotNil(t, res)
		require.Zero(t, localTerminals, "completed stream must not emit WriteLocalTerminalError on trailing reset")
		require.Empty(t, sink.rawTerminals, "completed stream must not emit extra terminal error frames")
		require.NotContains(t, sink.buf.String(), "response.failed")

		// Parse all emitted client SSE frames and confirm response.completed is the final event.
		var frames []sse.Frame
		sc := sse.NewReader(bytes.NewReader(sink.buf.Bytes()))
		for sc.Scan() {
			f := sc.Frame()
			frames = append(frames, sse.Frame{Event: f.Event, Data: bytes.Clone(f.Data)})
		}
		require.NoError(t, sc.Err())
		require.NotEmpty(t, frames)
		require.Equal(t, "response.completed", frames[len(frames)-1].Event)

		// Verify stop_reason arrival before a missing message_stop also finalizes response.completed cleanly.
		stopReasonOnlySSE := strings.TrimSuffix(completeSSE, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
		sink2 := &anthropicConvertedTestSink{}
		outCtx2 := upstream.NewDeferredOutputContext(sink2)
		ex2 := newTestResponsesToAnthropicExchange(t, []byte(`{"model":"gpt-5","stream":true,"input":"hi"}`))
		resp2 := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: &trailingErrReader{
				data:        strings.NewReader(stopReasonOnlySSE),
				trailingErr: io.ErrUnexpectedEOF,
			},
		}
		res2, err2 := ReadConvertedAnthropicStreaming(
			context.Background(),
			resp2,
			outCtx2,
			ex2,
			ConvertedAnthropicOutputOptions{
				DefaultErrorType: "server_error",
				WriteLocalTerminalError: func(int, string, string, string) {
					t.Fatal("WriteLocalTerminalError must not fire when stop_reason and block closures completed")
				},
			},
			"gpt-5",
			"claude-sonnet-4-6",
			"claude-sonnet-4-6",
			nil,
			time.Now(),
		)
		require.ErrorIs(t, err2, io.ErrUnexpectedEOF)
		require.NotNil(t, res2)
		require.Contains(t, sink2.buf.String(), "event: response.completed")
		require.NotContains(t, sink2.buf.String(), "response.failed")
	})
}

// TestReadConvertedAnthropicStreaming_ClientDisconnectThenUpstreamReadErrorPreservesResponseLosses
// reproduces a Chat-to-Anthropic stream carrying a signed thinking block where
// the downstream client disconnects mid-stream and the upstream subsequently
// fails during usage draining. The reader must preserve disconnect nil-error
// settlement while still closing the conversion stream so replay_token_dropped
// commits onto ProtocolConversion identically to a clean EOF exit.
func TestReadConvertedAnthropicStreaming_ClientDisconnectThenUpstreamReadErrorPreservesResponseLosses(t *testing.T) {
	signedSSE := "event: message_start\n" +
		"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_disc_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"stop_reason\":null,\"usage\":{\"input_tokens\":22,\"output_tokens\":0}}}\n\n" +
		"event: content_block_start\n" +
		"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n" +
		"event: content_block_delta\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"reason\"}}\n\n" +
		"event: content_block_delta\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig_anth_1\"}}\n\n" +
		"event: content_block_stop\n" +
		"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: content_block_start\n" +
		"data: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"hello\"}}\n\n" +
		"event: content_block_delta\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\" world\"}}\n\n"

	newChatToAnthropicExchange := func(t *testing.T) *convert.Exchange {
		t.Helper()
		ex, err := convert.NewExchange([]byte(`{"model":"gpt-5","stream":true,"messages":[{"role":"user","content":"hi"}]}`), convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolAnthropicMessages,
				Dialect: convert.Dialect{
					Name:         "anthropic",
					ReplayScheme: ir.SchemeAnthropic,
				},
			},
			ClientModel: "gpt-5",
			Runtime:     convert.DefaultRuntime(),
		})
		require.NoError(t, err)
		_, err = ex.UpstreamRequest()
		require.NoError(t, err)
		return ex
	}

	sink := &anthropicConvertedTestSink{failAfterWrites: 2}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newChatToAnthropicExchange(t)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: &trailingErrReader{
			data:        strings.NewReader(signedSSE),
			trailingErr: io.ErrUnexpectedEOF,
		},
	}

	res, err := ReadConvertedAnthropicStreaming(
		t.Context(),
		resp,
		outCtx,
		ex,
		ConvertedAnthropicOutputOptions{DefaultErrorType: "server_error"},
		"gpt-5",
		"claude-sonnet-4-6",
		"claude-sonnet-4-6",
		nil,
		time.Now(),
	)
	require.NoError(t, err, "post-disconnect upstream read error must retain nil error for billing settlement")
	require.NotNil(t, res)
	require.True(t, res.ClientDisconnect)
	require.NotNil(t, res.ProtocolConversion)
	require.Equal(t, 1, res.ProtocolConversion.LossCounts[string(ir.LossReplayTokenDropped)])
}
