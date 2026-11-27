package gemini

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// recordingConvertedSink captures Begin metadata, emitted OutputEvents, and
// optional simulated write disconnects or pre-committed SSE state for reader tests.
type recordingConvertedSink struct {
	initialCommitted bool                   // initial HTTP commitment reported by InitialOutput/OutputState
	failAfterWrites  int                    // 1-based non-empty Emit count at which Write returns an error
	begun            bool                   // whether Begin was invoked
	head             upstream.OutputHead    // metadata passed to Begin
	events           []upstream.OutputEvent // ordered copies of emitted OutputEvents
	body             bytes.Buffer           // concatenated non-empty payload bytes accepted before failure
	payloadWrites    int                    // count of non-empty payload Emit calls observed
	completedEvents  int                    // count of empty Terminal+Flush Complete signals observed
}

// InitialOutput returns the sink's configured initial commitment state before
// any response write runs. Pre-committed sinks report HTTP 200 with an empty
// header map, whereas unstarted sinks report status zero.
func (s *recordingConvertedSink) InitialOutput() upstream.OutputHead {
	status := 0
	if s.initialCommitted {
		status = http.StatusOK
	}
	return upstream.OutputHead{
		Committed: s.initialCommitted,
		Status:    status,
		Header:    make(http.Header),
	}
}

// OutputState reports current header and payload commitment without
// initializing deferred headers. ContentCommitted turns true only after at
// least one non-empty payload write has been accepted into s.body.
func (s *recordingConvertedSink) OutputState() upstream.OutputHead {
	committed := s.initialCommitted || s.begun
	return upstream.OutputHead{
		Committed:        committed,
		ContentCommitted: s.body.Len() > 0,
		Status:           s.head.Status,
	}
}

// Begin records the supplied OutputHead status and cloned headers before
// payload emission starts. Subsequent OutputState inspections report Committed
// as true with the captured header map.
func (s *recordingConvertedSink) Begin(head upstream.OutputHead) error {
	s.begun = true
	s.head = upstream.OutputHead{
		Committed: true,
		Status:    head.Status,
		Header:    upstream.CloneHeader(head.Header),
	}
	return nil
}

// Emit records a cloned OutputEvent, counts empty Terminal+Flush completion
// signals separately from payload writes, and appends accepted payload bytes
// onto s.body. When failAfterWrites is positive, reaching that 1-based payload
// write threshold returns io.ErrClosedPipe to simulate a client disconnect.
func (s *recordingConvertedSink) Emit(ev upstream.OutputEvent) error {
	cloned := ev
	cloned.Data = bytes.Clone(ev.Data)
	s.events = append(s.events, cloned)
	if len(ev.Data) == 0 && ev.Terminal && ev.Flush {
		s.completedEvents++
		return nil
	}
	if len(ev.Data) > 0 {
		s.payloadWrites++
		if s.failAfterWrites > 0 && s.payloadWrites >= s.failAfterWrites {
			return io.ErrClosedPipe
		}
		_, _ = s.body.Write(ev.Data)
	}
	return nil
}

// newTestGeminiExchange constructs a convert.Exchange from clientBody to
// ProtocolGeminiGenerateContent for clientProto in reader unit tests.
func newTestGeminiExchange(t *testing.T, clientProto protocol.ProtocolID, stream bool) *convert.Exchange {
	t.Helper()
	var body string
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		body = `{"model":"claude-alias","max_tokens":128,"stream":` + boolJSON(stream) + `,"messages":[{"role":"user","content":"hi"}]}`
	case protocol.ProtocolOpenAIResponses:
		body = `{"model":"gpt-alias","stream":` + boolJSON(stream) + `,"input":"hi"}`
	default:
		body = `{"model":"chat-alias","stream":` + boolJSON(stream) + `,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`
	}
	ex, err := convert.NewExchange([]byte(body), convert.Options{
		Route: convert.Route{
			Client:   clientProto,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: convert.Dialect{
				ReplayScheme:     "gemini",
				SchemaDialect:    convert.SchemaGemini,
				PlaceholderToken: "skip_thought_signature_validator",
			},
		},
		Runtime: convert.DefaultRuntime(),
	})
	require.NoError(t, err)
	return ex
}

// boolJSON formats a boolean literal for test request JSON strings.
func boolJSON(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

// parseSSEFramesForGrammar splits raw SSE output bytes into sse.Frame slices
// using sse.NewReader so testkit stream grammar validators can inspect them.
func parseSSEFramesForGrammar(t *testing.T, raw []byte) []sse.Frame {
	t.Helper()
	r := sse.NewReader(bytes.NewReader(raw))
	var out []sse.Frame
	for r.Scan() {
		out = append(out, r.Frame())
	}
	require.NoError(t, r.Err())
	return out
}

// TestReadConvertedStreaming_MergesSparseUsageTailAfterCompleteAcrossProtocols
// feeds a Gemini stream whose finishReason frame reports prompt and cache
// tokens, followed by a delayed sparse usageMetadata tail frame reporting only
// candidate and thought tokens. ReadConvertedStreaming must invoke Complete on
// finishReason, wait for the tail frame before calling Stream.Close(nil), pass
// stream grammar validation, and return identical merged usage to both the
// client stream and billing counters across Messages, Responses, and Chat.
func TestReadConvertedStreaming_MergesSparseUsageTailAfterCompleteAcrossProtocols(t *testing.T) {
	for _, clientProto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	} {
		t.Run(string(clientProto), func(t *testing.T) {
			pr, pw := io.Pipe()
			go func() {
				_, _ = io.WriteString(pw, "data: {\"responseId\":\"gem_tail_1\",\"modelVersion\":\"gemini-2.5-pro-001\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hello\",\"thought\":true,\"thoughtSignature\":\"sig_r1\"},{\"text\":\" world\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":100,\"cachedContentTokenCount\":40,\"candidatesTokenCount\":2}}\n\n")
				time.Sleep(25 * time.Millisecond)
				_, _ = io.WriteString(pw, "data: {\"modelVersion\":\"gemini-2.5-pro-final\",\"usageMetadata\":{\"candidatesTokenCount\":18,\"thoughtsTokenCount\":7}}\n\n")
				_ = pw.Close()
			}()

			sink := &recordingConvertedSink{}
			outCtx := upstream.NewDeferredOutputContext(sink)
			ex := newTestGeminiExchange(t, clientProto, true)

			var modelObs protocol.ResponseModelObserver
			adapter := &ResponseAdapter{
				Options: ResponseOptions{
					Context: t.Context(),
					ObserveRaw: func(data []byte) {
						modelObs.ObserveGemini(data)
					},
					ObserveImages: func([]byte) {},
				},
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"X-Goog-Request-Id": {"req-tail-1"}},
				Body:       pr,
			}

			billingUsage, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
				Exchange:  ex,
				RequestID: "req-tail-1",
			}, false)
			require.NoError(t, err)
			require.NotNil(t, billingUsage)

			// Verify billing counters merged prompt/cache from frame 1 with output/thoughts from tail frame 2.
			require.Equal(t, 60, billingUsage.InputTokens)
			require.Equal(t, 40, billingUsage.CacheReadInputTokens)
			require.Equal(t, 25, billingUsage.OutputTokens)
			require.Equal(t, "gemini-2.5-pro-final", modelObs.Model())
			require.Equal(t, 1, sink.completedEvents)
			require.Equal(t, "req-tail-1", sink.head.Header.Get("X-Request-Id"))

			frames := parseSSEFramesForGrammar(t, sink.body.Bytes())
			switch clientProto {
			case protocol.ProtocolAnthropicMessages:
				require.Empty(t, testkit.ValidateMessagesStream(frames, testkit.GrammarOptions{}))
				require.Contains(t, sink.body.String(), `"input_tokens":60`)
				require.Contains(t, sink.body.String(), `"cache_read_input_tokens":40`)
				require.Contains(t, sink.body.String(), `"output_tokens":25`)
				require.Contains(t, sink.body.String(), "trc1:gemini:")
			case protocol.ProtocolOpenAIResponses:
				require.Empty(t, testkit.ValidateResponsesStream(frames, testkit.GrammarOptions{}))
				require.Contains(t, sink.body.String(), `"input_tokens":100`)
				require.Contains(t, sink.body.String(), `"cached_tokens":40`)
				require.Contains(t, sink.body.String(), `"output_tokens":25`)
				require.Contains(t, sink.body.String(), `"reasoning_tokens":7`)
			case protocol.ProtocolOpenAIChatCompletions:
				require.Empty(t, testkit.ValidateChatStream(frames, testkit.GrammarOptions{}))
				require.Contains(t, sink.body.String(), `"prompt_tokens":100`)
				require.Contains(t, sink.body.String(), `"cached_tokens":40`)
				require.Contains(t, sink.body.String(), `"completion_tokens":25`)
				require.Contains(t, sink.body.String(), `"reasoning_tokens":7`)
			}
		})
	}
}

// TestReadConvertedStreaming_LeadingNonSemanticFramesHoldFailoverWindow feeds a
// non-semantic metadata/usage frame followed by an upstream 503 stream error.
// Staged leading frames must never flush to the sink, keeping ContentCommitted
// false, evaluating ClassifyStreamFailure once, and returning the failover error.
func TestReadConvertedStreaming_LeadingNonSemanticFramesHoldFailoverWindow(t *testing.T) {
	rawStream := "data: {\"responseId\":\"gem_pre_1\",\"modelVersion\":\"gemini-2.5-flash\",\"usageMetadata\":{\"promptTokenCount\":12}}\n\n" +
		"data: {\"error\":{\"code\":503,\"message\":\"model overloaded\",\"status\":\"UNAVAILABLE\"}}\n\n"

	sink := &recordingConvertedSink{}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestGeminiExchange(t, protocol.ProtocolOpenAIResponses, true)

	wantFailover := errors.New("switch-to-backup-provider")
	classifyCalls := 0
	adapter := &ResponseAdapter{
		Options: ResponseOptions{
			Context:       t.Context(),
			ObserveImages: func([]byte) {},
		},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(rawStream)),
	}

	_, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
		Exchange:  ex,
		RequestID: "req-failover-1",
		ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
			classifyCalls++
			require.False(t, started)
			require.Equal(t, http.StatusServiceUnavailable, streamErr.Status)
			return ConvertedStreamFailure{Failover: wantFailover}
		},
	}, false)

	require.ErrorIs(t, err, wantFailover)
	require.Equal(t, 1, classifyCalls)
	require.False(t, sink.begun)
	require.Zero(t, sink.body.Len())
	require.False(t, outCtx.Writer.ContentWritten())
}

// TestReadConvertedStreaming_PreCommittedKeepaliveEmitsUntimedTerminalError
// simulates DownstreamSSEKeepalive committing HTTP 200 SSE before any semantic
// content arrives, followed by a non-retryable 400 stream error. The reader
// must invoke WriteLocalTerminalError once without touching HTTP status.
func TestReadConvertedStreaming_PreCommittedKeepaliveEmitsUntimedTerminalError(t *testing.T) {
	rawStream := "data: {\"error\":{\"code\":400,\"message\":\"invalid schema\",\"status\":\"INVALID_ARGUMENT\"}}\n\n"

	sink := &recordingConvertedSink{initialCommitted: true}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestGeminiExchange(t, protocol.ProtocolOpenAIResponses, true)

	localTerminalCalls := 0
	writeErrorCalls := 0
	classifyCalls := 0
	adapter := &ResponseAdapter{
		Options: ResponseOptions{
			Context:       t.Context(),
			ObserveImages: func([]byte) {},
		},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(rawStream)),
	}

	_, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
		Exchange:     ex,
		CommittedSSE: func() bool { return true },
		ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
			classifyCalls++
			require.False(t, started)
			return ConvertedStreamFailure{
				Status:  http.StatusBadRequest,
				Type:    "invalid_request_error",
				Code:    "invalid_request",
				Message: streamErr.Message,
			}
		},
		WriteError: func(int, string, string, string) {
			writeErrorCalls++
		},
		WriteLocalTerminalError: func(status int, errType, code, message string) {
			localTerminalCalls++
			require.Equal(t, http.StatusBadRequest, status)
			require.Equal(t, "invalid_request_error", errType)
			require.Equal(t, "invalid_request", code)
			require.Equal(t, "invalid schema", message)
		},
	}, false)

	require.Error(t, err)
	require.Equal(t, 1, classifyCalls)
	require.Equal(t, 1, localTerminalCalls)
	require.Zero(t, writeErrorCalls)
}

// TestReadConvertedStreaming_MidstreamTruncationClosesOpenBlocksAndEvaluatesOnce
// feeds semantic text followed by abrupt EOF without finishReason across
// Messages and Responses clients. ReadConvertedStreaming must evaluate health
// policy once with started=true, close open blocks via Stream.Close(cause),
// deliver the terminal error frame via WriteRawTerminalSSE, and pass abnormal
// stream grammar validation while preserving partial billing usage.
func TestReadConvertedStreaming_MidstreamTruncationClosesOpenBlocksAndEvaluatesOnce(t *testing.T) {
	rawTruncated := "data: {\"responseId\":\"gem_trunc_1\",\"modelVersion\":\"gemini-2.5-flash\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial reply\"}]}}],\"usageMetadata\":{\"promptTokenCount\":25,\"candidatesTokenCount\":4}}\n\n"

	for _, clientProto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
	} {
		t.Run(string(clientProto), func(t *testing.T) {
			sink := &recordingConvertedSink{}
			outCtx := upstream.NewDeferredOutputContext(sink)
			ex := newTestGeminiExchange(t, clientProto, true)

			classifyCalls := 0
			rawTerminalCalls := 0
			adapter := &ResponseAdapter{
				Options: ResponseOptions{
					Context:       t.Context(),
					ObserveImages: func([]byte) {},
				},
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(rawTruncated)),
			}

			usage, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
				Exchange: ex,
				ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
					classifyCalls++
					require.True(t, started)
					require.Equal(t, "stream_incomplete", streamErr.Kind)
					return ConvertedStreamFailure{
						Status:  http.StatusBadGateway,
						Type:    "upstream_error",
						Code:    "upstream_error",
						Message: streamErr.Message,
					}
				},
				WriteRawTerminalSSE: func(_ int, _, _, _ string, data []byte) bool {
					rawTerminalCalls++
					_, _ = sink.body.Write(data)
					return true
				},
			}, false)

			require.Error(t, err)
			require.Equal(t, 1, classifyCalls)
			require.Equal(t, 1, rawTerminalCalls)
			require.Equal(t, 25, usage.InputTokens)
			require.Equal(t, 4, usage.OutputTokens)

			frames := parseSSEFramesForGrammar(t, sink.body.Bytes())
			switch clientProto {
			case protocol.ProtocolAnthropicMessages:
				require.Empty(t, testkit.ValidateMessagesStream(frames, testkit.GrammarOptions{AllowAbnormal: true}))
			case protocol.ProtocolOpenAIResponses:
				require.Empty(t, testkit.ValidateResponsesStream(frames, testkit.GrammarOptions{AllowAbnormal: true}))
				// Verify the terminal response.failed frame carries sequence_number and matches response.created.id.
				createdID := gjson.GetBytes(frames[0].Data, "response.id").String()
				lastFrame := frames[len(frames)-1]
				require.Equal(t, "response.failed", lastFrame.Event)
				require.Equal(t, createdID, gjson.GetBytes(lastFrame.Data, "response.id").String())
				require.True(t, gjson.GetBytes(lastFrame.Data, "sequence_number").Exists())
			}
		})
	}
}

// TestReadConvertedStreaming_ClientDisconnectDrainsUpstreamForBilling simulates
// a downstream write error on the first flushed batch while upstream continues
// emitting completion and trailing usage frames. The reader must stop writing
// downstream, drain upstream usage without invoking ClassifyStreamFailure, and
// return nil error with complete billing counters.
func TestReadConvertedStreaming_ClientDisconnectDrainsUpstreamForBilling(t *testing.T) {
	rawStream := "data: {\"responseId\":\"gem_disc_1\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"chunk-1\"}]}}],\"usageMetadata\":{\"promptTokenCount\":30,\"candidatesTokenCount\":2}}\n\n" +
		"data: {\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"chunk-2\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":30,\"candidatesTokenCount\":9}}\n\n"

	sink := &recordingConvertedSink{failAfterWrites: 2}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestGeminiExchange(t, protocol.ProtocolAnthropicMessages, true)

	classifyCalled := false
	adapter := &ResponseAdapter{
		Options: ResponseOptions{
			Context:       t.Context(),
			ObserveImages: func([]byte) {},
		},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(rawStream)),
	}

	usage, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
		Exchange: ex,
		ClassifyStreamFailure: func(context.Context, *upstream.GeminiStreamError, bool) ConvertedStreamFailure {
			classifyCalled = true
			return ConvertedStreamFailure{}
		},
	}, false)

	require.NoError(t, err)
	require.ErrorIs(t, outCtx.Err(), io.ErrClosedPipe)
	require.False(t, classifyCalled)
	require.Equal(t, 30, usage.InputTokens)
	require.Equal(t, 9, usage.OutputTokens)
}

// TestReadConvertedBuffered_HandlesUnaryJSONAndCollectedOAuthSSE exercises
// ReadConvertedBuffered across both unary JSON responses and project-bearing
// OAuth wrapped SSE streams with fragmented tool arguments and trailing usage.
// Both paths must emit valid client JSON and return merged billing token counts.
func TestReadConvertedBuffered_HandlesUnaryJSONAndCollectedOAuthSSE(t *testing.T) {
	t.Run("unary_json_responses_client", func(t *testing.T) {
		unary := `{"responseId":"gem_buf_1","modelVersion":"gemini-2.5-flash","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"buffered reply"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":15,"cachedContentTokenCount":5,"candidatesTokenCount":3}}`
		sink := &recordingConvertedSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestGeminiExchange(t, protocol.ProtocolOpenAIResponses, false)

		adapter := &ResponseAdapter{
			Options: ResponseOptions{
				Context:       t.Context(),
				ObserveImages: func([]byte) {},
			},
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(unary)),
		}

		usage, err := adapter.ReadConvertedBuffered(outCtx, resp, ConvertedOptions{
			Exchange:  ex,
			RequestID: "req-buf-1",
		}, false, false)
		require.NoError(t, err)
		require.Equal(t, 10, usage.InputTokens)
		require.Equal(t, 5, usage.CacheReadInputTokens)
		require.Equal(t, 3, usage.OutputTokens)
		require.Equal(t, "buffered reply", gjson.GetBytes(sink.body.Bytes(), "output.0.content.0.text").String())
	})

	t.Run("collected_oauth_sse_with_fragmented_tool_and_sparse_tail", func(t *testing.T) {
		sseStream := "data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"id\":\"gem-call-1\",\"name\":\"lookup\",\"args\":\"{\\\"city\\\":\"}}]}}],\"usageMetadata\":{\"promptTokenCount\":20}}}\n\n" +
			"data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"functionCall\":{\"id\":\"gem-call-1\",\"name\":\"lookup\",\"args\":\"\\\"Tokyo\\\"}\"},\"thoughtSignature\":\"sig-tokyo\"}]},\"finishReason\":\"STOP\"}]}}\n\n" +
			"data: {\"response\":{\"usageMetadata\":{\"candidatesTokenCount\":11}}}\n\n"

		sink := &recordingConvertedSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestGeminiExchange(t, protocol.ProtocolAnthropicMessages, false)

		adapter := &ResponseAdapter{
			Options: ResponseOptions{
				Context:       t.Context(),
				ObserveImages: func([]byte) {},
			},
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(sseStream)),
		}

		usage, err := adapter.ReadConvertedBuffered(outCtx, resp, ConvertedOptions{
			Exchange:  ex,
			RequestID: "req-buf-oauth",
		}, true, true)
		require.NoError(t, err)
		require.Equal(t, 20, usage.InputTokens)
		require.Equal(t, 11, usage.OutputTokens)

		var parsed map[string]any
		require.NoError(t, json.Unmarshal(sink.body.Bytes(), &parsed))
		require.Equal(t, "tool_use", gjson.GetBytes(sink.body.Bytes(), "stop_reason").String())
		require.JSONEq(t, `{"city":"Tokyo"}`, gjson.GetBytes(sink.body.Bytes(), "content.0.input").Raw)
		require.Equal(t, int64(20), gjson.GetBytes(sink.body.Bytes(), "usage.input_tokens").Int())
		require.Equal(t, int64(11), gjson.GetBytes(sink.body.Bytes(), "usage.output_tokens").Int())
	})

	t.Run("inline_http_200_error_classifies_once_for_failover", func(t *testing.T) {
		inlineErrJSON := `{"error":{"code":429,"message":"Resource exhausted"}}`
		sink := &recordingConvertedSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestGeminiExchange(t, protocol.ProtocolOpenAIResponses, false)

		wantFailover := errors.New("inline-429-failover")
		classifyCalls := 0
		adapter := &ResponseAdapter{
			Options: ResponseOptions{
				Context:       t.Context(),
				ObserveImages: func([]byte) {},
			},
		}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(inlineErrJSON)),
		}

		_, err := adapter.ReadConvertedBuffered(outCtx, resp, ConvertedOptions{
			Exchange: ex,
			ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
				classifyCalls++
				require.False(t, started)
				require.Equal(t, http.StatusTooManyRequests, streamErr.Status)
				require.Equal(t, "Resource exhausted", streamErr.Message)
				return ConvertedStreamFailure{Failover: wantFailover}
			},
		}, false, false)
		require.ErrorIs(t, err, wantFailover)
		require.Equal(t, 1, classifyCalls)
		require.Zero(t, sink.body.Len())
	})
}
