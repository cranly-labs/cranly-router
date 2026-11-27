package antigravity

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// convertedTestSink records OutputSink header commitments, emitted events, and
// optional simulated downstream write failures for response_converted.go tests.
type convertedTestSink struct {
	mu          sync.Mutex             // guards recorded fields across stream callbacks
	begun       bool                   // whether Begin has run on this sink
	head        upstream.OutputHead    // captured Begin metadata snapshot
	initialHead upstream.OutputHead    // pre-existing OutputState returned before Begin
	events      []upstream.OutputEvent // ordered emitted payload, flush, and terminal events
	body        bytes.Buffer           // concatenated payload bytes written to client
	// failAfterWrites rejects payload writes once writeCalls reaches a positive
	// threshold, modeling downstream client disconnection mid-stream.
	failAfterWrites int // 1-based payload write failure threshold; 0 disables
	writeCalls      int // count of non-empty payload Emit invocations
}

// testScopedCallMemory stores in-memory CallBinding entries keyed by client
// call ID for converted Antigravity response unit tests.
type testScopedCallMemory struct {
	mu    sync.Mutex                     // serializes concurrent test inspections
	items map[string]convert.CallBinding // stored bindings keyed by client call ID
}

// deadlineErrorReader serves prefix bytes first and then fails subsequent reads
// with err, invoking onFinalRead when configured so tests can model either an
// outbound HTTP client timeout with active ctx or an expired execution context.
type deadlineErrorReader struct {
	prefix      *strings.Reader // initial SSE frames yielded before read failure
	err         error           // terminal read error returned once prefix drains
	onFinalRead func()          // optional hook invoked immediately before returning err
}

// testDeadlineContext models an execution context whose deadline expires
// deterministically when expire runs, returning context.DeadlineExceeded from Err.
type testDeadlineContext struct {
	context.Context               // parent test context
	done            chan struct{} // closed once expire marks the deadline exceeded
	once            sync.Once     // guards closing done at most once
}

// Begin records the committed response status and cloned headers once.
func (s *convertedTestSink) Begin(head upstream.OutputHead) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.begun = true
	s.head = upstream.OutputHead{
		Committed:        true,
		ContentCommitted: head.ContentCommitted,
		Status:           head.Status,
		Header:           upstream.CloneHeader(head.Header),
	}
	return nil
}

// Emit appends a cloned copy of ev and simulates client write failure when
// failAfterWrites triggers on a non-empty payload write.
func (s *convertedTestSink) Emit(ev upstream.OutputEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cloned := ev
	cloned.Data = bytes.Clone(ev.Data)
	s.events = append(s.events, cloned)
	if len(ev.Data) > 0 {
		s.writeCalls++
		if s.failAfterWrites > 0 && s.writeCalls >= s.failAfterWrites {
			return io.ErrClosedPipe
		}
		if ev.CommitForRetry {
			s.head.ContentCommitted = true
		}
		_, _ = s.body.Write(ev.Data)
	}
	return nil
}

// OutputState returns current HTTP and content commitment without initializing
// deferred writer headers, mirroring gateway ResponseSink behavior.
func (s *convertedTestSink) OutputState() upstream.OutputHead {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.begun {
		return s.head
	}
	return s.initialHead
}

// InitialOutput returns pre-seeded commitment state when OutputContext initializes.
func (s *convertedTestSink) InitialOutput() upstream.OutputHead {
	return s.OutputState()
}

// Remember stores binding under clientCallID inside the test CallMemory map.
func (m *testScopedCallMemory) Remember(clientCallID string, binding convert.CallBinding) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.items == nil {
		m.items = make(map[string]convert.CallBinding)
	}
	var tokCopy *ir.ReplayToken
	if binding.Token != nil {
		cp := *binding.Token
		tokCopy = &cp
	}
	m.items[clientCallID] = convert.CallBinding{
		UpstreamID: binding.UpstreamID,
		Token:      tokCopy,
	}
}

// Recall retrieves a detached copy of any stored CallBinding for clientCallID.
func (m *testScopedCallMemory) Recall(clientCallID string) (convert.CallBinding, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.items[clientCallID]
	if !ok {
		return convert.CallBinding{}, false
	}
	if b.Token != nil {
		cp := *b.Token
		b.Token = &cp
	}
	return b, true
}

// Read yields bytes from r.prefix until EOF and then invokes onFinalRead once
// before returning r.err to simulate mid-stream or pre-output transport timeouts.
func (r *deadlineErrorReader) Read(p []byte) (int, error) {
	if r.prefix != nil && r.prefix.Len() > 0 {
		return r.prefix.Read(p)
	}
	if r.onFinalRead != nil {
		fn := r.onFinalRead
		r.onFinalRead = nil
		fn()
	}
	return 0, r.err
}

// Done returns the channel closed when expire marks the deadline exceeded.
func (d *testDeadlineContext) Done() <-chan struct{} {
	return d.done
}

// Err returns context.DeadlineExceeded once d.done closes, or the parent error while active.
func (d *testDeadlineContext) Err() error {
	select {
	case <-d.done:
		return context.DeadlineExceeded
	default:
		return d.Context.Err()
	}
}

// expire closes d.done once so subsequent Done and Err calls report DeadlineExceeded.
func (d *testDeadlineContext) expire() {
	d.once.Do(func() {
		close(d.done)
	})
}

// newTestAntigravityExchange constructs a convert.Exchange from clientBody onto
// Antigravity's GenerateContent dialect for either claude-* or gemini-* models.
func newTestAntigravityExchange(
	t *testing.T,
	source protocol.ProtocolID,
	clientBody []byte,
	model string,
	isClaude bool,
	calls convert.CallMemory,
) *convert.Exchange {
	t.Helper()
	scheme := ir.SchemeGemini
	placeholder := gemini.DummyThoughtSignature
	if isClaude {
		scheme = ir.SchemeAntigravityClaude
		placeholder = ""
	}
	ex, err := convert.NewExchange(clientBody, convert.Options{
		Route: convert.Route{
			Client:   source,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: convert.Dialect{
				Name:             "antigravity",
				ReplayScheme:     scheme,
				SchemaDialect:    convert.SchemaAntigravity,
				PlaceholderToken: placeholder,
				Reasoning: convert.ReasoningCapabilities{
					ForceBudget: isClaude,
				},
			},
		},
		ClientModel: model,
		Runtime:     convert.DefaultRuntime(),
		Calls:       calls,
	})
	require.NoError(t, err)
	return ex
}

// buildTestClientRequest constructs a streaming or buffered request body with a
// declared function or custom tool for the requested client protocol.
func buildTestClientRequest(source protocol.ProtocolID, model string, stream bool) []byte {
	streamLit := "false"
	if stream {
		streamLit = "true"
	}
	switch source {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"` + model + `","max_tokens":256,"stream":` + streamLit + `,"tools":[{"name":"lookup_order","description":"Lookup order","input_schema":{"type":"object","properties":{"order_id":{"type":"integer"}}}}],"messages":[{"role":"user","content":"Check order"}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"` + model + `","stream":` + streamLit + `,"tools":[{"type":"custom","name":"apply_patch","description":"Patch file"},{"type":"function","name":"lookup_order","description":"Lookup order","parameters":{"type":"object","properties":{"order_id":{"type":"integer"}}}}],"input":"Check order"}`)
	default:
		return []byte(`{"model":"` + model + `","stream":` + streamLit + `,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"lookup_order","description":"Lookup order","parameters":{"type":"object","properties":{"order_id":{"type":"integer"}}}}}],"messages":[{"role":"user","content":"Check order"}]}`)
	}
}

// parseTestSSEFrames reads all SSE frames from raw using protocol/sse.NewReader.
func parseTestSSEFrames(t *testing.T, raw []byte) []sse.Frame {
	t.Helper()
	r := sse.NewReader(bytes.NewReader(raw))
	var frames []sse.Frame
	for r.Scan() {
		frames = append(frames, r.Frame())
	}
	require.NoError(t, r.Err())
	return frames
}

// TestReadConvertedStreamingAndBuffered_AllProtocolsAndModelFamilies feeds
// v1internal-wrapped Antigravity SSE frames (carrying outer responseId, outer
// modelVersion, reasoning signatures, 64-bit integer tool arguments, late tool
// signatures, and sparse tail usageMetadata) into ReadConvertedStreaming and
// ReadConvertedBuffered across Messages, Responses, and Chat for claude-* and
// gemini-* dialects. Every combination must satisfy strict stream grammar, seal
// family-scoped trc1 carriers, populate CallMemory, stop heartbeats at
// finishReason before emitting final usage, and preserve 64-bit integer args.
func TestReadConvertedStreamingAndBuffered_AllProtocolsAndModelFamilies(t *testing.T) {
	models := []struct {
		name       string // target model identifier
		isClaude   bool   // whether model uses antigravity_claude carrier rules
		wantScheme string // expected trc1 carrier scheme
	}{
		{name: "claude-opus-4-6-thinking", isClaude: true, wantScheme: ir.SchemeAntigravityClaude},
		{name: "gemini-3.1-pro-high", isClaude: false, wantScheme: ir.SchemeGemini},
	}
	protocols := []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	}

	// Frame 1: reasoning + tool call without tool signature yet + prompt/cache usage;
	// Frame 2: late thoughtSignature revision on lookup_order + finishReason STOP;
	// Frame 3: sparse post-finishReason tail frame carrying candidatesTokenCount + thoughtsTokenCount.
	v1SSE := strings.Join([]string{
		`data: {"responseId":"outer_resp_99","modelVersion":"upstream-runtime-v2","response":{"responseId":"inner_overridden","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"analyzing order","thought":true,"thoughtSignature":"sig_reason_42"},{"functionCall":{"id":"up_call_77","name":"lookup_order","args":{"order_id":9007199254740993}}},{"text":"Order checked."}]}}],"usageMetadata":{"promptTokenCount":120,"cachedContentTokenCount":30}}}`,
		"",
		`data: {"responseId":"outer_resp_99","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"up_call_77","name":"lookup_order","args":{"order_id":9007199254740993}},"thoughtSignature":"sig_tool_late_77"}]},"finishReason":"STOP"}]}}`,
		"",
		`data: {"response":{"usageMetadata":{"candidatesTokenCount":18,"thoughtsTokenCount":7,"totalTokenCount":145}}}`,
		"",
	}, "\n")

	for _, m := range models {
		for _, proto := range protocols {
			for _, isStream := range []bool{true, false} {
				modeLabel := "buffered"
				if isStream {
					modeLabel = "stream"
				}
				t.Run(string(proto)+"/"+m.name+"/"+modeLabel, func(t *testing.T) {
					mem := &testScopedCallMemory{}
					reqBytes := buildTestClientRequest(proto, m.name, isStream)
					ex := newTestAntigravityExchange(t, proto, reqBytes, m.name, m.isClaude, mem)

					sink := &convertedTestSink{}
					outCtx := upstream.NewDeferredOutputContext(sink)
					var observedModels []string
					adapter := &ResponseAdapter{
						Options: ResponseOptions{
							Context: t.Context(),
							ObserveRaw: func(raw []byte) {
								var obs protocol.ResponseModelObserver
								obs.ObserveGemini(raw)
								if obs.Model() != "" {
									observedModels = append(observedModels, obs.Model())
								}
							},
						},
					}
					resp := &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"X-Request-Id": []string{"ag_req_1"}},
						Body:       io.NopCloser(strings.NewReader(v1SSE)),
					}
					convOpts := ConvertedOptions{
						Exchange:  ex,
						RequestID: "ag_req_1",
					}

					var res *StreamResult
					var err error
					if isStream {
						res, err = adapter.ReadConvertedStreaming(outCtx, resp, convOpts)
					} else {
						res, err = adapter.ReadConvertedBuffered(outCtx, resp, convOpts)
					}
					require.NoError(t, err)
					require.NotNil(t, res)
					require.NotNil(t, res.Usage)
					require.False(t, res.ClientDisconnect)

					// Native billing usage: Input = 120 - 30 = 90, CacheRead = 30, Output = 18 + 7 = 25.
					require.Equal(t, 90, res.Usage.InputTokens)
					require.Equal(t, 30, res.Usage.CacheReadInputTokens)
					require.Equal(t, 25, res.Usage.OutputTokens)
					require.Contains(t, observedModels, "upstream-runtime-v2")
					require.Equal(t, "ag_req_1", sink.head.Header.Get("X-Request-Id"))

					// CallMemory must hold the late tool signature under the model family's ReplayScheme.
					binding, recalled := mem.Recall("up_call_77")
					require.True(t, recalled)
					require.Equal(t, "up_call_77", binding.UpstreamID)
					require.NotNil(t, binding.Token)
					require.Equal(t, m.wantScheme, binding.Token.Scheme)
					require.Equal(t, "sig_tool_late_77", binding.Token.Data)

					outStr := sink.body.String()
					require.Contains(t, outStr, "9007199254740993", "64-bit integer precision must survive unwrapping and conversion")

					if isStream {
						frames := parseTestSSEFrames(t, sink.body.Bytes())
						require.Empty(t, testkit.ValidateStreamGrammar(proto, frames, testkit.GrammarOptions{}))

						// Verify c.Complete() emitted an empty-data Terminal notification before final tail frames.
						sawKeepaliveStop := false
						for _, ev := range sink.events {
							if len(ev.Data) == 0 && ev.Terminal && ev.Flush {
								sawKeepaliveStop = true
							}
						}
						require.True(t, sawKeepaliveStop, "expected c.Complete() terminal notification upon finishReason")
					}

					switch proto {
					case protocol.ProtocolAnthropicMessages, protocol.ProtocolOpenAIResponses:
						wantPrefix := "trc1:" + m.wantScheme + ":"
						require.Contains(t, outStr, wantPrefix)
						idx := strings.Index(outStr, wantPrefix)
						require.GreaterOrEqual(t, idx, 0)
						endQuote := strings.IndexByte(outStr[idx:], '"')
						require.Greater(t, endQuote, 0)
						tokenStr := outStr[idx : idx+endQuote]
						if isStream {
							tokenStr = strings.ReplaceAll(tokenStr, `\"`, `"`)
						}
						parsedCarrier, ok := ir.ParseCarrier(tokenStr)
						require.True(t, ok, "expected valid trc1 carrier in %s", tokenStr)
						require.Equal(t, m.wantScheme, parsedCarrier.Scheme)
						require.Equal(t, "sig_reason_42", parsedCarrier.Data)
						if m.isClaude {
							require.Equal(t, "analyzing order", parsedCarrier.Text)
						} else {
							require.Empty(t, parsedCarrier.Text)
						}
					case protocol.ProtocolOpenAIChatCompletions:
						require.NotContains(t, outStr, "trc1:")
						require.Contains(t, outStr, "analyzing order")
					}
				})
			}
		}
	}
}

// TestReadConvertedStreaming_LeadingFrameStagingCommittedSSEAndRawTerminal
// reproduces pre-semantic leading frame staging, pre-committed SSE colliding
// usage errors, and mid-stream truncation on Responses streams. Retryable
// pre-semantic errors must leave the sink uncommitted, pre-committed SSE errors
// must emit untimed terminal frames, and started streams must close via WriteRawTerminalSSE.
func TestReadConvertedStreaming_LeadingFrameStagingCommittedSSEAndRawTerminal(t *testing.T) {
	t.Run("leading_non_semantic_frame_followed_by_retryable_error_triggers_failover", func(t *testing.T) {
		// Frame 1 carries responseId and usageMetadata with empty parts (producing
		// non-semantic message_start / response.created), followed by a 503 error.
		sseBody := strings.Join([]string{
			`data: {"response":{"responseId":"ag_stage_1","candidates":[{"index":0,"content":{"role":"model","parts":[]}}],"usageMetadata":{"promptTokenCount":12}}}`,
			"",
			`data: {"error":{"code":503,"message":"Capacity exhausted","status":"UNAVAILABLE"}}`,
			"",
		}, "\n")

		for _, proto := range []protocol.ProtocolID{
			protocol.ProtocolAnthropicMessages,
			protocol.ProtocolOpenAIResponses,
			protocol.ProtocolOpenAIChatCompletions,
		} {
			ex := newTestAntigravityExchange(t, proto, buildTestClientRequest(proto, "claude-sonnet-4-5", true), "claude-sonnet-4-5", true, nil)
			sink := &convertedTestSink{}
			outCtx := upstream.NewDeferredOutputContext(sink)
			adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(sseBody)),
			}

			sentinelFailover := errors.New("switch-provider-now")
			classifyCalls := 0
			_, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
				Exchange: ex,
				ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
					classifyCalls++
					require.False(t, started, "leading non-semantic frame must remain staged")
					require.Equal(t, http.StatusServiceUnavailable, streamErr.Status)
					return ConvertedStreamFailure{Failover: sentinelFailover}
				},
			})
			require.ErrorIs(t, err, sentinelFailover)
			require.Equal(t, 1, classifyCalls)
			require.False(t, sink.begun, "sink must remain completely uncommitted for outer failover")
			require.Empty(t, sink.body.Bytes())
		}
	})

	t.Run("pre_committed_sse_and_colliding_usage_keys_route_to_untimed_terminal_error", func(t *testing.T) {
		collidingSSE := "data: {\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"bad\"}]}}],\"UsageMetadata\":{\"promptTokenCount\":999},\"usageMetadata\":{\"promptTokenCount\":10}}}\n\n"

		ex := newTestAntigravityExchange(
			t,
			protocol.ProtocolOpenAIResponses,
			buildTestClientRequest(protocol.ProtocolOpenAIResponses, "gemini-2.5-pro", true),
			"gemini-2.5-pro",
			false,
			nil,
		)
		sink := &convertedTestSink{
			initialHead: upstream.OutputHead{Committed: true, Status: http.StatusOK},
		}
		outCtx := upstream.NewDeferredOutputContext(sink)
		adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(collidingSSE)),
		}

		localTerminalCalled := false
		res, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
			Exchange:         ex,
			DefaultErrorType: "upstream_error",
			CommittedSSE:     func() bool { return true },
			ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
				require.False(t, started)
				require.ErrorIs(t, streamErr, ErrAmbiguousUsagePayload)
				return ConvertedStreamFailure{
					Status:  http.StatusBadGateway,
					Type:    "upstream_error",
					Code:    "upstream_error",
					Message: "Failed to parse upstream response",
				}
			},
			WriteLocalTerminalError: func(status int, errType, code, msg string) {
				localTerminalCalled = true
				require.Equal(t, http.StatusBadGateway, status)
				require.Equal(t, "upstream_error", errType)
				require.Equal(t, "upstream_error", code)
				require.Equal(t, "Failed to parse upstream response", msg)
			},
		})
		require.Error(t, err)
		require.NotNil(t, res)
		require.Zero(t, res.Usage.InputTokens, "colliding usageMetadata keys must never populate billing usage")
		require.True(t, localTerminalCalled)
		require.Empty(t, sink.body.Bytes())
	})

	t.Run("mid_stream_truncation_delivers_continuous_sequence_response_failed_via_raw_terminal", func(t *testing.T) {
		truncatedSSE := `data: {"responseId":"resp_cont_7","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"partial chunk"}]}}],"usageMetadata":{"promptTokenCount":15,"candidatesTokenCount":3}}}` + "\n\n"

		ex := newTestAntigravityExchange(
			t,
			protocol.ProtocolOpenAIResponses,
			buildTestClientRequest(protocol.ProtocolOpenAIResponses, "claude-sonnet-4-5", true),
			"claude-sonnet-4-5",
			true,
			nil,
		)
		sink := &convertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(truncatedSSE)),
		}

		var rawTerminalBytes []byte
		res, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
			Exchange:         ex,
			DefaultErrorType: "upstream_error",
			WriteRawTerminalSSE: func(status int, errType, code, msg string, data []byte) bool {
				require.Equal(t, http.StatusBadGateway, status)
				require.Equal(t, "upstream_error", errType)
				require.Equal(t, "stream_incomplete", code)
				require.Contains(t, msg, "ended before completion")
				rawTerminalBytes = bytes.Clone(data)
				_, _ = sink.body.Write(data)
				return true
			},
			ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
				require.True(t, started)
				return ConvertedStreamFailure{
					Status:  http.StatusBadGateway,
					Type:    "upstream_error",
					Code:    streamErr.Kind,
					Message: streamErr.Message,
				}
			},
		})
		require.Error(t, err)
		require.NotNil(t, res)
		require.Equal(t, 15, res.Usage.InputTokens)
		require.Equal(t, 3, res.Usage.OutputTokens)
		require.NotEmpty(t, rawTerminalBytes)
		require.Contains(t, string(rawTerminalBytes), "event: response.failed")
		require.Contains(t, string(rawTerminalBytes), `"id":"resp_cont_7"`)

		frames := parseTestSSEFrames(t, sink.body.Bytes())
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{
			AllowAbnormal: true,
		}))
	})
}

// TestReadConvertedStreaming_ClientDisconnectContinuesDrainingBillingUsage
// rejects downstream client writes after the opening text frame while feeding a
// trailing usageMetadata frame upstream. ReadConvertedStreaming must suppress
// further client writes, drain trailing counters, and report ClientDisconnect.
func TestReadConvertedStreaming_ClientDisconnectContinuesDrainingBillingUsage(t *testing.T) {
	sseBody := strings.Join([]string{
		`data: {"response":{"responseId":"ag_disc_1","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"first chunk"}]}}],"usageMetadata":{"promptTokenCount":25,"cachedContentTokenCount":5,"candidatesTokenCount":2}}}`,
		"",
		`data: {"response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"second chunk"}]},"finishReason":"STOP"}]}}`,
		"",
		`data: {"response":{"usageMetadata":{"candidatesTokenCount":19,"thoughtsTokenCount":4}}}`,
		"",
	}, "\n")

	ex := newTestAntigravityExchange(
		t,
		protocol.ProtocolAnthropicMessages,
		buildTestClientRequest(protocol.ProtocolAnthropicMessages, "claude-sonnet-4-5", true),
		"claude-sonnet-4-5",
		true,
		nil,
	)
	sink := &convertedTestSink{failAfterWrites: 2}
	outCtx := upstream.NewDeferredOutputContext(sink)
	adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(sseBody)),
	}

	res, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
		Exchange:  ex,
		RequestID: "ag_disc_req",
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.True(t, res.ClientDisconnect)
	require.Equal(t, 20, res.Usage.InputTokens)
	require.Equal(t, 5, res.Usage.CacheReadInputTokens)
	require.Equal(t, 23, res.Usage.OutputTokens, "trailing output + thoughts usage must be drained after client disconnect")
	require.NotContains(t, sink.body.String(), "second chunk")
}

// TestUnwrapV1InternalConvertedPayload_LosslessAndCollisionGuard feeds wrapped
// v1internal objects and ambiguous usage envelopes into unwrapV1InternalConvertedPayload.
// Outer responseId and modelVersion metadata must propagate without losing
// 64-bit integer precision, while duplicate or case-colliding keys fail with ErrAmbiguousUsagePayload.
func TestUnwrapV1InternalConvertedPayload_LosslessAndCollisionGuard(t *testing.T) {
	t.Run("outer_responseId_wins_and_outer_modelVersion_backfills_losslessly", func(t *testing.T) {
		raw := []byte(`{"responseId":"outer_id","modelVersion":"claude-opus-4-6","response":{"responseId":"inner_id","candidates":[{"content":{"parts":[{"functionCall":{"name":"fn","args":{"big":9007199254740993}}}]}}]}}`)
		got, err := unwrapV1InternalConvertedPayload(raw)
		require.NoError(t, err)
		require.Contains(t, string(got), `"responseId":"outer_id"`)
		require.Contains(t, string(got), `"modelVersion":"claude-opus-4-6"`)
		require.Contains(t, string(got), `9007199254740993`)

		var check map[string]jsontext.Value
		dec := jsontext.NewDecoder(bytes.NewReader(got))
		require.NoError(t, json.UnmarshalDecode(dec, &check))
	})

	t.Run("duplicate_usage_counter_key_rejected_by_wirejson", func(t *testing.T) {
		dupCounter := []byte(`{"response":{"candidates":[],"usageMetadata":{"promptTokenCount":1,"promptTokenCount":500}}}`)
		_, err := unwrapV1InternalConvertedPayload(dupCounter)
		require.ErrorIs(t, err, ErrAmbiguousUsagePayload)
	})

	t.Run("case_colliding_response_envelope_rejected_by_wirejson", func(t *testing.T) {
		dupResp := []byte(`{"Response":{"candidates":[]},"response":{"candidates":[]}}`)
		_, err := unwrapV1InternalConvertedPayload(dupResp)
		require.ErrorIs(t, err, ErrAmbiguousUsagePayload)
	})
}

// TestReadConverted_UpstreamDeadlineExceededAndContextTimeoutInvariants feeds
// GeminiStreamError causes wrapping context.DeadlineExceeded with an active
// request context alongside execution-context timeouts across streaming and
// buffered modes. Upstream timeouts must preserve pre-semantic failover and
// client error output, while CommittedSSE and started streams always receive
// untimed terminal SSE error frames.
func TestReadConverted_UpstreamDeadlineExceededAndContextTimeoutInvariants(t *testing.T) {
	outboundClientTimeout := fmt.Errorf("Client.Timeout exceeded while awaiting body: %w", context.DeadlineExceeded)
	sentinelFailover := errors.New("outbound-timeout-failover")

	t.Run("streaming_upstream_deadline_exceeded_after_preamble_triggers_failover", func(t *testing.T) {
		preamble := "data: {\"response\":{\"responseId\":\"ag_to_pre\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[]}}]}}\n\n"
		ex := newTestAntigravityExchange(
			t,
			protocol.ProtocolOpenAIResponses,
			buildTestClientRequest(protocol.ProtocolOpenAIResponses, "claude-sonnet-4-5", true),
			"claude-sonnet-4-5",
			true,
			nil,
		)
		sink := &convertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body: io.NopCloser(&deadlineErrorReader{
				prefix: strings.NewReader(preamble),
				err:    outboundClientTimeout,
			}),
		}

		classifyCalls := 0
		_, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
			Exchange:         ex,
			DefaultErrorType: "upstream_error",
			ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
				classifyCalls++
				require.False(t, started)
				require.Equal(t, "stream_read_error", streamErr.Kind)
				require.ErrorIs(t, streamErr, context.DeadlineExceeded)
				return ConvertedStreamFailure{Failover: sentinelFailover}
			},
		})
		require.ErrorIs(t, err, sentinelFailover)
		require.Equal(t, 1, classifyCalls)
		require.False(t, sink.begun, "staged non-semantic preamble must remain uncommitted on upstream timeout failover")
	})

	t.Run("streaming_upstream_deadline_exceeded_with_committed_sse_writes_terminal_error", func(t *testing.T) {
		ex := newTestAntigravityExchange(
			t,
			protocol.ProtocolAnthropicMessages,
			buildTestClientRequest(protocol.ProtocolAnthropicMessages, "claude-sonnet-4-5", true),
			"claude-sonnet-4-5",
			true,
			nil,
		)
		sink := &convertedTestSink{
			initialHead: upstream.OutputHead{Committed: true, Status: http.StatusOK},
		}
		outCtx := upstream.NewDeferredOutputContext(sink)
		adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(&deadlineErrorReader{err: outboundClientTimeout}),
		}

		localCalls := 0
		_, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
			Exchange:         ex,
			DefaultErrorType: "api_error",
			CommittedSSE:     func() bool { return true },
			WriteLocalTerminalError: func(status int, errType, code, message string) {
				localCalls++
				require.Equal(t, http.StatusBadGateway, status)
				require.Equal(t, "api_error", errType)
				require.NotEmpty(t, message)
			},
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, localCalls)
	})

	t.Run("streaming_started_upstream_deadline_exceeded_and_ctx_deadline_close_blocks", func(t *testing.T) {
		for _, expireExecCtx := range []bool{false, true} {
			name := "active_ctx_outbound_timeout"
			if expireExecCtx {
				name = "expired_execution_ctx_after_output_started"
			}
			t.Run(name, func(t *testing.T) {
				opening := "data: {\"response\":{\"responseId\":\"ag_to_open\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial\"}]}}]}}\n\n"
				ex := newTestAntigravityExchange(
					t,
					protocol.ProtocolOpenAIResponses,
					buildTestClientRequest(protocol.ProtocolOpenAIResponses, "claude-sonnet-4-5", true),
					"claude-sonnet-4-5",
					true,
					nil,
				)
				sink := &convertedTestSink{}
				outCtx := upstream.NewDeferredOutputContext(sink)
				execCtx := &testDeadlineContext{
					Context: t.Context(),
					done:    make(chan struct{}),
				}

				reader := &deadlineErrorReader{
					prefix: strings.NewReader(opening),
					err:    outboundClientTimeout,
				}
				if expireExecCtx {
					reader.onFinalRead = execCtx.expire
				}

				adapter := &ResponseAdapter{Options: ResponseOptions{Context: execCtx}}
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{},
					Body:       io.NopCloser(reader),
				}

				rawTerminalCalls := 0
				_, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
					Exchange:         ex,
					DefaultErrorType: "upstream_error",
					WriteRawTerminalSSE: func(status int, _, _, _ string, data []byte) bool {
						rawTerminalCalls++
						require.Equal(t, http.StatusBadGateway, status)
						_, _ = sink.body.Write(data)
						return true
					},
				})
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, 1, rawTerminalCalls)
				require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, parseTestSSEFrames(t, sink.body.Bytes()), testkit.GrammarOptions{
					AllowAbnormal: true,
				}))
			})
		}
	})

	t.Run("streaming_ctx_deadline_with_committed_sse_writes_local_terminal_error", func(t *testing.T) {
		ex := newTestAntigravityExchange(
			t,
			protocol.ProtocolOpenAIChatCompletions,
			buildTestClientRequest(protocol.ProtocolOpenAIChatCompletions, "gemini-2.5-pro", true),
			"gemini-2.5-pro",
			false,
			nil,
		)
		sink := &convertedTestSink{
			initialHead: upstream.OutputHead{Committed: true, Status: http.StatusOK},
		}
		outCtx := upstream.NewDeferredOutputContext(sink)
		execCtx := &testDeadlineContext{
			Context: t.Context(),
			done:    make(chan struct{}),
		}
		execCtx.expire()

		adapter := &ResponseAdapter{Options: ResponseOptions{Context: execCtx}}
		resp := &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{},
			Body:       io.NopCloser(&deadlineErrorReader{err: context.DeadlineExceeded}),
		}

		localCalls := 0
		_, err := adapter.ReadConvertedStreaming(outCtx, resp, ConvertedOptions{
			Exchange:         ex,
			DefaultErrorType: "upstream_error",
			CommittedSSE:     func() bool { return true },
			WriteLocalTerminalError: func(status int, errType, code, message string) {
				localCalls++
				require.Equal(t, http.StatusBadGateway, status)
				require.Equal(t, "upstream_error", errType)
				require.Equal(t, "upstream_stream_error", code)
				require.Equal(t, "Stream interrupted before completion", message)
			},
		})
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 1, localCalls)
	})

	t.Run("buffered_upstream_deadline_exceeded_supports_failover_json_and_committed_sse", func(t *testing.T) {
		newBufferedEx := func() *convert.Exchange {
			return newTestAntigravityExchange(
				t,
				protocol.ProtocolAnthropicMessages,
				buildTestClientRequest(protocol.ProtocolAnthropicMessages, "claude-sonnet-4-5", false),
				"claude-sonnet-4-5",
				true,
				nil,
			)
		}

		// Subcase A: active ctx + outbound timeout -> failover returned without committing sink.
		sinkFailover := &convertedTestSink{}
		adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
		_, foErr := adapter.ReadConvertedBuffered(
			upstream.NewDeferredOutputContext(sinkFailover),
			&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(&deadlineErrorReader{err: outboundClientTimeout})},
			ConvertedOptions{
				Exchange: newBufferedEx(),
				ClassifyStreamFailure: func(_ context.Context, streamErr *upstream.GeminiStreamError, started bool) ConvertedStreamFailure {
					require.False(t, started)
					require.ErrorIs(t, streamErr, context.DeadlineExceeded)
					return ConvertedStreamFailure{Failover: sentinelFailover}
				},
			},
		)
		require.ErrorIs(t, foErr, sentinelFailover)
		require.False(t, sinkFailover.begun)

		// Subcase B: active ctx + outbound timeout + no failover -> writes 502 JSON error.
		sinkJSON := &convertedTestSink{}
		_, jsonErr := adapter.ReadConvertedBuffered(
			upstream.NewDeferredOutputContext(sinkJSON),
			&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(&deadlineErrorReader{err: outboundClientTimeout})},
			ConvertedOptions{
				Exchange:         newBufferedEx(),
				DefaultErrorType: "api_error",
			},
		)
		require.ErrorIs(t, jsonErr, context.DeadlineExceeded)
		require.True(t, sinkJSON.begun)
		require.Equal(t, http.StatusBadGateway, sinkJSON.head.Status)
		require.Contains(t, sinkJSON.body.String(), `"type":"api_error"`)

		// Subcase C: expired execution ctx with pre-committed SSE -> WriteLocalTerminalError runs.
		execCtx := &testDeadlineContext{
			Context: t.Context(),
			done:    make(chan struct{}),
		}
		execCtx.expire()
		expiredAdapter := &ResponseAdapter{Options: ResponseOptions{Context: execCtx}}
		localCalls := 0
		_, termErr := expiredAdapter.ReadConvertedBuffered(
			upstream.NewDeferredOutputContext(&convertedTestSink{initialHead: upstream.OutputHead{Committed: true, Status: http.StatusOK}}),
			&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(&deadlineErrorReader{err: context.DeadlineExceeded})},
			ConvertedOptions{
				Exchange:         newBufferedEx(),
				DefaultErrorType: "api_error",
				CommittedSSE:     func() bool { return true },
				WriteLocalTerminalError: func(status int, errType, code, _ string) {
					localCalls++
					require.Equal(t, http.StatusBadGateway, status)
					require.Equal(t, "api_error", errType)
					require.Equal(t, "upstream_stream_error", code)
				},
			},
		)
		require.ErrorIs(t, termErr, context.DeadlineExceeded)
		require.Equal(t, 1, localCalls)
	})
}

// TestReadConverted_InlineImageMarkdownAndNonThoughtTextSignature feeds a
// v1internal SSE stream containing a non-thought signed text part and a
// base64-encoded inlineData image through ReadConvertedStreaming and
// ReadConvertedBuffered across all client protocols and model families. Both
// modes must render the image as a visible Markdown data URL and preserve
// trc1:antigravity_claude or trc1:gemini carriers on Messages and Responses.
func TestReadConverted_InlineImageMarkdownAndNonThoughtTextSignature(t *testing.T) {
	const pngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
	wantMarkdown := "![image](data:image/png;base64," + pngB64 + ")"
	v1InternalSSE := "data: {\"responseId\":\"ag_img_1\",\"modelVersion\":\"gemini-3-pro-image\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Generated badge:\",\"thoughtSignature\":\"sig_ag_plain_88\"}]}}]}}\n\n" +
		"data: {\"responseId\":\"ag_img_1\",\"modelVersion\":\"gemini-3-pro-image\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"inlineData\":{\"mimeType\":\"image/png\",\"data\":\"" + pngB64 + "\"}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":14,\"candidatesTokenCount\":22,\"totalTokenCount\":36}}}\n\n"

	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	} {
		for _, claudeFamily := range []bool{true, false} {
			model := "gemini-3-pro-image"
			wantCarrierPrefix := "trc1:gemini:"
			if claudeFamily {
				model = "claude-sonnet-4-5"
				wantCarrierPrefix = "trc1:antigravity_claude:"
			}

			// 1. Streaming path.
			sinkStream := &convertedTestSink{}
			adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
			exStream := newTestAntigravityExchange(
				t,
				proto,
				buildTestClientRequest(proto, model, true),
				model,
				claudeFamily,
				nil,
			)
			resStream, sErr := adapter.ReadConvertedStreaming(
				upstream.NewDeferredOutputContext(sinkStream),
				&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(v1InternalSSE))},
				ConvertedOptions{Exchange: exStream},
			)
			require.NoError(t, sErr)
			require.NotNil(t, resStream.Usage)
			require.Equal(t, 14, resStream.Usage.InputTokens)
			require.Equal(t, 22, resStream.Usage.OutputTokens)
			require.Contains(t, sinkStream.body.String(), wantMarkdown)
			if proto != protocol.ProtocolOpenAIChatCompletions {
				require.Contains(t, sinkStream.body.String(), wantCarrierPrefix)
			}

			// 2. Buffered path.
			sinkBuf := &convertedTestSink{}
			exBuf := newTestAntigravityExchange(
				t,
				proto,
				buildTestClientRequest(proto, model, false),
				model,
				claudeFamily,
				nil,
			)
			resBuf, bErr := adapter.ReadConvertedBuffered(
				upstream.NewDeferredOutputContext(sinkBuf),
				&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(v1InternalSSE))},
				ConvertedOptions{Exchange: exBuf},
			)
			require.NoError(t, bErr)
			require.NotNil(t, resBuf.Usage)
			require.Equal(t, 14, resBuf.Usage.InputTokens)
			require.Equal(t, 22, resBuf.Usage.OutputTokens)
			require.Contains(t, sinkBuf.body.String(), wantMarkdown)
			if proto != protocol.ProtocolOpenAIChatCompletions {
				require.Contains(t, sinkBuf.body.String(), wantCarrierPrefix)
			}
		}
	}
}
