package openai

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// convertedTestSink records Begin and Emit operations from upstream.OutputContext
// while optionally failing client writes after a configured emit count.
type convertedTestSink struct {
	committed        bool                   // pre-existing or recorded HTTP header commitment
	contentCommitted bool                   // recorded retry-marked payload commitment
	status           int                    // HTTP status passed to Begin
	header           http.Header            // cloned header map captured on Begin
	events           []upstream.OutputEvent // ordered emitted output events
	buf              bytes.Buffer           // concatenated emitted payload bytes
	failAfterWrites  int                    // 1-based payload write index at which Emit begins failing
	payloadWrites    int                    // count of non-empty payload writes seen so far
}

// Begin records HTTP status and cloned response headers on s and marks HTTP
// commitment true without failing.
func (s *convertedTestSink) Begin(head upstream.OutputHead) error {
	s.committed = true
	s.status = head.Status
	s.header = head.Header.Clone()
	return nil
}

// Emit appends cloned payload bytes onto s.buf and records retry commitment.
// When failAfterWrites is positive and payloadWrites reaches that threshold,
// Emit returns a simulated client disconnect error instead of writing bytes.
func (s *convertedTestSink) Emit(ev upstream.OutputEvent) error {
	cp := ev
	cp.Data = bytes.Clone(ev.Data)
	s.events = append(s.events, cp)
	if len(ev.Data) > 0 {
		s.payloadWrites++
		if s.failAfterWrites > 0 && s.payloadWrites >= s.failAfterWrites {
			return errors.New("simulated client disconnect")
		}
		_, _ = s.buf.Write(ev.Data)
		if ev.CommitForRetry {
			s.contentCommitted = true
		}
	}
	return nil
}

// OutputState exposes current commitment flags without initializing headers.
func (s *convertedTestSink) OutputState() upstream.OutputHead {
	return upstream.OutputHead{
		Committed:        s.committed,
		ContentCommitted: s.contentCommitted,
		Status:           s.status,
	}
}

// newTestMessagesToResponsesExchange builds a Messages-to-Responses Exchange for reader unit tests.
func newTestMessagesToResponsesExchange(t *testing.T) *convert.Exchange {
	t.Helper()
	ex, err := convert.NewExchange(
		[]byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"hi"}]}`),
		convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect: convert.Dialect{
					ReplayScheme: "openai",
					ProviderID:   42,
				},
			},
			ClientModel: "claude-sonnet-4-5",
		},
	)
	require.NoError(t, err)
	return ex
}

// newTestChatToResponsesExchange builds a Chat-to-Responses Exchange for reader unit tests.
func newTestChatToResponsesExchange(t *testing.T, includeUsage bool) *convert.Exchange {
	t.Helper()
	raw := `{"model":"gpt-5.4","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	if includeUsage {
		raw = `{"model":"gpt-5.4","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`
	}
	ex, err := convert.NewExchange(
		[]byte(raw),
		convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolOpenAIResponses,
			},
			ClientModel: "gpt-5.4",
		},
	)
	require.NoError(t, err)
	return ex
}

// TestReadConvertedStreaming_LeadingFrameStagingAllowsPreSemanticFailover sends
// response.created and response.in_progress before a retryable response.failed
// event. Non-semantic leading frames stay staged in memory so zero bytes reach
// the client sink and the failover error returns cleanly with a nil result.
func TestReadConvertedStreaming_LeadingFrameStagingAllowsPreSemanticFailover(t *testing.T) {
	sseBody := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\",\"model\":\"gpt-5.4-2026\"}}\n\n" +
		"event: response.in_progress\n" +
		"data: {\"type\":\"response.in_progress\",\"sequence_number\":1,\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\",\"model\":\"gpt-5.4-2026\"}}\n\n" +
		"event: response.failed\n" +
		"data: {\"type\":\"response.failed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_1\",\"status\":\"failed\",\"model\":\"gpt-5.4-2026\",\"error\":{\"code\":\"server_error\",\"message\":\"overloaded\"}}}\n\n"

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"X-Request-Id": []string{"req_stage_1"}},
		Body:       io.NopCloser(strings.NewReader(sseBody)),
	}
	sink := &convertedTestSink{}
	out := upstream.NewDeferredOutputContext(sink)
	sentinelFailover := errors.New("switch-to-provider-2")

	res, err := ReadConvertedStreaming(resp, out, newTestMessagesToResponsesExchange(t), ConvertedResponseOptions{
		StreamFailure: func(_ []byte, _ string, outputStarted, _ bool) ConvertedFailure {
			require.False(t, outputStarted, "non-semantic leading frames must not mark outputStarted")
			return ConvertedFailure{Failover: sentinelFailover}
		},
	}, "claude-sonnet-4-5", "gpt-5.4", time.Now())

	require.ErrorIs(t, err, sentinelFailover)
	require.Nil(t, res)
	require.False(t, sink.committed)
	require.Empty(t, sink.buf.String())
}

// TestReadConvertedStreaming_CodexStandaloneErrorRecoveredByCompleted emits a
// standalone Codex error frame followed by semantic output and response.completed.
// Lifecycle tracking clears the bare error so the client receives a complete
// Anthropic Messages stream and raw UpstreamResponseModel is preserved.
func TestReadConvertedStreaming_CodexStandaloneErrorRecoveredByCompleted(t *testing.T) {
	sseBody := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_rec\",\"status\":\"in_progress\",\"model\":\"gpt-5.3-codex-internal\"}}\n\n" +
		"event: error\n" +
		"data: {\"type\":\"error\",\"message\":\"transient warning\"}\n\n" +
		"event: response.output_item.added\n" +
		"data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\n" +
		"data: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"recovered ok\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"sequence_number\":4,\"response\":{\"id\":\"resp_rec\",\"status\":\"completed\",\"model\":\"gpt-5.3-codex-internal\",\"output\":[],\"usage\":{\"input_tokens\":25,\"output_tokens\":4,\"input_tokens_details\":{\"cached_tokens\":5}}}}\n\n"

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"X-Request-Id": []string{"req_rec"}},
		Body:       io.NopCloser(strings.NewReader(sseBody)),
	}
	sink := &convertedTestSink{}
	out := upstream.NewDeferredOutputContext(sink)

	res, err := ReadConvertedStreaming(resp, out, newTestMessagesToResponsesExchange(t), ConvertedResponseOptions{
		CodexFailureTerminal: true,
		StreamFailure: func(_ []byte, msg string, _, _ bool) ConvertedFailure {
			return ConvertedFailure{Status: http.StatusBadGateway, Type: "api_error", Message: msg}
		},
	}, "claude-sonnet-4-5", "gpt-5.3-codex", time.Now())

	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, "gpt-5.3-codex-internal", res.UpstreamResponseModel)
	require.Equal(t, "resp_rec", res.ResponseID)
	require.Equal(t, 25, res.Usage.InputTokens)
	require.Equal(t, 5, res.Usage.CacheReadInputTokens)
	require.Equal(t, 4, res.Usage.OutputTokens)
	clientSSE := sink.buf.String()
	require.Contains(t, clientSSE, "recovered ok")
	require.Contains(t, clientSSE, "event: message_stop")
	require.NotContains(t, clientSSE, "transient warning")
}

// TestReadConvertedStreaming_CommittedSSEUsesUntimedLocalTerminalError pre-commits
// HTTP 200 SSE via a waiting keepalive before an unrecoverable upstream failure
// arrives. ReadConvertedStreaming invokes WriteLocalTerminalError rather than
// writing a JSON status envelope or timed payload event.
func TestReadConvertedStreaming_CommittedSSEUsesUntimedLocalTerminalError(t *testing.T) {
	sseBody := "event: response.failed\n" +
		"data: {\"type\":\"response.failed\",\"sequence_number\":0,\"response\":{\"id\":\"resp_fail\",\"status\":\"failed\",\"model\":\"gpt-5.4-snap\",\"error\":{\"code\":\"invalid_prompt\",\"message\":\"bad input\"}}}\n\n"

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(sseBody)),
	}
	sink := &convertedTestSink{committed: true, status: http.StatusOK}
	out := upstream.NewDeferredOutputContext(sink)

	localTerminalCalls := 0
	var gotType, gotCode, gotMsg string
	res, err := ReadConvertedStreaming(resp, out, newTestMessagesToResponsesExchange(t), ConvertedResponseOptions{
		CommittedSSE: func() bool { return true },
		StreamFailure: func(_ []byte, msg string, _, _ bool) ConvertedFailure {
			return ConvertedFailure{
				Status:  http.StatusBadRequest,
				Type:    "invalid_request_error",
				Code:    "invalid_prompt",
				Message: msg,
			}
		},
		WriteLocalTerminalError: func(_ int, errType, code, message string) {
			localTerminalCalls++
			gotType = errType
			gotCode = code
			gotMsg = message
		},
	}, "claude-sonnet-4-5", "gpt-5.4", time.Now())

	require.Error(t, err)
	require.NotNil(t, res)
	require.Equal(t, "gpt-5.4-snap", res.UpstreamResponseModel)
	require.Equal(t, 1, localTerminalCalls)
	require.Equal(t, "invalid_request_error", gotType)
	require.Equal(t, "invalid_prompt", gotCode)
	require.Equal(t, "bad input", gotMsg)
	require.Empty(t, sink.buf.String(), "timed sinkWriter must not emit bytes when WriteLocalTerminalError handles committed SSE")
}

// TestReadConvertedStreaming_MidstreamTruncationClosesOpenBlockAndCapturesModel
// truncates an upstream Responses stream mid-tool-call after semantic text was
// delivered. ReadConvertedStreaming emits content_block_stop before the terminal
// error event and still reports partial usage plus raw UpstreamResponseModel.
func TestReadConvertedStreaming_MidstreamTruncationClosesOpenBlockAndCapturesModel(t *testing.T) {
	sseBody := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_trunc\",\"status\":\"in_progress\",\"model\":\"gpt-5.4-raw\"}}\n\n" +
		"event: response.in_progress\n" +
		"data: {\"type\":\"response.in_progress\",\"sequence_number\":1,\"response\":{\"id\":\"resp_trunc\",\"status\":\"in_progress\",\"model\":\"gpt-5.4-raw\",\"usage\":{\"input_tokens\":30,\"output_tokens\":2}}}\n\n" +
		"event: response.output_item.added\n" +
		"data: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"fc_1\",\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"bash\",\"arguments\":\"\"}}\n\n" +
		"event: response.function_call_arguments.delta\n" +
		"data: {\"type\":\"response.function_call_arguments.delta\",\"sequence_number\":3,\"item_id\":\"fc_1\",\"output_index\":0,\"delta\":\"{\\\"cmd\\\":\\\"ls\"}\n\n"

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(sseBody)),
	}
	sink := &convertedTestSink{}
	out := upstream.NewDeferredOutputContext(sink)

	var rawTerminal string
	res, err := ReadConvertedStreaming(resp, out, newTestMessagesToResponsesExchange(t), ConvertedResponseOptions{
		DefaultErrorType: "api_error",
		WriteRawTerminalSSE: func(data []byte) bool {
			rawTerminal = string(data)
			return true
		},
	}, "claude-sonnet-4-5", "gpt-5.4", time.Now())

	require.Error(t, err)
	require.NotNil(t, res)
	require.Equal(t, "gpt-5.4-raw", res.UpstreamResponseModel)
	require.Equal(t, 30, res.Usage.InputTokens)
	require.Contains(t, sink.buf.String(), "event: content_block_stop")
	require.Contains(t, rawTerminal, "event: error")
}

// TestReadConvertedStreaming_ClientDisconnectContinuesDrainingForBilling simulates
// a downstream write failure after the first semantic delta. ReadConvertedStreaming
// keeps scanning the upstream body until response.completed and returns final usage.
func TestReadConvertedStreaming_ClientDisconnectContinuesDrainingForBilling(t *testing.T) {
	sseBody := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_disc\",\"status\":\"in_progress\",\"model\":\"gpt-5.4-raw\"}}\n\n" +
		"event: response.output_item.added\n" +
		"data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\n" +
		"data: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n" +
		"event: response.output_text.delta\n" +
		"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"sequence_number\":4,\"response\":{\"id\":\"resp_disc\",\"status\":\"completed\",\"model\":\"gpt-5.4-raw\",\"output\":[],\"usage\":{\"input_tokens\":44,\"output_tokens\":9}}}\n\n"

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(sseBody)),
	}
	sink := &convertedTestSink{failAfterWrites: 2}
	out := upstream.NewDeferredOutputContext(sink)

	res, err := ReadConvertedStreaming(resp, out, newTestMessagesToResponsesExchange(t), ConvertedResponseOptions{}, "claude-sonnet-4-5", "gpt-5.4", time.Now())
	require.NoError(t, err)
	require.NotNil(t, res)
	require.True(t, res.ClientDisconnect)
	require.Equal(t, 44, res.Usage.InputTokens)
	require.Equal(t, 9, res.Usage.OutputTokens)
}

// TestReadConvertedStreaming_SilentRefusalAndEmptyCompletedHoldOutput checks
// both NativeOpenAI empty completions and large-request Chat silent refusals.
// Both return their dedicated failover errors with zero committed client bytes.
func TestReadConvertedStreaming_SilentRefusalAndEmptyCompletedHoldOutput(t *testing.T) {
	emptySSE := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_e\",\"status\":\"in_progress\",\"model\":\"gpt-5.4\"}}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_e\",\"status\":\"completed\",\"model\":\"gpt-5.4\",\"output\":[]}}\n\n"

	emptySentinel := errors.New("empty-completed-failover")
	sink1 := &convertedTestSink{}
	res1, err1 := ReadConvertedStreaming(
		&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(emptySSE))},
		upstream.NewDeferredOutputContext(sink1),
		newTestMessagesToResponsesExchange(t),
		ConvertedResponseOptions{
			NativeOpenAI:   true,
			EmptyCompleted: func(string) error { return emptySentinel },
		},
		"claude-sonnet-4-5", "gpt-5.4", time.Now(),
	)
	require.ErrorIs(t, err1, emptySentinel)
	require.Nil(t, res1)
	require.Empty(t, sink1.buf.String())

	silentSentinel := errors.New("silent-refusal-failover")
	sink2 := &convertedTestSink{}
	res2, err2 := ReadConvertedStreaming(
		&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(emptySSE))},
		upstream.NewDeferredOutputContext(sink2),
		newTestChatToResponsesExchange(t, false),
		ConvertedResponseOptions{
			RequestBodyLen: SilentRefusalMinRequestBodyBytes + 128,
			SilentRefusal:  func() error { return silentSentinel },
		},
		"gpt-5.4", "gpt-5.4", time.Now(),
	)
	require.ErrorIs(t, err2, silentSentinel)
	require.Nil(t, res2)
	require.Empty(t, sink2.buf.String())
}

// TestReadConvertedBuffered_FoldsResponsesSSEAndSealsCarriers reads a complete
// Responses SSE stream with encrypted reasoning and text into buffered Messages
// JSON. The client body receives a trc1:openai: signature envelope and echoes
// the client model while CompatResponseResult retains raw UpstreamResponseModel.
func TestReadConvertedBuffered_FoldsResponsesSSEAndSealsCarriers(t *testing.T) {
	sseBody := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_buf\",\"status\":\"in_progress\",\"model\":\"gpt-5.4-2026-08\"}}\n\n" +
		"event: response.output_item.done\n" +
		"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"rs_1\",\"type\":\"reasoning\",\"encrypted_content\":\"gAAAAAB_cipher\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"checked\"}]}}\n\n" +
		"event: response.output_item.done\n" +
		"data: {\"type\":\"response.output_item.done\",\"sequence_number\":2,\"output_index\":1,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello buffered\"}]}}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"sequence_number\":3,\"response\":{\"id\":\"resp_buf\",\"status\":\"completed\",\"model\":\"gpt-5.4-2026-08\",\"output\":[],\"usage\":{\"input_tokens\":18,\"output_tokens\":6}}}\n\n"

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(sseBody)),
	}
	sink := &convertedTestSink{}
	out := upstream.NewDeferredOutputContext(sink)

	res, err := ReadConvertedBuffered(resp, out, newTestMessagesToResponsesExchange(t), ConvertedResponseOptions{}, "claude-sonnet-4-5", "gpt-5.4", time.Now())
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, "gpt-5.4-2026-08", res.UpstreamResponseModel)
	require.Equal(t, 18, res.Usage.InputTokens)
	require.Equal(t, 6, res.Usage.OutputTokens)
	require.Equal(t, "application/json; charset=utf-8", sink.header.Get("Content-Type"))
	bodyStr := sink.buf.String()
	require.Contains(t, bodyStr, `"model":"claude-sonnet-4-5"`)
	require.Contains(t, bodyStr, `"hello buffered"`)
	require.Contains(t, bodyStr, `trc1:openai:`)
}

// TestReadConvertedStreaming_HeaderOnlyEventTypesAndLargeChatSemanticRelease
// streams bare response lifecycle objects and header-only response.refusal.delta
// frames to a large-request Chat client without usage or terminal output arrays.
// Semantic output releases held frames immediately, prevents false SilentRefusal
// failover, and folds cleanly in ReadConvertedBuffered without double wrapping.
func TestReadConvertedStreaming_HeaderOnlyEventTypesAndLargeChatSemanticRelease(t *testing.T) {
	sseBody := "event: response.created\n" +
		"data: {\"id\":\"resp_hdr\",\"object\":\"response\",\"status\":\"in_progress\",\"model\":\"gpt-5.4-raw\"}\n\n" +
		"event: response.output_item.added\n" +
		"data: {\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"in_progress\",\"content\":[]}}\n\n" +
		"event: response.content_part.added\n" +
		"data: {\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"refusal\",\"refusal\":\"\"}}\n\n" +
		"event: response.refusal.delta\n" +
		"data: {\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"cannot comply\"}\n\n" +
		"event: response.completed\n" +
		"data: {\"id\":\"resp_hdr\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"gpt-5.4-raw\",\"output\":[]}\n\n"

	silentSentinel := errors.New("unexpected-silent-refusal")
	streamSink := &convertedTestSink{}
	resStream, errStream := ReadConvertedStreaming(
		&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(sseBody))},
		upstream.NewDeferredOutputContext(streamSink),
		newTestChatToResponsesExchange(t, false),
		ConvertedResponseOptions{
			RequestBodyLen: SilentRefusalMinRequestBodyBytes + 256,
			SilentRefusal:  func() error { return silentSentinel },
		},
		"gpt-5.4", "gpt-5.4", time.Now(),
	)
	require.NoError(t, errStream)
	require.NotNil(t, resStream)
	require.Equal(t, "gpt-5.4-raw", resStream.UpstreamResponseModel)
	require.Equal(t, "resp_hdr", resStream.ResponseID)
	require.Contains(t, streamSink.buf.String(), `"refusal":"cannot comply"`)
	require.Contains(t, streamSink.buf.String(), "data: [DONE]\n\n")

	bufSink := &convertedTestSink{}
	resBuf, errBuf := ReadConvertedBuffered(
		&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(sseBody))},
		upstream.NewDeferredOutputContext(bufSink),
		newTestChatToResponsesExchange(t, false),
		ConvertedResponseOptions{},
		"gpt-5.4", "gpt-5.4", time.Now(),
	)
	require.NoError(t, errBuf)
	require.NotNil(t, resBuf)
	require.Equal(t, "resp_hdr", resBuf.ResponseID)
	require.Equal(t, "gpt-5.4-raw", resBuf.UpstreamResponseModel)
	require.Contains(t, bufSink.buf.String(), `"id":"resp_hdr"`)
	require.Contains(t, bufSink.buf.String(), `"refusal":"cannot comply"`)
}

// TestReadConvertedStreaming_CyberPolicyBeforeOutputCommitsHTTP200SSE sends a
// cyber_policy failure after non-semantic response.created on unstarted deferred
// writers for both Messages and Chat clients. Both paths commit HTTP 200 SSE
// rather than HTTP 400 JSON, record MarkCyber, and append data: [DONE] on Chat.
func TestReadConvertedStreaming_CyberPolicyBeforeOutputCommitsHTTP200SSE(t *testing.T) {
	cyberSSE := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_cyb\",\"status\":\"in_progress\",\"model\":\"gpt-5.5\"}}\n\n" +
		"event: response.failed\n" +
		"data: {\"type\":\"response.failed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_cyb\",\"object\":\"response\",\"status\":\"failed\",\"model\":\"gpt-5.5\",\"output\":[],\"error\":{\"code\":\"cyber_policy\",\"message\":\"flagged for cyber policy\"}}}\n\n"

	cyberSentinel := errors.New("cyber-forwarded")
	var chatCyber CyberObservation
	chatSink := &convertedTestSink{}
	resChat, errChat := ReadConvertedStreaming(
		&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(cyberSSE))},
		upstream.NewDeferredOutputContext(chatSink),
		newTestChatToResponsesExchange(t, false),
		ConvertedResponseOptions{
			RequestBodyLen:    SilentRefusalMinRequestBodyBytes + 64,
			AppendDoneOnCyber: true,
			CyberForwarded:    cyberSentinel,
			MarkCyber:         func(obs CyberObservation) { chatCyber = obs },
		},
		"gpt-5.5", "gpt-5.5", time.Now(),
	)
	require.ErrorIs(t, errChat, cyberSentinel)
	require.NotNil(t, resChat)
	require.Equal(t, http.StatusOK, chatSink.status)
	require.Equal(t, "text/event-stream", chatSink.header.Get("Content-Type"))
	require.Equal(t, "cyber_policy", chatCyber.Code)
	require.Contains(t, chatSink.buf.String(), `"cyber_policy"`)
	require.Contains(t, chatSink.buf.String(), "data: [DONE]\n\n")

	msgSink := &convertedTestSink{}
	resMsg, errMsg := ReadConvertedStreaming(
		&http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(cyberSSE))},
		upstream.NewDeferredOutputContext(msgSink),
		newTestMessagesToResponsesExchange(t),
		ConvertedResponseOptions{
			CyberForwarded: cyberSentinel,
		},
		"claude-sonnet-4-5", "gpt-5.5", time.Now(),
	)
	require.ErrorIs(t, errMsg, cyberSentinel)
	require.NotNil(t, resMsg)
	require.Equal(t, http.StatusOK, msgSink.status)
	require.Equal(t, "text/event-stream", msgSink.header.Get("Content-Type"))
	require.Contains(t, msgSink.buf.String(), "event: error")
}
