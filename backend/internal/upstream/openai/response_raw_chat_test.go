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
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// newTestMessagesToChatExchange builds a Messages-to-Chat Exchange for unit
// tests exercising ReadRawChatConvertedStreaming and ReadRawChatConvertedBuffered.
// Passing stream=false switches the inbound Messages payload to unary JSON mode
// while retaining tool-turn reasoning history settings on the target dialect.
func newTestMessagesToChatExchange(t *testing.T, stream bool) *convert.Exchange {
	t.Helper()
	reqJSON := `{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"hi"}]}`
	if !stream {
		reqJSON = `{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"messages":[{"role":"user","content":"hi"}]}`
	}
	ex, err := convert.NewExchange([]byte(reqJSON), convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolOpenAIChatCompletions,
			Dialect: convert.Dialect{
				ReasoningHistory: convert.HistoryToolTurns,
			},
		},
		ClientModel: "claude-sonnet-4-5",
	})
	require.NoError(t, err)
	return ex
}

// newTestResponsesToChatExchange builds a Responses-to-Chat Exchange with a
// custom free-text tool declaration so reader tests verify tool restoration and
// trc1:plain carrier sealing. Passing stream=false switches the client request
// to non-streaming mode while keeping function-only Chat tool support active.
func newTestResponsesToChatExchange(t *testing.T, stream bool) *convert.Exchange {
	t.Helper()
	reqJSON := `{"model":"gpt-5.4","stream":true,"input":"patch it","tools":[{"type":"custom","name":"apply_patch"}]}`
	if !stream {
		reqJSON = `{"model":"gpt-5.4","stream":false,"input":"patch it","tools":[{"type":"custom","name":"apply_patch"}]}`
	}
	ex, err := convert.NewExchange([]byte(reqJSON), convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolOpenAIChatCompletions,
			Dialect: convert.Dialect{
				Tools:            convert.ToolSupportFunction,
				ReasoningHistory: convert.HistoryToolTurns,
			},
		},
		ClientModel: "gpt-5.4",
	})
	require.NoError(t, err)
	return ex
}

// makeRawChatHTTPResponse constructs a 200 HTTP response carrying contentType,
// x-request-id reqID, and body string for converted raw Chat reader tests.
// Empty contentType or reqID inputs omit the corresponding header entry while
// wrapping body in an NopCloser reader.
func makeRawChatHTTPResponse(contentType, reqID, body string) *http.Response {
	h := make(http.Header)
	if contentType != "" {
		h.Set("Content-Type", contentType)
	}
	if reqID != "" {
		h.Set("x-request-id", reqID)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     h,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// TestReadRawChatConvertedStreaming_PreSemanticLeadingFrameFailover feeds an
// opening role-only Chat chunk followed by a retryable rate-limit error frame.
// Non-semantic client frames must remain staged so the reader returns the
// failover error without committing headers or payload bytes downstream.
func TestReadRawChatConvertedStreaming_PreSemanticLeadingFrameFailover(t *testing.T) {
	sseBody := "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"created\":1760000000,\"model\":\"deepseek-reasoner\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"error\":{\"type\":\"server_error\",\"code\":\"rate_limit_exceeded\",\"message\":\"upstream overloaded\"}}\n\n"

	sink := &convertedTestSink{}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestMessagesToChatExchange(t, true)
	resp := makeRawChatHTTPResponse("text/event-stream", "req_chat_fo_1", sseBody)

	sentinelFailover := errors.New("switch_to_secondary_chat_provider")
	got, err := ReadRawChatConvertedStreaming(
		outCtx,
		resp,
		ex,
		ConvertedRawChatOptions{
			DefaultErrorType: "api_error",
			StreamFailure: func(_ []byte, _ string, outputStarted bool) ConvertedFailure {
				require.False(t, outputStarted, "leading non-semantic frame must not mark outputStarted")
				return ConvertedFailure{Failover: sentinelFailover}
			},
		},
		"claude-sonnet-4-5",
		"deepseek-reasoner",
		nil,
		time.Now(),
	)

	require.ErrorIs(t, err, sentinelFailover)
	require.Nil(t, got)
	require.False(t, sink.committed)
	require.Zero(t, sink.buf.Len())
}

// TestReadRawChatConvertedStreaming_HeaderOnlyErrorBackfillAndEarlyCommittedSSE
// reproduces a waiting-heartbeat SSE stream where HTTP 200 SSE is already
// committed and the upstream sends a header-only event: error frame without a
// top-level error wrapper. Responses clients must receive an untimed local
// terminal error callback instead of raw JSON.
func TestReadRawChatConvertedStreaming_HeaderOnlyErrorBackfillAndEarlyCommittedSSE(t *testing.T) {
	sseBody := "event: error\ndata: {\"message\":\"gateway capacity exhausted\",\"code\":\"server_error\"}\n\n"

	sink := &convertedTestSink{committed: true}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestResponsesToChatExchange(t, true)
	resp := makeRawChatHTTPResponse("text/event-stream", "req_hdr_err_1", sseBody)

	var localErrCalls int
	var gotCode, gotMsg string
	res, err := ReadRawChatConvertedStreaming(
		outCtx,
		resp,
		ex,
		ConvertedRawChatOptions{
			DefaultErrorType: "upstream_error",
			CommittedSSE:     func() bool { return true },
			StreamFailure: func(_ []byte, message string, _ bool) ConvertedFailure {
				return ConvertedFailure{
					Status:  http.StatusBadGateway,
					Type:    "upstream_error",
					Code:    "server_error",
					Message: message,
				}
			},
			WriteLocalTerminalError: func(_ int, _, code, message string) {
				localErrCalls++
				gotCode = code
				gotMsg = message
			},
		},
		"gpt-5.4",
		"gpt-4o",
		nil,
		time.Now(),
	)

	require.Error(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, localErrCalls)
	require.Equal(t, "server_error", gotCode)
	require.Equal(t, "gateway capacity exhausted", gotMsg)
}

// TestReadRawChatConvertedStreaming_RestoresToolsSealsPlainCarrierAndCapturesFirstModel
// streams reasoning_content, custom tool arguments, and sparse trailing usage
// from a Chat upstream into a Responses client. It verifies first-declaration
// UpstreamResponseModel capture, trc1:plain carrier sealing, custom tool
// unwrapping, and exact billing usage accumulation.
func TestReadRawChatConvertedStreaming_RestoresToolsSealsPlainCarrierAndCapturesFirstModel(t *testing.T) {
	sseBody := ": keepalive\n\n" +
		"event: ping\ndata: {\"type\":\"ping\"}\n\n" +
		"data: {\"id\":\"chatcmpl_r1\",\"object\":\"chat.completion.chunk\",\"created\":1760000100,\"model\":\"deepseek-v3.2-0324\",\"service_tier\":\"priority\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"reasoning_content\":\"Plan patch first.\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl_r1\",\"object\":\"chat.completion.chunk\",\"created\":1760000100,\"model\":\"later-ignored-model\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_p1\",\"type\":\"function\",\"function\":{\"name\":\"apply_patch\",\"arguments\":\"{\\\"input\\\":\\\"*** Begin Patch***\\\"}\"}}]},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl_r1\",\"object\":\"chat.completion.chunk\",\"created\":1760000100,\"model\":\"later-ignored-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n" +
		"data: {\"id\":\"chatcmpl_r1\",\"object\":\"chat.completion.chunk\",\"created\":1760000100,\"model\":\"later-ignored-model\",\"choices\":[],\"usage\":{\"prompt_tokens\":45,\"completion_tokens\":18,\"total_tokens\":63,\"prompt_tokens_details\":{\"cached_tokens\":15},\"completion_tokens_details\":{\"reasoning_tokens\":7}}}\n\n" +
		"data: [DONE]"

	sink := &convertedTestSink{}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestResponsesToChatExchange(t, true)
	resp := makeRawChatHTTPResponse("text/event-stream", "req_ok_1", sseBody)
	effort := "high"
	tier := "priority"

	got, err := ReadRawChatConvertedStreaming(
		outCtx,
		resp,
		ex,
		ConvertedRawChatOptions{
			DefaultErrorType:     "upstream_error",
			ResolvedServiceTier:  func() *string { return &tier },
			NormalizeServiceTier: strings.ToLower,
		},
		"gpt-5.4",
		"deepseek-reasoner",
		&effort,
		time.Now(),
	)

	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, "deepseek-v3.2-0324", got.UpstreamResponseModel)
	require.Equal(t, 45, got.Usage.InputTokens)
	require.Equal(t, 15, got.Usage.CacheReadInputTokens)
	require.Equal(t, 18, got.Usage.OutputTokens)
	require.Equal(t, "priority", got.ServiceTier)
	require.NotNil(t, got.ResolvedTier)
	require.Equal(t, "priority", *got.ResolvedTier)

	streamText := sink.buf.String()
	require.Contains(t, streamText, `"trc1:plain:`)
	require.Contains(t, streamText, `"custom_tool_call"`)
	require.Contains(t, streamText, `"*** Begin Patch***"`)
	require.Contains(t, streamText, "event: response.completed\n")
}

// TestReadRawChatConvertedStreaming_MidstreamTruncationClosesOpenBlockAndKeepsUsage
// cuts a Chat stream after emitting semantic text and early usage without
// finish_reason or [DONE]. Messages clients must receive content_block_stop
// followed by event: error via WriteRawTerminalSSE while partial usage and
// UpstreamResponseModel survive on the returned result.
func TestReadRawChatConvertedStreaming_MidstreamTruncationClosesOpenBlockAndKeepsUsage(t *testing.T) {
	sseBody := "data: {\"id\":\"chatcmpl_trunc\",\"object\":\"chat.completion.chunk\",\"created\":1760000200,\"model\":\"gpt-4o-2026-08\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"partial answer\"},\"finish_reason\":null}],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":4,\"total_tokens\":24}}\n\n"

	sink := &convertedTestSink{}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestMessagesToChatExchange(t, true)
	resp := makeRawChatHTTPResponse("text/event-stream", "req_trunc_1", sseBody)

	var recordedTrunc error
	var rawTerminal []byte
	got, err := ReadRawChatConvertedStreaming(
		outCtx,
		resp,
		ex,
		ConvertedRawChatOptions{
			DefaultErrorType: "api_error",
			RecordTruncation: func(cause error) { recordedTrunc = cause },
			WriteRawTerminalSSE: func(data []byte) bool {
				rawTerminal = bytes.Clone(data)
				_, _ = sink.buf.Write(data)
				return true
			},
		},
		"claude-sonnet-4-5",
		"gpt-4o",
		nil,
		time.Now(),
	)

	require.Error(t, err)
	require.ErrorIs(t, recordedTrunc, ErrOpenAIUpstreamStreamTruncated)
	require.NotNil(t, got)
	require.Equal(t, "gpt-4o-2026-08", got.UpstreamResponseModel)
	require.Equal(t, 20, got.Usage.InputTokens)
	require.Equal(t, 4, got.Usage.OutputTokens)
	require.NotEmpty(t, rawTerminal)
	require.Contains(t, string(rawTerminal), "event: error")

	fullOut := sink.buf.String()
	stopIdx := strings.Index(fullOut, `"content_block_stop"`)
	errIdx := strings.Index(fullOut, "event: error")
	require.Greater(t, stopIdx, 0, "open text block must close before terminal error")
	require.Greater(t, errIdx, stopIdx, "terminal error must follow content_block_stop")
}

// TestReadRawChatConvertedStreaming_ClientDisconnectContinuesDrainingUsage
// injects a downstream write error after the opening chunk arrives. The reader
// must keep draining upstream chunks through [DONE] so trailing usage counters
// settle alongside ClientDisconnect=true.
func TestReadRawChatConvertedStreaming_ClientDisconnectContinuesDrainingUsage(t *testing.T) {
	sseBody := "data: {\"id\":\"chatcmpl_disc\",\"object\":\"chat.completion.chunk\",\"created\":1760000300,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hello\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"id\":\"chatcmpl_disc\",\"object\":\"chat.completion.chunk\",\"created\":1760000300,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\" world\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"id\":\"chatcmpl_disc\",\"object\":\"chat.completion.chunk\",\"created\":1760000300,\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":33,\"completion_tokens\":9,\"total_tokens\":42}}\n\n" +
		"data: [DONE]\n\n"

	sink := &convertedTestSink{failAfterWrites: 2}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestMessagesToChatExchange(t, true)
	resp := makeRawChatHTTPResponse("text/event-stream", "req_disc_1", sseBody)

	got, err := ReadRawChatConvertedStreaming(
		outCtx,
		resp,
		ex,
		ConvertedRawChatOptions{DefaultErrorType: "api_error"},
		"claude-sonnet-4-5",
		"gpt-4o",
		nil,
		time.Now(),
	)

	require.NoError(t, err)
	require.NotNil(t, got)
	require.True(t, got.ClientDisconnect)
	require.Equal(t, 33, got.Usage.InputTokens)
	require.Equal(t, 9, got.Usage.OutputTokens)
}

// TestReadRawChatConvertedStreaming_LargeRequestSilentRefusalFailsOver feeds an
// empty stop completion for a request body exceeding SilentRefusalMinRequestBodyBytes.
// SilentRefusal must return its failover error before any headers or payload
// bytes commit to the downstream sink.
func TestReadRawChatConvertedStreaming_LargeRequestSilentRefusalFailsOver(t *testing.T) {
	sseBody := "data: {\"id\":\"chatcmpl_sr\",\"object\":\"chat.completion.chunk\",\"created\":1760000400,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: [DONE]\n\n"

	sink := &convertedTestSink{}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestMessagesToChatExchange(t, true)
	resp := makeRawChatHTTPResponse("text/event-stream", "req_sr_1", sseBody)

	silentSentinel := errors.New("silent_refusal_failover")
	got, err := ReadRawChatConvertedStreaming(
		outCtx,
		resp,
		ex,
		ConvertedRawChatOptions{
			RequestBodyLen: SilentRefusalMinRequestBodyBytes + 128,
			SilentRefusal:  func() error { return silentSentinel },
		},
		"claude-sonnet-4-5",
		"gpt-4o",
		nil,
		time.Now(),
	)

	require.ErrorIs(t, err, silentSentinel)
	require.Nil(t, got)
	require.False(t, sink.committed)
	require.Zero(t, sink.buf.Len())
}

// TestReadRawChatConvertedBuffered_UnaryJSONAndSniffedSSE exercises unary Chat
// JSON conversion, sniffed SSE buffered folding, HTTP-200 error failover, and
// MissingUsage enforcement in ReadRawChatConvertedBuffered. Valid replies must
// emit unary client JSON with captured UpstreamResponseModel, whereas failover
// and missing-usage rejections leave the downstream writer uncommitted.
func TestReadRawChatConvertedBuffered_UnaryJSONAndSniffedSSE(t *testing.T) {
	t.Run("unary_json_seals_plain_carrier_and_restores_custom_tool", func(t *testing.T) {
		jsonBody := `{"id":"chatcmpl_b1","object":"chat.completion","created":1760000500,"model":"deepseek-r1-0528","choices":[{"index":0,"message":{"role":"assistant","content":null,"reasoning_content":"Verify patch context.","tool_calls":[{"id":"call_b1","type":"function","function":{"name":"apply_patch","arguments":"{\"input\":\"*** Update File ***\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":28,"completion_tokens":14,"total_tokens":42}}`

		sink := &convertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestResponsesToChatExchange(t, false)
		resp := makeRawChatHTTPResponse("application/json", "req_buf_1", jsonBody)

		got, err := ReadRawChatConvertedBuffered(
			outCtx,
			resp,
			ex,
			ConvertedRawChatOptions{DefaultErrorType: "upstream_error"},
			"gpt-5.4",
			"deepseek-reasoner",
			nil,
			time.Now(),
		)

		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, "deepseek-r1-0528", got.UpstreamResponseModel)
		require.Equal(t, 28, got.Usage.InputTokens)
		require.Equal(t, 14, got.Usage.OutputTokens)
		require.Equal(t, http.StatusOK, sink.status)
		outStr := sink.buf.String()
		require.Contains(t, outStr, `"trc1:plain:`)
		require.Contains(t, outStr, `"custom_tool_call"`)
		require.Contains(t, outStr, `"*** Update File ***"`)
	})

	t.Run("sniffed_sse_folds_into_unary_messages_json", func(t *testing.T) {
		sseBody := "data: {\"id\":\"chatcmpl_b2\",\"object\":\"chat.completion.chunk\",\"created\":1760000550,\"model\":\"kimi-k2\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"folded reply\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":3,\"total_tokens\":14}}\n\ndata: [DONE]\n\n"

		sink := &convertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestMessagesToChatExchange(t, false)
		resp := makeRawChatHTTPResponse("application/json", "req_buf_2", sseBody)

		got, err := ReadRawChatConvertedBuffered(
			outCtx,
			resp,
			ex,
			ConvertedRawChatOptions{DefaultErrorType: "api_error"},
			"claude-sonnet-4-5",
			"kimi-k2",
			nil,
			time.Now(),
		)

		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, "kimi-k2", got.UpstreamResponseModel)
		require.Equal(t, 11, got.Usage.InputTokens)
		require.Equal(t, 3, got.Usage.OutputTokens)
		require.Equal(t, "application/json; charset=utf-8", sink.header.Get("Content-Type"))
		require.Contains(t, sink.buf.String(), `"folded reply"`)
	})

	t.Run("http200_json_error_triggers_failover_without_writing_body", func(t *testing.T) {
		errJSON := `{"error":{"type":"server_error","code":"rate_limit_exceeded","message":"capacity full"}}`
		sink := &convertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestMessagesToChatExchange(t, false)
		resp := makeRawChatHTTPResponse("application/json", "req_buf_err", errJSON)

		foSentinel := errors.New("buffered_chat_failover")
		got, err := ReadRawChatConvertedBuffered(
			outCtx,
			resp,
			ex,
			ConvertedRawChatOptions{
				StreamFailure: func(_ []byte, _ string, _ bool) ConvertedFailure {
					return ConvertedFailure{Failover: foSentinel}
				},
			},
			"claude-sonnet-4-5",
			"gpt-4o",
			nil,
			time.Now(),
		)

		require.ErrorIs(t, err, foSentinel)
		require.Nil(t, got)
		require.False(t, sink.committed)
	})

	t.Run("missing_usage_callback_aborts_before_delivery", func(t *testing.T) {
		jsonBody := `{"id":"chatcmpl_b3","object":"chat.completion","created":1760000600,"model":"grok-4","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}]}`
		sink := &convertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestMessagesToChatExchange(t, false)
		resp := makeRawChatHTTPResponse("application/json", "req_buf_mu", jsonBody)

		missingSentinel := errors.New("grok_missing_usage")
		got, err := ReadRawChatConvertedBuffered(
			outCtx,
			resp,
			ex,
			ConvertedRawChatOptions{
				MissingUsage: func(model string, usage wire.ForwardUsage) error {
					require.Equal(t, "grok-4", model)
					require.Zero(t, usage.InputTokens)
					return missingSentinel
				},
			},
			"claude-sonnet-4-5",
			"grok-4",
			nil,
			time.Now(),
		)

		require.ErrorIs(t, err, missingSentinel)
		require.Nil(t, got)
		require.False(t, sink.committed)
	})
}

// TestReadRawChatConvertedStreaming_MidstreamCyberPolicyPreservesResponsesGrammar
// streams partial text output to a Responses client before emitting a Chat
// cyber_policy error frame. The reader must deliver stream.Close's terminal
// response.failed frame via WriteRawTerminalSSE instead of WriteLocalTerminalError
// so sequence numbers, response.id, and Abnormal stream grammar remain valid.
func TestReadRawChatConvertedStreaming_MidstreamCyberPolicyPreservesResponsesGrammar(t *testing.T) {
	sseBody := "data: {\"id\":\"chatcmpl_cyber_1\",\"object\":\"chat.completion.chunk\",\"created\":1760000700,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Step 1 started.\"},\"finish_reason\":null}]}\n\n" +
		"data: {\"error\":{\"type\":\"invalid_request_error\",\"code\":\"cyber_policy\",\"message\":\"Blocked by cyber policy\"}}\n\n"

	sink := &convertedTestSink{}
	outCtx := upstream.NewDeferredOutputContext(sink)
	ex := newTestResponsesToChatExchange(t, true)
	resp := makeRawChatHTTPResponse("text/event-stream", "req_cyber_mid_1", sseBody)

	cyberSentinel := errors.New("cyber_forwarded")
	var localTerminalCalls int
	var rawTerminalCalls int
	var markedCode string

	got, err := ReadRawChatConvertedStreaming(
		outCtx,
		resp,
		ex,
		ConvertedRawChatOptions{
			DefaultErrorType: "upstream_error",
			CyberForwarded:   cyberSentinel,
			MarkCyber: func(obs CyberObservation) {
				markedCode = obs.Code
			},
			WriteLocalTerminalError: func(_ int, _, _, _ string) {
				localTerminalCalls++
			},
			WriteRawTerminalSSE: func(data []byte) bool {
				rawTerminalCalls++
				_, _ = sink.buf.Write(data)
				return true
			},
		},
		"gpt-5.4",
		"gpt-4o",
		nil,
		time.Now(),
	)

	require.ErrorIs(t, err, cyberSentinel)
	require.NotNil(t, got)
	require.Equal(t, "cyber_policy", markedCode)
	require.Zero(t, localTerminalCalls, "started Responses stream must not fall back to bare WriteLocalTerminalError")
	require.Equal(t, 1, rawTerminalCalls)

	var frames []sse.Frame
	reader := sse.NewReader(bytes.NewReader(sink.buf.Bytes()))
	for reader.Scan() {
		f := reader.Frame()
		frames = append(frames, sse.Frame{
			Event: f.Event,
			Data:  bytes.Clone(f.Data),
		})
	}
	require.NoError(t, reader.Err())
	require.NotEmpty(t, frames)
	require.Equal(t, "response.failed", frames[len(frames)-1].Event)

	violations := testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{
		AllowAbnormal: true,
	})
	require.Empty(t, violations, "mid-stream cyber_policy Responses stream must satisfy Abnormal grammar: %v", violations)
}

// TestReadRawChatConverted_RejectsCollidingUsageEnvelopeAndPreservesHeaderChunks
// feeds ordinary Chat chunks carrying SSE event headers alongside chunks with
// colliding "Usage" and "usage" keys into the converted raw Chat readers.
// Event-headed Chat chunks must avoid synthetic top-level "type" injection, and
// colliding usage envelopes must be rejected from billing accumulation.
func TestReadRawChatConverted_RejectsCollidingUsageEnvelopeAndPreservesHeaderChunks(t *testing.T) {
	rawChunk := []byte(`{"id":"chatcmpl_h1","object":"chat.completion.chunk","created":1760000800,"model":"gpt-4o","service_tier":"flex","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}`)
	patched, _ := backfillHeaderOnlyChatErrorPayload("chat.completion.chunk", rawChunk, inspectRawChatPayload(rawChunk))
	require.Equal(t, string(rawChunk), string(patched), "ordinary Chat chunk with event header must not gain a type field")

	collidingSSE := "event: chat.completion.chunk\n" +
		"data: {\"id\":\"chatcmpl_col\",\"object\":\"chat.completion.chunk\",\"created\":1760000810,\"model\":\"gpt-4o\",\"service_tier\":\"flex\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"hello\"},\"finish_reason\":\"stop\"}],\"Usage\":{\"prompt_tokens\":999,\"completion_tokens\":999,\"total_tokens\":1998},\"usage\":{\"prompt_tokens\":42,\"completion_tokens\":8,\"total_tokens\":50}}\n\n" +
		"data: [DONE]\n\n"

	t.Run("streaming_rejects_colliding_usage_envelope", func(t *testing.T) {
		sink := &convertedTestSink{}
		outCtx := upstream.NewDeferredOutputContext(sink)
		ex := newTestMessagesToChatExchange(t, true)
		resp := makeRawChatHTTPResponse("text/event-stream", "req_col_stream", collidingSSE)

		got, err := ReadRawChatConvertedStreaming(
			outCtx,
			resp,
			ex,
			ConvertedRawChatOptions{DefaultErrorType: "api_error"},
			"claude-sonnet-4-5",
			"gpt-4o",
			nil,
			time.Now(),
		)
		// Client stream decoder rejects the colliding usage chunk while billing usage stays zero.
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Zero(t, got.Usage.InputTokens)
		require.Zero(t, got.Usage.OutputTokens)
	})

	t.Run("buffered_sse_rejects_colliding_usage_and_keeps_summary_service_tier", func(t *testing.T) {
		validSSE := "event: message\n" +
			"data: " + string(rawChunk) + "\n\n" +
			"data: [DONE]\n\n"
		sinkOK := &convertedTestSink{}
		outOK := upstream.NewDeferredOutputContext(sinkOK)
		exOK := newTestMessagesToChatExchange(t, false)
		respOK := makeRawChatHTTPResponse("text/event-stream", "req_tier_buf", validSSE)

		gotOK, errOK := ReadRawChatConvertedBuffered(
			outOK,
			respOK,
			exOK,
			ConvertedRawChatOptions{DefaultErrorType: "api_error"},
			"claude-sonnet-4-5",
			"gpt-4o",
			nil,
			time.Now(),
		)
		require.NoError(t, errOK)
		require.NotNil(t, gotOK)
		require.Equal(t, "flex", gotOK.ServiceTier)

		sinkCol := &convertedTestSink{}
		outCol := upstream.NewDeferredOutputContext(sinkCol)
		exCol := newTestMessagesToChatExchange(t, false)
		respCol := makeRawChatHTTPResponse("text/event-stream", "req_col_buf", collidingSSE)

		gotCol, errCol := ReadRawChatConvertedBuffered(
			outCol,
			respCol,
			exCol,
			ConvertedRawChatOptions{DefaultErrorType: "api_error"},
			"claude-sonnet-4-5",
			"gpt-4o",
			nil,
			time.Now(),
		)
		require.NoError(t, errCol)
		require.NotNil(t, gotCol)
		require.Zero(t, gotCol.Usage.InputTokens)
		require.Zero(t, gotCol.Usage.OutputTokens)
		require.NotContains(t, sinkCol.buf.String(), `"input_tokens":42`)
	})
}
