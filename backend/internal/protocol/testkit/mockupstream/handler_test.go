package mockupstream

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"testing/fstest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// tbCapture embeds testing.TB to intercept Fatalf messages triggered by AssertNoViolations.
type tbCapture struct {
	testing.TB        // embedded base interface satisfying private testing.TB marker
	fatalMsg   string // formatted Fatalf diagnostic message captured before panic unwind
}

// Helper records tbCapture as a test helper without delegating to a nil embedded TB.
func (c *tbCapture) Helper() {}

// Fatalf stores the formatted failure message on c and aborts the current call
// via panic. Callers recover the sentinel c pointer in captureAssertFailure to
// inspect the diagnostic text without failing the outer test.
func (c *tbCapture) Fatalf(format string, args ...any) {
	c.fatalMsg = fmt.Sprintf(format, args...)
	panic(c)
}

// intPtr allocates an int pointer for testkit.Match and testkit.Fault literals.
func intPtr(v int) *int {
	return &v
}

// boolPtr allocates a bool pointer for testkit.Match literals.
func boolPtr(v bool) *bool {
	return &v
}

// captureAssertFailure runs h.AssertNoViolations against a local tbCapture
// double and reports whether Fatalf terminated the call alongside its
// formatted diagnostic text. Any panic value other than the tbCapture
// sentinel is re-thrown unchanged.
func captureAssertFailure(h *Handler) (bool, string) {
	capTB := &tbCapture{}
	failed := false
	func() {
		defer func() {
			if r := recover(); r == capTB {
				failed = true
			} else if r != nil {
				panic(r)
			}
		}()
		h.AssertNoViolations(capTB)
	}()
	return failed, capTB.fatalMsg
}

// TestHandlerMultiTurnAndSidecarDefault runs a two-turn tool loop interleaved
// with an unmatched client sidecar request (such as background title generation).
// Conversation-shape matching must route sidecar traffic to Script.Default
// without advancing or desynchronizing main turns, and AssertNoViolations must pass.
func TestHandlerMultiTurnAndSidecarDefault(t *testing.T) {
	turn1SSE := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"role\":\"assistant\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	turn2SSE := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_2\",\"role\":\"assistant\"}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	sidecarJSON := `{"id":"msg_title","type":"message","role":"assistant","content":[{"type":"text","text":"Title"}]}`

	handler := New(testkit.Script{
		Name: "multi_turn_with_sidecar",
		Turns: []testkit.Turn{
			{
				Match: testkit.Match{
					AssistantMessages: intPtr(0),
					ToolResults:       intPtr(0),
					ModelPrefix:       "claude-sonnet",
					HasTools:          boolPtr(true),
				},
				Reply: testkit.Reply{Raw: turn1SSE},
			},
			{
				Match: testkit.Match{
					AssistantMessages: intPtr(1),
					ToolResults:       intPtr(1),
					ModelPrefix:       "claude-sonnet",
					HasTools:          boolPtr(true),
				},
				Reply: testkit.Reply{Raw: turn2SSE},
			},
		},
		Default: testkit.Reply{Raw: sidecarJSON},
	})

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	postMessages := func(body string) (int, string, string) {
		resp, err := srv.Client().Post(srv.URL+"/v1/messages", "application/json", strings.NewReader(body))
		require.NoError(t, err)
		defer func() { _ = resp.Body.Close() }()
		raw, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		return resp.StatusCode, resp.Header.Get("Content-Type"), string(raw)
	}

	// Sidecar Haiku title request arrives before Turn 1 and falls through to Default.
	status, ct, body := postMessages(`{"model":"claude-haiku-4-5","max_tokens":32,"messages":[{"role":"user","content":"summarize title"}]}`)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "application/json", ct)
	require.Equal(t, sidecarJSON, body)

	// Main Turn 1 request matches Turns[0].
	status, ct, body = postMessages(`{
		"model":"claude-sonnet-4-5",
		"max_tokens":128,
		"stream":true,
		"tools":[{"name":"bash","input_schema":{"type":"object"}}],
		"messages":[{"role":"user","content":"run ls"}]
	}`)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "text/event-stream", ct)
	require.Equal(t, turn1SSE, body)

	// Main Turn 2 request with one assistant tool_use and one user tool_result matches Turns[1].
	status, ct, body = postMessages(`{
		"model":"claude-sonnet-4-5",
		"max_tokens":128,
		"stream":true,
		"tools":[{"name":"bash","input_schema":{"type":"object"}}],
		"messages":[
			{"role":"user","content":"run ls"},
			{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"bash","input":{}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"file.txt"}]}
		]
	}`)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "text/event-stream", ct)
	require.Equal(t, turn2SSE, body)

	handler.AssertNoViolations(t)

	reqs := handler.Requests()
	require.Len(t, reqs, 3)
	require.Equal(t, -1, reqs[0].MatchedTurn)
	require.True(t, reqs[0].UsedDefault)
	require.Equal(t, 0, reqs[1].MatchedTurn)
	require.False(t, reqs[1].UsedDefault)
	require.Equal(t, 1, reqs[2].MatchedTurn)
	require.False(t, reqs[2].UsedDefault)
}

// TestHandlerCorpusRawFixturesAndStage0Guard loads Reply.Raw fixture paths from
// an in-memory Corpus alongside missing-file and Reply.Events or Reply.Response
// synthesis cases. Valid .sse and .json fixtures replay verbatim and registered
// text codecs synthesize HTTP 200 replies, while missing paths or unregistered
// protocol Events replies return HTTP 501 and log check errors.
func TestHandlerCorpusRawFixturesAndStage0Guard(t *testing.T) {
	memFS := fstest.MapFS{
		"wire/responses/stream/basic.sse": &fstest.MapFile{
			Data: []byte("event: response.created\r\ndata: {\"type\":\"response.created\"}\r\n\r\n"),
		},
		"wire/gemini/response/basic.json": &fstest.MapFile{
			Data: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]}}]}`),
		},
	}
	corpus := testkit.NewCorpus(memFS)

	t.Run("corpus sse and json fixtures replay verbatim", func(t *testing.T) {
		h := NewWithConfig(Config{
			Corpus: corpus,
			Script: testkit.Script{
				Name: "corpus_lookup",
				Turns: []testkit.Turn{
					{
						Match: testkit.Match{ModelPrefix: "gpt-"},
						Reply: testkit.Reply{Raw: "wire/responses/stream/basic.sse"},
					},
					{
						Match: testkit.Match{ModelPrefix: "gemini-"},
						Reply: testkit.Reply{Raw: "wire/gemini/response/basic.json"},
					},
				},
			},
		})
		srv := httptest.NewServer(h)
		t.Cleanup(srv.Close)

		resp1, err := srv.Client().Post(srv.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"gpt-5.4","input":"hello","stream":true}`))
		require.NoError(t, err)
		raw1, err := io.ReadAll(resp1.Body)
		_ = resp1.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp1.StatusCode)
		require.Equal(t, "text/event-stream", resp1.Header.Get("Content-Type"))
		require.Equal(t, "event: response.created\r\ndata: {\"type\":\"response.created\"}\r\n\r\n", string(raw1))

		resp2, err := srv.Client().Post(srv.URL+"/v1beta/models/gemini-2.5-flash:generateContent", "application/json", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`))
		require.NoError(t, err)
		raw2, err := io.ReadAll(resp2.Body)
		_ = resp2.Body.Close()
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp2.StatusCode)
		require.Equal(t, "application/json", resp2.Header.Get("Content-Type"))
		require.JSONEq(t, `{"candidates":[{"content":{"role":"model","parts":[{"text":"hi"}]}}]}`, string(raw2))
		h.AssertNoViolations(t)
	})

	t.Run("missing corpus path and unregistered or invalid IR synthesis report explicit errors", func(t *testing.T) {
		var irScript testkit.Script
		err := json.Unmarshal([]byte(`{
			"name":"ir_errors",
			"turns":[
				{"match":{"model_prefix":"gpt-"},"reply":{"raw":"wire/missing.sse"}},
				{"match":{"model_prefix":"claude-"},"reply":{"events":[{"type":"response_start"}]}},
				{"match":{"model_prefix":"gemini-"},"reply":{}}
			]
		}`), &irScript)
		require.NoError(t, err)

		h := NewWithConfig(Config{Corpus: corpus, Script: irScript})
		rec1 := httptest.NewRecorder()
		req1 := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.4","input":"hi"}`))
		h.ServeHTTP(rec1, req1)
		require.Equal(t, http.StatusNotImplemented, rec1.Code)
		require.Contains(t, rec1.Body.String(), "wire/missing.sse")

		rec2 := httptest.NewRecorder()
		req2 := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":32,"messages":[{"role":"user","content":"hi"}]}`))
		h.ServeHTTP(rec2, req2)
		require.Equal(t, http.StatusNotImplemented, rec2.Code)
		require.Contains(t, rec2.Body.String(), "invalid Reply.Events stream")

		rec3 := httptest.NewRecorder()
		req3 := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-2.5-flash:generateContent", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`))
		h.ServeHTTP(rec3, req3)
		require.Equal(t, http.StatusNotImplemented, rec3.Code)
		require.Contains(t, rec3.Body.String(), "empty Raw, Events, and Response payloads")
		require.Len(t, h.Violations(), 3)

		_, unregErr := h.resolveReply(
			Endpoint{Protocol: "qoder_chat"},
			nil,
			RequestSummary{Model: "qoder-1"},
			testkit.Reply{Response: &ir.Response{Finish: ir.Finish{Reason: ir.FinishStop}}},
		)
		require.ErrorContains(t, unregErr, "no registered response encoder")
	})

	t.Run("registered codecs decode IR requests and synthesize Reply.Events and Reply.Response", func(t *testing.T) {
		h := New(testkit.Script{
			Name: "ir_synthesis",
			Turns: []testkit.Turn{
				{
					Match: testkit.Match{ModelPrefix: "claude-", AssistantMessages: intPtr(0)},
					Expect: []testkit.Check{{
						Description: "decoded IR request received",
						Func: func(req *ir.Request, _ []byte) error {
							if req == nil || req.Model != "claude-sonnet-4-5" || len(req.Messages) != 1 {
								return fmt.Errorf("unexpected decoded request: %+v", req)
							}
							return nil
						},
					}},
					Reply: testkit.Reply{
						Events: []ir.Event{
							{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "msg_synth_1", Model: "claude-sonnet-4-5"}},
							{Kind: ir.EventPartStart, Index: 0, Part: &ir.Part{Kind: ir.PartText}},
							{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Text: "Synthesized hello"}},
							{Kind: ir.EventPartEnd, Index: 0, Part: &ir.Part{Kind: ir.PartText, Text: "Synthesized hello"}},
							{Kind: ir.EventUsage, Usage: &ir.Usage{Input: 12, Output: 4}},
							{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishStop}},
						},
					},
				},
				{
					Match: testkit.Match{ModelPrefix: "gpt-5"},
					Reply: testkit.Reply{
						Response: &ir.Response{
							ID:        "resp_synth_2",
							Model:     "gpt-5",
							CreatedAt: 1760000000,
							Parts:     []ir.Part{{Kind: ir.PartText, Text: "Buffered and streamed synthesis"}},
							Finish:    ir.Finish{Reason: ir.FinishStop},
							Usage:     &ir.Usage{Input: 10, CacheRead: 5, Output: 6},
						},
					},
				},
				{
					Match: testkit.Match{ModelPrefix: "gemini-"},
					Expect: []testkit.Check{{
						Description: "decoded Gemini IR request received",
						Func: func(req *ir.Request, _ []byte) error {
							if req == nil || len(req.Messages) != 1 {
								return fmt.Errorf("unexpected Gemini decoded request: %+v", req)
							}
							return nil
						},
					}},
					Reply: testkit.Reply{
						Response: &ir.Response{
							ID:     "gem_synth_3",
							Model:  "gemini-2.5-flash",
							Parts:  []ir.Part{{Kind: ir.PartText, Text: "Gemini IR synthesis"}},
							Finish: ir.Finish{Reason: ir.FinishStop},
							Usage:  &ir.Usage{Input: 8, CacheRead: 4, Output: 5, Reasoning: 2},
						},
					},
				},
			},
		})

		// 1. Streaming Messages request receives grammar-valid synthesized SSE stream.
		msgStreamRec := httptest.NewRecorder()
		msgStreamReq := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`))
		h.ServeHTTP(msgStreamRec, msgStreamReq)
		require.Equal(t, http.StatusOK, msgStreamRec.Code)
		require.Equal(t, "text/event-stream", msgStreamRec.Header().Get("Content-Type"))
		msgFrames, err := testkit.ParseSSEBytes(msgStreamRec.Body.Bytes())
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateMessagesStream(msgFrames, testkit.GrammarOptions{}))

		// 2. Non-streaming Messages request folds Reply.Events into valid Messages JSON.
		msgBufRec := httptest.NewRecorder()
		msgBufReq := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`))
		h.ServeHTTP(msgBufRec, msgBufReq)
		require.Equal(t, http.StatusOK, msgBufRec.Code)
		require.Equal(t, "application/json", msgBufRec.Header().Get("Content-Type"))
		require.Contains(t, msgBufRec.Body.String(), `"Synthesized hello"`)

		// 3. Streaming Responses request synthesizes grammar-valid SSE from Reply.Response.
		rspStreamRec := httptest.NewRecorder()
		rspStreamReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5","input":"hi","stream":true}`))
		h.ServeHTTP(rspStreamRec, rspStreamReq)
		require.Equal(t, http.StatusOK, rspStreamRec.Code)
		rspFrames, err := testkit.ParseSSEBytes(rspStreamRec.Body.Bytes())
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, rspFrames, testkit.GrammarOptions{}))

		// 4. Buffered Chat request against same Reply.Response synthesizes valid Chat JSON.
		chatBufRec := httptest.NewRecorder()
		chatBufReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-5","messages":[{"role":"user","content":"hi"}]}`))
		h.ServeHTTP(chatBufRec, chatBufReq)
		require.Equal(t, http.StatusOK, chatBufRec.Code)
		require.Equal(t, "application/json", chatBufRec.Header().Get("Content-Type"))
		require.Contains(t, chatBufRec.Body.String(), `"prompt_tokens":15`)

		// 5. Streaming and buffered Gemini requests decode IR and synthesize valid Gemini replies.
		gemStreamRec := httptest.NewRecorder()
		gemStreamReq := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`))
		h.ServeHTTP(gemStreamRec, gemStreamReq)
		require.Equal(t, http.StatusOK, gemStreamRec.Code)
		gemFrames, err := testkit.ParseSSEBytes(gemStreamRec.Body.Bytes())
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateGeminiStream(gemFrames, testkit.GrammarOptions{}))

		gemBufRec := httptest.NewRecorder()
		gemBufReq := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-2.5-flash:generateContent", strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`))
		h.ServeHTTP(gemBufRec, gemBufReq)
		require.Equal(t, http.StatusOK, gemBufRec.Code)
		require.Equal(t, "application/json", gemBufRec.Header().Get("Content-Type"))
		require.Contains(t, gemBufRec.Body.String(), `"Gemini IR synthesis"`)
		require.Contains(t, gemBufRec.Body.String(), `"promptTokenCount":12`)

		h.AssertNoViolations(t)
		reqs := h.Requests()
		require.Len(t, reqs, 6)
		require.NotNil(t, reqs[0].Decoded)
		require.Equal(t, "claude-sonnet-4-5", reqs[0].Decoded.Model)
		require.NotNil(t, reqs[4].Decoded)
	})
}

// TestHandlerStrictValidationAndProviderErrorShapes submits malformed or
// dialect-violating requests across all four protocols. Handler must reject each
// request with HTTP 400 in that provider's native JSON error envelope and log
// every RuleViolation for AssertNoViolations.
func TestHandlerStrictValidationAndProviderErrorShapes(t *testing.T) {
	h := NewWithConfig(Config{
		Validation: testkit.RequestValidationOptions{
			StrictTopLevel: true,
			DialectRules:   []string{"deepseek-thinking", "gemini-signature"},
		},
		Script: testkit.Script{
			Name:    "validation_gate",
			Default: testkit.Reply{Raw: `{"ok":true}`},
		},
	})

	// 1. Anthropic Messages unknown top-level field triggers MSG-STRICT-TOP-LEVEL.
	msgRec := httptest.NewRecorder()
	msgReq := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":64,"unexpected_flag":true,"messages":[{"role":"user","content":"hi"}]}`))
	h.ServeHTTP(msgRec, msgReq)
	require.Equal(t, http.StatusBadRequest, msgRec.Code)
	var msgErr anthropicErrorEnvelope
	require.NoError(t, json.Unmarshal(msgRec.Body.Bytes(), &msgErr))
	require.Equal(t, "error", msgErr.Type)
	require.Equal(t, "invalid_request_error", msgErr.Error.Type)
	require.Contains(t, msgErr.Error.Message, "MSG-STRICT-TOP-LEVEL")

	// 2. OpenAI Responses unpaired function_call triggers RSP-CALL-PAIRING.
	rspRec := httptest.NewRecorder()
	rspReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.4","input":[{"type":"function_call","call_id":"c1","name":"fn","arguments":"{}"}]}`))
	h.ServeHTTP(rspRec, rspReq)
	require.Equal(t, http.StatusBadRequest, rspRec.Code)
	var rspErr openAIErrorEnvelope
	require.NoError(t, json.Unmarshal(rspRec.Body.Bytes(), &rspErr))
	require.Equal(t, "invalid_request_error", rspErr.Error.Type)
	require.Equal(t, "$.input[0].call_id", rspErr.Error.Param)
	require.Contains(t, rspErr.Error.Message, "RSP-CALL-PAIRING")

	// 3. OpenAI Chat missing reasoning_content under deepseek-thinking triggers CHAT-DEEPSEEK-THINKING.
	chatRec := httptest.NewRecorder()
	chatReq := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
		"model":"deepseek-reasoner",
		"messages":[
			{"role":"user","content":"hi"},
			{"role":"assistant","tool_calls":[{"id":"c1","type":"function","function":{"name":"fn","arguments":"{}"}}]},
			{"role":"tool","tool_call_id":"c1","content":"ok"}
		]
	}`))
	h.ServeHTTP(chatRec, chatReq)
	require.Equal(t, http.StatusBadRequest, chatRec.Code)
	var chatErr openAIErrorEnvelope
	require.NoError(t, json.Unmarshal(chatRec.Body.Bytes(), &chatErr))
	require.Contains(t, chatErr.Error.Message, "CHAT-DEEPSEEK-THINKING")

	// 4. Gemini missing thoughtSignature under gemini-signature triggers GEM-SIGNATURE.
	gemRec := httptest.NewRecorder()
	gemReq := httptest.NewRequest(http.MethodPost, "/v1beta/models/gemini-2.5-pro:streamGenerateContent", strings.NewReader(`{
		"contents":[
			{"role":"user","parts":[{"text":"hi"}]},
			{"role":"model","parts":[{"functionCall":{"name":"fn","args":{}}}]},
			{"role":"user","parts":[{"functionResponse":{"name":"fn","response":{"ok":true}}}]}
		]
	}`))
	h.ServeHTTP(gemRec, gemReq)
	require.Equal(t, http.StatusBadRequest, gemRec.Code)
	var gemErr geminiErrorEnvelope
	require.NoError(t, json.Unmarshal(gemRec.Body.Bytes(), &gemErr))
	require.Equal(t, http.StatusBadRequest, gemErr.Error.Code)
	require.Equal(t, "INVALID_ARGUMENT", gemErr.Error.Status)
	require.Contains(t, gemErr.Error.Message, "GEM-SIGNATURE")

	violations := h.Violations()
	require.Len(t, violations, 4)
	failed, diag := captureAssertFailure(h)
	require.True(t, failed)
	require.Contains(t, diag, "MSG-STRICT-TOP-LEVEL")
	require.Contains(t, diag, "GEM-SIGNATURE")
}

// TestHandlerExpectChecksAndDuplicateMatchRetry drives two turns sharing an
// identical Match predicate where the first injects HTTP 529 and the second
// enforces Turn.Expect assertions. Retry attempts must advance to the second
// turn without logging fault violations while failed Expect checks log errors.
func TestHandlerExpectChecksAndDuplicateMatchRetry(t *testing.T) {
	h := New(testkit.Script{
		Name: "retry_then_expect",
		Turns: []testkit.Turn{
			{
				Match: testkit.Match{AssistantMessages: intPtr(0)},
				Fault: &testkit.Fault{Status: 529},
			},
			{
				Match: testkit.Match{AssistantMessages: intPtr(0)},
				Expect: []testkit.Check{
					{Description: "model check", JSONPath: "$.model", Equals: "claude-sonnet-4-5"},
					{Description: "body contains keyword", Contains: "hello"},
				},
				Reply: testkit.Reply{Raw: `{"id":"msg_ok","type":"message"}`},
			},
		},
	})

	// Attempt 1 hits Turns[0] and receives Anthropic-shaped 529 overloaded_error without logging a violation.
	rec1 := httptest.NewRecorder()
	req1 := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`))
	h.ServeHTTP(rec1, req1)
	require.Equal(t, 529, rec1.Code)
	require.Contains(t, rec1.Body.String(), "overloaded_error")
	h.AssertNoViolations(t)

	// Attempt 2 with identical conversation shape advances to Turns[1] and succeeds.
	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":32,"messages":[{"role":"user","content":"hello"}]}`))
	h.ServeHTTP(rec2, req2)
	require.Equal(t, http.StatusOK, rec2.Code)
	require.JSONEq(t, `{"id":"msg_ok","type":"message"}`, rec2.Body.String())
	h.AssertNoViolations(t)

	// Attempt 3 stays on Turns[1] and triggers the Expect failure when "hello" is absent.
	rec3 := httptest.NewRecorder()
	req3 := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":32,"messages":[{"role":"user","content":"goodbye"}]}`))
	h.ServeHTTP(rec3, req3)
	require.Equal(t, http.StatusBadRequest, rec3.Code)
	require.Len(t, h.Violations(), 1)
	require.Contains(t, h.Violations()[0].CheckErrors[0], "body contains keyword")
}

// TestHandlerFaultDelayCancelAndFrameTruncation tests pre-response delay
// cancellation and verbatim SSE frame truncation across both real TCP servers
// and in-memory ResponseRecorders. Clients reading a truncated stream over HTTP
// must receive the exact N leading frames followed by an abrupt EOF error.
func TestHandlerFaultDelayCancelAndFrameTruncation(t *testing.T) {
	quirkSSE := "\xef\xbb\xbf: initial comment\r\ndata:\r\n\r\nevent: response.created\r\ndata: {\"type\":\"response.created\"}\r\n\r\n: mid comment\ndata: {\"type\":\"delta\",\ndata: \"part\":1}\n\ndata: [DONE]\n\n"

	h := New(testkit.Script{
		Name: "faults",
		Turns: []testkit.Turn{
			{
				Match: testkit.Match{ContainsText: "cut-two"},
				Reply: testkit.Reply{Raw: quirkSSE},
				Fault: &testkit.Fault{TruncateAfterFrames: intPtr(2)},
			},
			{
				Match: testkit.Match{ContainsText: "cut-zero"},
				Reply: testkit.Reply{Raw: quirkSSE},
				Fault: &testkit.Fault{TruncateAfterFrames: intPtr(0)},
			},
			{
				Match: testkit.Match{ContainsText: "slow-turn"},
				Reply: testkit.Reply{Raw: `{"id":"slow"}`},
				Fault: &testkit.Fault{DelayMs: 500},
			},
		},
	})

	// Verify in-memory ResponseRecorder truncation does not panic without Hijacker.
	memRec := httptest.NewRecorder()
	memReq := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.4","input":"cut-two","stream":true}`))
	h.ServeHTTP(memRec, memReq)
	require.Equal(t, http.StatusOK, memRec.Code)
	memFrames, err := testkit.ParseSSEBytes(memRec.Body.Bytes())
	require.NoError(t, err)
	require.Len(t, memFrames, 2)
	require.Equal(t, "response.created", memFrames[0].Event)
	require.Equal(t, "{\"type\":\"delta\",\n\"part\":1}", string(memFrames[1].Data))
	require.True(t, strings.HasPrefix(memRec.Body.String(), "\xef\xbb\xbf: initial comment\r\n"))

	h.Reset()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	// Real TCP server truncation after 2 frames delivers both frames then ErrUnexpectedEOF.
	resp, err := srv.Client().Post(srv.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"gpt-5.4","input":"cut-two","stream":true}`))
	require.NoError(t, err)
	gotBytes, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.Error(t, readErr, "truncated chunked stream must surface a transport EOF error")
	tcpFrames, parseErr := testkit.ParseSSEBytes(gotBytes)
	require.NoError(t, parseErr)
	require.Len(t, tcpFrames, 2)

	// Truncation at 0 frames commits HTTP 200 text/event-stream and cuts before frame 1.
	respZero, err := srv.Client().Post(srv.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"gpt-5.4","input":"cut-zero","stream":true}`))
	require.NoError(t, err)
	zeroBytes, zeroErr := io.ReadAll(respZero.Body)
	_ = respZero.Body.Close()
	require.Error(t, zeroErr)
	require.Empty(t, zeroBytes)

	// Client cancellation during DelayMs aborts cleanly and still logs the attempt.
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	slowReq, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5.4","input":"slow-turn"}`))
	require.NoError(t, err)
	_, slowErr := srv.Client().Do(slowReq)
	require.Error(t, slowErr)

	require.Eventually(t, func() bool {
		return len(h.Requests()) == 3
	}, time.Second, 5*time.Millisecond)
	h.AssertNoViolations(t)
}

// TestHandlerUnknownPathMethodResetAndConcurrentCloning exercises non-generation
// paths, invalid HTTP methods, concurrent requests, unmatched turns, and Reset.
// Probe 404 and 405 responses must leave Violations empty while returned
// RecordedRequest values remain isolated from caller mutation.
func TestHandlerUnknownPathMethodResetAndConcurrentCloning(t *testing.T) {
	h := New(testkit.Script{
		Name: "concurrency_and_edges",
		Turns: []testkit.Turn{
			{
				Match: testkit.Match{ContainsText: "match-me"},
				Reply: testkit.Reply{Raw: "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n"},
			},
		},
	})

	// Token-counting probe returns 404 without adding a violation.
	probeRec := httptest.NewRecorder()
	h.ServeHTTP(probeRec, httptest.NewRequest(http.MethodPost, "/v1/messages/count_tokens", strings.NewReader(`{}`)))
	require.Equal(t, http.StatusNotFound, probeRec.Code)
	h.AssertNoViolations(t)

	// GET on generation endpoint returns 405 without adding a violation.
	getRec := httptest.NewRecorder()
	h.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil))
	require.Equal(t, http.StatusMethodNotAllowed, getRec.Code)
	h.AssertNoViolations(t)

	// Concurrent matching requests remain race-free and return independent clones.
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-5.4","stream":true,"messages":[{"role":"user","content":"match-me"}]}`))
			req.Header.Set("X-Trace", "orig")
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusOK, rec.Code)
		})
	}
	wg.Wait()
	h.AssertNoViolations(t)

	reqs := h.Requests()
	require.Len(t, reqs, 10)
	reqs[2].Body[0] = 'X'
	reqs[2].Headers.Set("X-Trace", "mutated")
	fresh := h.Requests()
	require.Equal(t, byte('{'), fresh[2].Body[0])
	require.Equal(t, "orig", fresh[2].Headers.Get("X-Trace"))

	// Unmatched turn with empty Default records a violation with observed summary counts.
	unmatchedRec := httptest.NewRecorder()
	h.ServeHTTP(unmatchedRec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-5.4","messages":[{"role":"user","content":"other"}]}`)))
	require.Equal(t, http.StatusBadRequest, unmatchedRec.Code)
	require.Len(t, h.Violations(), 1)
	require.Contains(t, h.Violations()[0].CheckErrors[0], "no script turn matched request")

	h.Reset()
	require.Empty(t, h.Requests())
	require.Empty(t, h.Violations())
}
