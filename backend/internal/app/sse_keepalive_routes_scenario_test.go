package app

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	gatewayhttp "github.com/TokenFlux/TokenRouter/internal/gateway/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	opscore "github.com/TokenFlux/TokenRouter/internal/ops"
	opsprovider "github.com/TokenFlux/TokenRouter/internal/ops/provider"
)

// sse_keepalive_routes_scenario_test.go exercises gateway_middleware.go,
// gateway_messages_http.go, gateway_openai_text_http.go, and httpapi/routes.go
// with production handlers and transport observation assembled through
// newHeartbeatRouteRouter. Authentication and storage use in-memory lab
// fixtures while the delayed upstream and client use real HTTP connections.

// TestGatewayRoutesKeepaliveBeforeFirstEvent registers the actual gateway routes
// and handlers across client protocols and native upstream families. Delaying
// either headers or the first payload by twenty-five idle intervals must produce
// a local heartbeat before upstream output is released. Responses
// header visibility also determines the expected provider turn-state ownership.
func TestGatewayRoutesKeepaliveBeforeFirstEvent(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			for _, path := range []string{"/v1/messages", "/v1/chat/completions", "/v1/responses"} {
				for _, platform := range []string{"anthropic", "openai", "gemini", "grok", "openai_passthrough", "openai_chat"} {
					t.Run(path+"/"+platform, func(t *testing.T) { testGatewayRouteKeepalive(t, path, platform, phase == "headers") })
				}
			}
			t.Run("gemini native", func(t *testing.T) {
				testGatewayRouteKeepalive(t, "/v1beta/models/gemini-2.5-flash:streamGenerateContent", "gemini", phase == "headers")
			})
			for _, path := range []string{"/responses", "/backend-api/codex/responses", "/chat/completions"} {
				t.Run("alias"+path, func(t *testing.T) { testGatewayRouteKeepalive(t, path, "openai", phase == "headers") })
			}
		})
	}
}

// testGatewayRouteKeepalive holds upstream events while a real downstream reads.
// The selected phase delays either response headers or the first payload by
// twenty-five heartbeat intervals. A beat observed only after release cannot pass this
// test, even if the final response content otherwise looks correct. Successful
// cases keep both servers alive until the client consumes terminal output;
// failures release the client and servers through the registered cleanup.
func testGatewayRouteKeepalive(t *testing.T, path, kind string, delayHeaders bool) {
	t.Helper()
	const interval = 10 * time.Millisecond
	var released atomic.Bool
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestBody, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		_ = r.Body.Close()
		// Read request content before flushing HTTP/1 response headers, which may
		// otherwise discard the unread body before the protocol fixture inspects it.
		r.Body = io.NopCloser(bytes.NewReader(requestBody))
		if delayHeaders {
			select {
			case <-time.After(25 * interval):
			case <-r.Context().Done():
				return
			}
			released.Store(true)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Request-ID", "route-upstream-request")
		w.Header().Set("Request-ID", "route-anthropic-request")
		w.Header().Set(gatewayhttp.CodexTurnStateHeader, "route-state")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("fixture lacks HTTP flushing")
			return
		}
		flusher.Flush()
		if !delayHeaders {
			select {
			case <-time.After(25 * interval):
			case <-r.Context().Done():
				return
			}
			released.Store(true)
		}
		if strings.Contains(r.URL.Path, "chat/completions") {
			_, _ = io.WriteString(w, "data: {\"id\":\"chat-route\",\"model\":\"gpt-5.4\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok-mixed\"},\"finish_reason\":\"stop\"}]}\n\ndata: {\"id\":\"chat-route\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3,\"total_tokens\":13}}\n\ndata: [DONE]\n\n")
			return
		}
		mixedUpstreamResponse(w, r, false)
	}))
	t.Cleanup(upstreamServer.Close)
	router, origins := newHeartbeatRouteRouter(t, upstreamServer.URL, kind, interval)
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)
	model := map[string]string{"anthropic": "claude-sonnet-4-5-20250929", "gemini": "gemini-2.5-flash", "grok": "grok-4.5", "openai": "gpt-5.4", "openai_passthrough": "gpt-5.4", "openai_chat": "gpt-5.4"}[kind]
	body := fmt.Sprintf(`{"model":%q,"max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`, model)
	if strings.HasSuffix(path, "/responses") {
		body = fmt.Sprintf(`{"model":%q,"input":"hello","stream":true}`, model)
	}
	if strings.Contains(path, "streamGenerateContent") {
		body = `{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`
	}
	client := server.Client()
	client.Timeout = 3 * time.Second
	req, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Session-Id", "route-header-state")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()
	reader := bufio.NewReader(resp.Body)
	var received strings.Builder
	heartbeat := false
	for !heartbeat {
		line, readErr := reader.ReadString('\n')
		_, _ = received.WriteString(line)
		if line == ": keepalive\n" || line == "event: ping\n" || (strings.Contains(path, "streamGenerateContent") && received.String() == "\n\n") {
			heartbeat = true
			require.False(t, released.Load(), "heartbeat arrived only after the first upstream event")
			break
		}
		if readErr != nil {
			break
		}
		if strings.Contains(line, "ok-mixed") {
			break
		}
	}
	rest, err := io.ReadAll(reader)
	require.NoError(t, err)
	_, _ = received.Write(rest)
	require.True(t, heartbeat, received.String())
	require.Equal(t, http.StatusOK, resp.StatusCode, received.String())
	require.Contains(t, received.String(), "ok-mixed")
	if strings.HasSuffix(path, "/responses") {
		owner, known := origins.Owner("8\x00route-header-state")
		if resp.Header.Get(gatewayhttp.CodexTurnStateHeader) == "" {
			require.False(t, known, "undelivered upstream state claimed provider ownership")
		} else {
			require.True(t, known)
			require.Equal(t, int64(1), owner)
		}
	}
}

// TestGatewayRoutesKeepaliveBeforeUpstreamFailure delays upstream headers beyond
// the downstream keepalive interval before returning 502, closing the connection,
// or returning a deterministic 400 error. Every public text route must emit only
// valid SSE frames with a terminal protocol error and record a non-recovered Ops
// stream failure with failed request telemetry.
func TestGatewayRoutesKeepaliveBeforeUpstreamFailure(t *testing.T) {
	paths := []string{"/v1/messages", "/v1/chat/completions", "/v1/responses"}
	platforms := []string{"anthropic", "openai", "openai_passthrough", "openai_chat", "gemini"}

	for _, path := range paths {
		for _, platform := range platforms {
			// Gemini upstream exchange retries 502 and closed connections with multi-second
			// backoff; cover its non-retrying 400 and 401 failure paths directly here.
			failures := []string{"502", "close", "400"}
			if platform == "gemini" {
				failures = []string{"400", "401"}
			}
			for _, failure := range failures {
				t.Run(path+"/"+platform+"/"+failure, func(t *testing.T) {
					testGatewayRouteKeepaliveUpstreamFailure(t, path, platform, failure)
				})
			}
		}
	}
}

// testGatewayRouteKeepaliveUpstreamFailure exercises a single-provider gateway
// route where DownstreamSSEKeepalive commits HTTP 200 text/event-stream before
// the delayed upstream fails. It verifies early heartbeat delivery, strict SSE
// framing with a terminal error event, failed telemetry state, and an Ops entry
// carrying a failed status rather than a recovered upstream summary.
func testGatewayRouteKeepaliveUpstreamFailure(t *testing.T, path, kind, failure string) {
	t.Helper()
	const interval = 10 * time.Millisecond
	var released atomic.Bool
	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		select {
		case <-time.After(15 * interval):
		case <-r.Context().Done():
			return
		}
		released.Store(true)
		switch failure {
		case "close":
			if hj, ok := w.(http.Hijacker); ok {
				conn, _, err := hj.Hijack()
				if err == nil {
					_ = conn.Close()
					return
				}
			}
			w.WriteHeader(http.StatusBadGateway)
		case "400":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":{"type":"invalid_request_error","code":"invalid_param","message":"invalid request parameter"}}`)
		case "401":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"type":"authentication_error","message":"upstream unauthorized"}}`)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = io.WriteString(w, `{"error":{"type":"upstream_error","message":"upstream bad gateway"}}`)
		}
	}))
	t.Cleanup(upstreamServer.Close)

	queue := newOpsCaptureQueue(4)
	ops := opscore.NewOpsService(nil, nil, nil, nil, nil, nil, nil, opsprovider.LogControl{})
	var lastRecord atomic.Value
	router, _ := newHeartbeatRouteRouter(t, upstreamServer.URL, kind, interval, heartbeatRouteOptions{
		Ops:      ops,
		OpsQueue: queue,
		OnTelemetry: func(r telemetry.RequestRecord) {
			lastRecord.Store(r)
		},
	})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	model := map[string]string{"anthropic": "claude-sonnet-4-5-20250929", "gemini": "gemini-2.5-flash", "openai": "gpt-5.4", "openai_passthrough": "gpt-5.4", "openai_chat": "gpt-5.4"}[kind]
	body := fmt.Sprintf(`{"model":%q,"max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`, model)
	if strings.HasSuffix(path, "/responses") {
		body = fmt.Sprintf(`{"model":%q,"input":"hello","stream":true}`, model)
	}
	client := server.Client()
	client.Timeout = 3 * time.Second
	req, err := http.NewRequest(http.MethodPost, server.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer func() { require.NoError(t, resp.Body.Close()) }()

	reader := bufio.NewReader(resp.Body)
	var received strings.Builder
	heartbeat := false
	for !heartbeat {
		line, readErr := reader.ReadString('\n')
		_, _ = received.WriteString(line)
		if line == ": keepalive\n" || line == "event: ping\n" {
			heartbeat = true
			require.False(t, released.Load(), "heartbeat arrived only after upstream failure")
			break
		}
		if readErr != nil {
			break
		}
	}
	rest, err := io.ReadAll(reader)
	require.NoError(t, err)
	_, _ = received.Write(rest)

	raw := received.String()
	require.True(t, heartbeat, raw)
	require.Equal(t, http.StatusOK, resp.StatusCode, raw)
	require.Contains(t, strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream")
	assertRouteSSELinesValid(t, raw)

	switch path {
	case "/v1/responses":
		require.Contains(t, raw, "event: response.failed\n", raw)
		require.Contains(t, raw, `"type":"response.failed"`, raw)
	case "/v1/messages":
		require.Contains(t, raw, `"type":"error"`, raw)
	default:
		require.Contains(t, raw, "event: error\n", raw)
	}

	require.Eventually(t, func() bool { return len(queue.jobs) > 0 }, time.Second, time.Millisecond)
	job := <-queue.jobs
	require.GreaterOrEqual(t, job.entry.StatusCode, http.StatusBadRequest, "Ops entry must record failed status, got %d (%q)", job.entry.StatusCode, job.entry.ErrorMessage)
	require.NotContains(t, job.entry.ErrorMessage, "Recovered upstream error")
	record, ok := lastRecord.Load().(telemetry.RequestRecord)
	require.True(t, ok)
	require.Equal(t, "failed", record.State)
	require.Equal(t, "stream_error", record.ErrorCode)
}

// assertRouteSSELinesValid checks that every non-blank line in body is valid
// SSE framing and every data line contains valid JSON, catching appended JSON.
func assertRouteSSELinesValid(t *testing.T, body string) {
	t.Helper()
	for line := range strings.SplitSeq(body, "\n") {
		if line == "" || strings.HasPrefix(line, ":") || strings.HasPrefix(line, "event: ") {
			continue
		}
		require.True(t, strings.HasPrefix(line, "data: "), "non-SSE line found in route response: %q (full body: %q)", line, body)
		payload := strings.TrimPrefix(line, "data: ")
		require.True(t, json.Valid([]byte(payload)), "SSE data line must hold valid JSON: %q", payload)
	}
}
