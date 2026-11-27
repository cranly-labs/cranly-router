package httpapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// TestConvertedOptions_PreCommittedSSEEmitsUntimedProtocolTerminalError checks
// WriteLocalTerminalError across Messages, Responses, and Chat clients after a
// waiting SSE heartbeat has committed HTTP 200. Each client receives its native
// terminal SSE error frame (synthetic response.failed on Responses) and marks
// TerminalSSEDelivered so outer attempt wrappers suppress duplicate errors.
func TestConvertedOptions_PreCommittedSSEEmitsUntimedProtocolTerminalError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, tc := range []struct {
		name     string              // test case name
		path     string              // request URL path
		source   protocol.ProtocolID // client protocol ID
		expected string              // expected SSE substring
	}{
		{
			name:     "messages client receives event error",
			path:     "/v1/messages",
			source:   protocol.ProtocolAnthropicMessages,
			expected: "event: error\ndata: {\"error\":{\"message\":\"upstream boom\",\"type\":\"api_error\"},\"type\":\"error\"}",
		},
		{
			name:     "responses client receives synthetic response.failed",
			path:     "/v1/responses",
			source:   protocol.ProtocolOpenAIResponses,
			expected: "event: response.failed\ndata: {\"type\":\"response.failed\"",
		},
		{
			name:     "chat client receives generic sse error",
			path:     "/v1/chat/completions",
			source:   protocol.ProtocolOpenAIChatCompletions,
			expected: "event: error\ndata: {\"error\":{\"code\":\"server_error\",\"message\":\"upstream boom\",\"type\":\"upstream_error\"}}",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
			c.Writer.Header().Set("Content-Type", "text/event-stream")
			c.Writer.WriteHeader(http.StatusOK)
			c.Writer.WriteHeaderNow()

			out := &OpenAIResponseOutput{}
			prov := &gatewayprovider.ExecutionProvider{
				Record: providercore.Record{ID: 55, Platform: "openai", Type: "apikey"},
			}
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Request-Id": []string{"req-123"}}}

			opts := out.ConvertedOptions(c, prov, resp, tc.source, "gpt-5.4", "gpt-5.4", "gpt-5.4", 128)
			require.True(t, opts.CommittedSSE())
			opts.WriteError(http.StatusBadGateway, opts.DefaultErrorType, "server_error", "upstream boom")

			require.True(t, TerminalSSEDelivered(c))
			require.Contains(t, rec.Body.String(), tc.expected)
		})
	}
}

// TestForwardAsChatCompletions_StreamCyberPolicyNoFailover_IRConversion runs
// a large streaming Chat request when the upstream Responses stream fails with
// cyber_policy before semantic output. The gateway commits HTTP 200 SSE without
// failover, records Ops cyber moderation metadata, and emits both the
// cyber_policy error frame and data: [DONE].
func TestForwardAsChatCompletions_StreamCyberPolicyNoFailover_IRConversion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"` + strings.Repeat("large prompt ", 6000) + `"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_cyber","model":"gpt-5.5","status":"in_progress","output":[]}}`,
		"",
		`event: response.failed`,
		`data: {"type":"response.failed","response":{"id":"resp_cyber","object":"response","model":"gpt-5.5","status":"failed","output":[],"error":{"code":"cyber_policy","message":"flagged for cyber policy"}}}`,
		"",
	}, "\n")
	upstreamTransport := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_cyber_ir"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstreamTransport})

	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           1,
			Name:         "openai-oauth",
			Platform:     capability.PlatformOpenAI,
			Type:         capability.ProviderTypeOAuth,
			Concurrency:  1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.5")
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr), "cyber must not trigger failover")
	require.NotNil(t, GetOpsCyberPolicy(c), "cyber mark must be set")
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/event-stream")
	respBody := rec.Body.String()
	require.Contains(t, respBody, `"error"`)
	require.Contains(t, respBody, `"cyber_policy"`)
	require.Contains(t, respBody, "data: [DONE]")
}
