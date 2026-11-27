package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
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
	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	upstreamcore "github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// TestBufferRawChatCompletions_RejectsOversizedResponse supplies a native Chat
// body larger than the configured read limit. It checks the body-limit error,
// absent result and HTTP 502 response.
func TestBufferRawChatCompletions_RejectsOversizedResponse(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader("toolong")),
	}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions()})
	svc.Output.Options.ReadLimit = 3

	result, err := openai.ReadRawChatBuffered(upstreamcore.NewDeferredOutputContext(ResponseSink{Writer: c.Writer}), resp, svc.Output.RawOptions(c, resp, rawChatCompletionsTestProvider(), "gpt-5.4", "gpt-5.4", nil, WriteForwardChatError), "gpt-5.4", "gpt-5.4", nil, time.Now())
	require.ErrorIs(t, err, httpclient.ErrResponseBodyTooLarge)
	require.Nil(t, result)
	require.Equal(t, http.StatusBadGateway, rec.Code)
}

// TestConvertedRawOptions_PreCommittedSSEAndTruncationFailover prepares a Gin
// context whose HTTP 200 SSE headers were already committed before response
// reading began. ConvertedRawOptions must emit untimed client-protocol terminal
// SSE errors (synthetic response.failed for Responses, event: error for Messages)
// and build UpstreamFailoverError values carrying provider headers on truncation.
func TestConvertedRawOptions_PreCommittedSSEAndTruncationFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, tc := range []struct {
		name     string              // subtest name
		path     string              // inbound endpoint path
		source   protocol.ProtocolID // client protocol ID
		expected string              // expected terminal SSE substring
	}{
		{
			name:     "messages client receives anthropic event error",
			path:     "/v1/messages",
			source:   protocol.ProtocolAnthropicMessages,
			expected: "event: error\ndata: {\"error\":{\"message\":\"chat upstream failed\",\"type\":\"api_error\"},\"type\":\"error\"}",
		},
		{
			name:     "responses client receives synthetic response.failed",
			path:     "/v1/responses",
			source:   protocol.ProtocolOpenAIResponses,
			expected: "event: response.failed\ndata: {\"type\":\"response.failed\"",
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
				Record: providercore.Record{ID: 61, Name: "chat-p1", Platform: "openai", Type: "apikey"},
			}
			resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"X-Request-Id": []string{"req-raw-61"}}}

			opts := out.ConvertedRawOptions(c, resp, prov, tc.source, "gpt-5", "gpt-4o", "gpt-4o", nil, 64)
			require.Equal(t, int64(61), opts.ProviderID)
			require.True(t, opts.CommittedSSE())

			opts.WriteError(http.StatusBadGateway, opts.DefaultErrorType, "server_error", "chat upstream failed")
			require.True(t, TerminalSSEDelivered(c))
			require.Contains(t, rec.Body.String(), tc.expected)

			foErr := opts.TruncatedFailover(openai.ErrOpenAIUpstreamStreamTruncated)
			var typedFailover *forwardcore.UpstreamFailoverError
			require.True(t, errors.As(foErr, &typedFailover))
			require.Equal(t, http.StatusBadGateway, typedFailover.StatusCode)
			require.Equal(t, "req-raw-61", http.Header(typedFailover.ResponseHeaders).Get("X-Request-Id"))
		})
	}
}

// TestOpenAITextExecutor_ViaRawChat_IRConversionEndToEnd executes both
// MessagesViaRawChat and ResponsesViaRawChat on OpenAITextExecutor. Both
// routes must capture the pre-conversion upstream response model, seal
// trc1:plain carriers, and deliver converted client SSE frames.
func TestOpenAITextExecutor_ViaRawChat_IRConversionEndToEnd(t *testing.T) {
	gin.SetMode(gin.TestMode)

	sseStream := strings.Join([]string{
		`data: {"id":"chatcmpl_e2e","object":"chat.completion.chunk","created":1760002000,"model":"deepseek-v3.2","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"Think step 1.","content":"Answer ok."},"finish_reason":"stop"}]}`,
		"",
		`data: {"id":"chatcmpl_e2e","object":"chat.completion.chunk","created":1760002000,"model":"deepseek-v3.2","choices":[],"usage":{"prompt_tokens":14,"completion_tokens":6,"total_tokens":20}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")

	t.Run("messages_via_raw_chat_ir", func(t *testing.T) {
		msgBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"hello"}]}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(msgBody))
		c.Request.Header.Set("Content-Type", "application/json")

		transport := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_msg_ir"}},
			Body:       io.NopCloser(strings.NewReader(sseStream)),
		}}
		svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: transport})

		got, err := svc.Text.MessagesViaRawChat(context.Background(), c, rawChatCompletionsTestProvider(), msgBody, "claude-sonnet-4-5")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, "deepseek-v3.2", got.UpstreamResponseModel)
		require.Equal(t, 14, got.Usage.InputTokens)
		require.Equal(t, 6, got.Usage.OutputTokens)
		require.Contains(t, rec.Body.String(), `"trc1:plain:`)
		require.Contains(t, rec.Body.String(), "Answer ok.")
	})

	t.Run("responses_via_raw_chat_ir", func(t *testing.T) {
		respBody := []byte(`{"model":"gpt-5.4","stream":true,"input":"hello"}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(respBody))
		c.Request.Header.Set("Content-Type", "application/json")

		transport := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_resp_ir"}},
			Body:       io.NopCloser(strings.NewReader(sseStream)),
		}}
		svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: transport})

		got, err := svc.Text.ResponsesViaRawChat(context.Background(), c, rawChatCompletionsTestProvider(), respBody)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, "deepseek-v3.2", got.UpstreamResponseModel)
		require.Equal(t, 14, got.Usage.InputTokens)
		require.Equal(t, 6, got.Usage.OutputTokens)
		require.Contains(t, rec.Body.String(), `"trc1:plain:`)
		require.Contains(t, rec.Body.String(), "Answer ok.")
	})
}

// TestConvertedRawOptions_EvaluatesStreamHealthOnceAndSuppressesDefault429
// feeds Chat SSE error frames through ReadRawChatConvertedStreaming with
// ConvertedRawOptions across both pre-output and post-output paths. Provider
// health policy must evaluate at most once per stream error while suppressing
// unconfigured default 429 rate-limit cooldowns.
func TestConvertedRawOptions_EvaluatesStreamHealthOnceAndSuppressesDefault429(t *testing.T) {
	gin.SetMode(gin.TestMode)

	newExchange := func(t *testing.T) *convert.Exchange {
		t.Helper()
		ex, err := convert.NewExchange(
			[]byte(`{"model":"gpt-5.5","stream":true,"input":"ping"}`),
			convert.Options{
				Route: convert.Route{
					Client:   protocol.ProtocolOpenAIResponses,
					Upstream: protocol.ProtocolOpenAIChatCompletions,
				},
				ClientModel: "gpt-5.5",
			},
		)
		require.NoError(t, err)
		return ex
	}

	t.Run("transient_502_records_single_streak_per_stream_error", func(t *testing.T) {
		for _, tc := range []struct {
			name    string // subtest label
			sseBody string // upstream Chat SSE frames
		}{
			{
				name: "output_not_started_non_failover",
				sseBody: "data: {\"error\":{\"type\":\"invalid_request_error\",\"code\":\"invalid_value\",\"status_code\":502,\"message\":\"temporary upstream failure\"}}\n\n" +
					"data: [DONE]\n\n",
			},
			{
				name: "output_started_midstream",
				sseBody: "data: {\"id\":\"chatcmpl_h_1\",\"object\":\"chat.completion.chunk\",\"created\":1760003000,\"model\":\"gpt-5.5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"partial\"},\"finish_reason\":null}]}\n\n" +
					"data: {\"error\":{\"type\":\"server_error\",\"status_code\":502,\"message\":\"temporary upstream failure\"}}\n\n",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				svc := newWSFixture(wsFixtureInputs{})
				setWSFixtureHealth(svc, newUpstreamHealthForTest(transientCooldownProviderRepo{}, &wsFixtureOptions{}, nil, providercore.HealthOptions{}, nil))
				prov := &gatewayprovider.ExecutionProvider{
					Record: providercore.Record{
						LoadLocation: time.LoadLocation,
						ID:           5201,
						Platform:     capability.PlatformOpenAI,
						Type:         capability.ProviderTypeAPIKey,
					},
				}

				runOnce := func() {
					rec := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(rec)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
					resp := &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
						Body:       io.NopCloser(strings.NewReader(tc.sseBody)),
					}
					opts := svc.Output.ConvertedRawOptions(c, resp, prov, protocol.ProtocolOpenAIResponses, "gpt-5.5", "gpt-5.5", "gpt-5.5", nil, 48)
					_, err := openai.ReadRawChatConvertedStreaming(
						upstreamcore.NewDeferredOutputContext(ResponseSink{Writer: c.Writer}),
						resp,
						newExchange(t),
						opts,
						"gpt-5.5",
						"gpt-5.5",
						nil,
						time.Now(),
					)
					require.Error(t, err)
				}

				runOnce()
				require.False(t, wsFixtureModelBlocked(svc, prov, "gpt-5.5"), "first transient stream error must only record streak=1")

				runOnce()
				require.True(t, wsFixtureModelBlocked(svc, prov, "gpt-5.5"), "second transient stream error must trigger model cooldown")
			})
		}
	})

	t.Run("stream_429_suppresses_default_rate_limit_cooldown", func(t *testing.T) {
		for _, tc := range []struct {
			name    string // subtest label
			sseBody string // upstream Chat SSE frames
		}{
			{
				name:    "output_not_started_non_failover_429",
				sseBody: "data: {\"error\":{\"type\":\"rate_limit_error\",\"code\":\"rate_limit_exceeded\",\"message\":\"Rate limit reached\"}}\n\n",
			},
			{
				name: "output_started_midstream_429",
				sseBody: "data: {\"id\":\"chatcmpl_rl\",\"object\":\"chat.completion.chunk\",\"created\":1760003010,\"model\":\"gpt-5.5\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"partial\"},\"finish_reason\":null}]}\n\n" +
					"data: {\"error\":{\"type\":\"rate_limit_error\",\"code\":\"rate_limit_exceeded\",\"message\":\"Rate limit reached\"}}\n\n",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rateRepo := &rateLimit429ProviderRepoStub{}
				svc := newWSFixture(wsFixtureInputs{
					health: newUpstreamHealthForTest(rateRepo, nil, nil, providercore.HealthOptions{}, nil),
				})
				prov := &gatewayprovider.ExecutionProvider{
					Record: providercore.Record{
						LoadLocation: time.LoadLocation,
						ID:           5202,
						Platform:     capability.PlatformOpenAI,
						Type:         capability.ProviderTypeAPIKey,
					},
				}
				// Restrict custom error handling to 503 so pre-output 429 declines failover
				// and exercises the non-failover delivery path.
				require.NoError(t, json.Unmarshal([]byte(`{"custom_error_codes_enabled":true,"custom_error_codes":[503]}`), &prov.Record.Credentials))

				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				resp := &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(tc.sseBody)),
				}
				opts := svc.Output.ConvertedRawOptions(c, resp, prov, protocol.ProtocolOpenAIResponses, "gpt-5.5", "gpt-5.5", "gpt-5.5", nil, 48)
				_, err := openai.ReadRawChatConvertedStreaming(
					upstreamcore.NewDeferredOutputContext(ResponseSink{Writer: c.Writer}),
					resp,
					newExchange(t),
					opts,
					"gpt-5.5",
					"gpt-5.5",
					nil,
					time.Now(),
				)
				require.Error(t, err)
				require.Zero(t, rateRepo.rateLimitCalls, "HTTP 200 SSE 429 without reset headers must not write default cooldown")
			})
		}
	})
}
