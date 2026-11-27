package httpapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/apikey"
	billingcore "github.com/TokenFlux/TokenRouter/internal/billing"
	"github.com/TokenFlux/TokenRouter/internal/egress"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	gatewaytestkit "github.com/TokenFlux/TokenRouter/internal/gateway/testkit"
	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	upstreamcore "github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/grok"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// Messages callers retain their client model while selected providers may
// consume native Anthropic, converted Responses or forced Chat requests. These
// fixtures compare prepared model and effort fields with returned attribution
// and translated Messages output. Cache keys and saved response IDs receive
// separate checks because changing a conversation prefix can alter replay while
// metadata identity stays stable.
//
// A terminal Responses event can finish Messages conversion even when its
// source body never supplies EOF. Blocking bodies separate that decision from
// executor-owned closure, and failed client writers expose continued usage
// collection. Other scripted failures inspect local continuation recovery and
// typed failover; usage and billing-model assertions observe forwarding
// metadata.

// openAICompatFailingWriter limits delegated byte writes to simulate client
// disconnection.
type openAICompatFailingWriter struct {
	// ResponseWriter supplies the remaining Gin operations while Write injects
	// the disconnection used by the downstream-draining scenarios in this file.
	gin.ResponseWriter
	// failAfter and writes govern delegated byte writes. Write increments writes
	// before calling the embedded writer and rejects when that count has reached
	// failAfter, so the counter measures delegation rather than successful delivery.
	failAfter int // delegated write-call limit
	writes    int // delegated write-call count
}

// errTailReader supplies fixture bytes before returning a configured terminal
// read error.
type errTailReader struct {
	// data, off and err describe sequential fixture reading. Read copies data
	// from off and advances it by the copied count before later calls return err;
	// callers retain data while reading and choose a terminating error.
	data []byte // upstream fixture bytes
	off  int    // next unread byte offset
	err  error  // configured terminal read error
}

// tempUnschedulableOpenAIProviderRepo records model-scoped health writes over
// a provider fixture.
type tempUnschedulableOpenAIProviderRepo struct {
	// stubOpenAIProviderRepo supplies the other provider methods while this
	// recorder overrides model-limit writes to expose their provider and scope.
	stubOpenAIProviderRepo
	// modelRateLimitProviderID and modelRateLimitKey record the latest fixture
	// callback together. They remain zero and empty until SetModelRateLimit
	// receives a model-scoped provider-health observation.
	modelRateLimitProviderID int64  // last model-limit provider identifier
	modelRateLimitKey        string // last model-limit scope key
}

// TestAdaptiveProtocolRoutesChatCompletionsToNativeChat captures standard Chat
// input for an adaptive Zhipu target. It checks the configured Chat URL,
// messages array and absence of Responses input before fixture transport
// failure.
func TestAdaptiveProtocolRoutesChatCompletionsToNativeChat(t *testing.T) {
	body := []byte(`{"model":"glm-4.7","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	upstream := &auxiliaryHTTPRecorder{err: errors.New("stop after capture")}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	provider := adaptiveProtocolTestProvider(capability.PlatformZhipu, map[string]any{
		providercore.APIProtocolChatCompletions: "http://chat.example",
		providercore.APIProtocolAnthropic:       "http://anthropic.example",
	})

	_, err := svc.Text.Chat(context.Background(), adaptiveProtocolTestContext("/v1/chat/completions", body), provider, body, "", "")
	require.Error(t, err)
	require.Equal(t, "http://chat.example/v1/chat/completions", upstream.lastReq.URL.String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "messages").IsArray())
	require.False(t, gjson.GetBytes(upstream.lastBody, "input").Exists())
}

// TestAdaptiveProtocolRoutesResponsesShapedChatToNativeResponses captures
// Responses-shaped input at the Chat entry for adaptive DeepSeek. It checks
// the native Responses URL and retained input shape before fixture transport
// failure.
func TestAdaptiveProtocolRoutesResponsesShapedChatToNativeResponses(t *testing.T) {
	body := []byte(`{"model":"deepseek-v4","input":"hello","max_output_tokens":32,"stream":false}`)
	upstream := &auxiliaryHTTPRecorder{err: errors.New("stop after capture")}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	provider := adaptiveProtocolTestProvider(capability.PlatformDeepseek, map[string]any{
		providercore.APIProtocolChatCompletions: "http://chat.example",
		providercore.APIProtocolAnthropic:       "http://anthropic.example",
		providercore.APIProtocolResponses:       "http://responses.example",
	})

	_, err := svc.Text.Chat(context.Background(), adaptiveProtocolTestContext("/v1/chat/completions", body), provider, body, "", "")
	require.Error(t, err)
	require.Equal(t, "http://responses.example/responses", upstream.lastReq.URL.String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "input").Exists())
	require.False(t, gjson.GetBytes(upstream.lastBody, "messages").Exists())
}

// TestAdaptiveProtocolConvertsResponsesShapedChatForChatOnlyProvider supplies
// Responses-shaped input to adaptive Zhipu without native Responses. It checks
// the Chat URL, converted messages and removed input field before transport
// failure.
func TestAdaptiveProtocolConvertsResponsesShapedChatForChatOnlyProvider(t *testing.T) {
	body := []byte(`{"model":"glm-4.7","input":"hello","max_output_tokens":32,"stream":false}`)
	upstream := &auxiliaryHTTPRecorder{err: errors.New("stop after capture")}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	provider := adaptiveProtocolTestProvider(capability.PlatformZhipu, map[string]any{
		providercore.APIProtocolChatCompletions: "http://chat.example",
		providercore.APIProtocolAnthropic:       "http://anthropic.example",
	})

	_, err := svc.Text.Chat(context.Background(), adaptiveProtocolTestContext("/v1/chat/completions", body), provider, body, "", "")
	require.Error(t, err)
	require.Equal(t, "http://chat.example/v1/chat/completions", upstream.lastReq.URL.String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "messages").IsArray())
	require.False(t, gjson.GetBytes(upstream.lastBody, "input").Exists())
}

// TestAdaptiveProtocolRoutesKimiResponsesShapedChatToNativeResponses supplies
// Responses-shaped Chat-entry input to adaptive Kimi. It checks the configured
// versioned Responses URL and retained input shape before fixture transport
// failure.
func TestAdaptiveProtocolRoutesKimiResponsesShapedChatToNativeResponses(t *testing.T) {
	body := []byte(`{"model":"k3-256k","input":"hello","max_output_tokens":32,"stream":false}`)
	upstream := &auxiliaryHTTPRecorder{err: errors.New("stop after capture")}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	provider := adaptiveProtocolTestProvider(capability.PlatformKimi, map[string]any{
		providercore.APIProtocolChatCompletions: "http://chat.example",
		providercore.APIProtocolAnthropic:       "http://anthropic.example",
		providercore.APIProtocolResponses:       "http://responses.example/v1",
	})

	_, err := svc.Text.Chat(context.Background(), adaptiveProtocolTestContext("/v1/chat/completions", body), provider, body, "", "")
	require.Error(t, err)
	require.Equal(t, "http://responses.example/v1/responses", upstream.lastReq.URL.String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "input").Exists())
	require.False(t, gjson.GetBytes(upstream.lastBody, "messages").Exists())
}

// TestForwardAsChatCompletions_ServiceTierFastNormalizedToPriorityUpstream
// captures a fast alias before an upstream HTTP 400. It checks the outgoing
// priority tier and the expected rejection after capture.
func TestForwardAsChatCompletions_ServiceTierFastNormalizedToPriorityUpstream(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}],"service_tier":"fast","stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid-chat-st"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"stop"}}`)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{}, transport: upstream, readers: newHTTPReadersFixture(&gatewaytestkit.FastPolicySettingsRepo{Values: map[string]string{}}, nil)})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 21,
			Name:        "openai-compatible",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-compatible"},
			Extra:       map[string]any{},
		},
	}

	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.5")
	require.Error(t, err) // The upstream returns 400 after the recorder captures the request.
	require.NotNil(t, upstream.lastBody)
	require.Equal(t, "priority", gjson.GetBytes(upstream.lastBody, "service_tier").String(),
		"client alias fast must reach upstream as priority")
}

// TestForwardAsChatCompletions_ServiceTierPriorityPreservedUpstream captures a
// priority request before an upstream HTTP 400. It checks the outgoing
// priority value and the expected fixture rejection after request
// construction.
func TestForwardAsChatCompletions_ServiceTierPriorityPreservedUpstream(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}],"service_tier":"priority","stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid-chat-st2"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"stop"}}`)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{}, transport: upstream, readers: newHTTPReadersFixture(&gatewaytestkit.FastPolicySettingsRepo{Values: map[string]string{}}, nil)})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 2,
			Name:        "openai-compatible",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-compatible"},
			Extra:       map[string]any{},
		},
	}

	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.5")
	require.Error(t, err)
	require.NotNil(t, upstream.lastBody)
	require.Equal(t, "priority", gjson.GetBytes(upstream.lastBody, "service_tier").String())
}

// TestForwardAsChatCompletions_KeepsOutboundAndObservedServiceTiersSeparate
// sends fast while the terminal Response declares default. It checks requested
// priority in the result, observed default attribution and default in buffered
// client output.
func TestForwardAsChatCompletions_KeepsOutboundAndObservedServiceTiersSeparate(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}],"service_tier":"fast","stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	streamPayload := "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_c1\",\"object\":\"response\",\"model\":\"gpt-5.5\",\"status\":\"in_progress\"}}\n\n" +
		"data: {\"type\":\"response.output_text.delta\",\"item_id\":\"it_1\",\"output_index\":0,\"delta\":\"hi\"}\n\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_c1\",\"object\":\"response\",\"model\":\"gpt-5.5\",\"status\":\"completed\",\"service_tier\":\"default\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n" +
		"data: [DONE]\n\n"

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid-chat-echo"}},
		Body:       io.NopCloser(strings.NewReader(streamPayload)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}, transport: upstream, readers: newHTTPReadersFixture(&gatewaytestkit.FastPolicySettingsRepo{Values: map[string]string{}}, nil)})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 21,
			Name:        "openai-compatible",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-compatible"},
			Extra:       map[string]any{},
			Status:      billingcore.StatusActive,
			Schedulable: true,
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.ServiceTier)
	require.Equal(t, "priority", *result.ServiceTier)
	require.Equal(t, "default", result.UpstreamResponseServiceTier)
	// Buffered Chat conversion reports the observed default tier to the client.
	require.Contains(t, rec.Body.String(), `"service_tier":"default"`)
	require.NotContains(t, rec.Body.String(), `"service_tier":"priority"`)
}

// TestOpenAIAgentIdentityChatRecoveryKeepsAutoDerivedSessionIsolationStable
// returns invalid-task errors around a local registration exchange. It checks
// two captured requests with the same derived cache key and isolated session
// header after recovery.
func TestOpenAIAgentIdentityChatRecoveryKeepsAutoDerivedSessionIsolationStable(t *testing.T) {
	key, privateKey := newTestAgentIdentityKey(t)
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 52, Name: "agent-identity", Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeOAuth,
			Status: billingcore.StatusActive, Schedulable: true, Concurrency: 1,
			Credentials: map[string]any{
				"auth_mode":         providercore.OpenAIAuthModeAgentIdentity,
				"agent_runtime_id":  key.RuntimeID,
				"agent_private_key": privateKey,
				"task_id":           "task-cache-old",
			},
			Extra: map[string]any{},
		},
	}
	repo := &agentIdentityForwardRepo{provider: provider}
	// A local registration endpoint returns the replacement task identity; close
	// it after both captured attempts so recovery uses only this fixture exchange.
	registerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// Discard handler write errors in this fixture. Later assertions inspect
		// captured retry identities, rather than propagation of a registration
		// response write failure.
		_, _ = io.WriteString(w, `{"task_id":"task-cache-new"}`)
	}))
	defer registerServer.Close()

	invalidTask := func() *http.Response {
		return &http.Response{StatusCode: http.StatusUnauthorized, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_task_id"}}`))}
	}
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{invalidTask(), invalidTask()}}
	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{}, providers: repo, registerTaskURL: registerServer.URL, transport: upstream})
	body := []byte(`{"model":"gpt-5.4","stream":false,"messages":[{"role":"user","content":"hi"}]}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 99})

	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.4")
	require.Error(t, err)
	require.Len(t, upstream.requests, 2)
	firstKey := gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").String()
	secondKey := gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").String()
	require.NotEmpty(t, firstKey)
	require.Equal(t, firstKey, secondKey)
	require.Equal(t, upstreamcore.GenerateSessionUUID(openai.IsolateOpenAIUpstreamSessionID(99, provideradapter.CodexIdentityNamespace(CodexIdentityRecord(c, provider.View())), firstKey)), upstream.requests[0].Header.Get("session_id"))
	require.Equal(t, upstream.requests[0].Header.Get("session_id"), upstream.requests[1].Header.Get("session_id"))
}

// TestForwardAsChatCompletions_OpenAICompatibleRawUsageGuard enumerates native
// Chat model and usage shapes, including mapped aliases. It checks successful
// forwarding for admitted fixtures and pre-output missing-Grok-usage recovery
// with request and Ops identity for guarded fixtures.
func TestForwardAsChatCompletions_OpenAICompatibleRawUsageGuard(t *testing.T) {
	tests := []struct {
		name             string // fixture case name
		model            string // client request model
		upstreamResponse string // upstream JSON response
		// optional provider model mapping
		modelMapping map[string]any
		wantGuarded  bool // expected missing-usage rejection
	}{
		{
			name:             "Grok response without usage",
			model:            "grok-4.5",
			upstreamResponse: `{"id":"resp_missing","object":"chat.completion","model":"grok-4.5","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}`,
			wantGuarded:      true,
		},
		{
			name:             "namespaced Grok response without usage",
			model:            "x-ai/grok-4.5",
			upstreamResponse: `{"id":"resp_namespaced","object":"chat.completion","model":"x-ai/grok-4.5","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}`,
			wantGuarded:      true,
		},
		{
			name:             "Grok response with aggregate usage passes",
			model:            "grok-4.5",
			upstreamResponse: `{"id":"resp_usage","object":"chat.completion","model":"grok-4.5","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":9,"completion_tokens":3,"total_tokens":12}}`,
			wantGuarded:      false,
		},
		{
			name:             "Grok alias mapped to non-Grok remains unchanged",
			model:            "grok-alias",
			upstreamResponse: `{"id":"resp_mapped","object":"chat.completion","model":"gpt-5.4","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}`,
			modelMapping:     map[string]any{"grok-alias": "gpt-5.4"},
			wantGuarded:      false,
		},
		{
			name:             "Grok response with detail-only usage",
			model:            "grok-4.5",
			upstreamResponse: `{"id":"resp_detail_only","object":"chat.completion","model":"grok-4.5","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"input_tokens_details":{"text_tokens":9,"image_tokens":2},"output_tokens_details":{"image_tokens":1}}}`,
			wantGuarded:      true,
		},
		{
			name:             "non-Grok response without usage remains unchanged",
			model:            "gpt-5.4",
			upstreamResponse: `{"id":"resp_openai","object":"chat.completion","model":"gpt-5.4","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}]}`,
			wantGuarded:      false,
		},
	}

	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			body := []byte(`{"model":"` + testCase.model + `","messages":[{"role":"user","content":"hello"}],"stream":false}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

			upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Request-Id": []string{"rid-openai-compatible"}},
				Body:       io.NopCloser(strings.NewReader(testCase.upstreamResponse)),
			}}
			service := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
			provider := rawChatCompletionsTestProvider()
			provider.Record.Name = "openai-compatible"
			provider.Record.Extra = map[string]any{"openai_responses_probe_status": "unsupported"}
			if testCase.modelMapping != nil {
				provider.Record.Credentials["model_mapping"] = testCase.modelMapping
			}

			result, err := service.Text.Chat(context.Background(), c, provider, body, "", "")

			if !testCase.wantGuarded {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.True(t, c.Writer.Written())
				return
			}
			require.Nil(t, result)
			var failoverErr *forwardcore.UpstreamFailoverError
			require.ErrorAs(t, err, &failoverErr)
			require.Equal(t, http.StatusBadGateway, failoverErr.StatusCode)
			require.Equal(t, "grok_missing_usage", gjson.GetBytes(failoverErr.ResponseBody, "error.code").String())
			require.Equal(t, "rid-openai-compatible", http.Header(failoverErr.ResponseHeaders).Get("x-request-id"))
			require.False(t, c.Writer.Written())
			require.Empty(t, recorder.Body.String())
			rawEvents, ok := c.Get(OpsUpstreamErrorsKey)
			require.True(t, ok)
			events, ok := rawEvents.([]*ops.OpsUpstreamErrorEvent)
			require.True(t, ok)
			require.NotEmpty(t, events)
			require.Equal(t, capability.PlatformGrok, events[len(events)-1].Platform)
		})
	}
}

// TestForwardAsChatCompletions_PreserveClientProtocolUsesVersionedChatURL
// forwards native Chat to a versioned configured base. It checks URL joining,
// TLS-selected User-Agent, request shape, observed usage and buffered answer
// output.
func TestForwardAsChatCompletions_PreserveClientProtocolUsesVersionedChatURL(t *testing.T) {
	body := []byte(`{"model":"glm-4.5-air","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_raw_chat"}},
			Body: io.NopCloser(strings.NewReader(
				`{"id":"chatcmpl_1","object":"chat.completion","model":"glm-4.5-air","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`,
			)),
		},
	}}

	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	provider := rawChatCompletionsTestProvider()
	provider.Record.Credentials["base_url"] = "https://open.bigmodel.cn/api/paas/v4"

	tlsMatch := egress.TLSFingerprintRouterMatchResult{Matched: true, UpstreamUserAgent: "router-agent"}
	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "", tlsMatch)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	require.Equal(t, "ok", gjson.GetBytes(rec.Body.Bytes(), "choices.0.message.content").String())
	require.Len(t, upstream.requests, 1)
	require.Equal(t, "https://open.bigmodel.cn/api/paas/v4/chat/completions", upstream.requests[0].URL.String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "messages").Exists())
	require.False(t, gjson.GetBytes(upstream.lastBody, "input").Exists())
	require.Equal(t, "router-agent", upstream.requests[0].Header.Get("User-Agent"))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"content":"ok"`)
}

// TestForwardAsChatCompletions_UnknownModelWithoutMessagesDispatchKeepsRequestedModel
// captures an unknown OAuth model before model-not-found rejection. It checks
// the requested model remains outgoing and the client receives the fixture
// HTTP 400.
func TestForwardAsChatCompletions_UnknownModelWithoutMessagesDispatchKeepsRequestedModel(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt6","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_chat_unknown_model"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"model not found"}}`)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, "gpt6", gjson.GetBytes(upstream.lastBody, "model").String())
	require.NotEqual(t, "gpt-5.4", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestForwardAsChatCompletions_APIKeyPropagatesPromptCacheKeyInResponsesBody
// forces API-key Chat through Responses with an explicit cache seed. It checks
// outgoing key/model, endpoint observation, authorization and isolated session
// header before rejection.
func TestForwardAsChatCompletions_APIKeyPropagatesPromptCacheKeyInResponsesBody(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("api_key", &apikey.APIKey{ID: 99})
	SetActualOpenAIUpstreamEndpoint(c, "/v1/chat/completions")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_chat_prompt_cache"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"stop before response parsing"}}`)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 2,
			Name:        "openai-compatible",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key": "sk-compatible",
			},
			Extra: map[string]any{
				"openai_text_route_mode": "force_responses",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "cache-key-123", "gpt-5.4")
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, "cache-key-123", gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String())
	require.Equal(t, "gpt-5.4", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, "https://api.openai.com/v1/responses", upstream.lastReq.URL.String())
	require.Equal(t, "/v1/responses", GetActualOpenAIUpstreamEndpoint(c))
	require.Equal(t, "Bearer sk-compatible", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, upstreamcore.GenerateSessionUUID(upstreamcore.IsolateSessionID(99, "cache-key-123")), upstream.lastReq.Header.Get("session_id"))
}

// TestForwardAsChatCompletions_APIKeyResponsesRecordsThirdPartyMaxEffort
// forces DeepSeek Chat input with max effort through Responses. It checks the
// endpoint and outgoing reasoning control plus returned max attribution.
func TestForwardAsChatCompletions_APIKeyResponsesRecordsThirdPartyMaxEffort(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hello"}],"reasoning_effort":"max","stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_deepseek_max","object":"response","model":"deepseek-v4-flash","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 23,
			Name:        "deepseek-compatible",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-compatible"},
			Extra: map[string]any{
				"openai_text_route_mode": "force_responses",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "https://api.openai.com/v1/responses", upstream.lastReq.URL.String())
	require.Equal(t, "max", gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
	require.NotNil(t, result.ReasoningEffort)
	require.Equal(t, "max", *result.ReasoningEffort)
}

// TestForwardAsChatCompletions_APIKeyResponsesDoesNotRecordDroppedNestedEffort
// supplies nested reasoning.effort to Chat conversion. It checks absence of
// outgoing reasoning and returned effort after the successful fixture
// response.
func TestForwardAsChatCompletions_APIKeyResponsesDoesNotRecordDroppedNestedEffort(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	// Supply nested effort that Chat conversion discards, then inspect outgoing
	// controls and result attribution to catch accidental restoration from client bytes.
	body := []byte(`{"model":"deepseek-v4-flash","messages":[{"role":"user","content":"hello"}],"reasoning":{"effort":"max"},"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_deepseek_default","object":"response","model":"deepseek-v4-flash","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 23,
			Name:        "deepseek-compatible",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-compatible"},
			Extra: map[string]any{
				"openai_text_route_mode": "force_responses",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "max", gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
	require.NotNil(t, result.ReasoningEffort)
	require.Equal(t, "max", *result.ReasoningEffort)
}

// TestForwardAsChatCompletions_TransportErrorFailsOver injects EOF after one
// captured Responses request. It checks absent result, a typed 502 recovery
// error and no hard client body before outer retry handling.
func TestForwardAsChatCompletions_TransportErrorFailsOver(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{
		err: errors.New(`Post "https://api.openai.com/v1/responses": EOF`),
	}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 22,
			Name:        "openai-compatible",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key": "sk-compatible",
			},
			Extra: map[string]any{
				"openai_text_route_mode": "force_responses",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.4")

	require.Nil(t, result)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr), "transport error must trigger provider failover")
	require.Equal(t, http.StatusBadGateway, failoverErr.StatusCode)
	require.Len(t, upstream.requests, 1)
	require.Equal(t, 0, rec.Body.Len(), "service must not write a hard 502 before handler can fail over")
}

// TestForwardAsChatCompletions_APIKeyAutoDerivesStableIsolatedPromptCacheKey
// compares appended history on one key with the same history on another. It
// checks stable first-key seeds and session headers, plus distinct other-key
// values.
func TestForwardAsChatCompletions_APIKeyAutoDerivesStableIsolatedPromptCacheKey(t *testing.T) {
	response := func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"stop before response parsing"}}`)),
		}
	}
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{response(), response(), response()}}
	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{}, transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 2, Name: "openai-compatible", Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey, Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-compatible"},
			Extra: map[string]any{
				"openai_text_route_mode": "force_responses",
			},
		},
	}
	firstBody := []byte(`{"model":"gpt-5.4","messages":[{"role":"system","content":"be concise"},{"role":"user","content":"hello"}],"stream":false}`)
	appendedBody := []byte(`{"model":"gpt-5.4","messages":[{"role":"system","content":"be concise"},{"role":"user","content":"hello"},{"role":"assistant","content":"hi"},{"role":"user","content":"continue"}],"stream":false}`)

	forward := func(apiKeyID int64, body []byte) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")
		c.Set("api_key", &apikey.APIKey{ID: apiKeyID})
		result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.4")
		require.Error(t, err)
		require.Nil(t, result)
	}

	forward(99, firstBody)
	forward(99, appendedBody)
	forward(100, appendedBody)

	firstKey := gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").String()
	appendedKey := gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").String()
	otherTenantKey := gjson.GetBytes(upstream.bodies[2], "prompt_cache_key").String()
	require.NotEmpty(t, firstKey)
	require.Equal(t, firstKey, appendedKey)
	require.NotEqual(t, firstKey, otherTenantKey)
	require.Equal(t, upstreamcore.GenerateSessionUUID(firstKey), upstream.requests[0].Header.Get("session_id"))
	require.Equal(t, upstream.requests[0].Header.Get("session_id"), upstream.requests[1].Header.Get("session_id"))
	require.Equal(t, upstreamcore.GenerateSessionUUID(otherTenantKey), upstream.requests[2].Header.Get("session_id"))
	require.NotEqual(t, upstream.requests[1].Header.Get("session_id"), upstream.requests[2].Header.Get("session_id"))
}

// TestForwardAsChatCompletions_ResponsesShapeDoesNotAutoDerivePromptCacheKey
// compares unrelated Responses-shaped bodies and one explicit cache seed. It
// checks absent automatic keys/headers and isolated propagation of the
// explicit seed.
func TestForwardAsChatCompletions_ResponsesShapeDoesNotAutoDerivePromptCacheKey(t *testing.T) {
	response := func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"stop before response parsing"}}`)),
		}
	}
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{response(), response(), response()}}
	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{}, transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 2, Name: "openai-compatible", Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey, Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-compatible"},
			Extra: map[string]any{
				"openai_text_route_mode": "force_responses",
			},
		},
	}
	firstBody := []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":[{"type":"input_text","text":"first unrelated input"}]}],"stream":false}`)
	secondBody := []byte(`{"model":"gpt-5.4","input":[{"role":"user","content":[{"type":"input_text","text":"second unrelated input"}]}],"stream":false}`)

	forward := func(body []byte, promptCacheKey string) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
		c.Set("api_key", &apikey.APIKey{ID: 99})
		result, err := svc.Text.Chat(context.Background(), c, provider, body, promptCacheKey, "gpt-5.4")
		require.Error(t, err)
		require.Nil(t, result)
	}

	forward(firstBody, "")
	forward(secondBody, "")
	forward(secondBody, "explicit-responses-key")

	require.False(t, gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").Exists())
	require.Empty(t, upstream.requests[0].Header.Get("session_id"))
	require.False(t, gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").Exists())
	require.Empty(t, upstream.requests[1].Header.Get("session_id"))
	require.Equal(t, "explicit-responses-key", gjson.GetBytes(upstream.bodies[2], "prompt_cache_key").String())
	require.Equal(t, upstreamcore.GenerateSessionUUID(upstreamcore.IsolateSessionID(99, "explicit-responses-key")), upstream.requests[2].Header.Get("session_id"))
}

// TestForwardAsChatCompletions_OAuthDoesNotInjectDefaultInstructions captures
// an OAuth Chat request without system content. It checks the Codex target,
// present empty instructions and absence of the listed default instruction
// text.
func TestForwardAsChatCompletions_OAuthDoesNotInjectDefaultInstructions(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_chat_no_default_instructions"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"stop before response parsing"}}`)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{}, transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 3,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.4")
	require.Error(t, err)
	require.Nil(t, result)
	require.NotNil(t, upstream.lastReq)
	require.Equal(t, ChatgptCodexURL, upstream.lastReq.URL.String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "instructions").Exists())
	require.Equal(t, "", gjson.GetBytes(upstream.lastBody, "instructions").String())
	require.NotContains(t, string(upstream.lastBody), "Communicate with the user by streaming thinking")
}

// forwardOAuthChatCompletionsForUpstreamBody captures one OAuth request before
// a deliberate HTTP 400. It asserts the expected error, absent result and
// nonempty body, then returns the recorder-owned outgoing bytes for focused
// request assertions.
func forwardOAuthChatCompletionsForUpstreamBody(t *testing.T, body []byte) []byte {
	t.Helper()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_chat_system_promotion"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"stop before response parsing"}}`)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{}, transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 4,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.4")
	require.Error(t, err)
	require.Nil(t, result)
	require.NotEmpty(t, upstream.lastBody)
	return upstream.lastBody
}

// TestForwardAsChatCompletions_OAuthPromotesSystemMessageWithoutDuplication
// converts text-only system and user messages for OAuth. It checks one
// instructions occurrence and a remaining user-only input array after
// promotion.
func TestForwardAsChatCompletions_OAuthPromotesSystemMessageWithoutDuplication(t *testing.T) {
	const systemPrompt = "Unique system prefix for token accounting."
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"system","content":"` + systemPrompt + `"},{"role":"user","content":"hello"}],"stream":false}`)

	upstreamBody := forwardOAuthChatCompletionsForUpstreamBody(t, body)

	require.Equal(t, systemPrompt, gjson.GetBytes(upstreamBody, "instructions").String())
	require.Equal(t, int64(1), gjson.GetBytes(upstreamBody, "input.#").Int())
	require.Equal(t, "user", gjson.GetBytes(upstreamBody, "input.0.role").String())
	require.Equal(t, 1, strings.Count(string(upstreamBody), systemPrompt))
}

// TestForwardAsChatCompletions_OAuthJsonObjectKeepsSystemMessageInInput
// requests json_object with text-only system content. It checks instructions
// plus retained developer input and two occurrences of that fixture system
// text.
func TestForwardAsChatCompletions_OAuthJsonObjectKeepsSystemMessageInInput(t *testing.T) {
	const systemPrompt = "Return JSON only."
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"system","content":"` + systemPrompt + `"},{"role":"user","content":"symbol data"}],"response_format":{"type":"json_object"},"stream":false}`)

	upstreamBody := forwardOAuthChatCompletionsForUpstreamBody(t, body)

	require.Equal(t, systemPrompt, gjson.GetBytes(upstreamBody, "instructions").String())
	require.Equal(t, "json_object", gjson.GetBytes(upstreamBody, "text.format.type").String())
	require.Equal(t, int64(2), gjson.GetBytes(upstreamBody, "input.#").Int())
	require.Equal(t, "developer", gjson.GetBytes(upstreamBody, "input.0.role").String())
	require.Contains(t, gjson.GetBytes(upstreamBody, "input.0.content").Raw, systemPrompt)
	require.Equal(t, 2, strings.Count(string(upstreamBody), systemPrompt))
}

// TestForwardAsChatCompletions_OAuthKeepsMixedSystemContentInInput supplies
// system text and an image together. It checks promoted instructions and
// retained developer content with the converted input-image URL.
func TestForwardAsChatCompletions_OAuthKeepsMixedSystemContentInInput(t *testing.T) {
	const systemPrompt = "Inspect this reference image."
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"system","content":[{"type":"text","text":"` + systemPrompt + `"},{"type":"image_url","image_url":{"url":"https://example.com/reference.png"}}]},{"role":"user","content":"hello"}],"stream":false}`)

	upstreamBody := forwardOAuthChatCompletionsForUpstreamBody(t, body)

	require.Equal(t, systemPrompt, gjson.GetBytes(upstreamBody, "instructions").String())
	require.Equal(t, int64(2), gjson.GetBytes(upstreamBody, "input.#").Int())
	require.Equal(t, "developer", gjson.GetBytes(upstreamBody, "input.0.role").String())
	require.Equal(t, int64(2), gjson.GetBytes(upstreamBody, "input.0.content.#").Int())
	require.Equal(t, "input_image", gjson.GetBytes(upstreamBody, "input.0.content.1.type").String())
	require.Equal(t, "https://example.com/reference.png", gjson.GetBytes(upstreamBody, "input.0.content.1.image_url").String())
}

// TestForwardAsChatCompletions_ClientDisconnectDrainsUpstreamUsage rejects
// downstream writes before terminal Responses usage arrives. It checks
// successful returned observations with the fixture input, output and
// cached-token quantities after draining.
func TestForwardAsChatCompletions_ClientDisconnectDrainsUpstreamUsage(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Writer = &openAIChatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":11,"output_tokens":5,"total_tokens":16,"input_tokens_details":{"cached_tokens":4}}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_disconnect"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 11, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
	require.Equal(t, 4, result.Usage.CacheReadInputTokens)
}

// TestForwardAsChatCompletions_BufferedContextWindowResponseFailedReturnsErrorWithoutFailover
// supplies a buffered context-window failure terminal. It checks no result, no
// failover wrapper and a written JSON 502 carrying the failure message.
func TestForwardAsChatCompletions_BufferedContextWindowResponseFailedReturnsErrorWithoutFailover(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"large prompt"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`event: response.failed`,
		`data: {"type":"response.failed","response":{"id":"resp_failed","object":"response","model":"gpt-5.5","status":"failed","output":[],"error":{"code":"upstream_error","message":"input exceeds the context window"}}}`,
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_failed_buffered"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.5")
	require.Error(t, err)
	require.Nil(t, result)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.True(t, c.Writer.Written())
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Body.String(), "input exceeds the context window")
}

// TestForwardAsChatCompletions_StreamContextWindowResponseFailedReturnsErrorWithoutFailover
// supplies a pre-content context-window terminal to a streaming request. It
// checks a returned observation, JSON 502, no failover wrapper and no DONE
// marker.
func TestForwardAsChatCompletions_StreamContextWindowResponseFailedReturnsErrorWithoutFailover(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	// Cross the large-request buffering threshold so converted preamble stays
	// pending until this fixture's terminal failure classification.
	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"` + strings.Repeat("large prompt ", 6000) + `"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_failed","model":"gpt-5.5","status":"in_progress","output":[]}}`,
		"",
		`event: response.failed`,
		`data: {"type":"response.failed","response":{"id":"resp_failed","object":"response","model":"gpt-5.5","status":"failed","output":[],"error":{"code":"upstream_error","message":"input exceeds the context window"}}}`,
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_failed_stream"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.5")
	require.Error(t, err)
	require.NotNil(t, result)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.True(t, c.Writer.Written())
	require.Equal(t, http.StatusBadGateway, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.Contains(t, rec.Body.String(), "input exceeds the context window")
	require.NotContains(t, rec.Body.String(), "[DONE]")
}

// TestForwardAsChatCompletions_StreamBareErrorAfterOutputDoesNotFailOver
// follows a text delta with a bare error. It checks partial result, retained
// content and failure message, absent DONE and no provider failover wrapper.
func TestForwardAsChatCompletions_StreamBareErrorAfterOutputDoesNotFailOver(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}`,
		"",
		`event: error`,
		`data: {"type":"error","error":{"type":"server_error","code":"server_error","message":"temporary upstream failure"}}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1, Name: "openai-oauth", Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeOAuth, Concurrency: 1,
			Credentials: map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-acc"},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.5")

	require.Error(t, err)
	require.NotNil(t, result)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.Contains(t, rec.Body.String(), "partial")
	require.Contains(t, rec.Body.String(), "temporary upstream failure")
	require.NotContains(t, rec.Body.String(), "[DONE]")
}

// TestForwardAsChatCompletions_StreamCyberPolicyNoFailover supplies a
// cyber-policy failed terminal after the Responses preamble. It checks the
// request risk marker, absence of failover wrapping and the client error/code
// and DONE fields.
func TestForwardAsChatCompletions_StreamCyberPolicyNoFailover(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	// Cross the large-request buffering threshold so converted preamble stays
	// pending until this fixture's terminal failure classification.
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
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_cyber"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.5")
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr), "cyber must NOT trigger failover")
	require.NotNil(t, GetOpsCyberPolicy(c), "cyber mark must be set")
	respBody := rec.Body.String()
	require.Contains(t, respBody, `"error"`)
	require.Contains(t, respBody, `"cyber_policy"`)
	require.Contains(t, respBody, "data: [DONE]")
}

// TestForwardAsChatCompletions_StreamsUsageWithoutClientStreamOptions supplies
// terminal usage despite no client stream_options field. It checks returned
// token quantities and their converted Chat usage fields in downstream output.
func TestForwardAsChatCompletions_StreamsUsageWithoutClientStreamOptions(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":13,"output_tokens":7,"total_tokens":20,"input_tokens_details":{"cached_tokens":5}}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_usage_no_stream_options"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 13, result.Usage.InputTokens)
	require.Equal(t, 7, result.Usage.OutputTokens)
	require.Equal(t, 5, result.Usage.CacheReadInputTokens)

	responseBody := rec.Body.String()
	require.NotContains(t, responseBody, `"usage"`)
	require.Contains(t, responseBody, "data: [DONE]")
}

// TestForwardAsChatCompletions_StreamsTopLevelTerminalUsage places usage
// beside the terminal response object in SSE. It checks returned input, output
// and cache observations plus their downstream Chat usage representation.
func TestForwardAsChatCompletions_StreamsTopLevelTerminalUsage(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":true,"stream_options":{"include_usage":true}}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_top","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_top","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}]},"usage":{"input_tokens":21,"output_tokens":9,"total_tokens":30,"input_tokens_details":{"cached_tokens":4}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_top_level_usage"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 21, result.Usage.InputTokens)
	require.Equal(t, 9, result.Usage.OutputTokens)
	require.Equal(t, 4, result.Usage.CacheReadInputTokens)

	responseBody := rec.Body.String()
	require.Contains(t, responseBody, `"usage"`)
	require.Contains(t, responseBody, `"prompt_tokens":21`)
	require.Contains(t, responseBody, `"completion_tokens":9`)
	require.Contains(t, responseBody, `"cached_tokens":4`)
}

// TestForwardAsChatCompletions_BufferedTopLevelTerminalUsage places usage
// beside the terminal Response for buffered conversion. It checks the observed
// token quantities and matching Chat JSON usage fields.
func TestForwardAsChatCompletions_BufferedTopLevelTerminalUsage(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_top_buffered","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}]},"usage":{"input_tokens":18,"output_tokens":6,"total_tokens":24,"input_tokens_details":{"cached_tokens":3}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_buffered_top_level_usage"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 18, result.Usage.InputTokens)
	require.Equal(t, 6, result.Usage.OutputTokens)
	require.Equal(t, 3, result.Usage.CacheReadInputTokens)

	responseBody := rec.Body.String()
	require.Contains(t, responseBody, `"usage"`)
	require.Contains(t, responseBody, `"prompt_tokens":18`)
	require.Contains(t, responseBody, `"completion_tokens":6`)
	require.Contains(t, responseBody, `"cached_tokens":3`)
}

// TestForwardAsChatCompletions_TerminalUsageWithoutUpstreamCloseReturns blocks
// reads after a terminal and rejects client writes. It checks timely return
// and usage even though the fixture supplies no EOF before executor cleanup.
func TestForwardAsChatCompletions_TerminalUsageWithoutUpstreamCloseReturns(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Writer = &openAIChatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":17,"output_tokens":8,"total_tokens":25,"input_tokens_details":{"cached_tokens":6}}}}` + "\n\n")
	// Keep reads blocked after terminal bytes to distinguish protocol completion
	// from EOF; deferred closure releases the fixture if the bounded wait fails.
	upstreamStream := gatewaytestkit.NewBlockingReadCloser(upstreamBody)
	defer func() {
		require.NoError(t, upstreamStream.Close())
	}()
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_terminal_no_close"}},
		Body:       upstreamStream,
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	// forwardResult carries one forwarding observation and error from the worker
	// to this test.
	type forwardResult struct {
		// result and err are captured by the same forwarding worker and published
		// together in one channel value, so assertions inspect the observation
		// associated with that operation's returned failure.
		// forwarding result observation
		result *forwardcore.OpenAIResult
		err    error // forwarding operation error
	}
	// Publish through a buffered result so the worker can finish even if the
	// wait times out; deferred source closure releases any remaining blocked read.
	resultCh := make(chan forwardResult, 1)
	go func() {
		result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
		resultCh <- forwardResult{result: result, err: err}
	}()

	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.result)
		require.Equal(t, 17, got.result.Usage.InputTokens)
		require.Equal(t, 8, got.result.Usage.OutputTokens)
		require.Equal(t, 6, got.result.Usage.CacheReadInputTokens)
	case <-time.After(time.Second):
		require.Fail(t, "ForwardAsChatCompletions should return after terminal usage event even if upstream keeps the connection open")
	}
}

// TestForwardAsChatCompletions_EventNamedTerminalWithoutUpstreamCloseReturns
// supplies event names when data omits type and then blocks reads. It checks
// timely completion, returned usage and converted text without requiring EOF
// from that blocking fixture.
func TestForwardAsChatCompletions_EventNamedTerminalWithoutUpstreamCloseReturns(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(strings.Join([]string{
		`event: response.created`,
		`data: {"response":{"id":"resp_1","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		``,
		`event: response.output_text.delta`,
		`data: {"delta":"ok"}`,
		``,
		`event: response.completed`,
		`data: {"response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":17,"output_tokens":8,"total_tokens":25,"input_tokens_details":{"cached_tokens":6}}}}`,
		``,
		``,
	}, "\n"))
	// Keep reads blocked after terminal bytes to distinguish protocol completion
	// from EOF; deferred closure releases the fixture if the bounded wait fails.
	upstreamStream := gatewaytestkit.NewBlockingReadCloser(upstreamBody)
	defer func() {
		require.NoError(t, upstreamStream.Close())
	}()
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_event_named_terminal"}},
		Body:       upstreamStream,
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	// forwardResult carries one forwarding observation and error from the worker
	// to this test.
	type forwardResult struct {
		// result and err are captured by the same forwarding worker and published
		// together in one channel value, so assertions inspect the observation
		// associated with that operation's returned failure.
		// forwarding result observation
		result *forwardcore.OpenAIResult
		err    error // forwarding operation error
	}
	// Publish through a buffered result so the worker can finish even if the
	// wait times out; deferred source closure releases any remaining blocked read.
	resultCh := make(chan forwardResult, 1)
	go func() {
		result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
		resultCh <- forwardResult{result: result, err: err}
	}()

	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.result)
		require.Equal(t, 17, got.result.Usage.InputTokens)
		require.Equal(t, 8, got.result.Usage.OutputTokens)
		require.Equal(t, 6, got.result.Usage.CacheReadInputTokens)
		require.Contains(t, rec.Body.String(), `"content":"ok"`)
	case <-time.After(time.Second):
		require.Fail(t, "ForwardAsChatCompletions should use SSE event names when data payloads omit type")
	}
}

// TestForwardAsChatCompletions_EventTypeDoesNotLeakAcrossFrames mixes named
// and data-typed SSE frames across blank boundaries. It checks successful text
// forwarding and DONE after the fixture terminal.
func TestForwardAsChatCompletions_EventTypeDoesNotLeakAcrossFrames(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`event: response.created`,
		`data: {"response":{"id":"resp_1","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		``,
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		``,
		`event: response.completed`,
		`data: {"response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":17,"output_tokens":8,"total_tokens":25,"input_tokens_details":{"cached_tokens":6}}}}`,
		``,
		`data: [DONE]`,
		``,
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_event_boundary"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, rec.Body.String(), `"content":"ok"`)
	require.Contains(t, rec.Body.String(), `data: [DONE]`)
}

// TestForwardAsChatCompletions_BufferedTerminalWithoutUpstreamCloseReturns
// blocks source reads after a completed terminal for nonstreaming Chat. It
// checks timely return, observed usage and a stop finish reason in buffered
// output.
func TestForwardAsChatCompletions_BufferedTerminalWithoutUpstreamCloseReturns(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":17,"output_tokens":8,"total_tokens":25,"input_tokens_details":{"cached_tokens":6}}}}` + "\n\n")
	// Keep reads blocked after terminal bytes to distinguish protocol completion
	// from EOF; deferred closure releases the fixture if the bounded wait fails.
	upstreamStream := gatewaytestkit.NewBlockingReadCloser(upstreamBody)
	defer func() {
		require.NoError(t, upstreamStream.Close())
	}()
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_buffered_terminal_no_close"}},
		Body:       upstreamStream,
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	// forwardResult carries one forwarding observation and error from the worker
	// to this test.
	type forwardResult struct {
		// result and err are captured by the same forwarding worker and published
		// together in one channel value, so assertions inspect the observation
		// associated with that operation's returned failure.
		// forwarding result observation
		result *forwardcore.OpenAIResult
		err    error // forwarding operation error
	}
	// Publish through a buffered result so the worker can finish even if the
	// wait times out; deferred source closure releases any remaining blocked read.
	resultCh := make(chan forwardResult, 1)
	go func() {
		result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
		resultCh <- forwardResult{result: result, err: err}
	}()

	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.result)
		require.Equal(t, 17, got.result.Usage.InputTokens)
		require.Equal(t, 8, got.result.Usage.OutputTokens)
		require.Equal(t, 6, got.result.Usage.CacheReadInputTokens)
		require.Contains(t, rec.Body.String(), `"finish_reason":"stop"`)
	case <-time.After(time.Second):
		require.Fail(t, "ForwardAsChatCompletions buffered response should return after terminal usage event even if upstream keeps the connection open")
	}
}

// TestForwardAsChatCompletions_DoneSentinelWithoutTerminalReturnsError
// supplies only the DONE sentinel to converted Chat. It checks a
// missing-terminal error with a returned observation whose input and output
// counts are zero.
func TestForwardAsChatCompletions_DoneSentinelWithoutTerminalReturnsError(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := "data: [DONE]\n\n"
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_missing_terminal"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing terminal event")
	require.NotNil(t, result)
	require.Zero(t, result.Usage.InputTokens)
	require.Zero(t, result.Usage.OutputTokens)
}

// TestForwardAsChatCompletions_UpstreamRequestIgnoresClientCancel cancels the
// client before converted forwarding and supplies a completed fixture
// response. It checks success and an uncancelled context on the captured
// request.
func TestForwardAsChatCompletions_UpstreamRequestIgnoresClientCancel(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	reqCtx, cancel := context.WithCancel(context.Background())
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body)).WithContext(reqCtx)
	c.Request.Header.Set("Content-Type", "application/json")
	// Cancel before conversion to inspect detachment on the captured upstream
	// request. The fixture supplies a completed response independently of that
	// client context.
	cancel()

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_chat_ctx"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Chat(reqCtx, c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, upstream.lastReq)
	require.NoError(t, upstream.lastReq.Context().Err())
}

// TestForwardGrokChatViaResponsesNonStreamingCachesAndReturnsChat bridges a
// cached Grok Chat request into Responses. It checks isolated cache/header
// identity, request tools and shape, quota snapshot, observed usage and
// buffered Chat answer.
func TestForwardGrokChatViaResponsesNonStreamingCachesAndReturnsChat(t *testing.T) {
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"system","content":"be concise"},{"role":"user","content":"hi"}],"stream":false,"prompt_cache_key":"stable-session","tools":[],"functions":null,"tool_choice":"none"}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7101})

	provider := grokChatBridgeTestProvider(71)
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: grokChatBridgeCompletedResponse("resp_grok_chat_cache", 9856)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	require.Equal(t, grok.GrokChatResponsesEndpoint, result.UpstreamEndpoint)
	require.Equal(t, "grok-4.5", result.BillingModel)
	require.Equal(t, "grok-4.5", result.UpstreamModel)
	require.Equal(t, 9908, result.Usage.InputTokens)
	require.Equal(t, 12, result.Usage.OutputTokens)
	require.Equal(t, 9856, result.Usage.CacheReadInputTokens)

	identity := gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String()
	require.NotEmpty(t, identity)
	require.NotEqual(t, "stable-session", identity)
	require.Equal(t, identity, upstream.lastReq.Header.Get(GrokConversationIDHeader))
	require.Equal(t, "web_search", gjson.GetBytes(upstream.lastBody, "tools.0.type").String())
	require.Equal(t, "x_search", gjson.GetBytes(upstream.lastBody, "tools.1.type").String())
	require.Equal(t, "none", gjson.GetBytes(upstream.lastBody, "tool_choice").String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "stream").Bool())
	require.Equal(t, "system", gjson.GetBytes(upstream.lastBody, "input.0.role").String())
	require.Equal(t, "user", gjson.GetBytes(upstream.lastBody, "input.1.role").String())
	require.False(t, gjson.GetBytes(upstream.lastBody, "instructions").Exists())
	require.False(t, gjson.GetBytes(upstream.lastBody, "include").Exists())
	require.False(t, gjson.GetBytes(upstream.lastBody, "store").Exists())

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, "cached ok", gjson.Get(recorder.Body.String(), "choices.0.message.content").String())
	require.Equal(t, int64(9856), gjson.Get(recorder.Body.String(), "usage.prompt_tokens_details.cached_tokens").Int())
	require.NotNil(t, repo.updates[provider.Record.ID]["grok_usage_snapshot"])
}

// TestForwardGrokChatViaResponsesNonStreamingRejectsCompletedResponseWithoutUsage
// supplies successful Grok content with no terminal usage. It checks
// missing-usage recovery, copied request identity and absence of committed
// buffered client output.
func TestForwardGrokChatViaResponsesNonStreamingRejectsCompletedResponseWithoutUsage(t *testing.T) {
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false,"prompt_cache_key":"stable-session"}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7102})

	provider := grokChatBridgeTestProvider(72)
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstreamBody := strings.Join([]string{
		`data: {"type":"response.output_text.delta","sequence_number":0,"delta":"ok"}`,
		"",
		`data: {"type":"response.completed","sequence_number":1,"response":{"id":"resp_missing_usage","object":"response","model":"grok-4.5","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}]}}`,
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "Xai-Request-Id": []string{"rid-responses-missing-usage"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	service := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := service.Text.Chat(context.Background(), c, provider, body, "", "")

	require.Nil(t, result)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, http.StatusBadGateway, failoverErr.StatusCode)
	require.Equal(t, "grok_missing_usage", gjson.GetBytes(failoverErr.ResponseBody, "error.code").String())
	require.Equal(t, "rid-responses-missing-usage", http.Header(failoverErr.ResponseHeaders).Get("x-request-id"))
	require.False(t, c.Writer.Written())
	require.Empty(t, recorder.Body.String())
}

// TestForwardGrokChatImageWithoutCacheIdentityUsesResponses supplies image
// content without a cache seed. It checks native Responses dispatch, converted
// text/image parts and absent cache-key and conversation-header values.
func TestForwardGrokChatImageWithoutCacheIdentityUsesResponses(t *testing.T) {
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":[{"type":"text","text":"what is this"},{"type":"image_url","image_url":{"url":"data:image/png;base64,QQ=="}}]}],"stream":false}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))

	provider := grokChatBridgeTestProvider(711)
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: grokChatBridgeCompletedResponse("resp_grok_chat_image", 0)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	require.Equal(t, grok.GrokChatResponsesEndpoint, result.UpstreamEndpoint)
	require.Equal(t, "input_text", gjson.GetBytes(upstream.lastBody, "input.0.content.0.type").String())
	require.Equal(t, "what is this", gjson.GetBytes(upstream.lastBody, "input.0.content.0.text").String())
	require.Equal(t, "input_image", gjson.GetBytes(upstream.lastBody, "input.0.content.1.type").String())
	require.Equal(t, "data:image/png;base64,QQ==", gjson.GetBytes(upstream.lastBody, "input.0.content.1.image_url").String())
	require.False(t, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").Exists())
	require.Empty(t, upstream.lastReq.Header.Get(GrokConversationIDHeader))
}

// TestForwardGrokChatViaResponsesCodeBuddyUsesStableConversationHeader changes
// request/message IDs and tool content across one CodeBuddy conversation. It
// checks the same derived identity and its outgoing cache/header values for
// both turns.
func TestForwardGrokChatViaResponsesCodeBuddyUsesStableConversationHeader(t *testing.T) {
	const conversationID = "codebuddy-session-42"
	tests := []struct {
		name      string // fixture case name
		requestID string // changing request identifier
		messageID string // changing message identifier
		body      []byte // client request body
	}{
		{
			name:      "first turn",
			requestID: "request-one",
			messageID: "message-one",
			body:      []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"first question"}],"stream":false,"tools":[]}`),
		},
		{
			name:      "later turn with tools",
			requestID: "request-two",
			messageID: "message-two",
			body:      []byte(`{"model":"grok-4.5","messages":[{"role":"system","content":"be concise"},{"role":"user","content":"different later question"}],"stream":false,"tools":[{"type":"function","function":{"name":"lookup","description":"Lookup a value","parameters":{"type":"object","properties":{"key":{"type":"string"}}}}}],"tool_choice":"auto"}`),
		},
	}

	var stableIdentity string
	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(tt.body))
			c.Request.Header.Set(CodeBuddyConversationHeader, conversationID)
			c.Request.Header.Set("X-Conversation-Request-ID", tt.requestID)
			c.Request.Header.Set("X-Conversation-Message-ID", tt.messageID)
			c.Request.Header.Set("X-Request-ID", "generic-"+tt.requestID)
			c.Set("api_key", &apikey.APIKey{ID: 7111})

			identity := ResolveGrokCacheIdentity(c, tt.body, "", "grok-4.5")
			require.NotEmpty(t, identity)
			if index == 0 {
				stableIdentity = identity
			} else {
				require.Equal(t, stableIdentity, identity)
			}
			require.NotContains(t, identity, conversationID)

			provider := grokChatBridgeTestProvider(int64(711 + index))
			repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
				providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
			}}
			upstream := &auxiliaryHTTPRecorder{resp: grokChatBridgeCompletedResponse("resp_codebuddy_"+strconv.Itoa(index), 4096)}
			svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

			result, err := svc.Text.Chat(context.Background(), c, provider, tt.body, "", "")
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
			require.Equal(t, grok.GrokChatResponsesEndpoint, result.UpstreamEndpoint)
			require.Equal(t, identity, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String())
			require.Equal(t, identity, upstream.lastReq.Header.Get(GrokConversationIDHeader))
		})
	}
}

// TestForwardGrokChatViaResponsesTraeToolHistoryKeepsCacheRoute appends
// tool-call history to a free-tier Grok cache request. It checks stable
// identity, Responses dispatch and retained function declarations, control
// flags and paired tool input/output.
func TestForwardGrokChatViaResponsesTraeToolHistoryKeepsCacheRoute(t *testing.T) {
	firstTurnBody := []byte(`{"model":"grok-4.5","messages":[{"role":"system","content":"Be concise"},{"role":"user","content":"Find alpha"}],"stream":false,"prompt_cache_key":"trae-session","tools":[{"type":"function","function":{"name":"lookup","description":"Lookup a value","parameters":{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]},"strict":false}}],"tool_choice":"auto","parallel_tool_calls":true}`)
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"system","content":"Be concise"},{"role":"user","content":"Find alpha"},{"role":"assistant","content":null,"tool_calls":[{"id":"call_lookup","type":"function","function":{"name":"lookup","arguments":"{\"key\":\"alpha\"}"}}]},{"role":"tool","tool_call_id":"call_lookup","content":"{\"value\":\"ok\"}"},{"role":"user","content":"Summarize"}],"stream":false,"prompt_cache_key":"trae-session","tools":[{"type":"function","function":{"name":"lookup","description":"Lookup a value","parameters":{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]},"strict":false}}],"tool_choice":"auto","parallel_tool_calls":true}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
	c.Request.Header.Set("X-TokenRouter-Grok-Client-Tool-Cache", "prefer-cache")
	c.Set("api_key", &apikey.APIKey{ID: 7151})

	provider := grokChatBridgeTestProvider(715)
	provider.Record.Credentials["subscription_tier"] = "free"
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: grokChatBridgeCompletedResponse("resp_grok_chat_trae", 8192)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	firstTurnIdentity := ResolveGrokCacheIdentity(c, firstTurnBody, "", "grok-4.5")
	extendedTurnIdentity := ResolveGrokCacheIdentity(c, body, "", "grok-4.5")
	require.NotEmpty(t, firstTurnIdentity)
	require.Equal(t, firstTurnIdentity, extendedTurnIdentity)

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	require.Equal(t, grok.GrokChatResponsesEndpoint, result.UpstreamEndpoint)
	require.Equal(t, extendedTurnIdentity, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String())
	require.Equal(t, extendedTurnIdentity, upstream.lastReq.Header.Get(GrokConversationIDHeader))

	tools := gjson.GetBytes(upstream.lastBody, "tools").Array()
	require.Len(t, tools, 3)
	require.Equal(t, "function", tools[0].Get("type").String())
	require.Equal(t, "lookup", tools[0].Get("name").String())
	require.Equal(t, "Lookup a value", tools[0].Get("description").String())
	require.Equal(t, "string", tools[0].Get("parameters.properties.key.type").String())
	require.True(t, tools[0].Get("strict").Exists())
	require.False(t, tools[0].Get("strict").Bool())
	require.Equal(t, "web_search", tools[1].Get("type").String())
	require.Equal(t, "x_search", tools[2].Get("type").String())
	require.Equal(t, "auto", gjson.GetBytes(upstream.lastBody, "tool_choice").String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "parallel_tool_calls").Bool())

	require.Equal(t, "function_call", gjson.GetBytes(upstream.lastBody, "input.2.type").String())
	require.Equal(t, "call_lookup", gjson.GetBytes(upstream.lastBody, "input.2.call_id").String())
	require.Equal(t, "lookup", gjson.GetBytes(upstream.lastBody, "input.2.name").String())
	require.Equal(t, `{"key":"alpha"}`, gjson.GetBytes(upstream.lastBody, "input.2.arguments").String())
	require.Equal(t, "function_call_output", gjson.GetBytes(upstream.lastBody, "input.3.type").String())
	require.Equal(t, "call_lookup", gjson.GetBytes(upstream.lastBody, "input.3.call_id").String())
	require.Equal(t, `{"value":"ok"}`, gjson.GetBytes(upstream.lastBody, "input.3.output").String())
}

// TestForwardGrokChatViaResponsesTraeCompatibilityFieldsKeepCacheRoute
// supplies JSON format, instructions, effort placeholders and tool reasoning
// history. It checks cache stability, transformed compatibility fields,
// retained tool pairing and cached-token output.
func TestForwardGrokChatViaResponsesTraeCompatibilityFieldsKeepCacheRoute(t *testing.T) {
	firstTurnBody := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"Find alpha"}],"instructions":"Return concise JSON","stream":false,"response_format":{"type":"json_object"},"service_tier":"fast","stop":null,"reasoning_effort":null,"tools":[{"type":"function","function":{"name":"lookup","description":"Lookup a value","parameters":{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}}}],"tool_choice":"auto","parallel_tool_calls":true}`)
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"Find alpha"},{"role":"assistant","content":null,"reasoning_content":"I should use lookup","tool_calls":[{"index":0,"id":"call_lookup","type":"function","function":{"name":"lookup","arguments":"{\"key\":\"alpha\"}"}}]},{"role":"tool","tool_call_id":"call_lookup","content":"{\"value\":\"ok\"}"},{"role":"user","content":"Summarize"}],"instructions":"Return concise JSON","stream":false,"response_format":{"type":"json_object"},"service_tier":"fast","stop":null,"reasoning_effort":null,"tools":[{"type":"function","function":{"name":"lookup","description":"Lookup a value","parameters":{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}}}],"tool_choice":"auto","parallel_tool_calls":true}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7161})

	provider := grokChatBridgeTestProvider(716)
	provider.Record.Credentials["subscription_tier"] = "free"
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: grokChatBridgeCompletedResponse("resp_grok_chat_trae_compat", 12288)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	firstTurnIdentity := ResolveGrokCacheIdentity(c, firstTurnBody, "", "grok-4.5")
	extendedTurnIdentity := ResolveGrokCacheIdentity(c, body, "", "grok-4.5")
	require.NotEmpty(t, firstTurnIdentity)
	require.Equal(t, firstTurnIdentity, extendedTurnIdentity)

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	require.Equal(t, grok.GrokChatResponsesEndpoint, result.UpstreamEndpoint)
	require.Equal(t, 12288, result.Usage.CacheReadInputTokens)
	require.Equal(t, extendedTurnIdentity, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String())
	require.Equal(t, extendedTurnIdentity, upstream.lastReq.Header.Get(GrokConversationIDHeader))
	require.Equal(t, "Return concise JSON", gjson.GetBytes(upstream.lastBody, "instructions").String())
	require.Equal(t, "json_object", gjson.GetBytes(upstream.lastBody, "text.format.type").String())
	require.Equal(t, "priority", gjson.GetBytes(upstream.lastBody, "service_tier").String())
	require.False(t, gjson.GetBytes(upstream.lastBody, "stop").Exists())
	require.False(t, gjson.GetBytes(upstream.lastBody, "reasoning").Exists())
	require.NotContains(t, string(upstream.lastBody), "<thinking>")
	require.Equal(t, "function_call", gjson.GetBytes(upstream.lastBody, "input.1.type").String())
	require.Equal(t, "function_call_output", gjson.GetBytes(upstream.lastBody, "input.2.type").String())

	tools := gjson.GetBytes(upstream.lastBody, "tools").Array()
	require.Len(t, tools, 3)
	require.Equal(t, "lookup", tools[0].Get("name").String())
	require.Equal(t, "web_search", tools[1].Get("type").String())
	require.Equal(t, "x_search", tools[2].Get("type").String())
	require.Equal(t, int64(12288), gjson.Get(recorder.Body.String(), "usage.prompt_tokens_details.cached_tokens").Int())
}

// TestForwardGrokChatViaResponsesStreamingPropagatesCachedUsage bridges Grok
// SSE to Chat with cached usage. It checks the Responses endpoint, stream
// result, converted text, cached count and DONE marker.
func TestForwardGrokChatViaResponsesStreamingPropagatesCachedUsage(t *testing.T) {
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true}}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7201})

	provider := grokChatBridgeTestProvider(72)
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: grokChatBridgeCompletedResponse("resp_grok_chat_stream", 4096)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, result.Stream)
	require.Equal(t, grok.GrokChatResponsesEndpoint, result.UpstreamEndpoint)
	require.Equal(t, 4096, result.Usage.CacheReadInputTokens)
	require.Contains(t, recorder.Header().Get("Content-Type"), "text/event-stream")
	require.Contains(t, recorder.Body.String(), `"content":"cached ok"`)
	require.Contains(t, recorder.Body.String(), `"cached_tokens":4096`)
	require.Contains(t, recorder.Body.String(), "data: [DONE]")
}

// TestForwardGrokChatRuntimeGateFallsBackToRaw compares missing cache identity
// and a non-cache-capable mapped model. It checks native Chat dispatch,
// selected model, absent injected tools and buffered answer for each fixture.
func TestForwardGrokChatRuntimeGateFallsBackToRaw(t *testing.T) {
	tests := []struct {
		name         string // fixture case name
		setAPIKey    bool   // bind downstream API key
		mappedModel  string // configured mapped model
		wantUpstream string // expected upstream model
	}{
		{name: "missing cache identity", wantUpstream: "grok-4.5"},
		{name: "non cache capable mapped model", setAPIKey: true, mappedModel: "grok-4.3", wantUpstream: "grok-4.3"},
	}

	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
			if tt.setAPIKey {
				c.Set("api_key", &apikey.APIKey{ID: int64(7301 + index)})
			}

			provider := grokChatBridgeTestProvider(int64(73 + index))
			if tt.mappedModel != "" {
				provider.Record.Credentials["model_mapping"] = map[string]any{"grok-4.5": tt.mappedModel}
			}
			repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
				providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
			}}
			upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body: io.NopCloser(strings.NewReader(
					`{"id":"chat_raw","object":"chat.completion","model":"` + tt.wantUpstream + `","choices":[{"index":0,"message":{"role":"assistant","content":"raw ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`,
				)),
			}}
			svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

			result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, grok.DefaultCLIBaseURL+"/chat/completions", upstream.lastReq.URL.String())
			require.Equal(t, grok.GrokChatRawEndpoint, result.UpstreamEndpoint)
			require.Equal(t, tt.wantUpstream, result.UpstreamModel)
			require.False(t, gjson.GetBytes(upstream.lastBody, "tools").Exists())
			require.Equal(t, "raw ok", gjson.Get(recorder.Body.String(), "choices.0.message.content").String())
		})
	}
}

// TestForwardGrokChatViaResponses429UsesGrokRateLimitPolicy returns HTTP 429
// with a forty-five-second retry delay. It checks typed recovery, rate-limit
// storage and runtime block, retained delay and the actual Responses endpoint.
func TestForwardGrokChatViaResponses429UsesGrokRateLimitPolicy(t *testing.T) {
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7501})

	provider := grokChatBridgeTestProvider(75)
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
			"Retry-After":  []string{"45"},
		},
		Body: io.NopCloser(strings.NewReader(`{"error":{"message":"rate limited"}}`)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})
	before := time.Now()

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.Error(t, err)
	require.Nil(t, result)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr))
	require.Equal(t, http.StatusTooManyRequests, failoverErr.StatusCode)
	require.Equal(t, "45", http.Header(failoverErr.ResponseHeaders).Get("Retry-After"))
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	require.Equal(t, grok.GrokChatResponsesEndpoint, GetActualOpenAIUpstreamEndpoint(c))
	require.Equal(t, 1, repo.rateLimitedCalls)
	require.Zero(t, repo.tempUnschedCalls)
	require.WithinDuration(t, before.Add(45*time.Second), repo.lastRateLimitResetAt, time.Second)
	require.True(t, httpFixtureRuntimeBlocked(svc, provider))
}

// TestForwardGrokRawChat429PreservesRetryAfter forces native Chat with a stop
// control and returns HTTP 429. It checks the raw endpoint and retained retry
// delay in the typed failure.
func TestForwardGrokRawChat429PreservesRetryAfter(t *testing.T) {
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false,"stop":"done"}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7551})

	provider := grokChatBridgeTestProvider(755)
	provider.Record.Credentials["expires_at"] = time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusTooManyRequests,
		Header: http.Header{
			"Content-Type": []string{"application/json"},
			"Retry-After":  []string{"45"},
		},
		Body: io.NopCloser(strings.NewReader(`{"error":{"message":"rate limited"}}`)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	require.Nil(t, result)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, http.StatusTooManyRequests, failoverErr.StatusCode)
	require.Equal(t, "45", http.Header(failoverErr.ResponseHeaders).Get("Retry-After"))
	require.Equal(t, grok.DefaultCLIBaseURL+"/chat/completions", upstream.lastReq.URL.String())
}

// TestForwardGrokRawChatErrorRecordsActualEndpoint forces native Chat with
// stop and returns a request rejection. It checks an error with absent result
// and matching captured and observed Chat endpoints.
func TestForwardGrokRawChatErrorRecordsActualEndpoint(t *testing.T) {
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false,"stop":"done"}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7601})

	provider := grokChatBridgeTestProvider(76)
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"bad request"}}`)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, grok.DefaultCLIBaseURL+"/chat/completions", upstream.lastReq.URL.String())
	require.Equal(t, grok.GrokChatRawEndpoint, GetActualOpenAIUpstreamEndpoint(c))
}

// grokChatBridgeTestProvider creates an active Grok OAuth target for the
// supplied ID.
func grokChatBridgeTestProvider(id int64) *gatewayprovider.ExecutionProvider {
	return &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: id,
			Name:        "grok-cache-bridge",
			Platform:    capability.PlatformGrok,
			Type:        capability.ProviderTypeOAuth,
			Status:      billingcore.StatusActive,
			Schedulable: true,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":  "access-token",
				"refresh_token": "refresh-token",
				"expires_at":    time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339),
				"base_url":      grok.DefaultCLIBaseURL,
			},
		},
	}
}

// grokChatBridgeCompletedResponse constructs the fixture SSE terminal with
// supplied cache usage and response identifier. Callers provide an identifier
// safe for direct JSON interpolation and close the returned body after using
// this response.
func grokChatBridgeCompletedResponse(responseID string, cachedTokens int) *http.Response {
	body := strings.Join([]string{
		`data: {"type":"response.output_text.delta","sequence_number":0,"delta":"cached ok"}`,
		"",
		`data: {"type":"response.completed","sequence_number":1,"response":{"id":"` + responseID + `","object":"response","model":"grok-4.5","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"cached ok"}]}],"usage":{"input_tokens":9908,"output_tokens":12,"total_tokens":9920,"input_tokens_details":{"cached_tokens":` + strconv.Itoa(cachedTokens) + `}}}}`,
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":                   []string{"text/event-stream"},
			"Xai-Request-Id":                 []string{responseID + "-request"},
			"X-Ratelimit-Limit-Requests":     []string{"10"},
			"X-Ratelimit-Remaining-Requests": []string{"9"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}

// TestForwardGrokChatViaResponsesDropsRedundantViewImage bridges inline image
// Chat input to Grok Responses. It checks the converted image part and
// outgoing tool policy through the shared inline-image assertions.
func TestForwardGrokChatViaResponsesDropsRedundantViewImage(t *testing.T) {
	body := grokInlineImageChatRequest()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7991})

	provider := grokChatBridgeTestProvider(799)
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: grokChatBridgeCompletedResponse("resp_chat_image", 0)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	require.Equal(t, "input_image", gjson.GetBytes(upstream.lastBody, "input.0.content.1.type").String())
	assertGrokInlineImageTools(t, upstream.lastBody, "tools.#(name==\"%s\")")
}

// TestForwardGrokRawChatDropsRedundantViewImage forwards inline image input to
// a configured Grok API-key Chat endpoint. It checks native image
// representation and outgoing tool policy through the shared assertions.
func TestForwardGrokRawChatDropsRedundantViewImage(t *testing.T) {
	body := grokInlineImageChatRequest()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))

	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 800, Platform: capability.PlatformGrok, Type: capability.ProviderTypeAPIKey, Concurrency: 1,
			Credentials: map[string]any{"api_key": "test-key", "base_url": "https://grok.example.test/v1"},
		},
	}
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(
			`{"id":"chatcmpl","object":"chat.completion","model":"grok-4.6","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`,
		)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "https://grok.example.test/v1/chat/completions", upstream.lastReq.URL.String())
	require.Equal(t, "image_url", gjson.GetBytes(upstream.lastBody, "messages.0.content.1.type").String())
	assertGrokInlineImageTools(t, upstream.lastBody, "tools.#(function.name==\"%s\")")
}

// grokInlineImageChatRequest creates Chat image input with view_image and
// shell tools.
func grokInlineImageChatRequest() []byte {
	return []byte(`{
		"model":"grok-4.6",
		"messages":[{"role":"user","content":[
			{"type":"text","text":"What text is in this image?"},
			{"type":"image_url","image_url":{"url":"data:image/png;base64,AA=="}}
		]}],
		"stream":false,
		"tools":[
			{"type":"function","function":{"name":"view_image","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}},
			{"type":"function","function":{"name":"shell_command","parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}}}
		],
		"tool_choice":"auto"
	}`)
}

// TestForwardAsChatCompletionsForGrokStopFallsBackToXAIChatCompletions
// supplies stop and a raw cache seed to Grok OAuth. It checks raw dispatch,
// isolated conversation identity, stripped cache field, usage and quota
// snapshot.
func TestForwardAsChatCompletionsForGrokStopFallsBackToXAIChatCompletions(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false,"stop":"done","prompt_cache_key":"raw-client-cache-key"}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 5101})

	provider := gatewaytestkit.HealthyGrokOAuthProvider(51, "access-token")
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{51: provider},
		},
	}
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":                   []string{"application/json"},
			"Xai-Request-Id":                 []string{"xai-req"},
			"X-Ratelimit-Limit-Requests":     []string{"10"},
			"X-Ratelimit-Remaining-Requests": []string{"9"},
			"X-Ratelimit-Limit-Tokens":       []string{"1000"},
			"X-Ratelimit-Remaining-Tokens":   []string{"990"},
		},
		Body: io.NopCloser(strings.NewReader(`{"id":"chatcmpl","object":"chat.completion","model":"grok-4.3","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":1}}}`)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.Equal(t, grok.DefaultCLIBaseURL+"/chat/completions", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer access-token", upstream.lastReq.Header.Get("Authorization"))
	require.NotEmpty(t, upstream.lastReq.Header.Get(GrokConversationIDHeader))
	require.NotEqual(t, "raw-client-cache-key", upstream.lastReq.Header.Get(GrokConversationIDHeader))
	require.Equal(t, "grok-4.5", gjson.GetBytes(upstream.lastBody, "model").String())
	require.False(t, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").Exists())
	require.Equal(t, "grok-4.5", result.Model)
	require.Equal(t, "grok-4.5", result.UpstreamModel)
	require.Equal(t, 1, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	require.Equal(t, 1, result.Usage.CacheReadInputTokens)
	require.NotNil(t, repo.updates[51]["grok_usage_snapshot"])
	require.Equal(t, http.StatusOK, recorder.Code)
}

// TestForwardAsChatCompletionsForGrokAPIKeyUsesConfiguredRawEndpointWithoutOAuthIdentity
// supplies a configured Grok API-key base. It checks the native Chat URL,
// bearer key and the fixture absence of OAuth CLI identity headers.
func TestForwardAsChatCompletionsForGrokAPIKeyUsesConfiguredRawEndpointWithoutOAuthIdentity(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 706,
			Platform:    capability.PlatformGrok,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "third-party-key",
				"base_url": "https://grok.example.test/v1",
			},
		},
	}
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"chatcmpl","object":"chat.completion","model":"grok-4.5","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.Equal(t, "https://grok.example.test/v1/chat/completions", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer third-party-key", upstream.lastReq.Header.Get("Authorization"))
	require.Empty(t, upstream.lastReq.Header.Get("X-Grok-Client-Version"))
	require.NotEqual(t, grok.DefaultGrokUpstreamUserAgent(), upstream.lastReq.Header.Get("User-Agent"))
}

// TestForwardAsChatCompletionsForGrokAPIKeyRejectsNonStreamingResponseWithoutUsage
// supplies completed native Chat content without usage. It checks
// missing-usage recovery, retained upstream request ID and no client
// commitment or body.
func TestForwardAsChatCompletionsForGrokAPIKeyRejectsNonStreamingResponseWithoutUsage(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 707,
			Name:        "grok-api-key",
			Platform:    capability.PlatformGrok,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "third-party-key",
				"base_url": "https://grok.example.test/v1",
			},
		},
	}
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "Xai-Request-Id": []string{"rid-grok-raw-missing-usage"}},
		Body: io.NopCloser(strings.NewReader(
			`{"id":"resp_missing_usage","object":"chat.completion","model":"grok-4.5","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`,
		)),
	}}
	service := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := service.Text.Chat(context.Background(), c, provider, body, "", "")

	require.Nil(t, result)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, http.StatusBadGateway, failoverErr.StatusCode)
	require.Equal(t, "grok_missing_usage", gjson.GetBytes(failoverErr.ResponseBody, "error.code").String())
	require.Equal(t, "rid-grok-raw-missing-usage", http.Header(failoverErr.ResponseHeaders).Get("x-request-id"))
	require.False(t, c.Writer.Written())
	require.Empty(t, recorder.Body.String())
}

// TestForwardAsChatCompletionsForGrokStreamingUsesRawXAIChatCompletions
// supplies an OAuth Grok stream without a cache identity. It checks raw
// dispatch, CLI headers, usage injection, returned usage, downstream DONE and
// quota snapshot.
func TestForwardAsChatCompletionsForGrokStreamingUsesRawXAIChatCompletions(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	provider := gatewaytestkit.HealthyGrokOAuthProvider(53, "access-token")
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{53: provider},
		},
	}
	upstreamBody := strings.Join([]string{
		`data: {"id":"chatcmpl_grok","object":"chat.completion.chunk","model":"grok-4.3","choices":[{"index":0,"delta":{"content":"ok"}}]}`,
		"",
		`data: {"id":"chatcmpl_grok","object":"chat.completion.chunk","model":"grok-4.3","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":4,"total_tokens":10,"prompt_tokens_details":{"cached_tokens":1}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":                   []string{"text/event-stream"},
			"X-Request-Id":                   []string{"chat-stream-req"},
			"X-Ratelimit-Limit-Requests":     []string{"10"},
			"X-Ratelimit-Remaining-Requests": []string{"7"},
		},
		Body: io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), options: protocolHTTPOptions(), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.Equal(t, grok.DefaultCLIBaseURL+"/chat/completions", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer access-token", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, "text/event-stream", upstream.lastReq.Header.Get("Accept"))
	require.Equal(t, grok.CLIUserAgent(grok.CLIClientVersion), upstream.lastReq.Header.Get("User-Agent"))
	require.Equal(t, "grok-4.5", gjson.GetBytes(upstream.lastBody, "model").String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "stream_options.include_usage").Bool())
	require.True(t, result.Stream)
	require.Equal(t, 6, result.Usage.InputTokens)
	require.Equal(t, 4, result.Usage.OutputTokens)
	require.Equal(t, 1, result.Usage.CacheReadInputTokens)
	require.Contains(t, recorder.Body.String(), "data: [DONE]")
	require.NotNil(t, repo.updates[53]["grok_usage_snapshot"])
}

// TestForwardAsChatCompletionsForGrokStreamingStopFallsBackToRawXAIChatCompletions
// supplies stop and a native conversation header on a keyed stream. It checks
// raw CLI dispatch, rewritten conversation identity, usage/DONE output and
// quota snapshot.
func TestForwardAsChatCompletionsForGrokStreamingStopFallsBackToRawXAIChatCompletions(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":true,"stop":"done"}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set(GrokConversationIDHeader, "native-client-conversation")
	c.Set("api_key", &apikey.APIKey{ID: 5301})

	provider := gatewaytestkit.HealthyGrokOAuthProvider(53, "access-token")
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{53: provider},
		},
	}
	upstreamBody := strings.Join([]string{
		`data: {"id":"chatcmpl_grok","object":"chat.completion.chunk","model":"grok-4.3","choices":[{"index":0,"delta":{"content":"ok"}}]}`,
		"",
		`data: {"id":"chatcmpl_grok","object":"chat.completion.chunk","model":"grok-4.3","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":4,"total_tokens":10,"prompt_tokens_details":{"cached_tokens":1}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":                   []string{"text/event-stream"},
			"X-Request-Id":                   []string{"chat-stream-req"},
			"X-Ratelimit-Limit-Requests":     []string{"10"},
			"X-Ratelimit-Remaining-Requests": []string{"7"},
		},
		Body: io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), options: protocolHTTPOptions(), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.Equal(t, grok.DefaultCLIBaseURL+"/chat/completions", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer access-token", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, "text/event-stream", upstream.lastReq.Header.Get("Accept"))
	require.Equal(t, grok.CLIUserAgent(grok.CLIClientVersion), upstream.lastReq.Header.Get("User-Agent"))
	require.Equal(t, grok.CLIClientVersion, upstream.lastReq.Header.Get("X-Grok-Client-Version"))
	require.NotEmpty(t, upstream.lastReq.Header.Get(GrokConversationIDHeader))
	require.NotEqual(t, "native-client-conversation", upstream.lastReq.Header.Get(GrokConversationIDHeader))
	require.Equal(t, "grok-4.5", gjson.GetBytes(upstream.lastBody, "model").String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "stream_options.include_usage").Bool())
	require.True(t, result.Stream)
	require.Equal(t, 6, result.Usage.InputTokens)
	require.Equal(t, 4, result.Usage.OutputTokens)
	require.Equal(t, 1, result.Usage.CacheReadInputTokens)
	require.Contains(t, recorder.Body.String(), "data: [DONE]")
	require.NotNil(t, repo.updates[53]["grok_usage_snapshot"])
}

// TestForwardAsChatCompletionsForGrokComposerBridgesImageInput describes one
// image before a Composer Chat request. It checks two endpoints, description
// replacement, exact large metadata number, separate conversation identity and
// summed usage.
func TestForwardAsChatCompletionsForGrokComposerBridgesImageInput(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"grok-composer-2.5-fast","messages":[{"role":"system","content":"You are concise."},{"role":"user","content":[{"type":"text","text":"What is shown?"},{"type":"image_url","image_url":{"url":"data:image/png;base64,QUJD"}}]}],"metadata":{"large_id":9007199254740993},"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Set("api_key", &apikey.APIKey{ID: 5501})

	provider := gatewaytestkit.HealthyGrokOAuthProvider(55, "access-token")
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{55: provider},
		},
	}
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "xai-request-id": []string{"vision-req"}},
			Body:       io.NopCloser(strings.NewReader(`{"id":"resp_vision","object":"response","model":"grok-build-0.1","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"A small diagram with ABC letters."}]}],"usage":{"input_tokens":11,"output_tokens":7,"total_tokens":18}}`)),
		},
		{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type":                   []string{"application/json"},
				"X-Request-Id":                   []string{"composer-req"},
				"X-Ratelimit-Limit-Requests":     []string{"10"},
				"X-Ratelimit-Remaining-Requests": []string{"9"},
				"X-Ratelimit-Limit-Tokens":       []string{"1000"},
				"X-Ratelimit-Remaining-Tokens":   []string{"980"},
			},
			Body: io.NopCloser(strings.NewReader(`{"id":"chatcmpl_composer","object":"chat.completion","model":"grok-composer-2.5-fast","choices":[{"index":0,"message":{"role":"assistant","content":"It shows ABC."},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}}`)),
		},
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), options: protocolHTTPOptions(), transport: upstream, providers: repo})

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.requests[0].URL.String())
	require.Empty(t, upstream.requests[0].Header.Get(GrokConversationIDHeader))
	require.Equal(t, "grok-build-0.1", gjson.GetBytes(upstream.bodies[0], "model").String())
	require.Equal(t, "input_image", gjson.GetBytes(upstream.bodies[0], "input.0.content.1.type").String())
	require.Equal(t, grok.DefaultCLIBaseURL+"/chat/completions", upstream.requests[1].URL.String())
	require.NotEmpty(t, upstream.requests[1].Header.Get(GrokConversationIDHeader))
	require.Equal(t, "grok-composer-2.5-fast", gjson.GetBytes(upstream.bodies[1], "model").String())
	require.False(t, strings.Contains(string(upstream.bodies[1]), "image_url"))
	require.Equal(t, "9007199254740993", gjson.GetBytes(upstream.bodies[1], "metadata.large_id").Raw)
	require.Contains(t, gjson.GetBytes(upstream.bodies[1], "messages.1.content").String(), "Image 1 description")
	require.Contains(t, gjson.GetBytes(upstream.bodies[1], "messages.1.content").String(), "A small diagram with ABC letters.")
	require.Equal(t, 14, result.Usage.InputTokens)
	require.Equal(t, 12, result.Usage.OutputTokens)
	require.Equal(t, "It shows ABC.", gjson.Get(recorder.Body.String(), "choices.0.message.content").String())
	require.NotNil(t, repo.updates[55]["grok_usage_snapshot"])
}

// forcedResponsesChatTestProvider creates an API-key target forcing Chat
// through Responses conversion.
func forcedResponsesChatTestProvider() *gatewayprovider.ExecutionProvider {
	provider := rawChatCompletionsTestProvider()
	provider.Record.Extra = map[string]any{"openai_text_route_mode": "force_responses"}
	return provider
}

// TestForwardAsChatCompletions_ResponseFailed_PassthroughRule applies a
// configured override to buffered context-window failure. It checks HTTP 400,
// upstream_error type and the retained context-window message after rule
// matching.
func TestForwardAsChatCompletions_ResponseFailed_PassthroughRule(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	bindPassthroughRule(c, "openai", []string{"context_length_exceeded"}, 400)

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(buildContextLengthFailedSSE())),
	}}
	svc := newWSFixture(wsFixtureInputs{options: rawChatCompletionsTestConfig(), transport: upstream})

	provider := forcedResponsesChatTestProvider()
	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "passthrough")
	require.Equal(t, 400, rec.Code, "passthrough rule should override 502 to 400")

	respBody := rec.Body.String()
	errType := gjson.Get(respBody, "error.type").String()
	require.Equal(t, "upstream_error", errType)
	errMsg := gjson.Get(respBody, "error.message").String()
	require.NotEmpty(t, errMsg, "passthrough should preserve error message")
	require.Contains(t, errMsg, "context window")
}

// TestForwardAsChatCompletions_ResponseFailed_NoRule_Still502 supplies the
// same buffered context-window failure with no presentation rule. It checks a
// returned error and the default HTTP 502 response.
func TestForwardAsChatCompletions_ResponseFailed_NoRule_Still502(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(buildContextLengthFailedSSE())),
	}}
	svc := newWSFixture(wsFixtureInputs{options: rawChatCompletionsTestConfig(), transport: upstream})

	provider := forcedResponsesChatTestProvider()
	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	require.Equal(t, http.StatusBadGateway, rec.Code, "without passthrough rule should still be 502")
}

// TestForwardAsChatCompletions_ResponseFailedCustomErrorMissReturnsGeneric500
// excludes the event status from configured provider error codes. It checks
// generic HTTP 500, no failover wrapper and no persistent error-store call.
func TestForwardAsChatCompletions_ResponseFailedCustomErrorMissReturnsGeneric500(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	failed := `{"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"temporary failure"},"output":[]}}`
	repo := &openAIWSPolicyRepo{}
	svc := newWSFixture(wsFixtureInputs{options: rawChatCompletionsTestConfig(), transport: &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("data: " + failed + "\n\n")),
	}}})
	setWSFixtureHealth(svc, newUpstreamHealthForTest(repo, svc.options, nil, providercore.HealthOptions{}, nil))

	provider := forcedResponsesChatTestProvider()
	provider.Record.Credentials["custom_error_codes_enabled"] = true
	provider.Record.Credentials["custom_error_codes"] = []any{float64(http.StatusUnprocessableEntity)}

	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, "Upstream gateway error", gjson.GetBytes(rec.Body.Bytes(), "error.message").String())
	require.Zero(t, repo.setErrorCalls)
}

// TestForwardAsChatCompletions_ResponseFailedCustomNonDefaultStatusFailsOver
// admits semantic HTTP 422 through provider custom codes. It checks typed
// recovery, scheduling error recording, no same-provider retry and no client
// commitment.
func TestForwardAsChatCompletions_ResponseFailedCustomNonDefaultStatusFailsOver(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	failed := `{"type":"response.failed","response":{"status":"failed","error":{"status_code":422,"code":"configured","type":"upstream_error","message":"configured failure"},"output":[]}}`
	repo := &openAIWSPolicyRepo{}
	svc := newWSFixture(wsFixtureInputs{options: rawChatCompletionsTestConfig(), transport: &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("data: " + failed + "\n\n")),
	}}})
	setWSFixtureHealth(svc, newUpstreamHealthForTest(repo, svc.options, nil, providercore.HealthOptions{}, nil))

	provider := forcedResponsesChatTestProvider()
	provider.Record.Credentials["custom_error_codes_enabled"] = true
	provider.Record.Credentials["custom_error_codes"] = []any{float64(http.StatusUnprocessableEntity)}

	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	var failoverErr *forwardcore.UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, http.StatusUnprocessableEntity, failoverErr.StatusCode)
	require.False(t, failoverErr.RetryableOnSameProvider)
	require.False(t, c.Writer.Written())
	require.Equal(t, 1, repo.setErrorCalls)
}

// TestForwardAsChatCompletions_ResponseFailed_ErrorCodeRuleMatchesViaSemanticStatus
// matches HTTP 400 inferred from a context-window event. It checks the
// configured final status, upstream_error type and context-window message in
// the envelope.
func TestForwardAsChatCompletions_ResponseFailed_ErrorCodeRuleMatchesViaSemanticStatus(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	bindStatusCodePassthroughRule(c, "openai", http.StatusBadRequest, "context_length_exceeded", http.StatusBadRequest)

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(buildContextLengthFailedSSE())),
	}}
	svc := newWSFixture(wsFixtureInputs{options: rawChatCompletionsTestConfig(), transport: upstream})

	provider := forcedResponsesChatTestProvider()
	_, err := svc.Text.Chat(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, rec.Code, "error-code-conditioned rule should match via semantic status inference")
	respBody := rec.Body.String()
	require.Equal(t, "upstream_error", gjson.Get(respBody, "error.type").String())
	require.Contains(t, gjson.Get(respBody, "error.message").String(), "context window")
}

// TestOpenAISetupTokenChatCompletionsUsesCodexTransform captures setup-token
// Chat before an HTTP 400 fixture rejection. It checks Codex URL, credential
// and identity headers, promoted instructions, user input and derived cache
// key.
func TestOpenAISetupTokenChatCompletionsUsesCodexTransform(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","messages":[{"role":"system","content":"setup instructions"},{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"type":"invalid_request_error","message":"stop after request capture"}}`)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{}, transport: upstream})
	provider := openAISetupTokenCompatProvider(71)

	result, err := svc.Text.Chat(context.Background(), c, provider, body, "", "gpt-5.4")

	require.Error(t, err)
	require.Nil(t, result)
	require.NotNil(t, upstream.lastReq)
	require.Equal(t, ChatgptCodexURL, upstream.lastReq.URL.String())
	require.Equal(t, "Bearer setup-token-value", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, "chatgpt-setup", upstream.lastReq.Header.Get("chatgpt-account-id"))
	require.NotEmpty(t, upstream.lastReq.Header.Get("originator"))
	require.Equal(t, "setup instructions", gjson.GetBytes(upstream.lastBody, "instructions").String())
	require.Equal(t, int64(1), gjson.GetBytes(upstream.lastBody, "input.#").Int())
	require.Equal(t, "user", gjson.GetBytes(upstream.lastBody, "input.0.role").String())
	require.NotEmpty(t, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String())
}

// TestAdaptiveProtocolRoutesMessagesToNativeAnthropic captures Messages input
// for an adaptive Zhipu target. It checks the configured native Anthropic
// endpoint and retained model before the deliberate transport failure.
func TestAdaptiveProtocolRoutesMessagesToNativeAnthropic(t *testing.T) {
	body := []byte(`{"model":"glm-4.7","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	upstream := &auxiliaryHTTPRecorder{err: errors.New("stop after capture")}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	provider := adaptiveProtocolTestProvider(capability.PlatformZhipu, map[string]any{
		providercore.APIProtocolChatCompletions: "http://chat.example",
		providercore.APIProtocolAnthropic:       "http://anthropic.example",
	})

	_, err := svc.Text.Messages(context.Background(), adaptiveProtocolTestContext("/v1/messages", body), provider, body, "", "")
	require.Error(t, err)
	require.Equal(t, "http://anthropic.example/v1/messages", upstream.lastReq.URL.String())
	require.Equal(t, "glm-4.7", gjson.GetBytes(upstream.lastBody, "model").String())
}

// Write delegates up to failAfter calls, then returns the fixture disconnect
// error. Nonpositive limits fail immediately; writes counts calls that reached
// the embedded writer, even if that delegated write itself reports an error.
func (w *openAICompatFailingWriter) Write(p []byte) (int, error) {
	if w.writes >= w.failAfter {
		return 0, errors.New("write failed: client disconnected")
	}
	w.writes++
	return w.ResponseWriter.Write(p)
}

// TestForwardAsAnthropic_UsesExactFableMessagesDispatchModel maps a Fable
// client model to the configured Responses model. It checks separate client,
// billing and upstream identities and restoration of the client model in
// Messages output.
func TestForwardAsAnthropic_UsesExactFableMessagesDispatchModel(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"claude-fable-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_fable","object":"response","model":"gpt-5.6-sol","status":"completed","output":[{"type":"message","id":"msg_fable","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_fable"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.6-sol")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "claude-fable-5", result.Model)
	require.Equal(t, "gpt-5.6-sol", result.BillingModel)
	require.Equal(t, "gpt-5.6-sol", result.UpstreamModel)
	require.Equal(t, "gpt-5.6-sol", gjson.GetBytes(upstream.lastBody, "model").String())
	require.NotContains(t, string(upstream.lastBody), "claude-fable-5")
	require.Equal(t, "claude-fable-5", gjson.GetBytes(rec.Body.Bytes(), "model").String())
}

// TestForwardAsAnthropic_NormalizesRoutingAndEffortForGpt54XHigh supplies a
// suffixed model without explicit effort. It checks normalized
// upstream/billing model, medium outgoing/result effort and restored client
// model and answer.
func TestForwardAsAnthropic_NormalizesRoutingAndEffortForGpt54XHigh(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4-xhigh","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_compat"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
				"model_mapping": map[string]any{
					"gpt-5.4": "gpt-5.4",
				},
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "gpt-5.4-xhigh", result.Model)
	require.Equal(t, "gpt-5.4", result.UpstreamModel)
	require.Equal(t, "gpt-5.4", result.BillingModel)
	require.NotNil(t, result.ReasoningEffort)
	require.Equal(t, "medium", *result.ReasoningEffort)

	require.Equal(t, "gpt-5.4", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, "medium", gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "gpt-5.4-xhigh", gjson.GetBytes(rec.Body.Bytes(), "model").String())
	require.Equal(t, "ok", gjson.GetBytes(rec.Body.Bytes(), "content.0.text").String())
	t.Logf("upstream body: %s", string(upstream.lastBody))
	t.Logf("response body: %s", rec.Body.String())
}

// TestForwardAsAnthropic_PreservesMaxForFinalGPT56ResponsesModel enumerates
// API-key and OAuth mappings into newer and older models. It checks final
// outgoing model and model-sensitive max/default effort in both body and
// result.
func TestForwardAsAnthropic_PreservesMaxForFinalGPT56ResponsesModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string // fixture case name
		// selected fixture execution provider
		provider      *gatewayprovider.ExecutionProvider
		model         string // client request model
		defaultMapped string // group-mapped model fallback
		effort        string // requested effort value
		wantModel     string // expected outgoing model
		wantEffort    string // expected outgoing effort
	}{
		{
			name:       "供应商限定型号保留显式 max 和完整 ID",
			provider:   rawGPT56ResponsesAPIKeyProvider("qualified", "openai/gpt-5.6-sol"),
			model:      "qualified",
			effort:     "max",
			wantModel:  "openai/gpt-5.6-sol",
			wantEffort: "max",
		},
		{
			name:       "API Key mapping keeps Luna max",
			provider:   rawGPT56ResponsesAPIKeyProvider("luna", "gpt-5.6-luna"),
			model:      "luna",
			effort:     "max",
			wantModel:  "gpt-5.6-luna",
			wantEffort: "max",
		},
		{
			name:       "OAuth mapping keeps Sol max",
			provider:   rawGPT56ResponsesOAuthProvider("sol", "gpt-5.6-sol"),
			model:      "sol",
			effort:     "max",
			wantModel:  "gpt-5.6-sol",
			wantEffort: "max",
		},
		{
			name:       "old model still maps max to xhigh",
			provider:   rawGPT56ResponsesAPIKeyProvider("gpt-5.5", "gpt-5.5"),
			model:      "gpt-5.5",
			effort:     "max",
			wantModel:  "gpt-5.5",
			wantEffort: "xhigh",
		},
		{
			name:       "mapping from GPT56 to old model downgrades max",
			provider:   rawGPT56ResponsesAPIKeyProvider("gpt-5.6-sol", "gpt-5.5"),
			model:      "gpt-5.6-sol",
			effort:     "max",
			wantModel:  "gpt-5.5",
			wantEffort: "xhigh",
		},
		{
			name:       "GPT56 default remains medium",
			provider:   rawGPT56ResponsesAPIKeyProvider("gpt-5.6-sol", "gpt-5.6-sol"),
			model:      "gpt-5.6-sol",
			wantModel:  "gpt-5.6-sol",
			wantEffort: "medium",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			body := `{"model":"` + tt.model + `","max_tokens":16,"messages":[{"role":"user","content":"hello"}`
			if tt.effort != "" {
				body += `],"output_config":{"effort":"` + tt.effort + `"},"stream":false}`
			} else {
				body += `],"stream":false}`
			}
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")

			upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_gpt56_"+tt.name, tt.wantModel)}
			svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})

			result, err := svc.Text.Messages(context.Background(), c, tt.provider, []byte(body), "", tt.defaultMapped)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tt.wantModel, result.UpstreamModel)
			require.Equal(t, tt.wantModel, gjson.GetBytes(upstream.lastBody, "model").String())
			require.Equal(t, tt.wantEffort, gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
			require.NotNil(t, result.ReasoningEffort)
			require.Equal(t, tt.wantEffort, *result.ReasoningEffort)
		})
	}
}

// rawGPT56ResponsesAPIKeyProvider creates an API-key Responses target with one
// supplied model mapping.
func rawGPT56ResponsesAPIKeyProvider(requestedModel, mappedModel string) *gatewayprovider.ExecutionProvider {
	return &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 501,
			Name:        "gpt56-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":       "sk-test",
				"base_url":      "https://api.example.com/v1",
				"model_mapping": map[string]any{requestedModel: mappedModel},
			},
			Extra: map[string]any{"use_responses_api": true},
		},
	}
}

// rawGPT56ResponsesOAuthProvider creates an OAuth Codex target with one
// supplied model mapping.
func rawGPT56ResponsesOAuthProvider(requestedModel, mappedModel string) *gatewayprovider.ExecutionProvider {
	return &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 502,
			Name:        "gpt56-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
				"model_mapping":      map[string]any{requestedModel: mappedModel},
			},
		},
	}
}

// TestForwardAsAnthropic_MappedClaudeModelAcceptsChatUsageShape returns
// Chat-style usage inside a Responses terminal for mapped Claude input. It
// checks client/billing/upstream identities and the exact input, output and
// cached-token observations.
func TestForwardAsAnthropic_MappedClaudeModelAcceptsChatUsageShape(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"claude-opus-4-7","max_tokens":16,"messages":[{"role":"user","content":"compact this"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_compact","model":"gpt-5.5","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_compact","object":"response","model":"gpt-5.5","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"prompt_tokens":31,"completion_tokens":9,"total_tokens":40,"prompt_tokens_details":{"cached_tokens":11}}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_compact_usage"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
				"model_mapping": map[string]any{
					"gpt-5.5": "gpt-5.5",
				},
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "claude-opus-4-7", result.Model)
	require.Equal(t, "gpt-5.5", result.BillingModel)
	require.Equal(t, "gpt-5.5", result.UpstreamModel)
	require.Equal(t, 31, result.Usage.InputTokens)
	require.Equal(t, 9, result.Usage.OutputTokens)
	require.Equal(t, 11, result.Usage.CacheReadInputTokens)
	require.Equal(t, "gpt-5.5", gjson.GetBytes(upstream.lastBody, "model").String())
}

// TestForwardAsAnthropic_InjectsPromptCacheKeyForAPIKeyMessagesDispatch
// supplies an explicit cache key for mapped Messages conversion. It checks
// outgoing cache/model fields and retained cache-read usage from the fixture
// response.
func TestForwardAsAnthropic_InjectsPromptCacheKeyForAPIKeyMessagesDispatch(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"metadata":{"user_id":"claude-session-1"},"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.3-codex","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7,"input_tokens_details":{"cached_tokens":3}}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_cache_key"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "stable-cache-key", gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String())
	require.Equal(t, "gpt-5.3-codex", gjson.GetBytes(upstream.lastBody, "model").String())
	require.Equal(t, 3, result.Usage.CacheReadInputTokens)
}

// TestForwardAsAnthropic_AutoDerivesPromptCacheKeyWhenMessagesDispatchHasNoSessionID
// supplies system/history without a session seed. It checks an Anthropic
// digest key and its isolated session UUID in the outgoing API-key request.
func TestForwardAsAnthropic_AutoDerivesPromptCacheKeyWhenMessagesDispatchHasNoSessionID(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"system":"You are helpful.","messages":[{"role":"user","content":"open repo"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.3-codex","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7,"input_tokens_details":{"cached_tokens":3}}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_auto_cache_key"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, result)
	cacheKey := gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String()
	require.NotEmpty(t, cacheKey)
	require.True(t, strings.HasPrefix(cacheKey, "anthropic-digest-"))
	require.Equal(t, upstreamcore.GenerateSessionUUID(upstreamcore.IsolateSessionID(0, cacheKey)), upstream.lastReq.Header.Get("session_id"))
}

// TestForwardAsAnthropic_GPT6AstraPromptCacheIdentityStableAcrossAppendedTurns
// appends history to the same mapped Astra conversation. It checks both
// captured upstream models and stable nonempty cache keys and session headers.
func TestForwardAsAnthropic_GPT6AstraPromptCacheIdentityStableAcrossAppendedTurns(t *testing.T) {
	t.Parallel()

	for _, mappedModel := range []string{"gpt-6-astra"} {
		t.Run(mappedModel, func(t *testing.T) {
			t.Parallel()

			upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
				openAICompatSSECompletedResponse("resp_gpt6_first", mappedModel),
				openAICompatSSECompletedResponse("resp_gpt6_second", mappedModel),
			}}
			svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
			provider := &gatewayprovider.ExecutionProvider{
				Record: providercore.Record{
					LoadLocation: time.LoadLocation, ID: 6615,
					Name:        "openai-apikey",
					Platform:    capability.PlatformOpenAI,
					Type:        capability.ProviderTypeAPIKey,
					Concurrency: 1,
					Credentials: map[string]any{
						"api_key":  "sk-test",
						"base_url": "https://api.openai.com/v1",
					},
				},
			}

			bodies := [][]byte{
				[]byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"system":"You are helpful.","messages":[{"role":"user","content":"Inspect the repository"}],"stream":false}`),
				[]byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"system":"You are helpful.","messages":[{"role":"user","content":"Inspect the repository"},{"role":"assistant","content":"Done."},{"role":"user","content":"Run the tests"}],"stream":false}`),
			}
			for _, body := range bodies {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
				c.Request.Header.Set("Content-Type", "application/json")

				result, err := svc.Text.Messages(context.Background(), c, provider, body, "", mappedModel)
				require.NoError(t, err)
				require.NotNil(t, result)
			}

			require.Len(t, upstream.bodies, 2)
			require.Len(t, upstream.requests, 2)
			require.Equal(t, mappedModel, gjson.GetBytes(upstream.bodies[0], "model").String())
			require.Equal(t, mappedModel, gjson.GetBytes(upstream.bodies[1], "model").String())
			firstCacheKey := gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").String()
			secondCacheKey := gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").String()
			require.NotEmpty(t, firstCacheKey)
			require.Equal(t, firstCacheKey, secondCacheKey)
			firstSessionID := upstream.requests[0].Header.Get("session_id")
			secondSessionID := upstream.requests[1].Header.Get("session_id")
			require.NotEmpty(t, firstSessionID)
			require.Equal(t, firstSessionID, secondSessionID)
		})
	}
}

// TestForwardAsAnthropic_DoesNotAutoDerivePromptCacheKeyForNonCodexModel maps
// Messages to the listed noncompatible API-key model. It checks successful
// forwarding with no derived cache field or session header.
func TestForwardAsAnthropic_DoesNotAutoDerivePromptCacheKeyForNonCodexModel(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-4o","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_no_cache_key"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-4o")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").Exists())
	require.Empty(t, upstream.lastReq.Header.Get("session_id"))
}

// TestForwardAsAnthropic_OAuthNonCodexModelRestoresCodexIdentity maps
// third-party Messages client input to gpt-4o through OAuth. It checks
// successful execution and the shared Codex identity-header assertions on the
// captured request.
func TestForwardAsAnthropic_OAuthNonCodexModelRestoresCodexIdentity(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("User-Agent", "messages-bridge/1.0")

	upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_bridge_identity", "gpt-4o")}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-4o")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, upstream.lastReq)
	requireOpenAIMessagesCodexIdentity(t, upstream.lastReq, openai.CodexCLIUserAgent, openai.CodexDefaultOriginator)
}

// TestForwardAsAnthropic_TrimsFullReplayOnlyForCodexCompatModels converts
// fifteen history messages for compatible and other API-key models. It checks
// the compatible retained suffix and guard, while the other fixture retains
// all messages.
func TestForwardAsAnthropic_TrimsFullReplayOnlyForCodexCompatModels(t *testing.T) {
	t.Parallel()

	messages := make([]string, 0, 12+3)
	// Exceed the compatibility replay suffix with distinguishable messages so
	// assertions can detect both trimming and retention of the earlier conversation.
	for i := range 12 + 3 {
		messages = append(messages, `{"role":"user","content":"message-`+fmt.Sprintf("%02d", i)+`"}`)
	}
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[` + strings.Join(messages, ",") + `],"stream":false}`)

	run := func(t *testing.T, mappedModel string) []byte {
		t.Helper()

		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
		c.Request.Header.Set("Content-Type", "application/json")

		upstreamBody := strings.Join([]string{
			`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"` + mappedModel + `","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
			"",
			"data: [DONE]",
			"",
		}, "\n")
		upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_trim"}},
			Body:       io.NopCloser(strings.NewReader(upstreamBody)),
		}}

		svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
		provider := &gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				LoadLocation: time.LoadLocation, ID: 1,
				Name:        "openai-apikey",
				Platform:    capability.PlatformOpenAI,
				Type:        capability.ProviderTypeAPIKey,
				Concurrency: 1,
				Credentials: map[string]any{
					"api_key":  "sk-test",
					"base_url": "https://api.openai.com/v1",
				},
			},
		}

		result, err := svc.Text.Messages(context.Background(), c, provider, body, "", mappedModel)
		require.NoError(t, err)
		require.NotNil(t, result)
		return upstream.lastBody
	}

	codexBody := run(t, "gpt-5.3-codex")
	require.Equal(t, int64(12+1), gjson.GetBytes(codexBody, "input.#").Int())
	require.Equal(t, "developer", gjson.GetBytes(codexBody, "input.0.role").String())
	require.Contains(t, gjson.GetBytes(codexBody, "input.0.content.0.text").String(), "<tokenrouter-claude-code-todo-guard>")
	require.Equal(t, "message-03", gjson.GetBytes(codexBody, "input.1.content.0.text").String())
	require.Equal(t, "message-14", gjson.GetBytes(codexBody, "input.12.content.0.text").String())

	nonCompatBody := run(t, "gpt-4o")
	require.Equal(t, int64(12+3), gjson.GetBytes(nonCompatBody, "input.#").Int())
	require.Equal(t, "message-00", gjson.GetBytes(nonCompatBody, "input.0.content.0.text").String())
}

// TestForwardAsAnthropic_OAuthCompatKeepsFullReplayForCacheGrowth sends
// fifteen history messages through OAuth Codex conversion. It checks the
// inserted developer guard, retained first/last history messages and absent
// outgoing prompt-cache field.
func TestForwardAsAnthropic_OAuthCompatKeepsFullReplayForCacheGrowth(t *testing.T) {
	t.Parallel()

	messages := make([]string, 0, 12+3)
	// Exceed the compatibility replay suffix with distinguishable messages so
	// assertions can detect both trimming and retention of the earlier conversation.
	for i := range 12 + 3 {
		messages = append(messages, `{"role":"user","content":"message-`+fmt.Sprintf("%02d", i)+`"}`)
	}
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[` + strings.Join(messages, ",") + `],"stream":false}`)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_oauth_trim", "gpt-5.4")}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int64(12+4), gjson.GetBytes(upstream.lastBody, "input.#").Int())
	require.Equal(t, "developer", gjson.GetBytes(upstream.lastBody, "input.0.role").String())
	require.Contains(t, gjson.GetBytes(upstream.lastBody, "input.0.content.0.text").String(), "<tokenrouter-claude-code-todo-guard>")
	require.Equal(t, "message-00", gjson.GetBytes(upstream.lastBody, "input.1.content.0.text").String())
	require.Equal(t, "message-14", gjson.GetBytes(upstream.lastBody, "input.15.content.0.text").String())
	require.False(t, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").Exists())
}

// TestForwardAsAnthropic_AttachesPreviousResponseIDForCompatContinuation
// executes two keyed turns with continuation enabled. It checks first-response
// binding, second previous-response identity and trimmed latest-turn input
// plus the developer guard.
func TestForwardAsAnthropic_AttachesPreviousResponseIDForCompatContinuation(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
			Extra: map[string]any{providercore.ExtraKeyResponsesContinuationSupported: true},
		},
	}

	firstBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"}],"stream":false}`)
	upstream.resp = openAICompatSSECompletedResponse("resp_first", "gpt-5.3-codex")
	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, firstResult)
	require.Equal(t, "resp_first", firstResult.ResponseID)
	require.False(t, gjson.GetBytes(upstream.lastBody, "previous_response_id").Exists())

	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	upstream.resp = openAICompatSSECompletedResponse("resp_second", "gpt-5.3-codex")
	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.Equal(t, "resp_second", secondResult.ResponseID)
	require.Equal(t, "resp_first", gjson.GetBytes(upstream.lastBody, "previous_response_id").String())
	require.Equal(t, int64(2), gjson.GetBytes(upstream.lastBody, "input.#").Int())
	require.Equal(t, "developer", gjson.GetBytes(upstream.lastBody, "input.0.role").String())
	require.Contains(t, gjson.GetBytes(upstream.lastBody, "input.0.content.0.text").String(), "<tokenrouter-claude-code-todo-guard>")
	require.Equal(t, "second", gjson.GetBytes(upstream.lastBody, "input.1.content.0.text").String())
}

// TestForwardAsAnthropic_DoesNotAttachPreviousResponseIDWhenCapabilityDisabled
// seeds an ID in storage for a provider without enabled continuation. It
// checks successful Messages forwarding and no outgoing previous_response_id.
func TestForwardAsAnthropic_DoesNotAttachPreviousResponseIDWhenCapabilityDisabled(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_disabled", "gpt-5.3-codex")}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "nested-tokenrouter",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://inner-tokenrouter.example/v1",
			},
		},
	}
	svc.Text.Continuation.BindResponse(session.CompatResponseKey(provider.Record.ID, 0, "stable-cache-key"), "resp_should_not_attach")

	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, gjson.GetBytes(upstream.lastBody, "previous_response_id").Exists())
}

// TestForwardAsAnthropic_ReplaysWithoutContinuationWhenNestedOAuthRejectsHTTPPreviousResponseID
// returns the nested HTTP continuation rejection before success. It checks two
// requests, initial saved ID and its absence on the replay.
func TestForwardAsAnthropic_ReplaysWithoutContinuationWhenNestedOAuthRejectsHTTPPreviousResponseID(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "nested-tokenrouter",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://inner-tokenrouter.example/v1",
			},
			Extra: map[string]any{providercore.ExtraKeyResponsesContinuationSupported: true},
		},
	}
	svc.Text.Continuation.BindResponse(session.CompatResponseKey(provider.Record.ID, 0, "stable-cache-key"), "resp_nested")
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	upstream.responses = []*http.Response{
		{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"previous_response_id requires an OpenAI API-key provider for HTTP requests"}}`)),
		},
		openAICompatSSECompletedResponse("resp_replayed_nested", "gpt-5.3-codex"),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "resp_replayed_nested", result.ResponseID)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, "resp_nested", gjson.GetBytes(upstream.bodies[0], "previous_response_id").String())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())
}

// TestForwardAsAnthropic_PreviousResponseIDKeepsMultiToolCallContext appends
// two tool calls and results after a bound first response. It checks both
// call/result identifiers, their order and the latest user text in continued
// input.
func TestForwardAsAnthropic_PreviousResponseIDKeepsMultiToolCallContext(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
			Extra: map[string]any{providercore.ExtraKeyResponsesContinuationSupported: true},
		},
	}

	firstBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"inspect files"}],"stream":false}`)
	upstream.resp = openAICompatSSECompletedResponse("resp_first_tools", "gpt-5.3-codex")
	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, firstResult)

	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"inspect files"},{"role":"assistant","content":[{"type":"tool_use","id":"call_one","name":"Read","input":{"file_path":"a.go"}},{"type":"tool_use","id":"call_two","name":"Read","input":{"file_path":"b.go"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_one","content":"package a"},{"type":"tool_result","tool_use_id":"call_two","content":"package b"},{"type":"text","text":"continue"}]}],"tools":[{"name":"Read","description":"read a file","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}}}}],"stream":false}`)
	upstream.resp = openAICompatSSECompletedResponse("resp_second_tools", "gpt-5.3-codex")
	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.Equal(t, "resp_first_tools", gjson.GetBytes(upstream.lastBody, "previous_response_id").String())
	require.Equal(t, "developer", gjson.GetBytes(upstream.lastBody, "input.0.role").String())
	require.Equal(t, "function_call", gjson.GetBytes(upstream.lastBody, "input.1.type").String())
	require.Equal(t, "call_one", gjson.GetBytes(upstream.lastBody, "input.1.call_id").String())
	require.Equal(t, "function_call", gjson.GetBytes(upstream.lastBody, "input.2.type").String())
	require.Equal(t, "call_two", gjson.GetBytes(upstream.lastBody, "input.2.call_id").String())
	require.Equal(t, "function_call_output", gjson.GetBytes(upstream.lastBody, "input.3.type").String())
	require.Equal(t, "call_one", gjson.GetBytes(upstream.lastBody, "input.3.call_id").String())
	require.Equal(t, "function_call_output", gjson.GetBytes(upstream.lastBody, "input.4.type").String())
	require.Equal(t, "call_two", gjson.GetBytes(upstream.lastBody, "input.4.call_id").String())
	require.Equal(t, "continue", gjson.GetBytes(upstream.lastBody, "input.5.content.0.text").String())
}

// TestForwardAsAnthropic_ReplaysFullToolHistoryWhenPreviousResponseUnavailable
// rejects a saved response ID before a successful replay. It checks stable
// TLS-selected headers and complete original user/tool/result/latest input
// after removing the continuation ID.
func TestForwardAsAnthropic_ReplaysFullToolHistoryWhenPreviousResponseUnavailable(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
			Extra: map[string]any{providercore.ExtraKeyResponsesContinuationSupported: true},
		},
	}

	svc.Text.Continuation.BindResponse(session.CompatResponseKey(provider.Record.ID, 0, "stable-cache-key"), "resp_missing")
	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"},{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"lookup","input":{"q":"first"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"found"},{"type":"text","text":"second"}]}],"tools":[{"name":"lookup","input_schema":{"type":"object"}}],"stream":false}`)
	upstream.responses = []*http.Response{
		{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_prev_missing"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"previous_response_id is not available for this user","type":"invalid_request_error"}}`)),
		},
		openAICompatSSECompletedResponse("resp_replayed", "gpt-5.3-codex"),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	c.Request.Header.Set("Content-Type", "application/json")

	tlsMatch := egress.TLSFingerprintRouterMatchResult{Matched: true, UpstreamUserAgent: "router-agent"}
	result, err := svc.Text.Messages(context.Background(), c, provider, secondBody, "stable-cache-key", "gpt-5.3-codex", tlsMatch)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "resp_replayed", result.ResponseID)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, "router-agent", upstream.requests[0].Header.Get("User-Agent"))
	require.Equal(t, "router-agent", upstream.requests[1].Header.Get("User-Agent"))
	require.Equal(t, "resp_missing", gjson.GetBytes(upstream.bodies[0], "previous_response_id").String())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())
	require.Equal(t, int64(5), gjson.GetBytes(upstream.bodies[1], "input.#").Int())
	require.Equal(t, "developer", gjson.GetBytes(upstream.bodies[1], "input.0.role").String())
	require.Contains(t, gjson.GetBytes(upstream.bodies[1], "input.0.content.0.text").String(), "<tokenrouter-claude-code-todo-guard>")
	require.Equal(t, "first", gjson.GetBytes(upstream.bodies[1], "input.1.content.0.text").String())
	require.Equal(t, "function_call", gjson.GetBytes(upstream.bodies[1], "input.2.type").String())
	require.Equal(t, "call_1", gjson.GetBytes(upstream.bodies[1], "input.2.call_id").String())
	require.Equal(t, "function_call_output", gjson.GetBytes(upstream.bodies[1], "input.3.type").String())
	require.Equal(t, "call_1", gjson.GetBytes(upstream.bodies[1], "input.3.call_id").String())
	require.Equal(t, "found", gjson.GetBytes(upstream.bodies[1], "input.3.output").String())
	require.Equal(t, "second", gjson.GetBytes(upstream.bodies[1], "input.4.content.0.text").String())
}

// TestOpenAICompatPreviousResponseUnavailableRecognitionIsStrict compares the
// supported unavailable-response message with wrong status and similar
// unrelated wording. It checks acceptance of the exact fixture and rejection
// of the listed alternatives.
func TestOpenAICompatPreviousResponseUnavailableRecognitionIsStrict(t *testing.T) {
	t.Parallel()
	body := []byte(`{"error":{"message":"previous_response_id is not available for this user","type":"invalid_request_error"}}`)
	require.True(t, gatewayprovider.CompatPreviousResponseNotFound(http.StatusBadRequest, "", body))
	require.False(t, gatewayprovider.CompatPreviousResponseNotFound(http.StatusForbidden, "", body))
	require.False(t, gatewayprovider.CompatPreviousResponseNotFound(http.StatusBadRequest, "permission is not available for this user", nil))
	require.False(t, gatewayprovider.CompatPreviousResponseNotFound(http.StatusBadRequest, "previous_response_id is not available for this project", nil))
}

// TestForwardAsAnthropic_PreviousResponseUnavailableRetryFailureDoesNotLoop
// returns unavailable-continuation rejection from both captured exchanges. It
// checks exactly two requests, with the saved ID removed from the second body.
func TestForwardAsAnthropic_PreviousResponseUnavailableRetryFailureDoesNotLoop(t *testing.T) {
	t.Parallel()

	unavailable := func() *http.Response {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"previous_response_id is not available for this user","type":"invalid_request_error"}}`)),
		}
	}
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{unavailable(), unavailable()}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{"api_key": "sk-test", "base_url": "https://api.openai.com/v1"},
			Extra:       map[string]any{providercore.ExtraKeyResponsesContinuationSupported: true},
		},
	}
	svc.Text.Continuation.BindResponse(session.CompatResponseKey(provider.Record.ID, 0, "stable-cache-key"), "resp_missing")
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))

	// Discard both forwarding returns because this case counts recovery requests
	// and inspects continuation removal, including the retry that also fails.
	_, _ = svc.Text.Messages(context.Background(), c, provider, body, "stable-cache-key", "gpt-5.3-codex")
	require.Len(t, upstream.requests, 2)
	require.True(t, gjson.GetBytes(upstream.bodies[0], "previous_response_id").Exists())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())
}

// TestForwardAsAnthropic_DisablesAPIKeyContinuationWhenUpstreamRequiresWebSocketV2
// rejects HTTP continuation, succeeds on replay and receives another turn. It
// checks the initial ID and its continued absence in replay and later captured
// requests.
func TestForwardAsAnthropic_DisablesAPIKeyContinuationWhenUpstreamRequiresWebSocketV2(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
			Extra: map[string]any{providercore.ExtraKeyResponsesContinuationSupported: true},
		},
	}

	svc.Text.Continuation.BindResponse(session.CompatResponseKey(provider.Record.ID, 0, "stable-cache-key"), "resp_http_unsupported")
	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	upstream.responses = []*http.Response{
		{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_prev_http_unsupported"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"previous_response_id is only supported on Responses WebSocket v2","type":"invalid_request_error"}}`)),
		},
		openAICompatSSECompletedResponse("resp_replayed", "gpt-5.5"),
		openAICompatSSECompletedResponse("resp_later", "gpt-5.5"),
	}

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "stable-cache-key", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "resp_replayed", result.ResponseID)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, "resp_http_unsupported", gjson.GetBytes(upstream.bodies[0], "previous_response_id").String())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())

	laterRec := httptest.NewRecorder()
	laterCtx, _ := gin.CreateTestContext(laterRec)
	laterCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	laterCtx.Request.Header.Set("Content-Type", "application/json")

	laterResult, err := svc.Text.Messages(context.Background(), laterCtx, provider, body, "stable-cache-key", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, laterResult)
	require.Equal(t, "resp_later", laterResult.ResponseID)
	require.Len(t, upstream.requests, 3)
	require.False(t, gjson.GetBytes(upstream.bodies[2], "previous_response_id").Exists())
}

// TestForwardAsAnthropic_APIKeyMetadataSessionSurvivesChangingCacheControlAnchorAfterContinuationDisabled
// changes history and cache anchors after disabling a metadata-keyed
// continuation. It checks stable cache identity, absent previous ID and
// retained full later history with its guard.
func TestForwardAsAnthropic_APIKeyMetadataSessionSurvivesChangingCacheControlAnchorAfterContinuationDisabled(t *testing.T) {
	t.Parallel()

	metadata := `{"user_id":"{\"device_id\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"account_uuid\":\"\",\"session_id\":\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\"}"}`
	firstBody := []byte(`{"model":"claude-haiku-4-5-20251001","max_tokens":16,"metadata":` + metadata + `,"system":[{"type":"text","text":"project docs","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"first"}],"stream":false}`)
	// Sixteen messages exceed the normal twelve-message suffix allowance, so
	// retained earlier history distinguishes full replay after continuation is
	// disabled.
	messages := make([]string, 0, 12+4)
	messages = append(messages, `{"role":"user","content":[{"type":"text","text":"rewritten context","cache_control":{"type":"ephemeral"}}]}`)
	for i := 1; i < 12+4; i++ {
		messages = append(messages, `{"role":"user","content":"message-`+fmt.Sprintf("%02d", i)+`"}`)
	}
	secondBody := []byte(`{"model":"claude-haiku-4-5-20251001","max_tokens":16,"metadata":` + metadata + `,"messages":[` + strings.Join(messages, ",") + `],"stream":false}`)

	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		openAICompatSSECompletedResponse("resp_first", "gpt-5.4-mini"),
		openAICompatSSECompletedResponse("resp_second", "gpt-5.4-mini"),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
			Extra: map[string]any{providercore.ExtraKeyResponsesContinuationSupported: true},
		},
	}

	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "", "gpt-5.4-mini")
	require.NoError(t, err)
	require.NotNil(t, firstResult)
	firstKey := gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").String()
	require.NotEmpty(t, firstKey)
	require.True(t, strings.HasPrefix(firstKey, "anthropic-metadata-"))

	// Disable the metadata-keyed binding before changing history and its cache
	// anchor; the next request must reuse that identity while replaying all input.
	svc.Text.Continuation.Disable(session.CompatResponseKey(provider.Record.ID, 0, firstKey))

	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "", "gpt-5.4-mini")
	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.Len(t, upstream.requests, 2)
	require.Equal(t, firstKey, gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").String())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())
	require.Equal(t, int64(12+5), gjson.GetBytes(upstream.bodies[1], "input.#").Int())
	require.Equal(t, "developer", gjson.GetBytes(upstream.bodies[1], "input.0.role").String())
	require.Contains(t, gjson.GetBytes(upstream.bodies[1], "input.0.content.0.text").String(), "<tokenrouter-claude-code-todo-guard>")
	require.Equal(t, "rewritten context", gjson.GetBytes(upstream.bodies[1], "input.1.content.0.text").String())
	require.Equal(t, "message-15", gjson.GetBytes(upstream.bodies[1], "input.16.content.0.text").String())
}

// TestForwardAsAnthropic_DoesNotAttachPreviousResponseIDForOAuthCompat seeds
// continuation storage for an OAuth target. It checks successful Messages
// execution and omission of the saved previous_response_id from the outgoing
// request.
func TestForwardAsAnthropic_DoesNotAttachPreviousResponseIDForOAuthCompat(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_oauth_next", "gpt-5.4")}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}
	svc.Text.Continuation.BindResponse(session.CompatResponseKey(provider.Record.ID, 0, "stable-cache-key"), "resp_oauth_prev")

	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "stable-cache-key", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, gjson.GetBytes(upstream.lastBody, "previous_response_id").Exists())
}

// TestForwardAsAnthropic_ReusesOAuthCodexTurnState returns opaque state on the
// first keyed OAuth turn. It checks its second-turn replay with stable
// isolated session identity, Codex headers and no previous-response field.
func TestForwardAsAnthropic_ReusesOAuthCodexTurnState(t *testing.T) {
	t.Parallel()

	firstResp := openAICompatSSECompletedResponse("resp_oauth_first", "gpt-5.4")
	firstResp.Header.Set("x-codex-turn-state", "turn_state_first")
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		firstResp,
		openAICompatSSECompletedResponse("resp_oauth_second", "gpt-5.4"),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	firstBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"}],"stream":false}`)
	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "stable-cache-key", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, firstResult)
	require.Empty(t, upstream.requests[0].Header.Get("x-codex-turn-state"))
	requireOpenAIMessagesCodexIdentity(t, upstream.requests[0], openai.CodexCLIUserAgent, openai.CodexDefaultOriginator)

	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "stable-cache-key", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.Equal(t, "turn_state_first", upstream.requests[1].Header.Get("x-codex-turn-state"))
	require.Equal(t, upstreamcore.GenerateSessionUUID(openai.IsolateOpenAIUpstreamSessionID(0, provideradapter.CodexIdentityNamespace(provider.View()), "stable-cache-key")), upstream.requests[1].Header.Get("session_id"))
	require.Empty(t, upstream.requests[1].Header.Get("conversation_id"))
	requireOpenAIMessagesCodexIdentity(t, upstream.requests[1], openai.CodexCLIUserAgent, openai.CodexDefaultOriginator)
	require.False(t, gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").Exists())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())
}

// TestForwardAsAnthropic_OAuthRestoresCodexIdentityHeaders compares official
// and third-party client identities with a mismatched originator. It checks
// preserved or default User-Agent plus paired originator and shared Codex
// version/beta headers.
func TestForwardAsAnthropic_OAuthRestoresCodexIdentityHeaders(t *testing.T) {
	const tuiUA = "codex-tui/9.9.9 (Mac OS X 14.0; arm64) iTerm (codex-tui; 9.9.9)"
	tests := []struct {
		name           string // fixture case name
		userAgent      string // client User-Agent value
		originator     string // client originator value
		wantUserAgent  string // expected upstream User-Agent
		wantOriginator string // expected upstream originator
	}{
		{
			name:           "官方UA逐字保留并重新配对",
			userAgent:      tuiUA,
			originator:     "opencode",
			wantUserAgent:  tuiUA,
			wantOriginator: "codex-tui",
		},
		{
			name:           "第三方UA回退为默认Codex身份",
			userAgent:      "third-party-client/1.0.0",
			originator:     "opencode",
			wantUserAgent:  openai.CodexCLIUserAgent,
			wantOriginator: openai.CodexDefaultOriginator,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
			c.Request.Header.Set("Content-Type", "application/json")
			c.Request.Header.Set("User-Agent", tt.userAgent)
			c.Request.Header.Set("originator", tt.originator)

			upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_identity", "gpt-5.4")}
			svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
			provider := &gatewayprovider.ExecutionProvider{
				Record: providercore.Record{
					LoadLocation: time.LoadLocation, ID: 1,
					Name:        "openai-oauth",
					Platform:    capability.PlatformOpenAI,
					Type:        capability.ProviderTypeOAuth,
					Concurrency: 1,
					Credentials: map[string]any{
						"access_token":       "oauth-token",
						"chatgpt_account_id": "chatgpt-acc",
					},
				},
			}

			result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.4")
			require.NoError(t, err)
			require.NotNil(t, result)
			requireOpenAIMessagesCodexIdentity(t, upstream.lastReq, tt.wantUserAgent, tt.wantOriginator)
		})
	}
}

// TestForwardAsAnthropic_OAuthDigestFallbackReusesTurnStateWithoutExplicitKey
// extends history without an explicit cache seed. It checks a stable
// digest-derived session, replayed opaque state and absent
// prompt-cache/previous-response fields.
func TestForwardAsAnthropic_OAuthDigestFallbackReusesTurnStateWithoutExplicitKey(t *testing.T) {
	t.Parallel()

	firstResp := openAICompatSSECompletedResponse("resp_oauth_digest_first", "gpt-5.4")
	firstResp.Header.Set("x-codex-turn-state", "turn_state_digest_first")
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		firstResp,
		openAICompatSSECompletedResponse("resp_oauth_digest_second", "gpt-5.4"),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	firstBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"}],"stream":false}`)
	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, firstResult)
	firstSessionID := upstream.requests[0].Header.Get("session_id")
	require.NotEmpty(t, firstSessionID)
	require.Empty(t, upstream.requests[0].Header.Get("x-codex-turn-state"))
	requireOpenAIMessagesCodexIdentity(t, upstream.requests[0], openai.CodexCLIUserAgent, openai.CodexDefaultOriginator)
	require.False(t, gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").Exists())

	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.Equal(t, firstSessionID, upstream.requests[1].Header.Get("session_id"))
	require.Equal(t, "turn_state_digest_first", upstream.requests[1].Header.Get("x-codex-turn-state"))
	require.Empty(t, upstream.requests[1].Header.Get("conversation_id"))
	requireOpenAIMessagesCodexIdentity(t, upstream.requests[1], openai.CodexCLIUserAgent, openai.CodexDefaultOriginator)
	require.False(t, gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").Exists())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())
}

// TestForwardAsAnthropic_OAuthMetadataSessionSurvivesDigestPrefixRewrite
// changes the history prefix with unchanged metadata session. It checks stable
// session identity and replayed turn state while outgoing cache and
// previous-response fields remain absent.
func TestForwardAsAnthropic_OAuthMetadataSessionSurvivesDigestPrefixRewrite(t *testing.T) {
	t.Parallel()

	firstResp := openAICompatSSECompletedResponse("resp_oauth_metadata_first", "gpt-5.5")
	firstResp.Header.Set("x-codex-turn-state", "turn_state_metadata_first")
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		firstResp,
		openAICompatSSECompletedResponse("resp_oauth_metadata_second", "gpt-5.5"),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}
	metadata := `{"user_id":"{\"device_id\":\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\",\"account_uuid\":\"\",\"session_id\":\"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa\"}"}`

	firstBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"metadata":` + metadata + `,"messages":[{"role":"user","content":"first plan"}],"stream":false}`)
	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, firstResult)
	firstSessionID := upstream.requests[0].Header.Get("session_id")
	require.NotEmpty(t, firstSessionID)
	require.Empty(t, upstream.requests[0].Header.Get("x-codex-turn-state"))
	require.False(t, gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").Exists())

	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"metadata":` + metadata + `,"messages":[{"role":"user","content":"rewritten plan"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.Equal(t, firstSessionID, upstream.requests[1].Header.Get("session_id"))
	require.Equal(t, "turn_state_metadata_first", upstream.requests[1].Header.Get("x-codex-turn-state"))
	require.Empty(t, upstream.requests[1].Header.Get("conversation_id"))
	require.False(t, gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").Exists())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())
}

// TestForwardAsAnthropic_OAuthMetadataSessionSurvivesChangingCacheControlAnchor
// changes system cache-control anchor text under one metadata session. It
// checks stable session identity, replayed turn state and absent
// cache/previous-response fields.
func TestForwardAsAnthropic_OAuthMetadataSessionSurvivesChangingCacheControlAnchor(t *testing.T) {
	t.Parallel()

	firstResp := openAICompatSSECompletedResponse("resp_oauth_cache_anchor_first", "gpt-5.5")
	firstResp.Header.Set("x-codex-turn-state", "turn_state_cache_anchor_first")
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		firstResp,
		openAICompatSSECompletedResponse("resp_oauth_cache_anchor_second", "gpt-5.5"),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}
	metadata := `{"user_id":"{\"device_id\":\"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\",\"account_uuid\":\"\",\"session_id\":\"bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb\"}"}`

	firstBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"metadata":` + metadata + `,"system":[{"type":"text","text":"anchor one","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"first"}],"stream":false}`)
	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, firstResult)
	firstSessionID := upstream.requests[0].Header.Get("session_id")
	require.NotEmpty(t, firstSessionID)
	require.Empty(t, upstream.requests[0].Header.Get("x-codex-turn-state"))
	require.False(t, gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").Exists())

	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"metadata":` + metadata + `,"system":[{"type":"text","text":"anchor two","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.Equal(t, firstSessionID, upstream.requests[1].Header.Get("session_id"))
	require.Equal(t, "turn_state_cache_anchor_first", upstream.requests[1].Header.Get("x-codex-turn-state"))
	require.Empty(t, upstream.requests[1].Header.Get("conversation_id"))
	require.False(t, gjson.GetBytes(upstream.bodies[1], "prompt_cache_key").Exists())
	require.False(t, gjson.GetBytes(upstream.bodies[1], "previous_response_id").Exists())
}

// TestForwardAsAnthropic_OAuthKeepsSystemAsDeveloperInput supplies system text
// with cache control to OAuth conversion. It checks retained developer
// content, present empty instructions and the captured Codex identity headers.
func TestForwardAsAnthropic_OAuthKeepsSystemAsDeveloperInput(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_oauth_system", "gpt-5.4")}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"system":[{"type":"text","text":"project instructions","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"first"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "developer", gjson.GetBytes(upstream.lastBody, "input.0.role").String())
	require.Equal(t, "input_text", gjson.GetBytes(upstream.lastBody, "input.0.content.0.type").String())
	require.Equal(t, "project instructions", gjson.GetBytes(upstream.lastBody, "input.0.content.0.text").String())
	instructions := gjson.GetBytes(upstream.lastBody, "instructions")
	require.True(t, instructions.Exists())
	require.Empty(t, instructions.String())
	requireOpenAIMessagesCodexIdentity(t, upstream.requests[0], openai.CodexCLIUserAgent, openai.CodexDefaultOriginator)
}

// TestForwardAsAnthropic_OAuthAddsClaudeCodeTodoGuardForCompatModel supplies
// system/user input for a compatible OAuth model. It checks the system
// developer item, separate todo-guard developer item and subsequent user role.
func TestForwardAsAnthropic_OAuthAddsClaudeCodeTodoGuardForCompatModel(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_oauth_todo_guard", "gpt-5.5")}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"system":"project instructions","messages":[{"role":"user","content":"review files"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.5")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "developer", gjson.GetBytes(upstream.lastBody, "input.0.role").String())
	require.Equal(t, "project instructions", gjson.GetBytes(upstream.lastBody, "input.0.content.0.text").String())
	require.Equal(t, "developer", gjson.GetBytes(upstream.lastBody, "input.1.role").String())
	require.Contains(t, gjson.GetBytes(upstream.lastBody, "input.1.content.0.text").String(), "<tokenrouter-claude-code-todo-guard>")
	require.Equal(t, "user", gjson.GetBytes(upstream.lastBody, "input.2.role").String())
}

// TestForwardAsAnthropic_OAuthPreservesClaudeCodeToolCallID converts paired
// Claude tool-use and result history. It checks the retained identifier in
// both function nodes and outgoing parallel, verbosity and strictness
// settings.
func TestForwardAsAnthropic_OAuthPreservesClaudeCodeToolCallID(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{resp: openAICompatSSECompletedResponse("resp_oauth_tool", "gpt-5.4")}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"list files"},{"role":"assistant","content":[{"type":"tool_use","id":"toolu_123","name":"Bash","input":{"command":"ls"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_123","content":"ok"}]}],"tools":[{"name":"Bash","description":"run shell","input_schema":{"type":"object","properties":{"command":{"type":"string"}}}}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "stable-cache-key", "gpt-5.4")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "toolu_123", gjson.GetBytes(upstream.lastBody, `input.#(type=="function_call").call_id`).String())
	require.Equal(t, "toolu_123", gjson.GetBytes(upstream.lastBody, `input.#(type=="function_call_output").call_id`).String())
	require.True(t, gjson.GetBytes(upstream.lastBody, "parallel_tool_calls").Bool())
	require.Equal(t, "medium", gjson.GetBytes(upstream.lastBody, "text.verbosity").String())
	require.False(t, gjson.GetBytes(upstream.lastBody, "tools.0.strict").Bool())
}

// TestForwardAsAnthropic_StoresStreamingResponseIDWithoutUsage returns a
// successful terminal ID with no usage on the first API-key stream. It checks
// that the second turn attaches that first ID as continuation.
func TestForwardAsAnthropic_StoresStreamingResponseIDWithoutUsage(t *testing.T) {
	t.Parallel()

	upstream := &auxiliaryHTTPRecorder{}
	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream, options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-apikey",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":  "sk-test",
				"base_url": "https://api.openai.com/v1",
			},
			Extra: map[string]any{providercore.ExtraKeyResponsesContinuationSupported: true},
		},
	}

	firstBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"}],"stream":true}`)
	upstream.resp = openAICompatSSEResponseWithoutUsage("resp_stream_first", "gpt-5.3-codex")
	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, firstResult)
	require.Equal(t, "resp_stream_first", firstResult.ResponseID)

	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"first"},{"role":"assistant","content":"ok"},{"role":"user","content":"second"}],"stream":false}`)
	upstream.resp = openAICompatSSECompletedResponse("resp_stream_second", "gpt-5.3-codex")
	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "stable-cache-key", "gpt-5.3-codex")
	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.Equal(t, "resp_stream_first", gjson.GetBytes(upstream.lastBody, "previous_response_id").String())
}

// openAICompatSSECompletedResponse builds a completed Responses fixture with
// token usage and DONE. Callers provide model and ID values safe for direct
// JSON interpolation and close the returned response body after its reader
// finishes.
func openAICompatSSECompletedResponse(responseID, model string) *http.Response {
	body := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"` + responseID + `","object":"response","model":"` + model + `","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_continuation"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// requireOpenAIMessagesCodexIdentity asserts User-Agent, originator, version
// and beta on a nonnil request.
func requireOpenAIMessagesCodexIdentity(t *testing.T, req *http.Request, wantUserAgent, wantOriginator string) {
	t.Helper()
	require.NotNil(t, req)
	require.Equal(t, wantUserAgent, req.Header.Get("User-Agent"))
	require.Equal(t, wantOriginator, req.Header.Get("originator"))
	require.Equal(t, openai.CodexCLIVersion, req.Header.Get("version"))
	require.Equal(t, "responses=experimental", req.Header.Get("OpenAI-Beta"))
}

// openAICompatSSEResponseWithoutUsage builds a completed Responses fixture
// with text and DONE but no usage. Model and response ID are interpolated
// directly, so callers supply JSON-safe values and retain body cleanup.
func openAICompatSSEResponseWithoutUsage(responseID, model string) *http.Response {
	body := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"` + responseID + `","object":"response","model":"` + model + `","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}]}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_" + responseID}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// TestForwardAsAnthropic_ForcedCodexInstructionsTemplatePrependsRenderedInstructions
// supplies a template string and client system text to the fixture executor.
// It checks the exact rendered server prefix followed by client instructions.
func TestForwardAsAnthropic_ForcedCodexInstructionsTemplatePrependsRenderedInstructions(t *testing.T) {
	t.Parallel()

	// The file is fixture setup; the executor below receives matching
	// ForcedTemplate text directly, so these rendering assertions do not exercise
	// template-file loading.
	templateDir := t.TempDir()
	templatePath := filepath.Join(templateDir, "codex-instructions.md.tmpl")
	require.NoError(t, os.WriteFile(templatePath, []byte("server-prefix\n\n{{ .ExistingInstructions }}"), 0o644))

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"system":"client-system","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_forced"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{ForcedTemplate: "server-prefix\n\n{{ .ExistingInstructions }}"}, transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "server-prefix\n\nclient-system", gjson.GetBytes(upstream.lastBody, "instructions").String())
}

// TestForwardAsAnthropic_ForcedCodexInstructionsTemplateUsesCachedTemplateContent
// supplies cached template text and client system instructions. It checks the
// exact outgoing rendered instructions from those configured fixture values.
func TestForwardAsAnthropic_ForcedCodexInstructionsTemplateUsesCachedTemplateContent(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"system":"client-system","messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_forced_cached"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{ForcedTemplate: "cached-prefix\n\n{{ .ExistingInstructions }}"}, transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "cached-prefix\n\nclient-system", gjson.GetBytes(upstream.lastBody, "instructions").String())
}

// TestForwardAsAnthropic_ClientDisconnectDrainsUpstreamUsage rejects client
// writes before terminal usage arrives from Responses. It checks successful
// returned observations with the exact input, output and cache-read
// quantities.
func TestForwardAsAnthropic_ClientDisconnectDrainsUpstreamUsage(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Writer = &openAICompatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13,"input_tokens_details":{"cached_tokens":3}}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_disconnect"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 9, result.Usage.InputTokens)
	require.Equal(t, 4, result.Usage.OutputTokens)
	require.Equal(t, 3, result.Usage.CacheReadInputTokens)
}

// TestForwardAsAnthropic_TerminalUsageWithoutUpstreamCloseReturns blocks
// source reads after terminal usage while client output fails. It checks
// timely return and captured usage without requiring EOF before executor
// cleanup closes the source.
func TestForwardAsAnthropic_TerminalUsageWithoutUpstreamCloseReturns(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Writer = &openAICompatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":15,"output_tokens":6,"total_tokens":21,"input_tokens_details":{"cached_tokens":5}}}}` + "\n\n")
	// Block reads after the terminal bytes to distinguish completion from EOF;
	// deferred closure releases any remaining read if the test wait times out.
	upstreamStream := gatewaytestkit.NewBlockingReadCloser(upstreamBody)
	defer func() {
		require.NoError(t, upstreamStream.Close())
	}()
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_terminal_no_close"}},
		Body:       upstreamStream,
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	// forwardResult carries one worker's forwarding observation and error to the
	// waiting test.
	type forwardResult struct {
		// result and err are captured by the same forwarding worker and published
		// together in one channel value, so assertions inspect the observation
		// associated with that operation's returned failure.
		result *forwardcore.OpenAIResult // forwarding result observation
		err    error                     // forwarding operation error
	}
	// A buffered result lets the worker publish after a wait timeout; deferred
	// body closure releases the blocking fixture if terminal-driven return did not occur.
	resultCh := make(chan forwardResult, 1)
	go func() {
		result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
		resultCh <- forwardResult{result: result, err: err}
	}()

	// The one-second guard turns waiting for unprovided EOF into an assertion
	// failure, after which deferred source closure releases blocked reads.
	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.result)
		require.Equal(t, 15, got.result.Usage.InputTokens)
		require.Equal(t, 6, got.result.Usage.OutputTokens)
		require.Equal(t, 5, got.result.Usage.CacheReadInputTokens)
	case <-time.After(time.Second):
		require.Fail(t, "ForwardAsAnthropic should return after terminal usage event even if upstream keeps the connection open")
	}
}

// TestForwardAsAnthropic_EventNamedTerminalWithoutUpstreamCloseReturns names a
// terminal outside JSON type and then blocks reads. It checks timely return
// with the fixture token quantities while client output is rejected.
func TestForwardAsAnthropic_EventNamedTerminalWithoutUpstreamCloseReturns(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Writer = &openAICompatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(strings.Join([]string{
		`event: response.completed`,
		`data: {"response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":15,"output_tokens":6,"total_tokens":21,"input_tokens_details":{"cached_tokens":5}}}}`,
		``,
		``,
	}, "\n"))
	// Block reads after the terminal bytes to distinguish completion from EOF;
	// deferred closure releases any remaining read if the test wait times out.
	upstreamStream := gatewaytestkit.NewBlockingReadCloser(upstreamBody)
	defer func() {
		require.NoError(t, upstreamStream.Close())
	}()
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_messages_event_named_terminal"}},
		Body:       upstreamStream,
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	// forwardResult carries one worker's forwarding observation and error to the
	// waiting test.
	type forwardResult struct {
		// result and err are captured by the same forwarding worker and published
		// together in one channel value, so assertions inspect the observation
		// associated with that operation's returned failure.
		result *forwardcore.OpenAIResult // forwarding result observation
		err    error                     // forwarding operation error
	}
	// A buffered result lets the worker publish after a wait timeout; deferred
	// body closure releases the blocking fixture if terminal-driven return did not occur.
	resultCh := make(chan forwardResult, 1)
	go func() {
		result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
		resultCh <- forwardResult{result: result, err: err}
	}()

	// The one-second guard turns waiting for unprovided EOF into an assertion
	// failure, after which deferred source closure releases blocked reads.
	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.result)
		require.Equal(t, 15, got.result.Usage.InputTokens)
		require.Equal(t, 6, got.result.Usage.OutputTokens)
		require.Equal(t, 5, got.result.Usage.CacheReadInputTokens)
	case <-time.After(time.Second):
		require.Fail(t, "ForwardAsAnthropic should use SSE event names when data payloads omit type")
	}
}

// TestForwardAsAnthropic_EventNamedTerminalWithKeepaliveReturns combines an
// upstream ping, named terminal and configured keepalive path. It checks
// timely returned usage from a blocking body while downstream writes are
// rejected; an actual keepalive beat is not asserted.
func TestForwardAsAnthropic_EventNamedTerminalWithKeepaliveReturns(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Writer = &openAICompatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(strings.Join([]string{
		`: upstream ping`,
		``,
		`event: response.completed`,
		`data: {"response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":15,"output_tokens":6,"total_tokens":21,"input_tokens_details":{"cached_tokens":5}}}}`,
		``,
		``,
	}, "\n"))
	// Block reads after the terminal bytes to distinguish completion from EOF;
	// deferred closure releases any remaining read if the test wait times out.
	upstreamStream := gatewaytestkit.NewBlockingReadCloser(upstreamBody)
	defer func() {
		require.NoError(t, upstreamStream.Close())
	}()
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_messages_event_named_keepalive"}},
		Body:       upstreamStream,
	}}

	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{Response: OpenAIResponseOptions{}}, transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	// forwardResult carries one worker's forwarding observation and error to the
	// waiting test.
	type forwardResult struct {
		// result and err are captured by the same forwarding worker and published
		// together in one channel value, so assertions inspect the observation
		// associated with that operation's returned failure.
		result *forwardcore.OpenAIResult // forwarding result observation
		err    error                     // forwarding operation error
	}
	// A buffered result lets the worker publish after a wait timeout; deferred
	// body closure releases the blocking fixture if terminal-driven return did not occur.
	resultCh := make(chan forwardResult, 1)
	go func() {
		result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
		resultCh <- forwardResult{result: result, err: err}
	}()

	// The one-second guard turns waiting for unprovided EOF into an assertion
	// failure, after which deferred source closure releases blocked reads.
	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.result)
		require.Equal(t, 15, got.result.Usage.InputTokens)
		require.Equal(t, 6, got.result.Usage.OutputTokens)
		require.Equal(t, 5, got.result.Usage.CacheReadInputTokens)
	case <-time.After(time.Second):
		require.Fail(t, "ForwardAsAnthropic keepalive path should use SSE event names when data payloads omit type")
	}
}

// TestForwardAsAnthropic_BufferedTerminalWithoutUpstreamCloseReturns blocks
// reads after a completed Response for nonstreaming Messages. It checks timely
// return, usage observations and end_turn in the buffered client output.
func TestForwardAsAnthropic_BufferedTerminalWithoutUpstreamCloseReturns(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":15,"output_tokens":6,"total_tokens":21,"input_tokens_details":{"cached_tokens":5}}}}` + "\n\n")
	// Block reads after the terminal bytes to distinguish completion from EOF;
	// deferred closure releases any remaining read if the test wait times out.
	upstreamStream := gatewaytestkit.NewBlockingReadCloser(upstreamBody)
	defer func() {
		require.NoError(t, upstreamStream.Close())
	}()
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_buffered_terminal_no_close"}},
		Body:       upstreamStream,
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	// forwardResult carries one worker's forwarding observation and error to the
	// waiting test.
	type forwardResult struct {
		// result and err are captured by the same forwarding worker and published
		// together in one channel value, so assertions inspect the observation
		// associated with that operation's returned failure.
		result *forwardcore.OpenAIResult // forwarding result observation
		err    error                     // forwarding operation error
	}
	// A buffered result lets the worker publish after a wait timeout; deferred
	// body closure releases the blocking fixture if terminal-driven return did not occur.
	resultCh := make(chan forwardResult, 1)
	go func() {
		result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
		resultCh <- forwardResult{result: result, err: err}
	}()

	// The one-second guard turns waiting for unprovided EOF into an assertion
	// failure, after which deferred source closure releases blocked reads.
	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.result)
		require.Equal(t, 15, got.result.Usage.InputTokens)
		require.Equal(t, 6, got.result.Usage.OutputTokens)
		require.Equal(t, 5, got.result.Usage.CacheReadInputTokens)
		require.Contains(t, rec.Body.String(), `"stop_reason":"end_turn"`)
	case <-time.After(time.Second):
		require.Fail(t, "ForwardAsAnthropic buffered response should return after terminal usage event even if upstream keeps the connection open")
	}
}

// TestForwardAsAnthropic_BufferedEventNamedTerminalWithoutUpstreamCloseReturns
// names the completed event outside its JSON type for buffered Messages. It
// checks timely usage return and end_turn without requiring EOF before
// executor cleanup closes the blocking body.
func TestForwardAsAnthropic_BufferedEventNamedTerminalWithoutUpstreamCloseReturns(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(strings.Join([]string{
		`event: response.completed`,
		`data: {"response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":15,"output_tokens":6,"total_tokens":21,"input_tokens_details":{"cached_tokens":5}}}}`,
		``,
		``,
	}, "\n"))
	// Block reads after the terminal bytes to distinguish completion from EOF;
	// deferred closure releases any remaining read if the test wait times out.
	upstreamStream := gatewaytestkit.NewBlockingReadCloser(upstreamBody)
	defer func() {
		require.NoError(t, upstreamStream.Close())
	}()
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_messages_buffered_event_named"}},
		Body:       upstreamStream,
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	// forwardResult carries one worker's forwarding observation and error to the
	// waiting test.
	type forwardResult struct {
		// result and err are captured by the same forwarding worker and published
		// together in one channel value, so assertions inspect the observation
		// associated with that operation's returned failure.
		result *forwardcore.OpenAIResult // forwarding result observation
		err    error                     // forwarding operation error
	}
	// A buffered result lets the worker publish after a wait timeout; deferred
	// body closure releases the blocking fixture if terminal-driven return did not occur.
	resultCh := make(chan forwardResult, 1)
	go func() {
		result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
		resultCh <- forwardResult{result: result, err: err}
	}()

	// The one-second guard turns waiting for unprovided EOF into an assertion
	// failure, after which deferred source closure releases blocked reads.
	select {
	case got := <-resultCh:
		require.NoError(t, got.err)
		require.NotNil(t, got.result)
		require.Equal(t, 15, got.result.Usage.InputTokens)
		require.Equal(t, 6, got.result.Usage.OutputTokens)
		require.Equal(t, 5, got.result.Usage.CacheReadInputTokens)
		require.Contains(t, rec.Body.String(), `"stop_reason":"end_turn"`)
	case <-time.After(time.Second):
		require.Fail(t, "ForwardAsAnthropic buffered response should use SSE event names when data payloads omit type")
	}
}

// TestForwardAsAnthropic_MissingTerminalBeforeOutputReturnsFailoverAndOps
// supplies only DONE before Messages content starts. It checks typed recovery,
// zero usage, untouched client output and one failover Ops event with
// provider/request identity.
func TestForwardAsAnthropic_MissingTerminalBeforeOutputReturnsFailoverAndOps(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := "data: [DONE]\n\n"
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"rid_missing_terminal"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
	require.Error(t, err)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr), "missing terminal before output must use failover path")
	require.Equal(t, http.StatusBadGateway, failoverErr.StatusCode)
	require.Contains(t, string(failoverErr.ResponseBody), "OpenAI converted stream ended before a terminal event")
	require.Nil(t, result)
	require.False(t, c.Writer.Written(), "no client body/header should be committed before safe failover")
	require.Empty(t, rec.Body.String())

	events := openAICompatOpsEvents(t, c)
	require.Len(t, events, 1)
	require.Equal(t, "failover", events[0].Kind)
	require.Equal(t, http.StatusBadGateway, events[0].UpstreamStatusCode)
	require.Equal(t, int64(1), events[0].ProviderID)
	require.Equal(t, "rid_missing_terminal", events[0].UpstreamRequestID)
	require.Contains(t, events[0].Message, "terminal event")
}

// TestForwardAsAnthropic_MissingTerminalAfterOutputRecordsOpsWithoutFailover
// ends after partial Messages content. It checks partial result/output, no
// failover wrapper and one missing-terminal Ops event tied to the captured
// request.
func TestForwardAsAnthropic_MissingTerminalAfterOutputRecordsOpsWithoutFailover(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"partial"}`,
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"rid_partial_missing_terminal"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing terminal event")
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr), "partial output must not be replayed through failover")
	require.NotNil(t, result)
	require.False(t, result.ClientDisconnect)
	require.True(t, c.Writer.Written())
	require.Contains(t, rec.Body.String(), "event: message_start")
	require.Contains(t, rec.Body.String(), "partial")

	events := openAICompatOpsEvents(t, c)
	require.Len(t, events, 1)
	require.Equal(t, "stream_missing_terminal", events[0].Kind)
	require.Equal(t, http.StatusBadGateway, events[0].UpstreamStatusCode)
	require.Equal(t, int64(1), events[0].ProviderID)
	require.Equal(t, "rid_partial_missing_terminal", events[0].UpstreamRequestID)
	require.Contains(t, events[0].Message, "terminal event")
}

// TestForwardAsAnthropic_MissingTerminalAfterClientDisconnectSkipsOpsAndFailover
// rejects client writes and ends after a preamble. It checks a disconnect
// observation with missing-terminal error, no failover wrapper and no
// upstream-error Ops key.
func TestForwardAsAnthropic_MissingTerminalAfterClientDisconnectSkipsOpsAndFailover(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Writer = &openAICompatFailingWriter{ResponseWriter: c.Writer, failAfter: 0}
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"partial"}`,
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"rid_client_disconnect_missing_terminal"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing terminal event")
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.NotNil(t, result)
	require.True(t, result.ClientDisconnect)
	require.Empty(t, rec.Body.String())
	_, ok := c.Get(OpsUpstreamErrorsKey)
	require.False(t, ok, "client disconnect must not be attributed as an upstream error")
}

// TestForwardAsAnthropic_CompleteStreamDoesNotRecordMissingTerminalOps
// supplies content and a successful usage terminal. It checks returned token
// quantities, downstream message_stop and absence of the upstream-error Ops
// key.
func TestForwardAsAnthropic_CompleteStreamDoesNotRecordMissingTerminalOps(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_1","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"ok"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":9,"output_tokens":4,"total_tokens":13}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"rid_complete_terminal"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 9, result.Usage.InputTokens)
	require.Equal(t, 4, result.Usage.OutputTokens)
	require.Contains(t, rec.Body.String(), "event: message_stop")
	_, ok := c.Get(OpsUpstreamErrorsKey)
	require.False(t, ok)
}

// openAICompatOpsEvents requires presence and slice type for the Gin
// upstream-error value, then returns that request-owned slice. Callers inspect
// it after forwarding; event pointers and backing slice storage remain shared.
func openAICompatOpsEvents(t *testing.T, c *gin.Context) []*ops.OpsUpstreamErrorEvent {
	t.Helper()
	v, ok := c.Get(OpsUpstreamErrorsKey)
	require.True(t, ok)
	events, ok := v.([]*ops.OpsUpstreamErrorEvent)
	require.True(t, ok)
	return events
}

// TestForwardAsAnthropic_UpstreamRequestIgnoresClientCancel cancels the client
// before converted forwarding and supplies a completed fixture response. It
// checks successful result and an uncancelled captured upstream request
// context.
func TestForwardAsAnthropic_UpstreamRequestIgnoresClientCancel(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	reqCtx, cancel := context.WithCancel(context.Background())
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body)).WithContext(reqCtx)
	c.Request.Header.Set("Content-Type", "application/json")
	// Cancel before forwarding to exercise the injected cancellation path while
	// the fixture controls the transport result independently of network progress.
	cancel()

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_ctx"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}

	svc := newResponsesFixture(responsesFixtureInputs{transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1,
			Name:        "openai-oauth",
			Platform:    capability.PlatformOpenAI,
			Type:        capability.ProviderTypeOAuth,
			Concurrency: 1,
			Credentials: map[string]any{
				"access_token":       "oauth-token",
				"chatgpt_account_id": "chatgpt-acc",
			},
		},
	}

	result, err := svc.Text.Messages(reqCtx, c, provider, body, "", "gpt-5.1")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, upstream.lastReq)
	require.NoError(t, upstream.lastReq.Context().Err())
}

// TestForwardGrokMessagesDropsRedundantViewImage converts inline image
// Messages to Grok Responses. It checks the endpoint, converted image content
// and outgoing tool policy through the shared inline-image assertions.
func TestForwardGrokMessagesDropsRedundantViewImage(t *testing.T) {
	body := []byte(`{
		"model":"grok-4.6","max_tokens":32,"stream":false,
		"messages":[{"role":"user","content":[
			{"type":"text","text":"What text is in this image?"},
			{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AA=="}}
		]}],
		"tools":[
			{"name":"view_image","input_schema":{"type":"object","properties":{"path":{"type":"string"}}}},
			{"name":"shell_command","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}}}}
		],
		"tool_choice":{"type":"auto"}
	}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 7992})

	provider := gatewaytestkit.HealthyGrokOAuthProvider(801, "access-token")
	repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{
		providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
	}}
	upstream := &auxiliaryHTTPRecorder{resp: grokMessagesSSECompletedResponse("resp_messages_image", 0)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	require.Equal(t, "input_image", gjson.GetBytes(upstream.lastBody, "input.0.content.1.type").String())
	assertGrokInlineImageTools(t, upstream.lastBody, "tools.#(name==\"%s\")")
}

// TestForwardAsAnthropicForGrokUsesXAIResponses converts Messages under keyed
// Grok OAuth execution. It checks Grok endpoint and CLI headers, isolated
// cache identity, tools, separate client/upstream model and converted
// cache-read usage.
func TestForwardAsAnthropicForGrokUsesXAIResponses(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"grok-4.5","max_tokens":32,"stream":false,"messages":[{"role":"user","content":"hi"}]}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("OpenAI-Beta", "grok-experimental")
	c.Request.Header.Set("originator", "opencode")
	c.Set("api_key", &apikey.APIKey{ID: 5401})

	provider := gatewaytestkit.HealthyGrokOAuthProvider(54, "access-token")
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{54: provider},
		},
	}
	upstream := &auxiliaryHTTPRecorder{resp: grokMessagesSSECompletedResponse("resp_grok_messages", 3)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	require.Equal(t, "Bearer access-token", upstream.lastReq.Header.Get("Authorization"))
	require.Equal(t, grok.CLIUserAgent(grok.CLIClientVersion), upstream.lastReq.Header.Get("User-Agent"))
	require.Equal(t, "grok-experimental", upstream.lastReq.Header.Get("OpenAI-Beta"))
	require.Empty(t, upstream.lastReq.Header.Get("originator"))
	require.Empty(t, upstream.lastReq.Header.Get("version"))
	require.Equal(t, grok.CLIClientVersion, upstream.lastReq.Header.Get("X-Grok-Client-Version"))
	require.Equal(t, "grok-4.5", gjson.GetBytes(upstream.lastBody, "model").String())
	require.NotEmpty(t, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String())
	require.Equal(t, gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String(), upstream.lastReq.Header.Get(GrokConversationIDHeader))
	require.Equal(t, "web_search", gjson.GetBytes(upstream.lastBody, "tools.0.type").String())
	require.Equal(t, "x_search", gjson.GetBytes(upstream.lastBody, "tools.1.type").String())
	require.Equal(t, "none", gjson.GetBytes(upstream.lastBody, "tool_choice").String())
	require.Empty(t, upstream.lastReq.Header.Get("session_id"))
	require.True(t, gjson.GetBytes(upstream.lastBody, "stream").Bool())
	require.NotContains(t, string(upstream.lastBody), "chatgpt.com")
	require.Equal(t, "grok-4.5", result.Model)
	require.Equal(t, "grok-4.5", result.BillingModel)
	require.Equal(t, "grok-4.5", result.UpstreamModel)
	require.Equal(t, 5, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	require.Equal(t, 3, result.Usage.CacheReadInputTokens)
	require.Contains(t, recorder.Body.String(), `"type":"message"`)
	require.Equal(t, int64(3), gjson.Get(recorder.Body.String(), "usage.cache_read_input_tokens").Int())
	require.Contains(t, recorder.Body.String(), "ok")
}

// TestForwardAsAnthropicForGrokRetriesInvalidEncryptedContentOnce returns a
// ciphertext rejection before success on the same route. It checks two
// requests with encrypted reasoning present initially and absent on retry,
// plus the final answer.
func TestForwardAsAnthropicForGrokRetriesInvalidEncryptedContentOnce(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	xaiCarrier, carrierErr := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeXAI,
		Data:       "enc-old-provider",
		ProviderID: 59,
	})
	require.NoError(t, carrierErr)
	body := []byte(`{
		"model":"grok-4.5","max_tokens":32,"stream":false,
		"messages":[
			{"role":"user","content":"plan a command"},
			{"role":"assistant","content":[{"type":"thinking","thinking":"plan","signature":"` + xaiCarrier + `"},{"type":"text","text":"run it"}]},
			{"role":"user","content":"continue"}
		]
	}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 5404})

	provider := gatewaytestkit.HealthyGrokOAuthProvider(59, "access-token")
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{59: provider},
		},
	}
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"Could not decrypt the provided encrypted_content."}}`)),
		},
		grokMessagesSSECompletedResponse("resp_grok_messages_retry", 1),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, upstream.requests, 2)
	require.True(t, gatewayprovider.GrokBodyCodec().RequestHasGrokEncryptedReasoning(upstream.bodies[0]))
	require.False(t, gatewayprovider.GrokBodyCodec().RequestHasGrokEncryptedReasoning(upstream.bodies[1]))
	require.Equal(t, upstream.requests[0].URL.String(), upstream.requests[1].URL.String())
	require.Contains(t, recorder.Body.String(), "ok")
}

// TestForwardAsAnthropicForGrokFunctionToolUsesCacheCapableMixedRoute combines
// client functions with hosted searches for a keyed Grok request. It checks
// isolated cache identity, declarations, restored tool use and separated
// uncached/cached token quantities.
func TestForwardAsAnthropicForGrokFunctionToolUsesCacheCapableMixedRoute(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{
		"model":"grok-4.5","max_tokens":32,"stream":false,
		"messages":[{"role":"user","content":"look up alpha"}],
		"tools":[{"name":"lookup","description":"look up a key","input_schema":{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}},{"name":"web_search","description":"search the web","input_schema":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}}],
		"tool_choice":{"type":"auto"}
	}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 5403})

	provider := gatewaytestkit.HealthyGrokOAuthProvider(58, "access-token")
	provider.Record.Extra = map[string]any{providercore.GrokUsageBillingExtraKey: map[string]any{
		"status_code":        http.StatusOK,
		"source":             "billing_probe",
		"monthly_updated_at": "2026-07-15T05:00:00Z",
	}}
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{58: provider},
		},
	}
	responseBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_grok_function","object":"response","model":"grok-4.5","status":"completed","output":[{"type":"function_call","id":"fc_lookup","call_id":"call_lookup","name":"lookup","arguments":"{\"key\":\"alpha\"}","status":"completed"}],"usage":{"input_tokens":7000,"output_tokens":2,"total_tokens":7002,"input_tokens_details":{"cached_tokens":6144}}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(responseBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, grok.DefaultCLIBaseURL+"/responses", upstream.lastReq.URL.String())
	identity := gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String()
	require.NotEmpty(t, identity)
	require.Equal(t, identity, upstream.lastReq.Header.Get(GrokConversationIDHeader))
	tools := gjson.GetBytes(upstream.lastBody, "tools").Array()
	require.Len(t, tools, 3)
	require.Equal(t, "function", tools[0].Get("type").String())
	require.Equal(t, "lookup", tools[0].Get("name").String())
	require.Equal(t, "object", tools[0].Get("parameters.type").String())
	require.Equal(t, "web_search", tools[1].Get("type").String())
	require.Equal(t, "x_search", tools[2].Get("type").String())
	require.Equal(t, "auto", gjson.GetBytes(upstream.lastBody, "tool_choice").String())

	require.Equal(t, 7000, result.Usage.InputTokens)
	require.Equal(t, 6144, result.Usage.CacheReadInputTokens)
	clientBody := recorder.Body.String()
	require.Equal(t, "tool_use", gjson.Get(clientBody, "content.0.type").String())
	require.Equal(t, "call_lookup", gjson.Get(clientBody, "content.0.id").String())
	require.Equal(t, "lookup", gjson.Get(clientBody, "content.0.name").String())
	require.Equal(t, "alpha", gjson.Get(clientBody, "content.0.input.key").String())
	require.Equal(t, "tool_use", gjson.Get(clientBody, "stop_reason").String())
	require.Equal(t, int64(856), gjson.Get(clientBody, "usage.input_tokens").Int())
	require.Equal(t, int64(6144), gjson.Get(clientBody, "usage.cache_read_input_tokens").Int())
}

// TestForwardAsAnthropicForGrokStreamingPreservesCacheUsage converts a keyed
// Grok Response into Messages SSE. It checks stable body/header identity,
// returned cached usage and the corresponding client cache-read token field.
func TestForwardAsAnthropicForGrokStreamingPreservesCacheUsage(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	body := []byte(`{"model":"grok-4.5","max_tokens":32,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Set("api_key", &apikey.APIKey{ID: 5402})

	provider := gatewaytestkit.HealthyGrokOAuthProvider(57, "access-token")
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{57: provider},
		},
	}
	upstream := &auxiliaryHTTPRecorder{resp: grokMessagesSSECompletedResponse("resp_grok_messages_stream", 2)}
	svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.Usage.CacheReadInputTokens)
	identity := gjson.GetBytes(upstream.lastBody, "prompt_cache_key").String()
	require.NotEmpty(t, identity)
	require.Equal(t, identity, upstream.lastReq.Header.Get(GrokConversationIDHeader))
	require.Contains(t, recorder.Header().Get("Content-Type"), "text/event-stream")
	require.Contains(t, recorder.Body.String(), `"cache_read_input_tokens":2`)
}

// nativeAnthropicTestProvider creates a Kimi API-key target with a native
// Anthropic base.
func nativeAnthropicTestProvider() *gatewayprovider.ExecutionProvider {
	return &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 702,
			Name:        "kimi-native",
			Platform:    capability.PlatformKimi,
			Type:        capability.ProviderTypeAPIKey,
			Concurrency: 1,
			Credentials: map[string]any{
				"api_key":      "sk-test",
				"api_protocol": providercore.APIProtocolAnthropic,
				"api_base_urls": map[string]any{
					providercore.APIProtocolAnthropic: "http://anthropic.example",
				},
			},
		},
	}
}

// nativeAnthropicGLMTestProvider creates a Zhipu variant of the native
// Anthropic fixture.
func nativeAnthropicGLMTestProvider() *gatewayprovider.ExecutionProvider {
	provider := nativeAnthropicTestProvider()
	provider.Record.Name = "zhipu-native"
	provider.Record.Platform = capability.PlatformZhipu
	return provider
}

// nativeAnthropicBufferedResponse builds a Kimi Messages JSON fixture with
// pong, end_turn, input 93 and output 16. Each call returns a fresh body for the
// executing fixture to consume and close.
func nativeAnthropicBufferedResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(
			`{"id":"msg_1","type":"message","role":"assistant","model":"k3",` +
				`"content":[{"type":"text","text":"pong"}],"stop_reason":"end_turn",` +
				`"usage":{"input_tokens":93,"output_tokens":16}}`,
		)),
	}
}

// nativeAnthropicStreamResponse builds a Kimi Messages stream from
// message_start through message_stop with pong text. Initial input 93 and
// output 1 precede terminal output 16; each call returns a fresh body that its
// executor consumes and closes.
func nativeAnthropicStreamResponse() *http.Response {
	sse := `event: message_start
data: {"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"k3","content":[],"stop_reason":null,"usage":{"input_tokens":93,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"pong"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":16}}

event: message_stop
data: {"type":"message_stop"}

`
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(sse)),
	}
}

// TestNativeAnthropicPassthroughRecordsOutputConfigEffort supplies low
// output_config effort to native Kimi Messages. It checks the observed
// upstream model and recorded low reasoning effort in the returned result.
func TestNativeAnthropicPassthroughRecordsOutputConfigEffort(t *testing.T) {
	body := []byte(`{"model":"k3","max_tokens":32,"stream":false,` +
		`"output_config":{"effort":"low"},` +
		`"messages":[{"role":"user","content":"hi"}]}`)
	upstream := &auxiliaryHTTPRecorder{resp: nativeAnthropicBufferedResponse()}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(),
		adaptiveProtocolTestContext("/v1/messages", body), nativeAnthropicTestProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "k3", result.UpstreamResponseModel)
	require.NotNil(t, result.ReasoningEffort)
	require.Equal(t, "low", *result.ReasoningEffort)
}

// TestNativeAnthropicPassthroughThinkingEnabledFallback supplies enabled
// thinking with no explicit effort to native Kimi. It checks the observed
// model and derived high effort in the returned result.
func TestNativeAnthropicPassthroughThinkingEnabledFallback(t *testing.T) {
	// With thinking enabled and effort omitted, the passback-required model k3 uses high.
	body := []byte(`{"model":"k3","max_tokens":32,"stream":false,` +
		`"thinking":{"type":"enabled","budget_tokens":1024},` +
		`"messages":[{"role":"user","content":"hi"}]}`)
	upstream := &auxiliaryHTTPRecorder{resp: nativeAnthropicBufferedResponse()}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(),
		adaptiveProtocolTestContext("/v1/messages", body), nativeAnthropicTestProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "k3", result.UpstreamResponseModel)
	require.NotNil(t, result.ReasoningEffort)
	require.Equal(t, "high", *result.ReasoningEffort)
}

// TestNativeAnthropicPassthroughStreamRecordsEffort supplies max effort to a
// complete native Messages stream. It checks successful observation of the
// upstream model and max reasoning effort in the result.
func TestNativeAnthropicPassthroughStreamRecordsEffort(t *testing.T) {
	body := []byte(`{"model":"k3","max_tokens":32,"stream":true,` +
		`"output_config":{"effort":"max"},` +
		`"messages":[{"role":"user","content":"hi"}]}`)
	upstream := &auxiliaryHTTPRecorder{resp: nativeAnthropicStreamResponse()}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(),
		adaptiveProtocolTestContext("/v1/messages", body), nativeAnthropicTestProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "k3", result.UpstreamResponseModel)
	require.NotNil(t, result.ReasoningEffort)
	require.Equal(t, "max", *result.ReasoningEffort)
}

// TestNativeAnthropicPassthroughNoEffortStaysNil supplies neither output
// effort nor enabled thinking to native Kimi. It checks a successful result
// with observed upstream model and absent reasoning-effort attribution.
func TestNativeAnthropicPassthroughNoEffortStaysNil(t *testing.T) {
	// Omitting output_config.effort and enabled thinking leaves requested-effort
	// attribution nil on the successful result, as the field assertion checks.
	body := []byte(`{"model":"k3","max_tokens":32,"stream":false,` +
		`"messages":[{"role":"user","content":"hi"}]}`)
	upstream := &auxiliaryHTTPRecorder{resp: nativeAnthropicBufferedResponse()}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(),
		adaptiveProtocolTestContext("/v1/messages", body), nativeAnthropicTestProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "k3", result.UpstreamResponseModel)
	require.Nil(t, result.ReasoningEffort)
}

// TestNativeAnthropicPassthroughNormalizesGLM53Thinking enumerates
// disabled/enabled thinking and effort values for GLM 5.3. It checks enabled
// outgoing thinking and the expected normalized output effort in buffered and
// streaming fixtures.
func TestNativeAnthropicPassthroughNormalizesGLM53Thinking(t *testing.T) {
	tests := []struct {
		name       string // fixture case name
		stream     bool   // client streaming preference
		preference string // fixture thinking/effort JSON
		wantEffort string // expected outgoing effort
	}{
		{name: "disabled buffered", preference: `"thinking":{"type":"disabled"},`, wantEffort: "low"},
		{name: "off buffered", preference: `"thinking":{"type":"off"},`, wantEffort: "low"},
		{name: "none buffered", preference: `"thinking":{"type":"none"},`, wantEffort: "low"},
		{name: "enabled buffered", preference: `"thinking":{"type":"enabled"},`, wantEffort: "high"},
		{name: "enabled streaming", stream: true, preference: `"thinking":{"type":"enabled"},`, wantEffort: "high"},
		{name: "adaptive buffered", preference: `"thinking":{"type":"adaptive"},`, wantEffort: "high"},
		{name: "adaptive streaming", stream: true, preference: `"thinking":{"type":"adaptive"},`, wantEffort: "high"},
		{name: "minimal buffered", preference: `"output_config":{"effort":"minimal"},`, wantEffort: "low"},
		{name: "low buffered", preference: `"output_config":{"effort":"low"},`, wantEffort: "low"},
		{name: "medium streaming", stream: true, preference: `"output_config":{"effort":"medium"},`, wantEffort: "high"},
		{name: "high buffered", preference: `"output_config":{"effort":"high"},`, wantEffort: "high"},
		{name: "xhigh buffered", preference: `"output_config":{"effort":"xhigh"},`, wantEffort: "max"},
		{name: "max buffered", preference: `"output_config":{"effort":"max"},`, wantEffort: "max"},
		{name: "ultra buffered", preference: `"output_config":{"effort":"ultra"},`, wantEffort: "max"},
		{name: "output effort wins over thinking", preference: `"thinking":{"type":"adaptive"},"output_config":{"effort":"low"},`, wantEffort: "low"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"model":"glm-5.3","max_tokens":32,"stream":%t,%s"messages":[{"role":"user","content":"hi"}]}`, tt.stream, tt.preference))
			response := nativeAnthropicBufferedResponse()
			if tt.stream {
				response = nativeAnthropicStreamResponse()
			}
			upstream := &auxiliaryHTTPRecorder{resp: response}
			svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

			_, err := svc.Text.Messages(context.Background(),
				adaptiveProtocolTestContext("/v1/messages", body), nativeAnthropicGLMTestProvider(), body, "", "")
			require.NoError(t, err)
			require.Equal(t, "enabled", gjson.GetBytes(upstream.lastBody, "thinking.type").String())
			require.Equal(t, tt.wantEffort, gjson.GetBytes(upstream.lastBody, "output_config.effort").String())
		})
	}
}

// TestNativeAnthropicPassthroughLeavesOtherThinkingUntouched compares
// unspecified GLM 5.3 and disabled GLM 5.2 input. It checks successful
// execution and JSON equality of each outgoing body with its supplied fixture.
func TestNativeAnthropicPassthroughLeavesOtherThinkingUntouched(t *testing.T) {
	tests := []struct {
		name string // fixture case name
		body string // client request JSON
	}{
		{name: "glm 5.3 unspecified", body: `{"model":"glm-5.3","max_tokens":32,"stream":false,"messages":[]}`},
		{name: "glm 5.2 disabled", body: `{"model":"glm-5.2","max_tokens":32,"stream":false,"thinking":{"type":"disabled"},"messages":[]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(tt.body)
			upstream := &auxiliaryHTTPRecorder{resp: nativeAnthropicBufferedResponse()}
			svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
			_, err := svc.Text.Messages(context.Background(),
				adaptiveProtocolTestContext("/v1/messages", body), nativeAnthropicGLMTestProvider(), body, "", "")
			require.NoError(t, err)
			require.JSONEq(t, tt.body, string(upstream.lastBody))
		})
	}
}

// forceChatMessagesFallbackProvider creates an API-key provider forcing
// Messages through Chat conversion.
func forceChatMessagesFallbackProvider() *gatewayprovider.ExecutionProvider {
	provider := rawChatCompletionsTestProvider()
	provider.Record.Extra = map[string]any{
		providercore.ExtraKeyTextRouteMode: string(providercore.TextRouteModeForceChatCompletions),
	}
	return provider
}

// Read copies remaining fixture bytes and advances its offset before returning
// the configured error on subsequent calls. Callers supply a nonnil terminal
// error when they need reads to terminate after all data; no EOF is
// synthesized here.
func (r *errTailReader) Read(p []byte) (int, error) {
	if r.off < len(r.data) {
		n := copy(p, r.data[r.off:])
		r.off += n
		return n, nil
	}
	return 0, r.err
}

// Close satisfies the fixture body interface without changing read state.
func (r *errTailReader) Close() error { return nil }

// TestForwardAsAnthropic_ForceChatCompletionsPreservesFinalModelReasoningEffort
// maps the listed models and efforts into native Chat. It checks final
// outgoing model, model-sensitive reasoning control and matching result
// attribution.
func TestForwardAsAnthropic_ForceChatCompletionsPreservesFinalModelReasoningEffort(t *testing.T) {
	tests := []struct {
		name       string // fixture case name
		model      string // client request model
		mapped     string // provider-mapped upstream model
		effortJSON string // client effort JSON fragment
		wantEffort string // expected outgoing effort
	}{
		{
			name:       "GPT56 max",
			model:      "luna",
			mapped:     "gpt-5.6-luna",
			effortJSON: `,"output_config":{"effort":"max"}`,
			wantEffort: "max",
		},
		{
			name:       "old model max",
			model:      "gpt-5.5",
			mapped:     "gpt-5.5",
			effortJSON: `,"output_config":{"effort":"max"}`,
			wantEffort: "xhigh",
		},
		{
			name:       "DeepSeek V4 preserves native max",
			model:      "deepseek-v4-flash",
			mapped:     "deepseek/deepseek-v4-flash-0731",
			effortJSON: `,"output_config":{"effort":"max"}`,
			wantEffort: "max",
		},
		{
			name:       "high remains high",
			model:      "gpt-5.6-luna",
			mapped:     "gpt-5.6-luna",
			effortJSON: `,"output_config":{"effort":"high"}`,
			wantEffort: "high",
		},
		{
			name:       "omitted defaults medium",
			model:      "gpt-5.6-luna",
			mapped:     "gpt-5.6-luna",
			wantEffort: "medium",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			body := `{"model":"` + tt.model + `","max_tokens":16,"messages":[{"role":"user","content":"hello"}]` + tt.effortJSON + `,"stream":false}`
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader([]byte(body)))
			c.Request.Header.Set("Content-Type", "application/json")

			upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body: io.NopCloser(strings.NewReader(
					`{"id":"chatcmpl_effort","object":"chat.completion","model":"` + tt.mapped + `","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
				)),
			}}
			provider := forceChatMessagesFallbackProvider()
			provider.Record.Credentials["model_mapping"] = map[string]any{tt.model: tt.mapped}

			svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
			result, err := svc.Text.Messages(context.Background(), c, provider, []byte(body), "", "")
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tt.mapped, gjson.GetBytes(upstream.lastBody, "model").String())
			require.Equal(t, tt.wantEffort, gjson.GetBytes(upstream.lastBody, "reasoning_effort").String())
			require.NotNil(t, result.ReasoningEffort)
			require.Equal(t, tt.wantEffort, *result.ReasoningEffort)
		})
	}
}

// TestForwardAsAnthropic_ForceChatCompletionsNonStreaming returns native Chat
// JSON under forced routing. It checks endpoint/profile, TLS User-Agent,
// request shape, Messages answer/usage and separate observed service tier.
func TestForwardAsAnthropic_ForceChatCompletionsNonStreaming(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("User-Agent", "client-agent")
	SetActualOpenAIUpstreamEndpoint(c, "/v1/responses")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_msg_chat_json"}},
		Body: io.NopCloser(strings.NewReader(
			`{"id":"chatcmpl_json","object":"chat.completion","model":"gpt-5.4","service_tier":"priority","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5,"prompt_tokens_details":{"cached_tokens":1}}}`,
		)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	tlsMatch := egress.TLSFingerprintRouterMatchResult{Matched: true, UpstreamUserAgent: "router-agent"}
	result, err := svc.Text.Messages(context.Background(), c, forceChatMessagesFallbackProvider(), body, "", "", tlsMatch)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "http://upstream.example/v1/chat/completions", upstream.lastReq.URL.String())
	require.Equal(t, "/v1/chat/completions", GetActualOpenAIUpstreamEndpoint(c))
	require.Equal(t, upstreamcore.HTTPUpstreamProfileOpenAI, upstreamcore.HTTPUpstreamProfileFromContext(upstream.lastReq.Context()))
	require.Equal(t, "router-agent", upstream.lastReq.Header.Get("User-Agent"))
	require.Equal(t, "hello", gjson.GetBytes(upstream.lastBody, "messages.0.content").String())
	require.False(t, gjson.GetBytes(upstream.lastBody, "input").Exists())
	require.True(t, gjson.GetBytes(upstream.lastBody, "stream_options").Exists() == false)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "assistant", gjson.Get(rec.Body.String(), "role").String())
	require.Equal(t, "ok", gjson.Get(rec.Body.String(), "content.0.text").String())
	require.Equal(t, 3, result.Usage.InputTokens)
	require.Equal(t, 2, result.Usage.OutputTokens)
	require.Equal(t, 1, result.Usage.CacheReadInputTokens)
	require.Nil(t, result.ServiceTier)
	require.Equal(t, "priority", result.UpstreamResponseServiceTier)
	require.False(t, result.Stream)
}

// TestForwardAsAnthropic_ForceChatCompletionsStreamingClosesOpenBlockOnDone
// sends text fragments, finish reason, usage and DONE from Chat. It checks
// usage injection, text output and block-stop/delta/message-stop ordering.
func TestForwardAsAnthropic_ForceChatCompletionsStreamingClosesOpenBlockOnDone(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"id":"chatcmpl_s","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_s","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"he"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_s","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"llo"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_s","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"",
		`data: {"id":"chatcmpl_s","object":"chat.completion.chunk","model":"gpt-5.4","choices":[],"usage":{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_msg_chat_stream"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(), c, forceChatMessagesFallbackProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, gjson.GetBytes(upstream.lastBody, "stream_options.include_usage").Bool())

	out := rec.Body.String()
	require.Contains(t, out, "event: message_start")
	require.Contains(t, out, `"text":"he"`)
	require.Contains(t, out, `"text":"llo"`)
	require.Contains(t, out, "event: content_block_stop")
	require.Contains(t, out, `"stop_reason":"end_turn"`)
	require.Contains(t, out, "event: message_stop")
	require.NotContains(t, out, "event: ping")

	blockStop := strings.Index(out, "event: content_block_stop")
	msgDelta := strings.Index(out, `"stop_reason":"end_turn"`)
	msgStop := strings.Index(out, "event: message_stop")
	require.Greater(t, msgDelta, blockStop, "content_block_stop must precede message_delta")
	require.Greater(t, msgStop, msgDelta, "message_delta must precede message_stop")

	require.Equal(t, 4, result.Usage.InputTokens)
	require.Equal(t, 3, result.Usage.OutputTokens)
	require.True(t, result.Stream)
	require.Nil(t, result.Timings.TTFTMs())
}

// TestForwardAsAnthropic_ForceChatCompletionsStreamingToolCallAggregation
// splits one indexed tool argument across Chat chunks. It checks disabled
// parallel mode, tool-use framing without synthesized pings, tool stop reason and
// observed usage.
func TestForwardAsAnthropic_ForceChatCompletionsStreamingToolCallAggregation(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"weather in sf?"}],"tools":[{"name":"get_weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"tool_choice":{"type":"auto","disable_parallel_tool_use":true},"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"id":"chatcmpl_t","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_t","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\":"}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_t","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"sf\"}"}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_t","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		"",
		`data: {"id":"chatcmpl_t","object":"chat.completion.chunk","model":"gpt-5.4","choices":[],"usage":{"prompt_tokens":6,"completion_tokens":5,"total_tokens":11}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_msg_chat_tool"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(), c, forceChatMessagesFallbackProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	parallelToolCalls := gjson.GetBytes(upstream.lastBody, "parallel_tool_calls")
	require.True(t, parallelToolCalls.Exists())
	require.False(t, parallelToolCalls.Bool())

	out := rec.Body.String()
	require.NotContains(t, out, "event: ping")
	require.Contains(t, out, `"type":"tool_use"`)
	require.Contains(t, out, `"name":"get_weather"`)
	require.Contains(t, out, `"input_json_delta"`)
	require.Contains(t, out, `"stop_reason":"tool_use"`)
	require.Contains(t, out, "event: message_stop")
	require.Less(t, strings.Index(out, "event: message_start"), strings.Index(out, `"type":"tool_use"`))
	require.Equal(t, 6, result.Usage.InputTokens)
	require.Equal(t, 5, result.Usage.OutputTokens)
}

// TestForwardAsAnthropic_ForceChatCompletionsStreamingInterleavedParallelToolCalls
// declares tool index one before zero and interleaves arguments. It checks
// per-block start/delta/stop transitions, ordered tool identities, assembled
// JSON, parallel mode and usage.
func TestForwardAsAnthropic_ForceChatCompletionsStreamingInterleavedParallelToolCalls(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":64,"messages":[{"role":"user","content":"read the file and print the directory"}],"tools":[{"name":"Read","input_schema":{"type":"object","properties":{"file_path":{"type":"string"}}}},{"name":"Bash","input_schema":{"type":"object","properties":{"command":{"type":"string"}}}}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"id":"chatcmpl_parallel","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":1,"id":"call_bash","type":"function","function":{"name":"Bash","arguments":"{\"command\":"}},{"index":0,"id":"call_read","type":"function","function":{"name":"Read","arguments":""}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_parallel","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"file_path\":"}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_parallel","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"\"pwd\"}"}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_parallel","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"README.md\"}"}}]},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_parallel","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`,
		"",
		`data: {"id":"chatcmpl_parallel","object":"chat.completion.chunk","model":"gpt-5.4","choices":[],"usage":{"prompt_tokens":12,"completion_tokens":9,"total_tokens":21}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_msg_chat_parallel"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(), c, forceChatMessagesFallbackProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	parallelToolCalls := gjson.GetBytes(upstream.lastBody, "parallel_tool_calls")
	require.True(t, parallelToolCalls.Exists())
	require.True(t, parallelToolCalls.Bool())

	// observedToolUse captures a rendered tool identity and its assembled JSON
	// arguments.
	type observedToolUse struct {
		ID   string // rendered tool call identifier
		Name string // rendered tool function name
		// Arguments grows with input_json_delta for this opened client tool block;
		// the parser uses its recorded block position to keep fragments paired.
		Arguments string // assembled JSON argument text
	}
	// Track client block indexes and tool positions separately so interleaved
	// partial arguments are checked against the tool_use block that actually opened them.
	blockStates := make(map[int]string)
	toolPositions := make(map[int]int)
	var tools []observedToolUse
	for line := range strings.SplitSeq(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		payload := strings.TrimPrefix(line, "data: ")
		eventType := gjson.Get(payload, "type").String()
		indexResult := gjson.Get(payload, "index")
		if !indexResult.Exists() {
			continue
		}
		index := int(indexResult.Int())

		switch eventType {
		case "content_block_start":
			require.Empty(t, blockStates[index], "block %d must start once", index)
			blockStates[index] = "open"
			if gjson.Get(payload, "content_block.type").String() == "tool_use" {
				toolPositions[index] = len(tools)
				tools = append(tools, observedToolUse{
					ID:   gjson.Get(payload, "content_block.id").String(),
					Name: gjson.Get(payload, "content_block.name").String(),
				})
			}
		case "content_block_delta":
			require.Equal(t, "open", blockStates[index], "block %d delta must occur while open", index)
			if gjson.Get(payload, "delta.type").String() == "input_json_delta" {
				position, ok := toolPositions[index]
				require.True(t, ok, "tool delta must follow its tool_use start")
				tools[position].Arguments += gjson.Get(payload, "delta.partial_json").String()
			}
		case "content_block_stop":
			require.Equal(t, "open", blockStates[index], "block %d stop must follow start", index)
			blockStates[index] = "closed"
		}
	}

	require.Equal(t, map[int]string{0: "closed", 1: "closed"}, blockStates)
	require.Len(t, tools, 2)
	require.Equal(t, "call_bash", tools[0].ID)
	require.Equal(t, "Bash", tools[0].Name)
	require.JSONEq(t, `{"command":"pwd"}`, tools[0].Arguments)
	require.Equal(t, "call_read", tools[1].ID)
	require.Equal(t, "Read", tools[1].Name)
	require.JSONEq(t, `{"file_path":"README.md"}`, tools[1].Arguments)
	require.Contains(t, rec.Body.String(), `"stop_reason":"tool_use"`)
	require.Equal(t, 12, result.Usage.InputTokens)
	require.Equal(t, 9, result.Usage.OutputTokens)
}

// TestForwardAsAnthropic_ForceChatCompletionsStreamingLengthMapsToMaxTokens
// ends native Chat with finish_reason length. It checks converted max_tokens
// stop reason and a terminal Messages message_stop event for this fixture.
func TestForwardAsAnthropic_ForceChatCompletionsStreamingLengthMapsToMaxTokens(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":8,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := strings.Join([]string{
		`data: {"id":"chatcmpl_l","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant","content":"truncat"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_l","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{},"finish_reason":"length"}],"usage":{"prompt_tokens":4,"completion_tokens":8,"total_tokens":12}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_msg_chat_len"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(), c, forceChatMessagesFallbackProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)

	out := rec.Body.String()
	require.Contains(t, out, `"stop_reason":"max_tokens"`)
	require.Contains(t, out, "event: message_stop")
}

// TestForwardAsAnthropic_ForceChatCompletionsEmptyStreamStillFramesMessage
// supplies an immediate native Chat DONE sentinel. It checks successful
// returned observation and Messages start, delta and stop framing for that
// empty response.
func TestForwardAsAnthropic_ForceChatCompletionsEmptyStreamStillFramesMessage(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":8,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_msg_chat_empty"}},
		Body:       io.NopCloser(strings.NewReader("data: [DONE]\n\n")),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(), c, forceChatMessagesFallbackProvider(), body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)

	out := rec.Body.String()
	require.Contains(t, out, "event: message_start")
	require.Contains(t, out, "event: message_delta")
	require.Contains(t, out, "event: message_stop")
}

// TestForwardAsAnthropic_ForceChatCompletionsNonFailover400UsesSharedErrorHandler
// returns a native Chat parameter rejection. It checks Anthropic HTTP 400
// envelope and the shared status, message and single HTTP-error Ops event.
func TestForwardAsAnthropic_ForceChatCompletionsNonFailover400UsesSharedErrorHandler(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":8,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"Content-Type": []string{"application/json"}, "x-request-id": []string{"rid_msg_chat_400"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"invalid roles","type":"invalid_request_error"}}`)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(), c, forceChatMessagesFallbackProvider(), body, "", "")
	require.Error(t, err)
	require.Nil(t, result)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Equal(t, "error", gjson.Get(rec.Body.String(), "type").String())
	require.Equal(t, "invalid_request_error", gjson.Get(rec.Body.String(), "error.type").String())
	require.Equal(t, "invalid roles", gjson.Get(rec.Body.String(), "error.message").String())

	statusVal, ok := c.Get(OpsUpstreamStatusCodeKey)
	require.True(t, ok, "shared handler must record the upstream status for ops")
	require.Equal(t, http.StatusBadRequest, statusVal)

	eventsVal, ok := c.Get(OpsUpstreamErrorsKey)
	require.True(t, ok, "shared handler must append an ops upstream error event")
	events, castOK := eventsVal.([]*ops.OpsUpstreamErrorEvent)
	require.True(t, castOK)
	require.Len(t, events, 1)
	require.Equal(t, http.StatusBadRequest, events[0].UpstreamStatusCode)
	require.Equal(t, "http_error", events[0].Kind)
	require.Equal(t, "invalid roles", events[0].Message)
}

// TestForwardAsAnthropic_ForceChatCompletionsStreamReadErrorSkipsFinalize
// returns a read failure after a native Chat text fragment. It checks partial
// result/output and the usage-incomplete error with no synthetic message_stop.
func TestForwardAsAnthropic_ForceChatCompletionsStreamReadErrorSkipsFinalize(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":8,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	partial := strings.Join([]string{
		`data: {"id":"chatcmpl_e","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"role":"assistant"},"finish_reason":null}]}`,
		"",
		`data: {"id":"chatcmpl_e","object":"chat.completion.chunk","model":"gpt-5.4","choices":[{"index":0,"delta":{"content":"he"},"finish_reason":null}]}`,
		"",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_msg_chat_err"}},
		Body:       &errTailReader{data: []byte(partial), err: errors.New("simulated upstream read failure")},
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	result, err := svc.Text.Messages(context.Background(), c, forceChatMessagesFallbackProvider(), body, "", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "stream usage incomplete")
	require.NotNil(t, result)
	require.True(t, result.Stream)

	out := rec.Body.String()
	require.Contains(t, out, `"text":"he"`, "delta emitted before the failure must reach the client")
	require.NotContains(t, out, "event: message_stop", "no synthetic completion after a broken read")
}

// TestForwardAsAnthropic_ResponsesSupportedProviderStillUsesResponsesEndpoint
// uses API-key protocol preservation for Messages conversion. It checks
// Responses endpoint and input shape, retained third-party identity fields and
// the converted answer.
func TestForwardAsAnthropic_ResponsesSupportedProviderStillUsesResponsesEndpoint(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":16,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")
	c.Request.Header.Set("User-Agent", "third-party-client/1.0.0")
	c.Request.Header.Set("originator", "opencode")
	SetActualOpenAIUpstreamEndpoint(c, "/v1/chat/completions")

	upstreamBody := strings.Join([]string{
		`data: {"type":"response.completed","response":{"id":"resp_native","object":"response","model":"gpt-5.4","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "x-request-id": []string{"rid_msg_native"}},
		Body:       io.NopCloser(strings.NewReader(upstreamBody)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	provider := rawChatCompletionsTestProvider()
	provider.Record.Extra = map[string]any{
		providercore.ExtraKeyTextRouteMode: string(providercore.TextRouteModePreserveClientProtocol),
	}

	result, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.True(t, strings.HasSuffix(upstream.lastReq.URL.Path, "/responses"),
		"responses-capable provider must stay on /v1/responses, got %s", upstream.lastReq.URL.String())
	require.Equal(t, "/v1/responses", GetActualOpenAIUpstreamEndpoint(c))
	require.True(t, gjson.GetBytes(upstream.lastBody, "input").Exists())
	require.False(t, gjson.GetBytes(upstream.lastBody, "messages").Exists())
	require.Equal(t, "third-party-client/1.0.0", upstream.lastReq.Header.Get("User-Agent"))
	require.Equal(t, "opencode", upstream.lastReq.Header.Get("originator"))
	require.Empty(t, upstream.lastReq.Header.Get("version"))
	require.Empty(t, upstream.lastReq.Header.Get("OpenAI-Beta"))
	require.Equal(t, "ok", gjson.Get(rec.Body.String(), "content.0.text").String())
}

// buildResponsesFailedSSEStream builds a failed Response event with fixed
// usage for error fixtures. Type and message are interpolated directly into
// JSON, so callers supply JSON-safe fixture values; the result contains one
// complete SSE data frame.
func buildResponsesFailedSSEStream(errType, errorMessage string) string {
	failed := fmt.Sprintf(`{"type":"response.failed","response":{"id":"resp_err","object":"response","status":"failed","error":{"type":"%s","message":"%s"},"output":[],"usage":{"input_tokens":10,"output_tokens":0,"total_tokens":10}}}`, errType, errorMessage)
	return fmt.Sprintf("data: %s\n\n", failed)
}

// TestForwardAsAnthropic_BufferedResponseFailed_ReturnsError supplies a
// nonretryable content-policy terminal to buffered Messages conversion. It
// checks the returned upstream-failure diagnostic and final HTTP 502 response
// for this fixture.
func TestForwardAsAnthropic_BufferedResponseFailed_ReturnsError(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	ssePayload := buildResponsesFailedSSEStream("invalid_request_error", "Content policy violation")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(ssePayload)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err, "non-cyber response.failed must return an error, not swallow as 200")
	require.Contains(t, err.Error(), "upstream response failed")
	require.Equal(t, http.StatusBadGateway, rec.Code, "should write 502 for non-failover failed response")
}

// TestForwardAsAnthropic_StreamingResponseFailed_ReturnsError supplies a
// nonretryable content-policy failed terminal for a streaming Messages
// request. It checks the returned error and upstream-failure diagnostic after
// conversion.
func TestForwardAsAnthropic_StreamingResponseFailed_ReturnsError(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	ssePayload := buildResponsesFailedSSEStream("invalid_request_error", "Content policy violation")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(ssePayload)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err, "streaming response.failed must return an error")
	require.Contains(t, err.Error(), "upstream response failed")
}

// TestForwardAsAnthropic_StreamingBareErrorAfterOutputIsVisible follows text
// with a bare mixed-tools failure. It checks retained text and error event, no
// message_stop, and a failure diagnostic distinct from missing-terminal
// handling.
func TestForwardAsAnthropic_StreamingBareErrorAfterOutputIsVisible(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	ssePayload := strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_bare_error","object":"response","model":"gpt-5.4","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}`,
		"",
		`event: error`,
		`data: {"type":"error","error":{"type":"server_error","code":"upstream_error","message":"mixed tools failed"}}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(ssePayload)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "upstream response failed: mixed tools failed")
	clientStream := rec.Body.String()
	require.Contains(t, clientStream, `"text":"partial"`)
	require.Contains(t, clientStream, "event: error")
	require.Contains(t, clientStream, "mixed tools failed")
	require.NotContains(t, clientStream, "event: message_stop")
	require.NotContains(t, err.Error(), "missing terminal event")
}

// TestForwardAsAnthropic_StreamingBareErrorBeforeOutputFailsOver supplies
// recognized temporary failure before Messages content. It checks typed
// provider recovery and an empty downstream body before any retry is
// attempted.
func TestForwardAsAnthropic_StreamingBareErrorBeforeOutputFailsOver(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	ssePayload := strings.Join([]string{
		`event: error`,
		`data: {"type":"error","error":{"type":"server_error","code":"upstream_error","message":"temporary upstream failure"}}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(ssePayload)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr), "pre-output retryable error must remain failover-safe: %T: %v", err, err)
	require.Empty(t, rec.Body.String(), "failover path must not commit downstream output")
}

// TestForwardAsAnthropic_StreamingGenericBareErrorBeforeOutputIsNotHiddenByFailover
// supplies generic mixed-tools error before content. It checks no failover
// wrapper and visible failure text in the final client response.
func TestForwardAsAnthropic_StreamingGenericBareErrorBeforeOutputIsNotHiddenByFailover(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	ssePayload := "event: error\n" +
		`data: {"type":"error","error":{"type":"server_error","code":"upstream_error","message":"mixed tools failed"}}` + "\n\n" +
		"data: [DONE]\n\n"
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(ssePayload)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	_, err := svc.Text.Messages(context.Background(), c, rawChatCompletionsTestProvider(), body, "", "")

	require.Error(t, err)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr))
	require.Contains(t, rec.Body.String(), "mixed tools failed")
}

// TestForwardAsAnthropic_BufferedResponseFailed_Failover supplies a rate-limit
// failure inside buffered Responses SSE. It checks a returned error with the
// provider-failover type for this converted Messages rejection.
func TestForwardAsAnthropic_BufferedResponseFailed_Failover(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	ssePayload := buildResponsesFailedSSEStream("rate_limit_error", "Rate limit reached")

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(ssePayload)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr), "rate_limit_error should trigger UpstreamFailoverError for failover, got: %T: %v", err, err)
}

// TestForwardAsAnthropic_StreamingResponseFailed_FailoverBeforeOutput supplies
// a rate-limit terminal before Messages content starts. It checks typed
// provider recovery and that the writer remains uncommitted for this attempt.
func TestForwardAsAnthropic_StreamingResponseFailed_FailoverBeforeOutput(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	ssePayload := buildResponsesFailedSSEStream("rate_limit_error", "Rate limit reached")
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(ssePayload)),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.False(t, c.Writer.Written(), "故障转移前不应向客户端写入响应")
}

// TestForwardAsAnthropic_TransportError_ReturnsFailoverError injects
// connection refusal from Messages forwarding. It checks the returned
// provider-failover type and HTTP 502 classification without running the outer
// provider loop.
func TestForwardAsAnthropic_TransportError_ReturnsFailoverError(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{
		err: errors.New(`dial tcp 1.2.3.4:443: connect: connection refused`),
	}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.True(t, errors.As(err, &failoverErr), "transport error should return UpstreamFailoverError for handler failover, got: %T", err)
	require.Equal(t, http.StatusBadGateway, failoverErr.StatusCode)
}

// TestForwardAsAnthropic_TransportError_DoesNotWriteResponse injects
// connection reset from Messages forwarding. It checks the recorder's initial
// status and empty body, leaving final client presentation to outer handling.
func TestForwardAsAnthropic_TransportError_DoesNotWriteResponse(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{
		err: errors.New(`read tcp: connection reset by peer`),
	}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	// Discard both returns to inspect writer state after transport failure; the
	// neighboring transport-error case checks the returned failover wrapper.
	_, _ = svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Equal(t, http.StatusOK, rec.Code, "transport error must not write HTTP response — handler owns the response for failover")
	require.Empty(t, rec.Body.String(), "response body must be empty so handler can write the correct error or failover")
}

// TestForwardAsAnthropic_TransportError_ClientCanceled_NoFailover provides a
// cancelled client and context.Canceled transport failure. It checks a
// returned error with no provider-failover wrapper for that cancellation path.
func TestForwardAsAnthropic_TransportError_ClientCanceled_NoFailover(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	cancelCtx, cancel := context.WithCancel(context.Background())
	// Cancel before forwarding to exercise the injected cancellation path while
	// the fixture controls the transport result independently of network progress.
	cancel()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body)).WithContext(cancelCtx)
	c.Request.Header.Set("Content-Type", "application/json")

	upstream := &auxiliaryHTTPRecorder{
		err: context.Canceled,
	}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	var failoverErr *forwardcore.UpstreamFailoverError
	require.False(t, errors.As(err, &failoverErr), "client-canceled transport error should NOT trigger failover")
}

// TestForwardAsAnthropic_ResponseFailed_PassthroughRule applies a buffered
// context-window presentation override. It checks the matched-rule diagnostic,
// HTTP 400 and a nonempty upstream message in the final envelope.
func TestForwardAsAnthropic_ResponseFailed_PassthroughRule(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	bindPassthroughRule(c, "openai", []string{"context_length_exceeded"}, 400)

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(buildContextLengthFailedSSE())),
	}}
	svc := newWSFixture(wsFixtureInputs{options: rawChatCompletionsTestConfig(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	require.Contains(t, err.Error(), "passthrough")
	require.Equal(t, 400, rec.Code, "passthrough rule should override 502 to 400")
	respBody := rec.Body.String()
	errMsg := gjson.Get(respBody, "error.message").String()
	require.NotEmpty(t, errMsg, "passthrough should preserve error message")
}

// TestForwardAsAnthropic_StreamingResponseFailed_PassthroughRule applies the
// context-window presentation override to a streaming request before content.
// It checks HTTP 400 and visible context-window text after the returned
// failure.
func TestForwardAsAnthropic_StreamingResponseFailed_PassthroughRule(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":true}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	bindPassthroughRule(c, "openai", []string{"context_length_exceeded"}, http.StatusBadRequest)

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(buildContextLengthFailedSSE())),
	}}
	svc := newWSFixture(wsFixtureInputs{options: rawChatCompletionsTestConfig(), transport: upstream})

	_, err := svc.Text.Messages(context.Background(), c, rawChatCompletionsTestProvider(), body, "", "")

	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "context window")
}

// TestForwardAsAnthropic_ResponseFailed_ErrorCodeRuleMatchesViaSemanticStatus
// matches inferred HTTP 400 and a context-length code inside SSE. It checks
// HTTP 400 and a nonempty message in buffered Messages error output.
func TestForwardAsAnthropic_ResponseFailed_ErrorCodeRuleMatchesViaSemanticStatus(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	bindStatusCodePassthroughRule(c, "openai", http.StatusBadRequest, "context_length_exceeded", http.StatusBadRequest)

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(buildContextLengthFailedSSE())),
	}}
	svc := newWSFixture(wsFixtureInputs{options: rawChatCompletionsTestConfig(), transport: upstream})

	provider := rawChatCompletionsTestProvider()
	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	require.Error(t, err)
	require.Equal(t, http.StatusBadRequest, rec.Code, "error-code-conditioned rule should match via semantic status inference")
	respBody := rec.Body.String()
	require.NotEmpty(t, gjson.Get(respBody, "error.message").String())
}

// SetModelRateLimit records the latest provider ID and model key for
// post-forward fixture assertions. Context, deadline and reason arguments are
// ignored, and tests serialize writes with inspection.
func (r *tempUnschedulableOpenAIProviderRepo) SetModelRateLimit(_ context.Context, providerID int64, modelKey string, _ time.Time, _ ...string) error {
	r.modelRateLimitProviderID = providerID
	r.modelRateLimitKey = modelKey
	return nil
}

// TestOpenAIGatewayService_ForwardAsAnthropic_CapacityShedReturnsRequestScopedFailoverWithoutCommit
// observes capacity rejection before a separately invoked healthy provider. It
// checks transient retry metadata, absent model-limit writes, untouched first
// output and successful second response.
func TestOpenAIGatewayService_ForwardAsAnthropic_CapacityShedReturnsRequestScopedFailoverWithoutCommit(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","max_tokens":32,"messages":[{"role":"user","content":"hello"}],"stream":false}`)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	c.Request.Header.Set("Content-Type", "application/json")

	upstreamBody := []byte(`{"error":{"message":"Our servers are currently overloaded. Please try again later."}}`)
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(upstreamBody)),
		},
		{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: io.NopCloser(strings.NewReader(strings.Join([]string{
				`data: {"type":"response.output_text.delta","delta":"ok"}`,
				"",
				`data: {"type":"response.completed","response":{"id":"resp_second","status":"completed","output":[],"usage":{"input_tokens":1,"output_tokens":1}}}`,
				"",
			}, "\n"))),
		},
	}}
	repo := &tempUnschedulableOpenAIProviderRepo{}
	healthObserver := newHTTPHealthFixture(repo, &responsesFixtureOptions{}, nil, providercore.HealthOptions{}, nil)

	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{
		Enabled: false, AllowInsecureHTTP: true,
	}}}, transport: upstream, health: healthObserver})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 5099, Name: "temporary-unschedulable", Platform: capability.PlatformOpenAI,
			Type: capability.ProviderTypeAPIKey, Concurrency: 1,
			Credentials: map[string]any{
				"api_key":                    "sk-test",
				"base_url":                   "http://upstream.example",
				"model_provider":             "env-openai",
				"temp_unschedulable_enabled": true,
				"temp_unschedulable_rules": []any{map[string]any{
					"error_code":       float64(http.StatusBadRequest),
					"keywords":         []any{"our servers are currently overloaded", "please try again later"},
					"duration_minutes": float64(1),
				}},
			},
		},
	}

	_, err := svc.Text.Messages(context.Background(), c, provider, body, "", "")

	var failoverErr *forwardcore.UpstreamFailoverError
	require.ErrorAs(t, err, &failoverErr)
	require.Equal(t, http.StatusBadRequest, failoverErr.StatusCode)
	require.True(t, failoverErr.ShouldRetryNextProvider())
	require.True(t, failoverErr.RetryableOnSameProvider)
	require.True(t, failoverErr.RequestScopedTransient)
	require.Zero(t, repo.modelRateLimitProviderID, "request-scoped capacity shedding must not change provider health")
	require.Empty(t, repo.modelRateLimitKey)
	require.False(t, IsResponseCommitted(c))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, rec.Body.String())

	secondRec := httptest.NewRecorder()
	secondContext, _ := gin.CreateTestContext(secondRec)
	secondContext.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	secondContext.Request.Header.Set("Content-Type", "application/json")
	secondProvider := *provider
	secondProvider.Record.ID = 5100
	secondProvider.Record.Name = "healthy-failover-provider"
	result, secondErr := svc.Text.Messages(context.Background(), secondContext, &secondProvider, body, "", "")
	require.NoError(t, secondErr)
	require.NotNil(t, result)
	require.Equal(t, "resp_second", result.ResponseID)
	require.NotEmpty(t, secondRec.Body.String())
}

// TestIsOpenAIOAuthLike enumerates the listed platform and credential
// combinations, including nil. It checks OAuth-like and Codex-protocol
// predicates separately so only the supplied OpenAI OAuth/setup fixtures
// qualify.
func TestIsOpenAIOAuthLike(t *testing.T) {
	tests := []struct {
		name string // fixture case name
		// selected fixture execution provider
		provider *gatewayprovider.ExecutionProvider
		want     bool // expected OAuth-like predicate
		codex    bool // expected Codex protocol predicate
	}{
		{name: "openai_oauth", provider: &gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeOAuth}}, want: true, codex: true},
		{name: "openai_setup_token", provider: &gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeSetupToken}}, want: true, codex: true},
		{name: "openai_api_key", provider: &gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey}}, want: false, codex: false},
		{name: "anthropic_setup_token", provider: &gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Platform: capability.PlatformAnthropic, Type: capability.ProviderTypeSetupToken}}, want: false, codex: false},
		{name: "grok_setup_token", provider: &gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Platform: capability.PlatformGrok, Type: capability.ProviderTypeSetupToken}}, want: false, codex: false},
		{name: "nil", provider: nil, want: false, codex: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.provider.View().IsOpenAIOAuthLike())
			require.Equal(t, tt.codex, tt.provider.View().UsesOpenAICodexProtocol())
		})
	}
}

// TestOpenAISetupTokenMessagesUsesCodexBridgeAndTurnState runs two setup-token
// Messages turns under one cache seed. It checks full replay/guard, Codex
// credentials and identity, and second-turn state replay with the isolated
// session header.
func TestOpenAISetupTokenMessagesUsesCodexBridgeAndTurnState(t *testing.T) {
	firstResp := openAICompatSSECompletedResponse("resp_setup_first", "gpt-5.4")
	firstResp.Header.Set("x-codex-turn-state", "turn_state_setup")
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		firstResp,
		openAICompatSSECompletedResponse("resp_setup_second", "gpt-5.4"),
	}}
	svc := newResponsesFixture(responsesFixtureInputs{options: &responsesFixtureOptions{Request: OpenAIRequestOptions{URLPolicy: egress.OperatorURLPolicy{Enabled: false}}}, transport: upstream})
	provider := openAISetupTokenCompatProvider(72)

	messages := make([]string, 0, 12+3)
	// Exceed the compatibility replay suffix with distinguishable messages so
	// assertions can detect both trimming and retention of the earlier conversation.
	for i := range 12 + 3 {
		messages = append(messages, `{"role":"user","content":"message-`+fmt.Sprintf("%02d", i)+`"}`)
	}
	firstBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[` + strings.Join(messages, ",") + `],"stream":false}`)
	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(firstBody))
	firstCtx.Request.Header.Set("Content-Type", "application/json")

	firstResult, err := svc.Text.Messages(context.Background(), firstCtx, provider, firstBody, "stable-cache-key", "gpt-5.4")

	require.NoError(t, err)
	require.NotNil(t, firstResult)
	require.True(t, IsOpenAICompatMessagesBridgeContext(firstCtx))
	require.Equal(t, int64(12+4), gjson.GetBytes(upstream.bodies[0], "input.#").Int())
	require.Equal(t, "developer", gjson.GetBytes(upstream.bodies[0], "input.0.role").String())
	require.Contains(t, gjson.GetBytes(upstream.bodies[0], "input.0.content.0.text").String(), gatewayprovider.OpenAICompatClaudeCodeTodoGuardMarker)
	require.Equal(t, "message-00", gjson.GetBytes(upstream.bodies[0], "input.1.content.0.text").String())
	require.False(t, gjson.GetBytes(upstream.bodies[0], "prompt_cache_key").Exists())
	require.Equal(t, ChatgptCodexURL, upstream.requests[0].URL.String())
	require.Equal(t, "Bearer setup-token-value", upstream.requests[0].Header.Get("Authorization"))
	require.Equal(t, "chatgpt-setup", upstream.requests[0].Header.Get("chatgpt-account-id"))
	requireOpenAIMessagesCodexIdentity(t, upstream.requests[0], openai.CodexCLIUserAgent, "codex-tui")
	require.Empty(t, upstream.requests[0].Header.Get("x-codex-turn-state"))

	secondBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":16,"messages":[{"role":"user","content":"next"}],"stream":false}`)
	secondRec := httptest.NewRecorder()
	secondCtx, _ := gin.CreateTestContext(secondRec)
	secondCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(secondBody))
	secondCtx.Request.Header.Set("Content-Type", "application/json")

	secondResult, err := svc.Text.Messages(context.Background(), secondCtx, provider, secondBody, "stable-cache-key", "gpt-5.4")

	require.NoError(t, err)
	require.NotNil(t, secondResult)
	require.True(t, IsOpenAICompatMessagesBridgeContext(secondCtx))
	require.Equal(t, "turn_state_setup", upstream.requests[1].Header.Get("x-codex-turn-state"))
	require.Equal(t, upstreamcore.GenerateSessionUUID(openai.IsolateOpenAIUpstreamSessionID(0, provideradapter.CodexIdentityNamespace(provider.View()), "stable-cache-key")), upstream.requests[1].Header.Get("session_id"))
	require.Empty(t, upstream.requests[1].Header.Get("conversation_id"))
	requireOpenAIMessagesCodexIdentity(t, upstream.requests[1], openai.CodexCLIUserAgent, "codex-tui")
}

// grokMessagesSSECompletedResponse builds a successful Grok Responses SSE
// fixture with supplied cached usage. Callers supply simple IDs whose fmt-quoted
// form is valid JSON; the executing fixture consumes and closes the returned
// readable body.
func grokMessagesSSECompletedResponse(responseID string, cachedTokens int) *http.Response {
	body := strings.Join([]string{
		fmt.Sprintf(`data: {"type":"response.completed","response":{"id":%q,"object":"response","model":"grok-4.3","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7,"input_tokens_details":{"cached_tokens":%d}}}}`, responseID, cachedTokens),
		"",
		"data: [DONE]",
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

// TestForwardGrokMessagesReasoningOptions enumerates model-specific effort
// inputs for Grok Messages conversion. It checks the outgoing effort value and
// exact model after each fixture completes successfully.
func TestForwardGrokMessagesReasoningOptions(t *testing.T) {
	for _, tc := range []struct {
		// model, effort and want form one model-sensitive conversion case. The
		// supplied model selects effort handling, and want names the forwarded
		// effort asserted after the fixture returns.
		// model: requested Grok model identity.
		// effort: supplied Messages effort value.
		// want: expected forwarded effort value.
		model, effort, want string
	}{
		{"grok-build-0.1", "", ""},
		{"grok-4.20-0309-reasoning", "high", ""},
		{"grok-4.7", "xhigh", "xhigh"},
		{"grok-private-route", "max", "max"},
	} {
		t.Run(tc.model, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			body := []byte(fmt.Sprintf(`{"model":%q,"max_tokens":32,"stream":false,"messages":[{"role":"user","content":"hi"}],"output_config":{"effort":%q}}`, tc.model, tc.effort))
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
			target := gatewaytestkit.HealthyGrokOAuthProvider(54, "access-token")
			repo := &grokQuotaProviderRepo{grokFixtureProviders: &grokFixtureProviders{providersByID: map[int64]*gatewayprovider.ExecutionProvider{54: target}}}
			upstream := &auxiliaryHTTPRecorder{resp: grokMessagesSSECompletedResponse("resp_options", 0)}
			svc := newResponsesFixture(responsesFixtureInputs{grokTokens: newHTTPGrokTokenFixture(repo, nil), transport: upstream, providers: repo})
			result, err := svc.Text.Messages(context.Background(), c, target, body, "", "")
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tc.want, gjson.GetBytes(upstream.lastBody, "reasoning.effort").String())
			require.Equal(t, tc.model, gjson.GetBytes(upstream.lastBody, "model").String())
		})
	}
}

// TestNativeAnthropicGLMUsesResolvedEffortBeforeConversion injects model
// attributes allowing medium and high before native GLM preparation. It checks
// retained medium outgoing/result effort and enabled thinking under that
// catalog fixture.
func TestNativeAnthropicGLMUsesResolvedEffortBeforeConversion(t *testing.T) {
	medium, high := "medium", "high"
	options := []modelcatalog.ReasoningOption{{Type: "effort", Values: []*string{&medium, &high}}}
	upstream := &auxiliaryHTTPRecorder{resp: nativeAnthropicBufferedResponse()}
	svc := newResponsesFixture(responsesFixtureInputs{options: protocolHTTPOptions(), transport: upstream})
	svc.Requests.Readers.Attributes = &routing.ModelAttributeService{Catalog: routing.ModelAttributeCatalog{Lookup: func(string) modelcatalog.Attributes { return modelcatalog.Attributes{ReasoningOptions: &options} }}}
	body := []byte(`{"model":"glm-5.3","max_tokens":32,"output_config":{"effort":"medium"},"messages":[{"role":"user","content":"hi"}]}`)
	result, err := svc.Text.Messages(context.Background(), adaptiveProtocolTestContext("/v1/messages", body), nativeAnthropicGLMTestProvider(), body, "", "")
	require.NoError(t, err)
	require.Equal(t, "medium", gjson.GetBytes(upstream.lastBody, "output_config.effort").String())
	require.Equal(t, "enabled", gjson.GetBytes(upstream.lastBody, "thinking.type").String())
	require.Equal(t, "medium", *result.ReasoningEffort)
}
