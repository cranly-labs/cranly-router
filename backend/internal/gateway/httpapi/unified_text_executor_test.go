package httpapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/apikey"
	"github.com/TokenFlux/TokenRouter/internal/billing"
	"github.com/TokenFlux/TokenRouter/internal/gateway/admission"
	"github.com/TokenFlux/TokenRouter/internal/gateway/completion"
	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayadapter "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/googleforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/messageforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/searchtools"
	"github.com/TokenFlux/TokenRouter/internal/identity"
	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/usage"
)

// unifiedRecordWriter retains usage rows produced by the common completion
// recorder.
type unifiedRecordWriter struct {
	rows []*usage.UsageLog // retained recorded usage-row pointers
}

// unifiedRecordEffects supplies inert completion side effects for native
// accounting assertions.
type unifiedRecordEffects struct{}

// unifiedRecordModels supplies one exact pricing candidate for native usage
// recording.
type unifiedRecordModels struct{}

// unifiedSearchSource supplies an absent search backend during native protocol
// dispatch.
type unifiedSearchSource struct{}

// openCodeGeminiSequenceTransport records dispatched HTTP requests and returns
// queued responses in order for OpenCode Gemini IR conversion tests.
type openCodeGeminiSequenceTransport struct {
	httpclient.UpstreamTransport                  // embedded unused TLS transport method
	requests                     []*http.Request  // cloned metadata and bodies per outbound call
	bodies                       [][]byte         // captured outbound request JSON payloads
	responses                    []*http.Response // queued upstream HTTP responses
}

// TestTextPricingCompactModel combines global and provider Compact mappings
// with billing sources and passthrough mode. It requires forwarding the chosen
// priced model, or local rejection before transport when the selected billing
// name lacks a price.
func TestTextPricingCompactModel(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		for _, tc := range []struct {
			// name labels the scenario; requested is the client model; global is the
			// default Compact model; providerCompact is the override; upstream is the
			// expected target; billingSource selects its price origin.
			name, requested, global, providerCompact, upstream, billingSource string
			// ordinary selects the normal Responses path; rejected marks expected
			// pricing denial.
			ordinary, rejected bool
		}{
			{name: "unpriced_global", requested: "gpt-5.6-luna", global: "private-compact", rejected: true},
			{name: "priced_global", requested: "private-model", global: "gpt-5.6-luna", upstream: "gpt-5.6-luna"},
			{name: "provider_overrides_global", requested: "gpt-5.6-luna", global: "private-compact", providerCompact: "gpt-5.6-luna", upstream: "gpt-5.6-luna"},
			{name: "unpriced_provider_override", requested: "gpt-5.6-luna", global: "gpt-5.6-luna", providerCompact: "private-compact", rejected: true},
			{name: "requested_billing", requested: "gpt-5.6-luna", global: "private-compact", upstream: "private-compact", billingSource: routing.BillingModelSourceRequested},
			{name: "ordinary_responses", requested: "gpt-5.6-luna", global: "private-compact", upstream: "gpt-5.6-luna", ordinary: true},
		} {
			t.Run(fmt.Sprintf("%s/passthrough=%t", tc.name, passthrough), func(t *testing.T) {
				prices := textPricingFixture(t)
				groupID := int64(100)
				key := &apikey.APIKey{GroupID: &groupID}
				source := protocol.ProtocolResponsesCompact
				path := "/v1/responses/compact"
				if tc.ordinary {
					source = protocol.ProtocolOpenAIResponses
					path = "/v1/responses"
				}
				billingSource := tc.billingSource
				if billingSource == "" {
					billingSource = routing.BillingModelSourceUpstream
				}
				mapping := routing.GroupMappingResult{MappedModel: tc.requested, BillingModelSource: billingSource}
				ctx := requeststate.WithClientProtocol(context.Background(), source)
				ctx = requeststate.WithRoutePlan(ctx, routing.Plan(routing.PlanInput{GroupID: &groupID, ClientProtocol: source, RequestedModel: tc.requested, GroupMapping: mapping}))
				response := fmt.Sprintf(`{"id":"resp-compact","object":"response","status":"completed","model":%q,"output":[],"usage":{"input_tokens":100,"output_tokens":10}}`, tc.upstream)
				transport := &auxiliaryHTTPRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(response))}}
				executor := &UnifiedTextExecutor{Pricing: prices, OpenAI: newResponsesFixture(responsesFixtureInputs{transport: transport, compactModel: tc.global})}
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, path, nil).WithContext(ctx)
				c.Set("gateway_effective_key", key)
				target := gatewayadapter.NewExecutionProvider(&provider.Record{
					ID: 1, Platform: provider.PlatformOpenAI, Type: "apikey",
					Extra:       map[string]any{"openai_passthrough": passthrough},
					Credentials: map[string]any{"api_key": "test-key", provider.UpstreamProtocolsKey: []string{"openai_responses", "openai_responses_compact"}},
				})
				if tc.providerCompact != "" {
					target.Record.Credentials["compact_model_mapping"] = map[string]any{tc.requested: tc.providerCompact}
				}
				body := []byte(fmt.Sprintf(`{"model":%q,"input":"compact this"}`, tc.requested))
				result, err := executor.Responses(ctx, c, target, body)
				if tc.rejected {
					require.ErrorIs(t, err, admission.ErrModelPricingRejected)
					require.Nil(t, result)
					require.Nil(t, transport.lastReq)
					require.Equal(t, http.StatusBadRequest, recorder.Code)
					require.Contains(t, recorder.Body.String(), "model_pricing_unavailable")
					return
				}
				require.NoError(t, err)
				require.NotNil(t, transport.lastReq)
				require.Equal(t, tc.upstream, gjson.GetBytes(transport.lastBody, "model").String())
				// Passthrough omits UpstreamModel for an unchanged model, so Model supplies the effective name.
				require.Equal(t, tc.upstream, completion.ForwardResultBillingModel(result.UpstreamModel, result.Model))
				billingModel := completion.OpenAIUsageBillingModel(&completion.Result{Model: result.Model, BillingModel: result.BillingModel, UpstreamModel: result.UpstreamModel}, mapping.ToUsageFields(tc.requested, result.UpstreamModel))
				require.NoError(t, prices.Check(ctx, &groupID, billingModel))
			})
		}
	}
}

// TestTextPricingResolvesResponsesChatRoute permits only Chat conversion for a
// Responses request and maps the provider model. It requires matching pricing
// and forwarding names while the original target route remains unchanged.
func TestTextPricingResolvesResponsesChatRoute(t *testing.T) {
	for _, passthrough := range []bool{false, true} {
		t.Run(fmt.Sprintf("passthrough=%t", passthrough), func(t *testing.T) {
			prices := textPricingFixture(t)
			group := &routing.Group{ID: 100, ProtocolFallbacks: map[protocol.ProtocolID][]protocol.ProtocolID{protocol.ProtocolOpenAIResponses: {protocol.ProtocolOpenAIChatCompletions}}}
			key := &apikey.APIKey{GroupID: &group.ID, Group: group}
			mapping := routing.GroupMappingResult{MappedModel: "codex-auto-review"}
			ctx := requeststate.WithClientProtocol(context.Background(), protocol.ProtocolOpenAIResponses)
			ctx = requeststate.WithRoutePlan(ctx, routing.Plan(routing.PlanInput{Group: group, ClientProtocol: protocol.ProtocolOpenAIResponses, RequestedModel: "codex-auto-review", GroupMapping: mapping}))
			target := gatewayadapter.NewExecutionProvider(&provider.Record{
				ID: 1, Platform: provider.PlatformOpenAI, Type: "apikey",
				Extra:       map[string]any{"openai_passthrough": passthrough},
				Credentials: map[string]any{"api_key": "test-key", provider.UpstreamProtocolsKey: []string{"openai_chat_completions"}, "model_mapping": map[string]any{"codex-auto-review": "gpt-5.6-luna"}},
			})
			transport := &auxiliaryHTTPRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"id":"chat-review","object":"chat.completion","model":"gpt-5.6-luna","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":10}}`))}}
			executor := &UnifiedTextExecutor{Pricing: prices, OpenAI: newResponsesFixture(responsesFixtureInputs{transport: transport})}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
			c.Set("gateway_effective_key", key)
			result, err := executor.Responses(ctx, c, target, []byte(`{"model":"codex-auto-review","input":"review"}`))
			require.NoError(t, err)
			require.NotNil(t, transport.lastReq)
			require.Equal(t, "/v1/chat/completions", transport.lastReq.URL.Path)
			require.Equal(t, "gpt-5.6-luna", result.BillingModel)
			require.Equal(t, "gpt-5.6-luna", result.UpstreamModel)
			require.Empty(t, target.Route.Protocol())
		})
	}
}

// TestTextPricingRouteErrorPreservesCause disables allowed conversion for a
// Chat-only target. It requires the route error rather than a missing-price
// classification and verifies no client output has been committed.
func TestTextPricingRouteErrorPreservesCause(t *testing.T) {
	group := &routing.Group{ID: 100, ProtocolFallbacks: map[protocol.ProtocolID][]protocol.ProtocolID{protocol.ProtocolOpenAIResponses: {}}}
	ctx := requeststate.WithClientProtocol(context.Background(), protocol.ProtocolOpenAIResponses)
	ctx = requeststate.WithRoutePlan(ctx, routing.Plan(routing.PlanInput{Group: group, ClientProtocol: protocol.ProtocolOpenAIResponses, RequestedModel: "codex-auto-review"}))
	target := gatewayadapter.NewExecutionProvider(&provider.Record{ID: 1, Platform: provider.PlatformOpenAI, Type: "apikey", Credentials: map[string]any{provider.UpstreamProtocolsKey: []string{"openai_chat_completions"}}})
	executor := &UnifiedTextExecutor{Pricing: textPricingFixture(t)}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
	_, err := executor.Responses(ctx, c, target, []byte(`{"model":"codex-auto-review","input":"review"}`))
	require.Error(t, err)
	require.NotErrorIs(t, err, admission.ErrModelPricingRejected)
	require.Contains(t, err.Error(), "no enabled route")
	require.False(t, c.Writer.Written())
}

// TestUnifiedTextDispatchUsesSelectedProvider configures only the selected
// platform executor for each scenario. It requires the expected upstream path,
// delivered content and native-usage presence appropriate to that platform.
func TestUnifiedTextDispatchUsesSelectedProvider(t *testing.T) {
	tests := []struct {
		// platform names the selected provider; model names its request target;
		// path names the expected endpoint; response supplies native reply JSON.
		platform, model, path, response string
	}{
		{"anthropic", "claude-sonnet-4-5", "/v1/messages", `{"id":"msg-native","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":3,"cache_creation_input_tokens":7,"cache_read_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":2,"ephemeral_1h_input_tokens":5},"speed":"fast"}}`},
		{"gemini", "gemini-2.5-flash", ":generateContent", `{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":3,"totalTokenCount":13}}`},
		{"opencode", "gemini-2.5-flash", ":generateContent", `{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":3,"totalTokenCount":13}}`},
		{"openai", "gpt-5.4", "/v1/responses", `{"id":"resp-native","object":"response","status":"completed","model":"gpt-5.4","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":10,"output_tokens":3,"total_tokens":13}}`},
	}
	for _, tt := range tests {
		t.Run(tt.platform, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			transport := &auxiliaryHTTPRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(tt.response))}}
			target := gatewayadapter.NewExecutionProvider(&provider.Record{ID: 42, Name: "selected", Platform: tt.platform, Type: "apikey", Concurrency: 1, Credentials: map[string]any{"api_key": "test-key"}})
			executor := &UnifiedTextExecutor{}
			switch tt.platform {
			case "anthropic":
				executor.Anthropic = NewMessagesExecutor(messageforward.NewRuntime(messageforward.Dependencies{Search: searchtools.NewEmulator(unifiedSearchSource{}, nil, nil, nil, nil, nil), Credentials: &provider.MessageCredentialSource{}, Transport: transport, Deferred: &provider.DeferredService{}}, messageforward.Options{Configured: true, ResponseReadLimit: 1 << 20}), nil)
			case "gemini", "opencode":
				executor.Gemini = &GeminiExecutor{Runtime: &googleforward.Gemini{Transport: transport, Options: googleforward.Options{Configured: true, ResponseReadLimit: 1 << 20}}}
			case "openai":
				executor.OpenAI = newResponsesFixture(responsesFixtureInputs{transport: transport})
			}
			ctx := context.Background()
			if tt.platform == "opencode" {
				SetActualOpenAIUpstreamEndpoint(c, "/v1/chat/completions")
				SetActualUpstreamEndpoint(c, "/v1/responses")
				target.Record.Credentials[provider.OpenCodeModelProtocolsKey] = map[string]any{"payg": map[string]any{tt.model: "gemini_generate_content"}}
				ctx = requeststate.WithClientProtocol(ctx, protocol.ProtocolAnthropicMessages)
				ctx = requeststate.WithRoutePlan(ctx, routing.Plan(routing.PlanInput{RequestedModel: tt.model, ClientProtocol: protocol.ProtocolAnthropicMessages}))
				c.Request.Header.Set("User-Agent", "fixture-agent/1")
				c.Request.Header.Set("Session-Id", "fixture-conversation")
			}
			var result *forward.OpenAIResult
			var err error
			if tt.platform == "openai" {
				result, err = executor.Responses(context.Background(), c, target, []byte(`{"model":"`+tt.model+`","input":"hello"}`))
			} else {
				result, err = executor.Messages(ctx, c, target, []byte(`{"model":"`+tt.model+`","max_tokens":20,"messages":[{"role":"user","content":"hello"}]}`), "", tt.model)
			}
			require.NoError(t, err)
			if tt.platform == "opencode" {
				require.Equal(t, "/inference/google/v1beta/models/"+tt.model+":generateContent", transport.lastReq.URL.Path)
				require.Equal(t, "Bearer test-key", transport.lastReq.Header.Get("Authorization"))
				require.Empty(t, transport.lastReq.Header.Get("x-goog-api-key"))
				require.Equal(t, "fixture-agent/1", transport.lastReq.Header.Get("User-Agent"))
				require.Equal(t, "fixture-conversation", transport.lastReq.Header.Get("X-OpenCode-Session"))
				require.Equal(t, EndpointGeminiModels, GetUpstreamEndpoint(c, provider.PlatformOpenCode))
			}
			require.NotNil(t, result)
			require.Contains(t, transport.lastReq.URL.Path, tt.path)
			require.Equal(t, http.StatusOK, rec.Code)
			require.Contains(t, rec.Body.String(), "ok")
			if tt.platform == "openai" {
				require.Nil(t, result.NativeUsage)
			} else {
				require.NotNil(t, result.NativeUsage)
				require.Equal(t, 10, result.NativeUsage.InputTokens)
			}
		})
	}
}

// Create appends the supplied usage-row pointer for fixture recording
// assertions.
func (w *unifiedRecordWriter) Create(_ context.Context, row *usage.UsageLog) (bool, error) {
	w.rows = append(w.rows, row)
	return true, nil
}

// ProviderUsed satisfies completion effects without scheduling a fixture
// provider update.
func (unifiedRecordEffects) ProviderUsed(int64) {}

// InvalidateAuth satisfies completion effects without changing fixture
// authentication cache state.
func (unifiedRecordEffects) InvalidateAuth(context.Context, string) {}

// Settled satisfies completion effects without adding settlement notifications
// to this fixture.
func (unifiedRecordEffects) Settled(completion.SettlementInput, *billing.UsageBillingApplyResult) {}

// Candidates returns the supplied model as the fixture's sole pricing
// candidate.
func (unifiedRecordModels) Candidates(model string, _ ...string) []string { return []string{model} }

// TestUnifiedNativeUsageSurvivesCaptureAndRecord converts and captures native
// cache buckets, then records their billing input. It requires independent
// usage values and retained native billing categories, with a separate OpenAI
// interpretation subtracting cache counts.
func TestUnifiedNativeUsageSurvivesCaptureAndRecord(t *testing.T) {
	tier := "priority"
	native := &forward.MessagesResult{RequestID: "native-usage", Model: "claude-sonnet-4-5", UpstreamModel: "claude-sonnet-4-5", Usage: protocol.TokenUsage{InputTokens: 100, OutputTokens: 10, CacheReadInputTokens: 30, CacheCreationInputTokens: 12, CacheCreation5mTokens: 5, CacheCreation1hTokens: 7, Speed: "fast"}, Stream: true, ClientDisconnect: true, ServiceTier: &tier, Duration: time.Second}
	result := nativeTextResult(native, "/v1/messages")
	require.NotNil(t, result.NativeUsage)
	require.Equal(t, native.Usage, *result.NativeUsage)
	require.True(t, result.ClientDisconnect)
	native.Usage.CacheCreation1hTokens = 99
	require.Equal(t, 7, result.NativeUsage.CacheCreation1hTokens)
	user := &identity.User{ID: 1}
	input := gatewayadapter.CaptureOpenAI(context.Background(), &gatewayadapter.OpenAICapture{Result: result, APIKey: &apikey.APIKey{ID: 2, User: user}, User: user, Provider: &provider.Record{ID: 3, Platform: "anthropic", Type: "apikey"}})
	require.True(t, input.Result.NativeUsage)
	require.Equal(t, 5, input.Result.Usage.CacheCreation5mTokens)
	require.Equal(t, 7, input.Result.Usage.CacheCreation1hTokens)
	require.Equal(t, "fast", input.Result.Usage.Speed)
	result.NativeUsage.InputTokens = 999
	require.Equal(t, 100, input.Result.Usage.InputTokens)
	logs := &unifiedRecordWriter{}
	recorder := completion.NewRecorder(completion.Dependencies{Calculator: billing.NewCalculator(nil, billing.CalculatorOptions{}), Funds: unifiedRecordFunds{}, Logs: logs, Effects: unifiedRecordEffects{}, Models: unifiedRecordModels{}}, completion.RecorderOptions{DefaultMultiplier: 1})
	require.NoError(t, recorder.Record(context.Background(), input, true))
	require.Len(t, logs.rows, 1)
	row := logs.rows[0]
	require.Equal(t, 100, row.InputTokens, "统一OpenAI外壳不能扣减原生缓存桶")
	require.Equal(t, 30, row.CacheReadTokens)
	require.Equal(t, 12, row.CacheCreationTokens)
	require.Equal(t, 5, row.CacheCreation5mTokens)
	require.Equal(t, 7, row.CacheCreation1hTokens)
	require.Equal(t, "anthropic", row.Platform)
	require.Equal(t, "priority", *row.ServiceTier)

	// When interpreted as OpenAI total input, the same value includes cache tokens that billing subtracts.
	input.Result.NativeUsage = false
	require.NoError(t, recorder.Record(context.Background(), input, true))
	require.Len(t, logs.rows, 2)
	require.Equal(t, 58, logs.rows[1].InputTokens)
}

// Current reports no external searcher for native dispatch fixture execution.
func (unifiedSearchSource) Current() searchtools.Searcher { return nil }

// TestUnifiedTextExecutorPrepareRefreshesEffortMappingAfterGroupDowngrade runs
// prepare on a mixed-platform group that downgrades Anthropic effort from max
// to high. Attempt route resolution must observe the downgraded effort so the
// bound upstream model uses the high-effort override rather than max.
func TestUnifiedTextExecutorPrepareRefreshesEffortMappingAfterGroupDowngrade(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body := []byte(`{"model":"claude-opus-5-5","output_config":{"effort":"max"},"messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(string(body)))
	ctx := requeststate.WithClientProtocol(req.Context(), protocol.ProtocolAnthropicMessages)
	ctx = requeststate.WithRoutingReasoningEffort(ctx, "max")
	group := &routing.Group{
		ID:                          11,
		MaxReasoningEffort:          "high",
		MaxReasoningEffortOverLimit: routing.ReasoningEffortOverLimitDowngrade,
	}
	plan := routing.Plan(routing.PlanInput{
		Group:           group,
		RequestedModel:  "claude-opus-5-5",
		GroupMapping:    routing.GroupMappingResult{MappedModel: "claude-opus-5-5"},
		ClientProtocol:  protocol.ProtocolAnthropicMessages,
		ReasoningEffort: "max",
	})
	ctx = requeststate.WithGroup(ctx, group)
	ctx = requeststate.WithRoutePlan(ctx, plan)
	c.Request = req.WithContext(ctx)
	c.Set("gateway_effective_key", &apikey.APIKey{ID: 5, GroupID: &group.ID, Group: group})

	target := gatewayadapter.NewExecutionProvider(&provider.Record{
		ID:       99,
		Platform: provider.PlatformAnthropic,
		Type:     provider.ProviderTypeAPIKey,
		Credentials: map[string]any{
			"api_key": "agy-key",
			provider.EffortModelMappingCredentialKey: map[string]any{
				"claude-opus-5-5": map[string]any{
					"high": "claude-opus-5-5-high",
					"max":  "claude-opus-5-5-max",
				},
			},
			provider.EffortModelMappingStripEffortCredentialKey: true,
		},
	})

	exec := &UnifiedTextExecutor{}
	routed, updatedBody, err := exec.prepare(c, target, body, protocol.ProtocolAnthropicMessages)
	require.NoError(t, err)
	require.Equal(t, "high", gjson.GetBytes(updatedBody, "output_config.effort").String())
	require.Equal(t, "high", requeststate.RoutingReasoningEffortFromContext(c.Request.Context()))

	candidate, planned := routed.Route.Candidate()
	require.True(t, planned)
	require.Equal(t, "claude-opus-5-5-high", candidate.Models.ProviderMappedModel)
	require.True(t, candidate.EffortMappingMatched)
	require.Equal(t, "claude-opus-5-5-high", gatewayadapter.ExecutionModelPolicy(routed).UpstreamModel(c.Request.Context(), "claude-opus-5-5"))
	require.True(t, gatewayadapter.ExecutionModelPolicy(routed).ShouldStripWireEffort(c.Request.Context(), "claude-opus-5-5"))

	// Outbound request builder must strip output_config.effort when strip flag is set.
	stripped := stripOpenAIWireEffortIfMatched(c.Request.Context(), c, routed, updatedBody, protocol.ProtocolAnthropicMessages)
	require.False(t, gjson.GetBytes(stripped, "output_config").Exists())
}

// TestUnifiedTextExecutorRestoresContextEffortAfterFailedAttempt runs a failed
// Anthropic attempt whose group policy downgraded max to high, then prepares a
// follow-up Gemini attempt on the same gin.Context. Restoring the baseline
// context on failure must keep the second attempt mapped under max rather than
// inheriting Anthropic's downgraded high effort.
func TestUnifiedTextExecutorRestoresContextEffortAfterFailedAttempt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	body := []byte(`{"model":"claude-opus-5-5","output_config":{"effort":"max"},"messages":[{"role":"user","content":"hi"}]}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(string(body)))
	baseCtx := requeststate.WithClientProtocol(req.Context(), protocol.ProtocolAnthropicMessages)
	baseCtx = requeststate.WithRoutingReasoningEffort(baseCtx, "max")
	group := &routing.Group{
		ID:                          12,
		MaxReasoningEffort:          "high",
		MaxReasoningEffortOverLimit: routing.ReasoningEffortOverLimitDowngrade,
	}
	plan := routing.Plan(routing.PlanInput{
		Group:           group,
		RequestedModel:  "claude-opus-5-5",
		GroupMapping:    routing.GroupMappingResult{MappedModel: "claude-opus-5-5"},
		ClientProtocol:  protocol.ProtocolAnthropicMessages,
		ReasoningEffort: "max",
	})
	baseCtx = requeststate.WithGroup(baseCtx, group)
	baseCtx = requeststate.WithRoutePlan(baseCtx, plan)
	c.Request = req.WithContext(baseCtx)
	c.Set("gateway_effective_key", &apikey.APIKey{ID: 6, GroupID: &group.ID, Group: group})

	// Anthropic attempt triggers max->high downgrade during prepare, then fails
	// when no route is enabled for the chosen client protocol.
	failingAnthropic := gatewayadapter.NewExecutionProvider(&provider.Record{
		ID:       101,
		Platform: provider.PlatformAnthropic,
		Type:     provider.ProviderTypeAPIKey,
		Credentials: map[string]any{
			"api_key":            "k",
			"upstream_protocols": []string{},
		},
	})
	exec := &UnifiedTextExecutor{}
	_, err := exec.Messages(c.Request.Context(), c, failingAnthropic, body, "", "claude-opus-5-5")
	require.Error(t, err)
	require.Equal(t, "max", requeststate.RoutingReasoningEffortFromContext(c.Request.Context()))
	require.NotNil(t, requeststate.RequestedReasoningEffortFromContext(c.Request.Context()))
	require.Equal(t, "max", *requeststate.RequestedReasoningEffortFromContext(c.Request.Context()))

	// Follow-up Gemini candidate must still resolve max -> claude-opus-5-5-max.
	geminiTarget := gatewayadapter.NewExecutionProvider(&provider.Record{
		ID:       102,
		Platform: provider.PlatformGemini,
		Type:     provider.ProviderTypeAPIKey,
		Credentials: map[string]any{
			"api_key": "g",
			provider.EffortModelMappingCredentialKey: map[string]any{
				"claude-opus-5-5": map[string]any{
					"high": "claude-opus-5-5-high",
					"max":  "claude-opus-5-5-max",
				},
			},
		},
	})
	routed, _, err := exec.prepare(c, geminiTarget, body, protocol.ProtocolAnthropicMessages)
	require.NoError(t, err)
	cand, planned := routed.Route.Candidate()
	require.True(t, planned)
	require.Equal(t, "claude-opus-5-5-max", cand.Models.ProviderMappedModel)
}

// Do reads and records req's body bytes alongside cloned request headers, then
// returns the queued HTTP response at the current zero-based call index. Nil
// request bodies record an empty byte slice without failing.
func (t *openCodeGeminiSequenceTransport) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	var raw []byte
	if req.Body != nil {
		raw, _ = io.ReadAll(req.Body)
	}
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	t.requests = append(t.requests, cloned)
	t.bodies = append(t.bodies, raw)
	idx := len(t.requests) - 1
	return t.responses[idx], nil
}

// TestUnifiedTextDispatchOpenCodeGeminiIRConversionAndSignatureRetry routes
// Anthropic Messages, OpenAI Responses, OpenAI Chat Completions, and Cursor
// input-shaped Chat requests onto an OpenCode Gemini model.
// Upstream HTTP 400 signature rejections must trigger one compatibility
// retry under ExternalRetry while preserving Bearer auth and X-OpenCode-Session headers.
func TestUnifiedTextDispatchOpenCodeGeminiIRConversionAndSignatureRetry(t *testing.T) {
	const model = "gemini-3-pro"

	for _, clientCase := range []struct {
		name   string              // subtest name
		proto  protocol.ProtocolID // inbound client protocol
		path   string              // inbound HTTP path
		cursor bool                // whether Chat body uses Cursor input shape
	}{
		{name: "messages", proto: protocol.ProtocolAnthropicMessages, path: "/v1/messages"},
		{name: "responses", proto: protocol.ProtocolOpenAIResponses, path: "/v1/responses"},
		{name: "chat", proto: protocol.ProtocolOpenAIChatCompletions, path: "/v1/chat/completions"},
		{name: "cursor_chat_input", proto: protocol.ProtocolOpenAIChatCompletions, path: "/v1/chat/completions", cursor: true},
	} {
		t.Run(clientCase.name, func(t *testing.T) {
			transport := &openCodeGeminiSequenceTransport{
				responses: []*http.Response{
					{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": {"application/json"}},
						Body: io.NopCloser(strings.NewReader(
							`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"id":"oc-call-1","name":"lookup","args":{"q":"status"}},"thoughtSignature":"oc-sig-1"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":14,"candidatesTokenCount":5}}`,
						)),
					},
					{
						StatusCode: http.StatusBadRequest,
						Header:     http.Header{"Content-Type": {"application/json"}},
						Body: io.NopCloser(strings.NewReader(
							`{"error":{"code":400,"message":"Invalid thought_signature on functionCall"}}`,
						)),
					},
					{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": {"application/json"}},
						Body: io.NopCloser(strings.NewReader(
							`{"candidates":[{"content":{"role":"model","parts":[{"text":"opencode-ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":20,"cachedContentTokenCount":6,"candidatesTokenCount":4}}`,
						)),
					},
				},
			}

			geminiRuntime := &googleforward.Gemini{
				Attributes: newAuxiliaryFixture(auxiliaryFixtureInputs{}).Requests.Readers.Attributes,
				Transport:  transport,
				Options: googleforward.Options{
					Configured:        true,
					ResponseReadLimit: 1 << 20,
				},
			}
			executor := &UnifiedTextExecutor{
				Gemini: &GeminiExecutor{Runtime: geminiRuntime},
			}

			makeTarget := func() *gatewayadapter.ExecutionProvider {
				return gatewayadapter.NewExecutionProvider(&provider.Record{
					ID:          88,
					Name:        "opencode-gemini",
					Platform:    provider.PlatformOpenCode,
					Type:        provider.ProviderTypeAPIKey,
					Concurrency: 2,
					Credentials: map[string]any{
						"api_key": "oc-secret",
						provider.OpenCodeModelProtocolsKey: map[string]any{
							"payg": map[string]any{model: "gemini_generate_content"},
						},
					},
				})
			}

			// Turn 1: obtain a tool call so CallMemory records (oc-call-1, oc-sig-1).
			rec1 := httptest.NewRecorder()
			c1, _ := gin.CreateTestContext(rec1)
			c1.Set("api_key", &apikey.APIKey{ID: 77})
			ctx1 := requeststate.WithClientProtocol(t.Context(), clientCase.proto)
			ctx1 = requeststate.WithRoutePlan(ctx1, routing.Plan(routing.PlanInput{
				RequestedModel: model,
				ClientProtocol: clientCase.proto,
			}))
			c1.Request = httptest.NewRequest(http.MethodPost, clientCase.path, nil).WithContext(ctx1)
			c1.Request.Header.Set("User-Agent", "opencode-test/1.0")
			c1.Request.Header.Set("X-Session-Id", "sess-opencode-1")

			turn1Body := fmt.Sprintf(`{"model":%q,"max_tokens":64,"tools":[{"name":"lookup","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"check status"}]}`, model)
			switch {
			case clientCase.proto == protocol.ProtocolOpenAIResponses || clientCase.cursor:
				turn1Body = fmt.Sprintf(`{"model":%q,"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],"input":"check status"}`, model)
			case clientCase.proto == protocol.ProtocolOpenAIChatCompletions:
				turn1Body = fmt.Sprintf(`{"model":%q,"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"check status"}]}`, model)
			}

			var err error
			switch clientCase.proto {
			case protocol.ProtocolAnthropicMessages:
				_, err = executor.Messages(ctx1, c1, makeTarget(), []byte(turn1Body), "", model)
			case protocol.ProtocolOpenAIResponses:
				_, err = executor.Responses(ctx1, c1, makeTarget(), []byte(turn1Body))
			default:
				_, err = executor.Chat(ctx1, c1, makeTarget(), []byte(turn1Body), "", model)
			}
			require.NoError(t, err)
			require.Len(t, transport.requests, 1)
			require.Equal(t, "/inference/google/v1beta/models/"+model+":generateContent", transport.requests[0].URL.Path)
			require.Equal(t, "Bearer oc-secret", transport.requests[0].Header.Get("Authorization"))
			require.Empty(t, transport.requests[0].Header.Get("x-goog-api-key"))
			require.Equal(t, "sess-opencode-1", transport.requests[0].Header.Get("X-OpenCode-Session"))

			var clientCallID string
			switch clientCase.proto {
			case protocol.ProtocolAnthropicMessages:
				clientCallID = gjson.Get(rec1.Body.String(), "content.0.id").String()
			case protocol.ProtocolOpenAIResponses:
				clientCallID = gjson.Get(rec1.Body.String(), "output.0.call_id").String()
			default:
				clientCallID = gjson.Get(rec1.Body.String(), "choices.0.message.tool_calls.0.id").String()
			}
			require.NotEmpty(t, clientCallID)

			// Turn 2: OpenCode upstream rejects oc-sig-1 with HTTP 400, triggering one compatibility retry.
			rec2 := httptest.NewRecorder()
			c2, _ := gin.CreateTestContext(rec2)
			c2.Set("api_key", &apikey.APIKey{ID: 77})
			ctx2 := requeststate.WithClientProtocol(t.Context(), clientCase.proto)
			ctx2 = requeststate.WithRoutePlan(ctx2, routing.Plan(routing.PlanInput{
				RequestedModel: model,
				ClientProtocol: clientCase.proto,
			}))
			c2.Request = httptest.NewRequest(http.MethodPost, clientCase.path, nil).WithContext(ctx2)
			c2.Request.Header.Set("X-Session-Id", "sess-opencode-1")

			turn2Body := fmt.Sprintf(`{"model":%q,"max_tokens":64,"messages":[{"role":"user","content":"check status"},{"role":"assistant","content":[{"type":"tool_use","id":%q,"name":"lookup","input":{"q":"status"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":%q,"content":"green"}]}]}`, model, clientCallID, clientCallID)
			switch {
			case clientCase.proto == protocol.ProtocolOpenAIResponses || clientCase.cursor:
				turn2Body = fmt.Sprintf(`{"model":%q,"input":[{"role":"user","content":"check status"},{"type":"function_call","call_id":%q,"name":"lookup","arguments":"{\"q\":\"status\"}"},{"type":"function_call_output","call_id":%q,"output":"green"}]}`, model, clientCallID, clientCallID)
			case clientCase.proto == protocol.ProtocolOpenAIChatCompletions:
				turn2Body = fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"check status"},{"role":"assistant","tool_calls":[{"type":"function","id":%q,"function":{"name":"lookup","arguments":"{\"q\":\"status\"}"}}]},{"role":"tool","tool_call_id":%q,"content":"green"}]}`, model, clientCallID, clientCallID)
			}

			var res2 *forward.OpenAIResult
			switch clientCase.proto {
			case protocol.ProtocolAnthropicMessages:
				res2, err = executor.Messages(ctx2, c2, makeTarget(), []byte(turn2Body), "", model)
			case protocol.ProtocolOpenAIResponses:
				res2, err = executor.Responses(ctx2, c2, makeTarget(), []byte(turn2Body))
			default:
				res2, err = executor.Chat(ctx2, c2, makeTarget(), []byte(turn2Body), "", model)
			}
			require.NoError(t, err)
			require.NotNil(t, res2)
			require.Len(t, transport.requests, 3)

			// Attempt 1 on turn 2 restored oc-call-1 and oc-sig-1; attempt 2 stripped IDs and wrote placeholder.
			require.Equal(t, "oc-call-1", gjson.GetBytes(transport.bodies[1], "contents.1.parts.0.functionCall.id").String())
			require.Equal(t, "oc-sig-1", gjson.GetBytes(transport.bodies[1], "contents.1.parts.0.thoughtSignature").String())
			require.False(t, gjson.GetBytes(transport.bodies[2], "contents.1.parts.0.functionCall.id").Exists())
			require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(transport.bodies[2], "contents.1.parts.0.thoughtSignature").String())
			require.Contains(t, rec2.Body.String(), "opencode-ok")
			require.NotNil(t, res2.NativeUsage)
			require.Equal(t, 14, res2.NativeUsage.InputTokens)
			require.Equal(t, 6, res2.NativeUsage.CacheReadInputTokens)
			require.Equal(t, 4, res2.NativeUsage.OutputTokens)
		})
	}
}
