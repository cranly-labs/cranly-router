package app

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/apikey"
	keytestkit "github.com/TokenFlux/TokenRouter/internal/apikey/testkit"
	"github.com/TokenFlux/TokenRouter/internal/app/lifecycle"
	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
	billingtestkit "github.com/TokenFlux/TokenRouter/internal/billing/testkit"
	"github.com/TokenFlux/TokenRouter/internal/config"
	gatewayhttp "github.com/TokenFlux/TokenRouter/internal/gateway/httpapi"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/selection"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	gatewaytestkit "github.com/TokenFlux/TokenRouter/internal/gateway/testkit"
	"github.com/TokenFlux/TokenRouter/internal/identity"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/scheduler"
)

// gateway_google_test.go exercises assembled Gemini native HTTP handlers,
// forwarding, and completion recording against a local upstream server. It
// verifies that native Gemini thinkingLevel values survive end-to-end request
// execution and land in persisted usage log fields for buffered, streaming, and
// strip-effort mapped requests.

// TestGeminiNativeHTTP_PersistsReasoningEffort sends buffered, streaming, and
// strip-effort native Gemini requests carrying thinkingLevel through the
// assembled v1beta handler. Persisted usage logs must record both
// requested_reasoning_effort and reasoning_effort while honoring wire effort
// stripping when configured.
func TestGeminiNativeHTTP_PersistsReasoningEffort(t *testing.T) {
	tests := []struct {
		name              string // scenario label for diagnostics
		path              string // inbound native Gemini URL path
		stream            bool   // whether the mock upstream responds with SSE
		stripEffort       bool   // whether effort_model_mapping_strip_effort is enabled
		wantUpstreamPath  string // expected upstream request path
		wantWireLevel     bool   // whether upstream receives thinkingLevel in JSON
		wantRequested     string // expected persisted requested_reasoning_effort
		wantReasoning     string // expected persisted reasoning_effort
		wantUpstreamModel string // expected persisted upstream model
	}{
		{
			name:              "buffered HIGH",
			path:              "/v1beta/models/gemini-4-hydrogen:generateContent",
			stream:            false,
			wantUpstreamPath:  "/v1beta/models/gemini-4-hydrogen:generateContent",
			wantWireLevel:     true,
			wantRequested:     "high",
			wantReasoning:     "high",
			wantUpstreamModel: "gemini-4-hydrogen",
		},
		{
			name:              "streaming HIGH",
			path:              "/v1beta/models/gemini-4-hydrogen:streamGenerateContent?alt=sse",
			stream:            true,
			wantUpstreamPath:  "/v1beta/models/gemini-4-hydrogen:streamGenerateContent",
			wantWireLevel:     true,
			wantRequested:     "high",
			wantReasoning:     "high",
			wantUpstreamModel: "gemini-4-hydrogen",
		},
		{
			name:              "strip effort mapping retains recorded effort",
			path:              "/v1beta/models/gemini-4-hydrogen:generateContent",
			stream:            false,
			stripEffort:       true,
			wantUpstreamPath:  "/v1beta/models/gemini-4-hydrogen-high:generateContent",
			wantWireLevel:     false,
			wantRequested:     "high",
			wantReasoning:     "high",
			wantUpstreamModel: "gemini-4-hydrogen-high",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			observed := make(chan openCodeHTTPExchange, 1)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				observed <- openCodeHTTPExchange{path: r.URL.Path, headers: r.Header.Clone(), body: body}
				w.Header().Set("X-Request-Id", "gemini-native-effort-req")
				if tc.stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":12,\"candidatesTokenCount\":4}}\n\n")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":4}}`)
			}))
			t.Cleanup(upstream.Close)

			groupID := int64(82)
			group := &routing.Group{
				ID: groupID, Hydrated: true, Status: "active", RateMultiplier: 1,
				AllowedProtocols: []protocol.ProtocolID{protocol.ProtocolGeminiGenerateContent},
			}
			cfg := &config.Config{}
			cfg.Billing.FallbackRateMultiplier = 1
			cfg.Security.URLAllowlist.AllowInsecureHTTP = true
			cfg.Gateway.Failover.MaxProviderSwitches = 1

			creds := map[string]any{
				"api_key":            "upstream-secret",
				"base_url":           upstream.URL,
				"provider_type":      "third_party",
				"upstream_protocols": []string{string(protocol.ProtocolGeminiGenerateContent)},
			}
			if tc.stripEffort {
				creds[provider.EffortModelMappingCredentialKey] = map[string]any{
					"gemini-4-hydrogen": map[string]any{"high": "gemini-4-hydrogen-high"},
				}
				creds[provider.EffortModelMappingStripEffortCredentialKey] = true
			}
			record := &provider.Record{
				ID: 81, Name: "Gemini native effort", Platform: provider.PlatformGemini,
				Type: provider.ProviderTypeAPIKey, Status: "active", Schedulable: true,
				Concurrency: 1, GroupIDs: []int64{groupID}, Credentials: creds,
			}
			providers := &mixedHTTPProviders{values: []gatewayprovider.ExecutionProvider{*gatewayprovider.NewExecutionProvider(record)}}
			transport := &mixedHTTPTransport{}

			user := &identity.User{ID: 83, Balance: 100, Status: "active"}
			key := &apikey.APIKey{
				ID: 84, Key: "client-secret", Status: "active", UserID: user.ID,
				User: user, GroupID: &groupID, Group: group, BillingMode: "balance",
			}
			keys := keytestkit.NewService(&openCodeHTTPKeys{key: key}, nil, nil, nil, nil, nil, cfg)
			t.Cleanup(keys.Stop)
			eligibility := newBillingEligibilityFixture(cfg)
			t.Cleanup(eligibility.Stop)
			funding := newFundingAdmissionFixture(eligibility, cfg)
			concurrency := scheduler.NewConcurrencyService(&fakeConcurrencyCache{})
			activity := &gatewayRequestActivity{Operations: lifecycle.NewOperations("gemini-native-effort")}
			planner := gatewayprovider.NewRoutePlanner(nil)
			calculator := billingtestkit.Calculator(nil, map[string]*pricing.ModelPricing{
				"gemini-4-hydrogen":      {InputPricePerToken: 1e-6, OutputPricePerToken: 2e-6},
				"gemini-4-hydrogen-high": {InputPricePerToken: 1e-6, OutputPricePerToken: 2e-6},
			})
			prices := billingtestkit.ResolverWithCards(t, calculator, nil)
			logs := &gatewaytestkit.UsageLogStore{Inserted: true}
			funds := &gatewaytestkit.SettlementStore{}
			recording := gatewaytestkit.NewRecording(logs, funds, nil, false)
			recording.Options.DefaultMultiplier = 1
			recording.Dependencies.Calculator = calculator
			recording.Dependencies.Prices = prices
			recorders := GatewayCompletionRecorders{Forward: recording.Core(nil, false), OpenAI: recording.Core(nil, true)}

			auth := provideAPIKeyAuth(keys, nil, cfg)
			middleware := provideGatewayRouteMiddleware(auth, keys, nil, nil, cfg, nil, nil, nil, nil)
			guards := gatewayhttp.NewRouteGuards(middleware)
			choices := selection.NewGeneric(selection.GenericDependencies{
				Reads:  selection.Reads{Providers: providers},
				Shared: selection.Shared{Concurrency: concurrency},
			}, selection.DefaultOptions())
			shared := provideMessageHTTPBindings(planner, nil, nil, funding, nil, nil, nil, nil, concurrency, cfg, choices, keys, nil)
			digest := session.NewDigestSessionStore()
			gemini := provideGeminiExecutor(provideGeminiForward(providers, nil, nil, nil, transport, nil, cfg, activity, nil, provideGeminiToolCalls()))
			runtime := provideMessageAttemptRuntime(nil, digest, nil, nil, gemini, funding, keys, recorders, shared, nil, nil, nil, cfg, nil, choices, nil, nil, prices)
			handler := provideGeminiNativeHTTP(shared, digest, runtime, activity, choices)

			router := gin.New()
			router.POST("/v1beta/models/*modelAction",
				middleware.GoogleAPIKeyAuth, middleware.RequireGroupGoogle,
				guards.RequireGeminiGenerateContentProtocol, handler.GeminiV1BetaModels,
			)

			reqBody := `{"contents":[{"role":"user","parts":[{"text":"hello"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"HIGH"}}}`
			request := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(reqBody))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("x-goog-api-key", "client-secret")
			response := httptest.NewRecorder()
			router.ServeHTTP(response, request)

			require.Equal(t, http.StatusOK, response.Code, response.Body.String())
			exchange := <-observed
			require.Equal(t, tc.wantUpstreamPath, exchange.path)
			require.Equal(t, tc.wantWireLevel, gjson.GetBytes(exchange.body, "generationConfig.thinkingConfig.thinkingLevel").Exists())
			if tc.wantWireLevel {
				require.Equal(t, "HIGH", gjson.GetBytes(exchange.body, "generationConfig.thinkingConfig.thinkingLevel").String())
			}

			require.Equal(t, 1, logs.Calls)
			require.NotNil(t, logs.LastLog)
			require.NotNil(t, logs.LastLog.UpstreamModel)
			require.Equal(t, tc.wantUpstreamModel, *logs.LastLog.UpstreamModel)
			require.NotNil(t, logs.LastLog.RequestedReasoningEffort)
			require.Equal(t, tc.wantRequested, *logs.LastLog.RequestedReasoningEffort)
			require.NotNil(t, logs.LastLog.ReasoningEffort)
			require.Equal(t, tc.wantReasoning, *logs.LastLog.ReasoningEffort)
		})
	}
}
