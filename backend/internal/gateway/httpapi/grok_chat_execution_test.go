package httpapi

import (
	"bytes"
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
	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream/grok"
)

// Grok Chat bridge admission distinguishes representable client fields from
// requests that need native Chat. The eligibility table checks supported text,
// images and function history alongside rejected legacy or malformed controls,
// and runtime cases add the selected model and cache identity requirements.

// TestGrokChatResponsesBridgeEligibility presents supported and rejected Chat
// field combinations to the codec. It checks both the eligibility result and
// exact rejection reason, including images, functions, histories and nullable
// controls.
func TestGrokChatResponsesBridgeEligibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string // scenario subtest label
		body   string // input Chat JSON
		want   bool   // expected bridge eligibility
		reason string // expected rejection reason
	}{
		{
			name: "plain text chat",
			body: `{"model":"grok","messages":[{"role":"system","content":"concise"},{"role":"user","content":"hi"}],"stream":false}`,
			want: true,
		},
		{
			name: "safe generation options",
			body: `{"model":"grok","messages":[{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true},"max_completion_tokens":256,"temperature":0.2,"top_p":0.9,"prompt_cache_key":"session","tools":[],"functions":null,"tool_choice":"none"}`,
			want: true,
		},
		{
			name: "Trae compatible fields",
			body: `{"model":"grok","messages":[{"role":"user","content":"return json"}],"instructions":"Be concise","response_format":{"type":"json_object"},"service_tier":"fast","stop":null,"reasoning_effort":null}`,
			want: true,
		},
		{
			name: "Trae nullable SDK fields",
			body: `{"model":"grok","messages":[{"role":"user","content":"hi"}],"instructions":null,"response_format":null,"service_tier":null,"stop":null,"reasoning_effort":null}`,
			want: true,
		},
		{
			name:   "invalid instructions fall back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"instructions":{"text":"invalid"}}`,
			reason: "invalid_instructions",
		},
		{
			name:   "invalid response format falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"response_format":"json_object"}`,
			reason: "invalid_response_format",
		},
		{
			name:   "invalid service tier falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"service_tier":1}`,
			reason: "invalid_service_tier",
		},
		{
			name:   "stop falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"stop":"done"}`,
			reason: "unsupported_stop",
		},
		{
			name:   "developer role falls back",
			body:   `{"model":"grok","messages":[{"role":"developer","content":"rules"},{"role":"user","content":"hi"}]}`,
			reason: "unsupported_message_role_developer",
		},
		{
			name: "image content is bridgeable",
			body: `{"model":"grok","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,QQ=="}}]}]}`,
			want: true,
		},
		{
			name: "text and image parts are bridgeable",
			body: `{"model":"grok","messages":[{"role":"user","content":[{"type":"text","text":"what is this"},{"type":"image_url","image_url":{"url":"data:image/png;base64,QQ=="}}]}]}`,
			want: true,
		},
		{
			name: "text only parts are bridgeable",
			body: `{"model":"grok","messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]}`,
			want: true,
		},
		{
			name:   "unknown content part falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"AA=="}}]}]}`,
			reason: "unsupported_content_part_input_audio",
		},
		{
			name:   "empty content array falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":[]}]}`,
			reason: "empty_message_content",
		},
		{
			name: "function tools bridge",
			body: `{"model":"grok","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"},"strict":false}}]}`,
			want: true,
		},
		{
			name:   "legacy functions fall back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"functions":[{"name":"lookup","parameters":{"type":"object"}}]}`,
			reason: "unsupported_functions",
		},
		{
			name: "automatic tool choice bridges",
			body: `{"model":"grok","messages":[{"role":"user","content":"hi"}],"tools":[],"tool_choice":"auto"}`,
			want: true,
		},
		{
			name:   "required tool choice without tools falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"tools":[],"tool_choice":"required"}`,
			reason: "required_tool_choice_without_tools",
		},
		{
			name: "tool history bridges",
			body: `{"model":"grok","messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"call_lookup","type":"function","function":{"name":"lookup","arguments":"{\"key\":\"alpha\"}"}}]},{"role":"tool","tool_call_id":"call_lookup","content":"{\"value\":\"ok\"}"},{"role":"user","content":"summarize"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"tool_choice":"auto","parallel_tool_calls":true}`,
			want: true,
		},
		{
			name: "Trae reasoning and indexed tool history bridges",
			body: `{"model":"grok","messages":[{"role":"assistant","content":null,"reasoning_content":"I should call lookup","tool_calls":[{"index":0,"id":"call_lookup","type":"function","function":{"name":"lookup","arguments":"{\"key\":\"alpha\"}"}}]},{"role":"tool","tool_call_id":"call_lookup","content":"{\"value\":\"ok\"}"},{"role":"user","content":"summarize"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]}`,
			want: true,
		},
		{
			name: "reasoning only assistant history bridges",
			body: `{"model":"grok","messages":[{"role":"assistant","reasoning_content":"Prior reasoning"},{"role":"user","content":"continue"}]}`,
			want: true,
		},
		{
			name:   "invalid reasoning content falls back",
			body:   `{"model":"grok","messages":[{"role":"assistant","reasoning_content":{"text":"invalid"}},{"role":"user","content":"continue"}]}`,
			reason: "invalid_reasoning_content",
		},
		{
			name:   "negative tool call index falls back",
			body:   `{"model":"grok","messages":[{"role":"assistant","content":null,"tool_calls":[{"index":-1,"id":"call_lookup","type":"function","function":{"name":"lookup","arguments":"{}"}}]}]}`,
			reason: "invalid_tool_call_index",
		},
		{
			name:   "unknown tool type falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"web_search","function":{"name":"lookup","parameters":{"type":"object"}}}]}`,
			reason: "unsupported_tool_type",
		},
		{
			name:   "missing tool schema falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"lookup"}}]}`,
			reason: "invalid_tool_function_parameters",
		},
		{
			name:   "named tool choice falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"tool_choice":{"type":"function","function":{"name":"lookup"}}}`,
			reason: "unsupported_tool_choice",
		},
		{
			name:   "invalid tool call arguments fall back",
			body:   `{"model":"grok","messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"call_lookup","type":"function","function":{"name":"lookup","arguments":"{"}}]}]}`,
			reason: "invalid_tool_call_arguments",
		},
		{
			name:   "tool result without call id falls back",
			body:   `{"model":"grok","messages":[{"role":"tool","content":"ok"}]}`,
			reason: "invalid_tool_call_id",
		},
		{
			name:   "non boolean parallel tool calls falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"parallel_tool_calls":"true"}`,
			reason: "invalid_parallel_tool_calls",
		},
		{
			name:   "reasoning effort falls back because conversion adds summary",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"high"}`,
			reason: "unsupported_reasoning_effort",
		},
		{
			name:   "both token limits fall back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"max_tokens":256,"max_completion_tokens":256}`,
			reason: "conflicting_max_tokens",
		},
		{
			name:   "empty message falls back",
			body:   `{"model":"grok","messages":[{"role":"assistant","content":""},{"role":"user","content":"hi"}]}`,
			reason: "empty_message_content",
		},
		{
			name:   "empty tool history falls back",
			body:   `{"model":"grok","messages":[{"role":"assistant","content":"","tool_calls":[]}]}`,
			reason: "empty_message_content",
		},
		{
			name:   "unknown field falls back",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"seed":7}`,
			reason: "unknown_field_seed",
		},
		{
			name:   "small max tokens falls back because conversion clamps it",
			body:   `{"model":"grok","messages":[{"role":"user","content":"hi"}],"max_tokens":32}`,
			reason: "unsafe_max_tokens",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, reason := gatewayprovider.GrokBodyCodec().GrokChatResponsesBridgeEligibility([]byte(tt.body))
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.reason, reason)
		})
	}
}

// TestGrokChatResponsesRuntimeEligibility varies supported Grok model IDs and
// cache identity presence. It checks eligible mapped model variants and
// rejects older/build-free models or a missing identity in the listed
// fixtures.
func TestGrokChatResponsesRuntimeEligibility(t *testing.T) {
	t.Parallel()
	require.True(t, gatewayprovider.GrokBodyCodec().GrokChatResponsesRuntimeEligible("grok-4.5", "isolated-id"))
	require.True(t, gatewayprovider.GrokBodyCodec().GrokChatResponsesRuntimeEligible("grok-4.6", "isolated-id"))
	require.True(t, gatewayprovider.GrokBodyCodec().GrokChatResponsesRuntimeEligible("grok-4.6-latest", "isolated-id"))
	require.False(t, gatewayprovider.GrokBodyCodec().GrokChatResponsesRuntimeEligible("grok-4.3", "isolated-id"))
	require.False(t, gatewayprovider.GrokBodyCodec().GrokChatResponsesRuntimeEligible("grok-4.5-build-free", "isolated-id"))
	require.False(t, gatewayprovider.GrokBodyCodec().GrokChatResponsesRuntimeEligible("grok-4.5", ""))
	require.False(t, gatewayprovider.GrokBodyCodec().GrokChatResponsesRuntimeEligible("grok-4.6", ""))
}

// newGrokIRTestProvider constructs an active Grok OAuth or API Key provider
// record for Grok Chat-to-Responses unit tests. Setting explicitResponses restricts
// upstream_protocols to openai_responses and binds a planned Responses route.
func newGrokIRTestProvider(id int64, providerType string, explicitResponses bool) *gatewayprovider.ExecutionProvider {
	creds := map[string]any{
		"base_url": grok.DefaultCLIBaseURL,
	}
	if providerType == capability.ProviderTypeOAuth {
		creds["access_token"] = "access-token"
		creds["refresh_token"] = "refresh-token"
		creds["expires_at"] = time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	} else {
		creds["api_key"] = "xai-test-key"
		creds["base_url"] = grok.DefaultBaseURL
	}
	if explicitResponses {
		creds[providercore.UpstreamProtocolsKey] = []protocol.ProtocolID{protocol.ProtocolOpenAIResponses}
	}
	out := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           id,
			Name:         fmt.Sprintf("grok-ir-%d", id),
			Platform:     capability.PlatformGrok,
			Type:         providerType,
			Status:       billing.StatusActive,
			Schedulable:  true,
			Concurrency:  1,
			Credentials:  creds,
		},
	}
	if explicitResponses {
		group := &routing.Group{
			ID:               91,
			AllowedProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		}
		plan := routing.Plan(routing.PlanInput{
			Group:          group,
			RequestedModel: "grok-4.5",
			GroupMapping:   routing.GroupMappingResult{MappedModel: "grok-4.5"},
			ClientProtocol: protocol.ProtocolOpenAIChatCompletions,
		})
		ctx := requeststate.WithRoutePlan(
			requeststate.WithClientProtocol(requeststate.WithGroup(context.Background(), group), protocol.ProtocolOpenAIChatCompletions),
			plan,
		)
		resolved, err := gatewayprovider.ProviderForProtocolAttempt(ctx, out)
		if err == nil && resolved != nil {
			out = resolved
		}
	}
	return out
}

// newGrokIRSSECompletedHTTPResponse builds an HTTP 200 SSE response carrying
// quota headers, one assistant message output item, and cached token usage.
// Callers receive an unconsumed NopCloser body ready for one reader pass.
func newGrokIRSSECompletedHTTPResponse(responseID, text, model string, cachedTokens int) *http.Response {
	sseBody := strings.Join([]string{
		fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":%q,"object":"response","created_at":1760000100,"model":%q,"status":"in_progress","output":[]}}`, responseID, model),
		"",
		fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":%q}]}}`, text),
		"",
		fmt.Sprintf(`data: {"type":"response.completed","sequence_number":2,"response":{"id":%q,"object":"response","created_at":1760000100,"model":%q,"status":"completed","output":[],"usage":{"input_tokens":%d,"output_tokens":12,"total_tokens":%d,"input_tokens_details":{"cached_tokens":%d}}}}`, responseID, model, cachedTokens+52, cachedTokens+64, cachedTokens),
		"",
	}, "\n")
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type":                   []string{"text/event-stream"},
			"Xai-Request-Id":                 []string{"xai-req-" + responseID},
			"X-Ratelimit-Limit-Requests":     []string{"100"},
			"X-Ratelimit-Remaining-Requests": []string{"95"},
		},
		Body: io.NopCloser(strings.NewReader(sseBody)),
	}
}

// newGrokIRServiceFixture constructs a ResponsesExecutor fixture wired with
// Text, Output, and Grok executors. It returns both the wired executor and the
// quota-recording provider repository.
func newGrokIRServiceFixture(provider *gatewayprovider.ExecutionProvider, transport *auxiliaryHTTPRecorder) (*OpenAIResponsesExecutor, *grokQuotaProviderRepo) {
	repo := &grokQuotaProviderRepo{
		grokFixtureProviders: &grokFixtureProviders{
			providersByID: map[int64]*gatewayprovider.ExecutionProvider{provider.Record.ID: provider},
		},
	}
	svc := newResponsesFixture(responsesFixtureInputs{
		grokTokens: newHTTPGrokTokenFixture(repo, nil),
		transport:  transport,
		providers:  repo,
	})
	return svc, repo
}

// TestGrokChatResponsesIR_OmitStoreAndIncludeAndCacheRoutes sends buffered OAuth
// and streaming API Key Chat requests through svc.Text.Chat. It verifies omitted
// store and include fields, Trae instructions and service_tier normalization,
// OAuth-only Free-tier companion tool injection, and Grok quota snapshot and raw
// UpstreamResponseModel observations.
func TestGrokChatResponsesIR_OmitStoreAndIncludeAndCacheRoutes(t *testing.T) {
	t.Run("oauth_buffered_and_streaming_cache_route", func(t *testing.T) {
		body := []byte(`{"model":"grok-4.5","messages":[{"role":"system","content":"be concise"},{"role":"user","content":"hi"}],"instructions":"Use JSON","service_tier":"fast","stream":false,"prompt_cache_key":"stable-session","tools":[],"functions":null,"tool_choice":"none"}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
		c.Set("api_key", &apikey.APIKey{ID: 8101})

		prov := newGrokIRTestProvider(81, capability.ProviderTypeOAuth, false)
		up := &auxiliaryHTTPRecorder{resp: newGrokIRSSECompletedHTTPResponse("resp_ir_oauth", "ir cached ok", "grok-4.5-snap", 4096)}
		svc, repo := newGrokIRServiceFixture(prov, up)

		res, err := svc.Text.Chat(context.Background(), c, prov, body, "", "")
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, grok.GrokChatResponsesEndpoint, res.UpstreamEndpoint)
		require.Equal(t, "xai-req-resp_ir_oauth", res.RequestID)
		require.Equal(t, "grok-4.5-snap", res.UpstreamResponseModel)
		require.NotNil(t, res.ServiceTier)
		require.Equal(t, "priority", *res.ServiceTier)
		require.Equal(t, 4096, res.Usage.CacheReadInputTokens)

		require.True(t, gjson.GetBytes(up.lastBody, "stream").Bool())
		require.False(t, gjson.GetBytes(up.lastBody, "store").Exists())
		require.False(t, gjson.GetBytes(up.lastBody, "include").Exists())
		require.Equal(t, "Use JSON", gjson.GetBytes(up.lastBody, "instructions").String())
		require.Equal(t, "priority", gjson.GetBytes(up.lastBody, "service_tier").String())
		require.Equal(t, "web_search", gjson.GetBytes(up.lastBody, "tools.0.type").String())
		require.Equal(t, "x_search", gjson.GetBytes(up.lastBody, "tools.1.type").String())
		require.Equal(t, "none", gjson.GetBytes(up.lastBody, "tool_choice").String())
		identity := gjson.GetBytes(up.lastBody, "prompt_cache_key").String()
		require.NotEmpty(t, identity)
		require.Equal(t, identity, up.lastReq.Header.Get(GrokConversationIDHeader))

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "ir cached ok", gjson.Get(rec.Body.String(), "choices.0.message.content").String())
		require.NotNil(t, repo.updates[prov.Record.ID]["grok_usage_snapshot"])
	})

	t.Run("apikey_explicit_responses_omits_oauth_free_tier_tools", func(t *testing.T) {
		body := []byte(`{"model":"grok-4.5","messages":[{"role":"developer","content":"strict"},{"role":"user","content":"hi"}],"stream":true,"stream_options":{"include_usage":true},"prompt_cache_key":"apikey-session"}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
		c.Set("api_key", &apikey.APIKey{ID: 8102})

		prov := newGrokIRTestProvider(82, capability.ProviderTypeAPIKey, true)
		up := &auxiliaryHTTPRecorder{resp: newGrokIRSSECompletedHTTPResponse("resp_ir_apikey", "apikey stream ok", "grok-4.5", 1024)}
		svc, _ := newGrokIRServiceFixture(prov, up)

		res, err := svc.Text.Chat(c.Request.Context(), c, prov, body, "", "")
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.Stream)
		require.Equal(t, grok.DefaultBaseURL+"/responses", up.lastReq.URL.String())
		require.False(t, gjson.GetBytes(up.lastBody, "store").Exists())
		require.False(t, gjson.GetBytes(up.lastBody, "include").Exists())
		require.False(t, gjson.GetBytes(up.lastBody, "tools").Exists(), "API Key Grok must not inject OAuth Free-tier search tools")
		require.Equal(t, "developer", gjson.GetBytes(up.lastBody, "input.0.role").String())
		require.NotEmpty(t, gjson.GetBytes(up.lastBody, "prompt_cache_key").String())
		require.Contains(t, rec.Body.String(), `"content":"apikey stream ok"`)
		require.Contains(t, rec.Body.String(), `"cached_tokens":1024`)
		require.Contains(t, rec.Body.String(), "data: [DONE]")
	})
}

// TestGrokChatResponsesIR_CursorInputShapedAndXAICarrierRetry exercises Cursor
// /v1/chat/completions requests carrying top-level input without messages on
// Grok Responses. It protects forced upstream stream=true, custom-tool lowering
// and JSON argument wrapping, same-account xai carrier unpacking, cross-account
// carrier dropping, and lossless HTTP 400 invalid_encrypted_content retries.
func TestGrokChatResponsesIR_CursorInputShapedAndXAICarrierRetry(t *testing.T) {
	t.Run("same_provider_xai_carrier_unpacked_and_custom_tool_restored", func(t *testing.T) {
		carrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeXAI,
			ProviderID: 83,
			Data:       "xai_cipher_p83",
		})
		require.NoError(t, err)

		cursorBody := []byte(fmt.Sprintf(`{
			"model":"grok-4.5",
			"stream":false,
			"prompt_cache_retention":"24h",
			"stream_options":{"include_usage":true},
			"input":[
				{"type":"message","role":"user","content":"patch readme"},
				{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":%q},
				{"type":"custom_tool_call","call_id":"call_p1","name":"apply_patch","input":"*** Begin Patch\n*** End Patch"},
				{"type":"custom_tool_call_output","call_id":"call_p1","output":"patched"}
			],
			"tools":[{"type":"custom","name":"apply_patch","description":"apply unified diff"}]
		}`, carrier))

		toolSSE := strings.Join([]string{
			`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_cur_xai","object":"response","created_at":1760000110,"model":"grok-4.5","status":"in_progress","output":[]}}`,
			"",
			`data: {"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"fc_2","type":"function_call","call_id":"call_p2","name":"apply_patch","status":"completed","arguments":"{\"input\":\"*** Next Patch\"}"}}`,
			"",
			`data: {"type":"response.completed","sequence_number":2,"response":{"id":"resp_cur_xai","object":"response","created_at":1760000110,"model":"grok-4.5","status":"completed","output":[],"usage":{"input_tokens":30,"output_tokens":8,"total_tokens":38}}}`,
			"",
		}, "\n")

		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(cursorBody))
		c.Set("api_key", &apikey.APIKey{ID: 8301})

		prov := newGrokIRTestProvider(83, capability.ProviderTypeAPIKey, true)
		up := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(toolSSE)),
		}}
		svc, _ := newGrokIRServiceFixture(prov, up)

		res, err := svc.Text.Chat(c.Request.Context(), c, prov, cursorBody, "", "")
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, gjson.GetBytes(up.lastBody, "stream").Bool())
		require.False(t, gjson.GetBytes(up.lastBody, "store").Exists())
		require.False(t, gjson.GetBytes(up.lastBody, "include").Exists())
		require.False(t, gjson.GetBytes(up.lastBody, "prompt_cache_retention").Exists())
		require.False(t, gjson.GetBytes(up.lastBody, "stream_options").Exists())
		require.Equal(t, "xai_cipher_p83", gjson.GetBytes(up.lastBody, "input.1.encrypted_content").String())
		require.Equal(t, "function", gjson.GetBytes(up.lastBody, "tools.0.type").String())
		require.Equal(t, "apply_patch", gjson.GetBytes(up.lastBody, "tools.0.name").String())

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, "apply_patch", gjson.Get(rec.Body.String(), "choices.0.message.tool_calls.0.function.name").String())
		require.Equal(t, `{"input":"*** Next Patch"}`, gjson.Get(rec.Body.String(), "choices.0.message.tool_calls.0.function.arguments").String())
	})

	t.Run("foreign_provider_xai_carrier_dropped_before_send", func(t *testing.T) {
		foreignCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeXAI,
			ProviderID: 999,
			Data:       "xai_cipher_other_account",
		})
		require.NoError(t, err)
		cursorBody := []byte(fmt.Sprintf(`{
			"model":"grok-4.5",
			"stream":false,
			"input":[
				{"type":"message","role":"user","content":"check"},
				{"type":"reasoning","id":"rs_f","summary":[],"encrypted_content":%q},
				{"type":"function_call","call_id":"c1","name":"lookup","arguments":"{}"},
				{"type":"function_call_output","call_id":"c1","output":"ok"}
			],
			"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]
		}`, foreignCarrier))

		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(cursorBody))

		prov := newGrokIRTestProvider(84, capability.ProviderTypeAPIKey, true)
		up := &auxiliaryHTTPRecorder{resp: newGrokIRSSECompletedHTTPResponse("resp_foreign_drop", "ok", "grok-4.5", 0)}
		svc, _ := newGrokIRServiceFixture(prov, up)

		res, err := svc.Text.Chat(c.Request.Context(), c, prov, cursorBody, "", "")
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, up.bodies, 1)
		require.NotContains(t, string(up.bodies[0]), "xai_cipher_other_account")
		require.NotContains(t, string(up.bodies[0]), "encrypted_content")
	})

	t.Run("stale_xai_ciphertext_400_retries_once_losslessly", func(t *testing.T) {
		sameCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeXAI,
			ProviderID: 85,
			Data:       "xai_stale_cipher_85",
		})
		require.NoError(t, err)
		cursorBody := []byte(fmt.Sprintf(`{
			"model":"grok-4.5",
			"stream":false,
			"input":[
				{"type":"message","role":"user","content":"check order"},
				{"type":"reasoning","id":"rs_stale","summary":[],"encrypted_content":%q},
				{"type":"function_call","call_id":"call_big","name":"lookup","arguments":"{\"order_id\":9007199254740993}"},
				{"type":"function_call_output","call_id":"call_big","output":"found"}
			],
			"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]
		}`, sameCarrier))

		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(cursorBody))

		prov := newGrokIRTestProvider(85, capability.ProviderTypeAPIKey, true)
		up := &auxiliaryHTTPRecorder{
			responses: []*http.Response{
				{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_encrypted_content","message":"Failed to decrypt encrypted_content"}}`)),
				},
				newGrokIRSSECompletedHTTPResponse("resp_after_strip", "recovered ok", "grok-4.5", 0),
			},
		}
		svc, _ := newGrokIRServiceFixture(prov, up)

		res, err := svc.Text.Chat(c.Request.Context(), c, prov, cursorBody, "", "")
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, up.bodies, 2)
		require.Contains(t, string(up.bodies[0]), "xai_stale_cipher_85")
		require.NotContains(t, string(up.bodies[1]), "xai_stale_cipher_85")
		require.NotContains(t, string(up.bodies[1]), "rs_stale")
		require.Contains(t, string(up.bodies[1]), `\"order_id\":9007199254740993`)
		require.Equal(t, "recovered ok", gjson.Get(rec.Body.String(), "choices.0.message.content").String())
	})
}

// TestGrokChatResponsesIR_ClientValidation400AndCommittedSSE submits malformed
// JSON, unsupported "ultra" effort, and image-only models to GrokExecutor.ChatResponses.
// It verifies that pre-stream rejections return HTTP 400
// with MODEL_CAPABILITY_UNSUPPORTED without contacting upstream or penalizing
// health, and that CommittedSSE streams receive an untimed terminal SSE error.
func TestGrokChatResponsesIR_ClientValidation400AndCommittedSSE(t *testing.T) {
	t.Run("ultra_effort_and_malformed_json_return_400_without_upstream_or_penalty", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			body []byte
		}{
			{
				name: "ultra_effort",
				body: []byte(`{"model":"grok-4.5","reasoning_effort":"ultra","messages":[{"role":"user","content":"hi"}]}`),
			},
			{
				name: "malformed_json",
				body: []byte(`{"model":"grok-4.5","messages":[`),
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(tc.body))

				prov := newGrokIRTestProvider(86, capability.ProviderTypeAPIKey, true)
				up := &auxiliaryHTTPRecorder{}
				svc, repo := newGrokIRServiceFixture(prov, up)

				res, handled, err := svc.Grok.ChatResponses(context.Background(), c, prov, tc.body, "", "")
				require.True(t, handled)
				require.Nil(t, res)
				require.Equal(t, "MODEL_CAPABILITY_UNSUPPORTED", apperror.Reason(err))
				require.Equal(t, http.StatusBadRequest, rec.Code)
				require.Equal(t, "invalid_request_error", gjson.Get(rec.Body.String(), "error.type").String())
				require.True(t, IsResponseCommitted(c))
				require.Empty(t, up.requests)
				require.Zero(t, repo.rateLimitedCalls)
				require.Zero(t, repo.tempUnschedCalls)
				require.False(t, httpFixtureRuntimeBlocked(svc, prov))
			})
		}
	})

	t.Run("image_only_model_and_committed_sse_emit_expected_error", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		body := []byte(`{"model":"grok-imagine-image","reasoning_effort":"ultra","messages":[{"role":"user","content":"draw"}],"stream":true}`)
		c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))

		// Simulate DownstreamSSEKeepalive having committed HTTP 200 text/event-stream.
		c.Writer.Header().Set("Content-Type", "text/event-stream")
		c.Writer.WriteHeader(http.StatusOK)
		_, _ = c.Writer.Write([]byte(": keepalive\n\n"))

		prov := newGrokIRTestProvider(87, capability.ProviderTypeAPIKey, true)
		up := &auxiliaryHTTPRecorder{}
		svc, _ := newGrokIRServiceFixture(prov, up)
		imgOut := []string{"image"}
		svc.Grok.Attributes = &routing.ModelAttributeService{
			Catalog: routing.ModelAttributeCatalog{
				Lookup: func(string) modelcatalog.Attributes {
					return modelcatalog.Attributes{OutputModalities: &imgOut}
				},
			},
		}

		res, handled, err := svc.Grok.ChatResponses(context.Background(), c, prov, body, "", "")
		require.True(t, handled)
		require.Nil(t, res)
		require.Equal(t, "MODEL_CAPABILITY_UNSUPPORTED", apperror.Reason(err))
		require.Empty(t, up.requests)
		require.Contains(t, rec.Body.String(), `"invalid_request_error"`)
	})
}

// TestGrokChatResponsesIR_SingleHealthEvaluationAndMissingUsage exercises
// upstream HTTP >= 400 error handling and empty-usage HTTP 200 responses on
// ChatResponses. It verifies single-pass health and Ops evaluation, Retry-After
// preservation on HTTP 429, content-policy 403 isolation, and grok_missing_usage
// failover before buffered Chat JSON is committed.
func TestGrokChatResponsesIR_SingleHealthEvaluationAndMissingUsage(t *testing.T) {
	t.Run("custom_error_codes_generic_500_evaluates_ops_and_health_once", func(t *testing.T) {
		body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))

		prov := newGrokIRTestProvider(88, capability.ProviderTypeAPIKey, true)
		prov.Record.Credentials["custom_error_codes_enabled"] = true
		prov.Record.Credentials["custom_error_codes"] = []any{float64(429)}

		up := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "Xai-Request-Id": []string{"xai-500-once"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"internal upstream boom"}}`)),
		}}
		svc, _ := newGrokIRServiceFixture(prov, up)

		res, handled, err := svc.Grok.ChatResponses(context.Background(), c, prov, body, "", "")
		require.True(t, handled)
		require.Nil(t, res)
		require.Error(t, err)
		require.Equal(t, http.StatusInternalServerError, rec.Code)
		require.Equal(t, "Upstream gateway error", gjson.Get(rec.Body.String(), "error.message").String())
		require.Len(t, GetOpsUpstreamErrors(c), 1, "Ops upstream error event must be appended exactly once")
	})

	t.Run("http_429_preserves_retry_after_and_content_policy_403_skips_penalty", func(t *testing.T) {
		body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`)
		rec429 := httptest.NewRecorder()
		c429, _ := gin.CreateTestContext(rec429)
		c429.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))

		prov429 := newGrokIRTestProvider(89, capability.ProviderTypeOAuth, true)
		up429 := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "Retry-After": []string{"45"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"rate limited"}}`)),
		}}
		svc429, repo429 := newGrokIRServiceFixture(prov429, up429)

		_, handled, err := svc429.Grok.ChatResponses(context.Background(), c429, prov429, body, "", "")
		require.True(t, handled)
		var foErr *forward.UpstreamFailoverError
		require.ErrorAs(t, err, &foErr)
		require.Equal(t, "45", http.Header(foErr.ResponseHeaders).Get("Retry-After"))
		require.Equal(t, 1, repo429.rateLimitedCalls)

		rec403 := httptest.NewRecorder()
		c403, _ := gin.CreateTestContext(rec403)
		c403.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))
		prov403 := newGrokIRTestProvider(90, capability.ProviderTypeOAuth, true)
		up403 := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusForbidden,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"content_policy_violation","message":"Content violates usage guidelines"}}`)),
		}}
		svc403, repo403 := newGrokIRServiceFixture(prov403, up403)

		_, handled403, err403 := svc403.Grok.ChatResponses(context.Background(), c403, prov403, body, "", "")
		require.True(t, handled403)
		require.Error(t, err403)
		require.Equal(t, http.StatusForbidden, rec403.Code)
		require.Zero(t, repo403.rateLimitedCalls)
		require.Zero(t, repo403.tempUnschedCalls)
		require.False(t, httpFixtureRuntimeBlocked(svc403, prov403))
	})

	t.Run("buffered_missing_usage_returns_grok_missing_usage_failover", func(t *testing.T) {
		body := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(body))

		prov := newGrokIRTestProvider(91, capability.ProviderTypeOAuth, true)
		noUsageSSE := strings.Join([]string{
			`data: {"type":"response.output_text.delta","sequence_number":0,"delta":"ok"}`,
			"",
			`data: {"type":"response.completed","sequence_number":1,"response":{"id":"resp_no_u","object":"response","model":"grok-4.5","status":"completed","output":[{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}]}}`,
			"",
		}, "\n")
		up := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "Xai-Request-Id": []string{"rid-ir-no-usage"}},
			Body:       io.NopCloser(strings.NewReader(noUsageSSE)),
		}}
		svc, _ := newGrokIRServiceFixture(prov, up)

		res, handled, err := svc.Grok.ChatResponses(context.Background(), c, prov, body, "", "")
		require.True(t, handled)
		require.Nil(t, res)
		var foErr *forward.UpstreamFailoverError
		require.ErrorAs(t, err, &foErr)
		require.Equal(t, "grok_missing_usage", gjson.GetBytes(foErr.ResponseBody, "error.code").String())
		require.Equal(t, "rid-ir-no-usage", http.Header(foErr.ResponseHeaders).Get("x-request-id"))
		require.False(t, c.Writer.Written())
	})
}

// TestGrokChatResponsesIR_RuntimeEligibilityFallsBackWhenUnplanned calls
// GrokExecutor.ChatResponses without a planned route for uncached grok-4.5 and
// cached grok-4.3 requests. Both ineligible attempts must return handled=false
// with nil error and zero upstream requests so callers fall back to raw Chat.
func TestGrokChatResponsesIR_RuntimeEligibilityFallsBackWhenUnplanned(t *testing.T) {
	for _, tc := range []struct {
		name       string // subtest scenario label
		body       []byte // inbound Chat request JSON
		withAPIKey bool   // whether to bind an API key onto c
	}{
		{
			name:       "uncached_grok_45",
			body:       []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}],"stream":false}`),
			withAPIKey: false,
		},
		{
			name:       "cached_grok_43",
			body:       []byte(`{"model":"grok-4.3","prompt_cache_key":"s-43","messages":[{"role":"user","content":"hi"}],"stream":false}`),
			withAPIKey: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, grok.GrokChatRawEndpoint, bytes.NewReader(tc.body))
			if tc.withAPIKey {
				c.Set("api_key", &apikey.APIKey{ID: 9201})
			}

			prov := newGrokIRTestProvider(92, capability.ProviderTypeOAuth, false)
			up := &auxiliaryHTTPRecorder{}
			svc, _ := newGrokIRServiceFixture(prov, up)

			res, handled, err := svc.Grok.ChatResponses(context.Background(), c, prov, tc.body, "", "")
			require.False(t, handled)
			require.NoError(t, err)
			require.Nil(t, res)
			require.Empty(t, up.requests)
		})
	}
}
