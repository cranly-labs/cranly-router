package qoder

import (
	"encoding/json"
	"maps"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	protocolqoder "github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/provider/qodermodel"
)

// Payload fixtures compare protocol history with Qoder's scalar content,
// structured text, and call/result fields. Site input limits and thinking
// controls are checked at every mirrored configuration path, while tool-policy
// fixtures exercise conditional tool pruning and allowed_tools subset selection.

// parseQoderRequestViaIRForTest decodes client JSON for proto through
// convert.NewExchange and PayloadRequestFromIR for payload builder tests.
func parseQoderRequestViaIRForTest(t *testing.T, proto protocol.ProtocolID, body []byte) QoderPayloadRequest {
	t.Helper()
	ex, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   proto,
			Upstream: protocol.ProtocolQoderChat,
			Dialect: convert.Dialect{
				Name:               "qoder",
				Tools:              convert.ToolSupportFunction,
				AllowBillingHeader: true,
				StatelessResponses: proto == protocol.ProtocolOpenAIResponses,
			},
		},
	})
	require.NoError(t, err)
	upBytes, err := ex.UpstreamRequest()
	require.NoError(t, err)
	protoReq, err := protocolqoder.DecodePayload(upBytes)
	require.NoError(t, err)
	req, err := PayloadRequestFromIR(protoReq)
	require.NoError(t, err)
	return req
}

// TestQoderHighestContextFlowsThroughAllProtocols parses Qwen input through
// Chat, Responses, and Messages. It checks the same route and million-token
// runtime configuration in every resulting global payload.
func TestQoderHighestContextFlowsThroughAllProtocols(t *testing.T) {
	tests := []struct {
		name  string              // fixture case label
		proto protocol.ProtocolID // client wire protocol
		body  []byte              // client request bytes
	}{
		{
			name:  "Chat Completions",
			proto: protocol.ProtocolOpenAIChatCompletions,
			body:  []byte(`{"model":"qwen3.8-max","messages":[{"role":"user","content":"hello"}]}`),
		},
		{
			name:  "Responses",
			proto: protocol.ProtocolOpenAIResponses,
			body:  []byte(`{"model":"qwen3.8-max","input":"hello"}`),
		},
		{
			name:  "Anthropic Messages",
			proto: protocol.ProtocolAnthropicMessages,
			body:  []byte(`{"model":"qwen3.8-max","max_tokens":1024,"messages":[{"role":"user","content":"hello"}]}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := parseQoderRequestViaIRForTest(t, tt.proto, tt.body)
			request.Site = SiteGlobal
			request.Capabilities = observedPayloadModel(t, SiteGlobal, request.Model)
			payload, modelKey, buildErr := BuildQoderPayloadWithOptions(request, "", request.Messages, true, true)
			require.NoError(t, buildErr)

			require.Equal(t, "qmodel_38max", modelKey)
			assertQoderContextCapabilityForTest(t, payload, 1000000, true)
		})
	}
}

// TestQoderDesktopPayloadUsesCanonicalContent reproduces the current desktop
// request shape for Cantus. User text stays in content, chat prompts remain
// strings, and observed display metadata replaces obsolete fixed model hints.
func TestQoderDesktopPayloadUsesCanonicalContent(t *testing.T) {
	request := parseQoderRequestViaIRForTest(t, protocol.ProtocolOpenAIChatCompletions, []byte(`{"model":"cmodel","messages":[{"role":"user","content":"hello"}]}`))
	request.Capabilities = observedPayloadModel(t, SiteGlobal, request.Model)
	payload, model, buildErr := BuildQoderPayloadWithOptions(request, "fixture", request.Messages, true, true)
	require.NoError(t, buildErr)
	require.Equal(t, "cmodel", model)
	config := qoderFixtureValue[map[string]any](t, payload["model_config"])
	require.Equal(t, "Cantus", config["display_name"])
	require.Equal(t, true, config["is_vl"])
	require.Equal(t, "", payload["aliyun_user_type"])
	chat := qoderFixtureValue[map[string]any](t, payload["chat_context"])
	require.Equal(t, "hello", chat["text"])
	require.Equal(t, "hello", qoderFixtureValue[map[string]any](t, chat["extra"])["originalContent"])
	message := qoderFixtureValue[map[string]any](t, qoderFixtureValue[[]any](t, payload["messages"])[0])
	require.NotContains(t, message, "contents")
	require.NotContains(t, message, "response_meta")
	require.Equal(t, "hello", qoderFixtureValue[map[string]any](t, qoderFixtureValue[[]any](t, message["content"])[0])["text"])
	require.Equal(t, true, qoderFixtureValue[map[string]any](t, payload["parameters"])["enable_thinking"])
}

// TestQoderPayloadCacheMarkerDoesNotMutateInput attaches an automatic cache
// marker to outgoing text. The original text block remains unchanged for
// conversation fingerprinting and subsequent attempt preparation.
func TestQoderPayloadCacheMarkerDoesNotMutateInput(t *testing.T) {
	block := map[string]any{"type": "text", "text": "hello"}
	message := QoderPayloadMessageFromMessage(QoderMessage{Role: "user", Text: "hello", TextContent: []map[string]any{block}})
	AddQoderEphemeralCacheControl([]any{message})
	require.NotContains(t, block, "cache_control")
	require.Contains(t, qoderFixtureValue[map[string]any](t, qoderFixtureValue[[]any](t, message["content"])[0]), "cache_control")
}

// TestQoderContextCapabilityUsesSiteAndRouteFallback compares observed selectable
// routes with unknown and retired names. Only observed capabilities introduce
// runtime context fields; a missing observation keeps the execution default.
func TestQoderContextCapabilityUsesSiteAndRouteFallback(t *testing.T) {
	for _, test := range []struct {
		model      string // request model identifier
		limit      int    // expected input tokens
		selectable bool   // expected context override
	}{{"auto", 1000000, true}, {"kimi-k2.8-preview", 1000000, true}, {"minimax-m3.1-flash", 1000000, true}, {"custom-route", 200000, false}, {"kimi-k2.7-code", 200000, false}} {
		request := parseQoderRequestViaIRForTest(t, protocol.ProtocolOpenAIChatCompletions, []byte(`{"model":"`+test.model+`","messages":[{"role":"user","content":"hello"}]}`))
		request.Capabilities = observedPayloadModel(t, SiteGlobal, test.model)
		payload, _, buildErr := BuildQoderPayloadWithOptions(request, "", request.Messages, true, true)
		require.NoError(t, buildErr)
		assertQoderContextCapabilityForTest(t, payload, test.limit, test.selectable)
	}
}

// TestQoderContextCapabilityMergesWithThinkingOverride enables thinking on a
// runtime-context route. It checks the mapped high effort and million-token limit
// coexist in parameters and ideModelConfigOverride.
func TestQoderContextCapabilityMergesWithThinkingOverride(t *testing.T) {
	request := parseQoderRequestViaIRForTest(t, protocol.ProtocolOpenAIChatCompletions, []byte(`{"model":"deepseek-v4-pro","reasoning_effort":"high","messages":[{"role":"user","content":"hello"}]}`))
	request.Capabilities = observedPayloadModel(t, SiteGlobal, request.Model)
	payload, _, buildErr := BuildQoderPayloadWithOptions(request, "", request.Messages, true, true)
	require.NoError(t, buildErr)
	assertQoderContextCapabilityForTest(t, payload, 1000000, true)

	parameters := requireQoderPayloadMapForTest(t, payload["parameters"], "parameters")
	require.Equal(t, "high", parameters["reasoning_effort"])
	chatContext := requireQoderPayloadMapForTest(t, chatContextFromPayloadForTest(t, payload), "chat_context")
	extra := requireQoderPayloadMapForTest(t, chatContext["extra"], "chat_context.extra")
	runtimeOverride := requireQoderPayloadMapForTest(t, extra["ideModelConfigOverride"], "chat_context.extra.ideModelConfigOverride")
	require.Equal(t, "high", runtimeOverride["reasoning_effort"])
	require.Equal(t, 1000000, runtimeOverride["max_input_tokens"])
}

// chatContextFromPayloadForTest returns the chat_context field for nested map assertions.
func chatContextFromPayloadForTest(t *testing.T, payload map[string]any) any {
	t.Helper()
	return payload["chat_context"]
}

// assertQoderContextCapabilityForTest checks model input limit and optional
// runtime configuration locations. Runtime-enabled fixtures need both parameter
// and IDE overrides; fixed-route fixtures require both to remain absent.
func assertQoderContextCapabilityForTest(t *testing.T, payload map[string]any, wantTokens int, wantRuntime bool) {
	t.Helper()
	modelConfig := requireQoderPayloadMapForTest(t, payload["model_config"], "model_config")
	require.EqualValues(t, wantTokens, modelConfig["max_input_tokens"])

	parameters := requireQoderPayloadMapForTest(t, payload["parameters"], "parameters")
	contextLength, hasContextLength := parameters["context_length"]
	chatContext := requireQoderPayloadMapForTest(t, payload["chat_context"], "chat_context")
	extra := requireQoderPayloadMapForTest(t, chatContext["extra"], "chat_context.extra")
	runtimeOverride, hasRuntimeOverride := extra["ideModelConfigOverride"].(map[string]any)
	if wantRuntime {
		require.True(t, hasContextLength)
		require.EqualValues(t, wantTokens, contextLength)
		require.True(t, hasRuntimeOverride)
		require.EqualValues(t, wantTokens, runtimeOverride["max_input_tokens"])
		return
	}

	require.False(t, hasContextLength)
	require.False(t, hasRuntimeOverride)
}

// requireQoderPayloadMapForTest requires a decoded object and returns it for
// payload assertions. Unexpected values fail with the supplied path, locating
// the incorrect nested representation in the fixture.
func requireQoderPayloadMapForTest(t *testing.T, value any, path string) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	require.True(t, ok, "%s must be a JSON object", path)
	return result
}

// TestBuildQoderPayloadUsesCNModelAndClientVersion converts the CN-only Qwen
// Flash alias into a payload. It checks the CN route key and business client
// version emitted for that site.
func TestBuildQoderPayloadUsesCNModelAndClientVersion(t *testing.T) {
	body := []byte(`{
		"model":"qwen3.8-flash",
		"messages":[{"role":"user","content":"hello"}]
	}`)

	request := parseQoderRequestViaIRForTest(t, protocol.ProtocolOpenAIChatCompletions, body)
	request.Site = SiteCN
	request.Capabilities = observedPayloadModel(t, SiteCN, request.Model)
	payload, modelKey, buildErr := BuildQoderPayloadWithOptions(request, "", request.Messages, true, true)
	require.NoError(t, buildErr)

	require.Equal(t, "qfmodel", modelKey)
	business, ok := payload["business"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "1.1.86-hotfix.5.3", business["version"])
}

// TestBuildQoderPayloadAddsCacheControlToLastEligibleTextBlock builds a Qoder
// payload from an Anthropic Messages request without explicit cache markers.
// AddQoderEphemeralCacheControl must attach an ephemeral cache marker only to
// the final nonblank text block.
func TestBuildQoderPayloadAddsCacheControlToLastEligibleTextBlock(t *testing.T) {
	body := []byte(`{
		"model":"claude-sonnet-4-5",
		"max_tokens":256,
		"messages":[
			{"role":"user","content":[{"type":"text","text":"first"},{"type":"text","text":"second"}]}
		]
	}`)
	request := parseQoderRequestViaIRForTest(t, protocol.ProtocolAnthropicMessages, body)
	payload, _, err := BuildQoderPayloadWithOptions(request, "", request.Messages, true, true)
	require.NoError(t, err)

	messages := qoderFixtureValue[[]any](t, payload["messages"])
	require.Len(t, messages, 1)
	userMsg := qoderFixtureValue[map[string]any](t, messages[0])
	blocks := qoderFixtureValue[[]any](t, userMsg["content"])
	require.Len(t, blocks, 2)
	require.NotContains(t, qoderFixtureValue[map[string]any](t, blocks[0]), "cache_control")
	require.Equal(t, map[string]any{"type": "ephemeral"}, qoderFixtureValue[map[string]any](t, blocks[1])["cache_control"])
}

// assertQoderThinkingPayload checks both reasoning switches and every optional
// effort configuration copy. It requires the expected effort text when enabled
// for that fixture, or absence of all four effort paths.
func assertQoderThinkingPayload(t *testing.T, payload map[string]any, enabled bool, effort string, hasEffort bool) {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	require.True(t, gjson.GetBytes(raw, "model_config.is_reasoning").Exists())
	require.Equal(t, enabled, gjson.GetBytes(raw, "model_config.is_reasoning").Bool())
	require.True(t, gjson.GetBytes(raw, "chat_context.extra.modelConfig.is_reasoning").Exists())
	require.Equal(t, enabled, gjson.GetBytes(raw, "chat_context.extra.modelConfig.is_reasoning").Bool())

	paths := []string{
		"parameters.reasoning_effort",
		"model_config.reasoning_effort",
		"chat_context.extra.modelConfig.reasoning_effort",
		"chat_context.extra.ideModelConfigOverride.reasoning_effort",
	}
	for _, path := range paths {
		if hasEffort {
			require.Equal(t, effort, gjson.GetBytes(raw, path).String(), path)
			continue
		}
		require.False(t, gjson.GetBytes(raw, path).Exists(), path)
	}
}

// TestBuildQoderThinkingPayloadBySiteAndModelCapability applies current model
// metadata to Chat input on both sites. Defaults, disable intent, effort
// promotion, and unsupported raw routes retain consistent mirrored fields.
func TestBuildQoderThinkingPayloadBySiteAndModelCapability(t *testing.T) {
	for _, site := range []Site{SiteGlobal, SiteCN} {
		for _, test := range []struct {
			model   string         // current public alias
			extra   map[string]any // native reasoning controls
			enabled bool           // expected thinking switch
			effort  string         // expected encoded effort
		}{
			{"qwen3.8-max", nil, true, "medium"},
			{"qwen3.8-max", map[string]any{"reasoning_effort": "high"}, true, "xhigh"},
			{"qwen4-max", map[string]any{"reasoning_effort": "low"}, true, "low"},
			{"qwen4-max", map[string]any{"reasoning_effort": "none"}, false, "none"},
			{"deepseek-v4-pro", map[string]any{"reasoning_effort": "low"}, true, "high"},
			{"glm-5.3", map[string]any{"reasoning_effort": "medium"}, true, "high"},
			{"minimax-m3.1-flash", nil, true, "xhigh"},
		} {
			body := map[string]any{"model": test.model, "messages": []map[string]string{{"role": "user", "content": "hello"}}}
			maps.Copy(body, test.extra)
			raw, err := json.Marshal(body)
			require.NoError(t, err)
			request := parseQoderRequestViaIRForTest(t, protocol.ProtocolOpenAIChatCompletions, raw)
			request.Site = site
			request.Capabilities = observedPayloadModel(t, site, test.model)
			payload, _, buildErr := BuildQoderPayloadWithOptions(request, "", request.Messages, true, true)
			require.NoError(t, buildErr)
			assertQoderThinkingPayload(t, payload, test.enabled, test.effort, true)
		}
	}
}

// observedPayloadModel resolves sanitized desktop metadata for payload tests.
// Unknown or retired names have no observation and therefore exercise the
// execution fallback instead of reintroducing historical service-key aliases.
func observedPayloadModel(t *testing.T, site Site, model string) *modelrules.Resolution {
	t.Helper()
	raw, err := os.ReadFile("testdata/models_" + string(site) + ".json")
	require.NoError(t, err)
	models, err := ParseModelCatalog(raw)
	require.NoError(t, err)
	index := qodermodel.NewCatalog(string(site), models)
	entry, found := index.Lookup(model)
	if !found {
		return nil
	}
	resolution := modelrules.View{}.Resolve(modelrules.Target{Platform: "qoder", Site: string(site), Model: entry.Key, ProviderModels: index}, modelcatalog.Attributes{})
	return &resolution
}

// TestBuildQoderThinkingPayloadKeepsGlobalOnlyModelIsolated requests max effort
// on a global Kimi route lacking adjustable controls. It checks disabled
// switches and absence of effort paths.
func TestBuildQoderThinkingPayloadKeepsGlobalOnlyModelIsolated(t *testing.T) {
	body := []byte(`{
		"model":"kimi-k3",
		"reasoning_effort":"max",
		"messages":[{"role":"user","content":"hello"}]
	}`)

	request := parseQoderRequestViaIRForTest(t, protocol.ProtocolOpenAIChatCompletions, body)
	request.Site = SiteGlobal
	payload, _, err := BuildQoderPayloadWithOptions(request, "", request.Messages, true, true)
	require.NoError(t, err)
	assertQoderThinkingPayload(t, payload, false, "", false)
}

// TestEffectiveToolPolicyAcrossTextProtocols exercises each public request shape.
// A mandatory choice must fail before an upstream request and optional tools
// are removed from both the native payload and the session tool fingerprint.
func TestEffectiveToolPolicyAcrossTextProtocols(t *testing.T) {
	res := modelrules.View{}.Resolve(modelrules.Target{Platform: "qoder", Model: "route"}, modelcatalog.Attributes{ToolCall: new(false)})
	executor := NewExecutor(ExecuteOptions{})
	for _, tc := range []struct {
		wire  protocol.ProtocolID
		body  string
		named string
	}{
		{protocol.ProtocolOpenAIChatCompletions, `{"model":"route","messages":[{"role":"user","content":"test"}],"tools":[{"type":"function","function":{"name":"demo"}}]}`, `{"type":"function","function":{"name":"demo"}}`},
		{protocol.ProtocolOpenAIResponses, `{"model":"route","input":"test","tools":[{"type":"function","name":"demo"}]}`, `{"type":"function","name":"demo"}`},
		{protocol.ProtocolAnthropicMessages, `{"model":"route","max_tokens":16,"messages":[{"role":"user","content":"test"}],"tools":[{"name":"demo","input_schema":{"type":"object"}}]}`, `{"type":"tool","name":"demo"}`},
	} {
		t.Run(string(tc.wire), func(t *testing.T) {
			for _, choice := range []string{"", tc.named, `"required"`, `"auto"`, `"none"`} {
				nativeChoice := choice
				if tc.wire == protocol.ProtocolAnthropicMessages && choice != tc.named && choice != "" {
					if choice == `"required"` {
						nativeChoice = `{"type":"any"}`
					} else {
						nativeChoice = `{"type":` + choice + `}`
					}
				}
				var body map[string]any
				require.NoError(t, json.Unmarshal([]byte(tc.body), &body))
				if nativeChoice != "" {
					body["tool_choice"] = json.RawMessage(nativeChoice)
				}
				raw, err := json.Marshal(body)
				require.NoError(t, err)
				req := parseQoderRequestViaIRForTest(t, tc.wire, raw)
				req.Capabilities = &res
				req.ExplicitSession = "fixture"
				payload, _, plan, err := executor.PrepareConversation(&Target{ProviderID: 1}, tc.wire, req)
				if choice == tc.named || choice == `"required"` {
					var invalid *RequestError
					require.ErrorAs(t, err, &invalid)
					require.Equal(t, "unsupported_tool_choice", invalid.Code)
					require.Nil(t, plan)
				} else {
					require.NoError(t, err)
					require.Empty(t, payload["tools"])
					require.NotContains(t, qoderFixtureValue[map[string]any](t, payload["parameters"]), "tool_choice")
					require.Equal(t, QoderFingerprintAny([]any{}), plan.ToolsFingerprint)
				}
			}
		})
	}
}

// TestConditionalToolsUseNativeEffort verifies conditional tool policy after
// effort projection. A missing intermediate level must use its supported higher
// level both for the wire parameters and for deciding whether tools are allowed.
func TestConditionalToolsUseNativeEffort(t *testing.T) {
	model := qodermodel.Model{Key: "route", Reasoning: new(true), DefaultThinking: true, DefaultEffort: "low", Efforts: []string{"low", "high"}}
	index := qodermodel.NewCatalog("fixture", []qodermodel.Model{model})
	res := modelrules.View{}.Resolve(modelrules.Target{Model: model.Key, ProviderModels: index}, modelcatalog.Attributes{ToolCall: new(true), Conditions: new(modelcatalog.ParameterConditions{"tool_call": {Efforts: []string{"high"}}})})
	request := QoderPayloadRequest{Model: model.Key, Capabilities: &res, Tools: []any{map[string]any{"type": "function", "function": map[string]any{"name": "demo"}}}}
	disabled, _, err := BuildQoderPayloadWithOptions(request, "", nil, true, true)
	require.NoError(t, err)
	require.Empty(t, disabled["tools"])
	request.Thinking = QoderThinkingDirective{Specified: true, Enabled: true, Effort: "medium"}
	enabled, _, err := BuildQoderPayloadWithOptions(request, "", nil, true, true)
	require.NoError(t, err)
	require.Len(t, enabled["tools"], 1)
	require.Equal(t, "high", qoderFixtureValue[map[string]any](t, enabled["parameters"])["reasoning_effort"])
}

// TestAllowedFunctionSubsetPreservesRequiredIntent checks flat and nested tool
// selectors. Native requests should contain only the named functions, and an
// explicit required subset must be rejected when the group disables tool calls.
func TestAllowedFunctionSubsetPreservesRequiredIntent(t *testing.T) {
	for _, tc := range []struct {
		proto protocol.ProtocolID
		body  string
	}{
		{protocol.ProtocolOpenAIChatCompletions, `{
			"model":"x", "messages":[{"role":"user","content":"test"}],
			"tools":[{"type":"function","function":{"name":"first"}},{"type":"function","function":{"name":"second"}}],
			"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"required","tools":[{"type":"function","function":{"name":"second"}}]}}
		}`},
		{protocol.ProtocolOpenAIResponses, `{
			"model":"x", "input":"test",
			"tools":[{"type":"function","name":"first"},{"type":"function","name":"second"}],
			"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"second"}]}
		}`},
	} {
		request := parseQoderRequestViaIRForTest(t, tc.proto, []byte(tc.body))
		out, _, err := BuildQoderPayloadWithOptions(request, "", nil, true, true)
		require.NoError(t, err)
		tools := QoderAnySlice(out["tools"])
		require.Len(t, tools, 1)
		require.Equal(t, "second", QoderDeclaredToolName(tools[0]))
		require.Equal(t, "required", qoderFixtureValue[map[string]any](t, out["parameters"])["tool_choice"])
		request.Capabilities = &modelrules.Resolution{Attributes: modelcatalog.Attributes{ToolCall: new(false)}}
		_, _, err = BuildQoderPayloadWithOptions(request, "", nil, true, true)
		require.True(t, IsRequestError(err))
	}
}
