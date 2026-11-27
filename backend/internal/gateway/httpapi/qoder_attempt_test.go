package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	gatewaytestkit "github.com/TokenFlux/TokenRouter/internal/gateway/testkit"
	protocolcore "github.com/TokenFlux/TokenRouter/internal/protocol"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	upstreamcore "github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/upstream/qoder"
)

// Qoder forwards client protocols through wrapped upstream SSE, with
// conversation identity separate from generated response IDs and provider
// model routes. These fixtures protect explicit session precedence, full
// request replay, client model restoration and usage reported by the upstream
// stream. Tool calls can arrive as fragments, placeholders or parallel calls
// without reliable indexes.
//
// The blocking client holds the first accepted stream open while another
// request inspects conversation reservation and recovery. Body snapshots use a
// mutex, and pipe completion is controlled separately from later malformed
// responses. Wire fixtures exercise tool identity across Chat, Messages and
// Responses, including text-encoded calls and cache accounting;
// client-disconnect writers verify that tail usage remains available even when
// output cannot continue.

// Qoder wire fixtures carry cached-token accounting and XML or JSON tool text
// through the production parser and converters.
const (
	qoderCachedUsageSSEForTest    = "data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":66637,\\\"completion_tokens\\\":6,\\\"total_tokens\\\":66643,\\\"prompt_tokens_details\\\":{\\\"cached_tokens\\\":66612,\\\"cacheable_tokens\\\":19},\\\"completion_tokens_details\\\":{\\\"reasoning_tokens\\\":0}}}\"}\n\n"
	qoderXMLToolCallFixture       = `<tool_call>Read<arg_value><arg_key>file_path</arg_key><arg_value>/workspace/campus-navigation/README.md</arg_value></tool_call>`
	qoderJSONShellToolCallFixture = `<tool_call>{"name":"shell","arguments":{"command":"pwd","description":"Print working directory"}}</tool_call>`
)

// qoderContextClientFixture exposes the execution context through a supplied
// request callback.
type qoderContextClientFixture struct {
	// request must be configured before forwarding; the fixture calls it with the
	// execution context and returns its response or error unchanged.
	request func(context.Context) (*http.Response, error) // execution-context fixture callback
}

// qoderForwardTestHeader carries one request-header override for forwarding
// fixtures.
type qoderForwardTestHeader struct {
	key   string // request header field name
	value string // request header field value
}

// blockingQoderClientStub holds initial stream completion while recording
// concurrent request bodies.
type blockingQoderClientStub struct {
	t *testing.T // owning fixture test handle
	// mu protects Bodies, Headers, firstWriter, firstDone and nextError across
	// concurrent forwarding and test control. Bodies are copied on capture and
	// BodyAt, while Headers retains the supplied map without cloning it.
	mu sync.Mutex // request observation state mutex
	// cond shares mu and is broadcast after request capture. waitForCalls polls
	// the protected count instead of waiting on this condition variable.
	cond   *sync.Cond // request-arrival notification condition
	Bodies [][]byte   // copied upstream request bodies
	// Headers retains the supplied map. Callers leave its entries immutable
	// while forwarding or test control reads it; mu serializes the field assignment.
	Headers     map[string]string // retained latest upstream headers
	firstWriter *io.PipeWriter    // open first response pipe
	firstDone   bool              // first completion ownership flag
	nextError   bool              // one-shot malformed later response
}

// qoderIRHealthRecorder counts SetRateLimited and SetOverloaded calls during Qoder IR tests.
type qoderIRHealthRecorder struct {
	// mu serializes concurrent health-observer callbacks and test reads across
	// rateLimited and overloaded so asynchronous cooldown writes stay race-free.
	mu          sync.Mutex // lock guarding rateLimited and overloaded
	rateLimited int        // count of SetRateLimited invocations
	overloaded  int        // count of SetOverloaded invocations
}

// assertQoderContextCapabilityForTest checks model input capacity and optional
// runtime override fields. Callers provide decoded object-shaped payloads;
// wantRuntime requires both context length and IDE override values, while
// false requires their absence.
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

// requireQoderPayloadMapForTest returns the supplied value as a decoded
// fixture object. A different dynamic type fails the test with its payload
// path, so callers can safely inspect the returned map afterward.
func requireQoderPayloadMapForTest(t *testing.T, value any, path string) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	require.True(t, ok, "%s 应为 JSON 对象", path)
	return result
}

// StreamRequestContext delegates the supplied execution context to the
// configured fixture callback. This fixture ignores session, endpoint, body
// and header arguments; callers configure request before forwarding and
// interpret its response or error.
func (c qoderContextClientFixture) StreamRequestContext(ctx context.Context, _ *qoder.SessionContext, _ string, _ []byte, _ map[string]string) (*http.Response, error) {
	return c.request(ctx)
}

// TestQoderForwardContextDetachesStreamingFromClientCancellation cancels the
// client inside upstream execution for buffered and streamed input. Both
// receive a bounded execution deadline, while only streaming preserves a live
// context and final usage.
func TestQoderForwardContextDetachesStreamingFromClientCancellation(t *testing.T) {
	for _, stream := range []bool{true, false} {
		t.Run(strconv.FormatBool(stream), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			value, fixture, _ := gatewaytestkit.NewDefaultQoderFixture()
			calls := 0
			fixture.Client = qoderContextClientFixture{request: func(execution context.Context) (*http.Response, error) {
				calls++
				deadline, ok := execution.Deadline()
				require.True(t, ok)
				remaining := time.Until(deadline)
				require.LessOrEqual(t, remaining, qoder.QoderStreamTimeout)
				// Allow fixture setup to consume a minute of the fresh execution budget.
				require.Greater(t, remaining, qoder.QoderStreamTimeout-time.Minute)
				cancel()
				if !stream {
					require.ErrorIs(t, execution.Err(), context.Canceled)
					return nil, execution.Err()
				}
				require.NoError(t, execution.Err())
				body := qoderWrappedSSELineForTest(t, map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"content": "served"}}}}) + qoderWrappedSSELineForTest(t, map[string]any{"usage": map[string]any{"prompt_tokens": 12, "completion_tokens": 3}}) + "data: {\"body\":\"[DONE]\"}\n\n"
				return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
			}}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
			body := []byte(`{"model":"auto","stream":` + strconv.FormatBool(stream) + `,"messages":[{"role":"user","content":"hi"}]}`)
			result, err := forwardQoderAttempt(ctx, c, fixture.Runtime, value, body, protocolcore.ProtocolOpenAIChatCompletions, false)
			require.Equal(t, 1, calls)
			if stream {
				require.NoError(t, err)
				require.NotNil(t, result)
				require.Equal(t, 3, result.Usage.OutputTokens)
			} else {
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result)
			}
		})
	}
}

// TestQoderForwardPartialResult emits real Chat content and usage before an
// upstream failure. Forwarding must return the error together with measured
// input/output usage rather than discard the served partial result.
func TestQoderForwardPartialResult(t *testing.T) {
	a, s, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = qoderWrappedSSELineForTest(t, map[string]any{"choices": []any{map[string]any{"delta": map[string]any{"content": "served"}}}}) + qoderWrappedSSELineForTest(t, map[string]any{"usage": map[string]any{"prompt_tokens": 12, "completion_tokens": 3}}) + qoderWrappedErrorSSELineForTest(t, 502, map[string]any{"code": "500", "message": "fixture failure"})
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	result, err := forwardQoderAttempt(c.Request.Context(), c, s.Runtime, a, []byte(`{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}`), protocolcore.ProtocolOpenAIChatCompletions, false)
	if err == nil || !strings.Contains(rec.Body.String(), "served") {
		t.Fatalf("fixture did not reach post-output failure: %v", err)
	}
	if result == nil {
		t.Fatal("ForwardChatCompletions discards observed partial usage before handler completion")
	}
	if result.Usage.InputTokens != 12 || result.Usage.OutputTokens != 3 {
		t.Fatalf("incorrect observed usage: %+v", result.Usage)
	}
}

// TestQoderCanceledBeforeForward cancels the request before native Qoder
// execution begins. No upstream request may be recorded, and the returned
// error must preserve context.Canceled for outer cancellation handling.
func TestQoderCanceledBeforeForward(t *testing.T) {
	a, s, client := gatewaytestkit.NewDefaultQoderFixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	_, err := forwardQoderAttempt(ctx, c, s.Runtime, a, []byte(`{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}`), protocolcore.ProtocolOpenAIChatCompletions, false)
	if len(client.Requests) != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled request starts %d upstream inference(s); err=%v", len(client.Requests), err)
	}
}

// TestQoderGatewayAllowsExplicitPreviewCompatibilityMapping maps a preview
// client model explicitly to a Qoder route. Upstream headers and context
// capability must use that route while result and client output preserve the
// requested alias.
func TestQoderGatewayAllowsExplicitPreviewCompatibilityMapping(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	provider := &providercore.Record{
		ID:       88,
		Name:     "qoder",
		Platform: capability.PlatformQoder,
		Type:     capability.ProviderTypeCosy,
		Credentials: map[string]any{
			"model_mapping": map[string]any{
				"qwen3.8-max-preview": "qmodel_38max",
			},
			"model_whitelist": []any{},
		},
	}
	gatewaytestkit.SetQoderCatalog(provider)
	client := &gatewaytestkit.QoderClient{
		Body: "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
			"data: {\"body\":\"[DONE]\"}\n\n",
	}
	svc := gatewaytestkit.NewQoderFixture(provideradapter.NewQoderTokenProvider(qoder.SessionBuilder{}),
		client, nil)

	svc.Tokens.Core.Sessions = map[int64]providercore.QoderSessionCacheEntry[*qoder.SessionContext]{
		provider.ID: {
			CredentialsHash: providercore.QoderCredentialsHash(provider.Credentials),
			Session:         &qoder.SessionContext{Identity: &qoder.AuthIdentity{SecurityOauthToken: "token"}},
		},
	}
	body := []byte(`{"model":"qwen3.8-max-preview","messages":[{"role":"user","content":"hi"}],"stream":true}`)

	result, err := forwardQoderAttempt(context.Background(), c, svc.Runtime, provider, body, protocolcore.ProtocolOpenAIChatCompletions, false)

	require.NoError(t, err)
	require.Equal(t, "qwen3.8-max-preview", result.Model)
	require.Equal(t, "qmodel_38max", result.UpstreamModel)
	require.Equal(t, "qmodel_38max", client.Headers["x-model-key"])
	require.Contains(t, rec.Body.String(), `"model":"qwen3.8-max-preview"`)
	assertQoderContextCapabilityForTest(t, qoderLastUpstreamPayloadForTest(t, client), 1000000, true)
}

// TestQoderGatewayForwardUsesOriginalModelAfterGroupMapping forwards mapped
// bodies through Chat, Responses and Messages with an explicit response-model
// override. Each result and client envelope must retain the original model
// while upstream identity follows the body.
func TestQoderGatewayForwardUsesOriginalModelAfterGroupMapping(t *testing.T) {
	tests := []struct {
		name string                                                                                                                                       // protocol or mapping case label
		path string                                                                                                                                       // fixture HTTP endpoint path
		body []byte                                                                                                                                       // encoded client request fixture
		call func(context.Context, *gatewaytestkit.QoderFixture, *gin.Context, *providercore.Record, []byte, string) (*forwardcore.MessagesResult, error) // selected protocol forwarding callback
	}{
		{
			name: "chat completions",
			path: "/v1/chat/completions",
			body: []byte(`{"model":"qmodel","messages":[{"role":"user","content":"hi"}],"stream":false}`),
			call: func(ctx context.Context, svc *gatewaytestkit.QoderFixture, c *gin.Context, provider *providercore.Record, body []byte, responseModel string) (*forwardcore.MessagesResult, error) {
				return forwardQoderAttempt(ctx, c, svc.Runtime, provider, body, protocolcore.ProtocolOpenAIChatCompletions, false, responseModel)
			},
		},
		{
			name: "responses",
			path: "/v1/responses",
			body: []byte(`{"model":"qmodel","input":"hi","stream":false}`),
			call: func(ctx context.Context, svc *gatewaytestkit.QoderFixture, c *gin.Context, provider *providercore.Record, body []byte, responseModel string) (*forwardcore.MessagesResult, error) {
				return forwardQoderAttempt(ctx, c, svc.Runtime, provider, body, protocolcore.ProtocolOpenAIResponses, false, responseModel)
			},
		},
		{
			name: "messages",
			path: "/v1/messages",
			body: []byte(`{"model":"qmodel","max_tokens":16,"messages":[{"role":"user","content":"hi"}],"stream":false}`),
			call: func(ctx context.Context, svc *gatewaytestkit.QoderFixture, c *gin.Context, provider *providercore.Record, body []byte, responseModel string) (*forwardcore.MessagesResult, error) {
				return forwardQoderAttempt(ctx, c, svc.Runtime, provider, body, protocolcore.ProtocolAnthropicMessages, false, responseModel)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()

			rec := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, tt.path, bytes.NewReader(tt.body))

			result, err := tt.call(context.Background(), svc, c, provider, tt.body, "qwen3.7-plus")

			require.NoError(t, err)
			require.Equal(t, "qwen3.7-plus", result.Model)
			require.Equal(t, "qmodel", result.UpstreamModel)
			require.Equal(t, "qmodel", client.Headers["x-model-key"])
			require.Equal(t, "qwen3.7-plus", gjson.Get(rec.Body.String(), "model").String())
		})
	}
}

// TestQoderGatewayChatCompletionsReusesSessionAndSendsFullReplay extends an
// explicitly identified Chat conversation. Upstream session identity must
// remain stable while the second request replays system, user, assistant and
// new user turns.
func TestQoderGatewayChatCompletionsReusesSessionAndSendsFullReplay(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()

	first := qoderForwardChatCompletionsForTest(t, svc, provider, "stable-chat-session", []byte(`{
		"model":"auto",
		"messages":[{"role":"system","content":"be terse"},{"role":"user","content":"hello"}],
		"stream":false
	}`))
	second := qoderForwardChatCompletionsForTest(t, svc, provider, "stable-chat-session", []byte(`{
		"model":"auto",
		"messages":[
			{"role":"system","content":"be terse"},
			{"role":"user","content":"hello"},
			{"role":"assistant","content":"hi"},
			{"role":"user","content":"next"}
		],
		"stream":false
	}`))

	require.Len(t, client.Bodies, 2)
	require.Equal(t, first["session_id"], second["session_id"])
	firstMessages := qoderFixtureValue[[]any](t, first["messages"])
	require.Len(t, firstMessages, 2)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, firstMessages[0])["role"])

	secondMessages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, secondMessages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, secondMessages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, secondMessages[1])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, secondMessages[2])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, secondMessages[3])["role"])
	require.Equal(t, "next", qoderFixtureValue[map[string]any](t, second["chat_context"])["text"])
}

// TestQoderGatewayChatCompletionsWithoutSessionDoesNotReuseByFirstText repeats
// initial Chat text without explicit session identity. Upstream sessions must
// differ while the second request retains its full three-message history for
// replay.
func TestQoderGatewayChatCompletionsWithoutSessionDoesNotReuseByFirstText(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()

	first := qoderForwardChatCompletionsForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"messages":[{"role":"user","content":"hello"}],
		"stream":false
	}`))
	second := qoderForwardChatCompletionsForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"messages":[
			{"role":"user","content":"hello"},
			{"role":"assistant","content":"hi"},
			{"role":"user","content":"how many turns?"}
		],
		"stream":false
	}`))

	require.NotEqual(t, first["session_id"], second["session_id"])
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 3)
	require.Equal(t, "hello", qoderPayloadMessageTextForTest(qoderFixtureValue[map[string]any](t, messages[0])))
}

// TestQoderGatewayChatCompletionsMapsUpstreamToolNameToDeclaredOpenAITool
// returns upstream Bash for a declared lowercase Chat tool. Buffered output
// must restore bash, use tool_calls finish reason and forward the matching
// declared tool metadata.
func TestQoderGatewayChatCompletionsMapsUpstreamToolNameToDeclaredOpenAITool(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = qoderWrappedSSELineForTest(t, map[string]any{"choices": []any{
		map[string]any{"delta": map[string]any{"tool_calls": []any{
			map[string]any{"index": 0, "id": "call_1", "type": "function", "function": map[string]any{"name": "Bash", "arguments": `{"command":"pwd"}`}},
		}}},
	}}) +
		"data: {\"body\":\"[DONE]\"}\n\n"
	body := []byte(`{
		"model":"deepseek-v4-pro",
		"messages":[{"role":"user","content":"run pwd"}],
		"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object","properties":{"command":{"type":"string"}}}}}],
		"stream":false
	}`)

	result, response := qoderForwardChatCompletionsResultAndBodyForTest(t, svc, provider, "", body)

	require.False(t, result.Stream)
	require.Equal(t, "tool_calls", gjson.Get(response, "choices.0.finish_reason").String())
	require.Equal(t, "bash", gjson.Get(response, "choices.0.message.tool_calls.0.function.name").String())
	require.NotContains(t, response, `"name":"Bash"`)
	upstream := qoderLastUpstreamPayloadForTest(t, client)
	tools := qoderFixtureValue[[]any](t, upstream["tools"])
	require.Equal(t, "bash", qoderFixtureValue[map[string]any](t, qoderFixtureValue[map[string]any](t, tools[0])["function"])["name"])
}

// TestQoderGatewayChatCompletionsConvertsLegacyFunctions supplies legacy Chat
// functions instead of modern tools. The upstream tool list must preserve the
// function name, description and object schema during compatibility
// conversion.
func TestQoderGatewayChatCompletionsConvertsLegacyFunctions(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	body := []byte(`{
		"model":"deepseek-v4-pro",
		"messages":[{"role":"user","content":"weather"}],
		"functions":[{
			"name":"get_weather",
			"description":"Get weather",
			"parameters":{"type":"object","properties":{"city":{"type":"string"}}}
		}],
		"function_call":{"name":"get_weather"},
		"stream":false
	}`)

	result, _ := qoderForwardChatCompletionsResultAndBodyForTest(t, svc, provider, "", body)

	require.False(t, result.Stream)
	upstream := qoderLastUpstreamPayloadForTest(t, client)
	tools := qoderFixtureValue[[]any](t, upstream["tools"])
	require.Len(t, tools, 1)
	// Legacy Chat clients supply tools through functions and function_call.
	function := qoderFixtureValue[map[string]any](t, qoderFixtureValue[map[string]any](t, tools[0])["function"])
	require.Equal(t, "get_weather", function["name"])
	require.Equal(t, "Get weather", function["description"])
	require.Equal(t, "object", qoderFixtureValue[map[string]any](t, function["parameters"])["type"])
}

// TestQoderGatewayChatCompletionsLegacyFunctionCallNoneClearsTools supplies
// legacy function definitions with function_call set to none. Buffered
// forwarding must leave the upstream tool list empty rather than expose
// disabled client tools.
func TestQoderGatewayChatCompletionsLegacyFunctionCallNoneClearsTools(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	body := []byte(`{
		"model":"deepseek-v4-pro",
		"messages":[{"role":"user","content":"plain answer"}],
		"functions":[{"name":"get_weather","parameters":{"type":"object"}}],
		"function_call":"none",
		"stream":false
	}`)

	result, _ := qoderForwardChatCompletionsResultAndBodyForTest(t, svc, provider, "", body)

	require.False(t, result.Stream)
	upstream := qoderLastUpstreamPayloadForTest(t, client)
	params := qoderFixtureValue[map[string]any](t, upstream["parameters"])
	require.Equal(t, "none", params["tool_choice"])
	tools := qoderFixtureValue[[]any](t, upstream["tools"])
	require.Len(t, tools, 1)
}

// TestQoderGatewayMessagesMapsUpstreamToolNameToDeclaredAnthropicTool returns
// upstream Bash for a lowercase Messages declaration. Buffered Anthropic
// content must restore the declared name with tool_use content and stop
// reason, retaining matching upstream tool metadata.
func TestQoderGatewayMessagesMapsUpstreamToolNameToDeclaredAnthropicTool(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = qoderWrappedSSELineForTest(t, map[string]any{"choices": []any{
		map[string]any{"delta": map[string]any{"tool_calls": []any{
			map[string]any{"index": 0, "id": "call_1", "type": "function", "function": map[string]any{"name": "Bash", "arguments": `{"command":"pwd"}`}},
		}}},
	}}) +
		"data: {\"body\":\"[DONE]\"}\n\n"
	body := []byte(`{
		"model":"deepseek-v4-pro",
		"messages":[{"role":"user","content":"run pwd"}],
		"tools":[{"name":"bash","input_schema":{"type":"object","properties":{"command":{"type":"string"}}}}],
		"stream":false
	}`)

	result, response := qoderForwardMessagesResultAndBodyForTest(t, svc, provider, body)

	require.False(t, result.Stream)
	require.Equal(t, "tool_use", gjson.Get(response, "stop_reason").String())
	require.Equal(t, "tool_use", gjson.Get(response, "content.0.type").String())
	require.Equal(t, "bash", gjson.Get(response, "content.0.name").String())
	require.NotContains(t, response, `"name":"Bash"`)
	upstream := qoderLastUpstreamPayloadForTest(t, client)
	tools := qoderFixtureValue[[]any](t, upstream["tools"])
	require.Equal(t, "bash", qoderFixtureValue[map[string]any](t, qoderFixtureValue[map[string]any](t, tools[0])["function"])["name"])
}

// TestQoderGatewayResponsesMapsUpstreamToolNameToDeclaredFunctionCall returns
// upstream Bash through buffered Responses conversion with a lowercase
// declaration. The function-call item must retain valid arguments and declared
// bash identity without the upstream spelling.
func TestQoderGatewayResponsesMapsUpstreamToolNameToDeclaredFunctionCall(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = qoderWrappedSSELineForTest(t, map[string]any{"choices": []any{
		map[string]any{"delta": map[string]any{"tool_calls": []any{
			map[string]any{"index": 0, "id": "call_1", "type": "function", "function": map[string]any{"name": "Bash", "arguments": `{"command":"pwd"}`}},
		}}},
	}}) +
		"data: {\"body\":\"[DONE]\"}\n\n"
	body := []byte(`{
		"model":"deepseek-v4-pro",
		"input":[{"role":"user","content":"run pwd"}],
		"tools":[{"type":"function","name":"bash","parameters":{"type":"object","properties":{"command":{"type":"string"}}}}],
		"stream":false
	}`)

	result, response := qoderForwardResponsesResultAndBodyForTest(t, svc, provider, body)

	require.False(t, result.Stream)
	require.Equal(t, "response", gjson.Get(response, "object").String())
	require.Equal(t, "function_call", gjson.Get(response, "output.0.type").String())
	require.Equal(t, "bash", gjson.Get(response, "output.0.name").String())
	require.JSONEq(t, `{"command":"pwd"}`, gjson.Get(response, "output.0.arguments").String())
	require.NotContains(t, response, `"name":"Bash"`)
	upstream := qoderLastUpstreamPayloadForTest(t, client)
	tools := qoderFixtureValue[[]any](t, upstream["tools"])
	require.Equal(t, "bash", qoderFixtureValue[map[string]any](t, qoderFixtureValue[map[string]any](t, tools[0])["function"])["name"])
}

// TestQoderGatewayResponsesStreamsDeclaredFunctionCallEvents streams an
// upstream tool under a declared lowercase name. Responses item and
// argument-completion events must expose bash and valid JSON, beginning with
// response.created and hiding upstream Bash.
func TestQoderGatewayResponsesStreamsDeclaredFunctionCallEvents(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = qoderWrappedSSELineForTest(t, map[string]any{"choices": []any{
		map[string]any{"delta": map[string]any{"tool_calls": []any{
			map[string]any{"index": 0, "id": "call_1", "type": "function", "function": map[string]any{"name": "Bash", "arguments": `{"command":"pwd"}`}},
		}}},
	}}) +
		"data: {\"body\":\"[DONE]\"}\n\n"
	body := []byte(`{
		"model":"deepseek-v4-pro",
		"input":[{"role":"user","content":"run pwd"}],
		"tools":[{"type":"function","name":"bash","parameters":{"type":"object","properties":{"command":{"type":"string"}}}}],
		"stream":true
	}`)

	result, response := qoderForwardResponsesResultAndBodyForTest(t, svc, provider, body)

	require.True(t, result.Stream)
	events := qoderResponsesStreamEventsForTest(t, response)
	require.Equal(t, "response.created", events[0].Get("type").String())
	var added gjson.Result
	var argsDone gjson.Result
	for _, event := range events {
		switch event.Get("type").String() {
		case "response.output_item.added":
			if event.Get("item.type").String() == "function_call" {
				added = event
			}
		case "response.function_call_arguments.done":
			argsDone = event
		}
	}
	require.Equal(t, "bash", added.Get("item.name").String())
	require.JSONEq(t, `{"command":"pwd"}`, argsDone.Get("arguments").String())
	require.NotContains(t, response, `"name":"Bash"`)
}

// TestQoderGatewayResponsesToolContinuationUsesToolResultsAsPrompt continues
// after two Responses function calls return results. The new upstream prompt
// must contain both identified tool results instead of replaying the original
// user command as the prompt.
func TestQoderGatewayResponsesToolContinuationUsesToolResultsAsPrompt(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	tools := `[{"type":"function","name":"bash","parameters":{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}}]`
	firstBody := []byte(`{
		"model":"deepseek-v4-pro",
		"input":[{"role":"user","content":"run two commands"}],
		"tools":` + tools + `,
		"stream":true
	}`)
	secondBody := []byte(`{
		"model":"deepseek-v4-pro",
		"input":[
			{"role":"user","content":"run two commands"},
			{"type":"function_call","call_id":"call_a","name":"bash","arguments":"{\"command\":\"printf A\"}"},
			{"type":"function_call","call_id":"call_b","name":"bash","arguments":"{\"command\":\"printf B\"}"},
			{"type":"function_call_output","call_id":"call_a","output":"A\n"},
			{"type":"function_call_output","call_id":"call_b","output":"B\n"}
		],
		"tools":` + tools + `,
		"stream":true
	}`)

	qoderForwardResponsesResultAndBodyForTest(t, svc, provider, firstBody, qoderHeader("session_id", "responses-tool-continuation"))
	qoderForwardResponsesResultAndBodyForTest(t, svc, provider, secondBody, qoderHeader("session_id", "responses-tool-continuation"))

	payload := qoderLastUpstreamPayloadForTest(t, client)
	prompt := qoderPayloadPromptForTest(t, payload)
	require.NotEqual(t, "run two commands", prompt)
	require.Contains(t, prompt, `<tool_result id="call_a">`)
	require.Contains(t, prompt, "A\n")
	require.Contains(t, prompt, `<tool_result id="call_b">`)
	require.Contains(t, prompt, "B\n")
}

// TestQoderGatewayResponsesToolContinuationGroupsFunctionCallsIntoOneAssistantTurn
// continues a Responses turn containing parallel calls. Upstream history must
// group both calls into one assistant message and include their identified
// result text in the new prompt.
func TestQoderGatewayResponsesToolContinuationGroupsFunctionCallsIntoOneAssistantTurn(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	tools := `[{"type":"function","name":"bash","parameters":{"type":"object","properties":{"command":{"type":"string"}},"required":["command"]}},{"type":"function","name":"glob","parameters":{"type":"object","properties":{"pattern":{"type":"string"}},"required":["pattern"]}}]`
	body := []byte(`{
		"model":"deepseek-v4-pro",
		"input":[
			{"role":"user","content":"list docs and print marker"},
			{"type":"function_call","call_id":"call_glob","name":"glob","arguments":"{\"pattern\":\"docs/*.md\"}"},
			{"type":"function_call","call_id":"call_bash","name":"bash","arguments":"{\"command\":\"echo CHAIN_OPENAI_OK\"}"},
			{"type":"function_call_output","call_id":"call_glob","output":"docs/a.md\ndocs/b.md"},
			{"type":"function_call_output","call_id":"call_bash","output":"CHAIN_OPENAI_OK\n"}
		],
		"tools":` + tools + `,
		"stream":true
	}`)

	qoderForwardResponsesResultAndBodyForTest(t, svc, provider, body, qoderHeader("session_id", "responses-grouped-tool-continuation"))

	payload := qoderLastUpstreamPayloadForTest(t, client)
	messages := qoderFixtureValue[[]any](t, payload["messages"])
	require.Len(t, messages, 4)
	assistant := qoderFixtureValue[map[string]any](t, messages[1])
	require.Equal(t, "assistant", assistant["role"])
	require.Len(t, qoderFixtureValue[[]any](t, assistant["tool_calls"]), 2)

	prompt := qoderPayloadPromptForTest(t, payload)
	require.NotEqual(t, "list docs and print marker", prompt)
	require.Contains(t, prompt, `<tool_result id="call_glob">`)
	require.Contains(t, prompt, "docs/a.md")
	require.Contains(t, prompt, `<tool_result id="call_bash">`)
	require.Contains(t, prompt, "CHAIN_OPENAI_OK")
}

// TestQoderGatewayClaudeRequestsWithoutSessionDoNotReuseByFirstText extends
// Messages input without explicit or detected Claude session state. Upstream
// sessions must differ, and the second request must retain tools and all four
// history turns.
func TestQoderGatewayClaudeRequestsWithoutSessionDoNotReuseByFirstText(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	largeTools := qoderLargeToolsJSONForTest()
	first := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"deepseek-v4-pro",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"你好"}],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	second := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"deepseek-v4-pro",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"你好"},
			{"role":"assistant","content":"你好"},
			{"role":"user","content":"你一共和我对话了几句话？"}
		],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))

	require.NotEqual(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	require.Len(t, qoderFixtureValue[[]any](t, second["messages"]), 4)
}

// TestQoderGatewayClaudeCodeContextWithoutSessionUsesStablePrefixKey installs
// Claude client context without explicit session identity. Stable prefix
// matching must reuse the upstream session while forwarding tools, complete
// history and the current continuation prompt.
func TestQoderGatewayClaudeCodeContextWithoutSessionUsesStablePrefixKey(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	largeTools := qoderLargeToolsJSONForTest()
	system1 := "x-anthropic-billing-header: cc_version=2.1.177.19c; cc_entrypoint=sdk-cli; cch=29156;\n" +
		"You are Claude Code, Anthropic's official CLI for Claude.\n" +
		"Stable Claude Code system body."
	system2 := "x-anthropic-billing-header: cc_version=2.1.177.19c; cc_entrypoint=sdk-cli; cch=40d8d;\n" +
		"You are Claude Code, Anthropic's official CLI for Claude.\n" +
		"Stable Claude Code system body."
	first := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"claude-opus-4-6",
		"system":`+strconv.Quote(system1)+`,
		"messages":[{"role":"user","content":"inspect"}],
		"tools":`+largeTools+`,
		"stream":false
	}`),
		qoderHeader("User-Agent", "claude-cli/2.1.177 (external, sdk-cli)"),
		qoderHeader("X-Test-Claude-Code-Context", "true"),
	)
	second := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"claude-opus-4-6",
		"system":`+strconv.Quote(system2)+`,
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"tools":`+largeTools+`,
		"stream":false
	}`),
		qoderHeader("User-Agent", "claude-cli/2.1.177 (external, sdk-cli)"),
		qoderHeader("X-Test-Claude-Code-Context", "true"),
	)

	require.Equal(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[3])["role"])
	require.Equal(t, "continue", qoderFixtureValue[map[string]any](t, second["chat_context"])["text"])
}

// TestQoderGatewayDoesNotCommitConversationOnUpstreamFailure fails one upstream
// attempt, then clears the fixture error and forwards the same user input. The
// subsequent request must contain one history message; no stable session seed
// is supplied, so this assertion observes replay rather than stable-key rollback.
func TestQoderGatewayDoesNotCommitConversationOnUpstreamFailure(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Err = errors.New("upstream failed")
	body := []byte(`{
		"model":"auto",
		"messages":[{"role":"user","content":"hello"}],
		"stream":false
	}`)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	_, err := forwardQoderAttempt(context.Background(), c, svc.Runtime, provider, body, protocolcore.ProtocolOpenAIChatCompletions, false)
	require.Error(t, err)

	client.Err = nil
	first := qoderForwardChatCompletionsForTest(t, svc, provider, "", body)
	require.Len(t, qoderFixtureValue[[]any](t, first["messages"]), 1)
}

// TestQoderGatewayReservesConversationAfterUpstreamAcceptsBeforeStreamCompletes
// leaves the first upstream stream unfinished while sending a continuation.
// Both payloads must share one session, and continuation must retain tools and
// full history before the first stream finishes.
func TestQoderGatewayReservesConversationAfterUpstreamAcceptsBeforeStreamCompletes(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	client := newBlockingQoderClientStub(t)
	svc.Client = client
	body1 := []byte(`{
		"model":"deepseek-v4-pro",
		"prompt_cache_key":"reserve-before-stream",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"inspect"}],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":true
	}`)
	body2 := []byte(`{
		"model":"deepseek-v4-pro",
		"prompt_cache_key":"reserve-before-stream",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":false
	}`)

	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body1))
	firstCtx.Request.Header.Set("User-Agent", "claude-cli/2.1.177 (external, cli)")
	var wg sync.WaitGroup
	wg.Add(1)
	var firstErr error
	// Leave the first upstream stream unfinished while continuation runs
	// independently; join forwarding after pipe completion before reading its error.
	go func() {
		defer wg.Done()
		_, firstErr = forwardQoderAttempt(context.Background(), firstCtx, svc.Runtime, provider, body1, protocolcore.ProtocolAnthropicMessages, false)
	}()
	client.waitForCalls(1)

	secondPayload := qoderForwardMessagesForTest(t, svc, provider, "", body2, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	client.finishFirst()
	wg.Wait()
	require.NoError(t, firstErr)

	firstPayload := qoderPayloadAtForTest(t, client, 0)
	require.Equal(t, firstPayload["session_id"], secondPayload["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, secondPayload["tools"]))
	messages := qoderFixtureValue[[]any](t, secondPayload["messages"])
	require.Len(t, messages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[3])["role"])
}

// TestQoderGatewayDoesNotCommitFailedPostToolStreamAsComplete fails a
// post-tool continuation while another accepted stream remains open. Retrying
// must reuse the session with tools and the full tool-result history rather
// than treat the failed tail as completed.
func TestQoderGatewayDoesNotCommitFailedPostToolStreamAsComplete(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	client := newBlockingQoderClientStub(t)
	svc.Client = client
	body1 := []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"failed-post-tool-stream",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"run pwd"}],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":true
	}`)
	body2 := []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"failed-post-tool-stream",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"run pwd"},
			{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Bash","input":{"command":"pwd"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"/repo"}]}
		],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":true
	}`)

	firstRec := httptest.NewRecorder()
	firstCtx, _ := gin.CreateTestContext(firstRec)
	firstCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body1))
	firstCtx.Request.Header.Set("User-Agent", "claude-cli/2.1.177 (external, cli)")
	var wg sync.WaitGroup
	wg.Add(1)
	var firstErr error
	// Leave the first upstream stream unfinished while continuation runs
	// independently; join forwarding after pipe completion before reading its error.
	go func() {
		defer wg.Done()
		_, firstErr = forwardQoderAttempt(context.Background(), firstCtx, svc.Runtime, provider, body1, protocolcore.ProtocolAnthropicMessages, false)
	}()
	client.waitForCalls(1)

	// Inject failure under the same lock consumed by later request capture,
	// leaving the already accepted first pipe unaffected by this one-shot malformed response.
	client.mu.Lock()
	client.nextError = true
	client.mu.Unlock()
	failedRec := httptest.NewRecorder()
	failedCtx, _ := gin.CreateTestContext(failedRec)
	failedCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body2))
	failedCtx.Request.Header.Set("User-Agent", "claude-cli/2.1.177 (external, cli)")
	_, failedErr := forwardQoderAttempt(context.Background(), failedCtx, svc.Runtime, provider, body2, protocolcore.ProtocolAnthropicMessages, false)
	require.Error(t, failedErr)

	retryPayload := qoderForwardMessagesForTest(t, svc, provider, "", body2, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	client.finishFirst()
	wg.Wait()
	require.NoError(t, firstErr)

	require.Equal(t, qoderPayloadAtForTest(t, client, 0)["session_id"], retryPayload["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, retryPayload["tools"]))
	messages := qoderFixtureValue[[]any](t, retryPayload["messages"])
	require.Len(t, messages, 4)
	system := qoderFixtureValue[map[string]any](t, messages[0])
	require.Equal(t, "system", system["role"])
	user := qoderFixtureValue[map[string]any](t, messages[1])
	require.Equal(t, "user", user["role"])
	require.Equal(t, "run pwd", qoderPayloadMessageTextForTest(user))
	assistant := qoderFixtureValue[map[string]any](t, messages[2])
	require.Equal(t, "assistant", assistant["role"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, assistant["tool_calls"]))
	toolResult := qoderFixtureValue[map[string]any](t, messages[3])
	require.Equal(t, "tool", toolResult["role"])
	require.Equal(t, "call_1", toolResult["tool_call_id"])
	require.Equal(t, "call_1", toolResult["tool_call_call_id"])
	require.Equal(t, "Bash", toolResult["name"])
	require.Equal(t, "/repo", toolResult["content"])
}

// TestQoderGatewayRollsBackAcceptedConversationOnStreamParseFailure injects
// malformed input after upstream acceptance. A retry must retain the same
// session but replay tools and complete tool-result history instead of
// consuming a failed reservation as completion.
func TestQoderGatewayRollsBackAcceptedConversationOnStreamParseFailure(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	body1 := []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"rollback-accepted-stream",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"run pwd"}],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":true
	}`)
	body2 := []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"rollback-accepted-stream",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"run pwd"},
			{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"Bash","input":{"command":"pwd"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"/repo"}]}
		],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":true
	}`)

	firstPayload := qoderForwardMessagesForTest(t, svc, provider, "", body1, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))

	client.Body = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\""

	failedRec := httptest.NewRecorder()
	failedCtx, _ := gin.CreateTestContext(failedRec)
	failedCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body2))
	failedCtx.Request.Header.Set("User-Agent", "claude-cli/2.1.177 (external, cli)")
	_, failedErr := forwardQoderAttempt(context.Background(), failedCtx, svc.Runtime, provider, body2, protocolcore.ProtocolAnthropicMessages, false)
	require.Error(t, failedErr)

	client.Body = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
		"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":5,\\\"completion_tokens\\\":1,\\\"total_tokens\\\":6}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	retryPayload := qoderForwardMessagesForTest(t, svc, provider, "", body2, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	require.Equal(t, firstPayload["session_id"], retryPayload["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, retryPayload["tools"]))
	messages := qoderFixtureValue[[]any](t, retryPayload["messages"])
	require.Len(t, messages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.Equal(t, "tool", qoderFixtureValue[map[string]any](t, messages[3])["role"])
}

// TestQoderGatewayFallsBackToFullReplayWhenPrefixDoesNotMatch changes a
// conversation prefix under an existing identity. Forwarding must create a
// different upstream session and retain the changed message plus remaining
// history in the replay.
func TestQoderGatewayFallsBackToFullReplayWhenPrefixDoesNotMatch(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	first := qoderForwardChatCompletionsForTest(t, svc, provider, "stable-session", []byte(`{
		"model":"auto",
		"messages":[{"role":"user","content":"first"}],
		"stream":false
	}`))
	second := qoderForwardChatCompletionsForTest(t, svc, provider, "stable-session", []byte(`{
		"model":"auto",
		"messages":[
			{"role":"user","content":"changed"},
			{"role":"assistant","content":"answer"},
			{"role":"user","content":"next"}
		],
		"stream":false
	}`))

	require.NotEqual(t, first["session_id"], second["session_id"])
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 3)
	require.Equal(t, "changed", qoderFixtureValue[map[string]any](t, qoderFixtureValue[[]any](t, qoderFixtureValue[map[string]any](t, messages[0])["content"])[0])["text"])
}

// TestQoderGatewayFallsBackToFullReplayWhenSystemOrToolsChange changes system
// instructions and tool definitions for a continuation. Forwarding must use a
// different session and replay the new system, tool metadata and complete
// history.
func TestQoderGatewayFallsBackToFullReplayWhenSystemOrToolsChange(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	first := qoderForwardChatCompletionsForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"messages":[{"role":"system","content":"be terse"},{"role":"user","content":"hello"}],
		"tools":[{"type":"function","function":{"name":"read","parameters":{"type":"object"}}}],
		"stream":false
	}`))
	second := qoderForwardChatCompletionsForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"messages":[
			{"role":"system","content":"be detailed"},
			{"role":"user","content":"hello"},
			{"role":"assistant","content":"hi"},
			{"role":"user","content":"next"}
		],
		"tools":[{"type":"function","function":{"name":"write","parameters":{"type":"object"}}}],
		"stream":false
	}`))

	require.NotEqual(t, first["session_id"], second["session_id"])
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "be detailed", qoderFixtureValue[map[string]any](t, messages[0])["content"])
	tools := qoderFixtureValue[[]any](t, second["tools"])
	require.Equal(t, "write", qoderFixtureValue[map[string]any](t, qoderFixtureValue[map[string]any](t, tools[0])["function"])["name"])
}

// TestQoderGatewayUsesExplicitBodySessionID supplies body session identity
// across a continuation. Upstream session must remain stable while the second
// request retains all three messages rather than dropping previous
// conversation turns.
func TestQoderGatewayUsesExplicitBodySessionID(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	first := qoderForwardChatCompletionsForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"session_id":"body-session-1",
		"messages":[{"role":"user","content":"hello"}],
		"stream":false
	}`))
	second := qoderForwardChatCompletionsForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"session_id":"body-session-1",
		"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"hi"},{"role":"user","content":"next"}],
		"stream":false
	}`))

	require.Equal(t, first["session_id"], second["session_id"])
	require.Len(t, qoderFixtureValue[[]any](t, second["messages"]), 3)
}

// TestQoderGatewayAnthropicMetadataSessionWinsOverChangingHeader keeps
// Anthropic metadata session identity while changing the header. The
// continuation must reuse the upstream session, replay its history and send
// the latest user prompt.
func TestQoderGatewayAnthropicMetadataSessionWinsOverChangingHeader(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	metadata := `{"device_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","provider_uuid":"","session_id":"11111111-2222-3333-4444-555555555555"}`
	first := qoderForwardMessagesForTest(t, svc, provider, "volatile-header-1", []byte(`{
		"model":"deepseek-v4-pro",
		"metadata":{"user_id":`+strconv.Quote(metadata)+`},
		"messages":[{"role":"user","content":"inspect"}],
		"stream":false
	}`))
	second := qoderForwardMessagesForTest(t, svc, provider, "volatile-header-2", []byte(`{
		"model":"deepseek-v4-pro",
		"metadata":{"user_id":`+strconv.Quote(metadata)+`},
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"stream":false
	}`))

	require.Equal(t, first["session_id"], second["session_id"])
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 3)
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.Equal(t, "continue", qoderFixtureValue[map[string]any](t, second["chat_context"])["text"])
}

// TestQoderGatewayClaudeCodeUsesExplicitHeaderSessionBeforeStableSeed combines
// Claude system text and user-agent headers with explicit session identity.
// Matching headers must
// reuse one session with complete history, while another header must create a
// separate upstream session.
func TestQoderGatewayClaudeCodeUsesExplicitHeaderSessionBeforeStableSeed(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	first := qoderForwardMessagesForTest(t, svc, provider, "stable-header", []byte(`{
		"model":"deepseek-v4-pro",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"inspect"}],
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.162 (external, cli)"))
	second := qoderForwardMessagesForTest(t, svc, provider, "stable-header", []byte(`{
		"model":"deepseek-v4-pro",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.162 (external, cli)"))

	require.Equal(t, first["session_id"], second["session_id"])
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[3])["role"])

	other := qoderForwardMessagesForTest(t, svc, provider, "other-header", []byte(`{
		"model":"deepseek-v4-pro",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.162 (external, cli)"))
	require.NotEqual(t, first["session_id"], other["session_id"])
}

// TestQoderGatewayClaudeCodeUsesMetadataSessionBeforeStableSeed combines
// Claude system text and user-agent headers with metadata session identity.
// Matching metadata must
// reuse one upstream session and current prompt; another metadata identity
// must separate the conversation while retaining tools.
func TestQoderGatewayClaudeCodeUsesMetadataSessionBeforeStableSeed(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	metadata1 := `{"device_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","provider_uuid":"","session_id":"11111111-2222-3333-4444-555555555555"}`
	largeTools := qoderLargeToolsJSONForTest()
	first := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"deepseek-v4-pro",
		"metadata":{"user_id":`+strconv.Quote(metadata1)+`},
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"inspect"}],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	second := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"deepseek-v4-pro",
		"metadata":{"user_id":`+strconv.Quote(metadata1)+`},
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))

	require.Equal(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[3])["role"])
	require.Equal(t, "continue", qoderFixtureValue[map[string]any](t, second["chat_context"])["text"])

	metadata2 := `{"device_id":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","provider_uuid":"","session_id":"66666666-7777-8888-9999-aaaaaaaaaaaa"}`
	other := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"deepseek-v4-pro",
		"metadata":{"user_id":`+strconv.Quote(metadata2)+`},
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	require.NotEqual(t, first["session_id"], other["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, other["tools"]))
}

// TestQoderGatewayClaudeCodeIgnoresVolatileBillingCCHForSystemReuse changes
// only the volatile billing CCH text in Claude system input. Session reuse
// must survive that variation while tools and full message history remain in
// the forwarded request.
func TestQoderGatewayClaudeCodeIgnoresVolatileBillingCCHForSystemReuse(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	largeTools := qoderLargeToolsJSONForTest()
	system1 := "x-anthropic-billing-header: cc_version=2.1.177.19c; cc_entrypoint=sdk-cli; cch=29156;\n" +
		"You are a Claude agent, built on Anthropic's Claude Agent SDK.\n" +
		"Stable Claude Code system body."
	system2 := "x-anthropic-billing-header: cc_version=2.1.177.19c; cc_entrypoint=sdk-cli; cch=40d8d;\n" +
		"You are a Claude agent, built on Anthropic's Claude Agent SDK.\n" +
		"Stable Claude Code system body."
	first := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"billing-cch-session",
		"system":`+strconv.Quote(system1)+`,
		"messages":[{"role":"user","content":"inspect"}],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, sdk-cli)"))
	second := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"billing-cch-session",
		"system":`+strconv.Quote(system2)+`,
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, sdk-cli)"))

	require.Equal(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[3])["role"])
}

// TestQoderGatewayClaudeCodeUltimateStablePromptCacheKeyReportsCacheRead
// continues a stable cached ultimate-model request after different upstream
// usage reports. The second response must retain its session and expose
// measured uncached input, cache-read input and output values.
func TestQoderGatewayClaudeCodeUltimateStablePromptCacheKeyReportsCacheRead(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	largeTools := qoderLargeToolsJSONForTest()
	system1 := "x-anthropic-billing-header: cc_version=2.1.177.19c; cc_entrypoint=sdk-cli; cch=29156;\n" +
		"You are a Claude agent, built on Anthropic's Claude Agent SDK.\n" +
		"Stable Claude Code system body."
	system2 := "x-anthropic-billing-header: cc_version=2.1.177.19c; cc_entrypoint=sdk-cli; cch=40d8d;\n" +
		"You are a Claude agent, built on Anthropic's Claude Agent SDK.\n" +
		"Stable Claude Code system body."
	firstBody := []byte(`{
		"model":"claude-opus-4-6",
		"prompt_cache_key":"ultimate-cache-hit-session",
		"system":` + strconv.Quote(system1) + `,
		"messages":[{"role":"user","content":"inspect"}],
		"tools":` + largeTools + `,
		"stream":false
	}`)
	secondBody := []byte(`{
		"model":"claude-opus-4-6",
		"prompt_cache_key":"ultimate-cache-hit-session",
		"system":` + strconv.Quote(system2) + `,
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"tools":` + largeTools + `,
		"stream":false
	}`)

	client.Body = "data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":1200,\\\"completion_tokens\\\":30,\\\"total_tokens\\\":1230}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	firstResult := qoderForwardMessagesResultForTest(t, svc, provider, firstBody, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, sdk-cli)"))
	firstPayload := qoderLastUpstreamPayloadForTest(t, client)
	require.Equal(t, "ultimate", firstResult.UpstreamModel)
	require.Equal(t, 1200, firstResult.Usage.InputTokens)
	require.Equal(t, "ultimate", client.Headers["x-model-key"])

	client.Body = "data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":1500,\\\"completion_tokens\\\":33,\\\"total_tokens\\\":1533,\\\"prompt_tokens_details\\\":{\\\"cached_tokens\\\":1400,\\\"cacheable_tokens\\\":100}}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	secondResult, secondResponse := qoderForwardMessagesResultAndBodyForTest(t, svc, provider, secondBody, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, sdk-cli)"))
	secondPayload := qoderLastUpstreamPayloadForTest(t, client)

	require.Equal(t, "ultimate", secondResult.UpstreamModel)
	require.Equal(t, firstPayload["session_id"], secondPayload["session_id"])
	require.Equal(t, 100, secondResult.Usage.InputTokens)
	require.Equal(t, 1400, secondResult.Usage.CacheReadInputTokens)
	require.Equal(t, 33, secondResult.Usage.OutputTokens)
	require.Equal(t, int64(1400), gjson.Get(secondResponse, "usage.cache_read_input_tokens").Int())
	require.Equal(t, int64(100), gjson.Get(secondResponse, "usage.input_tokens").Int())
}

// TestQoderGatewayStillFullReplaysWhenNonBillingSystemChanges changes
// substantive Claude system text beyond billing metadata. Forwarding must
// create a new session and retain tools plus complete four-message history for
// the new request.
func TestQoderGatewayStillFullReplaysWhenNonBillingSystemChanges(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	largeTools := qoderLargeToolsJSONForTest()
	system1 := "x-anthropic-billing-header: cc_version=2.1.177.19c; cc_entrypoint=sdk-cli; cch=29156;\n" +
		"Stable Claude Code system body."
	system2 := "x-anthropic-billing-header: cc_version=2.1.177.19c; cc_entrypoint=sdk-cli; cch=40d8d;\n" +
		"Changed Claude Code system body."
	first := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"system-change-session",
		"system":`+strconv.Quote(system1)+`,
		"messages":[{"role":"user","content":"inspect"}],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, sdk-cli)"))
	second := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"system-change-session",
		"system":`+strconv.Quote(system2)+`,
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, sdk-cli)"))

	require.NotEqual(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	require.Len(t, qoderFixtureValue[[]any](t, second["messages"]), 4)
}

// TestQoderGatewayReusedAnthropicConversationOmitsUnchangedTools extends a
// reused Anthropic conversation with unchanged tools. The current assertions
// require those tools and all four history turns to remain present alongside
// the reused upstream session.
func TestQoderGatewayReusedAnthropicConversationOmitsUnchangedTools(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	largeTools := qoderLargeToolsJSONForTest()
	first := qoderForwardMessagesForTest(t, svc, provider, "stable-session", []byte(`{
		"model":"deepseek-v4-pro",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"你好"}],
		"tools":`+largeTools+`,
		"stream":false
	}`))
	second := qoderForwardMessagesForTest(t, svc, provider, "stable-session", []byte(`{
		"model":"deepseek-v4-pro",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"你好"},
			{"role":"assistant","content":"你好"},
			{"role":"user","content":"你好"}
		],
		"tools":`+largeTools+`,
		"stream":false
	}`))

	require.NotEmpty(t, qoderFixtureValue[[]any](t, first["tools"]))
	require.Equal(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 4)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[3])["role"])
}

// TestQoderGatewayUsageComesFromUpstreamSSE supplies an upstream usage report
// unrelated to request byte length. The result must retain those measured
// input/output counts instead of estimating input from the body size.
func TestQoderGatewayUsageComesFromUpstreamSSE(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
		"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":1234,\\\"completion_tokens\\\":56,\\\"total_tokens\\\":1290}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	body := []byte(`{
		"model":"deepseek-v4-pro",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"你好"},
			{"role":"assistant","content":"你好"},
			{"role":"user","content":"你好"}
		],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":false
	}`)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	result, err := forwardQoderAttempt(context.Background(), c, svc.Runtime, provider, body, protocolcore.ProtocolAnthropicMessages, false)

	require.NoError(t, err)
	require.Equal(t, 1234, result.Usage.InputTokens)
	require.Equal(t, 56, result.Usage.OutputTokens)
	require.NotEqual(t, len(body), result.Usage.InputTokens)
}

// TestQoderGatewayBuildsClientVisibleOpenAIUsageWithUpstreamTotals supplies
// prompt usage containing a large cached portion. Internal usage must separate
// uncached and cache-read input while client Chat usage preserves upstream
// totals and detail buckets.
func TestQoderGatewayBuildsClientVisibleOpenAIUsageWithUpstreamTotals(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
		qoderCachedUsageSSEForTest +
		"data: {\"body\":\"[DONE]\"}\n\n"
	body := []byte(`{"model":"auto","messages":[{"role":"user","content":"hi"}],"stream":false}`)

	result, response := qoderForwardChatCompletionsResultAndBodyForTest(t, svc, provider, "", body)

	require.Equal(t, 25, result.Usage.InputTokens)
	require.Equal(t, 66612, result.Usage.CacheReadInputTokens)
	require.Equal(t, 6, result.Usage.OutputTokens)
	require.Equal(t, int64(66637), gjson.Get(response, "usage.prompt_tokens").Int())
	require.Equal(t, int64(6), gjson.Get(response, "usage.completion_tokens").Int())
	require.Equal(t, int64(66643), gjson.Get(response, "usage.total_tokens").Int())
	require.Equal(t, int64(66612), gjson.Get(response, "usage.prompt_tokens_details.cached_tokens").Int())
	require.Equal(t, int64(0), gjson.Get(response, "usage.completion_tokens_details.reasoning_tokens").Int())
}

// TestQoderGatewayOpenAIUsageKeepsUpstreamPromptWhenCachedExceedsPrompt
// reports cached tokens greater than total prompt tokens. Internal uncached
// input clamps to zero while client Chat prompt_tokens reflects CacheRead (15).
func TestQoderGatewayOpenAIUsageKeepsUpstreamPromptWhenCachedExceedsPrompt(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
		"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":10,\\\"completion_tokens\\\":6,\\\"total_tokens\\\":16,\\\"prompt_tokens_details\\\":{\\\"cached_tokens\\\":15,\\\"cacheable_tokens\\\":1}}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	body := []byte(`{"model":"auto","messages":[{"role":"user","content":"hi"}],"stream":false}`)

	result, response := qoderForwardChatCompletionsResultAndBodyForTest(t, svc, provider, "", body)

	require.Equal(t, 0, result.Usage.InputTokens)
	require.Equal(t, 15, result.Usage.CacheReadInputTokens)
	require.Equal(t, int64(15), gjson.Get(response, "usage.prompt_tokens").Int())
	require.Equal(t, int64(6), gjson.Get(response, "usage.completion_tokens").Int())
	require.Equal(t, int64(21), gjson.Get(response, "usage.total_tokens").Int())
	require.Equal(t, int64(15), gjson.Get(response, "usage.prompt_tokens_details.cached_tokens").Int())
	require.False(t, gjson.Get(response, "usage.cache_creation_input_tokens").Exists())
}

// TestQoderGatewayBuildsClientVisibleAnthropicUsageWithCacheRead converts
// cached upstream usage into Messages output. Internal and client input must
// expose the uncached portion separately from cache reads, with zero
// cache-creation tokens.
func TestQoderGatewayBuildsClientVisibleAnthropicUsageWithCacheRead(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	client.Body = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
		qoderCachedUsageSSEForTest +
		"data: {\"body\":\"[DONE]\"}\n\n"
	body := []byte(`{"model":"auto","messages":[{"role":"user","content":"hi"}],"stream":false}`)

	result, response := qoderForwardMessagesResultAndBodyForTest(t, svc, provider, body)

	require.Equal(t, 25, result.Usage.InputTokens)
	require.Equal(t, 66612, result.Usage.CacheReadInputTokens)
	require.Equal(t, 6, result.Usage.OutputTokens)
	require.Equal(t, int64(25), gjson.Get(response, "usage.input_tokens").Int())
	require.Equal(t, int64(66612), gjson.Get(response, "usage.cache_read_input_tokens").Int())
	require.Equal(t, int64(6), gjson.Get(response, "usage.output_tokens").Int())
	require.Zero(t, gjson.Get(response, "usage.cache_creation_input_tokens").Int())
}

// TestQoderGatewayDoesNotSubtractPreviousUsageOnFullReplay replays a
// conversation after a larger second usage report. The second result must
// retain the full current upstream input/output counts, tools and history
// without subtracting previous usage.
func TestQoderGatewayDoesNotSubtractPreviousUsageOnFullReplay(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	firstBody := []byte(`{
		"model":"deepseek-v4-pro",
		"prompt_cache_key":"usage-delta-session",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"inspect"}],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":false
	}`)
	secondBody := []byte(`{
		"model":"deepseek-v4-pro",
		"prompt_cache_key":"usage-delta-session",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":"ok"},
			{"role":"user","content":"continue"}
		],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":false
	}`)

	client.Body = "data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":1200,\\\"completion_tokens\\\":30,\\\"total_tokens\\\":1230}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	firstResult := qoderForwardMessagesResultForTest(t, svc, provider, firstBody, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	require.Equal(t, 1200, firstResult.Usage.InputTokens)

	client.Body = "data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":10000,\\\"completion_tokens\\\":33,\\\"total_tokens\\\":10033}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	secondResult := qoderForwardMessagesResultForTest(t, svc, provider, secondBody, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	require.Equal(t, 10000, secondResult.Usage.InputTokens)
	require.Equal(t, 33, secondResult.Usage.OutputTokens)
	secondPayload := qoderLastUpstreamPayloadForTest(t, client)
	require.NotEmpty(t, qoderFixtureValue[[]any](t, secondPayload["tools"]))
	require.Len(t, qoderFixtureValue[[]any](t, secondPayload["messages"]), 4)
}

// TestQoderGatewayReturnsDeltaUsageToAnthropicClientOnReusedConversation
// reuses a session with a larger second upstream usage report. Current
// assertions require full second-request input/output counts in both result
// and Anthropic response, with tools retained.
func TestQoderGatewayReturnsDeltaUsageToAnthropicClientOnReusedConversation(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	firstBody := []byte(`{
		"model":"deepseek-v4-pro",
		"prompt_cache_key":"client-usage-delta-session",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"inspect"}],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":false
	}`)
	secondBody := []byte(`{
		"model":"deepseek-v4-pro",
		"prompt_cache_key":"client-usage-delta-session",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"bash","input":{"cmd":"pwd"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"repo"}]},
			{"role":"assistant","content":"done"},
			{"role":"user","content":"continue"}
		],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":false
	}`)

	client.Body = "data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":1200,\\\"completion_tokens\\\":30,\\\"total_tokens\\\":1230}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	firstResult, firstResponse := qoderForwardMessagesResultAndBodyForTest(t, svc, provider, firstBody, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	require.Equal(t, 1200, firstResult.Usage.InputTokens)
	require.Equal(t, int64(1200), gjson.Get(firstResponse, "usage.input_tokens").Int())

	client.Body = "data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":10000,\\\"completion_tokens\\\":33,\\\"total_tokens\\\":10033}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	secondResult, secondResponse := qoderForwardMessagesResultAndBodyForTest(t, svc, provider, secondBody, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	require.Equal(t, 10000, secondResult.Usage.InputTokens)
	require.Equal(t, 33, secondResult.Usage.OutputTokens)
	require.Equal(t, int64(10000), gjson.Get(secondResponse, "usage.input_tokens").Int())
	require.Equal(t, int64(33), gjson.Get(secondResponse, "usage.output_tokens").Int())

	secondPayload := qoderLastUpstreamPayloadForTest(t, client)
	require.NotEmpty(t, qoderFixtureValue[[]any](t, secondPayload["tools"]))
}

// TestQoderGatewayAnthropicToolUseResultSendsIncrementalTail continues an
// Anthropic tool turn under the same session. Current assertions require tools
// and full system/user/assistant/tool history, with tool identity and result
// text also represented in the prompt.
func TestQoderGatewayAnthropicToolUseResultSendsIncrementalTail(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	first := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"prompt_cache_key":"anthropic-tool-session",
		"system":"be useful",
		"messages":[{"role":"user","content":"inspect"}],
		"tools":[{"name":"bash","input_schema":{"type":"object"}}],
		"stream":false
	}`))
	second := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"prompt_cache_key":"anthropic-tool-session",
		"system":"be useful",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"bash","input":{"cmd":"pwd"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"repo"}]}
		],
		"tools":[{"name":"bash","input_schema":{"type":"object"}}],
		"stream":false
	}`))

	require.Equal(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 4)

	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	user := qoderFixtureValue[map[string]any](t, messages[1])
	require.Equal(t, "user", user["role"])
	require.Equal(t, "inspect", qoderPayloadMessageTextForTest(user))

	assistant := qoderFixtureValue[map[string]any](t, messages[2])
	require.Equal(t, "assistant", assistant["role"])
	toolCalls := qoderFixtureValue[[]any](t, assistant["tool_calls"])
	require.Len(t, toolCalls, 1)
	require.Equal(t, "call_1", qoderFixtureValue[map[string]any](t, toolCalls[0])["id"])

	tool := qoderFixtureValue[map[string]any](t, messages[3])
	require.Equal(t, "tool", tool["role"])
	require.Equal(t, "call_1", tool["tool_call_id"])
	require.Equal(t, "call_1", tool["tool_call_call_id"])
	require.Equal(t, "bash", tool["name"])
	require.Equal(t, "repo", tool["content"])
	prompt := qoderPayloadPromptForTest(t, second)
	require.Contains(t, prompt, `<tool_result id="call_1">`)
	require.Contains(t, prompt, "repo\n")
}

// TestQoderGatewayOpenAIToolCallsSendIncrementalTail continues a Chat
// function-call turn under the same session. The forwarded request must retain
// tools and full user/assistant/tool history while the prompt contains the
// identified tool result.
func TestQoderGatewayOpenAIToolCallsSendIncrementalTail(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	first := qoderForwardChatCompletionsForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"prompt_cache_key":"openai-tool-session",
		"messages":[{"role":"user","content":"run pwd"}],
		"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],
		"stream":false
	}`))
	second := qoderForwardChatCompletionsForTest(t, svc, provider, "", []byte(`{
		"model":"auto",
		"prompt_cache_key":"openai-tool-session",
		"messages":[
			{"role":"user","content":"run pwd"},
			{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":{"cmd":"pwd"}}}]},
			{"role":"tool","tool_call_id":"call_1","name":"bash","content":"/repo"}
		],
		"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],
		"stream":false
	}`))

	require.Equal(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 3)
	user := qoderFixtureValue[map[string]any](t, messages[0])
	require.Equal(t, "user", user["role"])
	require.Equal(t, "run pwd", qoderPayloadMessageTextForTest(user))
	assistant := qoderFixtureValue[map[string]any](t, messages[1])
	require.Equal(t, "assistant", assistant["role"])
	require.Len(t, qoderFixtureValue[[]any](t, assistant["tool_calls"]), 1)
	tool := qoderFixtureValue[map[string]any](t, messages[2])
	require.Equal(t, "tool", tool["role"])
	require.Equal(t, "call_1", tool["tool_call_id"])
	require.Equal(t, "call_1", tool["tool_call_call_id"])
	require.Equal(t, "bash", tool["name"])
	prompt := qoderPayloadPromptForTest(t, second)
	require.Contains(t, prompt, `<tool_result id="call_1">`)
	require.Contains(t, prompt, "/repo\n")
}

// TestQoderGatewayRepeatedClaudeCodeRequestKeepsNonEmptyIncrementalTail
// repeats Claude conversation input after completed tool work. The session
// must remain stable and the forwarded request must retain tools and six
// history messages with its original prefix.
func TestQoderGatewayRepeatedClaudeCodeRequestKeepsNonEmptyIncrementalTail(t *testing.T) {
	provider, svc, _ := gatewaytestkit.NewDefaultQoderFixture()
	body := []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"repeated-claude-request-session",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"bash","input":{"cmd":"pwd"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"repo"}]},
			{"role":"assistant","content":"done"},
			{"role":"user","content":"continue"}
		],
		"tools":` + qoderLargeToolsJSONForTest() + `,
		"stream":false
	}`)

	first := qoderForwardMessagesForTest(t, svc, provider, "", body, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	second := qoderForwardMessagesForTest(t, svc, provider, "", body, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))

	require.Equal(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 6)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
}

// TestQoderGatewayStreamsDeltaUsageToOpenAIClientOnReusedConversation reuses
// Chat session state with a larger second usage report. Current assertions
// require full second upstream counts in both returned usage and streaming
// client usage, while tools remain present.
func TestQoderGatewayStreamsDeltaUsageToOpenAIClientOnReusedConversation(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	firstBody := []byte(`{
		"model":"auto",
		"prompt_cache_key":"openai-stream-usage-delta-session",
		"messages":[{"role":"user","content":"run pwd"}],
		"tools":[{"type":"function","function":{"name":"bash","description":"Run shell","parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}}}],
		"stream":true,
		"stream_options":{"include_usage":true}
	}`)
	secondBody := []byte(`{
		"model":"auto",
		"prompt_cache_key":"openai-stream-usage-delta-session",
		"messages":[
			{"role":"user","content":"run pwd"},
			{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"cmd\":\"pwd\"}"}}]},
			{"role":"tool","tool_call_id":"call_1","name":"bash","content":"/repo"},
			{"role":"assistant","content":"done"},
			{"role":"user","content":"continue"}
		],
		"tools":[{"type":"function","function":{"name":"bash","description":"Run shell","parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}}}],
		"stream":true,
		"stream_options":{"include_usage":true}
	}`)

	client.Body = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
		"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":1200,\\\"completion_tokens\\\":30,\\\"total_tokens\\\":1230}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	firstResult, firstStream := qoderForwardChatCompletionsResultAndBodyForTest(t, svc, provider, "", firstBody, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	require.Equal(t, 1200, firstResult.Usage.InputTokens)
	require.Contains(t, firstStream, `"prompt_tokens":1200`)

	client.Body = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
		"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":10000,\\\"completion_tokens\\\":33,\\\"total_tokens\\\":10033}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	secondResult, secondStream := qoderForwardChatCompletionsResultAndBodyForTest(t, svc, provider, "", secondBody, qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	secondPayload := qoderLastUpstreamPayloadForTest(t, client)
	require.NotEmpty(t, qoderFixtureValue[[]any](t, secondPayload["tools"]))
	require.Equal(t, 10000, secondResult.Usage.InputTokens)
	require.Equal(t, 33, secondResult.Usage.OutputTokens)
	require.Contains(t, secondStream, `"prompt_tokens":10000`)
	require.Contains(t, secondStream, `"completion_tokens":33`)
}

// TestQoderGatewayAnthropicConversationAfterToolResultKeepsReducingPayload
// extends a conversation after a tool result and assistant reply. Current
// assertions require six complete history turns and tools, with second payload
// size at least the first payload size.
func TestQoderGatewayAnthropicConversationAfterToolResultKeepsReducingPayload(t *testing.T) {
	provider, svc, client := gatewaytestkit.NewDefaultQoderFixture()
	largeTools := qoderLargeToolsJSONForTest()
	first := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"post-tool-reducing-session",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[{"role":"user","content":"inspect"}],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))
	second := qoderForwardMessagesForTest(t, svc, provider, "", []byte(`{
		"model":"glm-5.1",
		"prompt_cache_key":"post-tool-reducing-session",
		"system":"You are Claude Code, Anthropic's official CLI for Claude.",
		"messages":[
			{"role":"user","content":"inspect"},
			{"role":"assistant","content":[{"type":"tool_use","id":"call_1","name":"bash","input":{"cmd":"pwd"}}]},
			{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"repo"}]},
			{"role":"assistant","content":"done"},
			{"role":"user","content":"continue"}
		],
		"tools":`+largeTools+`,
		"stream":false
	}`), qoderHeader("User-Agent", "claude-cli/2.1.177 (external, cli)"))

	require.Equal(t, first["session_id"], second["session_id"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, second["tools"]))
	messages := qoderFixtureValue[[]any](t, second["messages"])
	require.Len(t, messages, 6)
	require.Equal(t, "system", qoderFixtureValue[map[string]any](t, messages[0])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[1])["role"])
	require.Equal(t, "inspect", qoderPayloadMessageTextForTest(qoderFixtureValue[map[string]any](t, messages[1])))
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[2])["role"])
	require.NotEmpty(t, qoderFixtureValue[[]any](t, qoderFixtureValue[map[string]any](t, messages[2])["tool_calls"]))
	require.Equal(t, "tool", qoderFixtureValue[map[string]any](t, messages[3])["role"])
	require.Equal(t, "call_1", qoderFixtureValue[map[string]any](t, messages[3])["tool_call_id"])
	require.Equal(t, "assistant", qoderFixtureValue[map[string]any](t, messages[4])["role"])
	require.Equal(t, "user", qoderFixtureValue[map[string]any](t, messages[5])["role"])

	require.GreaterOrEqual(t, len(client.BodyAt(1)), len(client.BodyAt(0)))
}

// TestQoderGatewayForwardChatCompletionsHonorsCanceledContext forwards Chat
// input under a canceled client context. The attempt must return
// context.Canceled and leave the response writer untouched so outer
// cancellation handling owns presentation.
func TestQoderGatewayForwardChatCompletionsHonorsCanceledContext(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	provider := &providercore.Record{
		ID:       93,
		Platform: capability.PlatformQoder,
		Type:     capability.ProviderTypeCosy,
		Credentials: map[string]any{
			"pat": "pat-token",
		},
	}
	tokenSource := provideradapter.NewQoderTokenProvider(qoder.SessionBuilder{ExchangePAT: func(ctx context.Context, _ string, _ *qoder.MachineIdentity) (*qoder.AuthIdentity, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			return &qoder.AuthIdentity{SecurityOauthToken: "token", UID: "uid"}, nil
		}
	}})
	svc := gatewaytestkit.NewQoderFixture(tokenSource, nil, nil)

	_, err := forwardQoderAttempt(ctx, c, svc.Runtime, provider, []byte(`{"model":"auto","messages":[{"role":"user","content":"hi"}]}`), protocolcore.ProtocolOpenAIChatCompletions, false)

	require.ErrorIs(t, err, context.Canceled)
	require.False(t, c.Writer.Written())
}

// qoderPayloadPromptForTest reads the nested prompt string from a decoded
// Qoder payload. Missing or differently typed chat_context, text object or
// text string fails the test through the typed fixture helper.
func qoderPayloadPromptForTest(t *testing.T, payload map[string]any) string {
	t.Helper()
	chatContext := qoderFixtureValue[map[string]any](t, payload["chat_context"])
	return qoderFixtureValue[string](t, chatContext["text"])
}

// qoderPayloadMessageTextForTest returns the first string text block,
// including an empty string, before legacy string content. Nil or text-free
// messages return empty; callers use this permissive reader to compare both
// upstream history encodings.
func qoderPayloadMessageTextForTest(msg map[string]any) string {
	if msg == nil {
		return ""
	}
	// Upstream fixture history can use typed content blocks or legacy string
	// content; ignore unusable blocks before selecting the first readable representation.
	contents, _ := msg["content"].([]any)
	for _, raw := range contents {
		block, ok := raw.(map[string]any)
		if !ok || block["type"] != "text" {
			continue
		}
		if text, ok := block["text"].(string); ok {
			return text
		}
	}
	if text, ok := msg["content"].(string); ok {
		return text
	}
	return ""
}

// qoderHeader returns one request-header override for the forwarding fixture
// helpers.
func qoderHeader(key, value string) qoderForwardTestHeader {
	return qoderForwardTestHeader{key: key, value: value}
}

// qoderForwardChatCompletionsForTest forwards Chat input and returns the last
// decoded upstream payload. It requires a QoderClient fixture and successful
// forwarding, discarding response/result values while failing on missing
// captured input.
func qoderForwardChatCompletionsForTest(t *testing.T, svc *gatewaytestkit.QoderFixture, provider *providercore.Record, sessionID string, body []byte, headers ...qoderForwardTestHeader) map[string]any {
	t.Helper()
	_, _ = qoderForwardChatCompletionsResultAndBodyForTest(t, svc, provider, sessionID, body, headers...)
	return qoderLastUpstreamPayloadForTest(t, qoderFixtureValue[*gatewaytestkit.QoderClient](t, svc.Client))
}

// qoderForwardChatCompletionsResultAndBodyForTest forwards Chat input into an
// in-memory recorder. Nonempty sessionID sets its header before additional
// overrides, which can replace it; forwarding errors fail the test and the
// result/body are returned.
func qoderForwardChatCompletionsResultAndBodyForTest(t *testing.T, svc *gatewaytestkit.QoderFixture, provider *providercore.Record, sessionID string, body []byte, headers ...qoderForwardTestHeader) (*forwardcore.MessagesResult, string) {
	t.Helper()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(body))
	if sessionID != "" {
		c.Request.Header.Set("session_id", sessionID)
	}
	for _, header := range headers {
		c.Request.Header.Set(header.key, header.value)
	}
	result, err := forwardQoderAttempt(context.Background(), c, svc.Runtime, provider, body, protocolcore.ProtocolOpenAIChatCompletions, false)
	require.NoError(t, err)
	return result, rec.Body.String()
}

// qoderForwardMessagesForTest forwards Messages input and returns the latest
// upstream payload. A nonblank sessionID supplies a header before later
// overrides, which can replace it. The helper requires a nonnil result and a
// client exposing body count/index access.
func qoderForwardMessagesForTest(t *testing.T, svc *gatewaytestkit.QoderFixture, provider *providercore.Record, sessionID string, body []byte, headers ...qoderForwardTestHeader) map[string]any {
	t.Helper()
	result := qoderForwardMessagesResultForTest(t, svc, provider, body, append([]qoderForwardTestHeader{qoderHeader("session_id", sessionID)}, headers...)...)
	require.NotNil(t, result)
	client, ok := svc.Client.(interface {
		// BodyAt returns fixture request bytes for the specified index. Consumers
		// decode valid JSON and require that the requested call exists; this port
		// does not prescribe synchronization or copy behavior beyond its concrete
		// fixture.
		BodyAt(int) []byte
		// BodyCount returns the number of captured fixture request bodies for
		// observation.
		BodyCount() int
	})
	require.True(t, ok)
	return qoderPayloadAtForTest(t, client, client.BodyCount()-1)
}

// qoderForwardMessagesResultForTest returns successful Messages forwarding
// observations while discarding client response bytes. Callers supply the
// bound fixture and header overrides; any forwarding error fails the test in
// the delegated helper.
func qoderForwardMessagesResultForTest(t *testing.T, svc *gatewaytestkit.QoderFixture, provider *providercore.Record, body []byte, headers ...qoderForwardTestHeader) *forwardcore.MessagesResult {
	t.Helper()
	result, _ := qoderForwardMessagesResultAndBodyForTest(t, svc, provider, body, headers...)
	return result
}

// qoderForwardMessagesResultAndBodyForTest forwards Messages input with
// nonblank header overrides. X-Test-Claude-Code-Context=true installs the
// test-only detected-client context before forwarding; errors fail the test,
// and result plus recorded response bytes are returned.
func qoderForwardMessagesResultAndBodyForTest(t *testing.T, svc *gatewaytestkit.QoderFixture, provider *providercore.Record, body []byte, headers ...qoderForwardTestHeader) (*forwardcore.MessagesResult, string) {
	t.Helper()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", bytes.NewReader(body))
	for _, header := range headers {
		if strings.TrimSpace(header.value) != "" {
			c.Request.Header.Set(header.key, header.value)
		}
	}
	// Install detection directly for stable-prefix fixtures so conversation behavior
	// can be exercised independently of production client-signature recognition on these bytes.
	if strings.EqualFold(strings.TrimSpace(c.Request.Header.Get("X-Test-Claude-Code-Context")), "true") {
		c.Request = c.Request.WithContext(requeststate.SetClaudeCodeClient(c.Request.Context(), true))
	}
	result, err := forwardQoderAttempt(context.Background(), c, svc.Runtime, provider, body, protocolcore.ProtocolAnthropicMessages, false)
	require.NoError(t, err)
	return result, rec.Body.String()
}

// qoderForwardResponsesResultAndBodyForTest forwards Responses input into an
// in-memory recorder using nonblank header overrides. Forwarding errors fail
// the test, while successful result observations and encoded response bytes
// are returned for assertions.
func qoderForwardResponsesResultAndBodyForTest(t *testing.T, svc *gatewaytestkit.QoderFixture, provider *providercore.Record, body []byte, headers ...qoderForwardTestHeader) (*forwardcore.MessagesResult, string) {
	t.Helper()

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
	for _, header := range headers {
		if strings.TrimSpace(header.value) != "" {
			c.Request.Header.Set(header.key, header.value)
		}
	}
	result, err := forwardQoderAttempt(context.Background(), c, svc.Runtime, provider, body, protocolcore.ProtocolOpenAIResponses, false)
	require.NoError(t, err)
	return result, rec.Body.String()
}

// qoderLastUpstreamPayloadForTest decodes the final recorded Qoder request
// payload. A nil client or empty capture fails the test before indexed
// reading; callers receive a decoded object from the fixture’s latest call.
func qoderLastUpstreamPayloadForTest(t *testing.T, client *gatewaytestkit.QoderClient) map[string]any {
	t.Helper()
	require.NotNil(t, client)
	require.NotZero(t, client.BodyCount())
	return qoderPayloadAtForTest(t, client, client.BodyCount()-1)
}

// qoderPayloadAtForTest decodes JSON captured at the specified fixture request
// index. Callers supply a client with an existing valid body; JSON decode
// failure aborts the test before any payload assertion.
func qoderPayloadAtForTest(t *testing.T, client interface {
	// BodyAt returns fixture request bytes for the specified index. Consumers
	// decode valid JSON and require that the requested call exists; this port
	// does not prescribe synchronization or copy behavior beyond its concrete
	// fixture.
	BodyAt(int) []byte
}, index int,
) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(client.BodyAt(index), &payload))
	return payload
}

// newBlockingQoderClientStub creates the blocking request fixture with a
// condition sharing its mutex. Callers wait until the first pipe writer has
// been installed before invoking finishFirst once; later calls consume
// ordinary or one-shot malformed responses.
func newBlockingQoderClientStub(t *testing.T) *blockingQoderClientStub {
	t.Helper()
	client := &blockingQoderClientStub{t: t}
	client.cond = sync.NewCond(&client.mu)
	return client
}

// StreamRequestContext captures request bytes under mu and exposes an open
// pipe for the first accepted call. Callers complete that pipe once or cancel
// its execution context; later calls consume a one-shot malformed response
// flag or return ordinary content and usage.
//
// Body copying and call numbering occur under the lock, while pipe creation
// and HTTP reading progress independently. The cancellation goroutine closes
// the first writer so a waiting reader can exit without test completion. Later
// error consumption clears nextError under the same mutex, preventing one
// injected malformed response from contaminating every subsequent request.
func (s *blockingQoderClientStub) StreamRequestContext(ctx context.Context, _ *qoder.SessionContext, _ string, bodyJSON []byte, headers map[string]string) (*http.Response, error) {
	s.mu.Lock()
	callNumber := len(s.Bodies) + 1
	// Copy request bytes and publish their count under mu before notification,
	// keeping concurrent inspection independent of the caller’s later byte-slice
	// mutation.
	s.Bodies = append(s.Bodies, append([]byte(nil), bodyJSON...))
	s.Headers = headers
	s.cond.Broadcast()
	s.mu.Unlock()

	if callNumber == 1 {
		reader, writer := io.Pipe()
		s.mu.Lock()
		s.firstWriter = writer
		s.mu.Unlock()
		// The pipe reader can outlive test control while forwarding is blocked;
		// cancellation closes its writer and unblocks reading even without finishFirst.
		go func() {
			<-ctx.Done()
			// Cancellation cleanup observes no separate close result; reader termination
			// is the fixture signal, and the injected upstream failure remains independent.
			_ = writer.Close()
		}()
		return &http.Response{StatusCode: http.StatusOK, Body: reader}, nil
	}
	s.mu.Lock()
	// Consume malformed-response injection once under mu so another concurrent
	// request cannot reuse the same failure after this call claims it.
	nextError := s.nextError
	s.nextError = false
	s.mu.Unlock()
	if nextError {
		body := "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\""
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
	}
	body := "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
		"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":5,\\\"completion_tokens\\\":1,\\\"total_tokens\\\":6}}\"}\n\n" +
		"data: {\"body\":\"[DONE]\"}\n\n"
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}, nil
}

// waitForCalls waits up to two seconds for the protected request count,
// failing the test on timeout. Callers choose a count expected from started
// forwarding; polling releases the mutex during each ten-millisecond interval.
//
// The count is checked again only after reacquiring the lock, allowing capture
// to progress while the test sleeps. Recording the first body precedes
// installation of its pipe writer, so this count alone does not guarantee
// finishFirst readiness. Although capture broadcasts cond, this helper does
// not use Cond.Wait; the deadline is a test observation budget.
func (s *blockingQoderClientStub) waitForCalls(count int) {
	s.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.Bodies) < count {
		if time.Now().After(deadline) {
			s.t.Fatalf("timed out waiting for %d qoder calls, got %d", count, len(s.Bodies))
		}
		s.mu.Unlock()
		// Release the mutex during polling so upstream request capture can increase
		// the observed count before the next bounded readiness check.
		time.Sleep(10 * time.Millisecond)
		s.mu.Lock()
	}
}

// finishFirst writes content, usage and DONE into the accepted first response
// pipe and closes it. Callers wait until firstWriter is installed and invoke
// it exactly once; waitForCalls alone does not establish that readiness.
// Absent or already-consumed completion ownership fails the test.
//
// The firstDone flag and writer pointer are read under the mutex before pipe
// I/O begins, preventing a second controller from claiming the same
// completion. Pipe writes happen after unlocking so the stream reader can
// progress independently. Write or close failure aborts the test instead of
// pretending that first-stream completion succeeded.
func (s *blockingQoderClientStub) finishFirst() {
	s.t.Helper()
	s.mu.Lock()
	writer := s.firstWriter
	if s.firstDone {
		writer = nil
	}
	// Claim pipe completion while holding mu, then release the lock before I/O
	// so readers progress and a second controller cannot complete this stream again.
	s.firstDone = true
	s.mu.Unlock()
	require.NotNil(s.t, writer)
	_, err := io.WriteString(writer,
		"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n"+
			"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":10,\\\"completion_tokens\\\":1,\\\"total_tokens\\\":11}}\"}\n\n"+
			"data: {\"body\":\"[DONE]\"}\n\n")
	require.NoError(s.t, err)
	// Close the completed first pipe after writing its terminal marker so the
	// independent reader can finish and the test can join forwarding.
	require.NoError(s.t, writer.Close())
}

// BodyAt returns an independent copy of one captured request under the mutex.
// Out-of-range indexes return nil; callers wait for the required capture
// before decoding so the returned bytes represent that recorded request.
func (s *blockingQoderClientStub) BodyAt(index int) []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index < 0 || index >= len(s.Bodies) {
		return nil
	}
	// Copy captured bytes while mu still protects the indexed entry, allowing the
	// caller to decode or mutate its result without changing fixture storage.
	return append([]byte(nil), s.Bodies[index]...)
}

// BodyCount returns the captured request count under the fixture mutex.
func (s *blockingQoderClientStub) BodyCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.Bodies)
}

// qoderLargeToolsJSONForTest returns two object-schema tools with deliberately
// large descriptions for replay fixtures. The fixed structure is marshaled as
// JSON; a construction failure panics rather than supplying invalid tool
// input.
func qoderLargeToolsJSONForTest() string {
	// Large descriptions make retained tools significant in replay payload-size assertions.
	description := strings.Repeat("large schema field used by Claude Code. ", 80)
	body, err := json.Marshal([]map[string]any{
		{
			"name":        "Read",
			"description": description,
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"file_path": map[string]any{
						"type":        "string",
						"description": description,
					},
					"offset": map[string]any{
						"type":        "integer",
						"description": description,
					},
				},
				"required": []string{"file_path"},
			},
		},
		{
			"name":        "Bash",
			"description": description,
			"input_schema": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"command": map[string]any{
						"type":        "string",
						"description": description,
					},
					"timeout": map[string]any{
						"type":        "integer",
						"description": description,
					},
				},
				"required": []string{"command"},
			},
		},
	})
	if err != nil {
		panic(err)
	}
	return string(body)
}

// TestQoderConversationKeyPrefersExplicitSessionOverClaudeCodeStableSeed
// combines Claude client headers with explicit session identity. Conversation
// key source must be header with the expected provider-scoped value. The
// fixture supplies no detected Claude context for stable-prefix matching.
func TestQoderConversationKeyPrefersExplicitSessionOverClaudeCodeStableSeed(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request.Header.Set("User-Agent", "claude-cli/2.1.177 (external, cli)")
	c.Request.Header.Set("X-Claude-Code-Session-Id", "header-session")

	request := qoder.QoderPayloadRequest{
		Model:    "deepseek-v4-pro",
		Messages: []qoder.QoderMessage{{Role: "user", Text: "inspect"}},
	}

	key, source := qoder.QoderConversationKey(QoderRequestMetadata(c), 7, request)

	require.Equal(t, "header", source)
	require.Equal(t, qoder.QoderProviderScopedConversationKey(7, "header:"+upstreamcore.IsolateSessionID(0, "header-session")), key)
	require.NotContains(t, key, "stable_seed")
}

// TestQoderConversationKeyPrefersMetadataOverClaudeCodeStableSeed combines
// Claude client headers with metadata session identity. Conversation key
// source and expected key must use metadata and provider scope. The fixture
// supplies no detected Claude context for stable-prefix matching.
func TestQoderConversationKeyPrefersMetadataOverClaudeCodeStableSeed(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request.Header.Set("User-Agent", "claude-cli/2.1.177 (external, cli)")

	request := qoder.QoderPayloadRequest{
		Model:          "deepseek-v4-pro",
		MetadataUserID: anthropic.FormatMetadataUserID("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "", "session-123", "2.1.80"),
		Messages:       []qoder.QoderMessage{{Role: "user", Text: "inspect"}},
	}

	key, source := qoder.QoderConversationKey(QoderRequestMetadata(c), 7, request)

	require.Equal(t, "metadata_user_id", source)
	require.Equal(t, qoder.QoderProviderScopedConversationKey(7, "metadata_user_id:"+upstreamcore.IsolateSessionID(0, "session-123")), key)
	require.NotContains(t, key, "stable_seed")
}

// TestQoderGatewayNonStreamingReadDoesNotCommitResponseBeforeError reads a
// wrapped upstream authentication failure without a client output sink. It
// must return an error with no events, response bytes, cache headers or
// written writer state.
func TestQoderGatewayNonStreamingReadDoesNotCommitResponseBeforeError(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	resp := &http.Response{
		Body: io.NopCloser(bytes.NewBufferString(
			"data: {\"headers\":{\"Content-Type\":[\"application/json\"]},\"body\":\"{\\\"code\\\":\\\"101\\\",\\\"message\\\":\\\"Signature invalid\\\"}\",\"statusCodeValue\":403,\"statusCode\":\"FORBIDDEN\"}\n\n",
		)),
	}

	events, err := qoder.ReadQoderSSEEventsContext(context.Background(), resp)

	require.Error(t, err)
	require.Empty(t, events)
	require.Empty(t, rec.Body.String())
	require.Empty(t, rec.Header().Get("Cache-Control"))
	require.False(t, c.Writer.Written())
}

// qoderWrappedSSELineForTest encodes a JSON object as the string-valued body
// of a Qoder wrapper. Marshal failures abort the test; the returned data-space
// frame includes its blank separator for production parsing.
func qoderWrappedSSELineForTest(t *testing.T, inner map[string]any) string {
	t.Helper()
	body, err := json.Marshal(inner)
	require.NoError(t, err)
	wrapper, err := json.Marshal(map[string]string{"body": string(body)})
	require.NoError(t, err)
	return "data: " + string(wrapper) + "\n\n"
}

// qoderWrappedErrorSSELineForTest encodes a Qoder error wrapper with JSON body
// text and source statusCodeValue. Marshal failures abort the test; callers
// select the status independently of body error codes.
func qoderWrappedErrorSSELineForTest(t *testing.T, statusCode int, inner map[string]any) string {
	t.Helper()
	body, err := json.Marshal(inner)
	require.NoError(t, err)
	wrapper, err := json.Marshal(map[string]any{
		"body":            string(body),
		"statusCodeValue": statusCode,
	})
	require.NoError(t, err)
	return "data: " + string(wrapper) + "\n\n"
}

// qoderResponsesStreamEventsForTest returns JSON from supported Responses
// data-space fields. Empty frames, comment-prefixed frames and DONE are
// ignored; selected malformed JSON fails the test, and callers supply this
// fixture serialization.
func qoderResponsesStreamEventsForTest(t *testing.T, body string) []gjson.Result {
	t.Helper()
	// Keep comment-prefixed fixture frames outside event assertions, then
	// validate selected JSON fields so Responses identity checks compare actual
	// client event objects.
	var events []gjson.Result
	for frame := range strings.SplitSeq(body, "\n\n") {
		frame = strings.TrimSpace(frame)
		if frame == "" || strings.HasPrefix(frame, ":") {
			continue
		}
		for line := range strings.SplitSeq(frame, "\n") {
			line = strings.TrimSpace(line)
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			if data == "" || data == "[DONE]" {
				continue
			}
			require.True(t, gjson.Valid(data), "invalid Responses SSE JSON data: %s", data)
			events = append(events, gjson.Parse(data))
		}
	}
	return events
}

// qoderFixtureValue returns the supplied decoded fixture value as T. A failed
// type assertion aborts the test, so callers use it before inspecting concrete
// map, slice or scalar fixture values.
func qoderFixtureValue[T any](t *testing.T, raw any) T {
	t.Helper()
	value, ok := raw.(T)
	require.True(t, ok, "unexpected decoded fixture type: %T", raw)
	return value
}

// TestQoderThinkingFlowsThroughAllEndpoints varies CN/global models and
// reasoning input across Chat, Responses and Messages. Upstream thinking
// switches and permitted effort copies must agree with each route’s configured
// normalization.
func TestQoderThinkingFlowsThroughAllEndpoints(t *testing.T) {
	tests := []struct {
		name        string     // protocol or mapping case label
		site        qoder.Site // configured Qoder site identity
		endpoint    string     // fixture client protocol selector
		body        string     // encoded client request fixture
		wantEnabled bool       // expected upstream reasoning switch
		// wantEffortPath selects whether all four upstream effort copies exist.
		// When true, each equals wantEffort; when false, the helper checks absence
		// and ignores the empty expected value.
		wantEffort     string // expected normalized reasoning effort
		wantEffortPath bool   // expected explicit effort fields
	}{
		{
			name:           "cn deepseek chat completions",
			site:           qoder.SiteCN,
			endpoint:       "chat",
			body:           `{"model":"deepseek-v4-pro","reasoning_effort":"medium","messages":[{"role":"user","content":"hello"}],"stream":true}`,
			wantEnabled:    true,
			wantEffort:     "high",
			wantEffortPath: true,
		},
		{
			name:           "cn deepseek responses",
			site:           qoder.SiteCN,
			endpoint:       "responses",
			body:           `{"model":"deepseek-v4-pro","reasoning":{"effort":"high"},"input":"hello","stream":true}`,
			wantEnabled:    true,
			wantEffort:     "high",
			wantEffortPath: true,
		},
		{
			name:           "cn deepseek anthropic messages",
			site:           qoder.SiteCN,
			endpoint:       "messages",
			body:           `{"model":"deepseek-v4-pro","max_tokens":1024,"thinking":{"type":"enabled","budget_tokens":1},"messages":[{"role":"user","content":"hello"}],"stream":true}`,
			wantEnabled:    true,
			wantEffort:     "max",
			wantEffortPath: true,
		},
		{
			name:           "global deepseek chat completions",
			site:           qoder.SiteGlobal,
			endpoint:       "chat",
			body:           `{"model":"deepseek-v4-pro","reasoning_effort":"medium","messages":[{"role":"user","content":"hello"}],"stream":true}`,
			wantEnabled:    true,
			wantEffort:     "high",
			wantEffortPath: true,
		},
		{
			name:           "global deepseek responses",
			site:           qoder.SiteGlobal,
			endpoint:       "responses",
			body:           `{"model":"deepseek-v4-pro","reasoning":{"effort":"high"},"input":"hello","stream":true}`,
			wantEnabled:    true,
			wantEffort:     "high",
			wantEffortPath: true,
		},
		{
			name:           "global deepseek anthropic budget",
			site:           qoder.SiteGlobal,
			endpoint:       "messages",
			body:           `{"model":"deepseek-v4-pro","max_tokens":1024,"thinking":{"type":"enabled","budget_tokens":1},"messages":[{"role":"user","content":"hello"}],"stream":true}`,
			wantEnabled:    true,
			wantEffort:     "max",
			wantEffortPath: true,
		},
		{
			name:           "global qwen 38 chat completions public alias",
			site:           qoder.SiteGlobal,
			endpoint:       "chat",
			body:           `{"model":"qwen3.8-max","reasoning_effort":"low","messages":[{"role":"user","content":"hello"}],"stream":true}`,
			wantEnabled:    true,
			wantEffort:     "low",
			wantEffortPath: true,
		},
		{
			name:           "global qwen 38 responses raw route",
			site:           qoder.SiteGlobal,
			endpoint:       "responses",
			body:           `{"model":"qmodel_38max","reasoning":{"effort":"high"},"input":"hello","stream":true}`,
			wantEnabled:    true,
			wantEffort:     "xhigh",
			wantEffortPath: true,
		},
		{
			name:           "global qwen 38 anthropic messages",
			site:           qoder.SiteGlobal,
			endpoint:       "messages",
			body:           `{"model":"qwen3.8-max","max_tokens":1024,"thinking":{"type":"adaptive"},"messages":[{"role":"user","content":"hello"}],"stream":true}`,
			wantEnabled:    true,
			wantEffort:     "medium",
			wantEffortPath: true,
		},
		{
			name:           "cn qwen 38 chat completions raw route",
			site:           qoder.SiteCN,
			endpoint:       "chat",
			body:           `{"model":"qmodel_38max","reasoning_effort":"medium","messages":[{"role":"user","content":"hello"}],"stream":true}`,
			wantEnabled:    true,
			wantEffort:     "medium",
			wantEffortPath: true,
		},
		{
			name:           "cn qwen 38 responses public alias",
			site:           qoder.SiteCN,
			endpoint:       "responses",
			body:           `{"model":"qwen3.8-max","reasoning":{"effort":"max"},"input":"hello","stream":true}`,
			wantEnabled:    true,
			wantEffort:     "xhigh",
			wantEffortPath: true,
		},
		{
			name:           "cn qwen 38 anthropic messages",
			site:           qoder.SiteCN,
			endpoint:       "messages",
			body:           `{"model":"qwen3.8-max","max_tokens":1024,"thinking":{"type":"enabled","budget_tokens":1},"messages":[{"role":"user","content":"hello"}],"stream":true}`,
			wantEnabled:    true,
			wantEffort:     "xhigh",
			wantEffortPath: true,
		},
	}

	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			credentials := map[string]any{"site": string(tt.site)}
			provider := &providercore.Record{
				ID:          int64(920 + index),
				Name:        "qoder-" + string(tt.site),
				Platform:    capability.PlatformQoder,
				Type:        capability.ProviderTypeCosy,
				Credentials: credentials,
			}
			gatewaytestkit.SetQoderCatalog(provider)
			client := &gatewaytestkit.QoderClient{
				Body: "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
					"data: {\"body\":\"[DONE]\"}\n\n",
			}
			service := gatewaytestkit.NewQoderFixture(provideradapter.NewQoderTokenProvider(qoder.SessionBuilder{}),
				client, nil)

			service.Tokens.Core.Sessions = map[int64]providercore.QoderSessionCacheEntry[*qoder.SessionContext]{
				provider.ID: {
					CredentialsHash: providercore.QoderCredentialsHash(provider.Credentials),
					Session:         &qoder.SessionContext{Identity: &qoder.AuthIdentity{SecurityOauthToken: "token"}},
				},
			}

			var err error
			switch tt.endpoint {
			case "chat":
				_, err = forwardQoderAttempt(context.Background(), c, service.Runtime, provider, []byte(tt.body), protocolcore.ProtocolOpenAIChatCompletions, false)
			case "responses":
				_, err = forwardQoderAttempt(context.Background(), c, service.Runtime, provider, []byte(tt.body), protocolcore.ProtocolOpenAIResponses, false)
			case "messages":
				_, err = forwardQoderAttempt(context.Background(), c, service.Runtime, provider, []byte(tt.body), protocolcore.ProtocolAnthropicMessages, false)
			default:
				t.Fatalf("unexpected endpoint %q", tt.endpoint)
			}
			require.NoError(t, err)
			payload := qoderLastUpstreamPayloadForTest(t, client)
			assertQoderThinkingPayload(t, payload, tt.wantEnabled, tt.wantEffort, tt.wantEffortPath)
		})
	}
}

// TestQoderThinkingUsesProviderMappedRouteKey maps a custom client alias to a
// reasoning-capable provider route. Upstream model and thinking switches must
// follow that mapped route while unsupported effort fields stay absent.
func TestQoderThinkingUsesProviderMappedRouteKey(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	provider := &providercore.Record{
		ID:       901,
		Name:     "qoder-global",
		Platform: capability.PlatformQoder,
		Type:     capability.ProviderTypeCosy,
		Credentials: map[string]any{
			"site": "global",
			"model_mapping": map[string]any{
				"custom-qwen": "qmodel_38max",
			},
		},
	}
	gatewaytestkit.SetQoderCatalog(provider)
	client := &gatewaytestkit.QoderClient{
		Body: "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"OK\\\"}}]}\"}\n\n" +
			"data: {\"body\":\"[DONE]\"}\n\n",
	}
	service := gatewaytestkit.NewQoderFixture(provideradapter.NewQoderTokenProvider(qoder.SessionBuilder{}),
		client, nil)

	service.Tokens.Core.Sessions = map[int64]providercore.QoderSessionCacheEntry[*qoder.SessionContext]{
		provider.ID: {
			CredentialsHash: providercore.QoderCredentialsHash(provider.Credentials),
			Session:         &qoder.SessionContext{Identity: &qoder.AuthIdentity{SecurityOauthToken: "token"}},
		},
	}
	body := []byte(`{
		"model":"custom-qwen",
		"reasoning_effort":"medium",
		"messages":[{"role":"user","content":"hello"}],
		"stream":true
	}`)

	result, err := forwardQoderAttempt(context.Background(), c, service.Runtime, provider, body, protocolcore.ProtocolOpenAIChatCompletions, false)
	require.NoError(t, err)
	require.Equal(t, "qmodel_38max", result.UpstreamModel)
	payload := qoderLastUpstreamPayloadForTest(t, client)
	assertQoderThinkingPayload(t, payload, true, "medium", true)
}

// assertQoderThinkingPayload checks both reasoning switches and all four
// effort locations in a decoded payload. Callers choose whether effort is
// present; absent effort requires every copy to be missing, while present
// values must agree.
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

// TestMixedGroupQoderValidationRetainsClientError sends a stateful request through
// the mixed-platform native dispatcher. The error must be presented once as a
// local 400 and stay recognizable to scheduler feedback without invoking transport.
func TestMixedGroupQoderValidationRetainsClientError(t *testing.T) {
	row, fixture, client := gatewaytestkit.NewDefaultQoderFixture()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	executor := UnifiedTextExecutor{Qoder: fixture.Runtime}
	result, err := executor.native(t.Context(), c, gatewayprovider.NewExecutionProvider(row), []byte(`{"model":"ultimate","input":"test","store":true}`), protocolcore.ProtocolOpenAIResponses)
	require.Nil(t, result)
	require.True(t, routing.IsModelAttributeError(err))
	require.True(t, qoder.IsRequestError(err))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "unsupported_response_state")
	require.True(t, IsResponseCommitted(c))
	require.Zero(t, client.BodyCount())
}

// TestUnifiedTextExecutor_QoderIRConversionRoutingAndFailover exercises
// UnifiedTextExecutor across Messages, Responses
// (including legacy messages ingress), Chat, and Cursor input-shaped Chat. It
// verifies local 400 capability rejections without health penalties, pre-output
// UpstreamFailoverError promotion on switchable errors, and untimed terminal
// SSE error emission on already-committed event streams.
func TestUnifiedTextExecutor_QoderIRConversionRoutingAndFailover(t *testing.T) {
	newIRRuntime := func(customClient qoder.StreamClient) (*providercore.Record, *gatewaytestkit.QoderFixture, *gatewaytestkit.QoderClient, *qoderIRHealthRecorder) {
		row, fixture, client := gatewaytestkit.NewDefaultQoderFixture()
		health := &qoderIRHealthRecorder{}
		streamClient := qoder.StreamClient(client)
		if customClient != nil {
			streamClient = customClient
		}
		fixture.Runtime = gatewayprovider.NewQoderRuntime(gatewayprovider.QoderRuntimeOptions{
			Tokens:        fixture.Tokens,
			Client:        streamClient,
			Conversations: qoder.NewQoderConversationStore(qoder.QoderConversationTTL),
			Health:        health,
		})
		return row, fixture, client, health
	}

	t.Run("routes Messages, legacy Responses messages, Chat, and Cursor input Chat through IR conversion", func(t *testing.T) {
		cases := []struct {
			name         string
			path         string
			source       protocolcore.ProtocolID
			body         string
			wantContains string
		}{
			{
				name:         "anthropic messages stream",
				path:         "/v1/messages",
				source:       protocolcore.ProtocolAnthropicMessages,
				body:         `{"model":"claude-opus-4-6","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`,
				wantContains: `"type":"message_start"`,
			},
			{
				name:         "responses legacy messages ingress buffered",
				path:         "/v1/responses",
				source:       protocolcore.ProtocolOpenAIResponses,
				body:         `{"model":"ultimate","stream":false,"messages":[{"role":"user","content":"hi legacy"}]}`,
				wantContains: `"object":"response"`,
			},
			{
				name:         "chat completions buffered",
				path:         "/v1/chat/completions",
				source:       protocolcore.ProtocolOpenAIChatCompletions,
				body:         `{"model":"auto","stream":false,"messages":[{"role":"user","content":"hi chat"}]}`,
				wantContains: `"object":"chat.completion"`,
			},
			{
				name:         "cursor input chat stream",
				path:         "/v1/chat/completions",
				source:       protocolcore.ProtocolOpenAIChatCompletions,
				body:         `{"model":"auto","stream":true,"input":[{"type":"message","role":"user","content":"hi cursor"}]}`,
				wantContains: `"object":"chat.completion.chunk"`,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				row, fixture, client, _ := newIRRuntime(nil)
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, tc.path, bytes.NewReader([]byte(tc.body)))

				executor := UnifiedTextExecutor{
					Qoder: fixture.Runtime,
				}
				res, err := executor.native(t.Context(), c, gatewayprovider.NewExecutionProvider(row), []byte(tc.body), tc.source)
				require.NoError(t, err)
				require.NotNil(t, res)
				require.Equal(t, http.StatusOK, rec.Code)
				require.Contains(t, rec.Body.String(), tc.wantContains)
				require.Equal(t, 1, client.BodyCount())
			})
		}
	})

	t.Run("local RequestError returns HTTP 400 MODEL_CAPABILITY_UNSUPPORTED without upstream dispatch or health penalty", func(t *testing.T) {
		row, fixture, client, health := newIRRuntime(nil)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

		executor := UnifiedTextExecutor{
			Qoder: fixture.Runtime,
		}
		res, err := executor.native(t.Context(), c, gatewayprovider.NewExecutionProvider(row), []byte(`{"model":"ultimate","input":"test","store":true}`), protocolcore.ProtocolOpenAIResponses)
		require.Nil(t, resultOrNilForQoderIRTest(res))
		require.True(t, routing.IsModelAttributeError(err))
		require.True(t, qoder.IsRequestError(err))
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "unsupported_response_state")
		require.Zero(t, client.BodyCount())
		require.Zero(t, health.totalCalls())
		_, hasStatus := c.Get(OpsUpstreamStatusCodeKey)
		_, hasMessage := c.Get(OpsUpstreamErrorMessageKey)
		require.False(t, hasStatus)
		require.False(t, hasMessage)
	})

	t.Run("pre-semantic switchable errors promote to UpstreamFailoverError without committing downstream bytes", func(t *testing.T) {
		for _, tc := range []struct {
			name           string
			sseLine        string
			wantClientCode int
		}{
			{
				name:           "agent limit 115 maps to 429 client status",
				sseLine:        qoderWrappedErrorSSELineForTest(t, 502, map[string]any{"code": "115", "message": "Agent limit reached"}),
				wantClientCode: http.StatusTooManyRequests,
			},
			{
				name:           "entitlement denied 112 maps to 403 client status",
				sseLine:        qoderWrappedErrorSSELineForTest(t, 403, map[string]any{"code": "112", "message": "Model entitlement denied"}),
				wantClientCode: http.StatusForbidden,
			},
			{
				name:           "upstream 502 maps to 502 client status",
				sseLine:        qoderWrappedErrorSSELineForTest(t, 502, map[string]any{"code": "500", "message": "Gateway error"}),
				wantClientCode: http.StatusBadGateway,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				row, fixture, client, _ := newIRRuntime(nil)
				client.Body = tc.sseLine
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

				executor := UnifiedTextExecutor{
					Qoder: fixture.Runtime,
				}
				res, err := executor.native(t.Context(), c, gatewayprovider.NewExecutionProvider(row), []byte(`{"model":"ultimate","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`), protocolcore.ProtocolAnthropicMessages)
				require.Nil(t, res)
				var failoverErr *forwardcore.UpstreamFailoverError
				require.ErrorAs(t, err, &failoverErr)
				require.Equal(t, capability.PlatformQoder, failoverErr.Platform)
				require.Equal(t, tc.wantClientCode, failoverErr.ClientStatusCode)
				require.Empty(t, rec.Body.String())
				require.False(t, c.Writer.Written())
			})
		}
	})

	t.Run("keepalive-committed SSE streams receive untimed protocol terminal error on local or non-switchable failures", func(t *testing.T) {
		for _, tc := range []struct {
			name         string
			path         string
			source       protocolcore.ProtocolID
			body         string
			sseBody      string
			wantText     string
			wantLocal400 bool
		}{
			{
				name:         "responses local rejection on committed SSE emits response.failed",
				path:         "/v1/responses",
				source:       protocolcore.ProtocolOpenAIResponses,
				body:         `{"model":"ultimate","stream":true,"input":"hi","store":true}`,
				wantText:     `"type":"response.failed"`,
				wantLocal400: true,
			},
			{
				name:         "messages local rejection on committed SSE emits anthropic error event and marks response committed",
				path:         "/v1/messages",
				source:       protocolcore.ProtocolAnthropicMessages,
				body:         `{"model":"ultimate","max_tokens":64,"stream":true,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`,
				wantText:     `"type":"error"`,
				wantLocal400: true,
			},
			{
				name:     "messages non-switchable 401 on committed SSE emits anthropic error event",
				path:     "/v1/messages",
				source:   protocolcore.ProtocolAnthropicMessages,
				body:     `{"model":"ultimate","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`,
				sseBody:  qoderWrappedErrorSSELineForTest(t, 401, map[string]any{"code": "101", "message": "Invalid signature"}),
				wantText: `"type":"error"`,
			},
			{
				name:     "chat non-switchable 401 on committed SSE emits generic error event",
				path:     "/v1/chat/completions",
				source:   protocolcore.ProtocolOpenAIChatCompletions,
				body:     `{"model":"auto","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
				sseBody:  qoderWrappedErrorSSELineForTest(t, 401, map[string]any{"code": "101", "message": "Invalid signature"}),
				wantText: "event: error",
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				row, fixture, client, health := newIRRuntime(nil)
				if tc.sseBody != "" {
					client.Body = tc.sseBody
				}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
				c.Writer.Header().Set("Content-Type", "text/event-stream")
				c.Writer.WriteHeader(http.StatusOK)
				c.Writer.WriteHeaderNow()

				executor := UnifiedTextExecutor{
					Qoder: fixture.Runtime,
				}
				res, err := executor.native(t.Context(), c, gatewayprovider.NewExecutionProvider(row), []byte(tc.body), tc.source)
				require.Nil(t, res)
				require.Error(t, err)
				require.Equal(t, http.StatusOK, rec.Code)
				require.Contains(t, rec.Body.String(), tc.wantText)
				require.True(t, TerminalSSEDelivered(c))
				require.True(t, IsResponseCommitted(c), "committed SSE terminal error must mark response committed so outer OtherFailure skips duplicate fallback and schedule penalty")
				if tc.wantLocal400 {
					require.True(t, routing.IsModelAttributeError(err))
					require.True(t, qoder.IsRequestError(err))
					require.Zero(t, client.BodyCount())
					require.Zero(t, health.totalCalls())
					_, hasStatus := c.Get(OpsUpstreamStatusCodeKey)
					require.False(t, hasStatus)
				}
			})
		}
	})
}

// SetRateLimited increments r.rateLimited under r.mu when health observation records a cooldown.
func (r *qoderIRHealthRecorder) SetRateLimited(context.Context, int64, time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rateLimited++
	return nil
}

// SetOverloaded increments r.overloaded under r.mu when health observation records provider overload.
func (r *qoderIRHealthRecorder) SetOverloaded(context.Context, int64, time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.overloaded++
	return nil
}

// totalCalls returns the sum of rateLimited and overloaded calls recorded so far.
func (r *qoderIRHealthRecorder) totalCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rateLimited + r.overloaded
}

// resultOrNilForQoderIRTest returns res directly so nil assertions read clearly.
func resultOrNilForQoderIRTest(res *forwardcore.OpenAIResult) *forwardcore.OpenAIResult {
	return res
}
