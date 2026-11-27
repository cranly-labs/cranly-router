package qoder

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	protocolqoder "github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/provider/qodermodel"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// payload_converted_test.go verifies IR-backed Qoder request conversion,
// billing-header cch fingerprint normalization, Cursor input-shaped Chat
// routing, and local RequestError capability gating before upstream dispatch.
// Standalone Executor probes also protect default billing-header normalization
// and description-less tool fingerprint stability when Dialect is omitted.

// qoderCaptureClientFunc adapts a function into a StreamClient for converted
// request and response unit tests.
type qoderCaptureClientFunc func(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error)

// TestPayloadRequestFromIR_PreservesNormalizedSystemAndCapabilities feeds a
// decoded protocol/qoder PayloadRequest containing volatile billing checksums,
// cache breakpoints, 64-bit JSON Schema bounds, and allowed_tools into
// PayloadRequestFromIR. It verifies stable cch normalization, discrete user
// blocks, scalar assistant/tool content, and exact schema preservation.
func TestPayloadRequestFromIR_PreservesNormalizedSystemAndCapabilities(t *testing.T) {
	rawA := []byte(`{
		"model":"ultimate",
		"system":"x-anthropic-billing-header: cc_version=2.1.0; cc_entrypoint=cli; cch=1a2b3;\nYou are helpful.",
		"max_tokens":512,
		"thinking":{"specified":true,"enabled":true,"effort":"high"},
		"session_id":"sess-explicit",
		"cache_key":"cache-1",
		"metadata_user_id":"user-42",
		"messages":[
			{
				"role":"user",
				"text":"first\nsecond",
				"text_blocks":[
					{"type":"text","text":"first"},
					{"type":"text","text":"second","cache_control":{"type":"ephemeral"}}
				]
			},
			{
				"role":"assistant",
				"text":"running bash",
				"text_blocks":[{"type":"text","text":"running bash"}],
				"tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{\"cmd\":\"pwd\"}"}}]
			},
			{
				"role":"tool",
				"text":"/workspace",
				"text_blocks":[{"type":"text","text":"/workspace"}],
				"tool_call_id":"call_1"
			}
		],
		"tools":[
			{"type":"function","function":{"name":"unused_tool","description":"Unused","parameters":{"type":"object"}}},
			{"type":"function","function":{"name":"bash","description":"Run shell","parameters":{"type":"object","properties":{"max":{"type":"integer","maximum":9007199254740993}}}}}
		],
		"tool_choice":{"mode":"required","allowed_tools":["bash"]}
	}`)
	rawB := []byte(strings.ReplaceAll(string(rawA), "cch=1a2b3;", "cch=9f8e7;"))

	protoA, err := protocolqoder.DecodePayload(rawA)
	require.NoError(t, err)
	protoB, err := protocolqoder.DecodePayload(rawB)
	require.NoError(t, err)

	reqA, err := PayloadRequestFromIR(protoA)
	require.NoError(t, err)
	reqB, err := PayloadRequestFromIR(protoB)
	require.NoError(t, err)

	require.Contains(t, reqA.System, "cch=<cch>;")
	require.Equal(t, reqA.System, reqB.System)
	require.Equal(t, QoderSystemFingerprint(reqA.System), QoderSystemFingerprint(reqB.System))
	require.Equal(t, "sess-explicit", reqA.ExplicitSession)
	require.Equal(t, "cache-1", reqA.PromptCacheKey)
	require.Equal(t, "user-42", reqA.MetadataUserID)
	require.Equal(t, 512, reqA.MaxTokens)
	require.Equal(t, QoderThinkingDirective{Specified: true, Enabled: true, Effort: "high"}, reqA.Thinking)
	require.Equal(t, "required", reqA.ToolChoice)

	require.Len(t, reqA.Tools, 1)
	toolsJSON, err := json.Marshal(reqA.Tools)
	require.NoError(t, err)
	require.Contains(t, string(toolsJSON), "9007199254740993")
	require.NotContains(t, string(toolsJSON), "unused_tool")

	require.Len(t, reqA.Messages, 3)
	require.Len(t, reqA.Messages[0].TextContent, 2)
	require.Empty(t, reqA.Messages[1].TextContent)
	require.Empty(t, reqA.Messages[2].TextContent)
	require.Equal(t, "bash", reqA.Messages[2].Raw["name"])

	assistantWire := QoderPayloadMessageFromMessage(reqA.Messages[1])
	require.Equal(t, "running bash", assistantWire["content"])
	toolWire := QoderPayloadMessageFromMessage(reqA.Messages[2])
	require.Equal(t, "/workspace", toolWire["content"])
}

// TestExecutor_ConvertedRequestGatingAndStatelessRejections exercises request
// decoding, Cursor input-shaped Chat routing, stateless Responses checks, and
// capability gating. Local rejections must return *RequestError with
// FailureClass="invalid_request" before touching credentials or upstream network clients.
func TestExecutor_ConvertedRequestGatingAndStatelessRejections(t *testing.T) {
	t.Run("cursor input chat request converts and executes", func(t *testing.T) {
		var capturedPayload map[string]any
		executor := NewExecutor(ExecuteOptions{})

		target := &Target{
			ProviderID: 7,
			Site:       SiteGlobal,
			Session:    func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
			Client: func() (StreamClient, error) {
				return qoderCaptureClientFunc(func(_ context.Context, _ *SessionContext, _ string, body []byte, _ map[string]string) (*http.Response, error) {
					require.NoError(t, json.Unmarshal(body, &capturedPayload))
					return modelTestClient{}.StreamRequestContext(context.Background(), nil, "", nil, nil)
				}), nil
			},
		}
		sink := &modelTestSink{}
		cursorBody := []byte(`{
			"model":"auto",
			"stream":true,
			"stream_options":{"include_usage":true},
			"input":[
				{"type":"message","role":"system","content":"System prompt"},
				{"type":"message","role":"user","content":[{"type":"input_text","text":"Hello from Cursor"}]}
			]
		}`)
		result, err := executor.Execute(context.Background(), upstream.AttemptInput{
			Protocol:      protocol.ProtocolOpenAIChatCompletions,
			Target:        target,
			Body:          cursorBody,
			ResponseModel: "cursor-alias",
			Stream:        true,
		}, sink)
		require.NoError(t, err)
		require.True(t, result.Served)
		require.Contains(t, sink.String(), `"object":"chat.completion.chunk"`)
		require.Contains(t, sink.String(), `"model":"cursor-alias"`)
		require.Contains(t, sink.String(), `"prompt_tokens":1`)
		require.NotNil(t, capturedPayload)
		chatCtx, _ := capturedPayload["chat_context"].(map[string]any)
		require.Equal(t, "Hello from Cursor", chatCtx["text"])
	})

	t.Run("local rejections never acquire session or dispatch upstream", func(t *testing.T) {
		sessionCalls := 0
		clientCalls := 0
		disableTools := false
		mandatoryThinkingModel := qodermodel.Model{
			Key:             "ultimate",
			Alias:           "ultimate",
			Enabled:         true,
			Reasoning:       new(true),
			CanDisable:      new(false),
			DefaultThinking: true,
			DefaultEffort:   "high",
			Efforts:         []string{"low", "medium", "high"},
		}
		catalog := qodermodel.NewCatalog("v1", []qodermodel.Model{mandatoryThinkingModel})

		cases := []struct {
			name     string
			proto    protocol.ProtocolID
			body     string
			attrs    func(context.Context, string, protocol.ProtocolID) (modelrules.Resolution, error)
			wantCode string
		}{
			{
				name:     "malformed json",
				proto:    protocol.ProtocolOpenAIChatCompletions,
				body:     `{"model":"auto",`,
				wantCode: "invalid_request",
			},
			{
				name:     "responses item_reference rejected",
				proto:    protocol.ProtocolOpenAIResponses,
				body:     `{"model":"auto","input":[{"type":"item_reference","id":"msg_1"}]}`,
				wantCode: "unsupported_response_state",
			},
			{
				name:     "responses unpaired function_call_output rejected",
				proto:    protocol.ProtocolOpenAIResponses,
				body:     `{"model":"auto","input":[{"type":"function_call_output","call_id":"call_missing","output":"ok"}]}`,
				wantCode: "missing_tool_call",
			},
			{
				name:  "mandatory reasoning model rejects explicit disable",
				proto: protocol.ProtocolAnthropicMessages,
				body:  `{"model":"ultimate","max_tokens":64,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`,
				attrs: func(_ context.Context, model string, wire protocol.ProtocolID) (modelrules.Resolution, error) {
					route := modelrules.Target{
						Model:          model,
						ClientProtocol: string(wire),
						ProviderModels: catalog,
					}
					return modelrules.View{}.Resolve(route, modelcatalog.Attributes{}), nil
				},
				wantCode: "invalid_reasoning_parameter",
			},
			{
				name:  "disabled tool capability rejects required tool_choice",
				proto: protocol.ProtocolOpenAIChatCompletions,
				body:  `{"model":"auto","messages":[{"role":"user","content":"hi"}],"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],"tool_choice":"required"}`,
				attrs: func(context.Context, string, protocol.ProtocolID) (modelrules.Resolution, error) {
					return modelrules.Resolution{
						Attributes: modelcatalog.Attributes{ToolCall: &disableTools},
					}, nil
				},
				wantCode: "unsupported_tool_choice",
			},
			{
				name:  "disabled tool capability rejects named function tool_choice",
				proto: protocol.ProtocolAnthropicMessages,
				body:  `{"model":"auto","max_tokens":64,"messages":[{"role":"user","content":"hi"}],"tools":[{"name":"bash","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"bash"}}`,
				attrs: func(context.Context, string, protocol.ProtocolID) (modelrules.Resolution, error) {
					return modelrules.Resolution{
						Attributes: modelcatalog.Attributes{ToolCall: &disableTools},
					}, nil
				},
				wantCode: "unsupported_tool_choice",
			},
		}

		executor := NewExecutor(ExecuteOptions{})
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				target := &Target{
					ProviderID: 1,
					Site:       SiteGlobal,
					Attributes: tc.attrs,
					Session: func(context.Context) (*SessionContext, error) {
						sessionCalls++
						return &SessionContext{}, nil
					},
					Client: func() (StreamClient, error) {
						clientCalls++
						return modelTestClient{}, nil
					},
				}
				res, execErr := executor.Execute(context.Background(), upstream.AttemptInput{
					Protocol: tc.proto,
					Target:   target,
					Body:     []byte(tc.body),
					Stream:   true,
				}, &modelTestSink{})
				var reqErr *RequestError
				require.ErrorAs(t, execErr, &reqErr)
				require.Equal(t, tc.wantCode, reqErr.Code)
				require.Equal(t, "invalid_request", res.FailureClass)
				require.False(t, res.Served)
				require.Zero(t, sessionCalls)
				require.Zero(t, clientCalls)
			})
		}
	})

	t.Run("disabled tool capability prunes optional tools while keeping history", func(t *testing.T) {
		disableTools := false
		var sentPayload map[string]any
		executor := NewExecutor(ExecuteOptions{})
		target := &Target{
			ProviderID: 2,
			Site:       SiteGlobal,
			Attributes: func(context.Context, string, protocol.ProtocolID) (modelrules.Resolution, error) {
				return modelrules.Resolution{
					Attributes: modelcatalog.Attributes{ToolCall: &disableTools},
				}, nil
			},
			Session: func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
			Client: func() (StreamClient, error) {
				return qoderCaptureClientFunc(func(_ context.Context, _ *SessionContext, _ string, body []byte, _ map[string]string) (*http.Response, error) {
					require.NoError(t, json.Unmarshal(body, &sentPayload))
					return modelTestClient{}.StreamRequestContext(context.Background(), nil, "", nil, nil)
				}), nil
			},
		}
		body := []byte(`{
			"model":"auto",
			"tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object"}}}],
			"tool_choice":"auto",
			"messages":[
				{"role":"user","content":"run pwd"},
				{"role":"assistant","content":"","tool_calls":[{"id":"call_1","type":"function","function":{"name":"bash","arguments":"{}"}}]},
				{"role":"tool","tool_call_id":"call_1","content":"/tmp"}
			]
		}`)
		res, err := executor.Execute(context.Background(), upstream.AttemptInput{
			Protocol: protocol.ProtocolOpenAIChatCompletions,
			Target:   target,
			Body:     body,
			Stream:   false,
		}, &modelTestSink{})
		require.NoError(t, err)
		require.True(t, res.Served)
		require.Empty(t, QoderAnySlice(sentPayload["tools"]))
		parameters, _ := sentPayload["parameters"].(map[string]any)
		require.NotContains(t, parameters, "tool_choice")
		require.Len(t, QoderAnySlice(sentPayload["messages"]), 3)
	})

	t.Run("standalone executor without Target.Dialect normalizes billing header and omits empty tool description", func(t *testing.T) {
		var sentPayload map[string]any
		executor := NewExecutor(ExecuteOptions{})
		target := &Target{
			ProviderID: 9,
			Site:       SiteGlobal,
			Session:    func(context.Context) (*SessionContext, error) { return &SessionContext{}, nil },
			Client: func() (StreamClient, error) {
				return qoderCaptureClientFunc(func(_ context.Context, _ *SessionContext, _ string, body []byte, _ map[string]string) (*http.Response, error) {
					require.NoError(t, json.Unmarshal(body, &sentPayload))
					return modelTestClient{}.StreamRequestContext(context.Background(), nil, "", nil, nil)
				}), nil
			},
		}
		msgBody := []byte(`{
			"model":"auto",
			"max_tokens":128,
			"system":[
				{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.330; cc_entrypoint=cli; cch=4d5e6;"},
				{"type":"text","text":"Follow repo instructions."}
			],
			"tools":[{"name":"pwd_tool","input_schema":{"type":"object","properties":{}}}],
			"messages":[{"role":"user","content":"run pwd_tool"}]
		}`)
		res, err := executor.Execute(context.Background(), upstream.AttemptInput{
			Protocol: protocol.ProtocolAnthropicMessages,
			Target:   target,
			Body:     msgBody,
			Stream:   false,
		}, &modelTestSink{})
		require.NoError(t, err)
		require.True(t, res.Served)

		sentMessages := QoderAnySlice(sentPayload["messages"])
		require.NotEmpty(t, sentMessages)
		sysMsg, _ := sentMessages[0].(map[string]any)
		require.Equal(t, "system", sysMsg["role"])
		systemPrompt, _ := sysMsg["content"].(string)
		require.Equal(t, "x-anthropic-billing-header: cc_version=2.1.330; cc_entrypoint=cli; cch=<cch>;\nFollow repo instructions.", systemPrompt)
		tools := QoderAnySlice(sentPayload["tools"])
		require.Len(t, tools, 1)
		toolMap, _ := tools[0].(map[string]any)
		fnMap, _ := toolMap["function"].(map[string]any)
		require.Equal(t, "pwd_tool", fnMap["name"])
		require.NotContains(t, fnMap, "description")
	})
}

// StreamRequestContext invokes f directly with the supplied Qoder inference parameters.
func (f qoderCaptureClientFunc) StreamRequestContext(ctx context.Context, s *SessionContext, path string, body []byte, headers map[string]string) (*http.Response, error) {
	return f(ctx, s, path, body, headers)
}
