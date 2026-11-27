package mockupstream

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestProtocolConstantsMatchTestkit checks that package-private protocol ID
// strings resolve to identical canonical protocol IDs in testkit. Keeping both
// sets aligned prevents Endpoint.Protocol from drifting away from validator keys.
func TestProtocolConstantsMatchTestkit(t *testing.T) {
	for _, raw := range []string{
		protocolAnthropicMessages,
		protocolOpenAIResponses,
		protocolOpenAIChatCompletions,
		protocolGeminiGenerateContent,
	} {
		parsed, ok := testkit.ParseProtocolDirName(raw)
		require.True(t, ok, "protocol %q must be recognized by testkit", raw)
		require.Equal(t, raw, string(parsed))
	}
}

// TestIdentifyEndpoint exercises upstream URL path classification across
// standard, prefixed, Vertex, and query-streamed endpoints alongside rejected
// token-counting and malformed paths. Generation routes must resolve their
// protocol and Gemini model/stream metadata while non-generation routes return false.
func TestIdentifyEndpoint(t *testing.T) {
	cases := []struct {
		name       string // subtest scenario name
		path       string // incoming request URL path
		query      string // incoming raw query string
		wantOK     bool   // expected endpoint recognition result
		wantProto  string // expected canonical protocol string
		wantModel  string // expected Gemini path model
		wantStream bool   // expected path stream flag
	}{
		{
			name:      "anthropic standard",
			path:      "/v1/messages",
			query:     "beta=true",
			wantOK:    true,
			wantProto: protocolAnthropicMessages,
		},
		{
			name:      "anthropic prefixed base",
			path:      "/anthropic/v1/messages/",
			wantOK:    true,
			wantProto: protocolAnthropicMessages,
		},
		{
			name:      "responses standard",
			path:      "/v1/responses",
			wantOK:    true,
			wantProto: protocolOpenAIResponses,
		},
		{
			name:      "responses codex alias",
			path:      "/backend-api/codex/responses",
			wantOK:    true,
			wantProto: protocolOpenAIResponses,
		},
		{
			name:      "chat completions",
			path:      "/v1/chat/completions",
			wantOK:    true,
			wantProto: protocolOpenAIChatCompletions,
		},
		{
			name:       "gemini buffered",
			path:       "/v1beta/models/gemini-2.5-flash:generateContent",
			wantOK:     true,
			wantProto:  protocolGeminiGenerateContent,
			wantModel:  "gemini-2.5-flash",
			wantStream: false,
		},
		{
			name:       "gemini alt sse on generateContent",
			path:       "/v1beta/models/gemini-2.5-flash:generateContent",
			query:      "alt=sse",
			wantOK:     true,
			wantProto:  protocolGeminiGenerateContent,
			wantModel:  "gemini-2.5-flash",
			wantStream: true,
		},
		{
			name:       "gemini streamGenerateContent",
			path:       "/v1beta/models/gemini-2.5-pro:streamGenerateContent",
			wantOK:     true,
			wantProto:  protocolGeminiGenerateContent,
			wantModel:  "gemini-2.5-pro",
			wantStream: true,
		},
		{
			name:       "vertex gemini stream path",
			path:       "/v1/projects/demo/locations/us-central1/publishers/google/models/gemini-2.5-flash:streamGenerateContent",
			wantOK:     true,
			wantProto:  protocolGeminiGenerateContent,
			wantModel:  "gemini-2.5-flash",
			wantStream: true,
		},
		{
			name:   "reject messages count_tokens",
			path:   "/v1/messages/count_tokens",
			wantOK: false,
		},
		{
			name:   "reject responses input_tokens",
			path:   "/v1/responses/input_tokens",
			wantOK: false,
		},
		{
			name:   "reject gemini countTokens",
			path:   "/v1beta/models/gemini-2.5-flash:countTokens",
			wantOK: false,
		},
		{
			name:   "reject empty gemini model",
			path:   "/v1beta/models/:generateContent",
			wantOK: false,
		},
		{
			name:   "reject nested slash in gemini model",
			path:   "/v1beta/models/nested/model:generateContent",
			wantOK: false,
		},
		{
			name:   "reject models list path",
			path:   "/v1/models",
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ep, ok := IdentifyEndpoint(tc.path, tc.query)
			require.Equal(t, tc.wantOK, ok)
			if !tc.wantOK {
				return
			}
			require.Equal(t, tc.wantProto, ep.Protocol)
			require.Equal(t, tc.wantModel, ep.PathModel)
			require.Equal(t, tc.wantStream, ep.StreamFromPath)
		})
	}
}

// TestInspectRequestAcrossProtocols feeds equivalent first-turn and second-turn
// parallel-tool payloads across Messages, Responses, Chat, and Gemini. All four
// protocols must report matching AssistantMessages, ToolResults, Model, Stream,
// and HasTools values so protocol-independent Script matchers behave identically.
func TestInspectRequestAcrossProtocols(t *testing.T) {
	t.Run("turn 1 simple text without tools", func(t *testing.T) {
		msgSummary, err := InspectRequest(
			Endpoint{Protocol: protocolAnthropicMessages},
			[]byte(`{"model":"claude-sonnet-4-5","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`),
		)
		require.NoError(t, err)
		require.Equal(t, RequestSummary{Model: "claude-sonnet-4-5", Stream: false, HasTools: false, AssistantMessages: 0, ToolResults: 0}, msgSummary)

		rspSummary, err := InspectRequest(
			Endpoint{Protocol: protocolOpenAIResponses},
			[]byte(`{"model":"gpt-5.4","input":"hello","stream":true}`),
		)
		require.NoError(t, err)
		require.Equal(t, RequestSummary{Model: "gpt-5.4", Stream: true, HasTools: false, AssistantMessages: 0, ToolResults: 0}, rspSummary)

		chatSummary, err := InspectRequest(
			Endpoint{Protocol: protocolOpenAIChatCompletions},
			[]byte(`{"model":"gpt-5.4","stream":true,"messages":[{"role":"system","content":"sys"},{"role":"user","content":"hello"}]}`),
		)
		require.NoError(t, err)
		require.Equal(t, RequestSummary{Model: "gpt-5.4", Stream: true, HasTools: false, AssistantMessages: 0, ToolResults: 0}, chatSummary)

		gemSummary, err := InspectRequest(
			Endpoint{Protocol: protocolGeminiGenerateContent, PathModel: "gemini-2.5-flash", StreamFromPath: false},
			[]byte(`{"tools":[{"functionDeclarations":[]}],"contents":[{"role":"user","parts":[{"text":"hello"}]}]}`),
		)
		require.NoError(t, err)
		require.Equal(t, RequestSummary{Model: "gemini-2.5-flash", Stream: false, HasTools: false, AssistantMessages: 0, ToolResults: 0}, gemSummary)
	})

	t.Run("turn 2 reasoning plus two parallel tool calls and results", func(t *testing.T) {
		msgBody := []byte(`{
			"model":"claude-sonnet-4-5",
			"max_tokens":256,
			"stream":true,
			"tools":[{"name":"read_file","input_schema":{"type":"object"}}],
			"messages":[
				{"role":"user","content":"check both files"},
				{"role":"assistant","content":[
					{"type":"thinking","thinking":"plan","signature":"sig_1"},
					{"type":"tool_use","id":"call_1","name":"read_file","input":{"path":"a"}},
					{"type":"tool_use","id":"call_2","name":"read_file","input":{"path":"b"}}
				]},
				{"role":"user","content":[
					{"type":"tool_result","tool_use_id":"call_1","content":"ok-a"},
					{"type":"tool_result","tool_use_id":"call_2","content":"ok-b"},
					{"type":"text","text":"now summarize"}
				]}
			]
		}`)
		msgSummary, err := InspectRequest(Endpoint{Protocol: protocolAnthropicMessages}, msgBody)
		require.NoError(t, err)
		require.Equal(t, RequestSummary{Model: "claude-sonnet-4-5", Stream: true, HasTools: true, AssistantMessages: 1, ToolResults: 2}, msgSummary)

		rspBody := []byte(`{
			"model":"gpt-5.4",
			"stream":true,
			"input":[
				{"type":"additional_tools","tools":[{"type":"function","name":"read_file"}]},
				{"role":"user","content":"check both files"},
				{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"plan"}]},
				{"type":"function_call","call_id":"call_1","name":"read_file","arguments":"{\"path\":\"a\"}"},
				{"type":"custom_tool_call","call_id":"call_2","name":"apply_patch","input":"***"},
				{"type":"function_call_output","call_id":"call_1","output":"ok-a"},
				{"type":"custom_tool_call_output","call_id":"call_2","output":"ok-b"}
			]
		}`)
		rspSummary, err := InspectRequest(Endpoint{Protocol: protocolOpenAIResponses}, rspBody)
		require.NoError(t, err)
		require.Equal(t, RequestSummary{Model: "gpt-5.4", Stream: true, HasTools: true, AssistantMessages: 1, ToolResults: 2}, rspSummary)

		chatBody := []byte(`{
			"model":"deepseek-reasoner",
			"stream":true,
			"functions":[{"name":"read_file"}],
			"messages":[
				{"role":"user","content":"check both files"},
				{"role":"assistant","reasoning_content":"plan","tool_calls":[
					{"id":"call_1","type":"function","function":{"name":"read_file","arguments":"{}"}},
					{"id":"call_2","type":"function","function":{"name":"read_file","arguments":"{}"}}
				]},
				{"role":"tool","tool_call_id":"call_1","content":"ok-a"},
				{"role":"function","name":"read_file","content":"ok-b"}
			]
		}`)
		chatSummary, err := InspectRequest(Endpoint{Protocol: protocolOpenAIChatCompletions}, chatBody)
		require.NoError(t, err)
		require.Equal(t, RequestSummary{Model: "deepseek-reasoner", Stream: true, HasTools: true, AssistantMessages: 1, ToolResults: 2}, chatSummary)

		gemBody := []byte(`{
			"tools":[{"googleSearch":{}}],
			"contents":[
				{"role":"user","parts":[{"text":"check both files"}]},
				{"role":"model","parts":[
					{"text":"plan","thought":true,"thoughtSignature":"sig_1"},
					{"functionCall":{"name":"read_file","args":{"path":"a"}},"thoughtSignature":"sig_2"},
					{"functionCall":{"name":"read_file","args":{"path":"b"}}}
				]},
				{"role":"user","parts":[
					{"functionResponse":{"name":"read_file","response":{"result":"ok-a"}}},
					{"functionResponse":{"name":"read_file","response":{"result":"ok-b"}}}
				]}
			]
		}`)
		gemSummary, err := InspectRequest(
			Endpoint{Protocol: protocolGeminiGenerateContent, PathModel: "gemini-2.5-pro", StreamFromPath: true},
			gemBody,
		)
		require.NoError(t, err)
		require.Equal(t, RequestSummary{Model: "gemini-2.5-pro", Stream: true, HasTools: true, AssistantMessages: 1, ToolResults: 2}, gemSummary)
	})
}

// TestInspectRequestToleratesInvalidUTF8AndRejectsDuplicateKeys feeds raw
// invalid UTF-8 string bytes, duplicate JSON object keys, and unknown protocol
// names into InspectRequest. Invalid UTF-8 strings must succeed while duplicate
// keys and unrecognized protocols return descriptive errors.
func TestInspectRequestToleratesInvalidUTF8AndRejectsDuplicateKeys(t *testing.T) {
	invalidUTF8Body := append([]byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"bad-`), 0xff, '"', '}', ']', '}')
	summary, err := InspectRequest(Endpoint{Protocol: protocolOpenAIChatCompletions}, invalidUTF8Body)
	require.NoError(t, err)
	require.Equal(t, "gpt-5.4", summary.Model)

	_, err = InspectRequest(
		Endpoint{Protocol: protocolOpenAIChatCompletions},
		[]byte(`{"model":"gpt-5.4","model":"gpt-5.4-dup","messages":[{"role":"user","content":"hi"}]}`),
	)
	require.Error(t, err)

	_, err = InspectRequest(Endpoint{Protocol: "unknown_protocol"}, []byte(`{}`))
	require.Error(t, err)
}

// TestInspectResponsesShellAndMCPToolOutputs feeds shell_call, local_shell_call,
// shell_call_output, local_shell_call_output, computer_call_output, and
// mcp_tool_call_output items into InspectRequest. All recognized tool output
// item types must increment ToolResults and delimit assistant turn runs.
func TestInspectResponsesShellAndMCPToolOutputs(t *testing.T) {
	body := []byte(`{
		"model":"grok-4.7-fast",
		"stream":true,
		"tools":[{"type":"shell"}],
		"input":[
			{"role":"user","content":"write marker"},
			{"type":"reasoning","id":"rs_1","summary":[]},
			{"type":"shell_call","call_id":"sh_1"},
			{"type":"shell_call_output","call_id":"sh_1","output":[{"stdout":"ok"}]},
			{"type":"local_shell_call","call_id":"lsh_1"},
			{"type":"local_shell_call_output","call_id":"lsh_1","output":"ok"},
			{"type":"mcp_call","call_id":"mcp_1"},
			{"type":"mcp_tool_call_output","call_id":"mcp_1","output":"ok"}
		]
	}`)
	summary, err := InspectRequest(Endpoint{Protocol: protocolOpenAIResponses}, body)
	require.NoError(t, err)
	require.Equal(t, RequestSummary{
		Model:             "grok-4.7-fast",
		Stream:            true,
		HasTools:          true,
		AssistantMessages: 3,
		ToolResults:       3,
	}, summary)
}
