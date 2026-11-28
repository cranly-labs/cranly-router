package httpapi

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// TestRestoreCodexToolNamesFromContext_HTTPAndWSPayloadShapes restores aliases
// in streaming item and unary output nodes. It requires unrelated text to
// remain literal and verifies clearing the reverse map leaves later tool names
// unchanged.
func TestRestoreCodexToolNamesFromContext_HTTPAndWSPayloadShapes(t *testing.T) {
	c, _ := gin.CreateTestContext(nil)
	SetCodexToolNameReverse(c, map[string]string{openai.CodexPythonToolAlias: "python"})

	streamEvent := RestoreCodexToolNamesFromContext(c, []byte(
		`{"type":"response.output_item.done","item":{"type":"function_call","name":"python__tokenrouter"},"note":"python__tokenrouter"}`,
	))
	require.Equal(t, "python", gjson.GetBytes(streamEvent, "item.name").String())
	require.Equal(t, "python__tokenrouter", gjson.GetBytes(streamEvent, "note").String())

	nonStreaming := RestoreCodexToolNamesFromContext(c, []byte(
		`{"id":"resp_1","output":[{"type":"function_call","name":"python__tokenrouter"}]}`,
	))
	require.Equal(t, "python", gjson.GetBytes(nonStreaming, "output.0.name").String())

	SetCodexToolNameReverse(c, nil)
	require.JSONEq(t,
		`{"type":"response.output_item.added","item":{"name":"python__tokenrouter"}}`,
		string(RestoreCodexToolNamesFromContext(c, []byte(`{"type":"response.output_item.added","item":{"name":"python__tokenrouter"}}`))),
	)
}

// TestRestoreCodexToolNamesInJSON_OnlyTouchesResponseToolCallNodesAndPreservesNumbers
// combines tool calls, ordinary name metadata and a large numeric literal. It
// requires targeted name restoration while unrelated names and exact numeric
// spelling survive.
func TestRestoreCodexToolNamesInJSON_OnlyTouchesResponseToolCallNodesAndPreservesNumbers(t *testing.T) {
	reverse := map[string]string{openai.CodexPythonToolAlias: "python"}
	body := []byte(`{"type":"response.completed","response":{"output":[{"type":"function_call","name":"python__tokenrouter"},{"type":"message","name":"python__tokenrouter","content":[]}]},"metadata":{"name":"python__tokenrouter"},"sequence":900719925474099312345}`)

	restored := openai.RestoreCodexToolNamesInJSON(body, reverse)
	require.Equal(t, "python", gjson.GetBytes(restored, "response.output.0.name").String())
	require.Equal(t, openai.CodexPythonToolAlias, gjson.GetBytes(restored, "response.output.1.name").String())
	require.Equal(t, openai.CodexPythonToolAlias, gjson.GetBytes(restored, "metadata.name").String())
	require.Equal(t, "900719925474099312345", gjson.GetBytes(restored, "sequence").Raw)
}

// TestRestoreCodexToolNamesInJSON_ExplicitHTTPAndSSEToolCallProtocols supplies
// Chat unary/delta and Messages tool-use shapes. Only their supported tool name
// nodes must be restored, leaving unrelated metadata names literal.
func TestRestoreCodexToolNamesInJSON_ExplicitHTTPAndSSEToolCallProtocols(t *testing.T) {
	reverse := map[string]string{openai.CodexPythonToolAlias: "python"}
	tests := []struct {
		name string // response shape scenario
		body string // protocol input JSON
		path string // restored tool name path
	}{
		{
			name: "chat http",
			body: `{"choices":[{"message":{"tool_calls":[{"type":"function","function":{"name":"python__tokenrouter"}}]}}],"metadata":{"name":"python__tokenrouter"}}`,
			path: "choices.0.message.tool_calls.0.function.name",
		},
		{
			name: "chat sse",
			body: `{"choices":[{"delta":{"tool_calls":[{"type":"function","function":{"name":"python__tokenrouter"}}]}}],"metadata":{"name":"python__tokenrouter"}}`,
			path: "choices.0.delta.tool_calls.0.function.name",
		},
		{
			name: "messages tool use",
			body: `{"type":"content_block_start","content":[{"type":"tool_use","name":"python__tokenrouter"}],"metadata":{"name":"python__tokenrouter"}}`,
			path: "content.0.name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			restored := openai.RestoreCodexToolNamesInJSON([]byte(tt.body), reverse)
			require.Equal(t, "python", gjson.GetBytes(restored, tt.path).String())
			require.Equal(t, openai.CodexPythonToolAlias, gjson.GetBytes(restored, "metadata.name").String())
		})
	}
}

// TestCodexToolNameReverse_WSSessionReplacementDoesNotChangeActiveTurn replaces
// future session declarations while active output uses earlier names. It
// protects active/session separation, explicit declaration precedence and later
// inheritance when a turn omits its own tools.
func TestCodexToolNameReverse_WSSessionReplacementDoesNotChangeActiveTurn(t *testing.T) {
	c, _ := gin.CreateTestContext(nil)
	SetCodexToolNameReverse(c, nil)
	first := []byte(`{"type":"response.create","tools":[{"type":"function","name":"python"}]}`)
	UpdateCodexToolNameReverseForWSFrame(c, first, map[string]string{openai.CodexPythonToolAlias: "python"})

	update := []byte(`{"type":"session.update","session":{"tools":[{"type":"function","name":"python__tokenrouter"}]}}`)
	UpdateCodexToolNameReverseForWSFrame(c, update, nil)
	currentOutput := RestoreCodexToolNamesFromContext(c, []byte(`{"type":"response.output_item.done","item":{"type":"function_call","name":"python__tokenrouter"}}`))
	require.Equal(t, "python", gjson.GetBytes(currentOutput, "item.name").String())
	sessionEcho := RestoreCodexToolNamesFromContext(c, []byte(`{"type":"session.updated","session":{"tools":[{"type":"function","name":"python__tokenrouter"}]}}`))
	require.Equal(t, openai.CodexPythonToolAlias, gjson.GetBytes(sessionEcho, "session.tools.0.name").String())

	next := []byte(`{"type":"response.create","input":"next"}`)
	UpdateCodexToolNameReverseForWSFrame(c, next, nil)
	nextOutput := RestoreCodexToolNamesFromContext(c, []byte(`{"type":"response.output_item.done","item":{"type":"function_call","name":"python__tokenrouter"}}`))
	require.Equal(t, openai.CodexPythonToolAlias, gjson.GetBytes(nextOutput, "item.name").String())

	sessionPython := []byte(`{"type":"session.update","session":{"tools":[{"type":"function","name":"python"}]}}`)
	UpdateCodexToolNameReverseForWSFrame(c, sessionPython, map[string]string{openai.CodexPythonToolAlias: "python"})
	explicitLiteral := []byte(`{"type":"response.create","input":[{"type":"additional_tools","tools":[{"type":"function","name":"python__tokenrouter"}]}]}`)
	UpdateCodexToolNameReverseForWSFrame(c, explicitLiteral, nil)
	literalOutput := RestoreCodexToolNamesFromContext(c, []byte(`{"type":"response.output_item.done","item":{"type":"function_call","name":"python__tokenrouter"}}`))
	require.Equal(t, openai.CodexPythonToolAlias, gjson.GetBytes(literalOutput, "item.name").String())
	UpdateCodexToolNameReverseForWSFrame(c, next, nil)
	inheritedOutput := RestoreCodexToolNamesFromContext(c, []byte(`{"type":"response.output_item.done","item":{"type":"function_call","name":"python__tokenrouter"}}`))
	require.Equal(t, "python", gjson.GetBytes(inheritedOutput, "item.name").String())
}

// TestRestoreCodexToolNamesFromSSEContextUsesEventLineTypeWithoutAddingType
// omits payload type while providing the event line classification. It requires
// restored tool naming and unchanged unrelated metadata, with no synthetic type
// left in successful output.
func TestRestoreCodexToolNamesFromSSEContextUsesEventLineTypeWithoutAddingType(t *testing.T) {
	c, _ := gin.CreateTestContext(nil)
	SetCodexToolNameReverse(c, map[string]string{openai.CodexPythonToolAlias: openai.CodexReservedPythonToolName})
	payload := []byte(`{"item":{"type":"function_call","name":"python__tokenrouter"},"metadata":{"name":"python__tokenrouter"}}`)

	restored := RestoreCodexToolNamesFromSSEContext(c, payload, "response.output_item.done")

	require.Equal(t, openai.CodexReservedPythonToolName, gjson.GetBytes(restored, "item.name").String())
	require.Equal(t, openai.CodexPythonToolAlias, gjson.GetBytes(restored, "metadata.name").String())
	require.False(t, gjson.GetBytes(restored, "type").Exists())
}

// TestIRResponsesClientToolMapStateAndBufferedRestoration publishes OpenAI,
// Grok, and WS HTTP bridge IR ToolMaps on request state. It verifies deep-copy
// isolation, lossless integer preservation on buffered restoration, non-object
// safety, and clean state reset when IR slots are cleared.
func TestIRResponsesClientToolMapStateAndBufferedRestoration(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())

	reqBody := []byte(`{"model":"gpt-5.4","tools":[{"type":"custom","name":"exec"},{"type":"tool_search","execution":"client"},{"type":"namespace","name":"db","tools":[{"type":"function","name":"query"}]}]}`)
	_, toolMap, _, err := convert.AdaptResponsesTools(reqBody, convert.Dialect{Tools: ir.ToolSupportFunction}, nil)
	require.NoError(t, err)
	require.True(t, toolMap.HasRestorations())

	SetOpenAIResponsesClientToolMap(c, toolMap)
	SetGrokResponsesClientToolMap(c, toolMap)
	SetOpenAIWSHTTPBridgeToolMap(c, toolMap)

	// Mutating the caller's map after publication must not alter stored request state.
	toolMap.CustomTools["mutated"] = true
	gotOpenAI, okOpenAI := OpenAIResponsesClientToolMap(c)
	require.True(t, okOpenAI)
	require.NotContains(t, gotOpenAI.CustomTools, "mutated")

	gotBridge, okBridge := OpenAIWSHTTPBridgeToolMapFromContext(c)
	require.True(t, okBridge)
	require.NotNil(t, gotBridge)
	require.NotEmpty(t, gotBridge.LoweredTools)

	upstreamPayload := []byte(`{"id":"resp_ir","sequence":900719925474099312345,"output":[{"id":"fc_1","type":"function_call","call_id":"call_1","name":"exec","arguments":"{\"input\":\"ls -la\"}"},{"id":"fc_2","type":"function_call","call_id":"call_2","name":"tool_search","arguments":"{\"query\":\"crm\"}"},{"id":"fc_3","type":"function_call","call_id":"call_3","name":"db__query","arguments":"{\"sql\":\"select 1\"}"}]}`)
	restoredOpenAI, err := RestoreOpenAIResponsesClientToolPayload(c, upstreamPayload)
	require.NoError(t, err)
	require.Equal(t, "900719925474099312345", gjson.GetBytes(restoredOpenAI, "sequence").Raw)
	require.Equal(t, "custom_tool_call", gjson.GetBytes(restoredOpenAI, "output.0.type").String())
	require.Equal(t, "ls -la", gjson.GetBytes(restoredOpenAI, "output.0.input").String())
	require.Equal(t, "tool_search_call", gjson.GetBytes(restoredOpenAI, "output.1.type").String())
	require.Equal(t, "client", gjson.GetBytes(restoredOpenAI, "output.1.execution").String())
	require.Equal(t, "function_call", gjson.GetBytes(restoredOpenAI, "output.2.type").String())
	require.Equal(t, "db", gjson.GetBytes(restoredOpenAI, "output.2.namespace").String())
	require.Equal(t, "query", gjson.GetBytes(restoredOpenAI, "output.2.name").String())

	restoredGrok, err := RestoreGrokResponsesClientToolPayload(c, upstreamPayload)
	require.NoError(t, err)
	require.JSONEq(t, string(restoredOpenAI), string(restoredGrok))

	// Non-object JSON scalars or arrays return unchanged without error.
	scalar := []byte(`["function_call"]`)
	unchangedScalar, err := RestoreOpenAIResponsesClientToolPayload(c, scalar)
	require.NoError(t, err)
	require.Equal(t, scalar, unchangedScalar)

	// Clearing IR maps disables IR restoration and allows explicit nil WS resets.
	ClearOpenAIResponsesClientToolMap(c)
	ClearGrokResponsesClientToolMap(c)
	_, okOpenAI = OpenAIResponsesClientToolMap(c)
	_, okGrok := GrokResponsesClientToolMap(c)
	require.False(t, okOpenAI)
	require.False(t, okGrok)

	SetOpenAIWSHTTPBridgeToolMap(c, nil)
	resetBridge, presentAfterReset := OpenAIWSHTTPBridgeToolMapFromContext(c)
	require.True(t, presentAfterReset)
	require.Nil(t, resetBridge)
}

// TestIRResponsesToolRestorationComposesWithCodexAliasAndNativeNamespace
// combines IR custom/namespace restoration with CodexToolNameReverse and
// native FlattenOpenAIResponsesNamespaces on the same response. Both streaming
// and buffered pipelines must restore every tool identity without interference.
func TestIRResponsesToolRestorationComposesWithCodexAliasAndNativeNamespace(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())

	// Seed Codex alias ("python__tokenrouter" -> "python"), native namespace
	// ("crm__lookup" -> crm.lookup), and IR ToolMap (custom "exec" + namespace "nb" with aliased python).
	SetCodexToolNameReverse(c, map[string]string{openai.CodexPythonToolAlias: openai.CodexReservedPythonToolName})
	flattened, report, err := FlattenOpenAIResponsesNamespaces(c, []byte(`{"sequence":9007199254740993,"tools":[{"type":"namespace","name":"crm","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}]}`))
	require.NoError(t, err)
	require.Contains(t, string(flattened), `"sequence":9007199254740993`)
	require.Equal(t, 1, report.Lowered)

	irReq := []byte(`{"model":"gpt-5.4","tools":[{"type":"custom","name":"exec"},{"type":"namespace","name":"nb","tools":[{"type":"function","name":"python__tokenrouter","parameters":{"type":"object"}}]}]}`)
	_, toolMap, _, err := convert.AdaptResponsesTools(irReq, convert.Dialect{Tools: ir.ToolSupportFunction}, nil)
	require.NoError(t, err)
	SetOpenAIResponsesClientToolMap(c, toolMap)

	// 1. Passthrough buffered order: RestoreNamespace -> RestoreToolNames -> RestoreOpenAITools.
	bufferedUpstream := []byte(`{"id":"resp_combo","output":[{"id":"fc_exec","type":"function_call","call_id":"c1","name":"exec","arguments":"{\"input\":\"pwd\"}"},{"id":"fc_nb","type":"function_call","call_id":"c2","name":"nb__python__tokenrouter","arguments":"{}"},{"id":"fc_crm","type":"function_call","call_id":"c3","name":"crm__lookup","arguments":"{}"}]}`)
	step1, err := RestoreOpenAIResponsesNamespacePayload(c, bufferedUpstream)
	require.NoError(t, err)
	step2 := RestoreCodexToolNamesFromContext(c, step1)
	passthroughBuffered, err := RestoreOpenAIResponsesClientToolPayload(c, step2)
	require.NoError(t, err)
	require.Equal(t, "custom_tool_call", gjson.GetBytes(passthroughBuffered, "output.0.type").String())
	require.Equal(t, "pwd", gjson.GetBytes(passthroughBuffered, "output.0.input").String())
	require.Equal(t, "nb", gjson.GetBytes(passthroughBuffered, "output.1.namespace").String())
	require.Equal(t, "python", gjson.GetBytes(passthroughBuffered, "output.1.name").String())
	require.Equal(t, "crm", gjson.GetBytes(passthroughBuffered, "output.2.namespace").String())
	require.Equal(t, "lookup", gjson.GetBytes(passthroughBuffered, "output.2.name").String())

	// 2. Standard buffered order: RestoreOpenAITools -> RestoreNamespace -> RestoreToolNames.
	std1, err := RestoreOpenAIResponsesClientToolPayload(c, bufferedUpstream)
	require.NoError(t, err)
	std2, err := RestoreOpenAIResponsesNamespacePayload(c, std1)
	require.NoError(t, err)
	standardBuffered := RestoreCodexToolNamesFromContext(c, std2)
	require.JSONEq(t, string(passthroughBuffered), string(standardBuffered))

	// 3. Streaming order: NewResponsesToolStreamBody -> RestoreNamespace -> RestoreCodexToolNamesFromSSEContext.
	rawSSE := strings.Join([]string{
		`event: response.output_item.done`,
		`data: {"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"fc_exec","type":"function_call","call_id":"c1","name":"exec","arguments":"{\"input\":\"pwd\"}"}}`,
		``,
		`event: response.output_item.done`,
		`data: {"type":"response.output_item.done","sequence_number":2,"output_index":1,"item":{"id":"fc_nb","type":"function_call","call_id":"c2","name":"nb__python__tokenrouter","arguments":"{}"}}`,
		``,
		`event: response.output_item.done`,
		`data: {"type":"response.output_item.done","sequence_number":3,"output_index":2,"item":{"id":"fc_crm","type":"function_call","call_id":"c3","name":"crm__lookup","arguments":"{}"}}`,
		``,
	}, "\n")
	wrapped := upstream.NewResponsesToolStreamBody(io.NopCloser(strings.NewReader(rawSSE)), toolMap, 64*1024)
	defer func() { _ = wrapped.Close() }()
	streamedBytes, err := io.ReadAll(wrapped)
	require.NoError(t, err)

	var donePayloads []string
	var pendingEvent string
	for line := range strings.SplitSeq(string(streamedBytes), "\n") {
		if after, ok := strings.CutPrefix(line, "event: "); ok {
			pendingEvent = strings.TrimSpace(after)
			continue
		}
		if after, ok := strings.CutPrefix(line, "data: "); ok {
			p, nsErr := RestoreOpenAIResponsesNamespacePayload(c, []byte(after))
			require.NoError(t, nsErr)
			p = RestoreCodexToolNamesFromSSEContext(c, p, pendingEvent)
			if gjson.GetBytes(p, "type").String() == "response.output_item.done" {
				donePayloads = append(donePayloads, string(p))
			}
			pendingEvent = ""
		}
	}
	require.Len(t, donePayloads, 3)
	require.Equal(t, "custom_tool_call", gjson.Get(donePayloads[0], "item.type").String())
	require.Equal(t, "pwd", gjson.Get(donePayloads[0], "item.input").String())
	require.Equal(t, "nb", gjson.Get(donePayloads[1], "item.namespace").String())
	require.Equal(t, "python", gjson.Get(donePayloads[1], "item.name").String())
	require.Equal(t, "crm", gjson.Get(donePayloads[2], "item.namespace").String())
	require.Equal(t, "lookup", gjson.Get(donePayloads[2], "item.name").String())
}
