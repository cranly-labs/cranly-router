package convert

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// toolCallNameRecord captures one historical tool invocation's call_id and
// upstream function name for tool-name stability verification.
type toolCallNameRecord struct {
	CallID string `json:"call_id"` // correlation identifier on the historical call
	Name   string `json:"name"`    // upstream executable tool name sent on the wire
}

// TestAdaptAndRestoreResponsesTools_CustomNamespaceSearchAndWSWarmup exercises
// same-protocol Responses client-tool lowering, ToolMap JSON serialization, WS
// warmup LoweredTools inheritance, compaction/cache-breakpoint preservation,
// and streaming restoration with hosted tool events. Adapted requests must pass
// function-only strict validation and restore client tool identities cleanly.
func TestAdaptAndRestoreResponsesTools_CustomNamespaceSearchAndWSWarmup(t *testing.T) {
	fnOnlyDialect := Dialect{
		Name:             "openai-apikey",
		Tools:            ToolSupportFunction,
		ToolNameMaxBytes: 64,
	}
	fullDialect := Dialect{
		Name:  "openai-oauth",
		Tools: ToolSupportAllResponses,
	}

	for _, scenario := range []string{"tools_custom", "tools_namespace", "tools_search"} {
		t.Run("wire_"+scenario, func(t *testing.T) {
			rawReq, err := testkit.ReadTestdataFile("wire/openai_responses/request/" + scenario + ".json")
			require.NoError(t, err)

			require.False(t, NeedsResponsesToolAdaptation(fullDialect, rawReq, nil))
			require.True(t, NeedsResponsesToolAdaptation(fnOnlyDialect, rawReq, nil))

			adapted, tm, losses, err := AdaptResponsesTools(rawReq, fnOnlyDialect, nil)
			require.NoError(t, err)
			require.NotNil(t, tm)
			require.NotEmpty(t, losses)

			violations := testkit.ValidateResponsesRequest(adapted, testkit.RequestValidationOptions{
				DialectRules: []string{"function-only"},
			})
			require.Empty(t, violations, "function-only violations on %s: %v", scenario, violations)

			// Verify ToolMap round-trips cleanly through JSON v2 serialization.
			marshaledMap, err := json.Marshal(tm, json.Deterministic(true))
			require.NoError(t, err)
			var unmarshaledMap ToolMap
			require.NoError(t, json.Unmarshal(marshaledMap, &unmarshaledMap))
			require.Equal(t, tm.CustomTools, unmarshaledMap.CustomTools)
			require.Equal(t, tm.CustomNames, unmarshaledMap.CustomNames)
			require.Equal(t, tm.ToolSearch, unmarshaledMap.ToolSearch)
			require.Equal(t, tm.NamespaceTools, unmarshaledMap.NamespaceTools)
			require.Equal(t, tm.CallNames, unmarshaledMap.CallNames)
		})
	}

	t.Run("preserves compaction, prompt_cache_breakpoint, bare reasoning encrypted_content, and >53-bit integers", func(t *testing.T) {
		clientBody := []byte(`{
			"model":"gpt-5.3-codex",
			"prompt_cache_key":"pck_9007199254740993",
			"seed_big":9007199254740993,
			"tools":[
				{"type":"custom","name":"apply_patch","description":"Patch files","format":{"type":"grammar","syntax":"lark"}},
				{"type":"web_search","search_context_size":"high"}
			],
			"input":[
				{"type":"compaction","encrypted_content":"cmp_opaque_blob"},
				{"type":"message","role":"user","prompt_cache_breakpoint":true,"content":[{"type":"input_text","text":"patch main.go"}]},
				{"id":"rs_bare_1","type":"reasoning","summary":[{"type":"summary_text","text":"plan patch"}],"encrypted_content":"gAAAAABn_bare_cipher"},
				{"id":"ctc_patch_1","type":"custom_tool_call","call_id":"call_p1","name":"apply_patch","input":"*** Begin Patch\n+ok"},
				{"id":"ctco_patch_1","type":"custom_tool_call_output","call_id":"call_p1","output":"applied"}
			]
		}`)

		adapted, tm, losses, err := AdaptResponsesTools(clientBody, fnOnlyDialect, nil)
		require.NoError(t, err)
		require.NotNil(t, tm)
		require.True(t, tm.IsCustomTool("apply_patch"))
		require.Len(t, losses, 3)

		adaptedStr := string(adapted)
		require.Contains(t, adaptedStr, `"seed_big":9007199254740993`)
		require.Contains(t, adaptedStr, `"type":"compaction"`)
		require.Contains(t, adaptedStr, `"prompt_cache_breakpoint":true`)
		require.Contains(t, adaptedStr, `"encrypted_content":"gAAAAABn_bare_cipher"`)
		require.NotContains(t, adaptedStr, "trc1:")
		require.Contains(t, adaptedStr, `"type":"web_search"`)
		require.Contains(t, adaptedStr, `"id":"fc_patch_1"`)
		require.Empty(t, testkit.ValidateResponsesRequest(adapted, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		}))
	})

	t.Run("WS warmup turn 1 LoweredTools carries into omitted-tools and omitted-model turns 2 and 3", func(t *testing.T) {
		turn1Warmup := []byte(`{
			"type":"response.create",
			"model":"gpt-5.3-codex",
			"generate":false,
			"tools":[
				{"type":"tool_search","description":"Custom workspace connector search."},
				{"type":"custom","name":"code.exec","description":"Execute snippet"},
				{"type":"custom","name":"apply_patch","description":"Apply patch"},
				{"type":"namespace","name":"functions","tools":[{"type":"function","name":"wait","parameters":{"type":"object"}}]},
				{"type":"namespace","name":"git","tools":[{"type":"function","name":"status","parameters":{"type":"object"}}]}
			],
			"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"warmup"}]}]
		}`)

		adapted1, tm1, _, err := AdaptResponsesTools(turn1Warmup, fnOnlyDialect, nil)
		require.NoError(t, err)
		require.NotNil(t, tm1)
		require.NotEmpty(t, tm1.LoweredTools)
		require.Contains(t, string(adapted1), `"generate":false`)

		// Round-trip ToolMap through JSON v2 to exercise persisted WS session state.
		serializedTM1, err := json.Marshal(tm1, json.Deterministic(true))
		require.NoError(t, err)
		var persistedTM1 ToolMap
		require.NoError(t, json.Unmarshal(serializedTM1, &persistedTM1))

		// Turn 2 omits both "tools" and "model", replaying custom + namespaced calls.
		turn2Omitted := []byte(`{
			"type":"response.create",
			"previous_response_id":"resp_warm_1",
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]},
				{"id":"ctc_1","type":"custom_tool_call","call_id":"c_patch","name":"apply_patch","input":"*** Begin Patch"},
				{"type":"custom_tool_call_output","call_id":"c_patch","output":"done"},
				{"id":"ctc_2","type":"custom_tool_call","call_id":"c_exec","name":"code.exec","input":"ls -la"},
				{"type":"custom_tool_call_output","call_id":"c_exec","output":"ok"},
				{"id":"fc_3","type":"function_call","call_id":"c_git","namespace":"git","name":"status","arguments":"{}"},
				{"type":"function_call_output","call_id":"c_git","output":"clean"}
			]
		}`)
		require.True(t, NeedsResponsesToolAdaptation(fnOnlyDialect, turn2Omitted, &persistedTM1))

		adapted2, tm2, _, err := AdaptResponsesTools(turn2Omitted, fnOnlyDialect, &persistedTM1)
		require.NoError(t, err)
		require.NotNil(t, tm2)
		require.NotContains(t, string(adapted2), `"model"`)
		require.Contains(t, string(adapted2), `"git__status"`)
		require.Contains(t, string(adapted2), `"code_exec"`)
		require.Contains(t, string(adapted2), toolSearchProxyDescription)

		// Adding model validates full function-only request invariants.
		withModel2 := append([]byte(`{"model":"gpt-5.3-codex",`), adapted2[1:]...)
		require.Empty(t, testkit.ValidateResponsesRequest(withModel2, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		}))

		// Stream restoration on Turn 2: hosted web_search events pass through,
		// sanitized custom tool restores original "code.exec" name on added/done/input.done,
		// default "functions" tool omits namespace, and sequence_number stays contiguous.
		upstreamFrames := []sse.Frame{
			{Event: "response.created", Data: []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_2","object":"response","created_at":1760000000,"status":"in_progress","model":"gpt-5.3-codex","output":[]}}`)},
			{Event: "response.in_progress", Data: []byte(`{"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_2","object":"response","created_at":1760000000,"status":"in_progress","model":"gpt-5.3-codex","output":[]}}`)},
			{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"in_progress"}}`)},
			{Event: "response.web_search_call.searching", Data: []byte(`{"type":"response.web_search_call.searching","sequence_number":3,"output_index":0,"item_id":"ws_1"}`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":4,"output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"completed"}}`)},
			{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":5,"output_index":1,"item":{"id":"fc_custom_1","type":"function_call","status":"in_progress","call_id":"call_p2","name":"code_exec","arguments":""}}`)},
			{Event: "response.function_call_arguments.delta", Data: []byte(`{"type":"response.function_call_arguments.delta","sequence_number":6,"output_index":1,"item_id":"fc_custom_1","delta":"{\"input\":\"echo "}`)},
			{Event: "response.function_call_arguments.delta", Data: []byte(`{"type":"response.function_call_arguments.delta","sequence_number":7,"output_index":1,"item_id":"fc_custom_1","delta":"hello\\n\"}"}`)},
			{Event: "response.function_call_arguments.done", Data: []byte(`{"type":"response.function_call_arguments.done","sequence_number":8,"output_index":1,"item_id":"fc_custom_1","arguments":"{\"input\":\"echo hello\\n\"}"}`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":9,"output_index":1,"item":{"id":"fc_custom_1","type":"function_call","status":"completed","call_id":"call_p2","name":"code_exec","arguments":"{\"input\":\"echo hello\\n\"}"}}`)},
			{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":10,"output_index":2,"item":{"id":"fc_git_2","type":"function_call","status":"in_progress","call_id":"call_g2","name":"git__status","arguments":""}}`)},
			{Event: "response.function_call_arguments.delta", Data: []byte(`{"type":"response.function_call_arguments.delta","sequence_number":11,"output_index":2,"item_id":"fc_git_2","delta":"{}"}`)},
			{Event: "response.function_call_arguments.done", Data: []byte(`{"type":"response.function_call_arguments.done","sequence_number":12,"output_index":2,"item_id":"fc_git_2","arguments":"{}"}`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":13,"output_index":2,"item":{"id":"fc_git_2","type":"function_call","status":"completed","call_id":"call_g2","name":"git__status","arguments":"{}"}}`)},
			{Event: "response.completed", Data: []byte(`{"type":"response.completed","sequence_number":14,"response":{"id":"resp_2","object":"response","created_at":1760000000,"status":"completed","model":"gpt-5.3-codex","output":[{"id":"ws_1","type":"web_search_call","status":"completed"},{"id":"fc_custom_1","type":"function_call","status":"completed","call_id":"call_p2","name":"code_exec","arguments":"{\"input\":\"echo hello\\n\"}"},{"id":"fc_git_2","type":"function_call","status":"completed","call_id":"call_g2","name":"git__status","arguments":"{}"}],"usage":{"input_tokens":50,"output_tokens":20,"total_tokens":70,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`)},
		}

		stream := NewResponsesToolStream(tm2)
		var clientFrames []sse.Frame
		for _, f := range upstreamFrames {
			batch, wErr := stream.Write(f)
			require.NoError(t, wErr)
			clientFrames = append(clientFrames, batch...)
		}
		tail, cErr := stream.Close(nil)
		require.NoError(t, cErr)
		clientFrames = append(clientFrames, tail...)

		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, clientFrames, testkit.GrammarOptions{}))
		rendered := string(formatSSEFrames(clientFrames))
		require.Contains(t, rendered, "response.web_search_call.searching")
		require.Contains(t, rendered, "response.custom_tool_call_input.delta")
		require.Contains(t, rendered, `"type":"response.custom_tool_call_input.done"`)
		require.Contains(t, rendered, `"name":"code.exec"`)
		require.NotContains(t, rendered, `"name":"code_exec"`)
		require.Contains(t, rendered, `"id":"ctc_custom_1"`)
		require.Contains(t, rendered, `"namespace":"git"`)
		require.Contains(t, rendered, `"name":"status"`)
		require.True(t, clientFrames[len(clientFrames)-1].Terminal)

		// Turn 3 also omits "tools" and "model", promotes tool_search_output discoveries,
		// and uses default-scope namespace tool_choice.
		turn3Omitted := []byte(`{
			"type":"response.create",
			"previous_response_id":"resp_2",
			"tool_choice":{"type":"namespace","name":"functions"},
			"input":[
				{"id":"tsc_3","type":"tool_search_call","call_id":"c_s3","execution":"client","arguments":{"query":"js"}},
				{"type":"tool_search_output","call_id":"c_s3","execution":"client","status":"completed","tools":[
					{"type":"function","name":"js","parameters":{"type":"object"}},
					{"type":"namespace","name":"pager","tools":[{"type":"function","name":"ack","parameters":{"type":"object"}}]}
				]},
				{"id":"fc_js_1","type":"function_call","call_id":"c_js_1","namespace":"js","name":"js","arguments":"{}"}
			]
		}`)
		adapted3, tm3, _, err := AdaptResponsesTools(turn3Omitted, fnOnlyDialect, tm2)
		require.NoError(t, err)
		require.NotNil(t, tm3)
		require.Contains(t, string(adapted3), `"tool_choice":"auto"`)
		require.Contains(t, string(adapted3), `"pager__ack"`)

		// Buffered RestoreResponsesTools on Turn 3 restores discovered plain function namespace "js",
		// strips default "functions" namespace from "wait", and preserves >53-bit integer precision.
		bufferedUpstream := []byte(`{"id":"resp_3","object":"response","status":"completed","big_num":9007199254740993,"output":[{"id":"fc_custom_1","type":"function_call","call_id":"call_p3","name":"code_exec","arguments":"{\"input\":\"pwd\"}"},{"id":"fc_search_1","type":"function_call","call_id":"call_s3","name":"tool_search","arguments":"{\"query\":\"git\"}"},{"id":"fc_js_2","type":"function_call","call_id":"call_js_2","name":"js","arguments":"{}"},{"id":"fc_wait_1","type":"function_call","call_id":"call_w1","namespace":"functions","name":"wait","arguments":"{}"}]}`)
		restoredBuf, err := RestoreResponsesTools(bufferedUpstream, tm3)
		require.NoError(t, err)
		restoredStr := string(restoredBuf)
		require.Contains(t, restoredStr, `"big_num":9007199254740993`)
		require.Contains(t, restoredStr, `"type":"custom_tool_call"`)
		require.Contains(t, restoredStr, `"name":"code.exec"`)
		require.Contains(t, restoredStr, `"type":"tool_search_call"`)
		require.Contains(t, restoredStr, `"execution":"client"`)
		require.Contains(t, restoredStr, `"namespace":"js"`)
		require.NotContains(t, restoredStr, `"namespace":"functions"`)

		// Explicit empty tools: [] with historical calls lowers history via inherited and resets ToolMap.
		resetWithHistory := []byte(`{
			"model":"gpt-5.3-codex",
			"tools":[],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"summary"}]},
				{"id":"ctc_r1","type":"custom_tool_call","call_id":"c_p1","name":"apply_patch","input":"*** Begin Patch"},
				{"type":"custom_tool_call_output","call_id":"c_p1","output":"ok"},
				{"id":"tsc_r2","type":"tool_search_call","call_id":"c_s1","execution":"client","arguments":{"query":"git"}},
				{"type":"tool_search_output","call_id":"c_s1","execution":"client","status":"completed","tools":[]},
				{"id":"fc_r3","type":"function_call","call_id":"c_g1","namespace":"git","name":"status","arguments":"{}"},
				{"type":"function_call_output","call_id":"c_g1","output":"clean"}
			]
		}`)
		require.True(t, NeedsResponsesToolAdaptation(fnOnlyDialect, resetWithHistory, tm3))
		adaptedReset, tmReset, _, err := AdaptResponsesTools(resetWithHistory, fnOnlyDialect, tm3)
		require.NoError(t, err)
		require.Nil(t, tmReset)
		require.Contains(t, string(adaptedReset), `"git__status"`)
		require.Empty(t, testkit.ValidateResponsesRequest(adaptedReset, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		}))

		// Explicit replacement with ordinary function tools clears inherited ToolMap while lowering history.
		replaceToolsBody := []byte(`{
			"model":"gpt-5.3-codex",
			"tools":[{"type":"function","name":"ping","parameters":{"type":"object"}}],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"ping now"}]},
				{"id":"fc_r4","type":"function_call","call_id":"c_g2","namespace":"git","name":"status","arguments":"{}"},
				{"type":"function_call_output","call_id":"c_g2","output":"clean"}
			]
		}`)
		require.True(t, NeedsResponsesToolAdaptation(fnOnlyDialect, replaceToolsBody, tm3))
		adaptedReplace, tmReplace, _, err := AdaptResponsesTools(replaceToolsBody, fnOnlyDialect, tm3)
		require.NoError(t, err)
		require.Nil(t, tmReplace)
		require.Contains(t, string(adaptedReplace), `"git__status"`)
		require.Empty(t, testkit.ValidateResponsesRequest(adaptedReplace, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		}))
	})

	t.Run("default functions-only WS warmup retains LoweredTools across three turns", func(t *testing.T) {
		warm := []byte(`{
			"type":"response.create",
			"model":"gpt-5.3-codex",
			"generate":false,
			"tools":[{"type":"namespace","name":"functions","tools":[{"type":"function","name":"wait","parameters":{"type":"object"}}]}],
			"input":"warmup"
		}`)
		_, tm1, _, err := AdaptResponsesTools(warm, fnOnlyDialect, nil)
		require.NoError(t, err)
		require.NotNil(t, tm1)
		require.Len(t, tm1.LoweredTools, 1)

		turn2 := []byte(`{"type":"response.create","tool_choice":{"type":"namespace","name":"functions"},"input":"step 2"}`)
		adapted2, tm2, _, err := AdaptResponsesTools(turn2, fnOnlyDialect, tm1)
		require.NoError(t, err)
		require.NotNil(t, tm2)
		require.Contains(t, string(adapted2), `"name":"wait"`)
		require.Contains(t, string(adapted2), `"tool_choice":"auto"`)

		turn3 := []byte(`{"type":"response.create","input":"step 3"}`)
		adapted3, tm3, _, err := AdaptResponsesTools(turn3, fnOnlyDialect, tm2)
		require.NoError(t, err)
		require.NotNil(t, tm3)
		require.Contains(t, string(adapted3), `"name":"wait"`)
	})

	t.Run("toolless continuation without inherited and standalone defer_loading lower cleanly", func(t *testing.T) {
		toollessHistory := []byte(`{
			"model":"gpt-5.3-codex",
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"summarize"}]},
				{"id":"ctc_h1","type":"custom_tool_call","call_id":"c_p1","name":"apply_patch","input":"*** Begin Patch"},
				{"type":"custom_tool_call_output","call_id":"c_p1","output":"patched"},
				{"id":"tsc_h2","type":"tool_search_call","call_id":"c_s1","execution":"client","arguments":{"query":"db"}},
				{"type":"tool_search_output","call_id":"c_s1","execution":"client","status":"completed","tools":[]},
				{"id":"fc_h3","type":"function_call","call_id":"c_n1","namespace":"db","name":"query","arguments":"{}"},
				{"type":"function_call_output","call_id":"c_n1","output":"rows"}
			]
		}`)
		require.True(t, NeedsResponsesToolAdaptation(fnOnlyDialect, toollessHistory, nil))
		adaptedToolless, tmToolless, _, err := AdaptResponsesTools(toollessHistory, fnOnlyDialect, nil)
		require.NoError(t, err)
		require.Nil(t, tmToolless)
		require.Contains(t, string(adaptedToolless), `"db__query"`)
		require.Empty(t, testkit.ValidateResponsesRequest(adaptedToolless, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		}))

		// Standalone defer_loading without tool_search strips defer_loading under function-only dialect.
		deferOnlyBody := []byte(`{
			"model":"gpt-5.3-codex",
			"tools":[
				{"type":"function","name":"lookup","defer_loading":true,"parameters":{"type":"object"}},
				{"type":"custom","name":"apply_patch","defer_loading":true}
			],
			"input":"hello"
		}`)
		require.True(t, NeedsResponsesToolAdaptation(fnOnlyDialect, deferOnlyBody, nil))
		adaptedDefer, _, _, err := AdaptResponsesTools(deferOnlyBody, fnOnlyDialect, nil)
		require.NoError(t, err)
		require.NotContains(t, string(adaptedDefer), `"defer_loading"`)
		require.Empty(t, testkit.ValidateResponsesRequest(adaptedDefer, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		}))
	})
}

// TestNeedsResponsesToolAdaptation_CapabilityAndEnvelopeMatrix verifies
// per-capability screening across custom, namespace, and client_search dialects,
// WS session.update versus nested response.create frames, single-object inputs,
// and allowed_tools selectors. Requests needing no lowering must return false
// and remain byte-identical under AdaptResponsesTools.
func TestNeedsResponsesToolAdaptation_CapabilityAndEnvelopeMatrix(t *testing.T) {
	fnOnly := Dialect{Tools: ToolSupportFunction, ToolNameMaxBytes: 64}
	keepNS := Dialect{Tools: ToolSupportFunction | ToolSupportNamespace | ToolSupportClientSearch, ToolNameMaxBytes: 64}
	keepCustom := Dialect{Tools: ToolSupportFunction | ToolSupportCustom | ToolSupportClientSearch, ToolNameMaxBytes: 64}
	keepSearch := Dialect{Tools: ToolSupportFunction | ToolSupportCustom | ToolSupportNamespace, ToolNameMaxBytes: 64}

	cases := []struct {
		name    string
		dialect Dialect
		body    string
		inh     *ToolMap
		want    bool
	}{
		{
			name:    "ordinary function and web_search need no adaptation",
			dialect: fnOnly,
			body:    `{"model":"gpt-5","tools":[{"type":"function","name":"ping","parameters":{"type":"object"}},{"type":"web_search"}],"input":"hi"}`,
			want:    false,
		},
		{
			name:    "empty namespace request skipped and kept byte-identical when dialect supports namespace",
			dialect: keepNS,
			body:    `{"model":"gpt-5","tools":[{"type":"namespace","name":"empty_ns","tools":[]}],"input":"hi"}`,
			want:    false,
		},
		{
			name:    "multi-child namespace request skipped and kept byte-identical when dialect supports namespace",
			dialect: keepNS,
			body:    `{"model":"gpt-5","tools":[{"type":"namespace","name":"git","tools":[{"type":"function","name":"status","parameters":{"type":"object"}},{"type":"function","name":"diff","parameters":{"type":"object"}}]}],"input":"hi"}`,
			want:    false,
		},
		{
			name:    "custom child inside namespace detected even when dialect supports namespace",
			dialect: keepNS,
			body:    `{"model":"gpt-5","tools":[{"type":"namespace","name":"patch","tools":[{"type":"custom","name":"apply.patch"},{"type":"function","name":"check","parameters":{"type":"object"}}]}],"tool_choice":{"type":"custom","namespace":"patch","name":"apply.patch"},"input":[{"id":"ctc_1","type":"custom_tool_call","call_id":"c1","namespace":"patch","name":"apply.patch","input":"***"},{"type":"custom_tool_call_output","call_id":"c1","output":"ok"}]}`,
			want:    true,
		},
		{
			name:    "defer_loading child inside multi-child namespace detected when dialect lacks search",
			dialect: keepSearch,
			body:    `{"model":"gpt-5","tools":[{"type":"namespace","name":"git","tools":[{"type":"function","name":"status","defer_loading":true,"parameters":{"type":"object"}},{"type":"function","name":"diff","parameters":{"type":"object"}}]}],"input":"hi"}`,
			want:    true,
		},
		{
			name:    "function_call with empty string namespace detected under namespace lowering",
			dialect: keepCustom,
			body:    `{"model":"gpt-5","input":[{"id":"fc_1","type":"function_call","call_id":"c1","namespace":"","name":"ping","arguments":"{}"},{"type":"function_call_output","call_id":"c1","output":"ok"}]}`,
			want:    true,
		},
		{
			name:    "custom_tool_call with namespace detected when custom kept and namespace lowered",
			dialect: keepCustom,
			body:    `{"model":"gpt-5","input":[{"id":"ctc_1","type":"custom_tool_call","call_id":"c1","namespace":"dev","name":"patch","input":"x"},{"type":"custom_tool_call_output","call_id":"c1","output":"ok"}]}`,
			want:    true,
		},
		{
			name:    "WS session.update frame ignored even with inherited ToolMap",
			dialect: fnOnly,
			body:    `{"type":"session.update","session":{"tools":[{"type":"custom","name":"exec"}]}}`,
			inh:     &ToolMap{CustomTools: map[string]bool{"exec": true}},
			want:    false,
		},
		{
			name:    "nested WS response.create envelope inspected",
			dialect: fnOnly,
			body:    `{"type":"response.create","response":{"model":"gpt-5","tools":[{"type":"custom","name":"exec"}],"input":"hi"}}`,
			want:    true,
		},
		{
			name:    "single-object input custom_tool_call detected",
			dialect: fnOnly,
			body:    `{"model":"grok-4.5","input":{"id":"ctc_1","type":"custom_tool_call","call_id":"c1","name":"exec","input":"pwd"}}`,
			want:    true,
		},
		{
			name:    "flat allowed_tools selector with custom and namespace entries detected",
			dialect: fnOnly,
			body:    `{"model":"gpt-5","tools":[{"type":"custom","name":"exec"},{"type":"namespace","name":"git","tools":[{"type":"function","name":"status","parameters":{"type":"object"}}]}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"custom","name":"exec"},{"type":"namespace","name":"git"}]},"input":"hi"}`,
			want:    true,
		},
		{
			name:    "nested allowed_tools selector with namespaced entry detected",
			dialect: fnOnly,
			body:    `{"model":"gpt-5","tools":[{"type":"namespace","name":"git","tools":[{"type":"function","name":"status","parameters":{"type":"object"}}]}],"tool_choice":{"type":"allowed_tools","allowed_tools":{"mode":"auto","tools":[{"type":"function","namespace":"git","name":"status"}]}},"input":"hi"}`,
			want:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := []byte(tc.body)
			got := NeedsResponsesToolAdaptation(tc.dialect, raw, tc.inh)
			require.Equal(t, tc.want, got)
			adapted, tm, _, err := AdaptResponsesTools(raw, tc.dialect, tc.inh)
			require.NoError(t, err)
			if !tc.want {
				require.True(t, bytes.Equal(raw, adapted), "non-adapted request must remain byte-for-byte identical")
				return
			}
			withModel := adapted
			if !strings.Contains(string(adapted), `"model"`) {
				withModel = append([]byte(`{"model":"gpt-5",`), adapted[1:]...)
			}
			var rules []string
			if tc.dialect.Tools == ToolSupportFunction {
				rules = []string{"function-only"}
			}
			// Grok Compact single-object input and Chat-nested allowed_tools wrappers
			// preserve their vendor envelope shapes rather than canonical /v1/responses create schema.
			if !strings.Contains(string(adapted), `"type":"response.create"`) &&
				!strings.Contains(tc.name, "single-object input") &&
				!strings.Contains(tc.name, "nested allowed_tools") {
				require.Empty(t, testkit.ValidateResponsesRequest(withModel, testkit.RequestValidationOptions{
					DialectRules: rules,
				}))
			}
			if strings.Contains(tc.name, "nested allowed_tools") {
				require.Contains(t, string(adapted), `"tools":[{"type":"function","name":"git__status"}]`)
				require.NotContains(t, string(adapted), `"namespace":"git"`)
			}
			if tc.dialect.Tools.Has(ToolSupportNamespace) && !tc.dialect.Tools.Has(ToolSupportCustom) && strings.Contains(tc.body, `"namespace":"patch"`) {
				adaptedStr := string(adapted)
				require.Contains(t, adaptedStr, `"type":"namespace"`)
				require.Contains(t, adaptedStr, `"name":"apply_patch"`)
				require.Contains(t, adaptedStr, `"name":"check"`)
				require.Contains(t, adaptedStr, `"tool_choice":{"type":"function","namespace":"patch","name":"apply_patch"}`)
				require.Contains(t, adaptedStr, `"type":"function_call","call_id":"c1","namespace":"patch","name":"apply_patch"`)
				require.NotNil(t, tm)

				// Round-trip WS omitted-tools continuation + streaming/buffered restoration under keepNS.
				wsCont := []byte(`{"type":"response.create","input":"next"}`)
				adaptedWS, tmWS, _, wsErr := AdaptResponsesTools(wsCont, tc.dialect, tm)
				require.NoError(t, wsErr)
				require.Contains(t, string(adaptedWS), `"type":"namespace"`)
				require.Contains(t, string(adaptedWS), `"name":"apply_patch"`)

				bufUp := []byte(`{"id":"r1","object":"response","status":"completed","output":[{"id":"fc_1","type":"function_call","call_id":"c2","namespace":"patch","name":"apply_patch","arguments":"{\"input\":\"patch text\"}"}]}`)
				restoredBuf, rErr := RestoreResponsesTools(bufUp, tmWS)
				require.NoError(t, rErr)
				require.Contains(t, string(restoredBuf), `"type":"custom_tool_call"`)
				require.Contains(t, string(restoredBuf), `"namespace":"patch"`)
				require.Contains(t, string(restoredBuf), `"name":"apply.patch"`)

				stream := NewResponsesToolStream(tmWS)
				fAdded, sErr := stream.Write(sse.Frame{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"c2","namespace":"patch","name":"apply_patch","arguments":""}}`)})
				require.NoError(t, sErr)
				require.Len(t, fAdded, 1)
				require.Contains(t, string(fAdded[0].Data), `"type":"custom_tool_call"`)
				require.Contains(t, string(fAdded[0].Data), `"namespace":"patch"`)
				require.Contains(t, string(fAdded[0].Data), `"name":"apply.patch"`)
			}
			if tc.dialect.Tools.Has(ToolSupportNamespace) && !tc.dialect.Tools.Has(ToolSupportClientSearch) {
				adaptedStr := string(adapted)
				require.Contains(t, adaptedStr, `"type":"namespace"`)
				require.NotContains(t, adaptedStr, `"defer_loading"`)
				require.Contains(t, adaptedStr, `"name":"diff"`)
			}
		})
	}
}

// TestAdaptResponsesTools_UTF8DuplicatesAdditionalToolsChoiceAndCollisions
// exercises invalid UTF-8 byte sequences, duplicate JSON keys, trailing data,
// additional_tools lifting, allowed_tools selectors, Compact single-object
// inputs, and disappeared namespace replay names. Malformed or colliding
// declarations must return errors while valid inputs preserve unrelated bytes.
func TestAdaptResponsesTools_UTF8DuplicatesAdditionalToolsChoiceAndCollisions(t *testing.T) {
	fnOnly := Dialect{Tools: ToolSupportFunction, ToolNameMaxBytes: 64}

	t.Run("tolerates invalid UTF-8 bytes in untouched and adapted payloads", func(t *testing.T) {
		untouched := append([]byte(`{"model":"gpt-5","input":"prefix `), 0xff, 0xfe)
		untouched = append(untouched, []byte(` suffix"}`)...)
		require.False(t, NeedsResponsesToolAdaptation(fnOnly, untouched, nil))
		adaptedUntouched, tm, losses, err := AdaptResponsesTools(untouched, fnOnly, nil)
		require.NoError(t, err)
		require.Nil(t, tm)
		require.Empty(t, losses)
		require.True(t, bytes.Equal(untouched, adaptedUntouched))

		withCustom := append([]byte(`{"model":"gpt-5","tools":[{"type":"custom","name":"exec"}],"input":"bad `), 0xff)
		withCustom = append(withCustom, []byte(` utf8"}`)...)
		require.True(t, NeedsResponsesToolAdaptation(fnOnly, withCustom, nil))
		adaptedCustom, tmCustom, _, err := AdaptResponsesTools(withCustom, fnOnly, nil)
		require.NoError(t, err)
		require.NotNil(t, tmCustom)
		require.Contains(t, string(adaptedCustom), `"type":"function"`)
	})

	t.Run("rejects duplicate keys and trailing JSON garbage", func(t *testing.T) {
		badPayloads := map[string][]byte{
			"top-level duplicate key":  []byte(`{"model":"gpt-5","model":"gpt-5","tools":[{"type":"custom","name":"exec"}],"input":"hi"}`),
			"nested tool duplicate":    []byte(`{"model":"gpt-5","tools":[{"type":"custom","name":"exec","name":"dup"}],"input":"hi"}`),
			"nested WS response dup":   []byte(`{"type":"response.create","response":{"model":"a","model":"b","tools":[{"type":"custom","name":"exec"}]}}`),
			"trailing garbage":         []byte(`{"model":"gpt-5","tools":[{"type":"custom","name":"exec"}],"input":"hi"} trailing`),
			"second JSON document":     []byte(`{"model":"gpt-5","tools":[{"type":"custom","name":"exec"}],"input":"hi"} {"model":"other"}`),
			"non-object root JSON":     []byte(`[{"type":"custom","name":"exec"}]`),
			"missing search call_id":   []byte(`{"model":"gpt-5","tools":[{"type":"tool_search"}],"input":[{"type":"tool_search_output","tools":[]}]}`),
			"invalid additional_tools": []byte(`{"model":"gpt-5","input":[{"type":"additional_tools","tools":"not_array"}]}`),
		}
		for label, raw := range badPayloads {
			t.Run(label, func(t *testing.T) {
				_, _, _, err := AdaptResponsesTools(raw, fnOnly, nil)
				require.Error(t, err)
			})
		}
	})

	t.Run("lifts additional_tools for plain functions, namespaces, and empty lists with exact Loss coordinates", func(t *testing.T) {
		// Plain function inside additional_tools when top-level tools is omitted must not vanish.
		plainAdd := []byte(`{
			"model":"gpt-5.3-codex",
			"tool_choice":{"type":"function","namespace":"inspect","name":"inspect"},
			"input":[
				{"type":"additional_tools","tools":[{"type":"function","name":"inspect","parameters":{"type":"object"},"vendor_tag":9007199254740993}]},
				{"type":"message","role":"user","content":"run"},
				{"id":"ctc_after_add","type":"custom_tool_call","call_id":"c1","name":"apply_patch","input":"***"}
			]
		}`)
		adaptedPlain, _, lossesPlain, err := AdaptResponsesTools(plainAdd, fnOnly, nil)
		require.NoError(t, err)
		require.Contains(t, string(adaptedPlain), `"tools":[{"type":"function","name":"inspect","parameters":{"type":"object"},"vendor_tag":9007199254740993}]`)
		require.Contains(t, string(adaptedPlain), `"tool_choice":{"type":"function","name":"inspect"}`)
		require.NotContains(t, string(adaptedPlain), `"additional_tools"`)
		// custom_tool_call was originally at input[2] before input[0] (additional_tools) was lifted.
		require.Contains(t, lossesPlain, ir.Loss{Kind: ir.LossToolLowered, Path: "input[2]"})

		// Empty additional_tools sets tools: [] and removes the carrier item from input[].
		emptyAdd := []byte(`{"model":"gpt-5","input":[{"type":"additional_tools","tools":[]},{"type":"message","role":"user","content":"hi"}]}`)
		adaptedEmpty, tmEmpty, _, err := AdaptResponsesTools(emptyAdd, fnOnly, nil)
		require.NoError(t, err)
		require.Nil(t, tmEmpty)
		require.Contains(t, string(adaptedEmpty), `"tools":[]`)
		require.NotContains(t, string(adaptedEmpty), `"additional_tools"`)
	})

	t.Run("preserves untouched tool raw bytes when sibling namespace expands and discoveries promote", func(t *testing.T) {
		rawFnTool := `{"type":"function","name":"keep_exact","description":"Untouched","parameters":{"type":"object","properties":{"id":{"type":"integer"}}},"vendor_big":9007199254740993}`
		rawHosted := `{"type":"mcp","server_label":"corp_mcp","require_approval":"never","vendor_ext":true}`
		body := []byte(`{
			"model":"gpt-5",
			"tools":[
				` + rawFnTool + `,
				` + rawHosted + `,
				{"type":"tool_search"},
				{"type":"namespace","name":"git","tools":[
					{"type":"function","name":"status","parameters":{"type":"object"},"child_ext":42},
					{"type":"function","name":"diff","parameters":{"type":"object"}}
				]}
			],
			"tool_choice":{
				"type":"allowed_tools",
				"mode":"required",
				"tools":[
					{"type":"custom","name":"code.exec"},
					{"type":"namespace","name":"git"},
					{"type":"function","namespace":"git","name":"status"},
					{"type":"tool_search"}
				]
			},
			"input":[
				{"type":"additional_tools","tools":[{"type":"custom","name":"code.exec","custom_ext":"keep"}]},
				{"id":"tsc_1","type":"tool_search_call","call_id":"s1","arguments":{"query":"db"}},
				{"type":"tool_search_output","call_id":"s1","status":"completed","tools":[
					{"type":"function","name":"db_ping","parameters":{"type":"object"},"disc_ext":7}
				]}
			]
		}`)

		adapted, tm, _, err := AdaptResponsesTools(body, fnOnly, nil)
		require.NoError(t, err)
		require.NotNil(t, tm)
		adaptedStr := string(adapted)
		require.Contains(t, adaptedStr, rawFnTool)
		require.Contains(t, adaptedStr, rawHosted)
		require.Contains(t, adaptedStr, `"child_ext":42`)
		require.Contains(t, adaptedStr, `"custom_ext":"keep"`)
		require.Contains(t, adaptedStr, `"disc_ext":7`)
		require.Contains(t, adaptedStr, `{"type":"function","name":"code_exec"}`)
		require.Contains(t, adaptedStr, `{"type":"function","name":"git__status"}`)
		require.Contains(t, adaptedStr, `{"type":"function","name":"git__diff"}`)
		require.Contains(t, adaptedStr, `{"type":"function","name":"tool_search"}`)
		require.NotContains(t, adaptedStr, `{"type":"namespace","name":"git"}`)
		require.Empty(t, testkit.ValidateResponsesRequest(adapted, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		}))

		// Empty declared namespace as sole allowed_tools entry degrades tool_choice to "auto".
		emptyAllowedNS := []byte(`{"model":"gpt-5","tools":[{"type":"namespace","name":"empty_ns","tools":[]}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"namespace","name":"empty_ns"}]},"input":"hi"}`)
		adaptedEmptyAllowed, _, _, err := AdaptResponsesTools(emptyAllowedNS, fnOnly, nil)
		require.NoError(t, err)
		require.Contains(t, string(adaptedEmptyAllowed), `"tool_choice":"auto"`)
		require.Empty(t, testkit.ValidateResponsesRequest(adaptedEmptyAllowed, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		}))
	})

	t.Run("single-object Compact input and incomplete tool_search_output preserve shape", func(t *testing.T) {
		compactSingleObj := []byte(`{"model":"grok-4.5","tools":[{"type":"custom","name":"exec"}],"input":{"id":"ctc_1","type":"custom_tool_call","call_id":"c1","name":"exec","input":"pwd"}}`)
		adaptedSingle, tmSingle, _, err := AdaptResponsesTools(compactSingleObj, fnOnly, nil)
		require.NoError(t, err)
		require.NotNil(t, tmSingle)
		require.Contains(t, string(adaptedSingle), `"input":{"id":"fc_1","type":"function_call","call_id":"c1","name":"exec","arguments":"{\"input\":\"pwd\"}"}`)

		incompleteSearchOut := []byte(`{"model":"gpt-5","tools":[{"type":"tool_search"}],"input":[{"id":"tsc_1","type":"tool_search_call","call_id":"s1","arguments":{}},{"type":"tool_search_output","call_id":"s1","status":"incomplete","execution":"client"}]}`)
		adaptedInc, _, _, err := AdaptResponsesTools(incompleteSearchOut, fnOnly, nil)
		require.NoError(t, err)
		require.Contains(t, string(adaptedInc), `"status":"incomplete"`)
		require.Contains(t, string(adaptedInc), `"execution":"client"`)
		require.NotContains(t, string(adaptedInc), `"output"`)
	})

	t.Run("omitted tools continuation deduplicates replayed warmup additional_tools while keeping new and conflicting entries", func(t *testing.T) {
		warmup := []byte(`{
			"type":"response.create",
			"model":"gpt-5",
			"tools":[
				{"type":"custom","name":"apply_patch","description":"Patch"},
				{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}
			],
			"input":[
				{"type":"additional_tools","tools":[
					{"type":"namespace","name":"collab","tools":[{"type":"function","name":"ping","parameters":{"type":"object"}}]}
				]},
				{"type":"message","role":"developer","content":"warmup"}
			]
		}`)
		_, warmMap, _, err := AdaptResponsesTools(warmup, fnOnly, nil)
		require.NoError(t, err)
		require.NotNil(t, warmMap)
		require.Len(t, warmMap.LoweredTools, 3)

		// Continuation omits tools[] while replaying Turn 1's additional_tools plus a new child in collab.
		turn2 := []byte(`{
			"type":"response.create",
			"input":[
				{"type":"additional_tools","tools":[
					{"type":"namespace","name":"collab","tools":[{"type":"function","name":"ping","parameters":{"type":"object"}}]}
				]},
				{"type":"additional_tools","tools":[
					{"type":"namespace","name":"collab","tools":[{"type":"function","name":"pong","parameters":{"type":"object"}}]}
				]},
				{"type":"message","role":"user","content":"continue"}
			]
		}`)
		adaptedT2, t2Map, _, err := AdaptResponsesTools(turn2, fnOnly, warmMap)
		require.NoError(t, err)
		require.NotNil(t, t2Map)
		require.Len(t, t2Map.LoweredTools, 4)
		require.Equal(t, 1, strings.Count(string(adaptedT2), `"name":"collab__ping"`))
		require.Equal(t, 1, strings.Count(string(adaptedT2), `"name":"collab__pong"`))
		require.Equal(t, 1, strings.Count(string(adaptedT2), `"name":"apply_patch"`))
		require.Equal(t, 1, strings.Count(string(adaptedT2), `"name":"db__query"`))

		// Conflicting replayed/new additional_tools (custom tool colliding with inherited function) still fails.
		conflictT2 := []byte(`{
			"type":"response.create",
			"input":[
				{"type":"additional_tools","tools":[{"type":"custom","name":"db__query"}]},
				{"type":"message","role":"user","content":"collide"}
			]
		}`)
		_, _, _, err = AdaptResponsesTools(conflictT2, fnOnly, warmMap)
		require.Error(t, err)
	})

	t.Run("disappeared historical namespaces allocate deterministic collision-free names", func(t *testing.T) {
		makeBody := func(reverse bool) []byte {
			calls := []string{
				`{"type":"function_call","call_id":"c1","namespace":"a","name":"b__c","arguments":"{}"}`,
				`{"type":"function_call_output","call_id":"c1","output":"1"}`,
				`{"type":"function_call","call_id":"c2","namespace":"a__b","name":"c","arguments":"{}"}`,
				`{"type":"function_call_output","call_id":"c2","output":"2"}`,
				`{"type":"function_call","call_id":"c3","namespace":"` + strings.Repeat("long", 20) + `","name":"run","arguments":"{}"}`,
				`{"type":"function_call_output","call_id":"c3","output":"3"}`,
				`{"type":"function_call","call_id":"c4","namespace":"` + strings.Repeat("long", 20) + `","name":"stop","arguments":"{}"}`,
				`{"type":"function_call_output","call_id":"c4","output":"4"}`,
				`{"type":"function_call","call_id":"c5","namespace":"空间.dots","name":"exec","arguments":"{}"}`,
				`{"type":"function_call_output","call_id":"c5","output":"5"}`,
				`{"type":"custom_tool_call","call_id":"c6","name":"code.exec","input":"ls"}`,
				`{"type":"custom_tool_call_output","call_id":"c6","output":"6"}`,
				`{"type":"function_call","call_id":"c7","namespace":"code_exec","name":" collide ","arguments":"{}"}`,
				`{"type":"function_call_output","call_id":"c7","output":"7"}`,
			}
			if reverse {
				var rev []string
				for i := len(calls) - 2; i >= 0; i -= 2 {
					rev = append(rev, calls[i], calls[i+1])
				}
				calls = rev
			}
			return []byte(`{"model":"gpt-5","tools":[{"type":"function","name":"a__b__c","parameters":{"type":"object"}}],"input":[` + strings.Join(calls, ",") + `]}`)
		}

		fwd, tmFwd, _, err := AdaptResponsesTools(makeBody(false), fnOnly, nil)
		require.NoError(t, err)
		require.Nil(t, tmFwd, "disappeared historical calls must not enter active ToolMap")
		rev, _, _, err := AdaptResponsesTools(makeBody(true), fnOnly, nil)
		require.NoError(t, err)

		require.Empty(t, testkit.ValidateResponsesRequest(fwd, testkit.RequestValidationOptions{DialectRules: []string{"function-only"}}))
		require.Empty(t, testkit.ValidateResponsesRequest(rev, testkit.RequestValidationOptions{DialectRules: []string{"function-only"}}))

		_, fwdCalls := extractUpstreamToolsAndCalls(t, protocol.ProtocolOpenAIResponses, fwd)
		_, revCalls := extractUpstreamToolsAndCalls(t, protocol.ProtocolOpenAIResponses, rev)
		fwdByID := make(map[string]string, len(fwdCalls))
		seen := map[string]bool{"a__b__c": true}
		validRe := regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
		for _, c := range fwdCalls {
			require.Regexp(t, validRe, c.Name)
			if c.CallID != "c6" {
				require.False(t, seen[c.Name], "duplicate replay name %q", c.Name)
			}
			seen[c.Name] = true
			fwdByID[c.CallID] = c.Name
		}
		for _, c := range revCalls {
			require.Equal(t, fwdByID[c.CallID], c.Name, "replay name for %s must not depend on input order", c.CallID)
		}
	})

	t.Run("rejects declaration, discovery, and undeclared selector conflicts", func(t *testing.T) {
		conflictBodies := map[string]string{
			"custom vs function collision":                      `{"model":"gpt-5","tools":[{"type":"custom","name":"same"},{"type":"function","name":"same"}],"input":"hi"}`,
			"tool_search vs function collision":                 `{"model":"gpt-5","tools":[{"type":"tool_search"},{"type":"function","name":"tool_search"}],"input":"hi"}`,
			"flattened namespace vs top-level function":         `{"model":"gpt-5","tools":[{"type":"function","name":"team__send"},{"type":"namespace","name":"team","tools":[{"type":"function","name":"send"}]}],"input":"hi"}`,
			"two namespaces flatten to same name":               `{"model":"gpt-5","tools":[{"type":"namespace","name":"a","tools":[{"type":"function","name":"b__c"}]},{"type":"namespace","name":"a__b","tools":[{"type":"function","name":"c"}]}],"input":"hi"}`,
			"historical discovery collides with top-level":      `{"model":"gpt-5","tools":[{"type":"function","name":"browser__js"}],"input":[{"type":"tool_search_output","status":"completed","tools":[{"type":"namespace","name":"browser","tools":[{"type":"function","name":"js"}]}]}]}`,
			"discovered plain function collides with namespace": `{"model":"gpt-5","tools":[{"type":"namespace","name":"inspect","tools":[{"type":"function","name":"inspect"}]}],"input":[{"type":"tool_search_output","status":"completed","tools":[{"type":"function","name":"inspect"}]}]}`,
			"undeclared namespace tool_choice":                  `{"model":"gpt-5","tools":[{"type":"namespace","name":"git","tools":[{"type":"function","name":"status"}]}],"tool_choice":{"type":"namespace","name":"unknown"},"input":"hi"}`,
			"undeclared namespace inside allowed_tools":         `{"model":"gpt-5","tools":[{"type":"namespace","name":"git","tools":[{"type":"function","name":"status"}]}],"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"namespace","name":"unknown"}]},"input":"hi"}`,
			"undeclared qualified function tool_choice":         `{"model":"gpt-5","tools":[{"type":"function","name":"js"}],"tool_choice":{"type":"function","namespace":"unknown","name":"js"},"input":"hi"}`,
		}
		for name, raw := range conflictBodies {
			t.Run(name, func(t *testing.T) {
				_, _, _, err := AdaptResponsesTools([]byte(raw), fnOnly, nil)
				require.Error(t, err)
			})
		}
	})
}

// TestRestoreResponsesToolsAndStream_HostedEventsEnvelopesAndQuirks exercises
// native hosted-tool intermediate streaming events (partial_image,
// web_search_call.searching, mcp_*, code_interpreter_*), omitted arguments.done
// synthesis, type-less SSE payloads with Event headers, and buffered JSON
// envelope variants. Hosted progress frames must retain exact wire bytes and
// Semantic flags while lowered calls restore client tool identities.
func TestRestoreResponsesToolsAndStream_HostedEventsEnvelopesAndQuirks(t *testing.T) {
	tm := &ToolMap{
		CustomTools: map[string]bool{
			"apply_patch":       true,
			"patcher__edit_raw": true,
		},
		CustomNames: map[string]NamespacedName{
			"patcher__edit_raw": {Namespace: "patcher", Name: "edit.raw"},
		},
		ToolSearch: true,
		NamespaceTools: map[string]NamespacedName{
			"git__status": {Namespace: "git", Name: "status"},
			"wait":        {Namespace: "", Name: "wait"},
		},
	}

	t.Run("preserves partial_image, mcp, code_interpreter, and web_search intermediate events with Semantic flags", func(t *testing.T) {
		frames := []sse.Frame{
			{Event: "response.created", Data: []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_h","object":"response","created_at":1760000000,"status":"in_progress","model":"gpt-5","output":[]}}`)},
			{Event: "response.in_progress", Data: []byte(`{"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_h","object":"response","created_at":1760000000,"status":"in_progress","model":"gpt-5","output":[]}}`)},
			{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"ig_1","type":"image_generation_call","status":"in_progress"}}`)},
			{Event: "response.image_generation_call.partial_image", Data: []byte(`{"type":"response.image_generation_call.partial_image","sequence_number":3,"output_index":0,"item_id":"ig_1","partial_image_index":0,"partial_image_b64":"cGFydGlhbA==","vendor_big":9007199254740993}`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":4,"output_index":0,"item":{"id":"ig_1","type":"image_generation_call","status":"completed","result":"Zm9v"}}`)},
			{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":5,"output_index":1,"item":{"id":"mcp_1","type":"mcp_call","status":"in_progress","server_label":"github","name":"list_prs"}}`)},
			{Event: "response.mcp_call_arguments.delta", Data: []byte(`{"type":"response.mcp_call_arguments.delta","sequence_number":6,"output_index":1,"item_id":"mcp_1","delta":"{\"state\":\"open\"}"}`)},
			{Event: "response.mcp_call_arguments.done", Data: []byte(`{"type":"response.mcp_call_arguments.done","sequence_number":7,"output_index":1,"item_id":"mcp_1","arguments":"{\"state\":\"open\"}"}`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":8,"output_index":1,"item":{"id":"mcp_1","type":"mcp_call","status":"completed","server_label":"github","name":"list_prs","arguments":"{\"state\":\"open\"}","output":"[]"}}`)},
			// Custom tool omitting function_call_arguments.done before output_item.done:
			{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":9,"output_index":2,"item":{"id":"fc_p1","type":"function_call","status":"in_progress","call_id":"c_p1","name":"patcher__edit_raw","arguments":""}}`)},
			{Event: "response.function_call_arguments.delta", Data: []byte(`{"type":"response.function_call_arguments.delta","sequence_number":10,"output_index":2,"item_id":"fc_p1","delta":"{\"input\":\"raw patch\"}"}`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":11,"output_index":2,"item":{"id":"fc_p1","type":"function_call","status":"completed","call_id":"c_p1","name":"patcher__edit_raw"}}`)},
			// Default "functions" scope bare call passes through byte-identical once resequenced:
			{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":12,"output_index":3,"item":{"id":"fc_w1","type":"function_call","status":"in_progress","call_id":"c_w1","name":"wait","arguments":""}}`)},
			{Event: "response.function_call_arguments.done", Data: []byte(`{"type":"response.function_call_arguments.done","sequence_number":13,"output_index":3,"item_id":"fc_w1","arguments":"{}"}`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":14,"output_index":3,"item":{"id":"fc_w1","type":"function_call","status":"completed","call_id":"c_w1","name":"wait","arguments":"{}"}}`)},
			{Event: "response.completed", Data: []byte(`{"type":"response.completed","sequence_number":15,"big_seq_ext":9007199254740993,"response":{"id":"resp_h","object":"response","created_at":1760000000,"status":"completed","model":"gpt-5","output":[{"id":"ig_1","type":"image_generation_call","status":"completed","result":"Zm9v"},{"id":"mcp_1","type":"mcp_call","status":"completed","server_label":"github","name":"list_prs","arguments":"{\"state\":\"open\"}","output":"[]"},{"id":"fc_p1","type":"function_call","status":"completed","call_id":"c_p1","name":"patcher__edit_raw","arguments":"{\"input\":\"raw patch\"}"},{"id":"fc_w1","type":"function_call","status":"completed","call_id":"c_w1","name":"wait","arguments":"{}"}],"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`)},
		}

		stream := NewResponsesToolStream(tm)
		var out []sse.Frame
		for _, f := range frames {
			batch, err := stream.Write(f)
			require.NoError(t, err)
			out = append(out, batch...)
		}
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, out, testkit.GrammarOptions{}))

		// Verify partial_image and mcp_call_arguments.delta preserved exact bytes and marked Semantic=true.
		require.Equal(t, frames[3].Data, out[3].Data)
		require.True(t, out[3].Semantic, "partial_image frame must be marked Semantic=true")
		require.Equal(t, frames[6].Data, out[6].Data)
		require.True(t, out[6].Semantic, "mcp_call_arguments.delta frame must be marked Semantic=true")

		rendered := string(formatSSEFrames(out))
		require.Contains(t, rendered, `"type":"response.custom_tool_call_input.delta"`)
		require.Contains(t, rendered, `"type":"response.custom_tool_call_input.done"`)
		require.Contains(t, rendered, `"name":"edit.raw"`)
		require.Contains(t, rendered, `"namespace":"patcher"`)
		require.Contains(t, rendered, `"big_seq_ext":9007199254740993`)
	})

	t.Run("type-less SSE data with Event header and all terminal/buffered envelopes restore cleanly", func(t *testing.T) {
		stream := NewResponsesToolStream(tm)
		typelessFrames := []sse.Frame{
			{Event: "response.output_item.added", Data: []byte(`{"output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"c1","name":"apply_patch","arguments":""}}`)},
			{Event: "response.function_call_arguments.delta", Data: []byte(`{"output_index":0,"delta":"{\"input\":\"hi\"}"}`)},
			{Event: "response.function_call_arguments.done", Data: []byte(`{"output_index":0}`)},
			{Event: "response.output_item.done", Data: []byte(`{"output_index":0,"item":{"id":"fc_1","type":"function_call","call_id":"c1","name":"apply_patch"}}`)},
			{Event: "response.incomplete", Data: []byte(`{"response":{"id":"r1","status":"incomplete","output":[{"id":"fc_1","type":"function_call","call_id":"c1","name":"apply_patch","arguments":"{\"input\":\"hi\"}"}]}}`)},
		}
		var got []sse.Frame
		for _, f := range typelessFrames {
			batch, err := stream.Write(f)
			require.NoError(t, err)
			got = append(got, batch...)
		}
		require.Len(t, got, 5)
		require.Equal(t, "response.custom_tool_call_input.delta", got[1].Event)
		require.Equal(t, "response.custom_tool_call_input.done", got[2].Event)
		require.True(t, got[4].Terminal)
		require.Contains(t, string(got[4].Data), `"type":"custom_tool_call"`)

		for _, termType := range []string{
			"response.completed", "response.done", "response.incomplete",
			"response.failed", "response.cancelled", "response.canceled",
		} {
			s := NewResponsesToolStream(tm)
			res, changed, err := s.RestorePayload([]byte(`{"type":"` + termType + `","sequence_number":3,"response":{"output":[{"type":"function_call","id":"fc_1","call_id":"c1","name":"apply_patch","arguments":"{\"input\":\"ok\"}"}]}}`))
			require.NoError(t, err)
			require.True(t, changed)
			require.Len(t, res, 1)
			require.Contains(t, string(res[0]), `"type":"custom_tool_call"`)
		}

		for _, env := range []string{
			`{"output":[{"type":"function_call","id":"fc_1","call_id":"c1","name":"git__status","arguments":"{}"}]}`,
			`{"response":{"output":[{"type":"function_call","id":"fc_1","call_id":"c1","name":"git__status","arguments":"{}"}]}}`,
			`{"data":{"output":[{"type":"function_call","id":"fc_1","call_id":"c1","name":"git__status","arguments":"{}"}]}}`,
			`{"data":{"response":{"output":[{"type":"function_call","id":"fc_1","call_id":"c1","name":"git__status","arguments":"{}"}]}}}`,
		} {
			restored, err := RestoreResponsesTools([]byte(env), tm)
			require.NoError(t, err)
			require.Contains(t, string(restored), `"namespace":"git"`)
			require.Contains(t, string(restored), `"name":"status"`)
		}

		unchanged := []byte(`{"id":"r_plain","output":[{"type":"function_call","id":"fc_plain","call_id":"c_plain","name":"unmapped_fn","arguments":"{}"}]}`)
		gotUnchanged, err := RestoreResponsesTools(unchanged, tm)
		require.NoError(t, err)
		require.True(t, bytes.Equal(unchanged, gotUnchanged))

		// Unnumbered ping frames and non-JSON-object keepalive lines must not gain
		// sequence_number, advance nextSeq, or fail Write when restorations are active.
		pingStream := NewResponsesToolStream(tm)
		pingFrames := []sse.Frame{
			{Event: "response.created", Data: []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"r_ping","object":"response","status":"in_progress","output":[]}}`)},
			{Event: "ping", Data: []byte(`{"type":"ping","cost":0}`)},
			{Event: "keepalive", Data: []byte(`keepalive-raw-text`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":1,"output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"One-shot message."}]}}`)},
			{Event: "response.completed", Data: []byte(`{"type":"response.completed","sequence_number":2,"response":{"id":"r_ping","object":"response","status":"completed","output":[]}}`)},
			{Data: []byte(`[DONE]`)},
		}
		var pingOut []sse.Frame
		for _, pf := range pingFrames {
			batch, pErr := pingStream.Write(pf)
			require.NoError(t, pErr)
			pingOut = append(pingOut, batch...)
		}
		require.Len(t, pingOut, 6)
		require.Equal(t, `{"type":"ping","cost":0}`, string(pingOut[1].Data), "ping must not receive injected sequence_number")
		require.Equal(t, `keepalive-raw-text`, string(pingOut[2].Data), "non-JSON keepalive must pass through without error")
		require.Contains(t, string(pingOut[3].Data), `"sequence_number":1`, "subsequent event must keep contiguous sequence_number 1")
		require.True(t, pingOut[3].Semantic, "unannounced one-shot output_item.done with visible text must mark Semantic=true")
		require.Contains(t, string(pingOut[4].Data), `"sequence_number":2`)
		require.True(t, pingOut[4].Terminal)
		require.True(t, pingOut[5].Terminal)
	})
}

// TestRecordedResponsesCorpus_LosslessPrecheckAndFieldPreservation scans every
// recorded Codex and pi Responses request in testkit/testdata/corpus/clients/
// across both full-capability and function-only target dialects. Payloads that
// require no lowering must remain 100% byte-for-byte identical, while adapted
// Codex requests (including Turn 2 custom_tool_call history and enriched cache
// breakpoint, compaction, bare reasoning token, and 64-bit unknown fields) must
// preserve every non-tool field and untouched item without loss.
func TestRecordedResponsesCorpus_LosslessPrecheckAndFieldPreservation(t *testing.T) {
	allToolsDialect := Dialect{
		Name:             "openai-oauth",
		Tools:            ToolSupportAllResponses,
		ToolNameMaxBytes: 64,
	}
	fnOnlyDialects := []Dialect{
		{
			Name:             "openai-apikey",
			Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
			ToolNameMaxBytes: 64,
		},
		{
			Name:             "deepseek-apikey",
			Tools:            ToolSupportFunction,
			ToolNameMaxBytes: 64,
		},
	}

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolOpenAIResponses)
	require.NoError(t, err)
	require.NotEmpty(t, corpusReqs)

	sawCodexText := false
	sawCodexTool := false
	sawCodexCustomTurn1 := false
	sawCodexCustomTurn2 := false
	sawPi := false

	for _, fix := range corpusReqs {
		switch {
		case strings.Contains(fix.Path, "/codex/") && strings.Contains(fix.Path, "/lab_text/"):
			sawCodexText = true
		case strings.Contains(fix.Path, "/codex/") && strings.Contains(fix.Path, "/lab_tool/"):
			sawCodexTool = true
		case strings.HasSuffix(fix.Path, "/lab_custom_tool/1.request.json"):
			sawCodexCustomTurn1 = true
		case strings.HasSuffix(fix.Path, "/lab_custom_tool/2.request.json"):
			sawCodexCustomTurn2 = true
		case strings.Contains(fix.Path, "/pi/"):
			sawPi = true
		}

		t.Run(fix.Path, func(t *testing.T) {
			// 1. When target dialect natively supports all Responses client tools,
			// every recorded request must pass through 100% byte-for-byte unchanged.
			require.False(t, NeedsResponsesToolAdaptation(allToolsDialect, fix.Data, nil))
			adaptedAll, tmAll, lossesAll, err := AdaptResponsesTools(fix.Data, allToolsDialect, nil)
			require.NoError(t, err)
			require.Nil(t, tmAll)
			require.Empty(t, lossesAll)
			require.True(t, bytes.Equal(fix.Data, adaptedAll),
				"full-capability dialect must preserve %s byte-for-byte", fix.Path)

			// 2. Evaluate function-only target dialects (OpenAI API Key / Grok and DeepSeek).
			for _, fnOnlyDialect := range fnOnlyDialects {
				needs := NeedsResponsesToolAdaptation(fnOnlyDialect, fix.Data, nil)
				adapted, tm, _, err := AdaptResponsesTools(fix.Data, fnOnlyDialect, nil)
				require.NoError(t, err)

				if !needs {
					require.Nil(t, tm)
					require.True(t, bytes.Equal(fix.Data, adapted),
						"non-adapted corpus fixture %s must remain byte-identical", fix.Path)
				} else {
					require.NotNil(t, tm)
					assertOnlyToolsAdaptedInCorpusRequest(t, fix.Data, adapted)

					// Strip lowerable tool declarations and lowered tool call/output items
					// from the Codex fixture and verify the control payload stays 100% byte-identical.
					strippedCodex := stripLowerableToolsFromRawRequest(t, fix.Data)
					require.False(t, NeedsResponsesToolAdaptation(fnOnlyDialect, strippedCodex, nil))
					adaptedStripped, tmStripped, lossesStripped, err := AdaptResponsesTools(strippedCodex, fnOnlyDialect, nil)
					require.NoError(t, err)
					require.Nil(t, tmStripped)
					require.Empty(t, lossesStripped)
					require.True(t, bytes.Equal(strippedCodex, adaptedStripped),
						"Codex fixture %s without lowerable tools/items must pass through byte-for-byte unchanged", fix.Path)

					// Enrich the recorded Codex payload with unknown top-level 64-bit integer
					// fields, item-level prompt_cache_breakpoint, compaction, bare reasoning
					// encrypted_content, additional_tools, and custom tool_choice, then verify
					// full lossless preservation of all non-tool fields and untouched items.
					enriched := enrichRecordedCodexWithNativeExtensions(t, fix.Data)
					require.False(t, NeedsResponsesToolAdaptation(allToolsDialect, enriched, nil))
					enrichedAll, _, _, err := AdaptResponsesTools(enriched, allToolsDialect, nil)
					require.NoError(t, err)
					require.True(t, bytes.Equal(enriched, enrichedAll))

					enrichedAdapted, enrichedTM, _, err := AdaptResponsesTools(enriched, fnOnlyDialect, nil)
					require.NoError(t, err)
					require.NotNil(t, enrichedTM)
					assertOnlyToolsAdaptedInCorpusRequest(t, enriched, enrichedAdapted)
					require.Empty(t, testkit.ValidateResponsesRequest(enrichedAdapted, testkit.RequestValidationOptions{
						DialectRules: []string{"function-only"},
					}))
				}

				require.Empty(t, testkit.ValidateResponsesRequest(adapted, testkit.RequestValidationOptions{
					DialectRules: []string{"function-only"},
				}))

				// Verify IR lowerTools via Exchange does not leak tool_search execution:"client" Extra onto function tools.
				ex, err := NewExchange(fix.Data, Options{
					Route: Route{
						Client:   protocol.ProtocolOpenAIResponses,
						Upstream: protocol.ProtocolOpenAIResponses,
						Dialect:  fnOnlyDialect,
					},
					Runtime: newDeterministicRuntime(),
				})
				require.NoError(t, err)
				upIRBytes, err := ex.UpstreamRequest()
				require.NoError(t, err)
				require.NotContains(t, string(upIRBytes), `"execution":"client"`)
				require.Empty(t, testkit.ValidateResponsesRequest(upIRBytes, testkit.RequestValidationOptions{
					DialectRules: []string{"function-only"},
				}))
			}
		})
	}

	require.True(t, sawCodexText, "corpus scan must cover recorded Codex lab_text fixture")
	require.True(t, sawCodexTool, "corpus scan must cover recorded Codex lab_tool fixtures")
	require.True(t, sawCodexCustomTurn1, "corpus scan must cover recorded Codex lab_custom_tool Turn 1 fixture")
	require.True(t, sawCodexCustomTurn2, "corpus scan must cover recorded Codex lab_custom_tool Turn 2 fixture")
	require.True(t, sawPi, "corpus scan must cover recorded pi Responses fixtures")
}

// assertOnlyToolsAdaptedInCorpusRequest verifies that adapted preserves every
// top-level member outside tools, tool_choice, and input, every non-lowered
// tool declaration, and every non-lowered input[] item (including unknown
// fields, cache breakpoints, compaction items, and bare reasoning tokens)
// byte-for-byte and semantically compared with orig. Any structural mismatch,
// reordered member key, or altered numeric literal fails t immediately.
func assertOnlyToolsAdaptedInCorpusRequest(t *testing.T, orig, adapted []byte) {
	t.Helper()
	origMembers, err := parseOrderedObject(orig)
	require.NoError(t, err)
	adaptedMembers, err := parseOrderedObject(adapted)
	require.NoError(t, err)
	require.Equal(t, len(origMembers), len(adaptedMembers))

	for i := range origMembers {
		name := origMembers[i].name
		require.Equal(t, name, adaptedMembers[i].name)
		switch name {
		case "tools":
			assertNonLoweredToolElementsUnchanged(t, origMembers[i].value, adaptedMembers[i].value)
		case "tool_choice":
			// Scalar tool_choice values (such as "auto") and function choices targeting
			// non-lowered tools must remain byte-for-byte identical.
			if origMembers[i].value.Kind() != '{' {
				require.Equal(t,
					compactRawJSONString(origMembers[i].value),
					compactRawJSONString(adaptedMembers[i].value),
				)
			} else {
				tcObj, tcErr := parseOrderedObject(origMembers[i].value)
				require.NoError(t, tcErr)
				tcType, _ := readMemberString(tcObj, "type")
				if tcType != "custom" && tcType != "namespace" && tcType != "allowed_tools" {
					require.Equal(t,
						compactRawJSONString(origMembers[i].value),
						compactRawJSONString(adaptedMembers[i].value),
					)
				}
			}
		case "input":
			assertNonLoweredInputItemsUnchanged(t, origMembers[i].value, adaptedMembers[i].value)
		default:
			// Corpus fixtures on disk are pretty-printed with 2-space indentation while
			// jsontext.Encoder emits compact JSON; compacting insignificant whitespace
			// verifies exact member order, string values, and numeric token literals.
			require.Equal(t,
				compactRawJSONString(origMembers[i].value),
				compactRawJSONString(adaptedMembers[i].value),
				"unrelated top-level member %q changed content or key order", name,
			)
			diffs, diffErr := testkit.JSONDiff(origMembers[i].value, adaptedMembers[i].value)
			require.NoError(t, diffErr)
			require.Empty(t, diffs)
		}
	}
}

// assertNonLoweredToolElementsUnchanged verifies that every non-lowered tool
// declaration present at the prefix of origRaw retains exact member order,
// numeric tokens, and JSON semantics inside adaptedRaw. Flattened namespace
// slots advance adaptedIdx by their child count while promoted discoveries
// append after the original prefix.
func assertNonLoweredToolElementsUnchanged(t *testing.T, origRaw, adaptedRaw jsontext.Value) {
	t.Helper()
	origTools, err := parseRawArray(origRaw)
	require.NoError(t, err)
	adaptedTools, err := parseRawArray(adaptedRaw)
	require.NoError(t, err)
	// Promoted additional_tools or tool_search discoveries append after top-level tools.
	require.GreaterOrEqual(t, len(adaptedTools), len(origTools))

	adaptedIdx := 0
	for j := range origTools {
		tm, err := parseOrderedObject(origTools[j])
		require.NoError(t, err)
		typ, _ := readMemberString(tm, "type")
		if typ == "namespace" {
			children, hasCh := findRawMember(tm, "tools")
			if !hasCh {
				children, _ = findRawMember(tm, "children")
			}
			childElems, chErr := parseRawArray(children)
			require.NoError(t, chErr)
			adaptedIdx += len(childElems)
			continue
		}
		if typ != "custom" && typ != "tool_search" && readMemberBoolPtr(tm, "defer_loading") == nil {
			require.Equal(t,
				compactRawJSONString(origTools[j]),
				compactRawJSONString(adaptedTools[adaptedIdx]),
				"non-lowered tool[%d] (type=%s) must preserve exact members and key order", j, typ,
			)
			diffs, diffErr := testkit.JSONDiff(origTools[j], adaptedTools[adaptedIdx])
			require.NoError(t, diffErr)
			require.Empty(t, diffs)
		}
		adaptedIdx++
	}
}

// assertNonLoweredInputItemsUnchanged verifies that every input[] item other
// than additional_tools and lowered tool call or output items remains
// byte-for-byte and semantically identical between origRaw and adaptedRaw.
// Lowered tool call and output items must still preserve their call_id and all
// unrelated vendor extension members without failing t.
func assertNonLoweredInputItemsUnchanged(t *testing.T, origRaw, adaptedRaw jsontext.Value) {
	t.Helper()
	if origRaw.Kind() != '[' {
		require.Equal(t, compactRawJSONString(origRaw), compactRawJSONString(adaptedRaw))
		return
	}
	origItems, err := parseRawArray(origRaw)
	require.NoError(t, err)
	adaptedItems, err := parseRawArray(adaptedRaw)
	require.NoError(t, err)

	adaptedIdx := 0
	for idx, origItem := range origItems {
		m, err := parseOrderedObject(origItem)
		require.NoError(t, err)
		typ, _ := readMemberString(m, "type")
		if typ == "additional_tools" {
			// additional_tools declarations are lifted into top-level tools[] and removed from input[].
			continue
		}
		require.Less(t, adaptedIdx, len(adaptedItems), "missing adapted input item for orig input[%d]", idx)
		adaptedItem := adaptedItems[adaptedIdx]
		adaptedIdx++

		ns, hasNS := readMemberString(m, "namespace")
		isLoweredItem := typ == "custom_tool_call" ||
			typ == "custom_tool_call_output" ||
			typ == "tool_search_call" ||
			typ == "tool_search_output" ||
			(typ == "function_call" && hasNS && ns != "")

		if !isLoweredItem {
			require.Equal(t,
				compactRawJSONString(origItem),
				compactRawJSONString(adaptedItem),
				"non-lowered input[%d] (type=%s) changed content or key order", idx, typ,
			)
			diffs, diffErr := testkit.JSONDiff(origItem, adaptedItem)
			require.NoError(t, diffErr)
			require.Empty(t, diffs)
			continue
		}

		adaptedObj, err := parseOrderedObject(adaptedItem)
		require.NoError(t, err)
		origCallID, _ := readMemberString(m, "call_id")
		gotCallID, _ := readMemberString(adaptedObj, "call_id")
		require.Equal(t, origCallID, gotCallID, "lowered input[%d] must preserve call_id", idx)

		for _, member := range m {
			switch member.name {
			case "type", "id", "name", "namespace", "input", "arguments", "tools", "execution", "output":
				continue
			default:
				gotVal, ok := findRawMember(adaptedObj, member.name)
				require.True(t, ok, "lowered input[%d] lost unrelated member %q", idx, member.name)
				require.Equal(t, compactRawJSONString(member.value), compactRawJSONString(gotVal))
			}
		}
	}
	require.Equal(t, len(adaptedItems), adaptedIdx, "adapted input[] length mismatch")
}

// enrichRecordedCodexWithNativeExtensions returns a detached copy of a recorded
// Codex request augmented with an unknown top-level 64-bit integer field,
// prompt_cache_options, custom tool_choice, additional_tools, item-level
// prompt_cache_breakpoint, a compaction item, and a bare reasoning token item.
// Callers require orig to contain a non-empty array-shaped input[] field.
func enrichRecordedCodexWithNativeExtensions(t *testing.T, orig []byte) []byte {
	t.Helper()
	members, err := parseOrderedObject(orig)
	require.NoError(t, err)

	members = setRawMember(members, "x_vendor_trace_int64", jsontext.Value(`9007199254740993`))
	members = setRawMember(members, "prompt_cache_options", jsontext.Value(`{"ttl":"5m","slot":9007199254740995}`))
	members = setRawMember(members, "tool_choice", jsontext.Value(`{"type":"custom","name":"apply_patch"}`))

	rawInput, ok := findRawMember(members, "input")
	require.True(t, ok)
	items, err := parseRawArray(rawInput)
	require.NoError(t, err)
	require.NotEmpty(t, items)

	// Attach prompt_cache_breakpoint and an unknown 64-bit token onto input[0].
	firstObj, err := parseOrderedObject(items[0])
	require.NoError(t, err)
	firstObj = setRawMember(firstObj, "prompt_cache_breakpoint", jsontext.Value(`true`))
	firstObj = setRawMember(firstObj, "x_item_seq", jsontext.Value(`9007199254740997`))
	items[0], err = encodeOrderedObject(firstObj)
	require.NoError(t, err)

	extraItems := []jsontext.Value{
		jsontext.Value(`{"type":"additional_tools","tools":[{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}]}`),
		jsontext.Value(`{"type":"compaction","id":"cmp_recorded_1","encrypted_content":"cmp_opaque_state_blob","x_cmp_ver":2}`),
		jsontext.Value(`{"type":"reasoning","id":"rs_bare_recorded","summary":[{"type":"summary_text","text":"bare thought"}],"encrypted_content":"gAAAA_bare_recorded_token"}`),
		jsontext.Value(`{"type":"message","id":"msg_after_bare","role":"assistant","content":[{"type":"output_text","text":"checked"}],"prompt_cache_breakpoint":true}`),
	}
	items = slices.Insert(items, 1, extraItems...)
	encInput, err := encodeRawArray(items)
	require.NoError(t, err)
	members = setRawMember(members, "input", encInput)

	out, err := encodeOrderedObject(members)
	require.NoError(t, err)
	return out
}

// TestRestoreResponsesTools_IdempotentAfterStreamRestoration runs terminal SSE
// frames already restored by NewResponsesToolStream through a second pass of
// RestoreResponsesTools. It verifies that explicit non-default namespaces keep
// their function identities even when default functions or client tool_search
// proxies share the same tool name.
func TestRestoreResponsesTools_IdempotentAfterStreamRestoration(t *testing.T) {
	fnOnlyDialect := Dialect{
		Name:             "grok",
		Tools:            ToolSupportFunction,
		ToolNameMaxBytes: 64,
	}
	reqBody := []byte(`{
		"model":"grok-4.5",
		"tools":[
			{"type":"tool_search","execution":"client"},
			{"type":"namespace","name":"git","tools":[
				{"type":"function","name":"status","parameters":{"type":"object"}},
				{"type":"function","name":"tool_search","parameters":{"type":"object"}}
			]},
			{"type":"namespace","name":"functions","tools":[{"type":"function","name":"status","parameters":{"type":"object"}}]},
			{"type":"custom","name":"apply_patch"}
		],
		"input":"check"
	}`)
	_, tm, _, err := AdaptResponsesTools(reqBody, fnOnlyDialect, nil)
	require.NoError(t, err)
	require.NotNil(t, tm)
	require.True(t, tm.ToolSearch)

	stream := NewResponsesToolStream(tm)
	terminal := sse.Frame{
		Event: "response.completed",
		Data:  []byte(`{"type":"response.completed","sequence_number":0,"response":{"id":"resp_idem","status":"completed","output":[{"type":"function_call","id":"fc_git","call_id":"call_git","name":"git__status","arguments":"{}"},{"type":"function_call","id":"fc_git_ts","call_id":"call_git_ts","name":"git__tool_search","arguments":"{\"q\":\"blame\"}"},{"type":"function_call","id":"fc_ts","call_id":"call_ts","name":"tool_search","arguments":"{\"query\":\"git\"}"},{"type":"function_call","id":"fc_def","call_id":"call_def","namespace":"functions","name":"status","arguments":"{}"},{"type":"function_call","id":"fc_patch","call_id":"call_patch","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\"}"}]}}`),
	}
	frames, err := stream.Write(terminal)
	require.NoError(t, err)
	require.Len(t, frames, 1)

	secondPass, err := RestoreResponsesTools(frames[0].Data, tm)
	require.NoError(t, err)
	require.Equal(t, frames[0].Data, secondPass, "RestoreResponsesTools must be byte-for-byte idempotent on already-restored terminal output")
	require.Contains(t, string(secondPass), `"id":"fc_git_ts","call_id":"call_git_ts","name":"tool_search","arguments":"{\"q\":\"blame\"}","namespace":"git"`)
	require.Contains(t, string(secondPass), `"type":"tool_search_call"`)
	require.Contains(t, string(secondPass), `"type":"custom_tool_call"`)
}

// stripLowerableToolsFromRawRequest removes custom, namespace, tool_search, and
// defer_loading declarations from tools[] together with lowerable tool call or
// output items from input[] to construct a control Codex request requiring zero
// lowering. Payloads without tools[] or input[] arrays leave absent members unchanged.
func stripLowerableToolsFromRawRequest(t *testing.T, orig []byte) []byte {
	t.Helper()
	members, err := parseOrderedObject(orig)
	require.NoError(t, err)
	if rawTools, ok := findRawMember(members, "tools"); ok && rawTools.Kind() == '[' {
		elems, parseErr := parseRawArray(rawTools)
		require.NoError(t, parseErr)
		kept := slices.DeleteFunc(elems, func(el jsontext.Value) bool {
			if el.Kind() != '{' {
				return false
			}
			m, objErr := parseOrderedObject(el)
			require.NoError(t, objErr)
			typ, _ := readMemberString(m, "type")
			if typ == "custom" || typ == "tool_search" || typ == "namespace" {
				return true
			}
			return readMemberBoolPtr(m, "defer_loading") != nil
		})
		encTools, encErr := encodeRawArray(kept)
		require.NoError(t, encErr)
		members = setRawMember(members, "tools", encTools)
	}
	if rawInput, ok := findRawMember(members, "input"); ok && rawInput.Kind() == '[' {
		items, parseErr := parseRawArray(rawInput)
		require.NoError(t, parseErr)
		keptItems := slices.DeleteFunc(items, func(el jsontext.Value) bool {
			if el.Kind() != '{' {
				return false
			}
			m, objErr := parseOrderedObject(el)
			require.NoError(t, objErr)
			typ, _ := readMemberString(m, "type")
			switch typ {
			case "additional_tools", "custom_tool_call", "custom_tool_call_output",
				"tool_search_call", "tool_search_output":
				return true
			case "function_call":
				ns, hasNS := readMemberString(m, "namespace")
				return hasNS && ns != ""
			default:
				return false
			}
		})
		encInput, encErr := encodeRawArray(keptItems)
		require.NoError(t, encErr)
		members = setRawMember(members, "input", encInput)
	}
	out, err := encodeOrderedObject(members)
	require.NoError(t, err)
	return out
}

// extractUpstreamToolsAndCalls decodes encodedBytes for upProto and returns
// declared callable function or tool names in order alongside historical call
// records. Provider-hosted server tools such as web_search or googleSearch are
// skipped so checks cover callable declarations and history invocations.
func extractUpstreamToolsAndCalls(t *testing.T, upProto protocol.ProtocolID, encodedBytes []byte) ([]string, []toolCallNameRecord) {
	t.Helper()

	var root map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(encodedBytes, &root, jsontext.AllowInvalidUTF8(true)))

	var declared []string
	if rawTools, ok := root["tools"]; ok {
		var list []map[string]jsontext.Value
		require.NoError(t, json.Unmarshal(rawTools, &list, jsontext.AllowInvalidUTF8(true)))
		for _, item := range list {
			if upProto == protocol.ProtocolGeminiGenerateContent {
				if rawDecls, hasDecls := item["functionDeclarations"]; hasDecls {
					var decls []struct {
						Name string `json:"name"`
					}
					require.NoError(t, json.Unmarshal(rawDecls, &decls, jsontext.AllowInvalidUTF8(true)))
					for _, d := range decls {
						if d.Name != "" {
							declared = append(declared, d.Name)
						}
					}
				}
				continue
			}
			var typ, name string
			if rawT, hasT := item["type"]; hasT {
				_ = json.Unmarshal(rawT, &typ, jsontext.AllowInvalidUTF8(true))
			}
			if rawN, hasN := item["name"]; hasN {
				_ = json.Unmarshal(rawN, &name, jsontext.AllowInvalidUTF8(true))
			}
			if typ == "function" && name == "" {
				if rawFn, hasFn := item["function"]; hasFn {
					var fn struct {
						Name string `json:"name"`
					}
					_ = json.Unmarshal(rawFn, &fn, jsontext.AllowInvalidUTF8(true))
					name = fn.Name
				}
			}
			// Skip provider-hosted server tools such as web_search_20250305 or x_search so
			// callable tool declaration comparisons exclude upstream server capabilities.
			if strings.HasPrefix(typ, "web_search") || typ == "x_search" {
				continue
			}
			if name != "" {
				declared = append(declared, name)
			}
		}
	}

	var calls []toolCallNameRecord
	switch upProto {
	case protocol.ProtocolAnthropicMessages:
		var msgs []struct {
			Role    string           `json:"role"`
			Content []jsontext.Value `json:"content"`
		}
		if rawM, ok := root["messages"]; ok {
			_ = json.Unmarshal(rawM, &msgs, jsontext.AllowInvalidUTF8(true))
		}
		for _, m := range msgs {
			if m.Role != "assistant" {
				continue
			}
			for _, blk := range m.Content {
				var tu struct {
					Type string `json:"type"`
					ID   string `json:"id"`
					Name string `json:"name"`
				}
				if json.Unmarshal(blk, &tu, jsontext.AllowInvalidUTF8(true)) == nil && tu.Type == "tool_use" {
					calls = append(calls, toolCallNameRecord{CallID: tu.ID, Name: tu.Name})
				}
			}
		}

	case protocol.ProtocolOpenAIChatCompletions:
		var msgs []struct {
			Role      string `json:"role"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Function struct {
					Name string `json:"name"`
				} `json:"function"`
			} `json:"tool_calls"`
		}
		if rawM, ok := root["messages"]; ok {
			_ = json.Unmarshal(rawM, &msgs, jsontext.AllowInvalidUTF8(true))
		}
		for _, m := range msgs {
			for _, tc := range m.ToolCalls {
				calls = append(calls, toolCallNameRecord{CallID: tc.ID, Name: tc.Function.Name})
			}
		}

	case protocol.ProtocolOpenAIResponses:
		var items []struct {
			Type   string `json:"type"`
			CallID string `json:"call_id"`
			Name   string `json:"name"`
		}
		if rawIn, ok := root["input"]; ok {
			_ = json.Unmarshal(rawIn, &items, jsontext.AllowInvalidUTF8(true))
		}
		for _, it := range items {
			if it.Type == "function_call" || it.Type == "custom_tool_call" {
				calls = append(calls, toolCallNameRecord{CallID: it.CallID, Name: it.Name})
			}
		}

	case protocol.ProtocolGeminiGenerateContent:
		var contents []struct {
			Role  string `json:"role"`
			Parts []struct {
				FunctionCall *struct {
					ID   string `json:"id"`
					Name string `json:"name"`
				} `json:"functionCall"`
			} `json:"parts"`
		}
		if rawC, ok := root["contents"]; ok {
			_ = json.Unmarshal(rawC, &contents, jsontext.AllowInvalidUTF8(true))
		}
		for _, c := range contents {
			if c.Role != "model" {
				continue
			}
			for _, p := range c.Parts {
				if p.FunctionCall != nil {
					calls = append(calls, toolCallNameRecord{CallID: p.FunctionCall.ID, Name: p.FunctionCall.Name})
				}
			}
		}
	}

	return declared, calls
}

// TestHasResponsesToolAdaptation_ExcludesPlainFunctionWSInheritance verifies
// that HasResponsesToolAdaptation returns false for plain function-only session
// maps carried across WebSocket bridge turns and returns true when custom,
// namespace, tool_search, or historical input lowering occurs.
func TestHasResponsesToolAdaptation_ExcludesPlainFunctionWSInheritance(t *testing.T) {
	require.False(t, HasResponsesToolAdaptation(nil, nil))
	plainSessionMap := &ToolMap{
		FunctionTools: map[string]bool{"read_file": true},
		LoweredTools:  []jsontext.Value{jsontext.Value(`{"type":"function","name":"read_file"}`)},
	}
	require.False(t, HasResponsesToolAdaptation(plainSessionMap, nil))

	fnOnlyDialect := Dialect{Tools: ToolSupportFunction}
	turn2Plain := []byte(`{"type":"response.create","response":{"model":"gpt-5","input":[{"type":"message","role":"user","content":"hi"}]}}`)
	_, tmPlain, lossesPlain, err := AdaptResponsesTools(turn2Plain, fnOnlyDialect, plainSessionMap)
	require.NoError(t, err)
	require.False(t, HasResponsesToolAdaptation(tmPlain, lossesPlain))

	withCustom := []byte(`{"model":"gpt-5","tools":[{"type":"custom","name":"apply_patch"}],"input":"hi"}`)
	_, tmCustom, lossesCustom, err := AdaptResponsesTools(withCustom, fnOnlyDialect, nil)
	require.NoError(t, err)
	require.True(t, HasResponsesToolAdaptation(tmCustom, lossesCustom))

	withDefaultNS := []byte(`{"model":"gpt-5","tools":[{"type":"namespace","name":"functions","tools":[{"type":"function","name":"ls","parameters":{"type":"object"}}]}],"input":"hi"}`)
	_, tmNS, lossesNS, err := AdaptResponsesTools(withDefaultNS, fnOnlyDialect, nil)
	require.NoError(t, err)
	require.True(t, HasResponsesToolAdaptation(tmNS, lossesNS))
}
