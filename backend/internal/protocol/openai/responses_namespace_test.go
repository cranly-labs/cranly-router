package openai

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

// TestFlattenResponsesNamespacesDefaultScopeRoundTrip lowers a default function
// declaration and its qualified history. The upstream uses one bare name and
// restored output omits namespace while retaining call correlation and arguments.
func TestFlattenResponsesNamespacesDefaultScopeRoundTrip(t *testing.T) {
	request := map[string]any{
		"tools": []any{map[string]any{"type": "namespace", "name": "functions", "tools": []any{map[string]any{"type": "function", "name": "wait"}}}},
		"input": []any{map[string]any{"type": "function_call", "name": "wait", "namespace": "functions", "call_id": "call_1", "arguments": "{}"}},
	}
	names, changed, err := FlattenResponsesNamespaces(request)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, ResponsesNamespaceName{Name: "wait"}, names["wait"])
	restored, _, err := RestoreResponsesNamespaceCalls([]byte(`{"output":[{"type":"function_call","name":"wait","namespace":"functions","call_id":"call_2","arguments":"{}"}]}`), names)
	require.NoError(t, err)
	require.JSONEq(t, `{"output":[{"type":"function_call","name":"wait","call_id":"call_2","arguments":"{}"}]}`, string(restored))
}

// TestFlattenResponsesNamespaces_RewritesDeclarationHistoryAndChoice flattens
// one namespace beside a plain function. It checks the returned identity pair,
// renamed declaration/call/choice, retained message metadata, and model value.
func TestFlattenResponsesNamespaces_RewritesDeclarationHistoryAndChoice(t *testing.T) {
	req := map[string]any{
		"model": "gpt-5.5",
		"tools": []any{
			map[string]any{"type": "function", "name": "plain", "description": "keep"},
			map[string]any{
				"type": "namespace",
				"name": "collaboration",
				"tools": []any{
					map[string]any{"type": "function", "name": "spawn_agent", "description": "spawn", "parameters": map[string]any{"type": "object"}},
				},
			},
		},
		"tool_choice": map[string]any{"type": "function", "name": "spawn_agent", "namespace": "collaboration"},
		"input": []any{
			map[string]any{"type": "function_call", "call_id": "call_1", "name": "spawn_agent", "namespace": "collaboration", "arguments": "{}"},
			map[string]any{"type": "message", "role": "user", "content": "hi", "name": "spawn_agent", "namespace": "collaboration"},
		},
	}

	names, changed, err := FlattenResponsesNamespaces(req)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, ResponsesNamespaceName{Namespace: "collaboration", Name: "spawn_agent"}, names["collaboration__spawn_agent"])

	tools, ok := req["tools"].([]any)
	require.True(t, ok)
	require.Len(t, tools, 2)
	plainTool, ok := tools[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "plain", plainTool["name"])
	flatTool, ok := tools[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "collaboration__spawn_agent", flatTool["name"])
	require.Equal(t, "spawn", flatTool["description"])

	choice, ok := req["tool_choice"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "collaboration__spawn_agent", choice["name"])
	require.NotContains(t, choice, "namespace")

	input, ok := req["input"].([]any)
	require.True(t, ok)
	require.Len(t, input, 2)
	call, ok := input[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "collaboration__spawn_agent", call["name"])
	require.NotContains(t, call, "namespace")
	message, ok := input[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "spawn_agent", message["name"])
	require.Equal(t, "collaboration", message["namespace"])
	require.Equal(t, "gpt-5.5", req["model"])
}

// TestFlattenResponsesNamespaces_RejectsFlatNameCollision supplies a namespace
// child whose joined name matches a top-level function. Conversion must return
// an error identifying that top-level conflict.
func TestFlattenResponsesNamespaces_RejectsFlatNameCollision(t *testing.T) {
	req := map[string]any{"tools": []any{
		map[string]any{"type": "function", "name": "collaboration__spawn_agent"},
		map[string]any{"type": "namespace", "name": "collaboration", "tools": []any{
			map[string]any{"type": "function", "name": "spawn_agent"},
		}},
	}}

	_, _, err := FlattenResponsesNamespaces(req)
	require.ErrorContains(t, err, "conflicts with a top-level tool")
}

// TestFlattenResponsesNamespaces_NamespaceGroupChoiceFallsBackToAuto selects a
// namespace containing two functions. Flattening reports a change and replaces
// the removed group choice with the auto string.
func TestFlattenResponsesNamespaces_NamespaceGroupChoiceFallsBackToAuto(t *testing.T) {
	req := map[string]any{
		"tools": []any{map[string]any{
			"type": "namespace", "name": "collaboration", "tools": []any{
				map[string]any{"type": "function", "name": "spawn_agent"},
				map[string]any{"type": "function", "name": "send_message"},
			},
		}},
		"tool_choice": map[string]any{"type": "namespace", "name": "collaboration"},
	}

	_, changed, err := FlattenResponsesNamespaces(req)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "auto", req["tool_choice"])
}

// TestFlattenResponsesNamespacesExcept_PreservesBuiltInNamespaceAndChoice
// exempts image_gen while flattening another namespace. It checks both
// resulting declarations, the returned mapping, and the unchanged choice of
// the exempt group.
func TestFlattenResponsesNamespacesExcept_PreservesBuiltInNamespaceAndChoice(t *testing.T) {
	req := map[string]any{
		"tools": []any{
			map[string]any{"type": "namespace", "name": "image_gen", "tools": []any{
				map[string]any{"type": "function", "name": "imagegen"},
			}},
			map[string]any{"type": "namespace", "name": "collaboration", "tools": []any{
				map[string]any{"type": "function", "name": "spawn_agent"},
			}},
		},
		"tool_choice": map[string]any{"type": "namespace", "name": "image_gen"},
	}

	names, changed, err := FlattenResponsesNamespacesExcept(req, map[string]bool{"image_gen": true})
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, names, "collaboration__spawn_agent")
	tools, ok := req["tools"].([]any)
	require.True(t, ok)
	require.Len(t, tools, 2)
	preservedTool, ok := tools[0].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "namespace", preservedTool["type"])
	require.Equal(t, "image_gen", preservedTool["name"])
	flatTool, ok := tools[1].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "function", flatTool["type"])
	require.Equal(t, "collaboration__spawn_agent", flatTool["name"])
	require.Equal(t, map[string]any{"type": "namespace", "name": "image_gen"}, req["tool_choice"])
}

// TestFlattenResponsesNamespaces_RejectsNamespaceCollision supplies two
// different namespace/name pairs that join to the same flat name. Conversion
// must return an error identifying the ambiguous joined identity.
func TestFlattenResponsesNamespaces_RejectsNamespaceCollision(t *testing.T) {
	req := map[string]any{"tools": []any{
		map[string]any{"type": "namespace", "name": "a", "tools": []any{
			map[string]any{"type": "function", "name": "b__c"},
		}},
		map[string]any{"type": "namespace", "name": "a__b", "tools": []any{
			map[string]any{"type": "function", "name": "c"},
		}},
	}}

	_, _, err := FlattenResponsesNamespaces(req)
	require.ErrorContains(t, err, "both flatten")
}

// TestFlattenResponsesNamespaces_TruncatesOverlongNameWithHashSuffix supplies a
// namespace and child function whose joined name exceeds the 64-byte limit.
// Flattening must bound the emitted name with a deterministic hash suffix, and
// call restoration must recover both full original strings.
func TestFlattenResponsesNamespaces_TruncatesOverlongNameWithHashSuffix(t *testing.T) {
	longNS := strings.Repeat("enterprise_namespace_", 2)
	longChild := "execute_very_long_diagnostic_query"
	req := map[string]any{
		"tools": []any{map[string]any{
			"type":     "namespace",
			"name":     longNS,
			"children": []any{map[string]any{"type": "function", "name": longChild}},
		}},
	}

	names, changed, err := FlattenResponsesNamespaces(req)
	require.NoError(t, err)
	require.True(t, changed)
	require.Len(t, names, 1)

	var flat string
	for k, v := range names {
		flat = k
		require.LessOrEqual(t, len(k), 64)
		require.Equal(t, ResponsesNamespaceName{Namespace: longNS, Name: longChild}, v)
	}

	restored, didRestore, err := RestoreResponsesNamespaceCalls(
		[]byte(`{"type":"response.output_item.done","item":{"type":"function_call","name":"`+flat+`","arguments":"{}"}}`),
		names,
	)
	require.NoError(t, err)
	require.True(t, didRestore)
	require.Equal(t, longChild, gjson.GetBytes(restored, "item.name").String())
	require.Equal(t, longNS, gjson.GetBytes(restored, "item.namespace").String())
}

// TestRestoreResponsesNamespaceCalls_RewritesOnlyFunctionCalls restores one
// mapped function in a mixed output list. JSON equality checks other
// calls/message data, and literal HTML characters remain unescaped.
func TestRestoreResponsesNamespaceCalls_RewritesOnlyFunctionCalls(t *testing.T) {
	payload := []byte(`{"type":"response.completed","response":{"output":[{"type":"function_call","name":"collaboration__spawn_agent","call_id":"call_1","arguments":"{}","extra":"keep"},{"type":"function_call","name":"plain","arguments":"{}"},{"type":"message","name":"collaboration__spawn_agent","content":"<tag>&value</tag>"}]}}`)
	names := map[string]ResponsesNamespaceName{
		"collaboration__spawn_agent": {Namespace: "collaboration", Name: "spawn_agent"},
	}

	got, changed, err := RestoreResponsesNamespaceCalls(payload, names)
	require.NoError(t, err)
	require.True(t, changed)
	require.JSONEq(t, `{"type":"response.completed","response":{"output":[{"type":"function_call","name":"spawn_agent","namespace":"collaboration","call_id":"call_1","arguments":"{}","extra":"keep"},{"type":"function_call","name":"plain","arguments":"{}"},{"type":"message","name":"collaboration__spawn_agent","content":"<tag>&value</tag>"}]}}`, string(got))
	require.Contains(t, string(got), "<tag>&value</tag>")
	require.NotContains(t, string(got), `\u003c`)
}

// TestRestoreResponsesNamespaceCalls_RewritesLifecycleItems supplies added and
// done lifecycle payloads with a mapped flat function name. Both decoded
// shapes must restore the child name and its namespace.
func TestRestoreResponsesNamespaceCalls_RewritesLifecycleItems(t *testing.T) {
	for _, eventType := range []string{"response.output_item.added", "response.output_item.done"} {
		t.Run(eventType, func(t *testing.T) {
			payload := []byte(`{"type":"` + eventType + `","item":{"type":"function_call","name":"collaboration__spawn_agent","arguments":"{}"}}`)
			got, changed, err := RestoreResponsesNamespaceCalls(payload, map[string]ResponsesNamespaceName{
				"collaboration__spawn_agent": {Namespace: "collaboration", Name: "spawn_agent"},
			})
			require.NoError(t, err)
			require.True(t, changed)
			require.JSONEq(t, `{"type":"`+eventType+`","item":{"type":"function_call","name":"spawn_agent","namespace":"collaboration","arguments":"{}"}}`, string(got))
		})
	}
}

// TestStripOpenAIResponsesInputNamespaces_SelectiveAndFullCleanup supplies
// mixed-case tool calls, user messages, nested content objects, and 64-bit
// integers inside a Responses request body. Selective mode must retain call
// namespaces while full mode removes all direct input namespaces without
// altering nested fields or integer precision.
func TestStripOpenAIResponsesInputNamespaces_SelectiveAndFullCleanup(t *testing.T) {
	body := []byte(`{` +
		`"meta":9007199254740993,` +
		`"tools":[{"type":"namespace","name":"team","namespace":"keep-decl"}],` +
		`"input":[` +
		`{"type":" Function_Call ","namespace":"team","name":"send","large":9007199254740993},` +
		`{"type":"custom_tool_call","namespace":"exec_ns","name":"patch"},` +
		`{"type":"message","role":"user","namespace":"drop_msg","content":{"namespace":"keep_nested"}}` +
		`]` +
		`}`)

	keptCalls, err := StripOpenAIResponsesInputNamespaces(body, true)
	require.NoError(t, err)
	require.Equal(t, "team", gjson.GetBytes(keptCalls, "input.0.namespace").String())
	require.Equal(t, "exec_ns", gjson.GetBytes(keptCalls, "input.1.namespace").String())
	require.False(t, gjson.GetBytes(keptCalls, "input.2.namespace").Exists())
	require.Equal(t, "keep_nested", gjson.GetBytes(keptCalls, "input.2.content.namespace").String())
	require.Equal(t, "keep-decl", gjson.GetBytes(keptCalls, "tools.0.namespace").String())
	require.Equal(t, "9007199254740993", gjson.GetBytes(keptCalls, "meta").Raw)
	require.Equal(t, "9007199254740993", gjson.GetBytes(keptCalls, "input.0.large").Raw)

	strippedAll, err := StripOpenAIResponsesInputNamespaces(body, false)
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(strippedAll, "input.0.namespace").Exists())
	require.False(t, gjson.GetBytes(strippedAll, "input.1.namespace").Exists())
	require.False(t, gjson.GetBytes(strippedAll, "input.2.namespace").Exists())
}
