package ir

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLinkResolvesFunctionCustomSearchAndNamespacedCalls runs Link on a
// multi-turn Request containing function, namespaced, custom, client_search,
// out-of-order, reused-ID, and orphan tool results. Each matched result must
// receive its nearest preceding ToolRef while unmatched results stay untouched.
func TestLinkResolvesFunctionCustomSearchAndNamespacedCalls(t *testing.T) {
	Link(nil)
	Link(&Request{})

	req := &Request{
		Model: "gpt-5",
		Messages: []Message{
			{
				// Out-of-order result before its call falls back to the request call table.
				Role: RoleTool,
				Parts: []Part{{
					Kind:   PartToolResult,
					Result: &ToolResult{CallID: "call_forward", Content: []Part{{Kind: PartText, Text: "late"}}},
				}},
			},
			{
				Role: RoleAssistant,
				Parts: []Part{
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "call_shared",
							Tool:      ToolRef{Kind: ToolCustom, Name: "apply_patch"},
							Arguments: "*** Begin Patch",
						},
					},
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "call_ns",
							Tool:      ToolRef{Kind: ToolFunction, Namespace: "pager", Name: "list_incidents"},
							Arguments: `{"severity":"sev1"}`,
						},
					},
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "call_search",
							Tool:      ToolRef{Kind: ToolClientSearch, Name: "tool_search"},
							Arguments: `{"query":"pager"}`,
						},
					},
				},
			},
			{
				Role: RoleTool,
				Parts: []Part{
					{
						Kind:   PartToolResult,
						Result: &ToolResult{CallID: "call_shared", Content: []Part{{Kind: PartText, Text: "patched"}}},
					},
					{
						Kind:   PartToolResult,
						Result: &ToolResult{CallID: "call_ns", Content: []Part{{Kind: PartText, Text: "[]"}}},
					},
					{
						Kind:   PartToolResult,
						Result: &ToolResult{CallID: "call_search", Content: []Part{{Kind: PartText, Text: "found"}}},
					},
					{
						Kind:   PartToolResult,
						Result: &ToolResult{CallID: "call_orphan", Content: []Part{{Kind: PartText, Text: "unmatched"}}},
					},
				},
			},
			{
				// Reuse call_shared in a later turn with a different tool identity.
				Role: RoleAssistant,
				Parts: []Part{
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "call_shared",
							Tool:      ToolRef{Kind: ToolFunction, Name: "run_tests"},
							Arguments: `{}`,
						},
					},
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "call_forward",
							Tool:      ToolRef{Kind: ToolFunction, Name: "deferred_fn"},
							Arguments: `{}`,
						},
					},
				},
			},
			{
				Role: RoleTool,
				Parts: []Part{{
					Kind:   PartToolResult,
					Result: &ToolResult{CallID: "call_shared", Content: []Part{{Kind: PartText, Text: "passed"}}},
				}},
			},
		},
	}

	Link(req)

	require.Equal(t, ToolRef{Kind: ToolFunction, Name: "deferred_fn"}, req.Messages[0].Parts[0].Result.Tool)
	require.Equal(t, ToolRef{Kind: ToolCustom, Name: "apply_patch"}, req.Messages[2].Parts[0].Result.Tool)
	require.Equal(t, ToolRef{Kind: ToolFunction, Namespace: "pager", Name: "list_incidents"}, req.Messages[2].Parts[1].Result.Tool)
	require.Equal(t, ToolRef{Kind: ToolClientSearch, Name: "tool_search"}, req.Messages[2].Parts[2].Result.Tool)
	require.Equal(t, ToolRef{}, req.Messages[2].Parts[3].Result.Tool)
	require.Equal(t, ToolRef{Kind: ToolFunction, Name: "run_tests"}, req.Messages[4].Parts[0].Result.Tool)
}
