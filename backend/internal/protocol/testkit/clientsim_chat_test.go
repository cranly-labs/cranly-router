package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestSimulateChatNextTurn folds streamed Chat Completions reasoning_content and
// tool_calls chunks across preserve_reasoning and drop_unknown variants. The
// preserved variant must satisfy deepseek-thinking validation, while drop_unknown
// must omit reasoning_content and still pass standard Chat validation.
func TestSimulateChatNextTurn(t *testing.T) {
	prevReq := []byte(`{
		"model": "deepseek-reasoner",
		"tools": [{"type": "function", "function": {"name": "read_doc"}}],
		"messages": [{"role": "user", "content": "Check docs"}]
	}`)
	frames := []sse.Frame{
		{Data: []byte(`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"deepseek-reasoner","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"Reading doc first"}}]}`)},
		{Data: []byte(`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"deepseek-reasoner","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"read_doc","arguments":"{\"id\":1}"}}]}}]}`)},
		{Data: []byte(`{"id":"c1","object":"chat.completion.chunk","created":1,"model":"deepseek-reasoner","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`)},
		{Data: []byte("[DONE]")},
	}

	preservedReq, err := SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, TurnInput{
		PrevRequest:              prevReq,
		ResponseFrames:           frames,
		SynthesizeMissingOutputs: true,
		Variant:                  SimVariantChatPreserveReasoning,
	})
	require.NoError(t, err)
	require.Empty(t, ValidateChatRequest(preservedReq, RequestValidationOptions{DialectRules: []string{"deepseek-thinking"}}))

	droppedReq, err := SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, TurnInput{
		PrevRequest:              prevReq,
		ResponseFrames:           frames,
		SynthesizeMissingOutputs: true,
		Variant:                  SimVariantChatDropUnknown,
	})
	require.NoError(t, err)
	require.Empty(t, ValidateChatRequest(droppedReq, RequestValidationOptions{}))
	require.NotContains(t, string(droppedReq), "reasoning_content")
}

// TestSimulateChatNextTurn_RefusalFoldsNullContent folds a refusal-only Chat
// Completions SSE stream alongside its buffered JSON counterpart. Both paths
// must keep assistant content as JSON null, preserve the refusal string, and
// produce zero semantic JSON differences on the next-turn request.
func TestSimulateChatNextTurn_RefusalFoldsNullContent(t *testing.T) {
	prevReq := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"Restricted request"}]}`)
	frames := []sse.Frame{
		{Data: []byte(`{"id":"c2","object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":null,"refusal":"I cannot "},"finish_reason":null}]}`)},
		{Data: []byte(`{"id":"c2","object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{"refusal":"comply."},"finish_reason":null}]}`)},
		{Data: []byte(`{"id":"c2","object":"chat.completion.chunk","created":1,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}`)},
		{Data: []byte("[DONE]")},
	}
	buffered := []byte(`{
		"id": "c2",
		"object": "chat.completion",
		"created": 1,
		"model": "gpt-4o",
		"choices": [{"index": 0, "message": {"role": "assistant", "content": null, "refusal": "I cannot comply."}, "finish_reason": "content_filter"}],
		"usage": {"prompt_tokens": 8, "completion_tokens": 4, "total_tokens": 12}
	}`)

	fromStream, err := SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, TurnInput{
		PrevRequest:      prevReq,
		ResponseFrames:   frames,
		FollowUpUserText: "Understood.",
	})
	require.NoError(t, err)
	require.Empty(t, ValidateChatRequest(fromStream, RequestValidationOptions{}))

	fromBuffered, err := SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, TurnInput{
		PrevRequest:      prevReq,
		ResponseBody:     buffered,
		FollowUpUserText: "Understood.",
	})
	require.NoError(t, err)
	diffs, diffErr := JSONDiff(fromStream, fromBuffered)
	require.NoError(t, diffErr)
	require.Empty(t, diffs)
}
