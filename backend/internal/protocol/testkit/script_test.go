package testkit

import (
	"errors"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestScenarioLoadingAndTurnMatching loads scenarios from an in-memory FS and
// evaluates turn matchers against sidecar Haiku requests and tool-turn counts.
// Unmatched background requests must be skipped, while matching turns and
// JSONPath or callback checks must report accurate pass/fail results.
func TestScenarioLoadingAndTurnMatching(t *testing.T) {
	mapFS := fstest.MapFS{
		"scenarios/tool_single.json": &fstest.MapFile{
			Data: []byte(`{
				"description": "single tool invocation and replay",
				"capabilities": {
					"tools": true,
					"clients": ["anthropic_messages", "openai_responses"]
				},
				"request": {
					"model": "gpt-5",
					"messages": [{"role": "user", "parts": [{"kind": "text", "text": "ping"}]}]
				}
			}`),
		},
	}

	c := NewCorpus(mapFS)
	scenarios, err := c.LoadScenarios()
	require.NoError(t, err)
	require.Len(t, scenarios, 1)
	sc := scenarios[0]
	require.Equal(t, "tool_single", sc.Name)
	require.True(t, sc.SupportsRoute(protocol.ProtocolAnthropicMessages, protocol.ProtocolOpenAIChatCompletions))
	require.False(t, sc.SupportsRoute(protocol.ProtocolOpenAIChatCompletions, protocol.ProtocolAnthropicMessages))

	zero := 0
	one := 1
	needTools := true
	turn1 := Match{AssistantMessages: &zero, ToolResults: &zero, ModelPrefix: "gpt-", HasTools: &needTools}
	turn2 := Match{AssistantMessages: &one, ToolResults: &one, ContainsText: "done_token"}

	// Sidecar Haiku background request without tools must not match turn1.
	require.False(t, turn1.MatchesObserved(0, 0, "claude-haiku-4-5", false, []byte(`{"messages":[]}`)))
	require.True(t, turn1.MatchesObserved(0, 0, "gpt-5", true, []byte(`{"tools":[{}]}`)))
	require.True(t, turn2.MatchesObserved(1, 1, "gpt-5", true, []byte(`{"output":"done_token"}`)))

	chk := Check{JSONPath: "$.messages[0].role", Equals: "user"}
	require.NoError(t, chk.Evaluate(sc.Request, []byte(`{"messages":[{"role":"user"}]}`)))
	require.Error(t, chk.Evaluate(sc.Request, []byte(`{"messages":[{"role":"assistant"}]}`)))

	cbCheck := Check{Func: func(req *ir.Request, _ []byte) error {
		if req.Model != "gpt-5" {
			return errors.New("wrong model")
		}
		return nil
	}}
	require.NoError(t, cbCheck.Evaluate(sc.Request, nil))
}
