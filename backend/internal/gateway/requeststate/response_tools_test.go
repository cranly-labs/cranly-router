package requeststate

import (
	"encoding/json/jsontext"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
)

// TestResponseToolsSessionUpdateKeepsActiveTurn publishes active names, mutates
// the caller map, and concurrently replaces session names. Active output must
// retain its copied name while the session slot receives the new name and a
// separate request stays empty.
func TestResponseToolsSessionUpdateKeepsActiveTurn(t *testing.T) {
	state := &ResponseTools{}
	input := map[string]string{"alias": "active"}
	state.SetCodexNames(false, input)
	input["alias"] = "caller changed"
	var tasks sync.WaitGroup
	// Concurrent session publications exercise the separate slot while every task
	// reads the earlier immutable active map; wait for all publications before
	// checking the final session.
	for range 32 {
		tasks.Go(func() {
			state.SetCodexNames(true, map[string]string{"alias": "next"})
			require.Equal(t, "active", state.CodexNames(false)["alias"])
		})
	}
	tasks.Wait()
	require.Equal(t, "active", state.CodexNames(false)["alias"])
	require.Equal(t, "next", state.CodexNames(true)["alias"])
	other := &ResponseTools{}
	require.Nil(t, other.CodexNames(false))
}

// TestResponseToolsIRClientAndBridgeToolMapIsolation publishes OpenAI, Grok,
// and WebSocket HTTP-bridge IR ToolMaps, mutates caller and returned copies,
// and concurrently reads and updates slots. Each slot must remain isolated and
// clear cleanly when empty or nil maps are published.
func TestResponseToolsIRClientAndBridgeToolMapIsolation(t *testing.T) {
	var nilState *ResponseTools
	require.Nil(t, nilState.ClientToolMap(false))
	require.Nil(t, nilState.ClientToolMap(true))
	bridgeNil, presentNil := nilState.BridgeToolMap()
	require.Nil(t, bridgeNil)
	require.False(t, presentNil)
	nilState.SetClientToolMap(false, &convert.ToolMap{ToolSearch: true})
	nilState.SetBridgeToolMap(&convert.ToolMap{ToolSearch: true})

	state := &ResponseTools{}
	openAIMap := &convert.ToolMap{
		CustomTools: map[string]bool{"apply_patch": true},
	}
	grokMap := &convert.ToolMap{
		NamespaceTools: map[string]convert.NamespacedName{
			"git__status": {Namespace: "git", Name: "status"},
		},
	}
	wsMap := &convert.ToolMap{
		CustomTools:  map[string]bool{"exec": true},
		LoweredTools: []jsontext.Value{jsontext.Value(`{"type":"function","name":"exec"}`)},
	}

	state.SetClientToolMap(false, openAIMap)
	state.SetClientToolMap(true, grokMap)
	state.SetBridgeToolMap(wsMap)

	// Mutating caller maps after publication must not alter stored snapshots.
	openAIMap.CustomTools["injected"] = true
	grokMap.NamespaceTools["git__diff"] = convert.NamespacedName{Namespace: "git", Name: "diff"}
	wsMap.LoweredTools[0] = jsontext.Value(`{"type":"function","name":"tampered"}`)

	gotOA := state.ClientToolMap(false)
	require.NotNil(t, gotOA)
	require.True(t, gotOA.IsCustomTool("apply_patch"))
	require.False(t, gotOA.IsCustomTool("injected"))
	gotOA.CustomTools["mutated_after_read"] = true
	require.False(t, state.ClientToolMap(false).IsCustomTool("mutated_after_read"))

	gotGrok := state.ClientToolMap(true)
	require.NotNil(t, gotGrok)
	_, ok := gotGrok.LookupNamespaceTool("git__status")
	require.True(t, ok)
	_, leaked := gotGrok.LookupNamespaceTool("git__diff")
	require.False(t, leaked)

	gotWS, present := state.BridgeToolMap()
	require.True(t, present)
	require.NotNil(t, gotWS)
	require.JSONEq(t, `{"type":"function","name":"exec"}`, string(gotWS.LoweredTools[0]))

	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			_ = state.ClientToolMap(false)
			_ = state.ClientToolMap(true)
			_, _ = state.BridgeToolMap()
			state.SetBridgeToolMap(&convert.ToolMap{
				LoweredTools: []jsontext.Value{jsontext.Value(`{"type":"function","name":"ping"}`)},
			})
		})
	}
	wg.Wait()

	// Declaration-only maps without restorations clear single-turn ClientToolMap,
	// whereas explicit nil SetBridgeToolMap retains bridge presence for tools: [] resets.
	state.SetClientToolMap(false, &convert.ToolMap{
		LoweredTools: []jsontext.Value{jsontext.Value(`{"type":"function","name":"ping"}`)},
	})
	require.Nil(t, state.ClientToolMap(false))

	state.SetBridgeToolMap(nil)
	clearedWS, clearedPresent := state.BridgeToolMap()
	require.True(t, clearedPresent)
	require.Nil(t, clearedWS)
}
