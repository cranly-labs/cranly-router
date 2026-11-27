package messages

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestEncodeRequestDialectReasoningSamplingAndTools verifies EncodeRequest across
// ultra effort rejection, max effort preservation when ControlsKnown is false,
// Codex effort="none" disabling, budget clamping with max_tokens headroom,
// sampling/stop drops, and custom/search/namespace tool lowering. Every lowered
// or dropped parameter must reconcile cleanly under testkit.VerifyEssence.
func TestEncodeRequestDialectReasoningSamplingAndTools(t *testing.T) {
	_, _, err := EncodeRequest(nil, ir.Dialect{})
	require.Error(t, err)

	base := &ir.Request{
		Model:    "claude-sonnet-4-5",
		Messages: []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "ping"}}}},
	}

	// "ultra" effort must fail with a validation error.
	ultraReq := base.Clone()
	ultraReq.Reasoning = &ir.ReasoningConfig{Effort: "ultra"}
	_, _, err = EncodeRequest(ultraReq, ir.Dialect{})
	require.Error(t, err)

	// ControlsKnown=false preserves "max" in output_config.effort.
	maxReq := base.Clone()
	maxReq.Reasoning = &ir.ReasoningConfig{Effort: "max"}
	maxBytes, _, err := EncodeRequest(maxReq, ir.Dialect{})
	require.NoError(t, err)
	require.Contains(t, string(maxBytes), `"effort":"max"`)

	// Effort "none" maps to thinking={"type":"disabled"} rather than budget 10240.
	noneReq := base.Clone()
	noneReq.Reasoning = &ir.ReasoningConfig{Effort: "none"}
	noneBytes, _, err := EncodeRequest(noneReq, ir.Dialect{})
	require.NoError(t, err)
	require.Contains(t, string(noneBytes), `"thinking":{"type":"disabled"}`)
	require.NotContains(t, string(noneBytes), `"budget_tokens"`)
	require.Empty(t, testkit.ValidateMessagesRequest(noneBytes, testkit.RequestValidationOptions{StrictTopLevel: true}))

	// Budget-only dialect converts high effort (10240) and raises max_tokens (4096 -> 14336).
	smallMax := int64(4096)
	highBudgetReq := base.Clone()
	highBudgetReq.MaxOutputTokens = &smallMax
	highBudgetReq.Reasoning = &ir.ReasoningConfig{Effort: "high"}
	highBytes, _, err := EncodeRequest(highBudgetReq, ir.Dialect{
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Budget:        true,
			BudgetMin:     2048,
			BudgetMax:     16000,
		},
	})
	require.NoError(t, err)
	require.Contains(t, string(highBytes), `"budget_tokens":10240`)
	require.Contains(t, string(highBytes), `"max_tokens":14336`)
	require.Empty(t, testkit.ValidateMessagesRequest(highBytes, testkit.RequestValidationOptions{StrictTopLevel: true}))

	// Sampling/stop drops, ultrafast -> speed:"fast", tool lowering, and structured output losses.
	temp := 0.5
	topP := 0.8
	topK := int64(20)
	seed := int64(42)
	freq := 0.1
	pres := 0.2
	cand := int64(3)
	strictTrue := true

	richReq := &ir.Request{
		Model:       "claude-sonnet-4-5",
		ServiceTier: "ultrafast",
		UserID:      "u_fallback",
		Sampling: ir.Sampling{
			Temperature:      &temp,
			TopP:             &topP,
			TopK:             &topK,
			Seed:             &seed,
			FrequencyPenalty: &freq,
			PresencePenalty:  &pres,
			CandidateCount:   &cand,
		},
		Stop: []string{"END"},
		Output: &ir.OutputFormat{
			Kind:        "json_schema",
			Name:        "eval_schema",
			Description: "Schema desc",
			Strict:      &strictTrue,
			Verbosity:   "low",
			Schema:      jsontext.Value(`{"type":"object","properties":{"v":{"type":"string"}}}`),
		},
		Tools: []ir.Tool{
			{Kind: ir.ToolHosted, Name: "web_search", Hosted: &ir.OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: "web_search", Raw: jsontext.Value(`{"type":"web_search"}`)}},
			{Kind: ir.ToolCustom, Namespace: "dev", Name: "apply_patch", Description: "Apply patch"},
			{Kind: ir.ToolClientSearch, Name: "tool_search"},
			{Kind: ir.ToolFunction, Name: "no_schema_fn"},
		},
		ToolChoice: &ir.ToolChoice{
			Mode:         ir.ToolChoiceRequired,
			AllowedTools: []ir.ToolRef{{Kind: ir.ToolCustom, Namespace: "dev", Name: "apply_patch"}},
		},
		Messages: []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "run"}}}},
	}

	encoded, losses, err := EncodeRequest(richReq, ir.Dialect{
		DropTemperature:   true,
		DropTopP:          true,
		DropTopK:          true,
		OmitStopSequences: true,
	})
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(encoded, testkit.RequestValidationOptions{StrictTopLevel: true}))
	require.Contains(t, string(encoded), `"speed":"fast"`)
	require.Contains(t, string(encoded), `"metadata":{"user_id":"u_fallback"}`)
	require.Contains(t, string(encoded), `"dev__apply_patch"`)
	require.Contains(t, string(encoded), `"tool_search"`)

	redecoded, err := DecodeRequest(encoded)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.Essence(richReq), ir.Essence(redecoded), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}

// TestEncodeRequestThinkingModesAndSingleProtocolLawQ2 exercises enabled
// thinking resolution across unknown-control models, dual budget-and-effort
// models, and effort-only adaptive models alongside single-protocol Law Q2
// round-trips. Enabled modes must emit bounded budget_tokens or adaptive
// thinking without inflating max_tokens on effort-only models, while native
// Messages output_config.effort without thinking remains unchanged.
func TestEncodeRequestThinkingModesAndSingleProtocolLawQ2(t *testing.T) {
	yes := true
	base := &ir.Request{
		Model:    "claude-opus-4-6",
		Messages: []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "ping"}}}},
	}

	// 1. ControlsKnown == false with Mode="enabled" emits bounded budget_tokens + effort.
	for _, tc := range []struct {
		effort     string
		wantBudget string
		wantMax    string
	}{
		{effort: "medium", wantBudget: `"budget_tokens":4096`, wantMax: `"max_tokens":8192`},
		{effort: "high", wantBudget: `"budget_tokens":10240`, wantMax: `"max_tokens":14336`},
		{effort: "xhigh", wantBudget: `"budget_tokens":32768`, wantMax: `"max_tokens":36864`},
		{effort: "max", wantBudget: `"budget_tokens":32768`, wantMax: `"max_tokens":36864`},
	} {
		req := base.Clone()
		req.Reasoning = &ir.ReasoningConfig{Mode: "enabled", Effort: tc.effort}
		encoded, losses, err := EncodeRequest(req, ir.Dialect{})
		require.NoError(t, err, tc.effort)
		require.Nil(t, losses, tc.effort)
		require.Empty(t, testkit.ValidateMessagesRequest(encoded, testkit.RequestValidationOptions{StrictTopLevel: true}), tc.effort)
		require.Contains(t, string(encoded), `"type":"enabled"`, tc.effort)
		require.Contains(t, string(encoded), tc.wantBudget, tc.effort)
		require.Contains(t, string(encoded), tc.wantMax, tc.effort)
		require.Contains(t, string(encoded), `"effort":"`+tc.effort+`"`, tc.effort)
	}

	// 2. Dual Budget + Efforts (claude-opus-4-6 / claude-sonnet-4-6) emits both budget thinking and effort when Mode="enabled".
	dualDialect := ir.Dialect{
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Supported:     &yes,
			Budget:        true,
			BudgetMin:     1024,
			Efforts:       []string{"low", "medium", "high", "max"},
		},
	}
	dualEnabled := base.Clone()
	dualEnabled.Reasoning = &ir.ReasoningConfig{Mode: "enabled", Effort: "xhigh"}
	dualBytes, _, err := EncodeRequest(dualEnabled, dualDialect)
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(dualBytes, testkit.RequestValidationOptions{StrictTopLevel: true}))
	require.Contains(t, string(dualBytes), `"thinking":{"type":"enabled","budget_tokens":32768}`)
	require.Contains(t, string(dualBytes), `"output_config":{"effort":"high"}`)

	// Single-protocol Mode="" on dual Budget + Efforts keeps thinking absent.
	dualEffortOnly := base.Clone()
	dualEffortOnly.Reasoning = &ir.ReasoningConfig{Effort: "medium"}
	dualEffortOnlyBytes, _, err := EncodeRequest(dualEffortOnly, dualDialect)
	require.NoError(t, err)
	require.NotContains(t, string(dualEffortOnlyBytes), `"thinking"`)
	require.Contains(t, string(dualEffortOnlyBytes), `"output_config":{"effort":"medium"}`)

	// 3. Effort-only adaptive models (claude-opus-4-7 / claude-sonnet-5) emit thinking={"type":"adaptive"} without budget_tokens or max_tokens inflation.
	adaptiveDialect := ir.Dialect{
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Supported:     &yes,
			Toggle:        true,
			Efforts:       []string{"low", "medium", "high", "xhigh", "max"},
		},
	}
	cap4096 := int64(4096)
	adaptiveReq := base.Clone()
	adaptiveReq.Model = "claude-opus-4-7"
	adaptiveReq.MaxOutputTokens = &cap4096
	adaptiveReq.Reasoning = &ir.ReasoningConfig{Mode: "enabled", Effort: "high"}
	adaptiveBytes, _, err := EncodeRequest(adaptiveReq, adaptiveDialect)
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(adaptiveBytes, testkit.RequestValidationOptions{StrictTopLevel: true}))
	require.Contains(t, string(adaptiveBytes), `"thinking":{"type":"adaptive"}`)
	require.NotContains(t, string(adaptiveBytes), `"budget_tokens"`)
	require.Contains(t, string(adaptiveBytes), `"max_tokens":4096`)
	require.Contains(t, string(adaptiveBytes), `"output_config":{"effort":"high"}`)

	// Explicit client token budget on an effort-only adaptive model maps onto effort alongside adaptive thinking.
	budget4096 := int64(4096)
	adaptiveFromBudget := base.Clone()
	adaptiveFromBudget.Model = "claude-sonnet-5"
	adaptiveFromBudget.Reasoning = &ir.ReasoningConfig{Mode: "enabled", BudgetTokens: &budget4096}
	adaptiveFromBudgetBytes, _, err := EncodeRequest(adaptiveFromBudget, adaptiveDialect)
	require.NoError(t, err)
	require.Empty(t, testkit.ValidateMessagesRequest(adaptiveFromBudgetBytes, testkit.RequestValidationOptions{StrictTopLevel: true}))
	require.Contains(t, string(adaptiveFromBudgetBytes), `"thinking":{"type":"adaptive"}`)
	require.Contains(t, string(adaptiveFromBudgetBytes), `"output_config":{"effort":"medium"}`)

	// 4. Budget-only claude-haiku-4-5 produces byte-for-byte identical wire output for Mode="" and Mode="enabled".
	haikuDialect := ir.Dialect{
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Supported:     &yes,
			Budget:        true,
			BudgetMin:     1024,
		},
	}
	haikuPre := base.Clone()
	haikuPre.Model = "claude-haiku-4-5"
	haikuPre.Reasoning = &ir.ReasoningConfig{Effort: "medium"}
	haikuPost := haikuPre.Clone()
	haikuPost.Reasoning.Mode = "enabled"
	haikuPreBytes, _, err := EncodeRequest(haikuPre, haikuDialect)
	require.NoError(t, err)
	haikuPostBytes, _, err := EncodeRequest(haikuPost, haikuDialect)
	require.NoError(t, err)
	require.Equal(t, haikuPreBytes, haikuPostBytes)

	// 5. Single-protocol Messages Law Q2 fixed point with output_config.effort and no thinking.
	for _, effort := range []string{"low", "medium", "high", "max"} {
		wireBody := []byte(`{"model":"claude-opus-4-6","max_tokens":2048,"output_config":{"effort":"` + effort + `"},"messages":[{"role":"user","content":"ping"}]}`)
		dec1, decErr := DecodeRequest(wireBody)
		require.NoError(t, decErr, effort)
		reencoded, q2Losses, encErr := EncodeRequest(dec1, ir.Dialect{})
		require.NoError(t, encErr, effort)
		require.Nil(t, q2Losses, effort)
		require.NotContains(t, string(reencoded), `"thinking"`, effort)
		dec2, dec2Err := DecodeRequest(reencoded)
		require.NoError(t, dec2Err, effort)
		require.Equal(t, dec1, dec2, "Law Q2 violated for native Messages effort=%s", effort)
	}
}
