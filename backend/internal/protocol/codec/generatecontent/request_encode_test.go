package generatecontent

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestEncodeRequestThinkingResolutionHeadroomAndMixedTools encodes a request
// combining positive thinking budgets, cramped maxOutputTokens, dialect-dropped
// sampling controls, and mixed client/hosted tools. EncodeRequest must pad
// maxOutputTokens above thinkingBudget, set includeServerSideToolInvocations,
// and pass strict antigravity-schema validation.
func TestEncodeRequestThinkingResolutionHeadroomAndMixedTools(t *testing.T) {
	maxOut := int64(1024)
	budget := int64(2048)
	temp := 0.7
	topP := 0.95
	topK := int64(32)
	outLimit := 8192

	req := &ir.Request{
		MaxOutputTokens: &maxOut,
		Sampling: ir.Sampling{
			Temperature: &temp,
			TopP:        &topP,
			TopK:        &topK,
		},
		Stop: []string{"<STOP>"},
		Reasoning: &ir.ReasoningConfig{
			Mode:         "enabled",
			BudgetTokens: &budget,
		},
		Output: &ir.OutputFormat{
			Kind:   "json_schema",
			Schema: jsontext.Value(`{"type":"object","properties":{"ok":{"type":"boolean"}},"additionalProperties":false}`),
		},
		Tools: []ir.Tool{
			{
				Kind:        ir.ToolFunction,
				Name:        "query_metrics",
				Description: "Query metric series",
				InputSchema: jsontext.Value(`{"type":"object","properties":{"metric":{"type":"string","minLength":2}},"required":["metric"]}`),
			},
			{
				Kind: ir.ToolHosted,
				Name: "web_search_20250305",
				Hosted: &ir.OpaqueItem{
					Origin: protocol.ProtocolAnthropicMessages,
					Type:   "web_search_20250305",
					Raw:    jsontext.Value(`{"type":"web_search_20250305","name":"web_search"}`),
				},
			},
		},
		Messages: []ir.Message{
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Check metrics and search"}}},
		},
	}

	dialect := ir.Dialect{
		Tools:             ir.ToolSupportFunction | ir.ToolSupportHostedWebSearch,
		SchemaDialect:     ir.SchemaAntigravity,
		DropTemperature:   true,
		DropTopP:          true,
		DropTopK:          true,
		OmitStopSequences: true,
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Budget:        true,
			ForceBudget:   true,
			BudgetMin:     1024,
			BudgetMax:     16384,
			BudgetPadding: 2048,
			OutputLimit:   &outLimit,
		},
	}

	encoded, losses, err := EncodeRequest(req, dialect)
	require.NoError(t, err)
	require.NotEmpty(t, losses)

	violations := testkit.ValidateGeminiRequest(encoded, testkit.RequestValidationOptions{
		DialectRules: []string{"antigravity-schema"},
	})
	require.Empty(t, violations, "unexpected Antigravity request violations: %v\nencoded=%s", violations, string(encoded))

	s := string(encoded)
	// maxOutputTokens (1024) <= budget (2048) raises maxOutputTokens to 2048 + 2048 = 4096.
	require.Contains(t, s, `"maxOutputTokens":4096`)
	require.Contains(t, s, `"thinkingBudget":2048`)
	require.Contains(t, s, `"includeServerSideToolInvocations":true`)
	require.NotContains(t, s, `"temperature"`)
	require.NotContains(t, s, `"topP"`)
	require.NotContains(t, s, `"topK"`)
	require.NotContains(t, s, `"stopSequences"`)

	redecoded, err := DecodeRequest(encoded)
	require.NoError(t, err)
	// Align auto-adjusted MaxOutputTokens and summary before VerifyEssence checks all reported losses.
	redecoded.MaxOutputTokens = req.MaxOutputTokens
	redecoded.Reasoning = req.Reasoning
	diffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(redecoded), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
}

// TestEncodeRequestLevelOnlyModelAndUnsupportedThinkingErrors encodes thinking
// requests against Gemini 3 level-only models and capability-restricted dialects.
// Level-only models must clamp budgets to compatible thinkingLevel ranks, whereas
// models forbidding thinking or toggle disabling must return an error.
func TestEncodeRequestLevelOnlyModelAndUnsupportedThinkingErrors(t *testing.T) {
	budget := int64(2048)
	req := &ir.Request{
		Reasoning: &ir.ReasoningConfig{
			Mode:         "enabled",
			BudgetTokens: &budget,
		},
		Messages: []ir.Message{
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Think in levels"}}},
		},
	}

	encoded, _, err := EncodeRequest(req, ir.Dialect{
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Efforts:       []string{"low", "high"},
		},
	})
	require.NoError(t, err)
	// Budget 2048 maps to "medium", which clamps down to greatest allowed <= medium ("low").
	require.Contains(t, string(encoded), `"thinkingLevel":"low"`)

	unsupported := false
	_, _, err = EncodeRequest(req, ir.Dialect{
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Supported:     &unsupported,
		},
	})
	require.Error(t, err)

	zero := int64(0)
	disableReq := &ir.Request{
		Reasoning: &ir.ReasoningConfig{
			Mode:         "disabled",
			BudgetTokens: &zero,
		},
		Messages: req.Messages,
	}
	_, _, err = EncodeRequest(disableReq, ir.Dialect{
		Reasoning: ir.ReasoningCapabilities{
			ControlsKnown: true,
			Toggle:        false,
			Budget:        true,
		},
	})
	require.Error(t, err)
}

// TestEncodeRequestThinkingMetadataGuardAndUpstreamIncludeThoughts supplies
// metadata-only ReasoningConfig values and unlisted upstream models with
// ControlsKnown=false. Configs carrying only IncludeEncrypted or Summary must
// omit thinkingConfig without error, while enabled upstream thinking emits
// includeThoughts=true without unrequested budgets.
func TestEncodeRequestThinkingMetadataGuardAndUpstreamIncludeThoughts(t *testing.T) {
	msgs := []ir.Message{
		{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Plain prompt"}}},
	}

	// 1. Metadata-only IncludeEncrypted/Summary without Mode, Effort, or BudgetTokens
	// must NOT error on non-reasoning models (e.g. gemini-2.0-flash) or inject thinkingBudget=-1.
	unsupported := false
	for _, metaOnly := range []*ir.ReasoningConfig{
		{IncludeEncrypted: true},
		{Summary: "auto"},
		{IncludeEncrypted: true, Summary: "detailed"},
	} {
		encUnsupported, _, err := EncodeRequest(&ir.Request{
			Reasoning: metaOnly,
			Messages:  msgs,
		}, ir.Dialect{
			SchemaDialect: ir.SchemaGemini,
			Reasoning: ir.ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     &unsupported,
			},
		})
		require.NoError(t, err)
		require.NotContains(t, string(encUnsupported), `"thinkingConfig"`)

		encBudgetModel, _, err := EncodeRequest(&ir.Request{
			Reasoning: metaOnly,
			Messages:  msgs,
		}, ir.Dialect{
			SchemaDialect: ir.SchemaGemini,
			Reasoning: ir.ReasoningCapabilities{
				ControlsKnown: true,
				Budget:        true,
				ForceBudget:   true,
			},
		})
		require.NoError(t, err)
		require.NotContains(t, string(encBudgetModel), `"thinkingConfig"`)
	}

	// 2. Upstream Gemini dialect with ControlsKnown=false still sets includeThoughts=true
	// when Messages/Chat enables effort without setting Summary or IncludeEncrypted.
	encUnlistedEffort, _, err := EncodeRequest(&ir.Request{
		Reasoning: &ir.ReasoningConfig{
			Mode:   "enabled",
			Effort: "high",
		},
		Messages: msgs,
	}, ir.Dialect{
		SchemaDialect: ir.SchemaGemini,
	})
	require.NoError(t, err)
	require.Contains(t, string(encUnlistedEffort), `"includeThoughts":true`)
	require.Contains(t, string(encUnlistedEffort), `"thinkingLevel":"high"`)
	require.NotContains(t, string(encUnlistedEffort), `"thinkingBudget"`)

	// 3. Adaptive mode without an explicit BudgetTokens pointer under ControlsKnown=false
	// emits includeThoughts=true without injecting thinkingBudget=-1.
	encUnlistedAdaptive, _, err := EncodeRequest(&ir.Request{
		Reasoning: &ir.ReasoningConfig{
			Mode: "adaptive",
		},
		Messages: msgs,
	}, ir.Dialect{
		SchemaDialect: ir.SchemaGemini,
	})
	require.NoError(t, err)
	require.Contains(t, string(encUnlistedAdaptive), `"includeThoughts":true`)
	require.NotContains(t, string(encUnlistedAdaptive), `"thinkingBudget"`)
}

// TestEncodeRequestResolveGeminiThinkingTableParity exercises Gemini
// thinkingConfig encoding across budget-only, level-only, dual-capability
// adaptive-effort, and unlisted budget-plus-effort cases. Every case must
// produce the expected serialized thinkingConfig JSON or unsupported-control error.
func TestEncodeRequestResolveGeminiThinkingTableParity(t *testing.T) {
	yes := true
	output := 64000
	budgetCaps := ir.ReasoningCapabilities{
		ControlsKnown: true,
		Supported:     &yes,
		Toggle:        true,
		Budget:        true,
		BudgetMax:     24576,
	}
	levelCaps := ir.ReasoningCapabilities{
		ControlsKnown: true,
		Supported:     &yes,
		Efforts:       []string{"low", "medium", "high"},
	}
	privateCaps := ir.ReasoningCapabilities{
		ControlsKnown:  true,
		Supported:      &yes,
		ForceBudget:    true,
		AdaptiveBudget: 24576,
		OutputLimit:    &output,
		BudgetPadding:  1000,
		Efforts:        []string{"low", "medium", "high", "max"},
	}
	dualCaps := ir.ReasoningCapabilities{
		ControlsKnown: true,
		Supported:     &yes,
		Budget:        true,
		Efforts:       []string{"low", "medium", "high"},
	}

	negOne := int64(-1)
	b1024 := int64(1024)
	b2048 := int64(2048)
	b4096 := int64(4096)

	cases := []struct {
		name    string                   // subtest case label
		rc      *ir.ReasoningConfig      // client IR reasoning configuration
		caps    ir.ReasoningCapabilities // resolved model thinking capabilities
		want    string                   // expected serialized thinkingConfig JSON ("" when omitted)
		failure bool                     // whether EncodeRequest must return an error
	}{
		{"absent", nil, levelCaps, "", false},
		{"budget high", &ir.ReasoningConfig{Effort: "high"}, budgetCaps, `{"includeThoughts":true,"thinkingBudget":10240}`, false},
		{"budget max capped", &ir.ReasoningConfig{Effort: "max"}, budgetCaps, `{"includeThoughts":true,"thinkingBudget":24576}`, false},
		{"dynamic", &ir.ReasoningConfig{Mode: "enabled", BudgetTokens: &negOne}, budgetCaps, `{"includeThoughts":true,"thinkingBudget":-1}`, false},
		{"disabled zero retained", &ir.ReasoningConfig{Mode: "disabled", Effort: "high"}, budgetCaps, `{"includeThoughts":false,"thinkingBudget":0}`, false},
		{"none cannot disable level", &ir.ReasoningConfig{Effort: "none"}, levelCaps, "", true},
		{"level high", &ir.ReasoningConfig{Effort: "high"}, levelCaps, `{"includeThoughts":true,"thinkingLevel":"high"}`, false},
		{"level minimal maps low", &ir.ReasoningConfig{Effort: "minimal"}, levelCaps, `{"includeThoughts":true,"thinkingLevel":"low"}`, false},
		{"level xhigh maps high", &ir.ReasoningConfig{Effort: "xhigh"}, levelCaps, `{"includeThoughts":true,"thinkingLevel":"high"}`, false},
		{"budget translated to level", &ir.ReasoningConfig{Mode: "enabled", BudgetTokens: &b4096}, levelCaps, `{"includeThoughts":true,"thinkingLevel":"medium"}`, false},
		{"private adaptive", &ir.ReasoningConfig{Mode: "adaptive", BudgetTokens: &b1024}, privateCaps, `{"includeThoughts":true,"thinkingBudget":24576}`, false},
		{"private effort", &ir.ReasoningConfig{Mode: "adaptive", Effort: "max"}, privateCaps, `{"includeThoughts":true,"thinkingBudget":32768}`, false},
		{"unknown controls preserved", &ir.ReasoningConfig{Effort: "future-level"}, ir.ReasoningCapabilities{}, `{"includeThoughts":true,"thinkingLevel":"future-level"}`, false},
		{"known no controls", &ir.ReasoningConfig{Effort: "high"}, ir.ReasoningCapabilities{ControlsKnown: true, Supported: &yes}, "", true},
		{"dual budget and effort adaptive without budget uses level", &ir.ReasoningConfig{Mode: "adaptive", Effort: "high"}, dualCaps, `{"includeThoughts":true,"thinkingLevel":"high"}`, false},
		{"unknown controls with explicit budget and effort uses budget", &ir.ReasoningConfig{Mode: "enabled", BudgetTokens: &b2048, Effort: "high"}, ir.ReasoningCapabilities{}, `{"includeThoughts":true,"thinkingBudget":2048}`, false},
	}

	msgs := []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "ping"}}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			encoded, _, err := EncodeRequest(&ir.Request{
				Reasoning: tc.rc,
				Messages:  msgs,
			}, ir.Dialect{
				SchemaDialect: ir.SchemaGemini,
				Reasoning:     tc.caps,
			})
			if tc.failure {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.want == "" {
				require.NotContains(t, string(encoded), `"thinkingConfig"`)
				return
			}
			var parsed wireOutRequest
			require.NoError(t, unmarshalJSONObject(encoded, &parsed))
			require.NotNil(t, parsed.GenerationConfig)
			require.JSONEq(t, tc.want, string(parsed.GenerationConfig.ThinkingConfig))
		})
	}
}
