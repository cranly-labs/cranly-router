package convert

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// dialect_test.go verifies dialect-specific request normalization rules across
// capability-guarded disabled thinking fallbacks and cross-protocol function
// tool strictness defaults. Each test asserts both wire-level JSON fields and
// Law Q4 Essence reconciliation against reported IR losses.

// TestMessagesDisabledThinkingCapabilityGuard converts Anthropic Messages
// requests that explicitly disable thinking (via thinking.type="disabled" or
// budget_tokens=0) onto OpenAI Responses and Chat Completions across target
// model capability profiles. It verifies that effort="none" is emitted only
// when Dialect.Reasoning explicitly lists "none", non-toggle pure reasoning
// models fall back to "minimal" or their lowest active tier, and non-reasoning,
// toggle-only, or unknown capability targets omit effort while satisfying Law Q4.
func TestMessagesDisabledThinkingCapabilityGuard(t *testing.T) {
	boolPtr := func(v bool) *bool { return &v }
	disabledBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":512,"temperature":1,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"Hello"}]}`)

	cases := []struct {
		name            string
		reasoning       ReasoningCapabilities
		stripEffort     bool
		dropTemperature bool
		wantEffort      string
	}{
		{
			name: "gpt-5.4 explicitly supports none",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Efforts:       []string{"none", "low", "medium", "high", "xhigh"},
			},
			wantEffort: "none",
		},
		{
			name: "kimi-for-coding explicitly supports none",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Efforts:       []string{"none", "low", "high", "max"},
			},
			dropTemperature: true,
			wantEffort:      "none",
		},
		{
			name: "gpt-5 supports minimal without none",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Efforts:       []string{"minimal", "low", "medium", "high"},
			},
			dropTemperature: true,
			wantEffort:      "minimal",
		},
		{
			name: "o3 and catalogued gpt-5-codex pure reasoning falls back to lowest tier low",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Toggle:        false,
				Efforts:       []string{"low", "medium", "high"},
			},
			dropTemperature: true,
			wantEffort:      "low",
		},
		{
			name: "gpt-5.4-pro pure reasoning falls back to lowest tier medium",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Toggle:        false,
				Efforts:       []string{"medium", "high", "xhigh"},
			},
			dropTemperature: true,
			wantEffort:      "medium",
		},
		{
			name: "deepseek-v4-pro toggleable model without none omits effort",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Toggle:        true,
				Efforts:       []string{"low", "high", "max"},
			},
			wantEffort: "",
		},
		{
			name: "kimi-k2.6 toggle-only model without efforts omits effort",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Toggle:        true,
			},
			dropTemperature: true,
			wantEffort:      "",
		},
		{
			name: "kimi-k2.7-code fixed reasoning model without efforts omits effort",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Toggle:        false,
			},
			dropTemperature: true,
			wantEffort:      "",
		},
		{
			name: "gpt-4o non-reasoning model omits effort",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(false),
			},
			wantEffort: "",
		},
		{
			name:       "uncatalogued model or zero Dialect ControlsKnown=false omits effort",
			reasoning:  ReasoningCapabilities{ControlsKnown: false},
			wantEffort: "",
		},
		{
			name: "StripEffort suppresses outgoing effort even when none is supported",
			reasoning: ReasoningCapabilities{
				ControlsKnown: true,
				Supported:     boolPtr(true),
				Efforts:       []string{"none", "low", "medium", "high"},
			},
			stripEffort: true,
			wantEffort:  "",
		},
	}

	srcIR, err := DecodeRequest(protocol.ProtocolAnthropicMessages, disabledBody)
	require.NoError(t, err)

	for _, tc := range cases {
		t.Run(tc.name+"/responses", func(t *testing.T) {
			dialect := Dialect{
				Name:            "test_responses",
				Reasoning:       tc.reasoning,
				StripEffort:     tc.stripEffort,
				DropTemperature: tc.dropTemperature,
			}
			ex, exErr := NewExchange(disabledBody, Options{
				Route: Route{
					Client:   protocol.ProtocolAnthropicMessages,
					Upstream: protocol.ProtocolOpenAIResponses,
					Dialect:  dialect,
				},
			})
			require.NoError(t, exErr)
			upBytes, upErr := ex.UpstreamRequest()
			require.NoError(t, upErr)

			var wire struct {
				Reasoning *struct {
					Effort  string `json:"effort"`
					Summary string `json:"summary"`
				} `json:"reasoning"`
			}
			require.NoError(t, jsonv2.Unmarshal(upBytes, &wire, jsontext.AllowInvalidUTF8(true)))
			if tc.wantEffort == "" {
				require.Nil(t, wire.Reasoning, "expected reasoning object to be omitted: %s", string(upBytes))
			} else {
				require.NotNil(t, wire.Reasoning, "expected reasoning.effort=%q: %s", tc.wantEffort, string(upBytes))
				require.Equal(t, tc.wantEffort, wire.Reasoning.Effort)
				require.Empty(t, wire.Reasoning.Summary, "disabled thinking must never request reasoning.summary")
			}

			require.Empty(t, testkit.ValidateResponsesRequest(upBytes, testkit.RequestValidationOptions{}))
			dstIR, decErr := DecodeRequest(protocol.ProtocolOpenAIResponses, upBytes)
			require.NoError(t, decErr)
			diffs, q4Err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex.Losses())
			require.NoError(t, q4Err)
			require.Empty(t, diffs)
		})

		t.Run(tc.name+"/chat", func(t *testing.T) {
			dialect := Dialect{
				Name:            "test_chat",
				Reasoning:       tc.reasoning,
				StripEffort:     tc.stripEffort,
				DropTemperature: tc.dropTemperature,
			}
			ex, exErr := NewExchange(disabledBody, Options{
				Route: Route{
					Client:   protocol.ProtocolAnthropicMessages,
					Upstream: protocol.ProtocolOpenAIChatCompletions,
					Dialect:  dialect,
				},
			})
			require.NoError(t, exErr)
			upBytes, upErr := ex.UpstreamRequest()
			require.NoError(t, upErr)

			var root map[string]jsontext.Value
			require.NoError(t, jsonv2.Unmarshal(upBytes, &root, jsontext.AllowInvalidUTF8(true)))
			rawEff, hasEff := root["reasoning_effort"]
			if tc.wantEffort == "" {
				require.False(t, hasEff, "expected reasoning_effort to be omitted: %s", string(upBytes))
			} else {
				require.True(t, hasEff, "expected reasoning_effort=%q: %s", tc.wantEffort, string(upBytes))
				var gotEff string
				require.NoError(t, jsonv2.Unmarshal(rawEff, &gotEff))
				require.Equal(t, tc.wantEffort, gotEff)
			}

			require.Empty(t, testkit.ValidateChatRequest(upBytes, testkit.RequestValidationOptions{}))
			dstIR, decErr := DecodeRequest(protocol.ProtocolOpenAIChatCompletions, upBytes)
			require.NoError(t, decErr)
			diffs, q4Err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex.Losses())
			require.NoError(t, q4Err)
			require.Empty(t, diffs)
		})
	}

	t.Run("zero budget_tokens=0 in IR applies identical capability fallback", func(t *testing.T) {
		zeroBudget := int64(0)
		for _, sub := range []struct {
			label      string
			reasoning  ReasoningCapabilities
			wantEffort string
		}{
			{
				label:      "unknown omits",
				reasoning:  ReasoningCapabilities{ControlsKnown: false},
				wantEffort: "",
			},
			{
				label: "none supported emits none",
				reasoning: ReasoningCapabilities{
					ControlsKnown: true,
					Efforts:       []string{"none", "low", "medium", "high"},
				},
				wantEffort: "none",
			},
			{
				label: "minimal supported emits minimal",
				reasoning: ReasoningCapabilities{
					ControlsKnown: true,
					Efforts:       []string{"minimal", "low", "medium", "high"},
				},
				wantEffort: "minimal",
			},
			{
				label: "pure reasoning emits low",
				reasoning: ReasoningCapabilities{
					ControlsKnown: true,
					Efforts:       []string{"low", "medium", "high"},
				},
				wantEffort: "low",
			},
		} {
			chatReq := &ir.Request{
				Model:     "claude-sonnet-4-5",
				Messages:  []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "ping"}}}},
				Reasoning: &ir.ReasoningConfig{BudgetTokens: &zeroBudget, Effort: "high"},
			}
			require.NoError(t, injectMessagesToChatDefaults(chatReq, Dialect{Reasoning: sub.reasoning}), sub.label)
			require.Equal(t, sub.wantEffort, chatReq.Reasoning.Effort, sub.label)

			respReq := &ir.Request{
				Model:     "claude-sonnet-4-5",
				Messages:  []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "ping"}}}},
				Reasoning: &ir.ReasoningConfig{BudgetTokens: &zeroBudget, Effort: "high", Summary: "auto"},
			}
			require.NoError(t, injectCrossProtocolResponsesDefaults(respReq, protocol.ProtocolAnthropicMessages, Dialect{Reasoning: sub.reasoning}), sub.label)
			require.Equal(t, sub.wantEffort, respReq.Reasoning.Effort, sub.label)
			require.Empty(t, respReq.Reasoning.Summary, sub.label)
		}
	})
}

// TestQoderToolStrictDefaultByClientProtocol converts tool-bearing Anthropic
// Messages, OpenAI Responses, and OpenAI Chat Completions requests onto Qoder.
// It verifies that Messages declarations without an explicit strict flag set
// strict=false with LossSchemaApproximated("strict") while Responses and Chat
// omit strict when unspecified.
func TestQoderToolStrictDefaultByClientProtocol(t *testing.T) {
	msgBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"tools":[{"name":"bash","input_schema":{"type":"object","properties":{}}}],"messages":[{"role":"user","content":"hi"}]}`)
	exMsg, err := NewExchange(msgBody, Options{
		Route: Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolQoderChat,
			Dialect:  Dialect{AllowBillingHeader: true},
		},
	})
	require.NoError(t, err)
	upMsg, err := exMsg.UpstreamRequest()
	require.NoError(t, err)
	require.Contains(t, string(upMsg), `"strict":false`)
	require.Contains(t, exMsg.Losses(), ir.Loss{
		Kind:   ir.LossSchemaApproximated,
		Path:   "tools[0]",
		Detail: "strict",
	})

	for _, tc := range []struct {
		name  string
		proto protocol.ProtocolID
		body  []byte
	}{
		{
			name:  "responses omits unspecified strict",
			proto: protocol.ProtocolOpenAIResponses,
			body:  []byte(`{"model":"gpt-5","tools":[{"type":"function","name":"bash","parameters":{"type":"object","properties":{}}}],"input":"hi"}`),
		},
		{
			name:  "chat omits unspecified strict",
			proto: protocol.ProtocolOpenAIChatCompletions,
			body:  []byte(`{"model":"gpt-4o","tools":[{"type":"function","function":{"name":"bash","parameters":{"type":"object","properties":{}}}}],"messages":[{"role":"user","content":"hi"}]}`),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex, exErr := NewExchange(tc.body, Options{
				Route: Route{
					Client:   tc.proto,
					Upstream: protocol.ProtocolQoderChat,
					Dialect:  Dialect{AllowBillingHeader: true},
				},
			})
			require.NoError(t, exErr)
			up, upErr := ex.UpstreamRequest()
			require.NoError(t, upErr)
			require.NotContains(t, string(up), `"strict"`)
		})
	}
}

// TestCrossProtocolMessagesThinkingActivationAndLawQ2 converts OpenAI Responses
// and Chat Completions requests into Anthropic Messages across unknown-control,
// dual budget-and-effort, effort-only adaptive, stripped, and
// thinking-unsupported dialects alongside same-protocol Messages replay.
// Medium-and-above cross-protocol efforts must activate extended thinking and
// pass strict validation and Law Q4, while low efforts and single-protocol
// Messages Law Q2 omit unrequested thinking fields.
func TestCrossProtocolMessagesThinkingActivationAndLawQ2(t *testing.T) {
	yes := true
	no := false

	clientSources := []struct {
		name     string
		proto    protocol.ProtocolID
		makeBody func(effort string) []byte
	}{
		{
			name:  "responses",
			proto: protocol.ProtocolOpenAIResponses,
			makeBody: func(effort string) []byte {
				return []byte(`{"model":"gpt-5","reasoning":{"effort":"` + effort + `"},"input":"ping"}`)
			},
		},
		{
			name:  "chat",
			proto: protocol.ProtocolOpenAIChatCompletions,
			makeBody: func(effort string) []byte {
				return []byte(`{"model":"gpt-5","reasoning_effort":"` + effort + `","messages":[{"role":"user","content":"ping"}]}`)
			},
		},
	}

	dialects := []struct {
		name             string
		dialect          Dialect
		effort           string
		wantThinkingJSON string
		wantEffortJSON   string
	}{
		{
			name:             "unknown controls low omits thinking",
			dialect:          Dialect{Name: "anthropic"},
			effort:           "low",
			wantThinkingJSON: "",
			wantEffortJSON:   `"effort":"low"`,
		},
		{
			name:             "unknown controls minimal maps to low without thinking",
			dialect:          Dialect{Name: "anthropic"},
			effort:           "minimal",
			wantThinkingJSON: "",
			wantEffortJSON:   `"effort":"low"`,
		},
		{
			name:             "unknown controls medium enables 4096 budget",
			dialect:          Dialect{Name: "anthropic"},
			effort:           "medium",
			wantThinkingJSON: `"thinking":{"type":"enabled","budget_tokens":4096}`,
			wantEffortJSON:   `"effort":"medium"`,
		},
		{
			name:             "unknown controls high enables 10240 budget",
			dialect:          Dialect{Name: "anthropic"},
			effort:           "high",
			wantThinkingJSON: `"thinking":{"type":"enabled","budget_tokens":10240}`,
			wantEffortJSON:   `"effort":"high"`,
		},
		{
			name:             "unknown controls max enables 32768 budget",
			dialect:          Dialect{Name: "anthropic"},
			effort:           "max",
			wantThinkingJSON: `"thinking":{"type":"enabled","budget_tokens":32768}`,
			wantEffortJSON:   `"effort":"max"`,
		},
		{
			name: "claude-opus-4-6 dual budget and effort medium",
			dialect: Dialect{
				Name: "anthropic",
				Reasoning: ReasoningCapabilities{
					ControlsKnown: true,
					Supported:     &yes,
					Budget:        true,
					BudgetMin:     1024,
					Efforts:       []string{"low", "medium", "high", "max"},
				},
			},
			effort:           "medium",
			wantThinkingJSON: `"thinking":{"type":"enabled","budget_tokens":4096}`,
			wantEffortJSON:   `"effort":"medium"`,
		},
		{
			name: "claude-sonnet-4-6 dual budget and effort xhigh clamps effort to high",
			dialect: Dialect{
				Name: "anthropic",
				Reasoning: ReasoningCapabilities{
					ControlsKnown: true,
					Supported:     &yes,
					Budget:        true,
					BudgetMin:     1024,
					Efforts:       []string{"low", "medium", "high", "max"},
				},
			},
			effort:           "xhigh",
			wantThinkingJSON: `"thinking":{"type":"enabled","budget_tokens":32768}`,
			wantEffortJSON:   `"effort":"high"`,
		},
		{
			name: "claude-opus-4-7 effort-only adaptive high",
			dialect: Dialect{
				Name:            "anthropic",
				DropTemperature: true,
				Reasoning: ReasoningCapabilities{
					ControlsKnown: true,
					Supported:     &yes,
					Efforts:       []string{"low", "medium", "high", "xhigh", "max"},
				},
			},
			effort:           "high",
			wantThinkingJSON: `"thinking":{"type":"adaptive"}`,
			wantEffortJSON:   `"effort":"high"`,
		},
		{
			name: "claude-sonnet-5 effort-only adaptive medium",
			dialect: Dialect{
				Name:            "anthropic",
				DropTemperature: true,
				Reasoning: ReasoningCapabilities{
					ControlsKnown: true,
					Supported:     &yes,
					Toggle:        true,
					Efforts:       []string{"low", "medium", "high", "xhigh", "max"},
				},
			},
			effort:           "medium",
			wantThinkingJSON: `"thinking":{"type":"adaptive"}`,
			wantEffortJSON:   `"effort":"medium"`,
		},
		{
			name: "strip effort suppresses both thinking and output_config",
			dialect: Dialect{
				Name:        "anthropic",
				StripEffort: true,
			},
			effort:           "high",
			wantThinkingJSON: "",
			wantEffortJSON:   "",
		},
	}

	for _, src := range clientSources {
		for _, dc := range dialects {
			t.Run(src.name+"_"+dc.name, func(t *testing.T) {
				body := src.makeBody(dc.effort)
				ex, err := NewExchange(body, Options{
					Route: Route{
						Client:   src.proto,
						Upstream: protocol.ProtocolAnthropicMessages,
						Dialect:  dc.dialect,
					},
				})
				require.NoError(t, err)
				upBytes, err := ex.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateMessagesRequest(upBytes, testkit.RequestValidationOptions{StrictTopLevel: true}))

				if dc.wantThinkingJSON == "" {
					require.NotContains(t, string(upBytes), `"thinking"`, string(upBytes))
				} else {
					require.Contains(t, string(upBytes), dc.wantThinkingJSON, string(upBytes))
				}
				if dc.wantEffortJSON == "" {
					require.NotContains(t, string(upBytes), `"output_config"`, string(upBytes))
				} else {
					require.Contains(t, string(upBytes), dc.wantEffortJSON, string(upBytes))
				}

				srcIR, err := DecodeRequest(src.proto, body)
				require.NoError(t, err)
				dstIR, err := DecodeRequest(protocol.ProtocolAnthropicMessages, upBytes)
				require.NoError(t, err)
				diffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex.Losses())
				require.NoError(t, err)
				require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
			})
		}

		// Targets declaring Supported=false reject active cross-protocol effort tiers cleanly.
		t.Run(src.name+"_unsupported_thinking_rejected", func(t *testing.T) {
			ex, err := NewExchange(src.makeBody("medium"), Options{
				Route: Route{
					Client:   src.proto,
					Upstream: protocol.ProtocolAnthropicMessages,
					Dialect: Dialect{
						Name: "anthropic",
						Reasoning: ReasoningCapabilities{
							ControlsKnown: true,
							Supported:     &no,
						},
					},
				},
			})
			require.NoError(t, err)
			_, err = ex.UpstreamRequest()
			require.ErrorContains(t, err, "selected model does not support thinking")
		})
	}

	// Same-protocol Messages -> Messages preserves absent thinking when output_config.effort=medium is present.
	msgNative := []byte(`{"model":"claude-opus-4-6","max_tokens":2048,"output_config":{"effort":"medium"},"messages":[{"role":"user","content":"ping"}]}`)
	exSame, err := NewExchange(msgNative, Options{
		Route: Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolAnthropicMessages,
		},
	})
	require.NoError(t, err)
	upSame, err := exSame.UpstreamRequest()
	require.NoError(t, err)
	require.NotContains(t, string(upSame), `"thinking"`)
	require.Contains(t, string(upSame), `"output_config":{"effort":"medium"}`)
	require.Empty(t, exSame.Losses())
	dec1, err := DecodeRequest(protocol.ProtocolAnthropicMessages, msgNative)
	require.NoError(t, err)
	dec2, err := DecodeRequest(protocol.ProtocolAnthropicMessages, upSame)
	require.NoError(t, err)
	require.Equal(t, dec1, dec2)
}
