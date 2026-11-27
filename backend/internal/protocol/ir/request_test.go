package ir

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// TestRequestJSONRoundTripAndExtraIsZero marshals and unmarshals a populated
// Request containing Extra fields, raw schema bytes, and empty Extra values
// carrying an Origin tag. It verifies that Extra.IsZero suppresses empty Extra
// objects under omitzero while preserving populated raw JSON members verbatim.
func TestRequestJSONRoundTripAndExtraIsZero(t *testing.T) {
	temp := 0.25
	maxOut := int64(2048)
	strict := true
	rawSchema := jsontext.Value(`{"type":"object","properties":{"limit":{"type":"integer","default":9007199254740993}}}`)

	req := Request{
		Model: "claude-sonnet-4-5",
		System: []Part{
			{
				Kind:  PartText,
				Text:  "Follow system policy.",
				Cache: &CacheMark{TTL: "5m"},
				Extra: Extra{Origin: protocol.ProtocolAnthropicMessages},
			},
		},
		Messages: []Message{
			{
				Role: RoleUser,
				Parts: []Part{
					{
						Kind: PartText,
						Text: "Check status",
						Extra: Extra{
							Origin: protocol.ProtocolAnthropicMessages,
							Members: map[string]jsontext.Value{
								"custom_id": jsontext.Value(`9007199254740993`),
							},
						},
					},
				},
				Extra: Extra{Origin: protocol.ProtocolAnthropicMessages},
			},
			{
				Role: RoleAssistant,
				Parts: []Part{
					{
						Kind: PartReasoning,
						Reasoning: &Reasoning{
							Segments: []string{"Inspecting cluster"},
							Token: &ReplayToken{
								Scheme: SchemeAnthropic,
								Data:   "sig_abc",
							},
						},
					},
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "call_1",
							Tool:      ToolRef{Kind: ToolFunction, Name: "check_cluster"},
							Arguments: `{"limit":9007199254740993}`,
						},
					},
				},
			},
			{
				Role: RoleTool,
				Parts: []Part{
					{
						Kind: PartToolResult,
						Result: &ToolResult{
							CallID:  "call_1",
							Tool:    ToolRef{Kind: ToolFunction, Name: "check_cluster"},
							Content: []Part{{Kind: PartText, Text: "healthy"}},
						},
					},
				},
			},
		},
		Tools: []Tool{
			{
				Kind:        ToolFunction,
				Name:        "check_cluster",
				InputSchema: rawSchema,
				Strict:      &strict,
			},
		},
		Sampling:        Sampling{Temperature: &temp},
		MaxOutputTokens: &maxOut,
		Extra:           Extra{Origin: protocol.ProtocolAnthropicMessages},
	}

	require.True(t, req.Extra.IsZero())
	require.False(t, req.Messages[0].Parts[0].Extra.IsZero())

	encoded, err := json.Marshal(req, json.Deterministic(true))
	require.NoError(t, err)
	require.NotContains(t, string(encoded), `"extra":{"origin":"anthropic_messages"}`)
	require.Contains(t, string(encoded), `"custom_id":9007199254740993`)

	var decoded Request
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, req.Model, decoded.Model)
	require.Equal(t, string(rawSchema), string(decoded.Tools[0].InputSchema))
	require.Equal(t, `9007199254740993`, string(decoded.Messages[0].Parts[0].Extra.Members["custom_id"]))
	require.True(t, decoded.Extra.IsZero())
}

// TestRequestAndPartCloneDeepIsolation clones a Request populated with every
// pointer, slice, nested tool-result part, and raw JSON member, then mutates
// every branch on the copy. The original Request must remain unchanged, and
// cloning a nil Request must return nil.
func TestRequestAndPartCloneDeepIsolation(t *testing.T) {
	var nilReq *Request
	require.Nil(t, nilReq.Clone())

	temp := 0.4
	topP := 0.9
	topK := int64(20)
	seed := int64(7)
	freq := 0.1
	pres := 0.2
	cand := int64(1)
	maxOut := int64(1024)
	budget := int64(512)
	flagTrue := true
	startIdx := int64(0)
	endIdx := int64(5)

	orig := &Request{
		Model: "gpt-5",
		System: []Part{{
			Kind:  PartText,
			Text:  "sys",
			Cache: &CacheMark{TTL: "5m", Extra: Extra{Origin: protocol.ProtocolAnthropicMessages, Members: map[string]jsontext.Value{"k": jsontext.Value(`1`)}}},
		}},
		Messages: []Message{
			{
				Role: RoleUser,
				ID:   "m1",
				Parts: []Part{
					{
						Kind: PartText,
						Text: "hello",
						Annotations: []Annotation{{
							Kind:       "url_citation",
							URL:        "https://go.dev",
							StartIndex: &startIdx,
							EndIndex:   &endIdx,
							Extra:      Extra{Origin: protocol.ProtocolOpenAIResponses, Members: map[string]jsontext.Value{"a": jsontext.Value(`"x"`)}},
						}},
						Extra: Extra{Origin: protocol.ProtocolOpenAIResponses, Members: map[string]jsontext.Value{"p": jsontext.Value(`true`)}},
					},
					{
						Kind:  PartMedia,
						Media: &Media{Type: MediaImage, MIME: "image/png", Source: MediaSourceBase64, Data: "iVBORw=="},
					},
					{
						Kind:   PartOpaque,
						Opaque: &OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: "compaction", Raw: jsontext.Value(`{"type":"compaction"}`)},
					},
				},
				Extra: Extra{Origin: protocol.ProtocolOpenAIResponses, Members: map[string]jsontext.Value{"m": jsontext.Value(`2`)}},
			},
			{
				Role: RoleAssistant,
				Parts: []Part{
					{
						Kind: PartReasoning,
						Reasoning: &Reasoning{
							Segments: []string{"think 1"},
							Token:    &ReplayToken{Scheme: SchemeOpenAI, ProviderID: 9, Data: "enc"},
						},
					},
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "c1",
							Tool:      ToolRef{Kind: ToolFunction, Name: "fn"},
							Arguments: `{"x":1}`,
							Token:     &ReplayToken{Scheme: SchemeGemini, Data: "sig"},
						},
					},
				},
			},
			{
				Role: RoleTool,
				Parts: []Part{{
					Kind: PartToolResult,
					Result: &ToolResult{
						CallID:  "c1",
						Content: []Part{{Kind: PartText, Text: "ok"}},
					},
				}},
			},
		},
		Tools: []Tool{{
			Kind:         ToolFunction,
			Name:         "fn",
			InputSchema:  jsontext.Value(`{"type":"object"}`),
			Strict:       &flagTrue,
			DeferLoading: &flagTrue,
			Hosted:       &OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: "web_search", Raw: jsontext.Value(`{"type":"web_search"}`)},
			Cache:        &CacheMark{TTL: "1h"},
			Extra:        Extra{Origin: protocol.ProtocolOpenAIResponses, Members: map[string]jsontext.Value{"t": jsontext.Value(`3`)}},
		}},
		Namespaces: []Namespace{{
			Name:         "ns",
			DeferLoading: &flagTrue,
			Cache:        &CacheMark{TTL: "5m"},
			Extra:        Extra{Origin: protocol.ProtocolOpenAIResponses, Members: map[string]jsontext.Value{"n": jsontext.Value(`4`)}},
		}},
		ToolChoice: &ToolChoice{
			Mode:         ToolChoiceAuto,
			AllowedTools: []ToolRef{{Kind: ToolFunction, Name: "fn"}},
		},
		ParallelToolCalls: &flagTrue,
		Sampling: Sampling{
			Temperature:      &temp,
			TopP:             &topP,
			TopK:             &topK,
			Seed:             &seed,
			FrequencyPenalty: &freq,
			PresencePenalty:  &pres,
			CandidateCount:   &cand,
		},
		MaxOutputTokens: &maxOut,
		Stop:            []string{"END"},
		Reasoning:       &ReasoningConfig{Mode: "enabled", BudgetTokens: &budget},
		Output:          &OutputFormat{Kind: "json_schema", Schema: jsontext.Value(`{"type":"object"}`), Strict: &flagTrue},
		IncludeUsage:    &flagTrue,
		Store:           &flagTrue,
		Extra:           Extra{Origin: protocol.ProtocolOpenAIResponses, Members: map[string]jsontext.Value{"root": jsontext.Value(`5`)}},
	}

	beforeJSON, err := json.Marshal(orig, json.Deterministic(true))
	require.NoError(t, err)

	cp := orig.Clone()
	cp.System[0].Text = "mutated"
	cp.System[0].Cache.TTL = "1h"
	cp.System[0].Cache.Extra.Members["k"] = jsontext.Value(`99`)
	*cp.Messages[0].Parts[0].Annotations[0].StartIndex = 99
	cp.Messages[0].Parts[0].Extra.Members["p"] = jsontext.Value(`false`)
	cp.Messages[0].Parts[1].Media.Data = "changed"
	cp.Messages[0].Parts[2].Opaque.Raw[0] = '['
	cp.Messages[1].Parts[0].Reasoning.Segments[0] = "changed"
	cp.Messages[1].Parts[0].Reasoning.Token.Data = "changed"
	cp.Messages[1].Parts[1].Call.Token.Data = "changed"
	cp.Messages[2].Parts[0].Result.Content[0].Text = "changed"
	cp.Tools[0].InputSchema[0] = '['
	*cp.Tools[0].Strict = false
	*cp.Namespaces[0].DeferLoading = false
	cp.ToolChoice.AllowedTools[0].Name = "other"
	*cp.Sampling.Temperature = 1.9
	*cp.MaxOutputTokens = 1
	cp.Stop[0] = "OTHER"
	*cp.Reasoning.BudgetTokens = 1
	cp.Output.Schema[0] = '['
	*cp.IncludeUsage = false
	*cp.Store = false
	cp.Extra.Members["root"] = jsontext.Value(`0`)

	afterJSON, err := json.Marshal(orig, json.Deterministic(true))
	require.NoError(t, err)
	require.Equal(t, string(beforeJSON), string(afterJSON))
}
