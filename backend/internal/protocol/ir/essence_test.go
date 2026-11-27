package ir

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// TestEssenceDocJSONRoundTrip marshals and unmarshals an EssenceDoc covering
// request messages, tool declarations, reasoning tokens, and media hashes. It
// verifies deterministic serialization and omission of zero-valued optional
// fields.
func TestEssenceDocJSONRoundTrip(t *testing.T) {
	doc := EssenceDoc{
		System: []EssencePart{
			{Kind: PartText, Text: "System prompt", CacheTTL: "5m"},
		},
		Messages: []EssenceMessage{
			{
				Role: RoleUser,
				Parts: []EssencePart{
					{
						Kind: PartMedia,
						Media: &EssenceMedia{
							Type:        MediaImage,
							MIME:        "image/png",
							Source:      MediaSourceBase64,
							ContentHash: "abc123",
						},
					},
				},
			},
			{
				Role: RoleAssistant,
				Parts: []EssencePart{
					{
						Kind: PartReasoning,
						Reasoning: &EssenceReasoning{
							Segments: []string{"step 1"},
							Token: &EssenceToken{
								Scheme:     SchemeOpenAI,
								ProviderID: 42,
								Data:       "enc_xyz",
							},
						},
					},
					{
						Kind: PartToolCall,
						Call: &EssenceToolCall{
							CallID:    "call_9",
							Tool:      ToolRef{Kind: ToolFunction, Name: "lookup"},
							Arguments: `{"id":1}`,
						},
					},
				},
			},
		},
		Tools: []EssenceTool{
			{
				Kind:       ToolFunction,
				Name:       "lookup",
				SchemaHash: "deadbeef",
			},
		},
	}

	encoded, err := json.Marshal(doc, json.Deterministic(true))
	require.NoError(t, err)
	require.NotContains(t, string(encoded), `"finish"`)

	var decoded EssenceDoc
	require.NoError(t, json.Unmarshal(encoded, &decoded))
	require.Equal(t, doc, decoded)
}

// TestEssenceAndResponseEssenceNormalization checks Essence and ResponseEssence
// across nil inputs, multi-segment reasoning concatenation, 64-bit JSON number
// canonicalization, media hint stripping, trc1 carrier unpacking, and default
// transport-field exclusion. Caller-owned requests must remain unmutated while
// cross-protocol injected fields stay absent from both summaries.
func TestEssenceAndResponseEssenceNormalization(t *testing.T) {
	require.Equal(t, EssenceDoc{}, Essence(nil))
	require.Equal(t, EssenceDoc{}, ResponseEssence(nil))

	plainCarrier, err := FormatCarrier(Carrier{
		Scheme: SchemePlain,
		Text:   "Recovered plaintext thought",
	})
	require.NoError(t, err)

	anthCarrier, err := FormatCarrier(Carrier{
		Scheme: SchemeAnthropic,
		Data:   "EqQ_sig_real",
		Text:   "Authoritative signed Claude thought.",
	})
	require.NoError(t, err)

	openAICarrier, err := FormatCarrier(Carrier{
		Scheme:     SchemeOpenAI,
		Data:       "gAAAAABnCipher",
		ProviderID: 77,
	})
	require.NoError(t, err)

	parallel := true
	maxOut := int64(128)
	reqA := &Request{
		System: []Part{{Kind: PartText, Text: "sys", Cache: &CacheMark{}}},
		Messages: []Message{
			{
				Role: RoleUser,
				Parts: []Part{
					{
						Kind: PartMedia,
						Media: &Media{
							Type:   MediaImage,
							MIME:   "IMAGE/PNG",
							Source: MediaSourceBase64,
							Data:   "iVBORw0KGgo=",
							Name:   "inline.png",
							Detail: "high",
						},
					},
					{
						Kind: PartMedia,
						Media: &Media{
							Type:   MediaImage,
							MIME:   "image/png",
							Source: MediaSourceURL,
							Data:   "https://example.com/diagram.png",
							Name:   "diagram.png",
							Detail: "auto",
						},
					},
				},
			},
			{
				Role: RoleAssistant,
				Parts: []Part{
					{
						Kind: PartReasoning,
						Reasoning: &Reasoning{
							// Multi-segment summary concatenates into one canonical element.
							Segments: []string{"First segment. ", "Second segment."},
							Summary:  true,
						},
					},
					{
						Kind: PartReasoning,
						Reasoning: &Reasoning{
							// Encrypted-only plain carrier with empty visible Segments.
							Summary: true,
							Token:   &ReplayToken{Data: plainCarrier},
						},
					},
					{
						Kind: PartReasoning,
						Reasoning: &Reasoning{
							// Abbreviated client summary replaced by signed carrier text.
							Segments: []string{"short summary"},
							Summary:  true,
							Token:    &ReplayToken{Data: anthCarrier},
						},
					},
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "c_json",
							Tool:      ToolRef{Kind: ToolFunction, Name: "query"},
							Arguments: "{\n  \"b\": 1.00,\n  \"a\": 9007199254740993\n}",
						},
					},
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "c_empty",
							Tool:      ToolRef{Kind: ToolFunction, Name: "ping"},
							Arguments: "",
						},
					},
					{
						Kind: PartToolCall,
						Call: &ToolCall{
							CallID:    "c_custom",
							Tool:      ToolRef{Kind: ToolCustom, Name: "apply_patch"},
							Arguments: "*** Begin Patch\n+line\n*** End Patch",
						},
					},
				},
			},
			{
				Role: RoleTool,
				Parts: []Part{{
					Kind:   PartToolResult,
					Result: &ToolResult{CallID: "c_json", Content: []Part{{Kind: PartText, Text: "ok"}}},
				}},
			},
		},
		Tools: []Tool{
			{
				Kind:        ToolFunction,
				Name:        "query",
				InputSchema: jsontext.Value(`{"properties":{"a":{"default":9007199254740993}},"type":"object"}`),
				Cache:       &CacheMark{},
			},
		},
		ParallelToolCalls: &parallel,
		MaxOutputTokens:   &maxOut,
		ServiceTier:       "priority",
		Reasoning:         &ReasoningConfig{Effort: "medium", Summary: "auto", IncludeEncrypted: true},
		Output:            &OutputFormat{Verbosity: "medium"},
	}

	docA := Essence(reqA)
	// Caller-owned reqA.Messages[2].Parts[0].Result.Tool must not be mutated in place.
	require.Equal(t, ToolRef{}, reqA.Messages[2].Parts[0].Result.Tool)
	require.Equal(t, ToolRef{Kind: ToolFunction, Name: "query"}, docA.Messages[2].Parts[0].Result.Tool)

	// Cross-protocol injected transport controls without LossKind handlers are omitted.
	require.Nil(t, docA.ParallelToolCalls)
	require.Nil(t, docA.MaxOutputTokens)
	require.Empty(t, docA.ServiceTier)
	require.Nil(t, docA.Reasoning)
	require.Nil(t, docA.Output)

	// Base64 media keeps normalized MIME without Name/Detail; URL media omits MIME/Name/Detail.
	require.Equal(t, "image/png", docA.Messages[0].Parts[0].Media.MIME)
	require.Empty(t, docA.Messages[0].Parts[0].Media.Name)
	require.Empty(t, docA.Messages[0].Parts[0].Media.Detail)
	require.Empty(t, docA.Messages[0].Parts[1].Media.MIME)
	require.Empty(t, docA.Messages[0].Parts[1].Media.Name)
	require.Empty(t, docA.Messages[0].Parts[1].Media.Detail)

	require.Equal(t, "ephemeral", docA.System[0].CacheTTL)
	require.Equal(t, "ephemeral", docA.Tools[0].CacheTTL)
	require.Equal(t, []string{"First segment. Second segment."}, docA.Messages[1].Parts[0].Reasoning.Segments)
	require.Equal(t, []string{"Recovered plaintext thought"}, docA.Messages[1].Parts[1].Reasoning.Segments)
	require.Nil(t, docA.Messages[1].Parts[1].Reasoning.Token)
	require.False(t, docA.Messages[1].Parts[1].Reasoning.Summary)

	require.Equal(t, []string{"Authoritative signed Claude thought."}, docA.Messages[1].Parts[2].Reasoning.Segments)
	require.Equal(t, &EssenceToken{Scheme: SchemeAnthropic, Data: "EqQ_sig_real"}, docA.Messages[1].Parts[2].Reasoning.Token)

	require.Equal(t, `{"a":9007199254740993,"b":1}`, docA.Messages[1].Parts[3].Call.Arguments)
	require.Equal(t, `{}`, docA.Messages[1].Parts[4].Call.Arguments)
	require.Equal(t, "*** Begin Patch\n+line\n*** End Patch", docA.Messages[1].Parts[5].Call.Arguments)

	// Structured OutputFormat populates doc.Output without copying standalone Verbosity.
	reqStructured := reqA.Clone()
	reqStructured.Output = &OutputFormat{
		Kind:      "json_schema",
		Name:      "report",
		Schema:    jsontext.Value(`{"type":"object"}`),
		Verbosity: "low",
	}
	docStructured := Essence(reqStructured)
	require.NotNil(t, docStructured.Output)
	require.Equal(t, "json_schema", docStructured.Output.Kind)
	require.Equal(t, "report", docStructured.Output.Name)
	require.NotEmpty(t, docStructured.Output.SchemaHash)
	require.Empty(t, docStructured.Output.Verbosity)

	// Adjacent integer 9007199254740992 must produce a distinct schema hash (no float64 rounding).
	reqDifferentInt := reqA.Clone()
	reqDifferentInt.Tools[0].InputSchema = jsontext.Value(`{"type":"object","properties":{"a":{"default":9007199254740992}}}`)
	docDiff := Essence(reqDifferentInt)
	require.NotEqual(t, docA.Tools[0].SchemaHash, docDiff.Tools[0].SchemaHash)

	// ResponseEssence unpacks carriers and omits protocol-asymmetric Finish, Usage, and ServiceTier.
	w5m := int64(30)
	resp := &Response{
		ID: "r1",
		Parts: []Part{{
			Kind: PartReasoning,
			Reasoning: &Reasoning{
				Segments: []string{"part a", " part b"},
				Token:    &ReplayToken{Data: openAICarrier},
			},
		}},
		Finish:      Finish{Reason: FinishStopSequence, StopSequence: "</answer>"},
		Usage:       &Usage{Input: 10, CacheWrite: 30, CacheWrite5m: &w5m, Output: 5},
		ServiceTier: "priority",
		Extra: Extra{
			Origin:  protocol.ProtocolOpenAIResponses,
			Members: map[string]jsontext.Value{"x": jsontext.Value(`1`)},
		},
	}
	rDoc := ResponseEssence(resp)
	require.Equal(t, []string{"part a part b"}, rDoc.Parts[0].Reasoning.Segments)
	require.Equal(t, &EssenceToken{Scheme: SchemeOpenAI, ProviderID: 77, Data: "gAAAAABnCipher"}, rDoc.Parts[0].Reasoning.Token)
	require.True(t, rDoc.HasExtra)
	require.Nil(t, rDoc.Finish)
	require.Nil(t, rDoc.Usage)
	require.Empty(t, rDoc.ServiceTier)
}
