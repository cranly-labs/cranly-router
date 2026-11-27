package ir_test

import (
	"bytes"
	"encoding/json/jsontext"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestEssenceCanonicalHashMatchesTestkit compares schema and grammar hashes
// produced by ir.Essence against testkit.CanonicalJSONHash on 64-bit integers,
// exponents, and nested maps. Both implementations must yield identical digests.
func TestEssenceCanonicalHashMatchesTestkit(t *testing.T) {
	schemas := []string{
		`{"b":1.000,"a":9007199254740993,"nested":{"z":-0.0,"y":1e21,"x":1e-6}}`,
		`{"type":"object","properties":{"arr":[1,2.0,3e0,true,null,"ok"]},"additionalProperties":false}`,
	}
	for _, raw := range schemas {
		wantHash, err := testkit.CanonicalJSONHash([]byte(raw))
		require.NoError(t, err)

		doc := ir.Essence(&ir.Request{
			Tools: []ir.Tool{{
				Kind:        ir.ToolFunction,
				Name:        "fn",
				InputSchema: jsontext.Value(raw),
			}},
		})
		require.Equal(t, wantHash, doc.Tools[0].SchemaHash)
	}
}

// TestEssenceReconcilesWithTestkitVerifyEssence builds source and target
// ir.Request and ir.Response pairs exercising carrier unpacking, custom tool
// lowering, media relocation, system folding, and loss projection. Every
// reported Loss must reconcile completely under testkit.VerifyEssence.
func TestEssenceReconcilesWithTestkitVerifyEssence(t *testing.T) {
	plainCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemePlain,
		Text:   "Inspect patch target before editing.",
	})
	require.NoError(t, err)

	foreignCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAAABnForeignCipher",
		ProviderID: 101,
	})
	require.NoError(t, err)

	srcReq := &ir.Request{
		Model: "gpt-5",
		System: []ir.Part{
			{Kind: ir.PartText, Text: "x-anthropic-billing-header: cch=abc;"},
			{Kind: ir.PartText, Text: "System base.", Cache: &ir.CacheMark{}},
		},
		Messages: []ir.Message{
			{
				Role:  ir.RoleDeveloper,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Developer addendum."}},
			},
			{
				Role: ir.RoleUser,
				Parts: []ir.Part{
					{Kind: ir.PartText, Text: "Apply the config fix."},
					{
						Kind: ir.PartOpaque,
						Opaque: &ir.OpaqueItem{
							Origin: protocol.ProtocolOpenAIResponses,
							Type:   "compaction",
							Raw:    jsontext.Value(`{"type":"compaction"}`),
						},
					},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							// Encrypted-only plain carrier recovered for Chat tool_turns.
							Summary: true,
							Token:   &ir.ReplayToken{Data: plainCarrier},
						},
					},
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Segments: []string{"Foreign signed step"},
							Token:    &ir.ReplayToken{Data: foreignCarrier},
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_patch_1",
							Tool:      ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"},
							Arguments: "*** Begin Patch\n*** End Patch",
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID: "call_patch_1",
						Content: []ir.Part{
							{Kind: ir.PartText, Text: "patched"},
							{
								Kind: ir.PartMedia,
								Media: &ir.Media{
									Type:   ir.MediaImage,
									MIME:   "image/png",
									Source: ir.MediaSourceBase64,
									Data:   testkit.SyntheticPNGDataBase64,
								},
							},
						},
					},
				}},
			},
		},
		Tools: []ir.Tool{
			{
				Kind: ir.ToolHosted,
				Name: "web_search",
				Hosted: &ir.OpaqueItem{
					Origin: protocol.ProtocolOpenAIResponses,
					Type:   "web_search",
					Raw:    jsontext.Value(`{"type":"web_search"}`),
				},
			},
			{
				Kind:        ir.ToolCustom,
				Name:        "apply_patch",
				Description: "Patch workspace files.",
				Format:      jsontext.Value(`{"type":"grammar","syntax":"lark"}`),
			},
		},
	}

	dstReq := &ir.Request{
		Model: "deepseek-reasoner",
		System: []ir.Part{
			{Kind: ir.PartText, Text: "System base.\nDeveloper addendum."},
		},
		Messages: []ir.Message{
			{
				Role:  ir.RoleUser,
				Parts: []ir.Part{{Kind: ir.PartText, Text: "Apply the config fix."}},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{
						Kind:      ir.PartReasoning,
						Reasoning: &ir.Reasoning{Segments: []string{"Inspect patch target before editing."}},
					},
					{
						Kind:      ir.PartReasoning,
						Reasoning: &ir.Reasoning{Segments: []string{"Foreign signed step"}},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "call_patch_1",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "apply_patch"},
							Arguments: `{"input":"*** Begin Patch\n*** End Patch"}`,
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID:  "call_patch_1",
						Content: []ir.Part{{Kind: ir.PartText, Text: "patched"}},
					},
				}},
			},
			{
				Role: ir.RoleUser,
				Parts: []ir.Part{
					{Kind: ir.PartText, Text: "[Tool output image for call_patch_1]"},
					{
						Kind: ir.PartMedia,
						Media: &ir.Media{
							Type:   ir.MediaImage,
							MIME:   "IMAGE/PNG",
							Source: ir.MediaSourceBase64,
							Data:   testkit.SyntheticPNGDataBase64,
						},
					},
				},
			},
		},
		Tools: []ir.Tool{
			{
				Kind:        ir.ToolFunction,
				Name:        "apply_patch",
				Description: "Patch workspace files.",
				InputSchema: jsontext.Value(`{"type":"object","properties":{"input":{"type":"string"}}}`),
			},
		},
	}

	losses := []ir.Loss{
		{Kind: ir.LossClientMarkerDropped, Path: "system[0]"},
		{Kind: ir.LossCacheMarkDropped, Path: "system[1]"},
		{Kind: ir.LossSystemRelocated, Path: "messages[0]"},
		{Kind: ir.LossOpaqueItemDropped, Path: "messages[1].parts[1]"},
		{Kind: ir.LossReplayTokenDropped, Path: "messages[2].parts[1].reasoning.token"},
		{Kind: ir.LossToolLowered, Path: "messages[2].parts[2]"},
		{Kind: ir.LossToolLowered, Path: "messages[3].parts[0]"},
		{Kind: ir.LossMediaRelocated, Path: "messages[3].parts[0]"},
		{Kind: ir.LossHostedToolDropped, Path: "tools[0]"},
		{Kind: ir.LossToolLowered, Path: "tools[1]"},
	}

	srcEssence := ir.Essence(srcReq)
	dstEssence := ir.Essence(dstReq)
	diffs, err := testkit.VerifyEssence(srcEssence, dstEssence, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

	projected, err := testkit.ApplyLosses(srcEssence, losses)
	require.NoError(t, err)
	require.Len(t, projected.Tools, 1)
}

// TestEssenceCrossProtocolDefaultsMultiSegmentMediaAndS4 checks Law Q4 and S4
// reconciliation across injected Responses/Messages defaults, multi-segment
// reasoning summaries, media detail/filename/URL MIME differences, and folded
// stop_sequence/refusal response finishes. All pairs must reconcile with zero diffs.
func TestEssenceCrossProtocolDefaultsMultiSegmentMediaAndS4(t *testing.T) {
	// Q4: Messages/Chat request with small max_tokens, image detail, PDF filename,
	// and URL image converted to Responses/Gemini with injected defaults and URL fileData MIME.
	smallMax := int64(32)
	injectedMax := int64(128)
	parallelTrue := true

	clientReq := &ir.Request{
		Model:           "claude-sonnet-4-5",
		MaxOutputTokens: &smallMax,
		ServiceTier:     "priority",
		Messages: []ir.Message{{
			Role: ir.RoleUser,
			Parts: []ir.Part{
				{
					Kind: ir.PartMedia,
					Media: &ir.Media{
						Type:   ir.MediaImage,
						Source: ir.MediaSourceURL,
						Data:   "https://example.com/chart.png",
						Detail: "high",
					},
				},
				{
					Kind: ir.PartMedia,
					Media: &ir.Media{
						Type:   ir.MediaDocument,
						MIME:   "application/pdf",
						Source: ir.MediaSourceBase64,
						Data:   testkit.SyntheticPDFDataBase64,
						Name:   "spec.pdf",
					},
				},
			},
		}},
	}

	upstreamDecodedReq := &ir.Request{
		Model:             "gpt-5",
		ParallelToolCalls: &parallelTrue,
		MaxOutputTokens:   &injectedMax,
		Reasoning:         &ir.ReasoningConfig{Effort: "medium", Summary: "auto", IncludeEncrypted: true},
		Output:            &ir.OutputFormat{Verbosity: "medium"},
		Messages: []ir.Message{{
			Role: ir.RoleUser,
			Parts: []ir.Part{
				{
					Kind: ir.PartMedia,
					Media: &ir.Media{
						Type:   ir.MediaImage,
						MIME:   "image/png",
						Source: ir.MediaSourceURL,
						Data:   "https://example.com/chart.png",
					},
				},
				{
					Kind: ir.PartMedia,
					Media: &ir.Media{
						Type:   ir.MediaDocument,
						MIME:   "application/pdf",
						Source: ir.MediaSourceBase64,
						Data:   testkit.SyntheticPDFDataBase64,
					},
				},
			},
		}},
	}

	q4Diffs, err := testkit.VerifyEssence(ir.Essence(clientReq), ir.Essence(upstreamDecodedReq), nil)
	require.NoError(t, err)
	require.Empty(t, q4Diffs, testkit.FormatJSONDiff(q4Diffs))

	// S4: Multi-segment Responses stream with OpenAI token, stop_sequence/usage/tier
	// converted to Messages or Chat where segments join, Finish folds to stop, and
	// IncludeUsage=false omits client stream usage.
	sealedStr, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAAABnStreamToken",
		ProviderID: 101,
	})
	require.NoError(t, err)

	w30m := int64(80)
	upstreamAggResp := &ir.Response{
		ID: "resp_s4",
		Parts: []ir.Part{
			{
				Kind: ir.PartReasoning,
				Reasoning: &ir.Reasoning{
					Segments: []string{"Segment one. ", "Segment two."},
					Summary:  true,
					Token: &ir.ReplayToken{
						Scheme:     ir.SchemeOpenAI,
						ProviderID: 101,
						Data:       "gAAAAABnStreamToken",
					},
				},
			},
			{Kind: ir.PartRefusal, Text: "Cannot assist with that request."},
		},
		Finish:      ir.Finish{Reason: ir.FinishStopSequence, StopSequence: "</answer>"},
		Usage:       &ir.Usage{Input: 50, CacheWrite: 80, CacheWrite30m: &w30m, Output: 20, Reasoning: 12, Speed: "fast"},
		ServiceTier: "priority",
	}

	clientAggResp := &ir.Response{
		ID: "msg_s4",
		Parts: []ir.Part{
			{
				Kind: ir.PartReasoning,
				Reasoning: &ir.Reasoning{
					Segments: []string{"Segment one. Segment two."},
					Token:    &ir.ReplayToken{Data: sealedStr},
				},
			},
			{Kind: ir.PartRefusal, Text: "Cannot assist with that request."},
		},
		Finish: ir.Finish{Reason: ir.FinishStop},
		Usage:  nil,
	}

	s4Diffs, err := testkit.VerifyEssence(ir.ResponseEssence(upstreamAggResp), ir.ResponseEssence(clientAggResp), nil)
	require.NoError(t, err)
	require.Empty(t, s4Diffs, testkit.FormatJSONDiff(s4Diffs))
}

// TestCarrierParsesAndRoundTripsEmbeddedWireFixtures scans Batch 0.5c wire
// request fixtures in testkit.DefaultCorpus for trc1 envelopes and legacy bare
// signatures. Embedded carriers must round-trip byte-for-byte via FormatCarrier
// while bare "gAAAA..." ciphertexts in legacy_history must return false.
func TestCarrierParsesAndRoundTripsEmbeddedWireFixtures(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	scenarios := []string{
		"reasoning_token_foreign",
		"reasoning_carrier_roundtrip",
		"reasoning_carrier_native",
		"reasoning_encrypted_only",
	}

	carrierCount := 0
	for _, sc := range scenarios {
		fixtures, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Kind:     testkit.FixtureRequest,
			Scenario: sc,
		})
		require.NoError(t, err)
		require.NotEmpty(t, fixtures)
		for _, fix := range fixtures {
			for _, candidate := range extractJSONStringsWithPrefix(t, fix.Data, ir.CarrierPrefix) {
				// Skip user prompt prose containing literal "trc1:" explanations.
				if strings.ContainsRune(candidate, ' ') {
					_, ok := ir.ParseCarrier(candidate)
					require.False(t, ok)
					continue
				}
				parsed, ok := ir.ParseCarrier(candidate)
				require.True(t, ok, "fixture %s carrier %q must parse", fix.Path, candidate)
				reformatted, fmtErr := ir.FormatCarrier(parsed)
				require.NoError(t, fmtErr)
				require.Equal(t, candidate, reformatted, "fixture %s carrier must round-trip byte-for-byte", fix.Path)
				carrierCount++
			}
		}
	}
	require.GreaterOrEqual(t, carrierCount, 12)

	// Legacy bare tokens in legacy_history must never parse as trc1 carriers.
	legacyFixtures, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Kind:     testkit.FixtureRequest,
		Scenario: "legacy_history",
	})
	require.NoError(t, err)
	require.NotEmpty(t, legacyFixtures)
	sawGAAAA := false
	for _, fix := range legacyFixtures {
		for _, str := range extractJSONStringsWithPrefix(t, fix.Data, "gAAAA") {
			sawGAAAA = true
			_, ok := ir.ParseCarrier(str)
			require.False(t, ok, "bare token %q in %s must not parse as carrier", str, fix.Path)
		}
	}
	require.True(t, sawGAAAA)
}

// extractJSONStringsWithPrefix collects all JSON string values in raw starting
// with prefix. Decoding stops at EOF or the first token error and returns the
// matched strings in scan order.
func extractJSONStringsWithPrefix(t *testing.T, raw []byte, prefix string) []string {
	t.Helper()
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	var matches []string
	for {
		tok, err := dec.ReadToken()
		if err != nil {
			break
		}
		if tok.Kind() == '"' && strings.HasPrefix(tok.String(), prefix) {
			matches = append(matches, tok.String())
		}
	}
	return matches
}
