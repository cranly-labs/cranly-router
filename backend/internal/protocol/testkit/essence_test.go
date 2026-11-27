package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestSupportedLossKindsCoversAllIRLosses reconciles a multi-turn EssenceDoc
// pair using original unshifted source paths across all twenty-three canonical
// ir.LossKind handlers and injects silent text and schema mutations. Reported
// losses must explain every difference without index drift, while unreported
// edits or missing target schemas must fail verification.
func TestSupportedLossKindsCoversAllIRLosses(t *testing.T) {
	expected := []ir.LossKind{
		ir.LossAnnotationDropped,
		ir.LossCacheMarkDropped,
		ir.LossCandidateCountIgnored,
		ir.LossClientMarkerDropped,
		ir.LossExtraDropped,
		ir.LossHostedToolDropped,
		ir.LossIDRewritten,
		ir.LossMediaRelocated,
		ir.LossMediaUnsupported,
		ir.LossOpaqueItemDropped,
		ir.LossOutputFormatApproximated,
		ir.LossPartsReordered,
		ir.LossReasoningTextDropped,
		ir.LossReasoningTextInlined,
		ir.LossRefusalInlined,
		ir.LossReplayTokenDropped,
		ir.LossSamplingDropped,
		ir.LossSchemaApproximated,
		ir.LossStopDropped,
		ir.LossSystemRelocated,
		ir.LossToolChoiceApproximated,
		ir.LossToolLowered,
		ir.LossToolPairingRepaired,
	}
	require.Equal(t, expected, SupportedLossKinds())

	temp := 0.7
	topK := int64(40)
	candCount := int64(2)
	src := ir.EssenceDoc{
		System: []ir.EssencePart{
			{Kind: ir.PartText, Text: "x-anthropic-billing-header: cch=123;"},
			{Kind: ir.PartText, Text: "Core rule", CacheTTL: "5m"},
		},
		Messages: []ir.EssenceMessage{
			{Role: ir.RoleDeveloper, Parts: []ir.EssencePart{{Kind: ir.PartText, Text: "Dev rule"}}},
			{
				Role:     ir.RoleUser,
				ID:       "msg_u1",
				HasExtra: true,
				Parts: []ir.EssencePart{
					{
						Kind:        ir.PartText,
						Text:        "Hello",
						Annotations: []ir.EssenceAnnotation{{URL: "https://example.com"}},
					},
					{
						Kind:  ir.PartMedia,
						Media: &ir.EssenceMedia{Type: ir.MediaAudio, Source: ir.MediaSourceBase64, ContentHash: "aud1"},
					},
					{Kind: ir.PartOpaque, OpaqueType: "openai_responses:compaction"},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolCall,
						Call: &ir.EssenceToolCall{
							CallID:    "call_orig",
							Tool:      ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"},
							Arguments: "*** Begin Patch",
						},
					},
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.EssenceReasoning{
							Segments: []string{"plan step"},
							Token:    &ir.EssenceToken{Scheme: ir.SchemeAnthropic, Data: "sig1"},
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolResult,
						Result: &ir.EssenceToolResult{
							CallID: "call_orig",
							Content: []ir.EssencePart{
								{Kind: ir.PartText, Text: "patched"},
								{Kind: ir.PartMedia, Media: &ir.EssenceMedia{Type: ir.MediaImage, MIME: "image/png", Source: ir.MediaSourceBase64, ContentHash: "img_hash"}},
							},
						},
					},
					{
						Kind:   ir.PartToolResult,
						Result: &ir.EssenceToolResult{CallID: "orphan_call", Content: []ir.EssencePart{{Kind: ir.PartText, Text: "stale"}}},
					},
				},
			},
		},
		Tools: []ir.EssenceTool{
			{Kind: ir.ToolHosted, Name: "web_search", HostedType: "openai_responses:web_search"},
			{Kind: ir.ToolCustom, Name: "apply_patch", Description: "patch file", FormatHash: "fmt1"},
		},
		ToolChoice: &ir.EssenceToolChoice{Mode: ir.ToolChoiceAuto},
		Sampling:   ir.Sampling{Temperature: &temp, TopK: &topK, CandidateCount: &candCount},
		Stop:       []string{"STOP"},
		Output:     &ir.EssenceOutputFormat{Kind: "json_schema", Name: "out", SchemaHash: "sch_orig"},
		HasExtra:   true,
	}

	dst := ir.EssenceDoc{
		System: []ir.EssencePart{
			{Kind: ir.PartText, Text: "Core rule\nDev rule"},
		},
		Messages: []ir.EssenceMessage{
			{
				Role: ir.RoleUser,
				ID:   "msg_rewritten",
				Parts: []ir.EssencePart{
					{Kind: ir.PartText, Text: "Hello"},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{Kind: ir.PartText, Text: "plan step"},
					{
						Kind: ir.PartToolCall,
						Call: &ir.EssenceToolCall{
							CallID:    "call_new",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "apply_patch"},
							Arguments: `{"input":"*** Begin Patch"}`,
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolResult,
						Result: &ir.EssenceToolResult{
							CallID:  "call_new",
							Content: []ir.EssencePart{{Kind: ir.PartText, Text: "patched"}},
						},
					},
				},
			},
			{
				Role: ir.RoleUser,
				Parts: []ir.EssencePart{
					{Kind: ir.PartText, Text: "[Tool output image for call_new]"},
					{Kind: ir.PartMedia, Media: &ir.EssenceMedia{Type: ir.MediaImage, MIME: "image/png", Source: ir.MediaSourceBase64, ContentHash: "img_hash"}},
				},
			},
		},
		Tools: []ir.EssenceTool{
			{Kind: ir.ToolFunction, Name: "apply_patch", Description: "patch file", SchemaHash: "sch_lowered"},
		},
		Sampling: ir.Sampling{Temperature: &temp},
		Output:   &ir.EssenceOutputFormat{Kind: "json_object"},
	}

	// Note that all paths use original source coordinates (system[1], messages[1],
	// messages[1].parts[2], messages[2], messages[3], tools[1] before tools[0]).
	losses := []ir.Loss{
		{Kind: ir.LossClientMarkerDropped, Path: "system[0]"},
		{Kind: ir.LossCacheMarkDropped, Path: "system[1]"},
		{Kind: ir.LossSystemRelocated, Path: "messages[0]"},
		{Kind: ir.LossExtraDropped, Path: ""},
		{Kind: ir.LossExtraDropped, Path: "messages[1]"},
		{Kind: ir.LossIDRewritten, Path: "messages[1].id"},
		{Kind: ir.LossAnnotationDropped, Path: "messages[1].parts[0]"},
		{Kind: ir.LossMediaUnsupported, Path: "messages[1].parts[1]"},
		{Kind: ir.LossOpaqueItemDropped, Path: "messages[1].parts[2]"},
		{Kind: ir.LossReplayTokenDropped, Path: "messages[2].parts[1]"},
		{Kind: ir.LossReasoningTextInlined, Path: "messages[2].parts[1]"},
		{Kind: ir.LossToolLowered, Path: "messages[2].parts[0]"},
		{Kind: ir.LossIDRewritten, Path: "messages[2].parts[0].call_id"},
		{Kind: ir.LossPartsReordered, Path: "messages[2]"},
		{Kind: ir.LossToolPairingRepaired, Path: "messages[3].parts[1]"},
		{Kind: ir.LossMediaRelocated, Path: "messages[3].parts[0]"},
		{Kind: ir.LossToolLowered, Path: "tools[1]"},
		{Kind: ir.LossHostedToolDropped, Path: "tools[0]"},
		{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"},
		{Kind: ir.LossSamplingDropped, Path: "sampling.top_k"},
		{Kind: ir.LossCandidateCountIgnored, Path: "sampling.candidate_count"},
		{Kind: ir.LossStopDropped, Path: "stop"},
		{Kind: ir.LossOutputFormatApproximated, Path: "output"},
	}

	diffs, err := VerifyEssence(src, dst, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))

	// Silent mutation of Hello -> Tampered without a Loss must be caught.
	tampered, err := cloneEssenceDoc(dst)
	require.NoError(t, err)
	tampered.Messages[0].Parts = []ir.EssencePart{{Kind: ir.PartText, Text: "Tampered"}}
	diffs, err = VerifyEssence(src, tampered, losses)
	require.NoError(t, err)
	require.NotEmpty(t, diffs)

	// Reporting schema_approximated when dst completely lost the schema must error.
	badSchemaSrc := ir.EssenceDoc{Tools: []ir.EssenceTool{{Kind: ir.ToolFunction, Name: "fn", SchemaHash: "h1"}}}
	badSchemaDst := ir.EssenceDoc{Tools: []ir.EssenceTool{{Kind: ir.ToolFunction, Name: "fn", SchemaHash: ""}}}
	_, err = VerifyEssence(badSchemaSrc, badSchemaDst, []ir.Loss{{Kind: ir.LossSchemaApproximated, Path: "tools[0].input_schema"}})
	require.Error(t, err)
}

// TestEssenceCrossStageIndicesAndConsecutiveDrops exercises pipeline-ordered
// tool losses (schema_approximated or tool_lowered on tools[1] followed by
// hosted_tool_dropped on tools[0]) and consecutive opaque/media drops in one
// message. Surviving ordinals must align without index drift or false errors.
func TestEssenceCrossStageIndicesAndConsecutiveDrops(t *testing.T) {
	src := ir.EssenceDoc{
		Messages: []ir.EssenceMessage{
			{
				Role: ir.RoleUser,
				Parts: []ir.EssencePart{
					{Kind: ir.PartOpaque, OpaqueType: "openai_responses:item_1"},
					{Kind: ir.PartOpaque, OpaqueType: "openai_responses:item_2"},
					{Kind: ir.PartMedia, Media: &ir.EssenceMedia{Type: ir.MediaVideo, Source: ir.MediaSourceURL, ContentHash: "v1"}},
					{Kind: ir.PartMedia, Media: &ir.EssenceMedia{Type: ir.MediaAudio, Source: ir.MediaSourceURL, ContentHash: "a1"}},
					{Kind: ir.PartText, Text: "surviving question"},
				},
			},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{Kind: ir.PartReasoning, Reasoning: &ir.EssenceReasoning{Segments: []string{"dropped thought"}}},
					{Kind: ir.PartText, Text: "final answer"},
				},
			},
		},
		Tools: []ir.EssenceTool{
			{Kind: ir.ToolHosted, Name: "web_search", HostedType: "openai_responses:web_search"},
			{Kind: ir.ToolFunction, Name: "lookup", Description: "search db", SchemaHash: "orig_schema_hash"},
		},
	}

	dst := ir.EssenceDoc{
		Messages: []ir.EssenceMessage{
			{
				Role:  ir.RoleUser,
				Parts: []ir.EssencePart{{Kind: ir.PartText, Text: "surviving question"}},
			},
			{
				Role:  ir.RoleAssistant,
				Parts: []ir.EssencePart{{Kind: ir.PartText, Text: "final answer"}},
			},
		},
		Tools: []ir.EssenceTool{
			{Kind: ir.ToolFunction, Name: "lookup", Description: "search db (hint)", SchemaHash: "gemini_cleaned_hash"},
		},
	}

	losses := []ir.Loss{
		{Kind: ir.LossSchemaApproximated, Path: "tools[1].input_schema"},
		{Kind: ir.LossHostedToolDropped, Path: "tools[0]"},
		{Kind: ir.LossOpaqueItemDropped, Path: "messages[0].parts[0]"},
		{Kind: ir.LossOpaqueItemDropped, Path: "messages[0].parts[1]"},
		{Kind: ir.LossMediaUnsupported, Path: "messages[0].parts[2]"},
		{Kind: ir.LossMediaUnsupported, Path: "messages[0].parts[3]"},
		{Kind: ir.LossReasoningTextDropped, Path: "messages[1].parts[0]"},
	}

	diffs, err := VerifyEssence(src, dst, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))

	projected, err := ApplyLosses(src, losses)
	require.NoError(t, err)
	require.Len(t, projected.Tools, 1)
	require.Equal(t, "lookup", projected.Tools[0].Name)
	require.Len(t, projected.Messages[0].Parts, 1)
	require.Equal(t, "surviving question", projected.Messages[0].Parts[0].Text)

	// Duplicate drop at the same original index must error rather than shifting onto parts[1].
	_, err = ApplyLosses(src, []ir.Loss{
		{Kind: ir.LossOpaqueItemDropped, Path: "messages[0].parts[0]"},
		{Kind: ir.LossOpaqueItemDropped, Path: "messages[0].parts[0]"},
	})
	require.Error(t, err)
}

// TestVerifyEssenceRelocatedToolMediaMarkerAndOutputDescription verifies that
// LossMediaRelocated strips synthetic Chat role="tool" relocation markers on
// both text-plus-media and media-only tool results. Approximating OutputFormat
// descriptions must also reconcile cleanly via LossOutputFormatApproximated.
func TestVerifyEssenceRelocatedToolMediaMarkerAndOutputDescription(t *testing.T) {
	imgPart := ir.EssencePart{
		Kind: ir.PartMedia,
		Media: &ir.EssenceMedia{
			Type:        ir.MediaImage,
			MIME:        "image/png",
			Source:      ir.MediaSourceBase64,
			ContentHash: "png_hash_1",
		},
	}
	src := ir.EssenceDoc{
		Output: &ir.EssenceOutputFormat{
			Kind:        "json_schema",
			Name:        "report",
			Description: "Detailed report",
			SchemaHash:  "sch_1",
		},
		Messages: []ir.EssenceMessage{
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{Kind: ir.PartToolCall, Call: &ir.EssenceToolCall{CallID: "c1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "snap"}, Arguments: "{}"}},
					{Kind: ir.PartToolCall, Call: &ir.EssenceToolCall{CallID: "c2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "raw"}, Arguments: "{}"}},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{{
					Kind: ir.PartToolResult,
					Result: &ir.EssenceToolResult{
						CallID: "c1",
						Content: []ir.EssencePart{
							{Kind: ir.PartText, Text: "Captured chart."},
							imgPart,
						},
					},
				}},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{{
					Kind: ir.PartToolResult,
					Result: &ir.EssenceToolResult{
						CallID:  "c2",
						Content: []ir.EssencePart{imgPart},
					},
				}},
			},
		},
	}

	dst := ir.EssenceDoc{
		Output: &ir.EssenceOutputFormat{
			Kind:       "json_schema",
			Name:       "report",
			SchemaHash: "sch_1",
		},
		Messages: []ir.EssenceMessage{
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{Kind: ir.PartToolCall, Call: &ir.EssenceToolCall{CallID: "c1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "snap"}, Arguments: "{}"}},
					{Kind: ir.PartToolCall, Call: &ir.EssenceToolCall{CallID: "c2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "raw"}, Arguments: "{}"}},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{{
					Kind: ir.PartToolResult,
					Result: &ir.EssenceToolResult{
						CallID: "c1",
						Content: []ir.EssencePart{
							{Kind: ir.PartText, Text: "Captured chart.\n" + relocatedToolMediaMarker},
						},
					},
				}},
			},
			{
				Role: ir.RoleUser,
				Parts: []ir.EssencePart{
					{Kind: ir.PartText, Text: "[Tool output media for call c1]"},
					imgPart,
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{{
					Kind: ir.PartToolResult,
					Result: &ir.EssenceToolResult{
						CallID: "c2",
						Content: []ir.EssencePart{
							{Kind: ir.PartText, Text: relocatedToolMediaMarker},
						},
					},
				}},
			},
			{
				Role: ir.RoleUser,
				Parts: []ir.EssencePart{
					{Kind: ir.PartText, Text: "[Tool output media for call c2]"},
					imgPart,
				},
			},
		},
	}

	losses := []ir.Loss{
		{Kind: ir.LossOutputFormatApproximated, Path: "output", Detail: "description"},
		{Kind: ir.LossMediaRelocated, Path: "messages[1].parts[0]"},
		{Kind: ir.LossMediaRelocated, Path: "messages[2].parts[0]"},
	}

	diffs, err := VerifyEssence(src, dst, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))
}

// TestVerifyEssenceSeparatesPartIDFromCallIDOnToolParts checks that reporting
// LossIDRewritten at ".id" versus ".call_id" on tool_call and tool_result parts
// normalizes Part.ID independently of CallID. Synthesizing a Responses item ID
// while keeping CallID intact, or rewriting both fields together, must pass
// VerifyEssence without masking an unreported CallID change.
func TestVerifyEssenceSeparatesPartIDFromCallIDOnToolParts(t *testing.T) {
	src := ir.EssenceDoc{
		Parts: []ir.EssencePart{
			{
				Kind: ir.PartToolCall,
				ID:   "",
				Call: &ir.EssenceToolCall{
					CallID:    "call_kept",
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "ping"},
					Arguments: "{}",
				},
			},
			{
				Kind: ir.PartToolCall,
				ID:   "",
				Call: &ir.EssenceToolCall{
					CallID:    "",
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "pong"},
					Arguments: "{}",
				},
			},
		},
	}
	dst := ir.EssenceDoc{
		Parts: []ir.EssencePart{
			{
				Kind: ir.PartToolCall,
				ID:   "fc_synth_0",
				Call: &ir.EssenceToolCall{
					CallID:    "call_kept",
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "ping"},
					Arguments: "{}",
				},
			},
			{
				Kind: ir.PartToolCall,
				ID:   "fc_synth_1",
				Call: &ir.EssenceToolCall{
					CallID:    "call_synth_1",
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "pong"},
					Arguments: "{}",
				},
			},
		},
	}

	losses := []ir.Loss{
		{Kind: ir.LossIDRewritten, Path: "parts[0].id"},
		{Kind: ir.LossIDRewritten, Path: "parts[1].id"},
		{Kind: ir.LossIDRewritten, Path: "parts[1].call_id"},
	}
	diffs, err := VerifyEssence(src, dst, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))

	// Mutating CallID when only ".id" was reported must still be flagged.
	tampered, err := cloneEssenceDoc(dst)
	require.NoError(t, err)
	tampered.Parts[0].Call.CallID = "call_silently_changed"
	diffs, err = VerifyEssence(src, tampered, losses)
	require.NoError(t, err)
	require.NotEmpty(t, diffs)
}

// TestVerifyEssenceSameKindNameSanitizationAndDuplicateOrphanSurvival reproduces
// native custom and namespaced tools whose identifiers were sanitized under
// LossToolLowered alongside duplicate CallIDs where one orphan was tombstoned.
// VerifyEssence must preserve surviving ToolCustom free-text inputs and paired
// calls while aligning empty client_search arguments lowered into functions.
func TestVerifyEssenceSameKindNameSanitizationAndDuplicateOrphanSurvival(t *testing.T) {
	src := ir.EssenceDoc{
		Tools: []ir.EssenceTool{
			{Kind: ir.ToolCustom, Name: "bad.custom/name", FormatHash: "fmt_1"},
			{Kind: ir.ToolFunction, Namespace: "ns", Name: "bad.fn/name", SchemaHash: "sch_1"},
		},
		Messages: []ir.EssenceMessage{
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolCall,
						Call: &ir.EssenceToolCall{
							CallID:    "c_shared",
							Tool:      ir.ToolRef{Kind: ir.ToolCustom, Name: "bad.custom/name"},
							Arguments: "*** Begin Patch\n+ok\n*** End Patch",
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.EssenceToolCall{
							CallID:    "c_shared",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "stale_dup"},
							Arguments: "{}",
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.EssenceToolCall{
							CallID:    "c_search",
							Tool:      ir.ToolRef{Kind: ir.ToolClientSearch},
							Arguments: "",
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolResult,
						Result: &ir.EssenceToolResult{
							CallID:  "c_shared",
							Tool:    ir.ToolRef{Kind: ir.ToolCustom, Name: "bad.custom/name"},
							Content: []ir.EssencePart{{Kind: ir.PartText, Text: "applied"}},
						},
					},
					{
						Kind: ir.PartToolResult,
						Result: &ir.EssenceToolResult{
							CallID:  "c_search",
							Tool:    ir.ToolRef{Kind: ir.ToolClientSearch},
							Content: []ir.EssencePart{{Kind: ir.PartText, Text: "found"}},
						},
					},
				},
			},
		},
	}

	dst := ir.EssenceDoc{
		Tools: []ir.EssenceTool{
			{Kind: ir.ToolCustom, Name: "bad_custom_name_12345678", FormatHash: "fmt_1"},
			{Kind: ir.ToolFunction, Namespace: "ns", Name: "bad_fn_name_87654321", SchemaHash: "sch_1"},
		},
		Messages: []ir.EssenceMessage{
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolCall,
						Call: &ir.EssenceToolCall{
							CallID:    "c_shared",
							Tool:      ir.ToolRef{Kind: ir.ToolCustom, Name: "bad_custom_name_12345678"},
							Arguments: "*** Begin Patch\n+ok\n*** End Patch",
						},
					},
					{
						Kind: ir.PartToolCall,
						Call: &ir.EssenceToolCall{
							CallID:    "c_search",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "tool_search"},
							Arguments: "{}",
						},
					},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolResult,
						Result: &ir.EssenceToolResult{
							CallID:  "c_shared",
							Tool:    ir.ToolRef{Kind: ir.ToolCustom, Name: "bad_custom_name_12345678"},
							Content: []ir.EssencePart{{Kind: ir.PartText, Text: "applied"}},
						},
					},
					{
						Kind: ir.PartToolResult,
						Result: &ir.EssenceToolResult{
							CallID:  "c_search",
							Tool:    ir.ToolRef{Kind: ir.ToolFunction, Name: "tool_search"},
							Content: []ir.EssencePart{{Kind: ir.PartText, Text: "found"}},
						},
					},
				},
			},
		},
	}

	losses := []ir.Loss{
		{Kind: ir.LossToolLowered, Path: "tools[0]"},
		{Kind: ir.LossToolLowered, Path: "tools[1]"},
		{Kind: ir.LossToolLowered, Path: "messages[0].parts[0]"},
		{Kind: ir.LossToolPairingRepaired, Path: "messages[0].parts[1]"},
		{Kind: ir.LossToolLowered, Path: "messages[0].parts[2]"},
		{Kind: ir.LossToolLowered, Path: "messages[1].parts[0]"},
		{Kind: ir.LossToolLowered, Path: "messages[1].parts[1]"},
	}

	diffs, err := VerifyEssence(src, dst, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))
}

// TestVerifyEssenceRefusalInlinedCarrierTokensAndSearchPromotion checks
// LossRefusalInlined, carrier-to-bare token metadata alignment, and promoted
// tool_search_output discoveries. Matching issuer Data must reconcile bare wire
// decodes while mismatched schemes still report differences.
func TestVerifyEssenceRefusalInlinedCarrierTokensAndSearchPromotion(t *testing.T) {
	deferTrue := true
	src := ir.EssenceDoc{
		Tools: []ir.EssenceTool{
			{Kind: ir.ToolClientSearch},
			{Kind: ir.ToolFunction, Name: "read_file", SchemaHash: "h1", DeferLoading: &deferTrue},
		},
		Messages: []ir.EssenceMessage{
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.EssenceReasoning{
							Redacted: true,
							Token:    &ir.EssenceToken{Scheme: ir.SchemeXAI, ProviderID: 202, Data: "enc_xai_1"},
						},
					},
					{Kind: ir.PartRefusal, Text: "Declined."},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolResult,
						Result: &ir.EssenceToolResult{
							CallID: "c1",
							Tool:   ir.ToolRef{Kind: ir.ToolClientSearch},
							Content: []ir.EssencePart{
								{Kind: ir.PartText, Text: `[{"type":"namespace","name":"pager","tools":[{"type":"function","name":"list_incidents"}]}]`},
							},
						},
					},
				},
			},
		},
	}

	dst := ir.EssenceDoc{
		Tools: []ir.EssenceTool{
			{Kind: ir.ToolFunction, Name: "tool_search", SchemaHash: "h_search"},
			{Kind: ir.ToolFunction, Name: "read_file", SchemaHash: "h1"},
			{Kind: ir.ToolFunction, Name: "pager__list_incidents", SchemaHash: "h_inc"},
		},
		Messages: []ir.EssenceMessage{
			{
				Role: ir.RoleAssistant,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartReasoning,
						Reasoning: &ir.EssenceReasoning{
							Token: &ir.EssenceToken{Data: "enc_xai_1"},
						},
					},
					{Kind: ir.PartText, Text: "Declined."},
				},
			},
			{
				Role: ir.RoleTool,
				Parts: []ir.EssencePart{
					{
						Kind: ir.PartToolResult,
						Result: &ir.EssenceToolResult{
							CallID: "c1",
							Tool:   ir.ToolRef{Kind: ir.ToolFunction, Name: "tool_search"},
							Content: []ir.EssencePart{
								{Kind: ir.PartText, Text: `[{"type":"namespace","name":"pager","tools":[{"type":"function","name":"list_incidents"}]}]`},
							},
						},
					},
				},
			},
		},
	}

	losses := []ir.Loss{
		{Kind: ir.LossToolLowered, Path: "tools[0]"},
		{Kind: ir.LossToolLowered, Path: "tools[1]"},
		{Kind: ir.LossRefusalInlined, Path: "messages[0].parts[1]"},
		{Kind: ir.LossToolLowered, Path: "messages[1].parts[0]"},
	}

	diffs, err := VerifyEssence(src, dst, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))

	// Distinct non-empty schemes must still fail reconciliation.
	dst.Messages[0].Parts[0].Reasoning.Token.Scheme = ir.SchemeOpenAI
	diffsMismatch, err := VerifyEssence(src, dst, losses)
	require.NoError(t, err)
	require.NotEmpty(t, diffsMismatch)
}

// TestVerifyEssence_StrictToggleAndHostedConfigHash reproduces cross-protocol
// tool Strict defaults and hosted web-search sub-configuration translations.
// Unreported Strict=false omissions or silent HostedConfigHash changes must
// fail VerifyEssence, whereas explicit schema_approximated(strict) and
// tool_lowered losses reconcile cleanly.
func TestVerifyEssence_StrictToggleAndHostedConfigHash(t *testing.T) {
	strictFalse := false
	srcStrict := ir.EssenceDoc{
		Tools: []ir.EssenceTool{{Kind: ir.ToolFunction, Name: "fn", SchemaHash: "h1", Strict: nil}},
	}
	dstStrict := ir.EssenceDoc{
		Tools: []ir.EssenceTool{{Kind: ir.ToolFunction, Name: "fn", SchemaHash: "h1", Strict: &strictFalse}},
	}

	// Unreported nil -> false transition must surface as an Essence diff.
	unreportedDiffs, err := VerifyEssence(srcStrict, dstStrict, nil)
	require.NoError(t, err)
	require.NotEmpty(t, unreportedDiffs)

	// Reported schema_approximated(strict) reconciles nil -> false, and fails if dst remained nil.
	strictLoss := []ir.Loss{{Kind: ir.LossSchemaApproximated, Path: "tools[0]", Detail: "strict"}}
	okDiffs, err := VerifyEssence(srcStrict, dstStrict, strictLoss)
	require.NoError(t, err)
	require.Empty(t, okDiffs)

	_, err = VerifyEssence(srcStrict, srcStrict, strictLoss)
	require.Error(t, err, "missing strict=false on target must fail strict normalization check")

	// Hosted config hash changes require tool_lowered when translating across protocols.
	srcHosted := ir.EssenceDoc{
		Tools: []ir.EssenceTool{{
			Kind:             ir.ToolHosted,
			Name:             "web_search",
			HostedType:       "anthropic_messages:web_search_20250305",
			HostedConfigHash: "hash_with_max_uses_3",
		}},
	}
	dstHosted := ir.EssenceDoc{
		Tools: []ir.EssenceTool{{
			Kind:             ir.ToolHosted,
			Name:             "web_search",
			HostedType:       "openai_responses:web_search",
			HostedConfigHash: "hash_bare_web_search",
		}},
	}
	hostedUnreported, err := VerifyEssence(srcHosted, dstHosted, nil)
	require.NoError(t, err)
	require.NotEmpty(t, hostedUnreported)

	hostedLowered, err := VerifyEssence(srcHosted, dstHosted, []ir.Loss{{
		Kind:   ir.LossToolLowered,
		Path:   "tools[0]",
		Detail: "hosted_web_search",
	}})
	require.NoError(t, err)
	require.Empty(t, hostedLowered)
}

// TestVerifyEssence_NamespaceCacheAndExtraLosses reproduces cache_mark_dropped
// and extra_dropped losses recorded against namespaces[i] coordinates while
// root HasExtra remains true. VerifyEssence must accept namespace paths as
// no-ops without clearing root or tool HasExtra flags.
func TestVerifyEssence_NamespaceCacheAndExtraLosses(t *testing.T) {
	doc := ir.EssenceDoc{
		HasExtra: true,
		Tools: []ir.EssenceTool{{
			Kind:       ir.ToolFunction,
			Namespace:  "ns1",
			Name:       "fn",
			SchemaHash: "h1",
			HasExtra:   true,
		}},
	}
	losses := []ir.Loss{
		{Kind: ir.LossCacheMarkDropped, Path: "namespaces[0]"},
		{Kind: ir.LossExtraDropped, Path: "namespaces[0]"},
		{Kind: ir.LossExtraDropped, Path: "namespaces[0].cache"},
	}
	diffs, err := VerifyEssence(doc, doc, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))
}

// TestVerifyEssenceInlineImageMarkdownAndSignedTextCarrier reproduces response
// conversions that render model-generated base64 images as Markdown data URLs
// and preserve non-thought text replay signatures inside trailing reasoning
// blocks. VerifyEssence must accept matching MIME and SHA-256 base64 digests
// (including Chat coalesced text) while rejecting tampered payloads or dropped
// text signatures.
func TestVerifyEssenceInlineImageMarkdownAndSignedTextCarrier(t *testing.T) {
	const pngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
	rawResp := &ir.Response{
		Parts: []ir.Part{
			{
				Kind:  ir.PartText,
				Text:  "Chart preview:\n",
				Token: &ir.ReplayToken{Scheme: ir.SchemeAntigravityClaude, Data: "sig_non_thought_1"},
			},
			{
				Kind: ir.PartMedia,
				Media: &ir.Media{
					Type:   ir.MediaImage,
					MIME:   "image/png",
					Source: ir.MediaSourceBase64,
					Data:   pngB64,
				},
			},
		},
	}
	srcEss := ir.ResponseEssence(rawResp)

	// 1. Messages/Responses shape: separate text, trailing signed reasoning, and Markdown image text.
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAntigravityClaude,
		Data:   "sig_non_thought_1",
	})
	require.NoError(t, err)
	dstMultiPart := ir.ResponseEssence(&ir.Response{
		Parts: []ir.Part{
			{Kind: ir.PartText, ID: "msg_0", Text: "Chart preview:\n"},
			{Kind: ir.PartReasoning, ID: "rs_1", Reasoning: &ir.Reasoning{Summary: true, Token: &ir.ReplayToken{Data: carrier}}},
			{Kind: ir.PartText, ID: "msg_2", Text: "![image](data:image/png;base64," + pngB64 + ")"},
		},
	})
	multiLosses := []ir.Loss{
		{Kind: ir.LossIDRewritten, Path: "parts[0].id"},
		{Kind: ir.LossMediaUnsupported, Path: "parts[1]", Detail: "markdown_data_url"},
		{Kind: ir.LossIDRewritten, Path: "parts[1].id"},
	}
	diffs, err := VerifyEssence(srcEss, dstMultiPart, multiLosses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))

	// Silent signature drop on dst must fail reconciliation.
	droppedSigDst := ir.ResponseEssence(&ir.Response{
		Parts: []ir.Part{
			{Kind: ir.PartText, ID: "msg_0", Text: "Chart preview:\n"},
			{Kind: ir.PartText, ID: "msg_2", Text: "![image](data:image/png;base64," + pngB64 + ")"},
		},
	})
	diffsMissingSig, err := VerifyEssence(srcEss, droppedSigDst, multiLosses)
	require.NoError(t, err)
	require.NotEmpty(t, diffsMissingSig)

	// Tampered base64 payload or MIME inside Markdown data URL must fail precondition check.
	tamperedImgDst := ir.ResponseEssence(&ir.Response{
		Parts: []ir.Part{
			{Kind: ir.PartText, ID: "msg_0", Text: "Chart preview:\n"},
			{Kind: ir.PartReasoning, ID: "rs_1", Reasoning: &ir.Reasoning{Summary: true, Token: &ir.ReplayToken{Data: carrier}}},
			{Kind: ir.PartText, ID: "msg_2", Text: "![image](data:image/png;base64,dGFtcGVyZWQ=)"},
		},
	})
	_, err = VerifyEssence(srcEss, tamperedImgDst, multiLosses)
	require.Error(t, err)

	// 2. Chat shape: replay_token_dropped on parts[0] + coalesced choices[0].message.content string.
	dstChat := ir.ResponseEssence(&ir.Response{
		Parts: []ir.Part{
			{Kind: ir.PartText, Text: "Chart preview:\n![image](data:image/png;base64," + pngB64 + ")"},
		},
	})
	chatLosses := []ir.Loss{
		{Kind: ir.LossReplayTokenDropped, Path: "parts[0]"},
		{Kind: ir.LossMediaUnsupported, Path: "parts[1]", Detail: "markdown_data_url"},
	}
	chatDiffs, err := VerifyEssence(srcEss, dstChat, chatLosses)
	require.NoError(t, err)
	require.Empty(t, chatDiffs, FormatJSONDiff(chatDiffs))
}

// TestVerifyEssenceMissingToolCallIDFoldedIntoUserTurn exercises
// LossToolPairingRepaired with Detail="missing_tool_call_id" across standalone,
// empty, and user-bracketed ID-less tool_result turns. It verifies that
// surviving text folds into adjacent RoleUser turns without collapsing separate
// trailing user turns and rejects non-empty CallIDs.
func TestVerifyEssenceMissingToolCallIDFoldedIntoUserTurn(t *testing.T) {
	srcStandalone := ir.Essence(&ir.Request{
		Model: "auto",
		Messages: []ir.Message{{
			Role: ir.RoleTool,
			Parts: []ir.Part{{
				Kind: ir.PartToolResult,
				Result: &ir.ToolResult{
					CallID: "",
					Content: []ir.Part{
						{Kind: ir.PartText, Text: "line1\n"},
						{Kind: ir.PartText, Text: "line2"},
						{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, MIME: "image/png", Source: ir.MediaSourceBase64, Data: "iVBORw=="}},
					},
				},
			}},
		}},
	})
	dstStandalone := ir.Essence(&ir.Request{
		Model: "auto",
		Messages: []ir.Message{{
			Role:  ir.RoleUser,
			Parts: []ir.Part{{Kind: ir.PartText, Text: "line1\nline2"}},
		}},
	})
	diffs, err := VerifyEssence(srcStandalone, dstStandalone, []ir.Loss{
		{Kind: ir.LossToolPairingRepaired, Path: "messages[0].parts[0]", Detail: "missing_tool_call_id"},
		{Kind: ir.LossMediaUnsupported, Path: "messages[0].parts[0].result.content[2]"},
	})
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))

	srcEmpty := ir.Essence(&ir.Request{
		Model: "auto",
		Messages: []ir.Message{{
			Role: ir.RoleTool,
			Parts: []ir.Part{{
				Kind:   ir.PartToolResult,
				Result: &ir.ToolResult{CallID: "", Content: []ir.Part{}},
			}},
		}},
	})
	dstEmpty := ir.Essence(&ir.Request{
		Model:    "auto",
		Messages: []ir.Message{{Role: ir.RoleUser}},
	})
	diffsEmpty, err := VerifyEssence(srcEmpty, dstEmpty, []ir.Loss{
		{Kind: ir.LossToolPairingRepaired, Path: "messages[0].parts[0]", Detail: "missing_tool_call_id"},
	})
	require.NoError(t, err)
	require.Empty(t, diffsEmpty, FormatJSONDiff(diffsEmpty))

	srcBracketed := ir.Essence(&ir.Request{
		Model: "auto",
		Messages: []ir.Message{
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "before"}}},
			{
				Role: ir.RoleTool,
				Parts: []ir.Part{{
					Kind: ir.PartToolResult,
					Result: &ir.ToolResult{
						CallID:  "",
						Content: []ir.Part{{Kind: ir.PartText, Text: "file.txt"}},
					},
				}},
			},
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "after"}}},
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "separate_turn"}}},
		},
	})
	dstBracketed := ir.Essence(&ir.Request{
		Model: "auto",
		Messages: []ir.Message{
			{
				Role: ir.RoleUser,
				Parts: []ir.Part{
					{Kind: ir.PartText, Text: "before"},
					{Kind: ir.PartText, Text: "file.txt"},
					{Kind: ir.PartText, Text: "after"},
				},
			},
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "separate_turn"}}},
		},
	})
	diffsBracketed, err := VerifyEssence(srcBracketed, dstBracketed, []ir.Loss{
		{Kind: ir.LossToolPairingRepaired, Path: "messages[1].parts[0]", Detail: "missing_tool_call_id"},
	})
	require.NoError(t, err)
	require.Empty(t, diffsBracketed, FormatJSONDiff(diffsBracketed))

	srcNonEmptyID := ir.Essence(&ir.Request{
		Model: "auto",
		Messages: []ir.Message{{
			Role: ir.RoleTool,
			Parts: []ir.Part{{
				Kind:   ir.PartToolResult,
				Result: &ir.ToolResult{CallID: "call_present", Content: []ir.Part{{Kind: ir.PartText, Text: "x"}}},
			}},
		}},
	})
	_, err = VerifyEssence(srcNonEmptyID, dstStandalone, []ir.Loss{
		{Kind: ir.LossToolPairingRepaired, Path: "messages[0].parts[0]", Detail: "missing_tool_call_id"},
	})
	require.ErrorContains(t, err, "missing_tool_call_id requires tool_result with empty call_id")
}

// TestVerifyEssencePreservesUnmergedLeadingRunsWhileCoalescingSplitAssistant
// reproduces a Codex CLI Turn 2 transcript carrying two consecutive developer
// turns and two consecutive user turns ahead of three split assistant items.
// VerifyEssence must preserve the four unmerged developer and user turns when
// target Chat keeps them separate and coalesce only the folded assistant run.
func TestVerifyEssencePreservesUnmergedLeadingRunsWhileCoalescingSplitAssistant(t *testing.T) {
	src := ir.Essence(&ir.Request{
		Model: "gpt-5.5",
		Messages: []ir.Message{
			{Role: ir.RoleDeveloper, ID: "msg_d0", Parts: []ir.Part{{Kind: ir.PartText, Text: "system prompt"}}},
			{Role: ir.RoleDeveloper, ID: "msg_d1", Parts: []ir.Part{{Kind: ir.PartText, Text: "skills"}, {Kind: ir.PartText, Text: "sandbox"}}},
			{Role: ir.RoleUser, ID: "msg_u0", Parts: []ir.Part{{Kind: ir.PartText, Text: "env"}}},
			{Role: ir.RoleUser, ID: "msg_u1", Parts: []ir.Part{{Kind: ir.PartText, Text: "run shell"}}},
			{Role: ir.RoleAssistant, Parts: []ir.Part{{Kind: ir.PartReasoning, ID: "rs_1", Reasoning: &ir.Reasoning{Segments: []string{"plan"}}}}},
			{Role: ir.RoleAssistant, ID: "msg_c1", Extra: ir.Extra{Origin: "openai_responses"}, Parts: []ir.Part{{Kind: ir.PartText, Text: "running"}}},
			{Role: ir.RoleAssistant, Parts: []ir.Part{{Kind: ir.PartToolCall, ID: "fc_1", Call: &ir.ToolCall{CallID: "c1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "exec_command"}, Arguments: "{}"}}}},
			{Role: ir.RoleTool, Parts: []ir.Part{{Kind: ir.PartToolResult, ID: "fco_1", Result: &ir.ToolResult{CallID: "c1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "exec_command"}, Content: []ir.Part{{Kind: ir.PartText, Text: "ok"}}}}}},
		},
	})
	dst := ir.Essence(&ir.Request{
		Model: "gpt-5.5",
		Messages: []ir.Message{
			{Role: ir.RoleDeveloper, Parts: []ir.Part{{Kind: ir.PartText, Text: "system prompt"}}},
			{Role: ir.RoleDeveloper, Parts: []ir.Part{{Kind: ir.PartText, Text: "skills"}, {Kind: ir.PartText, Text: "sandbox"}}},
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "env"}}},
			{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "run shell"}}},
			{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{
					{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Segments: []string{"plan"}}},
					{Kind: ir.PartText, Text: "running"},
					{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "c1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "exec_command"}, Arguments: "{}"}},
				},
			},
			{Role: ir.RoleTool, Parts: []ir.Part{{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "c1", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "exec_command"}, Content: []ir.Part{{Kind: ir.PartText, Text: "ok"}}}}}},
		},
	})
	losses := []ir.Loss{
		{Kind: ir.LossIDRewritten, Path: "messages[0].id"},
		{Kind: ir.LossIDRewritten, Path: "messages[1].id"},
		{Kind: ir.LossIDRewritten, Path: "messages[2].id"},
		{Kind: ir.LossIDRewritten, Path: "messages[3].id"},
		{Kind: ir.LossIDRewritten, Path: "messages[4].parts[0].id"},
		{Kind: ir.LossIDRewritten, Path: "messages[5].id"},
		{Kind: ir.LossExtraDropped, Path: "messages[5]"},
		{Kind: ir.LossIDRewritten, Path: "messages[6].parts[0].id"},
		{Kind: ir.LossIDRewritten, Path: "messages[7].parts[0].id"},
	}
	diffs, err := VerifyEssence(src, dst, losses)
	require.NoError(t, err)
	require.Empty(t, diffs, FormatJSONDiff(diffs))
}
