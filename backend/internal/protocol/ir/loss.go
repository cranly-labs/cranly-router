package ir

import "fmt"

// loss.go defines structured information-loss records emitted when an encoder
// or IR transformation drops, approximates, relocates, or reorders content that
// the target protocol or dialect cannot represent directly. Law tests apply
// each reported Loss to semantic Essence summaries to verify that every
// cross-protocol change is explicitly accounted for.

const (
	// LossKind constants enumerate every permitted category of encoder or
	// transformation loss across request and response conversion pipelines.
	LossExtraDropped             LossKind = "extra_dropped"              // unmodeled Extra members omitted for foreign target
	LossOpaqueItemDropped        LossKind = "opaque_item_dropped"        // single-protocol OpaqueItem omitted
	LossReplayTokenDropped       LossKind = "replay_token_dropped"       // incompatible or unsupported ReplayToken omitted
	LossReasoningTextDropped     LossKind = "reasoning_text_dropped"     // plaintext reasoning omitted under target HistoryMode
	LossReasoningTextInlined     LossKind = "reasoning_text_inlined"     // unsigned reasoning downgraded into visible text
	LossRefusalInlined           LossKind = "refusal_inlined"            // structured refusal downgraded into visible text
	LossCacheMarkDropped         LossKind = "cache_mark_dropped"         // prompt-cache breakpoint omitted
	LossMediaUnsupported         LossKind = "media_unsupported"          // unsupported attachment modality or source dropped
	LossMediaRelocated           LossKind = "media_relocated"            // tool-result attachment moved to following user turn
	LossToolLowered              LossKind = "tool_lowered"               // custom, namespace, or search tool lowered to function
	LossToolHistoryInlined       LossKind = "tool_history_inlined"       // structured tool call or tool result downgraded into visible text
	LossToolChoiceApproximated   LossKind = "tool_choice_approximated"   // tool choice widened or adapted for target
	LossHostedToolDropped        LossKind = "hosted_tool_dropped"        // unsupported hosted tool declaration omitted
	LossSchemaApproximated       LossKind = "schema_approximated"        // JSON Schema cleaned to fit target schema dialect
	LossSystemRelocated          LossKind = "system_relocated"           // system or developer instructions merged or moved
	LossPartsReordered           LossKind = "parts_reordered"            // message parts reordered to satisfy target grammar
	LossToolPairingRepaired      LossKind = "tool_pairing_repaired"      // unmatched tool call or tool result repaired
	LossSamplingDropped          LossKind = "sampling_dropped"           // forbidden sampling parameter omitted
	LossStopDropped              LossKind = "stop_dropped"               // forbidden stop sequences omitted
	LossAnnotationDropped        LossKind = "annotation_dropped"         // text citation annotations omitted
	LossOutputFormatApproximated LossKind = "output_format_approximated" // structured output or verbosity approximated
	LossClientMarkerDropped      LossKind = "client_marker_dropped"      // client-specific system marker stripped
	LossCandidateCountIgnored    LossKind = "candidate_count_ignored"    // candidate count > 1 ignored by target
	LossIDRewritten              LossKind = "id_rewritten"               // wire item or call identifier deterministically rewritten
)

// LossKind classifies one category of information loss or approximation during encoding.
type LossKind string

// Loss records one piece of information dropped, approximated, or rearranged at
// an IR path such as "messages[3].parts[1]" or "tools[0].input_schema".
type Loss struct {
	Kind   LossKind `json:"kind"`            // category of loss or approximation
	Path   string   `json:"path"`            // IR path locating the affected element
	Detail string   `json:"detail,omitzero"` // optional field name or reason detail
}

// IsValid reports whether k is one of the twenty-four canonical LossKind values.
func (k LossKind) IsValid() bool {
	switch k {
	case LossExtraDropped, LossOpaqueItemDropped, LossReplayTokenDropped,
		LossReasoningTextDropped, LossReasoningTextInlined, LossRefusalInlined,
		LossCacheMarkDropped, LossMediaUnsupported, LossMediaRelocated,
		LossToolLowered, LossToolHistoryInlined, LossToolChoiceApproximated,
		LossHostedToolDropped, LossSchemaApproximated, LossSystemRelocated,
		LossPartsReordered, LossToolPairingRepaired, LossSamplingDropped,
		LossStopDropped, LossAnnotationDropped, LossOutputFormatApproximated,
		LossClientMarkerDropped, LossCandidateCountIgnored, LossIDRewritten:
		return true
	default:
		return false
	}
}

// String formats l with its Kind, target IR path, and optional Detail label for
// test diagnostics and debug logging. Empty Path strings render as "<root>" so
// document-level losses remain unambiguous.
func (l Loss) String() string {
	loc := l.Path
	if loc == "" {
		loc = "<root>"
	}
	if l.Detail != "" {
		return fmt.Sprintf("%s at %s (%s)", l.Kind, loc, l.Detail)
	}
	return fmt.Sprintf("%s at %s", l.Kind, loc)
}
