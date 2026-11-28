package testkit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// essence.go implements per-LossKind semantic summary projections and pair
// verification for conversion laws Q4 and S4. Encoders may drop, approximate,
// relocate, or reorder IR content only when they emit a matching ir.Loss record;
// applying those reported losses to the source EssenceDoc must reconcile every
// difference against the decoded target EssenceDoc.
//
// Loss evaluation runs in two phases so multi-pass conversions and read-only
// encoder loops never suffer slice-index drift. Phase one resolves every
// Loss.Path against stable source slots, recording tombstones and transformation
// flags without deleting or inserting slice elements. Phase two materializes
// surviving source elements, executes structural relocations, pairs surviving
// tools, messages, and parts by live ordinal against the target EssenceDoc to
// check preconditions, and finally canonicalizes any reordered part slices.

const (
	// approximatedHashSentinel, loweredToolSentinel, and loweredArgsSentinel
	// replace wire-specific hashes, lowered tool names, and wrapped custom tool
	// inputs after structural preconditions pass on both EssenceDoc values.
	approximatedHashSentinel = "<approximated_schema>"
	loweredToolSentinel      = "<lowered_tool>"
	loweredArgsSentinel      = "<lowered_arguments>"

	// relocatedToolMediaMarker matches the synthetic placeholder written onto Chat
	// role="tool" messages when tool-result media moves to a following user message.
	relocatedToolMediaMarker = "[Tool output media moved to the following user message]"

	// geminiPlaceholderThoughtSignature matches the compatibility thoughtSignature
	// injected onto unsigned cross-protocol Gemini functionCall parts when required.
	geminiPlaceholderThoughtSignature = "skip_thought_signature_validator"
)

// lossDispatch maps each supported ir.LossKind constant to the phase-one
// handler that records its effect on a trackedDoc instance.
var lossDispatch = buildLossDispatch()

// lossRecordHandler validates and records one ir.Loss against a stable trackedDoc.
type lossRecordHandler func(doc *trackedDoc, loss ir.Loss) error

// irPathStep represents one segment in an IR path such as "messages[3].parts[1]".
type irPathStep struct {
	field    string // member name such as "messages" or "parts"
	hasIndex bool   // whether an array index suffix [n] was present
	index    int    // zero-based array index when hasIndex is true
}

// trackedPart wraps an ir.EssencePart with tombstone and deferred alignment flags
// so sibling indices stay fixed throughout phase-one loss recording.
type trackedPart struct {
	part             ir.EssencePart   // current normalized part state
	nestedContent    []trackedPart    // tracked Result.Content slots for tool_result parts
	inlinedMedia     *ir.EssenceMedia // expected base64 image descriptor after markdown_data_url
	dropped          bool             // true when a loss omitted this part
	textTokenDropped bool             // true when replay_token_dropped cleared a text-attached token
	inlined          bool             // true after reasoning_text_inlined converted this part
	historyToolName  string           // executable name captured before inlining a call
	// historyKind retains the original call or result kind through expansion.
	// historyCallID links a rendered header to later results when a separate
	// loss permits the upstream to rewrite the source call's identifier.
	historyKind     ir.PartKind // original inlined variant
	historyCallID   string      // source call identifier
	toolLowered     bool        // true when tool_lowered targeted this call or result
	wasCustom       bool        // true when a lowered call originated as ToolCustom
	partIDRewritten bool        // true when id_rewritten targeted Part.ID
	callIDRewritten bool        // true when id_rewritten targeted CallID on call or result
	relocateMedia   bool        // true when media_relocated targeted this tool_result
	missingIDToUser bool        // true when missing_tool_call_id folded an ID-less tool_result into user text
}

// trackedMsg wraps an ir.EssenceMessage with fixed part slots and turn-level flags.
type trackedMsg struct {
	role           ir.Role       // conversation turn role
	id             string        // wire message identifier
	parts          []trackedPart // fixed-index part slots
	hasExtra       bool          // whether Message.Extra carried members
	idRewritten    bool          // true when id_rewritten targeted Message.ID
	partsReordered bool          // true when parts_reordered targeted this message
}

// trackedTool wraps an ir.EssenceTool with tombstone, lowering, and schema flags.
type trackedTool struct {
	tool               ir.EssenceTool // normalized tool declaration
	dropped            bool           // true when hosted_tool_dropped, opaque_item_dropped, or duplicate_web_search hit
	lowered            bool           // true when tool_lowered targeted this declaration
	promotedToHosted   bool           // true when tool_lowered(promoted_google_search) promoted a search function to hosted
	schemaApproximated bool           // true when schema_approximated targeted this declaration
	strictNormalized   bool           // true when schema_approximated(strict) targeted this declaration
	strictDropped      bool           // true when schema_approximated(strict_dropped) omitted Strict
}

// trackedDoc holds the fixed-index shadow tree mutated during phase-one loss recording.
type trackedDoc struct {
	system                   []trackedPart              // fixed-index System part slots
	messages                 []trackedMsg               // fixed-index Message slots
	tools                    []trackedTool              // fixed-index Tool declaration slots
	toolChoice               *ir.EssenceToolChoice      // normalized tool choice
	parallelToolCalls        *bool                      // parallel tool-call toggle
	sampling                 ir.Sampling                // token sampling parameters
	maxOutputTokens          *int64                     // output token cap
	stop                     []string                   // custom stop sequences
	reasoning                *ir.EssenceReasoningConfig // reasoning request controls
	output                   *ir.EssenceOutputFormat    // structured output settings
	parts                    []trackedPart              // fixed-index Response.Parts slots
	finish                   *ir.Finish                 // response finish metadata
	usage                    *ir.Usage                  // response usage buckets
	serviceTier              string                     // requested or reported service tier
	hasExtra                 bool                       // whether root Extra carried members
	systemRelocated          bool                       // whether system_relocated was reported
	rootPartsReordered       bool                       // whether parts_reordered targeted response parts
	outputSchemaApproximated bool                       // whether schema_approximated targeted output
	toolChoiceApproximated   bool                       // whether tool_choice_approximated was reported
	outputApproximatedAll    bool                       // whether whole OutputFormat was approximated
	partialRootExtraDropped  bool                       // whether a root-embedded sub-object dropped extras
	repairedCallIDs          []string                   // orphan call IDs removed by tool_pairing_repaired
}

// materializedMsg holds surviving parts of one non-empty turn during phase-two alignment.
type materializedMsg struct {
	role           ir.Role       // turn speaker role
	id             string        // wire message identifier
	parts          []trackedPart // surviving non-dropped parts in turn order
	hasExtra       bool          // surviving message extra flag
	idRewritten    bool          // whether message ID was rewritten
	partsReordered bool          // whether parts inside this turn may be reordered
}

// markdownDataURLSpan holds the raw Markdown substring, MIME type, and base64
// payload extracted from one inline image data URL.
type markdownDataURLSpan struct {
	raw  string // complete "![image](data:<mime>;base64,<b64>)" substring
	mime string // extracted MIME string before ";base64,"
	b64  string // extracted base64 payload before ")"
}

// pendingMsg holds one compacted source turn and any relocated tool-output media
// produced while materializing trackedMsg entries in phase two.
type pendingMsg struct {
	msg                     materializedMsg // compacted turn before media relocation
	movedMedia              []trackedPart   // media parts extracted from tool_result content
	relocatedCallIDs        []string        // CallIDs whose tool_result media moved out
	originallyEmpty         bool            // whether the source turn started with zero parts
	fromMissingToolResultID bool            // whether this RoleUser turn came from a missing_tool_call_id tool_result
}

// SupportedLossKinds returns all ir.LossKind values handled by ApplyLoss and
// VerifyEssence in deterministic lexical order.
func SupportedLossKinds() []ir.LossKind {
	kinds := make([]ir.LossKind, 0, len(lossDispatch))
	for k := range lossDispatch {
		kinds = append(kinds, k)
	}
	slices.Sort(kinds)
	return kinds
}

// ApplyLoss returns an independent copy of doc after applying a single Loss
// entry without mutating caller-owned slices or nested pointers. Unknown
// LossKind values or out-of-range IR paths return descriptive errors.
func ApplyLoss(doc ir.EssenceDoc, loss ir.Loss) (ir.EssenceDoc, error) {
	return ApplyLosses(doc, []ir.Loss{loss})
}

// ApplyLosses returns an independent copy of doc after recording and
// materializing every Loss in losses without index drift. Unknown LossKind
// values, duplicate drops on the same element, or invalid IR paths return an error.
func ApplyLosses(doc ir.EssenceDoc, losses []ir.Loss) (ir.EssenceDoc, error) {
	td, err := newTrackedDoc(doc)
	if err != nil {
		return ir.EssenceDoc{}, err
	}
	for i, loss := range losses {
		handler, ok := lossDispatch[loss.Kind]
		if !ok {
			return ir.EssenceDoc{}, fmt.Errorf("loss[%d]: unsupported LossKind %q", i, loss.Kind)
		}
		if err := handler(td, loss); err != nil {
			return ir.EssenceDoc{}, fmt.Errorf("loss[%d] (%s at %q): %w", i, loss.Kind, loss.Path, err)
		}
	}
	normSrc, err := materializeAndAlign(td, nil, false)
	if err != nil {
		return ir.EssenceDoc{}, err
	}
	return normSrc, nil
}

// VerifyEssence reconciles cloned copies of src and dst under encoder-reported
// losses and returns any remaining unexplained JSON-path differences.
//
// Phase one records every Loss against fixed source indices so multi-loss loops
// and cross-stage pipeline passes never shift neighboring indices. Phase two
// compacts dropped items, executes system and media relocations, and aligns
// surviving tools, messages, and parts by live ordinal against dst to check
// preconditions such as non-empty approximated schemas and preserved part multisets.
//
// Precondition failures or unknown LossKind values return a non-nil error,
// whereas unreported semantic changes appear in the returned JSONDiffItem slice.
func VerifyEssence(src, dst ir.EssenceDoc, losses []ir.Loss) ([]JSONDiffItem, error) {
	td, err := newTrackedDoc(src)
	if err != nil {
		return nil, err
	}
	normDst, err := cloneEssenceDoc(dst)
	if err != nil {
		return nil, err
	}
	for i, loss := range losses {
		handler, ok := lossDispatch[loss.Kind]
		if !ok {
			return nil, fmt.Errorf("loss[%d]: unsupported LossKind %q", i, loss.Kind)
		}
		if err := handler(td, loss); err != nil {
			return nil, fmt.Errorf("loss[%d] (%s at %q): %w", i, loss.Kind, loss.Path, err)
		}
	}
	normSrc, err := materializeAndAlign(td, &normDst, true)
	if err != nil {
		return nil, err
	}
	cleanupEmptyCollections(&normDst)

	srcBytes, err := json.Marshal(normSrc, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	dstBytes, err := json.Marshal(normDst, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	return JSONDiff(srcBytes, dstBytes)
}

// buildLossDispatch constructs the complete dispatch table of 24 LossKind handlers.
func buildLossDispatch() map[ir.LossKind]lossRecordHandler {
	return map[ir.LossKind]lossRecordHandler{
		ir.LossExtraDropped:             recordExtraDropped,
		ir.LossOpaqueItemDropped:        recordOpaqueItemDropped,
		ir.LossReplayTokenDropped:       recordReplayTokenDropped,
		ir.LossReasoningTextDropped:     recordReasoningTextDropped,
		ir.LossReasoningTextInlined:     recordReasoningTextInlined,
		ir.LossRefusalInlined:           recordRefusalInlined,
		ir.LossCacheMarkDropped:         recordCacheMarkDropped,
		ir.LossMediaUnsupported:         recordMediaUnsupported,
		ir.LossMediaRelocated:           recordMediaRelocated,
		ir.LossToolLowered:              recordToolLowered,
		ir.LossToolHistoryInlined:       recordToolHistoryInlined,
		ir.LossToolChoiceApproximated:   recordToolChoiceApproximated,
		ir.LossHostedToolDropped:        recordHostedToolDropped,
		ir.LossSchemaApproximated:       recordSchemaApproximated,
		ir.LossSystemRelocated:          recordSystemRelocated,
		ir.LossPartsReordered:           recordPartsReordered,
		ir.LossToolPairingRepaired:      recordToolPairingRepaired,
		ir.LossSamplingDropped:          recordSamplingDropped,
		ir.LossStopDropped:              recordStopDropped,
		ir.LossAnnotationDropped:        recordAnnotationDropped,
		ir.LossOutputFormatApproximated: recordOutputFormatApproximated,
		ir.LossClientMarkerDropped:      recordClientMarkerDropped,
		ir.LossCandidateCountIgnored:    recordCandidateCountIgnored,
		ir.LossIDRewritten:              recordIDRewritten,
	}
}

// cloneEssenceDoc produces an independent deep copy of doc through JSON v2
// serialization. Loss handlers mutate only the returned copy so caller-owned
// slices and pointers remain unchanged.
func cloneEssenceDoc(doc ir.EssenceDoc) (ir.EssenceDoc, error) {
	raw, err := json.Marshal(doc)
	if err != nil {
		return ir.EssenceDoc{}, err
	}
	var copyDoc ir.EssenceDoc
	if err := json.Unmarshal(raw, &copyDoc); err != nil {
		return ir.EssenceDoc{}, err
	}
	return copyDoc, nil
}

// newTrackedDoc clones src and wraps its slices into fixed-index tracked slots.
func newTrackedDoc(src ir.EssenceDoc) (*trackedDoc, error) {
	cloned, err := cloneEssenceDoc(src)
	if err != nil {
		return nil, err
	}
	td := &trackedDoc{
		system:            wrapTrackedParts(cloned.System),
		messages:          make([]trackedMsg, len(cloned.Messages)),
		tools:             make([]trackedTool, len(cloned.Tools)),
		toolChoice:        cloned.ToolChoice,
		parallelToolCalls: cloned.ParallelToolCalls,
		sampling:          cloned.Sampling,
		maxOutputTokens:   cloned.MaxOutputTokens,
		stop:              cloned.Stop,
		reasoning:         cloned.Reasoning,
		output:            cloned.Output,
		parts:             wrapTrackedParts(cloned.Parts),
		finish:            cloned.Finish,
		usage:             cloned.Usage,
		serviceTier:       cloned.ServiceTier,
		hasExtra:          cloned.HasExtra,
	}
	for i, m := range cloned.Messages {
		td.messages[i] = trackedMsg{
			role:     m.Role,
			id:       m.ID,
			parts:    wrapTrackedParts(m.Parts),
			hasExtra: m.HasExtra,
		}
	}
	for i, t := range cloned.Tools {
		td.tools[i] = trackedTool{tool: t}
	}
	return td, nil
}

// wrapTrackedParts wraps each EssencePart and any nested tool_result Content parts.
func wrapTrackedParts(parts []ir.EssencePart) []trackedPart {
	if len(parts) == 0 {
		return nil
	}
	out := make([]trackedPart, len(parts))
	for i, p := range parts {
		tp := trackedPart{part: p}
		if p.Result != nil && len(p.Result.Content) > 0 {
			tp.nestedContent = wrapTrackedParts(p.Result.Content)
		}
		out[i] = tp
	}
	return out
}

// parseIRPath splits a dotted/indexed IR path into validated irPathStep tokens.
func parseIRPath(raw string) ([]irPathStep, error) {
	trimmed := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(raw), "$"))
	trimmed = strings.TrimPrefix(trimmed, ".")
	if trimmed == "" {
		return nil, nil
	}
	segments := strings.Split(trimmed, ".")
	steps := make([]irPathStep, 0, len(segments))
	for _, seg := range segments {
		if seg == "" {
			return nil, fmt.Errorf("empty segment in path %q", raw)
		}
		openIdx := strings.IndexByte(seg, '[')
		if openIdx < 0 {
			steps = append(steps, irPathStep{field: seg})
			continue
		}
		if !strings.HasSuffix(seg, "]") {
			return nil, fmt.Errorf("malformed index segment %q in path %q", seg, raw)
		}
		idxStr := seg[openIdx+1 : len(seg)-1]
		idx, err := strconv.Atoi(idxStr)
		if err != nil || idx < 0 {
			return nil, fmt.Errorf("invalid array index %q in path %q", idxStr, raw)
		}
		steps = append(steps, irPathStep{
			field:    seg[:openIdx],
			hasIndex: true,
			index:    idx,
		})
	}
	return steps, nil
}

// selectPartSlot resolves idx inside slots. Drop operations require the exact
// original slot to still be alive, whereas non-drop property operations fall
// back to the idx-th surviving slot when an earlier pipeline pass already
// deleted slots[idx].
func selectPartSlot(slots []trackedPart, idx int, forDrop bool) (*trackedPart, error) {
	if idx < 0 || idx >= len(slots) {
		return nil, fmt.Errorf("part index %d out of range (%d)", idx, len(slots))
	}
	if !slots[idx].dropped {
		return &slots[idx], nil
	}
	if forDrop {
		return nil, fmt.Errorf("part index %d was already dropped", idx)
	}
	alive := 0
	for i := range slots {
		if !slots[i].dropped {
			if alive == idx {
				return &slots[i], nil
			}
			alive++
		}
	}
	return nil, fmt.Errorf("part index %d refers to a dropped element", idx)
}

// locateTrackedPart resolves steps to a trackedPart inside system[i], parts[i],
// messages[m].parts[p], or messages[m].parts[p].result.content[c]. Invalid paths
// or out-of-bounds indices return an error.
func locateTrackedPart(doc *trackedDoc, steps []irPathStep, forDrop bool) (*trackedPart, error) {
	switch {
	case len(steps) == 1 && steps[0].field == "system" && steps[0].hasIndex:
		return selectPartSlot(doc.system, steps[0].index, forDrop)
	case len(steps) == 1 && steps[0].field == "parts" && steps[0].hasIndex:
		return selectPartSlot(doc.parts, steps[0].index, forDrop)
	case len(steps) >= 2 && steps[0].field == "messages" && steps[0].hasIndex && steps[1].field == "parts" && steps[1].hasIndex:
		mIdx := steps[0].index
		if mIdx >= len(doc.messages) {
			return nil, fmt.Errorf("messages index %d out of range (%d)", mIdx, len(doc.messages))
		}
		parent, err := selectPartSlot(doc.messages[mIdx].parts, steps[1].index, len(steps) == 2 && forDrop)
		if err != nil {
			return nil, err
		}
		if len(steps) == 2 {
			return parent, nil
		}
		if len(steps) == 4 && steps[2].field == "result" && !steps[2].hasIndex && steps[3].field == "content" && steps[3].hasIndex {
			if parent.part.Result == nil {
				return nil, fmt.Errorf("messages[%d].parts[%d] has nil result", mIdx, steps[1].index)
			}
			return selectPartSlot(parent.nestedContent, steps[3].index, forDrop)
		}
	}
	return nil, fmt.Errorf("path does not address an EssencePart")
}

// recordExtraDropped clears hasExtra on the root document, message, tool,
// annotation, or part identified by loss.Path, and clears serviceTier when
// loss.Path targets root service_tier. Namespace and sub-object paths omitted
// from EssenceDoc return nil without clearing root HasExtra, whereas malformed
// or out-of-range paths return an error.
func recordExtraDropped(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "extra")
	if len(steps) == 0 {
		doc.hasExtra = false
		return nil
	}
	if len(steps) == 1 && !steps[0].hasIndex {
		switch steps[0].field {
		case "tool_choice", "output", "reasoning", "metadata", "conversation", "service_tier":
			if steps[0].field == "service_tier" {
				doc.serviceTier = ""
			}
			doc.partialRootExtraDropped = true
			return nil
		}
	}
	if len(steps) == 1 && steps[0].field == "messages" && steps[0].hasIndex {
		if steps[0].index >= len(doc.messages) {
			return fmt.Errorf("messages index %d out of range", steps[0].index)
		}
		doc.messages[steps[0].index].hasExtra = false
		return nil
	}
	if len(steps) == 1 && steps[0].field == "tools" && steps[0].hasIndex {
		if steps[0].index >= len(doc.tools) {
			return fmt.Errorf("tools index %d out of range", steps[0].index)
		}
		doc.tools[steps[0].index].tool.HasExtra = false
		return nil
	}
	if steps[0].field == "namespaces" && steps[0].hasIndex &&
		(len(steps) == 1 || (len(steps) == 2 && steps[1].field == "cache" && !steps[1].hasIndex)) {
		// EssenceDoc omits Namespace declarations because child tools carry their
		// own Namespace strings, so namespace-level Extra drops leave doc unchanged.
		return nil
	}
	if len(steps) >= 2 && steps[len(steps)-1].field == "annotations" && steps[len(steps)-1].hasIndex {
		tp, err := locateTrackedPart(doc, steps[:len(steps)-1], false)
		if err != nil {
			return err
		}
		aIdx := steps[len(steps)-1].index
		if aIdx >= len(tp.part.Annotations) {
			return fmt.Errorf("annotations index %d out of range", aIdx)
		}
		tp.part.Annotations[aIdx].HasExtra = false
		return nil
	}
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		return err
	}
	tp.part.HasExtra = false
	return nil
}

// recordOpaqueItemDropped tombstones an opaque part or hosted tool at loss.Path.
func recordOpaqueItemDropped(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	if len(steps) == 1 && steps[0].field == "tools" && steps[0].hasIndex {
		idx := steps[0].index
		if idx >= len(doc.tools) {
			return fmt.Errorf("tools index %d out of range", idx)
		}
		if doc.tools[idx].dropped {
			return fmt.Errorf("tools[%d] was already dropped", idx)
		}
		doc.tools[idx].dropped = true
		return nil
	}
	tp, err := locateTrackedPart(doc, steps, true)
	if err != nil {
		return err
	}
	if tp.part.Kind != ir.PartOpaque {
		return fmt.Errorf("part kind is %q, want %q", tp.part.Kind, ir.PartOpaque)
	}
	tp.dropped = true
	return nil
}

// isDirectDroppedReasoningSlot reports whether steps directly indexes an
// original part slot that was already tombstoned as an ir.PartReasoning item.
// Companion token and text losses emitted when omitting an entire reasoning
// item remain idempotent regardless of whether Token or Segments cleared first.
func isDirectDroppedReasoningSlot(doc *trackedDoc, steps []irPathStep) bool {
	var slots []trackedPart
	var idx int
	switch {
	case len(steps) == 1 && steps[0].field == "system" && steps[0].hasIndex:
		slots, idx = doc.system, steps[0].index
	case len(steps) == 1 && steps[0].field == "parts" && steps[0].hasIndex:
		slots, idx = doc.parts, steps[0].index
	case len(steps) == 2 && steps[0].field == "messages" && steps[0].hasIndex && steps[1].field == "parts" && steps[1].hasIndex:
		if steps[0].index >= len(doc.messages) {
			return false
		}
		slots, idx = doc.messages[steps[0].index].parts, steps[1].index
	default:
		return false
	}
	return idx >= 0 && idx < len(slots) && slots[idx].dropped && slots[idx].part.Kind == ir.PartReasoning
}

// recordReplayTokenDropped clears Token on a reasoning, tool_call, or text part
// addressed by loss.Path. Reasoning or empty text parts left with no remaining
// visible content or metadata are tombstoned, while invalid targets return an error.
func recordReplayTokenDropped(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "token", "reasoning", "call")
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		if isDirectDroppedReasoningSlot(doc, steps) {
			return nil
		}
		return err
	}
	switch {
	case tp.part.Reasoning != nil:
		tp.part.Reasoning.Token = nil
		tp.part.Reasoning.Redacted = false
		if len(tp.part.Reasoning.Segments) == 0 {
			tp.dropped = true
		}
		return nil
	case tp.inlined:
		return nil
	case tp.part.Call != nil:
		tp.part.Call.Token = nil
		return nil
	case tp.part.Kind == ir.PartText && tp.part.Token != nil:
		tp.part.Token = nil
		tp.textTokenDropped = true
		return nil
	default:
		if isDirectDroppedReasoningSlot(doc, steps) {
			return nil
		}
		return fmt.Errorf("part kind %q has no reasoning, call, or text token", tp.part.Kind)
	}
}

// recordReasoningTextDropped clears visible reasoning segments and tombstones
// the reasoning part when no continuity token remains.
func recordReasoningTextDropped(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "segments", "reasoning")
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		if isDirectDroppedReasoningSlot(doc, steps) {
			return nil
		}
		return err
	}
	if tp.part.Kind != ir.PartReasoning || tp.part.Reasoning == nil {
		if isDirectDroppedReasoningSlot(doc, steps) {
			return nil
		}
		return fmt.Errorf("part kind is %q, want reasoning", tp.part.Kind)
	}
	tp.part.Reasoning.Segments = nil
	tp.part.Reasoning.Summary = false
	if tp.part.Reasoning.Token == nil {
		tp.dropped = true
	}
	return nil
}

// recordReasoningTextInlined converts a reasoning part into a visible text part
// joining its segments. Targets that inline unsigned historical thinking keep
// the resulting part alive in turn order.
func recordReasoningTextInlined(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "reasoning")
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		return err
	}
	if tp.part.Kind != ir.PartReasoning || tp.part.Reasoning == nil {
		return fmt.Errorf("part kind is %q, want reasoning", tp.part.Kind)
	}
	joined := strings.Join(tp.part.Reasoning.Segments, "\n")
	tp.part = ir.EssencePart{
		Kind:     ir.PartText,
		ID:       tp.part.ID,
		Text:     joined,
		CacheTTL: tp.part.CacheTTL,
		HasExtra: tp.part.HasExtra,
	}
	tp.inlined = true
	tp.dropped = false
	return nil
}

// recordToolHistoryInlined marks a message slot for recovery text expansion.
// The original call or result stays available while subsequent losses address
// its token, cache marker, or nested content using original source indices.
func recordToolHistoryInlined(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	if len(steps) != 2 || steps[0].field != "messages" || steps[1].field != "parts" {
		return fmt.Errorf("tool history requires a message part at %q", loss.Path)
	}
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		return err
	}
	if tp.part.Call == nil && tp.part.Result == nil {
		return fmt.Errorf("part kind is %q, want tool_call or tool_result", tp.part.Kind)
	}
	tp.historyKind = tp.part.Kind
	tp.historyToolName = loss.Detail
	return nil
}

// recordRefusalInlined downgrades a refusal part into a visible text part while
// preserving its Text, ID, Annotations, CacheTTL, and HasExtra fields. Protocols
// without a dedicated refusal block emit normal text paired with a refusal stop reason.
func recordRefusalInlined(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		return err
	}
	if tp.part.Kind != ir.PartRefusal {
		return fmt.Errorf("part kind is %q, want refusal", tp.part.Kind)
	}
	tp.part.Kind = ir.PartText
	return nil
}

// recordCacheMarkDropped clears CacheTTL on the part or tool declaration
// identified by loss.Path. Namespace paths return nil because EssenceDoc omits
// namespace metadata, whereas invalid paths or out-of-range indices return an error.
func recordCacheMarkDropped(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "cache")
	if len(steps) == 1 && steps[0].field == "tools" && steps[0].hasIndex {
		if steps[0].index >= len(doc.tools) {
			return fmt.Errorf("tools index %d out of range", steps[0].index)
		}
		doc.tools[steps[0].index].tool.CacheTTL = ""
		return nil
	}
	if len(steps) == 1 && steps[0].field == "namespaces" && steps[0].hasIndex {
		// EssenceDoc omits Namespace declarations; dropping a namespace-level
		// cache breakpoint leaves EssenceDoc unchanged.
		return nil
	}
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		return err
	}
	tp.part.CacheTTL = ""
	return nil
}

// recordMediaUnsupported tombstones an unsupported media part at loss.Path, or
// converts a base64 image part into an inline Markdown data URL placeholder when
// loss.Detail is "markdown_data_url" so phase two verifies MIME and payload hash.
// Out-of-range paths, non-media targets, or non-image markdown_data_url targets return an error.
func recordMediaUnsupported(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "media")
	if strings.TrimSpace(loss.Detail) == "markdown_data_url" {
		tp, err := locateTrackedPart(doc, steps, false)
		if err != nil {
			return err
		}
		if tp.part.Kind != ir.PartMedia || tp.part.Media == nil ||
			tp.part.Media.Type != ir.MediaImage ||
			tp.part.Media.Source != ir.MediaSourceBase64 ||
			tp.part.Media.MIME == "" || tp.part.Media.ContentHash == "" {
			return fmt.Errorf("markdown_data_url requires a base64 image media part with MIME and hash at %q", loss.Path)
		}
		medCopy := *tp.part.Media
		tp.inlinedMedia = &medCopy
		tp.part = ir.EssencePart{
			Kind:     ir.PartText,
			ID:       tp.part.ID,
			Text:     fmt.Sprintf("![image](data:%s;base64,<%s>)", medCopy.MIME, medCopy.ContentHash),
			CacheTTL: tp.part.CacheTTL,
			HasExtra: tp.part.HasExtra,
		}
		tp.dropped = false
		return nil
	}
	tp, err := locateTrackedPart(doc, steps, true)
	if err != nil {
		return err
	}
	if tp.part.Kind != ir.PartMedia {
		return fmt.Errorf("part kind is %q, want media", tp.part.Kind)
	}
	tp.dropped = true
	return nil
}

// recordMediaRelocated marks a tool_result part for media attachment relocation.
// Phase two moves its media items into the following user turn so phase-one
// message indices stay fixed.
func recordMediaRelocated(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	if len(steps) < 2 || steps[0].field != "messages" || !steps[0].hasIndex || steps[1].field != "parts" || !steps[1].hasIndex {
		return fmt.Errorf("media_relocated requires messages[m].parts[p] path")
	}
	tp, err := locateTrackedPart(doc, steps[:2], false)
	if err != nil {
		return err
	}
	if tp.part.Result == nil {
		return fmt.Errorf("target at %q is not a tool_result", loss.Path)
	}
	hasMedia := false
	for _, nested := range tp.nestedContent {
		if !nested.dropped && nested.part.Kind == ir.PartMedia {
			hasMedia = true
			break
		}
	}
	if !hasMedia {
		return fmt.Errorf("no media parts to relocate in %q", loss.Path)
	}
	tp.relocateMedia = true
	return nil
}

// recordToolLowered flags a tool declaration or historical call/result slot for
// phase-two surviving-ordinal lowering alignment. Antigravity legacy search
// functions report Detail "promoted_google_search" when promoted into a built-in
// googleSearch declaration or "duplicate_web_search" when deduplicated against
// an earlier search declaration.
func recordToolLowered(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	if len(steps) == 1 && steps[0].field == "tools" && steps[0].hasIndex {
		tIdx := steps[0].index
		if tIdx >= len(doc.tools) {
			return fmt.Errorf("tools index %d out of range", tIdx)
		}
		switch loss.Detail {
		case "duplicate_web_search":
			if doc.tools[tIdx].tool.Kind != ir.ToolFunction {
				return fmt.Errorf("tools[%d] kind is %q, want function for duplicate_web_search", tIdx, doc.tools[tIdx].tool.Kind)
			}
			doc.tools[tIdx].dropped = true
		case "promoted_google_search":
			if doc.tools[tIdx].tool.Kind != ir.ToolFunction {
				return fmt.Errorf("tools[%d] kind is %q, want function for promoted_google_search", tIdx, doc.tools[tIdx].tool.Kind)
			}
			doc.tools[tIdx].lowered = true
			doc.tools[tIdx].promotedToHosted = true
		default:
			doc.tools[tIdx].lowered = true
		}
		return nil
	}
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		return err
	}
	if tp.part.Call == nil && tp.part.Result == nil {
		return fmt.Errorf("part at %q is neither tool_call nor tool_result", loss.Path)
	}
	tp.toolLowered = true
	if tp.part.Call != nil && tp.part.Call.Tool.Kind == ir.ToolCustom {
		tp.wasCustom = true
	}
	return nil
}

// recordToolChoiceApproximated clears ToolChoice on doc and marks it approximated for phase two.
func recordToolChoiceApproximated(doc *trackedDoc, _ ir.Loss) error {
	doc.toolChoice = nil
	doc.toolChoiceApproximated = true
	return nil
}

// recordHostedToolDropped tombstones one hosted tool at tools[i] or all hosted
// tools when loss.Path addresses the entire tools slice.
func recordHostedToolDropped(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	if len(steps) == 1 && steps[0].field == "tools" {
		if steps[0].hasIndex {
			idx := steps[0].index
			if idx >= len(doc.tools) {
				return fmt.Errorf("tools index %d out of range", idx)
			}
			if doc.tools[idx].dropped {
				return fmt.Errorf("tools[%d] was already dropped", idx)
			}
			if doc.tools[idx].tool.Kind != ir.ToolHosted {
				return fmt.Errorf("tools[%d] kind is %q, want hosted", idx, doc.tools[idx].tool.Kind)
			}
			doc.tools[idx].dropped = true
			return nil
		}
		for i := range doc.tools {
			if doc.tools[i].tool.Kind == ir.ToolHosted {
				doc.tools[i].dropped = true
			}
		}
		return nil
	}
	return fmt.Errorf("hosted_tool_dropped requires tools or tools[i] path")
}

// recordSchemaApproximated flags tools[i] or output on doc according to
// loss.Path and loss.Detail so phase two reconciles target schema changes.
// Tool paths select strict boolean normalization on "strict", nil Strict
// verification on "strict_dropped", or schema-hash approximation otherwise,
// while nil Output, out-of-range tool indices, or unsupported paths return an error.
func recordSchemaApproximated(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "input_schema", "schema")
	if len(steps) == 1 && steps[0].field == "tools" && steps[0].hasIndex {
		idx := steps[0].index
		if idx >= len(doc.tools) {
			return fmt.Errorf("tools index %d out of range", idx)
		}
		switch loss.Detail {
		case "strict":
			doc.tools[idx].strictNormalized = true
		case "strict_dropped":
			doc.tools[idx].strictDropped = true
		default:
			doc.tools[idx].schemaApproximated = true
		}
		return nil
	}
	if len(steps) == 1 && steps[0].field == "output" && !steps[0].hasIndex {
		if doc.output == nil {
			return fmt.Errorf("source Output is nil")
		}
		doc.outputSchemaApproximated = true
		return nil
	}
	return fmt.Errorf("schema_approximated requires tools[i] or output path")
}

// recordSystemRelocated enables phase-two folding of system and developer turns.
func recordSystemRelocated(doc *trackedDoc, _ ir.Loss) error {
	doc.systemRelocated = true
	return nil
}

// recordPartsReordered marks either response parts or messages[i] for phase-two
// multiset canonicalization after per-kind part alignment completes.
func recordPartsReordered(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "parts")
	if len(steps) == 0 {
		doc.rootPartsReordered = true
		return nil
	}
	if len(steps) == 1 && steps[0].field == "messages" && steps[0].hasIndex {
		idx := steps[0].index
		if idx >= len(doc.messages) {
			return fmt.Errorf("messages index %d out of range", idx)
		}
		doc.messages[idx].partsReordered = true
		return nil
	}
	return fmt.Errorf("parts_reordered requires root parts or messages[i] path")
}

// recordToolPairingRepaired tombstones an unmatched tool_call or tool_result at
// loss.Path and records non-empty CallIDs so phase two removes synthetic repair
// stubs on dst. When loss.Detail is "missing_tool_call_id" on an empty-CallID
// tool_result, phase two instead folds surviving result text into a RoleUser turn.
func recordToolPairingRepaired(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	tp, err := locateTrackedPart(doc, steps, true)
	if err != nil {
		return err
	}
	if tp.missingIDToUser {
		return fmt.Errorf("part at %q was already repaired as missing_tool_call_id", loss.Path)
	}
	if strings.TrimSpace(loss.Detail) == "missing_tool_call_id" {
		if tp.part.Kind != ir.PartToolResult || tp.part.Result == nil || strings.TrimSpace(tp.part.Result.CallID) != "" {
			return fmt.Errorf("missing_tool_call_id requires tool_result with empty call_id at %q", loss.Path)
		}
		tp.missingIDToUser = true
		return nil
	}
	if tp.part.Call != nil && tp.part.Call.CallID != "" {
		doc.repairedCallIDs = append(doc.repairedCallIDs, tp.part.Call.CallID)
	} else if tp.part.Result != nil && tp.part.Result.CallID != "" {
		doc.repairedCallIDs = append(doc.repairedCallIDs, tp.part.Result.CallID)
	}
	tp.dropped = true
	return nil
}

// recordSamplingDropped clears the dropped sampling field named by loss.Path or loss.Detail.
func recordSamplingDropped(doc *trackedDoc, loss ir.Loss) error {
	field := strings.TrimSpace(loss.Detail)
	if field == "" {
		steps, err := parseIRPath(loss.Path)
		if err != nil {
			return err
		}
		if len(steps) == 2 && steps[0].field == "sampling" {
			field = steps[1].field
		} else if len(steps) == 1 {
			field = steps[0].field
		}
	}
	switch field {
	case "sampling", "":
		doc.sampling = ir.Sampling{CandidateCount: doc.sampling.CandidateCount}
	case "temperature":
		doc.sampling.Temperature = nil
	case "top_p":
		doc.sampling.TopP = nil
	case "top_k":
		doc.sampling.TopK = nil
	case "seed":
		doc.sampling.Seed = nil
	case "frequency_penalty":
		doc.sampling.FrequencyPenalty = nil
	case "presence_penalty":
		doc.sampling.PresencePenalty = nil
	default:
		return fmt.Errorf("unrecognized sampling field %q", field)
	}
	return nil
}

// recordStopDropped clears custom Stop sequences on doc when omitted by the target.
func recordStopDropped(doc *trackedDoc, _ ir.Loss) error {
	doc.stop = nil
	return nil
}

// recordAnnotationDropped clears structured Annotations on the targeted part.
func recordAnnotationDropped(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	steps = trimTrailingFieldSteps(steps, "annotations")
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		return err
	}
	tp.part.Annotations = nil
	return nil
}

// recordOutputFormatApproximated clears approximated OutputFormat subfields or
// flags the whole OutputFormat for reconciliation on both documents.
func recordOutputFormatApproximated(doc *trackedDoc, loss ir.Loss) error {
	if doc.output == nil {
		return nil
	}
	switch strings.TrimSpace(loss.Detail) {
	case "verbosity":
		doc.output.Verbosity = ""
		if *doc.output == (ir.EssenceOutputFormat{}) {
			doc.output = nil
		}
	case "description":
		doc.output.Description = ""
		if *doc.output == (ir.EssenceOutputFormat{}) {
			doc.output = nil
		}
	case "strict":
		doc.output.Strict = nil
	default:
		doc.output = nil
		doc.outputApproximatedAll = true
	}
	return nil
}

// recordClientMarkerDropped tombstones the client-marker text part at loss.Path.
// When an empty text part carried an attached replay token whose text body was
// omitted on the wire, it converts the slot into a token-bearing reasoning shell.
func recordClientMarkerDropped(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	tp, err := locateTrackedPart(doc, steps, true)
	if err != nil {
		return err
	}
	if tp.part.Kind == ir.PartText && tp.part.Text == "" && tp.part.Token != nil {
		tok := tp.part.Token
		tp.part = ir.EssencePart{
			Kind: ir.PartReasoning,
			Reasoning: &ir.EssenceReasoning{
				Token: tok,
			},
		}
		return nil
	}
	tp.dropped = true
	return nil
}

// recordCandidateCountIgnored clears Sampling.CandidateCount on doc when ignored by the target.
func recordCandidateCountIgnored(doc *trackedDoc, _ ir.Loss) error {
	doc.sampling.CandidateCount = nil
	return nil
}

// recordIDRewritten flags a message or part slot for identifier normalization.
// Phase two rewrites its wire Part.ID or tool CallID to a distinct paired
// sentinel across source and target turns, distinguishing ".id" from ".call_id"
// when a tool_call or tool_result carries both identifiers.
func recordIDRewritten(doc *trackedDoc, loss ir.Loss) error {
	steps, err := parseIRPath(loss.Path)
	if err != nil {
		return err
	}
	var leafField string
	if len(steps) > 0 && !steps[len(steps)-1].hasIndex {
		leafField = steps[len(steps)-1].field
	}
	steps = trimTrailingFieldSteps(steps, "id", "call_id", "call", "result")
	if len(steps) == 1 && steps[0].field == "messages" && steps[0].hasIndex {
		idx := steps[0].index
		if idx >= len(doc.messages) {
			return fmt.Errorf("messages index %d out of range", idx)
		}
		doc.messages[idx].idRewritten = true
		return nil
	}
	tp, err := locateTrackedPart(doc, steps, false)
	if err != nil {
		return err
	}
	switch leafField {
	case "id":
		tp.partIDRewritten = true
	case "call_id":
		tp.callIDRewritten = true
	default:
		if tp.part.Call != nil || tp.part.Result != nil {
			tp.callIDRewritten = true
		} else {
			tp.partIDRewritten = true
		}
	}
	return nil
}

// materializeAndAlign compacts tombstoned elements, executes system and media
// relocations, and pairs surviving tools, messages, and parts against dst. Any
// turns marked with parts_reordered are sorted after ordinal pairing completes.
func materializeAndAlign(td *trackedDoc, dst *ir.EssenceDoc, hasDst bool) (ir.EssenceDoc, error) {
	if hasDst {
		if td.toolChoiceApproximated {
			dst.ToolChoice = nil
		}
		if td.outputApproximatedAll {
			dst.Output = nil
		}
		for _, callID := range td.repairedCallIDs {
			if !hasSurvivingCallID(td, callID) {
				removeSyntheticCallIDFromDoc(dst, callID)
			}
		}
	}

	// Capture declared target names before tool alignment replaces them with
	// comparison sentinels. Recovery text contains the actual lowered names.
	materializeToolHistory(td, dst)
	survivingTools, err := alignSurvivingTools(td, dst, hasDst)
	if err != nil {
		return ir.EssenceDoc{}, err
	}

	if td.outputSchemaApproximated && td.output != nil {
		td.output.SchemaHash = approximatedHashSentinel
		if hasDst {
			if dst.Output == nil || dst.Output.SchemaHash == "" {
				return ir.EssenceDoc{}, fmt.Errorf("dst.Output missing schema hash after schema_approximated")
			}
			dst.Output.SchemaHash = approximatedHashSentinel
		}
	}

	sysParts := compactPlainParts(td.system)
	survivingMsgs := buildMaterializedMessages(td.messages, dst, hasDst)
	if td.systemRelocated {
		sysParts, survivingMsgs = foldTrackedSystemIntoTopLevel(sysParts, survivingMsgs)
		if hasDst {
			foldDocSystemIntoTopLevel(dst)
		}
	}
	if hasDst {
		survivingMsgs = coalesceAdjacentMaterializedTurns(survivingMsgs, dst)
		dst.Messages = coalesceAdjacentDstToolTurns(dst.Messages, survivingMsgs)
	}

	rootParts := compactTrackedParts(td.parts)
	if hasDst {
		var rootErr error
		rootParts, rootErr = reconcileRootResponseParts(rootParts, dst.Parts)
		if rootErr != nil {
			return ir.EssenceDoc{}, rootErr
		}
	}
	nextID := 0
	if err := alignSurvivingMessagesAndParts(survivingMsgs, rootParts, dst, hasDst, &nextID); err != nil {
		return ir.EssenceDoc{}, err
	}

	outMsgs := make([]ir.EssenceMessage, len(survivingMsgs))
	for i, m := range survivingMsgs {
		plain := unwrapTrackedParts(m.parts)
		if m.partsReordered {
			sortPartsDeterministically(plain)
			if hasDst && i < len(dst.Messages) {
				sortPartsDeterministically(dst.Messages[i].Parts)
			}
		}
		outMsgs[i] = ir.EssenceMessage{
			Role:     m.role,
			ID:       m.id,
			Parts:    plain,
			HasExtra: m.hasExtra,
		}
	}

	plainRootParts := unwrapTrackedParts(rootParts)
	if td.rootPartsReordered {
		sortPartsDeterministically(plainRootParts)
		if hasDst {
			sortPartsDeterministically(dst.Parts)
		}
	}

	if td.partialRootExtraDropped && (!hasDst || !dst.HasExtra) {
		td.hasExtra = false
	}

	out := ir.EssenceDoc{
		System:            sysParts,
		Messages:          outMsgs,
		Tools:             survivingTools,
		ToolChoice:        td.toolChoice,
		ParallelToolCalls: td.parallelToolCalls,
		Sampling:          td.sampling,
		MaxOutputTokens:   td.maxOutputTokens,
		Stop:              td.stop,
		Reasoning:         td.reasoning,
		Output:            td.output,
		Parts:             plainRootParts,
		Finish:            td.finish,
		Usage:             td.usage,
		ServiceTier:       td.serviceTier,
		HasExtra:          td.hasExtra,
	}
	cleanupEmptyCollections(&out)
	return out, nil
}

// materializeToolHistory expands each marked source slot into its expected
// visible text and surviving media after all original-coordinate losses have
// been applied. Inlined tool results belong to user turns in the retry.
//
// Keeping the nested slots intact until this point allows a cache loss or
// media omission to address an individual child even after its parent has
// been marked for expansion. The expected text is derived from source history;
// changing a name, argument, result, or attachment on the wire remains visible.
func materializeToolHistory(td *trackedDoc, dst *ir.EssenceDoc) {
	for i := range td.messages {
		msg := &td.messages[i]
		var expanded []trackedPart
		for j, slot := range msg.parts {
			if slot.historyKind == "" || slot.dropped {
				if expanded != nil {
					expanded = append(expanded, slot)
				}
				continue
			}
			if expanded == nil {
				expanded = make([]trackedPart, 0, len(msg.parts))
				expanded = append(expanded, msg.parts[:j]...)
			}
			text := trackedPart{
				part:            ir.EssencePart{Kind: ir.PartText, ID: slot.part.ID, CacheTTL: slot.part.CacheTTL, HasExtra: slot.part.HasExtra},
				partIDRewritten: slot.partIDRewritten,
			}
			if call := slot.part.Call; call != nil {
				text.part.Text = inlinedToolCallText(&slot, td.tools, dst)
				text.historyKind = ir.PartToolCall
				text.historyCallID = call.CallID
				text.callIDRewritten = slot.callIDRewritten
				expanded = append(expanded, text)
				continue
			}

			res := slot.part.Result
			text.historyKind = ir.PartToolResult
			text.historyCallID = res.CallID
			header := "(tool_result)"
			if res.CallID != "" {
				header += " tool_use_id=" + res.CallID
			}
			if res.IsError {
				header += " is_error=true"
			}
			lines := []string{header}
			var media []trackedPart
			for _, child := range slot.nestedContent {
				if child.dropped {
					continue
				}
				if child.part.Kind == ir.PartMedia {
					media = append(media, child)
					continue
				}
				if child.part.Text != "" {
					lines = append(lines, child.part.Text)
				}
				// Keep undeclared metadata loss visible on the merged text. Reported
				// child losses have already cleared their corresponding source fields.
				if child.part.CacheTTL != "" {
					text.part.CacheTTL = child.part.CacheTTL
				}
				text.part.HasExtra = text.part.HasExtra || child.part.HasExtra
			}
			text.part.Text = strings.Join(lines, "\n")
			expanded = append(expanded, text)
			expanded = append(expanded, media...)
			if msg.role == ir.RoleTool {
				msg.role = ir.RoleUser
			}
		}
		if expanded != nil {
			msg.parts = expanded
		}
	}
}

// inlinedToolCallText reconstructs the recovery header and arguments from the
// source call. A lowered declaration can supply its target name because the
// corresponding loss permits renaming. Custom inputs retain their original text
// inside the function argument wrapper used by the lowering pipeline.
//
// Only declarations that survive on the target consume a wire index. Keep the
// original call ID in the expected header until the alignment pass checks any
// separately recorded identifier rewrite against the corresponding result.
func inlinedToolCallText(slot *trackedPart, tools []trackedTool, dst *ir.EssenceDoc) string {
	call := slot.part.Call
	name := call.Tool.Name
	args := call.Arguments
	if slot.toolLowered {
		if call.Tool.Kind == ir.ToolCustom {
			// Wrapping a free-text custom argument as a JSON string preserves embedded
			// whitespace and quotes when the upstream dialect accepts functions only.
			raw, _ := json.Marshal(map[string]string{"input": args})
			args = string(raw)
		}
		if call.Tool.Kind == ir.ToolClientSearch {
			name = "tool_search"
		}
		if slot.historyToolName != "" {
			// Historical calls can outlive their declaration. Use the independently
			// recorded lowering result even when the current request omits the declaration.
			name = slot.historyToolName
		}
		if dst != nil && slot.historyToolName == "" {
			index := 0
			for _, t := range tools {
				if t.dropped {
					continue
				}
				if index < len(dst.Tools) && t.lowered && t.tool.Kind == call.Tool.Kind &&
					t.tool.Name == call.Tool.Name && t.tool.Namespace == call.Tool.Namespace {
					name = dst.Tools[index].Name
					break
				}
				index++
			}
		}
	}
	header := "(tool_use)"
	if name != "" {
		header += " name=" + name
	}
	if call.CallID != "" {
		header += " id=" + call.CallID
	}
	if args != "" && args != "null" {
		header += " input=" + args
	}
	return header
}

// alignSurvivingTools compacts non-dropped tools, pairs them by surviving
// ordinal against dst.Tools, and reconciles historical tool_search_output
// declarations promoted into dst.Tools when client search was lowered. Earlier
// hosted_tool_dropped or duplicate_web_search entries never shift the target
// index used by later checks. When promoted_google_search promotes a client
// search function into a built-in hosted declaration, dst.Tools[sIdx] must
// carry non-empty hosted metadata so source and target tool essences align.
func alignSurvivingTools(td *trackedDoc, dst *ir.EssenceDoc, hasDst bool) ([]ir.EssenceTool, error) {
	tools := td.tools
	var out []ir.EssenceTool
	searchLowered := false
	for _, st := range tools {
		if st.dropped {
			continue
		}
		sIdx := len(out)
		t := st.tool
		if st.lowered {
			origKind := t.Kind
			if origKind == ir.ToolClientSearch {
				searchLowered = true
			}
			t.Kind = ir.ToolFunction
			t.Namespace = ""
			t.Name = loweredToolSentinel
			t.FormatHash = ""
			t.SchemaHash = approximatedHashSentinel
			if hasDst {
				if sIdx >= len(dst.Tools) {
					return nil, fmt.Errorf("dst.Tools has %d items, want surviving lowered tool at %d", len(dst.Tools), sIdx)
				}
				dKind := dst.Tools[sIdx].Kind
				allowedHostedPromotion := st.promotedToHosted && origKind == ir.ToolFunction && dKind == ir.ToolHosted
				if (dKind != ir.ToolFunction && dKind != origKind && !allowedHostedPromotion) || dst.Tools[sIdx].Name == "" {
					return nil, fmt.Errorf("dst.Tools[%d] must be a named function or %s tool after tool_lowered", sIdx, origKind)
				}
				t.Kind = dKind
				t.Strict = cloneEssenceBoolPtr(dst.Tools[sIdx].Strict)
				t.DeferLoading = cloneEssenceBoolPtr(dst.Tools[sIdx].DeferLoading)
				if (origKind == ir.ToolHosted || st.promotedToHosted) && dKind == ir.ToolHosted {
					if dst.Tools[sIdx].HostedType == "" || dst.Tools[sIdx].HostedConfigHash == "" {
						return nil, fmt.Errorf("dst.Tools[%d] missing hosted metadata after tool_lowered", sIdx)
					}
					t.HostedType = dst.Tools[sIdx].HostedType
					t.HostedConfigHash = dst.Tools[sIdx].HostedConfigHash
				}
				dst.Tools[sIdx].Namespace = ""
				dst.Tools[sIdx].Name = loweredToolSentinel
				dst.Tools[sIdx].FormatHash = ""
				dst.Tools[sIdx].SchemaHash = approximatedHashSentinel
				// Antigravity schema cleaning appends unsupported constraint hints onto
				// the target tool description, so align descriptions after schema lowering.
				dst.Tools[sIdx].Description = t.Description
			}
		} else {
			if st.schemaApproximated {
				t.SchemaHash = approximatedHashSentinel
				if hasDst {
					if sIdx >= len(dst.Tools) || dst.Tools[sIdx].SchemaHash == "" {
						return nil, fmt.Errorf("dst.Tools[%d] missing schema hash after schema_approximated", sIdx)
					}
					dst.Tools[sIdx].SchemaHash = approximatedHashSentinel
					// Antigravity and Gemini schema approximation may append constraint hints
					// onto tool descriptions when stripping unsupported validation keywords.
					dst.Tools[sIdx].Description = t.Description
				}
			}
			if st.strictDropped {
				if hasDst {
					if sIdx >= len(dst.Tools) {
						return nil, fmt.Errorf("dst.Tools has %d items, want surviving tool at %d", len(dst.Tools), sIdx)
					}
					if dst.Tools[sIdx].Strict != nil {
						return nil, fmt.Errorf("dst.Tools[%d].Strict = %v after strict_dropped; expected nil", sIdx, *dst.Tools[sIdx].Strict)
					}
				}
				t.Strict = nil
			}
			if st.strictNormalized && hasDst {
				if sIdx >= len(dst.Tools) {
					return nil, fmt.Errorf("dst.Tools has %d items, want surviving tool at %d", len(dst.Tools), sIdx)
				}
				srcStrict := t.Strict
				dstStrict := dst.Tools[sIdx].Strict
				validToggle := (srcStrict == nil && dstStrict != nil && !*dstStrict) ||
					(srcStrict != nil && !*srcStrict && dstStrict == nil)
				if !validToggle {
					return nil, fmt.Errorf("dst.Tools[%d].Strict = %v after strict normalization from %v; expected toggle between nil and false", sIdx, dstStrict, srcStrict)
				}
				t.Strict = cloneEssenceBoolPtr(dstStrict)
			}
		}
		out = append(out, t)
	}

	if hasDst && searchLowered && len(dst.Tools) > len(out) {
		promoted := collectPromotedSearchDiscoveries(td)
		if len(dst.Tools) == len(out)+len(promoted) {
			for _, discName := range promoted {
				dIdx := len(out)
				dt := &dst.Tools[dIdx]
				if dt.Kind != ir.ToolFunction || dt.Name == "" {
					return nil, fmt.Errorf("promoted dst.Tools[%d] must be a named function tool for discovery %q", dIdx, discName)
				}
				if dt.Name != discName && !strings.Contains(dt.Name, discName) && len(dt.Name) != 64 {
					return nil, fmt.Errorf("promoted dst.Tools[%d] name %q does not match discovered tool %q", dIdx, dt.Name, discName)
				}
				dt.Namespace = ""
				dt.Name = loweredToolSentinel
				dt.FormatHash = ""
				dt.SchemaHash = approximatedHashSentinel
				out = append(out, *dt)
			}
		}
	}
	return out, nil
}

// cloneEssenceBoolPtr duplicates an optional bool pointer so src and dst EssenceTool structs stay detached.
func cloneEssenceBoolPtr(p *bool) *bool {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// collectPromotedSearchDiscoveries extracts newly discovered function and custom
// tool base names from historical tool_search_output JSON result arrays inside td.
func collectPromotedSearchDiscoveries(td *trackedDoc) []string {
	seenKeys := make(map[string]bool, len(td.tools))
	for _, st := range td.tools {
		if st.tool.Name == "" {
			continue
		}
		if st.tool.Namespace != "" && st.tool.Namespace != "functions" {
			seenKeys[st.tool.Namespace+"__"+st.tool.Name] = true
		} else {
			seenKeys[st.tool.Name] = true
		}
	}

	// rawDiscoveredChild decodes one child function declaration inside a discovered namespace object.
	type rawDiscoveredChild struct {
		Type string `json:"type"` // child tool kind ("function")
		Name string `json:"name"` // child tool name
	}
	// rawDiscoveredTool decodes one top-level discovered tool or namespace entry inside tool_search_output.
	type rawDiscoveredTool struct {
		Type     string               `json:"type"`     // "function", "custom", or "namespace"
		Name     string               `json:"name"`     // tool or namespace name
		Tools    []rawDiscoveredChild `json:"tools"`    // primary namespace children
		Children []rawDiscoveredChild `json:"children"` // alternate namespace children
	}

	var promoted []string
	for _, m := range td.messages {
		if m.role != ir.RoleTool {
			continue
		}
		for _, tp := range m.parts {
			if tp.dropped || tp.missingIDToUser || tp.part.Result == nil || tp.part.Result.IsError || len(tp.nestedContent) != 1 {
				continue
			}
			if tp.nestedContent[0].dropped || tp.nestedContent[0].part.Kind != ir.PartText {
				continue
			}
			txt := strings.TrimSpace(tp.nestedContent[0].part.Text)
			if !strings.HasPrefix(txt, "[") {
				continue
			}
			var arr []rawDiscoveredTool
			if err := json.Unmarshal([]byte(txt), &arr); err != nil {
				continue
			}
			for _, item := range arr {
				typ := strings.TrimSpace(item.Type)
				name := strings.TrimSpace(item.Name)
				switch typ {
				case "function", "custom":
					if name != "" && !seenKeys[name] {
						seenKeys[name] = true
						promoted = append(promoted, name)
					}
				case "namespace":
					if name == "" {
						continue
					}
					children := item.Tools
					if len(children) == 0 {
						children = item.Children
					}
					for _, ch := range children {
						if strings.TrimSpace(ch.Type) != "function" {
							continue
						}
						cName := strings.TrimSpace(ch.Name)
						if cName == "" {
							continue
						}
						key := cName
						if name != "functions" {
							key = name + "__" + cName
						}
						if !seenKeys[key] {
							seenKeys[key] = true
							promoted = append(promoted, cName)
						}
					}
				}
			}
		}
	}
	return promoted
}

// buildMaterializedMessages compacts dropped parts per message, folds ID-less
// tool_result text repaired with missing_tool_call_id into RoleUser turns, and
// relocates tool_result media attachments into the immediately following user turn.
//
// Source turns that started with zero parts remain when dst preserves empty
// turns, whereas turns emptied by tombstoning omit cleanly before
// coalesceAdjacentMaterializedTurns aligns surviving same-role runs against dst.
// Folded missing-ID tool_result turns merge into immediately preceding and
// immediately following metadata-free RoleUser turns without collapsing
// unrelated consecutive Chat user messages.
func buildMaterializedMessages(msgs []trackedMsg, dst *ir.EssenceDoc, hasDst bool) []materializedMsg {
	dstHasEmptyMsg := false
	if hasDst && dst != nil {
		for _, dm := range dst.Messages {
			if len(dm.Parts) == 0 {
				dstHasEmptyMsg = true
				break
			}
		}
	}
	var stage []pendingMsg
	for _, m := range msgs {
		stage = append(stage, expandTrackedMessageToPending(m, dstHasEmptyMsg)...)
	}

	var out []materializedMsg
	lastOutFoldedMissingID := false
	for i := range stage {
		cur := stage[i]
		if cur.fromMissingToolResultID {
			if len(out) > 0 && canMergeMaterializedUserTurns(out[len(out)-1], cur.msg) {
				prev := &out[len(out)-1]
				prev.parts = append(prev.parts, cur.msg.parts...)
				prev.idRewritten = prev.idRewritten || cur.msg.idRewritten
				prev.partsReordered = prev.partsReordered || cur.msg.partsReordered
			} else {
				out = append(out, cur.msg)
			}
			lastOutFoldedMissingID = true
			continue
		}

		if lastOutFoldedMissingID && (len(cur.msg.parts) > 0 || cur.originallyEmpty) &&
			len(out) > 0 && canMergeMaterializedUserTurns(out[len(out)-1], cur.msg) {
			prev := &out[len(out)-1]
			prev.parts = append(prev.parts, cur.msg.parts...)
			prev.idRewritten = prev.idRewritten || cur.msg.idRewritten
			prev.partsReordered = prev.partsReordered || cur.msg.partsReordered
			lastOutFoldedMissingID = false
		} else if len(cur.msg.parts) > 0 || cur.originallyEmpty {
			out = append(out, cur.msg)
			lastOutFoldedMissingID = false
		}
		if len(cur.movedMedia) == 0 {
			continue
		}
		if hasDst {
			stripRelocatedMediaAttributionOnDst(dst, unwrapTrackedParts(cur.movedMedia), cur.relocatedCallIDs)
		}
		if i+1 < len(stage) && stage[i+1].msg.role == ir.RoleUser {
			stage[i+1].msg.parts = append(slices.Clone(cur.movedMedia), stage[i+1].msg.parts...)
		} else {
			out = append(out, materializedMsg{
				role:  ir.RoleUser,
				parts: cur.movedMedia,
			})
			lastOutFoldedMissingID = false
		}
	}
	return out
}

// expandTrackedMessageToPending compacts one trackedMsg into zero or more
// pendingMsg entries in part order, splitting RoleTool turns at any
// missingIDToUser part so ID-less tool_result text materializes as RoleUser.
// Tombstoned non-empty turns return nil so adjacent surviving turns become contiguous.
func expandTrackedMessageToPending(m trackedMsg, dstHasEmptyMsg bool) []pendingMsg {
	origEmpty := len(m.parts) == 0
	var out []pendingMsg
	var liveParts []trackedPart
	var moved []trackedPart
	var relocatedCallIDs []string

	flushLive := func() {
		if len(liveParts) == 0 && len(moved) == 0 {
			return
		}
		out = append(out, pendingMsg{
			msg: materializedMsg{
				role:           m.role,
				id:             m.id,
				parts:          liveParts,
				hasExtra:       m.hasExtra,
				idRewritten:    m.idRewritten,
				partsReordered: m.partsReordered,
			},
			movedMedia:       moved,
			relocatedCallIDs: relocatedCallIDs,
		})
		liveParts = nil
		moved = nil
		relocatedCallIDs = nil
	}

	for _, tp := range m.parts {
		if tp.dropped {
			continue
		}
		if tp.missingIDToUser {
			flushLive()
			var texts []string
			for _, child := range tp.nestedContent {
				if !child.dropped && child.part.Kind == ir.PartText && child.part.Text != "" {
					texts = append(texts, child.part.Text)
				}
			}
			var userParts []trackedPart
			if joined := strings.Join(texts, ""); joined != "" {
				userParts = []trackedPart{{
					part: ir.EssencePart{
						Kind: ir.PartText,
						Text: joined,
					},
				}}
			}
			out = append(out, pendingMsg{
				msg: materializedMsg{
					role:        ir.RoleUser,
					id:          m.id,
					parts:       userParts,
					hasExtra:    m.hasExtra,
					idRewritten: m.idRewritten,
				},
				fromMissingToolResultID: true,
			})
			continue
		}

		copied := tp
		if copied.part.Result != nil {
			var keptContent []ir.EssencePart
			sawMoved := false
			for _, child := range copied.nestedContent {
				if child.dropped {
					continue
				}
				if copied.relocateMedia && child.part.Kind == ir.PartMedia {
					moved = append(moved, trackedPart{part: child.part})
					sawMoved = true
					continue
				}
				keptContent = append(keptContent, child.part)
			}
			if sawMoved && copied.part.Result.CallID != "" {
				relocatedCallIDs = append(relocatedCallIDs, copied.part.Result.CallID)
			}
			resCopy := *copied.part.Result
			resCopy.Content = keptContent
			copied.part.Result = &resCopy
		}
		liveParts = append(liveParts, copied)
	}
	flushLive()

	if len(out) == 0 {
		if origEmpty && dstHasEmptyMsg {
			out = append(out, pendingMsg{
				msg: materializedMsg{
					role:           m.role,
					id:             m.id,
					hasExtra:       m.hasExtra,
					idRewritten:    m.idRewritten,
					partsReordered: m.partsReordered,
				},
				originallyEmpty: true,
			})
			return out
		}
		return nil
	}
	return out
}

// canMergeMaterializedUserTurns reports whether two adjacent materializedMsg
// values are both RoleUser turns without surviving unrewritten IDs or Extra
// payloads. Turns carrying preserved IDs or Extra fields return false.
func canMergeMaterializedUserTurns(a, b materializedMsg) bool {
	return a.role == ir.RoleUser &&
		b.role == ir.RoleUser &&
		(a.id == "" || a.idRewritten) &&
		(b.id == "" || b.idRewritten) &&
		!a.hasExtra && !b.hasExtra
}

// coalesceAdjacentMaterializedTurns merges adjacent same-role source turns when
// the aligned contiguous role run in dst.Messages contains strictly fewer turns
// than msgs. Nil dst documents or slices with fewer than two turns return msgs
// unchanged without allocating.
//
// Comparing per-run lengths and target part capacities instead of global
// message counts keeps consecutive developer or user turns separate on Chat
// while still folding split assistant, user, or tool runs that the target
// encoder merged into a single wire message.
func coalesceAdjacentMaterializedTurns(msgs []materializedMsg, dst *ir.EssenceDoc) []materializedMsg {
	if dst == nil || len(msgs) <= 1 {
		return msgs
	}
	coalesced := make([]materializedMsg, 0, len(msgs))
	for i, m := range msgs {
		if len(coalesced) > 0 {
			k := len(coalesced) - 1
			prev := &coalesced[k]
			if k < len(dst.Messages) && prev.role == m.role && dst.Messages[k].Role == prev.role {
				srcRun := 1 + countMaterializedSameRoleRun(msgs[i:], prev.role)
				dstRun := countEssenceSameRoleRun(dst.Messages[k:], prev.role)
				extraCompatible := (!prev.hasExtra && !m.hasExtra) ||
					((prev.hasExtra || m.hasExtra) && dst.Messages[k].HasExtra)
				withinQuota := dstRun == 1 || len(prev.parts) < len(dst.Messages[k].Parts) || len(m.parts) == 0
				if srcRun > dstRun &&
					(prev.id == "" || prev.idRewritten) &&
					(m.id == "" || m.idRewritten) &&
					extraCompatible &&
					withinQuota {
					mergeMaterializedPartsIntoPrev(prev, m, dst.Messages[k].Parts)
					prev.hasExtra = prev.hasExtra || m.hasExtra
					prev.idRewritten = prev.idRewritten || m.idRewritten
					prev.partsReordered = prev.partsReordered || m.partsReordered
					continue
				}
			}
		}
		coalesced = append(coalesced, m)
	}
	return coalesced
}

// mergeMaterializedPartsIntoPrev appends m.parts onto prev.parts in place while
// reconciling reasoning part counts against dstParts. When the target turn
// collapsed multiple token-free PartReasoning items into one reasoning_content
// string, their text joins onto prev's last token-free reasoning part with "\n".
func mergeMaterializedPartsIntoPrev(prev *materializedMsg, m materializedMsg, dstParts []ir.EssencePart) {
	srcRsn := countTrackedPartsOfKind(prev.parts, ir.PartReasoning) + countTrackedPartsOfKind(m.parts, ir.PartReasoning)
	dstRsn := countPartsOfKind(dstParts, ir.PartReasoning)
	if srcRsn > dstRsn && dstRsn > 0 {
		lastRsnIdx := -1
		for idx, tp := range slices.Backward(prev.parts) {
			if tp.part.Kind == ir.PartReasoning && tp.part.Token == nil {
				lastRsnIdx = idx
				break
			}
		}
		if lastRsnIdx >= 0 {
			for _, tp := range m.parts {
				if tp.part.Kind == ir.PartReasoning && tp.part.Token == nil && srcRsn > dstRsn {
					prev.parts[lastRsnIdx].part.Text += "\n" + tp.part.Text
					srcRsn--
					continue
				}
				prev.parts = append(prev.parts, tp)
			}
			return
		}
	}
	prev.parts = append(prev.parts, m.parts...)
}

// countMaterializedSameRoleRun returns the length of the leading contiguous run
// of turns in msgs whose role equals role.
func countMaterializedSameRoleRun(msgs []materializedMsg, role ir.Role) int {
	n := 0
	for _, m := range msgs {
		if m.role != role {
			break
		}
		n++
	}
	return n
}

// countEssenceSameRoleRun returns the length of the leading contiguous run of
// messages in msgs whose Role equals role.
func countEssenceSameRoleRun(msgs []ir.EssenceMessage, role ir.Role) int {
	n := 0
	for _, m := range msgs {
		if m.Role != role {
			break
		}
		n++
	}
	return n
}

// coalesceAdjacentDstToolTurns merges consecutive metadata-free RoleTool
// messages in dstMsgs where the aligned contiguous RoleTool run in srcMsgs is
// shorter, such as a multi-result RoleTool turn expanded into per-result Chat
// tool messages. Slices with fewer than two messages return dstMsgs unchanged.
func coalesceAdjacentDstToolTurns(dstMsgs []ir.EssenceMessage, srcMsgs []materializedMsg) []ir.EssenceMessage {
	if len(dstMsgs) <= 1 {
		return dstMsgs
	}
	coalesced := make([]ir.EssenceMessage, 0, len(dstMsgs))
	for i, dm := range dstMsgs {
		if len(coalesced) > 0 {
			k := len(coalesced) - 1
			prev := &coalesced[k]
			if k < len(srcMsgs) && prev.Role == ir.RoleTool && dm.Role == ir.RoleTool && srcMsgs[k].role == ir.RoleTool {
				dstRun := 1 + countEssenceSameRoleRun(dstMsgs[i:], ir.RoleTool)
				srcRun := countMaterializedSameRoleRun(srcMsgs[k:], ir.RoleTool)
				if dstRun > srcRun && prev.ID == "" && dm.ID == "" && !prev.HasExtra && !dm.HasExtra {
					prev.Parts = append(prev.Parts, dm.Parts...)
					continue
				}
			}
		}
		coalesced = append(coalesced, dm)
	}
	return coalesced
}

// foldTrackedSystemIntoTopLevel extracts system and developer turns from msgs
// into sysParts in instruction appearance order. Empty system text slices yield
// a nil System slice so zero-value JSON serialization stays consistent.
func foldTrackedSystemIntoTopLevel(sysParts []ir.EssencePart, msgs []materializedMsg) ([]ir.EssencePart, []materializedMsg) {
	var collected []string
	for _, p := range sysParts {
		if p.Text != "" {
			collected = append(collected, p.Text)
		}
	}
	kept := make([]materializedMsg, 0, len(msgs))
	for _, m := range msgs {
		if m.role == ir.RoleSystem || m.role == ir.RoleDeveloper {
			for _, tp := range m.parts {
				if tp.part.Text != "" {
					collected = append(collected, tp.part.Text)
				}
			}
			continue
		}
		kept = append(kept, m)
	}
	if len(collected) == 0 {
		return nil, kept
	}
	return []ir.EssencePart{{
		Kind: ir.PartText,
		Text: strings.Join(collected, "\n"),
	}}, kept
}

// foldDocSystemIntoTopLevel folds system and developer turns on dst into dst.System.
func foldDocSystemIntoTopLevel(dst *ir.EssenceDoc) {
	var collected []string
	for _, p := range dst.System {
		if p.Text != "" {
			collected = append(collected, p.Text)
		}
	}
	kept := make([]ir.EssenceMessage, 0, len(dst.Messages))
	for _, m := range dst.Messages {
		if m.Role == ir.RoleSystem || m.Role == ir.RoleDeveloper {
			for _, p := range m.Parts {
				if p.Text != "" {
					collected = append(collected, p.Text)
				}
			}
			continue
		}
		kept = append(kept, m)
	}
	dst.Messages = kept
	if len(collected) == 0 {
		dst.System = nil
		return
	}
	dst.System = []ir.EssencePart{{
		Kind: ir.PartText,
		Text: strings.Join(collected, "\n"),
	}}
}

// alignSurvivingMessagesAndParts pairs surviving messages and response parts
// with dst by surviving turn index and per-kind part ordinal. Lowering and ID
// sentinels are applied before any parts_reordered sorting runs.
func alignSurvivingMessagesAndParts(msgs []materializedMsg, rootParts []trackedPart, dst *ir.EssenceDoc, hasDst bool, nextID *int) error {
	for mIdx := range msgs {
		if msgs[mIdx].idRewritten {
			sentinel := fmt.Sprintf("<rewritten_msg_id_%d>", *nextID)
			*nextID++
			msgs[mIdx].id = sentinel
			if hasDst && mIdx < len(dst.Messages) {
				dst.Messages[mIdx].ID = sentinel
			}
		}
		var dstParts []ir.EssencePart
		if hasDst && mIdx < len(dst.Messages) {
			dstParts = dst.Messages[mIdx].Parts
		}
		if err := alignPartSlice(msgs[mIdx].parts, mIdx, msgs, rootParts, dstParts, dst, hasDst, nextID); err != nil {
			return fmt.Errorf("messages[%d]: %w", mIdx, err)
		}
	}

	var dstRoot []ir.EssencePart
	if hasDst {
		dstRoot = dst.Parts
	}
	return alignPartSlice(rootParts, -1, msgs, rootParts, dstRoot, dst, hasDst, nextID)
}

// alignPartSlice applies toolLowered and idRewritten flags across one part
// slice. Per-kind ordinals locate counterparts inside dstParts even when target
// legalization reordered parts across kinds. A missing required counterpart
// or an incompatible target representation returns an error.
//
// Inlined history still carries the tool name, arguments and call ID in its text.
// Only the argument JSON has semantic whitespace and object-key normalization.
// A declared call-ID rewrite also applies to following result headers until a
// later call reuses the source identifier.
func alignPartSlice(parts []trackedPart, curMsgIdx int, allMsgs []materializedMsg, allRoot []trackedPart, dstParts []ir.EssencePart, dst *ir.EssenceDoc, hasDst bool, nextID *int) error {
	kindOrdinals := make(map[ir.PartKind]int)
	for pIdx := range parts {
		tp := &parts[pIdx]
		origKind := tp.part.Kind
		ord := kindOrdinals[origKind]
		kindOrdinals[origKind] = ord + 1

		var dPart *ir.EssencePart
		if hasDst {
			dPart = findNthPartOfKind(dstParts, origKind, ord)
		}

		if tp.historyKind == ir.PartToolCall && dPart != nil {
			if tp.callIDRewritten {
				// Resolve the declared ID rewrite from this call header, then require
				// subsequent inlined results to reference the same ID. The argument
				// JSON and result body may themselves contain the original ID.
				wantHeader, args, hasArgs := strings.Cut(tp.part.Text, " input=")
				gotHeader, _, _ := strings.Cut(dPart.Text, " input=")
				prefix := strings.TrimSuffix(wantHeader, " id="+tp.historyCallID) + " id="
				id, ok := strings.CutPrefix(gotHeader, prefix)
				if !ok || id == "" {
					return fmt.Errorf("inlined tool call at part %d has an invalid rewritten ID", pIdx)
				}
				rewriteCallIDInMaterialized(allMsgs, curMsgIdx, pIdx, tp.historyCallID, id)
				tp.part.Text = prefix + id
				if hasArgs {
					tp.part.Text += " input=" + args
				}
			}
			// Essence canonicalizes function JSON before comparison. Normalize only
			// the embedded argument bytes, preserving the exact wire name and call ID.
			if header, args, found := strings.Cut(dPart.Text, " input="); found {
				if canonical, err := CanonicalJSON([]byte(args)); err == nil {
					dPart.Text = header + " input=" + string(canonical)
				}
			}
		}
		if tp.relocateMedia && hasDst && dPart != nil && dPart.Result != nil {
			stripRelocatedMediaMarkerOnResult(dPart.Result)
		}

		if hasDst && dPart != nil {
			alignCarrierAndBareTokens(&tp.part, dPart)
		}

		if tp.toolLowered {
			var origCallKind ir.ToolKind
			var origCustomArgs string
			if tp.part.Call != nil {
				origCallKind = tp.part.Call.Tool.Kind
				origCustomArgs = tp.part.Call.Arguments
				tp.part.Call.Tool = ir.ToolRef{Kind: ir.ToolFunction, Name: loweredToolSentinel}
				if tp.wasCustom {
					tp.part.Call.Arguments = loweredArgsSentinel
				} else if strings.TrimSpace(tp.part.Call.Arguments) == "" {
					tp.part.Call.Arguments = "{}"
				}
			}
			if tp.part.Result != nil && (tp.part.Result.Tool.Kind != "" || tp.part.Result.Tool.Name != "") {
				tp.part.Result.Tool = ir.ToolRef{Kind: ir.ToolFunction, Name: loweredToolSentinel}
			}
			if hasDst {
				if dPart == nil {
					return fmt.Errorf("missing target %s part (ordinal %d) for tool_lowered", origKind, ord)
				}
				if dPart.Call != nil {
					dKind := dPart.Call.Tool.Kind
					if (dKind != ir.ToolFunction && dKind != origCallKind) || (dKind != ir.ToolClientSearch && dPart.Call.Tool.Name == "") {
						return fmt.Errorf("target call (ordinal %d) must be a named function or %s call", ord, origCallKind)
					}
					if tp.part.Call != nil {
						tp.part.Call.Tool = ir.ToolRef{Kind: dKind, Name: loweredToolSentinel}
						if tp.wasCustom && dKind == ir.ToolFunction {
							dPart.Call.Arguments = loweredArgsSentinel
						} else if tp.wasCustom {
							// Native custom tools that only sanitized their identifier keep free-text input verbatim.
							tp.part.Call.Arguments = origCustomArgs
						}
					}
					dPart.Call.Tool = ir.ToolRef{Kind: dKind, Name: loweredToolSentinel}
				}
				if dPart.Result != nil && ((tp.part.Result != nil && (tp.part.Result.Tool.Kind != "" || tp.part.Result.Tool.Name != "")) || dPart.Result.Tool.Kind != "" || dPart.Result.Tool.Name != "") {
					resKind := dPart.Result.Tool.Kind
					if resKind == "" {
						resKind = ir.ToolFunction
					}
					if tp.part.Result != nil {
						tp.part.Result.Tool = ir.ToolRef{Kind: resKind, Name: loweredToolSentinel}
					}
					dPart.Result.Tool = ir.ToolRef{Kind: resKind, Name: loweredToolSentinel}
				}
			}
		}

		if tp.partIDRewritten {
			sentinel := fmt.Sprintf("<rewritten_part_id_%d>", *nextID)
			*nextID++
			tp.part.ID = sentinel
			if hasDst && dPart != nil {
				dPart.ID = sentinel
			}
		}

		if tp.callIDRewritten && tp.historyKind != ir.PartToolCall {
			sentinel := fmt.Sprintf("<rewritten_id_%d>", *nextID)
			*nextID++
			switch {
			case tp.part.Call != nil:
				oldSrcID := tp.part.Call.CallID
				tp.part.Call.CallID = sentinel
				rewriteCallIDInMaterialized(allMsgs, curMsgIdx, pIdx, oldSrcID, sentinel)
				if hasDst {
					if dPart == nil || dPart.Call == nil || dPart.Call.CallID == "" {
						return fmt.Errorf("target call (ordinal %d) missing rewritten call_id", ord)
					}
					rewriteCallIDAcrossDoc(dst, curMsgIdx, dPart.Call.CallID, sentinel)
				}
			case tp.part.Result != nil:
				wasUnlinked := tp.part.Result.CallID == "" && tp.part.Result.Tool == (ir.ToolRef{})
				tp.part.Result.CallID = sentinel
				if hasDst && dPart != nil && dPart.Result != nil {
					dPart.Result.CallID = sentinel
					if wasUnlinked {
						// Unnamed source pairs lack CallIDs for ir.Link until encoding synthesizes one.
						tp.part.Result.Tool = dPart.Result.Tool
					}
				}
			default:
				tp.part.ID = sentinel
				if hasDst && dPart != nil {
					dPart.ID = sentinel
				}
			}
		}
	}
	return nil
}

// alignCarrierAndBareTokens reconciles Scheme, ProviderID, and token-only
// Redacted metadata when one EssencePart unpacked a trc1 carrier envelope while
// its counterpart decoded the identical bare issuer token from wire JSON.
// Step-initial Gemini placeholder call signatures injected onto unsigned source
// calls clear on dstPart, while distinct non-empty schemes never align.
func alignCarrierAndBareTokens(srcPart, dstPart *ir.EssencePart) {
	if srcPart.Token != nil && dstPart.Token != nil {
		sTok := srcPart.Token
		dTok := dstPart.Token
		if sTok.Data != "" && sTok.Data == dTok.Data {
			switch {
			case sTok.Scheme == "" && sTok.ProviderID == 0 && dTok.Scheme != "":
				sTok.Scheme = dTok.Scheme
				sTok.ProviderID = dTok.ProviderID
			case dTok.Scheme == "" && dTok.ProviderID == 0 && sTok.Scheme != "":
				dTok.Scheme = sTok.Scheme
				dTok.ProviderID = sTok.ProviderID
			}
		}
	}

	if srcPart.Reasoning != nil && dstPart.Reasoning != nil &&
		srcPart.Reasoning.Token != nil && dstPart.Reasoning.Token != nil {
		sTok := srcPart.Reasoning.Token
		dTok := dstPart.Reasoning.Token
		if sTok.Data != "" && sTok.Data == dTok.Data {
			switch {
			case sTok.Scheme == "" && sTok.ProviderID == 0 && dTok.Scheme != "":
				sTok.Scheme = dTok.Scheme
				sTok.ProviderID = dTok.ProviderID
				if len(srcPart.Reasoning.Segments) == 0 && len(dstPart.Reasoning.Segments) == 0 {
					srcPart.Reasoning.Redacted = dstPart.Reasoning.Redacted
				}
			case dTok.Scheme == "" && dTok.ProviderID == 0 && sTok.Scheme != "":
				dTok.Scheme = sTok.Scheme
				dTok.ProviderID = sTok.ProviderID
				if len(srcPart.Reasoning.Segments) == 0 && len(dstPart.Reasoning.Segments) == 0 {
					dstPart.Reasoning.Redacted = srcPart.Reasoning.Redacted
				}
			}
		}
	}

	if srcPart.Call != nil && dstPart.Call != nil {
		if srcPart.Call.Token == nil && dstPart.Call.Token != nil &&
			dstPart.Call.Token.Data == geminiPlaceholderThoughtSignature {
			// Cross-protocol Gemini requests inject Dialect.PlaceholderToken onto
			// step-initial unsigned calls that carried no source wire signature.
			dstPart.Call.Token = nil
		} else if srcPart.Call.Token != nil && dstPart.Call.Token != nil {
			sTok := srcPart.Call.Token
			dTok := dstPart.Call.Token
			if sTok.Data != "" && sTok.Data == dTok.Data {
				switch {
				case sTok.Scheme == "" && sTok.ProviderID == 0 && dTok.Scheme != "":
					sTok.Scheme = dTok.Scheme
					sTok.ProviderID = dTok.ProviderID
				case dTok.Scheme == "" && dTok.ProviderID == 0 && sTok.Scheme != "":
					dTok.Scheme = sTok.Scheme
					dTok.ProviderID = sTok.ProviderID
				}
			}
		}
	}
}

// findNthPartOfKind returns a pointer to the ordinal-th element of parts with
// Kind matching wantKind. It returns nil when fewer matching parts exist.
func findNthPartOfKind(parts []ir.EssencePart, wantKind ir.PartKind, ordinal int) *ir.EssencePart {
	seen := 0
	for i := range parts {
		if parts[i].Kind == wantKind {
			if seen == ordinal {
				return &parts[i]
			}
			seen++
		}
	}
	return nil
}

// rewriteCallIDInMaterialized replaces correlated Result CallIDs following the
// call at (startMsg, startPart) until another call reuses oldID. For inlined
// history it rewrites the header field alone, retaining result text even when
// that text also contains the old identifier.
func rewriteCallIDInMaterialized(msgs []materializedMsg, startMsg, startPart int, oldID, newID string) {
	if oldID == "" || startMsg < 0 {
		return
	}
	for mIdx := startMsg; mIdx < len(msgs); mIdx++ {
		pStart := 0
		if mIdx == startMsg {
			pStart = startPart + 1
		}
		for pIdx := pStart; pIdx < len(msgs[mIdx].parts); pIdx++ {
			slot := &msgs[mIdx].parts[pIdx]
			p := &slot.part
			if slot.historyCallID == oldID {
				if slot.historyKind == ir.PartToolCall {
					return
				}
				prefix := "(tool_result) tool_use_id="
				p.Text = prefix + newID + strings.TrimPrefix(p.Text, prefix+oldID)
			}
			if p.Call != nil && p.Call.CallID == oldID {
				return
			}
			if p.Result != nil && p.Result.CallID == oldID {
				p.Result.CallID = newID
			}
		}
	}
}

// rewriteCallIDAcrossDoc replaces matching tool CallIDs and correlated Result
// CallIDs from startMsg onward until another call reuses oldID.
func rewriteCallIDAcrossDoc(doc *ir.EssenceDoc, startMsg int, oldID, newID string) {
	if oldID == "" {
		return
	}
	if startMsg < 0 {
		for pIdx := range doc.Parts {
			if doc.Parts[pIdx].Call != nil && doc.Parts[pIdx].Call.CallID == oldID {
				doc.Parts[pIdx].Call.CallID = newID
			}
		}
		return
	}
	sawTargetCall := false
	for mIdx := startMsg; mIdx < len(doc.Messages); mIdx++ {
		for pIdx := range doc.Messages[mIdx].Parts {
			p := &doc.Messages[mIdx].Parts[pIdx]
			if p.Call != nil && p.Call.CallID == oldID {
				if sawTargetCall {
					return
				}
				sawTargetCall = true
				p.Call.CallID = newID
				continue
			}
			if p.Result != nil && p.Result.CallID == oldID {
				p.Result.CallID = newID
			}
		}
	}
}

// stripRelocatedMediaAttributionOnDst removes synthetic "[Tool output media...]"
// attribution text parts adjacent to relocated media hashes on dst user turns
// and strips relocatedToolMediaMarker from correlated dst RoleTool results.
func stripRelocatedMediaAttributionOnDst(dst *ir.EssenceDoc, moved []ir.EssencePart, relocatedCallIDs []string) {
	hashes := make(map[string]bool, len(moved))
	for _, m := range moved {
		if m.Media != nil && m.Media.ContentHash != "" {
			hashes[m.Media.ContentHash] = true
		}
	}
	callIDSet := make(map[string]bool, len(relocatedCallIDs))
	for _, cid := range relocatedCallIDs {
		callIDSet[cid] = true
	}
	for mIdx := range dst.Messages {
		if dst.Messages[mIdx].Role == ir.RoleTool && len(callIDSet) > 0 {
			for pIdx := range dst.Messages[mIdx].Parts {
				res := dst.Messages[mIdx].Parts[pIdx].Result
				if res != nil && callIDSet[res.CallID] {
					stripRelocatedMediaMarkerOnResult(res)
				}
			}
			continue
		}
		if dst.Messages[mIdx].Role != ir.RoleUser {
			continue
		}
		hasMatchingMedia := false
		for _, p := range dst.Messages[mIdx].Parts {
			if p.Kind == ir.PartMedia && p.Media != nil && hashes[p.Media.ContentHash] {
				hasMatchingMedia = true
				break
			}
		}
		if !hasMatchingMedia {
			continue
		}
		filtered := make([]ir.EssencePart, 0, len(dst.Messages[mIdx].Parts))
		for _, p := range dst.Messages[mIdx].Parts {
			if p.Kind == ir.PartText && strings.Contains(strings.ToLower(p.Text), "tool") && strings.Contains(p.Text, "[") {
				continue
			}
			filtered = append(filtered, p)
		}
		dst.Messages[mIdx].Parts = filtered
	}
}

// stripRelocatedMediaMarkerOnResult removes standalone or newline-appended
// relocatedToolMediaMarker strings from res.Content so relocated tool results
// reconcile against source ToolResult text parts.
func stripRelocatedMediaMarkerOnResult(res *ir.EssenceToolResult) {
	if res == nil || len(res.Content) == 0 {
		return
	}
	suffix := "\n" + relocatedToolMediaMarker
	filtered := make([]ir.EssencePart, 0, len(res.Content))
	for _, p := range res.Content {
		if p.Kind == ir.PartText {
			if p.Text == relocatedToolMediaMarker {
				continue
			}
			if after, ok := strings.CutSuffix(p.Text, suffix); ok {
				p.Text = after
			}
		}
		filtered = append(filtered, p)
	}
	if len(filtered) == 0 {
		res.Content = nil
	} else {
		res.Content = filtered
	}
}

// hasSurvivingCallID reports whether td still contains at least one non-dropped
// tool_call or tool_result carrying callID. Blank callID strings return false,
// while surviving matches prevent removeSyntheticCallIDFromDoc from deleting
// paired calls that shared an identifier with a dropped orphan.
func hasSurvivingCallID(td *trackedDoc, callID string) bool {
	if callID == "" {
		return false
	}
	for mIdx := range td.messages {
		for pIdx := range td.messages[mIdx].parts {
			tp := &td.messages[mIdx].parts[pIdx]
			if tp.dropped {
				continue
			}
			if tp.part.Call != nil && tp.part.Call.CallID == callID {
				return true
			}
			if tp.part.Result != nil && tp.part.Result.CallID == callID {
				return true
			}
		}
	}
	return false
}

// removeSyntheticCallIDFromDoc removes repaired synthetic tool_call/tool_result
// entries matching callID when an encoder repaired pairing via stub synthesis.
func removeSyntheticCallIDFromDoc(doc *ir.EssenceDoc, callID string) {
	for mIdx := range doc.Messages {
		filtered := make([]ir.EssencePart, 0, len(doc.Messages[mIdx].Parts))
		for _, p := range doc.Messages[mIdx].Parts {
			if p.Call != nil && p.Call.CallID == callID {
				continue
			}
			if p.Result != nil && p.Result.CallID == callID {
				continue
			}
			filtered = append(filtered, p)
		}
		doc.Messages[mIdx].Parts = filtered
	}
}

// reconcileRootResponseParts aligns surviving response rootParts against
// dstParts for cross-protocol inline image Markdown rendering, text-attached
// replay signatures, and Chat text coalescing. Missing Markdown image spans or
// mismatched MIME or SHA-256 base64 digests return a precondition error.
//
// When dstParts carries no text-attached tokens, signed source text parts split
// into visible text followed by a token-only reasoning block matching convert's
// cross-protocol carrier layout. Adjacent metadata-free text parts then coalesce
// when the target protocol concatenated them into a single content string.
func reconcileRootResponseParts(rootParts []trackedPart, dstParts []ir.EssencePart) ([]trackedPart, error) {
	var mediaSlots []int
	for i := range rootParts {
		if rootParts[i].inlinedMedia != nil {
			mediaSlots = append(mediaSlots, i)
		}
	}
	if len(mediaSlots) > 0 {
		spans := extractMarkdownDataURLSpans(dstParts)
		if len(spans) < len(mediaSlots) {
			return nil, fmt.Errorf("expected %d markdown_data_url image span(s) in target response, got %d", len(mediaSlots), len(spans))
		}
		for k, slotIdx := range mediaSlots {
			want := rootParts[slotIdx].inlinedMedia
			got := spans[k]
			if strings.ToLower(strings.TrimSpace(got.mime)) != want.MIME {
				return nil, fmt.Errorf("markdown_data_url[%d] MIME = %q, want %q", k, got.mime, want.MIME)
			}
			sum := sha256.Sum256([]byte(got.b64))
			gotHash := hex.EncodeToString(sum[:])
			if gotHash != want.ContentHash {
				return nil, fmt.Errorf("markdown_data_url[%d] payload sha256 = %s, want %s", k, gotHash, want.ContentHash)
			}
			rootParts[slotIdx].part.Text = got.raw
		}
	}

	dstHasTextToken := false
	for _, dp := range dstParts {
		if dp.Kind == ir.PartText && dp.Token != nil {
			dstHasTextToken = true
			break
		}
	}
	if !dstHasTextToken {
		expanded := make([]trackedPart, 0, len(rootParts))
		reasoningOrd := 0
		for _, tp := range rootParts {
			if tp.part.Kind == ir.PartReasoning {
				reasoningOrd++
				expanded = append(expanded, tp)
				continue
			}
			if tp.part.Kind == ir.PartText && tp.part.Token != nil {
				tok := tp.part.Token
				tp.part.Token = nil
				keptText := tp.part.ID != "" || tp.part.Text != "" || len(tp.part.Annotations) > 0 || tp.part.CacheTTL != "" || tp.part.HasExtra
				if keptText {
					expanded = append(expanded, tp)
				}
				dReasoning := findNthPartOfKind(dstParts, ir.PartReasoning, reasoningOrd)
				synth := trackedPart{
					part: ir.EssencePart{
						Kind: ir.PartReasoning,
						Reasoning: &ir.EssenceReasoning{
							Token: tok,
						},
					},
					partIDRewritten: (!keptText && tp.partIDRewritten) || (dReasoning != nil && dReasoning.ID != ""),
				}
				reasoningOrd++
				expanded = append(expanded, synth)
				continue
			}
			expanded = append(expanded, tp)
		}
		rootParts = expanded
	}

	wantTextCount := countPartsOfKind(dstParts, ir.PartText)
	if countTrackedPartsOfKind(rootParts, ir.PartText) > wantTextCount {
		filtered := make([]trackedPart, 0, len(rootParts))
		for i, tp := range rootParts {
			if tp.part.Kind == ir.PartText && tp.textTokenDropped &&
				tp.part.ID == "" && tp.part.Text == "" && len(tp.part.Annotations) == 0 &&
				tp.part.CacheTTL == "" && !tp.part.HasExtra &&
				countTrackedPartsOfKind(filtered, ir.PartText)+countTrackedPartsOfKind(rootParts[i+1:], ir.PartText) >= wantTextCount {
				continue
			}
			filtered = append(filtered, tp)
		}
		rootParts = filtered
	}
	if countTrackedPartsOfKind(rootParts, ir.PartText) > wantTextCount && wantTextCount > 0 {
		coalesced := make([]trackedPart, 0, len(rootParts))
		for i, tp := range rootParts {
			if tp.part.Kind == ir.PartText &&
				len(coalesced) > 0 &&
				coalesced[len(coalesced)-1].part.Kind == ir.PartText &&
				countTrackedPartsOfKind(coalesced, ir.PartText)+countTrackedPartsOfKind(rootParts[i:], ir.PartText) > wantTextCount {
				prev := &coalesced[len(coalesced)-1]
				if (prev.part.ID == "" || prev.partIDRewritten) &&
					(tp.part.ID == "" || tp.partIDRewritten) &&
					len(prev.part.Annotations) == 0 && len(tp.part.Annotations) == 0 &&
					prev.part.Token == nil && tp.part.Token == nil &&
					prev.part.CacheTTL == "" && tp.part.CacheTTL == "" &&
					!prev.part.HasExtra && !tp.part.HasExtra {
					prev.part.Text += tp.part.Text
					prev.partIDRewritten = prev.partIDRewritten || tp.partIDRewritten
					continue
				}
			}
			coalesced = append(coalesced, tp)
		}
		rootParts = coalesced
	}
	return rootParts, nil
}

// extractMarkdownDataURLSpans scans PartText entries in parts in appearance
// order and returns every well-formed ![image](data:<mime>;base64,<b64>) span.
// Non-text parts and incomplete data URL prefixes are skipped without failing.
func extractMarkdownDataURLSpans(parts []ir.EssencePart) []markdownDataURLSpan {
	const prefix = "![image](data:"
	const mid = ";base64,"
	var out []markdownDataURLSpan
	for _, p := range parts {
		if p.Kind != ir.PartText || p.Text == "" {
			continue
		}
		rem := p.Text
		for {
			idx := strings.Index(rem, prefix)
			if idx < 0 {
				break
			}
			afterPrefix := rem[idx+len(prefix):]
			midIdx := strings.Index(afterPrefix, mid)
			if midIdx <= 0 {
				rem = afterPrefix
				continue
			}
			mime := afterPrefix[:midIdx]
			afterMid := afterPrefix[midIdx+len(mid):]
			closeIdx := strings.IndexByte(afterMid, ')')
			if closeIdx <= 0 {
				rem = afterMid
				continue
			}
			b64 := afterMid[:closeIdx]
			fullLen := len(prefix) + midIdx + len(mid) + closeIdx + 1
			out = append(out, markdownDataURLSpan{
				raw:  rem[idx : idx+fullLen],
				mime: mime,
				b64:  b64,
			})
			rem = afterMid[closeIdx+1:]
		}
	}
	return out
}

// countPartsOfKind returns the number of EssencePart elements in parts whose Kind equals wantKind.
func countPartsOfKind(parts []ir.EssencePart, wantKind ir.PartKind) int {
	n := 0
	for _, p := range parts {
		if p.Kind == wantKind {
			n++
		}
	}
	return n
}

// countTrackedPartsOfKind returns the number of trackedPart elements in parts whose Kind equals wantKind.
func countTrackedPartsOfKind(parts []trackedPart, wantKind ir.PartKind) int {
	n := 0
	for _, tp := range parts {
		if tp.part.Kind == wantKind {
			n++
		}
	}
	return n
}

// compactPlainParts extracts surviving non-dropped EssencePart values from slots.
func compactPlainParts(slots []trackedPart) []ir.EssencePart {
	return unwrapTrackedParts(compactTrackedParts(slots))
}

// compactTrackedParts filters slots down to non-dropped trackedPart entries.
func compactTrackedParts(slots []trackedPart) []trackedPart {
	if len(slots) == 0 {
		return nil
	}
	out := make([]trackedPart, 0, len(slots))
	for _, tp := range slots {
		if !tp.dropped {
			out = append(out, tp)
		}
	}
	return out
}

// unwrapTrackedParts extracts the plain ir.EssencePart slice from slots.
func unwrapTrackedParts(slots []trackedPart) []ir.EssencePart {
	if len(slots) == 0 {
		return nil
	}
	out := make([]ir.EssencePart, len(slots))
	for i, tp := range slots {
		out[i] = tp.part
	}
	return out
}

// sortPartsDeterministically sorts parts by serialized JSON representation so
// multiset-equal part slices compare identical while any missing/altered part still fails.
func sortPartsDeterministically(parts []ir.EssencePart) {
	slices.SortFunc(parts, func(a, b ir.EssencePart) int {
		ab, _ := json.Marshal(a, json.Deterministic(true))
		bb, _ := json.Marshal(b, json.Deterministic(true))
		return strings.Compare(string(ab), string(bb))
	})
}

// trimTrailingFieldSteps strips optional terminal field names such as ".token"
// or ".input_schema". Callers can therefore report either element paths or leaf
// field paths interchangeably.
func trimTrailingFieldSteps(steps []irPathStep, names ...string) []irPathStep {
	for len(steps) > 0 && !steps[len(steps)-1].hasIndex && slices.Contains(names, steps[len(steps)-1].field) {
		steps = steps[:len(steps)-1]
	}
	return steps
}

// cleanupEmptyCollections removes messages whose Parts slice became empty and
// normalizes zero-length top-level slices to nil.
func cleanupEmptyCollections(doc *ir.EssenceDoc) {
	if len(doc.System) == 0 {
		doc.System = nil
	}
	if len(doc.Tools) == 0 {
		doc.Tools = nil
	}
	if len(doc.Parts) == 0 {
		doc.Parts = nil
	}
	kept := make([]ir.EssenceMessage, 0, len(doc.Messages))
	for _, m := range doc.Messages {
		if len(m.Parts) == 0 {
			continue
		}
		kept = append(kept, m)
	}
	if len(kept) == 0 {
		doc.Messages = nil
	} else {
		doc.Messages = kept
	}
}
