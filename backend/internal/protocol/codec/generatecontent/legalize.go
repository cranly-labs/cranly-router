package generatecontent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// legalize.go normalizes IR system instructions and multi-turn conversations into
// strictly valid Google Gemini GenerateContent contents arrays while recording
// original-coordinate ir.Loss entries for every relocated, repaired, lowered,
// dropped, inlined, or reordered element.
//
// System and developer messages fold into systemInstruction with
// LossSystemRelocated, adjacent assistant turns merge before pairing, and
// unanswered tool_call items or orphan tool_result items prune with
// LossToolPairingRepaired. Paired functionResponse parts reorder into call
// invocation order within contiguous runs (recording LossPartsReordered when
// changed) and relocate nested media into sibling user parts (recording
// LossMediaRelocated). Unsigned historical reasoning drops on standard Gemini or
// downgrades to visible text while stripping thinkingConfig under the
// antigravity_claude replay scheme, and step-initial unsigned functionCall parts
// receive Dialect.PlaceholderToken when required (4b05d79a5).

const (
	// customToolWrapperSchemaJSON defines the {"input": "..."} wrapper schema
	// used when a standalone Gemini request encodes an unlowered custom tool.
	customToolWrapperSchemaJSON = `{"type":"object","properties":{"input":{"type":"string","description":"The raw input for this tool, passed through verbatim."}},"required":["input"]}`

	// clientSearchProxyName and clientSearchProxySchemaJSON define the function
	// proxy used when a standalone Gemini request encodes an unlowered tool_search tool.
	clientSearchProxyName       = "tool_search"
	clientSearchProxySchemaJSON = `{"type":"object","properties":{"query":{"type":"string","description":"Search query for tools or connectors to load."},"limit":{"type":"integer","description":"Maximum number of tool groups to return."}},"required":["query"]}`

	// defaultGeminiToolNameMaxBytes bounds Gemini FunctionDeclaration names to
	// Google's 64-byte identifier limit when Dialect leaves ToolNameMaxBytes unset.
	defaultGeminiToolNameMaxBytes = 64
)

// geminiIdentPattern matches legal Gemini FunctionDeclaration names
// (ASCII letter or underscore start followed by up to 63 identifier bytes).
var geminiIdentPattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.:-]*$`)

// trackedReqPart pairs one source ir.Part with its original IR path coordinate.
type trackedReqPart struct {
	part      ir.Part // source IR part value
	path      string  // original IR coordinate such as "messages[2].parts[1]"
	origOrder int     // zero-based sequence index within the turn
}

// trackedCallCandidate holds a validated assistant functionCall awaiting pairing.
type trackedCallCandidate struct {
	callID           string                    // non-empty correlation identifier
	wireID           string                    // wire "id" written on functionCall ("" for gemcall_)
	name             string                    // sanitized Gemini FunctionDeclaration name
	lowered          bool                      // whether resolveGeminiCallableName recorded LossToolLowered
	args             jsontext.Value            // validated JSON object bytes
	signature        string                    // explicit ReplayToken data when signed
	unsigned         bool                      // whether CallMemory confirmed an unsigned call for step-tail preservation
	partUnknown      map[string]jsontext.Value // preserved outer Part.Extra members
	callUnknown      map[string]jsontext.Value // preserved nested functionCall Extra members
	path             string                    // original IR part coordinate
	origOrder        int                       // zero-based sequence index within the turn
	invocationSerial int                       // monotonic call serial across the conversation
}

// trackedResultSlot holds one split tool_result item during pairing resolution.
type trackedResultSlot struct {
	partPath    string                    // original IR path of the PartToolResult
	msgPath     string                    // original IR path of owning RoleTool message
	stageIdx    int                       // index inside stages slice
	msgUnknown  map[string]jsontext.Value // preserved Extra from owning RoleTool message
	part        ir.Part                   // source IR PartToolResult value
	result      ir.ToolResult             // cloned tool result with linked ToolRef
	wireID      string                    // wire "id" matched from paired call
	wireName    string                    // sanitized Gemini function name
	callLowered bool                      // whether paired call recorded LossToolLowered
	callSerial  int                       // paired call invocationSerial for ordering
	claimed     bool                      // true once paired to an assistant functionCall
	dropped     bool                      // true when invalidated by a pruned call
}

// stageTurn represents one intermediate user, assistant, or tool turn before pairing.
type stageTurn struct {
	role           ir.Role                   // RoleUser, RoleAssistant, or RoleTool
	firstMsgPath   string                    // original IR path of first merged message
	extraOwnerPath string                    // original IR path owning unknown when non-empty
	unknown        map[string]jsontext.Value // preserved same-origin Message.Extra members
	items          []stageAssistantItem      // ordered parts and call candidates on assistant turns
	seenCallIDs    map[string]bool           // deduplicates call IDs within one assistant turn
	userParts      []trackedReqPart          // ordered parts on user turns
	toolSlot       *trackedResultSlot        // single tool_result slot when role is RoleTool
	nextItemOrder  int                       // monotonic item ordinal within merged assistant turn
}

// stageAssistantItem holds either a non-call assistant part or a validated call candidate.
type stageAssistantItem struct {
	isCall bool                 // true when cand is active; false when part is active
	part   trackedReqPart       // non-call assistant part
	cand   trackedCallCandidate // validated assistant functionCall candidate
}

// legalizedConversation bundles the legalized system parts, contents turns, and
// whether antigravity_claude downgraded any unsigned historical reasoning block.
type legalizedConversation struct {
	systemParts      []wireOutPart    // outgoing systemInstruction.parts entries
	contents         []wireOutContent // alternating legalized user and model turns
	strippedThinking bool             // true when antigravity_claude inlined unsigned thinking
}

// stagedOutTurn holds outgoing Gemini parts before final adjacent same-role merging.
type stagedOutTurn struct {
	role         string                    // "user" or "model"
	irRole       ir.Role                   // originating IR Role (RoleUser, RoleTool, RoleAssistant)
	msgPath      string                    // original IR message coordinate
	extraMsgPath string                    // original IR message coordinate owning unknown
	parts        []wireOutPart             // non-empty outgoing Gemini Part entries
	unknown      map[string]jsontext.Value // preserved same-origin Content Extra members
}

// mergedContentTurn tracks an accumulated wireOutContent turn and which IR role
// runs contributed to it during coalesceStagedOutTurns.
type mergedContentTurn struct {
	wire         wireOutContent // accumulated Gemini user or model Content object
	sawUserRun   bool           // true after merging at least one RoleUser stage
	sawToolRun   bool           // true after merging at least one RoleTool stage
	sawModelRun  bool           // true after merging at least one RoleAssistant stage
	extraMsgPath string         // original IR message coordinate owning wire.Unknown
}

// legalizeRequestConversation converts req.System and req.Messages into Gemini
// systemInstruction parts and alternating user/model contents turns without
// mutating caller-owned slices. Empty message lists or conversations where no
// user or model parts survive legalization return a non-nil error.
//
// System and developer turns relocate into out.systemParts before turn staging,
// and adjacent assistant messages merge into one stageTurn so split call batches
// share a single pairing window against following RoleTool results. Out.strippedThinking
// reports whether an unsigned historical reasoning block downgraded to plain text
// under antigravity_claude so request encoding suppresses thinkingConfig.
func legalizeRequestConversation(
	req *ir.Request,
	dialect ir.Dialect,
	maxNameBytes int,
	losses *[]ir.Loss,
) (legalizedConversation, error) {
	var out legalizedConversation
	if len(req.Messages) == 0 {
		return out, errors.New("encode generatecontent request: messages must be non-empty")
	}

	callToolIndex := indexRequestCallRefs(req.Messages)

	for i, p := range req.System {
		sPath := fmt.Sprintf("system[%d]", i)
		if outPart, ok := encodeSystemOrUserPart(p, sPath, losses); ok {
			out.systemParts = append(out.systemParts, outPart)
		}
	}

	var stages []stageTurn
	invalidCallIDs := make(map[string]bool)
	nextCallSerial := 1

	for mIdx, msg := range req.Messages {
		mPath := fmt.Sprintf("messages[%d]", mIdx)
		if msg.ID != "" {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossIDRewritten,
				Path: mPath + ".id",
			})
		}

		if msg.Role == ir.RoleSystem || msg.Role == ir.RoleDeveloper {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossSystemRelocated,
				Path: mPath,
			})
			dropMessageExtraOnRelocate(msg.Extra, mPath, losses)
			for pIdx, p := range msg.Parts {
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				if outPart, ok := encodeRelocatedSystemPart(p, pPath, losses); ok {
					out.systemParts = append(out.systemParts, outPart)
				}
			}
			continue
		}

		msgUnknown := restoreOrDropExtra(msg.Extra, mPath, losses)

		if msg.Role == ir.RoleTool {
			for pIdx, p := range msg.Parts {
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				if p.Kind != ir.PartToolResult || p.Result == nil {
					continue
				}
				if p.ID != "" {
					*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
				}
				clonedPart := p.Clone()
				resCopy := *clonedPart.Result
				if resCopy.Tool == (ir.ToolRef{}) && resCopy.CallID != "" {
					if ref, ok := callToolIndex[resCopy.CallID]; ok {
						resCopy.Tool = ref
					}
				}
				var slotUnknown map[string]jsontext.Value
				if pIdx == 0 {
					slotUnknown = msgUnknown
				}
				slot := &trackedResultSlot{
					partPath:   pPath,
					msgPath:    mPath,
					stageIdx:   len(stages),
					msgUnknown: slotUnknown,
					part:       p,
					result:     resCopy,
				}
				stages = append(stages, stageTurn{
					role:         ir.RoleTool,
					firstMsgPath: mPath,
					toolSlot:     slot,
				})
			}
			continue
		}

		if msg.Role == ir.RoleAssistant {
			var target *stageTurn
			if len(stages) > 0 && stages[len(stages)-1].role == ir.RoleAssistant {
				target = &stages[len(stages)-1]
				dropExtraOnMergedAssistantStage(target, msgUnknown, mPath, losses)
			} else {
				var ownerPath string
				if len(msgUnknown) > 0 {
					ownerPath = mPath
				}
				stages = append(stages, stageTurn{
					role:           ir.RoleAssistant,
					firstMsgPath:   mPath,
					extraOwnerPath: ownerPath,
					unknown:        msgUnknown,
					seenCallIDs:    make(map[string]bool),
				})
				target = &stages[len(stages)-1]
			}

			for pIdx, p := range msg.Parts {
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				order := target.nextItemOrder
				target.nextItemOrder++
				if p.Kind == ir.PartToolCall && p.Call != nil {
					cand, keep, err := prepareCallCandidate(p, pPath, order, maxNameBytes, &nextCallSerial, target.seenCallIDs, invalidCallIDs, losses)
					if err != nil {
						return out, err
					}
					if keep {
						target.items = append(target.items, stageAssistantItem{
							isCall: true,
							cand:   cand,
						})
					}
					continue
				}
				target.items = append(target.items, stageAssistantItem{
					isCall: false,
					part: trackedReqPart{
						part:      p,
						path:      pPath,
						origOrder: order,
					},
				})
			}
			continue
		}

		// User turns accumulate parts in original appearance order.
		uParts := make([]trackedReqPart, len(msg.Parts))
		for pIdx, p := range msg.Parts {
			uParts[pIdx] = trackedReqPart{
				part:      p,
				path:      fmt.Sprintf("%s.parts[%d]", mPath, pIdx),
				origOrder: pIdx,
			}
		}
		stages = append(stages, stageTurn{
			role:         ir.RoleUser,
			firstMsgPath: mPath,
			unknown:      msgUnknown,
			userParts:    uParts,
		})
	}

	outContents, stripped, err := pairAndSerializeStages(stages, dialect, maxNameBytes, invalidCallIDs, losses)
	if err != nil {
		return out, err
	}
	if len(outContents) == 0 {
		return out, errors.New("encode generatecontent request: no conversation parts survived legalization")
	}
	out.contents = outContents
	out.strippedThinking = stripped
	return out, nil
}

// indexRequestCallRefs builds a read-only CallID-to-ToolRef map mimicking ir.Link
// without mutating the caller's ir.Request struct.
func indexRequestCallRefs(msgs []ir.Message) map[string]ir.ToolRef {
	seen := make(map[string]ir.ToolRef)
	for mIdx := range msgs {
		for pIdx := range msgs[mIdx].Parts {
			p := &msgs[mIdx].Parts[pIdx]
			if p.Kind == ir.PartToolCall && p.Call != nil && p.Call.CallID != "" {
				if p.Call.Tool != (ir.ToolRef{}) {
					seen[p.Call.CallID] = p.Call.Tool
				}
			}
		}
	}
	return seen
}

// dropMessageExtraOnRelocate appends LossExtraDropped at mPath when a relocated
// system or developer message carried non-zero Extra metadata. Messages with
// zero Extra values leave losses untouched without allocating.
func dropMessageExtraOnRelocate(extra ir.Extra, mPath string, losses *[]ir.Loss) {
	if !extra.IsZero() {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossExtraDropped,
			Path: mPath,
		})
	}
}

// dropExtraOnMergedAssistantStage clears Message.Extra on both merged
// RoleAssistant turns and appends LossExtraDropped for each side carrying Extra
// members. Clearing both sides keeps turn coalescing aligned with VerifyEssence.
func dropExtraOnMergedAssistantStage(
	target *stageTurn,
	msgUnknown map[string]jsontext.Value,
	mPath string,
	losses *[]ir.Loss,
) {
	if len(target.unknown) > 0 {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossExtraDropped,
			Path: target.extraOwnerPath,
		})
		target.unknown = nil
		target.extraOwnerPath = ""
	}
	if len(msgUnknown) > 0 {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossExtraDropped,
			Path: mPath,
		})
	}
}

// prepareCallCandidate validates and lowers an assistant PartToolCall before
// pairing against following RoleTool slots. Hosted tool calls drop as opaque
// items while malformed JSON arguments prune with LossToolPairingRepaired.
func prepareCallCandidate(
	p ir.Part,
	pPath string,
	order int,
	maxNameBytes int,
	nextSerial *int,
	seenInTurn map[string]bool,
	invalidCallIDs map[string]bool,
	losses *[]ir.Loss,
) (trackedCallCandidate, bool, error) {
	call := p.Call
	if call.Tool.Kind == ir.ToolHosted {
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOpaqueItemDropped,
			Path:   pPath,
			Detail: call.Tool.Name,
		})
		if call.CallID != "" {
			invalidCallIDs[call.CallID] = true
		}
		return trackedCallCandidate{}, false, nil
	}

	if call.CallID != "" && seenInTurn[call.CallID] {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolPairingRepaired,
			Path:   pPath,
			Detail: "duplicate_call_id_in_turn",
		})
		return trackedCallCandidate{}, false, nil
	}

	argsBytes, ok, err := buildGeminiCallArgs(*call)
	if err != nil {
		return trackedCallCandidate{}, false, err
	}
	if !ok {
		if call.CallID != "" {
			invalidCallIDs[call.CallID] = true
		}
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolPairingRepaired,
			Path:   pPath,
			Detail: "invalid_tool_arguments",
		})
		return trackedCallCandidate{}, false, nil
	}

	if p.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
	}
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
	}

	serial := *nextSerial
	*nextSerial++

	callID := call.CallID
	wireID := callID
	if callID == "" {
		callID = fmt.Sprintf("call_%d", serial)
		wireID = callID
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossIDRewritten,
			Path: pPath + ".call_id",
		})
	} else if strings.HasPrefix(callID, namelessCallIDPrefix) {
		// Omit wire "id" on calls that originated from nameless Gemini wire
		// functionCall parts so same-protocol round trips stay byte-identical.
		wireID = ""
	}

	seenInTurn[callID] = true
	lossesBefore := len(*losses)
	fnName := resolveGeminiCallableName(call.Tool, maxNameBytes, pPath, losses)
	wasLowered := len(*losses) > lossesBefore

	sig := rawReplayTokenData(call.Token)

	partUnknown := restoreOrDropExtra(p.Extra, pPath, losses)
	callUnknown, _ := extractSubObjectUnknown(partUnknown, "functionCall", "id", "name", "args")
	delete(partUnknown, "functionCall")
	stripModeledPartKeys(partUnknown)

	return trackedCallCandidate{
		callID:           callID,
		wireID:           wireID,
		name:             fnName,
		lowered:          wasLowered,
		args:             argsBytes,
		signature:        sig,
		unsigned:         call.Unsigned,
		partUnknown:      partUnknown,
		callUnknown:      callUnknown,
		path:             pPath,
		origOrder:        order,
		invocationSerial: serial,
	}, true, nil
}

// buildGeminiCallArgs normalizes call.Arguments into a single JSON object and
// wraps custom free-form strings inside `{"input":"..."}`. Blank arguments
// default to `"{}"`, whereas non-object JSON strings return `(nil, false, nil)`
// so callers can prune malformed historical calls or substitute empty objects.
func buildGeminiCallArgs(call ir.ToolCall) (jsontext.Value, bool, error) {
	if call.Tool.Kind == ir.ToolCustom {
		raw, err := marshalDeterministic(map[string]string{"input": call.Arguments})
		if err != nil {
			return nil, false, err
		}
		return jsontext.Value(raw), true, nil
	}
	trimmed := strings.TrimSpace(call.Arguments)
	if trimmed == "" {
		return jsontext.Value("{}"), true, nil
	}
	if !isValidSingleJSONObject([]byte(trimmed)) {
		return nil, false, nil
	}
	return compactRawJSON(jsontext.Value(trimmed)), true, nil
}

// rawReplayTokenData returns tok.Data verbatim when tok is non-nil without
// inspecting scheme tags or carrier prefixes, keeping token selection inside convert.
func rawReplayTokenData(tok *ir.ReplayToken) string {
	if tok == nil {
		return ""
	}
	return tok.Data
}

// pairAndSerializeStages pairs assistant functionCall candidates against the
// immediately following window of RoleTool slots, stages surviving parts,
// and coalesces adjacent same-role Gemini turns. It returns the legalized
// wireOutContent slice and whether any unsigned reasoning block inlined under
// antigravity_claude.
//
// Unanswered calls and unclaimed or invalidated tool results prune with
// LossToolPairingRepaired instead of emitting broken pairs that Gemini rejects.
// Nameless gemcall_ calls recheck their post-merge step and call ordinals after
// pruning and record LossIDRewritten whenever re-decoding would derive a
// different deterministic CallID.
func pairAndSerializeStages(
	stages []stageTurn,
	dialect ir.Dialect,
	maxNameBytes int,
	invalidCallIDs map[string]bool,
	losses *[]ir.Loss,
) ([]wireOutContent, bool, error) {
	var staged []stagedOutTurn
	strippedThinking := false
	finalModelStepOrd := 0
	finalModelCallCount := 0

	i := 0
	for i < len(stages) {
		st := &stages[i]
		switch st.role {
		case ir.RoleAssistant:
			var resultWindow []*trackedResultSlot
			scan := i + 1
			for scan < len(stages) && stages[scan].role != ir.RoleAssistant {
				if stages[scan].role == ir.RoleTool && stages[scan].toolSlot != nil {
					resultWindow = append(resultWindow, stages[scan].toolSlot)
				}
				scan++
			}

			mergesIntoPrevModel := len(staged) > 0 && staged[len(staged)-1].role == geminiRoleModel
			effectiveStepOrd := finalModelStepOrd + 1
			effectiveCallBase := 0
			if mergesIntoPrevModel {
				effectiveStepOrd = finalModelStepOrd
				effectiveCallBase = finalModelCallCount
			}

			matchedCalls := make(map[int]bool)
			survivingCallCount := 0
			for idx := range st.items {
				if !st.items[idx].isCall {
					continue
				}
				cand := &st.items[idx].cand
				var matchedSlot *trackedResultSlot
				for _, slot := range resultWindow {
					if !slot.claimed && !slot.dropped && slot.result.CallID != "" && slot.result.CallID == cand.callID {
						matchedSlot = slot
						break
					}
				}
				if matchedSlot == nil {
					*losses = append(*losses, ir.Loss{
						Kind:   ir.LossToolPairingRepaired,
						Path:   cand.path,
						Detail: "unanswered_tool_call",
					})
					continue
				}
				matchedSlot.claimed = true
				matchedSlot.wireID = cand.wireID
				matchedSlot.wireName = cand.name
				matchedSlot.callLowered = cand.lowered
				matchedSlot.callSerial = cand.invocationSerial
				matchedCalls[idx] = true

				// When wire "id" stays omitted on a nameless gemcall_ invocation,
				// check whether step merging, call pruning, or name sanitization
				// changes the deterministic CallID derived on re-decoding.
				if cand.wireID == "" {
					redecodedID := deriveNamelessCallID(
						effectiveStepOrd,
						effectiveCallBase+survivingCallCount,
						cand.name,
						string(cand.args),
					)
					if redecodedID != cand.callID {
						*losses = append(*losses, ir.Loss{
							Kind: ir.LossIDRewritten,
							Path: cand.path + ".call_id",
						})
					}
				}
				survivingCallCount++
			}

			modelParts, turnStripped := serializeAssistantTurnItems(st.items, matchedCalls, dialect, losses)
			if turnStripped {
				strippedThinking = true
			}
			if len(modelParts) > 0 {
				if mergesIntoPrevModel {
					finalModelCallCount += survivingCallCount
				} else {
					finalModelStepOrd++
					finalModelCallCount = survivingCallCount
				}
				cleanedUnknown := stripModeledContentKeys(st.unknown)
				var ownerPath string
				if len(cleanedUnknown) > 0 {
					ownerPath = st.extraOwnerPath
					if ownerPath == "" {
						ownerPath = st.firstMsgPath
					}
				}
				staged = append(staged, stagedOutTurn{
					role:         geminiRoleModel,
					irRole:       ir.RoleAssistant,
					msgPath:      st.firstMsgPath,
					extraMsgPath: ownerPath,
					parts:        modelParts,
					unknown:      cleanedUnknown,
				})
			} else if len(st.unknown) > 0 {
				ownerPath := st.extraOwnerPath
				if ownerPath == "" {
					ownerPath = st.firstMsgPath
				}
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossExtraDropped,
					Path: ownerPath,
				})
			}
			i++

		case ir.RoleTool:
			// Process a contiguous run of RoleTool stages together so paired
			// functionResponse parts reorder into call invocation order when needed.
			runEnd := i
			var runSlots []*trackedResultSlot
			for runEnd < len(stages) && stages[runEnd].role == ir.RoleTool {
				if stages[runEnd].toolSlot != nil {
					runSlots = append(runSlots, stages[runEnd].toolSlot)
				}
				runEnd++
			}
			userTurns, err := serializeToolResultRun(runSlots, dialect, maxNameBytes, invalidCallIDs, losses)
			if err != nil {
				return nil, false, err
			}
			staged = append(staged, userTurns...)
			i = runEnd

		case ir.RoleUser:
			uParts := make([]wireOutPart, 0, len(st.userParts))
			for _, tp := range st.userParts {
				if outPart, ok := encodeSystemOrUserPart(tp.part, tp.path, losses); ok {
					uParts = append(uParts, outPart)
				}
			}
			if len(uParts) > 0 {
				cleanedUnknown := stripModeledContentKeys(st.unknown)
				var ownerPath string
				if len(cleanedUnknown) > 0 {
					ownerPath = st.firstMsgPath
				}
				staged = append(staged, stagedOutTurn{
					role:         geminiRoleUser,
					irRole:       ir.RoleUser,
					msgPath:      st.firstMsgPath,
					extraMsgPath: ownerPath,
					parts:        uParts,
					unknown:      cleanedUnknown,
				})
			} else if len(st.unknown) > 0 {
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossExtraDropped,
					Path: st.firstMsgPath,
				})
			}
			i++
		}
	}

	// Coalesce adjacent same-role turns after serialization because dropping
	// empty assistant stages can leave two user turns adjacent, and same-IR-role
	// merges drop surviving Content.Extra maps with LossExtraDropped.
	merged := coalesceStagedOutTurns(staged, losses)
	return merged, strippedThinking, nil
}

// serializeAssistantTurnItems converts surviving assistant reasoning, text, media,
// opaque, and paired functionCall items into ordered Gemini model parts. It
// returns the outgoing part slice and whether antigravity_claude downgraded
// any unsigned historical reasoning block to plain text.
//
// Contiguous paired functionCall items form a single invocation step tracked by
// inFunctionStep, resetting whenever a reasoning, text, media, or opaque part
// intervenes. Unsigned calls receive dialect.PlaceholderToken on the first call
// of each step (even when Call.Unsigned is true) and on subsequent parallel
// calls whose Call.Unsigned flag is false, preserving known-unsigned parallel tails.
func serializeAssistantTurnItems(
	items []stageAssistantItem,
	matchedCalls map[int]bool,
	dialect ir.Dialect,
	losses *[]ir.Loss,
) ([]wireOutPart, bool) {
	parts := make([]wireOutPart, 0, len(items))
	strippedThinking := false
	inFunctionStep := false

	for idx, item := range items {
		if item.isCall {
			if !matchedCalls[idx] {
				continue
			}
			cand := item.cand
			sig := cand.signature
			// Inject Dialect.PlaceholderToken when unsigned unless inFunctionStep is
			// already active and CallMemory marked this parallel call Unsigned:
			// step-first calls always need a signature for strict Gemini models, and
			// unknown-unsigned parallel calls all receive the placeholder (4b05d79a5).
			if sig == "" && (!inFunctionStep || !cand.unsigned) && dialect.PlaceholderToken != "" {
				sig = dialect.PlaceholderToken
			}
			inFunctionStep = true

			fc := &wireFunctionCall{
				ID:      cand.wireID,
				Name:    cand.name,
				Args:    cand.args,
				Unknown: cand.callUnknown,
			}
			parts = append(parts, wireOutPart{
				part: wirePart{
					ThoughtSignature: sig,
					FunctionCall:     fc,
					Unknown:          cand.partUnknown,
				},
			})
			continue
		}

		inFunctionStep = false
		tp := item.part
		switch tp.part.Kind {
		case ir.PartReasoning:
			outPart, keep, inlined := encodeAssistantReasoningPart(tp.part, tp.path, dialect, losses)
			if inlined {
				strippedThinking = true
			}
			if keep {
				parts = append(parts, outPart)
			}

		default:
			if outPart, keep := encodeSystemOrUserPart(tp.part, tp.path, losses); keep {
				parts = append(parts, outPart)
			}
		}
	}
	return parts, strippedThinking
}

// encodeAssistantReasoningPart converts one historical assistant PartReasoning
// into a wireOutPart and reports (outPart, keep, inlined). Callers must
// suppress generationConfig.thinkingConfig for the whole request whenever
// inlined returns true.
//
// Any reasoning part carrying non-empty ReplayToken.Data emits thought:true with
// Token.Data written verbatim to thoughtSignature, clearing visible text and
// recording LossReasoningTextDropped when Redacted is also set on non-empty
// Segments. Tokenless reasoning parts downgrade to visible text (or drop when
// blank or Redacted) under ir.SchemeAntigravityClaude and drop with
// LossReasoningTextDropped on standard Gemini dialects.
func encodeAssistantReasoningPart(
	p ir.Part,
	pPath string,
	dialect ir.Dialect,
	losses *[]ir.Loss,
) (wireOutPart, bool, bool) {
	if p.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
	}
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
	}
	if p.Reasoning == nil {
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		return wireOutPart{}, false, false
	}

	joined := strings.Join(p.Reasoning.Segments, "")
	sig := rawReplayTokenData(p.Reasoning.Token)

	// Signed reasoning blocks (including redacted blocks carrying unpacked
	// antigravity_claude signatures or trc1 carriers) emit thought:true and
	// write Token.Data verbatim into thoughtSignature.
	if sig != "" {
		if p.Reasoning.Redacted && len(p.Reasoning.Segments) > 0 {
			*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
		}
		txt := joined
		if p.Reasoning.Redacted {
			txt = ""
		}
		unknown := restoreOrDropExtra(p.Extra, pPath, losses)
		stripModeledPartKeys(unknown)
		return wireOutPart{
			part: wirePart{
				Text:             &txt,
				Thought:          true,
				ThoughtSignature: sig,
				Unknown:          unknown,
			},
		}, true, false
	}

	// Unsigned historical thinking under antigravity_claude downgrades to plain
	// text and strips thinkingConfig for the entire outgoing request.
	if dialect.ReplayScheme == ir.SchemeAntigravityClaude {
		inlinedText := joined
		if p.Reasoning.Redacted {
			inlinedText = ""
		}
		unknown := restoreOrDropExtra(p.Extra, pPath, losses)
		stripModeledPartKeys(unknown)
		if strings.TrimSpace(inlinedText) == "" {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossReasoningTextDropped,
				Path: pPath,
			})
			return wireOutPart{}, false, true
		}
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossReasoningTextInlined,
			Path: pPath,
		})
		return wireOutPart{
			part: wirePart{
				Text:    &inlinedText,
				Unknown: unknown,
			},
		}, true, true
	}

	// Standard Gemini omits unsigned historical reasoning blocks so thinking
	// models never reject unsigned thought parts.
	_ = restoreOrDropExtra(p.Extra, pPath, losses)
	*losses = append(*losses, ir.Loss{
		Kind: ir.LossReasoningTextDropped,
		Path: pPath,
	})
	return wireOutPart{}, false, false
}

// serializeToolResultRun converts a contiguous slice of RoleTool stage slots
// into a single Gemini user stagedOutTurn containing paired functionResponse
// parts followed by any relocated media attachments. When every slot in slots
// is unclaimed or invalidated, it records LossToolPairingRepaired per orphan
// and returns (nil, nil).
//
// Surviving paired slots stably sort by their matched call's invocationSerial
// and record LossPartsReordered per reordered message when client tool results
// arrived out of call order. Nested PartMedia items inside ToolResult.Content
// append after all functionResponse parts in the same user turn with LossMediaRelocated.
func serializeToolResultRun(
	slots []*trackedResultSlot,
	dialect ir.Dialect,
	maxNameBytes int,
	invalidCallIDs map[string]bool,
	losses *[]ir.Loss,
) ([]stagedOutTurn, error) {
	var surviving []*trackedResultSlot
	var inheritedUnknown map[string]jsontext.Value
	var extraOwnerPath string
	var firstMsgPath string

	for _, slot := range slots {
		if !slot.claimed || slot.dropped || invalidCallIDs[slot.result.CallID] {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossToolPairingRepaired,
				Path:   slot.partPath,
				Detail: "orphan_tool_result",
			})
			if len(slot.msgUnknown) > 0 {
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossExtraDropped,
					Path: slot.msgPath,
				})
			}
			continue
		}
		if firstMsgPath == "" {
			firstMsgPath = slot.msgPath
		}
		if len(slot.msgUnknown) > 0 {
			// Multiple RoleTool messages coalescing into one run cannot keep
			// per-message Extra; single-message runs retain Extra when tool-only.
			if len(surviving) == 0 && len(inheritedUnknown) == 0 {
				inheritedUnknown = slot.msgUnknown
				extraOwnerPath = slot.msgPath
			} else {
				if len(inheritedUnknown) > 0 {
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossExtraDropped,
						Path: extraOwnerPath,
					})
					inheritedUnknown = nil
					extraOwnerPath = ""
				}
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossExtraDropped,
					Path: slot.msgPath,
				})
			}
		} else if len(surviving) > 0 && slot.msgPath != firstMsgPath && len(inheritedUnknown) > 0 {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossExtraDropped,
				Path: extraOwnerPath,
			})
			inheritedUnknown = nil
			extraOwnerPath = ""
		}
		surviving = append(surviving, slot)
	}

	if len(surviving) == 0 {
		return nil, nil
	}

	// Check whether paired results arrived out of assistant invocation order.
	reordered := false
	for idx := 1; idx < len(surviving); idx++ {
		if surviving[idx].callSerial < surviving[idx-1].callSerial {
			reordered = true
			break
		}
	}
	if reordered {
		slices.SortStableFunc(surviving, func(a, b *trackedResultSlot) int {
			return a.callSerial - b.callSerial
		})
		seenMsgPaths := make(map[string]bool)
		for _, slot := range surviving {
			if !seenMsgPaths[slot.msgPath] {
				seenMsgPaths[slot.msgPath] = true
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossPartsReordered,
					Path: slot.msgPath,
				})
			}
		}
	}

	respParts := make([]wireOutPart, 0, len(surviving))
	var relocatedMedia []wireOutPart

	for _, slot := range surviving {
		p := slot.part
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: slot.partPath})
		}
		if slot.callLowered || slot.result.Tool.Kind == ir.ToolCustom || slot.result.Tool.Kind == ir.ToolClientSearch || slot.result.Tool.Namespace != "" {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossToolLowered,
				Path:   slot.partPath,
				Detail: string(slot.result.Tool.Kind),
			})
		}
		name := slot.wireName
		if name == "" {
			name = sanitizeGeminiToolName(slot.result.Tool.Name, maxNameBytes)
			if name == "" {
				name = "tool"
			}
		}

		respObj, mediaParts, err := buildFunctionResponsePayload(slot.result, dialect, slot.partPath, losses)
		if err != nil {
			return nil, err
		}
		relocatedMedia = append(relocatedMedia, mediaParts...)

		partUnknown := restoreOrDropExtra(p.Extra, slot.partPath, losses)
		frUnknown, _ := extractSubObjectUnknown(partUnknown, "functionResponse", "id", "name", "response")
		delete(partUnknown, "functionResponse")
		var sig string
		if rawSig, ok := partUnknown["thoughtSignature"]; ok {
			if decodedSig, isStr := tryDecodeJSONString(rawSig); isStr {
				sig = decodedSig
			}
			delete(partUnknown, "thoughtSignature")
		}
		stripModeledPartKeys(partUnknown)

		respParts = append(respParts, wireOutPart{
			part: wirePart{
				ThoughtSignature: sig,
				FunctionResponse: &wireFunctionResponse{
					ID:       slot.wireID,
					Name:     name,
					Response: respObj,
					Unknown:  frUnknown,
				},
				Unknown: partUnknown,
			},
		})
	}

	cleanedInherited := stripModeledContentKeys(inheritedUnknown)
	if len(relocatedMedia) > 0 && len(cleanedInherited) > 0 {
		// Relocated media creates a trailing RoleUser run on re-decoding, which
		// would capture Content.Extra away from the RoleTool turn unless dropped.
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossExtraDropped,
			Path: extraOwnerPath,
		})
		cleanedInherited = nil
		extraOwnerPath = ""
	}
	parts := append(respParts, relocatedMedia...)
	irRole := ir.RoleTool
	return []stagedOutTurn{{
		role:         geminiRoleUser,
		irRole:       irRole,
		msgPath:      firstMsgPath,
		extraMsgPath: extraOwnerPath,
		parts:        parts,
		unknown:      cleanedInherited,
	}}, nil
}

// buildFunctionResponsePayload constructs the required JSON object for
// functionResponse.response and extracts nested media parts for sibling
// user-turn relocation with LossMediaRelocated. Non-error single-part JSON
// objects pass through directly unless they match reserved wrapper keys,
// whereas plain text wraps inside {"content":...} under standard Gemini or
// {"result":...} under SchemaAntigravity, and errors wrap inside {"error":...}.
func buildFunctionResponsePayload(
	res ir.ToolResult,
	dialect ir.Dialect,
	pPath string,
	losses *[]ir.Loss,
) (jsontext.Value, []wireOutPart, error) {
	var textPieces []string
	var relocatedMedia []wireOutPart
	recordedMediaLoss := false

	for cIdx, child := range res.Content {
		cPath := fmt.Sprintf("%s.result.content[%d]", pPath, cIdx)
		switch child.Kind {
		case ir.PartText:
			if child.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: cPath + ".id"})
			}
			if child.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: cPath})
			}
			_ = restoreOrDropExtra(child.Extra, cPath, losses)
			textPieces = append(textPieces, child.Text)

		case ir.PartMedia:
			if mediaVal, ok := encodeMediaPart(child, cPath, losses); ok {
				if !recordedMediaLoss {
					recordedMediaLoss = true
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossMediaRelocated,
						Path: pPath,
					})
				}
				relocatedMedia = append(relocatedMedia, mediaVal)
			}
		}
	}

	joined := strings.Join(textPieces, "\n")

	if res.IsError {
		rawObj, err := marshalDeterministic(map[string]string{"error": joined})
		if err != nil {
			return nil, nil, err
		}
		return jsontext.Value(rawObj), relocatedMedia, nil
	}

	// Preserve structured JSON object tool outputs directly unless they match the
	// reserved single-key {"error":"..."}, {"content":"..."}, or {"result":"..."} envelopes.
	trimmedJoined := strings.TrimSpace(joined)
	if len(textPieces) == 1 && isValidSingleJSONObject([]byte(trimmedJoined)) && !isReservedResponseEnvelope([]byte(trimmedJoined)) {
		return compactRawJSON(jsontext.Value(trimmedJoined)), relocatedMedia, nil
	}

	wrapKey := "content"
	if dialect.SchemaDialect == ir.SchemaAntigravity {
		wrapKey = "result"
	}
	rawObj, err := marshalDeterministic(map[string]string{wrapKey: joined})
	if err != nil {
		return nil, nil, err
	}
	return jsontext.Value(rawObj), relocatedMedia, nil
}

// isReservedResponseEnvelope reports whether objBytes is a 1-key {"error":string},
// 1-key {"content":string}, 1-key {"result":string}, or 2-key
// {"error":string,"is_error":true} object that unwrapFunctionResponseObject
// would unwrap on re-decoding.
func isReservedResponseEnvelope(objBytes []byte) bool {
	var m map[string]jsontext.Value
	if unmarshalJSONObject(objBytes, &m) != nil {
		return false
	}
	if len(m) == 1 {
		for _, reserved := range []string{"error", "content", "result"} {
			if v, ok := m[reserved]; ok {
				_, isStr := tryDecodeJSONString(v)
				return isStr
			}
		}
	}
	if len(m) == 2 {
		vErr, hasErr := m["error"]
		vFlag, hasFlag := m["is_error"]
		if hasErr && hasFlag && bytes.Equal(bytes.TrimSpace(vFlag), []byte("true")) {
			_, isStr := tryDecodeJSONString(vErr)
			return isStr
		}
	}
	return false
}

// encodeRelocatedSystemPart converts a Part from a relocated system or developer
// message into a systemInstruction part. Reasoning parts cannot appear inside
// systemInstruction, so they return keep==false and record LossReplayTokenDropped
// or LossReasoningTextDropped as applicable.
func encodeRelocatedSystemPart(p ir.Part, pPath string, losses *[]ir.Loss) (wireOutPart, bool) {
	switch p.Kind {
	case ir.PartReasoning:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		if p.Reasoning != nil {
			if p.Reasoning.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
			}
			if p.Reasoning.Token == nil || len(p.Reasoning.Segments) > 0 {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
			}
		}
		return wireOutPart{}, false
	default:
		return encodeSystemOrUserPart(p, pPath, losses)
	}
}

// encodeSystemOrUserPart converts a PartText, PartRefusal, PartMedia, or
// PartOpaque into a wireOutPart while recording incurred losses. Foreign opaque
// items and unsupported part kinds return keep==false without emitting empty
// Gemini part objects.
func encodeSystemOrUserPart(p ir.Part, pPath string, losses *[]ir.Loss) (wireOutPart, bool) {
	switch p.Kind {
	case ir.PartText, ir.PartRefusal:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if len(p.Annotations) > 0 {
			*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: pPath + ".annotations"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		if p.Kind == ir.PartRefusal {
			*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: pPath})
		}
		unknown := restoreOrDropExtra(p.Extra, pPath, losses)
		sig := rawReplayTokenData(p.Token)
		if rawSig, ok := unknown["thoughtSignature"]; ok {
			if decodedSig, isStr := tryDecodeJSONString(rawSig); isStr && sig == "" {
				sig = decodedSig
			}
			delete(unknown, "thoughtSignature")
		}
		stripModeledPartKeys(unknown)
		txt := p.Text
		return wireOutPart{
			part: wirePart{
				Text:             &txt,
				ThoughtSignature: sig,
				Unknown:          unknown,
			},
		}, true

	case ir.PartMedia:
		return encodeMediaPart(p, pPath, losses)

	case ir.PartOpaque:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolGeminiGenerateContent {
			return wireOutPart{raw: compactRawJSON(p.Opaque.Raw)}, true
		}
		detail := ""
		if p.Opaque != nil {
			detail = p.Opaque.Type
		}
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOpaqueItemDropped,
			Path:   pPath,
			Detail: detail,
		})
		return wireOutPart{}, false

	default:
		return wireOutPart{}, false
	}
}

// encodeMediaPart converts an ir.PartMedia into Gemini inlineData for base64
// payloads or fileData for URL and file_id sources while appending losses. Empty
// or nil media payloads return keep==false with LossMediaUnsupported, whereas
// base64 attachments lacking an explicit MIME string fall back to defaultModalityMIME.
func encodeMediaPart(p ir.Part, pPath string, losses *[]ir.Loss) (wireOutPart, bool) {
	if p.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
	}
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
	}
	if p.Media == nil || strings.TrimSpace(p.Media.Data) == "" {
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossMediaUnsupported,
			Path:   pPath,
			Detail: "empty_media",
		})
		return wireOutPart{}, false
	}

	partUnknown := restoreOrDropExtra(p.Extra, pPath, losses)
	var sig string
	if rawSig, ok := partUnknown["thoughtSignature"]; ok {
		if decodedSig, isStr := tryDecodeJSONString(rawSig); isStr {
			sig = decodedSig
		}
		delete(partUnknown, "thoughtSignature")
	}

	mime := strings.TrimSpace(p.Media.MIME)
	if p.Media.Source == ir.MediaSourceBase64 {
		if mime == "" {
			mime = defaultModalityMIME(p.Media.Type)
		}
		inlineUnknown, _ := extractSubObjectUnknown(partUnknown, "inlineData", "mimeType", "data")
		delete(partUnknown, "inlineData")
		stripModeledPartKeys(partUnknown)
		return wireOutPart{
			part: wirePart{
				ThoughtSignature: sig,
				InlineData: &wireInlineData{
					MimeType: mime,
					Data:     p.Media.Data,
					Unknown:  inlineUnknown,
				},
				Unknown: partUnknown,
			},
		}, true
	}

	fileUnknown, _ := extractSubObjectUnknown(partUnknown, "fileData", "mimeType", "fileUri")
	delete(partUnknown, "fileData")
	stripModeledPartKeys(partUnknown)
	return wireOutPart{
		part: wirePart{
			ThoughtSignature: sig,
			FileData: &wireFileData{
				MimeType: mime,
				FileURI:  p.Media.Data,
				Unknown:  fileUnknown,
			},
			Unknown: partUnknown,
		},
	}, true
}

// defaultModalityMIME supplies a canonical fallback MIME type when a
// cross-protocol base64 Media part lacks an explicit MIME string. Image, audio,
// and video modalities map to PNG, WAV, and MP4, while unrecognized modalities
// default to "application/pdf".
func defaultModalityMIME(t ir.MediaType) string {
	switch t {
	case ir.MediaImage:
		return "image/png"
	case ir.MediaAudio:
		return "audio/wav"
	case ir.MediaVideo:
		return "video/mp4"
	default:
		return "application/pdf"
	}
}

// coalesceStagedOutTurns merges consecutive turns sharing the same Gemini wire
// role ("user" or "model"). Split RoleTool+RoleUser runs from one Gemini user
// turn keep the first RoleUser run's Content.Extra, whereas coalescing two
// same-IR-role turns drops Content.Extra with LossExtraDropped so VerifyEssence
// turn coalescing and splitDecodedUserTurn re-decoding stay aligned.
func coalesceStagedOutTurns(turns []stagedOutTurn, losses *[]ir.Loss) []wireOutContent {
	var merged []mergedContentTurn

	for _, t := range turns {
		if len(t.parts) == 0 {
			continue
		}
		if len(merged) > 0 && merged[len(merged)-1].wire.Role == t.role {
			prev := &merged[len(merged)-1]
			prev.wire.Parts = append(prev.wire.Parts, t.parts...)

			sameIRRole := (t.irRole == ir.RoleUser && prev.sawUserRun) ||
				(t.irRole == ir.RoleTool && prev.sawToolRun) ||
				(t.irRole == ir.RoleAssistant && prev.sawModelRun)

			switch {
			case sameIRRole:
				if len(prev.wire.Unknown) > 0 {
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossExtraDropped,
						Path: prev.extraMsgPath,
					})
					prev.wire.Unknown = nil
					prev.extraMsgPath = ""
				}
				if len(t.unknown) > 0 {
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossExtraDropped,
						Path: t.extraMsgPath,
					})
				}

			case t.irRole == ir.RoleUser && !prev.sawUserRun:
				// Tool run followed by the first RoleUser run: drop any tool-owned
				// Extra (since splitDecodedUserTurn binds Content.Extra to RoleUser)
				// and keep the first RoleUser run's Extra.
				if len(prev.wire.Unknown) > 0 {
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossExtraDropped,
						Path: prev.extraMsgPath,
					})
					prev.wire.Unknown = nil
					prev.extraMsgPath = ""
				}
				if len(t.unknown) > 0 {
					prev.wire.Unknown = t.unknown
					prev.extraMsgPath = t.extraMsgPath
				}

			case t.irRole == ir.RoleTool && prev.sawUserRun:
				// User run followed by a RoleTool run: keep the preceding first
				// RoleUser run's Extra and drop any Extra on the trailing RoleTool run.
				if len(t.unknown) > 0 {
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossExtraDropped,
						Path: t.extraMsgPath,
					})
				}
			}

			switch t.irRole {
			case ir.RoleUser:
				prev.sawUserRun = true
			case ir.RoleTool:
				prev.sawToolRun = true
			case ir.RoleAssistant:
				prev.sawModelRun = true
			}
			continue
		}

		merged = append(merged, mergedContentTurn{
			wire: wireOutContent{
				Role:    t.role,
				Parts:   t.parts,
				Unknown: t.unknown,
			},
			sawUserRun:   t.irRole == ir.RoleUser,
			sawToolRun:   t.irRole == ir.RoleTool,
			sawModelRun:  t.irRole == ir.RoleAssistant,
			extraMsgPath: t.extraMsgPath,
		})
	}

	out := make([]wireOutContent, len(merged))
	for i, m := range merged {
		out[i] = m.wire
	}
	return out
}

// resolveGeminiCallableName converts ref into a valid Gemini identifier within
// maxNameBytes. When custom, client_search, namespaced, or sanitized tool names
// flatten into plain Gemini function names and path is non-empty, it appends
// LossToolLowered to losses.
func resolveGeminiCallableName(ref ir.ToolRef, maxNameBytes int, path string, losses *[]ir.Loss) string {
	lowered := false
	rawName := ref.Name
	switch ref.Kind {
	case ir.ToolClientSearch:
		lowered = true
		if strings.TrimSpace(rawName) == "" {
			rawName = clientSearchProxyName
		}
	case ir.ToolCustom:
		lowered = true
	}
	if ref.Namespace != "" {
		lowered = true
		rawName = ref.Namespace + "__" + rawName
	}
	sanitized := sanitizeGeminiToolName(rawName, maxNameBytes)
	if sanitized != rawName {
		lowered = true
	}
	if lowered && losses != nil && path != "" {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolLowered,
			Path:   path,
			Detail: string(ref.Kind),
		})
	}
	return sanitized
}

// sanitizeGeminiToolName normalizes name to match Gemini's identifier syntax
// within maxBytes (defaulting to 64 bytes when maxBytes <= 0 or > 64). Blank
// names become "tool", and names exceeding maxBytes truncate with a deterministic
// eight-hex SHA-256 suffix.
func sanitizeGeminiToolName(name string, maxBytes int) string {
	if maxBytes <= 0 || maxBytes > defaultGeminiToolNameMaxBytes {
		maxBytes = defaultGeminiToolNameMaxBytes
	}
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "tool"
	}
	if !geminiIdentPattern.MatchString(trimmed) {
		var sb strings.Builder
		for i := range len(trimmed) {
			ch := trimmed[i]
			validStart := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
			validCont := validStart || (ch >= '0' && ch <= '9') || ch == '.' || ch == ':' || ch == '-'
			if i == 0 {
				if validStart {
					_ = sb.WriteByte(ch)
				} else if validCont {
					_ = sb.WriteByte('_')
					_ = sb.WriteByte(ch)
				} else {
					_ = sb.WriteByte('_')
				}
			} else if validCont {
				_ = sb.WriteByte(ch)
			} else {
				_ = sb.WriteByte('_')
			}
		}
		trimmed = sb.String()
	}
	if len(trimmed) <= maxBytes {
		return trimmed
	}
	sum := sha256.Sum256([]byte(name))
	suffix := "__" + hex.EncodeToString(sum[:4])
	if maxBytes <= len(suffix)+1 {
		return trimmed[:maxBytes]
	}
	prefixLen := maxBytes - len(suffix)
	return trimmed[:prefixLen] + suffix
}

// stripModeledPartKeys deletes modeled Gemini Part member names from unknown in
// place so json:",embed" serialization never emits duplicate JSON object keys.
// Nil maps are ignored safely without allocation.
func stripModeledPartKeys(unknown map[string]jsontext.Value) {
	for _, k := range []string{
		"text",
		"thought",
		"thoughtSignature",
		"inlineData",
		"fileData",
		"functionCall",
		"functionResponse",
	} {
		delete(unknown, k)
	}
}

// stripModeledContentKeys deletes "role" and "parts" from unknown in place and
// returns the remaining map. Empty maps or maps left empty after stripping
// return nil so Content objects omit empty Unknown maps.
func stripModeledContentKeys(unknown map[string]jsontext.Value) map[string]jsontext.Value {
	if len(unknown) == 0 {
		return nil
	}
	delete(unknown, "role")
	delete(unknown, "parts")
	if len(unknown) == 0 {
		return nil
	}
	return unknown
}
