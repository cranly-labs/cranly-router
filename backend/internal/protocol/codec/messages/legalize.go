package messages

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// legalize.go normalizes IR system instructions and conversation turns into
// strictly valid Anthropic Messages block arrays while recording exact
// original-coordinate ir.Loss entries for every relocated, repaired, lowered,
// dropped, or reordered element.
//
// Leading and mid-conversation system or developer messages fold into the
// top-level system block array with LossSystemRelocated, adjacent assistant
// turns merge before pairing, and unanswered tool_use calls or orphan
// tool_result items prune with LossToolPairingRepaired. Paired tool_result
// blocks keep nested images in place and sit ahead of ordinary user blocks in
// the immediately following user turn, while thinking blocks stay in place when
// signed, emit unsigned ahead of tool_use under passback dialects, or drop with
// LossReasoningTextDropped before a final same-role merge enforces alternating turns.

const (
	// customToolInputSchemaJSON defines the {"input": "..."} wrapper schema used
	// when a standalone Messages request encodes an unlowered custom tool.
	customToolInputSchemaJSON = `{"type":"object","properties":{"input":{"type":"string","description":"The raw input for this tool, passed through verbatim."}},"required":["input"]}`

	// toolSearchProxyName and toolSearchProxySchemaJSON define the function proxy
	// used when a standalone Messages request encodes an unlowered tool_search tool.
	toolSearchProxyName       = "tool_search"
	toolSearchProxySchemaJSON = `{"type":"object","properties":{"query":{"type":"string","description":"Search query for tools or connectors to load."},"limit":{"type":"integer","description":"Maximum number of tool groups to return."}},"required":["query"]}`

	// defaultMessagesToolNameMaxBytes bounds flattened namespace tool names to
	// Anthropic Messages' 128-byte tool name limit when Dialect leaves it unset.
	defaultMessagesToolNameMaxBytes = 128
)

// trackedReqPart pairs one source ir.Part with its original IR path coordinate.
type trackedReqPart struct {
	part      ir.Part // source IR part value
	path      string  // original IR coordinate such as "messages[2].parts[1]"
	origOrder int     // zero-based sequence index within the turn
}

// trackedToolCallCandidate holds a pre-validated assistant tool_use candidate awaiting pairing.
type trackedToolCallCandidate struct {
	callID    string                    // non-empty correlation identifier
	name      string                    // valid Messages tool name
	arguments string                    // validated JSON object string
	cache     *wireCacheControl         // optional serialized cache_control
	unknown   map[string]jsontext.Value // preserved same-origin Extra members
	path      string                    // original IR part coordinate
	origOrder int                       // zero-based sequence index within the turn
}

// trackedToolResultSlot holds one split tool_result item during pairing resolution.
type trackedToolResultSlot struct {
	partPath   string                    // original IR path of the PartToolResult
	msgPath    string                    // original IR path of owning RoleTool message
	stageIdx   int                       // index inside stages slice for order preservation
	msgUnknown map[string]jsontext.Value // preserved Extra from owning RoleTool message
	part       ir.Part                   // source PartToolResult including Cache and Extra
	result     ir.ToolResult             // cloned tool result payload
	claimed    bool                      // true once paired to an assistant tool_use
	dropped    bool                      // true when invalidated by a pruned call
}

// stageTurn represents one intermediate user, assistant, or tool turn before pairing.
type stageTurn struct {
	role          ir.Role                   // RoleUser, RoleAssistant, or RoleTool
	firstMsgPath  string                    // original IR path of first merged message
	unknown       map[string]jsontext.Value // merged same-origin Message.Extra members
	items         []stageAssistantItem      // ordered parts and tool candidates on assistant turns
	userParts     []trackedReqPart          // ordered parts on user turns
	toolSlot      *trackedToolResultSlot    // single tool_result slot when role is RoleTool
	nextItemOrder int                       // monotonic item ordinal within merged assistant turn
}

// stageAssistantItem holds either a non-tool-call part or a validated tool_use candidate in turn order.
type stageAssistantItem struct {
	isCall bool                     // true when cand is active; false when part is active
	part   trackedReqPart           // non-tool-call assistant part
	cand   trackedToolCallCandidate // validated assistant tool_use candidate
}

// serializedBlock tags a serialized Messages block JSON payload with its block category
// so merging and thinking-order checks avoid reparsing JSON bytes.
type serializedBlock struct {
	blockType string         // "thinking", "redacted_thinking", "tool_use", "tool_result", "text", etc.
	raw       jsontext.Value // deterministic JSON block bytes
	origOrder int            // original turn item ordinal
}

// stagedWireMessage holds typed blocks before final consecutive same-role merging.
type stagedWireMessage struct {
	role      string                    // "user" or "assistant"
	msgPath   string                    // source message path for LossPartsReordered
	blocks    []serializedBlock         // ordered blocks on the message
	unknown   map[string]jsontext.Value // preserved same-origin Message.Extra
	reordered bool                      // whether blocks already recorded LossPartsReordered
}

// legalizeRequestMessages executes Steps 1 through 5 of section 4.1 on req,
// returning the serialized top-level system block array and legalized
// alternating user and assistant messages alongside any incurred ir.Loss records.
//
// Mid-conversation system or developer turns fold into top-level system blocks,
// adjacent assistant turns merge before tool pairing, and every loss coordinate
// references its original slice index in req without mutating caller slices.
func legalizeRequestMessages(
	req *ir.Request,
	dialect ir.Dialect,
	maxNameBytes int,
	losses *[]ir.Loss,
) ([]jsontext.Value, []wireOutMessage, error) {
	var sysBlocks []jsontext.Value
	for i, p := range req.System {
		sPath := fmt.Sprintf("system[%d]", i)
		blk, ok, err := encodeSystemPart(p, sPath, losses)
		if err != nil {
			return nil, nil, err
		}
		if ok {
			sysBlocks = append(sysBlocks, blk)
		}
	}

	var stages []stageTurn
	invalidCallIDs := make(map[string]bool)

	for mIdx, msg := range req.Messages {
		mPath := fmt.Sprintf("messages[%d]", mIdx)
		if msg.ID != "" {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossIDRewritten,
				Path: mPath + ".id",
			})
		}

		// Step 1: fold leading and mid-conversation system or developer turns into
		// top-level system blocks and report LossSystemRelocated.
		if msg.Role == ir.RoleSystem || msg.Role == ir.RoleDeveloper {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossSystemRelocated,
				Path: mPath,
			})
			_ = restoreOrDropExtra(msg.Extra, mPath, losses)
			for pIdx, p := range msg.Parts {
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				blk, ok, err := encodeRelocatedSystemPart(p, pPath, losses)
				if err != nil {
					return nil, nil, err
				}
				if ok {
					sysBlocks = append(sysBlocks, blk)
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
				var slotMsgUnknown map[string]jsontext.Value
				if pIdx == 0 {
					slotMsgUnknown = msgUnknown
				}
				slot := &trackedToolResultSlot{
					partPath:   pPath,
					msgPath:    mPath,
					stageIdx:   len(stages),
					msgUnknown: slotMsgUnknown,
					part:       p,
					result:     *p.Clone().Result,
				}
				if slot.result.CallID != "" && invalidCallIDs[slot.result.CallID] {
					slot.dropped = true
					*losses = append(*losses, ir.Loss{
						Kind:   ir.LossToolPairingRepaired,
						Path:   pPath,
						Detail: "invalid_call_dropped",
					})
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
			// Merge adjacent assistant turns before tool pairing so parallel calls
			// across split items pair together in one assistant turn.
			var target *stageTurn
			if len(stages) > 0 && stages[len(stages)-1].role == ir.RoleAssistant {
				target = &stages[len(stages)-1]
				target.unknown = mergeUnknownMaps(target.unknown, msgUnknown)
			} else {
				stages = append(stages, stageTurn{
					role:         ir.RoleAssistant,
					firstMsgPath: mPath,
					unknown:      msgUnknown,
				})
				target = &stages[len(stages)-1]
			}

			seenCallIDs := collectCandidateCallIDs(target.items)
			for pIdx, p := range msg.Parts {
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				ord := target.nextItemOrder
				target.nextItemOrder++
				if p.Kind == ir.PartToolCall && p.Call != nil {
					cand, ok, err := prepareMessagesToolCall(p, pPath, ord, maxNameBytes, seenCallIDs, invalidCallIDs, losses)
					if err != nil {
						return nil, nil, err
					}
					if ok {
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
						origOrder: ord,
					},
				})
			}
			continue
		}

		// RoleUser turn.
		uTurn := stageTurn{
			role:         ir.RoleUser,
			firstMsgPath: mPath,
			unknown:      msgUnknown,
			userParts:    make([]trackedReqPart, 0, len(msg.Parts)),
		}
		for pIdx, p := range msg.Parts {
			uTurn.userParts = append(uTurn.userParts, trackedReqPart{
				part:      p,
				path:      fmt.Sprintf("%s.parts[%d]", mPath, pIdx),
				origOrder: pIdx,
			})
		}
		stages = append(stages, uTurn)
	}

	outMessages, err := pairAndSerializeStages(stages, dialect, losses)
	if err != nil {
		return nil, nil, err
	}
	return sysBlocks, outMessages, nil
}

// collectCandidateCallIDs builds the set of CallIDs already present on an assistant stageTurn.
func collectCandidateCallIDs(items []stageAssistantItem) map[string]bool {
	seen := make(map[string]bool)
	for _, it := range items {
		if it.isCall {
			seen[it.cand.callID] = true
		}
	}
	return seen
}

// mergeUnknownMaps returns a new map combining entries from base and extra
// while keeping base values whenever duplicate keys appear. When both input
// maps are empty, it returns nil without allocating a destination map.
func mergeUnknownMaps(base, extra map[string]jsontext.Value) map[string]jsontext.Value {
	if len(extra) == 0 {
		return base
	}
	if len(base) == 0 {
		return cloneValueMap(extra)
	}
	merged := cloneValueMap(base)
	for k, v := range extra {
		if _, exists := merged[k]; !exists {
			merged[k] = v
		}
	}
	return merged
}

// recordDroppedEmptyTextLosses records replay token, cache, Extra, and
// empty-placeholder losses when an empty PartText or PartRefusal is omitted
// under MSG-EMPTY-TEXT. Recording metadata losses before LossClientMarkerDropped
// keeps VerifyEssence slot tombstoning aligned with original IR part coordinates.
func recordDroppedEmptyTextLosses(p ir.Part, path string, losses *[]ir.Loss) {
	if p.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
	}
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	if !p.Extra.IsZero() {
		*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: path})
	}
	*losses = append(*losses, ir.Loss{
		Kind:   ir.LossClientMarkerDropped,
		Path:   path,
		Detail: "empty_text",
	})
}

// encodeSystemPart serializes one Part from req.System into a Messages system
// block. Empty text parts without content and unsupported media or foreign
// opaque items return ok=false after recording any incurred Loss entries.
func encodeSystemPart(p ir.Part, path string, losses *[]ir.Loss) (jsontext.Value, bool, error) {
	if p.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
	}
	if len(p.Annotations) > 0 {
		*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: path})
	}
	switch p.Kind {
	case ir.PartText:
		if p.Text == "" {
			// Drop empty system text blocks because Anthropic rejects empty system
			// text entries, and report replay token, cache, Extra, and placeholder drops.
			recordDroppedEmptyTextLosses(p, path, losses)
			return nil, false, nil
		}
		if p.Token != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
		}
		raw, err := marshalDeterministic(wireTextBlock{
			Type:         "text",
			Text:         p.Text,
			CacheControl: encodeCacheControl(p.Cache, path+".cache", losses),
			Unknown:      restoreOrDropExtra(p.Extra, path, losses),
		})
		return jsontext.Value(raw), true, err

	case ir.PartOpaque:
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
		}
		_ = restoreOrDropExtra(p.Extra, path, losses)
		if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolAnthropicMessages {
			return jsontext.Value(bytes.Clone(p.Opaque.Raw)), true, nil
		}
		detail := ""
		if p.Opaque != nil {
			detail = p.Opaque.Type
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: path, Detail: detail})
		return nil, false, nil

	case ir.PartMedia:
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
		}
		_ = restoreOrDropExtra(p.Extra, path, losses)
		*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: path, Detail: "system"})
		return nil, false, nil

	default:
		return nil, false, nil
	}
}

// encodeRelocatedSystemPart converts a Part from a relocated RoleSystem or
// RoleDeveloper message into a top-level Messages system text block. Empty
// text parts and non-text parts return ok=false after recording their Losses.
func encodeRelocatedSystemPart(p ir.Part, path string, losses *[]ir.Loss) (jsontext.Value, bool, error) {
	if p.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
	}
	if len(p.Annotations) > 0 {
		*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: path})
	}
	switch p.Kind {
	case ir.PartText, ir.PartRefusal:
		if p.Text == "" {
			// Omit empty relocated system text blocks and record replay token, cache,
			// Extra, and placeholder losses so MSG-EMPTY-TEXT and Law Q4 both hold.
			recordDroppedEmptyTextLosses(p, path, losses)
			return nil, false, nil
		}
		if p.Token != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
		}
		if p.Kind == ir.PartRefusal {
			*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: path})
		}
		cache := encodeCacheControl(p.Cache, path+".cache", losses)
		unknown := restoreOrDropExtra(p.Extra, path, losses)
		raw, err := marshalDeterministic(wireTextBlock{
			Type:         "text",
			Text:         p.Text,
			CacheControl: cache,
			Unknown:      unknown,
		})
		return jsontext.Value(raw), true, err

	case ir.PartMedia:
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
		}
		_ = restoreOrDropExtra(p.Extra, path, losses)
		*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: path, Detail: "system"})
		return nil, false, nil

	case ir.PartOpaque:
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
		}
		_ = restoreOrDropExtra(p.Extra, path, losses)
		detail := ""
		if p.Opaque != nil {
			detail = p.Opaque.Type
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: path, Detail: detail})
		return nil, false, nil

	default:
		return nil, false, nil
	}
}

// prepareMessagesToolCall validates and normalizes one assistant PartToolCall
// candidate, returning ok=false when the call must be pruned. Custom,
// client_search, and non-default namespaced calls lower into standard function
// tool_use candidates and record LossToolLowered.
//
// Calls with blank or duplicate per-turn CallIDs, hosted tool targets, illegal
// Messages tool names, or non-object JSON arguments are pruned and registered in
// invalidCallIDs so matching downstream tool_result items also prune cleanly.
func prepareMessagesToolCall(
	p ir.Part,
	pPath string,
	origOrder int,
	maxNameBytes int,
	seenInTurn map[string]bool,
	invalidCallIDs map[string]bool,
	losses *[]ir.Loss,
) (trackedToolCallCandidate, bool, error) {
	call := p.Call
	if call.CallID == "" || seenInTurn[call.CallID] {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolPairingRepaired,
			Path: pPath,
		})
		return trackedToolCallCandidate{}, false, nil
	}
	if call.Tool.Kind == ir.ToolHosted {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossOpaqueItemDropped,
			Path: pPath,
		})
		invalidCallIDs[call.CallID] = true
		return trackedToolCallCandidate{}, false, nil
	}

	name := call.Tool.Name
	args := call.Arguments
	lowered := false

	switch call.Tool.Kind {
	case ir.ToolCustom:
		wrapped, err := marshalDeterministic(struct {
			Input string `json:"input"` // raw custom tool free-text payload
		}{Input: args})
		if err != nil {
			return trackedToolCallCandidate{}, false, err
		}
		args = string(wrapped)
		if call.Tool.Namespace != "" {
			name = flattenMessagesToolName(call.Tool.Namespace, name, maxNameBytes)
		}
		lowered = true

	case ir.ToolClientSearch:
		name = toolSearchProxyName
		if strings.TrimSpace(args) == "" {
			args = "{}"
		}
		lowered = true

	case ir.ToolFunction:
		if call.Tool.Namespace != "" {
			name = flattenMessagesToolName(call.Tool.Namespace, name, maxNameBytes)
			if call.Tool.Namespace != "functions" {
				lowered = true
			}
		}
	}

	if !isValidMessagesToolName(name, maxNameBytes) {
		invalidCallIDs[call.CallID] = true
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolPairingRepaired,
			Path:   pPath,
			Detail: "invalid_tool_name",
		})
		return trackedToolCallCandidate{}, false, nil
	}

	if strings.TrimSpace(args) == "" {
		args = "{}"
	}
	if !isValidSingleJSONObject([]byte(args)) {
		invalidCallIDs[call.CallID] = true
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolPairingRepaired,
			Path:   pPath,
			Detail: "invalid_json_arguments",
		})
		return trackedToolCallCandidate{}, false, nil
	}

	if p.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
	}
	if lowered {
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: pPath})
	}
	if call.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
	}
	cache := encodeCacheControl(p.Cache, pPath+".cache", losses)
	unknown := restoreOrDropExtra(p.Extra, pPath, losses)

	seenInTurn[call.CallID] = true
	return trackedToolCallCandidate{
		callID:    call.CallID,
		name:      name,
		arguments: string(compactRawJSON(jsontext.Value(args))),
		cache:     cache,
		unknown:   unknown,
		path:      pPath,
		origOrder: origOrder,
	}, true, nil
}

// isValidMessagesToolName reports whether name is non-empty, fits within maxLen
// bytes, and contains only ASCII letters, digits, underscores, or hyphens.
func isValidMessagesToolName(name string, maxLen int) bool {
	if name == "" || len(name) > maxLen {
		return false
	}
	for i := range len(name) {
		b := name[i]
		isAlpha := (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
		isDigit := b >= '0' && b <= '9'
		if !isAlpha && !isDigit && b != '_' && b != '-' {
			return false
		}
	}
	return true
}

// pairAndSerializeStages pairs assistant tool_use candidates against subsequent
// tool_result slots and serializes surviving turns into alternating user and
// assistant wireOutMessage entries. Empty turns after pruning are omitted.
//
// Paired tool_result blocks retain their original message appearance order
// rather than reordering to match assistant tool_use declarations, and pulling a
// tool_result ahead of an intervening non-empty turn records LossPartsReordered.
func pairAndSerializeStages(
	stages []stageTurn,
	dialect ir.Dialect,
	losses *[]ir.Loss,
) ([]wireOutMessage, error) {
	// resolvedAssistant pairs surviving assistant tool_use IDs with claimed tool_result slots.
	type resolvedAssistant struct {
		keptByID map[string]bool          // CallIDs with matched tool_result slots
		replies  []*trackedToolResultSlot // matched replies in source appearance order
	}
	resolved := make(map[int]resolvedAssistant)

	for i := range stages {
		if stages[i].role != ir.RoleAssistant {
			continue
		}
		ra := resolvedAssistant{keptByID: make(map[string]bool)}
		for _, item := range stages[i].items {
			if !item.isCall {
				continue
			}
			matched := claimMessagesToolReply(stages, i+1, item.cand.callID, losses)
			if matched == nil {
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossToolPairingRepaired,
					Path: item.cand.path,
				})
				continue
			}
			ra.keptByID[item.cand.callID] = true
			ra.replies = append(ra.replies, matched)
		}
		// Keep paired tool_result items in their original stage appearance order
		// so parallel out-of-order tool results round-trip without silent mutation.
		slices.SortFunc(ra.replies, func(a, b *trackedToolResultSlot) int {
			if a.stageIdx < b.stageIdx {
				return -1
			}
			if a.stageIdx > b.stageIdx {
				return 1
			}
			return 0
		})
		resolved[i] = ra
	}

	for i := range stages {
		if stages[i].role == ir.RoleTool && stages[i].toolSlot != nil {
			slot := stages[i].toolSlot
			if !slot.claimed && !slot.dropped {
				slot.dropped = true
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossToolPairingRepaired,
					Path: slot.partPath,
				})
			}
		}
	}

	stageBlocks := make([][]serializedBlock, len(stages))
	stageReordered := make([]bool, len(stages))
	for i := range stages {
		st := stages[i]
		switch st.role {
		case ir.RoleAssistant:
			ra := resolved[i]
			asstBlocks, reordered, err := serializeAssistantTurnBlocks(st, ra.keptByID, len(ra.replies) > 0, dialect, losses)
			if err != nil {
				return nil, err
			}
			stageBlocks[i] = asstBlocks
			stageReordered[i] = reordered
		case ir.RoleUser:
			uBlocks, err := serializeUserTurnBlocks(st.userParts, losses)
			if err != nil {
				return nil, err
			}
			stageBlocks[i] = uBlocks
		}
	}

	var staged []stagedWireMessage
	for i := range stages {
		st := stages[i]
		switch st.role {
		case ir.RoleTool:
			continue

		case ir.RoleAssistant:
			ra := resolved[i]
			asstBlocks := stageBlocks[i]
			if len(asstBlocks) == 0 && len(ra.replies) == 0 {
				continue
			}
			if len(asstBlocks) > 0 {
				appendOrMergeStagedMessage(&staged, stagedWireMessage{
					role:      "assistant",
					msgPath:   st.firstMsgPath,
					blocks:    asstBlocks,
					unknown:   st.unknown,
					reordered: stageReordered[i],
				}, losses)
			}
			if len(ra.replies) > 0 {
				var resBlocks []serializedBlock
				var replyMsgUnknown map[string]jsontext.Value
				movedAcrossTurn := false
				reorderedRecorded := make(map[string]bool)
				for rIdx, reply := range ra.replies {
					blk, err := serializeToolResultSlot(reply, losses)
					if err != nil {
						return nil, err
					}
					resBlocks = append(resBlocks, serializedBlock{
						blockType: "tool_result",
						raw:       blk,
						origOrder: rIdx,
					})
					replyMsgUnknown = mergeUnknownMaps(replyMsgUnknown, reply.msgUnknown)

					// Report LossPartsReordered whenever a paired tool_result had to
					// move ahead of an intervening non-empty user or assistant turn.
					for k := i + 1; k < reply.stageIdx; k++ {
						if len(stageBlocks[k]) > 0 {
							movedAcrossTurn = true
							if reply.msgPath != "" && !reorderedRecorded[reply.msgPath] {
								reorderedRecorded[reply.msgPath] = true
								*losses = append(*losses, ir.Loss{
									Kind: ir.LossPartsReordered,
									Path: reply.msgPath,
								})
							}
							break
						}
					}
				}
				appendOrMergeStagedMessage(&staged, stagedWireMessage{
					role:      "user",
					msgPath:   ra.replies[0].msgPath,
					blocks:    resBlocks,
					unknown:   replyMsgUnknown,
					reordered: movedAcrossTurn,
				}, losses)
			}

		case ir.RoleUser:
			uBlocks := stageBlocks[i]
			if len(uBlocks) == 0 {
				continue
			}
			appendOrMergeStagedMessage(&staged, stagedWireMessage{
				role:    "user",
				msgPath: st.firstMsgPath,
				blocks:  uBlocks,
				unknown: st.unknown,
			}, losses)
		}
	}

	if len(staged) == 0 {
		return nil, nil
	}
	out := make([]wireOutMessage, len(staged))
	for i, sm := range staged {
		raws := make([]jsontext.Value, len(sm.blocks))
		for j, b := range sm.blocks {
			raws[j] = b.raw
		}
		out[i] = wireOutMessage{
			Role:    sm.role,
			Content: raws,
			Unknown: sm.unknown,
		}
	}
	return out, nil
}

// claimMessagesToolReply scans forward from startIdx up to the next assistant
// turn redeclaring callID and claims matching unclaimed tool_result slots. When
// duplicate results exist for one call, the last result wins and earlier
// duplicates record LossToolPairingRepaired.
func claimMessagesToolReply(
	stages []stageTurn,
	startIdx int,
	callID string,
	losses *[]ir.Loss,
) *trackedToolResultSlot {
	var matches []*trackedToolResultSlot
	for j := startIdx; j < len(stages); j++ {
		if stages[j].role == ir.RoleAssistant {
			redeclares := false
			for _, it := range stages[j].items {
				if it.isCall && it.cand.callID == callID {
					redeclares = true
					break
				}
			}
			if redeclares {
				break
			}
		}
		if stages[j].role != ir.RoleTool || stages[j].toolSlot == nil {
			continue
		}
		slot := stages[j].toolSlot
		if slot.claimed || slot.dropped || slot.result.CallID != callID {
			continue
		}
		slot.claimed = true
		matches = append(matches, slot)
	}
	if len(matches) == 0 {
		return nil
	}
	for k := range len(matches) - 1 {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolPairingRepaired,
			Path:   matches[k].partPath,
			Detail: "duplicate_tool_result",
		})
	}
	return matches[len(matches)-1]
}

// serializeAssistantTurnBlocks converts surviving parts and paired tool_use
// candidates on an assistant stageTurn into serialized Messages blocks. It
// reports whether any thinking block had to move ahead of tool_use blocks.
//
// Signed thinking and redacted_thinking blocks remain in their original relative
// positions ahead of tool_use. Passback dialects emit unsigned thinking blocks
// at the start of tool-calling assistant turns, whereas non-passback dialects
// drop unsigned reasoning with LossReasoningTextDropped.
func serializeAssistantTurnBlocks(
	st stageTurn,
	keptCalls map[string]bool,
	hasPairedCalls bool,
	dialect ir.Dialect,
	losses *[]ir.Loss,
) ([]serializedBlock, bool, error) {
	var blocks []serializedBlock
	var passbackLeading []serializedBlock
	reordered := false

	for _, item := range st.items {
		if item.isCall {
			cand := item.cand
			if !keptCalls[cand.callID] {
				continue
			}
			raw, err := marshalDeterministic(wireToolUseBlock{
				Type:         "tool_use",
				ID:           cand.callID,
				Name:         cand.name,
				Input:        jsontext.Value(cand.arguments),
				CacheControl: cand.cache,
				Unknown:      cand.unknown,
			})
			if err != nil {
				return nil, false, err
			}
			blocks = append(blocks, serializedBlock{
				blockType: "tool_use",
				raw:       jsontext.Value(raw),
				origOrder: cand.origOrder,
			})
			continue
		}

		tp := item.part
		p := tp.part
		switch p.Kind {
		case ir.PartReasoning:
			if p.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: tp.path + ".id"})
			}
			hasToken := p.Reasoning != nil && p.Reasoning.Token != nil && p.Reasoning.Token.Data != ""
			var joined string
			if p.Reasoning != nil {
				joined = strings.Join(p.Reasoning.Segments, "")
			}

			if hasToken {
				cache := encodeCacheControl(p.Cache, tp.path+".cache", losses)
				unknown := restoreOrDropExtra(p.Extra, tp.path, losses)
				if p.Reasoning.Redacted {
					raw, err := marshalDeterministic(wireRedactedThinkingBlock{
						Type:         "redacted_thinking",
						Data:         p.Reasoning.Token.Data,
						CacheControl: cache,
						Unknown:      unknown,
					})
					if err != nil {
						return nil, false, err
					}
					blocks = append(blocks, serializedBlock{
						blockType: "redacted_thinking",
						raw:       jsontext.Value(raw),
						origOrder: tp.origOrder,
					})
				} else {
					raw, err := marshalDeterministic(wireThinkingBlock{
						Type:         "thinking",
						Thinking:     joined,
						Signature:    p.Reasoning.Token.Data,
						CacheControl: cache,
						Unknown:      unknown,
					})
					if err != nil {
						return nil, false, err
					}
					blocks = append(blocks, serializedBlock{
						blockType: "thinking",
						raw:       jsontext.Value(raw),
						origOrder: tp.origOrder,
					})
				}
				continue
			}

			// Unsigned reasoning: passback models emit unsigned thinking at the
			// start of tool-calling assistant turns; otherwise drop and report Loss.
			if dialect.ReasoningHistory == ir.HistoryToolTurns && hasPairedCalls && joined != "" {
				cache := encodeCacheControl(p.Cache, tp.path+".cache", losses)
				unknown := restoreOrDropExtra(p.Extra, tp.path, losses)
				raw, err := marshalDeterministic(wireThinkingBlock{
					Type:         "thinking",
					Thinking:     joined,
					CacheControl: cache,
					Unknown:      unknown,
				})
				if err != nil {
					return nil, false, err
				}
				if len(blocks) > 0 {
					reordered = true
				}
				passbackLeading = append(passbackLeading, serializedBlock{
					blockType: "thinking",
					raw:       jsontext.Value(raw),
					origOrder: tp.origOrder,
				})
				continue
			}

			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tp.path})
			}
			_ = restoreOrDropExtra(p.Extra, tp.path, losses)
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossReasoningTextDropped,
				Path: tp.path,
			})

		case ir.PartText, ir.PartRefusal:
			if p.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: tp.path + ".id"})
			}
			if len(p.Annotations) > 0 {
				*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: tp.path})
			}
			if p.Text == "" {
				// Omit empty assistant text blocks and report replay token, cache,
				// Extra, and placeholder drops so MSG-EMPTY-TEXT and Law Q4 both hold.
				recordDroppedEmptyTextLosses(p, tp.path, losses)
				continue
			}
			if p.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: tp.path})
			}
			if p.Kind == ir.PartRefusal {
				*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: tp.path})
			}
			cache := encodeCacheControl(p.Cache, tp.path+".cache", losses)
			unknown := restoreOrDropExtra(p.Extra, tp.path, losses)
			raw, err := marshalDeterministic(wireTextBlock{
				Type:         "text",
				Text:         p.Text,
				CacheControl: cache,
				Unknown:      unknown,
			})
			if err != nil {
				return nil, false, err
			}
			blocks = append(blocks, serializedBlock{
				blockType: "text",
				raw:       jsontext.Value(raw),
				origOrder: tp.origOrder,
			})

		case ir.PartMedia:
			if p.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: tp.path + ".id"})
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tp.path})
			}
			_ = restoreOrDropExtra(p.Extra, tp.path, losses)
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossMediaUnsupported,
				Path:   tp.path,
				Detail: "assistant",
			})

		case ir.PartOpaque:
			if p.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: tp.path + ".id"})
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tp.path})
			}
			_ = restoreOrDropExtra(p.Extra, tp.path, losses)
			if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolAnthropicMessages {
				blocks = append(blocks, serializedBlock{
					blockType: p.Opaque.Type,
					raw:       jsontext.Value(bytes.Clone(p.Opaque.Raw)),
					origOrder: tp.origOrder,
				})
			} else {
				detail := ""
				if p.Opaque != nil {
					detail = p.Opaque.Type
				}
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossOpaqueItemDropped,
					Path:   tp.path,
					Detail: detail,
				})
			}
		}
	}

	if len(passbackLeading) > 0 {
		blocks = append(passbackLeading, blocks...)
	}

	// Ensure thinking and redacted_thinking blocks precede tool_use blocks within
	// the same assistant turn to satisfy Anthropic's MSG-THINKING ordering rule.
	if enforceAssistantThinkingBeforeTools(blocks) {
		reordered = true
	}
	if reordered && st.firstMsgPath != "" {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossPartsReordered,
			Path: st.firstMsgPath,
		})
	}
	return blocks, reordered, nil
}

// enforceAssistantThinkingBeforeTools moves any thinking or redacted_thinking
// block that appeared after a tool_use block ahead of all tool_use blocks while
// preserving relative order within non-tool and tool groups. It returns true
// only when at least one block changed position.
func enforceAssistantThinkingBeforeTools(blocks []serializedBlock) bool {
	sawToolUse := false
	needsFix := false
	for _, b := range blocks {
		if b.blockType == "tool_use" {
			sawToolUse = true
		} else if sawToolUse && (b.blockType == "thinking" || b.blockType == "redacted_thinking") {
			needsFix = true
			break
		}
	}
	if !needsFix {
		return false
	}

	rebuilt := make([]serializedBlock, 0, len(blocks))
	for _, b := range blocks {
		if b.blockType == "thinking" || b.blockType == "redacted_thinking" {
			rebuilt = append(rebuilt, b)
		}
	}
	for _, b := range blocks {
		if b.blockType != "thinking" && b.blockType != "redacted_thinking" {
			rebuilt = append(rebuilt, b)
		}
	}
	copy(blocks, rebuilt)
	return true
}

// serializeUserTurnBlocks converts parts on a RoleUser stageTurn into serialized
// Messages text, image, document, or same-origin opaque blocks. Empty text parts
// and unsupported media or foreign opaque parts are omitted with Loss records.
func serializeUserTurnBlocks(parts []trackedReqPart, losses *[]ir.Loss) ([]serializedBlock, error) {
	var out []serializedBlock
	for _, tp := range parts {
		p := tp.part
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: tp.path + ".id"})
		}
		if len(p.Annotations) > 0 {
			*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: tp.path})
		}

		switch p.Kind {
		case ir.PartText, ir.PartRefusal:
			if p.Text == "" {
				// Omit empty user text blocks and report replay token, cache, Extra,
				// and placeholder drops so MSG-EMPTY-TEXT and Law Q4 both hold.
				recordDroppedEmptyTextLosses(p, tp.path, losses)
				continue
			}
			if p.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: tp.path})
			}
			if p.Kind == ir.PartRefusal {
				*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: tp.path})
			}
			cache := encodeCacheControl(p.Cache, tp.path+".cache", losses)
			unknown := restoreOrDropExtra(p.Extra, tp.path, losses)
			raw, err := marshalDeterministic(wireTextBlock{
				Type:         "text",
				Text:         p.Text,
				CacheControl: cache,
				Unknown:      unknown,
			})
			if err != nil {
				return nil, err
			}
			out = append(out, serializedBlock{
				blockType: "text",
				raw:       jsontext.Value(raw),
				origOrder: tp.origOrder,
			})

		case ir.PartMedia:
			if p.Media == nil {
				continue
			}
			rawMedia, ok, err := serializeMessagesMediaBlock(*p.Media, p.Cache, p.Extra, tp.path, losses)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, serializedBlock{
					blockType: string(p.Media.Type),
					raw:       rawMedia,
					origOrder: tp.origOrder,
				})
			}

		case ir.PartOpaque:
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tp.path})
			}
			_ = restoreOrDropExtra(p.Extra, tp.path, losses)
			if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolAnthropicMessages {
				out = append(out, serializedBlock{
					blockType: p.Opaque.Type,
					raw:       jsontext.Value(bytes.Clone(p.Opaque.Raw)),
					origOrder: tp.origOrder,
				})
			} else {
				detail := ""
				if p.Opaque != nil {
					detail = p.Opaque.Type
				}
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossOpaqueItemDropped,
					Path:   tp.path,
					Detail: detail,
				})
			}
		}
	}
	return out, nil
}

// serializeMessagesMediaBlock converts an ir.Media attachment into an Anthropic
// "image" or "document" JSON block. Unsupported audio or video modalities return
// ok=false and append LossMediaUnsupported at path.
func serializeMessagesMediaBlock(
	m ir.Media,
	cacheMark *ir.CacheMark,
	extra ir.Extra,
	path string,
	losses *[]ir.Loss,
) (jsontext.Value, bool, error) {
	var blockType, defaultMIME string
	switch m.Type {
	case ir.MediaImage:
		blockType = "image"
		defaultMIME = "image/png"
	case ir.MediaDocument:
		blockType = "document"
		defaultMIME = "application/pdf"
	default:
		if cacheMark != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
		}
		_ = restoreOrDropExtra(extra, path, losses)
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossMediaUnsupported,
			Path:   path,
			Detail: string(m.Type),
		})
		return nil, false, nil
	}

	var src wireMediaSource
	switch m.Source {
	case ir.MediaSourceBase64:
		mime := strings.TrimSpace(m.MIME)
		// Supply a modality-appropriate default MIME type when cross-protocol base64
		// attachments omit media_type so Anthropic base64 validation succeeds.
		if mime == "" {
			mime = defaultMIME
		}
		src = wireMediaSource{
			Type:      "base64",
			MediaType: mime,
			Data:      m.Data,
		}
	case ir.MediaSourceURL:
		src = wireMediaSource{
			Type: "url",
			URL:  m.Data,
		}
	case ir.MediaSourceFileID:
		src = wireMediaSource{
			Type:   "file",
			FileID: m.Data,
		}
	default:
		if cacheMark != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
		}
		_ = restoreOrDropExtra(extra, path, losses)
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossMediaUnsupported,
			Path:   path,
			Detail: blockType + ":" + m.Source,
		})
		return nil, false, nil
	}

	cache := encodeCacheControl(cacheMark, path+".cache", losses)
	unknown := restoreOrDropExtra(extra, path, losses)
	// Populate document block title from cross-protocol Media.Name only when
	// same-origin Extra did not already preserve an explicit wire title field.
	if m.Type == ir.MediaDocument && m.Name != "" {
		if _, hasTitle := unknown["title"]; !hasTitle {
			rawTitle, err := marshalDeterministic(m.Name)
			if err != nil {
				return nil, false, err
			}
			if unknown == nil {
				unknown = make(map[string]jsontext.Value, 1)
			}
			unknown["title"] = jsontext.Value(rawTitle)
		}
	}

	raw, err := marshalDeterministic(wireMediaBlock{
		Type:         blockType,
		Source:       &src,
		CacheControl: cache,
		Unknown:      unknown,
	})
	return jsontext.Value(raw), true, err
}

// serializeToolResultSlot encodes one paired trackedToolResultSlot into a
// Messages "tool_result" JSON block while keeping nested images inside content[].
// Single plain-text results serialize as compact string content, whereas mixed
// or cache-annotated results serialize as inner block arrays.
func serializeToolResultSlot(slot *trackedToolResultSlot, losses *[]ir.Loss) (jsontext.Value, error) {
	cache := encodeCacheControl(slot.part.Cache, slot.partPath+".cache", losses)
	unknown := restoreOrDropExtra(slot.part.Extra, slot.partPath, losses)

	var innerBlocks []jsontext.Value
	var singlePlainText *string
	plainCandidate := true

	for cIdx, child := range slot.result.Content {
		cPath := fmt.Sprintf("%s.result.content[%d]", slot.partPath, cIdx)
		if child.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: cPath + ".id"})
		}
		if len(child.Annotations) > 0 {
			*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: cPath})
		}

		switch child.Kind {
		case ir.PartText:
			if child.Text == "" {
				// Omit empty nested tool_result text blocks and report cache, Extra,
				// and placeholder drops so MSG-EMPTY-TEXT and Law Q4 both hold.
				recordDroppedEmptyTextLosses(child, cPath, losses)
				plainCandidate = false
				continue
			}
			cCache := encodeCacheControl(child.Cache, cPath+".cache", losses)
			cUnknown := restoreOrDropExtra(child.Extra, cPath, losses)
			if cCache != nil || len(cUnknown) > 0 {
				plainCandidate = false
			}
			rawItem, err := marshalDeterministic(wireTextBlock{
				Type:         "text",
				Text:         child.Text,
				CacheControl: cCache,
				Unknown:      cUnknown,
			})
			if err != nil {
				return nil, err
			}
			if len(innerBlocks) == 0 && plainCandidate {
				txt := child.Text
				singlePlainText = &txt
			} else {
				singlePlainText = nil
				plainCandidate = false
			}
			innerBlocks = append(innerBlocks, jsontext.Value(rawItem))

		case ir.PartMedia:
			plainCandidate = false
			singlePlainText = nil
			if child.Media == nil {
				continue
			}
			rawMedia, ok, err := serializeMessagesMediaBlock(*child.Media, child.Cache, child.Extra, cPath, losses)
			if err != nil {
				return nil, err
			}
			if ok {
				innerBlocks = append(innerBlocks, rawMedia)
			}
		}
	}

	// Emit compact string content for empty or single unannotated plain-text
	// outputs, and use inner block arrays whenever media, cache_control, or Extra exist.
	var contentVal jsontext.Value
	switch {
	case len(innerBlocks) == 0:
		contentVal = jsontext.Value(`""`)
	case len(innerBlocks) == 1 && singlePlainText != nil:
		rawStr, err := marshalDeterministic(*singlePlainText)
		if err != nil {
			return nil, err
		}
		contentVal = jsontext.Value(rawStr)
	default:
		rawArr, err := marshalDeterministic(innerBlocks)
		if err != nil {
			return nil, err
		}
		contentVal = jsontext.Value(rawArr)
	}

	rawBlock, err := marshalDeterministic(wireToolResultBlock{
		Type:         "tool_result",
		ToolUseID:    slot.result.CallID,
		Content:      contentVal,
		IsError:      slot.result.IsError,
		CacheControl: cache,
		Unknown:      unknown,
	})
	return jsontext.Value(rawBlock), err
}

// appendOrMergeStagedMessage appends next onto staged or merges it into the
// trailing message when roles match. Merged user turns keep tool_result blocks
// ahead of non-tool_result blocks and merged assistant turns keep thinking
// blocks ahead of tool_use blocks, recording LossPartsReordered when reordered.
func appendOrMergeStagedMessage(staged *[]stagedWireMessage, next stagedWireMessage, losses *[]ir.Loss) {
	if len(*staged) == 0 || (*staged)[len(*staged)-1].role != next.role {
		*staged = append(*staged, next)
		return
	}

	last := &(*staged)[len(*staged)-1]
	last.unknown = mergeUnknownMaps(last.unknown, next.unknown)
	if last.msgPath == "" {
		last.msgPath = next.msgPath
	}
	last.blocks = append(last.blocks, next.blocks...)

	if last.role == "user" {
		if enforceUserToolResultsFirst(last.blocks) && !last.reordered && last.msgPath != "" {
			last.reordered = true
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossPartsReordered,
				Path: last.msgPath,
			})
		}
		return
	}
	if enforceAssistantThinkingBeforeTools(last.blocks) && !last.reordered && last.msgPath != "" {
		last.reordered = true
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossPartsReordered,
			Path: last.msgPath,
		})
	}
}

// enforceUserToolResultsFirst moves all tool_result blocks ahead of ordinary
// text and media blocks inside a merged user message. It returns true only when
// at least one block changed position.
func enforceUserToolResultsFirst(blocks []serializedBlock) bool {
	sawNonResult := false
	needsFix := false
	for _, b := range blocks {
		if b.blockType != "tool_result" {
			sawNonResult = true
		} else if sawNonResult {
			needsFix = true
			break
		}
	}
	if !needsFix {
		return false
	}

	rebuilt := make([]serializedBlock, 0, len(blocks))
	for _, b := range blocks {
		if b.blockType == "tool_result" {
			rebuilt = append(rebuilt, b)
		}
	}
	for _, b := range blocks {
		if b.blockType != "tool_result" {
			rebuilt = append(rebuilt, b)
		}
	}
	copy(blocks, rebuilt)
	return true
}

// needsActiveToolLoopThinkingDowngrade reports whether messages ends with an
// active [assistant(tool_use), user(tool_result)] loop whose assistant turn
// lacks a leading thinking block. Non-passback Anthropic endpoints reject
// top-level enabled or adaptive thinking on such unsigned active tool loops.
func needsActiveToolLoopThinkingDowngrade(messages []wireOutMessage) bool {
	if len(messages) < 2 {
		return false
	}
	lastAsstIdx := -1
	lastAsstStartsWithThinking := false
	for i, m := range messages {
		if m.Role != "assistant" {
			continue
		}
		hasToolUse := false
		startsWithThinking := false
		for bIdx, rawBlk := range m.Content {
			var probe wireTypeProbe
			if unmarshalJSONValue(rawBlk, &probe) != nil {
				continue
			}
			if bIdx == 0 && (probe.Type == "thinking" || probe.Type == "redacted_thinking") {
				startsWithThinking = true
			}
			if probe.Type == "tool_use" {
				hasToolUse = true
			}
		}
		if hasToolUse {
			lastAsstIdx = i
			lastAsstStartsWithThinking = startsWithThinking
		}
	}
	return lastAsstIdx >= 0 && lastAsstIdx == len(messages)-2 && !lastAsstStartsWithThinking
}

// flattenMessagesToolName flattens a scoped namespace and bare tool name into a
// deterministic Messages-compatible tool identifier bounded by maxBytes.
// Default "functions" namespaces keep bare names when already within maxBytes.
func flattenMessagesToolName(namespace, name string, maxBytes int) string {
	if maxBytes <= 0 {
		maxBytes = defaultMessagesToolNameMaxBytes
	}
	combined := name
	if namespace != "" && namespace != "functions" {
		combined = namespace + "__" + name
	}
	sanitized := sanitizeMessagesToolChars(combined)
	if len(sanitized) <= maxBytes {
		return sanitized
	}
	// Append an 8-hex-digit SHA-256 suffix over namespace and name when truncating
	// oversized tool names so distinct long names never collide after shortening.
	sum := sha256.Sum256([]byte(namespace + "\x00" + name))
	suffix := "__" + hex.EncodeToString(sum[:4])
	if maxBytes <= len(suffix) {
		return hex.EncodeToString(sum[:])[:maxBytes]
	}
	prefixLen := maxBytes - len(suffix)
	return sanitized[:prefixLen] + suffix
}

// sanitizeMessagesToolChars replaces bytes outside [a-zA-Z0-9_-] with '_' so
// flattened tool identifiers always satisfy Anthropic's tool name pattern.
// Empty inputs fall back to "tool" so the returned identifier is non-empty.
func sanitizeMessagesToolChars(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := range len(s) {
		c := s[i]
		isAlpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := c >= '0' && c <= '9'
		if isAlpha || isDigit || c == '_' || c == '-' {
			_ = b.WriteByte(c)
		} else {
			_ = b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "tool"
	}
	return b.String()
}

// isLoweredToolRef reports whether ref requires lowering into a standard function identity.
func isLoweredToolRef(ref ir.ToolRef) bool {
	return ref.Kind == ir.ToolCustom ||
		ref.Kind == ir.ToolClientSearch ||
		(ref.Namespace != "" && ref.Namespace != "functions")
}
