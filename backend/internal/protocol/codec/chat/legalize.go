package chat

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"fmt"
	"maps"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// legalize.go normalizes IR conversation turns into valid OpenAI Chat
// Completions message sequences while recording exact original-index ir.Loss
// entries for every dropped, relocated, lowered, or reordered element.
//
// Out-of-band System instructions fold into a leading system message, adjacent
// assistant turns coalesce under non-empty Dialect.ReasoningHistory so split
// cross-protocol commentary and reasoning stay attached to their tool_calls, and
// assistant tool calls with malformed JSON arguments or missing tool replies are
// pruned alongside orphan results. Tool-result attachments relocate into
// attributed following user turns, while empty ReasoningHistory preserves
// consecutive Chat assistant messages verbatim for single-protocol Law Q2 fixed points.

const (
	// customToolInputSchemaJSON defines the {"input": "..."} wrapper schema used
	// when a standalone Chat request encodes an unlowered custom tool.
	customToolInputSchemaJSON = `{"type":"object","properties":{"input":{"type":"string","description":"The raw input for this tool, passed through verbatim."}},"required":["input"]}`

	// toolSearchProxyName and toolSearchProxySchemaJSON define the function proxy
	// used when a standalone Chat request encodes an unlowered tool_search tool.
	toolSearchProxyName       = "tool_search"
	toolSearchProxySchemaJSON = `{"type":"object","properties":{"query":{"type":"string","description":"Search query for tools or connectors to load."},"limit":{"type":"integer","description":"Maximum number of tool groups to return."}},"required":["query"]}`

	// defaultChatToolNameMaxBytes bounds flattened namespace function names to
	// OpenAI Chat Completions' 64-byte function identifier limit.
	defaultChatToolNameMaxBytes = 64
)

// partGroupOrder records a surviving part's original slice index and its wire
// serialization group rank (0=reasoning, 1=content, 2=refusal, 3=tool_calls).
type partGroupOrder struct {
	origOrder int // original part index inside source slice
	groupRank int // target Chat wire group rank
}

// trackedReqPart pairs one source ir.Part with its original IR coordinate path.
type trackedReqPart struct {
	part      ir.Part // cloned or source IR part value
	path      string  // original IR path such as "messages[2].parts[1]"
	origOrder int     // monotonic ordinal across a single or coalesced turn
}

// trackedToolCallCandidate holds a validated assistant tool call awaiting reply pairing.
type trackedToolCallCandidate struct {
	callID    string                    // non-empty correlation identifier
	name      string                    // target Chat function name
	arguments string                    // validated JSON argument string
	unknown   map[string]jsontext.Value // preserved same-origin Extra members
	path      string                    // original IR part path
	origOrder int                       // monotonic ordinal across a single or coalesced assistant turn
}

// trackedToolResultSlot holds one split tool_result turn item during pairing resolution.
type trackedToolResultSlot struct {
	partPath string        // original IR path of the PartToolResult
	msgPath  string        // original IR path of the owning RoleTool message
	msgID    string        // owning message ID (only on first split item)
	msgExtra ir.Extra      // owning message Extra (only on first split item)
	result   ir.ToolResult // correlated tool result payload
	claimed  bool          // true once paired to a preceding assistant tool call
	dropped  bool          // true when invalidated by a malformed call
}

// stageTurnMeta retains turn-level ID and Extra coordinates for one source
// message participating in an assistant stageTurn.
type stageTurnMeta struct {
	path  string   // original IR message coordinate such as "messages[1]"
	id    string   // original wire message ID
	extra ir.Extra // original message Extra members
}

// stageTurn represents one turn in the intermediate legalization pipeline.
type stageTurn struct {
	role          ir.Role                    // conversation turn speaker role
	path          string                     // original message path ("" for relocated System)
	id            string                     // original wire message ID
	extra         ir.Extra                   // original message Extra members
	mergedMeta    []stageTurnMeta            // source message metadata across coalesced assistant turns
	seenCallInMsg map[string]bool            // per-turn tool_call ID deduplication set
	nextItemOrder int                        // monotonic item ordinal within an assistant turn
	parts         []trackedReqPart           // non-tool-result parts on the turn
	candidates    []trackedToolCallCandidate // pre-validated assistant tool call candidates
	toolSlot      *trackedToolResultSlot     // single tool result when role is RoleTool
}

// legalizeMessages converts req.System and req.Messages into legal Chat
// wireOutMessage entries under dialect and appends all incurred Loss records onto
// losses. When dialect.ReasoningHistory is non-empty, adjacent RoleAssistant
// messages coalesce into one turn before pairing so split cross-protocol
// reasoning, commentary, and tool calls stay together; empty ReasoningHistory
// leaves consecutive Chat assistant messages separate to preserve single-protocol
// Law Q2 round-trip fixed points.
//
// Multi-result RoleTool turns first split into individual tool slots carrying
// their original message and part coordinates so invalid-argument calls,
// duplicate CallIDs, and orphan tool replies report exact source paths before
// pairAndEncodeStages resolves turn-level pairing and media relocation.
func legalizeMessages(req *ir.Request, dialect ir.Dialect, losses *[]ir.Loss) ([]wireOutMessage, error) {
	maxNameBytes := defaultChatToolNameMaxBytes
	if dialect.ToolNameMaxBytes > 0 {
		maxNameBytes = dialect.ToolNameMaxBytes
	}
	coalesceAssistant := dialect.ReasoningHistory != ""

	var stages []stageTurn
	if len(req.System) > 0 {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossSystemRelocated,
			Path: "system",
		})
		sysParts := make([]trackedReqPart, len(req.System))
		for i, p := range req.System {
			sysParts[i] = trackedReqPart{
				part: p,
				path: fmt.Sprintf("system[%d]", i),
			}
		}
		stages = append(stages, stageTurn{
			role:  ir.RoleSystem,
			parts: sysParts,
		})
	}

	invalidCallIDs := make(map[string]bool)
	for mIdx, msg := range req.Messages {
		mPath := fmt.Sprintf("messages[%d]", mIdx)
		if msg.Role == ir.RoleTool {
			for pIdx, p := range msg.Parts {
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				if p.Kind != ir.PartToolResult || p.Result == nil {
					continue
				}
				recordPartMetadataLosses(p, pPath, true, losses)
				_ = restoreOrDropExtra(p.Extra, pPath, losses)
				var mID string
				var mExtra ir.Extra
				if pIdx == 0 {
					mID = msg.ID
					mExtra = msg.Extra
				}
				slot := &trackedToolResultSlot{
					partPath: pPath,
					msgPath:  mPath,
					msgID:    mID,
					msgExtra: mExtra,
					result:   *p.Clone().Result,
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
					role:     ir.RoleTool,
					path:     mPath,
					toolSlot: slot,
				})
			}
			continue
		}

		if msg.Role == ir.RoleAssistant {
			// Coalesce adjacent assistant turns only when ReasoningHistory is active so
			// single-protocol Chat round trips keep explicit consecutive assistant turns.
			var target *stageTurn
			meta := stageTurnMeta{
				path:  mPath,
				id:    msg.ID,
				extra: msg.Extra,
			}
			if coalesceAssistant && len(stages) > 0 && stages[len(stages)-1].role == ir.RoleAssistant {
				target = &stages[len(stages)-1]
				target.mergedMeta = append(target.mergedMeta, meta)
			} else {
				stages = append(stages, stageTurn{
					role:          ir.RoleAssistant,
					path:          mPath,
					id:            msg.ID,
					extra:         msg.Extra,
					mergedMeta:    []stageTurnMeta{meta},
					seenCallInMsg: make(map[string]bool),
				})
				target = &stages[len(stages)-1]
			}
			for pIdx, p := range msg.Parts {
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				ord := target.nextItemOrder
				target.nextItemOrder++
				if p.Kind == ir.PartToolCall && p.Call != nil {
					cand, ok, err := prepareAssistantToolCall(p, pPath, ord, maxNameBytes, target.seenCallInMsg, invalidCallIDs, losses)
					if err != nil {
						return nil, err
					}
					if ok {
						target.candidates = append(target.candidates, cand)
					}
					continue
				}
				target.parts = append(target.parts, trackedReqPart{
					part:      p,
					path:      pPath,
					origOrder: ord,
				})
			}
			continue
		}

		st := stageTurn{
			role:  msg.Role,
			path:  mPath,
			id:    msg.ID,
			extra: msg.Extra,
		}
		for pIdx, p := range msg.Parts {
			pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
			st.parts = append(st.parts, trackedReqPart{
				part:      p,
				path:      pPath,
				origOrder: pIdx,
			})
		}
		stages = append(stages, st)
	}

	return pairAndEncodeStages(stages, dialect, losses)
}

// prepareAssistantToolCall validates and normalizes one assistant PartToolCall
// candidate, returning ok=false when the call must be pruned from the turn.
//
// Calls with blank or duplicate per-message CallIDs, hosted tool targets, or
// malformed JSON arguments are dropped and registered in invalidCallIDs so
// matching downstream tool_result turns also prune cleanly. Custom, search, and
// namespaced calls lower into valid JSON-object function calls with LossToolLowered.
func prepareAssistantToolCall(
	p ir.Part,
	pPath string,
	origOrder int,
	maxNameBytes int,
	seenInMsg map[string]bool,
	invalidCallIDs map[string]bool,
	losses *[]ir.Loss,
) (trackedToolCallCandidate, bool, error) {
	call := p.Call
	if call.CallID == "" || seenInMsg[call.CallID] {
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
			Input string `json:"input"` // raw custom tool text payload
		}{Input: args})
		if err != nil {
			return trackedToolCallCandidate{}, false, err
		}
		args = string(wrapped)
		if call.Tool.Namespace != "" {
			name = flattenChatToolName(call.Tool.Namespace, name, maxNameBytes)
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
			name = flattenChatToolName(call.Tool.Namespace, name, maxNameBytes)
			lowered = true
		}
	}

	if strings.TrimSpace(args) == "" {
		args = "{}"
	}
	if !isValidSingleJSON([]byte(args)) {
		invalidCallIDs[call.CallID] = true
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolPairingRepaired,
			Path:   pPath,
			Detail: "invalid_json_arguments",
		})
		return trackedToolCallCandidate{}, false, nil
	}

	if lowered {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolLowered,
			Path: pPath,
		})
	}
	recordPartMetadataLosses(p, pPath, true, losses)
	if call.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
	}
	unknown := restoreOrDropExtra(p.Extra, pPath, losses)
	seenInMsg[call.CallID] = true
	return trackedToolCallCandidate{
		callID:    call.CallID,
		name:      name,
		arguments: args,
		unknown:   unknown,
		path:      pPath,
		origOrder: origOrder,
	}, true, nil
}

// pairAndEncodeStages pairs assistant tool calls with subsequent unclaimed
// tool_result items, relocates tool-result media into user turns, and encodes
// all surviving turns into wireOutMessage values.
//
// Matched replies emit immediately after their owning assistant message in
// tool_calls declaration order so Chat's strict adjacency rule holds even when
// interleaved turns existed upstream. Relocated tool-output media buffers in
// pendingUserPrefix until the next surviving turn, merging into an immediately
// following user message or synthesizing a dedicated user message beforehand.
func pairAndEncodeStages(stages []stageTurn, dialect ir.Dialect, losses *[]ir.Loss) ([]wireOutMessage, error) {
	// resolvedAssistant pairs surviving assistant tool calls with their claimed reply slots.
	type resolvedAssistant struct {
		keptCalls []trackedToolCallCandidate // calls with matched replies
		replies   []*trackedToolResultSlot   // matched replies in keptCalls order
	}
	resolved := make(map[int]resolvedAssistant)

	for i := range stages {
		if stages[i].role != ir.RoleAssistant || len(stages[i].candidates) == 0 {
			continue
		}
		var ra resolvedAssistant
		for _, cand := range stages[i].candidates {
			matched := claimToolReply(stages, i+1, cand.callID, losses)
			if matched == nil {
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossToolPairingRepaired,
					Path: cand.path,
				})
				continue
			}
			ra.keptCalls = append(ra.keptCalls, cand)
			ra.replies = append(ra.replies, matched)
		}
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

	out := make([]wireOutMessage, 0, len(stages))
	var pendingUserPrefix []jsontext.Value

	for i := range stages {
		st := stages[i]
		if st.role == ir.RoleTool {
			continue
		}

		if st.role == ir.RoleAssistant {
			ra := resolved[i]
			asstMsg, keep, err := encodeAssistantStageTurn(st, ra.keptCalls, dialect, losses)
			if err != nil {
				return nil, err
			}
			// Skip pruned empty assistant turns before flushing pendingUserPrefix so
			// relocated tool media still merges into a following surviving user turn.
			if !keep && len(ra.replies) == 0 {
				continue
			}
			if len(pendingUserPrefix) > 0 {
				userMsg, err := buildRelocatedMediaUserMessage(pendingUserPrefix, nil)
				if err != nil {
					return nil, err
				}
				out = append(out, userMsg)
				pendingUserPrefix = nil
			}
			if keep {
				out = append(out, asstMsg)
			}
			for _, reply := range ra.replies {
				toolMsg, relocatedParts, err := encodePairedToolReply(reply, losses)
				if err != nil {
					return nil, err
				}
				out = append(out, toolMsg)
				pendingUserPrefix = append(pendingUserPrefix, relocatedParts...)
			}
			continue
		}

		if len(pendingUserPrefix) > 0 && st.role != ir.RoleUser {
			userMsg, err := buildRelocatedMediaUserMessage(pendingUserPrefix, nil)
			if err != nil {
				return nil, err
			}
			out = append(out, userMsg)
			pendingUserPrefix = nil
		}

		if st.role == ir.RoleUser && len(pendingUserPrefix) > 0 {
			userMsg, err := encodeUserStageWithPrefix(st, pendingUserPrefix, losses)
			if err != nil {
				return nil, err
			}
			out = append(out, userMsg)
			pendingUserPrefix = nil
			continue
		}

		encoded, err := encodeStandardStageTurn(st, losses)
		if err != nil {
			return nil, err
		}
		out = append(out, encoded)
	}

	if len(pendingUserPrefix) > 0 {
		userMsg, err := buildRelocatedMediaUserMessage(pendingUserPrefix, nil)
		if err != nil {
			return nil, err
		}
		out = append(out, userMsg)
	}
	return out, nil
}

// claimToolReply scans forward from startIdx up to the next assistant turn
// redeclaring callID and claims matching unclaimed tool_result slots. When
// duplicate replies exist for one call, earlier slots record LossToolPairingRepaired
// and the final reply wins so reused short IDs across turns stay isolated.
func claimToolReply(stages []stageTurn, startIdx int, callID string, losses *[]ir.Loss) *trackedToolResultSlot {
	var matches []*trackedToolResultSlot
	for j := startIdx; j < len(stages); j++ {
		if stages[j].role == ir.RoleAssistant {
			redeclares := false
			for _, c := range stages[j].candidates {
				if c.callID == callID {
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

// encodeStandardStageTurn serializes a system, developer, or user turn into a
// wireOutMessage. Non-empty IR message IDs record LossIDRewritten because Chat
// request messages have no wire id field.
func encodeStandardStageTurn(st stageTurn, losses *[]ir.Loss) (wireOutMessage, error) {
	if st.id != "" && st.path != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: st.path + ".id"})
	}
	unknown := restoreOrDropExtra(st.extra, st.path, losses)

	contentBytes, err := encodeRolePartsContent(st.role, st.parts, losses)
	if err != nil {
		return wireOutMessage{}, err
	}
	return wireOutMessage{
		Role:    string(st.role),
		Content: contentBytes,
		Unknown: unknown,
	}, nil
}

// encodeUserStageWithPrefix prepends relocated tool-output media attribution and
// image parts ahead of an immediately following user turn's own parts. Message
// IDs and Extra fields on the destination user turn are still accounted for.
func encodeUserStageWithPrefix(st stageTurn, prefix []jsontext.Value, losses *[]ir.Loss) (wireOutMessage, error) {
	if st.id != "" && st.path != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: st.path + ".id"})
	}
	unknown := restoreOrDropExtra(st.extra, st.path, losses)
	ownParts, _, err := collectSerializedRoleParts(ir.RoleUser, st.parts, losses)
	if err != nil {
		return wireOutMessage{}, err
	}
	combined := make([]jsontext.Value, 0, len(prefix)+len(ownParts))
	combined = append(combined, prefix...)
	combined = append(combined, ownParts...)
	return buildRelocatedMediaUserMessage(combined, unknown)
}

// buildRelocatedMediaUserMessage wraps serialized content part JSON values into a user wireOutMessage.
func buildRelocatedMediaUserMessage(parts []jsontext.Value, unknown map[string]jsontext.Value) (wireOutMessage, error) {
	rawContent, err := marshalDeterministic(parts)
	if err != nil {
		return wireOutMessage{}, err
	}
	return wireOutMessage{
		Role:    string(ir.RoleUser),
		Content: jsontext.Value(rawContent),
		Unknown: unknown,
	}, nil
}

// encodeRolePartsContent serializes parts for a system, developer, user, or
// tool message. Single non-empty plain text parts serialize as a scalar JSON
// string, empty slices serialize as "", and multi-part or typed content serializes as an array.
func encodeRolePartsContent(role ir.Role, parts []trackedReqPart, losses *[]ir.Loss) (jsontext.Value, error) {
	serialized, singlePlainText, err := collectSerializedRoleParts(role, parts, losses)
	if err != nil {
		return nil, err
	}
	if len(serialized) == 0 {
		return jsontext.Value(`""`), nil
	}
	if singlePlainText != nil {
		raw, err := marshalDeterministic(*singlePlainText)
		return jsontext.Value(raw), err
	}
	raw, err := marshalDeterministic(serialized)
	return jsontext.Value(raw), err
}

// collectSerializedRoleParts converts tracked parts on a non-assistant turn into
// raw Chat content-part JSON items. When the turn contains exactly one non-empty
// text part with no preserved Extra members, onlyText points to its string for scalar encoding.
func collectSerializedRoleParts(role ir.Role, parts []trackedReqPart, losses *[]ir.Loss) ([]jsontext.Value, *string, error) {
	var out []jsontext.Value
	var onlyText *string
	plainCandidate := true

	for _, tp := range parts {
		p := tp.part
		switch p.Kind {
		case ir.PartText:
			recordPartMetadataLosses(p, tp.path, true, losses)
			pUnknown := restoreOrDropExtra(p.Extra, tp.path, losses)
			if len(pUnknown) > 0 || p.Text == "" {
				plainCandidate = false
			}
			item, err := marshalDeterministic(wireContentPart{
				Type:    "text",
				Text:    p.Text,
				Unknown: pUnknown,
			})
			if err != nil {
				return nil, nil, err
			}
			if len(out) == 0 && plainCandidate {
				txt := p.Text
				onlyText = &txt
			} else {
				onlyText = nil
				plainCandidate = false
			}
			out = append(out, jsontext.Value(item))

		case ir.PartMedia:
			plainCandidate = false
			onlyText = nil
			recordPartMetadataLosses(p, tp.path, true, losses)
			pUnknown := restoreOrDropExtra(p.Extra, tp.path, losses)
			if role != ir.RoleUser || p.Media == nil {
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossMediaUnsupported,
					Path:   tp.path,
					Detail: string(role),
				})
				continue
			}
			mediaPart, ok, err := encodeUserMediaPart(*p.Media, pUnknown, tp.path, losses)
			if err != nil {
				return nil, nil, err
			}
			if ok {
				out = append(out, mediaPart)
			}

		case ir.PartOpaque:
			plainCandidate = false
			onlyText = nil
			recordPartMetadataLosses(p, tp.path, true, losses)
			_ = restoreOrDropExtra(p.Extra, tp.path, losses)
			if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolOpenAIChatCompletions {
				out = append(out, p.Opaque.Raw)
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

		default:
			plainCandidate = false
			onlyText = nil
		}
	}

	if len(out) != 1 {
		onlyText = nil
	}
	return out, onlyText, nil
}

// encodeUserMediaPart converts an ir.Media attachment into a serialized Chat
// user content part. Media sources unsupported on Chat, such as URL audio or
// URL documents, return ok=false and append LossMediaUnsupported at path.
func encodeUserMediaPart(m ir.Media, unknown map[string]jsontext.Value, path string, losses *[]ir.Loss) (jsontext.Value, bool, error) {
	switch m.Type {
	case ir.MediaImage:
		var urlStr string
		switch m.Source {
		case ir.MediaSourceBase64:
			urlStr = formatBase64DataURI(m.MIME, "image/png", m.Data)
		case ir.MediaSourceURL:
			urlStr = m.Data
		default:
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossMediaUnsupported,
				Path:   path,
				Detail: "image:" + m.Source,
			})
			return nil, false, nil
		}
		imgRaw, err := marshalDeterministic(wireImageURL{
			URL:    urlStr,
			Detail: m.Detail,
		})
		if err != nil {
			return nil, false, err
		}
		partRaw, err := marshalDeterministic(wireContentPart{
			Type:     "image_url",
			ImageURL: jsontext.Value(imgRaw),
			Unknown:  unknown,
		})
		return jsontext.Value(partRaw), true, err

	case ir.MediaAudio:
		if m.Source != ir.MediaSourceBase64 {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossMediaUnsupported,
				Path:   path,
				Detail: "audio:" + m.Source,
			})
			return nil, false, nil
		}
		partRaw, err := marshalDeterministic(wireContentPart{
			Type: "input_audio",
			InputAudio: &wireInputAudio{
				Data:   m.Data,
				Format: mimeToAudioFormat(m.MIME),
			},
			Unknown: unknown,
		})
		return jsontext.Value(partRaw), true, err

	case ir.MediaDocument:
		var wf wireFile
		wf.Filename = m.Name
		switch m.Source {
		case ir.MediaSourceFileID:
			wf.FileID = m.Data
		case ir.MediaSourceBase64:
			wf.FileData = formatBase64DataURI(m.MIME, "application/pdf", m.Data)
		default:
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossMediaUnsupported,
				Path:   path,
				Detail: "document:" + m.Source,
			})
			return nil, false, nil
		}
		partRaw, err := marshalDeterministic(wireContentPart{
			Type:    "file",
			File:    &wf,
			Unknown: unknown,
		})
		return jsontext.Value(partRaw), true, err

	default:
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossMediaUnsupported,
			Path:   path,
			Detail: string(m.Type),
		})
		return nil, false, nil
	}
}

// mimeToAudioFormat converts an audio MIME type into a Chat input_audio format
// token. Blank or non-"audio/*" MIME strings default to "wav".
func mimeToAudioFormat(mime string) string {
	lower := strings.ToLower(strings.TrimSpace(mime))
	switch lower {
	case "", "audio/wav", "audio/x-wav", "audio/wave":
		return "wav"
	case "audio/mp3", "audio/mpeg":
		return "mp3"
	default:
		if after, ok := strings.CutPrefix(lower, "audio/"); ok && after != "" {
			return after
		}
		return "wav"
	}
}

// encodeAssistantStageTurn serializes an assistant stageTurn and its matched
// tool_calls into a wireOutMessage, returning keep=false when every part on the
// turn was pruned or left empty.
//
// Chat wire messages group reasoning_content, content, refusal, and tool_calls
// into separate fields, so interleaved source parts record LossPartsReordered
// when their original indices cross group ranks. Under HistoryToolTurns,
// assistant turns with surviving tool_calls always emit a string reasoning_content
// field so DeepSeek thinking validation passes even after stripped thoughts.
func encodeAssistantStageTurn(
	st stageTurn,
	keptCalls []trackedToolCallCandidate,
	dialect ir.Dialect,
	losses *[]ir.Loss,
) (wireOutMessage, bool, error) {
	// trackedRefusal retains refusal text and same-origin Extra fields across an assistant turn.
	type trackedRefusal struct {
		text    string                    // refusal message text
		unknown map[string]jsontext.Value // preserved same-origin refusal Extra members
	}

	hasToolCalls := len(keptCalls) > 0
	keepReasoning := shouldRetainAssistantReasoning(dialect.ReasoningHistory, hasToolCalls)

	var orderCheck []partGroupOrder
	var reasoningSegments []string
	var contentItems []jsontext.Value
	var singlePlainText *string
	plainTextCandidate := true
	var refusals []trackedRefusal
	hadDroppedParts := len(st.candidates) > len(keptCalls)

	var firstSurvivingMsgPath string
	noteSurviving := func(partPath string) {
		if firstSurvivingMsgPath == "" {
			firstSurvivingMsgPath = owningMessagePath(partPath)
		}
	}

	for _, tp := range st.parts {
		p := tp.part
		origOrder := tp.origOrder

		switch p.Kind {
		case ir.PartReasoning:
			recordPartMetadataLosses(p, tp.path, true, losses)
			_ = restoreOrDropExtra(p.Extra, tp.path, losses)
			if p.Reasoning != nil && p.Reasoning.Token != nil {
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossReplayTokenDropped,
					Path: tp.path,
				})
			}
			var joined string
			if p.Reasoning != nil {
				joined = strings.Join(p.Reasoning.Segments, "")
			}
			if joined == "" {
				hadDroppedParts = true
				continue
			}
			if !keepReasoning {
				*losses = append(*losses, ir.Loss{
					Kind: ir.LossReasoningTextDropped,
					Path: tp.path,
				})
				hadDroppedParts = true
				continue
			}
			noteSurviving(tp.path)
			reasoningSegments = append(reasoningSegments, joined)
			orderCheck = append(orderCheck, partGroupOrder{origOrder: origOrder, groupRank: 0})

		case ir.PartText:
			recordPartMetadataLosses(p, tp.path, true, losses)
			pUnknown := restoreOrDropExtra(p.Extra, tp.path, losses)
			if len(pUnknown) > 0 || p.Text == "" {
				plainTextCandidate = false
			}
			rawItem, err := marshalDeterministic(wireContentPart{
				Type:    "text",
				Text:    p.Text,
				Unknown: pUnknown,
			})
			if err != nil {
				return wireOutMessage{}, false, err
			}
			if len(contentItems) == 0 && plainTextCandidate {
				txt := p.Text
				singlePlainText = &txt
			} else {
				singlePlainText = nil
				plainTextCandidate = false
			}
			noteSurviving(tp.path)
			contentItems = append(contentItems, jsontext.Value(rawItem))
			orderCheck = append(orderCheck, partGroupOrder{origOrder: origOrder, groupRank: 1})

		case ir.PartRefusal:
			recordPartMetadataLosses(p, tp.path, true, losses)
			pUnknown := restoreOrDropExtra(p.Extra, tp.path, losses)
			noteSurviving(tp.path)
			refusals = append(refusals, trackedRefusal{text: p.Text, unknown: pUnknown})
			orderCheck = append(orderCheck, partGroupOrder{origOrder: origOrder, groupRank: 2})

		case ir.PartMedia:
			recordPartMetadataLosses(p, tp.path, true, losses)
			_ = restoreOrDropExtra(p.Extra, tp.path, losses)
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossMediaUnsupported,
				Path:   tp.path,
				Detail: "assistant",
			})
			hadDroppedParts = true

		case ir.PartOpaque:
			recordPartMetadataLosses(p, tp.path, true, losses)
			_ = restoreOrDropExtra(p.Extra, tp.path, losses)
			if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolOpenAIChatCompletions {
				plainTextCandidate = false
				singlePlainText = nil
				noteSurviving(tp.path)
				contentItems = append(contentItems, p.Opaque.Raw)
				orderCheck = append(orderCheck, partGroupOrder{origOrder: origOrder, groupRank: 1})
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
				hadDroppedParts = true
			}
		}
	}

	if len(contentItems) != 1 {
		singlePlainText = nil
	}

	// Multiple refusal parts, empty refusal strings, or refusals with preserved
	// Extra members serialize inside the content array instead of scalar refusal.
	var refusalScalar string
	if len(refusals) == 1 && refusals[0].text != "" && len(refusals[0].unknown) == 0 {
		refusalScalar = refusals[0].text
	} else if len(refusals) > 0 {
		singlePlainText = nil
		for _, rItem := range refusals {
			rawRef, err := marshalDeterministic(wireContentPart{
				Type:    "refusal",
				Refusal: rItem.text,
				Unknown: rItem.unknown,
			})
			if err != nil {
				return wireOutMessage{}, false, err
			}
			contentItems = append(contentItems, jsontext.Value(rawRef))
		}
	}

	var wireCalls []wireToolCall
	if len(keptCalls) > 0 {
		wireCalls = make([]wireToolCall, 0, len(keptCalls))
	}
	for _, cand := range keptCalls {
		noteSurviving(cand.path)
		orderCheck = append(orderCheck, partGroupOrder{origOrder: cand.origOrder, groupRank: 3})
		rawArgs, err := marshalDeterministic(cand.arguments)
		if err != nil {
			return wireOutMessage{}, false, err
		}
		wireCalls = append(wireCalls, wireToolCall{
			ID:   cand.callID,
			Type: "function",
			Function: wireFunctionCall{
				Name:    cand.name,
				RawArgs: jsontext.Value(rawArgs),
			},
			Unknown: cand.unknown,
		})
	}

	// Prune assistant messages left completely empty so strict Chat request rules
	// never see bodyless assistant turns after empty replies or dropped items.
	hasVisibleContent := len(contentItems) > 0 && (singlePlainText == nil || *singlePlainText != "")
	if !hasVisibleContent && refusalScalar == "" && len(wireCalls) == 0 && (len(reasoningSegments) == 0 || hadDroppedParts) {
		if len(contentItems) == 0 && len(reasoningSegments) == 0 {
			return wireOutMessage{}, false, nil
		}
	}

	var unknown map[string]jsontext.Value
	if len(st.mergedMeta) <= 1 {
		if st.id != "" && st.path != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: st.path + ".id"})
		}
		unknown = restoreOrDropExtra(st.extra, st.path, losses)
	} else {
		// Coalesced assistant turns rewrite turn-level IDs, preserve same-origin Chat
		// Extra members via restoreOrDropExtra, and report LossExtraDropped on foreign origins.
		for _, meta := range st.mergedMeta {
			if meta.id != "" && meta.path != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: meta.path + ".id"})
			}
			unknown = mergeUnknownMaps(unknown, restoreOrDropExtra(meta.extra, meta.path, losses))
		}
	}

	reorderPath := firstSurvivingMsgPath
	if reorderPath == "" {
		reorderPath = st.path
	}
	if wasReordered := checkGroupOrdering(orderCheck); wasReordered && reorderPath != "" {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossPartsReordered,
			Path: reorderPath,
		})
	}

	var reasoningPtr *string
	if len(reasoningSegments) > 0 {
		joined := strings.Join(reasoningSegments, "\n")
		reasoningPtr = &joined
	} else if dialect.ReasoningHistory == ir.HistoryToolTurns && hasToolCalls {
		// DeepSeek thinking mode requires a string reasoning_content field on every
		// assistant message carrying tool_calls even when prior reasoning was stripped.
		empty := ""
		reasoningPtr = &empty
	}

	var contentVal jsontext.Value
	switch {
	case len(contentItems) == 0:
		if hasToolCalls || refusalScalar != "" {
			contentVal = jsontext.Value("null")
		} else {
			contentVal = jsontext.Value(`""`)
		}
	case singlePlainText != nil:
		rawStr, err := marshalDeterministic(*singlePlainText)
		if err != nil {
			return wireOutMessage{}, false, err
		}
		contentVal = jsontext.Value(rawStr)
	default:
		rawArr, err := marshalDeterministic(contentItems)
		if err != nil {
			return wireOutMessage{}, false, err
		}
		contentVal = jsontext.Value(rawArr)
	}

	return wireOutMessage{
		Role:             string(ir.RoleAssistant),
		Content:          contentVal,
		ReasoningContent: reasoningPtr,
		Refusal:          refusalScalar,
		ToolCalls:        wireCalls,
		Unknown:          unknown,
	}, true, nil
}

// encodePairedToolReply encodes one claimed tool_result slot into a role="tool"
// wireOutMessage and returns any relocated user-turn attribution and media parts.
//
// Chat role="tool" messages accept only text content, so supported image,
// audio, or document attachments inside result.Content move into a following
// user message with "[Tool output media for call <id>]" while leaving
// toolOutputMediaMarker inside the role="tool" content and recording LossMediaRelocated.
// Error results wrap joined text inside {"error": "...", "is_error": true}.
func encodePairedToolReply(slot *trackedToolResultSlot, losses *[]ir.Loss) (wireOutMessage, []jsontext.Value, error) {
	if slot.msgID != "" && slot.msgPath != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: slot.msgPath + ".id"})
	}
	unknown := restoreOrDropExtra(slot.msgExtra, slot.msgPath, losses)

	if isLoweredToolRef(slot.result.Tool) {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolLowered,
			Path: slot.partPath,
		})
	}

	var textParts []trackedReqPart
	var relocatedMedia []jsontext.Value
	sawSupportedMedia := false

	for cIdx, child := range slot.result.Content {
		cPath := fmt.Sprintf("%s.result.content[%d]", slot.partPath, cIdx)
		switch child.Kind {
		case ir.PartText:
			textParts = append(textParts, trackedReqPart{part: child, path: cPath})
		case ir.PartMedia:
			recordPartMetadataLosses(child, cPath, true, losses)
			cUnknown := restoreOrDropExtra(child.Extra, cPath, losses)
			if child.Media == nil {
				continue
			}
			mediaJSON, ok, err := encodeUserMediaPart(*child.Media, cUnknown, cPath, losses)
			if err != nil {
				return wireOutMessage{}, nil, err
			}
			if ok {
				sawSupportedMedia = true
				relocatedMedia = append(relocatedMedia, mediaJSON)
			}
		}
	}

	var userBatch []jsontext.Value
	if sawSupportedMedia {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossMediaRelocated,
			Path: slot.partPath,
		})
		attrRaw, err := marshalDeterministic(wireContentPart{
			Type: "text",
			Text: fmt.Sprintf(toolMediaAttributionFmt, slot.result.CallID),
		})
		if err != nil {
			return wireOutMessage{}, nil, err
		}
		userBatch = make([]jsontext.Value, 0, 1+len(relocatedMedia))
		userBatch = append(userBatch, jsontext.Value(attrRaw))
		userBatch = append(userBatch, relocatedMedia...)
	}

	contentVal, err := encodeToolResultContent(textParts, slot.result.IsError, sawSupportedMedia, losses)
	if err != nil {
		return wireOutMessage{}, nil, err
	}

	return wireOutMessage{
		Role:       string(ir.RoleTool),
		ToolCallID: slot.result.CallID,
		Content:    contentVal,
		Unknown:    unknown,
	}, userBatch, nil
}

// encodeToolResultContent serializes surviving tool_result text parts, adding
// toolOutputMediaMarker when media was relocated and wrapping structured errors
// inside the two-key {"error": "...", "is_error": true} envelope.
func encodeToolResultContent(
	textParts []trackedReqPart,
	isError bool,
	sawSupportedMedia bool,
	losses *[]ir.Loss,
) (jsontext.Value, error) {
	if isError {
		var errTexts []string
		for _, tp := range textParts {
			recordPartMetadataLosses(tp.part, tp.path, true, losses)
			_ = restoreOrDropExtra(tp.part.Extra, tp.path, losses)
			errTexts = append(errTexts, tp.part.Text)
		}
		errMsg := strings.Join(errTexts, "\n")
		if sawSupportedMedia {
			if len(errTexts) == 0 {
				errMsg = toolOutputMediaMarker
			} else {
				errMsg = errMsg + "\n" + toolOutputMediaMarker
			}
		}
		envJSON, err := marshalDeterministic(struct {
			Error   string `json:"error"`    // joined tool error message text
			IsError bool   `json:"is_error"` // structured tool error marker
		}{
			Error:   errMsg,
			IsError: true,
		})
		if err != nil {
			return nil, err
		}
		rawStr, err := marshalDeterministic(string(envJSON))
		if err != nil {
			return nil, err
		}
		return jsontext.Value(rawStr), nil
	}

	if !sawSupportedMedia {
		return encodeRolePartsContent(ir.RoleTool, textParts, losses)
	}

	serialized, singlePlainText, err := collectSerializedRoleParts(ir.RoleTool, textParts, losses)
	if err != nil {
		return nil, err
	}
	if len(serialized) == 0 {
		rawMarker, err := marshalDeterministic(toolOutputMediaMarker)
		return jsontext.Value(rawMarker), err
	}
	if len(serialized) == 1 && singlePlainText != nil {
		rawJoined, err := marshalDeterministic(*singlePlainText + "\n" + toolOutputMediaMarker)
		return jsontext.Value(rawJoined), err
	}
	markerPart, err := marshalDeterministic(wireContentPart{
		Type: "text",
		Text: toolOutputMediaMarker,
	})
	if err != nil {
		return nil, err
	}
	serialized = append(serialized, jsontext.Value(markerPart))
	rawArr, err := marshalDeterministic(serialized)
	return jsontext.Value(rawArr), err
}

// shouldRetainAssistantReasoning reports whether mode retains plaintext
// reasoning on an assistant turn with hasToolCalls. Empty mode defaults to all
// so standalone same-protocol round trips preserve existing reasoning_content.
func shouldRetainAssistantReasoning(mode ir.HistoryMode, hasToolCalls bool) bool {
	switch mode {
	case "", ir.HistoryAll:
		return true
	case ir.HistoryToolTurns:
		return hasToolCalls
	case ir.HistoryNone:
		return false
	default:
		return false
	}
}

// recordPartMetadataLosses emits cache_mark_dropped, id_rewritten,
// annotation_dropped, and replay_token_dropped losses for part metadata
// unsupported on Chat wire messages. Callers pass checkID=false when the
// enclosing part kind handles IDs separately.
func recordPartMetadataLosses(p ir.Part, path string, checkID bool, losses *[]ir.Loss) {
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	if checkID && p.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
	}
	if len(p.Annotations) > 0 {
		*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: path})
	}
	if p.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
	}
}

// flattenChatToolName flattens a scoped namespace/name pair into a bounded
// function identifier. Default "" and "functions" namespaces return name
// unchanged, while names exceeding maxBytes truncate on a UTF-8 rune boundary
// and append a deterministic 8-hex SHA-256 suffix.
func flattenChatToolName(namespace, name string, maxBytes int) string {
	if namespace == "" || namespace == "functions" {
		return name
	}
	full := namespace + "__" + name
	if maxBytes <= 0 || len(full) <= maxBytes {
		return full
	}
	sum := sha256.Sum256([]byte(full))
	suffix := "__" + hex.EncodeToString(sum[:4])
	if maxBytes <= len(suffix) {
		return suffix[len(suffix)-maxBytes:]
	}
	prefixLimit := maxBytes - len(suffix)
	var b strings.Builder
	// Truncate on whole UTF-8 runes so multi-byte characters never split mid-sequence
	// before appending the deterministic collision-avoidance hash suffix.
	for _, ch := range full {
		if b.Len()+len(string(ch)) > prefixLimit {
			break
		}
		_, _ = b.WriteRune(ch)
	}
	return b.String() + suffix
}

// isLoweredToolRef reports whether ref represents a custom, client_search, or
// namespaced tool that lowers to a flat function on Chat.
func isLoweredToolRef(ref ir.ToolRef) bool {
	if ref.Kind == ir.ToolCustom || ref.Kind == ir.ToolClientSearch {
		return true
	}
	return ref.Namespace != ""
}

// mergeUnknownMaps merges preserved same-origin Extra fields from extra into
// base, cloning JSON byte slices so caller maps stay unmutated. Empty extra
// maps return base directly, and earlier keys take precedence when two
// coalesced assistant turns declare the same extension key.
func mergeUnknownMaps(base, extra map[string]jsontext.Value) map[string]jsontext.Value {
	if len(extra) == 0 {
		return base
	}
	if len(base) == 0 {
		out := make(map[string]jsontext.Value, len(extra))
		maps.Copy(out, extra)
		for k, v := range out {
			out[k] = jsontext.Value(bytes.Clone(v))
		}
		return out
	}
	merged := make(map[string]jsontext.Value, len(base)+len(extra))
	for k, v := range base {
		merged[k] = jsontext.Value(bytes.Clone(v))
	}
	for k, v := range extra {
		if _, exists := merged[k]; !exists {
			merged[k] = jsontext.Value(bytes.Clone(v))
		}
	}
	return merged
}

// owningMessagePath extracts the leading "messages[n]" segment from partPath so
// coalesced assistant turns attribute reordering losses to their first surviving
// source message. Paths without an indexed message prefix return "".
func owningMessagePath(partPath string) string {
	dot := strings.IndexByte(partPath, '.')
	if dot <= 0 {
		return ""
	}
	prefix := partPath[:dot]
	if !strings.HasPrefix(prefix, "messages[") || !strings.HasSuffix(prefix, "]") {
		return ""
	}
	return prefix
}

// checkGroupOrdering reports whether entries sorted by origOrder violate
// Chat's monotonic [reasoning <= content <= refusal <= tool_calls] group order.
// Slices with zero or one surviving entry always return false.
func checkGroupOrdering(entries []partGroupOrder) bool {
	if len(entries) <= 1 {
		return false
	}
	// Entries may have been appended by part category; check whether any later
	// original part index carried a strictly smaller wire groupRank.
	for i := range entries {
		for j := i + 1; j < len(entries); j++ {
			left, right := entries[i], entries[j]
			if left.origOrder > right.origOrder {
				left, right = right, left
			}
			if left.groupRank > right.groupRank {
				return true
			}
		}
	}
	return false
}
