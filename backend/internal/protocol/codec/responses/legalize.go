package responses

import (
	"bytes"
	"encoding/json/jsontext"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// legalize.go flattens normalized IR conversation messages into valid OpenAI
// Responses input[] items while enforcing bijective call_id pairing, reasoning
// follower ordering, dialect tool lowering, and lossless preservation of
// same-origin item identifiers and Extra maps.
//
// Responses represents conversation history as a flat sequence of message,
// reasoning, call, and call_output objects rather than role-grouped turns.
// Legalization pre-scans tool calls and outputs across turns to pair correlated
// items without reordering surviving *_output entries, rewrites empty or reused
// call_ids deterministically, moves stranded reasoning items ahead of an
// associated assistant follower in the same turn with LossPartsReordered, and
// drops orphan calls, orphan outputs, or followerless reasoning with explicit
// original-coordinate ir.Loss records.

const (
	// customToolInputSchemaJSON defines the single-field string JSON Schema used
	// when lowering free-form ToolCustom declarations for function-only dialects.
	customToolInputSchemaJSON = `{"type":"object","properties":{"input":{"type":"string"}},"required":["input"],"additionalProperties":false}`

	// toolSearchInputSchemaJSON defines the query/limit JSON Schema used when
	// lowering client tool_search declarations for function-only dialects.
	toolSearchInputSchemaJSON = `{"type":"object","properties":{"query":{"type":"string"},"limit":{"type":"integer"}},"required":["query"]}`
)

// callPairingPlan stores pre-computed pairing, wire item type, and call_id
// decisions for every PartToolCall and PartToolResult coordinate in a Request.
type callPairingPlan struct {
	calls   map[partCoord]plannedCall   // surviving calls keyed by (msgIdx, partIdx)
	results map[partCoord]plannedResult // surviving results keyed by (msgIdx, partIdx)
}

// partCoord identifies a Part by original (Message index, Part index) coordinates.
type partCoord struct {
	mIdx int // zero-based index in Request.Messages
	pIdx int // zero-based index in Message.Parts
}

// plannedCall holds the resolved wire type, tool name, namespace, arguments,
// and unique call_id for one surviving PartToolCall.
type plannedCall struct {
	wireType  string // function_call, custom_tool_call, or tool_search_call
	callID    string // non-empty globally unique call_id
	namespace string // surviving namespace, or "" when flattened
	name      string // sanitized callable tool name
	arguments string // validated JSON string or custom free text
}

// plannedResult holds the resolved *_output wire type and paired call_id for one surviving PartToolResult.
type plannedResult struct {
	wireType string // function_call_output, custom_tool_call_output, or tool_search_output
	callID   string // correlated call_id matching a preceding plannedCall
}

// stagedInputItem wraps one serialized input[] JSON item with follower metadata
// so reasoning follower invariants can be checked across turn boundaries.
type stagedInputItem struct {
	raw      jsontext.Value // deterministic JSON encoding of the wire item
	kind     string         // wire item type discriminator
	role     string         // message role when kind is "message"
	mIdx     int            // originating Request.Messages index
	pIdx     int            // originating Message.Parts index, or -1 for empty turn
	hasToken bool           // whether reasoning item carried a ReplayToken
}

// effectiveToolSupport returns the active ToolSupport bitmask configured on dialect.
// Zero-valued ToolSupport bitmasks select ir.ToolSupportAllResponses so native
// OpenAI Responses callers preserve custom tools, namespaces, and tool_search.
func effectiveToolSupport(dialect ir.Dialect) ir.ToolSupport {
	if dialect.Tools == 0 {
		return ir.ToolSupportAllResponses
	}
	return dialect.Tools
}

// legalizeSystemInstructions flattens req.System into a top-level instructions
// string joined by newline separators. Dropped cache marks, foreign extras,
// replay tokens, annotations, media attachments, opaque blocks, empty client
// markers, and multi-block merges each append their corresponding ir.Loss entry.
func legalizeSystemInstructions(sys []ir.Part, losses *[]ir.Loss) string {
	if len(sys) == 0 {
		return ""
	}
	var texts []string
	for i, p := range sys {
		sPath := fmt.Sprintf("system[%d]", i)
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: sPath})
		}
		_ = restoreOrDropExtra(p.Extra, sPath, losses)
		switch p.Kind {
		case ir.PartText:
			if p.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: sPath})
			}
			if p.Text == "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossClientMarkerDropped, Path: sPath})
				continue
			}
			if len(p.Annotations) > 0 {
				*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: sPath})
			}
			texts = append(texts, p.Text)
		case ir.PartMedia:
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: sPath})
		case ir.PartOpaque:
			detail := ""
			if p.Opaque != nil {
				detail = p.Opaque.Type
			}
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: sPath, Detail: detail})
		}
	}
	if len(texts) == 0 {
		return ""
	}
	if len(texts) > 1 {
		*losses = append(*losses, ir.Loss{Kind: ir.LossSystemRelocated, Path: "system"})
	}
	return strings.Join(texts, "\n")
}

// legalizeInputItems converts req.Messages into flat OpenAI Responses input[]
// JSON items under dialect without mutating req.
//
// Responses expects bijective call_id pairing between call items and subsequent
// *_output items plus an associated assistant follower after every reasoning
// item. Legalization resolves call pairing across turns first, serializes user,
// system, developer, tool, and assistant turns in chronological order, and
// repairs or drops stranded reasoning items while recording original-path ir.Loss entries.
func legalizeInputItems(req *ir.Request, dialect ir.Dialect, maxNameBytes int, losses *[]ir.Loss) ([]jsontext.Value, error) {
	support := effectiveToolSupport(dialect)
	plan, err := planCallPairing(req.Messages, support, maxNameBytes, losses)
	if err != nil {
		return nil, err
	}

	var staged []stagedInputItem
	for mIdx, msg := range req.Messages {
		mPath := fmt.Sprintf("messages[%d]", mIdx)
		switch msg.Role {
		case ir.RoleUser, ir.RoleSystem, ir.RoleDeveloper:
			item, ok, err := encodeNonAssistantMessageItem(mIdx, msg, mPath, losses)
			if err != nil {
				return nil, err
			}
			if ok {
				staged = append(staged, item)
			}

		case ir.RoleTool:
			if msg.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: mPath + ".id"})
			}
			_ = restoreOrDropExtra(msg.Extra, mPath, losses)
			for pIdx, p := range msg.Parts {
				if p.Kind != ir.PartToolResult || p.Result == nil {
					continue
				}
				coord := partCoord{mIdx: mIdx, pIdx: pIdx}
				pr, keep := plan.results[coord]
				if !keep {
					continue
				}
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				rawOut, err := encodePlannedToolOutputItem(p, pr, pPath, losses)
				if err != nil {
					return nil, err
				}
				staged = append(staged, stagedInputItem{
					raw:  rawOut,
					kind: pr.wireType,
					mIdx: mIdx,
					pIdx: pIdx,
				})
			}

		case ir.RoleAssistant:
			turnItems, err := encodeAssistantTurnItems(mIdx, msg, dialect, plan, losses)
			if err != nil {
				return nil, err
			}
			staged = append(staged, turnItems...)
		}
	}

	finalized := enforceReasoningFollowers(staged, losses)
	out := make([]jsontext.Value, len(finalized))
	for i, it := range finalized {
		out[i] = it.raw
	}
	return out, nil
}

// planCallPairing resolves bijective call-to-output pairing, dialect tool
// lowering, JSON argument validation, and globally unique non-empty call_id
// assignments across msgs before item serialization runs.
//
// Correlated *_output items remain at their original turn coordinates so tool
// output order is never silently rearranged. Orphan calls, orphan or pre-call
// outputs, and malformed JSON function arguments drop with LossToolPairingRepaired,
// while unnamed pairs and multi-turn duplicate call_id reuses receive
// deterministic SHA-256 identifiers reported via LossIDRewritten.
func planCallPairing(msgs []ir.Message, support ir.ToolSupport, maxNameBytes int, losses *[]ir.Loss) (callPairingPlan, error) {
	plan := callPairingPlan{
		calls:   make(map[partCoord]plannedCall),
		results: make(map[partCoord]plannedResult),
	}

	// pendingEntry records an active preceding PartToolCall awaiting its matching *_output item.
	type pendingEntry struct {
		coord        partCoord   // source call coordinates
		origCallID   string      // original Call.CallID before rewrite
		resolvedCall plannedCall // lowered wire call shape
		wasLowered   bool        // whether the originating call lowered into function_call
	}

	var activeUnnamed []pendingEntry
	activeByID := make(map[string]pendingEntry)
	var activeOrder []string

	// matchedPair binds one PartToolCall coordinate to its correlated PartToolResult coordinate.
	type matchedPair struct {
		callCoord   partCoord   // paired call coordinates
		resCoord    partCoord   // paired result coordinates
		origCallID  string      // original shared CallID
		callPlanned plannedCall // resolved wire call shape
		resWireType string      // resolved *_output wire type
	}
	var pairs []matchedPair

	removeActiveID := func(cid string) {
		delete(activeByID, cid)
		for i, v := range activeOrder {
			if v == cid {
				activeOrder = append(activeOrder[:i], activeOrder[i+1:]...)
				return
			}
		}
	}

	for mIdx, msg := range msgs {
		for pIdx, p := range msg.Parts {
			coord := partCoord{mIdx: mIdx, pIdx: pIdx}
			pPath := fmt.Sprintf("messages[%d].parts[%d]", mIdx, pIdx)

			if p.Kind == ir.PartToolCall && p.Call != nil {
				if p.Call.Tool.Kind == ir.ToolHosted {
					if p.Cache != nil {
						*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
					}
					if p.Call.Token != nil {
						*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
					}
					_ = restoreOrDropExtra(p.Extra, pPath, losses)
					*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
					continue
				}

				pc, callLowered, validArgs, err := resolvePlannedCall(p.Call, support, maxNameBytes, pPath, losses)
				if err != nil {
					return plan, err
				}
				if !validArgs {
					if p.Cache != nil {
						*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
					}
					if p.Call.Token != nil {
						*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
					}
					_ = restoreOrDropExtra(p.Extra, pPath, losses)
					*losses = append(*losses, ir.Loss{Kind: ir.LossToolPairingRepaired, Path: pPath, Detail: "invalid_arguments"})
					continue
				}

				entry := pendingEntry{
					coord:        coord,
					origCallID:   p.Call.CallID,
					resolvedCall: pc,
					wasLowered:   callLowered,
				}
				if p.Call.CallID == "" {
					activeUnnamed = append(activeUnnamed, entry)
					continue
				}
				if prev, dupActive := activeByID[p.Call.CallID]; dupActive {
					prevPath := fmt.Sprintf("messages[%d].parts[%d]", prev.coord.mIdx, prev.coord.pIdx)
					recordDroppedToolPartMetadata(msgs[prev.coord.mIdx].Parts[prev.coord.pIdx], prevPath, losses)
					*losses = append(*losses, ir.Loss{Kind: ir.LossToolPairingRepaired, Path: prevPath, Detail: "unmatched_call"})
					removeActiveID(p.Call.CallID)
				}
				activeByID[p.Call.CallID] = entry
				activeOrder = append(activeOrder, p.Call.CallID)
				continue
			}

			if p.Kind == ir.PartToolResult && p.Result != nil {
				cid := p.Result.CallID
				if cid == "" {
					if len(activeUnnamed) == 0 {
						recordDroppedToolPartMetadata(p, pPath, losses)
						*losses = append(*losses, ir.Loss{Kind: ir.LossToolPairingRepaired, Path: pPath, Detail: "orphan_output"})
						continue
					}
					matched := activeUnnamed[0]
					activeUnnamed = activeUnnamed[1:]
					if matched.wasLowered {
						*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: pPath})
					}
					pairs = append(pairs, matchedPair{
						callCoord:   matched.coord,
						resCoord:    coord,
						origCallID:  "",
						callPlanned: matched.resolvedCall,
						resWireType: outputTypeForCallType(matched.resolvedCall.wireType),
					})
					continue
				}

				matched, exists := activeByID[cid]
				if !exists {
					recordDroppedToolPartMetadata(p, pPath, losses)
					*losses = append(*losses, ir.Loss{Kind: ir.LossToolPairingRepaired, Path: pPath, Detail: "orphan_output"})
					continue
				}
				removeActiveID(cid)

				// Record tool_lowered on correlated result parts whenever the paired
				// call lowered (even if ir.Link was not called beforehand on req).
				if matched.wasLowered ||
					(p.Result.Tool.Kind == ir.ToolCustom && !support.Has(ir.ToolSupportCustom)) ||
					(p.Result.Tool.Kind == ir.ToolClientSearch && !support.Has(ir.ToolSupportClientSearch)) ||
					(p.Result.Tool.Namespace != "" && !support.Has(ir.ToolSupportNamespace)) {
					*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: pPath})
				}

				pairs = append(pairs, matchedPair{
					callCoord:   matched.coord,
					resCoord:    coord,
					origCallID:  cid,
					callPlanned: matched.resolvedCall,
					resWireType: outputTypeForCallType(matched.resolvedCall.wireType),
				})
			}
		}
	}

	// Any remaining active calls never received a matching *_output item.
	for _, entry := range activeUnnamed {
		cPath := fmt.Sprintf("messages[%d].parts[%d]", entry.coord.mIdx, entry.coord.pIdx)
		recordDroppedToolPartMetadata(msgs[entry.coord.mIdx].Parts[entry.coord.pIdx], cPath, losses)
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolPairingRepaired, Path: cPath, Detail: "unmatched_call"})
	}
	for _, cid := range activeOrder {
		entry := activeByID[cid]
		cPath := fmt.Sprintf("messages[%d].parts[%d]", entry.coord.mIdx, entry.coord.pIdx)
		recordDroppedToolPartMetadata(msgs[entry.coord.mIdx].Parts[entry.coord.pIdx], cPath, losses)
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolPairingRepaired, Path: cPath, Detail: "unmatched_call"})
	}

	// Keep the first occurrence of each non-empty call_id untouched and rewrite
	// only subsequent duplicate reuse pairs into deterministic unique IDs.
	usedFinalIDs := make(map[string]bool, len(pairs))
	seenOrigID := make(map[string]bool, len(pairs))
	for _, pr := range pairs {
		if pr.origCallID != "" && !seenOrigID[pr.origCallID] {
			seenOrigID[pr.origCallID] = true
			usedFinalIDs[pr.origCallID] = true
		}
	}
	emittedFirstOrig := make(map[string]bool, len(pairs))

	for _, pr := range pairs {
		finalID := pr.origCallID
		cPath := fmt.Sprintf("messages[%d].parts[%d]", pr.callCoord.mIdx, pr.callCoord.pIdx)
		rPath := fmt.Sprintf("messages[%d].parts[%d]", pr.resCoord.mIdx, pr.resCoord.pIdx)

		if finalID == "" {
			seed := fmt.Sprintf("unnamed:%d:%d:%s", pr.callCoord.mIdx, pr.callCoord.pIdx, pr.callPlanned.name)
			finalID = uniqueDerivedCallID(seed, usedFinalIDs)
			*losses = append(*losses,
				ir.Loss{Kind: ir.LossIDRewritten, Path: cPath + ".call_id"},
				ir.Loss{Kind: ir.LossIDRewritten, Path: rPath + ".call_id"},
			)
		} else if !emittedFirstOrig[finalID] {
			emittedFirstOrig[finalID] = true
		} else {
			seed := fmt.Sprintf("dup:%s:%d:%d", finalID, pr.callCoord.mIdx, pr.callCoord.pIdx)
			finalID = uniqueDerivedCallID(seed, usedFinalIDs)
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: cPath + ".call_id"})
		}

		pc := pr.callPlanned
		pc.callID = finalID
		plan.calls[pr.callCoord] = pc
		plan.results[pr.resCoord] = plannedResult{
			wireType: pr.resWireType,
			callID:   finalID,
		}
	}

	return plan, nil
}

// uniqueDerivedCallID derives a deterministic "call_" identifier from seed and
// records the chosen identifier in used before returning it. Astronomical hash
// collisions increment a numeric suffix until an unused identifier is found.
func uniqueDerivedCallID(seed string, used map[string]bool) string {
	for attempt := 0; ; attempt++ {
		s := seed
		if attempt > 0 {
			s = fmt.Sprintf("%s#%d", seed, attempt)
		}
		candidate := deriveDeterministicID("call_", s)
		if !used[candidate] {
			used[candidate] = true
			return candidate
		}
	}
}

// recordDroppedToolPartMetadata records cache, token, and foreign extra losses
// on a tool call or result part that pairing repair tombstoned.
func recordDroppedToolPartMetadata(p ir.Part, path string, losses *[]ir.Loss) {
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	if p.Call != nil && p.Call.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
	}
	_ = restoreOrDropExtra(p.Extra, path, losses)
}

// resolvePlannedCall computes the wire call item type, namespace, sanitized
// name, lowering flag, and validated argument payload for call under support.
// Malformed non-custom JSON arguments return validArgs=false without mutating call.
func resolvePlannedCall(call *ir.ToolCall, support ir.ToolSupport, maxNameBytes int, path string, losses *[]ir.Loss) (plannedCall, bool, bool, error) {
	lowered := false
	ns := call.Tool.Namespace
	name := call.Tool.Name

	if ns != "" && !support.Has(ir.ToolSupportNamespace) {
		name = flattenNamespaceToolName(ns, name, maxNameBytes)
		ns = ""
		lowered = true
	}

	switch call.Tool.Kind {
	case ir.ToolCustom:
		if support.Has(ir.ToolSupportCustom) {
			sanitized, changed := sanitizeResponsesToolName(name, maxNameBytes)
			if changed {
				lowered = true
			}
			if lowered {
				*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: path})
			}
			return plannedCall{
				wireType:  "custom_tool_call",
				namespace: ns,
				name:      sanitized,
				arguments: call.Arguments,
			}, lowered, true, nil
		}
		sanitized, _ := sanitizeResponsesToolName(name, maxNameBytes)
		wrappedBytes, err := marshalDeterministic(map[string]string{"input": call.Arguments})
		if err != nil {
			return plannedCall{}, false, false, err
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: path})
		return plannedCall{
			wireType:  "function_call",
			namespace: ns,
			name:      sanitized,
			arguments: string(wrappedBytes),
		}, true, true, nil

	case ir.ToolClientSearch:
		args := strings.TrimSpace(call.Arguments)
		if args != "" && !isValidSingleJSON([]byte(args)) {
			return plannedCall{}, false, false, nil
		}
		if support.Has(ir.ToolSupportClientSearch) {
			return plannedCall{
				wireType:  "tool_search_call",
				arguments: args,
			}, false, true, nil
		}
		if args == "" {
			args = "{}"
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: path})
		return plannedCall{
			wireType:  "function_call",
			name:      "tool_search",
			arguments: args,
		}, true, true, nil

	default:
		sanitized, changed := sanitizeResponsesToolName(name, maxNameBytes)
		if changed {
			lowered = true
		}
		args := call.Arguments
		if strings.TrimSpace(args) == "" {
			args = "{}"
		} else if !isValidSingleJSON([]byte(args)) {
			return plannedCall{}, false, false, nil
		}
		if lowered {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: path})
		}
		return plannedCall{
			wireType:  "function_call",
			namespace: ns,
			name:      sanitized,
			arguments: args,
		}, lowered, true, nil
	}
}

// outputTypeForCallType returns the matching *_output wire discriminator for callType.
func outputTypeForCallType(callType string) string {
	switch callType {
	case "custom_tool_call":
		return "custom_tool_call_output"
	case "tool_search_call":
		return "tool_search_output"
	default:
		return "function_call_output"
	}
}

// encodeNonAssistantMessageItem serializes one user, system, or developer Message
// into a Responses message item while preserving Message.ID and Message.Extra.
// Source turns whose parts were all dropped return ok=false after recording metadata losses.
func encodeNonAssistantMessageItem(mIdx int, msg ir.Message, mPath string, losses *[]ir.Loss) (stagedInputItem, bool, error) {
	msgUnknown := restoreOrDropExtra(msg.Extra, mPath, losses)
	blocks := make([]jsontext.Value, 0, len(msg.Parts))
	hadSourceParts := len(msg.Parts) > 0

	for pIdx, p := range msg.Parts {
		pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
		block, ok, err := encodeInputMessagePart(p, msg.Role, pPath, losses)
		if err != nil {
			return stagedInputItem{}, false, err
		}
		if ok {
			blocks = append(blocks, block)
		}
	}

	if hadSourceParts && len(blocks) == 0 {
		if msg.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: mPath + ".id"})
		}
		if len(msgUnknown) > 0 {
			*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: mPath})
		}
		return stagedInputItem{}, false, nil
	}

	contentBytes, err := marshalDeterministic(blocks)
	if err != nil {
		return stagedInputItem{}, false, err
	}
	rawItem, err := marshalDeterministic(wireMessageItem{
		ID:      msg.ID,
		Type:    "message",
		Role:    string(msg.Role),
		Content: jsontext.Value(contentBytes),
		Unknown: msgUnknown,
	})
	if err != nil {
		return stagedInputItem{}, false, err
	}
	return stagedInputItem{
		raw:  jsontext.Value(rawItem),
		kind: "message",
		role: string(msg.Role),
		mIdx: mIdx,
		pIdx: -1,
	}, true, nil
}

// encodeInputMessagePart serializes one content Part inside a message or tool
// result array according to role. Unsupported media, foreign opaque items, and
// misplaced reasoning blocks return ok=false after appending their ir.Loss records.
func encodeInputMessagePart(p ir.Part, role ir.Role, path string, losses *[]ir.Loss) (jsontext.Value, bool, error) {
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	pUnknown := restoreOrDropExtra(p.Extra, path, losses)

	switch p.Kind {
	case ir.PartText:
		if p.Token != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
		}
		partType := "input_text"
		var anns []wireAnnotation
		if role == ir.RoleAssistant {
			partType = "output_text"
			for aIdx, a := range p.Annotations {
				aPath := fmt.Sprintf("%s.annotations[%d]", path, aIdx)
				anns = append(anns, encodeAnnotation(a, aPath, losses))
			}
		} else {
			if p.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
			}
			if len(p.Annotations) > 0 {
				*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: path})
			}
		}
		out := wireOutputTextPartWrite{
			Type:        partType,
			Text:        p.Text,
			Annotations: anns,
			Unknown:     pUnknown,
		}
		encoded, err := marshalDeterministic(out)
		return jsontext.Value(encoded), err == nil, err

	case ir.PartRefusal:
		if p.ID != "" && role != ir.RoleAssistant {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
		}
		out := wireRefusalPartWrite{
			Type:    "refusal",
			Refusal: p.Text,
			Unknown: pUnknown,
		}
		encoded, err := marshalDeterministic(out)
		return jsontext.Value(encoded), err == nil, err

	case ir.PartMedia:
		if p.Media == nil {
			return nil, false, nil
		}
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
		}
		wp, supported, err := buildWireMediaPart(*p.Media, pUnknown)
		if err != nil {
			return nil, false, err
		}
		if !supported {
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: path})
			return nil, false, nil
		}
		encoded, err := marshalDeterministic(wp)
		return jsontext.Value(encoded), err == nil, err

	case ir.PartOpaque:
		if p.ID != "" && role != ir.RoleAssistant {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
		}
		if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolOpenAIResponses {
			return jsontext.Value(bytes.Clone(p.Opaque.Raw)), true, nil
		}
		detail := ""
		if p.Opaque != nil {
			detail = p.Opaque.Type
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: path, Detail: detail})
		return nil, false, nil

	case ir.PartReasoning:
		// Non-assistant messages cannot carry reasoning blocks on Responses wire;
		// emit token and text tombstones so VerifyEssence drops unpacked carrier segments too.
		if p.Reasoning != nil && p.Reasoning.Token != nil && p.Reasoning.Token.Data != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: path})
		return nil, false, nil

	default:
		return nil, false, nil
	}
}

// buildWireMediaPart converts an ir.Media attachment into a Responses input_image,
// input_file, or input_audio content block while preserving unknown vendor members.
// Unsupported video attachments and non-base64 audio sources return supported=false
// without error so callers can record LossMediaUnsupported.
func buildWireMediaPart(m ir.Media, unknown map[string]jsontext.Value) (wireContentPart, bool, error) {
	switch m.Type {
	case ir.MediaImage:
		wp := wireContentPart{
			Type:    "input_image",
			Detail:  m.Detail,
			Unknown: unknown,
		}
		switch m.Source {
		case ir.MediaSourceBase64:
			uri := formatBase64DataURI(m.MIME, "image/png", m.Data)
			urlBytes, err := marshalDeterministic(uri)
			if err != nil {
				return wireContentPart{}, false, err
			}
			wp.ImageURL = jsontext.Value(urlBytes)
		case ir.MediaSourceURL:
			urlBytes, err := marshalDeterministic(m.Data)
			if err != nil {
				return wireContentPart{}, false, err
			}
			wp.ImageURL = jsontext.Value(urlBytes)
		case ir.MediaSourceFileID:
			wp.FileID = m.Data
		default:
			return wireContentPart{}, false, nil
		}
		return wp, true, nil

	case ir.MediaDocument:
		wp := wireContentPart{
			Type:     "input_file",
			Filename: m.Name,
			Unknown:  unknown,
		}
		switch m.Source {
		case ir.MediaSourceBase64:
			wp.FileData = formatBase64DataURI(m.MIME, "application/pdf", m.Data)
		case ir.MediaSourceURL:
			wp.FileURL = m.Data
		case ir.MediaSourceFileID:
			wp.FileID = m.Data
		default:
			return wireContentPart{}, false, nil
		}
		return wp, true, nil

	case ir.MediaAudio:
		if m.Source != ir.MediaSourceBase64 {
			return wireContentPart{}, false, nil
		}
		return wireContentPart{
			Type: "input_audio",
			InputAudio: &wireInputAudio{
				Data:   m.Data,
				Format: audioFormatFromMIME(m.MIME),
			},
			Unknown: unknown,
		}, true, nil

	default:
		return wireContentPart{}, false, nil
	}
}

// encodeAssistantTurnItems flattens one RoleAssistant Message into ordered
// reasoning, message, call, and opaque items for Responses input[]. Originally
// empty assistant turns emit an empty content array so Law Q2 fixed points and
// preceding reasoning followers remain intact.
//
// Consecutive PartText and PartRefusal blocks coalesce into a single assistant
// message item only while their Part.ID values match, flushing a separate
// message item whenever Part.ID changes so multi-message turns round-trip losslessly.
func encodeAssistantTurnItems(mIdx int, msg ir.Message, dialect ir.Dialect, plan callPairingPlan, losses *[]ir.Loss) ([]stagedInputItem, error) {
	mPath := fmt.Sprintf("messages[%d]", mIdx)
	msgUnknown := restoreOrDropExtra(msg.Extra, mPath, losses)

	if len(msg.Parts) == 0 {
		rawEmpty, err := marshalDeterministic(wireMessageItem{
			ID:      msg.ID,
			Type:    "message",
			Role:    "assistant",
			Content: jsontext.Value("[]"),
			Unknown: msgUnknown,
		})
		if err != nil {
			return nil, err
		}
		return []stagedInputItem{{
			raw:  jsontext.Value(rawEmpty),
			kind: "message",
			role: "assistant",
			mIdx: mIdx,
			pIdx: -1,
		}}, nil
	}

	if len(msgUnknown) == 0 && msg.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: mPath + ".id"})
	}

	hasToolCalls := false
	for pIdx := range msg.Parts {
		if _, ok := plan.calls[partCoord{mIdx: mIdx, pIdx: pIdx}]; ok {
			hasToolCalls = true
			break
		}
	}

	var out []stagedInputItem
	var pendingBlocks []jsontext.Value
	var pendingMsgID string
	var pendingPartPaths []string
	usedMsgUnknown := false

	flushMessage := func() error {
		if len(pendingBlocks) == 0 {
			return nil
		}
		contentBytes, err := marshalDeterministic(pendingBlocks)
		if err != nil {
			return err
		}
		itemID := pendingMsgID
		var itemUnk map[string]jsontext.Value
		if !usedMsgUnknown && len(msgUnknown) > 0 {
			if itemID == "" {
				itemID = msg.ID
			} else {
				if msg.ID != "" && msg.ID != itemID {
					*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: mPath + ".id"})
				}
				for _, pp := range pendingPartPaths {
					*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pp + ".id"})
				}
			}
			itemUnk = msgUnknown
			usedMsgUnknown = true
		}
		rawMsg, err := marshalDeterministic(wireMessageItem{
			ID:      itemID,
			Type:    "message",
			Role:    "assistant",
			Content: jsontext.Value(contentBytes),
			Unknown: itemUnk,
		})
		if err != nil {
			return err
		}
		out = append(out, stagedInputItem{
			raw:  jsontext.Value(rawMsg),
			kind: "message",
			role: "assistant",
			mIdx: mIdx,
			pIdx: -1,
		})
		pendingBlocks = nil
		pendingMsgID = ""
		pendingPartPaths = nil
		return nil
	}

	for pIdx, p := range msg.Parts {
		pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
		switch p.Kind {
		case ir.PartText, ir.PartRefusal:
			if len(pendingBlocks) > 0 && p.ID != pendingMsgID {
				if err := flushMessage(); err != nil {
					return nil, err
				}
			}
			block, ok, err := encodeInputMessagePart(p, ir.RoleAssistant, pPath, losses)
			if err != nil {
				return nil, err
			}
			if !ok {
				continue
			}
			if len(pendingBlocks) == 0 {
				pendingMsgID = p.ID
			}
			pendingBlocks = append(pendingBlocks, block)
			pendingPartPaths = append(pendingPartPaths, pPath)

		case ir.PartReasoning:
			if err := flushMessage(); err != nil {
				return nil, err
			}
			item, ok, err := encodeAssistantReasoningItem(mIdx, pIdx, p, dialect, hasToolCalls, pPath, losses)
			if err != nil {
				return nil, err
			}
			if ok {
				out = append(out, item)
			}

		case ir.PartToolCall:
			if err := flushMessage(); err != nil {
				return nil, err
			}
			pc, keep := plan.calls[partCoord{mIdx: mIdx, pIdx: pIdx}]
			if !keep {
				continue
			}
			rawCall, err := encodePlannedCallItem(p, pc, pPath, losses)
			if err != nil {
				return nil, err
			}
			out = append(out, stagedInputItem{
				raw:  rawCall,
				kind: pc.wireType,
				mIdx: mIdx,
				pIdx: pIdx,
			})

		case ir.PartOpaque:
			if err := flushMessage(); err != nil {
				return nil, err
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, losses)
			if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolOpenAIResponses {
				out = append(out, stagedInputItem{
					raw:  jsontext.Value(bytes.Clone(p.Opaque.Raw)),
					kind: p.Opaque.Type,
					mIdx: mIdx,
					pIdx: pIdx,
				})
				continue
			}
			detail := ""
			if p.Opaque != nil {
				detail = p.Opaque.Type
			}
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath, Detail: detail})

		case ir.PartMedia:
			if err := flushMessage(); err != nil {
				return nil, err
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: pPath})
		}
	}

	if err := flushMessage(); err != nil {
		return nil, err
	}
	if !usedMsgUnknown && len(msgUnknown) > 0 {
		*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: mPath})
		if msg.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: mPath + ".id"})
		}
	}
	return out, nil
}

// encodeAssistantReasoningItem serializes one PartReasoning into a Responses
// reasoning item while honoring dialect.ReasoningHistory and writing non-empty
// Token.Data verbatim into encrypted_content. Tokenless items dropped by
// ReasoningHistory return ok=false after recording their losses.
func encodeAssistantReasoningItem(
	mIdx int,
	pIdx int,
	p ir.Part,
	dialect ir.Dialect,
	hasToolCalls bool,
	path string,
	losses *[]ir.Loss,
) (stagedInputItem, bool, error) {
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	if p.Reasoning == nil {
		_ = restoreOrDropExtra(p.Extra, path, losses)
		return stagedInputItem{}, false, nil
	}

	var encToken string
	if p.Reasoning.Token != nil && p.Reasoning.Token.Data != "" {
		encToken = p.Reasoning.Token.Data
	}

	segments := p.Reasoning.Segments
	droppedText := false
	if encToken == "" && (len(segments) > 0 || p.Reasoning.Redacted) {
		switch dialect.ReasoningHistory {
		case ir.HistoryNone:
			*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: path})
			segments = nil
			droppedText = true
		case ir.HistoryToolTurns:
			if !hasToolCalls {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: path})
				segments = nil
				droppedText = true
			}
		}
	}

	if encToken == "" && len(segments) == 0 && droppedText {
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
		}
		_ = restoreOrDropExtra(p.Extra, path, losses)
		return stagedInputItem{}, false, nil
	}

	rUnknown := restoreOrDropExtra(p.Extra, path, losses)
	var extraContent []wireReasoningText
	if rawCnt, hasCnt := rUnknown["content"]; hasCnt {
		delete(rUnknown, "content")
		// Secondary reasoning_text segments stored in Extra["content"] alongside
		// non-empty summaries fall back to nil only if synthetic Extra JSON is invalid.
		_ = unmarshalJSONValue(rawCnt, &extraContent)
	}

	itemID := p.ID
	if itemID == "" {
		seed := fmt.Sprintf("rs:%d:%d:%s:%s", mIdx, pIdx, strings.Join(segments, "\x00"), encToken)
		itemID = deriveDeterministicID("rs_", seed)
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
	}

	summaries := make([]wireSummaryText, 0, len(segments))
	var rawContent []wireReasoningText
	if len(segments) > 0 {
		if p.Reasoning.Summary {
			for _, seg := range segments {
				summaries = append(summaries, wireSummaryText{Type: "summary_text", Text: seg})
			}
		} else {
			rawContent = make([]wireReasoningText, 0, len(segments))
			for _, seg := range segments {
				rawContent = append(rawContent, wireReasoningText{Type: "reasoning_text", Text: seg})
			}
		}
	}
	if len(rawContent) == 0 && len(extraContent) > 0 {
		rawContent = extraContent
	}

	rawBytes, err := marshalDeterministic(wireReasoningItem{
		ID:               itemID,
		Type:             "reasoning",
		Summary:          summaries,
		Content:          rawContent,
		EncryptedContent: encToken,
		Unknown:          rUnknown,
	})
	if err != nil {
		return stagedInputItem{}, false, err
	}
	return stagedInputItem{
		raw:      jsontext.Value(rawBytes),
		kind:     "reasoning",
		mIdx:     mIdx,
		pIdx:     pIdx,
		hasToken: encToken != "",
	}, true, nil
}

// encodePlannedCallItem serializes one surviving PartToolCall into its resolved
// function_call, custom_tool_call, or tool_search_call input item.
func encodePlannedCallItem(p ir.Part, pc plannedCall, path string, losses *[]ir.Loss) (jsontext.Value, error) {
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	if p.Call != nil && p.Call.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
	}
	cUnknown := restoreOrDropExtra(p.Extra, path, losses)

	switch pc.wireType {
	case "custom_tool_call":
		raw, err := marshalDeterministic(wireCustomToolCallItem{
			ID:        p.ID,
			Type:      "custom_tool_call",
			CallID:    pc.callID,
			Namespace: pc.namespace,
			Name:      pc.name,
			Input:     pc.arguments,
			Unknown:   cUnknown,
		})
		return jsontext.Value(raw), err

	case "tool_search_call":
		var rawArgs jsontext.Value
		if pc.arguments != "" {
			rawArgs = compactRawJSON(jsontext.Value(pc.arguments))
		}
		raw, err := marshalDeterministic(wireToolSearchCallItem{
			ID:        p.ID,
			Type:      "tool_search_call",
			CallID:    pc.callID,
			Arguments: rawArgs,
			Unknown:   cUnknown,
		})
		return jsontext.Value(raw), err

	default:
		raw, err := marshalDeterministic(wireFunctionCallItemWrite{
			ID:        p.ID,
			Type:      "function_call",
			CallID:    pc.callID,
			Namespace: pc.namespace,
			Name:      pc.name,
			Arguments: pc.arguments,
			Unknown:   cUnknown,
		})
		return jsontext.Value(raw), err
	}
}

// encodePlannedToolOutputItem serializes one surviving PartToolResult into a
// function_call_output, custom_tool_call_output, or tool_search_output item.
// Compact JSON array strings on tool_search_output populate tools instead of output.
func encodePlannedToolOutputItem(p ir.Part, pr plannedResult, path string, losses *[]ir.Loss) (jsontext.Value, error) {
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	outUnknown := restoreOrDropExtra(p.Extra, path, losses)
	res := p.Result

	if pr.wireType == "tool_search_output" && !hasMapKey(outUnknown, "tools") && !res.IsError &&
		len(res.Content) == 1 && isPlainMetadataFreeText(res.Content[0]) {
		rawTxt := res.Content[0].Text
		if strings.HasPrefix(rawTxt, "[") && isValidSingleJSON([]byte(rawTxt)) &&
			rawTxt == string(compactRawJSON(jsontext.Value(rawTxt))) {
			raw, err := marshalDeterministic(wireCallOutputItem{
				ID:      p.ID,
				Type:    pr.wireType,
				CallID:  pr.callID,
				Tools:   jsontext.Value(rawTxt),
				Unknown: outUnknown,
			})
			return jsontext.Value(raw), err
		}
	}

	outField, err := encodeToolResultOutputValue(res, path, losses)
	if err != nil {
		return nil, err
	}
	raw, err := marshalDeterministic(wireCallOutputItem{
		ID:      p.ID,
		Type:    pr.wireType,
		CallID:  pr.callID,
		Output:  outField,
		Unknown: outUnknown,
	})
	return jsontext.Value(raw), err
}

// encodeToolResultOutputValue serializes res.Content and res.IsError into a
// JSON string or content-part array for a Responses *_output item. Error
// results wrap concatenated text inside a deterministic is_error JSON string.
func encodeToolResultOutputValue(res *ir.ToolResult, path string, losses *[]ir.Loss) (jsontext.Value, error) {
	if res.IsError {
		var errParts []string
		for cIdx, child := range res.Content {
			cPath := fmt.Sprintf("%s.result.content[%d]", path, cIdx)
			if child.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: cPath})
			}
			_ = restoreOrDropExtra(child.Extra, cPath, losses)
			switch child.Kind {
			case ir.PartText:
				errParts = append(errParts, child.Text)
			case ir.PartMedia:
				*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: cPath})
			}
		}
		errStr := strings.Join(errParts, "\n")
		isErrTrue := true
		wrapped, err := marshalDeterministic(wireErrorToolEnvelope{
			Error:   &errStr,
			IsError: &isErrTrue,
		})
		if err != nil {
			return nil, err
		}
		strBytes, err := marshalDeterministic(string(wrapped))
		return jsontext.Value(strBytes), err
	}

	if len(res.Content) == 0 {
		return jsontext.Value(`""`), nil
	}
	if len(res.Content) == 1 && isPlainMetadataFreeText(res.Content[0]) && res.Content[0].Text != "" {
		strBytes, err := marshalDeterministic(res.Content[0].Text)
		return jsontext.Value(strBytes), err
	}

	blocks := make([]jsontext.Value, 0, len(res.Content))
	for cIdx, child := range res.Content {
		cPath := fmt.Sprintf("%s.result.content[%d]", path, cIdx)
		block, ok, err := encodeInputMessagePart(child, ir.RoleTool, cPath, losses)
		if err != nil {
			return nil, err
		}
		if ok {
			blocks = append(blocks, block)
		}
	}
	if len(blocks) == 0 {
		return jsontext.Value(`""`), nil
	}
	arrBytes, err := marshalDeterministic(blocks)
	return jsontext.Value(arrBytes), err
}

// enforceReasoningFollowers guarantees RSP-REASONING-FOLLOWER across flat
// staged input items before final JSON assembly.
//
// OpenAI Responses rejects reasoning items that dangle before a user or tool
// output item or at the end of input[]. When an assistant turn places reasoning
// after its last surviving call or message item, enforceReasoningFollowers
// moves the stranded reasoning item ahead of that turn's follower and records
// LossPartsReordered; turns lacking any surviving assistant follower drop their
// reasoning items with LossReplayTokenDropped and LossReasoningTextDropped.
func enforceReasoningFollowers(items []stagedInputItem, losses *[]ir.Loss) []stagedInputItem {
	if len(items) == 0 {
		return nil
	}
	reorderedMsg := make(map[int]bool)

	// Process contiguous runs belonging to the same assistant message index so
	// trailing or blocked reasoning items relocate before a surviving follower.
	var result []stagedInputItem
	for i := 0; i < len(items); {
		mIdx := items[i].mIdx
		j := i + 1
		for j < len(items) && items[j].mIdx == mIdx {
			j++
		}
		group := items[i:j]
		if !groupHasReasoning(group) {
			result = append(result, group...)
			i = j
			continue
		}

		// Determine whether the next item after this message group already supplies
		// an immediate valid assistant follower across adjacent assistant turns.
		nextProvidesFollower := stagedHasFollowerFrom(items, j)
		fixedGroup, reordered := fixAssistantGroupReasoning(group, nextProvidesFollower, losses)
		if reordered && !reorderedMsg[mIdx] {
			reorderedMsg[mIdx] = true
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossPartsReordered,
				Path: fmt.Sprintf("messages[%d]", mIdx),
			})
		}
		result = append(result, fixedGroup...)
		i = j
	}
	return result
}

// groupHasReasoning reports whether group contains at least one reasoning item.
func groupHasReasoning(group []stagedInputItem) bool {
	for _, it := range group {
		if it.kind == "reasoning" {
			return true
		}
	}
	return false
}

// fixAssistantGroupReasoning ensures every reasoning item in group precedes a
// valid assistant follower either inside group or via nextProvidesFollower.
// It reports whether any reasoning item moved ahead of a preceding item in group.
func fixAssistantGroupReasoning(group []stagedInputItem, nextProvidesFollower bool, losses *[]ir.Loss) ([]stagedInputItem, bool) {
	allSatisfied := true
	for idx := range group {
		if group[idx].kind == "reasoning" && !groupHasFollowerAfter(group, idx, nextProvidesFollower) {
			allSatisfied = false
			break
		}
	}
	if allSatisfied {
		return group, false
	}

	lastFollowerIdx := -1
	for idx, it := range group {
		if isValidAssistantFollower(it) {
			lastFollowerIdx = idx
		}
	}
	if lastFollowerIdx < 0 && !nextProvidesFollower {
		kept := make([]stagedInputItem, 0, len(group))
		for _, it := range group {
			if it.kind != "reasoning" {
				kept = append(kept, it)
				continue
			}
			dropFollowerlessReasoningLoss(it, losses)
		}
		return kept, false
	}

	// Place buffered reasoning items immediately ahead of the next valid follower
	// (or ahead of lastFollowerIdx when trailing after the last follower).
	rebuilt := make([]stagedInputItem, 0, len(group))
	var pendingReasoning []stagedInputItem
	for _, it := range group {
		if it.kind == "reasoning" {
			pendingReasoning = append(pendingReasoning, it)
			continue
		}
		if isValidAssistantFollower(it) && len(pendingReasoning) > 0 {
			rebuilt = append(rebuilt, pendingReasoning...)
			pendingReasoning = nil
		}
		rebuilt = append(rebuilt, it)
	}
	if len(pendingReasoning) > 0 {
		if nextProvidesFollower {
			rebuilt = append(rebuilt, pendingReasoning...)
		} else {
			insertAt := -1
			for idx, it := range rebuilt {
				if isValidAssistantFollower(it) {
					insertAt = idx
				}
			}
			if insertAt >= 0 {
				tail := append([]stagedInputItem(nil), rebuilt[insertAt:]...)
				rebuilt = append(rebuilt[:insertAt], pendingReasoning...)
				rebuilt = append(rebuilt, tail...)
			}
		}
	}
	return rebuilt, true
}

// dropFollowerlessReasoningLoss records token and text losses for a reasoning
// item omitted because no assistant output or call item followed it. Always
// emitting LossReasoningTextDropped clears visible segments, empty placeholders,
// and carrier-unpacked text without inspecting trc1 envelopes inside the codec.
func dropFollowerlessReasoningLoss(it stagedInputItem, losses *[]ir.Loss) {
	pPath := fmt.Sprintf("messages[%d].parts[%d]", it.mIdx, it.pIdx)
	if it.hasToken {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
	}
	*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
}

// groupHasFollowerAfter reports whether group[start+1:] reaches a valid
// assistant follower without hitting a non-follower non-reasoning item.
func groupHasFollowerAfter(group []stagedInputItem, start int, nextProvidesFollower bool) bool {
	for j := start + 1; j < len(group); j++ {
		if group[j].kind == "reasoning" {
			continue
		}
		return isValidAssistantFollower(group[j])
	}
	return nextProvidesFollower
}

// stagedHasFollowerFrom reports whether items[start:] begins with zero or more
// reasoning items followed immediately by a valid assistant follower.
func stagedHasFollowerFrom(items []stagedInputItem, start int) bool {
	for j := start; j < len(items); j++ {
		if items[j].kind == "reasoning" {
			continue
		}
		return isValidAssistantFollower(items[j])
	}
	return false
}

// isValidAssistantFollower matches the assistant follower item set enforced by
// testkit.ValidateResponsesRequest (RSP-REASONING-FOLLOWER).
func isValidAssistantFollower(it stagedInputItem) bool {
	switch it.kind {
	case "message":
		return it.role == "assistant"
	case "function_call", "custom_tool_call", "tool_search_call",
		"web_search_call", "mcp_call", "image_generation_call", "local_shell_call":
		return true
	default:
		return false
	}
}

// encodeAnnotation converts an ir.Annotation into a Responses wireAnnotation.
func encodeAnnotation(a ir.Annotation, path string, losses *[]ir.Loss) wireAnnotation {
	kind := a.Kind
	if kind == "" {
		kind = "url_citation"
	}
	return wireAnnotation{
		Type:       kind,
		URL:        a.URL,
		Title:      a.Title,
		StartIndex: a.StartIndex,
		EndIndex:   a.EndIndex,
		Unknown:    restoreOrDropExtra(a.Extra, path, losses),
	}
}

// isPlainMetadataFreeText reports whether p is a PartText carrying no wire ID,
// annotations, replay token, cache breakpoint, or Extra members.
func isPlainMetadataFreeText(p ir.Part) bool {
	return p.Kind == ir.PartText &&
		p.ID == "" &&
		len(p.Annotations) == 0 &&
		p.Token == nil &&
		p.Cache == nil &&
		p.Extra.IsZero()
}

// hasMapKey reports whether m is non-nil and contains key.
func hasMapKey(m map[string]jsontext.Value, key string) bool {
	if len(m) == 0 {
		return false
	}
	_, ok := m[key]
	return ok
}

// insertStringField marshals val as a JSON string and inserts it into m under key.
func insertStringField(m map[string]jsontext.Value, key, val string) map[string]jsontext.Value {
	encoded, err := marshalDeterministic(val)
	if err != nil {
		return m
	}
	return insertRawMember(m, key, jsontext.Value(encoded))
}
