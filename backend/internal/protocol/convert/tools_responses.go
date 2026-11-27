package convert

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// tools_responses.go implements same-protocol OpenAI Responses client-tool
// lowering and response restoration without full IR round-tripping. Function-only
// Responses endpoints (OpenAI API Key, DeepSeek, Grok, and WS HTTP bridges)
// reject custom grammar tools, scoped namespaces, and built-in tool_search
// items while still requiring unrelated request fields, item-level
// prompt_cache_breakpoint markers, compaction items, raw reasoning tokens, and
// hosted-tool streaming events to pass through untouched.
//
// Ordered raw JSON member manipulation rewrites only tools[], additional_tools,
// tool_choice, and lowered tool call or output items while preserving exact
// numeric tokens and unknown fields, carries LoweredTools across WebSocket
// warmup and continuation turns that omit tools[] or model, and resequences
// native SSE streams when proxy argument deltas are buffered or expanded.

// ResponsesToolStream restores lowered custom, search, and namespaced tool
// calls across native OpenAI Responses SSE frames while maintaining contiguous
// sequence_number values and hosted-tool intermediate events.
type ResponsesToolStream struct {
	tools       *ToolMap                             // immutable lowered tool identity map
	seenSeq     bool                                 // whether initial sequence_number was observed
	nextSeq     int64                                // next contiguous sequence_number to emit
	hasSeq      bool                                 // whether incoming events carry sequence_number
	calls       map[string]*responsesActiveProxyCall // item_id or call_id -> active proxy call
	byOutput    map[int]*responsesActiveProxyCall    // output_index -> active proxy call
	active      []*responsesActiveProxyCall          // open proxy calls in registration order
	semByOutput map[int]bool                         // output_index slots that already emitted Semantic=true
	semByItemID map[string]bool                      // item IDs that already emitted Semantic=true
}

// responsesActiveProxyCall tracks buffered arguments and client identity for
// one open custom or tool_search proxy output item.
type responsesActiveProxyCall struct {
	kind             string          // "custom" or "tool_search"
	upstreamName     string          // upstream proxy function name
	clientName       string          // restored client custom tool name
	clientNamespace  string          // restored client namespace when scoped
	callID           string          // correlation call identifier string
	itemID           string          // upstream output item ID
	clientItemID     string          // retyped ctc_ or tsc_ client item ID
	outputIdx        int             // output_index inside response.output
	hasOutputIdx     bool            // whether output_index was explicitly present
	sawAdded         bool            // whether response.output_item.added was emitted
	emittedInputDone bool            // whether custom_tool_call_input.done was emitted
	argBuf           strings.Builder // accumulated function argument fragments
}

// rawTopToolEntry records one top-level tools[] JSON element and, when it is a
// namespace container, its child-array member key and raw child elements.
type rawTopToolEntry struct {
	raw         jsontext.Value   // original top-level tools[] JSON element
	isNamespace bool             // true when top-level element has type == "namespace"
	nsName      string           // trimmed namespace name when isNamespace is true
	childKey    string           // "tools" or "children" member name on namespace object
	rawChildren []jsontext.Value // original child array elements inside namespace object
}

// rawToolOrigin pairs an IR tool slot's source raw JSON bytes with its
// top-level tools[] container index and optional namespace child slot.
type rawToolOrigin struct {
	raw           jsontext.Value // original tool or namespace-child JSON object
	fromNamespace bool           // true when extracted from a namespace tools/children array
	topIndex      int            // index into rawTopToolEntry slice
	childIndex    int            // index into rawTopToolEntry.rawChildren when fromNamespace is true
}

// NewResponsesToolStream allocates a stateful native Responses SSE restorer
// initialized with a detached clone of tools. Nil or restoration-free tool maps
// pass incoming frames through while preserving Semantic and Terminal annotations.
func NewResponsesToolStream(tools *ToolMap) *ResponsesToolStream {
	return &ResponsesToolStream{
		tools:       tools.Clone(),
		calls:       make(map[string]*responsesActiveProxyCall),
		byOutput:    make(map[int]*responsesActiveProxyCall),
		semByOutput: make(map[int]bool),
		semByItemID: make(map[string]bool),
	}
}

// Write restores one native Responses SSE frame into zero, one, or multiple
// client SSE frames with updated Event headers and Semantic or Terminal flags.
// Blank lines return nil, while non-object keepalive or sentinel frames pass
// through as independent byte clones without failing stream translation.
//
// Proxy function_call_arguments deltas stay buffered until item closure, while
// unrelated output items, one-shot output_item.done payloads, and hosted-tool
// intermediate events retain exact Semantic flags and sequence_number values.
func (s *ResponsesToolStream) Write(frame sse.Frame) ([]sse.Frame, error) {
	trimmed := bytes.TrimSpace(frame.Data)
	if len(trimmed) == 0 {
		return nil, nil
	}
	// Pass [DONE] sentinels, non-JSON-object keepalive lines, and streams with no
	// active restorations through directly while stamping Semantic and Terminal flags.
	if bytes.Equal(trimmed, []byte("[DONE]")) || trimmed[0] != '{' || !isPermissiveJSONObject(trimmed) ||
		s == nil || !s.tools.HasRestorations() {
		evType, sem, term := s.annotateFrameSemantic(frame.Data, frame.Event)
		outEvent := frame.Event
		if strings.TrimSpace(outEvent) != "" && evType != "" {
			outEvent = evType
		}
		return []sse.Frame{{
			Event:    outEvent,
			Data:     bytes.Clone(frame.Data),
			Semantic: sem,
			Terminal: term,
		}}, nil
	}

	payloads, _, err := s.restorePayloadWithEvent(frame.Data, frame.Event)
	if err != nil {
		return nil, err
	}
	if len(payloads) == 0 {
		return nil, nil
	}

	hadEventHeader := strings.TrimSpace(frame.Event) != ""
	out := make([]sse.Frame, 0, len(payloads))
	for _, p := range payloads {
		evType, sem, term := s.annotateFrameSemantic(p, frame.Event)
		hdr := ""
		if hadEventHeader {
			if evType != "" {
				hdr = evType
			} else {
				hdr = strings.TrimSpace(frame.Event)
			}
		}
		out = append(out, sse.Frame{
			Event:    hdr,
			Data:     p,
			Semantic: sem,
			Terminal: term,
		})
	}
	return out, nil
}

// annotateFrameSemantic classifies payload with fallbackEvent and tracks which
// output items already emitted at least one Semantic=true frame. Unannounced
// one-shot response.output_item.done payloads carrying visible message,
// reasoning, or callable tool content promote to Semantic=true when no earlier
// frame for that item was marked semantic.
func (s *ResponsesToolStream) annotateFrameSemantic(payload []byte, fallbackEvent string) (string, bool, bool) {
	evType, sem, term := classifyResponsesWireFrameWithEvent(payload, fallbackEvent)
	if s == nil {
		return evType, sem, term
	}
	if sem {
		outIdx, hasOutIdx, itemID, _ := inspectResponsesStreamFrameItem(payload)
		if hasOutIdx {
			s.semByOutput[outIdx] = true
		}
		if itemID != "" {
			s.semByItemID[itemID] = true
		}
		return evType, true, term
	}
	if evType == "response.output_item.done" {
		outIdx, hasOutIdx, itemID, itemHasSem := inspectResponsesStreamFrameItem(payload)
		alreadySaw := (hasOutIdx && s.semByOutput[outIdx]) || (itemID != "" && s.semByItemID[itemID])
		if !alreadySaw && itemHasSem {
			if hasOutIdx {
				s.semByOutput[outIdx] = true
			}
			if itemID != "" {
				s.semByItemID[itemID] = true
			}
			return evType, true, term
		}
	}
	return evType, false, term
}

// Close finalizes s at upstream stream completion or transport error. Native
// Responses streams emit terminal frames directly on upstream events, so Close
// clears buffered proxy state without synthesizing extra frames when cause is nil.
func (s *ResponsesToolStream) Close(_ error) ([]sse.Frame, error) {
	if s != nil {
		clear(s.calls)
		clear(s.byOutput)
		clear(s.semByOutput)
		clear(s.semByItemID)
		s.active = s.active[:0]
	}
	return nil, nil
}

// RestorePayload restores one raw JSON Responses stream event payload into zero
// or more client event payloads and reports whether any wire bytes changed.
// Empty payloads return nil, and streams with no active restorations return an
// independent clone of payload without re-encoding untouched members.
func (s *ResponsesToolStream) RestorePayload(payload []byte) ([][]byte, bool, error) {
	return s.restorePayloadWithEvent(payload, "")
}

// restorePayloadWithEvent restores one raw JSON Responses stream event payload,
// falling back to fallbackEvent when the JSON object omits a top-level type
// discriminator. Empty payloads return nil, and streams with no active
// restorations return a detached clone without re-encoding untouched members.
//
// Initial sequence_number state anchors on the first parsed event so downstream
// frames stay contiguous after proxy argument deltas are buffered or expanded.
func (s *ResponsesToolStream) restorePayloadWithEvent(
	payload []byte,
	fallbackEvent string,
) ([][]byte, bool, error) {
	if len(bytes.TrimSpace(payload)) == 0 {
		return nil, false, nil
	}
	if s == nil || !s.tools.HasRestorations() {
		return [][]byte{bytes.Clone(payload)}, false, nil
	}

	members, err := parseOrderedObject(payload)
	if err != nil {
		return nil, false, fmt.Errorf("restore responses stream event: %w", err)
	}

	evType, _ := readMemberString(members, "type")
	evType = strings.TrimSpace(evType)
	if evType == "" && strings.TrimSpace(fallbackEvent) != "" {
		evType = strings.TrimSpace(fallbackEvent)
	}
	inSeq, hasInSeq := readMemberInt64(members, "sequence_number")
	if !s.seenSeq {
		s.seenSeq = true
		s.hasSeq = hasInSeq
		s.nextSeq = inSeq
	}

	objTag, _ := readMemberString(members, "object")
	// Compatible upstreams occasionally emit a bare response envelope on SSE
	// without a top-level event type; restore its embedded output[] items too.
	if isResponsesTerminalStreamType(evType) || (evType == "" && strings.TrimSpace(objTag) == "response") {
		restored, changed, err := restoreResponsesEnvelopeMembers(members, s.tools)
		if err != nil {
			return nil, false, err
		}
		if !changed {
			return s.emitRawOrResequence(payload, members, inSeq, hasInSeq, false)
		}
		rebuilt, err := encodeOrderedObject(restored)
		if err != nil {
			return nil, false, err
		}
		return s.emitRawOrResequence(rebuilt, restored, inSeq, hasInSeq, true)
	}

	switch evType {
	case "response.output_item.added":
		return s.handleOutputItemAdded(payload, members, inSeq, hasInSeq)
	case "response.function_call_arguments.delta":
		return s.handleFunctionArgsDelta(payload, members, inSeq, hasInSeq)
	case "response.function_call_arguments.done":
		return s.handleFunctionArgsDone(payload, members, inSeq, hasInSeq)
	case "response.output_item.done":
		return s.handleOutputItemDone(payload, members, inSeq, hasInSeq)
	default:
		return s.emitRawOrResequence(payload, members, inSeq, hasInSeq, false)
	}
}

// handleOutputItemAdded restores custom_tool_call, tool_search_call, or
// namespaced function_call headers on response.output_item.added and registers
// active proxy calls for downstream delta buffering. Non-function or unmapped
// items pass through unchanged aside from optional sequence renumbering.
func (s *ResponsesToolStream) handleOutputItemAdded(
	origPayload []byte,
	members []rawMember,
	inSeq int64,
	hasInSeq bool,
) ([][]byte, bool, error) {
	rawItem, ok := findRawMember(members, "item")
	if !ok || rawItem.Kind() != '{' {
		return s.emitRawOrResequence(origPayload, members, inSeq, hasInSeq, false)
	}
	iMembers, err := parseOrderedObject(rawItem)
	if err != nil {
		return nil, false, err
	}
	iType, _ := readMemberString(iMembers, "type")
	if iType != "function_call" {
		return s.emitRawOrResequence(origPayload, members, inSeq, hasInSeq, false)
	}

	name, _ := readMemberString(iMembers, "name")
	ns, _ := readMemberString(iMembers, "namespace")
	itemID, _ := readMemberString(iMembers, "id")
	callID, _ := readMemberString(iMembers, "call_id")
	outIdx64, hasOutIdx := readMemberInt64(members, "output_index")
	outIdx := int(outIdx64)

	origCustom, isCustom := s.tools.LookupCustomCall(ns, name)
	switch {
	case isCustom:
		call := s.registerProxyCall("custom", name, origCustom.Name, origCustom.Namespace, itemID, callID, outIdx, hasOutIdx, iMembers)
		call.sawAdded = true
		iMembers = setStringMember(iMembers, "type", "custom_tool_call")
		if call.clientItemID != "" {
			iMembers = setStringMember(iMembers, "id", call.clientItemID)
		}
		iMembers = setStringMember(iMembers, "name", origCustom.Name)
		iMembers = setStringMember(iMembers, "input", "")
		iMembers = deleteRawMember(iMembers, "arguments")
		if origCustom.Namespace == "" {
			iMembers = deleteRawMember(iMembers, "namespace")
		} else {
			iMembers = setStringMember(iMembers, "namespace", origCustom.Namespace)
		}
		encItem, err := encodeOrderedObject(iMembers)
		if err != nil {
			return nil, false, err
		}
		members = setRawMember(members, "item", encItem)
		return s.emitModifiedEvent(members, hasInSeq)

	case s.tools.IsToolSearchProxyCall(ns, name):
		call := s.registerProxyCall("tool_search", name, "", "", itemID, callID, outIdx, hasOutIdx, iMembers)
		call.sawAdded = true
		iMembers = setStringMember(iMembers, "type", "tool_search_call")
		if call.clientItemID != "" {
			iMembers = setStringMember(iMembers, "id", call.clientItemID)
		}
		iMembers = setStringMember(iMembers, "execution", "client")
		iMembers = setRawMember(iMembers, "arguments", jsontext.Value("{}"))
		iMembers = deleteRawMember(iMembers, "name")
		iMembers = deleteRawMember(iMembers, "namespace")
		encItem, err := encodeOrderedObject(iMembers)
		if err != nil {
			return nil, false, err
		}
		members = setRawMember(members, "item", encItem)
		return s.emitModifiedEvent(members, hasInSeq)

	default:
		trimmedNS := strings.TrimSpace(ns)
		if trimmedNS == "" || trimmedNS == defaultFunctionsNamespace {
			if pair, ok := s.tools.LookupNamespaceTool(name); ok {
				updatedItem, itemChanged := applyRestoredNamespaceMembers(iMembers, name, pair)
				if itemChanged {
					encItem, err := encodeOrderedObject(updatedItem)
					if err != nil {
						return nil, false, err
					}
					members = setRawMember(members, "item", encItem)
					return s.emitModifiedEvent(members, hasInSeq)
				}
			}
		}
		return s.emitRawOrResequence(origPayload, members, inSeq, hasInSeq, false)
	}
}

// handleFunctionArgsDelta buffers incoming JSON wrapper fragments for active
// custom or tool_search proxy calls without advancing sequence_number, or
// restores bare function names on namespaced calls. Unmapped calls pass through.
func (s *ResponsesToolStream) handleFunctionArgsDelta(
	origPayload []byte,
	members []rawMember,
	inSeq int64,
	hasInSeq bool,
) ([][]byte, bool, error) {
	if call := s.lookupActiveProxyCall(members); call != nil {
		delta, _ := readMemberString(members, "delta")
		_, _ = call.argBuf.WriteString(delta)
		// Suppress proxy function argument deltas without advancing nextSeq so
		// custom JSON wrappers can be unwrapped atomically once complete.
		return nil, true, nil
	}
	if name, ok := readMemberString(members, "name"); ok {
		if pair, found := s.tools.LookupNamespaceTool(name); found && name != pair.Name {
			members = setStringMember(members, "name", pair.Name)
			return s.emitModifiedEvent(members, hasInSeq)
		}
	}
	return s.emitRawOrResequence(origPayload, members, inSeq, hasInSeq, false)
}

// handleFunctionArgsDone unwraps buffered JSON wrappers for custom proxy calls
// into response.custom_tool_call_input.delta and done frames, and suppresses
// function_call_arguments.done on tool_search proxy calls. Namespaced and
// unmapped function calls emit their restored or original completion event.
func (s *ResponsesToolStream) handleFunctionArgsDone(
	origPayload []byte,
	members []rawMember,
	inSeq int64,
	hasInSeq bool,
) ([][]byte, bool, error) {
	if call := s.lookupActiveProxyCall(members); call != nil {
		if args, ok := readMemberString(members, "arguments"); ok && args != "" {
			call.argBuf.Reset()
			_, _ = call.argBuf.WriteString(args)
		}
		call.emittedInputDone = true
		if call.kind != "custom" {
			return nil, true, nil
		}
		batch, err := s.emitCustomInputEvents(call, unwrapCustomToolInput(call.argBuf.String()), hasInSeq)
		if err != nil {
			return nil, false, err
		}
		return batch, true, nil
	}
	if name, ok := readMemberString(members, "name"); ok {
		if pair, found := s.tools.LookupNamespaceTool(name); found && name != pair.Name {
			members = setStringMember(members, "name", pair.Name)
			return s.emitModifiedEvent(members, hasInSeq)
		}
	}
	return s.emitRawOrResequence(origPayload, members, inSeq, hasInSeq, false)
}

// handleOutputItemDone rewrites completed function_call items on
// response.output_item.done back into custom_tool_call, tool_search_call, or
// namespaced function_call items and removes completed proxy calls from active
// lookup indices. Unmapped function calls and non-function items pass through
// unchanged aside from optional sequence_number alignment.
//
// When a compatible upstream opened a custom proxy item via output_item.added
// without sending function_call_arguments.done, handleOutputItemDone flushes
// synthesized custom_tool_call_input.delta and done frames ahead of item closure.
func (s *ResponsesToolStream) handleOutputItemDone(
	origPayload []byte,
	members []rawMember,
	inSeq int64,
	hasInSeq bool,
) ([][]byte, bool, error) {
	rawItem, ok := findRawMember(members, "item")
	if !ok || rawItem.Kind() != '{' {
		return s.emitRawOrResequence(origPayload, members, inSeq, hasInSeq, false)
	}
	iMembers, err := parseOrderedObject(rawItem)
	if err != nil {
		return nil, false, err
	}
	iType, _ := readMemberString(iMembers, "type")
	if iType != "function_call" {
		return s.emitRawOrResequence(origPayload, members, inSeq, hasInSeq, false)
	}

	name, _ := readMemberString(iMembers, "name")
	ns, _ := readMemberString(iMembers, "namespace")
	itemID, _ := readMemberString(iMembers, "id")
	callID, _ := readMemberString(iMembers, "call_id")
	outIdx64, hasOutIdx := readMemberInt64(members, "output_index")
	outIdx := int(outIdx64)

	var kind string
	var origCustom NamespacedName
	if orig, ok := s.tools.LookupCustomCall(ns, name); ok {
		kind = "custom"
		origCustom = orig
	} else if s.tools.IsToolSearchProxyCall(ns, name) {
		kind = "tool_search"
	}
	if kind != "" {
		call := s.registerProxyCall(kind, name, origCustom.Name, origCustom.Namespace, itemID, callID, outIdx, hasOutIdx, iMembers)
		s.removeProxyCall(call)

		var outBatch [][]byte
		if kind == "custom" {
			unwrapped := unwrapCustomToolInput(call.argBuf.String())
			if call.sawAdded && !call.emittedInputDone {
				// Compatible upstreams may close a function_call item without sending
				// function_call_arguments.done; flush custom input events before item.done.
				synth, synthErr := s.emitCustomInputEvents(call, unwrapped, hasInSeq)
				if synthErr != nil {
					return nil, false, synthErr
				}
				outBatch = append(outBatch, synth...)
			}
			iMembers = setStringMember(iMembers, "type", "custom_tool_call")
			if call.clientItemID != "" {
				iMembers = setStringMember(iMembers, "id", call.clientItemID)
			}
			iMembers = setStringMember(iMembers, "name", call.clientName)
			iMembers = setStringMember(iMembers, "input", unwrapped)
			iMembers = deleteRawMember(iMembers, "arguments")
			if call.clientNamespace == "" {
				iMembers = deleteRawMember(iMembers, "namespace")
			} else {
				iMembers = setStringMember(iMembers, "namespace", call.clientNamespace)
			}
		} else {
			iMembers = setStringMember(iMembers, "type", "tool_search_call")
			if call.clientItemID != "" {
				iMembers = setStringMember(iMembers, "id", call.clientItemID)
			}
			iMembers = setStringMember(iMembers, "execution", "client")
			iMembers = setRawMember(iMembers, "arguments", normalizeSearchArgumentsRaw(call.argBuf.String()))
			iMembers = deleteRawMember(iMembers, "name")
			iMembers = deleteRawMember(iMembers, "namespace")
		}
		encItem, err := encodeOrderedObject(iMembers)
		if err != nil {
			return nil, false, err
		}
		members = setRawMember(members, "item", encItem)
		donePayloads, _, err := s.emitModifiedEvent(members, hasInSeq)
		if err != nil {
			return nil, false, err
		}
		outBatch = append(outBatch, donePayloads...)
		return outBatch, true, nil
	}

	trimmedNS := strings.TrimSpace(ns)
	if trimmedNS == "" || trimmedNS == defaultFunctionsNamespace {
		if pair, found := s.tools.LookupNamespaceTool(name); found {
			updatedItem, itemChanged := applyRestoredNamespaceMembers(iMembers, name, pair)
			if itemChanged {
				encItem, err := encodeOrderedObject(updatedItem)
				if err != nil {
					return nil, false, err
				}
				members = setRawMember(members, "item", encItem)
				return s.emitModifiedEvent(members, hasInSeq)
			}
		}
	}
	return s.emitRawOrResequence(origPayload, members, inSeq, hasInSeq, false)
}

// emitCustomInputEvents constructs and encodes incremental custom tool input
// payloads (response.custom_tool_call_input.delta followed by
// response.custom_tool_call_input.done) for an open proxy call. Empty
// unwrapped input strings skip the delta event and emit only the terminal done payload.
func (s *ResponsesToolStream) emitCustomInputEvents(
	call *responsesActiveProxyCall,
	unwrapped string,
	hasInSeq bool,
) ([][]byte, error) {
	var batch [][]byte
	if unwrapped != "" {
		deltaMembers := buildCustomInputDeltaMembers(call, unwrapped, s.claimSeq(hasInSeq))
		rawDelta, err := encodeOrderedObject(deltaMembers)
		if err != nil {
			return nil, err
		}
		batch = append(batch, rawDelta)
	}
	doneMembers := buildCustomInputDoneMembers(call, unwrapped, s.claimSeq(hasInSeq))
	rawDone, err := encodeOrderedObject(doneMembers)
	if err != nil {
		return nil, err
	}
	batch = append(batch, rawDone)
	return batch, nil
}

// registerProxyCall finds an open responsesActiveProxyCall by item_id, call_id,
// or explicit output_index, or allocates and indexes a new record for
// downstream delta correlation. Non-empty arguments on iMembers overwrite the
// accumulated buffer so one-shot output_item.done events unwrap final text.
func (s *ResponsesToolStream) registerProxyCall(
	kind, upstreamName, clientName, clientNamespace, itemID, callID string,
	outIdx int,
	hasOutIdx bool,
	iMembers []rawMember,
) *responsesActiveProxyCall {
	var call *responsesActiveProxyCall
	if itemID != "" {
		call = s.calls[itemID]
	}
	if call == nil && callID != "" {
		call = s.calls[callID]
	}
	if call == nil && hasOutIdx {
		call = s.byOutput[outIdx]
	}
	if call == nil {
		targetType := "tool_search_call"
		if kind == "custom" {
			targetType = "custom_tool_call"
			if clientName == "" {
				clientName = upstreamName
			}
		}
		call = &responsesActiveProxyCall{
			kind:            kind,
			upstreamName:    upstreamName,
			clientName:      clientName,
			clientNamespace: clientNamespace,
			callID:          callID,
			itemID:          itemID,
			clientItemID:    retypeToolCallItemID(itemID, targetType),
			outputIdx:       outIdx,
			hasOutputIdx:    hasOutIdx,
		}
		if itemID != "" {
			s.calls[itemID] = call
		}
		if callID != "" {
			s.calls[callID] = call
		}
		if hasOutIdx {
			s.byOutput[outIdx] = call
		}
		s.active = append(s.active, call)
	}
	if args, ok := readMemberString(iMembers, "arguments"); ok && args != "" {
		call.argBuf.Reset()
		_, _ = call.argBuf.WriteString(args)
	}
	return call
}

// removeProxyCall deletes call from the item_id, call_id, output_index, and
// registration-order tracking tables once response.output_item.done closes the
// item. Nil call pointers return immediately without mutating active maps.
func (s *ResponsesToolStream) removeProxyCall(call *responsesActiveProxyCall) {
	if call == nil {
		return
	}
	if call.itemID != "" {
		delete(s.calls, call.itemID)
	}
	if call.callID != "" {
		delete(s.calls, call.callID)
	}
	if call.hasOutputIdx {
		delete(s.byOutput, call.outputIdx)
	}
	s.active = slices.DeleteFunc(s.active, func(c *responsesActiveProxyCall) bool {
		return c == call
	})
}

// lookupActiveProxyCall locates an active proxy call from event item_id,
// output_index, or call_id, returning nil when no open custom or tool_search
// call matches. When item_id is omitted, it falls back to scanning open proxy
// calls in deterministic registration order by upstream function name.
func (s *ResponsesToolStream) lookupActiveProxyCall(members []rawMember) *responsesActiveProxyCall {
	itemID, _ := readMemberString(members, "item_id")
	if itemID != "" {
		if call := s.calls[itemID]; call != nil {
			return call
		}
	}
	if outIdx, hasIdx := readMemberInt64(members, "output_index"); hasIdx {
		if call := s.byOutput[int(outIdx)]; call != nil {
			return call
		}
	}
	callID, _ := readMemberString(members, "call_id")
	if callID != "" {
		if call := s.calls[callID]; call != nil {
			return call
		}
	}
	if itemID == "" {
		// Compatible upstreams may omit item_id, output_index, and call_id on
		// argument deltas; match the earliest open proxy call sharing name.
		if name, _ := readMemberString(members, "name"); name != "" {
			for _, call := range s.active {
				if call.upstreamName == name {
					return call
				}
			}
		}
	}
	return nil
}

// claimSeq allocates the next contiguous sequence_number for synthesized custom
// input events when the stream already observed numbered upstream frames.
// Numberless streams return nil so synthetic payloads omit sequence_number too.
func (s *ResponsesToolStream) claimSeq(hasInSeq bool) *int64 {
	if !s.hasSeq && !hasInSeq {
		return nil
	}
	s.hasSeq = true
	seq := s.nextSeq
	s.nextSeq++
	return &seq
}

// emitModifiedEvent encodes members into a single event payload, updating
// sequence_number only when the incoming upstream event carried hasInSeq.
// Unnumbered upstream events omit sequence_number without advancing nextSeq.
func (s *ResponsesToolStream) emitModifiedEvent(members []rawMember, hasInSeq bool) ([][]byte, bool, error) {
	if hasInSeq {
		s.hasSeq = true
		want := s.nextSeq
		s.nextSeq++
		members = setRawMember(members, "sequence_number", jsontext.Value(strconv.FormatInt(want, 10)))
	}
	encoded, err := encodeOrderedObject(members)
	if err != nil {
		return nil, false, err
	}
	return [][]byte{encoded}, true, nil
}

// emitRawOrResequence returns a detached clone of payload when sequence_number
// aligns or is absent, and rewrites only sequence_number via ordered members
// when buffered deltas shifted numbering. Keepalive or ping events that omit
// sequence_number pass through untouched without advancing s.nextSeq.
func (s *ResponsesToolStream) emitRawOrResequence(
	payload []byte,
	members []rawMember,
	inSeq int64,
	hasInSeq bool,
	alreadyChanged bool,
) ([][]byte, bool, error) {
	if !hasInSeq {
		// Upstream keepalive or cost-ping frames omit sequence_number even on
		// numbered streams; leave them unnumbered so heartbeat filtering never
		// creates sequence gaps on subsequent Responses events.
		return [][]byte{bytes.Clone(payload)}, alreadyChanged, nil
	}
	s.hasSeq = true
	want := s.nextSeq
	s.nextSeq++
	if inSeq == want && !alreadyChanged {
		return [][]byte{bytes.Clone(payload)}, false, nil
	}
	members = setRawMember(members, "sequence_number", jsontext.Value(strconv.FormatInt(want, 10)))
	encoded, err := encodeOrderedObject(members)
	if err != nil {
		return nil, false, err
	}
	return [][]byte{encoded}, true, nil
}

// applyRestoredNamespaceMembers updates name and namespace on iMembers when
// they differ from pair and reports whether any JSON member changed. Bare
// default-scope functions that already omit namespace return changed=false so
// callers preserve original raw bytes.
func applyRestoredNamespaceMembers(
	iMembers []rawMember,
	curName string,
	pair NamespacedName,
) ([]rawMember, bool) {
	changed := false
	if curName != pair.Name {
		iMembers = setStringMember(iMembers, "name", pair.Name)
		changed = true
	}
	if pair.Namespace == "" {
		if _, hasNS := findRawMember(iMembers, "namespace"); hasNS {
			iMembers = deleteRawMember(iMembers, "namespace")
			changed = true
		}
	} else {
		curNS, hasNS := readMemberString(iMembers, "namespace")
		if !hasNS || curNS != pair.Namespace {
			iMembers = setStringMember(iMembers, "namespace", pair.Namespace)
			changed = true
		}
	}
	return iMembers, changed
}

// buildCustomInputDeltaMembers constructs ordered JSON object members for a
// response.custom_tool_call_input.delta event on call. Non-nil seq pointers
// insert sequence_number immediately after the event type discriminator.
func buildCustomInputDeltaMembers(call *responsesActiveProxyCall, delta string, seq *int64) []rawMember {
	out := []rawMember{
		{name: "type", value: mustMarshalString("response.custom_tool_call_input.delta")},
	}
	if seq != nil {
		out = append(out, rawMember{name: "sequence_number", value: jsontext.Value(strconv.FormatInt(*seq, 10))})
	}
	out = append(out,
		rawMember{name: "output_index", value: jsontext.Value(strconv.Itoa(call.outputIdx))},
		rawMember{name: "item_id", value: mustMarshalString(call.clientItemID)},
		rawMember{name: "delta", value: mustMarshalString(delta)},
	)
	return out
}

// buildCustomInputDoneMembers constructs ordered JSON object members for a
// response.custom_tool_call_input.done event carrying the restored client tool
// name. Optional sequence_number, call_id, and name fields are omitted when absent on call.
func buildCustomInputDoneMembers(call *responsesActiveProxyCall, input string, seq *int64) []rawMember {
	out := []rawMember{
		{name: "type", value: mustMarshalString("response.custom_tool_call_input.done")},
	}
	if seq != nil {
		out = append(out, rawMember{name: "sequence_number", value: jsontext.Value(strconv.FormatInt(*seq, 10))})
	}
	out = append(out,
		rawMember{name: "output_index", value: jsontext.Value(strconv.Itoa(call.outputIdx))},
		rawMember{name: "item_id", value: mustMarshalString(call.clientItemID)},
	)
	if call.callID != "" {
		out = append(out, rawMember{name: "call_id", value: mustMarshalString(call.callID)})
	}
	if call.clientName != "" {
		out = append(out, rawMember{name: "name", value: mustMarshalString(call.clientName)})
	}
	out = append(out, rawMember{name: "input", value: mustMarshalString(input)})
	return out
}

// NeedsResponsesToolAdaptation reports whether a same-protocol OpenAI Responses
// HTTP or WebSocket request requires client-tool lowering under dialect.
// Dialects that natively support custom, namespace, and client_search tools, as
// well as malformed JSON bodies or WS frames other than response.create, return
// false without allocating an IR tree.
//
// Requests carrying non-empty inherited mappings always return true so WS
// continuation, explicit tools: [] reset, and tool-replacement turns update
// session state cleanly.
func NeedsResponsesToolAdaptation(dialect Dialect, clientBody []byte, inherited *ToolMap) bool {
	eff := resolveEffectiveDialect(Route{
		Client:   protocol.ProtocolOpenAIResponses,
		Upstream: protocol.ProtocolOpenAIResponses,
		Dialect:  dialect,
	})
	needCustom := !eff.Tools.Has(ToolSupportCustom)
	needNamespace := !eff.Tools.Has(ToolSupportNamespace)
	needSearch := !eff.Tools.Has(ToolSupportClientSearch)
	if !needCustom && !needNamespace && !needSearch {
		return false
	}

	root, err := parseOrderedObject(clientBody)
	if err != nil {
		return false
	}
	scope, _, skip, err := selectResponsesAdaptationScope(root)
	if err != nil || skip {
		return false
	}

	if !inherited.IsEmpty() {
		return true
	}

	if rawTools, ok := findRawMember(scope, "tools"); ok && rawTools.Kind() == '[' {
		if elems, err := parseRawArray(rawTools); err == nil {
			for _, el := range elems {
				if rawToolDeclNeedsAdaptation(el, needCustom, needNamespace, needSearch) {
					return true
				}
			}
		}
	}

	if rawInput, ok := findRawMember(scope, "input"); ok {
		switch rawInput.Kind() {
		case '[':
			if items, err := parseRawArray(rawInput); err == nil {
				for _, item := range items {
					if rawInputItemNeedsAdaptation(item, needCustom, needNamespace, needSearch) {
						return true
					}
				}
			}
		case '{':
			if rawInputItemNeedsAdaptation(rawInput, needCustom, needNamespace, needSearch) {
				return true
			}
		}
	}

	if rawTC, ok := findRawMember(scope, "tool_choice"); ok && rawTC.Kind() == '{' {
		if rawToolChoiceNeedsAdaptation(rawTC, needCustom, needNamespace, needSearch) {
			return true
		}
	}
	return false
}

// rawToolDeclNeedsAdaptation reports whether one tools[] JSON element or any
// child inside a namespace container requires lowering under needCustom,
// needNamespace, or needSearch. Non-object or malformed JSON elements return false.
func rawToolDeclNeedsAdaptation(el jsontext.Value, needCustom, needNamespace, needSearch bool) bool {
	if el.Kind() != '{' {
		return false
	}
	m, err := parseOrderedObject(el)
	if err != nil {
		return false
	}
	typ, _ := readMemberString(m, "type")
	typ = strings.TrimSpace(typ)
	if (needCustom && typ == "custom") ||
		(needSearch && (typ == "tool_search" || readMemberBoolPtr(m, "defer_loading") != nil)) ||
		(needNamespace && typ == "namespace") {
		return true
	}
	if typ == "namespace" && (needCustom || needSearch) {
		childrenRaw, hasTools := findRawMember(m, "tools")
		if !hasTools || childrenRaw.Kind() != '[' {
			childrenRaw, _ = findRawMember(m, "children")
		}
		if len(childrenRaw) > 0 && childrenRaw.Kind() == '[' {
			if chElems, err := parseRawArray(childrenRaw); err == nil {
				for _, ch := range chElems {
					if rawToolDeclNeedsAdaptation(ch, needCustom, false, needSearch) {
						return true
					}
				}
			}
		}
	}
	return false
}

// rawInputItemNeedsAdaptation reports whether one input item object requires
// lowering under needCustom, needNamespace, or needSearch. Non-object items,
// malformed JSON, and ordinary messages or bare function calls return false.
func rawInputItemNeedsAdaptation(item jsontext.Value, needCustom, needNamespace, needSearch bool) bool {
	if item.Kind() != '{' {
		return false
	}
	m, err := parseOrderedObject(item)
	if err != nil {
		return false
	}
	typ, _ := readMemberString(m, "type")
	switch strings.TrimSpace(typ) {
	case "custom_tool_call":
		if needCustom {
			return true
		}
		if needNamespace {
			if _, hasNS := findRawMember(m, "namespace"); hasNS {
				return true
			}
		}
	case "custom_tool_call_output":
		return needCustom
	case "tool_search_call", "tool_search_output":
		return needSearch
	case "additional_tools":
		return needNamespace || needCustom || needSearch
	case "function_call":
		if needNamespace {
			if _, hasNS := findRawMember(m, "namespace"); hasNS {
				return true
			}
		}
	}
	return false
}

// rawToolChoiceNeedsAdaptation reports whether an object-shaped tool_choice
// value or any entry inside its flat or nested allowed_tools list requires
// lowering. Malformed JSON or unmapped function selectors return false.
func rawToolChoiceNeedsAdaptation(rawTC jsontext.Value, needCustom, needNamespace, needSearch bool) bool {
	m, err := parseOrderedObject(rawTC)
	if err != nil {
		return false
	}
	typ, _ := readMemberString(m, "type")
	typ = strings.TrimSpace(typ)
	_, hasNS := findRawMember(m, "namespace")
	if (needCustom && typ == "custom") ||
		(needSearch && typ == "tool_search") ||
		(needNamespace && (typ == "namespace" || hasNS)) {
		return true
	}
	if typ == "allowed_tools" {
		if rawList, ok := findRawMember(m, "tools"); ok && rawList.Kind() == '[' {
			if allowedListNeedsAdaptation(rawList, needCustom, needNamespace, needSearch) {
				return true
			}
		}
		if rawWrap, ok := findRawMember(m, "allowed_tools"); ok && rawWrap.Kind() == '{' {
			if wm, err := parseOrderedObject(rawWrap); err == nil {
				if rawList, ok := findRawMember(wm, "tools"); ok && rawList.Kind() == '[' {
					if allowedListNeedsAdaptation(rawList, needCustom, needNamespace, needSearch) {
						return true
					}
				}
			}
		}
	}
	return false
}

// allowedListNeedsAdaptation reports whether any entry inside a raw
// allowed_tools JSON array references a custom, search, or namespaced tool
// requiring lowering. Malformed arrays or non-object entries return false.
func allowedListNeedsAdaptation(rawList jsontext.Value, needCustom, needNamespace, needSearch bool) bool {
	elems, err := parseRawArray(rawList)
	if err != nil {
		return false
	}
	for _, el := range elems {
		if el.Kind() != '{' {
			continue
		}
		em, err := parseOrderedObject(el)
		if err != nil {
			continue
		}
		eType, _ := readMemberString(em, "type")
		eType = strings.TrimSpace(eType)
		_, hasNS := findRawMember(em, "namespace")
		if (needCustom && eType == "custom") ||
			(needSearch && eType == "tool_search") ||
			(needNamespace && (eType == "namespace" || hasNS)) {
			return true
		}
	}
	return false
}

// AdaptResponsesTools lowers unsupported custom, namespace, and tool_search
// declarations and history items inside a same-protocol Responses HTTP or WS
// request body in place while preserving unrelated fields, item-level cache
// breakpoints, compaction items, raw reasoning tokens, and large integers.
//
// Payloads that require no changes return clientBody unchanged alongside a
// detached clone of inherited (or nil when explicit replacement cleared tools).
// When tools are lowered, the returned ToolMap retains serialized LoweredTools
// declarations so subsequent WS continuation turns omitting tools[] or model
// still supply the lowered list upstream.
func AdaptResponsesTools(
	clientBody []byte,
	dialect Dialect,
	inherited *ToolMap,
) ([]byte, *ToolMap, []ir.Loss, error) {
	eff := resolveEffectiveDialect(Route{
		Client:   protocol.ProtocolOpenAIResponses,
		Upstream: protocol.ProtocolOpenAIResponses,
		Dialect:  dialect,
	})
	needCustom := !eff.Tools.Has(ToolSupportCustom)
	needNamespace := !eff.Tools.Has(ToolSupportNamespace)
	needSearch := !eff.Tools.Has(ToolSupportClientSearch)
	if !needCustom && !needNamespace && !needSearch {
		return clientBody, inherited.Clone(), nil, nil
	}

	root, err := parseOrderedObject(clientBody)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("adapt responses tools: %w", err)
	}

	scope, nestedScope, skip, err := selectResponsesAdaptationScope(root)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("adapt responses tools nested response: %w", err)
	}
	if skip {
		return clientBody, inherited.Clone(), nil, nil
	}

	rebuiltScope, tm, losses, changed, err := adaptResponsesScopeMembers(
		scope,
		eff,
		needCustom,
		needNamespace,
		needSearch,
		inherited,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	if !changed {
		return clientBody, tm, nil, nil
	}

	if nestedScope {
		encNested, err := encodeOrderedObject(rebuiltScope)
		if err != nil {
			return nil, nil, nil, err
		}
		root = setRawMember(root, "response", encNested)
	} else {
		root = rebuiltScope
	}
	outBytes, err := encodeOrderedObject(root)
	if err != nil {
		return nil, nil, nil, err
	}
	return outBytes, tm, losses, nil
}

// RestoreResponsesTools restores lowered custom, search, and namespaced tool
// calls inside a buffered Responses JSON response body without re-parsing
// unrelated numeric or extension fields through float64. Empty bodies, nil
// mappings, or payloads with no lowered call items return upstreamBody unchanged.
func RestoreResponsesTools(upstreamBody []byte, tools *ToolMap) ([]byte, error) {
	if len(bytes.TrimSpace(upstreamBody)) == 0 || !tools.HasRestorations() {
		return upstreamBody, nil
	}
	root, err := parseOrderedObject(upstreamBody)
	if err != nil {
		return nil, fmt.Errorf("restore responses tools: %w", err)
	}
	restored, changed, err := restoreResponsesEnvelopeMembers(root, tools)
	if err != nil {
		return nil, err
	}
	if !changed {
		return upstreamBody, nil
	}
	out, err := encodeOrderedObject(restored)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// selectResponsesAdaptationScope resolves whether root is a non-create WS frame
// (skip=true), a nested WS response.create envelope (nested=true), or a
// top-level Responses request object. Top-level tools, input, or tool_choice
// fields take precedence so unrelated root response fields never shadow HTTP payloads.
func selectResponsesAdaptationScope(root []rawMember) ([]rawMember, bool, bool, error) {
	if rawType, hasType := findRawMember(root, "type"); hasType {
		var frameType string
		if err := unmarshalPermissive(rawType, &frameType); err == nil {
			trimmed := strings.TrimSpace(frameType)
			if trimmed != "" && trimmed != "response.create" {
				return nil, false, true, nil
			}
		}
	}
	_, hasTools := findRawMember(root, "tools")
	_, hasInput := findRawMember(root, "input")
	_, hasToolChoice := findRawMember(root, "tool_choice")
	// Enter a nested response object only when root omits all request tool and
	// input fields, matching nested WebSocket response.create envelopes safely.
	if !hasTools && !hasInput && !hasToolChoice {
		if rawResp, hasResp := findRawMember(root, "response"); hasResp && rawResp.Kind() == '{' {
			nested, err := parseOrderedObject(rawResp)
			if err != nil {
				return nil, false, false, err
			}
			return nested, true, false, nil
		}
	}
	return root, false, false, nil
}

// adaptResponsesScopeMembers lowers tools, input, and tool_choice on scope
// according to dialect and inherited session state, returning the updated
// ordered members, next-turn ToolMap, original-coordinate ir.Loss slice, and
// whether any wire member changed. Requests requiring no rewrites return
// changed=false without re-encoding untouched fields.
//
// Omitted tools[] turns reconstruct declarations from inherited.LoweredTools.
// Explicit tools: [] resets or ordinary function replacements clear returned
// session restorations while consulting non-conflicting inherited mappings to
// lower historical calls in input[].
func adaptResponsesScopeMembers(
	scope []rawMember,
	dialect Dialect,
	needCustom, needNamespace, needSearch bool,
	inherited *ToolMap,
) ([]rawMember, *ToolMap, []ir.Loss, bool, error) {
	maxBytes := dialect.ToolNameMaxBytes
	if maxBytes <= 0 {
		maxBytes = defaultOpenAIToolNameMaxBytes
	}

	rawToolsVal, toolsPresent := findRawMember(scope, "tools")
	var toolElems []jsontext.Value
	toolsReset := false
	injectedFromInherited := false

	if toolsPresent {
		if rawToolsVal.Kind() == '[' {
			parsed, err := parseRawArray(rawToolsVal)
			if err != nil {
				return nil, nil, nil, false, err
			}
			toolElems = parsed
			if len(toolElems) == 0 {
				toolsReset = true
			}
		} else {
			toolsReset = true
		}
	} else if inherited != nil && len(inherited.LoweredTools) > 0 {
		rebuilt, err := rebuildClientToolsFromInherited(inherited.LoweredTools, inherited)
		if err != nil {
			return nil, nil, nil, false, err
		}
		toolElems = rebuilt
		injectedFromInherited = true
	}

	var inputElems []jsontext.Value
	var origInputIndices []int
	hasInputItems := false
	singleObjectInput := false
	if rawInputVal, ok := findRawMember(scope, "input"); ok {
		switch rawInputVal.Kind() {
		case '[':
			parsed, err := parseRawArray(rawInputVal)
			if err != nil {
				return nil, nil, nil, false, err
			}
			inputElems = parsed
			origInputIndices = make([]int, len(parsed))
			for i := range parsed {
				origInputIndices[i] = i
			}
			hasInputItems = true
		case '{':
			inputElems = []jsontext.Value{rawInputVal}
			origInputIndices = []int{0}
			hasInputItems = true
			singleObjectInput = true
		}
	}

	origInputElems := inputElems
	changed := injectedFromInherited
	liftedAdditionalTools := false
	var losses []ir.Loss

	// Lift additional_tools carrier items out of input[] into toolElems unless
	// tools[] was explicitly reset, because function-only Responses endpoints
	// reject additional_tools inside input[].
	if !toolsReset && hasInputItems && (needNamespace || needCustom || needSearch) {
		filteredInput := make([]jsontext.Value, 0, len(inputElems))
		filteredIndices := make([]int, 0, len(inputElems))
		for i, item := range inputElems {
			if item.Kind() == '{' {
				if m, err := parseOrderedObject(item); err == nil {
					if typ, _ := readMemberString(m, "type"); strings.TrimSpace(typ) == "additional_tools" {
						addToolsRaw, ok := findRawMember(m, "tools")
						if !ok || addToolsRaw.Kind() != '[' {
							return nil, nil, nil, false, fmt.Errorf("additional_tools tools must be an array")
						}
						addElems, err := parseRawArray(addToolsRaw)
						if err != nil {
							return nil, nil, nil, false, err
						}
						// Deduplicate identical replayed additional_tools declarations against
						// inherited session tools on omitted-tools continuation turns while
						// keeping new or conflicting definitions for lowering and collision checks.
						if injectedFromInherited {
							toolElems = appendInheritedAdditionalTools(toolElems, addElems)
						} else {
							toolElems = append(toolElems, addElems...)
						}
						liftedAdditionalTools = true
						changed = true
						continue
					}
				}
			}
			filteredInput = append(filteredInput, item)
			filteredIndices = append(filteredIndices, origInputIndices[i])
		}
		inputElems = filteredInput
		origInputIndices = filteredIndices
		if liftedAdditionalTools && singleObjectInput {
			singleObjectInput = false
		}
	}

	if toolsReset || len(toolElems) == 0 {
		var effInherited *ToolMap
		if inherited != nil && !toolsReset {
			effInherited = inherited.Clone()
		}
		var inheritedForHistory *ToolMap
		if inherited != nil {
			inheritedForHistory = inherited.Clone()
		}
		histTM, err := collectResponsesInputHistoryMap(inputElems, inheritedForHistory, maxBytes)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if liftedAdditionalTools && !toolsReset {
			scope = setRawMember(scope, "tools", jsontext.Value("[]"))
			changed = true
		}
		if hasInputItems {
			rebuiltIn, inChanged, inLosses, err := rewriteResponsesRawInputItems(
				inputElems,
				origInputIndices,
				histTM,
				maxBytes,
				needCustom,
				needNamespace,
				needSearch,
			)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if inChanged || liftedAdditionalTools {
				updatedScope, encErr := writeRebuiltInputOnScope(scope, rebuiltIn, singleObjectInput)
				if encErr != nil {
					return nil, nil, nil, false, encErr
				}
				scope = updatedScope
				changed = true
				losses = append(losses, inLosses...)
			}
		}
		if !toolsReset {
			tcUpdated, tcChanged, err := rewriteResponsesRawToolChoice(scope, histTM, nil, maxBytes, needCustom, needNamespace, needSearch, &losses)
			if err != nil {
				return nil, nil, nil, false, err
			}
			if tcChanged {
				scope = tcUpdated
				changed = true
			}
		}
		if toolsReset {
			return scope, nil, losses, changed, nil
		}
		return scope, effInherited, losses, changed, nil
	}

	irTools, irNamespaces, topEntries, origins, err := parseRawResponsesToolsForLowering(toolElems)
	if err != nil {
		return nil, nil, nil, false, err
	}

	syntheticReq := &ir.Request{
		Model:      "gpt-5",
		Tools:      irTools,
		Namespaces: irNamespaces,
		Messages:   extractToolSearchMessagesFromRawInput(inputElems),
	}
	coords := newSourceCoords(syntheticReq)
	var inheritedForMap *ToolMap
	if !toolsPresent || injectedFromInherited {
		inheritedForMap = inherited
	}
	tm, toolLosses, err := lowerTools(
		syntheticReq,
		coords,
		Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  dialect,
		},
		inheritedForMap,
	)
	if err != nil {
		return nil, nil, nil, false, err
	}
	if tm == nil {
		tm = &ToolMap{}
	}

	if len(toolLosses) > 0 || len(syntheticReq.Tools) != len(irTools) || (needNamespace && len(irNamespaces) > 0) {
		changed = true
		losses = append(losses, toolLosses...)
	}

	// Merge historical tool_search_output and additional_tools evidence from
	// pre-lift input into tm while enforcing name-collision checks against active tools.
	if err := mergeRawHistoryIdentityEvidence(tm, origInputElems, maxBytes); err != nil {
		return nil, nil, nil, false, err
	}

	rebuiltTools, toolsRewritten, err := serializeAdaptedResponsesTools(
		topEntries,
		origins,
		coords,
		syntheticReq.Tools,
		maxBytes,
		needCustom,
		needNamespace,
		needSearch,
	)
	if err != nil {
		return nil, nil, nil, false, err
	}
	if toolsRewritten || injectedFromInherited || liftedAdditionalTools {
		encTools, err := encodeRawArray(rebuiltTools)
		if err != nil {
			return nil, nil, nil, false, err
		}
		scope = setRawMember(scope, "tools", encTools)
		changed = true
	}

	// Merge non-conflicting inherited mappings into inputTM when explicit tools[]
	// replaced an earlier turn's session tools so historical calls in input[]
	// still lower cleanly while returned tm tracks only the active turn's tools.
	inputTM := tm
	if toolsPresent && !injectedFromInherited && inherited != nil {
		inputTM = mergeInheritedForHistoryRewrite(tm, inherited)
	}

	if hasInputItems {
		rebuiltIn, inChanged, inLosses, err := rewriteResponsesRawInputItems(
			inputElems,
			origInputIndices,
			inputTM,
			maxBytes,
			needCustom,
			needNamespace,
			needSearch,
		)
		if err != nil {
			return nil, nil, nil, false, err
		}
		if inChanged || liftedAdditionalTools {
			updatedScope, encErr := writeRebuiltInputOnScope(scope, rebuiltIn, singleObjectInput)
			if encErr != nil {
				return nil, nil, nil, false, encErr
			}
			scope = updatedScope
			changed = true
			losses = append(losses, inLosses...)
		}
	}

	tcUpdated, tcChanged, err := rewriteResponsesRawToolChoice(scope, tm, rebuiltTools, maxBytes, needCustom, needNamespace, needSearch, &losses)
	if err != nil {
		return nil, nil, nil, false, err
	}
	if tcChanged {
		scope = tcUpdated
		changed = true
	}

	if !tm.HasRestorations() && len(tm.DeclaredNamespaces) == 0 {
		return scope, nil, losses, changed, nil
	}
	tm.LoweredTools = cloneRawValueSlice(rebuiltTools)
	return scope, tm, losses, changed, nil
}

// appendInheritedAdditionalTools appends lifted additional_tools elements onto
// inherited session toolElems while skipping exact duplicate declarations from
// replayed WebSocket warmup or prior-turn input history. New declarations and
// conflicting shapes with the same name remain in the returned slice so
// downstream lowering and collision detection still enforce uniqueness.
func appendInheritedAdditionalTools(base, added []jsontext.Value) []jsontext.Value {
	baseTools, baseNS, _, _, err := parseRawResponsesToolsForLowering(base)
	if err != nil {
		// Preserve all inherited and newly lifted elements when pre-parsing base
		// fails so adaptResponsesScopeMembers's main parseRawResponsesToolsForLowering
		// call reports the authoritative JSON syntax error without losing added items.
		return append(base, added...)
	}
	seen := make(map[string]bool, len(baseTools))
	for _, t := range baseTools {
		if fp := inheritedToolFingerprint(t); fp != "" {
			seen[fp] = true
		}
	}
	seenNS := make(map[string]bool, len(baseNS))
	for _, ns := range baseNS {
		if ns.Name != "" {
			seenNS[ns.Name] = true
		}
	}

	out := base
	for _, el := range added {
		elTools, _, topEntries, origins, pErr := parseRawResponsesToolsForLowering([]jsontext.Value{el})
		if pErr != nil || len(topEntries) == 0 {
			// Retain malformed or unparsed additional_tools elements verbatim in out
			// so the subsequent lowering pass surfaces the exact tool decoding failure
			// rather than silently discarding invalid carrier entries.
			out = append(out, el)
			continue
		}
		if !topEntries[0].isNamespace {
			if len(elTools) == 1 {
				if fp := inheritedToolFingerprint(elTools[0]); fp != "" {
					if seen[fp] {
						continue
					}
					seen[fp] = true
				}
			}
			out = append(out, el)
			continue
		}

		nsName := topEntries[0].nsName
		if len(elTools) == 0 {
			if nsName != "" && seenNS[nsName] {
				continue
			}
			if nsName != "" {
				seenNS[nsName] = true
			}
			out = append(out, el)
			continue
		}

		var keptChildren []jsontext.Value
		for idx, ct := range elTools {
			fp := inheritedToolFingerprint(ct)
			if fp != "" && seen[fp] {
				continue
			}
			if fp != "" {
				seen[fp] = true
			}
			if idx < len(origins) && origins[idx].childIndex >= 0 && origins[idx].childIndex < len(topEntries[0].rawChildren) {
				keptChildren = append(keptChildren, topEntries[0].rawChildren[origins[idx].childIndex])
			}
		}
		if len(keptChildren) == 0 {
			continue
		}
		if len(keptChildren) == len(topEntries[0].rawChildren) {
			out = append(out, el)
			continue
		}
		if m, mErr := parseOrderedObject(el); mErr == nil {
			if encArr, aErr := encodeRawArray(keptChildren); aErr == nil {
				m = setRawMember(m, topEntries[0].childKey, encArr)
				if rebuiltEl, rErr := encodeOrderedObject(m); rErr == nil {
					out = append(out, rebuiltEl)
					continue
				}
			}
		}
		// Retain the original namespace wrapper when partial child re-encoding fails
		// so surviving novel or conflicting child declarations are never dropped before
		// downstream lowering and collision validation run.
		out = append(out, el)
	}
	return out
}

// inheritedToolFingerprint returns a canonical identity key combining tool kind,
// namespace, bare name, and normalized schema or grammar shape for deduplicating
// replayed WebSocket additional_tools items against inherited session tools.
// Unnamed function or custom tools, hosted items with nil raw payloads, and
// unrecognized ToolKind values return an empty string so callers skip seen-map
// deduplication and preserve those entries untouched.
func inheritedToolFingerprint(t ir.Tool) string {
	switch t.Kind {
	case ir.ToolClientSearch:
		return "search"
	case ir.ToolFunction:
		if t.Name == "" {
			return ""
		}
		return "fn\x00" + t.Namespace + "\x00" + t.Name + "\x00" +
			fingerprintIRToolShape(ir.ToolFunction, t.Name, t.Description, t.InputSchema, t.Strict)
	case ir.ToolCustom:
		if t.Name == "" {
			return ""
		}
		return "custom\x00" + t.Namespace + "\x00" + t.Name + "\x00" +
			fingerprintIRToolShape(ir.ToolCustom, t.Name, t.Description, t.Format, nil)
	case ir.ToolHosted:
		if t.Hosted == nil {
			return ""
		}
		return "hosted\x00" + string(bytes.TrimSpace(t.Hosted.Raw))
	default:
		return ""
	}
}

// writeRebuiltInputOnScope writes rebuiltIn back to scope["input"] as either a
// single JSON object when singleObject is true with one element, or a compact
// JSON array otherwise. Any encoding failure returns a non-nil error.
func writeRebuiltInputOnScope(
	scope []rawMember,
	rebuiltIn []jsontext.Value,
	singleObject bool,
) ([]rawMember, error) {
	if singleObject && len(rebuiltIn) == 1 {
		return setRawMember(scope, "input", rebuiltIn[0]), nil
	}
	encIn, err := encodeRawArray(rebuiltIn)
	if err != nil {
		return nil, err
	}
	return setRawMember(scope, "input", encIn), nil
}

// mergeInheritedForHistoryRewrite returns a detached ToolMap combining active
// declarations with non-conflicting inherited namespace and custom call
// mappings. Active FunctionTools take precedence and shadow inherited entries
// sharing the same upstream identifier.
func mergeInheritedForHistoryRewrite(active, inherited *ToolMap) *ToolMap {
	merged := active.Clone()
	if merged == nil {
		merged = &ToolMap{}
	}
	if inherited == nil {
		return merged
	}
	if merged.CustomTools == nil && len(inherited.CustomTools) > 0 {
		merged.CustomTools = make(map[string]bool, len(inherited.CustomTools))
	}
	for k, v := range inherited.CustomTools {
		if !merged.FunctionTools[k] {
			merged.CustomTools[k] = v
		}
	}
	if merged.CustomNames == nil && len(inherited.CustomNames) > 0 {
		merged.CustomNames = make(map[string]NamespacedName, len(inherited.CustomNames))
	}
	for k, ident := range inherited.CustomNames {
		if _, exists := merged.CustomNames[k]; !exists && !merged.FunctionTools[k] {
			merged.CustomNames[k] = ident
		}
	}
	if merged.ScopedCustom == nil && len(inherited.ScopedCustom) > 0 {
		merged.ScopedCustom = make(map[NamespacedName]NamespacedName, len(inherited.ScopedCustom))
	}
	for k, ident := range inherited.ScopedCustom {
		if _, exists := merged.ScopedCustom[k]; !exists {
			merged.ScopedCustom[k] = ident
		}
	}
	if merged.CallNames == nil && len(inherited.CallNames) > 0 {
		merged.CallNames = make(map[NamespacedName]string, len(inherited.CallNames))
	}
	for ident, up := range inherited.CallNames {
		if _, exists := merged.CallNames[ident]; !exists && !merged.FunctionTools[up] {
			merged.CallNames[ident] = up
		}
	}
	if merged.NamespaceTools == nil && len(inherited.NamespaceTools) > 0 {
		merged.NamespaceTools = make(map[string]NamespacedName, len(inherited.NamespaceTools))
	}
	for up, ident := range inherited.NamespaceTools {
		if _, exists := merged.NamespaceTools[up]; !exists && !merged.FunctionTools[up] {
			merged.NamespaceTools[up] = ident
		}
	}
	return merged
}

// rebuildClientToolsFromInherited reconstructs client-shaped tool JSON items
// from inherited.LoweredTools and inherited identity maps for WebSocket
// continuation turns that omit tools[]. Non-object entries and unmapped
// function or hosted declarations return independent byte clones.
//
// Proxy tool_search functions revert to tool_search declarations, custom
// functions restore their original unsanitized client names and optional
// namespace wrappers, and flattened or preserved namespace containers restore
// child identities so lowerTools reproduces identical mappings.
func rebuildClientToolsFromInherited(lowered []jsontext.Value, inherited *ToolMap) ([]jsontext.Value, error) {
	out := make([]jsontext.Value, 0, len(lowered))
	for _, raw := range lowered {
		if raw.Kind() != '{' {
			out = append(out, jsontext.Value(bytes.Clone(raw)))
			continue
		}
		m, err := parseOrderedObject(raw)
		if err != nil {
			return nil, err
		}
		typ, _ := readMemberString(m, "type")
		typ = strings.TrimSpace(typ)
		name, _ := readMemberString(m, "name")
		name = strings.TrimSpace(name)

		if typ == "namespace" {
			rebuiltNS, nsErr := rebuildInheritedNamespaceEntry(raw, m, name, inherited)
			if nsErr != nil {
				return nil, nsErr
			}
			out = append(out, rebuiltNS)
			continue
		}

		switch {
		case inherited.IsToolSearchProxy(name):
			tsMembers := []rawMember{{name: "type", value: mustMarshalString("tool_search")}}
			if rawDesc, hasDesc := findRawMember(m, "description"); hasDesc {
				tsMembers = append(tsMembers, rawMember{name: "description", value: rawDesc})
			}
			ts, err := encodeOrderedObject(tsMembers)
			if err != nil {
				return nil, err
			}
			out = append(out, ts)

		case inherited.IsCustomTool(name):
			orig, _ := inherited.LookupCustomTool(name)
			m = setStringMember(m, "type", "custom")
			if orig.Name != "" {
				m = setStringMember(m, "name", orig.Name)
			}
			m = deleteRawMember(m, "parameters")
			childEnc, err := encodeOrderedObject(m)
			if err != nil {
				return nil, err
			}
			if orig.Namespace == "" {
				out = append(out, childEnc)
				continue
			}
			childArr, err := encodeRawArray([]jsontext.Value{childEnc})
			if err != nil {
				return nil, err
			}
			nsObj, err := encodeOrderedObject([]rawMember{
				{name: "type", value: mustMarshalString("namespace")},
				{name: "name", value: mustMarshalString(orig.Namespace)},
				{name: "tools", value: childArr},
			})
			if err != nil {
				return nil, err
			}
			out = append(out, nsObj)

		case inherited.NamespaceTools[name].Namespace != "" && !inherited.FunctionTools[name]:
			ident := inherited.NamespaceTools[name]
			m = setStringMember(m, "type", "function")
			m = setStringMember(m, "name", ident.Name)
			childEnc, err := encodeOrderedObject(m)
			if err != nil {
				return nil, err
			}
			childArr, err := encodeRawArray([]jsontext.Value{childEnc})
			if err != nil {
				return nil, err
			}
			nsObj, err := encodeOrderedObject([]rawMember{
				{name: "type", value: mustMarshalString("namespace")},
				{name: "name", value: mustMarshalString(ident.Namespace)},
				{name: "tools", value: childArr},
			})
			if err != nil {
				return nil, err
			}
			out = append(out, nsObj)

		default:
			out = append(out, jsontext.Value(bytes.Clone(raw)))
		}
	}
	return out, nil
}

// rebuildInheritedNamespaceEntry restores lowered custom child declarations
// inside a preserved namespace wrapper stored in inherited.LoweredTools.
// Namespaces with no lowered custom children return a detached clone of raw.
func rebuildInheritedNamespaceEntry(
	raw jsontext.Value,
	m []rawMember,
	nsName string,
	inherited *ToolMap,
) (jsontext.Value, error) {
	if inherited == nil || len(inherited.ScopedCustom) == 0 {
		return jsontext.Value(bytes.Clone(raw)), nil
	}
	childKey := "tools"
	childrenRaw, hasTools := findRawMember(m, "tools")
	if !hasTools || childrenRaw.Kind() != '[' {
		childKey = "children"
		childrenRaw, _ = findRawMember(m, "children")
	}
	if len(childrenRaw) == 0 || childrenRaw.Kind() != '[' {
		return jsontext.Value(bytes.Clone(raw)), nil
	}
	chElems, err := parseRawArray(childrenRaw)
	if err != nil {
		return nil, err
	}
	anyChanged := false
	rebuiltChildren := make([]jsontext.Value, len(chElems))
	for i, ch := range chElems {
		if ch.Kind() != '{' {
			rebuiltChildren[i] = ch
			continue
		}
		cm, cErr := parseOrderedObject(ch)
		if cErr != nil {
			return nil, cErr
		}
		cName, _ := readMemberString(cm, "name")
		orig, isScopedCustom := inherited.LookupCustomCall(nsName, strings.TrimSpace(cName))
		if !isScopedCustom {
			rebuiltChildren[i] = ch
			continue
		}
		cm = setStringMember(cm, "type", "custom")
		if orig.Name != "" {
			cm = setStringMember(cm, "name", orig.Name)
		}
		cm = deleteRawMember(cm, "parameters")
		encCh, cErr := encodeOrderedObject(cm)
		if cErr != nil {
			return nil, cErr
		}
		rebuiltChildren[i] = encCh
		anyChanged = true
	}
	if !anyChanged {
		return jsontext.Value(bytes.Clone(raw)), nil
	}
	encArr, err := encodeRawArray(rebuiltChildren)
	if err != nil {
		return nil, err
	}
	m = setRawMember(m, childKey, encArr)
	return encodeOrderedObject(m)
}

// parseRawResponsesToolsForLowering extracts IR Tool and Namespace slices from
// rawTools alongside top-level container descriptors and per-IR-slot
// rawToolOrigin records so untouched tools, namespace wrappers, and lowered
// child objects retain all original JSON members and numeric tokens.
// Unrecognized tool types stay wrapped as ToolHosted opaque items.
func parseRawResponsesToolsForLowering(
	rawTools []jsontext.Value,
) ([]ir.Tool, []ir.Namespace, []rawTopToolEntry, []rawToolOrigin, error) {
	var tools []ir.Tool
	var namespaces []ir.Namespace
	topEntries := make([]rawTopToolEntry, len(rawTools))
	var origins []rawToolOrigin

	for topIdx, raw := range rawTools {
		topEntries[topIdx] = rawTopToolEntry{raw: raw}
		if raw.Kind() != '{' {
			origins = append(origins, rawToolOrigin{raw: raw, topIndex: topIdx})
			tools = append(tools, ir.Tool{
				Kind:   ir.ToolHosted,
				Hosted: &ir.OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Raw: raw},
			})
			continue
		}
		m, err := parseOrderedObject(raw)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		typ, _ := readMemberString(m, "type")
		typ = strings.TrimSpace(typ)
		name, _ := readMemberString(m, "name")
		name = strings.TrimSpace(name)
		desc, _ := readMemberString(m, "description")
		deferPtr := readMemberBoolPtr(m, "defer_loading")
		strictPtr := readMemberBoolPtr(m, "strict")

		switch typ {
		case "function":
			params, _ := findRawMember(m, "parameters")
			origins = append(origins, rawToolOrigin{raw: raw, topIndex: topIdx})
			tools = append(tools, ir.Tool{
				Kind:         ir.ToolFunction,
				Name:         name,
				Description:  desc,
				InputSchema:  params,
				Strict:       strictPtr,
				DeferLoading: deferPtr,
			})
		case "custom":
			format, _ := findRawMember(m, "format")
			origins = append(origins, rawToolOrigin{raw: raw, topIndex: topIdx})
			tools = append(tools, ir.Tool{
				Kind:         ir.ToolCustom,
				Name:         name,
				Description:  desc,
				Format:       format,
				DeferLoading: deferPtr,
			})
		case "tool_search":
			origins = append(origins, rawToolOrigin{raw: raw, topIndex: topIdx})
			tools = append(tools, ir.Tool{
				Kind:         ir.ToolClientSearch,
				Name:         name,
				Description:  desc,
				Strict:       strictPtr,
				DeferLoading: deferPtr,
			})
		case "namespace":
			topEntries[topIdx].isNamespace = true
			topEntries[topIdx].nsName = name
			if name != "" {
				namespaces = append(namespaces, ir.Namespace{
					Name:         name,
					Description:  desc,
					DeferLoading: deferPtr,
				})
			}
			childKey := "tools"
			childrenRaw, hasTools := findRawMember(m, "tools")
			if !hasTools || childrenRaw.Kind() != '[' {
				if altRaw, hasAlt := findRawMember(m, "children"); hasAlt && altRaw.Kind() == '[' {
					childKey = "children"
					childrenRaw = altRaw
				}
			}
			topEntries[topIdx].childKey = childKey
			if len(childrenRaw) > 0 && childrenRaw.Kind() == '[' {
				chElems, err := parseRawArray(childrenRaw)
				if err != nil {
					return nil, nil, nil, nil, err
				}
				topEntries[topIdx].rawChildren = chElems
				for childIdx, ch := range chElems {
					if ch.Kind() != '{' {
						continue
					}
					cm, err := parseOrderedObject(ch)
					if err != nil {
						return nil, nil, nil, nil, err
					}
					cType, _ := readMemberString(cm, "type")
					cName, _ := readMemberString(cm, "name")
					cName = strings.TrimSpace(cName)
					cDesc, _ := readMemberString(cm, "description")
					cDefer := readMemberBoolPtr(cm, "defer_loading")
					cStrict := readMemberBoolPtr(cm, "strict")
					switch strings.TrimSpace(cType) {
					case "function":
						cParams, _ := findRawMember(cm, "parameters")
						origins = append(origins, rawToolOrigin{
							raw:           ch,
							fromNamespace: true,
							topIndex:      topIdx,
							childIndex:    childIdx,
						})
						tools = append(tools, ir.Tool{
							Kind:         ir.ToolFunction,
							Namespace:    name,
							Name:         cName,
							Description:  cDesc,
							InputSchema:  cParams,
							Strict:       cStrict,
							DeferLoading: cDefer,
						})
					case "custom":
						cFmt, _ := findRawMember(cm, "format")
						origins = append(origins, rawToolOrigin{
							raw:           ch,
							fromNamespace: true,
							topIndex:      topIdx,
							childIndex:    childIdx,
						})
						tools = append(tools, ir.Tool{
							Kind:         ir.ToolCustom,
							Namespace:    name,
							Name:         cName,
							Description:  cDesc,
							Format:       cFmt,
							DeferLoading: cDefer,
						})
					}
				}
			}
		default:
			origins = append(origins, rawToolOrigin{raw: raw, topIndex: topIdx})
			tools = append(tools, ir.Tool{
				Kind:         ir.ToolHosted,
				Name:         name,
				DeferLoading: deferPtr,
				Hosted:       &ir.OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: typ, Raw: raw},
			})
		}
	}
	return tools, namespaces, topEntries, origins, nil
}

// extractToolSearchMessagesFromRawInput extracts completed tool_search_output
// tool arrays from inputElems into synthetic RoleTool messages so lowerTools
// promotes discovered declarations into active tools. Incomplete, failed, or
// non-array items are skipped without failing request adaptation.
func extractToolSearchMessagesFromRawInput(inputElems []jsontext.Value) []ir.Message {
	var msgs []ir.Message
	for _, item := range inputElems {
		if item.Kind() != '{' {
			continue
		}
		m, err := parseOrderedObject(item)
		if err != nil {
			continue
		}
		typ, _ := readMemberString(m, "type")
		if strings.TrimSpace(typ) != "tool_search_output" {
			continue
		}
		if _, hasStatus := findRawMember(m, "status"); hasStatus {
			statusStr, ok := readMemberString(m, "status")
			if !ok || strings.TrimSpace(statusStr) != "completed" {
				continue
			}
		}
		rawDisc, ok := findRawMember(m, "tools")
		if !ok || rawDisc.Kind() != '[' {
			continue
		}
		callID, _ := readMemberString(m, "call_id")
		msgs = append(msgs, ir.Message{
			Role: ir.RoleTool,
			Parts: []ir.Part{{
				Kind: ir.PartToolResult,
				Result: &ir.ToolResult{
					CallID:  callID,
					Tool:    ir.ToolRef{Kind: ir.ToolClientSearch},
					Content: []ir.Part{{Kind: ir.PartText, Text: string(rawDisc)}},
				},
			}},
		})
	}
	return msgs
}

// serializeAdaptedResponsesTools serializes loweredIR using topEntries,
// origins, and coords so untouched top-level function, hosted, and preserved
// namespace tools stay byte-for-byte identical across empty, single-child, and
// multi-child groups. It reports whether any tools[] element changed.
//
// When needNamespace is true, namespace wrappers flatten into top-level
// functions while updating only modified JSON members on each child object.
// When needNamespace is false, outer namespace containers keep their raw JSON
// unless a child custom tool lowered, defer_loading stripped, or discoveries
// appended into that group.
func serializeAdaptedResponsesTools(
	topEntries []rawTopToolEntry,
	origins []rawToolOrigin,
	coords *sourceCoords,
	loweredIR []ir.Tool,
	maxBytes int,
	needCustom, needNamespace, needSearch bool,
) ([]jsontext.Value, bool, error) {
	needStripDefer := needSearch && !hasClientSearchTool(loweredIR)
	if !needNamespace {
		return serializeNamespacePreservingTools(
			topEntries,
			origins,
			coords,
			loweredIR,
			maxBytes,
			needStripDefer,
			needCustom,
			needSearch,
		)
	}

	changed := len(loweredIR) != len(origins)
	for _, top := range topEntries {
		if top.isNamespace {
			changed = true
			break
		}
	}
	out := make([]jsontext.Value, 0, len(loweredIR))

	for i, lt := range loweredIR {
		srcIdx := coords.origToolIndex(i)
		if srcIdx >= 0 && srcIdx < len(origins) {
			orig := origins[srcIdx]
			if orig.fromNamespace {
				changed = true
			}
			rendered, itemChanged, handled, err := renderOriginBackedTool(
				orig.raw,
				lt,
				needStripDefer,
				needCustom,
				needSearch,
			)
			if err != nil {
				return nil, false, err
			}
			if handled {
				if itemChanged {
					changed = true
				}
				out = append(out, rendered)
				continue
			}
		}

		changed = true
		enc, err := marshalIRToolAsResponsesWire(lt, maxBytes, true)
		if err != nil {
			return nil, false, err
		}
		out = append(out, enc)
	}
	return out, changed, nil
}

// serializeNamespacePreservingTools serializes loweredIR when the target
// dialect natively supports namespace wrappers (needNamespace is false).
// Untouched empty, single-child, and multi-child namespace containers retain
// their exact raw JSON bytes without marking tools[] rewritten.
//
// Groups whose child custom tools lowered, whose defer_loading flags stripped,
// or whose tool_search discoveries promoted into an existing namespace rewrite
// only their affected child array or outer members in place. Newly discovered
// namespaces append as trailing namespace objects in discovery order.
func serializeNamespacePreservingTools(
	topEntries []rawTopToolEntry,
	origins []rawToolOrigin,
	coords *sourceCoords,
	loweredIR []ir.Tool,
	maxBytes int,
	needStripDefer, needCustom, needSearch bool,
) ([]jsontext.Value, bool, error) {
	changed := false
	topPresent := make([]bool, len(topEntries))
	topRendered := make([]jsontext.Value, len(topEntries))
	nsChildren := make([][]jsontext.Value, len(topEntries))
	nsChanged := make([]bool, len(topEntries))
	for g, top := range topEntries {
		if top.isNamespace && len(top.rawChildren) > 0 {
			nsChildren[g] = slices.Clone(top.rawChildren)
		}
	}

	var promotedTop []jsontext.Value
	promotedByNS := make(map[string][]jsontext.Value)
	var promotedNSOrder []string

	for i, lt := range loweredIR {
		srcIdx := coords.origToolIndex(i)
		if srcIdx >= 0 && srcIdx < len(origins) {
			orig := origins[srcIdx]
			rendered, itemChanged, handled, err := renderOriginBackedTool(
				orig.raw,
				lt,
				needStripDefer,
				needCustom,
				needSearch,
			)
			if err != nil {
				return nil, false, err
			}
			if !handled {
				rendered, err = marshalIRToolAsResponsesWire(lt, maxBytes, false)
				if err != nil {
					return nil, false, err
				}
				itemChanged = true
			}
			if orig.fromNamespace {
				if orig.topIndex >= 0 && orig.topIndex < len(nsChildren) &&
					orig.childIndex >= 0 && orig.childIndex < len(nsChildren[orig.topIndex]) {
					nsChildren[orig.topIndex][orig.childIndex] = rendered
					if itemChanged {
						nsChanged[orig.topIndex] = true
						changed = true
					}
				}
			} else if orig.topIndex >= 0 && orig.topIndex < len(topEntries) {
				topPresent[orig.topIndex] = true
				topRendered[orig.topIndex] = rendered
				if itemChanged {
					changed = true
				}
			}
			continue
		}

		changed = true
		enc, err := marshalIRToolAsResponsesWire(lt, maxBytes, false)
		if err != nil {
			return nil, false, err
		}
		if lt.Namespace == "" {
			promotedTop = append(promotedTop, enc)
		} else {
			if _, seen := promotedByNS[lt.Namespace]; !seen {
				promotedNSOrder = append(promotedNSOrder, lt.Namespace)
			}
			promotedByNS[lt.Namespace] = append(promotedByNS[lt.Namespace], enc)
		}
	}

	out := make([]jsontext.Value, 0, len(topEntries)+len(promotedTop)+len(promotedNSOrder))
	for g, top := range topEntries {
		if !top.isNamespace {
			if !topPresent[g] {
				changed = true
				continue
			}
			out = append(out, topRendered[g])
			continue
		}

		childrenSlice := nsChildren[g]
		if extraPromoted := promotedByNS[top.nsName]; len(extraPromoted) > 0 {
			childrenSlice = append(childrenSlice, extraPromoted...)
			nsChanged[g] = true
			changed = true
			delete(promotedByNS, top.nsName)
		}

		outerM, mErr := parseOrderedObject(top.raw)
		if mErr != nil {
			return nil, false, mErr
		}
		_, hasOuterDefer := findRawMember(outerM, "defer_loading")
		stripOuterDefer := needStripDefer && hasOuterDefer
		if !nsChanged[g] && !stripOuterDefer {
			out = append(out, top.raw)
			continue
		}

		if nsChanged[g] {
			encChildren, cErr := encodeRawArray(childrenSlice)
			if cErr != nil {
				return nil, false, cErr
			}
			key := top.childKey
			if key == "" {
				key = "tools"
			}
			outerM = setRawMember(outerM, key, encChildren)
		}
		if stripOuterDefer {
			outerM = deleteRawMember(outerM, "defer_loading")
		}
		encNS, encErr := encodeOrderedObject(outerM)
		if encErr != nil {
			return nil, false, encErr
		}
		out = append(out, encNS)
		changed = true
	}

	out = append(out, promotedTop...)
	for _, nsName := range promotedNSOrder {
		rem := promotedByNS[nsName]
		if len(rem) == 0 {
			continue
		}
		encArr, err := encodeRawArray(rem)
		if err != nil {
			return nil, false, err
		}
		encNS, err := encodeOrderedObject([]rawMember{
			{name: "type", value: mustMarshalString("namespace")},
			{name: "name", value: mustMarshalString(nsName)},
			{name: "tools", value: encArr},
		})
		if err != nil {
			return nil, false, err
		}
		out = append(out, encNS)
	}
	return out, changed, nil
}

// renderOriginBackedTool updates origRaw in place to match lowered IR tool lt
// while keeping untouched JSON members verbatim. Unrecognized origin transitions
// return handled=false so callers fall back to marshalIRToolAsResponsesWire.
func renderOriginBackedTool(
	origRaw jsontext.Value,
	lt ir.Tool,
	needStripDefer, needCustom, needSearch bool,
) (jsontext.Value, bool, bool, error) {
	if lt.Kind == ir.ToolHosted {
		if needStripDefer && origRaw.Kind() == '{' {
			if m, err := parseOrderedObject(origRaw); err == nil {
				if _, hasD := findRawMember(m, "defer_loading"); hasD {
					m = deleteRawMember(m, "defer_loading")
					enc, encErr := encodeOrderedObject(m)
					if encErr != nil {
						return nil, false, false, encErr
					}
					return enc, true, true, nil
				}
			}
		}
		return origRaw, false, true, nil
	}
	if origRaw.Kind() != '{' {
		return nil, false, false, nil
	}
	m, err := parseOrderedObject(origRaw)
	if err != nil {
		return nil, false, false, nil
	}
	origType, _ := readMemberString(m, "type")
	origType = strings.TrimSpace(origType)
	origName, _ := readMemberString(m, "name")
	origName = strings.TrimSpace(origName)

	switch {
	case origType == "function" && lt.Kind == ir.ToolFunction:
		itemChanged := false
		if lt.Name != origName {
			m = setStringMember(m, "name", lt.Name)
			itemChanged = true
		}
		if needStripDefer {
			if _, hasD := findRawMember(m, "defer_loading"); hasD {
				m = deleteRawMember(m, "defer_loading")
				itemChanged = true
			}
		}
		if !itemChanged {
			return origRaw, false, true, nil
		}
		enc, encErr := encodeOrderedObject(m)
		if encErr != nil {
			return nil, false, false, encErr
		}
		return enc, true, true, nil

	case origType == "custom" && needCustom && lt.Kind == ir.ToolFunction:
		m = setStringMember(m, "type", "function")
		m = setStringMember(m, "name", lt.Name)
		m = setRawMember(m, "parameters", jsontext.Value(customToolInputSchema))
		m = deleteRawMember(m, "format")
		if needStripDefer {
			m = deleteRawMember(m, "defer_loading")
		}
		enc, encErr := encodeOrderedObject(m)
		if encErr != nil {
			return nil, false, false, encErr
		}
		return enc, true, true, nil

	case origType == "custom" && !needCustom && lt.Kind == ir.ToolCustom:
		itemChanged := false
		if lt.Name != origName {
			m = setStringMember(m, "name", lt.Name)
			itemChanged = true
		}
		if needStripDefer {
			if _, hasD := findRawMember(m, "defer_loading"); hasD {
				m = deleteRawMember(m, "defer_loading")
				itemChanged = true
			}
		}
		if !itemChanged {
			return origRaw, false, true, nil
		}
		enc, encErr := encodeOrderedObject(m)
		if encErr != nil {
			return nil, false, false, encErr
		}
		return enc, true, true, nil

	case origType == "tool_search" && !needSearch:
		return origRaw, false, true, nil
	}
	return nil, false, false, nil
}

// marshalIRToolAsResponsesWire serializes one lowered or promoted ir.Tool into
// a Responses tools[] JSON object within maxBytes while preserving carried
// extension fields. Hosted tools with raw wire bytes return a detached clone,
// and flattenNamespace controls whether t.Namespace prefixes t.Name.
func marshalIRToolAsResponsesWire(t ir.Tool, maxBytes int, flattenNamespace bool) (jsontext.Value, error) {
	if t.Kind == ir.ToolHosted && t.Hosted != nil && len(t.Hosted.Raw) > 0 {
		return jsontext.Value(bytes.Clone(t.Hosted.Raw)), nil
	}
	name := t.Name
	if flattenNamespace && t.Namespace != "" {
		name = flattenNamespaceToolName(t.Namespace, name, maxBytes)
	}
	switch t.Kind {
	case ir.ToolCustom:
		members := []rawMember{
			{name: "type", value: mustMarshalString("custom")},
			{name: "name", value: mustMarshalString(name)},
		}
		if t.Description != "" {
			members = append(members, rawMember{name: "description", value: mustMarshalString(t.Description)})
		}
		if len(t.Format) > 0 {
			members = append(members, rawMember{name: "format", value: t.Format})
		}
		if t.DeferLoading != nil {
			members = append(members, rawMember{name: "defer_loading", value: marshalBoolRaw(*t.DeferLoading)})
		}
		members = appendSortedExtraMembers(members, t.Extra)
		return encodeOrderedObject(members)

	case ir.ToolClientSearch:
		members := []rawMember{
			{name: "type", value: mustMarshalString("tool_search")},
		}
		if t.Description != "" {
			members = append(members, rawMember{name: "description", value: mustMarshalString(t.Description)})
		}
		members = appendSortedExtraMembers(members, t.Extra)
		return encodeOrderedObject(members)

	default:
		members := []rawMember{
			{name: "type", value: mustMarshalString("function")},
			{name: "name", value: mustMarshalString(name)},
		}
		if t.Description != "" {
			members = append(members, rawMember{name: "description", value: mustMarshalString(t.Description)})
		}
		if len(t.InputSchema) > 0 {
			members = append(members, rawMember{name: "parameters", value: t.InputSchema})
		}
		if t.Strict != nil {
			members = append(members, rawMember{name: "strict", value: marshalBoolRaw(*t.Strict)})
		}
		if t.DeferLoading != nil {
			members = append(members, rawMember{name: "defer_loading", value: marshalBoolRaw(*t.DeferLoading)})
		}
		members = appendSortedExtraMembers(members, t.Extra)
		return encodeOrderedObject(members)
	}
}

// appendSortedExtraMembers appends non-colliding extension entries from extra
// onto members in deterministic lexical key order without direct map indexing.
// Zero Extra values or keys already present on members are skipped.
func appendSortedExtraMembers(members []rawMember, extra ir.Extra) []rawMember {
	if extra.IsZero() {
		return members
	}
	var pairs []rawMember
	for k, v := range extra.Members {
		if _, exists := findRawMember(members, k); !exists {
			pairs = append(pairs, rawMember{name: k, value: v})
		}
	}
	slices.SortFunc(pairs, func(a, b rawMember) int {
		return strings.Compare(a.name, b.name)
	})
	return append(members, pairs...)
}

// collectResponsesInputHistoryMap extracts declaration evidence from
// additional_tools and tool_search_output items when tools[] is absent or
// explicitly reset. Non-nil inherited mappings merge afterward and return an
// error if historical declarations conflict with inherited session identities.
func collectResponsesInputHistoryMap(
	inputElems []jsontext.Value,
	inherited *ToolMap,
	maxBytes int,
) (*ToolMap, error) {
	tm := &ToolMap{
		CustomTools:        make(map[string]bool),
		CustomNames:        make(map[string]NamespacedName),
		ScopedCustom:       make(map[NamespacedName]NamespacedName),
		NamespaceTools:     make(map[string]NamespacedName),
		CallNames:          make(map[NamespacedName]string),
		FunctionTools:      make(map[string]bool),
		DeclaredNamespaces: make(map[string]bool),
	}
	if err := mergeRawHistoryIdentityEvidence(tm, inputElems, maxBytes); err != nil {
		return nil, err
	}
	if inherited != nil {
		maps.Copy(tm.CustomTools, inherited.CustomTools)
		maps.Copy(tm.CustomNames, inherited.CustomNames)
		maps.Copy(tm.ScopedCustom, inherited.ScopedCustom)
		if inherited.ToolSearch {
			tm.ToolSearch = true
		}
		for ident, up := range inherited.CallNames {
			if cur, exists := tm.CallNames[ident]; exists && cur != up {
				return nil, fmt.Errorf("historical tool identity conflicts with inherited mapping")
			}
			tm.CallNames[ident] = up
		}
		for up, ident := range inherited.NamespaceTools {
			if cur, exists := tm.NamespaceTools[up]; exists && cur != ident {
				return nil, fmt.Errorf("historical upstream tool name conflicts with inherited mapping")
			}
			tm.NamespaceTools[up] = ident
		}
		maps.Copy(tm.DeclaredNamespaces, inherited.DeclaredNamespaces)
	}
	return tm, nil
}

// mergeRawHistoryIdentityEvidence registers tool declarations embedded in
// completed tool_search_output and additional_tools items into tm while
// rejecting executable name collisions against active tools. Incomplete search
// outputs, non-declaration items, and null tool arrays are skipped without
// mutating tm, whereas non-array tools fields return an error.
//
// Plain functions discovered through tool_search_output or additional_tools
// also register {Namespace: fnName, Name: fnName} aliases so Codex calls that
// repeat the function's own name in namespace resolve and restore cleanly.
func mergeRawHistoryIdentityEvidence(tm *ToolMap, inputElems []jsontext.Value, maxBytes int) error {
	if tm.CustomTools == nil {
		tm.CustomTools = make(map[string]bool)
	}
	if tm.CustomNames == nil {
		tm.CustomNames = make(map[string]NamespacedName)
	}
	if tm.NamespaceTools == nil {
		tm.NamespaceTools = make(map[string]NamespacedName)
	}
	if tm.CallNames == nil {
		tm.CallNames = make(map[NamespacedName]string)
	}
	if tm.FunctionTools == nil {
		tm.FunctionTools = make(map[string]bool)
	}
	if tm.DeclaredNamespaces == nil {
		tm.DeclaredNamespaces = make(map[string]bool)
	}

	owners := make(map[string]string)
	for up := range tm.FunctionTools {
		owners[up] = fmt.Sprintf("function:%q:%q", "", up)
	}
	for up := range tm.CustomTools {
		canon := NamespacedName{Name: up}
		if orig, ok := tm.CustomNames[up]; ok {
			canon = orig
		}
		owners[up] = fmt.Sprintf("custom:%q:%q", canon.Namespace, canon.Name)
	}
	for up, canon := range tm.NamespaceTools {
		if tm.FunctionTools[up] && canon.Namespace == canon.Name && canon.Name == up {
			owners[up] = fmt.Sprintf("function:%q:%q", "", up)
		} else {
			owners[up] = fmt.Sprintf("function:%q:%q", canon.Namespace, canon.Name)
		}
	}

	discoveredFunctions := make(map[string]bool)
	for _, item := range inputElems {
		if item.Kind() != '{' {
			continue
		}
		m, err := parseOrderedObject(item)
		if err != nil {
			continue
		}
		typ, _ := readMemberString(m, "type")
		typ = strings.TrimSpace(typ)
		if typ != "tool_search_output" && typ != "additional_tools" {
			continue
		}
		if typ == "tool_search_output" {
			if _, hasStatus := findRawMember(m, "status"); hasStatus {
				st, ok := readMemberString(m, "status")
				if !ok || strings.TrimSpace(st) != "completed" {
					continue
				}
			}
		}
		rawTools, ok := findRawMember(m, "tools")
		if !ok || rawTools.Kind() == 'n' {
			continue
		}
		if rawTools.Kind() != '[' {
			return fmt.Errorf("%s tools must be an array", typ)
		}
		var histTools []wireDiscoveredTool
		if err := unmarshalPermissive(rawTools, &histTools); err != nil {
			return err
		}
		for _, ht := range histTools {
			htType := strings.TrimSpace(ht.Type)
			htName := strings.TrimSpace(ht.Name)
			switch htType {
			case "namespace":
				if htName == "" {
					continue
				}
				tm.DeclaredNamespaces[htName] = true
				children := ht.Tools
				if len(children) == 0 {
					children = ht.Children
				}
				for _, ch := range children {
					cType := strings.TrimSpace(ch.Type)
					cName := strings.TrimSpace(ch.Name)
					if cName == "" {
						continue
					}
					switch cType {
					case "function":
						up := flattenNamespaceToolName(htName, cName, maxBytes)
						canon := NamespacedName{Namespace: htName, Name: cName}
						if canon.Namespace == defaultFunctionsNamespace {
							canon.Namespace = ""
						}
						owner := fmt.Sprintf("function:%q:%q", canon.Namespace, canon.Name)
						if prev, exists := owners[up]; exists && prev != owner {
							return fmt.Errorf("namespace tool %q/%q conflicts with upstream function name %q", htName, cName, up)
						}
						owners[up] = owner
						tm.CallNames[NamespacedName{Namespace: htName, Name: cName}] = up
						tm.NamespaceTools[up] = canon
						if canon.Namespace == "" {
							tm.CallNames[canon] = up
							tm.FunctionTools[up] = true
						}
					case "custom":
						up, _ := sanitizeExecutableToolName(flattenNamespaceToolName(htName, cName, maxBytes), maxBytes)
						canon := NamespacedName{Namespace: htName, Name: cName}
						if canon.Namespace == defaultFunctionsNamespace {
							canon.Namespace = ""
						}
						owner := fmt.Sprintf("custom:%q:%q", canon.Namespace, canon.Name)
						if prev, exists := owners[up]; exists && prev != owner {
							return fmt.Errorf("namespace custom tool %q/%q conflicts with upstream function name %q", htName, cName, up)
						}
						owners[up] = owner
						tm.CallNames[NamespacedName{Namespace: htName, Name: cName}] = up
					}
				}
			case "function":
				if htName == "" {
					continue
				}
				owner := fmt.Sprintf("function:%q:%q", "", htName)
				if prev, exists := owners[htName]; exists && prev != owner {
					return fmt.Errorf("tool %q conflicts with an existing converted tool identity", htName)
				}
				owners[htName] = owner
				tm.CallNames[NamespacedName{Name: htName}] = htName
				tm.CallNames[NamespacedName{Namespace: defaultFunctionsNamespace, Name: htName}] = htName
				tm.FunctionTools[htName] = true
				discoveredFunctions[htName] = true

			case "custom":
				if htName == "" {
					continue
				}
				sanitized, _ := sanitizeExecutableToolName(htName, maxBytes)
				owner := fmt.Sprintf("custom:%q:%q", "", htName)
				if prev, exists := owners[sanitized]; exists && prev != owner {
					return fmt.Errorf("tool %q conflicts with an existing converted tool identity", htName)
				}
				owners[sanitized] = owner
				tm.CallNames[NamespacedName{Name: htName}] = sanitized
				tm.CallNames[NamespacedName{Namespace: defaultFunctionsNamespace, Name: htName}] = sanitized
			}
		}
	}

	// Codex calls to plain functions discovered via tool_search_output or
	// additional_tools place the function's bare name in namespace; register that
	// self-qualified alias unless it collides with a declared namespace tool.
	for fnName := range discoveredFunctions {
		if !tm.FunctionTools[fnName] || fnName == defaultFunctionsNamespace {
			continue
		}
		ident := NamespacedName{Namespace: fnName, Name: fnName}
		if prev, exists := tm.CallNames[ident]; exists && prev != fnName {
			return fmt.Errorf("discovered function %q conflicts with a namespace tool of the same identity", fnName)
		}
		tm.CallNames[ident] = fnName
		tm.NamespaceTools[fnName] = ident
	}
	return nil
}

// rewriteResponsesRawInputItems lowers custom, search, and namespaced call and
// output items in inputElems while keeping all other items (such as compaction,
// raw reasoning tokens, and prompt_cache_breakpoint) byte-for-byte unchanged.
// Disappeared namespace calls receive deterministic collision-free replay names
// from allocateIRHistoryNames without mutating active restoration state in tm.
//
// It reports whether any element changed alongside original-coordinate input[i]
// LossToolLowered entries, and returns an error when historical replay names
// collide or tool_search_output omits a non-empty string call_id.
func rewriteResponsesRawInputItems(
	inputElems []jsontext.Value,
	origIndices []int,
	tm *ToolMap,
	maxBytes int,
	needCustom, needNamespace, needSearch bool,
) ([]jsontext.Value, bool, []ir.Loss, error) {
	var syntheticMsgs []ir.Message
	for _, item := range inputElems {
		if item.Kind() != '{' {
			continue
		}
		m, err := parseOrderedObject(item)
		if err != nil {
			continue
		}
		typ, _ := readMemberString(m, "type")
		switch strings.TrimSpace(typ) {
		case "function_call":
			ns, _ := readMemberString(m, "namespace")
			name, _ := readMemberString(m, "name")
			syntheticMsgs = append(syntheticMsgs, ir.Message{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{Tool: ir.ToolRef{Kind: ir.ToolFunction, Namespace: ns, Name: name}},
				}},
			})
		case "custom_tool_call":
			ns, _ := readMemberString(m, "namespace")
			name, _ := readMemberString(m, "name")
			syntheticMsgs = append(syntheticMsgs, ir.Message{
				Role: ir.RoleAssistant,
				Parts: []ir.Part{{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{Tool: ir.ToolRef{Kind: ir.ToolCustom, Namespace: ns, Name: name}},
				}},
			})
		}
	}
	historyNames, err := allocateIRHistoryNames(syntheticMsgs, tm, maxBytes, needNamespace, false)
	if err != nil {
		return nil, false, nil, err
	}

	var losses []ir.Loss
	changed := false
	out := make([]jsontext.Value, len(inputElems))

	for idx, item := range inputElems {
		if item.Kind() != '{' {
			out[idx] = item
			continue
		}
		m, err := parseOrderedObject(item)
		if err != nil {
			return nil, false, nil, err
		}
		typ, _ := readMemberString(m, "type")
		typ = strings.TrimSpace(typ)
		origIdx := idx
		if idx < len(origIndices) {
			origIdx = origIndices[idx]
		}
		iPath := fmt.Sprintf("input[%d]", origIdx)

		switch typ {
		case "custom_tool_call":
			if needCustom {
				name, _ := readMemberString(m, "name")
				trimmedName := strings.TrimSpace(name)
				ns, _ := readMemberString(m, "namespace")
				trimmedNS := strings.TrimSpace(ns)
				targetName := trimmedName
				if needNamespace {
					if trimmedNS != "" {
						targetName = flattenNamespaceToolName(trimmedNS, targetName, maxBytes)
					}
					m = deleteRawMember(m, "namespace")
				}
				sanitized, _ := sanitizeExecutableToolName(targetName, maxBytes)
				inStr, _ := readMemberString(m, "input")
				m = setStringMember(m, "type", "function_call")
				m = setStringMember(m, "name", sanitized)
				m = setStringMember(m, "arguments", wrapCustomToolInput(inStr))
				m = deleteRawMember(m, "input")
				m = normalizeRawItemIDMember(m)
				enc, err := encodeOrderedObject(m)
				if err != nil {
					return nil, false, nil, err
				}
				out[idx] = enc
				changed = true
				losses = append(losses, ir.Loss{Kind: ir.LossToolLowered, Path: iPath})
				continue
			}
			if needNamespace {
				if _, hasNS := findRawMember(m, "namespace"); hasNS {
					ns, _ := readMemberString(m, "namespace")
					trimmedNS := strings.TrimSpace(ns)
					name, _ := readMemberString(m, "name")
					trimmedName := strings.TrimSpace(name)
					if trimmedNS != "" {
						flat := flattenNamespaceToolName(trimmedNS, trimmedName, maxBytes)
						if flat != name {
							m = setStringMember(m, "name", flat)
						}
					}
					m = deleteRawMember(m, "namespace")
					enc, err := encodeOrderedObject(m)
					if err != nil {
						return nil, false, nil, err
					}
					out[idx] = enc
					changed = true
					if trimmedNS != "" && trimmedNS != defaultFunctionsNamespace {
						losses = append(losses, ir.Loss{Kind: ir.LossToolLowered, Path: iPath})
					}
					continue
				}
			}
			out[idx] = item

		case "custom_tool_call_output":
			if needCustom {
				m = setStringMember(m, "type", "function_call_output")
				m = normalizeRawItemIDMember(m)
				m = normalizeRawToolOutputMember(m)
				enc, err := encodeOrderedObject(m)
				if err != nil {
					return nil, false, nil, err
				}
				out[idx] = enc
				changed = true
				losses = append(losses, ir.Loss{Kind: ir.LossToolLowered, Path: iPath})
				continue
			}
			out[idx] = item

		case "tool_search_call":
			if needSearch {
				argsStr := extractRawArgumentsAsString(m)
				m = setStringMember(m, "type", "function_call")
				m = setStringMember(m, "name", toolSearchProxyName)
				m = setStringMember(m, "arguments", argsStr)
				m = deleteRawMember(m, "execution")
				m = normalizeRawItemIDMember(m)
				enc, err := encodeOrderedObject(m)
				if err != nil {
					return nil, false, nil, err
				}
				out[idx] = enc
				changed = true
				losses = append(losses, ir.Loss{Kind: ir.LossToolLowered, Path: iPath})
				continue
			}
			out[idx] = item

		case "tool_search_output":
			if needSearch {
				callID, _ := readMemberString(m, "call_id")
				if strings.TrimSpace(callID) == "" {
					return nil, false, nil, fmt.Errorf("tool_search_output requires non-empty string call_id")
				}
				m = setStringMember(m, "type", "function_call_output")
				m = normalizeRawItemIDMember(m)
				_, hasOut := findRawMember(m, "output")
				rawDisc, hasDisc := findRawMember(m, "tools")
				if !hasOut && hasDisc {
					m = setRawMember(m, "output", rawDisc)
					hasOut = true
				}
				if hasOut {
					m = normalizeRawToolOutputMember(m)
					m = deleteRawMember(m, "tools")
					m = deleteRawMember(m, "status")
					m = deleteRawMember(m, "execution")
				}
				enc, err := encodeOrderedObject(m)
				if err != nil {
					return nil, false, nil, err
				}
				out[idx] = enc
				changed = true
				losses = append(losses, ir.Loss{Kind: ir.LossToolLowered, Path: iPath})
				continue
			}
			out[idx] = item

		case "function_call":
			if needNamespace {
				_, hasNSField := findRawMember(m, "namespace")
				ns, _ := readMemberString(m, "namespace")
				name, _ := readMemberString(m, "name")
				if hasNSField || ns != "" {
					// Codex treats "functions" as the implicit bare-function scope; strip
					// that default namespace without renaming or recording LossToolLowered.
					resolved := name
					if ns != "" && ns != defaultFunctionsNamespace {
						var ok bool
						resolved, ok = historyNames[NamespacedName{Namespace: ns, Name: name}]
						if !ok {
							return nil, false, nil, fmt.Errorf("cannot convert historical tool call %q in namespace %q", name, ns)
						}
					}
					itemChanged := false
					if resolved != name {
						m = setStringMember(m, "name", resolved)
						itemChanged = true
					}
					if hasNSField {
						m = deleteRawMember(m, "namespace")
						itemChanged = true
					}
					if itemChanged {
						enc, err := encodeOrderedObject(m)
						if err != nil {
							return nil, false, nil, err
						}
						out[idx] = enc
						changed = true
						if ns != "" && ns != defaultFunctionsNamespace {
							losses = append(losses, ir.Loss{Kind: ir.LossToolLowered, Path: iPath})
						}
						continue
					}
				}
			}
			out[idx] = item

		default:
			out[idx] = item
		}
	}
	return out, changed, losses, nil
}

// rewriteResponsesRawToolChoice lowers custom, search, namespace, and
// allowed_tools tool_choice objects on scope while appending a "tool_choice"
// LossToolChoiceApproximated entry when any selector changes. Undeclared
// namespace groups or unresolved qualified selectors return an error.
func rewriteResponsesRawToolChoice(
	scope []rawMember,
	tm *ToolMap,
	rebuiltTools []jsontext.Value,
	maxBytes int,
	needCustom, needNamespace, needSearch bool,
	losses *[]ir.Loss,
) ([]rawMember, bool, error) {
	rawTC, ok := findRawMember(scope, "tool_choice")
	if !ok || rawTC.Kind() != '{' {
		return scope, false, nil
	}
	m, err := parseOrderedObject(rawTC)
	if err != nil {
		return nil, false, err
	}
	typ, _ := readMemberString(m, "type")
	typ = strings.TrimSpace(typ)
	name, _ := readMemberString(m, "name")
	name = strings.TrimSpace(name)

	if needNamespace && typ == "namespace" {
		if isDeclaredNamespaceInMap(tm, name) {
			// Function-only Responses endpoints cannot restrict selection to a
			// flattened multi-tool namespace group; approximate group choice to "auto".
			scope = setRawMember(scope, "tool_choice", jsontext.Value(`"auto"`))
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
			return scope, true, nil
		}
		return nil, false, fmt.Errorf("cannot convert namespace choice %q without a declared group", name)
	}

	if typ == "allowed_tools" {
		updatedM, atChanged, degradeAuto, err := rewriteRawAllowedToolsChoice(m, tm, rebuiltTools, maxBytes, needCustom, needNamespace, needSearch)
		if err != nil {
			return nil, false, err
		}
		if !atChanged {
			return scope, false, nil
		}
		if degradeAuto {
			// Empty declared namespaces inside allowed_tools leave zero callable
			// functions when no sibling entries survive; degrade choice to "auto".
			scope = setRawMember(scope, "tool_choice", jsontext.Value(`"auto"`))
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
			return scope, true, nil
		}
		encTC, err := encodeOrderedObject(updatedM)
		if err != nil {
			return nil, false, err
		}
		scope = setRawMember(scope, "tool_choice", encTC)
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
		return scope, true, nil
	}

	updatedM, tcChanged, err := rewriteSingleRawToolSelector(m, tm, maxBytes, needCustom, needNamespace, needSearch)
	if err != nil {
		return nil, false, err
	}
	if !tcChanged {
		return scope, false, nil
	}
	encTC, err := encodeOrderedObject(updatedM)
	if err != nil {
		return nil, false, err
	}
	scope = setRawMember(scope, "tool_choice", encTC)
	*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
	return scope, true, nil
}

// rewriteRawAllowedToolsChoice lowers custom, search, namespace-group, and
// qualified function entries inside flat tools[] or nested allowed_tools.tools[]
// on an allowed_tools selector. When namespace expansion leaves zero callable
// entries overall, degradeAuto signals the caller to replace tool_choice with "auto".
func rewriteRawAllowedToolsChoice(
	m []rawMember,
	tm *ToolMap,
	rebuiltTools []jsontext.Value,
	maxBytes int,
	needCustom, needNamespace, needSearch bool,
) ([]rawMember, bool, bool, error) {
	changed := false
	degradeAuto := false
	if rawList, ok := findRawMember(m, "tools"); ok && rawList.Kind() == '[' {
		rebuilt, listChanged, emptyAfterNS, err := rewriteRawAllowedToolArray(rawList, tm, rebuiltTools, maxBytes, needCustom, needNamespace, needSearch)
		if err != nil {
			return nil, false, false, err
		}
		if listChanged {
			m = setRawMember(m, "tools", rebuilt)
			changed = true
			if emptyAfterNS {
				degradeAuto = true
			}
		}
	}
	if rawWrap, ok := findRawMember(m, "allowed_tools"); ok && rawWrap.Kind() == '{' {
		wm, err := parseOrderedObject(rawWrap)
		if err != nil {
			return nil, false, false, err
		}
		if rawList, ok := findRawMember(wm, "tools"); ok && rawList.Kind() == '[' {
			rebuilt, listChanged, emptyAfterNS, err := rewriteRawAllowedToolArray(rawList, tm, rebuiltTools, maxBytes, needCustom, needNamespace, needSearch)
			if err != nil {
				return nil, false, false, err
			}
			if listChanged {
				wm = setRawMember(wm, "tools", rebuilt)
				encWrap, err := encodeOrderedObject(wm)
				if err != nil {
					return nil, false, false, err
				}
				m = setRawMember(m, "allowed_tools", encWrap)
				changed = true
				if emptyAfterNS {
					degradeAuto = true
				}
			}
		}
	}
	return m, changed, degradeAuto, nil
}

// rewriteRawAllowedToolArray lowers individual entries inside an allowed_tools
// array, expanding declared namespace entries into their flattened function
// references when needNamespace is true. Undeclared namespace entries return an
// error, while empty expansions with no surviving peers set emptyAfterNS=true.
func rewriteRawAllowedToolArray(
	rawList jsontext.Value,
	tm *ToolMap,
	rebuiltTools []jsontext.Value,
	maxBytes int,
	needCustom, needNamespace, needSearch bool,
) (jsontext.Value, bool, bool, error) {
	elems, err := parseRawArray(rawList)
	if err != nil {
		return nil, false, false, err
	}
	changed := false
	sawEmptyNS := false
	seenFn := make(map[string]bool)
	out := make([]jsontext.Value, 0, len(elems))

	appendUniqueAllowed := func(raw jsontext.Value) {
		if raw.Kind() == '{' {
			if pm, pErr := parseOrderedObject(raw); pErr == nil {
				pType, _ := readMemberString(pm, "type")
				pName, _ := readMemberString(pm, "name")
				_, hasPNS := findRawMember(pm, "namespace")
				if strings.TrimSpace(pType) == "function" && !hasPNS && strings.TrimSpace(pName) != "" {
					key := strings.TrimSpace(pName)
					if seenFn[key] {
						changed = true
						return
					}
					seenFn[key] = true
				}
			}
		}
		out = append(out, raw)
	}

	for _, el := range elems {
		if el.Kind() != '{' {
			out = append(out, el)
			continue
		}
		em, err := parseOrderedObject(el)
		if err != nil {
			return nil, false, false, err
		}
		eType, _ := readMemberString(em, "type")
		eType = strings.TrimSpace(eType)
		if needNamespace && eType == "namespace" {
			nsName, _ := readMemberString(em, "name")
			nsName = strings.TrimSpace(nsName)
			if !isDeclaredNamespaceInMap(tm, nsName) {
				return nil, false, false, fmt.Errorf("cannot convert namespace choice %q without a declared group", nsName)
			}
			expanded := collectNamespaceExecutableNames(tm, rebuiltTools, nsName)
			if len(expanded) == 0 {
				sawEmptyNS = true
			}
			for _, fnName := range expanded {
				encFn, fErr := encodeOrderedObject([]rawMember{
					{name: "type", value: mustMarshalString("function")},
					{name: "name", value: mustMarshalString(fnName)},
				})
				if fErr != nil {
					return nil, false, false, fErr
				}
				appendUniqueAllowed(encFn)
			}
			changed = true
			continue
		}

		updated, itemChanged, err := rewriteSingleRawToolSelector(em, tm, maxBytes, needCustom, needNamespace, needSearch)
		if err != nil {
			return nil, false, false, err
		}
		if !itemChanged {
			appendUniqueAllowed(el)
			continue
		}
		enc, err := encodeOrderedObject(updated)
		if err != nil {
			return nil, false, false, err
		}
		appendUniqueAllowed(enc)
		changed = true
	}
	if !changed {
		return rawList, false, false, nil
	}
	encArr, err := encodeRawArray(out)
	if err != nil {
		return nil, false, false, err
	}
	return encArr, true, sawEmptyNS && len(out) == 0, nil
}

// rewriteSingleRawToolSelector lowers one specific function, custom, or
// tool_search selector object in place across flat name and nested function.name
// shapes. Unresolved namespace qualifications propagate an error from tm.ResolveCallName.
func rewriteSingleRawToolSelector(
	m []rawMember,
	tm *ToolMap,
	maxBytes int,
	needCustom, needNamespace, needSearch bool,
) ([]rawMember, bool, error) {
	typ, _ := readMemberString(m, "type")
	typ = strings.TrimSpace(typ)
	name, _ := readMemberString(m, "name")
	name = strings.TrimSpace(name)
	_, hasNSField := findRawMember(m, "namespace")
	ns, _ := readMemberString(m, "namespace")

	tcChanged := false
	if needNamespace && (hasNSField || ns != "") {
		targetName := name
		useNestedFn := false
		if targetName == "" {
			if rawFn, hasFn := findRawMember(m, "function"); hasFn && rawFn.Kind() == '{' {
				if fnMembers, err := parseOrderedObject(rawFn); err == nil {
					targetName, _ = readMemberString(fnMembers, "name")
					useNestedFn = true
				}
			}
		}
		resolved := targetName
		if ns != "" {
			var err error
			resolved, err = tm.ResolveCallName(ns, targetName)
			if err != nil {
				return nil, false, err
			}
		}
		if useNestedFn {
			if resolved != targetName {
				rawFn, _ := findRawMember(m, "function")
				fnMembers, _ := parseOrderedObject(rawFn)
				fnMembers = setStringMember(fnMembers, "name", resolved)
				encFn, err := encodeOrderedObject(fnMembers)
				if err != nil {
					return nil, false, err
				}
				m = setRawMember(m, "function", encFn)
			}
		} else if resolved != name {
			m = setStringMember(m, "name", resolved)
		}
		m = deleteRawMember(m, "namespace")
		name = resolved
		ns = ""
		tcChanged = true
	}

	if needCustom && typ == "custom" {
		sanitized, _ := sanitizeExecutableToolName(name, maxBytes)
		_, isScopedCustom := tm.LookupCustomCall(ns, sanitized)
		if isScopedCustom || tm.IsCustomTool(sanitized) {
			m = setStringMember(m, "type", "function")
			if sanitized != name {
				m = setStringMember(m, "name", sanitized)
			}
			tcChanged = true
		}
	} else if needSearch && typ == "tool_search" && tm != nil && tm.ToolSearch {
		m = []rawMember{
			{name: "type", value: mustMarshalString("function")},
			{name: "name", value: mustMarshalString(toolSearchProxyName)},
		}
		tcChanged = true
	}
	return m, tcChanged, nil
}

// restoreResponsesEnvelopeMembers restores lowered tool call items inside
// output[], response.output[], data.output[], and data.response.output[]
// envelopes recursively. Unchanged arrays and unrelated envelope fields keep
// their original raw JSON bytes without re-encoding.
func restoreResponsesEnvelopeMembers(members []rawMember, tools *ToolMap) ([]rawMember, bool, error) {
	changed := false
	if rawOut, ok := findRawMember(members, "output"); ok && rawOut.Kind() == '[' {
		rebuilt, outChanged, err := restoreResponsesOutputArray(rawOut, tools)
		if err != nil {
			return nil, false, err
		}
		if outChanged {
			members = setRawMember(members, "output", rebuilt)
			changed = true
		}
	}
	if rawResp, ok := findRawMember(members, "response"); ok && rawResp.Kind() == '{' {
		rMembers, err := parseOrderedObject(rawResp)
		if err != nil {
			return nil, false, err
		}
		if nestedOut, hasOut := findRawMember(rMembers, "output"); hasOut && nestedOut.Kind() == '[' {
			rebuilt, nChanged, err := restoreResponsesOutputArray(nestedOut, tools)
			if err != nil {
				return nil, false, err
			}
			if nChanged {
				rMembers = setRawMember(rMembers, "output", rebuilt)
				encResp, err := encodeOrderedObject(rMembers)
				if err != nil {
					return nil, false, err
				}
				members = setRawMember(members, "response", encResp)
				changed = true
			}
		}
	}
	if rawData, ok := findRawMember(members, "data"); ok && rawData.Kind() == '{' {
		dMembers, err := parseOrderedObject(rawData)
		if err != nil {
			return nil, false, err
		}
		dRestored, dChanged, err := restoreResponsesEnvelopeMembers(dMembers, tools)
		if err != nil {
			return nil, false, err
		}
		if dChanged {
			encData, err := encodeOrderedObject(dRestored)
			if err != nil {
				return nil, false, err
			}
			members = setRawMember(members, "data", encData)
			changed = true
		}
	}
	return members, changed, nil
}

// restoreResponsesOutputArray restores lowered function_call items inside
// rawOutput back into custom_tool_call, tool_search_call, or namespaced
// function_call items using tools. Unmapped function calls, hosted tool items,
// and message or reasoning items remain byte-for-byte untouched.
func restoreResponsesOutputArray(rawOutput jsontext.Value, tools *ToolMap) (jsontext.Value, bool, error) {
	items, err := parseRawArray(rawOutput)
	if err != nil {
		return nil, false, err
	}
	changed := false
	out := make([]jsontext.Value, len(items))
	for i, item := range items {
		if item.Kind() != '{' {
			out[i] = item
			continue
		}
		m, err := parseOrderedObject(item)
		if err != nil {
			return nil, false, err
		}
		typ, _ := readMemberString(m, "type")
		if strings.TrimSpace(typ) != "function_call" {
			out[i] = item
			continue
		}
		name, _ := readMemberString(m, "name")
		trimmedName := strings.TrimSpace(name)
		ns, _ := readMemberString(m, "namespace")
		itemID, _ := readMemberString(m, "id")

		origCustom, isCustom := tools.LookupCustomCall(ns, trimmedName)
		switch {
		case isCustom:
			args := extractRawArgumentsAsString(m)
			m = setStringMember(m, "type", "custom_tool_call")
			if retyped := retypeToolCallItemID(strings.TrimSpace(itemID), "custom_tool_call"); retyped != itemID && itemID != "" {
				m = setStringMember(m, "id", retyped)
			}
			m = setStringMember(m, "name", origCustom.Name)
			m = setStringMember(m, "input", unwrapCustomToolInput(args))
			m = deleteRawMember(m, "arguments")
			if origCustom.Namespace == "" {
				m = deleteRawMember(m, "namespace")
			} else {
				m = setStringMember(m, "namespace", origCustom.Namespace)
			}
			enc, err := encodeOrderedObject(m)
			if err != nil {
				return nil, false, err
			}
			out[i] = enc
			changed = true

		case tools.IsToolSearchProxyCall(ns, trimmedName):
			args := extractRawArgumentsAsString(m)
			m = setStringMember(m, "type", "tool_search_call")
			if retyped := retypeToolCallItemID(strings.TrimSpace(itemID), "tool_search_call"); retyped != itemID && itemID != "" {
				m = setStringMember(m, "id", retyped)
			}
			m = setStringMember(m, "execution", "client")
			m = setRawMember(m, "arguments", normalizeSearchArgumentsRaw(args))
			m = deleteRawMember(m, "name")
			m = deleteRawMember(m, "namespace")
			enc, err := encodeOrderedObject(m)
			if err != nil {
				return nil, false, err
			}
			out[i] = enc
			changed = true

		default:
			trimmedNS := strings.TrimSpace(ns)
			if trimmedNS == "" || trimmedNS == defaultFunctionsNamespace {
				if pair, found := tools.LookupNamespaceTool(trimmedName); found {
					updatedItem, itemChanged := applyRestoredNamespaceMembers(m, name, pair)
					if itemChanged {
						enc, err := encodeOrderedObject(updatedItem)
						if err != nil {
							return nil, false, err
						}
						out[i] = enc
						changed = true
						continue
					}
				}
			}
			out[i] = item
		}
	}
	if !changed {
		return rawOutput, false, nil
	}
	enc, err := encodeRawArray(out)
	if err != nil {
		return nil, false, err
	}
	return enc, true, nil
}

// isResponsesTerminalStreamType reports whether evType is a terminal Responses lifecycle event.
func isResponsesTerminalStreamType(evType string) bool {
	switch evType {
	case "response.completed", "response.done", "response.incomplete",
		"response.failed", "response.cancelled", "response.canceled":
		return true
	default:
		return false
	}
}

// isPermissiveJSONObject reports whether trimmed is a single syntactically
// complete JSON object under jsontext.AllowInvalidUTF8(true) and
// jsontext.AllowDuplicateNames(true). Non-object scalars, arrays, or raw SSE
// keepalives return false, while duplicate-key objects enter parseOrderedObject
// to surface a structured duplicate-key error.
func isPermissiveJSONObject(trimmed []byte) bool {
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true), jsontext.AllowDuplicateNames(true))
	if _, err := dec.ReadValue(); err != nil {
		return false
	}
	_, tailErr := dec.ReadToken()
	return errors.Is(tailErr, io.EOF)
}

// normalizeRawItemIDMember retypes ctc_ and tsc_ item IDs to fc_ prefixes on
// lowered function items. Unrecognized non-empty IDs that would violate
// Responses function item prefix rules are removed, while valid fc_ IDs remain unchanged.
func normalizeRawItemIDMember(members []rawMember) []rawMember {
	id, ok := readMemberString(members, "id")
	if !ok {
		return members
	}
	norm, changed := normalizeLoweredItemID(id)
	if !changed {
		return members
	}
	if norm == "" {
		return deleteRawMember(members, "id")
	}
	return setStringMember(members, "id", norm)
}

// normalizeRawToolOutputMember normalizes the output field on a lowered
// function_call_output item. String values and supported content-block arrays
// remain untouched, JSON null converts to "", and other JSON values compact into
// single-line JSON strings.
func normalizeRawToolOutputMember(members []rawMember) []rawMember {
	rawOut, ok := findRawMember(members, "output")
	if !ok || rawOut.Kind() == '"' {
		return members
	}
	if rawOut.Kind() == 'n' {
		return setStringMember(members, "output", "")
	}
	if rawOut.Kind() == '[' && isSupportedResponsesOutputPartArray(rawOut) {
		return members
	}
	return setStringMember(members, "output", compactRawJSONString(rawOut))
}

// isSupportedResponsesOutputPartArray reports whether rawArr is a non-empty
// JSON array containing only input_text, input_image, or input_file objects.
// Empty arrays, malformed JSON, or unsupported block types return false.
func isSupportedResponsesOutputPartArray(rawArr jsontext.Value) bool {
	elems, err := parseRawArray(rawArr)
	if err != nil || len(elems) == 0 {
		return false
	}
	for _, el := range elems {
		if el.Kind() != '{' {
			return false
		}
		m, err := parseOrderedObject(el)
		if err != nil {
			return false
		}
		typ, _ := readMemberString(m, "type")
		switch typ {
		case "input_text", "input_image", "input_file":
		default:
			return false
		}
	}
	return true
}

// extractRawArgumentsAsString extracts the arguments member from members as a
// JSON string for function_call compatibility. Missing or empty values fall
// back to "{}", while non-string JSON values compact into a single-line string.
func extractRawArgumentsAsString(members []rawMember) string {
	rawArgs, ok := findRawMember(members, "arguments")
	if !ok || len(rawArgs) == 0 {
		return "{}"
	}
	if rawArgs.Kind() == '"' {
		var s string
		if err := unmarshalPermissive(rawArgs, &s); err == nil {
			return s
		}
	}
	return compactRawJSONString(rawArgs)
}

// compactRawJSONString compacts insignificant JSON whitespace inside raw while
// preserving exact member order and arbitrary-precision numeric literals.
// Inputs that fail JSON compaction fall back to trimmed raw bytes without mutating raw.
func compactRawJSONString(raw jsontext.Value) string {
	cloned := jsontext.Value(bytes.Clone(bytes.TrimSpace(raw)))
	if err := cloned.Compact(); err == nil {
		return string(cloned)
	}
	return string(bytes.TrimSpace(raw))
}

// normalizeSearchArgumentsRaw converts arguments text into a valid JSON value
// for restored tool_search_call.arguments members. Blank strings fall back to
// "{}", single JSON values return verbatim, and non-JSON text marshals as a string.
func normalizeSearchArgumentsRaw(args string) jsontext.Value {
	trimmed := strings.TrimSpace(args)
	if trimmed == "" {
		return jsontext.Value("{}")
	}
	dec := jsontext.NewDecoder(strings.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	val, err := dec.ReadValue()
	if err == nil {
		// Clone val before calling ReadToken because jsontext.Decoder reuses its
		// internal token buffer on the subsequent EOF probe.
		cloned := bytes.Clone(val)
		if _, tailErr := dec.ReadToken(); errors.Is(tailErr, io.EOF) {
			return jsontext.Value(cloned)
		}
	}
	return mustMarshalString(args)
}

// isDeclaredNamespaceInMap reports whether nsName is a non-empty namespace
// group declared directly in tm or backed by non-self-aliased namespace tools.
// Nil maps, empty names, or bare discovered functions return false.
func isDeclaredNamespaceInMap(tm *ToolMap, nsName string) bool {
	if tm == nil || nsName == "" {
		return false
	}
	if tm.DeclaredNamespaces[nsName] {
		return true
	}
	return hasNamespaceInToolMap(tm, nsName)
}

// isSelfAliasedBareFunction reports whether (ident, up) is the synthetic
// {Namespace: fn, Name: fn} alias registered for a bare discovered function
// rather than a real namespace group. Declared namespaces return false.
func isSelfAliasedBareFunction(tm *ToolMap, ns, up string, ident NamespacedName) bool {
	return tm.FunctionTools[up] && ident.Namespace == ident.Name && !tm.DeclaredNamespaces[ns]
}

// hasNamespaceInToolMap reports whether tm has any registered scoped tool
// belonging to ns across CallNames, NamespaceTools, and ScopedCustom. Nil maps,
// empty namespace strings, and self-qualified bare discovered function aliases
// return false.
func hasNamespaceInToolMap(tm *ToolMap, ns string) bool {
	if tm == nil || ns == "" {
		return false
	}
	for ident, up := range tm.CallNames {
		if ident.Namespace == ns && !isSelfAliasedBareFunction(tm, ns, up, ident) {
			return true
		}
	}
	for up, ident := range tm.NamespaceTools {
		if ident.Namespace == ns && !isSelfAliasedBareFunction(tm, ns, up, ident) {
			return true
		}
	}
	for ident := range tm.ScopedCustom {
		if ident.Namespace == ns {
			return true
		}
	}
	return false
}

// collectNamespaceExecutableNames returns all flattened upstream function names
// belonging to declared namespace nsName in deterministic order. Declaration
// order in rebuiltTools takes precedence, with lexical sorting for remaining aliases.
func collectNamespaceExecutableNames(
	tm *ToolMap,
	rebuiltTools []jsontext.Value,
	nsName string,
) []string {
	if tm == nil || nsName == "" {
		return nil
	}
	candidates := make(map[string]bool)
	if nsName == defaultFunctionsNamespace {
		for up, ident := range tm.NamespaceTools {
			if ident.Namespace == "" && tm.FunctionTools[up] {
				candidates[up] = true
			}
		}
		if len(candidates) == 0 {
			for up := range tm.FunctionTools {
				candidates[up] = true
			}
		}
	} else {
		for ident, up := range tm.CallNames {
			if ident.Namespace == nsName && !isSelfAliasedBareFunction(tm, nsName, up, ident) {
				candidates[up] = true
			}
		}
		for up, ident := range tm.NamespaceTools {
			if ident.Namespace == nsName && !isSelfAliasedBareFunction(tm, nsName, up, ident) {
				candidates[up] = true
			}
		}
		for up, ident := range tm.CustomNames {
			if ident.Namespace == nsName {
				candidates[up] = true
			}
		}
	}
	if len(candidates) == 0 {
		return nil
	}

	out := make([]string, 0, len(candidates))
	seen := make(map[string]bool, len(candidates))
	for _, raw := range rebuiltTools {
		if raw.Kind() != '{' {
			continue
		}
		m, err := parseOrderedObject(raw)
		if err != nil {
			continue
		}
		typ, _ := readMemberString(m, "type")
		if strings.TrimSpace(typ) != "function" {
			continue
		}
		fnName, _ := readMemberString(m, "name")
		fnName = strings.TrimSpace(fnName)
		if candidates[fnName] && !seen[fnName] {
			out = append(out, fnName)
			seen[fnName] = true
		}
	}

	var rem []string
	for up := range candidates {
		if !seen[up] {
			rem = append(rem, up)
		}
	}
	slices.Sort(rem)
	return append(out, rem...)
}

// readMemberInt64 decodes a signed 64-bit integer JSON member named key from
// members. Missing keys, non-numeric JSON tokens, fractional literals, or
// values outside int64 range return (0, false).
func readMemberInt64(members []rawMember, key string) (int64, bool) {
	raw, ok := findRawMember(members, key)
	if !ok || len(raw) == 0 || raw.Kind() != '0' {
		return 0, false
	}
	v, err := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

// readMemberBoolPtr decodes an optional boolean JSON member named key from
// members into a newly allocated boolean pointer. Missing keys, null values, or
// non-boolean JSON tokens return nil.
func readMemberBoolPtr(members []rawMember, key string) *bool {
	raw, ok := findRawMember(members, key)
	if !ok || len(raw) == 0 {
		return nil
	}
	switch raw.Kind() {
	case 't':
		v := true
		return &v
	case 'f':
		v := false
		return &v
	default:
		return nil
	}
}

// marshalBoolRaw returns the raw JSON boolean literal bytes for v.
func marshalBoolRaw(v bool) jsontext.Value {
	if v {
		return jsontext.Value("true")
	}
	return jsontext.Value("false")
}

// cloneRawValueSlice returns an independent deep copy of every raw JSON element
// in in so callers cannot mutate cached declarations. Empty inputs return nil
// so ToolMap.LoweredTools omits empty arrays during JSON serialization.
func cloneRawValueSlice(in []jsontext.Value) []jsontext.Value {
	if len(in) == 0 {
		return nil
	}
	out := make([]jsontext.Value, len(in))
	for i, v := range in {
		out[i] = jsontext.Value(bytes.Clone(v))
	}
	return out
}
