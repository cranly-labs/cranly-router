package qoderchat

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
)

// stream_decode.go decodes wrapped Qoder SSE frames and buffered JSON
// envelopes into canonical sequential IR event streams and ir.Response values.
// Qoder SSE payloads wrap stringified JSON bodies inside outer status envelopes,
// mixing OpenAI choice deltas, Anthropic-shaped content_block_delta and
// tool_use_* envelopes, text-encoded XML/JSON/DSML tool calls, drifted parallel
// tool indices, and replaceable "{}" argument placeholders.
//
// Validating outer status envelopes and wirejson usage trees before mutating
// decoder state ensures failed frames return typed *qoder.APIError or syntax
// errors without leaving dangling open IR parts. Active tool call fragments
// coalesce inside toolCallAccumulator until visible text, reasoning, usage
// completion, or stream closure flushes completed sequential PartToolCall events
// satisfying all seven ir.ValidateStream invariants.

const (
	// maxSparseToolIndex caps pre-allocated tool call slots when an upstream
	// frame supplies an explicit non-sequential index, falling back to compact
	// slot binding above this threshold so adversarial indices cannot exhaust memory.
	maxSparseToolIndex = 64
)

// toolCallSlotState holds merged identity and JSON arguments for one accumulated tool call slot.
type toolCallSlotState struct {
	id        string // resolved or synthesized call identifier
	toolType  string // normalized tool kind ("function")
	name      string // normalized function name
	arguments string // merged JSON argument text
}

// toolCallAccumulator merges incremental tool call fragments across index drift,
// reused wire indices, unindexed parallel completions, and "{}" placeholders.
type toolCallAccumulator struct {
	calls               []toolCallSlotState // ordered accumulated call slots
	slotByUpstreamIndex map[int]int         // latest upstream index -> calls slot index
}

// StreamDecoder incrementally translates upstream Qoder SSE frames into
// sequential ir.Event slices satisfying ir.ValidateStream.
type StreamDecoder struct {
	textTransformer textToolCallTransformer // stateful XML/JSON/DSML text-call recognizer
	toolAcc         toolCallAccumulator     // stateful parallel tool call assembler
	started         bool                    // whether EventResponseStart has been emitted
	sawTermination  bool                    // whether [DONE], message_stop, or finish_reason arrived
	done            bool                    // whether a terminal IR event has been emitted
	nextPartIndex   int                     // next sequential zero-based IR part index
	openKind        ir.PartKind             // active open part kind ("", PartText, or PartReasoning)
	openText        strings.Builder         // accumulated text for the currently open part
	finishReason    ir.FinishReason         // mapped finish reason from choice finish_reason
	emittedToolCall bool                    // whether at least one PartToolCall was emitted
	mergedUsage     *ir.Usage               // latest monotonically merged usage snapshot
}

// NewStreamDecoder allocates an isolated stateful StreamDecoder for one
// upstream Qoder SSE stream attempt. Callers must construct a fresh instance per
// stream so buffered text-tool prefixes, tool slots, and usage state stay isolated.
func NewStreamDecoder() *StreamDecoder {
	return &StreamDecoder{}
}

// NewStreamDecoder allocates a fresh stateful StreamDecoder on Codec to satisfy
// convert's upstreamCodec contract. Each upstream Qoder SSE attempt must use its
// own decoder instance rather than sharing state across concurrent streams.
func (Codec) NewStreamDecoder() *StreamDecoder {
	return NewStreamDecoder()
}

// DecodeResponse parses either a multi-frame Qoder SSE byte buffer or a single
// wrapped/unwrapped Qoder JSON response body into a validated *ir.Response.
// Wrapped upstream error statuses, malformed JSON, or truncated tool arguments
// return a non-nil error.
func DecodeResponse(body []byte) (*ir.Response, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, errors.New("qoderchat: empty response body")
	}

	if looksLikeSSEStream(trimmed) {
		return decodeSSEBytesToResponse(trimmed)
	}

	innerBytes, err := unwrapBufferedJSONBody(trimmed)
	if err != nil {
		return nil, err
	}
	wrappedFrame, err := marshalDeterministic(wireSSEWrapper{Body: string(innerBytes)})
	if err != nil {
		return nil, err
	}

	dec := NewStreamDecoder()
	var acc ir.Accumulator
	events, err := dec.Decode(sse.Frame{Data: wrappedFrame})
	if err != nil {
		return nil, err
	}
	acc.Add(events...)
	doneEvents, err := dec.Decode(sse.Frame{Data: []byte("[DONE]")})
	if err != nil {
		return nil, err
	}
	acc.Add(doneEvents...)
	acc.Add(dec.Close(nil)...)

	if streamErr := acc.StreamError(); streamErr != nil {
		return nil, fmt.Errorf("qoderchat: %s", streamErr.Message)
	}
	resp := acc.Response()
	if err := ir.ValidateResponse(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// DecodeResponse delegates buffered Qoder JSON or SSE response parsing to the
// package-level DecodeResponse function on Codec. Wrapped HTTP error statuses or
// malformed response bodies return a non-nil error without partial output.
func (Codec) DecodeResponse(body []byte) (*ir.Response, error) {
	return DecodeResponse(body)
}

// MergeToolArguments combines incremental Qoder tool argument fragments while
// treating an empty JSON object "{}" on either side as a replaceable setup
// placeholder rather than concatenating "{}{...}". Blank deltas leave existing
// arguments unchanged, and non-placeholder fragments concatenate in arrival order.
func MergeToolArguments(existing, delta string) string {
	if delta == "" {
		return existing
	}
	if existing == "" {
		return delta
	}
	if strings.TrimSpace(delta) == "{}" {
		return existing
	}
	if strings.TrimSpace(existing) == "{}" {
		return delta
	}
	return existing + delta
}

// Decode parses one Qoder SSE frame into zero or more sequential IR events.
// Empty heartbeat frames or frames arriving after stream termination return
// (nil, nil).
//
// Outer status envelopes and wirejson usage trees validate before any open part
// or tool accumulator state mutates on d. Malformed JSON, duplicate usage keys,
// or wrapped HTTP >= 400 error payloads therefore return an error (including
// *qoder.APIError) while leaving d ready for a clean terminal Close(err) flush.
func (d *StreamDecoder) Decode(frame sse.Frame) ([]ir.Event, error) {
	if d.done {
		return nil, nil
	}
	trimmed := bytes.TrimSpace(frame.Data)
	// Empty SSE data lines represent Qoder keepalive heartbeats rather than malformed
	// envelopes, so consume them silently without interrupting active stream state.
	if len(trimmed) == 0 {
		return nil, nil
	}
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		d.sawTermination = true
		return d.finalizeNormal(), nil
	}

	var wrapper wireSSEWrapper
	if err := unmarshalPermissive(trimmed, &wrapper); err != nil {
		return nil, fmt.Errorf("qoderchat: decode SSE wrapper: %w", err)
	}
	if statusCode := parseWrapperStatus(wrapper); statusCode >= 400 {
		return nil, parseWrappedAPIError(wrapper, statusCode)
	}
	if wrapper.Body == "" {
		return nil, nil
	}
	if wrapper.Body == "[DONE]" {
		d.sawTermination = true
		return d.finalizeNormal(), nil
	}

	bodyBytes := []byte(wrapper.Body)
	var inner wireSSEInner
	if err := unmarshalPermissive(bodyBytes, &inner); err != nil {
		return nil, fmt.Errorf("qoderchat: decode SSE inner: %w", err)
	}
	if err := validateInnerUsage(bodyBytes, inner); err != nil {
		return nil, fmt.Errorf("qoderchat: invalid SSE usage JSON: %w", err)
	}

	rawObs, sawFinish, finishReason := extractInnerObservations(inner)
	if sawFinish {
		d.sawTermination = true
		d.finishReason = finishReason
	}

	var events []ir.Event
	for _, raw := range rawObs {
		normalized := d.textTransformer.append(raw)
		for _, obs := range normalized {
			if d.done {
				break
			}
			events = append(events, d.applyObservation(obs)...)
		}
		if d.done {
			break
		}
	}
	return events, nil
}

// Close flushes buffered text-tool suffixes, open text/reasoning parts, and
// accumulated tool calls at SSE stream EOF. Repeated calls after a terminal
// event return nil immediately.
//
// When cause is nil and an explicit [DONE], message_stop, or choice finish_reason
// arrived with syntactically valid tool arguments, Close emits EventResponseEnd.
// Silent truncation before any protocol termination marker or non-nil transport
// causes close open parts (marking unfinished tool calls Incomplete=true) before
// appending a terminal EventError.
func (d *StreamDecoder) Close(cause error) []ir.Event {
	if d.done {
		return nil
	}
	if cause == nil && d.sawTermination {
		return d.finalizeNormal()
	}

	var events []ir.Event
	for _, obs := range d.textTransformer.flush() {
		if obs.kind == obsDone {
			continue
		}
		events = append(events, d.applyObservation(obs)...)
	}
	events = append(events, d.ensureStarted()...)
	events = append(events, d.closeOpenPart()...)
	toolEvents, _ := d.flushToolCalls(true)
	events = append(events, toolEvents...)

	msg := "qoder stream ended before terminal marker"
	if cause != nil {
		msg = cause.Error()
		if strings.TrimSpace(msg) == "" {
			msg = "qoder stream read error"
		}
	}
	events = append(events, ir.Event{
		Kind: ir.EventError,
		Err: &ir.StreamError{
			Code:    "stream_error",
			Message: msg,
		},
	})
	d.done = true
	return events
}

// finalizeNormal flushes remaining buffered text and tool calls upon normal
// protocol termination. Assembled tool calls carrying blank names or invalid
// JSON arguments terminate with EventError instead of EventResponseEnd, and
// repeated calls after stream completion return nil.
func (d *StreamDecoder) finalizeNormal() []ir.Event {
	if d.done {
		return nil
	}
	var events []ir.Event
	for _, obs := range d.textTransformer.flush() {
		if obs.kind == obsDone {
			continue
		}
		events = append(events, d.applyObservation(obs)...)
	}
	events = append(events, d.ensureStarted()...)
	events = append(events, d.closeOpenPart()...)

	toolEvents, hasInvalidTool := d.flushToolCalls(false)
	events = append(events, toolEvents...)
	if hasInvalidTool {
		events = append(events, ir.Event{
			Kind: ir.EventError,
			Err: &ir.StreamError{
				Code:    "malformed_tool_arguments",
				Message: "malformed qoder tool arguments",
			},
		})
		d.done = true
		return events
	}

	reason := d.finishReason
	if reason == "" {
		reason = ir.FinishStop
	}
	if d.emittedToolCall {
		reason = ir.FinishToolCalls
	}
	events = append(events, ir.Event{
		Kind:   ir.EventResponseEnd,
		Finish: &ir.Finish{Reason: reason},
	})
	d.done = true
	return events
}

// applyObservation advances d's open part, tool accumulator, or usage state for
// one normalized observation and returns any immediately ready IR events.
// Pending tool calls flush ahead of visible text or reasoning deltas, and empty
// text or reasoning deltas return nil without opening a part.
func (d *StreamDecoder) applyObservation(obs rawObservation) []ir.Event {
	switch obs.kind {
	case obsText:
		if obs.text == "" {
			return nil
		}
		var events []ir.Event
		events = append(events, d.ensureStarted()...)
		if d.toolAcc.hasPending() {
			toolEvents, invalid := d.flushToolCalls(false)
			events = append(events, toolEvents...)
			if invalid {
				events = append(events, ir.Event{
					Kind: ir.EventError,
					Err: &ir.StreamError{
						Code:    "malformed_tool_arguments",
						Message: "malformed qoder tool arguments",
					},
				})
				d.done = true
				return events
			}
		}
		if d.openKind != ir.PartText {
			events = append(events, d.closeOpenPart()...)
			events = append(events, ir.Event{
				Kind:  ir.EventPartStart,
				Index: d.nextPartIndex,
				Part:  &ir.Part{Kind: ir.PartText},
			})
			d.openKind = ir.PartText
			d.openText.Reset()
		}
		_, _ = d.openText.WriteString(obs.text)
		events = append(events, ir.Event{
			Kind:  ir.EventPartDelta,
			Index: d.nextPartIndex,
			Delta: &ir.Delta{Text: obs.text},
		})
		return events

	case obsReasoning:
		if obs.text == "" {
			return nil
		}
		var events []ir.Event
		events = append(events, d.ensureStarted()...)
		if d.toolAcc.hasPending() {
			toolEvents, invalid := d.flushToolCalls(false)
			events = append(events, toolEvents...)
			if invalid {
				events = append(events, ir.Event{
					Kind: ir.EventError,
					Err: &ir.StreamError{
						Code:    "malformed_tool_arguments",
						Message: "malformed qoder tool arguments",
					},
				})
				d.done = true
				return events
			}
		}
		if d.openKind != ir.PartReasoning {
			events = append(events, d.closeOpenPart()...)
			events = append(events, ir.Event{
				Kind:  ir.EventPartStart,
				Index: d.nextPartIndex,
				Part:  &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{}},
			})
			d.openKind = ir.PartReasoning
			d.openText.Reset()
		}
		_, _ = d.openText.WriteString(obs.text)
		events = append(events, ir.Event{
			Kind:  ir.EventPartDelta,
			Index: d.nextPartIndex,
			Delta: &ir.Delta{Text: obs.text},
		})
		return events

	case obsToolCall:
		var events []ir.Event
		events = append(events, d.ensureStarted()...)
		events = append(events, d.closeOpenPart()...)
		d.toolAcc.appendDelta(obs)
		return events

	case obsUsage:
		var events []ir.Event
		events = append(events, d.ensureStarted()...)
		merged := mergeUsageSample(d.mergedUsage, obs.usage)
		d.mergedUsage = merged
		events = append(events, ir.Event{
			Kind:  ir.EventUsage,
			Usage: merged.Clone(),
		})
		return events

	case obsDone:
		d.sawTermination = true
		return d.finalizeNormal()

	default:
		return nil
	}
}

// ensureStarted emits the leading EventResponseStart event on its first call
// and marks d.started true. Subsequent invocations return nil so a stream never
// emits duplicate response_start events.
func (d *StreamDecoder) ensureStarted() []ir.Event {
	if d.started {
		return nil
	}
	d.started = true
	return []ir.Event{{
		Kind:     ir.EventResponseStart,
		Response: &ir.ResponseMeta{},
	}}
}

// closeOpenPart emits EventPartEnd for the currently open PartText or
// PartReasoning slot, resets d.openText, and increments d.nextPartIndex. When no
// text or reasoning part is open, it returns nil without advancing part indices.
func (d *StreamDecoder) closeOpenPart() []ir.Event {
	switch d.openKind {
	case ir.PartText:
		part := ir.Part{
			Kind: ir.PartText,
			Text: d.openText.String(),
		}
		idx := d.nextPartIndex
		d.openKind = ""
		d.openText.Reset()
		d.nextPartIndex++
		return []ir.Event{{
			Kind:  ir.EventPartEnd,
			Index: idx,
			Part:  &part,
		}}

	case ir.PartReasoning:
		var segs []string
		if txt := d.openText.String(); txt != "" {
			segs = []string{txt}
		}
		part := ir.Part{
			Kind:      ir.PartReasoning,
			Reasoning: &ir.Reasoning{Segments: segs},
		}
		idx := d.nextPartIndex
		d.openKind = ""
		d.openText.Reset()
		d.nextPartIndex++
		return []ir.Event{{
			Kind:  ir.EventPartEnd,
			Index: idx,
			Part:  &part,
		}}

	default:
		return nil
	}
}

// flushToolCalls drains accumulated slots from d.toolAcc into sequential
// PartToolCall start, delta, and end triples while resetting d.toolAcc for
// later turns. Empty accumulators return (nil, false) without advancing
// d.nextPartIndex.
//
// Normal flushes (forceIncomplete=false) validate each call's name and JSON
// arguments; unnamed slots or truncated JSON mark the emitted snapshot
// Incomplete=true and return hasInvalid=true so the caller closes the stream
// with EventError rather than violating ir.ValidateStream's prohibition on
// incomplete tool calls inside EventResponseEnd streams.
func (d *StreamDecoder) flushToolCalls(forceIncomplete bool) ([]ir.Event, bool) {
	slots := d.toolAcc.drain()
	if len(slots) == 0 {
		return nil, false
	}

	var events []ir.Event
	hasInvalid := false

	for _, slot := range slots {
		name := strings.TrimSpace(slot.name)
		args := slot.arguments
		if name != "" {
			name, args = NormalizeToolCallNameAndArguments(name, args)
		}

		incomplete := forceIncomplete
		if name == "" {
			name = "unknown"
			incomplete = true
			hasInvalid = true
		}
		if strings.TrimSpace(args) == "" && !incomplete {
			args = "{}"
		}
		if !incomplete {
			if err := validateSingleJSONValue([]byte(args)); err != nil {
				incomplete = true
				hasInvalid = true
			}
		} else if validateSingleJSONValue([]byte(args)) != nil || strings.TrimSpace(slot.name) == "" {
			incomplete = true
		}

		idx := d.nextPartIndex
		d.nextPartIndex++
		callID := strings.TrimSpace(slot.id)
		if callID == "" {
			callID = derivedToolCallID(idx, name, args)
		}

		ref := ir.ToolRef{Kind: ir.ToolFunction, Name: name}
		events = append(events, ir.Event{
			Kind:  ir.EventPartStart,
			Index: idx,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID: callID,
					Tool:   ref,
				},
			},
		})
		if args != "" {
			events = append(events, ir.Event{
				Kind:  ir.EventPartDelta,
				Index: idx,
				Delta: &ir.Delta{Arguments: args},
			})
		}
		events = append(events, ir.Event{
			Kind:  ir.EventPartEnd,
			Index: idx,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID:     callID,
					Tool:       ref,
					Arguments:  args,
					Incomplete: incomplete,
				},
			},
		})
		if !incomplete {
			d.emittedToolCall = true
		}
	}
	return events, hasInvalid
}

// derivedToolCallID generates a deterministic "call_<24hex>" identifier from
// partIndex, name, and args when an upstream Qoder tool call omits id,
// tool_call_id, and call_id. Identical stream inputs yield identical IDs.
func derivedToolCallID(partIndex int, name, args string) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "qoder_call:%d:%s:%s", partIndex, name, args))
	return "call_" + hex.EncodeToString(sum[:12])
}

// hasPending reports whether any non-empty tool call slot is buffered in a.
func (a *toolCallAccumulator) hasPending() bool {
	for _, c := range a.calls {
		if c.id != "" || c.name != "" || c.arguments != "" {
			return true
		}
	}
	return false
}

// drain extracts all non-empty tool call slots in arrival order and clears both
// a.calls and a.slotByUpstreamIndex. Empty accumulators return nil without
// allocating an output slice.
func (a *toolCallAccumulator) drain() []toolCallSlotState {
	if len(a.calls) == 0 {
		return nil
	}
	out := make([]toolCallSlotState, 0, len(a.calls))
	for _, c := range a.calls {
		if c.id == "" && c.name == "" && c.arguments == "" {
			continue
		}
		out = append(out, c)
	}
	a.calls = nil
	clear(a.slotByUpstreamIndex)
	return out
}

// appendDelta merges one normalized obsToolCall fragment into its resolved slot
// and updates upstream index bindings on a. Type-only function placeholders and
// unindexed argument-only fragments across multiple active calls are ignored so
// ambiguous fragments cannot corrupt parallel tool arguments.
func (a *toolCallAccumulator) appendDelta(obs rawObservation) {
	if strings.TrimSpace(obs.toolName) != "" {
		obs.toolName, obs.arguments = NormalizeToolCallNameAndArguments(obs.toolName, obs.arguments)
	}
	if obs.toolCallID == "" && obs.toolName == "" && strings.TrimSpace(obs.arguments) == "" && obs.toolType != "" {
		return
	}
	// Discard argument-only fragments lacking ID, index, and name when multiple
	// calls are already active so stray fragments cannot corrupt a parallel call.
	if obs.toolCallID == "" && !obs.hasToolCallIndex && obs.toolName == "" && obs.arguments != "" && len(a.calls) > 1 {
		return
	}

	slot := a.resolveSlot(obs)
	for len(a.calls) <= slot {
		a.calls = append(a.calls, toolCallSlotState{toolType: "function"})
	}
	state := &a.calls[slot]
	if obs.toolCallID != "" {
		state.id = obs.toolCallID
	}
	if obs.toolType != "" {
		state.toolType = obs.toolType
	} else if state.toolType == "" {
		state.toolType = "function"
	}
	if obs.toolName != "" {
		state.name = obs.toolName
	}
	if obs.arguments != "" {
		state.arguments = MergeToolArguments(state.arguments, obs.arguments)
	}
	a.bindIndex(obs, slot)
}

// resolveSlot selects the target slot index for obs before mutating state,
// matching explicit call IDs ahead of bound upstream indices.
//
// Compatible Qoder streams can drift upstream indices across fragments of the
// same call ID or reuse index zero across sequential named parallel calls.
// Searching existing IDs first prevents splitting an established call, while
// completed non-placeholder JSON arguments in a reused slot allocate a fresh
// slot and indices at or above maxSparseToolIndex bind compactly to prevent
// adversarial memory allocation.
func (a *toolCallAccumulator) resolveSlot(obs rawObservation) int {
	if obs.toolCallID != "" {
		for i := range a.calls {
			if a.calls[i].id == obs.toolCallID {
				return i
			}
		}
	}
	if obs.hasToolCallIndex && obs.toolCallIndex >= 0 {
		if bound, ok := a.slotByUpstreamIndex[obs.toolCallIndex]; ok && bound >= 0 && bound < len(a.calls) {
			if obs.toolCallID == "" || a.calls[bound].id == "" || a.calls[bound].id == obs.toolCallID {
				if a.shouldStartNewInSlot(obs, bound) {
					return len(a.calls)
				}
				return bound
			}
			return len(a.calls)
		}
	}
	if obs.hasToolCallIndex && obs.toolCallIndex >= 0 && obs.toolCallID == "" && obs.toolName != "" {
		if obs.toolCallIndex < len(a.calls) {
			if a.shouldStartNewInSlot(obs, obs.toolCallIndex) {
				return len(a.calls)
			}
			return obs.toolCallIndex
		}
		if obs.toolCallIndex < maxSparseToolIndex {
			return obs.toolCallIndex
		}
		return len(a.calls)
	}
	if a.shouldStartImplicitCall(obs) {
		return len(a.calls)
	}
	if len(a.calls) > 0 {
		last := &a.calls[len(a.calls)-1]
		if obs.toolCallID == "" || last.id == "" || last.id == obs.toolCallID {
			return len(a.calls) - 1
		}
	}
	if obs.hasToolCallIndex && obs.toolCallIndex >= 0 && obs.toolCallIndex < maxSparseToolIndex {
		return obs.toolCallIndex
	}
	return len(a.calls)
}

// shouldStartImplicitCall reports whether an unindexed, ID-less named fragment
// begins a separate tool call after a completed call in the final slot. Empty
// accumulators or fragments carrying an explicit ID or index return false.
func (a *toolCallAccumulator) shouldStartImplicitCall(obs rawObservation) bool {
	if len(a.calls) == 0 || obs.toolCallID != "" || obs.hasToolCallIndex {
		return false
	}
	return a.shouldStartNewInSlot(obs, len(a.calls)-1)
}

// shouldStartNewInSlot reports whether obs begins a distinct named tool call
// inside an occupied slot whose prior call already holds complete
// non-placeholder JSON arguments. Out-of-range slot indices, explicit call IDs,
// or unnamed argument fragments return false so ongoing deltas continue merging.
func (a *toolCallAccumulator) shouldStartNewInSlot(obs rawObservation, slot int) bool {
	if slot < 0 || slot >= len(a.calls) || obs.toolCallID != "" || obs.toolName == "" {
		return false
	}
	existing := a.calls[slot]
	if existing.id == "" && existing.name == "" && existing.arguments == "" {
		return false
	}
	trimmedArgs := strings.TrimSpace(existing.arguments)
	if trimmedArgs != "" && trimmedArgs != "{}" && validateSingleJSONValue([]byte(trimmedArgs)) == nil {
		return true
	}
	return existing.name != "" && existing.arguments == "" && obs.arguments == ""
}

// bindIndex records the latest mapping from obs.toolCallIndex to slot in
// a.slotByUpstreamIndex, lazily allocating the map when needed. Missing or
// negative upstream indices and out-of-range slot positions are ignored.
func (a *toolCallAccumulator) bindIndex(obs rawObservation, slot int) {
	if !obs.hasToolCallIndex || obs.toolCallIndex < 0 || slot < 0 || slot >= len(a.calls) {
		return
	}
	if a.slotByUpstreamIndex == nil {
		a.slotByUpstreamIndex = make(map[int]int)
	}
	a.slotByUpstreamIndex[obs.toolCallIndex] = slot
}

// validateInnerUsage runs wirejson usage-envelope and usage-object validation
// on bodyBytes whenever inner carries a non-null top-level usage field or a
// message_delta envelope with non-null data.usage. Frames lacking usage skip
// wirejson usage checks so non-usage data envelopes accept arbitrary vendor keys.
func validateInnerUsage(bodyBytes []byte, inner wireSSEInner) error {
	trimmedTop := bytes.TrimSpace(inner.Usage)
	if len(trimmedTop) > 0 && !bytes.Equal(trimmedTop, []byte("null")) {
		if err := wirejson.ValidateUsageEnvelope(bodyBytes); err != nil {
			return err
		}
		if err := wirejson.ValidateUsageObject(trimmedTop); err != nil {
			return err
		}
	}
	if firstNonEmptyString(inner.Event, inner.Type) == "message_delta" {
		trimmedData := bytes.TrimSpace(inner.Data)
		if len(trimmedData) > 0 && trimmedData[0] == '{' {
			var delta wireSSEMessageDelta
			if unmarshalPermissive(trimmedData, &delta) == nil {
				trimmedDeltaUsage := bytes.TrimSpace(delta.Usage)
				if len(trimmedDeltaUsage) > 0 && !bytes.Equal(trimmedDeltaUsage, []byte("null")) {
					if err := wirejson.ValidateUsageEnvelope(bodyBytes); err != nil {
						return err
					}
					if err := wirejson.ValidateUsageObject(trimmedDeltaUsage); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// extractInnerObservations converts envelope events, choice deltas or final
// messages, and usage objects on inner into ordered rawObservation entries. When
// a choice carries a non-empty finish_reason alongside a populated final message
// object, an explicit obsDone marker is appended at the tail.
func extractInnerObservations(inner wireSSEInner) ([]rawObservation, bool, ir.FinishReason) {
	out := extractEnvelopeObservations(inner)
	hasFinalMessage := false
	sawFinish := false
	var finishReason ir.FinishReason

	for _, choice := range inner.Choices {
		delta := choice.Delta
		if delta.ReasoningContent != "" {
			out = append(out, rawObservation{kind: obsReasoning, text: delta.ReasoningContent})
		}
		if delta.Content != "" {
			out = append(out, rawObservation{kind: obsText, text: delta.Content})
		}
		out = appendWireToolCalls(out, delta.ToolCalls)

		msg := choice.Message
		if msg.ReasoningContent != "" {
			out = append(out, rawObservation{kind: obsReasoning, text: msg.ReasoningContent})
		}
		if txt := extractMessageContentText(msg.Content); txt != "" {
			out = append(out, rawObservation{kind: obsText, text: txt})
		}
		out = appendWireToolCalls(out, msg.ToolCalls)

		if choice.FinishReason != "" {
			sawFinish = true
			finishReason = mapFinishReason(choice.FinishReason)
			if messageHasPayload(msg) {
				hasFinalMessage = true
			}
		}
	}

	if sample, ok := decodeUsageSample(inner.Usage); ok {
		out = append(out, rawObservation{kind: obsUsage, usage: sample})
	}
	if hasFinalMessage {
		out = append(out, rawObservation{kind: obsDone})
	}
	return out, sawFinish, finishReason
}

// extractEnvelopeObservations converts Anthropic-like event or type envelopes on
// inner into ordered rawObservation entries. Envelopes with unrecognized labels
// or nested data fields whose JSON types do not match expected wire structs
// return nil so subsequent frames on the stream continue decoding.
func extractEnvelopeObservations(inner wireSSEInner) []rawObservation {
	evType := firstNonEmptyString(inner.Event, inner.Type)
	if evType == "" {
		return nil
	}
	trimmedData := bytes.TrimSpace(inner.Data)

	switch evType {
	case "content_block_delta":
		if len(trimmedData) == 0 || trimmedData[0] != '{' {
			return nil
		}
		var delta wireSSEContentBlockDelta
		// Ignore content_block_delta envelopes whose nested data fields have
		// incompatible JSON types or empty text so later valid frames still decode.
		if err := unmarshalPermissive(trimmedData, &delta); err != nil || delta.Text == "" {
			return nil
		}
		kind := obsText
		if delta.Type == "thinking_delta" {
			kind = obsReasoning
		}
		return []rawObservation{{kind: kind, text: delta.Text}}

	case "tool_use_start":
		if len(trimmedData) == 0 || trimmedData[0] != '{' {
			return nil
		}
		var start wireSSEToolUseStart
		// Ignore tool_use_start envelopes whose nested data fields fail typed struct
		// unmarshaling rather than aborting subsequent choice or tool delta frames.
		if err := unmarshalPermissive(trimmedData, &start); err != nil {
			return nil
		}
		obs := rawObservation{
			kind:       obsToolCall,
			toolCallID: firstNonEmptyString(start.ID, start.ToolCallID, start.CallID),
			toolType:   normalizeWireToolType(start.Type),
			toolName:   start.Name,
		}
		if idx := firstNonNilInt(start.Index, inner.Index); idx != nil {
			obs.toolCallIndex = *idx
			obs.hasToolCallIndex = true
		}
		return []rawObservation{obs}

	case "tool_use_delta", "tool_call_delta":
		if len(trimmedData) == 0 || trimmedData[0] != '{' {
			return nil
		}
		var delta wireSSEToolUseDelta
		// Ignore tool_use_delta envelopes whose nested data fields fail typed struct
		// unmarshaling so mismatched metadata fields never corrupt active call slots.
		if err := unmarshalPermissive(trimmedData, &delta); err != nil {
			return nil
		}
		args := parseToolArgumentsRaw(delta.Arguments, delta.Input, delta.Parameters)
		obs := rawObservation{
			kind:       obsToolCall,
			toolCallID: firstNonEmptyString(delta.ID, delta.ToolCallID, delta.CallID),
			toolType:   normalizeWireToolType(delta.Type),
			toolName:   firstNonEmptyString(delta.Name, delta.ToolName),
			arguments:  args,
		}
		if obs.toolCallID == "" && obs.toolName == "" && strings.TrimSpace(obs.arguments) == "" && obs.toolType == "function" {
			return nil
		}
		if idx := firstNonNilInt(delta.Index, inner.Index); idx != nil {
			obs.toolCallIndex = *idx
			obs.hasToolCallIndex = true
		}
		return []rawObservation{obs}

	case "message_delta":
		if len(trimmedData) == 0 || trimmedData[0] != '{' {
			return nil
		}
		var delta wireSSEMessageDelta
		// Ignore message_delta envelopes whose nested data fields fail typed struct
		// unmarshaling when no valid usage sub-object can be read for accounting.
		if err := unmarshalPermissive(trimmedData, &delta); err != nil {
			return nil
		}
		sample, ok := decodeUsageSample(delta.Usage)
		if !ok {
			return nil
		}
		return []rawObservation{{kind: obsUsage, usage: sample}}

	case "message_stop":
		return []rawObservation{{kind: obsDone}}

	default:
		return nil
	}
}

// appendWireToolCalls appends obsToolCall entries onto out for each non-empty
// item in calls. Type-only function placeholders are skipped, and multi-call
// slices that omit explicit index fields receive compact zero-based positions.
func appendWireToolCalls(out []rawObservation, calls []wireSSEToolCall) []rawObservation {
	syntheticIdx := 0
	for _, tc := range calls {
		args := parseToolArgumentsRaw(tc.Function.Arguments, tc.Arguments, tc.Input, tc.Parameters)
		id := firstNonEmptyString(tc.ID, tc.ToolCallID, tc.CallID)
		name := firstNonEmptyString(tc.Function.Name, tc.Name, tc.ToolName)
		toolType := normalizeWireToolType(tc.Type)
		if id == "" && name == "" && strings.TrimSpace(args) == "" && (strings.TrimSpace(tc.Type) == "" || toolType == "function") {
			continue
		}
		obs := rawObservation{
			kind:       obsToolCall,
			toolCallID: id,
			toolType:   toolType,
			toolName:   name,
			arguments:  args,
		}
		if tc.Index != nil {
			obs.toolCallIndex = *tc.Index
			obs.hasToolCallIndex = true
		} else if len(calls) > 1 {
			obs.toolCallIndex = syntheticIdx
			obs.hasToolCallIndex = true
		}
		out = append(out, obs)
		syntheticIdx++
	}
	return out
}

// decodeUsageSample parses a non-null Qoder usage JSON value into a
// parsedUsageSample and reports whether an observation was present. Empty or
// null tokens return (parsedUsageSample{}, false), whereas non-null payloads keep
// prompt_tokens_details presence distinct from zero cached token counts.
func decodeUsageSample(raw jsontext.Value) (parsedUsageSample, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return parsedUsageSample{}, false
	}
	var u wireSSEUsage
	// Malformed non-null usage values emit a zero-count observation rather than
	// aborting stream content delivery, matching QoderSSEUsage.UnmarshalJSON.
	if err := unmarshalPermissive(trimmed, &u); err != nil {
		return parsedUsageSample{}, true
	}

	prompt := parseFlexibleInt64(u.PromptTokens)
	if prompt == 0 {
		prompt = parseFlexibleInt64(u.InputTokens)
	}
	completion := parseFlexibleInt64(u.CompletionTokens)
	if completion == 0 {
		completion = parseFlexibleInt64(u.OutputTokens)
	}

	sample := parsedUsageSample{
		promptTokens:     max(prompt, 0),
		completionTokens: max(completion, 0),
	}
	ptTrim := bytes.TrimSpace(u.PromptTokensDetails)
	if len(ptTrim) > 0 && !bytes.Equal(ptTrim, []byte("null")) {
		sample.hasPromptDetails = true
		var pt wirePromptTokensDetails
		// Keep hasPromptDetails=true with zero cachedTokens when prompt_tokens_details
		// is non-object JSON, matching Qoder's detail-presence cache-splitting contract.
		if unmarshalPermissive(ptTrim, &pt) == nil {
			sample.cachedTokens = max(parseFlexibleInt64(pt.CachedTokens), 0)
		}
	}
	ctTrim := bytes.TrimSpace(u.CompletionTokensDetails)
	if len(ctTrim) > 0 && !bytes.Equal(ctTrim, []byte("null")) {
		sample.hasReasoningTokens = true
		var ct wireCompletionTokensDetails
		// Leave reasoningTokens at zero when completion_tokens_details fails object
		// decoding so primary prompt and completion counts still survive.
		if unmarshalPermissive(ctTrim, &ct) == nil {
			sample.reasoningTokens = max(parseFlexibleInt64(ct.ReasoningTokens), 0)
		}
	}
	return sample, true
}

// mergeUsageSample converts sample into disjoint ir.Usage buckets and merges
// sparse trailing observations onto prev without erasing earlier input or
// cache-read counts.
//
// Qoder reports total prompt_tokens inclusive of cached_tokens alongside an
// informational cacheable_tokens field that does not represent cache creation.
// Subtracting cached_tokens from prompt_tokens (clamped at zero) keeps Input and
// CacheRead disjoint with CacheWrite=0, while preserving prior positive prompt
// and cache_read counts when a tail message_delta frame reports output tokens alone.
func mergeUsageSample(prev *ir.Usage, sample parsedUsageSample) *ir.Usage {
	cur := &ir.Usage{
		Output: sample.completionTokens,
	}
	if sample.hasPromptDetails {
		// Qoder prompt_tokens includes cached_tokens; clamp uncached Input at zero
		// while keeping CacheWrite zero because cacheable_tokens is not a write count.
		cur.Input = max(sample.promptTokens-sample.cachedTokens, 0)
		cur.CacheRead = sample.cachedTokens
	} else {
		cur.Input = sample.promptTokens
	}
	if sample.hasReasoningTokens {
		cur.Reasoning = sample.reasoningTokens
	} else if prev != nil && cur.Output > 0 && prev.Reasoning > 0 && prev.Reasoning <= cur.Output {
		cur.Reasoning = prev.Reasoning
	}

	if prev == nil {
		return cur
	}
	if sample.promptTokens == 0 && (!sample.hasPromptDetails || sample.cachedTokens == 0) && cur.HasObservedTokens() {
		// Preserve previously observed prompt and cache_read counts when a trailing
		// message_delta or usage chunk reports completion tokens alone.
		cur.Input = prev.Input
		cur.CacheRead = prev.CacheRead
	} else if prev.CacheRead > 0 && cur.CacheRead == 0 && !sample.hasPromptDetails && cur.HasObservedTokens() {
		// Retain prior cache_read split when a later summary frame omits prompt_tokens_details
		// while repeating total prompt_tokens >= prev.CacheRead.
		if cur.Input >= prev.CacheRead {
			cur.Input -= prev.CacheRead
		}
		cur.CacheRead = prev.CacheRead
	}
	if cur.Output == 0 && prev.Output > 0 && (cur.Input > 0 || cur.CacheRead > 0) {
		cur.Output = prev.Output
		if cur.Reasoning == 0 {
			cur.Reasoning = prev.Reasoning
		}
	}
	return cur
}

// mapFinishReason maps a Qoder choice finish_reason label onto its canonical
// ir.FinishReason constant. Blank, "stop", and "end_turn" strings resolve to
// FinishStop, while unrecognized non-empty vendor labels resolve to FinishOther.
func mapFinishReason(raw string) ir.FinishReason {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "stop", "end_turn":
		return ir.FinishStop
	case "length", "max_tokens":
		return ir.FinishLength
	case "tool_calls", "tool_use", "function_call":
		return ir.FinishToolCalls
	case "content_filter":
		return ir.FinishContentFilter
	default:
		return ir.FinishOther
	}
}

// looksLikeSSEStream reports whether trimmed begins with an SSE field prefix.
func looksLikeSSEStream(trimmed []byte) bool {
	return bytes.HasPrefix(trimmed, []byte("data:")) ||
		bytes.HasPrefix(trimmed, []byte("event:")) ||
		bytes.HasPrefix(trimmed, []byte(":")) ||
		bytes.HasPrefix(trimmed, []byte("id:")) ||
		bytes.HasPrefix(trimmed, []byte("retry:"))
}

// decodeSSEBytesToResponse scans raw SSE bytes through sse.Reader,
// StreamDecoder, and ir.Accumulator to assemble a validated *ir.Response.
// Scanner failures, wrapped API errors, or streams ending with EventError
// return a non-nil error rather than an incomplete response.
func decodeSSEBytesToResponse(raw []byte) (*ir.Response, error) {
	reader := sse.NewReader(bytes.NewReader(raw))
	dec := NewStreamDecoder()
	var acc ir.Accumulator

	for reader.Scan() {
		events, decErr := dec.Decode(reader.Frame())
		if decErr != nil {
			acc.Add(dec.Close(decErr)...)
			return nil, decErr
		}
		acc.Add(events...)
	}
	if err := reader.Err(); err != nil {
		acc.Add(dec.Close(err)...)
		return nil, err
	}
	acc.Add(dec.Close(nil)...)
	if streamErr := acc.StreamError(); streamErr != nil {
		return nil, fmt.Errorf("qoderchat: %s", streamErr.Message)
	}
	resp := acc.Response()
	if err := ir.ValidateResponse(resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// unwrapBufferedJSONBody resolves a buffered Qoder JSON response object into
// inner response JSON bytes. Non-object inputs, syntax errors, or duplicate
// outer keys return an error immediately.
//
// Outer {body, statusCode, statusCodeValue} wrappers check their effective HTTP
// status before inspecting body presence so 4xx/5xx envelopes lacking body still
// return a typed *qoder.APIError. Successful wrappers with omitted, empty, or
// null body normalize to "{}" for parity with StreamDecoder, while unwrapped
// direct choice objects return trimmed unchanged.
func unwrapBufferedJSONBody(trimmed []byte) ([]byte, error) {
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("qoderchat: response body is not a JSON object")
	}
	var wrapper wireJSONResponseWrapper
	if err := unmarshalPermissive(trimmed, &wrapper); err != nil {
		return nil, fmt.Errorf("qoderchat: decode response body: %w", err)
	}
	statusCode := 0
	if wrapper.StatusCodeValue != nil && *wrapper.StatusCodeValue > 0 {
		statusCode = *wrapper.StatusCodeValue
	} else {
		statusCode = parseStatusString(wrapper.StatusCode)
	}

	bodyTrim := bytes.TrimSpace(wrapper.Body)
	isWrapper := len(bodyTrim) > 0 || wrapper.StatusCodeValue != nil || strings.TrimSpace(wrapper.StatusCode) != ""
	if !isWrapper {
		return trimmed, nil
	}

	var innerStr string
	if len(bodyTrim) > 0 && !bytes.Equal(bodyTrim, []byte("null")) {
		if bodyTrim[0] == '"' {
			if err := unmarshalPermissive(bodyTrim, &innerStr); err != nil {
				return nil, err
			}
		} else {
			innerStr = string(bodyTrim)
		}
	}
	if statusCode >= 400 {
		return nil, parseWrappedAPIError(wireSSEWrapper{
			Body:            innerStr,
			StatusCode:      wrapper.StatusCode,
			StatusCodeValue: statusCode,
		}, statusCode)
	}
	trimmedInner := strings.TrimSpace(innerStr)
	if trimmedInner == "" || trimmedInner == "[DONE]" {
		return []byte("{}"), nil
	}
	return []byte(innerStr), nil
}
