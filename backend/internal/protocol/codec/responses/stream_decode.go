package responses

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream_decode.go incrementally decodes OpenAI Responses SSE frames into
// canonical sequential ir.Event streams. Upstream Responses servers and Codex
// edge proxies can emit bare Response objects with SSE event headers, legacy
// response.done terminal labels, one-shot output_item.done frames without
// preceding deltas, terminal envelopes with empty output arrays after streaming
// items, or standalone error frames followed by a recovering response.completed.
//
// StreamDecoder normalizes lifecycle envelopes through
// openai.ResponseLifecycleNormalizer while validating raw usage envelopes via
// wirejson before sjson repairs run. Output item slots serialize through
// ir.Sequencer so concurrent or out-of-order item updates still emit strictly
// sequential single-open-part events, empty output_text/refusal blocks defer
// EventPartStart until non-empty content or metadata arrives for Law S2 parity,
// and non-custom tool arguments failing single-JSON validation mark the call
// Incomplete and finish with EventError.

// wireStreamEvent decodes one incoming OpenAI Responses SSE JSON payload.
type wireStreamEvent struct {
	Type         string         `json:"type,omitzero"`          // SSE JSON event discriminator
	OutputIndex  *int           `json:"output_index,omitzero"`  // zero-based output item index
	ContentIndex *int           `json:"content_index,omitzero"` // zero-based message content index
	SummaryIndex *int           `json:"summary_index,omitzero"` // zero-based reasoning summary index
	ItemID       string         `json:"item_id,omitzero"`       // target output item identifier
	Delta        *string        `json:"delta,omitzero"`         // text or argument delta fragment
	Text         *string        `json:"text,omitzero"`          // completed text snapshot on *.done
	Refusal      *string        `json:"refusal,omitzero"`       // completed refusal snapshot on refusal.done
	Arguments    jsontext.Value `json:"arguments,omitzero"`     // completed arguments on *.done
	Input        *string        `json:"input,omitzero"`         // completed custom input on *.done
	Response     jsontext.Value `json:"response,omitzero"`      // nested Response object on lifecycle events
	Item         jsontext.Value `json:"item,omitzero"`          // output item object on output_item.*
	Part         jsontext.Value `json:"part,omitzero"`          // content/summary part object on *_part.*
	Error        jsontext.Value `json:"error,omitzero"`         // error object on standalone error events
}

// streamContentSlot tracks one nested content_index block inside a message output item.
type streamContentSlot struct {
	seqKey      int             // allocated slot key inside ir.Sequencer
	kind        ir.PartKind     // PartText or PartRefusal
	partID      string          // message or content part identifier
	annotations []ir.Annotation // latest decoded citations
	extra       ir.Extra        // Extra captured on content_part
	emittedText strings.Builder // concatenated Delta.Text already pushed to Sequencer
	started     bool            // whether EventPartStart has been pushed to Sequencer
	closed      bool            // whether EventPartEnd has been pushed to Sequencer
}

// streamOutputItemSlot tracks one output_index entry across its SSE lifecycle.
type streamOutputItemSlot struct {
	outputIndex  int                        // upstream zero-based output_index
	itemType     string                     // message, reasoning, function_call, custom_tool_call, tool_search_call, or opaque
	itemID       string                     // upstream item.id identifier string
	msgExtra     ir.Extra                   // message-level Extra to attach to first surviving content part
	usedMsgExtra bool                       // whether msgExtra was attached to a content part
	seqKey       int                        // Sequencer slot key for non-message items
	started      bool                       // whether EventPartStart has been pushed to Sequencer
	closed       bool                       // whether EventPartEnd has been pushed to Sequencer
	isSummary    bool                       // whether reasoning item observed summary_text events
	callID       string                     // call_id locked at EventPartStart
	namespace    string                     // tool namespace on call items
	toolName     string                     // tool name on call items
	itemExtra    ir.Extra                   // item-level Extra captured on added/done
	emittedBuf   strings.Builder            // streamed reasoning text or tool arguments
	contentSlots map[int]*streamContentSlot // message content slots keyed by content_index
	contentOrder []int                      // observed content_index keys in arrival order
}

// StreamDecoder decodes OpenAI Responses SSE frames into canonical sequential ir.Event values.
type StreamDecoder struct {
	seq                ir.Sequencer                       // enforces sequential single-open-part ordering
	lifecycle          openai.ResponseLifecycleNormalizer // normalizes bare responses and legacy response.done
	usageAcc           openai.UsageAccumulator            // accumulates sparse top-level and nested usage
	prevUsage          *ir.Usage                          // latest emitted cumulative Usage snapshot
	done               bool                               // true once terminal EventResponseEnd or EventError emitted
	nextSeqKey         int                                // monotonic slot key allocator for ir.Sequencer
	respID             string                             // latest non-empty response.id
	model              string                             // latest non-empty response.model
	createdAt          int64                              // latest non-zero response.created_at
	serviceTier        string                             // latest non-empty response.service_tier
	sawCallableTool    bool                               // true once any non-hosted tool_call emitted
	sawOutputItem      bool                               // true once any output item slot produced IR parts
	invalidToolArgsErr *ir.StreamError                    // non-nil when a completed call had invalid JSON args
	pendingBareError   *ir.StreamError                    // buffered standalone error frame awaiting recovery or EOF
	items              map[int]*streamOutputItemSlot      // tracked output items keyed by output_index
	itemOrder          []int                              // observed output_index keys in arrival order
}

// parsedResponseBlock holds a validated response message content block decoded
// from output_item.added, content_part.added, content_part.done, or output_item.done.
type parsedResponseBlock struct {
	hasSnapshot bool            // true when decoded from an explicit JSON block object
	kind        ir.PartKind     // PartText or PartRefusal
	text        string          // decoded output_text or refusal string
	annotations []ir.Annotation // validated citation annotations
	extra       ir.Extra        // collision-safe block Extra members
}

// supplementItem pairs raw JSON bytes with a pre-validated output item from a
// terminal response envelope when no item events arrived earlier on the stream.
type supplementItem struct {
	raw  []byte                 // trimmed JSON object bytes of the output item
	item wireResponseOutputItem // unmarshaled output item metadata
}

// NewStreamDecoder allocates a stateful StreamDecoder for one upstream OpenAI
// Responses SSE attempt. Each decoder owns mutable ir.Sequencer and usage
// accumulation state and must not be shared concurrently across streams.
func NewStreamDecoder() *StreamDecoder {
	return &StreamDecoder{
		items: make(map[int]*streamOutputItemSlot),
	}
}

// NewStreamDecoder returns a fresh StreamDecoder configured for OpenAI Responses
// SSE frames so Codec satisfies the streaming decoder factory contract. Callers
// must allocate a separate decoder per upstream stream attempt.
func (Codec) NewStreamDecoder() *StreamDecoder {
	return NewStreamDecoder()
}

// Decode processes one SSE frame and returns zero or more canonical sequential
// ir.Event values. Blank frames, [DONE] sentinels after a completed terminal
// event, and duplicate completed lifecycle frames return (nil, nil), whereas
// malformed JSON or ambiguous usage envelopes return an error.
//
// Standalone Codex error frames buffer in pendingBareError rather than
// terminating immediately so a subsequent recovering response.completed clears
// the transient error while response.failed or Close flushes it as EventError.
func (d *StreamDecoder) Decode(frame sse.Frame) ([]ir.Event, error) {
	if d.done {
		return nil, nil
	}
	trimmed := bytes.TrimSpace(frame.Data)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		if d.pendingBareError != nil {
			errToEmit := d.pendingBareError
			d.pendingBareError = nil
			return d.finishWithError(errToEmit), nil
		}
		return nil, nil
	}
	if trimmed[0] != '{' || !isValidSingleJSON(trimmed) {
		return nil, fmt.Errorf("decode responses stream frame: invalid JSON object")
	}

	repaired, effectiveType, duplicateCompleted := d.lifecycle.Normalize(trimmed, frame.Event)
	var ev wireStreamEvent
	if err := unmarshalJSONObject(repaired, &ev); err != nil {
		return nil, fmt.Errorf("decode responses stream event: %w", err)
	}
	evType := strings.TrimSpace(effectiveType)
	if evType == "" {
		evType = strings.TrimSpace(ev.Type)
	}
	if evType == "" {
		evType = strings.TrimSpace(frame.Event)
	}

	isTerminalLifecycle := evType == "response.completed" || evType == "response.incomplete"
	usageSnap, hasUsage, err := observeResponsesStreamUsage(&d.usageAcc, d.prevUsage, trimmed, repaired, isTerminalLifecycle)
	if err != nil {
		return nil, fmt.Errorf("decode responses stream usage: %w", err)
	}
	if duplicateCompleted {
		return nil, nil
	}

	switch evType {
	case "error":
		se := decodeResponsesError(ev.Error)
		if se == nil {
			se = decodeResponsesError(trimmed)
		}
		if se == nil {
			se = &ir.StreamError{Type: "server_error", Message: defaultResponsesErrorMessage}
		}
		// Buffer standalone Codex error frames until stream end or response.failed
		// so a subsequent recovering response.completed can clear the error cleanly.
		d.pendingBareError = se
		return nil, nil

	case "response.failed":
		var respEnv wireResponse
		var se *ir.StreamError
		// Malformed or absent response envelopes on response.failed frames fall back
		// to pendingBareError or defaultResponsesErrorMessage so EventError still fires.
		if unmarshalJSONObject(ev.Response, &respEnv) == nil {
			d.captureEnvelopeMeta(respEnv)
			se = decodeResponsesError(respEnv.Error)
		}
		if se == nil && d.pendingBareError != nil {
			se = d.pendingBareError
		}
		d.pendingBareError = nil
		if se == nil {
			se = &ir.StreamError{Type: "server_error", Code: "server_error", Message: defaultResponsesErrorMessage}
		}
		return d.finishWithError(se), nil

	case "response.cancelled", "response.canceled":
		d.pendingBareError = nil
		return d.finishWithError(&ir.StreamError{
			Type:    "stream_error",
			Code:    "response_cancelled",
			Message: "upstream response was cancelled",
		}), nil

	case "response.created", "response.in_progress":
		var respEnv wireResponse
		if err := unmarshalJSONObject(ev.Response, &respEnv); err != nil {
			return nil, fmt.Errorf("decode %s.response: %w", evType, err)
		}
		d.captureEnvelopeMeta(respEnv)
		var out []ir.Event
		out = append(out, d.ensureStarted()...)
		if hasUsage && !isTerminalLifecycle {
			d.prevUsage = usageSnap.Clone()
			out = append(out, d.seq.Push(ir.Event{Kind: ir.EventUsage, Usage: usageSnap})...)
		}
		return out, nil

	case "response.output_item.added":
		return d.handleOutputItemAdded(ev)

	case "response.content_part.added":
		return d.handleContentPartAdded(ev)

	case "response.output_text.delta":
		return d.handleContentDelta(ev, ir.PartText)

	case "response.refusal.delta":
		return d.handleContentDelta(ev, ir.PartRefusal)

	case "response.output_text.done":
		return d.handleContentTextDone(ev, ir.PartText)

	case "response.refusal.done":
		return d.handleContentTextDone(ev, ir.PartRefusal)

	case "response.content_part.done":
		return d.handleContentPartDone(ev)

	case "response.reasoning_summary_part.added":
		outIdx := derefIndex(ev.OutputIndex)
		slot := d.getOrCreateItemSlot(outIdx, "reasoning")
		if slot.itemType != "reasoning" {
			return nil, fmt.Errorf("reasoning_summary_part.added targets non-reasoning output_index %d (%s)", outIdx, slot.itemType)
		}
		slot.isSummary = true
		return d.ensureItemSlotStarted(slot), nil

	case "response.reasoning_summary_text.delta":
		return d.handleReasoningDelta(ev, true)

	case "response.reasoning_text.delta":
		return d.handleReasoningDelta(ev, false)

	case "response.reasoning_summary_text.done", "response.reasoning_summary_part.done", "response.reasoning_text.done":
		return d.handleReasoningSegmentDone(ev, strings.Contains(evType, "summary"))

	case "response.function_call_arguments.delta":
		return d.handleToolArgumentsDelta(ev, "function_call")

	case "response.custom_tool_call_input.delta":
		return d.handleToolArgumentsDelta(ev, "custom_tool_call")

	case "response.function_call_arguments.done", "response.custom_tool_call_input.done":
		return d.handleToolArgumentsDone(ev, evType)

	case "response.output_item.done":
		return d.handleOutputItemDone(ev)

	case "response.completed", "response.incomplete":
		return d.handleTerminalLifecycle(ev, evType, usageSnap, hasUsage)

	default:
		// Ignore benign progress notifications such as response.web_search_call.searching
		// while still forwarding any validated mid-stream usage observation.
		if hasUsage {
			var out []ir.Event
			out = append(out, d.ensureStarted()...)
			d.prevUsage = usageSnap.Clone()
			out = append(out, d.seq.Push(ir.Event{Kind: ir.EventUsage, Usage: usageSnap})...)
			return out, nil
		}
		return nil, nil
	}
}

// Close finalizes the stream when the underlying SSE reader reaches EOF,
// encounters a transport read error, or terminates via caller policy.
// Already-terminated streams return nil, whereas unterminated streams or
// buffered standalone error frames close open slots (marking open tool calls
// Incomplete) and emit a terminal EventError, preferring an explicit
// *ir.StreamError cause over any raw buffered bare error.
func (d *StreamDecoder) Close(err error) []ir.Event {
	if d.done {
		return nil
	}
	if explicit, ok := errors.AsType[*ir.StreamError](err); ok && explicit != nil {
		d.pendingBareError = nil
		return d.finishWithError(truncatedResponsesStreamError(explicit))
	}
	se := d.pendingBareError
	d.pendingBareError = nil
	if se == nil {
		se = truncatedResponsesStreamError(err)
	}
	return d.finishWithError(se)
}

// captureEnvelopeMeta records non-empty response metadata from lifecycle envelopes.
func (d *StreamDecoder) captureEnvelopeMeta(resp wireResponse) {
	if resp.ID != "" {
		d.respID = resp.ID
	}
	if resp.Model != "" {
		d.model = resp.Model
	}
	if resp.CreatedAt != 0 {
		d.createdAt = resp.CreatedAt
	}
	if resp.ServiceTier != "" {
		d.serviceTier = resp.ServiceTier
	}
}

// ensureStarted emits the unique leading EventResponseStart populated with
// captured response metadata when d.seq has not yet started. Subsequent calls
// return nil without emitting duplicate response-start events.
func (d *StreamDecoder) ensureStarted() []ir.Event {
	if d.seq.Started() {
		return nil
	}
	return d.seq.Push(ir.Event{
		Kind: ir.EventResponseStart,
		Response: &ir.ResponseMeta{
			ID:          d.respID,
			Model:       d.model,
			CreatedAt:   d.createdAt,
			ServiceTier: d.serviceTier,
		},
	})
}

// allocSeqKey returns the next monotonic slot key for ir.Sequencer.
func (d *StreamDecoder) allocSeqKey() int {
	k := d.nextSeqKey
	d.nextSeqKey++
	return k
}

// pushSlot stamps ev.Index with slotKey and routes ev through d.seq.Push.
func (d *StreamDecoder) pushSlot(slotKey int, ev ir.Event) []ir.Event {
	ev.Index = slotKey
	return d.seq.Push(ev)
}

// getOrCreateItemSlot looks up or allocates the streamOutputItemSlot at outIdx.
func (d *StreamDecoder) getOrCreateItemSlot(outIdx int, defaultType string) *streamOutputItemSlot {
	if slot, ok := d.items[outIdx]; ok {
		if slot.itemType == "" && defaultType != "" {
			slot.itemType = defaultType
		}
		return slot
	}
	slot := &streamOutputItemSlot{
		outputIndex:  outIdx,
		itemType:     defaultType,
		seqKey:       -1,
		contentSlots: make(map[int]*streamContentSlot),
	}
	if defaultType != "" && defaultType != "message" {
		slot.seqKey = d.allocSeqKey()
	}
	d.items[outIdx] = slot
	d.itemOrder = append(d.itemOrder, outIdx)
	return slot
}

// handleOutputItemAdded processes response.output_item.added for reasoning,
// tool calls, messages, and hosted opaque items. Item fields are validated
// before mutating slots or pushing to d.seq so error returns leave Sequencer state clean.
func (d *StreamDecoder) handleOutputItemAdded(ev wireStreamEvent) ([]ir.Event, error) {
	var item wireResponseOutputItem
	if err := unmarshalJSONObject(ev.Item, &item); err != nil {
		return nil, fmt.Errorf("decode output_item.added.item: %w", err)
	}
	item.Type = strings.TrimSpace(item.Type)
	if item.Type == "" {
		return nil, fmt.Errorf("decode output_item.added: missing item.type")
	}

	outIdx := derefIndex(ev.OutputIndex)
	var parsedBlocks []parsedResponseBlock
	var initToolArgs string
	switch item.Type {
	case "message":
		parsedBlocks = make([]parsedResponseBlock, len(item.Content))
		for cIdx, rawBlock := range item.Content {
			txt, kind, anns, extra, err := decodeResponseContentBlock(rawBlock, fmt.Sprintf("output_item.added.content[%d]", cIdx))
			if err != nil {
				return nil, err
			}
			parsedBlocks[cIdx] = parsedResponseBlock{
				hasSnapshot: true,
				kind:        kind,
				text:        txt,
				annotations: anns,
				extra:       extra,
			}
		}
	case "function_call":
		if strings.TrimSpace(item.Name) == "" {
			return nil, fmt.Errorf("decode output_item.added: function_call requires non-empty name")
		}
		var err error
		if initToolArgs, _, err = decodeResponseFunctionArgs(item.RawArguments, "output_item.added.arguments"); err != nil {
			return nil, err
		}
	case "custom_tool_call":
		if strings.TrimSpace(item.Name) == "" {
			return nil, fmt.Errorf("decode output_item.added: custom_tool_call requires non-empty name")
		}
	case "tool_search_call":
		var err error
		if initToolArgs, _, err = decodeResponseFunctionArgs(item.RawArguments, fmt.Sprintf("output[%d].arguments", outIdx)); err != nil {
			return nil, err
		}
	}

	if existing, ok := d.items[outIdx]; ok && (existing.started || existing.closed || (existing.itemType != "" && existing.itemType != item.Type)) {
		return nil, fmt.Errorf("decode output_item.added: conflicting item at output_index %d", outIdx)
	}
	slot := d.getOrCreateItemSlot(outIdx, item.Type)
	slot.itemType = item.Type
	if item.ID != "" {
		slot.itemID = item.ID
	}

	out := d.ensureStarted()

	switch item.Type {
	case "message":
		slot.msgExtra = extractResponseMessageExtra(item.Unknown)
		for cIdx, pb := range parsedBlocks {
			out = append(out, d.openOrSeedMessageBlock(slot, cIdx, pb)...)
		}
		return out, nil

	case "reasoning":
		if slot.seqKey < 0 {
			slot.seqKey = d.allocSeqKey()
		}
		slot.itemExtra = toExtra(item.Unknown)
		if len(item.Summary) > 0 {
			slot.isSummary = true
		}
		out = append(out, d.ensureItemSlotStarted(slot)...)
		return out, nil

	case "function_call":
		if slot.seqKey < 0 {
			slot.seqKey = d.allocSeqKey()
		}
		slot.callID = item.CallID
		slot.namespace = item.Namespace
		slot.toolName = item.Name
		slot.itemExtra = toExtra(item.Unknown)
		out = append(out, d.ensureItemSlotStarted(slot)...)
		if initToolArgs != "" {
			_, _ = slot.emittedBuf.WriteString(initToolArgs)
			out = append(out, d.pushSlot(slot.seqKey, ir.Event{
				Kind:  ir.EventPartDelta,
				Delta: &ir.Delta{Arguments: initToolArgs},
			})...)
		}
		return out, nil

	case "custom_tool_call":
		if slot.seqKey < 0 {
			slot.seqKey = d.allocSeqKey()
		}
		slot.callID = item.CallID
		slot.namespace = item.Namespace
		slot.toolName = item.Name
		slot.itemExtra = toExtra(item.Unknown)
		out = append(out, d.ensureItemSlotStarted(slot)...)
		if item.Input != "" {
			_, _ = slot.emittedBuf.WriteString(item.Input)
			out = append(out, d.pushSlot(slot.seqKey, ir.Event{
				Kind:  ir.EventPartDelta,
				Delta: &ir.Delta{Arguments: item.Input},
			})...)
		}
		return out, nil

	case "tool_search_call":
		if slot.seqKey < 0 {
			slot.seqKey = d.allocSeqKey()
		}
		slot.callID = item.CallID
		unk := cloneValueMap(item.Unknown)
		if item.Execution != "" && item.Execution != "client" {
			unk = insertStringField(unk, "execution", item.Execution)
		}
		slot.itemExtra = toExtra(unk)
		out = append(out, d.ensureItemSlotStarted(slot)...)
		if initToolArgs != "" {
			_, _ = slot.emittedBuf.WriteString(initToolArgs)
		}
		return out, nil

	default:
		// Reserve a slot key in arrival order for hosted/opaque items (such as
		// web_search_call) so subsequent message items wait until output_item.done.
		if slot.seqKey < 0 {
			slot.seqKey = d.allocSeqKey()
		}
		return out, nil
	}
}

// ensureItemSlotStarted starts the response stream if needed, assigns a
// Sequencer slot key, and pushes EventPartStart once for a non-message
// reasoning, tool_call, or opaque slot. Already-started or closed slots return nil.
func (d *StreamDecoder) ensureItemSlotStarted(slot *streamOutputItemSlot) []ir.Event {
	if slot.started || slot.closed {
		return nil
	}
	out := d.ensureStarted()
	if slot.seqKey < 0 {
		slot.seqKey = d.allocSeqKey()
	}
	var skeleton ir.Part
	switch slot.itemType {
	case "reasoning":
		skeleton = ir.Part{
			Kind:      ir.PartReasoning,
			ID:        slot.itemID,
			Reasoning: &ir.Reasoning{Summary: slot.isSummary},
		}
	case "custom_tool_call":
		name := slot.toolName
		if name == "" {
			name = "custom"
		}
		d.sawCallableTool = true
		skeleton = ir.Part{
			Kind: ir.PartToolCall,
			ID:   slot.itemID,
			Call: &ir.ToolCall{
				CallID: slot.callID,
				Tool: ir.ToolRef{
					Kind:      ir.ToolCustom,
					Namespace: slot.namespace,
					Name:      name,
				},
			},
		}
	case "tool_search_call":
		d.sawCallableTool = true
		skeleton = ir.Part{
			Kind: ir.PartToolCall,
			ID:   slot.itemID,
			Call: &ir.ToolCall{
				CallID: slot.callID,
				Tool:   ir.ToolRef{Kind: ir.ToolClientSearch},
			},
		}
	default:
		name := slot.toolName
		if name == "" {
			name = "function"
		}
		d.sawCallableTool = true
		skeleton = ir.Part{
			Kind: ir.PartToolCall,
			ID:   slot.itemID,
			Call: &ir.ToolCall{
				CallID: slot.callID,
				Tool: ir.ToolRef{
					Kind:      ir.ToolFunction,
					Namespace: slot.namespace,
					Name:      name,
				},
			},
		}
	}
	slot.started = true
	d.sawOutputItem = true
	out = append(out, d.pushSlot(slot.seqKey, ir.Event{
		Kind: ir.EventPartStart,
		Part: &skeleton,
	})...)
	return out
}

// handleContentPartAdded records a nested message content_part and starts its
// IR slot immediately only when initial text, annotations, or Extra are non-empty.
// Malformed blocks or non-message target slots return an error before mutating Sequencer state.
func (d *StreamDecoder) handleContentPartAdded(ev wireStreamEvent) ([]ir.Event, error) {
	txt, kind, anns, extra, err := decodeResponseContentBlock(ev.Part, "content_part.added.part")
	if err != nil {
		return nil, err
	}
	pb := parsedResponseBlock{
		hasSnapshot: true,
		kind:        kind,
		text:        txt,
		annotations: anns,
		extra:       extra,
	}
	outIdx := derefIndex(ev.OutputIndex)
	cIdx := derefIndex(ev.ContentIndex)
	slot := d.getOrCreateItemSlot(outIdx, "message")
	if slot.itemType != "message" {
		return nil, fmt.Errorf("content_part.added targets non-message output_index %d (%s)", outIdx, slot.itemType)
	}
	if ev.ItemID != "" && slot.itemID == "" {
		slot.itemID = ev.ItemID
	}
	out := d.ensureStarted()
	out = append(out, d.openOrSeedMessageBlock(slot, cIdx, pb)...)
	return out, nil
}

// openOrSeedMessageBlock initializes content_index cIdx inside slot, emitting
// EventPartStart immediately only when initial content or metadata is non-empty.
// Already-closed content slots ignore late duplicate additions and return nil.
func (d *StreamDecoder) openOrSeedMessageBlock(slot *streamOutputItemSlot, cIdx int, pb parsedResponseBlock) []ir.Event {
	wireType := "output_text"
	if pb.kind == ir.PartRefusal {
		wireType = "refusal"
	}
	cs := d.getOrCreateContentSlot(slot, cIdx, wireType)
	if cs.closed {
		return nil
	}
	if cs.partID == "" {
		cs.partID = slot.itemID
	}
	if len(pb.annotations) > 0 {
		cs.annotations = pb.annotations
	}
	partExtra := pb.extra
	if !slot.msgExtra.IsZero() && !slot.usedMsgExtra && partExtra.IsZero() {
		partExtra = slot.msgExtra
		slot.usedMsgExtra = true
	}
	if !partExtra.IsZero() {
		cs.extra = partExtra
	}

	initText := pb.text
	emitted := cs.emittedText.String()
	var out []ir.Event
	if initText != "" || len(cs.annotations) > 0 || !cs.extra.IsZero() {
		out = append(out, d.ensureContentSlotStarted(cs)...)
		if strings.HasPrefix(initText, emitted) && len(initText) > len(emitted) {
			suffix := initText[len(emitted):]
			_, _ = cs.emittedText.WriteString(suffix)
			out = append(out, d.pushSlot(cs.seqKey, ir.Event{
				Kind:  ir.EventPartDelta,
				Delta: &ir.Delta{Text: suffix},
			})...)
		}
	}
	return out
}

// getOrCreateContentSlot returns the streamContentSlot for cIdx inside slot.
func (d *StreamDecoder) getOrCreateContentSlot(slot *streamOutputItemSlot, cIdx int, wireType string) *streamContentSlot {
	if cs, ok := slot.contentSlots[cIdx]; ok {
		if wireType == "refusal" && !cs.started {
			cs.kind = ir.PartRefusal
		}
		return cs
	}
	kind := ir.PartText
	if wireType == "refusal" {
		kind = ir.PartRefusal
	}
	cs := &streamContentSlot{
		seqKey: -1,
		kind:   kind,
		partID: slot.itemID,
	}
	slot.contentSlots[cIdx] = cs
	slot.contentOrder = append(slot.contentOrder, cIdx)
	return cs
}

// ensureContentSlotStarted starts the response stream if needed, allocates a
// Sequencer slot key, and pushes EventPartStart once for a message content
// block. Already-started or closed content slots return nil without emitting events.
func (d *StreamDecoder) ensureContentSlotStarted(cs *streamContentSlot) []ir.Event {
	if cs.started || cs.closed {
		return nil
	}
	out := d.ensureStarted()
	if cs.seqKey < 0 {
		cs.seqKey = d.allocSeqKey()
	}
	cs.started = true
	d.sawOutputItem = true
	skeleton := ir.Part{
		Kind: cs.kind,
		ID:   cs.partID,
	}
	out = append(out, d.pushSlot(cs.seqKey, ir.Event{
		Kind: ir.EventPartStart,
		Part: &skeleton,
	})...)
	return out
}

// handleContentDelta appends a text or refusal delta fragment to its target
// message content_index slot and routes EventPartDelta through Sequencer.
// Empty deltas ensure stream startup without opening empty content blocks, and
// non-message output_index collisions return an error.
func (d *StreamDecoder) handleContentDelta(ev wireStreamEvent, kind ir.PartKind) ([]ir.Event, error) {
	if ev.Delta == nil || *ev.Delta == "" {
		return d.ensureStarted(), nil
	}
	outIdx := derefIndex(ev.OutputIndex)
	cIdx := derefIndex(ev.ContentIndex)
	slot := d.getOrCreateItemSlot(outIdx, "message")
	if slot.itemType != "message" {
		return nil, fmt.Errorf("content delta targets non-message output_index %d (%s)", outIdx, slot.itemType)
	}
	if ev.ItemID != "" && slot.itemID == "" {
		slot.itemID = ev.ItemID
	}
	wireType := "output_text"
	if kind == ir.PartRefusal {
		wireType = "refusal"
	}
	cs := d.getOrCreateContentSlot(slot, cIdx, wireType)
	if cs.closed {
		return nil, nil
	}
	out := d.ensureContentSlotStarted(cs)
	_, _ = cs.emittedText.WriteString(*ev.Delta)
	out = append(out, d.pushSlot(cs.seqKey, ir.Event{
		Kind:  ir.EventPartDelta,
		Delta: &ir.Delta{Text: *ev.Delta},
	})...)
	return out, nil
}

// handleContentTextDone reconciles any unstreamed suffix on output_text.done
// or refusal.done guarded by strings.HasPrefix without closing the content
// slot. Non-message output_index targets return an error, and closed slots return nil.
func (d *StreamDecoder) handleContentTextDone(ev wireStreamEvent, kind ir.PartKind) ([]ir.Event, error) {
	finalPtr := ev.Text
	wireType := "output_text"
	if kind == ir.PartRefusal {
		finalPtr = ev.Refusal
		wireType = "refusal"
	}
	if finalPtr == nil || *finalPtr == "" {
		return d.ensureStarted(), nil
	}
	outIdx := derefIndex(ev.OutputIndex)
	cIdx := derefIndex(ev.ContentIndex)
	slot := d.getOrCreateItemSlot(outIdx, "message")
	if slot.itemType != "message" {
		return nil, fmt.Errorf("content text done targets non-message output_index %d (%s)", outIdx, slot.itemType)
	}
	if ev.ItemID != "" && slot.itemID == "" {
		slot.itemID = ev.ItemID
	}
	cs := d.getOrCreateContentSlot(slot, cIdx, wireType)
	if cs.closed {
		return nil, nil
	}
	emitted := cs.emittedText.String()
	finalText := *finalPtr
	var out []ir.Event
	if strings.HasPrefix(finalText, emitted) && len(finalText) > len(emitted) {
		suffix := finalText[len(emitted):]
		out = append(out, d.ensureContentSlotStarted(cs)...)
		_, _ = cs.emittedText.WriteString(suffix)
		out = append(out, d.pushSlot(cs.seqKey, ir.Event{
			Kind:  ir.EventPartDelta,
			Delta: &ir.Delta{Text: suffix},
		})...)
	}
	return out, nil
}

// handleContentPartDone parses the final content_part.done snapshot and closes
// the targeted message content_index slot. Malformed part JSON or non-message
// output_index targets return an error before mutating Sequencer state.
func (d *StreamDecoder) handleContentPartDone(ev wireStreamEvent) ([]ir.Event, error) {
	var pb parsedResponseBlock
	wireType := "output_text"
	if partTrim := bytes.TrimSpace(ev.Part); len(partTrim) > 0 && !bytes.Equal(partTrim, []byte("null")) {
		txt, kind, anns, extra, err := decodeResponseContentBlock(partTrim, "content_part.done.part")
		if err != nil {
			return nil, err
		}
		pb = parsedResponseBlock{
			hasSnapshot: true,
			kind:        kind,
			text:        txt,
			annotations: anns,
			extra:       extra,
		}
		if kind == ir.PartRefusal {
			wireType = "refusal"
		}
	}

	outIdx := derefIndex(ev.OutputIndex)
	cIdx := derefIndex(ev.ContentIndex)
	slot := d.getOrCreateItemSlot(outIdx, "message")
	if slot.itemType != "message" {
		return nil, fmt.Errorf("content_part.done targets non-message output_index %d (%s)", outIdx, slot.itemType)
	}
	if ev.ItemID != "" && slot.itemID == "" {
		slot.itemID = ev.ItemID
	}
	cs := d.getOrCreateContentSlot(slot, cIdx, wireType)
	if cs.closed {
		return nil, nil
	}
	return d.closeContentSlotWithBlock(slot, cs, pb), nil
}

// closeContentSlotWithBlock reconciles any unstreamed suffix on pb and closes cs.
// Empty blocks that never started and carry neither citations nor Extra are omitted for Law S2 parity.
func (d *StreamDecoder) closeContentSlotWithBlock(slot *streamOutputItemSlot, cs *streamContentSlot, pb parsedResponseBlock) []ir.Event {
	if cs.closed {
		return nil
	}
	if cs.partID == "" {
		cs.partID = slot.itemID
	}
	if len(pb.annotations) > 0 {
		cs.annotations = pb.annotations
	}
	if !pb.extra.IsZero() {
		cs.extra = pb.extra
	} else if !slot.msgExtra.IsZero() && !slot.usedMsgExtra && cs.extra.IsZero() {
		cs.extra = slot.msgExtra
		slot.usedMsgExtra = true
	}

	finalText := pb.text
	emitted := cs.emittedText.String()
	var out []ir.Event
	if pb.hasSnapshot && strings.HasPrefix(finalText, emitted) && len(finalText) > len(emitted) {
		suffix := finalText[len(emitted):]
		out = append(out, d.ensureContentSlotStarted(cs)...)
		_, _ = cs.emittedText.WriteString(suffix)
		emitted = cs.emittedText.String()
		out = append(out, d.pushSlot(cs.seqKey, ir.Event{
			Kind:  ir.EventPartDelta,
			Delta: &ir.Delta{Text: suffix},
		})...)
	}
	if emitted != "" && !strings.HasPrefix(finalText, emitted) {
		// Preserve already-streamed delta text when a malformed content_part.done
		// snapshot contradicts earlier deltas so ValidateStream snapshot checks hold.
		finalText = emitted
	} else if finalText == "" {
		finalText = emitted
	}

	// Omit empty metadata-free blocks so buffered and streamed empty replies agree under Law S2.
	if !cs.started && finalText == "" && len(cs.annotations) == 0 && cs.extra.IsZero() {
		cs.closed = true
		return out
	}

	out = append(out, d.ensureContentSlotStarted(cs)...)
	endPart := ir.Part{
		Kind:  cs.kind,
		ID:    cs.partID,
		Text:  finalText,
		Extra: cs.extra,
	}
	if cs.kind == ir.PartText {
		endPart.Annotations = cs.annotations
	}
	cs.closed = true
	out = append(out, d.pushSlot(cs.seqKey, ir.Event{
		Kind: ir.EventPartEnd,
		Part: &endPart,
	})...)
	return out
}

// handleReasoningDelta opens or reuses the reasoning slot at ev.OutputIndex
// and routes non-empty summary or raw reasoning text deltas into Sequencer.
// Targeting an output_index bound to a non-reasoning item type returns an error.
func (d *StreamDecoder) handleReasoningDelta(ev wireStreamEvent, summary bool) ([]ir.Event, error) {
	outIdx := derefIndex(ev.OutputIndex)
	slot := d.getOrCreateItemSlot(outIdx, "reasoning")
	if slot.itemType != "reasoning" {
		return nil, fmt.Errorf("reasoning delta targets non-reasoning output_index %d (%s)", outIdx, slot.itemType)
	}
	if ev.ItemID != "" && slot.itemID == "" {
		slot.itemID = ev.ItemID
	}
	if summary {
		slot.isSummary = true
	}
	out := d.ensureItemSlotStarted(slot)
	if ev.Delta != nil && *ev.Delta != "" && !slot.closed {
		_, _ = slot.emittedBuf.WriteString(*ev.Delta)
		out = append(out, d.pushSlot(slot.seqKey, ir.Event{
			Kind:  ir.EventPartDelta,
			Delta: &ir.Delta{Text: *ev.Delta},
		})...)
	}
	return out, nil
}

// handleReasoningSegmentDone flushes unstreamed reasoning text on segment index
// 0 when an upstream skips reasoning deltas and sends *.done directly.
// Non-reasoning output_index collisions return an error without mutating Sequencer.
func (d *StreamDecoder) handleReasoningSegmentDone(ev wireStreamEvent, summary bool) ([]ir.Event, error) {
	outIdx := derefIndex(ev.OutputIndex)
	slot := d.getOrCreateItemSlot(outIdx, "reasoning")
	if slot.itemType != "reasoning" {
		return nil, fmt.Errorf("reasoning done targets non-reasoning output_index %d (%s)", outIdx, slot.itemType)
	}
	if ev.ItemID != "" && slot.itemID == "" {
		slot.itemID = ev.ItemID
	}
	if summary {
		slot.isSummary = true
	}
	out := d.ensureItemSlotStarted(slot)
	segIdx := derefIndex(ev.SummaryIndex)
	if !slot.closed && segIdx == 0 && ev.Text != nil && *ev.Text != "" {
		emitted := slot.emittedBuf.String()
		if emitted == "" {
			_, _ = slot.emittedBuf.WriteString(*ev.Text)
			out = append(out, d.pushSlot(slot.seqKey, ir.Event{
				Kind:  ir.EventPartDelta,
				Delta: &ir.Delta{Text: *ev.Text},
			})...)
		}
	}
	return out, nil
}

// handleToolArgumentsDelta opens or reuses the function_call or custom_tool_call
// slot at ev.OutputIndex and streams non-empty argument fragments into
// Sequencer. Incompatible item types on the targeted output_index return an error.
func (d *StreamDecoder) handleToolArgumentsDelta(ev wireStreamEvent, defaultItemType string) ([]ir.Event, error) {
	outIdx := derefIndex(ev.OutputIndex)
	slot := d.getOrCreateItemSlot(outIdx, defaultItemType)
	if slot.itemType != "function_call" && slot.itemType != "custom_tool_call" {
		return nil, fmt.Errorf("tool arguments delta targets incompatible output_index %d (%s)", outIdx, slot.itemType)
	}
	if ev.ItemID != "" && slot.itemID == "" {
		slot.itemID = ev.ItemID
	}
	out := d.ensureItemSlotStarted(slot)
	if ev.Delta != nil && *ev.Delta != "" && !slot.closed {
		_, _ = slot.emittedBuf.WriteString(*ev.Delta)
		out = append(out, d.pushSlot(slot.seqKey, ir.Event{
			Kind:  ir.EventPartDelta,
			Delta: &ir.Delta{Arguments: *ev.Delta},
		})...)
	}
	return out, nil
}

// handleToolArgumentsDone reconciles any unstreamed argument suffix on
// *_arguments.done or *_input.done guarded by strings.HasPrefix before item
// closure. Incompatible item types or malformed JSON string escapes return an error.
func (d *StreamDecoder) handleToolArgumentsDone(ev wireStreamEvent, evType string) ([]ir.Event, error) {
	defaultType := "function_call"
	if strings.Contains(evType, "custom_tool_call") {
		defaultType = "custom_tool_call"
	}
	outIdx := derefIndex(ev.OutputIndex)
	slot := d.getOrCreateItemSlot(outIdx, defaultType)
	if slot.itemType != "function_call" && slot.itemType != "custom_tool_call" {
		return nil, fmt.Errorf("tool arguments done targets incompatible output_index %d (%s)", outIdx, slot.itemType)
	}
	if slot.closed {
		return nil, nil
	}
	var finalStr string
	if defaultType == "custom_tool_call" && ev.Input != nil {
		finalStr = *ev.Input
	} else if len(bytes.TrimSpace(ev.Arguments)) > 0 {
		decoded, _, err := decodeResponseFunctionArgs(ev.Arguments, evType+".arguments")
		if err != nil {
			return nil, err
		}
		finalStr = decoded
	}
	out := d.ensureItemSlotStarted(slot)
	emitted := slot.emittedBuf.String()
	if strings.HasPrefix(finalStr, emitted) && len(finalStr) > len(emitted) {
		suffix := finalStr[len(emitted):]
		_, _ = slot.emittedBuf.WriteString(suffix)
		out = append(out, d.pushSlot(slot.seqKey, ir.Event{
			Kind:  ir.EventPartDelta,
			Delta: &ir.Delta{Arguments: suffix},
		})...)
	}
	return out, nil
}

// handleOutputItemDone parses response.output_item.done and delegates final
// slot completion or one-shot synthesis to completeItemFromSnapshot. Malformed
// item JSON or missing item.type discriminators return an error.
func (d *StreamDecoder) handleOutputItemDone(ev wireStreamEvent) ([]ir.Event, error) {
	rawItem := bytes.TrimSpace(ev.Item)
	var item wireResponseOutputItem
	if err := unmarshalJSONObject(rawItem, &item); err != nil {
		return nil, fmt.Errorf("decode output_item.done.item: %w", err)
	}
	item.Type = strings.TrimSpace(item.Type)
	if item.Type == "" {
		return nil, fmt.Errorf("decode output_item.done: missing item.type")
	}

	outIdx := derefIndex(ev.OutputIndex)
	return d.completeItemFromSnapshot(outIdx, item, rawItem)
}

// completeItemFromSnapshot closes or one-shot emits the slot at outIdx from item
// after validating item fields so failed decodes never mutate Sequencer state.
//
// When an upstream server sends output_item.done without preceding added or
// delta events, completeItemFromSnapshot synthesizes the missing opening and
// suffix delta steps before closing the slot. Malformed non-custom JSON tool
// arguments mark the closed call Incomplete and arm invalidToolArgsErr so the
// terminal lifecycle frame emits EventError instead of EventResponseEnd.
func (d *StreamDecoder) completeItemFromSnapshot(outIdx int, item wireResponseOutputItem, rawItem []byte) ([]ir.Event, error) {
	item.Type = strings.TrimSpace(item.Type)
	effType := item.Type
	if existing, ok := d.items[outIdx]; ok && existing.started {
		effType = existing.itemType
	}

	switch effType {
	case "message":
		parsedBlocks := make([]parsedResponseBlock, len(item.Content))
		for cIdx, rawBlock := range item.Content {
			txt, kind, anns, extra, err := decodeResponseContentBlock(rawBlock, fmt.Sprintf("output[%d].content[%d]", outIdx, cIdx))
			if err != nil {
				return nil, err
			}
			parsedBlocks[cIdx] = parsedResponseBlock{
				hasSnapshot: true,
				kind:        kind,
				text:        txt,
				annotations: anns,
				extra:       extra,
			}
		}
		slot := d.getOrCreateItemSlot(outIdx, item.Type)
		if !slot.started {
			slot.itemType = item.Type
		}
		if item.ID != "" {
			slot.itemID = item.ID
		}
		out := d.ensureStarted()
		if slot.msgExtra.IsZero() {
			slot.msgExtra = extractResponseMessageExtra(item.Unknown)
		}
		for cIdx, pb := range parsedBlocks {
			wireType := "output_text"
			if pb.kind == ir.PartRefusal {
				wireType = "refusal"
			}
			cs := d.getOrCreateContentSlot(slot, cIdx, wireType)
			out = append(out, d.closeContentSlotWithBlock(slot, cs, pb)...)
		}
		for _, cIdx := range slot.contentOrder {
			cs := slot.contentSlots[cIdx]
			if !cs.closed {
				out = append(out, d.closeContentSlotWithBlock(slot, cs, parsedResponseBlock{})...)
			}
		}
		slot.closed = true
		return out, nil

	case "reasoning":
		if existing, ok := d.items[outIdx]; ok && existing.closed {
			return d.ensureStarted(), nil
		}
		endPart, err := decodeResponseReasoningOutput(item, fmt.Sprintf("output[%d]", outIdx))
		if err != nil {
			return nil, err
		}
		slot := d.getOrCreateItemSlot(outIdx, item.Type)
		if !slot.started {
			slot.itemType = item.Type
		}
		if item.ID != "" {
			slot.itemID = item.ID
		}
		if endPart.ID == "" {
			endPart.ID = slot.itemID
		}
		if endPart.Extra.IsZero() && !slot.itemExtra.IsZero() {
			endPart.Extra = slot.itemExtra
		}
		if len(endPart.Reasoning.Segments) > 0 {
			slot.isSummary = endPart.Reasoning.Summary
		}
		out := d.ensureStarted()
		out = append(out, d.ensureItemSlotStarted(slot)...)

		finalJoined := strings.Join(endPart.Reasoning.Segments, "")
		emitted := slot.emittedBuf.String()
		if strings.HasPrefix(finalJoined, emitted) && len(finalJoined) > len(emitted) {
			suffix := finalJoined[len(emitted):]
			_, _ = slot.emittedBuf.WriteString(suffix)
			emitted = slot.emittedBuf.String()
			out = append(out, d.pushSlot(slot.seqKey, ir.Event{
				Kind:  ir.EventPartDelta,
				Delta: &ir.Delta{Text: suffix},
			})...)
		}
		if emitted != "" && !strings.HasPrefix(finalJoined, emitted) {
			// Preserve already-streamed delta text when a malformed done snapshot
			// contradicts earlier deltas so ValidateStream snapshot checks hold.
			endPart.Reasoning.Segments = []string{emitted}
		}
		slot.closed = true
		out = append(out, d.pushSlot(slot.seqKey, ir.Event{
			Kind: ir.EventPartEnd,
			Part: &endPart,
		})...)
		return out, nil

	case "function_call", "custom_tool_call", "tool_search_call":
		if existing, ok := d.items[outIdx]; ok && existing.closed {
			return d.ensureStarted(), nil
		}
		return d.completeCallSlot(outIdx, effType, item)

	default:
		slot := d.getOrCreateItemSlot(outIdx, item.Type)
		if !slot.started {
			slot.itemType = item.Type
		}
		if item.ID != "" {
			slot.itemID = item.ID
		}
		out := d.ensureStarted()
		if slot.closed {
			return out, nil
		}
		if slot.seqKey < 0 {
			slot.seqKey = d.allocSeqKey()
		}
		opaquePart := ir.Part{
			Kind: ir.PartOpaque,
			ID:   slot.itemID,
			Opaque: &ir.OpaqueItem{
				Origin: protocol.ProtocolOpenAIResponses,
				Type:   item.Type,
				Raw:    compactRawJSON(rawItem),
			},
		}
		slot.started = true
		slot.closed = true
		d.sawOutputItem = true
		out = append(out, d.pushSlot(slot.seqKey, ir.Event{
			Kind: ir.EventPartStart,
			Part: &opaquePart,
		})...)
		out = append(out, d.pushSlot(slot.seqKey, ir.Event{
			Kind: ir.EventPartEnd,
			Part: &opaquePart,
		})...)
		return out, nil
	}
}

// completeCallSlot validates and closes a function_call, custom_tool_call, or
// tool_search_call slot upon receiving response.output_item.done. Unsent
// function argument suffixes flush as a trailing EventPartDelta, empty final
// arguments fall back to previously streamed fragments, and non-JSON
// function_call arguments or non-object tool_search_call arguments mark
// Call.Incomplete and record d.invalidToolArgsErr for terminal EventError
// reporting instead of returning an immediate decode error.
func (d *StreamDecoder) completeCallSlot(
	outIdx int,
	effType string,
	item wireResponseOutputItem,
) ([]ir.Event, error) {
	path := fmt.Sprintf("output[%d]", outIdx)
	existing := d.items[outIdx]
	var endPart ir.Part
	var err error

	switch effType {
	case "custom_tool_call":
		if item.Name == "" && existing != nil {
			item.Name = existing.toolName
		}
		endPart, err = decodeResponseCustomCallOutput(item, path)
		if err != nil {
			return nil, err
		}
	case "tool_search_call":
		endPart, err = decodeResponseToolSearchCallOutput(item, false, path)
		if err != nil {
			return nil, err
		}
	default:
		if item.Name == "" && existing != nil {
			item.Name = existing.toolName
		}
		endPart, err = decodeResponseFunctionCallOutput(item, false, path)
		if err != nil {
			return nil, err
		}
	}

	slot := d.getOrCreateItemSlot(outIdx, effType)
	if !slot.started {
		slot.itemType = effType
	}
	if item.ID != "" {
		slot.itemID = item.ID
	}
	if endPart.ID == "" {
		endPart.ID = slot.itemID
	}
	if slot.started && slot.callID != "" {
		endPart.Call.CallID = slot.callID
	} else if slot.callID == "" {
		slot.callID = endPart.Call.CallID
	}
	if slot.toolName == "" {
		slot.toolName = endPart.Call.Tool.Name
	}
	if slot.namespace == "" {
		slot.namespace = endPart.Call.Tool.Namespace
	}
	if endPart.Extra.IsZero() && !slot.itemExtra.IsZero() {
		endPart.Extra = slot.itemExtra
	}

	out := d.ensureStarted()
	out = append(out, d.ensureItemSlotStarted(slot)...)

	emitted := slot.emittedBuf.String()
	finalArgs := endPart.Call.Arguments
	if strings.HasPrefix(finalArgs, emitted) && len(finalArgs) > len(emitted) && slot.itemType != "tool_search_call" {
		suffix := finalArgs[len(emitted):]
		_, _ = slot.emittedBuf.WriteString(suffix)
		emitted = slot.emittedBuf.String()
		out = append(out, d.pushSlot(slot.seqKey, ir.Event{
			Kind:  ir.EventPartDelta,
			Delta: &ir.Delta{Arguments: suffix},
		})...)
	}
	if emitted != "" && finalArgs == "" {
		finalArgs = emitted
		endPart.Call.Arguments = finalArgs
	}

	// Defer invalid JSON argument failures to d.invalidToolArgsErr while still
	// closing the open slot as Incomplete so Sequencer satisfies ValidateStream
	// part-closure invariants before emitting the terminal EventError.
	invalidArgs := false
	switch slot.itemType {
	case "tool_search_call":
		invalidArgs = strings.TrimSpace(finalArgs) != "" && !isValidSingleJSONObject([]byte(finalArgs))
	case "function_call":
		invalidArgs = strings.TrimSpace(finalArgs) != "" && !isValidSingleJSON([]byte(finalArgs))
	}
	if invalidArgs {
		endPart.Call.Incomplete = true
		d.invalidToolArgsErr = &ir.StreamError{
			Type:    "invalid_response_error",
			Code:    "invalid_tool_arguments",
			Message: invalidToolArgsMessage(slot.itemType),
		}
	} else {
		endPart.Call.Incomplete = false
	}

	slot.closed = true
	out = append(out, d.pushSlot(slot.seqKey, ir.Event{
		Kind: ir.EventPartEnd,
		Part: &endPart,
	})...)
	return out, nil
}

// handleTerminalLifecycle processes response.completed and response.incomplete
// terminal envelopes into final usage and completion or error events.
//
// Any buffered standalone Codex error frame is cleared upon reaching a valid
// completion envelope, and terminal response.output arrays supplement missing
// items only when no output item events arrived earlier on the stream. When an
// earlier function or tool_search call failed single-JSON argument validation,
// handleTerminalLifecycle finishes the stream with EventError rather than EventResponseEnd.
func (d *StreamDecoder) handleTerminalLifecycle(
	ev wireStreamEvent,
	evType string,
	usageSnap *ir.Usage,
	hasUsage bool,
) ([]ir.Event, error) {
	var resp wireResponse
	if err := unmarshalJSONObject(ev.Response, &resp); err != nil {
		return nil, fmt.Errorf("decode %s.response: %w", evType, err)
	}
	var parsedSupplements []supplementItem
	if !d.sawOutputItem && len(resp.Output) > 0 {
		parsedSupplements = make([]supplementItem, len(resp.Output))
		for idx, rawElem := range resp.Output {
			elemTrim := bytes.TrimSpace(rawElem)
			var item wireResponseOutputItem
			if err := unmarshalJSONObject(elemTrim, &item); err != nil {
				return nil, fmt.Errorf("decode %s.response.output[%d]: %w", evType, idx, err)
			}
			item.Type = strings.TrimSpace(item.Type)
			if item.Type == "" {
				return nil, fmt.Errorf("decode %s.response.output[%d]: missing item.type", evType, idx)
			}
			if _, _, err := decodeResponseOutputItem(elemTrim, fmt.Sprintf("output[%d]", idx)); err != nil &&
				item.Type != "function_call" && item.Type != "tool_search_call" {
				return nil, err
			}
			if (item.Type == "function_call" || item.Type == "custom_tool_call") && strings.TrimSpace(item.Name) == "" {
				return nil, fmt.Errorf("decode %s.response.output[%d]: %s requires non-empty name", evType, idx, item.Type)
			}
			if item.Type == "function_call" || item.Type == "tool_search_call" {
				if _, _, err := decodeResponseFunctionArgs(item.RawArguments, fmt.Sprintf("output[%d].arguments", idx)); err != nil {
					return nil, err
				}
			}
			parsedSupplements[idx] = supplementItem{raw: elemTrim, item: item}
		}
	}

	d.captureEnvelopeMeta(resp)
	d.pendingBareError = nil

	out := d.ensureStarted()

	for idx, sup := range parsedSupplements {
		events, err := d.completeItemFromSnapshot(idx, sup.item, sup.raw)
		if err != nil {
			return nil, err
		}
		out = append(out, events...)
	}

	// Close any open slots cleanly before emitting terminal events.
	out = append(out, d.closeDanglingSlots(false)...)

	if hasUsage {
		d.prevUsage = usageSnap.Clone()
		out = append(out, d.seq.Push(ir.Event{Kind: ir.EventUsage, Usage: usageSnap})...)
	}

	d.done = true
	if d.invalidToolArgsErr != nil {
		out = append(out, d.seq.Push(ir.Event{
			Kind: ir.EventError,
			Err:  d.invalidToolArgsErr,
		})...)
		return out, nil
	}

	status := resp.Status
	if status == "" {
		if evType == "response.incomplete" {
			status = "incomplete"
		} else {
			status = "completed"
		}
	}
	finish := decodeResponsesFinish(status, resp.IncompleteDetails, d.sawCallableTool)
	out = append(out, d.seq.Push(ir.Event{
		Kind:        ir.EventResponseEnd,
		Finish:      &finish,
		ServiceTier: d.serviceTier,
	})...)
	return out, nil
}

// finishWithError closes all open item and content slots, marks open or invalid
// tool calls Incomplete, ensures EventResponseStart, and emits terminal EventError.
// Nil or blank errors substitute defaultResponsesErrorMessage before pushing EventError.
func (d *StreamDecoder) finishWithError(se *ir.StreamError) []ir.Event {
	out := d.ensureStarted()
	out = append(out, d.closeDanglingSlots(true)...)
	d.done = true
	if se == nil || strings.TrimSpace(se.Message) == "" {
		se = &ir.StreamError{
			Type:    "server_error",
			Message: defaultResponsesErrorMessage,
		}
	}
	out = append(out, d.seq.Push(ir.Event{
		Kind: ir.EventError,
		Err:  se,
	})...)
	return out
}

// closeDanglingSlots closes any open content or item slots in arrival order so
// ir.Sequencer flushes every started slot before a terminal event arrives.
// Abnormal terminations mark unfinished tool calls Incomplete, whereas normal
// completions require single-value JSON function_call arguments and JSON object
// tool_search_call arguments, recording d.invalidToolArgsErr on violations.
func (d *StreamDecoder) closeDanglingSlots(abnormal bool) []ir.Event {
	var out []ir.Event
	for _, outIdx := range d.itemOrder {
		slot := d.items[outIdx]
		if slot.closed {
			continue
		}
		if slot.itemType == "message" {
			for _, cIdx := range slot.contentOrder {
				cs := slot.contentSlots[cIdx]
				if cs.closed || !cs.started {
					cs.closed = true
					continue
				}
				endPart := ir.Part{
					Kind:  cs.kind,
					ID:    cs.partID,
					Text:  cs.emittedText.String(),
					Extra: cs.extra,
				}
				if cs.kind == ir.PartText {
					endPart.Annotations = cs.annotations
				}
				cs.closed = true
				out = append(out, d.pushSlot(cs.seqKey, ir.Event{
					Kind: ir.EventPartEnd,
					Part: &endPart,
				})...)
			}
			slot.closed = true
			continue
		}

		if !slot.started {
			slot.closed = true
			continue
		}

		switch slot.itemType {
		case "reasoning":
			var segments []string
			if txt := slot.emittedBuf.String(); txt != "" {
				segments = []string{txt}
			}
			endPart := ir.Part{
				Kind: slot.PartKind(),
				ID:   slot.itemID,
				Reasoning: &ir.Reasoning{
					Segments: segments,
					Summary:  slot.isSummary,
				},
				Extra: slot.itemExtra,
			}
			slot.closed = true
			out = append(out, d.pushSlot(slot.seqKey, ir.Event{
				Kind: ir.EventPartEnd,
				Part: &endPart,
			})...)

		case "function_call", "custom_tool_call", "tool_search_call":
			args := slot.emittedBuf.String()
			incomplete := abnormal
			// Record malformed argument payloads on d.invalidToolArgsErr while closing
			// the dangling slot as Incomplete so Sequencer closes all open parts before
			// emitting the terminal EventError.
			invalidArgs := false
			switch slot.itemType {
			case "tool_search_call":
				invalidArgs = strings.TrimSpace(args) != "" && !isValidSingleJSONObject([]byte(args))
			case "function_call":
				invalidArgs = strings.TrimSpace(args) != "" && !isValidSingleJSON([]byte(args))
			}
			if !incomplete && invalidArgs {
				incomplete = true
				d.invalidToolArgsErr = &ir.StreamError{
					Type:    "invalid_response_error",
					Code:    "invalid_tool_arguments",
					Message: invalidToolArgsMessage(slot.itemType),
				}
			}
			toolKind := ir.ToolFunction
			name := slot.toolName
			switch slot.itemType {
			case "custom_tool_call":
				toolKind = ir.ToolCustom
				if name == "" {
					name = "custom"
				}
			case "tool_search_call":
				toolKind = ir.ToolClientSearch
				if !incomplete {
					if strings.TrimSpace(args) == "" {
						args = "{}"
					} else {
						args = string(compactRawJSON(jsontext.Value(args)))
					}
				}
			default:
				if name == "" {
					name = "function"
				}
			}
			endPart := ir.Part{
				Kind: ir.PartToolCall,
				ID:   slot.itemID,
				Call: &ir.ToolCall{
					CallID: slot.callID,
					Tool: ir.ToolRef{
						Kind:      toolKind,
						Namespace: slot.namespace,
						Name:      name,
					},
					Arguments:  args,
					Incomplete: incomplete,
				},
				Extra: slot.itemExtra,
			}
			slot.closed = true
			out = append(out, d.pushSlot(slot.seqKey, ir.Event{
				Kind: ir.EventPartEnd,
				Part: &endPart,
			})...)
		}
	}
	return out
}

// PartKind returns the IR PartKind corresponding to s.itemType.
func (s *streamOutputItemSlot) PartKind() ir.PartKind {
	if s.itemType == "reasoning" {
		return ir.PartReasoning
	}
	return ir.PartToolCall
}

// invalidToolArgsMessage formats the StreamError message for malformed tool
// arguments on itemType. Client search items report the JSON object constraint,
// while function calls report single-value JSON validation failure.
func invalidToolArgsMessage(itemType string) string {
	if itemType == "tool_search_call" {
		return "upstream tool_search_call arguments must be a valid JSON object"
	}
	return fmt.Sprintf("upstream %s arguments are not valid single-value JSON", itemType)
}

// derefIndex returns the integer index stored at *p when non-nil. Absent JSON
// index pointers default to slot zero so implicit single-item SSE frames stay addressable.
func derefIndex(p *int) int {
	if p == nil {
		return 0
	}
	return *p
}
