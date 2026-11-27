package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream_decode.go incrementally decodes Anthropic Messages SSE frames into
// canonical sequential ir.Event streams. Upstream Messages providers may omit
// SSE event headers (22235a63c), send keepalive ping frames, stream sparse
// message_delta usage counters after full message_start usage, attach
// citations_delta events to text blocks, stream server_tool_use input via
// input_json_delta, or drop the connection mid-block without message_stop (bd39eb589).
//
// StreamDecoder routes block lifecycles through ir.Sequencer, defers opening
// empty text blocks until non-empty deltas or citations arrive so streamed and
// buffered replies satisfy Law S2, validates usage envelopes via wirejson, and
// checks accumulated tool_use JSON objects for completeness and duplicate keys
// on normal completion, marking malformed calls Incomplete and emitting EventError.

const (
	// blockKindText through blockKindOpaque tag the active state-machine behavior
	// of an open Anthropic content block in StreamDecoder.
	blockKindText             streamBlockKind = iota // visible text block with optional citations_delta
	blockKindThinking                                // thinking block with thinking_delta and signature_delta
	blockKindRedactedThinking                        // redacted_thinking block carrying encrypted data
	blockKindToolUse                                 // client tool_use block with input_json_delta
	blockKindOpaque                                  // server_tool_use or provider-specific opaque block
)

// streamBlockKind classifies how StreamDecoder accumulates deltas for an open content block.
type streamBlockKind uint8

// streamBlockSlot tracks accumulated text, signatures, tool JSON fragments, and
// citations for one upstream content_block index in StreamDecoder.
type streamBlockSlot struct {
	rawIndex    int             // upstream content_block index
	seqKey      int             // monotonic slot key inside ir.Sequencer
	kind        streamBlockKind // block state-machine category
	id          string          // tool_use call correlation ID
	name        string          // tool_use invoked function name
	initText    string          // initial text or thinking on content_block_start
	initInput   jsontext.Value  // initial tool_use input JSON object
	rawOpaque   jsontext.Value  // initial compacted JSON for opaque blocks
	opaqueType  string          // wire block type for opaque blocks
	cache       *ir.CacheMark   // optional prompt cache breakpoint
	extra       ir.Extra        // unmodeled block-level members
	annotations []ir.Annotation // accumulated citations from start and citations_delta
	textBuf     strings.Builder // concatenated text_delta or thinking_delta fragments
	sigBuf      strings.Builder // concatenated signature_delta fragments
	argsBuf     strings.Builder // concatenated input_json_delta partial_json fragments
	sawPartial  bool            // true once any input_json_delta arrived
	started     bool            // true once EventPartStart was pushed to ir.Sequencer
	closed      bool            // true once content_block_stop finalized this slot
}

// StreamDecoder decodes Anthropic Messages SSE frames into canonical sequential ir.Event values.
type StreamDecoder struct {
	seq                ir.Sequencer             // serializes block slots into single-open-part order
	usage              streamUsageTracker       // reconciles message_start and sparse message_delta usage
	sawMessageStart    bool                     // true once message_start was decoded
	sawStopReason      bool                     // true once message_delta supplied stop_reason
	sawToolUse         bool                     // true once any tool_use block started
	sawInvalidToolArgs bool                     // true when a closed block had invalid input JSON
	done               bool                     // true after EventResponseEnd or EventError emitted
	stopReason         string                   // latest non-empty stop_reason from message_delta
	stopSequence       *string                  // latest stop_sequence pointer from message_delta
	nextSeqKey         int                      // next monotonic slot key for ir.Sequencer
	blocks             map[int]*streamBlockSlot // active or recently opened blocks by wire index
}

// NewStreamDecoder constructs a fresh StreamDecoder for one upstream Anthropic
// Messages SSE stream attempt. Callers must not reuse a StreamDecoder across
// retries or concurrent upstream attempts because block indices and usage accumulate.
func NewStreamDecoder() *StreamDecoder {
	return &StreamDecoder{}
}

// NewStreamDecoder constructs a fresh StreamDecoder for one upstream Anthropic
// Messages SSE attempt by delegating to the package-level NewStreamDecoder.
// Each stream attempt requires its own independent decoder instance.
func (Codec) NewStreamDecoder() *StreamDecoder {
	return NewStreamDecoder()
}

// Decode processes one SSE frame and returns zero or more canonical sequential
// ir.Event values. Blank frames, ping frames, and frames arriving after a
// terminal event return (nil, nil), whereas malformed JSON, ambiguous usage
// envelopes, or incompatible block deltas return an error.
func (d *StreamDecoder) Decode(frame sse.Frame) ([]ir.Event, error) {
	if d.done {
		return nil, nil
	}
	trimmed := bytes.TrimSpace(frame.Data)
	if len(trimmed) == 0 {
		return nil, nil
	}
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		if d.sawStopReason {
			return d.finishNormal(), nil
		}
		return nil, nil
	}

	var ev wireStreamEvent
	if err := unmarshalJSONObject(trimmed, &ev); err != nil {
		return nil, fmt.Errorf("decode messages stream event: %w", err)
	}

	evType := strings.TrimSpace(ev.Type)
	if evType == "" {
		evType = strings.TrimSpace(frame.Event)
	}
	if evType == "" {
		return nil, errors.New("decode messages stream event: missing event type")
	}

	switch evType {
	case "ping":
		return nil, nil

	case "message_start":
		return d.handleMessageStart(trimmed, ev)

	case "content_block_start":
		return d.handleContentBlockStart(ev)

	case "content_block_delta":
		return d.handleContentBlockDelta(ev)

	case "content_block_stop":
		return d.handleContentBlockStop(ev)

	case "message_delta":
		return d.handleMessageDelta(trimmed, ev)

	case "message_stop":
		return d.finishNormal(), nil

	case "error":
		se := decodeStreamError(ev.Error)
		if se == nil {
			se = &ir.StreamError{
				Type:    "api_error",
				Message: defaultStreamErrorMessage,
			}
		}
		return d.finishWithError(se), nil

	default:
		return nil, fmt.Errorf("decode messages stream event: unsupported event type %q", evType)
	}
}

// Close finalizes the stream at EOF or transport interruption and returns any
// remaining block closures followed by the terminal EventResponseEnd or
// EventError. Clean EOF without message_stop succeeds only when message_delta
// already delivered a stop_reason and all blocks closed; otherwise Close marks
// open tool calls Incomplete and emits a truncation EventError (bd39eb589).
func (d *StreamDecoder) Close(cause error) []ir.Event {
	if d.done {
		return nil
	}
	if cause != nil {
		return d.finishWithError(truncatedStreamError(cause))
	}
	if d.sawStopReason && !d.hasUnclosedBlocks() {
		return d.finishNormal()
	}
	return d.finishWithError(truncatedStreamError(nil))
}

// handleMessageStart processes a message_start SSE frame by pushing
// EventResponseStart into ir.Sequencer and recording response metadata.
// Duplicate message_start frames or invalid initial usage envelopes return an error.
func (d *StreamDecoder) handleMessageStart(frameBytes []byte, ev wireStreamEvent) ([]ir.Event, error) {
	if d.sawMessageStart {
		return nil, errors.New("duplicate message_start event")
	}
	d.sawMessageStart = true

	var meta ir.ResponseMeta
	var rawUsage jsontext.Value
	if ev.Message != nil {
		meta.ID = ev.Message.ID
		meta.Model = ev.Message.Model
		rawUsage = ev.Message.Usage
	}

	out := d.seq.Push(ir.Event{
		Kind:     ir.EventResponseStart,
		Response: &meta,
	})

	if len(bytes.TrimSpace(rawUsage)) > 0 {
		u, ok, err := d.usage.observe(frameBytes, rawUsage, true)
		if err != nil {
			return nil, fmt.Errorf("decode message_start usage: %w", err)
		}
		if ok && u != nil {
			out = append(out, d.seq.Push(ir.Event{
				Kind:  ir.EventUsage,
				Usage: u,
			})...)
		}
	}
	return out, nil
}

// handleContentBlockStart opens a new content block slot at ev.Index and pushes
// its skeleton EventPartStart into ir.Sequencer when immediately observable.
// Empty text blocks without citations, cache_control, or Extra defer EventPartStart
// until their first non-empty delta so empty blocks match DecodeResponse under Law S2.
func (d *StreamDecoder) handleContentBlockStart(ev wireStreamEvent) ([]ir.Event, error) {
	if ev.Index == nil || *ev.Index < 0 {
		return nil, errors.New("content_block_start requires non-negative index")
	}
	idx := *ev.Index
	if d.blocks != nil {
		if prev := d.blocks[idx]; prev != nil && !prev.closed {
			return nil, fmt.Errorf("overlapping content_block_start at index %d", idx)
		}
	}

	cbTrim := bytes.TrimSpace(ev.ContentBlock)
	var probe wireTypeProbe
	if err := unmarshalJSONObject(cbTrim, &probe); err != nil {
		return nil, fmt.Errorf("decode content_block_start[%d]: %w", idx, err)
	}
	if strings.TrimSpace(probe.Type) == "" {
		return nil, fmt.Errorf("content_block_start[%d]: empty content_block.type", idx)
	}

	if d.blocks == nil {
		d.blocks = make(map[int]*streamBlockSlot)
	}

	slot := &streamBlockSlot{
		rawIndex: idx,
	}

	var startPart ir.Part
	switch probe.Type {
	case "text":
		var tb wireResponseTextBlock
		if err := unmarshalJSONObject(cbTrim, &tb); err != nil {
			return nil, fmt.Errorf("decode text content_block_start[%d]: %w", idx, err)
		}
		cache, err := decodeCacheControl(tb.CacheControl)
		if err != nil {
			return nil, err
		}
		slot.kind = blockKindText
		slot.initText = tb.Text
		slot.cache = cache
		slot.extra = toExtra(tb.Unknown)
		slot.annotations = decodeCitations(tb.Citations)
		// Defer EventPartStart on empty unannotated text blocks so blocks that
		// close with zero text deltas match DecodeResponse's empty-text filtering.
		if tb.Text == "" && len(slot.annotations) == 0 && cache == nil && slot.extra.IsZero() {
			d.blocks[idx] = slot
			return nil, nil
		}
		startPart = ir.Part{
			Kind:  ir.PartText,
			Text:  tb.Text,
			Cache: cloneCacheMarkPtr(cache),
			Extra: slot.extra.Clone(),
		}

	case "thinking":
		var th wireThinkingBlock
		if err := unmarshalJSONObject(cbTrim, &th); err != nil {
			return nil, fmt.Errorf("decode thinking content_block_start[%d]: %w", idx, err)
		}
		cache, err := decodeCacheControl(th.CacheControl)
		if err != nil {
			return nil, err
		}
		slot.kind = blockKindThinking
		slot.initText = th.Thinking
		if th.Signature != "" {
			_, _ = slot.sigBuf.WriteString(th.Signature)
		}
		slot.cache = cache
		slot.extra = toExtra(th.Unknown)
		var segs []string
		if th.Thinking != "" {
			segs = []string{th.Thinking}
		}
		startPart = ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Segments: segs,
			},
			Cache: cloneCacheMarkPtr(cache),
			Extra: slot.extra.Clone(),
		}

	case "redacted_thinking":
		var rt wireRedactedThinkingBlock
		if err := unmarshalJSONObject(cbTrim, &rt); err != nil {
			return nil, fmt.Errorf("decode redacted_thinking content_block_start[%d]: %w", idx, err)
		}
		if rt.Data == "" {
			return nil, fmt.Errorf("content_block_start[%d]: redacted_thinking requires non-empty data", idx)
		}
		cache, err := decodeCacheControl(rt.CacheControl)
		if err != nil {
			return nil, err
		}
		slot.kind = blockKindRedactedThinking
		_, _ = slot.sigBuf.WriteString(rt.Data)
		slot.cache = cache
		slot.extra = toExtra(rt.Unknown)
		startPart = ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Redacted: true,
				Token:    &ir.ReplayToken{Data: rt.Data},
			},
			Cache: cloneCacheMarkPtr(cache),
			Extra: slot.extra.Clone(),
		}

	case "tool_use":
		var tu wireToolUseBlock
		if err := unmarshalJSONObject(cbTrim, &tu); err != nil {
			return nil, fmt.Errorf("decode tool_use content_block_start[%d]: %w", idx, err)
		}
		if strings.TrimSpace(tu.Name) == "" {
			return nil, fmt.Errorf("content_block_start[%d]: tool_use requires non-empty name", idx)
		}
		if len(bytes.TrimSpace(tu.Input)) > 0 && !isValidSingleJSONObject(tu.Input) {
			return nil, fmt.Errorf("content_block_start[%d]: tool_use.input must be a JSON object", idx)
		}
		cache, err := decodeCacheControl(tu.CacheControl)
		if err != nil {
			return nil, err
		}
		slot.kind = blockKindToolUse
		slot.id = tu.ID
		slot.name = tu.Name
		slot.initInput = compactRawJSON(tu.Input)
		slot.cache = cache
		slot.extra = toExtra(tu.Unknown)
		d.sawToolUse = true
		startPart = ir.Part{
			Kind: ir.PartToolCall,
			Call: &ir.ToolCall{
				CallID: tu.ID,
				Tool:   ir.ToolRef{Kind: ir.ToolFunction, Name: tu.Name},
			},
			Cache: cloneCacheMarkPtr(cache),
			Extra: slot.extra.Clone(),
		}

	default:
		slot.kind = blockKindOpaque
		slot.opaqueType = probe.Type
		slot.rawOpaque = compactRawJSON(cbTrim)
		startPart = ir.Part{
			Kind: ir.PartOpaque,
			Opaque: &ir.OpaqueItem{
				Origin: protocol.ProtocolAnthropicMessages,
				Type:   probe.Type,
				Raw:    jsontext.Value(bytes.Clone(slot.rawOpaque)),
			},
		}
	}

	slot.seqKey = d.nextSeqKey
	d.nextSeqKey++
	slot.started = true
	d.blocks[idx] = slot
	return d.seq.Push(ir.Event{
		Kind:  ir.EventPartStart,
		Index: slot.seqKey,
		Part:  &startPart,
	}), nil
}

// ensureTextSlotStarted allocates a Sequencer key and emits EventPartStart for
// a deferred text block slot upon its first non-empty text or citation delta.
// Slots that already emitted EventPartStart return nil without side effects.
func (d *StreamDecoder) ensureTextSlotStarted(slot *streamBlockSlot) []ir.Event {
	if slot.started {
		return nil
	}
	slot.seqKey = d.nextSeqKey
	d.nextSeqKey++
	slot.started = true
	startPart := ir.Part{
		Kind:  ir.PartText,
		Text:  slot.initText,
		Cache: cloneCacheMarkPtr(slot.cache),
		Extra: slot.extra.Clone(),
	}
	return d.seq.Push(ir.Event{
		Kind:  ir.EventPartStart,
		Index: slot.seqKey,
		Part:  &startPart,
	})
}

// handleContentBlockDelta validates delta type compatibility against the open
// block at ev.Index and accumulates incremental fragments. Visible text,
// thinking, and client tool-argument deltas push EventPartDelta into ir.Sequencer.
func (d *StreamDecoder) handleContentBlockDelta(ev wireStreamEvent) ([]ir.Event, error) {
	if ev.Index == nil || ev.Delta == nil {
		return nil, errors.New("content_block_delta requires index and delta")
	}
	var slot *streamBlockSlot
	if d.blocks != nil {
		slot = d.blocks[*ev.Index]
	}
	if slot == nil || slot.closed {
		return nil, fmt.Errorf("content_block_delta for unopened block index %d", *ev.Index)
	}

	switch ev.Delta.Type {
	case "text_delta":
		if slot.kind != blockKindText {
			return nil, fmt.Errorf("text_delta incompatible with block index %d", *ev.Index)
		}
		if ev.Delta.Text == "" {
			return nil, nil
		}
		out := d.ensureTextSlotStarted(slot)
		_, _ = slot.textBuf.WriteString(ev.Delta.Text)
		out = append(out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: slot.seqKey,
			Delta: &ir.Delta{Text: ev.Delta.Text},
		})...)
		return out, nil

	case "citations_delta":
		if slot.kind != blockKindText {
			return nil, fmt.Errorf("citations_delta incompatible with block index %d", *ev.Index)
		}
		citTrim := bytes.TrimSpace(ev.Delta.Citation)
		if len(citTrim) == 0 || bytes.Equal(citTrim, []byte("null")) {
			return nil, nil
		}
		var cit wireCitation
		if err := unmarshalJSONObject(citTrim, &cit); err != nil {
			return nil, fmt.Errorf("decode citations_delta: %w", err)
		}
		out := d.ensureTextSlotStarted(slot)
		slot.annotations = append(slot.annotations, decodeCitations([]wireCitation{cit})...)
		return out, nil

	case "thinking_delta":
		if slot.kind != blockKindThinking {
			return nil, fmt.Errorf("thinking_delta incompatible with block index %d", *ev.Index)
		}
		if ev.Delta.Thinking == "" {
			return nil, nil
		}
		_, _ = slot.textBuf.WriteString(ev.Delta.Thinking)
		return d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: slot.seqKey,
			Delta: &ir.Delta{Text: ev.Delta.Thinking},
		}), nil

	case "signature_delta":
		if slot.kind != blockKindThinking {
			return nil, fmt.Errorf("signature_delta incompatible with block index %d", *ev.Index)
		}
		_, _ = slot.sigBuf.WriteString(ev.Delta.Signature)
		return nil, nil

	case "input_json_delta":
		if slot.kind != blockKindToolUse && slot.kind != blockKindOpaque {
			return nil, fmt.Errorf("input_json_delta incompatible with block index %d", *ev.Index)
		}
		slot.sawPartial = true
		if ev.Delta.PartialJSON == "" {
			return nil, nil
		}
		_, _ = slot.argsBuf.WriteString(ev.Delta.PartialJSON)
		if slot.kind == blockKindOpaque {
			// PartOpaque forbids EventPartDelta in ValidateStream; server_tool_use
			// arguments merge into the complete OpaqueItem.Raw snapshot at block stop.
			return nil, nil
		}
		return d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: slot.seqKey,
			Delta: &ir.Delta{Arguments: ev.Delta.PartialJSON},
		}), nil

	default:
		return nil, fmt.Errorf("unsupported content_block_delta type %q", ev.Delta.Type)
	}
}

// handleContentBlockStop finalizes the open block at ev.Index and pushes its
// complete EventPartEnd snapshot into ir.Sequencer. Missing, unopened, or
// already closed block indices return an error immediately.
func (d *StreamDecoder) handleContentBlockStop(ev wireStreamEvent) ([]ir.Event, error) {
	if ev.Index == nil {
		return nil, errors.New("content_block_stop requires index")
	}
	var slot *streamBlockSlot
	if d.blocks != nil {
		slot = d.blocks[*ev.Index]
	}
	if slot == nil || slot.closed {
		return nil, fmt.Errorf("content_block_stop for unopened block index %d", *ev.Index)
	}
	return d.closeSlot(slot, true), nil
}

// handleMessageDelta updates stop_reason and stop_sequence state from ev.Delta
// on a message_delta SSE frame. Non-empty usage objects are validated with
// wirejson and merged into a cumulative EventUsage snapshot.
func (d *StreamDecoder) handleMessageDelta(frameBytes []byte, ev wireStreamEvent) ([]ir.Event, error) {
	if ev.Delta != nil {
		if strings.TrimSpace(ev.Delta.StopReason) != "" {
			d.sawStopReason = true
			d.stopReason = ev.Delta.StopReason
		}
		if ev.Delta.StopSequence != nil {
			seq := *ev.Delta.StopSequence
			d.stopSequence = &seq
		}
	}

	var out []ir.Event
	if len(bytes.TrimSpace(ev.Usage)) > 0 {
		u, ok, err := d.usage.observe(frameBytes, ev.Usage, false)
		if err != nil {
			return nil, fmt.Errorf("decode message_delta usage: %w", err)
		}
		if ok && u != nil {
			out = append(out, d.seq.Push(ir.Event{
				Kind:  ir.EventUsage,
				Usage: u,
			})...)
		}
	}
	return out, nil
}

// closeSlot materializes the complete Part snapshot for slot and pushes
// EventPartEnd into ir.Sequencer. Deferred empty text blocks that never opened
// mark themselves closed and emit no events.
//
// On normal closures (normal=true), accumulated tool_use and server_tool_use
// input_json_delta strings must parse as single duplicate-free JSON objects;
// malformed or duplicate-key inputs mark tool_use Incomplete and set
// sawInvalidToolArgs so finishNormal terminates the stream with EventError.
func (d *StreamDecoder) closeSlot(slot *streamBlockSlot, normal bool) []ir.Event {
	slot.closed = true
	if !slot.started {
		return nil
	}
	var endPart ir.Part

	switch slot.kind {
	case blockKindText:
		endPart = ir.Part{
			Kind:        ir.PartText,
			Text:        slot.initText + slot.textBuf.String(),
			Annotations: slices.Clone(slot.annotations),
			Cache:       cloneCacheMarkPtr(slot.cache),
			Extra:       slot.extra.Clone(),
		}

	case blockKindThinking:
		fullThinking := slot.initText + slot.textBuf.String()
		var segs []string
		if fullThinking != "" {
			segs = []string{fullThinking}
		}
		var tok *ir.ReplayToken
		if slot.sigBuf.Len() > 0 {
			tok = &ir.ReplayToken{Data: slot.sigBuf.String()}
		}
		endPart = ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Segments: segs,
				Token:    tok,
			},
			Cache: cloneCacheMarkPtr(slot.cache),
			Extra: slot.extra.Clone(),
		}

	case blockKindRedactedThinking:
		endPart = ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Redacted: true,
				Token:    &ir.ReplayToken{Data: slot.sigBuf.String()},
			},
			Cache: cloneCacheMarkPtr(slot.cache),
			Extra: slot.extra.Clone(),
		}

	case blockKindToolUse:
		incomplete := !normal
		var finalArgs string
		if slot.sawPartial {
			rawArgs := slot.argsBuf.String()
			trimmedArgs := strings.TrimSpace(rawArgs)
			if trimmedArgs == "" {
				finalArgs = "{}"
			} else if !normal {
				finalArgs = rawArgs
			} else if !isValidSingleJSONObject([]byte(trimmedArgs)) {
				incomplete = true
				d.sawInvalidToolArgs = true
				finalArgs = rawArgs
			} else {
				finalArgs = string(compactRawJSON(jsontext.Value(trimmedArgs)))
			}
		} else if len(slot.initInput) > 0 {
			finalArgs = string(slot.initInput)
		} else {
			finalArgs = "{}"
		}
		endPart = ir.Part{
			Kind: ir.PartToolCall,
			Call: &ir.ToolCall{
				CallID:     slot.id,
				Tool:       ir.ToolRef{Kind: ir.ToolFunction, Name: slot.name},
				Arguments:  finalArgs,
				Incomplete: incomplete,
			},
			Cache: cloneCacheMarkPtr(slot.cache),
			Extra: slot.extra.Clone(),
		}

	case blockKindOpaque:
		rawSnap := jsontext.Value(bytes.Clone(slot.rawOpaque))
		if slot.sawPartial {
			trimmedArgs := strings.TrimSpace(slot.argsBuf.String())
			if trimmedArgs != "" {
				if isValidSingleJSONObject([]byte(trimmedArgs)) {
					if patched, err := replaceJSONObjectMember(rawSnap, "input", jsontext.Value(trimmedArgs)); err == nil {
						rawSnap = patched
					}
				} else if normal {
					d.sawInvalidToolArgs = true
				}
			}
		}
		endPart = ir.Part{
			Kind: ir.PartOpaque,
			Opaque: &ir.OpaqueItem{
				Origin: protocol.ProtocolAnthropicMessages,
				Type:   slot.opaqueType,
				Raw:    rawSnap,
			},
		}
	}

	return d.seq.Push(ir.Event{
		Kind:  ir.EventPartEnd,
		Index: slot.seqKey,
		Part:  &endPart,
	})
}

// hasUnclosedBlocks reports whether any started content block slot has not yet received content_block_stop.
func (d *StreamDecoder) hasUnclosedBlocks() bool {
	for _, slot := range d.blocks {
		if !slot.closed {
			return true
		}
	}
	return false
}

// closeRemainingBlocks finalizes all unclosed slots in ascending Sequencer key
// order and returns their closing events. Deferred empty text slots mark
// themselves closed without emitting EventPartEnd.
func (d *StreamDecoder) closeRemainingBlocks(normal bool) []ir.Event {
	if len(d.blocks) == 0 {
		return nil
	}
	openSlots := make([]*streamBlockSlot, 0, len(d.blocks))
	for _, slot := range d.blocks {
		if !slot.closed {
			openSlots = append(openSlots, slot)
		}
	}
	slices.SortFunc(openSlots, func(a, b *streamBlockSlot) int {
		if a.seqKey < b.seqKey {
			return -1
		}
		if a.seqKey > b.seqKey {
			return 1
		}
		return 0
	})
	var out []ir.Event
	for _, slot := range openSlots {
		out = append(out, d.closeSlot(slot, normal)...)
	}
	return out
}

// finishNormal closes any remaining open blocks and emits EventResponseEnd. When
// any closed tool block accumulated malformed or duplicate-key JSON arguments,
// it emits a terminal EventError instead of EventResponseEnd.
func (d *StreamDecoder) finishNormal() []ir.Event {
	var out []ir.Event
	out = append(out, d.closeRemainingBlocks(true)...)
	if d.sawInvalidToolArgs {
		d.done = true
		out = append(out, d.seq.Push(ir.Event{
			Kind: ir.EventError,
			Err: &ir.StreamError{
				Type:    "api_error",
				Code:    "invalid_tool_arguments",
				Message: "upstream messages stream ended with invalid tool_use input JSON",
			},
		})...)
		return out
	}

	fin := decodeStopReason(d.stopReason, d.stopSequence, d.sawToolUse)
	out = append(out, d.seq.Push(ir.Event{
		Kind:   ir.EventResponseEnd,
		Finish: &fin,
	})...)
	d.done = true
	return out
}

// finishWithError closes all remaining open blocks abnormally so unfinished
// tool_use calls report Incomplete=true. It then pushes a single terminal
// EventError carrying se and marks d done so subsequent calls return nil.
func (d *StreamDecoder) finishWithError(se *ir.StreamError) []ir.Event {
	var out []ir.Event
	out = append(out, d.closeRemainingBlocks(false)...)
	out = append(out, d.seq.Push(ir.Event{
		Kind: ir.EventError,
		Err:  se,
	})...)
	d.done = true
	return out
}
