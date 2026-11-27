package chat

import (
	"bytes"
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream_decode.go incrementally decodes OpenAI Chat Completions SSE frames
// into canonical sequential ir.Event streams. Upstream Chat providers can
// interleave parallel tool-call argument chunks across indices, emit empty id
// or function.name strings on follow-up argument chunks (826f45a2c), send
// arguments before function.name arrives, deliver sparse usage-only tail chunks
// after finish_reason, or drop the connection without any terminal marker (bd39eb589).
//
// StreamDecoder delegates slot ordering to ir.Sequencer so active content
// streams immediately while concurrent parallel tool calls buffer in index
// order until the active part closes. Follow-up empty identities never clobber
// earlier tool metadata, and pre-name arguments buffer until function.name arrives.
// Observing finish_reason defers EventResponseEnd until [DONE] or clean EOF so
// trailing usage chunks emit beforehand, whereas silent EOF without finish_reason
// emits an abnormal EventError after marking open tool calls Incomplete.

// streamToolSlot tracks accumulated identity, pre-name argument fragments, and
// Sequencer slot assignment for one upstream delta.tool_calls[index] entry.
type streamToolSlot struct {
	rawIndex     int             // upstream tool_calls[].index value
	seqKey       int             // monotonic slot key inside ir.Sequencer
	id           string          // first non-empty call identifier observed
	name         string          // first non-empty function.name observed
	extra        ir.Extra        // Extra members captured on opening tool_call delta
	bufferedArgs []string        // argument fragments received before name arrived
	accArgs      strings.Builder // concatenated arguments for terminal JSON check
	started      bool            // whether EventPartStart has been pushed to Sequencer
	closed       bool            // whether EventPartEnd has been pushed to Sequencer
}

// StreamDecoder decodes OpenAI Chat Completions SSE frames into canonical ir.Event sequences.
type StreamDecoder struct {
	seq                ir.Sequencer            // serializes interleaved tool call slots
	usageAcc           openai.UsageAccumulator // accumulates sparse usage chunks
	prevUsage          *ir.Usage               // latest emitted cumulative Usage snapshot
	done               bool                    // true after EventResponseEnd or EventError
	sawFinish          bool                    // true once a non-empty finish_reason arrived
	sawNamelessTool    bool                    // true when a closed tool slot never received name
	sawInvalidToolArgs bool                    // true when a closed tool slot had invalid JSON args
	finishReason       string                  // latest non-empty finish_reason string
	sawToolCall        bool                    // true once any tool_calls delta arrived
	serviceTier        string                  // latest non-empty service_tier string
	openPhase          ir.PartKind             // active non-tool part kind, or ""
	phaseSlot          int                     // Sequencer slot key of openPhase
	nextSlot           int                     // next monotonically increasing slot key
	toolSlotBase       int                     // base Sequencer key for current tool group
	maxToolKey         int                     // highest Sequencer key used in current tool group
	toolSlots          map[int]*streamToolSlot // open tool group keyed by upstream index
	negToolOffset      int                     // sequential key allocator for negative fuzz indices
}

// NewStreamDecoder constructs a stateful StreamDecoder for one upstream Chat
// Completions SSE attempt. Each decoder owns mutable Sequencer and usage
// accumulation state and must not be shared concurrently across streams.
func NewStreamDecoder() *StreamDecoder {
	return &StreamDecoder{}
}

// NewStreamDecoder constructs a fresh StreamDecoder for one upstream Chat
// Completions SSE attempt so Codec satisfies the streaming decoder contract.
// Callers must allocate an independent decoder per upstream stream attempt.
func (Codec) NewStreamDecoder() *StreamDecoder {
	return NewStreamDecoder()
}

// Decode processes one SSE frame and returns zero or more canonical sequential
// ir.Event values. Blank frames and frames arriving after [DONE] or an error
// frame return (nil, nil), whereas malformed JSON or ambiguous usage envelopes
// return an error so the caller can abort via Close.
//
// Non-error JSON chunks emit EventResponseStart once from the first observed
// chunk envelope, route choices[0] deltas through ir.Sequencer, and emit
// EventUsage after wirejson validation and monotonic sparse-chunk merging.
func (d *StreamDecoder) Decode(frame sse.Frame) ([]ir.Event, error) {
	if d.done {
		return nil, nil
	}
	trimmed := bytes.TrimSpace(frame.Data)
	if len(trimmed) == 0 {
		return nil, nil
	}

	if bytes.Equal(trimmed, []byte("[DONE]")) {
		return d.finishNormal(), nil
	}

	var chunk wireStreamChunk
	if err := unmarshalJSONObject(trimmed, &chunk); err != nil {
		return nil, fmt.Errorf("decode chat stream chunk: %w", err)
	}

	if se := decodeStreamError(chunk.Error); se != nil {
		return d.finishWithError(se), nil
	}

	var out []ir.Event
	if !d.seq.Started() {
		out = append(out, d.seq.Push(ir.Event{
			Kind: ir.EventResponseStart,
			Response: &ir.ResponseMeta{
				ID:          chunk.ID,
				Model:       chunk.Model,
				CreatedAt:   chunk.Created,
				ServiceTier: chunk.ServiceTier,
			},
		})...)
	}
	if chunk.ServiceTier != "" {
		d.serviceTier = chunk.ServiceTier
	}

	if len(chunk.Choices) > 0 {
		if err := d.decodeChoiceDelta(chunk.Choices[0], &out); err != nil {
			return nil, err
		}
	}

	if len(chunk.Usage) > 0 {
		mergedUsage, ok, err := observeStreamUsage(&d.usageAcc, d.prevUsage, trimmed, chunk.Usage)
		if err != nil {
			return nil, fmt.Errorf("decode chat stream usage: %w", err)
		}
		if ok && mergedUsage != nil {
			d.prevUsage = mergedUsage.Clone()
			out = append(out, d.seq.Push(ir.Event{
				Kind:  ir.EventUsage,
				Usage: mergedUsage,
			})...)
		}
	}

	return out, nil
}

// Close finalizes the stream at EOF or transport interruption and returns any
// remaining part closures and the single terminal EventResponseEnd or EventError.
// Clean EOF without a prior [DONE] succeeds only when finish_reason was observed;
// otherwise Close emits an abnormal truncation EventError (bd39eb589).
func (d *StreamDecoder) Close(cause error) []ir.Event {
	if d.done {
		return nil
	}
	if cause != nil {
		return d.finishWithError(truncatedStreamError(cause))
	}
	if d.sawFinish {
		return d.finishNormal()
	}
	return d.finishWithError(truncatedStreamError(nil))
}

// decodeChoiceDelta processes reasoning, visible text, refusal, tool_calls, and
// finish_reason fields on choices[0] of an incoming Chat stream chunk. Legacy
// function_call deltas map onto tool slot zero alongside modern tool_calls.
func (d *StreamDecoder) decodeChoiceDelta(choice wireChunkChoice, out *[]ir.Event) error {
	delta := choice.Delta

	// Explicit empty reasoning_content suppresses fallback to reasoning on the
	// same chunk, matching protocol/openai.ChatDelta.ReasoningText.
	var reasoningFrag *string
	if delta.ReasoningContent != nil {
		reasoningFrag = delta.ReasoningContent
	} else {
		reasoningFrag = delta.Reasoning
	}
	if reasoningFrag != nil && *reasoningFrag != "" {
		d.switchToPhase(ir.PartReasoning, out)
		*out = append(*out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: d.phaseSlot,
			Delta: &ir.Delta{Text: *reasoningFrag},
		})...)
	}

	if delta.Content != nil && *delta.Content != "" {
		d.switchToPhase(ir.PartText, out)
		*out = append(*out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: d.phaseSlot,
			Delta: &ir.Delta{Text: *delta.Content},
		})...)
	}

	if delta.Refusal != nil && *delta.Refusal != "" {
		d.switchToPhase(ir.PartRefusal, out)
		*out = append(*out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: d.phaseSlot,
			Delta: &ir.Delta{Text: *delta.Refusal},
		})...)
	}

	for i, tc := range delta.ToolCalls {
		if err := d.consumeToolCallDelta(tc, out); err != nil {
			return fmt.Errorf("tool_calls[%d]: %w", i, err)
		}
	}
	if delta.FunctionCall != nil {
		zero := 0
		synthetic := wireToolCall{
			Index:    &zero,
			Type:     "function",
			Function: *delta.FunctionCall,
		}
		if err := d.consumeToolCallDelta(synthetic, out); err != nil {
			return fmt.Errorf("function_call: %w", err)
		}
	}

	if choice.FinishReason != nil {
		if fr := strings.TrimSpace(*choice.FinishReason); fr != "" {
			d.sawFinish = true
			d.finishReason = fr
		}
	}
	return nil
}

// switchToPhase closes any open tool group or different non-tool part phase and
// opens a new Sequencer slot of kind. Consecutive deltas of the same kind reuse
// the active slot without emitting extra part boundaries.
func (d *StreamDecoder) switchToPhase(kind ir.PartKind, out *[]ir.Event) {
	if len(d.toolSlots) > 0 {
		if d.closeToolGroup(true, out) {
			d.sawNamelessTool = true
		}
	}
	if d.openPhase == kind {
		return
	}
	if d.openPhase != "" {
		*out = append(*out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: d.phaseSlot,
		})...)
		d.openPhase = ""
	}
	d.phaseSlot = d.nextSlot
	d.nextSlot++
	d.openPhase = kind

	skel := ir.Part{Kind: kind}
	if kind == ir.PartReasoning {
		skel.Reasoning = &ir.Reasoning{}
	}
	*out = append(*out, d.seq.Push(ir.Event{
		Kind:  ir.EventPartStart,
		Index: d.phaseSlot,
		Part:  &skel,
	})...)
}

// consumeToolCallDelta updates or opens the tool slot addressed by tc.Index and
// pushes EventPartStart and EventPartDelta events into ir.Sequencer.
//
// Follow-up argument deltas from OpenAI-compatible gateways routinely omit or
// blank id and function.name, so consumeToolCallDelta latches the first
// non-empty values per slot and buffers early argument fragments until a chunk
// supplies a non-empty function.name.
func (d *StreamDecoder) consumeToolCallDelta(tc wireToolCall, out *[]ir.Event) error {
	if d.openPhase != "" {
		*out = append(*out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: d.phaseSlot,
		})...)
		d.openPhase = ""
	}

	args, err := decodeFunctionArguments(tc.Function.RawArgs)
	if err != nil {
		return err
	}

	rawIdx := 0
	if tc.Index != nil {
		rawIdx = *tc.Index
	}
	slot := d.getOrCreateToolSlot(rawIdx)

	// Guard against 826f45a2c: follow-up argument chunks with empty id or empty
	// function.name must never overwrite already-established call metadata.
	if slot.id == "" && tc.ID != "" {
		slot.id = tc.ID
	}
	if slot.name == "" && strings.TrimSpace(tc.Function.Name) != "" {
		slot.name = tc.Function.Name
	}
	if slot.extra.IsZero() && len(tc.Unknown) > 0 {
		slot.extra = toExtra(tc.Unknown)
	}
	d.sawToolCall = true

	if !slot.started {
		if slot.name == "" {
			// Hold early argument fragments until a chunk supplies function.name
			// so EventPartStart satisfies ValidateStream's non-empty name invariant.
			if args != "" {
				slot.bufferedArgs = append(slot.bufferedArgs, args)
			}
			return nil
		}
		slot.started = true
		*out = append(*out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartStart,
			Index: slot.seqKey,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID: slot.id,
					Tool:   ir.ToolRef{Kind: ir.ToolFunction, Name: slot.name},
				},
				Extra: slot.extra.Clone(),
			},
		})...)
		for _, buffered := range slot.bufferedArgs {
			_, _ = slot.accArgs.WriteString(buffered)
			*out = append(*out, d.seq.Push(ir.Event{
				Kind:  ir.EventPartDelta,
				Index: slot.seqKey,
				Delta: &ir.Delta{Arguments: buffered},
			})...)
		}
		slot.bufferedArgs = nil
	}

	if args != "" {
		_, _ = slot.accArgs.WriteString(args)
		*out = append(*out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: slot.seqKey,
			Delta: &ir.Delta{Arguments: args},
		})...)
	}
	return nil
}

// getOrCreateToolSlot returns the streamToolSlot for rawIdx inside the active
// tool group. Non-negative indices map onto ascending Sequencer keys starting
// at toolSlotBase, whereas negative fuzzed indices receive isolated high keys.
func (d *StreamDecoder) getOrCreateToolSlot(rawIdx int) *streamToolSlot {
	if d.toolSlots == nil {
		d.toolSlots = make(map[int]*streamToolSlot)
		d.toolSlotBase = d.nextSlot
		d.maxToolKey = d.nextSlot
	}
	if existing, ok := d.toolSlots[rawIdx]; ok {
		return existing
	}
	var seqKey int
	if rawIdx >= 0 {
		seqKey = d.toolSlotBase + rawIdx
	} else {
		// Negative indices in fuzzed payloads allocate distinct keys above the
		// non-negative window without colliding with valid 0..N slots.
		d.negToolOffset++
		seqKey = d.maxToolKey + 1000 + d.negToolOffset
	}
	if seqKey > d.maxToolKey {
		d.maxToolKey = seqKey
	}
	slot := &streamToolSlot{
		rawIndex: rawIdx,
		seqKey:   seqKey,
	}
	d.toolSlots[rawIdx] = slot
	return slot
}

// closeToolGroup finalizes all tracked tool slots in ascending Sequencer key
// order and reports whether any unstarted nameless tool call fragment remained.
// When normal is false or a started slot accumulated malformed or duplicate-key
// JSON arguments, closed tool calls carry Incomplete=true on EventPartEnd.
func (d *StreamDecoder) closeToolGroup(normal bool, out *[]ir.Event) bool {
	if len(d.toolSlots) == 0 {
		return false
	}
	slots := make([]*streamToolSlot, 0, len(d.toolSlots))
	for _, s := range d.toolSlots {
		slots = append(slots, s)
	}
	slices.SortFunc(slots, func(a, b *streamToolSlot) int {
		if a.seqKey < b.seqKey {
			return -1
		}
		if a.seqKey > b.seqKey {
			return 1
		}
		return 0
	})

	sawNameless := false
	for _, slot := range slots {
		if !slot.started {
			if slot.id != "" || len(slot.bufferedArgs) > 0 {
				sawNameless = true
			}
			continue
		}
		if slot.closed {
			continue
		}
		slot.closed = true
		incomplete := !normal
		// Validate completed non-empty tool argument strings against single-value
		// JSON syntax and duplicate-key rejection before claiming normal completion.
		if normal {
			if fullArgs := slot.accArgs.String(); strings.TrimSpace(fullArgs) != "" && !isValidSingleJSON([]byte(fullArgs)) {
				incomplete = true
				d.sawInvalidToolArgs = true
			}
		}
		*out = append(*out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: slot.seqKey,
			Part: &ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID:     slot.id,
					Tool:       ir.ToolRef{Kind: ir.ToolFunction, Name: slot.name},
					Incomplete: incomplete,
				},
				Extra: slot.extra.Clone(),
			},
		})...)
	}

	d.nextSlot = max(d.nextSlot, d.maxToolKey+1)
	d.toolSlots = nil
	return sawNameless
}

// finishNormal closes open phases and tool slots cleanly and emits
// EventResponseEnd. If any tool slot lacked function.name or accumulated
// invalid JSON arguments, finishNormal emits a terminal EventError instead.
func (d *StreamDecoder) finishNormal() []ir.Event {
	var out []ir.Event
	if d.openPhase != "" {
		out = append(out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: d.phaseSlot,
		})...)
		d.openPhase = ""
	}
	if d.closeToolGroup(true, &out) || d.sawNamelessTool {
		d.done = true
		out = append(out, d.seq.Push(ir.Event{
			Kind: ir.EventError,
			Err: &ir.StreamError{
				Type:    "stream_error",
				Code:    "missing_tool_name",
				Message: "upstream chat stream ended before tool_call function.name arrived",
			},
		})...)
		return out
	}
	if d.sawInvalidToolArgs {
		d.done = true
		out = append(out, d.seq.Push(ir.Event{
			Kind: ir.EventError,
			Err: &ir.StreamError{
				Type:    "stream_error",
				Code:    "invalid_tool_arguments",
				Message: "upstream chat stream ended with invalid tool_call function.arguments JSON",
			},
		})...)
		return out
	}

	fin := decodeFinishReason(d.finishReason, d.sawToolCall)
	out = append(out, d.seq.Push(ir.Event{
		Kind:        ir.EventResponseEnd,
		Finish:      &fin,
		ServiceTier: d.serviceTier,
	})...)
	d.done = true
	return out
}

// finishWithError closes open parts and emits a single terminal EventError.
// Any started tool call still open at failure time receives Incomplete=true.
func (d *StreamDecoder) finishWithError(se *ir.StreamError) []ir.Event {
	var out []ir.Event
	if d.openPhase != "" {
		out = append(out, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: d.phaseSlot,
		})...)
		d.openPhase = ""
	}
	_ = d.closeToolGroup(false, &out)
	out = append(out, d.seq.Push(ir.Event{
		Kind: ir.EventError,
		Err:  se,
	})...)
	d.done = true
	return out
}
