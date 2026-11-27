package ir

import (
	"slices"
	"strings"
)

// sequencer.go serializes interleaved upstream part events into a canonical
// single-open-part IR stream with contiguous zero-based indices and complete
// part_end snapshots. OpenAI Chat Completions and compatible upstreams can
// interleave argument chunks across multiple parallel tool calls by index,
// whereas Anthropic Messages and OpenAI Responses stream encoders require one
// contiguous block or item lifecycle at a time.
//
// The first opened slot streams immediately without buffering latency so plain
// text and sequential tool calls reach the client chunk by chunk. Concurrent
// slots arriving while an active part remains open buffer their skeleton,
// deltas, and completion state in slot-index order until the active part
// closes. Normal completion flushes unclosed slots cleanly before response_end,
// whereas abnormal error termination marks unfinished tool calls Incomplete
// before emitting the terminal error event.

// Sequencer reorders interleaved upstream slot events into canonical IR stream order.
// Its zero value is ready for use without calling NewSequencer.
type Sequencer struct {
	started    bool             // whether EventResponseStart has been emitted
	terminated bool             // whether EventResponseEnd or EventError was emitted
	nextIndex  int              // next canonical sequential Event.Index to assign
	arrivalSeq int              // monotonic counter preserving tie-break arrival order
	active     *seqSlot         // currently open canonical output part, or nil
	pending    []*seqSlot       // buffered slots waiting for active to close
	openByKey  map[int]*seqSlot // currently unended slots keyed by upstream Index
}

// seqSlot holds the buffered skeleton, deltas, explicit end overrides, and
// accumulated snapshot for one upstream part slot.
type seqSlot struct {
	key            int             // caller-supplied upstream slot key
	seq            int             // monotonic arrival ordinal for tie-breaking
	canonicalIndex int             // assigned output Event.Index once promoted
	base           Part            // initial skeleton Part captured at part_start
	deltas         []Delta         // buffered fragments while waiting in pending
	textBuf        strings.Builder // accumulated Delta.Text fragments
	argsBuf        strings.Builder // accumulated Delta.Arguments fragments
	endPart        *Part           // cloned Part supplied at part_end, retained for late deltas
	finalPart      Part            // complete Part snapshot materialized on finalize
	ended          bool            // true once finalized via part_end, flush, or abort
	incomplete     bool            // true when finalized abnormally during error abort
}

// NewSequencer constructs an empty stateful Sequencer ready to serialize
// interleaved upstream stream events into single-open-part order. Callers must
// allocate a separate Sequencer per upstream stream attempt.
func NewSequencer() *Sequencer {
	return &Sequencer{}
}

// Push ingests one or more upstream Events whose Index fields identify upstream
// part slots and returns canonical sequential Events ready for downstream
// transforms, client encoders, and ValidateStream. Events pushed after a
// terminal EventResponseEnd or EventError produce no further output.
//
// Active-slot starts and non-empty deltas emit immediately, while non-active
// slots buffer in ascending upstream slot order and drain automatically as soon
// as the active slot closes. Pushing EventResponseEnd flushes any remaining open
// slots normally before emitting response_end; pushing EventError closes
// unfinished slots first, marking unended tool calls Incomplete, before emitting
// the terminal error event.
func (s *Sequencer) Push(events ...Event) []Event {
	var out []Event
	for _, ev := range events {
		if s.terminated {
			break
		}
		switch ev.Kind {
		case EventResponseStart:
			if s.started {
				continue
			}
			s.started = true
			meta := &ResponseMeta{}
			if ev.Response != nil {
				cp := *ev.Response
				meta = &cp
			}
			out = append(out, Event{
				Kind:     EventResponseStart,
				Response: meta,
			})

		case EventPartStart:
			if ev.Part == nil {
				continue
			}
			s.ensureStarted(&out)
			s.startSlot(ev.Index, ev.Part.Clone(), &out)

		case EventPartDelta:
			if ev.Delta == nil || (ev.Delta.Text == "" && ev.Delta.Arguments == "") {
				continue
			}
			s.ensureStarted(&out)
			s.deltaSlot(ev.Index, *ev.Delta, &out)

		case EventPartEnd:
			if ev.Part == nil {
				s.ensureStarted(&out)
				s.endSlot(ev.Index, nil, false, &out)
				continue
			}
			s.ensureStarted(&out)
			endCopy := ev.Part.Clone()
			s.endSlot(ev.Index, &endCopy, false, &out)

		case EventUsage:
			if ev.Usage == nil {
				continue
			}
			s.ensureStarted(&out)
			out = append(out, Event{
				Kind:  EventUsage,
				Usage: ev.Usage.Clone(),
			})

		case EventResponseEnd:
			s.ensureStarted(&out)
			s.closeAllOpen(false, &out)
			fin := &Finish{Reason: FinishStop}
			if ev.Finish != nil {
				cp := *ev.Finish
				fin = &cp
			}
			out = append(out, Event{
				Kind:        EventResponseEnd,
				Finish:      fin,
				ServiceTier: ev.ServiceTier,
			})
			s.terminated = true

		case EventError:
			s.ensureStarted(&out)
			s.closeAllOpen(true, &out)
			se := &StreamError{Message: "stream error"}
			if ev.Err != nil {
				cp := *ev.Err
				if strings.TrimSpace(cp.Message) == "" {
					cp.Message = "stream error"
				}
				se = &cp
			}
			out = append(out, Event{
				Kind: EventError,
				Err:  se,
			})
			s.terminated = true
		}
	}
	return out
}

// FlushOpenParts closes the active part and drains all buffered pending parts
// as normal completions while leaving the stream open for later parts, trailing
// usage snapshots, or terminal events. Terminated sequencers return nil.
func (s *Sequencer) FlushOpenParts() []Event {
	if s.terminated || !s.HasOpenParts() {
		return nil
	}
	var out []Event
	s.ensureStarted(&out)
	s.closeAllOpen(false, &out)
	return out
}

// HasOpenParts reports whether an active part is open or any buffered slot is pending.
func (s *Sequencer) HasOpenParts() bool {
	return s.active != nil || len(s.pending) > 0
}

// Started reports whether s has already emitted its initial EventResponseStart event.
func (s *Sequencer) Started() bool {
	return s.started
}

// Terminated reports whether a terminal EventResponseEnd or EventError has been emitted.
func (s *Sequencer) Terminated() bool {
	return s.terminated
}

// ensureStarted synthesizes an empty leading EventResponseStart into out when a
// caller pushes content, usage, or a frame-zero error before an explicit start
// event. Subsequent calls become no-ops once s.started is set.
func (s *Sequencer) ensureStarted(out *[]Event) {
	if s.started {
		return
	}
	s.started = true
	*out = append(*out, Event{
		Kind:     EventResponseStart,
		Response: &ResponseMeta{},
	})
}

// startSlot opens a new slot for key, finalizing any unended prior slot at the
// same key before registering the replacement. When no active part blocks the
// stream, startSlot promotes the new slot immediately; otherwise it queues in pending.
func (s *Sequencer) startSlot(key int, startPart Part, out *[]Event) {
	if s.openByKey == nil {
		s.openByKey = make(map[int]*seqSlot)
	}
	if prev, exists := s.openByKey[key]; exists && !prev.ended {
		s.endSlot(key, nil, false, out)
	}

	s.arrivalSeq++
	slot := &seqSlot{
		key:  key,
		seq:  s.arrivalSeq,
		base: startPart,
	}
	s.openByKey[key] = slot

	if s.active == nil {
		s.promoteSlot(slot, out)
		return
	}
	s.enqueuePending(slot)
}

// deltaSlot appends d to an open or buffered pending slot for key, auto-opening
// a text skeleton when a non-empty text delta arrives without an earlier start.
// Late deltas targeting an already-ended buffered slot extend its retained
// endPart snapshot without discarding previously captured IDs, tokens, or Extra fields.
func (s *Sequencer) deltaSlot(key int, d Delta, out *[]Event) {
	var slot *seqSlot
	if s.openByKey != nil {
		slot = s.openByKey[key]
	}
	if slot == nil {
		// Check buffered pending slots that closed early before promotion so late
		// interleaved chunks still merge into their unreleased part snapshot.
		for _, pendingSlot := range slices.Backward(s.pending) {
			if pendingSlot.key == key {
				slot = pendingSlot
				break
			}
		}
	}
	if slot == nil {
		if d.Text == "" {
			return
		}
		s.startSlot(key, Part{Kind: PartText}, out)
		slot = s.openByKey[key]
	}

	if slot.ended {
		// Seed buffered deltas from any one-shot endPart payload and append the late
		// fragment onto endPart so IDs, tokens, and Extra metadata survive re-finalization.
		slot.mergeLateDelta(d)
		return
	}

	if d.Text != "" {
		_, _ = slot.textBuf.WriteString(d.Text)
	}
	if d.Arguments != "" {
		_, _ = slot.argsBuf.WriteString(d.Arguments)
	}

	if slot == s.active {
		cp := d
		*out = append(*out, Event{
			Kind:  EventPartDelta,
			Index: slot.canonicalIndex,
			Delta: &cp,
		})
		return
	}
	slot.deltas = append(slot.deltas, d)
}

// endSlot finalizes the slot identified by key, retaining a deep copy of endPart
// on the slot, and drains pending slots when the active slot closes. Unopened
// keys receiving a complete endPart synthesize a matching start skeleton first.
func (s *Sequencer) endSlot(key int, endPart *Part, incomplete bool, out *[]Event) {
	var slot *seqSlot
	if s.openByKey != nil {
		slot = s.openByKey[key]
	}
	if slot == nil {
		if endPart == nil {
			return
		}
		s.startSlot(key, skeletonFromCompletePart(*endPart), out)
		slot = s.openByKey[key]
	}

	if endPart != nil {
		cp := endPart.Clone()
		slot.endPart = &cp
	}
	slot.incomplete = slot.incomplete || incomplete
	slot.finalize(slot.endPart, slot.incomplete)
	delete(s.openByKey, key)

	if slot == s.active {
		snap := slot.finalPart.Clone()
		*out = append(*out, Event{
			Kind:  EventPartEnd,
			Index: slot.canonicalIndex,
			Part:  &snap,
		})
		s.active = nil
		s.drainPending(out)
	}
}

// closeAllOpen finalizes the active slot and any unended pending slots, marking
// unfinished tool calls Incomplete when incomplete is true. All buffered slot
// events then drain into out in canonical index order.
func (s *Sequencer) closeAllOpen(incomplete bool, out *[]Event) {
	if s.active != nil && !s.active.ended {
		s.active.incomplete = incomplete
		s.active.finalize(s.active.endPart, incomplete)
		delete(s.openByKey, s.active.key)
		snap := s.active.finalPart.Clone()
		*out = append(*out, Event{
			Kind:  EventPartEnd,
			Index: s.active.canonicalIndex,
			Part:  &snap,
		})
		s.active = nil
	}
	for _, slot := range s.pending {
		if !slot.ended {
			slot.incomplete = incomplete
			slot.finalize(slot.endPart, incomplete)
			delete(s.openByKey, slot.key)
		}
	}
	s.drainPending(out)
}

// drainPending repeatedly promotes the head of s.pending into s.active while no
// active part blocks output. Slots that already closed while buffered emit
// their complete lifecycle immediately before the loop advances.
func (s *Sequencer) drainPending(out *[]Event) {
	for s.active == nil && len(s.pending) > 0 {
		next := s.pending[0]
		s.pending = s.pending[1:]
		s.promoteSlot(next, out)
	}
}

// promoteSlot assigns the next canonical output index to slot and appends its
// start skeleton and buffered deltas to out. Already-finalized slots also emit
// EventPartEnd immediately and clear s.active.
func (s *Sequencer) promoteSlot(slot *seqSlot, out *[]Event) {
	slot.canonicalIndex = s.nextIndex
	s.nextIndex++
	s.active = slot

	startCopy := slot.base.Clone()
	*out = append(*out, Event{
		Kind:  EventPartStart,
		Index: slot.canonicalIndex,
		Part:  &startCopy,
	})
	for _, d := range slot.deltas {
		dCopy := d
		*out = append(*out, Event{
			Kind:  EventPartDelta,
			Index: slot.canonicalIndex,
			Delta: &dCopy,
		})
	}
	slot.deltas = nil

	if slot.ended {
		endCopy := slot.finalPart.Clone()
		*out = append(*out, Event{
			Kind:  EventPartEnd,
			Index: slot.canonicalIndex,
			Part:  &endCopy,
		})
		s.active = nil
	}
}

// enqueuePending inserts slot into s.pending sorted by upstream slot key first
// and arrival ordinal second. Reused keys that arrive later stay ordered behind
// earlier occurrences while distinct concurrent indices release in key order.
func (s *Sequencer) enqueuePending(slot *seqSlot) {
	s.pending = append(s.pending, slot)
	slices.SortStableFunc(s.pending, func(a, b *seqSlot) int {
		if a.key != b.key {
			if a.key < b.key {
				return -1
			}
			return 1
		}
		if a.seq < b.seq {
			return -1
		}
		if a.seq > b.seq {
			return 1
		}
		return 0
	})
}

// mergeLateDelta incorporates a late interleaved Delta d into a buffered pending
// slot that already received EventPartEnd before promotion. Empty deltas on the
// active branch are ignored without altering the buffered snapshot.
//
// One-shot endPart payloads that arrived without prior deltas first seed their
// initial text, reasoning, or tool-argument payload into deltas so the promoted
// event sequence and finalPart match under ValidateStream while retaining IDs,
// continuity tokens, cache marks, and Extra metadata.
func (slot *seqSlot) mergeLateDelta(d Delta) {
	switch slot.base.Kind {
	case PartText, PartRefusal:
		if d.Text == "" {
			return
		}
		if slot.endPart != nil && slot.endPart.Text != "" && slot.base.Text == "" && slot.textBuf.Len() == 0 {
			_, _ = slot.textBuf.WriteString(slot.endPart.Text)
			slot.deltas = append(slot.deltas, Delta{Text: slot.endPart.Text})
		}
		_, _ = slot.textBuf.WriteString(d.Text)
		slot.deltas = append(slot.deltas, Delta{Text: d.Text})
		if slot.endPart != nil && slot.endPart.Text != "" {
			slot.endPart.Text += d.Text
		}

	case PartReasoning:
		if d.Text == "" {
			return
		}
		var baseSegs string
		if slot.base.Reasoning != nil {
			baseSegs = strings.Join(slot.base.Reasoning.Segments, "")
		}
		if slot.endPart != nil && slot.endPart.Reasoning != nil {
			endText := strings.Join(slot.endPart.Reasoning.Segments, "")
			if endText != "" && baseSegs == "" && slot.textBuf.Len() == 0 {
				_, _ = slot.textBuf.WriteString(endText)
				slot.deltas = append(slot.deltas, Delta{Text: endText})
			}
			if len(slot.endPart.Reasoning.Segments) > 0 {
				last := len(slot.endPart.Reasoning.Segments) - 1
				slot.endPart.Reasoning.Segments[last] += d.Text
			}
		}
		_, _ = slot.textBuf.WriteString(d.Text)
		slot.deltas = append(slot.deltas, Delta{Text: d.Text})

	case PartToolCall:
		if d.Arguments == "" {
			return
		}
		var baseArgs string
		if slot.base.Call != nil {
			baseArgs = slot.base.Call.Arguments
		}
		if slot.endPart != nil && slot.endPart.Call != nil {
			if slot.endPart.Call.Arguments != "" && baseArgs == "" && slot.argsBuf.Len() == 0 {
				_, _ = slot.argsBuf.WriteString(slot.endPart.Call.Arguments)
				slot.deltas = append(slot.deltas, Delta{Arguments: slot.endPart.Call.Arguments})
			}
			if slot.endPart.Call.Arguments != "" {
				slot.endPart.Call.Arguments += d.Arguments
			}
		}
		_, _ = slot.argsBuf.WriteString(d.Arguments)
		slot.deltas = append(slot.deltas, Delta{Arguments: d.Arguments})
	}

	slot.finalize(slot.endPart, slot.incomplete)
}

// finalize merges slot.base skeleton fields, accumulated deltas, and optional
// endPart overrides into slot.finalPart. Setting incomplete marks unfinished
// tool calls Incomplete when an upstream stream aborts before normal closure.
func (slot *seqSlot) finalize(endPart *Part, incomplete bool) {
	snap := slot.base.Clone()
	if endPart != nil {
		if endPart.ID != "" {
			snap.ID = endPart.ID
		}
		if len(endPart.Annotations) > 0 {
			snap.Annotations = endPart.Clone().Annotations
		}
		if endPart.Token != nil {
			snap.Token = cloneReplayToken(endPart.Token)
		}
		if endPart.Cache != nil {
			snap.Cache = cloneCacheMark(endPart.Cache)
		}
		if !endPart.Extra.IsZero() {
			snap.Extra = endPart.Extra.Clone()
		}
	}

	switch snap.Kind {
	case PartText, PartRefusal:
		accumulated := slot.base.Text + slot.textBuf.String()
		if endPart != nil && endPart.Text != "" {
			snap.Text = endPart.Text
		} else {
			snap.Text = accumulated
		}

	case PartReasoning:
		if snap.Reasoning == nil {
			snap.Reasoning = &Reasoning{}
		}
		if endPart != nil && endPart.Reasoning != nil {
			snap.Reasoning.Summary = snap.Reasoning.Summary || endPart.Reasoning.Summary
			snap.Reasoning.Redacted = snap.Reasoning.Redacted || endPart.Reasoning.Redacted
			if endPart.Reasoning.Token != nil {
				snap.Reasoning.Token = cloneReplayToken(endPart.Reasoning.Token)
			}
			if len(endPart.Reasoning.Segments) > 0 {
				snap.Reasoning.Segments = cloneNonEmpty(endPart.Reasoning.Segments)
			}
		}
		if slot.textBuf.Len() > 0 {
			if len(snap.Reasoning.Segments) == 0 {
				snap.Reasoning.Segments = []string{slot.textBuf.String()}
			} else if endPart == nil || endPart.Reasoning == nil || len(endPart.Reasoning.Segments) == 0 {
				last := len(snap.Reasoning.Segments) - 1
				snap.Reasoning.Segments[last] += slot.textBuf.String()
			}
		}

	case PartToolCall:
		if snap.Call == nil {
			snap.Call = &ToolCall{}
		}
		var baseArgs string
		if slot.base.Call != nil {
			baseArgs = slot.base.Call.Arguments
		}
		accumulated := baseArgs + slot.argsBuf.String()
		if endPart != nil && endPart.Call != nil {
			if endPart.Call.CallID != "" {
				snap.Call.CallID = endPart.Call.CallID
			}
			if isNonZeroToolRef(endPart.Call.Tool) {
				snap.Call.Tool = endPart.Call.Tool
			}
			if endPart.Call.Token != nil {
				snap.Call.Token = cloneReplayToken(endPart.Call.Token)
			}
			snap.Call.Unsigned = snap.Call.Unsigned || endPart.Call.Unsigned
			snap.Call.Incomplete = snap.Call.Incomplete || endPart.Call.Incomplete
			if endPart.Call.Arguments != "" {
				snap.Call.Arguments = endPart.Call.Arguments
			} else {
				snap.Call.Arguments = accumulated
			}
		} else {
			snap.Call.Arguments = accumulated
		}
		if incomplete {
			snap.Call.Incomplete = true
		}

	case PartMedia:
		if endPart != nil && endPart.Media != nil {
			med := *endPart.Media
			snap.Media = &med
		}

	case PartOpaque:
		if endPart != nil && endPart.Opaque != nil {
			snap.Opaque = cloneOpaqueItem(endPart.Opaque)
		}
	}

	slot.finalPart = snap
	slot.ended = true
}

// skeletonFromCompletePart builds a minimal EventPartStart skeleton for one-shot
// complete parts arriving without a prior part_start. Streamed text, reasoning
// segments, tool arguments, and continuity tokens remain on the part_end snapshot.
func skeletonFromCompletePart(full Part) Part {
	skel := Part{
		Kind: full.Kind,
		ID:   full.ID,
	}
	switch full.Kind {
	case PartReasoning:
		rs := &Reasoning{}
		if full.Reasoning != nil {
			rs.Summary = full.Reasoning.Summary
			rs.Redacted = full.Reasoning.Redacted
		}
		skel.Reasoning = rs
	case PartToolCall:
		call := &ToolCall{}
		if full.Call != nil {
			call.CallID = full.Call.CallID
			call.Tool = full.Call.Tool
			call.Unsigned = full.Call.Unsigned
		}
		skel.Call = call
	case PartMedia:
		if full.Media != nil {
			med := *full.Media
			skel.Media = &med
		}
	case PartOpaque:
		skel.Opaque = cloneOpaqueItem(full.Opaque)
	}
	return skel
}
