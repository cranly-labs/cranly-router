package generatecontent

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream_decode.go incrementally decodes Google Gemini GenerateContent SSE
// frames into canonical sequential IR Event streams. Gemini SSE frames carry
// full GenerateContentResponse envelopes rather than start/delta/stop block
// markers, and compatible relays may send cumulative text prefixes, batch-replay
// the immediately preceding complete functionCall frame (2435ab3f5), attach late
// thoughtSignature values to already-closed explicit-ID calls (4b05d79a5), emit
// unnamed parallel calls to the same function (d91aacd8e), or send a sparse
// usageMetadata tail frame within five seconds after finishReason.
//
// StreamDecoder reconciles visible text and thought channels through
// computeGeminiTextDelta while splitting blocks whenever non-empty signatures or
// vendor Extra fields change, tracks adjacent pure-tool batch snapshots and
// closed explicit IDs without synthesizing fallback CallIDs, merges non-null
// usageMetadata fields across frames, and defers EventResponseEnd until
// Close(nil) so post-finish usage tail frames emit valid EventUsage updates.

const (
	// openNone through openTool classify the single active open part tracked by StreamDecoder.
	openNone    openChannelKind = iota // no part is currently open
	openText                           // open PartText accumulating visible text deltas
	openThought                        // open PartReasoning accumulating thought deltas
	openTool                           // open PartToolCall accumulating arguments or signatures
)

// openChannelKind identifies the currently open streaming part inside StreamDecoder.
type openChannelKind uint8

// LateToolSignature pairs an already-closed sequential IR part index and
// explicit upstream functionCall.id with a late thoughtSignature delivered on a
// subsequent SSE frame.
type LateToolSignature struct {
	PartIndex  int    // zero-based sequential IR part index emitted earlier
	UpstreamID string // explicit non-empty upstream functionCall.id
	Signature  string // non-empty revised thoughtSignature token
}

// toolBatchItem captures normalized identity, argument, and signature strings
// from one complete functionCall part for adjacent-frame replay trimming.
type toolBatchItem struct {
	id        string // wire functionCall.id ("" when omitted)
	name      string // invoked FunctionDeclaration name
	args      string // compacted JSON object arguments
	signature string // wire thoughtSignature on this part
}

// closedToolInfo records the sequential IR part index and latest signature of an
// already-closed explicit-ID functionCall part within the current stream.
type closedToolInfo struct {
	partIndex int    // zero-based sequential IR part index
	signature string // latest non-empty thoughtSignature observed
}

// toolRecord stores an active or buffered Gemini functionCall's accumulated
// arguments, signature, and Extra metadata before closure.
type toolRecord struct {
	rawID       string                    // wire functionCall.id ("" when omitted upstream)
	callID      string                    // IR CallID ("" when omitted upstream)
	name        string                    // invoked FunctionDeclaration name
	args        string                    // accumulated JSON object or string fragments
	sig         string                    // latest non-empty thoughtSignature
	partUnknown map[string]jsontext.Value // preserved outer Part.Extra members
	callUnknown map[string]jsontext.Value // preserved nested functionCall Extra members
}

// StreamDecoder decodes upstream Google Gemini GenerateContent SSE frames into
// sequential canonical IR events enforcing all seven invariants of section 3.5.
type StreamDecoder struct {
	seq           ir.Sequencer              // enforces single-open-part sequential event emission
	usage         streamUsageTracker        // merges sparse non-null usageMetadata observations
	grounding     groundingState            // captures first non-empty grounding queries and chunks
	started       bool                      // whether EventResponseStart has been emitted
	terminated    bool                      // whether EventResponseEnd or EventError was emitted
	sawFinish     bool                      // whether candidates[0].finishReason arrived
	finishReason  string                    // recorded terminal Gemini FinishReason enum
	sawToolCall   bool                      // whether any PartToolCall was emitted in this stream
	nextSlot      int                       // next sequential part index allocated for Sequencer
	activeSlot    int                       // sequential part index of currently open part
	openKind      openChannelKind           // kind of the currently open part
	seenText      []byte                    // cumulative UTF-8 bytes observed across open PartText deltas
	seenThinking  []byte                    // cumulative UTF-8 bytes observed across open PartReasoning deltas
	openSig       string                    // pending thoughtSignature for open text or thought part
	openUnknown   map[string]jsontext.Value // pending Extra members for open text or thought part
	activeTool    *toolRecord               // currently open tool call awaiting completion
	prevToolBatch []toolBatchItem           // complete pure-tool batch from immediately preceding frame
	closedByID    map[string]closedToolInfo // explicit functionCall.id -> closed part metadata
	lateRevisions []LateToolSignature       // pending late signature revisions for convert CallMemory
}

// NewStreamDecoder allocates a fresh stateful StreamDecoder for one upstream
// Gemini GenerateContent SSE stream attempt. Callers must construct an
// independent decoder per stream so Sequencer slots and usage counters stay isolated.
func NewStreamDecoder() *StreamDecoder {
	return &StreamDecoder{}
}

// DrainLateToolSignatures returns and clears any late thoughtSignature revisions
// observed on already-closed explicit-ID tool calls since the previous drain.
// Empty queues return nil without allocating a slice, and callers must drain
// after both Decode and Close.
func (d *StreamDecoder) DrainLateToolSignatures() []LateToolSignature {
	if len(d.lateRevisions) == 0 {
		return nil
	}
	out := slices.Clone(d.lateRevisions)
	d.lateRevisions = d.lateRevisions[:0]
	return out
}

// Decode parses one upstream Gemini SSE frame into zero or more sequential IR
// events. Blank keepalives, compatible [DONE] sentinels, or frames arriving
// after terminal error events return (nil, nil), whereas malformed JSON,
// post-finish content parts, or invalid usage counters return an error.
//
// Observing candidates[0].finishReason records the completion reason without
// immediately emitting EventResponseEnd so trailing usageMetadata frames during
// the upstream five-second tail window still emit valid EventUsage snapshots
// before Close(nil) finalizes the stream.
func (d *StreamDecoder) Decode(frame sse.Frame) ([]ir.Event, error) {
	if d.terminated {
		return nil, nil
	}
	trimmed := bytes.TrimSpace(frame.Data)
	if len(trimmed) == 0 {
		return nil, nil
	}
	// Compatible Gemini proxies occasionally append an OpenAI-style [DONE]
	// sentinel; treat it as an explicit end-of-stream marker when frames preceded it.
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		if d.started && !d.sawFinish {
			d.sawFinish = true
			d.finishReason = "STOP"
		}
		return nil, nil
	}

	var env wireResponse
	if err := unmarshalJSONObject(trimmed, &env); err != nil {
		return nil, fmt.Errorf("decode gemini sse frame: %w", err)
	}

	var events []ir.Event
	if se := decodeStreamError(env.Error); se != nil {
		d.ensureStarted(&events, env.ResponseID, env.ModelVersion)
		d.abortOpenPartForError(&events)
		events = append(events, d.seq.Push(ir.Event{
			Kind: ir.EventError,
			Err:  se,
		})...)
		d.terminated = true
		return events, nil
	}

	usageSnap, hasUsage, err := d.usage.observe(trimmed, env.UsageMetadata)
	if err != nil {
		return nil, fmt.Errorf("decode gemini sse usage: %w", err)
	}

	if len(env.Candidates) == 0 && !hasUsage {
		// Empty keepalive envelopes carrying neither candidates nor usageMetadata
		// start the response header only when responseId or modelVersion is present.
		if env.ResponseID != "" || env.ModelVersion != "" {
			d.ensureStarted(&events, env.ResponseID, env.ModelVersion)
		}
		return events, nil
	}

	d.ensureStarted(&events, env.ResponseID, env.ModelVersion)

	if len(env.Candidates) > 0 {
		cand := env.Candidates[0]
		if cand.Content != nil {
			role := strings.TrimSpace(cand.Content.Role)
			if role != "" && role != geminiRoleModel {
				return nil, fmt.Errorf("decode gemini sse frame: candidate content.role = %q, want %q", cand.Content.Role, geminiRoleModel)
			}
			if d.sawFinish && len(cand.Content.Parts) > 0 {
				return nil, errors.New("decode gemini sse frame: candidate content parts arrived after finishReason")
			}
			if err := d.decodeCandidateParts(&events, cand.Content.Parts); err != nil {
				return nil, err
			}
		}
		d.grounding.observe(cand.GroundingMetadata)
		if isTerminalFinishReason(cand.FinishReason) {
			d.sawFinish = true
			d.finishReason = cand.FinishReason
		}
	}

	if hasUsage {
		events = append(events, d.seq.Push(ir.Event{
			Kind:  ir.EventUsage,
			Usage: usageSnap,
		})...)
	}
	return events, nil
}

// Close finalizes the stream when the upstream SSE reader reaches EOF
// (cause==nil) or aborts with a transport error (cause!=nil). Calling Close on
// an already-terminated StreamDecoder returns nil without emitting further events.
//
// When finishReason was recorded and cause is nil, Close flushes any pending
// grounding footer, validates and closes the open part, and emits
// EventResponseEnd. Missing finishReason at EOF, non-nil cause errors, or
// malformed streamed tool arguments abort open parts (marking open tool calls
// Incomplete=true) and emit a terminal EventError.
func (d *StreamDecoder) Close(cause error) []ir.Event {
	if d.terminated {
		return nil
	}
	d.terminated = true

	var events []ir.Event
	if cause != nil || !d.started || !d.sawFinish {
		d.ensureStarted(&events, "", "")
		d.abortOpenPartForError(&events)
		events = append(events, d.seq.Push(ir.Event{
			Kind: ir.EventError,
			Err:  truncatedStreamError(cause),
		})...)
		return events
	}

	if err := d.flushNormalCompletion(&events); err != nil {
		d.abortOpenPartForError(&events)
		events = append(events, d.seq.Push(ir.Event{
			Kind: ir.EventError,
			Err: &ir.StreamError{
				Type:    "api_error",
				Code:    "invalid_tool_arguments",
				Message: err.Error(),
			},
		})...)
		return events
	}

	fin := decodeFinishReason(d.finishReason, d.sawToolCall)
	events = append(events, d.seq.Push(ir.Event{
		Kind:   ir.EventResponseEnd,
		Finish: &fin,
	})...)
	return events
}

// ensureStarted emits EventResponseStart once through d.seq before any part,
// usage, or terminal event is pushed. Subsequent calls on an already-started
// decoder return immediately without emitting duplicate start events.
func (d *StreamDecoder) ensureStarted(events *[]ir.Event, respID, model string) {
	if d.started {
		return
	}
	d.started = true
	*events = append(*events, d.seq.Push(ir.Event{
		Kind: ir.EventResponseStart,
		Response: &ir.ResponseMeta{
			ID:    respID,
			Model: model,
		},
	})...)
}

// allocatePartSlot advances d.nextSlot, updates d.activeSlot, and returns the
// newly reserved zero-based sequential slot index. Callers must immediately
// push an EventPartStart at the returned slot to keep d.seq contiguous.
func (d *StreamDecoder) allocatePartSlot() int {
	slot := d.nextSlot
	d.nextSlot++
	d.activeSlot = slot
	return slot
}

// pushAtomicPart allocates a fresh Sequencer slot and pushes a paired
// EventPartStart (using skeleton) and EventPartEnd (using complete) onto events.
// Callers must close any currently open part before invoking pushAtomicPart.
func (d *StreamDecoder) pushAtomicPart(events *[]ir.Event, skeleton, complete ir.Part) {
	slot := d.allocatePartSlot()
	*events = append(*events, d.seq.Push(
		ir.Event{Kind: ir.EventPartStart, Index: slot, Part: &skeleton},
		ir.Event{Kind: ir.EventPartEnd, Index: slot, Part: &complete},
	)...)
}

// decodeCandidateParts decodes candidates[0].content.parts for one SSE frame
// into sequential IR events appended to events. Empty part slices return nil
// without clearing d.prevToolBatch so sparse usage frames preserve batch state.
func (d *StreamDecoder) decodeCandidateParts(events *[]ir.Event, rawParts []jsontext.Value) error {
	if len(rawParts) == 0 {
		return nil
	}

	parsed := make([]wirePart, len(rawParts))
	rawTrimmed := make([][]byte, len(rawParts))
	for i, rp := range rawParts {
		trimmed := bytes.TrimSpace(rp)
		rawTrimmed[i] = trimmed
		if err := unmarshalJSONObject(trimmed, &parsed[i]); err != nil {
			return fmt.Errorf("candidates[0].content.parts[%d]: %w", i, err)
		}
	}

	startIdx := d.trimAdjacentToolReplay(parsed)
	toolSeenInFrame := false

	for i := startIdx; i < len(parsed); i++ {
		if err := d.processSingleStreamPart(events, parsed[i], rawTrimmed[i], &toolSeenInFrame); err != nil {
			return fmt.Errorf("candidates[0].content.parts[%d]: %w", i, err)
		}
	}
	return nil
}

// trimAdjacentToolReplay compares parsed against d.prevToolBatch and returns
// the starting index after any replayed prefix of complete pure-tool calls.
//
// Compatible Gemini relays occasionally re-emit the previous frame's complete
// functionCall array as a prefix of the next frame (2435ab3f5). Any frame
// containing text, thought, media, opaque items, or unfinished string-fragment
// arguments clears d.prevToolBatch and returns 0 so legitimate repeated tool
// calls after intervening model prose are never suppressed.
func (d *StreamDecoder) trimAdjacentToolReplay(parsed []wirePart) int {
	batch := make([]toolBatchItem, 0, len(parsed))
	for _, wp := range parsed {
		if wp.FunctionCall == nil || wp.Text != nil || wp.InlineData != nil || wp.FileData != nil || wp.FunctionResponse != nil || wp.Thought {
			d.prevToolBatch = nil
			return 0
		}
		argStr, structured, err := extractStreamCallArgs(wp.FunctionCall.Args)
		if err != nil || !structured || !isValidSingleJSONObject([]byte(argStr)) {
			d.prevToolBatch = nil
			return 0
		}
		batch = append(batch, toolBatchItem{
			id:        wp.FunctionCall.ID,
			name:      wp.FunctionCall.Name,
			args:      argStr,
			signature: wp.ThoughtSignature,
		})
	}

	previous := d.prevToolBatch
	d.prevToolBatch = batch
	if len(previous) > 0 && len(batch) >= len(previous) && slices.EqualFunc(previous, batch[:len(previous)], func(a, b toolBatchItem) bool {
		return a.id == b.id && a.name == b.name && a.args == b.args && (a.signature == b.signature || b.signature == "")
	}) {
		return len(previous)
	}
	return 0
}

// processSingleStreamPart advances d's open part state for one validated
// wirePart and appends emitted IR events to events. Parts carrying
// functionResponse, thought=true on a functionCall, or multiple content union
// branches return an error, while *toolSeenInFrame turns true on the first call.
func (d *StreamDecoder) processSingleStreamPart(
	events *[]ir.Event,
	wp wirePart,
	trimmed []byte,
	toolSeenInFrame *bool,
) error {
	branches := 0
	if wp.Text != nil {
		branches++
	}
	if wp.InlineData != nil {
		branches++
	}
	if wp.FileData != nil {
		branches++
	}
	if wp.FunctionCall != nil {
		branches++
	}
	if wp.FunctionResponse != nil {
		return errors.New("functionResponse is not allowed in model stream output")
	}
	if branches > 1 {
		return errors.New("stream part must not set multiple content union branches")
	}

	if branches == 0 {
		if wp.Thought {
			empty := ""
			wp.Text = &empty
			return d.processStreamThought(events, wp)
		}
		if wp.ThoughtSignature != "" && len(wp.Unknown) == 0 {
			// Attach bare trailing signature-only frames onto an open unsigned text
			// slot when present; otherwise emit a signature-only thought slot so
			// StreamDecoder matches DecodeResponse under Law S2.
			empty := ""
			wp.Text = &empty
			if d.openKind == openText && d.openSig == "" && len(d.openUnknown) == 0 {
				return d.processStreamText(events, wp)
			}
			wp.Thought = true
			return d.processStreamThought(events, wp)
		}
		if err := d.closeCurrentOpenPart(events); err != nil {
			return err
		}
		p, _, err := decodeOpaquePart(trimmed, wp.Unknown)
		if err != nil {
			return err
		}
		d.pushAtomicPart(events, p, p)
		return nil
	}

	switch {
	case wp.Text != nil && wp.Thought:
		return d.processStreamThought(events, wp)

	case wp.Text != nil:
		return d.processStreamText(events, wp)

	case wp.FunctionCall != nil:
		if wp.Thought {
			return errors.New("functionCall part cannot set thought=true")
		}
		err := d.processStreamFunctionCall(events, wp, *toolSeenInFrame)
		*toolSeenInFrame = true
		return err

	case wp.InlineData != nil:
		if err := d.closeCurrentOpenPart(events); err != nil {
			return err
		}
		p, err := decodeInlineDataPart(wp)
		if err != nil {
			return err
		}
		skeleton := ir.Part{
			Kind: ir.PartMedia,
			Media: &ir.Media{
				Type:   p.Media.Type,
				MIME:   p.Media.MIME,
				Source: p.Media.Source,
			},
		}
		d.pushAtomicPart(events, skeleton, p)
		return nil

	case wp.FileData != nil:
		if err := d.closeCurrentOpenPart(events); err != nil {
			return err
		}
		p, err := decodeFileDataPart(wp)
		if err != nil {
			return err
		}
		skeleton := ir.Part{
			Kind: ir.PartMedia,
			Media: &ir.Media{
				Type:   p.Media.Type,
				MIME:   p.Media.MIME,
				Source: p.Media.Source,
			},
		}
		d.pushAtomicPart(events, skeleton, p)
		return nil

	default:
		return errors.New("unreachable stream part branch")
	}
}

// processStreamThought appends incremental thought deltas onto an open
// PartReasoning part or closes the active part before opening a fresh
// PartReasoning slot. When d.openSig is already populated and wp carries a
// distinct signature or non-cumulative text, splitting preserves every signature
// in parity with buffered DecodeResponse (Law S2).
func (d *StreamDecoder) processStreamThought(events *[]ir.Event, wp wirePart) error {
	mustSplit := d.openKind != openThought ||
		len(d.openUnknown) > 0 ||
		len(wp.Unknown) > 0 ||
		(d.openSig != "" && ((wp.ThoughtSignature != "" && wp.ThoughtSignature != d.openSig) || !isCumulativeTextContinuation(d.seenThinking, *wp.Text)))

	if mustSplit {
		if err := d.closeCurrentOpenPart(events); err != nil {
			return err
		}
		d.openKind = openThought
		d.seenThinking = d.seenThinking[:0]
		d.openSig = ""
		d.openUnknown = nil
		slot := d.allocatePartSlot()
		startPart := ir.Part{
			Kind:      ir.PartReasoning,
			Reasoning: &ir.Reasoning{Summary: true},
		}
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartStart,
			Index: slot,
			Part:  &startPart,
		})...)
	}

	var delta string
	delta, d.seenThinking = computeGeminiTextDelta(d.seenThinking, *wp.Text)
	if wp.ThoughtSignature != "" {
		d.openSig = wp.ThoughtSignature
	}
	if len(wp.Unknown) > 0 && len(d.openUnknown) == 0 {
		d.openUnknown = cloneValueMap(wp.Unknown)
	}
	if delta != "" {
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: d.activeSlot,
			Delta: &ir.Delta{Text: delta},
		})...)
	}
	return nil
}

// processStreamText appends incremental visible text deltas onto an open
// PartText part or closes the active part before opening a fresh PartText slot.
// When d.openSig is already populated and wp carries a distinct signature or
// non-cumulative text, splitting preserves per-block Part.Token signatures under Law S2.
func (d *StreamDecoder) processStreamText(events *[]ir.Event, wp wirePart) error {
	mustSplit := d.openKind != openText ||
		len(d.openUnknown) > 0 ||
		len(wp.Unknown) > 0 ||
		(d.openSig != "" && ((wp.ThoughtSignature != "" && wp.ThoughtSignature != d.openSig) || !isCumulativeTextContinuation(d.seenText, *wp.Text)))

	if mustSplit {
		if err := d.closeCurrentOpenPart(events); err != nil {
			return err
		}
		d.openKind = openText
		d.seenText = d.seenText[:0]
		d.openSig = ""
		d.openUnknown = nil
		slot := d.allocatePartSlot()
		startPart := ir.Part{Kind: ir.PartText}
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartStart,
			Index: slot,
			Part:  &startPart,
		})...)
	}

	var delta string
	delta, d.seenText = computeGeminiTextDelta(d.seenText, *wp.Text)
	if wp.ThoughtSignature != "" {
		d.openSig = wp.ThoughtSignature
	}
	if len(wp.Unknown) > 0 && len(d.openUnknown) == 0 {
		d.openUnknown = cloneValueMap(wp.Unknown)
	}
	if delta != "" {
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: d.activeSlot,
			Delta: &ir.Delta{Text: delta},
		})...)
	}
	return nil
}

// isCumulativeTextContinuation reports whether incoming continues seen under
// cumulative prefix streaming after stripping a single trailing NUL byte. Empty
// incoming chunks return true, whereas empty seen buffers with non-empty
// incoming chunks return false.
func isCumulativeTextContinuation(seen []byte, incoming string) bool {
	cleaned := strings.TrimSuffix(incoming, "\u0000")
	if cleaned == "" {
		return true
	}
	if len(seen) == 0 {
		return false
	}
	return stringHasBytesPrefix(cleaned, seen) || bytesHaveStringPrefix(seen, cleaned)
}

// processStreamFunctionCall reconciles incoming functionCall parts against
// d.closedByID and d.activeTool while leaving omitted wire IDs empty on IR parts.
//
// Replayed explicit functionCall.id entries that already closed suppress
// duplicate tool emission and record any newly arrived thoughtSignature in
// d.lateRevisions (4b05d79a5). Active calls accept string-fragment or snapshot
// continuations across frames until their JSON object completes, whereas second
// unnamed calls within the same frame always open distinct invocations (d91aacd8e).
func (d *StreamDecoder) processStreamFunctionCall(
	events *[]ir.Event,
	wp wirePart,
	toolSeenInFrame bool,
) error {
	fc := wp.FunctionCall
	if strings.TrimSpace(fc.Name) == "" {
		return errors.New("functionCall requires non-empty name")
	}
	argChunk, structured, err := extractStreamCallArgs(fc.Args)
	if err != nil {
		return err
	}

	if fc.ID != "" && d.updateClosedTool(fc.ID, wp.ThoughtSignature) {
		return nil
	}

	if d.openKind == openTool && d.activeTool != nil {
		if d.continuesActiveTool(fc, argChunk, structured, wp.ThoughtSignature, toolSeenInFrame) {
			delta := appendActiveToolArgs(d.activeTool, argChunk, structured, wp.ThoughtSignature)
			if delta != "" {
				*events = append(*events, d.seq.Push(ir.Event{
					Kind:  ir.EventPartDelta,
					Index: d.activeSlot,
					Delta: &ir.Delta{Arguments: delta},
				})...)
			}
			return nil
		}
	}

	if err := d.closeCurrentOpenPart(events); err != nil {
		return err
	}

	rec := &toolRecord{
		rawID:       fc.ID,
		callID:      fc.ID,
		name:        fc.Name,
		partUnknown: cloneValueMap(wp.Unknown),
		callUnknown: cloneValueMap(fc.Unknown),
	}
	initialDelta := appendActiveToolArgs(rec, argChunk, structured, wp.ThoughtSignature)

	d.openKind = openTool
	d.sawToolCall = true
	d.activeTool = rec

	slot := d.allocatePartSlot()
	startPart := ir.Part{
		Kind: ir.PartToolCall,
		Call: &ir.ToolCall{
			CallID: fc.ID,
			Tool:   ir.ToolRef{Kind: ir.ToolFunction, Name: fc.Name},
		},
	}
	*events = append(*events, d.seq.Push(ir.Event{
		Kind:  ir.EventPartStart,
		Index: slot,
		Part:  &startPart,
	})...)
	if initialDelta != "" {
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartDelta,
			Index: slot,
			Delta: &ir.Delta{Arguments: initialDelta},
		})...)
	}
	return nil
}

// updateClosedTool reports whether rawID already closed earlier in this stream.
// When sig supplies a new non-empty thoughtSignature, it updates d.closedByID
// and appends a LateToolSignature entry to d.lateRevisions for CallMemory refresh.
func (d *StreamDecoder) updateClosedTool(rawID, sig string) bool {
	if d.closedByID == nil {
		return false
	}
	prev, ok := d.closedByID[rawID]
	if !ok {
		return false
	}
	if sig != "" && sig != prev.signature {
		prev.signature = sig
		d.closedByID[rawID] = prev
		d.lateRevisions = append(d.lateRevisions, LateToolSignature{
			PartIndex:  prev.partIndex,
			UpstreamID: rawID,
			Signature:  sig,
		})
	}
	return true
}

// continuesActiveTool reports whether fc continues d.activeTool rather than
// closing the open slot and starting a distinct tool invocation.
//
// Non-empty functionCall.id values are authoritative whenever either call
// carries an explicit ID, requiring both ID and function name to match. For
// unnamed calls, toolSeenInFrame keeps second invocations within the same SSE
// frame separate (d91aacd8e), conflicting signatures split calls, and
// already-complete JSON argument objects reject further chunks across frames.
func (d *StreamDecoder) continuesActiveTool(
	fc *wireFunctionCall,
	argChunk string,
	structured bool,
	incomingSig string,
	toolSeenInFrame bool,
) bool {
	act := d.activeTool
	if fc.ID != "" || act.rawID != "" {
		return fc.ID != "" && fc.ID == act.rawID && fc.Name == act.name
	}
	if toolSeenInFrame || fc.Name != act.name {
		return false
	}
	if act.sig != "" && incomingSig != "" && act.sig != incomingSig {
		return false
	}
	if incomingSig != "" && argChunk == "" {
		return true
	}
	if act.args == argChunk {
		return true
	}
	if act.args == "" || isValidSingleJSONObject([]byte(act.args)) {
		return false
	}
	return !structured || strings.HasPrefix(argChunk, act.args)
}

// appendActiveToolArgs merges incomingChunk and incomingSig into rec and returns
// only the newly added JSON argument suffix for EventPartDelta emission.
//
// Once rec.args already forms a valid single JSON object, later argument chunks
// are ignored while still updating rec.sig so explicit-ID signature replays do
// not corrupt completed arguments. Prefix-overlapping chunks append only when
// the combined string stays a valid JSON object or incomplete JSON prefix.
func appendActiveToolArgs(
	rec *toolRecord,
	incomingChunk string,
	_ bool,
	incomingSig string,
) string {
	if incomingSig != "" {
		rec.sig = incomingSig
	}
	if isValidSingleJSONObject([]byte(rec.args)) || incomingChunk == "" || incomingChunk == rec.args {
		return ""
	}
	if rec.args == "" {
		rec.args = incomingChunk
		return incomingChunk
	}
	if delta, ok := strings.CutPrefix(incomingChunk, rec.args); ok {
		rec.args = incomingChunk
		return delta
	}
	combined := rec.args + incomingChunk
	if strings.HasPrefix(rec.args, incomingChunk) {
		if !canContinueJSONObjectPrefix(combined) {
			return ""
		}
	}
	rec.args = combined
	return incomingChunk
}

// canContinueJSONObjectPrefix reports whether combined is either a complete JSON
// object or an incomplete JSON object prefix ending with io.ErrUnexpectedEOF.
// Non-object prefixes, duplicate keys, or syntax errors before EOF return false.
func canContinueJSONObjectPrefix(combined string) bool {
	trimmed := strings.TrimSpace(combined)
	if trimmed == "" || trimmed[0] != '{' {
		return false
	}
	if isValidSingleJSONObject([]byte(trimmed)) {
		return true
	}
	dec := jsontext.NewDecoder(strings.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	_, err := dec.ReadValue()
	return errors.Is(err, io.ErrUnexpectedEOF)
}

// extractStreamCallArgs parses rawArgs as omitted ("", false), a JSON string
// fragment (frag, false), or a compacted JSON object (obj, true). Arrays,
// numbers, booleans, or malformed JSON values return an error immediately.
func extractStreamCallArgs(rawArgs jsontext.Value) (string, bool, error) {
	trimmed := bytes.TrimSpace(rawArgs)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", false, nil
	}
	if trimmed[0] == '"' {
		var frag string
		if err := unmarshalJSONValue(trimmed, &frag); err != nil {
			return "", false, fmt.Errorf("decode streamed functionCall.args string: %w", err)
		}
		return frag, false, nil
	}
	if !isValidSingleJSONObject(trimmed) {
		return "", false, errors.New("functionCall.args must be a JSON object or streamed JSON string")
	}
	return string(compactRawJSON(trimmed)), true, nil
}

// flushNormalCompletion appends any pending Markdown grounding footer and
// citations before closing d's active part ahead of EventResponseEnd. When a
// text part is already open, grounding appends directly to that slot; otherwise
// flushNormalCompletion closes the active part and emits a dedicated text slot.
func (d *StreamDecoder) flushNormalCompletion(events *[]ir.Event) error {
	footer, anns := d.grounding.format()
	if footer != "" || len(anns) > 0 {
		if d.openKind == openText {
			if footer != "" {
				d.seenText = append(d.seenText, footer...)
				*events = append(*events, d.seq.Push(ir.Event{
					Kind:  ir.EventPartDelta,
					Index: d.activeSlot,
					Delta: &ir.Delta{Text: footer},
				})...)
			}
			return d.closeTextPartWithAnnotations(events, anns)
		}
		if err := d.closeCurrentOpenPart(events); err != nil {
			return err
		}
		slot := d.allocatePartSlot()
		startPart := ir.Part{Kind: ir.PartText}
		endPart := ir.Part{
			Kind:        ir.PartText,
			Text:        footer,
			Annotations: anns,
		}
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartStart,
			Index: slot,
			Part:  &startPart,
		})...)
		if footer != "" {
			*events = append(*events, d.seq.Push(ir.Event{
				Kind:  ir.EventPartDelta,
				Index: slot,
				Delta: &ir.Delta{Text: footer},
			})...)
		}
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: slot,
			Part:  &endPart,
		})...)
		return nil
	}
	return d.closeCurrentOpenPart(events)
}

// closeCurrentOpenPart finalizes d's currently open text, thought, or tool part
// and resets d.openKind to openNone. Tool calls with blank arguments default to
// "{}", whereas non-object streamed tool arguments close as Incomplete=true and
// return an error.
func (d *StreamDecoder) closeCurrentOpenPart(events *[]ir.Event) error {
	switch d.openKind {
	case openNone:
		return nil

	case openText:
		return d.closeTextPartWithAnnotations(events, nil)

	case openThought:
		var segs []string
		if len(d.seenThinking) > 0 {
			segs = []string{string(d.seenThinking)}
		}
		var tok *ir.ReplayToken
		if d.openSig != "" {
			tok = &ir.ReplayToken{Data: d.openSig}
		}
		endPart := ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Segments: segs,
				Summary:  true,
				Token:    tok,
			},
			Extra: toExtra(d.openUnknown),
		}
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: d.activeSlot,
			Part:  &endPart,
		})...)
		d.openKind = openNone
		d.seenThinking = d.seenThinking[:0]
		d.openSig = ""
		d.openUnknown = nil
		return nil

	case openTool:
		rec := d.activeTool
		slot := d.activeSlot
		d.openKind = openNone
		d.activeTool = nil
		if rec == nil {
			return nil
		}
		finalArgs := rec.args
		if strings.TrimSpace(finalArgs) == "" {
			finalArgs = "{}"
		} else if !isValidSingleJSONObject([]byte(finalArgs)) {
			// Close the open Sequencer slot as Incomplete=true before returning an
			// error so Close(nil) or Decode error paths leave Sequencer consistent.
			d.reopenAndCloseIncompleteTool(events, slot, rec)
			return errors.New("streamed functionCall.args is not a valid JSON object")
		}
		rec.args = finalArgs
		if rec.rawID != "" {
			if d.closedByID == nil {
				d.closedByID = make(map[string]closedToolInfo)
			}
			d.closedByID[rec.rawID] = closedToolInfo{
				partIndex: slot,
				signature: rec.sig,
			}
		}

		var tok *ir.ReplayToken
		if rec.sig != "" {
			tok = &ir.ReplayToken{Data: rec.sig}
		}
		unknown, err := mergeNestedPartUnknown(rec.partUnknown, "", "functionCall", rec.callUnknown)
		if err != nil {
			return err
		}
		endPart := ir.Part{
			Kind: ir.PartToolCall,
			Call: &ir.ToolCall{
				CallID:    rec.callID,
				Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: rec.name},
				Arguments: finalArgs,
				Token:     tok,
			},
			Extra: toExtra(unknown),
		}
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: slot,
			Part:  &endPart,
		})...)
		return nil

	default:
		d.openKind = openNone
		return nil
	}
}

// reopenAndCloseIncompleteTool closes slot inside d.seq as an Incomplete=true
// PartToolCall when fragmented JSON argument validation fails or the stream
// aborts mid-call. Calls when d.seq has no open parts return without emitting events.
func (d *StreamDecoder) reopenAndCloseIncompleteTool(events *[]ir.Event, slot int, rec *toolRecord) {
	if !d.seq.HasOpenParts() {
		return
	}
	var tok *ir.ReplayToken
	if rec.sig != "" {
		tok = &ir.ReplayToken{Data: rec.sig}
	}
	endPart := ir.Part{
		Kind: ir.PartToolCall,
		Call: &ir.ToolCall{
			CallID:     rec.callID,
			Tool:       ir.ToolRef{Kind: ir.ToolFunction, Name: rec.name},
			Arguments:  rec.args,
			Token:      tok,
			Incomplete: true,
		},
	}
	*events = append(*events, d.seq.Push(ir.Event{
		Kind:  ir.EventPartEnd,
		Index: slot,
		Part:  &endPart,
	})...)
}

// closeTextPartWithAnnotations emits EventPartEnd for d's open PartText slot
// with accumulated d.seenText, optional grounding anns, and any pending
// thoughtSignature stored in Part.Token. After pushing EventPartEnd, it resets
// d.openKind, d.seenText, d.openSig, and d.openUnknown.
func (d *StreamDecoder) closeTextPartWithAnnotations(events *[]ir.Event, anns []ir.Annotation) error {
	var tok *ir.ReplayToken
	if d.openSig != "" {
		tok = &ir.ReplayToken{Data: d.openSig}
	}
	endPart := ir.Part{
		Kind:        ir.PartText,
		Text:        string(d.seenText),
		Annotations: anns,
		Token:       tok,
		Extra:       toExtra(d.openUnknown),
	}
	*events = append(*events, d.seq.Push(ir.Event{
		Kind:  ir.EventPartEnd,
		Index: d.activeSlot,
		Part:  &endPart,
	})...)
	d.openKind = openNone
	d.seenText = d.seenText[:0]
	d.openSig = ""
	d.openUnknown = nil
	return nil
}

// abortOpenPartForError closes any open part inside d.seq before a terminal
// EventError is emitted so section 3.5 invariant 7 holds. Interrupted tool
// calls close with Incomplete=true, while decoders with no open Sequencer parts
// simply clear d.openKind and d.activeTool.
func (d *StreamDecoder) abortOpenPartForError(events *[]ir.Event) {
	if !d.seq.HasOpenParts() {
		d.openKind = openNone
		d.activeTool = nil
		return
	}
	switch d.openKind {
	case openText:
		_ = d.closeTextPartWithAnnotations(events, nil)
	case openThought:
		var segs []string
		if len(d.seenThinking) > 0 {
			segs = []string{string(d.seenThinking)}
		}
		var tok *ir.ReplayToken
		if d.openSig != "" {
			tok = &ir.ReplayToken{Data: d.openSig}
		}
		endPart := ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Segments: segs,
				Summary:  true,
				Token:    tok,
			},
			Extra: toExtra(d.openUnknown),
		}
		*events = append(*events, d.seq.Push(ir.Event{
			Kind:  ir.EventPartEnd,
			Index: d.activeSlot,
			Part:  &endPart,
		})...)
		d.seenThinking = d.seenThinking[:0]
		d.openSig = ""
		d.openUnknown = nil
	case openTool:
		if d.activeTool != nil {
			d.reopenAndCloseIncompleteTool(events, d.activeSlot, d.activeTool)
		}
	}
	d.openKind = openNone
	d.activeTool = nil
}

// computeGeminiTextDelta reconciles mixed cumulative and incremental Gemini
// text or thought stream payloads without quadratic string reallocations across
// multi-thousand-frame streams. It strips a single trailing NUL byte from
// incoming and retains seen unchanged when incoming becomes empty.
//
// When incoming extends seen as a cumulative prefix, computeGeminiTextDelta
// returns only the newly added suffix and overwrites seen in place; when
// incoming is a shorter or equal prefix of seen, it suppresses duplicate or
// rewound output; otherwise it treats incoming as an incremental fragment and
// appends its bytes onto seen.
func computeGeminiTextDelta(seen []byte, incoming string) (string, []byte) {
	incoming = strings.TrimSuffix(incoming, "\u0000")
	if incoming == "" {
		return "", seen
	}
	if stringHasBytesPrefix(incoming, seen) {
		delta := incoming[len(seen):]
		seen = append(seen[:0], incoming...)
		return delta, seen
	}
	// Suppress shorter or equal incoming prefixes emitted by duplicate or rewound
	// cumulative SSE relays while retaining the longest observed seen history.
	if bytesHaveStringPrefix(seen, incoming) {
		return "", seen
	}
	return incoming, append(seen, incoming...)
}

// stringHasBytesPrefix reports whether s starts with the exact byte sequence in prefix
// without allocating a temporary string or byte slice.
func stringHasBytesPrefix(s string, prefix []byte) bool {
	if len(s) < len(prefix) {
		return false
	}
	for i, b := range prefix {
		if s[i] != b {
			return false
		}
	}
	return true
}

// bytesHaveStringPrefix reports whether b starts with the exact UTF-8 bytes of prefix
// without allocating a temporary byte slice.
func bytesHaveStringPrefix(b []byte, prefix string) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := range len(prefix) {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}
