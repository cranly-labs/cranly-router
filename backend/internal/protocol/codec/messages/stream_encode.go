package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream_encode.go encodes canonical sequential ir.Event streams into client
// Anthropic Messages SSE frames annotated with Semantic and Terminal delivery
// flags. Gateway stream forwarders inspect Semantic to mark retry cutoffs after
// visible content starts and inspect Terminal to close the client stream cleanly.
//
// Outgoing streams emit a single message_start envelope once initial metadata
// and any immediately following EventUsage observation are ready, assign
// contiguous zero-based block indices only to emitted content blocks so dropped
// parts leave no index gaps, withhold non-object tool arguments so input:{}
// stands on cross-protocol calls, send reasoning tokens via signature_delta
// ahead of content_block_stop, and close any open block cleanly before a
// terminal error frame so abnormal truncations satisfy Law S7 grammar rules.

// StreamEncoder encodes sequential ir.Event values into Anthropic Messages SSE frames.
type StreamEncoder struct {
	view            ir.ClientView    // client model, original request, and runtime callbacks
	meta            *ir.ResponseMeta // initial response metadata captured before message_start
	started         bool             // whether message_start has been emitted
	terminated      bool             // whether message_stop or error has been emitted
	hasToolUse      bool             // whether at least one callable tool_use block opened
	nextBlockIdx    int              // next zero-based wire content_block index
	toolSeq         int              // monotonic counter for fallback toolu_ IDs
	openBlockIdx    int              // active wire block index, or -1 when no block is open
	openKind        ir.PartKind      // active canonical PartKind, or ""
	openToolKind    ir.ToolKind      // active ToolCall.Tool.Kind when openKind is PartToolCall
	openEmittedText strings.Builder  // exact text or thinking bytes already streamed
	openEmittedArgs strings.Builder  // exact partial_json bytes already streamed
	openPendingArgs strings.Builder  // leading whitespace buffered before '{' check
	openArgsStarted bool             // true once partial_json confirmed leading '{'
	openNonObjArgs  bool             // true when tool arguments do not begin with '{'
	openHostedSkip  bool             // true when current open tool_call is a skipped hosted call
	openCache       *ir.CacheMark    // initial cache breakpoint captured on EventPartStart
	openExtra       ir.Extra         // initial Extra members captured on EventPartStart
	openSentCache   bool             // whether open block already serialized Cache on start
	openSentExtra   bool             // whether open block already serialized Extra on start
	usage           *ir.Usage        // latest cumulative token usage snapshot
}

// NewStreamEncoder constructs a client-facing Anthropic Messages StreamEncoder
// configured from view. Callers must allocate a separate StreamEncoder for each
// outgoing client response stream so block numbering starts at zero.
func NewStreamEncoder(view ir.ClientView) *StreamEncoder {
	return &StreamEncoder{
		view:         view,
		openBlockIdx: -1,
	}
}

// NewStreamEncoder constructs a client-facing Anthropic Messages StreamEncoder
// configured from view by delegating to the package-level NewStreamEncoder.
// Each client response stream requires its own independent encoder instance.
func (Codec) NewStreamEncoder(view ir.ClientView) *StreamEncoder {
	return NewStreamEncoder(view)
}

// Encode converts one canonical ir.Event into zero or more Anthropic Messages
// sse.Frame values alongside any incurred ir.Loss records. Events supplied
// after a terminal EventResponseEnd or EventError return nil without emitting.
func (e *StreamEncoder) Encode(ev ir.Event) ([]sse.Frame, []ir.Loss, error) {
	if e.terminated {
		return nil, nil, nil
	}

	var frames []sse.Frame
	var losses []ir.Loss

	switch ev.Kind {
	case ir.EventResponseStart:
		if ev.Response != nil && e.meta == nil {
			cp := *ev.Response
			e.meta = &cp
		}

	case ir.EventUsage:
		if ev.Usage != nil {
			e.usage = ev.Usage.Clone()
		}

	case ir.EventPartStart:
		if err := e.ensureStarted(&frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartStart(ev, &frames, &losses); err != nil {
			return nil, nil, err
		}

	case ir.EventPartDelta:
		if err := e.ensureStarted(&frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartDelta(ev, &frames, &losses); err != nil {
			return nil, nil, err
		}

	case ir.EventPartEnd:
		if err := e.ensureStarted(&frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartEnd(ev, &frames, &losses); err != nil {
			return nil, nil, err
		}

	case ir.EventResponseEnd:
		if err := e.ensureStarted(&frames); err != nil {
			return nil, nil, err
		}
		if err := e.handleResponseEnd(ev, &frames); err != nil {
			return nil, nil, err
		}

	case ir.EventError:
		if err := e.handleError(ev.Err, &frames); err != nil {
			return nil, nil, err
		}
	}

	return frames, losses, nil
}

// ensureStarted emits the unique leading message_start SSE frame once before
// any content block or message_delta frame. Any ResponseMeta or initial
// EventUsage observation buffered beforehand is embedded into message_start.
func (e *StreamEncoder) ensureStarted(frames *[]sse.Frame) error {
	if e.started {
		return nil
	}
	var metaID, metaModel string
	if e.meta != nil {
		metaID = e.meta.ID
		metaModel = e.meta.Model
	}
	id := metaID
	if id == "" {
		id = e.view.Runtime.NextID("msg_", "msg_0")
	}
	model := resolveClientModel(e.view, metaModel)

	raw, err := marshalDeterministic(wireMessageStartWrite{
		Type: "message_start",
		Message: wireMessageStartBodyWrite{
			ID:           id,
			Type:         messagesTypeMessage,
			Role:         messagesRoleAssistant,
			Model:        model,
			Content:      []jsontext.Value{},
			StopReason:   nil,
			StopSequence: nil,
			Usage:        encodeMessagesUsage(e.usage),
		},
	})
	if err != nil {
		return err
	}
	e.started = true
	*frames = append(*frames, sse.Frame{
		Event:    "message_start",
		Data:     raw,
		Semantic: false,
		Terminal: false,
	})
	return nil
}

// resetOpenState clears per-part tracking fields once a part closes.
func (e *StreamEncoder) resetOpenState() {
	e.openBlockIdx = -1
	e.openKind = ""
	e.openToolKind = ""
	e.openEmittedText.Reset()
	e.openEmittedArgs.Reset()
	e.openPendingArgs.Reset()
	e.openArgsStarted = false
	e.openNonObjArgs = false
	e.openHostedSkip = false
	e.openCache = nil
	e.openExtra = ir.Extra{}
	e.openSentCache = false
	e.openSentExtra = false
}

// handlePartStart prepares or opens a new content block for ev.Part. Text and
// refusal parts open lazily on their first non-empty delta so empty parts never
// allocate block indices, whereas non-redacted thinking and callable tool_use
// blocks open immediately for incremental delta streaming.
func (e *StreamEncoder) handlePartStart(ev ir.Event, frames *[]sse.Frame, losses *[]ir.Loss) error {
	e.resetOpenState()
	if ev.Part == nil {
		return nil
	}
	p := *ev.Part
	pPath := fmt.Sprintf("parts[%d]", ev.Index)
	e.openKind = p.Kind
	if p.Cache != nil {
		cp := *p.Cache
		cp.Extra = p.Cache.Extra.Clone()
		e.openCache = &cp
	}
	e.openExtra = p.Extra.Clone()

	switch p.Kind {
	case ir.PartText, ir.PartRefusal:
		if p.Text != "" {
			if err := e.ensureTextBlockOpen(pPath, frames); err != nil {
				return err
			}
			if err := e.emitTextDelta(e.openBlockIdx, p.Text, frames); err != nil {
				return err
			}
			_, _ = e.openEmittedText.WriteString(p.Text)
		}

	case ir.PartReasoning:
		if p.Reasoning != nil && p.Reasoning.Redacted {
			// Redacted thinking carries complete encrypted data at EventPartEnd.
			return nil
		}
		if err := e.openThinkingBlock(pPath, frames); err != nil {
			return err
		}
		if p.Reasoning != nil {
			joined := strings.Join(p.Reasoning.Segments, "")
			if joined != "" {
				if err := e.emitThinkingDelta(e.openBlockIdx, joined, frames); err != nil {
					return err
				}
				_, _ = e.openEmittedText.WriteString(joined)
			}
		}

	case ir.PartToolCall:
		if p.Call == nil {
			e.openKind = ""
			return nil
		}
		if p.Call.Tool.Kind == ir.ToolHosted {
			e.openHostedSkip = true
			return nil
		}
		e.openToolKind = p.Call.Tool.Kind
		if err := e.openToolUseBlock(*p.Call, pPath, frames, losses); err != nil {
			return err
		}
		if p.Call.Tool.Kind != ir.ToolCustom && p.Call.Arguments != "" {
			if err := e.streamFunctionArgsFragment(p.Call.Arguments, frames); err != nil {
				return err
			}
		}
	}
	return nil
}

// ensureTextBlockOpen emits content_block_start for an unopened text or refusal
// part and assigns its contiguous wire block index. Repeated calls on an
// already open block are no-ops.
func (e *StreamEncoder) ensureTextBlockOpen(pPath string, frames *[]sse.Frame) error {
	if e.openBlockIdx >= 0 {
		return nil
	}
	var ignoredLosses []ir.Loss
	var cache *wireCacheControl
	if e.openCache != nil {
		cache = encodeCacheControl(e.openCache, pPath+".cache", &ignoredLosses)
		e.openSentCache = true
	}
	var unknown map[string]jsontext.Value
	if e.openExtra.Origin == protocol.ProtocolAnthropicMessages && !e.openExtra.IsZero() {
		unknown = restoreOrDropExtra(e.openExtra, pPath, &ignoredLosses)
		e.openSentExtra = true
	}

	rawBlk, err := marshalDeterministic(wireTextBlock{
		Type:         "text",
		Text:         "",
		CacheControl: cache,
		Unknown:      unknown,
	})
	if err != nil {
		return err
	}
	idx := e.nextBlockIdx
	e.nextBlockIdx++
	e.openBlockIdx = idx
	return e.emitBlockStart(idx, rawBlk, false, frames)
}

// openThinkingBlock emits content_block_start with empty initial thinking text
// for a non-redacted reasoning part and assigns its contiguous wire block index.
// Same-origin cache_control and Extra fields from EventPartStart attach on start.
func (e *StreamEncoder) openThinkingBlock(pPath string, frames *[]sse.Frame) error {
	var ignoredLosses []ir.Loss
	var cache *wireCacheControl
	if e.openCache != nil {
		cache = encodeCacheControl(e.openCache, pPath+".cache", &ignoredLosses)
		e.openSentCache = true
	}
	var unknown map[string]jsontext.Value
	if e.openExtra.Origin == protocol.ProtocolAnthropicMessages && !e.openExtra.IsZero() {
		unknown = restoreOrDropExtra(e.openExtra, pPath, &ignoredLosses)
		e.openSentExtra = true
	}

	rawBlk, err := marshalDeterministic(wireThinkingBlock{
		Type:         "thinking",
		Thinking:     "",
		CacheControl: cache,
		Unknown:      unknown,
	})
	if err != nil {
		return err
	}
	idx := e.nextBlockIdx
	e.nextBlockIdx++
	e.openBlockIdx = idx
	return e.emitBlockStart(idx, rawBlk, false, frames)
}

// openToolUseBlock emits content_block_start with input:{} for a callable
// tool_use part and marks the frame Semantic=true. Missing CallIDs synthesize
// deterministic toolu_ identifiers via ClientView.Runtime and record LossIDRewritten.
func (e *StreamEncoder) openToolUseBlock(
	call ir.ToolCall,
	pPath string,
	frames *[]sse.Frame,
	losses *[]ir.Loss,
) error {
	var ignoredLosses []ir.Loss
	var cache *wireCacheControl
	if e.openCache != nil {
		cache = encodeCacheControl(e.openCache, pPath+".cache", &ignoredLosses)
		e.openSentCache = true
	}
	var unknown map[string]jsontext.Value
	if e.openExtra.Origin == protocol.ProtocolAnthropicMessages && !e.openExtra.IsZero() {
		unknown = restoreOrDropExtra(e.openExtra, pPath, &ignoredLosses)
		e.openSentExtra = true
	}

	e.toolSeq++
	callID := call.CallID
	if callID == "" {
		callID = e.view.Runtime.NextID("toolu_", fmt.Sprintf("toolu_%d", e.toolSeq))
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossIDRewritten,
			Path: pPath + ".call_id",
		})
	}
	fnName := resolveResponseCallName(call.Tool, pPath, losses)

	rawBlk, err := marshalDeterministic(wireToolUseBlock{
		Type:         "tool_use",
		ID:           callID,
		Name:         fnName,
		Input:        jsontext.Value("{}"),
		CacheControl: cache,
		Unknown:      unknown,
	})
	if err != nil {
		return err
	}
	idx := e.nextBlockIdx
	e.nextBlockIdx++
	e.openBlockIdx = idx
	e.hasToolUse = true
	return e.emitBlockStart(idx, rawBlk, true, frames)
}

// streamFunctionArgsFragment streams incremental input_json_delta fragments for
// a callable non-custom tool_use block once the leading non-whitespace byte is
// confirmed to be '{'. Non-object JSON values such as "[]" or "null" withhold
// deltas so content_block_start's input:{} stands identically to EncodeResponse.
func (e *StreamEncoder) streamFunctionArgsFragment(fragment string, frames *[]sse.Frame) error {
	if fragment == "" || e.openNonObjArgs || e.openBlockIdx < 0 {
		return nil
	}
	if e.openArgsStarted {
		if err := e.emitInputJSONDelta(e.openBlockIdx, fragment, frames); err != nil {
			return err
		}
		_, _ = e.openEmittedArgs.WriteString(fragment)
		return nil
	}

	_, _ = e.openPendingArgs.WriteString(fragment)
	buffered := e.openPendingArgs.String()
	trimmed := strings.TrimLeft(buffered, " \t\n\r")
	if trimmed == "" {
		return nil
	}
	if trimmed[0] != '{' {
		// Suppress input_json_delta when cross-protocol tool arguments start with a
		// non-object JSON token so Messages stream grammar and S6 parity both hold.
		e.openNonObjArgs = true
		e.openPendingArgs.Reset()
		return nil
	}

	e.openArgsStarted = true
	e.openPendingArgs.Reset()
	if err := e.emitInputJSONDelta(e.openBlockIdx, buffered, frames); err != nil {
		return err
	}
	_, _ = e.openEmittedArgs.WriteString(buffered)
	return nil
}

// handlePartDelta emits incremental text_delta, thinking_delta, or
// input_json_delta SSE frames for the currently open part. Nil or empty deltas
// and custom/hosted tool calls emit no frames here.
func (e *StreamEncoder) handlePartDelta(ev ir.Event, frames *[]sse.Frame, _ *[]ir.Loss) error {
	if ev.Delta == nil {
		return nil
	}
	pPath := fmt.Sprintf("parts[%d]", ev.Index)

	switch e.openKind {
	case ir.PartText, ir.PartRefusal:
		if ev.Delta.Text != "" {
			if err := e.ensureTextBlockOpen(pPath, frames); err != nil {
				return err
			}
			if err := e.emitTextDelta(e.openBlockIdx, ev.Delta.Text, frames); err != nil {
				return err
			}
			_, _ = e.openEmittedText.WriteString(ev.Delta.Text)
		}

	case ir.PartReasoning:
		if e.openBlockIdx >= 0 && ev.Delta.Text != "" {
			if err := e.emitThinkingDelta(e.openBlockIdx, ev.Delta.Text, frames); err != nil {
				return err
			}
			_, _ = e.openEmittedText.WriteString(ev.Delta.Text)
		}

	case ir.PartToolCall:
		if !e.openHostedSkip && e.openToolKind != ir.ToolCustom && e.openBlockIdx >= 0 && ev.Delta.Arguments != "" {
			if err := e.streamFunctionArgsFragment(ev.Delta.Arguments, frames); err != nil {
				return err
			}
		}
	}
	return nil
}

// handlePartEnd flushes any unstreamed suffix or signature_delta on the active
// part, emits content_block_stop when a wire block was opened, and records all
// part-level ir.Loss entries against parts[ev.Index].
//
// Redacted thinking and same-origin opaque blocks emit as atomic
// content_block_start + content_block_stop pairs on EventPartEnd, while empty
// text parts and dropped media, hosted, or foreign opaque parts allocate no
// wire block index so emitted block numbering stays strictly contiguous.
func (e *StreamEncoder) handlePartEnd(ev ir.Event, frames *[]sse.Frame, losses *[]ir.Loss) error {
	defer e.resetOpenState()

	if ev.Part == nil {
		if e.openBlockIdx >= 0 {
			return e.emitBlockStop(e.openBlockIdx, frames)
		}
		return nil
	}

	p := *ev.Part
	pPath := fmt.Sprintf("parts[%d]", ev.Index)

	switch p.Kind {
	case ir.PartText, ir.PartRefusal:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if len(p.Annotations) > 0 {
			*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: pPath})
		}
		if p.Token != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
		}

		// Open lazily when non-empty text, cache_control, or same-origin Extra
		// arrived only on the completed part_end snapshot.
		hasSameOriginExtra := p.Extra.Origin == protocol.ProtocolAnthropicMessages && !p.Extra.IsZero()
		if e.openBlockIdx < 0 && (p.Text != "" || p.Cache != nil || hasSameOriginExtra) {
			if p.Cache != nil {
				cp := *p.Cache
				cp.Extra = p.Cache.Extra.Clone()
				e.openCache = &cp
			}
			e.openExtra = p.Extra.Clone()
			if err := e.ensureTextBlockOpen(pPath, frames); err != nil {
				return err
			}
		}

		e.reconcileEndMetadataLosses(p.Cache, p.Extra, pPath, losses)

		if e.openBlockIdx >= 0 {
			if p.Kind == ir.PartRefusal {
				*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: pPath})
			}
			emitted := e.openEmittedText.String()
			if emitted == "" && p.Text != "" {
				if err := e.emitTextDelta(e.openBlockIdx, p.Text, frames); err != nil {
					return err
				}
			} else if len(p.Text) > len(emitted) && strings.HasPrefix(p.Text, emitted) {
				if err := e.emitTextDelta(e.openBlockIdx, p.Text[len(emitted):], frames); err != nil {
					return err
				}
			}
			return e.emitBlockStop(e.openBlockIdx, frames)
		}
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossClientMarkerDropped,
			Path:   pPath,
			Detail: "empty_text",
		})

	case ir.PartReasoning:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Reasoning != nil && p.Reasoning.Redacted {
			pUnknown := restoreOrDropExtra(p.Extra, pPath, losses)
			if p.Reasoning.Token != nil && p.Reasoning.Token.Data != "" {
				if joined := strings.Join(p.Reasoning.Segments, ""); joined != "" {
					*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
				}
				cache := encodeCacheControl(p.Cache, pPath+".cache", losses)
				rawBlk, err := marshalDeterministic(wireRedactedThinkingBlock{
					Type:         "redacted_thinking",
					Data:         p.Reasoning.Token.Data,
					CacheControl: cache,
					Unknown:      pUnknown,
				})
				if err != nil {
					return err
				}
				idx := e.nextBlockIdx
				e.nextBlockIdx++
				if err := e.emitBlockStart(idx, rawBlk, true, frames); err != nil {
					return err
				}
				return e.emitBlockStop(idx, frames)
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			if len(p.Reasoning.Segments) > 0 {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
			}
			return nil
		}

		if e.openBlockIdx < 0 {
			if p.Cache != nil {
				cp := *p.Cache
				cp.Extra = p.Cache.Extra.Clone()
				e.openCache = &cp
			}
			e.openExtra = p.Extra.Clone()
			if err := e.openThinkingBlock(pPath, frames); err != nil {
				return err
			}
		}
		e.reconcileEndMetadataLosses(p.Cache, p.Extra, pPath, losses)

		if p.Reasoning != nil {
			joined := strings.Join(p.Reasoning.Segments, "")
			emitted := e.openEmittedText.String()
			if emitted == "" && joined != "" {
				if err := e.emitThinkingDelta(e.openBlockIdx, joined, frames); err != nil {
					return err
				}
			} else if len(joined) > len(emitted) && strings.HasPrefix(joined, emitted) {
				if err := e.emitThinkingDelta(e.openBlockIdx, joined[len(emitted):], frames); err != nil {
					return err
				}
			}
			if p.Reasoning.Token != nil && p.Reasoning.Token.Data != "" {
				if err := e.emitSignatureDelta(e.openBlockIdx, p.Reasoning.Token.Data, frames); err != nil {
					return err
				}
			}
		}
		return e.emitBlockStop(e.openBlockIdx, frames)

	case ir.PartToolCall:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if e.openHostedSkip || (p.Call != nil && p.Call.Tool.Kind == ir.ToolHosted) {
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
			return nil
		}
		if p.Call == nil {
			return nil
		}
		if e.openBlockIdx < 0 {
			if p.Cache != nil {
				cp := *p.Cache
				cp.Extra = p.Cache.Extra.Clone()
				e.openCache = &cp
			}
			e.openExtra = p.Extra.Clone()
			e.openToolKind = p.Call.Tool.Kind
			if err := e.openToolUseBlock(*p.Call, pPath, frames, losses); err != nil {
				return err
			}
		}
		if p.Call.Token != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
		}
		e.reconcileEndMetadataLosses(p.Cache, p.Extra, pPath, losses)

		if p.Call.Tool.Kind == ir.ToolCustom {
			wrapped, err := encodeResponseToolInput(*p.Call)
			if err != nil {
				return err
			}
			if err := e.emitInputJSONDelta(e.openBlockIdx, string(wrapped), frames); err != nil {
				return err
			}
		} else {
			emitted := e.openEmittedArgs.String()
			finalArgs := p.Call.Arguments
			trimmed := strings.TrimSpace(finalArgs)
			if emitted == "" {
				if p.Call.Incomplete {
					if trimmed != "" && trimmed[0] == '{' {
						if err := e.emitInputJSONDelta(e.openBlockIdx, finalArgs, frames); err != nil {
							return err
						}
					}
				} else if trimmed != "" && isValidSingleJSONObject([]byte(trimmed)) {
					normalized := string(compactRawJSON(jsontext.Value(trimmed)))
					if err := e.emitInputJSONDelta(e.openBlockIdx, normalized, frames); err != nil {
						return err
					}
				}
			} else if len(finalArgs) > len(emitted) && strings.HasPrefix(finalArgs, emitted) {
				// Emit a remaining suffix only when finalArgs strictly extends the
				// already-streamed prefix, avoiding byte-offset corruption when
				// EventPartEnd normalizes escapes, whitespace, or invalid UTF-8.
				if p.Call.Incomplete || isValidSingleJSONObject([]byte(trimmed)) {
					if err := e.emitInputJSONDelta(e.openBlockIdx, finalArgs[len(emitted):], frames); err != nil {
						return err
					}
				}
			}
		}
		return e.emitBlockStop(e.openBlockIdx, frames)

	case ir.PartMedia:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossMediaUnsupported,
			Path:   pPath,
			Detail: "assistant_output",
		})

	case ir.PartOpaque:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolAnthropicMessages {
			idx := e.nextBlockIdx
			e.nextBlockIdx++
			if err := e.emitBlockStart(idx, bytes.Clone(p.Opaque.Raw), true, frames); err != nil {
				return err
			}
			return e.emitBlockStop(idx, frames)
		}
		detail := ""
		if p.Opaque != nil {
			detail = p.Opaque.Type
		}
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOpaqueItemDropped,
			Path:   pPath,
			Detail: detail,
		})
	}
	return nil
}

// reconcileEndMetadataLosses accounts for Cache and Extra fields on EventPartEnd
// against what was already written into content_block_start. Metadata arriving
// late after content_block_start or from a foreign protocol records Loss entries.
func (e *StreamEncoder) reconcileEndMetadataLosses(
	cache *ir.CacheMark,
	extra ir.Extra,
	pPath string,
	losses *[]ir.Loss,
) {
	if cache != nil {
		if e.openSentCache {
			_ = restoreOrDropExtra(cache.Extra, pPath+".cache", losses)
		} else {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
	}
	if !extra.IsZero() {
		if extra.Origin != protocol.ProtocolAnthropicMessages || !e.openSentExtra {
			*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: pPath})
		}
	}
}

// handleResponseEnd closes any still-open content block and emits message_delta
// carrying stop_reason, stop_sequence, and cumulative usage followed by the
// terminal message_stop frame. Subsequent Encode calls return nil once terminated.
func (e *StreamEncoder) handleResponseEnd(ev ir.Event, frames *[]sse.Frame) error {
	if e.openBlockIdx >= 0 {
		if err := e.emitBlockStop(e.openBlockIdx, frames); err != nil {
			return err
		}
		e.resetOpenState()
	}

	var fin ir.Finish
	if ev.Finish != nil {
		fin = *ev.Finish
	}
	stopReason, stopSeq := encodeStopReason(fin, e.hasToolUse)

	rawDelta, err := marshalDeterministic(wireMessageDeltaWrite{
		Type: "message_delta",
		Delta: wireMessageDeltaBodyWrite{
			StopReason:   stopReason,
			StopSequence: stopSeq,
		},
		Usage: encodeMessagesUsage(e.usage),
	})
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "message_delta",
		Data:     rawDelta,
		Semantic: false,
		Terminal: false,
	})

	rawStop, err := marshalDeterministic(wireMessageStopWrite{Type: "message_stop"})
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "message_stop",
		Data:     rawStop,
		Semantic: false,
		Terminal: true,
	})
	e.terminated = true
	return nil
}

// handleError closes any open content block cleanly before emitting a terminal
// event: error frame. Closing the active block ensures truncated streams satisfy
// ValidateMessagesStream in AllowAbnormal mode.
func (e *StreamEncoder) handleError(se *ir.StreamError, frames *[]sse.Frame) error {
	if e.openBlockIdx >= 0 {
		if err := e.emitBlockStop(e.openBlockIdx, frames); err != nil {
			return err
		}
		e.resetOpenState()
	}
	errBytes, err := encodeStreamError(se)
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "error",
		Data:     errBytes,
		Semantic: false,
		Terminal: true,
	})
	e.terminated = true
	return nil
}

// emitBlockStart serializes one content_block_start SSE frame for wire index
// idx and appends it onto frames. Callers set semantic=true only when the
// opening block itself commits visible client state such as tool_use.
func (e *StreamEncoder) emitBlockStart(idx int, blockRaw []byte, semantic bool, frames *[]sse.Frame) error {
	raw, err := marshalDeterministic(wireContentBlockStartWrite{
		Type:         "content_block_start",
		Index:        idx,
		ContentBlock: jsontext.Value(blockRaw),
	})
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "content_block_start",
		Data:     raw,
		Semantic: semantic,
		Terminal: false,
	})
	return nil
}

// emitBlockStop serializes one non-semantic content_block_stop SSE frame for
// wire index idx and appends it onto frames. JSON marshaling errors return
// immediately without appending a partial SSE frame.
func (e *StreamEncoder) emitBlockStop(idx int, frames *[]sse.Frame) error {
	raw, err := marshalDeterministic(wireContentBlockStopWrite{
		Type:  "content_block_stop",
		Index: idx,
	})
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "content_block_stop",
		Data:     raw,
		Semantic: false,
		Terminal: false,
	})
	return nil
}

// emitTextDelta serializes a semantic text_delta SSE frame for wire index idx
// and appends it onto frames. Callers supply non-empty text fragments so retry
// cutoffs advance only when visible text is emitted.
func (e *StreamEncoder) emitTextDelta(idx int, text string, frames *[]sse.Frame) error {
	raw, err := marshalDeterministic(wireContentBlockDeltaWrite{
		Type:  "content_block_delta",
		Index: idx,
		Delta: wireDeltaWrite{
			Type: "text_delta",
			Text: &text,
		},
	})
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "content_block_delta",
		Data:     raw,
		Semantic: true,
		Terminal: false,
	})
	return nil
}

// emitThinkingDelta serializes a semantic thinking_delta SSE frame for wire
// index idx and appends it onto frames. Callers supply non-empty reasoning
// text fragments on an already opened thinking block.
func (e *StreamEncoder) emitThinkingDelta(idx int, thinking string, frames *[]sse.Frame) error {
	raw, err := marshalDeterministic(wireContentBlockDeltaWrite{
		Type:  "content_block_delta",
		Index: idx,
		Delta: wireDeltaWrite{
			Type:     "thinking_delta",
			Thinking: &thinking,
		},
	})
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "content_block_delta",
		Data:     raw,
		Semantic: true,
		Terminal: false,
	})
	return nil
}

// emitSignatureDelta serializes a semantic signature_delta SSE frame for wire
// index idx and appends it onto frames. Callers emit signature_delta ahead of
// content_block_stop so clients capture continuity signatures on the thinking block.
func (e *StreamEncoder) emitSignatureDelta(idx int, sig string, frames *[]sse.Frame) error {
	raw, err := marshalDeterministic(wireContentBlockDeltaWrite{
		Type:  "content_block_delta",
		Index: idx,
		Delta: wireDeltaWrite{
			Type:      "signature_delta",
			Signature: sig,
		},
	})
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "content_block_delta",
		Data:     raw,
		Semantic: true,
		Terminal: false,
	})
	return nil
}

// emitInputJSONDelta serializes a semantic input_json_delta SSE frame for wire
// index idx and appends it onto frames. Accumulated partial_json fragments on
// normal completions must form a valid JSON object at content_block_stop.
func (e *StreamEncoder) emitInputJSONDelta(idx int, partial string, frames *[]sse.Frame) error {
	raw, err := marshalDeterministic(wireContentBlockDeltaWrite{
		Type:  "content_block_delta",
		Index: idx,
		Delta: wireDeltaWrite{
			Type:        "input_json_delta",
			PartialJSON: &partial,
		},
	})
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Event:    "content_block_delta",
		Data:     raw,
		Semantic: true,
		Terminal: false,
	})
	return nil
}
