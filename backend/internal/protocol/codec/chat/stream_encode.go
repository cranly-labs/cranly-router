package chat

import (
	"encoding/json/jsontext"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream_encode.go encodes canonical sequential ir.Event streams into client
// OpenAI Chat Completions SSE frames with Semantic and Terminal delivery flags.
//
// Every chunk in one stream shares identical id, created, and model fields so
// strict client accumulators never reject mismatched chunk envelopes. Opening
// tool_call chunks announce index, id, type, function.name, and empty arguments
// while follow-up argument chunks carry only index and function.arguments to
// prevent 091ec1f41 empty-name overwrites. One-shot parts arriving complete at
// EventPartEnd flush unstreamed suffixes automatically, and usage tail chunks
// obey ClientView.Request.IncludeUsage before the terminal [DONE] marker.

// StreamEncoder encodes sequential ir.Event values into Chat Completions SSE frames.
type StreamEncoder struct {
	view            ir.ClientView   // client model, original request, and runtime callbacks
	includeUsage    bool            // whether stream_options.include_usage was enabled
	started         bool            // whether initial role="assistant" chunk was emitted
	terminated      bool            // whether [DONE] or error frame was emitted
	id              string          // locked stream completion identifier
	model           string          // locked client-visible model identifier
	created         int64           // locked Unix creation timestamp
	serviceTier     string          // latest non-empty service tier
	nextToolIdx     int             // next zero-based Chat tool_calls index
	openKind        ir.PartKind     // currently open canonical PartKind, or ""
	openToolKind    ir.ToolKind     // open tool category when openKind is PartToolCall
	openToolIdx     int             // active Chat tool_calls index when openKind is PartToolCall
	openEmittedLen  int             // byte length already emitted via deltas for open text/reasoning/refusal part
	openEmittedArgs strings.Builder // raw tool argument bytes already streamed for open PartToolCall
	openHostedSkip  bool            // true when current open tool_call is a skipped hosted call
	usage           *ir.Usage       // latest cumulative Usage snapshot for optional tail chunk
}

// NewStreamEncoder constructs a client-facing Chat Completions StreamEncoder
// configured from view. Usage tail chunks emit only when view.Request
// explicitly enables IncludeUsage, and callers allocate one encoder per stream.
func NewStreamEncoder(view ir.ClientView) *StreamEncoder {
	inc := view.Request != nil && view.Request.IncludeUsage != nil && *view.Request.IncludeUsage
	return &StreamEncoder{
		view:         view,
		includeUsage: inc,
	}
}

// NewStreamEncoder constructs a client-facing Chat Completions StreamEncoder
// configured from view by delegating to the package-level NewStreamEncoder.
// Callers must allocate a separate encoder instance for each client stream.
func (Codec) NewStreamEncoder(view ir.ClientView) *StreamEncoder {
	return NewStreamEncoder(view)
}

// Encode converts one canonical ir.Event into zero or more Chat Completions
// sse.Frame values alongside any incurred ir.Loss entries. Events supplied
// after a terminal EventResponseEnd or EventError return nil without emitting.
//
// Only chunks carrying visible text, reasoning, refusal, or tool_call openings
// and arguments set Frame.Semantic=true so upstream retry budgeting ignores
// initial role envelopes, finish_reason chunks, usage tails, and [DONE] markers.
func (e *StreamEncoder) Encode(ev ir.Event) ([]sse.Frame, []ir.Loss, error) {
	if e.terminated {
		return nil, nil, nil
	}

	var frames []sse.Frame
	var losses []ir.Loss

	switch ev.Kind {
	case ir.EventResponseStart:
		if err := e.ensureStarted(ev.Response, &frames); err != nil {
			return nil, nil, err
		}

	case ir.EventPartStart:
		if err := e.ensureStarted(nil, &frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartStart(ev, &frames, &losses); err != nil {
			return nil, nil, err
		}

	case ir.EventPartDelta:
		if err := e.ensureStarted(nil, &frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartDelta(ev, &frames); err != nil {
			return nil, nil, err
		}

	case ir.EventPartEnd:
		if err := e.ensureStarted(nil, &frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartEnd(ev, &frames, &losses); err != nil {
			return nil, nil, err
		}

	case ir.EventUsage:
		if ev.Usage != nil {
			e.usage = ev.Usage.Clone()
		}

	case ir.EventResponseEnd:
		if ev.ServiceTier != "" {
			e.serviceTier = ev.ServiceTier
		}
		if err := e.ensureStarted(nil, &frames); err != nil {
			return nil, nil, err
		}
		if err := e.handleResponseEnd(ev, &frames); err != nil {
			return nil, nil, err
		}

	case ir.EventError:
		errBytes, err := encodeStreamError(ev.Err)
		if err != nil {
			return nil, nil, err
		}
		frames = append(frames, sse.Frame{
			Data:     errBytes,
			Semantic: false,
			Terminal: true,
		})
		e.terminated = true
	}

	return frames, losses, nil
}

// ensureStarted locks stream envelope metadata once and emits the initial
// non-semantic role="assistant" chunk when not already sent. Subsequent calls
// update only a newly observed ServiceTier string.
func (e *StreamEncoder) ensureStarted(meta *ir.ResponseMeta, frames *[]sse.Frame) error {
	if e.started {
		if meta != nil && meta.ServiceTier != "" {
			e.serviceTier = meta.ServiceTier
		}
		return nil
	}
	var metaID, metaModel string
	var metaCreated int64
	if meta != nil {
		metaID = meta.ID
		metaModel = meta.Model
		metaCreated = meta.CreatedAt
		if meta.ServiceTier != "" {
			e.serviceTier = meta.ServiceTier
		}
	}
	if metaID != "" {
		e.id = metaID
	} else {
		e.id = e.view.Runtime.NextID("chatcmpl-", "chatcmpl-0")
	}
	if metaCreated > 0 {
		e.created = metaCreated
	} else {
		e.created = e.view.Runtime.NowUnix()
	}
	e.model = resolveClientModel(e.view, metaModel)
	e.started = true

	raw, err := e.marshalDeltaChunk(wireDeltaWrite{Role: "assistant"}, nil)
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Data:     raw,
		Semantic: false,
		Terminal: false,
	})
	return nil
}

// handlePartStart opens a new streaming part and records initial byte progress
// when ev.Part already carries skeleton text or arguments.
//
// Function tool calls emit an opening chunk containing index, id, type,
// function.name, and arguments:"" before any non-empty initial arguments delta,
// synthesizing missing CallIDs via Runtime and skipping provider-hosted calls.
func (e *StreamEncoder) handlePartStart(ev ir.Event, frames *[]sse.Frame, losses *[]ir.Loss) error {
	e.openKind = ""
	e.openToolKind = ""
	e.openEmittedLen = 0
	e.openEmittedArgs.Reset()
	e.openHostedSkip = false
	if ev.Part == nil {
		return nil
	}

	p := *ev.Part
	pPath := fmt.Sprintf("parts[%d]", ev.Index)
	e.openKind = p.Kind

	switch p.Kind {
	case ir.PartText:
		if p.Text != "" {
			if err := e.emitTextDelta(p.Text, frames); err != nil {
				return err
			}
			e.openEmittedLen = len(p.Text)
		}

	case ir.PartRefusal:
		if p.Text != "" {
			if err := e.emitRefusalDelta(p.Text, frames); err != nil {
				return err
			}
			e.openEmittedLen = len(p.Text)
		}

	case ir.PartReasoning:
		if p.Reasoning != nil {
			joined := strings.Join(p.Reasoning.Segments, "")
			if joined != "" {
				if err := e.emitReasoningDelta(joined, frames); err != nil {
					return err
				}
				e.openEmittedLen = len(joined)
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
		toolIdx := e.nextToolIdx
		e.nextToolIdx++
		e.openToolIdx = toolIdx
		e.openToolKind = p.Call.Tool.Kind

		callID := p.Call.CallID
		if callID == "" {
			callID = e.view.Runtime.NextID("call_", fmt.Sprintf("call_%d", toolIdx+1))
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossIDRewritten,
				Path: pPath + ".call_id",
			})
		}
		fnName := resolveResponseCallName(p.Call.Tool, pPath, losses)
		var tcUnknown map[string]jsontext.Value
		if p.Extra.Origin == protocol.ProtocolOpenAIChatCompletions && !p.Extra.IsZero() {
			tcUnknown = p.Extra.Clone().Members
		}

		emptyArgs := ""
		rawOpen, err := e.marshalDeltaChunk(wireDeltaWrite{
			ToolCalls: []wireToolCallWrite{{
				Index: &toolIdx,
				ID:    callID,
				Type:  "function",
				Function: wireFunctionCallWrite{
					Name:      fnName,
					Arguments: &emptyArgs,
				},
				Unknown: tcUnknown,
			}},
		}, nil)
		if err != nil {
			return err
		}
		*frames = append(*frames, sse.Frame{
			Data:     rawOpen,
			Semantic: true,
			Terminal: false,
		})

		if p.Call.Tool.Kind != ir.ToolCustom && p.Call.Arguments != "" {
			if err := e.emitToolArgsDelta(toolIdx, p.Call.Arguments, frames); err != nil {
				return err
			}
			_, _ = e.openEmittedArgs.WriteString(p.Call.Arguments)
		}
	}
	return nil
}

// handlePartDelta emits incremental content, reasoning_content, refusal, or
// tool_calls argument chunks for the currently open part. Nil deltas, empty
// fragments, custom free-text calls wrapped at closure, and skipped hosted tool
// calls return nil without emitting frames.
func (e *StreamEncoder) handlePartDelta(ev ir.Event, frames *[]sse.Frame) error {
	if ev.Delta == nil {
		return nil
	}
	switch e.openKind {
	case ir.PartText:
		if ev.Delta.Text != "" {
			if err := e.emitTextDelta(ev.Delta.Text, frames); err != nil {
				return err
			}
			e.openEmittedLen += len(ev.Delta.Text)
		}

	case ir.PartRefusal:
		if ev.Delta.Text != "" {
			if err := e.emitRefusalDelta(ev.Delta.Text, frames); err != nil {
				return err
			}
			e.openEmittedLen += len(ev.Delta.Text)
		}

	case ir.PartReasoning:
		if ev.Delta.Text != "" {
			if err := e.emitReasoningDelta(ev.Delta.Text, frames); err != nil {
				return err
			}
			e.openEmittedLen += len(ev.Delta.Text)
		}

	case ir.PartToolCall:
		if !e.openHostedSkip && e.openToolKind != ir.ToolCustom && ev.Delta.Arguments != "" {
			if err := e.emitToolArgsDelta(e.openToolIdx, ev.Delta.Arguments, frames); err != nil {
				return err
			}
			_, _ = e.openEmittedArgs.WriteString(ev.Delta.Arguments)
		}
	}
	return nil
}

// handlePartEnd flushes any unstreamed suffix on one-shot parts and records
// part-level information losses against parts[ev.Index].
//
// Upstreams such as Gemini deliver complete reasoning or tool arguments only at
// part closure without preceding EventPartDelta frames, while custom free-text
// calls wrap into {"input":"..."} at closure and completed zero-argument
// function calls normalize to "{}" for parity with EncodeResponse.
func (e *StreamEncoder) handlePartEnd(ev ir.Event, frames *[]sse.Frame, losses *[]ir.Loss) error {
	defer func() {
		e.openKind = ""
		e.openToolKind = ""
		e.openEmittedLen = 0
		e.openEmittedArgs.Reset()
		e.openHostedSkip = false
	}()

	if ev.Part == nil {
		return nil
	}
	p := *ev.Part
	pPath := fmt.Sprintf("parts[%d]", ev.Index)

	switch p.Kind {
	case ir.PartText:
		if len(p.Text) > e.openEmittedLen {
			if err := e.emitTextDelta(p.Text[e.openEmittedLen:], frames); err != nil {
				return err
			}
		}
		recordPartMetadataLosses(p, pPath, true, losses)
		_ = restoreOrDropExtra(p.Extra, pPath, losses)

	case ir.PartRefusal:
		if len(p.Text) > e.openEmittedLen {
			if err := e.emitRefusalDelta(p.Text[e.openEmittedLen:], frames); err != nil {
				return err
			}
		}
		recordPartMetadataLosses(p, pPath, true, losses)
		_ = restoreOrDropExtra(p.Extra, pPath, losses)

	case ir.PartReasoning:
		recordPartMetadataLosses(p, pPath, true, losses)
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		if p.Reasoning != nil {
			if p.Reasoning.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
			}
			joined := strings.Join(p.Reasoning.Segments, "")
			if len(joined) > e.openEmittedLen {
				if err := e.emitReasoningDelta(joined[e.openEmittedLen:], frames); err != nil {
					return err
				}
			}
		}

	case ir.PartToolCall:
		if e.openHostedSkip || (p.Call != nil && p.Call.Tool.Kind == ir.ToolHosted) {
			recordPartMetadataLosses(p, pPath, false, losses)
			_ = restoreOrDropExtra(p.Extra, pPath, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
			return nil
		}
		recordPartMetadataLosses(p, pPath, true, losses)
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		if p.Call != nil {
			if p.Call.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
			}
			if p.Call.Tool.Kind == ir.ToolCustom {
				// Wrap free-text custom tool arguments inside {"input":"..."} so Chat
				// tool_calls always emit valid JSON matching buffered EncodeResponse.
				wrapped, err := marshalDeterministic(struct {
					Input string `json:"input"` // raw custom tool text payload
				}{Input: p.Call.Arguments})
				if err != nil {
					return err
				}
				return e.emitToolArgsDelta(e.openToolIdx, string(wrapped), frames)
			}
			emitted := e.openEmittedArgs.String()
			finalArgs := p.Call.Arguments
			// Emit "{}" when a completed function call streamed zero argument bytes so
			// client accumulators receive valid JSON matching buffered EncodeResponse.
			if strings.TrimSpace(finalArgs) == "" && emitted == "" && !p.Call.Incomplete {
				finalArgs = "{}"
			}
			if len(finalArgs) > len(emitted) && strings.HasPrefix(finalArgs, emitted) {
				if err := e.emitToolArgsDelta(e.openToolIdx, finalArgs[len(emitted):], frames); err != nil {
					return err
				}
			}
		}

	case ir.PartMedia:
		recordPartMetadataLosses(p, pPath, true, losses)
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: pPath, Detail: "stream"})

	case ir.PartOpaque:
		recordPartMetadataLosses(p, pPath, true, losses)
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		detail := ""
		if p.Opaque != nil {
			detail = p.Opaque.Type
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath, Detail: detail})
	}
	return nil
}

// handleResponseEnd emits the finish_reason chunk, optional choices-empty usage
// tail chunk, and the terminal [DONE] marker frame. Missing Finish pointers
// default to "stop" before marking the encoder terminated.
func (e *StreamEncoder) handleResponseEnd(ev ir.Event, frames *[]sse.Frame) error {
	fin := ir.Finish{Reason: ir.FinishStop}
	if ev.Finish != nil {
		fin = *ev.Finish
	}
	fr := encodeFinishReason(fin)
	finishBytes, err := e.marshalDeltaChunk(wireDeltaWrite{}, &fr)
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{
		Data:     finishBytes,
		Semantic: false,
		Terminal: false,
	})

	if e.includeUsage && e.usage != nil {
		usageChunk, err := marshalDeterministic(wireStreamChunkWrite{
			ID:          e.id,
			Object:      chatObjectChunk,
			Created:     e.created,
			Model:       e.model,
			ServiceTier: e.serviceTier,
			Choices:     []wireChunkChoiceWrite{},
			Usage:       encodeChatUsage(e.usage),
		})
		if err != nil {
			return err
		}
		*frames = append(*frames, sse.Frame{
			Data:     usageChunk,
			Semantic: false,
			Terminal: false,
		})
	}

	*frames = append(*frames, sse.Frame{
		Data:     []byte("[DONE]"),
		Semantic: false,
		Terminal: true,
	})
	e.terminated = true
	return nil
}

// emitTextDelta serializes and appends one semantic Chat chunk frame carrying visible delta.content text.
func (e *StreamEncoder) emitTextDelta(text string, frames *[]sse.Frame) error {
	raw, err := e.marshalDeltaChunk(wireDeltaWrite{Content: &text}, nil)
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{Data: raw, Semantic: true, Terminal: false})
	return nil
}

// emitReasoningDelta serializes and appends one semantic Chat chunk frame carrying delta.reasoning_content text.
func (e *StreamEncoder) emitReasoningDelta(text string, frames *[]sse.Frame) error {
	raw, err := e.marshalDeltaChunk(wireDeltaWrite{ReasoningContent: &text}, nil)
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{Data: raw, Semantic: true, Terminal: false})
	return nil
}

// emitRefusalDelta serializes and appends one semantic Chat chunk frame carrying assistant delta.refusal text.
func (e *StreamEncoder) emitRefusalDelta(text string, frames *[]sse.Frame) error {
	raw, err := e.marshalDeltaChunk(wireDeltaWrite{Refusal: &text}, nil)
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{Data: raw, Semantic: true, Terminal: false})
	return nil
}

// emitToolArgsDelta appends a semantic follow-up tool_calls argument chunk
// carrying only index and function.arguments.
func (e *StreamEncoder) emitToolArgsDelta(toolIdx int, args string, frames *[]sse.Frame) error {
	raw, err := e.marshalDeltaChunk(wireDeltaWrite{
		ToolCalls: []wireToolCallWrite{{
			Index: &toolIdx,
			Function: wireFunctionCallWrite{
				Arguments: &args,
			},
		}},
	}, nil)
	if err != nil {
		return err
	}
	*frames = append(*frames, sse.Frame{Data: raw, Semantic: true, Terminal: false})
	return nil
}

// marshalDeltaChunk serializes one single-choice Chat streaming chunk envelope using locked stream metadata.
func (e *StreamEncoder) marshalDeltaChunk(delta wireDeltaWrite, finishReason *string) ([]byte, error) {
	return marshalDeterministic(wireStreamChunkWrite{
		ID:          e.id,
		Object:      chatObjectChunk,
		Created:     e.created,
		Model:       e.model,
		ServiceTier: e.serviceTier,
		Choices: []wireChunkChoiceWrite{{
			Index:        0,
			Delta:        delta,
			FinishReason: finishReason,
		}},
	})
}
