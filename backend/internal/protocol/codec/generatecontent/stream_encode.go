package generatecontent

import (
	"encoding/json/jsontext"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream_encode.go incrementally encodes canonical sequential IR Event streams
// into client-facing Google Gemini GenerateContent SSE frames. Gemini SSE
// streams emit self-contained GenerateContentResponse JSON objects on unnamed
// data: frames without [DONE] sentinels, carrying incremental text and thought
// parts on intermediate frames and finishReason on the terminal frame.
//
// Buffering envelope metadata until the first content or terminal event avoids
// emitting empty candidate frames that Gemini SDKs reject. Tracking whether
// visible deltas already streamed lets encodePartEnd append trailing signature
// frames without duplicating text, emit complete tool calls atomically, and keep
// Loss records identical to buffered EncodeResponse under Law S6.

// StreamEncoder encodes canonical sequential IR events into client-facing
// Gemini GenerateContent SSE frames for mockupstream and Law S3/S6/S7 tests.
type StreamEncoder struct {
	view         ir.ClientView // client model presentation and fallback ID generator
	responseID   string        // presented responseId carried on every emitted frame
	modelVersion string        // presented modelVersion carried on every emitted frame
	started      bool          // whether response metadata has been initialized
	terminated   bool          // whether a terminal finishReason or error frame emitted
	// openPart and emittedDelta track the active part between EventPartStart and
	// EventPartEnd so encodePartEnd knows whether incremental text or thought
	// frames already streamed before final metadata and signatures arrive.
	openPart     *ir.Part  // skeleton Part captured at EventPartStart
	emittedDelta bool      // whether openPart already emitted at least one delta frame
	lastUsage    *ir.Usage // latest cumulative Usage snapshot awaiting response_end
}

// NewStreamEncoder allocates a fresh stateful StreamEncoder configured with view
// for one downstream Gemini GenerateContent SSE response stream. Callers must
// construct an independent encoder per stream so part and termination state stay isolated.
func NewStreamEncoder(view ir.ClientView) *StreamEncoder {
	return &StreamEncoder{view: view}
}

// Encode converts one sequential IR event into zero or more Gemini SSE frames
// and any incurred information-loss records. Events supplied after a terminal
// response_end or error event return nil slices without emitting further frames.
//
// Response metadata buffers on EventResponseStart until the first content or
// terminal frame so StreamEncoder never emits an empty GenerateContentResponse
// envelope. Text and non-redacted thought deltas stream immediately as single-part
// frames, tool calls emit atomically at EventPartEnd, and EventResponseEnd writes
// finishReason alongside the latest cumulative usageMetadata snapshot.
func (e *StreamEncoder) Encode(event ir.Event) ([]sse.Frame, []ir.Loss, error) {
	if e.terminated {
		return nil, nil, nil
	}

	var losses []ir.Loss

	switch event.Kind {
	case ir.EventResponseStart:
		e.ensureEnvelopeMeta(event.Response)
		return nil, nil, nil

	case ir.EventPartStart:
		e.ensureEnvelopeMeta(nil)
		if event.Part != nil {
			cloned := event.Part.Clone()
			e.openPart = &cloned
		} else {
			e.openPart = nil
		}
		e.emittedDelta = false
		return nil, nil, nil

	case ir.EventPartDelta:
		e.ensureEnvelopeMeta(nil)
		if e.openPart == nil || event.Delta == nil || event.Delta.Text == "" {
			return nil, nil, nil
		}
		switch e.openPart.Kind {
		case ir.PartText, ir.PartRefusal:
			txt := event.Delta.Text
			frame, err := e.marshalSinglePartFrame(wirePart{Text: &txt})
			if err != nil {
				return nil, nil, err
			}
			e.emittedDelta = true
			return []sse.Frame{frame}, nil, nil

		case ir.PartReasoning:
			if e.openPart.Reasoning != nil && e.openPart.Reasoning.Redacted {
				return nil, nil, nil
			}
			txt := event.Delta.Text
			frame, err := e.marshalSinglePartFrame(wirePart{
				Text:    &txt,
				Thought: true,
			})
			if err != nil {
				return nil, nil, err
			}
			e.emittedDelta = true
			return []sse.Frame{frame}, nil, nil

		default:
			return nil, nil, nil
		}

	case ir.EventPartEnd:
		e.ensureEnvelopeMeta(nil)
		hadDeltas := e.emittedDelta
		e.openPart = nil
		e.emittedDelta = false
		if event.Part == nil {
			return nil, nil, nil
		}
		return e.encodePartEnd(*event.Part, event.Index, hadDeltas, &losses)

	case ir.EventUsage:
		e.ensureEnvelopeMeta(nil)
		if event.Usage != nil {
			e.lastUsage = event.Usage.Clone()
		}
		return nil, nil, nil

	case ir.EventResponseEnd:
		e.ensureEnvelopeMeta(nil)
		e.terminated = true
		if event.ServiceTier != "" {
			losses = append(losses, ir.Loss{
				Kind:   ir.LossExtraDropped,
				Path:   "service_tier",
				Detail: event.ServiceTier,
			})
		}
		var fin ir.Finish
		if event.Finish != nil {
			fin = *event.Finish
		}
		finishStr := encodeFinishReason(fin, &losses)
		payload, err := marshalDeterministic(wireResponseWrite{
			ResponseID:   e.responseID,
			ModelVersion: e.modelVersion,
			Candidates: []wireCandidateWrite{{
				Index: 0,
				Content: &wireContent{
					Role:  geminiRoleModel,
					Parts: []jsontext.Value{},
				},
				FinishReason: finishStr,
			}},
			UsageMetadata: encodeGeminiUsage(e.lastUsage),
		})
		if err != nil {
			return nil, nil, err
		}
		return []sse.Frame{{Data: payload}}, losses, nil

	case ir.EventError:
		e.terminated = true
		errBytes, err := encodeStreamError(event.Err)
		if err != nil {
			return nil, nil, err
		}
		return []sse.Frame{{Data: errBytes}}, nil, nil

	default:
		return nil, nil, nil
	}
}

// ensureEnvelopeMeta initializes e.responseID and e.modelVersion once from meta
// and e.view so every emitted candidate frame carries consistent identifiers.
// Subsequent calls on an already-started StreamEncoder leave both fields unchanged.
func (e *StreamEncoder) ensureEnvelopeMeta(meta *ir.ResponseMeta) {
	if e.started {
		return
	}
	e.started = true
	var upID, upModel string
	if meta != nil {
		upID = meta.ID
		upModel = meta.Model
	}
	if upID != "" {
		e.responseID = upID
	} else {
		e.responseID = e.view.Runtime.NextID("gem_", "gem_0")
	}
	e.modelVersion = resolveClientModel(e.view, upModel)
}

// encodePartEnd emits any remaining completion frame and information-loss
// records for p at EventPartEnd, keeping Loss sequences identical to EncodeResponse.
//
// When visible text or thought deltas already streamed (hadDeltas==true),
// encodePartEnd suppresses duplicate text emission and writes a trailing empty-text
// frame only if Part.Token, Part.Extra, or Reasoning.Token supplied a
// thoughtSignature or vendor extension. Tool calls, media attachments, and
// same-origin opaque items emit one complete part frame unless an incomplete
// tool call carries truncated JSON.
func (e *StreamEncoder) encodePartEnd(
	p ir.Part,
	idx int,
	hadDeltas bool,
	losses *[]ir.Loss,
) ([]sse.Frame, []ir.Loss, error) {
	pPath := fmt.Sprintf("parts[%d]", idx)

	switch p.Kind {
	case ir.PartText, ir.PartRefusal:
		rawPart, keep, err := encodeSingleResponsePart(p, pPath, losses)
		if err != nil || !keep {
			return nil, *losses, err
		}
		if !hadDeltas {
			frame, err := e.marshalRawPartFrame(rawPart)
			if err != nil {
				return nil, nil, err
			}
			return []sse.Frame{frame}, *losses, nil
		}
		// When visible deltas already streamed, emit a trailing empty-text part
		// only if Part.Token or Part.Extra carried a thoughtSignature or vendor fields.
		var wp wirePart
		if err := unmarshalJSONObject(rawPart, &wp); err != nil {
			return nil, nil, err
		}
		if wp.ThoughtSignature == "" && len(wp.Unknown) == 0 {
			return nil, *losses, nil
		}
		empty := ""
		wp.Text = &empty
		frame, err := e.marshalSinglePartFrame(wp)
		if err != nil {
			return nil, nil, err
		}
		return []sse.Frame{frame}, *losses, nil

	case ir.PartReasoning:
		rawPart, keep, err := encodeSingleResponsePart(p, pPath, losses)
		if err != nil || !keep {
			return nil, *losses, err
		}
		if !hadDeltas {
			frame, err := e.marshalRawPartFrame(rawPart)
			if err != nil {
				return nil, nil, err
			}
			return []sse.Frame{frame}, *losses, nil
		}
		var wp wirePart
		if err := unmarshalJSONObject(rawPart, &wp); err != nil {
			return nil, nil, err
		}
		if wp.ThoughtSignature == "" && len(wp.Unknown) == 0 {
			return nil, *losses, nil
		}
		empty := ""
		wp.Text = &empty
		wp.Thought = true
		frame, err := e.marshalSinglePartFrame(wp)
		if err != nil {
			return nil, nil, err
		}
		return []sse.Frame{frame}, *losses, nil

	default:
		// Tool calls, media attachments, and same-origin opaque parts emit one
		// complete atomic part frame at EventPartEnd when kept.
		rawPart, keep, err := encodeSingleResponsePart(p, pPath, losses)
		if err != nil || !keep {
			return nil, *losses, err
		}
		// Skip incomplete tool calls emitted immediately before an EventError frame
		// when hadDeltas is irrelevant and incomplete calls precede abnormal termination.
		if p.Kind == ir.PartToolCall && p.Call != nil && p.Call.Incomplete && !isValidSingleJSONObject([]byte(strings.TrimSpace(p.Call.Arguments))) {
			return nil, *losses, nil
		}
		frame, err := e.marshalRawPartFrame(rawPart)
		if err != nil {
			return nil, nil, err
		}
		return []sse.Frame{frame}, *losses, nil
	}
}

// marshalSinglePartFrame serializes wp deterministically and wraps it inside a
// single-part GenerateContentResponse SSE frame carrying e.responseID and
// e.modelVersion. Serialization errors return an empty sse.Frame and the error.
func (e *StreamEncoder) marshalSinglePartFrame(wp wirePart) (sse.Frame, error) {
	partBytes, err := marshalDeterministic(wp)
	if err != nil {
		return sse.Frame{}, err
	}
	return e.marshalRawPartFrame(jsontext.Value(partBytes))
}

// marshalRawPartFrame wraps pre-serialized part JSON bytes inside
// candidates[0].content.parts on an unnamed data-only Gemini SSE frame. Every
// emitted frame populates responseId, modelVersion, and role="model", returning
// any serialization error alongside an empty sse.Frame.
func (e *StreamEncoder) marshalRawPartFrame(rawPart jsontext.Value) (sse.Frame, error) {
	payload, err := marshalDeterministic(wireResponseWrite{
		ResponseID:   e.responseID,
		ModelVersion: e.modelVersion,
		Candidates: []wireCandidateWrite{{
			Index: 0,
			Content: &wireContent{
				Role:  geminiRoleModel,
				Parts: []jsontext.Value{rawPart},
			},
		}},
	})
	if err != nil {
		return sse.Frame{}, err
	}
	return sse.Frame{Data: payload}, nil
}
