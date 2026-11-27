package convert

import (
	"slices"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream.go implements incremental SSE frame conversion from an upstream wire
// protocol to a client wire protocol for one Exchange attempt. Each upstream
// frame decodes into sequential IR events, stamps upstream token schemes,
// restores lowered client tool calls, seals cross-protocol trc1 continuity
// envelopes, updates stream Summary telemetry, and encodes client SSE frames
// annotated with structured Semantic and Terminal flags.
//
// Upstream frames can arrive fragmented or terminate abruptly, so Stream holds
// per-attempt decoder, raiser, sealer, encoder, and outputObserver state until
// Close runs once. Response-side losses accumulate locally during streaming and
// commit onto the owning Exchange at closure so concurrent readers of
// Exchange.Losses never observe half-written stream slices.

// Summary captures stream lifecycle metadata, terminal completion state,
// client-visible merged token usage, and content presence flags observed by Stream.
type Summary struct {
	ID          string          // upstream or synthesized response identifier
	Model       string          // upstream reported model identifier
	ServiceTier string          // latest non-empty processing service tier
	Finish      ir.Finish       // terminal completion reason and stop sequence
	Usage       *ir.Usage       // client-visible merged token usage snapshot
	HasContent  bool            // true when text, reasoning, refusal, or tool call occurred
	HasText     bool            // true when visible PartText content occurred
	HasToolCall bool            // true when at least one non-hosted PartToolCall occurred
	Err         *ir.StreamError // terminal error details when stream ended via EventError
}

// Stream converts incoming upstream SSE frames into outgoing client SSE frames
// for a single Exchange attempt. Callers serialize Write and Close calls on one Stream.
type Stream struct {
	exchange *Exchange            // owning Exchange holding route, codecs, and ToolMap
	decoder  StreamDecoder        // upstream protocol SSE frame decoder
	encoder  StreamEncoder        // client protocol IR event encoder
	raiser   *toolStreamRaiser    // stateful lowered tool identity restorer
	sealer   *streamCarrierSealer // stateful trc1 continuity envelope sealer
	observer outputObserver       // structured Semantic/Terminal and content tracker
	summary  Summary              // accumulated metadata, usage, and finish state
	losses   []ir.Loss            // cumulative response-side Loss entries
	closed   bool                 // whether Close has already been invoked
}

// newStream allocates a fresh Stream bound to ex.
func newStream(ex *Exchange) *Stream {
	return &Stream{
		exchange: ex,
		decoder:  ex.upCodec.NewStreamDecoder(),
		encoder:  ex.clientCodec.NewStreamEncoder(ex.clientView()),
		raiser:   newToolStreamRaiser(ex.toolMap),
		sealer:   newStreamCarrierSealer(ex.route, ex.calls, ex.runtime),
	}
}

// Write decodes one upstream SSE frame and returns zero or more converted
// client SSE frames with Semantic and Terminal flags populated. Writes after
// Close return nil without emitting further frames.
func (s *Stream) Write(frame sse.Frame) ([]sse.Frame, error) {
	if s.closed {
		return nil, nil
	}
	if s.exchange.upstreamErr != nil {
		return nil, s.exchange.upstreamErr
	}
	events, err := s.decoder.Decode(frame)
	if err != nil {
		return nil, err
	}
	frames, err := s.processEvents(events)
	if err != nil {
		return nil, err
	}
	s.drainDecoderRevisions()
	return frames, nil
}

// Close finalizes the upstream stream decoder (passing nil on normal EOF or a
// non-nil transport/timeout cause on abnormal disconnect) and flushes any
// trailing client completion or error frames. Repeated Close calls return nil.
func (s *Stream) Close(cause error) ([]sse.Frame, error) {
	if s.closed {
		return nil, nil
	}
	s.closed = true
	if s.exchange.upstreamErr != nil {
		return nil, s.exchange.upstreamErr
	}
	tail := s.decoder.Close(cause)
	frames, err := s.processEvents(tail)
	if err != nil {
		return nil, err
	}
	s.drainDecoderRevisions()
	s.exchange.appendLosses(s.losses)
	return frames, nil
}

// Summary returns a detached snapshot of response metadata, finish reason,
// client-visible Usage, and content flags observed so far on s. Nested Usage
// and StreamError pointers are deep-cloned so callers can inspect them safely.
func (s *Stream) Summary() Summary {
	out := s.summary
	out.Usage = s.summary.Usage.Clone()
	if s.summary.Err != nil {
		errCopy := *s.summary.Err
		out.Err = &errCopy
	}
	out.HasContent = s.observer.hasContent
	out.HasText = s.observer.hasText
	out.HasToolCall = s.observer.hasToolCall
	return out
}

// Losses returns a detached slice of all response-side ir.Loss records emitted
// so far on s. Mutating the returned slice never alters s's internal loss log.
func (s *Stream) Losses() []ir.Loss {
	return slices.Clone(s.losses)
}

// processEvents runs token tagging, optional upstream tool-name reverse mapping,
// tool raising, carrier sealing, summary observation, and client frame encoding
// across events in arrival order. Empty event slices return (nil, nil), and
// incurred losses accumulate on s.losses.
func (s *Stream) processEvents(events []ir.Event) ([]sse.Frame, error) {
	if len(events) == 0 {
		return nil, nil
	}
	tagUpstreamEventTokens(events, s.exchange.route.Dialect)
	mapped := mapUpstreamEventToolNames(events, s.exchange.mapUpstreamToolName)
	raised := s.raiser.Raise(mapped)
	sealed, err := s.sealer.sealEvents(raised, &s.losses)
	if err != nil {
		return nil, err
	}

	var out []sse.Frame
	for _, ev := range sealed {
		s.recordSummaryEvent(ev)
		batch, encLosses, err := s.encoder.Encode(ev)
		if err != nil {
			return nil, err
		}
		s.observer.observeAndAnnotate(ev, batch)
		if len(encLosses) > 0 {
			s.losses = remapResponseLosses(s.losses, encLosses, s.sealer.outPartToOrig)
		}
		if len(batch) > 0 {
			out = append(out, batch...)
		}
	}
	return out, nil
}

// drainDecoderRevisions applies any late tool-call signature revisions buffered
// by s.decoder onto s.sealer's CallMemory after event sealing completes. Decoders
// that do not implement lateToolSignatureDrainer are skipped without allocation.
func (s *Stream) drainDecoderRevisions() {
	if drainer, ok := s.decoder.(lateToolSignatureDrainer); ok {
		s.sealer.applyLateToolSignatures(drainer.drainLateToolSignatures())
	}
}

// recordSummaryEvent updates s.summary metadata, usage, finish, and error fields from ev.
func (s *Stream) recordSummaryEvent(ev ir.Event) {
	switch ev.Kind {
	case ir.EventResponseStart:
		if ev.Response != nil {
			if ev.Response.ID != "" {
				s.summary.ID = ev.Response.ID
			}
			if ev.Response.Model != "" {
				s.summary.Model = ev.Response.Model
			}
			if ev.Response.ServiceTier != "" {
				s.summary.ServiceTier = ev.Response.ServiceTier
			}
		}

	case ir.EventUsage:
		if ev.Usage != nil {
			s.summary.Usage = ev.Usage.Clone()
		}

	case ir.EventResponseEnd:
		if ev.Finish != nil {
			s.summary.Finish = *ev.Finish
		}
		if ev.ServiceTier != "" {
			s.summary.ServiceTier = ev.ServiceTier
		}

	case ir.EventError:
		if s.summary.Finish.Reason == "" {
			s.summary.Finish = ir.Finish{Reason: ir.FinishError}
		}
		if ev.Err != nil {
			cp := *ev.Err
			s.summary.Err = &cp
		}
	}
}
