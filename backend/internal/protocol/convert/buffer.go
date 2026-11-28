package convert

import (
	"errors"
	"fmt"
	"slices"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// buffer.go folds upstream SSE stream frames into a single buffered client JSON
// response body through ir.Accumulator and clientCodec.EncodeResponse. Upstreams
// such as OpenAI Responses on Chat-origin conversions always stream SSE frames
// even when the downstream client requested a non-streaming JSON completion;
// Buffer shares the exact same stream decoding, tool raising, and trc1 carrier
// sealing stages as Stream before serializing the accumulated ir.Response.

// Buffer consumes upstream SSE frames and produces one complete client JSON
// response body when Close succeeds.
type Buffer struct {
	exchange *Exchange            // owning Exchange holding route, codecs, and ToolMap
	decoder  StreamDecoder        // upstream protocol SSE frame decoder
	raiser   *toolStreamRaiser    // stateful lowered tool identity restorer
	sealer   *streamCarrierSealer // stateful trc1 continuity envelope sealer
	acc      ir.Accumulator       // canonical event-to-Response accumulator
	lateRevs []lateToolSignature  // late tool-call signatures applied before encoding
	losses   []ir.Loss            // cumulative response-side Loss entries
	closed   bool                 // whether Close has already been invoked
}

// newBuffer constructs a stateful SSE-to-JSON Buffer bound to ex.
func newBuffer(ex *Exchange) *Buffer {
	return &Buffer{
		exchange: ex,
		decoder:  ex.upCodec.NewStreamDecoder(),
		raiser:   newToolStreamRaiser(ex.toolMap),
		sealer:   newStreamCarrierSealer(ex.route, ex.calls, ex.runtime),
	}
}

// Write decodes one upstream SSE frame and folds the resulting raised and
// sealed IR events into b's response accumulator. Writes invoked after Close
// or after a request preparation failure return an error immediately.
func (b *Buffer) Write(frame sse.Frame) error {
	if b.closed {
		return errors.New("convert: write to closed Buffer")
	}
	if b.exchange.upstreamErr != nil {
		return b.exchange.upstreamErr
	}
	events, err := b.decoder.Decode(frame)
	if err != nil {
		return err
	}
	if err := b.foldEvents(events); err != nil {
		return err
	}
	b.drainDecoderRevisions()
	return nil
}

// Close finalizes the upstream stream decoder with cause and encodes the folded
// ir.Response into client wire JSON bytes. Non-nil cause errors or upstream
// stream EventError terminations return an error rather than emitting truncated
// JSON, while still committing any response-side losses observed before closure
// onto the owning Exchange.
func (b *Buffer) Close(cause error) ([]byte, error) {
	if b.closed {
		return nil, errors.New("convert: Buffer already closed")
	}
	b.closed = true
	if b.exchange.upstreamErr != nil {
		return nil, b.exchange.upstreamErr
	}
	// Commit accumulated response-side losses onto the owning Exchange even when
	// stream termination fails so diagnostic accounting reflects observed work.
	defer func() {
		b.exchange.appendLosses(b.losses)
	}()

	tail := b.decoder.Close(cause)
	if err := b.foldEvents(tail); err != nil {
		return nil, err
	}
	b.drainDecoderRevisions()
	if cause != nil {
		return nil, cause
	}
	if streamErr := b.acc.StreamError(); streamErr != nil {
		if streamErr.Type != "" {
			return nil, fmt.Errorf("upstream stream error (%s): %s", streamErr.Type, streamErr.Message)
		}
		return nil, fmt.Errorf("upstream stream error: %s", streamErr.Message)
	}

	resp := b.acc.Response()
	b.patchResponseLateSignatures(resp)
	encoded, encLosses, err := b.exchange.clientCodec.EncodeResponse(resp, b.exchange.clientView())
	if err != nil {
		return nil, err
	}
	b.losses = remapResponseLosses(b.losses, encLosses, b.sealer.outPartToOrig)
	return encoded, nil
}

// Losses returns a detached slice of all response-side ir.Loss records emitted
// so far on b. Mutating the returned slice never alters b's internal loss log.
func (b *Buffer) Losses() []ir.Loss {
	return slices.Clone(b.losses)
}

// foldEvents runs token tagging, optional upstream tool-name reverse mapping,
// tool raising, carrier sealing, and Accumulator.Add across events in arrival
// order. Empty slices return nil without mutating accumulator state, while
// carrier losses append onto b.losses.
func (b *Buffer) foldEvents(events []ir.Event) error {
	if len(events) == 0 {
		return nil
	}
	tagUpstreamEventTokens(events, b.exchange.route.Dialect)
	mapped := mapUpstreamEventToolNames(events, b.exchange.mapUpstreamToolName)
	raised := b.raiser.Raise(mapped)
	sealed, err := b.sealer.sealEvents(raised, &b.losses)
	if err != nil {
		return err
	}
	b.acc.Add(sealed...)
	return nil
}

// drainDecoderRevisions refreshes b.sealer's CallMemory and appends late
// tool-call signature revisions to b.lateRevs for patching onto b.acc's folded
// Response. Decoders without late signature queues return immediately.
func (b *Buffer) drainDecoderRevisions() {
	drainer, ok := b.decoder.(lateToolSignatureDrainer)
	if !ok {
		return
	}
	revs := drainer.drainLateToolSignatures()
	if len(revs) == 0 {
		return
	}
	b.sealer.applyLateToolSignatures(revs)
	b.lateRevs = append(b.lateRevs, revs...)
}

// patchResponseLateSignatures attaches late thoughtSignature tokens observed
// during SSE streaming onto their corresponding closed PartToolCall entries on
// resp. Nil responses, empty revision queues, or out-of-range part indices are
// ignored safely.
func (b *Buffer) patchResponseLateSignatures(resp *ir.Response) {
	if resp == nil || len(b.lateRevs) == 0 {
		return
	}
	scheme := b.exchange.route.Dialect.ReplayScheme
	pid := int64(0)
	if ir.IsAccountBoundScheme(scheme) && b.exchange.route.Dialect.ProviderID > 0 {
		pid = b.exchange.route.Dialect.ProviderID
	}
	for _, rev := range b.lateRevs {
		if rev.signature == "" {
			continue
		}
		targetIdx := rev.partIndex
		if mapped, ok := b.sealer.origToOutIdx[rev.partIndex]; ok {
			targetIdx = mapped
		}
		if targetIdx < 0 || targetIdx >= len(resp.Parts) {
			continue
		}
		p := &resp.Parts[targetIdx]
		if p.Kind == ir.PartToolCall && p.Call != nil {
			p.Call.Token = &ir.ReplayToken{
				Scheme:     scheme,
				ProviderID: pid,
				Data:       rev.signature,
			}
		}
	}
}
