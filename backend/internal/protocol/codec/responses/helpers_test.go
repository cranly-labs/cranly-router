package responses

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// decodeFullSSEStream feeds raw SSE bytes through sse.NewReader and
// StreamDecoder, returning the complete canonical ir.Event slice. Any decoder
// or reader error aborts the stream via Close so abnormal terminations stay testable.
func decodeFullSSEStream(t *testing.T, raw []byte) []ir.Event {
	t.Helper()
	reader := sse.NewReader(bytes.NewReader(raw))
	dec := NewStreamDecoder()
	var events []ir.Event
	for reader.Scan() {
		batch, err := dec.Decode(reader.Frame())
		if err != nil {
			events = append(events, dec.Close(err)...)
			return events
		}
		events = append(events, batch...)
	}
	events = append(events, dec.Close(reader.Err())...)
	return events
}

// encodeFullEventStream feeds events sequentially through a StreamEncoder bound
// to view and returns all emitted SSE frames alongside accumulated ir.Loss entries.
// Any encoding error fails t immediately via require.NoError.
func encodeFullEventStream(t *testing.T, view ir.ClientView, events []ir.Event) ([]sse.Frame, []ir.Loss) {
	t.Helper()
	enc := NewStreamEncoder(view)
	var frames []sse.Frame
	var losses []ir.Loss
	for _, ev := range events {
		fBatch, lBatch, err := enc.Encode(ev)
		require.NoError(t, err)
		frames = append(frames, fBatch...)
		losses = append(losses, lBatch...)
	}
	return frames, losses
}

// formatSSEFrames serializes frames into wire SSE bytes through sse.Append.
func formatSSEFrames(frames []sse.Frame) []byte {
	var out []byte
	for _, f := range frames {
		out = sse.Append(out, f)
	}
	return out
}

// newDeterministicRuntime returns an ir.Runtime seeded with a fixed Unix
// timestamp and sequential counter-backed identifiers. Calling it separately for
// buffered and streaming encoders yields matching synthetic wire IDs across runs.
func newDeterministicRuntime() ir.Runtime {
	seq := 0
	fixedTime := time.Unix(1760000000, 0).UTC()
	return ir.Runtime{
		Now: func() time.Time {
			return fixedTime
		},
		NewID: func(prefix string) string {
			seq++
			return fmt.Sprintf("%sdet_%d", prefix, seq)
		},
	}
}
