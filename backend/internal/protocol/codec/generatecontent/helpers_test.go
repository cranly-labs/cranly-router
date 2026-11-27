package generatecontent

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// newDeterministicRuntime returns a repeatable Runtime with a fixed Unix
// timestamp (1760000000) and sequential "<prefix><n>" response identifiers.
// Tests construct independent Runtime instances so counters never share state.
func newDeterministicRuntime() ir.Runtime {
	counter := 0
	return ir.Runtime{
		Now: func() time.Time {
			return time.Unix(1760000000, 0).UTC()
		},
		NewID: func(prefix string) string {
			counter++
			return fmt.Sprintf("%s%d", prefix, counter)
		},
	}
}

// decodeFullSSEStream scans rawSSE with sse.NewReader, feeds every frame into a
// fresh StreamDecoder, and appends Close(nil) to finalize the stream. Any SSE
// reader error or frame decode error fails tb immediately.
func decodeFullSSEStream(tb testing.TB, rawSSE []byte) []ir.Event {
	tb.Helper()
	rd := sse.NewReader(bytes.NewReader(rawSSE))
	dec := NewStreamDecoder()
	var events []ir.Event
	for rd.Scan() {
		batch, err := dec.Decode(rd.Frame())
		require.NoError(tb, err)
		events = append(events, batch...)
	}
	require.NoError(tb, rd.Err())
	events = append(events, dec.Close(nil)...)
	return events
}

// encodeFullEventStream feeds events sequentially through a fresh StreamEncoder
// configured with view and returns all emitted SSE frames and Loss records. Any
// encoding error fails tb immediately.
func encodeFullEventStream(tb testing.TB, view ir.ClientView, events []ir.Event) ([]sse.Frame, []ir.Loss) {
	tb.Helper()
	enc := NewStreamEncoder(view)
	var frames []sse.Frame
	var losses []ir.Loss
	for _, ev := range events {
		fBatch, lBatch, err := enc.Encode(ev)
		require.NoError(tb, err)
		frames = append(frames, fBatch...)
		losses = append(losses, lBatch...)
	}
	return frames, losses
}

// formatSSEFrames renders frames into wire SSE bytes via sse.Append.
func formatSSEFrames(frames []sse.Frame) []byte {
	var out []byte
	for _, f := range frames {
		out = sse.Append(out, f)
	}
	return out
}

// bytesEqual compares two byte slices without allocating diagnostic diffs on hot fuzz paths.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
