package qoder

import (
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// output_observation.go forwards sink commitment queries and records downstream
// write failures around the execution sink. InitialOutput and OutputState expose
// pre-existing HTTP and content commitment to standard and deferred writers,
// while failed Begin or Emit calls retain disconnect evidence so Execute can
// report client disconnection after upstream usage collection finishes.

// executionOutput records disconnect evidence and forwards commitment queries
// around one serialized output sink.
type executionOutput struct {
	sink upstream.OutputSink // downstream output sink

	// disconnected records a failed Begin or Emit even when upstream collection
	// continues. Execute copies this observation into completion's
	// client-disconnect evidence.
	disconnected bool // failed downstream write
}

// Begin forwards output headers and records whether the downstream sink rejects
// them. A failed header write marks disconnect before returning its error,
// allowing Execute to retain that observation during completion handling.
func (o *executionOutput) Begin(h upstream.OutputHead) error {
	err := o.sink.Begin(h)
	if err != nil {
		o.disconnected = true
	}
	return err
}

// Emit forwards one structured output event to the downstream sink and records
// disconnect state when the write fails. Callers supply Semantic and Terminal
// flags directly on event before calling Emit.
func (o *executionOutput) Emit(event upstream.OutputEvent) error {
	err := o.sink.Emit(event)
	if err != nil {
		o.disconnected = true
	}
	return err
}

// InitialOutput returns existing sink commitment metadata when the sink exposes
// it. Sinks lacking the optional source interface produce a zero head, so the
// conversion writer starts from its normal defaults.
func (o *executionOutput) InitialOutput() upstream.OutputHead {
	if source, ok := o.sink.(interface {
		// InitialOutput reports response commitment and headers already selected by
		// the sink. The conversion writer reads that metadata at initialization and
		// retains the status when the response is committed.
		InitialOutput() upstream.OutputHead
	}); ok {
		return source.InitialOutput()
	}
	return upstream.OutputHead{}
}

// OutputState forwards live commitment metadata from o.sink without initializing
// deferred response headers. Sinks that do not implement the optional state
// interface return a zero OutputHead so uninitialized writers report uncommitted output.
func (o *executionOutput) OutputState() upstream.OutputHead {
	if state, ok := o.sink.(interface {
		// OutputState reports current HTTP and content commitment without mutating
		// response headers or stopping waiting keepalive heartbeats.
		OutputState() upstream.OutputHead
	}); ok {
		return state.OutputState()
	}
	return upstream.OutputHead{}
}
