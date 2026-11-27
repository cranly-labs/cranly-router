package ir

// event.go defines the sequential streaming event model emitted by upstream
// stream decoders and consumed by sequencers, accumulators, and client stream
// encoders. Stream decoders serialize interleaved upstream chunks so at most one
// part is open at any moment and each part_end carries a complete Part snapshot.

const (
	// EventKind values tag the lifecycle, content-part, usage, and terminal
	// stages of a canonical IR event stream.
	EventResponseStart EventKind = "response_start" // unique initial stream metadata event
	EventPartStart     EventKind = "part_start"     // opens the next sequential output part
	EventPartDelta     EventKind = "part_delta"     // appends text or argument fragment to open part
	EventPartEnd       EventKind = "part_end"       // closes open part with its complete snapshot
	EventUsage         EventKind = "usage"          // delivers a merged cumulative Usage snapshot
	EventResponseEnd   EventKind = "response_end"   // normal terminal completion event
	EventError         EventKind = "error"          // abnormal terminal stream error event
)

// EventKind identifies the variant carried by an Event value.
type EventKind string

// Event represents one step in a canonical sequential response stream.
type Event struct {
	Kind        EventKind     `json:"kind"`                  // event variant tag
	Response    *ResponseMeta `json:"response,omitzero"`     // metadata for EventResponseStart
	Index       int           `json:"index,omitzero"`        // zero-based sequential part index
	Part        *Part         `json:"part,omitzero"`         // skeleton on part_start, full snapshot on part_end
	Delta       *Delta        `json:"delta,omitzero"`        // incremental fragment on part_delta
	Usage       *Usage        `json:"usage,omitzero"`        // merged usage snapshot on usage
	Finish      *Finish       `json:"finish,omitzero"`       // completion reason on response_end
	ServiceTier string        `json:"service_tier,omitzero"` // terminal-frame service tier on response_end
	Err         *StreamError  `json:"error,omitzero"`        // error payload on error
}

// ResponseMeta carries envelope metadata announced when a stream begins.
type ResponseMeta struct {
	ID          string `json:"id,omitzero"`           // upstream response identifier
	Model       string `json:"model,omitzero"`        // upstream model identifier
	CreatedAt   int64  `json:"created_at,omitzero"`   // Unix creation timestamp in seconds
	ServiceTier string `json:"service_tier,omitzero"` // initial upstream service tier
}

// Delta holds incremental text or tool-call argument fragments for an open Part.
type Delta struct {
	Text      string `json:"text,omitzero"`      // text, refusal, or reasoning fragment
	Arguments string `json:"arguments,omitzero"` // function or custom tool argument fragment
}

// StreamError describes a protocol or transport failure that terminates a stream.
type StreamError struct {
	Type    string `json:"type,omitzero"`   // protocol error category
	Code    string `json:"code,omitzero"`   // machine-readable error code
	Message string `json:"message"`         // human-readable error message
	Status  int    `json:"status,omitzero"` // optional HTTP-equivalent status code
}

// IsValid reports whether k belongs to the closed set of canonical EventKind values.
func (k EventKind) IsValid() bool {
	switch k {
	case EventResponseStart, EventPartStart, EventPartDelta, EventPartEnd,
		EventUsage, EventResponseEnd, EventError:
		return true
	default:
		return false
	}
}

// IsTerminal reports whether k ends a stream via normal completion or an error.
func (k EventKind) IsTerminal() bool {
	return k == EventResponseEnd || k == EventError
}

// Clone returns a detached copy of d, or nil when d is nil. Callers can modify
// the returned Delta struct without mutating buffered or previously emitted events.
func (d *Delta) Clone() *Delta {
	if d == nil {
		return nil
	}
	cp := *d
	return &cp
}

// Error returns e's human-readable Message string so *StreamError satisfies
// the standard error interface when passed as a cause to Stream.Close. Nil
// receivers safely return an empty string without panicking.
func (e *StreamError) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

// Clone returns an independent deep copy of e across Response, Part, Delta,
// Usage, Finish, and StreamError branches. Buffered sequencer slots and stream
// accumulators can retain or mutate the copy without sharing underlying storage.
func (e Event) Clone() Event {
	out := e
	if e.Response != nil {
		meta := *e.Response
		out.Response = &meta
	}
	if e.Part != nil {
		part := e.Part.Clone()
		out.Part = &part
	}
	out.Delta = e.Delta.Clone()
	out.Usage = e.Usage.Clone()
	if e.Finish != nil {
		fin := *e.Finish
		out.Finish = &fin
	}
	if e.Err != nil {
		se := *e.Err
		out.Err = &se
	}
	return out
}
