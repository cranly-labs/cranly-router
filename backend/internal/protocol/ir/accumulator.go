package ir

// accumulator.go folds a canonical sequential Event stream into a single
// buffered Response value. Non-streaming client encoders and streaming-to-JSON
// gateway buffers share this folding step so upstream protocols forced into SSE
// streaming still produce the same Response structure as native JSON replies.
//
// Canonical stream invariant 4 guarantees that each EventPartEnd carries a
// complete Part snapshot, while invariant 5 guarantees that each EventUsage
// carries a merged cumulative token snapshot. Accumulator therefore collects
// closed part snapshots in index order, retains the latest usage snapshot and
// response metadata, and records terminal finish or stream-error state without
// protocol-specific delta reconstruction.

// Accumulator incrementally folds canonical IR stream Events into a detached Response.
// Its zero value is ready for use without calling NewAccumulator.
type Accumulator struct {
	id          string       // latest non-empty response identifier from response_start
	model       string       // latest non-empty model identifier from response_start
	createdAt   int64        // latest non-zero Unix creation timestamp
	serviceTier string       // latest non-empty service tier from start or terminal frame
	parts       []Part       // completed Part snapshots in sequential part_end order
	usage       *Usage       // latest cumulative Usage snapshot
	finish      Finish       // terminal completion reason
	err         *StreamError // terminal StreamError when ended by EventError
	finished    bool         // true once EventResponseEnd or EventError was observed
}

// NewAccumulator constructs an empty stateful Accumulator ready to fold
// sequential stream Events into a buffered Response. Callers must allocate a
// separate Accumulator instance for each upstream stream attempt.
func NewAccumulator() *Accumulator {
	return &Accumulator{}
}

// Accumulate validates events with ValidateStream and folds them into a complete
// buffered Response. Invalid event sequences return nil alongside the validation
// error; valid EventError terminations return a Response with FinishError.
func Accumulate(events []Event) (*Response, error) {
	if err := ValidateStream(events); err != nil {
		return nil, err
	}
	var acc Accumulator
	acc.Add(events...)
	return acc.Response(), nil
}

// Add folds one or more sequential Events into a, cloning retained Part, Usage,
// Finish, and StreamError values so caller mutations never alias accumulator state.
// Events arriving after a terminal EventResponseEnd or EventError are ignored.
func (a *Accumulator) Add(events ...Event) {
	for _, ev := range events {
		if a.finished {
			return
		}
		switch ev.Kind {
		case EventResponseStart:
			if ev.Response == nil {
				continue
			}
			if ev.Response.ID != "" {
				a.id = ev.Response.ID
			}
			if ev.Response.Model != "" {
				a.model = ev.Response.Model
			}
			if ev.Response.CreatedAt != 0 {
				a.createdAt = ev.Response.CreatedAt
			}
			if ev.Response.ServiceTier != "" {
				a.serviceTier = ev.Response.ServiceTier
			}

		case EventPartEnd:
			if ev.Part != nil {
				a.parts = append(a.parts, ev.Part.Clone())
			}

		case EventUsage:
			if ev.Usage != nil {
				a.usage = ev.Usage.Clone()
			}

		case EventResponseEnd:
			a.finished = true
			if ev.Finish != nil {
				a.finish = *ev.Finish
			}
			if ev.ServiceTier != "" {
				a.serviceTier = ev.ServiceTier
			}

		case EventError:
			a.finished = true
			if a.finish.Reason == "" {
				a.finish = Finish{Reason: FinishError}
			}
			if ev.Err != nil {
				cp := *ev.Err
				a.err = &cp
			}
		}
	}
}

// Response returns an independent deep copy of the folded Response assembled so
// far, populating an empty non-nil Parts slice when zero parts have completed.
// Subsequent Add calls do not mutate previously returned Response snapshots.
func (a *Accumulator) Response() *Response {
	parts := make([]Part, len(a.parts))
	for i, p := range a.parts {
		parts[i] = p.Clone()
	}
	return &Response{
		ID:          a.id,
		Model:       a.model,
		CreatedAt:   a.createdAt,
		Parts:       parts,
		Finish:      a.finish,
		Usage:       a.usage.Clone(),
		ServiceTier: a.serviceTier,
	}
}

// StreamError returns a detached copy of the terminal StreamError when the
// stream ended via EventError, or nil when no stream error occurred. Callers
// may mutate the returned struct without affecting accumulator state.
func (a *Accumulator) StreamError() *StreamError {
	if a.err == nil {
		return nil
	}
	cp := *a.err
	return &cp
}

// Finished reports whether a has observed a terminal EventResponseEnd or EventError.
func (a *Accumulator) Finished() bool {
	return a.finished
}
