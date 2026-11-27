package ir

// response.go defines the buffered response intermediate representation,
// normalized completion finish reasons, and independent response cloning.
// Buffered upstream decoders and stream accumulators both produce Response
// values so non-streaming client encoders share one output representation
// regardless of whether the upstream answered in JSON or SSE.

const (
	// FinishReason values normalize protocol-specific stop indicators into a
	// closed set shared by Response and terminal stream Events.
	FinishStop          FinishReason = "stop"           // natural end of assistant turn
	FinishLength        FinishReason = "length"         // token or context window limit reached
	FinishToolCalls     FinishReason = "tool_calls"     // turn paused to execute tool calls
	FinishContentFilter FinishReason = "content_filter" // safety or refusal filter stopped output
	FinishStopSequence  FinishReason = "stop_sequence"  // custom stop sequence matched
	FinishPause         FinishReason = "pause"          // server-side pause_turn boundary
	FinishError         FinishReason = "error"          // abnormal error termination
	FinishOther         FinishReason = "other"          // unrecognized upstream finish reason
)

// FinishReason classifies why generation ended for a Response or stream.
type FinishReason string

// Response represents a complete normalized model reply assembled either from
// a non-streaming upstream payload or by folding a canonical Event stream.
type Response struct {
	ID          string `json:"id,omitzero"`           // upstream or synthesized response identifier
	Model       string `json:"model,omitzero"`        // upstream reported model identifier
	CreatedAt   int64  `json:"created_at,omitzero"`   // Unix creation timestamp in seconds
	Parts       []Part `json:"parts"`                 // ordered assistant output parts
	Finish      Finish `json:"finish"`                // normalized completion reason
	Usage       *Usage `json:"usage,omitzero"`        // observed token usage snapshot
	ServiceTier string `json:"service_tier,omitzero"` // upstream reported service tier
	Extra       Extra  `json:"extra,omitzero"`        // unmodeled response-level members
}

// Finish pairs a normalized FinishReason with any matched stop sequence text.
type Finish struct {
	Reason       FinishReason `json:"reason"`                 // normalized stop category
	StopSequence string       `json:"stop_sequence,omitzero"` // matched stop string when Reason is stop_sequence
}

// IsValid reports whether r belongs to the closed set of canonical FinishReason values.
func (r FinishReason) IsValid() bool {
	switch r {
	case FinishStop, FinishLength, FinishToolCalls, FinishContentFilter,
		FinishStopSequence, FinishPause, FinishError, FinishOther:
		return true
	default:
		return false
	}
}

// Clone returns an independent deep copy of r with cloned Parts, Usage, and
// Extra maps. Nil receivers return nil, while non-nil empty Parts slices retain
// their non-nil shape for JSON array serialization.
func (r *Response) Clone() *Response {
	if r == nil {
		return nil
	}
	out := *r
	if r.Parts != nil {
		out.Parts = make([]Part, len(r.Parts))
		for i, p := range r.Parts {
			out.Parts[i] = p.Clone()
		}
	}
	out.Usage = r.Usage.Clone()
	out.Extra = r.Extra.Clone()
	return &out
}
