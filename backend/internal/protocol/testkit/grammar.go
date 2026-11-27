package testkit

import (
	"fmt"
	"strconv"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// grammar.go defines shared SSE stream grammar violation types, options, and
// protocol dispatchers together with lossless jsonNode field extractors. Each
// protocol grammar validator runs an independent state machine directly over
// wire SSE frames without calling protocol codecs so encoder bugs cannot mask
// matching decoder assumptions.

// Violation describes one stream grammar state-machine or schema rule failure
// on a sequence of SSE frames.
type Violation struct {
	Protocol   protocol.ProtocolID `json:"protocol"`       // validated client wire protocol
	Rule       string              `json:"rule"`           // stable machine-readable rule code
	FrameIndex int                 `json:"frame_index"`    // zero-based frame index, or -1 at stream EOF
	Event      string              `json:"event,omitzero"` // SSE event header or JSON event type
	Path       string              `json:"path,omitzero"`  // JSON path within frame Data when applicable
	Message    string              `json:"message"`        // human-readable explanation of the violation
}

// GrammarOptions controls whether stream validation requires normal completion
// or permits clean error termination after upstream truncation (law S7).
type GrammarOptions struct {
	// AllowAbnormal permits a stream to end with a protocol-valid error frame
	// after closing any open blocks or items, while still rejecting silent EOF
	// truncations and frames emitted after termination.
	AllowAbnormal bool
}

// String formats v as a concise single-line diagnostic suitable for test failures.
func (v Violation) String() string {
	loc := fmt.Sprintf("frame[%d]", v.FrameIndex)
	if v.FrameIndex < 0 {
		loc = "eof"
	}
	if v.Event != "" {
		loc += "(" + v.Event + ")"
	}
	if v.Path != "" {
		loc += " " + v.Path
	}
	return fmt.Sprintf("[%s/%s] %s: %s", v.Protocol, v.Rule, loc, v.Message)
}

// ValidateStreamGrammar runs the target protocol's SSE state-machine validator
// over frames under opts and returns all detected violations in emission order.
// Unrecognized protocol identifiers return a single unsupported_protocol
// violation rather than silently passing.
func ValidateStreamGrammar(proto protocol.ProtocolID, frames []sse.Frame, opts GrammarOptions) []Violation {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return ValidateMessagesStream(frames, opts)
	case protocol.ProtocolOpenAIResponses:
		return ValidateResponsesStream(frames, opts)
	case protocol.ProtocolOpenAIChatCompletions:
		return ValidateChatStream(frames, opts)
	case protocol.ProtocolGeminiGenerateContent:
		return ValidateGeminiStream(frames, opts)
	default:
		return []Violation{{
			Protocol:   proto,
			Rule:       "unsupported_protocol",
			FrameIndex: -1,
			Message:    fmt.Sprintf("no stream grammar validator registered for protocol %q", proto),
		}}
	}
}

// parseFrameJSONObject decodes data as a single JSON object without duplicate keys.
func parseFrameJSONObject(data []byte) (map[string]jsonNode, error) {
	node, err := parseJSONNode(data, false)
	if err != nil {
		return nil, err
	}
	if node.kind != nodeObject {
		return nil, fmt.Errorf("frame payload must be a JSON object")
	}
	return node.objVal, nil
}

// nodeStringField extracts a string member from obj, reporting (val, present, isString).
func nodeStringField(obj map[string]jsonNode, key string) (string, bool, bool) {
	val, present := obj[key]
	if !present {
		return "", false, false
	}
	if val.kind != nodeString {
		return "", true, false
	}
	return val.strVal, true, true
}

// nodeIntField extracts an exact integer member from obj, reporting (val, present, isInt).
func nodeIntField(obj map[string]jsonNode, key string) (int64, bool, bool) {
	val, present := obj[key]
	if !present {
		return 0, false, false
	}
	if val.kind != nodeNumber {
		return 0, true, false
	}
	if val.numVal.zero {
		return 0, true, true
	}
	if val.numVal.exp < 0 {
		return 0, true, false
	}
	parsed, err := strconv.ParseInt(val.numVal.canonicalString(), 10, 64)
	if err != nil {
		return 0, true, false
	}
	return parsed, true, true
}

// nodeObjectField extracts a nested JSON object member from obj, reporting (map, present, isObj).
func nodeObjectField(obj map[string]jsonNode, key string) (map[string]jsonNode, bool, bool) {
	val, present := obj[key]
	if !present {
		return nil, false, false
	}
	if val.kind != nodeObject {
		return nil, true, false
	}
	return val.objVal, true, true
}

// nodeArrayField extracts a nested JSON array member from obj, reporting (slice, present, isArr).
func nodeArrayField(obj map[string]jsonNode, key string) ([]jsonNode, bool, bool) {
	val, present := obj[key]
	if !present {
		return nil, false, false
	}
	if val.kind != nodeArray {
		return nil, true, false
	}
	return val.arrVal, true, true
}
