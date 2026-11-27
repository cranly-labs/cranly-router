package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// finish.go maps Anthropic Messages stop_reason and stop_sequence fields as well
// as SSE error frames to and from normalized IR completion states. Messages
// distinguishes natural end_turn, max_tokens and model_context_window_exceeded
// length truncations, stop_sequence matches, tool_use pauses, safety refusals,
// and server-side pause_turn boundaries, while representing mid-stream failures
// via dedicated event: error frames.

const (
	// defaultStreamErrorMessage and truncatedStreamErrorMessage supply non-empty
	// fallback diagnostics required by ir.ValidateStream on abnormal terminations.
	defaultStreamErrorMessage   = "upstream messages stream error"
	truncatedStreamErrorMessage = "upstream messages stream ended without message_stop"
)

// wireErrorPayload decodes the nested error object on a Messages error response or SSE frame.
type wireErrorPayload struct {
	Type    string         `json:"type,omitzero"`   // Anthropic error category such as overloaded_error
	Code    jsontext.Value `json:"code,omitzero"`   // optional string or numeric provider error code
	Message string         `json:"message"`         // human-readable error message
	Status  int            `json:"status,omitzero"` // optional HTTP status code
}

// wireErrorEventWrite serializes a terminal Anthropic Messages SSE error frame payload.
type wireErrorEventWrite struct {
	Type  string             `json:"type"`  // fixed "error" event discriminator
	Error wireErrorBodyWrite `json:"error"` // nested Anthropic error descriptor
}

// wireErrorBodyWrite serializes the nested error fields inside a terminal Messages error frame.
type wireErrorBodyWrite struct {
	Type    string `json:"type"`            // Anthropic error category label
	Code    string `json:"code,omitzero"`   // optional machine-readable error code
	Message string `json:"message"`         // non-empty client-facing error message
	Status  int    `json:"status,omitzero"` // optional HTTP status code
}

// decodeStopReason normalizes an Anthropic Messages stop_reason string and
// optional stop_sequence pointer into an ir.Finish value. When rawReason is
// blank on a completed reply, hasToolUse selects FinishToolCalls and its
// absence selects FinishStop.
func decodeStopReason(rawReason string, stopSeq *string, hasToolUse bool) ir.Finish {
	switch strings.TrimSpace(rawReason) {
	case "":
		if hasToolUse {
			return ir.Finish{Reason: ir.FinishToolCalls}
		}
		return ir.Finish{Reason: ir.FinishStop}
	case "end_turn":
		return ir.Finish{Reason: ir.FinishStop}
	case "max_tokens", "model_context_window_exceeded":
		return ir.Finish{Reason: ir.FinishLength}
	case "stop_sequence":
		var seq string
		if stopSeq != nil {
			seq = *stopSeq
		}
		return ir.Finish{
			Reason:       ir.FinishStopSequence,
			StopSequence: seq,
		}
	case "tool_use":
		return ir.Finish{Reason: ir.FinishToolCalls}
	case "refusal":
		return ir.Finish{Reason: ir.FinishContentFilter}
	case "pause_turn":
		return ir.Finish{Reason: ir.FinishPause}
	default:
		return ir.Finish{Reason: ir.FinishOther}
	}
}

// encodeStopReason maps a canonical ir.Finish value onto Anthropic Messages'
// stop_reason string and optional stop_sequence pointer. Blank Finish.Reason
// values infer "tool_use" when hasToolUse is true and "end_turn" otherwise.
func encodeStopReason(finish ir.Finish, hasToolUse bool) (string, *string) {
	switch finish.Reason {
	case "":
		if hasToolUse {
			return "tool_use", nil
		}
		return "end_turn", nil
	case ir.FinishLength:
		return "max_tokens", nil
	case ir.FinishToolCalls:
		return "tool_use", nil
	case ir.FinishContentFilter:
		return "refusal", nil
	case ir.FinishPause:
		return "pause_turn", nil
	case ir.FinishStopSequence:
		if finish.StopSequence != "" {
			seq := finish.StopSequence
			return "stop_sequence", &seq
		}
		return "stop_sequence", nil
	case ir.FinishStop, ir.FinishError, ir.FinishOther:
		return "end_turn", nil
	default:
		return "end_turn", nil
	}
}

// decodeStreamError parses a non-empty raw "error" member from a Messages
// response or SSE frame into an ir.StreamError with a non-empty Message. Absent
// or null values return nil, while scalar strings and malformed objects fall
// back to defaultStreamErrorMessage so EventError always satisfies ValidateStream.
func decodeStreamError(raw jsontext.Value) *ir.StreamError {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	if trimmed[0] == '"' {
		var msg string
		// Malformed or whitespace-only error strings substitute
		// defaultStreamErrorMessage so terminal EventError validation always passes.
		if unmarshalJSONValue(trimmed, &msg) == nil && strings.TrimSpace(msg) != "" {
			return &ir.StreamError{
				Type:    "api_error",
				Message: msg,
			}
		}
		return &ir.StreamError{
			Type:    "api_error",
			Message: defaultStreamErrorMessage,
		}
	}

	var parsed wireErrorPayload
	// Non-object error payloads still indicate upstream failure and fall back
	// to defaultStreamErrorMessage rather than dropping the error signal.
	if unmarshalJSONValue(trimmed, &parsed) != nil {
		return &ir.StreamError{
			Type:    "api_error",
			Message: defaultStreamErrorMessage,
		}
	}
	errType := strings.TrimSpace(parsed.Type)
	if errType == "" {
		errType = "api_error"
	}
	msg := parsed.Message
	if strings.TrimSpace(msg) == "" {
		msg = defaultStreamErrorMessage
	}
	return &ir.StreamError{
		Type:    errType,
		Code:    decodeErrorCode(parsed.Code),
		Message: msg,
		Status:  parsed.Status,
	}
}

// decodeErrorCode extracts a string representation from a JSON string or number
// error code token inside an upstream Messages error payload. Absent or null
// tokens return "" without failing error envelope decoding.
func decodeErrorCode(raw jsontext.Value) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	if trimmed[0] == '"' {
		var code string
		// Retain the raw token text if unquoting fails on a non-standard provider
		// error code so diagnostic identifiers are still preserved.
		if unmarshalJSONValue(trimmed, &code) == nil {
			return code
		}
	}
	return string(trimmed)
}

// encodeStreamError formats se as a deterministic {"type":"error","error":{...}}
// JSON payload for a terminal Anthropic Messages SSE error frame. Nil inputs or
// blank fields default to "api_error" and defaultStreamErrorMessage.
func encodeStreamError(se *ir.StreamError) ([]byte, error) {
	body := wireErrorBodyWrite{
		Type:    "api_error",
		Message: defaultStreamErrorMessage,
	}
	if se != nil {
		if strings.TrimSpace(se.Type) != "" {
			body.Type = se.Type
		}
		body.Code = se.Code
		if strings.TrimSpace(se.Message) != "" {
			body.Message = se.Message
		}
		body.Status = se.Status
	}
	return marshalDeterministic(wireErrorEventWrite{
		Type:  "error",
		Error: body,
	})
}

// truncatedStreamError constructs the StreamError emitted when an upstream
// Anthropic Messages SSE stream ends before message_stop arrives. Structured
// *ir.StreamError causes retain their caller-classified Type, Code, Message,
// and Status fields; other non-nil transport errors populate Code="stream_interrupted",
// whereas silent upstream EOF produces Code="stream_truncated" with truncatedStreamErrorMessage.
func truncatedStreamError(cause error) *ir.StreamError {
	if se, ok := errors.AsType[*ir.StreamError](cause); ok && se != nil {
		cp := *se
		if strings.TrimSpace(cp.Type) == "" {
			cp.Type = "api_error"
		}
		if strings.TrimSpace(cp.Message) == "" {
			cp.Message = defaultStreamErrorMessage
		}
		return &cp
	}
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		return &ir.StreamError{
			Type:    "api_error",
			Code:    "stream_interrupted",
			Message: cause.Error(),
		}
	}
	return &ir.StreamError{
		Type:    "api_error",
		Code:    "stream_truncated",
		Message: truncatedStreamErrorMessage,
	}
}
