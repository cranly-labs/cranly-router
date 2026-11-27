package chat

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// finish.go maps OpenAI Chat Completions finish_reason strings and mid-stream
// error envelopes to and from normalized IR completion states. Chat Completions
// collapses distinct stop_sequence and pause_turn boundaries into "stop" while
// representing abnormal stream terminations through a top-level {"error": ...}
// JSON member outside choices.

const (
	// defaultStreamErrorMessage and truncatedStreamErrorMessage provide non-empty
	// fallback diagnostics required by ir.ValidateStream on abnormal stream ends.
	defaultStreamErrorMessage   = "upstream chat stream error"
	truncatedStreamErrorMessage = "upstream chat stream ended without finish_reason or [DONE]"
)

// wireErrorPayload represents a structured error object inside {"error": ...}.
type wireErrorPayload struct {
	Message string         `json:"message"`         // human-readable error message
	Type    string         `json:"type,omitzero"`   // provider error category
	Code    jsontext.Value `json:"code,omitzero"`   // string or numeric error code
	Status  int            `json:"status,omitzero"` // optional HTTP status code
}

// wireErrorEnvelopeWrite serializes a terminal Chat stream error frame.
type wireErrorEnvelopeWrite struct {
	Error wireErrorBodyWrite `json:"error"` // nested Chat error object
}

// wireErrorBodyWrite serializes the nested error fields on a terminal Chat error frame.
type wireErrorBodyWrite struct {
	Type    string `json:"type,omitzero"`   // error category label
	Code    string `json:"code,omitzero"`   // machine-readable error code
	Message string `json:"message"`         // non-empty client-facing message
	Status  int    `json:"status,omitzero"` // optional HTTP status code
}

// decodeFinishReason normalizes a Chat choice finish_reason string into ir.Finish.
// When raw is blank on a [DONE]-terminated stream, hasToolCalls selects
// FinishToolCalls and its absence selects FinishStop.
func decodeFinishReason(raw string, hasToolCalls bool) ir.Finish {
	switch strings.TrimSpace(raw) {
	case "":
		if hasToolCalls {
			return ir.Finish{Reason: ir.FinishToolCalls}
		}
		return ir.Finish{Reason: ir.FinishStop}
	case "stop":
		return ir.Finish{Reason: ir.FinishStop}
	case "length":
		return ir.Finish{Reason: ir.FinishLength}
	case "tool_calls", "function_call":
		return ir.Finish{Reason: ir.FinishToolCalls}
	case "content_filter":
		return ir.Finish{Reason: ir.FinishContentFilter}
	default:
		return ir.Finish{Reason: ir.FinishOther}
	}
}

// encodeFinishReason maps a canonical ir.Finish value onto OpenAI Chat
// Completions' wire finish_reason vocabulary ("stop", "length", "tool_calls",
// or "content_filter"). Stop sequences and server pauses fold into "stop".
func encodeFinishReason(finish ir.Finish) string {
	switch finish.Reason {
	case ir.FinishLength:
		return "length"
	case ir.FinishToolCalls:
		return "tool_calls"
	case ir.FinishContentFilter:
		return "content_filter"
	case ir.FinishStop, ir.FinishStopSequence, ir.FinishPause, ir.FinishError, ir.FinishOther, "":
		return "stop"
	default:
		return "stop"
	}
}

// decodeStreamError parses a non-empty raw "error" member from a Chat stream
// chunk or response into an ir.StreamError with a guaranteed non-empty Message.
// Absent or null values return nil, while string, object, and numeric-code error
// shapes are normalized without failing error delivery.
func decodeStreamError(raw jsontext.Value) *ir.StreamError {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	if trimmed[0] == '"' {
		var msg string
		// Malformed or whitespace-only error strings fall back to
		// defaultStreamErrorMessage so EventError always satisfies ValidateStream.
		if unmarshalJSONValue(trimmed, &msg) == nil && strings.TrimSpace(msg) != "" {
			return &ir.StreamError{
				Type:    "server_error",
				Message: msg,
			}
		}
		return &ir.StreamError{
			Type:    "server_error",
			Message: defaultStreamErrorMessage,
		}
	}

	var parsed wireErrorPayload
	// Non-object error payloads such as booleans, numbers, or arrays still signal
	// upstream failure and fall back to defaultStreamErrorMessage.
	if unmarshalJSONValue(trimmed, &parsed) != nil {
		return &ir.StreamError{
			Type:    "server_error",
			Message: defaultStreamErrorMessage,
		}
	}
	msg := parsed.Message
	if strings.TrimSpace(msg) == "" {
		msg = defaultStreamErrorMessage
	}
	return &ir.StreamError{
		Type:    parsed.Type,
		Code:    decodeErrorCode(parsed.Code),
		Message: msg,
		Status:  parsed.Status,
	}
}

// decodeErrorCode extracts a string representation from a JSON string or number
// error code token. Absent or null tokens return "" so callers omit empty codes.
func decodeErrorCode(raw jsontext.Value) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	if trimmed[0] == '"' {
		var code string
		// Retain the raw token slice if string unescaping fails on a non-standard
		// provider code field so diagnostic error details are still preserved.
		if unmarshalJSONValue(trimmed, &code) == nil {
			return code
		}
	}
	return string(trimmed)
}

// encodeStreamError formats se as a deterministic {"error": {...}} JSON payload
// for a terminal Chat Completions SSE error frame. Nil inputs or blank messages
// substitute "server_error" and defaultStreamErrorMessage.
func encodeStreamError(se *ir.StreamError) ([]byte, error) {
	body := wireErrorBodyWrite{
		Type:    "server_error",
		Message: defaultStreamErrorMessage,
	}
	if se != nil {
		if se.Type != "" {
			body.Type = se.Type
		}
		body.Code = se.Code
		if strings.TrimSpace(se.Message) != "" {
			body.Message = se.Message
		}
		body.Status = se.Status
	}
	return marshalDeterministic(wireErrorEnvelopeWrite{Error: body})
}

// truncatedStreamError constructs the StreamError emitted when a Chat stream
// ends without finish_reason or [DONE]. Structured *ir.StreamError causes retain
// their caller-classified Type, Code, Message, and Status fields; other non-nil
// transport causes populate code "stream_interrupted", whereas silent upstream EOF
// produces "stream_truncated".
func truncatedStreamError(cause error) *ir.StreamError {
	if se, ok := errors.AsType[*ir.StreamError](cause); ok && se != nil {
		cp := *se
		if strings.TrimSpace(cp.Message) == "" {
			cp.Message = defaultStreamErrorMessage
		}
		return &cp
	}
	if cause != nil && strings.TrimSpace(cause.Error()) != "" {
		return &ir.StreamError{
			Type:    "stream_error",
			Code:    "stream_interrupted",
			Message: cause.Error(),
		}
	}
	return &ir.StreamError{
		Type:    "stream_error",
		Code:    "stream_truncated",
		Message: truncatedStreamErrorMessage,
	}
}
