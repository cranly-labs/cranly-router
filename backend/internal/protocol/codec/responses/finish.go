package responses

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// finish.go maps OpenAI Responses lifecycle statuses, incomplete_details
// reasons, and error envelopes to and from normalized IR Finish and StreamError
// states. OpenAI Responses expresses normal tool-calling pauses as
// status="completed" with callable output items rather than a separate
// finish_reason field, while length and safety stops set status="incomplete"
// alongside incomplete_details.reason.

const (
	// defaultResponsesErrorMessage and truncatedResponsesErrorMessage supply
	// non-empty fallback diagnostics required by ir.ValidateStream on abnormal terminations.
	defaultResponsesErrorMessage   = "upstream responses stream error"
	truncatedResponsesErrorMessage = "upstream responses stream ended before terminal event"
)

// wireIncompleteDetails represents the incomplete_details subobject on an incomplete Responses envelope.
type wireIncompleteDetails struct {
	Reason string `json:"reason,omitzero"` // max_output_tokens or content_filter
}

// wireErrorPayload decodes structured error objects from response.error or top-level error SSE frames.
type wireErrorPayload struct {
	Message string         `json:"message"`         // human-readable error description
	Type    string         `json:"type,omitzero"`   // error category label
	Code    jsontext.Value `json:"code,omitzero"`   // string or numeric error code
	Status  int            `json:"status,omitzero"` // optional HTTP status code
}

// wireErrorBodyWrite serializes the error subobject inside a failed OpenAI Responses envelope.
type wireErrorBodyWrite struct {
	Type    string `json:"type,omitzero"`   // error category label
	Code    string `json:"code,omitzero"`   // machine-readable error code
	Message string `json:"message"`         // non-empty client-facing error message
	Status  int    `json:"status,omitzero"` // optional HTTP status code
}

// decodeResponsesFinish maps a Responses envelope status, optional
// incomplete_details, and presence of non-hosted callable output items onto a
// canonical ir.Finish value. Completed envelopes with callable tool invocations
// map to FinishToolCalls, whereas max_output_tokens and content_filter map to
// FinishLength and FinishContentFilter respectively.
func decodeResponsesFinish(status string, details *wireIncompleteDetails, hasCallableTools bool) ir.Finish {
	reason := ""
	if details != nil {
		reason = strings.TrimSpace(details.Reason)
	}
	switch strings.TrimSpace(status) {
	case "", "completed":
		if hasCallableTools {
			return ir.Finish{Reason: ir.FinishToolCalls}
		}
		return ir.Finish{Reason: ir.FinishStop}

	case "incomplete":
		switch reason {
		case "max_output_tokens":
			return ir.Finish{Reason: ir.FinishLength}
		case "content_filter":
			return ir.Finish{Reason: ir.FinishContentFilter}
		case "pause_turn":
			return ir.Finish{Reason: ir.FinishPause}
		default:
			if reason == "" {
				return ir.Finish{Reason: ir.FinishLength}
			}
			return ir.Finish{Reason: ir.FinishOther}
		}

	case "failed":
		return ir.Finish{Reason: ir.FinishError}

	case "cancelled", "canceled":
		return ir.Finish{Reason: ir.FinishOther}

	default:
		return ir.Finish{Reason: ir.FinishOther}
	}
}

// encodeResponsesFinish maps a canonical ir.Finish value onto an OpenAI
// Responses envelope status, optional incomplete_details object, and terminal
// SSE event label. Length and content_filter reasons select status "incomplete"
// with matching reason details, FinishError selects "failed", and all remaining
// reasons select "completed".
func encodeResponsesFinish(finish ir.Finish) (status string, details *wireIncompleteDetails, terminalEvent string) {
	switch finish.Reason {
	case ir.FinishLength:
		return "incomplete", &wireIncompleteDetails{Reason: "max_output_tokens"}, "response.incomplete"
	case ir.FinishContentFilter:
		return "incomplete", &wireIncompleteDetails{Reason: "content_filter"}, "response.incomplete"
	case ir.FinishError:
		return "failed", nil, "response.failed"
	case ir.FinishStop, ir.FinishToolCalls, ir.FinishStopSequence, ir.FinishPause, ir.FinishOther, "":
		return "completed", nil, "response.completed"
	default:
		return "completed", nil, "response.completed"
	}
}

// decodeResponsesError extracts a non-empty ir.StreamError from raw response.error
// or standalone error event bytes. Absent or null values return nil, while
// nested {"error": {...}}, flat {...}, and scalar string forms all normalize to
// a non-empty Message.
func decodeResponsesError(raw jsontext.Value) *ir.StreamError {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	if trimmed[0] == '"' {
		var msg string
		// Malformed or whitespace-only error strings fall back to
		// defaultResponsesErrorMessage so EventError always satisfies ValidateStream.
		if unmarshalJSONValue(trimmed, &msg) == nil && strings.TrimSpace(msg) != "" {
			return &ir.StreamError{
				Type:    "server_error",
				Message: msg,
			}
		}
		return &ir.StreamError{
			Type:    "server_error",
			Message: defaultResponsesErrorMessage,
		}
	}

	var wrapped struct {
		Error jsontext.Value `json:"error,omitzero"` // nested error object on standalone SSE error frames
	}
	// Non-object values or flat error objects without a nested "error" field
	// ignore wrapper unmarshaling errors and decode directly via decodeFlatErrorObject.
	if unmarshalJSONValue(trimmed, &wrapped) == nil {
		inner := bytes.TrimSpace(wrapped.Error)
		if len(inner) > 0 && !bytes.Equal(inner, []byte("null")) {
			return decodeFlatErrorObject(inner)
		}
	}
	return decodeFlatErrorObject(trimmed)
}

// decodeFlatErrorObject parses a flat JSON error object into an ir.StreamError.
// Blank messages or non-object JSON tokens fall back to "server_error" and
// defaultResponsesErrorMessage so downstream stream validators always receive a
// non-empty diagnostic message.
func decodeFlatErrorObject(trimmed []byte) *ir.StreamError {
	var parsed wireErrorPayload
	// Non-object error values such as numbers or arrays still indicate upstream
	// failure and normalize to defaultResponsesErrorMessage.
	if unmarshalJSONValue(trimmed, &parsed) != nil {
		return &ir.StreamError{
			Type:    "server_error",
			Message: defaultResponsesErrorMessage,
		}
	}
	msg := parsed.Message
	if strings.TrimSpace(msg) == "" {
		msg = defaultResponsesErrorMessage
	}
	errType := parsed.Type
	code := decodeErrorCode(parsed.Code)
	if errType == "" && code == "" {
		errType = "server_error"
	}
	return &ir.StreamError{
		Type:    errType,
		Code:    code,
		Message: msg,
		Status:  parsed.Status,
	}
}

// decodeErrorCode extracts a string error code from a raw JSON string or
// numeric code token. Absent or null tokens return "", while non-string JSON
// scalars return their trimmed raw representation.
func decodeErrorCode(raw jsontext.Value) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	if trimmed[0] == '"' {
		var s string
		// Preserve raw token bytes if string unescaping fails on non-standard
		// provider codes so diagnostic context is not discarded.
		if unmarshalJSONValue(trimmed, &s) == nil {
			return s
		}
	}
	return string(trimmed)
}

// encodeErrorBody converts se into a wireErrorBodyWrite suitable for a
// response.failed envelope. Nil inputs or blank messages substitute code
// "server_error" and defaultResponsesErrorMessage so serialized failure frames
// remain well-formed.
func encodeErrorBody(se *ir.StreamError) *wireErrorBodyWrite {
	body := &wireErrorBodyWrite{
		Code:    "server_error",
		Message: defaultResponsesErrorMessage,
	}
	if se != nil {
		body.Type = se.Type
		if se.Code != "" {
			body.Code = se.Code
		} else if se.Type != "" {
			body.Code = ""
		}
		if strings.TrimSpace(se.Message) != "" {
			body.Message = se.Message
		}
		body.Status = se.Status
	}
	return body
}

// truncatedResponsesStreamError constructs the StreamError emitted when an
// upstream Responses SSE stream ends without a terminal completion or failure
// event. Structured *ir.StreamError causes retain their caller-classified Type,
// Code, Message, and Status fields; other transport errors populate code
// "stream_interrupted", whereas silent EOF produces code "stream_truncated".
func truncatedResponsesStreamError(cause error) *ir.StreamError {
	if se, ok := errors.AsType[*ir.StreamError](cause); ok && se != nil {
		cp := *se
		if strings.TrimSpace(cp.Message) == "" {
			cp.Message = defaultResponsesErrorMessage
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
		Message: truncatedResponsesErrorMessage,
	}
}
