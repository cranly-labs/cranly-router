package generatecontent

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"strconv"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// finish.go maps Google Gemini FinishReason enumerations and Google RPC error
// envelopes to and from normalized IR completion states. Gemini signals tool-call
// pauses through FinishReason="STOP" accompanied by functionCall parts rather
// than a dedicated tool_calls enum, while grouping safety, recitation, blocklist,
// PII, and image-safety rejections under distinct filter enumerations.
//
// Inline error envelopes on HTTP 200 Gemini streams can carry numeric HTTP
// status codes alongside string gRPC status identifiers or compatible proxy
// error payloads. Reconciling both code and status tokens inside
// decodeStreamError preserves HTTP status classification and machine-readable
// error codes across streaming aborts and re-encoded error frames.

const (
	// defaultStreamErrorMessage and truncatedStreamErrorMessage provide non-empty
	// fallback diagnostics required by ir.ValidateStream on abnormal terminations.
	defaultStreamErrorMessage   = "upstream gemini stream error"
	truncatedStreamErrorMessage = "upstream gemini stream ended without finishReason"
)

// wireErrorPayload decodes the nested Google RPC or compatible error object on a
// Gemini error response or terminal SSE error frame.
type wireErrorPayload struct {
	Type    string         `json:"type,omitzero"`    // optional compatible error category label
	Code    jsontext.Value `json:"code,omitzero"`    // HTTP/RPC numeric status or string error code
	Message string         `json:"message,omitzero"` // human-readable error diagnostic message
	Status  jsontext.Value `json:"status,omitzero"`  // gRPC status string such as "UNAVAILABLE" or HTTP int
}

// wireErrorEventWrite serializes a terminal Gemini SSE error envelope payload.
type wireErrorEventWrite struct {
	Error wireErrorBodyWrite `json:"error"` // nested Google RPC error descriptor
}

// wireErrorBodyWrite serializes the nested error fields inside a Gemini error frame.
type wireErrorBodyWrite struct {
	Code    int    `json:"code,omitzero"`   // optional HTTP status code when positive
	Message string `json:"message"`         // non-empty client-facing error message
	Status  string `json:"status,omitzero"` // optional gRPC or machine-readable error code
}

// isTerminalFinishReason reports whether rawReason is a non-empty Gemini
// FinishReason other than the placeholder FINISH_REASON_UNSPECIFIED value.
func isTerminalFinishReason(rawReason string) bool {
	trimmed := strings.TrimSpace(rawReason)
	return trimmed != "" && trimmed != "FINISH_REASON_UNSPECIFIED"
}

// decodeFinishReason normalizes a Gemini FinishReason enum string alongside
// whether candidate content included any functionCall part into an ir.Finish.
// Blank or FINISH_REASON_UNSPECIFIED reasons select FinishToolCalls when
// hasToolCall is true and FinishStop otherwise, matching STOP tool-call turns.
func decodeFinishReason(rawReason string, hasToolCall bool) ir.Finish {
	switch strings.TrimSpace(rawReason) {
	case "", "FINISH_REASON_UNSPECIFIED", "STOP":
		if hasToolCall {
			return ir.Finish{Reason: ir.FinishToolCalls}
		}
		return ir.Finish{Reason: ir.FinishStop}
	case "MAX_TOKENS":
		return ir.Finish{Reason: ir.FinishLength}
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII", "IMAGE_SAFETY":
		return ir.Finish{Reason: ir.FinishContentFilter}
	case "MALFORMED_FUNCTION_CALL", "UNEXPECTED_TOOL_CALL", "LANGUAGE", "OTHER":
		return ir.Finish{Reason: ir.FinishOther}
	default:
		return ir.Finish{Reason: ir.FinishOther}
	}
}

// encodeFinishReason maps a canonical ir.Finish value onto a valid Gemini
// FinishReason enum string. Blank, stop, tool_calls, stop_sequence, and pause
// reasons serialize as "STOP" because Gemini distinguishes tool pauses via
// functionCall parts rather than a dedicated FinishReason enum value.
func encodeFinishReason(finish ir.Finish, _ *[]ir.Loss) string {
	switch finish.Reason {
	case "", ir.FinishStop, ir.FinishToolCalls, ir.FinishStopSequence, ir.FinishPause:
		return "STOP"
	case ir.FinishLength:
		return "MAX_TOKENS"
	case ir.FinishContentFilter:
		return "SAFETY"
	case ir.FinishError, ir.FinishOther:
		return "OTHER"
	default:
		return "STOP"
	}
}

// decodeStreamError parses a non-empty raw "error" member from a Gemini response
// or SSE frame into an ir.StreamError with a non-empty Message. Absent or null
// values return nil, while scalar strings and malformed non-object payloads fall
// back to defaultStreamErrorMessage so terminal EventError validation holds.
//
// Google RPC error envelopes store numeric HTTP codes in "code" and canonical
// gRPC status strings (such as "UNAVAILABLE") in "status", whereas compatible
// proxies may reverse those roles. Inspecting both tokens preserves HTTP status
// integers alongside symbolic gRPC codes across round-trip error serialization.
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
	// Non-object error payloads still indicate upstream failure and fall back to
	// defaultStreamErrorMessage rather than dropping the error signal.
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

	codeStr, codeInt, codeIsInt := parseStringOrIntToken(parsed.Code)
	statusStr, statusInt, statusIsInt := parseStringOrIntToken(parsed.Status)

	var httpStatus int
	var errCode string
	switch {
	case codeIsInt:
		httpStatus = codeInt
		if statusStr != "" {
			errCode = statusStr
		} else if !statusIsInt {
			errCode = strconv.Itoa(codeInt)
		}
	case statusIsInt:
		httpStatus = statusInt
		errCode = codeStr
	default:
		if statusStr != "" {
			errCode = statusStr
		} else {
			errCode = codeStr
		}
	}

	return &ir.StreamError{
		Type:    errType,
		Code:    errCode,
		Message: msg,
		Status:  httpStatus,
	}
}

// parseStringOrIntToken inspects a raw JSON scalar token and returns its string
// spelling alongside any parsed integer value when the token is a JSON number.
// Absent or null tokens return ("", 0, false) without failing error decoding.
func parseStringOrIntToken(raw jsontext.Value) (string, int, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", 0, false
	}
	if trimmed[0] == '"' {
		var s string
		// Retain the raw token text if unquoting fails on a non-standard provider
		// error token so diagnostic identifiers remain inspectable.
		if unmarshalJSONValue(trimmed, &s) == nil {
			return s, 0, false
		}
		return string(trimmed), 0, false
	}
	if n, err := strconv.Atoi(string(trimmed)); err == nil {
		return string(trimmed), n, true
	}
	return string(trimmed), 0, false
}

// encodeStreamError formats se as a deterministic {"error":{...}} JSON payload
// for a terminal Gemini SSE error frame. Nil inputs or blank messages default
// to defaultStreamErrorMessage so ValidateGeminiStream AllowAbnormal checks pass.
func encodeStreamError(se *ir.StreamError) ([]byte, error) {
	body := wireErrorBodyWrite{
		Message: defaultStreamErrorMessage,
	}
	if se != nil {
		if strings.TrimSpace(se.Message) != "" {
			body.Message = se.Message
		}
		if se.Status > 0 {
			body.Code = se.Status
		}
		if strings.TrimSpace(se.Code) != "" && se.Code != strconv.Itoa(se.Status) {
			body.Status = se.Code
		}
	}
	return marshalDeterministic(wireErrorEventWrite{
		Error: body,
	})
}

// truncatedStreamError constructs the StreamError emitted when an upstream
// Gemini SSE stream ends before any candidate carries a terminal finishReason.
// Structured *ir.StreamError causes retain their caller-classified fields;
// other non-nil transport errors populate Code="stream_interrupted", whereas
// silent upstream EOF produces Code="stream_truncated".
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
