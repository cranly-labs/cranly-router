package qoderchat

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
)

// wire.go defines private Qoder SSE envelope structs and JSON v2 parsing
// helpers shared across stream and buffered response decoding. Qoder inference
// responses arrive wrapped inside an outer JSON envelope carrying an effective
// HTTP status alongside a stringified inner body that mixes OpenAI-shaped
// choices, Anthropic-shaped event envelopes, tolerated string/float token
// counts, and nested vendor error objects.
//
// Centralizing wire types and bounded recursive error extraction here keeps
// stream_decode.go focused on sequential IR state transitions while guaranteeing
// duplicate JSON object keys fail closed and malformed optional detail objects
// never panic or trigger unbounded recursion under adversarial inputs.

const (
	// maxErrorNestingDepth bounds recursive extraction across nested vendor error,
	// data, and JSON-string message fields so deeply nested envelopes never overflow.
	maxErrorNestingDepth = 8
)

// Codec implements upstream-only IR request encoding and SSE response decoding
// for Qoder chat inference (protocol.ProtocolQoderChat).
type Codec struct{}

// wireSSEWrapper decodes the outer JSON envelope emitted on Qoder SSE data lines.
type wireSSEWrapper struct {
	Body string `json:"body,omitzero"` // stringified inner SSE payload or error JSON
	// StatusCodeValue supplies the numeric HTTP status when positive, while
	// StatusCode supplies a decimal code or named HTTP status label otherwise.
	StatusCode      string `json:"statusCode,omitzero"`      // textual or decimal status code
	StatusCodeValue int    `json:"statusCodeValue,omitzero"` // numeric HTTP status code
}

// wireJSONResponseWrapper decodes a buffered Qoder JSON response wrapper whose
// body field may hold a JSON string, an inline JSON object, or null.
type wireJSONResponseWrapper struct {
	Body jsontext.Value `json:"body,omitzero"` // string, object, or null inner payload
	// StatusCode and StatusCodeValue report the wrapper's effective HTTP status; a
	// positive StatusCodeValue takes precedence over decimal or named StatusCode text.
	StatusCode      string `json:"statusCode,omitzero"`      // textual or decimal status code
	StatusCodeValue *int   `json:"statusCodeValue,omitzero"` // numeric HTTP status code when present
}

// wireSSEInner decodes choice, usage, and event-envelope variants inside wrapper.Body.
type wireSSEInner struct {
	Choices []wireSSEChoice `json:"choices,omitzero"` // OpenAI-shaped incremental or final choices
	Usage   jsontext.Value  `json:"usage,omitzero"`   // raw usage object or null token
	// Event and Type hold alternative Anthropic-like envelope labels on Qoder SSE
	// frames; non-blank Event takes precedence over Type during envelope dispatch.
	Event string         `json:"event,omitzero"` // primary Anthropic-like event discriminator
	Type  string         `json:"type,omitzero"`  // fallback event discriminator
	Data  jsontext.Value `json:"data,omitzero"`  // nested event payload JSON
	Index *int           `json:"index,omitzero"` // optional envelope-level tool slot index
}

// wireSSEChoice decodes incremental delta fields, final message fields, and completion reason.
type wireSSEChoice struct {
	Delta        wireSSEDelta   `json:"delta,omitzero"`         // incremental choice content
	Message      wireSSEMessage `json:"message,omitzero"`       // final assembled message content
	FinishReason string         `json:"finish_reason,omitzero"` // choice termination reason
}

// wireSSEDelta decodes incremental visible text, reasoning text, and tool call fragments.
type wireSSEDelta struct {
	Content          string            `json:"content,omitzero"`           // visible text delta
	ReasoningContent string            `json:"reasoning_content,omitzero"` // thinking text delta
	ToolCalls        []wireSSEToolCall `json:"tool_calls,omitzero"`        // incremental tool call items
}

// wireSSEMessage decodes a completed choice message carrying string or block-array content.
type wireSSEMessage struct {
	Content          jsontext.Value    `json:"content,omitzero"`           // string, block array, or null
	ReasoningContent string            `json:"reasoning_content,omitzero"` // final reasoning text
	ToolCalls        []wireSSEToolCall `json:"tool_calls,omitzero"`        // completed tool call items
}

// wireSSEToolCall decodes nested and flat tool call identity and argument fields.
type wireSSEToolCall struct {
	Index *int `json:"index,omitzero"` // explicit upstream tool call position
	// ID, ToolCallID, and CallID provide alternative call identifiers resolved in
	// declaration order so downstream slot binding retains a single call ID.
	ID         string `json:"id,omitzero"`           // preferred call identifier
	ToolCallID string `json:"tool_call_id,omitzero"` // alternate upstream call identifier
	CallID     string `json:"call_id,omitzero"`      // Responses-style call identifier
	Type       string `json:"type,omitzero"`         // wire tool kind label
	// Name, ToolName, Arguments, Input, Parameters, and Function provide flat and
	// nested call metadata variants; Function.Name precedes Name and ToolName while
	// Function.Arguments precedes Arguments, Input, and Parameters during normalization.
	Name       string              `json:"name,omitzero"`       // flat function name
	ToolName   string              `json:"tool_name,omitzero"`  // alternate flat function name
	Arguments  jsontext.Value      `json:"arguments,omitzero"`  // flat JSON arguments value
	Input      jsontext.Value      `json:"input,omitzero"`      // Anthropic-style input value
	Parameters jsontext.Value      `json:"parameters,omitzero"` // parameters alias value
	Function   wireSSEToolFunction `json:"function,omitzero"`   // nested OpenAI function object
}

// wireSSEToolFunction decodes the nested function object on a wireSSEToolCall.
type wireSSEToolFunction struct {
	Name      string         `json:"name,omitzero"`      // nested function name
	Arguments jsontext.Value `json:"arguments,omitzero"` // nested function arguments string or object
}

// wireSSEContentBlockDelta decodes the data object of a content_block_delta envelope.
type wireSSEContentBlockDelta struct {
	Type string `json:"type,omitzero"` // block delta kind ("thinking_delta" or text)
	Text string `json:"text,omitzero"` // incremental text or reasoning payload
}

// wireSSEToolUseStart decodes the data object of a tool_use_start envelope.
type wireSSEToolUseStart struct {
	Index      *int   `json:"index,omitzero"`        // data-level tool call slot index
	ID         string `json:"id,omitzero"`           // preferred call identifier
	ToolCallID string `json:"tool_call_id,omitzero"` // alternate call identifier
	CallID     string `json:"call_id,omitzero"`      // Responses-style call identifier
	Name       string `json:"name,omitzero"`         // invoked function name
	Type       string `json:"type,omitzero"`         // wire tool kind label
}

// wireSSEToolUseDelta decodes the data object of a tool_use_delta or tool_call_delta envelope.
type wireSSEToolUseDelta struct {
	Index      *int           `json:"index,omitzero"`        // data-level tool call slot index
	ID         string         `json:"id,omitzero"`           // preferred call identifier
	ToolCallID string         `json:"tool_call_id,omitzero"` // alternate call identifier
	CallID     string         `json:"call_id,omitzero"`      // Responses-style call identifier
	Name       string         `json:"name,omitzero"`         // flat function name
	ToolName   string         `json:"tool_name,omitzero"`    // alternate function name
	Type       string         `json:"type,omitzero"`         // wire tool kind label
	Arguments  jsontext.Value `json:"arguments,omitzero"`    // primary JSON argument fragment
	Input      jsontext.Value `json:"input,omitzero"`        // alternate input JSON fragment
	Parameters jsontext.Value `json:"parameters,omitzero"`   // alternate parameters JSON fragment
}

// wireSSEMessageDelta decodes the data object of a message_delta envelope.
type wireSSEMessageDelta struct {
	Usage jsontext.Value `json:"usage,omitzero"` // nested envelope usage object
}

// wireSSEUsage decodes primary and alias token counters plus optional detail objects.
type wireSSEUsage struct {
	PromptTokens            jsontext.Value `json:"prompt_tokens,omitzero"`             // primary prompt token count
	CompletionTokens        jsontext.Value `json:"completion_tokens,omitzero"`         // primary completion token count
	TotalTokens             jsontext.Value `json:"total_tokens,omitzero"`              // reported total token count
	InputTokens             jsontext.Value `json:"input_tokens,omitzero"`              // alias prompt token count
	OutputTokens            jsontext.Value `json:"output_tokens,omitzero"`             // alias completion token count
	PromptTokensDetails     jsontext.Value `json:"prompt_tokens_details,omitzero"`     // optional cached/cacheable breakdown
	CompletionTokensDetails jsontext.Value `json:"completion_tokens_details,omitzero"` // optional reasoning breakdown
}

// wirePromptTokensDetails decodes cached and cacheable prompt token counts.
type wirePromptTokensDetails struct {
	CachedTokens    jsontext.Value `json:"cached_tokens,omitzero"`    // cache-hit input tokens
	CacheableTokens jsontext.Value `json:"cacheable_tokens,omitzero"` // cache-eligible input tokens (not cache writes)
}

// wireCompletionTokensDetails decodes completion reasoning token counts inside completion_tokens_details objects.
type wireCompletionTokensDetails struct {
	ReasoningTokens jsontext.Value `json:"reasoning_tokens,omitzero"` // generated reasoning tokens
}

// wireTextBlock decodes one typed element inside an array-valued choice message content field.
type wireTextBlock struct {
	Type string `json:"type,omitzero"` // content block type ("text")
	Text string `json:"text,omitzero"` // visible text segment
}

// wireErrorBody decodes nested vendor error metadata inside failed Qoder envelopes.
type wireErrorBody struct {
	Code                jsontext.Value `json:"code,omitzero"`                // string or numeric vendor error code
	Message             string         `json:"message,omitzero"`             // vendor diagnostic message
	Type                string         `json:"type,omitzero"`                // fallback vendor error classification
	Error               jsontext.Value `json:"error,omitzero"`               // nested OpenAPI error object
	Data                jsontext.Value `json:"data,omitzero"`                // nested Qoder data error object
	AgentLimitResetTime jsontext.Value `json:"agentLimitResetTime,omitzero"` // Unix millisecond reset timestamp
}

// unmarshalPermissive decodes data into dst using JSON v2 while tolerating
// invalid UTF-8 byte sequences in string literals. Duplicate JSON object member
// names and syntax errors still return a non-nil error.
func unmarshalPermissive(data []byte, dst any) error {
	return json.Unmarshal(data, dst, jsontext.AllowInvalidUTF8(true))
}

// marshalDeterministic serializes src into sorted-key JSON v2 bytes while
// tolerating invalid UTF-8 byte sequences inside strings. Unsupported Go value
// types return a non-nil serialization error.
func marshalDeterministic(src any) ([]byte, error) {
	return json.Marshal(src, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
}

// validateSingleJSONValue verifies that raw contains exactly one syntactically
// valid JSON value without duplicate object keys or trailing tokens. Blank
// inputs, syntax errors, duplicate object keys, or trailing tokens return an error.
func validateSingleJSONValue(raw []byte) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return fmt.Errorf("empty JSON value")
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	var v jsontext.Value
	if err := json.UnmarshalDecode(dec, &v, jsontext.AllowInvalidUTF8(true)); err != nil {
		return err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON token")
	}
	return nil
}

// parseWrapperStatus resolves the effective HTTP status code on wrapper,
// preferring positive StatusCodeValue ahead of decimal or named StatusCode
// strings. Missing or unrecognized status strings resolve to zero.
func parseWrapperStatus(wrapper wireSSEWrapper) int {
	if wrapper.StatusCodeValue > 0 {
		return wrapper.StatusCodeValue
	}
	return parseStatusString(wrapper.StatusCode)
}

// parseStatusString maps a decimal code or uppercase status name to an HTTP
// status integer. Blank or unrecognized strings return zero so callers treat
// ordinary SSE wrappers as HTTP 200 frames.
func parseStatusString(raw string) int {
	status := strings.TrimSpace(raw)
	if status == "" {
		return 0
	}
	if parsed, err := strconv.Atoi(status); err == nil {
		return parsed
	}
	switch strings.ToUpper(status) {
	case "BAD_REQUEST":
		return 400
	case "UNAUTHORIZED":
		return 401
	case "FORBIDDEN":
		return 403
	case "NOT_FOUND":
		return 404
	case "TOO_MANY_REQUESTS":
		return 429
	case "INTERNAL_SERVER_ERROR":
		return 500
	case "BAD_GATEWAY":
		return 502
	case "SERVICE_UNAVAILABLE":
		return 503
	case "GATEWAY_TIMEOUT":
		return 504
	default:
		return 0
	}
}

// parseWrappedAPIError constructs a *qoder.APIError from a failed Qoder status
// wrapper and extracts nested vendor error codes and reset timestamps. Both
// Body and Message are scrubbed through qoder.RedactErrorText before returning.
func parseWrappedAPIError(wrapper wireSSEWrapper, statusCode int) *qoder.APIError {
	apiErr := &qoder.APIError{
		StatusCode: statusCode,
		Body:       wrapper.Body,
		Message:    fmt.Sprintf("Qoder upstream returned HTTP %d", statusCode),
	}
	applyErrorPayload(apiErr, []byte(wrapper.Body), 0)
	apiErr.Body = qoder.RedactErrorText(apiErr.Body)
	apiErr.Message = qoder.RedactErrorText(apiErr.Message)
	return apiErr
}

// applyErrorPayload recursively extracts vendor error codes, messages, and
// positive agent-limit reset timestamps up to maxErrorNestingDepth levels deep.
// Nil targets, empty buffers, or malformed inner error fragments return
// immediately without erasing outer status and message values.
//
// Qoder failure wrappers often nest the authoritative vendor code (such as 112
// or 115) and agentLimitResetTime inside inner data, OpenAPI error objects, or
// JSON-serialized message strings. Inspecting those nested payloads after outer
// fields lets deeper vendor metadata refine retry and cooldown classification
// while preserving the enclosing HTTP status code.
func applyErrorPayload(apiErr *qoder.APIError, payload []byte, depth int) {
	if apiErr == nil || len(payload) == 0 || depth >= maxErrorNestingDepth {
		return
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return
	}
	var body wireErrorBody
	// Keep outer HTTP failure metadata when nested diagnostic text is non-JSON or
	// carries duplicate keys so stream error classification never loses StatusCode.
	if err := unmarshalPermissive(trimmed, &body); err != nil {
		return
	}
	if code := parseErrorCode(body.Code); code != "" {
		apiErr.Code = code
	}
	if strings.TrimSpace(body.Message) != "" {
		apiErr.Message = body.Message
	} else if body.Type != "" && (apiErr.Message == "" || strings.HasPrefix(apiErr.Message, "Qoder upstream returned HTTP ")) {
		apiErr.Message = body.Type
	}
	if resetTime := parseFlexibleInt64(body.AgentLimitResetTime); resetTime > 0 {
		apiErr.AgentLimitResetTime = resetTime
	}
	if len(body.Data) > 0 {
		applyErrorPayload(apiErr, body.Data, depth+1)
	}
	if len(body.Error) > 0 {
		applyErrorPayload(apiErr, body.Error, depth+1)
	}
	msgTrim := strings.TrimSpace(body.Message)
	if strings.HasPrefix(msgTrim, "{") {
		applyErrorPayload(apiErr, []byte(msgTrim), depth+1)
	}
}

// parseErrorCode formats a JSON string or JSON number vendor error code as a
// trimmed string. Absent, null, non-scalar, or non-finite values return an
// empty string so callers retain any previously extracted vendor code.
func parseErrorCode(raw jsontext.Value) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalPermissive(trimmed, &s); err == nil {
			return strings.TrimSpace(s)
		}
		return ""
	}
	var f float64
	if err := unmarshalPermissive(trimmed, &f); err == nil && !math.IsNaN(f) && !math.IsInf(f, 0) {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return ""
}

// parseFlexibleInt64 converts a JSON number or numeric string into an int64,
// truncating finite floating-point representations toward zero. Absent, null,
// malformed, non-finite, or out-of-range inputs return zero without error.
func parseFlexibleInt64(raw jsontext.Value) int64 {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalPermissive(trimmed, &s); err != nil {
			return 0
		}
		s = strings.TrimSpace(s)
		if parsed, err := strconv.ParseInt(s, 10, 64); err == nil {
			return parsed
		}
		if parsedFloat, err := strconv.ParseFloat(s, 64); err == nil {
			return floatToBoundedInt64(parsedFloat)
		}
		return 0
	}
	if parsed, err := strconv.ParseInt(string(trimmed), 10, 64); err == nil {
		return parsed
	}
	var parsedFloat float64
	// Usage payloads from compatible Qoder routes occasionally format token counts
	// as decimal numbers; truncate representable floats toward zero safely.
	if err := unmarshalPermissive(trimmed, &parsedFloat); err == nil {
		return floatToBoundedInt64(parsedFloat)
	}
	return 0
}

// floatToBoundedInt64 truncates a finite float64 within signed 64-bit integer
// bounds toward zero. NaN, infinities, and magnitudes exceeding int64 range
// return zero so malformed usage counters cannot overflow.
func floatToBoundedInt64(v float64) int64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	// Reject magnitudes at or beyond the int64 boundary before float-to-int
	// conversion produces undefined wraparound on 64-bit targets.
	if v >= float64(math.MaxInt64) || v < float64(math.MinInt64) {
		return 0
	}
	return int64(v)
}

// parseToolArgumentsRaw selects the first non-empty tool argument candidate in
// order, unquoting non-blank JSON string fragments and preserving raw JSON
// object or array bytes verbatim. When every candidate is absent, null, or
// whitespace-only, it returns an empty string.
func parseToolArgumentsRaw(candidates ...jsontext.Value) string {
	for _, raw := range candidates {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
			continue
		}
		if trimmed[0] == '"' {
			var s string
			// Tool call deltas encode arguments either as escaped JSON strings or
			// inline JSON objects; unquote non-blank strings while skipping empty quotes.
			if err := unmarshalPermissive(trimmed, &s); err == nil {
				if strings.TrimSpace(s) != "" {
					return s
				}
				continue
			}
		}
		return string(trimmed)
	}
	return ""
}

// extractMessageContentText extracts visible string content or newline-joined
// text blocks from a completed choice message's content field. Null, absent, or
// malformed content values return an empty string without failing choice parsing.
func extractMessageContentText(raw jsontext.Value) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalPermissive(trimmed, &s); err == nil {
			return s
		}
		return ""
	}
	if trimmed[0] == '[' {
		var blocks []wireTextBlock
		// Completed choice messages may encode content as a block array; ignore
		// malformed arrays so adjacent tool calls on the choice still decode.
		if err := unmarshalPermissive(trimmed, &blocks); err != nil {
			return ""
		}
		parts := make([]string, 0, len(blocks))
		for _, b := range blocks {
			if b.Type == "text" && b.Text != "" {
				parts = append(parts, b.Text)
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

// messageHasPayload reports whether msg carries non-null content, reasoning text, or tool calls.
func messageHasPayload(msg wireSSEMessage) bool {
	trimmedContent := bytes.TrimSpace(msg.Content)
	hasContent := len(trimmedContent) > 0 && !bytes.Equal(trimmedContent, []byte("null"))
	return hasContent || msg.ReasoningContent != "" || len(msg.ToolCalls) > 0
}

// firstNonEmptyString returns the first element in values that contains at
// least one non-whitespace rune while preserving its original spacing. When all
// supplied strings are empty or whitespace-only, it returns an empty string.
func firstNonEmptyString(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// firstNonNilInt returns the first non-nil int pointer in values so explicit
// zero slot indices remain distinguishable from omitted fields. When every
// candidate pointer is nil, it returns nil.
func firstNonNilInt(values ...*int) *int {
	for _, v := range values {
		if v != nil {
			return v
		}
	}
	return nil
}

// normalizeWireToolType maps blank, "tool_use", and "tool_call" Qoder wire
// labels onto the canonical "function" tool kind. Other non-blank tool type
// strings return unchanged so downstream callers can inspect custom labels.
func normalizeWireToolType(raw string) string {
	switch strings.TrimSpace(raw) {
	case "", "tool_use", "tool_call":
		return "function"
	default:
		return raw
	}
}
