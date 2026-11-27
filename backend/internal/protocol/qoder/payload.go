package qoder

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strings"
	"time"
)

// payload.go defines normalized Qoder request payloads, session metadata,
// thinking directives, tool-choice restrictions, and typed capability or
// upstream errors consumed by Qoder protocol conversion and execution.
//
// Qoder inference separates protocol-level request normalization from
// account-pinned capability resolution and conversation prefix planning. Keeping
// discrete user text blocks, explicit session identifiers, unredacted billing
// headers, and allowed-function subsets inside PayloadRequest lets codec
// conversion stay stateless and deterministic while upstream execution resolves
// account model controls, normalizes volatile billing checksums through
// NormalizedSystem, and maps local RequestError rejections to HTTP 400 without
// triggering provider failover or health penalties.

const (
	// DefaultMaxTokens supplies the completion token budget when a client request
	// omits max_output_tokens or specifies zero.
	DefaultMaxTokens = 32768

	// RequestError code constants classify client parameter or state rejections
	// returned before any upstream Qoder network attempt begins.
	CodeInvalidRequest            = "invalid_request"             // missing model or malformed client payload
	CodeUnsupportedResponseState  = "unsupported_response_state"  // stateful store, previous_response_id, conversation, or item_reference
	CodeMissingToolCall           = "missing_tool_call"           // tool output lacking a matching call in request history
	CodeInvalidReasoningParameter = "invalid_reasoning_parameter" // explicit thinking disable on mandatory-reasoning model
	CodeUnsupportedToolChoice     = "unsupported_tool_choice"     // undeclared allowed_tools entry or disallowed required tool call

	// ToolChoice mode constants identify normalized Qoder tool selection policies
	// before route-level capability filtering runs.
	ToolChoiceAuto     = "auto"     // model may invoke zero or more declared functions
	ToolChoiceNone     = "none"     // model must not invoke any function
	ToolChoiceRequired = "required" // model must invoke at least one declared function
	ToolChoiceFunction = "function" // model must invoke ToolChoice.FunctionName

	// Vendor API error codes identify Qoder quota and model entitlement failures
	// inside wrapped SSE status envelopes.
	VendorCodeEntitlementDenied = "112" // account or plan lacks entitlement for target model
	VendorCodeAgentLimit        = "115" // agent usage quota reached until reset timestamp

	// shanghaiOffsetSeconds specifies the fixed UTC+8 offset in seconds used when
	// formatting Qoder agent-limit quota reset timestamps in Asia/Shanghai.
	shanghaiOffsetSeconds = 8 * 60 * 60

	// maxJSONRedactDepth bounds recursive JSON and stringified-JSON traversal inside
	// RedactErrorText so adversarial nested error bodies terminate with a marker.
	maxJSONRedactDepth = 32

	// nonCodeSensitiveKeyAlt lists every non-code credential and account key in
	// prefix-safe order for case-insensitive plain-text and JSON-fragment redaction.
	nonCodeSensitiveKeyAlt = `authorization_code|code_verifier|access_token|refresh_token|refreshtoken|id_token|client_secret|password|authorization|set-cookie|cookie|security_oauth_token|securityoauthtoken|personal_token|personaltoken|cosy-key|cosykey|cosy_user|cosy-user|token|uid|aid`
)

var (
	// claudeBillingCCHPattern matches five-hex cch checksums inside Anthropic
	// billing headers so session fingerprinting ignores per-turn checksum churn.
	claudeBillingCCHPattern = regexp.MustCompile(`(x-anthropic-billing-header:[^\n\r;]*?(?:;[^\n\r;]*?)*\bcch=)[0-9a-fA-F]{5}(;)`)

	// sensitiveErrorKeys holds the case-folded union of shared OAuth/credential keys
	// and Qoder session or account keys masked during structured JSON error redaction.
	sensitiveErrorKeys = map[string]struct{}{
		"authorization_code":   {},
		"code":                 {},
		"code_verifier":        {},
		"access_token":         {},
		"refresh_token":        {},
		"refreshtoken":         {},
		"id_token":             {},
		"client_secret":        {},
		"password":             {},
		"authorization":        {},
		"cookie":               {},
		"set-cookie":           {},
		"securityoauthtoken":   {},
		"security_oauth_token": {},
		"personaltoken":        {},
		"personal_token":       {},
		"token":                {},
		"cosy-key":             {},
		"cosykey":              {},
		"cosy_user":            {},
		"cosy-user":            {},
		"uid":                  {},
		"aid":                  {},
	}

	// Credential redaction patterns scrub credential-shaped prefixes, bearer headers,
	// cookies, escaped JSON fragments, query parameters, and plain assignments while
	// keeping one-to-eight-digit decimal vendor error codes intact.
	gocspSecretPattern  = regexp.MustCompile(`GOCSPX-[0-9A-Za-z_-]{24,}`)
	aizaSecretPattern   = regexp.MustCompile(`AIza[0-9A-Za-z_-]{35}`)
	bearerSecretPattern = regexp.MustCompile(`(?i)\b(authorization\s*[:=]\s*bearer\s+|bearer\s+)([^\s"',;]+)`)
	cookieSecretPattern = regexp.MustCompile(`(?i)\b(cookie|set-cookie)(\s*[:=]\s*)([^\r\n"]+)`)
	jsonStringSecretPat = regexp.MustCompile(`(?i)("(` + nonCodeSensitiveKeyAlt + `)"\s*:\s*)"((?:\\.|[^"\\])*)"`)
	jsonScalarSecretPat = regexp.MustCompile(`(?i)("(` + nonCodeSensitiveKeyAlt + `)"\s*:\s*)(-?[0-9]+(?:\.[0-9]+)?|true|false)`)
	querySecretPattern  = regexp.MustCompile(`(?i)\b((?:` + nonCodeSensitiveKeyAlt + `))=([^&\s"',;]+)`)
	plainSecretPattern  = regexp.MustCompile(`(?i)\b((?:` + nonCodeSensitiveKeyAlt + `))\b(\s*[:=]\s*)([^,\s"']+)`)
	jsonCodeStringPat   = regexp.MustCompile(`(?i)("code"\s*:\s*)"((?:\\.|[^"\\])*)"`)
	jsonCodeNumberPat   = regexp.MustCompile(`(?i)("code"\s*:\s*)(-?[0-9]+(?:\.[0-9]+)?)`)
	queryCodePattern    = regexp.MustCompile(`(?i)\b(code)=([^&\s"',;]+)`)
	plainCodePattern    = regexp.MustCompile(`(?i)\b(code)\b(\s*[:=]\s*)([^,\s"']+)`)
)

// PayloadRequest represents a protocol-normalized Qoder generation request
// serialized by codec/qoderchat and consumed by upstream Qoder execution.
type PayloadRequest struct {
	Model      string            `json:"model"`                // requested client or mapped model identifier
	System     string            `json:"system,omitzero"`      // newline-joined system and developer instructions
	Messages   []Message         `json:"messages,omitzero"`    // ordered user, assistant, and tool history turns
	Tools      []Tool            `json:"tools,omitzero"`       // normalized function tool declarations
	ToolChoice *ToolChoice       `json:"tool_choice,omitzero"` // normalized per-request tool invocation policy
	MaxTokens  int               `json:"max_tokens,omitzero"`  // positive output token cap or DefaultMaxTokens
	Thinking   ThinkingDirective `json:"thinking,omitzero"`    // normalized client thinking switch and effort intent
	// SessionID, ConversationID, CacheKey, and MetadataUserID preserve distinct
	// client affinity sources so Qoder conversation planning applies precedence
	// and API-key plus provider isolation without reparsing raw client JSON.
	SessionID      string `json:"session_id,omitzero"`       // explicit client session_id field
	ConversationID string `json:"conversation_id,omitzero"`  // explicit flat conversation_id field
	CacheKey       string `json:"cache_key,omitzero"`        // client prompt_cache_key affinity hint
	MetadataUserID string `json:"metadata_user_id,omitzero"` // client metadata.user_id identity string
}

// Message represents one normalized user, assistant, or tool turn inside PayloadRequest.
type Message struct {
	Role string `json:"role"` // speaker role: "user", "assistant", or "tool"
	// Text carries scalar turn content; empty tool results populate "(empty)"
	// alongside EmptyContent=true so desktop wire serialization and IR round-trips both hold.
	Text string `json:"text,omitzero"`
	// TextBlocks retains discrete text segments and cache_control breakpoints so
	// user turns emit structured content arrays and IR Essence checks preserve block boundaries.
	TextBlocks []TextBlock `json:"text_blocks,omitzero"`
	ToolCalls  []ToolCall  `json:"tool_calls,omitzero"`   // assistant function invocations on this turn
	ToolCallID string      `json:"tool_call_id,omitzero"` // correlated call identifier for "tool" role messages
	Name       string      `json:"name,omitzero"`         // resolved function name for "tool" role messages
	IsError    bool        `json:"is_error,omitzero"`     // whether tool execution reported an error
	// EmptyContent records that a tool result originally carried zero text parts
	// and received the "(empty)" wire placeholder in Text.
	EmptyContent bool `json:"empty_content,omitzero"`
}

// TextBlock represents one discrete text content block with an optional cache marker.
type TextBlock struct {
	Type         string        `json:"type"`                   // block discriminator, always "text"
	Text         string        `json:"text"`                   // block text payload
	CacheControl *CacheControl `json:"cache_control,omitzero"` // optional prompt cache breakpoint
}

// CacheControl represents an Anthropic-style ephemeral prompt cache marker on a TextBlock.
type CacheControl struct {
	Type string `json:"type"`         // cache breakpoint type, typically "ephemeral"
	TTL  string `json:"ttl,omitzero"` // optional cache lifetime "", "5m", or "1h"
}

// ToolCall represents one normalized assistant function invocation in Message.ToolCalls.
type ToolCall struct {
	ID       string           `json:"id"`       // call correlation identifier
	Type     string           `json:"type"`     // invocation type, always "function"
	Function ToolCallFunction `json:"function"` // invoked function name and JSON argument string
}

// ToolCallFunction carries the function name and serialized JSON arguments of a ToolCall.
type ToolCallFunction struct {
	Name      string `json:"name"`      // invoked function identifier
	Arguments string `json:"arguments"` // JSON object argument string ("{}" when empty)
}

// Tool represents one normalized function tool declaration in PayloadRequest.Tools.
type Tool struct {
	Type     string       `json:"type"`     // declaration type, always "function"
	Function ToolFunction `json:"function"` // function name, description, schema, and strict flag
}

// ToolFunction describes a callable function's name, description, JSON Schema, and strictness.
type ToolFunction struct {
	Name        string         `json:"name"`                 // declared function identifier
	Description string         `json:"description,omitzero"` // optional human-readable description
	Parameters  jsontext.Value `json:"parameters,omitzero"`  // raw JSON Schema object bytes
	Strict      *bool          `json:"strict,omitzero"`      // optional strict schema adherence toggle
}

// ToolChoice represents a normalized Qoder tool selection policy and optional allowed subset.
type ToolChoice struct {
	Mode         string   `json:"mode"`                   // ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired, or ToolChoiceFunction
	FunctionName string   `json:"function_name,omitzero"` // target function name when Mode is ToolChoiceFunction
	AllowedTools []string `json:"allowed_tools,omitzero"` // restricted declared function names for auto or required mode
}

// ThinkingDirective captures normalized client reasoning intent prior to model capability projection.
type ThinkingDirective struct {
	// Specified distinguishes explicit client disabling (Specified=true, Enabled=false)
	// from an omitted control (Specified=false) that inherits the account model's default.
	Specified bool `json:"specified,omitzero"`
	// Enabled reports whether reasoning is requested, while Effort carries the
	// normalized intensity label ("low", "medium", "high", "xhigh", or "max") when active.
	Enabled bool   `json:"enabled,omitzero"` // whether client controls enable reasoning
	Effort  string `json:"effort,omitzero"`  // normalized effort tier when Enabled is true
}

// RequestError reports a deterministic client parameter or capability rejection
// that must return HTTP 400 locally without provider failover or health penalties.
type RequestError struct {
	Code    string `json:"code,omitzero"`    // stable machine-readable rejection code
	Message string `json:"message,omitzero"` // caller-facing diagnostic explanation
}

// APIError represents an upstream HTTP or wrapped Qoder SSE envelope failure
// carrying vendor status, error code, and agent quota reset metadata.
type APIError struct {
	StatusCode          int    `json:"status_code,omitzero"`            // effective HTTP status code
	Body                string `json:"body,omitzero"`                   // redacted error payload text
	Code                string `json:"code,omitzero"`                   // vendor error code string
	Message             string `json:"message,omitzero"`                // redacted diagnostic message
	AgentLimitResetTime int64  `json:"agent_limit_reset_time,omitzero"` // Unix millisecond quota reset timestamp
}

// DecodePayload parses deterministic or caller-supplied JSON bytes into a
// PayloadRequest structure. Empty input, trailing tokens, duplicate JSON keys,
// or missing model names return a descriptive error.
func DecodePayload(body []byte) (*PayloadRequest, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		return nil, &RequestError{Code: CodeInvalidRequest, Message: "empty qoder payload"}
	}
	var req PayloadRequest
	if err := json.Unmarshal(trimmed, &req, jsontext.AllowInvalidUTF8(true)); err != nil {
		return nil, fmt.Errorf("decode qoder payload: %w", err)
	}
	if strings.TrimSpace(req.Model) == "" {
		return nil, &RequestError{Code: CodeInvalidRequest, Message: "model is required"}
	}
	return &req, nil
}

// MarshalJSONBytes serializes r into deterministic UTF-8-tolerant JSON bytes
// suitable for Qoder upstream request caching and cross-attempt comparison. Nil
// receivers return an error without panicking or emitting partial bytes.
func (r *PayloadRequest) MarshalJSONBytes() ([]byte, error) {
	if r == nil {
		return nil, errors.New("qoder: nil PayloadRequest")
	}
	raw, err := json.Marshal(r, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return nil, fmt.Errorf("marshal qoder payload: %w", err)
	}
	return raw, nil
}

// Clone returns an independent deep copy of r with detached message slices,
// tool declarations, raw JSON Schema buffers, and tool-choice pointer fields.
// Nil receivers return nil, while empty optional slices normalize to nil.
func (r *PayloadRequest) Clone() *PayloadRequest {
	if r == nil {
		return nil
	}
	out := *r
	if len(r.Messages) > 0 {
		out.Messages = make([]Message, len(r.Messages))
		for i, m := range r.Messages {
			out.Messages[i] = m.Clone()
		}
	} else {
		out.Messages = nil
	}
	if len(r.Tools) > 0 {
		out.Tools = make([]Tool, len(r.Tools))
		for i, t := range r.Tools {
			out.Tools[i] = t.Clone()
		}
	} else {
		out.Tools = nil
	}
	if r.ToolChoice != nil {
		tc := *r.ToolChoice
		if len(r.ToolChoice.AllowedTools) > 0 {
			tc.AllowedTools = slices.Clone(r.ToolChoice.AllowedTools)
		} else {
			tc.AllowedTools = nil
		}
		out.ToolChoice = &tc
	}
	return &out
}

// ExplicitSession returns the trimmed client session identifier, preferring
// SessionID ahead of ConversationID when a client populates both affinity fields.
// Nil receivers or whitespace-only identifiers return an empty string so
// upstream session planning falls back to cache-key or history-prefix routing.
func (r *PayloadRequest) ExplicitSession() string {
	if r == nil {
		return ""
	}
	if sid := strings.TrimSpace(r.SessionID); sid != "" {
		return sid
	}
	return strings.TrimSpace(r.ConversationID)
}

// NormalizedSystem returns a copy of r.System with five-hex Anthropic billing
// header checksums replaced by the stable "cch=<cch>;" marker without mutating
// r.System in place. Nil receivers return an empty string, while callers and
// DecodePayloadRequest can still read the unmodified r.System field as stored.
func (r *PayloadRequest) NormalizedSystem() string {
	if r == nil {
		return ""
	}
	return NormalizeBillingHeader(r.System)
}

// RestrictAllowedTools returns a cloned PayloadRequest with Tools filtered to
// ToolChoice.AllowedTools when an allowed subset is present, collapsing
// ToolChoice onto its underlying auto or required Mode. Invalid subsets or
// unsupported modes return a *RequestError.
func (r *PayloadRequest) RestrictAllowedTools() (PayloadRequest, error) {
	if r == nil {
		return PayloadRequest{}, &RequestError{Code: CodeInvalidRequest, Message: "nil qoder payload"}
	}
	cloned := r.Clone()
	tools, choice, err := RestrictTools(cloned.Tools, cloned.ToolChoice)
	if err != nil {
		return PayloadRequest{}, err
	}
	cloned.Tools = tools
	cloned.ToolChoice = choice
	return *cloned, nil
}

// Clone returns an independent deep copy of m with detached TextBlocks,
// CacheControl pointers, and ToolCalls slices. Empty optional slices on m
// normalize to nil on the returned Message copy.
func (m Message) Clone() Message {
	out := m
	if len(m.TextBlocks) > 0 {
		out.TextBlocks = make([]TextBlock, len(m.TextBlocks))
		for i, b := range m.TextBlocks {
			tb := b
			if b.CacheControl != nil {
				cc := *b.CacheControl
				tb.CacheControl = &cc
			}
			out.TextBlocks[i] = tb
		}
	} else {
		out.TextBlocks = nil
	}
	if len(m.ToolCalls) > 0 {
		out.ToolCalls = slices.Clone(m.ToolCalls)
	} else {
		out.ToolCalls = nil
	}
	return out
}

// Clone returns an independent deep copy of t with detached Parameters JSON
// bytes and a copied Strict boolean pointer. Empty Parameters slices normalize
// to nil so callers can mutate schemas without aliasing t.
func (t Tool) Clone() Tool {
	out := t
	if len(t.Function.Parameters) > 0 {
		out.Function.Parameters = jsontext.Value(bytes.Clone(t.Function.Parameters))
	} else {
		out.Function.Parameters = nil
	}
	if t.Function.Strict != nil {
		v := *t.Function.Strict
		out.Function.Strict = &v
	}
	return out
}

// RestrictTools validates and applies an allowed_tools restriction on choice
// against tools, returning the filtered declaration list in original order and
// the collapsed auto/required ToolChoice. Choices without AllowedTools return
// cloned slices unchanged without mutating caller-owned input.
//
// Qoder's desktop parameters object accepts only scalar modes or a single
// function selector rather than an allowed_tools array. Filtering declared
// functions before session fingerprinting keeps the conversation's tool digest
// identical to the declarations transmitted on the wire, while empty subsets,
// modes other than auto or required, or undeclared function names fail closed
// with a *RequestError.
func RestrictTools(tools []Tool, choice *ToolChoice) ([]Tool, *ToolChoice, error) {
	if choice == nil || len(choice.AllowedTools) == 0 {
		var outTools []Tool
		if len(tools) > 0 {
			outTools = make([]Tool, len(tools))
			for i, t := range tools {
				outTools[i] = t.Clone()
			}
		}
		var outChoice *ToolChoice
		if choice != nil {
			cp := *choice
			cp.AllowedTools = nil
			outChoice = &cp
		}
		return outTools, outChoice, nil
	}

	invalid := &RequestError{
		Code:    CodeUnsupportedToolChoice,
		Message: "Select declared Qoder functions and an auto or required allowed_tools mode.",
	}
	if choice.Mode != ToolChoiceAuto && choice.Mode != ToolChoiceRequired {
		return nil, nil, invalid
	}

	missing := make(map[string]bool, len(choice.AllowedTools))
	for _, rawName := range choice.AllowedTools {
		name := strings.TrimSpace(rawName)
		if name == "" {
			return nil, nil, invalid
		}
		missing[name] = true
	}
	if len(missing) == 0 {
		return nil, nil, invalid
	}

	selected := make([]Tool, 0, len(missing))
	for _, tool := range tools {
		name := strings.TrimSpace(tool.Function.Name)
		if _, ok := missing[name]; ok {
			selected = append(selected, tool.Clone())
			missing[name] = false
		}
	}
	for _, stillMissing := range missing {
		if stillMissing {
			return nil, nil, invalid
		}
	}
	return selected, &ToolChoice{Mode: choice.Mode}, nil
}

// WireValue converts c into the OpenAI-shaped parameters["tool_choice"] value
// expected by Qoder's desktop payload after RestrictTools has collapsed subsets.
// Nil receivers, blank function names, or unrecognized modes return nil so
// callers omit tool_choice from outgoing parameters.
func (c *ToolChoice) WireValue() any {
	if c == nil {
		return nil
	}
	switch c.Mode {
	case ToolChoiceAuto, ToolChoiceNone, ToolChoiceRequired:
		return c.Mode
	case ToolChoiceFunction:
		name := strings.TrimSpace(c.FunctionName)
		if name == "" {
			return nil
		}
		return map[string]any{
			"type":     "function",
			"function": map[string]any{"name": name},
		}
	default:
		return nil
	}
}

// NormalizeBillingHeader replaces volatile five-hex cch checksums inside
// x-anthropic-billing-header lines with the stable marker "<cch>". Input
// strings lacking an Anthropic billing header return unchanged without regex allocation.
func NormalizeBillingHeader(system string) string {
	if !strings.Contains(system, "x-anthropic-billing-header") || !strings.Contains(system, "cch=") {
		return system
	}
	return claudeBillingCCHPattern.ReplaceAllString(system, "${1}<cch>${2}")
}

// DeriveThinkingDirective resolves Qoder thinking intent from IR reasoning
// mode, effort, and token budget fields using Qoder's precedence order.
//
// Explicit disable modes ("disabled", "none", "off") and disable effort labels
// override all enabling signals and return {Specified: true, Enabled: false}.
// Otherwise, a recognized active effort wins over a positive budget_tokens
// fallback ("max"), followed by "enabled" or "adaptive" modes that inherit the
// selected account model's default effort. Unrecognized effort strings fall
// through so independent budgets or mode switches remain effective.
func DeriveThinkingDirective(mode, effort string, budgetTokens *int64) ThinkingDirective {
	normMode := strings.ToLower(strings.TrimSpace(mode))
	if normMode == "disabled" || normMode == "none" || normMode == "off" {
		return ThinkingDirective{Specified: true}
	}
	if strings.TrimSpace(effort) != "" {
		mapped, disabled := NormalizeThinkingEffort(effort)
		if disabled {
			return ThinkingDirective{Specified: true}
		}
		if mapped != "" {
			return ThinkingDirective{Specified: true, Enabled: true, Effort: mapped}
		}
	}
	if budgetTokens != nil && *budgetTokens > 0 {
		return ThinkingDirective{Specified: true, Enabled: true, Effort: "max"}
	}
	if normMode == "enabled" || normMode == "adaptive" {
		return ThinkingDirective{Specified: true, Enabled: true}
	}
	return ThinkingDirective{}
}

// NormalizeThinkingEffort maps case-insensitive effort strings onto Qoder's
// canonical tiers ("low", "medium", "high", "xhigh", "max") and reports explicit
// disable labels ("none", "disabled", "off"). Unknown effort labels return
// ("", false) so callers can fall back to budget or mode switches.
func NormalizeThinkingEffort(raw string) (effort string, disabled bool) {
	cleaned := strings.ToLower(strings.TrimSpace(raw))
	cleaned = strings.NewReplacer("-", "", "_", "", " ", "").Replace(cleaned)
	switch cleaned {
	case "none", "disabled", "off":
		return "", true
	case "minimal", "low":
		return "low", false
	case "medium":
		return "medium", false
	case "high":
		return "high", false
	case "xhigh", "veryhigh", "extrahigh":
		return "xhigh", false
	case "max":
		return "max", false
	default:
		return "", false
	}
}

// Error returns the caller-facing validation diagnostic stored in e.Message,
// falling back to e.Code or a generic label when Message is blank. Nil
// receivers return an empty string without dereferencing fields.
func (e *RequestError) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return e.Message
	}
	if e.Code != "" {
		return "qoder request error: " + e.Code
	}
	return "qoder request error"
}

// IsRequestError reports whether err or any error inside its unwrap chain is a
// *RequestError indicating local client validation rejection. Nil errors return
// false without allocating.
func IsRequestError(err error) bool {
	_, ok := errors.AsType[*RequestError](err)
	return ok
}

// AsRequestError extracts the first *RequestError in err's unwrap chain when
// present and reports true. Nil errors or error chains without a *RequestError
// return (nil, false) without allocating.
func AsRequestError(err error) (*RequestError, bool) {
	return errors.AsType[*RequestError](err)
}

// Error formats e for diagnostics and retry logging, prioritizing agent-limit
// quota resets formatted in Asia/Shanghai ahead of vendor code/message pairs
// and HTTP status fallbacks. Nil receivers return an empty string.
func (e *APIError) Error() string {
	if e == nil {
		return ""
	}
	if e.IsAgentLimit() {
		if resetAt, ok := e.AgentLimitResetAt(); ok {
			zone := time.FixedZone("Asia/Shanghai", shanghaiOffsetSeconds)
			return fmt.Sprintf("Qoder agent limit reached; resets at %s", resetAt.In(zone).Format("2006-01-02 15:04:05 Asia/Shanghai"))
		}
		return "Qoder agent limit reached"
	}
	if e.Code != "" && e.Message != "" {
		return fmt.Sprintf("Qoder upstream error %s: %s", e.Code, e.Message)
	}
	if e.Message != "" {
		return e.Message
	}
	if e.StatusCode > 0 {
		return fmt.Sprintf("Qoder upstream returned HTTP %d", e.StatusCode)
	}
	return "Qoder upstream error"
}

// IsAgentLimit reports whether e carries vendor code 115 or a positive agent
// quota reset timestamp. Nil receivers return false so callers can classify
// nil interface targets safely.
func (e *APIError) IsAgentLimit() bool {
	return e != nil && (e.Code == VendorCodeAgentLimit || e.AgentLimitResetTime > 0)
}

// IsEntitlementDenied reports whether e carries vendor code 112 indicating
// model or plan entitlement denial. Nil receivers return false so retry logic
// excludes entitlement rejections from token refresh attempts.
func (e *APIError) IsEntitlementDenied() bool {
	return e != nil && e.Code == VendorCodeEntitlementDenied
}

// AgentLimitResetAt converts a positive AgentLimitResetTime Unix-millisecond
// value into a time.Time and reports true so callers can schedule quota cooldowns.
// Nil receivers or nonpositive timestamps return (time.Time{}, false).
func (e *APIError) AgentLimitResetAt() (time.Time, bool) {
	if e == nil || e.AgentLimitResetTime <= 0 {
		return time.Time{}, false
	}
	return time.UnixMilli(e.AgentLimitResetTime), true
}

// AsAPIError extracts the first *APIError in err's unwrap chain when present
// and reports true. Nil errors or error chains lacking an *APIError return
// (nil, false) without allocating.
func AsAPIError(err error) (*APIError, bool) {
	return errors.AsType[*APIError](err)
}

// RedactErrorText trims diagnostic input and masks recognized OAuth, password,
// cookie, bearer, and Qoder session or account credentials while preserving
// one-to-eight-digit decimal vendor error codes. Blank inputs return an empty
// string without allocating buffers.
//
// Valid JSON payloads stream through jsontext so escaped quotes, numeric tokens,
// and nested objects under sensitive member names are replaced cleanly while
// string values still scrub embedded bearer, cookie, or key/value credentials.
// Non-JSON or truncated text falls back to escape-aware JSON-fragment, query,
// and plain-assignment expressions that retain short numeric vendor codes such
// as 101, 112, and 115 for retry diagnostics.
func RedactErrorText(input string) string {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return ""
	}
	return redactNestedText(trimmed, 0)
}

// redactNestedText scrubs structured JSON when text encodes a complete JSON
// object or array and applies plain-text credential redaction otherwise. Depth
// values exceeding maxJSONRedactDepth return a fixed marker to bound recursion
// on nested stringified JSON error envelopes.
func redactNestedText(text string, depth int) string {
	if depth > maxJSONRedactDepth {
		return "<max-depth-redacted>"
	}
	trimmed := strings.TrimSpace(text)
	if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
		if structured, ok := redactStructuredJSON([]byte(trimmed), depth); ok {
			return structured
		}
	}
	return redactPlainErrorText(text)
}

// redactStructuredJSON traverses raw with jsontext and masks recognized
// sensitive member values, returning false when raw is not a single complete
// JSON value. Duplicate object keys are permitted during traversal so malformed
// upstream error payloads still receive structured field masking.
func redactStructuredJSON(raw []byte, depth int) (string, bool) {
	dec := jsontext.NewDecoder(
		bytes.NewReader(raw),
		jsontext.AllowInvalidUTF8(true),
		jsontext.AllowDuplicateNames(true),
	)
	var out bytes.Buffer
	enc := jsontext.NewEncoder(
		&out,
		jsontext.AllowInvalidUTF8(true),
		jsontext.AllowDuplicateNames(true),
	)
	if err := redactJSONValue(dec, enc, depth); err != nil {
		return "", false
	}
	// Reject inputs with trailing non-whitespace tokens after the root JSON value
	// so mixed diagnostic lines fall through to plain-text pattern redaction.
	if _, err := dec.ReadToken(); err != io.EOF {
		return "", false
	}
	return strings.TrimSpace(out.String()), true
}

// redactJSONValue copies one JSON value from dec to enc while replacing values
// under sensitiveErrorKeys with "***" and scrubbing inline secrets from strings.
// Nesting deeper than maxJSONRedactDepth skips the remaining subtree and emits
// a bounded redaction marker.
func redactJSONValue(dec *jsontext.Decoder, enc *jsontext.Encoder, depth int) error {
	if depth > maxJSONRedactDepth {
		if err := dec.SkipValue(); err != nil {
			return err
		}
		return enc.WriteToken(jsontext.String("<max-depth-redacted>"))
	}

	switch dec.PeekKind() {
	case '{':
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		if err := enc.WriteToken(jsontext.BeginObject); err != nil {
			return err
		}
		for dec.PeekKind() != '}' {
			keyTok, err := dec.ReadToken()
			if err != nil {
				return err
			}
			keyStr := keyTok.String()
			if err := enc.WriteToken(jsontext.String(keyStr)); err != nil {
				return err
			}
			normKey := strings.ToLower(strings.TrimSpace(keyStr))
			if _, isSensitive := sensitiveErrorKeys[normKey]; isSensitive {
				if normKey == "code" {
					rawVal, err := dec.ReadValue()
					if err != nil {
						return err
					}
					// Keep one-to-eight-digit decimal vendor error codes visible while
					// masking OAuth authorization codes or structured secret values.
					if isNumericJSONCodeValue(rawVal) {
						if err := enc.WriteValue(bytes.TrimSpace(rawVal)); err != nil {
							return err
						}
						continue
					}
					if err := enc.WriteToken(jsontext.String("***")); err != nil {
						return err
					}
					continue
				}
				if err := dec.SkipValue(); err != nil {
					return err
				}
				if err := enc.WriteToken(jsontext.String("***")); err != nil {
					return err
				}
				continue
			}
			if err := redactJSONValue(dec, enc, depth+1); err != nil {
				return err
			}
		}
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		return enc.WriteToken(jsontext.EndObject)

	case '[':
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for dec.PeekKind() != ']' {
			if err := redactJSONValue(dec, enc, depth+1); err != nil {
				return err
			}
		}
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		return enc.WriteToken(jsontext.EndArray)

	case '"':
		strTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		scrubbed := redactNestedText(strTok.String(), depth+1)
		return enc.WriteToken(jsontext.String(scrubbed))

	default:
		rawVal, err := dec.ReadValue()
		if err != nil {
			return err
		}
		return enc.WriteValue(rawVal)
	}
}

// redactPlainErrorText scrubs credential-shaped tokens, bearer headers, cookies,
// escaped JSON fragments, query assignments, and plain key/value pairs in
// unstructured text. Short numeric vendor code assignments pass to
// redactPlainCodeFields so diagnostic numbers survive while secret codes mask.
func redactPlainErrorText(input string) string {
	out := gocspSecretPattern.ReplaceAllString(input, "GOCSPX-***")
	out = aizaSecretPattern.ReplaceAllString(out, "AIza***")
	out = bearerSecretPattern.ReplaceAllString(out, "${1}***")
	out = cookieSecretPattern.ReplaceAllString(out, "${1}${2}***")
	out = jsonStringSecretPat.ReplaceAllString(out, `${1}"***"`)
	out = jsonScalarSecretPat.ReplaceAllString(out, `${1}"***"`)
	out = querySecretPattern.ReplaceAllString(out, "${1}=***")
	out = plainSecretPattern.ReplaceAllString(out, "${1}${2}***")
	return redactPlainCodeFields(out)
}

// redactPlainCodeFields masks non-numeric or oversized "code" assignments inside
// non-JSON text. One-to-eight-digit decimal vendor error codes remain untouched
// across quoted JSON, numeric JSON, query, and plain assignment forms.
func redactPlainCodeFields(input string) string {
	out := jsonCodeStringPat.ReplaceAllStringFunc(input, func(m string) string {
		parts := jsonCodeStringPat.FindStringSubmatch(m)
		if len(parts) == 3 && isOneToEightASCIIDigits(parts[2]) {
			return m
		}
		return parts[1] + `"***"`
	})
	out = jsonCodeNumberPat.ReplaceAllStringFunc(out, func(m string) string {
		parts := jsonCodeNumberPat.FindStringSubmatch(m)
		if len(parts) == 3 && isOneToEightASCIIDigits(parts[2]) {
			return m
		}
		return parts[1] + `"***"`
	})
	out = queryCodePattern.ReplaceAllStringFunc(out, func(m string) string {
		parts := queryCodePattern.FindStringSubmatch(m)
		if len(parts) == 3 && isOneToEightASCIIDigits(parts[2]) {
			return m
		}
		return parts[1] + "=***"
	})
	out = plainCodePattern.ReplaceAllStringFunc(out, func(m string) string {
		parts := plainCodePattern.FindStringSubmatch(m)
		if len(parts) == 4 && isOneToEightASCIIDigits(parts[3]) {
			return m
		}
		return parts[1] + parts[2] + "***"
	})
	return out
}

// isNumericJSONCodeValue reports whether raw encodes a JSON string or JSON
// integer consisting solely of one through eight ASCII decimal digits. Non-digit
// OAuth codes, floats, negative numbers, and containers return false.
func isNumericJSONCodeValue(raw jsontext.Value) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}
	if trimmed[0] == '"' {
		var unquoted string
		if err := json.Unmarshal(trimmed, &unquoted, jsontext.AllowInvalidUTF8(true)); err != nil {
			return false
		}
		return isOneToEightASCIIDigits(unquoted)
	}
	return isOneToEightASCIIDigits(string(trimmed))
}

// isOneToEightASCIIDigits reports whether s has length 1..8 and contains only ASCII digits.
func isOneToEightASCIIDigits(s string) bool {
	if s == "" || len(s) > 8 {
		return false
	}
	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
