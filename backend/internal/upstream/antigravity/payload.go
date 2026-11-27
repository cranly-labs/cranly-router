package antigravity

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/google/uuid"

	"github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	googlewire "github.com/TokenFlux/TokenRouter/internal/protocol/google"
)

// payload.go prepares Antigravity request envelopes, identity instructions,
// and Gemini tool schemas.
// Antigravity generation payloads carry a project and internal model route
// outside the Gemini request object. The envelope carries the model supplied by
// the caller alongside a new agent request ID and a fixed client identity.
//
// InjectIdentityPatchToGeminiRequestWithPrompt prepends custom or default
// identity instructions through lossless jsontext member rewriting, whereas
// InjectIdentityPatchToGeminiRequest and CleanGeminiRequest decode mutable JSON
// maps and reencode when their conditions require changes. Existing camelCase
// declaration arrays take precedence. Directly supplied schema maps can be
// mutated by schema cleaners; byte wrappers work on separately decoded objects.

const (
	// MaxTokensBudgetPadding defines the additional output allowance above
	// thinking budgets for callers enforcing the Claude max-token relationship.
	MaxTokensBudgetPadding = 1000

	// antigravityIdentity supplies the built-in platform instruction when
	// identity patching is enabled and no user identity marker is present.
	antigravityIdentity = `<identity>
You are Antigravity, a powerful agentic AI coding assistant designed by the Google Deepmind team working on Advanced Agentic Coding.
You are pair programming with a USER to solve their coding task. The task may require creating a new codebase, modifying or debugging an existing codebase, or simply answering a question.
The USER will send you requests, which you must always prioritize addressing. Along with each USER request, we will attach additional metadata about their current state, such as what files they have open and where their cursor is.
This information may or may not be relevant to the coding task, it is up for you to decide.
</identity>
<communication_style>
- **Proactiveness**. As an agent, you are allowed to be proactive, but only in the course of completing the user's task. For example, if the user asks you to add a new component, you can edit the code, verify build and test statuses, and take any other obvious follow-up actions, such as performing additional research. However, avoid surprising the user. For example, if the user asks HOW to approach something, you should answer their question and instead of jumping into editing a file.</communication_style>`
)

var (
	// AntigravityPassthroughErrorMessages lists lowercase fragments that permit upstream
	// text to reach clients through substring matching in local error handling.
	AntigravityPassthroughErrorMessages = []string{
		"prompt is too long",
	}

	// ErrProjectIDRequired reports a missing trimmed project for the generic
	// v1internal wrapper, before generation data can be serialized for sending.
	ErrProjectIDRequired = errors.New("this standard-tier Antigravity provider requires project_id")

	// DefaultAntigravityModelMapping is an allocated empty map so the admin
	// defaults endpoint encodes it as a JSON object.
	DefaultAntigravityModelMapping = map[string]string{}
)

// PromptTooLongError retains a prompt-length response status, request
// identity, and captured body.
type PromptTooLongError struct {
	StatusCode int    // upstream HTTP status code
	RequestID  string // upstream response request identifier
	Body       []byte // captured upstream error bytes
}

// Error returns the prompt-length failure label and its upstream HTTP status.
func (e *PromptTooLongError) Error() string {
	return fmt.Sprintf("prompt too long: status=%d", e.StatusCode)
}

// GetDefaultIdentityPatch returns the built-in Antigravity identity
// instruction for request preparation.
func GetDefaultIdentityPatch() string {
	return antigravityIdentity
}

// ApplyThinkingModelSuffix selects the Sonnet 4.5 thinking name when enabled
// and the mapped name exactly matches its base. Other names and disabled
// thinking retain their supplied model identity.
func ApplyThinkingModelSuffix(mappedModel string, thinkingEnabled bool) string {
	if !thinkingEnabled {
		return mappedModel
	}
	if mappedModel == "claude-sonnet-4-5" {
		return "claude-sonnet-4-5-thinking"
	}
	return mappedModel
}

// InjectIdentityPatchToGeminiRequest adds the default identity before existing
// camelCase systemInstruction parts. Callers supply a nonnull JSON object;
// decoded numeric values use float64 when a changed request is reencoded.
//
// A text part containing the identity marker anywhere selects the supplied
// bytes unchanged. Otherwise existing parts follow the injected part, while
// absent or differently shaped instruction/parts values are replaced. This
// ordering keeps valid user instructions after the platform identity without
// appending another identity to a marked request.
func InjectIdentityPatchToGeminiRequest(body []byte) ([]byte, error) {
	var request map[string]any
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, fmt.Errorf("parse Gemini request: %w", err)
	}

	if sysInst, ok := request["systemInstruction"].(map[string]any); ok {
		if parts, ok := sysInst["parts"].([]any); ok {
			for _, part := range parts {
				if partMap, ok := part.(map[string]any); ok {
					if text, ok := partMap["text"].(string); ok {
						if strings.Contains(text, "You are Antigravity") {
							return body, nil
						}
					}
				}
			}
		}
	}

	identityPatch := GetDefaultIdentityPatch()

	// Prepend the identity to existing parts so user instructions remain present;
	// a supplied marker selects unchanged bytes before any reserialization
	// occurs.
	newPart := map[string]any{"text": identityPatch}

	if existing, ok := request["systemInstruction"].(map[string]any); ok {
		if parts, ok := existing["parts"].([]any); ok {
			existing["parts"] = append([]any{newPart}, parts...)
		} else {
			existing["parts"] = []any{newPart}
		}
	} else {
		request["systemInstruction"] = map[string]any{
			"parts": []any{newPart},
		}
	}

	return json.Marshal(request)
}

// InjectIdentityPatchToGeminiRequestWithPrompt prepends customPrompt (or
// GetDefaultIdentityPatch when customPrompt trims to empty) before existing
// camelCase systemInstruction parts via lossless jsontext token rewriting.
// Requests whose existing systemInstruction.parts already contain the
// "You are Antigravity" marker return body unchanged, while non-object inputs
// or malformed JSON return a parse error.
func InjectIdentityPatchToGeminiRequestWithPrompt(body []byte, customPrompt string) ([]byte, error) {
	// rawObjectMember preserves exact JSON member order and borrowed raw value bytes during patching.
	type rawObjectMember struct {
		name  string // decoded object member key
		value []byte // validated raw JSON member slice
	}
	// textPartPayload encodes a single Gemini text part inside systemInstruction.parts.
	type textPartPayload struct {
		Text string `json:"text"` // injected identity instruction text
	}

	readObjectMembers := func(raw []byte) ([]rawObjectMember, bool) {
		dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
		tok, err := dec.ReadToken()
		if err != nil || tok.Kind() != '{' {
			return nil, false
		}
		var members []rawObjectMember
		for dec.PeekKind() != '}' {
			kTok, kErr := dec.ReadToken()
			if kErr != nil {
				return nil, false
			}
			keyName := kTok.String()
			vStart := int(dec.InputOffset())
			if vErr := dec.SkipValue(); vErr != nil {
				return nil, false
			}
			vEnd := int(dec.InputOffset())
			span := bytes.TrimSpace(raw[vStart:vEnd])
			if len(span) > 0 && span[0] == ':' {
				span = bytes.TrimSpace(span[1:])
			}
			members = append(members, rawObjectMember{
				name:  keyName,
				value: span,
			})
		}
		if _, err := dec.ReadToken(); err != nil {
			return nil, false
		}
		if _, err := dec.ReadToken(); err != io.EOF {
			return nil, false
		}
		return members, true
	}

	topMembers, ok := readObjectMembers(body)
	if !ok {
		return nil, fmt.Errorf("parse Gemini request: invalid JSON object")
	}

	sysIdx := -1
	for i, m := range topMembers {
		if m.name == "systemInstruction" {
			sysIdx = i
		}
	}

	var sysMembers []rawObjectMember
	var existingParts [][]byte
	hasSysObject := false
	partsIdx := -1
	if sysIdx >= 0 {
		if parsedSys, sysOK := readObjectMembers(topMembers[sysIdx].value); sysOK {
			hasSysObject = true
			sysMembers = parsedSys
			for i, sm := range sysMembers {
				if sm.name == "parts" {
					partsIdx = i
				}
			}
			if partsIdx >= 0 {
				partsRaw := sysMembers[partsIdx].value
				pDec := jsontext.NewDecoder(bytes.NewReader(partsRaw), jsontext.AllowInvalidUTF8(true))
				if pTok, pErr := pDec.ReadToken(); pErr == nil && pTok.Kind() == '[' {
					var elems [][]byte
					validArr := true
					for pDec.PeekKind() != ']' {
						eStart := int(pDec.InputOffset())
						if eErr := pDec.SkipValue(); eErr != nil {
							validArr = false
							break
						}
						eEnd := int(pDec.InputOffset())
						elemSlice := bytes.TrimSpace(partsRaw[eStart:eEnd])
						if len(elemSlice) > 0 && elemSlice[0] == ',' {
							elemSlice = bytes.TrimSpace(elemSlice[1:])
						}
						if partMembers, pOK := readObjectMembers(elemSlice); pOK {
							for _, pm := range partMembers {
								if pm.name == "text" {
									var txt string
									if jsonv2.Unmarshal(pm.value, &txt, jsontext.AllowInvalidUTF8(true)) == nil &&
										strings.Contains(txt, "You are Antigravity") {
										return body, nil
									}
								}
							}
						}
						elems = append(elems, elemSlice)
					}
					if validArr {
						if _, endErr := pDec.ReadToken(); endErr == nil {
							existingParts = elems
						}
					}
				}
			}
		}
	}

	identityPatch := strings.TrimSpace(customPrompt)
	if identityPatch == "" {
		identityPatch = GetDefaultIdentityPatch()
	}
	newPartBytes, err := jsonv2.Marshal(textPartPayload{Text: identityPatch}, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return nil, fmt.Errorf("marshal identity patch part: %w", err)
	}

	newPartsValue := make([]byte, 0, len(newPartBytes)+64)
	newPartsValue = append(newPartsValue, '[')
	newPartsValue = append(newPartsValue, newPartBytes...)
	for _, ep := range existingParts {
		newPartsValue = append(newPartsValue, ',')
		newPartsValue = append(newPartsValue, ep...)
	}
	newPartsValue = append(newPartsValue, ']')

	encodeObject := func(members []rawObjectMember, sizeHint int) ([]byte, error) {
		out := make([]byte, 0, max(sizeHint, 2))
		out = append(out, '{')
		for i, m := range members {
			if i > 0 {
				out = append(out, ',')
			}
			keyBytes, kErr := jsonv2.Marshal(m.name, jsontext.AllowInvalidUTF8(true))
			if kErr != nil {
				return nil, kErr
			}
			out = append(out, keyBytes...)
			out = append(out, ':')
			out = append(out, m.value...)
		}
		out = append(out, '}')
		return out, nil
	}

	if hasSysObject {
		if partsIdx >= 0 {
			sysMembers[partsIdx].value = newPartsValue
		} else {
			sysMembers = append(sysMembers, rawObjectMember{name: "parts", value: newPartsValue})
		}
	} else {
		sysMembers = []rawObjectMember{{name: "parts", value: newPartsValue}}
	}
	sysBytes, err := encodeObject(sysMembers, len(newPartsValue)+32)
	if err != nil {
		return nil, err
	}
	if sysIdx >= 0 {
		topMembers[sysIdx].value = sysBytes
	} else {
		topMembers = append(topMembers, rawObjectMember{name: "systemInstruction", value: sysBytes})
	}
	return encodeObject(topMembers, len(body)+len(newPartBytes)+64)
}

// WrapV1InternalRequest adds project/model routing and a fresh agent ID around
// decoded request data. projectID is trimmed and required; request JSON can be
// any decoded value, and reencoding uses ordinary float64 numbers.
func WrapV1InternalRequest(projectID, model string, originalBody []byte) ([]byte, error) {
	var request any
	if err := json.Unmarshal(originalBody, &request); err != nil {
		return nil, fmt.Errorf("parse request body: %w", err)
	}
	projectID = strings.TrimSpace(projectID)
	if projectID == "" {
		return nil, ErrProjectIDRequired
	}

	wrapped := map[string]any{
		"project":     projectID,
		"requestId":   "agent-" + uuid.New().String(),
		"userAgent":   "antigravity", // fixed protocol identity
		"requestType": "agent",
		"model":       model,
		"request":     request,
	}

	return json.Marshal(wrapped)
}

// IsSignatureRelatedError searches extracted platform text or the raw body for
// signature and expected-thinking wording. Matching is case-insensitive and
// broad; callers use HTTP status and policy to decide whether it permits
// recovery.
func IsSignatureRelatedError(respBody []byte) bool {
	msg := strings.ToLower(strings.TrimSpace(googlewire.ExtractPlatformMessage(respBody)))
	if msg == "" {
		// Search captured body text when structured extraction is blank so
		// compatible or malformed envelopes can still supply a signature recovery
		// marker.
		msg = strings.ToLower(string(respBody))
	}

	// Accept both signature and thought_signature wording.
	if strings.Contains(msg, "thought_signature") || strings.Contains(msg, "signature") {
		return true
	}

	// Expected-thinking wording also selects the signature-related category here;
	// the response owner still checks status and policy before attempting recovery.
	if strings.Contains(msg, "expected") && (strings.Contains(msg, "thinking") || strings.Contains(msg, "redacted_thinking")) {
		return true
	}

	return false
}

// IsPromptTooLongError recognizes prompt/context length or max_tokens wording
// in platform text. Blank extraction searches the raw body; callers determine
// status and model-fallback eligibility separately from this textual
// classifier.
func IsPromptTooLongError(respBody []byte) bool {
	msg := strings.ToLower(strings.TrimSpace(googlewire.ExtractPlatformMessage(respBody)))
	if msg == "" {
		// Search the captured body when structured extraction is blank so an
		// unrecognized envelope can still provide a prompt-length recovery marker.
		msg = strings.ToLower(string(respBody))
	}
	return strings.Contains(msg, "prompt is too long") ||
		strings.Contains(msg, "request is too long") ||
		strings.Contains(msg, "context length exceeded") ||
		strings.Contains(msg, "max_tokens")
}

// IsPassthroughErrorMessage reports whether case-folded text contains a
// locally allowed message pattern.
func IsPassthroughErrorMessage(msg string) bool {
	lower := strings.ToLower(msg)
	for _, pattern := range AntigravityPassthroughErrorMessages {
		if strings.Contains(lower, pattern) {
			return true
		}
	}
	return false
}

// GetPassthroughOrDefault returns upstream text when its case-folded spelling
// contains an allowed pattern. Otherwise it returns defaultMsg, letting caller
// error handling retain fixed client-facing text for other failures.
func GetPassthroughOrDefault(upstreamMsg, defaultMsg string) string {
	if IsPassthroughErrorMessage(upstreamMsg) {
		return upstreamMsg
	}
	return defaultMsg
}

// CleanGeminiRequest cleans decoded object schemas under Gemini function
// declarations. Invalid JSON returns an error; a request without a selected
// object parameters map returns its supplied bytes.
//
// An existing camelCase declaration array takes precedence over snake_case
// even when empty. Selected parameter maps pass through undefined removal and
// schema adaptation, and processing any such map selects request reencoding
// even if the cleaner makes no visible change. Other tool entries retain their
// decoded values, including float64 numbers, within that reencoded request.
// Schema cleanup requires terminating references and comparable entries in
// union required arrays; valid JSON alone does not establish those conditions.
func CleanGeminiRequest(body []byte) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}

	modified := false

	if tools, ok := payload["tools"].([]any); ok && len(tools) > 0 {
		for _, t := range tools {
			toolMap, ok := t.(map[string]any)
			if !ok {
				continue
			}

			// Prefer an existing camelCase declaration array over snake_case so cleanup
			// uses one selected schema source; unchanged tools bypass request
			// reserialization.
			var funcs []any
			if f, ok := toolMap["functionDeclarations"].([]any); ok {
				funcs = f
			} else if f, ok := toolMap["function_declarations"].([]any); ok {
				funcs = f
			}

			if len(funcs) == 0 {
				continue
			}

			for _, f := range funcs {
				funcMap, ok := f.(map[string]any)
				if !ok {
					continue
				}

				if params, ok := funcMap["parameters"].(map[string]any); ok {
					DeepCleanUndefined(params)
					cleaned := CleanJSONSchema(params)
					funcMap["parameters"] = cleaned
					modified = true
				}
			}
		}
	}

	if !modified {
		return body, nil
	}

	return json.Marshal(payload)
}

// CleanJSONSchema delegates recursive schema adaptation to protocol/gemini and
// returns the mutated schema map. Schema and nested maps can be mutated, so
// callers copy mutable schema data shared with other requests before cleanup.
// Acyclic containers, terminating references, and comparable union-required
// entries are required by the shared cleaner; malformed values can panic.
func CleanJSONSchema(schema map[string]any) map[string]any { return gemini.CleanJSONSchema(schema) }

// DeepCleanUndefined delegates recursive undefined-field removal to
// protocol/gemini. Object maps within value can be mutated, so callers isolate
// mutable schema data they share with other requests before applying cleanup.
// The input needs acyclic ordinary maps and arrays because recursive traversal
// has no cycle guard.
func DeepCleanUndefined(value any) { gemini.DeepCleanUndefined(value) }
