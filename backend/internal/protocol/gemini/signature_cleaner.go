package gemini

import (
	"bytes"
	"encoding/json"
	"strings"
)

// signature_cleaner.go repairs missing functionCall thought signatures and
// replaces stale thoughtSignature values across decoded Gemini requests.
// Gemini replay signatures belong to the upstream session that produced their
// history, while strict Gemini models reject unsigned functionCall parts in
// native or replayed requests. Provider-switch recovery replaces existing
// thoughtSignature fields with a caller-selected placeholder, and native
// forwarding fills absent or whitespace-only signatures on camelCase
// functionCall parts with DummyThoughtSignature.
//
// CleanNativeThoughtSignatures traverses decoded maps and slices recursively
// while leaving neighboring "signature" fields untouched.
// NativeEnsureGeminiFunctionCallThoughtSignatures screens raw bytes for a
// literal "functionCall" token first so requests without tool calls retain
// their exact byte slice and numeric formatting without generic map decoding.

// DummyThoughtSignature is the Gemini compatibility sentinel used to bypass
// upstream thoughtSignature validation when replaying unsigned or cross-account
// history parts.
const DummyThoughtSignature = "skip_thought_signature_validator"

// CleanNativeThoughtSignatures replaces every exact thoughtSignature field
// with placeholder in a decoded request. Empty bodies and decoding or encoding
// failures return the supplied bytes; successful calls reserialize the whole
// value even when no matching signature field existed.
//
// The caller chooses the placeholder and determines whether another provider
// accepts that replay strategy. This transformation replaces field values,
// including nonstring ones, without establishing cryptographic validity.
// Ordinary map decoding can round unrelated large numbers before serialization,
// so callers use this operation for the intended native replay-recovery path.
func CleanNativeThoughtSignatures(body []byte, placeholder string) []byte {
	if len(body) == 0 {
		return body
	}

	var data any
	if err := json.Unmarshal(body, &data); err != nil {
		// A failed JSON decode leaves no traversable request value to repair.
		// Return the supplied bytes so the caller retains the complete body for other recovery handling.
		return body
	}

	replaced := replaceThoughtSignaturesRecursive(data, placeholder)

	result, err := json.Marshal(replaced)
	if err != nil {
		// Failed final encoding leaves the replacement value unpublished.
		// Return the supplied request body so the caller receives a complete representation for further handling.
		return body
	}

	return result
}

// NativeEnsureGeminiFunctionCallThoughtSignatures populates absent or
// whitespace-only thoughtSignature fields with dummySignature on contents parts
// carrying a non-nil camelCase functionCall object. Payloads without a literal
// "functionCall" substring, malformed JSON, or requests where every call part
// already carries a non-blank signature return body unchanged, while non-map
// contents or part elements are skipped without aborting sibling turns.
//
// Modified payloads are re-encoded from decoded map[string]any containers,
// which formats numbers through float64; marshaling errors fall back to
// returning body unchanged.
func NativeEnsureGeminiFunctionCallThoughtSignatures(dummySignature string, body []byte) []byte {
	// Skip generic JSON decoding when no literal "functionCall" key appears so
	// ordinary text requests keep exact wire formatting and integer precision.
	if !bytes.Contains(body, []byte(`"functionCall"`)) {
		return body
	}

	var payload map[string]any
	// Preserve malformed JSON verbatim so downstream request handling reports
	// the caller's original syntax error rather than receiving a partial body.
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}

	contentsAny, ok := payload["contents"].([]any)
	if !ok || len(contentsAny) == 0 {
		return body
	}

	modified := false
	for _, c := range contentsAny {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		partsAny, ok := cm["parts"].([]any)
		if !ok || len(partsAny) == 0 {
			continue
		}
		for _, p := range partsAny {
			pm, ok := p.(map[string]any)
			if !ok || pm == nil {
				continue
			}
			if fc, ok := pm["functionCall"].(map[string]any); !ok || fc == nil {
				continue
			}
			ts, _ := pm["thoughtSignature"].(string)
			// Fill only missing or whitespace-only signatures on functionCall
			// parts while keeping existing non-blank upstream signatures intact.
			if strings.TrimSpace(ts) == "" {
				pm["thoughtSignature"] = dummySignature
				modified = true
			}
		}
	}

	if !modified {
		return body
	}
	// Return the original slice if re-marshaling fails so optional signature
	// repair never replaces a valid payload with empty bytes.
	b, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return b
}

// replaceThoughtSignaturesRecursive copies map[string]any and []any containers
// while replacing exact thoughtSignature keys with placeholder. Other field
// names retain their recursively processed values, so a field named signature
// remains distinct from the Gemini replay field.
//
// Nested supported containers are copied before their values are replaced,
// allowing the supplied maps and slices to retain their membership and values.
// Scalars and unsupported Go container types return unchanged, which can
// retain sharing for manually constructed inputs. Callers pass decoded JSON
// when they need every object and array traversed by this helper.
func replaceThoughtSignaturesRecursive(data any, placeholder string) any {
	switch v := data.(type) {
	case map[string]any:
		result := make(map[string]any, len(v))
		for key, value := range v {
			if key == "thoughtSignature" {
				// Field-name matching confines the placeholder to Gemini replay
				// data; a neighboring signature field can belong to another format and keeps its value.
				result[key] = placeholder
				continue
			}
			result[key] = replaceThoughtSignaturesRecursive(value, placeholder)
		}
		return result

	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = replaceThoughtSignaturesRecursive(item, placeholder)
		}
		return result

	default:
		return v
	}
}
