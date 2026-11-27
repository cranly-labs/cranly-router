package gemini

import "encoding/json"

// aistudio_request.go normalizes built-in Google Search tool field names in
// Gemini GenerateContent requests targeting the AI Studio REST endpoint.
// Converted GenerateContent payloads emit camelCase "googleSearch" tool entries
// accepted by Code Assist and Vertex envelopes, whereas direct AI Studio REST
// endpoints expect snake_case "google_search" when no explicit snake_case entry
// is already present.

// NativeNormalizeGeminiRequestForAIStudio renames tool-level "googleSearch"
// keys to "google_search" inside tools array objects when "google_search" is
// absent. Malformed JSON, missing or empty tools arrays, and payloads needing
// no key rename return body unchanged without reallocating or re-encoding.
//
// When both "googleSearch" and "google_search" exist on the same tool entry,
// both remain untouched so an explicit snake_case declaration is never
// overwritten. Modified payloads are re-encoded through encoding/json maps, and
// marshaling errors fall back to returning body unchanged.
func NativeNormalizeGeminiRequestForAIStudio(body []byte) []byte {
	var payload map[string]any
	// Return malformed JSON unchanged so downstream validation or upstream error
	// handling receives the caller's original request bytes.
	if err := json.Unmarshal(body, &payload); err != nil {
		return body
	}

	tools, ok := payload["tools"].([]any)
	if !ok || len(tools) == 0 {
		return body
	}

	modified := false
	for _, rawTool := range tools {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			continue
		}
		googleSearch, ok := tool["googleSearch"]
		if !ok {
			continue
		}
		if _, exists := tool["google_search"]; exists {
			continue
		}
		// Rename camelCase googleSearch only when snake_case google_search is
		// absent so explicit caller snake_case settings retain precedence.
		tool["google_search"] = googleSearch
		delete(tool, "googleSearch")
		modified = true
	}

	if !modified {
		return body
	}

	// Fall back to the original body if re-marshaling fails rather than
	// returning empty bytes for an optional compatibility normalization.
	normalized, err := json.Marshal(payload)
	if err != nil {
		return body
	}
	return normalized
}
