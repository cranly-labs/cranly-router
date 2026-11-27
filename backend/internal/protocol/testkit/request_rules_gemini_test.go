package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestValidateGeminiRequest feeds parallel Gemini functionCall turns with user
// properties named "deprecated" alongside missing step signatures and forbidden
// schema keywords. Valid parallel steps must pass, while missing signatures and
// additionalProperties keywords must report GEM-SIGNATURE and GEM-SCHEMA.
func TestValidateGeminiRequest(t *testing.T) {
	// Two parallel functionCalls in one step: first has thoughtSignature, second is unsigned.
	// Also includes a user property named "deprecated" inside properties to confirm
	// property names are not mistaken for schema keywords.
	validParallel := []byte(`{
		"tools": [{
			"functionDeclarations": [{
				"name": "query_item",
				"parameters": {
					"type": "OBJECT",
					"properties": {
						"deprecated": {"type": "BOOLEAN", "description": "filter flag"}
					},
					"required": ["deprecated"]
				}
			}]
		}],
		"generationConfig": {
			"maxOutputTokens": 4096,
			"thinkingConfig": {"thinkingBudget": 1024}
		},
		"contents": [
			{"role": "user", "parts": [{"text": "Check both"}]},
			{"role": "model", "parts": [
				{"text": "Checking", "thought": true, "thoughtSignature": "sig_t"},
				{"functionCall": {"name": "query_item", "args": {"deprecated": false}}, "thoughtSignature": "sig_fc1"},
				{"functionCall": {"name": "query_item", "args": {"deprecated": true}}}
			]},
			{"role": "user", "parts": [
				{"functionResponse": {"name": "query_item", "response": {"count": 3}}},
				{"functionResponse": {"name": "query_item", "response": {"count": 1}}}
			]}
		]
	}`)
	require.Empty(t, ValidateGeminiRequest(validParallel, RequestValidationOptions{
		DialectRules: []string{"gemini-signature", "antigravity-schema"},
	}))

	// Missing thoughtSignature on the first functionCall fails gemini-signature.
	missingSig := []byte(`{
		"contents": [
			{"role": "user", "parts": [{"text": "Check"}]},
			{"role": "model", "parts": [
				{"functionCall": {"name": "query_item", "args": {}}}
			]},
			{"role": "user", "parts": [
				{"functionResponse": {"name": "query_item", "response": {}}}
			]}
		]
	}`)
	v := ValidateGeminiRequest(missingSig, RequestValidationOptions{DialectRules: []string{"gemini-signature"}})
	require.Len(t, v, 1)
	require.Equal(t, "GEM-SIGNATURE", v[0].RuleID)

	// Unsupported $ref / additionalProperties inside parameters fails GEM-SCHEMA.
	badSchema := []byte(`{
		"tools": [{
			"functionDeclarations": [{
				"name": "query_item",
				"parameters": {"type": "OBJECT", "additionalProperties": false}
			}]
		}],
		"contents": [{"role": "user", "parts": [{"text": "hi"}]}]
	}`)
	v = ValidateGeminiRequest(badSchema, RequestValidationOptions{})
	require.Len(t, v, 1)
	require.Equal(t, "GEM-SCHEMA", v[0].RuleID)
}
