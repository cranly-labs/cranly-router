package generatecontent

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestCleanGeminiSchemaPreservesUserPropertyNamesAndConvertsBounds verifies
// CleanGeminiSchema across Draft-2020-12 keywords, user properties named like
// forbidden keywords, type arrays, scalar/compound enums, and 64-bit integer
// exclusiveMinimum increments. Cleaned parameters must pass ValidateGeminiRequest.
func TestCleanGeminiSchemaPreservesUserPropertyNamesAndConvertsBounds(t *testing.T) {
	raw := jsontext.Value(`{
		"$schema": "https://json-schema.org/draft/2020-12/schema",
		"type": ["null", "object"],
		"additionalProperties": false,
		"properties": {
			"deprecated": {"type": "boolean", "deprecated": true},
			"minLength": {"type": "string", "minLength": 3},
			"count": {"type": "integer", "exclusiveMinimum": 9007199254740993},
			"bounded": {"type": "integer", "minimum": 10, "exclusiveMinimum": 5},
			"mode": {"type": "string", "enum": ["fast", 2, true, null]},
			"badEnum": {"type": "string", "enum": [{"nested": 1}]}
		},
		"required": ["deprecated", "count"]
	}`)

	cleaned, changed, err := CleanGeminiSchema(raw)
	require.NoError(t, err)
	require.True(t, changed)

	reqBody := []byte(`{"tools":[{"functionDeclarations":[{"name":"check_fn","parameters":` + string(cleaned) + `}]}],"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)
	violations := testkit.ValidateGeminiRequest(reqBody, testkit.RequestValidationOptions{})
	require.Empty(t, violations, "unexpected Gemini schema violations: %v", violations)

	s := string(cleaned)
	require.Contains(t, s, `"deprecated":{"type":"BOOLEAN"}`)
	require.Contains(t, s, `"minLength":{"type":"STRING"}`)
	require.Contains(t, s, `"count":{"minimum":9007199254740994,"type":"INTEGER"}`)
	require.Contains(t, s, `"bounded":{"minimum":10,"type":"INTEGER"}`)
	require.Contains(t, s, `"enum":["fast","2","true","null"]`)
	require.NotContains(t, s, `"nested"`)
}

// TestCleanAntigravitySchemaHandlesRefsCyclesUnionsAndUndefined exercises
// CleanAntigravitySchema on cyclic $defs/$ref graphs, allOf/anyOf merges,
// non-comparable required arrays, constraint hints, and [undefined] sentinels.
// Resulting schemas must pass strict antigravity-schema request validation and
// include the synthesized empty-object reason property.
func TestCleanAntigravitySchemaHandlesRefsCyclesUnionsAndUndefined(t *testing.T) {
	raw := jsontext.Value(`{
		"$defs": {
			"Node": {
				"type": "object",
				"properties": {
					"label": {"type": ["string", "null"], "minLength": 1, "pattern": "^[a-z]+$"},
					"next": {"$ref": "#/$defs/Node"}
				},
				"required": ["label", {"bad": 1}, {"bad": 1}]
			}
		},
		"allOf": [
			{"$ref": "#/$defs/Node"},
			{"properties": {"ghost": "[undefined]", "tags": {"type": "array", "items": [{"type": "integer"}, {"type": "object", "properties": {}}]}}}
		],
		"anyOf": [
			{"type": "null"},
			{"type": "object", "properties": {"rank": {"type": "integer", "enum": [1, 2]}}, "required": ["rank", ["uncomparable"], ["uncomparable"]]}
		]
	}`)

	cleaned, changed, err := CleanAntigravitySchema(raw)
	require.NoError(t, err)
	require.True(t, changed)

	reqBody := []byte(`{"tools":[{"functionDeclarations":[{"name":"ag_fn","parameters":` + string(cleaned) + `}]}],"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)
	violations := testkit.ValidateGeminiRequest(reqBody, testkit.RequestValidationOptions{
		DialectRules: []string{"antigravity-schema"},
	})
	require.Empty(t, violations, "unexpected Antigravity schema violations: %v", violations)

	s := string(cleaned)
	require.NotContains(t, s, "[undefined]")
	require.Contains(t, s, "[Constraint: minLen: 1, pattern: ^[a-z]+$]")
	require.Contains(t, s, "(nullable)")
	require.Contains(t, s, `"reason":{"description":"Reason for calling this tool","type":"string"}`)
	require.Contains(t, s, `"enum":["1","2"]`)
}

// TestCleanSchemasEmptyAndInvalidInputs feeds nil, JSON null, array, and scalar
// schema inputs into CleanGeminiSchema and CleanAntigravitySchema. Empty or null
// schemas must synthesize valid fallback objects, whereas non-object JSON tokens
// must return an error.
func TestCleanSchemasEmptyAndInvalidInputs(t *testing.T) {
	gemFallback, gemChanged, err := CleanGeminiSchema(nil)
	require.NoError(t, err)
	require.True(t, gemChanged)
	require.JSONEq(t, `{"type":"OBJECT","properties":{}}`, string(gemFallback))

	agFallback, agChanged, err := CleanAntigravitySchema(jsontext.Value("null"))
	require.NoError(t, err)
	require.True(t, agChanged)
	require.Contains(t, string(agFallback), `"reason"`)

	_, _, err = CleanGeminiSchema(jsontext.Value(`["not-an-object"]`))
	require.Error(t, err)
	_, _, err = CleanAntigravitySchema(jsontext.Value(`"scalar"`))
	require.Error(t, err)
}
