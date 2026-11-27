package gemini

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCleanJSONSchema_ExpandsRefsMergesCompositionsAndMigratesConstraints
// builds a composite object schema with $defs/$ref, allOf, anyOf, nullable type
// unions, non-string enums, and string constraints. CleanJSONSchema must inline
// definitions, keep only whitelisted keys with constraint hints, coerce enums
// to strings, and remain idempotent on a second pass.
func TestCleanJSONSchema_ExpandsRefsMergesCompositionsAndMigratesConstraints(t *testing.T) {
	require.Nil(t, CleanJSONSchema(nil))

	schema := map[string]any{
		"type": "object",
		"$defs": map[string]any{
			"Target": map[string]any{
				"type":      "string",
				"minLength": 2,
				"pattern":   "^[a-z]+$",
			},
		},
		"allOf": []any{
			map[string]any{
				"properties": map[string]any{
					"target": map[string]any{
						"$ref":        "#/$defs/Target",
						"description": "Path slug",
					},
				},
				"required": []any{"target", "missing_prop"},
			},
		},
		"properties": map[string]any{
			"mode": map[string]any{
				"type": []any{"null", "INTEGER"},
				"enum": []any{1, 2, nil},
			},
			"payload": map[string]any{
				"anyOf": []any{
					map[string]any{"type": "null"},
					map[string]any{
						"type": "object",
						"properties": map[string]any{
							"flag": map[string]any{"type": "boolean"},
						},
					},
				},
			},
		},
		"additionalProperties": false,
	}

	cleaned := CleanJSONSchema(schema)
	require.NotNil(t, cleaned)
	require.NotContains(t, cleaned, "$defs")
	require.NotContains(t, cleaned, "allOf")
	require.NotContains(t, cleaned, "additionalProperties")
	require.Equal(t, []any{"target"}, cleaned["required"])

	props, ok := cleaned["properties"].(map[string]any)
	require.True(t, ok)

	targetProp, ok := props["target"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "string", targetProp["type"])
	require.Equal(t, "Path slug [Constraint: minLen: 2, pattern: ^[a-z]+$]", targetProp["description"])
	require.NotContains(t, targetProp, "minLength")
	require.NotContains(t, targetProp, "$ref")

	modeProp, ok := props["mode"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "string", modeProp["type"])
	require.Equal(t, "(nullable)", modeProp["description"])
	require.Equal(t, []any{"1", "2", "null"}, modeProp["enum"])

	payloadProp, ok := props["payload"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "object", payloadProp["type"])
	payloadSubProps, ok := payloadProp["properties"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, payloadSubProps, "flag")

	// Second pass must remain idempotent without duplicating constraint hints.
	again := CleanJSONSchema(cleaned)
	againProps, ok := again["properties"].(map[string]any)
	require.True(t, ok)
	againTarget, ok := againProps["target"].(map[string]any)
	require.True(t, ok)
	require.Equal(
		t,
		"Path slug [Constraint: minLen: 2, pattern: ^[a-z]+$]",
		againTarget["description"],
	)
}

// TestCleanJSONSchema_EmptyObjectInjectsReasonAndDeepCleanUndefinedStripsSentinel
// supplies an empty lowercase object schema and a tuple-typed array carrying
// literal "[undefined]" map entries. Cleanup must remove "[undefined]" keys,
// inject a required "reason" string property on the empty object, and select
// the object branch as the array item schema.
func TestCleanJSONSchema_EmptyObjectInjectsReasonAndDeepCleanUndefinedStripsSentinel(t *testing.T) {
	raw := map[string]any{
		"type":        "object",
		"description": "[undefined]",
		"properties":  map[string]any{},
	}
	DeepCleanUndefined(raw)
	require.NotContains(t, raw, "description")

	cleaned := CleanJSONSchema(raw)
	require.Equal(t, "object", cleaned["type"])
	require.Equal(t, []any{"reason"}, cleaned["required"])
	props, ok := cleaned["properties"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, props, "reason")

	tupleArr := CleanJSONSchema(map[string]any{
		"type": "array",
		"items": []any{
			map[string]any{"type": "string"},
			map[string]any{
				"type": "object",
				"properties": map[string]any{
					"id": map[string]any{"type": "string", "unused": "[undefined]"},
				},
			},
		},
	})
	DeepCleanUndefined(tupleArr)
	itemSchema, ok := tupleArr["items"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "object", itemSchema["type"])
}
