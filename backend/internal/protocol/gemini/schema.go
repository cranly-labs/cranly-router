package gemini

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// schema.go adapts JSON Schema tool parameter maps in place to the restricted
// seven-key lowercase subset accepted by internal Gemini function declarations.
// Internal Gemini function declarations reject $defs, $ref, allOf, anyOf,
// oneOf, tuple item arrays, nullable type unions, and numeric or string length
// validators. Root definitions expand through their final slash component,
// allOf merges immediate object branches, and unions or tuple items select a
// ranked representative schema before unsupported fields are stripped.
//
// Unsupported validation keywords are preserved as human-readable
// "[Constraint: ...]" suffixes in description before key whitelisting removes
// them, while empty lowercase object schemas receive a required "reason" string
// parameter and non-string enum literals coerce to string representations.
// Because CleanJSONSchema and DeepCleanUndefined mutate caller-owned maps in
// place without cycle detection, callers pass acyclic JSON-decoded containers
// and clone shared schemas beforehand when needed.

// CleanJSONSchema adapts schema in place to the restricted internal Gemini
// function parameter subset and returns the mutated map, or nil when schema is
// nil. Root $defs and definitions are extracted and deleted before nested $ref
// expansion and recursive approximation.
//
// Reference expansion, allOf merging, union branch selection, constraint hint
// migration, empty-object "reason" injection, and enum stringification narrow
// or loosen JSON Schema validation rules to avoid upstream HTTP 400 rejections.
// Callers supply acyclic map[string]any and []any containers with comparable
// union required entries.
func CleanJSONSchema(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	// Extract and delete root $defs and definitions before expanding nested
	// $ref targets so root definition keys never reach the final whitelist.
	flattenRefs(schema, extractDefs(schema))

	cleaned := cleanJSONSchemaRecursive(schema)
	result, ok := cleaned.(map[string]any)
	if !ok {
		return nil
	}

	return result
}

// DeepCleanUndefined deletes map entries whose value equals the exact string
// "[undefined]" across nested map[string]any and []any containers in place.
// Nil inputs and scalar array elements remain unchanged; callers supply acyclic
// containers.
func DeepCleanUndefined(value any) {
	if value == nil {
		return
	}
	switch v := value.(type) {
	case map[string]any:
		for k, val := range v {
			// Delete map keys holding the literal "[undefined]" placeholder string
			// while keeping positional elements inside JSON arrays untouched.
			if s, ok := val.(string); ok && s == "[undefined]" {
				delete(v, k)
				continue
			}
			DeepCleanUndefined(val)
		}
	case []any:
		for _, val := range v {
			DeepCleanUndefined(val)
		}
	}
}

// extractDefs deletes map-valued root "$defs" and "definitions" entries from
// schema in place and returns their combined definition map. Legacy
// "definitions" entries overwrite duplicate "$defs" keys, while non-map values
// stay untouched on schema.
func extractDefs(schema map[string]any) map[string]any {
	defs := make(map[string]any)
	if d, ok := schema["$defs"].(map[string]any); ok {
		maps.Copy(defs, d)
		delete(schema, "$defs")
	}
	if d, ok := schema["definitions"].(map[string]any); ok {
		maps.Copy(defs, d)
		delete(schema, "definitions")
	}
	return defs
}

// flattenRefs expands string "$ref" entries in schema using defs by matching
// the final slash-separated segment and copying missing definition keys via
// deepCopy. Callers pass terminating reference chains and acyclic containers.
func flattenRefs(schema map[string]any, defs map[string]any) {
	if len(defs) == 0 {
		return
	}

	if ref, ok := schema["$ref"].(string); ok {
		delete(schema, "$ref")
		// Resolve root definition names from the trailing slash segment without
		// interpreting external URI prefixes or JSON Pointer escapes.
		parts := strings.Split(ref, "/")
		refName := parts[len(parts)-1]

		if defSchema, exists := defs[refName]; exists {
			if defMap, ok := defSchema.(map[string]any); ok {
				// Preserve keys already declared at the $ref site and deep-copy
				// newly populated containers before recursive cleanup mutates them.
				for k, v := range defMap {
					if _, has := schema[k]; !has {
						schema[k] = deepCopy(v)
					}
				}
				flattenRefs(schema, defs)
			}
		}
	}

	for _, v := range schema {
		if subMap, ok := v.(map[string]any); ok {
			flattenRefs(subMap, defs)
		} else if subArr, ok := v.([]any); ok {
			for _, item := range subArr {
				if itemMap, ok := item.(map[string]any); ok {
					flattenRefs(itemMap, defs)
				}
			}
		}
	}
}

// deepCopy recursively allocates fresh map[string]any and []any containers from
// src so later in-place schema cleanup cannot mutate shared definitions. Nil
// inputs return nil, while scalar values and other dynamic types return as
// supplied; callers pass acyclic JSON-decoded values.
func deepCopy(src any) any {
	if src == nil {
		return nil
	}
	switch v := src.(type) {
	case map[string]any:
		dst := make(map[string]any, len(v))
		for k, val := range v {
			dst[k] = deepCopy(val)
		}
		return dst
	case []any:
		dst := make([]any, len(v))
		for i, val := range v {
			dst[i] = deepCopy(val)
		}
		return dst
	default:
		return src
	}
}

// cleanJSONSchemaRecursive approximates a decoded JSON Schema map in place and
// returns non-map values unchanged. It merges immediate allOf branches first,
// visits properties maps preferentially over items or generic nested
// containers, and folds the highest-ranked anyOf or oneOf branch into untyped
// or object parents.
//
// Maps carrying schema keywords then migrate unsupported validation bounds into
// description hints before applying the seven-key whitelist. Empty lowercase
// object schemas gain a required "reason" property prior to nullable type
// normalization so late object defaults do not receive synthetic parameters,
// and non-string enum literals coerce to strings.
func cleanJSONSchemaRecursive(value any) any {
	schemaMap, ok := value.(map[string]any)
	if !ok {
		return value
	}

	// Merge immediate allOf branches first so contributed properties and
	// required names participate in property traversal and whitelisting below.
	mergeAllOf(schemaMap)

	// Traverse properties maps preferentially over items or generic nested
	// containers so property-name maps are not treated as schema objects.
	if props, ok := schemaMap["properties"].(map[string]any); ok {
		for _, v := range props {
			cleanJSONSchemaRecursive(v)
		}
	} else if items, ok := schemaMap["items"]; ok {
		// Collapse tuple item arrays to their highest-ranked representative
		// schema because Gemini array declarations accept a single item schema.
		if itemsArr, ok := items.([]any); ok {
			best := extractBestSchemaFromUnion(itemsArr)
			if best == nil {
				// Substitute a string item schema when an empty or zero-scored
				// tuple array yields a nil representative.
				best = map[string]any{"type": "string"}
			}
			cleanedBest := cleanJSONSchemaRecursive(best)
			schemaMap["items"] = cleanedBest
		} else {
			cleanJSONSchemaRecursive(items)
		}
	} else {
		for _, v := range schemaMap {
			if _, isMap := v.(map[string]any); isMap {
				cleanJSONSchemaRecursive(v)
			} else if arr, isArr := v.([]any); isArr {
				for _, item := range arr {
					cleanJSONSchemaRecursive(item)
				}
			}
		}
	}

	// Fold anyOf or oneOf into the parent when type is absent, empty, or
	// "object" so union-typed parameters retain their richest object/array shape.
	var unionArray []any
	typeStr, _ := schemaMap["type"].(string)
	if typeStr == "" || typeStr == "object" {
		if anyOf, ok := schemaMap["anyOf"].([]any); ok {
			unionArray = anyOf
		} else if oneOf, ok := schemaMap["oneOf"].([]any); ok {
			unionArray = oneOf
		}
	}

	// Copy missing fields, properties, and required names from the selected
	// union branch while keeping explicit parent declarations authoritative.
	if len(unionArray) > 0 {
		if bestBranch := extractBestSchemaFromUnion(unionArray); bestBranch != nil {
			if bestMap, ok := bestBranch.(map[string]any); ok {
				for k, v := range bestMap {
					if k == "properties" {
						targetProps, _ := schemaMap["properties"].(map[string]any)
						if targetProps == nil {
							targetProps = make(map[string]any)
							schemaMap["properties"] = targetProps
						}
						if sourceProps, ok := v.(map[string]any); ok {
							for pk, pv := range sourceProps {
								if _, exists := targetProps[pk]; !exists {
									targetProps[pk] = deepCopy(pv)
								}
							}
						}
					} else if k == "required" {
						targetReq, _ := schemaMap["required"].([]any)
						if sourceReq, ok := v.([]any); ok {
							for _, rv := range sourceReq {
								if !slices.Contains(targetReq, rv) {
									targetReq = append(targetReq, rv)
								}
							}
							schemaMap["required"] = targetReq
						}
					} else if _, exists := schemaMap[k]; !exists {
						schemaMap[k] = deepCopy(v)
					}
				}
			}
		}
	}

	// Whitelist only maps that carry schema structural keys so arbitrary
	// property-name maps keep all declared parameter names intact.
	looksLikeSchema := hasKey(schemaMap, "type") ||
		hasKey(schemaMap, "properties") ||
		hasKey(schemaMap, "items") ||
		hasKey(schemaMap, "enum") ||
		hasKey(schemaMap, "anyOf") ||
		hasKey(schemaMap, "oneOf") ||
		hasKey(schemaMap, "allOf")

	if looksLikeSchema {
		// Record unsupported validation bounds as readable description suffixes
		// before the seven-key whitelist deletes those validation keywords from schemaMap.
		migrateConstraints(schemaMap)

		allowedFields := map[string]bool{
			"type":        true,
			"description": true,
			"properties":  true,
			"required":    true,
			"items":       true,
			"enum":        true,
			"title":       true,
		}
		for k := range schemaMap {
			if !allowedFields[k] {
				delete(schemaMap, k)
			}
		}

		// Inject a required "reason" string property into empty lowercase object
		// schemas so Antigravity upstream accepts zero-parameter tool definitions.
		if t, _ := schemaMap["type"].(string); t == "object" {
			hasProps := false
			if props, ok := schemaMap["properties"].(map[string]any); ok && len(props) > 0 {
				hasProps = true
			}
			if !hasProps {
				schemaMap["properties"] = map[string]any{
					"reason": map[string]any{
						"type":        "string",
						"description": "Reason for calling this tool",
					},
				}
				schemaMap["required"] = []any{"reason"}
			}
		}

		// Prune required entries that do not match an existing property key so
		// upstream validators never reject orphaned required names.
		if props, ok := schemaMap["properties"].(map[string]any); ok {
			if req, ok := schemaMap["required"].([]any); ok {
				var validReq []any
				for _, r := range req {
					if rStr, ok := r.(string); ok {
						if _, exists := props[rStr]; exists {
							validReq = append(validReq, r)
						}
					}
				}
				if len(validReq) > 0 {
					schemaMap["required"] = validReq
				} else {
					delete(schemaMap, "required")
				}
			}
		}

		// Select a single lowercase non-null type and record nullability in
		// description after empty-object injection has run.
		isEffectivelyNullable := false
		if typeVal, exists := schemaMap["type"]; exists {
			var selectedType string
			switch v := typeVal.(type) {
			case string:
				lower := strings.ToLower(v)
				if lower == "null" {
					isEffectivelyNullable = true
					selectedType = "string"
				} else {
					selectedType = lower
				}
			case []any:
				// Pick the first non-null type string from a JSON Schema type
				// union and fall back to "string" when only "null" was listed.
				for _, t := range v {
					if ts, ok := t.(string); ok {
						lower := strings.ToLower(ts)
						if lower == "null" {
							isEffectivelyNullable = true
						} else if selectedType == "" {
							selectedType = lower
						}
					}
				}
				if selectedType == "" {
					selectedType = "string"
				}
			}
			schemaMap["type"] = selectedType
		} else {
			schemaMap["type"] = "object"
		}

		// Append "(nullable)" once to description when a null alternative was
		// collapsed out of the schema's type union or literal.
		if isEffectivelyNullable {
			desc, _ := schemaMap["description"].(string)
			if !strings.Contains(desc, "nullable") {
				if desc != "" {
					desc += " "
				}
				desc += "(nullable)"
				schemaMap["description"] = desc
			}
		}

		// Coerce non-string enum literals to strings and switch type to "string"
		// because internal Gemini enum declarations require string literals.
		if enumVals, ok := schemaMap["enum"].([]any); ok {
			hasNonString := false
			for i, val := range enumVals {
				if _, isStr := val.(string); !isStr {
					hasNonString = true
					if val == nil {
						enumVals[i] = "null"
					} else {
						enumVals[i] = fmt.Sprintf("%v", val)
					}
				}
			}
			if hasNonString {
				schemaMap["type"] = "string"
			}
		}
	}

	return schemaMap
}

// hasKey reports whether m contains key k, even when the stored value is nil.
func hasKey(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}

// migrateConstraints mutates m in place to append non-nil validation keywords
// onto m["description"] in fixed key order as a "[Constraint: ...]" suffix.
// Existing string descriptions stay ahead of the suffix, and an identical
// suffix already present in description is not appended again.
func migrateConstraints(m map[string]any) {
	constraints := []struct {
		key   string // source JSON Schema validation keyword
		label string // short label emitted inside the description hint
	}{
		{"minLength", "minLen"},
		{"maxLength", "maxLen"},
		{"pattern", "pattern"},
		{"minimum", "min"},
		{"maximum", "max"},
		{"multipleOf", "multipleOf"},
		{"exclusiveMinimum", "exclMin"},
		{"exclusiveMaximum", "exclMax"},
		{"minItems", "minItems"},
		{"maxItems", "maxItems"},
		{"propertyNames", "propertyNames"},
		{"format", "format"},
	}

	var hints []string
	for _, c := range constraints {
		if val, ok := m[c.key]; ok && val != nil {
			hints = append(hints, fmt.Sprintf("%s: %v", c.label, val))
		}
	}

	// Skip appending when description already contains the exact generated
	// suffix so repeated CleanJSONSchema passes remain idempotent on shared maps.
	if len(hints) > 0 {
		suffix := fmt.Sprintf(" [Constraint: %s]", strings.Join(hints, ", "))
		desc, _ := m["description"].(string)
		if !strings.Contains(desc, suffix) {
			m["description"] = desc + suffix
		}
	}
}

// mergeAllOf deletes an array-valued "allOf" key from m and folds immediate
// map branches into m in place. Explicit parent keys take precedence over
// branch values, while later branches override earlier branch properties and
// deduplicated string required names append after existing parent entries.
func mergeAllOf(m map[string]any) {
	allOf, ok := m["allOf"].([]any)
	if !ok {
		return
	}
	delete(m, "allOf")

	mergedProps := make(map[string]any)
	mergedReq := make(map[string]bool)
	otherFields := make(map[string]any)

	// Let later allOf branches override earlier branch properties while keeping
	// first-seen non-property fields and preserving explicit parent keys below.
	for _, sub := range allOf {
		if subMap, ok := sub.(map[string]any); ok {
			if props, ok := subMap["properties"].(map[string]any); ok {
				maps.Copy(mergedProps, props)
			}
			if reqs, ok := subMap["required"].([]any); ok {
				for _, r := range reqs {
					if s, ok := r.(string); ok {
						mergedReq[s] = true
					}
				}
			}
			for k, v := range subMap {
				if k != "properties" && k != "required" && k != "allOf" {
					if _, exists := otherFields[k]; !exists {
						otherFields[k] = v
					}
				}
			}
		}
	}

	for k, v := range otherFields {
		if _, exists := m[k]; !exists {
			m[k] = v
		}
	}
	if len(mergedProps) > 0 {
		existProps, _ := m["properties"].(map[string]any)
		if existProps == nil {
			existProps = make(map[string]any)
			m["properties"] = existProps
		}
		for k, v := range mergedProps {
			if _, exists := existProps[k]; !exists {
				existProps[k] = v
			}
		}
	}
	if len(mergedReq) > 0 {
		existReq, _ := m["required"].([]any)
		var validReqs []any
		for _, r := range existReq {
			if s, ok := r.(string); ok {
				validReqs = append(validReqs, s)
				delete(mergedReq, s)
			}
		}
		for r := range mergedReq {
			validReqs = append(validReqs, r)
		}
		m["required"] = validReqs
	}
}

// extractBestSchemaFromUnion selects the first element in unionArray with the
// highest scoreSchemaOption rank so equal-ranked branches preserve input order.
// Empty slices return nil, and a returned map shares storage with unionArray.
func extractBestSchemaFromUnion(unionArray []any) any {
	var bestOption any
	bestScore := -1

	// Require a strictly higher score before replacing bestOption so union
	// branches with identical ranks preserve their input declaration order.
	for _, item := range unionArray {
		score := scoreSchemaOption(item)
		if score > bestScore {
			bestScore = score
			bestOption = item
		}
	}
	return bestOption
}

// scoreSchemaOption ranks object schemas (3) above array schemas (2), non-null
// scalar types (1), and null or non-map candidates (0).
func scoreSchemaOption(val any) int {
	m, ok := val.(map[string]any)
	if !ok {
		return 0
	}
	typeStr, _ := m["type"].(string)

	if hasKey(m, "properties") || typeStr == "object" {
		return 3
	}
	if hasKey(m, "items") || typeStr == "array" {
		return 2
	}
	if typeStr != "" && typeStr != "null" {
		return 1
	}
	return 0
}
