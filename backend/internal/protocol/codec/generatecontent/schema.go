package generatecontent

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
)

// schema.go implements pure JSON Schema cleaning routines for standard Google
// Gemini (SchemaGemini) and Antigravity internal Gemini (SchemaAntigravity)
// function declarations and structured output schemas. Gemini FunctionDeclaration
// parameters reject Draft-07/2020-12 keywords such as $schema, $ref, $defs, and
// additionalProperties, while Antigravity enforces a strict seven-key whitelist.
//
// CleanGeminiSchema rewrites standard Gemini schemas while preserving json/v2
// lossless numbers: stripping unsupported keywords while keeping user-defined
// property names inside properties maps intact, converting type strings to
// uppercase, selecting the first non-null branch of type arrays, normalizing
// scalar enum entries to JSON strings, and folding integer exclusiveMinimum
// bounds into inclusive minimum. CleanAntigravitySchema rewrites Antigravity
// schemas with bounded $ref expansion, allOf merging, ranked anyOf/oneOf and
// tuple-items branch selection, constraint description hints, empty-object
// reason injection, and [undefined] pruning.

const (
	// maxSchemaRefDepth bounds recursive $ref definition expansion so cyclic
	// self-referencing $defs schemas terminate without exhausting stack space.
	maxSchemaRefDepth = 32
)

var (
	// geminiUnsupportedSchemaKeys lists JSON Schema keywords stripped by
	// CleanGeminiSchema at schema object levels outside properties maps.
	geminiUnsupportedSchemaKeys = map[string]bool{
		"$schema":              true,
		"$id":                  true,
		"$ref":                 true,
		"$defs":                true,
		"definitions":          true,
		"additionalProperties": true,
		"patternProperties":    true,
		"minLength":            true,
		"maxLength":            true,
		"minItems":             true,
		"maxItems":             true,
		"exclusiveMinimum":     true,
		"deprecated":           true,
	}

	// antigravityWhitelistedKeys lists the sole schema keywords permitted on
	// Antigravity internal Gemini function declarations after constraint migration.
	antigravityWhitelistedKeys = map[string]bool{
		"type":        true,
		"description": true,
		"properties":  true,
		"required":    true,
		"items":       true,
		"enum":        true,
		"title":       true,
	}
)

// schemaNumber preserves exact raw JSON numeric tokens across schema decoding,
// integer exclusiveMinimum increments, and deterministic re-serialization.
type schemaNumber string

// MarshalJSONTo writes n directly into enc as a raw JSON numeric token without
// float64 rounding or exponent rewriting. Invalid numeric literals return the
// underlying encoder error from enc.WriteValue.
func (n schemaNumber) MarshalJSONTo(enc *jsontext.Encoder) error {
	return enc.WriteValue(jsontext.Value(n))
}

// String returns the exact decimal JSON spelling of n for fmt display formatting.
func (n schemaNumber) String() string {
	return string(n)
}

// CleanGeminiSchema cleans raw for standard Gemini FunctionDeclaration.parameters
// or generationConfig.responseSchema and reports whether the canonical JSON
// changed from raw. Empty or null inputs synthesize {"properties":{},"type":"OBJECT"}
// and report approximated=true, while non-object schemas return an error.
func CleanGeminiSchema(raw jsontext.Value) (jsontext.Value, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		fallback, err := marshalDeterministic(map[string]any{
			"type":       "OBJECT",
			"properties": map[string]any{},
		})
		if err != nil {
			return nil, false, err
		}
		return jsontext.Value(fallback), true, nil
	}

	parsed, err := decodeSchemaMap(trimmed)
	if err != nil {
		return nil, false, err
	}
	cleaned := CleanGeminiSchemaMap(parsed)
	outBytes, err := marshalDeterministic(cleaned)
	if err != nil {
		return nil, false, err
	}
	origBytes, err := marshalDeterministic(parsed)
	if err != nil {
		return nil, false, err
	}
	return jsontext.Value(outBytes), !bytes.Equal(origBytes, outBytes), nil
}

// CleanGeminiSchemaMap rebuilds a decoded JSON Schema map for standard Gemini
// endpoints without mutating caller-owned containers. Nil input maps return nil
// without allocating a fallback object.
func CleanGeminiSchemaMap(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	cleaned, _ := cleanGeminiSchemaValue(schema).(map[string]any)
	return cleaned
}

// cleanGeminiSchemaValue recursively cleans one schema value while preserving
// arbitrary user-defined property names inside "properties" maps. Nil inputs
// return nil and scalar tokens return unchanged.
//
// Standard Gemini parameters forbid Draft-2020-12 keywords, require single
// uppercase type names, require string enum literals, and reject exclusiveMinimum.
// Treating entries inside "properties" as user-named sub-schemas avoids deleting
// legitimate tool parameters named "deprecated" or "minLength", while INTEGER
// exclusiveMinimum bounds increment into inclusive minimum values when stronger.
func cleanGeminiSchemaValue(schema any) any {
	if schema == nil {
		return nil
	}
	switch v := schema.(type) {
	case map[string]any:
		cleaned := make(map[string]any, len(v))
		for key, value := range v {
			if geminiUnsupportedSchemaKeys[key] {
				continue
			}
			if key == "properties" {
				if propMap, ok := value.(map[string]any); ok {
					cleanedProps := make(map[string]any, len(propMap))
					for propName, propVal := range propMap {
						cleanedProps[propName] = cleanGeminiSchemaValue(propVal)
					}
					cleaned[key] = cleanedProps
					continue
				}
			}
			cleaned[key] = cleanGeminiSchemaValue(value)
		}

		// Compound or unencodable enum entries remove the entire enum array so
		// Gemini never receives a partially filtered set of allowed values.
		if enumSlice, ok := cleaned["enum"].([]any); ok {
			if normalized, valid := normalizeGeminiEnum(enumSlice); valid {
				cleaned["enum"] = normalized
			} else {
				delete(cleaned, "enum")
			}
		}

		// Standard Gemini Schema.type expects a single uppercase type name; type
		// arrays pick their first non-null string or drop "type" when none exists.
		if typeStr, ok := cleaned["type"].(string); ok {
			cleaned["type"] = strings.ToUpper(typeStr)
		} else if typeArr, ok := cleaned["type"].([]any); ok {
			selected := ""
			for _, elem := range typeArr {
				if name, isStr := elem.(string); isStr && !strings.EqualFold(name, "null") {
					selected = strings.ToUpper(name)
					break
				}
			}
			if selected != "" {
				cleaned["type"] = selected
			} else {
				delete(cleaned, "type")
			}
		}

		// Convert integer exclusiveMinimum into an incremented inclusive minimum
		// when no stronger minimum bound is already present on the schema.
		if cleaned["type"] == "INTEGER" {
			if minVal, ok := incrementIntegralSchemaBound(v["exclusiveMinimum"]); ok {
				if existing, exists := cleaned["minimum"]; !exists || schemaNumberLess(existing, minVal) {
					cleaned["minimum"] = minVal
				}
			}
		}
		return cleaned

	case []any:
		cleaned := make([]any, len(v))
		for i, item := range v {
			cleaned[i] = cleanGeminiSchemaValue(item)
		}
		return cleaned

	default:
		return v
	}
}

// normalizeGeminiEnum converts scalar enum values into JSON-formatted strings
// for Gemini's string-only enum constraint. When any element is a map, array, or
// unsupported Go type, it returns (nil, false) so callers omit the invalid enum.
func normalizeGeminiEnum(values []any) ([]any, bool) {
	normalized := make([]any, len(values))
	for i, val := range values {
		if s, ok := val.(string); ok {
			normalized[i] = s
			continue
		}
		switch val.(type) {
		case nil, bool, float32, float64, int, int8, int16, int32, int64,
			uint, uint8, uint16, uint32, uint64, schemaNumber:
			encoded, err := marshalDeterministic(val)
			if err != nil {
				return nil, false
			}
			normalized[i] = string(encoded)
		default:
			return nil, false
		}
	}
	return normalized, true
}

// incrementIntegralSchemaBound adds one to an integral exclusiveMinimum value to
// form an equivalent inclusive INTEGER minimum bound. Fractional numbers, NaN or
// Inf floats, integer overflows, and float64 precision loss return (nil, false).
func incrementIntegralSchemaBound(value any) (any, bool) {
	switch v := value.(type) {
	case schemaNumber:
		if i, err := strconv.ParseInt(string(v), 10, 64); err == nil {
			if i == math.MaxInt64 {
				return nil, false
			}
			return schemaNumber(strconv.FormatInt(i+1, 10)), true
		}
		f, err := strconv.ParseFloat(string(v), 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) || f != math.Trunc(f) || f+1 <= f {
			return nil, false
		}
		return f + 1, true
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) || v+1 <= v {
			return nil, false
		}
		return v + 1, true
	case int:
		if v == math.MaxInt {
			return nil, false
		}
		return v + 1, true
	case int64:
		if v == math.MaxInt64 {
			return nil, false
		}
		return v + 1, true
	default:
		return nil, false
	}
}

// schemaNumberLess compares two finite schema numbers via float64 conversion.
func schemaNumberLess(left, right any) bool {
	lVal, lOK := schemaNumberFloat64(left)
	rVal, rOK := schemaNumberFloat64(right)
	return lOK && rOK && lVal < rVal
}

// schemaNumberFloat64 converts a supported numeric schema value into a finite
// float64 for inclusive bound comparison. Non-numeric Go types, unparseable
// numeric strings, NaN values, and infinities return (0, false).
func schemaNumberFloat64(value any) (float64, bool) {
	switch v := value.(type) {
	case schemaNumber:
		f, err := strconv.ParseFloat(string(v), 64)
		return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
	case float64:
		return v, !math.IsNaN(v) && !math.IsInf(v, 0)
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

// CleanAntigravitySchema cleans raw for Antigravity's strict seven-key Gemini
// schema dialect, pruning "[undefined]" sentinels, expanding root $defs/$ref
// definitions, migrating unsupported constraints into description hints, and
// injecting the required "reason" placeholder on empty lowercase object schemas.
// It returns the deterministic JSON bytes and whether canonical JSON changed.
func CleanAntigravitySchema(raw jsontext.Value) (jsontext.Value, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		empty := map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		}
		cleaned := CleanAntigravitySchemaMap(empty)
		outBytes, err := marshalDeterministic(cleaned)
		if err != nil {
			return nil, false, err
		}
		return jsontext.Value(outBytes), true, nil
	}

	parsed, err := decodeSchemaMap(trimmed)
	if err != nil {
		return nil, false, err
	}
	origBytes, err := marshalDeterministic(parsed)
	if err != nil {
		return nil, false, err
	}

	DeepCleanUndefined(parsed)
	cleaned := CleanAntigravitySchemaMap(parsed)
	outBytes, err := marshalDeterministic(cleaned)
	if err != nil {
		return nil, false, err
	}
	return jsontext.Value(outBytes), !bytes.Equal(origBytes, outBytes), nil
}

// CleanAntigravitySchemaMap adapts a decoded JSON Schema map in place to
// Antigravity's seven-key schema whitelist and returns the cleaned map. Callers
// must pass an unshared map or nil, as $ref flattening and key pruning mutate schema.
func CleanAntigravitySchemaMap(schema map[string]any) map[string]any {
	if schema == nil {
		return nil
	}
	defs := extractRootSchemaDefs(schema)
	flattenSchemaRefs(schema, defs, 0)
	cleaned, _ := cleanAntigravitySchemaNode(schema).(map[string]any)
	return cleaned
}

// DeepCleanUndefined removes map entries whose value equals the literal string
// "[undefined]" across nested map[string]any and []any containers in place. Nil
// or scalar values return immediately without allocation or modification.
func DeepCleanUndefined(value any) {
	switch v := value.(type) {
	case map[string]any:
		for k, elem := range v {
			// Prune "[undefined]" placeholder values emitted by JavaScript clients
			// before schema cleaning inspects properties or constraint hints.
			if s, ok := elem.(string); ok && s == "[undefined]" {
				delete(v, k)
				continue
			}
			DeepCleanUndefined(elem)
		}
	case []any:
		for _, elem := range v {
			DeepCleanUndefined(elem)
		}
	}
}

// extractRootSchemaDefs deletes map-valued root "$defs" and "definitions"
// entries from schema in place and returns their merged lookup table. Entries
// present under both keys prefer "definitions" on collision, and absent maps
// return an empty lookup table.
func extractRootSchemaDefs(schema map[string]any) map[string]any {
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

// flattenSchemaRefs expands "$ref" pointers inside schema in place against defs
// up to maxSchemaRefDepth hops. Substituted definition fields are deep-copied
// without overwriting existing local keys, and cyclic or deeper references strip
// "$ref" cleanly once depth reaches maxSchemaRefDepth.
func flattenSchemaRefs(schema map[string]any, defs map[string]any, depth int) {
	if len(defs) == 0 || depth >= maxSchemaRefDepth {
		return
	}
	if ref, ok := schema["$ref"].(string); ok {
		delete(schema, "$ref")
		parts := strings.Split(ref, "/")
		refName := parts[len(parts)-1]
		if defVal, exists := defs[refName]; exists {
			if defMap, isMap := defVal.(map[string]any); isMap {
				for k, v := range defMap {
					if _, hasLocal := schema[k]; !hasLocal {
						schema[k] = deepCopySchemaValue(v)
					}
				}
				flattenSchemaRefs(schema, defs, depth+1)
			}
		}
	}

	for k, v := range schema {
		if k == "properties" {
			if props, ok := v.(map[string]any); ok {
				for _, propVal := range props {
					if propMap, isMap := propVal.(map[string]any); isMap {
						flattenSchemaRefs(propMap, defs, depth+1)
					}
				}
				continue
			}
		}
		if subMap, ok := v.(map[string]any); ok {
			flattenSchemaRefs(subMap, defs, depth+1)
		} else if subArr, ok := v.([]any); ok {
			for _, elem := range subArr {
				if elemMap, isMap := elem.(map[string]any); isMap {
					flattenSchemaRefs(elemMap, defs, depth+1)
				}
			}
		}
	}
}

// deepCopySchemaValue recursively clones map[string]any and []any containers so schema cleaning never mutates shared subtrees.
func deepCopySchemaValue(src any) any {
	switch v := src.(type) {
	case map[string]any:
		dst := make(map[string]any, len(v))
		for k, val := range v {
			dst[k] = deepCopySchemaValue(val)
		}
		return dst
	case []any:
		dst := make([]any, len(v))
		for i, val := range v {
			dst[i] = deepCopySchemaValue(val)
		}
		return dst
	default:
		return src
	}
}

// cleanAntigravitySchemaNode normalizes one schema node in place, enforcing the
// seven-key Antigravity whitelist on every schema map while treating entries
// inside "properties" as user-named sub-schema definitions. Non-map inputs
// return unchanged.
//
// Merging allOf and ranked anyOf/oneOf representatives runs before recursive
// property and item cleaning so newly contributed branch fields also pass the
// seven-key whitelist. Empty lowercase object schemas gain Antigravity's
// required "reason" string property before type array and enum normalization.
func cleanAntigravitySchemaNode(value any) any {
	schemaMap, ok := value.(map[string]any)
	if !ok {
		return value
	}

	mergeSchemaAllOf(schemaMap)

	// Select anyOf/oneOf union representatives before recursing into properties
	// or items so newly contributed branch fields also pass recursive whitelist cleaning.
	var unionArray []any
	typeStr, _ := schemaMap["type"].(string)
	if typeStr == "" || typeStr == "object" {
		if anyOf, isArr := schemaMap["anyOf"].([]any); isArr {
			unionArray = anyOf
		} else if oneOf, isArr := schemaMap["oneOf"].([]any); isArr {
			unionArray = oneOf
		}
	}
	if len(unionArray) > 0 {
		if bestBranch := extractBestUnionBranch(unionArray); bestBranch != nil {
			if bestMap, isMap := bestBranch.(map[string]any); isMap {
				mergeSelectedUnionBranch(schemaMap, bestMap)
			}
		}
	}

	if props, hasProps := schemaMap["properties"].(map[string]any); hasProps {
		for propName, propVal := range props {
			props[propName] = cleanAntigravitySchemaNode(propVal)
		}
	} else {
		delete(schemaMap, "properties")
	}

	if items, hasItems := schemaMap["items"]; hasItems {
		if itemsArr, isArr := items.([]any); isArr {
			// Positional JSON Schema tuple arrays collapse to their highest-ranked
			// element schema or a fallback string item so items is always an object.
			best := extractBestUnionBranch(itemsArr)
			bestMap, isMap := best.(map[string]any)
			if !isMap || bestMap == nil {
				bestMap = map[string]any{"type": "string"}
			}
			schemaMap["items"] = cleanAntigravitySchemaNode(deepCopySchemaValue(bestMap))
		} else if itemsMap, isMap := items.(map[string]any); isMap {
			schemaMap["items"] = cleanAntigravitySchemaNode(itemsMap)
		} else {
			delete(schemaMap, "items")
		}
	}

	migrateAntigravityConstraints(schemaMap)

	for k := range schemaMap {
		if !antigravityWhitelistedKeys[k] {
			delete(schemaMap, k)
		}
	}

	// Empty lowercase object schemas receive Antigravity's synthetic "reason"
	// parameter so internal tool declarations always expose at least one property.
	if t, isStr := schemaMap["type"].(string); isStr && t == "object" {
		props, _ := schemaMap["properties"].(map[string]any)
		if len(props) == 0 {
			schemaMap["properties"] = map[string]any{
				"reason": map[string]any{
					"type":        "string",
					"description": "Reason for calling this tool",
				},
			}
			schemaMap["required"] = []any{"reason"}
		}
	}

	if props, hasProps := schemaMap["properties"].(map[string]any); hasProps {
		if req, hasReq := schemaMap["required"].([]any); hasReq {
			var validReq []any
			for _, r := range req {
				if rStr, isStr := r.(string); isStr {
					if _, exists := props[rStr]; exists && !containsAnyString(validReq, rStr) {
						validReq = append(validReq, rStr)
					}
				}
			}
			if len(validReq) > 0 {
				schemaMap["required"] = validReq
			} else {
				delete(schemaMap, "required")
			}
		}
	} else if req, hasReq := schemaMap["required"].([]any); hasReq {
		var validReq []any
		for _, r := range req {
			if rStr, isStr := r.(string); isStr && !containsAnyString(validReq, rStr) {
				validReq = append(validReq, rStr)
			}
		}
		if len(validReq) > 0 {
			schemaMap["required"] = validReq
		} else {
			delete(schemaMap, "required")
		}
	}

	normalizeAntigravityTypeAndEnum(schemaMap)
	return schemaMap
}

// mergeSelectedUnionBranch merges properties, string required entries, and
// missing fields from bestMap into schemaMap in place. Existing keys on
// schemaMap take precedence, and non-string entries inside malformed required
// arrays are skipped without panicking on non-comparable values.
func mergeSelectedUnionBranch(schemaMap, bestMap map[string]any) {
	for k, v := range bestMap {
		switch k {
		case "properties":
			sourceProps, ok := v.(map[string]any)
			if !ok {
				continue
			}
			targetProps, _ := schemaMap["properties"].(map[string]any)
			if targetProps == nil {
				targetProps = make(map[string]any, len(sourceProps))
				schemaMap["properties"] = targetProps
			}
			for pk, pv := range sourceProps {
				if _, exists := targetProps[pk]; !exists {
					targetProps[pk] = deepCopySchemaValue(pv)
				}
			}
		case "required":
			sourceReq, ok := v.([]any)
			if !ok {
				continue
			}
			var targetReq []any
			if existingReq, isArr := schemaMap["required"].([]any); isArr {
				for _, er := range existingReq {
					if s, isStr := er.(string); isStr && !containsAnyString(targetReq, s) {
						targetReq = append(targetReq, s)
					}
				}
			}
			for _, rv := range sourceReq {
				if s, isStr := rv.(string); isStr && !containsAnyString(targetReq, s) {
					targetReq = append(targetReq, s)
				}
			}
			if len(targetReq) > 0 {
				schemaMap["required"] = targetReq
			}
		default:
			if _, exists := schemaMap[k]; !exists {
				schemaMap[k] = deepCopySchemaValue(v)
			}
		}
	}
}

// normalizeAntigravityTypeAndEnum normalizes schemaMap["type"] in place to a
// single lowercase string (defaulting missing types to "object" and null-only
// types to "string") while appending "(nullable)" to description when null was
// removed. Non-string enum elements stringify in place and coerce type to
// "string", whereas non-slice enum fields are deleted.
func normalizeAntigravityTypeAndEnum(schemaMap map[string]any) {
	isNullable := false
	if typeVal, exists := schemaMap["type"]; exists {
		selectedType := ""
		switch v := typeVal.(type) {
		case string:
			lower := strings.ToLower(v)
			if lower == "null" {
				isNullable = true
				selectedType = "string"
			} else if lower != "" {
				selectedType = lower
			} else {
				selectedType = "string"
			}
		case []any:
			for _, elem := range v {
				if ts, ok := elem.(string); ok {
					lower := strings.ToLower(ts)
					if lower == "null" {
						isNullable = true
					} else if selectedType == "" && lower != "" {
						selectedType = lower
					}
				}
			}
			if selectedType == "" {
				selectedType = "string"
			}
		default:
			selectedType = "object"
		}
		schemaMap["type"] = selectedType
	} else {
		schemaMap["type"] = "object"
	}

	if isNullable {
		desc, _ := schemaMap["description"].(string)
		if !strings.Contains(desc, "nullable") {
			if desc != "" {
				desc += " "
			}
			desc += "(nullable)"
			schemaMap["description"] = desc
		}
	}

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
	} else {
		delete(schemaMap, "enum")
	}
}

// migrateAntigravityConstraints appends unsupported JSON Schema validation
// keywords onto m["description"] in deterministic label order before the
// Antigravity whitelist deletes those keywords. Duplicate `[Constraint: ...]`
// suffixes are suppressed when m["description"] already contains the exact hint.
func migrateAntigravityConstraints(m map[string]any) {
	constraints := []struct {
		key   string // JSON Schema validation keyword
		label string // compact hint label written into description
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
	if len(hints) > 0 {
		suffix := fmt.Sprintf(" [Constraint: %s]", strings.Join(hints, ", "))
		desc, _ := m["description"].(string)
		if !strings.Contains(desc, suffix) {
			m["description"] = desc + suffix
		}
	}
}

// mergeSchemaAllOf folds immediate map branches from m["allOf"] into m in place
// and deletes "allOf". Parent fields on m remain authoritative on collision,
// while newly contributed string required names sort deterministically.
func mergeSchemaAllOf(m map[string]any) {
	allOf, ok := m["allOf"].([]any)
	if !ok {
		return
	}
	delete(m, "allOf")

	mergedProps := make(map[string]any)
	var extraReqs []string
	seenReq := make(map[string]bool)
	otherFields := make(map[string]any)

	for _, sub := range allOf {
		subMap, isMap := sub.(map[string]any)
		if !isMap {
			continue
		}
		if props, hasProps := subMap["properties"].(map[string]any); hasProps {
			for pk, pv := range props {
				mergedProps[pk] = deepCopySchemaValue(pv)
			}
		}
		if reqs, hasReqs := subMap["required"].([]any); hasReqs {
			for _, r := range reqs {
				if s, isStr := r.(string); isStr && !seenReq[s] {
					seenReq[s] = true
					extraReqs = append(extraReqs, s)
				}
			}
		}
		for k, v := range subMap {
			if k != "properties" && k != "required" && k != "allOf" {
				if _, exists := otherFields[k]; !exists {
					otherFields[k] = deepCopySchemaValue(v)
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
			existProps = make(map[string]any, len(mergedProps))
			m["properties"] = existProps
		}
		for k, v := range mergedProps {
			if _, exists := existProps[k]; !exists {
				existProps[k] = v
			}
		}
	}
	if len(extraReqs) > 0 {
		var validReqs []any
		if existReq, isArr := m["required"].([]any); isArr {
			for _, r := range existReq {
				if s, isStr := r.(string); isStr && !containsAnyString(validReqs, s) {
					validReqs = append(validReqs, s)
				}
			}
		}
		slices.Sort(extraReqs)
		for _, s := range extraReqs {
			if !containsAnyString(validReqs, s) {
				validReqs = append(validReqs, s)
			}
		}
		m["required"] = validReqs
	}
}

// extractBestUnionBranch picks the first element in unionArray achieving the
// highest structural score (object > array > non-null scalar > untyped). Empty
// unionArray slices return nil without allocating a fallback map.
func extractBestUnionBranch(unionArray []any) any {
	var best any
	bestScore := -1
	for _, item := range unionArray {
		score := scoreUnionSchemaOption(item)
		if score > bestScore {
			bestScore = score
			best = item
		}
	}
	return best
}

// scoreUnionSchemaOption ranks a union candidate map by structural richness.
func scoreUnionSchemaOption(val any) int {
	m, ok := val.(map[string]any)
	if !ok {
		return 0
	}
	typeStr, _ := m["type"].(string)
	if _, hasProps := m["properties"]; hasProps || typeStr == "object" {
		return 3
	}
	if _, hasItems := m["items"]; hasItems || typeStr == "array" {
		return 2
	}
	if typeStr != "" && typeStr != "null" {
		return 1
	}
	return 0
}

// containsAnyString reports whether items already contains a string equal to target.
func containsAnyString(items []any, target string) bool {
	for _, item := range items {
		if s, ok := item.(string); ok && s == target {
			return true
		}
	}
	return false
}

// decodeSchemaMap parses trimmed JSON object bytes into a map[string]any while
// keeping JSON numbers as exact schemaNumber tokens. Empty inputs, non-object
// roots, duplicate object keys, or trailing JSON tokens return an error.
func decodeSchemaMap(trimmed []byte) (map[string]any, error) {
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("schema must be a JSON object")
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	val, err := readLosslessSchemaValue(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); err == nil {
		return nil, errors.New("trailing JSON token after schema object")
	}
	m, ok := val.(map[string]any)
	if !ok {
		return nil, errors.New("schema root must be a JSON object")
	}
	return m, nil
}

// readLosslessSchemaValue consumes one complete JSON value from dec into
// map[string]any, []any, string, bool, nil, or schemaNumber. Syntax errors or
// duplicate object member names detected by dec return an error immediately.
func readLosslessSchemaValue(dec *jsontext.Decoder) (any, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	switch tok.Kind() {
	case 'n':
		return nil, nil
	case 't':
		return true, nil
	case 'f':
		return false, nil
	case '"':
		return tok.String(), nil
	case '0':
		return schemaNumber(tok.String()), nil
	case '{':
		obj := make(map[string]any)
		for dec.PeekKind() != '}' {
			kTok, err := dec.ReadToken()
			if err != nil {
				return nil, err
			}
			key := kTok.String()
			val, err := readLosslessSchemaValue(dec)
			if err != nil {
				return nil, err
			}
			obj[key] = val
		}
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		return obj, nil
	case '[':
		arr := make([]any, 0)
		for dec.PeekKind() != ']' {
			elem, err := readLosslessSchemaValue(dec)
			if err != nil {
				return nil, err
			}
			arr = append(arr, elem)
		}
		if _, err := dec.ReadToken(); err != nil {
			return nil, err
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("unexpected JSON token %v", tok.Kind())
	}
}
