package testkit

import (
	"fmt"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// request_rules_gemini.go validates Google Gemini GenerateContent request JSON
// payloads against user/model role constraints, functionCall/functionResponse
// turn pairing, FunctionDeclaration naming, gemini-signature step thought
// signatures, Gemini and Antigravity schema keyword subsets, and thinkingBudget
// versus maxOutputTokens ordering.
//
// Gemini FunctionDeclaration.parameters rejects JSON Schema keywords such as
// $ref and additionalProperties while allowing arbitrary user property names
// inside properties maps, and thinking-enabled Gemini models require a
// non-empty thoughtSignature on the first functionCall of each model step while
// leaving subsequent unsigned parallel calls in the same step untouched (4b05d79a5).

var (
	// geminiForbiddenSchemaKeys lists JSON Schema keywords rejected inside
	// FunctionDeclaration.parameters by standard Gemini endpoints.
	geminiForbiddenSchemaKeys = map[string]bool{
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

	// antigravityAllowedSchemaKeys lists the strict schema property whitelist
	// enforced by Antigravity's internal Gemini function declaration cleaner.
	antigravityAllowedSchemaKeys = map[string]bool{
		"type":        true,
		"description": true,
		"properties":  true,
		"required":    true,
		"items":       true,
		"enum":        true,
		"title":       true,
	}
)

// ValidateGeminiRequest checks body against strict Google Gemini GenerateContent
// request rules under opts and returns all detected violations in scan order.
//
// Contents turns must use only user and model roles, model functionCall parts
// must pair by function name multiset with user functionResponse parts in the
// next turn, and both args and response fields must be JSON objects.
//
// Dialect rules "gemini-signature" and "antigravity-schema" enforce step
// thoughtSignature presence and Antigravity's restricted schema key whitelist.
func ValidateGeminiRequest(body []byte, opts RequestValidationOptions) []RuleViolation {
	var violations []RuleViolation
	add := func(ruleID, path, msg string) {
		violations = append(violations, RuleViolation{
			RuleID:    ruleID,
			Protocol:  protocol.ProtocolGeminiGenerateContent,
			Path:      path,
			Message:   msg,
			Reference: ruleReference(ruleID),
		})
	}

	root, err := parseRequestJSONObject(body)
	if err != nil {
		add("GEM-JSON", "$", err.Error())
		return violations
	}

	validateGeminiToolsAndSchemas(root, opts, add)
	validateGeminiGenerationConfig(root, add)

	contents, hasContents, okContents := nodeArrayField(root, "contents")
	if !hasContents || !okContents || len(contents) == 0 {
		add("GEM-JSON", "$.contents", "contents must be a non-empty array")
		return violations
	}

	requireSignature := opts.HasDialect("gemini-signature")
	var pendingCalls map[string]int
	var pendingCallOrder []string
	pendingModelTurn := -1

	for i, cNode := range contents {
		cPath := fmt.Sprintf("$.contents[%d]", i)
		if cNode.kind != nodeObject {
			add("GEM-JSON", cPath, "content turn must be a JSON object")
			continue
		}
		cObj := cNode.objVal
		role, _, _ := nodeStringField(cObj, "role")
		if role != "user" && role != "model" {
			add("GEM-JSON", cPath+".role", fmt.Sprintf("contents[%d].role = %q, want \"user\" or \"model\"", i, role))
		}
		parts, hasParts, okParts := nodeArrayField(cObj, "parts")
		if !hasParts || !okParts || len(parts) == 0 {
			add("GEM-JSON", cPath+".parts", "parts must be a non-empty array")
			continue
		}

		calls, callOrder, responses, responseOrder := inspectGeminiContentParts(i, role, parts, requireSignature, opts, add)

		if len(pendingCalls) > 0 {
			if role != "user" || len(responses) == 0 {
				add("GEM-FUNC-PAIRING", cPath, fmt.Sprintf("model functionCall in contents[%d] must be followed by a user turn with matching functionResponse parts", pendingModelTurn))
			} else {
				verifyGeminiFunctionPairing(pendingModelTurn, i, pendingCalls, pendingCallOrder, responses, responseOrder, add)
			}
			pendingCalls = nil
			pendingCallOrder = nil
			pendingModelTurn = -1
		} else if len(responses) > 0 {
			add("GEM-FUNC-PAIRING", cPath+".parts", fmt.Sprintf("user contents[%d] has functionResponse parts with no preceding model functionCall", i))
		}

		if role == "model" && len(calls) > 0 {
			pendingCalls = calls
			pendingCallOrder = callOrder
			pendingModelTurn = i
		}
	}

	if len(pendingCalls) > 0 {
		add("GEM-FUNC-PAIRING", fmt.Sprintf("$.contents[%d].parts", pendingModelTurn), "model functionCall at end of contents has no following user functionResponse turn")
	}

	return violations
}

// validateGeminiToolsAndSchemas checks FunctionDeclaration names and parameter schema maps.
func validateGeminiToolsAndSchemas(root map[string]jsonNode, opts RequestValidationOptions, add func(string, string, string)) {
	toolsArr, hasTools, okTools := nodeArrayField(root, "tools")
	if !hasTools {
		return
	}
	if !okTools {
		add("GEM-JSON", "$.tools", "tools must be an array")
		return
	}
	maxLen := 64
	if opts.ToolNameMaxBytes > 0 && opts.ToolNameMaxBytes < maxLen {
		maxLen = opts.ToolNameMaxBytes
	}
	antigravitySchema := opts.HasDialect("antigravity-schema")

	for tIdx, tNode := range toolsArr {
		tPath := fmt.Sprintf("$.tools[%d]", tIdx)
		if tNode.kind != nodeObject {
			add("GEM-JSON", tPath, "tool entry must be an object")
			continue
		}
		decls, hasDecls, okDecls := nodeArrayField(tNode.objVal, "functionDeclarations")
		if !hasDecls {
			continue
		}
		if !okDecls {
			add("GEM-JSON", tPath+".functionDeclarations", "functionDeclarations must be an array")
			continue
		}
		for dIdx, dNode := range decls {
			dPath := fmt.Sprintf("%s.functionDeclarations[%d]", tPath, dIdx)
			if dNode.kind != nodeObject {
				add("GEM-JSON", dPath, "functionDeclaration must be an object")
				continue
			}
			dObj := dNode.objVal
			name, _, _ := nodeStringField(dObj, "name")
			if !geminiToolNamePattern.MatchString(name) || len(name) > maxLen {
				add("GEM-TOOL-NAME", dPath+".name", fmt.Sprintf("functionDeclaration name %q must match ^[a-zA-Z_][a-zA-Z0-9_.:-]{0,%d}$", name, maxLen-1))
			}
			if paramsVal, hasParams := dObj["parameters"]; hasParams && paramsVal.kind != nodeNull {
				if paramsVal.kind != nodeObject {
					add("GEM-SCHEMA", dPath+".parameters", "functionDeclaration.parameters must be a JSON object")
				} else {
					validateGeminiSchemaNode(dPath+".parameters", paramsVal, antigravitySchema, add)
				}
			}
		}
	}
}

// validateGeminiSchemaNode recursively checks schema maps inside parameters,
// treating values inside "properties" as user-named sub-schemas rather than keywords.
func validateGeminiSchemaNode(path string, n jsonNode, antigravityMode bool, add func(string, string, string)) {
	if n.kind == nodeArray {
		for i, elem := range n.arrVal {
			validateGeminiSchemaNode(fmt.Sprintf("%s[%d]", path, i), elem, antigravityMode, add)
		}
		return
	}
	if n.kind != nodeObject {
		return
	}

	if typeVal, hasType := n.objVal["type"]; hasType && typeVal.kind == nodeArray {
		add("GEM-SCHEMA", path+".type", "Gemini Schema.type must be a single string rather than a JSON Schema type array")
	}

	for _, key := range orderedNodeKeys(n) {
		val := n.objVal[key]
		childPath := appendJSONMemberPath(path, key)
		if antigravityMode {
			if !antigravityAllowedSchemaKeys[key] {
				add("GEM-SCHEMA", childPath, fmt.Sprintf("schema keyword %q is forbidden under antigravity-schema dialect", key))
			}
		} else if geminiForbiddenSchemaKeys[key] {
			add("GEM-SCHEMA", childPath, fmt.Sprintf("schema keyword %q is unsupported in Gemini FunctionDeclaration.parameters", key))
		}

		if key == "properties" && val.kind == nodeObject {
			for _, propName := range orderedNodeKeys(val) {
				propSchema := val.objVal[propName]
				validateGeminiSchemaNode(appendJSONMemberPath(childPath, propName), propSchema, antigravityMode, add)
			}
			continue
		}
		validateGeminiSchemaNode(childPath, val, antigravityMode, add)
	}
}

// validateGeminiGenerationConfig checks thinkingBudget < maxOutputTokens when both are positive.
func validateGeminiGenerationConfig(root map[string]jsonNode, add func(string, string, string)) {
	genObj, hasGen, okGen := nodeObjectField(root, "generationConfig")
	if !hasGen || !okGen {
		return
	}
	maxOut, hasMax, okMax := nodeIntField(genObj, "maxOutputTokens")
	thObj, hasTh, okTh := nodeObjectField(genObj, "thinkingConfig")
	if !hasMax || !okMax || maxOut <= 0 || !hasTh || !okTh {
		return
	}
	budget, hasBudget, okBudget := nodeIntField(thObj, "thinkingBudget")
	if hasBudget && okBudget && budget > 0 && maxOut <= budget {
		add("GEM-THINKING-BUDGET", "$.generationConfig.maxOutputTokens", fmt.Sprintf("maxOutputTokens (%d) must be greater than thinkingBudget (%d)", maxOut, budget))
	}
}

// inspectGeminiContentParts validates parts in contents[turnIdx] and returns
// call-name and response-name multisets alongside their first-appearance order.
func inspectGeminiContentParts(turnIdx int, role string, parts []jsonNode, requireSig bool, opts RequestValidationOptions, add func(string, string, string)) (map[string]int, []string, map[string]int, []string) {
	maxNameLen := 64
	if opts.ToolNameMaxBytes > 0 && opts.ToolNameMaxBytes < maxNameLen {
		maxNameLen = opts.ToolNameMaxBytes
	}

	calls := make(map[string]int)
	var callOrder []string
	responses := make(map[string]int)
	var responseOrder []string
	inFunctionStep := false

	for pIdx, pNode := range parts {
		pPath := fmt.Sprintf("$.contents[%d].parts[%d]", turnIdx, pIdx)
		if pNode.kind != nodeObject {
			add("GEM-JSON", pPath, "part must be a JSON object")
			continue
		}
		pObj := pNode.objVal

		if fcObj, hasFC, okFC := nodeObjectField(pObj, "functionCall"); hasFC {
			if role != "model" {
				add("GEM-FUNC-PAIRING", pPath+".functionCall", "functionCall may only appear in model turns")
			}
			if !okFC {
				add("GEM-FUNC-PAIRING", pPath+".functionCall", "functionCall must be a JSON object")
				continue
			}
			name, _, _ := nodeStringField(fcObj, "name")
			if !geminiToolNamePattern.MatchString(name) || len(name) > maxNameLen {
				add("GEM-TOOL-NAME", pPath+".functionCall.name", fmt.Sprintf("functionCall name %q must match ^[a-zA-Z_][a-zA-Z0-9_.:-]{0,%d}$", name, maxNameLen-1))
			} else {
				if calls[name] == 0 {
					callOrder = append(callOrder, name)
				}
				calls[name]++
			}
			if argsVal, hasArgs := fcObj["args"]; !hasArgs || argsVal.kind != nodeObject {
				add("GEM-FUNC-PAIRING", pPath+".functionCall.args", "functionCall.args must be a JSON object")
			}
			if requireSig && !inFunctionStep {
				if sig, _, _ := nodeStringField(pObj, "thoughtSignature"); sig == "" {
					add("GEM-SIGNATURE", pPath+".thoughtSignature", "first functionCall in a model step requires a non-empty thoughtSignature")
				}
			}
			inFunctionStep = true
			continue
		}

		inFunctionStep = false

		if frObj, hasFR, okFR := nodeObjectField(pObj, "functionResponse"); hasFR {
			if role != "user" {
				add("GEM-FUNC-PAIRING", pPath+".functionResponse", "functionResponse may only appear in user turns")
			}
			if !okFR {
				add("GEM-FUNC-PAIRING", pPath+".functionResponse", "functionResponse must be a JSON object")
				continue
			}
			name, _, _ := nodeStringField(frObj, "name")
			if !geminiToolNamePattern.MatchString(name) || len(name) > maxNameLen {
				add("GEM-TOOL-NAME", pPath+".functionResponse.name", fmt.Sprintf("functionResponse name %q must match ^[a-zA-Z_][a-zA-Z0-9_.:-]{0,%d}$", name, maxNameLen-1))
			} else {
				if responses[name] == 0 {
					responseOrder = append(responseOrder, name)
				}
				responses[name]++
			}
			if respVal, hasResp := frObj["response"]; !hasResp || respVal.kind != nodeObject {
				add("GEM-FUNC-PAIRING", pPath+".functionResponse.response", "functionResponse.response must be a JSON object")
			}
		}
	}

	return calls, callOrder, responses, responseOrder
}

// verifyGeminiFunctionPairing checks that functionCall and functionResponse name
// counts match between contents[modelIdx] and contents[userIdx] in scan order.
func verifyGeminiFunctionPairing(modelIdx, userIdx int, calls map[string]int, callOrder []string, responses map[string]int, responseOrder []string, add func(string, string, string)) {
	uPath := fmt.Sprintf("$.contents[%d].parts", userIdx)
	for _, name := range callOrder {
		wantCount := calls[name]
		if gotCount := responses[name]; gotCount != wantCount {
			add("GEM-FUNC-PAIRING", uPath, fmt.Sprintf("functionResponse count for %q = %d, want %d from contents[%d]", name, gotCount, wantCount, modelIdx))
		}
	}
	for _, name := range responseOrder {
		if calls[name] == 0 {
			add("GEM-FUNC-PAIRING", uPath, fmt.Sprintf("unexpected functionResponse for %q not called in contents[%d]", name, modelIdx))
		}
	}
}
