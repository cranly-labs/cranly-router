package testkit

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// clientsim_gemini.go simulates how Google Gemini GenerateContent clients fold
// streamed or buffered candidates[0].content.parts into a model history turn
// and append user functionResponse parts for multi-turn tool execution.
// Thinking-enabled Gemini endpoints attach thoughtSignature values to thought
// summaries and initial functionCall parts that subsequent turns must preserve.
//
// Stream folding coalesces consecutive visible text deltas and consecutive
// thought text deltas separately while retaining trailing thoughtSignature
// fields and atomic functionCall objects. Tool outputs that are not already JSON
// objects are wrapped inside {"result": ...} to satisfy functionResponse rules.

// SimulateGeminiNextTurn builds the next Gemini GenerateContent request JSON
// from in while preserving tools, systemInstruction, and generationConfig.
//
// Consecutive streamed text parts with matching thought flags coalesce into a
// single model part, whereas SimVariantGeminiDropThoughts removes pure thought
// text parts while keeping functionCall parts and their thoughtSignature values.
//
// Every model functionCall appends a matching user functionResponse object
// paired by call ID, function name, or call position.
func SimulateGeminiNextTurn(in TurnInput) ([]byte, error) {
	reqNode, err := parseJSONNode(in.PrevRequest, true)
	if err != nil {
		return nil, fmt.Errorf("parse previous Gemini request: %w", err)
	}
	if reqNode.kind != nodeObject {
		return nil, fmt.Errorf("previous Gemini request must be a JSON object")
	}

	contentsArr, hasC, okC := nodeArrayField(reqNode.objVal, "contents")
	if !hasC || !okC {
		return nil, fmt.Errorf("previous Gemini request missing contents array")
	}

	var modelParts []jsonNode
	if len(in.ResponseFrames) > 0 {
		modelParts, err = reconstructGeminiPartsFromSSE(in.ResponseFrames)
	} else {
		modelParts, err = reconstructGeminiPartsFromJSON(in.ResponseBody)
	}
	if err != nil {
		return nil, err
	}

	if in.Variant == SimVariantGeminiDropThoughts {
		filtered := make([]jsonNode, 0, len(modelParts))
		for _, p := range modelParts {
			if p.kind == nodeObject {
				if thVal, hasTh := p.objVal["thought"]; hasTh && thVal.kind == nodeBool && thVal.boolVal {
					if _, hasFC := p.objVal["functionCall"]; !hasFC {
						continue
					}
				}
			}
			filtered = append(filtered, p)
		}
		modelParts = filtered
	}
	if len(modelParts) == 0 {
		return nil, fmt.Errorf("gemini model turn produced no replayable parts")
	}

	contentsArr = append(contentsArr, jsonNode{
		kind: nodeObject,
		objVal: map[string]jsonNode{
			"role":  makeStringNode("model"),
			"parts": {kind: nodeArray, arrVal: modelParts},
		},
	})

	// gemCall pairs a reconstructed Gemini functionCall identifier with its declaration name.
	type gemCall struct {
		id   string // optional Gemini functionCall ID
		name string // invoked FunctionDeclaration name
	}
	var calls []gemCall
	for _, p := range modelParts {
		if p.kind != nodeObject {
			continue
		}
		if fcObj, hasFC, okFC := nodeObjectField(p.objVal, "functionCall"); hasFC && okFC {
			id, _, _ := nodeStringField(fcObj, "id")
			name, _, _ := nodeStringField(fcObj, "name")
			calls = append(calls, gemCall{id: id, name: name})
		}
	}

	if len(calls) > 0 {
		used := make([]bool, len(in.ToolOutputs))
		respParts := make([]jsonNode, 0, len(calls)+1)
		for idx, c := range calls {
			out, err := resolveToolOutput(idx, c.id, c.name, in, used)
			if err != nil {
				return nil, err
			}
			respObjNode := wrapGeminiFunctionResponsePayload(out.Output)
			frFields := map[string]jsonNode{
				"name":     makeStringNode(c.name),
				"response": respObjNode,
			}
			if c.id != "" {
				frFields["id"] = makeStringNode(c.id)
			}
			respParts = append(respParts, jsonNode{
				kind: nodeObject,
				objVal: map[string]jsonNode{
					"functionResponse": {kind: nodeObject, objVal: frFields},
				},
			})
		}
		if in.FollowUpUserText != "" {
			respParts = append(respParts, jsonNode{
				kind: nodeObject,
				objVal: map[string]jsonNode{
					"text": makeStringNode(in.FollowUpUserText),
				},
			})
		}
		contentsArr = append(contentsArr, jsonNode{
			kind: nodeObject,
			objVal: map[string]jsonNode{
				"role":  makeStringNode("user"),
				"parts": {kind: nodeArray, arrVal: respParts},
			},
		})
	} else if in.FollowUpUserText != "" {
		contentsArr = append(contentsArr, jsonNode{
			kind: nodeObject,
			objVal: map[string]jsonNode{
				"role": makeStringNode("user"),
				"parts": {
					kind: nodeArray,
					arrVal: []jsonNode{{
						kind:   nodeObject,
						objVal: map[string]jsonNode{"text": makeStringNode(in.FollowUpUserText)},
					}},
				},
			},
		})
	}

	reqNode.objVal["contents"] = jsonNode{kind: nodeArray, arrVal: contentsArr}
	return encodeLosslessNode(reqNode)
}

// reconstructGeminiPartsFromJSON extracts candidates[0].content.parts from a buffered Gemini response.
func reconstructGeminiPartsFromJSON(body []byte) ([]jsonNode, error) {
	respNode, err := parseJSONNode(body, true)
	if err != nil {
		return nil, fmt.Errorf("parse Gemini response JSON: %w", err)
	}
	if respNode.kind != nodeObject {
		return nil, fmt.Errorf("gemini response must be a JSON object")
	}
	cands, hasC, okC := nodeArrayField(respNode.objVal, "candidates")
	if !hasC || !okC || len(cands) == 0 || cands[0].kind != nodeObject {
		return nil, fmt.Errorf("gemini response missing candidates[0]")
	}
	contentObj, hasCnt, okCnt := nodeObjectField(cands[0].objVal, "content")
	if !hasCnt || !okCnt {
		return nil, fmt.Errorf("gemini candidates[0] missing content")
	}
	parts, hasP, okP := nodeArrayField(contentObj, "parts")
	if !hasP || !okP {
		return nil, fmt.Errorf("gemini candidates[0].content missing parts")
	}
	return parts, nil
}

// reconstructGeminiPartsFromSSE folds streamed GenerateContentResponse frames
// while coalescing adjacent pure text or thought text fragments.
func reconstructGeminiPartsFromSSE(frames []sse.Frame) ([]jsonNode, error) {
	var merged []jsonNode
	for i, f := range frames {
		trimmed := bytes.TrimSpace(f.Data)
		if len(trimmed) == 0 {
			continue
		}
		node, err := parseJSONNode(trimmed, true)
		if err != nil {
			return nil, fmt.Errorf("gemini frame[%d]: %w", i, err)
		}
		if node.kind != nodeObject {
			continue
		}
		cands, hasC, okC := nodeArrayField(node.objVal, "candidates")
		if !hasC || !okC || len(cands) == 0 || cands[0].kind != nodeObject {
			continue
		}
		contentObj, hasCnt, okCnt := nodeObjectField(cands[0].objVal, "content")
		if !hasCnt || !okCnt {
			continue
		}
		parts, hasP, okP := nodeArrayField(contentObj, "parts")
		if !hasP || !okP {
			continue
		}
		for _, part := range parts {
			merged = appendCoalescedGeminiPart(merged, part)
		}
	}
	return merged, nil
}

// appendCoalescedGeminiPart merges consecutive text or thought-text delta parts
// while preserving thoughtSignature fields attached to trailing fragments.
func appendCoalescedGeminiPart(dst []jsonNode, next jsonNode) []jsonNode {
	if next.kind != nodeObject || len(dst) == 0 {
		return append(dst, next)
	}
	nextTxt, hasNextTxt, okNextTxt := nodeStringField(next.objVal, "text")
	_, nextHasFC := next.objVal["functionCall"]
	if !hasNextTxt || !okNextTxt || nextHasFC {
		return append(dst, next)
	}
	prev := &dst[len(dst)-1]
	if prev.kind != nodeObject {
		return append(dst, next)
	}
	prevTxt, hasPrevTxt, okPrevTxt := nodeStringField(prev.objVal, "text")
	_, prevHasFC := prev.objVal["functionCall"]
	if !hasPrevTxt || !okPrevTxt || prevHasFC {
		return append(dst, next)
	}
	if isThoughtPart(prev.objVal) != isThoughtPart(next.objVal) {
		return append(dst, next)
	}
	prev.objVal["text"] = makeStringNode(prevTxt + nextTxt)
	if sig, _, _ := nodeStringField(next.objVal, "thoughtSignature"); sig != "" {
		prev.objVal["thoughtSignature"] = makeStringNode(sig)
	}
	return dst
}

// isThoughtPart reports whether pObj carries a boolean true thought field.
func isThoughtPart(pObj map[string]jsonNode) bool {
	v, ok := pObj["thought"]
	return ok && v.kind == nodeBool && v.boolVal
}

// wrapGeminiFunctionResponsePayload parses raw as a JSON object when valid.
// Plain text or scalar tool outputs are wrapped inside {"result": raw} so
// functionResponse.response always satisfies Gemini's object constraint.
func wrapGeminiFunctionResponsePayload(raw string) jsonNode {
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" {
		if parsed, err := parseJSONNode([]byte(trimmed), true); err == nil && parsed.kind == nodeObject {
			return parsed
		}
	}
	return jsonNode{
		kind: nodeObject,
		objVal: map[string]jsonNode{
			"result": makeStringNode(raw),
		},
	}
}
