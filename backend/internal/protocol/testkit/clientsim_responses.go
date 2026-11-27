package testkit

import (
	"bytes"
	"fmt"
	"slices"

	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// clientsim_responses.go simulates how OpenAI Responses clients such as Codex
// CLI and pi append response.output items and *_call_output entries onto the
// previous request's input list for multi-turn replay testing. Real Responses
// clients differ in whether they echo full reasoning items, strip
// encrypted_content for third-party providers, or retain only encrypted_content.
//
// Simulation normalizes string shorthand inputs into canonical user message
// items, extracts completed output items from terminal envelopes or ordered
// output_item.done events, applies the selected reasoning continuity variant,
// and appends matching function_call_output, custom_tool_call_output, or
// tool_search_output items without invoking protocol codecs.

// SimulateResponsesNextTurn builds the next OpenAI Responses request JSON from
// in while preserving all non-input fields on in.PrevRequest.
//
// Shorthand string inputs expand into a one-element user message array before
// appending output items from ResponseFrames or ResponseBody. Reasoning items
// keep all fields under SimVariantResponsesPreserveAll, drop encrypted_content
// (and vanish when empty) under SimVariantResponsesStripEncrypted, and clear
// visible summary text under SimVariantResponsesEncryptedOnly.
//
// Every function_call, custom_tool_call, and tool_search_call item receives a
// corresponding *_output item matched by CallID, Name, or position.
func SimulateResponsesNextTurn(in TurnInput) ([]byte, error) {
	reqNode, err := parseJSONNode(in.PrevRequest, true)
	if err != nil {
		return nil, fmt.Errorf("parse previous Responses request: %w", err)
	}
	if reqNode.kind != nodeObject {
		return nil, fmt.Errorf("previous Responses request must be a JSON object")
	}

	inputItems, err := normalizeResponsesInputToArray(reqNode.objVal["input"])
	if err != nil {
		return nil, err
	}

	var outputItems []jsonNode
	if len(in.ResponseFrames) > 0 {
		outputItems, err = reconstructResponsesOutputFromSSE(in.ResponseFrames)
	} else {
		outputItems, err = reconstructResponsesOutputFromJSON(in.ResponseBody)
	}
	if err != nil {
		return nil, err
	}

	outputItems = applyResponsesSimVariant(outputItems, in.Variant)
	inputItems = append(inputItems, outputItems...)

	// respCall captures the item discriminator, call_id, and tool name of an emitted call item.
	type respCall struct {
		itemType string // call item type discriminator
		callID   string // correlated call_id string
		name     string // invoked tool name
	}
	var calls []respCall
	for _, it := range outputItems {
		if it.kind != nodeObject {
			continue
		}
		iType, _, _ := nodeStringField(it.objVal, "type")
		switch iType {
		case "function_call", "custom_tool_call", "tool_search_call":
			callID, _, _ := nodeStringField(it.objVal, "call_id")
			name, _, _ := nodeStringField(it.objVal, "name")
			calls = append(calls, respCall{itemType: iType, callID: callID, name: name})
		}
	}

	if len(calls) > 0 {
		used := make([]bool, len(in.ToolOutputs))
		for idx, c := range calls {
			out, err := resolveToolOutput(idx, c.callID, c.name, in, used)
			if err != nil {
				return nil, err
			}
			outType := "function_call_output"
			switch c.itemType {
			case "custom_tool_call":
				outType = "custom_tool_call_output"
			case "tool_search_call":
				outType = "tool_search_output"
			}
			outItem := jsonNode{
				kind: nodeObject,
				objVal: map[string]jsonNode{
					"type":    makeStringNode(outType),
					"call_id": makeStringNode(c.callID),
					"output":  makeStringNode(out.Output),
				},
			}
			inputItems = append(inputItems, outItem)
		}
	}
	if in.FollowUpUserText != "" {
		inputItems = append(inputItems, makeResponsesUserMessageItem(in.FollowUpUserText))
	}

	reqNode.objVal["input"] = jsonNode{kind: nodeArray, arrVal: inputItems}
	return encodeLosslessNode(reqNode)
}

// normalizeResponsesInputToArray converts a string or array Responses input into an item slice.
func normalizeResponsesInputToArray(inVal jsonNode) ([]jsonNode, error) {
	switch inVal.kind {
	case nodeString:
		return []jsonNode{makeResponsesUserMessageItem(inVal.strVal)}, nil
	case nodeArray:
		return slices.Clone(inVal.arrVal), nil
	default:
		return nil, fmt.Errorf("responses request input must be a string or array")
	}
}

// makeResponsesUserMessageItem creates a canonical user message item for Responses input[].
func makeResponsesUserMessageItem(text string) jsonNode {
	return jsonNode{
		kind: nodeObject,
		objVal: map[string]jsonNode{
			"type": makeStringNode("message"),
			"role": makeStringNode("user"),
			"content": {
				kind: nodeArray,
				arrVal: []jsonNode{{
					kind: nodeObject,
					objVal: map[string]jsonNode{
						"type": makeStringNode("input_text"),
						"text": makeStringNode(text),
					},
				}},
			},
		},
	}
}

// reconstructResponsesOutputFromJSON reads output[] from a buffered Responses payload.
func reconstructResponsesOutputFromJSON(body []byte) ([]jsonNode, error) {
	respNode, err := parseJSONNode(body, true)
	if err != nil {
		return nil, fmt.Errorf("parse Responses JSON: %w", err)
	}
	if respNode.kind != nodeObject {
		return nil, fmt.Errorf("responses body must be a JSON object")
	}
	arr, has, ok := nodeArrayField(respNode.objVal, "output")
	if !has || !ok {
		return nil, fmt.Errorf("responses body missing output array")
	}
	return arr, nil
}

// reconstructResponsesOutputFromSSE extracts output items from response.completed/incomplete
// when populated, falling back to completed output_item.done events ordered by output_index.
func reconstructResponsesOutputFromSSE(frames []sse.Frame) ([]jsonNode, error) {
	// indexedItem associates an output_item.done JSON node with its stream output_index.
	type indexedItem struct {
		idx  int64    // stream output_index position
		item jsonNode // completed output item object
	}
	var doneItems []indexedItem
	var terminalOutput []jsonNode

	for i, f := range frames {
		trimmed := bytes.TrimSpace(f.Data)
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("[DONE]")) {
			continue
		}
		node, err := parseJSONNode(trimmed, true)
		if err != nil {
			return nil, fmt.Errorf("responses frame[%d]: %w", i, err)
		}
		if node.kind != nodeObject {
			continue
		}
		evType, _, _ := nodeStringField(node.objVal, "type")
		switch evType {
		case "response.output_item.done":
			outIdx, _, _ := nodeIntField(node.objVal, "output_index")
			if itNode, has := node.objVal["item"]; has && itNode.kind == nodeObject {
				doneItems = append(doneItems, indexedItem{idx: outIdx, item: itNode})
			}
		case "response.completed", "response.incomplete":
			if respObj, hasR, okR := nodeObjectField(node.objVal, "response"); hasR && okR {
				if outArr, hasOut, okOut := nodeArrayField(respObj, "output"); hasOut && okOut && len(outArr) > 0 {
					terminalOutput = outArr
				}
			}
		}
	}

	if len(terminalOutput) > 0 {
		return terminalOutput, nil
	}
	slices.SortFunc(doneItems, func(a, b indexedItem) int {
		if a.idx < b.idx {
			return -1
		}
		if a.idx > b.idx {
			return 1
		}
		return 0
	})
	out := make([]jsonNode, 0, len(doneItems))
	for _, entry := range doneItems {
		out = append(out, entry.item)
	}
	return out, nil
}

// applyResponsesSimVariant mutates cloned output items according to variant's
// reasoning continuity habit.
func applyResponsesSimVariant(items []jsonNode, variant ClientSimVariant) []jsonNode {
	out := make([]jsonNode, 0, len(items))
	for _, item := range items {
		if item.kind != nodeObject {
			out = append(out, item)
			continue
		}
		copiedObj := make(map[string]jsonNode, len(item.objVal))
		for k, v := range item.objVal {
			// Omit response-only item status fields such as "completed" when replaying
			// output items into next-turn input[] so strict upstream schemas accept them.
			if k == "status" {
				continue
			}
			copiedObj[k] = v
		}
		iType, _, _ := nodeStringField(copiedObj, "type")
		if iType == "reasoning" {
			switch variant {
			case SimVariantResponsesStripEncrypted:
				delete(copiedObj, "encrypted_content")
				sumArr, hasSum, okSum := nodeArrayField(copiedObj, "summary")
				cntArr, hasCnt, okCnt := nodeArrayField(copiedObj, "content")
				if (!hasSum || !okSum || len(sumArr) == 0) && (!hasCnt || !okCnt || len(cntArr) == 0) {
					continue
				}
			case SimVariantResponsesEncryptedOnly:
				copiedObj["summary"] = jsonNode{kind: nodeArray, arrVal: nil}
				delete(copiedObj, "content")
			}
		}
		out = append(out, jsonNode{kind: nodeObject, objVal: copiedObj})
	}
	return out
}
