package testkit

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// clientsim_messages.go simulates how Anthropic Messages clients such as Claude
// Code and the official Anthropic SDK fold streamed or buffered assistant
// replies and user tool_result blocks onto the previous request JSON.
// Multi-turn replay defects often arise only when client-assembled history
// re-enters a different upstream protocol or native carrier path on turn two.
//
// Stream reconstruction concatenates text_delta, thinking_delta,
// signature_delta, and input_json_delta fragments per content block index in
// arrival order without calling protocol codecs. Under the drop_thinking
// variant, historical thinking and redacted_thinking blocks are omitted before
// pairing every assistant tool_use with leading user tool_result blocks.

// simMessagesBlock holds one reconstructed assistant content block during SSE
// stream folding.
type simMessagesBlock struct {
	block      jsonNode        // initial content_block object
	textBuf    strings.Builder // concatenated text_delta or thinking_delta
	sigBuf     strings.Builder // concatenated thinking signature_delta fragments
	partialBuf strings.Builder // concatenated tool_use input_json_delta fragments
	sawPartial bool            // whether any input_json_delta arrived
}

// SimulateMessagesNextTurn builds the next Anthropic Messages request JSON by
// appending the reconstructed assistant message and user tool_result turn onto
// in.PrevRequest while preserving all unrelated top-level request members.
//
// Streamed inputs in ResponseFrames take precedence over ResponseBody, and
// accumulated input_json_delta strings must parse as valid JSON objects. Under
// SimVariantMessagesDropThinking, thinking and redacted_thinking blocks are
// dropped before validating that at least one assistant block remains.
//
// Each emitted tool_use block resolves its matching SimulatedToolOutput and is
// placed at the front of the appended user message ahead of any FollowUpUserText.
func SimulateMessagesNextTurn(in TurnInput) ([]byte, error) {
	reqNode, err := parseJSONNode(in.PrevRequest, true)
	if err != nil {
		return nil, fmt.Errorf("parse previous Messages request: %w", err)
	}
	if reqNode.kind != nodeObject {
		return nil, fmt.Errorf("previous Messages request must be a JSON object")
	}

	var blocks []jsonNode
	if len(in.ResponseFrames) > 0 {
		blocks, err = reconstructMessagesBlocksFromSSE(in.ResponseFrames)
	} else {
		blocks, err = reconstructMessagesBlocksFromJSON(in.ResponseBody)
	}
	if err != nil {
		return nil, err
	}

	if in.Variant == SimVariantMessagesDropThinking {
		filtered := make([]jsonNode, 0, len(blocks))
		for _, b := range blocks {
			if b.kind == nodeObject {
				bType, _, _ := nodeStringField(b.objVal, "type")
				if bType == "thinking" || bType == "redacted_thinking" {
					continue
				}
			}
			filtered = append(filtered, b)
		}
		blocks = filtered
	}
	if len(blocks) == 0 {
		return nil, fmt.Errorf("messages assistant reply produced no replayable content blocks")
	}

	msgsArr, hasMsgs, okMsgs := nodeArrayField(reqNode.objVal, "messages")
	if !hasMsgs || !okMsgs {
		return nil, fmt.Errorf("previous Messages request missing messages array")
	}

	asstMsg := jsonNode{
		kind: nodeObject,
		objVal: map[string]jsonNode{
			"role":    makeStringNode("assistant"),
			"content": {kind: nodeArray, arrVal: blocks},
		},
	}
	msgsArr = append(msgsArr, asstMsg)

	// callInfo pairs a reconstructed Anthropic tool_use block ID with its function name.
	type callInfo struct {
		id   string // emitted tool_use block ID
		name string // emitted tool_use function name
	}
	var calls []callInfo
	for _, b := range blocks {
		if b.kind != nodeObject {
			continue
		}
		bType, _, _ := nodeStringField(b.objVal, "type")
		if bType == "tool_use" {
			id, _, _ := nodeStringField(b.objVal, "id")
			name, _, _ := nodeStringField(b.objVal, "name")
			calls = append(calls, callInfo{id: id, name: name})
		}
	}

	if len(calls) > 0 {
		used := make([]bool, len(in.ToolOutputs))
		userBlocks := make([]jsonNode, 0, len(calls)+1)
		for idx, c := range calls {
			out, err := resolveToolOutput(idx, c.id, c.name, in, used)
			if err != nil {
				return nil, err
			}
			trObj := map[string]jsonNode{
				"type":        makeStringNode("tool_result"),
				"tool_use_id": makeStringNode(c.id),
				"content":     makeStringNode(out.Output),
			}
			if out.IsError {
				trObj["is_error"] = makeBoolNode(true)
			}
			userBlocks = append(userBlocks, jsonNode{kind: nodeObject, objVal: trObj})
		}
		if in.FollowUpUserText != "" {
			userBlocks = append(userBlocks, jsonNode{
				kind: nodeObject,
				objVal: map[string]jsonNode{
					"type": makeStringNode("text"),
					"text": makeStringNode(in.FollowUpUserText),
				},
			})
		}
		msgsArr = append(msgsArr, jsonNode{
			kind: nodeObject,
			objVal: map[string]jsonNode{
				"role":    makeStringNode("user"),
				"content": {kind: nodeArray, arrVal: userBlocks},
			},
		})
	} else if in.FollowUpUserText != "" {
		msgsArr = append(msgsArr, jsonNode{
			kind: nodeObject,
			objVal: map[string]jsonNode{
				"role":    makeStringNode("user"),
				"content": makeStringNode(in.FollowUpUserText),
			},
		})
	}

	reqNode.objVal["messages"] = jsonNode{kind: nodeArray, arrVal: msgsArr}
	return encodeLosslessNode(reqNode)
}

// reconstructMessagesBlocksFromJSON extracts content[] blocks from a non-streaming Messages response.
func reconstructMessagesBlocksFromJSON(body []byte) ([]jsonNode, error) {
	respNode, err := parseJSONNode(body, true)
	if err != nil {
		return nil, fmt.Errorf("parse Messages response JSON: %w", err)
	}
	if respNode.kind != nodeObject {
		return nil, fmt.Errorf("messages response must be a JSON object")
	}
	arr, has, ok := nodeArrayField(respNode.objVal, "content")
	if !has || !ok {
		return nil, fmt.Errorf("messages response missing content array")
	}
	return arr, nil
}

// reconstructMessagesBlocksFromSSE folds Messages SSE frames into completed content blocks.
func reconstructMessagesBlocksFromSSE(frames []sse.Frame) ([]jsonNode, error) {
	byIndex := make(map[int64]*simMessagesBlock)
	var order []int64

	for i, f := range frames {
		trimmed := bytes.TrimSpace(f.Data)
		if len(trimmed) == 0 {
			continue
		}
		node, err := parseJSONNode(trimmed, true)
		if err != nil {
			return nil, fmt.Errorf("messages frame[%d]: %w", i, err)
		}
		if node.kind != nodeObject {
			continue
		}
		evType, _, _ := nodeStringField(node.objVal, "type")
		switch evType {
		case "content_block_start":
			idx, hasIdx, okIdx := nodeIntField(node.objVal, "index")
			blk, hasBlk := node.objVal["content_block"]
			if !hasIdx || !okIdx || !hasBlk || blk.kind != nodeObject {
				return nil, fmt.Errorf("messages frame[%d]: malformed content_block_start", i)
			}
			entry := &simMessagesBlock{block: blk}
			bType, _, _ := nodeStringField(blk.objVal, "type")
			switch bType {
			case "text":
				if initTxt, _, _ := nodeStringField(blk.objVal, "text"); initTxt != "" {
					_, _ = entry.textBuf.WriteString(initTxt)
				}
			case "thinking":
				if initTh, _, _ := nodeStringField(blk.objVal, "thinking"); initTh != "" {
					_, _ = entry.textBuf.WriteString(initTh)
				}
				if initSig, _, _ := nodeStringField(blk.objVal, "signature"); initSig != "" {
					_, _ = entry.sigBuf.WriteString(initSig)
				}
			}
			byIndex[idx] = entry
			order = append(order, idx)

		case "content_block_delta":
			idx, _, _ := nodeIntField(node.objVal, "index")
			entry := byIndex[idx]
			if entry == nil {
				continue
			}
			deltaObj, hasD, okD := nodeObjectField(node.objVal, "delta")
			if !hasD || !okD {
				continue
			}
			dType, _, _ := nodeStringField(deltaObj, "type")
			switch dType {
			case "text_delta":
				txt, _, _ := nodeStringField(deltaObj, "text")
				_, _ = entry.textBuf.WriteString(txt)
			case "thinking_delta":
				th, _, _ := nodeStringField(deltaObj, "thinking")
				_, _ = entry.textBuf.WriteString(th)
			case "signature_delta":
				sig, _, _ := nodeStringField(deltaObj, "signature")
				_, _ = entry.sigBuf.WriteString(sig)
			case "input_json_delta":
				pj, _, _ := nodeStringField(deltaObj, "partial_json")
				_, _ = entry.partialBuf.WriteString(pj)
				entry.sawPartial = true
			}
		}
	}

	out := make([]jsonNode, 0, len(order))
	for _, idx := range order {
		entry := byIndex[idx]
		bObj := entry.block.objVal
		bType, _, _ := nodeStringField(bObj, "type")
		switch bType {
		case "text":
			bObj["text"] = makeStringNode(entry.textBuf.String())
		case "thinking":
			bObj["thinking"] = makeStringNode(entry.textBuf.String())
			if entry.sigBuf.Len() > 0 {
				bObj["signature"] = makeStringNode(entry.sigBuf.String())
			}
		case "tool_use", "server_tool_use":
			if entry.sawPartial {
				rawIn := strings.TrimSpace(entry.partialBuf.String())
				if rawIn == "" {
					bObj["input"] = jsonNode{kind: nodeObject, objVal: map[string]jsonNode{}}
				} else {
					parsedIn, err := parseJSONNode([]byte(rawIn), true)
					if err != nil {
						return nil, fmt.Errorf("messages tool_use[%d] invalid accumulated input JSON: %w", idx, err)
					}
					bObj["input"] = parsedIn
				}
			}
		}
		out = append(out, jsonNode{kind: nodeObject, objVal: bObj})
	}
	return out, nil
}
