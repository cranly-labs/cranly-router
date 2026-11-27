package testkit

import (
	"bytes"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// clientsim_chat.go simulates how OpenAI Chat Completions clients fold streamed
// chunks or buffered choices[0].message replies into the next-turn messages
// array for multi-turn replay verification. OpenAI-compatible SDKs split into
// clients that preserve reasoning_content on assistant history turns and strict
// schema-driven clients that discard non-standard assistant fields.
//
// Chunk reconstruction concatenates content, reasoning_content, refusal, and
// indexed tool_calls arguments directly from wire JSON without calling codecs.
// Each assembled tool call then appends a dedicated role="tool" result message
// keyed by tool_call_id ahead of any optional follow-up user prompt.

// simChatCall holds one assembled assistant tool_call during chunk folding.
type simChatCall struct {
	idx  int64           // chunk tool_calls index
	id   string          // correlated tool_call identifier
	name string          // invoked function name
	args strings.Builder // concatenated function.arguments string
}

// SimulateChatNextTurn builds the next OpenAI Chat Completions request JSON
// from in while preserving unrelated top-level parameters on in.PrevRequest.
//
// Streamed chunks in ResponseFrames fold by tool_calls index and populate empty
// argument strings with "{}" when a zero-argument call finishes. Under
// SimVariantChatDropUnknown, reasoning_content and reasoning fields are removed
// from the assistant turn before appending role="tool" result messages.
//
// Missing tool outputs fail unless SynthesizeMissingOutputs is enabled on in.
func SimulateChatNextTurn(in TurnInput) ([]byte, error) {
	reqNode, err := parseJSONNode(in.PrevRequest, true)
	if err != nil {
		return nil, fmt.Errorf("parse previous Chat request: %w", err)
	}
	if reqNode.kind != nodeObject {
		return nil, fmt.Errorf("previous Chat request must be a JSON object")
	}

	msgsArr, hasMsgs, okMsgs := nodeArrayField(reqNode.objVal, "messages")
	if !hasMsgs || !okMsgs {
		return nil, fmt.Errorf("previous Chat request missing messages array")
	}

	var asstMsg jsonNode
	var calls []*simChatCall
	if len(in.ResponseFrames) > 0 {
		asstMsg, calls, err = reconstructChatMessageFromSSE(in.ResponseFrames)
	} else {
		asstMsg, calls, err = reconstructChatMessageFromJSON(in.ResponseBody)
	}
	if err != nil {
		return nil, err
	}

	if in.Variant == SimVariantChatDropUnknown {
		delete(asstMsg.objVal, "reasoning_content")
		delete(asstMsg.objVal, "reasoning")
	}

	msgsArr = append(msgsArr, asstMsg)

	if len(calls) > 0 {
		used := make([]bool, len(in.ToolOutputs))
		for idx, c := range calls {
			out, err := resolveToolOutput(idx, c.id, c.name, in, used)
			if err != nil {
				return nil, err
			}
			msgsArr = append(msgsArr, jsonNode{
				kind: nodeObject,
				objVal: map[string]jsonNode{
					"role":         makeStringNode("tool"),
					"tool_call_id": makeStringNode(c.id),
					"content":      makeStringNode(out.Output),
				},
			})
		}
	}
	if in.FollowUpUserText != "" {
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

// reconstructChatMessageFromJSON extracts choices[0].message and its tool_calls
// from a buffered Chat Completions response.
func reconstructChatMessageFromJSON(body []byte) (jsonNode, []*simChatCall, error) {
	respNode, err := parseJSONNode(body, true)
	if err != nil {
		return jsonNode{}, nil, fmt.Errorf("parse Chat response JSON: %w", err)
	}
	if respNode.kind != nodeObject {
		return jsonNode{}, nil, fmt.Errorf("chat response must be a JSON object")
	}
	choices, hasC, okC := nodeArrayField(respNode.objVal, "choices")
	if !hasC || !okC || len(choices) == 0 || choices[0].kind != nodeObject {
		return jsonNode{}, nil, fmt.Errorf("chat response missing choices[0]")
	}
	msgObj, hasM, okM := nodeObjectField(choices[0].objVal, "message")
	if !hasM || !okM {
		return jsonNode{}, nil, fmt.Errorf("chat choices[0] missing message object")
	}

	copied := make(map[string]jsonNode, len(msgObj)+1)
	maps.Copy(copied, msgObj)
	copied["role"] = makeStringNode("assistant")

	var calls []*simChatCall
	if tcArr, hasTC, okTC := nodeArrayField(copied, "tool_calls"); hasTC && okTC {
		for idx, tc := range tcArr {
			if tc.kind != nodeObject {
				continue
			}
			id, _, _ := nodeStringField(tc.objVal, "id")
			fnObj, _, _ := nodeObjectField(tc.objVal, "function")
			name, _, _ := nodeStringField(fnObj, "name")
			args, _, _ := nodeStringField(fnObj, "arguments")
			sc := &simChatCall{idx: int64(idx), id: id, name: name}
			_, _ = sc.args.WriteString(args)
			calls = append(calls, sc)
		}
	}
	return jsonNode{kind: nodeObject, objVal: copied}, calls, nil
}

// reconstructChatMessageFromSSE folds Chat Completions SSE chunks into one
// assistant message object and ordered tool calls. Tool-only and refusal-only
// replies without text deltas encode content as JSON null so streamed and
// buffered Chat Completions messages share identical wire shapes.
func reconstructChatMessageFromSSE(frames []sse.Frame) (jsonNode, []*simChatCall, error) {
	var contentBuf, reasoningBuf, refusalBuf strings.Builder
	sawReasoning := false
	byIdx := make(map[int64]*simChatCall)

	for i, f := range frames {
		trimmed := bytes.TrimSpace(f.Data)
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("[DONE]")) {
			continue
		}
		node, err := parseJSONNode(trimmed, true)
		if err != nil {
			return jsonNode{}, nil, fmt.Errorf("chat frame[%d]: %w", i, err)
		}
		if node.kind != nodeObject {
			continue
		}
		choices, hasC, okC := nodeArrayField(node.objVal, "choices")
		if !hasC || !okC || len(choices) == 0 || choices[0].kind != nodeObject {
			continue
		}
		deltaObj, hasD, okD := nodeObjectField(choices[0].objVal, "delta")
		if !hasD || !okD {
			continue
		}

		if cStr, hasCS, okCS := nodeStringField(deltaObj, "content"); hasCS && okCS {
			_, _ = contentBuf.WriteString(cStr)
		}
		if rStr, hasRS, okRS := nodeStringField(deltaObj, "reasoning_content"); hasRS && okRS {
			sawReasoning = true
			_, _ = reasoningBuf.WriteString(rStr)
		} else if rAlt, hasAlt, okAlt := nodeStringField(deltaObj, "reasoning"); hasAlt && okAlt {
			sawReasoning = true
			_, _ = reasoningBuf.WriteString(rAlt)
		}
		if refStr, hasRef, okRef := nodeStringField(deltaObj, "refusal"); hasRef && okRef {
			_, _ = refusalBuf.WriteString(refStr)
		}

		if tcArr, hasTC, okTC := nodeArrayField(deltaObj, "tool_calls"); hasTC && okTC {
			for _, tc := range tcArr {
				if tc.kind != nodeObject {
					continue
				}
				idx, _, _ := nodeIntField(tc.objVal, "index")
				entry := byIdx[idx]
				if entry == nil {
					entry = &simChatCall{idx: idx}
					byIdx[idx] = entry
				}
				if id, _, _ := nodeStringField(tc.objVal, "id"); id != "" {
					entry.id = id
				}
				if fnObj, hasFn, okFn := nodeObjectField(tc.objVal, "function"); hasFn && okFn {
					if name, _, _ := nodeStringField(fnObj, "name"); name != "" {
						entry.name = name
					}
					if args, _, _ := nodeStringField(fnObj, "arguments"); args != "" {
						_, _ = entry.args.WriteString(args)
					}
				}
			}
		}
	}

	calls := make([]*simChatCall, 0, len(byIdx))
	for _, c := range byIdx {
		calls = append(calls, c)
	}
	slices.SortFunc(calls, func(a, b *simChatCall) int {
		if a.idx < b.idx {
			return -1
		}
		if a.idx > b.idx {
			return 1
		}
		return 0
	})

	msgObj := map[string]jsonNode{
		"role": makeStringNode("assistant"),
	}
	if contentBuf.Len() > 0 || (len(calls) == 0 && refusalBuf.Len() == 0) {
		msgObj["content"] = makeStringNode(contentBuf.String())
	} else {
		msgObj["content"] = jsonNode{kind: nodeNull}
	}
	if sawReasoning {
		msgObj["reasoning_content"] = makeStringNode(reasoningBuf.String())
	}
	if refusalBuf.Len() > 0 {
		msgObj["refusal"] = makeStringNode(refusalBuf.String())
	}
	if len(calls) > 0 {
		tcNodes := make([]jsonNode, 0, len(calls))
		for _, c := range calls {
			args := c.args.String()
			if args == "" {
				args = "{}"
			}
			tcNodes = append(tcNodes, jsonNode{
				kind: nodeObject,
				objVal: map[string]jsonNode{
					"id":   makeStringNode(c.id),
					"type": makeStringNode("function"),
					"function": {
						kind: nodeObject,
						objVal: map[string]jsonNode{
							"name":      makeStringNode(c.name),
							"arguments": makeStringNode(args),
						},
					},
				},
			})
		}
		msgObj["tool_calls"] = jsonNode{kind: nodeArray, arrVal: tcNodes}
	}
	return jsonNode{kind: nodeObject, objVal: msgObj}, calls, nil
}
