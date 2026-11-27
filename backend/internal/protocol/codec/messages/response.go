package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// response.go decodes upstream non-streaming Anthropic Messages JSON replies
// into validated ir.Response values and encodes normalized ir.Response values
// into deterministic client-facing Anthropic Messages JSON bodies.
//
// Decoding validates usage envelopes through wirejson, enforces single-value
// duplicate-free JSON object syntax on tool_use.input, maps text citations onto
// Part.Annotations, and skips empty text blocks without metadata so buffered
// and streamed replies produce identical ResponseEssence summaries under Law S2.
// Encoding synthesizes missing message and tool_use identifiers through
// ClientView.Runtime and reports every dropped annotation, unsupported media
// item, foreign extension, or lowered tool reference as an ir.Loss record.

// DecodeResponse parses a non-streaming Anthropic Messages JSON response body
// into a validated ir.Response. Malformed JSON, duplicate object keys, invalid
// usage envelopes, non-object tool_use inputs, or upstream error envelopes
// return a descriptive error.
func DecodeResponse(body []byte) (*ir.Response, error) {
	var raw wireResponse
	if err := unmarshalJSONObject(body, &raw); err != nil {
		return nil, fmt.Errorf("decode messages response: %w", err)
	}
	if raw.Type == "error" || (len(raw.Content) == 0 && len(bytes.TrimSpace(raw.Error)) > 0) {
		if se := decodeStreamError(raw.Error); se != nil {
			return nil, fmt.Errorf("decode messages response: upstream error: %s", se.Message)
		}
		return nil, errors.New("decode messages response: upstream error")
	}

	usage, err := decodeMessagesUsage(body, raw.Usage)
	if err != nil {
		return nil, fmt.Errorf("decode messages response usage: %w", err)
	}

	parts := make([]ir.Part, 0, len(raw.Content))
	hasToolUse := false
	for i, elem := range raw.Content {
		part, keep, isToolUse, err := decodeResponseBlock(elem)
		if err != nil {
			return nil, fmt.Errorf("decode messages content[%d]: %w", i, err)
		}
		if isToolUse {
			hasToolUse = true
		}
		if keep {
			parts = append(parts, part)
		}
	}

	var rawReason string
	if raw.StopReason != nil {
		rawReason = *raw.StopReason
	}
	resp := &ir.Response{
		ID:     raw.ID,
		Model:  raw.Model,
		Parts:  parts,
		Finish: decodeStopReason(rawReason, raw.StopSequence, hasToolUse),
		Usage:  usage,
		Extra:  toExtra(raw.Unknown),
	}

	if err := ir.ValidateResponse(resp); err != nil {
		return nil, fmt.Errorf("validate decoded messages response: %w", err)
	}
	return resp, nil
}

// DecodeResponse parses a non-streaming Anthropic Messages JSON response body
// into a validated ir.Response by delegating to the package-level DecodeResponse.
// Malformed JSON, duplicate object keys, or upstream error bodies return an error.
func (Codec) DecodeResponse(body []byte) (*ir.Response, error) {
	return DecodeResponse(body)
}

// decodeResponseBlock parses one content block inside a buffered Messages
// response into an ir.Part. It reports whether the block survives empty-text
// filtering and whether it represents a callable tool_use block.
func decodeResponseBlock(raw jsontext.Value) (ir.Part, bool, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	var probe wireTypeProbe
	if err := unmarshalJSONObject(trimmed, &probe); err != nil {
		return ir.Part{}, false, false, err
	}

	switch probe.Type {
	case "text":
		var tb wireResponseTextBlock
		if err := unmarshalJSONObject(trimmed, &tb); err != nil {
			return ir.Part{}, false, false, err
		}
		cache, err := decodeCacheControl(tb.CacheControl)
		if err != nil {
			return ir.Part{}, false, false, err
		}
		anns := decodeCitations(tb.Citations)
		// Drop empty text blocks carrying no citations, cache breakpoint, or Extra
		// members so buffered replies match ir.Accumulate on empty streamed text blocks.
		if tb.Text == "" && len(anns) == 0 && cache == nil && len(tb.Unknown) == 0 {
			return ir.Part{}, false, false, nil
		}
		return ir.Part{
			Kind:        ir.PartText,
			Text:        tb.Text,
			Annotations: anns,
			Cache:       cache,
			Extra:       toExtra(tb.Unknown),
		}, true, false, nil

	case "thinking":
		var th wireThinkingBlock
		if err := unmarshalJSONObject(trimmed, &th); err != nil {
			return ir.Part{}, false, false, err
		}
		cache, err := decodeCacheControl(th.CacheControl)
		if err != nil {
			return ir.Part{}, false, false, err
		}
		var segs []string
		if th.Thinking != "" {
			segs = []string{th.Thinking}
		}
		var tok *ir.ReplayToken
		if th.Signature != "" {
			tok = &ir.ReplayToken{Data: th.Signature}
		}
		return ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Segments: segs,
				Token:    tok,
			},
			Cache: cache,
			Extra: toExtra(th.Unknown),
		}, true, false, nil

	case "redacted_thinking":
		var rt wireRedactedThinkingBlock
		if err := unmarshalJSONObject(trimmed, &rt); err != nil {
			return ir.Part{}, false, false, err
		}
		if rt.Data == "" {
			return ir.Part{}, false, false, errors.New("redacted_thinking block requires non-empty data")
		}
		cache, err := decodeCacheControl(rt.CacheControl)
		if err != nil {
			return ir.Part{}, false, false, err
		}
		return ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Redacted: true,
				Token:    &ir.ReplayToken{Data: rt.Data},
			},
			Cache: cache,
			Extra: toExtra(rt.Unknown),
		}, true, false, nil

	case "tool_use":
		var tu wireToolUseBlock
		if err := unmarshalJSONObject(trimmed, &tu); err != nil {
			return ir.Part{}, false, false, err
		}
		if strings.TrimSpace(tu.Name) == "" {
			return ir.Part{}, false, false, errors.New("tool_use block requires non-empty name")
		}
		if !isValidSingleJSONObject(tu.Input) {
			return ir.Part{}, false, false, errors.New("tool_use.input must be a valid JSON object")
		}
		cache, err := decodeCacheControl(tu.CacheControl)
		if err != nil {
			return ir.Part{}, false, false, err
		}
		return ir.Part{
			Kind: ir.PartToolCall,
			Call: &ir.ToolCall{
				CallID:    tu.ID,
				Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: tu.Name},
				Arguments: string(compactRawJSON(tu.Input)),
			},
			Cache: cache,
			Extra: toExtra(tu.Unknown),
		}, true, true, nil

	case "":
		return ir.Part{}, false, false, errors.New("response content block requires non-empty type")

	default:
		return ir.Part{
			Kind: ir.PartOpaque,
			Opaque: &ir.OpaqueItem{
				Origin: protocol.ProtocolAnthropicMessages,
				Type:   probe.Type,
				Raw:    compactRawJSON(trimmed),
			},
		}, true, false, nil
	}
}

// EncodeResponse serializes resp into deterministic Anthropic Messages JSON
// bytes using view for client model presentation and missing ID synthesis,
// returning all incurred information-loss records. Nil responses return an error.
//
// Refusal parts serialize as normal text blocks paired with stop_reason="refusal"
// when Finish.Reason is FinishContentFilter, and part-level Loss entries emit in
// the exact sequence produced by StreamEncoder so Law S6 parity holds.
func EncodeResponse(resp *ir.Response, view ir.ClientView) ([]byte, []ir.Loss, error) {
	if resp == nil {
		return nil, nil, errors.New("encode messages response: nil response")
	}

	var losses []ir.Loss
	id := resp.ID
	if id == "" {
		id = view.Runtime.NextID("msg_", "msg_0")
	}
	model := resolveClientModel(view, resp.Model)

	// Strip envelope keys from restored Extra so modeled top-level response
	// fields never collide with json:",embed" members during marshaling.
	unknown := restoreOrDropExtra(resp.Extra, "", &losses)
	delete(unknown, "id")
	delete(unknown, "type")
	delete(unknown, "role")
	delete(unknown, "model")
	delete(unknown, "content")
	delete(unknown, "stop_reason")
	delete(unknown, "stop_sequence")
	delete(unknown, "usage")

	content := make([]jsontext.Value, 0, len(resp.Parts))
	hasToolUse := false
	toolSeq := 0

	for idx, p := range resp.Parts {
		pPath := fmt.Sprintf("parts[%d]", idx)
		switch p.Kind {
		case ir.PartText, ir.PartRefusal:
			if p.ID != "" {
				losses = append(losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			if len(p.Annotations) > 0 {
				losses = append(losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: pPath})
			}
			if p.Token != nil {
				losses = append(losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
			}
			cache := encodeCacheControl(p.Cache, pPath+".cache", &losses)
			pUnknown := restoreOrDropExtra(p.Extra, pPath, &losses)
			if p.Text == "" && cache == nil && len(pUnknown) == 0 {
				losses = append(losses, ir.Loss{
					Kind:   ir.LossClientMarkerDropped,
					Path:   pPath,
					Detail: "empty_text",
				})
				continue
			}
			if p.Kind == ir.PartRefusal {
				losses = append(losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: pPath})
			}
			raw, err := marshalDeterministic(wireTextBlock{
				Type:         "text",
				Text:         p.Text,
				CacheControl: cache,
				Unknown:      pUnknown,
			})
			if err != nil {
				return nil, nil, err
			}
			content = append(content, jsontext.Value(raw))

		case ir.PartReasoning:
			if p.ID != "" {
				losses = append(losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			if p.Reasoning != nil && p.Reasoning.Redacted {
				pUnknown := restoreOrDropExtra(p.Extra, pPath, &losses)
				if p.Reasoning.Token != nil && p.Reasoning.Token.Data != "" {
					if joined := strings.Join(p.Reasoning.Segments, ""); joined != "" {
						losses = append(losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
					}
					cache := encodeCacheControl(p.Cache, pPath+".cache", &losses)
					raw, err := marshalDeterministic(wireRedactedThinkingBlock{
						Type:         "redacted_thinking",
						Data:         p.Reasoning.Token.Data,
						CacheControl: cache,
						Unknown:      pUnknown,
					})
					if err != nil {
						return nil, nil, err
					}
					content = append(content, jsontext.Value(raw))
				} else {
					if p.Cache != nil {
						losses = append(losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
					}
					if len(p.Reasoning.Segments) > 0 {
						losses = append(losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
					}
				}
				continue
			}

			cache := encodeCacheControl(p.Cache, pPath+".cache", &losses)
			pUnknown := restoreOrDropExtra(p.Extra, pPath, &losses)
			var joined, sig string
			if p.Reasoning != nil {
				joined = strings.Join(p.Reasoning.Segments, "")
				if p.Reasoning.Token != nil {
					sig = p.Reasoning.Token.Data
				}
			}
			raw, err := marshalDeterministic(wireThinkingBlock{
				Type:         "thinking",
				Thinking:     joined,
				Signature:    sig,
				CacheControl: cache,
				Unknown:      pUnknown,
			})
			if err != nil {
				return nil, nil, err
			}
			content = append(content, jsontext.Value(raw))

		case ir.PartToolCall:
			if p.Call == nil {
				continue
			}
			if p.Call.Tool.Kind == ir.ToolHosted {
				if p.ID != "" {
					losses = append(losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
				}
				if p.Cache != nil {
					losses = append(losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
				}
				_ = restoreOrDropExtra(p.Extra, pPath, &losses)
				losses = append(losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
				continue
			}

			toolSeq++
			callID := p.Call.CallID
			if callID == "" {
				callID = view.Runtime.NextID("toolu_", fmt.Sprintf("toolu_%d", toolSeq))
				losses = append(losses, ir.Loss{
					Kind: ir.LossIDRewritten,
					Path: pPath + ".call_id",
				})
			}
			fnName := resolveResponseCallName(p.Call.Tool, pPath, &losses)
			if p.ID != "" {
				losses = append(losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			if p.Call.Token != nil {
				losses = append(losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
			}
			cache := encodeCacheControl(p.Cache, pPath+".cache", &losses)
			pUnknown := restoreOrDropExtra(p.Extra, pPath, &losses)

			inputVal, err := encodeResponseToolInput(*p.Call)
			if err != nil {
				return nil, nil, err
			}
			raw, err := marshalDeterministic(wireToolUseBlock{
				Type:         "tool_use",
				ID:           callID,
				Name:         fnName,
				Input:        inputVal,
				CacheControl: cache,
				Unknown:      pUnknown,
			})
			if err != nil {
				return nil, nil, err
			}
			hasToolUse = true
			content = append(content, jsontext.Value(raw))

		case ir.PartMedia:
			if p.ID != "" {
				losses = append(losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			if p.Cache != nil {
				losses = append(losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, &losses)
			losses = append(losses, ir.Loss{
				Kind:   ir.LossMediaUnsupported,
				Path:   pPath,
				Detail: "assistant_output",
			})

		case ir.PartOpaque:
			if p.ID != "" {
				losses = append(losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			if p.Cache != nil {
				losses = append(losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, &losses)
			if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolAnthropicMessages {
				content = append(content, jsontext.Value(bytes.Clone(p.Opaque.Raw)))
			} else {
				detail := ""
				if p.Opaque != nil {
					detail = p.Opaque.Type
				}
				losses = append(losses, ir.Loss{
					Kind:   ir.LossOpaqueItemDropped,
					Path:   pPath,
					Detail: detail,
				})
			}
		}
	}

	stopReason, stopSeq := encodeStopReason(resp.Finish, hasToolUse)
	out := wireResponseWrite{
		ID:           id,
		Type:         messagesTypeMessage,
		Role:         messagesRoleAssistant,
		Model:        model,
		Content:      content,
		StopReason:   &stopReason,
		StopSequence: stopSeq,
		Usage:        encodeMessagesUsage(resp.Usage),
		Unknown:      unknown,
	}

	encoded, err := marshalDeterministic(out)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal messages response: %w", err)
	}
	return encoded, losses, nil
}

// EncodeResponse serializes resp into deterministic Anthropic Messages JSON
// bytes using view for client model presentation and missing ID synthesis.
// Nil responses return an error while dropped or lowered fields return Loss entries.
func (Codec) EncodeResponse(resp *ir.Response, view ir.ClientView) ([]byte, []ir.Loss, error) {
	return EncodeResponse(resp, view)
}

// resolveClientModel selects the client-visible model name in priority order:
// ClientView.Model, upstream responseModel, and ClientView.Request.Model. When
// all three sources are empty, it returns "unknown" so model is never blank.
func resolveClientModel(view ir.ClientView, responseModel string) string {
	if view.Model != "" {
		return view.Model
	}
	if responseModel != "" {
		return responseModel
	}
	if view.Request != nil && view.Request.Model != "" {
		return view.Request.Model
	}
	return "unknown"
}

// resolveResponseCallName maps ref onto an Anthropic Messages tool name for
// response or stream encoding. Custom, client_search, and non-default namespaced
// tool references record LossToolLowered at path.
func resolveResponseCallName(ref ir.ToolRef, path string, losses *[]ir.Loss) string {
	name := ref.Name
	if ref.Kind == ir.ToolClientSearch && name == "" {
		name = toolSearchProxyName
	}
	if ref.Namespace != "" {
		name = flattenMessagesToolName(ref.Namespace, name, defaultMessagesToolNameMaxBytes)
	}
	name = sanitizeMessagesToolChars(name)
	if len(name) > defaultMessagesToolNameMaxBytes {
		name = flattenMessagesToolName(ref.Namespace, name, defaultMessagesToolNameMaxBytes)
	}
	if isLoweredToolRef(ref) {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolLowered,
			Path: path,
		})
	}
	return name
}

// encodeResponseToolInput converts call.Arguments into a valid JSON object for
// wireToolUseBlock.Input. Custom tools wrap free-form text inside {"input": ...},
// while blank or incomplete invalid arguments fall back to {}.
func encodeResponseToolInput(call ir.ToolCall) (jsontext.Value, error) {
	if call.Tool.Kind == ir.ToolCustom {
		wrapped, err := marshalDeterministic(struct {
			Input string `json:"input"` // raw custom tool free-text payload
		}{Input: call.Arguments})
		if err != nil {
			return nil, err
		}
		return jsontext.Value(wrapped), nil
	}
	trimmed := strings.TrimSpace(call.Arguments)
	// Substitute {} when cross-protocol function arguments are empty or truncated
	// so outgoing Messages tool_use.input always serializes a valid JSON object.
	if trimmed == "" || !isValidSingleJSONObject([]byte(trimmed)) {
		return jsontext.Value("{}"), nil
	}
	return compactRawJSON(jsontext.Value(trimmed)), nil
}
