package chat

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// response.go decodes upstream non-streaming OpenAI Chat Completions JSON
// replies into ir.Response values and encodes normalized ir.Response values
// back into deterministic client-facing Chat Completions JSON bodies.
//
// Decoding validates any present usage envelope through wirejson before
// projecting disjoint token buckets, preserves empty-string versus structured
// tool arguments, and folds empty assistant content strings into zero output
// parts so buffered and streamed empty replies share identical ResponseEssence
// summaries under Law S2. Encoding synthesizes missing response or call
// identifiers via ClientView.Runtime and reports every dropped token,
// annotation, extension field, or part-group reordering as an ir.Loss entry.

// DecodeResponse parses a non-streaming OpenAI Chat Completions JSON response
// body into a validated ir.Response. Malformed JSON, duplicate keys, ambiguous
// usage envelopes, or upstream error payloads without choices return an error.
//
// When choices contains multiple completion candidates, DecodeResponse extracts
// choices[0] to match TokenRouter's single-candidate IR model while preserving
// top-level id, model, created, service_tier, and Chat Extra members.
func DecodeResponse(body []byte) (*ir.Response, error) {
	var raw wireResponse
	if err := unmarshalJSONObject(body, &raw); err != nil {
		return nil, fmt.Errorf("decode chat response: %w", err)
	}
	if len(raw.Choices) == 0 {
		if se := decodeStreamError(raw.Error); se != nil {
			return nil, fmt.Errorf("decode chat response: upstream error: %s", se.Message)
		}
	}

	usage, err := decodeChatUsage(body, raw.Usage)
	if err != nil {
		return nil, fmt.Errorf("decode chat response usage: %w", err)
	}

	resp := &ir.Response{
		ID:          raw.ID,
		Model:       raw.Model,
		CreatedAt:   raw.Created,
		Parts:       []ir.Part{},
		Finish:      ir.Finish{Reason: ir.FinishStop},
		Usage:       usage,
		ServiceTier: raw.ServiceTier,
		Extra:       toExtra(raw.Unknown),
	}

	if len(raw.Choices) > 0 {
		choice := raw.Choices[0]
		parts, hasToolCalls, err := decodeResponseMessageParts(choice.Message)
		if err != nil {
			return nil, fmt.Errorf("decode chat choices[0].message: %w", err)
		}
		resp.Parts = parts
		resp.Finish = decodeFinishReason(choice.FinishReason, hasToolCalls)
	}

	if err := ir.ValidateResponse(resp); err != nil {
		return nil, fmt.Errorf("validate decoded chat response: %w", err)
	}
	return resp, nil
}

// DecodeResponse parses a non-streaming OpenAI Chat Completions JSON response
// body into a validated ir.Response by delegating to the package-level
// DecodeResponse. Malformed JSON, error envelopes, or invalid tool arguments return an error.
func (Codec) DecodeResponse(body []byte) (*ir.Response, error) {
	return DecodeResponse(body)
}

// decodeResponseMessageParts extracts ordered IR output parts from a buffered
// assistant wireInboundMessage and reports whether any tool call was decoded.
// Non-empty tool call arguments must be syntactically valid single JSON values
// without duplicate keys, while empty plain text strings without annotations
// are omitted so empty buffered replies match empty streamed replies under Law S2.
func decodeResponseMessageParts(m wireInboundMessage) ([]ir.Part, bool, error) {
	parts := make([]ir.Part, 0, 2+len(m.ToolCalls))

	if reasoningText := selectMessageReasoning(m.ReasoningContent, m.Reasoning); reasoningText != "" {
		parts = append(parts, ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Segments: []string{reasoningText},
			},
		})
	}

	contentParts, err := decodeBufferedAssistantContent(m.Content, m.Annotations)
	if err != nil {
		return nil, false, err
	}
	parts = append(parts, contentParts...)

	if m.Refusal != "" {
		parts = append(parts, ir.Part{
			Kind: ir.PartRefusal,
			Text: m.Refusal,
		})
	}

	hasToolCalls := false
	for i, tc := range m.ToolCalls {
		if strings.TrimSpace(tc.Function.Name) == "" {
			return nil, false, fmt.Errorf("tool_calls[%d]: empty function.name", i)
		}
		args, err := decodeFunctionArguments(tc.Function.RawArgs)
		if err != nil {
			return nil, false, fmt.Errorf("tool_calls[%d]: %w", i, err)
		}
		if strings.TrimSpace(args) != "" && !isValidSingleJSON([]byte(args)) {
			return nil, false, fmt.Errorf("tool_calls[%d]: invalid function.arguments JSON", i)
		}
		hasToolCalls = true
		parts = append(parts, ir.Part{
			Kind: ir.PartToolCall,
			Call: &ir.ToolCall{
				CallID:    tc.ID,
				Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: tc.Function.Name},
				Arguments: args,
			},
			Extra: toExtra(tc.Unknown),
		})
	}

	if m.FunctionCall != nil {
		if strings.TrimSpace(m.FunctionCall.Name) == "" {
			return nil, false, errors.New("function_call: empty name")
		}
		args, err := decodeFunctionArguments(m.FunctionCall.RawArgs)
		if err != nil {
			return nil, false, fmt.Errorf("function_call: %w", err)
		}
		if strings.TrimSpace(args) != "" && !isValidSingleJSON([]byte(args)) {
			return nil, false, errors.New("function_call: invalid arguments JSON")
		}
		hasToolCalls = true
		parts = append(parts, ir.Part{
			Kind: ir.PartToolCall,
			Call: &ir.ToolCall{
				CallID:    "",
				Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: m.FunctionCall.Name},
				Arguments: args,
			},
		})
	}

	return parts, hasToolCalls, nil
}

// decodeBufferedAssistantContent parses scalar or array content on a buffered
// response message. Empty text blocks without annotations or Extra fields are
// skipped, while message-level annotations attach to the first surviving text part.
func decodeBufferedAssistantContent(raw jsontext.Value, msgAnns []wireAnnotation) ([]ir.Part, error) {
	trimmed := bytes.TrimSpace(raw)
	anns := decodeWireAnnotations(msgAnns)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		if len(anns) > 0 {
			return []ir.Part{{Kind: ir.PartText, Annotations: anns}}, nil
		}
		return nil, nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return nil, fmt.Errorf("decode response content string: %w", err)
		}
		if s == "" && len(anns) == 0 {
			return nil, nil
		}
		return []ir.Part{{
			Kind:        ir.PartText,
			Text:        s,
			Annotations: anns,
		}}, nil
	}
	parsed, err := decodeContentPartArray(trimmed, ir.RoleAssistant)
	if err != nil {
		return nil, err
	}
	filtered := make([]ir.Part, 0, len(parsed))
	for _, p := range parsed {
		if p.Kind == ir.PartText && p.Text == "" && len(p.Annotations) == 0 && p.Extra.IsZero() {
			continue
		}
		filtered = append(filtered, p)
	}
	if len(anns) > 0 {
		if len(filtered) > 0 && filtered[0].Kind == ir.PartText {
			filtered[0].Annotations = append(filtered[0].Annotations, anns...)
		} else {
			filtered = append([]ir.Part{{Kind: ir.PartText, Annotations: anns}}, filtered...)
		}
	}
	return filtered, nil
}

// EncodeResponse serializes resp into deterministic OpenAI Chat Completions
// JSON bytes using view for client model naming and missing ID or timestamp
// synthesis, returning all incurred information-loss records. Nil responses
// return an error without invoking Runtime callbacks.
//
// Assistant output parts fold into reasoning_content, content, refusal, and
// tool_calls fields while recording LossPartsReordered when source part order
// crossed Chat's fixed field groups. Completed non-custom tool calls with blank
// arguments emit "{}" so buffered and streamed client turn simulators agree.
func EncodeResponse(resp *ir.Response, view ir.ClientView) ([]byte, []ir.Loss, error) {
	if resp == nil {
		return nil, nil, errors.New("encode chat response: nil response")
	}

	var losses []ir.Loss
	id := resp.ID
	if id == "" {
		id = view.Runtime.NextID("chatcmpl-", "chatcmpl-0")
	}
	created := resp.CreatedAt
	if created <= 0 {
		created = view.Runtime.NowUnix()
	}
	model := resolveClientModel(view, resp.Model)
	unknown := restoreOrDropExtra(resp.Extra, "", &losses)

	var orderEntries []partGroupOrder
	var reasoningBuf strings.Builder
	var contentBuf strings.Builder
	var refusalBuf strings.Builder
	sawRefusal := false
	var toolCalls []wireToolCallWrite

	for idx, p := range resp.Parts {
		pPath := fmt.Sprintf("parts[%d]", idx)
		switch p.Kind {
		case ir.PartReasoning:
			recordPartMetadataLosses(p, pPath, true, &losses)
			_ = restoreOrDropExtra(p.Extra, pPath, &losses)
			if p.Reasoning != nil {
				if p.Reasoning.Token != nil {
					losses = append(losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
				}
				joined := strings.Join(p.Reasoning.Segments, "")
				if joined != "" {
					_, _ = reasoningBuf.WriteString(joined)
					orderEntries = append(orderEntries, partGroupOrder{origOrder: idx, groupRank: 0})
				}
			}

		case ir.PartText:
			recordPartMetadataLosses(p, pPath, true, &losses)
			_ = restoreOrDropExtra(p.Extra, pPath, &losses)
			_, _ = contentBuf.WriteString(p.Text)
			if p.Text != "" {
				orderEntries = append(orderEntries, partGroupOrder{origOrder: idx, groupRank: 1})
			}

		case ir.PartRefusal:
			recordPartMetadataLosses(p, pPath, true, &losses)
			_ = restoreOrDropExtra(p.Extra, pPath, &losses)
			sawRefusal = true
			_, _ = refusalBuf.WriteString(p.Text)
			orderEntries = append(orderEntries, partGroupOrder{origOrder: idx, groupRank: 2})

		case ir.PartToolCall:
			if p.Call == nil {
				continue
			}
			if p.Call.Tool.Kind == ir.ToolHosted {
				recordPartMetadataLosses(p, pPath, false, &losses)
				_ = restoreOrDropExtra(p.Extra, pPath, &losses)
				losses = append(losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
				continue
			}
			callID := p.Call.CallID
			if callID == "" {
				callID = view.Runtime.NextID("call_", fmt.Sprintf("call_%d", len(toolCalls)+1))
				losses = append(losses, ir.Loss{
					Kind: ir.LossIDRewritten,
					Path: pPath + ".call_id",
				})
			}
			fnName := resolveResponseCallName(p.Call.Tool, pPath, &losses)
			recordPartMetadataLosses(p, pPath, true, &losses)
			tcUnknown := restoreOrDropExtra(p.Extra, pPath, &losses)
			if p.Call.Token != nil {
				losses = append(losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
			}
			argsCopy := p.Call.Arguments
			// Wrap custom free-text inputs into {"input":"..."} and normalize blank
			// completed function calls to "{}" so buffered responses match StreamEncoder.
			if p.Call.Tool.Kind == ir.ToolCustom {
				wrapped, err := marshalDeterministic(struct {
					Input string `json:"input"` // raw custom tool text payload
				}{Input: argsCopy})
				if err != nil {
					return nil, nil, err
				}
				argsCopy = string(wrapped)
			} else if strings.TrimSpace(argsCopy) == "" && !p.Call.Incomplete {
				argsCopy = "{}"
			}
			toolCalls = append(toolCalls, wireToolCallWrite{
				ID:   callID,
				Type: "function",
				Function: wireFunctionCallWrite{
					Name:      fnName,
					Arguments: &argsCopy,
				},
				Unknown: tcUnknown,
			})
			orderEntries = append(orderEntries, partGroupOrder{origOrder: idx, groupRank: 3})

		case ir.PartMedia:
			recordPartMetadataLosses(p, pPath, true, &losses)
			_ = restoreOrDropExtra(p.Extra, pPath, &losses)
			losses = append(losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: pPath, Detail: "response"})

		case ir.PartOpaque:
			recordPartMetadataLosses(p, pPath, true, &losses)
			_ = restoreOrDropExtra(p.Extra, pPath, &losses)
			detail := ""
			if p.Opaque != nil {
				detail = p.Opaque.Type
			}
			losses = append(losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath, Detail: detail})
		}
	}

	if checkGroupOrdering(orderEntries) {
		losses = append(losses, ir.Loss{
			Kind: ir.LossPartsReordered,
			Path: "",
		})
	}

	var contentVal jsontext.Value
	if contentBuf.Len() > 0 || (len(toolCalls) == 0 && !sawRefusal) {
		rawStr, err := marshalDeterministic(contentBuf.String())
		if err != nil {
			return nil, nil, err
		}
		contentVal = jsontext.Value(rawStr)
	} else {
		contentVal = jsontext.Value("null")
	}

	out := wireResponseWrite{
		ID:          id,
		Object:      chatObjectCompletion,
		Created:     created,
		Model:       model,
		ServiceTier: resp.ServiceTier,
		Choices: []wireChoiceWrite{{
			Index: 0,
			Message: wireResponseMessage{
				Role:             "assistant",
				Content:          contentVal,
				ReasoningContent: reasoningBuf.String(),
				Refusal:          refusalBuf.String(),
				ToolCalls:        toolCalls,
			},
			FinishReason: encodeFinishReason(resp.Finish),
		}},
		Usage:   encodeChatUsage(resp.Usage),
		Unknown: unknown,
	}

	encoded, err := marshalDeterministic(out)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal chat response: %w", err)
	}
	return encoded, losses, nil
}

// EncodeResponse serializes resp into deterministic OpenAI Chat Completions
// JSON bytes under view by delegating to the package-level EncodeResponse. Nil
// responses return an error without invoking Runtime callbacks.
func (Codec) EncodeResponse(resp *ir.Response, view ir.ClientView) ([]byte, []ir.Loss, error) {
	return EncodeResponse(resp, view)
}

// resolveClientModel selects the client-visible model name in priority order:
// ClientView.Model, upstream responseModel, then ClientView.Request.Model.
// When all three are blank, resolveClientModel returns "unknown".
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

// resolveResponseCallName maps ref onto a Chat function name for response or
// stream encoding. Custom, client_search, and non-default namespaced references
// append LossToolLowered at path.
func resolveResponseCallName(ref ir.ToolRef, path string, losses *[]ir.Loss) string {
	name := ref.Name
	if ref.Kind == ir.ToolClientSearch && name == "" {
		name = toolSearchProxyName
	}
	if ref.Namespace != "" {
		name = flattenChatToolName(ref.Namespace, name, defaultChatToolNameMaxBytes)
	}
	if isLoweredToolRef(ref) {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolLowered,
			Path: path,
		})
	}
	return name
}
