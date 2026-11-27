package responses

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// response.go decodes non-streaming OpenAI Responses JSON replies into
// normalized ir.Response values and encodes ir.Response values back into
// deterministic client-facing OpenAI Responses JSON bodies.
//
// Buffered decoding validates usage envelopes through wirejson before
// projecting disjoint prompt and cache counters, validates non-empty function
// and tool_search JSON arguments without restricting free-form custom_tool_call
// inputs, and omits empty metadata-free output_text and refusal blocks so
// buffered and streamed empty replies match under Law S2. Outbound encoding
// shares output-item and request-echo builders with StreamEncoder so buffered
// and streaming paths emit identical identifiers, items, and ir.Loss records.

// responseEnvelopeFieldNames lists canonical lower-case field names written by
// wireResponseWrite so case-variant Unknown keys never collide under wirejson.
var responseEnvelopeFieldNames = map[string]bool{
	"id":                   true,
	"object":               true,
	"created_at":           true,
	"status":               true,
	"incomplete_details":   true,
	"error":                true,
	"service_tier":         true,
	"model":                true,
	"instructions":         true,
	"max_output_tokens":    true,
	"parallel_tool_calls":  true,
	"previous_response_id": true,
	"store":                true,
	"temperature":          true,
	"top_p":                true,
	"reasoning":            true,
	"text":                 true,
	"tools":                true,
	"tool_choice":          true,
	"output":               true,
	"usage":                true,
}

// responseEchoFields bundles optional request configuration echoed back onto
// client-facing OpenAI Responses envelopes.
type responseEchoFields struct {
	instructions       string               // echoed system instructions string
	maxOutputTokens    *int64               // echoed max_output_tokens cap
	parallelToolCalls  *bool                // echoed parallel_tool_calls toggle
	previousResponseID string               // echoed previous_response_id string
	store              *bool                // echoed store toggle
	temperature        *float64             // echoed sampling temperature
	topP               *float64             // echoed nucleus sampling top_p
	reasoning          *wireReasoningConfig // echoed reasoning effort and summary
	text               *wireTextConfig      // echoed text format and verbosity
	tools              []jsontext.Value     // echoed request tool declarations
	toolChoice         jsontext.Value       // echoed request tool_choice selector
}

// DecodeResponse parses a non-streaming OpenAI Responses JSON reply into a
// validated ir.Response. Malformed JSON, duplicate object keys, ambiguous usage
// envelopes, upstream failed responses without output, or malformed JSON
// function arguments return an error.
//
// Assistant message blocks lacking text, citations, and Extra fields are omitted
// so buffered and streamed empty replies produce identical zero-part responses
// under Law S2. Free-form custom_tool_call inputs bypass JSON validation while
// standard echoed request fields stay modeled on wireResponse rather than
// polluting Response.Extra.
func DecodeResponse(body []byte) (*ir.Response, error) {
	var raw wireResponse
	if err := unmarshalJSONObject(body, &raw); err != nil {
		return nil, fmt.Errorf("decode responses reply: %w", err)
	}

	if len(raw.Output) == 0 {
		if se := decodeResponsesError(raw.Error); se != nil {
			return nil, fmt.Errorf("decode responses reply: upstream error: %s", se.Message)
		}
		if strings.TrimSpace(raw.Status) == "failed" {
			return nil, errors.New("decode responses reply: upstream response status is failed")
		}
	}

	usage, err := decodeResponsesUsage(body, raw.Usage)
	if err != nil {
		return nil, fmt.Errorf("decode responses usage: %w", err)
	}

	parts := make([]ir.Part, 0, len(raw.Output))
	hasCallableTools := false
	for idx, rawElem := range raw.Output {
		itemParts, callable, decErr := decodeResponseOutputItem(bytes.TrimSpace(rawElem), fmt.Sprintf("output[%d]", idx))
		if decErr != nil {
			return nil, decErr
		}
		if callable {
			hasCallableTools = true
		}
		parts = append(parts, itemParts...)
	}

	finish := decodeResponsesFinish(raw.Status, raw.IncompleteDetails, hasCallableTools)
	resp := &ir.Response{
		ID:          raw.ID,
		Model:       raw.Model,
		CreatedAt:   raw.CreatedAt,
		Parts:       parts,
		Finish:      finish,
		Usage:       usage,
		ServiceTier: raw.ServiceTier,
		Extra:       toExtra(raw.Unknown),
	}

	if err := ir.ValidateResponse(resp); err != nil {
		return nil, fmt.Errorf("validate decoded responses reply: %w", err)
	}
	return resp, nil
}

// DecodeResponse parses a non-streaming OpenAI Responses JSON reply into a
// validated ir.Response by delegating to the package-level DecodeResponse function.
// Malformed JSON, invalid usage envelopes, or failed upstream envelopes without output return an error.
func (Codec) DecodeResponse(body []byte) (*ir.Response, error) {
	return DecodeResponse(body)
}

// EncodeResponse serializes resp into deterministic OpenAI Responses JSON bytes
// using view for client model naming, request echo fields, and missing ID or
// timestamp synthesis, returning all incurred information-loss records. Nil
// responses return an error without invoking Runtime callbacks.
//
// Consecutive text and refusal parts coalesce into assistant message items,
// completed blank function_call arguments normalize to "{}", and Runtime.NextID
// executes in the exact same order as StreamEncoder so buffered and streaming
// client responses match under Law S6.
func EncodeResponse(resp *ir.Response, view ir.ClientView) ([]byte, []ir.Loss, error) {
	if resp == nil {
		return nil, nil, errors.New("encode responses reply: nil response")
	}

	var losses []ir.Loss
	id := resp.ID
	if id == "" {
		id = view.Runtime.NextID("resp_", "resp_0")
	}
	createdAt := resp.CreatedAt
	if createdAt <= 0 {
		createdAt = view.Runtime.NowUnix()
	}
	model := resolveClientModel(view, resp.Model)
	respUnknown := sanitizeResponseUnknown(restoreOrDropExtra(resp.Extra, "", &losses), &losses)

	outputItems, err := encodeResponseOutputItems(resp.Parts, view.Runtime, &losses)
	if err != nil {
		return nil, nil, err
	}
	if outputItems == nil {
		outputItems = []jsontext.Value{}
	}

	status, incDetails, _ := encodeResponsesFinish(resp.Finish)
	var errBody *wireErrorBodyWrite
	if status == "failed" {
		errBody = encodeErrorBody(nil)
	}
	echo := buildResponseEchoFields(view.Request)

	out := wireResponseWrite{
		ID:                 id,
		Object:             responsesObjectTag,
		CreatedAt:          createdAt,
		Status:             status,
		IncompleteDetails:  incDetails,
		Error:              errBody,
		ServiceTier:        resp.ServiceTier,
		Model:              model,
		Instructions:       echo.instructions,
		MaxOutputTokens:    echo.maxOutputTokens,
		ParallelToolCalls:  echo.parallelToolCalls,
		PreviousResponseID: echo.previousResponseID,
		Store:              echo.store,
		Temperature:        echo.temperature,
		TopP:               echo.topP,
		Reasoning:          echo.reasoning,
		Text:               echo.text,
		Tools:              echo.tools,
		ToolChoice:         echo.toolChoice,
		Output:             outputItems,
		Usage:              encodeResponsesUsage(resp.Usage),
		Unknown:            respUnknown,
	}

	encoded, err := marshalDeterministic(out)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal responses reply: %w", err)
	}
	return encoded, losses, nil
}

// EncodeResponse serializes resp into deterministic OpenAI Responses JSON bytes
// by delegating to the package-level EncodeResponse function with view. Nil
// responses return an error without invoking view.Runtime callbacks.
func (Codec) EncodeResponse(resp *ir.Response, view ir.ClientView) ([]byte, []ir.Loss, error) {
	return EncodeResponse(resp, view)
}

// decodeResponseOutputItem parses one element of response.output[] into zero or
// more IR output Parts and reports whether the item is a client-callable tool call.
// Missing or whitespace-only item types and malformed JSON payloads return an error.
func decodeResponseOutputItem(raw []byte, path string) ([]ir.Part, bool, error) {
	var item wireResponseOutputItem
	if err := unmarshalJSONObject(raw, &item); err != nil {
		return nil, false, fmt.Errorf("decode %s: %w", path, err)
	}
	item.Type = strings.TrimSpace(item.Type)

	switch item.Type {
	case "message":
		return decodeResponseMessageOutput(item, path)

	case "reasoning":
		part, err := decodeResponseReasoningOutput(item, path)
		if err != nil {
			return nil, false, err
		}
		return []ir.Part{part}, false, nil

	case "function_call":
		part, err := decodeResponseFunctionCallOutput(item, true, path)
		if err != nil {
			return nil, false, err
		}
		return []ir.Part{part}, true, nil

	case "custom_tool_call":
		part, err := decodeResponseCustomCallOutput(item, path)
		if err != nil {
			return nil, false, err
		}
		return []ir.Part{part}, true, nil

	case "tool_search_call":
		part, err := decodeResponseToolSearchCallOutput(item, true, path)
		if err != nil {
			return nil, false, err
		}
		return []ir.Part{part}, true, nil

	case "":
		return nil, false, fmt.Errorf("decode %s: missing output item type", path)

	default:
		return []ir.Part{{
			Kind: ir.PartOpaque,
			ID:   item.ID,
			Opaque: &ir.OpaqueItem{
				Origin: protocol.ProtocolOpenAIResponses,
				Type:   item.Type,
				Raw:    compactRawJSON(raw),
			},
		}}, false, nil
	}
}

// decodeResponseMessageOutput extracts output_text and refusal parts from a
// buffered or completed assistant message output item. Empty blocks lacking
// annotations and Extra fields are omitted so Law S2 parity holds on empty replies.
func decodeResponseMessageOutput(item wireResponseOutputItem, path string) ([]ir.Part, bool, error) {
	var parts []ir.Part
	msgExtra := extractResponseMessageExtra(item.Unknown)
	usedMsgExtra := false

	for cIdx, rawBlock := range item.Content {
		cPath := fmt.Sprintf("%s.content[%d]", path, cIdx)
		textVal, kind, anns, partExtra, err := decodeResponseContentBlock(rawBlock, cPath)
		if err != nil {
			return nil, false, err
		}
		if !msgExtra.IsZero() && !usedMsgExtra && partExtra.IsZero() {
			partExtra = msgExtra
			usedMsgExtra = true
		}

		if kind == ir.PartRefusal {
			if textVal == "" && partExtra.IsZero() {
				continue
			}
			parts = append(parts, ir.Part{
				Kind:  ir.PartRefusal,
				ID:    item.ID,
				Text:  textVal,
				Extra: partExtra,
			})
			continue
		}

		if textVal == "" && len(anns) == 0 && partExtra.IsZero() {
			continue
		}
		parts = append(parts, ir.Part{
			Kind:        ir.PartText,
			ID:          item.ID,
			Text:        textVal,
			Annotations: anns,
			Extra:       partExtra,
		})
	}
	return parts, false, nil
}

// extractResponseMessageExtra strips content-part field names from message-level
// Unknown maps before converting remaining vendor members into an ir.Extra value.
// Empty maps return a zero ir.Extra without allocating.
func extractResponseMessageExtra(unknown map[string]jsontext.Value) ir.Extra {
	if len(unknown) == 0 {
		return ir.Extra{}
	}
	cleaned := cloneValueMap(unknown)
	delete(cleaned, "type")
	delete(cleaned, "text")
	delete(cleaned, "refusal")
	delete(cleaned, "annotations")
	return toExtra(cleaned)
}

// decodeResponseContentBlock parses one raw JSON block inside a response message
// content array into its text/refusal payload, PartKind, annotations, and collision-safe Extra.
// Unknown or blank block types and malformed citation arrays return an error.
func decodeResponseContentBlock(raw jsontext.Value, path string) (string, ir.PartKind, []ir.Annotation, ir.Extra, error) {
	var block wireResponseContentPart
	if err := unmarshalJSONObject(bytes.TrimSpace(raw), &block); err != nil {
		return "", "", nil, ir.Extra{}, fmt.Errorf("decode %s: %w", path, err)
	}
	blockType := strings.TrimSpace(block.Type)
	switch blockType {
	case "output_text", "text":
		var anns []ir.Annotation
		annTrim := bytes.TrimSpace(block.Annotations)
		if len(annTrim) > 0 && !bytes.Equal(annTrim, []byte("null")) {
			var rawAnns []wireAnnotation
			if err := unmarshalJSONValue(annTrim, &rawAnns); err != nil {
				return "", "", nil, ir.Extra{}, fmt.Errorf("decode %s.annotations: %w", path, err)
			}
			var err error
			anns, err = decodeAnnotations(rawAnns, path+".annotations")
			if err != nil {
				return "", "", nil, ir.Extra{}, err
			}
		}
		unk := cloneValueMap(block.Unknown)
		if block.Refusal != nil {
			unk = insertStringField(unk, "refusal", *block.Refusal)
		}
		textVal := ""
		if block.Text != nil {
			textVal = *block.Text
		}
		return textVal, ir.PartText, anns, toExtra(unk), nil

	case "refusal":
		unk := cloneValueMap(block.Unknown)
		if block.Text != nil {
			unk = insertStringField(unk, "text", *block.Text)
		}
		if annTrim := bytes.TrimSpace(block.Annotations); len(annTrim) > 0 {
			unk = insertRawMember(unk, "annotations", annTrim)
		}
		refusalVal := ""
		if block.Refusal != nil {
			refusalVal = *block.Refusal
		}
		return refusalVal, ir.PartRefusal, nil, toExtra(unk), nil

	case "":
		return "", "", nil, ir.Extra{}, fmt.Errorf("decode %s: empty message content part type", path)

	default:
		return "", "", nil, ir.Extra{}, fmt.Errorf("decode %s: unsupported response message content block type %q", path, block.Type)
	}
}

// decodeResponseReasoningOutput converts a response reasoning item into an
// ir.PartReasoning value. When both summary and content arrays are populated,
// summary segments populate Reasoning.Segments while content preserves inside Extra.
func decodeResponseReasoningOutput(item wireResponseOutputItem, path string) (ir.Part, error) {
	for i, seg := range item.Summary {
		if seg.Type != "" && seg.Type != "summary_text" {
			return ir.Part{}, fmt.Errorf("decode %s.summary[%d]: unsupported type %q", path, i, seg.Type)
		}
	}
	var parsedContent []wireReasoningText
	if len(item.Content) > 0 {
		parsedContent = make([]wireReasoningText, 0, len(item.Content))
		for i, rawSeg := range item.Content {
			var seg wireReasoningText
			if err := unmarshalJSONObject(bytes.TrimSpace(rawSeg), &seg); err != nil {
				return ir.Part{}, fmt.Errorf("decode %s.content[%d]: %w", path, i, err)
			}
			if seg.Type != "" && seg.Type != "reasoning_text" {
				return ir.Part{}, fmt.Errorf("decode %s.content[%d]: unsupported type %q", path, i, seg.Type)
			}
			parsedContent = append(parsedContent, seg)
		}
	}

	unk := cloneValueMap(item.Unknown)
	var segments []string
	var isSummary bool
	if len(item.Summary) > 0 {
		isSummary = true
		segments = make([]string, 0, len(item.Summary))
		for _, seg := range item.Summary {
			segments = append(segments, seg.Text)
		}
		if len(parsedContent) > 0 {
			rawCnt, err := marshalDeterministic(parsedContent)
			if err != nil {
				return ir.Part{}, err
			}
			unk = insertRawMember(unk, "content", jsontext.Value(rawCnt))
		}
	} else if len(parsedContent) > 0 {
		segments = make([]string, 0, len(parsedContent))
		for _, seg := range parsedContent {
			segments = append(segments, seg.Text)
		}
	}
	var tok *ir.ReplayToken
	if item.EncryptedContent != "" {
		tok = &ir.ReplayToken{Data: item.EncryptedContent}
	}
	return ir.Part{
		Kind: ir.PartReasoning,
		ID:   item.ID,
		Reasoning: &ir.Reasoning{
			Segments: segments,
			Summary:  isSummary,
			Token:    tok,
		},
		Extra: toExtra(unk),
	}, nil
}

// decodeResponseFunctionCallOutput converts a response function_call item into
// an ir.PartToolCall. When requireValidJSON is true, truncated or duplicate-key
// JSON arguments return an error; otherwise invalid fragments set Call.Incomplete.
func decodeResponseFunctionCallOutput(item wireResponseOutputItem, requireValidJSON bool, path string) (ir.Part, error) {
	if strings.TrimSpace(item.Name) == "" {
		return ir.Part{}, fmt.Errorf("decode %s: function_call requires non-empty name", path)
	}
	args, validJSON, err := decodeResponseFunctionArgs(item.RawArguments, path+".arguments")
	if err != nil {
		return ir.Part{}, err
	}
	if requireValidJSON && !validJSON {
		return ir.Part{}, fmt.Errorf("decode %s.arguments: function arguments are not valid single-value JSON", path)
	}
	return ir.Part{
		Kind: ir.PartToolCall,
		ID:   item.ID,
		Call: &ir.ToolCall{
			CallID: item.CallID,
			Tool: ir.ToolRef{
				Kind:      ir.ToolFunction,
				Namespace: item.Namespace,
				Name:      item.Name,
			},
			Arguments:  args,
			Incomplete: !validJSON,
		},
		Extra: toExtra(item.Unknown),
	}, nil
}

// decodeResponseFunctionArgs unmarshals string or raw JSON function_call
// arguments and reports whether non-blank content forms a single valid JSON value.
// Absent or null tokens return an empty string with validJSON set to true.
func decodeResponseFunctionArgs(raw jsontext.Value, path string) (string, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", true, nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return "", false, fmt.Errorf("decode %s: %w", path, err)
		}
		if strings.TrimSpace(s) == "" {
			return s, true, nil
		}
		return s, isValidSingleJSON([]byte(s)), nil
	}
	if !isValidSingleJSON(trimmed) {
		return string(trimmed), false, nil
	}
	return string(compactRawJSON(trimmed)), true, nil
}

// decodeResponseCustomCallOutput converts a response custom_tool_call item into
// an ir.PartToolCall while preserving free-form Input without JSON restrictions.
func decodeResponseCustomCallOutput(item wireResponseOutputItem, path string) (ir.Part, error) {
	if strings.TrimSpace(item.Name) == "" {
		return ir.Part{}, fmt.Errorf("decode %s: custom_tool_call requires non-empty name", path)
	}
	return ir.Part{
		Kind: ir.PartToolCall,
		ID:   item.ID,
		Call: &ir.ToolCall{
			CallID: item.CallID,
			Tool: ir.ToolRef{
				Kind:      ir.ToolCustom,
				Namespace: item.Namespace,
				Name:      item.Name,
			},
			Arguments: item.Input,
		},
		Extra: toExtra(item.Unknown),
	}, nil
}

// decodeResponseToolSearchCallOutput converts a response tool_search_call item
// into an ir.PartToolCall. Absent, null, or blank arguments normalize to "{}",
// while non-object or truncated JSON payloads return an error when
// requireValidJSON is true or mark Call.Incomplete when false.
func decodeResponseToolSearchCallOutput(item wireResponseOutputItem, requireValidJSON bool, path string) (ir.Part, error) {
	unk := cloneValueMap(item.Unknown)
	if item.Execution != "" && item.Execution != "client" {
		unk = insertStringField(unk, "execution", item.Execution)
	}
	args, validJSON, err := decodeResponseFunctionArgs(item.RawArguments, path+".arguments")
	if err != nil {
		return ir.Part{}, err
	}
	if validJSON && strings.TrimSpace(args) != "" && !isValidSingleJSONObject([]byte(args)) {
		validJSON = false
	}
	if requireValidJSON && !validJSON {
		return ir.Part{}, fmt.Errorf("decode %s.arguments: tool_search_call arguments must be a valid JSON object", path)
	}
	if validJSON {
		if strings.TrimSpace(args) == "" {
			args = "{}"
		} else {
			args = string(compactRawJSON(jsontext.Value(args)))
		}
	}
	return ir.Part{
		Kind: ir.PartToolCall,
		ID:   item.ID,
		Call: &ir.ToolCall{
			CallID:     item.CallID,
			Tool:       ir.ToolRef{Kind: ir.ToolClientSearch},
			Arguments:  args,
			Incomplete: !validJSON,
		},
		Extra: toExtra(unk),
	}, nil
}

// encodeResponseOutputItems converts resp.Parts into ordered OpenAI Responses
// output[] JSON objects. Grouping and rt.NextID calls follow the exact same
// order as StreamEncoder so buffered and streaming replies match under Law S6.
func encodeResponseOutputItems(parts []ir.Part, rt ir.Runtime, losses *[]ir.Loss) ([]jsontext.Value, error) {
	var out []jsontext.Value
	var msgContent []jsontext.Value
	var msgID string
	var msgExplicitID bool

	flushMsg := func() error {
		if len(msgContent) == 0 {
			return nil
		}
		raw, err := marshalDeterministic(wireResponseMessageWrite{
			ID:      msgID,
			Type:    "message",
			Status:  "completed",
			Role:    "assistant",
			Content: msgContent,
		})
		if err != nil {
			return err
		}
		out = append(out, jsontext.Value(raw))
		msgContent = nil
		msgID = ""
		msgExplicitID = false
		return nil
	}

	for idx, p := range parts {
		pPath := fmt.Sprintf("parts[%d]", idx)
		switch p.Kind {
		case ir.PartText, ir.PartRefusal:
			if len(msgContent) > 0 && p.ID != "" && msgExplicitID && p.ID != msgID {
				if err := flushMsg(); err != nil {
					return nil, err
				}
			}
			if len(msgContent) == 0 {
				if p.ID != "" {
					msgID = p.ID
					msgExplicitID = true
				} else {
					msgID = rt.NextID("msg_", fmt.Sprintf("msg_%d", len(out)))
					msgExplicitID = false
				}
			}
			if p.ID != msgID {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			cp, err := marshalResponseContentPartWrite(p, pPath, losses)
			if err != nil {
				return nil, err
			}
			msgContent = append(msgContent, cp)

		case ir.PartReasoning:
			if err := flushMsg(); err != nil {
				return nil, err
			}
			itemID := p.ID
			if itemID == "" {
				itemID = rt.NextID("rs_", fmt.Sprintf("rs_%d", len(out)))
			}
			if p.ID != itemID {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			raw, err := buildCompletedReasoningOutputJSON(itemID, p, pPath, losses)
			if err != nil {
				return nil, err
			}
			out = append(out, raw)

		case ir.PartToolCall:
			if p.Call == nil {
				continue
			}
			if p.Call.Tool.Kind == ir.ToolHosted {
				if err := flushMsg(); err != nil {
					return nil, err
				}
				if p.Cache != nil {
					*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
				}
				if p.Call.Token != nil {
					*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
				}
				_ = restoreOrDropExtra(p.Extra, pPath, losses)
				*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
				continue
			}
			if err := flushMsg(); err != nil {
				return nil, err
			}
			itemID := p.ID
			if itemID == "" {
				prefix := callItemIDPrefix(p.Call.Tool.Kind)
				itemID = rt.NextID(prefix, fmt.Sprintf("%s%d", prefix, len(out)))
			}
			callID := p.Call.CallID
			if callID == "" {
				callID = rt.NextID("call_", fmt.Sprintf("call_%d", len(out)))
			}
			if p.ID != itemID {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			if p.Call.CallID != callID {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".call_id"})
			}
			raw, err := buildCompletedToolCallOutputJSON(itemID, callID, p, pPath, losses)
			if err != nil {
				return nil, err
			}
			out = append(out, raw)

		case ir.PartOpaque:
			if err := flushMsg(); err != nil {
				return nil, err
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, losses)
			if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolOpenAIResponses {
				out = append(out, jsontext.Value(bytes.Clone(p.Opaque.Raw)))
				continue
			}
			detail := ""
			if p.Opaque != nil {
				detail = p.Opaque.Type
			}
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath, Detail: detail})

		case ir.PartMedia:
			if err := flushMsg(); err != nil {
				return nil, err
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: pPath, Detail: "response"})
		}
	}

	if err := flushMsg(); err != nil {
		return nil, err
	}
	return out, nil
}

// marshalResponseContentPartWrite serializes a completed output_text or refusal
// block into deterministic JSON bytes while recording dropped text replay tokens
// and unsupported Extra or annotation fields into losses. Output text blocks
// always initialize an empty non-nil annotations array so strict client SDKs
// never encounter null citations.
func marshalResponseContentPartWrite(p ir.Part, path string, losses *[]ir.Loss) (jsontext.Value, error) {
	if p.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
	}
	pUnk := restoreOrDropExtra(p.Extra, path, losses)
	if p.Kind == ir.PartRefusal {
		raw, err := marshalDeterministic(wireRefusalPartWrite{
			Type:    "refusal",
			Refusal: p.Text,
			Unknown: pUnk,
		})
		return jsontext.Value(raw), err
	}
	anns := make([]wireAnnotation, 0, len(p.Annotations))
	for aIdx, a := range p.Annotations {
		aPath := fmt.Sprintf("%s.annotations[%d]", path, aIdx)
		anns = append(anns, encodeAnnotation(a, aPath, losses))
	}
	raw, err := marshalDeterministic(wireOutputTextPartWrite{
		Type:        "output_text",
		Text:        p.Text,
		Annotations: anns,
		Unknown:     pUnk,
	})
	return jsontext.Value(raw), err
}

// buildCompletedReasoningOutputJSON serializes a completed reasoning output item
// into deterministic JSON bytes. Non-empty Token.Data values write directly into
// encrypted_content without carrier or scheme inspection per section 3.6.
func buildCompletedReasoningOutputJSON(itemID string, p ir.Part, path string, losses *[]ir.Loss) (jsontext.Value, error) {
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	rUnk := restoreOrDropExtra(p.Extra, path, losses)
	var extraContent []wireReasoningText
	if rawCnt, hasCnt := rUnk["content"]; hasCnt {
		delete(rUnk, "content")
		// Secondary reasoning_text segments stored in Extra["content"] alongside
		// non-empty summaries fall back to nil only if synthetic Extra JSON is invalid.
		_ = unmarshalJSONValue(rawCnt, &extraContent)
	}

	var encToken string
	var segments []string
	if p.Reasoning != nil {
		segments = p.Reasoning.Segments
		if p.Reasoning.Token != nil && p.Reasoning.Token.Data != "" {
			encToken = p.Reasoning.Token.Data
		}
	}

	summaries := make([]wireSummaryText, 0, len(segments))
	for _, seg := range segments {
		summaries = append(summaries, wireSummaryText{Type: "summary_text", Text: seg})
	}

	raw, err := marshalDeterministic(wireReasoningItem{
		ID:               itemID,
		Type:             "reasoning",
		Summary:          summaries,
		Content:          extraContent,
		EncryptedContent: encToken,
		Unknown:          rUnk,
	})
	return jsontext.Value(raw), err
}

// buildCompletedToolCallOutputJSON serializes a completed function_call,
// custom_tool_call, or tool_search_call output item for buffered and SSE responses.
// Non-blank client tool names are preserved verbatim without hash suffixes, while
// blank names fall back to "tool" and blank completed function or non-object
// tool_search_call arguments fall back to "{}" on the wire.
func buildCompletedToolCallOutputJSON(itemID, callID string, p ir.Part, path string, losses *[]ir.Loss) (jsontext.Value, error) {
	if p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
	}
	if p.Call != nil && p.Call.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path})
	}
	cUnk := restoreOrDropExtra(p.Extra, path, losses)
	call := p.Call

	switch call.Tool.Kind {
	case ir.ToolCustom:
		name, defaulted := resolveResponseToolCallName(call.Tool.Name)
		if defaulted {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: path})
		}
		raw, err := marshalDeterministic(wireResponseCustomCallWrite{
			ID:        itemID,
			Type:      "custom_tool_call",
			Status:    "completed",
			CallID:    callID,
			Namespace: call.Tool.Namespace,
			Name:      name,
			Input:     call.Arguments,
			Unknown:   cUnk,
		})
		return jsontext.Value(raw), err

	case ir.ToolClientSearch:
		execMode := "client"
		if rawExec, ok := cUnk["execution"]; ok {
			var s string
			// Non-string or empty execution Extra values fall back to "client" while
			// removing the key so Unknown cannot duplicate the modeled execution field.
			if unmarshalJSONValue(rawExec, &s) == nil && s != "" {
				execMode = s
			}
			delete(cUnk, "execution")
		}
		// Fall back to "{}" when tool_search_call arguments are blank or incomplete
		// non-objects so wireResponseToolSearchCallWrite always emits a JSON object.
		argsRaw := jsontext.Value("{}")
		if trimmed := strings.TrimSpace(call.Arguments); trimmed != "" && isValidSingleJSONObject([]byte(trimmed)) {
			argsRaw = compactRawJSON(jsontext.Value(trimmed))
		}
		raw, err := marshalDeterministic(wireResponseToolSearchCallWrite{
			ID:        itemID,
			Type:      "tool_search_call",
			Status:    "completed",
			CallID:    callID,
			Execution: execMode,
			Arguments: argsRaw,
			Unknown:   cUnk,
		})
		return jsontext.Value(raw), err

	default:
		name, defaulted := resolveResponseToolCallName(call.Tool.Name)
		if defaulted {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: path})
		}
		args := call.Arguments
		if strings.TrimSpace(args) == "" && !call.Incomplete {
			args = "{}"
		}
		raw, err := marshalDeterministic(wireResponseFunctionCallWrite{
			ID:        itemID,
			Type:      "function_call",
			Status:    "completed",
			CallID:    callID,
			Namespace: call.Tool.Namespace,
			Name:      name,
			Arguments: args,
			Unknown:   cUnk,
		})
		return jsontext.Value(raw), err
	}
}

// resolveResponseToolCallName returns raw untouched when non-blank so client-declared
// or ToolMap-restored custom and function names survive response encoding verbatim.
// Whitespace-only or empty names fall back to "tool" and report defaulted=true.
func resolveResponseToolCallName(raw string) (string, bool) {
	if strings.TrimSpace(raw) == "" {
		return "tool", true
	}
	return raw, false
}

// callItemIDPrefix returns the standard OpenAI item ID prefix for kind.
func callItemIDPrefix(kind ir.ToolKind) string {
	switch kind {
	case ir.ToolCustom:
		return "ctc_"
	case ir.ToolClientSearch:
		return "tsc_"
	default:
		return "fc_"
	}
}

// sanitizeResponseUnknown removes case-variant collisions against modeled
// wireResponseWrite fields from unknown. Dropping every member in unknown
// records LossExtraDropped at the root path so VerifyEssence stays balanced.
func sanitizeResponseUnknown(unknown map[string]jsontext.Value, losses *[]ir.Loss) map[string]jsontext.Value {
	if len(unknown) == 0 {
		return nil
	}
	droppedAny := false
	for k := range unknown {
		if responseEnvelopeFieldNames[strings.ToLower(k)] {
			delete(unknown, k)
			droppedAny = true
		}
	}
	if len(unknown) == 0 {
		if droppedAny {
			*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: ""})
		}
		return nil
	}
	return unknown
}

// resolveClientModel selects view.Model when non-blank so downstream responses
// reflect the client-requested model alias. Blank client model strings fall back
// to upstreamModel verbatim without trimming or rewriting.
func resolveClientModel(view ir.ClientView, upstreamModel string) string {
	if strings.TrimSpace(view.Model) != "" {
		return view.Model
	}
	return upstreamModel
}

// buildResponseEchoFields extracts non-mutating echo metadata from req for
// inclusion on buffered and streaming client-facing Responses envelopes. Nil
// requests return a zero responseEchoFields value without allocating.
func buildResponseEchoFields(req *ir.Request) responseEchoFields {
	if req == nil {
		return responseEchoFields{}
	}
	var sysTexts []string
	for _, p := range req.System {
		if p.Kind == ir.PartText && p.Text != "" {
			sysTexts = append(sysTexts, p.Text)
		}
	}
	var reasoning *wireReasoningConfig
	if req.Reasoning != nil && (req.Reasoning.Effort != "" || req.Reasoning.Summary != "") {
		reasoning = &wireReasoningConfig{
			Effort:  req.Reasoning.Effort,
			Summary: req.Reasoning.Summary,
		}
	}
	var ignoreLosses []ir.Loss
	var textCfg *wireTextConfig
	if req.Output != nil {
		textCfg, _ = encodeResponsesTextConfig(req.Output, &ignoreLosses)
	}
	toolsEcho, err := encodeResponsesTools(req.Tools, req.Namespaces, ir.Dialect{}, defaultResponsesToolNameMaxBytes, &ignoreLosses)
	// Unserializable synthetic tool definitions in partial ClientView.Request values
	// omit the optional tools echo field rather than aborting response encoding.
	if err != nil {
		toolsEcho = nil
	}
	toolChoiceEcho, _, err := encodeResponsesToolChoice(req.ToolChoice, ir.Dialect{}, defaultResponsesToolNameMaxBytes, nil, &ignoreLosses)
	// Unsupported synthetic ToolChoice modes in partial ClientView.Request values
	// omit the optional tool_choice echo field rather than aborting response encoding.
	if err != nil {
		toolChoiceEcho = nil
	}
	return responseEchoFields{
		instructions:       strings.Join(sysTexts, "\n"),
		maxOutputTokens:    cloneInt64(req.MaxOutputTokens),
		parallelToolCalls:  cloneBool(req.ParallelToolCalls),
		previousResponseID: req.PreviousResponseID,
		store:              cloneBool(req.Store),
		temperature:        req.Sampling.Temperature,
		topP:               req.Sampling.TopP,
		reasoning:          reasoning,
		text:               textCfg,
		tools:              toolsEcho,
		toolChoice:         toolChoiceEcho,
	}
}
