package generatecontent

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// response.go decodes and encodes buffered Google Gemini GenerateContentResponse
// JSON payloads to and from validated IR Response values. Gemini nests assistant
// output inside candidates[0].content.parts, reports grounding web search
// citations inside candidates[0].groundingMetadata, and uses FinishReason="STOP"
// for both plain text replies and functionCall tool-invocation turns.
//
// Decoding extracts candidates[0] parts while leaving omitted functionCall IDs
// empty for downstream convert sealing, merges split functionCall parts that
// share an explicit call ID, appends formatted Markdown grounding footers
// alongside structured url_citation Annotations, and validates usageMetadata via
// wirejson. Outbound encoding writes ReplayToken.Data verbatim without inspecting
// token schemes or envelopes and emits Loss records in exact parity with StreamEncoder.

// groundingState accumulates the first observed webSearchQueries and
// groundingChunks across buffered or streamed candidate observations.
type groundingState struct {
	queries []string             // first non-empty webSearchQueries list
	chunks  []wireGroundingChunk // first non-empty groundingChunks list
}

// bufferedToolSlot tracks an explicit-ID or unnamed functionCall part during
// buffered candidate decoding so split explicit-ID parts merge in place.
type bufferedToolSlot struct {
	partIndex int        // index of the placeholder Part inside parts slice
	rec       toolRecord // accumulated arguments, signature, and Extra maps
}

// observe records webSearchQueries and groundingChunks from gm when not already
// captured on g. Nil metadata pointers are ignored, and later streamed frames
// do not overwrite the first non-empty query or chunk slices observed.
func (g *groundingState) observe(gm *wireGroundingMetadata) {
	if gm == nil {
		return
	}
	if len(gm.WebSearchQueries) > 0 && len(g.queries) == 0 {
		g.queries = append([]string(nil), gm.WebSearchQueries...)
	}
	if len(gm.GroundingChunks) > 0 && len(g.chunks) == 0 {
		g.chunks = append([]wireGroundingChunk(nil), gm.GroundingChunks...)
	}
}

// format builds the Markdown grounding footer text for search queries and web
// sources, and extracts structured url_citation Annotations from captured web
// chunks. When neither queries nor web chunks were observed, format returns
// ("", nil) so ungrounded replies receive no trailing text.
func (g *groundingState) format() (string, []ir.Annotation) {
	if len(g.queries) == 0 && len(g.chunks) == 0 {
		return "", nil
	}

	var sb strings.Builder
	if len(g.queries) > 0 {
		_, _ = sb.WriteString("\n\n---\nWeb search queries: ")
		_, _ = sb.WriteString(strings.Join(g.queries, ", "))
	}

	var annotations []ir.Annotation
	if len(g.chunks) > 0 {
		var links []string
		for i, ch := range g.chunks {
			if ch.Web == nil {
				continue
			}
			rawTitle := strings.TrimSpace(ch.Web.Title)
			rawURI := strings.TrimSpace(ch.Web.URI)
			// Retain every web chunk at its 1-based original chunk index using
			// "Source" and "#" display defaults when title or URI is blank.
			dispTitle := rawTitle
			if dispTitle == "" {
				dispTitle = "Source"
			}
			dispURI := rawURI
			if dispURI == "" {
				dispURI = "#"
			}
			links = append(links, fmt.Sprintf("[%d] [%s](%s)", i+1, dispTitle, dispURI))
			if rawURI != "" || rawTitle != "" {
				annotations = append(annotations, ir.Annotation{
					Kind:  "url_citation",
					URL:   rawURI,
					Title: rawTitle,
				})
			}
		}
		if len(links) > 0 {
			_, _ = sb.WriteString("\n\nSources:\n")
			_, _ = sb.WriteString(strings.Join(links, "\n"))
		}
	}

	return sb.String(), annotations
}

// DecodeResponse parses a buffered Google Gemini GenerateContentResponse JSON
// body into a validated ir.Response. Malformed JSON, duplicate object keys,
// inline error envelopes, non-object functionCall arguments, or negative token
// counters return a descriptive error.
func DecodeResponse(body []byte) (*ir.Response, error) {
	var raw wireResponse
	if err := unmarshalJSONObject(body, &raw); err != nil {
		return nil, fmt.Errorf("decode generatecontent response: %w", err)
	}
	if se := decodeStreamError(raw.Error); se != nil {
		return nil, fmt.Errorf("decode generatecontent response: upstream error: %s", se.Message)
	}

	usage, err := decodeGeminiUsage(body, raw.UsageMetadata)
	if err != nil {
		return nil, fmt.Errorf("decode generatecontent response usage: %w", err)
	}

	topUnknown := cloneValueMap(raw.Unknown)

	parts := []ir.Part{}
	var rawFinishReason string
	hasToolCall := false

	if len(raw.Candidates) > 0 {
		cand := raw.Candidates[0]
		rawFinishReason = cand.FinishReason
		if len(cand.Unknown) > 0 {
			candExtra, err := marshalUnknownMap(cand.Unknown)
			if err != nil {
				return nil, err
			}
			topUnknown = insertRawMember(topUnknown, "candidate", candExtra)
		}

		if cand.Content != nil {
			role := strings.TrimSpace(cand.Content.Role)
			if role != "" && role != geminiRoleModel {
				return nil, fmt.Errorf("decode generatecontent response: candidate content.role = %q, want %q", cand.Content.Role, geminiRoleModel)
			}
			decodedParts, sawCall, err := decodeBufferedCandidateParts(cand.Content.Parts)
			if err != nil {
				return nil, err
			}
			parts = decodedParts
			hasToolCall = sawCall
		}

		var gs groundingState
		gs.observe(cand.GroundingMetadata)
		footer, anns := gs.format()
		if footer != "" || len(anns) > 0 {
			if len(parts) > 0 && parts[len(parts)-1].Kind == ir.PartText {
				last := &parts[len(parts)-1]
				last.Text += footer
				last.Annotations = append(last.Annotations, anns...)
			} else {
				parts = append(parts, ir.Part{
					Kind:        ir.PartText,
					Text:        footer,
					Annotations: anns,
				})
			}
		}
	}

	resp := &ir.Response{
		ID:     raw.ResponseID,
		Model:  raw.ModelVersion,
		Parts:  parts,
		Finish: decodeFinishReason(rawFinishReason, hasToolCall),
		Usage:  usage,
		Extra:  toExtra(topUnknown),
	}
	if err := ir.ValidateResponse(resp); err != nil {
		return nil, fmt.Errorf("validate decoded generatecontent response: %w", err)
	}
	return resp, nil
}

// decodeBufferedCandidateParts decodes candidates[0].content.parts on a
// buffered response into ordered IR parts and reports whether any functionCall
// part appeared. Split functionCall entries sharing a non-empty explicit ID
// merge in place, whereas omitted wire IDs remain empty on Part.Call.CallID.
func decodeBufferedCandidateParts(rawParts []jsontext.Value) ([]ir.Part, bool, error) {
	parts := []ir.Part{}
	var toolSlots []*bufferedToolSlot
	toolsByID := make(map[string]*bufferedToolSlot)
	hasToolCall := false

	for pIdx, rawPart := range rawParts {
		trimmed := bytes.TrimSpace(rawPart)
		var wp wirePart
		if err := unmarshalJSONObject(trimmed, &wp); err != nil {
			return nil, false, fmt.Errorf("candidates[0].content.parts[%d]: %w", pIdx, err)
		}

		if wp.FunctionCall != nil && wp.Text == nil && wp.InlineData == nil && wp.FileData == nil && wp.FunctionResponse == nil && !wp.Thought {
			if err := collectBufferedFunctionCall(wp, &parts, &toolSlots, toolsByID); err != nil {
				return nil, false, fmt.Errorf("candidates[0].content.parts[%d]: %w", pIdx, err)
			}
			hasToolCall = true
			continue
		}

		if wp.Text == nil && wp.InlineData == nil && wp.FileData == nil && wp.FunctionCall == nil && wp.FunctionResponse == nil &&
			!wp.Thought && wp.ThoughtSignature != "" && len(wp.Unknown) == 0 {
			// Attach bare trailing signature-only parts to an open unsigned text block
			// when immediately preceded by one; otherwise promote to a signed thought
			// part so DecodeResponse stays aligned with StreamDecoder under Law S2.
			if len(parts) > 0 && parts[len(parts)-1].Kind == ir.PartText &&
				parts[len(parts)-1].Token == nil &&
				len(parts[len(parts)-1].Annotations) == 0 &&
				parts[len(parts)-1].Extra.IsZero() {
				parts[len(parts)-1].Token = &ir.ReplayToken{Data: wp.ThoughtSignature}
				continue
			}
			wp.Thought = true
		}

		p, err := decodeNonToolResponsePart(trimmed, wp)
		if err != nil {
			return nil, false, fmt.Errorf("candidates[0].content.parts[%d]: %w", pIdx, err)
		}
		parts = appendCoalescedResponsePart(parts, p)
	}

	for _, slot := range toolSlots {
		finalArgs := slot.rec.args
		if strings.TrimSpace(finalArgs) == "" {
			finalArgs = "{}"
		} else if !isValidSingleJSONObject([]byte(finalArgs)) {
			return nil, false, errors.New("buffered functionCall.args must assemble a valid JSON object")
		} else {
			finalArgs = string(compactRawJSON(jsontext.Value(finalArgs)))
		}
		var tok *ir.ReplayToken
		if slot.rec.sig != "" {
			tok = &ir.ReplayToken{Data: slot.rec.sig}
		}
		unknown, err := mergeNestedPartUnknown(slot.rec.partUnknown, "", "functionCall", slot.rec.callUnknown)
		if err != nil {
			return nil, false, err
		}
		parts[slot.partIndex] = ir.Part{
			Kind: ir.PartToolCall,
			Call: &ir.ToolCall{
				CallID:    slot.rec.callID,
				Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: slot.rec.name},
				Arguments: finalArgs,
				Token:     tok,
			},
			Extra: toExtra(unknown),
		}
	}

	return parts, hasToolCall, nil
}

// collectBufferedFunctionCall records a new bufferedToolSlot in parts or merges
// string argument fragments and late thoughtSignature values into an existing
// slot matching fc.ID. Empty functionCall names or malformed non-string/non-object
// argument tokens return an error immediately.
func collectBufferedFunctionCall(
	wp wirePart,
	parts *[]ir.Part,
	toolSlots *[]*bufferedToolSlot,
	toolsByID map[string]*bufferedToolSlot,
) error {
	fc := wp.FunctionCall
	if strings.TrimSpace(fc.Name) == "" {
		return errors.New("functionCall requires non-empty name")
	}
	argChunk, structured, err := extractStreamCallArgs(fc.Args)
	if err != nil {
		return err
	}

	// Collected OAuth response parts can revisit an explicit call ID across
	// interleaved tool parts (4b05d79a5); merge fragments and signatures onto the original slot.
	if fc.ID != "" {
		if prior, exists := toolsByID[fc.ID]; exists {
			_ = appendActiveToolArgs(&prior.rec, argChunk, structured, wp.ThoughtSignature)
			if len(wp.Unknown) > 0 && len(prior.rec.partUnknown) == 0 {
				prior.rec.partUnknown = cloneValueMap(wp.Unknown)
			}
			if len(fc.Unknown) > 0 && len(prior.rec.callUnknown) == 0 {
				prior.rec.callUnknown = cloneValueMap(fc.Unknown)
			}
			return nil
		}
	}

	slot := &bufferedToolSlot{
		partIndex: len(*parts),
		rec: toolRecord{
			rawID:       fc.ID,
			callID:      fc.ID,
			name:        fc.Name,
			partUnknown: cloneValueMap(wp.Unknown),
			callUnknown: cloneValueMap(fc.Unknown),
		},
	}
	_ = appendActiveToolArgs(&slot.rec, argChunk, structured, wp.ThoughtSignature)
	*parts = append(*parts, ir.Part{Kind: ir.PartToolCall})
	*toolSlots = append(*toolSlots, slot)
	if fc.ID != "" {
		toolsByID[fc.ID] = slot
	}
	return nil
}

// decodeNonToolResponsePart parses one non-functionCall Gemini Part inside
// candidates[0].content.parts into an IR Part. Parts carrying functionResponse
// or multiple union branches return an error, whereas signature-only
// thought=true objects with omitted text decode as empty-segment PartReasoning.
func decodeNonToolResponsePart(trimmed []byte, wp wirePart) (ir.Part, error) {
	branches := 0
	if wp.Text != nil {
		branches++
	}
	if wp.InlineData != nil {
		branches++
	}
	if wp.FileData != nil {
		branches++
	}
	if wp.FunctionCall != nil {
		branches++
	}
	if wp.FunctionResponse != nil {
		return ir.Part{}, errors.New("functionResponse is not allowed in model response output")
	}
	if branches > 1 {
		return ir.Part{}, errors.New("response part must not set multiple content union branches")
	}

	if branches == 0 {
		if wp.Thought {
			var tok *ir.ReplayToken
			if wp.ThoughtSignature != "" {
				tok = &ir.ReplayToken{Data: wp.ThoughtSignature}
			}
			return ir.Part{
				Kind: ir.PartReasoning,
				Reasoning: &ir.Reasoning{
					Summary: true,
					Token:   tok,
				},
				Extra: toExtra(wp.Unknown),
			}, nil
		}
		p, _, err := decodeOpaquePart(trimmed, wp.Unknown)
		return p, err
	}

	switch {
	case wp.Text != nil:
		p, _, err := decodeTextOrThoughtPart(wp, geminiRoleModel)
		return p, err
	case wp.InlineData != nil:
		return decodeInlineDataPart(wp)
	case wp.FileData != nil:
		return decodeFileDataPart(wp)
	default:
		return ir.Part{}, errors.New("unreachable non-tool response part branch")
	}
}

// appendCoalescedResponsePart merges adjacent plain text parts or adjacent
// unsigned-to-signed thought text parts when neither carries conflicting Extra
// metadata or signatures, returning the updated slice.
//
// Gemini SSE streams emit incremental text and thought fragments as separate
// single-part frames without block start/stop delimiters. Coalescing matching
// adjacent text or thought parts only while the leading part has no signature or
// vendor Extra keeps DecodeResponse aligned with StreamDecoder under Law S2.
func appendCoalescedResponsePart(dst []ir.Part, next ir.Part) []ir.Part {
	if len(dst) == 0 {
		return append(dst, next)
	}
	prev := &dst[len(dst)-1]
	if prev.Kind == ir.PartText && next.Kind == ir.PartText &&
		len(prev.Annotations) == 0 && len(next.Annotations) == 0 &&
		prev.Token == nil &&
		prev.Extra.IsZero() && next.Extra.IsZero() {
		prev.Text += next.Text
		prev.Token = next.Token
		return dst
	}
	if prev.Kind == ir.PartReasoning && next.Kind == ir.PartReasoning &&
		prev.Reasoning != nil && next.Reasoning != nil &&
		!prev.Reasoning.Redacted && !next.Reasoning.Redacted &&
		prev.Reasoning.Token == nil &&
		prev.Extra.IsZero() && next.Extra.IsZero() {
		joined := strings.Join(prev.Reasoning.Segments, "") + strings.Join(next.Reasoning.Segments, "")
		if joined != "" {
			prev.Reasoning.Segments = []string{joined}
		}
		prev.Reasoning.Summary = prev.Reasoning.Summary || next.Reasoning.Summary
		prev.Reasoning.Token = next.Reasoning.Token
		return dst
	}
	return append(dst, next)
}

// EncodeResponse serializes resp into deterministic Gemini GenerateContentResponse
// JSON bytes using view for model presentation and fallback response IDs,
// returning all incurred information-loss records. Nil responses return an error.
func EncodeResponse(resp *ir.Response, view ir.ClientView) ([]byte, []ir.Loss, error) {
	if resp == nil {
		return nil, nil, errors.New("encode generatecontent response: nil response")
	}

	var losses []ir.Loss
	respID := resp.ID
	if respID == "" {
		respID = view.Runtime.NextID("gem_", "gem_0")
	}
	model := resolveClientModel(view, resp.Model)

	if resp.ServiceTier != "" {
		losses = append(losses, ir.Loss{
			Kind:   ir.LossExtraDropped,
			Path:   "service_tier",
			Detail: resp.ServiceTier,
		})
	}

	topUnknown := restoreOrDropExtra(resp.Extra, "", &losses)
	candUnknown, _ := extractSubObjectUnknown(topUnknown, "candidate", "index", "content", "finishReason", "groundingMetadata")
	for _, k := range []string{"responseId", "modelVersion", "candidates", "usageMetadata", "error", "candidate"} {
		delete(topUnknown, k)
	}

	partsOut := make([]jsontext.Value, 0, len(resp.Parts))

	for idx, p := range resp.Parts {
		pPath := fmt.Sprintf("parts[%d]", idx)
		encodedPart, keep, err := encodeSingleResponsePart(p, pPath, &losses)
		if err != nil {
			return nil, nil, err
		}
		if keep {
			partsOut = append(partsOut, encodedPart)
		}
	}

	finishStr := encodeFinishReason(resp.Finish, &losses)
	cand := wireCandidateWrite{
		Index: 0,
		Content: &wireContent{
			Role:  geminiRoleModel,
			Parts: partsOut,
		},
		FinishReason: finishStr,
		Unknown:      candUnknown,
	}

	out := wireResponseWrite{
		ResponseID:    respID,
		ModelVersion:  model,
		Candidates:    []wireCandidateWrite{cand},
		UsageMetadata: encodeGeminiUsage(resp.Usage),
		Unknown:       topUnknown,
	}
	encoded, err := marshalDeterministic(out)
	if err != nil {
		return nil, nil, err
	}
	return encoded, losses, nil
}

// encodeSingleResponsePart serializes one IR response Part into a Gemini wirePart
// JSON object and reports whether a wire part was emitted alongside any Losses.
//
// ReplayToken.Data writes directly into thoughtSignature without inspecting
// Token.Scheme or carrier envelopes so convert remains the sole owner of token
// classification. Refusal parts inline as visible text with LossRefusalInlined,
// structured text annotations drop with LossAnnotationDropped because grounding
// footers already live in Text, and tokenless redacted thinking blocks drop with
// LossReasoningTextDropped in exact parity with StreamEncoder.
func encodeSingleResponsePart(
	p ir.Part,
	pPath string,
	losses *[]ir.Loss,
) (jsontext.Value, bool, error) {
	switch p.Kind {
	case ir.PartText, ir.PartRefusal:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if len(p.Annotations) > 0 {
			*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: pPath + ".annotations"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		unknown := restoreOrDropExtra(p.Extra, pPath, losses)
		sig := rawReplayTokenData(p.Token)
		if rawSig, ok := unknown["thoughtSignature"]; ok {
			if decodedSig, isStr := tryDecodeJSONString(rawSig); isStr && sig == "" {
				sig = decodedSig
			}
			delete(unknown, "thoughtSignature")
		}
		stripModeledPartKeys(unknown)
		if p.Text == "" && sig == "" && len(unknown) == 0 {
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossClientMarkerDropped,
				Path:   pPath,
				Detail: "empty_text",
			})
			return nil, false, nil
		}
		if p.Kind == ir.PartRefusal {
			*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: pPath})
		}
		txt := p.Text
		raw, err := marshalDeterministic(wirePart{
			Text:             &txt,
			ThoughtSignature: sig,
			Unknown:          unknown,
		})
		if err != nil {
			return nil, false, err
		}
		return jsontext.Value(raw), true, nil

	case ir.PartReasoning:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		unknown := restoreOrDropExtra(p.Extra, pPath, losses)
		stripModeledPartKeys(unknown)
		if p.Reasoning == nil {
			return nil, false, nil
		}
		joined := strings.Join(p.Reasoning.Segments, "")
		sig := rawReplayTokenData(p.Reasoning.Token)
		if p.Reasoning.Redacted {
			if sig == "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
				return nil, false, nil
			}
			if joined != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
			}
			empty := ""
			raw, err := marshalDeterministic(wirePart{
				Text:             &empty,
				Thought:          true,
				ThoughtSignature: sig,
				Unknown:          unknown,
			})
			if err != nil {
				return nil, false, err
			}
			return jsontext.Value(raw), true, nil
		}
		if joined == "" && sig == "" && len(unknown) == 0 {
			return nil, false, nil
		}
		raw, err := marshalDeterministic(wirePart{
			Text:             &joined,
			Thought:          true,
			ThoughtSignature: sig,
			Unknown:          unknown,
		})
		if err != nil {
			return nil, false, err
		}
		return jsontext.Value(raw), true, nil

	case ir.PartToolCall:
		if p.Call == nil {
			return nil, false, nil
		}
		if p.Call.Tool.Kind == ir.ToolHosted {
			if p.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
			}
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, losses)
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossOpaqueItemDropped,
				Path:   pPath,
				Detail: p.Call.Tool.Name,
			})
			return nil, false, nil
		}
		fnName := resolveGeminiCallableName(p.Call.Tool, defaultGeminiToolNameMaxBytes, pPath, losses)
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		sig := rawReplayTokenData(p.Call.Token)
		partUnknown := restoreOrDropExtra(p.Extra, pPath, losses)
		callUnknown, _ := extractSubObjectUnknown(partUnknown, "functionCall", "id", "name", "args")
		delete(partUnknown, "functionCall")
		stripModeledPartKeys(partUnknown)

		argsVal, ok, err := buildGeminiCallArgs(*p.Call)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			if p.Call.Incomplete {
				// Substitute an empty JSON object when serializing an interrupted tool call
				// on an abnormal response so wire functionCall.args stays object-typed.
				argsVal = jsontext.Value("{}")
			} else {
				return nil, false, fmt.Errorf("%s: tool_call arguments must be a valid JSON object", pPath)
			}
		}

		raw, err := marshalDeterministic(wirePart{
			ThoughtSignature: sig,
			FunctionCall: &wireFunctionCall{
				ID:      p.Call.CallID,
				Name:    fnName,
				Args:    argsVal,
				Unknown: callUnknown,
			},
			Unknown: partUnknown,
		})
		if err != nil {
			return nil, false, err
		}
		return jsontext.Value(raw), true, nil

	case ir.PartMedia:
		outPart, ok := encodeMediaPart(p, pPath, losses)
		if !ok {
			return nil, false, nil
		}
		raw, err := marshalDeterministic(outPart.part)
		if err != nil {
			return nil, false, err
		}
		return jsontext.Value(raw), true, nil

	case ir.PartOpaque:
		if p.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		if p.Opaque != nil && p.Opaque.Origin == protocol.ProtocolGeminiGenerateContent {
			return compactRawJSON(p.Opaque.Raw), true, nil
		}
		detail := ""
		if p.Opaque != nil {
			detail = p.Opaque.Type
		}
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOpaqueItemDropped,
			Path:   pPath,
			Detail: detail,
		})
		return nil, false, nil

	default:
		return nil, false, nil
	}
}
