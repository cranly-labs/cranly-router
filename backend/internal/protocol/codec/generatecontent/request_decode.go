package generatecontent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// request_decode.go decodes Google Gemini GenerateContent JSON request bodies
// into validated IR Request structures. Gemini embeds tool execution outputs as
// functionResponse parts inside user contents turns and allows functionCall and
// functionResponse parts to omit explicit call identifiers, whereas IR places
// tool results in dedicated RoleTool turns correlated by non-empty CallID strings.
//
// Decoding splits mixed user turns into contiguous RoleTool and RoleUser message
// runs so same-protocol round trips reconstruct the exact wire part order.
// Omitted functionCall IDs receive deterministic SHA-256 identifiers derived
// from model-turn ordinal, call ordinal within the model turn, function name,
// and canonical JSON arguments, and subsequent nameless functionResponse parts
// claim matching pending calls in turn order before falling back to content hashes.

// pendingNamelessCall tracks an unclaimed nameless model functionCall awaiting
// correlation with a subsequent user functionResponse carrying the same Name.
type pendingNamelessCall struct {
	name    string // FunctionDeclaration name invoked by the call
	callID  string // deterministic gemcall_ identifier assigned to the call
	claimed bool   // whether an incoming functionResponse already bound to callID
}

// decodedGenConfig bundles the IR fields extracted from generationConfig.
type decodedGenConfig struct {
	sampling        ir.Sampling         // decoded temperature, topP, topK, seed, penalties, candidateCount
	maxOutputTokens *int64              // decoded positive maxOutputTokens cap
	stop            []string            // decoded stopSequences slice
	reasoning       *ir.ReasoningConfig // decoded thinkingConfig settings
	output          *ir.OutputFormat    // decoded responseMimeType and schema settings
	extra           jsontext.Value      // preserved unmodeled generationConfig members or "{}"
}

// DecodeRequest parses a Google Gemini GenerateContent JSON request body into a
// validated ir.Request. Malformed JSON, duplicate object member names, missing
// or empty contents arrays, invalid generationConfig bounds, non-object tool
// schemas or arguments, or IR structural violations return a descriptive error.
func DecodeRequest(body []byte) (*ir.Request, error) {
	var raw wireInboundRequest
	if err := unmarshalJSONObject(body, &raw); err != nil {
		return nil, fmt.Errorf("decode generatecontent request: %w", err)
	}
	if raw.Contents == nil || len(*raw.Contents) == 0 {
		return nil, errors.New("decode generatecontent request: contents must be a non-empty array")
	}

	topUnknown := cloneValueMap(raw.Unknown)

	sysParts, sysExtra, err := decodeSystemInstruction(raw.SystemInstruction)
	if err != nil {
		return nil, err
	}
	if len(sysExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "systemInstruction", sysExtra)
	}

	msgs, err := decodeRequestContents(*raw.Contents)
	if err != nil {
		return nil, err
	}

	tools, err := decodeGeminiTools(raw.Tools)
	if err != nil {
		return nil, err
	}

	toolChoice, tcExtra, err := decodeToolConfigField(raw.ToolConfig, hasMixedDecodedTools(tools))
	if err != nil {
		return nil, err
	}
	if len(tcExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "toolConfig", tcExtra)
	}

	genDecoded, err := decodeGenerationConfigField(raw.GenerationConfig)
	if err != nil {
		return nil, err
	}
	if len(genDecoded.extra) > 0 {
		topUnknown = insertRawMember(topUnknown, "generationConfig", genDecoded.extra)
	}

	req := &ir.Request{
		System:          sysParts,
		Messages:        msgs,
		Tools:           tools,
		ToolChoice:      toolChoice,
		Sampling:        genDecoded.sampling,
		MaxOutputTokens: genDecoded.maxOutputTokens,
		Stop:            genDecoded.stop,
		Reasoning:       genDecoded.reasoning,
		Output:          genDecoded.output,
		Extra:           toExtra(topUnknown),
	}
	if err := ir.Validate(req); err != nil {
		return nil, fmt.Errorf("validate decoded generatecontent request: %w", err)
	}
	return req, nil
}

// decodeSystemInstruction parses top-level systemInstruction into System parts
// alongside any unmodeled systemInstruction fields (such as "role":"user").
// Absent or null values return nil slices, while reasoning or tool parts inside
// systemInstruction return an error so ir.Validate system rules hold.
func decodeSystemInstruction(raw jsontext.Value) ([]ir.Part, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil, nil
	}
	var sys wireSystemInstruction
	if err := unmarshalJSONObject(trimmed, &sys); err != nil {
		return nil, nil, fmt.Errorf("decode systemInstruction: %w", err)
	}
	if len(sys.Parts) == 0 {
		return nil, nil, errors.New("decode systemInstruction: parts must be a non-empty array")
	}

	parts := make([]ir.Part, 0, len(sys.Parts))
	for i, rawPart := range sys.Parts {
		p, err := decodeSystemPart(rawPart)
		if err != nil {
			return nil, nil, fmt.Errorf("systemInstruction.parts[%d]: %w", i, err)
		}
		parts = append(parts, p)
	}

	extraRaw, err := marshalUnknownMap(sys.Unknown)
	if err != nil {
		return nil, nil, fmt.Errorf("decode systemInstruction extra: %w", err)
	}
	return parts, extraRaw, nil
}

// decodeSystemPart parses one Part inside systemInstruction into an IR system
// Part. Only text, inlineData, fileData, and opaque vendor branches are
// permitted; thought, functionCall, and functionResponse parts return an error.
func decodeSystemPart(raw jsontext.Value) (ir.Part, error) {
	p, isToolResult, err := decodeContentPart(raw, geminiRoleUser, 0, 0, nil)
	if err != nil {
		return ir.Part{}, err
	}
	if isToolResult || p.Kind == ir.PartReasoning || p.Kind == ir.PartToolCall || p.Kind == ir.PartToolResult {
		return ir.Part{}, fmt.Errorf("%s part is not allowed inside systemInstruction", p.Kind)
	}
	return p, nil
}

// decodeRequestContents parses the top-level contents array into ordered IR
// Messages while validating speaker roles and non-empty part slices. Omitted
// content.role strings default to "user", whereas roles other than "user" and
// "model" or functionResponse parts inside model turns return an error.
//
// Each model turn increments modelStepOrdinal and collects derived gemcall_
// entries in pendingCalls so nameless functionResponse parts in the immediately
// following user turn correlate in invocation order by function name. Mixed
// user turns containing both functionResponse and ordinary user parts split into
// contiguous RoleTool and RoleUser messages via splitDecodedUserTurn.
func decodeRequestContents(rawContents []jsontext.Value) ([]ir.Message, error) {
	var messages []ir.Message
	var pendingCalls []pendingNamelessCall
	modelStepOrdinal := 0

	for cIdx, rawTurn := range rawContents {
		var turn wireInboundContent
		if err := unmarshalJSONObject(rawTurn, &turn); err != nil {
			return nil, fmt.Errorf("contents[%d]: %w", cIdx, err)
		}
		role := strings.TrimSpace(turn.Role)
		// Default omitted content.role values to "user" when no functionCall is
		// present; explicit roles other than "user" and "model" fail immediately.
		if role == "" {
			role = geminiRoleUser
		}
		if role != geminiRoleUser && role != geminiRoleModel {
			return nil, fmt.Errorf("contents[%d].role: unsupported role %q", cIdx, turn.Role)
		}
		if turn.Parts == nil || len(*turn.Parts) == 0 {
			return nil, fmt.Errorf("contents[%d].parts: must be a non-empty array", cIdx)
		}

		turnExtra := toExtra(turn.Unknown)

		if role == geminiRoleModel {
			modelStepOrdinal++
			pendingCalls = nil
			callOrdinal := 0
			modelParts := make([]ir.Part, 0, len(*turn.Parts))
			for pIdx, rawPart := range *turn.Parts {
				p, isToolResult, err := decodeContentPart(rawPart, geminiRoleModel, modelStepOrdinal, callOrdinal, nil)
				if err != nil {
					return nil, fmt.Errorf("contents[%d].parts[%d]: %w", cIdx, pIdx, err)
				}
				if isToolResult {
					return nil, fmt.Errorf("contents[%d].parts[%d]: functionResponse is not allowed in model turn", cIdx, pIdx)
				}
				if p.Kind == ir.PartToolCall && p.Call != nil {
					callOrdinal++
					if strings.HasPrefix(p.Call.CallID, namelessCallIDPrefix) {
						pendingCalls = append(pendingCalls, pendingNamelessCall{
							name:   p.Call.Tool.Name,
							callID: p.Call.CallID,
						})
					}
				}
				modelParts = append(modelParts, p)
			}
			messages = append(messages, ir.Message{
				Role:  ir.RoleAssistant,
				Parts: modelParts,
				Extra: turnExtra,
			})
			continue
		}

		// Split user turns into contiguous runs of RoleTool (functionResponse)
		// and RoleUser (text, media, opaque) parts while attaching Content.Extra
		// to the first emitted user run (or first tool run when tool-only).
		userMsgs, err := splitDecodedUserTurn(cIdx, *turn.Parts, turnExtra, &pendingCalls)
		if err != nil {
			return nil, err
		}
		messages = append(messages, userMsgs...)
		pendingCalls = nil
	}
	return messages, nil
}

// splitDecodedUserTurn partitions a Gemini "user" Content turn's parts into
// alternating contiguous RoleTool and RoleUser IR messages without reordering
// parts. Non-zero turnExtra attaches to the first emitted RoleUser run (or to
// runs[0] on tool-only turns) so same-protocol round trips restore Content.Extra.
func splitDecodedUserTurn(
	cIdx int,
	rawParts []jsontext.Value,
	turnExtra ir.Extra,
	pendingCalls *[]pendingNamelessCall,
) ([]ir.Message, error) {
	var runs []ir.Message
	resultOrdinal := 0

	for pIdx, rawPart := range rawParts {
		p, isToolResult, err := decodeContentPart(rawPart, geminiRoleUser, cIdx, resultOrdinal, pendingCalls)
		if err != nil {
			return nil, fmt.Errorf("contents[%d].parts[%d]: %w", cIdx, pIdx, err)
		}
		targetRole := ir.RoleUser
		if isToolResult {
			resultOrdinal++
			targetRole = ir.RoleTool
		}
		if len(runs) > 0 && runs[len(runs)-1].Role == targetRole {
			runs[len(runs)-1].Parts = append(runs[len(runs)-1].Parts, p)
		} else {
			runs = append(runs, ir.Message{
				Role:  targetRole,
				Parts: []ir.Part{p},
			})
		}
	}

	if !turnExtra.IsZero() && len(runs) > 0 {
		attached := false
		for i := range runs {
			if runs[i].Role == ir.RoleUser {
				runs[i].Extra = turnExtra
				attached = true
				break
			}
		}
		if !attached {
			runs[0].Extra = turnExtra
		}
	}
	return runs, nil
}

// decodeContentPart parses one Gemini Part JSON object inside contents[turnOrd].
// It enforces mutual exclusion across modeled Part union branches and falls
// back to PartOpaque for unmodeled Gemini branches such as executableCode.
func decodeContentPart(
	raw jsontext.Value,
	wireRole string,
	turnOrd int,
	itemOrd int,
	pendingCalls *[]pendingNamelessCall,
) (ir.Part, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	var wp wirePart
	if err := unmarshalJSONObject(trimmed, &wp); err != nil {
		return ir.Part{}, false, err
	}

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
		branches++
	}
	if branches > 1 {
		return ir.Part{}, false, errors.New("part must not set multiple content union branches")
	}

	if branches == 0 {
		if wp.Thought {
			// A thought:true part with omitted text represents a signature-only or
			// empty thought block on model turns.
			if wireRole != geminiRoleModel {
				return ir.Part{}, false, errors.New("thought part is only allowed in model turns")
			}
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
			}, false, nil
		}
		return decodeOpaquePart(trimmed, wp.Unknown)
	}

	switch {
	case wp.Text != nil:
		return decodeTextOrThoughtPart(wp, wireRole)
	case wp.InlineData != nil:
		p, err := decodeInlineDataPart(wp)
		return p, false, err
	case wp.FileData != nil:
		p, err := decodeFileDataPart(wp)
		return p, false, err
	case wp.FunctionCall != nil:
		if wireRole != geminiRoleModel {
			return ir.Part{}, false, errors.New("functionCall is only allowed in model turns")
		}
		p, err := decodeFunctionCallPart(wp, turnOrd, itemOrd)
		return p, false, err
	case wp.FunctionResponse != nil:
		if wireRole != geminiRoleUser {
			return ir.Part{}, false, errors.New("functionResponse is only allowed in user turns")
		}
		p, err := decodeFunctionResponsePart(wp, turnOrd, itemOrd, pendingCalls)
		return p, true, err
	default:
		return ir.Part{}, false, errors.New("unreachable part branch")
	}
}

// decodeTextOrThoughtPart converts a Gemini text or thought-text wirePart into
// PartReasoning (when Thought is true) or PartText. Thought parts outside model
// turns return an error, assistant plain-text parts store wire thoughtSignature
// values inside Part.Token, and non-model plain-text parts retain unmodeled
// thoughtSignature fields inside Part.Extra for lossless same-protocol replay.
func decodeTextOrThoughtPart(wp wirePart, wireRole string) (ir.Part, bool, error) {
	txt := *wp.Text
	if wp.Thought {
		if wireRole != geminiRoleModel {
			return ir.Part{}, false, errors.New("thought part is only allowed in model turns")
		}
		var segs []string
		if txt != "" {
			segs = []string{txt}
		}
		var tok *ir.ReplayToken
		if wp.ThoughtSignature != "" {
			tok = &ir.ReplayToken{Data: wp.ThoughtSignature}
		}
		return ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Segments: segs,
				Summary:  true,
				Token:    tok,
			},
			Extra: toExtra(wp.Unknown),
		}, false, nil
	}

	if wireRole == geminiRoleModel {
		var tok *ir.ReplayToken
		if wp.ThoughtSignature != "" {
			tok = &ir.ReplayToken{Data: wp.ThoughtSignature}
		}
		return ir.Part{
			Kind:  ir.PartText,
			Text:  txt,
			Token: tok,
			Extra: toExtra(wp.Unknown),
		}, false, nil
	}

	unknown := cloneValueMap(wp.Unknown)
	// Non-model text parts cannot carry assistant ReplayTokens; retain any wire
	// thoughtSignature inside Part.Extra so same-protocol round trips keep it verbatim.
	if wp.ThoughtSignature != "" {
		sigRaw, err := marshalDeterministic(wp.ThoughtSignature)
		if err != nil {
			return ir.Part{}, false, err
		}
		unknown = insertRawMember(unknown, "thoughtSignature", jsontext.Value(sigRaw))
	}
	return ir.Part{
		Kind:  ir.PartText,
		Text:  txt,
		Extra: toExtra(unknown),
	}, false, nil
}

// decodeInlineDataPart converts a Gemini inlineData wirePart into a base64
// PartMedia and preserves nested inlineData vendor extensions inside Part.Extra.
// Setting thought=true or leaving mimeType or data blank returns an error.
func decodeInlineDataPart(wp wirePart) (ir.Part, error) {
	if wp.Thought {
		return ir.Part{}, errors.New("inlineData part cannot set thought=true")
	}
	mime := strings.TrimSpace(wp.InlineData.MimeType)
	if mime == "" {
		return ir.Part{}, errors.New("inlineData requires non-empty mimeType")
	}
	if strings.TrimSpace(wp.InlineData.Data) == "" {
		return ir.Part{}, errors.New("inlineData requires non-empty data")
	}
	unknown, err := mergeNestedPartUnknown(wp.Unknown, wp.ThoughtSignature, "inlineData", wp.InlineData.Unknown)
	if err != nil {
		return ir.Part{}, err
	}
	return ir.Part{
		Kind: ir.PartMedia,
		Media: &ir.Media{
			Type:   classifyMimeModality(mime, ""),
			MIME:   mime,
			Source: ir.MediaSourceBase64,
			Data:   wp.InlineData.Data,
		},
		Extra: toExtra(unknown),
	}, nil
}

// decodeFileDataPart converts a Gemini fileData wirePart into a URL or file_id
// PartMedia and preserves nested fileData vendor extensions inside Part.Extra.
// Setting thought=true or leaving fileUri blank returns an error, while omitted
// mimeType values infer modality from fileUri suffixes.
func decodeFileDataPart(wp wirePart) (ir.Part, error) {
	if wp.Thought {
		return ir.Part{}, errors.New("fileData part cannot set thought=true")
	}
	uri := wp.FileData.FileURI
	if strings.TrimSpace(uri) == "" {
		return ir.Part{}, errors.New("fileData requires non-empty fileUri")
	}
	mime := strings.TrimSpace(wp.FileData.MimeType)
	srcKind := classifyFileURISource(uri)
	unknown, err := mergeNestedPartUnknown(wp.Unknown, wp.ThoughtSignature, "fileData", wp.FileData.Unknown)
	if err != nil {
		return ir.Part{}, err
	}
	return ir.Part{
		Kind: ir.PartMedia,
		Media: &ir.Media{
			Type:   classifyMimeModality(mime, uri),
			MIME:   mime,
			Source: srcKind,
			Data:   uri,
		},
		Extra: toExtra(unknown),
	}, nil
}

// classifyFileURISource maps HTTP, HTTPS, Cloud Storage ("gs://"), and inline
// data URIs onto MediaSourceURL. Bare Gemini Files API resource identifiers
// without those URI prefixes fall back to MediaSourceFileID.
func classifyFileURISource(uri string) string {
	lower := strings.ToLower(strings.TrimSpace(uri))
	if strings.HasPrefix(lower, "http://") ||
		strings.HasPrefix(lower, "https://") ||
		strings.HasPrefix(lower, "gs://") ||
		strings.HasPrefix(lower, "data:") {
		return ir.MediaSourceURL
	}
	return ir.MediaSourceFileID
}

// classifyMimeModality selects MediaImage, MediaAudio, MediaVideo, or
// MediaDocument from mime when non-empty. Omitted mime strings inspect known
// file extensions on uri and default to MediaDocument when unrecognized.
func classifyMimeModality(mime, uri string) ir.MediaType {
	lower := strings.ToLower(strings.TrimSpace(mime))
	switch {
	case strings.HasPrefix(lower, "image/"):
		return ir.MediaImage
	case strings.HasPrefix(lower, "audio/"):
		return ir.MediaAudio
	case strings.HasPrefix(lower, "video/"):
		return ir.MediaVideo
	case lower != "":
		return ir.MediaDocument
	}
	lowerURI := strings.ToLower(strings.TrimSpace(uri))
	switch {
	case strings.HasSuffix(lowerURI, ".png"),
		strings.HasSuffix(lowerURI, ".jpg"),
		strings.HasSuffix(lowerURI, ".jpeg"),
		strings.HasSuffix(lowerURI, ".gif"),
		strings.HasSuffix(lowerURI, ".webp"):
		return ir.MediaImage
	case strings.HasSuffix(lowerURI, ".mp3"),
		strings.HasSuffix(lowerURI, ".wav"),
		strings.HasSuffix(lowerURI, ".ogg"):
		return ir.MediaAudio
	case strings.HasSuffix(lowerURI, ".mp4"),
		strings.HasSuffix(lowerURI, ".mov"),
		strings.HasSuffix(lowerURI, ".webm"):
		return ir.MediaVideo
	default:
		return ir.MediaDocument
	}
}

// decodeFunctionCallPart converts a Gemini functionCall wirePart into an IR
// PartToolCall and stores any wire thoughtSignature inside Call.Token.Data.
// Omitted wire IDs derive a deterministic gemcall_ CallID from modelStepOrd and
// callOrd, whereas blank names, thought=true, or non-object args return an error.
func decodeFunctionCallPart(wp wirePart, modelStepOrd, callOrd int) (ir.Part, error) {
	if wp.Thought {
		return ir.Part{}, errors.New("functionCall part cannot set thought=true")
	}
	fc := wp.FunctionCall
	if strings.TrimSpace(fc.Name) == "" {
		return ir.Part{}, errors.New("functionCall requires non-empty name")
	}
	argsStr, err := normalizeInboundCallArgs(fc.Args)
	if err != nil {
		return ir.Part{}, err
	}

	callID := fc.ID
	if callID == "" {
		callID = deriveNamelessCallID(modelStepOrd, callOrd, fc.Name, argsStr)
	}

	var tok *ir.ReplayToken
	if wp.ThoughtSignature != "" {
		tok = &ir.ReplayToken{Data: wp.ThoughtSignature}
	}

	unknown, err := mergeNestedPartUnknown(wp.Unknown, "", "functionCall", fc.Unknown)
	if err != nil {
		return ir.Part{}, err
	}

	return ir.Part{
		Kind: ir.PartToolCall,
		Call: &ir.ToolCall{
			CallID:    callID,
			Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: fc.Name},
			Arguments: argsStr,
			Token:     tok,
		},
		Extra: toExtra(unknown),
	}, nil
}

// normalizeInboundCallArgs validates rawArgs on an incoming request functionCall
// and returns its canonicalized JSON object string. Omitted or null rawArgs
// default to "{}", whereas arrays, scalars, or duplicate keys return an error.
func normalizeInboundCallArgs(rawArgs jsontext.Value) (string, error) {
	trimmed := bytes.TrimSpace(rawArgs)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "{}", nil
	}
	if !isValidSingleJSONObject(trimmed) {
		return "", errors.New("functionCall.args must be a JSON object")
	}
	return string(compactRawJSON(trimmed)), nil
}

// deriveNamelessCallID computes a deterministic gemcall_ identifier from the
// 1-based model step ordinal, 0-based call ordinal, function name, and
// key-sorted canonical JSON arguments. Canonicalizing object member order keeps
// derived IDs invariant to wire map serialization order while preserving Law Q2.
func deriveNamelessCallID(stepOrd, callOrd int, name, canonicalArgs string) string {
	normArgs := canonicalArgs
	var obj map[string]jsontext.Value
	if unmarshalJSONObject([]byte(canonicalArgs), &obj) == nil {
		if det, err := marshalDeterministic(obj); err == nil {
			normArgs = string(det)
		}
	}
	sum := sha256.Sum256(fmt.Appendf(nil, "call:%d:%d:%s:%s", stepOrd, callOrd, name, normArgs))
	return namelessCallIDPrefix + hex.EncodeToString(sum[:6])
}

// deriveOrphanResultCallID computes a deterministic gemcall_ identifier for a
// nameless functionResponse part that matched no unclaimed preceding model call.
// Hashing turnOrd, resOrd, name, and canonicalResp keeps orphan IDs distinct
// from paired call hashes and stable across repeated decodes.
func deriveOrphanResultCallID(turnOrd, resOrd int, name, canonicalResp string) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "resp:%d:%d:%s:%s", turnOrd, resOrd, name, canonicalResp))
	return namelessCallIDPrefix + hex.EncodeToString(sum[:6])
}

// decodeFunctionResponsePart converts a Gemini functionResponse wirePart into an
// IR PartToolResult while preserving outer and nested vendor extensions in Extra.
// Setting thought=true, leaving functionResponse.name blank, or supplying a
// non-object response payload returns an error.
//
// When functionResponse.id is omitted, decodeFunctionResponsePart claims the
// earliest unclaimed entry in pendingCalls with a matching function name so
// parallel nameless calls to the same tool pair in invocation order. Unmatched
// nameless responses fall back to deriveOrphanResultCallID and retain a direct
// Result.Tool reference, while unwrapFunctionResponseObject extracts plain-text
// and error wrappers.
func decodeFunctionResponsePart(
	wp wirePart,
	turnOrd int,
	resOrd int,
	pendingCalls *[]pendingNamelessCall,
) (ir.Part, error) {
	if wp.Thought {
		return ir.Part{}, errors.New("functionResponse part cannot set thought=true")
	}
	fr := wp.FunctionResponse
	if strings.TrimSpace(fr.Name) == "" {
		return ir.Part{}, errors.New("functionResponse requires non-empty name")
	}
	respTrim := bytes.TrimSpace(fr.Response)
	if !isValidSingleJSONObject(respTrim) {
		return ir.Part{}, errors.New("functionResponse.response must be a JSON object")
	}

	compactedResp := string(compactRawJSON(respTrim))
	callID := fr.ID
	matchedPending := false
	if callID == "" && pendingCalls != nil {
		for i := range *pendingCalls {
			pc := &(*pendingCalls)[i]
			if !pc.claimed && pc.name == fr.Name {
				pc.claimed = true
				callID = pc.callID
				matchedPending = true
				break
			}
		}
	}
	if callID == "" {
		callID = deriveOrphanResultCallID(turnOrd, resOrd, fr.Name, compactedResp)
	}

	contentParts, isErr := unwrapFunctionResponseObject(respTrim, compactedResp)

	unknown, err := mergeNestedPartUnknown(wp.Unknown, wp.ThoughtSignature, "functionResponse", fr.Unknown)
	if err != nil {
		return ir.Part{}, err
	}

	var fallbackTool ir.ToolRef
	// Populate Result.Tool directly so orphan functionResponse parts retain their
	// declared Gemini function name while ir.Link still overwrites matched calls.
	if !matchedPending {
		fallbackTool = ir.ToolRef{Kind: ir.ToolFunction, Name: fr.Name}
	}

	return ir.Part{
		Kind: ir.PartToolResult,
		Result: &ir.ToolResult{
			CallID:  callID,
			Tool:    fallbackTool,
			Content: contentParts,
			IsError: isErr,
		},
		Extra: toExtra(unknown),
	}, nil
}

// unwrapFunctionResponseObject extracts ToolResult.Content parts and IsError
// status from a validated functionResponse.response JSON object. Single-key
// {"error":"<string>"} (or {"error":"<string>","is_error":true}) envelopes return
// IsError=true text and {"content":"<string>"} or {"result":"<string>"} wrappers
// return IsError=false text, whereas structured multi-key objects remain intact
// as compacted JSON text.
func unwrapFunctionResponseObject(trimmed []byte, compacted string) ([]ir.Part, bool) {
	var rawMap map[string]jsontext.Value
	if unmarshalJSONObject(trimmed, &rawMap) == nil {
		if len(rawMap) == 1 {
			if rawErr, ok := rawMap["error"]; ok {
				if errText, isStr := tryDecodeJSONString(rawErr); isStr {
					return singleTextSlice(errText), true
				}
			}
			for _, wrapKey := range []string{"content", "result"} {
				if rawVal, ok := rawMap[wrapKey]; ok {
					if valText, isStr := tryDecodeJSONString(rawVal); isStr {
						return singleTextSlice(valText), false
					}
				}
			}
		} else if len(rawMap) == 2 {
			rawErr, hasErr := rawMap["error"]
			rawFlag, hasFlag := rawMap["is_error"]
			if hasErr && hasFlag && bytes.Equal(bytes.TrimSpace(rawFlag), []byte("true")) {
				if errText, isStr := tryDecodeJSONString(rawErr); isStr {
					return singleTextSlice(errText), true
				}
			}
		}
	}
	return []ir.Part{{Kind: ir.PartText, Text: compacted}}, false
}

// tryDecodeJSONString unmarshals raw as a JSON string literal and reports
// whether decoding succeeded. Empty slices, non-string JSON tokens, or invalid
// string escapes return ("", false) without raising an error.
func tryDecodeJSONString(raw jsontext.Value) (string, bool) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return "", false
	}
	var s string
	if unmarshalJSONValue(trimmed, &s) != nil {
		return "", false
	}
	return s, true
}

// singleTextSlice builds a non-nil ToolResult.Content slice from unwrapped text s.
// Empty strings return an allocated zero-length slice rather than a blank PartText
// so ir.Validate and re-encoding preserve empty tool outputs cleanly.
func singleTextSlice(s string) []ir.Part {
	if s == "" {
		return []ir.Part{}
	}
	return []ir.Part{{Kind: ir.PartText, Text: s}}
}

// mergeNestedPartUnknown clones partUnknown and inserts optional non-call
// thoughtSig strings plus serialized branchUnknown members under branchKey. When
// all inputs are empty, it returns (nil, nil) without allocating an Extra map.
func mergeNestedPartUnknown(
	partUnknown map[string]jsontext.Value,
	thoughtSig string,
	branchKey string,
	branchUnknown map[string]jsontext.Value,
) (map[string]jsontext.Value, error) {
	merged := cloneValueMap(partUnknown)
	if thoughtSig != "" {
		sigBytes, err := marshalDeterministic(thoughtSig)
		if err != nil {
			return nil, err
		}
		merged = insertRawMember(merged, "thoughtSignature", jsontext.Value(sigBytes))
	}
	if len(branchUnknown) > 0 {
		nestedBytes, err := marshalUnknownMap(branchUnknown)
		if err != nil {
			return nil, err
		}
		merged = insertRawMember(merged, branchKey, nestedBytes)
	}
	return merged, nil
}

// decodeOpaquePart wraps an unmodeled non-empty Gemini Part union object (such
// as executableCode or codeExecutionResult) into a PartOpaque item. Empty `{}`
// or whitespace-only branch keys return an error so OpaqueItem.Type is valid.
func decodeOpaquePart(trimmed []byte, unknown map[string]jsontext.Value) (ir.Part, bool, error) {
	if len(unknown) == 0 {
		return ir.Part{}, false, errors.New("empty Gemini part object has no union branch")
	}
	var keys []string
	for k := range unknown {
		if strings.TrimSpace(k) != "" {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return ir.Part{}, false, errors.New("opaque Gemini part requires a non-blank branch key")
	}
	slices.Sort(keys)
	typ := keys[0]
	for _, preferred := range []string{"executableCode", "codeExecutionResult", "videoMetadata"} {
		if _, ok := unknown[preferred]; ok {
			typ = preferred
			break
		}
	}
	return ir.Part{
		Kind: ir.PartOpaque,
		Opaque: &ir.OpaqueItem{
			Origin: protocol.ProtocolGeminiGenerateContent,
			Type:   typ,
			Raw:    compactRawJSON(trimmed),
		},
	}, false, nil
}

// decodeGeminiTools parses the top-level Gemini tools array into ordered IR
// function and hosted Tool declarations. Empty tool slices return (nil, nil),
// whereas empty tool objects, non-object built-in configs, or blank hosted keys
// return an error.
func decodeGeminiTools(rawTools []jsontext.Value) ([]ir.Tool, error) {
	if len(rawTools) == 0 {
		return nil, nil
	}
	var out []ir.Tool
	for tIdx, rawEntry := range rawTools {
		var entry wireToolEntry
		if err := unmarshalJSONObject(rawEntry, &entry); err != nil {
			return nil, fmt.Errorf("tools[%d]: %w", tIdx, err)
		}

		sawMember := false
		if entry.FunctionDeclarations != nil {
			sawMember = true
			for dIdx, rawDecl := range *entry.FunctionDeclarations {
				fnTool, err := decodeFunctionDeclaration(rawDecl)
				if err != nil {
					return nil, fmt.Errorf("tools[%d].functionDeclarations[%d]: %w", tIdx, dIdx, err)
				}
				out = append(out, fnTool)
			}
		}

		for _, builtIn := range []struct {
			key string         // canonical or snake_case wire tool member name
			val jsontext.Value // raw configuration JSON value
		}{
			{"googleSearch", entry.GoogleSearch},
			{"google_search", entry.GoogleSearchSnake},
			{"codeExecution", entry.CodeExecution},
			{"urlContext", entry.URLContext},
		} {
			valTrim := bytes.TrimSpace(builtIn.val)
			if len(valTrim) == 0 || bytes.Equal(valTrim, []byte("null")) {
				continue
			}
			if !isValidSingleJSONObject(valTrim) {
				return nil, fmt.Errorf("tools[%d].%s: must be a JSON object", tIdx, builtIn.key)
			}
			sawMember = true
			hostedRaw, err := marshalDeterministic(map[string]jsontext.Value{
				builtIn.key: compactRawJSON(valTrim),
			})
			if err != nil {
				return nil, err
			}
			out = append(out, ir.Tool{
				Kind: ir.ToolHosted,
				Name: builtIn.key,
				Hosted: &ir.OpaqueItem{
					Origin: protocol.ProtocolGeminiGenerateContent,
					Type:   builtIn.key,
					Raw:    jsontext.Value(hostedRaw),
				},
			})
		}

		if len(entry.Unknown) > 0 {
			sawMember = true
			keys := make([]string, 0, len(entry.Unknown))
			for k := range entry.Unknown {
				if strings.TrimSpace(k) == "" {
					return nil, fmt.Errorf("tools[%d]: hosted tool key must be non-blank", tIdx)
				}
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				singleRaw, err := marshalDeterministic(map[string]jsontext.Value{
					k: compactRawJSON(entry.Unknown[k]),
				})
				if err != nil {
					return nil, err
				}
				out = append(out, ir.Tool{
					Kind: ir.ToolHosted,
					Name: k,
					Hosted: &ir.OpaqueItem{
						Origin: protocol.ProtocolGeminiGenerateContent,
						Type:   k,
						Raw:    jsontext.Value(singleRaw),
					},
				})
			}
		}

		if !sawMember {
			return nil, fmt.Errorf("tools[%d]: empty tool declaration object", tIdx)
		}
	}
	return out, nil
}

// decodeFunctionDeclaration parses one Gemini FunctionDeclaration into an
// ir.Tool while preserving unmodeled fields inside Tool.Extra. Declarations
// must carry a non-empty name and at most one JSON-object schema across
// parameters and parametersJsonSchema, returning an error on violations.
func decodeFunctionDeclaration(raw jsontext.Value) (ir.Tool, error) {
	var decl wireFunctionDeclaration
	if err := unmarshalJSONObject(raw, &decl); err != nil {
		return ir.Tool{}, err
	}
	if strings.TrimSpace(decl.Name) == "" {
		return ir.Tool{}, errors.New("functionDeclaration requires non-empty name")
	}

	paramsTrim := bytes.TrimSpace(decl.Parameters)
	jsonSchemaTrim := bytes.TrimSpace(decl.ParametersJSONSchema)
	hasParams := len(paramsTrim) > 0 && !bytes.Equal(paramsTrim, []byte("null"))
	hasJSONSchema := len(jsonSchemaTrim) > 0 && !bytes.Equal(jsonSchemaTrim, []byte("null"))
	if hasParams && hasJSONSchema {
		return ir.Tool{}, errors.New("functionDeclaration cannot set both parameters and parametersJsonSchema")
	}

	var schema jsontext.Value
	unknown := cloneValueMap(decl.Unknown)
	if hasParams {
		if !isValidSingleJSONObject(paramsTrim) {
			return ir.Tool{}, errors.New("functionDeclaration.parameters must be a JSON object")
		}
		schema = compactRawJSON(paramsTrim)
	} else if hasJSONSchema {
		if !isValidSingleJSONObject(jsonSchemaTrim) {
			return ir.Tool{}, errors.New("functionDeclaration.parametersJsonSchema must be a JSON object")
		}
		schema = compactRawJSON(jsonSchemaTrim)
	}

	return ir.Tool{
		Kind:        ir.ToolFunction,
		Name:        decl.Name,
		Description: decl.Description,
		InputSchema: schema,
		Extra:       toExtra(unknown),
	}, nil
}

// hasMixedDecodedTools reports whether tools contains both callable function
// declarations and built-in googleSearch or codeExecution declarations.
func hasMixedDecodedTools(tools []ir.Tool) bool {
	hasFunc := false
	hasBuiltIn := false
	for _, t := range tools {
		if t.Kind == ir.ToolFunction {
			hasFunc = true
		} else if t.Kind == ir.ToolHosted && t.Hosted != nil {
			switch t.Hosted.Type {
			case "googleSearch", "google_search", "codeExecution":
				hasBuiltIn = true
			}
		}
	}
	return hasFunc && hasBuiltIn
}

// decodeToolConfigField parses the top-level Gemini toolConfig JSON value into
// an optional ir.ToolChoice and Extra bytes for topUnknown["toolConfig"].
// Omitted or null raw values return (nil, nil, nil), whereas non-object JSON,
// blank allowedFunctionNames, or unsupported modes return an error.
//
// Mode "VALIDATED" maps to ToolChoiceAuto while recording `"mode":"VALIDATED"`
// in Extra for same-protocol round trips, and mode "ANY" selects ToolChoiceTool
// when allowedFunctionNames names a single function or ToolChoiceRequired
// otherwise. An explicit includeServerSideToolInvocations=true is omitted from
// Extra when hasMixedTools already implies it, and empty `{}` objects return
// `jsontext.Value("{}")` so empty wrappers stay distinct from absent fields.
func decodeToolConfigField(raw jsontext.Value, hasMixedTools bool) (*ir.ToolChoice, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil, nil
	}
	var tc wireToolConfig
	if err := unmarshalJSONObject(trimmed, &tc); err != nil {
		return nil, nil, fmt.Errorf("decode toolConfig: %w", err)
	}

	tcUnknown := cloneValueMap(tc.Unknown)
	derivedServerSideTrue := hasMixedTools && tc.IncludeServerSideToolInvocations != nil && *tc.IncludeServerSideToolInvocations
	if tc.IncludeServerSideToolInvocations != nil && !derivedServerSideTrue {
		flagBytes, err := marshalDeterministic(*tc.IncludeServerSideToolInvocations)
		if err != nil {
			return nil, nil, err
		}
		tcUnknown = insertRawMember(tcUnknown, "includeServerSideToolInvocations", jsontext.Value(flagBytes))
	}

	var choice *ir.ToolChoice
	fccTrim := bytes.TrimSpace(tc.FunctionCallingConfig)
	if len(fccTrim) > 0 && !bytes.Equal(fccTrim, []byte("null")) {
		var fcc wireFunctionCallingConfig
		if err := unmarshalJSONObject(fccTrim, &fcc); err != nil {
			return nil, nil, fmt.Errorf("decode toolConfig.functionCallingConfig: %w", err)
		}
		fccUnknown := cloneValueMap(fcc.Unknown)

		allowed := make([]ir.ToolRef, 0, len(fcc.AllowedFunctionNames))
		for _, fn := range fcc.AllowedFunctionNames {
			if strings.TrimSpace(fn) == "" {
				return nil, nil, errors.New("allowedFunctionNames entries must be non-empty")
			}
			allowed = append(allowed, ir.ToolRef{Kind: ir.ToolFunction, Name: fn})
		}

		mode := strings.ToUpper(strings.TrimSpace(fcc.Mode))
		switch mode {
		case "":
			if len(allowed) > 0 {
				choice = &ir.ToolChoice{
					Mode:         ir.ToolChoiceAuto,
					AllowedTools: allowed,
				}
			} else if len(fccUnknown) == 0 {
				fccUnknown = insertRawMember(fccUnknown, "__empty", jsontext.Value("true"))
			}
		case "AUTO", "VALIDATED":
			if mode == "VALIDATED" {
				fccUnknown = insertRawMember(fccUnknown, "mode", jsontext.Value(`"VALIDATED"`))
			}
			choice = &ir.ToolChoice{
				Mode:         ir.ToolChoiceAuto,
				AllowedTools: allowed,
			}
		case "NONE":
			choice = &ir.ToolChoice{
				Mode:         ir.ToolChoiceNone,
				AllowedTools: allowed,
			}
		case "ANY":
			if len(allowed) == 1 {
				choice = &ir.ToolChoice{
					Mode: ir.ToolChoiceTool,
					Tool: allowed[0],
				}
			} else {
				choice = &ir.ToolChoice{
					Mode:         ir.ToolChoiceRequired,
					AllowedTools: allowed,
				}
			}
		default:
			return nil, nil, fmt.Errorf("unsupported functionCallingConfig.mode %q", fcc.Mode)
		}

		if _, hasEmpty := fccUnknown["__empty"]; hasEmpty {
			delete(fccUnknown, "__empty")
			if len(fccUnknown) == 0 {
				tcUnknown = insertRawMember(tcUnknown, "functionCallingConfig", jsontext.Value("{}"))
			}
		}
		if len(fccUnknown) > 0 {
			fccExtra, err := marshalUnknownMap(fccUnknown)
			if err != nil {
				return nil, nil, err
			}
			tcUnknown = insertRawMember(tcUnknown, "functionCallingConfig", fccExtra)
		}
	}

	if len(tcUnknown) > 0 {
		extraRaw, err := marshalUnknownMap(tcUnknown)
		if err != nil {
			return nil, nil, err
		}
		return choice, extraRaw, nil
	}
	if choice == nil && !derivedServerSideTrue {
		return nil, jsontext.Value("{}"), nil
	}
	return choice, nil, nil
}

// decodeGenerationConfigField parses generationConfig into Sampling,
// MaxOutputTokens, Stop, Reasoning, OutputFormat, and preserved Extra bytes.
// Omitted or null values return a zero decodedGenConfig, empty `{}` wrappers
// record `res.extra = "{}"`, and non-positive maxOutputTokens/candidateCount or
// negative topK values return an error.
func decodeGenerationConfigField(raw jsontext.Value) (decodedGenConfig, error) {
	var res decodedGenConfig
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return res, nil
	}
	var gc wireGenerationConfig
	if err := unmarshalJSONObject(trimmed, &gc); err != nil {
		return res, fmt.Errorf("decode generationConfig: %w", err)
	}

	if gc.MaxOutputTokens != nil {
		if *gc.MaxOutputTokens <= 0 {
			return res, fmt.Errorf("generationConfig.maxOutputTokens must be positive, got %d", *gc.MaxOutputTokens)
		}
		v := *gc.MaxOutputTokens
		res.maxOutputTokens = &v
	}
	if gc.TopK != nil && *gc.TopK < 0 {
		return res, fmt.Errorf("generationConfig.topK must be nonnegative, got %d", *gc.TopK)
	}
	if gc.CandidateCount != nil && *gc.CandidateCount <= 0 {
		return res, fmt.Errorf("generationConfig.candidateCount must be positive, got %d", *gc.CandidateCount)
	}

	res.sampling = ir.Sampling{
		Temperature:      cloneFloat64Ptr(gc.Temperature),
		TopP:             cloneFloat64Ptr(gc.TopP),
		TopK:             cloneInt64Ptr(gc.TopK),
		Seed:             cloneInt64Ptr(gc.Seed),
		FrequencyPenalty: cloneFloat64Ptr(gc.FrequencyPenalty),
		PresencePenalty:  cloneFloat64Ptr(gc.PresencePenalty),
		CandidateCount:   cloneInt64Ptr(gc.CandidateCount),
	}
	if len(gc.StopSequences) > 0 {
		res.stop = slices.Clone(gc.StopSequences)
	}

	gcUnknown := cloneValueMap(gc.Unknown)

	outFmt, keepMimeInExtra, err := decodeGenerationOutputFormat(gc.ResponseMimeType, gc.ResponseSchema, gc.ResponseJSONSchema)
	if err != nil {
		return res, err
	}
	res.output = outFmt
	if keepMimeInExtra && gc.ResponseMimeType != "" {
		mimeBytes, err := marshalDeterministic(gc.ResponseMimeType)
		if err != nil {
			return res, err
		}
		gcUnknown = insertRawMember(gcUnknown, "responseMimeType", jsontext.Value(mimeBytes))
	}

	reasoning, thExtra, err := decodeThinkingConfigField(gc.ThinkingConfig)
	if err != nil {
		return res, err
	}
	res.reasoning = reasoning
	if len(thExtra) > 0 {
		gcUnknown = insertRawMember(gcUnknown, "thinkingConfig", thExtra)
	}

	if len(gcUnknown) > 0 {
		extraBytes, err := marshalUnknownMap(gcUnknown)
		if err != nil {
			return res, err
		}
		res.extra = extraBytes
		return res, nil
	}

	modeledSet := res.maxOutputTokens != nil ||
		len(res.stop) > 0 ||
		res.reasoning != nil ||
		res.output != nil ||
		res.sampling != (ir.Sampling{})
	if !modeledSet {
		res.extra = jsontext.Value("{}")
	}
	return res, nil
}

// decodeGenerationOutputFormat maps responseMimeType, responseSchema, and
// responseJsonSchema onto ir.OutputFormat and reports whether mime must stay in
// Extra as an unmodeled custom MIME type. Setting both schema fields, pairing a
// schema with a non-JSON MIME type, or passing a non-object schema returns an error.
func decodeGenerationOutputFormat(
	mime string,
	respSchema jsontext.Value,
	respJSONSchema jsontext.Value,
) (*ir.OutputFormat, bool, error) {
	schemaTrim := bytes.TrimSpace(respSchema)
	jsonSchemaTrim := bytes.TrimSpace(respJSONSchema)
	hasSchema := len(schemaTrim) > 0 && !bytes.Equal(schemaTrim, []byte("null"))
	hasJSONSchema := len(jsonSchemaTrim) > 0 && !bytes.Equal(jsonSchemaTrim, []byte("null"))
	if hasSchema && hasJSONSchema {
		return nil, false, errors.New("generationConfig cannot set both responseSchema and responseJsonSchema")
	}

	trimmedMime := strings.TrimSpace(mime)
	if hasSchema || hasJSONSchema {
		if trimmedMime != "" && trimmedMime != "application/json" {
			return nil, false, fmt.Errorf("responseSchema requires responseMimeType \"application/json\", got %q", mime)
		}
		chosen := schemaTrim
		if hasJSONSchema {
			chosen = jsonSchemaTrim
		}
		if !isValidSingleJSONObject(chosen) {
			return nil, false, errors.New("generationConfig response schema must be a JSON object")
		}
		return &ir.OutputFormat{
			Kind:   "json_schema",
			Schema: compactRawJSON(chosen),
		}, false, nil
	}

	switch trimmedMime {
	case "":
		return nil, false, nil
	case "application/json":
		return &ir.OutputFormat{Kind: "json_object"}, false, nil
	case "text/plain":
		return &ir.OutputFormat{Kind: "text"}, false, nil
	default:
		return nil, true, nil
	}
}

// decodeThinkingConfigField parses generationConfig.thinkingConfig into an
// optional ir.ReasoningConfig and Extra bytes for unmodeled fields. Omitted or
// null values return (nil, nil, nil), whereas budgets below -1 or configs
// setting both thinkingBudget and thinkingLevel return an error.
//
// Budgets of 0, -1, and positive counts map onto "disabled", "adaptive", and
// "enabled" modes respectively, while includeThoughts=true populates
// Summary="auto" (and Mode="enabled" when unset). Explicit includeThoughts=false
// values and empty `{}` wrappers persist in extraBytes so same-protocol round
// trips remain byte-identical without activating cross-protocol thinking.
func decodeThinkingConfigField(raw jsontext.Value) (*ir.ReasoningConfig, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil, nil
	}
	var tc wireThinkingConfig
	if err := unmarshalJSONObject(trimmed, &tc); err != nil {
		return nil, nil, fmt.Errorf("decode generationConfig.thinkingConfig: %w", err)
	}
	if tc.ThinkingBudget != nil && *tc.ThinkingBudget < -1 {
		return nil, nil, fmt.Errorf("thinkingConfig.thinkingBudget must be >= -1, got %d", *tc.ThinkingBudget)
	}
	if tc.ThinkingBudget != nil && strings.TrimSpace(tc.ThinkingLevel) != "" {
		return nil, nil, errors.New("thinkingConfig cannot set both thinkingBudget and thinkingLevel")
	}

	thUnknown := cloneValueMap(tc.Unknown)
	rc := &ir.ReasoningConfig{}
	hasRC := false

	if tc.ThinkingBudget != nil {
		b := *tc.ThinkingBudget
		rc.BudgetTokens = &b
		hasRC = true
		switch b {
		case 0:
			rc.Mode = "disabled"
		case -1:
			rc.Mode = "adaptive"
		default:
			rc.Mode = "enabled"
		}
	}

	if strings.TrimSpace(tc.ThinkingLevel) != "" {
		effort := strings.ToLower(strings.TrimSpace(tc.ThinkingLevel))
		rc.Mode = "enabled"
		rc.Effort = effort
		hasRC = true
	}

	if tc.IncludeThoughts != nil {
		if *tc.IncludeThoughts {
			rc.Summary = "auto"
			if rc.Mode == "" {
				rc.Mode = "enabled"
			}
			hasRC = true
		} else {
			thUnknown = insertRawMember(thUnknown, "includeThoughts", jsontext.Value("false"))
		}
	}

	var extraBytes jsontext.Value
	if len(thUnknown) > 0 {
		var err error
		extraBytes, err = marshalUnknownMap(thUnknown)
		if err != nil {
			return nil, nil, err
		}
	} else if !hasRC {
		extraBytes = jsontext.Value("{}")
	}

	if !hasRC {
		return nil, extraBytes, nil
	}
	return rc, extraBytes, nil
}

// cloneFloat64Ptr returns a fresh heap copy of p or nil when p is nil.
func cloneFloat64Ptr(p *float64) *float64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// cloneInt64Ptr returns a fresh heap copy of p or nil when p is nil.
func cloneInt64Ptr(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
