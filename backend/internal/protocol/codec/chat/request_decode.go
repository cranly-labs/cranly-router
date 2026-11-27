package chat

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// request_decode.go parses OpenAI Chat Completions JSON request bodies into
// normalized ir.Request structures without losing wire information. Modern and
// legacy Chat shapes, including scalar or multimodal content arrays, assistant
// reasoning_content and refusal fields, legacy functions and function_call
// declarations, and allowed_tools selectors, map into canonical IR fields while
// unmodeled members land in Extra and unknown content blocks become OpaqueItem.
//
// Decoding tolerates invalid UTF-8 sequences inside JSON strings to match legacy
// gateway ingress behavior while keeping json/v2 duplicate-key rejection active
// on every object. Legacy assistant function_call items without explicit call
// IDs derive deterministic SHA-256 correlation IDs paired to subsequent
// role="function" result turns so repeated decodes stay byte-for-byte stable.

// wireAllowedToolsEnvelope decodes Chat tool_choice selector objects whose type discriminator equals allowed_tools.
type wireAllowedToolsEnvelope struct {
	Type         string                 `json:"type"`           // literal "allowed_tools" selector discriminator
	Mode         string                 `json:"mode,omitzero"`  // flat fallback mode
	Tools        []wireAllowedToolEntry `json:"tools,omitzero"` // flat fallback allowed tools
	AllowedTools *struct {
		Mode  string                 `json:"mode"`  // "auto" or "required"
		Tools []wireAllowedToolEntry `json:"tools"` // restricted tool subset
	} `json:"allowed_tools,omitzero"` // canonical nested allowed_tools object
}

// wireAllowedToolEntry decodes one entry inside an allowed_tools selector list.
type wireAllowedToolEntry struct {
	Type     string `json:"type"`          // "function" or "custom"
	Name     string `json:"name,omitzero"` // flat tool name fallback
	Function *struct {
		Name string `json:"name"` // nested function name
	} `json:"function,omitzero"` // canonical Chat function reference
	Custom *struct {
		Name string `json:"name"` // nested custom tool name
	} `json:"custom,omitzero"` // optional custom tool reference
}

// wireErrorToolEnvelope matches the exact two-key {"error": "...", "is_error": true}
// JSON wrapper used to carry structured tool execution errors in Chat tool messages.
type wireErrorToolEnvelope struct {
	Error   *string                   `json:"error"`    // required error message string
	IsError *bool                     `json:"is_error"` // must be true when present
	Unknown map[string]jsontext.Value `json:",embed"`   // any extra key disqualifies envelope
}

// DecodeRequest parses an OpenAI Chat Completions JSON request body into a
// validated ir.Request. Malformed JSON, duplicate object keys, non-object
// roots, negative token caps, or IR invariant violations return an error.
//
// When a request carries overlapping modern and legacy Chat controls,
// max_completion_tokens takes precedence over max_tokens, nested
// reasoning.effort takes precedence over top-level reasoning_effort, and
// tool_choice takes precedence over legacy function_call while legacy
// functions[] append after tools[].
func DecodeRequest(body []byte) (*ir.Request, error) {
	var raw wireInboundRequest
	if err := unmarshalJSONObject(body, &raw); err != nil {
		return nil, fmt.Errorf("decode chat request: %w", err)
	}

	topUnknown := cloneValueMap(raw.Unknown)

	incUsage, keepStreamOptsRaw, err := decodeRequestStreamOptions(raw.StreamOptions)
	if err != nil {
		return nil, err
	}
	if keepStreamOptsRaw {
		topUnknown = insertRawMember(topUnknown, "stream_options", raw.StreamOptions)
	}

	metaUserID, keepMetadataRaw, err := decodeRequestMetadata(raw.Metadata)
	if err != nil {
		return nil, err
	}
	if keepMetadataRaw {
		topUnknown = insertRawMember(topUnknown, "metadata", raw.Metadata)
	}
	if len(bytes.TrimSpace(raw.Thinking)) > 0 {
		// Retain the raw compatible thinking member inside Extra so same-origin Chat
		// round-trips preserve vendor extensions while cross-protocol encoders read Reasoning.
		topUnknown = insertRawMember(topUnknown, "thinking", raw.Thinking)
	}

	req := &ir.Request{
		Model:             raw.Model,
		Messages:          make([]ir.Message, 0, len(raw.Messages)),
		ParallelToolCalls: raw.ParallelToolCalls,
		Sampling: ir.Sampling{
			Temperature:      raw.Temperature,
			TopP:             raw.TopP,
			Seed:             raw.Seed,
			FrequencyPenalty: raw.FrequencyPenalty,
			PresencePenalty:  raw.PresencePenalty,
			CandidateCount:   raw.N,
		},
		Stream:         raw.Stream,
		IncludeUsage:   incUsage,
		ServiceTier:    raw.ServiceTier,
		CacheKey:       raw.PromptCacheKey,
		UserID:         raw.User,
		MetadataUserID: metaUserID,
		SessionID:      raw.SessionID,
		ConversationID: raw.ConversationID,
		Store:          raw.Store,
		Extra:          toExtra(topUnknown),
	}

	if raw.N != nil && *raw.N <= 0 {
		return nil, fmt.Errorf("decode chat request: n must be positive, got %d", *raw.N)
	}

	maxOut := raw.MaxTokens
	if raw.MaxCompletionTokens != nil {
		maxOut = raw.MaxCompletionTokens
	}
	if maxOut != nil {
		if *maxOut < 0 {
			return nil, fmt.Errorf("decode chat request: negative max output tokens %d", *maxOut)
		}
		v := *maxOut
		req.MaxOutputTokens = &v
	}

	stop, err := decodeStopSequences(raw.Stop)
	if err != nil {
		return nil, err
	}
	req.Stop = stop

	req.Reasoning = decodeReasoningConfig(raw.Reasoning, raw.ReasoningEffort, raw.Thinking)

	outFmt, err := decodeResponseFormat(raw.ResponseFormat)
	if err != nil {
		return nil, err
	}
	req.Output = outFmt

	tools, err := decodeChatTools(raw.Tools, raw.Functions)
	if err != nil {
		return nil, err
	}
	req.Tools = tools

	tc, err := decodeChatToolChoice(raw.ToolChoice, raw.FunctionCall)
	if err != nil {
		return nil, err
	}
	req.ToolChoice = tc

	legacyCallIDs := make(map[string]string)
	for idx, m := range raw.Messages {
		decodedMsg, err := decodeChatMessage(idx, m, legacyCallIDs)
		if err != nil {
			return nil, fmt.Errorf("decode chat messages[%d]: %w", idx, err)
		}
		req.Messages = append(req.Messages, decodedMsg)
	}

	if err := ir.Validate(req); err != nil {
		return nil, fmt.Errorf("validate decoded chat request: %w", err)
	}
	return req, nil
}

// DecodeRequest parses an OpenAI Chat Completions JSON request body into a
// validated ir.Request by delegating to the package-level DecodeRequest.
// Malformed JSON, duplicate keys, or IR invariant violations return an error.
func (Codec) DecodeRequest(body []byte) (*ir.Request, error) {
	return DecodeRequest(body)
}

// decodeRequestStreamOptions parses raw stream_options JSON into IncludeUsage.
// Objects that omit or null include_usage report keepRaw=true so callers retain
// vendor stream_options extensions inside Request.Extra.
func decodeRequestStreamOptions(raw jsontext.Value) (*bool, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, false, nil
	}
	var parsed wireStreamOptions
	if err := unmarshalJSONObject(trimmed, &parsed); err != nil {
		return nil, false, fmt.Errorf("decode chat stream_options: %w", err)
	}
	if parsed.IncludeUsage != nil {
		inc := *parsed.IncludeUsage
		return &inc, false, nil
	}
	return nil, true, nil
}

// decodeRequestMetadata parses raw metadata JSON into MetadataUserID. Objects
// without a non-empty user_id report keepRaw=true so arbitrary metadata maps
// survive same-protocol round trips inside Request.Extra.
func decodeRequestMetadata(raw jsontext.Value) (string, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", false, nil
	}
	var parsed wireMetadata
	if err := unmarshalJSONObject(trimmed, &parsed); err != nil {
		return "", false, fmt.Errorf("decode chat metadata: %w", err)
	}
	if parsed.UserID != "" {
		return parsed.UserID, false, nil
	}
	return "", true, nil
}

// decodeStopSequences parses a string or string-array stop field into a slice.
// Absent, null, or empty array values return nil so Request.Stop omits empty lists.
func decodeStopSequences(raw jsontext.Value) ([]string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if trimmed[0] == '"' {
		var single string
		if err := unmarshalJSONValue(trimmed, &single); err != nil {
			return nil, fmt.Errorf("decode chat stop string: %w", err)
		}
		return []string{single}, nil
	}
	var list []string
	if err := unmarshalJSONValue(trimmed, &list); err != nil {
		return nil, fmt.Errorf("decode chat stop array: %w", err)
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list, nil
}

// decodeReasoningConfig extracts Chat reasoning effort and optional compatible
// thinking switches or token budgets into ir.ReasoningConfig. Non-empty nested
// reasoning.effort takes precedence over top-level reasoning_effort, and
// requests with neither effort nor recognized thinking controls return nil.
func decodeReasoningConfig(nested *wireReasoning, topEffort string, rawThinking jsontext.Value) *ir.ReasoningConfig {
	var effort string
	// Compatible Chat clients send either top-level reasoning_effort or a nested
	// reasoning object; nested effort wins when both appear on the same request.
	if nested != nil {
		effort = strings.TrimSpace(nested.Effort)
	}
	if effort == "" {
		effort = strings.TrimSpace(topEffort)
	}
	if effort != "" {
		if normalized := protocol.NormalizeReasoningEffort(effort); normalized != "" {
			effort = normalized
		}
	}

	var mode string
	var budget *int64
	trimmedThinking := bytes.TrimSpace(rawThinking)
	if len(trimmedThinking) > 0 && trimmedThinking[0] == '{' {
		var compat wireCompatThinking
		// Leave mode and budget unset when a vendor thinking object uses incompatible
		// field types so raw thinking JSON still round-trips intact through Extra.
		if err := unmarshalJSONObject(trimmedThinking, &compat); err == nil {
			switch strings.ToLower(strings.TrimSpace(compat.Type)) {
			case "disabled", "none", "off":
				mode = "disabled"
			case "enabled":
				mode = "enabled"
			case "adaptive":
				mode = "adaptive"
			}
			if compat.BudgetTokens != nil && *compat.BudgetTokens >= 0 {
				b := *compat.BudgetTokens
				budget = &b
			}
		}
	}

	if effort == "" && mode == "" && budget == nil {
		return nil
	}
	return &ir.ReasoningConfig{
		Mode:         mode,
		Effort:       effort,
		BudgetTokens: budget,
	}
}

// decodeResponseFormat maps Chat response_format into an ir.OutputFormat value.
// Nil inputs return (nil, nil), whereas unknown format types return an error.
func decodeResponseFormat(rf *wireResponseFormat) (*ir.OutputFormat, error) {
	if rf == nil {
		return nil, nil
	}
	switch rf.Type {
	case "text":
		return &ir.OutputFormat{Kind: "text"}, nil
	case "json_object":
		return &ir.OutputFormat{Kind: "json_object"}, nil
	case "json_schema":
		out := &ir.OutputFormat{Kind: "json_schema"}
		if rf.JSONSchema != nil {
			out.Name = rf.JSONSchema.Name
			out.Description = rf.JSONSchema.Description
			out.Strict = rf.JSONSchema.Strict
			schema := bytes.TrimSpace(rf.JSONSchema.Schema)
			if len(schema) > 0 && !bytes.Equal(schema, []byte("null")) {
				out.Schema = compactRawJSON(schema)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported chat response_format type %q", rf.Type)
	}
}

// decodeChatTools converts modern tools[] and legacy functions[] entries into
// ir.Tool declarations. Non-function tools[] entries become ToolHosted with
// compacted raw bytes preserved inside Hosted OpaqueItem values.
func decodeChatTools(rawTools []jsontext.Value, legacyFns []wireLegacyFunction) ([]ir.Tool, error) {
	if len(rawTools) == 0 && len(legacyFns) == 0 {
		return nil, nil
	}
	out := make([]ir.Tool, 0, len(rawTools)+len(legacyFns))
	for i, rawElem := range rawTools {
		trimmed := bytes.TrimSpace(rawElem)
		var wt wireTool
		if err := unmarshalJSONObject(trimmed, &wt); err != nil {
			return nil, fmt.Errorf("decode tools[%d]: %w", i, err)
		}
		if wt.Type == "function" {
			if wt.Function == nil || strings.TrimSpace(wt.Function.Name) == "" {
				return nil, fmt.Errorf("tools[%d]: function tool requires non-empty function.name", i)
			}
			out = append(out, ir.Tool{
				Kind:        ir.ToolFunction,
				Name:        wt.Function.Name,
				Description: wt.Function.Description,
				InputSchema: cleanRawSchema(wt.Function.Parameters),
				Strict:      wt.Function.Strict,
				Extra:       toExtra(wt.Unknown),
			})
			continue
		}
		typ := strings.TrimSpace(wt.Type)
		if typ == "" {
			return nil, fmt.Errorf("tools[%d]: empty tool type", i)
		}
		out = append(out, ir.Tool{
			Kind: ir.ToolHosted,
			Name: typ,
			Hosted: &ir.OpaqueItem{
				Origin: protocol.ProtocolOpenAIChatCompletions,
				Type:   typ,
				Raw:    compactRawJSON(trimmed),
			},
		})
	}
	for i, fn := range legacyFns {
		if strings.TrimSpace(fn.Name) == "" {
			return nil, fmt.Errorf("functions[%d]: function requires non-empty name", i)
		}
		// Legacy functions[] upgrade into outer wireTool objects on encode, so strip
		// colliding "type" or "function" extension keys that wireTool reserves.
		fnUnknown := cloneValueMap(fn.Unknown)
		delete(fnUnknown, "type")
		delete(fnUnknown, "function")
		out = append(out, ir.Tool{
			Kind:        ir.ToolFunction,
			Name:        fn.Name,
			Description: fn.Description,
			InputSchema: cleanRawSchema(fn.Parameters),
			Strict:      fn.Strict,
			Extra:       toExtra(fnUnknown),
		})
	}
	return out, nil
}

// cleanRawSchema trims and compacts raw JSON schema bytes. Absent or literal
// null schema tokens normalize to nil so Tool.InputSchema omits null literals.
func cleanRawSchema(raw jsontext.Value) jsontext.Value {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	return compactRawJSON(trimmed)
}

// decodeChatToolChoice parses modern tool_choice or legacy function_call into
// an ir.ToolChoice value. Non-null tool_choice takes precedence when both are
// present, and absent or null fields return (nil, nil).
func decodeChatToolChoice(rawToolChoice, rawFunctionCall jsontext.Value) (*ir.ToolChoice, error) {
	trimmedTC := bytes.TrimSpace(rawToolChoice)
	if len(trimmedTC) > 0 && !bytes.Equal(trimmedTC, []byte("null")) {
		return parseToolChoiceValue(trimmedTC, false)
	}
	trimmedFC := bytes.TrimSpace(rawFunctionCall)
	if len(trimmedFC) > 0 && !bytes.Equal(trimmedFC, []byte("null")) {
		return parseToolChoiceValue(trimmedFC, true)
	}
	return nil, nil
}

// parseToolChoiceValue parses one scalar or object tool_choice or legacy
// function_call value. When legacy is true, "required" strings fail and object
// values expect a flat {"name": "..."} shape.
func parseToolChoiceValue(trimmed []byte, legacy bool) (*ir.ToolChoice, error) {
	if trimmed[0] == '"' {
		var mode string
		if err := unmarshalJSONValue(trimmed, &mode); err != nil {
			return nil, fmt.Errorf("decode tool_choice string: %w", err)
		}
		switch mode {
		case "auto":
			return &ir.ToolChoice{Mode: ir.ToolChoiceAuto}, nil
		case "none":
			return &ir.ToolChoice{Mode: ir.ToolChoiceNone}, nil
		case "required":
			if legacy {
				return nil, fmt.Errorf("unsupported legacy function_call string %q", mode)
			}
			return &ir.ToolChoice{Mode: ir.ToolChoiceRequired}, nil
		default:
			return nil, fmt.Errorf("unsupported tool_choice mode %q", mode)
		}
	}

	if legacy {
		var legacyObj struct {
			Name string `json:"name"` // target legacy function name
		}
		if err := unmarshalJSONObject(trimmed, &legacyObj); err != nil {
			return nil, fmt.Errorf("decode legacy function_call: %w", err)
		}
		if strings.TrimSpace(legacyObj.Name) == "" {
			return nil, errors.New("legacy function_call object requires non-empty name")
		}
		return &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: legacyObj.Name},
		}, nil
	}

	var disc struct {
		Type     string `json:"type"`          // function, custom, or allowed_tools
		Name     string `json:"name,omitzero"` // flat fallback name
		Function *struct {
			Name string `json:"name"` // nested function name
		} `json:"function,omitzero"` // function selector target
		Custom *struct {
			Name string `json:"name"` // nested custom tool name
		} `json:"custom,omitzero"` // custom selector target
	}
	if err := unmarshalJSONObject(trimmed, &disc); err != nil {
		return nil, fmt.Errorf("decode tool_choice object: %w", err)
	}

	switch disc.Type {
	case "function":
		name := disc.Name
		if disc.Function != nil && disc.Function.Name != "" {
			name = disc.Function.Name
		}
		if strings.TrimSpace(name) == "" {
			return nil, errors.New("function tool_choice requires non-empty function.name")
		}
		return &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: name},
		}, nil

	case "custom":
		name := disc.Name
		if disc.Custom != nil && disc.Custom.Name != "" {
			name = disc.Custom.Name
		}
		if strings.TrimSpace(name) == "" {
			return nil, errors.New("custom tool_choice requires non-empty custom.name")
		}
		return &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{Kind: ir.ToolCustom, Name: name},
		}, nil

	case "allowed_tools":
		return decodeAllowedToolsChoice(trimmed)

	default:
		return nil, fmt.Errorf("unsupported tool_choice object type %q", disc.Type)
	}
}

// decodeAllowedToolsChoice parses a Chat allowed_tools tool_choice object into
// ir.ToolChoice. Nested allowed_tools fields override flat compatibility fields,
// and an omitted mode defaults to ToolChoiceAuto.
func decodeAllowedToolsChoice(trimmed []byte) (*ir.ToolChoice, error) {
	var env wireAllowedToolsEnvelope
	if err := unmarshalJSONObject(trimmed, &env); err != nil {
		return nil, fmt.Errorf("decode allowed_tools: %w", err)
	}
	modeStr := env.Mode
	entries := env.Tools
	if env.AllowedTools != nil {
		if env.AllowedTools.Mode != "" {
			modeStr = env.AllowedTools.Mode
		}
		if len(env.AllowedTools.Tools) > 0 {
			entries = env.AllowedTools.Tools
		}
	}
	var mode ir.ToolChoiceMode
	switch modeStr {
	case "", "auto":
		mode = ir.ToolChoiceAuto
	case "required":
		mode = ir.ToolChoiceRequired
	case "none":
		mode = ir.ToolChoiceNone
	default:
		return nil, fmt.Errorf("unsupported allowed_tools mode %q", modeStr)
	}

	var allowed []ir.ToolRef
	if len(entries) > 0 {
		allowed = make([]ir.ToolRef, 0, len(entries))
		for i, entry := range entries {
			kind := ir.ToolFunction
			name := entry.Name
			switch entry.Type {
			case "function", "":
				if entry.Function != nil && entry.Function.Name != "" {
					name = entry.Function.Name
				}
			case "custom":
				kind = ir.ToolCustom
				if entry.Custom != nil && entry.Custom.Name != "" {
					name = entry.Custom.Name
				}
			default:
				return nil, fmt.Errorf("allowed_tools[%d]: unsupported tool type %q", i, entry.Type)
			}
			if strings.TrimSpace(name) == "" {
				return nil, fmt.Errorf("allowed_tools[%d]: empty tool name", i)
			}
			allowed = append(allowed, ir.ToolRef{Kind: kind, Name: name})
		}
	}
	return &ir.ToolChoice{
		Mode:         mode,
		AllowedTools: allowed,
	}, nil
}

// decodeChatMessage converts one Chat wireInboundMessage into a normalized
// ir.Message. Deterministic IDs derived for legacy assistant function_call
// items are stored in legacyCallIDs so subsequent role="function" turns pair
// by tool name, while non-function participant names survive in Message.Extra.
func decodeChatMessage(msgIdx int, m wireInboundMessage, legacyCallIDs map[string]string) (ir.Message, error) {
	switch m.Role {
	case "system":
		return decodeStandardRoleMessage(ir.RoleSystem, m)
	case "developer":
		return decodeStandardRoleMessage(ir.RoleDeveloper, m)
	case "user":
		return decodeStandardRoleMessage(ir.RoleUser, m)
	case "assistant":
		return decodeAssistantMessage(msgIdx, m, legacyCallIDs)
	case "tool":
		return decodeToolResultMessage(m.ToolCallID, m, true)
	case "function":
		fnName, err := decodeLegacyFunctionRoleName(m.RawName)
		if err != nil {
			return ir.Message{}, err
		}
		callID := m.ToolCallID
		// Legacy role="function" replies omit tool_call_id and identify their call
		// by function name; orphan replies synthesize a stable index-hashed ID.
		if callID == "" && fnName != "" {
			if derived, ok := legacyCallIDs[fnName]; ok {
				callID = derived
			} else {
				callID = deriveLegacyFunctionCallID(msgIdx, fnName, "")
			}
		}
		return decodeToolResultMessage(callID, m, false)
	default:
		return ir.Message{}, fmt.Errorf("unsupported message role %q", m.Role)
	}
}

// decodeLegacyFunctionRoleName parses the optional string "name" field on a
// legacy role="function" message, rejecting non-string non-null values.
func decodeLegacyFunctionRoleName(raw jsontext.Value) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}
	var name string
	if err := unmarshalJSONValue(trimmed, &name); err != nil {
		return "", fmt.Errorf("decode function message name: %w", err)
	}
	return name, nil
}

// messageExtraWithName builds Message.Extra from unknown and preserves any
// non-empty RawName token under "name" for non-function message roles.
func messageExtraWithName(unknown map[string]jsontext.Value, rawName jsontext.Value) ir.Extra {
	trimmedName := bytes.TrimSpace(rawName)
	if len(trimmedName) == 0 {
		return toExtra(unknown)
	}
	merged := insertRawMember(cloneValueMap(unknown), "name", trimmedName)
	return toExtra(merged)
}

// cloneValueMap returns a shallow map copy of src, or nil when src is empty.
func cloneValueMap(src map[string]jsontext.Value) map[string]jsontext.Value {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]jsontext.Value, len(src))
	maps.Copy(out, src)
	return out
}

// insertRawMember stores a compacted copy of raw under key in dst, allocating dst when nil.
func insertRawMember(dst map[string]jsontext.Value, key string, raw jsontext.Value) map[string]jsontext.Value {
	if dst == nil {
		dst = make(map[string]jsontext.Value, 1)
	}
	dst[key] = compactRawJSON(raw)
	return dst
}

// decodeStandardRoleMessage decodes a system, developer, or user
// wireInboundMessage into ir.Message. Absent, null, or empty scalar content
// normalizes to an empty non-nil Parts slice.
func decodeStandardRoleMessage(role ir.Role, m wireInboundMessage) (ir.Message, error) {
	parts, err := decodeMessageContentParts(m.Content, role)
	if err != nil {
		return ir.Message{}, err
	}
	if parts == nil {
		parts = []ir.Part{}
	}
	return ir.Message{
		Role:  role,
		Parts: parts,
		Extra: messageExtraWithName(m.Unknown, m.RawName),
	}, nil
}

// decodeAssistantMessage decodes an assistant wireInboundMessage into canonical
// IR part order: reasoning first, followed by visible text/media/opaque content,
// refusal, and tool calls.
//
// Legacy assistant function_call fields lack wire call IDs, so decodeAssistantMessage
// hashes the message index, function name, and decoded arguments into a stable
// CallID and records the latest ID per function name in legacyCallIDs.
func decodeAssistantMessage(msgIdx int, m wireInboundMessage, legacyCallIDs map[string]string) (ir.Message, error) {
	parts := make([]ir.Part, 0, 2+len(m.ToolCalls))

	reasoningText := selectMessageReasoning(m.ReasoningContent, m.Reasoning)
	if reasoningText != "" {
		parts = append(parts, ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Segments: []string{reasoningText},
			},
		})
	}

	contentParts, err := decodeAssistantContentParts(m.Content, m.Annotations)
	if err != nil {
		return ir.Message{}, err
	}
	parts = append(parts, contentParts...)

	if m.Refusal != "" {
		parts = append(parts, ir.Part{
			Kind: ir.PartRefusal,
			Text: m.Refusal,
		})
	}

	for tIdx, tc := range m.ToolCalls {
		if strings.TrimSpace(tc.Function.Name) == "" {
			return ir.Message{}, fmt.Errorf("tool_calls[%d]: empty function.name", tIdx)
		}
		args, err := decodeFunctionArguments(tc.Function.RawArgs)
		if err != nil {
			return ir.Message{}, fmt.Errorf("tool_calls[%d]: %w", tIdx, err)
		}
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
			return ir.Message{}, errors.New("function_call: empty name")
		}
		args, err := decodeFunctionArguments(m.FunctionCall.RawArgs)
		if err != nil {
			return ir.Message{}, fmt.Errorf("function_call: %w", err)
		}
		derivedID := deriveLegacyFunctionCallID(msgIdx, m.FunctionCall.Name, args)
		legacyCallIDs[m.FunctionCall.Name] = derivedID
		parts = append(parts, ir.Part{
			Kind: ir.PartToolCall,
			Call: &ir.ToolCall{
				CallID:    derivedID,
				Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: m.FunctionCall.Name},
				Arguments: args,
			},
		})
	}

	return ir.Message{
		Role:  ir.RoleAssistant,
		Parts: parts,
		Extra: messageExtraWithName(m.Unknown, m.RawName),
	}, nil
}

// decodeAssistantContentParts parses scalar or array content on an assistant turn.
// Empty scalar content without annotations omits a redundant empty text part so
// empty, tool-only, and refusal-only turns normalize identically to null.
func decodeAssistantContentParts(raw jsontext.Value, msgAnns []wireAnnotation) ([]ir.Part, error) {
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
			return nil, fmt.Errorf("decode assistant content string: %w", err)
		}
		// Drop empty scalar content when no annotations exist so content:"" and
		// content:null normalize to identical IR assistant messages.
		if s == "" && len(anns) == 0 {
			return nil, nil
		}
		return []ir.Part{{
			Kind:        ir.PartText,
			Text:        s,
			Annotations: anns,
		}}, nil
	}
	parts, err := decodeContentPartArray(trimmed, ir.RoleAssistant)
	if err != nil {
		return nil, err
	}
	if len(anns) > 0 {
		attached := false
		for i := range parts {
			if parts[i].Kind == ir.PartText {
				parts[i].Annotations = append(parts[i].Annotations, anns...)
				attached = true
				break
			}
		}
		if !attached {
			parts = append([]ir.Part{{Kind: ir.PartText, Annotations: anns}}, parts...)
		}
	}
	return parts, nil
}

// decodeToolResultMessage converts a role="tool" or legacy role="function"
// message into a dedicated ir.RoleTool turn containing one PartToolResult.
// Structured two-key error JSON strings unpack into ToolResult.IsError=true,
// and preserveName retains RawName in Message.Extra for role="tool" messages.
func decodeToolResultMessage(callID string, m wireInboundMessage, preserveName bool) (ir.Message, error) {
	trimmed := bytes.TrimSpace(m.Content)
	var resultParts []ir.Part
	var isError bool

	switch {
	case len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")):
		resultParts = []ir.Part{}
	case trimmed[0] == '"':
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return ir.Message{}, fmt.Errorf("decode tool content string: %w", err)
		}
		if errMsg, ok := unpackToolErrorJSON(s); ok {
			isError = true
			resultParts = []ir.Part{{Kind: ir.PartText, Text: errMsg}}
		} else if s == "" {
			resultParts = []ir.Part{}
		} else {
			resultParts = []ir.Part{{Kind: ir.PartText, Text: s}}
		}
	case trimmed[0] == '[':
		parsed, err := decodeContentPartArray(trimmed, ir.RoleTool)
		if err != nil {
			return ir.Message{}, err
		}
		resultParts = parsed
	default:
		return ir.Message{}, errors.New("tool content must be string, array, or null")
	}

	extra := toExtra(m.Unknown)
	if preserveName {
		extra = messageExtraWithName(m.Unknown, m.RawName)
	}
	return ir.Message{
		Role: ir.RoleTool,
		Parts: []ir.Part{{
			Kind: ir.PartToolResult,
			Result: &ir.ToolResult{
				CallID:  callID,
				Content: resultParts,
				IsError: isError,
			},
		}},
		Extra: extra,
	}, nil
}

// unpackToolErrorJSON extracts the error message when s is the exact two-key
// {"error": "<msg>", "is_error": true} structured error envelope. Any extra
// JSON keys, non-true is_error values, or non-JSON text return ("", false).
func unpackToolErrorJSON(s string) (string, bool) {
	trimmed := strings.TrimSpace(s)
	if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
		return "", false
	}
	var env wireErrorToolEnvelope
	// Ordinary brace-wrapped tool outputs that are not valid JSON objects remain
	// plain tool text, so unmarshal failures return false without failing decode.
	if unmarshalJSONObject([]byte(trimmed), &env) != nil {
		return "", false
	}
	if len(env.Unknown) > 0 || env.Error == nil || env.IsError == nil || !*env.IsError {
		return "", false
	}
	return *env.Error, true
}

// decodeMessageContentParts parses scalar string, part-array, or null content
// on system, developer, or user messages into ordered ir.Part elements. Absent,
// null, or empty scalar strings return (nil, nil), whereas non-string/non-array JSON fails.
func decodeMessageContentParts(raw jsontext.Value, role ir.Role) ([]ir.Part, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return nil, fmt.Errorf("decode %s content string: %w", role, err)
		}
		if s == "" {
			return nil, nil
		}
		return []ir.Part{{Kind: ir.PartText, Text: s}}, nil
	}
	if trimmed[0] == '[' {
		return decodeContentPartArray(trimmed, role)
	}
	return nil, fmt.Errorf("decode %s content: expected string, array, or null", role)
}

// decodeContentPartArray parses a JSON array of Chat content parts for role.
// Non-object elements or part-level validation errors return an indexed error.
func decodeContentPartArray(trimmed []byte, role ir.Role) ([]ir.Part, error) {
	var rawParts []jsontext.Value
	if err := unmarshalJSONValue(trimmed, &rawParts); err != nil {
		return nil, fmt.Errorf("decode content array: %w", err)
	}
	out := make([]ir.Part, 0, len(rawParts))
	for i, rawElem := range rawParts {
		elemTrimmed := bytes.TrimSpace(rawElem)
		var wcp wireContentPart
		if err := unmarshalJSONObject(elemTrimmed, &wcp); err != nil {
			return nil, fmt.Errorf("content[%d]: %w", i, err)
		}
		part, err := decodeOneContentPart(wcp, elemTrimmed, role)
		if err != nil {
			return nil, fmt.Errorf("content[%d]: %w", i, err)
		}
		out = append(out, part)
	}
	return out, nil
}

// decodeOneContentPart maps a single typed Chat content part into an ir.Part.
// Unrecognized non-empty part types on non-tool messages preserve their raw JSON
// inside an OpaqueItem, whereas tool_result only accepts text and media parts.
func decodeOneContentPart(wcp wireContentPart, rawElem []byte, role ir.Role) (ir.Part, error) {
	switch wcp.Type {
	case "text":
		return ir.Part{
			Kind:        ir.PartText,
			Text:        wcp.Text,
			Annotations: decodeWireAnnotations(wcp.Annotations),
			Extra:       toExtra(wcp.Unknown),
		}, nil

	case "refusal":
		if role == ir.RoleTool || role == ir.RoleSystem || role == ir.RoleDeveloper {
			return ir.Part{}, fmt.Errorf("refusal part is not valid in %s message", role)
		}
		return ir.Part{
			Kind:  ir.PartRefusal,
			Text:  wcp.Refusal,
			Extra: toExtra(wcp.Unknown),
		}, nil

	case "image_url":
		media, err := decodeImageURLPart(wcp.ImageURL)
		if err != nil {
			return ir.Part{}, err
		}
		return ir.Part{
			Kind:  ir.PartMedia,
			Media: media,
			Extra: toExtra(wcp.Unknown),
		}, nil

	case "input_audio":
		media, err := decodeInputAudioPart(wcp.InputAudio)
		if err != nil {
			return ir.Part{}, err
		}
		return ir.Part{
			Kind:  ir.PartMedia,
			Media: media,
			Extra: toExtra(wcp.Unknown),
		}, nil

	case "file":
		media, err := decodeFilePart(wcp.File)
		if err != nil {
			return ir.Part{}, err
		}
		return ir.Part{
			Kind:  ir.PartMedia,
			Media: media,
			Extra: toExtra(wcp.Unknown),
		}, nil

	default:
		typ := strings.TrimSpace(wcp.Type)
		if typ == "" {
			return ir.Part{}, errors.New("empty content part type")
		}
		if role == ir.RoleTool {
			return ir.Part{}, fmt.Errorf("unsupported tool_result content part type %q", typ)
		}
		return ir.Part{
			Kind: ir.PartOpaque,
			Opaque: &ir.OpaqueItem{
				Origin: protocol.ProtocolOpenAIChatCompletions,
				Type:   typ,
				Raw:    compactRawJSON(rawElem),
			},
		}, nil
	}
}

// decodeImageURLPart converts a Chat image_url field into an ir.Media image
// value. Both canonical {"url": "..."} objects and compatible bare URL strings
// are accepted, with base64 data URIs split into MIME and payload bytes.
func decodeImageURLPart(raw jsontext.Value) (*ir.Media, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, errors.New("missing image_url value")
	}
	var rawURL, detail string
	if trimmed[0] == '"' {
		if err := unmarshalJSONValue(trimmed, &rawURL); err != nil {
			return nil, fmt.Errorf("decode image_url string: %w", err)
		}
	} else {
		var img wireImageURL
		if err := unmarshalJSONObject(trimmed, &img); err != nil {
			return nil, fmt.Errorf("decode image_url object: %w", err)
		}
		rawURL = img.URL
		detail = img.Detail
	}
	if strings.TrimSpace(rawURL) == "" {
		return nil, errors.New("empty image_url.url")
	}
	if mime, payload, isData, valid := parseBase64DataURI(rawURL); isData {
		if !valid {
			return nil, errors.New("malformed base64 image data URI")
		}
		return &ir.Media{
			Type:   ir.MediaImage,
			MIME:   mime,
			Source: ir.MediaSourceBase64,
			Data:   payload,
			Detail: detail,
		}, nil
	}
	return &ir.Media{
		Type:   ir.MediaImage,
		Source: ir.MediaSourceURL,
		Data:   rawURL,
		Detail: detail,
	}, nil
}

// decodeInputAudioPart converts a Chat input_audio object into an ir.Media
// audio value. Missing objects, empty base64 payloads, or non-audio MIME formats
// return an error.
func decodeInputAudioPart(audio *wireInputAudio) (*ir.Media, error) {
	if audio == nil || strings.TrimSpace(audio.Data) == "" {
		return nil, errors.New("input_audio requires non-empty base64 data")
	}
	mime, err := audioFormatToMIME(audio.Format)
	if err != nil {
		return nil, err
	}
	return &ir.Media{
		Type:   ir.MediaAudio,
		MIME:   mime,
		Source: ir.MediaSourceBase64,
		Data:   audio.Data,
	}, nil
}

// audioFormatToMIME normalizes a Chat input_audio format label into a canonical
// audio MIME type. Blank or wave labels map to "audio/wav", mpeg labels map to
// "audio/mp3", and slash-containing strings outside "audio/*" return an error.
func audioFormatToMIME(format string) (string, error) {
	f := strings.ToLower(strings.TrimSpace(format))
	if after, ok := strings.CutPrefix(f, "audio/"); ok {
		f = strings.TrimSpace(after)
	}
	if strings.Contains(f, "/") {
		return "", fmt.Errorf("invalid input_audio format %q", format)
	}
	switch f {
	case "", "wav", "x-wav", "wave":
		return "audio/wav", nil
	case "mp3", "mpeg":
		return "audio/mp3", nil
	default:
		return "audio/" + f, nil
	}
}

// decodeFilePart converts a Chat file attachment object into an ir.Media
// document value. Non-empty file_id takes precedence over file_data, and raw
// base64 file_data without a data URI header defaults to "application/pdf".
func decodeFilePart(file *wireFile) (*ir.Media, error) {
	if file == nil {
		return nil, errors.New("missing file attachment object")
	}
	if strings.TrimSpace(file.FileID) != "" {
		return &ir.Media{
			Type:   ir.MediaDocument,
			Source: ir.MediaSourceFileID,
			Data:   file.FileID,
			Name:   file.Filename,
		}, nil
	}
	if strings.TrimSpace(file.FileData) == "" {
		return nil, errors.New("file attachment requires file_id or file_data")
	}
	if mime, payload, isData, valid := parseBase64DataURI(file.FileData); isData {
		if !valid {
			return nil, errors.New("malformed base64 file_data URI")
		}
		return &ir.Media{
			Type:   ir.MediaDocument,
			MIME:   mime,
			Source: ir.MediaSourceBase64,
			Data:   payload,
			Name:   file.Filename,
		}, nil
	}
	return &ir.Media{
		Type:   ir.MediaDocument,
		MIME:   "application/pdf",
		Source: ir.MediaSourceBase64,
		Data:   file.FileData,
		Name:   file.Filename,
	}, nil
}

// decodeWireAnnotations converts Chat citation annotations into ir.Annotation
// slices. Nested url_citation fields override flat fallback fields, and invalid
// negative or inverted span offsets are cleared before validation.
func decodeWireAnnotations(anns []wireAnnotation) []ir.Annotation {
	if len(anns) == 0 {
		return nil
	}
	out := make([]ir.Annotation, 0, len(anns))
	for _, a := range anns {
		ia := ir.Annotation{
			Kind:       a.Type,
			URL:        a.URL,
			Title:      a.Title,
			StartIndex: a.StartIndex,
			EndIndex:   a.EndIndex,
			Extra:      toExtra(a.Unknown),
		}
		if a.URLCitation != nil {
			if a.URLCitation.URL != "" {
				ia.URL = a.URLCitation.URL
			}
			if a.URLCitation.Title != "" {
				ia.Title = a.URLCitation.Title
			}
			if a.URLCitation.StartIndex != nil {
				ia.StartIndex = a.URLCitation.StartIndex
			}
			if a.URLCitation.EndIndex != nil {
				ia.EndIndex = a.URLCitation.EndIndex
			}
		}
		// Scrub negative or inverted citation span indices emitted by compatible
		// providers so malformed spans do not fail ir.Validate on the turn.
		if ia.StartIndex != nil && *ia.StartIndex < 0 {
			ia.StartIndex = nil
		}
		if ia.EndIndex != nil && *ia.EndIndex < 0 {
			ia.EndIndex = nil
		}
		if ia.StartIndex != nil && ia.EndIndex != nil && *ia.EndIndex < *ia.StartIndex {
			ia.EndIndex = nil
		}
		out = append(out, ia)
	}
	return out
}

// selectMessageReasoning returns non-empty primary reasoning_content when
// present. Otherwise it falls back to non-empty reasoning or returns "".
func selectMessageReasoning(primary, fallback *string) string {
	if primary != nil && *primary != "" {
		return *primary
	}
	if fallback != nil && *fallback != "" {
		return *fallback
	}
	return ""
}

// deriveLegacyFunctionCallID derives a deterministic correlation ID for a legacy
// Chat function_call or orphan function-role message. Hashing msgIdx, name, and
// args keeps repeated decodes identical without mutable random generators.
func deriveLegacyFunctionCallID(msgIdx int, name, args string) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "chat_legacy_fn:%d:%s:%s", msgIdx, name, args))
	return "call_fn_" + hex.EncodeToString(sum[:6])
}
