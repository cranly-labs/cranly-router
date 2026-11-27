package responses

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
)

// request_decode.go parses OpenAI Responses JSON request bodies into validated
// ir.Request values without discarding wire fields. String shorthand inputs,
// typed and role-shorthand message items, reasoning items with summary and
// encrypted continuity tokens, function/custom/tool_search calls and outputs,
// namespace groups, additional_tools lifters, and hosted server tools all map
// into canonical IR turns and declarations while unmodeled fields land in Extra.
//
// Consecutive assistant-side items (reasoning, assistant messages, calls, and
// opaque assistant items) group into a single RoleAssistant Message, consecutive
// *_output items group into a single RoleTool Message preserving original item
// order and IDs, and user, system, and developer messages each occupy their own
// turn so flattening during EncodeRequest reproduces the original item sequence.

// wireToolChoiceObject decodes object-shaped OpenAI Responses tool_choice selector payloads on incoming requests.
type wireToolChoiceObject struct {
	Type         string                    `json:"type"`                   // function, custom, tool_search, allowed_tools, or hosted type
	Name         string                    `json:"name,omitzero"`          // flat tool name on function or custom selectors
	Namespace    string                    `json:"namespace,omitzero"`     // optional namespace on function or custom selectors
	Mode         string                    `json:"mode,omitzero"`          // "auto" or "required" on flat allowed_tools
	Tools        []wireAllowedToolEntry    `json:"tools,omitzero"`         // allowed tool subset on flat allowed_tools
	Function     *wireCompatChoiceName     `json:"function,omitzero"`      // compatible Chat-nested function selector
	AllowedTools *wireCompatAllowedWrapper `json:"allowed_tools,omitzero"` // compatible Chat-nested allowed_tools wrapper
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled tool_choice members
}

// wireCompatChoiceName decodes compatible Chat-shaped {"function": {"name": ...}} tool selectors.
type wireCompatChoiceName struct {
	Name string `json:"name"` // nested function name
}

// wireCompatAllowedWrapper decodes compatible Chat-shaped {"allowed_tools": {"mode": ..., "tools": [...]}} selectors.
type wireCompatAllowedWrapper struct {
	Mode  string                 `json:"mode"`  // "auto" or "required"
	Tools []wireAllowedToolEntry `json:"tools"` // restricted tool entries
}

// wireAllowedToolEntry decodes or encodes one entry inside a Responses allowed_tools selector.
type wireAllowedToolEntry struct {
	Type      string                `json:"type"`               // function, custom, tool_search, or hosted tool type
	Name      string                `json:"name,omitzero"`      // flat tool name
	Namespace string                `json:"namespace,omitzero"` // optional namespace name
	Function  *wireCompatChoiceName `json:"function,omitzero"`  // compatible nested function name
}

// wireErrorToolEnvelope matches the exact two-key {"error": "...", "is_error": true}
// JSON wrapper used to represent structured tool execution failures in *_output items.
type wireErrorToolEnvelope struct {
	Error   *string                   `json:"error"`    // required error message string
	IsError *bool                     `json:"is_error"` // must equal true when present
	Unknown map[string]jsontext.Value `json:",embed"`   // any third key disqualifies envelope
}

// reasoningSubExtras holds unmodeled reasoning object members and non-encrypted include entries.
type reasoningSubExtras struct {
	reasoningRaw jsontext.Value // unknown members or "{}" from top-level reasoning
	includeRaw   jsontext.Value // remaining include[] strings or "[]"
}

// DecodeRequest parses an OpenAI Responses JSON request body into a validated
// ir.Request. Malformed JSON, duplicate object keys, non-object roots, blank
// models, missing inputs, invalid service_tier labels, or IR invariant
// violations return a descriptive error.
//
// Top-level instructions decode into Request.System while system and developer
// message items inside input[] remain in Request.Messages at their original
// turn positions. Non-null conversation strings or objects set ConversationSet
// alongside ConversationID so stateless target dialects can detect stateful bindings.
func DecodeRequest(body []byte) (*ir.Request, error) {
	var raw wireInboundRequest
	if err := unmarshalJSONObject(body, &raw); err != nil {
		return nil, fmt.Errorf("decode responses request: %w", err)
	}
	if strings.TrimSpace(raw.Model) == "" {
		return nil, errors.New("decode responses request: model must be a non-empty string")
	}
	if raw.MaxOutputTokens != nil && *raw.MaxOutputTokens < 0 {
		return nil, fmt.Errorf("decode responses request: negative max_output_tokens %d", *raw.MaxOutputTokens)
	}

	topUnknown := cloneValueMap(raw.Unknown)

	serviceTier, err := decodeServiceTierField(raw.RawServiceTier)
	if err != nil {
		return nil, err
	}

	reasoningCfg, reasoningExtra, err := decodeReasoningAndInclude(raw.Reasoning, raw.Include)
	if err != nil {
		return nil, err
	}
	if len(reasoningExtra.reasoningRaw) > 0 {
		topUnknown = insertRawMember(topUnknown, "reasoning", reasoningExtra.reasoningRaw)
	}
	if len(reasoningExtra.includeRaw) > 0 {
		topUnknown = insertRawMember(topUnknown, "include", reasoningExtra.includeRaw)
	}

	outFmt, textExtra, err := decodeTextConfig(raw.Text)
	if err != nil {
		return nil, err
	}
	if len(textExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "text", textExtra)
	}

	metaUserID, metaExtra, err := decodeRequestMetadata(raw.Metadata)
	if err != nil {
		return nil, err
	}
	if len(metaExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "metadata", metaExtra)
	}

	convID, convSet, convExtra, err := decodeConversationField(raw.Conversation, raw.ConversationID)
	if err != nil {
		return nil, err
	}
	if len(convExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "conversation", convExtra)
	}
	if convSet && raw.ConversationID != "" {
		rawCID, mErr := marshalDeterministic(raw.ConversationID)
		if mErr != nil {
			return nil, mErr
		}
		topUnknown = insertRawMember(topUnknown, "conversation_id", jsontext.Value(rawCID))
	}

	tools, namespaces, err := decodeResponsesTools(raw.Tools)
	if err != nil {
		return nil, err
	}

	messages, liftedTools, liftedNamespaces, err := decodeResponsesInput(raw.Input)
	if err != nil {
		return nil, err
	}
	tools = append(tools, liftedTools...)
	namespaces = append(namespaces, liftedNamespaces...)

	tc, tcExtra, err := decodeResponsesToolChoice(raw.ToolChoice)
	if err != nil {
		return nil, err
	}
	if len(tcExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "tool_choice", tcExtra)
	}

	var sysParts []ir.Part
	if raw.Instructions != nil && *raw.Instructions != "" {
		sysParts = []ir.Part{{
			Kind: ir.PartText,
			Text: *raw.Instructions,
		}}
	}

	req := &ir.Request{
		Model:             raw.Model,
		System:            sysParts,
		Messages:          messages,
		Tools:             tools,
		Namespaces:        namespaces,
		ToolChoice:        tc,
		ParallelToolCalls: raw.ParallelToolCalls,
		Sampling: ir.Sampling{
			Temperature: raw.Temperature,
			TopP:        raw.TopP,
		},
		MaxOutputTokens:    cloneInt64(raw.MaxOutputTokens),
		Reasoning:          reasoningCfg,
		Output:             outFmt,
		Stream:             raw.Stream,
		ServiceTier:        serviceTier,
		CacheKey:           raw.PromptCacheKey,
		PreviousResponseID: raw.PreviousResponseID,
		SessionID:          raw.SessionID,
		ConversationID:     convID,
		ConversationSet:    convSet,
		Store:              cloneBool(raw.Store),
		UserID:             raw.User,
		MetadataUserID:     metaUserID,
		Extra:              toExtra(topUnknown),
	}

	if err := ir.Validate(req); err != nil {
		return nil, fmt.Errorf("validate decoded responses request: %w", err)
	}
	return req, nil
}

// DecodeRequest parses an OpenAI Responses JSON request body into a validated
// ir.Request by delegating directly to the package-level DecodeRequest function.
// Malformed JSON, duplicate object keys, or IR invariant violations return an error.
func (Codec) DecodeRequest(body []byte) (*ir.Request, error) {
	return DecodeRequest(body)
}

// decodeServiceTierField validates a raw service_tier JSON token through
// openai.NormalizeServiceTier and maps the "fast" alias onto "priority".
// Absent or null tokens return ("", nil), whereas explicit blank, non-string,
// or unrecognized tier labels return an *openai.InvalidServiceTierError.
func decodeServiceTierField(raw jsontext.Value) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}
	if trimmed[0] != '"' {
		return "", &openai.InvalidServiceTierError{Value: "<non-string>"}
	}
	var s string
	if err := unmarshalJSONValue(trimmed, &s); err != nil {
		return "", fmt.Errorf("decode service_tier: %w", err)
	}
	trimmedStr := strings.TrimSpace(s)
	if trimmedStr == "" {
		return "", &openai.InvalidServiceTierError{Value: trimmedStr}
	}
	norm := openai.NormalizeServiceTier(trimmedStr)
	if norm == nil {
		return "", &openai.InvalidServiceTierError{Value: trimmedStr}
	}
	return *norm, nil
}

// decodeReasoningAndInclude parses top-level reasoning and include fields into
// ir.ReasoningConfig plus any sub-object or auxiliary include extras.
func decodeReasoningAndInclude(rawReasoning, rawInclude jsontext.Value) (*ir.ReasoningConfig, reasoningSubExtras, error) {
	var extras reasoningSubExtras
	var effort, summary string
	var includeEnc bool

	rTrim := bytes.TrimSpace(rawReasoning)
	if len(rTrim) > 0 && !bytes.Equal(rTrim, []byte("null")) {
		var wr wireReasoningConfig
		if err := unmarshalJSONObject(rTrim, &wr); err != nil {
			return nil, extras, fmt.Errorf("decode reasoning: %w", err)
		}
		effort = strings.TrimSpace(wr.Effort)
		if effort != "" {
			if norm := protocol.NormalizeReasoningEffort(effort); norm != "" {
				effort = norm
			}
		}
		summary = strings.TrimSpace(wr.Summary)
		switch summary {
		case "", "auto", "concise", "detailed":
		default:
			return nil, extras, fmt.Errorf("decode reasoning.summary: unsupported value %q", summary)
		}
		if len(wr.Unknown) > 0 {
			unkBytes, err := marshalUnknownMap(wr.Unknown)
			if err != nil {
				return nil, extras, err
			}
			extras.reasoningRaw = unkBytes
		} else if effort == "" && summary == "" {
			extras.reasoningRaw = jsontext.Value("{}")
		}
	}

	incTrim := bytes.TrimSpace(rawInclude)
	if len(incTrim) > 0 && !bytes.Equal(incTrim, []byte("null")) {
		var items []string
		if err := unmarshalJSONValue(incTrim, &items); err != nil {
			return nil, extras, fmt.Errorf("decode include: %w", err)
		}
		var otherIncludes []string
		for _, entry := range items {
			if entry == "reasoning.encrypted_content" {
				includeEnc = true
				continue
			}
			otherIncludes = append(otherIncludes, entry)
		}
		if len(otherIncludes) > 0 {
			slices.Sort(otherIncludes)
			otherIncludes = slices.Compact(otherIncludes)
			encOther, err := marshalDeterministic(otherIncludes)
			if err != nil {
				return nil, extras, err
			}
			extras.includeRaw = jsontext.Value(encOther)
		} else if !includeEnc {
			extras.includeRaw = jsontext.Value("[]")
		}
	}

	if effort == "" && summary == "" && !includeEnc {
		return nil, extras, nil
	}
	return &ir.ReasoningConfig{
		Effort:           effort,
		Summary:          summary,
		IncludeEncrypted: includeEnc,
	}, extras, nil
}

// decodeTextConfig parses top-level text.format and text.verbosity into
// ir.OutputFormat while capturing unmodeled text or format object fields.
func decodeTextConfig(raw jsontext.Value) (*ir.OutputFormat, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil, nil
	}
	var wt wireTextConfig
	if err := unmarshalJSONObject(trimmed, &wt); err != nil {
		return nil, nil, fmt.Errorf("decode text: %w", err)
	}

	textUnknown := cloneValueMap(wt.Unknown)
	var out ir.OutputFormat
	out.Verbosity = strings.TrimSpace(wt.Verbosity)

	if wt.Format != nil {
		switch wt.Format.Type {
		case "text":
			out.Kind = "text"
		case "json_object":
			out.Kind = "json_object"
		case "json_schema":
			out.Kind = "json_schema"
			out.Name = wt.Format.Name
			out.Description = wt.Format.Description
			out.Strict = cloneBool(wt.Format.Strict)
			sTrim := bytes.TrimSpace(wt.Format.Schema)
			if len(sTrim) > 0 && !bytes.Equal(sTrim, []byte("null")) {
				if !isValidSingleJSON(sTrim) {
					return nil, nil, errors.New("decode text.format.schema: invalid JSON value")
				}
				out.Schema = compactRawJSON(sTrim)
			}
		default:
			return nil, nil, fmt.Errorf("decode text.format: unsupported type %q", wt.Format.Type)
		}
		if len(wt.Format.Unknown) > 0 {
			fmtUnk, err := marshalUnknownMap(wt.Format.Unknown)
			if err != nil {
				return nil, nil, err
			}
			textUnknown = insertRawMember(textUnknown, "format", fmtUnk)
		}
	}

	var extraRaw jsontext.Value
	if len(textUnknown) > 0 {
		var err error
		extraRaw, err = marshalUnknownMap(textUnknown)
		if err != nil {
			return nil, nil, err
		}
	} else if out.Kind == "" && out.Verbosity == "" {
		extraRaw = jsontext.Value("{}")
	}

	if out.Kind == "" && out.Verbosity == "" {
		return nil, extraRaw, nil
	}
	return &out, extraRaw, nil
}

// decodeRequestMetadata extracts metadata.user_id and preserves remaining
// metadata entries or explicit empty objects for lossless round-tripping.
func decodeRequestMetadata(raw jsontext.Value) (string, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil, nil
	}
	var wm wireMetadata
	if err := unmarshalJSONObject(trimmed, &wm); err != nil {
		return "", nil, fmt.Errorf("decode metadata: %w", err)
	}
	if len(wm.Unknown) > 0 {
		unk, err := marshalUnknownMap(wm.Unknown)
		if err != nil {
			return "", nil, err
		}
		return wm.UserID, unk, nil
	}
	if wm.UserID == "" {
		return "", jsontext.Value("{}"), nil
	}
	return wm.UserID, nil, nil
}

// decodeConversationField parses top-level conversation (string or object) and
// fallback conversation_id into ConversationID, ConversationSet, and any
// object-form extra members.
func decodeConversationField(rawConv jsontext.Value, flatConvID string) (string, bool, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(rawConv)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return flatConvID, false, nil, nil
	}
	if trimmed[0] == '"' {
		var cid string
		if err := unmarshalJSONValue(trimmed, &cid); err != nil {
			return "", false, nil, fmt.Errorf("decode conversation string: %w", err)
		}
		return cid, true, nil, nil
	}
	var co wireConversationObject
	if err := unmarshalJSONObject(trimmed, &co); err != nil {
		return "", false, nil, fmt.Errorf("decode conversation object: %w", err)
	}
	if len(co.Unknown) > 0 {
		unk, err := marshalUnknownMap(co.Unknown)
		if err != nil {
			return "", false, nil, err
		}
		return co.ID, true, unk, nil
	}
	if co.ID == "" {
		return "", true, jsontext.Value("{}"), nil
	}
	return co.ID, true, nil, nil
}

// decodeResponsesTools parses top-level tools[] declarations, expanding
// namespace child tools into tools with matching Tool.Namespace values while
// recording Namespace descriptors in namespaces.
func decodeResponsesTools(rawTools []jsontext.Value) ([]ir.Tool, []ir.Namespace, error) {
	if len(rawTools) == 0 {
		return nil, nil, nil
	}
	var tools []ir.Tool
	var namespaces []ir.Namespace
	for i, rawElem := range rawTools {
		elemTrim := bytes.TrimSpace(rawElem)
		var probe wireTypeRoleProbe
		if err := unmarshalJSONObject(elemTrim, &probe); err != nil {
			return nil, nil, fmt.Errorf("decode tools[%d]: %w", i, err)
		}
		if strings.TrimSpace(probe.Type) == "" {
			return nil, nil, fmt.Errorf("decode tools[%d]: missing tool type", i)
		}
		switch probe.Type {
		case "function":
			tool, err := decodeFunctionTool(elemTrim, "", fmt.Sprintf("tools[%d]", i))
			if err != nil {
				return nil, nil, err
			}
			tools = append(tools, tool)

		case "custom":
			tool, err := decodeCustomTool(elemTrim, "", fmt.Sprintf("tools[%d]", i))
			if err != nil {
				return nil, nil, err
			}
			tools = append(tools, tool)

		case "tool_search":
			var wst wireToolSearchTool
			if err := unmarshalJSONObject(elemTrim, &wst); err != nil {
				return nil, nil, fmt.Errorf("decode tools[%d]: %w", i, err)
			}
			tools = append(tools, ir.Tool{
				Kind:         ir.ToolClientSearch,
				Name:         wst.Name,
				Description:  wst.Description,
				Strict:       cloneBool(wst.Strict),
				DeferLoading: cloneBool(wst.DeferLoading),
				Extra:        toExtra(wst.Unknown),
			})

		case "namespace":
			ns, childTools, err := decodeNamespaceTool(elemTrim, fmt.Sprintf("tools[%d]", i))
			if err != nil {
				return nil, nil, err
			}
			namespaces = append(namespaces, ns)
			tools = append(tools, childTools...)

		case "":
			return nil, nil, fmt.Errorf("decode tools[%d]: missing tool type", i)

		default:
			tools = append(tools, ir.Tool{
				Kind: ir.ToolHosted,
				Name: probe.Type,
				Hosted: &ir.OpaqueItem{
					Origin: protocol.ProtocolOpenAIResponses,
					Type:   probe.Type,
					Raw:    compactRawJSON(elemTrim),
				},
			})
		}
	}
	return tools, namespaces, nil
}

// decodeFunctionTool decodes one flat or Chat-nested function declaration at path.
func decodeFunctionTool(raw []byte, namespace, path string) (ir.Tool, error) {
	var wft wireFunctionTool
	if err := unmarshalJSONObject(raw, &wft); err != nil {
		return ir.Tool{}, fmt.Errorf("decode %s: %w", path, err)
	}
	name := wft.Name
	desc := wft.Description
	params := wft.Parameters
	strict := wft.Strict
	if strings.TrimSpace(name) == "" && wft.Function != nil {
		name = wft.Function.Name
		if desc == "" {
			desc = wft.Function.Description
		}
		if len(bytes.TrimSpace(params)) == 0 {
			params = wft.Function.Parameters
		}
		if strict == nil {
			strict = wft.Function.Strict
		}
	}
	if strings.TrimSpace(name) == "" {
		return ir.Tool{}, fmt.Errorf("%s: function tool requires non-empty name", path)
	}
	var schema jsontext.Value
	pTrim := bytes.TrimSpace(params)
	if len(pTrim) > 0 && !bytes.Equal(pTrim, []byte("null")) {
		if !isValidSingleJSON(pTrim) {
			return ir.Tool{}, fmt.Errorf("%s.parameters: invalid JSON value", path)
		}
		schema = compactRawJSON(pTrim)
	}
	return ir.Tool{
		Kind:         ir.ToolFunction,
		Namespace:    namespace,
		Name:         name,
		Description:  desc,
		InputSchema:  schema,
		Strict:       cloneBool(strict),
		DeferLoading: cloneBool(wft.DeferLoading),
		Extra:        toExtra(wft.Unknown),
	}, nil
}

// decodeCustomTool decodes one custom free-form tool declaration at path.
func decodeCustomTool(raw []byte, namespace, path string) (ir.Tool, error) {
	var wct wireCustomTool
	if err := unmarshalJSONObject(raw, &wct); err != nil {
		return ir.Tool{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if strings.TrimSpace(wct.Name) == "" {
		return ir.Tool{}, fmt.Errorf("%s: custom tool requires non-empty name", path)
	}
	var format jsontext.Value
	fTrim := bytes.TrimSpace(wct.Format)
	if len(fTrim) > 0 && !bytes.Equal(fTrim, []byte("null")) {
		if !isValidSingleJSON(fTrim) {
			return ir.Tool{}, fmt.Errorf("%s.format: invalid JSON value", path)
		}
		format = compactRawJSON(fTrim)
	}
	return ir.Tool{
		Kind:         ir.ToolCustom,
		Namespace:    namespace,
		Name:         wct.Name,
		Description:  wct.Description,
		Format:       format,
		DeferLoading: cloneBool(wct.DeferLoading),
		Extra:        toExtra(wct.Unknown),
	}, nil
}

// decodeNamespaceTool decodes a namespace declaration and expands its child
// function and custom declarations with Namespace set to wns.Name.
func decodeNamespaceTool(raw []byte, path string) (ir.Namespace, []ir.Tool, error) {
	var wns wireNamespaceTool
	if err := unmarshalJSONObject(raw, &wns); err != nil {
		return ir.Namespace{}, nil, fmt.Errorf("decode %s: %w", path, err)
	}
	if strings.TrimSpace(wns.Name) == "" {
		return ir.Namespace{}, nil, fmt.Errorf("%s: namespace requires non-empty name", path)
	}
	rawChildren := wns.Tools
	if len(rawChildren) == 0 && len(wns.Children) > 0 {
		rawChildren = wns.Children
	}
	children := make([]ir.Tool, 0, len(rawChildren))
	for cIdx, chRaw := range rawChildren {
		chTrim := bytes.TrimSpace(chRaw)
		cPath := fmt.Sprintf("%s.tools[%d]", path, cIdx)
		var probe wireTypeRoleProbe
		if err := unmarshalJSONObject(chTrim, &probe); err != nil {
			return ir.Namespace{}, nil, fmt.Errorf("decode %s: %w", cPath, err)
		}
		probe.Type = strings.TrimSpace(probe.Type)
		switch probe.Type {
		case "function", "":
			child, err := decodeFunctionTool(chTrim, wns.Name, cPath)
			if err != nil {
				return ir.Namespace{}, nil, err
			}
			children = append(children, child)
		case "custom":
			child, err := decodeCustomTool(chTrim, wns.Name, cPath)
			if err != nil {
				return ir.Namespace{}, nil, err
			}
			children = append(children, child)
		default:
			return ir.Namespace{}, nil, fmt.Errorf("%s: unsupported namespace child tool type %q", cPath, probe.Type)
		}
	}
	ns := ir.Namespace{
		Name:         wns.Name,
		Description:  wns.Description,
		DeferLoading: cloneBool(wns.DeferLoading),
		Extra:        toExtra(wns.Unknown),
	}
	return ns, children, nil
}

// decodeResponsesToolChoice parses string and object tool_choice values into
// ir.ToolChoice alongside any unmodeled object fields.
func decodeResponsesToolChoice(raw jsontext.Value) (*ir.ToolChoice, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil, nil
	}
	if trimmed[0] == '"' {
		var mode string
		if err := unmarshalJSONValue(trimmed, &mode); err != nil {
			return nil, nil, fmt.Errorf("decode tool_choice string: %w", err)
		}
		switch mode {
		case "auto":
			return &ir.ToolChoice{Mode: ir.ToolChoiceAuto}, nil, nil
		case "none":
			return &ir.ToolChoice{Mode: ir.ToolChoiceNone}, nil, nil
		case "required":
			return &ir.ToolChoice{Mode: ir.ToolChoiceRequired}, nil, nil
		default:
			return nil, nil, fmt.Errorf("unsupported tool_choice mode %q", mode)
		}
	}

	var wtc wireToolChoiceObject
	if err := unmarshalJSONObject(trimmed, &wtc); err != nil {
		return nil, nil, fmt.Errorf("decode tool_choice object: %w", err)
	}
	extraBytes, err := marshalUnknownMap(wtc.Unknown)
	if err != nil {
		return nil, nil, err
	}

	if strings.TrimSpace(wtc.Type) == "" {
		return nil, nil, errors.New("tool_choice object requires non-empty type")
	}

	switch wtc.Type {
	case "function":
		name := wtc.Name
		if strings.TrimSpace(name) == "" && wtc.Function != nil {
			name = wtc.Function.Name
		}
		if strings.TrimSpace(name) == "" {
			return nil, nil, errors.New("tool_choice function requires non-empty name")
		}
		return &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{
				Kind:      ir.ToolFunction,
				Namespace: wtc.Namespace,
				Name:      name,
			},
		}, extraBytes, nil

	case "custom":
		if strings.TrimSpace(wtc.Name) == "" {
			return nil, nil, errors.New("tool_choice custom requires non-empty name")
		}
		return &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{
				Kind:      ir.ToolCustom,
				Namespace: wtc.Namespace,
				Name:      wtc.Name,
			},
		}, extraBytes, nil

	case "tool_search":
		name := wtc.Name
		if strings.TrimSpace(name) == "" {
			name = "tool_search"
		}
		return &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{
				Kind: ir.ToolClientSearch,
				Name: name,
			},
		}, extraBytes, nil

	case "allowed_tools":
		modeStr := wtc.Mode
		entries := wtc.Tools
		if wtc.AllowedTools != nil {
			if modeStr == "" {
				modeStr = wtc.AllowedTools.Mode
			}
			if len(entries) == 0 {
				entries = wtc.AllowedTools.Tools
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
			return nil, nil, fmt.Errorf("tool_choice.allowed_tools: unsupported mode %q", modeStr)
		}
		var allowed []ir.ToolRef
		if len(entries) > 0 {
			allowed = make([]ir.ToolRef, 0, len(entries))
			for idx, ent := range entries {
				ref, err := decodeAllowedToolEntry(ent, idx)
				if err != nil {
					return nil, nil, err
				}
				allowed = append(allowed, ref)
			}
		}
		return &ir.ToolChoice{
			Mode:         mode,
			AllowedTools: allowed,
		}, extraBytes, nil

	default:
		return &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{
				Kind: ir.ToolHosted,
				Name: wtc.Type,
			},
		}, extraBytes, nil
	}
}

// decodeAllowedToolEntry converts one allowed_tools[] item into an ir.ToolRef.
func decodeAllowedToolEntry(ent wireAllowedToolEntry, idx int) (ir.ToolRef, error) {
	if strings.TrimSpace(ent.Type) == "" {
		return ir.ToolRef{}, fmt.Errorf("tool_choice.tools[%d]: missing type", idx)
	}
	switch ent.Type {
	case "function":
		name := ent.Name
		if strings.TrimSpace(name) == "" && ent.Function != nil {
			name = ent.Function.Name
		}
		if strings.TrimSpace(name) == "" {
			return ir.ToolRef{}, fmt.Errorf("tool_choice.tools[%d]: function requires non-empty name", idx)
		}
		return ir.ToolRef{Kind: ir.ToolFunction, Namespace: ent.Namespace, Name: name}, nil
	case "custom":
		if strings.TrimSpace(ent.Name) == "" {
			return ir.ToolRef{}, fmt.Errorf("tool_choice.tools[%d]: custom requires non-empty name", idx)
		}
		return ir.ToolRef{Kind: ir.ToolCustom, Namespace: ent.Namespace, Name: ent.Name}, nil
	case "tool_search":
		return ir.ToolRef{Kind: ir.ToolClientSearch, Name: ent.Name}, nil
	case "":
		return ir.ToolRef{}, fmt.Errorf("tool_choice.tools[%d]: missing type", idx)
	default:
		return ir.ToolRef{Kind: ir.ToolHosted, Name: ent.Type}, nil
	}
}

// decodeResponsesInput parses a string shorthand or flat item-array input field
// into ordered ir.Message turns alongside any tools lifted from additional_tools.
//
// Consecutive metadata-free assistant items (reasoning, assistant messages,
// tool calls, and opaque assistant items) coalesce into one RoleAssistant turn,
// while consecutive *_output items coalesce into one RoleTool turn in exact
// wire order. User, system, and developer message items each remain in their
// own turn so EncodeRequest reproduces the original flat input[] sequence.
func decodeResponsesInput(raw jsontext.Value) ([]ir.Message, []ir.Tool, []ir.Namespace, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil, nil, errors.New("decode responses input: input is required and must be a string or array")
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return nil, nil, nil, fmt.Errorf("decode responses input string: %w", err)
		}
		var parts []ir.Part
		if s != "" {
			parts = []ir.Part{{Kind: ir.PartText, Text: s}}
		} else {
			parts = []ir.Part{}
		}
		return []ir.Message{{
			Role:  ir.RoleUser,
			Parts: parts,
		}}, nil, nil, nil
	}
	if trimmed[0] != '[' {
		return nil, nil, nil, errors.New("decode responses input: must be a string or array of items")
	}

	var rawItems []jsontext.Value
	if err := unmarshalJSONValue(trimmed, &rawItems); err != nil {
		return nil, nil, nil, fmt.Errorf("decode responses input array: %w", err)
	}
	if len(rawItems) == 0 {
		return []ir.Message{}, nil, nil, nil
	}

	messages := []ir.Message{}
	var liftedTools []ir.Tool
	var liftedNamespaces []ir.Namespace

	// flushOrAppendAssistant appends part onto an active metadata-free assistant
	// turn or opens a new RoleAssistant turn when the preceding turn is non-assistant
	// or carries isolated message-level ID/Extra metadata.
	var openAssistantMergeable bool
	var openToolMergeable bool

	appendAssistantPart := func(p ir.Part) {
		openToolMergeable = false
		if openAssistantMergeable && len(messages) > 0 && messages[len(messages)-1].Role == ir.RoleAssistant {
			messages[len(messages)-1].Parts = append(messages[len(messages)-1].Parts, p)
			return
		}
		messages = append(messages, ir.Message{
			Role:  ir.RoleAssistant,
			Parts: []ir.Part{p},
		})
		openAssistantMergeable = true
	}

	appendToolResultPart := func(p ir.Part) {
		openAssistantMergeable = false
		if openToolMergeable && len(messages) > 0 && messages[len(messages)-1].Role == ir.RoleTool {
			messages[len(messages)-1].Parts = append(messages[len(messages)-1].Parts, p)
			return
		}
		messages = append(messages, ir.Message{
			Role:  ir.RoleTool,
			Parts: []ir.Part{p},
		})
		openToolMergeable = true
	}

	for idx, elem := range rawItems {
		elemTrim := bytes.TrimSpace(elem)
		iPath := fmt.Sprintf("input[%d]", idx)
		var probe wireTypeRoleProbe
		if err := unmarshalJSONObject(elemTrim, &probe); err != nil {
			return nil, nil, nil, fmt.Errorf("decode %s: %w", iPath, err)
		}
		itemType := probe.Type
		if itemType == "" && strings.TrimSpace(probe.Role) != "" {
			itemType = "message"
		}
		if strings.TrimSpace(itemType) == "" {
			return nil, nil, nil, fmt.Errorf("decode %s: missing item type and role", iPath)
		}

		switch itemType {
		case "message":
			msg, mergeIntoAssistant, err := decodeInputMessageItem(elemTrim, iPath)
			if err != nil {
				return nil, nil, nil, err
			}
			if mergeIntoAssistant {
				for _, p := range msg.Parts {
					appendAssistantPart(p)
				}
			} else {
				openAssistantMergeable = false
				openToolMergeable = false
				messages = append(messages, msg)
			}

		case "reasoning":
			part, err := decodeInputReasoningItem(elemTrim, iPath)
			if err != nil {
				return nil, nil, nil, err
			}
			appendAssistantPart(part)

		case "function_call":
			part, err := decodeInputFunctionCallItem(elemTrim, iPath)
			if err != nil {
				return nil, nil, nil, err
			}
			appendAssistantPart(part)

		case "custom_tool_call":
			part, err := decodeInputCustomCallItem(elemTrim, iPath)
			if err != nil {
				return nil, nil, nil, err
			}
			appendAssistantPart(part)

		case "tool_search_call":
			part, err := decodeInputToolSearchCallItem(elemTrim, iPath)
			if err != nil {
				return nil, nil, nil, err
			}
			appendAssistantPart(part)

		case "function_call_output", "custom_tool_call_output", "tool_search_output":
			part, err := decodeInputCallOutputItem(elemTrim, itemType, iPath)
			if err != nil {
				return nil, nil, nil, err
			}
			appendToolResultPart(part)

		case "additional_tools":
			var wat wireAdditionalToolsItem
			if err := unmarshalJSONObject(elemTrim, &wat); err != nil {
				return nil, nil, nil, fmt.Errorf("decode %s: %w", iPath, err)
			}
			moreTools, moreNS, err := decodeResponsesTools(wat.Tools)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("decode %s.tools: %w", iPath, err)
			}
			liftedTools = append(liftedTools, moreTools...)
			liftedNamespaces = append(liftedNamespaces, moreNS...)

		case "":
			return nil, nil, nil, fmt.Errorf("decode %s: missing item type and role", iPath)

		default:
			appendAssistantPart(ir.Part{
				Kind: ir.PartOpaque,
				ID:   probe.ID,
				Opaque: &ir.OpaqueItem{
					Origin: protocol.ProtocolOpenAIResponses,
					Type:   itemType,
					Raw:    compactRawJSON(elemTrim),
				},
			})
		}
	}

	return messages, liftedTools, liftedNamespaces, nil
}

// decodeInputMessageItem decodes a user, system, developer, or assistant message
// item from input[]. Non-empty assistant messages without item-level Extra fields
// report mergeIntoAssistant=true so adjacent reasoning, message, and call items
// share one RoleAssistant turn.
func decodeInputMessageItem(raw []byte, path string) (ir.Message, bool, error) {
	var wm wireMessageItem
	if err := unmarshalJSONObject(raw, &wm); err != nil {
		return ir.Message{}, false, fmt.Errorf("decode %s: %w", path, err)
	}
	var role ir.Role
	switch wm.Role {
	case "user":
		role = ir.RoleUser
	case "assistant":
		role = ir.RoleAssistant
	case "system":
		role = ir.RoleSystem
	case "developer":
		role = ir.RoleDeveloper
	default:
		return ir.Message{}, false, fmt.Errorf("decode %s: unsupported message role %q", path, wm.Role)
	}

	parts, err := decodeMessageContent(wm.Content, role, path+".content")
	if err != nil {
		return ir.Message{}, false, err
	}

	if role == ir.RoleAssistant && len(wm.Unknown) == 0 && len(parts) > 0 {
		if wm.ID != "" {
			for i := range parts {
				if parts[i].ID == "" {
					parts[i].ID = wm.ID
				}
			}
		}
		return ir.Message{
			Role:  role,
			Parts: parts,
		}, true, nil
	}

	return ir.Message{
		Role:  role,
		ID:    wm.ID,
		Parts: parts,
		Extra: toExtra(wm.Unknown),
	}, false, nil
}

// decodeMessageContent parses a string shorthand or content-part array on a message item.
func decodeMessageContent(raw jsontext.Value, role ir.Role, path string) ([]ir.Part, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return []ir.Part{}, nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return nil, fmt.Errorf("decode %s string: %w", path, err)
		}
		if s == "" {
			return []ir.Part{}, nil
		}
		return []ir.Part{{Kind: ir.PartText, Text: s}}, nil
	}
	if trimmed[0] != '[' {
		return nil, fmt.Errorf("decode %s: must be string or array", path)
	}

	var rawBlocks []jsontext.Value
	if err := unmarshalJSONValue(trimmed, &rawBlocks); err != nil {
		return nil, fmt.Errorf("decode %s array: %w", path, err)
	}
	parts := make([]ir.Part, 0, len(rawBlocks))
	for i, elem := range rawBlocks {
		elemTrim := bytes.TrimSpace(elem)
		bPath := fmt.Sprintf("%s[%d]", path, i)
		part, err := decodeContentPart(elemTrim, role, false, bPath)
		if err != nil {
			return nil, err
		}
		parts = append(parts, part)
	}
	return parts, nil
}

// decodeContentPart parses one content block inside message.content[] or *_output.output[].
func decodeContentPart(raw []byte, role ir.Role, insideToolResult bool, path string) (ir.Part, error) {
	var wp wireContentPart
	if err := unmarshalJSONObject(raw, &wp); err != nil {
		return ir.Part{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if strings.TrimSpace(wp.Type) == "" {
		return ir.Part{}, fmt.Errorf("decode %s: empty content block type", path)
	}
	var err error

	switch wp.Type {
	case "input_text", "output_text", "text":
		var anns []ir.Annotation
		if len(wp.Annotations) > 0 {
			anns, err = decodeAnnotations(wp.Annotations, path+".annotations")
			if err != nil {
				return ir.Part{}, err
			}
		}
		return ir.Part{
			Kind:        ir.PartText,
			Text:        wp.Text,
			Annotations: anns,
			Extra:       toExtra(wp.Unknown),
		}, nil

	case "refusal":
		if insideToolResult || (role != ir.RoleAssistant && role != "") {
			return ir.Part{}, fmt.Errorf("decode %s: refusal part is only valid on assistant messages", path)
		}
		return ir.Part{
			Kind:  ir.PartRefusal,
			Text:  wp.Refusal,
			Extra: toExtra(wp.Unknown),
		}, nil

	case "input_image":
		if role == ir.RoleAssistant {
			return ir.Part{}, fmt.Errorf("decode %s: input_image is not valid on assistant messages", path)
		}
		media, err := decodeInputImagePart(wp, path)
		if err != nil {
			return ir.Part{}, err
		}
		return ir.Part{
			Kind:  ir.PartMedia,
			Media: media,
			Extra: toExtra(wp.Unknown),
		}, nil

	case "input_file":
		if role == ir.RoleAssistant {
			return ir.Part{}, fmt.Errorf("decode %s: input_file is not valid on assistant messages", path)
		}
		media, err := decodeInputFilePart(wp, path)
		if err != nil {
			return ir.Part{}, err
		}
		return ir.Part{
			Kind:  ir.PartMedia,
			Media: media,
			Extra: toExtra(wp.Unknown),
		}, nil

	case "input_audio":
		if role == ir.RoleAssistant {
			return ir.Part{}, fmt.Errorf("decode %s: input_audio is not valid on assistant messages", path)
		}
		if wp.InputAudio == nil || wp.InputAudio.Data == "" {
			return ir.Part{}, fmt.Errorf("decode %s: input_audio requires non-empty data", path)
		}
		return ir.Part{
			Kind: ir.PartMedia,
			Media: &ir.Media{
				Type:   ir.MediaAudio,
				MIME:   mimeFromAudioFormat(wp.InputAudio.Format),
				Source: ir.MediaSourceBase64,
				Data:   wp.InputAudio.Data,
			},
			Extra: toExtra(wp.Unknown),
		}, nil

	default:
		if insideToolResult {
			return ir.Part{}, fmt.Errorf("decode %s: unsupported tool_result content block type %q", path, wp.Type)
		}
		if role == ir.RoleAssistant {
			return ir.Part{}, fmt.Errorf("decode %s: unsupported assistant message content block type %q", path, wp.Type)
		}
		if reservedItemTypes[wp.Type] {
			return ir.Part{}, fmt.Errorf("decode %s: reserved top-level item type %q cannot appear inside message content", path, wp.Type)
		}
		var idProbe wireTypeRoleProbe
		// raw already unmarshaled cleanly into wireContentPart above; probing id
		// keeps PartOpaque.ID aligned with Raw when user content wraps vendor blocks.
		_ = unmarshalJSONObject(raw, &idProbe)
		return ir.Part{
			Kind: ir.PartOpaque,
			ID:   idProbe.ID,
			Opaque: &ir.OpaqueItem{
				Origin: protocol.ProtocolOpenAIResponses,
				Type:   wp.Type,
				Raw:    compactRawJSON(raw),
			},
		}, nil
	}
}

// decodeInputImagePart parses an input_image content block into an ir.Media value.
func decodeInputImagePart(wp wireContentPart, path string) (*ir.Media, error) {
	detail := wp.Detail
	imgTrim := bytes.TrimSpace(wp.ImageURL)
	if len(imgTrim) > 0 && !bytes.Equal(imgTrim, []byte("null")) {
		var rawURL string
		if imgTrim[0] == '"' {
			if err := unmarshalJSONValue(imgTrim, &rawURL); err != nil {
				return nil, fmt.Errorf("decode %s.image_url: %w", path, err)
			}
		} else {
			var obj wireImageURLObject
			if err := unmarshalJSONObject(imgTrim, &obj); err != nil {
				return nil, fmt.Errorf("decode %s.image_url object: %w", path, err)
			}
			rawURL = obj.URL
			if detail == "" {
				detail = obj.Detail
			}
		}
		if rawURL == "" {
			return nil, fmt.Errorf("decode %s: empty image_url", path)
		}
		mime, b64, isData, valid := parseBase64DataURI(rawURL)
		if isData {
			if !valid {
				return nil, fmt.Errorf("decode %s: malformed base64 data URI", path)
			}
			return &ir.Media{
				Type:   ir.MediaImage,
				MIME:   mime,
				Source: ir.MediaSourceBase64,
				Data:   b64,
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
	if wp.FileID != "" {
		return &ir.Media{
			Type:   ir.MediaImage,
			Source: ir.MediaSourceFileID,
			Data:   wp.FileID,
			Detail: detail,
		}, nil
	}
	return nil, fmt.Errorf("decode %s: input_image requires image_url or file_id", path)
}

// decodeInputFilePart parses an input_file content block into a document ir.Media value.
func decodeInputFilePart(wp wireContentPart, path string) (*ir.Media, error) {
	if wp.FileData != "" {
		mime, b64, isData, valid := parseBase64DataURI(wp.FileData)
		if isData {
			if !valid {
				return nil, fmt.Errorf("decode %s: malformed file_data data URI", path)
			}
			return &ir.Media{
				Type:   ir.MediaDocument,
				MIME:   mime,
				Source: ir.MediaSourceBase64,
				Data:   b64,
				Name:   wp.Filename,
			}, nil
		}
		return &ir.Media{
			Type:   ir.MediaDocument,
			MIME:   "application/pdf",
			Source: ir.MediaSourceBase64,
			Data:   wp.FileData,
			Name:   wp.Filename,
		}, nil
	}
	if wp.FileURL != "" {
		return &ir.Media{
			Type:   ir.MediaDocument,
			Source: ir.MediaSourceURL,
			Data:   wp.FileURL,
			Name:   wp.Filename,
		}, nil
	}
	if wp.FileID != "" {
		return &ir.Media{
			Type:   ir.MediaDocument,
			Source: ir.MediaSourceFileID,
			Data:   wp.FileID,
			Name:   wp.Filename,
		}, nil
	}
	return nil, fmt.Errorf("decode %s: input_file requires file_data, file_url, or file_id", path)
}

// decodeAnnotations converts wire citation annotations on output_text into ir.Annotation slices.
func decodeAnnotations(rawAnns []wireAnnotation, path string) ([]ir.Annotation, error) {
	if len(rawAnns) == 0 {
		return nil, nil
	}
	out := make([]ir.Annotation, 0, len(rawAnns))
	for i, wa := range rawAnns {
		aPath := fmt.Sprintf("%s[%d]", path, i)
		if strings.TrimSpace(wa.Type) == "" {
			return nil, fmt.Errorf("%s: annotation requires non-empty type", aPath)
		}
		url := wa.URL
		title := wa.Title
		startIdx := wa.StartIndex
		endIdx := wa.EndIndex
		if wa.URLCitation != nil {
			if url == "" {
				url = wa.URLCitation.URL
			}
			if title == "" {
				title = wa.URLCitation.Title
			}
			if startIdx == nil {
				startIdx = wa.URLCitation.StartIndex
			}
			if endIdx == nil {
				endIdx = wa.URLCitation.EndIndex
			}
		}
		if startIdx != nil && *startIdx < 0 {
			return nil, fmt.Errorf("%s.start_index: negative offset %d", aPath, *startIdx)
		}
		if endIdx != nil && *endIdx < 0 {
			return nil, fmt.Errorf("%s.end_index: negative offset %d", aPath, *endIdx)
		}
		if startIdx != nil && endIdx != nil && *endIdx < *startIdx {
			return nil, fmt.Errorf("%s: end_index %d precedes start_index %d", aPath, *endIdx, *startIdx)
		}
		out = append(out, ir.Annotation{
			Kind:       wa.Type,
			URL:        url,
			Title:      title,
			StartIndex: cloneInt64(startIdx),
			EndIndex:   cloneInt64(endIdx),
			Extra:      toExtra(wa.Unknown),
		})
	}
	return out, nil
}

// decodeInputReasoningItem parses a reasoning item inside Responses input[],
// retaining secondary reasoning_text content inside Extra when summary is also present.
func decodeInputReasoningItem(raw []byte, path string) (ir.Part, error) {
	var wr wireReasoningItem
	if err := unmarshalJSONObject(raw, &wr); err != nil {
		return ir.Part{}, fmt.Errorf("decode %s: %w", path, err)
	}
	for i, seg := range wr.Summary {
		if seg.Type != "" && seg.Type != "summary_text" {
			return ir.Part{}, fmt.Errorf("decode %s.summary[%d]: unsupported type %q", path, i, seg.Type)
		}
	}
	for i, seg := range wr.Content {
		if seg.Type != "" && seg.Type != "reasoning_text" {
			return ir.Part{}, fmt.Errorf("decode %s.content[%d]: unsupported type %q", path, i, seg.Type)
		}
	}

	unk := cloneValueMap(wr.Unknown)
	var segments []string
	var isSummary bool
	if len(wr.Summary) > 0 {
		isSummary = true
		segments = make([]string, 0, len(wr.Summary))
		for _, seg := range wr.Summary {
			segments = append(segments, seg.Text)
		}
		if len(wr.Content) > 0 {
			rawCnt, err := marshalDeterministic(wr.Content)
			if err != nil {
				return ir.Part{}, err
			}
			unk = insertRawMember(unk, "content", jsontext.Value(rawCnt))
		}
	} else if len(wr.Content) > 0 {
		segments = make([]string, 0, len(wr.Content))
		for _, seg := range wr.Content {
			segments = append(segments, seg.Text)
		}
	}
	var tok *ir.ReplayToken
	if wr.EncryptedContent != "" {
		tok = &ir.ReplayToken{Data: wr.EncryptedContent}
	}
	return ir.Part{
		Kind: ir.PartReasoning,
		ID:   wr.ID,
		Reasoning: &ir.Reasoning{
			Segments: segments,
			Summary:  isSummary,
			Token:    tok,
		},
		Extra: toExtra(unk),
	}, nil
}

// decodeInputFunctionCallItem parses a function_call item inside Responses input[].
func decodeInputFunctionCallItem(raw []byte, path string) (ir.Part, error) {
	var wfc wireFunctionCallItem
	if err := unmarshalJSONObject(raw, &wfc); err != nil {
		return ir.Part{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if strings.TrimSpace(wfc.Name) == "" {
		return ir.Part{}, fmt.Errorf("decode %s: function_call requires non-empty name", path)
	}
	args, err := decodeRequestFunctionArgs(wfc.RawArgs, path+".arguments")
	if err != nil {
		return ir.Part{}, err
	}
	return ir.Part{
		Kind: ir.PartToolCall,
		ID:   wfc.ID,
		Call: &ir.ToolCall{
			CallID: wfc.CallID,
			Tool: ir.ToolRef{
				Kind:      ir.ToolFunction,
				Namespace: wfc.Namespace,
				Name:      wfc.Name,
			},
			Arguments: args,
		},
		Extra: toExtra(wfc.Unknown),
	}, nil
}

// decodeRequestFunctionArgs extracts a string or raw JSON argument payload from
// a request function_call item. Absent, null, or whitespace-only string values
// normalize to "{}", while malformed JSON strings or non-JSON values return an error.
func decodeRequestFunctionArgs(rawArgs jsontext.Value, path string) (string, error) {
	trimmed := bytes.TrimSpace(rawArgs)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "{}", nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return "", fmt.Errorf("decode %s: %w", path, err)
		}
		if strings.TrimSpace(s) == "" {
			return "{}", nil
		}
		return s, nil
	}
	if !isValidSingleJSON(trimmed) {
		return "", fmt.Errorf("decode %s: invalid JSON arguments", path)
	}
	return string(compactRawJSON(trimmed)), nil
}

// decodeInputCustomCallItem parses a custom_tool_call item inside Responses input[].
func decodeInputCustomCallItem(raw []byte, path string) (ir.Part, error) {
	var wcc wireCustomToolCallItem
	if err := unmarshalJSONObject(raw, &wcc); err != nil {
		return ir.Part{}, fmt.Errorf("decode %s: %w", path, err)
	}
	if strings.TrimSpace(wcc.Name) == "" {
		return ir.Part{}, fmt.Errorf("decode %s: custom_tool_call requires non-empty name", path)
	}
	return ir.Part{
		Kind: ir.PartToolCall,
		ID:   wcc.ID,
		Call: &ir.ToolCall{
			CallID: wcc.CallID,
			Tool: ir.ToolRef{
				Kind:      ir.ToolCustom,
				Namespace: wcc.Namespace,
				Name:      wcc.Name,
			},
			Arguments: wcc.Input,
		},
		Extra: toExtra(wcc.Unknown),
	}, nil
}

// decodeInputToolSearchCallItem parses a tool_search_call item inside Responses input[].
func decodeInputToolSearchCallItem(raw []byte, path string) (ir.Part, error) {
	var wtsc wireToolSearchCallItem
	if err := unmarshalJSONObject(raw, &wtsc); err != nil {
		return ir.Part{}, fmt.Errorf("decode %s: %w", path, err)
	}
	args, err := decodeToolSearchArguments(wtsc.Arguments, path+".arguments")
	if err != nil {
		return ir.Part{}, err
	}
	return ir.Part{
		Kind: ir.PartToolCall,
		ID:   wtsc.ID,
		Call: &ir.ToolCall{
			CallID:    wtsc.CallID,
			Tool:      ir.ToolRef{Kind: ir.ToolClientSearch},
			Arguments: args,
		},
		Extra: toExtra(wtsc.Unknown),
	}, nil
}

// decodeToolSearchArguments normalizes object-shaped or JSON-string arguments on
// a request tool_search_call item into compacted JSON object text. Absent, null,
// or blank string inputs return "", whereas non-object JSON values return an error.
func decodeToolSearchArguments(raw jsontext.Value, path string) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}
	if trimmed[0] == '"' {
		// Unpack stringified JSON objects sent by compatible clients while rejecting
		// JSON string scalars that cannot round-trip as inline object arguments.
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return "", fmt.Errorf("decode %s: %w", path, err)
		}
		sTrim := strings.TrimSpace(s)
		if sTrim == "" {
			return "", nil
		}
		if !isValidSingleJSONObject([]byte(sTrim)) {
			return "", fmt.Errorf("decode %s: tool_search_call string arguments must be a JSON object", path)
		}
		return string(compactRawJSON(jsontext.Value(sTrim))), nil
	}
	if !isValidSingleJSONObject(trimmed) {
		return "", fmt.Errorf("decode %s: tool_search_call arguments must be a JSON object", path)
	}
	return string(compactRawJSON(trimmed)), nil
}

// decodeInputCallOutputItem parses function_call_output, custom_tool_call_output,
// and tool_search_output items into RoleTool ir.PartToolResult parts while
// leaving ToolResult.Tool empty for ir.Link resolution per section 3.3.
func decodeInputCallOutputItem(raw []byte, itemType, path string) (ir.Part, error) {
	var wco wireCallOutputItem
	if err := unmarshalJSONObject(raw, &wco); err != nil {
		return ir.Part{}, fmt.Errorf("decode %s: %w", path, err)
	}

	outUnknown := cloneValueMap(wco.Unknown)
	outTrim := bytes.TrimSpace(wco.Output)
	toolsTrim := bytes.TrimSpace(wco.Tools)
	hasOutput := len(outTrim) > 0 && !bytes.Equal(outTrim, []byte("null"))
	hasTools := len(toolsTrim) > 0 && !bytes.Equal(toolsTrim, []byte("null"))

	var content []ir.Part
	var isError bool

	if hasOutput {
		var err error
		content, isError, err = decodeCallOutputValue(outTrim, path+".output")
		if err != nil {
			return ir.Part{}, err
		}
		if hasTools {
			outUnknown = insertRawMember(outUnknown, "tools", toolsTrim)
		}
	} else if itemType == "tool_search_output" && hasTools {
		if toolsTrim[0] != '[' || !isValidSingleJSON(toolsTrim) {
			return ir.Part{}, fmt.Errorf("decode %s.tools: must be a valid JSON array", path)
		}
		content = []ir.Part{{
			Kind: ir.PartText,
			Text: string(compactRawJSON(toolsTrim)),
		}}
	} else if hasTools {
		outUnknown = insertRawMember(outUnknown, "tools", toolsTrim)
	}

	return ir.Part{
		Kind: ir.PartToolResult,
		ID:   wco.ID,
		Result: &ir.ToolResult{
			CallID:  wco.CallID,
			Content: content,
			IsError: isError,
		},
		Extra: toExtra(outUnknown),
	}, nil
}

// decodeCallOutputValue parses a string or content-part array output field on a
// *_output item, unwrapping exact two-key {"error": "...", "is_error": true} envelopes.
func decodeCallOutputValue(trimmed []byte, path string) ([]ir.Part, bool, error) {
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return nil, false, fmt.Errorf("decode %s string: %w", path, err)
		}
		if errMsg, ok := unwrapToolErrorString(s); ok {
			var content []ir.Part
			if errMsg != "" {
				content = []ir.Part{{Kind: ir.PartText, Text: errMsg}}
			}
			return content, true, nil
		}
		if s == "" {
			return nil, false, nil
		}
		return []ir.Part{{Kind: ir.PartText, Text: s}}, false, nil
	}
	if trimmed[0] != '[' {
		return nil, false, fmt.Errorf("decode %s: must be string or array", path)
	}
	var rawBlocks []jsontext.Value
	if err := unmarshalJSONValue(trimmed, &rawBlocks); err != nil {
		return nil, false, fmt.Errorf("decode %s array: %w", path, err)
	}
	if len(rawBlocks) == 0 {
		return nil, false, nil
	}
	out := make([]ir.Part, 0, len(rawBlocks))
	for i, elem := range rawBlocks {
		part, err := decodeContentPart(bytes.TrimSpace(elem), ir.RoleTool, true, fmt.Sprintf("%s[%d]", path, i))
		if err != nil {
			return nil, false, err
		}
		out = append(out, part)
	}
	return out, false, nil
}

// unwrapToolErrorString inspects s for the exact two-key {"error": "<msg>",
// "is_error": true} envelope used to transport structured tool errors over string outputs.
func unwrapToolErrorString(s string) (string, bool) {
	trimmed := strings.TrimSpace(s)
	if !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
		return "", false
	}
	var env wireErrorToolEnvelope
	// Ordinary non-JSON or multi-key JSON tool outputs are not error envelopes
	// and retain their raw string contents untouched when unmarshaling fails.
	if unmarshalJSONObject([]byte(trimmed), &env) != nil {
		return "", false
	}
	if env.Error == nil || env.IsError == nil || !*env.IsError || len(env.Unknown) > 0 {
		return "", false
	}
	return *env.Error, true
}

// mimeFromAudioFormat maps short OpenAI audio format labels onto canonical MIME strings.
func mimeFromAudioFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "mp3", "mpeg":
		return "audio/mpeg"
	case "wav":
		return "audio/wav"
	case "ogg":
		return "audio/ogg"
	case "flac":
		return "audio/flac"
	case "pcm16", "pcm":
		return "audio/pcm"
	default:
		clean := strings.ToLower(strings.TrimSpace(format))
		if clean == "" {
			return "audio/wav"
		}
		if strings.Contains(clean, "/") {
			return clean
		}
		return "audio/" + clean
	}
}

// audioFormatFromMIME extracts a short OpenAI input_audio format label from mime.
func audioFormatFromMIME(mime string) string {
	switch strings.ToLower(strings.TrimSpace(mime)) {
	case "audio/mpeg", "audio/mp3":
		return "mp3"
	case "audio/wav", "audio/x-wav", "":
		return "wav"
	default:
		clean := strings.ToLower(strings.TrimSpace(mime))
		if _, sub, ok := strings.Cut(clean, "/"); ok && sub != "" {
			return sub
		}
		return clean
	}
}

// cloneBool returns a fresh copy of *p, or nil when p is nil.
func cloneBool(p *bool) *bool {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// cloneInt64 returns a fresh copy of *p, or nil when p is nil.
func cloneInt64(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
