package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// request_decode.go parses Anthropic Messages JSON request bodies into
// normalized ir.Request structures without losing wire information. Scalar or
// block-array system instructions, alternating user and assistant turns, mixed
// tool_result and user media blocks, extended thinking and redacted_thinking
// blocks, callable and hosted server tools, and structured output formats map
// onto canonical IR fields while unmodeled members land in Extra and
// protocol-specific blocks become OpaqueItem values.
//
// Decoding tolerates invalid UTF-8 sequences inside JSON strings while keeping
// json/v2 duplicate-key rejection enabled on every object. User messages mixing
// tool_result blocks with text or media split into contiguous RoleTool and
// RoleUser turns in original block appearance order so same-protocol round
// trips reconstruct the exact wire block sequence.

// arrayDecoderPool reuses jsontext.Decoder instances and bytes.Reader state
// when slicing JSON content arrays into borrowed element spans without cloning.
var arrayDecoderPool = sync.Pool{
	New: newPooledArrayDecoder,
}

// pooledArrayDecoder bundles a reusable bytes.Reader and jsontext.Decoder so
// content-array span extraction avoids per-message decoder buffer allocations.
type pooledArrayDecoder struct {
	reader bytes.Reader      // resettable byte reader backed by the caller slice
	dec    *jsontext.Decoder // token scanner configured with AllowInvalidUTF8(true)
}

// newPooledArrayDecoder allocates an initialized pooledArrayDecoder configured
// with AllowInvalidUTF8(true) for reuse inside arrayDecoderPool.
func newPooledArrayDecoder() any {
	p := &pooledArrayDecoder{}
	p.dec = jsontext.NewDecoder(&p.reader, jsontext.AllowInvalidUTF8(true))
	return p
}

// DecodeRequest parses an Anthropic Messages JSON request body into a validated
// ir.Request. Malformed JSON, duplicate object member names, non-object roots,
// invalid cache_control or thinking settings, non-object tool schemas or tool
// inputs, or IR structural violations return a descriptive error.
func DecodeRequest(body []byte) (*ir.Request, error) {
	var raw wireInboundRequest
	if err := unmarshalJSONObject(body, &raw); err != nil {
		return nil, fmt.Errorf("decode messages request: %w", err)
	}

	if raw.MaxTokens != nil && *raw.MaxTokens < 0 {
		return nil, fmt.Errorf("decode messages request: negative max_tokens %d", *raw.MaxTokens)
	}
	if raw.TopK != nil && *raw.TopK < 0 {
		return nil, fmt.Errorf("decode messages request: negative top_k %d", *raw.TopK)
	}

	topUnknown := cloneValueMap(raw.Unknown)

	sysParts, err := decodeSystemField(raw.System)
	if err != nil {
		return nil, err
	}

	thinkingMode, thinkingBudget, thinkingExtra, err := decodeThinkingField(raw.Thinking)
	if err != nil {
		return nil, err
	}
	if len(thinkingExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "thinking", thinkingExtra)
	}

	effort, outFmt, ocExtra, err := decodeOutputConfigField(raw.OutputConfig)
	if err != nil {
		return nil, err
	}
	if len(ocExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "output_config", ocExtra)
	}

	metaUserID, metaExtra, err := decodeMetadataField(raw.Metadata)
	if err != nil {
		return nil, err
	}
	if len(metaExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "metadata", metaExtra)
	}

	serviceTier, keepSpeedRaw, err := decodeSpeedField(raw.RawSpeed)
	if err != nil {
		return nil, err
	}
	if keepSpeedRaw {
		topUnknown = insertRawMember(topUnknown, "speed", raw.RawSpeed)
	}

	tools, err := decodeMessagesTools(raw.Tools)
	if err != nil {
		return nil, err
	}

	tc, parallelCalls, tcExtra, err := decodeMessagesToolChoice(raw.ToolChoice)
	if err != nil {
		return nil, err
	}
	if len(tcExtra) > 0 {
		topUnknown = insertRawMember(topUnknown, "tool_choice", tcExtra)
	}

	var maxOut *int64
	if raw.MaxTokens != nil {
		v := *raw.MaxTokens
		maxOut = &v
	}

	var stopSeqs []string
	if len(raw.StopSequences) > 0 {
		stopSeqs = slices.Clone(raw.StopSequences)
	}

	var reasoning *ir.ReasoningConfig
	if thinkingMode != "" || thinkingBudget != nil || effort != "" {
		reasoning = &ir.ReasoningConfig{
			Mode:         thinkingMode,
			Effort:       effort,
			BudgetTokens: thinkingBudget,
		}
	}

	req := &ir.Request{
		Model:             raw.Model,
		System:            sysParts,
		Messages:          make([]ir.Message, 0, len(raw.Messages)),
		Tools:             tools,
		ToolChoice:        tc,
		ParallelToolCalls: parallelCalls,
		Sampling: ir.Sampling{
			Temperature: raw.Temperature,
			TopP:        raw.TopP,
			TopK:        raw.TopK,
		},
		MaxOutputTokens: maxOut,
		Stop:            stopSeqs,
		Reasoning:       reasoning,
		Output:          outFmt,
		Stream:          raw.Stream,
		ServiceTier:     serviceTier,
		CacheKey:        raw.PromptCacheKey,
		SessionID:       raw.SessionID,
		ConversationID:  raw.ConversationID,
		UserID:          metaUserID,
		MetadataUserID:  metaUserID,
		Extra:           toExtra(topUnknown),
	}

	for idx, m := range raw.Messages {
		decodedTurns, err := decodeInboundMessage(m)
		if err != nil {
			return nil, fmt.Errorf("decode messages[%d]: %w", idx, err)
		}
		req.Messages = append(req.Messages, decodedTurns...)
	}

	if err := ir.Validate(req); err != nil {
		return nil, fmt.Errorf("validate decoded messages request: %w", err)
	}
	return req, nil
}

// DecodeRequest parses an Anthropic Messages JSON request body into a validated
// ir.Request by delegating directly to the package-level DecodeRequest function.
// Malformed JSON, duplicate keys, or IR invariant violations return an error.
func (Codec) DecodeRequest(body []byte) (*ir.Request, error) {
	return DecodeRequest(body)
}

// decodeSystemField parses a string or block-array top-level system field into
// ir.Part slices. Absent, null, or empty scalar strings return (nil, nil), and
// non-text system blocks preserve their raw JSON inside OpaqueItem values.
func decodeSystemField(raw jsontext.Value) ([]ir.Part, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return nil, fmt.Errorf("decode system string: %w", err)
		}
		if s == "" {
			return nil, nil
		}
		return []ir.Part{{Kind: ir.PartText, Text: s}}, nil
	}
	if trimmed[0] != '[' {
		return nil, errors.New("decode system: must be string or array")
	}

	var rawBlocks []jsontext.Value
	if err := unmarshalJSONValue(trimmed, &rawBlocks); err != nil {
		return nil, fmt.Errorf("decode system array: %w", err)
	}
	if len(rawBlocks) == 0 {
		return nil, nil
	}

	out := make([]ir.Part, 0, len(rawBlocks))
	for i, elem := range rawBlocks {
		elemTrim := bytes.TrimSpace(elem)
		blockType, fastOK := peekKnownBlockType(elemTrim)
		if !fastOK {
			var probe wireTypeProbe
			if err := unmarshalJSONObject(elemTrim, &probe); err != nil {
				return nil, fmt.Errorf("decode system[%d]: %w", i, err)
			}
			blockType = probe.Type
		}
		switch blockType {
		case "text":
			var tb wireTextBlock
			if err := unmarshalJSONObject(elemTrim, &tb); err != nil {
				return nil, fmt.Errorf("decode system[%d]: %w", i, err)
			}
			cache, err := decodeCacheControl(tb.CacheControl)
			if err != nil {
				return nil, fmt.Errorf("decode system[%d].cache_control: %w", i, err)
			}
			// Omit empty text blocks carrying neither cache directives nor Extra
			// fields so client empty-text quirks normalize cleanly.
			if tb.Text == "" && cache == nil && len(tb.Unknown) == 0 {
				continue
			}
			out = append(out, ir.Part{
				Kind:  ir.PartText,
				Text:  tb.Text,
				Cache: cache,
				Extra: toExtra(tb.Unknown),
			})

		case "":
			return nil, fmt.Errorf("decode system[%d]: empty block type", i)

		default:
			if fastOK {
				var probe wireTypeProbe
				if err := unmarshalJSONObject(elemTrim, &probe); err != nil {
					return nil, fmt.Errorf("decode system[%d]: %w", i, err)
				}
			}
			out = append(out, ir.Part{
				Kind: ir.PartOpaque,
				Opaque: &ir.OpaqueItem{
					Origin: protocol.ProtocolAnthropicMessages,
					Type:   blockType,
					Raw:    compactRawJSON(elemTrim),
				},
			})
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// decodeThinkingField parses the top-level Messages thinking object into mode,
// optional positive budget_tokens, and any unmodeled thinking members such as display.
// Enabled mode requires a positive budget, whereas adaptive and disabled modes
// reject budget_tokens fields.
func decodeThinkingField(raw jsontext.Value) (string, *int64, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil, nil, nil
	}
	var wt wireThinking
	if err := unmarshalJSONObject(trimmed, &wt); err != nil {
		return "", nil, nil, fmt.Errorf("decode thinking: %w", err)
	}
	extraRaw, err := marshalUnknownMap(wt.Unknown)
	if err != nil {
		return "", nil, nil, fmt.Errorf("decode thinking extra: %w", err)
	}
	switch wt.Type {
	case "enabled":
		if wt.BudgetTokens == nil || *wt.BudgetTokens <= 0 {
			return "", nil, nil, errors.New("decode thinking: enabled mode requires positive budget_tokens")
		}
		b := *wt.BudgetTokens
		return "enabled", &b, extraRaw, nil
	case "adaptive":
		if wt.BudgetTokens != nil {
			return "", nil, nil, errors.New("decode thinking: adaptive mode cannot set budget_tokens")
		}
		return "adaptive", nil, extraRaw, nil
	case "disabled":
		if wt.BudgetTokens != nil {
			return "", nil, nil, errors.New("decode thinking: disabled mode cannot set budget_tokens")
		}
		return "disabled", nil, extraRaw, nil
	default:
		return "", nil, nil, fmt.Errorf("decode thinking: unsupported type %q", wt.Type)
	}
}

// decodeOutputConfigField parses output_config.effort and output_config.format
// into canonical IR controls and returns any residual unmodeled members as JSON.
// Empty objects and vendor extensions on output_config or format survive inside
// Request.Extra while invalid effort labels or non-object schemas return an error.
func decodeOutputConfigField(raw jsontext.Value) (string, *ir.OutputFormat, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil, nil, nil
	}
	var oc wireOutputConfig
	if err := unmarshalJSONObject(trimmed, &oc); err != nil {
		return "", nil, nil, fmt.Errorf("decode output_config: %w", err)
	}

	var effort string
	if strings.TrimSpace(oc.Effort) != "" {
		lower := strings.ToLower(strings.TrimSpace(oc.Effort))
		switch lower {
		case "low", "medium", "high", "xhigh", "max", "ultra":
			effort = lower
		default:
			return "", nil, nil, fmt.Errorf("decode output_config.effort: unsupported value %q", oc.Effort)
		}
	}

	ocUnknown := cloneValueMap(oc.Unknown)
	var outFmt *ir.OutputFormat
	if oc.Format != nil {
		if oc.Format.Type != "json_schema" {
			return "", nil, nil, fmt.Errorf("decode output_config.format: unsupported type %q", oc.Format.Type)
		}
		if !isValidSingleJSONObject(oc.Format.Schema) {
			return "", nil, nil, errors.New("decode output_config.format.schema: must be a valid JSON object")
		}
		outFmt = &ir.OutputFormat{
			Kind:   "json_schema",
			Schema: compactRawJSON(oc.Format.Schema),
		}
		if len(oc.Format.Unknown) > 0 {
			fmtExtra, err := marshalUnknownMap(oc.Format.Unknown)
			if err != nil {
				return "", nil, nil, fmt.Errorf("decode output_config.format extra: %w", err)
			}
			ocUnknown = insertRawMember(ocUnknown, "format", fmtExtra)
		}
	}

	if len(ocUnknown) > 0 {
		extraRaw, err := marshalUnknownMap(ocUnknown)
		if err != nil {
			return "", nil, nil, fmt.Errorf("decode output_config extra: %w", err)
		}
		return effort, outFmt, extraRaw, nil
	}
	if effort == "" && outFmt == nil {
		return "", nil, jsontext.Value("{}"), nil
	}
	return effort, outFmt, nil, nil
}

// decodeMetadataField extracts metadata.user_id into UserID and MetadataUserID
// while returning any additional metadata members as compacted JSON bytes.
// Empty metadata objects return "{}" so round-trip encoding retains metadata:{}.
func decodeMetadataField(raw jsontext.Value) (string, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil, nil
	}
	var parsed wireMetadata
	if err := unmarshalJSONObject(trimmed, &parsed); err != nil {
		return "", nil, fmt.Errorf("decode metadata: %w", err)
	}
	if len(parsed.Unknown) > 0 {
		extraRaw, err := marshalUnknownMap(parsed.Unknown)
		if err != nil {
			return "", nil, fmt.Errorf("decode metadata extra: %w", err)
		}
		return parsed.UserID, extraRaw, nil
	}
	if parsed.UserID != "" {
		return parsed.UserID, nil, nil
	}
	return "", jsontext.Value("{}"), nil
}

// decodeSpeedField maps speed="fast" onto ServiceTier="priority" for cross-protocol
// priority routing. Other non-empty speed strings such as "standard" return
// keepRaw=true so they survive inside Request.Extra.
func decodeSpeedField(raw jsontext.Value) (string, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", false, nil
	}
	var speed string
	if err := unmarshalJSONValue(trimmed, &speed); err != nil {
		return "", false, fmt.Errorf("decode speed: %w", err)
	}
	if strings.EqualFold(strings.TrimSpace(speed), "fast") {
		return "priority", false, nil
	}
	if speed != "" {
		return "", true, nil
	}
	return "", false, nil
}

// decodeMessagesTools parses tools[] declarations into ir.Tool entries. Entries
// with omitted type or type="custom" become ToolFunction and require a non-empty
// name plus an object input_schema, whereas other types become ToolHosted.
func decodeMessagesTools(rawTools []jsontext.Value) ([]ir.Tool, error) {
	if len(rawTools) == 0 {
		return nil, nil
	}
	out := make([]ir.Tool, 0, len(rawTools))
	for i, elem := range rawTools {
		trimmed := bytes.TrimSpace(elem)
		var wt wireInboundTool
		if err := unmarshalJSONObject(trimmed, &wt); err != nil {
			return nil, fmt.Errorf("decode tools[%d]: %w", i, err)
		}
		if wt.Type == "" || wt.Type == "custom" {
			if strings.TrimSpace(wt.Name) == "" {
				return nil, fmt.Errorf("decode tools[%d]: function tool requires non-empty name", i)
			}
			if !isValidSingleJSONObject(wt.InputSchema) {
				return nil, fmt.Errorf("decode tools[%d]: function tool requires object input_schema", i)
			}
			cache, err := decodeCacheControl(wt.CacheControl)
			if err != nil {
				return nil, fmt.Errorf("decode tools[%d].cache_control: %w", i, err)
			}
			out = append(out, ir.Tool{
				Kind:         ir.ToolFunction,
				Name:         wt.Name,
				Description:  wt.Description,
				InputSchema:  compactRawJSON(wt.InputSchema),
				Strict:       wt.Strict,
				DeferLoading: wt.DeferLoading,
				Cache:        cache,
				Extra:        toExtra(wt.Unknown),
			})
			continue
		}

		typ := strings.TrimSpace(wt.Type)
		if typ == "" {
			return nil, fmt.Errorf("decode tools[%d]: whitespace-only tool type", i)
		}
		name := wt.Name
		// Server-executed Anthropic tools may omit name; copy the versioned tool
		// type so ir.Validate's non-empty Tool.Name invariant stays satisfied.
		if strings.TrimSpace(name) == "" {
			name = typ
		}
		out = append(out, ir.Tool{
			Kind: ir.ToolHosted,
			Name: name,
			Hosted: &ir.OpaqueItem{
				Origin: protocol.ProtocolAnthropicMessages,
				Type:   typ,
				Raw:    compactRawJSON(trimmed),
			},
		})
	}
	return out, nil
}

// decodeMessagesToolChoice parses tool_choice into an ir.ToolChoice, an
// inverted ParallelToolCalls pointer derived from disable_parallel_tool_use,
// and any unmodeled tool_choice members. Absent or null values return nils,
// while unsupported types or blank tool names return an error.
func decodeMessagesToolChoice(raw jsontext.Value) (*ir.ToolChoice, *bool, jsontext.Value, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil, nil, nil
	}
	var wt wireToolChoice
	if err := unmarshalJSONObject(trimmed, &wt); err != nil {
		return nil, nil, nil, fmt.Errorf("decode tool_choice: %w", err)
	}
	extraRaw, err := marshalUnknownMap(wt.Unknown)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("decode tool_choice extra: %w", err)
	}

	var tc *ir.ToolChoice
	switch wt.Type {
	case "auto":
		tc = &ir.ToolChoice{Mode: ir.ToolChoiceAuto}
	case "any":
		tc = &ir.ToolChoice{Mode: ir.ToolChoiceRequired}
	case "none":
		tc = &ir.ToolChoice{Mode: ir.ToolChoiceNone}
	case "tool":
		if strings.TrimSpace(wt.Name) == "" {
			return nil, nil, nil, errors.New("decode tool_choice: type \"tool\" requires non-empty name")
		}
		tc = &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: wt.Name},
		}
	default:
		return nil, nil, nil, fmt.Errorf("decode tool_choice: unsupported type %q", wt.Type)
	}

	var parallel *bool
	if wt.Type != "none" && wt.DisableParallelToolUse != nil {
		allow := !*wt.DisableParallelToolUse
		parallel = &allow
	}
	return tc, parallel, extraRaw, nil
}

// decodeInboundMessage converts one wireInboundMessage into one or more
// normalized ir.Message turns. User messages mixing tool_result blocks with
// text or media split into contiguous RoleTool and RoleUser turns in block order.
func decodeInboundMessage(m wireInboundMessage) ([]ir.Message, error) {
	switch m.Role {
	case "user":
		return decodeUserInboundMessage(m)
	case "assistant":
		msg, err := decodeRoleInboundMessage(ir.RoleAssistant, m)
		if err != nil {
			return nil, err
		}
		return []ir.Message{msg}, nil
	case "system":
		msg, err := decodeRoleInboundMessage(ir.RoleSystem, m)
		if err != nil {
			return nil, err
		}
		return []ir.Message{msg}, nil
	default:
		return nil, fmt.Errorf("unsupported message role %q", m.Role)
	}
}

// decodeUserInboundMessage decodes scalar or block-array user message content
// into one or more normalized ir.Message turns. Non-string, non-array content
// values or malformed inner blocks return an error.
//
// Anthropic user messages combine tool_result blocks with ordinary text and
// media in a single turn. Splitting contiguous tool_result runs into RoleTool
// turns while keeping Message.Extra on the first emitted turn preserves block
// order and satisfies IR role constraints without losing message extensions.
func decodeUserInboundMessage(m wireInboundMessage) ([]ir.Message, error) {
	trimmed := bytes.TrimSpace(m.Content)
	msgExtra := toExtra(m.Unknown)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return []ir.Message{{
			Role:  ir.RoleUser,
			Parts: []ir.Part{},
			Extra: msgExtra,
		}}, nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return nil, fmt.Errorf("decode user content string: %w", err)
		}
		parts := []ir.Part{}
		if s != "" {
			parts = []ir.Part{{Kind: ir.PartText, Text: s}}
		}
		return []ir.Message{{
			Role:  ir.RoleUser,
			Parts: parts,
			Extra: msgExtra,
		}}, nil
	}
	if trimmed[0] != '[' {
		return nil, errors.New("user content must be string or array")
	}

	var stackSpans [16][]byte
	rawBlocks, err := appendJSONArrayElementSpans(stackSpans[:0], trimmed)
	if err != nil {
		return nil, fmt.Errorf("decode user content array: %w", err)
	}

	decoded := make([]ir.Part, 0, len(rawBlocks))
	for i, elem := range rawBlocks {
		part, keep, err := decodeRequestBlock(jsontext.Value(elem), ir.RoleUser)
		if err != nil {
			return nil, fmt.Errorf("content[%d]: %w", i, err)
		}
		if keep {
			decoded = append(decoded, part)
		}
	}

	if len(decoded) == 0 {
		return []ir.Message{{
			Role:  ir.RoleUser,
			Parts: []ir.Part{},
			Extra: msgExtra,
		}}, nil
	}

	var turns []ir.Message
	for _, p := range decoded {
		wantRole := ir.RoleUser
		if p.Kind == ir.PartToolResult {
			wantRole = ir.RoleTool
		}
		if len(turns) == 0 || turns[len(turns)-1].Role != wantRole {
			var extra ir.Extra
			if len(turns) == 0 {
				extra = msgExtra
			}
			turns = append(turns, ir.Message{
				Role:  wantRole,
				Parts: []ir.Part{p},
				Extra: extra,
			})
			continue
		}
		turns[len(turns)-1].Parts = append(turns[len(turns)-1].Parts, p)
	}
	return turns, nil
}

// decodeRoleInboundMessage decodes an assistant or compatible system message
// into a single ir.Message with non-nil Parts. Absent, null, or empty scalar
// content strings yield an empty Parts slice while retaining Message.Extra.
func decodeRoleInboundMessage(role ir.Role, m wireInboundMessage) (ir.Message, error) {
	trimmed := bytes.TrimSpace(m.Content)
	msgExtra := toExtra(m.Unknown)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ir.Message{
			Role:  role,
			Parts: []ir.Part{},
			Extra: msgExtra,
		}, nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return ir.Message{}, fmt.Errorf("decode %s content string: %w", role, err)
		}
		parts := []ir.Part{}
		if s != "" {
			parts = []ir.Part{{Kind: ir.PartText, Text: s}}
		}
		return ir.Message{
			Role:  role,
			Parts: parts,
			Extra: msgExtra,
		}, nil
	}
	if trimmed[0] != '[' {
		return ir.Message{}, fmt.Errorf("%s content must be string or array", role)
	}

	var stackSpans [16][]byte
	rawBlocks, err := appendJSONArrayElementSpans(stackSpans[:0], trimmed)
	if err != nil {
		return ir.Message{}, fmt.Errorf("decode %s content array: %w", role, err)
	}

	parts := make([]ir.Part, 0, len(rawBlocks))
	for i, elem := range rawBlocks {
		part, keep, err := decodeRequestBlock(jsontext.Value(elem), role)
		if err != nil {
			return ir.Message{}, fmt.Errorf("content[%d]: %w", i, err)
		}
		if keep {
			parts = append(parts, part)
		}
	}
	return ir.Message{
		Role:  role,
		Parts: parts,
		Extra: msgExtra,
	}, nil
}

// decodeRequestBlock parses one JSON content block inside a request message of
// msgRole. Empty text blocks without cache_control or Extra fields return
// keep=false so client empty-text quirks do not produce empty IR text parts.
func decodeRequestBlock(raw jsontext.Value, msgRole ir.Role) (ir.Part, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	blockType, fastOK := peekKnownBlockType(trimmed)
	if !fastOK {
		var probe wireTypeProbe
		if err := unmarshalJSONObject(trimmed, &probe); err != nil {
			return ir.Part{}, false, err
		}
		blockType = probe.Type
	}

	switch blockType {
	case "text":
		var tb wireTextBlock
		if err := unmarshalJSONObject(trimmed, &tb); err != nil {
			return ir.Part{}, false, err
		}
		cache, err := decodeCacheControl(tb.CacheControl)
		if err != nil {
			return ir.Part{}, false, err
		}
		// Omit empty text blocks without cache directives or Extra members so client
		// empty-text placeholders never fail IR part validation.
		if tb.Text == "" && cache == nil && len(tb.Unknown) == 0 {
			return ir.Part{}, false, nil
		}
		return ir.Part{
			Kind:  ir.PartText,
			Text:  tb.Text,
			Cache: cache,
			Extra: toExtra(tb.Unknown),
		}, true, nil

	case "image":
		return decodeMediaRequestBlock(trimmed, ir.MediaImage, false)

	case "document":
		return decodeMediaRequestBlock(trimmed, ir.MediaDocument, true)

	case "thinking":
		var th wireThinkingBlock
		if err := unmarshalJSONObject(trimmed, &th); err != nil {
			return ir.Part{}, false, err
		}
		if msgRole != ir.RoleAssistant {
			return ir.Part{}, false, fmt.Errorf("thinking block is not allowed in %s message", msgRole)
		}
		cache, err := decodeCacheControl(th.CacheControl)
		if err != nil {
			return ir.Part{}, false, err
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
		}, true, nil

	case "redacted_thinking":
		var rt wireRedactedThinkingBlock
		if err := unmarshalJSONObject(trimmed, &rt); err != nil {
			return ir.Part{}, false, err
		}
		if msgRole != ir.RoleAssistant {
			return ir.Part{}, false, fmt.Errorf("redacted_thinking block is not allowed in %s message", msgRole)
		}
		if rt.Data == "" {
			return ir.Part{}, false, errors.New("redacted_thinking block requires non-empty data")
		}
		cache, err := decodeCacheControl(rt.CacheControl)
		if err != nil {
			return ir.Part{}, false, err
		}
		return ir.Part{
			Kind: ir.PartReasoning,
			Reasoning: &ir.Reasoning{
				Redacted: true,
				Token:    &ir.ReplayToken{Data: rt.Data},
			},
			Cache: cache,
			Extra: toExtra(rt.Unknown),
		}, true, nil

	case "tool_use":
		var tu wireToolUseBlock
		if err := unmarshalJSONObject(trimmed, &tu); err != nil {
			return ir.Part{}, false, err
		}
		if msgRole != ir.RoleAssistant {
			return ir.Part{}, false, fmt.Errorf("tool_use block is only allowed in assistant messages, got %s", msgRole)
		}
		if strings.TrimSpace(tu.Name) == "" {
			return ir.Part{}, false, errors.New("tool_use block requires non-empty name")
		}
		if !isValidSingleJSONObject(tu.Input) {
			return ir.Part{}, false, errors.New("tool_use.input must be a valid JSON object")
		}
		cache, err := decodeCacheControl(tu.CacheControl)
		if err != nil {
			return ir.Part{}, false, err
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
		}, true, nil

	case "tool_result":
		if msgRole != ir.RoleUser {
			var tr wireToolResultBlock
			if err := unmarshalJSONObject(trimmed, &tr); err != nil {
				return ir.Part{}, false, err
			}
			return ir.Part{}, false, fmt.Errorf("tool_result block is only allowed in user messages, got %s", msgRole)
		}
		return decodeToolResultBlock(trimmed)

	case "":
		return ir.Part{}, false, errors.New("content block requires non-empty type")

	default:
		return ir.Part{
			Kind: ir.PartOpaque,
			Opaque: &ir.OpaqueItem{
				Origin: protocol.ProtocolAnthropicMessages,
				Type:   blockType,
				Raw:    compactRawJSON(trimmed),
			},
		}, true, nil
	}
}

// decodeMediaRequestBlock parses an Anthropic "image" or "document" block whose
// source.type is "base64", "url", or "file" and trims surrounding whitespace
// from base64 media_type labels. Document blocks using Anthropic plaintext or
// custom content sources fall back to OpaqueItem when allowOpaqueFallback is true.
func decodeMediaRequestBlock(trimmed []byte, modality ir.MediaType, allowOpaqueFallback bool) (ir.Part, bool, error) {
	var mb wireMediaBlock
	if err := unmarshalJSONObject(trimmed, &mb); err != nil {
		return ir.Part{}, false, err
	}
	if mb.Source == nil {
		return ir.Part{}, false, fmt.Errorf("%s block requires source object", modality)
	}
	cache, err := decodeCacheControl(mb.CacheControl)
	if err != nil {
		return ir.Part{}, false, err
	}

	var media ir.Media
	media.Type = modality
	switch mb.Source.Type {
	case "base64":
		// Trim surrounding whitespace from base64 media_type on decode because
		// serializeMessagesMediaBlock trims MIME strings without recording a Loss,
		// keeping decoded Media.MIME symmetric across Law Q2 round trips.
		mime := strings.TrimSpace(mb.Source.MediaType)
		if mime == "" {
			return ir.Part{}, false, fmt.Errorf("%s base64 source requires non-empty media_type", modality)
		}
		if mb.Source.Data == "" {
			return ir.Part{}, false, fmt.Errorf("%s base64 source requires non-empty data", modality)
		}
		media.Source = ir.MediaSourceBase64
		media.MIME = mime
		media.Data = mb.Source.Data

	case "url":
		if mb.Source.URL == "" {
			return ir.Part{}, false, fmt.Errorf("%s url source requires non-empty url", modality)
		}
		media.Source = ir.MediaSourceURL
		media.Data = mb.Source.URL

	case "file":
		if mb.Source.FileID == "" {
			return ir.Part{}, false, fmt.Errorf("%s file source requires non-empty file_id", modality)
		}
		media.Source = ir.MediaSourceFileID
		media.Data = mb.Source.FileID

	default:
		// Anthropic document blocks also support "text" and "content" sources
		// that have no cross-protocol Media equivalent; keep them as OpaqueItem.
		if allowOpaqueFallback && strings.TrimSpace(mb.Source.Type) != "" {
			return ir.Part{
				Kind: ir.PartOpaque,
				Opaque: &ir.OpaqueItem{
					Origin: protocol.ProtocolAnthropicMessages,
					Type:   mb.Type,
					Raw:    compactRawJSON(trimmed),
				},
			}, true, nil
		}
		return ir.Part{}, false, fmt.Errorf("unsupported %s source.type %q", modality, mb.Source.Type)
	}

	return ir.Part{
		Kind:  ir.PartMedia,
		Media: &media,
		Cache: cache,
		Extra: toExtra(mb.Unknown),
	}, true, nil
}

// decodeToolResultBlock parses an Anthropic "tool_result" block into a
// PartToolResult carrying non-nil text and media Content parts. Scalar string
// and block-array payloads normalize into Content while other JSON types error.
func decodeToolResultBlock(trimmed []byte) (ir.Part, bool, error) {
	var tr wireToolResultBlock
	if err := unmarshalJSONObject(trimmed, &tr); err != nil {
		return ir.Part{}, false, err
	}
	cache, err := decodeCacheControl(tr.CacheControl)
	if err != nil {
		return ir.Part{}, false, err
	}

	innerTrim := bytes.TrimSpace(tr.Content)
	contentParts := []ir.Part{}
	switch {
	case len(innerTrim) == 0 || bytes.Equal(innerTrim, []byte("null")):
		// Keep an allocated empty Content slice because ir.Validate rejects nil
		// ToolResult.Content slices even when tool_result.content is omitted.

	case innerTrim[0] == '"':
		var s string
		if err := unmarshalJSONValue(innerTrim, &s); err != nil {
			return ir.Part{}, false, fmt.Errorf("decode tool_result content string: %w", err)
		}
		if s != "" {
			contentParts = []ir.Part{{Kind: ir.PartText, Text: s}}
		}

	case innerTrim[0] == '[':
		var stackSpans [8][]byte
		rawInner, err := appendJSONArrayElementSpans(stackSpans[:0], innerTrim)
		if err != nil {
			return ir.Part{}, false, fmt.Errorf("decode tool_result content array: %w", err)
		}
		contentParts = make([]ir.Part, 0, len(rawInner))
		for i, elem := range rawInner {
			child, keep, err := decodeToolResultInnerBlock(jsontext.Value(elem))
			if err != nil {
				return ir.Part{}, false, fmt.Errorf("tool_result.content[%d]: %w", i, err)
			}
			if keep {
				contentParts = append(contentParts, child)
			}
		}

	default:
		return ir.Part{}, false, errors.New("tool_result.content must be string, array, or null")
	}

	return ir.Part{
		Kind: ir.PartToolResult,
		Result: &ir.ToolResult{
			CallID:  tr.ToolUseID,
			Content: contentParts,
			IsError: tr.IsError,
		},
		Cache: cache,
		Extra: toExtra(tr.Unknown),
	}, true, nil
}

// decodeToolResultInnerBlock parses one nested text, image, or document block
// inside a tool_result content array. Empty placeholder text blocks return
// keep=false, while unsupported inner block types return an error so decoded
// ToolResult.Content slices always satisfy ir.Validate.
func decodeToolResultInnerBlock(raw jsontext.Value) (ir.Part, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	blockType, fastOK := peekKnownBlockType(trimmed)
	if !fastOK {
		var probe wireTypeProbe
		if err := unmarshalJSONObject(trimmed, &probe); err != nil {
			return ir.Part{}, false, err
		}
		blockType = probe.Type
	}
	switch blockType {
	case "text":
		var tb wireTextBlock
		if err := unmarshalJSONObject(trimmed, &tb); err != nil {
			return ir.Part{}, false, err
		}
		cache, err := decodeCacheControl(tb.CacheControl)
		if err != nil {
			return ir.Part{}, false, err
		}
		// Omit empty text blocks without cache directives or Extra members so client
		// empty-text placeholders never fail IR part validation.
		if tb.Text == "" && cache == nil && len(tb.Unknown) == 0 {
			return ir.Part{}, false, nil
		}
		return ir.Part{
			Kind:  ir.PartText,
			Text:  tb.Text,
			Cache: cache,
			Extra: toExtra(tb.Unknown),
		}, true, nil

	case "image":
		return decodeMediaRequestBlock(trimmed, ir.MediaImage, false)

	case "document":
		return decodeMediaRequestBlock(trimmed, ir.MediaDocument, false)

	default:
		if fastOK {
			var probe wireTypeProbe
			if err := unmarshalJSONObject(trimmed, &probe); err != nil {
				return ir.Part{}, false, err
			}
		}
		return ir.Part{}, false, fmt.Errorf("unsupported tool_result inner block type %q", blockType)
	}
}

// appendJSONArrayElementSpans slices a validated JSON array in trimmed into
// borrowed element byte slices appended to dst without cloning each element.
// Invalid UTF-8 inside JSON string values is tolerated, while non-array inputs,
// malformed element syntax, or trailing tokens return an error.
func appendJSONArrayElementSpans(dst [][]byte, trimmed []byte) ([][]byte, error) {
	pooled, ok := arrayDecoderPool.Get().(*pooledArrayDecoder)
	if !ok {
		pooled = &pooledArrayDecoder{}
		pooled.dec = jsontext.NewDecoder(&pooled.reader, jsontext.AllowInvalidUTF8(true))
	}
	pooled.reader.Reset(trimmed)
	pooled.dec.Reset(&pooled.reader, jsontext.AllowInvalidUTF8(true))
	defer func() {
		pooled.reader.Reset(nil)
		arrayDecoderPool.Put(pooled)
	}()

	tok, err := pooled.dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '[' {
		return nil, errors.New("expected JSON array")
	}
	for pooled.dec.PeekKind() != ']' {
		eStart := int(pooled.dec.InputOffset())
		if err := pooled.dec.SkipValue(); err != nil {
			return nil, err
		}
		eEnd := int(pooled.dec.InputOffset())
		span := bytes.TrimSpace(trimmed[eStart:eEnd])
		if len(span) > 0 && span[0] == ',' {
			span = bytes.TrimSpace(span[1:])
		}
		dst = append(dst, span)
	}
	if _, err := pooled.dec.ReadToken(); err != nil {
		return nil, err
	}
	if _, err := pooled.dec.ReadToken(); err != io.EOF {
		return nil, errors.New("unexpected trailing JSON token after array")
	}
	return dst, nil
}

// peekKnownBlockType extracts an unescaped "type" string constant when "type"
// is either the leading or the trailing top-level member of trimmed and matches
// one of the seven modeled Anthropic request content block kinds. Checking both
// ends covers struct-ordered SDK payloads ("type" first) and sorted-map JSON
// payloads ("type" last) with zero allocations while falling back to full
// wireTypeProbe decoding for escaped, middle-positioned, or unknown types.
func peekKnownBlockType(trimmed []byte) (string, bool) {
	if len(trimmed) < 10 || trimmed[0] != '{' || trimmed[len(trimmed)-1] != '}' {
		return "", false
	}
	if typ, ok := peekLeadingKnownBlockType(trimmed); ok {
		return typ, true
	}
	return peekTrailingKnownBlockType(trimmed)
}

// peekLeadingKnownBlockType checks whether the opening member of trimmed is an
// unescaped "type" key matching one of the seven modeled content block kinds.
// Callers must pass a slice of at least ten bytes beginning with '{'; escaped
// characters, control bytes, non-"type" first keys, or unknown kinds return ("", false).
func peekLeadingKnownBlockType(trimmed []byte) (string, bool) {
	i := skipJSONSpace(trimmed, 1)
	const typeKey = `"type"`
	if i+len(typeKey) >= len(trimmed) || string(trimmed[i:i+len(typeKey)]) != typeKey {
		return "", false
	}
	i = skipJSONSpace(trimmed, i+len(typeKey))
	if i >= len(trimmed) || trimmed[i] != ':' {
		return "", false
	}
	i = skipJSONSpace(trimmed, i+1)
	if i >= len(trimmed) || trimmed[i] != '"' {
		return "", false
	}
	valStart := i + 1
	for valEnd := valStart; valEnd < len(trimmed); valEnd++ {
		ch := trimmed[valEnd]
		if ch == '"' {
			return matchKnownBlockType(trimmed[valStart:valEnd])
		}
		if ch == '\\' || ch < 0x20 {
			return "", false
		}
	}
	return "", false
}

// peekTrailingKnownBlockType checks whether the final top-level member before
// trimmed's closing '}' brace is an unescaped "type" key matching a modeled kind.
// Callers must pass a slice of at least ten bytes ending with '}'; escaped
// characters, control bytes, non-"type" trailing keys, or unknown kinds return ("", false).
func peekTrailingKnownBlockType(trimmed []byte) (string, bool) {
	j := skipJSONSpaceBack(trimmed, len(trimmed)-2)
	if j <= 0 || trimmed[j] != '"' {
		return "", false
	}
	valEnd := j
	openValQuote := -1
	for k := j - 1; k > 0; k-- {
		ch := trimmed[k]
		if ch == '"' {
			openValQuote = k
			break
		}
		if ch == '\\' || ch < 0x20 {
			return "", false
		}
	}
	if openValQuote <= 0 {
		return "", false
	}
	typ, ok := matchKnownBlockType(trimmed[openValQuote+1 : valEnd])
	if !ok {
		return "", false
	}
	j = skipJSONSpaceBack(trimmed, openValQuote-1)
	if j <= 0 || trimmed[j] != ':' {
		return "", false
	}
	j = skipJSONSpaceBack(trimmed, j-1)
	const typeKey = `"type"`
	keyStart := j - len(typeKey) + 1
	if keyStart <= 0 || string(trimmed[keyStart:j+1]) != typeKey {
		return "", false
	}
	prev := skipJSONSpaceBack(trimmed, keyStart-1)
	if prev < 0 || (trimmed[prev] != ',' && trimmed[prev] != '{') {
		return "", false
	}
	return typ, true
}

// matchKnownBlockType maps raw unescaped block-type bytes onto one of the seven
// modeled Anthropic request block constants without allocating on the heap.
// Unrecognized or empty byte slices return ("", false) so callers fall back to wireTypeProbe.
func matchKnownBlockType(raw []byte) (string, bool) {
	switch string(raw) {
	case "text":
		return "text", true
	case "image":
		return "image", true
	case "document":
		return "document", true
	case "thinking":
		return "thinking", true
	case "redacted_thinking":
		return "redacted_thinking", true
	case "tool_use":
		return "tool_use", true
	case "tool_result":
		return "tool_result", true
	default:
		return "", false
	}
}

// skipJSONSpace advances a non-negative byte offset idx past RFC 8259 ASCII
// whitespace bytes in raw and returns the first non-whitespace offset. When
// only whitespace remains from idx onward, skipJSONSpace returns len(raw).
func skipJSONSpace(raw []byte, idx int) int {
	for idx < len(raw) {
		switch raw[idx] {
		case ' ', '\t', '\n', '\r':
			idx++
		default:
			return idx
		}
	}
	return idx
}

// skipJSONSpaceBack rewinds byte offset idx (< len(raw)) past RFC 8259 ASCII
// whitespace bytes in raw and returns the rightmost non-whitespace offset. When
// no non-whitespace byte precedes or sits at idx, skipJSONSpaceBack returns -1.
func skipJSONSpaceBack(raw []byte, idx int) int {
	for idx >= 0 {
		switch raw[idx] {
		case ' ', '\t', '\n', '\r':
			idx--
		default:
			return idx
		}
	}
	return -1
}
