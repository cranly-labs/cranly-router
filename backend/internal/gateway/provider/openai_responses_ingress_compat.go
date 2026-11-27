package provider

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// openai_responses_ingress_compat.go normalizes legacy Chat messages, string
// prompt aliases, and commands fields in OpenAI Responses requests. Legacy
// Responses ingress can carry Chat messages or a string prompt alongside native
// Responses input; nonnull native input remains authoritative, whereas adopted
// message history removes previous_response_id so inline history is not resolved
// twice.
//
// Conversion translates Chat-shaped message subsets and controls via
// protocol/convert and ordered jsontext members without routing tool schemas or
// 64-bit integer literals through any or float64. Raw member inspection governs
// whether converted tools, tool_choice, and reasoning replace top-level fields,
// while object or array prompt templates stay untouched for upstream validation.

// rawLegacyIngressMember preserves one top-level JSON object member in original
// appearance order as a raw jsontext.Value slice.
type rawLegacyIngressMember struct {
	name  string         // decoded object member key
	value jsontext.Value // untouched raw JSON member value
}

// convertedLegacyResponsesMessages holds lossless jsontext.Value fields
// produced by convertLegacyResponsesMessages without decoding through any.
type convertedLegacyResponsesMessages struct {
	input           jsontext.Value // converted Responses input array JSON
	instructions    jsontext.Value // optional converted instructions string JSON
	maxOutputTokens jsontext.Value // optional converted max_output_tokens JSON
	temperature     jsontext.Value // optional converted temperature JSON
	topP            jsontext.Value // optional converted top_p JSON
	tools           jsontext.Value // optional converted Responses tools array JSON
	reasoning       jsontext.Value // optional converted Responses reasoning JSON
	toolChoice      jsontext.Value // optional converted Responses tool_choice JSON
	serviceTier     jsontext.Value // optional converted service_tier string JSON
	// hasChatTools, hasChatToolChoice, and hasReasoningEffort track explicit Chat
	// legacy fields on the inbound object so applyLegacyResponsesTopLevelMembers
	// never overwrites native Responses tools, tool_choice, or reasoning objects.
	hasChatTools       bool // explicit Chat-style tools[] or legacy functions[]
	hasChatToolChoice  bool // explicit Chat-style tool_choice or function_call
	hasReasoningEffort bool // explicit non-blank top-level reasoning_effort
}

// NormalizeOpenAIResponsesLegacyIngress normalizes legacy Chat messages, string
// prompt aliases, and commands fields in a Responses request body using
// protocol/convert and ordered jsontext members. Empty bodies or valid JSON
// objects without legacy fields return body byte-for-byte unchanged with false
// and nil error, whereas non-object JSON, duplicate keys, or Chat conversion
// failures return body with false and an error.
//
// Present non-null native input stays authoritative over converted messages,
// retaining native tools, tool_choice, and previous_response_id while removing
// messages. When native input is absent or null, converted message history
// adopts compatible top-level controls, clears previous_response_id, and keeps
// every untouched top-level or nested 64-bit integer and extension field intact.
func NormalizeOpenAIResponsesLegacyIngress(body []byte) ([]byte, bool, error) {
	if len(body) == 0 {
		return body, false, nil
	}
	members, hasLegacyKeys, err := decodeRawLegacyIngressObject(body)
	if err != nil {
		return body, false, fmt.Errorf("normalize legacy Responses ingress: %w", err)
	}
	if !hasLegacyKeys {
		return body, false, nil
	}

	changed := false
	if msgVal, hasMessages := findRawLegacyMember(members, "messages"); hasMessages {
		if isNonEmptyRawJSONArray(msgVal) {
			legacy, convErr := convertLegacyResponsesMessages(members)
			if convErr != nil {
				return body, false, convErr
			}
			// Adopt converted Chat messages only when native input is absent or null,
			// removing previous_response_id so inline history is not resolved twice.
			if !hasNonNullRawLegacyMember(members, "input") {
				upsertRawLegacyMember(&members, "input", legacy.input)
				applyLegacyResponsesTopLevelMembers(&members, legacy)
				deleteRawLegacyMember(&members, "previous_response_id")
			}
		}
		deleteRawLegacyMember(&members, "messages")
		changed = true
	}

	if promptVal, hasPrompt := findRawLegacyMember(members, "prompt"); hasPrompt {
		trimmedPrompt := bytes.TrimSpace(promptVal)
		// Only string prompt aliases translate into input; object prompt templates
		// and array shapes stay untouched for upstream Responses validation.
		if len(trimmedPrompt) > 0 && trimmedPrompt[0] == '"' {
			if !hasNonNullRawLegacyMember(members, "input") {
				upsertRawLegacyMember(&members, "input", jsontext.Value(bytes.Clone(trimmedPrompt)))
			}
			deleteRawLegacyMember(&members, "prompt")
			changed = true
		}
	}

	if _, hasCommands := findRawLegacyMember(members, "commands"); hasCommands {
		deleteRawLegacyMember(&members, "commands")
		changed = true
	}

	if !changed {
		return body, false, nil
	}
	normalized, err := marshalRawLegacyIngressObject(members)
	if err != nil {
		return body, false, fmt.Errorf("serialize legacy Responses ingress: %w", err)
	}
	return normalized, true, nil
}

// convertLegacyResponsesMessages converts the Chat-shaped member subset of
// members into lossless jsontext.Value Responses fields through
// convert.NewExchange. Malformed Chat message arrays, dialect conversion
// failures, or empty converted input slices return an error without mutating
// caller-supplied members.
//
// Sub-request assembly copies only Chat-compatible controls (model, messages,
// token caps, sampling scalars, service_tier, explicit reasoning_effort, nested
// function tools or tool_choice, and deprecated functions or function_call).
// Leaving flat Responses tools, tool_choice, and reasoning objects outside that
// temporary sub-request prevents codec/chat from rejecting hybrid requests while
// preserving 64-bit integer bounds and unmodeled schema keywords.
func convertLegacyResponsesMessages(members []rawLegacyIngressMember) (convertedLegacyResponsesMessages, error) {
	var out convertedLegacyResponsesMessages

	modelVal, _ := findRawLegacyMember(members, "model")
	modelStr := decodeOptionalJSONString(modelVal)

	toolsVal, hasTools := findRawLegacyMember(members, "tools")
	functionsVal, hasFunctions := findRawLegacyMember(members, "functions")
	out.hasChatTools = (hasTools && hasChatStyleToolsRaw(toolsVal)) || (hasFunctions && isNonNullRawJSON(functionsVal))

	tcVal, hasTC := findRawLegacyMember(members, "tool_choice")
	fnCallVal, hasFnCall := findRawLegacyMember(members, "function_call")
	out.hasChatToolChoice = (hasTC && hasChatStyleToolChoiceRaw(tcVal)) || (hasFnCall && isNonNullRawJSON(fnCallVal))

	effortVal, _ := findRawLegacyMember(members, "reasoning_effort")
	out.hasReasoningEffort = strings.TrimSpace(decodeOptionalJSONString(effortVal)) != ""

	var chatSub []rawLegacyIngressMember
	for _, key := range []string{"model", "messages", "max_tokens", "max_completion_tokens", "temperature", "top_p", "service_tier"} {
		if val, ok := findRawLegacyMember(members, key); ok {
			chatSub = append(chatSub, rawLegacyIngressMember{name: key, value: val})
		}
	}
	if out.hasReasoningEffort {
		chatSub = append(chatSub, rawLegacyIngressMember{name: "reasoning_effort", value: effortVal})
	}
	if out.hasChatTools {
		if hasTools && hasChatStyleToolsRaw(toolsVal) {
			chatSub = append(chatSub, rawLegacyIngressMember{name: "tools", value: toolsVal})
		}
		if hasFunctions && isNonNullRawJSON(functionsVal) {
			chatSub = append(chatSub, rawLegacyIngressMember{name: "functions", value: functionsVal})
		}
	}
	if out.hasChatToolChoice {
		if hasTC && hasChatStyleToolChoiceRaw(tcVal) {
			chatSub = append(chatSub, rawLegacyIngressMember{name: "tool_choice", value: tcVal})
		} else if hasFnCall && isNonNullRawJSON(fnCallVal) {
			chatSub = append(chatSub, rawLegacyIngressMember{name: "function_call", value: fnCallVal})
		}
	}

	subBody, err := marshalRawLegacyIngressObject(chatSub)
	if err != nil {
		return out, fmt.Errorf("normalize legacy Responses messages: %w", err)
	}

	dialect := legacyIngressChatToResponsesDialect(modelStr)
	ex, err := convert.NewExchange(subBody, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  dialect,
		},
	})
	if err != nil {
		return out, fmt.Errorf("normalize legacy Responses messages: %w", err)
	}
	convertedBytes, err := ex.UpstreamRequest()
	if err != nil {
		return out, fmt.Errorf("normalize legacy Responses messages: %w", err)
	}

	var encoded struct {
		Input           jsontext.Value            `json:"input"`             // converted Responses input item array
		Instructions    jsontext.Value            `json:"instructions"`      // converted system instructions string
		MaxOutputTokens jsontext.Value            `json:"max_output_tokens"` // converted output token cap
		Temperature     jsontext.Value            `json:"temperature"`       // converted sampling temperature scalar
		TopP            jsontext.Value            `json:"top_p"`             // converted nucleus sampling scalar
		Tools           jsontext.Value            `json:"tools"`             // converted flat Responses tool array
		Reasoning       jsontext.Value            `json:"reasoning"`         // converted Responses reasoning configuration
		ToolChoice      jsontext.Value            `json:"tool_choice"`       // converted Responses tool selection
		ServiceTier     jsontext.Value            `json:"service_tier"`      // converted upstream service tier string
		Unknown         map[string]jsontext.Value `json:",embed"`            // unused converted sibling members
	}
	dec := jsontext.NewDecoder(bytes.NewReader(convertedBytes), jsontext.AllowInvalidUTF8(true))
	if err := jsonv2.UnmarshalDecode(dec, &encoded); err != nil {
		return out, fmt.Errorf("decode normalized legacy Responses input: %w", err)
	}
	if len(bytes.TrimSpace(encoded.Input)) == 0 {
		return out, fmt.Errorf("normalize legacy Responses messages: empty input")
	}

	out.input = encoded.Input
	out.instructions = encoded.Instructions
	out.maxOutputTokens = encoded.MaxOutputTokens
	out.temperature = encoded.Temperature
	out.topP = encoded.TopP
	out.serviceTier = encoded.ServiceTier
	if out.hasChatTools && len(bytes.TrimSpace(encoded.Tools)) > 0 {
		out.tools = encoded.Tools
	}
	if out.hasReasoningEffort && len(bytes.TrimSpace(encoded.Reasoning)) > 0 {
		out.reasoning = encoded.Reasoning
	}
	if out.hasChatToolChoice && len(bytes.TrimSpace(encoded.ToolChoice)) > 0 {
		out.toolChoice = encoded.ToolChoice
	}
	return out, nil
}

// legacyIngressChatToResponsesDialect constructs the OpenAI Responses target
// dialect for legacy Chat-messages ingress normalization, retaining full
// Responses tool support before final provider routing.
func legacyIngressChatToResponsesDialect(model string) convert.Dialect {
	return ConversionDialect(
		context.Background(),
		nil,
		modelrules.Resolution{
			Target: modelrules.Target{
				Platform:         capability.PlatformOpenAI,
				ProviderType:     capability.ProviderTypeOAuth,
				Model:            model,
				UpstreamProtocol: string(protocol.ProtocolOpenAIResponses),
			},
		},
		protocol.ProtocolOpenAIResponses,
	)
}

// applyLegacyResponsesTopLevelMembers merges converted Chat fields from legacy
// into members and strips deprecated Chat-only top-level member keys. Existing
// native Responses tools, tool_choice, and reasoning values remain untouched
// when no corresponding Chat-shaped field was supplied.
func applyLegacyResponsesTopLevelMembers(members *[]rawLegacyIngressMember, legacy convertedLegacyResponsesMessages) {
	if strings.TrimSpace(decodeOptionalJSONString(legacy.instructions)) != "" {
		existingInst, _ := findRawLegacyMember(*members, "instructions")
		if strings.TrimSpace(decodeOptionalJSONString(existingInst)) == "" {
			upsertRawLegacyMember(members, "instructions", legacy.instructions)
		}
	}
	if isNonNullRawJSON(legacy.maxOutputTokens) {
		upsertRawLegacyMember(members, "max_output_tokens", legacy.maxOutputTokens)
	}
	if isNonNullRawJSON(legacy.temperature) {
		upsertRawLegacyMember(members, "temperature", legacy.temperature)
	}
	if isNonNullRawJSON(legacy.topP) {
		upsertRawLegacyMember(members, "top_p", legacy.topP)
	}
	if legacy.hasChatTools && isNonNullRawJSON(legacy.tools) {
		upsertRawLegacyMember(members, "tools", legacy.tools)
	}
	if legacy.hasReasoningEffort && isNonNullRawJSON(legacy.reasoning) {
		upsertRawLegacyMember(members, "reasoning", legacy.reasoning)
	}
	if legacy.hasChatToolChoice && isNonNullRawJSON(legacy.toolChoice) {
		upsertRawLegacyMember(members, "tool_choice", legacy.toolChoice)
	}
	if strings.TrimSpace(decodeOptionalJSONString(legacy.serviceTier)) != "" {
		upsertRawLegacyMember(members, "service_tier", legacy.serviceTier)
	}
	for _, key := range []string{"max_tokens", "max_completion_tokens", "reasoning_effort", "functions", "function_call"} {
		deleteRawLegacyMember(members, key)
	}
}

// decodeRawLegacyIngressObject parses body as one JSON object with invalid-UTF-8
// tolerance and duplicate-key rejection, returning ordered top-level members.
// Non-object roots, duplicate keys, or trailing tokens return an error.
func decodeRawLegacyIngressObject(body []byte) ([]rawLegacyIngressMember, bool, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, false, err
	}
	if tok.Kind() != '{' {
		return nil, false, errors.New("JSON root must be an object")
	}

	var members []rawLegacyIngressMember
	seen := make(map[string]bool)
	hasLegacy := false
	for dec.PeekKind() != '}' {
		keyTok, kErr := dec.ReadToken()
		if kErr != nil {
			return nil, false, kErr
		}
		name := keyTok.String()
		if seen[name] {
			return nil, false, fmt.Errorf("duplicate JSON object key %q", name)
		}
		seen[name] = true
		switch name {
		case "messages", "prompt", "commands":
			hasLegacy = true
		}
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return nil, false, vErr
		}
		members = append(members, rawLegacyIngressMember{
			name:  name,
			value: bytes.Clone(val),
		})
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, false, err
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, false, errors.New("multiple JSON values are not allowed")
		}
		return nil, false, err
	}
	return members, hasLegacy, nil
}

// marshalRawLegacyIngressObject writes members as a compact UTF-8 JSON object
// without HTML escaping or trailing newlines. Each member value is compacted
// before writing, and invalid raw JSON returns an error.
func marshalRawLegacyIngressObject(members []rawLegacyIngressMember) ([]byte, error) {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return nil, err
	}
	for _, m := range members {
		if err := enc.WriteToken(jsontext.String(m.name)); err != nil {
			return nil, err
		}
		val := jsontext.Value(bytes.Clone(m.value))
		if err := val.Compact(); err != nil {
			return nil, err
		}
		if err := enc.WriteValue(val); err != nil {
			return nil, err
		}
	}
	if err := enc.WriteToken(jsontext.EndObject); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(buf.Bytes(), []byte("\n")), nil
}

// findRawLegacyMember scans members in appearance order for key and returns its
// stored raw JSON value with true when present. Missing keys return nil and
// false without allocating or mutating members, and callers must not modify the
// returned slice in place.
func findRawLegacyMember(members []rawLegacyIngressMember, key string) (jsontext.Value, bool) {
	for _, m := range members {
		if m.name == key {
			return m.value, true
		}
	}
	return nil, false
}

// hasNonNullRawLegacyMember reports whether key appears in members with a
// non-blank value other than literal JSON null. Absent keys and explicit null
// values both return false so legacy fallbacks can populate missing inputs.
func hasNonNullRawLegacyMember(members []rawLegacyIngressMember, key string) bool {
	val, ok := findRawLegacyMember(members, key)
	return ok && isNonNullRawJSON(val)
}

// upsertRawLegacyMember replaces the value of key at its existing index when
// present or appends a new entry at the end of members. The stored jsontext
// bytes are cloned from val so callers retain independent ownership of their
// input slice.
func upsertRawLegacyMember(members *[]rawLegacyIngressMember, key string, val jsontext.Value) {
	cloned := jsontext.Value(bytes.Clone(val))
	for i := range *members {
		if (*members)[i].name == key {
			(*members)[i].value = cloned
			return
		}
	}
	*members = append(*members, rawLegacyIngressMember{name: key, value: cloned})
}

// deleteRawLegacyMember removes every member matching key from members in place
// while compacting the backing slice. All surviving entries retain their
// original relative appearance order and raw JSON byte slices.
func deleteRawLegacyMember(members *[]rawLegacyIngressMember, key string) {
	out := (*members)[:0]
	for _, m := range *members {
		if m.name != key {
			out = append(out, m)
		}
	}
	*members = out
}

// isNonNullRawJSON reports whether raw is non-empty after trimming whitespace and is not literal JSON null.
func isNonNullRawJSON(raw jsontext.Value) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

// isNonEmptyRawJSONArray reports whether raw begins a JSON array containing at
// least one element. Non-array or malformed inputs return false so legacy
// ingress removes non-array messages fields without invoking Chat conversion.
func isNonEmptyRawJSONArray(raw jsontext.Value) bool {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '[' {
		// Treat non-array or unreadable messages values as empty so outer ingress
		// drops the legacy alias without attempting Chat message conversion.
		return false
	}
	return dec.PeekKind() != ']' && dec.PeekKind() != 0
}

// hasChatStyleToolsRaw reports whether raw is a JSON array containing at least
// one tool object with a nested JSON object under its "function" member.
// Non-array or flat Responses tool declarations return false.
func hasChatStyleToolsRaw(raw jsontext.Value) bool {
	var elems []jsontext.Value
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	if err := jsonv2.UnmarshalDecode(dec, &elems); err != nil {
		// Non-array tools values cannot carry Chat function declarations, so leave
		// them untouched on the outer request for upstream validation.
		return false
	}
	return slices.ContainsFunc(elems, hasNestedFunctionObjectRaw)
}

// hasChatStyleToolChoiceRaw reports whether raw is a JSON object containing a
// nested JSON object under its "function" member. String selectors and flat
// Responses tool_choice objects return false.
func hasChatStyleToolChoiceRaw(raw jsontext.Value) bool {
	return hasNestedFunctionObjectRaw(raw)
}

// hasNestedFunctionObjectRaw reports whether raw is a JSON object whose
// "function" member is itself a JSON object. Scalar, array, or flat tool
// objects without a nested function member return false.
func hasNestedFunctionObjectRaw(raw jsontext.Value) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	var obj struct {
		Function jsontext.Value            `json:"function"` // candidate nested function member value
		Unknown  map[string]jsontext.Value `json:",embed"`   // remaining tool or choice members
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	if err := jsonv2.UnmarshalDecode(dec, &obj); err != nil {
		// Ignore malformed candidate tool entries here so flat or custom shapes do
		// not masquerade as Chat function wrappers.
		return false
	}
	fn := bytes.TrimSpace(obj.Function)
	return len(fn) > 0 && fn[0] == '{'
}

// decodeOptionalJSONString decodes raw when it is a JSON string literal and
// otherwise returns an empty string. Non-string values stay silent because
// callers use empty strings to detect absent or non-scalar optional fields.
func decodeOptionalJSONString(raw jsontext.Value) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '"' {
		return ""
	}
	var out string
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	if err := jsonv2.UnmarshalDecode(dec, &out); err != nil {
		// Return empty on malformed string tokens because validated object members
		// only reach here after root decoding has already checked syntax.
		return ""
	}
	return out
}
