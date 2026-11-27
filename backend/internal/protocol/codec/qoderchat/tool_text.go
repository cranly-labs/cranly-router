package qoderchat

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

// tool_text.go recognizes XML, embedded JSON, and DSML tool invocations
// streamed inside Qoder visible text deltas and converts them into structured
// tool call observations. Qoder routes backed by compatible open-weight models
// emit <tool_call> or <｜｜DSML｜｜tool_calls> markup directly on the text
// channel, often splitting opening tags and parameter blocks across chunk boundaries.
//
// Holding trailing prefixes that could complete an opening marker inside a
// linear byte buffer prevents split markup from leaking into client text while
// flushing incomplete or malformed segments verbatim as ordinary text when a
// non-text observation arrives or the stream ends. Deterministic call IDs derive
// from the invocation ordinal and raw segment bytes via SHA-256, and shell tool
// aliases normalize onto Bash before downstream argument assembly.

const (
	// XML text-call markers delimit <tool_call> blocks and named <arg_key> or
	// <arg_value> pairs emitted inside visible text deltas by compatible Qoder models.
	xmlToolCallStart = "<tool_call>"
	xmlToolCallEnd   = "</tool_call>"
	xmlArgKeyStart   = "<arg_key>"
	xmlArgKeyEnd     = "</arg_key>"
	xmlArgValueStart = "<arg_value>"
	xmlArgValueEnd   = "</arg_value>"

	// DSML text-call markers delimit DeepSeek-style <｜｜DSML｜｜tool_calls> wrappers,
	// child invoke elements, and named parameter blocks inside streamed text chunks.
	dsmlToolCallsStart = "<｜｜DSML｜｜tool_calls>"
	dsmlToolCallsEnd   = "</｜｜DSML｜｜tool_calls>"
	dsmlInvokeStart    = "<｜｜DSML｜｜invoke"
	dsmlInvokeEnd      = "</｜｜DSML｜｜invoke>"
	dsmlParameterStart = "<｜｜DSML｜｜parameter"
	dsmlParameterEnd   = "</｜｜DSML｜｜parameter>"

	// obsText through obsDone tag normalized stream observations passed from
	// wire frame parsing through text-tool recognition into StreamDecoder.
	obsText      rawObsKind = iota // incremental visible text delta
	obsReasoning                   // incremental thinking text delta
	obsToolCall                    // structured or parsed tool call fragment
	obsUsage                       // token usage observation
	obsDone                        // terminal stream marker
)

// rawObsKind classifies a normalized Qoder SSE observation before IR sequencing.
type rawObsKind uint8

// rawObservation represents one internal text, reasoning, tool, usage, or
// completion observation extracted from a Qoder SSE frame.
type rawObservation struct {
	kind             rawObsKind        // active observation variant
	text             string            // payload for obsText and obsReasoning
	toolCallID       string            // call identifier on obsToolCall
	toolCallIndex    int               // upstream slot index on obsToolCall
	hasToolCallIndex bool              // true when toolCallIndex was explicitly supplied
	toolType         string            // normalized tool type ("function")
	toolName         string            // invoked function name on obsToolCall
	arguments        string            // raw JSON argument fragment on obsToolCall
	usage            parsedUsageSample // extracted usage counters on obsUsage
}

// parsedUsageSample holds token counts extracted from one wireSSEUsage payload.
type parsedUsageSample struct {
	promptTokens       int64 // primary or alias prompt token count
	completionTokens   int64 // primary or alias completion token count
	hasPromptDetails   bool  // whether prompt_tokens_details was present
	cachedTokens       int64 // cached prompt input token count
	hasReasoningTokens bool  // whether completion_tokens_details was present
	reasoningTokens    int64 // generated completion reasoning token count
}

// wireTextJSONToolCall decodes a JSON tool invocation embedded inside an XML tool_call block.
type wireTextJSONToolCall struct {
	ID          string               `json:"id,omitzero"`           // preferred call identifier
	ToolCallID  string               `json:"tool_call_id,omitzero"` // alternate call identifier
	CallID      string               `json:"call_id,omitzero"`      // Responses-style call identifier
	Type        string               `json:"type,omitzero"`         // tool kind label
	Name        string               `json:"name,omitzero"`         // flat function name
	Tool        string               `json:"tool,omitzero"`         // alternate flat tool name
	ToolName    string               `json:"tool_name,omitzero"`    // alternate flat tool_name
	Arguments   jsontext.Value       `json:"arguments,omitzero"`    // flat arguments JSON
	Input       jsontext.Value       `json:"input,omitzero"`        // flat input JSON
	Parameters  jsontext.Value       `json:"parameters,omitzero"`   // flat parameters JSON
	Command     jsontext.Value       `json:"command,omitzero"`      // inline shell command field
	Cmd         jsontext.Value       `json:"cmd,omitzero"`          // inline shell cmd alias
	Description jsontext.Value       `json:"description,omitzero"`  // inline shell description field
	Function    wireTextJSONFunction `json:"function,omitzero"`     // nested function payload
}

// wireTextJSONFunction decodes nested function fields inside a JSON text tool call.
type wireTextJSONFunction struct {
	Name      string         `json:"name,omitzero"`      // nested function name
	Arguments jsontext.Value `json:"arguments,omitzero"` // nested function arguments
}

// textToolCallTransformer buffers potential XML and DSML tool-call markup across
// consecutive visible text deltas and emits structured obsToolCall observations.
type textToolCallTransformer struct {
	// buf accumulates pending visible text bytes in linear amortized time so
	// unclosed markup across many frames never triggers quadratic string copies.
	buf          bytes.Buffer // buffered visible text suffix
	nextToolCall int          // monotonic synthetic call ordinal
}

// NormalizeToolCallNameAndArguments normalizes shell tool aliases ("shell" and
// "execute_bash") onto "Bash" and rewrites Bash "cmd" or plain command text
// arguments into canonical {"command": ...} JSON objects. Non-shell tool names
// keep their trimmed spelling and original argument text.
//
// Malformed JSON fragments beginning with '{' or '[' remain untouched on Bash
// calls so partial deltas continue merging inside toolCallAccumulator and
// surface validation errors if truncated at stream completion.
func NormalizeToolCallNameAndArguments(name, arguments string) (string, string) {
	trimmedName := strings.TrimSpace(name)
	normalizedName := trimmedName
	if strings.EqualFold(trimmedName, "shell") || strings.EqualFold(trimmedName, "execute_bash") {
		normalizedName = "Bash"
	}
	if normalizedName != "Bash" {
		return normalizedName, arguments
	}

	var args map[string]jsontext.Value
	if err := unmarshalPermissive([]byte(arguments), &args); err != nil {
		cmd := strings.TrimSpace(arguments)
		if cmd == "" || cmd == "{}" {
			return normalizedName, "{}"
		}
		if strings.HasPrefix(cmd, "{") || strings.HasPrefix(cmd, "[") {
			return normalizedName, arguments
		}
		// Plain non-JSON shell command strings upgrade into a structured {"command": ...}
		// JSON object so downstream function-tool consumers receive valid object arguments.
		wrapped, marshalErr := marshalDeterministic(map[string]string{"command": cmd})
		if marshalErr != nil {
			return normalizedName, arguments
		}
		return normalizedName, string(wrapped)
	}
	if _, hasCommand := args["command"]; !hasCommand {
		if cmdVal, hasCmd := args["cmd"]; hasCmd {
			if args == nil {
				args = make(map[string]jsontext.Value, 1)
			}
			args["command"] = cmdVal
			delete(args, "cmd")
		}
	}
	if args == nil {
		return normalizedName, "{}"
	}
	encoded, err := marshalDeterministic(args)
	if err != nil {
		return normalizedName, arguments
	}
	return normalizedName, string(encoded)
}

// append processes one rawObservation, buffering obsText fragments to extract
// complete XML or DSML tool calls and flushing buffered text ahead of any
// non-text observation. Empty text observations return nil without mutating state.
func (t *textToolCallTransformer) append(obs rawObservation) []rawObservation {
	if obs.kind == obsText {
		if obs.text == "" {
			return nil
		}
		_, _ = t.buf.WriteString(obs.text)
		return t.drain(false)
	}
	out := t.drain(true)
	out = append(out, obs)
	return out
}

// flush releases all remaining buffered text or completed tool calls at stream
// termination and clears t.buf. Unclosed or malformed trailing markup flushes
// verbatim as visible text so truncated prose never disappears.
func (t *textToolCallTransformer) flush() []rawObservation {
	return t.drain(true)
}

// drain extracts complete text and tool-call observations from t.buf. Empty
// buffers return nil immediately without allocating an output slice.
//
// Opening <tool_call> or <｜｜DSML｜｜tool_calls> tags can split at any byte
// boundary across consecutive SSE text chunks. When final is false, trailing
// bytes that form a proper prefix of an opening tag remain buffered for the
// next text delta while preceding text flushes immediately; when final is true,
// incomplete or unparseable segments release verbatim as visible text so user
// prose is never dropped.
func (t *textToolCallTransformer) drain(final bool) []rawObservation {
	if t.buf.Len() == 0 {
		return nil
	}
	data := t.buf.String()
	var out []rawObservation

	for data != "" {
		start, marker := findToolCallMarkerStart(data)
		if start < 0 {
			if final {
				out = append(out, rawObservation{kind: obsText, text: data})
				data = ""
				break
			}
			// Retain any trailing prefix of an opening tag so split markup across
			// adjacent SSE chunks can complete on the following text delta.
			suffixLen := openingMarkerSuffixLen(data)
			emitText := data[:len(data)-suffixLen]
			data = data[len(data)-suffixLen:]
			if emitText != "" {
				out = append(out, rawObservation{kind: obsText, text: emitText})
			}
			break
		}
		if start > 0 {
			out = append(out, rawObservation{kind: obsText, text: data[:start]})
			data = data[start:]
			continue
		}

		startTag, endTag := markerTagPair(marker)
		endRel := strings.Index(data[len(startTag):], endTag)
		if endRel < 0 {
			if final {
				out = append(out, rawObservation{kind: obsText, text: data})
				data = ""
			}
			break
		}

		segmentEnd := len(startTag) + endRel + len(endTag)
		segment := data[:segmentEnd]
		// Preserve complete segments as visible text whenever inner tool parsing
		// fails so prose containing literal <tool_call> examples remains intact.
		if parsed, ok := t.parseSegment(segment, marker); ok {
			out = append(out, parsed...)
		} else {
			out = append(out, rawObservation{kind: obsText, text: segment})
		}
		data = data[segmentEnd:]
	}

	t.buf.Reset()
	if data != "" {
		_, _ = t.buf.WriteString(data)
	}
	return out
}

// findToolCallMarkerStart returns the earliest byte offset of an XML or DSML
// opening marker inside text alongside the matched opening tag constant. When
// neither opening marker appears in text, it returns -1 and an unused tag string.
func findToolCallMarkerStart(text string) (int, string) {
	xmlIdx := strings.Index(text, xmlToolCallStart)
	dsmlIdx := strings.Index(text, dsmlToolCallsStart)
	if xmlIdx < 0 {
		return dsmlIdx, dsmlToolCallsStart
	}
	if dsmlIdx < 0 || xmlIdx < dsmlIdx {
		return xmlIdx, xmlToolCallStart
	}
	return dsmlIdx, dsmlToolCallsStart
}

// markerTagPair returns the opening and closing tag delimiters corresponding
// to marker for segment framing. Unrecognized marker strings default to the XML
// <tool_call> and </tool_call> delimiter pair so callers always receive non-empty tags.
func markerTagPair(marker string) (string, string) {
	if marker == dsmlToolCallsStart {
		return dsmlToolCallsStart, dsmlToolCallsEnd
	}
	return xmlToolCallStart, xmlToolCallEnd
}

// openingMarkerSuffixLen returns the byte length of the longest proper prefix
// of an XML or DSML opening tag that appears as a trailing suffix of text.
// Complete opening tags are excluded because findToolCallMarkerStart handles them.
func openingMarkerSuffixLen(text string) int {
	maxSuffix := 0
	for _, marker := range []string{xmlToolCallStart, dsmlToolCallsStart} {
		limit := min(len(marker)-1, len(text))
		for n := limit; n > maxSuffix; n-- {
			if strings.HasPrefix(marker, text[len(text)-n:]) {
				maxSuffix = n
				break
			}
		}
	}
	return maxSuffix
}

// parseSegment dispatches a complete framed tool-call segment to DSML or
// XML/JSON parsing according to marker. Segments that fail inner structural
// validation return (nil, false) so drain emits the raw text unchanged.
func (t *textToolCallTransformer) parseSegment(segment, marker string) ([]rawObservation, bool) {
	if marker == dsmlToolCallsStart {
		return t.parseDSMLSegment(segment)
	}
	obs, ok := t.parseXMLOrJSONSegment(segment)
	if !ok {
		return nil, false
	}
	return []rawObservation{obs}, true
}

// parseXMLOrJSONSegment parses one complete <tool_call>...</tool_call> segment
// into a structured obsToolCall observation and reports whether recognition
// succeeded. Blank inner bodies or missing argument delimiters return false so
// drain emits the raw markup as literal text.
//
// Compatible models emit either serialized JSON objects or XML-style function
// names followed by <arg_key> and <arg_value> pairs inside <tool_call> tags.
// Trying embedded JSON recognition first prevents a valid JSON object from
// being misclassified as a function name, and only successful parses advance
// t.nextToolCall and derive a deterministic SHA-256 call identifier.
func (t *textToolCallTransformer) parseXMLOrJSONSegment(segment string) (rawObservation, bool) {
	inner := strings.TrimPrefix(segment, xmlToolCallStart)
	inner = strings.TrimSuffix(inner, xmlToolCallEnd)
	inner = strings.TrimSpace(inner)
	if inner == "" {
		return rawObservation{}, false
	}
	if obs, ok := t.parseEmbeddedJSONCall(inner, segment); ok {
		return obs, true
	}

	firstTag := len(inner)
	for _, tag := range []string{xmlArgValueStart, xmlArgKeyStart} {
		if idx := strings.Index(inner, tag); idx >= 0 && idx < firstTag {
			firstTag = idx
		}
	}
	name := strings.TrimSpace(unescapeEntities(inner[:firstTag]))
	if name == "" {
		return rawObservation{}, false
	}

	args := make(map[string]string)
	rest := inner[firstTag:]
	for {
		keyOpen := strings.Index(rest, xmlArgKeyStart)
		if keyOpen < 0 {
			break
		}
		keyStart := keyOpen + len(xmlArgKeyStart)
		keyEnd := strings.Index(rest[keyStart:], xmlArgKeyEnd)
		if keyEnd < 0 {
			return rawObservation{}, false
		}
		key := strings.TrimSpace(unescapeEntities(rest[keyStart : keyStart+keyEnd]))
		afterKey := rest[keyStart+keyEnd+len(xmlArgKeyEnd):]

		valOpen := strings.Index(afterKey, xmlArgValueStart)
		if valOpen < 0 {
			return rawObservation{}, false
		}
		valStart := valOpen + len(xmlArgValueStart)
		valEnd := strings.Index(afterKey[valStart:], xmlArgValueEnd)
		if valEnd < 0 {
			return rawObservation{}, false
		}
		if key != "" {
			args[key] = unescapeEntities(afterKey[valStart : valStart+valEnd])
		}
		rest = afterKey[valStart+valEnd+len(xmlArgValueEnd):]
	}

	argBytes, err := marshalDeterministic(args)
	if err != nil {
		return rawObservation{}, false
	}
	normName, normArgs := NormalizeToolCallNameAndArguments(name, string(argBytes))
	idx := t.nextToolCall
	callID := textToolCallID(idx, segment)
	t.nextToolCall++
	return rawObservation{
		kind:             obsToolCall,
		toolCallID:       callID,
		toolCallIndex:    idx,
		hasToolCallIndex: true,
		toolType:         "function",
		toolName:         normName,
		arguments:        normArgs,
	}, true
}

// parseEmbeddedJSONCall parses a JSON object inside <tool_call>{...}</tool_call>,
// resolving nested function or flat name/argument aliases and inline shell fields.
// Non-object payloads, duplicate JSON keys, or unnamed objects return false
// without advancing t.nextToolCall.
func (t *textToolCallTransformer) parseEmbeddedJSONCall(inner, segment string) (rawObservation, bool) {
	decoded := strings.TrimSpace(unescapeEntities(inner))
	if !strings.HasPrefix(decoded, "{") {
		return rawObservation{}, false
	}
	var payload wireTextJSONToolCall
	if err := unmarshalPermissive([]byte(decoded), &payload); err != nil {
		return rawObservation{}, false
	}
	name := firstNonEmptyString(
		payload.Function.Name,
		payload.Name,
		payload.Tool,
		payload.ToolName,
	)
	if name == "" {
		return rawObservation{}, false
	}
	inlineArgs := buildInlineShellArguments(payload)
	args := firstPresentToolArguments(
		payload.Function.Arguments,
		payload.Arguments,
		payload.Input,
		payload.Parameters,
		inlineArgs,
	)
	normName, normArgs := NormalizeToolCallNameAndArguments(name, args)
	idx := t.nextToolCall
	callID := firstNonEmptyString(
		payload.ID,
		payload.ToolCallID,
		payload.CallID,
		textToolCallID(idx, segment),
	)
	t.nextToolCall++
	return rawObservation{
		kind:             obsToolCall,
		toolCallID:       callID,
		toolCallIndex:    idx,
		hasToolCallIndex: true,
		toolType:         firstNonEmptyString(payload.Type, "function"),
		toolName:         normName,
		arguments:        normArgs,
	}, true
}

// buildInlineShellArguments packs top-level command, cmd, or description fields
// from a flat JSON text tool call into a deterministic JSON object candidate.
// Payloads lacking all three inline shell fields return nil without allocating JSON.
func buildInlineShellArguments(p wireTextJSONToolCall) jsontext.Value {
	fields := make(map[string]jsontext.Value)
	if len(bytes.TrimSpace(p.Command)) > 0 {
		fields["command"] = p.Command
	}
	if len(bytes.TrimSpace(p.Cmd)) > 0 {
		fields["cmd"] = p.Cmd
	}
	if len(bytes.TrimSpace(p.Description)) > 0 {
		fields["description"] = p.Description
	}
	if len(fields) == 0 {
		return nil
	}
	raw, err := marshalDeterministic(fields)
	if err != nil {
		return nil
	}
	return jsontext.Value(raw)
}

// firstPresentToolArguments returns the first non-blank serialized JSON argument
// string across candidates after unquoting string-encoded JSON payloads. When
// every candidate is empty, null, or whitespace-only, it defaults to "{}".
func firstPresentToolArguments(candidates ...jsontext.Value) string {
	for _, raw := range candidates {
		trimmed := bytes.TrimSpace(raw)
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
			continue
		}
		if trimmed[0] == '"' {
			var s string
			if err := unmarshalPermissive(trimmed, &s); err == nil {
				if strings.TrimSpace(s) != "" {
					return s
				}
				continue
			}
		}
		return string(trimmed)
	}
	return "{}"
}

// parseDSMLSegment scans a complete <｜｜DSML｜｜tool_calls> wrapper and converts
// each valid <｜｜DSML｜｜invoke> block into an obsToolCall observation. Empty
// wrappers, unclosed invoke tags, or wrappers yielding zero valid calls return false.
func (t *textToolCallTransformer) parseDSMLSegment(segment string) ([]rawObservation, bool) {
	inner := strings.TrimPrefix(segment, dsmlToolCallsStart)
	inner = strings.TrimSuffix(inner, dsmlToolCallsEnd)
	inner = strings.TrimSpace(inner)
	if inner == "" {
		return nil, false
	}
	var out []rawObservation
	rest := inner
	for {
		start := strings.Index(rest, dsmlInvokeStart)
		if start < 0 {
			break
		}
		rest = rest[start:]
		openEnd := strings.IndexByte(rest, '>')
		if openEnd < 0 {
			return nil, false
		}
		closeRel := strings.Index(rest[openEnd+1:], dsmlInvokeEnd)
		if closeRel < 0 {
			return nil, false
		}
		openTag := rest[:openEnd+1]
		body := rest[openEnd+1 : openEnd+1+closeRel]
		// Skip unnamed or parameterless child invocations while keeping sibling
		// valid invocations inside the same DSML wrapper.
		if obs, ok := t.parseDSMLInvoke(openTag, body, segment); ok {
			out = append(out, obs)
		}
		rest = rest[openEnd+1+closeRel+len(dsmlInvokeEnd):]
	}
	return out, len(out) > 0
}

// parseDSMLInvoke converts one DSML invoke tag and its named parameter children
// into a normalized obsToolCall observation. Unnamed invokes, malformed
// parameter tags, or invokes with zero named parameters return false.
func (t *textToolCallTransformer) parseDSMLInvoke(openTag, body, segment string) (rawObservation, bool) {
	name := extractTagAttribute(openTag, "name")
	if name == "" {
		return rawObservation{}, false
	}
	args := make(map[string]string)
	rest := body
	for {
		start := strings.Index(rest, dsmlParameterStart)
		if start < 0 {
			break
		}
		rest = rest[start:]
		openEnd := strings.IndexByte(rest, '>')
		if openEnd < 0 {
			return rawObservation{}, false
		}
		closeRel := strings.Index(rest[openEnd+1:], dsmlParameterEnd)
		if closeRel < 0 {
			return rawObservation{}, false
		}
		paramTag := rest[:openEnd+1]
		paramName := extractTagAttribute(paramTag, "name")
		if paramName != "" {
			args[paramName] = unescapeEntities(rest[openEnd+1 : openEnd+1+closeRel])
		}
		rest = rest[openEnd+1+closeRel+len(dsmlParameterEnd):]
	}
	if len(args) == 0 {
		return rawObservation{}, false
	}
	argBytes, err := marshalDeterministic(args)
	if err != nil {
		return rawObservation{}, false
	}
	normName, normArgs := NormalizeToolCallNameAndArguments(name, string(argBytes))
	idx := t.nextToolCall
	t.nextToolCall++
	return rawObservation{
		kind:             obsToolCall,
		toolCallID:       textToolCallID(idx, segment),
		toolCallIndex:    idx,
		hasToolCallIndex: true,
		toolType:         "function",
		toolName:         normName,
		arguments:        normArgs,
	}, true
}

// extractTagAttribute reads, entity-unescapes, and trims a double-quoted XML or
// DSML attribute named attrName from tag. Missing attributes or unclosed quote
// delimiters return an empty string so malformed invoke tags fail cleanly.
func extractTagAttribute(tag, attrName string) string {
	pattern := attrName + `="`
	start := strings.Index(tag, pattern)
	if start < 0 {
		return ""
	}
	start += len(pattern)
	end := strings.IndexByte(tag[start:], '"')
	if end < 0 {
		return ""
	}
	return strings.TrimSpace(unescapeEntities(tag[start : start+end]))
}

// textToolCallID derives a deterministic "call_<24hex>" identifier from the
// zero-based invocation ordinal and raw segment text using SHA-256. Repeated
// runs on identical text-encoded tool calls produce identical call IDs.
func textToolCallID(index int, segment string) string {
	sum := sha256.Sum256(fmt.Appendf(nil, "%d\n%s", index, segment))
	// Truncate the SHA-256 digest to twelve bytes (24 hex characters) so generated
	// call IDs fit compactly inside OpenAI and Anthropic tool_call_id length limits.
	return "call_" + hex.EncodeToString(sum[:12])
}

// unescapeEntities decodes XML and common HTML character entities (&quot;,
// &apos;, &lt;, &gt;, &amp;, &nbsp;, &#dec;, and &#xhex;) without importing html.
// Strings without '&' return unchanged, and malformed or unclosed entity spans
// remain literal text.
func unescapeEntities(s string) string {
	if !strings.ContainsRune(s, '&') {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	i := 0
	for i < len(s) {
		amp := strings.IndexByte(s[i:], '&')
		if amp < 0 {
			_, _ = b.WriteString(s[i:])
			break
		}
		_, _ = b.WriteString(s[i : i+amp])
		i += amp
		semi := strings.IndexByte(s[i:], ';')
		// Cap semicolon lookahead at 16 bytes so long unescaped shell expressions
		// containing '&&' operators keep linear scanning cost without backtracking.
		if semi <= 1 || semi > 16 {
			_ = b.WriteByte('&')
			i++
			continue
		}
		entity := s[i+1 : i+semi]
		if decoded, ok := decodeSingleEntity(entity); ok {
			_, _ = b.WriteString(decoded)
			i += semi + 1
			continue
		}
		_ = b.WriteByte('&')
		i++
	}
	return b.String()
}

// decodeSingleEntity resolves one entity body between '&' and ';' into UTF-8
// text and reports whether entity matched a recognized named or numeric form.
// Out-of-range or surrogate numeric code points map to utf8.RuneError.
func decodeSingleEntity(entity string) (string, bool) {
	switch entity {
	case "quot", "QUOT":
		return `"`, true
	case "apos":
		return `'`, true
	case "lt", "LT":
		return `<`, true
	case "gt", "GT":
		return `>`, true
	case "amp", "AMP":
		return `&`, true
	case "nbsp":
		return "\u00a0", true
	}
	if strings.HasPrefix(entity, "#") {
		numStr := entity[1:]
		base := 10
		if strings.HasPrefix(numStr, "x") || strings.HasPrefix(numStr, "X") {
			base = 16
			numStr = numStr[1:]
		}
		if numStr == "" {
			return "", false
		}
		cp, err := strconv.ParseInt(numStr, base, 32)
		if err != nil || cp <= 0 || cp > utf8.MaxRune || !utf8.ValidRune(rune(cp)) {
			return string(utf8.RuneError), err == nil
		}
		return string(rune(cp)), true
	}
	return "", false
}
