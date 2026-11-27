package anthropic

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
)

// compat_replay_guard.go trims raw Anthropic Messages request histories to a
// recent message suffix while keeping paired tool_use and tool_result blocks
// together without decoding unmodeled JSON fields.
// Compatibility replay onto stateless upstream endpoints caps replayed history
// length, yet cutting at a fixed message count can separate a retained
// tool_result from an earlier assistant tool_use or strand a retained tool_use
// without its recorded result.
//
// Boundary selection scans raw content-block arrays with jsontext, records the
// earliest message index of every literal tool identifier, and expands the
// candidate suffix backward until no retained call or result references an
// earlier partner. Rebuilding only the messages array from raw jsontext.Value
// slices preserves top-level member order, unmodeled Extra fields, arbitrary-
// precision integers, and invalid UTF-8 byte sequences in retained messages.

// openAICompatAnthropicReplayMaxTailMessages sets the initial twelve-message
// suffix window before paired tool relationships expand the starting index.
const openAICompatAnthropicReplayMaxTailMessages = 12

// rawAnthropicObjectMember retains one top-level JSON object member's decoded
// name and raw value bytes during lossless request rewriting.
type rawAnthropicObjectMember struct {
	name  string         // decoded member name for key matching and emission
	value jsontext.Value // original raw JSON member value bytes
}

// ApplyAnthropicCompatFullReplayGuardBytes trims the top-level messages array in
// body to a recent suffix while keeping paired tool_use and tool_result history
// intact. Malformed JSON, absent or non-array messages, histories of twelve or
// fewer messages, and boundaries that expand back to index zero return body
// unchanged with false.
//
// Tool-boundary expansion matches literal non-empty block id and tool_use_id
// strings across message content arrays until the suffix start reaches a fixed
// point. When trimming occurs, retained message elements and all sibling
// top-level request fields are re-emitted from their original jsontext.Value
// slices so unmodeled fields, large integers, and invalid UTF-8 survive intact,
// and the returned int reports the retained message count.
func ApplyAnthropicCompatFullReplayGuardBytes(body []byte) ([]byte, int, bool) {
	if len(body) == 0 {
		return body, 0, false
	}

	members, messagesIdx, ok := readAnthropicTopLevelMembers(body)
	if !ok || messagesIdx < 0 {
		return body, 0, false
	}

	items, ok := readAnthropicRawJSONArray(members[messagesIdx].value)
	if !ok {
		return body, 0, false
	}
	if len(items) <= openAICompatAnthropicReplayMaxTailMessages {
		return body, len(items), false
	}

	// Start from the nominal tail window and expand backward before slicing so
	// cross-boundary tool_use and tool_result partners stay inside the replay suffix.
	start := len(items) - openAICompatAnthropicReplayMaxTailMessages
	start = expandAnthropicRawTrimBoundary(items, start)
	if start <= 0 {
		return body, len(items), false
	}

	retained := items[start:]
	encodedMessages, err := encodeAnthropicRawJSONArray(retained)
	if err != nil {
		// Retain the caller's original request bytes when array re-encoding fails
		// so replay preparation never forwards a partially serialized document.
		return body, len(items), false
	}

	members[messagesIdx].value = encodedMessages
	updated, err := encodeAnthropicTopLevelMembers(members)
	if err != nil {
		// Fall back to the untouched input slice if top-level object assembly fails,
		// keeping full valid history available for downstream request conversion.
		return body, len(items), false
	}
	return updated, len(retained), true
}

// readAnthropicTopLevelMembers parses a top-level JSON object into ordered raw
// members and locates the last messages field index. Non-object inputs, syntax
// errors, or trailing tokens after the root object return false.
func readAnthropicTopLevelMembers(body []byte) ([]rawAnthropicObjectMember, int, bool) {
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return nil, -1, false
	}

	var members []rawAnthropicObjectMember
	messagesIdx := -1
	for dec.PeekKind() != '}' {
		nameTok, err := dec.ReadToken()
		if err != nil || nameTok.Kind() != '"' {
			return nil, -1, false
		}
		name := nameTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return nil, -1, false
		}
		if name == "messages" {
			messagesIdx = len(members)
		}
		members = append(members, rawAnthropicObjectMember{
			name:  name,
			value: bytes.Clone(val),
		})
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, -1, false
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return nil, -1, false
	}
	return members, messagesIdx, true
}

// readAnthropicRawJSONArray splits a raw JSON array value into independent
// cloned element slices while permitting invalid UTF-8 bytes. Non-array JSON
// values or malformed element tokens return nil and false.
func readAnthropicRawJSONArray(raw jsontext.Value) ([]jsontext.Value, bool) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '[' {
		return nil, false
	}
	var items []jsontext.Value
	for dec.PeekKind() != ']' {
		val, err := dec.ReadValue()
		if err != nil {
			return nil, false
		}
		items = append(items, bytes.Clone(val))
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, false
	}
	return items, true
}

// expandAnthropicRawTrimBoundary moves start backward to include earliest
// indexed tool_use and tool_result partners referenced inside items[start:].
// Out-of-range or zero starts return unchanged.
//
// Each backward jump can pull intervening messages with additional tool IDs into
// the suffix, so expansion repeats until start reaches a fixed point or zero.
// Messages with string or non-array content contribute no tool IDs while
// remaining eligible for retention inside an expanded range.
func expandAnthropicRawTrimBoundary(items []jsontext.Value, start int) int {
	if start <= 0 || start >= len(items) {
		return start
	}

	useLists := make([][]string, len(items))
	resultLists := make([][]string, len(items))
	toolUseIndex := make(map[string]int)
	toolResultIndex := make(map[string]int)
	for i, item := range items {
		uses, results := extractAnthropicRawMessageToolIDs(item)
		useLists[i] = uses
		resultLists[i] = results
		for _, id := range uses {
			if _, exists := toolUseIndex[id]; !exists {
				toolUseIndex[id] = i
			}
		}
		for _, id := range results {
			if _, exists := toolResultIndex[id]; !exists {
				toolResultIndex[id] = i
			}
		}
	}

	// Enlarging the suffix can include intervening tool calls or results whose
	// partners sit even earlier; iterate until no partner precedes start.
	for {
		next := start
		for i := start; i < len(items); i++ {
			for _, id := range resultLists[i] {
				if useIdx, ok := toolUseIndex[id]; ok && useIdx < next {
					next = useIdx
				}
			}
			for _, id := range useLists[i] {
				if resultIdx, ok := toolResultIndex[id]; ok && resultIdx < next {
					next = resultIdx
				}
			}
		}
		if next == start {
			return start
		}
		start = next
	}
}

// extractAnthropicRawMessageToolIDs collects literal non-empty tool_use id and
// tool_result tool_use_id values from a raw Anthropic message object. String
// content or malformed blocks return nil slices without rejecting the message.
func extractAnthropicRawMessageToolIDs(rawMsg jsontext.Value) ([]string, []string) {
	dec := jsontext.NewDecoder(bytes.NewReader(rawMsg), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return nil, nil
	}

	var contentRaw jsontext.Value
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return nil, nil
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return nil, nil
		}
		if key == "content" {
			contentRaw = bytes.Clone(val)
		}
	}
	if len(contentRaw) == 0 {
		return nil, nil
	}

	blocks, ok := readAnthropicRawJSONArray(contentRaw)
	if !ok {
		// Ordinary string message content is valid in Anthropic requests and carries
		// no typed tool blocks; return empty ID lists while keeping the message.
		return nil, nil
	}

	var uses []string
	var results []string
	for _, block := range blocks {
		blockType, id, toolUseID := extractAnthropicRawBlockToolFields(block)
		switch blockType {
		case "tool_use":
			if id != "" {
				uses = append(uses, id)
			}
		case "tool_result":
			if toolUseID != "" {
				results = append(results, toolUseID)
			}
		}
	}
	return uses, results
}

// extractAnthropicRawBlockToolFields reads type, id, and tool_use_id string
// members from one Anthropic content block object. Non-object blocks or
// non-string field values yield empty strings for the affected attributes.
func extractAnthropicRawBlockToolFields(rawBlock jsontext.Value) (string, string, string) {
	dec := jsontext.NewDecoder(bytes.NewReader(rawBlock), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return "", "", ""
	}
	var blockType, id, toolUseID string
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return "", "", ""
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return "", "", ""
		}
		switch key {
		case "type":
			_ = jsonv2.Unmarshal(val, &blockType, jsontext.AllowInvalidUTF8(true))
		case "id":
			_ = jsonv2.Unmarshal(val, &id, jsontext.AllowInvalidUTF8(true))
		case "tool_use_id":
			_ = jsonv2.Unmarshal(val, &toolUseID, jsontext.AllowInvalidUTF8(true))
		}
	}
	return blockType, id, toolUseID
}

// encodeAnthropicRawJSONArray serializes a slice of raw JSON values as a
// compact JSON array while allowing invalid UTF-8 bytes inside retained
// elements. Token encoding errors return nil alongside the underlying error.
func encodeAnthropicRawJSONArray(items []jsontext.Value) (jsontext.Value, error) {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := enc.WriteToken(jsontext.BeginArray); err != nil {
		return nil, err
	}
	for _, item := range items {
		if err := enc.WriteValue(item); err != nil {
			return nil, err
		}
	}
	if err := enc.WriteToken(jsontext.EndArray); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// encodeAnthropicTopLevelMembers serializes ordered raw JSON object members
// into compact object bytes without re-decoding member values. Encoding errors
// return nil alongside the failing jsontext write error.
func encodeAnthropicTopLevelMembers(members []rawAnthropicObjectMember) ([]byte, error) {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return nil, err
	}
	for _, m := range members {
		if err := enc.WriteToken(jsontext.String(m.name)); err != nil {
			return nil, err
		}
		if err := enc.WriteValue(m.value); err != nil {
			return nil, err
		}
	}
	if err := enc.WriteToken(jsontext.EndObject); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
