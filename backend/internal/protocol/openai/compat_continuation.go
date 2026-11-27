package openai

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
	"strings"
)

// compat_continuation.go trims raw Responses input arrays for continuation
// requests and injects leading developer guard messages without re-decoding
// retained JSON items.
// Continuation requests carrying previous_response_id send only the newest
// turn's input items plus any earlier function_call entries required by
// retained function_call_output items, while non-Codex Claude Code bridges
// inject task-tracking guidance immediately after leading developer messages.
//
// Operating on raw jsontext.Value slices preserves top-level field ordering,
// structured array outputs on function_call_output items, unmodeled Extra
// extensions, 64-bit integer precision, and invalid UTF-8 sequences that typed
// ResponsesInputItem round trips would discard or coerce.

// rawResponsesObjectMember holds one top-level JSON object member's decoded
// name and raw value slice for lossless reassembly.
type rawResponsesObjectMember struct {
	name  string         // decoded member name for key lookup and emission
	value jsontext.Value // original raw JSON member value bytes
}

// compatResponsesItemHeader captures the minimal string discriminators needed
// to locate continuation turn boundaries and leading developer messages.
type compatResponsesItemHeader struct {
	itemType   string         // item "type" field value
	role       string         // item "role" field value
	callID     string         // item "call_id" field value
	contentRaw jsontext.Value // raw "content" field value when present
}

// compatDeveloperGuardPart serializes the single input_text part inside an
// injected developer guard message.
type compatDeveloperGuardPart struct {
	Type string `json:"type"` // literal "input_text" content part discriminator
	Text string `json:"text"` // injected developer guard guidance text
}

// compatDeveloperGuardItem serializes an injected developer message item for
// Responses input arrays.
type compatDeveloperGuardItem struct {
	Type    string                     `json:"type"`    // literal "message" input item discriminator
	Role    string                     `json:"role"`    // literal "developer" role value
	Content []compatDeveloperGuardPart `json:"content"` // single-element input_text content array
}

// TrimCompatResponsesInputToLatestTurnBytes trims the top-level input array in
// body to its latest continuation turn and any earlier paired function_call
// items. Empty or malformed JSON, missing or non-array input, single-turn
// arrays whose start stays at index zero, and re-encoding errors return body
// unchanged with false.
//
// When the final item is a function_call_output or an explicit user message
// preceded by function_call_output items, boundary selection includes that
// contiguous output run and walks backward to the nearest earlier function_call
// matching each trimmed non-empty call_id. Retained items and sibling request
// fields are emitted from their original jsontext.Value slices without loss.
func TrimCompatResponsesInputToLatestTurnBytes(body []byte) ([]byte, bool) {
	if len(body) == 0 {
		return body, false
	}

	members, inputIdx, ok := readResponsesTopLevelMembers(body)
	if !ok || inputIdx < 0 {
		return body, false
	}

	items, ok := readResponsesRawJSONArray(members[inputIdx].value)
	if !ok || len(items) == 0 {
		return body, false
	}

	headers := make([]compatResponsesItemHeader, len(items))
	for i, item := range items {
		headers[i] = inspectCompatResponsesItem(item)
	}

	start := latestCompatResponsesTurnStart(headers)
	if start <= 0 || start >= len(items) {
		return body, false
	}

	encodedInput, err := encodeResponsesRawJSONArray(items[start:])
	if err != nil {
		// Retain the caller's original request bytes when array serialization fails
		// so continuation preparation never emits truncated JSON.
		return body, false
	}
	members[inputIdx].value = encodedInput
	updated, err := encodeResponsesTopLevelMembers(members)
	if err != nil {
		// Keep the full valid request slice if root object encoding fails so
		// upstream forwarding still receives a complete document.
		return body, false
	}
	return updated, true
}

// InsertResponsesLeadingDeveloperGuardBytes inserts a developer message carrying
// guardText immediately after leading developer message items in body's input
// array. Malformed JSON, empty guardText, absent or empty array input, or an
// existing item whose content contains any non-blank marker return body
// unchanged with false.
//
// Leading item detection trims string type and role fields so whitespace-padded
// developer messages stay ahead of the injected guard while subsequent user,
// reasoning, and tool history keeps its exact order and raw JSON bytes.
func InsertResponsesLeadingDeveloperGuardBytes(body []byte, guardText string, markers ...string) ([]byte, bool) {
	if len(body) == 0 || guardText == "" {
		return body, false
	}

	members, inputIdx, ok := readResponsesTopLevelMembers(body)
	if !ok || inputIdx < 0 {
		return body, false
	}

	items, ok := readResponsesRawJSONArray(members[inputIdx].value)
	if !ok || len(items) == 0 {
		return body, false
	}

	headers := make([]compatResponsesItemHeader, len(items))
	for i, item := range items {
		headers[i] = inspectCompatResponsesItem(item)
		for _, marker := range markers {
			trimmedMarker := strings.TrimSpace(marker)
			if trimmedMarker != "" && rawContentContainsNeedle(headers[i].contentRaw, trimmedMarker) {
				return body, false
			}
		}
	}

	guardRaw, err := jsonv2.Marshal(compatDeveloperGuardItem{
		Type: "message",
		Role: "developer",
		Content: []compatDeveloperGuardPart{{
			Type: "input_text",
			Text: guardText,
		}},
	}, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		// Leave the caller's request untouched when guard marshaling fails rather
		// than mutating the input list with an empty element.
		return body, false
	}

	// Place the injected guard after initial developer instructions so task
	// guidance stays grouped with the leading instruction prefix.
	insertAt := 0
	for insertAt < len(headers) &&
		strings.TrimSpace(headers[insertAt].itemType) == "message" &&
		strings.TrimSpace(headers[insertAt].role) == "developer" {
		insertAt++
	}

	nextItems := make([]jsontext.Value, 0, len(items)+1)
	nextItems = append(nextItems, items[:insertAt]...)
	nextItems = append(nextItems, jsontext.Value(guardRaw))
	nextItems = append(nextItems, items[insertAt:]...)

	encodedInput, err := encodeResponsesRawJSONArray(nextItems)
	if err != nil {
		// Preserve the untouched input document if array assembly fails so the
		// caller can still forward the unguarded request safely.
		return body, false
	}
	members[inputIdx].value = encodedInput
	updated, err := encodeResponsesTopLevelMembers(members)
	if err != nil {
		// Return the original byte slice if top-level object re-encoding fails so
		// downstream stages never observe a half-written payload.
		return body, false
	}
	return updated, true
}

// latestCompatResponsesTurnStart computes the starting index of the newest
// continuation turn in headers. Empty slices return zero, while trailing
// function_call_output runs and user messages preceded by outputs expand
// backward to include matching earlier function_call entries.
func latestCompatResponsesTurnStart(headers []compatResponsesItemHeader) int {
	if len(headers) == 0 {
		return 0
	}

	start := len(headers) - 1
	last := headers[start]
	switch {
	case last.itemType == "function_call_output":
		for start > 0 && headers[start-1].itemType == "function_call_output" {
			start--
		}
	case last.itemType == "message" && last.role == "user":
		for start > 0 && headers[start-1].itemType == "function_call_output" {
			start--
		}
	default:
		return start
	}

	return expandCompatResponsesToolCallStart(headers, start)
}

// expandCompatResponsesToolCallStart walks backward from start to include the
// nearest earlier function_call item for each trimmed non-empty call_id
// referenced by function_call_output items in headers[start:]. Out-of-range
// start indices or suffixes without non-blank output IDs return start unchanged.
func expandCompatResponsesToolCallStart(headers []compatResponsesItemHeader, start int) int {
	if start < 0 || start >= len(headers) {
		return start
	}

	needed := make(map[string]struct{})
	for i := start; i < len(headers); i++ {
		if headers[i].itemType != "function_call_output" {
			continue
		}
		if callID := strings.TrimSpace(headers[i].callID); callID != "" {
			needed[callID] = struct{}{}
		}
	}
	if len(needed) == 0 {
		return start
	}

	// Retain intervening items whenever an earlier paired function_call shifts
	// the continuation start backward, matching typed continuation replay rules.
	expandedStart := start
	for i := start - 1; i >= 0 && len(needed) > 0; i-- {
		if headers[i].itemType != "function_call" {
			continue
		}
		callID := strings.TrimSpace(headers[i].callID)
		if _, ok := needed[callID]; !ok {
			continue
		}
		delete(needed, callID)
		expandedStart = i
	}
	return expandedStart
}

// inspectCompatResponsesItem reads top-level type, role, call_id, and cloned
// raw content members from one raw Responses input item object. Non-object
// items or malformed JSON tokens return a zero-valued header without failing.
func inspectCompatResponsesItem(rawItem jsontext.Value) compatResponsesItemHeader {
	dec := jsontext.NewDecoder(bytes.NewReader(rawItem), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return compatResponsesItemHeader{}
	}

	var hdr compatResponsesItemHeader
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return compatResponsesItemHeader{}
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return compatResponsesItemHeader{}
		}
		switch key {
		case "type":
			_ = jsonv2.Unmarshal(val, &hdr.itemType, jsontext.AllowInvalidUTF8(true))
		case "role":
			_ = jsonv2.Unmarshal(val, &hdr.role, jsontext.AllowInvalidUTF8(true))
		case "call_id":
			_ = jsonv2.Unmarshal(val, &hdr.callID, jsontext.AllowInvalidUTF8(true))
		case "content":
			hdr.contentRaw = bytes.Clone(val)
		}
	}
	return hdr
}

// rawContentContainsNeedle reports whether rawContent contains needle either in
// raw UTF-8 bytes or inside any decoded JSON string token within rawContent.
// Empty rawContent slices or blank needle strings return false immediately.
func rawContentContainsNeedle(rawContent jsontext.Value, needle string) bool {
	if len(rawContent) == 0 || needle == "" {
		return false
	}
	if bytes.Contains(rawContent, []byte(needle)) {
		return true
	}
	dec := jsontext.NewDecoder(bytes.NewReader(rawContent), jsontext.AllowInvalidUTF8(true))
	for {
		tok, err := dec.ReadToken()
		if err != nil {
			return false
		}
		if tok.Kind() == '"' && strings.Contains(tok.String(), needle) {
			return true
		}
	}
}

// readResponsesTopLevelMembers parses a root JSON object into ordered raw
// members and records the last top-level input member index when present.
// Non-object payloads, syntax errors, or trailing tokens after the root object
// return nil, -1, and false.
func readResponsesTopLevelMembers(body []byte) ([]rawResponsesObjectMember, int, bool) {
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return nil, -1, false
	}

	var members []rawResponsesObjectMember
	inputIdx := -1
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
		if name == "input" {
			inputIdx = len(members)
		}
		members = append(members, rawResponsesObjectMember{
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
	return members, inputIdx, true
}

// readResponsesRawJSONArray splits a raw JSON array value into independent
// cloned element slices while permitting invalid UTF-8 bytes. Non-array JSON
// values or malformed elements return nil and false.
func readResponsesRawJSONArray(raw jsontext.Value) ([]jsontext.Value, bool) {
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

// encodeResponsesRawJSONArray writes cloned raw JSON element values into a
// compact JSON array while preserving invalid UTF-8 sequences. Write errors
// return nil alongside the failing jsontext error.
func encodeResponsesRawJSONArray(items []jsontext.Value) (jsontext.Value, error) {
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

// encodeResponsesTopLevelMembers writes ordered raw JSON object members into
// compact root object bytes without re-decoding member values. Write errors
// return nil alongside the failing jsontext error.
func encodeResponsesTopLevelMembers(members []rawResponsesObjectMember) ([]byte, error) {
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
