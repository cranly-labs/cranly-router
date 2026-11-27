package openai

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
	"slices"
	"strings"
)

// encrypted_replay.go removes rejected encrypted reasoning and compaction state
// from OpenAI Responses request payloads for same-provider recovery retries.
// Recovery retries drop encrypted_content from reasoning items and discard
// encrypted compaction entries as complete objects while preserving plaintext
// summaries or reasoning content that can still inform the model.
//
// TrimEncryptedReasoningItems mutates decoded Go maps for legacy callers,
// whereas StripResponsesEncryptedReasoningBytes rewrites raw JSON bytes through
// ordered jsontext members with invalid UTF-8 tolerated so untouched Extra
// fields, member order, and 64-bit integer literals survive verbatim. Summary-
// free reasoning skeletons emptied by ciphertext removal are omitted from the
// rebuilt input array instead of forwarding orphan identifiers upstream.

// rawEncryptedReplayMember pairs one JSON object key with its verbatim raw value bytes.
type rawEncryptedReplayMember struct {
	name  string         // decoded JSON object key
	value jsontext.Value // cloned raw JSON value slice
}

// TrimEncryptedReasoningItems sanitizes the request's input for an encrypted-state
// recovery attempt and reports whether any supported item changed. Input may be
// []any, []map[string]any, or one map[string]any; other representations pass through
// with a false result. Retained items keep their order, and an emptied input is
// removed from reqBody after a change.
//
// Callers provide exclusively owned mutable request data or copy shared history
// first. Filtering reuses array storage, and the sanitizer edits reasoning maps
// before the top-level input is replaced. A false result means no supported
// cleanup was reported, rather than validating the request for an upstream.
func TrimEncryptedReasoningItems(reqBody map[string]any) bool {
	if len(reqBody) == 0 {
		return false
	}

	inputValue, has := reqBody["input"]
	if !has {
		return false
	}

	switch input := inputValue.(type) {
	case []any:
		// Compact retained entries into the same storage. Other slice aliases can
		// still expose dropped tail entries and overwritten positions after cleanup.
		filtered := input[:0]
		changed := false
		for _, item := range input {
			nextItem, itemChanged, keep := SanitizeEncryptedReasoningInputItem(item)
			if itemChanged {
				changed = true
			}
			if !keep {
				continue
			}
			filtered = append(filtered, nextItem)
		}
		if !changed {
			return false
		}
		if len(filtered) == 0 {
			delete(reqBody, "input")
			return true
		}
		reqBody["input"] = filtered
		return true
	case []map[string]any:
		// Keep the typed map-array form while compacting its existing storage;
		// retained maps share the sanitizer's in-place field deletions with aliases.
		filtered := input[:0]
		changed := false
		for _, item := range input {
			nextItem, itemChanged, keep := SanitizeEncryptedReasoningInputItem(item)
			if itemChanged {
				changed = true
			}
			if !keep {
				continue
			}
			nextMap, ok := nextItem.(map[string]any)
			// A replacement of another kind cannot enter a typed map array. Keep
			// this entry while earlier item mutations remain in the shared storage.
			if !ok {
				filtered = append(filtered, item)
				continue
			}
			filtered = append(filtered, nextMap)
		}
		if !changed {
			return false
		}
		if len(filtered) == 0 {
			delete(reqBody, "input")
			return true
		}
		reqBody["input"] = filtered
		return true
	case map[string]any:
		nextItem, changed, keep := SanitizeEncryptedReasoningInputItem(input)
		if !changed {
			return false
		}
		if !keep {
			delete(reqBody, "input")
			return true
		}
		nextMap, ok := nextItem.(map[string]any)
		// This branch installs map replacements. An unexpected replacement kind
		// leaves input assigned as before, after any mutation performed by sanitation.
		if !ok {
			return false
		}
		reqBody["input"] = nextMap
		return true
	default:
		return false
	}
}

// SanitizeEncryptedReasoningInputItem removes replay state from one decoded item
// and returns its replacement, change flag, and retention decision. Only
// map[string]any values with string type labels participate; surrounding type
// whitespace is ignored. Any encrypted_content key drops compaction or
// compaction_summary, including a key whose value is nil.
//
// Reasoning maps lose encrypted_content and literal nil content while other fields
// remain shared with the supplied map. A changed map containing only its type is
// dropped. Callers honor keep before installing next and isolate shared maps
// before calling; a dropped reasoning item may already have been mutated.
func SanitizeEncryptedReasoningInputItem(item any) (next any, changed bool, keep bool) {
	inputItem, ok := item.(map[string]any)
	if !ok {
		return item, false, true
	}

	itemType, _ := inputItem["type"].(string)
	switch strings.TrimSpace(itemType) {
	case "compaction", "compaction_summary":
		// Compaction recovery discards the whole encrypted item, including its
		// identifier; key presence governs this path even for an empty or nil value.
		if _, encrypted := inputItem["encrypted_content"]; encrypted {
			return nil, true, false
		}
		return item, false, true
	case "reasoning":
	default:
		return item, false, true
	}

	// Reasoning can retain its plaintext summary after ciphertext removal, so
	// delete this field in place before deciding whether useful attributes remain.
	if _, has := inputItem["encrypted_content"]; has {
		delete(inputItem, "encrypted_content")
		changed = true
	}

	// Literal nil content is removed independently of ciphertext. Recovery can
	// therefore retain a summary-only reasoning object even when no encrypted field
	// was supplied; an interface holding a typed nil value does not match this check.
	if v, has := inputItem["content"]; has && v == nil {
		delete(inputItem, "content")
		changed = true
	}

	if !changed {
		return item, false, true
	}
	// Field removal can leave only the type discriminator. Drop that skeleton
	// from replay; surviving summary, ID, or extension fields keep the object present.
	if len(inputItem) == 1 {
		return nil, true, false
	}
	return inputItem, true, true
}

// StripResponsesEncryptedReasoningBytes removes rejected encrypted_content
// fields and encrypted compaction entries from a Responses JSON request body
// using ordered jsontext members and reports whether body changed. Empty,
// malformed, or already unencrypted payloads return body unchanged and false.
//
// Reasoning items that still carry a non-empty summary array or non-empty
// content value keep their visible reasoning text (dropping synthetic id fields
// when top-level store is false), whereas reasoning items left without visible
// summary or content are removed from input[] completely. Emptying input[]
// retains an empty JSON array rather than deleting the required input field,
// and untouched sibling members and 64-bit integer literals survive verbatim.
func StripResponsesEncryptedReasoningBytes(body []byte) ([]byte, bool) {
	if len(body) == 0 || (!bytes.Contains(body, []byte("encrypted_content")) && !bytes.Contains(body, []byte(`\u`))) {
		return body, false
	}
	rootMembers, err := parseEncryptedReplayObject(body)
	if err != nil {
		// Malformed or non-object request payloads cannot be rewritten safely;
		// keep the caller's original slice intact for standard error handling.
		return body, false
	}
	rawInput, hasInput := findEncryptedReplayMember(rootMembers, "input")
	if !hasInput || len(rawInput) == 0 {
		return body, false
	}

	storeFalse := false
	if rawStore, hasStore := findEncryptedReplayMember(rootMembers, "store"); hasStore {
		var storeVal bool
		if jsonv2.Unmarshal(rawStore, &storeVal, jsontext.AllowInvalidUTF8(true)) == nil && !storeVal {
			storeFalse = true
		}
	}

	var items []jsontext.Value
	switch rawInput.Kind() {
	case '[':
		items, err = parseEncryptedReplayArray(rawInput)
		if err != nil {
			return body, false
		}
	case '{':
		items = []jsontext.Value{rawInput.Clone()}
	default:
		return body, false
	}

	kept := make([]jsontext.Value, 0, len(items))
	changed := false
	for _, elem := range items {
		rebuilt, elemChanged, keepElem, elemErr := stripEncryptedReplayInputItemBytes(elem, storeFalse)
		if elemErr != nil {
			// Abort the rewrite when an input item contains truncated JSON so callers
			// never dispatch a partially reconstructed transcript upstream.
			return body, false
		}
		if elemChanged {
			changed = true
		}
		if keepElem {
			kept = append(kept, rebuilt)
		}
	}
	if !changed {
		return body, false
	}

	encodedInput, err := encodeEncryptedReplayArray(kept)
	if err != nil {
		return body, false
	}
	rootMembers = setEncryptedReplayMember(rootMembers, "input", encodedInput)
	rebuiltRoot, err := encodeEncryptedReplayObject(rootMembers)
	if err != nil {
		return body, false
	}
	return rebuiltRoot, true
}

// stripEncryptedReplayInputItemBytes strips encrypted state from one raw input
// item value and reports the updated JSON value, whether it changed, and
// whether the item remains in input[]. Non-object values return unchanged.
func stripEncryptedReplayInputItemBytes(elem jsontext.Value, storeFalse bool) (jsontext.Value, bool, bool, error) {
	if len(elem) == 0 || elem.Kind() != '{' {
		return elem, false, true, nil
	}
	members, err := parseEncryptedReplayObject(elem)
	if err != nil {
		return elem, false, false, err
	}
	itemType := strings.TrimSpace(readEncryptedReplayMemberString(members, "type"))
	switch {
	case IsCompactionItemType(itemType):
		if _, hasEnc := findEncryptedReplayMember(members, "encrypted_content"); hasEnc {
			// Encrypted compaction items hold opaque server state that cannot be
			// replayed without ciphertext, so discard the entire compaction object.
			return nil, true, false, nil
		}
		return elem, false, true, nil

	case itemType == "reasoning":
		changed := false
		if _, hasEnc := findEncryptedReplayMember(members, "encrypted_content"); hasEnc {
			members = deleteEncryptedReplayMember(members, "encrypted_content")
			changed = true
		}
		if rawContent, hasContent := findEncryptedReplayMember(members, "content"); hasContent && bytes.Equal(bytes.TrimSpace(rawContent), []byte("null")) {
			members = deleteEncryptedReplayMember(members, "content")
			changed = true
		}
		if !changed {
			return elem, false, true, nil
		}
		if !hasVisibleReasoningContentBytes(members) {
			// Drop emptied reasoning skeletons that carry neither visible summary nor
			// plaintext content so upstream validators never reject orphan reasoning IDs.
			return nil, true, false, nil
		}
		if storeFalse {
			// Stateless store=false requests reject synthetic reasoning item IDs once
			// encrypted_content is removed, so strip id alongside the ciphertext.
			members = deleteEncryptedReplayMember(members, "id")
		}
		rebuilt, encErr := encodeEncryptedReplayObject(members)
		if encErr != nil {
			return nil, false, false, encErr
		}
		return rebuilt, true, true, nil

	default:
		return elem, false, true, nil
	}
}

// hasVisibleReasoningContentBytes reports whether a reasoning object's members
// retain a non-empty summary array or a non-empty content string or array.
func hasVisibleReasoningContentBytes(members []rawEncryptedReplayMember) bool {
	if rawSummary, ok := findEncryptedReplayMember(members, "summary"); ok && isNonEmptyJSONContentValue(rawSummary) {
		return true
	}
	if rawContent, ok := findEncryptedReplayMember(members, "content"); ok && isNonEmptyJSONContentValue(rawContent) {
		return true
	}
	return false
}

// isNonEmptyJSONContentValue reports whether raw encodes a non-blank JSON
// string, an object/scalar value, or a JSON array with at least one element.
// Empty slices, literal null, whitespace-only strings, and malformed arrays return false.
func isNonEmptyJSONContentValue(raw jsontext.Value) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return false
	}
	switch trimmed[0] {
	case '[':
		elems, err := parseEncryptedReplayArray(trimmed)
		return err == nil && len(elems) > 0
	case '"':
		var s string
		return jsonv2.Unmarshal(trimmed, &s, jsontext.AllowInvalidUTF8(true)) == nil && strings.TrimSpace(s) != ""
	default:
		return true
	}
}

// parseEncryptedReplayObject decodes one top-level JSON object into ordered raw
// members with invalid UTF-8 tolerated. Non-object roots, truncated members, or
// trailing tokens after the closing brace return an error.
func parseEncryptedReplayObject(raw []byte) ([]rawEncryptedReplayMember, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '{' {
		return nil, io.ErrUnexpectedEOF
	}
	var members []rawEncryptedReplayMember
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return nil, kErr
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return nil, vErr
		}
		members = append(members, rawEncryptedReplayMember{
			name:  key,
			value: val.Clone(),
		})
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return nil, io.ErrUnexpectedEOF
	}
	return members, nil
}

// encodeEncryptedReplayObject serializes ordered raw JSON object members into a
// single compact JSON object slice while accepting invalid UTF-8 strings.
// Token or value write failures abort serialization and return nil with the encoder error.
func encodeEncryptedReplayObject(members []rawEncryptedReplayMember) (jsontext.Value, error) {
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
	return jsontext.Value(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

// parseEncryptedReplayArray decodes a JSON array slice into cloned element
// values with invalid UTF-8 permitted. Non-array roots or malformed elements
// return an error before mutating caller slices.
func parseEncryptedReplayArray(raw jsontext.Value) ([]jsontext.Value, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '[' {
		return nil, io.ErrUnexpectedEOF
	}
	var elems []jsontext.Value
	for dec.PeekKind() != ']' {
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return nil, vErr
		}
		elems = append(elems, val.Clone())
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	return elems, nil
}

// encodeEncryptedReplayArray serializes raw JSON element values into a compact
// JSON array slice, emitting [] when elems is empty. Element write failures
// return nil alongside the underlying jsontext encoder error.
func encodeEncryptedReplayArray(elems []jsontext.Value) (jsontext.Value, error) {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := enc.WriteToken(jsontext.BeginArray); err != nil {
		return nil, err
	}
	for _, el := range elems {
		if err := enc.WriteValue(el); err != nil {
			return nil, err
		}
	}
	if err := enc.WriteToken(jsontext.EndArray); err != nil {
		return nil, err
	}
	return jsontext.Value(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

// findEncryptedReplayMember scans members from back to front and returns the
// last raw JSON value matching name under last-wins duplicate-key semantics.
// Absent keys return nil and false without copying slice storage.
func findEncryptedReplayMember(members []rawEncryptedReplayMember, name string) (jsontext.Value, bool) {
	for _, m := range slices.Backward(members) {
		if m.name == name {
			return m.value, true
		}
	}
	return nil, false
}

// readEncryptedReplayMemberString decodes the last string member named name
// from members with invalid UTF-8 tolerated. Absent or non-string values return "".
func readEncryptedReplayMemberString(members []rawEncryptedReplayMember, name string) string {
	raw, ok := findEncryptedReplayMember(members, name)
	if !ok || len(raw) == 0 || raw.Kind() != '"' {
		return ""
	}
	var s string
	if jsonv2.Unmarshal(raw, &s, jsontext.AllowInvalidUTF8(true)) != nil {
		return ""
	}
	return s
}

// setEncryptedReplayMember returns a newly allocated slice where the first
// member named name receives val in place and any later duplicates are dropped.
// When name is absent, the new member is appended at the end of the returned slice.
func setEncryptedReplayMember(members []rawEncryptedReplayMember, name string, val jsontext.Value) []rawEncryptedReplayMember {
	out := make([]rawEncryptedReplayMember, 0, len(members)+1)
	replaced := false
	for _, m := range members {
		if m.name == name {
			if !replaced {
				out = append(out, rawEncryptedReplayMember{name: name, value: val})
				replaced = true
			}
			continue
		}
		out = append(out, m)
	}
	if !replaced {
		out = append(out, rawEncryptedReplayMember{name: name, value: val})
	}
	return out
}

// deleteEncryptedReplayMember returns a newly allocated slice with all members
// matching name removed. Surviving entries retain their original relative order.
func deleteEncryptedReplayMember(members []rawEncryptedReplayMember, name string) []rawEncryptedReplayMember {
	out := make([]rawEncryptedReplayMember, 0, len(members))
	for _, m := range members {
		if m.name != name {
			out = append(out, m)
		}
	}
	return out
}
