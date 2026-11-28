package anthropic

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"io"
	"slices"
)

// signature_json.go strips rejected historical thinking signatures and blocks
// from Anthropic Messages request payloads without rounding large integers.
// Cross-model Claude failover attempts can reject replayed reasoning signatures
// with HTTP 400 before output starts. Standard Claude endpoints reject unsigned
// historical thinking blocks outright and require active tool loops to omit
// top-level thinking when their final assistant turn no longer begins with
// reasoning, whereas passback-compatible endpoints accept unsigned thinking text.
//
// StripMessagesSignatureBlocksForRetryBytes streams ordered jsontext members
// with invalid UTF-8 tolerated so untouched Extra fields, key ordering, and
// 64-bit integer literals survive verbatim across standard and passback retries.

const (
	// signatureRetryAssistantPlaceholder fills an assistant content array emptied
	// after rejected thinking and redacted_thinking blocks are removed on retry.
	signatureRetryAssistantPlaceholder = `{"type":"text","text":"(assistant content removed)"}`
	// clearThinkingStrategyType names the context_management edit strategy that
	// Anthropic rejects when top-level thinking is disabled or removed on retry.
	clearThinkingStrategyType = "clear_thinking_20251015"
)

// rawSignatureMember holds one JSON object key and its cloned raw value bytes in
// wire encounter order during lossless signature-retry rewriting.
type rawSignatureMember struct {
	name  string         // decoded JSON member key
	value jsontext.Value // verbatim raw JSON value slice
}

// SignatureRetryLosses counts nonempty replay tokens and visible reasoning
// text actually removed from a successfully encoded Messages recovery payload.
type SignatureRetryLosses struct {
	Tokens    int // removed nonempty signatures or redacted payloads
	Reasoning int // removed blocks with nonempty reasoning text
}

// StripMessagesSignatureBlocksForRetryBytes removes rejected historical
// thinking signatures or reasoning blocks from an Anthropic Messages request
// body via ordered jsontext members and reports whether body changed. Empty,
// malformed, or already signature-free payloads return body unchanged and false.
//
// Standard Claude routes (passback false) drop assistant thinking and
// redacted_thinking blocks, insert an assistant text placeholder when removal
// empties a turn, and delete top-level thinking plus dependent clear-thinking
// context edits when an active tool loop loses its leading reasoning block.
// Passback routes (passback true) strip signature fields from non-empty
// thinking blocks while dropping empty thinking and redacted_thinking blocks.
func StripMessagesSignatureBlocksForRetryBytes(body []byte, passback bool) ([]byte, bool) {
	rewritten, changed, _ := StripMessagesSignatureBlocksWithLosses(body, passback)
	return rewritten, changed
}

// StripMessagesSignatureBlocksWithLosses applies the Messages signature retry
// and counts what it removed from the outgoing payload. Partial rewrites return
// the original body and zero losses, so diagnostics never describe an unsent draft.
func StripMessagesSignatureBlocksWithLosses(body []byte, passback bool) ([]byte, bool, SignatureRetryLosses) {
	if len(body) == 0 || (!bytes.Contains(body, []byte("thinking")) && !bytes.Contains(body, []byte(`\u`))) {
		return body, false, SignatureRetryLosses{}
	}
	rootMembers, err := parseOrderedSignatureObject(body)
	if err != nil {
		// Malformed or non-object request payloads cannot be repaired safely; keep
		// the caller's original byte slice intact for standard HTTP 400 presentation.
		return body, false, SignatureRetryLosses{}
	}
	rawMsgs, ok := findSignatureMember(rootMembers, "messages")
	if !ok || len(rawMsgs) == 0 || rawMsgs.Kind() != '[' {
		return body, false, SignatureRetryLosses{}
	}
	msgElems, err := parseSignatureArray(rawMsgs)
	if err != nil || len(msgElems) == 0 {
		return body, false, SignatureRetryLosses{}
	}

	var losses SignatureRetryLosses
	rebuiltMsgs := make([]jsontext.Value, len(msgElems))
	anyMsgChanged := false
	for i, elem := range msgElems {
		rebuilt, changed, msgErr := stripAssistantSignatureBlocksInMessage(elem, passback, &losses)
		if msgErr != nil {
			// Abort the rewrite if any message element contains truncated JSON so
			// callers never send a partially rewritten conversation upstream.
			return body, false, SignatureRetryLosses{}
		}
		rebuiltMsgs[i] = rebuilt
		if changed {
			anyMsgChanged = true
		}
	}

	topChanged := false
	if !passback && isSignatureTopThinkingEnabled(rootMembers) {
		lacksLeading, loopErr := signatureToolLoopLacksLeadingThinking(rebuiltMsgs)
		if loopErr != nil {
			return body, false, SignatureRetryLosses{}
		}
		if lacksLeading {
			// Active [assistant(tool_use), user(tool_result)] loops fail Anthropic
			// validation when thinking remains enabled without a leading thinking block.
			rootMembers = deleteSignatureMember(rootMembers, "thinking")
			rootMembers = stripClearThinkingContextStrategyMembers(rootMembers)
			topChanged = true
		}
	}

	if !anyMsgChanged && !topChanged {
		return body, false, SignatureRetryLosses{}
	}
	if anyMsgChanged {
		encodedMsgs, encErr := encodeSignatureArray(rebuiltMsgs)
		if encErr != nil {
			return body, false, SignatureRetryLosses{}
		}
		rootMembers = setSignatureMember(rootMembers, "messages", encodedMsgs)
	}
	out, err := encodeOrderedSignatureObject(rootMembers)
	if err != nil {
		return body, false, SignatureRetryLosses{}
	}
	return out, true, losses
}

// stripAssistantSignatureBlocksInMessage rewrites one assistant message element
// to remove rejected thinking signatures or reasoning blocks while leaving user
// messages, string content, and unrelated members byte-for-byte untouched.
// Emptied assistant content arrays receive signatureRetryAssistantPlaceholder
// so Anthropic does not reject empty content on retry.
func stripAssistantSignatureBlocksInMessage(elem jsontext.Value, passback bool, losses *SignatureRetryLosses) (jsontext.Value, bool, error) {
	if len(elem) == 0 || elem.Kind() != '{' {
		return elem, false, nil
	}
	mMembers, err := parseOrderedSignatureObject(elem)
	if err != nil {
		return elem, false, err
	}
	role, _ := readSignatureMemberString(mMembers, "role")
	rawContent, hasContent := findSignatureMember(mMembers, "content")
	if role != "assistant" || !hasContent || len(rawContent) == 0 || rawContent.Kind() != '[' {
		return elem, false, nil
	}

	blocks, err := parseSignatureArray(rawContent)
	if err != nil {
		return elem, false, err
	}
	kept := make([]jsontext.Value, 0, len(blocks))
	changed := false
	for _, blk := range blocks {
		if len(blk) == 0 || blk.Kind() != '{' {
			kept = append(kept, blk)
			continue
		}
		bMembers, bErr := parseOrderedSignatureObject(blk)
		if bErr != nil {
			return elem, false, bErr
		}
		bType, _ := readSignatureMemberString(bMembers, "type")
		switch bType {
		case "thinking":
			if signature, _ := readSignatureMemberString(bMembers, "signature"); signature != "" {
				losses.Tokens++
			}
			thinkText, _ := readSignatureMemberString(bMembers, "thinking")
			if passback {
				if thinkText == "" {
					// Empty passback thinking blocks carry no reasoning context and fail
					// non-empty content rules once signature fields are stripped.
					changed = true
					continue
				}
				if _, hasSig := findSignatureMember(bMembers, "signature"); hasSig {
					bMembers = deleteSignatureMember(bMembers, "signature")
					rebuiltBlk, encErr := encodeOrderedSignatureObject(bMembers)
					if encErr != nil {
						return elem, false, encErr
					}
					kept = append(kept, rebuiltBlk)
					changed = true
					continue
				}
				kept = append(kept, blk)
				continue
			}
			changed = true

			if thinkText != "" {
				losses.Reasoning++
			}
		case "redacted_thinking":
			if data, _ := readSignatureMemberString(bMembers, "data"); data != "" {
				losses.Tokens++
			}
			changed = true

		default:
			kept = append(kept, blk)
		}
	}

	if !changed {
		return elem, false, nil
	}
	if len(kept) == 0 {
		// Supply a non-empty text placeholder when stripping reasoning empties an
		// assistant turn so Anthropic does not reject content:[] with HTTP 400.
		kept = []jsontext.Value{jsontext.Value(signatureRetryAssistantPlaceholder)}
	}
	encodedContent, err := encodeSignatureArray(kept)
	if err != nil {
		return elem, false, err
	}
	mMembers = setSignatureMember(mMembers, "content", encodedContent)
	rebuiltMsg, err := encodeOrderedSignatureObject(mMembers)
	if err != nil {
		return elem, false, err
	}
	return rebuiltMsg, true, nil
}

// isSignatureTopThinkingEnabled reports whether rootMembers configures top-level
// thinking.type as "enabled" or "adaptive".
func isSignatureTopThinkingEnabled(rootMembers []rawSignatureMember) bool {
	rawTh, ok := findSignatureMember(rootMembers, "thinking")
	if !ok || len(rawTh) == 0 || rawTh.Kind() != '{' {
		return false
	}
	thMembers, err := parseOrderedSignatureObject(rawTh)
	if err != nil {
		return false
	}
	thType, _ := readSignatureMemberString(thMembers, "type")
	return thType == "enabled" || thType == "adaptive"
}

// signatureToolLoopLacksLeadingThinking reports whether the trailing pair of
// messages forms an active [assistant(tool_use), user(tool_result)] loop whose
// assistant turn does not start with a thinking or redacted_thinking block.
// Shorter histories or non-tool turns return false with a nil error.
func signatureToolLoopLacksLeadingThinking(msgs []jsontext.Value) (bool, error) {
	if len(msgs) < 2 {
		return false, nil
	}
	lastUser := msgs[len(msgs)-1]
	lastAsst := msgs[len(msgs)-2]
	if len(lastUser) == 0 || lastUser.Kind() != '{' || len(lastAsst) == 0 || lastAsst.Kind() != '{' {
		return false, nil
	}
	uMembers, err := parseOrderedSignatureObject(lastUser)
	if err != nil {
		return false, err
	}
	if role, _ := readSignatureMemberString(uMembers, "role"); role != "user" {
		return false, nil
	}
	hasToolResult, _, _, err := inspectSignatureMessageBlocks(uMembers)
	if err != nil || !hasToolResult {
		return false, err
	}

	aMembers, err := parseOrderedSignatureObject(lastAsst)
	if err != nil {
		return false, err
	}
	if role, _ := readSignatureMemberString(aMembers, "role"); role != "assistant" {
		return false, nil
	}
	_, hasToolUse, startsWithThinking, err := inspectSignatureMessageBlocks(aMembers)
	if err != nil {
		return false, err
	}
	return hasToolUse && !startsWithThinking, nil
}

// inspectSignatureMessageBlocks scans content[] in msgMembers and reports
// whether it contains tool_result blocks, tool_use blocks, and a leading
// thinking or redacted_thinking block at index zero. String or absent content
// fields return all-false flags with a nil error.
func inspectSignatureMessageBlocks(msgMembers []rawSignatureMember) (bool, bool, bool, error) {
	rawContent, ok := findSignatureMember(msgMembers, "content")
	if !ok || len(rawContent) == 0 || rawContent.Kind() != '[' {
		return false, false, false, nil
	}
	blocks, err := parseSignatureArray(rawContent)
	if err != nil {
		return false, false, false, err
	}
	hasToolResult := false
	hasToolUse := false
	startsWithThinking := false
	for idx, blk := range blocks {
		if len(blk) == 0 || blk.Kind() != '{' {
			continue
		}
		bMembers, bErr := parseOrderedSignatureObject(blk)
		if bErr != nil {
			return false, false, false, bErr
		}
		bType, _ := readSignatureMemberString(bMembers, "type")
		if idx == 0 && (bType == "thinking" || bType == "redacted_thinking") {
			startsWithThinking = true
		}
		switch bType {
		case "tool_result":
			hasToolResult = true
		case "tool_use":
			hasToolUse = true
		}
	}
	return hasToolResult, hasToolUse, startsWithThinking, nil
}

// stripClearThinkingContextStrategyMembers removes clear_thinking_20251015 edit
// entries from context_management.edits when top-level thinking is deleted on
// retry. When no edits or sibling keys remain, context_management is removed
// completely so Anthropic does not reject an empty edits array.
func stripClearThinkingContextStrategyMembers(rootMembers []rawSignatureMember) []rawSignatureMember {
	rawCM, ok := findSignatureMember(rootMembers, "context_management")
	if !ok || len(rawCM) == 0 || rawCM.Kind() != '{' {
		return rootMembers
	}
	cmMembers, err := parseOrderedSignatureObject(rawCM)
	if err != nil {
		return rootMembers
	}
	rawEdits, hasEdits := findSignatureMember(cmMembers, "edits")
	if !hasEdits || len(rawEdits) == 0 || rawEdits.Kind() != '[' {
		return rootMembers
	}
	edits, err := parseSignatureArray(rawEdits)
	if err != nil {
		return rootMembers
	}
	kept := make([]jsontext.Value, 0, len(edits))
	removed := false
	for _, edit := range edits {
		if len(edit) == 0 || edit.Kind() != '{' {
			kept = append(kept, edit)
			continue
		}
		eMembers, eErr := parseOrderedSignatureObject(edit)
		if eErr != nil {
			return rootMembers
		}
		if eType, _ := readSignatureMemberString(eMembers, "type"); eType == clearThinkingStrategyType {
			// Anthropic rejects clear_thinking_20251015 edits when top-level thinking
			// is absent, so drop only that dependent strategy while keeping others.
			removed = true
			continue
		}
		kept = append(kept, edit)
	}
	if !removed {
		return rootMembers
	}
	if len(kept) == 0 && len(cmMembers) == 1 {
		return deleteSignatureMember(rootMembers, "context_management")
	}
	encEdits, err := encodeSignatureArray(kept)
	if err != nil {
		return rootMembers
	}
	cmMembers = setSignatureMember(cmMembers, "edits", encEdits)
	rebuiltCM, err := encodeOrderedSignatureObject(cmMembers)
	if err != nil {
		return rootMembers
	}
	return setSignatureMember(rootMembers, "context_management", rebuiltCM)
}

// parseOrderedSignatureObject decodes one top-level JSON object into ordered raw
// members with invalid UTF-8 permitted. Non-object roots, truncated members, or
// trailing tokens after the closing brace return an error.
func parseOrderedSignatureObject(raw []byte) ([]rawSignatureMember, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '{' {
		return nil, io.ErrUnexpectedEOF
	}
	var members []rawSignatureMember
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
		members = append(members, rawSignatureMember{
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

// encodeOrderedSignatureObject serializes ordered raw JSON object members into a
// single compact JSON object with invalid UTF-8 bytes preserved.
func encodeOrderedSignatureObject(members []rawSignatureMember) (jsontext.Value, error) {
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

// parseSignatureArray decodes a JSON array slice into cloned element values with
// invalid UTF-8 permitted. Non-array roots or malformed elements return an
// error before modifying caller slices.
func parseSignatureArray(raw jsontext.Value) ([]jsontext.Value, error) {
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

// encodeSignatureArray serializes raw JSON values into a compact JSON array slice.
func encodeSignatureArray(elems []jsontext.Value) (jsontext.Value, error) {
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

// findSignatureMember scans members from back to front and returns the last raw
// JSON value matching name. Missing keys return nil and false without copying bytes.
func findSignatureMember(members []rawSignatureMember, name string) (jsontext.Value, bool) {
	for _, m := range slices.Backward(members) {
		if m.name == name {
			return m.value, true
		}
	}
	return nil, false
}

// readSignatureMemberString decodes the last string member named name from
// members with invalid UTF-8 tolerated. Absent keys or non-string values return
// an empty string and false.
func readSignatureMemberString(members []rawSignatureMember, name string) (string, bool) {
	raw, ok := findSignatureMember(members, name)
	if !ok || len(raw) == 0 || raw.Kind() != '"' {
		return "", false
	}
	var s string
	if err := jsonv2.Unmarshal(raw, &s, jsontext.AllowInvalidUTF8(true)); err != nil {
		return "", false
	}
	return s, true
}

// setSignatureMember replaces the first member named name in place while
// dropping later duplicates, or appends it at the end of members when absent.
// Unchanged sibling values keep their existing raw slices.
func setSignatureMember(members []rawSignatureMember, name string, val jsontext.Value) []rawSignatureMember {
	out := make([]rawSignatureMember, 0, len(members)+1)
	replaced := false
	for _, m := range members {
		if m.name == name {
			if !replaced {
				out = append(out, rawSignatureMember{name: name, value: val})
				replaced = true
			}
			continue
		}
		out = append(out, m)
	}
	if !replaced {
		out = append(out, rawSignatureMember{name: name, value: val})
	}
	return out
}

// deleteSignatureMember removes all members matching name while preserving the
// relative order of remaining members.
func deleteSignatureMember(members []rawSignatureMember, name string) []rawSignatureMember {
	out := make([]rawSignatureMember, 0, len(members))
	for _, m := range members {
		if m.name != name {
			out = append(out, m)
		}
	}
	return out
}
