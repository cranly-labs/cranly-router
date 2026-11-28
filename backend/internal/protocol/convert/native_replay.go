package convert

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// native_replay.go unpacks matching gateway trc1 continuity envelopes and
// removes foreign or mismatched-account envelopes inside native Anthropic
// Messages, OpenAI Responses HTTP, and Responses WebSocket request payloads.
// Clients that ran turn N through cross-protocol conversion carry trc1
// envelopes in their conversation history; if turn N+1 routes to a native
// passthrough provider, forwarding raw trc1 strings upstream would fail
// cryptographic signature or encrypted_content validation.
//
// PrepareNativeReplay short-circuits via bytes.Contains and a token-path-only
// jsontext.Decoder scan so any payload without an envelope on protocol token
// paths returns its exact input byte slice untouched, even when user text,
// tool results, or WS session.update frames contain literal "trc1:" substrings
// or invalid UTF-8 bytes. When a token-path envelope is present, surgical
// ordered-member rewriting preserves untouched JSON members and large integers
// verbatim while enforcing Messages empty-turn placeholders, passback thinking
// retention, active tool-loop thinking downgrades, and Responses summary-only
// retention.

const (
	// removedAssistantPlaceholderText fills an assistant content array emptied
	// after foreign thinking and redacted_thinking blocks are removed on Messages.
	removedAssistantPlaceholderText = "(assistant content removed)" // non-empty fallback text preventing 400 errors
)

// rawMember stores one JSON object member in wire encounter order without float64 decoding.
type rawMember struct {
	name  string         // unescaped JSON object key
	value jsontext.Value // verbatim raw JSON value bytes
}

// PrepareNativeReplay opens matching trc1 continuity envelopes or removes
// incompatible envelopes inside a native Messages or Responses HTTP/WebSocket
// request body. Payloads without a valid trc1 envelope at protocol token paths
// return body unchanged with nil losses and nil error.
//
// When token-path envelopes are modified, untouched JSON members, key order,
// and arbitrary-precision numbers remain verbatim while emptied Messages
// assistant turns receive fallback text placeholders and active tool loops
// lacking a leading thinking block downgrade remaining thinking blocks cleanly.
func PrepareNativeReplay(route Route, body []byte) ([]byte, []ir.Loss, error) {
	rewritten, losses, _, err := PrepareNativeReplayWithStatus(route, body)
	return rewritten, losses, err
}

// PrepareNativeReplayWithStatus unpacks matching trc1 continuity envelopes or
// strips foreign envelopes identically to PrepareNativeReplay and reports via
// applied whether at least one valid trc1 envelope was found and rewritten at a
// protocol token path. Matching envelopes unpacked without loss still report
// applied == true, whereas literal "trc1:" substrings in user text or non-create
// WebSocket frames return applied == false with body untouched.
func PrepareNativeReplayWithStatus(route Route, body []byte) ([]byte, []ir.Loss, bool, error) {
	target := route.Upstream
	if target == "" {
		target = route.Client
	}
	switch target {
	case protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolResponsesWebSocket:
	default:
		return body, nil, false, nil
	}

	if !bytes.Contains(body, []byte(ir.CarrierPrefix)) {
		return body, nil, false, nil
	}

	hasCarrier, err := scanNativeTokenPaths(target, body)
	if err != nil {
		return nil, nil, false, err
	}
	if !hasCarrier {
		return body, nil, false, nil
	}

	if target == protocol.ProtocolAnthropicMessages {
		rewritten, losses, rewriteErr := rewriteNativeMessagesBody(body, route.Dialect)
		if rewriteErr != nil {
			return nil, nil, false, rewriteErr
		}
		return rewritten, losses, true, nil
	}
	rewritten, losses, rewriteErr := rewriteNativeResponsesBody(body, route.Dialect)
	if rewriteErr != nil {
		return nil, nil, false, rewriteErr
	}
	return rewritten, losses, true, nil
}

// scanNativeTokenPaths inspects only protocol signature and encrypted_content
// JSON paths using jsontext.Decoder with invalid UTF-8 tolerated, reporting
// whether at least one valid trc1 carrier envelope exists on those paths.
func scanNativeTokenPaths(target protocol.ProtocolID, body []byte) (bool, error) {
	members, err := parseOrderedObject(body)
	if err != nil {
		return false, fmt.Errorf("prepare native replay: %w", err)
	}

	if target == protocol.ProtocolAnthropicMessages {
		rawMsgs, ok := findRawMember(members, "messages")
		if !ok {
			return false, nil
		}
		return scanMessagesArrayForCarrier(rawMsgs)
	}

	if rawType, hasType := findRawMember(members, "type"); hasType {
		var frameType string
		if err := unmarshalPermissive(rawType, &frameType); err == nil {
			trimmed := strings.TrimSpace(frameType)
			if trimmed != "" && trimmed != "response.create" {
				return false, nil
			}
		}
	}

	if rawInput, ok := findRawMember(members, "input"); ok {
		found, err := scanResponsesInputForCarrier(rawInput)
		if err != nil || found {
			return found, err
		}
	}
	if rawResp, ok := findRawMember(members, "response"); ok && rawResp.Kind() == '{' {
		respMembers, err := parseOrderedObject(rawResp)
		if err != nil {
			return false, fmt.Errorf("prepare native replay response envelope: %w", err)
		}
		if nestedInput, ok := findRawMember(respMembers, "input"); ok {
			return scanResponsesInputForCarrier(nestedInput)
		}
	}
	return false, nil
}

// scanMessagesArrayForCarrier checks assistant content[*].thinking.signature and
// content[*].redacted_thinking.data inside rawMsgs for a valid trc1 carrier.
func scanMessagesArrayForCarrier(rawMsgs jsontext.Value) (bool, error) {
	if len(rawMsgs) == 0 || rawMsgs.Kind() != '[' {
		return false, nil
	}
	elems, err := parseRawArray(rawMsgs)
	if err != nil {
		return false, err
	}
	for _, elem := range elems {
		if elem.Kind() != '{' {
			continue
		}
		mMembers, err := parseOrderedObject(elem)
		if err != nil {
			return false, err
		}
		role, _ := readMemberString(mMembers, "role")
		if role != "assistant" {
			continue
		}
		rawContent, ok := findRawMember(mMembers, "content")
		if !ok || rawContent.Kind() != '[' {
			continue
		}
		blocks, err := parseRawArray(rawContent)
		if err != nil {
			return false, err
		}
		for _, blk := range blocks {
			if blk.Kind() != '{' {
				continue
			}
			bMembers, err := parseOrderedObject(blk)
			if err != nil {
				return false, err
			}
			bType, _ := readMemberString(bMembers, "type")
			switch bType {
			case "thinking":
				if sig, ok := readMemberString(bMembers, "signature"); ok {
					if _, valid := ir.ParseCarrier(sig); valid {
						return true, nil
					}
				}
			case "redacted_thinking":
				if data, ok := readMemberString(bMembers, "data"); ok {
					if _, valid := ir.ParseCarrier(data); valid {
						return true, nil
					}
				}
			}
		}
	}
	return false, nil
}

// scanResponsesInputForCarrier checks input[*] items with type=="reasoning" for
// an encrypted_content string that parses via ir.ParseCarrier.
func scanResponsesInputForCarrier(rawInput jsontext.Value) (bool, error) {
	if len(rawInput) == 0 || rawInput.Kind() != '[' {
		return false, nil
	}
	items, err := parseRawArray(rawInput)
	if err != nil {
		return false, err
	}
	for _, item := range items {
		if item.Kind() != '{' {
			continue
		}
		iMembers, err := parseOrderedObject(item)
		if err != nil {
			return false, err
		}
		iType, _ := readMemberString(iMembers, "type")
		if iType != "reasoning" {
			continue
		}
		if enc, ok := readMemberString(iMembers, "encrypted_content"); ok {
			if _, valid := ir.ParseCarrier(enc); valid {
				return true, nil
			}
		}
	}
	return false, nil
}

// rewriteNativeMessagesBody unpacks or removes trc1 carriers in a native
// Anthropic Messages request body while preserving member order and raw numbers.
func rewriteNativeMessagesBody(body []byte, dialect Dialect) ([]byte, []ir.Loss, error) {
	rootMembers, err := parseOrderedObject(body)
	if err != nil {
		return nil, nil, err
	}
	rawMsgs, ok := findRawMember(rootMembers, "messages")
	if !ok || rawMsgs.Kind() != '[' {
		return body, nil, nil
	}
	msgElems, err := parseRawArray(rawMsgs)
	if err != nil {
		return nil, nil, err
	}

	passback := dialect.ReasoningHistory == HistoryToolTurns
	var losses []ir.Loss
	rebuiltMsgs := make([]jsontext.Value, len(msgElems))

	for mIdx, elem := range msgElems {
		if elem.Kind() != '{' {
			rebuiltMsgs[mIdx] = elem
			continue
		}
		mMembers, err := parseOrderedObject(elem)
		if err != nil {
			return nil, nil, err
		}
		role, _ := readMemberString(mMembers, "role")
		rawContent, hasContent := findRawMember(mMembers, "content")
		if role != "assistant" || !hasContent || rawContent.Kind() != '[' {
			rebuiltMsgs[mIdx] = elem
			continue
		}

		blocks, err := parseRawArray(rawContent)
		if err != nil {
			return nil, nil, err
		}
		msgChanged := false
		keptBlocks := make([]jsontext.Value, 0, len(blocks))

		for bIdx, blk := range blocks {
			if blk.Kind() != '{' {
				keptBlocks = append(keptBlocks, blk)
				continue
			}
			bMembers, err := parseOrderedObject(blk)
			if err != nil {
				return nil, nil, err
			}
			bType, _ := readMemberString(bMembers, "type")
			pPath := fmt.Sprintf("messages[%d].parts[%d]", mIdx, bIdx)

			switch bType {
			case "thinking":
				sig, hasSig := readMemberString(bMembers, "signature")
				carrier, valid := ir.ParseCarrier(sig)
				if !hasSig || !valid {
					keptBlocks = append(keptBlocks, blk)
					continue
				}
				msgChanged = true
				if schemeAndProviderMatch(carrier.Scheme, carrier.ProviderID, dialect) {
					if carrier.Redacted {
						rebuiltBlk, err := buildRedactedBlockFromThinking(bMembers, carrier.Data)
						if err != nil {
							return nil, nil, err
						}
						keptBlocks = append(keptBlocks, rebuiltBlk)
					} else {
						bMembers = setStringMember(bMembers, "signature", carrier.Data)
						if carrier.Text != "" {
							bMembers = setStringMember(bMembers, "thinking", carrier.Text)
						}
						rebuiltBlk, err := encodeOrderedObject(bMembers)
						if err != nil {
							return nil, nil, err
						}
						keptBlocks = append(keptBlocks, rebuiltBlk)
					}
					continue
				}

				if carrier.Scheme != ir.SchemePlain {
					losses = append(losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
				}
				if passback && carrier.Text != "" {
					bMembers = setStringMember(bMembers, "thinking", carrier.Text)
					bMembers = deleteRawMember(bMembers, "signature")
					rebuiltBlk, err := encodeOrderedObject(bMembers)
					if err != nil {
						return nil, nil, err
					}
					keptBlocks = append(keptBlocks, rebuiltBlk)
					continue
				}
				origThinking, _ := readMemberString(bMembers, "thinking")
				if origThinking != "" || carrier.Text != "" {
					losses = append(losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
				}

			case "redacted_thinking":
				dataStr, hasData := readMemberString(bMembers, "data")
				carrier, valid := ir.ParseCarrier(dataStr)
				if !hasData || !valid {
					keptBlocks = append(keptBlocks, blk)
					continue
				}
				msgChanged = true
				if schemeAndProviderMatch(carrier.Scheme, carrier.ProviderID, dialect) {
					if !carrier.Redacted && carrier.Text != "" {
						rebuiltBlk, err := buildThinkingBlockFromRedacted(bMembers, carrier.Text, carrier.Data)
						if err != nil {
							return nil, nil, err
						}
						keptBlocks = append(keptBlocks, rebuiltBlk)
					} else {
						bMembers = setStringMember(bMembers, "data", carrier.Data)
						rebuiltBlk, err := encodeOrderedObject(bMembers)
						if err != nil {
							return nil, nil, err
						}
						keptBlocks = append(keptBlocks, rebuiltBlk)
					}
					continue
				}

				if carrier.Scheme != ir.SchemePlain {
					losses = append(losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
				}
				if passback && carrier.Text != "" {
					rebuiltBlk, err := buildThinkingBlockFromRedacted(bMembers, carrier.Text, "")
					if err != nil {
						return nil, nil, err
					}
					keptBlocks = append(keptBlocks, rebuiltBlk)
					continue
				}

			default:
				keptBlocks = append(keptBlocks, blk)
			}
		}

		if !msgChanged {
			rebuiltMsgs[mIdx] = elem
			continue
		}
		if len(keptBlocks) == 0 {
			// Supply a non-empty placeholder text block when carrier deletion empties
			// an assistant turn so Anthropic endpoints do not reject content:[].
			placeholder, err := marshalTextPlaceholderBlock(removedAssistantPlaceholderText)
			if err != nil {
				return nil, nil, err
			}
			keptBlocks = []jsontext.Value{placeholder}
		}
		encodedContent, err := encodeRawArray(keptBlocks)
		if err != nil {
			return nil, nil, err
		}
		mMembers = setRawMember(mMembers, "content", encodedContent)
		rebuiltMsg, err := encodeOrderedObject(mMembers)
		if err != nil {
			return nil, nil, err
		}
		rebuiltMsgs[mIdx] = rebuiltMsg
	}

	if !passback && isMessagesThinkingEnabled(rootMembers) {
		needsDowngrade, err := nativeToolLoopLacksLeadingThinking(rebuiltMsgs)
		if err != nil {
			return nil, nil, err
		}
		if needsDowngrade {
			// Downgrade remaining thinking blocks to plain text and strip top-level
			// thinking when carrier removal left the active tool loop without leading thinking.
			rootMembers = deleteRawMember(rootMembers, "thinking")
			rebuiltMsgs, err = downgradeRemainingMessagesThinking(rebuiltMsgs, &losses)
			if err != nil {
				return nil, nil, err
			}
		}
	}

	encodedMsgs, err := encodeRawArray(rebuiltMsgs)
	if err != nil {
		return nil, nil, err
	}
	rootMembers = setRawMember(rootMembers, "messages", encodedMsgs)
	outBytes, err := encodeOrderedObject(rootMembers)
	if err != nil {
		return nil, nil, err
	}
	return outBytes, losses, nil
}

// isMessagesThinkingEnabled reports whether rootMembers sets thinking.type to "enabled" or "adaptive".
func isMessagesThinkingEnabled(rootMembers []rawMember) bool {
	rawTh, ok := findRawMember(rootMembers, "thinking")
	if !ok || rawTh.Kind() != '{' {
		return false
	}
	thMembers, err := parseOrderedObject(rawTh)
	if err != nil {
		return false
	}
	thType, _ := readMemberString(thMembers, "type")
	return thType == "enabled" || thType == "adaptive"
}

// nativeToolLoopLacksLeadingThinking reports whether the final two messages in
// msgs form an active [assistant(tool_use), user(tool_result)] loop whose
// assistant turn no longer begins with a thinking or redacted_thinking block.
func nativeToolLoopLacksLeadingThinking(msgs []jsontext.Value) (bool, error) {
	if len(msgs) < 2 {
		return false, nil
	}
	lastUser := msgs[len(msgs)-1]
	lastAsst := msgs[len(msgs)-2]
	if lastUser.Kind() != '{' || lastAsst.Kind() != '{' {
		return false, nil
	}

	uMembers, err := parseOrderedObject(lastUser)
	if err != nil {
		return false, err
	}
	if role, _ := readMemberString(uMembers, "role"); role != "user" {
		return false, nil
	}
	hasToolResult, _, _, err := inspectNativeMessageBlocks(uMembers)
	if err != nil || !hasToolResult {
		return false, err
	}

	aMembers, err := parseOrderedObject(lastAsst)
	if err != nil {
		return false, err
	}
	if role, _ := readMemberString(aMembers, "role"); role != "assistant" {
		return false, nil
	}
	_, hasToolUse, startsWithThinking, err := inspectNativeMessageBlocks(aMembers)
	if err != nil {
		return false, err
	}
	return hasToolUse && !startsWithThinking, nil
}

// inspectNativeMessageBlocks inspects content[] inside msgMembers for tool_result,
// tool_use, and leading thinking/redacted_thinking blocks.
func inspectNativeMessageBlocks(msgMembers []rawMember) (bool, bool, bool, error) {
	rawContent, ok := findRawMember(msgMembers, "content")
	if !ok || rawContent.Kind() != '[' {
		return false, false, false, nil
	}
	blocks, err := parseRawArray(rawContent)
	if err != nil {
		return false, false, false, err
	}
	hasToolResult := false
	hasToolUse := false
	startsWithThinking := false
	for idx, blk := range blocks {
		if blk.Kind() != '{' {
			continue
		}
		bMembers, err := parseOrderedObject(blk)
		if err != nil {
			return false, false, false, err
		}
		bType, _ := readMemberString(bMembers, "type")
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

// downgradeRemainingMessagesThinking converts any surviving assistant thinking
// blocks in msgs into ordinary text blocks and removes surviving redacted_thinking
// blocks via jsontext without float64 re-parsing.
func downgradeRemainingMessagesThinking(msgs []jsontext.Value, losses *[]ir.Loss) ([]jsontext.Value, error) {
	out := make([]jsontext.Value, len(msgs))
	for mIdx, elem := range msgs {
		if elem.Kind() != '{' {
			out[mIdx] = elem
			continue
		}
		mMembers, err := parseOrderedObject(elem)
		if err != nil {
			return nil, err
		}
		role, _ := readMemberString(mMembers, "role")
		rawContent, hasContent := findRawMember(mMembers, "content")
		if role != "assistant" || !hasContent || rawContent.Kind() != '[' {
			out[mIdx] = elem
			continue
		}

		blocks, err := parseRawArray(rawContent)
		if err != nil {
			return nil, err
		}
		changed := false
		var newBlocks []jsontext.Value
		for bIdx, blk := range blocks {
			if blk.Kind() != '{' {
				newBlocks = append(newBlocks, blk)
				continue
			}
			bMembers, err := parseOrderedObject(blk)
			if err != nil {
				return nil, err
			}
			bType, _ := readMemberString(bMembers, "type")
			pPath := fmt.Sprintf("messages[%d].parts[%d]", mIdx, bIdx)
			switch bType {
			case "thinking":
				changed = true
				thinkText, _ := readMemberString(bMembers, "thinking")
				if thinkText != "" {
					txtBlk, err := buildTextBlockFromThinking(bMembers, thinkText)
					if err != nil {
						return nil, err
					}
					newBlocks = append(newBlocks, txtBlk)
					*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextInlined, Path: pPath})
				} else {
					*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: pPath})
				}
			case "redacted_thinking":
				changed = true
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
			default:
				newBlocks = append(newBlocks, blk)
			}
		}

		if !changed {
			out[mIdx] = elem
			continue
		}
		if len(newBlocks) == 0 {
			placeholder, err := marshalTextPlaceholderBlock(removedAssistantPlaceholderText)
			if err != nil {
				return nil, err
			}
			newBlocks = []jsontext.Value{placeholder}
		}
		encContent, err := encodeRawArray(newBlocks)
		if err != nil {
			return nil, err
		}
		mMembers = setRawMember(mMembers, "content", encContent)
		rebuilt, err := encodeOrderedObject(mMembers)
		if err != nil {
			return nil, err
		}
		out[mIdx] = rebuilt
	}
	return out, nil
}

// rewriteNativeResponsesBody unpacks matching trc1 carriers and strips
// mismatched carriers inside HTTP Responses or WS response.create payloads.
func rewriteNativeResponsesBody(body []byte, dialect Dialect) ([]byte, []ir.Loss, error) {
	rootMembers, err := parseOrderedObject(body)
	if err != nil {
		return nil, nil, err
	}

	var losses []ir.Loss
	changed := false
	topStoreFalse := isStoreExplicitlyFalse(rootMembers)

	if rawInput, ok := findRawMember(rootMembers, "input"); ok && rawInput.Kind() == '[' {
		rebuiltInput, inChanged, inLosses, err := rewriteNativeResponsesInputArray(rawInput, dialect, topStoreFalse)
		if err != nil {
			return nil, nil, err
		}
		if inChanged {
			rootMembers = setRawMember(rootMembers, "input", rebuiltInput)
			changed = true
			losses = append(losses, inLosses...)
		}
	}

	if rawResp, ok := findRawMember(rootMembers, "response"); ok && rawResp.Kind() == '{' {
		respMembers, err := parseOrderedObject(rawResp)
		if err != nil {
			return nil, nil, err
		}
		nestedStoreFalse := topStoreFalse || isStoreExplicitlyFalse(respMembers)
		if nestedInput, hasIn := findRawMember(respMembers, "input"); hasIn && nestedInput.Kind() == '[' {
			rebuiltNested, nChanged, nLosses, err := rewriteNativeResponsesInputArray(nestedInput, dialect, nestedStoreFalse)
			if err != nil {
				return nil, nil, err
			}
			if nChanged {
				respMembers = setRawMember(respMembers, "input", rebuiltNested)
				encodedResp, err := encodeOrderedObject(respMembers)
				if err != nil {
					return nil, nil, err
				}
				rootMembers = setRawMember(rootMembers, "response", encodedResp)
				changed = true
				losses = append(losses, nLosses...)
			}
		}
	}

	if !changed {
		return body, nil, nil
	}
	outBytes, err := encodeOrderedObject(rootMembers)
	if err != nil {
		return nil, nil, err
	}
	return outBytes, losses, nil
}

// rewriteNativeResponsesInputArray rewrites reasoning items carrying trc1
// envelopes inside rawInput, preserving non-empty summary/content items (and
// removing synthetic IDs when storeFalse is true) while omitting empty items.
func rewriteNativeResponsesInputArray(
	rawInput jsontext.Value,
	dialect Dialect,
	storeFalse bool,
) (jsontext.Value, bool, []ir.Loss, error) {
	items, err := parseRawArray(rawInput)
	if err != nil {
		return nil, false, nil, err
	}

	var losses []ir.Loss
	changed := false
	kept := make([]jsontext.Value, 0, len(items))

	for idx, item := range items {
		if item.Kind() != '{' {
			kept = append(kept, item)
			continue
		}
		iMembers, err := parseOrderedObject(item)
		if err != nil {
			return nil, false, nil, err
		}
		iType, _ := readMemberString(iMembers, "type")
		if iType != "reasoning" {
			kept = append(kept, item)
			continue
		}
		encStr, hasEnc := readMemberString(iMembers, "encrypted_content")
		carrier, valid := ir.ParseCarrier(encStr)
		if !hasEnc || !valid {
			kept = append(kept, item)
			continue
		}

		changed = true
		iPath := fmt.Sprintf("input[%d]", idx)
		if schemeAndProviderMatch(carrier.Scheme, carrier.ProviderID, dialect) {
			iMembers = setStringMember(iMembers, "encrypted_content", carrier.Data)
			rebuilt, err := encodeOrderedObject(iMembers)
			if err != nil {
				return nil, false, nil, err
			}
			kept = append(kept, rebuilt)
			continue
		}

		if carrier.Scheme != ir.SchemePlain {
			losses = append(losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: iPath})
		}
		if reasoningItemHasVisibleBody(iMembers) && nativeResponsesHasFollower(items, idx) {
			iMembers = deleteRawMember(iMembers, "encrypted_content")
			if storeFalse {
				iMembers = deleteRawMember(iMembers, "id")
			}
			rebuilt, err := encodeOrderedObject(iMembers)
			if err != nil {
				return nil, false, nil, err
			}
			kept = append(kept, rebuilt)
			continue
		}
		if carrier.Scheme == ir.SchemePlain && carrier.Text != "" {
			losses = append(losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: iPath})
		}
	}

	if !changed {
		return rawInput, false, nil, nil
	}
	encoded, err := encodeRawArray(kept)
	if err != nil {
		return nil, false, nil, err
	}
	return encoded, true, losses, nil
}

// isStoreExplicitlyFalse reports whether members contains a boolean false value under "store".
func isStoreExplicitlyFalse(members []rawMember) bool {
	rawStore, ok := findRawMember(members, "store")
	return ok && rawStore.Kind() == 'f'
}

// reasoningItemHasVisibleBody reports whether a Responses reasoning item carries
// at least one non-empty summary_text or reasoning_text entry.
func reasoningItemHasVisibleBody(members []rawMember) bool {
	for _, field := range []string{"summary", "content"} {
		rawArr, ok := findRawMember(members, field)
		if !ok || rawArr.Kind() != '[' {
			continue
		}
		elems, err := parseRawArray(rawArr)
		if err != nil || len(elems) == 0 {
			continue
		}
		for _, el := range elems {
			if el.Kind() != '{' {
				continue
			}
			elMembers, err := parseOrderedObject(el)
			if err != nil {
				continue
			}
			if txt, _ := readMemberString(elMembers, "text"); txt != "" {
				return true
			}
		}
	}
	return false
}

// nativeResponsesHasFollower reports whether items[start+1:] contains an
// associated assistant message or tool call before hitting user/output items.
func nativeResponsesHasFollower(items []jsontext.Value, start int) bool {
	for j := start + 1; j < len(items); j++ {
		if items[j].Kind() != '{' {
			return false
		}
		m, err := parseOrderedObject(items[j])
		if err != nil {
			return false
		}
		typ, _ := readMemberString(m, "type")
		role, _ := readMemberString(m, "role")
		if typ == "" && role != "" {
			typ = "message"
		}
		if typ == "reasoning" {
			continue
		}
		switch typ {
		case "message":
			return role == "assistant"
		case "function_call", "custom_tool_call", "tool_search_call",
			"web_search_call", "mcp_call", "image_generation_call", "local_shell_call":
			return true
		default:
			return false
		}
	}
	return false
}

// buildRedactedBlockFromThinking constructs a redacted_thinking block preserving optional cache_control.
func buildRedactedBlockFromThinking(src []rawMember, data string) (jsontext.Value, error) {
	out := []rawMember{
		{name: "type", value: mustMarshalString("redacted_thinking")},
		{name: "data", value: mustMarshalString(data)},
	}
	if cc, ok := findRawMember(src, "cache_control"); ok {
		out = append(out, rawMember{name: "cache_control", value: cc})
	}
	return encodeOrderedObject(out)
}

// buildThinkingBlockFromRedacted constructs a thinking block from a redacted_thinking carrier.
func buildThinkingBlockFromRedacted(src []rawMember, thinkingText, signature string) (jsontext.Value, error) {
	out := []rawMember{
		{name: "type", value: mustMarshalString("thinking")},
		{name: "thinking", value: mustMarshalString(thinkingText)},
	}
	if signature != "" {
		out = append(out, rawMember{name: "signature", value: mustMarshalString(signature)})
	}
	if cc, ok := findRawMember(src, "cache_control"); ok {
		out = append(out, rawMember{name: "cache_control", value: cc})
	}
	return encodeOrderedObject(out)
}

// buildTextBlockFromThinking downgrades a thinking block into a plain text block preserving cache_control.
func buildTextBlockFromThinking(src []rawMember, text string) (jsontext.Value, error) {
	out := []rawMember{
		{name: "type", value: mustMarshalString("text")},
		{name: "text", value: mustMarshalString(text)},
	}
	if cc, ok := findRawMember(src, "cache_control"); ok {
		out = append(out, rawMember{name: "cache_control", value: cc})
	}
	return encodeOrderedObject(out)
}

// marshalTextPlaceholderBlock serializes a Messages text content block containing placeholder.
func marshalTextPlaceholderBlock(placeholder string) (jsontext.Value, error) {
	return encodeOrderedObject([]rawMember{
		{name: "type", value: mustMarshalString("text")},
		{name: "text", value: mustMarshalString(placeholder)},
	})
}

// parseOrderedObject parses raw as a single JSON object with unique keys in encounter order.
func parseOrderedObject(raw []byte) ([]rawMember, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '{' {
		return nil, errors.New("expected JSON object")
	}
	seen := make(map[string]bool)
	var members []rawMember
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		key := kTok.String()
		if seen[key] {
			return nil, fmt.Errorf("duplicate JSON object key %q", key)
		}
		seen[key] = true
		val, err := dec.ReadValue()
		if err != nil {
			return nil, err
		}
		members = append(members, rawMember{
			name:  key,
			value: jsontext.Value(bytes.Clone(val)),
		})
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		if err == nil {
			return nil, errors.New("unexpected trailing JSON data")
		}
		return nil, err
	}
	return members, nil
}

// parseRawArray parses raw as a JSON array of raw jsontext.Value elements.
func parseRawArray(raw []byte) ([]jsontext.Value, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '[' {
		return nil, errors.New("expected JSON array")
	}
	var items []jsontext.Value
	for dec.PeekKind() != ']' {
		val, err := dec.ReadValue()
		if err != nil {
			return nil, err
		}
		items = append(items, jsontext.Value(bytes.Clone(val)))
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		if err == nil {
			return nil, errors.New("unexpected trailing JSON data")
		}
		return nil, err
	}
	return items, nil
}

// encodeOrderedObject serializes members in exact slice order without re-parsing raw values.
func encodeOrderedObject(members []rawMember) (jsontext.Value, error) {
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

// encodeRawArray serializes items into a compact JSON array preserving each element's raw bytes.
func encodeRawArray(items []jsontext.Value) (jsontext.Value, error) {
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
	return jsontext.Value(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

// findRawMember returns the value of key inside members and whether it was present.
func findRawMember(members []rawMember, key string) (jsontext.Value, bool) {
	for _, m := range members {
		if m.name == key {
			return m.value, true
		}
	}
	return nil, false
}

// readMemberString decodes key inside members when present and string-typed.
func readMemberString(members []rawMember, key string) (string, bool) {
	raw, ok := findRawMember(members, key)
	if !ok || len(raw) == 0 || raw.Kind() != '"' {
		return "", false
	}
	var s string
	if err := unmarshalPermissive(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// setRawMember replaces key in place inside members or appends it when absent.
func setRawMember(members []rawMember, key string, val jsontext.Value) []rawMember {
	for i := range members {
		if members[i].name == key {
			members[i].value = val
			return members
		}
	}
	return append(members, rawMember{name: key, value: val})
}

// setStringMember marshals s into a JSON string token and sets key on members.
func setStringMember(members []rawMember, key, s string) []rawMember {
	return setRawMember(members, key, mustMarshalString(s))
}

// deleteRawMember removes key from members while keeping remaining keys in order.
func deleteRawMember(members []rawMember, key string) []rawMember {
	out := make([]rawMember, 0, len(members))
	for _, m := range members {
		if m.name != key {
			out = append(out, m)
		}
	}
	return out
}

// mustMarshalString encodes s into a valid JSON string literal with invalid UTF-8 tolerated.
func mustMarshalString(s string) jsontext.Value {
	raw, _ := json.Marshal(s, jsontext.AllowInvalidUTF8(true))
	return jsontext.Value(raw)
}

// unmarshalPermissive decodes raw into dst with invalid UTF-8 tolerated and duplicate keys rejected.
func unmarshalPermissive(raw []byte, dst any) error {
	return json.Unmarshal(raw, dst, jsontext.AllowInvalidUTF8(true))
}
