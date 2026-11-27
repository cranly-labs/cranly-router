package provider

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"strings"

	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
	protocolopenai "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// openai_compat_prompt_cache_key.go derives stable compatibility cache keys
// from conversation prefixes and cache anchors.
// Compatibility cache identity follows stable request content rather than the
// entire growing conversation. Chat seeds include system text, the first user
// content, and selected model/tool configuration; ordinary Anthropic seeds use
// the same opening-content approach, while explicit ephemeral anchors select a
// separate identity path.
//
// Anchor identity includes all qualifying system and assistant text blocks
// plus one first-user text anchor, and it excludes model/configuration fields.
// That distinction lets callers recognize bridge keys by prefix but does not
// make every pair of different requests distinct. JSON normalization and
// hashing happen after seed selection; unencodable tool declarations or
// malformed anchor content are omitted so remaining evidence can still produce
// a key.

// compatPromptCacheKeyPrefix identifies hashes derived from compatibility opening
// content and configuration, distinguishing them from explicit Anthropic anchor keys.
const compatPromptCacheKeyPrefix = "compat_cc_"

// ShouldAutoInjectPromptCacheKeyForCompat recognizes automatic-key model
// identities after trimming and case folding. Exact gpt-6-astra, gpt-5
// prefixes, and names containing codex qualify; other GPT-6 spellings and
// vendor-prefixed Astra names do not.
func ShouldAutoInjectPromptCacheKeyForCompat(model string) bool {
	trimmed := strings.TrimSpace(strings.ToLower(model))
	return trimmed == "gpt-6-astra" || strings.HasPrefix(trimmed, "gpt-5") || strings.Contains(trimmed, "codex")
}

// DeriveCompatPromptCacheKey hashes opening Chat content and selected request
// configuration for compatibility caching. Nil requests return empty, and a
// blank mappedModel uses the trimmed request model.
//
// The seed includes effort, tool choice, serializable tool/function
// declarations, every system message, and the first user message. Later user
// and assistant turns stay outside this seed so appended history can reuse a
// stable opening identity. Failed declaration encoding omits that seed
// component, while NormalizeCompatSeedJSON defines how each accepted JSON
// component is normalized before hashing.
func DeriveCompatPromptCacheKey(req *protocolopenai.ChatCompletionsRequest, mappedModel string) string {
	if req == nil {
		return ""
	}

	normalizedModel := strings.TrimSpace(mappedModel)
	if normalizedModel == "" {
		normalizedModel = strings.TrimSpace(req.Model)
	}

	seedParts := []string{"model=" + normalizedModel}
	if req.ReasoningEffort != "" {
		seedParts = append(seedParts, "reasoning_effort="+strings.TrimSpace(req.ReasoningEffort))
	}
	if len(req.ToolChoice) > 0 {
		seedParts = append(seedParts, "tool_choice="+protocolopenai.NormalizeCompatSeedJSON(req.ToolChoice))
	}
	// Tool declarations participate when serializable; a failed JSON encoding leaves
	// the remaining opening-content seed usable, with no declaration component added.
	if len(req.Tools) > 0 {
		if raw, err := json.Marshal(req.Tools); err == nil {
			seedParts = append(seedParts, "tools="+protocolopenai.NormalizeCompatSeedJSON(raw))
		}
	}
	// Legacy function declarations use the same serializable-seed rule as tools;
	// encoding failure retains the other seed parts instead of rejecting cache identity.
	if len(req.Functions) > 0 {
		if raw, err := json.Marshal(req.Functions); err == nil {
			seedParts = append(seedParts, "functions="+protocolopenai.NormalizeCompatSeedJSON(raw))
		}
	}

	firstUserCaptured := false
	for _, msg := range req.Messages {
		switch strings.TrimSpace(msg.Role) {
		case "system":
			seedParts = append(seedParts, "system="+protocolopenai.NormalizeCompatSeedJSON(msg.Content))
		case "user":
			if !firstUserCaptured {
				seedParts = append(seedParts, "first_user="+protocolopenai.NormalizeCompatSeedJSON(msg.Content))
				firstUserCaptured = true
			}
		}
	}

	return compatPromptCacheKeyPrefix + upstream.HashSensitiveValueForLog(strings.Join(seedParts, "|"))
}

// DeriveAnthropicCompatPromptCacheKey prefers an ephemeral-text anchor key
// before deriving a configuration/opening-content key. Nil requests return
// empty, and nonempty anchor output is returned without adding the model or
// other configuration seed fields.
//
// The ordinary seed uses mapped model or request model, explicit effort, tool
// choice, serializable tools, system content, and the first user message.
// Later history is excluded from that path. Anchor selection uses its own
// role/text rules, so callers distinguish the resulting anthropic-cache prefix
// from the compat_cc prefix when identifying bridge requests.
func DeriveAnthropicCompatPromptCacheKey(req *protocolanthropic.AnthropicRequest, mappedModel string) string {
	if req == nil {
		return ""
	}
	// Explicit text anchors select their own content-only namespace before model
	// and configuration seeds are assembled for the ordinary compatibility key.
	if anchorKey := DeriveAnthropicCacheControlPromptCacheKey(req); anchorKey != "" {
		return anchorKey
	}

	normalizedModel := strings.TrimSpace(mappedModel)
	if normalizedModel == "" {
		normalizedModel = strings.TrimSpace(req.Model)
	}

	seedParts := []string{"model=" + normalizedModel}
	if req.OutputConfig != nil && strings.TrimSpace(req.OutputConfig.Effort) != "" {
		seedParts = append(seedParts, "effort="+strings.TrimSpace(req.OutputConfig.Effort))
	}
	if len(req.ToolChoice) > 0 {
		seedParts = append(seedParts, "tool_choice="+protocolopenai.NormalizeCompatSeedJSON(req.ToolChoice))
	}
	// Tool declarations participate when serializable; a failed JSON encoding leaves
	// the remaining opening-content seed usable, with no declaration component added.
	if len(req.Tools) > 0 {
		if raw, err := json.Marshal(req.Tools); err == nil {
			seedParts = append(seedParts, "tools="+protocolopenai.NormalizeCompatSeedJSON(raw))
		}
	}
	if len(req.System) > 0 {
		seedParts = append(seedParts, "system="+protocolopenai.NormalizeCompatSeedJSON(req.System))
	}

	firstUserCaptured := false
	for _, msg := range req.Messages {
		if strings.TrimSpace(msg.Role) != "user" || firstUserCaptured {
			continue
		}
		seedParts = append(seedParts, "first_user="+protocolopenai.NormalizeCompatSeedJSON(msg.Content))
		firstUserCaptured = true
	}

	return compatPromptCacheKeyPrefix + upstream.HashSensitiveValueForLog(strings.Join(seedParts, "|"))
}

// DeriveAnthropicCacheControlPromptCacheKey hashes selected nonblank ephemeral
// text anchors and returns empty when none are found. Malformed system/content
// JSON contributes no blocks, leaving other decodable messages eligible.
//
// All qualifying system and assistant text anchors are retained in encounter
// order, while the first user anchor is appended after them. Role and text
// values are trimmed; block type and cache-control spelling require their
// supported exact values. The key uses the first sixteen SHA-256 bytes over
// this text seed and intentionally omits model identity and nontext anchor
// data.
func DeriveAnthropicCacheControlPromptCacheKey(req *protocolanthropic.AnthropicRequest) string {
	if req == nil {
		return ""
	}

	var parts []string
	var systemBlocks []protocolanthropic.AnthropicContentBlock
	// Malformed system blocks cannot supply typed text anchors. Ignore that
	// component while decodable message anchors still contribute to the cache key.
	if len(req.System) > 0 && json.Unmarshal(req.System, &systemBlocks) == nil {
		for _, block := range systemBlocks {
			if block.Type == "text" &&
				block.CacheControl != nil &&
				strings.TrimSpace(block.CacheControl.Type) == "ephemeral" &&
				strings.TrimSpace(block.Text) != "" {
				parts = append(parts, "system:"+strings.TrimSpace(block.Text))
			}
		}
	}

	firstUserAnchor := ""
	for _, msg := range req.Messages {
		var blocks []protocolanthropic.AnthropicContentBlock
		// Unreadable message content supplies no reliable anchor blocks; continue with
		// other decodable history so one malformed message does not abort key derivation.
		if len(msg.Content) == 0 || json.Unmarshal(msg.Content, &blocks) != nil {
			continue
		}
		role := strings.TrimSpace(msg.Role)
		for _, block := range blocks {
			if block.Type != "text" ||
				block.CacheControl == nil ||
				strings.TrimSpace(block.CacheControl.Type) != "ephemeral" ||
				strings.TrimSpace(block.Text) == "" {
				continue
			}
			switch role {
			case "user":
				if firstUserAnchor == "" {
					firstUserAnchor = strings.TrimSpace(block.Text)
				}
			case "assistant":
				parts = append(parts, "assistant:"+strings.TrimSpace(block.Text))
			}
		}
	}
	if firstUserAnchor != "" {
		parts = append(parts, "user_anchor:"+firstUserAnchor)
	}
	if len(parts) == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte("anthropic-cache:" + strings.Join(parts, "\n")))
	// Hex encoding this prefix keeps the digest suffix at 32 characters.
	return fmt.Sprintf("anthropic-cache-%x", sum[:16])
}

// DeriveCompatPromptCacheKeyBytes hashes opening Chat content and request
// configuration directly from raw JSON bytes using jsontext without decoding
// into subset structs or map[string]any. Empty or non-object bodies return "".
//
// The seed matches DeriveCompatPromptCacheKey's opening-content boundaries:
// resolved model, non-empty reasoning_effort, tool_choice, non-empty tools and
// functions arrays, all system messages, and the first user message. Each JSON
// component is canonicalized via jsontext.Value.Canonicalize so whitespace
// layout differences hash identically while 64-bit integer precision survives.
func DeriveCompatPromptCacheKeyBytes(body []byte, mappedModel string) string {
	if len(body) == 0 {
		return ""
	}

	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return ""
	}

	var (
		reqModel        string
		reasoningEffort string
		toolChoiceRaw   jsontext.Value
		toolsRaw        jsontext.Value
		functionsRaw    jsontext.Value
		messagesRaw     jsontext.Value
	)
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return ""
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return ""
		}
		switch key {
		case "model":
			_ = jsonv2.Unmarshal(val, &reqModel, jsontext.AllowInvalidUTF8(true))
		case "reasoning_effort":
			_ = jsonv2.Unmarshal(val, &reasoningEffort, jsontext.AllowInvalidUTF8(true))
		case "tool_choice":
			toolChoiceRaw = bytes.Clone(val)
		case "tools":
			toolsRaw = bytes.Clone(val)
		case "functions":
			functionsRaw = bytes.Clone(val)
		case "messages":
			messagesRaw = bytes.Clone(val)
		}
	}

	normalizedModel := strings.TrimSpace(mappedModel)
	if normalizedModel == "" {
		normalizedModel = strings.TrimSpace(reqModel)
	}

	seedParts := []string{"model=" + normalizedModel}
	if reasoningEffort != "" {
		seedParts = append(seedParts, "reasoning_effort="+strings.TrimSpace(reasoningEffort))
	}
	if len(toolChoiceRaw) > 0 {
		seedParts = append(seedParts, "tool_choice="+canonicalCompatSeedValue(toolChoiceRaw))
	}
	if compatRawArrayNonEmpty(toolsRaw) {
		seedParts = append(seedParts, "tools="+canonicalCompatSeedValue(toolsRaw))
	}
	if compatRawArrayNonEmpty(functionsRaw) {
		seedParts = append(seedParts, "functions="+canonicalCompatSeedValue(functionsRaw))
	}

	if len(messagesRaw) > 0 {
		msgDec := jsontext.NewDecoder(bytes.NewReader(messagesRaw), jsontext.AllowInvalidUTF8(true))
		if arrTok, err := msgDec.ReadToken(); err == nil && arrTok.Kind() == '[' {
			firstUserCaptured := false
			for msgDec.PeekKind() != ']' {
				msgVal, err := msgDec.ReadValue()
				if err != nil {
					break
				}
				role, contentVal := extractCompatMessageRoleAndContent(msgVal)
				switch strings.TrimSpace(role) {
				case "system":
					seedParts = append(seedParts, "system="+canonicalCompatSeedValue(contentVal))
				case "user":
					if !firstUserCaptured {
						seedParts = append(seedParts, "first_user="+canonicalCompatSeedValue(contentVal))
						firstUserCaptured = true
					}
				}
			}
		}
	}

	return compatPromptCacheKeyPrefix + upstream.HashSensitiveValueForLog(strings.Join(seedParts, "|"))
}

// DeriveAnthropicCompatPromptCacheKeyBytes derives an Anthropic compatibility
// cache key from raw JSON bytes without unmarshaling into subset structs.
// Explicit ephemeral text anchors take precedence over opening-content seeds,
// while empty or non-object bodies return an empty string.
func DeriveAnthropicCompatPromptCacheKeyBytes(body []byte, mappedModel string) string {
	if len(body) == 0 {
		return ""
	}
	if anchorKey := DeriveAnthropicCacheControlPromptCacheKeyBytes(body); anchorKey != "" {
		return anchorKey
	}

	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return ""
	}

	var (
		reqModel      string
		toolChoiceRaw jsontext.Value
		toolsRaw      jsontext.Value
		systemRaw     jsontext.Value
		messagesRaw   jsontext.Value
	)
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return ""
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return ""
		}
		switch key {
		case "model":
			_ = jsonv2.Unmarshal(val, &reqModel, jsontext.AllowInvalidUTF8(true))
		case "tool_choice":
			toolChoiceRaw = bytes.Clone(val)
		case "tools":
			toolsRaw = bytes.Clone(val)
		case "system":
			systemRaw = bytes.Clone(val)
		case "messages":
			messagesRaw = bytes.Clone(val)
		}
	}

	normalizedModel := strings.TrimSpace(mappedModel)
	if normalizedModel == "" {
		normalizedModel = strings.TrimSpace(reqModel)
	}

	seedParts := []string{"model=" + normalizedModel}
	if effort := strings.TrimSpace(ExtractAnthropicOutputEffortBytes(body)); effort != "" {
		seedParts = append(seedParts, "effort="+effort)
	}
	if len(toolChoiceRaw) > 0 {
		seedParts = append(seedParts, "tool_choice="+canonicalCompatSeedValue(toolChoiceRaw))
	}
	if compatRawArrayNonEmpty(toolsRaw) {
		seedParts = append(seedParts, "tools="+canonicalCompatSeedValue(toolsRaw))
	}
	if len(systemRaw) > 0 {
		seedParts = append(seedParts, "system="+canonicalCompatSeedValue(systemRaw))
	}

	if len(messagesRaw) > 0 {
		msgDec := jsontext.NewDecoder(bytes.NewReader(messagesRaw), jsontext.AllowInvalidUTF8(true))
		if arrTok, err := msgDec.ReadToken(); err == nil && arrTok.Kind() == '[' {
			firstUserCaptured := false
			for msgDec.PeekKind() != ']' {
				msgVal, err := msgDec.ReadValue()
				if err != nil {
					break
				}
				role, contentVal := extractCompatMessageRoleAndContent(msgVal)
				if strings.TrimSpace(role) != "user" || firstUserCaptured {
					continue
				}
				seedParts = append(seedParts, "first_user="+canonicalCompatSeedValue(contentVal))
				firstUserCaptured = true
			}
		}
	}

	return compatPromptCacheKeyPrefix + upstream.HashSensitiveValueForLog(strings.Join(seedParts, "|"))
}

// DeriveAnthropicCacheControlPromptCacheKeyBytes hashes non-blank ephemeral text
// anchors from raw Anthropic Messages JSON bytes without decoding into subset
// structs. Empty or malformed inputs or requests lacking qualifying anchors
// return "".
//
// System and assistant ephemeral text blocks are collected in encounter order
// and followed by the first user ephemeral text block, producing the identical
// anthropic-cache- digest as DeriveAnthropicCacheControlPromptCacheKey.
func DeriveAnthropicCacheControlPromptCacheKeyBytes(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return ""
	}

	var systemRaw, messagesRaw jsontext.Value
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return ""
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return ""
		}
		switch key {
		case "system":
			systemRaw = bytes.Clone(val)
		case "messages":
			messagesRaw = bytes.Clone(val)
		}
	}

	var parts []string
	// Non-array system fields carry no cache_control blocks; ignore them while
	// still scanning message content arrays for qualifying anchors.
	for _, text := range collectEphemeralTextAnchors(systemRaw) {
		parts = append(parts, "system:"+text)
	}

	firstUserAnchor := ""
	if len(messagesRaw) > 0 {
		msgDec := jsontext.NewDecoder(bytes.NewReader(messagesRaw), jsontext.AllowInvalidUTF8(true))
		if arrTok, err := msgDec.ReadToken(); err == nil && arrTok.Kind() == '[' {
			for msgDec.PeekKind() != ']' {
				msgVal, err := msgDec.ReadValue()
				if err != nil {
					break
				}
				role, contentVal := extractCompatMessageRoleAndContent(msgVal)
				trimmedRole := strings.TrimSpace(role)
				// String or malformed message content yields no anchor blocks without
				// aborting anchor discovery in subsequent conversation messages.
				for _, text := range collectEphemeralTextAnchors(contentVal) {
					switch trimmedRole {
					case "user":
						if firstUserAnchor == "" {
							firstUserAnchor = text
						}
					case "assistant":
						parts = append(parts, "assistant:"+text)
					}
				}
			}
		}
	}
	if firstUserAnchor != "" {
		parts = append(parts, "user_anchor:"+firstUserAnchor)
	}
	if len(parts) == 0 {
		return ""
	}
	sum := sha256.Sum256([]byte("anthropic-cache:" + strings.Join(parts, "\n")))
	// Hex encoding sixteen digest bytes produces the fixed 32-character key suffix.
	return fmt.Sprintf("anthropic-cache-%x", sum[:16])
}

// canonicalCompatSeedValue canonicalizes raw JSON value bytes via jsontext so
// whitespace and object-member order normalize without float64 rounding. Empty
// inputs return "", and malformed tokens fall back to their original raw string.
func canonicalCompatSeedValue(raw jsontext.Value) string {
	if len(raw) == 0 {
		return ""
	}
	cloned := jsontext.Value(bytes.Clone(raw))
	if err := cloned.Canonicalize(jsontext.AllowInvalidUTF8(true)); err != nil {
		// Retain the original byte text when canonicalization fails so malformed
		// components still distinguish different seeds instead of collapsing to empty.
		return string(raw)
	}
	return string(cloned)
}

// compatRawArrayNonEmpty reports whether raw is a valid JSON array containing at
// least one element under permissive UTF-8 scanning. Empty byte slices,
// non-array tokens, and syntax errors return false without panicking.
func compatRawArrayNonEmpty(raw jsontext.Value) bool {
	if len(raw) == 0 {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '[' {
		return false
	}
	return dec.PeekKind() != ']'
}

// extractCompatMessageRoleAndContent extracts the string role and cloned raw
// content value from one Chat or Anthropic message JSON object. Non-object or
// malformed message tokens return an empty role and nil content slice.
func extractCompatMessageRoleAndContent(rawMsg jsontext.Value) (string, jsontext.Value) {
	dec := jsontext.NewDecoder(bytes.NewReader(rawMsg), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return "", nil
	}
	var role string
	var content jsontext.Value
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return "", nil
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return "", nil
		}
		switch key {
		case "role":
			_ = jsonv2.Unmarshal(val, &role, jsontext.AllowInvalidUTF8(true))
		case "content":
			content = bytes.Clone(val)
		}
	}
	return role, content
}

// collectEphemeralTextAnchors extracts trimmed non-empty text strings from raw
// content-block arrays whose blocks carry type=="text" and ephemeral
// cache_control. Empty, non-array, or malformed block arrays return nil.
func collectEphemeralTextAnchors(rawBlocks jsontext.Value) []string {
	if len(rawBlocks) == 0 {
		return nil
	}
	dec := jsontext.NewDecoder(bytes.NewReader(rawBlocks), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '[' {
		return nil
	}
	var out []string
	for dec.PeekKind() != ']' {
		blockVal, err := dec.ReadValue()
		if err != nil {
			return nil
		}
		if text, ok := inspectEphemeralTextBlock(blockVal); ok {
			out = append(out, text)
		}
	}
	return out
}

// inspectEphemeralTextBlock checks one content block object for type=="text",
// cache_control.type=="ephemeral", and non-blank text, returning the trimmed
// text and true. Other block shapes or malformed objects return "" and false.
func inspectEphemeralTextBlock(rawBlock jsontext.Value) (string, bool) {
	dec := jsontext.NewDecoder(bytes.NewReader(rawBlock), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return "", false
	}
	var blockType, text, cacheType string
	hasCacheObj := false
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return "", false
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return "", false
		}
		switch key {
		case "type":
			_ = jsonv2.Unmarshal(val, &blockType, jsontext.AllowInvalidUTF8(true))
		case "text":
			_ = jsonv2.Unmarshal(val, &text, jsontext.AllowInvalidUTF8(true))
		case "cache_control":
			cacheDec := jsontext.NewDecoder(bytes.NewReader(val), jsontext.AllowInvalidUTF8(true))
			if cTok, cErr := cacheDec.ReadToken(); cErr == nil && cTok.Kind() == '{' {
				hasCacheObj = true
				for cacheDec.PeekKind() != '}' {
					ckTok, ckErr := cacheDec.ReadToken()
					if ckErr != nil {
						return "", false
					}
					ck := ckTok.String()
					cv, cvErr := cacheDec.ReadValue()
					if cvErr != nil {
						return "", false
					}
					if ck == "type" {
						_ = jsonv2.Unmarshal(cv, &cacheType, jsontext.AllowInvalidUTF8(true))
					}
				}
			}
		}
	}
	trimmedText := strings.TrimSpace(text)
	if blockType != "text" || !hasCacheObj || strings.TrimSpace(cacheType) != "ephemeral" || trimmedText == "" {
		return "", false
	}
	return trimmedText, true
}
