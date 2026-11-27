package ir

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// essence.go defines protocol-independent semantic summary types and projects
// IR Requests and Responses into canonical EssenceDoc values. Law tests Q4, S2,
// S4, S5, and S6 compare EssenceDoc values before and after applying
// encoder-reported Loss records so undocumented drops, mutations, or
// reorderings fail immediately without per-route hand-written wire snapshots.
//
// Summarization strips wire-only framing differences and cross-protocol default
// injections that have no corresponding LossKind while keeping 1-to-1 message,
// part, and tool slot alignment with original IR coordinates. JSON tool
// arguments and schemas canonicalize object key order and arbitrary-precision
// decimal numbers without float64 rounding, reasoning segments concatenate into
// one canonical string, gateway trc1 carrier envelopes unpack into underlying
// issuer schemes or plaintext reasoning segments, and correlated tool results
// resolve their originating ToolRef without mutating caller-owned requests.

const (
	// essenceNull through essenceObject classify lossless JSON scalar and
	// container nodes canonicalized when hashing schemas or tool arguments.
	essenceNull   essenceJSONKind = iota // JSON null literal value
	essenceBool                          // JSON boolean scalar value
	essenceNumber                        // arbitrary-precision JSON number value
	essenceString                        // decoded JSON string value
	essenceArray                         // ordered JSON array container
	essenceObject                        // key-value JSON object container
)

// essenceJSONKind tags the active value stored in an essenceJSONNode.
type essenceJSONKind uint8

// EssenceDoc captures the normalized, protocol-neutral semantic content of
// either an IR Request or an IR Response.
type EssenceDoc struct {
	System            []EssencePart           `json:"system,omitzero"`              // normalized leading system parts
	Messages          []EssenceMessage        `json:"messages,omitzero"`            // normalized conversation turns
	Tools             []EssenceTool           `json:"tools,omitzero"`               // normalized tool declarations
	ToolChoice        *EssenceToolChoice      `json:"tool_choice,omitzero"`         // normalized tool selection rule
	ParallelToolCalls *bool                   `json:"parallel_tool_calls,omitzero"` // parallel tool-call flag
	Sampling          Sampling                `json:"sampling,omitzero"`            // sampling and candidate parameters
	MaxOutputTokens   *int64                  `json:"max_output_tokens,omitzero"`   // output token cap
	Stop              []string                `json:"stop,omitzero"`                // custom stop sequences
	Reasoning         *EssenceReasoningConfig `json:"reasoning,omitzero"`           // reasoning request controls
	Output            *EssenceOutputFormat    `json:"output,omitzero"`              // structured output controls
	Parts             []EssencePart           `json:"parts,omitzero"`               // response output parts
	Finish            *Finish                 `json:"finish,omitzero"`              // response completion reason
	Usage             *Usage                  `json:"usage,omitzero"`               // response token usage buckets
	ServiceTier       string                  `json:"service_tier,omitzero"`        // requested or reported service tier
	HasExtra          bool                    `json:"has_extra,omitzero"`           // whether root Extra carried members
}

// EssenceMessage summarizes one conversation turn in an EssenceDoc.
type EssenceMessage struct {
	Role     Role          `json:"role"`               // message speaker role
	ID       string        `json:"id,omitzero"`        // wire message ID when tracked
	Parts    []EssencePart `json:"parts,omitzero"`     // ordered normalized parts
	HasExtra bool          `json:"has_extra,omitzero"` // whether Message.Extra carried members
}

// EssencePart summarizes one content, reasoning, tool, media, or opaque Part.
type EssencePart struct {
	Kind        PartKind            `json:"kind"`                 // part variant tag
	ID          string              `json:"id,omitzero"`          // wire item ID when tracked
	Text        string              `json:"text,omitzero"`        // text or refusal content
	Annotations []EssenceAnnotation `json:"annotations,omitzero"` // normalized text citations
	Token       *EssenceToken       `json:"token,omitzero"`       // unpacked text-attached continuity token
	Reasoning   *EssenceReasoning   `json:"reasoning,omitzero"`   // normalized reasoning content
	Call        *EssenceToolCall    `json:"call,omitzero"`        // normalized tool invocation
	Result      *EssenceToolResult  `json:"result,omitzero"`      // normalized tool result
	Media       *EssenceMedia       `json:"media,omitzero"`       // normalized media descriptor
	OpaqueType  string              `json:"opaque_type,omitzero"` // origin:type tag for opaque parts
	CacheTTL    string              `json:"cache_ttl,omitzero"`   // cache breakpoint TTL or "ephemeral"
	HasExtra    bool                `json:"has_extra,omitzero"`   // whether Part.Extra carried members
}

// EssenceAnnotation summarizes a structured citation attached to text.
type EssenceAnnotation struct {
	Kind       string `json:"kind,omitzero"`        // citation kind label
	URL        string `json:"url,omitzero"`         // cited source URL
	Title      string `json:"title,omitzero"`       // cited source title
	StartIndex *int64 `json:"start_index,omitzero"` // start span offset
	EndIndex   *int64 `json:"end_index,omitzero"`   // end span offset
	HasExtra   bool   `json:"has_extra,omitzero"`   // whether Annotation.Extra carried members
}

// EssenceReasoning summarizes visible reasoning text, flags, and unpacked continuity token.
type EssenceReasoning struct {
	Segments []string      `json:"segments,omitzero"` // visible reasoning text segments
	Summary  bool          `json:"summary,omitzero"`  // summary reasoning flag
	Redacted bool          `json:"redacted,omitzero"` // redacted_thinking block flag
	Token    *EssenceToken `json:"token,omitzero"`    // unpacked scheme and raw issuer token
}

// EssenceToken summarizes a ReplayToken after gateway carrier envelope unpacking.
type EssenceToken struct {
	Scheme     string `json:"scheme,omitzero"`      // continuity scheme identifier
	ProviderID int64  `json:"provider_id,omitzero"` // optional account-bound provider ID
	Data       string `json:"data"`                 // underlying issuer token data
}

// EssenceToolCall summarizes an assistant tool call with canonicalized JSON arguments.
type EssenceToolCall struct {
	CallID    string        `json:"call_id"`        // call correlation identifier
	Tool      ToolRef       `json:"tool"`           // tool kind, namespace, and name
	Arguments string        `json:"arguments"`      // canonical JSON arguments or custom free text
	Token     *EssenceToken `json:"token,omitzero"` // call-bound continuity token
}

// EssenceToolResult summarizes a tool execution output part inside a tool turn.
type EssenceToolResult struct {
	CallID  string        `json:"call_id"`           // correlated call identifier
	Tool    ToolRef       `json:"tool,omitzero"`     // linked tool identity when resolved
	Content []EssencePart `json:"content,omitzero"`  // normalized text and media parts
	IsError bool          `json:"is_error,omitzero"` // error status flag
}

// EssenceMedia summarizes a media attachment by modality, MIME type, and payload hash.
type EssenceMedia struct {
	Type        MediaType `json:"type"`            // attachment media modality
	MIME        string    `json:"mime,omitzero"`   // normalized MIME type
	Source      string    `json:"source"`          // base64, url, or file_id
	ContentHash string    `json:"content_hash"`    // SHA-256 hex digest of Media.Data
	Name        string    `json:"name,omitzero"`   // optional attachment name
	Detail      string    `json:"detail,omitzero"` // optional image detail level
}

// EssenceTool summarizes a declared tool and canonical hashes of its schema or grammar.
type EssenceTool struct {
	Kind             ToolKind `json:"kind"`                        // declared tool category
	Namespace        string   `json:"namespace,omitzero"`          // namespace scope when present
	Name             string   `json:"name"`                        // declared tool name
	Description      string   `json:"description,omitzero"`        // human-readable tool description
	SchemaHash       string   `json:"schema_hash,omitzero"`        // SHA-256 hex digest of canonical InputSchema
	FormatHash       string   `json:"format_hash,omitzero"`        // SHA-256 hex digest of canonical Format
	Strict           *bool    `json:"strict,omitzero"`             // strict schema flag
	DeferLoading     *bool    `json:"defer_loading,omitzero"`      // deferred loading flag
	HostedType       string   `json:"hosted_type,omitzero"`        // origin:type tag for hosted tools
	HostedConfigHash string   `json:"hosted_config_hash,omitzero"` // SHA-256 hex digest of canonical Hosted.Raw
	CacheTTL         string   `json:"cache_ttl,omitzero"`          // cache breakpoint TTL or "ephemeral"
	HasExtra         bool     `json:"has_extra,omitzero"`          // whether Tool.Extra carried members
}

// EssenceToolChoice summarizes a Request's ToolChoice mode and allowed subset.
type EssenceToolChoice struct {
	Mode         ToolChoiceMode `json:"mode"`                   // tool choice mode
	Tool         ToolRef        `json:"tool,omitzero"`          // required tool reference
	AllowedTools []ToolRef      `json:"allowed_tools,omitzero"` // allowed tool subset
}

// EssenceReasoningConfig summarizes requested thinking mode, effort, and token budget.
type EssenceReasoningConfig struct {
	Mode             string `json:"mode,omitzero"`              // requested thinking mode
	Effort           string `json:"effort,omitzero"`            // reasoning effort tier
	BudgetTokens     *int64 `json:"budget_tokens,omitzero"`     // explicit token budget
	Summary          string `json:"summary,omitzero"`           // reasoning summary mode
	IncludeEncrypted bool   `json:"include_encrypted,omitzero"` // encrypted reasoning request flag
}

// EssenceOutputFormat summarizes structured response schema hashes and verbosity settings.
type EssenceOutputFormat struct {
	Kind        string `json:"kind,omitzero"`        // output format kind
	Name        string `json:"name,omitzero"`        // JSON schema name
	Description string `json:"description,omitzero"` // optional JSON schema description
	SchemaHash  string `json:"schema_hash,omitzero"` // SHA-256 hex digest of canonical Schema
	Strict      *bool  `json:"strict,omitzero"`      // strict mode flag
	Verbosity   string `json:"verbosity,omitzero"`   // text verbosity level
}

// essenceDecNumber stores an arbitrary-precision JSON decimal number without float64 loss.
type essenceDecNumber struct {
	zero   bool   // true when numeric value equals zero
	neg    bool   // true for strictly negative numbers
	digits string // trimmed significand digits
	exp    int    // base-10 exponent applied to digits
}

// essenceJSONNode represents a decoded JSON value for canonical argument and schema hashing.
type essenceJSONNode struct {
	kind   essenceJSONKind            // scalar or container category
	bVal   bool                       // decoded boolean scalar value
	nVal   essenceDecNumber           // normalized decimal number payload
	sVal   string                     // decoded string scalar value
	arrVal []essenceJSONNode          // ordered array element nodes
	objVal map[string]essenceJSONNode // object members keyed by name
}

// Essence constructs a protocol-independent EssenceDoc summary of req without
// mutating req or sharing slice or pointer storage with it. Nil requests return
// a zero EssenceDoc value.
//
// Each message, part, and tool keeps its exact slice position in req so
// Loss.Path indices recorded during encoding match tracked EssenceDoc slots in
// testkit.VerifyEssence. Protocol-defaulted transport controls without LossKind
// handlers (ParallelToolCalls, MaxOutputTokens, ServiceTier, Reasoning, and
// standalone Output.Verbosity) are excluded so injected cross-protocol defaults
// do not cause spurious Q4 mismatches, while correlated tool results resolve
// their originating ToolRef on a cloned working copy.
func Essence(req *Request) EssenceDoc {
	if req == nil {
		return EssenceDoc{}
	}
	linked := req.Clone()
	Link(linked)

	doc := EssenceDoc{
		System:   summarizePartSlice(linked.System),
		Sampling: cloneSampling(linked.Sampling),
		Stop:     cloneNonEmpty(linked.Stop),
		HasExtra: hasMaterialRootExtra(linked.Extra),
	}

	if len(linked.Messages) > 0 {
		doc.Messages = make([]EssenceMessage, len(linked.Messages))
		for i, m := range linked.Messages {
			doc.Messages[i] = EssenceMessage{
				Role:     m.Role,
				ID:       m.ID,
				Parts:    summarizePartSlice(m.Parts),
				HasExtra: !m.Extra.IsZero(),
			}
		}
	}

	if len(linked.Tools) > 0 {
		doc.Tools = make([]EssenceTool, len(linked.Tools))
		for i, t := range linked.Tools {
			doc.Tools[i] = summarizeTool(t)
		}
	}

	if linked.ToolChoice != nil {
		doc.ToolChoice = &EssenceToolChoice{
			Mode:         linked.ToolChoice.Mode,
			Tool:         linked.ToolChoice.Tool,
			AllowedTools: cloneNonEmpty(linked.ToolChoice.AllowedTools),
		}
	}

	if linked.Output != nil {
		of := EssenceOutputFormat{
			Kind:        linked.Output.Kind,
			Name:        linked.Output.Name,
			Description: linked.Output.Description,
			SchemaHash:  hashCanonicalOrRaw(linked.Output.Schema),
			Strict:      cloneBoolPtr(linked.Output.Strict),
		}
		if of != (EssenceOutputFormat{}) {
			doc.Output = &of
		}
	}

	return doc
}

// ResponseEssence constructs a protocol-independent EssenceDoc summary of resp
// containing unpacked output Parts and root Extra presence for Law S4 checks.
// Nil responses return a zero EssenceDoc value, and protocol-asymmetric Finish,
// Usage, and ServiceTier fields are omitted because single-protocol laws
// inspect decoded Response values directly and S8 verifies usage buckets separately.
func ResponseEssence(resp *Response) EssenceDoc {
	if resp == nil {
		return EssenceDoc{}
	}
	return EssenceDoc{
		Parts:    summarizePartSlice(resp.Parts),
		HasExtra: !resp.Extra.IsZero(),
	}
}

// summarizePartSlice projects each Part in parts into a detached EssencePart
// slice in turn order. Empty or nil inputs return nil so omitzero omits empty
// part collections during JSON comparison.
func summarizePartSlice(parts []Part) []EssencePart {
	if len(parts) == 0 {
		return nil
	}
	out := make([]EssencePart, len(parts))
	for i, p := range parts {
		out[i] = summarizePart(p)
	}
	return out
}

// summarizePart projects one IR Part into a protocol-neutral EssencePart.
// Media parts hash their payload bytes, omit optional Name and Detail hints,
// and record normalized MIME types only for inline base64 attachments because
// URL media lacks wire MIME fields outside Gemini fileData.
func summarizePart(p Part) EssencePart {
	ep := EssencePart{
		Kind:     p.Kind,
		ID:       p.ID,
		Text:     p.Text,
		Token:    summarizeToken(p.Token),
		CacheTTL: summarizeCacheTTL(p.Cache),
		HasExtra: hasMaterialPartExtra(p),
	}
	if len(p.Annotations) > 0 {
		ep.Annotations = make([]EssenceAnnotation, len(p.Annotations))
		for i, a := range p.Annotations {
			ep.Annotations[i] = EssenceAnnotation{
				Kind:       a.Kind,
				URL:        a.URL,
				Title:      a.Title,
				StartIndex: cloneInt64Ptr(a.StartIndex),
				EndIndex:   cloneInt64Ptr(a.EndIndex),
				HasExtra:   !a.Extra.IsZero(),
			}
		}
	}
	if p.Reasoning != nil {
		ep.Reasoning = summarizeReasoning(p.Reasoning)
	}
	if p.Call != nil {
		ep.Call = summarizeToolCall(p.Call)
	}
	if p.Result != nil {
		ep.Result = &EssenceToolResult{
			CallID:  p.Result.CallID,
			Tool:    p.Result.Tool,
			Content: summarizePartSlice(p.Result.Content),
			IsError: p.Result.IsError,
		}
	}
	if p.Media != nil {
		var mime string
		if p.Media.Source == MediaSourceBase64 {
			mime = strings.ToLower(strings.TrimSpace(p.Media.MIME))
		}
		ep.Media = &EssenceMedia{
			Type:        p.Media.Type,
			MIME:        mime,
			Source:      p.Media.Source,
			ContentHash: sha256Hex([]byte(p.Media.Data)),
		}
	}
	if p.Opaque != nil {
		ep.OpaqueType = formatOriginType(p.Opaque.Origin, p.Opaque.Type)
	}
	return ep
}

// hasMaterialRootExtra reports whether extra carries unmodeled request fields
// beyond Gemini's round-trip generationConfig.thinkingConfig.includeThoughts=false hint.
func hasMaterialRootExtra(extra Extra) bool {
	if extra.IsZero() {
		return false
	}
	if extra.Origin == protocol.ProtocolGeminiGenerateContent && len(extra.Members) == 1 {
		if rawGC, ok := extra.Members["generationConfig"]; ok {
			if canon, err := canonicalizeJSONBytes(rawGC); err == nil &&
				bytes.Equal(canon, []byte(`{"thinkingConfig":{"includeThoughts":false}}`)) {
				return false
			}
		}
	}
	return true
}

// hasMaterialPartExtra reports whether p.Extra holds protocol extension fields
// beyond Anthropic document title hints populated from cross-protocol Media.Name.
func hasMaterialPartExtra(p Part) bool {
	if p.Extra.IsZero() {
		return false
	}
	if p.Kind == PartMedia && p.Media != nil && p.Media.Type == MediaDocument &&
		p.Extra.Origin == protocol.ProtocolAnthropicMessages {
		for k := range p.Extra.Members {
			if k != "title" {
				return true
			}
		}
		return false
	}
	return true
}

// summarizeReasoning joins multi-segment reasoning text into a single canonical
// element, unpacks any trc1 carrier on r.Token, and omits wire-only Summary flags.
// Multi-segment OpenAI Responses summaries therefore reconcile directly with
// single-string Messages thinking blocks, Chat reasoning_content, and unpacked
// carrier text without needing a synthetic segment-merge LossKind.
//
// Plain carriers restore missing reasoning text and leave Token nil because they
// carry no upstream cryptographic signature. Anthropic and Antigravity Claude
// non-redacted carriers replace abbreviated client summaries with the exact
// signed thinking text stored in the envelope.
func summarizeReasoning(r *Reasoning) *EssenceReasoning {
	var segments []string
	if joined := strings.Join(r.Segments, ""); joined != "" {
		segments = []string{joined}
	}
	er := &EssenceReasoning{
		Segments: segments,
		Redacted: r.Redacted,
	}
	if r.Token == nil || r.Token.Data == "" {
		return er
	}
	carrier, ok := ParseCarrier(r.Token.Data)
	if !ok {
		er.Token = &EssenceToken{
			Scheme:     r.Token.Scheme,
			ProviderID: r.Token.ProviderID,
			Data:       r.Token.Data,
		}
		return er
	}

	er.Redacted = er.Redacted || carrier.Redacted
	switch carrier.Scheme {
	case SchemePlain:
		// Plain carriers wrap tokenless reasoning text rather than an upstream
		// cryptographic token; restore missing segments and leave Token nil.
		if len(er.Segments) == 0 && carrier.Text != "" {
			er.Segments = []string{carrier.Text}
		}

	case SchemeAnthropic, SchemeAntigravityClaude:
		if !er.Redacted && carrier.Text != "" {
			// Anthropic signatures bind to exact thinking text stored inside the
			// carrier, replacing abbreviated client summaries on replay.
			er.Segments = []string{carrier.Text}
		}
		er.Token = &EssenceToken{
			Scheme:     carrier.Scheme,
			ProviderID: carrier.ProviderID,
			Data:       carrier.Data,
		}

	default:
		er.Token = &EssenceToken{
			Scheme:     carrier.Scheme,
			ProviderID: carrier.ProviderID,
			Data:       carrier.Data,
		}
	}
	return er
}

// summarizeToolCall canonicalizes JSON arguments for non-custom tools while
// keeping custom-tool free text and truncated partial JSON fragments verbatim.
// Completed function calls with whitespace-empty argument strings normalize to
// "{}" so string-based Chat or Responses calls match object-based Messages and Gemini calls.
func summarizeToolCall(c *ToolCall) *EssenceToolCall {
	args := c.Arguments
	if c.Tool.Kind != ToolCustom {
		if strings.TrimSpace(args) == "" && !c.Incomplete && c.Tool.Kind == ToolFunction {
			// Object-based protocols (Messages, Gemini) represent empty function
			// arguments as {}, matching empty string arguments in Chat/Responses.
			args = "{}"
		} else if canon, err := canonicalizeJSONBytes([]byte(args)); err == nil {
			args = string(canon)
		}
	}
	return &EssenceToolCall{
		CallID:    c.CallID,
		Tool:      c.Tool,
		Arguments: args,
		Token:     summarizeToken(c.Token),
	}
}

// summarizeToken unpacks a call-attached ReplayToken when encoded as a trc1
// carrier and returns nil when tok is nil, empty, or wraps a plain text carrier.
// Bare upstream tokens copy their Scheme, ProviderID, and Data fields directly.
func summarizeToken(tok *ReplayToken) *EssenceToken {
	if tok == nil || tok.Data == "" {
		return nil
	}
	if carrier, ok := ParseCarrier(tok.Data); ok {
		if carrier.Scheme == SchemePlain {
			return nil
		}
		return &EssenceToken{
			Scheme:     carrier.Scheme,
			ProviderID: carrier.ProviderID,
			Data:       carrier.Data,
		}
	}
	return &EssenceToken{
		Scheme:     tok.Scheme,
		ProviderID: tok.ProviderID,
		Data:       tok.Data,
	}
}

// summarizeTool projects one Tool declaration into an EssenceTool with canonical
// SHA-256 schema, grammar, and hosted configuration hashes. Hosted tools record
// both their "origin:type" discriminator and raw configuration digest, and all
// tools preserve detached Strict, DeferLoading, and CacheTTL values.
func summarizeTool(t Tool) EssenceTool {
	et := EssenceTool{
		Kind:         t.Kind,
		Namespace:    t.Namespace,
		Name:         t.Name,
		Description:  t.Description,
		SchemaHash:   hashCanonicalOrRaw(t.InputSchema),
		FormatHash:   hashCanonicalOrRaw(t.Format),
		Strict:       cloneBoolPtr(t.Strict),
		DeferLoading: cloneBoolPtr(t.DeferLoading),
		CacheTTL:     summarizeCacheTTL(t.Cache),
		HasExtra:     !t.Extra.IsZero(),
	}
	if t.Hosted != nil {
		et.HostedType = formatOriginType(t.Hosted.Origin, t.Hosted.Type)
		et.HostedConfigHash = hashCanonicalOrRaw(t.Hosted.Raw)
	}
	return et
}

// summarizeCacheTTL maps nil CacheMarks to "", untimed breakpoints with empty
// TTL to "ephemeral", and explicit lifetime strings ("5m", "1h") to themselves.
// Separating nil from empty TTL lets VerifyEssence detect dropped untimed breakpoints.
func summarizeCacheTTL(mark *CacheMark) string {
	if mark == nil {
		return ""
	}
	if mark.TTL == "" {
		return "ephemeral"
	}
	return mark.TTL
}

// formatOriginType joins origin and typ as "origin:type" for opaque and hosted
// tool summaries. Empty origin values return typ alone without prepending a
// leading colon separator.
func formatOriginType(origin protocol.ProtocolID, typ string) string {
	if origin == "" {
		return typ
	}
	return string(origin) + ":" + typ
}

// hashCanonicalOrRaw returns the lowercase SHA-256 hex digest of canonicalized
// JSON bytes, or "" when raw contains only whitespace. Inputs that fail JSON
// parsing fall back to hashing trimmed raw bytes so callers never panic.
func hashCanonicalOrRaw(raw jsontext.Value) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	canon, err := canonicalizeJSONBytes(trimmed)
	if err != nil {
		// Malformed synthetic test schemas cannot parse into JSON nodes; hash
		// trimmed raw bytes so distinct invalid inputs still produce distinct digests.
		return sha256Hex(trimmed)
	}
	return sha256Hex(canon)
}

// sha256Hex computes the lowercase SHA-256 hexadecimal digest of data.
func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// canonicalizeJSONBytes parses raw without float64 precision loss and re-encodes
// it with sorted object keys and normalized decimal numbers. Duplicate object
// member names, syntax errors, or trailing tokens after the root value return an error.
func canonicalizeJSONBytes(raw []byte) ([]byte, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	node, err := readEssenceJSONNode(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("unexpected trailing JSON token")
		}
		return nil, err
	}
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := writeEssenceJSONNode(enc, node); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// readEssenceJSONNode recursively consumes one complete JSON value from dec into
// an essenceJSONNode tree. Duplicate member names within the same JSON object
// or malformed tokens return an error immediately.
func readEssenceJSONNode(dec *jsontext.Decoder) (essenceJSONNode, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return essenceJSONNode{}, err
	}
	switch tok.Kind() {
	case 'n':
		return essenceJSONNode{kind: essenceNull}, nil
	case 'f', 't':
		return essenceJSONNode{kind: essenceBool, bVal: tok.Bool()}, nil
	case '0':
		num, err := parseEssenceNumber(tok.String())
		if err != nil {
			return essenceJSONNode{}, err
		}
		return essenceJSONNode{kind: essenceNumber, nVal: num}, nil
	case '"':
		return essenceJSONNode{kind: essenceString, sVal: tok.String()}, nil
	case '[':
		var items []essenceJSONNode
		for dec.PeekKind() != ']' {
			elem, err := readEssenceJSONNode(dec)
			if err != nil {
				return essenceJSONNode{}, err
			}
			items = append(items, elem)
		}
		if _, err := dec.ReadToken(); err != nil {
			return essenceJSONNode{}, err
		}
		return essenceJSONNode{kind: essenceArray, arrVal: items}, nil
	case '{':
		members := make(map[string]essenceJSONNode)
		for dec.PeekKind() != '}' {
			keyTok, err := dec.ReadToken()
			if err != nil {
				return essenceJSONNode{}, err
			}
			key := keyTok.String()
			if _, exists := members[key]; exists {
				return essenceJSONNode{}, fmt.Errorf("duplicate JSON key %q", key)
			}
			val, err := readEssenceJSONNode(dec)
			if err != nil {
				return essenceJSONNode{}, err
			}
			members[key] = val
		}
		if _, err := dec.ReadToken(); err != nil {
			return essenceJSONNode{}, err
		}
		return essenceJSONNode{kind: essenceObject, objVal: members}, nil
	default:
		return essenceJSONNode{}, fmt.Errorf("unexpected token kind %q", tok.Kind())
	}
}

// parseEssenceNumber decomposes a valid JSON number token into sign, trimmed
// significand digits, and base-10 exponent. Zero representations such as "-0.0"
// normalize to a single unsigned zero value.
func parseEssenceNumber(raw string) (essenceDecNumber, error) {
	s := raw
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = s[1:]
	}
	expPart := 0
	if idx := strings.IndexAny(s, "eE"); idx >= 0 {
		parsedExp, err := strconv.Atoi(s[idx+1:])
		if err != nil {
			return essenceDecNumber{}, err
		}
		expPart = parsedExp
		s = s[:idx]
	}
	intPart, fracPart, hasDot := strings.Cut(s, ".")
	combined := intPart + fracPart
	if hasDot {
		expPart -= len(fracPart)
	}
	trimmedLead := strings.TrimLeft(combined, "0")
	if trimmedLead == "" {
		return essenceDecNumber{zero: true}, nil
	}
	trimmed := strings.TrimRight(trimmedLead, "0")
	expPart += len(trimmedLead) - len(trimmed)
	return essenceDecNumber{
		neg:    neg,
		digits: trimmed,
		exp:    expPart,
	}, nil
}

// formatEssenceNumber renders n into canonical JSON decimal spelling matching
// testkit.CanonicalJSON. Exponents placing the decimal point in [-5, 21] use
// fixed-point notation while larger magnitudes switch to scientific notation.
func formatEssenceNumber(n essenceDecNumber) string {
	if n.zero {
		return "0"
	}
	var b strings.Builder
	if n.neg {
		_ = b.WriteByte('-')
	}
	// Render moderate exponents in fixed-point notation and use scientific
	// notation outside [-5, 21] to match testkit.CanonicalJSON and bound buffer size.
	pointPos := len(n.digits) + n.exp
	if pointPos > 0 && pointPos <= 21 {
		if pointPos >= len(n.digits) {
			_, _ = b.WriteString(n.digits)
			_, _ = b.WriteString(strings.Repeat("0", pointPos-len(n.digits)))
		} else {
			_, _ = b.WriteString(n.digits[:pointPos])
			_ = b.WriteByte('.')
			_, _ = b.WriteString(n.digits[pointPos:])
		}
		return b.String()
	}
	if pointPos <= 0 && pointPos >= -5 {
		_, _ = b.WriteString("0.")
		_, _ = b.WriteString(strings.Repeat("0", -pointPos))
		_, _ = b.WriteString(n.digits)
		return b.String()
	}
	_ = b.WriteByte(n.digits[0])
	if len(n.digits) > 1 {
		_ = b.WriteByte('.')
		_, _ = b.WriteString(n.digits[1:])
	}
	sciExp := len(n.digits) - 1 + n.exp
	fmt.Fprintf(&b, "e%d", sciExp)
	return b.String()
}

// writeEssenceJSONNode writes n into enc with lexicographically sorted object
// member keys and canonicalized decimal numbers. Any underlying encoder write
// failure aborts traversal and returns that error.
func writeEssenceJSONNode(enc *jsontext.Encoder, n essenceJSONNode) error {
	switch n.kind {
	case essenceNull:
		return enc.WriteToken(jsontext.Null)
	case essenceBool:
		return enc.WriteToken(jsontext.Bool(n.bVal))
	case essenceNumber:
		return enc.WriteValue(jsontext.Value(formatEssenceNumber(n.nVal)))
	case essenceString:
		return enc.WriteToken(jsontext.String(n.sVal))
	case essenceArray:
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for _, elem := range n.arrVal {
			if err := writeEssenceJSONNode(enc, elem); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndArray)
	case essenceObject:
		if err := enc.WriteToken(jsontext.BeginObject); err != nil {
			return err
		}
		keys := make([]string, 0, len(n.objVal))
		for k := range n.objVal {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			if err := enc.WriteToken(jsontext.String(k)); err != nil {
				return err
			}
			if err := writeEssenceJSONNode(enc, n.objVal[k]); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndObject)
	default:
		return fmt.Errorf("unknown essenceJSONKind %d", n.kind)
	}
}
