package ir

import (
	"bytes"
	"encoding/json/jsontext"
	"slices"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// request.go defines the protocol-neutral conversation request intermediate
// representation, its extension containers, and deep-copy constructors.
// Decoders normalize protocol wire payloads into ordered messages and typed
// parts while preserving unmodeled object members in Extra and single-protocol
// items in OpaqueItem so no input information disappears during request decoding.
//
// Request transformations mutate messages, tool declarations, and continuity
// tokens for each upstream attempt while response encoders still inspect the
// original decoded client request through ClientView. Clone methods copy slices,
// pointer branches, and raw JSON byte buffers so attempt-local rewrites never
// alias caller-retained request snapshots.

const (
	// Role values classify conversation turns after lifting tool results into
	// dedicated tool turns and separating leading instructions into Request.System.
	RoleSystem    Role = "system"    // system instruction turn
	RoleDeveloper Role = "developer" // developer instruction turn
	RoleUser      Role = "user"      // end-user input turn
	RoleAssistant Role = "assistant" // model assistant turn
	RoleTool      Role = "tool"      // tool execution result turn

	// PartKind values select the active payload branch on a Part union value
	// across system instructions, conversation messages, and model responses.
	PartText       PartKind = "text"        // visible plain text content
	PartReasoning  PartKind = "reasoning"   // model reasoning or summary block
	PartToolCall   PartKind = "tool_call"   // assistant tool invocation
	PartToolResult PartKind = "tool_result" // tool execution output
	PartMedia      PartKind = "media"       // image, audio, video, or document attachment
	PartRefusal    PartKind = "refusal"     // structured model refusal text
	PartOpaque     PartKind = "opaque"      // origin-specific wire item

	// ToolKind values distinguish callable functions, free-form custom grammars,
	// client tool search, and provider-hosted server tools.
	ToolFunction     ToolKind = "function"      // JSON-schema function tool
	ToolCustom       ToolKind = "custom"        // free-text or grammar custom tool
	ToolClientSearch ToolKind = "client_search" // client-executed tool_search declaration
	ToolHosted       ToolKind = "hosted"        // upstream-hosted server tool

	// MediaType values identify the attachment modality carried by a Media part
	// so encoders can enforce protocol media support rules.
	MediaImage    MediaType = "image"    // still or animated image content
	MediaAudio    MediaType = "audio"    // audio input clip
	MediaVideo    MediaType = "video"    // video input clip
	MediaDocument MediaType = "document" // PDF or document attachment

	// MediaSource constants describe how Media.Data encodes the attachment
	// payload across inline bytes, remote URLs, and uploaded file references.
	MediaSourceBase64 = "base64"  // inline base64-encoded bytes
	MediaSourceURL    = "url"     // remote or data URL reference
	MediaSourceFileID = "file_id" // upstream file identifier

	// ToolChoiceMode values govern how the model may select among declared
	// tools on a single generation request.
	ToolChoiceAuto     ToolChoiceMode = "auto"     // model chooses zero or more tools
	ToolChoiceNone     ToolChoiceMode = "none"     // tool invocation disabled
	ToolChoiceRequired ToolChoiceMode = "required" // model must invoke at least one tool
	ToolChoiceTool     ToolChoiceMode = "tool"     // model must invoke ToolChoice.Tool

	// Replay continuity schemes label who issued a ReplayToken or gateway carrier
	// envelope so encoders only replay tokens accepted by the target endpoint.
	SchemeAnthropic         = "anthropic"          // Anthropic Messages thinking signature
	SchemeAntigravityClaude = "antigravity_claude" // Antigravity Claude thinking signature
	SchemeOpenAI            = "openai"             // OpenAI account-bound encrypted reasoning
	SchemeXAI               = "xai"                // xAI account-bound encrypted reasoning
	SchemeGemini            = "gemini"             // Gemini thoughtSignature token
	SchemePlain             = "plain"              // gateway plaintext reasoning carrier
	SchemeUnknown           = "unknown"            // unrecognized client-supplied bare token
)

// Role identifies the speaker or turn category of a conversation Message.
type Role string

// PartKind identifies which branch of a Part union carries a value.
type PartKind string

// ToolKind identifies the invocation contract of a declared or called tool.
type ToolKind string

// MediaType classifies the modality of a Media part.
type MediaType string

// ToolChoiceMode specifies the tool selection constraint on a Request.
type ToolChoiceMode string

// Extra holds JSON object members that an origin protocol decoder did not map
// onto modeled IR fields. Only an encoder targeting the same Origin protocol
// writes Members back; cross-protocol encoders drop them and report Loss.
type Extra struct {
	Origin  protocol.ProtocolID       `json:"origin"`  // protocol that decoded Members
	Members map[string]jsontext.Value `json:"members"` // unmodeled raw JSON object members
}

// Request represents one normalized text or multimodal generation request
// across Anthropic Messages, OpenAI Responses, Chat Completions, Gemini, and Qoder.
type Request struct {
	Model              string           `json:"model"`                         // target or client model identifier
	System             []Part           `json:"system,omitzero"`               // out-of-band leading instructions
	Messages           []Message        `json:"messages"`                      // ordered conversation turns
	Tools              []Tool           `json:"tools,omitzero"`                // callable and hosted tool declarations
	Namespaces         []Namespace      `json:"namespaces,omitzero"`           // Responses namespace declarations
	ToolChoice         *ToolChoice      `json:"tool_choice,omitzero"`          // tool invocation policy
	ParallelToolCalls  *bool            `json:"parallel_tool_calls,omitzero"`  // explicit parallel tool-call toggle
	Sampling           Sampling         `json:"sampling,omitzero"`             // decoding and candidate controls
	MaxOutputTokens    *int64           `json:"max_output_tokens,omitzero"`    // maximum generated output tokens
	Stop               []string         `json:"stop,omitzero"`                 // custom stop sequences
	Reasoning          *ReasoningConfig `json:"reasoning,omitzero"`            // thinking mode, effort, and budget
	Output             *OutputFormat    `json:"output,omitzero"`               // structured output and verbosity settings
	Stream             bool             `json:"stream,omitzero"`               // whether streaming delivery was requested
	IncludeUsage       *bool            `json:"include_usage,omitzero"`        // Chat stream_options.include_usage preference
	ServiceTier        string           `json:"service_tier,omitzero"`         // requested processing tier
	CacheKey           string           `json:"cache_key,omitzero"`            // cross-protocol prompt_cache_key value
	PreviousResponseID string           `json:"previous_response_id,omitzero"` // stateful Responses continuation reference
	SessionID          string           `json:"session_id,omitzero"`           // explicit client session identifier
	ConversationID     string           `json:"conversation_id,omitzero"`      // explicit conversation identifier
	// ConversationSet records a non-null Responses top-level conversation field
	// even when ConversationID cannot represent an object form, letting stateless
	// upstreams reject stateful conversation bindings deterministically.
	ConversationSet bool   `json:"conversation_set,omitzero"`
	Store           *bool  `json:"store,omitzero"`            // upstream response persistence toggle
	UserID          string `json:"user_id,omitzero"`          // caller end-user account identifier
	MetadataUserID  string `json:"metadata_user_id,omitzero"` // metadata.user_id when distinct from UserID
	Extra           Extra  `json:"extra,omitzero"`            // unmodeled top-level request members
}

// Message represents one ordered turn in a Request with sequential content Parts.
type Message struct {
	Role  Role   `json:"role"`           // turn speaker role
	ID    string `json:"id,omitzero"`    // wire message identifier when present
	Parts []Part `json:"parts"`          // ordered content parts within the turn
	Extra Extra  `json:"extra,omitzero"` // unmodeled wire message members
}

// Part represents a tagged union element inside System, Message.Parts,
// ToolResult.Content, or Response.Parts. Exactly one branch matching Kind is
// populated on valid IR values.
type Part struct {
	Kind        PartKind     `json:"kind"`                 // active union variant tag
	ID          string       `json:"id,omitzero"`          // wire item identifier when assigned
	Text        string       `json:"text,omitzero"`        // payload for text and refusal parts
	Annotations []Annotation `json:"annotations,omitzero"` // structured citations attached to text
	// Token holds a replay continuity signature attached to a PartText inside an
	// assistant message or response/stream output; validation rejects non-nil
	// values on non-text parts, system instructions, tool results, and user turns.
	Token     *ReplayToken `json:"token,omitzero"`
	Reasoning *Reasoning   `json:"reasoning,omitzero"` // payload for reasoning parts
	Call      *ToolCall    `json:"call,omitzero"`      // payload for tool_call parts
	Result    *ToolResult  `json:"result,omitzero"`    // payload for tool_result parts
	Media     *Media       `json:"media,omitzero"`     // payload for media parts
	Opaque    *OpaqueItem  `json:"opaque,omitzero"`    // payload for opaque parts
	Cache     *CacheMark   `json:"cache,omitzero"`     // prompt cache breakpoint directive
	Extra     Extra        `json:"extra,omitzero"`     // unmodeled part or item members
}

// Annotation represents a citation or grounding span attached to a text Part.
type Annotation struct {
	Kind       string `json:"kind,omitzero"`        // citation kind such as url_citation
	URL        string `json:"url,omitzero"`         // cited source URL
	Title      string `json:"title,omitzero"`       // cited source title
	StartIndex *int64 `json:"start_index,omitzero"` // UTF-8 or character start offset
	EndIndex   *int64 `json:"end_index,omitzero"`   // UTF-8 or character end offset
	Extra      Extra  `json:"extra,omitzero"`       // unmodeled citation members
}

// CacheMark records a prompt-cache breakpoint and optional TTL preference.
type CacheMark struct {
	TTL   string `json:"ttl,omitzero"`   // cache lifetime "", "5m", or "1h"
	Extra Extra  `json:"extra,omitzero"` // unmodeled cache_control members
}

// Namespace records a Responses tool namespace group whose child tools are
// expanded into Request.Tools with matching Tool.Namespace values.
type Namespace struct {
	Name         string     `json:"name"`                   // declared namespace group name
	Description  string     `json:"description,omitzero"`   // human-readable namespace description
	DeferLoading *bool      `json:"defer_loading,omitzero"` // deferred tool loading flag
	Cache        *CacheMark `json:"cache,omitzero"`         // optional cache breakpoint
	Extra        Extra      `json:"extra,omitzero"`         // unmodeled namespace members
}

// Reasoning holds visible thinking segments, redaction state, and any issuer
// replay continuity token for a reasoning Part.
type Reasoning struct {
	Segments []string     `json:"segments,omitzero"` // visible reasoning text segments
	Summary  bool         `json:"summary,omitzero"`  // true for summaries rather than raw reasoning
	Redacted bool         `json:"redacted,omitzero"` // true for Anthropic redacted_thinking blocks
	Token    *ReplayToken `json:"token,omitzero"`    // continuity token or carrier data
}

// ReplayToken holds opaque continuity data issued by an upstream reasoning,
// assistant text, or tool-call signature scheme. Codecs copy wire strings into
// Data verbatim while the conversion engine assigns Scheme and ProviderID.
type ReplayToken struct {
	Scheme     string `json:"scheme,omitzero"`      // continuity scheme label assigned by convert
	ProviderID int64  `json:"provider_id,omitzero"` // issuing provider ID for account-bound schemes
	Data       string `json:"data"`                 // exact wire token or carrier text
}

// ToolCall represents an assistant tool invocation inside a Part.
type ToolCall struct {
	CallID    string       `json:"call_id"`        // call correlation identifier
	Tool      ToolRef      `json:"tool"`           // invoked tool identity
	Arguments string       `json:"arguments"`      // JSON object text or custom tool free text
	Token     *ReplayToken `json:"token,omitzero"` // call-attached replay token such as thoughtSignature
	// Unsigned reports that CallMemory observed this historical call from the
	// upstream without a signature so Gemini encoders omit placeholder tokens.
	Unsigned   bool `json:"unsigned,omitzero"`
	Incomplete bool `json:"incomplete,omitzero"` // true when stream ended before arguments completed
}

// ToolRef identifies a declared or invoked tool by kind, optional namespace, and name.
type ToolRef struct {
	Kind      ToolKind `json:"kind"`               // invoked tool category
	Namespace string   `json:"namespace,omitzero"` // owning Responses namespace when scoped
	Name      string   `json:"name"`               // bare tool name
}

// ToolResult represents the output returned for a preceding ToolCall.
type ToolResult struct {
	CallID  string  `json:"call_id"`           // correlated tool call identifier
	Tool    ToolRef `json:"tool,omitzero"`     // resolved tool identity filled by Link
	Content []Part  `json:"content"`           // text and media result parts
	IsError bool    `json:"is_error,omitzero"` // whether tool execution failed
}

// Media represents an image, audio, video, or document attachment.
type Media struct {
	Type   MediaType `json:"type"`            // attachment media modality
	MIME   string    `json:"mime,omitzero"`   // media MIME type when known
	Source string    `json:"source"`          // base64, url, or file_id encoding tag
	Data   string    `json:"data"`            // base64 payload, URL, or file ID
	Name   string    `json:"name,omitzero"`   // optional filename or document title
	Detail string    `json:"detail,omitzero"` // image detail hint such as low, high, or auto
}

// OpaqueItem preserves a complete wire item or hosted declaration that only its
// Origin protocol can express.
type OpaqueItem struct {
	Origin protocol.ProtocolID `json:"origin"` // protocol that produced Raw
	Type   string              `json:"type"`   // wire item or block type discriminator
	Raw    jsontext.Value      `json:"raw"`    // verbatim JSON encoding of the wire item
}

// Tool represents a callable function, custom grammar tool, client search tool,
// or upstream-hosted tool declaration.
type Tool struct {
	Kind         ToolKind       `json:"kind"`                   // tool declaration category
	Namespace    string         `json:"namespace,omitzero"`     // owning namespace name when scoped
	Name         string         `json:"name"`                   // declared tool identifier
	Description  string         `json:"description,omitzero"`   // human-readable tool description
	InputSchema  jsontext.Value `json:"input_schema,omitzero"`  // raw JSON Schema without float64 reparse
	Strict       *bool          `json:"strict,omitzero"`        // strict schema adherence toggle
	Format       jsontext.Value `json:"format,omitzero"`        // raw custom-tool grammar specification
	Hosted       *OpaqueItem    `json:"hosted,omitzero"`        // raw hosted-tool declaration
	DeferLoading *bool          `json:"defer_loading,omitzero"` // deferred loading hint
	Cache        *CacheMark     `json:"cache,omitzero"`         // prompt cache breakpoint
	Extra        Extra          `json:"extra,omitzero"`         // unmodeled tool members
}

// ToolChoice constrains how the model selects among declared tools.
type ToolChoice struct {
	Mode         ToolChoiceMode `json:"mode"`                   // tool selection mode
	Tool         ToolRef        `json:"tool,omitzero"`          // target tool when Mode is ToolChoiceTool
	AllowedTools []ToolRef      `json:"allowed_tools,omitzero"` // explicit subset for restricted auto/required modes
}

// Sampling groups token-sampling parameters and candidate generation counts on a Request.
type Sampling struct {
	Temperature      *float64 `json:"temperature,omitzero"`       // randomness sampling temperature
	TopP             *float64 `json:"top_p,omitzero"`             // nucleus sampling probability mass
	TopK             *int64   `json:"top_k,omitzero"`             // top-k token cutoff
	Seed             *int64   `json:"seed,omitzero"`              // deterministic sampling seed
	FrequencyPenalty *float64 `json:"frequency_penalty,omitzero"` // token frequency penalty
	PresencePenalty  *float64 `json:"presence_penalty,omitzero"`  // token presence penalty
	CandidateCount   *int64   `json:"candidate_count,omitzero"`   // requested completion candidate count
}

// ReasoningConfig captures requested thinking mode, effort level, token budget,
// summary verbosity, and encrypted continuity inclusion.
type ReasoningConfig struct {
	Mode             string `json:"mode,omitzero"`              // "", enabled, adaptive, or disabled
	Effort           string `json:"effort,omitzero"`            // "", none, minimal, low, medium, high, xhigh, or max
	BudgetTokens     *int64 `json:"budget_tokens,omitzero"`     // explicit thinking token budget
	Summary          string `json:"summary,omitzero"`           // "", auto, concise, or detailed
	IncludeEncrypted bool   `json:"include_encrypted,omitzero"` // whether encrypted reasoning was requested
}

// OutputFormat specifies structured response formatting and text verbosity.
type OutputFormat struct {
	Kind        string         `json:"kind,omitzero"`        // "", text, json_object, or json_schema
	Name        string         `json:"name,omitzero"`        // JSON schema name
	Description string         `json:"description,omitzero"` // optional JSON schema description
	Schema      jsontext.Value `json:"schema,omitzero"`      // raw JSON schema definition
	Strict      *bool          `json:"strict,omitzero"`      // strict schema enforcement flag
	Verbosity   string         `json:"verbosity,omitzero"`   // text verbosity preference
}

// IsZero reports whether e carries no unmodeled JSON object members. Returning
// true when Members is empty lets json/v2 omitzero omit Extra even when Origin is set.
func (e Extra) IsZero() bool {
	return len(e.Members) == 0
}

// Clone returns an independent copy of e with cloned raw JSON member buffers.
// Empty Members maps normalize to nil so omitzero continues omitting zero Extra
// values even when Origin was populated during decoding.
func (e Extra) Clone() Extra {
	if len(e.Members) == 0 {
		return Extra{Origin: e.Origin}
	}
	members := make(map[string]jsontext.Value, len(e.Members))
	for k, v := range e.Members {
		members[k] = cloneJSONValue(v)
	}
	return Extra{
		Origin:  e.Origin,
		Members: members,
	}
}

// Clone returns an independent deep copy of r, including nested message parts,
// tool schemas, sampling pointers, and raw Extra maps. Nil receivers return
// nil, while empty optional slices normalize to nil for omitzero serialization.
func (r *Request) Clone() *Request {
	if r == nil {
		return nil
	}
	out := *r
	out.System = cloneParts(r.System)
	if r.Messages != nil {
		out.Messages = make([]Message, len(r.Messages))
		for i, m := range r.Messages {
			out.Messages[i] = m.Clone()
		}
	}
	if len(r.Tools) > 0 {
		out.Tools = make([]Tool, len(r.Tools))
		for i, t := range r.Tools {
			out.Tools[i] = t.Clone()
		}
	} else {
		out.Tools = nil
	}
	if len(r.Namespaces) > 0 {
		out.Namespaces = make([]Namespace, len(r.Namespaces))
		for i, ns := range r.Namespaces {
			out.Namespaces[i] = ns.Clone()
		}
	} else {
		out.Namespaces = nil
	}
	if r.ToolChoice != nil {
		tc := *r.ToolChoice
		tc.AllowedTools = cloneNonEmpty(r.ToolChoice.AllowedTools)
		out.ToolChoice = &tc
	}
	out.ParallelToolCalls = cloneBoolPtr(r.ParallelToolCalls)
	out.Sampling = cloneSampling(r.Sampling)
	out.MaxOutputTokens = cloneInt64Ptr(r.MaxOutputTokens)
	out.Stop = cloneNonEmpty(r.Stop)
	if r.Reasoning != nil {
		rc := *r.Reasoning
		rc.BudgetTokens = cloneInt64Ptr(r.Reasoning.BudgetTokens)
		out.Reasoning = &rc
	}
	if r.Output != nil {
		of := *r.Output
		of.Schema = cloneJSONValue(r.Output.Schema)
		of.Strict = cloneBoolPtr(r.Output.Strict)
		out.Output = &of
	}
	out.IncludeUsage = cloneBoolPtr(r.IncludeUsage)
	out.Store = cloneBoolPtr(r.Store)
	out.Extra = r.Extra.Clone()
	return &out
}

// Clone returns an independent deep copy of m with cloned Parts and Extra members.
// Non-nil empty Parts slices stay non-nil to preserve empty turn JSON arrays,
// while nil Parts slices remain nil.
func (m Message) Clone() Message {
	out := m
	if m.Parts != nil {
		out.Parts = make([]Part, len(m.Parts))
		for i, p := range m.Parts {
			out.Parts[i] = p.Clone()
		}
	}
	out.Extra = m.Extra.Clone()
	return out
}

// Clone returns an independent deep copy of p across every union pointer branch,
// nested tool-result content slice, cache mark, and Extra map. Callers may mutate
// the returned Part without affecting earlier stream or request snapshots.
func (p Part) Clone() Part {
	out := p
	if len(p.Annotations) > 0 {
		out.Annotations = make([]Annotation, len(p.Annotations))
		for i, a := range p.Annotations {
			out.Annotations[i] = Annotation{
				Kind:       a.Kind,
				URL:        a.URL,
				Title:      a.Title,
				StartIndex: cloneInt64Ptr(a.StartIndex),
				EndIndex:   cloneInt64Ptr(a.EndIndex),
				Extra:      a.Extra.Clone(),
			}
		}
	} else {
		out.Annotations = nil
	}
	out.Token = cloneReplayToken(p.Token)
	if p.Reasoning != nil {
		rs := *p.Reasoning
		rs.Segments = cloneNonEmpty(p.Reasoning.Segments)
		rs.Token = cloneReplayToken(p.Reasoning.Token)
		out.Reasoning = &rs
	}
	if p.Call != nil {
		call := *p.Call
		call.Token = cloneReplayToken(p.Call.Token)
		out.Call = &call
	}
	if p.Result != nil {
		res := *p.Result
		if p.Result.Content != nil {
			res.Content = make([]Part, len(p.Result.Content))
			for i, child := range p.Result.Content {
				res.Content[i] = child.Clone()
			}
		}
		out.Result = &res
	}
	if p.Media != nil {
		med := *p.Media
		out.Media = &med
	}
	out.Opaque = cloneOpaqueItem(p.Opaque)
	out.Cache = cloneCacheMark(p.Cache)
	out.Extra = p.Extra.Clone()
	return out
}

// Clone returns an independent deep copy of t with cloned raw schemas, grammar
// formats, hosted item payloads, cache marks, and Extra members. Callers may
// rewrite tool fields in place without affecting the source Tool declaration.
func (t Tool) Clone() Tool {
	out := t
	out.InputSchema = cloneJSONValue(t.InputSchema)
	out.Strict = cloneBoolPtr(t.Strict)
	out.Format = cloneJSONValue(t.Format)
	out.Hosted = cloneOpaqueItem(t.Hosted)
	out.DeferLoading = cloneBoolPtr(t.DeferLoading)
	out.Cache = cloneCacheMark(t.Cache)
	out.Extra = t.Extra.Clone()
	return out
}

// Clone returns an independent deep copy of ns with cloned DeferLoading flags,
// cache breakpoints, and Extra members. Callers can modify the returned
// Namespace without mutating fields or maps on ns.
func (ns Namespace) Clone() Namespace {
	out := ns
	out.DeferLoading = cloneBoolPtr(ns.DeferLoading)
	out.Cache = cloneCacheMark(ns.Cache)
	out.Extra = ns.Extra.Clone()
	return out
}

// cloneParts deep-copies each Part in parts into a newly allocated slice.
// Empty or nil input slices normalize to nil so omitzero omits absent system
// instructions and nested tool-result part lists.
func cloneParts(parts []Part) []Part {
	if len(parts) == 0 {
		return nil
	}
	out := make([]Part, len(parts))
	for i, p := range parts {
		out[i] = p.Clone()
	}
	return out
}

// cloneReplayToken returns a detached copy of tok, or nil when tok is nil.
// Callers may rewrite Scheme, ProviderID, or Data on the returned struct without mutating tok.
func cloneReplayToken(tok *ReplayToken) *ReplayToken {
	if tok == nil {
		return nil
	}
	cp := *tok
	return &cp
}

// cloneOpaqueItem returns a detached copy of item with cloned Raw JSON bytes,
// or nil when item is nil. Modifying Raw bytes on the returned copy leaves item intact.
func cloneOpaqueItem(item *OpaqueItem) *OpaqueItem {
	if item == nil {
		return nil
	}
	return &OpaqueItem{
		Origin: item.Origin,
		Type:   item.Type,
		Raw:    cloneJSONValue(item.Raw),
	}
}

// cloneCacheMark returns a detached copy of mark with cloned Extra members, or
// nil when mark is nil. Callers can adjust TTL or Extra entries without mutating mark.
func cloneCacheMark(mark *CacheMark) *CacheMark {
	if mark == nil {
		return nil
	}
	return &CacheMark{
		TTL:   mark.TTL,
		Extra: mark.Extra.Clone(),
	}
}

// cloneSampling returns a Sampling value with freshly allocated copies of all
// non-nil pointer fields on s. Nil pointer fields remain nil on the returned Sampling value.
func cloneSampling(s Sampling) Sampling {
	return Sampling{
		Temperature:      cloneFloat64Ptr(s.Temperature),
		TopP:             cloneFloat64Ptr(s.TopP),
		TopK:             cloneInt64Ptr(s.TopK),
		Seed:             cloneInt64Ptr(s.Seed),
		FrequencyPenalty: cloneFloat64Ptr(s.FrequencyPenalty),
		PresencePenalty:  cloneFloat64Ptr(s.PresencePenalty),
		CandidateCount:   cloneInt64Ptr(s.CandidateCount),
	}
}

// cloneJSONValue copies raw JSON bytes from v into a freshly allocated buffer
// when non-empty. Empty or nil values return nil so omitzero omits absent JSON fragments.
func cloneJSONValue(v jsontext.Value) jsontext.Value {
	if len(v) == 0 {
		return nil
	}
	return jsontext.Value(bytes.Clone(v))
}

// cloneBoolPtr allocates a fresh bool initialized to *p, or returns nil when p
// is nil. Mutating the returned pointer never alters the original boolean variable.
func cloneBoolPtr(p *bool) *bool {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// cloneInt64Ptr allocates a fresh int64 initialized to *p, or returns nil when
// p is nil. Mutating the returned pointer never alters the original integer variable.
func cloneInt64Ptr(p *int64) *int64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// cloneFloat64Ptr allocates a fresh float64 initialized to *p, or returns nil
// when p is nil. Mutating the returned pointer never alters the original float64 variable.
func cloneFloat64Ptr(p *float64) *float64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// cloneNonEmpty returns a shallow copy of s when non-empty and returns nil for
// empty or nil slices. Returning nil ensures omitzero omits empty slice fields during marshaling.
func cloneNonEmpty[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	return slices.Clone(s)
}
