package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"maps"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// wire.go defines private Anthropic Messages JSON wire structures, deterministic
// JSON v2 serialization helpers, object-member surgery routines, and Extra
// extension containers for the messages codec package. Keeping wire types
// unexported inside this package isolates Anthropic block shapes, cache_control
// wrappers, and SSE frame envelopes from ir and cross-protocol callers.
//
// Wire structs backed by IR Extra containers (requests, responses, messages,
// content blocks, tool declarations, cache_control directives, citations, and
// top-level request sub-objects such as thinking, output_config, metadata, and
// tool_choice) embed a map[string]jsontext.Value fallback tagged with
// json:",embed" so unmodeled vendor extensions survive same-protocol round trips
// without float64 rounding or HTML escaping. Request and response decoders
// tolerate invalid UTF-8 inside JSON strings while keeping json/v2 duplicate-key
// rejection enabled across every nested object.

const (
	// messagesTypeMessage and messagesRoleAssistant identify the fixed envelope
	// discriminators written on outgoing Anthropic Messages responses and streams.
	messagesTypeMessage   = "message"   // top-level Anthropic response object type
	messagesRoleAssistant = "assistant" // assistant speaker role on model replies

	// cacheControlTypeEphemeral names the sole Anthropic prompt-cache breakpoint
	// type accepted on system, message, tool, and tool_result blocks.
	cacheControlTypeEphemeral = "ephemeral" // Anthropic ephemeral cache_control type
)

// Codec implements client-side and upstream-side protocol encoding and decoding
// for Anthropic Messages (protocol.ProtocolAnthropicMessages).
type Codec struct{}

// wireInboundRequest decodes an incoming Anthropic Messages JSON request body
// while retaining raw system, metadata, output_config, and speed subtrees.
type wireInboundRequest struct {
	Model          string                    `json:"model"`                     // requested model identifier
	MaxTokens      *int64                    `json:"max_tokens,omitzero"`       // maximum output token cap
	System         jsontext.Value            `json:"system,omitzero"`           // scalar string or system block array
	Messages       []wireInboundMessage      `json:"messages"`                  // ordered conversation turns
	Tools          []jsontext.Value          `json:"tools,omitzero"`            // callable and server tool declarations
	ToolChoice     jsontext.Value            `json:"tool_choice,omitzero"`      // raw tool selection policy object
	Thinking       jsontext.Value            `json:"thinking,omitzero"`         // raw extended thinking configuration
	OutputConfig   jsontext.Value            `json:"output_config,omitzero"`    // raw effort and structured format config
	Metadata       jsontext.Value            `json:"metadata,omitzero"`         // raw request metadata object
	StopSequences  []string                  `json:"stop_sequences,omitzero"`   // custom stop sequence strings
	Stream         bool                      `json:"stream,omitzero"`           // SSE streaming delivery toggle
	Temperature    *float64                  `json:"temperature,omitzero"`      // randomness sampling temperature scalar
	TopP           *float64                  `json:"top_p,omitzero"`            // nucleus sampling probability mass
	TopK           *int64                    `json:"top_k,omitzero"`            // top-k token sampling cutoff
	RawSpeed       jsontext.Value            `json:"speed,omitzero"`            // raw speed processing mode token
	PromptCacheKey string                    `json:"prompt_cache_key,omitzero"` // compatible cross-protocol cache key
	SessionID      string                    `json:"session_id,omitzero"`       // compatible explicit session identifier
	ConversationID string                    `json:"conversation_id,omitzero"`  // compatible explicit conversation identifier
	Unknown        map[string]jsontext.Value `json:",embed"`                    // unmodeled top-level request members
}

// wireOutRequest serializes a legalized outgoing Anthropic Messages request body.
type wireOutRequest struct {
	Model         string                    `json:"model"`                   // target model identifier
	MaxTokens     int64                     `json:"max_tokens"`              // required positive output token cap
	System        []jsontext.Value          `json:"system,omitzero"`         // leading system block array
	Messages      []wireOutMessage          `json:"messages"`                // alternating user and assistant turns
	Tools         []jsontext.Value          `json:"tools,omitzero"`          // serialized function and hosted tools
	ToolChoice    *wireToolChoice           `json:"tool_choice,omitzero"`    // serialized tool selection object
	Thinking      *wireThinking             `json:"thinking,omitzero"`       // resolved extended thinking config
	OutputConfig  *wireOutputConfig         `json:"output_config,omitzero"`  // resolved effort and schema format
	Metadata      *wireMetadata             `json:"metadata,omitzero"`       // nested metadata.user_id wrapper
	StopSequences []string                  `json:"stop_sequences,omitzero"` // custom stop sequence strings
	Stream        bool                      `json:"stream,omitzero"`         // SSE streaming delivery toggle
	Temperature   *float64                  `json:"temperature,omitzero"`    // randomness sampling temperature scalar
	TopP          *float64                  `json:"top_p,omitzero"`          // nucleus sampling probability mass
	TopK          *int64                    `json:"top_k,omitzero"`          // top-k token sampling cutoff
	Speed         string                    `json:"speed,omitzero"`          // "fast" when priority tier requested
	Unknown       map[string]jsontext.Value `json:",embed"`                  // preserved same-origin Extra members
}

// wireThinking represents the top-level thinking configuration on a Messages request.
type wireThinking struct {
	Type         string                    `json:"type"`                   // "enabled", "adaptive", or "disabled"
	BudgetTokens *int64                    `json:"budget_tokens,omitzero"` // positive token budget when enabled
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled thinking fields such as display
}

// wireOutputConfig represents effort and structured format controls inside output_config.
type wireOutputConfig struct {
	Effort  string                    `json:"effort,omitzero"` // "low", "medium", "high", "xhigh", or "max"
	Format  *wireOutputFormat         `json:"format,omitzero"` // optional json_schema output format
	Unknown map[string]jsontext.Value `json:",embed"`          // unmodeled output_config fields
}

// wireOutputFormat represents the nested output_config.format structured output specification.
type wireOutputFormat struct {
	Type    string                    `json:"type"`            // fixed "json_schema" format discriminator
	Schema  jsontext.Value            `json:"schema,omitzero"` // raw JSON Schema object
	Unknown map[string]jsontext.Value `json:",embed"`          // unmodeled output_config.format fields
}

// wireMetadata represents the top-level metadata object carrying user_id on Messages requests.
type wireMetadata struct {
	UserID  string                    `json:"user_id,omitzero"` // caller end-user or device/session identifier
	Unknown map[string]jsontext.Value `json:",embed"`           // unmodeled metadata entries
}

// wireCacheControl represents an Anthropic prompt-cache breakpoint directive object.
type wireCacheControl struct {
	Type    string                    `json:"type"`         // fixed "ephemeral" cache type
	TTL     string                    `json:"ttl,omitzero"` // optional "", "5m", or "1h" lifetime
	Unknown map[string]jsontext.Value `json:",embed"`       // unmodeled cache_control members
}

// wireInboundMessage decodes one message turn inside an Anthropic Messages request.
type wireInboundMessage struct {
	Role    string                    `json:"role"`             // "user", "assistant", or compatible "system"
	Content jsontext.Value            `json:"content,omitzero"` // scalar string or content block array
	Unknown map[string]jsontext.Value `json:",embed"`           // unmodeled message-level members
}

// wireOutMessage serializes one legalized turn inside an outgoing Anthropic Messages request.
type wireOutMessage struct {
	Role    string                    `json:"role"`    // "user" or "assistant"
	Content []jsontext.Value          `json:"content"` // non-empty serialized content block list
	Unknown map[string]jsontext.Value `json:",embed"`  // preserved same-origin Extra members
}

// wireMediaSource represents the nested source object on an Anthropic image or document block.
type wireMediaSource struct {
	Type      string `json:"type"`                // "base64", "url", or "file"
	MediaType string `json:"media_type,omitzero"` // MIME type for base64 sources
	Data      string `json:"data,omitzero"`       // base64 payload when type is "base64"
	URL       string `json:"url,omitzero"`        // remote URL when type is "url"
	FileID    string `json:"file_id,omitzero"`    // uploaded file ID when type is "file"
}

// wireTextBlock decodes or serializes a Messages "text" content block on requests.
// Leaving citations undeclared on request blocks routes citations into Unknown
// so Part.Extra preserves them verbatim per section 4.1.
type wireTextBlock struct {
	Type         string                    `json:"type"`                   // fixed "text" block discriminator
	Text         string                    `json:"text"`                   // required text string, including "" on skeletons
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled text-block members including citations
}

// wireMediaBlock decodes or serializes a Messages "image" or "document" content block.
// Leaving document title, context, and citations undeclared routes them into
// Unknown on decode while serializeMessagesMediaBlock supplies cross-protocol filenames.
type wireMediaBlock struct {
	Type         string                    `json:"type"`                   // "image" or "document" block discriminator
	Source       *wireMediaSource          `json:"source"`                 // attachment payload descriptor
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled media members (title, context, citations)
}

// wireThinkingBlock decodes or serializes a Messages "thinking" content block.
type wireThinkingBlock struct {
	Type         string                    `json:"type"`                   // fixed "thinking" block discriminator
	Thinking     string                    `json:"thinking"`               // visible reasoning text, including ""
	Signature    string                    `json:"signature,omitzero"`     // cryptographic continuity token or carrier
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled thinking-block members
}

// wireRedactedThinkingBlock decodes or serializes a Messages "redacted_thinking" block.
type wireRedactedThinkingBlock struct {
	Type         string                    `json:"type"`                   // fixed "redacted_thinking" block discriminator
	Data         string                    `json:"data"`                   // opaque encrypted reasoning payload
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled redacted_thinking members
}

// wireToolUseBlock decodes or serializes a Messages "tool_use" content block.
type wireToolUseBlock struct {
	Type         string                    `json:"type"`                   // fixed "tool_use" block discriminator
	ID           string                    `json:"id"`                     // call correlation identifier
	Name         string                    `json:"name"`                   // invoked function tool name
	Input        jsontext.Value            `json:"input"`                  // JSON object arguments
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled tool_use members
}

// wireToolResultBlock decodes or serializes a Messages "tool_result" content block.
type wireToolResultBlock struct {
	Type         string                    `json:"type"`                   // fixed "tool_result" block discriminator
	ToolUseID    string                    `json:"tool_use_id"`            // correlated assistant tool_use ID
	Content      jsontext.Value            `json:"content,omitzero"`       // scalar string or inner text/media array
	IsError      bool                      `json:"is_error,omitzero"`      // true when tool execution failed
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled tool_result members
}

// wireInboundTool decodes one entry from the top-level Messages tools array.
type wireInboundTool struct {
	Type         string                    `json:"type,omitzero"`          // "", "custom", or hosted server tool type
	Name         string                    `json:"name,omitzero"`          // declared tool identifier
	Description  string                    `json:"description,omitzero"`   // human-readable tool description
	InputSchema  jsontext.Value            `json:"input_schema,omitzero"`  // raw JSON Schema object
	Strict       *bool                     `json:"strict,omitzero"`        // strict schema enforcement flag
	DeferLoading *bool                     `json:"defer_loading,omitzero"` // deferred tool loading flag
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled tool declaration members
}

// wireToolWrite serializes a callable function tool entry in an outgoing Messages request.
type wireToolWrite struct {
	Name         string                    `json:"name"`                   // declared callable tool identifier
	Description  string                    `json:"description,omitzero"`   // human-readable tool description
	InputSchema  jsontext.Value            `json:"input_schema"`           // required JSON Schema object
	Strict       *bool                     `json:"strict,omitzero"`        // strict schema enforcement flag
	DeferLoading *bool                     `json:"defer_loading,omitzero"` // deferred tool loading flag
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // preserved same-origin Extra members
}

// wireToolChoice decodes or serializes the Messages tool_choice selector object.
type wireToolChoice struct {
	Type                   string                    `json:"type"`                               // "auto", "any", "tool", or "none"
	Name                   string                    `json:"name,omitzero"`                      // target tool name when type is "tool"
	DisableParallelToolUse *bool                     `json:"disable_parallel_tool_use,omitzero"` // inverted ParallelToolCalls flag
	Unknown                map[string]jsontext.Value `json:",embed"`                             // unmodeled tool_choice fields
}

// wireCitation decodes one citation entry attached to a response text block or citations_delta.
type wireCitation struct {
	Type           string                    `json:"type"`                      // citation location discriminator
	URL            string                    `json:"url,omitzero"`              // cited web URL when applicable
	Title          string                    `json:"title,omitzero"`            // cited document or page title
	StartCharIndex *int64                    `json:"start_char_index,omitzero"` // optional character start offset
	EndCharIndex   *int64                    `json:"end_char_index,omitzero"`   // optional character end offset
	Unknown        map[string]jsontext.Value `json:",embed"`                    // unmodeled citation fields
}

// wireResponseTextBlock decodes a "text" block on a buffered Messages response or stream start.
type wireResponseTextBlock struct {
	Type         string                    `json:"type"`                   // fixed "text" block discriminator
	Text         string                    `json:"text"`                   // visible assistant text payload
	Citations    []wireCitation            `json:"citations,omitzero"`     // structured grounding citations
	CacheControl *wireCacheControl         `json:"cache_control,omitzero"` // optional prompt cache breakpoint
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled response text-block members
}

// wireResponse decodes a non-streaming Anthropic Messages JSON response body.
type wireResponse struct {
	ID           string                    `json:"id"`                     // response message identifier
	Type         string                    `json:"type"`                   // "message" or "error"
	Role         string                    `json:"role,omitzero"`          // fixed "assistant" speaker role
	Model        string                    `json:"model,omitzero"`         // upstream model identifier
	Content      []jsontext.Value          `json:"content,omitzero"`       // ordered assistant content blocks
	StopReason   *string                   `json:"stop_reason,omitzero"`   // completion stop reason or null
	StopSequence *string                   `json:"stop_sequence,omitzero"` // matched stop sequence or null
	Usage        jsontext.Value            `json:"usage,omitzero"`         // raw usage object for wirejson validation
	Error        jsontext.Value            `json:"error,omitzero"`         // error object when type is "error"
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled top-level response members
}

// wireResponseWrite serializes an outgoing non-streaming Anthropic Messages JSON response.
type wireResponseWrite struct {
	ID           string                    `json:"id"`            // client-facing message identifier
	Type         string                    `json:"type"`          // fixed "message" response discriminator
	Role         string                    `json:"role"`          // fixed "assistant" speaker role
	Model        string                    `json:"model"`         // presented client model identifier
	Content      []jsontext.Value          `json:"content"`       // ordered assistant content blocks
	StopReason   *string                   `json:"stop_reason"`   // non-null stop reason on completed reply
	StopSequence *string                   `json:"stop_sequence"` // matched stop sequence or explicit null
	Usage        wireUsageWrite            `json:"usage"`         // disjoint Messages token usage counters
	Unknown      map[string]jsontext.Value `json:",embed"`        // preserved same-origin Extra members
}

// wireStreamEvent decodes one incoming Anthropic Messages SSE event JSON object.
type wireStreamEvent struct {
	Type         string            `json:"type,omitzero"`          // SSE event type discriminator
	Message      *wireMessageStart `json:"message,omitzero"`       // envelope payload on message_start
	Index        *int              `json:"index,omitzero"`         // zero-based block index on block events
	ContentBlock jsontext.Value    `json:"content_block,omitzero"` // initial block object on content_block_start
	Delta        *wireStreamDelta  `json:"delta,omitzero"`         // incremental payload on *_delta events
	Usage        jsontext.Value    `json:"usage,omitzero"`         // raw usage object on message_delta
	Error        jsontext.Value    `json:"error,omitzero"`         // raw error payload on error events
}

// wireMessageStart decodes the nested message envelope on a message_start SSE event.
type wireMessageStart struct {
	ID    string         `json:"id,omitzero"`    // upstream response message identifier
	Model string         `json:"model,omitzero"` // upstream model identifier
	Usage jsontext.Value `json:"usage,omitzero"` // initial raw usage object
}

// wireStreamDelta decodes incremental fields on content_block_delta and message_delta events.
type wireStreamDelta struct {
	Type         string         `json:"type,omitzero"`          // delta discriminator on content_block_delta
	Text         string         `json:"text,omitzero"`          // fragment on text_delta
	Thinking     string         `json:"thinking,omitzero"`      // fragment on thinking_delta
	Signature    string         `json:"signature,omitzero"`     // fragment on signature_delta
	PartialJSON  string         `json:"partial_json,omitzero"`  // fragment on input_json_delta
	Citation     jsontext.Value `json:"citation,omitzero"`      // single citation object on citations_delta
	StopReason   string         `json:"stop_reason,omitzero"`   // terminal reason on message_delta
	StopSequence *string        `json:"stop_sequence,omitzero"` // matched stop string on message_delta
}

// wireMessageStartWrite serializes a client-facing message_start SSE frame payload.
type wireMessageStartWrite struct {
	Type    string                    `json:"type"`    // fixed "message_start" event discriminator
	Message wireMessageStartBodyWrite `json:"message"` // initial assistant message envelope
}

// wireMessageStartBodyWrite serializes the nested message object inside message_start.
type wireMessageStartBodyWrite struct {
	ID           string           `json:"id"`            // response message identifier
	Type         string           `json:"type"`          // fixed "message" envelope discriminator
	Role         string           `json:"role"`          // fixed "assistant" speaker role
	Model        string           `json:"model"`         // presented client model identifier
	Content      []jsontext.Value `json:"content"`       // empty array [] on message_start
	StopReason   *string          `json:"stop_reason"`   // explicit null on message_start
	StopSequence *string          `json:"stop_sequence"` // explicit null on message_start
	Usage        wireUsageWrite   `json:"usage"`         // initial usage counters and optional speed
}

// wireContentBlockStartWrite serializes a client-facing content_block_start SSE frame payload.
type wireContentBlockStartWrite struct {
	Type         string         `json:"type"`          // fixed "content_block_start" event discriminator
	Index        int            `json:"index"`         // contiguous zero-based block index
	ContentBlock jsontext.Value `json:"content_block"` // serialized initial content block object
}

// wireContentBlockDeltaWrite serializes a client-facing content_block_delta SSE frame payload.
type wireContentBlockDeltaWrite struct {
	Type  string         `json:"type"`  // fixed "content_block_delta" event discriminator
	Index int            `json:"index"` // open content block index
	Delta wireDeltaWrite `json:"delta"` // typed incremental fragment
}

// wireDeltaWrite serializes incremental text, thinking, signature, or partial_json payloads.
type wireDeltaWrite struct {
	Type        string  `json:"type"`                  // delta type discriminator
	Text        *string `json:"text,omitzero"`         // present on text_delta
	Thinking    *string `json:"thinking,omitzero"`     // present on thinking_delta
	Signature   string  `json:"signature,omitzero"`    // non-empty on signature_delta
	PartialJSON *string `json:"partial_json,omitzero"` // present on input_json_delta
}

// wireContentBlockStopWrite serializes a client-facing content_block_stop SSE frame payload.
type wireContentBlockStopWrite struct {
	Type  string `json:"type"`  // fixed "content_block_stop" event discriminator
	Index int    `json:"index"` // closed content block index
}

// wireMessageDeltaWrite serializes a client-facing message_delta SSE frame payload.
type wireMessageDeltaWrite struct {
	Type  string                    `json:"type"`  // fixed "message_delta" event discriminator
	Delta wireMessageDeltaBodyWrite `json:"delta"` // terminal stop_reason and stop_sequence
	Usage wireUsageWrite            `json:"usage"` // cumulative token usage snapshot
}

// wireMessageDeltaBodyWrite serializes terminal stop_reason and stop_sequence fields on message_delta.
type wireMessageDeltaBodyWrite struct {
	StopReason   string  `json:"stop_reason"`   // non-empty Messages stop_reason
	StopSequence *string `json:"stop_sequence"` // matched stop sequence or explicit null
}

// wireMessageStopWrite serializes the terminal message_stop SSE frame payload.
type wireMessageStopWrite struct {
	Type string `json:"type"` // fixed "message_stop" event discriminator
}

// wireTypeProbe extracts only the string "type" discriminator from a raw JSON object.
type wireTypeProbe struct {
	Type string `json:"type,omitzero"` // block or item type discriminator
}

// marshalDeterministic encodes v with sorted map keys so identical IR inputs
// produce byte-for-byte identical JSON outputs. Invalid UTF-8 sequences inside
// caller strings survive serialization without error.
func marshalDeterministic(v any) ([]byte, error) {
	return json.Marshal(v, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
}

// unmarshalJSONObject decodes data into dst after checking that data begins
// with a top-level '{' brace and contains a single valid JSON object without
// duplicate keys. Invalid UTF-8 inside JSON string values is tolerated.
func unmarshalJSONObject(data []byte, dst any) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return errors.New("empty JSON payload")
	}
	if trimmed[0] != '{' {
		return errors.New("JSON payload must be an object")
	}
	return json.Unmarshal(trimmed, dst, jsontext.AllowInvalidUTF8(true))
}

// unmarshalJSONValue decodes raw into dst using json/v2 while tolerating
// invalid UTF-8 sequences inside JSON string values. Duplicate object member
// names, syntax errors, or trailing tokens return an error.
func unmarshalJSONValue(raw []byte, dst any) error {
	return json.Unmarshal(raw, dst, jsontext.AllowInvalidUTF8(true))
}

// isValidSingleJSON reports whether raw encodes exactly one syntactically valid
// JSON value with no duplicate object keys and no trailing tokens.
func isValidSingleJSON(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	if _, err := dec.ReadValue(); err != nil {
		return false
	}
	_, err := dec.ReadToken()
	return err == io.EOF
}

// isValidSingleJSONObject reports whether raw encodes one syntactically valid
// JSON object beginning with '{' and containing no duplicate object keys.
func isValidSingleJSONObject(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '{' && isValidSingleJSON(trimmed)
}

// compactRawJSON normalizes raw JSON bytes through json/v2's value encoder
// without reparsing numbers into float64 or reordering object member keys.
// Normalized bytes match post-marshal output on fixed-point round trips even
// when input strings contained invalid UTF-8 or non-canonical escapes.
func compactRawJSON(raw jsontext.Value) jsontext.Value {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil
	}
	v := jsontext.Value(bytes.Clone(trimmed))
	// Inputs come from json/v2 decoding; if re-marshaling ever rejects a fragment,
	// retaining the cloned trimmed bytes preserves the original wire value.
	normalized, err := marshalDeterministic(v)
	if err != nil {
		return v
	}
	return jsontext.Value(normalized)
}

// replaceJSONObjectMember returns a compacted copy of rawObj with top-level
// member key replaced by newVal, or appends key at the end of the object when
// absent. Non-object inputs or malformed JSON tokens return an error.
//
// Streaming server_tool_use blocks arrive with an empty input:{} placeholder on
// content_block_start and accumulate input_json_delta fragments across later
// frames. Rewriting the object token-by-token through jsontext patches input
// without converting sibling 64-bit numbers into float64 or disturbing member order.
func replaceJSONObjectMember(rawObj jsontext.Value, key string, newVal jsontext.Value) (jsontext.Value, error) {
	dec := jsontext.NewDecoder(bytes.NewReader(bytes.TrimSpace(rawObj)), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return nil, errors.New("target JSON value is not an object")
	}

	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := enc.WriteToken(jsontext.BeginObject); err != nil {
		return nil, err
	}

	replaced := false
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil {
			return nil, err
		}
		memberKey := kTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return nil, err
		}
		if err := enc.WriteToken(jsontext.String(memberKey)); err != nil {
			return nil, err
		}
		if memberKey == key {
			if err := enc.WriteValue(compactRawJSON(newVal)); err != nil {
				return nil, err
			}
			replaced = true
			continue
		}
		if err := enc.WriteValue(val); err != nil {
			return nil, err
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, err
	}
	if !replaced {
		if err := enc.WriteToken(jsontext.String(key)); err != nil {
			return nil, err
		}
		if err := enc.WriteValue(compactRawJSON(newVal)); err != nil {
			return nil, err
		}
	}
	if err := enc.WriteToken(jsontext.EndObject); err != nil {
		return nil, err
	}
	return compactRawJSON(jsontext.Value(buf.Bytes())), nil
}

// cloneValueMap returns a shallow map copy of src so callers can delete or add
// keys without mutating decoded Unknown maps. Empty or nil maps return nil so
// structs without extensions avoid allocating empty map headers.
func cloneValueMap(src map[string]jsontext.Value) map[string]jsontext.Value {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]jsontext.Value, len(src))
	maps.Copy(out, src)
	return out
}

// insertRawMember writes a compacted copy of raw under key in dst and returns
// the updated map. Nil destination maps are allocated automatically so callers
// can chain insertions without pre-allocating an Unknown map.
func insertRawMember(dst map[string]jsontext.Value, key string, raw jsontext.Value) map[string]jsontext.Value {
	if dst == nil {
		dst = make(map[string]jsontext.Value, 1)
	}
	dst[key] = compactRawJSON(raw)
	return dst
}

// marshalUnknownMap serializes a non-empty unknown-member map into deterministic
// JSON object bytes for storage in Request.Extra. Empty or nil maps return (nil, nil)
// so modeled sub-objects without extra fields leave Request.Extra untouched.
func marshalUnknownMap(m map[string]jsontext.Value) (jsontext.Value, error) {
	if len(m) == 0 {
		return nil, nil
	}
	normalized := make(map[string]jsontext.Value, len(m))
	for k, v := range m {
		normalized[k] = compactRawJSON(v)
	}
	raw, err := marshalDeterministic(normalized)
	if err != nil {
		return nil, err
	}
	return jsontext.Value(raw), nil
}

// extractSubObjectUnknown parses a preserved JSON object stored under key in
// topUnknown and deletes modeled stripKeys from the resulting map. Missing keys,
// non-object values, or maps left empty after stripping return nil.
func extractSubObjectUnknown(
	topUnknown map[string]jsontext.Value,
	key string,
	stripKeys ...string,
) map[string]jsontext.Value {
	if len(topUnknown) == 0 {
		return nil
	}
	raw, ok := topUnknown[key]
	if !ok {
		return nil
	}
	var parsed map[string]jsontext.Value
	// Ignore non-object values in topUnknown[key] so scalar vendor overrides
	// cannot corrupt typed sub-object serialization.
	if unmarshalJSONObject(raw, &parsed) != nil || len(parsed) == 0 {
		return nil
	}
	for _, sk := range stripKeys {
		delete(parsed, sk)
	}
	if len(parsed) == 0 {
		return nil
	}
	return parsed
}

// toExtra wraps unknown JSON object members decoded from an Anthropic Messages
// wire struct into an ir.Extra tagged with ProtocolAnthropicMessages. Empty
// maps return a zero ir.Extra so structs without extensions stay comparable.
func toExtra(unknown map[string]jsontext.Value) ir.Extra {
	if len(unknown) == 0 {
		return ir.Extra{}
	}
	cloned := make(map[string]jsontext.Value, len(unknown))
	for k, v := range unknown {
		cloned[k] = compactRawJSON(v)
	}
	return ir.Extra{
		Origin:  protocol.ProtocolAnthropicMessages,
		Members: cloned,
	}
}

// restoreOrDropExtra returns deep-copied Extra members when extra originated
// from Anthropic Messages. Non-zero Extra maps from other protocols are omitted
// and recorded as an extra_dropped Loss at path.
func restoreOrDropExtra(extra ir.Extra, path string, losses *[]ir.Loss) map[string]jsontext.Value {
	if extra.IsZero() {
		return nil
	}
	if extra.Origin == protocol.ProtocolAnthropicMessages {
		out := make(map[string]jsontext.Value, len(extra.Members))
		maps.Copy(out, extra.Members)
		for k, v := range out {
			out[k] = jsontext.Value(bytes.Clone(v))
		}
		return out
	}
	*losses = append(*losses, ir.Loss{
		Kind: ir.LossExtraDropped,
		Path: path,
	})
	return nil
}

// decodeCacheControl converts an optional Anthropic wireCacheControl into an
// ir.CacheMark. Non-"ephemeral" types or unrecognized TTL values return an error
// so decoded requests always satisfy ir.Validate and Law Q2 round-trip invariants.
func decodeCacheControl(cc *wireCacheControl) (*ir.CacheMark, error) {
	if cc == nil {
		return nil, nil
	}
	typ := strings.TrimSpace(cc.Type)
	if typ != cacheControlTypeEphemeral {
		return nil, fmt.Errorf("unsupported cache_control type %q", cc.Type)
	}
	switch cc.TTL {
	case "", "5m", "1h":
	default:
		return nil, fmt.Errorf("unsupported cache_control ttl %q", cc.TTL)
	}
	return &ir.CacheMark{
		TTL:   cc.TTL,
		Extra: toExtra(cc.Unknown),
	}, nil
}

// encodeCacheControl converts an optional ir.CacheMark back into a Messages
// wireCacheControl object with type="ephemeral". Nil marks return nil, while
// foreign CacheMark.Extra maps are omitted and record LossExtraDropped at path.
func encodeCacheControl(mark *ir.CacheMark, path string, losses *[]ir.Loss) *wireCacheControl {
	if mark == nil {
		return nil
	}
	return &wireCacheControl{
		Type:    cacheControlTypeEphemeral,
		TTL:     mark.TTL,
		Unknown: restoreOrDropExtra(mark.Extra, path, losses),
	}
}

// cloneCacheMarkPtr returns an independent heap copy of mark with deep-cloned
// Extra members so stream start and end events never share mutable map state.
// Nil input pointers return nil.
func cloneCacheMarkPtr(mark *ir.CacheMark) *ir.CacheMark {
	if mark == nil {
		return nil
	}
	return &ir.CacheMark{
		TTL:   mark.TTL,
		Extra: mark.Extra.Clone(),
	}
}

// decodeCitations converts wireCitation items into normalized ir.Annotation
// values while preserving unmodeled citation fields in Annotation.Extra. Empty
// slices return nil, and negative or inverted character offsets are omitted.
func decodeCitations(raw []wireCitation) []ir.Annotation {
	if len(raw) == 0 {
		return nil
	}
	out := make([]ir.Annotation, len(raw))
	for i, c := range raw {
		var startIdx, endIdx *int64
		// Drop negative or inverted character span bounds from compatible providers
		// so decoded Annotation spans always satisfy ir.ValidateResponse.
		if c.StartCharIndex != nil && *c.StartCharIndex >= 0 {
			v := *c.StartCharIndex
			startIdx = &v
		}
		if c.EndCharIndex != nil && *c.EndCharIndex >= 0 && (startIdx == nil || *c.EndCharIndex >= *startIdx) {
			v := *c.EndCharIndex
			endIdx = &v
		}
		out[i] = ir.Annotation{
			Kind:       c.Type,
			URL:        c.URL,
			Title:      c.Title,
			StartIndex: startIdx,
			EndIndex:   endIdx,
			Extra:      toExtra(c.Unknown),
		}
	}
	return out
}
