package chat

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

// wire.go defines private OpenAI Chat Completions JSON wire structures,
// deterministic JSON v2 encoding helpers, data-URI media parsers, and Extra
// member preservation containers for the chat codec package. Keeping wire
// schemas private to this package prevents protocol-specific JSON shapes from
// leaking into ir or cross-protocol callers.
//
// Containers backed by IR Extra fields (top-level requests and responses,
// messages, content parts, tool declarations, tool calls, and annotations)
// embed a map[string]jsontext.Value fallback tagged with json:",embed" so
// unmodeled vendor extensions survive same-protocol round trips without float64
// numeric coercion or HTML escaping, while nested sub-objects without IR Extra
// containers decode only their modeled Chat fields. Request and response
// decoders accept invalid UTF-8 inside JSON strings while keeping json/v2
// duplicate-key rejection enabled across every nested object, and
// restoreOrDropExtra writes unknown members back only when Extra.Origin matches
// OpenAI Chat Completions.

const (
	// chatObjectCompletion and chatObjectChunk identify the canonical OpenAI
	// object discriminators written on buffered and streaming Chat responses.
	chatObjectCompletion = "chat.completion"       // non-streaming completion object tag
	chatObjectChunk      = "chat.completion.chunk" // streaming chunk object tag

	// defaultResponseSchemaName supplies a valid identifier when a cross-protocol
	// json_schema OutputFormat arrives without a schema name.
	defaultResponseSchemaName = "response_schema" // fallback json_schema wrapper name

	// toolOutputMediaMarker and toolMediaAttributionFmt pair the placeholder left
	// inside role="tool" messages with the call-attributed user text part
	// prepended ahead of relocated tool-result media attachments.
	toolOutputMediaMarker   = "[Tool output media moved to the following user message]" // tool-side relocation marker
	toolMediaAttributionFmt = "[Tool output media for call %s]"                         // user-side call attribution marker
)

// Codec implements client-side and upstream-side protocol encoding and decoding
// for OpenAI Chat Completions (protocol.ProtocolOpenAIChatCompletions).
type Codec struct{}

// wireInboundRequest decodes a Chat Completions request while keeping raw tool,
// stream_options, and metadata bytes available for lossless preservation.
type wireInboundRequest struct {
	Model               string                    `json:"model"`                          // target model name identifier
	Messages            []wireInboundMessage      `json:"messages"`                       // ordered conversation message turns
	Tools               []jsontext.Value          `json:"tools,omitzero"`                 // raw tool declaration objects
	Functions           []wireLegacyFunction      `json:"functions,omitzero"`             // legacy top-level function declarations
	ToolChoice          jsontext.Value            `json:"tool_choice,omitzero"`           // string or object tool selector
	FunctionCall        jsontext.Value            `json:"function_call,omitzero"`         // legacy function_call selector value
	ParallelToolCalls   *bool                     `json:"parallel_tool_calls,omitzero"`   // explicit parallel tool-call toggle
	MaxCompletionTokens *int64                    `json:"max_completion_tokens,omitzero"` // preferred output token cap
	MaxTokens           *int64                    `json:"max_tokens,omitzero"`            // legacy output token cap
	Temperature         *float64                  `json:"temperature,omitzero"`           // randomness sampling temperature scalar
	TopP                *float64                  `json:"top_p,omitzero"`                 // nucleus sampling probability mass
	Seed                *int64                    `json:"seed,omitzero"`                  // deterministic sampling seed integer
	FrequencyPenalty    *float64                  `json:"frequency_penalty,omitzero"`     // token frequency repetition penalty
	PresencePenalty     *float64                  `json:"presence_penalty,omitzero"`      // token presence repetition penalty
	N                   *int64                    `json:"n,omitzero"`                     // requested completion candidate count
	Stop                jsontext.Value            `json:"stop,omitzero"`                  // string or array stop sequences
	ReasoningEffort     string                    `json:"reasoning_effort,omitzero"`      // top-level reasoning effort label
	Reasoning           *wireReasoning            `json:"reasoning,omitzero"`             // nested reasoning effort configuration
	Thinking            jsontext.Value            `json:"thinking,omitzero"`              // compatible vendor thinking mode/budget object
	ResponseFormat      *wireResponseFormat       `json:"response_format,omitzero"`       // structured output format object
	Stream              bool                      `json:"stream,omitzero"`                // SSE streaming delivery toggle
	StreamOptions       jsontext.Value            `json:"stream_options,omitzero"`        // raw stream_options object value
	ServiceTier         string                    `json:"service_tier,omitzero"`          // requested processing service tier
	PromptCacheKey      string                    `json:"prompt_cache_key,omitzero"`      // prompt cache routing key
	User                string                    `json:"user,omitzero"`                  // caller end-user account identifier
	Metadata            jsontext.Value            `json:"metadata,omitzero"`              // raw request metadata object value
	SessionID           string                    `json:"session_id,omitzero"`            // explicit session continuity identifier
	ConversationID      string                    `json:"conversation_id,omitzero"`       // explicit conversation continuity identifier
	Store               *bool                     `json:"store,omitzero"`                 // upstream completion storage toggle
	Unknown             map[string]jsontext.Value `json:",embed"`                         // unmodeled top-level request members
}

// wireOutRequest serializes a normalized OpenAI Chat Completions request body.
type wireOutRequest struct {
	Model               string                    `json:"model"`                          // target model name identifier
	Messages            []wireOutMessage          `json:"messages"`                       // legalized outgoing conversation turns
	Tools               []jsontext.Value          `json:"tools,omitzero"`                 // serialized tool declaration objects
	ToolChoice          jsontext.Value            `json:"tool_choice,omitzero"`           // serialized tool selection rule
	ParallelToolCalls   *bool                     `json:"parallel_tool_calls,omitzero"`   // explicit parallel tool-call toggle
	MaxCompletionTokens *int64                    `json:"max_completion_tokens,omitzero"` // maximum generated completion tokens
	Temperature         *float64                  `json:"temperature,omitzero"`           // randomness sampling temperature scalar
	TopP                *float64                  `json:"top_p,omitzero"`                 // nucleus sampling probability mass
	Seed                *int64                    `json:"seed,omitzero"`                  // deterministic sampling seed integer
	FrequencyPenalty    *float64                  `json:"frequency_penalty,omitzero"`     // token frequency repetition penalty
	PresencePenalty     *float64                  `json:"presence_penalty,omitzero"`      // token presence repetition penalty
	N                   *int64                    `json:"n,omitzero"`                     // requested completion candidate count
	Stop                []string                  `json:"stop,omitzero"`                  // custom stop sequence strings
	ReasoningEffort     string                    `json:"reasoning_effort,omitzero"`      // normalized reasoning effort label
	ResponseFormat      *wireResponseFormat       `json:"response_format,omitzero"`       // structured output format object
	Stream              bool                      `json:"stream,omitzero"`                // SSE streaming delivery toggle
	StreamOptions       *wireStreamOptions        `json:"stream_options,omitzero"`        // streaming usage reporting options
	ServiceTier         string                    `json:"service_tier,omitzero"`          // requested processing service tier
	PromptCacheKey      string                    `json:"prompt_cache_key,omitzero"`      // prompt cache routing key
	User                string                    `json:"user,omitzero"`                  // caller end-user account identifier
	Metadata            *wireMetadata             `json:"metadata,omitzero"`              // nested metadata.user_id wrapper object
	SessionID           string                    `json:"session_id,omitzero"`            // explicit session continuity identifier
	ConversationID      string                    `json:"conversation_id,omitzero"`       // explicit conversation continuity identifier
	Store               *bool                     `json:"store,omitzero"`                 // upstream completion storage toggle
	Unknown             map[string]jsontext.Value `json:",embed"`                         // preserved same-origin Extra members
}

// wireStreamOptions represents streaming usage reporting controls inside the Chat stream_options request object.
type wireStreamOptions struct {
	IncludeUsage *bool `json:"include_usage,omitzero"` // emit final usage-only chunk
}

// wireMetadata represents Chat request metadata carrying an optional nested user_id string.
type wireMetadata struct {
	UserID string `json:"user_id,omitzero"` // nested metadata user identifier
}

// wireReasoning represents the compatible nested reasoning effort object on Chat requests.
type wireReasoning struct {
	Effort string `json:"effort,omitzero"` // nested reasoning effort label
}

// wireCompatThinking decodes optional vendor thinking controls supplied on
// compatible Chat Completions endpoints while keeping raw bytes in Extra.
type wireCompatThinking struct {
	Type         string                    `json:"type,omitzero"`          // "", enabled, adaptive, disabled, none, or off
	BudgetTokens *int64                    `json:"budget_tokens,omitzero"` // optional thinking token budget
	Unknown      map[string]jsontext.Value `json:",embed"`                 // additional vendor thinking members
}

// wireResponseFormat represents structured output mode and JSON Schema settings inside Chat response_format.
type wireResponseFormat struct {
	Type       string              `json:"type"`                 // text, json_object, or json_schema
	JSONSchema *wireJSONSchemaSpec `json:"json_schema,omitzero"` // nested schema wrapper when type is json_schema
}

// wireJSONSchemaSpec represents the nested json_schema definition inside Chat response_format.
type wireJSONSchemaSpec struct {
	Name        string         `json:"name"`                 // structured output schema identifier
	Description string         `json:"description,omitzero"` // optional human-readable schema description
	Schema      jsontext.Value `json:"schema,omitzero"`      // raw JSON Schema definition object
	Strict      *bool          `json:"strict,omitzero"`      // strict schema adherence toggle
}

// wireInboundMessage decodes one message entry in a Chat Completions request or buffered response.
type wireInboundMessage struct {
	Role             string                    `json:"role"`                       // message speaker role label
	Content          jsontext.Value            `json:"content,omitzero"`           // string, part array, or explicit null
	ReasoningContent *string                   `json:"reasoning_content,omitzero"` // primary plaintext reasoning field
	Reasoning        *string                   `json:"reasoning,omitzero"`         // compatible fallback reasoning alias
	Refusal          string                    `json:"refusal,omitzero"`           // assistant safety refusal text
	RawName          jsontext.Value            `json:"name,omitzero"`              // legacy function name or participant name
	ToolCalls        []wireToolCall            `json:"tool_calls,omitzero"`        // assistant tool call invocations
	FunctionCall     *wireFunctionCall         `json:"function_call,omitzero"`     // legacy single function call
	ToolCallID       string                    `json:"tool_call_id,omitzero"`      // correlated call ID on role="tool"
	Annotations      []wireAnnotation          `json:"annotations,omitzero"`       // citation annotations on assistant messages
	Unknown          map[string]jsontext.Value `json:",embed"`                     // unmodeled wire message members
}

// wireOutMessage serializes one legalized message entry in an outgoing Chat Completions request.
type wireOutMessage struct {
	Role             string                    `json:"role"`                       // message speaker role label
	Content          jsontext.Value            `json:"content,omitzero"`           // string, part array, or explicit null
	ReasoningContent *string                   `json:"reasoning_content,omitzero"` // optional plaintext reasoning text
	Refusal          string                    `json:"refusal,omitzero"`           // optional assistant refusal text
	ToolCalls        []wireToolCall            `json:"tool_calls,omitzero"`        // paired assistant tool calls
	ToolCallID       string                    `json:"tool_call_id,omitzero"`      // correlated call ID on role="tool"
	Unknown          map[string]jsontext.Value `json:",embed"`                     // preserved same-origin Extra members
}

// wireContentPart represents one typed element inside an array-valued Chat message content field.
type wireContentPart struct {
	Type        string                    `json:"type"`                 // text, image_url, input_audio, file, or refusal
	Text        string                    `json:"text,omitzero"`        // text payload when type is text
	Refusal     string                    `json:"refusal,omitzero"`     // refusal payload when type is refusal
	ImageURL    jsontext.Value            `json:"image_url,omitzero"`   // object or string URL when type is image_url
	InputAudio  *wireInputAudio           `json:"input_audio,omitzero"` // audio payload when type is input_audio
	File        *wireFile                 `json:"file,omitzero"`        // document payload when type is file
	Annotations []wireAnnotation          `json:"annotations,omitzero"` // optional text-part citation annotations
	Unknown     map[string]jsontext.Value `json:",embed"`               // unmodeled content-part members
}

// wireImageURL represents the nested image_url object on a Chat image content part.
type wireImageURL struct {
	URL    string `json:"url"`             // remote HTTPS URL or data URI
	Detail string `json:"detail,omitzero"` // optional resolution detail hint
}

// wireInputAudio represents the nested input_audio object on a Chat audio content part.
type wireInputAudio struct {
	Data   string `json:"data"`   // base64-encoded audio payload bytes
	Format string `json:"format"` // audio format such as wav or mp3
}

// wireFile represents the nested file attachment object on a Chat document content part.
type wireFile struct {
	Filename string `json:"filename,omitzero"`  // optional attached document filename
	FileData string `json:"file_data,omitzero"` // base64 payload or data URI
	FileID   string `json:"file_id,omitzero"`   // uploaded upstream file identifier
}

// wireAnnotation represents a Chat citation annotation entry such as url_citation.
type wireAnnotation struct {
	Type        string                    `json:"type"`                  // citation discriminator such as url_citation
	URLCitation *wireURLCitation          `json:"url_citation,omitzero"` // nested url_citation payload object
	URL         string                    `json:"url,omitzero"`          // flat fallback citation URL
	Title       string                    `json:"title,omitzero"`        // flat fallback citation title
	StartIndex  *int64                    `json:"start_index,omitzero"`  // flat fallback start offset
	EndIndex    *int64                    `json:"end_index,omitzero"`    // flat fallback end offset
	Unknown     map[string]jsontext.Value `json:",embed"`                // unmodeled citation annotation members
}

// wireURLCitation represents the nested url_citation object on a Chat message annotation.
type wireURLCitation struct {
	URL        string `json:"url,omitzero"`         // cited source web URL
	Title      string `json:"title,omitzero"`       // cited source document title
	StartIndex *int64 `json:"start_index,omitzero"` // character span start index
	EndIndex   *int64 `json:"end_index,omitzero"`   // character span end index
}

// wireTool represents an entry in the top-level Chat tools array.
type wireTool struct {
	Type     string                    `json:"type"`              // function or hosted tool type
	Function *wireFunction             `json:"function,omitzero"` // function specification when type is function
	Unknown  map[string]jsontext.Value `json:",embed"`            // unmodeled outer tool members
}

// wireFunction represents a Chat function specification nested beneath tools[].function.
type wireFunction struct {
	Name        string         `json:"name"`                 // callable function tool identifier
	Description string         `json:"description,omitzero"` // human-readable tool description text
	Parameters  jsontext.Value `json:"parameters,omitzero"`  // raw JSON Schema parameters object
	Strict      *bool          `json:"strict,omitzero"`      // strict schema adherence toggle
}

// wireLegacyFunction decodes a legacy top-level functions[] entry alongside any vendor extension fields.
type wireLegacyFunction struct {
	Name        string                    `json:"name"`                 // callable legacy function identifier
	Description string                    `json:"description,omitzero"` // human-readable function description text
	Parameters  jsontext.Value            `json:"parameters,omitzero"`  // raw JSON Schema parameters object
	Strict      *bool                     `json:"strict,omitzero"`      // strict schema adherence toggle
	Unknown     map[string]jsontext.Value `json:",embed"`               // unmodeled legacy function members
}

// wireToolCall represents an assistant tool call in requests, buffered responses, or stream deltas.
type wireToolCall struct {
	Index    *int                      `json:"index,omitzero"` // zero-based tool slot index in streaming chunks
	ID       string                    `json:"id,omitzero"`    // tool call correlation identifier
	Type     string                    `json:"type,omitzero"`  // tool call category, normally "function"
	Function wireFunctionCall          `json:"function"`       // invoked function name and argument fragment
	Unknown  map[string]jsontext.Value `json:",embed"`         // unmodeled tool_call object members
}

// wireFunctionCall represents the nested function object on a tool_call or legacy function_call.
type wireFunctionCall struct {
	Name      string         `json:"name,omitzero"`      // invoked target function name
	Arguments string         `json:"-"`                  // decoded string or raw JSON argument text
	RawArgs   jsontext.Value `json:"arguments,omitzero"` // raw JSON wire representation of arguments
}

// wireToolCallWrite serializes one complete or incremental tool_call with explicit string arguments.
type wireToolCallWrite struct {
	Index    *int                      `json:"index,omitzero"` // streaming tool slot index
	ID       string                    `json:"id,omitzero"`    // call correlation ID on opening/request items
	Type     string                    `json:"type,omitzero"`  // "function" on opening/request items
	Function wireFunctionCallWrite     `json:"function"`       // serialized function name and string arguments
	Unknown  map[string]jsontext.Value `json:",embed"`         // preserved same-origin Extra members
}

// wireFunctionCallWrite serializes function name and string arguments on outgoing tool_calls.
type wireFunctionCallWrite struct {
	Name      string  `json:"name,omitzero"`      // omitted on follow-up argument delta chunks
	Arguments *string `json:"arguments,omitzero"` // non-nil emits exact string including ""
}

// wireResponse represents a non-streaming OpenAI Chat Completions JSON response.
type wireResponse struct {
	ID          string                    `json:"id"`                    // upstream chat completion identifier
	Object      string                    `json:"object"`                // literal "chat.completion" object tag
	Created     int64                     `json:"created"`               // Unix creation timestamp in seconds
	Model       string                    `json:"model"`                 // upstream model name identifier
	Choices     []wireChoice              `json:"choices"`               // completion candidate choice entries
	Usage       jsontext.Value            `json:"usage,omitzero"`        // raw usage object for wirejson validation
	ServiceTier string                    `json:"service_tier,omitzero"` // processing service tier label
	Error       jsontext.Value            `json:"error,omitzero"`        // non-200 or compatible error object
	Unknown     map[string]jsontext.Value `json:",embed"`                // unmodeled top-level response members
}

// wireChoice represents one completion choice inside a non-streaming Chat response.
type wireChoice struct {
	Index        int                `json:"index"`         // zero-based choice position index
	Message      wireInboundMessage `json:"message"`       // completed assistant message object
	FinishReason string             `json:"finish_reason"` // completion stop reason string
}

// wireResponseMessage serializes a completed assistant message inside choices[0].message.
type wireResponseMessage struct {
	Role             string              `json:"role"`                       // literal "assistant" speaker role
	Content          jsontext.Value      `json:"content"`                    // string or explicit null
	ReasoningContent string              `json:"reasoning_content,omitzero"` // optional plaintext reasoning text
	Refusal          string              `json:"refusal,omitzero"`           // optional safety refusal text
	ToolCalls        []wireToolCallWrite `json:"tool_calls,omitzero"`        // optional assistant tool call list
	FunctionCall     *wireFunctionCall   `json:"function_call,omitzero"`     // legacy function call on decode
	Annotations      []wireAnnotation    `json:"annotations,omitzero"`       // optional citation annotations on decode
}

// wireResponseWrite serializes an outgoing non-streaming Chat Completions JSON response.
type wireResponseWrite struct {
	ID          string                    `json:"id"`                    // client-facing completion identifier
	Object      string                    `json:"object"`                // literal "chat.completion" object tag
	Created     int64                     `json:"created"`               // Unix creation timestamp in seconds
	Model       string                    `json:"model"`                 // presented client model name
	ServiceTier string                    `json:"service_tier,omitzero"` // optional processing service tier
	Choices     []wireChoiceWrite         `json:"choices"`               // single assistant choice array
	Usage       *wireUsageWrite           `json:"usage,omitzero"`        // normalized Chat usage object
	Unknown     map[string]jsontext.Value `json:",embed"`                // preserved same-origin Extra members
}

// wireChoiceWrite serializes one completion choice on an outgoing Chat response.
type wireChoiceWrite struct {
	Index        int                 `json:"index"`         // zero-based choice position index
	Message      wireResponseMessage `json:"message"`       // completed assistant message object
	FinishReason string              `json:"finish_reason"` // normalized Chat finish_reason string
}

// wireStreamChunk represents an incoming or outgoing Chat Completions SSE JSON chunk.
type wireStreamChunk struct {
	ID          string            `json:"id"`                    // consistent stream completion identifier
	Object      string            `json:"object"`                // literal "chat.completion.chunk" object tag
	Created     int64             `json:"created"`               // Unix creation timestamp in seconds
	Model       string            `json:"model"`                 // upstream model name identifier
	ServiceTier string            `json:"service_tier,omitzero"` // optional processing service tier
	Choices     []wireChunkChoice `json:"choices"`               // choice deltas or empty slice on usage tail
	Usage       jsontext.Value    `json:"usage,omitzero"`        // raw usage object on incoming chunks
	Error       jsontext.Value    `json:"error,omitzero"`        // mid-stream error payload when present
}

// wireStreamChunkWrite serializes an outgoing Chat Completions SSE chunk.
type wireStreamChunkWrite struct {
	ID          string                 `json:"id"`                    // stream completion identifier string
	Object      string                 `json:"object"`                // literal "chat.completion.chunk" object tag
	Created     int64                  `json:"created"`               // Unix creation timestamp in seconds
	Model       string                 `json:"model"`                 // presented client model name
	ServiceTier string                 `json:"service_tier,omitzero"` // optional processing service tier
	Choices     []wireChunkChoiceWrite `json:"choices"`               // single delta choice or empty usage slice
	Usage       *wireUsageWrite        `json:"usage,omitzero"`        // optional usage tail object
}

// wireChunkChoice represents one choice entry inside an incoming Chat SSE chunk.
type wireChunkChoice struct {
	Index        int       `json:"index"`         // zero-based choice position index
	Delta        wireDelta `json:"delta"`         // incremental assistant delta object
	FinishReason *string   `json:"finish_reason"` // nil until completion chunk
}

// wireChunkChoiceWrite serializes one choice entry inside an outgoing Chat SSE chunk.
type wireChunkChoiceWrite struct {
	Index        int            `json:"index"`         // zero-based choice position index
	Delta        wireDeltaWrite `json:"delta"`         // incremental assistant delta object
	FinishReason *string        `json:"finish_reason"` // serializes null when nil
}

// wireDelta represents incremental assistant fields inside an incoming Chat SSE chunk.
type wireDelta struct {
	Role             string            `json:"role,omitzero"`              // "assistant" on initial chunk
	Content          *string           `json:"content,omitzero"`           // visible text fragment
	ReasoningContent *string           `json:"reasoning_content,omitzero"` // primary reasoning fragment
	Reasoning        *string           `json:"reasoning,omitzero"`         // fallback reasoning fragment
	Refusal          *string           `json:"refusal,omitzero"`           // refusal text fragment
	ToolCalls        []wireToolCall    `json:"tool_calls,omitzero"`        // indexed tool call fragments
	FunctionCall     *wireFunctionCall `json:"function_call,omitzero"`     // legacy function_call fragment
}

// wireDeltaWrite serializes incremental assistant fields inside an outgoing Chat SSE chunk.
type wireDeltaWrite struct {
	Role             string              `json:"role,omitzero"`              // "assistant" on initial chunk
	Content          *string             `json:"content,omitzero"`           // visible text fragment
	ReasoningContent *string             `json:"reasoning_content,omitzero"` // reasoning text fragment
	Refusal          *string             `json:"refusal,omitzero"`           // safety refusal fragment
	ToolCalls        []wireToolCallWrite `json:"tool_calls,omitzero"`        // indexed tool call fragments
}

// marshalDeterministic encodes v with sorted map keys so identical IR inputs
// always produce byte-identical wire JSON. Invalid UTF-8 inside caller strings
// is preserved without failing serialization.
func marshalDeterministic(v any) ([]byte, error) {
	return json.Marshal(v, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
}

// unmarshalJSONObject decodes data into dst after verifying that data starts
// with a top-level JSON object brace '{' and contains no trailing tokens.
// Invalid UTF-8 inside strings is tolerated while duplicate object keys fail.
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

// unmarshalJSONValue decodes raw into dst with invalid-UTF-8 string tolerance.
// Duplicate object member names and trailing tokens still return an error.
func unmarshalJSONValue(raw []byte, dst any) error {
	return json.Unmarshal(raw, dst, jsontext.AllowInvalidUTF8(true))
}

// isValidSingleJSON reports whether raw encodes exactly one syntactically valid
// JSON value without duplicate object member names. Empty inputs and payloads
// with trailing tokens return false.
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

// decodeFunctionArguments extracts string-encoded or raw JSON-encoded function
// arguments from rawArgs. Absent or literal null tokens normalize to "", while
// malformed JSON strings or invalid raw JSON values return an error.
func decodeFunctionArguments(rawArgs jsontext.Value) (string, error) {
	trimmed := bytes.TrimSpace(rawArgs)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}
	if trimmed[0] == '"' {
		var s string
		if err := unmarshalJSONValue(trimmed, &s); err != nil {
			return "", fmt.Errorf("decode function arguments string: %w", err)
		}
		return s, nil
	}
	if !isValidSingleJSON(trimmed) {
		return "", errors.New("invalid raw JSON function arguments")
	}
	return string(trimmed), nil
}

// parseBase64DataURI splits "data:<mime>;base64,<payload>" into its lowercase
// MIME type and base64 payload. Regular remote URLs return isDataURI=false,
// while malformed "data:" prefixes return isDataURI=true and valid=false.
func parseBase64DataURI(raw string) (mime string, data string, isDataURI bool, valid bool) {
	if !strings.HasPrefix(raw, "data:") {
		return "", "", false, false
	}
	rest := raw[len("data:"):]
	meta, payload, hasComma := strings.Cut(rest, ",")
	if !hasComma {
		return "", "", true, false
	}
	if !strings.HasSuffix(strings.ToLower(meta), ";base64") {
		return "", "", true, false
	}
	rawMIME := strings.TrimSpace(meta[:len(meta)-len(";base64")])
	if rawMIME == "" || payload == "" {
		return "", "", true, false
	}
	return strings.ToLower(rawMIME), payload, true, true
}

// formatBase64DataURI formats mime and base64 data into a canonical data URI.
// Blank mime strings fall back to defaultMIME so wire URLs remain well-formed.
func formatBase64DataURI(mime, defaultMIME, data string) string {
	resolved := strings.TrimSpace(mime)
	if resolved == "" {
		resolved = defaultMIME
	}
	return "data:" + resolved + ";base64," + data
}

// compactRawJSON normalizes raw JSON bytes through json/v2's value encoder
// without reparsing numbers or reordering object keys. Decoded schemas, opaque
// items, and Extra values then match post-marshal wire bytes on fixed-point
// round trips even when strings contained invalid UTF-8 or surrogate escapes.
func compactRawJSON(raw jsontext.Value) jsontext.Value {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil
	}
	v := jsontext.Value(bytes.Clone(trimmed))
	// Callers pass slices already parsed by json/v2; if re-encoding ever rejects
	// a fragment, retaining the cloned trimmed bytes preserves the original value.
	normalized, err := marshalDeterministic(v)
	if err != nil {
		return v
	}
	return jsontext.Value(normalized)
}

// toExtra wraps unknown JSON members decoded from a Chat wire object into an
// ir.Extra tagged with ProtocolOpenAIChatCompletions. Empty maps return a zero
// ir.Extra so structs without vendor extensions stay comparable.
func toExtra(unknown map[string]jsontext.Value) ir.Extra {
	if len(unknown) == 0 {
		return ir.Extra{}
	}
	cloned := make(map[string]jsontext.Value, len(unknown))
	for k, v := range unknown {
		cloned[k] = compactRawJSON(v)
	}
	return ir.Extra{
		Origin:  protocol.ProtocolOpenAIChatCompletions,
		Members: cloned,
	}
}

// restoreOrDropExtra returns deep-cloned Extra members when extra originated
// from OpenAI Chat Completions. Foreign non-zero Extra payloads are omitted and
// recorded as an extra_dropped Loss at path.
func restoreOrDropExtra(extra ir.Extra, path string, losses *[]ir.Loss) map[string]jsontext.Value {
	if extra.IsZero() {
		return nil
	}
	if extra.Origin == protocol.ProtocolOpenAIChatCompletions {
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
