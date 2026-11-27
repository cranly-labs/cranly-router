package responses

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"maps"
	"regexp"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// wire.go defines private OpenAI Responses JSON wire schemas, deterministic
// JSON v2 serialization helpers, base64 data-URI codecs, tool-name sanitizers,
// and Extra extension containers for the responses codec package. Isolating
// wire structures inside this package keeps protocol-specific item layouts,
// envelope status tags, and streaming payload shapes hidden from ir and sibling
// protocol codecs.
//
// Containers backed by IR Extra maps (requests, responses, message items,
// content blocks, reasoning items, tool calls, tool outputs, tool declarations,
// namespaces, and citations) embed a map[string]jsontext.Value fallback tagged
// with json:",embed" so vendor fields survive same-protocol round trips without
// float64 coercion or HTML escaping. Request and response decoders tolerate
// invalid UTF-8 sequences inside JSON strings while keeping json/v2 duplicate
// member rejection active on every object, and restoreOrDropExtra emits stored
// members only when Extra.Origin equals protocol.ProtocolOpenAIResponses.

const (
	// responsesObjectTag identifies the canonical object discriminator written on
	// buffered and streaming OpenAI Responses envelopes.
	responsesObjectTag = "response" // canonical response envelope object value

	// defaultResponseSchemaName supplies a valid schema identifier when a
	// cross-protocol json_schema OutputFormat arrives without an explicit Name.
	defaultResponseSchemaName = "response_schema" // fallback json_schema name

	// defaultResponsesToolNameMaxBytes caps function and custom tool names when a
	// target Dialect leaves ToolNameMaxBytes unset or zero.
	defaultResponsesToolNameMaxBytes = 64 // standard OpenAI 64-byte tool name limit
)

var (
	// validResponsesToolNamePattern matches legal OpenAI Responses function and
	// custom tool identifiers before byte-length enforcement runs.
	validResponsesToolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

	// reservedItemTypes lists top-level Responses input item discriminators that
	// cannot appear as untyped opaque message content blocks without changing
	// union branch kind when flattened and re-decoded.
	reservedItemTypes = map[string]bool{
		"message":                 true,
		"reasoning":               true,
		"function_call":           true,
		"custom_tool_call":        true,
		"tool_search_call":        true,
		"function_call_output":    true,
		"custom_tool_call_output": true,
		"tool_search_output":      true,
		"additional_tools":        true,
	}
)

// Codec implements client-side and upstream-side protocol encoding and decoding
// for OpenAI Responses (protocol.ProtocolOpenAIResponses).
type Codec struct{}

// wireInboundRequest decodes an incoming OpenAI Responses JSON request body
// while retaining raw sub-object bytes for lossless Extra preservation.
type wireInboundRequest struct {
	Model              string                    `json:"model"`                         // target model identifier
	Instructions       *string                   `json:"instructions,omitzero"`         // optional top-level system instructions
	Input              jsontext.Value            `json:"input,omitzero"`                // string shorthand or flat item array
	Tools              []jsontext.Value          `json:"tools,omitzero"`                // raw tool and namespace declarations
	ToolChoice         jsontext.Value            `json:"tool_choice,omitzero"`          // string or object tool selector
	ParallelToolCalls  *bool                     `json:"parallel_tool_calls,omitzero"`  // explicit parallel tool-call toggle
	MaxOutputTokens    *int64                    `json:"max_output_tokens,omitzero"`    // maximum generated output tokens
	Temperature        *float64                  `json:"temperature,omitzero"`          // randomness sampling temperature scalar
	TopP               *float64                  `json:"top_p,omitzero"`                // nucleus sampling probability mass
	Reasoning          jsontext.Value            `json:"reasoning,omitzero"`            // raw reasoning effort and summary object
	Include            jsontext.Value            `json:"include,omitzero"`              // raw include array for encrypted_content
	Text               jsontext.Value            `json:"text,omitzero"`                 // raw text format and verbosity object
	Stream             bool                      `json:"stream,omitzero"`               // SSE streaming delivery toggle
	RawServiceTier     jsontext.Value            `json:"service_tier,omitzero"`         // raw service_tier token for validation
	PromptCacheKey     string                    `json:"prompt_cache_key,omitzero"`     // cross-protocol prompt cache key
	PreviousResponseID string                    `json:"previous_response_id,omitzero"` // stateful continuation response ID
	Store              *bool                     `json:"store,omitzero"`                // upstream persistence toggle
	Conversation       jsontext.Value            `json:"conversation,omitzero"`         // string or object conversation binding
	ConversationID     string                    `json:"conversation_id,omitzero"`      // compatible flat conversation identifier
	SessionID          string                    `json:"session_id,omitzero"`           // explicit client session identifier
	User               string                    `json:"user,omitzero"`                 // caller end-user account identifier
	Metadata           jsontext.Value            `json:"metadata,omitzero"`             // raw request metadata object
	Unknown            map[string]jsontext.Value `json:",embed"`                        // unmodeled top-level request members
}

// wireOutRequest serializes a normalized OpenAI Responses JSON request body.
type wireOutRequest struct {
	Model              string                    `json:"model"`                         // target model identifier
	Instructions       string                    `json:"instructions,omitzero"`         // joined top-level system instructions
	Input              []jsontext.Value          `json:"input"`                         // legalized flat input item array
	Tools              []jsontext.Value          `json:"tools,omitzero"`                // serialized tool and namespace objects
	ToolChoice         jsontext.Value            `json:"tool_choice,omitzero"`          // serialized tool selector value
	ParallelToolCalls  *bool                     `json:"parallel_tool_calls,omitzero"`  // explicit parallel tool-call toggle
	MaxOutputTokens    *int64                    `json:"max_output_tokens,omitzero"`    // maximum generated output tokens
	Temperature        *float64                  `json:"temperature,omitzero"`          // randomness sampling temperature scalar
	TopP               *float64                  `json:"top_p,omitzero"`                // nucleus sampling probability mass
	Reasoning          *wireReasoningConfig      `json:"reasoning,omitzero"`            // reasoning effort and summary settings
	Include            []string                  `json:"include,omitzero"`              // requested auxiliary response fields
	Text               *wireTextConfig           `json:"text,omitzero"`                 // structured output and verbosity config
	Stream             bool                      `json:"stream,omitzero"`               // SSE streaming delivery toggle
	ServiceTier        string                    `json:"service_tier,omitzero"`         // normalized processing service tier
	PromptCacheKey     string                    `json:"prompt_cache_key,omitzero"`     // cross-protocol prompt cache key
	PreviousResponseID string                    `json:"previous_response_id,omitzero"` // stateful continuation response ID
	Store              *bool                     `json:"store,omitzero"`                // upstream persistence toggle
	Conversation       jsontext.Value            `json:"conversation,omitzero"`         // serialized string or object conversation
	ConversationID     string                    `json:"conversation_id,omitzero"`      // flat conversation_id when ConversationSet is false
	SessionID          string                    `json:"session_id,omitzero"`           // explicit client session identifier
	User               string                    `json:"user,omitzero"`                 // caller end-user account identifier
	Metadata           *wireMetadata             `json:"metadata,omitzero"`             // request metadata object
	Unknown            map[string]jsontext.Value `json:",embed"`                        // preserved same-origin Extra members
}

// wireReasoningConfig represents the top-level reasoning configuration object on a Responses request or response echo.
type wireReasoningConfig struct {
	Effort  string                    `json:"effort,omitzero"`  // reasoning effort level label
	Summary string                    `json:"summary,omitzero"` // reasoning summary verbosity mode
	Unknown map[string]jsontext.Value `json:",embed"`           // unmodeled reasoning object members
}

// wireTextConfig represents the top-level text formatting and verbosity object on a Responses request or response echo.
type wireTextConfig struct {
	Format    *wireTextFormat           `json:"format,omitzero"`    // structured output format specification
	Verbosity string                    `json:"verbosity,omitzero"` // text output verbosity preference
	Unknown   map[string]jsontext.Value `json:",embed"`             // unmodeled text object members
}

// wireTextFormat represents text.format inside an OpenAI Responses request or response echo.
type wireTextFormat struct {
	Type        string                    `json:"type"`                 // text, json_object, or json_schema
	Name        string                    `json:"name,omitzero"`        // schema identifier when type is json_schema
	Description string                    `json:"description,omitzero"` // optional human-readable schema description
	Schema      jsontext.Value            `json:"schema,omitzero"`      // raw JSON Schema object bytes
	Strict      *bool                     `json:"strict,omitzero"`      // strict schema adherence toggle
	Unknown     map[string]jsontext.Value `json:",embed"`               // unmodeled text.format object members
}

// wireMetadata represents the top-level metadata object on a Responses request.
type wireMetadata struct {
	UserID  string                    `json:"user_id,omitzero"` // nested metadata user identifier
	Unknown map[string]jsontext.Value `json:",embed"`           // unmodeled metadata entries
}

// wireConversationObject decodes or encodes object-shaped top-level conversation bindings.
type wireConversationObject struct {
	ID      string                    `json:"id,omitzero"` // conversation identifier string
	Unknown map[string]jsontext.Value `json:",embed"`      // unmodeled conversation object members
}

// wireTypeRoleProbe inspects the type and role discriminators of a raw JSON object.
type wireTypeRoleProbe struct {
	ID      string                    `json:"id,omitzero"`   // optional wire item identifier
	Type    string                    `json:"type,omitzero"` // wire item or block type discriminator
	Role    string                    `json:"role,omitzero"` // speaker role on message items
	Unknown map[string]jsontext.Value `json:",embed"`        // remaining uninspected object fields
}

// wireMessageItem decodes or encodes a message item inside Responses input[].
type wireMessageItem struct {
	ID      string                    `json:"id,omitzero"`      // optional message item identifier
	Type    string                    `json:"type,omitzero"`    // "message" on canonical encoded items
	Role    string                    `json:"role"`             // user, assistant, system, or developer
	Content jsontext.Value            `json:"content,omitzero"` // string shorthand or content block array
	Unknown map[string]jsontext.Value `json:",embed"`           // unmodeled message item members
}

// wireContentPart decodes or encodes one typed block inside a request message content or tool output array.
type wireContentPart struct {
	Type        string                    `json:"type"`                 // input_text, output_text, refusal, input_image, input_file, or input_audio
	Text        string                    `json:"text,omitzero"`        // text payload for input_text and output_text
	Refusal     string                    `json:"refusal,omitzero"`     // refusal text when type is refusal
	Annotations []wireAnnotation          `json:"annotations,omitzero"` // structured citations on output_text
	ImageURL    jsontext.Value            `json:"image_url,omitzero"`   // string URL/data-URI or nested object on input_image
	FileID      string                    `json:"file_id,omitzero"`     // uploaded file reference on input_image or input_file
	Detail      string                    `json:"detail,omitzero"`      // image detail hint such as high or auto
	Filename    string                    `json:"filename,omitzero"`    // document filename on input_file
	FileData    string                    `json:"file_data,omitzero"`   // base64 payload or data URI on input_file
	FileURL     string                    `json:"file_url,omitzero"`    // remote document URL on input_file
	InputAudio  *wireInputAudio           `json:"input_audio,omitzero"` // inline audio payload on input_audio
	Unknown     map[string]jsontext.Value `json:",embed"`               // unmodeled content-part members
}

// wireResponseContentPart decodes an output_text or refusal block inside response message items.
type wireResponseContentPart struct {
	Type        string                    `json:"type"`                 // output_text, text, or refusal discriminator
	Text        *string                   `json:"text,omitzero"`        // text payload pointer when present
	Refusal     *string                   `json:"refusal,omitzero"`     // refusal payload pointer when present
	Annotations jsontext.Value            `json:"annotations,omitzero"` // raw citation array on output_text
	Unknown     map[string]jsontext.Value `json:",embed"`               // unmodeled response content-part members
}

// wireOutputTextPartWrite serializes an input_text or output_text block without declaring refusal.
type wireOutputTextPartWrite struct {
	Type        string                    `json:"type"`                 // literal "input_text" or "output_text" discriminator
	Text        string                    `json:"text"`                 // visible text string including ""
	Annotations []wireAnnotation          `json:"annotations,omitzero"` // citation annotations attached to output_text
	Unknown     map[string]jsontext.Value `json:",embed"`               // preserved same-origin content Extra members
}

// wireRefusalPartWrite serializes an assistant refusal block without declaring text or annotations.
type wireRefusalPartWrite struct {
	Type    string                    `json:"type"`    // literal "refusal" discriminator
	Refusal string                    `json:"refusal"` // safety refusal text including ""
	Unknown map[string]jsontext.Value `json:",embed"`  // preserved same-origin refusal Extra members
}

// wireInputAudio represents inline base64 audio inside an input_audio block.
type wireInputAudio struct {
	Data   string `json:"data"`   // base64-encoded audio bytes
	Format string `json:"format"` // audio format label such as wav or mp3
}

// wireImageURLObject decodes compatible object-shaped image_url fields on input_image blocks.
type wireImageURLObject struct {
	URL    string `json:"url"`             // remote URL or base64 data URI
	Detail string `json:"detail,omitzero"` // optional detail hint
}

// wireAnnotation decodes or encodes an annotation entry attached to output_text.
type wireAnnotation struct {
	Type        string                    `json:"type"`                  // citation discriminator such as url_citation
	URL         string                    `json:"url,omitzero"`          // flat cited URL on Responses url_citation
	Title       string                    `json:"title,omitzero"`        // flat cited source title
	StartIndex  *int64                    `json:"start_index,omitzero"`  // UTF-8 or character start offset
	EndIndex    *int64                    `json:"end_index,omitzero"`    // UTF-8 or character end offset
	URLCitation *wireNestedURLCitation    `json:"url_citation,omitzero"` // compatible Chat-style nested url_citation
	Unknown     map[string]jsontext.Value `json:",embed"`                // unmodeled citation members
}

// wireNestedURLCitation decodes compatible Chat-shaped url_citation wrapper objects inside output_text annotations.
type wireNestedURLCitation struct {
	URL        string `json:"url,omitzero"`         // cited web URL
	Title      string `json:"title,omitzero"`       // cited document title
	StartIndex *int64 `json:"start_index,omitzero"` // span start offset
	EndIndex   *int64 `json:"end_index,omitzero"`   // span end offset
}

// wireSummaryText represents one summary_text segment object inside a reasoning item summary array.
type wireSummaryText struct {
	Type string `json:"type"` // literal "summary_text" discriminator
	Text string `json:"text"` // visible summary segment text
}

// wireReasoningText represents one reasoning_text segment object inside a reasoning item content array.
type wireReasoningText struct {
	Type string `json:"type"` // literal "reasoning_text" discriminator
	Text string `json:"text"` // raw plaintext reasoning segment
}

// wireReasoningItem decodes or encodes a reasoning item inside Responses input[].
type wireReasoningItem struct {
	ID               string                    `json:"id,omitzero"`                // reasoning item identifier
	Type             string                    `json:"type"`                       // literal "reasoning" discriminator
	Summary          []wireSummaryText         `json:"summary"`                    // summary segments (empty slice serializes [])
	Content          []wireReasoningText       `json:"content,omitzero"`           // optional raw reasoning_text segments
	EncryptedContent string                    `json:"encrypted_content,omitzero"` // opaque continuity token or trc1 carrier
	Unknown          map[string]jsontext.Value `json:",embed"`                     // unmodeled reasoning item members
}

// wireFunctionCallItem decodes or encodes a function_call item inside Responses input[].
type wireFunctionCallItem struct {
	ID        string                    `json:"id,omitzero"`        // optional item identifier such as fc_...
	Type      string                    `json:"type"`               // literal "function_call" discriminator
	CallID    string                    `json:"call_id"`            // call correlation identifier
	Namespace string                    `json:"namespace,omitzero"` // optional tool namespace name
	Name      string                    `json:"name"`               // invoked function name
	Arguments string                    `json:"-"`                  // normalized JSON argument string
	RawArgs   jsontext.Value            `json:"arguments"`          // raw wire JSON string or object arguments
	Unknown   map[string]jsontext.Value `json:",embed"`             // unmodeled function_call item members
}

// wireFunctionCallItemWrite serializes a function_call item in input[] with string arguments.
type wireFunctionCallItemWrite struct {
	ID        string                    `json:"id,omitzero"`        // optional item identifier
	Type      string                    `json:"type"`               // literal "function_call" discriminator
	CallID    string                    `json:"call_id"`            // call correlation identifier
	Namespace string                    `json:"namespace,omitzero"` // optional tool namespace
	Name      string                    `json:"name"`               // invoked function name
	Arguments string                    `json:"arguments"`          // JSON-encoded argument string
	Unknown   map[string]jsontext.Value `json:",embed"`             // preserved same-origin Extra members
}

// wireCustomToolCallItem decodes or encodes a custom_tool_call item inside Responses input[].
type wireCustomToolCallItem struct {
	ID        string                    `json:"id,omitzero"`        // optional item identifier such as ctc_...
	Type      string                    `json:"type"`               // literal "custom_tool_call" discriminator
	CallID    string                    `json:"call_id"`            // call correlation identifier
	Namespace string                    `json:"namespace,omitzero"` // optional tool namespace
	Name      string                    `json:"name"`               // invoked custom tool name
	Input     string                    `json:"input"`              // free-form custom tool input text
	Unknown   map[string]jsontext.Value `json:",embed"`             // unmodeled custom_tool_call members
}

// wireToolSearchCallItem decodes or encodes a tool_search_call item inside Responses input[].
type wireToolSearchCallItem struct {
	ID        string                    `json:"id,omitzero"`        // optional item identifier such as tsc_...
	Type      string                    `json:"type"`               // literal "tool_search_call" discriminator
	CallID    string                    `json:"call_id"`            // call correlation identifier
	Arguments jsontext.Value            `json:"arguments,omitzero"` // raw JSON object or string search arguments
	Unknown   map[string]jsontext.Value `json:",embed"`             // unmodeled tool_search_call members
}

// wireCallOutputItem decodes or encodes function_call_output, custom_tool_call_output, and tool_search_output items.
type wireCallOutputItem struct {
	ID      string                    `json:"id,omitzero"`     // optional output item identifier
	Type    string                    `json:"type"`            // call output item discriminator
	CallID  string                    `json:"call_id"`         // correlated call identifier
	Output  jsontext.Value            `json:"output,omitzero"` // string or content part array
	Tools   jsontext.Value            `json:"tools,omitzero"`  // discovered tools array on tool_search_output
	Unknown map[string]jsontext.Value `json:",embed"`          // unmodeled *_output item members
}

// wireAdditionalToolsItem decodes an additional_tools pseudo-item inside Responses input[].
type wireAdditionalToolsItem struct {
	Type  string           `json:"type"`  // literal "additional_tools" discriminator
	Tools []jsontext.Value `json:"tools"` // additional tool declarations lifted to Request.Tools
}

// wireFunctionTool decodes or encodes a flat Responses function tool declaration.
type wireFunctionTool struct {
	Type         string                    `json:"type"`                   // literal "function" discriminator
	Name         string                    `json:"name,omitzero"`          // flat function name
	Description  string                    `json:"description,omitzero"`   // human-readable tool description
	Parameters   jsontext.Value            `json:"parameters,omitzero"`    // raw JSON Schema parameters object
	Strict       *bool                     `json:"strict,omitzero"`        // strict schema adherence toggle
	DeferLoading *bool                     `json:"defer_loading,omitzero"` // deferred tool discovery flag
	Function     *wireCompatNestedFunction `json:"function,omitzero"`      // compatible Chat-nested function fallback
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled function tool members
}

// wireCompatNestedFunction decodes Chat-shaped tools[].function fallback fields on compatible requests.
type wireCompatNestedFunction struct {
	Name        string         `json:"name"`                 // nested function name
	Description string         `json:"description,omitzero"` // nested function description
	Parameters  jsontext.Value `json:"parameters,omitzero"`  // nested JSON Schema parameters
	Strict      *bool          `json:"strict,omitzero"`      // nested strict toggle
}

// wireCustomTool decodes or encodes a Responses custom free-form tool declaration.
type wireCustomTool struct {
	Type         string                    `json:"type"`                   // literal "custom" discriminator
	Name         string                    `json:"name"`                   // declared custom tool name
	Description  string                    `json:"description,omitzero"`   // human-readable tool description
	Format       jsontext.Value            `json:"format,omitzero"`        // raw grammar or text format object
	DeferLoading *bool                     `json:"defer_loading,omitzero"` // deferred tool discovery flag
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled custom tool members
}

// wireToolSearchTool decodes or encodes a Responses tool_search declaration.
type wireToolSearchTool struct {
	Type         string                    `json:"type"`                   // literal "tool_search" discriminator
	Name         string                    `json:"name,omitzero"`          // optional explicit tool_search name
	Description  string                    `json:"description,omitzero"`   // optional discovery instructions
	Strict       *bool                     `json:"strict,omitzero"`        // optional strict toggle
	DeferLoading *bool                     `json:"defer_loading,omitzero"` // optional deferred flag
	Unknown      map[string]jsontext.Value `json:",embed"`                 // execution, parameters, and vendor members
}

// wireNamespaceTool decodes or encodes a Responses namespace tool group declaration.
type wireNamespaceTool struct {
	Type         string                    `json:"type"`                   // literal "namespace" discriminator
	Name         string                    `json:"name"`                   // namespace group identifier
	Description  string                    `json:"description,omitzero"`   // human-readable namespace description
	DeferLoading *bool                     `json:"defer_loading,omitzero"` // deferred namespace discovery flag
	Tools        []jsontext.Value          `json:"tools,omitzero"`         // child function and custom declarations
	Children     []jsontext.Value          `json:"children,omitzero"`      // compatible alias for child declarations
	Unknown      map[string]jsontext.Value `json:",embed"`                 // unmodeled namespace members
}

// wireResponse decodes a non-streaming OpenAI Responses JSON reply or stream lifecycle response envelope.
type wireResponse struct {
	ID                 string                    `json:"id"`                            // upstream response identifier
	Object             string                    `json:"object"`                        // literal "response" object tag
	CreatedAt          int64                     `json:"created_at"`                    // Unix creation timestamp in seconds
	CompletedAt        jsontext.Value            `json:"completed_at,omitzero"`         // optional completion timestamp
	Status             string                    `json:"status"`                        // completed, in_progress, incomplete, failed, or cancelled
	IncompleteDetails  *wireIncompleteDetails    `json:"incomplete_details,omitzero"`   // reason when status is incomplete
	Error              jsontext.Value            `json:"error,omitzero"`                // error object when status is failed
	Model              string                    `json:"model"`                         // upstream model identifier
	Output             []jsontext.Value          `json:"output"`                        // ordered output item objects
	Usage              jsontext.Value            `json:"usage,omitzero"`                // raw usage object for wirejson validation
	ServiceTier        string                    `json:"service_tier,omitzero"`         // processing service tier label
	Instructions       jsontext.Value            `json:"instructions,omitzero"`         // echoed request instructions
	MaxOutputTokens    jsontext.Value            `json:"max_output_tokens,omitzero"`    // echoed request max_output_tokens
	MaxToolCalls       jsontext.Value            `json:"max_tool_calls,omitzero"`       // echoed request max_tool_calls
	ParallelToolCalls  jsontext.Value            `json:"parallel_tool_calls,omitzero"`  // echoed request parallel_tool_calls
	PreviousResponseID jsontext.Value            `json:"previous_response_id,omitzero"` // echoed request previous_response_id
	Store              jsontext.Value            `json:"store,omitzero"`                // echoed store toggle
	Temperature        jsontext.Value            `json:"temperature,omitzero"`          // echoed request temperature
	TopP               jsontext.Value            `json:"top_p,omitzero"`                // echoed request top_p
	TopLogprobs        jsontext.Value            `json:"top_logprobs,omitzero"`         // echoed request top_logprobs
	Truncation         jsontext.Value            `json:"truncation,omitzero"`           // echoed truncation policy
	User               jsontext.Value            `json:"user,omitzero"`                 // echoed user identifier
	SafetyIdentifier   jsontext.Value            `json:"safety_identifier,omitzero"`    // echoed safety identifier
	PromptCacheKey     jsontext.Value            `json:"prompt_cache_key,omitzero"`     // echoed request prompt_cache_key
	Reasoning          jsontext.Value            `json:"reasoning,omitzero"`            // echoed reasoning config
	Text               jsontext.Value            `json:"text,omitzero"`                 // echoed text config
	Tools              jsontext.Value            `json:"tools,omitzero"`                // echoed tool declarations
	ToolChoice         jsontext.Value            `json:"tool_choice,omitzero"`          // echoed tool_choice selector
	Metadata           jsontext.Value            `json:"metadata,omitzero"`             // echoed metadata map
	Background         jsontext.Value            `json:"background,omitzero"`           // echoed background flag
	Conversation       jsontext.Value            `json:"conversation,omitzero"`         // echoed conversation binding
	Unknown            map[string]jsontext.Value `json:",embed"`                        // unmodeled response-level vendor members
}

// wireResponseWrite serializes a buffered or streaming OpenAI Responses envelope.
type wireResponseWrite struct {
	ID                 string                    `json:"id"`                            // client-facing response identifier
	Object             string                    `json:"object"`                        // literal "response" object tag
	CreatedAt          int64                     `json:"created_at"`                    // Unix creation timestamp in seconds
	Status             string                    `json:"status"`                        // in_progress, completed, incomplete, or failed
	IncompleteDetails  *wireIncompleteDetails    `json:"incomplete_details,omitzero"`   // populated when status is incomplete
	Error              *wireErrorBodyWrite       `json:"error,omitzero"`                // populated when status is failed
	ServiceTier        string                    `json:"service_tier,omitzero"`         // optional processing service tier
	Model              string                    `json:"model"`                         // client-visible model identifier
	Instructions       string                    `json:"instructions,omitzero"`         // echoed request instructions
	MaxOutputTokens    *int64                    `json:"max_output_tokens,omitzero"`    // echoed request max_output_tokens
	ParallelToolCalls  *bool                     `json:"parallel_tool_calls,omitzero"`  // echoed request parallel_tool_calls
	PreviousResponseID string                    `json:"previous_response_id,omitzero"` // echoed request previous_response_id
	Store              *bool                     `json:"store,omitzero"`                // echoed request store flag
	Temperature        *float64                  `json:"temperature,omitzero"`          // echoed request temperature
	TopP               *float64                  `json:"top_p,omitzero"`                // echoed request top_p
	Reasoning          *wireReasoningConfig      `json:"reasoning,omitzero"`            // echoed request reasoning config
	Text               *wireTextConfig           `json:"text,omitzero"`                 // echoed request text config
	Tools              []jsontext.Value          `json:"tools,omitzero"`                // echoed request tool declarations
	ToolChoice         jsontext.Value            `json:"tool_choice,omitzero"`          // echoed request tool_choice selector
	Output             []jsontext.Value          `json:"output"`                        // non-nil output item array
	Usage              *wireUsageWrite           `json:"usage,omitzero"`                // normalized Responses usage object
	Unknown            map[string]jsontext.Value `json:",embed"`                        // preserved same-origin response Extra members
}

// wireResponseOutputItem decodes a typed output item inside response.output[] or stream item events.
type wireResponseOutputItem struct {
	ID               string                    `json:"id,omitzero"`                // output item identifier
	Type             string                    `json:"type"`                       // message, reasoning, function_call, custom_tool_call, or tool_search_call
	Status           string                    `json:"status,omitzero"`            // in_progress, completed, or incomplete
	Role             string                    `json:"role,omitzero"`              // "assistant" on message items
	Content          []jsontext.Value          `json:"content,omitzero"`           // raw message content blocks or reasoning_text blocks
	Summary          []wireSummaryText         `json:"summary,omitzero"`           // summary_text segments on reasoning items
	EncryptedContent string                    `json:"encrypted_content,omitzero"` // encrypted continuity token on reasoning items
	CallID           string                    `json:"call_id,omitzero"`           // call correlation identifier
	Namespace        string                    `json:"namespace,omitzero"`         // optional tool namespace
	Name             string                    `json:"name,omitzero"`              // invoked tool name
	Execution        string                    `json:"execution,omitzero"`         // "client" or "server" on tool_search_call
	RawArguments     jsontext.Value            `json:"arguments,omitzero"`         // string on function_call, object on tool_search_call
	Input            string                    `json:"input,omitzero"`             // free-text input on custom_tool_call
	Unknown          map[string]jsontext.Value `json:",embed"`                     // unmodeled output-item vendor members
}

// wireResponseMessageWrite serializes a completed or in-progress assistant message output item.
type wireResponseMessageWrite struct {
	ID      string                    `json:"id"`      // message item identifier
	Type    string                    `json:"type"`    // literal "message" discriminator
	Status  string                    `json:"status"`  // "in_progress" or "completed"
	Role    string                    `json:"role"`    // literal "assistant" speaker role
	Content []jsontext.Value          `json:"content"` // non-nil serialized content part slice
	Unknown map[string]jsontext.Value `json:",embed"`  // optional preserved message Extra members
}

// wireResponseFunctionCallWrite serializes a function_call output item on responses and SSE events.
type wireResponseFunctionCallWrite struct {
	ID        string                    `json:"id"`                 // item identifier such as fc_...
	Type      string                    `json:"type"`               // literal "function_call" discriminator
	Status    string                    `json:"status"`             // "in_progress" or "completed"
	CallID    string                    `json:"call_id"`            // call correlation identifier
	Namespace string                    `json:"namespace,omitzero"` // optional tool namespace
	Name      string                    `json:"name"`               // invoked function name
	Arguments string                    `json:"arguments"`          // JSON argument string ("" on added)
	Unknown   map[string]jsontext.Value `json:",embed"`             // preserved same-origin Extra members
}

// wireResponseCustomCallWrite serializes a custom_tool_call output item on responses and SSE events.
type wireResponseCustomCallWrite struct {
	ID        string                    `json:"id"`                 // item identifier such as ctc_...
	Type      string                    `json:"type"`               // literal "custom_tool_call" discriminator
	Status    string                    `json:"status"`             // "in_progress" or "completed"
	CallID    string                    `json:"call_id"`            // call correlation identifier
	Namespace string                    `json:"namespace,omitzero"` // optional tool namespace
	Name      string                    `json:"name"`               // invoked custom tool name
	Input     string                    `json:"input"`              // free-form input string ("" on added)
	Unknown   map[string]jsontext.Value `json:",embed"`             // preserved same-origin Extra members
}

// wireResponseToolSearchCallWrite serializes a tool_search_call output item on responses and SSE events.
type wireResponseToolSearchCallWrite struct {
	ID        string                    `json:"id"`                 // item identifier such as tsc_...
	Type      string                    `json:"type"`               // literal "tool_search_call" discriminator
	Status    string                    `json:"status"`             // "in_progress" or "completed"
	CallID    string                    `json:"call_id"`            // call correlation identifier
	Execution string                    `json:"execution,omitzero"` // "client" execution mode
	Arguments jsontext.Value            `json:"arguments"`          // raw JSON object arguments
	Unknown   map[string]jsontext.Value `json:",embed"`             // preserved same-origin Extra members
}

// marshalDeterministic encodes v with sorted map keys and invalid-UTF-8 string
// tolerance so identical IR inputs produce byte-for-byte identical JSON output.
// Callers receive an error only when v contains an unmarshalable Go value.
func marshalDeterministic(v any) ([]byte, error) {
	return json.Marshal(v, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
}

// unmarshalJSONObject decodes data into dst after verifying that data starts
// with a top-level JSON object brace '{' and contains no trailing tokens.
// Invalid UTF-8 inside strings is accepted while duplicate object keys fail.
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

// unmarshalJSONValue decodes raw into dst with invalid-UTF-8 string tolerance
// enabled so legacy non-UTF-8 client byte sequences survive decoding. Duplicate
// object member names, type mismatches, and trailing tokens return an error.
func unmarshalJSONValue(raw []byte, dst any) error {
	return json.Unmarshal(raw, dst, jsontext.AllowInvalidUTF8(true))
}

// isValidSingleJSON reports whether raw encodes one syntactically valid JSON
// value with no duplicate object member names and no trailing tokens. Blank
// inputs and concatenated JSON values return false.
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
// Blank payloads, non-object JSON values, and trailing tokens return false.
func isValidSingleJSONObject(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '{' && isValidSingleJSON(trimmed)
}

// compactRawJSON normalizes raw JSON bytes through json/v2's value encoder
// without reparsing numbers or reordering object keys. Whitespace-only inputs
// return nil, while valid values return an owned compacted slice that stays
// byte-stable across round trips even when strings contain invalid UTF-8.
func compactRawJSON(raw jsontext.Value) jsontext.Value {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil
	}
	v := jsontext.Value(bytes.Clone(trimmed))
	// Callers supply slices already accepted by json/v2; falling back to cloned
	// trimmed bytes preserves the original value if re-encoding ever fails.
	normalized, err := marshalDeterministic(v)
	if err != nil {
		return v
	}
	return jsontext.Value(normalized)
}

// cloneValueMap returns an owned copy of src with every raw JSON entry
// compacted via compactRawJSON. Nil or empty maps return nil so zero-valued
// extension maps remain comparable during Law Q2 checks.
func cloneValueMap(src map[string]jsontext.Value) map[string]jsontext.Value {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]jsontext.Value, len(src))
	for k, v := range src {
		out[k] = compactRawJSON(v)
	}
	return out
}

// insertRawMember compacts raw and stores it into m under key, allocating m
// lazily when nil and returning the updated map. Whitespace-only or empty raw
// slices leave m untouched without allocating a map.
func insertRawMember(m map[string]jsontext.Value, key string, raw jsontext.Value) map[string]jsontext.Value {
	compacted := compactRawJSON(raw)
	if len(compacted) == 0 {
		return m
	}
	if m == nil {
		m = make(map[string]jsontext.Value, 1)
	}
	m[key] = compacted
	return m
}

// marshalUnknownMap serializes unknown into deterministic JSON object bytes for
// storage inside parent Extra maps. Empty or nil maps return (nil, nil) so
// callers omit absent sub-object extensions cleanly.
func marshalUnknownMap(unknown map[string]jsontext.Value) (jsontext.Value, error) {
	if len(unknown) == 0 {
		return nil, nil
	}
	encoded, err := marshalDeterministic(unknown)
	if err != nil {
		return nil, err
	}
	return jsontext.Value(encoded), nil
}

// extractSubObjectUnknown parses a stored JSON object from container[key] and
// removes modeledKeys so only unmodeled extension members remain. Non-object or
// absent entries return nil without failing encoding.
func extractSubObjectUnknown(container map[string]jsontext.Value, key string, modeledKeys ...string) map[string]jsontext.Value {
	raw, ok := container[key]
	if !ok {
		return nil
	}
	var parsed map[string]jsontext.Value
	// Stored sub-objects were validated on decode; ignoring unexpected non-object
	// shapes here avoids aborting outbound encoding on synthetic IR test fixtures.
	if unmarshalJSONObject(raw, &parsed) != nil || len(parsed) == 0 {
		return nil
	}
	for _, mk := range modeledKeys {
		delete(parsed, mk)
	}
	if len(parsed) == 0 {
		return nil
	}
	return parsed
}

// toExtra wraps unknown JSON members decoded from an OpenAI Responses wire
// object into an ir.Extra tagged with ProtocolOpenAIResponses. Empty maps return
// a zero ir.Extra value.
func toExtra(unknown map[string]jsontext.Value) ir.Extra {
	if len(unknown) == 0 {
		return ir.Extra{}
	}
	return ir.Extra{
		Origin:  protocol.ProtocolOpenAIResponses,
		Members: cloneValueMap(unknown),
	}
}

// restoreOrDropExtra returns deep-copied Extra members when extra originated
// from OpenAI Responses. Non-zero foreign Extra maps are omitted and recorded
// as an extra_dropped Loss at path.
func restoreOrDropExtra(extra ir.Extra, path string, losses *[]ir.Loss) map[string]jsontext.Value {
	if extra.IsZero() {
		return nil
	}
	if extra.Origin == protocol.ProtocolOpenAIResponses {
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

// parseBase64DataURI splits a "data:<mime>;base64,<payload>" string into its
// lowercase MIME type and raw base64 payload. Ordinary remote URLs without a
// "data:" prefix return isDataURI=false, whereas malformed "data:" headers
// missing ";base64," or payload bytes return isDataURI=true and valid=false.
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

// formatBase64DataURI formats mime and base64 data into a canonical
// "data:<mime>;base64,<data>" URI string. When mime is empty or whitespace,
// defaultMIME is substituted so outgoing image and document fields remain valid.
func formatBase64DataURI(mime, defaultMIME, data string) string {
	resolved := strings.TrimSpace(mime)
	if resolved == "" {
		resolved = defaultMIME
	}
	return "data:" + resolved + ";base64," + data
}

// deriveDeterministicID builds a collision-resistant identifier combining prefix
// with 16 lowercase SHA-256 hex characters derived from seed. Repeated encodings
// of the same request produce identical fallback IDs without Runtime randomness.
func deriveDeterministicID(prefix, seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return prefix + hex.EncodeToString(sum[:8])
}

// sanitizeResponsesToolName enforces OpenAI's ^[a-zA-Z0-9_-]{1,maxBytes}$ tool
// identifier rule and reports whether raw was modified. Overlong or invalid
// names receive a deterministic 8-hex SHA-256 suffix so distinct source tools
// never collide after flattening or character replacement.
func sanitizeResponsesToolName(raw string, maxBytes int) (string, bool) {
	if maxBytes <= 0 {
		maxBytes = defaultResponsesToolNameMaxBytes
	}
	if raw != "" && len(raw) <= maxBytes && validResponsesToolNamePattern.MatchString(raw) {
		return raw, false
	}
	var b strings.Builder
	b.Grow(len(raw))
	for i := range len(raw) {
		ch := raw[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' || ch == '-' {
			_ = b.WriteByte(ch)
		} else {
			_ = b.WriteByte('_')
		}
	}
	cleaned := b.String()
	if cleaned == "" {
		cleaned = "tool"
	}
	sum := sha256.Sum256([]byte(raw))
	hashSuffix := hex.EncodeToString(sum[:4])
	if len(cleaned) > maxBytes {
		if maxBytes <= 9 {
			return hashSuffix[:maxBytes], true
		}
		prefixLen := maxBytes - 1 - len(hashSuffix)
		return cleaned[:prefixLen] + "_" + hashSuffix, true
	}
	if cleaned != raw {
		candidate := cleaned + "_" + hashSuffix
		if len(candidate) <= maxBytes {
			return candidate, true
		}
		if maxBytes <= 9 {
			return hashSuffix[:maxBytes], true
		}
		prefixLen := maxBytes - 1 - len(hashSuffix)
		return cleaned[:prefixLen] + "_" + hashSuffix, true
	}
	return cleaned, cleaned != raw
}

// flattenNamespaceToolName joins non-empty namespace and name strings with "__"
// and sanitizes the combined identifier to fit within maxBytes. Non-positive
// maxBytes values default to OpenAI's 64-byte function name limit.
func flattenNamespaceToolName(namespace, name string, maxBytes int) string {
	joined := name
	if namespace != "" {
		joined = namespace + "__" + name
	}
	sanitized, _ := sanitizeResponsesToolName(joined, maxBytes)
	return sanitized
}
