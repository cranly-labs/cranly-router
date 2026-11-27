package mockupstream

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// inspect.go recognizes upstream protocol endpoints from HTTP request paths,
// decodes registered text protocols into ir.Request values via convert, and
// retains fallback wire-level turn inspection when InspectRequest is called
// directly or an unregistered protocol ID is queried. Scripted upstreams match
// testkit.Turn predicates by historical assistant message count, tool result
// count, model prefix, and declared tools without relying on global request
// counters that client background requests would desynchronize.
//
// DecodeAndSummarizeRequest delegates to convert.DecodeRequest whenever a
// client codec is registered so Turn.Expect checks inspect normalized IR trees,
// and falls back to lightweight wire inspection for any unregistered protocol
// identifier. Wire inspection decodes through encoding/json/v2
// and jsontext with invalid UTF-8 tolerated and duplicate keys rejected so turn
// counts stay aligned across Anthropic Messages, OpenAI Responses, OpenAI Chat
// Completions, and Gemini GenerateContent.

const (
	// Canonical protocol identifier strings match internal/protocol constants
	// so Endpoint and RecordedRequest report persisted protocol IDs directly.
	protocolAnthropicMessages     = "anthropic_messages"      // Anthropic Messages wire protocol
	protocolOpenAIResponses       = "openai_responses"        // OpenAI Responses wire protocol
	protocolOpenAIChatCompletions = "openai_chat_completions" // OpenAI Chat Completions wire protocol
	protocolGeminiGenerateContent = "gemini_generate_content" // Google Gemini GenerateContent wire protocol
)

// Endpoint describes the upstream wire protocol and path-encoded attributes
// recognized from an incoming HTTP request URL.
type Endpoint struct {
	Protocol       string // canonical protocol ID string
	PathModel      string // model segment extracted from Gemini paths, or empty
	StreamFromPath bool   // true when URL action or alt=sse selects SSE streaming
}

// RequestSummary holds top-level metadata and historical turn counts extracted
// from an encoded upstream request payload.
type RequestSummary struct {
	Model             string // model name from JSON body or Gemini URL path
	Stream            bool   // whether the request expects an SSE stream response
	HasTools          bool   // whether at least one tool declaration is present
	AssistantMessages int    // count of historical assistant turns in the request
	ToolResults       int    // count of historical tool result items in the request
}

// messagesInspectWire captures the top-level Anthropic Messages fields needed
// for lightweight wire-level turn inspection.
type messagesInspectWire struct {
	Model    string                   `json:"model"`    // requested Claude or compatible model
	Stream   bool                     `json:"stream"`   // whether SSE streaming was requested
	Tools    []jsontext.Value         `json:"tools"`    // declared function and server tools
	Messages []messagesInspectMessage `json:"messages"` // conversation turn list
}

// messagesInspectMessage captures one Anthropic Messages turn role and raw content.
type messagesInspectMessage struct {
	Role    string         `json:"role"`    // turn role such as "user" or "assistant"
	Content jsontext.Value `json:"content"` // string shorthand or content block array
}

// messagesInspectBlock captures the discriminator of one Messages content block.
type messagesInspectBlock struct {
	Type string `json:"type"` // content block type such as "text" or "tool_result"
}

// responsesInspectWire captures the top-level OpenAI Responses fields needed
// for lightweight wire-level turn inspection.
type responsesInspectWire struct {
	Model  string           `json:"model"`  // requested Responses model identifier
	Stream bool             `json:"stream"` // whether SSE streaming was requested
	Tools  []jsontext.Value `json:"tools"`  // declared function, custom, namespace, or hosted tools
	Input  jsontext.Value   `json:"input"`  // string shorthand or flat item array
}

// responsesInspectItem captures the type, role, and lifted tool list of one
// item inside a Responses input array.
type responsesInspectItem struct {
	Type  string           `json:"type"`  // item discriminator such as "message" or "function_call"
	Role  string           `json:"role"`  // message role when item represents a message
	Tools []jsontext.Value `json:"tools"` // tools carried on an additional_tools item
}

// chatInspectWire captures the top-level OpenAI Chat Completions fields needed
// for lightweight wire-level turn inspection.
type chatInspectWire struct {
	Model     string               `json:"model"`     // requested Chat model identifier
	Stream    bool                 `json:"stream"`    // whether SSE streaming was requested
	Tools     []jsontext.Value     `json:"tools"`     // modern function tool declarations
	Functions []jsontext.Value     `json:"functions"` // legacy function declarations
	Messages  []chatInspectMessage `json:"messages"`  // conversation message list
}

// chatInspectMessage captures the role of one Chat Completions message entry.
type chatInspectMessage struct {
	Role string `json:"role"` // message role such as "assistant" or "tool"
}

// geminiInspectWire captures the top-level Google Gemini GenerateContent fields
// needed for lightweight wire-level turn inspection.
type geminiInspectWire struct {
	Model    string                 `json:"model"`    // optional body model override when present
	Tools    []jsontext.Value       `json:"tools"`    // Gemini Tool objects containing declarations
	Contents []geminiInspectContent `json:"contents"` // user and model turn list
}

// geminiInspectContent captures one Gemini Content turn's role and parts array.
type geminiInspectContent struct {
	Role  string              `json:"role"`  // Gemini turn role ("user" or "model")
	Parts []geminiInspectPart `json:"parts"` // ordered turn parts
}

// geminiInspectPart captures optional functionResponse presence on a Gemini part.
type geminiInspectPart struct {
	FunctionResponse jsontext.Value `json:"functionResponse"` // tool execution result object when present
}

// IdentifyEndpoint recognizes an upstream generation protocol from urlPath and
// rawQuery, covering Messages, Responses, Chat Completions, and Gemini
// generateContent or streamGenerateContent paths. Token-counting and
// unrecognized paths return false so handlers respond with 404 without running
// generation request validators.
func IdentifyEndpoint(urlPath, rawQuery string) (Endpoint, bool) {
	cleanPath := strings.TrimRight(strings.TrimSpace(urlPath), "/")
	if cleanPath == "" {
		return Endpoint{}, false
	}

	switch {
	case cleanPath == "/v1/messages" || strings.HasSuffix(cleanPath, "/v1/messages") || cleanPath == "/messages":
		return Endpoint{Protocol: protocolAnthropicMessages}, true
	case cleanPath == "/v1/responses" || strings.HasSuffix(cleanPath, "/v1/responses") || cleanPath == "/responses" || strings.HasSuffix(cleanPath, "/codex/responses"):
		return Endpoint{Protocol: protocolOpenAIResponses}, true
	case cleanPath == "/v1/chat/completions" || strings.HasSuffix(cleanPath, "/v1/chat/completions") || cleanPath == "/chat/completions":
		return Endpoint{Protocol: protocolOpenAIChatCompletions}, true
	}

	_, tail, hasModels := strings.CutLast(cleanPath, "/models/")
	if !hasModels {
		return Endpoint{}, false
	}
	var rawModel string
	streamFromPath := false
	if stem, ok := strings.CutSuffix(tail, ":streamGenerateContent"); ok {
		rawModel = stem
		streamFromPath = true
	} else if stem, ok := strings.CutSuffix(tail, ":generateContent"); ok {
		rawModel = stem
	} else {
		return Endpoint{}, false
	}

	if rawModel == "" || strings.Contains(rawModel, "/") {
		return Endpoint{}, false
	}
	unescapedModel, err := url.PathUnescape(rawModel)
	if err != nil || strings.TrimSpace(unescapedModel) == "" {
		return Endpoint{}, false
	}
	if !streamFromPath && rawQuery != "" {
		// Ignore malformed raw query strings so valid model generation paths still
		// route to the Gemini request validator instead of failing with 404.
		if q, parseErr := url.ParseQuery(rawQuery); parseErr == nil && strings.EqualFold(q.Get("alt"), "sse") {
			streamFromPath = true
		}
	}
	return Endpoint{
		Protocol:       protocolGeminiGenerateContent,
		PathModel:      unescapedModel,
		StreamFromPath: streamFromPath,
	}, true
}

// DecodeAndSummarizeRequest decodes body into a canonical ir.Request when
// endpoint.Protocol has a registered codec in convert and derives its
// RequestSummary from the decoded IR turns. Unregistered protocol identifiers
// fall back to lightweight wire JSON inspection via InspectRequest with a nil
// ir.Request.
func DecodeAndSummarizeRequest(endpoint Endpoint, body []byte) (*ir.Request, RequestSummary, error) {
	if proto, ok := testkit.ParseProtocolDirName(endpoint.Protocol); ok && convert.HasClientCodec(proto) {
		req, err := convert.DecodeRequest(proto, body)
		if err != nil {
			return nil, RequestSummary{}, err
		}
		return req, summarizeDecodedRequest(endpoint, req), nil
	}
	summary, err := InspectRequest(endpoint, body)
	if err != nil {
		return nil, RequestSummary{}, err
	}
	return nil, summary, nil
}

// summarizeDecodedRequest extracts model, streaming mode, tool presence,
// historical assistant turn counts, and tool_result part counts from req.
func summarizeDecodedRequest(endpoint Endpoint, req *ir.Request) RequestSummary {
	if req == nil {
		return RequestSummary{
			Model:  strings.TrimPrefix(endpoint.PathModel, "models/"),
			Stream: endpoint.StreamFromPath,
		}
	}
	model := strings.TrimSpace(req.Model)
	if model == "" && endpoint.PathModel != "" {
		model = strings.TrimPrefix(endpoint.PathModel, "models/")
	}
	summary := RequestSummary{
		Model:    model,
		Stream:   req.Stream || endpoint.StreamFromPath,
		HasTools: len(req.Tools) > 0 || len(req.Namespaces) > 0,
	}
	for _, msg := range req.Messages {
		if msg.Role == ir.RoleAssistant {
			summary.AssistantMessages++
		}
		for _, p := range msg.Parts {
			if p.Kind == ir.PartToolResult {
				summary.ToolResults++
			}
		}
	}
	return summary
}

// InspectRequest extracts model, streaming mode, tool declaration presence,
// historical assistant turn count, and historical tool result count from body
// according to endpoint.Protocol. Malformed JSON payloads, duplicate object
// keys, or unrecognized protocol identifiers return an error.
func InspectRequest(endpoint Endpoint, body []byte) (RequestSummary, error) {
	switch endpoint.Protocol {
	case protocolAnthropicMessages:
		return inspectMessagesRequest(body)
	case protocolOpenAIResponses:
		return inspectResponsesRequest(body)
	case protocolOpenAIChatCompletions:
		return inspectChatRequest(body)
	case protocolGeminiGenerateContent:
		return inspectGeminiRequest(endpoint, body)
	default:
		return RequestSummary{}, fmt.Errorf("unsupported endpoint protocol %q", endpoint.Protocol)
	}
}

// inspectMessagesRequest extracts model, stream mode, tool presence, assistant
// turn counts, and user tool_result block counts from an Anthropic Messages body.
// Consecutive assistant messages coalesce into one turn, while string message
// contents skip block-array decoding without error.
func inspectMessagesRequest(body []byte) (RequestSummary, error) {
	var wire messagesInspectWire
	if err := json.Unmarshal(body, &wire, jsontext.AllowInvalidUTF8(true)); err != nil {
		return RequestSummary{}, fmt.Errorf("unmarshal Messages request: %w", err)
	}
	summary := RequestSummary{
		Model:    wire.Model,
		Stream:   wire.Stream,
		HasTools: len(wire.Tools) > 0,
	}
	prevAssistant := false
	for _, msg := range wire.Messages {
		if msg.Role == "assistant" {
			if !prevAssistant {
				summary.AssistantMessages++
				prevAssistant = true
			}
		} else {
			prevAssistant = false
		}
		if len(msg.Content) == 0 || msg.Content.Kind() != '[' {
			continue
		}
		var blocks []messagesInspectBlock
		if err := json.Unmarshal(msg.Content, &blocks, jsontext.AllowInvalidUTF8(true)); err != nil {
			return RequestSummary{}, fmt.Errorf("unmarshal Messages content array: %w", err)
		}
		for _, blk := range blocks {
			if blk.Type == "tool_result" {
				summary.ToolResults++
			}
		}
	}
	return summary, nil
}

// inspectResponsesRequest extracts turn summary fields from an OpenAI Responses
// JSON body while preserving turn-count parity with message-grouped protocols.
// Shorthand string inputs report zero historical assistant turns and zero tool
// results without allocating an item slice.
//
// Array inputs encode reasoning items, assistant messages, and tool calls as
// flat sibling entries rather than nested blocks. Contiguous assistant-owned
// items coalesce into one assistant turn, additional_tools entries mark
// HasTools when non-empty, and *_output items increment ToolResults while
// closing any active assistant turn group.
func inspectResponsesRequest(body []byte) (RequestSummary, error) {
	var wire responsesInspectWire
	if err := json.Unmarshal(body, &wire, jsontext.AllowInvalidUTF8(true)); err != nil {
		return RequestSummary{}, fmt.Errorf("unmarshal Responses request: %w", err)
	}
	summary := RequestSummary{
		Model:    wire.Model,
		Stream:   wire.Stream,
		HasTools: len(wire.Tools) > 0,
	}
	if len(wire.Input) == 0 || wire.Input.Kind() != '[' {
		return summary, nil
	}
	var items []responsesInspectItem
	if err := json.Unmarshal(wire.Input, &items, jsontext.AllowInvalidUTF8(true)); err != nil {
		return RequestSummary{}, fmt.Errorf("unmarshal Responses input array: %w", err)
	}

	// Responses encodes reasoning, assistant messages, and tool calls as flat
	// sibling items in input[]; coalesce contiguous assistant-owned items so one
	// model step counts as one assistant turn just like Messages, Chat, and Gemini.
	inAssistant := false
	for _, item := range items {
		typ := item.Type
		if typ == "" && item.Role != "" {
			typ = "message"
		}
		switch typ {
		case "additional_tools":
			if len(item.Tools) > 0 {
				summary.HasTools = true
			}
		case "function_call_output", "custom_tool_call_output", "tool_search_output", "tool_search_call_output",
			"local_shell_call_output", "shell_call_output", "computer_call_output", "mcp_tool_call_output":
			summary.ToolResults++
			inAssistant = false
		case "message":
			if item.Role == "assistant" {
				if !inAssistant {
					summary.AssistantMessages++
					inAssistant = true
				}
			} else {
				inAssistant = false
			}
		default:
			if isResponsesAssistantItem(typ) {
				if !inAssistant {
					summary.AssistantMessages++
					inAssistant = true
				}
			} else if strings.HasSuffix(typ, "_output") {
				inAssistant = false
			}
		}
	}
	return summary, nil
}

// isResponsesAssistantItem reports whether a non-message Responses input item
// type belongs to an assistant output step.
func isResponsesAssistantItem(itemType string) bool {
	switch itemType {
	case "reasoning", "function_call", "custom_tool_call", "tool_search_call",
		"web_search_call", "local_shell_call", "shell_call", "mcp_call", "mcp_list_tools",
		"mcp_approval_request", "image_generation_call", "code_interpreter_call",
		"file_search_call", "computer_call":
		return true
	default:
		return false
	}
}

// inspectChatRequest extracts model, stream mode, modern or legacy tool
// declarations, assistant message counts, and tool or function result counts
// from an OpenAI Chat Completions JSON body. Duplicate object keys or invalid
// JSON syntax return an unmarshal error.
func inspectChatRequest(body []byte) (RequestSummary, error) {
	var wire chatInspectWire
	if err := json.Unmarshal(body, &wire, jsontext.AllowInvalidUTF8(true)); err != nil {
		return RequestSummary{}, fmt.Errorf("unmarshal Chat request: %w", err)
	}
	summary := RequestSummary{
		Model:    wire.Model,
		Stream:   wire.Stream,
		HasTools: len(wire.Tools) > 0 || len(wire.Functions) > 0,
	}
	for _, msg := range wire.Messages {
		switch msg.Role {
		case "assistant":
			summary.AssistantMessages++
		case "tool", "function":
			summary.ToolResults++
		}
	}
	return summary, nil
}

// inspectGeminiRequest extracts turn summary fields from a Google Gemini
// GenerateContent JSON body combined with URL-derived model and stream flags.
// Body model strings take precedence when present, falling back to
// endpoint.PathModel with any leading "models/" prefix stripped.
func inspectGeminiRequest(endpoint Endpoint, body []byte) (RequestSummary, error) {
	var wire geminiInspectWire
	if err := json.Unmarshal(body, &wire, jsontext.AllowInvalidUTF8(true)); err != nil {
		return RequestSummary{}, fmt.Errorf("unmarshal Gemini request: %w", err)
	}
	model := strings.TrimPrefix(strings.TrimSpace(wire.Model), "models/")
	if model == "" {
		model = strings.TrimPrefix(endpoint.PathModel, "models/")
	}
	hasTools := slices.ContainsFunc(wire.Tools, geminiToolDeclaresAny)
	summary := RequestSummary{
		Model:    model,
		Stream:   endpoint.StreamFromPath,
		HasTools: hasTools,
	}
	prevModel := false
	for _, content := range wire.Contents {
		if content.Role == "model" {
			if !prevModel {
				summary.AssistantMessages++
				prevModel = true
			}
		} else {
			prevModel = false
		}
		for _, part := range content.Parts {
			if len(part.FunctionResponse) > 0 && part.FunctionResponse.Kind() == '{' {
				summary.ToolResults++
			}
		}
	}
	return summary, nil
}

// geminiToolDeclaresAny reports whether one Gemini tools[] element declares at
// least one functionDeclaration or non-null hosted tool member. Empty
// functionDeclarations arrays, non-object JSON values, and malformed tokens
// return false without aborting inspection.
func geminiToolDeclaresAny(raw jsontext.Value) bool {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		// Treat non-object or malformed tool entries as declaring no tools here;
		// strict request validation reports GEM-JSON violations on scripted paths.
		return false
	}
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return false
		}
		key := keyTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return false
		}
		if key == "functionDeclarations" {
			var decls []jsontext.Value
			if err := json.Unmarshal(val, &decls, jsontext.AllowInvalidUTF8(true)); err == nil && len(decls) > 0 {
				return true
			}
			continue
		}
		if val.Kind() != 'n' {
			return true
		}
	}
	return false
}
