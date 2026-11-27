package chat

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// request_encode.go serializes normalized ir.Request values into deterministic
// OpenAI Chat Completions JSON request bytes under target Dialect capabilities.
// Outgoing requests normalize legacy max_tokens into max_completion_tokens,
// stop into string arrays, and reasoning settings into top-level reasoning_effort.
//
// Target endpoints vary in tool categories, sampling parameters, and structured
// output naming rules, so request_encode.go lowers custom, client_search, and
// namespaced tools into 64-byte function declarations, filters tool_choice
// references against surviving declared tools, and synthesizes fallback schema
// names when missing. Every dropped extension field, omitted parameter, or
// approximated setting emits a matching ir.Loss entry for Law Q4 verification.

// wireNamedFunctionRef serializes the nested function name object inside Chat tool_choice selectors.
type wireNamedFunctionRef struct {
	Name string `json:"name"` // target Chat function identifier
}

// wireFunctionToolChoiceWrite serializes a Chat tool_choice object forcing invocation of one named function.
type wireFunctionToolChoiceWrite struct {
	Type     string               `json:"type"`     // literal "function" selector discriminator
	Function wireNamedFunctionRef `json:"function"` // selected function reference object
}

// wireAllowedToolsBodyWrite serializes the mode and restricted tool list inside Chat allowed_tools selectors.
type wireAllowedToolsBodyWrite struct {
	Mode  string                 `json:"mode"`  // "auto" or "required"
	Tools []wireAllowedToolEntry `json:"tools"` // restricted function subset list
}

// wireAllowedToolsChoiceWrite serializes an outgoing Chat tool_choice envelope restricting calls to allowed_tools.
type wireAllowedToolsChoiceWrite struct {
	Type         string                    `json:"type"`          // literal "allowed_tools" selector discriminator
	AllowedTools wireAllowedToolsBodyWrite `json:"allowed_tools"` // nested selector configuration object
}

// EncodeRequest serializes req into deterministic OpenAI Chat Completions JSON
// bytes according to dialect and returns all incurred information-loss records.
// Nil requests or unsupported "ultra" reasoning efforts return an error.
//
// Message legalization, tool lowering, tool_choice reconciliation, and sampling
// or stop-sequence dialect drops all record original-coordinate ir.Loss values
// while leaving req untouched so retry attempts can re-encode safely.
func EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	if req == nil {
		return nil, nil, errors.New("encode chat request: nil request")
	}

	var losses []ir.Loss
	effort, err := resolveChatReasoningEffort(req.Reasoning, dialect)
	if err != nil {
		return nil, nil, err
	}

	messages, err := legalizeMessages(req, dialect, &losses)
	if err != nil {
		return nil, nil, err
	}
	if messages == nil {
		messages = []wireOutMessage{}
	}

	maxNameBytes := defaultChatToolNameMaxBytes
	if dialect.ToolNameMaxBytes > 0 {
		maxNameBytes = dialect.ToolNameMaxBytes
	}
	tools, declaredNames, err := encodeChatTools(req.Tools, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}

	toolChoice, err := encodeChatToolChoice(req.ToolChoice, len(req.Tools) > 0, declaredNames, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}

	out := wireOutRequest{
		Model:               req.Model,
		Messages:            messages,
		Tools:               tools,
		ToolChoice:          toolChoice,
		ParallelToolCalls:   req.ParallelToolCalls,
		MaxCompletionTokens: req.MaxOutputTokens,
		Seed:                req.Sampling.Seed,
		FrequencyPenalty:    req.Sampling.FrequencyPenalty,
		PresencePenalty:     req.Sampling.PresencePenalty,
		N:                   req.Sampling.CandidateCount,
		ReasoningEffort:     effort,
		Stream:              req.Stream,
		ServiceTier:         req.ServiceTier,
		PromptCacheKey:      req.CacheKey,
		User:                req.UserID,
		SessionID:           req.SessionID,
		ConversationID:      req.ConversationID,
		Store:               req.Store,
		Unknown:             restoreOrDropExtra(req.Extra, "", &losses),
	}

	if req.Sampling.Temperature != nil {
		if dialect.DropTemperature {
			losses = append(losses, ir.Loss{
				Kind:   ir.LossSamplingDropped,
				Path:   "sampling.temperature",
				Detail: "temperature",
			})
		} else {
			out.Temperature = req.Sampling.Temperature
		}
	}
	if req.Sampling.TopP != nil {
		if dialect.DropTopP {
			losses = append(losses, ir.Loss{
				Kind:   ir.LossSamplingDropped,
				Path:   "sampling.top_p",
				Detail: "top_p",
			})
		} else {
			out.TopP = req.Sampling.TopP
		}
	}
	// Chat Completions has no top_k parameter; always report sampling_dropped when present.
	if req.Sampling.TopK != nil {
		losses = append(losses, ir.Loss{
			Kind:   ir.LossSamplingDropped,
			Path:   "sampling.top_k",
			Detail: "top_k",
		})
	}

	if len(req.Stop) > 0 {
		if dialect.OmitStopSequences {
			losses = append(losses, ir.Loss{
				Kind: ir.LossStopDropped,
				Path: "stop",
			})
		} else {
			out.Stop = req.Stop
		}
	}

	if req.Output != nil {
		out.ResponseFormat = encodeResponseFormat(req.Output, &losses)
	}
	if req.IncludeUsage != nil {
		inc := *req.IncludeUsage
		out.StreamOptions = &wireStreamOptions{IncludeUsage: &inc}
	}
	if req.MetadataUserID != "" {
		out.Metadata = &wireMetadata{UserID: req.MetadataUserID}
	}

	encoded, err := marshalDeterministic(out)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal chat request: %w", err)
	}
	return encoded, losses, nil
}

// EncodeRequest serializes req into deterministic OpenAI Chat Completions JSON
// bytes under dialect by delegating to the package-level EncodeRequest. Nil
// requests or unsupported reasoning effort values return an error without mutating req.
func (Codec) EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	return EncodeRequest(req, dialect)
}

// resolveChatReasoningEffort extracts and validates the outgoing reasoning_effort
// string under dialect. Nil configs and StripEffort return "", whereas "ultra"
// fails immediately because no Chat endpoint accepts that effort tier.
func resolveChatReasoningEffort(rc *ir.ReasoningConfig, dialect ir.Dialect) (string, error) {
	if rc == nil || dialect.StripEffort {
		return "", nil
	}
	effort := strings.TrimSpace(rc.Effort)
	if effort == "" {
		return "", nil
	}
	if strings.EqualFold(effort, "ultra") {
		return "", fmt.Errorf("reasoning effort %q is not supported", effort)
	}
	if normalized := protocol.NormalizeReasoningEffort(effort); normalized != "" {
		return normalized, nil
	}
	return effort, nil
}

// encodeResponseFormat converts out into a Chat wireResponseFormat. When a
// cross-protocol json_schema format omits Name or sets Verbosity, the encoder
// synthesizes defaultResponseSchemaName and records output_format_approximated.
func encodeResponseFormat(out *ir.OutputFormat, losses *[]ir.Loss) *wireResponseFormat {
	if out.Verbosity != "" {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output.verbosity",
			Detail: "verbosity",
		})
	}
	switch out.Kind {
	case "":
		return nil
	case "text":
		return &wireResponseFormat{Type: "text"}
	case "json_object":
		return &wireResponseFormat{Type: "json_object"}
	case "json_schema":
		name := out.Name
		if strings.TrimSpace(name) == "" {
			name = defaultResponseSchemaName
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossOutputFormatApproximated,
				Path:   "output",
				Detail: "name",
			})
		}
		var schema jsontext.Value
		if len(out.Schema) > 0 {
			schema = jsontext.Value(bytes.Clone(out.Schema))
		}
		return &wireResponseFormat{
			Type: "json_schema",
			JSONSchema: &wireJSONSchemaSpec{
				Name:        name,
				Description: out.Description,
				Schema:      schema,
				Strict:      out.Strict,
			},
		}
	default:
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossOutputFormatApproximated,
			Path:   "output",
			Detail: out.Kind,
		})
		return nil
	}
}

// encodeChatTools serializes req.Tools into Chat tools[] JSON items and returns
// the set of declared wire tool names alongside any lowering or drop losses.
//
// Same-origin hosted tools pass through as raw bytes while foreign hosted tools
// drop with LossHostedToolDropped. Custom tools wrap raw text inside a single
// {"input": "..."} function schema, multiple ToolClientSearch entries deduplicate
// into one tool_search function proxy, and scoped namespaces flatten within
// maxNameBytes.
func encodeChatTools(tools []ir.Tool, maxNameBytes int, losses *[]ir.Loss) ([]jsontext.Value, map[string]bool, error) {
	if len(tools) == 0 {
		return nil, nil, nil
	}
	var out []jsontext.Value
	declared := make(map[string]bool, len(tools))
	searchEmitted := false

	for i, t := range tools {
		tPath := fmt.Sprintf("tools[%d]", i)
		if t.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tPath})
		}

		switch t.Kind {
		case ir.ToolHosted:
			_ = restoreOrDropExtra(t.Extra, tPath, losses)
			if t.Hosted != nil && t.Hosted.Origin == protocol.ProtocolOpenAIChatCompletions {
				out = append(out, jsontext.Value(bytes.Clone(t.Hosted.Raw)))
				if t.Name != "" {
					declared[t.Name] = true
				}
				if t.Hosted.Type != "" {
					declared[t.Hosted.Type] = true
				}
			} else {
				detail := t.Name
				if t.Hosted != nil && t.Hosted.Type != "" {
					detail = t.Hosted.Type
				}
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossHostedToolDropped,
					Path:   tPath,
					Detail: detail,
				})
			}

		case ir.ToolFunction:
			unknown := restoreOrDropExtra(t.Extra, tPath, losses)
			name := t.Name
			lowered := t.DeferLoading != nil
			if t.Namespace != "" {
				name = flattenChatToolName(t.Namespace, name, maxNameBytes)
				if t.Namespace != "functions" {
					lowered = true
				}
			}
			if lowered {
				*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			}
			var schema jsontext.Value
			if len(t.InputSchema) > 0 {
				schema = jsontext.Value(bytes.Clone(t.InputSchema))
			}
			rawTool, err := marshalDeterministic(wireTool{
				Type: "function",
				Function: &wireFunction{
					Name:        name,
					Description: t.Description,
					Parameters:  schema,
					Strict:      t.Strict,
				},
				Unknown: unknown,
			})
			if err != nil {
				return nil, nil, err
			}
			declared[name] = true
			out = append(out, jsontext.Value(rawTool))

		case ir.ToolCustom:
			unknown := restoreOrDropExtra(t.Extra, tPath, losses)
			name := t.Name
			if t.Namespace != "" {
				name = flattenChatToolName(t.Namespace, name, maxNameBytes)
			}
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			rawTool, err := marshalDeterministic(wireTool{
				Type: "function",
				Function: &wireFunction{
					Name:        name,
					Description: t.Description,
					Parameters:  jsontext.Value(customToolInputSchemaJSON),
				},
				Unknown: unknown,
			})
			if err != nil {
				return nil, nil, err
			}
			declared[name] = true
			out = append(out, jsontext.Value(rawTool))

		case ir.ToolClientSearch:
			_ = restoreOrDropExtra(t.Extra, tPath, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			if searchEmitted {
				continue
			}
			searchEmitted = true
			rawTool, err := marshalDeterministic(wireTool{
				Type: "function",
				Function: &wireFunction{
					Name:        toolSearchProxyName,
					Description: "Search and load tools, plugins, connectors, and MCP namespaces for the current task.",
					Parameters:  jsontext.Value(toolSearchProxySchemaJSON),
				},
			})
			if err != nil {
				return nil, nil, err
			}
			declared[toolSearchProxyName] = true
			out = append(out, jsontext.Value(rawTool))
		}
	}

	if len(out) == 0 {
		return nil, declared, nil
	}
	return out, declared, nil
}

// encodeChatToolChoice serializes tc into Chat's scalar, function-object, or
// allowed_tools wire shape while reconciling references against surviving tools.
//
// When every source tool was dropped during encoding, non-"none" selectors omit
// tool_choice and emit LossToolChoiceApproximated so Chat upstreams never see a
// dangling tool_choice without tools[]. Allowed-tool lists filter out undeclared
// entries and fall back to their scalar mode when no allowed entry survives.
func encodeChatToolChoice(
	tc *ir.ToolChoice,
	hadSourceTools bool,
	declared map[string]bool,
	maxNameBytes int,
	losses *[]ir.Loss,
) (jsontext.Value, error) {
	if tc == nil {
		return nil, nil
	}
	if hadSourceTools && len(declared) == 0 && tc.Mode != ir.ToolChoiceNone {
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossToolChoiceApproximated,
			Path: "tool_choice",
		})
		return nil, nil
	}

	if len(tc.AllowedTools) > 0 && (tc.Mode == ir.ToolChoiceAuto || tc.Mode == ir.ToolChoiceRequired) {
		var entries []wireAllowedToolEntry
		approximated := false
		for _, ref := range tc.AllowedTools {
			name := resolveChoiceToolName(ref, maxNameBytes)
			if isLoweredToolRef(ref) {
				approximated = true
			}
			if len(declared) > 0 && !declared[name] {
				approximated = true
				continue
			}
			entries = append(entries, wireAllowedToolEntry{
				Type: "function",
				Function: &struct {
					Name string `json:"name"` // nested function name
				}{Name: name},
			})
		}
		if approximated {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossToolChoiceApproximated,
				Path: "tool_choice",
			})
		}
		if len(entries) == 0 {
			rawMode, err := marshalDeterministic(string(tc.Mode))
			return jsontext.Value(rawMode), err
		}
		rawAllowed, err := marshalDeterministic(wireAllowedToolsChoiceWrite{
			Type: "allowed_tools",
			AllowedTools: wireAllowedToolsBodyWrite{
				Mode:  string(tc.Mode),
				Tools: entries,
			},
		})
		return jsontext.Value(rawAllowed), err
	}

	switch tc.Mode {
	case ir.ToolChoiceAuto, ir.ToolChoiceNone, ir.ToolChoiceRequired:
		raw, err := marshalDeterministic(string(tc.Mode))
		return jsontext.Value(raw), err

	case ir.ToolChoiceTool:
		if tc.Tool.Kind == ir.ToolHosted {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossToolChoiceApproximated,
				Path: "tool_choice",
			})
			return nil, nil
		}
		name := resolveChoiceToolName(tc.Tool, maxNameBytes)
		if isLoweredToolRef(tc.Tool) {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossToolChoiceApproximated,
				Path: "tool_choice",
			})
		}
		if len(declared) > 0 && !declared[name] {
			*losses = append(*losses, ir.Loss{
				Kind: ir.LossToolChoiceApproximated,
				Path: "tool_choice",
			})
			return nil, nil
		}
		raw, err := marshalDeterministic(wireFunctionToolChoiceWrite{
			Type:     "function",
			Function: wireNamedFunctionRef{Name: name},
		})
		return jsontext.Value(raw), err

	default:
		return nil, fmt.Errorf("unsupported tool_choice mode %q", tc.Mode)
	}
}

// resolveChoiceToolName maps an ir.ToolRef in ToolChoice onto its Chat function
// name. Client-search references resolve to toolSearchProxyName, and scoped
// namespaces flatten within maxNameBytes.
func resolveChoiceToolName(ref ir.ToolRef, maxNameBytes int) string {
	if ref.Kind == ir.ToolClientSearch {
		return toolSearchProxyName
	}
	if ref.Namespace != "" {
		return flattenChatToolName(ref.Namespace, ref.Name, maxNameBytes)
	}
	return ref.Name
}
