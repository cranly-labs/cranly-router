package testkit

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// request_rules.go defines shared types, rule metadata registries, and protocol
// dispatchers for strict wire-request validation (section 5.2.1). Both
// mockupstream and conversion law tests (Q3 and M1) run these validators
// directly over encoded JSON bytes to catch requests that real providers would
// reject with HTTP 400.

var (
	// openAIToolNamePattern enforces the ^[a-zA-Z0-9_-]{1,64}$ function and
	// custom tool identifier rule shared by OpenAI Chat and Responses APIs.
	openAIToolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

	// messagesToolNamePattern enforces Anthropic Messages' ^[a-zA-Z0-9_-]{1,128}$
	// tool name syntax before any optional tighter ToolNameMaxBytes cap.
	messagesToolNamePattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

	// geminiToolNamePattern enforces Google Gemini FunctionDeclaration naming:
	// leading letter or underscore followed by up to 63 [a-zA-Z0-9_.:-] characters.
	geminiToolNamePattern = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.:-]{0,63}$`)

	// requestRuleCatalog enumerates every registered strict request rule with its
	// authoritative specification URL or repository regression commit.
	requestRuleCatalog = buildRequestRuleCatalog()
)

// RequestRuleSpec documents one strict request validation rule, its owning
// protocol, its check summary, and its upstream specification or issue source.
type RequestRuleSpec struct {
	RuleID      string              `json:"rule_id"`     // stable rule code such as "MSG-TOOL-PAIRING"
	Protocol    protocol.ProtocolID `json:"protocol"`    // owning wire protocol
	Description string              `json:"description"` // concise invariant statement
	Reference   string              `json:"reference"`   // official API doc URL or commit/issue ID
}

// RuleViolation reports one strict request rule failure at a JSON path.
type RuleViolation struct {
	RuleID    string              `json:"rule_id"`   // violated rule code
	Protocol  protocol.ProtocolID `json:"protocol"`  // validated wire protocol
	Path      string              `json:"path"`      // JSONPath location in request body
	Message   string              `json:"message"`   // human-readable diagnostic message
	Reference string              `json:"reference"` // specification URL or commit reference
}

// RequestValidationOptions configures strictness and provider dialect rules when
// validating an encoded upstream request body.
type RequestValidationOptions struct {
	// StrictTopLevel rejects undocumented top-level JSON members on protocols
	// such as Anthropic Messages that return 400 on extra inputs.
	StrictTopLevel bool
	// DialectRules enables endpoint-specific validation checks such as
	// "deepseek-thinking", "passback", "function-only", "gemini-signature", and
	// "antigravity-schema"; omitted names leave those dialect checks inactive.
	DialectRules []string
	// ToolNameMaxBytes tightens the protocol's default maximum tool-name byte
	// length when positive, while zero or negative values retain the protocol default.
	ToolNameMaxBytes int
}

// String formats v as a single-line diagnostic including rule ID, path, message, and source reference.
func (v RuleViolation) String() string {
	return fmt.Sprintf("[%s/%s] %s: %s (ref: %s)", v.Protocol, v.RuleID, v.Path, v.Message, v.Reference)
}

// HasDialect reports whether opts.DialectRules includes the named dialect rule.
func (opts RequestValidationOptions) HasDialect(rule string) bool {
	return slices.Contains(opts.DialectRules, rule)
}

// RegisteredRequestRules returns a copy of all registered strict request rules.
func RegisteredRequestRules() []RequestRuleSpec {
	return slices.Clone(requestRuleCatalog)
}

// ValidateRequestRules runs the strict wire-request validator for proto over raw
// JSON bytes under opts and returns all detected violations in scan order.
// Unsupported protocol identifiers return a REQ-UNSUPPORTED-PROTOCOL violation.
func ValidateRequestRules(proto protocol.ProtocolID, body []byte, opts RequestValidationOptions) []RuleViolation {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return ValidateMessagesRequest(body, opts)
	case protocol.ProtocolOpenAIResponses:
		return ValidateResponsesRequest(body, opts)
	case protocol.ProtocolOpenAIChatCompletions:
		return ValidateChatRequest(body, opts)
	case protocol.ProtocolGeminiGenerateContent:
		return ValidateGeminiRequest(body, opts)
	default:
		return []RuleViolation{{
			RuleID:    "REQ-UNSUPPORTED-PROTOCOL",
			Protocol:  proto,
			Path:      "$",
			Message:   fmt.Sprintf("no strict request validator registered for protocol %q", proto),
			Reference: "protocol-conversion-ir-refactor.md#5.2.1",
		}}
	}
}

// parseRequestJSONRoot decodes body as a single JSON object node preserving both
// member map lookups and wire member appearance order. Strings containing
// invalid UTF-8 bytes remain accepted while duplicate object keys fail.
func parseRequestJSONRoot(body []byte) (jsonNode, error) {
	node, err := parseJSONNode(body, true)
	if err != nil {
		return jsonNode{}, err
	}
	if node.kind != nodeObject {
		return jsonNode{}, fmt.Errorf("request body must be a JSON object")
	}
	return node, nil
}

// parseRequestJSONObject parses body as a single JSON object without duplicate
// keys while tolerating invalid UTF-8 sequences inside strings.
func parseRequestJSONObject(body []byte) (map[string]jsonNode, error) {
	node, err := parseRequestJSONRoot(body)
	if err != nil {
		return nil, err
	}
	return node.objVal, nil
}

// orderedNodeKeys returns n.objKeys in wire appearance order when populated
// during JSON decoding. Synthesized in-memory nodes fall back to sorted keys.
func orderedNodeKeys(n jsonNode) []string {
	if len(n.objKeys) == len(n.objVal) {
		return n.objKeys
	}
	keys := make([]string, 0, len(n.objVal))
	for k := range n.objVal {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// ruleReference looks up the registered Reference string for ruleID.
func ruleReference(ruleID string) string {
	for _, spec := range requestRuleCatalog {
		if spec.RuleID == ruleID {
			return spec.Reference
		}
	}
	return "protocol-conversion-ir-refactor.md#5.2.1"
}

// buildRequestRuleCatalog initializes the complete strict request rule metadata table.
func buildRequestRuleCatalog() []RequestRuleSpec {
	return []RequestRuleSpec{
		{RuleID: "MSG-JSON", Protocol: protocol.ProtocolAnthropicMessages, Description: "Request body is a valid JSON object without duplicate keys and with non-empty model and messages.", Reference: "https://docs.anthropic.com/en/api/messages"},
		{RuleID: "MSG-MAX-TOKENS", Protocol: protocol.ProtocolAnthropicMessages, Description: "max_tokens is positive and exceeds thinking.budget_tokens when thinking is enabled.", Reference: "https://docs.anthropic.com/en/docs/build-with-claude/extended-thinking"},
		{RuleID: "MSG-ROLES", Protocol: protocol.ProtocolAnthropicMessages, Description: "messages roles are strictly alternating user and assistant turns.", Reference: "https://docs.anthropic.com/en/api/messages"},
		{RuleID: "MSG-EMPTY-TEXT", Protocol: protocol.ProtocolAnthropicMessages, Description: "system and message content contain no empty strings or empty text blocks.", Reference: "https://docs.anthropic.com/en/api/messages"},
		{RuleID: "MSG-TOOL-PAIRING", Protocol: protocol.ProtocolAnthropicMessages, Description: "Every assistant tool_use matches a tool_result in the immediately following user message and vice versa.", Reference: "https://docs.anthropic.com/en/docs/build-with-claude/tool-use"},
		{RuleID: "MSG-TOOL-RESULT-ORDER", Protocol: protocol.ProtocolAnthropicMessages, Description: "All tool_result blocks precede text or media blocks inside a user message.", Reference: "https://docs.anthropic.com/en/docs/build-with-claude/tool-use"},
		{RuleID: "MSG-THINKING", Protocol: protocol.ProtocolAnthropicMessages, Description: "Historical thinking blocks carry non-empty signatures (unless passback dialect), precede tool_use blocks when thinking is active, and output_config.effort uses a documented Anthropic tier.", Reference: "https://docs.anthropic.com/en/docs/build-with-claude/extended-thinking (commit 057437f5e)"},
		{RuleID: "MSG-TOOL-NAME", Protocol: protocol.ProtocolAnthropicMessages, Description: "Tool names match ^[a-zA-Z0-9_-]{1,128}$ and function tools carry object input_schema.", Reference: "https://docs.anthropic.com/en/docs/build-with-claude/tool-use"},
		{RuleID: "MSG-STRICT-TOP-LEVEL", Protocol: protocol.ProtocolAnthropicMessages, Description: "Strict mode rejects unknown top-level request members.", Reference: "https://docs.anthropic.com/en/api/messages"},

		{RuleID: "RSP-JSON", Protocol: protocol.ProtocolOpenAIResponses, Description: "Request body is a valid JSON object with non-empty model and string or array input.", Reference: "https://platform.openai.com/docs/api-reference/responses/create"},
		{RuleID: "RSP-CALL-PAIRING", Protocol: protocol.ProtocolOpenAIResponses, Description: "Every call item in input has a matching output item with identical call_id and kind, and vice versa.", Reference: "OpenAI Codex validate_request_body_invariants"},
		{RuleID: "RSP-REASONING-FOLLOWER", Protocol: protocol.ProtocolOpenAIResponses, Description: "Every reasoning item in input is followed by its associated assistant output or tool call item.", Reference: "https://platform.openai.com/docs/guides/reasoning"},
		{RuleID: "RSP-TOOL-NAME", Protocol: protocol.ProtocolOpenAIResponses, Description: "Function and custom tool names match ^[a-zA-Z0-9_-]{1,64}$ and function_call arguments are valid JSON.", Reference: "https://platform.openai.com/docs/api-reference/responses/create"},
		{RuleID: "RSP-DIALECT-FUNCTION-ONLY", Protocol: protocol.ProtocolOpenAIResponses, Description: "Function-only dialect forbids unlowered custom, namespace, or tool_search declarations and items.", Reference: "protocol-conversion-ir-refactor.md#3.11"},

		{RuleID: "CHAT-JSON", Protocol: protocol.ProtocolOpenAIChatCompletions, Description: "Request body is a valid JSON object with non-empty model and messages array.", Reference: "https://platform.openai.com/docs/api-reference/chat/create"},
		{RuleID: "CHAT-TOOL-PAIRING", Protocol: protocol.ProtocolOpenAIChatCompletions, Description: "Assistant tool_calls are immediately followed by matching tool messages for every call ID before the next non-tool turn.", Reference: "https://platform.openai.com/docs/guides/function-calling"},
		{RuleID: "CHAT-ASSISTANT-BODY", Protocol: protocol.ProtocolOpenAIChatCompletions, Description: "Assistant messages contain non-empty content, refusal, tool_calls, or function_call.", Reference: "https://platform.openai.com/docs/api-reference/chat/create"},
		{RuleID: "CHAT-TOOL-ARGS-JSON", Protocol: protocol.ProtocolOpenAIChatCompletions, Description: "Every assistant tool_call and function_call arguments string is valid JSON.", Reference: "https://platform.openai.com/docs/guides/function-calling"},
		{RuleID: "CHAT-TOOL-NAME", Protocol: protocol.ProtocolOpenAIChatCompletions, Description: "Function names match ^[a-zA-Z0-9_-]{1,64}$.", Reference: "https://platform.openai.com/docs/api-reference/chat/create"},
		{RuleID: "CHAT-DEEPSEEK-THINKING", Protocol: protocol.ProtocolOpenAIChatCompletions, Description: "Under deepseek-thinking dialect, assistant messages with tool_calls include reasoning_content and assistant messages without tool_calls omit reasoning_content.", Reference: "https://api-docs.deepseek.com/guides/thinking_mode#tool-calls (commit b567a28f9)"},

		{RuleID: "GEM-JSON", Protocol: protocol.ProtocolGeminiGenerateContent, Description: "Request body is a valid JSON object with non-empty contents using only user and model roles.", Reference: "https://ai.google.dev/api/generate-content"},
		{RuleID: "GEM-FUNC-PAIRING", Protocol: protocol.ProtocolGeminiGenerateContent, Description: "Every functionResponse matches a functionCall in the preceding model turn and vice versa, with object args and response.", Reference: "https://ai.google.dev/gemini-api/docs/function-calling"},
		{RuleID: "GEM-TOOL-NAME", Protocol: protocol.ProtocolGeminiGenerateContent, Description: "Function names match ^[a-zA-Z_][a-zA-Z0-9_.:-]{0,63}$.", Reference: "https://ai.google.dev/api/caching#FunctionDeclaration"},
		{RuleID: "GEM-SIGNATURE", Protocol: protocol.ProtocolGeminiGenerateContent, Description: "Under gemini-signature dialect, the first functionCall of each model step carries thoughtSignature.", Reference: "https://ai.google.dev/gemini-api/docs/thinking#signatures (commit 4b05d79a5)"},
		{RuleID: "GEM-SCHEMA", Protocol: protocol.ProtocolGeminiGenerateContent, Description: "Function parameter schemas exclude keywords unsupported by the target Gemini or Antigravity schema dialect.", Reference: "https://ai.google.dev/api/caching#Schema"},
		{RuleID: "GEM-THINKING-BUDGET", Protocol: protocol.ProtocolGeminiGenerateContent, Description: "Positive generationConfig.maxOutputTokens exceeds positive thinkingConfig.thinkingBudget.", Reference: "https://ai.google.dev/gemini-api/docs/thinking"},
	}
}
