package antigravity

import (
	"bytes"
	"context"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
)

// probe.go builds Claude and Gemini probes and executes them through
// Antigravity retries.
// Antigravity probes send minimal Claude or Gemini data through the same
// project envelope and provider-local retry adapter as forwarding. A nonblank
// first supplied prompt selects probe text, and response extraction joins
// first-candidate text from individual JSON/SSE lines.
//
// Optional lifecycle registration remains held through response closing, while
// the caller read limit retains a bounded prefix. Truncation can succeed
// without an oversize error, and extraction skips undecodable lines. Provider
// switching becomes a fixed test message; the probe owner decides health
// updates and model qualification from this technical result.

// TestConnectionResult holds extracted probe text and the caller-selected
// mapped model identity.
type TestConnectionResult struct {
	Text        string // extracted probe response text
	MappedModel string // model used by the probe
}

// BuildGeminiTestRequest wraps a one-output-token Gemini probe with its
// identity instruction. A blank or absent first prompt uses a dot, later
// prompts are ignored, and wrapping requires a trimmed project.
func BuildGeminiTestRequest(projectID, model string, prompts ...string) ([]byte, error) {
	prompt := "."
	if len(prompts) > 0 && strings.TrimSpace(prompts[0]) != "" {
		prompt = strings.TrimSpace(prompts[0])
	}
	payload := map[string]any{
		"contents": []map[string]any{
			{
				"role": "user",
				"parts": []map[string]any{
					{"text": prompt},
				},
			},
		},
		// Include the default platform identity before the minimal probe contents so
		// probe conversion uses the instruction selected by ordinary forwarding.
		"systemInstruction": map[string]any{
			"parts": []map[string]any{
				{"text": GetDefaultIdentityPatch()},
			},
		},
		"generationConfig": map[string]any{
			// One output token bounds generation requested by connection probes.
			"maxOutputTokens": 1,
		},
	}
	// Fixed maps and string/numeric values form this probe; encoding cannot
	// encounter unsupported values or a reference cycle before project wrapping.
	payloadBytes, _ := json.Marshal(payload)
	return WrapV1InternalRequest(projectID, model, payloadBytes)
}

// BuildClaudeTestRequest converts a one-token Anthropic Messages probe into a
// v1internal Antigravity Gemini request through protocol/convert,
// InjectIdentityPatchToGeminiRequestWithPrompt, CleanGeminiRequest, and
// WrapV1InternalRequest. Blank or absent first prompts default to a dot while
// later prompts are ignored.
func BuildClaudeTestRequest(projectID, mappedModel string, prompts ...string) ([]byte, error) {
	prompt := "."
	if len(prompts) > 0 && strings.TrimSpace(prompts[0]) != "" {
		prompt = strings.TrimSpace(prompts[0])
	}

	// claudeProbeMessage encodes the single user turn inside BuildClaudeTestRequest.
	type claudeProbeMessage struct {
		Role    string `json:"role"`    // probe message role ("user")
		Content string `json:"content"` // trimmed probe prompt string
	}
	// claudeProbeRequest encodes a minimal buffered Anthropic Messages probe payload.
	type claudeProbeRequest struct {
		Model     string               `json:"model"`      // mapped probe model identifier
		MaxTokens int                  `json:"max_tokens"` // one-token probe output cap
		Stream    bool                 `json:"stream"`     // buffered probe transport flag
		Messages  []claudeProbeMessage `json:"messages"`   // single-turn user probe history
	}

	clientBytes, err := jsonv2.Marshal(claudeProbeRequest{
		Model:     mappedModel,
		MaxTokens: 1,
		Stream:    false,
		Messages: []claudeProbeMessage{{
			Role:    "user",
			Content: prompt,
		}},
	})
	if err != nil {
		return nil, fmt.Errorf("marshal claude probe request: %w", err)
	}

	thinking := false
	resolved := (modelrules.View{}).Resolve(
		modelrules.Target{Platform: "antigravity", Model: mappedModel},
		modelcatalog.Attributes{},
	).ForRequest(modelrules.RequestState{Thinking: &thinking})

	lowerModel := strings.ToLower(strings.TrimSpace(mappedModel))
	isClaude := strings.HasPrefix(lowerModel, "claude-")
	replayScheme := ""
	placeholderToken := ""
	switch {
	case isClaude:
		replayScheme = ir.SchemeAntigravityClaude
	case strings.HasPrefix(lowerModel, "gemini-"):
		replayScheme = ir.SchemeGemini
		placeholderToken = gemini.DummyThoughtSignature
	}

	outputLimit := modelrules.AntigravityDefaultOutputLimit
	if resolved.Attributes.OutputLimit != nil && *resolved.Attributes.OutputLimit > 0 {
		outputLimit = min(outputLimit, *resolved.Attributes.OutputLimit)
	}
	var budgetMax int
	if _, budget := resolved.Attributes.ReasoningBudget(); budget != nil && *budget > 0 {
		budgetMax = *budget
	}

	dialect := convert.Dialect{
		Name:              "antigravity",
		ReplayScheme:      replayScheme,
		SchemaDialect:     convert.SchemaAntigravity,
		PlaceholderToken:  placeholderToken,
		OmitStopSequences: resolved.Rules.OmitStopSequences,
		DropTemperature:   resolved.Attributes.Temperature != nil && !*resolved.Attributes.Temperature,
		DropTopP:          resolved.Attributes.TopP != nil && !*resolved.Attributes.TopP,
		DropTopK:          slices.Contains(resolved.Rules.DropFields, "top_k"),
		Reasoning: convert.ReasoningCapabilities{
			ForceBudget:    isClaude,
			BudgetMax:      budgetMax,
			AdaptiveBudget: resolved.Rules.AdaptiveThinkingBudget,
			OutputLimit:    &outputLimit,
			BudgetPadding:  resolved.Rules.BudgetPadding,
		},
	}

	exchange, err := convert.NewExchange(clientBytes, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect:  dialect,
		},
		ClientModel: mappedModel,
		Runtime:     convert.DefaultRuntime(),
	})
	if err != nil {
		return nil, fmt.Errorf("convert claude probe request: %w", err)
	}
	rawGeminiBody, err := exchange.UpstreamRequest()
	if err != nil {
		return nil, fmt.Errorf("encode claude probe request: %w", err)
	}
	patched, err := InjectIdentityPatchToGeminiRequestWithPrompt(rawGeminiBody, "")
	if err != nil {
		return nil, fmt.Errorf("inject claude probe identity patch: %w", err)
	}
	// Keep the identity-patched Gemini bytes emitted by codec/generatecontent when
	// CleanGeminiRequest fails so probe envelope wrapping still succeeds without
	// aborting connection diagnostics.
	if cleaned, cleanErr := CleanGeminiRequest(patched); cleanErr == nil {
		patched = cleaned
	}
	return WrapV1InternalRequest(projectID, mappedModel, patched)
}

// ExtractTextFromSSEResponse joins first-candidate text from independently
// decoded response lines. It accepts optional data prefixes and object
// response wrappers; malformed, nonobject, and structurally incomplete lines
// are skipped.
//
// Only the first candidate contributes parts, in source line and part order.
// Thought text participates because extraction checks the text field without
// excluding the thought flag. A wrapper that is not an object leaves
// extraction on the direct object, and a body containing no selected text
// returns an empty string.
func ExtractTextFromSSEResponse(respBody []byte) string {
	var texts []string
	lines := bytes.SplitSeq(respBody, []byte("\n"))

	for line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		if after, ok := bytes.CutPrefix(line, []byte("data:")); ok {
			line = after
			line = bytes.TrimSpace(line)
		}

		if len(line) == 0 || line[0] != '{' {
			continue
		}

		// Decode each line independently; a truncated or malformed line is skipped
		// while text already collected from earlier complete lines remains available.
		var data map[string]any
		if err := json.Unmarshal(line, &data); err != nil {
			continue
		}

		// Use an object response wrapper when present and otherwise inspect the
		// direct object, letting local fixtures and platform streams share
		// first-candidate extraction.
		response, ok := data["response"].(map[string]any)
		if !ok {
			response = data
		}

		candidates, ok := response["candidates"].([]any)
		if !ok || len(candidates) == 0 {
			continue
		}

		candidate, ok := candidates[0].(map[string]any)
		if !ok {
			continue
		}

		content, ok := candidate["content"].(map[string]any)
		if !ok {
			continue
		}

		parts, ok := content["parts"].([]any)
		if !ok {
			continue
		}

		for _, part := range parts {
			if partMap, ok := part.(map[string]any); ok {
				if text, ok := partMap["text"].(string); ok && text != "" {
					texts = append(texts, text)
				}
			}
		}
	}

	return strings.Join(texts, "")
}

// Probe runs provider-local retries using ctx and extracts text from a
// caller-bounded response prefix. Optional enter returns a nonnil release
// callback held through response closing; callers supply limit and applicable
// retry callbacks.
//
// Callers return a positive read limit to bound retained bytes; the reader
// accepts truncation without detecting oversize.
// Read and HTTP failures return errors, while provider switching becomes a
// fixed connection-test message. Successful text can be empty when lines are
// malformed or nontext. The caller model remains the requested probe identity
// beside those observations.
func Probe(ctx context.Context, input RetryInput, options RetryOptions, model string, limit func() int64, enter func() (func(), error)) (*TestConnectionResult, error) {
	if enter != nil {
		done, err := enter()
		if err != nil {
			return nil, err
		}
		defer done()
	}
	input.Ctx = ctx
	result, err := (&RetryAdapter{Options: options}).AntigravityRetryLoop(input)
	if err != nil {
		if switchErr, ok := errors.AsType[*AntigravityProviderSwitchError](err); ok {
			return nil, fmt.Errorf("provider model %s is rate limited; try again later", switchErr.RateLimitedModel)
		}
		return nil, err
	}
	if result == nil || result.Resp == nil {
		return nil, errors.New("upstream returned empty response")
	}
	defer func() { _ = result.Resp.Body.Close() }()
	// Read only the caller-selected prefix while lifecycle registration and
	// response ownership remain active; a truncated successful response can still
	// yield partial probe text.
	body, err := io.ReadAll(io.LimitReader(result.Resp.Body, limit()))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if result.Resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API returned %d: %s", result.Resp.StatusCode, string(body))
	}
	return &TestConnectionResult{Text: ExtractTextFromSSEResponse(body), MappedModel: model}, nil
}
