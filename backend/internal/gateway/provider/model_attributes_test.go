package provider

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/modelcatalog/testkit"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/upstream/qoder"
)

// mediaAttributeRepo returns a configured group attribute value and counts its
// reads.
type mediaAttributeRepo struct {
	// unused attribute repository operations
	routing.ModelAttributeRepository
	// config is returned by reference for service snapshot capture. Keep it
	// stable while the service reads and copies the configured rules for each request.
	config *routing.ModelAttributeConfig // configured group attribute value
	reads  int                           // observed group configuration reads
}

// TestApplyModelAttributeParameters resolves offline attributes for sampling,
// Compact effort, GLM, DeepSeek, and unknown relay cases. Rewritten JSON
// matches each final-route expectation while unknown capabilities retain
// supplied fields.
func TestApplyModelAttributeParameters(t *testing.T) {
	cases := []struct {
		name string // parameter policy scenario label
		// final route attribute identity
		target modelrules.Target
		// body: input request JSON. expected: rewritten request JSON.
		body, expected string
	}{
		{"gpt sampling none", modelrules.Target{Platform: "openai", Model: "gpt-5.4"}, `{"reasoning":{"effort":"none"},"temperature":0.4,"top_p":0.8}`, `{"reasoning":{"effort":"none"},"temperature":0.4,"top_p":0.8}`},
		{"gpt sampling high", modelrules.Target{Platform: "openai", Model: "gpt-5.4"}, `{"reasoning":{"effort":"high"},"temperature":0.4,"top_p":0.8}`, `{"reasoning":{"effort":"high"}}`},
		{"compact max", modelrules.Target{Platform: "openai", ProviderType: "oauth", Model: "gpt-5.6-sol", Endpoint: "compact"}, `{"reasoning":{"effort":"max","summary":"auto"},"max_output_tokens":4096,"temperature":0.4,"top_p":0.8}`, `{"reasoning":{"effort":"xhigh","summary":"auto"}}`},
		{"standard max", modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-5.6-sol"}, `{"reasoning":{"effort":"max"}}`, `{"reasoning":{"effort":"max"}}`},
		{"glm high", modelrules.Target{Platform: "zhipu", Model: "glm-5.2"}, `{"reasoning_effort":"low"}`, `{"reasoning_effort":"high"}`},
		{"glm max", modelrules.Target{Platform: "zhipu", Model: "glm-5.3"}, `{"reasoning_effort":"xhigh"}`, `{"reasoning_effort":"max"}`},
		{"deepseek thinking", modelrules.Target{Platform: "deepseek", Model: "deepseek-v4-pro"}, `{"thinking":{"type":"enabled"},"temperature":0.4,"top_p":0.98}`, `{"thinking":{"type":"enabled"},"top_p":0.98}`},
		{"deepseek disabled", modelrules.Target{Platform: "deepseek", Model: "deepseek-v4-flash"}, `{"thinking":{"type":"disabled"},"temperature":0.4,"top_p":0.98}`, `{"thinking":{"type":"disabled"},"temperature":0.4}`},
		{"unknown", modelrules.Target{Platform: "openai", Model: "company-latest", BaseURL: "https://relay.example/v1"}, `{"reasoning":{"effort":"none"},"temperature":0.4,"top_p":0.8}`, `{"reasoning":{"effort":"none"},"temperature":0.4,"top_p":0.8}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resolved := testkit.OfflineView().Resolve(tc.target, modelcatalog.Attributes{})
			body, err := ApplyModelAttributeParameters([]byte(tc.body), resolved)
			require.NoError(t, err)
			require.JSONEq(t, tc.expected, string(body))
		})
	}
}

// TestApplyModelAttributeParametersOllamaOverrides resolves nonpositive and
// positive Ollama output caps plus an explicit output attribute. Positive
// limits clamp selected fields, nonpositive caps retain output values, and the
// resolved attribute governs later clamping.
func TestApplyModelAttributeParametersOllamaOverrides(t *testing.T) {
	for _, cap := range []int{-1, 0, 32768, 65535} {
		target := modelrules.Target{Platform: "openai", Model: "company-model", Ollama: true, OllamaOutputCap: &cap}
		resolved := testkit.OfflineView().Resolve(target, modelcatalog.Attributes{})
		body, err := ApplyModelAttributeParameters([]byte(`{"max_tokens":90000,"max_completion_tokens":100000}`), resolved)
		require.NoError(t, err)
		if cap > 0 {
			require.EqualValues(t, cap, gjson.GetBytes(body, "max_tokens").Int())
			require.EqualValues(t, cap, gjson.GetBytes(body, "max_completion_tokens").Int())
		} else {
			require.EqualValues(t, 90000, gjson.GetBytes(body, "max_tokens").Int())
		}
	}
	cap := 80000
	target := modelrules.Target{Platform: "openai", Model: "company-model", Ollama: true}
	resolved := testkit.OfflineView().Resolve(target, modelcatalog.Attributes{OutputLimit: &cap})
	body := ApplyOllamaCloudRawChatCompletionsRequest(ollamaCloudRawChatCompletionsTestProvider(), []byte(`{"max_tokens":100000}`))
	body, err := ApplyModelAttributeParameters(body, resolved)
	require.NoError(t, err)
	require.EqualValues(t, cap, gjson.GetBytes(body, "max_tokens").Int())
}

// ForGroup increments the fixture read count and returns its configured
// attribute pointer. The value remains borrowed for the service to snapshot;
// other repository methods stay unused through the embedded interface.
func (r *mediaAttributeRepo) ForGroup(context.Context, int64) (*routing.ModelAttributeConfig, error) {
	r.reads++
	return r.config, nil
}

// TestPrepareModelAttributeParametersImageToolOverrides overrides fidelity for
// one image-tool model while other tools use catalogue or unknown data. Only
// that model's field follows the override, original bytes remain unchanged,
// and group config is read once.
func TestPrepareModelAttributeParametersImageToolOverrides(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		repo := &mediaAttributeRepo{config: &routing.ModelAttributeConfig{Status: "active", Rules: []routing.ModelAttributeRule{{Models: []string{"gpt-image-2"}, Attributes: modelcatalog.Attributes{Media: &modelcatalog.MediaCapabilities{InputFidelity: &enabled}}}}}}
		image := []string{"image"}
		repo.config.Rules = append(repo.config.Rules, routing.ModelAttributeRule{Models: []string{"private-image"}, Attributes: modelcatalog.Attributes{OutputModalities: &image}})
		service := &routing.ModelAttributeService{Repo: repo, Catalog: routing.ModelAttributeCatalog{View: testkit.OfflineView}}
		ctx := requeststate.WithGroup(context.Background(), &routing.Group{ID: 1})
		provider := &ExecutionProvider{Record: providercore.Record{Platform: "openai", Type: "apikey"}}
		original := []byte(`{"model":"gpt-4o","tools":[{"type":"image_generation","model":"gpt-image-2","input_fidelity":"high"},{"type":"image_generation","model":"gpt-image-1.5","input_fidelity":"high"},{"type":"image_generation","model":"private-image","input_fidelity":"high"}]}`)
		body, err := PrepareModelAttributeParameters(ctx, provider, "gpt-4o", original, service, "openai_responses")
		require.NoError(t, err)
		require.Equal(t, enabled, gjson.GetBytes(body, "tools.0.input_fidelity").Exists())
		require.Equal(t, "high", gjson.GetBytes(body, "tools.1.input_fidelity").String())
		require.Equal(t, "high", gjson.GetBytes(body, "tools.2.input_fidelity").String())
		require.Equal(t, "high", gjson.GetBytes(original, "tools.0.input_fidelity").String())
		require.Equal(t, 1, repo.reads)
	}
}

// TestConversionDialectGeminiThinkingDistinguishesUnknownAndEmptyOptions
// compares unknown reasoning controls with explicit empty or disabled reasoning
// capability through ConversionDialect and convert.NewExchange. Effort and
// budget input succeeds under unknown options and fails when selectable
// controls are explicitly unavailable.
func TestConversionDialectGeminiThinkingDistinguishesUnknownAndEmptyOptions(t *testing.T) {
	yes, no := true, false
	empty := []modelcatalog.ReasoningOption{}
	for _, tc := range []struct {
		name  string                  // thinking capability scenario label
		attrs modelcatalog.Attributes // configured thinking capability attributes
		fail  bool                    // expected control validation failure
	}{
		{"unknown", modelcatalog.Attributes{}, false},
		{"reasoning known options unknown", modelcatalog.Attributes{Reasoning: &yes}, false},
		{"known empty", modelcatalog.Attributes{Reasoning: &yes, ReasoningOptions: &empty}, true},
		{"reasoning disabled", modelcatalog.Attributes{Reasoning: &no}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := testkit.OfflineView().Resolve(modelrules.Target{Platform: "gemini", Model: "private-model"}, tc.attrs)
			for _, control := range []struct {
				name   string              // control scenario label
				client protocol.ProtocolID // client request protocol
				body   string              // input request JSON
				effort string              // expected thinkingLevel
				budget int64               // expected thinkingBudget
			}{
				{"effort", protocol.ProtocolOpenAIChatCompletions, `{"model":"private-model","messages":[{"role":"user","content":"hi"}],"reasoning_effort":"high"}`, "high", 0},
				{"budget", protocol.ProtocolAnthropicMessages, `{"model":"private-model","max_tokens":8192,"messages":[{"role":"user","content":"hi"}],"thinking":{"type":"enabled","budget_tokens":4096}}`, "", 4096},
			} {
				t.Run(control.name, func(t *testing.T) {
					dialect := ConversionDialect(t.Context(), nil, result.ForRequest(ModelRequestState([]byte(control.body))), protocol.ProtocolGeminiGenerateContent)
					ex, err := convert.NewExchange([]byte(control.body), convert.Options{
						Route: convert.Route{
							Client:   control.client,
							Upstream: protocol.ProtocolGeminiGenerateContent,
							Dialect:  dialect,
						},
					})
					require.NoError(t, err)
					upBytes, err := ex.UpstreamRequest()
					if tc.fail {
						require.Error(t, err)
						return
					}
					require.NoError(t, err)
					require.Equal(t, control.effort, gjson.GetBytes(upBytes, "generationConfig.thinkingConfig.thinkingLevel").String())
					require.Equal(t, control.budget, gjson.GetBytes(upBytes, "generationConfig.thinkingConfig.thinkingBudget").Int())
				})
			}
		})
	}
}

// TestApplyModelAttributeParametersUsesGLMEffortOverrides supplies configured
// GLM effort sets across Chat, Responses, and Messages routes. Returned values
// stay within those sets, empty options remove effort, and Messages encodes
// enabled thinking.
func TestApplyModelAttributeParametersUsesGLMEffortOverrides(t *testing.T) {
	for _, tc := range []struct {
		// name: effort scenario label. model: final GLM identity.
		// protocol: upstream encoding protocol. body: input request
		// JSON. path: checked effort field. want: expected outgoing
		// effort.
		name, model, protocol, body, path, want string
		// configured selectable effort levels
		efforts []string
	}{
		{"chat accepts configured low", "glm-5.2", "openai_chat_completions", `{"reasoning_effort":"low"}`, "reasoning_effort", "low", []string{"low", "high"}},
		{"responses accepts configured medium", "glm-5.3", "openai_responses", `{"reasoning":{"effort":"medium"}}`, "reasoning.effort", "medium", []string{"medium", "high"}},
		{"alias stays in allowed set", "glm-5.3", "openai_chat_completions", `{"reasoning_effort":"xhigh"}`, "reasoning_effort", "high", []string{"low", "high"}},
		{"messages accepts configured medium", "glm-5.3", "anthropic_messages", `{"output_config":{"effort":"medium"}}`, "output_config.effort", "medium", []string{"medium", "high"}},
		{"messages alias stays in allowed set", "glm-5.3", "anthropic_messages", `{"thinking":{"type":"disabled"}}`, "output_config.effort", "high", []string{"high", "max"}},
		{"no selectable efforts", "glm-5.2", "openai_chat_completions", `{"reasoning_effort":"low"}`, "reasoning_effort", "", []string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			values := make([]*string, 0, len(tc.efforts))
			for _, effort := range tc.efforts {
				values = append(values, &effort)
			}
			options := []modelcatalog.ReasoningOption{{Type: "effort", Values: values}}
			result := testkit.OfflineView().Resolve(modelrules.Target{Platform: "zhipu", Model: tc.model, UpstreamProtocol: tc.protocol}, modelcatalog.Attributes{ReasoningOptions: &options})
			body, err := ApplyModelAttributeParameters([]byte(tc.body), result)
			require.NoError(t, err)
			require.Equal(t, tc.want, gjson.GetBytes(body, tc.path).String())
			if tc.want != "" {
				require.True(t, *result.Attributes.EffortSupport(tc.want))
			}
			if tc.protocol == "anthropic_messages" {
				require.Equal(t, "enabled", gjson.GetBytes(body, "thinking.type").String())
			}
		})
	}
}

// TestModelRequestStateGeminiFieldAliases varies camel and snake
// generation/thinking fields with level or budget controls. State recognizes
// each spelling and records the expected enabled/disabled value.
func TestModelRequestStateGeminiFieldAliases(t *testing.T) {
	for _, config := range []string{"generationConfig", "generation_config"} {
		for _, thinking := range []string{"thinkingConfig", "thinking_config"} {
			for _, tc := range []struct {
				// field: Gemini control spelling. value:
				// encoded control value.
				field, value string
				// expected request thinking state
				enabled bool
			}{
				{"thinkingLevel", `"high"`, true},
				{"thinking_level", `"none"`, false},
				{"thinkingBudget", "1024", true},
				{"thinking_budget", "0", false},
			} {
				t.Run(config+"/"+thinking+"/"+tc.field, func(t *testing.T) {
					body := []byte(fmt.Sprintf(`{%q:{%q:{%q:%s}}}`, config, thinking, tc.field, tc.value))
					state := ModelRequestState(body)
					require.NotNil(t, state.Thinking)
					require.Equal(t, tc.enabled, *state.Thinking)
				})
			}
		}
	}
}

// TestModelRequestStateFieldPrecedence mixes effort, thinking, duplicate keys,
// escaped keys, and Gemini aliases before mutating source bytes. Parsed state
// follows the tested precedence and retains copied effort text independently.
func TestModelRequestStateFieldPrecedence(t *testing.T) {
	high, low, medium, none := "high", "low", "medium", "none"
	yes, no := true, false
	for _, tc := range []struct {
		// name: precedence scenario label. body: input request JSON.
		name, body string
		effort     *string // expected copied effort pointer
		thinking   *bool   // expected optional thinking state
	}{
		{"absent", `{}`, nil, nil},
		{"effort priority", `{"output_config":{"effort":"low"},"reasoning_effort":"medium","reasoning":{"effort":" HIGH "}}`, &high, &yes},
		{"thinking overrides effort", `{"thinking":{"type":"disabled"},"reasoning":{"effort":"high"}}`, &high, &no},
		{"duplicate nested match", `{"reasoning":{},"reasoning":{"effort":"high"}}`, &high, &yes},
		{"duplicate null match", `{"reasoning":{"effort":null},"reasoning":{"effort":"high"},"reasoning_effort":"medium"}`, &medium, &yes},
		{"duplicate scalar match", `{"reasoning_effort":"low","reasoning_effort":"high"}`, &low, &yes},
		{"duplicate thinking match", `{"thinking":{},"thinking":{"type":"disabled"},"thinking":{"type":"enabled"}}`, nil, &no},
		{"config spelling priority", `{"generation_config":{"thinkingConfig":{"thinkingLevel":"none"}},"generationConfig":{"thinking_config":{"thinking_level":"high"}}}`, &none, &no},
		{"duplicate config match", `{"generationConfig":{},"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`, nil, nil},
		{"budget overrides level", `{"generationConfig":{"thinkingConfig":{"thinkingLevel":"high","thinkingBudget":0}}}`, &high, &no},
		{"escaped field", `{"reasoning_\u0065ffort":"high"}`, &high, &yes},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			state := ModelRequestState(body)
			for i := range body {
				body[i] = 'x'
			}
			require.Equal(t, modelrules.RequestState{Effort: tc.effort, Thinking: tc.thinking}, state)
		})
	}
}

// BenchmarkModelRequestState measures control extraction as unrelated input
// grows from KiB to MiB fixtures. Each timed iteration also checks that high
// effort is recovered from the same body.
func BenchmarkModelRequestState(b *testing.B) {
	for _, size := range []int{1024, 1024 * 1024, 8 * 1024 * 1024} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			body := []byte(`{"input":"` + strings.Repeat("x", size) + `","reasoning":{"effort":"high"}}`)
			b.SetBytes(int64(len(body)))
			b.ResetTimer()
			for range b.N {
				state := ModelRequestState(body)
				if state.Effort == nil || *state.Effort != "high" {
					b.Fatal("missing reasoning effort")
				}
			}
		})
	}
}

// TestComposerControlsFollowResolvedOverride exercises the complete Grok
// preparation pass with platform defaults and an administrator effort override.
// The final payload must retain configured controls even for Composer identities.
func TestComposerControlsFollowResolvedOverride(t *testing.T) {
	ctx := requeststate.WithGroup(context.Background(), &routing.Group{ID: 1})
	high := "max"
	options := []modelcatalog.ReasoningOption{{Type: "effort", Values: []*string{&high}}}
	repo := &mediaAttributeRepo{config: &routing.ModelAttributeConfig{Status: routing.StatusActive, Rules: []routing.ModelAttributeRule{{Models: []string{"grok-composer"}, Attributes: modelcatalog.Attributes{ReasoningOptions: &options}}}}}
	service := &routing.ModelAttributeService{Repo: repo, Catalog: routing.ModelAttributeCatalog{View: testkit.OfflineView}}
	provider := &ExecutionProvider{Record: providercore.Record{Platform: "grok", Type: "apikey"}}
	body, err := PrepareGrokResponses(ctx, provider, "grok-composer", []byte(`{"model":"grok-composer","input":"hello","reasoning":{"effort":"max"}}`), service)
	require.NoError(t, err)
	require.Equal(t, "max", gjson.GetBytes(body, "reasoning.effort").String())
	service.Repo = nil
	body, err = PrepareGrokResponses(context.Background(), provider, "grok-composer", body, service)
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(body, "reasoning").Exists())
}

// TestImageToolRequiresDeclaredOutput validates hosted image tools against
// configured output modalities. Unknown and explicitly unsupported identities
// fail before encoding, while an arbitrary configured image name is accepted.
func TestImageToolRequiresDeclaredOutput(t *testing.T) {
	image := []string{"image"}
	text := []string{"text"}
	service := &routing.ModelAttributeService{Catalog: routing.ModelAttributeCatalog{Lookup: func(model string) modelcatalog.Attributes {
		switch model {
		case "canvas":
			return modelcatalog.Attributes{OutputModalities: &image}
		case "gpt-image-text":
			return modelcatalog.Attributes{OutputModalities: &text}
		default:
			return modelcatalog.Attributes{}
		}
	}}}
	provider := &ExecutionProvider{Record: providercore.Record{Platform: "openai", Type: "apikey"}}
	for _, tc := range []struct {
		model  string // requested tool model identity
		reason string // expected local capability failure
	}{{"canvas", ""}, {"gpt-image-unknown", "MODEL_CAPABILITY_UNKNOWN"}, {"gpt-image-text", "MODEL_CAPABILITY_UNSUPPORTED"}} {
		t.Run(tc.model, func(t *testing.T) {
			body := []byte(fmt.Sprintf(`{"model":"driver","tools":[{"type":"image_generation","model":%q}]}`, tc.model))
			_, err := PrepareModelAttributeParameters(context.Background(), provider, "driver", body, service, "openai_responses")
			if tc.reason == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.reason)
			}
		})
	}
	for _, platform := range []string{"openai", "grok", "zhipu"} {
		t.Run(platform+"/all-tools", func(t *testing.T) {
			provider.Record.Platform = platform
			body := []byte(`{"model":"driver","tools":[{"type":"image_generation","model":"canvas"},{"type":"image_generation","model":"unknown-second-tool"}]}`)
			_, err := PrepareModelAttributeParameters(context.Background(), provider, "driver", body, service, "openai_responses")
			require.ErrorContains(t, err, "MODEL_CAPABILITY_UNKNOWN")
		})
	}
}

// TestOpenCodeClaudeRequestUsesAdaptiveEncoding prepares a Messages body containing
// an explicit budget and temperature. The declared Claude model changes thinking
// to adaptive, removes its budget and temperature, and retains OpenCode identity.
func TestOpenCodeClaudeRequestUsesAdaptiveEncoding(t *testing.T) {
	service := &routing.ModelAttributeService{Catalog: routing.ModelAttributeCatalog{View: testkit.OfflineView}}
	value := &ExecutionProvider{Record: providercore.Record{Platform: "opencode", Type: "apikey", Credentials: map[string]any{"api_key": "test"}}}
	body, err := PrepareModelAttributeParameters(context.Background(), value, "claude-opus-4-7", []byte(`{"model":"claude-opus-4-7","thinking":{"type":"enabled","budget_tokens":10240},"max_tokens":8192,"temperature":0.4}`), service, "anthropic_messages")
	require.NoError(t, err)
	require.Equal(t, "adaptive", gjson.GetBytes(body, "thinking.type").String())
	require.False(t, gjson.GetBytes(body, "thinking.budget_tokens").Exists())
	require.False(t, gjson.GetBytes(body, "temperature").Exists())
	require.Equal(t, "opencode", value.Capabilities.Target.Platform)
}

// TestCanonicalModelParameterBodyParity sends the same parameters through author
// routes and explicitly identified OpenCode aliases. The resulting JSON follows
// model sampling and field restrictions while retaining API-key output controls.
func TestCanonicalModelParameterBodyParity(t *testing.T) {
	for _, test := range []struct {
		name      string // parameter scenario label
		platform  string // author route platform
		model     string // author model identity
		canonical string // qualified metadata identity
		protocol  string // upstream request encoding
		body      string // original request parameters
		expected  string // final request parameters
	}{
		{"gpt high responses", "openai", "gpt-5.4", "openai/gpt-5.4", "openai_responses", `{"reasoning":{"effort":"high"},"temperature":0.4,"top_p":0.8,"max_output_tokens":4096}`, `{"reasoning":{"effort":"high"},"max_output_tokens":4096}`},
		{"gpt none responses", "openai", "gpt-5.4", "openai/gpt-5.4", "openai_responses", `{"reasoning":{"effort":"none"},"temperature":0.4,"top_p":0.8}`, `{"reasoning":{"effort":"none"},"temperature":0.4,"top_p":0.8}`},
		{"gpt default responses", "openai", "gpt-5.4", "openai/gpt-5.4", "openai_responses", `{"temperature":0.4,"top_p":0.8}`, `{"temperature":0.4,"top_p":0.8}`},
		{"gpt high chat", "openai", "gpt-5.4", "openai/gpt-5.4", "openai_chat_completions", `{"reasoning_effort":"high","temperature":0.4,"top_p":0.8}`, `{"reasoning_effort":"high"}`},
		{"gpt none chat", "openai", "gpt-5.4", "openai/gpt-5.4", "openai_chat_completions", `{"reasoning_effort":"none","temperature":0.4,"top_p":0.8}`, `{"reasoning_effort":"none","temperature":0.4,"top_p":0.8}`},
		{"deepseek enabled", "deepseek", "deepseek-v4-pro", "deepseek/deepseek-v4-pro", "openai_chat_completions", `{"thinking":{"type":"enabled"},"temperature":0.4,"top_p":0.98}`, `{"thinking":{"type":"enabled"},"top_p":0.98}`},
		{"deepseek disabled", "deepseek", "deepseek-v4-pro", "deepseek/deepseek-v4-pro", "anthropic_messages", `{"thinking":{"type":"disabled"},"temperature":0.4,"top_p":0.98}`, `{"thinking":{"type":"disabled"},"temperature":0.4}`},
		{"deepseek default", "deepseek", "deepseek-v4-pro", "deepseek/deepseek-v4-pro", "openai_chat_completions", `{"temperature":0.4,"top_p":0.98}`, `{"top_p":0.98}`},
		{"grok penalties", "grok", "grok-4.5", "xai/grok-4.5", "openai_chat_completions", `{"presence_penalty":1,"presencePenalty":1,"frequency_penalty":1,"frequencyPenalty":1,"stop":["end"],"max_tokens":100}`, `{"max_tokens":100}`},
		{"grok logprobs", "grok", "grok-4.20", "xai/grok-4.20", "openai_responses", `{"logprobs":true,"top_logprobs":2,"max_output_tokens":100}`, `{"max_output_tokens":100}`},
		{"apikey output controls", "openai", "gpt-5.6-sol", "openai/gpt-5.6-sol", "openai_responses", `{"reasoning":{"effort":"max"},"max_output_tokens":4096,"frequency_penalty":0.2,"presence_penalty":0.3}`, `{"reasoning":{"effort":"max"},"max_output_tokens":4096,"frequency_penalty":0.2,"presence_penalty":0.3}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, target := range []modelrules.Target{
				{Platform: test.platform, ProviderType: "apikey", Model: test.model, UpstreamProtocol: test.protocol},
				{Platform: "opencode", ProviderType: "apikey", Model: "service-alias", AttributeModel: test.canonical, UpstreamProtocol: test.protocol},
			} {
				resolved := testkit.OfflineView().Resolve(target, modelcatalog.Attributes{})
				body, err := ApplyModelAttributeParameters([]byte(test.body), resolved)
				require.NoError(t, err)
				require.JSONEq(t, test.expected, string(body), "%s", target.Platform)
				require.Equal(t, target.Platform, resolved.Target.Platform)
				require.Equal(t, target.Model, resolved.Target.Model)
			}
		})
	}
}

// TestReferencedAttributesPreserveOutboundModel borrows sampling rules for a custom alias.
// Parameter preparation applies the target's effort condition while the JSON
// model and retained route still name the actual upstream alias.
func TestReferencedAttributesPreserveOutboundModel(t *testing.T) {
	repo := &mediaAttributeRepo{config: &routing.ModelAttributeConfig{Status: routing.StatusActive, Rules: []routing.ModelAttributeRule{{Models: []string{"custom-*"}, ReferenceModel: "gpt-5.4"}}}}
	service := &routing.ModelAttributeService{Repo: repo, Catalog: routing.ModelAttributeCatalog{View: testkit.OfflineView}}
	target := modelrules.Target{Model: "custom-openai", Platform: "openai", ProviderType: "apikey", ClientProtocol: "openai_chat_completions", UpstreamProtocol: "openai_chat_completions"}
	result, err := service.NewSession().Resolve(t.Context(), 7, target)
	require.NoError(t, err)
	require.Equal(t, "custom-openai", result.Target.Model)
	body, err := ApplyModelAttributeParameters([]byte(`{"model":"custom-openai","reasoning":{"effort":"high"},"temperature":0.5}`), result)
	require.NoError(t, err)
	require.Equal(t, "custom-openai", gjson.GetBytes(body, "model").String())
	require.False(t, gjson.GetBytes(body, "temperature").Exists())
}

// TestQoderPreviewAndRuntimeShareRoute resolves both a key and its account
// alias under protocol-specific rules. Profile references must borrow another
// model's data without changing the service key used by the payload encoder.
func TestQoderPreviewAndRuntimeShareRoute(t *testing.T) {
	row := qoderAvailabilityRecord(7, "cn", []map[string]any{
		{"key": "native", "alias": "public-name", "name": "Original", "enabled": true, "input_limit": 1000000},
		{"key": "other", "alias": "other-name", "name": "Referenced", "enabled": true, "input_limit": 64000},
	})
	row.PrepareQoderCatalog()
	rule := routing.ModelAttributeRule{Models: []string{"native"}, Match: &routing.ModelAttributeMatch{UpstreamProtocol: "qoder_chat"}, Attributes: modelcatalog.Attributes{InputLimit: new(128000)}}
	config := &routing.ModelAttributeConfig{Status: "active", Rules: []routing.ModelAttributeRule{rule}}
	service := &routing.ModelAttributeService{Repo: &mediaAttributeRepo{config: config}}
	runtime := NewQoderRuntime(QoderRuntimeOptions{Attributes: service})
	ctx := requeststate.WithGroup(t.Context(), &routing.Group{ID: 7})
	for _, wire := range []protocol.ProtocolID{protocol.ProtocolOpenAIResponses, protocol.ProtocolOpenAIChatCompletions, protocol.ProtocolAnthropicMessages} {
		config.Rules[0].Match.ClientProtocol = string(wire)
		for _, name := range []string{"native", "public-name"} {
			rules, ok := CatalogueProvider(&row, requeststate.AttemptRoute{}).Rules.(catalogueRules)
			require.True(t, ok)
			target := rules.AttributeTarget(name, "native", wire, protocol.ProtocolQoderChat)
			preview, err := service.Preview(ctx, 7, []modelrules.Target{target}, config)
			require.NoError(t, err)
			actual, err := runtime.Target(qoder.RequestMetadata{}, &row).Attributes(ctx, name, wire)
			require.NoError(t, err)
			require.Equal(t, preview[0], actual)
			require.Equal(t, new(128000), actual.Attributes.InputLimit)
			payload, key, err := qoder.BuildQoderPayloadWithOptions(qoder.QoderPayloadRequest{Model: name, Capabilities: &actual}, "", nil, true, true)
			require.NoError(t, err)
			require.Equal(t, "native", key)
			config, ok := payload["model_config"].(map[string]any)
			require.True(t, ok)
			require.Equal(t, 128000, config["max_input_tokens"])
		}
	}
	config.Rules[0] = routing.ModelAttributeRule{Models: []string{"native"}, ReferenceModel: "other-name"}
	actual, err := runtime.Target(qoder.RequestMetadata{}, &row).Attributes(ctx, "public-name", protocol.ProtocolOpenAIResponses)
	require.NoError(t, err)
	require.Equal(t, "native", actual.Target.Model)
	require.Equal(t, new("Referenced"), actual.Attributes.DisplayName)
	require.Equal(t, new(64000), actual.Attributes.InputLimit)
	config.Rules[0].ReferenceModel = "missing"
	absent, err := runtime.Target(qoder.RequestMetadata{}, &row).Attributes(ctx, "public-name", protocol.ProtocolOpenAIResponses)
	require.NoError(t, err)
	require.Nil(t, absent.Attributes.InputLimit)
	require.Nil(t, absent.Attributes.DisplayName)
}

// TestConversionDialect_PlatformsAuthAndReplaySchemes resolves dialects across
// OpenAI, Anthropic, Grok, Gemini, Antigravity, DeepSeek, Kimi, Zhipu, OpenCode,
// and Qoder routes. It protects tool bitmasks, byte caps, replay schemes,
// Antigravity family separation, passback history, and protocol-specific flags.
func TestConversionDialect_PlatformsAuthAndReplaySchemes(t *testing.T) {
	t.Parallel()

	view := testkit.OfflineView()
	hostedAndFunction := convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch

	cases := []struct {
		name              string                // route scenario label
		provider          *ExecutionProvider    // optional execution target
		target            modelrules.Target     // attribute resolution target
		upstream          protocol.ProtocolID   // selected upstream protocol
		wantName          string                // expected diagnostic dialect name
		wantProviderID    int64                 // expected account-bound provider ID
		wantTools         convert.ToolSupport   // expected native tool capability mask
		wantMaxBytes      int                   // expected tool name byte limit
		wantReplayScheme  string                // expected continuity replay scheme
		wantHistory       convert.HistoryMode   // expected plaintext reasoning history mode
		wantSchema        convert.SchemaDialect // expected JSON Schema cleaning dialect
		wantOmitStore     bool                  // expected OmitStoreAndInclude flag
		wantBillingHeader bool                  // expected AllowBillingHeader flag
		wantPlaceholder   string                // expected Gemini thoughtSignature placeholder
		wantForceBudget   bool                  // expected Antigravity Claude ForceBudget flag
	}{
		{
			name:             "openai_oauth_responses",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 101, Platform: "openai", Type: "oauth"}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "oauth", Model: "gpt-5.4", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "openai",
			wantProviderID:   101,
			wantTools:        convert.ToolSupportAllResponses,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeOpenAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "openai_setup_token_responses_ws",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 102, Platform: "openai", Type: "setup-token"}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "setup-token", Model: "gpt-5.4", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolResponsesWebSocket,
			wantName:         "openai",
			wantProviderID:   102,
			wantTools:        convert.ToolSupportAllResponses,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeOpenAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "openai_apikey_responses_function_only",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 103, Platform: "openai", Type: "apikey"}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-5.4", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "openai",
			wantProviderID:   103,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeOpenAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "openai_apikey_responses_ws_bridge_function_only",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 104, Platform: "openai", Type: "apikey"}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-5.4", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolResponsesWebSocket,
			wantName:         "openai",
			wantProviderID:   104,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeOpenAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "openai_apikey_local_relay_retains_openai_scheme",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 105, Platform: "openai", Type: "apikey", Credentials: map[string]any{"base_url": "http://127.0.0.1:18080/v1"}}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "apikey", BaseURL: "http://127.0.0.1:18080/v1", Model: "company-latest", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "openai",
			wantProviderID:   105,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeOpenAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "openai_apikey_xai_host_uses_xai_scheme",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 106, Platform: "openai", Type: "apikey", Credentials: map[string]any{"base_url": "https://api.x.ai/v1"}}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "apikey", BaseURL: "https://api.x.ai/v1", Model: "grok-4.5", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "openai",
			wantProviderID:   106,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeXAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
			wantOmitStore:    false,
		},
		{
			name:             "openai_apikey_deepseek_host_clears_scheme",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 107, Platform: "openai", Type: "apikey", Credentials: map[string]any{"base_url": "https://api.deepseek.com"}}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "apikey", BaseURL: "https://api.deepseek.com", Model: "deepseek-v4-pro", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "openai",
			wantProviderID:   107,
			wantTools:        convert.ToolSupportFunction,
			wantMaxBytes:     64,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "openai_apikey_relay_passback_model_clears_scheme",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 108, Platform: "openai", Type: "apikey", Credentials: map[string]any{"base_url": "https://relay.example/v1"}}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "apikey", BaseURL: "https://relay.example/v1", Model: "deepseek-reasoner", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "openai",
			wantProviderID:   108,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "openai_apikey_chat_completions",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 109, Platform: "openai", Type: "apikey"}},
			target:           modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-4o", UpstreamProtocol: "openai_chat_completions"},
			upstream:         protocol.ProtocolOpenAIChatCompletions,
			wantName:         "openai",
			wantProviderID:   109,
			wantTools:        convert.ToolSupportFunction,
			wantMaxBytes:     64,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryToolTurns,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "anthropic_oauth_messages",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 201, Platform: "anthropic", Type: "oauth"}},
			target:           modelrules.Target{Platform: "anthropic", ProviderType: "oauth", Model: "claude-sonnet-4-5", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "anthropic",
			wantProviderID:   201,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: ir.SchemeAnthropic,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "anthropic_bedrock_messages",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 202, Platform: "anthropic", Type: "bedrock"}},
			target:           modelrules.Target{Platform: "anthropic", ProviderType: "bedrock", Model: "us.anthropic.claude-opus-4-6-v1", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "anthropic",
			wantProviderID:   202,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: ir.SchemeAnthropic,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "anthropic_vertex_service_account_messages",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 203, Platform: "anthropic", Type: "service_account"}},
			target:           modelrules.Target{Platform: "anthropic", ProviderType: "service_account", Model: "claude-sonnet-4-5@20250929", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "anthropic",
			wantProviderID:   203,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: ir.SchemeAnthropic,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "grok_oauth_responses",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 301, Platform: "grok", Type: "oauth"}},
			target:           modelrules.Target{Platform: "grok", ProviderType: "oauth", Model: "grok-4.5", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "grok",
			wantProviderID:   301,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeXAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
			wantOmitStore:    true,
		},
		{
			name:             "grok_apikey_responses",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 302, Platform: "grok", Type: "apikey"}},
			target:           modelrules.Target{Platform: "grok", ProviderType: "apikey", Model: "grok-4.20", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "grok",
			wantProviderID:   302,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeXAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
			wantOmitStore:    true,
		},
		{
			name:             "grok_chat_completions",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 303, Platform: "grok", Type: "apikey"}},
			target:           modelrules.Target{Platform: "grok", ProviderType: "apikey", Model: "grok-4.5", UpstreamProtocol: "openai_chat_completions"},
			upstream:         protocol.ProtocolOpenAIChatCompletions,
			wantName:         "grok",
			wantProviderID:   303,
			wantTools:        convert.ToolSupportFunction,
			wantMaxBytes:     64,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryToolTurns,
			wantSchema:       convert.SchemaJSONSchema,
			wantOmitStore:    false,
		},
		{
			name:             "gemini_apikey_generate_content",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 401, Platform: "gemini", Type: "apikey"}},
			target:           modelrules.Target{Platform: "gemini", ProviderType: "apikey", Model: "gemini-2.5-pro", UpstreamProtocol: "gemini_generate_content"},
			upstream:         protocol.ProtocolGeminiGenerateContent,
			wantName:         "gemini",
			wantProviderID:   401,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeGemini,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaGemini,
			wantPlaceholder:  gemini.DummyThoughtSignature,
		},
		{
			name:             "gemini_vertex_service_account_resource_model",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 402, Platform: "gemini", Type: "service_account"}},
			target:           modelrules.Target{Platform: "gemini", ProviderType: "service_account", Model: "projects/demo/locations/us-central1/publishers/google/models/gemini-2.5-flash", UpstreamProtocol: "gemini_generate_content"},
			upstream:         protocol.ProtocolGeminiGenerateContent,
			wantName:         "gemini",
			wantProviderID:   402,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeGemini,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaGemini,
			wantPlaceholder:  gemini.DummyThoughtSignature,
		},
		{
			name:             "gemini_count_tokens_clears_scheme_and_placeholder",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 403, Platform: "gemini", Type: "oauth"}},
			target:           modelrules.Target{Platform: "gemini", ProviderType: "oauth", Model: "gemini-2.5-pro", UpstreamProtocol: "gemini_generate_content", Endpoint: "count_tokens"},
			upstream:         protocol.ProtocolGeminiGenerateContent,
			wantName:         "gemini",
			wantProviderID:   403,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaGemini,
			wantPlaceholder:  "",
		},
		{
			name:             "antigravity_claude_family_split",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 501, Platform: "antigravity", Type: "oauth"}},
			target:           modelrules.Target{Platform: "antigravity", ProviderType: "oauth", Model: "claude-opus-4-6", ClientProtocol: "anthropic_messages", UpstreamProtocol: "gemini_generate_content"},
			upstream:         protocol.ProtocolGeminiGenerateContent,
			wantName:         "antigravity",
			wantProviderID:   501,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeAntigravityClaude,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaAntigravity,
			wantPlaceholder:  "",
			wantForceBudget:  true,
		},
		{
			name:             "antigravity_gemini_family_split",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 502, Platform: "antigravity", Type: "oauth"}},
			target:           modelrules.Target{Platform: "antigravity", ProviderType: "oauth", Model: "models/gemini-2.5-pro", ClientProtocol: "openai_responses", UpstreamProtocol: "gemini_generate_content"},
			upstream:         protocol.ProtocolGeminiGenerateContent,
			wantName:         "antigravity",
			wantProviderID:   502,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeGemini,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaAntigravity,
			wantPlaceholder:  gemini.DummyThoughtSignature,
			wantForceBudget:  false,
		},
		{
			name:             "antigravity_unknown_family_clears_scheme_and_placeholder",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 503, Platform: "antigravity", Type: "oauth"}},
			target:           modelrules.Target{Platform: "antigravity", ProviderType: "oauth", Model: "custom-experimental", ClientProtocol: "anthropic_messages", UpstreamProtocol: "gemini_generate_content"},
			upstream:         protocol.ProtocolGeminiGenerateContent,
			wantName:         "antigravity",
			wantProviderID:   503,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaAntigravity,
			wantPlaceholder:  "",
			wantForceBudget:  false,
		},
		{
			name:             "deepseek_messages_passback",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 601, Platform: "deepseek", Type: "apikey"}},
			target:           modelrules.Target{Platform: "deepseek", ProviderType: "apikey", Model: "deepseek-reasoner", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "deepseek",
			wantProviderID:   601,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryToolTurns,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "deepseek_responses_function_only",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 602, Platform: "deepseek", Type: "apikey"}},
			target:           modelrules.Target{Platform: "deepseek", ProviderType: "apikey", Model: "deepseek-reasoner", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "deepseek",
			wantProviderID:   602,
			wantTools:        convert.ToolSupportFunction,
			wantMaxBytes:     64,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "kimi_responses_keeps_full_tools",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 603, Platform: "kimi", Type: "apikey"}},
			target:           modelrules.Target{Platform: "kimi", ProviderType: "apikey", Model: "kimi-k2.5", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "kimi",
			wantProviderID:   603,
			wantTools:        convert.ToolSupportAllResponses,
			wantMaxBytes:     64,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "kimi_messages_passback",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 604, Platform: "kimi", Type: "apikey"}},
			target:           modelrules.Target{Platform: "kimi", ProviderType: "apikey", Model: "kimi-k2.5", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "kimi",
			wantProviderID:   604,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryToolTurns,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "zhipu_messages_passback",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 605, Platform: "zhipu", Type: "apikey"}},
			target:           modelrules.Target{Platform: "zhipu", ProviderType: "apikey", Model: "glm-5.3", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "zhipu",
			wantProviderID:   605,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryToolTurns,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "compatible_relay_minimax_and_qwen_passback",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 606, Platform: "anthropic", Type: "apikey", Credentials: map[string]any{"base_url": "https://relay.example"}}},
			target:           modelrules.Target{Platform: "anthropic", ProviderType: "apikey", BaseURL: "https://relay.example", Model: "qwen3-235b-thinking", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "anthropic",
			wantProviderID:   606,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryToolTurns,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "opencode_claude_messages",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 701, Platform: "opencode", Type: "apikey"}},
			target:           modelrules.Target{Platform: "opencode", ProviderType: "apikey", Model: "service-claude", AttributeModel: "anthropic/claude-opus-4-7", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "opencode",
			wantProviderID:   701,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: ir.SchemeAnthropic,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "opencode_openai_responses",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 702, Platform: "opencode", Type: "apikey"}},
			target:           modelrules.Target{Platform: "opencode", ProviderType: "apikey", Model: "service-gpt", AttributeModel: "openai/gpt-5.4", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "opencode",
			wantProviderID:   702,
			wantTools:        convert.ToolSupportAllResponses,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeOpenAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "opencode_xai_responses",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 703, Platform: "opencode", Type: "apikey"}},
			target:           modelrules.Target{Platform: "opencode", ProviderType: "apikey", Model: "service-grok", AttributeModel: "xai/grok-4.5", UpstreamProtocol: "openai_responses"},
			upstream:         protocol.ProtocolOpenAIResponses,
			wantName:         "opencode",
			wantProviderID:   703,
			wantTools:        convert.ToolSupportAllResponses,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeXAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
			wantOmitStore:    false,
		},
		{
			name:             "opencode_gemini_generate_content",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 704, Platform: "opencode", Type: "apikey"}},
			target:           modelrules.Target{Platform: "opencode", ProviderType: "apikey", Model: "service-gemini", AttributeModel: "google/gemini-2.5-pro", UpstreamProtocol: "gemini_generate_content"},
			upstream:         protocol.ProtocolGeminiGenerateContent,
			wantName:         "opencode",
			wantProviderID:   704,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeGemini,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaGemini,
			wantPlaceholder:  gemini.DummyThoughtSignature,
		},
		{
			name:             "opencode_minimax_passback_messages",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 705, Platform: "opencode", Type: "apikey"}},
			target:           modelrules.Target{Platform: "opencode", ProviderType: "apikey", Model: "service-minimax", AttributeModel: "minimax/minimax-m2.5", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "opencode",
			wantProviderID:   705,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryToolTurns,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:             "opencode_unverified_author_clears_scheme",
			provider:         &ExecutionProvider{Record: providercore.Record{ID: 706, Platform: "opencode", Type: "apikey"}},
			target:           modelrules.Target{Platform: "opencode", ProviderType: "apikey", Model: "claude-opus-4-7-free", AttributeModel: "foreign/claude-opus-4-7", UpstreamProtocol: "anthropic_messages"},
			upstream:         protocol.ProtocolAnthropicMessages,
			wantName:         "opencode",
			wantProviderID:   706,
			wantTools:        hostedAndFunction,
			wantMaxBytes:     128,
			wantReplayScheme: "",
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
		{
			name:              "qoder_cosy_retains_billing_header",
			provider:          &ExecutionProvider{Record: providercore.Record{ID: 801, Platform: "qoder", Type: "cosy"}},
			target:            modelrules.Target{Platform: "qoder", ProviderType: "cosy", Model: "ultimate", UpstreamProtocol: "qoder_chat"},
			upstream:          protocol.ProtocolQoderChat,
			wantName:          "qoder",
			wantProviderID:    801,
			wantTools:         convert.ToolSupportFunction,
			wantMaxBytes:      64,
			wantReplayScheme:  "",
			wantHistory:       convert.HistoryNone,
			wantSchema:        convert.SchemaJSONSchema,
			wantBillingHeader: true,
		},
		{
			name:             "nil_target_uses_resolution_metadata",
			provider:         nil,
			target:           modelrules.Target{Platform: "openai", ProviderType: "oauth", Model: "gpt-5.4", UpstreamProtocol: "openai_responses"},
			upstream:         "",
			wantName:         "openai",
			wantProviderID:   0,
			wantTools:        convert.ToolSupportAllResponses,
			wantMaxBytes:     64,
			wantReplayScheme: ir.SchemeOpenAI,
			wantHistory:      convert.HistoryNone,
			wantSchema:       convert.SchemaJSONSchema,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resolved := view.Resolve(tc.target, modelcatalog.Attributes{})
			got := ConversionDialect(t.Context(), tc.provider, resolved, tc.upstream)

			require.Equal(t, tc.wantName, got.Name)
			require.Equal(t, tc.wantProviderID, got.ProviderID)
			require.Equal(t, tc.wantTools, got.Tools)
			require.Equal(t, tc.wantMaxBytes, got.ToolNameMaxBytes)
			require.Equal(t, tc.wantReplayScheme, got.ReplayScheme)
			require.Equal(t, tc.wantHistory, got.ReasoningHistory)
			require.Equal(t, tc.wantSchema, got.SchemaDialect)
			require.Equal(t, tc.wantOmitStore, got.OmitStoreAndInclude)
			require.Equal(t, tc.wantBillingHeader, got.AllowBillingHeader)
			require.Equal(t, tc.wantPlaceholder, got.PlaceholderToken)
			require.Equal(t, tc.wantForceBudget, got.Reasoning.ForceBudget)
		})
	}
}

// TestConversionDialect_StripEffort checks planned and unplanned effort-model
// mappings with strip_effort enabled, disabled, or unmatched. It verifies that
// pre-mapped route models activate StripEffort even after result.Target.Model
// carries the mapped outbound name, and that nil contexts do not panic.
func TestConversionDialect_StripEffort(t *testing.T) {
	t.Parallel()

	view := testkit.OfflineView()
	makeProvider := func(strip bool) *ExecutionProvider {
		return &ExecutionProvider{
			Record: providercore.Record{
				ID:          901,
				Platform:    "anthropic",
				Type:        "apikey",
				Status:      "active",
				Schedulable: true,
				Credentials: map[string]any{
					"api_key": "sk-ant-test",
					providercore.EffortModelMappingStripEffortCredentialKey: strip,
					providercore.EffortModelMappingCredentialKey: map[string]any{
						"claude-sonnet-4-5": map[string]any{
							"high": "claude-opus-4-6",
						},
					},
					"model_mapping": map[string]any{
						"claude-sonnet-4-5": "claude-sonnet-4-5-20250929",
					},
				},
			},
		}
	}

	resolvedMapped := view.Resolve(modelrules.Target{
		Platform:         "anthropic",
		ProviderType:     "apikey",
		Model:            "claude-opus-4-6",
		UpstreamProtocol: "anthropic_messages",
	}, modelcatalog.Attributes{})

	t.Run("planned_attempt_effort_match_and_toggle", func(t *testing.T) {
		t.Parallel()
		ctx := requeststate.WithClientProtocol(t.Context(), protocol.ProtocolOpenAIResponses)
		ctx = requeststate.WithRoutingReasoningEffort(ctx, "high")
		plan := RoutePlanForMapping(ctx, nil, nil, "claude-sonnet-4-5", routing.GroupMappingResult{MappedModel: "claude-sonnet-4-5"})
		ctx = requeststate.WithRoutePlan(ctx, plan)

		routedStrip, err := ProviderForProtocolAttempt(ctx, makeProvider(true))
		require.NoError(t, err)
		require.True(t, routedStrip.Route.EffortMappingMatched())
		require.True(t, ConversionDialect(ctx, routedStrip, resolvedMapped, protocol.ProtocolAnthropicMessages).StripEffort)

		routedKeep, err := ProviderForProtocolAttempt(ctx, makeProvider(false))
		require.NoError(t, err)
		require.True(t, routedKeep.Route.EffortMappingMatched())
		require.False(t, ConversionDialect(ctx, routedKeep, resolvedMapped, protocol.ProtocolAnthropicMessages).StripEffort)
	})

	t.Run("unplanned_route_plan_uses_pre_mapped_model", func(t *testing.T) {
		t.Parallel()
		highCtx := requeststate.WithClientProtocol(t.Context(), protocol.ProtocolOpenAIResponses)
		highCtx = requeststate.WithRoutingReasoningEffort(highCtx, "high")
		plan := RoutePlanForMapping(highCtx, nil, nil, "client-alias", routing.GroupMappingResult{MappedModel: "claude-sonnet-4-5"})
		highCtx = requeststate.WithRoutePlan(highCtx, plan)

		require.True(t, ConversionDialect(highCtx, makeProvider(true), resolvedMapped, protocol.ProtocolAnthropicMessages).StripEffort)

		lowCtx := requeststate.WithClientProtocol(t.Context(), protocol.ProtocolOpenAIResponses)
		lowCtx = requeststate.WithRoutingReasoningEffort(lowCtx, "low")
		lowPlan := RoutePlanForMapping(lowCtx, nil, nil, "client-alias", routing.GroupMappingResult{MappedModel: "claude-sonnet-4-5"})
		lowCtx = requeststate.WithRoutePlan(lowCtx, lowPlan)

		require.False(t, ConversionDialect(lowCtx, makeProvider(true), resolvedMapped, protocol.ProtocolAnthropicMessages).StripEffort)
	})

	t.Run("nil_context_and_nil_target_safe", func(t *testing.T) {
		t.Parallel()
		var nilCtx context.Context
		got := ConversionDialect(nilCtx, nil, resolvedMapped, protocol.ProtocolAnthropicMessages)
		require.False(t, got.StripEffort)
	})
}

// TestConversionDialect_SamplingStopAndReasoningCapabilities evaluates
// request-conditional sampling restrictions, Antigravity and Grok field drops,
// and ReasoningCapabilities population. It verifies that unknown capabilities
// retain sampling parameters and returned slices/pointers are detached copies.
func TestConversionDialect_SamplingStopAndReasoningCapabilities(t *testing.T) {
	t.Parallel()

	view := testkit.OfflineView()

	t.Run("conditional_sampling_gpt_and_deepseek", func(t *testing.T) {
		t.Parallel()
		gptBase := view.Resolve(modelrules.Target{
			Platform:         "openai",
			ProviderType:     "apikey",
			Model:            "gpt-5.4",
			UpstreamProtocol: "openai_responses",
		}, modelcatalog.Attributes{})

		noneDialect := ConversionDialect(t.Context(), nil, gptBase.ForRequest(ModelRequestState([]byte(`{"reasoning":{"effort":"none"}}`))), protocol.ProtocolOpenAIResponses)
		require.False(t, noneDialect.DropTemperature)
		require.False(t, noneDialect.DropTopP)

		highDialect := ConversionDialect(t.Context(), nil, gptBase.ForRequest(ModelRequestState([]byte(`{"reasoning":{"effort":"high"}}`))), protocol.ProtocolOpenAIResponses)
		require.True(t, highDialect.DropTemperature)
		require.True(t, highDialect.DropTopP)

		oauthBase := view.Resolve(modelrules.Target{
			Platform:         "openai",
			ProviderType:     "oauth",
			Model:            "gpt-5.4",
			UpstreamProtocol: "openai_responses",
		}, modelcatalog.Attributes{})
		oauthDialect := ConversionDialect(t.Context(), nil, oauthBase, protocol.ProtocolOpenAIResponses)
		require.True(t, oauthDialect.DropTemperature)
		require.True(t, oauthDialect.DropTopP)

		dsBase := view.Resolve(modelrules.Target{
			Platform:         "deepseek",
			ProviderType:     "apikey",
			Model:            "deepseek-v4-pro",
			UpstreamProtocol: "openai_chat_completions",
		}, modelcatalog.Attributes{})
		dsEnabled := ConversionDialect(t.Context(), nil, dsBase.ForRequest(ModelRequestState([]byte(`{"thinking":{"type":"enabled"}}`))), protocol.ProtocolOpenAIChatCompletions)
		require.True(t, dsEnabled.DropTemperature)
		require.False(t, dsEnabled.DropTopP)

		dsDisabled := ConversionDialect(t.Context(), nil, dsBase.ForRequest(ModelRequestState([]byte(`{"thinking":{"type":"disabled"}}`))), protocol.ProtocolOpenAIChatCompletions)
		require.False(t, dsDisabled.DropTemperature)
		require.True(t, dsDisabled.DropTopP)
	})

	t.Run("antigravity_top_k_stop_and_grok_stop_restrictions", func(t *testing.T) {
		t.Parallel()
		agThinking := view.Resolve(modelrules.Target{
			Platform:         "antigravity",
			ProviderType:     "oauth",
			Model:            "gemini-2.5-flash-thinking",
			ClientProtocol:   "anthropic_messages",
			UpstreamProtocol: "gemini_generate_content",
		}, modelcatalog.Attributes{})
		agDialect := ConversionDialect(t.Context(), nil, agThinking, protocol.ProtocolGeminiGenerateContent)
		require.True(t, agDialect.DropTemperature)
		require.True(t, agDialect.DropTopP)
		require.True(t, agDialect.DropTopK)
		require.True(t, agDialect.OmitStopSequences)
		require.Equal(t, 1000, agDialect.Reasoning.BudgetPadding)

		grok45 := view.Resolve(modelrules.Target{
			Platform:         "grok",
			ProviderType:     "apikey",
			Model:            "grok-4.5",
			UpstreamProtocol: "openai_chat_completions",
		}, modelcatalog.Attributes{})
		grokDialect := ConversionDialect(t.Context(), nil, grok45, protocol.ProtocolOpenAIChatCompletions)
		require.True(t, grokDialect.OmitStopSequences)

		unknownRelay := view.Resolve(modelrules.Target{
			Platform:         "openai",
			ProviderType:     "apikey",
			BaseURL:          "https://relay.example/v1",
			Model:            "unknown-relay-model",
			UpstreamProtocol: "openai_responses",
		}, modelcatalog.Attributes{})
		unknownDialect := ConversionDialect(t.Context(), nil, unknownRelay, protocol.ProtocolOpenAIResponses)
		require.False(t, unknownDialect.DropTemperature)
		require.False(t, unknownDialect.DropTopP)
		require.False(t, unknownDialect.DropTopK)
		require.False(t, unknownDialect.OmitStopSequences)
		require.False(t, unknownDialect.Reasoning.ControlsKnown)
	})

	t.Run("reasoning_capabilities_fields_and_detachment", func(t *testing.T) {
		t.Parallel()
		yes := true
		low, high := "low", "high"
		minBudget, maxBudget := 1024, 32000
		outLimit := 64000
		opts := []modelcatalog.ReasoningOption{
			{Type: "toggle"},
			{Type: "effort", Values: []*string{&low, &high}},
			{Type: "budget_tokens", Min: &minBudget, Max: &maxBudget},
		}
		resolved := view.Resolve(modelrules.Target{
			Platform:         "antigravity",
			ProviderType:     "oauth",
			Model:            "claude-opus-4-7-thinking",
			ClientProtocol:   "openai_responses",
			UpstreamProtocol: "gemini_generate_content",
		}, modelcatalog.Attributes{
			Reasoning:        &yes,
			ReasoningOptions: &opts,
			OutputLimit:      &outLimit,
		})

		dialect := ConversionDialect(t.Context(), nil, resolved, protocol.ProtocolGeminiGenerateContent)
		require.True(t, dialect.Reasoning.ControlsKnown)
		require.NotNil(t, dialect.Reasoning.Supported)
		require.True(t, *dialect.Reasoning.Supported)
		require.True(t, dialect.Reasoning.Toggle)
		require.Equal(t, []string{"low", "high"}, dialect.Reasoning.Efforts)
		require.True(t, dialect.Reasoning.Budget)
		require.Equal(t, 1024, dialect.Reasoning.BudgetMin)
		require.Equal(t, 32000, dialect.Reasoning.BudgetMax)
		require.True(t, dialect.Reasoning.ForceBudget)
		require.Equal(t, modelrules.AntigravityAdaptiveBudget, dialect.Reasoning.AdaptiveBudget)
		require.NotNil(t, dialect.Reasoning.OutputLimit)
		require.Equal(t, 64000, *dialect.Reasoning.OutputLimit)
		require.Equal(t, 1000, dialect.Reasoning.BudgetPadding)

		// Mutating returned Reasoning fields must not alias resolved.Attributes.
		dialect.Reasoning.Efforts[0] = "mutated"
		*dialect.Reasoning.Supported = false
		*dialect.Reasoning.OutputLimit = 128
		require.Equal(t, []string{"low", "high"}, resolved.Attributes.Efforts())
		require.True(t, *resolved.Attributes.Reasoning)
		require.Equal(t, 64000, *resolved.Attributes.OutputLimit)
	})

	t.Run("explicit_drop_fields_and_antigravity_count_tokens", func(t *testing.T) {
		t.Parallel()
		manual := modelrules.Resolution{
			Target: modelrules.Target{
				Platform: "antigravity",
				Model:    "gemini-2.5-pro",
				Endpoint: "countTokens",
			},
			Rules: modelrules.ExecutionRules{
				DropFields: []string{"temperature", "topP", "topK", "stopSequences"},
			},
		}
		got := ConversionDialect(t.Context(), nil, manual, protocol.ProtocolGeminiGenerateContent)
		require.True(t, got.DropTemperature)
		require.True(t, got.DropTopP)
		require.True(t, got.DropTopK)
		require.True(t, got.OmitStopSequences)
		require.Empty(t, got.ReplayScheme)
		require.Empty(t, got.PlaceholderToken)
	})
}

// TestConversionDialect_IntegratesWithConvertEngine exercises ConversionDialect
// outputs inside convert.NeedsResponsesToolAdaptation and convert.NewExchange
// across resolved provider routes. It verifies client-tool lowering gates,
// pre-strip Info().Effort retention, and Grok store/include omission.
func TestConversionDialect_IntegratesWithConvertEngine(t *testing.T) {
	t.Parallel()

	service := &routing.ModelAttributeService{
		Catalog: routing.ModelAttributeCatalog{View: testkit.OfflineView},
	}
	customToolBody := []byte(`{"model":"gpt-5.4","input":"patch file","tools":[{"type":"custom","name":"apply_patch"}]}`)

	for _, tc := range []struct {
		name      string             // provider scenario label
		provider  *ExecutionProvider // target provider under test
		model     string             // target model identifier
		wantAdapt bool               // expected NeedsResponsesToolAdaptation result
	}{
		{
			name:      "openai_oauth_skips_lowering",
			provider:  &ExecutionProvider{Record: providercore.Record{ID: 11, Platform: "openai", Type: "oauth"}},
			model:     "gpt-5.4",
			wantAdapt: false,
		},
		{
			name:      "openai_apikey_lowers_custom_tools",
			provider:  &ExecutionProvider{Record: providercore.Record{ID: 12, Platform: "openai", Type: "apikey"}},
			model:     "gpt-5.4",
			wantAdapt: true,
		},
		{
			name:      "grok_oauth_lowers_custom_tools",
			provider:  &ExecutionProvider{Record: providercore.Record{ID: 13, Platform: "grok", Type: "oauth"}},
			model:     "grok-4.5",
			wantAdapt: true,
		},
		{
			name:      "deepseek_apikey_lowers_custom_tools",
			provider:  &ExecutionProvider{Record: providercore.Record{ID: 14, Platform: "deepseek", Type: "apikey"}},
			model:     "deepseek-reasoner",
			wantAdapt: true,
		},
		{
			name:      "kimi_apikey_skips_lowering",
			provider:  &ExecutionProvider{Record: providercore.Record{ID: 15, Platform: "kimi", Type: "apikey"}},
			model:     "kimi-k2.5",
			wantAdapt: false,
		},
		{
			name:      "opencode_apikey_skips_lowering",
			provider:  &ExecutionProvider{Record: providercore.Record{ID: 16, Platform: "opencode", Type: "apikey"}},
			model:     "gpt-5.4",
			wantAdapt: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res, err := ResolveModelAttributes(t.Context(), tc.provider, tc.model, service, "openai_responses")
			require.NoError(t, err)
			dialect := ConversionDialect(t.Context(), tc.provider, res, protocol.ProtocolOpenAIResponses)
			require.Equal(t, tc.wantAdapt, convert.NeedsResponsesToolAdaptation(dialect, customToolBody, nil))
		})
	}

	// Verify StripEffort preserves Info().Effort while stripping wire output_config.effort,
	// and Grok OmitStoreAndInclude omits cross-protocol store and include defaults.
	stripProvider := &ExecutionProvider{
		Record: providercore.Record{
			ID:          21,
			Platform:    "anthropic",
			Type:        "apikey",
			Status:      "active",
			Schedulable: true,
			Credentials: map[string]any{
				"api_key": "sk-ant",
				providercore.EffortModelMappingStripEffortCredentialKey: true,
				providercore.EffortModelMappingCredentialKey: map[string]any{
					"gpt-5": map[string]any{"high": "claude-opus-4-7"},
				},
			},
		},
	}
	ctx := requeststate.WithClientProtocol(t.Context(), protocol.ProtocolOpenAIResponses)
	ctx = requeststate.WithRoutingReasoningEffort(ctx, "high")
	plan := RoutePlanForMapping(ctx, nil, nil, "gpt-5", routing.GroupMappingResult{MappedModel: "gpt-5"})
	ctx = requeststate.WithRoutePlan(ctx, plan)
	routedStrip, err := ProviderForProtocolAttempt(ctx, stripProvider)
	require.NoError(t, err)

	reqBytes := []byte(`{"model":"gpt-5","reasoning":{"effort":"high"},"input":"explain"}`)
	resStrip, err := ResolveModelAttributes(ctx, routedStrip, "claude-opus-4-7", service, "anthropic_messages")
	require.NoError(t, err)
	dialectStrip := ConversionDialect(ctx, routedStrip, resStrip.ForRequest(ModelRequestState(reqBytes)), protocol.ProtocolAnthropicMessages)
	require.True(t, dialectStrip.StripEffort)

	exStrip, err := convert.NewExchange(reqBytes, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect:  dialectStrip,
		},
	})
	require.NoError(t, err)
	require.Equal(t, "high", exStrip.Info().Effort)
	upStripBytes, err := exStrip.UpstreamRequest()
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(upStripBytes, "output_config.effort").Exists())

	grokProvider := &ExecutionProvider{Record: providercore.Record{ID: 31, Platform: "grok", Type: "oauth"}}
	chatBody := []byte(`{"model":"grok-4.5","messages":[{"role":"user","content":"hi"}]}`)
	resGrok, err := ResolveModelAttributes(t.Context(), grokProvider, "grok-4.5", service, "openai_responses")
	require.NoError(t, err)
	dialectGrok := ConversionDialect(t.Context(), grokProvider, resGrok.ForRequest(ModelRequestState(chatBody)), protocol.ProtocolOpenAIResponses)
	exGrok, err := convert.NewExchange(chatBody, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  dialectGrok,
		},
	})
	require.NoError(t, err)
	upGrokBytes, err := exGrok.UpstreamRequest()
	require.NoError(t, err)
	require.False(t, gjson.GetBytes(upGrokBytes, "store").Exists())
	require.False(t, gjson.GetBytes(upGrokBytes, "include").Exists())

	// Cross-protocol Messages -> Responses preserves hosted {"type":"web_search"} on
	// OpenAI API Key and Grok OAuth (including Free-tier companion cache routing).
	webSearchMsgBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"tools":[{"type":"web_search_20250305","name":"web_search"},{"name":"lookup","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"search news"}]}`)
	for _, targetProv := range []*ExecutionProvider{
		{Record: providercore.Record{ID: 41, Platform: "openai", Type: "apikey"}},
		{Record: providercore.Record{ID: 42, Platform: "grok", Type: "oauth", Credentials: map[string]any{"subscription_tier": "free"}, Extra: map[string]any{"grok_client_tool_cache_enabled": false}}},
	} {
		targetModel := "gpt-5.4"
		if targetProv.Record.Platform == "grok" {
			targetModel = "grok-4.5"
		}
		resWS, wsErr := ResolveModelAttributes(t.Context(), targetProv, targetModel, service, "openai_responses")
		require.NoError(t, wsErr)
		dialectWS := ConversionDialect(t.Context(), targetProv, resWS.ForRequest(ModelRequestState(webSearchMsgBody)), protocol.ProtocolOpenAIResponses)
		exWS, wsErr := convert.NewExchange(webSearchMsgBody, convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect:  dialectWS,
			},
		})
		require.NoError(t, wsErr)
		upWS, wsErr := exWS.UpstreamRequest()
		require.NoError(t, wsErr)
		require.Equal(t, "web_search", gjson.GetBytes(upWS, "tools.0.type").String())
		require.Equal(t, "function", gjson.GetBytes(upWS, "tools.1.type").String())
		if targetProv.Record.Platform == "grok" {
			routedGrok, routeErr := ApplyGrokFreeMessagesFunctionToolCacheRoute(upWS, upWS, targetProv, "grok-cache-id")
			require.NoError(t, routeErr)
			require.Equal(t, "x_search", gjson.GetBytes(routedGrok, "tools.2.type").String())
		}
	}
}

// TestConversionDialect_DisabledThinkingRealCatalogModels resolves real
// embedded catalog entries through OfflineView and ConversionDialect when an
// Anthropic Messages request explicitly disables thinking. It protects GPT-5.4,
// GPT-5, GPT-5-Codex, o3, DeepSeek, Kimi, and GPT-4o routes from emitting
// unsupported effort="none" or unsupported reasoning_effort values.
func TestConversionDialect_DisabledThinkingRealCatalogModels(t *testing.T) {
	msgBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":1024,"temperature":1,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"ping"}]}`)
	view := testkit.OfflineView()

	cases := []struct {
		name       string
		target     modelrules.Target
		override   modelcatalog.Attributes
		upstream   protocol.ProtocolID
		wantEffort string
	}{
		{
			name:       "openai gpt-5.4 responses emits none",
			target:     modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-5.4", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_responses"},
			upstream:   protocol.ProtocolOpenAIResponses,
			wantEffort: "none",
		},
		{
			name:       "openai gpt-5.4 chat emits none",
			target:     modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-5.4", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "none",
		},
		{
			name:       "openai gpt-5.3-codex responses emits none",
			target:     modelrules.Target{Platform: "openai", ProviderType: "oauth", Model: "gpt-5.3-codex", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_responses"},
			upstream:   protocol.ProtocolOpenAIResponses,
			wantEffort: "none",
		},
		{
			name:       "openai gpt-5 responses falls back to minimal",
			target:     modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-5", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_responses"},
			upstream:   protocol.ProtocolOpenAIResponses,
			wantEffort: "minimal",
		},
		{
			name:       "openai gpt-5 chat falls back to minimal",
			target:     modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-5", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "minimal",
		},
		{
			name:       "openai o3 responses falls back to low",
			target:     modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "o3", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_responses"},
			upstream:   protocol.ProtocolOpenAIResponses,
			wantEffort: "low",
		},
		{
			name:       "openai o3 chat falls back to low",
			target:     modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "o3", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "low",
		},
		{
			name:       "openai uncatalogued gpt-5-codex responses omits effort",
			target:     modelrules.Target{Platform: "openai", ProviderType: "oauth", Model: "gpt-5-codex", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_responses"},
			upstream:   protocol.ProtocolOpenAIResponses,
			wantEffort: "",
		},
		{
			name:   "openai gpt-5-codex with configured [low,medium,high] falls back to low",
			target: modelrules.Target{Platform: "openai", ProviderType: "oauth", Model: "gpt-5-codex", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_responses"},
			override: modelcatalog.Attributes{
				Reasoning: new(true),
				ReasoningOptions: &[]modelcatalog.ReasoningOption{{
					Type:   "effort",
					Values: []*string{new("low"), new("medium"), new("high")},
				}},
			},
			upstream:   protocol.ProtocolOpenAIResponses,
			wantEffort: "low",
		},
		{
			name:       "openai gpt-4o chat omits reasoning_effort",
			target:     modelrules.Target{Platform: "openai", ProviderType: "apikey", Model: "gpt-4o", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "",
		},
		{
			name:       "deepseek reasoner chat omits reasoning_effort",
			target:     modelrules.Target{Platform: "deepseek", ProviderType: "apikey", Model: "deepseek-reasoner", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "",
		},
		{
			name:       "deepseek chat omits reasoning_effort",
			target:     modelrules.Target{Platform: "deepseek", ProviderType: "apikey", Model: "deepseek-chat", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "",
		},
		{
			name:       "deepseek v4 pro chat omits reasoning_effort",
			target:     modelrules.Target{Platform: "deepseek", ProviderType: "apikey", Model: "deepseek-v4-pro", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "",
		},
		{
			name:       "kimi k2.6 chat omits reasoning_effort",
			target:     modelrules.Target{Platform: "kimi", ProviderType: "apikey", Model: "kimi-k2.6", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "",
		},
		{
			name:       "kimi k2.5 chat omits reasoning_effort",
			target:     modelrules.Target{Platform: "kimi", ProviderType: "apikey", Model: "kimi-k2.5", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "",
		},
		{
			name:       "kimi k3 chat falls back to low",
			target:     modelrules.Target{Platform: "kimi", ProviderType: "apikey", Model: "kimi-k3", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "low",
		},
		{
			name:       "kimi-for-coding chat emits none",
			target:     modelrules.Target{Platform: "kimi", ProviderType: "apikey", CatalogProvider: "kimi-code-plan-cn", Model: "kimi-for-coding", ClientProtocol: "anthropic_messages", UpstreamProtocol: "openai_chat_completions"},
			upstream:   protocol.ProtocolOpenAIChatCompletions,
			wantEffort: "none",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := view.Resolve(tc.target, tc.override).ForRequest(ModelRequestState(msgBody))
			execProv := &ExecutionProvider{
				Record: providercore.Record{
					ID:       77,
					Platform: tc.target.Platform,
					Type:     tc.target.ProviderType,
				},
			}
			dialect := ConversionDialect(t.Context(), execProv, res, tc.upstream)
			ex, err := convert.NewExchange(msgBody, convert.Options{
				Route: convert.Route{
					Client:   protocol.ProtocolAnthropicMessages,
					Upstream: tc.upstream,
					Dialect:  dialect,
				},
			})
			require.NoError(t, err)
			upBytes, err := ex.UpstreamRequest()
			require.NoError(t, err)

			if tc.upstream == protocol.ProtocolOpenAIResponses {
				effRes := gjson.GetBytes(upBytes, "reasoning.effort")
				if tc.wantEffort == "" {
					require.False(t, gjson.GetBytes(upBytes, "reasoning").Exists(), "unexpected reasoning object: %s", string(upBytes))
				} else {
					require.Equal(t, tc.wantEffort, effRes.String(), "body=%s", string(upBytes))
					require.False(t, gjson.GetBytes(upBytes, "reasoning.summary").Exists(), "unexpected summary: %s", string(upBytes))
				}
			} else {
				effRes := gjson.GetBytes(upBytes, "reasoning_effort")
				if tc.wantEffort == "" {
					require.False(t, effRes.Exists(), "unexpected reasoning_effort: %s", string(upBytes))
				} else {
					require.Equal(t, effRes.String(), tc.wantEffort, "body=%s", string(upBytes))
				}
			}
		})
	}
}

// TestPrepareNativeReplay_DialectAndByteIdentity exercises PrepareNativeReplay
// across Messages, HTTP Responses, and WebSocket Responses targets. Clean
// payloads, literal "trc1:" user text, and session.update frames must stay
// byte-for-byte identical, while matching schemes restore vendor tokens and
// foreign or cross-account envelopes strip or retain passback thinking cleanly.
func TestPrepareNativeReplay_DialectAndByteIdentity(t *testing.T) {
	anthCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "EqQBRealAnthropicSig",
		Text:   "Canonical signed thinking text.",
	})
	require.NoError(t, err)
	plainCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemePlain,
		Text:   "Plain tool-turn thought.",
	})
	require.NoError(t, err)
	openAICarrier42, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_openai_acct_42",
		ProviderID: 42,
	})
	require.NoError(t, err)

	t.Run("user_literal_trc1_and_ws_session_update_are_byte_identical", func(t *testing.T) {
		msgLiteral := []byte("{\n  \"model\": \"claude-sonnet-4-6\",\n  \"nonce\": 9007199254740993,\n  \"messages\": [{\"role\": \"user\", \"content\": \"keep literal trc1:plain:eyJ0IjoiaGkifQ untouched\"}]\n}")
		anthTarget := &ExecutionProvider{Record: providercore.Record{ID: 11, Platform: "anthropic", Type: "apikey"}}
		gotMsg, losses, replayErr := PrepareNativeReplay(
			t.Context(),
			anthTarget,
			"claude-sonnet-4-6",
			msgLiteral,
			nil,
			protocol.ProtocolAnthropicMessages,
			protocol.ProtocolAnthropicMessages,
		)
		require.NoError(t, replayErr)
		require.Empty(t, losses)
		require.Equal(t, msgLiteral, gotMsg)

		wsSessionUpdate := []byte("{\n  \"type\": \"session.update\",\n  \"session\": {\"instructions\": \"mention trc1:openai:eyJkIjoiZiJ9 literally\"},\n  \"seq\": 9007199254740993\n}")
		openAITarget := &ExecutionProvider{Record: providercore.Record{ID: 42, Platform: "openai", Type: "oauth"}}
		gotWS, wsLosses, wsErr := PrepareNativeReplay(
			t.Context(),
			openAITarget,
			"gpt-5.4",
			wsSessionUpdate,
			nil,
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolResponsesWebSocket,
		)
		require.NoError(t, wsErr)
		require.Empty(t, wsLosses)
		require.Equal(t, wsSessionUpdate, gotWS)
	})

	t.Run("messages_same_scheme_restores_and_passback_retains_unsigned_thinking", func(t *testing.T) {
		msgBody := []byte(`{"model":"claude-sonnet-4-6","max_tokens":256,"messages":[{"role":"user","content":"check"},{"role":"assistant","content":[{"type":"thinking","thinking":"abbreviated","signature":"` + anthCarrier + `"},{"type":"thinking","thinking":"","signature":"` + plainCarrier + `"},{"type":"text","text":"done"}]}]}`)

		anthTarget := &ExecutionProvider{Record: providercore.Record{ID: 12, Platform: "anthropic", Type: "oauth"}}
		gotAnth, anthLosses, anthErr := PrepareNativeReplay(
			t.Context(),
			anthTarget,
			"claude-sonnet-4-6",
			msgBody,
			nil,
			protocol.ProtocolAnthropicMessages,
			protocol.ProtocolAnthropicMessages,
		)
		require.NoError(t, anthErr)
		require.NotEmpty(t, anthLosses)
		require.Equal(t, "EqQBRealAnthropicSig", gjson.GetBytes(gotAnth, "messages.1.content.0.signature").String())
		require.Equal(t, "Canonical signed thinking text.", gjson.GetBytes(gotAnth, "messages.1.content.0.thinking").String())
		require.Equal(t, "text", gjson.GetBytes(gotAnth, "messages.1.content.1.type").String())

		passbackTarget := &ExecutionProvider{Record: providercore.Record{ID: 13, Platform: "deepseek", Type: "apikey"}}
		gotPassback, _, pbErr := PrepareNativeReplay(
			t.Context(),
			passbackTarget,
			"deepseek-reasoner",
			msgBody,
			nil,
			protocol.ProtocolAnthropicMessages,
			protocol.ProtocolAnthropicMessages,
		)
		require.NoError(t, pbErr)
		require.Equal(t, "thinking", gjson.GetBytes(gotPassback, "messages.1.content.0.type").String())
		require.Equal(t, "Canonical signed thinking text.", gjson.GetBytes(gotPassback, "messages.1.content.0.thinking").String())
		require.False(t, gjson.GetBytes(gotPassback, "messages.1.content.0.signature").Exists())
		require.Equal(t, "Plain tool-turn thought.", gjson.GetBytes(gotPassback, "messages.1.content.1.thinking").String())
		require.False(t, gjson.GetBytes(gotPassback, "messages.1.content.1.signature").Exists())
	})

	t.Run("responses_http_and_ws_same_account_restores_and_cross_account_strips", func(t *testing.T) {
		wsCreate := []byte(`{"type":"response.create","model":"gpt-5.4","store":false,"nonce":9007199254740993,"input":[{"type":"message","role":"user","content":"hi"},{"type":"reasoning","id":"rs_vis","summary":[{"type":"summary_text","text":"visible summary"}],"encrypted_content":"` + openAICarrier42 + `"},{"type":"reasoning","id":"rs_empty","summary":[],"encrypted_content":"` + openAICarrier42 + `"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`)

		sameAcct := &ExecutionProvider{Record: providercore.Record{ID: 42, Platform: "openai", Type: "oauth"}}
		gotSame, sameLosses, sameErr := PrepareNativeReplay(
			t.Context(),
			sameAcct,
			"gpt-5.4",
			wsCreate,
			nil,
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolResponsesWebSocket,
		)
		require.NoError(t, sameErr)
		require.Empty(t, sameLosses)
		require.Equal(t, "gAAAA_openai_acct_42", gjson.GetBytes(gotSame, "input.1.encrypted_content").String())
		require.Equal(t, "gAAAA_openai_acct_42", gjson.GetBytes(gotSame, "input.2.encrypted_content").String())
		require.Equal(t, "9007199254740993", gjson.GetBytes(gotSame, "nonce").Raw)

		otherAcct := &ExecutionProvider{Record: providercore.Record{ID: 99, Platform: "openai", Type: "apikey"}}
		gotOther, otherLosses, otherErr := PrepareNativeReplay(
			t.Context(),
			otherAcct,
			"gpt-5.4",
			wsCreate,
			nil,
			protocol.ProtocolOpenAIResponses,
			protocol.ProtocolOpenAIResponses,
		)
		require.NoError(t, otherErr)
		require.Len(t, otherLosses, 2)
		require.Equal(t, "reasoning", gjson.GetBytes(gotOther, "input.1.type").String())
		require.Equal(t, "visible summary", gjson.GetBytes(gotOther, "input.1.summary.0.text").String())
		require.False(t, gjson.GetBytes(gotOther, "input.1.encrypted_content").Exists())
		require.False(t, gjson.GetBytes(gotOther, "input.1.id").Exists())
		require.Equal(t, "message", gjson.GetBytes(gotOther, "input.2.type").String())
		require.Equal(t, "9007199254740993", gjson.GetBytes(gotOther, "nonce").Raw)
	})
}

// TestConversionDialect_AntigravityModelFamiliesAndThinkingAlignment resolves
// Antigravity claude-* and gemini-* families across resource/author prefixes,
// thinking variants, count_tokens preflights, native pass-through routes, and
// administrator overrides. Every Dialect and ReasoningCapabilities field must
// match resolved modelrules thinking budgets, family-isolated replay schemes,
// and convert.NewExchange encoding behavior.
func TestConversionDialect_AntigravityModelFamiliesAndThinkingAlignment(t *testing.T) {
	t.Parallel()

	view := testkit.OfflineView()
	agProvider := &ExecutionProvider{
		Record: providercore.Record{
			ID:       588,
			Platform: "antigravity",
			Type:     "oauth",
		},
	}
	wantTools := convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch

	t.Run("claude_and_gemini_prefixes_and_thinking_variants", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name              string              // table scenario label
			model             string              // outbound Antigravity model spelling
			clientProto       string              // client protocol identifier
			endpoint          string              // optional count_tokens endpoint
			upstream          protocol.ProtocolID // explicit or empty upstream protocol
			wantScheme        string              // expected ReplayScheme
			wantPlaceholder   string              // expected PlaceholderToken
			wantForceBudget   bool                // expected Reasoning.ForceBudget
			wantAdaptive      int                 // expected Reasoning.AdaptiveBudget
			wantOutputLimit   int                 // expected *Reasoning.OutputLimit (0 means nil)
			wantPadding       int                 // expected Reasoning.BudgetPadding
			wantControlsKnown bool                // expected Reasoning.ControlsKnown
			wantDropSampling  bool                // expected DropTemperature/DropTopP/DropTopK/OmitStopSequences
		}{
			{
				name:              "claude_sonnet_4_5_messages",
				model:             "claude-sonnet-4-5",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeAntigravityClaude,
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      0,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "claude_sonnet_4_5_thinking_messages",
				model:             "claude-sonnet-4-5-thinking",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeAntigravityClaude,
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      0,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "models_prefix_claude_opus_4_6_responses",
				model:             "models/claude-opus-4-6",
				clientProto:       "openai_responses",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeAntigravityClaude,
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      24576,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "anthropic_prefix_claude_opus_4_7_messages",
				model:             "anthropic/claude-opus-4-7",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeAntigravityClaude,
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      24576,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "anthropic_prefix_claude_opus_4_6_thinking_chat",
				model:             "anthropic/claude-opus-4-6-thinking",
				clientProto:       "openai_chat_completions",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeAntigravityClaude,
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      24576,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "publisher_anthropic_claude_opus_4_8_thinking",
				model:             "publishers/anthropic/models/claude-opus-4-8-thinking",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeAntigravityClaude,
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      24576,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "project_publisher_anthropic_claude_sonnet_4_5",
				model:             "projects/my-proj/locations/global/publishers/anthropic/models/claude-sonnet-4-5",
				clientProto:       "openai_responses",
				upstream:          "",
				wantScheme:        ir.SchemeAntigravityClaude,
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      0,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "vertex_anthropic_prefix_with_version_suffix",
				model:             "google-vertex-anthropic/claude-opus-4-6@default",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeAntigravityClaude,
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      24576,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "claude_count_tokens_clears_replay_scheme",
				model:             "anthropic/claude-opus-4-6",
				clientProto:       "anthropic_messages",
				endpoint:          "count_tokens",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   true,
				wantAdaptive:      24576,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "gemini_2_5_pro_messages_uses_65000_cap",
				model:             "gemini-2.5-pro",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeGemini,
				wantPlaceholder:   gemini.DummyThoughtSignature,
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "google_prefix_gemini_2_5_flash_responses",
				model:             "google/gemini-2.5-flash",
				clientProto:       "openai_responses",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeGemini,
				wantPlaceholder:   gemini.DummyThoughtSignature,
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "models_prefix_gemini_3_flash_chat_uses_64000_cap",
				model:             "models/gemini-3-flash-preview",
				clientProto:       "openai_chat_completions",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeGemini,
				wantPlaceholder:   gemini.DummyThoughtSignature,
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   64000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "project_publisher_google_gemini_2_5_pro",
				model:             "projects/my-proj/locations/us-central1/publishers/google/models/gemini-2.5-pro",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeGemini,
				wantPlaceholder:   gemini.DummyThoughtSignature,
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "google_prefix_gemini_3_pro_high_suppresses_sampling_and_stops",
				model:             "google/gemini-3-pro-high",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeGemini,
				wantPlaceholder:   gemini.DummyThoughtSignature,
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
				wantDropSampling:  true,
			},
			{
				name:              "models_prefix_gemini_3_6_flash_tiered_suppresses_sampling_and_stops",
				model:             "models/gemini-3.6-flash-tiered",
				clientProto:       "openai_responses",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeGemini,
				wantPlaceholder:   gemini.DummyThoughtSignature,
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
				wantDropSampling:  true,
			},
			{
				name:              "smaller_catalog_output_limit_is_preserved",
				model:             "gemini-3-pro-image-preview",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeGemini,
				wantPlaceholder:   gemini.DummyThoughtSignature,
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   32768,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "native_gemini_client_keeps_catalog_output_limit_and_zero_padding",
				model:             "gemini-2.5-flash",
				clientProto:       "gemini_generate_content",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        ir.SchemeGemini,
				wantPlaceholder:   gemini.DummyThoughtSignature,
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65536,
				wantPadding:       0,
				wantControlsKnown: true,
			},
			{
				name:              "gemini_count_tokens_clears_scheme_and_placeholder",
				model:             "google/gemini-2.5-pro",
				clientProto:       "anthropic_messages",
				endpoint:          "count_tokens",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: true,
			},
			{
				name:              "foreign_vendor_prefix_clears_scheme_placeholder_and_force_budget",
				model:             "foreign/claude-opus-4-6",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "foreign_publisher_models_path_clears_scheme_placeholder_and_force_budget",
				model:             "publishers/foreign/models/claude-opus-4-6",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "project_foreign_publisher_models_path_clears_scheme_placeholder_and_force_budget",
				model:             "projects/p/locations/l/publishers/foreign/models/claude-opus-4-6",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "custom_nested_models_path_clears_scheme_placeholder_and_force_budget",
				model:             "custom/models/gemini-2.5-pro",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "stacked_models_anthropic_prefix_clears_scheme_placeholder_and_force_budget",
				model:             "models/anthropic/claude-opus-4-6",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "empty_project_location_segments_clear_scheme_placeholder_and_force_budget",
				model:             "projects//locations//publishers/google/models/gemini-2.5-pro",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
			{
				name:              "leading_multi_slash_models_path_clears_scheme_placeholder_and_force_budget",
				model:             "///models/claude-opus-4-6",
				clientProto:       "anthropic_messages",
				upstream:          protocol.ProtocolGeminiGenerateContent,
				wantScheme:        "",
				wantPlaceholder:   "",
				wantForceBudget:   false,
				wantAdaptive:      0,
				wantOutputLimit:   65000,
				wantPadding:       1000,
				wantControlsKnown: false,
			},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				resolved := view.Resolve(modelrules.Target{
					Platform:         "antigravity",
					ProviderType:     "oauth",
					Model:            tc.model,
					ClientProtocol:   tc.clientProto,
					UpstreamProtocol: string(tc.upstream),
					Endpoint:         tc.endpoint,
				}, modelcatalog.Attributes{})

				got := ConversionDialect(t.Context(), agProvider, resolved, tc.upstream)
				require.Equal(t, "antigravity", got.Name)
				require.Equal(t, int64(588), got.ProviderID)
				require.Equal(t, wantTools, got.Tools)
				require.Equal(t, 64, got.ToolNameMaxBytes)
				require.Equal(t, convert.SchemaAntigravity, got.SchemaDialect)
				require.Equal(t, tc.wantScheme, got.ReplayScheme)
				require.Equal(t, convert.HistoryNone, got.ReasoningHistory)
				require.Equal(t, tc.wantPlaceholder, got.PlaceholderToken)
				require.False(t, got.OmitStoreAndInclude)
				require.False(t, got.AllowBillingHeader)
				require.Equal(t, tc.wantForceBudget, got.Reasoning.ForceBudget)
				require.Equal(t, tc.wantAdaptive, got.Reasoning.AdaptiveBudget)
				require.Equal(t, tc.wantPadding, got.Reasoning.BudgetPadding)
				require.Equal(t, tc.wantControlsKnown, got.Reasoning.ControlsKnown)
				if tc.wantOutputLimit == 0 {
					require.Nil(t, got.Reasoning.OutputLimit)
				} else {
					require.NotNil(t, got.Reasoning.OutputLimit)
					require.Equal(t, tc.wantOutputLimit, *got.Reasoning.OutputLimit)
				}
				if tc.wantDropSampling {
					require.True(t, got.DropTemperature)
					require.True(t, got.DropTopP)
					require.True(t, got.DropTopK)
					require.True(t, got.OmitStopSequences)
				}
			})
		}
	})

	t.Run("admin_overrides_and_models_prefix_strip_effort", func(t *testing.T) {
		t.Parallel()
		override := modelcatalog.Attributes{
			OutputLimit: new(90000),
			Temperature: new(true),
			TopP:        new(true),
		}
		resolved := view.Resolve(modelrules.Target{
			Platform:         "antigravity",
			ProviderType:     "oauth",
			Model:            "google/gemini-3-pro-high",
			ClientProtocol:   "anthropic_messages",
			UpstreamProtocol: "gemini_generate_content",
		}, override)
		got := ConversionDialect(t.Context(), agProvider, resolved, protocol.ProtocolGeminiGenerateContent)
		require.NotNil(t, got.Reasoning.OutputLimit)
		require.Equal(t, 90000, *got.Reasoning.OutputLimit)
		require.False(t, got.DropTemperature)
		require.False(t, got.DropTopP)

		stripProv := &ExecutionProvider{
			Record: providercore.Record{
				ID:          589,
				Platform:    "antigravity",
				Type:        "oauth",
				Status:      "active",
				Schedulable: true,
				Credentials: map[string]any{
					"access_token": "tok",
					"project_id":   "proj",
					providercore.EffortModelMappingStripEffortCredentialKey: true,
					providercore.EffortModelMappingCredentialKey: map[string]any{
						"claude-sonnet-4-5": map[string]any{"high": "claude-opus-4-6-thinking"},
					},
				},
			},
		}
		ctx := requeststate.WithClientProtocol(t.Context(), protocol.ProtocolOpenAIResponses)
		ctx = requeststate.WithRoutingReasoningEffort(ctx, "high")
		plan := RoutePlanForMapping(ctx, nil, nil, "models/claude-sonnet-4-5", routing.GroupMappingResult{MappedModel: "models/claude-sonnet-4-5"})
		ctx = requeststate.WithRoutePlan(ctx, plan)
		mappedRes := view.Resolve(modelrules.Target{
			Platform:         "antigravity",
			ProviderType:     "oauth",
			Model:            "claude-opus-4-6-thinking",
			ClientProtocol:   "openai_responses",
			UpstreamProtocol: "gemini_generate_content",
		}, modelcatalog.Attributes{})
		require.True(t, ConversionDialect(ctx, stripProv, mappedRes, protocol.ProtocolGeminiGenerateContent).StripEffort)
	})

	t.Run("exchange_thinking_and_continuity_parity_with_legacy_antigravity", func(t *testing.T) {
		t.Parallel()
		// 1. Effort-only Claude Opus 4.7 converts effort to thinkingBudget via ForceBudget (matching Antigravity budget rules).
		opus47Res := view.Resolve(modelrules.Target{
			Platform:         "antigravity",
			ProviderType:     "oauth",
			Model:            "anthropic/claude-opus-4-7",
			ClientProtocol:   "openai_responses",
			UpstreamProtocol: "gemini_generate_content",
		}, modelcatalog.Attributes{})
		respEffortBody := []byte(`{"model":"claude-opus-4-7","reasoning":{"effort":"high"},"max_output_tokens":70000,"input":"solve"}`)
		opus47Dialect := ConversionDialect(t.Context(), agProvider, opus47Res.ForRequest(ModelRequestState(respEffortBody)), protocol.ProtocolGeminiGenerateContent)
		exOpus47, err := convert.NewExchange(respEffortBody, convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  opus47Dialect,
			},
		})
		require.NoError(t, err)
		upOpus47, err := exOpus47.UpstreamRequest()
		require.NoError(t, err)
		require.Equal(t, int64(10240), gjson.GetBytes(upOpus47, "generationConfig.thinkingConfig.thinkingBudget").Int())
		require.False(t, gjson.GetBytes(upOpus47, "generationConfig.thinkingConfig.thinkingLevel").Exists())
		require.Equal(t, int64(64000), gjson.GetBytes(upOpus47, "generationConfig.maxOutputTokens").Int())

		// 2. Adaptive Claude Opus 4.6 without effort uses 24,576 adaptive budget and pads maxOutputTokens by 1,000.
		opus46Res := view.Resolve(modelrules.Target{
			Platform:         "antigravity",
			ProviderType:     "oauth",
			Model:            "models/claude-opus-4-6-thinking",
			ClientProtocol:   "anthropic_messages",
			UpstreamProtocol: "gemini_generate_content",
		}, modelcatalog.Attributes{})
		adaptiveBody := []byte(`{"model":"claude-opus-4-6-thinking","max_tokens":4096,"thinking":{"type":"adaptive"},"messages":[{"role":"user","content":"analyze"}]}`)
		opus46Dialect := ConversionDialect(t.Context(), agProvider, opus46Res.ForRequest(ModelRequestState(adaptiveBody)), protocol.ProtocolGeminiGenerateContent)
		exAdaptive, err := convert.NewExchange(adaptiveBody, convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  opus46Dialect,
			},
		})
		require.NoError(t, err)
		upAdaptive, err := exAdaptive.UpstreamRequest()
		require.NoError(t, err)
		require.Equal(t, int64(24576), gjson.GetBytes(upAdaptive, "generationConfig.thinkingConfig.thinkingBudget").Int())
		require.Equal(t, int64(25576), gjson.GetBytes(upAdaptive, "generationConfig.maxOutputTokens").Int())

		// 3. Unsigned historical thinking on antigravity_claude inlines as text and suppresses thinkingConfig.
		unsignedHistoryBody := []byte(`{"model":"claude-opus-4-6-thinking","max_tokens":4096,"thinking":{"type":"enabled","budget_tokens":2048},"messages":[{"role":"user","content":"q1"},{"role":"assistant","content":[{"type":"thinking","thinking":"unsigned thought","signature":""},{"type":"text","text":"a1"}]},{"role":"user","content":"q2"}]}`)
		exDowngrade, err := convert.NewExchange(unsignedHistoryBody, convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  opus46Dialect,
			},
		})
		require.NoError(t, err)
		upDowngrade, err := exDowngrade.UpstreamRequest()
		require.NoError(t, err)
		require.False(t, gjson.GetBytes(upDowngrade, "generationConfig.thinkingConfig").Exists())
		require.Equal(t, "unsigned thought", gjson.GetBytes(upDowngrade, "contents.1.parts.0.text").String())
		require.False(t, gjson.GetBytes(upDowngrade, "contents.1.parts.0.thought").Bool())

		// 4. Signed trc1:antigravity_claude carrier restores exact text + signature on Claude, and drops on Gemini.
		agClaudeCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAntigravityClaude,
			Data:   "sig_ag_claude_v1",
			Text:   "Exact signed Antigravity Claude thought.",
		})
		require.NoError(t, err)
		carrierHistoryBody := []byte(`{"model":"claude-opus-4-6-thinking","max_tokens":4096,"thinking":{"type":"enabled","budget_tokens":2048},"messages":[{"role":"user","content":"q1"},{"role":"assistant","content":[{"type":"thinking","thinking":"abbreviated","signature":"` + agClaudeCarrier + `"},{"type":"text","text":"a1"}]},{"role":"user","content":"q2"}]}`)
		exSignedClaude, err := convert.NewExchange(carrierHistoryBody, convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  opus46Dialect,
			},
		})
		require.NoError(t, err)
		upSignedClaude, err := exSignedClaude.UpstreamRequest()
		require.NoError(t, err)
		require.True(t, gjson.GetBytes(upSignedClaude, "contents.1.parts.0.thought").Bool())
		require.Equal(t, "Exact signed Antigravity Claude thought.", gjson.GetBytes(upSignedClaude, "contents.1.parts.0.text").String())
		require.Equal(t, "sig_ag_claude_v1", gjson.GetBytes(upSignedClaude, "contents.1.parts.0.thoughtSignature").String())

		gemRes := view.Resolve(modelrules.Target{
			Platform:         "antigravity",
			ProviderType:     "oauth",
			Model:            "google/gemini-2.5-pro",
			ClientProtocol:   "anthropic_messages",
			UpstreamProtocol: "gemini_generate_content",
		}, modelcatalog.Attributes{})
		gemDialect := ConversionDialect(t.Context(), agProvider, gemRes.ForRequest(ModelRequestState(carrierHistoryBody)), protocol.ProtocolGeminiGenerateContent)
		exMismatchGem, err := convert.NewExchange(carrierHistoryBody, convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  gemDialect,
			},
		})
		require.NoError(t, err)
		upMismatchGem, err := exMismatchGem.UpstreamRequest()
		require.NoError(t, err)
		require.NotContains(t, string(upMismatchGem), "sig_ag_claude_v1")
		require.Equal(t, "a1", gjson.GetBytes(upMismatchGem, "contents.1.parts.0.text").String())
	})
}

// TestConversionDialect_CrossProtocolMessagesThinkingRealCatalogModels resolves
// real embedded catalog entries and unlisted Claude names through OfflineView,
// ConversionDialect, convert.NewExchange, and ApplyModelAttributeParameters for
// OpenAI Responses and Chat Completions requests targeting Anthropic Messages.
// Medium-and-above efforts must emit budget thinking on claude-haiku-4-5,
// claude-opus-4-6, claude-sonnet-4-6, and unlisted models, and adaptive
// thinking without budget_tokens on claude-opus-4-7 and claude-sonnet-5.
func TestConversionDialect_CrossProtocolMessagesThinkingRealCatalogModels(t *testing.T) {
	view := testkit.OfflineView()
	anthProvider := &ExecutionProvider{
		Record: providercore.Record{
			ID:       301,
			Platform: "anthropic",
			Type:     "apikey",
		},
	}

	sources := []struct {
		name           string
		proto          protocol.ProtocolID
		clientProtoKey string
		body           []byte
	}{
		{
			name:           "responses",
			proto:          protocol.ProtocolOpenAIResponses,
			clientProtoKey: "openai_responses",
			body:           []byte(`{"model":"gpt-5.4","temperature":0.7,"reasoning":{"effort":"medium","summary":"auto"},"input":"ping"}`),
		},
		{
			name:           "chat",
			proto:          protocol.ProtocolOpenAIChatCompletions,
			clientProtoKey: "openai_chat_completions",
			body:           []byte(`{"model":"gpt-5.4","temperature":0.7,"reasoning_effort":"medium","messages":[{"role":"user","content":"ping"}]}`),
		},
	}

	models := []struct {
		model         string
		wantThinkType string
		wantBudget    int64
		wantHasBudget bool
		wantEffort    string
		wantKnown     bool
	}{
		{
			model:         "claude-haiku-4-5",
			wantThinkType: "enabled",
			wantBudget:    4096,
			wantHasBudget: true,
			wantEffort:    "",
			wantKnown:     true,
		},
		{
			model:         "claude-opus-4-6",
			wantThinkType: "enabled",
			wantBudget:    4096,
			wantHasBudget: true,
			wantEffort:    "medium",
			wantKnown:     true,
		},
		{
			model:         "claude-sonnet-4-6",
			wantThinkType: "enabled",
			wantBudget:    4096,
			wantHasBudget: true,
			wantEffort:    "medium",
			wantKnown:     true,
		},
		{
			model:         "claude-opus-4-7",
			wantThinkType: "adaptive",
			wantHasBudget: false,
			wantEffort:    "medium",
			wantKnown:     true,
		},
		{
			model:         "claude-sonnet-5",
			wantThinkType: "adaptive",
			wantHasBudget: false,
			wantEffort:    "medium",
			wantKnown:     true,
		},
		{
			model:         "claude-custom-unlisted",
			wantThinkType: "enabled",
			wantBudget:    4096,
			wantHasBudget: true,
			wantEffort:    "medium",
			wantKnown:     false,
		},
	}

	for _, src := range sources {
		for _, mc := range models {
			t.Run(src.name+"_"+mc.model, func(t *testing.T) {
				res := view.Resolve(modelrules.Target{
					Platform:         "anthropic",
					ProviderType:     "apikey",
					Model:            mc.model,
					ClientProtocol:   src.clientProtoKey,
					UpstreamProtocol: "anthropic_messages",
				}, modelcatalog.Attributes{}).ForRequest(ModelRequestState(src.body))
				dialect := ConversionDialect(t.Context(), anthProvider, res, protocol.ProtocolAnthropicMessages)
				require.Equal(t, mc.wantKnown, dialect.Reasoning.ControlsKnown)
				require.True(t, dialect.DropTemperature, "active Claude thinking must drop temperature on %s", mc.model)

				ex, err := convert.NewExchange(src.body, convert.Options{
					Route: convert.Route{
						Client:   src.proto,
						Upstream: protocol.ProtocolAnthropicMessages,
						Dialect:  dialect,
					},
				})
				require.NoError(t, err)
				upBytes, err := ex.UpstreamRequest()
				require.NoError(t, err)
				prepared, err := ApplyModelAttributeParameters(upBytes, res)
				require.NoError(t, err)

				require.Equal(t, mc.wantThinkType, gjson.GetBytes(prepared, "thinking.type").String(), string(prepared))
				require.Equal(t, mc.wantHasBudget, gjson.GetBytes(prepared, "thinking.budget_tokens").Exists(), string(prepared))
				if mc.wantHasBudget {
					require.Equal(t, mc.wantBudget, gjson.GetBytes(prepared, "thinking.budget_tokens").Int(), string(prepared))
				}
				if mc.wantEffort == "" {
					require.False(t, gjson.GetBytes(prepared, "output_config").Exists(), string(prepared))
				} else {
					require.Equal(t, mc.wantEffort, gjson.GetBytes(prepared, "output_config.effort").String(), string(prepared))
				}
				require.False(t, gjson.GetBytes(prepared, "temperature").Exists(), string(prepared))
			})
		}
	}
}
