package tokenestimate

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	protocolopenai "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestEstimateOpenAIInputTokens_RequestSamples covers plain message input,
// instructions with a tool schema, and parts with tool output. Their exact
// local totals protect framing and text-counting choices.
func TestEstimateOpenAIInputTokens_RequestSamples(t *testing.T) {
	cases := []struct {
		name string  // descriptive request case label
		req  Request // supplied local estimation request
		want int     // expected input token estimate
	}{
		{
			name: "simple text input",
			req: Request{
				Model: "gpt-5",
				Input: json.RawMessage(`[{"role":"user","content":"hello world"}]`),
			},
			want: 6,
		},
		{
			name: "instructions plus tool schema",
			req: Request{
				Model:        "gpt-5",
				Instructions: "You are helpful.",
				Input:        json.RawMessage(`[{"role":"user","content":"lookup weather in shanghai"}]`),
				Tools: []protocolopenai.ResponsesTool{
					{
						Type:        "function",
						Name:        "lookup_weather",
						Description: "Look up current weather",
						Parameters:  json.RawMessage(`{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}`),
					},
				},
			},
			want: 50,
		},
		{
			name: "input parts and tool output",
			req: Request{
				Model: "gpt-4.1",
				Input: json.RawMessage(`[
					{"role":"user","content":[{"type":"input_text","text":"first line"},{"type":"input_text","text":"second line"}]},
					{"type":"function_call_output","call_id":"call_123","output":"{\"ok\":true}"}
				]`),
			},
			want: 24,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Responses(tt.req)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

// TestEstimateGrokCountTokens_AnthropicRequests exercises text, system blocks,
// tools, and an empty Anthropic conversation for Grok. Accepted converted
// requests must receive a positive local input estimate.
func TestEstimateGrokCountTokens_AnthropicRequests(t *testing.T) {
	cases := []struct {
		name string // descriptive request case label
		body string // Anthropic JSON request bytes
	}{
		{
			name: "simple message",
			body: `{"model":"grok-4","messages":[{"role":"user","content":"hello world"}]}`,
		},
		{
			name: "system blocks and tools",
			body: `{
				"model":"grok-4",
				"system":[{"type":"text","text":"You are helpful."}],
				"messages":[{"role":"user","content":[{"type":"text","text":"look up the weather"}]}],
				"tools":[{"name":"lookup_weather","description":"Look up weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],
				"tool_choice":{"type":"auto"}
			}`,
		},
		{
			name: "empty conversation uses positive minimum",
			body: `{"model":"grok-4","messages":[]}`,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Anthropic([]byte(tt.body))
			require.NoError(t, err)
			require.Positive(t, got)
		})
	}
}

// TestEstimateGrokCountTokens_RejectsInvalidRequests supplies malformed JSON,
// a missing model, and unsupported message content. Each input must return an
// estimation error instead of an accepted token count.
func TestEstimateGrokCountTokens_RejectsInvalidRequests(t *testing.T) {
	for _, body := range []string{
		`{`,
		`{"messages":[{"role":"user","content":"hello"}]}`,
		`{"model":"grok-4","messages":[{"role":"user","content":{"unexpected":true}}]}`,
	} {
		_, err := Anthropic([]byte(body))
		require.Error(t, err, "body=%s", body)
	}
}

// TestOpenAIInputTokensEncodingForModel checks recent GPT names and older
// GPT-4 or GPT-3.5 prefixes. Each name must select the expected local
// tokenizer encoding.
func TestOpenAIInputTokensEncodingForModel(t *testing.T) {
	cases := []struct {
		model string // supplied tokenizer model name
		want  string // expected tokenizer encoding label
	}{
		{model: "gpt-5", want: "o200k_base"},
		{model: "gpt-5.3-codex", want: "o200k_base"},
		{model: "gpt-4o-mini", want: "o200k_base"},
		{model: "gpt-4.1", want: "o200k_base"},
		{model: "gpt-4-turbo", want: "cl100k_base"},
		{model: "gpt-3.5-turbo", want: "cl100k_base"},
	}

	for _, tt := range cases {
		t.Run(tt.model, func(t *testing.T) {
			require.Equal(t, tt.want, string(EncodingForModel(tt.model)))
		})
	}
}

// TestCountingPayloadAndResponsesBytes_LosslessProjectionAndExactParity supplies
// modeled Responses inputs alongside custom Lark grammars, 64-bit integer
// bounds, invalid UTF-8 string bytes, and duplicate JSON object keys.
// ProjectCountingPayload, MarshalCountingPayload, and ResponsesBytes must match
// legacy Responses counts on modeled shapes while preserving raw schemas,
// replacing invalid UTF-8 with U+FFFD during serialization, and rejecting
// duplicate keys.
func TestCountingPayloadAndResponsesBytes_LosslessProjectionAndExactParity(t *testing.T) {
	rawSamples := []struct {
		name string // test scenario label
		body string // raw Responses JSON payload
		want int    // expected local token estimate
	}{
		{
			name: "simple_message_array",
			body: `{"model":"gpt-5","store":false,"nonce":9007199254740993,"input":[{"role":"user","content":"hello world"}]}`,
			want: 6,
		},
		{
			name: "instructions_plus_tool_schema",
			body: `{"model":"gpt-5","instructions":"You are helpful.","input":[{"role":"user","content":"lookup weather in shanghai"}],"tools":[{"type":"function","name":"lookup_weather","description":"Look up current weather","parameters":{"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}}]}`,
			want: 50,
		},
		{
			name: "parts_image_data_header_and_structured_output",
			body: `{"model":"gpt-4.1","input":[{"role":"user","content":[{"type":"input_text","text":"first line"},{"type":"input_image","image_url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg=="}]},{"type":"function_call_output","call_id":"call_123","output":[{"type":"input_text","text":"ok"}]}]}`,
			want: 34,
		},
	}

	for _, tc := range rawSamples {
		t.Run(tc.name, func(t *testing.T) {
			gotIR, err := ResponsesBytes([]byte(tc.body))
			require.NoError(t, err)
			require.Equal(t, tc.want, gotIR)

			var legacyReq Request
			require.NoError(t, json.Unmarshal([]byte(tc.body), &legacyReq))
			gotLegacy, err := Responses(legacyReq)
			require.NoError(t, err)
			require.Equal(t, gotLegacy, gotIR)
		})
	}

	t.Run("projects_five_fields_and_preserves_custom_tool_and_uint64_bounds", func(t *testing.T) {
		raw := []byte(`{"model":"gpt-5.4","store":false,"max_output_tokens":512,"instructions":"Use patch.","input":"edit file","tools":[{"type":"custom","name":"apply_patch","format":{"type":"grammar","syntax":"lark","definition":"start: \"ok\""}},{"type":"function","name":"query","parameters":{"type":"object","properties":{"id":{"type":"integer","minimum":9007199254740993}}}}],"tool_choice":{"type":"custom","name":"apply_patch"}}`)
		projected, err := ProjectCountingPayload(raw)
		require.NoError(t, err)
		require.False(t, projected.IsZero())

		marshaled, err := MarshalCountingPayload(projected)
		require.NoError(t, err)
		text := string(marshaled)
		require.True(t, strings.HasPrefix(text, `{"model":"gpt-5.4","instructions":"Use patch.","input":"edit file","tools":[`))
		require.Contains(t, text, `"minimum":9007199254740993`, text)
		require.Contains(t, text, `"syntax":"lark"`, text)
		require.NotContains(t, text, `"store"`)
		require.NotContains(t, text, `"max_output_tokens"`)

		cnt, err := EstimateCountingPayload(projected)
		require.NoError(t, err)
		require.Positive(t, cnt)
	})

	t.Run("tolerates_invalid_utf8_and_rejects_duplicate_keys", func(t *testing.T) {
		invalidUTF8 := append([]byte(`{"model":"gpt-5","input":"prefix `), 0xff, '"', '}')
		cnt, err := ResponsesBytes(invalidUTF8)
		require.NoError(t, err)
		require.Positive(t, cnt)

		projSimple, err := ProjectCountingPayload(invalidUTF8)
		require.NoError(t, err)
		marshaledSimple, err := MarshalCountingPayload(projSimple)
		require.NoError(t, err)
		require.True(t, utf8.Valid(marshaledSimple))
		require.Contains(t, string(marshaledSimple), "prefix \ufffd")

		fullInvalid := slices.Concat(
			[]byte(`{"model":"gpt-5","instructions":"inst `),
			[]byte{0xff},
			[]byte(`","input":[{"role":"user","content":"msg `),
			[]byte{0xfe},
			[]byte(`"}],"tools":[{"type":"function","name":"fn","description":"desc `),
			[]byte{0xff},
			[]byte(`","parameters":{"type":"object"}}],"tool_choice":{"type":"function","name":"fn","note":"`),
			[]byte{0xff},
			[]byte(`"}}`),
		)

		projFull, err := ProjectCountingPayload(fullInvalid)
		require.NoError(t, err)
		marshaledFull, err := MarshalCountingPayload(projFull)
		require.NoError(t, err)
		require.True(t, utf8.Valid(marshaledFull))
		require.Equal(t, 4, strings.Count(string(marshaledFull), "\ufffd"))
		cntFull, err := EstimateCountingPayload(projFull)
		require.NoError(t, err)
		require.Positive(t, cntFull)

		_, err = ProjectCountingPayload([]byte(`{"model":"gpt-5","model":"gpt-4o","input":"hi"}`))
		require.Error(t, err)
		_, err = Anthropic([]byte(`{"model":"claude-sonnet-4-5","model":"claude-opus-4-1","messages":[{"role":"user","content":"hi"}]}`))
		require.Error(t, err)
	})
}

// TestAnthropicAndResponsesBytes_CorpusValidation exercises Anthropic and
// ResponsesBytes across every wire and recorded client request fixture in
// DefaultCorpus. Every fixture must preserve whitespace invariance, match its
// projected EstimateCountingPayload value, and unpack OpenAI carriers while
// stripping foreign carriers.
func TestAnthropicAndResponsesBytes_CorpusValidation(t *testing.T) {
	corpus := testkit.DefaultCorpus()

	t.Run("openai_responses_wire_and_client_corpus", func(t *testing.T) {
		var allFixtures []testkit.Fixture
		for _, src := range []testkit.FixtureSource{testkit.SourceWire, testkit.SourceCorpusClient} {
			matched, err := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   src,
				Protocol: protocol.ProtocolOpenAIResponses,
				Kind:     testkit.FixtureRequest,
			})
			require.NoError(t, err)
			allFixtures = append(allFixtures, matched...)
		}
		require.NotEmpty(t, allFixtures)

		for _, f := range allFixtures {
			t.Run(f.Path, func(t *testing.T) {
				gotIR, err := ResponsesBytes(f.Data)
				require.NoError(t, err)
				require.GreaterOrEqual(t, gotIR, 0)

				compactedInput, err := compactCountingRawValue(f.Data)
				require.NoError(t, err)
				gotIRCompacted, err := ResponsesBytes(compactedInput)
				require.NoError(t, err)
				require.Equal(t, gotIRCompacted, gotIR, "ResponsesBytes must be invariant to insignificant JSON whitespace on %s", f.Path)

				irPayload, err := ProjectCountingPayload(compactedInput)
				require.NoError(t, err)
				var legacyReq Request
				require.NoError(t, json.Unmarshal(compactedInput, &legacyReq))
				gotLegacy, err := Responses(legacyReq)
				require.NoError(t, err)

				// Non-tool counting fields {model, instructions, input, tool_choice}
				// must produce identical token estimates on every Responses fixture.
				irNoTools, err := EstimateCountingPayload(CountingPayload{
					Model:        irPayload.Model,
					Instructions: irPayload.Instructions,
					Input:        irPayload.Input,
					ToolChoice:   irPayload.ToolChoice,
				})
				require.NoError(t, err)
				legNoTools, err := Responses(Request{
					Model:        legacyReq.Model,
					Instructions: legacyReq.Instructions,
					Input:        legacyReq.Input,
					ToolChoice:   legacyReq.ToolChoice,
				})
				require.NoError(t, err)
				require.Equal(t, legNoTools, irNoTools, "four-field Responses estimate mismatch on %s", f.Path)

				if len(irPayload.Tools) == 0 {
					require.Equal(t, gotLegacy, gotIR, "toolless Responses count mismatch on %s", f.Path)
				} else {
					require.GreaterOrEqual(t, gotIR, gotLegacy, "ResponsesBytes dropped tool tokens relative to Request on %s", f.Path)
				}
			})
		}
	})

	t.Run("anthropic_messages_wire_and_client_corpus", func(t *testing.T) {
		var allFixtures []testkit.Fixture
		for _, src := range []testkit.FixtureSource{testkit.SourceWire, testkit.SourceCorpusClient} {
			matched, err := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   src,
				Protocol: protocol.ProtocolAnthropicMessages,
				Kind:     testkit.FixtureRequest,
			})
			require.NoError(t, err)
			allFixtures = append(allFixtures, matched...)
		}
		require.NotEmpty(t, allFixtures)

		for _, f := range allFixtures {
			t.Run(f.Path, func(t *testing.T) {
				got, err := Anthropic(f.Data)
				require.NoError(t, err, "Anthropic failed on %s", f.Path)
				require.Positive(t, got)

				irPayload := buildAnthropicCountingPayloadForTest(t, f.Data)
				wantFromPayload, pErr := EstimateCountingPayload(irPayload)
				require.NoError(t, pErr)
				if wantFromPayload < openAIInputTokensFallbackMinimum {
					wantFromPayload = openAIInputTokensFallbackMinimum
				}
				require.Equal(t, wantFromPayload, got, "Anthropic diverged from EstimateCountingPayload on %s", f.Path)
			})
		}
	})

	t.Run("anthropic_unpacks_openai_carrier_and_strips_foreign_carriers", func(t *testing.T) {
		oaCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeOpenAI,
			Data:   "gAAAA_tokenestimate_unpacked_ciphertext",
		})
		require.NoError(t, err)
		anthCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "EqQBForeignAnthropicSig",
			Text:   "Foreign Anthropic thought dropped for OpenAI counting.",
		})
		require.NoError(t, err)

		withOpenAI := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"q"},{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":"` + oaCarrier + `"},{"type":"text","text":"a"}]},{"role":"user","content":"next"}]}`)
		withForeign := []byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"q"},{"role":"assistant","content":[{"type":"thinking","thinking":"stub","signature":"` + anthCarrier + `"},{"type":"text","text":"a"}]},{"role":"user","content":"next"}]}`)

		cntOpenAI, err := Anthropic(withOpenAI)
		require.NoError(t, err)
		cntForeign, err := Anthropic(withForeign)
		require.NoError(t, err)
		// Same-scheme OpenAI carrier retains reasoning item overhead, whereas foreign Anthropic carrier is stripped.
		require.Greater(t, cntOpenAI, cntForeign)
	})
}

// buildAnthropicCountingPayloadForTest converts body using Anthropic's counting
// dialect and returns the projected CountingPayload with Model set to the
// original client model string. Any conversion or projection failure aborts t
// immediately so callers receive a validated CountingPayload.
func buildAnthropicCountingPayloadForTest(t *testing.T, body []byte) CountingPayload {
	t.Helper()
	ex, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect: convert.Dialect{
				Name:             "openai",
				Tools:            convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch,
				ToolNameMaxBytes: defaultCountingToolNameMaxBytes,
				ReplayScheme:     ir.SchemeOpenAI,
			},
		},
	})
	require.NoError(t, err)
	convBytes, err := ex.UpstreamRequest()
	require.NoError(t, err)
	payload, err := ProjectCountingPayload(convBytes)
	require.NoError(t, err)
	payload.Model = ex.Info().Model
	return payload
}
