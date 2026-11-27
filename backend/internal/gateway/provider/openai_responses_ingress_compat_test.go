package provider

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestNormalizeOpenAIResponsesLegacyIngressMessagesOnly supplies Chat
// system/tool history with a stale previous-response ID and no native input.
// Conversion creates paired Responses call/output input, keeps stream=false,
// and removes legacy messages plus continuation.
func TestNormalizeOpenAIResponsesLegacyIngressMessagesOnly(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","stream":false,"messages":[{"role":"system","content":"repo policy"},{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"hello\"}"}}]},{"role":"tool","tool_call_id":"call_1","content":"ok"}],"previous_response_id":"resp_stale"}`)

	normalized, changed, err := NormalizeOpenAIResponsesLegacyIngress(body)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, gjson.GetBytes(normalized, "messages").Exists())
	require.False(t, gjson.GetBytes(normalized, "previous_response_id").Exists())
	require.Equal(t, "system", gjson.GetBytes(normalized, "input.0.role").String())
	require.Equal(t, "repo policy", gjson.GetBytes(normalized, "input.0.content.0.text").String())
	require.Equal(t, "function_call", gjson.GetBytes(normalized, "input.1.type").String())
	require.Equal(t, "call_1", gjson.GetBytes(normalized, "input.1.call_id").String())
	require.Equal(t, "function_call_output", gjson.GetBytes(normalized, "input.2.type").String())
	require.Equal(t, "call_1", gjson.GetBytes(normalized, "input.2.call_id").String())
	require.Equal(t, gjson.False, gjson.GetBytes(normalized, "stream").Type)
}

// TestNormalizeOpenAIResponsesLegacyIngressConvertsChatTopLevelFields supplies
// Chat tools, nested choice, token cap, effort, and tier alongside messages.
// Conversion produces flat tool fields, Responses output cap and reasoning,
// while removing legacy top-level spellings.
func TestNormalizeOpenAIResponsesLegacyIngressConvertsChatTopLevelFields(t *testing.T) {
	body := []byte(`{"model":"gpt-4.1","messages":[{"role":"user","content":"weather?"}],"tools":[{"type":"function","function":{"name":"lookup","description":"Lookup weather","parameters":{"type":"object"}}}],"tool_choice":{"type":"function","function":{"name":"lookup"}},"max_tokens":64,"reasoning_effort":"high","service_tier":"flex","temperature":0.2,"top_p":0.7}`)

	normalized, changed, err := NormalizeOpenAIResponsesLegacyIngress(body)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "user", gjson.GetBytes(normalized, "input.0.role").String())
	require.Equal(t, "function", gjson.GetBytes(normalized, "tools.0.type").String())
	require.Equal(t, "lookup", gjson.GetBytes(normalized, "tools.0.name").String())
	require.False(t, gjson.GetBytes(normalized, "tools.0.function").Exists())
	require.Equal(t, "function", gjson.GetBytes(normalized, "tool_choice.type").String())
	require.Equal(t, "lookup", gjson.GetBytes(normalized, "tool_choice.name").String())
	require.False(t, gjson.GetBytes(normalized, "tool_choice.function").Exists())
	require.Equal(t, int64(128), gjson.GetBytes(normalized, "max_output_tokens").Int())
	require.Equal(t, "high", gjson.GetBytes(normalized, "reasoning.effort").String())
	require.Equal(t, "auto", gjson.GetBytes(normalized, "reasoning.summary").String())
	require.Equal(t, "flex", gjson.GetBytes(normalized, "service_tier").String())
	require.False(t, gjson.GetBytes(normalized, "max_tokens").Exists())
	require.False(t, gjson.GetBytes(normalized, "reasoning_effort").Exists())
}

// TestNormalizeOpenAIResponsesLegacyIngressNativeInputRemainsAuthoritative
// supplies native input with legacy messages and nested tool fields. The
// native history, tool/choice shape, and previous-response ID remain, while
// messages is removed.
func TestNormalizeOpenAIResponsesLegacyIngressNativeInputRemainsAuthoritative(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","input":[{"id":"msg_native","type":"message","role":"user","content":[{"type":"input_text","text":"keep me"}]}],"messages":[{"role":"user","content":"do not merge"}],"tools":[{"type":"function","function":{"name":"native_shape"}}],"tool_choice":{"type":"function","function":{"name":"native_shape"}},"previous_response_id":"resp_keep"}`)

	normalized, changed, err := NormalizeOpenAIResponsesLegacyIngress(body)
	require.NoError(t, err)
	require.True(t, changed)
	require.False(t, gjson.GetBytes(normalized, "messages").Exists())
	require.Equal(t, "msg_native", gjson.GetBytes(normalized, "input.0.id").String())
	require.Equal(t, int64(1), gjson.GetBytes(normalized, "input.#").Int())
	require.Equal(t, "native_shape", gjson.GetBytes(normalized, "tools.0.function.name").String())
	require.False(t, gjson.GetBytes(normalized, "tools.0.name").Exists())
	require.Equal(t, "native_shape", gjson.GetBytes(normalized, "tool_choice.function.name").String())
	require.Equal(t, "resp_keep", gjson.GetBytes(normalized, "previous_response_id").String())
}

// TestNormalizeOpenAIResponsesLegacyIngressKeepsPromptAliasAndDropsCommands
// supplies string prompt and commands with no native input. The prompt text
// becomes input and both legacy fields disappear.
func TestNormalizeOpenAIResponsesLegacyIngressKeepsPromptAliasAndDropsCommands(t *testing.T) {
	body := []byte(`{"model":"gpt-4.1","prompt":"hello","commands":[{"name":"legacy"}]}`)

	normalized, changed, err := NormalizeOpenAIResponsesLegacyIngress(body)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, "hello", gjson.GetBytes(normalized, "input").String())
	require.False(t, gjson.GetBytes(normalized, "prompt").Exists())
	require.False(t, gjson.GetBytes(normalized, "commands").Exists())
}

// TestNormalizeOpenAIResponsesLegacyIngressPreservesNativePromptTemplate
// supplies an object prompt template as native Responses data. Normalization
// reports no change, retains its template ID, and creates no input field.
func TestNormalizeOpenAIResponsesLegacyIngressPreservesNativePromptTemplate(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","prompt":{"id":"pmpt_abc","version":"7","variables":{"topic":"ownership"}}}`)

	normalized, changed, err := NormalizeOpenAIResponsesLegacyIngress(body)
	require.NoError(t, err)
	require.False(t, changed)
	require.JSONEq(t, string(body), string(normalized))
	require.Equal(t, "pmpt_abc", gjson.GetBytes(normalized, "prompt.id").String())
	require.False(t, gjson.GetBytes(normalized, "input").Exists())
}

// TestNormalizeOpenAIResponsesLegacyIngressPreservesUnknownPromptShapeWhileDroppingCommands
// supplies a two-element array prompt alongside legacy commands and no native
// input. Normalization drops commands while retaining both prompt elements and
// leaving input absent.
func TestNormalizeOpenAIResponsesLegacyIngressPreservesUnknownPromptShapeWhileDroppingCommands(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","prompt":["one","two"],"commands":[{"name":"legacy"}]}`)

	normalized, changed, err := NormalizeOpenAIResponsesLegacyIngress(body)
	require.NoError(t, err)
	require.True(t, changed)
	require.Equal(t, int64(2), gjson.GetBytes(normalized, "prompt.#").Int())
	require.False(t, gjson.GetBytes(normalized, "input").Exists())
	require.False(t, gjson.GetBytes(normalized, "commands").Exists())
}

// TestNormalizeOpenAIResponsesLegacyIngress_LosslessHybridAndUint64Preservation
// supplies hybrid Chat messages, flat Responses tools, legacy functions and
// function_call fields, authoritative native input, prompt aliases, 64-bit
// integer literals, invalid UTF-8 bytes, and duplicate object keys.
// Normalization must convert Chat-shaped controls while preserving native
// Responses shapes, unrounded 64-bit integers, and unknown top-level extensions
// and rejecting duplicate keys.
func TestNormalizeOpenAIResponsesLegacyIngress_LosslessHybridAndUint64Preservation(t *testing.T) {
	t.Run("messages_system_and_tools_with_uint64_and_unknown_top_level_fields", func(t *testing.T) {
		body := []byte(`{
			"model":"gpt-5.4",
			"stream":false,
			"messages":[
				{"role":"system","content":"repo policy"},
				{"role":"assistant","tool_calls":[{"id":"call_1","type":"function","function":{"name":"lookup","arguments":"{\"id\":9007199254740993}"}}]},
				{"role":"tool","tool_call_id":"call_1","content":"ok"}
			],
			"tools":[{"type":"function","function":{"name":"lookup","description":"Lookup record","parameters":{"type":"object","properties":{"id":{"type":"integer","minimum":9007199254740993}}}}}],
			"tool_choice":{"type":"function","function":{"name":"lookup"}},
			"max_tokens":64,
			"reasoning_effort":"high",
			"service_tier":"flex",
			"previous_response_id":"resp_stale",
			"prompt_cache_key":"pc_keep",
			"custom_ext":{"trace_id":9007199254740995,"nested":"untouched"}
		}`)

		normalized, changed, err := NormalizeOpenAIResponsesLegacyIngress(body)
		require.NoError(t, err)
		require.True(t, changed)
		require.False(t, gjson.GetBytes(normalized, "messages").Exists())
		require.False(t, gjson.GetBytes(normalized, "previous_response_id").Exists())
		require.False(t, gjson.GetBytes(normalized, "max_tokens").Exists())
		require.False(t, gjson.GetBytes(normalized, "reasoning_effort").Exists())
		require.Equal(t, gjson.False, gjson.GetBytes(normalized, "stream").Type)
		require.Equal(t, "system", gjson.GetBytes(normalized, "input.0.role").String())
		require.Equal(t, "repo policy", gjson.GetBytes(normalized, "input.0.content.0.text").String())
		require.Equal(t, "function_call", gjson.GetBytes(normalized, "input.1.type").String())
		require.Equal(t, "call_1", gjson.GetBytes(normalized, "input.1.call_id").String())
		require.Contains(t, gjson.GetBytes(normalized, "input.1.arguments").String(), "9007199254740993")
		require.Equal(t, "function_call_output", gjson.GetBytes(normalized, "input.2.type").String())
		require.Equal(t, "lookup", gjson.GetBytes(normalized, "tools.0.name").String())
		require.False(t, gjson.GetBytes(normalized, "tools.0.function").Exists())
		require.Equal(t, "9007199254740993", gjson.GetBytes(normalized, "tools.0.parameters.properties.id.minimum").Raw)
		require.Equal(t, "function", gjson.GetBytes(normalized, "tool_choice.type").String())
		require.Equal(t, "lookup", gjson.GetBytes(normalized, "tool_choice.name").String())
		require.Equal(t, int64(128), gjson.GetBytes(normalized, "max_output_tokens").Int())
		require.Equal(t, "high", gjson.GetBytes(normalized, "reasoning.effort").String())
		require.Equal(t, "auto", gjson.GetBytes(normalized, "reasoning.summary").String())
		require.Equal(t, "flex", gjson.GetBytes(normalized, "service_tier").String())
		require.Equal(t, "pc_keep", gjson.GetBytes(normalized, "prompt_cache_key").String())
		require.Equal(t, "9007199254740995", gjson.GetBytes(normalized, "custom_ext.trace_id").Raw)
	})

	t.Run("hybrid_messages_with_flat_responses_tools_and_reasoning_stays_intact", func(t *testing.T) {
		body := []byte(`{
			"model":"gpt-5.4",
			"messages":[{"role":"user","content":"patch file"}],
			"tools":[
				{"type":"custom","name":"apply_patch","format":{"type":"grammar","syntax":"lark","definition":"start: \"x\""}},
				{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"n":{"type":"integer","maximum":9007199254740993}}}}
			],
			"tool_choice":{"type":"custom","name":"apply_patch"},
			"reasoning":{"effort":"medium","summary":"detailed"}
		}`)

		normalized, changed, err := NormalizeOpenAIResponsesLegacyIngress(body)
		require.NoError(t, err)
		require.True(t, changed)
		require.False(t, gjson.GetBytes(normalized, "messages").Exists())
		require.Equal(t, "user", gjson.GetBytes(normalized, "input.0.role").String())
		require.Equal(t, "custom", gjson.GetBytes(normalized, "tools.0.type").String())
		require.Equal(t, "lark", gjson.GetBytes(normalized, "tools.0.format.syntax").String())
		require.Equal(t, "9007199254740993", gjson.GetBytes(normalized, "tools.1.parameters.properties.n.maximum").Raw)
		require.Equal(t, "custom", gjson.GetBytes(normalized, "tool_choice.type").String())
		require.Equal(t, "detailed", gjson.GetBytes(normalized, "reasoning.summary").String())
	})

	t.Run("native_input_authoritative_over_messages_and_legacy_functions_converted", func(t *testing.T) {
		mixed := []byte(`{
			"model":"gpt-5.4",
			"input":[{"id":"msg_native","type":"message","role":"user","content":[{"type":"input_text","text":"keep me"}],"seq":9007199254740993}],
			"messages":[{"role":"user","content":"discard me"}],
			"tools":[{"type":"function","function":{"name":"native_shape"}}],
			"tool_choice":{"type":"function","function":{"name":"native_shape"}},
			"previous_response_id":"resp_keep"
		}`)
		normMixed, changed, err := NormalizeOpenAIResponsesLegacyIngress(mixed)
		require.NoError(t, err)
		require.True(t, changed)
		require.False(t, gjson.GetBytes(normMixed, "messages").Exists())
		require.Equal(t, "msg_native", gjson.GetBytes(normMixed, "input.0.id").String())
		require.Equal(t, "9007199254740993", gjson.GetBytes(normMixed, "input.0.seq").Raw)
		require.Equal(t, "native_shape", gjson.GetBytes(normMixed, "tools.0.function.name").String())
		require.Equal(t, "resp_keep", gjson.GetBytes(normMixed, "previous_response_id").String())

		legacyFns := []byte(`{
			"model":"gpt-4.1",
			"messages":[{"role":"user","content":"call legacy"}],
			"functions":[{"name":"legacy_fn","description":"Legacy tool","parameters":{"type":"object"}}],
			"function_call":{"name":"legacy_fn"}
		}`)
		normFns, changed, err := NormalizeOpenAIResponsesLegacyIngress(legacyFns)
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, "legacy_fn", gjson.GetBytes(normFns, "tools.0.name").String())
		require.Equal(t, "function", gjson.GetBytes(normFns, "tool_choice.type").String())
		require.Equal(t, "legacy_fn", gjson.GetBytes(normFns, "tool_choice.name").String())
		require.False(t, gjson.GetBytes(normFns, "functions").Exists())
		require.False(t, gjson.GetBytes(normFns, "function_call").Exists())
	})

	t.Run("prompt_commands_invalid_utf8_and_duplicate_key_guards", func(t *testing.T) {
		promptBody := []byte(`{"model":"gpt-4.1","prompt":"hello","commands":[{"name":"legacy"}]}`)
		normPrompt, changed, err := NormalizeOpenAIResponsesLegacyIngress(promptBody)
		require.NoError(t, err)
		require.True(t, changed)
		require.Equal(t, "hello", gjson.GetBytes(normPrompt, "input").String())
		require.False(t, gjson.GetBytes(normPrompt, "prompt").Exists())
		require.False(t, gjson.GetBytes(normPrompt, "commands").Exists())

		templateBody := []byte("{\n  \"model\": \"gpt-5.4\",\n  \"prompt\": {\"id\": \"pmpt_abc\"}\n}")
		normTmpl, changed, err := NormalizeOpenAIResponsesLegacyIngress(templateBody)
		require.NoError(t, err)
		require.False(t, changed)
		require.Equal(t, templateBody, normTmpl)

		invalidUTF8 := append([]byte(`{"model":"gpt-5.4","messages":[{"role":"user","content":"raw `), 0xff, '"', '}', ']', '}')
		normUTF8, changed, err := NormalizeOpenAIResponsesLegacyIngress(invalidUTF8)
		require.NoError(t, err)
		require.True(t, changed)
		require.True(t, gjson.GetBytes(normUTF8, "input").Exists())

		_, changed, err = NormalizeOpenAIResponsesLegacyIngress([]byte(`{"model":"gpt-5","model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}`))
		require.Error(t, err)
		require.False(t, changed)
	})
}

// TestNormalizeOpenAIResponsesLegacyIngress_CorpusValidation replays every
// OpenAI Responses and Chat Completions request fixture from DefaultCorpus
// through NormalizeOpenAIResponsesLegacyIngress. Native Responses fixtures
// without legacy keys must return byte-identical slices, while Chat Completions
// fixtures must strip legacy keys and pass strict Responses request validation.
func TestNormalizeOpenAIResponsesLegacyIngress_CorpusValidation(t *testing.T) {
	corpus := testkit.DefaultCorpus()

	t.Run("native_responses_corpus_returns_unchanged_bytes", func(t *testing.T) {
		var respFixtures []testkit.Fixture
		for _, src := range []testkit.FixtureSource{testkit.SourceWire, testkit.SourceCorpusClient} {
			matched, err := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   src,
				Protocol: protocol.ProtocolOpenAIResponses,
				Kind:     testkit.FixtureRequest,
			})
			require.NoError(t, err)
			respFixtures = append(respFixtures, matched...)
		}
		require.NotEmpty(t, respFixtures)

		for _, f := range respFixtures {
			t.Run(f.Path, func(t *testing.T) {
				got, changed, err := NormalizeOpenAIResponsesLegacyIngress(f.Data)
				require.NoError(t, err)
				if !changed {
					require.True(t, bytes.Equal(f.Data, got), "unmodified Responses fixture must stay byte-identical on %s", f.Path)
				} else {
					require.True(t, gjson.GetBytes(got, "input").Exists())
				}
			})
		}
	})

	t.Run("chat_completions_wire_and_client_corpus_as_legacy_ingress", func(t *testing.T) {
		var chatFixtures []testkit.Fixture
		for _, src := range []testkit.FixtureSource{testkit.SourceWire, testkit.SourceCorpusClient} {
			matched, err := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   src,
				Protocol: protocol.ProtocolOpenAIChatCompletions,
				Kind:     testkit.FixtureRequest,
			})
			require.NoError(t, err)
			chatFixtures = append(chatFixtures, matched...)
		}
		require.NotEmpty(t, chatFixtures)

		for _, f := range chatFixtures {
			t.Run(f.Path, func(t *testing.T) {
				got, changed, err := NormalizeOpenAIResponsesLegacyIngress(f.Data)
				require.NoError(t, err)
				require.True(t, changed)

				for _, removedKey := range []string{
					"messages",
					"max_tokens",
					"max_completion_tokens",
					"reasoning_effort",
					"functions",
					"function_call",
					"previous_response_id",
				} {
					require.False(t, gjson.GetBytes(got, removedKey).Exists(), "normalizer kept %s on %s", removedKey, f.Path)
				}
				require.True(t, gjson.GetBytes(got, "input").Exists())

				projected := projectConvertedIngressValidationBody(t, got)
				violations := testkit.ValidateRequestRules(
					protocol.ProtocolOpenAIResponses,
					projected,
					testkit.RequestValidationOptions{},
				)
				require.Empty(t, violations, "legacy ingress normalizer violated Responses rules on %s: %v", f.Path, violations)

				switch f.Scenario {
				case "document_input":
					require.Contains(t, string(got), `"input_file"`)
				case "refusal":
					require.Contains(t, string(got), `"refusal"`)
				case "reasoning_tool_replay":
					require.NotContains(t, string(got), "<thinking>")
				}
			})
		}
	})
}

// projectConvertedIngressValidationBody extracts the Responses fields produced
// by legacy ingress normalization into a standalone JSON object. Chat-only
// sibling keys left for downstream handlers (such as stream_options or
// response_format) are excluded so strict Responses validation inspects only
// normalized fields.
func projectConvertedIngressValidationBody(t *testing.T, body []byte) []byte {
	t.Helper()
	var src map[string]jsontext.Value
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	require.NoError(t, json.UnmarshalDecode(dec, &src))
	dst := make(map[string]jsontext.Value)
	for _, k := range []string{"model", "instructions", "input", "tools", "tool_choice", "max_output_tokens", "temperature", "top_p", "reasoning", "service_tier", "stream"} {
		if v, ok := src[k]; ok {
			dst[k] = v
		}
	}
	out, err := json.Marshal(dst, jsontext.AllowInvalidUTF8(true))
	require.NoError(t, err)
	return out
}
