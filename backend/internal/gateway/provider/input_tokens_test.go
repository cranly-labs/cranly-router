package provider

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// TestPrepareAnthropicInputTokens_FiveFieldProjectionAndCarrierUnpacking
// supplies an Anthropic Messages count_tokens payload carrying untrimmed model
// whitespace, billing headers, 64-bit integer schema bounds, and mixed
// continuity envelopes. Preparation must preserve ingress model identity,
// project only the five official /v1/responses/input_tokens fields, unpack
// matching trc1:openai: carriers, and strip foreign, cross-account, and bare
// legacy signatures.
func TestPrepareAnthropicInputTokens_FiveFieldProjectionAndCarrierUnpacking(t *testing.T) {
	sameAcctCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_prep_acct_77",
		ProviderID: 77,
	})
	require.NoError(t, err)
	redactedCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_prep_redacted_77",
		Redacted:   true,
		ProviderID: 77,
	})
	require.NoError(t, err)
	crossAcctCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_prep_acct_88",
		ProviderID: 88,
	})
	require.NoError(t, err)
	foreignCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "EqQBForeignClaudeSig",
		Text:   "Foreign Anthropic thought.",
	})
	require.NoError(t, err)

	body := []byte(`{
		"model":"  claude-sonnet-4-5  ",
		"max_tokens":2048,
		"stream":true,
		"temperature":0.4,
		"speed":"fast",
		"system":[
			{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.330"},
			{"type":"text","text":"Count tokens carefully."}
		],
		"messages":[
			{"role":"user","content":"keep literal trc1:openai:demo"},
			{"role":"assistant","content":[
				{"type":"thinking","thinking":"","signature":"` + sameAcctCarrier + `"},
				{"type":"redacted_thinking","data":"` + redactedCarrier + `"},
				{"type":"thinking","thinking":"","signature":"` + crossAcctCarrier + `"},
				{"type":"thinking","thinking":"stub","signature":"` + foreignCarrier + `"},
				{"type":"thinking","thinking":"legacy","signature":"bare_legacy_sig"},
				{"type":"text","text":"prior answer"}
			]},
			{"role":"user","content":"follow up"}
		],
		"tools":[{"name":"lookup","description":"Find entry","input_schema":{"type":"object","properties":{"id":{"type":"integer","minimum":9007199254740993}}}}],
		"tool_choice":{"type":"tool","name":"lookup"}
	}`)

	prov := &ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           77,
			Platform:     capability.PlatformOpenAI,
			Type:         capability.ProviderTypeAPIKey,
			Credentials:  map[string]any{"api_key": "sk-test"},
		},
	}

	prepared, err := PrepareAnthropicInputTokens(body, prov, "gpt-5.4")
	require.NoError(t, err)
	require.Equal(t, "  claude-sonnet-4-5  ", prepared.OriginalModel)
	require.Equal(t, "  claude-sonnet-4-5  ", prepared.NormalizedModel)
	require.Equal(t, "gpt-5.4", prepared.BillingModel)
	require.Equal(t, "gpt-5.4", prepared.UpstreamModel)
	require.Equal(t, "gpt-5.4", prepared.Payload.Model)
	require.Equal(t, "gpt-5.4", prepared.Request.Model)

	marshaled, err := prepared.MarshalBody()
	require.NoError(t, err)
	assertOnlyCountingTopLevelFields(t, marshaled)
	require.Contains(t, string(marshaled), `"minimum":9007199254740993`)
	require.Contains(t, string(marshaled), `keep literal trc1:openai:demo`)
	require.NotContains(t, string(marshaled), "x-anthropic-billing-header:")
	require.NotContains(t, string(marshaled), "gAAAA_prep_acct_88")
	require.NotContains(t, string(marshaled), "EqQBForeignClaudeSig")
	require.NotContains(t, string(marshaled), "bare_legacy_sig")
	require.Equal(t, "developer", gjson.GetBytes(marshaled, "input.0.role").String())
	require.Equal(t, "user", gjson.GetBytes(marshaled, "input.1.role").String())
	require.Equal(t, "gAAAA_prep_acct_77", gjson.GetBytes(marshaled, "input.2.encrypted_content").String())
	require.Equal(t, "gAAAA_prep_redacted_77", gjson.GetBytes(marshaled, "input.3.encrypted_content").String())
	require.Equal(t, "message", gjson.GetBytes(marshaled, "input.4.type").String())
	require.Equal(t, "function", gjson.GetBytes(marshaled, "tool_choice.type").String())
	require.Equal(t, "lookup", gjson.GetBytes(marshaled, "tool_choice.name").String())

	localEst, err := prepared.EstimateLocal()
	require.NoError(t, err)
	require.Positive(t, localEst)
}

// TestPrepareNativeInputTokensAndEstimateLocally exercises native Responses
// counting preparation across store=false continuity carriers, invalid UTF-8
// string bytes, blank-model and duplicate-key errors, and custom base URLs.
// Prepared bodies must unpack same-account carriers, strip cross-account
// envelopes, serialize invalid UTF-8 bytes as U+FFFD without dropping Request
// tools, and select local estimation for non-official hosts.
func TestPrepareNativeInputTokensAndEstimateLocally(t *testing.T) {
	acct7Carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_native_prep_7",
		ProviderID: 7,
	})
	require.NoError(t, err)

	nativeBody := []byte(`{
		"model":"  gpt-5.4  ",
		"store":false,
		"max_output_tokens":512,
		"instructions":"Count concise.",
		"input":[
			{"type":"message","role":"user","content":"literal trc1:plain:demo"},
			{"type":"reasoning","id":"rs_synth","summary":[{"type":"summary_text","text":"summary"}],"encrypted_content":"` + acct7Carrier + `"},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}
		],
		"tools":[{"type":"custom","name":"apply_patch","format":{"type":"grammar","syntax":"lark","definition":"start: \"x\""}}]
	}`)

	sameProv := &ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           7,
			Platform:     capability.PlatformOpenAI,
			Type:         capability.ProviderTypeAPIKey,
		},
	}
	crossProv := &ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           8,
			Platform:     capability.PlatformOpenAI,
			Type:         capability.ProviderTypeAPIKey,
		},
	}

	prepSame, err := PrepareNativeInputTokens(nativeBody, sameProv)
	require.NoError(t, err)
	require.Equal(t, "gpt-5.4", prepSame.OriginalModel)
	sameBytes, err := prepSame.MarshalBody()
	require.NoError(t, err)
	assertOnlyCountingTopLevelFields(t, sameBytes)
	require.Equal(t, "gAAAA_native_prep_7", gjson.GetBytes(sameBytes, "input.1.encrypted_content").String())
	require.Contains(t, string(sameBytes), `"syntax":"lark"`)

	prepCross, err := PrepareNativeInputTokens(nativeBody, crossProv)
	require.NoError(t, err)
	crossBytes, err := prepCross.MarshalBody()
	require.NoError(t, err)
	assertOnlyCountingTopLevelFields(t, crossBytes)
	require.Equal(t, "summary", gjson.GetBytes(crossBytes, "input.1.summary.0.text").String())
	require.False(t, gjson.GetBytes(crossBytes, "input.1.encrypted_content").Exists())
	require.False(t, gjson.GetBytes(crossBytes, "input.1.id").Exists())

	_, err = PrepareNativeInputTokens([]byte(`{"model":"   ","input":"hi"}`), sameProv)
	require.Error(t, err)
	_, err = PrepareNativeInputTokens([]byte(`{"model":"gpt-5","model":"gpt-4o","input":"hi"}`), sameProv)
	require.Error(t, err)

	// Invalid UTF-8 bytes inside input, instructions, tools, and tool_choice must
	// marshal cleanly as U+FFFD rather than failing json/v2 strict UTF-8 checks.
	minimalInvalidUTF8 := append([]byte(`{"model":"gpt-5","input":"prefix `), 0xff, '"', '}')
	prepMinUTF8, err := PrepareNativeInputTokens(minimalInvalidUTF8, sameProv)
	require.NoError(t, err)
	minUTF8Bytes, err := prepMinUTF8.MarshalBody()
	require.NoError(t, err)
	require.True(t, utf8.Valid(minUTF8Bytes))
	require.Contains(t, string(minUTF8Bytes), "prefix \ufffd")

	richInvalidUTF8 := slices.Concat(
		[]byte(`{"model":"gpt-5","instructions":"inst `),
		[]byte{0xff},
		[]byte(`","input":"body `),
		[]byte{0xfe},
		[]byte(`","tools":[{"type":"function","name":"lookup","description":"desc `),
		[]byte{0xff},
		[]byte(`","parameters":{"type":"object"}}],"tool_choice":"auto"}`),
	)

	prepRichUTF8, err := PrepareNativeInputTokens(richInvalidUTF8, sameProv)
	require.NoError(t, err)
	require.Len(t, prepRichUTF8.Request.Tools, 1)
	richUTF8Bytes, err := prepRichUTF8.MarshalBody()
	require.NoError(t, err)
	require.True(t, utf8.Valid(richUTF8Bytes))
	require.Equal(t, 3, strings.Count(string(richUTF8Bytes), "\ufffd"))
	estRichUTF8, err := prepRichUTF8.EstimateLocal()
	require.NoError(t, err)
	require.Positive(t, estRichUTF8)

	anthInvalidUTF8 := append([]byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"anth `), 0xff, '"', '}', ']', '}')
	prepAnthUTF8, err := PrepareAnthropicInputTokens(anthInvalidUTF8, sameProv, "gpt-5.4")
	require.NoError(t, err)
	anthUTF8Bytes, err := prepAnthUTF8.MarshalBody()
	require.NoError(t, err)
	require.True(t, utf8.Valid(anthUTF8Bytes))
	require.Contains(t, string(anthUTF8Bytes), "anth \ufffd")

	require.True(t, EstimateInputTokensLocally(nil))
	require.False(t, EstimateInputTokensLocally(&ExecutionProvider{
		Record: providercore.Record{Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeOAuth},
	}))
	require.False(t, EstimateInputTokensLocally(&ExecutionProvider{
		Record: providercore.Record{Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey, Credentials: map[string]any{"base_url": "https://api.openai.com/v1"}},
	}))
	require.True(t, EstimateInputTokensLocally(&ExecutionProvider{
		Record: providercore.Record{Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey, Credentials: map[string]any{"base_url": "https://relay.example/v1"}},
	}))
	require.True(t, EstimateInputTokensLocally(&ExecutionProvider{
		Record: providercore.Record{Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey, Credentials: map[string]any{"base_url": "://bad-url"}},
	}))
}

// TestPrepareInputTokens_CorpusAndWireValidation validates counting preparation
// across every Anthropic Messages and OpenAI Responses request fixture in
// DefaultCorpus. All marshaled counting bodies must expose only the five
// counting fields, pass strict Responses request validation, unpack or strip
// trc1 carriers cleanly, and preserve source tools losslessly.
func TestPrepareInputTokens_CorpusAndWireValidation(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	openAIProv := &ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           101,
			Platform:     capability.PlatformOpenAI,
			Type:         capability.ProviderTypeAPIKey,
			Credentials:  map[string]any{"api_key": "sk-diff"},
		},
	}

	t.Run("anthropic_messages_wire_and_client_corpus", func(t *testing.T) {
		var fixtures []testkit.Fixture
		for _, src := range []testkit.FixtureSource{testkit.SourceWire, testkit.SourceCorpusClient} {
			matched, err := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   src,
				Protocol: protocol.ProtocolAnthropicMessages,
				Kind:     testkit.FixtureRequest,
			})
			require.NoError(t, err)
			fixtures = append(fixtures, matched...)
		}
		require.NotEmpty(t, fixtures)

		for _, f := range fixtures {
			t.Run(f.Path, func(t *testing.T) {
				prep, err := PrepareAnthropicInputTokens(f.Data, openAIProv, "gpt-5.4")
				require.NoError(t, err)
				require.Equal(t, "gpt-5.4", prep.UpstreamModel)

				irBytes, err := prep.MarshalBody()
				require.NoError(t, err)
				assertOnlyCountingTopLevelFields(t, irBytes)

				violations := testkit.ValidateRequestRules(
					protocol.ProtocolOpenAIResponses,
					irBytes,
					testkit.RequestValidationOptions{},
				)
				require.Empty(t, violations, "input_tokens payload violated Responses rules on %s: %v", f.Path, violations)
				gjson.GetBytes(irBytes, "input").ForEach(func(_, item gjson.Result) bool {
					require.NotContains(t, item.Get("encrypted_content").String(), "trc1:", "trc1 carrier must not leak in encrypted_content on %s", f.Path)
					return true
				})

				switch f.Scenario {
				case "document_input":
					require.Contains(t, string(irBytes), `"input_file"`)
				case "tool_result_error":
					require.Contains(t, string(irBytes), `is_error`)
				case "legacy_history", "reasoning_token_foreign", "reasoning_tool_replay":
					require.NotContains(t, string(irBytes), `"encrypted_content"`)
				case "reasoning_carrier_native", "reasoning_carrier_roundtrip":
					require.Contains(t, string(irBytes), `"encrypted_content"`)
				}

				est, err := prep.EstimateLocal()
				require.NoError(t, err)
				require.Positive(t, est)
			})
		}
	})

	t.Run("openai_responses_wire_and_client_corpus", func(t *testing.T) {
		var fixtures []testkit.Fixture
		for _, src := range []testkit.FixtureSource{testkit.SourceWire, testkit.SourceCorpusClient} {
			matched, err := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   src,
				Protocol: protocol.ProtocolOpenAIResponses,
				Kind:     testkit.FixtureRequest,
			})
			require.NoError(t, err)
			fixtures = append(fixtures, matched...)
		}
		require.NotEmpty(t, fixtures)

		for _, f := range fixtures {
			t.Run(f.Path, func(t *testing.T) {
				prep, err := PrepareNativeInputTokens(f.Data, openAIProv)
				require.NoError(t, err)

				irBytes, err := prep.MarshalBody()
				require.NoError(t, err)
				assertOnlyCountingTopLevelFields(t, irBytes)
				gjson.GetBytes(irBytes, "input").ForEach(func(_, item gjson.Result) bool {
					require.NotContains(t, item.Get("encrypted_content").String(), "trc1:", "trc1 carrier leaked in encrypted_content on %s", f.Path)
					return true
				})

				if bytes.Contains(f.Data, []byte("trc1:")) {
					if f.Scenario == "reasoning_carrier_native" {
						require.Contains(t, string(irBytes), "gAAAAABnNativeOpenAISameAcct01")
						require.Contains(t, string(irBytes), "trc1:openai:demo")
					} else {
						require.NotContains(t, string(irBytes), `"encrypted_content"`)
					}
				} else {
					srcThree := projectSelectedCountingMembersForTest(t, f.Data, "instructions", "input", "tool_choice")
					irThree := projectSelectedCountingMembersForTest(t, irBytes, "instructions", "input", "tool_choice")
					diffs, diffErr := testkit.JSONDiff(srcThree, irThree, testkit.WithAllowInvalidUTF8(true))
					require.NoError(t, diffErr)
					require.Empty(t, diffs, "native counting altered non-model fields on %s:\n%s", f.Path, testkit.FormatJSONDiff(diffs))
				}

				srcTools := projectSelectedCountingMembersForTest(t, f.Data, "tools")
				irTools := projectSelectedCountingMembersForTest(t, irBytes, "tools")
				srcToolDiffs, diffErr := testkit.JSONDiff(srcTools, irTools, testkit.WithAllowInvalidUTF8(true))
				require.NoError(t, diffErr)
				require.Empty(t, srcToolDiffs, "native counting altered source tools on %s:\n%s", f.Path, testkit.FormatJSONDiff(srcToolDiffs))

				est, err := prep.EstimateLocal()
				require.NoError(t, err)
				require.Positive(t, est)
			})
		}
	})
}

// assertOnlyCountingTopLevelFields decodes body as a single JSON object and
// verifies that every top-level key belongs to {model, instructions, input,
// tools, tool_choice}. Unexpected sibling members or malformed JSON fail t
// immediately.
func assertOnlyCountingTopLevelFields(t *testing.T, body []byte) {
	t.Helper()
	allowed := []string{"model", "instructions", "input", "tools", "tool_choice"}
	var top map[string]jsontext.Value
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	require.NoError(t, json.UnmarshalDecode(dec, &top))
	for k := range top {
		require.True(t, slices.Contains(allowed, k), "unexpected field %q in projected counting payload %s", k, string(body))
	}
}

// projectSelectedCountingMembersForTest extracts only the requested top-level
// keys from raw into deterministic JSON bytes for targeted JSONDiff checks.
// Null members and empty tools arrays are omitted so absent and empty slices
// compare consistently.
func projectSelectedCountingMembersForTest(t *testing.T, raw []byte, keys ...string) []byte {
	t.Helper()
	var src map[string]jsontext.Value
	require.NoError(t, json.Unmarshal(raw, &src, jsontext.AllowInvalidUTF8(true)))
	dst := make(map[string]jsontext.Value, len(keys))
	for _, k := range keys {
		v, ok := src[k]
		if !ok {
			continue
		}
		trimmed := bytes.TrimSpace(v)
		if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) || (k == "tools" && bytes.Equal(trimmed, []byte("[]"))) {
			continue
		}
		dst[k] = v
	}
	out, err := json.Marshal(dst, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
	require.NoError(t, err)
	return out
}
