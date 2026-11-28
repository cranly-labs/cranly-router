package convert

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestPrepareNativeReplay_ByteIdenticalWithoutTokenPathCarriers feeds every
// Messages and Responses fixture lacking trc1 envelopes at protocol token paths
// through PrepareNativeReplay, along with literal "trc1:" strings in user text,
// WS session.update frames, and invalid UTF-8 bytes. Every input must return
// byte-for-byte unchanged with zero reported losses.
func TestPrepareNativeReplay_ByteIdenticalWithoutTokenPathCarriers(t *testing.T) {
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
	} {
		for _, fix := range allProtocolRequestFixtures(t, proto) {
			switch fix.Scenario {
			case "reasoning_token_foreign", "reasoning_carrier_roundtrip",
				"reasoning_carrier_native", "reasoning_encrypted_only":
				continue
			}
			if fix.Scenario == "lab_reasoning_tool" && bytes.Contains(fix.Data, []byte(ir.CarrierPrefix)) {
				continue
			}
			t.Run(fix.Path, func(t *testing.T) {
				route := Route{
					Client:   proto,
					Upstream: proto,
					Dialect:  Dialect{ReplayScheme: ir.SchemeAnthropic},
				}
				if proto == protocol.ProtocolOpenAIResponses {
					route.Dialect = Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 101}
				}
				got, losses, err := PrepareNativeReplay(route, fix.Data)
				require.NoError(t, err)
				require.Empty(t, losses)
				require.True(t, bytes.Equal(fix.Data, got), "fixture %s mutated despite having no trc1 carrier", fix.Path)
			})
		}
	}

	validPlainCarrier, err := ir.FormatCarrier(ir.Carrier{Scheme: ir.SchemePlain, Text: "hidden"})
	require.NoError(t, err)

	t.Run("literal trc1 carrier in user, system, tool_result, and bare gAAAA signature", func(t *testing.T) {
		msgRaw := []byte("{\n  \"model\": \"claude-sonnet-4-5\",\n  \"max_tokens\": 1024,\n  \"system\": \"Literal " + validPlainCarrier + " in system\",\n  \"messages\": [\n    {\"role\": \"user\", \"content\": \"Check " + validPlainCarrier + " and trc1:openai:demo\"},\n    {\"role\": \"assistant\", \"content\": [{\"type\": \"thinking\", \"thinking\": \"bare\", \"signature\": \"gAAAAABnLegacyBareToken\"}, {\"type\": \"tool_use\", \"id\": \"t1\", \"name\": \"fn\", \"input\": {\"x\": 9007199254740993}}]},\n    {\"role\": \"user\", \"content\": [{\"type\": \"tool_result\", \"tool_use_id\": \"t1\", \"content\": \"" + validPlainCarrier + "\"}]}\n  ]\n}")
		got, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect:  Dialect{ReplayScheme: ir.SchemeAnthropic},
		}, msgRaw)
		require.NoError(t, err)
		require.Empty(t, losses)
		require.True(t, bytes.Equal(msgRaw, got))

		rspRaw := []byte("{\n  \"model\": \"gpt-5\",\n  \"instructions\": \"Keep " + validPlainCarrier + "\",\n  \"input\": [\n    {\"type\": \"message\", \"role\": \"user\", \"content\": [{\"type\": \"input_text\", \"text\": \"" + validPlainCarrier + "\"}]},\n    {\"id\": \"rs_1\", \"type\": \"reasoning\", \"summary\": [], \"encrypted_content\": \"gAAAAABnBareEncToken\"}\n  ]\n}")
		gotRsp, lossesRsp, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 101},
		}, rspRaw)
		require.NoError(t, err)
		require.Empty(t, lossesRsp)
		require.True(t, bytes.Equal(rspRaw, gotRsp))
	})

	t.Run("invalid UTF-8 bytes combined with literal trc1 in user text stay byte-identical", func(t *testing.T) {
		rawInvalidUTF8 := append([]byte(`{"model":"claude-sonnet-4-5","max_tokens":64,"messages":[{"role":"user","content":"prefix trc1:plain:eyJ0IjoiaGkifQ and bad bytes `), 0xff, 0xfe)
		rawInvalidUTF8 = append(rawInvalidUTF8, []byte(` suffix"}]}`)...)

		got, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect:  Dialect{ReplayScheme: ir.SchemeAnthropic},
		}, rawInvalidUTF8)
		require.NoError(t, err)
		require.Empty(t, losses)
		require.True(t, bytes.Equal(rawInvalidUTF8, got))

		rspInvalidUTF8 := append([]byte(`{"model":"gpt-5","input":"literal trc1:plain:eyJ0IjoiaGkifQ plus `), 0xff, 0xfe)
		rspInvalidUTF8 = append(rspInvalidUTF8, []byte(`"}`)...)
		gotRsp, lossesRsp, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  Dialect{ReplayScheme: ir.SchemeOpenAI},
		}, rspInvalidUTF8)
		require.NoError(t, err)
		require.Empty(t, lossesRsp)
		require.True(t, bytes.Equal(rspInvalidUTF8, gotRsp))
	})

	t.Run("WS session.update frame containing trc1 stays byte-identical", func(t *testing.T) {
		wsSessionUpdate := []byte("{\n  \"type\": \"session.update\",\n  \"session\": {\n    \"instructions\": \"" + validPlainCarrier + "\"\n  },\n  \"input\": [\n    {\"type\": \"reasoning\", \"summary\": [], \"encrypted_content\": \"" + validPlainCarrier + "\"}\n  ]\n}")
		got, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolResponsesWebSocket,
			Upstream: protocol.ProtocolResponsesWebSocket,
			Dialect:  Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 101},
		}, wsSessionUpdate)
		require.NoError(t, err)
		require.Empty(t, losses)
		require.True(t, bytes.Equal(wsSessionUpdate, got))
	})
}

// TestPrepareNativeReplay_MessagesSubScenarios exercises same-scheme restoration,
// active tool-loop thinking downgrade, foreign removal with empty-turn
// placeholder insertion, and passback plaintext thinking retention on Messages.
// Rewritten payloads must pass strict Messages request validation without
// leaving residual trc1 envelopes on signature or redacted data paths.
func TestPrepareNativeReplay_MessagesSubScenarios(t *testing.T) {
	fix, err := testkit.ReadTestdataFile("wire/anthropic_messages/request/reasoning_carrier_native.json")
	require.NoError(t, err)

	t.Run("anthropic scheme restores turn 1 and downgrades active tool loop when turn 2 loses foreign thinking", func(t *testing.T) {
		out, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect:  Dialect{ReplayScheme: ir.SchemeAnthropic},
		}, fix)
		require.NoError(t, err)
		require.NotEmpty(t, losses)

		// Literal trc1:plain:example in user text must survive untouched.
		require.Contains(t, string(out), "trc1:plain:example")
		// No trc1: envelopes may remain on signature/data fields; top-level thinking
		// was removed and turn 1's restored thinking downgraded to a text block.
		require.NotContains(t, string(out), `"trc1:`)
		require.NotContains(t, string(out), `"thinking":{`)
		require.Contains(t, string(out), "Initial plan: inspect gateway timeout config before comparing SLA limits.")

		violations := testkit.ValidateMessagesRequest(out, testkit.RequestValidationOptions{StrictTopLevel: true})
		require.Empty(t, violations, "strict Messages violations: %v", violations)
	})

	t.Run("foreign scheme deletes all carriers and inserts placeholder on emptied assistant turn", func(t *testing.T) {
		out, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect:  Dialect{ReplayScheme: ir.SchemeGemini},
		}, fix)
		require.NoError(t, err)
		require.NotEmpty(t, losses)
		require.Contains(t, string(out), removedAssistantPlaceholderText)
		require.Contains(t, string(out), "trc1:plain:example")
		require.NotContains(t, string(out), `"trc1:`)

		violations := testkit.ValidateMessagesRequest(out, testkit.RequestValidationOptions{StrictTopLevel: true})
		require.Empty(t, violations, "strict Messages violations: %v", violations)
	})

	t.Run("same-scheme active tool loop keeps signed thinking when latest turn matches", func(t *testing.T) {
		sigCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "EqQNativeMatchSig",
			Text:   "Exact signed Anthropic thought.",
		})
		require.NoError(t, err)

		reqJSON := []byte(`{
			"model":"claude-sonnet-4-5",
			"max_tokens":2048,
			"thinking":{"type":"enabled","budget_tokens":1024},
			"tools":[{"name":"read_cfg","input_schema":{"type":"object","properties":{"id":{"type":"integer","default":9007199254740993}}}}],
			"messages":[
				{"role":"user","content":"check config"},
				{"role":"assistant","content":[
					{"type":"thinking","thinking":"abbreviated","signature":"` + sigCarrier + `"},
					{"type":"tool_use","id":"call_1","name":"read_cfg","input":{"id":9007199254740993}}
				]},
				{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_1","content":"ok"}]}
			]
		}`)

		out, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect:  Dialect{ReplayScheme: ir.SchemeAnthropic},
		}, reqJSON)
		require.NoError(t, err)
		require.Empty(t, losses)
		require.Contains(t, string(out), `"signature":"EqQNativeMatchSig"`)
		require.Contains(t, string(out), `"thinking":"Exact signed Anthropic thought."`)
		// Large integer > 2^53 stays exact without float64 rounding.
		require.Contains(t, string(out), "9007199254740993")
		require.Empty(t, testkit.ValidateMessagesRequest(out, testkit.RequestValidationOptions{StrictTopLevel: true}))
	})

	t.Run("passback retains plaintext thinking from text-carrying envelopes and omits signature", func(t *testing.T) {
		out, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect: Dialect{
				Name:             "deepseek",
				ReasoningHistory: HistoryToolTurns,
			},
		}, fix)
		require.NoError(t, err)
		require.NotEmpty(t, losses)
		require.Contains(t, string(out), "Initial plan: inspect gateway timeout config before comparing SLA limits.")
		require.NotContains(t, string(out), `"trc1:`)

		violations := testkit.ValidateMessagesRequest(out, testkit.RequestValidationOptions{
			StrictTopLevel: true,
			DialectRules:   []string{"passback"},
		})
		require.Empty(t, violations, "passback violations: %v", violations)
	})
}

// TestPrepareNativeReplay_ResponsesAndWSSubScenarios exercises same-account
// restoration, cross-account deletion, summary retention with store:false ID
// cleanup, empty reasoning deletion, and WS response.create nested frames.
// Rewritten payloads must pass strict Responses validation and keep untouched
// fields and large integers exact.
func TestPrepareNativeReplay_ResponsesAndWSSubScenarios(t *testing.T) {
	fix, err := testkit.ReadTestdataFile("wire/openai_responses/request/reasoning_carrier_native.json")
	require.NoError(t, err)

	t.Run("same scheme and matching ProviderID 101 restores encrypted_content and deletes foreign xai empty item", func(t *testing.T) {
		out, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 101},
		}, fix)
		require.NoError(t, err)
		require.Equal(t, []ir.Loss{{Kind: ir.LossReplayTokenDropped, Path: "input[4]"}}, losses)
		require.Contains(t, string(out), "trc1:openai:demo")
		require.Contains(t, string(out), `"encrypted_content":"gAAAAABnNativeOpenAISameAcct01"`)
		require.Contains(t, string(out), `"id":"rs_nat_same_1"`)
		require.NotContains(t, string(out), "rs_nat_empty_2")
		require.NotContains(t, string(out), "enc_xai_native_foreign_02")

		violations := testkit.ValidateResponsesRequest(out, testkit.RequestValidationOptions{})
		require.Empty(t, violations, "Responses violations: %v", violations)
	})

	t.Run("cross-account ProviderID 999 strips encrypted_content and synthetic id under store:false while keeping summary", func(t *testing.T) {
		out, losses, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 999},
		}, fix)
		require.NoError(t, err)
		require.Len(t, losses, 2)
		require.NotContains(t, string(out), `"encrypted_content"`)
		require.NotContains(t, string(out), `"rs_nat_same_1"`)
		require.Contains(t, string(out), "Read gateway.yaml to check read and write timeouts.")
		require.NotContains(t, string(out), "rs_nat_empty_2")

		violations := testkit.ValidateResponsesRequest(out, testkit.RequestValidationOptions{})
		require.Empty(t, violations, "Responses violations: %v", violations)
	})

	t.Run("WS response.create nested response.input rewrites carriers cleanly", func(t *testing.T) {
		openaiCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeOpenAI,
			ProviderID: 42,
			Data:       "gAAAAABnWSNested42",
		})
		require.NoError(t, err)
		plainCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemePlain,
			Text:   "plain thought without summary",
		})
		require.NoError(t, err)

		wsCreate, err := json.Marshal(map[string]jsontext.Value{
			"type": jsontext.Value(`"response.create"`),
			"response": jsontext.Value(`{
				"model":"gpt-5.3-codex",
				"store":false,
				"input":[
					{"id":"rs_ws_1","type":"reasoning","summary":[{"type":"summary_text","text":"kept summary"}],"encrypted_content":"` + openaiCarrier + `"},
					{"id":"rs_ws_2","type":"reasoning","summary":[],"encrypted_content":"` + plainCarrier + `"},
					{"id":"msg_ws_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]},
					{"type":"message","role":"user","content":[{"type":"input_text","text":"next turn"}]}
				]
			}`),
		}, json.Deterministic(true))
		require.NoError(t, err)

		// Matching account 42 restores rs_ws_1 and drops tokenless empty-summary rs_ws_2.
		outSame, lossesSame, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolResponsesWebSocket,
			Upstream: protocol.ProtocolResponsesWebSocket,
			Dialect:  Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 42},
		}, wsCreate)
		require.NoError(t, err)
		require.Equal(t, []ir.Loss{{Kind: ir.LossReasoningTextDropped, Path: "input[1]"}}, lossesSame)
		require.Contains(t, string(outSame), `"encrypted_content":"gAAAAABnWSNested42"`)
		require.NotContains(t, string(outSame), "rs_ws_2")

		// Mismatched account 77 strips encrypted_content and id from rs_ws_1 while keeping summary.
		outOther, lossesOther, err := PrepareNativeReplay(Route{
			Client:   protocol.ProtocolResponsesWebSocket,
			Upstream: protocol.ProtocolResponsesWebSocket,
			Dialect:  Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 77},
		}, wsCreate)
		require.NoError(t, err)
		require.Len(t, lossesOther, 2)
		require.False(t, strings.Contains(string(outOther), "encrypted_content"))
		require.False(t, strings.Contains(string(outOther), "rs_ws_1"))
		require.Contains(t, string(outOther), "kept summary")
	})
}

// TestPrepareNativeReplayWithStatus_DistinguishesMatchedCarriersFromLiterals
// verifies that PrepareNativeReplayWithStatus reports applied=true for both
// lossless matching carrier unpacking and lossy foreign stripping, while
// reporting applied=false when "trc1:" appears only inside user prompt text.
func TestPrepareNativeReplayWithStatus_DistinguishesMatchedCarriersFromLiterals(t *testing.T) {
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "sig_native_ok",
		Text:   "step by step",
	})
	require.NoError(t, err)

	// 1. Literal "trc1:" inside user message text does not trigger native_replay.
	literalOnly := []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"messages":[{"role":"user","content":"Literal string ` + carrier + ` in prompt"}]}`)
	outLit, lossesLit, appliedLit, err := PrepareNativeReplayWithStatus(Route{
		Client:   protocol.ProtocolAnthropicMessages,
		Upstream: protocol.ProtocolAnthropicMessages,
		Dialect:  Dialect{ReplayScheme: ir.SchemeAnthropic},
	}, literalOnly)
	require.NoError(t, err)
	require.False(t, appliedLit)
	require.Empty(t, lossesLit)
	require.Equal(t, literalOnly, outLit)

	// 2. Matching carrier on thinking.signature unpacks losslessly and reports applied=true.
	withMatchingCarrier := []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":[{"type":"thinking","thinking":"step by step","signature":"` + carrier + `"},{"type":"text","text":"answer"}]},{"role":"user","content":"next"}]}`)
	outMatch, lossesMatch, appliedMatch, err := PrepareNativeReplayWithStatus(Route{
		Client:   protocol.ProtocolAnthropicMessages,
		Upstream: protocol.ProtocolAnthropicMessages,
		Dialect:  Dialect{ReplayScheme: ir.SchemeAnthropic},
	}, withMatchingCarrier)
	require.NoError(t, err)
	require.True(t, appliedMatch)
	require.Empty(t, lossesMatch)
	require.Contains(t, string(outMatch), `"signature":"sig_native_ok"`)

	// 3. Foreign scheme carrier strips thinking block, records loss, and reports applied=true.
	outForeign, lossesForeign, appliedForeign, err := PrepareNativeReplayWithStatus(Route{
		Client:   protocol.ProtocolAnthropicMessages,
		Upstream: protocol.ProtocolAnthropicMessages,
		Dialect:  Dialect{ReplayScheme: ir.SchemeGemini},
	}, withMatchingCarrier)
	require.NoError(t, err)
	require.True(t, appliedForeign)
	require.NotEmpty(t, lossesForeign)
	require.NotContains(t, string(outForeign), "sig_native_ok")
}
