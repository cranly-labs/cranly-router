package testkit

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// wire_corpus_scenario_test.go verifies embedded wire fixtures across corpus.go,
// corpus_import.go, request_rules.go, request_rules_messages.go,
// request_rules_responses.go, request_rules_chat.go, request_rules_gemini.go,
// grammar.go, grammar_messages.go, grammar_responses.go, grammar_chat.go,
// grammar_gemini.go, clientsim.go, clientsim_messages.go,
// clientsim_responses.go, clientsim_chat.go, clientsim_gemini.go, and jsondiff.go.
// Every hand-written fixture under testdata/wire/ is discovered automatically
// from DefaultCorpus so scenario batches inherit loading, secret scanning,
// strict request validation, SSE grammar checks, and buffered response envelope
// verification without manual test registration.

var (
	// allWireProtocols lists the four text generation wire protocols under
	// testdata/wire/ iterated by scenario batch coverage and replay checks.
	allWireProtocols = []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
		protocol.ProtocolGeminiGenerateContent,
	}

	// batch05aFullScenarios lists Batch 0.5a scenarios that provide request,
	// stream, and buffered response fixtures for all four wire protocols.
	batch05aFullScenarios = []string{
		"text_basic",
		"text_unicode_split",
		"system_roles",
		"tool_single",
		"tool_parallel",
		"tool_args_fragmented",
		"tool_args_empty",
	}

	// batch05bFullScenarios lists Batch 0.5b tool-result scenarios that provide
	// request, stream, and buffered response fixtures for all four wire protocols.
	batch05bFullScenarios = []string{
		"tool_result_image",
		"tool_result_error",
	}

	// batch05bResponsesClientToolScenarios lists Responses client-tool scenarios
	// whose requests exist only in OpenAI Responses while all four protocols
	// supply native or lowered upstream stream and buffered response fixtures.
	batch05bResponsesClientToolScenarios = []string{
		"tools_custom",
		"tools_namespace",
		"tools_search",
	}

	// batch05cFullScenarios lists Batch 0.5c reasoning scenarios that populate
	// request, stream, and buffered response fixtures across all four wire protocols.
	batch05cFullScenarios = []string{
		"reasoning_summary",
		"reasoning_tool_replay",
	}

	// batch05cMessagesResponsesClientScenarios lists Batch 0.5c continuity scenarios
	// whose client requests exist on Anthropic Messages and OpenAI Responses while
	// all four wire protocols supply upstream stream and buffered response fixtures.
	batch05cMessagesResponsesClientScenarios = []string{
		"reasoning_carrier_roundtrip",
		"legacy_history",
	}

	// batch05dFullScenarios lists Batch 0.5d multimodal, structured-output,
	// truncation finish-reason, and refusal scenarios that populate request,
	// stream, and buffered response fixtures across all four wire protocols.
	batch05dFullScenarios = []string{
		"image_input",
		"document_input",
		"structured_output",
		"stop_sequence",
		"max_tokens",
		"refusal",
	}

	// batch05eFullScenarios lists Batch 0.5e scenarios that populate request,
	// stream, and buffered response fixtures across all four wire protocols.
	batch05eFullScenarios = []string{
		"usage_cache",
		"upstream_sse_quirks",
		"empty_response",
	}

	// batch05eStreamOnlyScenarios lists Batch 0.5e mid-stream fault scenarios
	// that populate request and SSE stream fixtures across all four wire protocols
	// while omitting buffered JSON response slots.
	batch05eStreamOnlyScenarios = []string{
		"stream_truncated",
		"stream_error_event",
	}

	// allSection55Scenarios enumerates all 33 canonical wire scenario slugs
	// defined in section 5.5 of protocol-conversion-ir-refactor.md.
	allSection55Scenarios = []string{
		"text_basic",
		"text_unicode_split",
		"system_roles",
		"tool_single",
		"tool_parallel",
		"tool_parallel_interleaved",
		"tool_args_fragmented",
		"tool_args_empty",
		"tool_result_image",
		"tool_result_error",
		"tool_ids_missing",
		"reasoning_summary",
		"reasoning_tool_replay",
		"reasoning_token_foreign",
		"reasoning_carrier_roundtrip",
		"reasoning_carrier_native",
		"reasoning_encrypted_only",
		"legacy_history",
		"tools_custom",
		"tools_namespace",
		"tools_search",
		"tools_hosted_search",
		"image_input",
		"document_input",
		"structured_output",
		"stop_sequence",
		"max_tokens",
		"refusal",
		"usage_cache",
		"stream_truncated",
		"stream_error_event",
		"upstream_sse_quirks",
		"empty_response",
	}

	// validCarrierSchemes enumerates the six continuity schemes permitted inside
	// trc1:<scheme>:<payload> envelopes under section 3.10.
	validCarrierSchemes = []string{
		ir.SchemeAnthropic,
		ir.SchemeAntigravityClaude,
		ir.SchemeOpenAI,
		ir.SchemeXAI,
		ir.SchemeGemini,
		ir.SchemePlain,
	}

	// expectedClientIngressRequestRules maps pre-conversion client-ingress request
	// wire fixtures carrying legacy empty signatures to the exact strict upstream
	// rule IDs they must trigger before conversion or passback normalization.
	expectedClientIngressRequestRules = map[string][]string{
		"wire/anthropic_messages/request/legacy_history.json": {
			"MSG-THINKING",
		},
	}

	// expectedUpstreamQuirkStreamRules maps intentional upstream-quirk, truncated,
	// and mid-stream error SSE wire fixtures to the exact normal-mode client
	// grammar rule codes they must trigger so strict Law S3 validators remain
	// unweakened while unexpected framing or envelope bugs still fail tests.
	expectedUpstreamQuirkStreamRules = map[string][]string{
		"wire/openai_chat_completions/stream/tool_ids_missing.sse": {
			"empty_tool_name_delta",
			"missing_tool_call_id",
		},
		"wire/anthropic_messages/stream/stream_truncated.sse": {
			"unclosed_content_block",
			"missing_terminal_event",
		},
		"wire/openai_responses/stream/stream_truncated.sse": {
			"missing_terminal_event",
		},
		"wire/openai_chat_completions/stream/stream_truncated.sse": {
			"invalid_tool_arguments_json",
			"missing_finish_reason",
			"missing_done_sentinel",
		},
		"wire/gemini_generate_content/stream/stream_truncated.sse": {
			"missing_finish_reason",
		},
		"wire/anthropic_messages/stream/stream_error_event.sse": {
			"unexpected_error_event",
		},
		"wire/openai_responses/stream/stream_error_event.sse": {
			"unexpected_response_failed",
		},
		"wire/openai_chat_completions/stream/stream_error_event.sse": {
			"unexpected_error_frame",
			"missing_finish_reason",
			"missing_done_sentinel",
		},
		"wire/gemini_generate_content/stream/stream_error_event.sse": {
			"unexpected_error_frame",
			"missing_finish_reason",
		},
		"wire/openai_responses/stream/upstream_sse_quirks.sse": {
			"missing_event_type",
			"invalid_output_index",
			"event_before_created",
			"missing_terminal_event",
		},
	}

	// expectedAbnormalTruncatedStreamRules maps raw truncated upstream SSE wire
	// fixtures to the exact grammar rule codes that must still fire under
	// AllowAbnormal mode because silent EOF lacks a terminal error frame.
	expectedAbnormalTruncatedStreamRules = map[string][]string{
		"wire/anthropic_messages/stream/stream_truncated.sse": {
			"unclosed_content_block",
			"missing_terminal_event",
		},
		"wire/openai_responses/stream/stream_truncated.sse": {
			"missing_terminal_event",
		},
		"wire/openai_chat_completions/stream/stream_truncated.sse": {
			"missing_terminal_or_error",
		},
		"wire/gemini_generate_content/stream/stream_truncated.sse": {
			"missing_terminal_or_error",
		},
	}
)

// wireReplayPair pairs a wire protocol with a scenario slug for multi-turn SimulateNextTurn checks.
type wireReplayPair struct {
	proto    protocol.ProtocolID // wire protocol under client simulation
	scenario string              // wire scenario slug
}

// decodedTestCarrier holds parsed trc1 envelope fields verified across Batch 0.5c fixtures.
type decodedTestCarrier struct {
	scheme     string // continuity scheme segment
	tokenData  string // raw issuer token payload "d"
	text       string // signed or plain reasoning text "t"
	redacted   bool   // redacted thinking flag "r"
	providerID int64  // account-bound provider ID "p"
}

// TestWireFixturesLoadAndValidate walks every hand-written fixture under
// testdata/wire/ via DefaultCorpus and validates scenario lookup, strict
// request invariants, SSE stream grammar, and buffered response envelopes.
// Newly added wire fixtures in later batches are discovered and checked
// automatically without updating a static scenario allowlist.
func TestWireFixturesLoadAndValidate(t *testing.T) {
	corpus := DefaultCorpus()
	fixtures, err := corpus.QueryFixtures(FixtureQuery{Source: SourceWire})
	require.NoError(t, err)
	require.NotEmpty(t, fixtures, "testdata/wire/ must contain embedded wire fixtures")

	for _, fix := range fixtures {
		t.Run(fix.Path, func(t *testing.T) {
			require.NotEmpty(t, fix.Name)
			require.NotEmpty(t, fix.Scenario)
			require.NotEmpty(t, fix.Protocol)
			require.NotEmpty(t, fix.Data)
			require.LessOrEqual(t, len(fix.Data), MaxCorpusFileBytes)
			require.Empty(t, DetectSecretViolations(fix.Data), "secret pattern in %s", fix.Path)

			matched, queryErr := corpus.QueryFixtures(FixtureQuery{
				Source:   SourceWire,
				Protocol: fix.Protocol,
				Kind:     fix.Kind,
				Scenario: fix.Scenario,
			})
			require.NoError(t, queryErr)
			require.Len(t, matched, 1, "scenario query must resolve a unique wire fixture")
			require.Equal(t, fix.Path, matched[0].Path)

			switch fix.Kind {
			case FixtureRequest:
				violations := ValidateRequestRules(fix.Protocol, fix.Data, RequestValidationOptions{
					StrictTopLevel: true,
				})
				assertWireRequestViolations(t, fix.Path, violations)

			case FixtureStream:
				frames, frameErr := fix.Frames()
				require.NoError(t, frameErr)
				require.NotEmpty(t, frames, "stream fixture must emit at least one SSE frame")
				violations := ValidateStreamGrammar(fix.Protocol, frames, GrammarOptions{})
				assertWireStreamViolations(t, fix.Path, violations)

			case FixtureResponse:
				require.NoError(t, validateBufferedWireResponse(fix.Protocol, fix.Data))

			default:
				t.Fatalf("unexpected wire fixture kind %q", fix.Kind)
			}
		})
	}
}

// TestWireBatch05aScenarioCoverageAndReplay checks that all eight Batch 0.5a
// scenarios populate their expected protocol and framing slots, pass Gemini
// signature and schema dialect rules, and fold cleanly through SimulateNextTurn.
// Chat-only upstream applicability on tool_parallel_interleaved is enforced so
// non-Chat protocols do not carry invalid interleaved stream fixtures.
func TestWireBatch05aScenarioCoverageAndReplay(t *testing.T) {
	corpus := DefaultCorpus()

	for _, scenario := range batch05aFullScenarios {
		for _, proto := range allWireProtocols {
			for _, kind := range []FixtureKind{FixtureRequest, FixtureStream, FixtureResponse} {
				matches, err := corpus.QueryFixtures(FixtureQuery{
					Source:   SourceWire,
					Protocol: proto,
					Kind:     kind,
					Scenario: scenario,
				})
				require.NoError(t, err)
				require.Len(t, matches, 1, "missing %s/%s/%s", proto, kind, scenario)
			}
		}
	}

	// tool_parallel_interleaved supplies client requests across all protocols while
	// restricting upstream stream and buffered response fixtures to Chat Completions.
	for _, proto := range allWireProtocols {
		reqs, err := corpus.QueryFixtures(FixtureQuery{
			Source:   SourceWire,
			Protocol: proto,
			Kind:     FixtureRequest,
			Scenario: "tool_parallel_interleaved",
		})
		require.NoError(t, err)
		require.Len(t, reqs, 1, "missing request fixture for %s/tool_parallel_interleaved", proto)

		streams, err := corpus.QueryFixtures(FixtureQuery{
			Source:   SourceWire,
			Protocol: proto,
			Kind:     FixtureStream,
			Scenario: "tool_parallel_interleaved",
		})
		require.NoError(t, err)
		resps, err := corpus.QueryFixtures(FixtureQuery{
			Source:   SourceWire,
			Protocol: proto,
			Kind:     FixtureResponse,
			Scenario: "tool_parallel_interleaved",
		})
		require.NoError(t, err)

		if proto == protocol.ProtocolOpenAIChatCompletions {
			require.Len(t, streams, 1)
			require.Len(t, resps, 1)
		} else {
			require.Empty(t, streams, "non-Chat protocol %s must not define interleaved stream", proto)
			require.Empty(t, resps, "non-Chat protocol %s must not define interleaved response", proto)
		}
	}

	// Verify Batch 0.5a Gemini request fixtures also satisfy strict step signature and schema dialects.
	batch05aAll := append(slices.Clone(batch05aFullScenarios), "tool_parallel_interleaved")
	for _, scenario := range batch05aAll {
		req := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, scenario)
		v := ValidateGeminiRequest(req.Data, RequestValidationOptions{
			DialectRules: []string{"gemini-signature", "antigravity-schema"},
		})
		require.Empty(t, v, "gemini dialect violations in %s: %v", req.Path, v)
	}

	// Verify multi-turn client simulation succeeds from both streamed and buffered
	// replies on standard single-turn and multi-turn Batch 0.5a scenarios.
	replayScenarios := []string{
		"text_basic",
		"text_unicode_split",
		"system_roles",
		"tool_single",
		"tool_parallel",
		"tool_args_fragmented",
	}
	for _, scenario := range replayScenarios {
		for _, proto := range allWireProtocols {
			t.Run(fmt.Sprintf("replay/%s/%s", proto, scenario), func(t *testing.T) {
				reqFix := mustSingleWireFixture(t, corpus, proto, FixtureRequest, scenario)
				streamFix := mustSingleWireFixture(t, corpus, proto, FixtureStream, scenario)
				respFix := mustSingleWireFixture(t, corpus, proto, FixtureResponse, scenario)

				frames, frameErr := streamFix.Frames()
				require.NoError(t, frameErr)

				nextFromStream, simErr := SimulateNextTurn(proto, TurnInput{
					PrevRequest:              reqFix.Data,
					ResponseFrames:           frames,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Continue.",
				})
				require.NoError(t, simErr)
				require.Empty(t, ValidateRequestRules(proto, nextFromStream, RequestValidationOptions{
					StrictTopLevel: true,
				}))

				nextFromResp, simErr := SimulateNextTurn(proto, TurnInput{
					PrevRequest:              reqFix.Data,
					ResponseBody:             respFix.Data,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Continue.",
				})
				require.NoError(t, simErr)
				require.Empty(t, ValidateRequestRules(proto, nextFromResp, RequestValidationOptions{
					StrictTopLevel: true,
				}))
			})
		}
	}
}

// TestWireBatch05bScenarioCoverageAndReplay verifies protocol slot presence,
// dialect rule activation, synthetic PNG media invariants, 64-byte flattened
// namespace names, and SimulateNextTurn replay across all seven Batch 0.5b
// tool-result and special-tool scenarios. Protocol-specific client capabilities
// (custom, namespace, tool_search, and hosted web search) and upstream ID quirks
// enforce both populated slots and omitted inapplicable slots.
func TestWireBatch05bScenarioCoverageAndReplay(t *testing.T) {
	corpus := DefaultCorpus()

	// tool_result_image and tool_result_error populate request, stream, and
	// buffered response slots across all four wire protocols.
	for _, scenario := range batch05bFullScenarios {
		for _, proto := range allWireProtocols {
			for _, kind := range []FixtureKind{FixtureRequest, FixtureStream, FixtureResponse} {
				_ = mustSingleWireFixture(t, corpus, proto, kind, scenario)
			}
		}
	}

	// Verify tool_result_image request fixtures use SyntheticPNGDataBase64 across all protocols.
	for _, proto := range allWireProtocols {
		req := mustSingleWireFixture(t, corpus, proto, FixtureRequest, "tool_result_image")
		require.True(t, bytes.Contains(req.Data, []byte(SyntheticPNGDataBase64)), "%s must embed SyntheticPNGDataBase64", req.Path)
	}

	// tool_ids_missing supplies client requests for all four protocols while
	// restricting upstream stream and response fixtures to Chat and Gemini.
	for _, proto := range allWireProtocols {
		_ = mustSingleWireFixture(t, corpus, proto, FixtureRequest, "tool_ids_missing")
		if proto == protocol.ProtocolOpenAIChatCompletions || proto == protocol.ProtocolGeminiGenerateContent {
			_ = mustSingleWireFixture(t, corpus, proto, FixtureStream, "tool_ids_missing")
			_ = mustSingleWireFixture(t, corpus, proto, FixtureResponse, "tool_ids_missing")
		} else {
			assertNoWireFixture(t, corpus, proto, FixtureStream, "tool_ids_missing")
			assertNoWireFixture(t, corpus, proto, FixtureResponse, "tool_ids_missing")
		}
	}

	// tools_custom, tools_namespace, and tools_search define client requests only
	// on OpenAI Responses while supplying native or lowered upstream streams and
	// buffered responses across all four wire protocols.
	for _, scenario := range batch05bResponsesClientToolScenarios {
		for _, proto := range allWireProtocols {
			if proto == protocol.ProtocolOpenAIResponses {
				_ = mustSingleWireFixture(t, corpus, proto, FixtureRequest, scenario)
			} else {
				assertNoWireFixture(t, corpus, proto, FixtureRequest, scenario)
			}
			_ = mustSingleWireFixture(t, corpus, proto, FixtureStream, scenario)
			_ = mustSingleWireFixture(t, corpus, proto, FixtureResponse, scenario)
		}

		// Confirm the Responses request exercises RSP-DIALECT-FUNCTION-ONLY when unlowered.
		rspReq := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, scenario)
		dialectViolations := ValidateResponsesRequest(rspReq.Data, RequestValidationOptions{
			DialectRules: []string{"function-only"},
		})
		require.NotEmpty(t, dialectViolations, "%s must trigger function-only dialect rules before lowering", rspReq.Path)
		for _, v := range dialectViolations {
			require.Equal(t, "RSP-DIALECT-FUNCTION-ONLY", v.RuleID, "unexpected rule in %s: %v", rspReq.Path, v)
		}
	}

	// Verify tools_namespace lowered upstream fixtures carry the 64-byte hash-shortened name.
	const wantFlattened64 = "enterprise_observability_telemetry_connector__query_di__5375327d"
	require.Len(t, wantFlattened64, 64)
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIChatCompletions,
		protocol.ProtocolGeminiGenerateContent,
	} {
		for _, kind := range []FixtureKind{FixtureStream, FixtureResponse} {
			fix := mustSingleWireFixture(t, corpus, proto, kind, "tools_namespace")
			require.True(t, bytes.Contains(fix.Data, []byte(wantFlattened64)), "%s must contain 64-byte flattened tool name", fix.Path)
		}
	}

	// tools_hosted_search applies to Messages, Responses, and Gemini while
	// omitting Chat Completions, which has no hosted web search tool declaration or result block.
	for _, proto := range allWireProtocols {
		for _, kind := range []FixtureKind{FixtureRequest, FixtureStream, FixtureResponse} {
			if proto == protocol.ProtocolOpenAIChatCompletions {
				assertNoWireFixture(t, corpus, proto, kind, "tools_hosted_search")
			} else {
				_ = mustSingleWireFixture(t, corpus, proto, kind, "tools_hosted_search")
			}
		}
	}

	// Verify all Batch 0.5b Gemini request fixtures satisfy strict step signature and schema dialects.
	for _, scenario := range []string{"tool_result_image", "tool_result_error", "tool_ids_missing", "tools_hosted_search"} {
		req := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, scenario)
		v := ValidateGeminiRequest(req.Data, RequestValidationOptions{
			DialectRules: []string{"gemini-signature", "antigravity-schema"},
		})
		require.Empty(t, v, "gemini dialect violations in %s: %v", req.Path, v)
	}

	var replayPairs []wireReplayPair
	for _, scenario := range batch05bFullScenarios {
		for _, proto := range allWireProtocols {
			replayPairs = append(replayPairs, wireReplayPair{proto: proto, scenario: scenario})
		}
	}
	replayPairs = append(replayPairs, wireReplayPair{
		proto:    protocol.ProtocolGeminiGenerateContent,
		scenario: "tool_ids_missing",
	})
	for _, scenario := range batch05bResponsesClientToolScenarios {
		replayPairs = append(replayPairs, wireReplayPair{
			proto:    protocol.ProtocolOpenAIResponses,
			scenario: scenario,
		})
	}
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolGeminiGenerateContent,
	} {
		replayPairs = append(replayPairs, wireReplayPair{
			proto:    proto,
			scenario: "tools_hosted_search",
		})
	}

	for _, pair := range replayPairs {
		t.Run(fmt.Sprintf("replay/%s/%s", pair.proto, pair.scenario), func(t *testing.T) {
			assertWireNextTurnReplay(t, corpus, pair.proto, pair.scenario)
		})
	}
}

// TestWireBatch05cScenarioCoverageAndReplay verifies protocol slot presence,
// trc1 carrier envelope formatting across all six schemes, DeepSeek and
// passback dialect rules, stream-versus-buffered folding equivalence, and
// SimulateNextTurn multi-turn replay across all seven Batch 0.5c reasoning and
// continuity scenarios. Pre-conversion client ingress fixtures carrying legacy
// empty signatures or carrier envelopes are checked without weakening strict
// upstream request rules.
func TestWireBatch05cScenarioCoverageAndReplay(t *testing.T) {
	corpus := DefaultCorpus()

	// reasoning_summary and reasoning_tool_replay populate request, stream, and
	// buffered response slots across all four wire protocols.
	for _, scenario := range batch05cFullScenarios {
		for _, proto := range allWireProtocols {
			for _, kind := range []FixtureKind{FixtureRequest, FixtureStream, FixtureResponse} {
				_ = mustSingleWireFixture(t, corpus, proto, kind, scenario)
			}
		}
	}

	// reasoning_token_foreign populates client requests on protocols carrying wire
	// replay tokens (Messages, Responses, Gemini), omits Chat requests, and
	// provides upstream stream and buffered response fixtures on all four protocols.
	for _, proto := range allWireProtocols {
		if proto == protocol.ProtocolOpenAIChatCompletions {
			assertNoWireFixture(t, corpus, proto, FixtureRequest, "reasoning_token_foreign")
		} else {
			_ = mustSingleWireFixture(t, corpus, proto, FixtureRequest, "reasoning_token_foreign")
		}
		_ = mustSingleWireFixture(t, corpus, proto, FixtureStream, "reasoning_token_foreign")
		_ = mustSingleWireFixture(t, corpus, proto, FixtureResponse, "reasoning_token_foreign")
	}

	// reasoning_carrier_roundtrip and legacy_history populate client requests on
	// Anthropic Messages and OpenAI Responses while supplying upstream stream and
	// buffered response fixtures on all four wire protocols.
	for _, scenario := range batch05cMessagesResponsesClientScenarios {
		for _, proto := range allWireProtocols {
			if proto == protocol.ProtocolAnthropicMessages || proto == protocol.ProtocolOpenAIResponses {
				_ = mustSingleWireFixture(t, corpus, proto, FixtureRequest, scenario)
			} else {
				assertNoWireFixture(t, corpus, proto, FixtureRequest, scenario)
			}
			_ = mustSingleWireFixture(t, corpus, proto, FixtureStream, scenario)
			_ = mustSingleWireFixture(t, corpus, proto, FixtureResponse, scenario)
		}
	}

	// reasoning_carrier_native scopes request, stream, and response fixtures to
	// native Anthropic Messages and OpenAI Responses paths that run PrepareNativeReplay.
	for _, proto := range allWireProtocols {
		for _, kind := range []FixtureKind{FixtureRequest, FixtureStream, FixtureResponse} {
			if proto == protocol.ProtocolAnthropicMessages || proto == protocol.ProtocolOpenAIResponses {
				_ = mustSingleWireFixture(t, corpus, proto, kind, "reasoning_carrier_native")
			} else {
				assertNoWireFixture(t, corpus, proto, kind, "reasoning_carrier_native")
			}
		}
	}

	// reasoning_encrypted_only defines client requests exclusively on OpenAI
	// Responses while providing upstream stream and buffered response fixtures on
	// all four wire protocols.
	for _, proto := range allWireProtocols {
		if proto == protocol.ProtocolOpenAIResponses {
			_ = mustSingleWireFixture(t, corpus, proto, FixtureRequest, "reasoning_encrypted_only")
		} else {
			assertNoWireFixture(t, corpus, proto, FixtureRequest, "reasoning_encrypted_only")
		}
		_ = mustSingleWireFixture(t, corpus, proto, FixtureStream, "reasoning_encrypted_only")
		_ = mustSingleWireFixture(t, corpus, proto, FixtureResponse, "reasoning_encrypted_only")
	}

	assertBatch05cCarrierEnvelopes(t, corpus)

	// Verify Chat reasoning_tool_replay satisfies deepseek-thinking when preserving
	// reasoning_content and fails deepseek-thinking when client simulation drops it.
	chatReplayReq := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureRequest, "reasoning_tool_replay")
	require.Empty(t, ValidateChatRequest(chatReplayReq.Data, RequestValidationOptions{
		DialectRules: []string{"deepseek-thinking"},
	}))
	chatReplayResp := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureResponse, "reasoning_tool_replay")
	chatDroppedNext, err := SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, TurnInput{
		PrevRequest:              chatReplayReq.Data,
		ResponseBody:             chatReplayResp.Data,
		SynthesizeMissingOutputs: true,
		Variant:                  SimVariantChatDropUnknown,
	})
	require.NoError(t, err)
	deepseekViolations := ValidateChatRequest(chatDroppedNext, RequestValidationOptions{
		DialectRules: []string{"deepseek-thinking"},
	})
	require.NotEmpty(t, deepseekViolations, "dropping reasoning_content must trigger CHAT-DEEPSEEK-THINKING")
	require.Equal(t, "CHAT-DEEPSEEK-THINKING", deepseekViolations[0].RuleID)

	// Verify Gemini Batch 0.5c request fixtures satisfy step signature and schema dialects.
	for _, scenario := range []string{"reasoning_summary", "reasoning_tool_replay", "reasoning_token_foreign"} {
		req := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, scenario)
		v := ValidateGeminiRequest(req.Data, RequestValidationOptions{
			DialectRules: []string{"gemini-signature", "antigravity-schema"},
		})
		require.Empty(t, v, "gemini dialect violations in %s: %v", req.Path, v)
	}

	// Verify legacy_history carries bare non-carrier tokens and that Messages
	// legacy_history passes passback validation while failing strict MSG-THINKING.
	msgLegacyReq := mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureRequest, "legacy_history")
	msgLegacyTokens := collectWireTokenStrings(t, protocol.ProtocolAnthropicMessages, msgLegacyReq.Data)
	require.Equal(t, []string{
		"gAAAAABnLegacyBareEncryptedContentFromPreUpgradeTurn1",
		"",
	}, msgLegacyTokens)
	require.Empty(t, ValidateMessagesRequest(msgLegacyReq.Data, RequestValidationOptions{
		StrictTopLevel: true,
		DialectRules:   []string{"passback"},
	}))

	rspLegacyReq := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, "legacy_history")
	rspLegacyTokens := collectWireTokenStrings(t, protocol.ProtocolOpenAIResponses, rspLegacyReq.Data)
	require.Equal(t, []string{
		"gAAAAABnLegacyBareResponsesEncFromPreUpgrade01",
		"enc_legacy_redis_reasoning_history_opaque_02",
	}, rspLegacyTokens)

	// Verify multi-turn client simulation and stream/response equivalence across
	// all Batch 0.5c protocol-scenario pairs that populate request, stream, and response.
	var replayPairs []wireReplayPair
	for _, scenario := range batch05cFullScenarios {
		for _, proto := range allWireProtocols {
			replayPairs = append(replayPairs, wireReplayPair{proto: proto, scenario: scenario})
		}
	}
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolGeminiGenerateContent,
	} {
		replayPairs = append(replayPairs, wireReplayPair{proto: proto, scenario: "reasoning_token_foreign"})
	}
	for _, scenario := range []string{"reasoning_carrier_roundtrip", "reasoning_carrier_native"} {
		replayPairs = append(replayPairs,
			wireReplayPair{proto: protocol.ProtocolAnthropicMessages, scenario: scenario},
			wireReplayPair{proto: protocol.ProtocolOpenAIResponses, scenario: scenario},
		)
	}
	replayPairs = append(replayPairs,
		wireReplayPair{proto: protocol.ProtocolOpenAIResponses, scenario: "reasoning_encrypted_only"},
		wireReplayPair{proto: protocol.ProtocolOpenAIResponses, scenario: "legacy_history"},
		wireReplayPair{proto: protocol.ProtocolAnthropicMessages, scenario: "legacy_history"},
	)

	for _, pair := range replayPairs {
		t.Run(fmt.Sprintf("replay/%s/%s", pair.proto, pair.scenario), func(t *testing.T) {
			assertBatch05cNextTurnReplay(t, corpus, pair.proto, pair.scenario)
		})
	}
}

// TestWireBatch05dScenarioCoverageAndReplay exercises all six Batch 0.5d
// multimodal, structured-output, truncation finish-reason, and refusal wire
// scenarios across Anthropic Messages, OpenAI Responses, Chat Completions, and
// Gemini GenerateContent. Every protocol slot must carry valid SyntheticPNG or
// SyntheticPDF payloads, matching JSON schemas, accurate terminal stop reasons,
// and identical next-turn requests when folded from SSE frames or buffered JSON.
func TestWireBatch05dScenarioCoverageAndReplay(t *testing.T) {
	corpus := DefaultCorpus()

	for _, scenario := range batch05dFullScenarios {
		for _, proto := range allWireProtocols {
			for _, kind := range []FixtureKind{FixtureRequest, FixtureStream, FixtureResponse} {
				_ = mustSingleWireFixture(t, corpus, proto, kind, scenario)
			}
		}
	}

	assertBatch05dMultimodalAndStructuredRequests(t, corpus)
	assertBatch05dFinishAndRefusalContracts(t, corpus)

	for _, scenario := range batch05dFullScenarios {
		req := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, scenario)
		v := ValidateGeminiRequest(req.Data, RequestValidationOptions{
			DialectRules: []string{"gemini-signature", "antigravity-schema"},
		})
		require.Empty(t, v, "gemini dialect violations in %s: %v", req.Path, v)
	}

	for _, scenario := range batch05dFullScenarios {
		for _, proto := range allWireProtocols {
			t.Run(fmt.Sprintf("replay/%s/%s", proto, scenario), func(t *testing.T) {
				assertBatch05dNextTurnReplay(t, corpus, proto, scenario)
			})
		}
	}
}

// TestWireBatch05eScenarioCoverageAndReplay exercises the final five Batch 0.5e
// wire scenarios (usage_cache, stream_truncated, stream_error_event,
// upstream_sse_quirks, and empty_response) across all four protocols.
// Cache token bucket arithmetic, bidirectional AllowAbnormal SSE grammar modes,
// raw CRLF and multiline framing quirks, empty-body envelopes, and multi-turn
// client simulation invariants must all hold without weakening strict validators.
func TestWireBatch05eScenarioCoverageAndReplay(t *testing.T) {
	corpus := DefaultCorpus()

	for _, scenario := range batch05eFullScenarios {
		for _, proto := range allWireProtocols {
			for _, kind := range []FixtureKind{FixtureRequest, FixtureStream, FixtureResponse} {
				_ = mustSingleWireFixture(t, corpus, proto, kind, scenario)
			}
		}
	}

	for _, scenario := range batch05eStreamOnlyScenarios {
		for _, proto := range allWireProtocols {
			_ = mustSingleWireFixture(t, corpus, proto, FixtureRequest, scenario)
			_ = mustSingleWireFixture(t, corpus, proto, FixtureStream, scenario)
			assertNoWireFixture(t, corpus, proto, FixtureResponse, scenario)
		}
	}

	batch05eAll := append(slices.Clone(batch05eFullScenarios), batch05eStreamOnlyScenarios...)
	for _, scenario := range batch05eAll {
		req := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, scenario)
		v := ValidateGeminiRequest(req.Data, RequestValidationOptions{
			DialectRules: []string{"gemini-signature", "antigravity-schema"},
		})
		require.Empty(t, v, "gemini dialect violations in %s: %v", req.Path, v)
	}

	assertBatch05eUsageCacheContracts(t, corpus)
	assertBatch05eAbnormalStreamModes(t, corpus)
	assertBatch05eUpstreamSSEQuirks(t, corpus)
	assertBatch05eEmptyResponseContracts(t, corpus)
}

// TestWireSection55ScenarioCatalogComplete asserts that all 33 canonical
// scenarios from section 5.5 of protocol-conversion-ir-refactor.md are present
// on disk under testdata/wire/ with no missing or unrecognized scenario slugs.
// Every scenario must supply both client request and SSE stream fixtures, and
// all non-fault scenarios must also supply buffered response fixtures.
func TestWireSection55ScenarioCatalogComplete(t *testing.T) {
	require.Len(t, allSection55Scenarios, 33, "section 5.5 defines exactly 33 canonical scenario slugs")

	wantSet := make(map[string]bool, len(allSection55Scenarios))
	for _, slug := range allSection55Scenarios {
		require.False(t, wantSet[slug], "duplicate scenario slug %q in allSection55Scenarios", slug)
		wantSet[slug] = true
	}

	corpus := DefaultCorpus()
	allWire, err := corpus.QueryFixtures(FixtureQuery{Source: SourceWire})
	require.NoError(t, err)
	require.NotEmpty(t, allWire)

	seenScenarios := make(map[string]bool, len(allSection55Scenarios))
	seenProtocols := make(map[protocol.ProtocolID]bool, len(allWireProtocols))
	byScenarioKind := make(map[string]map[FixtureKind]int, len(allSection55Scenarios))

	for _, fix := range allWire {
		require.True(t, wantSet[fix.Scenario], "unrecognized scenario %q in %s", fix.Scenario, fix.Path)
		require.Contains(t, allWireProtocols, fix.Protocol, "unexpected wire protocol %q in %s", fix.Protocol, fix.Path)
		seenScenarios[fix.Scenario] = true
		seenProtocols[fix.Protocol] = true

		kinds := byScenarioKind[fix.Scenario]
		if kinds == nil {
			kinds = make(map[FixtureKind]int, 3)
			byScenarioKind[fix.Scenario] = kinds
		}
		kinds[fix.Kind]++
	}

	for _, proto := range allWireProtocols {
		require.True(t, seenProtocols[proto], "missing wire protocol directory for %s", proto)
	}

	gotSorted := make([]string, 0, len(seenScenarios))
	for slug := range seenScenarios {
		gotSorted = append(gotSorted, slug)
	}
	slices.Sort(gotSorted)

	wantSorted := slices.Clone(allSection55Scenarios)
	slices.Sort(wantSorted)
	require.Equal(t, wantSorted, gotSorted, "on-disk testdata/wire/ scenario set must match all 33 section 5.5 scenarios")

	for _, slug := range allSection55Scenarios {
		kinds := byScenarioKind[slug]
		require.Positive(t, kinds[FixtureRequest], "scenario %q must populate at least one request fixture", slug)
		require.Positive(t, kinds[FixtureStream], "scenario %q must populate at least one stream fixture", slug)
		if slices.Contains(batch05eStreamOnlyScenarios, slug) {
			require.Zero(t, kinds[FixtureResponse], "mid-stream fault scenario %q must omit buffered response fixtures", slug)
		} else {
			require.Positive(t, kinds[FixtureResponse], "scenario %q must populate at least one buffered response fixture", slug)
		}
	}
}

// assertWireRequestViolations checks ValidateRequestRules output for the wire
// request fixture at fixturePath. Ordinary request fixtures must report zero
// violations, whereas client-ingress fixtures registered in
// expectedClientIngressRequestRules must trigger every listed rule ID and no other violation.
func assertWireRequestViolations(t *testing.T, fixturePath string, violations []RuleViolation) {
	t.Helper()
	wantRules, isIngressQuirk := expectedClientIngressRequestRules[fixturePath]
	if !isIngressQuirk {
		require.Empty(t, violations, "strict request violations: %v", violations)
		return
	}
	require.NotEmpty(t, violations, "client-ingress fixture %s must trigger expected strict upstream rules", fixturePath)
	seen := make(map[string]bool, len(violations))
	for _, v := range violations {
		require.Contains(t, wantRules, v.RuleID, "unexpected request rule violation in %s: %s", fixturePath, v.String())
		seen[v.RuleID] = true
	}
	for _, want := range wantRules {
		require.True(t, seen[want], "client-ingress fixture %s did not trigger expected rule %q", fixturePath, want)
	}
}

// assertWireStreamViolations verifies ValidateStreamGrammar results for the
// SSE wire fixture at fixturePath. Ordinary streams must produce zero
// violations, whereas entries registered in expectedUpstreamQuirkStreamRules
// must trigger every listed rule code and no other grammar violation.
func assertWireStreamViolations(t *testing.T, fixturePath string, violations []Violation) {
	t.Helper()
	wantRules, isQuirk := expectedUpstreamQuirkStreamRules[fixturePath]
	if !isQuirk {
		require.Empty(t, violations, "stream grammar violations: %v", violations)
		return
	}
	require.NotEmpty(t, violations, "upstream-quirk stream %s must trigger expected client grammar rules", fixturePath)
	seen := make(map[string]bool, len(violations))
	for _, v := range violations {
		require.Contains(t, wantRules, v.Rule, "unexpected grammar violation in %s: %s", fixturePath, v.String())
		seen[v.Rule] = true
	}
	for _, want := range wantRules {
		require.True(t, seen[want], "upstream-quirk stream %s did not trigger expected rule %q", fixturePath, want)
	}
}

// assertNoWireFixture verifies that corpus defines no wire fixture for an
// inapplicable protocol, kind, and scenario slot.
func assertNoWireFixture(t *testing.T, corpus *Corpus, proto protocol.ProtocolID, kind FixtureKind, scenario string) {
	t.Helper()
	matches, err := corpus.QueryFixtures(FixtureQuery{
		Source:   SourceWire,
		Protocol: proto,
		Kind:     kind,
		Scenario: scenario,
	})
	require.NoError(t, err)
	require.Empty(t, matches, "inapplicable wire slot %s/%s/%s must remain empty", proto, kind, scenario)
}

// assertWireNextTurnReplay simulates the next client request from both the
// streamed and buffered wire replies for proto and scenario. Resulting next-turn
// requests must pass strict top-level request validation, plus gemini-signature
// and antigravity-schema dialect rules when proto is Gemini GenerateContent.
func assertWireNextTurnReplay(t *testing.T, corpus *Corpus, proto protocol.ProtocolID, scenario string) {
	t.Helper()
	reqFix := mustSingleWireFixture(t, corpus, proto, FixtureRequest, scenario)
	streamFix := mustSingleWireFixture(t, corpus, proto, FixtureStream, scenario)
	respFix := mustSingleWireFixture(t, corpus, proto, FixtureResponse, scenario)

	frames, frameErr := streamFix.Frames()
	require.NoError(t, frameErr)

	opts := RequestValidationOptions{StrictTopLevel: true}
	if proto == protocol.ProtocolGeminiGenerateContent {
		opts.DialectRules = []string{"gemini-signature", "antigravity-schema"}
	}

	nextFromStream, simErr := SimulateNextTurn(proto, TurnInput{
		PrevRequest:              reqFix.Data,
		ResponseFrames:           frames,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Continue.",
	})
	require.NoError(t, simErr)
	require.Empty(t, ValidateRequestRules(proto, nextFromStream, opts))

	nextFromResp, simErr := SimulateNextTurn(proto, TurnInput{
		PrevRequest:              reqFix.Data,
		ResponseBody:             respFix.Data,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Continue.",
	})
	require.NoError(t, simErr)
	require.Empty(t, ValidateRequestRules(proto, nextFromResp, opts))
}

// assertBatch05cCarrierEnvelopes validates every trc1:<scheme>:<payload> token
// across Batch 0.5c client request fixtures against section 3.10 scheme,
// unpadded base64url, and payload field rules. Upstream stream and buffered
// response fixtures must emit native bare tokens rather than gateway envelopes.
func assertBatch05cCarrierEnvelopes(t *testing.T, corpus *Corpus) {
	t.Helper()
	carrierRequests := []wireReplayPair{
		{proto: protocol.ProtocolAnthropicMessages, scenario: "reasoning_token_foreign"},
		{proto: protocol.ProtocolOpenAIResponses, scenario: "reasoning_token_foreign"},
		{proto: protocol.ProtocolGeminiGenerateContent, scenario: "reasoning_token_foreign"},
		{proto: protocol.ProtocolAnthropicMessages, scenario: "reasoning_carrier_roundtrip"},
		{proto: protocol.ProtocolOpenAIResponses, scenario: "reasoning_carrier_roundtrip"},
		{proto: protocol.ProtocolAnthropicMessages, scenario: "reasoning_carrier_native"},
		{proto: protocol.ProtocolOpenAIResponses, scenario: "reasoning_carrier_native"},
		{proto: protocol.ProtocolOpenAIResponses, scenario: "reasoning_encrypted_only"},
	}

	seenSchemes := make(map[string]bool, len(validCarrierSchemes))
	for _, pair := range carrierRequests {
		fix := mustSingleWireFixture(t, corpus, pair.proto, FixtureRequest, pair.scenario)
		tokens := collectWireTokenStrings(t, pair.proto, fix.Data)
		require.NotEmpty(t, tokens, "%s must contain at least one replay token", fix.Path)
		for _, tok := range tokens {
			parsed := parseTestCarrierEnvelope(t, fix.Path, tok)
			seenSchemes[parsed.scheme] = true
		}
	}
	for _, wantScheme := range validCarrierSchemes {
		require.True(t, seenSchemes[wantScheme], "Batch 0.5c carrier fixtures must exercise scheme %q", wantScheme)
	}

	// Verify reasoning_carrier_native also embeds literal "trc1:" inside user text
	// so PrepareNativeReplay path-scoped scanning can verify non-token text preservation.
	msgNative := mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureRequest, "reasoning_carrier_native")
	require.True(t, bytes.Contains(msgNative.Data, []byte("trc1:plain:example")), "%s must include literal trc1 text in user turn", msgNative.Path)
	rspNative := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, "reasoning_carrier_native")
	require.True(t, bytes.Contains(rspNative.Data, []byte("trc1:openai:demo")), "%s must include literal trc1 text in user turn", rspNative.Path)

	// Verify reasoning_encrypted_only carries an empty-summary reasoning item with a plain carrier.
	encOnlyFix := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, "reasoning_encrypted_only")
	encRoot, err := parseRequestJSONObject(encOnlyFix.Data)
	require.NoError(t, err)
	inArr, hasIn, okIn := nodeArrayField(encRoot, "input")
	require.True(t, hasIn && okIn && len(inArr) >= 2)
	rsObj := inArr[1].objVal
	sumArr, hasSum, okSum := nodeArrayField(rsObj, "summary")
	require.True(t, hasSum && okSum && len(sumArr) == 0, "reasoning_encrypted_only must have empty summary array")
	_, hasContent := rsObj["content"]
	require.False(t, hasContent, "reasoning_encrypted_only must omit visible content array")
	encStr, _, _ := nodeStringField(rsObj, "encrypted_content")
	parsedPlain := parseTestCarrierEnvelope(t, encOnlyFix.Path, encStr)
	require.Equal(t, ir.SchemePlain, parsedPlain.scheme)
	require.NotEmpty(t, parsedPlain.text)

	// Verify Batch 0.5c upstream stream and buffered response fixtures carry bare
	// provider tokens rather than pre-sealed gateway trc1 envelopes.
	allBatch05cScenarios := []string{
		"reasoning_summary",
		"reasoning_tool_replay",
		"reasoning_token_foreign",
		"reasoning_carrier_roundtrip",
		"reasoning_carrier_native",
		"reasoning_encrypted_only",
		"legacy_history",
	}
	for _, scenario := range allBatch05cScenarios {
		for _, kind := range []FixtureKind{FixtureStream, FixtureResponse} {
			items, qErr := corpus.QueryFixtures(FixtureQuery{
				Source:   SourceWire,
				Kind:     kind,
				Scenario: scenario,
			})
			require.NoError(t, qErr)
			for _, item := range items {
				require.False(t, bytes.Contains(item.Data, []byte("trc1:")), "upstream %s must emit native bare tokens without trc1 carrier prefix", item.Path)
			}
		}
	}
}

// assertBatch05cNextTurnReplay folds both streamed and buffered replies for
// proto and scenario through SimulateNextTurn and asserts semantic JSON
// equality between the two folded next-turn requests. Protocol-specific dialect
// rules and client simulation variants are applied before strict request validation.
func assertBatch05cNextTurnReplay(t *testing.T, corpus *Corpus, proto protocol.ProtocolID, scenario string) {
	t.Helper()
	reqFix := mustSingleWireFixture(t, corpus, proto, FixtureRequest, scenario)
	streamFix := mustSingleWireFixture(t, corpus, proto, FixtureStream, scenario)
	respFix := mustSingleWireFixture(t, corpus, proto, FixtureResponse, scenario)

	frames, frameErr := streamFix.Frames()
	require.NoError(t, frameErr)

	variant := ClientSimVariant("")
	if proto == protocol.ProtocolOpenAIResponses && scenario == "reasoning_encrypted_only" {
		variant = SimVariantResponsesEncryptedOnly
	}

	nextFromStream, simErr := SimulateNextTurn(proto, TurnInput{
		PrevRequest:              reqFix.Data,
		ResponseFrames:           frames,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Continue.",
		Variant:                  variant,
	})
	require.NoError(t, simErr)

	nextFromResp, simErr := SimulateNextTurn(proto, TurnInput{
		PrevRequest:              reqFix.Data,
		ResponseBody:             respFix.Data,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Continue.",
		Variant:                  variant,
	})
	require.NoError(t, simErr)

	diffs, diffErr := JSONDiff(nextFromStream, nextFromResp)
	require.NoError(t, diffErr)
	require.Empty(t, diffs, "streamed and buffered replies for %s/%s must fold to identical next-turn JSON", proto, scenario)

	opts := RequestValidationOptions{StrictTopLevel: true}
	switch {
	case proto == protocol.ProtocolGeminiGenerateContent:
		opts.DialectRules = []string{"gemini-signature", "antigravity-schema"}
	case proto == protocol.ProtocolOpenAIChatCompletions && scenario == "reasoning_tool_replay":
		opts.DialectRules = []string{"deepseek-thinking"}
	case proto == protocol.ProtocolAnthropicMessages && scenario == "legacy_history":
		strictViolations := ValidateMessagesRequest(nextFromStream, opts)
		assertWireRequestViolations(t, reqFix.Path, strictViolations)
		opts.DialectRules = []string{"passback"}
	}

	require.Empty(t, ValidateRequestRules(proto, nextFromStream, opts))
	require.Empty(t, ValidateRequestRules(proto, nextFromResp, opts))
}

// assertBatch05dMultimodalAndStructuredRequests checks inline base64 and remote
// URL image parts on image_input, inline PDF attachments on document_input, and
// identical json_schema contracts plus schema-valid assistant JSON outputs on
// structured_output across all four wire protocols.
func assertBatch05dMultimodalAndStructuredRequests(t *testing.T, corpus *Corpus) {
	t.Helper()
	const wantRemoteImageURL = "https://assets.example.com/diagrams/architecture-overview.png"

	for _, proto := range allWireProtocols {
		imgReq := mustSingleWireFixture(t, corpus, proto, FixtureRequest, "image_input")
		require.True(t, bytes.Contains(imgReq.Data, []byte(SyntheticPNGDataBase64)), "%s must embed SyntheticPNGDataBase64", imgReq.Path)
		require.True(t, bytes.Contains(imgReq.Data, []byte(wantRemoteImageURL)), "%s must include remote HTTPS image URL", imgReq.Path)

		docReq := mustSingleWireFixture(t, corpus, proto, FixtureRequest, "document_input")
		require.True(t, bytes.Contains(docReq.Data, []byte(SyntheticPDFDataBase64)), "%s must embed SyntheticPDFDataBase64", docReq.Path)
		require.True(t, bytes.Contains(docReq.Data, []byte("application/pdf")), "%s must declare application/pdf MIME", docReq.Path)
	}

	assertBatch05dImageInputShapes(t, corpus, wantRemoteImageURL)
	assertBatch05dDocumentInputShapes(t, corpus)
	assertBatch05dStructuredOutputShapes(t, corpus)
}

// assertBatch05dImageInputShapes verifies protocol-specific base64 and URL image
// request representations across Messages, Responses, Chat, and Gemini.
func assertBatch05dImageInputShapes(t *testing.T, corpus *Corpus, wantRemoteURL string) {
	t.Helper()

	msgRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureRequest, "image_input").Data)
	require.NoError(t, err)
	msgTurns, _, _ := nodeArrayField(msgRoot, "messages")
	msgBlocks, _, _ := nodeArrayField(msgTurns[0].objVal, "content")
	require.Len(t, msgBlocks, 3)
	src0, _, _ := nodeObjectField(msgBlocks[0].objVal, "source")
	src0Type, _, _ := nodeStringField(src0, "type")
	require.Equal(t, "base64", src0Type)
	src1, _, _ := nodeObjectField(msgBlocks[1].objVal, "source")
	src1Type, _, _ := nodeStringField(src1, "type")
	src1URL, _, _ := nodeStringField(src1, "url")
	require.Equal(t, "url", src1Type)
	require.Equal(t, wantRemoteURL, src1URL)

	rspRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, "image_input").Data)
	require.NoError(t, err)
	rspItems, _, _ := nodeArrayField(rspRoot, "input")
	rspParts, _, _ := nodeArrayField(rspItems[0].objVal, "content")
	require.Len(t, rspParts, 3)
	rspURL0, _, _ := nodeStringField(rspParts[0].objVal, "image_url")
	rspURL1, _, _ := nodeStringField(rspParts[1].objVal, "image_url")
	require.True(t, strings.HasPrefix(rspURL0, "data:image/png;base64,"))
	require.Equal(t, wantRemoteURL, rspURL1)

	chatRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureRequest, "image_input").Data)
	require.NoError(t, err)
	chatMsgs, _, _ := nodeArrayField(chatRoot, "messages")
	chatParts, _, _ := nodeArrayField(chatMsgs[0].objVal, "content")
	require.Len(t, chatParts, 3)
	chatImg0, _, _ := nodeObjectField(chatParts[0].objVal, "image_url")
	chatImg1, _, _ := nodeObjectField(chatParts[1].objVal, "image_url")
	chatURL0, _, _ := nodeStringField(chatImg0, "url")
	chatURL1, _, _ := nodeStringField(chatImg1, "url")
	require.True(t, strings.HasPrefix(chatURL0, "data:image/png;base64,"))
	require.Equal(t, wantRemoteURL, chatURL1)

	gemRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, "image_input").Data)
	require.NoError(t, err)
	gemTurns, _, _ := nodeArrayField(gemRoot, "contents")
	gemParts, _, _ := nodeArrayField(gemTurns[0].objVal, "parts")
	require.Len(t, gemParts, 3)
	_, hasInline, okInline := nodeObjectField(gemParts[0].objVal, "inlineData")
	fileObj, hasFile, okFile := nodeObjectField(gemParts[1].objVal, "fileData")
	require.True(t, hasInline && okInline)
	require.True(t, hasFile && okFile)
	gemURI, _, _ := nodeStringField(fileObj, "fileUri")
	require.Equal(t, wantRemoteURL, gemURI)
}

// assertBatch05dDocumentInputShapes verifies protocol-specific PDF document
// blocks and file parts across Messages, Responses, Chat, and Gemini.
func assertBatch05dDocumentInputShapes(t *testing.T, corpus *Corpus) {
	t.Helper()

	msgRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureRequest, "document_input").Data)
	require.NoError(t, err)
	msgTurns, _, _ := nodeArrayField(msgRoot, "messages")
	msgBlocks, _, _ := nodeArrayField(msgTurns[0].objVal, "content")
	bType, _, _ := nodeStringField(msgBlocks[0].objVal, "type")
	require.Equal(t, "document", bType)
	docSrc, _, _ := nodeObjectField(msgBlocks[0].objVal, "source")
	docMIME, _, _ := nodeStringField(docSrc, "media_type")
	require.Equal(t, "application/pdf", docMIME)
	title, _, _ := nodeStringField(msgBlocks[0].objVal, "title")
	require.Equal(t, "quarterly-sla-report.pdf", title)

	rspRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, "document_input").Data)
	require.NoError(t, err)
	rspItems, _, _ := nodeArrayField(rspRoot, "input")
	rspParts, _, _ := nodeArrayField(rspItems[0].objVal, "content")
	rspType, _, _ := nodeStringField(rspParts[0].objVal, "type")
	rspData, _, _ := nodeStringField(rspParts[0].objVal, "file_data")
	require.Equal(t, "input_file", rspType)
	require.True(t, strings.HasPrefix(rspData, "data:application/pdf;base64,"))

	chatRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureRequest, "document_input").Data)
	require.NoError(t, err)
	chatMsgs, _, _ := nodeArrayField(chatRoot, "messages")
	chatParts, _, _ := nodeArrayField(chatMsgs[0].objVal, "content")
	chatType, _, _ := nodeStringField(chatParts[0].objVal, "type")
	chatFileObj, _, _ := nodeObjectField(chatParts[0].objVal, "file")
	chatData, _, _ := nodeStringField(chatFileObj, "file_data")
	require.Equal(t, "file", chatType)
	require.True(t, strings.HasPrefix(chatData, "data:application/pdf;base64,"))

	gemRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, "document_input").Data)
	require.NoError(t, err)
	gemTurns, _, _ := nodeArrayField(gemRoot, "contents")
	gemParts, _, _ := nodeArrayField(gemTurns[0].objVal, "parts")
	gemInline, _, _ := nodeObjectField(gemParts[0].objVal, "inlineData")
	gemMIME, _, _ := nodeStringField(gemInline, "mimeType")
	require.Equal(t, "application/pdf", gemMIME)
}

// assertBatch05dStructuredOutputShapes verifies that all four structured_output
// request fixtures carry identical JSON Schema definitions and that all four
// buffered responses contain valid JSON payloads matching the declared schema.
func assertBatch05dStructuredOutputShapes(t *testing.T, corpus *Corpus) {
	t.Helper()

	msgRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureRequest, "structured_output").Data)
	require.NoError(t, err)
	outCfg, _, _ := nodeObjectField(msgRoot, "output_config")
	msgFmt, _, _ := nodeObjectField(outCfg, "format")
	msgFmtType, _, _ := nodeStringField(msgFmt, "type")
	require.Equal(t, "json_schema", msgFmtType)
	msgSchemaBytes, err := encodeLosslessNode(msgFmt["schema"])
	require.NoError(t, err)

	rspRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, "structured_output").Data)
	require.NoError(t, err)
	rspTxt, _, _ := nodeObjectField(rspRoot, "text")
	rspFmt, _, _ := nodeObjectField(rspTxt, "format")
	rspFmtType, _, _ := nodeStringField(rspFmt, "type")
	require.Equal(t, "json_schema", rspFmtType)
	rspSchemaBytes, err := encodeLosslessNode(rspFmt["schema"])
	require.NoError(t, err)

	chatRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureRequest, "structured_output").Data)
	require.NoError(t, err)
	chatRF, _, _ := nodeObjectField(chatRoot, "response_format")
	chatRFType, _, _ := nodeStringField(chatRF, "type")
	require.Equal(t, "json_schema", chatRFType)
	chatJS, _, _ := nodeObjectField(chatRF, "json_schema")
	chatSchemaBytes, err := encodeLosslessNode(chatJS["schema"])
	require.NoError(t, err)

	gemRoot, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, "structured_output").Data)
	require.NoError(t, err)
	gemGen, _, _ := nodeObjectField(gemRoot, "generationConfig")
	gemMIME, _, _ := nodeStringField(gemGen, "responseMimeType")
	require.Equal(t, "application/json", gemMIME)
	gemSchemaBytes, err := encodeLosslessNode(gemGen["responseJsonSchema"])
	require.NoError(t, err)

	for _, other := range [][]byte{rspSchemaBytes, chatSchemaBytes, gemSchemaBytes} {
		diffs, diffErr := JSONDiff(msgSchemaBytes, other)
		require.NoError(t, diffErr)
		require.Empty(t, diffs, "structured_output JSON schemas must match across all four protocols")
	}

	for _, proto := range allWireProtocols {
		respFix := mustSingleWireFixture(t, corpus, proto, FixtureResponse, "structured_output")
		textOut := extractBatch05dBufferedAssistantText(t, proto, respFix.Data)
		parsed, parseErr := parseFrameJSONObject([]byte(textOut))
		require.NoError(t, parseErr, "%s assistant structured output must parse as a JSON object", respFix.Path)
		svc, _, _ := nodeStringField(parsed, "service")
		require.Equal(t, "edge-gateway", svc)
		require.Equal(t, nodeBool, parsed["healthy"].kind)
		require.True(t, parsed["healthy"].boolVal)
		incidents, hasInc, okInc := nodeArrayField(parsed, "open_incidents")
		require.True(t, hasInc && okInc && len(incidents) == 0)
	}
}

// assertBatch05dFinishAndRefusalContracts checks protocol-specific request stop
// settings, truncation terminal reasons on stop_sequence and max_tokens, and
// historical plus response-side refusal shapes across all four wire protocols.
func assertBatch05dFinishAndRefusalContracts(t *testing.T, corpus *Corpus) {
	t.Helper()

	// stop_sequence request parameters and response stop metadata.
	msgStopReq, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureRequest, "stop_sequence").Data)
	require.NoError(t, err)
	msgStops, hasMsgStops, okMsgStops := nodeArrayField(msgStopReq, "stop_sequences")
	require.True(t, hasMsgStops && okMsgStops && len(msgStops) == 2)

	rspStopReq, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, "stop_sequence").Data)
	require.NoError(t, err)
	_, rspHasStop := rspStopReq["stop"]
	_, rspHasStopSeq := rspStopReq["stop_sequences"]
	require.False(t, rspHasStop || rspHasStopSeq, "Responses stop_sequence request must omit unsupported stop fields")

	chatStopReq, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureRequest, "stop_sequence").Data)
	require.NoError(t, err)
	chatStops, hasChatStops, okChatStops := nodeArrayField(chatStopReq, "stop")
	require.True(t, hasChatStops && okChatStops && len(chatStops) == 2)

	gemStopReq, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, "stop_sequence").Data)
	require.NoError(t, err)
	gemGen, _, _ := nodeObjectField(gemStopReq, "generationConfig")
	gemStops, hasGemStops, okGemStops := nodeArrayField(gemGen, "stopSequences")
	require.True(t, hasGemStops && okGemStops && len(gemStops) == 2)

	msgStopResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureResponse, "stop_sequence").Data)
	require.NoError(t, err)
	msgSR, _, _ := nodeStringField(msgStopResp, "stop_reason")
	msgSS, _, _ := nodeStringField(msgStopResp, "stop_sequence")
	require.Equal(t, "stop_sequence", msgSR)
	require.Equal(t, "</answer>", msgSS)
	msgStopStream := mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureStream, "stop_sequence")
	require.True(t, bytes.Contains(msgStopStream.Data, []byte(`"stop_reason":"stop_sequence","stop_sequence":"</answer>"`)))

	// max_tokens truncation reasons across all four protocols.
	msgMaxResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureResponse, "max_tokens").Data)
	require.NoError(t, err)
	msgMaxSR, _, _ := nodeStringField(msgMaxResp, "stop_reason")
	require.Equal(t, "max_tokens", msgMaxSR)

	rspMaxResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureResponse, "max_tokens").Data)
	require.NoError(t, err)
	rspMaxStatus, _, _ := nodeStringField(rspMaxResp, "status")
	rspIncDetails, _, _ := nodeObjectField(rspMaxResp, "incomplete_details")
	rspIncReason, _, _ := nodeStringField(rspIncDetails, "reason")
	require.Equal(t, "incomplete", rspMaxStatus)
	require.Equal(t, "max_output_tokens", rspIncReason)
	rspMaxStream := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureStream, "max_tokens")
	require.True(t, bytes.Contains(rspMaxStream.Data, []byte("event: response.incomplete\n")))

	chatMaxResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureResponse, "max_tokens").Data)
	require.NoError(t, err)
	chatChoices, _, _ := nodeArrayField(chatMaxResp, "choices")
	chatMaxFR, _, _ := nodeStringField(chatChoices[0].objVal, "finish_reason")
	require.Equal(t, "length", chatMaxFR)

	gemMaxResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureResponse, "max_tokens").Data)
	require.NoError(t, err)
	gemCands, _, _ := nodeArrayField(gemMaxResp, "candidates")
	gemMaxFR, _, _ := nodeStringField(gemCands[0].objVal, "finishReason")
	require.Equal(t, "MAX_TOKENS", gemMaxFR)

	assertBatch05dRefusalShapes(t, corpus)
}

// assertBatch05dRefusalShapes verifies historical refusal parts on Responses and
// Chat requests alongside protocol-native refusal stream events and finish reasons.
func assertBatch05dRefusalShapes(t *testing.T, corpus *Corpus) {
	t.Helper()

	msgRefResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureResponse, "refusal").Data)
	require.NoError(t, err)
	msgRefSR, _, _ := nodeStringField(msgRefResp, "stop_reason")
	require.Equal(t, "refusal", msgRefSR)

	rspRefReq, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureRequest, "refusal").Data)
	require.NoError(t, err)
	rspIn, _, _ := nodeArrayField(rspRefReq, "input")
	rspHistParts, _, _ := nodeArrayField(rspIn[1].objVal, "content")
	rspHistType, _, _ := nodeStringField(rspHistParts[0].objVal, "type")
	require.Equal(t, "refusal", rspHistType)

	rspRefStream := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureStream, "refusal")
	require.True(t, bytes.Contains(rspRefStream.Data, []byte("event: response.refusal.delta\n")))
	require.True(t, bytes.Contains(rspRefStream.Data, []byte("event: response.refusal.done\n")))

	chatRefReq, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureRequest, "refusal").Data)
	require.NoError(t, err)
	chatInMsgs, _, _ := nodeArrayField(chatRefReq, "messages")
	require.Equal(t, nodeNull, chatInMsgs[1].objVal["content"].kind)
	chatHistRef, _, _ := nodeStringField(chatInMsgs[1].objVal, "refusal")
	require.NotEmpty(t, chatHistRef)

	chatRefResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureResponse, "refusal").Data)
	require.NoError(t, err)
	chatRefChoices, _, _ := nodeArrayField(chatRefResp, "choices")
	chatRefFR, _, _ := nodeStringField(chatRefChoices[0].objVal, "finish_reason")
	chatRefMsg, _, _ := nodeObjectField(chatRefChoices[0].objVal, "message")
	chatOutRef, _, _ := nodeStringField(chatRefMsg, "refusal")
	require.Equal(t, "content_filter", chatRefFR)
	require.Equal(t, nodeNull, chatRefMsg["content"].kind)
	require.NotEmpty(t, chatOutRef)

	gemRefResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureResponse, "refusal").Data)
	require.NoError(t, err)
	gemRefCands, _, _ := nodeArrayField(gemRefResp, "candidates")
	gemRefFR, _, _ := nodeStringField(gemRefCands[0].objVal, "finishReason")
	require.Equal(t, "SAFETY", gemRefFR)
}

// extractBatch05dBufferedAssistantText reads the first assistant text payload
// from a buffered wire response JSON document for proto. Unsupported protocol
// identifiers fail t immediately rather than returning an empty string.
func extractBatch05dBufferedAssistantText(t *testing.T, proto protocol.ProtocolID, data []byte) string {
	t.Helper()
	obj, err := parseFrameJSONObject(data)
	require.NoError(t, err)

	switch proto {
	case protocol.ProtocolAnthropicMessages:
		blocks, _, _ := nodeArrayField(obj, "content")
		txt, _, _ := nodeStringField(blocks[0].objVal, "text")
		return txt
	case protocol.ProtocolOpenAIResponses:
		outArr, _, _ := nodeArrayField(obj, "output")
		parts, _, _ := nodeArrayField(outArr[0].objVal, "content")
		txt, _, _ := nodeStringField(parts[0].objVal, "text")
		return txt
	case protocol.ProtocolOpenAIChatCompletions:
		choices, _, _ := nodeArrayField(obj, "choices")
		msg, _, _ := nodeObjectField(choices[0].objVal, "message")
		txt, _, _ := nodeStringField(msg, "content")
		return txt
	case protocol.ProtocolGeminiGenerateContent:
		cands, _, _ := nodeArrayField(obj, "candidates")
		cnt, _, _ := nodeObjectField(cands[0].objVal, "content")
		parts, _, _ := nodeArrayField(cnt, "parts")
		txt, _, _ := nodeStringField(parts[0].objVal, "text")
		return txt
	default:
		t.Fatalf("unsupported protocol %q", proto)
		return ""
	}
}

// assertBatch05dNextTurnReplay folds both streamed and buffered replies for
// proto and scenario through SimulateNextTurn and compares the resulting
// next-turn requests with JSONDiff. Both folded requests must be semantically
// identical and pass strict top-level and Gemini dialect request validation.
func assertBatch05dNextTurnReplay(t *testing.T, corpus *Corpus, proto protocol.ProtocolID, scenario string) {
	t.Helper()
	reqFix := mustSingleWireFixture(t, corpus, proto, FixtureRequest, scenario)
	streamFix := mustSingleWireFixture(t, corpus, proto, FixtureStream, scenario)
	respFix := mustSingleWireFixture(t, corpus, proto, FixtureResponse, scenario)

	frames, frameErr := streamFix.Frames()
	require.NoError(t, frameErr)

	nextFromStream, simErr := SimulateNextTurn(proto, TurnInput{
		PrevRequest:              reqFix.Data,
		ResponseFrames:           frames,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Continue.",
	})
	require.NoError(t, simErr)

	nextFromResp, simErr := SimulateNextTurn(proto, TurnInput{
		PrevRequest:              reqFix.Data,
		ResponseBody:             respFix.Data,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Continue.",
	})
	require.NoError(t, simErr)

	diffs, diffErr := JSONDiff(nextFromStream, nextFromResp)
	require.NoError(t, diffErr)
	require.Empty(t, diffs, "streamed and buffered replies for %s/%s must fold to identical next-turn JSON", proto, scenario)

	opts := RequestValidationOptions{StrictTopLevel: true}
	if proto == protocol.ProtocolGeminiGenerateContent {
		opts.DialectRules = []string{"gemini-signature", "antigravity-schema"}
	}
	require.Empty(t, ValidateRequestRules(proto, nextFromStream, opts))
	require.Empty(t, ValidateRequestRules(proto, nextFromResp, opts))
}

// assertBatch05eUsageCacheContracts verifies prompt cache request controls,
// disjoint versus inclusive input token bucket totals, 5m/1h and 30m/12h TTL
// breakdowns, sparse tail usage frames, and next-turn replay equivalence on
// usage_cache across all four wire protocols.
func assertBatch05eUsageCacheContracts(t *testing.T, corpus *Corpus) {
	t.Helper()
	const (
		wantFreshInput = int64(120)
		wantCacheRead  = int64(640)
		wantCacheWrite = int64(300)
		wantShortTTL   = int64(180)
		wantLongTTL    = int64(120)
		wantOutput     = int64(28)
		wantInclusive  = wantFreshInput + wantCacheRead + wantCacheWrite
	)

	// Anthropic Messages: disjoint input_tokens, 5m/1h cache_creation, speed:"fast",
	// and sparse output-only message_delta usage.
	msgReq, err := parseRequestJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureRequest, "usage_cache").Data)
	require.NoError(t, err)
	msgSpeed, _, _ := nodeStringField(msgReq, "speed")
	require.Equal(t, "fast", msgSpeed)
	msgSys, _, _ := nodeArrayField(msgReq, "system")
	require.Len(t, msgSys, 2)
	cc0, _, _ := nodeObjectField(msgSys[0].objVal, "cache_control")
	ttl0, _, _ := nodeStringField(cc0, "ttl")
	require.Equal(t, "1h", ttl0)
	cc1, _, _ := nodeObjectField(msgSys[1].objVal, "cache_control")
	ttl1, _, _ := nodeStringField(cc1, "ttl")
	require.Equal(t, "5m", ttl1)

	msgStream := mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureStream, "usage_cache")
	msgFrames, err := msgStream.Frames()
	require.NoError(t, err)
	msgDeltaObj, err := parseFrameJSONObject(msgFrames[len(msgFrames)-2].Data)
	require.NoError(t, err)
	msgDeltaUsage, _, _ := nodeObjectField(msgDeltaObj, "usage")
	_, deltaHasInput := msgDeltaUsage["input_tokens"]
	require.False(t, deltaHasInput, "Messages usage_cache message_delta must exercise sparse output-only usage merge")

	msgResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureResponse, "usage_cache").Data)
	require.NoError(t, err)
	msgUsage, _, _ := nodeObjectField(msgResp, "usage")
	msgIn, _, _ := nodeIntField(msgUsage, "input_tokens")
	msgRead, _, _ := nodeIntField(msgUsage, "cache_read_input_tokens")
	msgWrite, _, _ := nodeIntField(msgUsage, "cache_creation_input_tokens")
	msgOut, _, _ := nodeIntField(msgUsage, "output_tokens")
	msgRespSpeed, _, _ := nodeStringField(msgUsage, "speed")
	msgCC, _, _ := nodeObjectField(msgUsage, "cache_creation")
	msg5m, _, _ := nodeIntField(msgCC, "ephemeral_5m_input_tokens")
	msg1h, _, _ := nodeIntField(msgCC, "ephemeral_1h_input_tokens")
	require.Equal(t, wantFreshInput, msgIn)
	require.Equal(t, wantCacheRead, msgRead)
	require.Equal(t, wantCacheWrite, msgWrite)
	require.Equal(t, wantShortTTL, msg5m)
	require.Equal(t, wantLongTTL, msg1h)
	require.Equal(t, wantOutput, msgOut)
	require.Equal(t, "fast", msgRespSpeed)

	// OpenAI Responses: inclusive input_tokens and 30m/12h cache_write_tokens_details.
	rspResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureResponse, "usage_cache").Data)
	require.NoError(t, err)
	rspTier, _, _ := nodeStringField(rspResp, "service_tier")
	require.Equal(t, "priority", rspTier)
	rspUsage, _, _ := nodeObjectField(rspResp, "usage")
	rspIn, _, _ := nodeIntField(rspUsage, "input_tokens")
	rspOut, _, _ := nodeIntField(rspUsage, "output_tokens")
	rspTot, _, _ := nodeIntField(rspUsage, "total_tokens")
	rspInDet, _, _ := nodeObjectField(rspUsage, "input_tokens_details")
	rspCached, _, _ := nodeIntField(rspInDet, "cached_tokens")
	rspCW, _, _ := nodeIntField(rspInDet, "cache_write_tokens")
	rspCWDet, _, _ := nodeObjectField(rspInDet, "cache_write_tokens_details")
	rsp30m, _, _ := nodeIntField(rspCWDet, "ttl_30m_tokens")
	rsp12h, _, _ := nodeIntField(rspCWDet, "ttl_12h_tokens")
	require.Equal(t, wantInclusive, rspIn)
	require.Equal(t, wantCacheRead, rspCached)
	require.Equal(t, wantCacheWrite, rspCW)
	require.Equal(t, wantShortTTL, rsp30m)
	require.Equal(t, wantLongTTL, rsp12h)
	require.Equal(t, wantOutput, rspOut)
	require.Equal(t, wantInclusive+wantOutput, rspTot)

	// OpenAI Chat Completions: inclusive prompt_tokens plus audio and prediction details.
	chatResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureResponse, "usage_cache").Data)
	require.NoError(t, err)
	chatUsage, _, _ := nodeObjectField(chatResp, "usage")
	chatPrompt, _, _ := nodeIntField(chatUsage, "prompt_tokens")
	chatComp, _, _ := nodeIntField(chatUsage, "completion_tokens")
	chatTot, _, _ := nodeIntField(chatUsage, "total_tokens")
	chatPDet, _, _ := nodeObjectField(chatUsage, "prompt_tokens_details")
	chatCached, _, _ := nodeIntField(chatPDet, "cached_tokens")
	chatCW, _, _ := nodeIntField(chatPDet, "cache_write_tokens")
	chatCDet, _, _ := nodeObjectField(chatUsage, "completion_tokens_details")
	chatAccPred, _, _ := nodeIntField(chatCDet, "accepted_prediction_tokens")
	chatRejPred, _, _ := nodeIntField(chatCDet, "rejected_prediction_tokens")
	require.Equal(t, wantInclusive, chatPrompt)
	require.Equal(t, wantCacheRead, chatCached)
	require.Equal(t, wantCacheWrite, chatCW)
	require.Equal(t, wantOutput, chatComp)
	require.Equal(t, wantInclusive+wantOutput, chatTot)
	require.Equal(t, int64(4), chatAccPred)
	require.Equal(t, int64(2), chatRejPred)

	// Gemini GenerateContent: promptTokenCount includes cachedContentTokenCount,
	// and stream ends with a post-finish usageMetadata-only tail frame.
	gemStream := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureStream, "usage_cache")
	gemFrames, err := gemStream.Frames()
	require.NoError(t, err)
	require.Len(t, gemFrames, 4)
	gemTailObj, err := parseFrameJSONObject(gemFrames[len(gemFrames)-1].Data)
	require.NoError(t, err)
	_, tailHasCandidates := gemTailObj["candidates"]
	require.False(t, tailHasCandidates, "Gemini usage_cache final frame must exercise usageMetadata-only tail window")

	gemResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureResponse, "usage_cache").Data)
	require.NoError(t, err)
	gemUM, _, _ := nodeObjectField(gemResp, "usageMetadata")
	gemPrompt, _, _ := nodeIntField(gemUM, "promptTokenCount")
	gemCached, _, _ := nodeIntField(gemUM, "cachedContentTokenCount")
	gemCand, _, _ := nodeIntField(gemUM, "candidatesTokenCount")
	require.Equal(t, wantFreshInput+wantCacheRead, gemPrompt)
	require.Equal(t, wantCacheRead, gemCached)
	require.Equal(t, wantOutput, gemCand)

	for _, proto := range allWireProtocols {
		t.Run(fmt.Sprintf("replay/%s/usage_cache", proto), func(t *testing.T) {
			assertBatch05dNextTurnReplay(t, corpus, proto, "usage_cache")
		})
	}
}

// assertBatch05eAbnormalStreamModes enforces bidirectional GrammarOptions
// checks on stream_error_event and stream_truncated across all four protocols.
// Mid-stream error events must fail normal completion mode and pass
// AllowAbnormal mode with zero violations, whereas silent EOF truncations must
// fail both normal and AllowAbnormal modes.
func assertBatch05eAbnormalStreamModes(t *testing.T, corpus *Corpus) {
	t.Helper()

	for _, proto := range allWireProtocols {
		errFix := mustSingleWireFixture(t, corpus, proto, FixtureStream, "stream_error_event")
		errFrames, err := errFix.Frames()
		require.NoError(t, err)
		assertOneByteSSEFraming(t, errFix.Path, errFix.Data, errFrames)

		normalViolations := ValidateStreamGrammar(proto, errFrames, GrammarOptions{AllowAbnormal: false})
		assertWireStreamViolations(t, errFix.Path, normalViolations)

		abnormalViolations := ValidateStreamGrammar(proto, errFrames, GrammarOptions{AllowAbnormal: true})
		require.Empty(t, abnormalViolations, "%s must pass ValidateStreamGrammar under AllowAbnormal=true", errFix.Path)

		truncFix := mustSingleWireFixture(t, corpus, proto, FixtureStream, "stream_truncated")
		truncFrames, err := truncFix.Frames()
		require.NoError(t, err)
		assertOneByteSSEFraming(t, truncFix.Path, truncFix.Data, truncFrames)

		truncNormal := ValidateStreamGrammar(proto, truncFrames, GrammarOptions{AllowAbnormal: false})
		assertWireStreamViolations(t, truncFix.Path, truncNormal)

		truncAbnormal := ValidateStreamGrammar(proto, truncFrames, GrammarOptions{AllowAbnormal: true})
		wantAbnRules, ok := expectedAbnormalTruncatedStreamRules[truncFix.Path]
		require.True(t, ok, "missing expectedAbnormalTruncatedStreamRules entry for %s", truncFix.Path)
		require.NotEmpty(t, truncAbnormal, "silent EOF truncation in %s must still fail AllowAbnormal=true", truncFix.Path)
		seen := make(map[string]bool, len(truncAbnormal))
		for _, v := range truncAbnormal {
			require.Contains(t, wantAbnRules, v.Rule, "unexpected AllowAbnormal violation in %s: %s", truncFix.Path, v.String())
			seen[v.Rule] = true
		}
		for _, want := range wantAbnRules {
			require.True(t, seen[want], "%s under AllowAbnormal=true did not trigger expected rule %q", truncFix.Path, want)
		}
	}
}

// assertBatch05eUpstreamSSEQuirks verifies that raw byte-level SSE framing
// quirks (CRLF line endings, trailing spaces, comment lines, multiline data
// payloads, missing final blank delimiters, UTF-8 BOM, data-only headers,
// Responses lifecycle variants, and Gemini cumulative replays) remain intact on
// disk and normalize cleanly across sse.Reader and client replay simulators.
func assertBatch05eUpstreamSSEQuirks(t *testing.T, corpus *Corpus) {
	t.Helper()

	for _, proto := range allWireProtocols {
		fix := mustSingleWireFixture(t, corpus, proto, FixtureStream, "upstream_sse_quirks")
		require.True(t, bytes.Contains(fix.Data, []byte("\r\n")), "%s must retain CRLF line endings via .gitattributes", fix.Path)
		require.True(t, bytes.Contains(fix.Data, []byte("  \r\n")), "%s must retain trailing horizontal spaces before CRLF", fix.Path)
		require.True(t, bytes.Contains(fix.Data, []byte(": ")), "%s must include an SSE comment line", fix.Path)
		lastByte := fix.Data[len(fix.Data)-1]
		require.NotEqual(t, byte('\n'), lastByte, "%s must omit a trailing LF at EOF", fix.Path)
		require.NotEqual(t, byte('\r'), lastByte, "%s must omit a trailing CR at EOF", fix.Path)

		frames, err := fix.Frames()
		require.NoError(t, err)
		assertOneByteSSEFraming(t, fix.Path, fix.Data, frames)
	}

	// Messages: every frame is data-only (empty Event header) and includes ping.
	msgStream := mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureStream, "upstream_sse_quirks")
	msgFrames, err := msgStream.Frames()
	require.NoError(t, err)
	sawPing := false
	for _, f := range msgFrames {
		require.Empty(t, f.Event, "Messages upstream_sse_quirks must omit event: headers (22235a63c)")
		if bytes.Contains(f.Data, []byte(`"type":"ping"`)) {
			sawPing = true
		}
	}
	require.True(t, sawPing, "Messages upstream_sse_quirks must include a ping frame")

	// Responses: bare response.created, output_item.done without added, and response.done with empty output.
	rspStream := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureStream, "upstream_sse_quirks")
	rspFrames, err := rspStream.Frames()
	require.NoError(t, err)
	require.Len(t, rspFrames, 3)
	require.Equal(t, "response.created", rspFrames[0].Event)
	require.Empty(t, rspFrames[1].Event)
	require.Equal(t, "response.done", rspFrames[2].Event)

	// Chat Completions: leading UTF-8 BOM, id/retry fields, delta.reasoning alias, and bare data: [DONE] EOF.
	chatStream := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureStream, "upstream_sse_quirks")
	require.True(t, bytes.HasPrefix(chatStream.Data, []byte{0xEF, 0xBB, 0xBF}), "Chat upstream_sse_quirks must start with UTF-8 BOM")
	require.True(t, bytes.HasSuffix(chatStream.Data, []byte("data: [DONE]")), "Chat upstream_sse_quirks must end with unterminated data: [DONE]")
	require.True(t, bytes.Contains(chatStream.Data, []byte(`"reasoning":`)), "Chat upstream_sse_quirks must exercise delta.reasoning alias")

	// Gemini: cumulative text extension (frame 0 -> frame 1) and cumulative tool batch replay (frame 2 -> frame 3).
	gemStream := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureStream, "upstream_sse_quirks")
	gemFrames, err := gemStream.Frames()
	require.NoError(t, err)
	require.Len(t, gemFrames, 4)
	require.True(t, bytes.Contains(gemFrames[0].Data, []byte(`"Normalized quirky upstream SSE "`)))
	require.True(t, bytes.Contains(gemFrames[1].Data, []byte(`"Normalized quirky upstream SSE frames cleanly."`)))
	require.Equal(t, 1, bytes.Count(gemFrames[2].Data, []byte(`"functionCall"`)))
	require.Equal(t, 2, bytes.Count(gemFrames[3].Data, []byte(`"functionCall"`)))
	require.True(t, bytes.Contains(gemFrames[3].Data, []byte(`"call_gem_quirk_1"`)))
	require.True(t, bytes.Contains(gemFrames[3].Data, []byte(`"call_gem_quirk_2"`)))

	// Messages, Responses, and Chat fold stream and buffered response identically;
	// Gemini buffered response replays cleanly under strict step-signature and schema dialects.
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	} {
		t.Run(fmt.Sprintf("replay/%s/upstream_sse_quirks", proto), func(t *testing.T) {
			assertBatch05dNextTurnReplay(t, corpus, proto, "upstream_sse_quirks")
		})
	}

	gemReq := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureRequest, "upstream_sse_quirks")
	gemResp := mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureResponse, "upstream_sse_quirks")
	gemNext, err := SimulateNextTurn(protocol.ProtocolGeminiGenerateContent, TurnInput{
		PrevRequest:              gemReq.Data,
		ResponseBody:             gemResp.Data,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Continue.",
	})
	require.NoError(t, err)
	require.Empty(t, ValidateGeminiRequest(gemNext, RequestValidationOptions{
		StrictTopLevel: true,
		DialectRules:   []string{"gemini-signature", "antigravity-schema"},
	}))
}

// assertBatch05eEmptyResponseContracts verifies zero-content stream and
// buffered response shapes across all four protocols alongside protocol-specific
// empty-turn replay rules in SimulateNextTurn and ValidateRequestRules.
func assertBatch05eEmptyResponseContracts(t *testing.T, corpus *Corpus) {
	t.Helper()

	msgResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolAnthropicMessages, FixtureResponse, "empty_response").Data)
	require.NoError(t, err)
	msgBlocks, hasMB, okMB := nodeArrayField(msgResp, "content")
	require.True(t, hasMB && okMB && len(msgBlocks) == 0)

	rspResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIResponses, FixtureResponse, "empty_response").Data)
	require.NoError(t, err)
	rspOut, hasRO, okRO := nodeArrayField(rspResp, "output")
	require.True(t, hasRO && okRO && len(rspOut) == 0)

	chatResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureResponse, "empty_response").Data)
	require.NoError(t, err)
	chatChoices, _, _ := nodeArrayField(chatResp, "choices")
	chatMsg, _, _ := nodeObjectField(chatChoices[0].objVal, "message")
	chatContent, hasCC, okCC := nodeStringField(chatMsg, "content")
	require.True(t, hasCC && okCC && chatContent == "")

	gemResp, err := parseFrameJSONObject(mustSingleWireFixture(t, corpus, protocol.ProtocolGeminiGenerateContent, FixtureResponse, "empty_response").Data)
	require.NoError(t, err)
	gemCands, _, _ := nodeArrayField(gemResp, "candidates")
	gemCnt, _, _ := nodeObjectField(gemCands[0].objVal, "content")
	gemParts, hasGP, okGP := nodeArrayField(gemCnt, "parts")
	require.True(t, hasGP && okGP && len(gemParts) == 0)

	// Responses folds empty output[] from both stream and buffered response cleanly.
	assertBatch05dNextTurnReplay(t, corpus, protocol.ProtocolOpenAIResponses, "empty_response")

	// Chat folds empty content:"" identically from stream and buffered response,
	// and strict request validation flags the empty historical assistant turn.
	chatReq := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureRequest, "empty_response")
	chatStream := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureStream, "empty_response")
	chatRespFix := mustSingleWireFixture(t, corpus, protocol.ProtocolOpenAIChatCompletions, FixtureResponse, "empty_response")
	chatFrames, err := chatStream.Frames()
	require.NoError(t, err)
	chatNextStream, err := SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, TurnInput{
		PrevRequest:      chatReq.Data,
		ResponseFrames:   chatFrames,
		FollowUpUserText: "Continue.",
	})
	require.NoError(t, err)
	chatNextResp, err := SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, TurnInput{
		PrevRequest:      chatReq.Data,
		ResponseBody:     chatRespFix.Data,
		FollowUpUserText: "Continue.",
	})
	require.NoError(t, err)
	chatDiffs, err := JSONDiff(chatNextStream, chatNextResp)
	require.NoError(t, err)
	require.Empty(t, chatDiffs)
	chatEmptyViolations := ValidateChatRequest(chatNextStream, RequestValidationOptions{StrictTopLevel: true})
	require.Len(t, chatEmptyViolations, 1)
	require.Equal(t, "CHAT-ASSISTANT-BODY", chatEmptyViolations[0].RuleID)

	// Messages and Gemini reject appending a zero-part assistant turn into history.
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolGeminiGenerateContent,
	} {
		reqFix := mustSingleWireFixture(t, corpus, proto, FixtureRequest, "empty_response")
		streamFix := mustSingleWireFixture(t, corpus, proto, FixtureStream, "empty_response")
		respFix := mustSingleWireFixture(t, corpus, proto, FixtureResponse, "empty_response")
		frames, frameErr := streamFix.Frames()
		require.NoError(t, frameErr)
		_, streamSimErr := SimulateNextTurn(proto, TurnInput{
			PrevRequest:      reqFix.Data,
			ResponseFrames:   frames,
			FollowUpUserText: "Continue.",
		})
		require.Error(t, streamSimErr, "%s SimulateNextTurn must reject empty streamed assistant turns", proto)
		_, respSimErr := SimulateNextTurn(proto, TurnInput{
			PrevRequest:      reqFix.Data,
			ResponseBody:     respFix.Data,
			FollowUpUserText: "Continue.",
		})
		require.Error(t, respSimErr, "%s SimulateNextTurn must reject empty buffered assistant turns", proto)
	}
}

// assertOneByteSSEFraming feeds raw byte-by-byte through sse.NewReader and
// verifies that every emitted Frame matches wantFrames identically.
func assertOneByteSSEFraming(t *testing.T, fixturePath string, raw []byte, wantFrames []sse.Frame) {
	t.Helper()
	rd := sse.NewReader(iotest.OneByteReader(bytes.NewReader(raw)))
	var got []sse.Frame
	for rd.Scan() {
		got = append(got, rd.Frame())
	}
	require.NoError(t, rd.Err(), "one-byte SSE scan failed for %s", fixturePath)
	require.Equal(t, wantFrames, got, "one-byte SSE scan produced different frames for %s", fixturePath)
}

// collectWireTokenStrings extracts protocol continuity token strings in scan
// order from assistant or model history items inside a wire request JSON payload.
// Chat Completions requests carry no opaque replay token fields and return nil.
func collectWireTokenStrings(t *testing.T, proto protocol.ProtocolID, body []byte) []string {
	t.Helper()
	root, err := parseRequestJSONObject(body)
	require.NoError(t, err)

	var tokens []string
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		msgs, hasMsgs, okMsgs := nodeArrayField(root, "messages")
		require.True(t, hasMsgs && okMsgs)
		for _, mNode := range msgs {
			if mNode.kind != nodeObject {
				continue
			}
			role, _, _ := nodeStringField(mNode.objVal, "role")
			if role != "assistant" {
				continue
			}
			blocks, hasBlocks, okBlocks := nodeArrayField(mNode.objVal, "content")
			if !hasBlocks || !okBlocks {
				continue
			}
			for _, bNode := range blocks {
				if bNode.kind != nodeObject {
					continue
				}
				bType, _, _ := nodeStringField(bNode.objVal, "type")
				switch bType {
				case "thinking":
					sig, _, _ := nodeStringField(bNode.objVal, "signature")
					tokens = append(tokens, sig)
				case "redacted_thinking":
					dataStr, _, _ := nodeStringField(bNode.objVal, "data")
					tokens = append(tokens, dataStr)
				}
			}
		}

	case protocol.ProtocolOpenAIResponses:
		items, hasIn, okIn := nodeArrayField(root, "input")
		require.True(t, hasIn && okIn)
		for _, iNode := range items {
			if iNode.kind != nodeObject {
				continue
			}
			iType, _, _ := nodeStringField(iNode.objVal, "type")
			if iType != "reasoning" {
				continue
			}
			if enc, hasEnc, okEnc := nodeStringField(iNode.objVal, "encrypted_content"); hasEnc && okEnc {
				tokens = append(tokens, enc)
			}
		}

	case protocol.ProtocolGeminiGenerateContent:
		turns, hasC, okC := nodeArrayField(root, "contents")
		require.True(t, hasC && okC)
		for _, cNode := range turns {
			if cNode.kind != nodeObject {
				continue
			}
			role, _, _ := nodeStringField(cNode.objVal, "role")
			if role != "model" {
				continue
			}
			parts, hasP, okP := nodeArrayField(cNode.objVal, "parts")
			if !hasP || !okP {
				continue
			}
			for _, pNode := range parts {
				if pNode.kind != nodeObject {
					continue
				}
				if sig, hasSig, okSig := nodeStringField(pNode.objVal, "thoughtSignature"); hasSig && okSig {
					tokens = append(tokens, sig)
				}
			}
		}
	}
	return tokens
}

// parseTestCarrierEnvelope parses one trc1:<scheme>:<payload> carrier string
// using unpadded base64url JSON decoding without duplicate object keys.
// Scheme-specific rules from section 3.10 govern required issuer tokens,
// verbatim reasoning text, redaction flags, and positive provider IDs.
func parseTestCarrierEnvelope(t *testing.T, fixturePath, raw string) decodedTestCarrier {
	t.Helper()
	parts := strings.Split(raw, ":")
	require.Len(t, parts, 3, "carrier in %s must have three colon-separated parts: %q", fixturePath, raw)
	require.Equal(t, "trc1", parts[0], "carrier in %s must start with trc1 prefix", fixturePath)

	scheme := parts[1]
	require.Contains(t, validCarrierSchemes, scheme, "invalid carrier scheme %q in %s", scheme, fixturePath)

	payloadB64 := parts[2]
	require.NotEmpty(t, payloadB64, "empty carrier payload in %s", fixturePath)
	require.False(t, strings.ContainsRune(payloadB64, '='), "carrier payload in %s must use unpadded base64url", fixturePath)

	decodedBytes, err := base64.RawURLEncoding.DecodeString(payloadB64)
	require.NoError(t, err, "invalid base64url payload in %s", fixturePath)
	require.Equal(t, payloadB64, base64.RawURLEncoding.EncodeToString(decodedBytes), "non-canonical base64url payload in %s", fixturePath)

	obj, err := parseFrameJSONObject(decodedBytes)
	require.NoError(t, err, "carrier payload in %s must be a JSON object without duplicate keys", fixturePath)

	for k := range obj {
		require.Contains(t, []string{"d", "t", "r", "p"}, k, "unexpected carrier JSON key %q in %s", k, fixturePath)
	}

	var out decodedTestCarrier
	out.scheme = scheme
	if dStr, hasD, okD := nodeStringField(obj, "d"); hasD {
		require.True(t, okD, "carrier d must be a string in %s", fixturePath)
		out.tokenData = dStr
	}
	if tStr, hasT, okT := nodeStringField(obj, "t"); hasT {
		require.True(t, okT, "carrier t must be a string in %s", fixturePath)
		out.text = tStr
	}
	if rVal, hasR := obj["r"]; hasR {
		require.Equal(t, nodeBool, rVal.kind, "carrier r must be a boolean in %s", fixturePath)
		out.redacted = rVal.boolVal
	}
	if pVal, hasP, okP := nodeIntField(obj, "p"); hasP {
		require.True(t, okP && pVal > 0, "carrier p must be a positive integer in %s", fixturePath)
		out.providerID = pVal
	}

	switch scheme {
	case ir.SchemePlain:
		require.Empty(t, out.tokenData, "plain carrier in %s must omit issuer token d", fixturePath)
		require.NotEmpty(t, out.text, "plain carrier in %s requires non-empty reasoning text t", fixturePath)
		require.False(t, out.redacted, "plain carrier in %s must not set r", fixturePath)
		require.Zero(t, out.providerID, "plain carrier in %s must omit provider ID p", fixturePath)

	case ir.SchemeAnthropic, ir.SchemeAntigravityClaude:
		require.NotEmpty(t, out.tokenData, "%s carrier in %s requires issuer token d", scheme, fixturePath)
		require.Zero(t, out.providerID, "%s carrier in %s must omit provider ID p", scheme, fixturePath)
		if out.redacted {
			require.Empty(t, out.text, "redacted %s carrier in %s must omit reasoning text t", scheme, fixturePath)
		} else {
			require.NotEmpty(t, out.text, "thinking %s carrier in %s requires verbatim text t", scheme, fixturePath)
		}

	case ir.SchemeOpenAI, ir.SchemeXAI:
		require.NotEmpty(t, out.tokenData, "%s carrier in %s requires issuer token d", scheme, fixturePath)
		require.Empty(t, out.text, "%s carrier in %s must omit reasoning text t", scheme, fixturePath)
		require.Positive(t, out.providerID, "account-bound %s carrier in %s requires positive p", scheme, fixturePath)

	case ir.SchemeGemini:
		require.NotEmpty(t, out.tokenData, "gemini carrier in %s requires issuer token d", fixturePath)
		require.Empty(t, out.text, "gemini carrier in %s must omit reasoning text t", fixturePath)
		require.False(t, out.redacted, "gemini carrier in %s must not set r", fixturePath)
		require.Zero(t, out.providerID, "gemini carrier in %s must omit provider ID p", fixturePath)
	}

	return out
}

// mustSingleWireFixture queries corpus for a single wire fixture matching
// proto, kind, and scenario. Missing or duplicate fixtures fail t immediately
// before returning the resolved Fixture.
func mustSingleWireFixture(t *testing.T, corpus *Corpus, proto protocol.ProtocolID, kind FixtureKind, scenario string) Fixture {
	t.Helper()
	items, err := corpus.QueryFixtures(FixtureQuery{
		Source:   SourceWire,
		Protocol: proto,
		Kind:     kind,
		Scenario: scenario,
	})
	require.NoError(t, err)
	require.Len(t, items, 1)
	return items[0]
}

// validateBufferedWireResponse checks required envelope, content, finish, and
// usage fields on a non-streaming wire response JSON payload for proto.
// Duplicate JSON object keys and invalid UTF-8 sequences are rejected through
// parseFrameJSONObject to match strict response decoding invariants.
func validateBufferedWireResponse(proto protocol.ProtocolID, data []byte) error {
	obj, err := parseFrameJSONObject(data)
	if err != nil {
		return err
	}
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return validateBufferedMessagesResponse(obj)
	case protocol.ProtocolOpenAIResponses:
		return validateBufferedResponsesResponse(obj)
	case protocol.ProtocolOpenAIChatCompletions:
		return validateBufferedChatResponse(obj)
	case protocol.ProtocolGeminiGenerateContent:
		return validateBufferedGeminiResponse(obj)
	default:
		return fmt.Errorf("unsupported response wire protocol %q", proto)
	}
}

// validateBufferedMessagesResponse checks mandatory envelope, stop_reason, and
// non-negative usage counters on a non-streaming Anthropic Messages JSON object.
// Any missing or out-of-range field returns a descriptive validation error.
func validateBufferedMessagesResponse(obj map[string]jsonNode) error {
	if id, _, _ := nodeStringField(obj, "id"); id == "" {
		return fmt.Errorf("messages response missing non-empty id")
	}
	if typ, _, _ := nodeStringField(obj, "type"); typ != "message" {
		return fmt.Errorf("messages response type = %q, want \"message\"", typ)
	}
	if role, _, _ := nodeStringField(obj, "role"); role != "assistant" {
		return fmt.Errorf("messages response role = %q, want \"assistant\"", role)
	}
	if model, _, _ := nodeStringField(obj, "model"); model == "" {
		return fmt.Errorf("messages response missing non-empty model")
	}
	if _, hasContent, okContent := nodeArrayField(obj, "content"); !hasContent || !okContent {
		return fmt.Errorf("messages response missing content array")
	}
	if sr, _, _ := nodeStringField(obj, "stop_reason"); sr == "" {
		return fmt.Errorf("messages response missing non-empty stop_reason")
	}
	usageObj, hasUsage, okUsage := nodeObjectField(obj, "usage")
	if !hasUsage || !okUsage {
		return fmt.Errorf("messages response missing usage object")
	}
	if inTok, hasIn, okIn := nodeIntField(usageObj, "input_tokens"); !hasIn || !okIn || inTok < 0 {
		return fmt.Errorf("messages response usage.input_tokens must be a non-negative integer")
	}
	if outTok, hasOut, okOut := nodeIntField(usageObj, "output_tokens"); !hasOut || !okOut || outTok < 0 {
		return fmt.Errorf("messages response usage.output_tokens must be a non-negative integer")
	}
	return nil
}

// validateBufferedResponsesResponse checks mandatory envelope and nested usage
// detail fields on a non-streaming OpenAI Responses JSON object. Detected
// envelope violations return an error citing the first failed rule.
func validateBufferedResponsesResponse(obj map[string]jsonNode) error {
	var violations []Violation
	validateResponsesEnvelope(0, "response", map[string]jsonNode{
		"response": {kind: nodeObject, objVal: obj},
	}, true, func(idx int, event, rule, path, msg string) {
		violations = append(violations, Violation{
			Protocol:   protocol.ProtocolOpenAIResponses,
			Rule:       rule,
			FrameIndex: idx,
			Event:      event,
			Path:       path,
			Message:    msg,
		})
	})
	if len(violations) > 0 {
		return fmt.Errorf("responses envelope violation: %s", violations[0].String())
	}
	return nil
}

// validateBufferedChatResponse checks mandatory completion envelope fields,
// non-empty choices, valid finish_reason values, and non-negative usage
// counters on a buffered OpenAI Chat Completions JSON object. Any structural
// or counter violation returns a descriptive error.
func validateBufferedChatResponse(obj map[string]jsonNode) error {
	if id, _, _ := nodeStringField(obj, "id"); id == "" {
		return fmt.Errorf("chat response missing non-empty id")
	}
	if tag, _, _ := nodeStringField(obj, "object"); tag != "chat.completion" {
		return fmt.Errorf("chat response object = %q, want \"chat.completion\"", tag)
	}
	if created, hasC, okC := nodeIntField(obj, "created"); !hasC || !okC || created < 0 {
		return fmt.Errorf("chat response created must be a non-negative integer")
	}
	if model, _, _ := nodeStringField(obj, "model"); model == "" {
		return fmt.Errorf("chat response missing non-empty model")
	}
	choices, hasChoices, okChoices := nodeArrayField(obj, "choices")
	if !hasChoices || !okChoices || len(choices) == 0 || choices[0].kind != nodeObject {
		return fmt.Errorf("chat response requires non-empty choices array")
	}
	cObj := choices[0].objVal
	if _, hasMsg, okMsg := nodeObjectField(cObj, "message"); !hasMsg || !okMsg {
		return fmt.Errorf("chat response choices[0] missing message object")
	}
	if fr, _, _ := nodeStringField(cObj, "finish_reason"); !isValidChatFinishReason(fr) {
		return fmt.Errorf("chat response choices[0].finish_reason = %q is invalid", fr)
	}
	usageObj, hasUsage, okUsage := nodeObjectField(obj, "usage")
	if !hasUsage || !okUsage {
		return fmt.Errorf("chat response missing usage object")
	}
	for _, field := range []string{"prompt_tokens", "completion_tokens", "total_tokens"} {
		if v, has, ok := nodeIntField(usageObj, field); !has || !ok || v < 0 {
			return fmt.Errorf("chat response usage.%s must be a non-negative integer", field)
		}
	}
	return nil
}

// validateBufferedGeminiResponse checks candidate array presence, finishReason
// validity, model role, response parts, and usageMetadata on a buffered Gemini
// GenerateContent JSON object. Any structural or part violation returns an error.
func validateBufferedGeminiResponse(obj map[string]jsonNode) error {
	cands, hasCands, okCands := nodeArrayField(obj, "candidates")
	if !hasCands || !okCands || len(cands) == 0 || cands[0].kind != nodeObject {
		return fmt.Errorf("gemini response requires non-empty candidates array")
	}
	candObj := cands[0].objVal
	if fr, _, _ := nodeStringField(candObj, "finishReason"); !isValidGeminiFinishReason(fr) {
		return fmt.Errorf("gemini response candidates[0].finishReason = %q is invalid", fr)
	}
	if contentObj, hasCnt, okCnt := nodeObjectField(candObj, "content"); hasCnt {
		if !okCnt {
			return fmt.Errorf("gemini response candidates[0].content must be an object")
		}
		if role, hasR, okR := nodeStringField(contentObj, "role"); hasR && (!okR || role != "model") {
			return fmt.Errorf("gemini response content.role = %q, want \"model\"", role)
		}
		parts, hasP, okP := nodeArrayField(contentObj, "parts")
		if hasP && !okP {
			return fmt.Errorf("gemini response content.parts must be an array")
		}
		var partViolations []Violation
		for idx, pNode := range parts {
			validateGeminiResponsePart(0, "", idx, pNode, func(fi int, ev, rule, path, msg string) {
				partViolations = append(partViolations, Violation{
					Protocol:   protocol.ProtocolGeminiGenerateContent,
					Rule:       rule,
					FrameIndex: fi,
					Event:      ev,
					Path:       path,
					Message:    msg,
				})
			})
		}
		if len(partViolations) > 0 {
			return fmt.Errorf("gemini response part violation: %s", partViolations[0].String())
		}
	}
	if _, hasUsage, okUsage := nodeObjectField(obj, "usageMetadata"); !hasUsage || !okUsage {
		return fmt.Errorf("gemini response missing usageMetadata object")
	}
	return nil
}
