package convert

import (
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// multi_turn_scenario_test.go verifies Law M1 (two-turn conversion and replay
// continuity) across exchange.go, stream.go, buffer.go, registry.go,
// dialect.go, tools_lower.go, tools_raise.go, tools_map.go, tools_responses.go,
// carriers.go, native_replay.go, markers.go, and output_meaning.go. Matrix
// runs execute Turn 1 across all twelve Stage 1, Stage 3 Gemini, and Stage 6
// Qoder cross-protocol routes over applicable wire scenarios, synthetic Qoder
// streams, recorded client corpus fixtures, and all client SDK replay variants,
// then validate Turn 2 across every cross-protocol target dialect (including
// Stage 4 Antigravity claude-* and gemini-* and Stage 6 Qoder targets) and
// native PrepareNativeReplay passthrough.
//
// Targeted two-turn suites additionally exercise Stage 3 Gemini continuity
// (including legacy_history wire checks), all six Stage 4 Antigravity
// {Messages, Responses, Chat} x {claude-*, gemini-*} routes, and all three
// Stage 6 Qoder {Messages, Responses, Chat} -> qoder_chat routes over synthetic
// request and SSE fixtures with default client turn simulation. Those scenarios
// cover trc1:gemini, trc1:antigravity_claude, and trc1:plain reasoning and
// plain-text signature envelopes, CallMemory thoughtSignature and upstream call
// ID recall, late signature backfill, unsigned parallel call preservation
// (4b05d79a5), Antigravity cross-family isolation, unsigned Claude thinking
// text downgrade without thinkingConfig, inline response image Markdown data
// URL replay, and Qoder lowered client tool and upstream alias restoration.

var (
	// m1ExcludedWireScenarios lists single-turn abnormal or native-only wire
	// fixtures excluded from normal two-turn Law M1 continuation loops.
	m1ExcludedWireScenarios = map[string]bool{
		"empty_response":           true,
		"stream_truncated":         true,
		"stream_error_event":       true,
		"reasoning_carrier_native": true,
	}

	// turn2Targets enumerates all Turn 2 upstream dialects, covering Anthropic
	// strict and passback, OpenAI same-account and cross-account, xAI function-only,
	// DeepSeek thinking, Gemini signature, Antigravity claude-* and gemini-*, and
	// stateless Qoder targets.
	turn2Targets = []turn2Target{
		{
			name:     "anthropic_strict",
			upstream: protocol.ProtocolAnthropicMessages,
			dialect: Dialect{
				Name:         "anthropic",
				Tools:        ToolSupportFunction | ToolSupportHostedWebSearch,
				ReplayScheme: ir.SchemeAnthropic,
			},
			validation: testkit.RequestValidationOptions{StrictTopLevel: true},
		},
		{
			name:     "anthropic_passback",
			upstream: protocol.ProtocolAnthropicMessages,
			dialect: Dialect{
				Name:             "deepseek-messages",
				Tools:            ToolSupportFunction,
				ReasoningHistory: HistoryToolTurns,
			},
			validation: testkit.RequestValidationOptions{
				StrictTopLevel: true,
				DialectRules:   []string{"passback"},
			},
		},
		{
			name:     "responses_openai_acct101",
			upstream: protocol.ProtocolOpenAIResponses,
			dialect: Dialect{
				Name:         "openai-101",
				ProviderID:   101,
				Tools:        ToolSupportAllResponses,
				ReplayScheme: ir.SchemeOpenAI,
			},
			validation: testkit.RequestValidationOptions{},
		},
		{
			name:     "responses_openai_acct202_switch",
			upstream: protocol.ProtocolOpenAIResponses,
			dialect: Dialect{
				Name:         "openai-202",
				ProviderID:   202,
				Tools:        ToolSupportAllResponses,
				ReplayScheme: ir.SchemeOpenAI,
			},
			validation: testkit.RequestValidationOptions{},
		},
		{
			name:     "responses_xai_fn_only",
			upstream: protocol.ProtocolOpenAIResponses,
			dialect: Dialect{
				Name:                "xai-grok",
				ProviderID:          303,
				Tools:               ToolSupportFunction,
				ReplayScheme:        ir.SchemeXAI,
				OmitStoreAndInclude: true,
			},
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"function-only"},
			},
		},
		{
			name:     "chat_deepseek_thinking",
			upstream: protocol.ProtocolOpenAIChatCompletions,
			dialect: Dialect{
				Name:             "deepseek-chat",
				Tools:            ToolSupportFunction,
				ReasoningHistory: HistoryToolTurns,
			},
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"deepseek-thinking"},
			},
		},
		{
			name:     "gemini_strict_signature",
			upstream: protocol.ProtocolGeminiGenerateContent,
			dialect: Dialect{
				Name:             "gemini",
				Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
				ReplayScheme:     ir.SchemeGemini,
				SchemaDialect:    SchemaGemini,
				PlaceholderToken: geminiPlaceholderSignature,
			},
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"gemini-signature"},
			},
		},
		{
			name:     "antigravity_claude",
			upstream: protocol.ProtocolGeminiGenerateContent,
			dialect: Dialect{
				Name:             "antigravity",
				Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
				ReplayScheme:     ir.SchemeAntigravityClaude,
				SchemaDialect:    SchemaAntigravity,
				PlaceholderToken: "",
			},
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"antigravity-schema"},
			},
		},
		{
			name:     "antigravity_gemini",
			upstream: protocol.ProtocolGeminiGenerateContent,
			dialect: Dialect{
				Name:             "antigravity",
				Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
				ReplayScheme:     ir.SchemeGemini,
				SchemaDialect:    SchemaAntigravity,
				PlaceholderToken: geminiPlaceholderSignature,
			},
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"antigravity-schema", "gemini-signature"},
			},
		},
		{
			name:     "qoder_stateless",
			upstream: protocol.ProtocolQoderChat,
			dialect: Dialect{
				Name:               "qoder",
				Tools:              ToolSupportFunction,
				AllowBillingHeader: true,
			},
			validation: testkit.RequestValidationOptions{},
		},
	}
)

// turn2Target describes a Turn 2 destination endpoint and its strict validation options.
type turn2Target struct {
	name       string                           // diagnostic target name
	upstream   protocol.ProtocolID              // Turn 2 upstream protocol
	dialect    Dialect                          // Turn 2 upstream Dialect
	validation testkit.RequestValidationOptions // strict request validator options
}

// geminiTestWirePart decodes a subset of Gemini Content.parts for continuity assertions.
type geminiTestWirePart struct {
	ThoughtSignature string `json:"thoughtSignature"` // decoded part-level thoughtSignature
	FunctionCall     *struct {
		ID   string `json:"id"`   // decoded functionCall.id
		Name string `json:"name"` // decoded functionCall.name
	} `json:"functionCall"` // decoded functionCall branch
	FunctionResponse *struct {
		ID       string         `json:"id"`       // decoded functionResponse.id
		Name     string         `json:"name"`     // decoded functionResponse.name
		Response jsontext.Value `json:"response"` // decoded functionResponse.response payload
	} `json:"functionResponse"` // decoded functionResponse branch
}

// TestLawM1_MultiTurnReplayAcrossAllRoutesSchemesAndNative executes Turn 1
// through each cross-protocol route P->Q across all applicable T3 wire fixtures
// and T4 recorded client corpus requests, then constructs Turn 2 via P's client
// simulator across all SDK replay variants. Turn 2 must pass strict target
// request validation across all cross-protocol Q' targets and native
// PrepareNativeReplay passthrough routes without leaving residual trc1 tokens.
func TestLawM1_MultiTurnReplayAcrossAllRoutesSchemesAndNative(t *testing.T) {
	for _, spec := range activeConversionRoutes {
		t.Run(spec.slug, func(t *testing.T) {
			clientReqs, err := testkit.WireFixtures(spec.route.Client, testkit.FixtureRequest)
			require.NoError(t, err)
			reqByScenario := make(map[string]testkit.Fixture, len(clientReqs))
			for _, f := range clientReqs {
				reqByScenario[f.Scenario] = f
			}

			upStreams := upstreamGoldenStreamFixtures(t, spec.route.Upstream)
			streamByScenario := make(map[string]testkit.Fixture, len(upStreams))
			for _, f := range upStreams {
				streamByScenario[f.Scenario] = f
			}

			variants := testkit.ClientSimVariants(spec.route.Client)
			require.NotEmpty(t, variants)

			seenScenarios := make(map[string]bool, len(clientReqs)+4)
			for _, reqFix := range clientReqs {
				if m1ExcludedWireScenarios[reqFix.Scenario] {
					continue
				}
				seenScenarios[reqFix.Scenario] = true
				streamFix := selectM1UpstreamStream(t, spec.route.Client, reqFix, streamByScenario)
				t.Run("wire/"+reqFix.Scenario, func(t *testing.T) {
					runLawM1Turn1AndTurn2Matrix(t, spec, reqFix.Scenario, reqFix.Data, streamFix, variants)
				})
			}

			// Also cover normal upstream stream scenarios whose client protocol uses a
			// shared request fixture (such as reasoning_encrypted_only or tools_custom).
			for _, streamFix := range upStreams {
				if m1ExcludedWireScenarios[streamFix.Scenario] || seenScenarios[streamFix.Scenario] {
					continue
				}
				reqBytes := selectClientRequestForStream(spec.route.Client, streamFix.Scenario, fixtureMapBytes(reqByScenario))
				t.Run("stream_only/"+streamFix.Scenario, func(t *testing.T) {
					runLawM1Turn1AndTurn2Matrix(t, spec, streamFix.Scenario, reqBytes, streamFix, variants)
				})
			}

			// T4 recorded client corpus replay across all client-protocol corpus requests.
			corpusReqs, err := testkit.CorpusRequests(spec.route.Client)
			require.NoError(t, err)
			require.NotEmpty(t, corpusReqs)
			for _, cFix := range corpusReqs {
				streamFix := selectM1CorpusStream(t, spec.route.Client, cFix.Data, streamByScenario)
				t.Run("corpus/"+cFix.Path, func(t *testing.T) {
					runLawM1Turn1AndTurn2Matrix(t, spec, "corpus:"+cFix.Path, cFix.Data, streamFix, variants)
				})
			}
		})
	}
}

// fixtureMapBytes extracts raw byte slices keyed by scenario name from byScenario.
func fixtureMapBytes(byScenario map[string]testkit.Fixture) map[string][]byte {
	out := make(map[string][]byte, len(byScenario))
	for k, v := range byScenario {
		out[k] = v.Data
	}
	return out
}

// selectM1UpstreamStream pairs reqFix with its matching upstream stream fixture,
// or falls back to reasoning_tool_replay / reasoning_summary when an upstream
// protocol omits a scenario-specific stream file.
func selectM1UpstreamStream(
	t *testing.T,
	clientProto protocol.ProtocolID,
	reqFix testkit.Fixture,
	streamByScenario map[string]testkit.Fixture,
) testkit.Fixture {
	t.Helper()
	if sf, ok := streamByScenario[reqFix.Scenario]; ok {
		return sf
	}
	return selectM1CorpusStream(t, clientProto, reqFix.Data, streamByScenario)
}

// selectM1CorpusStream pairs a client request with reasoning_tool_replay when
// the request declares tools and reasoning_summary when it declares no tools.
func selectM1CorpusStream(
	t *testing.T,
	clientProto protocol.ProtocolID,
	reqBytes []byte,
	streamByScenario map[string]testkit.Fixture,
) testkit.Fixture {
	t.Helper()
	decoded, err := DecodeRequest(clientProto, reqBytes)
	require.NoError(t, err)
	if len(decoded.Tools) > 0 {
		sf, ok := streamByScenario["reasoning_tool_replay"]
		require.True(t, ok, "missing reasoning_tool_replay upstream stream fixture")
		return sf
	}
	sf, ok := streamByScenario["reasoning_summary"]
	require.True(t, ok, "missing reasoning_summary upstream stream fixture")
	return sf
}

// runLawM1Turn1AndTurn2Matrix runs Turn 1 on spec, simulates Turn 2 across all
// client replay variants, and validates Turn 2 across every cross-protocol and
// native PrepareNativeReplay target.
func runLawM1Turn1AndTurn2Matrix(
	t *testing.T,
	spec routeSpec,
	scenario string,
	reqData []byte,
	streamFix testkit.Fixture,
	variants []testkit.ClientSimVariant,
) {
	t.Helper()
	mem := newMemCallMemory()
	ex1, err := NewExchange(reqData, Options{
		Route:       spec.route,
		ClientModel: spec.clientMdl,
		Runtime:     newDeterministicRuntime(),
		Calls:       mem,
	})
	require.NoError(t, err)

	upReq1, err := ex1.UpstreamRequest()
	require.NoError(t, err)
	v1 := validateUpstreamRequestInvariants(
		spec.route.Upstream,
		upReq1,
		spec.validation,
		spec.route.Client == protocol.ProtocolOpenAIResponses,
	)
	require.Empty(t, v1, "Turn 1 Q3 violations on %s: %v", scenario, v1)

	rawFrames1, err := streamFix.Frames()
	require.NoError(t, err)
	st1 := ex1.Stream()
	var clientFrames1 []sse.Frame
	for _, f := range rawFrames1 {
		batch, wErr := st1.Write(f)
		require.NoError(t, wErr)
		clientFrames1 = append(clientFrames1, batch...)
	}
	tail1, cErr := st1.Close(nil)
	require.NoError(t, cErr)
	clientFrames1 = append(clientFrames1, tail1...)

	for _, variant := range variants {
		turn2ClientReq, err := testkit.SimulateNextTurn(spec.route.Client, testkit.TurnInput{
			PrevRequest:              reqData,
			ResponseFrames:           clientFrames1,
			SynthesizeMissingOutputs: true,
			FollowUpUserText:         "Proceed with next verification step.",
			Variant:                  variant,
		})
		require.NoError(t, err)

		for _, target := range turn2Targets {
			if target.upstream == spec.route.Client {
				// Same-protocol Turn 2: exercise native passthrough via PrepareNativeReplay
				// (plus AdaptResponsesTools when target is function-only Responses).
				verifyTurn2NativePath(t, spec.route.Client, target, variant, scenario, turn2ClientReq)
				continue
			}

			ex2, err := NewExchange(turn2ClientReq, Options{
				Route: Route{
					Client:   spec.route.Client,
					Upstream: target.upstream,
					Dialect:  target.dialect,
				},
				ClientModel: spec.clientMdl,
				Runtime:     newDeterministicRuntime(),
				Calls:       mem,
			})
			require.NoError(t, err)

			upReq2, err := ex2.UpstreamRequest()
			require.NoError(t, err)

			v2 := validateUpstreamRequestInvariants(
				target.upstream,
				upReq2,
				target.validation,
				spec.route.Client == protocol.ProtocolOpenAIResponses,
			)
			require.Empty(
				t,
				v2,
				"Law M1 Turn 2 cross-protocol failed (%s -> %s, variant=%s, scenario=%s): %v\nbody=%s",
				spec.slug,
				target.name,
				variant,
				scenario,
				v2,
				string(upReq2),
			)
		}
	}
}

// verifyTurn2NativePath validates Turn 2 when routed to a native same-protocol
// upstream via PrepareNativeReplay (and AdaptResponsesTools on function-only Responses).
func verifyTurn2NativePath(
	t *testing.T,
	clientProto protocol.ProtocolID,
	target turn2Target,
	variant testkit.ClientSimVariant,
	scenario string,
	turn2ClientReq []byte,
) {
	t.Helper()
	nativeBody, losses, err := PrepareNativeReplay(Route{
		Client:   clientProto,
		Upstream: target.upstream,
		Dialect:  target.dialect,
	}, turn2ClientReq)
	require.NoError(t, err)
	for _, l := range losses {
		require.True(t, l.Kind.IsValid())
	}

	if target.upstream == protocol.ProtocolOpenAIResponses &&
		!target.dialect.Tools.Has(ToolSupportCustom) &&
		NeedsResponsesToolAdaptation(target.dialect, nativeBody, nil) {
		nativeBody, _, _, err = AdaptResponsesTools(nativeBody, target.dialect, nil)
		require.NoError(t, err)
	}

	// Confirm no residual trc1: carrier remains on protocol token paths after PrepareNativeReplay.
	if clientProto == protocol.ProtocolAnthropicMessages || clientProto == protocol.ProtocolOpenAIResponses {
		stillHasCarrier, scanErr := scanNativeTokenPaths(target.upstream, nativeBody)
		require.NoError(t, scanErr)
		require.False(t, stillHasCarrier, "residual trc1 carrier remained on token path in %s", string(nativeBody))
	}

	// Pre-upgrade legacy_history on Messages intentionally starts with an empty
	// bare signature in turn 0 that only passback or cross-protocol conversion repairs.
	if scenario == "legacy_history" &&
		clientProto == protocol.ProtocolAnthropicMessages &&
		target.dialect.ReasoningHistory != HistoryToolTurns {
		return
	}

	// SimVariantMessagesDropThinking simulates a client stripping thinking blocks
	// from the active assistant tool turn while leaving top-level thinking enabled;
	// non-carrier native passthrough rightly leaves the unsealed payload untouched,
	// so strict Anthropic rejects it while passback and cross-protocol routes succeed.
	if clientProto == protocol.ProtocolAnthropicMessages &&
		variant == testkit.SimVariantMessagesDropThinking &&
		target.dialect.ReasoningHistory != HistoryToolTurns {
		return
	}

	valOpts := target.validation
	if clientProto == protocol.ProtocolOpenAIChatCompletions {
		// Native Chat passthrough leaves client messages untouched: under DeepSeek
		// thinking mode, clients preserve reasoning_content on active tool_calls
		// turns (SimVariantChatPreserveReasoning) and strip reasoning_content once a
		// non-tool turn completes (SimVariantChatDropUnknown).
		turn1HasToolCalls := strings.Contains(string(nativeBody), `"tool_calls"`)
		validDeepSeekClientVariant := (turn1HasToolCalls &&
			variant == testkit.SimVariantChatPreserveReasoning &&
			strings.HasPrefix(scenario, "reasoning_") &&
			scenario != "reasoning_encrypted_only") ||
			(!turn1HasToolCalls && variant == testkit.SimVariantChatDropUnknown)
		if !validDeepSeekClientVariant {
			valOpts = testkit.RequestValidationOptions{}
		}
	}

	vNative := testkit.ValidateRequestRules(target.upstream, nativeBody, valOpts)
	require.Empty(
		t,
		vNative,
		"Law M1 Turn 2 native PrepareNativeReplay failed (proto=%s, target=%s, variant=%s, scenario=%s): %v\nbody=%s",
		clientProto,
		target.name,
		variant,
		scenario,
		vNative,
		string(nativeBody),
	)
}

// TestLawM1_GeminiContinuityCallMemoryLateSignatureAndUnsignedStep exercises
// Stage 3 Gemini two-turn continuity across Messages, Responses, and Chat
// clients. It verifies trc1:gemini reasoning round-trips and foreign drops,
// explicit and nameless CallMemory ID/signature recall, late tool signature
// backfill across Stream and Buffer, unsigned parallel sibling preservation
// (4b05d79a5), compatibility retry fallback, and legacy_history Loss accounting.
func TestLawM1_GeminiContinuityCallMemoryLateSignatureAndUnsignedStep(t *testing.T) {
	t.Run("trc1:gemini reasoning envelope roundtrip and cross-scheme switching", func(t *testing.T) {
		for _, spec := range stage3GeminiRoutes[:2] { // messages_to_gemini and responses_to_gemini carry trc1 envelopes
			t.Run(spec.slug, func(t *testing.T) {
				mem := newMemCallMemory()
				req1 := minimalToolClientRequest(spec.route.Client)
				ex1, err := NewExchange(req1, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)

				st1 := ex1.Stream()
				upstreamFrames := []sse.Frame{
					{Data: []byte(`{"responseId":"gem_rt_1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Checking weather service.","thought":true,"thoughtSignature":"CpEBCocB_gem_thought_sig_1"},{"functionCall":{"id":"gem-call-1","name":"get_weather","args":{"city":"Tokyo"}},"thoughtSignature":"CpEBCocB_gem_fc_sig_1"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":18,"candidatesTokenCount":12,"thoughtsTokenCount":9,"totalTokenCount":39}}`)},
				}
				var clientFrames []sse.Frame
				for _, f := range upstreamFrames {
					batch, wErr := st1.Write(f)
					require.NoError(t, wErr)
					clientFrames = append(clientFrames, batch...)
				}
				tail, err := st1.Close(nil)
				require.NoError(t, err)
				clientFrames = append(clientFrames, tail...)

				rawClientSSE := string(formatSSEFrames(clientFrames))
				require.Contains(t, rawClientSSE, `"trc1:gemini:`, "client SSE must seal Gemini thoughtSignature into trc1:gemini carrier")

				turn2Req, err := testkit.SimulateNextTurn(spec.route.Client, testkit.TurnInput{
					PrevRequest:              req1,
					ResponseFrames:           clientFrames,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Summarize the forecast.",
				})
				require.NoError(t, err)

				// Same-scheme Turn 2 back to Gemini unpacks both thoughtSignature values without leaking trc1.
				ex2Gem, err := NewExchange(turn2Req, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				upGem2, err := ex2Gem.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, upGem2, spec.validation))
				require.NotContains(t, string(upGem2), "trc1:")
				require.Contains(t, string(upGem2), `"thought":true`)
				require.Contains(t, string(upGem2), `"thoughtSignature":"CpEBCocB_gem_thought_sig_1"`)
				require.Contains(t, string(upGem2), `"thoughtSignature":"CpEBCocB_gem_fc_sig_1"`)
				require.Contains(t, string(upGem2), `"id":"gem-call-1"`)

				// Switching Turn 2 to foreign schemes drops trc1:gemini with LossReplayTokenDropped and passes strict rules.
				for _, foreign := range turn2Targets {
					if foreign.upstream == protocol.ProtocolGeminiGenerateContent {
						continue
					}
					if foreign.upstream == spec.route.Client {
						nativeOut, nLosses, nErr := PrepareNativeReplay(Route{
							Client:   spec.route.Client,
							Upstream: foreign.upstream,
							Dialect:  foreign.dialect,
						}, turn2Req)
						require.NoError(t, nErr)
						require.Empty(t, testkit.ValidateRequestRules(foreign.upstream, nativeOut, foreign.validation))
						require.NotContains(t, string(nativeOut), "trc1:")
						require.True(t, containsLossKind(nLosses, ir.LossReplayTokenDropped))
						continue
					}
					exForeign, fErr := NewExchange(turn2Req, Options{
						Route: Route{
							Client:   spec.route.Client,
							Upstream: foreign.upstream,
							Dialect:  foreign.dialect,
						},
						ClientModel: spec.clientMdl,
						Runtime:     newDeterministicRuntime(),
						Calls:       mem,
					})
					require.NoError(t, fErr)
					upForeign, fErr := exForeign.UpstreamRequest()
					require.NoError(t, fErr)
					require.Empty(t, validateUpstreamRequestInvariants(
						foreign.upstream,
						upForeign,
						foreign.validation,
						spec.route.Client == protocol.ProtocolOpenAIResponses,
					))
					require.NotContains(t, string(upForeign), "trc1:")
					require.True(t, containsLossKind(exForeign.Losses(), ir.LossReplayTokenDropped))
				}
			})
		}
	})

	t.Run("CallMemory explicit ID rewrite, nameless call omission, and compatibility retry", func(t *testing.T) {
		for _, spec := range stage3GeminiRoutes {
			t.Run(spec.slug, func(t *testing.T) {
				mem := newMemCallMemory()
				mem.Remember("client_paris_id", CallBinding{
					UpstreamID: "gemini-paris",
					Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig_paris_explicit"},
				})
				mem.Remember("client_london_nameless", CallBinding{
					UpstreamID: "",
					Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig_london_nameless"},
				})

				// Client returns tool results in reverse completion order (London before Paris).
				turn2Body := buildTwoCallClientHistory(spec.route.Client, "client_paris_id", "client_london_nameless")
				ex, err := NewExchange(turn2Body, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				upBytes, err := ex.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, upBytes, spec.validation))

				parts1, parts2 := extractGeminiTurn1And2Parts(t, upBytes)
				require.Len(t, parts1, 2)
				require.Len(t, parts2, 2)

				// Call 0 (Paris): recalled explicit Gemini ID + signature; Call 1 (London): nameless wire call + recalled signature.
				require.Equal(t, "gemini-paris", parts1[0].FunctionCall.ID)
				require.Equal(t, "sig_paris_explicit", parts1[0].ThoughtSignature)
				require.Empty(t, parts1[1].FunctionCall.ID, "nameless Gemini call must omit wire id on Turn 2")
				require.Equal(t, "sig_london_nameless", parts1[1].ThoughtSignature)

				// Results reorder to [Paris, London]; Paris carries "gemini-paris" and London omits wire id.
				require.Equal(t, "gemini-paris", parts2[0].FunctionResponse.ID)
				require.Contains(t, string(parts2[0].FunctionResponse.Response), "Paris sunny")
				require.Empty(t, parts2[1].FunctionResponse.ID)
				require.Contains(t, string(parts2[1].FunctionResponse.Response), "London cloudy")

				// Compatibility retry (ReplayScheme cleared, Calls=nil) omits all wire IDs and injects placeholder signature.
				retryDialect := spec.route.Dialect
				retryDialect.ReplayScheme = ""
				exRetry, err := NewExchange(turn2Body, Options{
					Route: Route{
						Client:   spec.route.Client,
						Upstream: protocol.ProtocolGeminiGenerateContent,
						Dialect:  retryDialect,
					},
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
				})
				require.NoError(t, err)
				retryBytes, err := exRetry.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, retryBytes, spec.validation))
				rParts1, rParts2 := extractGeminiTurn1And2Parts(t, retryBytes)
				require.Empty(t, rParts1[0].FunctionCall.ID)
				require.Empty(t, rParts1[1].FunctionCall.ID)
				require.Empty(t, rParts2[0].FunctionResponse.ID)
				require.Empty(t, rParts2[1].FunctionResponse.ID)
				require.Equal(t, geminiPlaceholderSignature, rParts1[0].ThoughtSignature)
				require.Equal(t, geminiPlaceholderSignature, rParts1[1].ThoughtSignature)
			})
		}
	})

	t.Run("late tool signature backfill replays across Stream and Buffer on all 3 routes", func(t *testing.T) {
		lateFrames := []sse.Frame{
			{Data: []byte(`{"responseId":"gem_late_m1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gemini-osaka-1","name":"get_weather","args":{"city":"Osaka"}}},{"text":"Checking second station."}]}}]}`)},
			{Data: []byte(`{"responseId":"gem_late_m1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gemini-osaka-1","name":"get_weather","args":{"city":"Osaka"}},"thoughtSignature":"sig_backfilled_osaka"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":14,"candidatesTokenCount":10,"totalTokenCount":24}}`)},
		}

		for _, spec := range stage3GeminiRoutes {
			t.Run(spec.slug+"/stream", func(t *testing.T) {
				mem := newMemCallMemory()
				req1 := minimalToolClientRequest(spec.route.Client)
				ex1, err := NewExchange(req1, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				st := ex1.Stream()
				var clientFrames []sse.Frame
				for _, f := range lateFrames {
					batch, wErr := st.Write(f)
					require.NoError(t, wErr)
					clientFrames = append(clientFrames, batch...)
				}
				tail, err := st.Close(nil)
				require.NoError(t, err)
				clientFrames = append(clientFrames, tail...)

				turn2Client, err := testkit.SimulateNextTurn(spec.route.Client, testkit.TurnInput{
					PrevRequest:              req1,
					ResponseFrames:           clientFrames,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Done.",
				})
				require.NoError(t, err)

				ex2, err := NewExchange(turn2Client, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				up2, err := ex2.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, up2, spec.validation))
				require.Contains(t, string(up2), `"id":"gemini-osaka-1"`)
				require.Contains(t, string(up2), `"thoughtSignature":"sig_backfilled_osaka"`)
			})

			t.Run(spec.slug+"/buffer", func(t *testing.T) {
				mem := newMemCallMemory()
				req1 := minimalToolClientRequest(spec.route.Client)
				ex1, err := NewExchange(req1, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				buf := ex1.Buffer()
				for _, f := range lateFrames {
					require.NoError(t, buf.Write(f))
				}
				_, err = buf.Close(nil)
				require.NoError(t, err)

				recalled, ok := mem.Recall("gemini-osaka-1")
				require.True(t, ok)
				require.NotNil(t, recalled.Token)
				require.Equal(t, "sig_backfilled_osaka", recalled.Token.Data)
			})
		}
	})

	t.Run("unsigned parallel calls inject placeholder only on step-first call when known (4b05d79a5)", func(t *testing.T) {
		unsignedFrames := []sse.Frame{
			{Data: []byte(`{"responseId":"gem_unsigned_1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gem-u1","name":"get_weather","args":{"city":"Paris"}}},{"functionCall":{"id":"gem-u2","name":"get_weather","args":{"city":"London"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":10,"totalTokenCount":22}}`)},
		}

		for _, spec := range stage3GeminiRoutes {
			t.Run(spec.slug, func(t *testing.T) {
				mem := newMemCallMemory()
				req1 := minimalToolClientRequest(spec.route.Client)
				ex1, err := NewExchange(req1, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				st := ex1.Stream()
				var clientFrames []sse.Frame
				for _, f := range unsignedFrames {
					batch, wErr := st.Write(f)
					require.NoError(t, wErr)
					clientFrames = append(clientFrames, batch...)
				}
				tail, err := st.Close(nil)
				require.NoError(t, err)
				clientFrames = append(clientFrames, tail...)

				turn2Client, err := testkit.SimulateNextTurn(spec.route.Client, testkit.TurnInput{
					PrevRequest:              req1,
					ResponseFrames:           clientFrames,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Compare both cities.",
				})
				require.NoError(t, err)

				// Known-unsigned parallel calls via CallMemory: first gets placeholder, second stays unsigned.
				exKnown, err := NewExchange(turn2Client, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				upKnown, err := exKnown.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, upKnown, spec.validation))

				knownParts1, _ := extractGeminiTurn1And2Parts(t, upKnown)
				require.Len(t, knownParts1, 2)
				require.Equal(t, geminiPlaceholderSignature, knownParts1[0].ThoughtSignature)
				require.Empty(t, knownParts1[1].ThoughtSignature, "second known-unsigned parallel call must stay free of thoughtSignature")

				// Cold CallMemory (Calls == nil): both unknown-unsigned calls receive placeholder.
				exCold, err := NewExchange(turn2Client, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
				})
				require.NoError(t, err)
				upCold, err := exCold.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, upCold, spec.validation))

				coldParts1, _ := extractGeminiTurn1And2Parts(t, upCold)
				require.Len(t, coldParts1, 2)
				require.Equal(t, geminiPlaceholderSignature, coldParts1[0].ThoughtSignature)
				require.Equal(t, geminiPlaceholderSignature, coldParts1[1].ThoughtSignature)
			})
		}
	})

	t.Run("legacy_history drops foreign bare tokens with replay_token_dropped and passes gemini-signature", func(t *testing.T) {
		for _, spec := range stage3GeminiRoutes[:2] {
			t.Run(spec.slug, func(t *testing.T) {
				reqs, err := testkit.WireFixtures(spec.route.Client, testkit.FixtureRequest)
				require.NoError(t, err)
				found := false
				for _, f := range reqs {
					if f.Scenario != "legacy_history" {
						continue
					}
					found = true
					ex, exErr := NewExchange(f.Data, Options{
						Route:       spec.route,
						ClientModel: spec.clientMdl,
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, exErr)
					upBytes, upErr := ex.UpstreamRequest()
					require.NoError(t, upErr)
					require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, upBytes, spec.validation))
					require.True(t, containsLossKind(ex.Losses(), ir.LossReplayTokenDropped),
						"legacy_history on %s must report replay_token_dropped", spec.slug)
					require.NotContains(t, string(upBytes), `"thought":true`)
				}
				require.True(t, found)
			})
		}
	})
}

// TestLawM1_AntigravityClaudeAndGeminiContinuityAndCrossFamilyIsolation
// exercises Stage 4 Antigravity two-turn continuity across Messages, Responses,
// and Chat clients on both claude-* (SchemaAntigravity + antigravity_claude)
// and gemini-* (SchemaAntigravity + gemini + DummyThoughtSignature) dialects.
// Same-family Turn 2 replays restore exact signed thinking text and CallMemory
// bindings while preserving client call IDs on nameless turns, whereas
// cross-family switches isolate signatures, downgrade unsigned Claude thinking
// to visible text without thinkingConfig, and pass strict Antigravity rules.
func TestLawM1_AntigravityClaudeAndGeminiContinuityAndCrossFamilyIsolation(t *testing.T) {
	yes := true
	agClaudeOutLimit := 64000
	agGeminiOutLimit := 65000
	agClaudeDialect := Dialect{
		Name:             "antigravity",
		Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
		ReplayScheme:     ir.SchemeAntigravityClaude,
		SchemaDialect:    SchemaAntigravity,
		PlaceholderToken: "",
		Reasoning: ReasoningCapabilities{
			ControlsKnown:  true,
			Supported:      &yes,
			Toggle:         true,
			Efforts:        []string{"low", "medium", "high", "max"},
			Budget:         true,
			BudgetMin:      1024,
			BudgetMax:      63000,
			ForceBudget:    true,
			AdaptiveBudget: 24576,
			OutputLimit:    &agClaudeOutLimit,
			BudgetPadding:  1000,
		},
	}
	agGeminiDialect := Dialect{
		Name:             "antigravity",
		Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
		ReplayScheme:     ir.SchemeGemini,
		SchemaDialect:    SchemaAntigravity,
		PlaceholderToken: geminiPlaceholderSignature,
		Reasoning: ReasoningCapabilities{
			ControlsKnown: true,
			Supported:     &yes,
			Toggle:        true,
			Efforts:       []string{"minimal", "low", "medium", "high"},
			Budget:        true,
			BudgetMin:     128,
			BudgetMax:     32768,
			OutputLimit:   &agGeminiOutLimit,
			BudgetPadding: 1000,
		},
	}

	clients := []struct {
		slug  string              // diagnostic client route label
		proto protocol.ProtocolID // client wire protocol
		model string              // client-visible model echoed in responses
	}{
		{slug: "messages", proto: protocol.ProtocolAnthropicMessages, model: "claude-sonnet-4-5"},
		{slug: "responses", proto: protocol.ProtocolOpenAIResponses, model: "gpt-5"},
		{slug: "chat", proto: protocol.ProtocolOpenAIChatCompletions, model: "gpt-4o"},
	}

	t.Run("antigravity_claude carrier roundtrip, cross-family gemini switch, and native PrepareNativeReplay", func(t *testing.T) {
		for _, cl := range clients {
			t.Run(cl.slug, func(t *testing.T) {
				mem := newMemCallMemory()
				var req1 []byte
				switch cl.proto {
				case protocol.ProtocolAnthropicMessages:
					req1 = []byte(`{"model":"claude-sonnet-4-5","max_tokens":2048,"stream":true,"thinking":{"type":"enabled","budget_tokens":1024},"tools":[{"name":"get_weather","description":"Weather lookup","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"messages":[{"role":"user","content":"Weather?"}]}`)
				case protocol.ProtocolOpenAIResponses:
					req1 = []byte(`{"model":"gpt-5","stream":true,"reasoning":{"effort":"high","summary":"auto"},"tools":[{"type":"function","name":"get_weather","description":"Weather lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Weather?"}]}]}`)
				default:
					req1 = minimalToolClientRequest(cl.proto)
				}

				route1 := Route{
					Client:   cl.proto,
					Upstream: protocol.ProtocolGeminiGenerateContent,
					Dialect:  agClaudeDialect,
				}
				ex1, err := NewExchange(req1, Options{
					Route:       route1,
					ClientModel: cl.model,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				up1, err := ex1.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateGeminiRequest(up1, testkit.RequestValidationOptions{
					DialectRules: []string{"antigravity-schema"},
				}))

				st1 := ex1.Stream()
				upstreamFrames := []sse.Frame{
					{Data: []byte(`{"responseId":"ag_claude_1","modelVersion":"claude-sonnet-4-5","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Exact signed Claude thinking text.","thought":true,"thoughtSignature":"sig_ag_claude_thought_1"},{"functionCall":{"name":"get_weather","args":{"city":"Kyoto"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":18,"candidatesTokenCount":12,"thoughtsTokenCount":9,"totalTokenCount":39}}`)},
				}
				var clientFrames []sse.Frame
				for _, f := range upstreamFrames {
					batch, wErr := st1.Write(f)
					require.NoError(t, wErr)
					clientFrames = append(clientFrames, batch...)
				}
				tail, err := st1.Close(nil)
				require.NoError(t, err)
				clientFrames = append(clientFrames, tail...)

				rawClientSSE := string(formatSSEFrames(clientFrames))
				if cl.proto != protocol.ProtocolOpenAIChatCompletions {
					require.Contains(t, rawClientSSE, `"trc1:antigravity_claude:`, "client SSE must seal Antigravity Claude signature into trc1:antigravity_claude carrier")
				}

				turn2Req, err := testkit.SimulateNextTurn(cl.proto, testkit.TurnInput{
					PrevRequest:              req1,
					ResponseFrames:           clientFrames,
					ToolOutputs:              []testkit.SimulatedToolOutput{{Name: "get_weather", Output: "Kyoto 18C"}},
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Summarize Kyoto weather.",
				})
				require.NoError(t, err)

				// Same-family Turn 2 back to antigravity_claude unpacks signature on Messages/Responses,
				// or downgrades unsigned Chat reasoning_content to plain text and clears thinkingConfig.
				ex2Same, err := NewExchange(turn2Req, Options{
					Route:       route1,
					ClientModel: cl.model,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				up2Same, err := ex2Same.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateGeminiRequest(up2Same, testkit.RequestValidationOptions{
					DialectRules: []string{"antigravity-schema"},
				}))
				require.NotContains(t, string(up2Same), "trc1:")
				require.NotContains(t, string(up2Same), geminiPlaceholderSignature)
				parts1Same, parts2Same := extractGeminiTurn1And2Parts(t, up2Same)
				if cl.proto != protocol.ProtocolOpenAIChatCompletions {
					require.Contains(t, string(up2Same), `"thought":true`)
					require.Contains(t, string(up2Same), `"thoughtSignature":"sig_ag_claude_thought_1"`)
					require.Contains(t, string(up2Same), `"Exact signed Claude thinking text."`)
					require.NotEmpty(t, parts1Same[len(parts1Same)-1].FunctionCall.ID, "SchemaAntigravity must retain synthesized client call ID when upstream omitted ID")
					require.Equal(t, parts1Same[len(parts1Same)-1].FunctionCall.ID, parts2Same[0].FunctionResponse.ID)
				} else {
					require.True(t, containsLossKind(ex2Same.Losses(), ir.LossReasoningTextInlined))
					require.NotContains(t, string(up2Same), `"thinkingConfig"`)
				}
				require.Contains(t, string(parts2Same[0].FunctionResponse.Response), `"result":`)

				// Cross-family switch inside Antigravity group (claude-* -> gemini-*):
				// drops trc1:antigravity_claude, omits unsigned historical thinking, and injects placeholder on step-first call.
				ex2Gem, err := NewExchange(turn2Req, Options{
					Route: Route{
						Client:   cl.proto,
						Upstream: protocol.ProtocolGeminiGenerateContent,
						Dialect:  agGeminiDialect,
					},
					ClientModel: cl.model,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				up2Gem, err := ex2Gem.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateGeminiRequest(up2Gem, testkit.RequestValidationOptions{
					DialectRules: []string{"antigravity-schema", "gemini-signature"},
				}))
				require.NotContains(t, string(up2Gem), "trc1:")
				require.NotContains(t, string(up2Gem), `"thought":true`)
				parts1Gem, _ := extractGeminiTurn1And2Parts(t, up2Gem)
				require.Len(t, parts1Gem, 1)
				require.Equal(t, geminiPlaceholderSignature, parts1Gem[0].ThoughtSignature)
				if cl.proto != protocol.ProtocolOpenAIChatCompletions {
					require.True(t, containsLossKind(ex2Gem.Losses(), ir.LossReplayTokenDropped))
				}

				// Native passthrough Turn 2 (PrepareNativeReplay) onto Messages or Responses cleans trc1:antigravity_claude.
				if cl.proto == protocol.ProtocolAnthropicMessages || cl.proto == protocol.ProtocolOpenAIResponses {
					for _, target := range turn2Targets {
						if target.upstream != cl.proto {
							continue
						}
						nativeOut, nLosses, nErr := PrepareNativeReplay(Route{
							Client:   cl.proto,
							Upstream: target.upstream,
							Dialect:  target.dialect,
						}, turn2Req)
						require.NoError(t, nErr)
						require.Empty(t, testkit.ValidateRequestRules(target.upstream, nativeOut, target.validation))
						require.NotContains(t, string(nativeOut), "trc1:")
						require.True(t, containsLossKind(nLosses, ir.LossReplayTokenDropped))
					}
				}
			})
		}
	})

	t.Run("antigravity_gemini Turn 1 to antigravity_claude Turn 2 downgrades unsigned thinking and suppresses thinkingConfig", func(t *testing.T) {
		for _, cl := range clients[:2] { // Messages and Responses carry trc1:gemini envelopes
			t.Run(cl.slug, func(t *testing.T) {
				mem := newMemCallMemory()
				var req1 []byte
				if cl.proto == protocol.ProtocolAnthropicMessages {
					req1 = []byte(`{"model":"gemini-2.5-pro","max_tokens":2048,"stream":true,"thinking":{"type":"enabled","budget_tokens":1024},"tools":[{"name":"get_weather","description":"Weather lookup","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"messages":[{"role":"user","content":"Weather?"}]}`)
				} else {
					req1 = []byte(`{"model":"gemini-2.5-pro","stream":true,"reasoning":{"effort":"high","summary":"auto"},"tools":[{"type":"function","name":"get_weather","description":"Weather lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Weather?"}]}]}`)
				}

				ex1, err := NewExchange(req1, Options{
					Route: Route{
						Client:   cl.proto,
						Upstream: protocol.ProtocolGeminiGenerateContent,
						Dialect:  agGeminiDialect,
					},
					ClientModel: cl.model,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				st1 := ex1.Stream()
				upstreamFrames := []sse.Frame{
					{Data: []byte(`{"responseId":"ag_gem_1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Gemini thought summary.","thought":true,"thoughtSignature":"sig_ag_gem_thought_1"},{"functionCall":{"id":"ag_gem_call_1","name":"get_weather","args":{"city":"Sapporo"}},"thoughtSignature":"sig_ag_gem_fc_1"},{"functionCall":{"id":"ag_gem_call_2","name":"get_weather","args":{"city":"Naha"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":18,"candidatesTokenCount":14,"thoughtsTokenCount":7,"totalTokenCount":39}}`)},
				}
				var clientFrames []sse.Frame
				for _, f := range upstreamFrames {
					batch, wErr := st1.Write(f)
					require.NoError(t, wErr)
					clientFrames = append(clientFrames, batch...)
				}
				tail, err := st1.Close(nil)
				require.NoError(t, err)
				clientFrames = append(clientFrames, tail...)

				turn2Req, err := testkit.SimulateNextTurn(cl.proto, testkit.TurnInput{
					PrevRequest:              req1,
					ResponseFrames:           clientFrames,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Compare Sapporo and Naha.",
				})
				require.NoError(t, err)

				// Same-family Turn 2 back to antigravity_gemini recalls step-first signature and keeps known-unsigned sibling clean.
				ex2Same, err := NewExchange(turn2Req, Options{
					Route: Route{
						Client:   cl.proto,
						Upstream: protocol.ProtocolGeminiGenerateContent,
						Dialect:  agGeminiDialect,
					},
					ClientModel: cl.model,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				up2Same, err := ex2Same.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateGeminiRequest(up2Same, testkit.RequestValidationOptions{
					DialectRules: []string{"antigravity-schema", "gemini-signature"},
				}))
				p1Same, _ := extractGeminiTurn1And2Parts(t, up2Same)
				require.Len(t, p1Same, 3)
				require.Equal(t, "sig_ag_gem_thought_1", p1Same[0].ThoughtSignature)
				require.Equal(t, "ag_gem_call_1", p1Same[1].FunctionCall.ID)
				require.Equal(t, "sig_ag_gem_fc_1", p1Same[1].ThoughtSignature)
				require.Equal(t, "ag_gem_call_2", p1Same[2].FunctionCall.ID)
				require.Empty(t, p1Same[2].ThoughtSignature, "known-unsigned parallel call on Antigravity Gemini must stay unsigned")

				// Cross-family switch to antigravity_claude: drops trc1:gemini, downgrades visible summary to text,
				// suppresses thinkingConfig, and omits foreign CallMemory signatures.
				ex2Claude, err := NewExchange(turn2Req, Options{
					Route: Route{
						Client:   cl.proto,
						Upstream: protocol.ProtocolGeminiGenerateContent,
						Dialect:  agClaudeDialect,
					},
					ClientModel: cl.model,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				up2Claude, err := ex2Claude.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateGeminiRequest(up2Claude, testkit.RequestValidationOptions{
					DialectRules: []string{"antigravity-schema"},
				}))
				require.NotContains(t, string(up2Claude), "trc1:")
				require.NotContains(t, string(up2Claude), `"thought":true`)
				require.NotContains(t, string(up2Claude), `"thinkingConfig"`)
				require.Contains(t, string(up2Claude), `"Gemini thought summary."`)
				require.True(t, containsLossKind(ex2Claude.Losses(), ir.LossReplayTokenDropped))
				require.True(t, containsLossKind(ex2Claude.Losses(), ir.LossReasoningTextInlined))
			})
		}
	})

	t.Run("non_thought_text_signature_and_inline_image_multi_turn_replay", func(t *testing.T) {
		const pngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
		turn1SSE := []byte(
			"data: {\"responseId\":\"ag_txt_sig_turn1\",\"modelVersion\":\"claude-sonnet-4-5-thinking\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Step one complete.\",\"thoughtSignature\":\"sig_non_thought_replay_77\"},{\"inlineData\":{\"mimeType\":\"image/png\",\"data\":\"" + pngB64 + "\"}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":15,\"candidatesTokenCount\":12,\"totalTokenCount\":27}}\n\n",
		)
		rawFrames, err := (testkit.Fixture{
			Protocol: protocol.ProtocolGeminiGenerateContent,
			Kind:     testkit.FixtureStream,
			Data:     turn1SSE,
		}).Frames()
		require.NoError(t, err)

		for _, cl := range []struct {
			slug  string
			proto protocol.ProtocolID
			model string
		}{
			{slug: "messages", proto: protocol.ProtocolAnthropicMessages, model: "claude-sonnet-4-5"},
			{slug: "responses", proto: protocol.ProtocolOpenAIResponses, model: "gpt-5"},
		} {
			for _, schemeCase := range []struct {
				name    string
				dialect Dialect
				wantSig string
				rules   []string
			}{
				{
					name:    "antigravity_claude",
					dialect: agClaudeDialect,
					wantSig: "sig_non_thought_replay_77",
					rules:   []string{"antigravity-schema"},
				},
				{
					name:    "antigravity_gemini",
					dialect: agGeminiDialect,
					wantSig: "sig_non_thought_replay_77",
					rules:   []string{"antigravity-schema", "gemini-signature"},
				},
			} {
				t.Run(cl.slug+"_"+schemeCase.name, func(t *testing.T) {
					req1 := minimalClientRequest(cl.proto, true)
					route := Route{
						Client:   cl.proto,
						Upstream: protocol.ProtocolGeminiGenerateContent,
						Dialect:  schemeCase.dialect,
					}
					ex1, err := NewExchange(req1, Options{
						Route:       route,
						ClientModel: cl.model,
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)
					st1 := ex1.Stream()
					var clientFrames []sse.Frame
					for _, f := range rawFrames {
						batch, wErr := st1.Write(f)
						require.NoError(t, wErr)
						clientFrames = append(clientFrames, batch...)
					}
					tail, err := st1.Close(nil)
					require.NoError(t, err)
					clientFrames = append(clientFrames, tail...)

					turn2ClientReq, err := testkit.SimulateNextTurn(cl.proto, testkit.TurnInput{
						PrevRequest:      req1,
						ResponseFrames:   clientFrames,
						FollowUpUserText: "Proceed to step two.",
					})
					require.NoError(t, err)
					require.Contains(t, string(turn2ClientReq), "![image](data:image/png;base64,"+pngB64+")")
					require.Contains(t, string(turn2ClientReq), "trc1:"+schemeCase.dialect.ReplayScheme+":")

					// Same-scheme Turn 2 replay restores the exact thoughtSignature onto model turn.
					ex2Same, err := NewExchange(turn2ClientReq, Options{
						Route:       route,
						ClientModel: cl.model,
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)
					up2Same, err := ex2Same.UpstreamRequest()
					require.NoError(t, err)
					require.Empty(t, testkit.ValidateGeminiRequest(up2Same, testkit.RequestValidationOptions{
						DialectRules: schemeCase.rules,
					}))
					require.False(t, containsLossKind(ex2Same.Losses(), ir.LossReplayTokenDropped))
					require.Contains(t, string(up2Same), `"thoughtSignature":"`+schemeCase.wantSig+`"`)
					require.Contains(t, string(up2Same), "![image](data:image/png;base64,"+pngB64+")")

					// PrepareNativeReplay on same-protocol native target strips foreign carrier cleanly.
					nativePrepared, nativeLosses, err := PrepareNativeReplay(Route{
						Client:   cl.proto,
						Upstream: cl.proto,
						Dialect:  Dialect{ReplayScheme: ir.SchemeAnthropic},
					}, turn2ClientReq)
					require.NoError(t, err)
					require.NotContains(t, string(nativePrepared), "trc1:")
					require.True(t, containsLossKind(nativeLosses, ir.LossReplayTokenDropped))
				})
			}
		}
	})
}

// containsLossKind reports whether losses contains at least one entry of kind.
func containsLossKind(losses []ir.Loss, kind ir.LossKind) bool {
	for _, l := range losses {
		if l.Kind == kind {
			return true
		}
	}
	return false
}

// minimalToolClientRequest builds a single-turn streaming client request for
// clientProto declaring a single get_weather function tool. Unknown protocol
// identifiers fall back to the OpenAI Chat Completions request shape.
func minimalToolClientRequest(clientProto protocol.ProtocolID) []byte {
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"tools":[{"name":"get_weather","description":"Weather lookup","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"messages":[{"role":"user","content":"Weather?"}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"gpt-5","stream":true,"tools":[{"type":"function","name":"get_weather","description":"Weather lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Weather?"}]}]}`)
	default:
		return []byte(`{"model":"gpt-4o","stream":true,"tools":[{"type":"function","function":{"name":"get_weather","description":"Weather lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],"messages":[{"role":"user","content":"Weather?"}]}`)
	}
}

// buildTwoCallClientHistory builds a Turn 2 client request for clientProto
// containing two parallel get_weather calls (idA=Paris, idB=London) followed by
// reversed tool results (London before Paris). All three protocol shapes set an
// explicit 256-token output cap so cross-route comparisons share one token limit.
func buildTwoCallClientHistory(clientProto protocol.ProtocolID, idA, idB string) []byte {
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"tools":[{"name":"get_weather","description":"Weather lookup","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"messages":[{"role":"user","content":"Weather?"},{"role":"assistant","content":[{"type":"tool_use","id":"` + idA + `","name":"get_weather","input":{"city":"Paris"}},{"type":"tool_use","id":"` + idB + `","name":"get_weather","input":{"city":"London"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + idB + `","content":"London cloudy"},{"type":"tool_result","tool_use_id":"` + idA + `","content":"Paris sunny"}]}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"gpt-5","max_output_tokens":256,"tools":[{"type":"function","name":"get_weather","description":"Weather lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Weather?"}]},{"type":"function_call","call_id":"` + idA + `","name":"get_weather","arguments":"{\"city\":\"Paris\"}"},{"type":"function_call","call_id":"` + idB + `","name":"get_weather","arguments":"{\"city\":\"London\"}"},{"type":"function_call_output","call_id":"` + idB + `","output":"London cloudy"},{"type":"function_call_output","call_id":"` + idA + `","output":"Paris sunny"}]}`)
	default:
		return []byte(`{"model":"gpt-4o","max_tokens":256,"tools":[{"type":"function","function":{"name":"get_weather","description":"Weather lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],"messages":[{"role":"user","content":"Weather?"},{"role":"assistant","tool_calls":[{"id":"` + idA + `","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Paris\"}"}},{"id":"` + idB + `","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"London\"}"}}]},{"role":"tool","tool_call_id":"` + idB + `","content":"London cloudy"},{"role":"tool","tool_call_id":"` + idA + `","content":"Paris sunny"}]}`)
	}
}

// extractGeminiTurn1And2Parts decodes contents[1].parts and contents[2].parts
// from upBytes and asserts that Turn 1 uses role "model" and Turn 2 uses role "user".
// Malformed JSON or fewer than three conversation turns fail t immediately.
func extractGeminiTurn1And2Parts(t *testing.T, upBytes []byte) ([]geminiTestWirePart, []geminiTestWirePart) {
	t.Helper()
	var doc struct {
		Contents []struct {
			Role  string               `json:"role"`
			Parts []geminiTestWirePart `json:"parts"`
		} `json:"contents"`
	}
	require.NoError(t, jsonv2.Unmarshal(upBytes, &doc))
	require.GreaterOrEqual(t, len(doc.Contents), 3)
	require.Equal(t, "model", doc.Contents[1].Role)
	require.Equal(t, "user", doc.Contents[2].Role)
	return doc.Contents[1].Parts, doc.Contents[2].Parts
}

// TestLawM1_QoderPlainCarrierLoweredToolsAndUpstreamAliasReplay exercises
// Stage 6 Qoder two-turn continuity across Messages, Responses, and Chat
// clients. Turn 1 emits tokenless Qoder reasoning (sealed as trc1:plain on
// Messages and Responses), upstream tool aliases reversed via
// MapUpstreamToolName, and lowered Responses custom, namespaced, and
// tool_search invocations; Turn 2 replays cleanly onto Qoder, unpacks
// trc1:plain on passback/DeepSeek targets, strips carriers on strict targets,
// and passes PrepareNativeReplay without residual trc1 tokens.
func TestLawM1_QoderPlainCarrierLoweredToolsAndUpstreamAliasReplay(t *testing.T) {
	t.Run("trc1_plain_and_upstream_alias_roundtrip_across_all_three_qoder_routes", func(t *testing.T) {
		for _, spec := range stage6QoderRoutes {
			t.Run(spec.slug, func(t *testing.T) {
				mem := newMemCallMemory()
				req1 := minimalToolClientRequest(spec.route.Client)
				ex1, err := NewExchange(req1, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
					MapUpstreamToolName: func(name string) string {
						if name == "WeatherLookupAlias" {
							return "get_weather"
						}
						return name
					},
				})
				require.NoError(t, err)
				up1, err := ex1.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, validateUpstreamRequestInvariants(
					protocol.ProtocolQoderChat,
					up1,
					spec.validation,
					spec.route.Client == protocol.ProtocolOpenAIResponses,
				))

				rawQoderSSE := wrapQoderSSEInners([]string{
					`{"choices":[{"delta":{"role":"assistant","reasoning_content":"Checking Osaka station pressure; "}}]}`,
					`{"choices":[{"delta":{"reasoning_content":"now calling weather tool."}}]}`,
					`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_qoder_alias_1","type":"function","function":{"name":"WeatherLookupAlias","arguments":"{\"city\":\"Osaka\"}"}}]}}]}`,
					`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
					`{"usage":{"prompt_tokens":28,"completion_tokens":19,"completion_tokens_details":{"reasoning_tokens":10}}}`,
				}, true)
				upstreamFrames, err := (testkit.Fixture{
					Protocol: protocol.ProtocolQoderChat,
					Kind:     testkit.FixtureStream,
					Data:     rawQoderSSE,
				}).Frames()
				require.NoError(t, err)

				st1 := ex1.Stream()
				var clientFrames []sse.Frame
				for _, f := range upstreamFrames {
					batch, wErr := st1.Write(f)
					require.NoError(t, wErr)
					clientFrames = append(clientFrames, batch...)
				}
				tail, err := st1.Close(nil)
				require.NoError(t, err)
				clientFrames = append(clientFrames, tail...)

				rawClientSSE := string(formatSSEFrames(clientFrames))
				require.Contains(t, rawClientSSE, `"get_weather"`)
				require.NotContains(t, rawClientSSE, `"WeatherLookupAlias"`)
				if spec.route.Client != protocol.ProtocolOpenAIChatCompletions {
					require.Contains(t, rawClientSSE, `"trc1:plain:`, "Messages and Responses must seal Qoder plaintext reasoning in trc1:plain")
				} else {
					require.Contains(t, rawClientSSE, `"reasoning_content"`)
				}

				turn2Req, err := testkit.SimulateNextTurn(spec.route.Client, testkit.TurnInput{
					PrevRequest:              req1,
					ResponseFrames:           clientFrames,
					ToolOutputs:              []testkit.SimulatedToolOutput{{Name: "get_weather", Output: "Osaka 21C clear"}},
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Summarize Osaka conditions.",
				})
				require.NoError(t, err)

				// Continuing Turn 2 onto Qoder strips historical reasoning cleanly and pairs get_weather.
				ex2Qoder, err := NewExchange(turn2Req, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
					Calls:       mem,
				})
				require.NoError(t, err)
				up2Qoder, err := ex2Qoder.UpstreamRequest()
				require.NoError(t, err)
				require.Empty(t, validateUpstreamRequestInvariants(
					protocol.ProtocolQoderChat,
					up2Qoder,
					spec.validation,
					spec.route.Client == protocol.ProtocolOpenAIResponses,
				))
				require.NotContains(t, string(up2Qoder), "trc1:")
				require.Contains(t, string(up2Qoder), `"Osaka 21C clear"`)

				// Switching Turn 2 across every foreign target and native PrepareNativeReplay.
				for _, target := range turn2Targets {
					if target.upstream == protocol.ProtocolQoderChat {
						continue
					}
					if target.upstream == spec.route.Client {
						nativeOut, _, nErr := PrepareNativeReplay(Route{
							Client:   spec.route.Client,
							Upstream: target.upstream,
							Dialect:  target.dialect,
						}, turn2Req)
						require.NoError(t, nErr)
						require.NotContains(t, string(nativeOut), "trc1:")
						valOpts := target.validation
						if spec.route.Client == protocol.ProtocolOpenAIChatCompletions {
							// Completed tool turn followed by user follow-up omits DeepSeek active-step reasoning_content.
							valOpts = testkit.RequestValidationOptions{}
						}
						require.Empty(t, testkit.ValidateRequestRules(target.upstream, nativeOut, valOpts))
						continue
					}
					exForeign, fErr := NewExchange(turn2Req, Options{
						Route: Route{
							Client:   spec.route.Client,
							Upstream: target.upstream,
							Dialect:  target.dialect,
						},
						ClientModel: spec.clientMdl,
						Runtime:     newDeterministicRuntime(),
						Calls:       mem,
					})
					require.NoError(t, fErr)
					upForeign, fErr := exForeign.UpstreamRequest()
					require.NoError(t, fErr)
					require.Empty(t, validateUpstreamRequestInvariants(
						target.upstream,
						upForeign,
						target.validation,
						spec.route.Client == protocol.ProtocolOpenAIResponses,
					))
					require.NotContains(t, string(upForeign), "trc1:")
				}
			})
		}
	})

	t.Run("responses_to_qoder_lowered_custom_namespace_and_search_tools_multi_turn", func(t *testing.T) {
		respSpec := stage6QoderRoutes[1]
		req1 := []byte(`{
			"model":"gpt-5",
			"stream":true,
			"tools":[
				{"type":"custom","name":"apply_patch","description":"Apply file patch"},
				{"type":"namespace","name":"git","tools":[
					{"type":"function","name":"status","parameters":{"type":"object","properties":{}}}
				]},
				{"type":"tool_search"}
			],
			"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Patch README, check git status, and search pager tools."}]}]
		}`)

		mem := newMemCallMemory()
		ex1, err := NewExchange(req1, Options{
			Route:       respSpec.route,
			ClientModel: respSpec.clientMdl,
			Runtime:     newDeterministicRuntime(),
			Calls:       mem,
			MapUpstreamToolName: func(name string) string {
				if name == "GitStatusAlias" {
					return "git__status"
				}
				return name
			},
		})
		require.NoError(t, err)
		up1, err := ex1.UpstreamRequest()
		require.NoError(t, err)
		require.Empty(t, validateUpstreamRequestInvariants(protocol.ProtocolQoderChat, up1, respSpec.validation, true))

		rawQoderSSE := wrapQoderSSEInners([]string{
			`{"choices":[{"delta":{"role":"assistant","reasoning_content":"Running patch, status, and tool search."}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_qp_1","type":"function","function":{"name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\\n\"}"}},{"index":1,"id":"call_qg_2","type":"function","function":{"name":"GitStatusAlias","arguments":"{}"}},{"index":2,"id":"call_qs_3","type":"function","function":{"name":"tool_search","arguments":"{\"query\":\"pager\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":64,"completion_tokens":38,"completion_tokens_details":{"reasoning_tokens":9}}}`,
		}, true)
		frames1, err := (testkit.Fixture{
			Protocol: protocol.ProtocolQoderChat,
			Kind:     testkit.FixtureStream,
			Data:     rawQoderSSE,
		}).Frames()
		require.NoError(t, err)

		st1 := ex1.Stream()
		var clientFrames []sse.Frame
		for _, f := range frames1 {
			batch, wErr := st1.Write(f)
			require.NoError(t, wErr)
			clientFrames = append(clientFrames, batch...)
		}
		tail, err := st1.Close(nil)
		require.NoError(t, err)
		clientFrames = append(clientFrames, tail...)

		rawSSE := string(formatSSEFrames(clientFrames))
		require.Contains(t, rawSSE, `"type":"custom_tool_call"`)
		require.Contains(t, rawSSE, `"namespace":"git"`)
		require.Contains(t, rawSSE, `"type":"tool_search_call"`)
		require.Contains(t, rawSSE, `"trc1:plain:`)

		turn2Req, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
			PrevRequest:              req1,
			ResponseFrames:           clientFrames,
			SynthesizeMissingOutputs: true,
			FollowUpUserText:         "All tool outputs collected.",
		})
		require.NoError(t, err)

		// Turn 2 back onto Qoder lowers all three restored client tool calls and pairs every output.
		ex2Qoder, err := NewExchange(turn2Req, Options{
			Route:       respSpec.route,
			ClientModel: respSpec.clientMdl,
			Runtime:     newDeterministicRuntime(),
			Calls:       mem,
		})
		require.NoError(t, err)
		up2Qoder, err := ex2Qoder.UpstreamRequest()
		require.NoError(t, err)
		require.Empty(t, validateUpstreamRequestInvariants(protocol.ProtocolQoderChat, up2Qoder, respSpec.validation, true))
		require.Contains(t, string(up2Qoder), `"git__status"`)
		require.NotContains(t, string(up2Qoder), "trc1:")

		// Turn 2 onto native OpenAI Responses via PrepareNativeReplay + AdaptResponsesTools.
		for _, target := range turn2Targets {
			if target.upstream == protocol.ProtocolOpenAIResponses {
				verifyTurn2NativePath(t, protocol.ProtocolOpenAIResponses, target, "", "qoder_lowered_tools", turn2Req)
			}
		}
	})
}
