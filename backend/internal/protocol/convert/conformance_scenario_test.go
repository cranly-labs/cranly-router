package convert

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	protocolqoder "github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// conformance_scenario_test.go verifies T3 cross-protocol conversion laws and
// T4 recorded client and upstream corpus replay laws (Q3, Q4, Q5, S3, S4, S5,
// S6, S7, S8) across Stage 1 text routes, Stage 3 Gemini routes, Stage 4
// Antigravity request dialects, Stage 6 Qoder routes, and Stage 7 off-table
// Cursor input, Gemini count_tokens, Grok OmitStoreAndInclude, and DeepSeek
// HistoryToolTurns dialects alongside section 3.4 Extra.Members and gjson/sjson
// source scans. It also checks unmodeled field census and route loss goldens
// across exchange.go, stream.go, buffer.go, registry.go, dialect.go,
// tools_lower.go, tools_raise.go, tools_map.go, tools_responses.go, carriers.go,
// native_replay.go, markers.go, and output_meaning.go.

// lossReportEntry records all reported conversion losses for one fixture on one route.
type lossReportEntry struct {
	Fixture string    `json:"fixture"`         // relative testdata fixture path
	Kind    string    `json:"kind"`            // "request" or "stream"
	Losses  []ir.Loss `json:"losses,omitzero"` // ordered reported Loss records
}

// routeLossReport bundles deterministic request and stream loss reports for one route.
type routeLossReport struct {
	Route   string            `json:"route"`   // stable route slug
	Entries []lossReportEntry `json:"entries"` // ordered per-fixture loss entries
}

// chunkedReader feeds data in fixed step-byte slices per Read call to test arbitrary SSE chunk boundaries.
type chunkedReader struct {
	data   []byte // underlying raw SSE bytes
	offset int    // current read cursor
	step   int    // maximum bytes returned per Read call
}

// streamEvalContext bundles stream-level test inputs and converted outputs for
// per-frame (Semantic, Terminal) wire-flag invariant verification.
type streamEvalContext struct {
	spec        routeSpec    // evaluated cross-protocol route descriptor
	fixturePath string       // relative upstream stream fixture path
	abnormal    bool         // true for intentional stream_truncated or stream_error_event scenarios
	newFrames   []sse.Frame  // convert.Stream client SSE frames
	newResp     *ir.Response // decoded convert.Stream client response when normal
}

// TestNoDirectExtraMembersIndexingAndForbiddenLibs enforces section 3.4 rule 1
// and section 3.12 across all handwritten production Go files in convert,
// codec/{chat,messages,responses,generatecontent,qoderchat}, and qoder.
// Production code must never index Extra.Members directly by key or import
// gjson or sjson packages.
func TestNoDirectExtraMembersIndexingAndForbiddenLibs(t *testing.T) {
	sources, err := testkit.ReadGoProductionSources(".", "../codec", "../qoder")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(sources), 55, "expected 13 convert + 41 codec + 1 qoder production files")

	for _, src := range sources {
		text := string(src.Data)
		if strings.Contains(text, ".Members[") {
			t.Fatalf("%s contains forbidden direct .Members[ map indexing", src.Path)
		}
		if strings.Contains(text, `"github.com/tidwall/gjson"`) ||
			strings.Contains(text, `"github.com/tidwall/sjson"`) {
			t.Fatalf("%s imports forbidden gjson or sjson package", src.Path)
		}
	}
}

// TestCorpusUnmodeledFieldCensusGolden walks every recorded client corpus
// request for Messages, Responses, Chat Completions, and Gemini GenerateContent
// and enumerates all JSON paths landing in Extra, OpaqueItem, or Tool.Hosted.
// The sorted census report must match testdata/golden/census/<protocol>.txt so
// newly unmodeled client fields surface immediately during review.
func TestCorpusUnmodeledFieldCensusGolden(t *testing.T) {
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
		protocol.ProtocolGeminiGenerateContent,
	} {
		t.Run(string(proto), func(t *testing.T) {
			corpusReqs, err := testkit.CorpusRequests(proto)
			require.NoError(t, err)
			require.NotEmpty(t, corpusReqs)

			uniqueSet := make(map[string]bool)
			// fixtureCensus pairs a corpus fixture relative path with its unmodeled path lines.
			type fixtureCensus struct {
				name  string   // relative corpus fixture path
				items []string // sorted unmodeled JSON paths observed on fixture
			}
			perFixture := make([]fixtureCensus, 0, len(corpusReqs))

			for _, fix := range corpusReqs {
				req, decErr := DecodeRequest(proto, fix.Data)
				require.NoError(t, decErr)

				items := collectUnmodeledPaths(req)
				for _, it := range items {
					uniqueSet[normalizeCensusSummaryPath(it)] = true
				}
				perFixture = append(perFixture, fixtureCensus{
					name:  fix.Path,
					items: items,
				})
			}

			summaryLines := make([]string, 0, len(uniqueSet))
			for s := range uniqueSet {
				summaryLines = append(summaryLines, s)
			}
			slices.Sort(summaryLines)

			var b strings.Builder
			fmt.Fprintf(&b, "# Unmodeled field census for %s (%d corpus requests)\n", proto, len(corpusReqs))
			_, _ = b.WriteString("[summary]\n")
			if len(summaryLines) == 0 {
				_, _ = b.WriteString("(none)\n")
			} else {
				for _, line := range summaryLines {
					_, _ = b.WriteString(line + "\n")
				}
			}

			for _, fc := range perFixture {
				fmt.Fprintf(&b, "\n[%s]\n", fc.name)
				if len(fc.items) == 0 {
					_, _ = b.WriteString("(none)\n")
					continue
				}
				for _, it := range fc.items {
					_, _ = b.WriteString(it + "\n")
				}
			}

			testkit.Golden(t, "census/"+string(proto)+".txt", []byte(b.String()))
		})
	}
}

// TestLawsQ3Q4Q5_CrossProtocolRequestsAndLossGoldens evaluates Laws Q3 (strict
// target request validation or Qoder PayloadRequest structural invariants), Q4
// (Essence loss accounting), and Q5 (byte determinism) across all Stage 1,
// Stage 3 Gemini, and Stage 6 Qoder routes over every wire and corpus request
// fixture, plus restricted Responses, passback Messages, Gemini budget, Stage 4
// Antigravity claude-* and gemini-*, and Qoder billing/stateless dialect variants.
// Reported route loss inventories must also match testdata/golden/loss/*.json.
func TestLawsQ3Q4Q5_CrossProtocolRequestsAndLossGoldens(t *testing.T) {
	for _, spec := range activeConversionRoutes {
		t.Run(spec.slug, func(t *testing.T) {
			reqs := allProtocolRequestFixtures(t, spec.route.Client)
			var reqLossEntries []lossReportEntry

			for _, fix := range reqs {
				t.Run(fix.Path, func(t *testing.T) {
					ex1, err := NewExchange(fix.Data, Options{
						Route:       spec.route,
						ClientModel: spec.clientMdl,
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)

					up1, err := ex1.UpstreamRequest()
					require.NoError(t, err)
					losses1 := ex1.Losses()
					for _, l := range losses1 {
						require.True(t, l.Kind.IsValid(), "invalid LossKind %q", l.Kind)
					}

					// Law Q3: Converted upstream request passes target strict request validator or Qoder payload invariants.
					violations := validateUpstreamRequestInvariants(
						spec.route.Upstream,
						up1,
						spec.validation,
						spec.route.Client == protocol.ProtocolOpenAIResponses,
					)
					require.Empty(t, violations, "Law Q3 violations on %s (%s): %v\nup=%s", fix.Path, spec.slug, violations, string(up1))

					// Law Q4: Reported losses completely reconcile source and target Essence.
					srcIR, err := DecodeRequest(spec.route.Client, fix.Data)
					require.NoError(t, err)
					dstIR, err := decodeTargetRequestForQ4(spec.route.Upstream, up1)
					require.NoError(t, err)

					essDiffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), losses1)
					require.NoError(t, err, "Law Q4 VerifyEssence error on %s (%s), losses=%v", fix.Path, spec.slug, losses1)
					require.Empty(t, essDiffs, "Law Q4 Essence diff on %s (%s):\n%s\nlosses=%v", fix.Path, spec.slug, testkit.FormatJSONDiff(essDiffs), losses1)

					// Law Q5: Repeated conversions produce byte-identical upstream JSON and losses.
					ex2, err := NewExchange(fix.Data, Options{
						Route:       spec.route,
						ClientModel: spec.clientMdl,
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)
					up2, err := ex2.UpstreamRequest()
					require.NoError(t, err)
					require.True(t, bytes.Equal(up1, up2), "Law Q5 byte non-determinism on %s (%s)", fix.Path, spec.slug)
					require.Equal(t, losses1, ex2.Losses())

					verifyDialectVariantsQ3Q4(t, spec, fix)
				})

				// Record deterministic loss report entry outside subtest parallel closures.
				exReport, err := NewExchange(fix.Data, Options{
					Route:       spec.route,
					ClientModel: spec.clientMdl,
					Runtime:     newDeterministicRuntime(),
				})
				require.NoError(t, err)
				_, err = exReport.UpstreamRequest()
				require.NoError(t, err)
				reqLossEntries = append(reqLossEntries, lossReportEntry{
					Fixture: fix.Path,
					Kind:    "request",
					Losses:  exReport.Losses(),
				})
			}

			streamLossEntries := collectRouteStreamLosses(t, spec)
			report := routeLossReport{
				Route:   spec.slug,
				Entries: append(reqLossEntries, streamLossEntries...),
			}
			testkit.GoldenJSON(t, "loss/"+spec.slug+".json", report)
		})
	}
}

// TestLawsS3S4S5S6S7S8_CrossProtocolResponsesAndStreams evaluates Laws S3
// (client stream grammar), S4 (response Essence loss accounting including
// trc1:plain carriers), S5 (arbitrary SSE byte chunking and text delta
// split/merge invariance), S6 (Stream, Buffer, and Response parity), S7
// (truncation safety at every frame prefix), and S8 (disjoint client usage
// bucket projection and per-frame Semantic/Terminal invariants) across all
// Stage 1, Stage 3 Gemini, and Stage 6 Qoder routes. Every wire, synthetic
// Qoder, and recorded upstream stream fixture must satisfy all applicable laws.
func TestLawsS3S4S5S6S7S8_CrossProtocolResponsesAndStreams(t *testing.T) {
	for _, spec := range activeConversionRoutes {
		t.Run(spec.slug, func(t *testing.T) {
			streams, respByScenario := upstreamStreamAndResponseFixtures(t, spec.route.Upstream)

			clientReqByScenario := make(map[string][]byte)
			clientWireReqs, err := testkit.WireFixtures(spec.route.Client, testkit.FixtureRequest)
			require.NoError(t, err)
			for _, cf := range clientWireReqs {
				clientReqByScenario[cf.Scenario] = cf.Data
			}

			for _, sFix := range streams {
				t.Run(sFix.Name, func(t *testing.T) {
					clientReqBytes := selectClientRequestForStream(spec.route.Client, sFix.Scenario, clientReqByScenario)
					ex, err := NewExchange(clientReqBytes, Options{
						Route:       spec.route,
						ClientModel: spec.clientMdl,
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)
					_, err = ex.UpstreamRequest()
					require.NoError(t, err)

					rawFrames, err := sFix.Frames()
					require.NoError(t, err)

					stream := ex.Stream()
					var outFrames []sse.Frame
					var writeCause error
					for _, f := range rawFrames {
						batch, wErr := stream.Write(f)
						if wErr != nil {
							require.Equal(t, "stream_error_event", sFix.Scenario, "unexpected stream.Write error on %s: %v", sFix.Path, wErr)
							writeCause = wErr
							break
						}
						outFrames = append(outFrames, batch...)
					}
					tail, cErr := stream.Close(writeCause)
					require.NoError(t, cErr)
					outFrames = append(outFrames, tail...)

					abnormal := sFix.Scenario == "stream_truncated" || sFix.Scenario == "stream_error_event"

					// Law S3: Client output passes target stream grammar validator.
					violations := testkit.ValidateStreamGrammar(spec.route.Client, outFrames, testkit.GrammarOptions{
						AllowAbnormal: abnormal,
					})
					require.Empty(t, violations, "Law S3 grammar violations on %s (%s): %v", sFix.Path, spec.slug, violations)

					if abnormal {
						require.NotNil(t, stream.Summary().Err, "abnormal stream %s must record Summary().Err", sFix.Path)
						require.NotEmpty(t, outFrames)
						require.True(t, outFrames[len(outFrames)-1].Terminal)
						verifyNewStreamFlagInvariants(t, streamEvalContext{
							spec:        spec,
							fixturePath: sFix.Path,
							abnormal:    true,
							newFrames:   outFrames,
						})
					} else {
						// Law S4: Normal stream output reconciles ResponseEssence against reported losses.
						_, srcResp := decodeRawStreamToResponse(t, spec.route.Upstream, sFix.Data)
						tagUpstreamResponseTokens(srcResp, ex.route.Dialect)
						raiseResponseTools(srcResp, ex.ToolMap())

						_, dstResp := decodeRawStreamToResponse(t, spec.route.Client, formatSSEFrames(outFrames))
						essDiffs, vErr := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(dstResp), stream.Losses())
						require.NoError(t, vErr, "Law S4 VerifyEssence error on %s (%s), losses=%v", sFix.Path, spec.slug, stream.Losses())
						require.Empty(t, essDiffs, "Law S4 Essence diff on %s (%s):\n%s\nlosses=%v", sFix.Path, spec.slug, testkit.FormatJSONDiff(essDiffs), stream.Losses())

						// Law S5: Re-chunking SSE bytes at 1-byte and 7-byte boundaries and merging/splitting text deltas preserves Essence.
						verifyLawS5ChunkAndDeltaInvariance(t, spec, clientReqBytes, sFix.Data, dstResp)

						// Law S6: Stream, Buffer, and buffered Response produce matching client responses.
						if rFix, hasPairedResp := respByScenario[sFix.Scenario]; hasPairedResp {
							verifyLawS6StreamBufferResponseParity(t, spec, clientReqBytes, sFix, rFix, outFrames, dstResp)
						}

						// Law S8: Client-visible usage derives from disjoint buckets and per-frame (Semantic, Terminal) flags hold.
						verifyLawS8UsageBuckets(t, spec, clientReqBytes, rawFrames, srcResp.Usage, dstResp.Usage)
						verifyNewStreamFlagInvariants(t, streamEvalContext{
							spec:        spec,
							fixturePath: sFix.Path,
							abnormal:    false,
							newFrames:   outFrames,
							newResp:     dstResp,
						})
					}

					// Law S7: Truncating at every raw SSE frame prefix and calling Close(err) aborts validly without panic.
					verifyLawS7PrefixTruncations(t, spec, clientReqBytes, rawFrames)
				})
			}
		})
	}
}

// verifyDialectVariantsQ3Q4 checks function-only Responses, passback Messages,
// budget/level-governed Gemini, Antigravity claude-* and gemini-*, and Qoder
// billing-stripped and effort-stripped dialect variants under Laws Q3, Q4, and Q5.
func verifyDialectVariantsQ3Q4(t *testing.T, spec routeSpec, fix testkit.Fixture) {
	t.Helper()
	switch spec.route.Upstream {
	case protocol.ProtocolOpenAIResponses:
		fnRoute := spec.route
		fnRoute.Dialect.Name = "openai-apikey"
		fnRoute.Dialect.Tools = ToolSupportFunction
		exFn, err := NewExchange(fix.Data, Options{
			Route:       fnRoute,
			ClientModel: spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upFn, err := exFn.UpstreamRequest()
		require.NoError(t, err)
		vFn := testkit.ValidateResponsesRequest(upFn, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		})
		require.Empty(t, vFn, "function-only Q3 violations on %s: %v", fix.Path, vFn)

		srcIR, err := DecodeRequest(fnRoute.Client, fix.Data)
		require.NoError(t, err)
		dstIR, err := DecodeRequest(fnRoute.Upstream, upFn)
		require.NoError(t, err)
		diffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), exFn.Losses())
		require.NoError(t, err)
		require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

	case protocol.ProtocolAnthropicMessages:
		pbRoute := spec.route
		pbRoute.Dialect.Name = "passback-messages"
		pbRoute.Dialect.ReplayScheme = ""
		pbRoute.Dialect.ReasoningHistory = HistoryToolTurns
		exPB, err := NewExchange(fix.Data, Options{
			Route:       pbRoute,
			ClientModel: spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upPB, err := exPB.UpstreamRequest()
		require.NoError(t, err)
		vPB := testkit.ValidateMessagesRequest(upPB, testkit.RequestValidationOptions{
			StrictTopLevel: true,
			DialectRules:   []string{"passback"},
		})
		require.Empty(t, vPB, "passback Q3 violations on %s: %v", fix.Path, vPB)

		srcIR, err := DecodeRequest(pbRoute.Client, fix.Data)
		require.NoError(t, err)
		dstIR, err := DecodeRequest(pbRoute.Upstream, upPB)
		require.NoError(t, err)
		diffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), exPB.Losses())
		require.NoError(t, err)
		require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

	case protocol.ProtocolGeminiGenerateContent:
		yes := true
		gemBudgetRoute := spec.route
		gemBudgetRoute.Dialect.Name = "gemini-2.5-pro-budget"
		gemBudgetRoute.Dialect.Reasoning = ReasoningCapabilities{
			ControlsKnown: true,
			Supported:     &yes,
			Toggle:        true,
			Budget:        true,
			BudgetMin:     128,
			BudgetMax:     32768,
		}
		exGem, err := NewExchange(fix.Data, Options{
			Route:       gemBudgetRoute,
			ClientModel: spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upGem, err := exGem.UpstreamRequest()
		require.NoError(t, err)
		vGem := testkit.ValidateGeminiRequest(upGem, testkit.RequestValidationOptions{
			DialectRules: []string{"gemini-signature"},
		})
		require.Empty(t, vGem, "gemini-2.5-pro-budget Q3 violations on %s: %v", fix.Path, vGem)

		srcIR, err := DecodeRequest(gemBudgetRoute.Client, fix.Data)
		require.NoError(t, err)
		dstIR, err := DecodeRequest(gemBudgetRoute.Upstream, upGem)
		require.NoError(t, err)
		diffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), exGem.Losses())
		require.NoError(t, err)
		require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

		// Verify both Stage 4 Antigravity model-family dialects (claude-* and gemini-*)
		// across Laws Q3, Q4, and Q5 over every wire and client corpus request fixture.
		agClaudeOutLimit := 64000
		agGeminiOutLimit := 65000
		if spec.route.Client == protocol.ProtocolOpenAIChatCompletions {
			agGeminiOutLimit = 64000
		}
		// agDialectCase pairs an Antigravity model-family dialect with its strict request validator rules.
		type agDialectCase struct {
			name         string   // diagnostic variant label
			dialect      Dialect  // target Antigravity model-family dialect
			dialectRules []string // strict Gemini validator dialect rules
		}
		agCases := []agDialectCase{
			{
				name: "antigravity-claude",
				dialect: Dialect{
					Name:             "antigravity",
					Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
					ToolNameMaxBytes: 64,
					ReplayScheme:     ir.SchemeAntigravityClaude,
					ReasoningHistory: HistoryNone,
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
				},
				dialectRules: []string{"antigravity-schema"},
			},
			{
				name: "antigravity-gemini",
				dialect: Dialect{
					Name:             "antigravity",
					Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
					ToolNameMaxBytes: 64,
					ReplayScheme:     ir.SchemeGemini,
					ReasoningHistory: HistoryNone,
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
				},
				dialectRules: []string{"antigravity-schema", "gemini-signature"},
			},
		}
		for _, ag := range agCases {
			agRoute := Route{
				Client:   spec.route.Client,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  ag.dialect,
			}
			exAG1, agErr := NewExchange(fix.Data, Options{
				Route:       agRoute,
				ClientModel: spec.clientMdl,
				Runtime:     newDeterministicRuntime(),
			})
			require.NoError(t, agErr)
			upAG1, agErr := exAG1.UpstreamRequest()
			require.NoError(t, agErr)
			agLosses1 := exAG1.Losses()
			for _, l := range agLosses1 {
				require.True(t, l.Kind.IsValid(), "invalid LossKind %q on %s", l.Kind, ag.name)
			}

			// Law Q3: Antigravity-encoded request passes strict Gemini and antigravity-schema rules.
			vAG := testkit.ValidateGeminiRequest(upAG1, testkit.RequestValidationOptions{
				DialectRules: ag.dialectRules,
			})
			require.Empty(t, vAG, "%s Q3 violations on %s (%s): %v\nup=%s", ag.name, fix.Path, spec.slug, vAG, string(upAG1))

			// Law Q4: Reported losses reconcile source and target Essence completely.
			agDstIR, agErr := DecodeRequest(agRoute.Upstream, upAG1)
			require.NoError(t, agErr)
			agDiffs, agErr := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(agDstIR), agLosses1)
			require.NoError(t, agErr, "%s Q4 VerifyEssence error on %s (%s), losses=%v", ag.name, fix.Path, spec.slug, agLosses1)
			require.Empty(t, agDiffs, "%s Q4 Essence diff on %s (%s):\n%s\nlosses=%v", ag.name, fix.Path, spec.slug, testkit.FormatJSONDiff(agDiffs), agLosses1)

			// Law Q5: Repeated Antigravity conversions produce byte-identical output and losses.
			exAG2, agErr := NewExchange(fix.Data, Options{
				Route:       agRoute,
				ClientModel: spec.clientMdl,
				Runtime:     newDeterministicRuntime(),
			})
			require.NoError(t, agErr)
			upAG2, agErr := exAG2.UpstreamRequest()
			require.NoError(t, agErr)
			require.True(t, bytes.Equal(upAG1, upAG2), "%s Q5 byte non-determinism on %s (%s)", ag.name, fix.Path, spec.slug)
			require.Equal(t, agLosses1, exAG2.Losses())
		}

	case protocol.ProtocolQoderChat:
		srcIR, err := DecodeRequest(spec.route.Client, fix.Data)
		require.NoError(t, err)

		qRoute := spec.route
		qRoute.Dialect.Name = "qoder-stripped"
		qRoute.Dialect.AllowBillingHeader = false
		qRoute.Dialect.StripEffort = true

		exQ1, err := NewExchange(fix.Data, Options{
			Route:       qRoute,
			ClientModel: spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upQ1, err := exQ1.UpstreamRequest()
		require.NoError(t, err)
		qLosses1 := exQ1.Losses()
		for _, l := range qLosses1 {
			require.True(t, l.Kind.IsValid(), "invalid LossKind %q on qoder-stripped", l.Kind)
		}

		vQ := validateUpstreamRequestInvariants(
			protocol.ProtocolQoderChat,
			upQ1,
			spec.validation,
			spec.route.Client == protocol.ProtocolOpenAIResponses,
		)
		require.Empty(t, vQ, "qoder-stripped Q3 violations on %s (%s): %v", fix.Path, spec.slug, vQ)
		require.NotContains(t, string(upQ1), "x-anthropic-billing-header:")

		dstQIR, err := decodeTargetRequestForQ4(protocol.ProtocolQoderChat, upQ1)
		require.NoError(t, err)
		qDiffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstQIR), qLosses1)
		require.NoError(t, err, "qoder-stripped Q4 VerifyEssence error on %s (%s), losses=%v", fix.Path, spec.slug, qLosses1)
		require.Empty(t, qDiffs, "qoder-stripped Q4 Essence diff on %s (%s):\n%s\nlosses=%v", fix.Path, spec.slug, testkit.FormatJSONDiff(qDiffs), qLosses1)

		exQ2, err := NewExchange(fix.Data, Options{
			Route:       qRoute,
			ClientModel: spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upQ2, err := exQ2.UpstreamRequest()
		require.NoError(t, err)
		require.True(t, bytes.Equal(upQ1, upQ2), "qoder-stripped Q5 byte non-determinism on %s (%s)", fix.Path, spec.slug)
		require.Equal(t, qLosses1, exQ2.Losses())
	}
}

// collectRouteStreamLosses collects deterministic stream conversion losses for golden/loss/<slug>.json.
func collectRouteStreamLosses(t *testing.T, spec routeSpec) []lossReportEntry {
	t.Helper()
	wireStreams := upstreamGoldenStreamFixtures(t, spec.route.Upstream)
	clientWireReqs, err := testkit.WireFixtures(spec.route.Client, testkit.FixtureRequest)
	require.NoError(t, err)
	reqByScenario := make(map[string][]byte, len(clientWireReqs))
	for _, cf := range clientWireReqs {
		reqByScenario[cf.Scenario] = cf.Data
	}

	entries := make([]lossReportEntry, 0, len(wireStreams))
	for _, sFix := range wireStreams {
		if sFix.Scenario == "stream_truncated" || sFix.Scenario == "stream_error_event" {
			continue
		}
		reqBytes := selectClientRequestForStream(spec.route.Client, sFix.Scenario, reqByScenario)
		ex, err := NewExchange(reqBytes, Options{
			Route:       spec.route,
			ClientModel: spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		_, err = ex.UpstreamRequest()
		require.NoError(t, err)
		frames, err := sFix.Frames()
		require.NoError(t, err)
		st := ex.Stream()
		for _, f := range frames {
			_, err = st.Write(f)
			require.NoError(t, err)
		}
		_, err = st.Close(nil)
		require.NoError(t, err)
		entries = append(entries, lossReportEntry{
			Fixture: sFix.Path,
			Kind:    "stream",
			Losses:  st.Losses(),
		})
	}
	return entries
}

// selectClientRequestForStream selects the client wire request JSON matching
// scenario from byScenario, remapping Qoder text-encoded and envelope get_weather
// streams to tool_single when no dedicated request file exists. Missing
// scenarios fall back to minimalClientRequest, and Chat Completions requests
// always enable stream_options.include_usage so usage tail chunks emit.
func selectClientRequestForStream(
	clientProto protocol.ProtocolID,
	scenario string,
	byScenario map[string][]byte,
) []byte {
	lookupScenario := scenario
	switch scenario {
	case "tool_xml_text", "tool_json_text", "tool_dsml_text", "anthropic_envelope":
		if _, ok := byScenario[scenario]; !ok {
			lookupScenario = "tool_single"
		}
	}
	if raw, ok := byScenario[lookupScenario]; ok {
		if clientProto == protocol.ProtocolOpenAIChatCompletions {
			// Ensure include_usage is true when testing response stream laws so Chat
			// emits its usage tail chunk consistently across S4, S6, and S8.
			if members, err := parseOrderedObject(raw); err == nil {
				if _, hasSO := findRawMember(members, "stream_options"); !hasSO {
					members = setRawMember(members, "stream_options", jsontext.Value(`{"include_usage":true}`))
					if enc, encErr := encodeOrderedObject(members); encErr == nil {
						return enc
					}
				}
			}
		}
		return raw
	}
	return minimalClientRequest(clientProto, true)
}

// Read returns at most r.step bytes from r.data, returning io.EOF when exhausted.
func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	n := min(len(p), r.step, len(r.data)-r.offset)
	copy(p, r.data[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}

// verifyLawS5ChunkAndDeltaInvariance checks 1-byte and 7-byte SSE chunking plus
// adjacent text delta merging and single-rune splitting.
func verifyLawS5ChunkAndDeltaInvariance(
	t *testing.T,
	spec routeSpec,
	clientReqBytes []byte,
	rawSSE []byte,
	wantResp *ir.Response,
) {
	t.Helper()
	for _, step := range []int{1, 7} {
		rd := sse.NewReader(&chunkedReader{data: rawSSE, step: step})
		ex, err := NewExchange(clientReqBytes, Options{
			Route:       spec.route,
			ClientModel: spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		st := ex.Stream()
		var outFrames []sse.Frame
		for rd.Scan() {
			batch, wErr := st.Write(rd.Frame())
			require.NoError(t, wErr)
			outFrames = append(outFrames, batch...)
		}
		require.NoError(t, rd.Err())
		tail, cErr := st.Close(nil)
		require.NoError(t, cErr)
		outFrames = append(outFrames, tail...)

		_, gotResp := decodeRawStreamToResponse(t, spec.route.Client, formatSSEFrames(outFrames))
		require.Equal(t, ir.ResponseEssence(wantResp), ir.ResponseEssence(gotResp))
		require.Equal(t, wantResp.Finish, gotResp.Finish)
		require.Equal(t, wantResp.Usage, gotResp.Usage)
	}

	// Also verify merging and splitting adjacent text deltas before client StreamEncoder.
	rawEvents, _ := decodeRawStreamToResponse(t, spec.route.Upstream, rawSSE)
	mergedEvents := mergeAdjacentTextDeltas(rawEvents)
	splitEvents := splitTextDeltasByRune(rawEvents)

	encMerged := encodeTransformedEventSlice(t, spec, clientReqBytes, mergedEvents)
	encSplit := encodeTransformedEventSlice(t, spec, clientReqBytes, splitEvents)
	require.Equal(t, ir.ResponseEssence(wantResp), ir.ResponseEssence(encMerged))
	require.Equal(t, ir.ResponseEssence(wantResp), ir.ResponseEssence(encSplit))
}

// mergeAdjacentTextDeltas coalesces consecutive EventPartDelta.Text events on the same Index.
func mergeAdjacentTextDeltas(events []ir.Event) []ir.Event {
	var out []ir.Event
	for _, ev := range events {
		cp := ev.Clone()
		if cp.Kind == ir.EventPartDelta && cp.Delta != nil && cp.Delta.Text != "" && cp.Delta.Arguments == "" &&
			len(out) > 0 {
			prev := &out[len(out)-1]
			if prev.Kind == ir.EventPartDelta && prev.Index == cp.Index && prev.Delta != nil && prev.Delta.Arguments == "" {
				prev.Delta.Text += cp.Delta.Text
				continue
			}
		}
		out = append(out, cp)
	}
	return out
}

// splitTextDeltasByRune splits each EventPartDelta.Text into individual UTF-8 rune deltas.
func splitTextDeltasByRune(events []ir.Event) []ir.Event {
	var out []ir.Event
	for _, ev := range events {
		if ev.Kind == ir.EventPartDelta && ev.Delta != nil && ev.Delta.Text != "" && ev.Delta.Arguments == "" {
			for _, r := range ev.Delta.Text {
				out = append(out, ir.Event{
					Kind:  ir.EventPartDelta,
					Index: ev.Index,
					Delta: &ir.Delta{Text: string(r)},
				})
			}
			continue
		}
		out = append(out, ev.Clone())
	}
	return out
}

// encodeTransformedEventSlice runs events through convert's response pipeline and decodes the client stream.
func encodeTransformedEventSlice(
	t *testing.T,
	spec routeSpec,
	clientReqBytes []byte,
	events []ir.Event,
) *ir.Response {
	t.Helper()
	ex, err := NewExchange(clientReqBytes, Options{
		Route:       spec.route,
		ClientModel: spec.clientMdl,
		Runtime:     newDeterministicRuntime(),
	})
	require.NoError(t, err)
	st := ex.Stream()
	frames, err := st.processEvents(events)
	require.NoError(t, err)
	_, resp := decodeRawStreamToResponse(t, spec.route.Client, formatSSEFrames(frames))
	return resp
}

// verifyLawS6StreamBufferResponseParity compares Stream, Buffer, and buffered
// Response conversions on paired (stream, response) fixtures.
func verifyLawS6StreamBufferResponseParity(
	t *testing.T,
	spec routeSpec,
	clientReqBytes []byte,
	sFix, rFix testkit.Fixture,
	streamFrames []sse.Frame,
	streamDecodedResp *ir.Response,
) {
	t.Helper()
	rawFrames, err := sFix.Frames()
	require.NoError(t, err)

	exBuf, err := NewExchange(clientReqBytes, Options{
		Route:       spec.route,
		ClientModel: spec.clientMdl,
		Runtime:     newDeterministicRuntime(),
	})
	require.NoError(t, err)
	buf := exBuf.Buffer()
	for _, f := range rawFrames {
		require.NoError(t, buf.Write(f))
	}
	bufBytes, err := buf.Close(nil)
	require.NoError(t, err)

	exResp, err := NewExchange(clientReqBytes, Options{
		Route:       spec.route,
		ClientModel: spec.clientMdl,
		Runtime:     newDeterministicRuntime(),
	})
	require.NoError(t, err)
	respBytes, err := exResp.Response(rFix.Data)
	require.NoError(t, err)

	bufDecoded, err := DecodeResponse(spec.route.Client, bufBytes)
	require.NoError(t, err)
	respDecoded, err := DecodeResponse(spec.route.Client, respBytes)
	require.NoError(t, err)

	require.Equal(t, ir.ResponseEssence(streamDecodedResp), ir.ResponseEssence(bufDecoded))
	require.Equal(t, ir.ResponseEssence(streamDecodedResp), ir.ResponseEssence(respDecoded))
	require.Equal(t, streamDecodedResp.Finish, bufDecoded.Finish)
	require.Equal(t, streamDecodedResp.Finish, respDecoded.Finish)
	require.Equal(t, streamDecodedResp.Usage, bufDecoded.Usage)
	require.Equal(t, streamDecodedResp.Usage, respDecoded.Usage)

	if sFix.Scenario == "empty_response" {
		return
	}
	nextFromStream, err := testkit.SimulateNextTurn(spec.route.Client, testkit.TurnInput{
		PrevRequest:              clientReqBytes,
		ResponseFrames:           streamFrames,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "follow up",
	})
	require.NoError(t, err)
	nextFromBuf, err := testkit.SimulateNextTurn(spec.route.Client, testkit.TurnInput{
		PrevRequest:              clientReqBytes,
		ResponseBody:             bufBytes,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "follow up",
	})
	require.NoError(t, err)

	reqFromStream, err := DecodeRequest(spec.route.Client, nextFromStream)
	require.NoError(t, err)
	reqFromBuf, err := DecodeRequest(spec.route.Client, nextFromBuf)
	require.NoError(t, err)
	require.Equal(t, ir.Essence(reqFromBuf), ir.Essence(reqFromStream))

	normFromBuf := canonicalizeStringifiedToolArgs(nextFromBuf)
	normFromStream := canonicalizeStringifiedToolArgs(nextFromStream)
	simDiffs, err := testkit.JSONDiff(normFromBuf, normFromStream)
	require.NoError(t, err)
	require.Empty(t, simDiffs, "Law S6 SimulateNextTurn diff on %s (%s):\n%s", sFix.Name, spec.slug, testkit.FormatJSONDiff(simDiffs))
}

// canonicalizeStringifiedToolArgs normalizes JSON-encoded string literals stored
// under object "arguments" keys so incremental stream escape sequences (\uXXXX)
// and buffered compacted UTF-8 tool arguments compare by decoded JSON structure.
func canonicalizeStringifiedToolArgs(raw []byte) []byte {
	var root any
	if err := json.Unmarshal(raw, &root); err != nil {
		return raw
	}
	canonicalizeArgumentsInNode(root)
	out, err := json.Marshal(root, json.Deterministic(true))
	if err != nil {
		return raw
	}
	return out
}

// canonicalizeArgumentsInNode walks decoded JSON maps and slices in place,
// canonicalizing valid JSON strings found at map key "arguments".
func canonicalizeArgumentsInNode(node any) {
	switch v := node.(type) {
	case map[string]any:
		for k, child := range v {
			if k == "arguments" {
				if str, ok := child.(string); ok {
					var parsed any
					if err := json.Unmarshal([]byte(str), &parsed); err == nil {
						if canon, mErr := json.Marshal(parsed, json.Deterministic(true)); mErr == nil {
							v[k] = string(canon)
							continue
						}
					}
				}
			}
			canonicalizeArgumentsInNode(child)
		}
	case []any:
		for _, elem := range v {
			canonicalizeArgumentsInNode(elem)
		}
	}
}

// verifyLawS7PrefixTruncations cuts rawFrames at every prefix 0..len(rawFrames)-1
// and checks that Close(err) never panics and passes AllowAbnormal grammar.
func verifyLawS7PrefixTruncations(
	t *testing.T,
	spec routeSpec,
	clientReqBytes []byte,
	rawFrames []sse.Frame,
) {
	t.Helper()
	abortErr := errors.New("simulated upstream socket disconnect")
	for cut := range rawFrames {
		ex, err := NewExchange(clientReqBytes, Options{
			Route:       spec.route,
			ClientModel: spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		st := ex.Stream()
		var out []sse.Frame
		closeCause := abortErr
		for _, f := range rawFrames[:cut] {
			batch, wErr := st.Write(f)
			if wErr != nil {
				closeCause = wErr
				break
			}
			out = append(out, batch...)
		}
		tail, cErr := st.Close(closeCause)
		require.NoError(t, cErr)
		out = append(out, tail...)

		violations := testkit.ValidateStreamGrammar(spec.route.Client, out, testkit.GrammarOptions{
			AllowAbnormal: true,
		})
		require.Empty(t, violations, "Law S7 cut=%d violations on %s: %v", cut, spec.slug, violations)
	}
}

// verifyLawS8UsageBuckets checks that client-visible usage matches section 3.3's
// disjoint bucket arithmetic without double-counting cached or cache-write tokens.
func verifyLawS8UsageBuckets(
	t *testing.T,
	spec routeSpec,
	clientReqBytes []byte,
	rawFrames []sse.Frame,
	srcUsage, dstUsage *ir.Usage,
) {
	t.Helper()
	if srcUsage == nil {
		return
	}
	require.NotNil(t, dstUsage)
	require.Equal(t, srcUsage.Input, dstUsage.Input)
	require.Equal(t, srcUsage.CacheRead, dstUsage.CacheRead)
	require.Equal(t, srcUsage.CacheWrite, dstUsage.CacheWrite)
	require.Equal(t, srcUsage.Output, dstUsage.Output)

	// Verify wire-level JSON fields on buffered output to confirm section 3.3 totals directly.
	ex, err := NewExchange(clientReqBytes, Options{
		Route:       spec.route,
		ClientModel: spec.clientMdl,
		Runtime:     newDeterministicRuntime(),
	})
	require.NoError(t, err)
	buf := ex.Buffer()
	for _, f := range rawFrames {
		require.NoError(t, buf.Write(f))
	}
	rawJSON, err := buf.Close(nil)
	require.NoError(t, err)

	var wireEnv struct {
		Usage *struct {
			InputTokens              int64 `json:"input_tokens"`
			CacheReadInputTokens     int64 `json:"cache_read_input_tokens"`
			CacheCreationInputTokens int64 `json:"cache_creation_input_tokens"`
			OutputTokens             int64 `json:"output_tokens"`
			PromptTokens             int64 `json:"prompt_tokens"`
			CompletionTokens         int64 `json:"completion_tokens"`
			TotalTokens              int64 `json:"total_tokens"`
			InputTokensDetails       *struct {
				CachedTokens     int64 `json:"cached_tokens"`
				CacheWriteTokens int64 `json:"cache_write_tokens"`
			} `json:"input_tokens_details"`
			PromptTokensDetails *struct {
				CachedTokens     int64 `json:"cached_tokens"`
				CacheWriteTokens int64 `json:"cache_write_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	require.NoError(t, json.Unmarshal(rawJSON, &wireEnv))
	require.NotNil(t, wireEnv.Usage)

	wantCombinedInput := srcUsage.Input + srcUsage.CacheRead + srcUsage.CacheWrite
	switch spec.route.Client {
	case protocol.ProtocolAnthropicMessages:
		require.Equal(t, srcUsage.Input, wireEnv.Usage.InputTokens)
		require.Equal(t, srcUsage.CacheRead, wireEnv.Usage.CacheReadInputTokens)
		require.Equal(t, srcUsage.CacheWrite, wireEnv.Usage.CacheCreationInputTokens)
		require.Equal(t, srcUsage.Output, wireEnv.Usage.OutputTokens)

	case protocol.ProtocolOpenAIResponses:
		require.Equal(t, wantCombinedInput, wireEnv.Usage.InputTokens)
		require.Equal(t, srcUsage.Output, wireEnv.Usage.OutputTokens)
		require.Equal(t, wantCombinedInput+srcUsage.Output, wireEnv.Usage.TotalTokens)
		require.NotNil(t, wireEnv.Usage.InputTokensDetails)
		require.Equal(t, srcUsage.CacheRead, wireEnv.Usage.InputTokensDetails.CachedTokens)

	case protocol.ProtocolOpenAIChatCompletions:
		require.Equal(t, wantCombinedInput, wireEnv.Usage.PromptTokens)
		require.Equal(t, srcUsage.Output, wireEnv.Usage.CompletionTokens)
		require.Equal(t, wantCombinedInput+srcUsage.Output, wireEnv.Usage.TotalTokens)
		if srcUsage.CacheRead > 0 {
			require.NotNil(t, wireEnv.Usage.PromptTokensDetails)
			require.Equal(t, srcUsage.CacheRead, wireEnv.Usage.PromptTokensDetails.CachedTokens)
		}
	}
}

// collectUnmodeledPaths enumerates all Extra, OpaqueItem, and Hosted paths inside req.
func collectUnmodeledPaths(req *ir.Request) []string {
	var paths []string
	appendExtraKeys(&paths, "$", req.Extra)

	for i, p := range req.System {
		collectPartUnmodeled(&paths, fmt.Sprintf("$.system[%d]", i), p)
	}
	for m, msg := range req.Messages {
		mPath := fmt.Sprintf("$.messages[%d]", m)
		appendExtraKeys(&paths, mPath, msg.Extra)
		for p, part := range msg.Parts {
			collectPartUnmodeled(&paths, fmt.Sprintf("%s.parts[%d]", mPath, p), part)
		}
	}
	for i, t := range req.Tools {
		tPath := fmt.Sprintf("$.tools[%d]", i)
		appendExtraKeys(&paths, tPath, t.Extra)
		if t.Cache != nil {
			appendExtraKeys(&paths, tPath+".cache", t.Cache.Extra)
		}
		if t.Hosted != nil {
			paths = append(paths, fmt.Sprintf("hosted_tool\t%s (%s:%s)", tPath, t.Hosted.Origin, t.Hosted.Type))
		}
	}
	for i, ns := range req.Namespaces {
		nsPath := fmt.Sprintf("$.namespaces[%d]", i)
		appendExtraKeys(&paths, nsPath, ns.Extra)
		if ns.Cache != nil {
			appendExtraKeys(&paths, nsPath+".cache", ns.Cache.Extra)
		}
	}
	slices.Sort(paths)
	return slices.Compact(paths)
}

// collectPartUnmodeled records Extra and OpaqueItem entries inside p and nested tool_result Content.
func collectPartUnmodeled(paths *[]string, basePath string, p ir.Part) {
	appendExtraKeys(paths, basePath, p.Extra)
	if p.Cache != nil {
		appendExtraKeys(paths, basePath+".cache", p.Cache.Extra)
	}
	for i, ann := range p.Annotations {
		appendExtraKeys(paths, fmt.Sprintf("%s.annotations[%d]", basePath, i), ann.Extra)
	}
	if p.Opaque != nil {
		*paths = append(*paths, fmt.Sprintf("opaque\t%s (%s:%s)", basePath, p.Opaque.Origin, p.Opaque.Type))
	}
	if p.Result != nil {
		for i, child := range p.Result.Content {
			collectPartUnmodeled(paths, fmt.Sprintf("%s.result.content[%d]", basePath, i), child)
		}
	}
}

// appendExtraKeys expands Extra member keys (and one level of nested sub-object keys) onto paths.
func appendExtraKeys(paths *[]string, basePath string, extra ir.Extra) {
	if extra.IsZero() {
		return
	}
	for key, rawVal := range extra.Members {
		full := basePath + "." + key
		if basePath == "$" && len(rawVal) > 0 && rawVal.Kind() == '{' {
			if subMembers, err := parseOrderedObject(rawVal); err == nil && len(subMembers) > 0 {
				for _, sm := range subMembers {
					*paths = append(*paths, fmt.Sprintf("extra\t%s.%s", full, sm.name))
				}
				continue
			}
		}
		*paths = append(*paths, fmt.Sprintf("extra\t%s", full))
	}
}

// normalizeCensusSummaryPath replaces concrete array indices [0], [12] with [*] for summary aggregation.
func normalizeCensusSummaryPath(line string) string {
	var b strings.Builder
	for i := 0; i < len(line); i++ {
		if line[i] == '[' {
			end := strings.IndexByte(line[i:], ']')
			if end > 1 {
				allDigits := true
				for k := i + 1; k < i+end; k++ {
					if line[k] < '0' || line[k] > '9' {
						allDigits = false
						break
					}
				}
				if allDigits {
					_, _ = b.WriteString("[*]")
					i += end
					continue
				}
			}
		}
		_ = b.WriteByte(line[i])
	}
	return b.String()
}

// TestQoderStatelessAndCursorRequestInvariants feeds Cursor input-shaped Chat
// Completions payloads and stateful or unpaired Responses requests into NewExchange
// on Qoder routes. It verifies that Cursor Chat payloads satisfy Laws Q3, Q4,
// and Q5 while stateful or unpaired Responses inputs return typed
// *protocolqoder.RequestError codes.
func TestQoderStatelessAndCursorRequestInvariants(t *testing.T) {
	cursorChatBody := []byte(`{
		"model":"auto",
		"stream":true,
		"input":[
			{"type":"message","role":"system","content":"Be concise."},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"Explain Qoder routing."}]}
		]
	}`)
	chatSpec := stage6QoderRoutes[2]
	ex1, err := NewExchange(cursorChatBody, Options{
		Route:       chatSpec.route,
		ClientModel: chatSpec.clientMdl,
		Runtime:     newDeterministicRuntime(),
	})
	require.NoError(t, err)
	up1, err := ex1.UpstreamRequest()
	require.NoError(t, err)
	require.Empty(t, validateUpstreamRequestInvariants(protocol.ProtocolQoderChat, up1, chatSpec.validation, true))

	srcIR, err := DecodeRequest(protocol.ProtocolOpenAIResponses, cursorChatBody)
	require.NoError(t, err)
	dstIR, err := decodeTargetRequestForQ4(protocol.ProtocolQoderChat, up1)
	require.NoError(t, err)
	essDiffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex1.Losses())
	require.NoError(t, err)
	require.Empty(t, essDiffs)

	ex2, err := NewExchange(cursorChatBody, Options{
		Route:       chatSpec.route,
		ClientModel: chatSpec.clientMdl,
		Runtime:     newDeterministicRuntime(),
	})
	require.NoError(t, err)
	up2, err := ex2.UpstreamRequest()
	require.NoError(t, err)
	require.True(t, bytes.Equal(up1, up2))

	respSpec := stage6QoderRoutes[1]
	for _, tc := range []struct {
		name     string
		body     string
		wantCode string
	}{
		{
			name:     "store=true rejected",
			body:     `{"model":"gpt-5","store":true,"input":"hello"}`,
			wantCode: protocolqoder.CodeUnsupportedResponseState,
		},
		{
			name:     "previous_response_id rejected",
			body:     `{"model":"gpt-5","previous_response_id":"resp_prev","input":"hello"}`,
			wantCode: protocolqoder.CodeUnsupportedResponseState,
		},
		{
			name:     "conversation string rejected",
			body:     `{"model":"gpt-5","conversation":"conv_prev","input":"hello"}`,
			wantCode: protocolqoder.CodeUnsupportedResponseState,
		},
		{
			name:     "item_reference rejected",
			body:     `{"model":"gpt-5","input":[{"type":"item_reference","id":"msg_prev"}]}`,
			wantCode: protocolqoder.CodeUnsupportedResponseState,
		},
		{
			name:     "unpaired function_call_output rejected",
			body:     `{"model":"gpt-5","input":[{"type":"function_call_output","call_id":"call_missing","output":"ok"}]}`,
			wantCode: protocolqoder.CodeMissingToolCall,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ex, exErr := NewExchange([]byte(tc.body), Options{
				Route:       respSpec.route,
				ClientModel: respSpec.clientMdl,
				Runtime:     newDeterministicRuntime(),
			})
			if exErr == nil {
				_, exErr = ex.UpstreamRequest()
			}
			var qErr *protocolqoder.RequestError
			require.ErrorAs(t, exErr, &qErr)
			require.Equal(t, tc.wantCode, qErr.Code)
		})
	}
}

// TestStage7OffTableAndDialectLaws verifies Laws Q3, Q4, Q5, and S3 across the
// Grok xai OmitStoreAndInclude Responses dialect, the DeepSeek HistoryToolTurns
// Chat dialect, Cursor input-shaped Chat Completions requests across all target
// protocols, and Gemini count_tokens preflight conversion. Every converted
// request must pass strict target dialect validation, reconcile Essence losses,
// and remain byte-deterministic across repeated conversions.
func TestStage7OffTableAndDialectLaws(t *testing.T) {
	t.Run("grok_xai_omit_store_and_include_responses_dialect", func(t *testing.T) {
		grokDialect := Dialect{
			Name:                "grok-xai",
			Tools:               ToolSupportFunction,
			ToolNameMaxBytes:    64,
			ReplayScheme:        ir.SchemeXAI,
			ReasoningHistory:    HistoryNone,
			OmitStoreAndInclude: true,
		}
		for _, srcProto := range []protocol.ProtocolID{
			protocol.ProtocolAnthropicMessages,
			protocol.ProtocolOpenAIChatCompletions,
		} {
			t.Run(string(srcProto), func(t *testing.T) {
				reqs := allProtocolRequestFixtures(t, srcProto)
				for _, fix := range reqs {
					route := Route{
						Client:   srcProto,
						Upstream: protocol.ProtocolOpenAIResponses,
						Dialect:  grokDialect,
					}
					ex1, err := NewExchange(fix.Data, Options{
						Route:       route,
						ClientModel: "grok-4.5",
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)
					up1, err := ex1.UpstreamRequest()
					require.NoError(t, err)

					violations := testkit.ValidateResponsesRequest(up1, testkit.RequestValidationOptions{
						DialectRules: []string{"function-only"},
					})
					require.Empty(t, violations, "grok Q3 violations on %s: %v", fix.Path, violations)
					srcIR, err := DecodeRequest(srcProto, fix.Data)
					require.NoError(t, err)
					if srcIR.Store == nil {
						require.False(t, bytes.Contains(up1, []byte(`"store":`)), "grok request must omit default store on %s", fix.Path)
					}
					require.False(t, bytes.Contains(up1, []byte(`"include":`)), "grok request must omit include on %s", fix.Path)

					dstIR, err := DecodeRequest(protocol.ProtocolOpenAIResponses, up1)
					require.NoError(t, err)
					diffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex1.Losses())
					require.NoError(t, err)
					require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

					ex2, err := NewExchange(fix.Data, Options{
						Route:       route,
						ClientModel: "grok-4.5",
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)
					up2, err := ex2.UpstreamRequest()
					require.NoError(t, err)
					require.True(t, bytes.Equal(up1, up2))
				}
			})
		}
	})

	t.Run("deepseek_history_tool_turns_chat_dialect", func(t *testing.T) {
		deepseekDialect := Dialect{
			Name:             "deepseek-reasoner",
			Tools:            ToolSupportFunction,
			ToolNameMaxBytes: 64,
			ReplayScheme:     ir.SchemePlain,
			ReasoningHistory: HistoryToolTurns,
		}
		sawCodexSplitCommentary := false
		for _, srcProto := range []protocol.ProtocolID{
			protocol.ProtocolAnthropicMessages,
			protocol.ProtocolOpenAIResponses,
		} {
			t.Run(string(srcProto), func(t *testing.T) {
				reqs := allProtocolRequestFixtures(t, srcProto)
				for _, fix := range reqs {
					t.Run(fix.Path, func(t *testing.T) {
						route := Route{
							Client:   srcProto,
							Upstream: protocol.ProtocolOpenAIChatCompletions,
							Dialect:  deepseekDialect,
						}
						ex1, err := NewExchange(fix.Data, Options{
							Route:       route,
							ClientModel: "deepseek-reasoner",
							Runtime:     newDeterministicRuntime(),
						})
						require.NoError(t, err)
						up1, err := ex1.UpstreamRequest()
						require.NoError(t, err)

						violations := testkit.ValidateChatRequest(up1, testkit.RequestValidationOptions{
							DialectRules: []string{"deepseek-thinking"},
						})
						require.Empty(t, violations, "deepseek-thinking Q3 violations on %s: %v\nup=%s", fix.Path, violations, string(up1))

						srcIR, err := DecodeRequest(srcProto, fix.Data)
						require.NoError(t, err)
						dstIR, err := DecodeRequest(protocol.ProtocolOpenAIChatCompletions, up1)
						require.NoError(t, err)
						diffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex1.Losses())
						require.NoError(t, err, "deepseek-thinking Q4 error on %s", fix.Path)
						require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

						ex2, err := NewExchange(fix.Data, Options{
							Route:       route,
							ClientModel: "deepseek-reasoner",
							Runtime:     newDeterministicRuntime(),
						})
						require.NoError(t, err)
						up2, err := ex2.UpstreamRequest()
						require.NoError(t, err)
						require.True(t, bytes.Equal(up1, up2))

						if fix.Path == "corpus/clients/codex/0.171.0/lab_reasoning_tool/2.request.json" {
							sawCodexSplitCommentary = true
							// Assert group5_codex_to_deepseek_chat (ReplayScheme="", HistoryToolTurns)
							// produces byte-identical output to explicit ReplayScheme=SchemePlain and
							// coalesces trc1:plain reasoning + commentary + function_call onto one assistant turn.
							liveCanaryRoute := Route{
								Client:   protocol.ProtocolOpenAIResponses,
								Upstream: protocol.ProtocolOpenAIChatCompletions,
								Dialect: Dialect{
									Name:             "deepseek",
									Tools:            ToolSupportFunction,
									ToolNameMaxBytes: 64,
									ReasoningHistory: HistoryToolTurns,
								},
							}
							exCanary, cErr := NewExchange(fix.Data, Options{
								Route:       liveCanaryRoute,
								ClientModel: "deepseek-reasoner",
								Runtime:     newDeterministicRuntime(),
							})
							require.NoError(t, cErr)
							upCanary, cErr := exCanary.UpstreamRequest()
							require.NoError(t, cErr)
							require.True(t, bytes.Equal(up1, upCanary), "group5_codex_to_deepseek_chat output must match byte-for-byte")
							require.Contains(t, string(upCanary), `"reasoning_content":"Plan shell command to write result.txt."`)
							require.Contains(t, string(upCanary), `"content":"Writing marker to result.txt now."`)
						}
					})
				}
			})
		}
		require.True(t, sawCodexSplitCommentary, "corpus/clients/codex/0.171.0/lab_reasoning_tool/2.request.json must be exercised")
	})

	t.Run("cursor_chat_responses_input_across_target_protocols", func(t *testing.T) {
		cursorBody := []byte(`{
			"model":"gpt-4o",
			"stream":true,
			"instructions":"Answer concisely.",
			"tools":[{"type":"function","name":"lookup_doc","description":"Find doc","parameters":{"type":"object","properties":{"topic":{"type":"string"}}}}],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"Find migration notes"}]},
				{"type":"function_call","id":"fc_cur_1","call_id":"call_cur_1","name":"lookup_doc","arguments":"{\"topic\":\"ir\"}"},
				{"type":"function_call_output","call_id":"call_cur_1","output":"Stage 7 ready"}
			]
		}`)

		// targetCase binds one target upstream protocol to its dialect and validator options.
		type targetCase struct {
			name       string                           // subtest case display name
			upstream   protocol.ProtocolID              // target upstream protocol
			dialect    Dialect                          // target dialect capabilities
			validation testkit.RequestValidationOptions // strict validator options
			streamSSE  string                           // sample upstream SSE stream for S3 check
		}
		targets := []targetCase{
			{
				name:     "to_openai_responses",
				upstream: protocol.ProtocolOpenAIResponses,
				dialect: Dialect{
					Name:             "openai-responses",
					Tools:            ToolSupportAllResponses,
					ToolNameMaxBytes: 64,
					ReplayScheme:     ir.SchemeOpenAI,
				},
				validation: testkit.RequestValidationOptions{},
				streamSSE: strings.Join([]string{
					`event: response.created`,
					`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_cur","object":"response","created_at":1760000000,"status":"in_progress","model":"gpt-5.4","output":[]}}`,
					``,
					`event: response.output_item.added`,
					`data: {"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"msg_cur","type":"message","status":"in_progress","role":"assistant","content":[]}}`,
					``,
					`event: response.content_part.added`,
					`data: {"type":"response.content_part.added","sequence_number":2,"item_id":"msg_cur","output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"text":""}}`,
					``,
					`event: response.output_text.delta`,
					`data: {"type":"response.output_text.delta","sequence_number":3,"item_id":"msg_cur","output_index":0,"content_index":0,"delta":"Cursor OK"}`,
					``,
					`event: response.output_text.done`,
					`data: {"type":"response.output_text.done","sequence_number":4,"item_id":"msg_cur","output_index":0,"content_index":0,"text":"Cursor OK"}`,
					``,
					`event: response.content_part.done`,
					`data: {"type":"response.content_part.done","sequence_number":5,"item_id":"msg_cur","output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"text":"Cursor OK"}}`,
					``,
					`event: response.output_item.done`,
					`data: {"type":"response.output_item.done","sequence_number":6,"output_index":0,"item":{"id":"msg_cur","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","annotations":[],"text":"Cursor OK"}]}}`,
					``,
					`event: response.completed`,
					`data: {"type":"response.completed","sequence_number":7,"response":{"id":"resp_cur","object":"response","created_at":1760000000,"status":"completed","model":"gpt-5.4","output":[{"id":"msg_cur","type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","annotations":[],"text":"Cursor OK"}]}],"usage":{"input_tokens":12,"output_tokens":3,"total_tokens":15}}}`,
					``,
					``,
				}, "\n"),
			},
			{
				name:     "to_openai_chat_completions",
				upstream: protocol.ProtocolOpenAIChatCompletions,
				dialect: Dialect{
					Name:             "openai-chat",
					Tools:            ToolSupportFunction,
					ToolNameMaxBytes: 64,
					ReplayScheme:     ir.SchemePlain,
				},
				validation: testkit.RequestValidationOptions{},
				streamSSE: strings.Join([]string{
					`data: {"id":"chatcmpl_cur","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":"Cursor OK"},"finish_reason":null}]}`,
					``,
					`data: {"id":"chatcmpl_cur","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
					``,
					`data: [DONE]`,
					``,
					``,
				}, "\n"),
			},
			{
				name:     "to_anthropic_messages",
				upstream: protocol.ProtocolAnthropicMessages,
				dialect: Dialect{
					Name:             "anthropic-messages",
					Tools:            ToolSupportFunction,
					ToolNameMaxBytes: 64,
					ReplayScheme:     ir.SchemeAnthropic,
				},
				validation: testkit.RequestValidationOptions{StrictTopLevel: true},
				streamSSE: strings.Join([]string{
					`event: message_start`,
					`data: {"type":"message_start","message":{"id":"msg_cur","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":12,"output_tokens":1}}}`,
					``,
					`event: content_block_start`,
					`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
					``,
					`event: content_block_delta`,
					`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Cursor OK"}}`,
					``,
					`event: content_block_stop`,
					`data: {"type":"content_block_stop","index":0}`,
					``,
					`event: message_delta`,
					`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":3}}`,
					``,
					`event: message_stop`,
					`data: {"type":"message_stop"}`,
					``,
					``,
				}, "\n"),
			},
			{
				name:     "to_gemini_generate_content",
				upstream: protocol.ProtocolGeminiGenerateContent,
				dialect: Dialect{
					Name:             "gemini",
					Tools:            ToolSupportFunction,
					ToolNameMaxBytes: 64,
					ReplayScheme:     ir.SchemeGemini,
					SchemaDialect:    SchemaGemini,
					PlaceholderToken: geminiPlaceholderSignature,
				},
				validation: testkit.RequestValidationOptions{
					DialectRules: []string{"gemini-signature"},
				},
				streamSSE: "data: {\"responseId\":\"gem_cur\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Cursor OK\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":12,\"candidatesTokenCount\":3,\"totalTokenCount\":15}}\n\n",
			},
		}

		srcIR, err := DecodeRequest(protocol.ProtocolOpenAIResponses, cursorBody)
		require.NoError(t, err)

		for _, tc := range targets {
			t.Run(tc.name, func(t *testing.T) {
				route := Route{
					Client:   protocol.ProtocolOpenAIChatCompletions,
					Upstream: tc.upstream,
					Dialect:  tc.dialect,
				}
				ex1, exErr := NewExchange(cursorBody, Options{
					Route:       route,
					ClientModel: "gpt-4o",
					Runtime:     newDeterministicRuntime(),
				})
				require.NoError(t, exErr)
				up1, exErr := ex1.UpstreamRequest()
				require.NoError(t, exErr)

				violations := validateUpstreamRequestInvariants(tc.upstream, up1, tc.validation, true)
				require.Empty(t, violations, "%s Q3 violations: %v\nup=%s", tc.name, violations, string(up1))

				dstIR, decErr := DecodeRequest(tc.upstream, up1)
				require.NoError(t, decErr)
				diffs, vErr := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex1.Losses())
				require.NoError(t, vErr)
				require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

				ex2, exErr := NewExchange(cursorBody, Options{
					Route:       route,
					ClientModel: "gpt-4o",
					Runtime:     newDeterministicRuntime(),
				})
				require.NoError(t, exErr)
				up2, exErr := ex2.UpstreamRequest()
				require.NoError(t, exErr)
				require.True(t, bytes.Equal(up1, up2))

				stream := ex1.Stream()
				rdr := sse.NewReader(strings.NewReader(tc.streamSSE))
				var outFrames []sse.Frame
				for rdr.Scan() {
					batch, wErr := stream.Write(rdr.Frame())
					require.NoError(t, wErr)
					outFrames = append(outFrames, batch...)
				}
				require.NoError(t, rdr.Err())
				tail, cErr := stream.Close(nil)
				require.NoError(t, cErr)
				outFrames = append(outFrames, tail...)
				require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, outFrames, testkit.GrammarOptions{}))
			})
		}
	})

	t.Run("gemini_count_tokens_preflight_dialect_suppresses_signatures", func(t *testing.T) {
		countDialect := Dialect{
			Name:             "gemini-count-tokens",
			Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
			ToolNameMaxBytes: 64,
			ReplayScheme:     "",
			PlaceholderToken: "",
			SchemaDialect:    SchemaGemini,
		}
		reqs := allProtocolRequestFixtures(t, protocol.ProtocolAnthropicMessages)
		for _, fix := range reqs {
			route := Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  countDialect,
			}
			ex1, err := NewExchange(fix.Data, Options{
				Route:       route,
				ClientModel: "gemini-2.5-pro",
				Runtime:     newDeterministicRuntime(),
			})
			require.NoError(t, err)
			up1, err := ex1.UpstreamRequest()
			require.NoError(t, err)
			require.Empty(t, testkit.ValidateGeminiRequest(up1, testkit.RequestValidationOptions{}))
			require.False(t, bytes.Contains(up1, []byte(`"thoughtSignature"`)), "count_tokens request must omit thoughtSignature on %s", fix.Path)

			srcIR, err := DecodeRequest(protocol.ProtocolAnthropicMessages, fix.Data)
			require.NoError(t, err)
			dstIR, err := DecodeRequest(protocol.ProtocolGeminiGenerateContent, up1)
			require.NoError(t, err)
			diffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex1.Losses())
			require.NoError(t, err)
			require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

			ex2, err := NewExchange(fix.Data, Options{
				Route:       route,
				ClientModel: "gemini-2.5-pro",
				Runtime:     newDeterministicRuntime(),
			})
			require.NoError(t, err)
			up2, err := ex2.UpstreamRequest()
			require.NoError(t, err)
			require.True(t, bytes.Equal(up1, up2))
		}
	})
}

// decodeTargetRequestForQ4 decodes a converted upstream request back into IR
// through DecodeUpstreamRequest for Law Q4 semantic verification. Malformed
// JSON or invalid reconstructed IR structures return a non-nil error.
func decodeTargetRequestForQ4(proto protocol.ProtocolID, upBytes []byte) (*ir.Request, error) {
	return DecodeUpstreamRequest(proto, upBytes)
}

// upstreamStreamAndResponseFixtures returns all stream fixtures and
// scenario-keyed buffered response fixtures for proto. Public protocols combine
// wire and recorded corpus streams with wire responses, whereas
// ProtocolQoderChat returns Qoder's wrapped SSE and buffered JSON fixtures.
func upstreamStreamAndResponseFixtures(
	t *testing.T,
	proto protocol.ProtocolID,
) ([]testkit.Fixture, map[string]testkit.Fixture) {
	t.Helper()
	if proto == protocol.ProtocolQoderChat {
		return qoderUpstreamFixtures()
	}
	wireStreams, err := testkit.WireFixtures(proto, testkit.FixtureStream)
	require.NoError(t, err)
	require.NotEmpty(t, wireStreams)
	corpusStreams, err := testkit.CorpusStreams(proto)
	require.NoError(t, err)
	streams := make([]testkit.Fixture, 0, len(wireStreams)+len(corpusStreams))
	streams = append(streams, wireStreams...)
	streams = append(streams, corpusStreams...)

	wireResps, err := testkit.WireFixtures(proto, testkit.FixtureResponse)
	require.NoError(t, err)
	respByScenario := make(map[string]testkit.Fixture, len(wireResps))
	for _, rf := range wireResps {
		respByScenario[rf.Scenario] = rf
	}
	return streams, respByScenario
}

// verifyNewStreamFlagInvariants asserts per-frame wire (Semantic, Terminal)
// compliance, single-final-Terminal ordering, and mutual exclusion of Semantic
// and Terminal on ctx.newFrames. Abnormal scenarios must end with a protocol
// error frame, while normal streams require Semantic frame presence to match
// ResponseHasSemanticContent(ctx.newResp).
func verifyNewStreamFlagInvariants(t *testing.T, ctx streamEvalContext) {
	t.Helper()
	verifyClientFrameOutputMeaning(t, ctx.spec.route.Client, ctx.newFrames, ctx.fixturePath, ctx.spec.slug)
	require.NotEmpty(t, ctx.newFrames, "new client stream emitted zero frames on %s (%s)", ctx.fixturePath, ctx.spec.slug)
	for i, f := range ctx.newFrames {
		require.False(t, f.Semantic && f.Terminal,
			"frame[%d] cannot set both Semantic=true and Terminal=true on %s (%s)", i, ctx.fixturePath, ctx.spec.slug)
		if i < len(ctx.newFrames)-1 {
			require.False(t, f.Terminal,
				"premature Terminal=true at frame[%d] of %d on %s (%s)", i, len(ctx.newFrames), ctx.fixturePath, ctx.spec.slug)
		} else {
			require.True(t, f.Terminal,
				"final frame[%d] must set Terminal=true on %s (%s)", i, ctx.fixturePath, ctx.spec.slug)
		}
	}
	if ctx.abnormal {
		require.True(t, hasWireErrorFrame(ctx.spec.route.Client, ctx.newFrames),
			"abnormal stream must terminate with a protocol error frame on %s (%s)", ctx.fixturePath, ctx.spec.slug)
		return
	}
	if ctx.newResp != nil {
		require.Equal(t, ResponseHasSemanticContent(ctx.newResp), hasAnySemanticFrame(ctx.newFrames),
			"Semantic=true frame presence must match ResponseHasSemanticContent on %s (%s)", ctx.fixturePath, ctx.spec.slug)
	}
}

// verifyClientFrameOutputMeaning independently parses every frame in frames for
// clientProto and asserts wire-protocol (Semantic, Terminal) compliance. Any
// invalid event, misflagged frame, or non-final Terminal=true frame fails t immediately.
func verifyClientFrameOutputMeaning(
	t *testing.T,
	clientProto protocol.ProtocolID,
	frames []sse.Frame,
	fixturePath, routeSlug string,
) {
	t.Helper()
	mismatches := findWireFrameFlagMismatches(clientProto, frames)
	require.Empty(t, mismatches,
		"new client stream violated per-frame Semantic/Terminal wire contract on %s (%s): %v",
		fixturePath, routeSlug, mismatches)
}

// findWireFrameFlagMismatches inspects every frame in frames against the client
// wire protocol's independent (Semantic, Terminal) contract. It returns a slice
// of human-readable mismatch descriptions when any frame is invalid, misflagged,
// or followed by additional frames after Terminal=true.
func findWireFrameFlagMismatches(clientProto protocol.ProtocolID, frames []sse.Frame) []string {
	var mismatches []string
	terminalCount := 0
	seenRespSemOut := make(map[int]bool)
	seenRespSemID := make(map[string]bool)
	for i, f := range frames {
		label, wantSem, wantTerm, validFrame := expectedWireFrameOutputMeaning(clientProto, f)
		if !validFrame {
			mismatches = append(mismatches, fmt.Sprintf("frame[%d](%s): invalid event for %s", i, label, clientProto))
			continue
		}
		if clientProto == protocol.ProtocolOpenAIResponses {
			if wantSem {
				outIdx, hasOutIdx, itemID, _ := inspectResponsesStreamFrameItem(f.Data)
				if hasOutIdx {
					seenRespSemOut[outIdx] = true
				}
				if itemID != "" {
					seenRespSemID[itemID] = true
				}
			} else if label == "response.output_item.done" {
				outIdx, hasOutIdx, itemID, itemHasSem := inspectResponsesStreamFrameItem(f.Data)
				alreadySaw := (hasOutIdx && seenRespSemOut[outIdx]) || (itemID != "" && seenRespSemID[itemID])
				if !alreadySaw && itemHasSem {
					wantSem = true
					if hasOutIdx {
						seenRespSemOut[outIdx] = true
					}
					if itemID != "" {
						seenRespSemID[itemID] = true
					}
				}
			}
		}
		if f.Semantic != wantSem || f.Terminal != wantTerm {
			mismatches = append(mismatches, fmt.Sprintf(
				"frame[%d](%s): got (Semantic=%v,Terminal=%v), want (Semantic=%v,Terminal=%v)",
				i, label, f.Semantic, f.Terminal, wantSem, wantTerm))
		}
		if f.Terminal {
			terminalCount++
			if i != len(frames)-1 {
				mismatches = append(mismatches, fmt.Sprintf("frame[%d](%s): premature Terminal=true before final frame", i, label))
			}
		}
	}
	if terminalCount > 1 {
		mismatches = append(mismatches, fmt.Sprintf("multiple Terminal=true frames (%d)", terminalCount))
	}
	return mismatches
}

// expectedWireFrameOutputMeaning independently computes the canonical role label
// and expected (Semantic, Terminal, validForProtocol) flags for one wire SSE
// frame. Events unrecognized or malformed under clientProto return
// validForProtocol=false so callers detect invalid wire events.
func expectedWireFrameOutputMeaning(clientProto protocol.ProtocolID, f sse.Frame) (string, bool, bool, bool) {
	trimmed := bytes.TrimSpace(f.Data)
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		ev := strings.TrimSpace(f.Event)
		switch ev {
		case "message_start", "content_block_stop", "message_delta", "ping":
			return ev, false, false, true
		case "message_stop", "error":
			return ev, false, true, true
		case "content_block_start":
			var payload struct {
				ContentBlock struct {
					Type string `json:"type"`
					Data string `json:"data"`
				} `json:"content_block"`
			}
			_ = json.Unmarshal(trimmed, &payload, jsontext.AllowInvalidUTF8(true))
			bt := payload.ContentBlock.Type
			sem := bt == "tool_use" || (bt == "redacted_thinking" && payload.ContentBlock.Data != "")
			return "content_block_start:" + bt, sem, false, true
		case "content_block_delta":
			var payload struct {
				Delta struct {
					Type        string `json:"type"`
					Text        string `json:"text"`
					Thinking    string `json:"thinking"`
					Signature   string `json:"signature"`
					PartialJSON string `json:"partial_json"`
				} `json:"delta"`
			}
			_ = json.Unmarshal(trimmed, &payload, jsontext.AllowInvalidUTF8(true))
			dt := payload.Delta.Type
			sem := false
			switch dt {
			case "text_delta":
				sem = payload.Delta.Text != ""
			case "thinking_delta":
				sem = payload.Delta.Thinking != ""
			case "signature_delta":
				sem = payload.Delta.Signature != ""
			case "input_json_delta":
				sem = payload.Delta.PartialJSON != ""
			}
			return "content_block_delta:" + dt, sem, false, true
		default:
			return ev, false, false, false
		}

	case protocol.ProtocolOpenAIResponses:
		if bytes.Equal(trimmed, []byte("[DONE]")) {
			// Reject Chat-style [DONE] sentinels on OpenAI Responses streams because
			// Responses terminates with typed response.completed, response.failed, or response.incomplete events.
			return "[DONE]", false, true, false
		}
		evType, sem, term := classifyResponsesWireFrameWithEvent(trimmed, f.Event)
		if evType == "" {
			return "data", false, term, term
		}
		return evType, sem, term, true

	case protocol.ProtocolOpenAIChatCompletions:
		if bytes.Equal(trimmed, []byte("[DONE]")) {
			return "[DONE]", false, true, true
		}
		var probe struct {
			Error   jsontext.Value `json:"error"`
			Usage   jsontext.Value `json:"usage"`
			Choices []struct {
				FinishReason jsontext.Value `json:"finish_reason"`
				Delta        struct {
					Content          string `json:"content"`
					ReasoningContent string `json:"reasoning_content"`
					Reasoning        string `json:"reasoning"`
					Refusal          string `json:"refusal"`
					ToolCalls        []struct {
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
		}
		if json.Unmarshal(trimmed, &probe, jsontext.AllowInvalidUTF8(true)) != nil {
			return "invalid_json", false, false, false
		}
		if len(probe.Error) > 0 && !bytes.Equal(bytes.TrimSpace(probe.Error), []byte("null")) {
			return "chat_error", false, true, true
		}
		if len(probe.Choices) == 0 && len(probe.Usage) > 0 && !bytes.Equal(bytes.TrimSpace(probe.Usage), []byte("null")) {
			return "chat_usage", false, false, true
		}
		hasToolOpen := false
		hasToolArgs := false
		hasReasoning := false
		hasRefusal := false
		hasText := false
		hasFinish := false
		for _, ch := range probe.Choices {
			if len(ch.FinishReason) > 0 && !bytes.Equal(bytes.TrimSpace(ch.FinishReason), []byte("null")) {
				hasFinish = true
			}
			for _, tc := range ch.Delta.ToolCalls {
				if tc.ID != "" || tc.Function.Name != "" {
					hasToolOpen = true
				} else {
					hasToolArgs = true
				}
			}
			if ch.Delta.ReasoningContent != "" || ch.Delta.Reasoning != "" {
				hasReasoning = true
			}
			if ch.Delta.Refusal != "" {
				hasRefusal = true
			}
			if ch.Delta.Content != "" {
				hasText = true
			}
		}
		switch {
		case hasToolOpen:
			return "chat_tool_open", true, false, true
		case hasToolArgs:
			return "chat_tool_args_delta", true, false, true
		case hasReasoning:
			return "chat_reasoning_delta", true, false, true
		case hasRefusal:
			return "chat_refusal_delta", true, false, true
		case hasText:
			return "chat_text_delta", true, false, true
		case hasFinish:
			return "chat_finish", false, false, true
		default:
			return "chat_role", false, false, true
		}

	default:
		return f.Event, false, false, false
	}
}

// hasAnySemanticFrame reports whether at least one frame in frames carries Semantic=true.
func hasAnySemanticFrame(frames []sse.Frame) bool {
	for _, f := range frames {
		if f.Semantic {
			return true
		}
	}
	return false
}

// hasWireErrorFrame reports whether frames includes a client-protocol terminal error frame.
func hasWireErrorFrame(clientProto protocol.ProtocolID, frames []sse.Frame) bool {
	for _, f := range frames {
		lbl, _, _, _ := expectedWireFrameOutputMeaning(clientProto, f)
		switch clientProto {
		case protocol.ProtocolAnthropicMessages:
			if lbl == "error" {
				return true
			}
		case protocol.ProtocolOpenAIResponses:
			if lbl == "response.failed" || lbl == "error" {
				return true
			}
		case protocol.ProtocolOpenAIChatCompletions:
			if lbl == "chat_error" {
				return true
			}
		}
	}
	return false
}
