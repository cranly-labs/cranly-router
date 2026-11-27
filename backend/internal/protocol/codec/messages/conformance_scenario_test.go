package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// conformance_scenario_test.go verifies T2 single-protocol conformance laws
// (Q1, Q2, Q5, S1, S2, S3, S6, S7) and the section 3.4 Extra.Members anti-abuse
// source scan. It also checks decoded IR golden snapshots across wire.go,
// request_decode.go, request_encode.go, legalize.go, response.go,
// stream_decode.go, stream_encode.go, usage.go, and finish.go.

// allMessagesRequestFixtures loads every hand-written wire Messages request
// fixture and every recorded Claude Code and pi Messages corpus request fixture
// in deterministic order. Both sets must be non-empty so conformance laws cover
// synthetic and live recorded client payloads.
func allMessagesRequestFixtures(t *testing.T) []testkit.Fixture {
	t.Helper()
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureRequest)
	require.NoError(t, err)
	require.NotEmpty(t, wireReqs)

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolAnthropicMessages)
	require.NoError(t, err)
	require.NotEmpty(t, corpusReqs, "recorded claude-code and pi Messages corpus fixtures must be present")

	combined := make([]testkit.Fixture, 0, len(wireReqs)+len(corpusReqs))
	combined = append(combined, wireReqs...)
	combined = append(combined, corpusReqs...)
	return combined
}

// TestNoDirectExtraMembersIndexing scans all handwritten production Go files in
// codec/messages for direct Extra.Members map indexing. Section 3.4 anti-abuse
// rule 1 forbids codecs from reading vendor extension keys by name in production paths.
func TestNoDirectExtraMembersIndexing(t *testing.T) {
	sources, err := testkit.ReadGoProductionSources(".")
	require.NoError(t, err)
	require.Len(t, sources, 9, "expected all 9 codec/messages production Go files")

	for _, src := range sources {
		if strings.Contains(string(src.Data), ".Members[") {
			t.Fatalf("%s contains forbidden direct .Members[ map indexing", src.Path)
		}
	}
}

// TestLawQ1_RequestFixturesDecodeAndValidate checks that every Messages wire
// and recorded client corpus request fixture decodes without error. Every
// decoded ir.Request must satisfy all structural invariants enforced by ir.Validate.
func TestLawQ1_RequestFixturesDecodeAndValidate(t *testing.T) {
	var codec Codec
	for _, fix := range allMessagesRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			req, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)
			require.NoError(t, ir.Validate(req))
		})
	}
}

// TestLawQ2_RequestRoundTripFixedPoint checks that every strict-valid Messages
// wire and corpus request fixture incurs zero Loss under EncodeRequest, passes
// strict Messages validation, satisfies Dec(Enc(Dec(x))) == Dec(x), and satisfies
// Enc(Dec(y)) == y byte-for-byte. Pre-upgrade legacy_history (which carries an
// empty thinking signature) must pass zero-loss Q2 under passback dialect and
// pass strict validation plus VerifyEssence under the default Anthropic dialect.
func TestLawQ2_RequestRoundTripFixedPoint(t *testing.T) {
	var codec Codec
	for _, fix := range allMessagesRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			dec1, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)

			if fix.Scenario == "legacy_history" {
				// 1. Under passback dialect, unsigned tool-turn thinking survives with zero Loss.
				passbackDialect := ir.Dialect{ReasoningHistory: ir.HistoryToolTurns}
				yPass, lossesPass, err := codec.EncodeRequest(dec1, passbackDialect)
				require.NoError(t, err)
				require.Empty(t, lossesPass)

				vPass := testkit.ValidateMessagesRequest(yPass, testkit.RequestValidationOptions{
					StrictTopLevel: true,
					DialectRules:   []string{"passback"},
				})
				require.Empty(t, vPass, "passback violations: %v", vPass)

				decPass, err := codec.DecodeRequest(yPass)
				require.NoError(t, err)
				require.Equal(t, dec1, decPass)

				yPass2, lossesPass2, err := codec.EncodeRequest(decPass, passbackDialect)
				require.NoError(t, err)
				require.Empty(t, lossesPass2)
				require.True(t, bytes.Equal(yPass, yPass2))

				// 2. Under strict default dialect, unsigned thinking drops with LossReasoningTextDropped,
				// passes strict Messages validation, reconciles Essence, and reaches a byte fixed point.
				yStrict, lossesStrict, err := codec.EncodeRequest(dec1, ir.Dialect{})
				require.NoError(t, err)
				require.Equal(t, []ir.Loss{{
					Kind: ir.LossReasoningTextDropped,
					Path: "messages[3].parts[0]",
				}}, lossesStrict)

				vStrict := testkit.ValidateMessagesRequest(yStrict, testkit.RequestValidationOptions{StrictTopLevel: true})
				require.Empty(t, vStrict, "strict violations on repaired legacy_history: %v", vStrict)

				decStrict, err := codec.DecodeRequest(yStrict)
				require.NoError(t, err)
				essDiffs, err := testkit.VerifyEssence(ir.Essence(dec1), ir.Essence(decStrict), lossesStrict)
				require.NoError(t, err)
				require.Empty(t, essDiffs, testkit.FormatJSONDiff(essDiffs))

				yStrict2, lossesStrict2, err := codec.EncodeRequest(decStrict, ir.Dialect{})
				require.NoError(t, err)
				require.Empty(t, lossesStrict2)
				require.True(t, bytes.Equal(yStrict, yStrict2))
				return
			}

			y, losses, err := codec.EncodeRequest(dec1, ir.Dialect{})
			require.NoError(t, err)
			require.Empty(t, losses, "same-protocol Messages round trip must incur zero Loss")

			violations := testkit.ValidateMessagesRequest(y, testkit.RequestValidationOptions{
				StrictTopLevel: true,
			})
			require.Empty(t, violations, "encoded Messages request violations: %v", violations)

			dec2, err := codec.DecodeRequest(y)
			require.NoError(t, err)
			require.Equal(t, dec1, dec2, "Dec(Enc(Dec(x))) must equal Dec(x)")

			y2, losses2, err := codec.EncodeRequest(dec2, ir.Dialect{})
			require.NoError(t, err)
			require.Empty(t, losses2)
			require.True(t, bytes.Equal(y, y2), "Enc(Dec(y)) must equal y byte-for-byte")
		})
	}
}

// TestLawQ5_RequestEncodingDeterministic verifies repeated EncodeRequest calls
// on the same decoded request across Extra maps, system blocks, and tool arrays.
// Every invocation must produce byte-for-byte identical JSON output.
func TestLawQ5_RequestEncodingDeterministic(t *testing.T) {
	var codec Codec
	for _, fix := range allMessagesRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			req, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)

			first, _, err := codec.EncodeRequest(req, ir.Dialect{})
			require.NoError(t, err)
			for range 5 {
				next, _, err := codec.EncodeRequest(req, ir.Dialect{})
				require.NoError(t, err)
				require.True(t, bytes.Equal(first, next), "EncodeRequest output varied across runs")
			}
		})
	}
}

// TestLawS1_StreamFixturesDecodeAndValidate feeds every Messages wire SSE
// stream fixture through sse.NewReader and StreamDecoder. Each resulting event
// sequence must satisfy ir.ValidateStream without lifecycle or usage violations.
func TestLawS1_StreamFixturesDecodeAndValidate(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureStream)
	require.NoError(t, err)
	require.NotEmpty(t, streams)

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			require.NoError(t, ir.ValidateStream(events))
		})
	}
}

// TestLawS2_StreamAndBufferedResponseParity compares paired Messages stream and
// buffered JSON response fixtures across all 29 shared scenarios. Folding the
// decoded stream via ir.Accumulate must match DecodeResponse in ResponseEssence,
// Finish, Usage, ID, Model, CreatedAt, and ServiceTier.
func TestLawS2_StreamAndBufferedResponseParity(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureStream)
	require.NoError(t, err)
	responses, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureResponse)
	require.NoError(t, err)

	respByScenario := make(map[string]testkit.Fixture, len(responses))
	for _, r := range responses {
		respByScenario[r.Scenario] = r
	}
	require.GreaterOrEqual(t, len(respByScenario), 29)

	var codec Codec
	matchedPairs := 0
	for _, sFix := range streams {
		rFix, ok := respByScenario[sFix.Scenario]
		if !ok {
			continue
		}
		matchedPairs++
		t.Run(sFix.Scenario, func(t *testing.T) {
			events := decodeFullSSEStream(t, sFix.Data)
			folded, err := ir.Accumulate(events)
			require.NoError(t, err)

			buffered, err := codec.DecodeResponse(rFix.Data)
			require.NoError(t, err)

			require.Equal(t, ir.ResponseEssence(buffered), ir.ResponseEssence(folded))
			require.Equal(t, buffered.Finish, folded.Finish)
			require.Equal(t, buffered.Usage, folded.Usage)
			require.Equal(t, buffered.ID, folded.ID)
			require.Equal(t, buffered.Model, folded.Model)
			require.Equal(t, buffered.CreatedAt, folded.CreatedAt)
			require.Equal(t, buffered.ServiceTier, folded.ServiceTier)
		})
	}
	require.Equal(t, len(respByScenario), matchedPairs)
}

// TestLawS3_StreamEncodePassesGrammar encodes every decoded Messages stream
// fixture with StreamEncoder and validates the emitted frames via
// testkit.ValidateMessagesStream. Normal fixtures must pass strict mode, while
// stream_truncated and stream_error_event pass AllowAbnormal mode.
func TestLawS3_StreamEncodePassesGrammar(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureStream)
	require.NoError(t, err)

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			view := ir.ClientView{
				Model:   "claude-sonnet-4-5",
				Runtime: newDeterministicRuntime(),
			}
			frames, _ := encodeFullEventStream(t, view, events)

			abnormal := fix.Scenario == "stream_truncated" || fix.Scenario == "stream_error_event"
			violations := testkit.ValidateMessagesStream(frames, testkit.GrammarOptions{
				AllowAbnormal: abnormal,
			})
			require.Empty(t, violations, "ValidateMessagesStream violations on %s: %v", fix.Path, violations)
		})
	}
}

// TestLawS6_StreamAndBufferedEncodeParity encodes every normal Messages stream
// fixture through both StreamEncoder and buffered EncodeResponse. Re-decoding
// both outputs and running SimulateMessagesNextTurn must produce identical
// responses, identical Loss records, and zero JSONDiff on non-empty replies.
func TestLawS6_StreamAndBufferedEncodeParity(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureStream)
	require.NoError(t, err)

	prevReq := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"messages":[{"role":"user","content":"prompt"}]}`)

	for _, fix := range streams {
		if fix.Scenario == "stream_truncated" || fix.Scenario == "stream_error_event" {
			continue
		}
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			srcResp, err := ir.Accumulate(events)
			require.NoError(t, err)

			streamFrames, streamLosses := encodeFullEventStream(t, ir.ClientView{
				Model:   "claude-sonnet-4-5",
				Runtime: newDeterministicRuntime(),
			}, events)

			bufBytes, bufLosses, err := EncodeResponse(srcResp, ir.ClientView{
				Model:   "claude-sonnet-4-5",
				Runtime: newDeterministicRuntime(),
			})
			require.NoError(t, err)
			require.Equal(t, bufLosses, streamLosses)

			redecodedEvents := decodeFullSSEStream(t, formatSSEFrames(streamFrames))
			streamAggResp, err := ir.Accumulate(redecodedEvents)
			require.NoError(t, err)

			bufDecodedResp, err := DecodeResponse(bufBytes)
			require.NoError(t, err)
			require.Equal(t, bufDecodedResp, streamAggResp)

			if fix.Scenario == "empty_response" {
				return
			}

			nextFromStream, err := testkit.SimulateMessagesNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseFrames:           streamFrames,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)
			nextFromBuf, err := testkit.SimulateMessagesNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseBody:             bufBytes,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)

			diffs, err := testkit.JSONDiff(nextFromBuf, nextFromStream)
			require.NoError(t, err)
			require.Empty(t, diffs, "SimulateMessagesNextTurn diff on %s:\n%s", fix.Name, testkit.FormatJSONDiff(diffs))
		})
	}
}

// TestLawS6_CrossGroupAndDroppedPartsParity verifies streaming and buffered
// Messages encoding parity on valid canonical event streams mixing cross-group
// tool calls, multi-segment reasoning, refusals, citations, missing call IDs,
// and dropped foreign opaque/media/hosted parts. Emitted block indices must
// stay contiguous while client-reconstructed next turns match byte-for-byte.
func TestLawS6_CrossGroupAndDroppedPartsParity(t *testing.T) {
	prevReq := []byte(`{"model":"claude-sonnet-4-5","max_tokens":512,"messages":[{"role":"user","content":"inspect cluster"}]}`)
	fiveMin := int64(45)
	carrierSig, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "sig_think_4",
		Text:   "Checking node health. Checking pod restarts.",
	})
	require.NoError(t, err)

	cases := []struct {
		name       string
		hasRefusal bool
		events     []ir.Event
	}{
		{
			name: "tool_then_dropped_parts_then_multi_segment_reasoning_then_text_then_custom_tool",
			events: []ir.Event{
				{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{Model: "gpt-5"}},
				{
					Kind:  ir.EventPartStart,
					Index: 0,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "check_nodes"}}},
				},
				{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Arguments: `{"zone":"us-east"}`}},
				{
					Kind:  ir.EventPartEnd,
					Index: 0,
					Part: &ir.Part{
						Kind: ir.PartToolCall,
						Call: &ir.ToolCall{
							CallID:    "",
							Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "check_nodes"},
							Arguments: `{"zone":"us-east"}`,
							Token:     &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig_call_0"},
						},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 1,
					Part: &ir.Part{
						Kind:   ir.PartOpaque,
						Opaque: &ir.OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: "compaction", Raw: jsontext.Value(`{"type":"compaction"}`)},
					},
				},
				{
					Kind:  ir.EventPartEnd,
					Index: 1,
					Part: &ir.Part{
						Kind:   ir.PartOpaque,
						Opaque: &ir.OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: "compaction", Raw: jsontext.Value(`{"type":"compaction"}`)},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 2,
					Part:  &ir.Part{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, Source: ir.MediaSourceURL, Data: "https://example.com/a.png"}},
				},
				{
					Kind:  ir.EventPartEnd,
					Index: 2,
					Part:  &ir.Part{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, Source: ir.MediaSourceURL, Data: "https://example.com/a.png"}},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 3,
					Part: &ir.Part{
						Kind: ir.PartOpaque,
						Opaque: &ir.OpaqueItem{
							Origin: protocol.ProtocolAnthropicMessages,
							Type:   "server_tool_use",
							Raw:    jsontext.Value(`{"type":"server_tool_use","id":"srvtoolu_1","name":"web_search","input":{"query":"cluster"}}`),
						},
					},
				},
				{
					Kind:  ir.EventPartEnd,
					Index: 3,
					Part: &ir.Part{
						Kind: ir.PartOpaque,
						Opaque: &ir.OpaqueItem{
							Origin: protocol.ProtocolAnthropicMessages,
							Type:   "server_tool_use",
							Raw:    jsontext.Value(`{"type":"server_tool_use","id":"srvtoolu_1","name":"web_search","input":{"query":"cluster"}}`),
						},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 4,
					Part:  &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{}},
				},
				{Kind: ir.EventPartDelta, Index: 4, Delta: &ir.Delta{Text: "Checking node health. "}},
				{Kind: ir.EventPartDelta, Index: 4, Delta: &ir.Delta{Text: "Checking pod restarts."}},
				{
					Kind:  ir.EventPartEnd,
					Index: 4,
					Part: &ir.Part{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Segments: []string{"Checking node health. ", "Checking pod restarts."},
							Token:    &ir.ReplayToken{Data: carrierSig},
						},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 5,
					Part:  &ir.Part{Kind: ir.PartText},
				},
				{Kind: ir.EventPartDelta, Index: 5, Delta: &ir.Delta{Text: "Dispatching secondary probe."}},
				{
					Kind:  ir.EventPartEnd,
					Index: 5,
					Part: &ir.Part{
						Kind:        ir.PartText,
						ID:          "msg_txt_5",
						Text:        "Dispatching secondary probe.",
						Annotations: []ir.Annotation{{Kind: "url_citation", URL: "https://go.dev"}},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 6,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_custom_6", Tool: ir.ToolRef{Kind: ir.ToolCustom, Namespace: "k8s", Name: "apply_patch"}}},
				},
				{Kind: ir.EventPartDelta, Index: 6, Delta: &ir.Delta{Arguments: "*** Begin Patch\n+ok"}},
				{
					Kind:  ir.EventPartEnd,
					Index: 6,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_custom_6", Tool: ir.ToolRef{Kind: ir.ToolCustom, Namespace: "k8s", Name: "apply_patch"}, Arguments: "*** Begin Patch\n+ok"}},
				},
				{Kind: ir.EventUsage, Usage: &ir.Usage{Input: 80, CacheRead: 20, CacheWrite: 45, CacheWrite5m: &fiveMin, Output: 35, Speed: "fast"}},
				{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishToolCalls}},
			},
		},
		{
			name:       "refusal_then_redacted_thinking_then_thinking_cross_group",
			hasRefusal: true,
			events: []ir.Event{
				{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "msg_ref_cross", Model: "claude-sonnet-4-5"}},
				{Kind: ir.EventPartStart, Index: 0, Part: &ir.Part{Kind: ir.PartRefusal}},
				{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Text: "Request declined by policy."}},
				{Kind: ir.EventPartEnd, Index: 0, Part: &ir.Part{Kind: ir.PartRefusal, Text: "Request declined by policy."}},
				{
					Kind:  ir.EventPartStart,
					Index: 1,
					Part:  &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Redacted: true}},
				},
				{
					Kind:  ir.EventPartEnd,
					Index: 1,
					Part:  &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Redacted: true, Token: &ir.ReplayToken{Data: "EmwKRedactedCrossGroup"}}},
				},
				{Kind: ir.EventPartStart, Index: 2, Part: &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{}}},
				{Kind: ir.EventPartDelta, Index: 2, Delta: &ir.Delta{Text: "Evaluated safety boundary."}},
				{Kind: ir.EventPartEnd, Index: 2, Part: &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Segments: []string{"Evaluated safety boundary."}, Token: &ir.ReplayToken{Data: "EqQThinkingCrossGroup"}}}},
				{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishContentFilter}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, ir.ValidateStream(tc.events))
			srcResp, err := ir.Accumulate(tc.events)
			require.NoError(t, err)

			streamFrames, streamLosses := encodeFullEventStream(t, ir.ClientView{
				Model:   "claude-sonnet-4-5",
				Runtime: newDeterministicRuntime(),
			}, tc.events)
			require.Empty(t, testkit.ValidateMessagesStream(streamFrames, testkit.GrammarOptions{}))

			bufBytes, bufLosses, err := EncodeResponse(srcResp, ir.ClientView{
				Model:   "claude-sonnet-4-5",
				Runtime: newDeterministicRuntime(),
			})
			require.NoError(t, err)
			require.Equal(t, bufLosses, streamLosses)

			nextFromStream, err := testkit.SimulateMessagesNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseFrames:           streamFrames,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)
			nextFromBuf, err := testkit.SimulateMessagesNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseBody:             bufBytes,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)

			simDiffs, err := testkit.JSONDiff(nextFromBuf, nextFromStream)
			require.NoError(t, err)
			require.Empty(t, simDiffs, testkit.FormatJSONDiff(simDiffs))

			bufDecoded, err := DecodeResponse(bufBytes)
			require.NoError(t, err)

			streamDecoded, err := ir.Accumulate(decodeFullSSEStream(t, formatSSEFrames(streamFrames)))
			require.NoError(t, err)
			require.Equal(t, bufDecoded, streamDecoded)

			if tc.hasRefusal {
				// Messages represents refusal via normal text blocks paired with stop_reason="refusal"
				// while recording LossRefusalInlined so Law S4 VerifyEssence reconciles cleanly.
				require.Equal(t, ir.PartText, bufDecoded.Parts[0].Kind)
				require.Equal(t, ir.FinishContentFilter, bufDecoded.Finish.Reason)
			}

			bufEssenceDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(bufDecoded), bufLosses)
			require.NoError(t, err)
			require.Empty(t, bufEssenceDiffs, testkit.FormatJSONDiff(bufEssenceDiffs))

			streamEssenceDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(streamDecoded), streamLosses)
			require.NoError(t, err)
			require.Empty(t, streamEssenceDiffs, testkit.FormatJSONDiff(streamEssenceDiffs))
		})
	}
}

// TestLawS7_StreamTruncationAbortsValidly truncates every Messages wire stream
// fixture at every raw SSE frame prefix and every non-terminal IR event prefix.
// Closing with a transport error must always satisfy ir.ValidateStream and
// testkit.ValidateMessagesStream in AllowAbnormal mode without panicking.
func TestLawS7_StreamTruncationAbortsValidly(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureStream)
	require.NoError(t, err)

	abortErr := errors.New("simulated upstream disconnect")

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			rawFrames, err := fix.Frames()
			require.NoError(t, err)

			// 1. Truncate at every raw SSE frame boundary 0..len(rawFrames)-1.
			for cut := range rawFrames {
				dec := NewStreamDecoder()
				var events []ir.Event
				for _, f := range rawFrames[:cut] {
					batch, decErr := dec.Decode(f)
					require.NoError(t, decErr)
					events = append(events, batch...)
				}
				events = append(events, dec.Close(abortErr)...)
				require.NoError(t, ir.ValidateStream(events), "raw frame cut=%d failed ValidateStream", cut)

				outFrames, _ := encodeFullEventStream(t, ir.ClientView{
					Model:   "claude-sonnet-4-5",
					Runtime: newDeterministicRuntime(),
				}, events)
				violations := testkit.ValidateMessagesStream(outFrames, testkit.GrammarOptions{AllowAbnormal: true})
				require.Empty(t, violations, "raw frame cut=%d grammar violations: %v", cut, violations)
			}

			// 2. Truncate at every non-terminal IR event boundary 0..len(fullEvents)-1.
			fullEvents := decodeFullSSEStream(t, fix.Data)
			for cut := range fullEvents {
				if fullEvents[cut].Kind.IsTerminal() {
					break
				}
				enc := NewStreamEncoder(ir.ClientView{
					Model:   "claude-sonnet-4-5",
					Runtime: newDeterministicRuntime(),
				})
				var outFrames []sse.Frame
				for _, ev := range fullEvents[:cut] {
					fBatch, _, encErr := enc.Encode(ev)
					require.NoError(t, encErr)
					outFrames = append(outFrames, fBatch...)
				}
				errBatch, _, encErr := enc.Encode(ir.Event{
					Kind: ir.EventError,
					Err:  &ir.StreamError{Type: "api_error", Message: abortErr.Error()},
				})
				require.NoError(t, encErr)
				outFrames = append(outFrames, errBatch...)

				violations := testkit.ValidateMessagesStream(outFrames, testkit.GrammarOptions{AllowAbnormal: true})
				require.Empty(t, violations, "event cut=%d grammar violations: %v", cut, violations)
			}
		})
	}
}

// TestMessagesFixturesIRGolden compares decoded IR snapshots of all Messages
// wire request, response, and stream fixtures plus recorded Claude Code and pi
// Messages corpus requests against testdata/golden/ir/. Any unreviewed change to
// decoded IR field values or event sequences fails until golden snapshots update.
func TestMessagesFixturesIRGolden(t *testing.T) {
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureRequest)
	require.NoError(t, err)
	for _, fix := range wireReqs {
		t.Run("wire_request/"+fix.Name, func(t *testing.T) {
			req, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/wire/request/"+fix.Name+".json", req)
		})
	}

	wireResps, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureResponse)
	require.NoError(t, err)
	for _, fix := range wireResps {
		t.Run("wire_response/"+fix.Name, func(t *testing.T) {
			resp, decErr := DecodeResponse(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/wire/response/"+fix.Name+".json", resp)
		})
	}

	wireStreams, err := testkit.WireFixtures(protocol.ProtocolAnthropicMessages, testkit.FixtureStream)
	require.NoError(t, err)
	for _, fix := range wireStreams {
		t.Run("wire_stream/"+fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			testkit.GoldenJSON(t, "ir/wire/stream/"+fix.Name+".json", events)
		})
	}

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolAnthropicMessages)
	require.NoError(t, err)
	for _, fix := range corpusReqs {
		t.Run("corpus_request/"+fix.Name, func(t *testing.T) {
			req, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/corpus/"+fix.Name+".json", req)
		})
	}
}
