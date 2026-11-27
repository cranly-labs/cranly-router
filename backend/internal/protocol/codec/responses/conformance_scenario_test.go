package responses

import (
	"bytes"
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
// source scan. It also checks decoded IR golden snapshots across wire, Codex,
// and pi fixtures covering wire.go, request_decode.go, request_encode.go,
// legalize.go, response.go, stream_decode.go, stream_encode.go, usage.go, and finish.go.

// allResponsesRequestFixtures loads every hand-written wire Responses request
// fixture and every recorded Codex and pi corpus Responses request fixture in
// deterministic order. Both sets must be non-empty so laws cover live client shapes.
func allResponsesRequestFixtures(t *testing.T) []testkit.Fixture {
	t.Helper()
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureRequest)
	require.NoError(t, err)
	require.NotEmpty(t, wireReqs)

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolOpenAIResponses)
	require.NoError(t, err)
	require.NotEmpty(t, corpusReqs, "recorded Codex and pi Responses corpus fixtures must be present")

	sawCodex := false
	sawPi := false
	for _, f := range corpusReqs {
		if strings.Contains(f.Path, "codex/") {
			sawCodex = true
		}
		if strings.Contains(f.Path, "pi/") {
			sawPi = true
		}
	}
	require.True(t, sawCodex, "expected recorded codex corpus fixtures")
	require.True(t, sawPi, "expected recorded pi corpus fixtures")

	combined := make([]testkit.Fixture, 0, len(wireReqs)+len(corpusReqs))
	combined = append(combined, wireReqs...)
	combined = append(combined, corpusReqs...)
	return combined
}

// TestNoDirectExtraMembersIndexing scans all handwritten production Go files in
// codec/responses for direct Extra.Members map indexing. Section 3.4 anti-abuse
// rule 1 forbids codecs from reading vendor extension keys by name in production paths.
func TestNoDirectExtraMembersIndexing(t *testing.T) {
	sources, err := testkit.ReadGoProductionSources(".")
	require.NoError(t, err)
	require.Len(t, sources, 9, "expected all 9 codec/responses production Go files")

	for _, src := range sources {
		if strings.Contains(string(src.Data), ".Members[") {
			t.Fatalf("%s contains forbidden direct .Members[ map indexing", src.Path)
		}
	}
}

// TestLawQ1_RequestFixturesDecodeAndValidate checks that every OpenAI Responses
// wire fixture and recorded Codex/pi corpus request fixture decodes without
// error. Every decoded ir.Request must pass all structural invariants in ir.Validate.
func TestLawQ1_RequestFixturesDecodeAndValidate(t *testing.T) {
	var codec Codec
	for _, fix := range allResponsesRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			req, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)
			require.NoError(t, ir.Validate(req))
		})
	}
}

// TestLawQ2_RequestRoundTripFixedPoint checks that for every Responses wire and
// recorded Codex/pi corpus request fixture x, y = Enc(Dec(x)) incurs zero Loss
// and passes strict Responses request validation. Re-decoding y must equal
// Dec(x) structurally, and Enc(Dec(y)) must match y byte-for-byte.
func TestLawQ2_RequestRoundTripFixedPoint(t *testing.T) {
	var codec Codec
	for _, fix := range allResponsesRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			dec1, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)

			y, losses, err := codec.EncodeRequest(dec1, ir.Dialect{})
			require.NoError(t, err)
			require.Empty(t, losses, "same-protocol Responses round trip must incur zero Loss")

			violations := testkit.ValidateResponsesRequest(y, testkit.RequestValidationOptions{})
			require.Empty(t, violations, "encoded Responses request violations: %v", violations)

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
// on the same decoded request across Extra maps, namespaces, and multi-tool turns.
// Every invocation must produce byte-for-byte identical JSON bytes.
func TestLawQ5_RequestEncodingDeterministic(t *testing.T) {
	var codec Codec
	for _, fix := range allResponsesRequestFixtures(t) {
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

// TestLawS1_StreamFixturesDecodeAndValidate feeds every OpenAI Responses wire
// SSE stream fixture through sse.NewReader and StreamDecoder. Each resulting
// event slice must satisfy ir.ValidateStream without lifecycle or slot violations.
func TestLawS1_StreamFixturesDecodeAndValidate(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureStream)
	require.NoError(t, err)
	require.NotEmpty(t, streams)

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			require.NoError(t, ir.ValidateStream(events))
		})
	}
}

// TestLawS2_StreamAndBufferedResponseParity compares paired OpenAI Responses
// stream and non-streaming response fixtures across all 29 shared scenarios.
// Folding the decoded stream via ir.Accumulate must match DecodeResponse in
// ResponseEssence, Finish, Usage, ID, Model, CreatedAt, and ServiceTier.
func TestLawS2_StreamAndBufferedResponseParity(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureStream)
	require.NoError(t, err)
	responses, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureResponse)
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

// TestLawS3_StreamEncodePassesGrammar encodes every decoded Responses stream
// fixture with StreamEncoder and validates the emitted frames through
// testkit.ValidateResponsesStream. Normal fixtures must pass strict grammar
// mode, while stream_truncated and stream_error_event must pass AllowAbnormal mode.
func TestLawS3_StreamEncodePassesGrammar(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureStream)
	require.NoError(t, err)

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			view := ir.ClientView{
				Model:   "gpt-5",
				Runtime: newDeterministicRuntime(),
			}
			frames, _ := encodeFullEventStream(t, view, events)

			abnormal := fix.Scenario == "stream_truncated" || fix.Scenario == "stream_error_event"
			violations := testkit.ValidateResponsesStream(frames, testkit.GrammarOptions{
				AllowAbnormal: abnormal,
			})
			require.Empty(t, violations, "ValidateResponsesStream violations on %s: %v", fix.Path, violations)
		})
	}
}

// TestLawS6_StreamAndBufferedEncodeParity encodes every normal Responses stream
// fixture through both StreamEncoder and buffered EncodeResponse. Re-decoding
// both outputs and running SimulateResponsesNextTurn must produce identical
// assistant state and identical Loss records.
func TestLawS6_StreamAndBufferedEncodeParity(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureStream)
	require.NoError(t, err)

	prevReq := []byte(`{"model":"gpt-5","input":"prompt"}`)

	for _, fix := range streams {
		if fix.Scenario == "stream_truncated" || fix.Scenario == "stream_error_event" {
			continue
		}
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			srcResp, err := ir.Accumulate(events)
			require.NoError(t, err)

			streamFrames, streamLosses := encodeFullEventStream(t, ir.ClientView{
				Model:   "gpt-5",
				Runtime: newDeterministicRuntime(),
			}, events)

			bufBytes, bufLosses, err := EncodeResponse(srcResp, ir.ClientView{
				Model:   "gpt-5",
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

			nextFromStream, err := testkit.SimulateResponsesNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseFrames:           streamFrames,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)
			nextFromBuf, err := testkit.SimulateResponsesNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseBody:             bufBytes,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)

			diffs, err := testkit.JSONDiff(nextFromBuf, nextFromStream)
			require.NoError(t, err)
			require.Empty(t, diffs, "SimulateResponsesNextTurn diff on %s:\n%s", fix.Name, testkit.FormatJSONDiff(diffs))
		})
	}
}

// TestLawS6_CrossGroupAndMultiSegmentParity verifies streaming and buffered
// Responses encoding parity on valid canonical event streams mixing missing IDs,
// multi-segment reasoning, grouped text/refusal messages, custom calls, and
// unsealed foreign tokens. Both paths must match byte-for-byte in next-turn
// client simulation and pass VerifyEssence.
func TestLawS6_CrossGroupAndMultiSegmentParity(t *testing.T) {
	prevReq := []byte(`{"model":"gpt-5","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"inspect cluster"}]}]}`)

	cases := []struct {
		name   string     // scenario subtest name
		events []ir.Event // canonical input event sequence
	}{
		{
			name: "tool_then_multi_segment_reasoning_then_text_refusal_then_custom_call",
			events: []ir.Event{
				{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{Model: "claude-sonnet-4-5", ServiceTier: "priority"}},
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
							Token:     &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig_0"},
						},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 1,
					Part:  &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{}},
				},
				{Kind: ir.EventPartDelta, Index: 1, Delta: &ir.Delta{Text: "Checking node health. "}},
				{Kind: ir.EventPartDelta, Index: 1, Delta: &ir.Delta{Text: "Checking pod restarts."}},
				{
					Kind:  ir.EventPartEnd,
					Index: 1,
					Part: &ir.Part{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Segments: []string{"Checking node health. ", "Checking pod restarts."},
							Token:    &ir.ReplayToken{Data: "enc_bare_1"},
						},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 2,
					Part:  &ir.Part{Kind: ir.PartText},
				},
				{Kind: ir.EventPartDelta, Index: 2, Delta: &ir.Delta{Text: "Dispatching patch. "}},
				{
					Kind:  ir.EventPartEnd,
					Index: 2,
					Part:  &ir.Part{Kind: ir.PartText, Text: "Dispatching patch. "},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 3,
					Part:  &ir.Part{Kind: ir.PartRefusal},
				},
				{Kind: ir.EventPartDelta, Index: 3, Delta: &ir.Delta{Text: "Skipping prod secrets."}},
				{
					Kind:  ir.EventPartEnd,
					Index: 3,
					Part:  &ir.Part{Kind: ir.PartRefusal, Text: "Skipping prod secrets."},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 4,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_patch_1", Tool: ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"}}},
				},
				{Kind: ir.EventPartDelta, Index: 4, Delta: &ir.Delta{Arguments: "*** Begin Patch\n"}},
				{
					Kind:  ir.EventPartEnd,
					Index: 4,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_patch_1", Tool: ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"}, Arguments: "*** Begin Patch\n*** End Patch\n"}},
				},
				{Kind: ir.EventUsage, Usage: &ir.Usage{Input: 80, CacheRead: 20, Output: 35, Reasoning: 12}},
				{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishToolCalls}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, ir.ValidateStream(tc.events))
			srcResp, err := ir.Accumulate(tc.events)
			require.NoError(t, err)

			reqEcho := &ir.Request{System: []ir.Part{{Kind: ir.PartText, Text: "Be thorough."}}}
			streamFrames, streamLosses := encodeFullEventStream(t, ir.ClientView{
				Model:   "gpt-5",
				Request: reqEcho,
				Runtime: newDeterministicRuntime(),
			}, tc.events)
			require.Empty(t, testkit.ValidateResponsesStream(streamFrames, testkit.GrammarOptions{}))

			bufBytes, bufLosses, err := EncodeResponse(srcResp, ir.ClientView{
				Model:   "gpt-5",
				Request: reqEcho,
				Runtime: newDeterministicRuntime(),
			})
			require.NoError(t, err)
			require.Equal(t, bufLosses, streamLosses)

			nextFromStream, err := testkit.SimulateResponsesNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseFrames:           streamFrames,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)
			nextFromBuf, err := testkit.SimulateResponsesNextTurn(testkit.TurnInput{
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
			bufEssenceDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(bufDecoded), bufLosses)
			require.NoError(t, err)
			require.Empty(t, bufEssenceDiffs, testkit.FormatJSONDiff(bufEssenceDiffs))

			streamDecoded, err := ir.Accumulate(decodeFullSSEStream(t, formatSSEFrames(streamFrames)))
			require.NoError(t, err)
			require.Equal(t, bufDecoded, streamDecoded)
			streamEssenceDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(streamDecoded), streamLosses)
			require.NoError(t, err)
			require.Empty(t, streamEssenceDiffs, testkit.FormatJSONDiff(streamEssenceDiffs))
		})
	}
}

// TestLawS7_StreamTruncationAbortsValidly truncates every Responses wire stream
// fixture at every raw SSE frame prefix and every non-terminal IR event prefix.
// Closing with a transport error must always satisfy ir.ValidateStream and
// testkit.ValidateResponsesStream in AllowAbnormal mode without panicking.
func TestLawS7_StreamTruncationAbortsValidly(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureStream)
	require.NoError(t, err)

	abortErr := errors.New("simulated upstream disconnect")

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			rawFrames, err := fix.Frames()
			require.NoError(t, err)

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
					Model:   "gpt-5",
					Runtime: newDeterministicRuntime(),
				}, events)
				violations := testkit.ValidateResponsesStream(outFrames, testkit.GrammarOptions{AllowAbnormal: true})
				require.Empty(t, violations, "raw frame cut=%d grammar violations: %v", cut, violations)
			}

			fullEvents := decodeFullSSEStream(t, fix.Data)
			for cut := range fullEvents {
				if fullEvents[cut].Kind.IsTerminal() {
					break
				}
				enc := NewStreamEncoder(ir.ClientView{
					Model:   "gpt-5",
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
					Err:  &ir.StreamError{Type: "stream_error", Message: abortErr.Error()},
				})
				require.NoError(t, encErr)
				outFrames = append(outFrames, errBatch...)

				violations := testkit.ValidateResponsesStream(outFrames, testkit.GrammarOptions{AllowAbnormal: true})
				require.Empty(t, violations, "event cut=%d grammar violations: %v", cut, violations)
			}
		})
	}
}

// TestResponsesFixturesIRGolden compares decoded IR snapshots of all Responses
// wire request, response, and stream fixtures plus recorded Codex and pi corpus
// requests. Any drift against testdata/golden/ir/ fails the test unless
// snapshots are explicitly updated via TOKENROUTER_UPDATE_GOLDEN=1.
func TestResponsesFixturesIRGolden(t *testing.T) {
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureRequest)
	require.NoError(t, err)
	for _, fix := range wireReqs {
		t.Run("wire_request/"+fix.Name, func(t *testing.T) {
			req, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/wire/request/"+fix.Name+".json", req)
		})
	}

	wireResps, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureResponse)
	require.NoError(t, err)
	for _, fix := range wireResps {
		t.Run("wire_response/"+fix.Name, func(t *testing.T) {
			resp, decErr := DecodeResponse(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/wire/response/"+fix.Name+".json", resp)
		})
	}

	wireStreams, err := testkit.WireFixtures(protocol.ProtocolOpenAIResponses, testkit.FixtureStream)
	require.NoError(t, err)
	for _, fix := range wireStreams {
		t.Run("wire_stream/"+fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			testkit.GoldenJSON(t, "ir/wire/stream/"+fix.Name+".json", events)
		})
	}

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolOpenAIResponses)
	require.NoError(t, err)
	for _, fix := range corpusReqs {
		t.Run("corpus_request/"+fix.Name, func(t *testing.T) {
			req, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/corpus/"+fix.Name+".json", req)
		})
	}
}
