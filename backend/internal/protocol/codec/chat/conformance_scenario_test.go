package chat

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
// source scan. It also checks decoded IR golden snapshots across wire.go,
// request_decode.go, request_encode.go, legalize.go, response.go,
// stream_decode.go, stream_encode.go, usage.go, and finish.go.

// allChatRequestFixtures loads every hand-written wire Chat request fixture and
// every recorded client corpus Chat request fixture in deterministic order. Both
// fixture sets must be non-empty so conformance laws cover synthetic and live shapes.
func allChatRequestFixtures(t *testing.T) []testkit.Fixture {
	t.Helper()
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureRequest)
	require.NoError(t, err)
	require.NotEmpty(t, wireReqs)

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolOpenAIChatCompletions)
	require.NoError(t, err)
	require.NotEmpty(t, corpusReqs, "recorded pi Chat corpus fixtures must be present")

	combined := make([]testkit.Fixture, 0, len(wireReqs)+len(corpusReqs))
	combined = append(combined, wireReqs...)
	combined = append(combined, corpusReqs...)
	return combined
}

// TestNoDirectExtraMembersIndexing scans all handwritten production Go files in
// codec/chat for direct Extra.Members map indexing. Section 3.4 anti-abuse rule 1
// forbids codecs from reading vendor extension keys by name in production paths.
func TestNoDirectExtraMembersIndexing(t *testing.T) {
	sources, err := testkit.ReadGoProductionSources(".")
	require.NoError(t, err)
	require.Len(t, sources, 9, "expected all 9 codec/chat production Go files")

	for _, src := range sources {
		if strings.Contains(string(src.Data), ".Members[") {
			t.Fatalf("%s contains forbidden direct .Members[ map indexing", src.Path)
		}
	}
}

// TestLawQ1_RequestFixturesDecodeAndValidate checks that every Chat wire and
// recorded corpus request fixture decodes without error. Every decoded ir.Request
// must satisfy all structural invariants enforced by ir.Validate.
func TestLawQ1_RequestFixturesDecodeAndValidate(t *testing.T) {
	var codec Codec
	for _, fix := range allChatRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			req, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)
			require.NoError(t, ir.Validate(req))
		})
	}
}

// TestLawQ2_RequestRoundTripFixedPoint checks that for every Chat wire and
// corpus request fixture x, y = Enc(Dec(x)) incurs zero Loss and passes strict
// Chat request validation. Re-decoding y must equal Dec(x) structurally, and
// Enc(Dec(y)) must match y byte-for-byte.
func TestLawQ2_RequestRoundTripFixedPoint(t *testing.T) {
	var codec Codec
	for _, fix := range allChatRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			dec1, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)

			y, losses, err := codec.EncodeRequest(dec1, ir.Dialect{})
			require.NoError(t, err)
			require.Empty(t, losses, "same-protocol Chat round trip must incur zero Loss")

			violations := testkit.ValidateChatRequest(y, testkit.RequestValidationOptions{})
			require.Empty(t, violations, "encoded Chat request violations: %v", violations)

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
// on the same decoded request across Extra maps and multi-tool turns. Every
// invocation must produce byte-for-byte identical JSON bytes.
func TestLawQ5_RequestEncodingDeterministic(t *testing.T) {
	var codec Codec
	for _, fix := range allChatRequestFixtures(t) {
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

// TestLawS1_StreamFixturesDecodeAndValidate feeds every Chat wire SSE stream
// fixture through sse.NewReader and StreamDecoder. Each resulting event slice
// must satisfy ir.ValidateStream without violating lifecycle or slot ordering.
func TestLawS1_StreamFixturesDecodeAndValidate(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureStream)
	require.NoError(t, err)
	require.NotEmpty(t, streams)

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			require.NoError(t, ir.ValidateStream(events))
		})
	}
}

// TestLawS2_StreamAndBufferedResponseParity compares paired Chat stream and
// non-streaming response fixtures across all 29 shared scenarios. Folding the
// decoded stream via ir.Accumulate must match DecodeResponse in ResponseEssence,
// Finish, Usage, ID, Model, CreatedAt, and ServiceTier.
func TestLawS2_StreamAndBufferedResponseParity(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureStream)
	require.NoError(t, err)
	responses, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureResponse)
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

// TestLawS3_StreamEncodePassesGrammar encodes every decoded Chat stream fixture
// with StreamEncoder and validates the emitted frames via testkit.ValidateChatStream.
// Normal fixtures including repaired tool_ids_missing must pass strict mode,
// while stream_truncated and stream_error_event pass AllowAbnormal mode.
func TestLawS3_StreamEncodePassesGrammar(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureStream)
	require.NoError(t, err)

	incTrue := true
	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			view := ir.ClientView{
				Model:   "gpt-4o",
				Request: &ir.Request{IncludeUsage: &incTrue},
				Runtime: newDeterministicRuntime(),
			}
			frames, _ := encodeFullEventStream(t, view, events)

			abnormal := fix.Scenario == "stream_truncated" || fix.Scenario == "stream_error_event"
			violations := testkit.ValidateChatStream(frames, testkit.GrammarOptions{
				AllowAbnormal: abnormal,
			})
			require.Empty(t, violations, "ValidateChatStream violations on %s: %v", fix.Path, violations)
		})
	}
}

// TestLawS6_StreamAndBufferedEncodeParity encodes every normal Chat stream
// fixture through both StreamEncoder and buffered EncodeResponse. Re-decoding
// both outputs and running SimulateChatNextTurn must produce identical assistant
// state and identical Loss records.
func TestLawS6_StreamAndBufferedEncodeParity(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureStream)
	require.NoError(t, err)

	incTrue := true
	prevReq := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"prompt"}]}`)

	for _, fix := range streams {
		if fix.Scenario == "stream_truncated" || fix.Scenario == "stream_error_event" {
			continue
		}
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			srcResp, err := ir.Accumulate(events)
			require.NoError(t, err)

			streamFrames, streamLosses := encodeFullEventStream(t, ir.ClientView{
				Model:   "gpt-4o",
				Request: &ir.Request{IncludeUsage: &incTrue},
				Runtime: newDeterministicRuntime(),
			}, events)

			bufBytes, bufLosses, err := EncodeResponse(srcResp, ir.ClientView{
				Model:   "gpt-4o",
				Request: &ir.Request{IncludeUsage: &incTrue},
				Runtime: newDeterministicRuntime(),
			})
			require.NoError(t, err)
			require.Equal(t, bufLosses, streamLosses)

			// Compare decoded IR Response from streamed SSE versus buffered JSON.
			redecodedEvents := decodeFullSSEStream(t, formatSSEFrames(streamFrames))
			streamAggResp, err := ir.Accumulate(redecodedEvents)
			require.NoError(t, err)

			bufDecodedResp, err := DecodeResponse(bufBytes)
			require.NoError(t, err)
			require.Equal(t, bufDecodedResp, streamAggResp)

			// Also compare independent client-simulator next-turn assistant reconstruction.
			nextFromStream, err := testkit.SimulateChatNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseFrames:           streamFrames,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)
			nextFromBuf, err := testkit.SimulateChatNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseBody:             bufBytes,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)

			diffs, err := testkit.JSONDiff(nextFromBuf, nextFromStream)
			require.NoError(t, err)
			require.Empty(t, diffs, "SimulateChatNextTurn diff on %s:\n%s", fix.Name, testkit.FormatJSONDiff(diffs))
		})
	}
}

// TestLawS6_CrossGroupAndMultiSegmentParity verifies streaming and buffered
// Chat encoding parity on valid canonical event streams whose parts cross Chat's
// [reasoning, text, refusal, tool_calls] field groups. Client-reconstructed next
// turns must match byte-for-byte while both paths pass VerifyEssence.
func TestLawS6_CrossGroupAndMultiSegmentParity(t *testing.T) {
	incTrue := true
	prevReq := []byte(`{"model":"gpt-4o","messages":[{"role":"user","content":"inspect cluster"}]}`)

	cases := []struct {
		name   string
		events []ir.Event
	}{
		{
			name: "tool_then_multi_segment_reasoning_then_text_then_tool",
			events: []ir.Event{
				{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{Model: "claude-sonnet-4-5", ServiceTier: "default"}},
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
							Token:    &ir.ReplayToken{Scheme: ir.SchemeAnthropic, Data: "sig_1"},
						},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 2,
					Part:  &ir.Part{Kind: ir.PartText},
				},
				{Kind: ir.EventPartDelta, Index: 2, Delta: &ir.Delta{Text: "Dispatching secondary probe."}},
				{
					Kind:  ir.EventPartEnd,
					Index: 2,
					Part:  &ir.Part{Kind: ir.PartText, ID: "msg_txt_2", Text: "Dispatching secondary probe."},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 3,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_explicit_2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Namespace: "k8s", Name: "list_pods"}}},
				},
				{
					Kind:  ir.EventPartEnd,
					Index: 3,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_explicit_2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Namespace: "k8s", Name: "list_pods"}, Arguments: ""}},
				},
				{Kind: ir.EventUsage, Usage: &ir.Usage{Input: 80, CacheRead: 20, Output: 35, Reasoning: 12}},
				{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishToolCalls}},
			},
		},
		{
			name: "refusal_then_reasoning_cross_group",
			events: []ir.Event{
				{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "chatcmpl_ref_cross", Model: "gpt-4o", CreatedAt: 1760000100}},
				{Kind: ir.EventPartStart, Index: 0, Part: &ir.Part{Kind: ir.PartRefusal}},
				{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Text: "Request declined by policy."}},
				{Kind: ir.EventPartEnd, Index: 0, Part: &ir.Part{Kind: ir.PartRefusal, Text: "Request declined by policy."}},
				{Kind: ir.EventPartStart, Index: 1, Part: &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{}}},
				{Kind: ir.EventPartDelta, Index: 1, Delta: &ir.Delta{Text: "Evaluated safety boundary."}},
				{Kind: ir.EventPartEnd, Index: 1, Part: &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Segments: []string{"Evaluated safety boundary."}}}},
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
				Model:   "gpt-4o",
				Request: &ir.Request{IncludeUsage: &incTrue},
				Runtime: newDeterministicRuntime(),
			}, tc.events)
			require.Empty(t, testkit.ValidateChatStream(streamFrames, testkit.GrammarOptions{}))

			bufBytes, bufLosses, err := EncodeResponse(srcResp, ir.ClientView{
				Model:   "gpt-4o",
				Request: &ir.Request{IncludeUsage: &incTrue},
				Runtime: newDeterministicRuntime(),
			})
			require.NoError(t, err)
			require.Contains(t, bufLosses, ir.Loss{Kind: ir.LossPartsReordered, Path: ""})

			nextFromStream, err := testkit.SimulateChatNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseFrames:           streamFrames,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)
			nextFromBuf, err := testkit.SimulateChatNextTurn(testkit.TurnInput{
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
			streamEssenceDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(streamDecoded), streamLosses)
			require.NoError(t, err)
			require.Empty(t, streamEssenceDiffs, testkit.FormatJSONDiff(streamEssenceDiffs))
		})
	}
}

// TestLawS7_StreamTruncationAbortsValidly truncates every Chat wire stream
// fixture at every raw SSE frame prefix and every non-terminal IR event prefix.
// Closing with a transport error must always satisfy ir.ValidateStream and
// testkit.ValidateChatStream in AllowAbnormal mode without panicking.
func TestLawS7_StreamTruncationAbortsValidly(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureStream)
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
					Model:   "gpt-4o",
					Runtime: newDeterministicRuntime(),
				}, events)
				violations := testkit.ValidateChatStream(outFrames, testkit.GrammarOptions{AllowAbnormal: true})
				require.Empty(t, violations, "raw frame cut=%d grammar violations: %v", cut, violations)
			}

			// 2. Truncate at every non-terminal IR event boundary 0..len(fullEvents)-1.
			fullEvents := decodeFullSSEStream(t, fix.Data)
			for cut := range fullEvents {
				if fullEvents[cut].Kind.IsTerminal() {
					break
				}
				enc := NewStreamEncoder(ir.ClientView{
					Model:   "gpt-4o",
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

				violations := testkit.ValidateChatStream(outFrames, testkit.GrammarOptions{AllowAbnormal: true})
				require.Empty(t, violations, "event cut=%d grammar violations: %v", cut, violations)
			}
		})
	}
}

// TestChatFixturesIRGolden compares decoded IR snapshots of all Chat wire
// request, response, and stream fixtures plus recorded pi Chat corpus requests.
// Snapshot drift against testdata/golden/ir/ fails the test unless updated.
func TestChatFixturesIRGolden(t *testing.T) {
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureRequest)
	require.NoError(t, err)
	for _, fix := range wireReqs {
		t.Run("wire_request/"+fix.Name, func(t *testing.T) {
			req, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/wire/request/"+fix.Name+".json", req)
		})
	}

	wireResps, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureResponse)
	require.NoError(t, err)
	for _, fix := range wireResps {
		t.Run("wire_response/"+fix.Name, func(t *testing.T) {
			resp, decErr := DecodeResponse(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/wire/response/"+fix.Name+".json", resp)
		})
	}

	wireStreams, err := testkit.WireFixtures(protocol.ProtocolOpenAIChatCompletions, testkit.FixtureStream)
	require.NoError(t, err)
	for _, fix := range wireStreams {
		t.Run("wire_stream/"+fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			testkit.GoldenJSON(t, "ir/wire/stream/"+fix.Name+".json", events)
		})
	}

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolOpenAIChatCompletions)
	require.NoError(t, err)
	for _, fix := range corpusReqs {
		t.Run("corpus_request/"+fix.Name, func(t *testing.T) {
			req, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/corpus/"+fix.Name+".json", req)
		})
	}
}
