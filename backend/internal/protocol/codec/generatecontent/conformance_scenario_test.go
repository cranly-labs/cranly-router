package generatecontent

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
// source scan. It also checks decoded IR golden snapshots across wire and
// recorded pi Gemini fixtures covering wire.go, schema.go, request_decode.go,
// request_encode.go, legalize.go, response.go, stream_decode.go,
// stream_encode.go, usage.go, and finish.go.

// allGeminiRequestFixtures loads every hand-written wire Gemini GenerateContent
// request fixture and every recorded pi Gemini corpus request fixture in
// deterministic order. Both sets must be non-empty so conformance laws cover
// synthetic and live recorded client payloads.
func allGeminiRequestFixtures(t *testing.T) []testkit.Fixture {
	t.Helper()
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureRequest)
	require.NoError(t, err)
	require.NotEmpty(t, wireReqs)

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolGeminiGenerateContent)
	require.NoError(t, err)
	require.NotEmpty(t, corpusReqs, "recorded pi Gemini corpus fixtures must be present")

	combined := make([]testkit.Fixture, 0, len(wireReqs)+len(corpusReqs))
	combined = append(combined, wireReqs...)
	combined = append(combined, corpusReqs...)
	return combined
}

// TestNoDirectExtraMembersIndexing scans all handwritten production Go files in
// codec/generatecontent for direct Extra.Members map indexing. Section 3.4
// anti-abuse rule 1 forbids codecs from reading vendor extension keys by name in
// production paths.
func TestNoDirectExtraMembersIndexing(t *testing.T) {
	sources, err := testkit.ReadGoProductionSources(".")
	require.NoError(t, err)
	require.Len(t, sources, 10, "expected all 10 codec/generatecontent production Go files")

	for _, src := range sources {
		if strings.Contains(string(src.Data), ".Members[") {
			t.Fatalf("%s contains forbidden direct .Members[ map indexing", src.Path)
		}
	}
}

// TestLawQ1_RequestFixturesDecodeAndValidate checks that every Gemini wire and
// recorded client corpus request fixture decodes without error. Every decoded
// ir.Request must satisfy all structural invariants enforced by ir.Validate.
func TestLawQ1_RequestFixturesDecodeAndValidate(t *testing.T) {
	var codec Codec
	for _, fix := range allGeminiRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			req, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)
			require.NoError(t, ir.Validate(req))
		})
	}
}

// TestLawQ2_RequestRoundTripFixedPoint checks that every strict-valid Gemini
// wire and corpus request fixture incurs zero Loss under EncodeRequest and passes
// strict Gemini request validation. Re-decoding and re-encoding must satisfy
// both Dec(Enc(Dec(x))) == Dec(x) and Enc(Dec(y)) == y byte-for-byte.
func TestLawQ2_RequestRoundTripFixedPoint(t *testing.T) {
	var codec Codec
	for _, fix := range allGeminiRequestFixtures(t) {
		t.Run(fix.Path, func(t *testing.T) {
			dec1, err := codec.DecodeRequest(fix.Data)
			require.NoError(t, err)

			y, losses, err := codec.EncodeRequest(dec1, ir.Dialect{})
			require.NoError(t, err)
			require.Empty(t, losses, "same-protocol Gemini round trip must incur zero Loss")

			violations := testkit.ValidateGeminiRequest(y, testkit.RequestValidationOptions{})
			require.Empty(t, violations, "encoded Gemini request violations: %v", violations)

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
// on the same decoded request across Extra maps, systemInstruction, and tool
// declarations. Every invocation must produce byte-for-byte identical JSON output.
func TestLawQ5_RequestEncodingDeterministic(t *testing.T) {
	var codec Codec
	for _, fix := range allGeminiRequestFixtures(t) {
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

// TestLawS1_StreamFixturesDecodeAndValidate feeds every Gemini wire SSE stream
// fixture through sse.NewReader and StreamDecoder. Each resulting event sequence
// must satisfy ir.ValidateStream without lifecycle or sparse-usage violations.
func TestLawS1_StreamFixturesDecodeAndValidate(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureStream)
	require.NoError(t, err)
	require.NotEmpty(t, streams)

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			require.NoError(t, ir.ValidateStream(events))
		})
	}
}

// TestLawS2_StreamAndBufferedResponseParity compares paired Gemini stream and
// buffered JSON response fixtures across all 29 shared scenarios. Folding the
// decoded stream via ir.Accumulate must match DecodeResponse in ResponseEssence,
// Finish, Usage, ID, Model, CreatedAt, and ServiceTier.
func TestLawS2_StreamAndBufferedResponseParity(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureStream)
	require.NoError(t, err)
	responses, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureResponse)
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

// TestLawS3_StreamEncodePassesGrammar encodes every decoded Gemini stream
// fixture with StreamEncoder and validates the emitted frames via
// testkit.ValidateGeminiStream. Normal fixtures must pass strict mode, while
// stream_truncated and stream_error_event pass AllowAbnormal mode.
func TestLawS3_StreamEncodePassesGrammar(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureStream)
	require.NoError(t, err)

	for _, fix := range streams {
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			view := ir.ClientView{
				Model:   "gemini-2.5-pro",
				Runtime: newDeterministicRuntime(),
			}
			frames, _ := encodeFullEventStream(t, view, events)

			abnormal := fix.Scenario == "stream_truncated" || fix.Scenario == "stream_error_event"
			violations := testkit.ValidateGeminiStream(frames, testkit.GrammarOptions{
				AllowAbnormal: abnormal,
			})
			require.Empty(t, violations, "ValidateGeminiStream violations on %s: %v", fix.Path, violations)
		})
	}
}

// TestLawS6_StreamAndBufferedEncodeParity encodes every normal Gemini stream
// fixture through both StreamEncoder and buffered EncodeResponse. Re-decoding
// both outputs and running SimulateGeminiNextTurn must produce identical
// responses, identical Loss records, and zero JSONDiff on non-empty replies.
func TestLawS6_StreamAndBufferedEncodeParity(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureStream)
	require.NoError(t, err)

	prevReq := []byte(`{"contents":[{"role":"user","parts":[{"text":"prompt"}]}],"generationConfig":{"maxOutputTokens":256}}`)

	for _, fix := range streams {
		if fix.Scenario == "stream_truncated" || fix.Scenario == "stream_error_event" {
			continue
		}
		t.Run(fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			srcResp, err := ir.Accumulate(events)
			require.NoError(t, err)

			streamFrames, streamLosses := encodeFullEventStream(t, ir.ClientView{
				Model:   "gemini-2.5-pro",
				Runtime: newDeterministicRuntime(),
			}, events)

			bufBytes, bufLosses, err := EncodeResponse(srcResp, ir.ClientView{
				Model:   "gemini-2.5-pro",
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

			nextFromStream, err := testkit.SimulateGeminiNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseFrames:           streamFrames,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)
			nextFromBuf, err := testkit.SimulateGeminiNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseBody:             bufBytes,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)

			diffs, err := testkit.JSONDiff(nextFromBuf, nextFromStream)
			require.NoError(t, err)
			require.Empty(t, diffs, "SimulateGeminiNextTurn diff on %s:\n%s", fix.Name, testkit.FormatJSONDiff(diffs))
		})
	}
}

// TestLawS6_CrossGroupAndDroppedPartsParity verifies streaming and buffered
// Gemini encoding parity on canonical event streams mixing cross-group tool
// calls, multi-segment reasoning, refusals, citations, missing call IDs, media,
// and dropped foreign opaque parts. Both paths must produce identical Loss
// slices, identical re-decoded IR responses, and zero VerifyEssence diffs.
func TestLawS6_CrossGroupAndDroppedPartsParity(t *testing.T) {
	prevReq := []byte(`{"contents":[{"role":"user","parts":[{"text":"inspect cluster"}]}],"generationConfig":{"maxOutputTokens":512}}`)

	cases := []struct {
		name   string
		events []ir.Event
	}{
		{
			name: "tool_then_dropped_opaque_then_multi_segment_reasoning_then_text_then_media_then_custom_tool",
			events: []ir.Event{
				{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "gem_cross_1", Model: "gpt-5"}},
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
					Part:  &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Summary: true}},
				},
				{Kind: ir.EventPartDelta, Index: 2, Delta: &ir.Delta{Text: "Checking node health. "}},
				{Kind: ir.EventPartDelta, Index: 2, Delta: &ir.Delta{Text: "Checking pod restarts."}},
				{
					Kind:  ir.EventPartEnd,
					Index: 2,
					Part: &ir.Part{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Segments: []string{"Checking node health. ", "Checking pod restarts."},
							Summary:  true,
							Token:    &ir.ReplayToken{Data: "sig_think_2"},
						},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 3,
					Part:  &ir.Part{Kind: ir.PartText},
				},
				{Kind: ir.EventPartDelta, Index: 3, Delta: &ir.Delta{Text: "Dispatching secondary probe."}},
				{
					Kind:  ir.EventPartEnd,
					Index: 3,
					Part: &ir.Part{
						Kind:        ir.PartText,
						ID:          "msg_txt_3",
						Text:        "Dispatching secondary probe.",
						Annotations: []ir.Annotation{{Kind: "url_citation", URL: "https://go.dev"}},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 4,
					Part:  &ir.Part{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, Source: ir.MediaSourceURL, MIME: "image/png"}},
				},
				{
					Kind:  ir.EventPartEnd,
					Index: 4,
					Part:  &ir.Part{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, Source: ir.MediaSourceURL, MIME: "image/png", Data: "https://example.com/a.png"}},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 5,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_custom_5", Tool: ir.ToolRef{Kind: ir.ToolCustom, Namespace: "k8s", Name: "apply_patch"}}},
				},
				{Kind: ir.EventPartDelta, Index: 5, Delta: &ir.Delta{Arguments: "*** Begin Patch\n+ok"}},
				{
					Kind:  ir.EventPartEnd,
					Index: 5,
					Part:  &ir.Part{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "call_custom_5", Tool: ir.ToolRef{Kind: ir.ToolCustom, Namespace: "k8s", Name: "apply_patch"}, Arguments: "*** Begin Patch\n+ok"}},
				},
				{Kind: ir.EventUsage, Usage: &ir.Usage{Input: 80, CacheRead: 20, Output: 35, Reasoning: 10}},
				{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishToolCalls}},
			},
		},
		{
			name: "refusal_then_signed_redacted_and_tokenless_redacted_thinking",
			events: []ir.Event{
				{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "gem_ref_cross", Model: "gemini-2.5-pro"}},
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
					Part: &ir.Part{
						Kind: ir.PartReasoning,
						Reasoning: &ir.Reasoning{
							Redacted: true,
							Token:    &ir.ReplayToken{Scheme: ir.SchemeAntigravityClaude, Data: "CpEBRedactedBareSignature"},
						},
					},
				},
				{
					Kind:  ir.EventPartStart,
					Index: 2,
					Part:  &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Redacted: true}},
				},
				{
					Kind:  ir.EventPartEnd,
					Index: 2,
					Part:  &ir.Part{Kind: ir.PartReasoning, Reasoning: &ir.Reasoning{Redacted: true}},
				},
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
				Model:   "gemini-2.5-pro",
				Runtime: newDeterministicRuntime(),
			}, tc.events)
			require.Empty(t, testkit.ValidateGeminiStream(streamFrames, testkit.GrammarOptions{}))

			bufBytes, bufLosses, err := EncodeResponse(srcResp, ir.ClientView{
				Model:   "gemini-2.5-pro",
				Runtime: newDeterministicRuntime(),
			})
			require.NoError(t, err)
			require.Equal(t, bufLosses, streamLosses)

			nextFromStream, err := testkit.SimulateGeminiNextTurn(testkit.TurnInput{
				PrevRequest:              prevReq,
				ResponseFrames:           streamFrames,
				SynthesizeMissingOutputs: true,
			})
			require.NoError(t, err)
			nextFromBuf, err := testkit.SimulateGeminiNextTurn(testkit.TurnInput{
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

			bufEssenceDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(bufDecoded), bufLosses)
			require.NoError(t, err)
			require.Empty(t, bufEssenceDiffs, testkit.FormatJSONDiff(bufEssenceDiffs))

			streamEssenceDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(streamDecoded), streamLosses)
			require.NoError(t, err)
			require.Empty(t, streamEssenceDiffs, testkit.FormatJSONDiff(streamEssenceDiffs))
		})
	}
}

// TestLawS7_StreamTruncationAbortsValidly truncates every Gemini wire stream
// fixture at every raw SSE frame prefix and every non-terminal IR event prefix.
// Closing with a transport error must always satisfy ir.ValidateStream and
// testkit.ValidateGeminiStream in AllowAbnormal mode without panicking.
func TestLawS7_StreamTruncationAbortsValidly(t *testing.T) {
	streams, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureStream)
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
					Model:   "gemini-2.5-pro",
					Runtime: newDeterministicRuntime(),
				}, events)
				violations := testkit.ValidateGeminiStream(outFrames, testkit.GrammarOptions{AllowAbnormal: true})
				require.Empty(t, violations, "raw frame cut=%d grammar violations: %v", cut, violations)
			}

			// 2. Truncate at every non-terminal IR event boundary 0..len(fullEvents)-1.
			fullEvents := decodeFullSSEStream(t, fix.Data)
			for cut := range fullEvents {
				if fullEvents[cut].Kind.IsTerminal() {
					break
				}
				enc := NewStreamEncoder(ir.ClientView{
					Model:   "gemini-2.5-pro",
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

				violations := testkit.ValidateGeminiStream(outFrames, testkit.GrammarOptions{AllowAbnormal: true})
				require.Empty(t, violations, "event cut=%d grammar violations: %v", cut, violations)
			}
		})
	}
}

// TestGeminiFixturesIRGolden compares decoded IR snapshots of all Gemini wire
// request, response, and stream fixtures plus recorded pi Gemini corpus requests
// against testdata/golden/ir/. Any unreviewed change to decoded IR field values
// or event sequences fails until golden snapshots are explicitly updated.
func TestGeminiFixturesIRGolden(t *testing.T) {
	wireReqs, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureRequest)
	require.NoError(t, err)
	for _, fix := range wireReqs {
		t.Run("wire_request/"+fix.Name, func(t *testing.T) {
			req, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/wire/request/"+fix.Name+".json", req)
		})
	}

	wireResps, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureResponse)
	require.NoError(t, err)
	for _, fix := range wireResps {
		t.Run("wire_response/"+fix.Name, func(t *testing.T) {
			resp, decErr := DecodeResponse(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/wire/response/"+fix.Name+".json", resp)
		})
	}

	wireStreams, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureStream)
	require.NoError(t, err)
	for _, fix := range wireStreams {
		t.Run("wire_stream/"+fix.Name, func(t *testing.T) {
			events := decodeFullSSEStream(t, fix.Data)
			testkit.GoldenJSON(t, "ir/wire/stream/"+fix.Name+".json", events)
		})
	}

	corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolGeminiGenerateContent)
	require.NoError(t, err)
	for _, fix := range corpusReqs {
		t.Run("corpus_request/"+fix.Name, func(t *testing.T) {
			req, decErr := DecodeRequest(fix.Data)
			require.NoError(t, decErr)
			testkit.GoldenJSON(t, "ir/corpus/"+fix.Name+".json", req)
		})
	}
}
