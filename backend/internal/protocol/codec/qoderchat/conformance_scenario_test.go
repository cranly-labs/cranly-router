package qoderchat_test

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/qoderchat"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// conformance_scenario_test.go verifies T2 conformance laws (Q4, Q5, S1, S6,
// and S7) and section 3.4/3.12 source invariants across wire.go,
// request_encode.go, tool_text.go, and stream_decode.go. Saved IR request
// goldens from Messages, Responses, and Chat codecs combine with wrapped Qoder
// SSE stream fixtures to test chunk-split invariance, buffered parity, and
// frame-prefix truncation handling.

// conformanceRequestCase pairs a synthetic or edge-case IR request with expected Q4/Q5 behavior.
type conformanceRequestCase struct {
	name           string      // subtest scenario name
	req            *ir.Request // input canonical IR request
	dialect        ir.Dialect  // target Qoder dialect flags
	wantRequestErr bool        // true when *qoder.RequestError is expected
}

// conformanceStreamFixture pairs a wrapped Qoder SSE byte stream with an optional buffered JSON form.
type conformanceStreamFixture struct {
	name        string // stable fixture scenario label
	sseBytes    []byte // multi-frame wrapped Qoder SSE stream bytes
	wrappedJSON []byte // optional single-envelope buffered JSON equivalent
}

// chunkedByteReader returns at most step bytes per Read call to exercise arbitrary SSE chunk splits.
type chunkedByteReader struct {
	data   []byte // underlying raw SSE byte slice
	offset int    // current read cursor into data
	step   int    // maximum bytes copied per Read call
}

// Read copies up to r.step bytes from r.data into p and returns io.EOF once all
// buffered bytes have been consumed. Zero or negative step values fall back to
// single-byte reads so callers never stall on progress.
func (r *chunkedByteReader) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	step := max(r.step, 1)
	n := min(len(p), step, len(r.data)-r.offset)
	copy(p, r.data[r.offset:r.offset+n])
	r.offset += n
	return n, nil
}

// TestNoDirectExtraMembersIndexingAndForbiddenLibs scans all handwritten
// production Go files in codec/qoderchat and protocol/qoder via testkit. It
// verifies the exact five-file count and confirms that zero files index
// Extra.Members directly or import encoding/json v1, gjson, or sjson.
func TestNoDirectExtraMembersIndexingAndForbiddenLibs(t *testing.T) {
	sources, err := testkit.ReadGoProductionSources(".", "../../qoder")
	require.NoError(t, err)
	require.Len(t, sources, 5, "expected 4 codec/qoderchat + 1 protocol/qoder production files")

	for _, src := range sources {
		text := string(src.Data)
		if strings.Contains(text, ".Members[") {
			t.Fatalf("%s contains forbidden direct .Members[ map indexing", src.Path)
		}
		if strings.Contains(text, `"github.com/tidwall/gjson"`) ||
			strings.Contains(text, `"github.com/tidwall/sjson"`) {
			t.Fatalf("%s imports forbidden gjson or sjson package", src.Path)
		}
		if strings.Contains(text, "\"encoding/json\"\n") {
			t.Fatalf("%s imports encoding/json v1 instead of encoding/json/v2", src.Path)
		}
	}
}

// TestLawsQ4AndQ5_SavedIRRequestGoldens loads every saved wire and recorded
// client corpus IR request golden across Anthropic Messages, OpenAI Responses,
// and OpenAI Chat Completions alongside synthetic multi-feature cases. It
// verifies Law Q5 byte determinism and Law Q4 VerifyEssence equivalence on
// qoderchat.EncodeRequest and qoderchat.DecodePayloadRequest.
func TestLawsQ4AndQ5_SavedIRRequestGoldens(t *testing.T) {
	// protocolGoldenGroup binds one client protocol's saved IR golden folders to its Qoder stateless mode.
	type protocolGoldenGroup struct {
		name               string   // subtest protocol group label
		dirs               []string // relative saved IR request golden directories
		statelessResponses bool     // whether Responses stateless gates apply
	}
	groups := []protocolGoldenGroup{
		{
			name: "anthropic_messages",
			dirs: []string{
				"../messages/testdata/golden/ir/wire/request",
				"../messages/testdata/golden/ir/corpus",
			},
			statelessResponses: false,
		},
		{
			name: "openai_responses",
			dirs: []string{
				"../responses/testdata/golden/ir/wire/request",
				"../responses/testdata/golden/ir/corpus",
			},
			statelessResponses: true,
		},
		{
			name: "openai_chat_completions",
			dirs: []string{
				"../chat/testdata/golden/ir/wire/request",
				"../chat/testdata/golden/ir/corpus",
			},
			statelessResponses: false,
		},
	}

	for _, grp := range groups {
		t.Run(grp.name, func(t *testing.T) {
			files, err := testkit.ReadJSONFixtures(grp.dirs...)
			require.NoError(t, err)
			require.NotEmpty(t, files)

			dialect := ir.Dialect{
				AllowBillingHeader: true,
				ToolNameMaxBytes:   64,
				StatelessResponses: grp.statelessResponses,
			}
			for _, f := range files {
				t.Run(f.Path, func(t *testing.T) {
					var srcReq ir.Request
					require.NoError(t, json.Unmarshal(f.Data, &srcReq, jsontext.AllowInvalidUTF8(true)))

					enc1, losses1, err := qoderchat.EncodeRequest(&srcReq, dialect)
					require.NoError(t, err)
					for range 2 {
						encNext, _, nextErr := qoderchat.EncodeRequest(&srcReq, dialect)
						require.NoError(t, nextErr)
						require.True(t, bytes.Equal(enc1, encNext), "Law Q5 determinism violated")
					}

					dstIR, err := qoderchat.DecodePayloadRequest(enc1)
					require.NoError(t, err)
					diffs, err := testkit.VerifyEssence(ir.Essence(&srcReq), ir.Essence(dstIR), losses1)
					require.NoError(t, err)
					require.Empty(t, diffs, "Law Q4 essence mismatch:\n%s", testkit.FormatJSONDiff(diffs))
				})
			}
		})
	}

	t.Run("synthetic_edge_cases", func(t *testing.T) {
		for _, fx := range buildConformanceRequests() {
			t.Run(fx.name, func(t *testing.T) {
				enc1, losses1, err := qoderchat.EncodeRequest(fx.req, fx.dialect)
				if fx.wantRequestErr {
					require.Error(t, err)
					require.True(t, qoder.IsRequestError(err))
					return
				}
				require.NoError(t, err)

				enc2, _, err := qoderchat.EncodeRequest(fx.req, fx.dialect)
				require.NoError(t, err)
				require.Equal(t, enc1, enc2, "Law Q5 determinism violated")

				dstIR, err := qoderchat.DecodePayloadRequest(enc1)
				require.NoError(t, err)
				diffs, err := testkit.VerifyEssence(ir.Essence(fx.req), ir.Essence(dstIR), losses1)
				require.NoError(t, err)
				require.Empty(t, diffs, "Law Q4 essence mismatch:\n%s", testkit.FormatJSONDiff(diffs))
			})
		}
	})
}

// TestLawS1_StreamFixturesDecodeAndChunkSplitInvariance feeds wrapped Qoder SSE
// streams covering choice deltas, Anthropic envelopes, XML/JSON/DSML text tool
// calls, parallel slot drift, and sparse cache usage into StreamDecoder as a
// whole buffer and across 1, 3, and 13-byte chunk splits. It verifies that
// every run satisfies ir.ValidateStream and yields identical accumulated responses.
func TestLawS1_StreamFixturesDecodeAndChunkSplitInvariance(t *testing.T) {
	for _, fx := range buildConformanceStreams() {
		t.Run(fx.name, func(t *testing.T) {
			wholeEvents, wholeResp := decodeStreamReader(t, bytes.NewReader(fx.sseBytes))
			require.NoError(t, ir.ValidateStream(wholeEvents))
			require.NoError(t, ir.ValidateResponse(wholeResp))

			for _, step := range []int{1, 3, 13} {
				splitEvents, splitResp := decodeStreamReader(t, &chunkedByteReader{
					data: fx.sseBytes,
					step: step,
				})
				require.NoError(t, ir.ValidateStream(splitEvents), "step=%d", step)
				require.NoError(t, ir.ValidateResponse(splitResp), "step=%d", step)
				require.Equal(t, ir.ResponseEssence(wholeResp), ir.ResponseEssence(splitResp), "step=%d", step)
				require.Equal(t, wholeResp.Finish, splitResp.Finish, "step=%d", step)
				require.Equal(t, wholeResp.Usage, splitResp.Usage, "step=%d", step)
			}
		})
	}
}

// TestLawS6_StreamAndBufferedResponseParity compares accumulated StreamDecoder
// output against qoderchat.DecodeResponse across multi-frame SSE streams and
// single-envelope JSON wrappers. It verifies identical ResponseEssence, Finish,
// and Usage projections between the streaming and buffered paths.
func TestLawS6_StreamAndBufferedResponseParity(t *testing.T) {
	var codec qoderchat.Codec
	for _, fx := range buildConformanceStreams() {
		t.Run(fx.name, func(t *testing.T) {
			_, streamResp := decodeStreamReader(t, bytes.NewReader(fx.sseBytes))

			bufFromSSE, err := codec.DecodeResponse(fx.sseBytes)
			require.NoError(t, err)
			require.NoError(t, ir.ValidateResponse(bufFromSSE))
			require.Equal(t, ir.ResponseEssence(streamResp), ir.ResponseEssence(bufFromSSE))
			require.Equal(t, streamResp.Finish, bufFromSSE.Finish)
			require.Equal(t, streamResp.Usage, bufFromSSE.Usage)

			if len(fx.wrappedJSON) > 0 {
				bufFromJSON, err := codec.DecodeResponse(fx.wrappedJSON)
				require.NoError(t, err)
				require.NoError(t, ir.ValidateResponse(bufFromJSON))
				require.Equal(t, ir.ResponseEssence(streamResp), ir.ResponseEssence(bufFromJSON))
				require.Equal(t, streamResp.Finish, bufFromJSON.Finish)
				require.Equal(t, streamResp.Usage, bufFromJSON.Usage)
			}
		})
	}
}

// TestLawS7_StreamTruncationAbortsValidly truncates every wrapped Qoder SSE
// stream fixture at each frame prefix preceding its terminal marker and closes
// StreamDecoder with both io.ErrUnexpectedEOF and nil causes. It verifies that
// every truncated prefix satisfies ir.ValidateStream and ends with EventError.
func TestLawS7_StreamTruncationAbortsValidly(t *testing.T) {
	for _, fx := range buildConformanceStreams() {
		t.Run(fx.name, func(t *testing.T) {
			frames, err := testkit.ParseSSEBytes(fx.sseBytes)
			require.NoError(t, err)
			require.GreaterOrEqual(t, len(frames), 2)

			// Stop one frame before the terminal [DONE] or message_stop marker so
			// every prefix represents an incomplete upstream stream.
			for cut := range len(frames) - 1 {
				for _, cause := range []error{io.ErrUnexpectedEOF, nil} {
					dec := qoderchat.NewStreamDecoder()
					var events []ir.Event
					var streamErr error
					for _, f := range frames[:cut] {
						batch, decErr := dec.Decode(f)
						if decErr != nil {
							streamErr = decErr
							break
						}
						events = append(events, batch...)
					}
					if streamErr == nil {
						streamErr = cause
					}
					events = append(events, dec.Close(streamErr)...)
					require.NoError(t, ir.ValidateStream(events), "cut=%d cause=%v", cut, cause)
					require.Equal(t, ir.EventError, events[len(events)-1].Kind, "cut=%d cause=%v", cut, cause)
				}
			}
		})
	}
}

// decodeStreamReader scans r through sse.NewReader and qoderchat.StreamDecoder,
// returning the full emitted event slice alongside its accumulated *ir.Response.
// Unexpected frame decode or reader errors fail t immediately.
func decodeStreamReader(t *testing.T, r io.Reader) ([]ir.Event, *ir.Response) {
	t.Helper()
	reader := sse.NewReader(r)
	dec := qoderchat.NewStreamDecoder()
	var events []ir.Event
	for reader.Scan() {
		batch, err := dec.Decode(reader.Frame())
		require.NoError(t, err)
		events = append(events, batch...)
	}
	require.NoError(t, reader.Err())
	events = append(events, dec.Close(nil)...)

	var acc ir.Accumulator
	acc.Add(events...)
	require.Nil(t, acc.StreamError())
	return events, acc.Response()
}

// wrapSSEInners formats inner Qoder JSON strings as wrapped SSE data lines
// followed by a terminal frame. Callers pass appendDone=true to append a
// wrapped [DONE] sentinel when inners do not end with message_stop.
func wrapSSEInners(inners []string, appendDone bool) []byte {
	var buf bytes.Buffer
	for _, inner := range inners {
		fmt.Fprintf(&buf, "data: {\"body\":%q}\n\n", inner)
	}
	if appendDone {
		_, _ = buf.WriteString("data: {\"body\":\"[DONE]\"}\n\n")
	}
	return buf.Bytes()
}

// buildConformanceStreams constructs wrapped Qoder SSE stream fixtures spanning
// choice deltas, Anthropic-style envelopes, XML/JSON/DSML text tool calls,
// parallel index reuse/drift, and sparse cache-hit token accounting. Repeated
// calls return fresh deterministic byte slices for S1, S6, and S7 law checks.
func buildConformanceStreams() []conformanceStreamFixture {
	return []conformanceStreamFixture{
		{
			name: "choice_deltas_parallel_tools_and_cache_usage",
			sseBytes: wrapSSEInners([]string{
				`{"choices":[{"delta":{"reasoning_content":"checking "}}]}`,
				`{"choices":[{"delta":{"reasoning_content":"files"}}]}`,
				`{"choices":[{"delta":{"content":"Inspecting repository."}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c_read","function":{"name":"Read","arguments":"{\"path\":"}},{"index":1,"id":"c_bash","function":{"name":"shell","arguments":"{\"cmd\":\"pwd\"}"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"c_read","function":{"arguments":"\"go.mod\"}"}}]}}]}`,
				`{"usage":{"prompt_tokens":120,"completion_tokens":18,"prompt_tokens_details":{"cached_tokens":90,"cacheable_tokens":10},"completion_tokens_details":{"reasoning_tokens":5}}}`,
			}, true),
			wrappedJSON: []byte(`{"statusCodeValue":200,"body":"{\"choices\":[{\"message\":{\"reasoning_content\":\"checking files\",\"content\":\"Inspecting repository.\",\"tool_calls\":[{\"id\":\"c_read\",\"type\":\"function\",\"function\":{\"name\":\"Read\",\"arguments\":\"{\\\"path\\\":\\\"go.mod\\\"}\"}},{\"id\":\"c_bash\",\"type\":\"function\",\"function\":{\"name\":\"Bash\",\"arguments\":\"{\\\"command\\\":\\\"pwd\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}],\"usage\":{\"prompt_tokens\":120,\"completion_tokens\":18,\"prompt_tokens_details\":{\"cached_tokens\":90,\"cacheable_tokens\":10},\"completion_tokens_details\":{\"reasoning_tokens\":5}}}"}`),
		},
		{
			name: "anthropic_envelopes_placeholder_and_slot_drift",
			sseBytes: wrapSSEInners([]string{
				`{"event":"content_block_delta","data":{"type":"thinking_delta","text":"step one"}}`,
				`{"event":"content_block_delta","data":{"type":"text_delta","text":"Opening target."}}`,
				`{"event":"tool_use_start","index":0,"data":{"id":"call_env_1","name":"Read","type":"tool_use"}}`,
				`{"event":"tool_use_delta","index":0,"data":{"id":"call_env_1","arguments":"{}"}}`,
				`{"event":"tool_use_delta","index":4,"data":{"id":"call_env_1","arguments":"{\"file_path\":\"README.md\"}"}}`,
				`{"usage":{"input_tokens":50,"output_tokens":4,"prompt_tokens_details":{"cached_tokens":20}}}`,
				`{"event":"message_delta","data":{"usage":{"output_tokens":9}}}`,
				`{"event":"message_stop"}`,
			}, false),
		},
		{
			name: "text_encoded_xml_json_and_dsml_tool_calls",
			sseBytes: wrapSSEInners([]string{
				`{"choices":[{"delta":{"content":"Lead text <tool_cal"}}]}`,
				`{"choices":[{"delta":{"content":"l>Read<arg_key>path</arg_key><arg_value>a&amp;b.go</arg_value></tool_call> mid "}}]}`,
				`{"choices":[{"delta":{"content":"<tool_call>{\"name\":\"shell\",\"cmd\":\"ls -la\"}</tool_call> and "}}]}`,
				`{"choices":[{"delta":{"content":"<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"Grep\"><｜｜DSML｜｜parameter name=\"pattern\">func </｜｜DSML｜｜parameter></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>"}}]}`,
			}, true),
		},
	}
}

// buildConformanceRequests constructs representative IR requests spanning media,
// citations, refusals, custom/search/namespaced tools, interleaved parallel
// tool replies, and stateless Responses rejections. Callers may encode each
// returned case across multiple Law Q4 and Law Q5 checks without aliasing.
func buildConformanceRequests() []conformanceRequestCase {
	storeTrue := true
	topK := int64(40)
	candCount := int64(2)
	deferTrue := true

	return []conformanceRequestCase{
		{
			name: "multimodal_citations_refusal_and_reordered_parallel_tools",
			dialect: ir.Dialect{
				AllowBillingHeader: true,
				ToolNameMaxBytes:   64,
				StatelessResponses: true,
			},
			req: &ir.Request{
				Model:    "qwen3.8-max",
				Sampling: ir.Sampling{TopK: &topK, CandidateCount: &candCount},
				Output:   &ir.OutputFormat{Kind: "json_object", Verbosity: "low"},
				Namespaces: []ir.Namespace{{
					Name:  "fs",
					Cache: &ir.CacheMark{TTL: "5m"},
				}},
				Tools: []ir.Tool{
					{
						Kind:         ir.ToolFunction,
						Namespace:    "fs",
						Name:         "read_file",
						Description:  "Read file",
						DeferLoading: &deferTrue,
						Cache:        &ir.CacheMark{},
					},
					{
						Kind:        ir.ToolCustom,
						Name:        "apply_patch",
						Description: "Apply patch",
					},
					{
						Kind: ir.ToolClientSearch,
					},
				},
				ToolChoice: &ir.ToolChoice{
					Mode: ir.ToolChoiceAuto,
					AllowedTools: []ir.ToolRef{
						{Kind: ir.ToolFunction, Namespace: "fs", Name: "read_file"},
						{Kind: ir.ToolCustom, Name: "apply_patch"},
					},
				},
				System: []ir.Part{
					{Kind: ir.PartText, ID: "sys_1", Text: "x-anthropic-billing-header: cc_version=2.1; cch=abcde;", Cache: &ir.CacheMark{}},
					{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, Source: ir.MediaSourceURL, Data: "https://example.com/a.png"}},
				},
				Messages: []ir.Message{
					{
						Role: ir.RoleUser,
						ID:   "msg_u1",
						Parts: []ir.Part{
							{
								Kind: ir.PartText,
								ID:   "p_u1",
								Text: "Read two files",
								Annotations: []ir.Annotation{{
									Kind:  "url_citation",
									URL:   "https://example.com",
									Title: "Example",
								}},
								Cache: &ir.CacheMark{TTL: "1h"},
							},
							{
								Kind:  ir.PartMedia,
								Media: &ir.Media{Type: ir.MediaDocument, MIME: "application/pdf", Source: ir.MediaSourceBase64, Data: "JVBERi0="},
							},
						},
					},
					{
						Role: ir.RoleAssistant,
						ID:   "msg_a1",
						Parts: []ir.Part{
							{
								Kind: ir.PartToolCall,
								ID:   "fc_1",
								Call: &ir.ToolCall{
									CallID:    "call_1",
									Tool:      ir.ToolRef{Kind: ir.ToolFunction, Namespace: "fs", Name: "read_file"},
									Arguments: `{"path":"a.txt"}`,
									Token:     &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig1"},
								},
							},
							{
								Kind: ir.PartToolCall,
								ID:   "ctc_2",
								Call: &ir.ToolCall{
									CallID:    "call_2",
									Tool:      ir.ToolRef{Kind: ir.ToolCustom, Name: "apply_patch"},
									Arguments: "*** Begin Patch",
								},
							},
							{
								Kind: ir.PartRefusal,
								Text: "Note: restricted directory skipped.",
							},
						},
					},
					{
						Role: ir.RoleTool,
						Parts: []ir.Part{
							{
								Kind: ir.PartToolResult,
								Result: &ir.ToolResult{
									CallID:  "call_2",
									Content: []ir.Part{{Kind: ir.PartText, Text: "patched"}},
								},
							},
							{
								Kind: ir.PartToolResult,
								Result: &ir.ToolResult{
									CallID: "call_1",
									Content: []ir.Part{
										{Kind: ir.PartText, Text: "contents"},
										{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, MIME: "image/png", Source: ir.MediaSourceBase64, Data: "iVBORw=="}},
									},
									IsError: true,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "hosted_only_tools_approximates_tool_choice",
			req: &ir.Request{
				Model: "qwen3.8-max",
				Tools: []ir.Tool{{
					Kind: ir.ToolHosted,
					Name: "web_search",
					Hosted: &ir.OpaqueItem{
						Origin: protocol.ProtocolAnthropicMessages,
						Type:   "web_search_20250305",
						Raw:    []byte(`{"type":"web_search_20250305","name":"web_search"}`),
					},
				}},
				ToolChoice: &ir.ToolChoice{Mode: ir.ToolChoiceAuto},
				Messages:   []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "search"}}}},
			},
		},
		{
			name:    "non_responses_missing_id_tool_result_bracketed_by_user_text",
			dialect: ir.Dialect{StatelessResponses: false},
			req: &ir.Request{
				Model: "qwen3.8-max",
				Messages: []ir.Message{
					{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "before"}}},
					{
						Role: ir.RoleTool,
						Parts: []ir.Part{{
							Kind: ir.PartToolResult,
							Result: &ir.ToolResult{
								CallID: "",
								Content: []ir.Part{
									{Kind: ir.PartText, Text: "file.txt"},
									{Kind: ir.PartMedia, Media: &ir.Media{Type: ir.MediaImage, MIME: "image/png", Source: ir.MediaSourceBase64, Data: "iVBORw=="}},
								},
							},
						}},
					},
					{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "after"}}},
					{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "next_turn"}}},
				},
			},
		},
		{
			name:           "stateful_store_rejected_in_responses_mode",
			dialect:        ir.Dialect{StatelessResponses: true},
			wantRequestErr: true,
			req: &ir.Request{
				Model:    "qwen3.8-max",
				Store:    &storeTrue,
				Messages: []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "hi"}}}},
			},
		},
	}
}
