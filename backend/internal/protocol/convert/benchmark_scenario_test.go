package convert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// benchmark_scenario_test.go benchmarks cross-protocol request and streaming
// conversion and same-protocol client-tool adaptation across exchange.go,
// stream.go, registry.go, dialect.go, tools_lower.go, tools_raise.go,
// tools_map.go, tools_responses.go, carriers.go, markers.go, and
// output_meaning.go on all six Stage 1 text routes, all three Stage 3 Gemini
// GenerateContent routes, and Stage 5 same-protocol OpenAI Responses
// client-tool adaptation. Each benchmark constructs deterministic wire payloads
// once during setup and reports per-operation latency, byte throughput, and
// heap allocations.
//
// Request benchmarks convert 2 MiB multi-turn client payloads containing system
// prompts, tool schemas, continuity carriers, lowered custom/namespace/search
// tools, and paired tool calls, while streaming benchmarks convert 5,000-frame
// upstream SSE streams carrying reasoning deltas, visible text deltas,
// fragmented or structured tool arguments, and terminal usage.
// TestBenchmarkPayloadsValid runs during normal unit tests so payload sizes,
// frame counts, strict protocol rules, and cross-iteration slice bounds stay
// verified outside benchmark runs.

const (
	// benchMinRequestBytes, benchSerializationMarginBytes, benchStreamFrameCount,
	// and stage5ResponsesRouteSlug govern payload sizing and sub-benchmark naming
	// for the 2 MiB request and 5,000-frame streaming benchmarks.
	benchMinRequestBytes          = 2 * 1024 * 1024                 // minimum 2 MiB serialized client request payload
	benchSerializationMarginBytes = 65536                           // extra turn target keeping marshaled JSON comfortably above 2 MiB
	benchStreamFrameCount         = 5000                            // exact upstream SSE frame count per stream benchmark
	stage5ResponsesRouteSlug      = "responses_same_protocol_tools" // route slug for Stage 5 same-protocol Responses tool adaptation
)

// benchRouteAssets caches the prebuilt 2 MiB client request and 5,000-frame raw
// upstream SSE stream for one cross-protocol route.
type benchRouteAssets struct {
	spec         routeSpec // cross-protocol route configuration and client model
	requestBody  []byte    // >= 2 MiB valid client request JSON payload
	rawStreamSSE []byte    // 5,000-frame valid upstream SSE wire stream
	streamReq    []byte    // compact client request initializing stream state
}

// benchStage5Assets caches the prebuilt 2 MiB client request, 5,000-frame
// upstream Responses SSE stream, and lowered ToolMap for Stage 5 same-protocol
// Responses client-tool adaptation benchmarks.
type benchStage5Assets struct {
	dialect      Dialect  // function-only target Responses dialect
	requestBody  []byte   // >= 2 MiB Responses request mixing client tools and non-tool items
	rawStreamSSE []byte   // 5,000-frame lowered upstream Responses SSE stream
	newToolMap   *ToolMap // precomputed convert ToolMap for stream restoration
}

// TestBenchmarkPayloadsValid builds the 2 MiB request payloads and 5,000-frame
// upstream SSE streams across all six Stage 1 text routes, all three Stage 3
// Gemini routes, and Stage 5 same-protocol Responses client-tool adaptation.
// Every payload must meet its size or frame-count target, convert without error,
// pass strict target request and stream grammar validators, and keep
// per-iteration Exchange and stream slices bounded across repeated runs.
func TestBenchmarkPayloadsValid(t *testing.T) {
	assets := buildAllBenchRouteAssets(t)
	require.Len(t, assets, len(stage1Routes)+len(stage3GeminiRoutes))

	for _, item := range assets {
		t.Run(item.spec.slug, func(t *testing.T) {
			require.GreaterOrEqual(t, len(item.requestBody), benchMinRequestBytes,
				"request payload for %s must be at least 2 MiB", item.spec.slug)

			frameCount := countRawSSEFrames(item.rawStreamSSE)
			require.Equal(t, benchStreamFrameCount, frameCount,
				"upstream stream for %s must contain exactly 5,000 SSE frames", item.spec.slug)

			newReqBytes, err := runNewRequestConversion(item.spec, item.requestBody)
			require.NoError(t, err)
			reqViol := testkit.ValidateRequestRules(item.spec.route.Upstream, newReqBytes, item.spec.validation)
			require.Empty(t, reqViol, "new converted 2 MiB request violated %s rules: %v", item.spec.slug, reqViol)

			ex, err := NewExchange(item.streamReq, Options{
				Route:       item.spec.route,
				ClientModel: item.spec.clientMdl,
				Runtime:     newDeterministicRuntime(),
			})
			require.NoError(t, err)
			ex.ensureUpstreamPrepared()
			reqLossLen := len(ex.requestLosses)

			var newStreamBytes []byte
			for range 2 {
				newStreamBytes, err = runNewStreamConversion(ex, item.rawStreamSSE)
				require.NoError(t, err)
				require.NotEmpty(t, newStreamBytes)
				ex.responseLosses = ex.responseLosses[:0]
				require.Len(t, ex.Losses(), reqLossLen,
					"Exchange.responseLosses must not grow across repeated stream benchmark iterations on %s", item.spec.slug)
			}

			outFrames := parseFramesForValidation(t, newStreamBytes)
			gramViol := testkit.ValidateStreamGrammar(item.spec.route.Client, outFrames, testkit.GrammarOptions{})
			require.Empty(t, gramViol, "new converted 5,000-frame stream violated %s grammar: %v", item.spec.slug, gramViol)
		})
	}

	t.Run(stage5ResponsesRouteSlug, func(t *testing.T) {
		s5 := buildStage5BenchAssets(t)
		require.GreaterOrEqual(t, len(s5.requestBody), benchMinRequestBytes,
			"Stage 5 Responses request payload must be at least 2 MiB")
		require.Equal(t, benchStreamFrameCount, countRawSSEFrames(s5.rawStreamSSE),
			"Stage 5 Responses upstream stream must contain exactly 5,000 SSE frames")

		newReqBytes, newTM, _, err := AdaptResponsesTools(s5.requestBody, s5.dialect, nil)
		require.NoError(t, err)
		require.NotNil(t, newTM)
		require.True(t, newTM.HasRestorations())
		reqViol := testkit.ValidateRequestRules(protocol.ProtocolOpenAIResponses, newReqBytes, testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		})
		require.Empty(t, reqViol, "Stage 5 adapted 2 MiB request violated function-only rules: %v", reqViol)

		var newStreamBytes []byte
		var firstStreamLen int
		for iter := range 2 {
			newStreamBytes, err = runNewStage5StreamAdaptation(s5.newToolMap, s5.rawStreamSSE)
			require.NoError(t, err)
			require.NotEmpty(t, newStreamBytes)
			if iter == 0 {
				firstStreamLen = len(newStreamBytes)
			} else {
				require.Equal(t, firstStreamLen, len(newStreamBytes),
					"Stage 5 restored stream output must remain stable across iterations")
			}
		}

		outFrames := parseFramesForValidation(t, newStreamBytes)
		gramViol := testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, outFrames, testkit.GrammarOptions{})
		require.Empty(t, gramViol, "Stage 5 restored 5,000-frame stream violated Responses grammar: %v", gramViol)

		decodedResp, err := tryDecodeClientStream(protocol.ProtocolOpenAIResponses, outFrames)
		require.NoError(t, err)
		var sawCustom, sawSearch, sawNamespace bool
		for _, p := range decodedResp.Parts {
			if p.Call == nil {
				continue
			}
			switch p.Call.Tool.Kind {
			case ir.ToolCustom:
				sawCustom = p.Call.Tool.Name == "apply_patch"
			case ir.ToolClientSearch:
				sawSearch = true
			case ir.ToolFunction:
				if p.Call.Tool.Namespace == "collaboration" && p.Call.Tool.Name == "spawn_agent" {
					sawNamespace = true
				}
			}
		}
		require.True(t, sawCustom && sawSearch && sawNamespace,
			"Stage 5 5,000-frame stream must restore custom, tool_search, and namespaced function calls")
	})
}

// BenchmarkRequestConversion2MB drives 2 MiB multi-turn client request payloads
// through convert.Exchange across all six Stage 1 text routes and all three
// Stage 3 Gemini routes. Each sub-benchmark reports per-operation latency, byte
// throughput, and heap allocations.
func BenchmarkRequestConversion2MB(b *testing.B) {
	assets := buildAllBenchRouteAssets(b)

	for _, item := range assets {
		b.Run(item.spec.slug+"/new", func(b *testing.B) {
			b.SetBytes(int64(len(item.requestBody)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				out, err := runNewRequestConversion(item.spec, item.requestBody)
				if err != nil || len(out) == 0 {
					b.Fatalf("new request conversion failed on %s: %v", item.spec.slug, err)
				}
			}
		})
	}
}

// BenchmarkStreamConversion5000Frames streams 5,000-frame upstream SSE payloads
// containing reasoning, text, and tool-call deltas through convert.Stream
// across all six Stage 1 text routes and all three Stage 3 Gemini routes. Each
// sub-benchmark measures end-to-end frame scanning, conversion, semantic
// classification, and SSE wire serialization while resetting per-iteration
// response losses on the shared Exchange.
func BenchmarkStreamConversion5000Frames(b *testing.B) {
	assets := buildAllBenchRouteAssets(b)

	for _, item := range assets {
		ex, err := NewExchange(item.streamReq, Options{
			Route:       item.spec.route,
			ClientModel: item.spec.clientMdl,
			Runtime:     newDeterministicRuntime(),
		})
		if err != nil {
			b.Fatalf("NewExchange setup failed for %s: %v", item.spec.slug, err)
		}
		ex.ensureUpstreamPrepared()

		b.Run(item.spec.slug+"/new", func(b *testing.B) {
			b.SetBytes(int64(len(item.rawStreamSSE)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				out, runErr := runNewStreamConversion(ex, item.rawStreamSSE)
				if runErr != nil || len(out) == 0 {
					b.Fatalf("new stream conversion failed on %s: %v", item.spec.slug, runErr)
				}
				// Clear response-side losses appended by stream.Close so repeated
				// benchmark iterations reuse backing capacity without unbounded growth.
				ex.responseLosses = ex.responseLosses[:0]
			}
		})
	}
}

// BenchmarkResponsesToolAdaptation2MB benchmarks Stage 5 lightweight
// same-protocol Responses client-tool lowering (AdaptResponsesTools) on a 2 MiB
// payload mixing custom, namespace, and tool_search items with large non-tool
// turns. It reports per-operation latency, throughput, and heap allocations.
func BenchmarkResponsesToolAdaptation2MB(b *testing.B) {
	s5 := buildStage5BenchAssets(b)

	b.Run(stage5ResponsesRouteSlug+"/new", func(b *testing.B) {
		b.SetBytes(int64(len(s5.requestBody)))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			out, tm, _, err := AdaptResponsesTools(s5.requestBody, s5.dialect, nil)
			if err != nil || len(out) == 0 || tm == nil {
				b.Fatalf("AdaptResponsesTools failed: %v", err)
			}
		}
	})
}

// BenchmarkResponsesToolStream5000Frames benchmarks Stage 5
// NewResponsesToolStream on a 5,000-frame upstream Responses SSE stream carrying
// reasoning, message text, and lowered custom, tool_search, and namespace calls.
// It measures end-to-end SSE scanning, restoration, semantic classification, and
// wire serialization.
func BenchmarkResponsesToolStream5000Frames(b *testing.B) {
	s5 := buildStage5BenchAssets(b)

	b.Run(stage5ResponsesRouteSlug+"/new", func(b *testing.B) {
		b.SetBytes(int64(len(s5.rawStreamSSE)))
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			out, err := runNewStage5StreamAdaptation(s5.newToolMap, s5.rawStreamSSE)
			if err != nil || len(out) == 0 {
				b.Fatalf("NewResponsesToolStream failed: %v", err)
			}
		}
	})
}

// runNewRequestConversion executes one complete client-to-upstream request
// conversion through NewExchange and UpstreamRequest for spec. Malformed client
// JSON payloads or unsupported target dialect settings return an error without
// partial wire output.
func runNewRequestConversion(spec routeSpec, clientBody []byte) ([]byte, error) {
	ex, err := NewExchange(clientBody, Options{
		Route:       spec.route,
		ClientModel: spec.clientMdl,
		Runtime:     newDeterministicRuntime(),
	})
	if err != nil {
		return nil, err
	}
	return ex.UpstreamRequest()
}

// runNewStreamConversion scans rawStreamSSE with sse.NewReader, converts every
// frame through ex.Stream, and serializes outgoing client SSE wire bytes.
// Scanner read errors, malformed upstream event frames, or stream finalization
// errors abort conversion immediately.
func runNewStreamConversion(ex *Exchange, rawStreamSSE []byte) ([]byte, error) {
	stream := ex.Stream()
	rd := sse.NewReader(bytes.NewReader(rawStreamSSE))
	out := make([]byte, 0, len(rawStreamSSE))

	for rd.Scan() {
		frames, err := stream.Write(rd.Frame())
		if err != nil {
			return nil, err
		}
		for _, f := range frames {
			out = sse.Append(out, f)
		}
	}
	if err := rd.Err(); err != nil {
		return nil, err
	}
	tail, err := stream.Close(nil)
	if err != nil {
		return nil, err
	}
	for _, f := range tail {
		out = sse.Append(out, f)
	}
	return out, nil
}

// runNewStage5StreamAdaptation scans rawStreamSSE with sse.NewReader, restores
// lowered client tool calls through NewResponsesToolStream, and serializes
// client SSE frames via sse.Append. Any scanner or restoration error returns
// immediately without partial output.
func runNewStage5StreamAdaptation(tm *ToolMap, rawStreamSSE []byte) ([]byte, error) {
	stream := NewResponsesToolStream(tm)
	rd := sse.NewReader(bytes.NewReader(rawStreamSSE))
	out := make([]byte, 0, len(rawStreamSSE))

	for rd.Scan() {
		frames, err := stream.Write(rd.Frame())
		if err != nil {
			return nil, err
		}
		for _, f := range frames {
			out = sse.Append(out, f)
		}
	}
	if err := rd.Err(); err != nil {
		return nil, err
	}
	tail, err := stream.Close(nil)
	if err != nil {
		return nil, err
	}
	for _, f := range tail {
		out = sse.Append(out, f)
	}
	return out, nil
}

// tryDecodeClientStream decodes client SSE frames for proto through
// NewStreamDecoder and ir.Accumulate into a complete ir.Response. Any
// unsupported protocol, SSE framing error, frame decode failure, or accumulator
// invariant violation returns an error immediately.
func tryDecodeClientStream(proto protocol.ProtocolID, frames []sse.Frame) (*ir.Response, error) {
	dec, err := NewStreamDecoder(proto)
	if err != nil {
		return nil, err
	}
	var events []ir.Event
	rd := sse.NewReader(bytes.NewReader(formatSSEFrames(frames)))
	for rd.Scan() {
		batch, dErr := dec.Decode(rd.Frame())
		if dErr != nil {
			return nil, dErr
		}
		events = append(events, batch...)
	}
	if err := rd.Err(); err != nil {
		return nil, err
	}
	events = append(events, dec.Close(nil)...)
	return ir.Accumulate(events)
}

// buildAllBenchRouteAssets constructs deterministic 2 MiB client request bodies
// and 5,000-frame upstream SSE streams for all six Stage 1 text routes and all
// three Stage 3 Gemini routes. Any fixture marshaling failure aborts tb.
func buildAllBenchRouteAssets(tb testing.TB) []benchRouteAssets {
	tb.Helper()

	reqByProto := map[protocol.ProtocolID][]byte{
		protocol.ProtocolAnthropicMessages:     buildBenchMessagesRequest2MB(tb),
		protocol.ProtocolOpenAIResponses:       buildBenchResponsesRequest2MB(tb),
		protocol.ProtocolOpenAIChatCompletions: buildBenchChatRequest2MB(tb),
	}
	streamByProto := map[protocol.ProtocolID][]byte{
		protocol.ProtocolAnthropicMessages:     buildBenchMessagesStream5000(tb),
		protocol.ProtocolOpenAIResponses:       buildBenchResponsesStream5000(tb),
		protocol.ProtocolOpenAIChatCompletions: buildBenchChatStream5000(tb),
		protocol.ProtocolGeminiGenerateContent: buildBenchGeminiStream5000(tb),
	}

	allSpecs := make([]routeSpec, 0, len(stage1Routes)+len(stage3GeminiRoutes))
	allSpecs = append(allSpecs, stage1Routes...)
	allSpecs = append(allSpecs, stage3GeminiRoutes...)

	out := make([]benchRouteAssets, 0, len(allSpecs))
	for _, spec := range allSpecs {
		out = append(out, benchRouteAssets{
			spec:         spec,
			requestBody:  reqByProto[spec.route.Client],
			rawStreamSSE: streamByProto[spec.route.Upstream],
			streamReq:    buildStreamInitRequest(spec.route.Client),
		})
	}
	return out
}

// buildStage5BenchAssets constructs deterministic 2 MiB client request and
// 5,000-frame upstream SSE stream fixtures alongside a precomputed ToolMap for
// Stage 5 same-protocol Responses client-tool benchmarks. Any setup error aborts tb.
func buildStage5BenchAssets(tb testing.TB) benchStage5Assets {
	tb.Helper()
	dialect := Dialect{
		Name:             "openai-apikey",
		Tools:            ToolSupportFunction,
		ToolNameMaxBytes: defaultOpenAIToolNameMaxBytes,
	}
	reqBody := buildBenchStage5ResponsesToolsRequest2MB(tb)
	_, newTM, _, err := AdaptResponsesTools(reqBody, dialect, nil)
	if err != nil {
		tb.Fatalf("AdaptResponsesTools setup failed: %v", err)
	}
	return benchStage5Assets{
		dialect:      dialect,
		requestBody:  reqBody,
		rawStreamSSE: buildBenchStage5ResponsesToolsStream5000(tb),
		newToolMap:   newTM,
	}
}

// buildStreamInitRequest returns a compact client request declaring lookup_doc
// for proto so stream conversion benchmarks initialize tool context on
// convert.Exchange. Non-Messages, non-Responses protocols fall back to Chat
// Completions with stream_options.include_usage=true.
func buildStreamInitRequest(proto protocol.ProtocolID) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"claude-sonnet-4-5","max_tokens":4096,"stream":true,"tools":[{"name":"lookup_doc","description":"Read a documentation chunk","input_schema":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"],"additionalProperties":false}}],"messages":[{"role":"user","content":"Summarize the architecture modules."}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"gpt-5","stream":true,"tools":[{"type":"function","name":"lookup_doc","description":"Read a documentation chunk","parameters":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"],"additionalProperties":false}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Summarize the architecture modules."}]}]}`)
	default:
		return []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"lookup_doc","description":"Read a documentation chunk","parameters":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"],"additionalProperties":false}}}],"messages":[{"role":"user","content":"Summarize the architecture modules."}]}`)
	}
}

// buildBenchMessagesRequest2MB constructs an Anthropic Messages JSON request of
// at least 2 MiB containing system blocks, tool schemas, thinking carriers, and
// paired tool_use/tool_result multi-turn history. Serialization or carrier
// formatting failures abort tb immediately.
func buildBenchMessagesRequest2MB(tb testing.TB) []byte {
	tb.Helper()
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemePlain,
		Text:   "Inspecting repository module boundaries and verifying call invariants.",
	})
	if err != nil {
		tb.Fatalf("FormatCarrier failed: %v", err)
	}

	userChunk := strings.Repeat("Repository module dependency check: gateway -> routing -> scheduler -> protocol. ", 70)
	toolOutChunk := strings.Repeat("Verified package boundary, exported symbols, and deterministic wire serialization rules. ", 90)
	assistantChunk := strings.Repeat("Confirmed invariants hold across request decoding, tool lowering, and stream framing. ", 45)

	var messages []map[string]any
	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < benchMinRequestBytes+benchSerializationMarginBytes {
		turn++
		callID := fmt.Sprintf("toolu_bench_%04d", turn)
		uText := fmt.Sprintf("Turn %d context:\n%s", turn, userChunk)
		tArgs := map[string]any{
			"path":  fmt.Sprintf("internal/module_%03d/service.go", turn),
			"limit": 120,
		}
		tRes := fmt.Sprintf("File internal/module_%03d/service.go summary:\n%s", turn, toolOutChunk)
		aText := fmt.Sprintf("Turn %d synthesis:\n%s", turn, assistantChunk)

		messages = append(messages,
			map[string]any{
				"role": "user",
				"content": []map[string]any{
					{"type": "text", "text": uText},
				},
			},
			map[string]any{
				"role": "assistant",
				"content": []map[string]any{
					{
						"type":      "thinking",
						"thinking":  "Inspecting repository module boundaries and verifying call invariants.",
						"signature": carrier,
					},
					{
						"type":  "tool_use",
						"id":    callID,
						"name":  "read_module_doc",
						"input": tArgs,
					},
				},
			},
			map[string]any{
				"role": "user",
				"content": []map[string]any{
					{
						"type":        "tool_result",
						"tool_use_id": callID,
						"content":     tRes,
					},
				},
			},
			map[string]any{
				"role": "assistant",
				"content": []map[string]any{
					{"type": "text", "text": aText},
				},
			},
		)
		sizeEst += len(uText) + len(tRes) + len(aText) + 512
	}
	messages = append(messages, map[string]any{
		"role":    "user",
		"content": "Provide the final cross-module verification summary.",
	})

	payload := map[string]any{
		"model":      "claude-sonnet-4-5",
		"max_tokens": 4096,
		"stream":     true,
		"system": []map[string]any{
			{"type": "text", "text": "You are a precision software architecture auditor."},
		},
		"tools": []map[string]any{
			{
				"name":        "read_module_doc",
				"description": "Read a repository source or documentation slice.",
				"input_schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path":  map[string]any{"type": "string"},
						"limit": map[string]any{"type": "integer"},
					},
					"required":             []string{"path", "limit"},
					"additionalProperties": false,
				},
			},
		},
		"messages": messages,
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		tb.Fatalf("marshal bench messages request: %v", err)
	}
	return raw
}

// buildBenchResponsesRequest2MB constructs an OpenAI Responses JSON request of
// at least 2 MiB containing instructions, function and custom tools, reasoning
// carriers, and paired function_call/function_call_output items. Serialization
// or carrier formatting failures abort tb immediately.
func buildBenchResponsesRequest2MB(tb testing.TB) []byte {
	tb.Helper()
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemePlain,
		Text:   "Checking cross-protocol tool call pairing and reasoning history preservation.",
	})
	if err != nil {
		tb.Fatalf("FormatCarrier failed: %v", err)
	}

	userChunk := strings.Repeat("Responses item history verification across multi-turn agent tool loops and context slices. ", 70)
	toolOutChunk := strings.Repeat("Module analysis output: pure leaf package constraints and deterministic JSON marshaling verified. ", 90)
	assistantChunk := strings.Repeat("Synthesized findings for turn with complete tool result correlation and zero schema drift. ", 45)

	var input []map[string]any
	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < benchMinRequestBytes+benchSerializationMarginBytes {
		turn++
		callID := fmt.Sprintf("call_bench_%04d", turn)
		uText := fmt.Sprintf("Turn %d prompt:\n%s", turn, userChunk)
		argsJSON := fmt.Sprintf(`{"path":"internal/pkg_%03d/handler.go","limit":120}`, turn)
		tRes := fmt.Sprintf("Result for internal/pkg_%03d/handler.go:\n%s", turn, toolOutChunk)
		aText := fmt.Sprintf("Turn %d response:\n%s", turn, assistantChunk)

		input = append(input,
			map[string]any{
				"type": "message",
				"role": "user",
				"content": []map[string]any{
					{"type": "input_text", "text": uText},
				},
			},
			map[string]any{
				"type":              "reasoning",
				"id":                fmt.Sprintf("rs_bench_%04d", turn),
				"encrypted_content": carrier,
				"summary": []map[string]any{
					{"type": "summary_text", "text": "Checking cross-protocol tool call pairing and reasoning history preservation."},
				},
			},
			map[string]any{
				"type":      "function_call",
				"id":        fmt.Sprintf("fc_bench_%04d", turn),
				"call_id":   callID,
				"name":      "read_module_doc",
				"arguments": argsJSON,
			},
			map[string]any{
				"type":    "function_call_output",
				"call_id": callID,
				"output":  tRes,
			},
			map[string]any{
				"type": "message",
				"role": "assistant",
				"content": []map[string]any{
					{"type": "output_text", "text": aText},
				},
			},
		)
		sizeEst += len(uText) + len(tRes) + len(aText) + 640
	}
	input = append(input, map[string]any{
		"type": "message",
		"role": "user",
		"content": []map[string]any{
			{"type": "input_text", "text": "Summarize all verified module boundaries."},
		},
	})

	payload := map[string]any{
		"model":        "gpt-5",
		"stream":       true,
		"instructions": "You are a precision software architecture auditor.",
		"tools": []map[string]any{
			{
				"type":        "function",
				"name":        "read_module_doc",
				"description": "Read a repository source or documentation slice.",
				"strict":      true,
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path":  map[string]any{"type": "string"},
						"limit": map[string]any{"type": "integer"},
					},
					"required":             []string{"path", "limit"},
					"additionalProperties": false,
				},
			},
			{
				"type":        "custom",
				"name":        "apply_patch",
				"description": "Apply a structured workspace patch.",
			},
		},
		"input": input,
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		tb.Fatalf("marshal bench responses request: %v", err)
	}
	return raw
}

// buildBenchChatRequest2MB constructs an OpenAI Chat Completions JSON request
// of at least 2 MiB containing system prompt, function tools, assistant
// reasoning_content on tool turns, and paired tool messages. Serialization
// failures abort tb immediately.
func buildBenchChatRequest2MB(tb testing.TB) []byte {
	tb.Helper()

	userChunk := strings.Repeat("Chat Completions multi-turn context inspecting gateway forwarding and codec invariants. ", 70)
	toolOutChunk := strings.Repeat("Detailed inspection output covering wire JSON validation, SSE framing, and token usage buckets. ", 90)
	assistantChunk := strings.Repeat("Completed verification for turn and recorded invariant compliance across target codecs. ", 45)

	messages := []map[string]any{
		{
			"role":    "system",
			"content": "You are a precision software architecture auditor.",
		},
	}

	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < benchMinRequestBytes+benchSerializationMarginBytes {
		turn++
		callID := fmt.Sprintf("call_chat_bench_%04d", turn)
		uText := fmt.Sprintf("Turn %d input:\n%s", turn, userChunk)
		argsJSON := fmt.Sprintf(`{"path":"internal/service_%03d/core.go","limit":120}`, turn)
		tRes := fmt.Sprintf("Output for internal/service_%03d/core.go:\n%s", turn, toolOutChunk)
		aText := fmt.Sprintf("Turn %d conclusion:\n%s", turn, assistantChunk)

		messages = append(messages,
			map[string]any{
				"role":    "user",
				"content": uText,
			},
			map[string]any{
				"role":              "assistant",
				"reasoning_content": "Evaluating tool call parameters for repository slice inspection.",
				"tool_calls": []map[string]any{
					{
						"id":   callID,
						"type": "function",
						"function": map[string]any{
							"name":      "read_module_doc",
							"arguments": argsJSON,
						},
					},
				},
			},
			map[string]any{
				"role":         "tool",
				"tool_call_id": callID,
				"content":      tRes,
			},
			map[string]any{
				"role":    "assistant",
				"content": aText,
			},
		)
		sizeEst += len(uText) + len(tRes) + len(aText) + 512
	}
	messages = append(messages, map[string]any{
		"role":    "user",
		"content": "Deliver the final verification report.",
	})

	payload := map[string]any{
		"model":          "gpt-4o",
		"stream":         true,
		"stream_options": map[string]any{"include_usage": true},
		"tools": []map[string]any{
			{
				"type": "function",
				"function": map[string]any{
					"name":        "read_module_doc",
					"description": "Read a repository source or documentation slice.",
					"parameters": map[string]any{
						"type": "object",
						"properties": map[string]any{
							"path":  map[string]any{"type": "string"},
							"limit": map[string]any{"type": "integer"},
						},
						"required":             []string{"path", "limit"},
						"additionalProperties": false,
					},
				},
			},
		},
		"messages": messages,
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		tb.Fatalf("marshal bench chat request: %v", err)
	}
	return raw
}

// buildBenchMessagesStream5000 builds an Anthropic Messages upstream SSE stream
// with exactly 5,000 frames (1 message_start + 999 thinking block frames +
// 3,000 text block frames + 998 tool_use block frames + 1 message_delta + 1
// message_stop). Fragment quoting failures abort tb immediately.
func buildBenchMessagesStream5000(tb testing.TB) []byte {
	tb.Helper()
	var out []byte
	emit := func(event, data string) {
		out = sse.Append(out, sse.Frame{Event: event, Data: []byte(data)})
	}

	emit("message_start", `{"type":"message_start","message":{"id":"msg_bench_5000","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":2048,"cache_creation_input_tokens":0,"cache_read_input_tokens":512,"output_tokens":1}}}`)

	// Block 0: thinking (1 start + 996 thinking_delta + 1 signature_delta + 1 stop = 999 frames).
	emit("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`)
	for range 996 {
		emit("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"step "}}`)
	}
	emit("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"anth_sig_bench_5000"}}`)
	emit("content_block_stop", `{"type":"content_block_stop","index":0}`)

	// Block 1: text (1 start + 2,998 text_delta + 1 stop = 3,000 frames).
	emit("content_block_start", `{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}`)
	for range 2998 {
		emit("content_block_delta", `{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"token "}}`)
	}
	emit("content_block_stop", `{"type":"content_block_stop","index":1}`)

	// Block 2: tool_use (1 start + 996 input_json_delta + 1 stop = 998 frames).
	emit("content_block_start", `{"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"toolu_bench_stream_1","name":"lookup_doc","input":{}}}`)
	frags := buildFragmentedItemsJSON(996)
	for _, frag := range frags {
		quoted, err := json.Marshal(frag)
		if err != nil {
			tb.Fatalf("marshal messages tool fragment: %v", err)
		}
		emit("content_block_delta", fmt.Sprintf(`{"type":"content_block_delta","index":2,"delta":{"type":"input_json_delta","partial_json":%s}}`, quoted))
	}
	emit("content_block_stop", `{"type":"content_block_stop","index":2}`)

	emit("message_delta", `{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":4990}}`)
	emit("message_stop", `{"type":"message_stop"}`)
	return out
}

// buildBenchResponsesStream5000 builds an OpenAI Responses upstream SSE stream
// with exactly 5,000 frames (2 lifecycle + 1,000 reasoning + 3,000 message text
// + 997 function_call + 1 response.completed). Event marshaling failures abort
// tb immediately.
func buildBenchResponsesStream5000(tb testing.TB) []byte {
	tb.Helper()
	var out []byte
	seq := 0
	emit := func(event string, payload map[string]any) {
		payload["sequence_number"] = seq
		seq++
		raw, err := json.Marshal(payload)
		if err != nil {
			tb.Fatalf("marshal responses stream event %s: %v", event, err)
		}
		out = sse.Append(out, sse.Frame{Event: event, Data: raw})
	}

	baseResp := func(status string) map[string]any {
		return map[string]any{
			"id":         "resp_bench_5000",
			"object":     "response",
			"created_at": 1760000000,
			"status":     status,
			"model":      "gpt-5",
			"output":     []any{},
		}
	}

	emit("response.created", map[string]any{"type": "response.created", "response": baseResp("in_progress")})
	emit("response.in_progress", map[string]any{"type": "response.in_progress", "response": baseResp("in_progress")})

	// Output 0: reasoning (1 added + 1 part.added + 995 delta + 1 text.done + 1 part.done + 1 item.done = 1,000 frames).
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 0,
		"item":         map[string]any{"id": "rs_stream_1", "type": "reasoning", "summary": []any{}},
	})
	emit("response.reasoning_summary_part.added", map[string]any{
		"type":          "response.reasoning_summary_part.added",
		"item_id":       "rs_stream_1",
		"output_index":  0,
		"summary_index": 0,
		"part":          map[string]any{"type": "summary_text", "text": ""},
	})
	fullReasoning := strings.Repeat("step ", 995)
	for range 995 {
		emit("response.reasoning_summary_text.delta", map[string]any{
			"type":          "response.reasoning_summary_text.delta",
			"item_id":       "rs_stream_1",
			"output_index":  0,
			"summary_index": 0,
			"delta":         "step ",
		})
	}
	emit("response.reasoning_summary_text.done", map[string]any{
		"type":          "response.reasoning_summary_text.done",
		"item_id":       "rs_stream_1",
		"output_index":  0,
		"summary_index": 0,
		"text":          fullReasoning,
	})
	emit("response.reasoning_summary_part.done", map[string]any{
		"type":          "response.reasoning_summary_part.done",
		"item_id":       "rs_stream_1",
		"output_index":  0,
		"summary_index": 0,
		"part":          map[string]any{"type": "summary_text", "text": fullReasoning},
	})
	reasoningDoneItem := map[string]any{
		"id":                "rs_stream_1",
		"type":              "reasoning",
		"encrypted_content": "enc_openai_bench_5000",
		"summary":           []any{map[string]any{"type": "summary_text", "text": fullReasoning}},
	}
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 0,
		"item":         reasoningDoneItem,
	})

	// Output 1: assistant message (1 added + 1 part.added + 2,995 delta + 1 text.done + 1 part.done + 1 item.done = 3,000 frames).
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 1,
		"item":         map[string]any{"id": "msg_stream_1", "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}},
	})
	emit("response.content_part.added", map[string]any{
		"type":          "response.content_part.added",
		"item_id":       "msg_stream_1",
		"output_index":  1,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
	})
	fullText := strings.Repeat("token ", 2995)
	for range 2995 {
		emit("response.output_text.delta", map[string]any{
			"type":          "response.output_text.delta",
			"item_id":       "msg_stream_1",
			"output_index":  1,
			"content_index": 0,
			"delta":         "token ",
		})
	}
	emit("response.output_text.done", map[string]any{
		"type":          "response.output_text.done",
		"item_id":       "msg_stream_1",
		"output_index":  1,
		"content_index": 0,
		"text":          fullText,
	})
	emit("response.content_part.done", map[string]any{
		"type":          "response.content_part.done",
		"item_id":       "msg_stream_1",
		"output_index":  1,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": fullText, "annotations": []any{}},
	})
	msgDoneItem := map[string]any{
		"id":      "msg_stream_1",
		"type":    "message",
		"status":  "completed",
		"role":    "assistant",
		"content": []any{map[string]any{"type": "output_text", "text": fullText, "annotations": []any{}}},
	}
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 1,
		"item":         msgDoneItem,
	})

	// Output 2: function_call (1 added + 994 delta + 1 args.done + 1 item.done = 997 frames).
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 2,
		"item": map[string]any{
			"id":        "fc_stream_1",
			"type":      "function_call",
			"status":    "in_progress",
			"call_id":   "call_resp_stream_1",
			"name":      "lookup_doc",
			"arguments": "",
		},
	})
	frags := buildFragmentedItemsJSON(994)
	fullArgs := strings.Join(frags, "")
	for _, frag := range frags {
		emit("response.function_call_arguments.delta", map[string]any{
			"type":         "response.function_call_arguments.delta",
			"item_id":      "fc_stream_1",
			"output_index": 2,
			"delta":        frag,
		})
	}
	emit("response.function_call_arguments.done", map[string]any{
		"type":         "response.function_call_arguments.done",
		"item_id":      "fc_stream_1",
		"output_index": 2,
		"arguments":    fullArgs,
	})
	fcDoneItem := map[string]any{
		"id":        "fc_stream_1",
		"type":      "function_call",
		"status":    "completed",
		"call_id":   "call_resp_stream_1",
		"name":      "lookup_doc",
		"arguments": fullArgs,
	}
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 2,
		"item":         fcDoneItem,
	})

	completed := baseResp("completed")
	completed["output"] = []any{reasoningDoneItem, msgDoneItem, fcDoneItem}
	completed["usage"] = map[string]any{
		"input_tokens":          2560,
		"input_tokens_details":  map[string]any{"cached_tokens": 512},
		"output_tokens":         4990,
		"output_tokens_details": map[string]any{"reasoning_tokens": 995},
		"total_tokens":          7550,
	}
	emit("response.completed", map[string]any{"type": "response.completed", "response": completed})
	return out
}

// buildBenchChatStream5000 builds an OpenAI Chat Completions upstream SSE
// stream with exactly 5,000 frames (1,000 reasoning chunks + 3,000 text chunks
// + 997 tool_call chunks + 1 finish chunk + 1 usage chunk + 1 [DONE]). Fragment
// quoting failures abort tb immediately.
func buildBenchChatStream5000(tb testing.TB) []byte {
	tb.Helper()
	var out []byte
	emitData := func(data string) {
		out = sse.Append(out, sse.Frame{Data: []byte(data)})
	}

	// Frame 1: role + first reasoning chunk.
	emitData(`{"id":"chatcmpl_bench_5000","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"step "},"finish_reason":null}]}`)
	// Frames 2..1,000: 999 reasoning chunks.
	for range 999 {
		emitData(`{"id":"chatcmpl_bench_5000","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"reasoning_content":"step "},"finish_reason":null}]}`)
	}
	// Frames 1,001..4,000: 3,000 visible text chunks.
	for range 3000 {
		emitData(`{"id":"chatcmpl_bench_5000","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"content":"token "},"finish_reason":null}]}`)
	}
	// Frame 4,001: tool_call header chunk.
	emitData(`{"id":"chatcmpl_bench_5000","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_chat_stream_1","type":"function","function":{"name":"lookup_doc","arguments":""}}]},"finish_reason":null}]}`)
	// Frames 4,002..4,997: 996 argument fragments.
	frags := buildFragmentedItemsJSON(996)
	for _, frag := range frags {
		quoted, err := json.Marshal(frag)
		if err != nil {
			tb.Fatalf("marshal chat tool fragment: %v", err)
		}
		emitData(fmt.Sprintf(`{"id":"chatcmpl_bench_5000","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":%s}}]},"finish_reason":null}]}`, quoted))
	}
	// Frame 4,998: finish_reason chunk.
	emitData(`{"id":"chatcmpl_bench_5000","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`)
	// Frame 4,999: terminal usage chunk.
	emitData(`{"id":"chatcmpl_bench_5000","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[],"usage":{"prompt_tokens":2560,"prompt_tokens_details":{"cached_tokens":512},"completion_tokens":4990,"completion_tokens_details":{"reasoning_tokens":1000},"total_tokens":7550}}`)
	// Frame 5,000: [DONE] sentinel.
	emitData("[DONE]")
	return out
}

// buildBenchGeminiStream5000 builds a Google Gemini GenerateContent upstream
// SSE stream with exactly 5,000 frames (1,000 thought chunks + 3,990 visible
// text chunks + 8 functionCall chunks + 1 finishReason chunk + 1 sparse
// usageMetadata tail chunk). Distinct per-chunk prefixes keep cumulative-prefix
// deduplication from suppressing incremental deltas in StreamDecoder.
func buildBenchGeminiStream5000(tb testing.TB) []byte {
	tb.Helper()
	var out []byte
	emitData := func(data string) {
		out = sse.Append(out, sse.Frame{Data: []byte(data)})
	}

	// Frames 1..999: 999 incremental thought chunks.
	for i := range 999 {
		emitData(fmt.Sprintf(`{"responseId":"gem_bench_5000","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"s%04d ","thought":true}]}}]}`, i))
	}
	// Frame 1,000: signed final thought chunk.
	emitData(`{"responseId":"gem_bench_5000","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"s0999 ","thought":true,"thoughtSignature":"gem_sig_bench_5000"}]}}]}`)

	// Frames 1,001..4,990: 3,990 incremental visible text chunks.
	for i := range 3990 {
		emitData(fmt.Sprintf(`{"responseId":"gem_bench_5000","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"t%04d "}]}}]}`, i))
	}

	// Frames 4,991..4,998: 8 functionCall chunks (step-initial call carries thoughtSignature).
	for i := range 8 {
		sigField := ""
		if i == 0 {
			sigField = `,"thoughtSignature":"gem_fc_sig_bench_1"`
		}
		emitData(fmt.Sprintf(`{"responseId":"gem_bench_5000","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"call_gem_stream_%d","name":"lookup_doc","args":{"items":["doc_%02d_a","doc_%02d_b"]}}%s}]}}]}`, i+1, i, i, sigField))
	}

	// Frame 4,999: finishReason chunk with initial usageMetadata.
	emitData(`{"responseId":"gem_bench_5000","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2560,"cachedContentTokenCount":512,"candidatesTokenCount":4000,"totalTokenCount":6560}}`)

	// Frame 5,000: sparse usageMetadata tail frame updating output and thought counts.
	emitData(`{"responseId":"gem_bench_5000","modelVersion":"gemini-2.5-pro","usageMetadata":{"candidatesTokenCount":4990,"thoughtsTokenCount":1000,"totalTokenCount":8550}}`)
	return out
}

// buildBenchStage5ResponsesToolsRequest2MB builds a >= 2 MiB OpenAI Responses
// JSON request mixing custom, namespace, and tool_search client tools and
// historical calls with large untouched message, reasoning, and compaction items.
// Any marshaling error aborts tb immediately.
func buildBenchStage5ResponsesToolsRequest2MB(tb testing.TB) []byte {
	tb.Helper()

	userChunk := strings.Repeat("Same-protocol Responses passthrough context slice preserving item-level cache breakpoints and big integers. ", 75)
	toolOutChunk := strings.Repeat("Workspace verification output across pure leaf codecs, lossless raw JSON members, and deterministic tool lowering. ", 85)
	assistantChunk := strings.Repeat("Confirmed client tool declarations and multi-turn call outputs round-trip without mutating unrelated payload items. ", 50)

	var input []map[string]any
	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < benchMinRequestBytes+benchSerializationMarginBytes {
		turn++
		uText := fmt.Sprintf("Turn %d user request:\n%s", turn, userChunk)
		tRes := fmt.Sprintf("Turn %d tool output:\n%s", turn, toolOutChunk)
		aText := fmt.Sprintf("Turn %d assistant synthesis:\n%s", turn, assistantChunk)

		input = append(input,
			map[string]any{
				"type": "message",
				"role": "user",
				"content": []map[string]any{
					{"type": "input_text", "text": uText},
				},
			},
			map[string]any{
				"type":              "reasoning",
				"id":                fmt.Sprintf("rs_s5_%04d", turn),
				"encrypted_content": fmt.Sprintf("gAAAAABm_raw_native_reasoning_token_%04d", turn),
				"summary": []map[string]any{
					{"type": "summary_text", "text": "Planning client tool invocations and preserving raw token bytes."},
				},
			},
		)

		switch turn % 3 {
		case 1:
			callID := fmt.Sprintf("call_s5_patch_%04d", turn)
			patchInput := fmt.Sprintf("*** Begin Patch\n*** Update File: internal/mod_%03d/file.go\n+verified\n*** End Patch\n", turn)
			input = append(input,
				map[string]any{
					"type":    "custom_tool_call",
					"id":      fmt.Sprintf("ctc_s5_%04d", turn),
					"call_id": callID,
					"name":    "apply_patch",
					"input":   patchInput,
				},
				map[string]any{
					"type":    "custom_tool_call_output",
					"call_id": callID,
					"output":  tRes,
				},
			)
		case 2:
			callID := fmt.Sprintf("call_s5_ns_%04d", turn)
			nsArgs := fmt.Sprintf(`{"task":"audit_module_%03d"}`, turn)
			input = append(input,
				map[string]any{
					"type":      "function_call",
					"id":        fmt.Sprintf("fc_s5_ns_%04d", turn),
					"call_id":   callID,
					"namespace": "collaboration",
					"name":      "spawn_agent",
					"arguments": nsArgs,
				},
				map[string]any{
					"type":    "function_call_output",
					"call_id": callID,
					"output":  tRes,
				},
			)
		default:
			searchID := fmt.Sprintf("call_s5_search_%04d", turn)
			fnID := fmt.Sprintf("call_s5_doc_%04d", turn)
			input = append(input,
				map[string]any{
					"type":      "tool_search_call",
					"id":        fmt.Sprintf("tsc_s5_%04d", turn),
					"call_id":   searchID,
					"execution": "client",
					"arguments": map[string]any{"query": "incident", "limit": 2},
				},
				map[string]any{
					"type":      "tool_search_output",
					"call_id":   searchID,
					"status":    "completed",
					"execution": "client",
					"tools": []map[string]any{
						{
							"type":        "namespace",
							"name":        "pager",
							"description": "Incident paging connector.",
							"tools": []map[string]any{
								{
									"type":        "function",
									"name":        "list_incidents",
									"description": "List active incidents.",
									"parameters": map[string]any{
										"type":       "object",
										"properties": map[string]any{"severity": map[string]any{"type": "string"}},
									},
								},
							},
						},
					},
				},
				map[string]any{
					"type":      "function_call",
					"id":        fmt.Sprintf("fc_s5_doc_%04d", turn),
					"call_id":   fnID,
					"name":      "read_module_doc",
					"arguments": fmt.Sprintf(`{"path":"docs/mod_%03d.md","limit":64}`, turn),
				},
				map[string]any{
					"type":    "function_call_output",
					"call_id": fnID,
					"output":  tRes,
				},
			)
		}

		input = append(input, map[string]any{
			"type": "message",
			"role": "assistant",
			"content": []map[string]any{
				{"type": "output_text", "text": aText},
			},
		})
		if turn%4 == 0 {
			input = append(input, map[string]any{
				"type":              "compaction",
				"id":                fmt.Sprintf("cmp_s5_%04d", turn),
				"encrypted_content": fmt.Sprintf("cmp_opaque_state_blob_%04d", turn),
			})
		}
		sizeEst += len(uText) + len(tRes) + len(aText) + 768
	}

	input = append(input, map[string]any{
		"type": "message",
		"role": "user",
		"content": []map[string]any{
			{"type": "input_text", "text": "Summarize all adapted client tool findings."},
		},
	})

	payload := map[string]any{
		"model":               "gpt-5.3-codex",
		"stream":              true,
		"store":               false,
		"prompt_cache_key":    "bench_stage5_cache_key",
		"instructions":        "You are a precision software architecture auditor.",
		"parallel_tool_calls": true,
		"tools": []map[string]any{
			{
				"type":        "function",
				"name":        "read_module_doc",
				"description": "Read a repository source or documentation slice.",
				"strict":      true,
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path":  map[string]any{"type": "string"},
						"limit": map[string]any{"type": "integer"},
					},
					"required":             []string{"path", "limit"},
					"additionalProperties": false,
				},
			},
			{
				"type":        "custom",
				"name":        "apply_patch",
				"description": "Apply a structured workspace patch.",
			},
			{
				"type":        "namespace",
				"name":        "collaboration",
				"description": "Multi-agent task delegation tools.",
				"tools": []map[string]any{
					{
						"type":        "function",
						"name":        "spawn_agent",
						"description": "Spawn a worker agent for a bounded task.",
						"parameters": map[string]any{
							"type": "object",
							"properties": map[string]any{
								"task": map[string]any{"type": "string"},
							},
							"required":             []string{"task"},
							"additionalProperties": false,
						},
					},
				},
			},
			{
				"type": "tool_search",
			},
			{
				"type":          "function",
				"name":          "archive_audit_log",
				"description":   "Archive audit findings.",
				"defer_loading": true,
				"parameters": map[string]any{
					"type":       "object",
					"properties": map[string]any{"summary": map[string]any{"type": "string"}},
				},
			},
		},
		"input": input,
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		tb.Fatalf("marshal Stage 5 bench responses request: %v", err)
	}
	return raw
}

// buildBenchStage5ResponsesToolsStream5000 builds an OpenAI Responses upstream
// SSE stream with exactly 5,000 frames (2 lifecycle + 995 reasoning + 3,000
// message text + 500 lowered custom apply_patch + 6 lowered tool_search + 496
// lowered namespaced collaboration__spawn_agent + 1 response.completed). Any
// event marshaling failure aborts tb immediately.
func buildBenchStage5ResponsesToolsStream5000(tb testing.TB) []byte {
	tb.Helper()
	var out []byte
	seq := 0
	emit := func(event string, payload map[string]any) {
		payload["sequence_number"] = seq
		seq++
		raw, err := json.Marshal(payload)
		if err != nil {
			tb.Fatalf("marshal Stage 5 stream event %s: %v", event, err)
		}
		out = sse.Append(out, sse.Frame{Event: event, Data: raw})
	}

	baseResp := func(status string) map[string]any {
		return map[string]any{
			"id":         "resp_s5_bench_5000",
			"object":     "response",
			"created_at": 1760000000,
			"status":     status,
			"model":      "gpt-5.3-codex",
			"output":     []any{},
		}
	}

	emit("response.created", map[string]any{"type": "response.created", "response": baseResp("in_progress")})
	emit("response.in_progress", map[string]any{"type": "response.in_progress", "response": baseResp("in_progress")})

	// Output 0: reasoning (1 added + 1 part.added + 990 delta + 1 text.done + 1 part.done + 1 item.done = 995 frames).
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 0,
		"item":         map[string]any{"id": "rs_s5_stream_1", "type": "reasoning", "summary": []any{}},
	})
	emit("response.reasoning_summary_part.added", map[string]any{
		"type":          "response.reasoning_summary_part.added",
		"item_id":       "rs_s5_stream_1",
		"output_index":  0,
		"summary_index": 0,
		"part":          map[string]any{"type": "summary_text", "text": ""},
	})
	fullReasoning := strings.Repeat("step ", 990)
	for range 990 {
		emit("response.reasoning_summary_text.delta", map[string]any{
			"type":          "response.reasoning_summary_text.delta",
			"item_id":       "rs_s5_stream_1",
			"output_index":  0,
			"summary_index": 0,
			"delta":         "step ",
		})
	}
	emit("response.reasoning_summary_text.done", map[string]any{
		"type":          "response.reasoning_summary_text.done",
		"item_id":       "rs_s5_stream_1",
		"output_index":  0,
		"summary_index": 0,
		"text":          fullReasoning,
	})
	emit("response.reasoning_summary_part.done", map[string]any{
		"type":          "response.reasoning_summary_part.done",
		"item_id":       "rs_s5_stream_1",
		"output_index":  0,
		"summary_index": 0,
		"part":          map[string]any{"type": "summary_text", "text": fullReasoning},
	})
	reasoningDoneItem := map[string]any{
		"id":                "rs_s5_stream_1",
		"type":              "reasoning",
		"encrypted_content": "gAAAAABm_s5_stream_5000",
		"summary":           []any{map[string]any{"type": "summary_text", "text": fullReasoning}},
	}
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 0,
		"item":         reasoningDoneItem,
	})

	// Output 1: assistant message (1 added + 1 part.added + 2,995 delta + 1 text.done + 1 part.done + 1 item.done = 3,000 frames).
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 1,
		"item":         map[string]any{"id": "msg_s5_stream_1", "type": "message", "status": "in_progress", "role": "assistant", "content": []any{}},
	})
	emit("response.content_part.added", map[string]any{
		"type":          "response.content_part.added",
		"item_id":       "msg_s5_stream_1",
		"output_index":  1,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": "", "annotations": []any{}},
	})
	fullText := strings.Repeat("token ", 2995)
	for range 2995 {
		emit("response.output_text.delta", map[string]any{
			"type":          "response.output_text.delta",
			"item_id":       "msg_s5_stream_1",
			"output_index":  1,
			"content_index": 0,
			"delta":         "token ",
		})
	}
	emit("response.output_text.done", map[string]any{
		"type":          "response.output_text.done",
		"item_id":       "msg_s5_stream_1",
		"output_index":  1,
		"content_index": 0,
		"text":          fullText,
	})
	emit("response.content_part.done", map[string]any{
		"type":          "response.content_part.done",
		"item_id":       "msg_s5_stream_1",
		"output_index":  1,
		"content_index": 0,
		"part":          map[string]any{"type": "output_text", "text": fullText, "annotations": []any{}},
	})
	msgDoneItem := map[string]any{
		"id":      "msg_s5_stream_1",
		"type":    "message",
		"status":  "completed",
		"role":    "assistant",
		"content": []any{map[string]any{"type": "output_text", "text": fullText, "annotations": []any{}}},
	}
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 1,
		"item":         msgDoneItem,
	})

	// Output 2: lowered custom tool apply_patch (1 added + 497 delta + 1 args.done + 1 item.done = 500 frames).
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 2,
		"item": map[string]any{
			"id":        "fc_s5_patch_1",
			"type":      "function_call",
			"status":    "in_progress",
			"call_id":   "call_s5_patch_stream_1",
			"name":      "apply_patch",
			"arguments": "",
		},
	})
	patchFrags := make([]string, 0, 497)
	patchFrags = append(patchFrags, `{"input":"*** Begin Patch\n`)
	for i := range 495 {
		patchFrags = append(patchFrags, fmt.Sprintf(`+line_%04d\n`, i))
	}
	patchFrags = append(patchFrags, `*** End Patch"}`)
	fullPatchArgs := strings.Join(patchFrags, "")
	for _, frag := range patchFrags {
		emit("response.function_call_arguments.delta", map[string]any{
			"type":         "response.function_call_arguments.delta",
			"item_id":      "fc_s5_patch_1",
			"output_index": 2,
			"delta":        frag,
		})
	}
	emit("response.function_call_arguments.done", map[string]any{
		"type":         "response.function_call_arguments.done",
		"item_id":      "fc_s5_patch_1",
		"output_index": 2,
		"arguments":    fullPatchArgs,
	})
	patchDoneItem := map[string]any{
		"id":        "fc_s5_patch_1",
		"type":      "function_call",
		"status":    "completed",
		"call_id":   "call_s5_patch_stream_1",
		"name":      "apply_patch",
		"arguments": fullPatchArgs,
	}
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 2,
		"item":         patchDoneItem,
	})

	// Output 3: lowered tool_search proxy (1 added + 3 delta + 1 args.done + 1 item.done = 6 frames).
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 3,
		"item": map[string]any{
			"id":        "fc_s5_search_1",
			"type":      "function_call",
			"status":    "in_progress",
			"call_id":   "call_s5_search_stream_1",
			"name":      "tool_search",
			"arguments": "",
		},
	})
	searchFrags := []string{`{"query":`, `"incident",`, `"limit":2}`}
	fullSearchArgs := strings.Join(searchFrags, "")
	for _, frag := range searchFrags {
		emit("response.function_call_arguments.delta", map[string]any{
			"type":         "response.function_call_arguments.delta",
			"item_id":      "fc_s5_search_1",
			"output_index": 3,
			"delta":        frag,
		})
	}
	emit("response.function_call_arguments.done", map[string]any{
		"type":         "response.function_call_arguments.done",
		"item_id":      "fc_s5_search_1",
		"output_index": 3,
		"arguments":    fullSearchArgs,
	})
	searchDoneItem := map[string]any{
		"id":        "fc_s5_search_1",
		"type":      "function_call",
		"status":    "completed",
		"call_id":   "call_s5_search_stream_1",
		"name":      "tool_search",
		"arguments": fullSearchArgs,
	}
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 3,
		"item":         searchDoneItem,
	})

	// Output 4: lowered namespace call collaboration__spawn_agent (1 added + 493 delta + 1 args.done + 1 item.done = 496 frames).
	emit("response.output_item.added", map[string]any{
		"type":         "response.output_item.added",
		"output_index": 4,
		"item": map[string]any{
			"id":        "fc_s5_ns_1",
			"type":      "function_call",
			"status":    "in_progress",
			"call_id":   "call_s5_ns_stream_1",
			"name":      "collaboration__spawn_agent",
			"arguments": "",
		},
	})
	nsFrags := buildFragmentedItemsJSON(493)
	fullNSArgs := strings.Join(nsFrags, "")
	for _, frag := range nsFrags {
		emit("response.function_call_arguments.delta", map[string]any{
			"type":         "response.function_call_arguments.delta",
			"item_id":      "fc_s5_ns_1",
			"output_index": 4,
			"delta":        frag,
		})
	}
	emit("response.function_call_arguments.done", map[string]any{
		"type":         "response.function_call_arguments.done",
		"item_id":      "fc_s5_ns_1",
		"output_index": 4,
		"arguments":    fullNSArgs,
	})
	nsDoneItem := map[string]any{
		"id":        "fc_s5_ns_1",
		"type":      "function_call",
		"status":    "completed",
		"call_id":   "call_s5_ns_stream_1",
		"name":      "collaboration__spawn_agent",
		"arguments": fullNSArgs,
	}
	emit("response.output_item.done", map[string]any{
		"type":         "response.output_item.done",
		"output_index": 4,
		"item":         nsDoneItem,
	})

	completed := baseResp("completed")
	completed["output"] = []any{reasoningDoneItem, msgDoneItem, patchDoneItem, searchDoneItem, nsDoneItem}
	completed["usage"] = map[string]any{
		"input_tokens":          2560,
		"input_tokens_details":  map[string]any{"cached_tokens": 512},
		"output_tokens":         4990,
		"output_tokens_details": map[string]any{"reasoning_tokens": 990},
		"total_tokens":          7550,
	}
	emit("response.completed", map[string]any{"type": "response.completed", "response": completed})
	return out
}

// buildFragmentedItemsJSON splits a valid {"items":[...]} JSON object into
// exactly n non-empty string fragments for streaming tool-argument deltas.
// Callers must pass n >= 2 so the opening and closing delimiters each occupy one slot.
func buildFragmentedItemsJSON(n int) []string {
	frags := make([]string, 0, n)
	frags = append(frags, `{"items":[`)
	middle := n - 2
	for i := range middle {
		if i == 0 {
			frags = append(frags, fmt.Sprintf(`"item_%04d"`, i))
		} else {
			frags = append(frags, fmt.Sprintf(`,"item_%04d"`, i))
		}
	}
	frags = append(frags, `]}`)
	return frags
}

// countRawSSEFrames counts non-empty SSE frames scanned by sse.NewReader from
// rawSSE in wire appearance order. Scanning stops at EOF or the first SSE
// reader error and returns the number of complete frames observed so far.
func countRawSSEFrames(rawSSE []byte) int {
	rd := sse.NewReader(bytes.NewReader(rawSSE))
	n := 0
	for rd.Scan() {
		n++
	}
	return n
}

// parseFramesForValidation parses rawSSE through sse.NewReader into an ordered
// slice of sse.Frame values for downstream stream grammar validation. Any
// scanner error reported by rd.Err() fails tb immediately via require.NoError.
func parseFramesForValidation(tb testing.TB, rawSSE []byte) []sse.Frame {
	tb.Helper()
	rd := sse.NewReader(bytes.NewReader(rawSSE))
	var frames []sse.Frame
	for rd.Scan() {
		frames = append(frames, rd.Frame())
	}
	require.NoError(tb, rd.Err())
	return frames
}
