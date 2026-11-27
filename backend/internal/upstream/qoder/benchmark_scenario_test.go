package qoder

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	protocolqoder "github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// benchmark_scenario_test.go benchmarks Qoder IR request parsing and streaming
// response conversion across payload.go, payload_converted.go,
// stream_conversion.go, stream_tool_text.go, response_converted.go,
// output_observation.go, thinking.go, and execute.go on all three Qoder client
// routes (messages_to_qoder, responses_to_qoder, and chat_to_qoder).
//
// Request benchmarks measure convert.NewExchange, protocolqoder.DecodePayload,
// PayloadRequestFromIR, and withEffectiveControls on deterministic 2 MiB
// multi-turn client payloads. Streaming benchmarks measure ReadConvertedStreaming
// and Exchange.Stream over a 5,000-frame wrapped Qoder SSE stream, while
// TestBenchmarkQoderPayloadsValid verifies payload sizes, frame counts, Qoder
// PayloadRequest invariants, client stream grammar, billing usage accounting,
// and bounded per-iteration state during standard unit test runs.

const (
	// qoderBenchMinRequestBytes, qoderBenchMarginBytes, qoderBenchStreamFrames,
	// and qoderBenchRespID govern payload sizing and deterministic response IDs
	// for Stage 6 Qoder 2 MiB request and 5,000-frame stream benchmarks.
	qoderBenchMinRequestBytes = 2 * 1024 * 1024                     // minimum 2 MiB serialized client request payload
	qoderBenchMarginBytes     = 65536                               // extra turn target keeping marshaled JSON comfortably above 2 MiB
	qoderBenchStreamFrames    = 5000                                // exact upstream wrapped Qoder SSE frame count
	qoderBenchRespID          = "resp_qoder_bench_stream_000000001" // deterministic Responses envelope ID
)

// qoderBenchRoutes enumerates all three Stage 6 Qoder client-to-upstream routes
// evaluated across 2 MiB request and 5,000-frame stream benchmarks.
var qoderBenchRoutes = []qoderBenchRouteSpec{
	{
		slug: "messages_to_qoder",
		route: convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolQoderChat,
			Dialect: convert.Dialect{
				Name:               "qoder",
				Tools:              convert.ToolSupportFunction,
				AllowBillingHeader: true,
			},
		},
		clientModel: "claude-sonnet-4-5",
	},
	{
		slug: "responses_to_qoder",
		route: convert.Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolQoderChat,
			Dialect: convert.Dialect{
				Name:               "qoder",
				Tools:              convert.ToolSupportFunction,
				AllowBillingHeader: true,
				StatelessResponses: true,
			},
		},
		clientModel: "gpt-5",
	},
	{
		slug: "chat_to_qoder",
		route: convert.Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolQoderChat,
			Dialect: convert.Dialect{
				Name:               "qoder",
				Tools:              convert.ToolSupportFunction,
				AllowBillingHeader: true,
			},
		},
		clientModel: "gpt-4o",
	},
}

// qoderBenchRouteSpec pairs a stable Qoder route slug with its convert.Route
// and client-echoed model identifier for Stage 6 benchmarks.
type qoderBenchRouteSpec struct {
	slug        string        // stable route benchmark identifier
	route       convert.Route // client protocol, ProtocolQoderChat upstream, and Dialect
	clientModel string        // client-visible model echoed in responses
}

// qoderBenchAssets bundles the prebuilt 2 MiB client request, 5,000-frame
// wrapped Qoder upstream SSE stream, compact stream-init request, and declared
// tool-name mappers for one Stage 6 Qoder route.
type qoderBenchAssets struct {
	spec         qoderBenchRouteSpec // Qoder route configuration
	requestBody  []byte              // >= 2 MiB client request JSON payload
	rawStreamSSE []byte              // 5,000-frame wrapped Qoder SSE stream
	streamReq    []byte              // compact client request initializing stream state
	newMapper    QoderToolNameMapper // precomputed IR tool-name mapper
}

// qoderBenchStreamSink captures raw client SSE bytes emitted through
// executionOutput without per-event SSE re-parsing overhead during benchmark loops.
type qoderBenchStreamSink struct {
	buf bytes.Buffer // accumulated downstream SSE payload bytes
}

// Begin accepts initial output head metadata without copying response headers
// and returns nil immediately. Omitting header clones lets streaming benchmark
// loops measure SSE payload conversion without per-attempt map allocations.
func (s *qoderBenchStreamSink) Begin(upstream.OutputHead) error {
	return nil
}

// Emit appends non-empty payload bytes from ev.Data onto s.buf and returns any
// underlying buffer write error. Flush-only events carrying empty Data slices
// return nil immediately without modifying s.buf.
func (s *qoderBenchStreamSink) Emit(ev upstream.OutputEvent) error {
	if len(ev.Data) > 0 {
		_, err := s.buf.Write(ev.Data)
		return err
	}
	return nil
}

// Reset rewinds s.buf length to zero while retaining its allocated backing
// byte slice. Consecutive benchmark iterations reuse the existing buffer
// capacity without reallocating downstream SSE output storage.
func (s *qoderBenchStreamSink) Reset() {
	s.buf.Reset()
}

// TestBenchmarkQoderPayloadsValid builds the 2 MiB client request payloads and
// 5,000-frame wrapped Qoder SSE stream across all three Stage 6 Qoder routes.
// Every payload must meet its size and frame-count targets, satisfy Qoder
// PayloadRequest structural and stateless invariants plus strict client stream
// grammar, report positive input and output billing TokenUsage, and keep
// per-iteration slices bounded across repeated runs.
func TestBenchmarkQoderPayloadsValid(t *testing.T) {
	assets := buildAllQoderBenchAssets(t)
	require.Len(t, assets, len(qoderBenchRoutes))

	for _, item := range assets {
		t.Run(item.spec.slug, func(t *testing.T) {
			require.GreaterOrEqual(t, len(item.requestBody), qoderBenchMinRequestBytes,
				"Qoder request payload for %s must be at least 2 MiB", item.spec.slug)

			require.Equal(t, qoderBenchStreamFrames, qoderBenchCountSSEFrames(item.rawStreamSSE),
				"Qoder upstream stream for %s must contain exactly 5,000 SSE frames", item.spec.slug)

			newReq, upBytes, err := runNewQoderBenchRequest(item.spec, item.requestBody)
			require.NoError(t, err)
			require.NotEmpty(t, newReq.Model)
			require.Positive(t, newReq.MaxTokens)
			if item.spec.route.Client == protocol.ProtocolAnthropicMessages {
				require.Contains(t, newReq.System, "cch=<cch>;")
			}
			stateless := item.spec.route.Client == protocol.ProtocolOpenAIResponses
			reqViol := validateQoderBenchPayload(upBytes, stateless)
			require.Empty(t, reqViol, "new Qoder 2 MiB request violated %s invariants: %v", item.spec.slug, reqViol)

			var sink qoderBenchStreamSink
			var firstLen int
			var newRes *QoderConvertedResult
			for iter := range 2 {
				sink.Reset()
				newRes, err = runNewQoderBenchStream(&sink, item)
				require.NoError(t, err)
				require.NotNil(t, newRes)
				require.Positive(t, sink.buf.Len())
				if iter == 0 {
					firstLen = sink.buf.Len()
				} else {
					require.Equal(t, firstLen, sink.buf.Len(),
						"repeated Qoder stream runs must emit deterministic byte counts without state drift on %s", item.spec.slug)
				}
			}

			outFrames := qoderBenchParseSSEFrames(t, sink.buf.Bytes())
			gramViol := testkit.ValidateStreamGrammar(item.spec.route.Client, outFrames, testkit.GrammarOptions{})
			require.Empty(t, gramViol, "new Qoder 5,000-frame stream violated %s grammar: %v", item.spec.slug, gramViol)

			require.True(t, newRes.HasUsage)
			require.Positive(t, newRes.Usage.InputTokens+newRes.Usage.CacheReadInputTokens)
			require.Positive(t, newRes.Usage.OutputTokens)
		})
	}
}

// BenchmarkQoderRequestConversion2MB benchmarks 2 MiB multi-turn client request
// conversion across messages_to_qoder, responses_to_qoder, and chat_to_qoder
// through convert.NewExchange and PayloadRequestFromIR. Each sub-benchmark
// reports per-operation latency, byte throughput, and heap allocations.
func BenchmarkQoderRequestConversion2MB(b *testing.B) {
	assets := buildAllQoderBenchAssets(b)

	for _, item := range assets {
		b.Run(item.spec.slug+"/new", func(b *testing.B) {
			b.SetBytes(int64(len(item.requestBody)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				req, _, err := runNewQoderBenchRequest(item.spec, item.requestBody)
				if err != nil || len(req.Messages) == 0 {
					b.Fatalf("new Qoder request conversion failed on %s: %v", item.spec.slug, err)
				}
			}
		})
	}
}

// BenchmarkQoderStreamConversion5000Frames benchmarks 5,000-frame wrapped Qoder
// SSE response streaming across messages_to_qoder, responses_to_qoder, and
// chat_to_qoder through ReadConvertedStreaming and Exchange.Stream. Each
// sub-benchmark resets its sink per iteration and measures end-to-end wrapped
// SSE scanning and conversion.
func BenchmarkQoderStreamConversion5000Frames(b *testing.B) {
	assets := buildAllQoderBenchAssets(b)

	for _, item := range assets {
		b.Run(item.spec.slug+"/new", func(b *testing.B) {
			var sink qoderBenchStreamSink
			b.SetBytes(int64(len(item.rawStreamSSE)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				sink.Reset()
				if _, err := runNewQoderBenchStream(&sink, item); err != nil || sink.buf.Len() == 0 {
					b.Fatalf("new Qoder stream conversion failed on %s: %v", item.spec.slug, err)
				}
			}
		})
	}
}

// runNewQoderBenchRequest executes convert.NewExchange, ex.UpstreamRequest,
// protocolqoder.DecodePayload, PayloadRequestFromIR, and withEffectiveControls
// to match Executor.executeConverted request preparation. Responses routes
// attach qoderBenchRespID, and any decode, stateless preflight, or capability
// check failure returns an error immediately.
func runNewQoderBenchRequest(spec qoderBenchRouteSpec, body []byte) (QoderPayloadRequest, []byte, error) {
	respID := ""
	if spec.route.Client == protocol.ProtocolOpenAIResponses {
		respID = qoderBenchRespID
	}
	ex, err := convert.NewExchange(body, convert.Options{
		Route:       spec.route,
		ClientModel: spec.clientModel,
		Runtime:     newQoderBenchDeterministicRuntime(respID),
	})
	if err != nil {
		return QoderPayloadRequest{}, nil, err
	}
	upBytes, err := ex.UpstreamRequest()
	if err != nil {
		return QoderPayloadRequest{}, nil, err
	}
	protoReq, err := protocolqoder.DecodePayload(upBytes)
	if err != nil {
		return QoderPayloadRequest{}, nil, err
	}
	req, err := PayloadRequestFromIR(protoReq)
	if err != nil {
		return QoderPayloadRequest{}, nil, err
	}
	req.ResponseID = respID
	effReq, err := req.withEffectiveControls()
	if err != nil {
		return QoderPayloadRequest{}, nil, err
	}
	return effReq, upBytes, nil
}

// runNewQoderBenchStream drives one complete 5,000-frame wrapped Qoder SSE
// response through ReadConvertedStreaming and executionOutput using a freshly
// initialized convert.Exchange so per-attempt loss slices never grow. Request
// setup or stream conversion failures return an error immediately.
func runNewQoderBenchStream(sink upstream.OutputSink, item qoderBenchAssets) (*QoderConvertedResult, error) {
	respID := ""
	if item.spec.route.Client == protocol.ProtocolOpenAIResponses {
		respID = qoderBenchRespID
	}
	mapper := item.newMapper
	ex, err := convert.NewExchange(item.streamReq, convert.Options{
		Route:       item.spec.route,
		ClientModel: item.spec.clientModel,
		Runtime:     newQoderBenchDeterministicRuntime(respID),
		MapUpstreamToolName: func(name string) string {
			if mapper == nil {
				return name
			}
			return mapper(name)
		},
	})
	if err != nil {
		return nil, err
	}

	outCtx := upstream.NewDeferredOutputContext(&executionOutput{sink: sink})
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(item.rawStreamSSE)),
	}
	return ReadConvertedStreaming(context.Background(), outCtx, resp, ConvertedResponseOptions{
		Exchange:  ex,
		RequestID: respID,
	})
}

// buildAllQoderBenchAssets constructs deterministic 2 MiB client request bodies,
// 5,000-frame wrapped Qoder SSE streams, and precomputed tool-name mappers for
// all three Qoder routes. Any setup failure aborts tb immediately.
func buildAllQoderBenchAssets(tb testing.TB) []qoderBenchAssets {
	tb.Helper()

	reqByProto := map[protocol.ProtocolID][]byte{
		protocol.ProtocolAnthropicMessages:     buildQoderBenchMessagesRequest2MB(tb),
		protocol.ProtocolOpenAIResponses:       buildQoderBenchResponsesRequest2MB(tb),
		protocol.ProtocolOpenAIChatCompletions: buildQoderBenchChatRequest2MB(tb),
	}
	rawStream := buildQoderBenchStream5000(tb)

	out := make([]qoderBenchAssets, 0, len(qoderBenchRoutes))
	for _, spec := range qoderBenchRoutes {
		streamReq := buildQoderBenchStreamInitRequest(spec.route.Client)
		newParsed, _, err := runNewQoderBenchRequest(spec, streamReq)
		if err != nil {
			tb.Fatalf("runNewQoderBenchRequest(%s) setup failed: %v", spec.slug, err)
		}
		out = append(out, qoderBenchAssets{
			spec:         spec,
			requestBody:  reqByProto[spec.route.Client],
			rawStreamSSE: rawStream,
			streamReq:    streamReq,
			newMapper:    QoderDeclaredToolNameMapper(newParsed.Tools),
		})
	}
	return out
}

// newQoderBenchDeterministicRuntime returns a deterministic convert.Runtime
// with a fixed UTC clock and monotonic SHA-256 identifier generator for Qoder
// benchmarks. Supplying a non-empty responseID returns that exact value on the
// initial "resp_" identifier request before advancing the sequence counter.
func newQoderBenchDeterministicRuntime(responseID string) convert.Runtime {
	var seq uint64
	return convert.Runtime{
		Now: func() time.Time { return time.Unix(1760000000, 0).UTC() },
		NewID: func(prefix string) string {
			if prefix == "resp_" && responseID != "" && seq == 0 {
				seq++
				return responseID
			}
			seq++
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], seq)
			sum := sha256.Sum256(b[:])
			return fmt.Sprintf("%s%x", prefix, sum[:6])
		},
	}
}

// validateQoderBenchPayload checks structural Qoder PayloadRequest invariants,
// <=64-byte function tool identifiers, JSON Schema/argument validity, stateless
// tool-result pairing, and IR round-trip decoding on upBytes. Any violated
// invariant returns a descriptive string entry; valid payloads return nil.
func validateQoderBenchPayload(upBytes []byte, statelessResponses bool) []string {
	payload, err := protocolqoder.DecodePayload(upBytes)
	if err != nil {
		return []string{"DecodePayload: " + err.Error()}
	}
	var violations []string
	if payload.Model == "" {
		violations = append(violations, "model must be non-empty")
	}
	if payload.MaxTokens <= 0 {
		violations = append(violations, "max_tokens must be positive")
	}
	if _, restrictErr := payload.RestrictAllowedTools(); restrictErr != nil {
		violations = append(violations, "RestrictAllowedTools: "+restrictErr.Error())
	}
	seenTools := make(map[string]bool, len(payload.Tools))
	for i, tool := range payload.Tools {
		fnName := tool.Function.Name
		if fnName == "" || len(fnName) > 64 || !isQoderBenchToolIdent(fnName) {
			violations = append(violations, fmt.Sprintf("tools[%d]: invalid identifier %q", i, fnName))
		}
		if seenTools[fnName] {
			violations = append(violations, fmt.Sprintf("tools[%d]: duplicate identifier %q", i, fnName))
		}
		seenTools[fnName] = true
		var schema map[string]jsontext.Value
		if sErr := jsonv2.Unmarshal(tool.Function.Parameters, &schema, jsontext.AllowInvalidUTF8(true)); sErr != nil {
			violations = append(violations, fmt.Sprintf("tools[%d]: invalid schema: %v", i, sErr))
		}
	}
	seenCalls := make(map[string]bool)
	for i, msg := range payload.Messages {
		switch msg.Role {
		case "user":
		case "assistant":
			for j, tc := range msg.ToolCalls {
				if tc.ID == "" {
					violations = append(violations, fmt.Sprintf("messages[%d].tool_calls[%d]: empty id", i, j))
				} else {
					seenCalls[tc.ID] = true
				}
				var parsed any
				if aErr := jsonv2.Unmarshal([]byte(tc.Function.Arguments), &parsed, jsontext.AllowInvalidUTF8(true)); aErr != nil {
					violations = append(violations, fmt.Sprintf("messages[%d].tool_calls[%d]: invalid args: %v", i, j, aErr))
				}
			}
		case "tool":
			if msg.ToolCallID == "" {
				violations = append(violations, fmt.Sprintf("messages[%d]: empty tool_call_id", i))
			} else if statelessResponses && !seenCalls[msg.ToolCallID] {
				violations = append(violations, fmt.Sprintf("messages[%d]: unpaired tool_call_id %q", i, msg.ToolCallID))
			}
		default:
			violations = append(violations, fmt.Sprintf("messages[%d]: invalid role %q", i, msg.Role))
		}
	}
	if _, err := convert.DecodeUpstreamRequest(protocol.ProtocolQoderChat, upBytes); err != nil {
		violations = append(violations, "DecodeUpstreamRequest: "+err.Error())
	}
	return violations
}

// isQoderBenchToolIdent reports whether s contains only ASCII letters, digits,
// underscores, and hyphens.
func isQoderBenchToolIdent(s string) bool {
	if s == "" {
		return false
	}
	for i := range len(s) {
		ch := s[i]
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '_' && ch != '-' {
			return false
		}
	}
	return true
}

// buildQoderBenchStreamInitRequest returns a compact client request declaring
// lookup_doc for proto, setting store=false on Responses so stateless Qoder
// routing validates cleanly. Non-Messages, non-Responses protocols fall back
// to Chat Completions with stream_options.include_usage=true.
func buildQoderBenchStreamInitRequest(proto protocol.ProtocolID) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"claude-sonnet-4-5","max_tokens":4096,"stream":true,"tools":[{"name":"lookup_doc","description":"Read a documentation chunk","input_schema":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"]}}],"messages":[{"role":"user","content":"Summarize the architecture modules."}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"gpt-5","store":false,"stream":true,"tools":[{"type":"function","name":"lookup_doc","description":"Read a documentation chunk","parameters":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"]}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Summarize the architecture modules."}]}]}`)
	default:
		return []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"lookup_doc","description":"Read a documentation chunk","parameters":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"]}}}],"messages":[{"role":"user","content":"Summarize the architecture modules."}]}`)
	}
}

// buildQoderBenchMessagesRequest2MB constructs a >= 2 MiB Anthropic Messages
// JSON request carrying x-anthropic-billing-header cch checksums, tool schemas,
// thinking carriers, and paired tool_use/tool_result multi-turn history.
// Carrier formatting or JSON marshaling failures abort tb immediately.
func buildQoderBenchMessagesRequest2MB(tb testing.TB) []byte {
	tb.Helper()
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemePlain,
		Text:   "Inspecting Qoder Cosy payload normalization and conversation fingerprinting.",
	})
	if err != nil {
		tb.Fatalf("FormatCarrier failed: %v", err)
	}

	userChunk := strings.Repeat("Qoder module dependency check: gateway/provider -> upstream/qoder -> protocol/codec/qoderchat. ", 70)
	toolOutChunk := strings.Repeat("Verified PayloadRequestFromIR tool enrichment, billing header cch normalization, and stateless rules. ", 90)
	assistantChunk := strings.Repeat("Confirmed multi-turn conversation history and tool pairing invariants hold across Qoder codecs. ", 45)

	var messages []map[string]any
	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < qoderBenchMinRequestBytes+qoderBenchMarginBytes {
		turn++
		callID := fmt.Sprintf("toolu_qoder_bench_%04d", turn)
		uText := fmt.Sprintf("Turn %d context:\n%s", turn, userChunk)
		tRes := fmt.Sprintf("File internal/qoder_%03d/service.go summary:\n%s", turn, toolOutChunk)
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
						"thinking":  "Inspecting Qoder Cosy payload normalization and conversation fingerprinting.",
						"signature": carrier,
					},
					{
						"type":  "tool_use",
						"id":    callID,
						"name":  "read_module_doc",
						"input": map[string]any{"path": fmt.Sprintf("internal/qoder_%03d/service.go", turn), "limit": 120},
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
		"content": "Provide the final Qoder verification summary.",
	})

	payload := map[string]any{
		"model":      "claude-sonnet-4-5",
		"max_tokens": 4096,
		"stream":     true,
		"system": []map[string]any{
			{"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.330.975; cch=1a2b3; cc_entrypoint=sdk-cli;"},
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
		tb.Fatalf("marshal Qoder bench messages request: %v", err)
	}
	return raw
}

// buildQoderBenchResponsesRequest2MB constructs a >= 2 MiB stateless OpenAI
// Responses JSON request (store=false) carrying instructions, function and
// custom tools, reasoning carriers, and paired function_call/output items.
// Carrier formatting or JSON marshaling failures abort tb immediately.
func buildQoderBenchResponsesRequest2MB(tb testing.TB) []byte {
	tb.Helper()
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemePlain,
		Text:   "Checking stateless Qoder Responses tool pairing and trc1:plain carriers.",
	})
	if err != nil {
		tb.Fatalf("FormatCarrier failed: %v", err)
	}

	userChunk := strings.Repeat("Stateless Qoder Responses history verification across multi-turn agent tool loops. ", 70)
	toolOutChunk := strings.Repeat("Module analysis output: pure leaf package constraints and Qoder tool lowering verified. ", 90)
	assistantChunk := strings.Repeat("Synthesized findings for turn with complete tool result correlation and zero schema drift. ", 45)

	var input []map[string]any
	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < qoderBenchMinRequestBytes+qoderBenchMarginBytes {
		turn++
		callID := fmt.Sprintf("call_qoder_resp_%04d", turn)
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
				"id":                fmt.Sprintf("rs_qoder_bench_%04d", turn),
				"encrypted_content": carrier,
				"summary": []map[string]any{
					{"type": "summary_text", "text": "Checking stateless Qoder Responses tool pairing and trc1:plain carriers."},
				},
			},
			map[string]any{
				"type":      "function_call",
				"id":        fmt.Sprintf("fc_qoder_bench_%04d", turn),
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
			{"type": "input_text", "text": "Summarize all verified Qoder boundaries."},
		},
	})

	payload := map[string]any{
		"model":        "gpt-5",
		"store":        false,
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
		tb.Fatalf("marshal Qoder bench responses request: %v", err)
	}
	return raw
}

// buildQoderBenchChatRequest2MB constructs a >= 2 MiB OpenAI Chat Completions
// JSON request carrying system prompt, function tools, reasoning_content tool
// turns, and paired tool messages. Serialization failures abort tb immediately.
func buildQoderBenchChatRequest2MB(tb testing.TB) []byte {
	tb.Helper()

	userChunk := strings.Repeat("Chat Completions Qoder multi-turn context inspecting forwarding and codec invariants. ", 70)
	toolOutChunk := strings.Repeat("Detailed inspection output covering Qoder wrapped SSE framing and token usage buckets. ", 90)
	assistantChunk := strings.Repeat("Completed verification for turn and recorded invariant compliance across Qoder codecs. ", 45)

	messages := []map[string]any{
		{
			"role":    "system",
			"content": "You are a precision software architecture auditor.",
		},
	}

	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < qoderBenchMinRequestBytes+qoderBenchMarginBytes {
		turn++
		callID := fmt.Sprintf("call_qoder_chat_%04d", turn)
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
				"reasoning_content": "Evaluating tool call parameters for Qoder repository slice inspection.",
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
		"content": "Deliver the final Qoder verification report.",
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
		tb.Fatalf("marshal Qoder bench chat request: %v", err)
	}
	return raw
}

// buildQoderBenchStream5000 builds a wrapped Qoder upstream SSE stream with
// exactly 5,000 frames (1,000 reasoning chunks + 3,000 visible text chunks +
// 997 fragmented tool_call chunks + 1 finish_reason chunk + 1 usage chunk + 1
// wrapped [DONE] sentinel). Fragment quoting failures abort tb immediately.
func buildQoderBenchStream5000(tb testing.TB) []byte {
	tb.Helper()
	var buf bytes.Buffer
	emitInner := func(inner string) {
		fmt.Fprintf(&buf, "data: {\"body\":%q}\n\n", inner)
	}

	// Frame 1: role + first reasoning chunk.
	emitInner(`{"choices":[{"delta":{"role":"assistant","reasoning_content":"step "}}]}`)
	// Frames 2..1,000: 999 reasoning chunks.
	for range 999 {
		emitInner(`{"choices":[{"delta":{"reasoning_content":"step "}}]}`)
	}
	// Frames 1,001..4,000: 3,000 visible text chunks.
	for range 3000 {
		emitInner(`{"choices":[{"delta":{"content":"token "}}]}`)
	}
	// Frame 4,001: tool_call header chunk.
	emitInner(`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_qoder_stream_1","type":"function","function":{"name":"lookup_doc","arguments":""}}]}}]}`)
	// Frames 4,002..4,997: 996 argument fragments.
	frags := qoderBenchFragmentedItemsJSON(996)
	for _, frag := range frags {
		quoted, err := json.Marshal(frag)
		if err != nil {
			tb.Fatalf("marshal Qoder tool fragment: %v", err)
		}
		emitInner(fmt.Sprintf(`{"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":%s}}]}}]}`, quoted))
	}
	// Frame 4,998: finish_reason chunk.
	emitInner(`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`)
	// Frame 4,999: usage chunk with cached and cacheable token details.
	emitInner(`{"usage":{"prompt_tokens":2560,"completion_tokens":4990,"prompt_tokens_details":{"cached_tokens":512,"cacheable_tokens":128},"completion_tokens_details":{"reasoning_tokens":1000}}}`)
	// Frame 5,000: wrapped [DONE] sentinel.
	emitInner("[DONE]")
	return buf.Bytes()
}

// qoderBenchFragmentedItemsJSON splits a valid {"items":[...]} JSON object into
// exactly n non-empty string fragments for streaming tool-argument deltas.
// Callers must pass n >= 2 so the opening and closing delimiters each occupy one slot.
func qoderBenchFragmentedItemsJSON(n int) []string {
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

// qoderBenchCountSSEFrames counts non-empty SSE frames scanned by sse.NewReader
// from rawSSE in wire appearance order. Scanning stops at EOF or the first SSE
// reader error and returns the number of complete frames observed so far.
func qoderBenchCountSSEFrames(rawSSE []byte) int {
	rd := sse.NewReader(bytes.NewReader(rawSSE))
	n := 0
	for rd.Scan() {
		n++
	}
	return n
}

// qoderBenchParseSSEFrames parses rawSSE through sse.NewReader into an ordered
// slice of sse.Frame values for client stream grammar validation. Any scanner
// error reported by rd.Err() fails tb immediately via require.NoError.
func qoderBenchParseSSEFrames(tb testing.TB, rawSSE []byte) []sse.Frame {
	tb.Helper()
	rd := sse.NewReader(bytes.NewReader(rawSSE))
	var frames []sse.Frame
	for rd.Scan() {
		frames = append(frames, rd.Frame())
	}
	require.NoError(tb, rd.Err())
	return frames
}
