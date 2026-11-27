package antigravity

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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// benchmark_scenario_test.go benchmarks Antigravity IR request and streaming
// response conversion across execute.go, payload.go, response_converted.go, and
// response_stream.go across all six {Messages, Responses, Chat} x {claude-*,
// gemini-*} route combinations.
//
// Request benchmarks measure the unified single-hop IR pipeline
// (convert.NewExchange, InjectIdentityPatchToGeminiRequestWithPrompt,
// CleanGeminiRequest, and WrapV1InternalRequest) on deterministic 2 MiB
// multi-turn client payloads. Streaming benchmarks drive 5,000-frame v1internal
// SSE responses end to end through Executor.Execute in ModeConvertedResponse
// (ReadConvertedStreaming and Exchange.Stream), while
// TestBenchmarkAntigravityPayloadsValid verifies payload sizes, frame counts,
// v1internal and strict Gemini request invariants, client SSE grammar, billing
// usage accounting, and bounded iteration state during standard unit test runs.

const (
	// agBenchMinRequestBytes, agBenchMarginBytes, agBenchStreamFrames, and
	// agBenchProjectID govern payload sizing and deterministic envelope metadata
	// for Stage 4 Antigravity 2 MiB request and 5,000-frame stream benchmarks.
	agBenchMinRequestBytes = 2 * 1024 * 1024          // minimum 2 MiB serialized client request payload
	agBenchMarginBytes     = 65536                    // extra turn target keeping marshaled JSON comfortably above 2 MiB
	agBenchStreamFrames    = 5000                     // exact upstream v1internal SSE frame count per stream benchmark
	agBenchProjectID       = "antigravity-bench-proj" // deterministic GCP project ID for v1internal wrapping
)

// agBenchModelRulesView loads the embedded offline model catalog and official
// attribute supplements once so Antigravity dialect and thinking resolutions
// match production modelrules deterministically.
var agBenchModelRulesView = sync.OnceValue(func() modelrules.View {
	raw, err := modelcatalog.Offline()
	if err != nil {
		panic(err)
	}
	cat, err := modelcatalog.Parse(raw)
	if err != nil {
		panic(err)
	}
	supplements := make(map[string]json.RawMessage)
	if err := json.Unmarshal(modelcatalog.Supplements(), &supplements); err != nil {
		panic(err)
	}
	for model, patch := range supplements {
		if err := cat.ApplyAttributeSupplement(model, patch); err != nil {
			panic(err)
		}
	}
	for model, patch := range supplements {
		for _, alias := range cat.FirstPartyAliases(model) {
			if err := cat.ApplyAttributeSupplement(alias, patch); err != nil {
				panic(err)
			}
		}
	}
	return modelrules.NewView(cat.Publication())
})

// agBenchRouteSpec describes one (client protocol, target model family)
// Antigravity conversion route evaluated by the benchmarks.
type agBenchRouteSpec struct {
	slug        string                           // stable route benchmark identifier
	clientProto protocol.ProtocolID              // inbound client wire protocol
	clientModel string                           // model name echoed back to the client
	targetModel string                           // mapped Antigravity upstream model
	route       convert.Route                    // pre-resolved convert.Route with SchemaAntigravity Dialect
	validation  testkit.RequestValidationOptions // strict inner Gemini request validator rules
}

// agBenchRouteAssets bundles the prebuilt 2 MiB client request, 5,000-frame
// v1internal SSE stream, and compact stream-init request for one Antigravity route.
type agBenchRouteAssets struct {
	spec         agBenchRouteSpec // evaluated Antigravity route configuration
	requestBody  []byte           // >= 2 MiB client request JSON payload
	rawStreamSSE []byte           // 5,000-frame v1internal upstream SSE stream
	streamReq    []byte           // compact client request initializing stream state
	outStreamReq []byte           // compact wrapped v1internal body passed to AttemptInput.Body
}

// agBenchStreamSink captures raw client SSE bytes emitted through Executor.Execute
// with minimal buffer overhead during streaming benchmark loops.
type agBenchStreamSink struct {
	buf bytes.Buffer // accumulated downstream SSE payload bytes
}

// Begin accepts initial output head metadata without copying response headers
// and returns nil immediately. Omitting header clones lets streaming benchmark
// loops measure SSE payload conversion without per-attempt map allocations.
func (s *agBenchStreamSink) Begin(upstream.OutputHead) error {
	return nil
}

// Emit appends non-empty payload bytes from ev.Data onto s.buf and returns any
// underlying buffer write error. Flush-only events carrying empty Data slices
// return nil immediately without modifying s.buf.
func (s *agBenchStreamSink) Emit(ev upstream.OutputEvent) error {
	if len(ev.Data) > 0 {
		_, err := s.buf.Write(ev.Data)
		return err
	}
	return nil
}

// Reset rewinds s.buf length to zero while retaining its allocated backing
// byte slice. Consecutive benchmark iterations reuse the existing buffer
// capacity without reallocating downstream SSE output storage.
func (s *agBenchStreamSink) Reset() {
	s.buf.Reset()
}

// TestBenchmarkAntigravityPayloadsValid builds the 2 MiB request payloads and
// 5,000-frame v1internal SSE streams across all six Stage 4 Antigravity routes.
// Every payload must satisfy size and frame-count requirements, pass v1internal
// envelope and strict Gemini request validators plus client stream grammar
// checks, report positive billing TokenUsage counters, and avoid unbounded
// slice growth across repeated stream iterations.
func TestBenchmarkAntigravityPayloadsValid(t *testing.T) {
	assets := buildAllAgBenchAssets(t)
	require.Len(t, assets, 6)

	for _, item := range assets {
		t.Run(item.spec.slug, func(t *testing.T) {
			require.GreaterOrEqual(t, len(item.requestBody), agBenchMinRequestBytes,
				"Antigravity request payload for %s must be at least 2 MiB", item.spec.slug)

			require.Equal(t, agBenchStreamFrames, agBenchCountSSEFrames(item.rawStreamSSE),
				"Antigravity upstream stream for %s must contain exactly 5,000 SSE frames", item.spec.slug)

			newWrapped, err := runNewAgBenchRequest(item.spec, item.requestBody)
			require.NoError(t, err)
			innerBytes := verifyAgBenchV1InternalEnvelope(t, item.spec.targetModel, newWrapped)
			reqViol := testkit.ValidateGeminiRequest(innerBytes, item.spec.validation)
			require.Empty(t, reqViol, "new Antigravity 2 MiB request violated %s rules: %v", item.spec.slug, reqViol)

			var sink agBenchStreamSink
			var firstLen int
			var newRes upstream.AttemptResult
			for iter := range 2 {
				sink.Reset()
				newRes, err = runNewAgBenchStream(&sink, item)
				require.NoError(t, err)
				require.Positive(t, sink.buf.Len())
				if iter == 0 {
					firstLen = sink.buf.Len()
				} else {
					require.Equal(t, firstLen, sink.buf.Len(),
						"repeated Antigravity stream runs must emit deterministic byte counts without state drift on %s", item.spec.slug)
				}
			}

			outFrames := agBenchParseSSEFrames(t, sink.buf.Bytes())
			gramViol := testkit.ValidateStreamGrammar(item.spec.clientProto, outFrames, testkit.GrammarOptions{})
			require.Empty(t, gramViol, "new Antigravity 5,000-frame stream violated %s grammar: %v", item.spec.slug, gramViol)

			require.True(t, newRes.HasUsage)
			require.Positive(t, newRes.Usage.InputTokens+newRes.Usage.CacheReadInputTokens)
			require.Positive(t, newRes.Usage.OutputTokens)
		})
	}
}

// BenchmarkAntigravityRequestConversion2MB benchmarks 2 MiB multi-turn request
// conversion across all six Antigravity routes through convert.NewExchange and
// identity/clean/wrap preparation. Each sub-benchmark reports per-operation
// latency, throughput, and heap allocations.
func BenchmarkAntigravityRequestConversion2MB(b *testing.B) {
	assets := buildAllAgBenchAssets(b)

	for _, item := range assets {
		b.Run(item.spec.slug+"/new", func(b *testing.B) {
			b.SetBytes(int64(len(item.requestBody)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				out, err := runNewAgBenchRequest(item.spec, item.requestBody)
				if err != nil || len(out) == 0 {
					b.Fatalf("new Antigravity request conversion failed on %s: %v", item.spec.slug, err)
				}
			}
		})
	}
}

// BenchmarkAntigravityStreamConversion5000Frames benchmarks 5,000-frame
// v1internal SSE response streaming across all six Antigravity routes through
// Executor.Execute in ModeConvertedResponse (ReadConvertedStreaming +
// Exchange.Stream). Each sub-benchmark resets its output sink per iteration and
// measures end-to-end SSE unwrapping, conversion, and wire serialization.
func BenchmarkAntigravityStreamConversion5000Frames(b *testing.B) {
	assets := buildAllAgBenchAssets(b)

	for _, item := range assets {
		b.Run(item.spec.slug+"/new", func(b *testing.B) {
			var sink agBenchStreamSink
			b.SetBytes(int64(len(item.rawStreamSSE)))
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				sink.Reset()
				if _, err := runNewAgBenchStream(&sink, item); err != nil || sink.buf.Len() == 0 {
					b.Fatalf("new Antigravity stream conversion failed on %s: %v", item.spec.slug, err)
				}
			}
		})
	}
}

// runNewAgBenchRequest executes the IR request pipeline across
// convert.NewExchange, ex.UpstreamRequest, lossless identity injection,
// CleanGeminiRequest, and WrapV1InternalRequest. CleanGeminiRequest errors fall
// back to the identity-patched bytes matching prepareAntigravityWireBody, while
// any exchange, identity-patch, or envelope failure returns an error.
func runNewAgBenchRequest(spec agBenchRouteSpec, body []byte) ([]byte, error) {
	ex, err := convert.NewExchange(body, convert.Options{
		Route:       spec.route,
		ClientModel: spec.clientModel,
		Runtime:     newAgBenchDeterministicRuntime(),
	})
	if err != nil {
		return nil, err
	}
	rawGemini, err := ex.UpstreamRequest()
	if err != nil {
		return nil, err
	}
	patched, err := InjectIdentityPatchToGeminiRequestWithPrompt(rawGemini, "")
	if err != nil {
		return nil, err
	}
	// Retain patched bytes if CleanGeminiRequest reports an error, matching
	// prepareAntigravityWireBody fallback behavior after CleanAntigravitySchema has already sanitized schemas.
	if cleaned, cleanErr := CleanGeminiRequest(patched); cleanErr == nil {
		patched = cleaned
	}
	return WrapV1InternalRequest(agBenchProjectID, spec.targetModel, patched)
}

// runNewAgBenchStream drives one complete 5,000-frame v1internal SSE response
// through Executor.Execute in ModeConvertedResponse using a freshly initialized
// convert.Exchange so per-attempt loss slices never accumulate across iterations.
// Any request setup or stream execution failure returns an error alongside the
// partial AttemptResult.
func runNewAgBenchStream(sink upstream.OutputSink, item agBenchRouteAssets) (upstream.AttemptResult, error) {
	ex, err := convert.NewExchange(item.streamReq, convert.Options{
		Route:       item.spec.route,
		ClientModel: item.spec.clientModel,
		Runtime:     newAgBenchDeterministicRuntime(),
	})
	if err != nil {
		return upstream.AttemptResult{}, err
	}

	target := &Target{
		ProviderID: 1,
		Model:      item.spec.targetModel,
		Mode:       ModeConvertedResponse,
		Exchange: func(context.Context) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(item.rawStreamSSE)),
			}, nil
		},
		Converted: ConvertedOptions{
			Exchange:  ex,
			RequestID: "req_ag_bench_stream",
		},
	}
	return (Executor{}).Execute(context.Background(), upstream.AttemptInput{
		Protocol:      item.spec.clientProto,
		Body:          item.outStreamReq,
		ResponseModel: item.spec.clientModel,
		Stream:        true,
		Target:        target,
	}, sink)
}

// buildAllAgBenchAssets constructs all six Antigravity route specs, 2 MiB
// multi-turn client request bodies, and 5,000-frame v1internal SSE streams. Any
// setup or marshaling failure aborts tb immediately.
func buildAllAgBenchAssets(tb testing.TB) []agBenchRouteAssets {
	tb.Helper()

	reqByProto := map[protocol.ProtocolID][]byte{
		protocol.ProtocolAnthropicMessages:     buildAgBenchMessagesRequest2MB(tb),
		protocol.ProtocolOpenAIResponses:       buildAgBenchResponsesRequest2MB(tb),
		protocol.ProtocolOpenAIChatCompletions: buildAgBenchChatRequest2MB(tb),
	}
	streamByModel := map[string][]byte{
		"claude-sonnet-4-5": buildAgBenchV1InternalStream5000(tb, "claude-sonnet-4-5"),
		"gemini-2.5-flash":  buildAgBenchV1InternalStream5000(tb, "gemini-2.5-flash"),
	}

	specs := buildAgBenchRouteSpecs()
	out := make([]agBenchRouteAssets, 0, len(specs))
	for _, spec := range specs {
		streamReq := buildAgBenchStreamInitRequest(spec.clientProto)
		outStreamReq, err := runNewAgBenchRequest(spec, streamReq)
		if err != nil {
			tb.Fatalf("runNewAgBenchRequest(%s) stream-init failed: %v", spec.slug, err)
		}
		out = append(out, agBenchRouteAssets{
			spec:         spec,
			requestBody:  reqByProto[spec.clientProto],
			rawStreamSSE: streamByModel[spec.targetModel],
			streamReq:    streamReq,
			outStreamReq: outStreamReq,
		})
	}
	return out
}

// buildAgBenchRouteSpecs returns the six canonical Antigravity
// (client protocol x model family) route specifications.
func buildAgBenchRouteSpecs() []agBenchRouteSpec {
	// pair groups static route metadata before modelrules and dialect resolution.
	type pair struct {
		slug        string              // stable route identifier
		clientProto protocol.ProtocolID // inbound client wire protocol
		clientModel string              // client-echoed model identifier
		targetModel string              // mapped upstream Antigravity model
	}
	pairs := []pair{
		{"antigravity_messages_claude", protocol.ProtocolAnthropicMessages, "claude-sonnet-4-5", "claude-sonnet-4-5"},
		{"antigravity_messages_gemini", protocol.ProtocolAnthropicMessages, "gemini-2.5-flash", "gemini-2.5-flash"},
		{"antigravity_responses_claude", protocol.ProtocolOpenAIResponses, "gpt-5", "claude-sonnet-4-5"},
		{"antigravity_responses_gemini", protocol.ProtocolOpenAIResponses, "gpt-5", "gemini-2.5-flash"},
		{"antigravity_chat_claude", protocol.ProtocolOpenAIChatCompletions, "gpt-4o", "claude-sonnet-4-5"},
		{"antigravity_chat_gemini", protocol.ProtocolOpenAIChatCompletions, "gpt-4o", "gemini-2.5-flash"},
	}

	view := agBenchModelRulesView()
	out := make([]agBenchRouteSpec, 0, len(pairs))
	for _, p := range pairs {
		res := view.Resolve(modelrules.Target{
			Platform:       "antigravity",
			Model:          p.targetModel,
			ClientProtocol: string(p.clientProto),
		}, modelcatalog.Attributes{})
		dialect := buildAgBenchDialect(res, p.clientProto, p.targetModel)
		rules := []string{"antigravity-schema"}
		if strings.HasPrefix(p.targetModel, "gemini-") {
			rules = append(rules, "gemini-signature")
		}
		out = append(out, agBenchRouteSpec{
			slug:        p.slug,
			clientProto: p.clientProto,
			clientModel: p.clientModel,
			targetModel: p.targetModel,
			route: convert.Route{
				Client:   p.clientProto,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  dialect,
			},
			validation: testkit.RequestValidationOptions{DialectRules: rules},
		})
	}
	return out
}

// buildAgBenchDialect constructs the Antigravity convert.Dialect matching
// gatewayprovider.ConversionDialect from a resolved modelrules.Resolution and
// mappedModel prefix. Claude targets select SchemeAntigravityClaude with
// ForceBudget=true and empty PlaceholderToken, whereas Gemini targets select
// SchemeGemini with gemini.DummyThoughtSignature.
func buildAgBenchDialect(
	res modelrules.Resolution,
	source protocol.ProtocolID,
	mappedModel string,
) convert.Dialect {
	attrs := res.Attributes
	minB, maxB := attrs.ReasoningBudget()
	isClaude := strings.HasPrefix(mappedModel, "claude-")

	agLimit := 65000
	if isClaude || source == protocol.ProtocolOpenAIChatCompletions {
		agLimit = modelrules.AntigravityDefaultOutputLimit
	}
	outputLimit := modelcatalog.MinimumLimit(attrs.OutputLimit, &agLimit)

	budgetPadding := res.Rules.BudgetPadding
	if budgetPadding == 0 {
		budgetPadding = MaxTokensBudgetPadding
	}

	replayScheme := ir.SchemeGemini
	placeholder := gemini.DummyThoughtSignature
	if isClaude {
		replayScheme = ir.SchemeAntigravityClaude
		placeholder = ""
	}

	var minVal, maxVal int
	if minB != nil {
		minVal = *minB
	}
	if maxB != nil {
		maxVal = *maxB
	}

	return convert.Dialect{
		Name:             "antigravity",
		Tools:            convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch,
		ToolNameMaxBytes: 64,
		ReplayScheme:     replayScheme,
		ReasoningHistory: convert.HistoryNone,
		SchemaDialect:    convert.SchemaAntigravity,
		Reasoning: (convert.ReasoningCapabilities{
			ControlsKnown:  attrs.ReasoningOptions != nil,
			Supported:      attrs.Reasoning,
			Toggle:         attrs.HasReasoningOption("toggle"),
			Efforts:        attrs.Efforts(),
			Budget:         attrs.HasReasoningOption("budget_tokens"),
			BudgetMin:      minVal,
			BudgetMax:      maxVal,
			ForceBudget:    isClaude,
			AdaptiveBudget: res.Rules.AdaptiveThinkingBudget,
			OutputLimit:    outputLimit,
			BudgetPadding:  budgetPadding,
		}).Clone(),
		DropTemperature:   (attrs.Temperature != nil && !*attrs.Temperature) || slices.Contains(res.Rules.DropFields, "temperature"),
		DropTopP:          (attrs.TopP != nil && !*attrs.TopP) || slices.Contains(res.Rules.DropFields, "top_p") || slices.Contains(res.Rules.DropFields, "topP"),
		DropTopK:          slices.Contains(res.Rules.DropFields, "top_k") || slices.Contains(res.Rules.DropFields, "topK"),
		OmitStopSequences: res.Rules.OmitStopSequences || slices.Contains(res.Rules.DropFields, "stop") || slices.Contains(res.Rules.DropFields, "stop_sequences") || slices.Contains(res.Rules.DropFields, "stopSequences"),
		PlaceholderToken:  placeholder,
	}
}

// newAgBenchDeterministicRuntime returns a deterministic convert.Runtime with a
// fixed UTC clock and monotonic SHA-256 identifier generator. Each NewID call
// increments a local counter so repeated benchmark runs produce identical IDs.
func newAgBenchDeterministicRuntime() convert.Runtime {
	var seq uint64
	return convert.Runtime{
		Now: func() time.Time { return time.Unix(1760000000, 0).UTC() },
		NewID: func(prefix string) string {
			seq++
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], seq)
			sum := sha256.Sum256(b[:])
			return fmt.Sprintf("%s%x", prefix, sum[:6])
		},
	}
}

// verifyAgBenchV1InternalEnvelope checks outer v1internal envelope fields on
// wrapped and returns the inner GenerateContent request JSON bytes. Any missing
// or mismatched outer envelope field fails t immediately.
func verifyAgBenchV1InternalEnvelope(t *testing.T, targetModel string, wrapped []byte) []byte {
	t.Helper()
	var env struct {
		Project     string         `json:"project"`     // GCP project identifier
		Model       string         `json:"model"`       // mapped upstream model
		UserAgent   string         `json:"userAgent"`   // fixed protocol userAgent
		RequestType string         `json:"requestType"` // fixed outer requestType
		RequestID   string         `json:"requestId"`   // generated agent request ID
		Request     jsontext.Value `json:"request"`     // inner GenerateContent payload
	}
	dec := jsontext.NewDecoder(bytes.NewReader(wrapped), jsontext.AllowInvalidUTF8(true))
	require.NoError(t, jsonv2.UnmarshalDecode(dec, &env))
	require.Equal(t, agBenchProjectID, env.Project)
	require.Equal(t, targetModel, env.Model)
	require.Equal(t, "antigravity", env.UserAgent)
	require.Equal(t, "agent", env.RequestType)
	require.True(t, strings.HasPrefix(env.RequestID, "agent-"))
	require.NotEmpty(t, env.Request)

	var inner map[string]jsontext.Value
	require.NoError(t, jsonv2.Unmarshal(env.Request, &inner, jsontext.AllowInvalidUTF8(true)))
	_, hasSessionID := inner["sessionId"]
	require.False(t, hasSessionID, "inner Antigravity request must omit sessionId")
	return []byte(env.Request)
}

// buildAgBenchStreamInitRequest returns a compact client request declaring
// lookup_doc for proto so stream conversion benchmarks initialize consistent
// tool context on convert.Exchange. Non-Messages, non-Responses protocols fall
// back to Chat Completions with stream_options.include_usage=true.
func buildAgBenchStreamInitRequest(proto protocol.ProtocolID) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"claude-sonnet-4-5","max_tokens":4096,"stream":true,"tools":[{"name":"lookup_doc","description":"Read a documentation chunk","input_schema":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"]}}],"messages":[{"role":"user","content":"Summarize the architecture modules."}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"gpt-5","stream":true,"tools":[{"type":"function","name":"lookup_doc","description":"Read a documentation chunk","parameters":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"]}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Summarize the architecture modules."}]}]}`)
	default:
		return []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"lookup_doc","description":"Read a documentation chunk","parameters":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}},"required":["items"]}}}],"messages":[{"role":"user","content":"Summarize the architecture modules."}]}`)
	}
}

// buildAgBenchMessagesRequest2MB constructs a >= 2 MiB Anthropic Messages JSON
// request carrying billing-header and discrete system blocks, constrained tool
// schemas, sealed thinking carriers, and paired tool_use/tool_result turns.
// Carrier formatting or JSON marshaling failures abort tb immediately.
func buildAgBenchMessagesRequest2MB(tb testing.TB) []byte {
	tb.Helper()
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAntigravityClaude,
		Data:   "ag_claude_sig_bench_2mb",
		Text:   "Inspecting Antigravity v1internal schema and tool call invariants.",
	})
	if err != nil {
		tb.Fatalf("FormatCarrier failed: %v", err)
	}

	userChunk := strings.Repeat("Antigravity module boundary check: googleforward -> upstream/antigravity -> protocol/convert. ", 70)
	toolOutChunk := strings.Repeat("Verified v1internal envelope wrapping, lowercase schema constraints, and CallMemory preservation. ", 90)
	assistantChunk := strings.Repeat("Confirmed single-hop GenerateContent encoding and stream unwrapping invariants hold. ", 45)

	var messages []map[string]any
	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < agBenchMinRequestBytes+agBenchMarginBytes {
		turn++
		callID := fmt.Sprintf("toolu_ag_bench_%04d", turn)
		uText := fmt.Sprintf("Turn %d context:\n%s", turn, userChunk)
		tRes := fmt.Sprintf("File internal/ag_%03d/service.go summary:\n%s", turn, toolOutChunk)
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
						"thinking":  "Inspecting Antigravity v1internal schema and tool call invariants.",
						"signature": carrier,
					},
					{
						"type":  "tool_use",
						"id":    callID,
						"name":  "read_module_doc",
						"input": map[string]any{"path": fmt.Sprintf("internal/ag_%03d/service.go", turn), "limit": 120},
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
		"content": "Provide the final Antigravity verification summary.",
	})

	payload := map[string]any{
		"model":      "claude-sonnet-4-5",
		"max_tokens": 4096,
		"stream":     true,
		"system": []map[string]any{
			{"type": "text", "text": "x-anthropic-billing-header: cc_version=2.1.330.975; cc_entrypoint=sdk-cli;"},
			{"type": "text", "text": "You are a precision software architecture auditor."},
		},
		"tools": []map[string]any{
			{
				"name":        "read_module_doc",
				"description": "Read a repository source or documentation slice.",
				"input_schema": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path":  map[string]any{"type": "string", "minLength": 3},
						"limit": map[string]any{"type": "integer", "minimum": 1},
					},
					"required": []string{"path", "limit"},
				},
			},
		},
		"messages": messages,
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		tb.Fatalf("marshal Antigravity bench messages request: %v", err)
	}
	return raw
}

// buildAgBenchResponsesRequest2MB constructs a >= 2 MiB OpenAI Responses JSON
// request carrying instructions, function and custom tools, reasoning carriers,
// and paired function_call/function_call_output multi-turn history. Carrier
// formatting or JSON marshaling failures abort tb immediately.
func buildAgBenchResponsesRequest2MB(tb testing.TB) []byte {
	tb.Helper()
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeGemini,
		Data:   "ag_gemini_sig_bench_2mb",
	})
	if err != nil {
		tb.Fatalf("FormatCarrier failed: %v", err)
	}

	userChunk := strings.Repeat("Antigravity Responses history verification across multi-turn agent tool loops and schema constraints. ", 70)
	toolOutChunk := strings.Repeat("Module analysis output: v1internal envelope and CleanAntigravitySchema verified losslessly. ", 90)
	assistantChunk := strings.Repeat("Synthesized findings for turn with complete tool result correlation and zero schema drift. ", 45)

	var input []map[string]any
	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < agBenchMinRequestBytes+agBenchMarginBytes {
		turn++
		callID := fmt.Sprintf("call_ag_resp_%04d", turn)
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
				"id":                fmt.Sprintf("rs_ag_bench_%04d", turn),
				"encrypted_content": carrier,
				"summary": []map[string]any{
					{"type": "summary_text", "text": "Checking Antigravity tool call pairing and thoughtSignature continuity."},
				},
			},
			map[string]any{
				"type":      "function_call",
				"id":        fmt.Sprintf("fc_ag_bench_%04d", turn),
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
			{"type": "input_text", "text": "Summarize all verified Antigravity boundaries."},
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
				"parameters": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"path":  map[string]any{"type": "string", "minLength": 3},
						"limit": map[string]any{"type": "integer", "minimum": 1},
					},
					"required": []string{"path", "limit"},
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
		tb.Fatalf("marshal Antigravity bench responses request: %v", err)
	}
	return raw
}

// buildAgBenchChatRequest2MB constructs a >= 2 MiB OpenAI Chat Completions JSON
// request carrying system instructions, function tools, reasoning_content tool
// turns, and paired tool result messages. Serialization failures abort tb
// immediately.
func buildAgBenchChatRequest2MB(tb testing.TB) []byte {
	tb.Helper()

	userChunk := strings.Repeat("Chat Completions Antigravity multi-turn context inspecting forwarding and schema invariants. ", 70)
	toolOutChunk := strings.Repeat("Detailed inspection output covering v1internal unwrapping, SSE framing, and token usage buckets. ", 90)
	assistantChunk := strings.Repeat("Completed verification for turn and recorded invariant compliance across target models. ", 45)

	messages := []map[string]any{
		{
			"role":    "system",
			"content": "You are a precision software architecture auditor.",
		},
	}

	sizeEst := 0
	turn := 0
	// Pad estimated turn bytes by 64 KiB so marshaled JSON always exceeds 2 MiB.
	for sizeEst < agBenchMinRequestBytes+agBenchMarginBytes {
		turn++
		callID := fmt.Sprintf("call_ag_chat_%04d", turn)
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
				"reasoning_content": "Evaluating tool call parameters for Antigravity slice inspection.",
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
		"content": "Deliver the final Antigravity verification report.",
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
							"path":  map[string]any{"type": "string", "minLength": 3},
							"limit": map[string]any{"type": "integer", "minimum": 1},
						},
						"required": []string{"path", "limit"},
					},
				},
			},
		},
		"messages": messages,
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		tb.Fatalf("marshal Antigravity bench chat request: %v", err)
	}
	return raw
}

// buildAgBenchV1InternalStream5000 builds an Antigravity v1internal upstream
// SSE stream with exactly 5,000 frames (1,000 thought chunks, 3,990 visible
// text chunks, 8 functionCall chunks, 1 finishReason chunk, and 1 sparse
// usageMetadata tail chunk) for modelVersion. Distinct per-chunk prefixes keep
// cumulative-prefix deduplication from suppressing incremental stream deltas.
func buildAgBenchV1InternalStream5000(tb testing.TB, modelVersion string) []byte {
	tb.Helper()
	var out []byte
	emitWrapped := func(inner string) {
		wrapped := fmt.Sprintf(`{"responseId":"ag_bench_5000","modelVersion":%q,"response":%s}`, modelVersion, inner)
		out = sse.Append(out, sse.Frame{Data: []byte(wrapped)})
	}

	// Frames 1..999: 999 incremental thought chunks.
	for i := range 999 {
		emitWrapped(fmt.Sprintf(`{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"s%04d ","thought":true}]}}]}`, i))
	}
	// Frame 1,000: signed final thought chunk.
	emitWrapped(`{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"s0999 ","thought":true,"thoughtSignature":"ag_sig_bench_5000"}]}}]}`)

	// Frames 1,001..4,990: 3,990 incremental visible text chunks.
	for i := range 3990 {
		emitWrapped(fmt.Sprintf(`{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"t%04d "}]}}]}`, i))
	}

	// Frames 4,991..4,998: 8 functionCall chunks (step-initial call carries thoughtSignature).
	for i := range 8 {
		sigField := ""
		if i == 0 {
			sigField = `,"thoughtSignature":"ag_fc_sig_bench_1"`
		}
		emitWrapped(fmt.Sprintf(`{"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"call_ag_stream_%d","name":"lookup_doc","args":{"items":["doc_%02d_a","doc_%02d_b"]}}%s}]}}]}`, i+1, i, i, sigField))
	}

	// Frame 4,999: finishReason chunk with initial usageMetadata.
	emitWrapped(`{"candidates":[{"index":0,"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":2560,"cachedContentTokenCount":512,"candidatesTokenCount":4000,"totalTokenCount":6560}}`)

	// Frame 5,000: sparse usageMetadata tail frame updating output and thought counts.
	emitWrapped(`{"usageMetadata":{"candidatesTokenCount":4990,"thoughtsTokenCount":1000,"totalTokenCount":8550}}`)
	return out
}

// agBenchCountSSEFrames counts non-empty SSE frames scanned by sse.NewReader
// from rawSSE in wire appearance order. Scanning stops at EOF or the first SSE
// reader error and returns the number of complete frames observed so far.
func agBenchCountSSEFrames(rawSSE []byte) int {
	rd := sse.NewReader(bytes.NewReader(rawSSE))
	n := 0
	for rd.Scan() {
		n++
	}
	return n
}

// agBenchParseSSEFrames parses rawSSE through sse.NewReader into an ordered
// slice of sse.Frame values for client stream grammar validation. Any scanner
// error reported by rd.Err() fails tb immediately via require.NoError.
func agBenchParseSSEFrames(tb testing.TB, rawSSE []byte) []sse.Frame {
	tb.Helper()
	rd := sse.NewReader(bytes.NewReader(rawSSE))
	var frames []sse.Frame
	for rd.Scan() {
		frames = append(frames, rd.Frame())
	}
	require.NoError(tb, rd.Err())
	return frames
}
