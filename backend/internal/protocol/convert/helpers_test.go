package convert

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/qoderchat"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	protocolqoder "github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// helpers_test.go defines shared cross-protocol route tables, deterministic
// runtime and CallMemory test doubles, Qoder payload invariant validators, and
// fixture loaders for package convert. Conformance, multi-turn replay, and
// benchmark scenario suites reuse these helpers so all routes share consistent
// dialect configurations and repeatable identifiers.

// geminiPlaceholderSignature is the compatibility thoughtSignature marker used
// on Gemini routes when historical functionCall parts carry no cached signature.
const geminiPlaceholderSignature = "skip_thought_signature_validator"

var (
	// stage1Routes enumerates all six directed cross-protocol routes among
	// Anthropic Messages, OpenAI Responses, and OpenAI Chat Completions.
	stage1Routes = []routeSpec{
		{
			slug: "messages_to_responses",
			route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect: Dialect{
					Name:         "openai",
					ProviderID:   101,
					Tools:        ToolSupportAllResponses,
					ReplayScheme: ir.SchemeOpenAI,
				},
			},
			clientMdl:  "claude-sonnet-4-5",
			validation: testkit.RequestValidationOptions{},
		},
		{
			slug: "messages_to_chat",
			route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIChatCompletions,
				Dialect: Dialect{
					Name:             "deepseek",
					Tools:            ToolSupportFunction,
					ReasoningHistory: HistoryToolTurns,
				},
			},
			clientMdl: "claude-sonnet-4-5",
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"deepseek-thinking"},
			},
		},
		{
			slug: "responses_to_messages",
			route: Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: protocol.ProtocolAnthropicMessages,
				Dialect: Dialect{
					Name:         "anthropic",
					Tools:        ToolSupportFunction | ToolSupportHostedWebSearch,
					ReplayScheme: ir.SchemeAnthropic,
				},
			},
			clientMdl: "gpt-5",
			validation: testkit.RequestValidationOptions{
				StrictTopLevel: true,
			},
		},
		{
			slug: "responses_to_chat",
			route: Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: protocol.ProtocolOpenAIChatCompletions,
				Dialect: Dialect{
					Name:             "deepseek",
					Tools:            ToolSupportFunction,
					ReasoningHistory: HistoryToolTurns,
				},
			},
			clientMdl: "gpt-5",
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"deepseek-thinking"},
			},
		},
		{
			slug: "chat_to_responses",
			route: Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect: Dialect{
					Name:         "openai",
					ProviderID:   101,
					Tools:        ToolSupportAllResponses,
					ReplayScheme: ir.SchemeOpenAI,
				},
			},
			clientMdl:  "gpt-4o",
			validation: testkit.RequestValidationOptions{},
		},
		{
			slug: "chat_to_messages",
			route: Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolAnthropicMessages,
				Dialect: Dialect{
					Name:         "anthropic",
					Tools:        ToolSupportFunction | ToolSupportHostedWebSearch,
					ReplayScheme: ir.SchemeAnthropic,
				},
			},
			clientMdl: "gpt-4o",
			validation: testkit.RequestValidationOptions{
				StrictTopLevel: true,
			},
		},
	}

	// stage3GeminiRoutes enumerates the three Stage 3 cross-protocol routes from
	// Anthropic Messages, OpenAI Responses, and OpenAI Chat Completions to Google
	// Gemini GenerateContent.
	stage3GeminiRoutes = []routeSpec{
		{
			slug: "messages_to_gemini",
			route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect: Dialect{
					Name:             "gemini",
					Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
					ReplayScheme:     ir.SchemeGemini,
					SchemaDialect:    SchemaGemini,
					PlaceholderToken: geminiPlaceholderSignature,
				},
			},
			clientMdl: "claude-sonnet-4-5",
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"gemini-signature"},
			},
		},
		{
			slug: "responses_to_gemini",
			route: Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect: Dialect{
					Name:             "gemini",
					Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
					ReplayScheme:     ir.SchemeGemini,
					SchemaDialect:    SchemaGemini,
					PlaceholderToken: geminiPlaceholderSignature,
				},
			},
			clientMdl: "gpt-5",
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"gemini-signature"},
			},
		},
		{
			slug: "chat_to_gemini",
			route: Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect: Dialect{
					Name:             "gemini",
					Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
					ReplayScheme:     ir.SchemeGemini,
					SchemaDialect:    SchemaGemini,
					PlaceholderToken: geminiPlaceholderSignature,
				},
			},
			clientMdl: "gpt-4o",
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"gemini-signature"},
			},
		},
	}

	// stage6QoderRoutes enumerates the three Stage 6 cross-protocol routes from
	// Anthropic Messages, OpenAI Responses, and OpenAI Chat Completions to Qoder
	// Cosy (qoder_chat).
	stage6QoderRoutes = []routeSpec{
		{
			slug: "messages_to_qoder",
			route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolQoderChat,
				Dialect: Dialect{
					Name:               "qoder",
					Tools:              ToolSupportFunction,
					AllowBillingHeader: true,
				},
			},
			clientMdl:  "claude-sonnet-4-5",
			validation: testkit.RequestValidationOptions{},
		},
		{
			slug: "responses_to_qoder",
			route: Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: protocol.ProtocolQoderChat,
				Dialect: Dialect{
					Name:               "qoder",
					Tools:              ToolSupportFunction,
					AllowBillingHeader: true,
					StatelessResponses: true,
				},
			},
			clientMdl:  "gpt-5",
			validation: testkit.RequestValidationOptions{},
		},
		{
			slug: "chat_to_qoder",
			route: Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolQoderChat,
				Dialect: Dialect{
					Name:               "qoder",
					Tools:              ToolSupportFunction,
					AllowBillingHeader: true,
				},
			},
			clientMdl:  "gpt-4o",
			validation: testkit.RequestValidationOptions{},
		},
	}

	// activeConversionRoutes combines all Stage 1 text routes, Stage 3 Gemini
	// routes, and Stage 6 Qoder routes for T3 and T4 conformance and multi-turn
	// replay matrices.
	activeConversionRoutes = append(
		append(append([]routeSpec(nil), stage1Routes...), stage3GeminiRoutes...),
		stage6QoderRoutes...,
	)
)

// routeSpec pairs a stable route slug with its default Route, echoed client
// model, and strict validation options for Stage 1, Stage 3, and Stage 6
// conversion conformance, replay, and benchmark suites.
type routeSpec struct {
	slug       string                           // stable route identifier such as "messages_to_responses"
	route      Route                            // client protocol, upstream protocol, and default Dialect
	clientMdl  string                           // client-visible model echoed in responses
	validation testkit.RequestValidationOptions // strict upstream request validator options
}

// memCallMemory is a thread-safe in-memory CallMemory implementation for tests.
type memCallMemory struct {
	mu      sync.Mutex             // serializes concurrent Remember and Recall calls
	entries map[string]CallBinding // client call ID -> cloned CallBinding
}

// newMemCallMemory constructs an empty thread-safe in-memory memCallMemory store for tests.
func newMemCallMemory() *memCallMemory {
	return &memCallMemory{
		entries: make(map[string]CallBinding),
	}
}

// Remember stores an independent copy of binding under clientCallID.
func (m *memCallMemory) Remember(clientCallID string, binding CallBinding) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := binding
	if binding.Token != nil {
		tok := *binding.Token
		cp.Token = &tok
	}
	m.entries[clientCallID] = cp
}

// Recall retrieves a detached copy of the CallBinding recorded for clientCallID.
func (m *memCallMemory) Recall(clientCallID string) (CallBinding, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.entries[clientCallID]
	if !ok {
		return CallBinding{}, false
	}
	if b.Token != nil {
		tok := *b.Token
		b.Token = &tok
	}
	return b, true
}

// newDeterministicRuntime returns a repeatable Runtime fixed at 2025-10-09 UTC
// with sequentially numbered identifiers for golden and parity assertions.
func newDeterministicRuntime() Runtime {
	seq := 0
	return Runtime{
		Now: func() time.Time {
			return time.Unix(1760000000, 0).UTC()
		},
		NewID: func(prefix string) string {
			seq++
			if prefix == "" {
				return fmt.Sprintf("id_%d", seq)
			}
			if prefix[len(prefix)-1] == '_' || prefix[len(prefix)-1] == '-' {
				return fmt.Sprintf("%s%d", prefix, seq)
			}
			return fmt.Sprintf("%s_%d", prefix, seq)
		},
	}
}

// allProtocolRequestFixtures returns all wire and recorded client corpus request
// fixtures for proto in deterministic order.
func allProtocolRequestFixtures(t *testing.T, proto protocol.ProtocolID) []testkit.Fixture {
	t.Helper()
	wireReqs, err := testkit.WireFixtures(proto, testkit.FixtureRequest)
	require.NoError(t, err)
	require.NotEmpty(t, wireReqs)

	corpusReqs, err := testkit.CorpusRequests(proto)
	require.NoError(t, err)
	require.NotEmpty(t, corpusReqs)

	combined := make([]testkit.Fixture, 0, len(wireReqs)+len(corpusReqs))
	combined = append(combined, wireReqs...)
	combined = append(combined, corpusReqs...)
	return combined
}

// minimalClientRequest returns a minimal valid request JSON body for proto so
// response/stream laws can construct an Exchange even when a stream scenario has
// no matching client request fixture on proto.
func minimalClientRequest(proto protocol.ProtocolID, includeUsage bool) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"prompt"}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"gpt-5","stream":true,"input":"prompt"}`)
	default:
		if includeUsage {
			return []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"prompt"}]}`)
		}
		return []byte(`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"prompt"}]}`)
	}
}

// formatSSEFrames serializes frames into wire SSE bytes through sse.Append.
func formatSSEFrames(frames []sse.Frame) []byte {
	var out []byte
	for _, f := range frames {
		out = sse.Append(out, f)
	}
	return out
}

// decodeRawStreamToResponse parses rawSSE with sse.NewReader, decodes all frames
// through proto's registered StreamDecoder, and folds them into an ir.Response.
func decodeRawStreamToResponse(t *testing.T, proto protocol.ProtocolID, rawSSE []byte) ([]ir.Event, *ir.Response) {
	t.Helper()
	rd := sse.NewReader(bytes.NewReader(rawSSE))
	dec, err := NewStreamDecoder(proto)
	require.NoError(t, err)

	var events []ir.Event
	for rd.Scan() {
		batch, decErr := dec.Decode(rd.Frame())
		require.NoError(t, decErr)
		events = append(events, batch...)
	}
	require.NoError(t, rd.Err())
	events = append(events, dec.Close(nil)...)
	require.NoError(t, ir.ValidateStream(events))

	resp, err := ir.Accumulate(events)
	require.NoError(t, err)
	return events, resp
}

// validateUpstreamRequestInvariants runs strict wire request validation for
// public downstream protocols through testkit.ValidateRequestRules. On
// ProtocolQoderChat routes, it verifies PayloadRequest decoding, allowed-tool
// consistency, identifier bounds, JSON schema/argument syntax, stateless tool
// pairing when statelessResponses is true, and IR round-trip validity.
func validateUpstreamRequestInvariants(
	proto protocol.ProtocolID,
	upBytes []byte,
	opts testkit.RequestValidationOptions,
	statelessResponses bool,
) []testkit.RuleViolation {
	if proto != protocol.ProtocolQoderChat {
		return testkit.ValidateRequestRules(proto, upBytes, opts)
	}
	payload, err := protocolqoder.DecodePayload(upBytes)
	if err != nil {
		return []testkit.RuleViolation{{RuleID: "REQ-QODER-DECODE", Path: "$", Message: err.Error()}}
	}
	var violations []testkit.RuleViolation
	if payload.Model == "" {
		violations = append(violations, testkit.RuleViolation{
			RuleID:  "REQ-QODER-MODEL",
			Path:    "$.model",
			Message: "model must be non-empty",
		})
	}
	if payload.MaxTokens <= 0 {
		violations = append(violations, testkit.RuleViolation{
			RuleID:  "REQ-QODER-MAX-TOKENS",
			Path:    "$.max_tokens",
			Message: "max_tokens must be positive",
		})
	}
	if payload.ToolChoice != nil {
		switch payload.ToolChoice.Mode {
		case "", "auto", "none", "required", "function":
		default:
			violations = append(violations, testkit.RuleViolation{
				RuleID:  "REQ-QODER-TOOL-CHOICE",
				Path:    "$.tool_choice.mode",
				Message: fmt.Sprintf("unsupported tool_choice mode %q", payload.ToolChoice.Mode),
			})
		}
	}
	if _, restrictErr := payload.RestrictAllowedTools(); restrictErr != nil {
		violations = append(violations, testkit.RuleViolation{
			RuleID:  "REQ-QODER-ALLOWED-TOOLS",
			Path:    "$.tool_choice.allowed_tools",
			Message: restrictErr.Error(),
		})
	}
	seenTools := make(map[string]bool, len(payload.Tools))
	for i, tool := range payload.Tools {
		fnName := tool.Function.Name
		if fnName == "" || len(fnName) > defaultOpenAIToolNameMaxBytes || !isValidQoderIdentifier(fnName) {
			violations = append(violations, testkit.RuleViolation{
				RuleID:  "REQ-QODER-TOOL-NAME",
				Path:    fmt.Sprintf("$.tools[%d].function.name", i),
				Message: fmt.Sprintf("invalid Qoder function identifier %q", fnName),
			})
		}
		if seenTools[fnName] {
			violations = append(violations, testkit.RuleViolation{
				RuleID:  "REQ-QODER-TOOL-DUP",
				Path:    fmt.Sprintf("$.tools[%d].function.name", i),
				Message: fmt.Sprintf("duplicate Qoder function identifier %q", fnName),
			})
		}
		seenTools[fnName] = true
		var schema map[string]jsontext.Value
		if err := jsonv2.Unmarshal(tool.Function.Parameters, &schema, jsontext.AllowInvalidUTF8(true)); err != nil {
			violations = append(violations, testkit.RuleViolation{
				RuleID:  "REQ-QODER-TOOL-SCHEMA",
				Path:    fmt.Sprintf("$.tools[%d].function.parameters", i),
				Message: err.Error(),
			})
		}
	}
	seenCallIDs := make(map[string]bool)
	for i, msg := range payload.Messages {
		switch msg.Role {
		case "user":
		case "assistant":
			for j, tc := range msg.ToolCalls {
				if tc.ID == "" {
					violations = append(violations, testkit.RuleViolation{
						RuleID:  "REQ-QODER-CALL-ID",
						Path:    fmt.Sprintf("$.messages[%d].tool_calls[%d].id", i, j),
						Message: "assistant tool_call id must be non-empty",
					})
				} else {
					seenCallIDs[tc.ID] = true
				}
				var parsedArgs any
				if err := jsonv2.Unmarshal([]byte(tc.Function.Arguments), &parsedArgs, jsontext.AllowInvalidUTF8(true)); err != nil {
					violations = append(violations, testkit.RuleViolation{
						RuleID:  "REQ-QODER-CALL-ARGS",
						Path:    fmt.Sprintf("$.messages[%d].tool_calls[%d].function.arguments", i, j),
						Message: err.Error(),
					})
				}
			}
		case "tool":
			if msg.ToolCallID == "" {
				violations = append(violations, testkit.RuleViolation{
					RuleID:  "REQ-QODER-RESULT-ID",
					Path:    fmt.Sprintf("$.messages[%d].tool_call_id", i),
					Message: "tool message tool_call_id must be non-empty",
				})
			} else if statelessResponses && !seenCallIDs[msg.ToolCallID] {
				violations = append(violations, testkit.RuleViolation{
					RuleID:  "REQ-QODER-UNPAIRED-RESULT",
					Path:    fmt.Sprintf("$.messages[%d].tool_call_id", i),
					Message: fmt.Sprintf("unpaired tool_call_id %q in stateless Responses mode", msg.ToolCallID),
				})
			}
		default:
			violations = append(violations, testkit.RuleViolation{
				RuleID:  "REQ-QODER-ROLE",
				Path:    fmt.Sprintf("$.messages[%d].role", i),
				Message: fmt.Sprintf("unsupported Qoder message role %q", msg.Role),
			})
		}
	}
	if _, err := qoderchat.DecodePayloadRequest(upBytes); err != nil {
		violations = append(violations, testkit.RuleViolation{
			RuleID:  "REQ-QODER-IR-ROUNDTRIP",
			Path:    "$",
			Message: err.Error(),
		})
	}
	return violations
}

// isValidQoderIdentifier reports whether name consists solely of ASCII letters,
// digits, underscores, or hyphens.
func isValidQoderIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		ch := name[i]
		if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '_' && ch != '-' {
			return false
		}
	}
	return true
}

// upstreamGoldenStreamFixtures returns deterministic wire stream fixtures used
// when recording route loss goldens for proto. Public downstream protocols load
// testkit wire streams, whereas ProtocolQoderChat supplies its deterministic
// wrapped SSE suite.
func upstreamGoldenStreamFixtures(t *testing.T, proto protocol.ProtocolID) []testkit.Fixture {
	t.Helper()
	if proto == protocol.ProtocolQoderChat {
		streams, _ := qoderUpstreamFixtures()
		return streams
	}
	wireStreams, err := testkit.WireFixtures(proto, testkit.FixtureStream)
	require.NoError(t, err)
	require.NotEmpty(t, wireStreams)
	return wireStreams
}

// wrapQoderSSEInners formats inner Qoder JSON strings into wrapped SSE data
// frames. When appendDone is true, it appends a terminal wrapped [DONE] frame
// so StreamDecoder observes explicit stream completion.
func wrapQoderSSEInners(inners []string, appendDone bool) []byte {
	var buf bytes.Buffer
	for _, inner := range inners {
		fmt.Fprintf(&buf, "data: {\"body\":%q}\n\n", inner)
	}
	if appendDone {
		_, _ = buf.WriteString("data: {\"body\":\"[DONE]\"}\n\n")
	}
	return buf.Bytes()
}

// wrapQoderBufferedJSON wraps a single inner Qoder response JSON string inside
// Qoder's HTTP 200 outer envelope for buffered Response law assertions.
func wrapQoderBufferedJSON(inner string) []byte {
	return fmt.Appendf(nil, `{"statusCodeValue":200,"body":%q}`, inner)
}

// qoderUpstreamFixtures builds deterministic wrapped Qoder SSE stream fixtures
// and paired buffered JSON response fixtures spanning plain text, UTF-8 rune
// splits, reasoning summaries, structured and text-encoded XML/JSON/DSML tool
// calls, parallel and parameterless calls, lowered custom/namespace/search
// tools, Anthropic-shaped envelopes, cache usage, wrapped APIError frames, and
// truncated streams. Normal completion scenarios populate respByScenario for
// Law S6 checks, whereas abnormal error, truncation, and stream-only envelope
// scenarios omit buffered response entries.
func qoderUpstreamFixtures() ([]testkit.Fixture, map[string]testkit.Fixture) {
	// qoderSpec pairs a Qoder scenario name with its wrapped SSE and buffered JSON payloads.
	type qoderSpec struct {
		scenario    string // scenario fixture identifier
		sseBytes    []byte // wrapped Qoder SSE stream payload
		wrappedResp []byte // optional paired HTTP 200 buffered JSON envelope
	}
	specs := []qoderSpec{
		{
			scenario: "text_basic",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","content":"Hello, "}}]}`,
				`{"choices":[{"delta":{"content":"world!"}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
				`{"usage":{"prompt_tokens":16,"completion_tokens":4}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","content":"Hello, world!"},"finish_reason":"stop"}],"usage":{"prompt_tokens":16,"completion_tokens":4}}`),
		},
		{
			scenario: "text_unicode_split",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","content":"こんにちは"}}]}`,
				`{"choices":[{"delta":{"content":"世界 🚀 "}}]}`,
				`{"choices":[{"delta":{"content":"café"}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
				`{"usage":{"prompt_tokens":18,"completion_tokens":6}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","content":"こんにちは世界 🚀 café"},"finish_reason":"stop"}],"usage":{"prompt_tokens":18,"completion_tokens":6}}`),
		},
		{
			scenario: "reasoning_summary",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","reasoning_content":"Merge sort preserves equal-key order during left-before-right merging. "}}]}`,
				`{"choices":[{"delta":{"reasoning_content":"In-place quicksort partitions swap non-adjacent elements and can invert equal keys."}}]}`,
				`{"choices":[{"delta":{"content":"Merge sort is stable because equal elements keep their original order when sorted halves merge."}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
				`{"usage":{"prompt_tokens":42,"completion_tokens":50,"completion_tokens_details":{"reasoning_tokens":24}}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","reasoning_content":"Merge sort preserves equal-key order during left-before-right merging. In-place quicksort partitions swap non-adjacent elements and can invert equal keys.","content":"Merge sort is stable because equal elements keep their original order when sorted halves merge."},"finish_reason":"stop"}],"usage":{"prompt_tokens":42,"completion_tokens":50,"completion_tokens_details":{"reasoning_tokens":24}}}`),
		},
		{
			scenario: "reasoning_tool_replay",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","reasoning_content":"SKU-42 has 4 units against a reorder point of 20; "}}]}`,
				`{"choices":[{"delta":{"reasoning_content":"now query SKU-99 stock."}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_inv_sku99_2","type":"function","function":{"name":"query_inventory","arguments":"{\"sku\":"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_inv_sku99_2","function":{"arguments":"\"SKU-99\"}"}}]}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"usage":{"prompt_tokens":96,"completion_tokens":52,"completion_tokens_details":{"reasoning_tokens":28}}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","reasoning_content":"SKU-42 has 4 units against a reorder point of 20; now query SKU-99 stock.","tool_calls":[{"id":"call_inv_sku99_2","type":"function","function":{"name":"query_inventory","arguments":"{\"sku\":\"SKU-99\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":96,"completion_tokens":52,"completion_tokens_details":{"reasoning_tokens":28}}}`),
		},
		{
			scenario: "tool_single",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_weather_osa_2","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_weather_osa_2","function":{"arguments":"\"Osaka\"}"}}]}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"usage":{"prompt_tokens":64,"completion_tokens":18}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_weather_osa_2","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Osaka\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":64,"completion_tokens":18}}`),
		},
		{
			scenario: "tool_parallel",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_weather_tyo_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_weather_tyo_1","function":{"arguments":"\"Tokyo\"}"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_time_tyo_2","type":"function","function":{"name":"get_time","arguments":"{\"timezone\":"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_time_tyo_2","function":{"arguments":"\"Asia/Tokyo\"}"}}]}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"usage":{"prompt_tokens":96,"completion_tokens":32}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_weather_tyo_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Tokyo\"}"}},{"id":"call_time_tyo_2","type":"function","function":{"name":"get_time","arguments":"{\"timezone\":\"Asia/Tokyo\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":96,"completion_tokens":32}}`),
		},
		{
			scenario: "tool_args_empty",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_empty_str_1","type":"function","function":{"name":"get_clock","arguments":""}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_empty_obj_2","type":"function","function":{"name":"get_uptime","arguments":"{}"}}]}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"usage":{"prompt_tokens":44,"completion_tokens":20}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_empty_str_1","type":"function","function":{"name":"get_clock","arguments":"{}"}},{"id":"call_empty_obj_2","type":"function","function":{"name":"get_uptime","arguments":"{}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":44,"completion_tokens":20}}`),
		},
		{
			scenario: "tool_xml_text",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"content":"Checking forecast: <tool_cal"}}]}`,
				`{"choices":[{"delta":{"content":"l>get_weather<arg_key>city</arg_key><arg_value>Tokyo</arg_value></tool_call>"}}]}`,
				`{"usage":{"prompt_tokens":36,"completion_tokens":14}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"content":"Checking forecast: <tool_call>get_weather<arg_key>city</arg_key><arg_value>Tokyo</arg_value></tool_call>"}}],"usage":{"prompt_tokens":36,"completion_tokens":14}}`),
		},
		{
			scenario: "tool_json_text",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"content":"Invoking weather lookup <tool_call>{\"name\":\"get_weather\","}}]}`,
				`{"choices":[{"delta":{"content":"\"arguments\":{\"city\":\"Osaka\"}}</tool_call>"}}]}`,
				`{"usage":{"prompt_tokens":38,"completion_tokens":15}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"content":"Invoking weather lookup <tool_call>{\"name\":\"get_weather\",\"arguments\":{\"city\":\"Osaka\"}}</tool_call>"}}],"usage":{"prompt_tokens":38,"completion_tokens":15}}`),
		},
		{
			scenario: "tool_dsml_text",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"content":"Lookup via DSML: <｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"get_weather\">"}}]}`,
				`{"choices":[{"delta":{"content":"<｜｜DSML｜｜parameter name=\"city\">Kyoto</｜｜DSML｜｜parameter></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>"}}]}`,
				`{"usage":{"prompt_tokens":40,"completion_tokens":16}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"content":"Lookup via DSML: <｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"get_weather\"><｜｜DSML｜｜parameter name=\"city\">Kyoto</｜｜DSML｜｜parameter></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>"}}],"usage":{"prompt_tokens":40,"completion_tokens":16}}`),
		},
		{
			scenario: "tools_custom",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_patch_2","type":"function","function":{"name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** Update File: README.md\\n"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_patch_2","function":{"arguments":"@@\\n-Default timeout: 5s\\n+Default timeout: 15s\\n*** End Patch\\n\"}"}}]}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"usage":{"prompt_tokens":104,"completion_tokens":42}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_patch_2","type":"function","function":{"name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** Update File: README.md\\n@@\\n-Default timeout: 5s\\n+Default timeout: 15s\\n*** End Patch\\n\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":104,"completion_tokens":42}}`),
		},
		{
			scenario: "tools_namespace",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_ns_trace_2","type":"function","function":{"name":"enterprise_observability_telemetry_connector__query_di__5375327d","arguments":"{\"service\":\"checkout\",\"limit\":20}"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_ns_spawn_2","type":"function","function":{"name":"collaboration__spawn_agent","arguments":"{\"task\":\"summarize slow spans\"}"}}]}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"usage":{"prompt_tokens":132,"completion_tokens":38}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_ns_trace_2","type":"function","function":{"name":"enterprise_observability_telemetry_connector__query_di__5375327d","arguments":"{\"service\":\"checkout\",\"limit\":20}"}},{"id":"call_ns_spawn_2","type":"function","function":{"name":"collaboration__spawn_agent","arguments":"{\"task\":\"summarize slow spans\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":132,"completion_tokens":38}}`),
		},
		{
			scenario: "tools_search",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_search_2","type":"function","function":{"name":"tool_search","arguments":"{\"query\":\"slack notify\",\"limit\":3}"}}]}}]}`,
				`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_inc_2","type":"function","function":{"name":"pager__list_incidents","arguments":"{\"severity\":\"sev2\"}"}}]}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
				`{"usage":{"prompt_tokens":148,"completion_tokens":36}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","tool_calls":[{"id":"call_search_2","type":"function","function":{"name":"tool_search","arguments":"{\"query\":\"slack notify\",\"limit\":3}"}},{"id":"call_inc_2","type":"function","function":{"name":"pager__list_incidents","arguments":"{\"severity\":\"sev2\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":148,"completion_tokens":36}}`),
		},
		{
			scenario: "usage_cache",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","content":"Cached prefix reused."}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
				`{"usage":{"prompt_tokens":120,"completion_tokens":24,"prompt_tokens_details":{"cached_tokens":80,"cacheable_tokens":15},"completion_tokens_details":{"reasoning_tokens":6}}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","content":"Cached prefix reused."},"finish_reason":"stop"}],"usage":{"prompt_tokens":120,"completion_tokens":24,"prompt_tokens_details":{"cached_tokens":80,"cacheable_tokens":15},"completion_tokens_details":{"reasoning_tokens":6}}}`),
		},
		{
			scenario: "max_tokens",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant","content":"Truncated by max_tokens ceiling"}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"length"}]}`,
				`{"usage":{"prompt_tokens":22,"completion_tokens":16}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","content":"Truncated by max_tokens ceiling"},"finish_reason":"length"}],"usage":{"prompt_tokens":22,"completion_tokens":16}}`),
		},
		{
			scenario: "anthropic_envelope",
			sseBytes: wrapQoderSSEInners([]string{
				`{"event":"content_block_delta","data":{"type":"thinking_delta","text":"Checking Osaka weather."}}`,
				`{"event":"content_block_delta","data":{"type":"text_delta","text":"Calling weather API."}}`,
				`{"event":"tool_use_start","index":0,"data":{"id":"call_env_osa_1","name":"get_weather","type":"tool_use"}}`,
				`{"event":"tool_use_delta","index":0,"data":{"id":"call_env_osa_1","arguments":"{}"}}`,
				`{"event":"tool_use_delta","index":4,"data":{"id":"call_env_osa_1","arguments":"{\"city\":\"Osaka\"}"}}`,
				`{"usage":{"input_tokens":50,"output_tokens":4,"prompt_tokens_details":{"cached_tokens":20}}}`,
				`{"event":"message_delta","data":{"usage":{"output_tokens":11}}}`,
				`{"event":"message_stop"}`,
			}, false),
		},
		{
			scenario: "empty_response",
			sseBytes: wrapQoderSSEInners([]string{
				`{"choices":[{"delta":{"role":"assistant"}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
				`{"usage":{"prompt_tokens":12,"completion_tokens":0}}`,
			}, true),
			wrappedResp: wrapQoderBufferedJSON(`{"choices":[{"message":{"role":"assistant","content":""},"finish_reason":"stop"}],"usage":{"prompt_tokens":12,"completion_tokens":0}}`),
		},
		{
			scenario: "stream_error_event",
			sseBytes: []byte(
				"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"role\\\":\\\"assistant\\\",\\\"content\\\":\\\"Partial output before error\\\"}}]}\"}\n\n" +
					"data: {\"statusCode\":\"TOO_MANY_REQUESTS\",\"statusCodeValue\":429,\"body\":\"{\\\"error\\\":{\\\"code\\\":\\\"rate_limit_exceeded\\\",\\\"message\\\":\\\"Too many requests\\\"}}\"}\n\n",
			),
		},
		{
			scenario: "stream_truncated",
			sseBytes: []byte(
				"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"tool_calls\\\":[{\\\"index\\\":0,\\\"id\\\":\\\"call_trunc_1\\\",\\\"type\\\":\\\"function\\\",\\\"function\\\":{\\\"name\\\":\\\"get_weather\\\",\\\"arguments\\\":\\\"{\\\\\\\"city\\\\\\\":\\\"}}]}}]}\"}\n\n",
			),
		},
	}

	streams := make([]testkit.Fixture, 0, len(specs))
	respByScenario := make(map[string]testkit.Fixture, len(specs))
	for _, s := range specs {
		streams = append(streams, testkit.Fixture{
			Protocol: protocol.ProtocolQoderChat,
			Kind:     testkit.FixtureStream,
			Source:   testkit.SourceWire,
			Name:     s.scenario,
			Scenario: s.scenario,
			Path:     "wire/qoder_chat/stream/" + s.scenario + ".sse",
			Data:     s.sseBytes,
		})
		if len(s.wrappedResp) > 0 {
			respByScenario[s.scenario] = testkit.Fixture{
				Protocol: protocol.ProtocolQoderChat,
				Kind:     testkit.FixtureResponse,
				Source:   testkit.SourceWire,
				Name:     s.scenario,
				Scenario: s.scenario,
				Path:     "wire/qoder_chat/response/" + s.scenario + ".json",
				Data:     s.wrappedResp,
			}
		}
	}
	return streams, respByScenario
}
