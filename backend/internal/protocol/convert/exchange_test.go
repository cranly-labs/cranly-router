package convert

import (
	"encoding/json/v2"
	"errors"
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

// TestExchangeCursorChatInputStripEffortAndCallMemory exercises Cursor's
// input-shaped /v1/chat/completions ingress, pre-stripping RequestInfo.Effort
// capture, Dialect sampling/stop filters, CallMemory synthesis and recall, and
// Buffer error propagation on truncated streams. Converted requests and replies
// must pass strict validators while preserving pre-strip telemetry fields.
func TestExchangeCursorChatInputStripEffortAndCallMemory(t *testing.T) {
	t.Run("Cursor input-shaped Chat Completions body decodes via Responses and replies as Chat", func(t *testing.T) {
		cursorBody := []byte(`{
			"model":"claude-sonnet-4-5",
			"stream":true,
			"stream_options":{"include_usage":true},
			"reasoning":{"effort":"high"},
			"temperature":0.4,
			"top_p":0.9,
			"stop":["END"],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"Explain Go interfaces."}]}
			]
		}`)

		ex, err := NewExchange(cursorBody, Options{
			Route: Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolAnthropicMessages,
				Dialect: Dialect{
					Name:              "anthropic",
					ReplayScheme:      ir.SchemeAnthropic,
					StripEffort:       true,
					DropTemperature:   true,
					DropTopP:          true,
					OmitStopSequences: true,
				},
			},
			ClientModel: "cursor-sonnet",
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)

		// Info() preserves client Effort before StripEffort clears it on the upstream body.
		info := ex.Info()
		require.Equal(t, "claude-sonnet-4-5", info.Model)
		require.True(t, info.Stream)
		require.True(t, info.IncludeUsage)
		require.Equal(t, "high", info.Effort)

		upReq, err := ex.UpstreamRequest()
		require.NoError(t, err)
		upStr := string(upReq)
		require.NotContains(t, upStr, `"effort"`)
		require.NotContains(t, upStr, `"temperature"`)
		require.NotContains(t, upStr, `"top_p"`)
		require.NotContains(t, upStr, `"stop_sequences"`)
		require.Empty(t, testkit.ValidateMessagesRequest(upReq, testkit.RequestValidationOptions{StrictTopLevel: true}))

		// Verify consumed stream_options and stop caused no spurious top-level extra_dropped,
		// while OmitStopSequences=true recorded LossStopDropped against decoded.Stop.
		losses := ex.Losses()
		for _, l := range losses {
			require.NotEqual(t, ir.LossExtraDropped, l.Kind, "consumed Cursor Chat controls must not report extra_dropped: %v", losses)
		}
		require.Contains(t, losses, ir.Loss{Kind: ir.LossStopDropped, Path: "stop"})

		// With OmitStopSequences=false, Chat stop (array or scalar string) encodes into Messages stop_sequences.
		exKeepStop, err := NewExchange([]byte(`{
			"model":"claude-sonnet-4-5",
			"stream":true,
			"stream_options":{"include_usage":true},
			"reasoning_effort":"medium",
			"stop":"HALT",
			"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Say hi."}]}]
		}`), Options{
			Route: Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolAnthropicMessages,
				Dialect: Dialect{
					Name:         "anthropic",
					ReplayScheme: ir.SchemeAnthropic,
				},
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		require.Equal(t, "medium", exKeepStop.Info().Effort)
		require.True(t, exKeepStop.Info().IncludeUsage)
		upKeepStop, err := exKeepStop.UpstreamRequest()
		require.NoError(t, err)
		require.Contains(t, string(upKeepStop), `"stop_sequences":["HALT"]`)
		for _, l := range exKeepStop.Losses() {
			require.NotEqual(t, ir.LossExtraDropped, l.Kind)
		}

		// Targeting OpenAI Responses must not leak consumed stream_options or reasoning_effort onto /v1/responses.
		exToResponses, err := NewExchange([]byte(`{
			"model":"gpt-5",
			"stream":true,
			"stream_options":{"include_usage":true},
			"reasoning_effort":"high",
			"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Explain channels."}]}]
		}`), Options{
			Route: Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect: Dialect{
					Name:         "openai",
					ReplayScheme: ir.SchemeOpenAI,
				},
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		require.Equal(t, "high", exToResponses.Info().Effort)
		upRespBytes, err := exToResponses.UpstreamRequest()
		require.NoError(t, err)
		upRespStr := string(upRespBytes)
		require.NotContains(t, upRespStr, `"stream_options"`)
		require.NotContains(t, upRespStr, `"reasoning_effort"`)
		require.Contains(t, upRespStr, `"effort":"high"`)
		require.Empty(t, testkit.ValidateResponsesRequest(upRespBytes, testkit.RequestValidationOptions{}))
		for _, l := range exToResponses.Losses() {
			require.NotEqual(t, ir.LossExtraDropped, l.Kind)
		}

		// Stream Anthropic reply back to Cursor as valid Chat Completions SSE with usage chunk.
		stream := ex.Stream()
		anthSSE := []sse.Frame{
			{Event: "message_start", Data: []byte(`{"type":"message_start","message":{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-5","content":[],"stop_reason":null,"usage":{"input_tokens":18,"output_tokens":1}}}`)},
			{Event: "content_block_start", Data: []byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)},
			{Event: "content_block_delta", Data: []byte(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Interfaces define behavior."}}`)},
			{Event: "content_block_stop", Data: []byte(`{"type":"content_block_stop","index":0}`)},
			{Event: "message_delta", Data: []byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":6}}`)},
			{Event: "message_stop", Data: []byte(`{"type":"message_stop"}`)},
		}
		var outFrames []sse.Frame
		for _, f := range anthSSE {
			batch, wErr := stream.Write(f)
			require.NoError(t, wErr)
			outFrames = append(outFrames, batch...)
		}
		tail, cErr := stream.Close(nil)
		require.NoError(t, cErr)
		outFrames = append(outFrames, tail...)

		require.Empty(t, testkit.ValidateChatStream(outFrames, testkit.GrammarOptions{}))
		sum := stream.Summary()
		require.Equal(t, "msg_1", sum.ID)
		require.True(t, sum.HasContent)
		require.True(t, sum.HasText)
		require.False(t, sum.HasToolCall)
		require.NotNil(t, sum.Usage)
		require.Equal(t, int64(18), sum.Usage.Input)
		require.Equal(t, int64(6), sum.Usage.Output)
	})

	t.Run("CallMemory records synthesized IDs, call tokens, and unsigned flags across turns", func(t *testing.T) {
		mem := newMemCallMemory()
		ex1, err := NewExchange(minimalClientRequest(protocol.ProtocolAnthropicMessages, false), Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIChatCompletions,
				Dialect:  Dialect{ReplayScheme: ir.SchemeGemini},
			},
			Runtime: newDeterministicRuntime(),
			Calls:   mem,
		})
		require.NoError(t, err)

		// Synthesize a buffered response containing one signed nameless-ID call and one unsigned call.
		resp1 := &ir.Response{
			ID:    "resp_cm_1",
			Model: "gemini-2.5-pro",
			Parts: []ir.Part{
				{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{
						CallID:    "",
						Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "lookup_host"},
						Arguments: `{"host":"db"}`,
						Token:     &ir.ReplayToken{Data: "sig_gemini_step_1"},
					},
				},
				{
					Kind: ir.PartToolCall,
					Call: &ir.ToolCall{
						CallID:    "up_call_2",
						Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "ping_host"},
						Arguments: `{"host":"db"}`,
					},
				},
			},
			Finish: ir.Finish{Reason: ir.FinishToolCalls},
		}
		tagUpstreamResponseTokens(resp1, ex1.route.Dialect)
		var sealLosses []ir.Loss
		_, err = sealResponseCarriers(resp1, ex1.route, mem, ex1.runtime, &sealLosses)
		require.NoError(t, err)
		synthID := resp1.Parts[0].Call.CallID
		require.True(t, strings.HasPrefix(synthID, "toolu_"))

		b1, ok := mem.Recall(synthID)
		require.True(t, ok)
		require.Empty(t, b1.UpstreamID)
		require.NotNil(t, b1.Token)
		require.Equal(t, ir.SchemeGemini, b1.Token.Scheme)
		require.Equal(t, "sig_gemini_step_1", b1.Token.Data)

		b2, ok := mem.Recall("up_call_2")
		require.True(t, ok)
		require.Equal(t, "up_call_2", b2.UpstreamID)
		require.Nil(t, b2.Token)

		// Recall on Turn 2 populates Call.Token on the first call and Call.Unsigned on the second.
		turn2Req := &ir.Request{
			Model: "gemini-2.5-pro",
			Messages: []ir.Message{
				{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "check"}}},
				{
					Role: ir.RoleAssistant,
					Parts: []ir.Part{
						{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: synthID, Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "lookup_host"}, Arguments: `{"host":"db"}`}},
						{Kind: ir.PartToolCall, Call: &ir.ToolCall{CallID: "up_call_2", Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: "ping_host"}, Arguments: `{"host":"db"}`}},
					},
				},
				{
					Role: ir.RoleTool,
					Parts: []ir.Part{
						{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: synthID, Content: []ir.Part{{Kind: ir.PartText, Text: "ok"}}}},
						{Kind: ir.PartToolResult, Result: &ir.ToolResult{CallID: "up_call_2", Content: []ir.Part{{Kind: ir.PartText, Text: "ok"}}}},
					},
				},
			},
		}
		coords := newSourceCoords(turn2Req)
		_, recalled := openCarriers(turn2Req, coords, Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect:  Dialect{ReplayScheme: ir.SchemeGemini},
		}, mem)
		require.NotNil(t, turn2Req.Messages[1].Parts[0].Call.Token)
		require.True(t, turn2Req.Messages[1].Parts[1].Call.Unsigned)

		// Selecting for a matching Gemini dialect keeps bare signature; selecting for Chat drops without phantom loss.
		chatLosses := selectReplayTokens(turn2Req, coords, Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolOpenAIChatCompletions,
		}, recalled)
		require.Empty(t, chatLosses)
		require.Nil(t, turn2Req.Messages[1].Parts[0].Call.Token)
	})

	t.Run("Buffer folds complete SSE stream and rejects truncated stream with error", func(t *testing.T) {
		ex, err := NewExchange(minimalClientRequest(protocol.ProtocolOpenAIResponses, false), Options{
			Route: Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: protocol.ProtocolOpenAIChatCompletions,
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		require.True(t, NeedsConversion(ex.route))

		buf := ex.Buffer()
		require.NoError(t, buf.Write(sse.Frame{Data: []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":"Buffered ok"}}],"usage":null}`)}))
		require.NoError(t, buf.Write(sse.Frame{Data: []byte(`{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)}))
		require.NoError(t, buf.Write(sse.Frame{Data: []byte("[DONE]")}))
		rawResp, err := buf.Close(nil)
		require.NoError(t, err)
		require.Contains(t, string(rawResp), `"Buffered ok"`)

		decodedResp, err := DecodeResponse(protocol.ProtocolOpenAIResponses, rawResp)
		require.NoError(t, err)
		require.True(t, ResponseHasSemanticContent(decodedResp))

		// Truncated stream without finish_reason fails Buffer.Close instead of returning partial 200 JSON (bd39eb589).
		truncBuf := ex.Buffer()
		require.NoError(t, truncBuf.Write(sse.Frame{Data: []byte(`{"id":"chatcmpl-2","object":"chat.completion.chunk","created":1760000000,"model":"gpt-4o","choices":[{"index":0,"delta":{"role":"assistant","content":"Cut mid-sentence"}}]}`)}))
		_, truncErr := truncBuf.Close(nil)
		require.Error(t, truncErr)

		// Explicit transport cause propagates directly.
		causeBuf := ex.Buffer()
		_, causeErr := causeBuf.Close(errors.New("socket reset"))
		require.EqualError(t, causeErr, "socket reset")
	})

	t.Run("Messages disabled thinking guards effort=none by dialect capabilities and ultra effort fails", func(t *testing.T) {
		disabledBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":64,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`)
		exUnknown, err := NewExchange(disabledBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect:  Dialect{OmitStoreAndInclude: true},
			},
		})
		require.NoError(t, err)
		upUnknownBytes, err := exUnknown.UpstreamRequest()
		require.NoError(t, err)
		require.NotContains(t, string(upUnknownBytes), `"reasoning"`)
		require.Contains(t, string(upUnknownBytes), `"max_output_tokens":128`)
		require.NotContains(t, string(upUnknownBytes), `"store"`)
		require.NotContains(t, string(upUnknownBytes), `"include"`)

		exNone, err := NewExchange(disabledBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect: Dialect{
					OmitStoreAndInclude: true,
					Reasoning: ReasoningCapabilities{
						ControlsKnown: true,
						Efforts:       []string{"none", "low", "medium", "high", "xhigh"},
					},
				},
			},
		})
		require.NoError(t, err)
		upNoneBytes, err := exNone.UpstreamRequest()
		require.NoError(t, err)
		require.Contains(t, string(upNoneBytes), `"effort":"none"`)
		require.NotContains(t, string(upNoneBytes), `"summary"`)
		require.Contains(t, string(upNoneBytes), `"max_output_tokens":128`)
		require.NotContains(t, string(upNoneBytes), `"store"`)
		require.NotContains(t, string(upNoneBytes), `"include"`)

		ultraBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"output_config":{"effort":"ultra"},"messages":[{"role":"user","content":"hi"}]}`)
		exUltra, err := NewExchange(ultraBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
			},
		})
		require.NoError(t, err)
		_, ultraErr := exUltra.UpstreamRequest()
		require.Error(t, ultraErr)
	})
}

// TestExchangeCrossProtocolToolSanitizationAndOrphanDeferLoading reproduces
// oversized Messages function names, colliding sanitized tool declarations, and
// orphaned defer_loading flags on OpenAI Responses OAuth and Chat upstreams. It
// verifies that lowerTools populates ToolMap for 65-128 byte function round-trip
// restoration, rejects sanitization collisions, and strips orphaned defer_loading.
func TestExchangeCrossProtocolToolSanitizationAndOrphanDeferLoading(t *testing.T) {
	t.Run("oversized Messages function name round-trips through Responses OAuth ToolSupportAllResponses", func(t *testing.T) {
		longName := "enterprise_observability_cluster_diagnostics_fetch_regional_latency_histogram_v2"
		require.Equal(t, 80, len(longName))
		expectedUp, changed := sanitizeExecutableToolName(longName, defaultOpenAIToolNameMaxBytes)
		require.True(t, changed)
		require.Equal(t, 64, len(expectedUp))

		reqBody := []byte(`{
			"model":"claude-sonnet-4-5",
			"max_tokens":512,
			"tools":[{"name":"` + longName + `","description":"Fetch latency.","input_schema":{"type":"object","properties":{"region":{"type":"string"}}}}],
			"tool_choice":{"type":"tool","name":"` + longName + `"},
			"messages":[
				{"role":"user","content":"Check us-east-1."},
				{"role":"assistant","content":[{"type":"tool_use","id":"toolu_01","name":"` + longName + `","input":{"region":"us-east-1"}}]},
				{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_01","content":"p99=12ms"}]}
			]
		}`)

		ex, err := NewExchange(reqBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect: Dialect{
					Tools:            ToolSupportAllResponses,
					ToolNameMaxBytes: defaultOpenAIToolNameMaxBytes,
				},
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)

		upBytes, err := ex.UpstreamRequest()
		require.NoError(t, err)
		require.Contains(t, string(upBytes), `"name":"`+expectedUp+`"`)
		require.NotContains(t, string(upBytes), longName)
		require.NotNil(t, ex.ToolMap(), "oversized function lowering on ToolSupportAllResponses must populate Exchange.ToolMap")

		origIdent, ok := ex.ToolMap().LookupNamespaceTool(expectedUp)
		require.True(t, ok)
		require.Equal(t, NamespacedName{Name: longName}, origIdent)

		// Verify streaming restoration back to the exact 80-byte Messages tool name.
		st := ex.Stream()
		var outFrames []sse.Frame
		upstreamFrames := []sse.Frame{
			{Event: "response.created", Data: []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_long_1","object":"response","created_at":1760000000,"model":"gpt-5","status":"in_progress","output":[]}}`)},
			{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"fc_01","type":"function_call","call_id":"call_long_1","name":"` + expectedUp + `","arguments":"","status":"in_progress"}}`)},
			{Event: "response.function_call_arguments.delta", Data: []byte(`{"type":"response.function_call_arguments.delta","sequence_number":2,"output_index":0,"item_id":"fc_01","delta":"{\"region\":\"eu-west-1\"}"}`)},
			{Event: "response.function_call_arguments.done", Data: []byte(`{"type":"response.function_call_arguments.done","sequence_number":3,"output_index":0,"item_id":"fc_01","arguments":"{\"region\":\"eu-west-1\"}"}`)},
			{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":4,"output_index":0,"item":{"id":"fc_01","type":"function_call","call_id":"call_long_1","name":"` + expectedUp + `","arguments":"{\"region\":\"eu-west-1\"}","status":"completed"}}`)},
			{Event: "response.completed", Data: []byte(`{"type":"response.completed","sequence_number":5,"response":{"id":"resp_long_1","object":"response","created_at":1760000000,"model":"gpt-5","status":"completed","output":[{"id":"fc_01","type":"function_call","call_id":"call_long_1","name":"` + expectedUp + `","arguments":"{\"region\":\"eu-west-1\"}","status":"completed"}],"usage":{"input_tokens":20,"input_tokens_details":{"cached_tokens":0},"output_tokens":10,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":30}}}`)},
		}
		for _, f := range upstreamFrames {
			batch, wErr := st.Write(f)
			require.NoError(t, wErr)
			outFrames = append(outFrames, batch...)
		}
		tail, cErr := st.Close(nil)
		require.NoError(t, cErr)
		outFrames = append(outFrames, tail...)

		renderedSSE := string(formatSSEFrames(outFrames))
		require.Contains(t, renderedSSE, `"name":"`+longName+`"`)
		require.NotContains(t, renderedSSE, expectedUp)

		// Verify buffered response restoration back to the 80-byte Messages tool name.
		bufResp, rErr := ex.Response([]byte(`{"id":"resp_long_1","object":"response","created_at":1760000000,"model":"gpt-5","status":"completed","output":[{"id":"fc_01","type":"function_call","call_id":"call_long_1","name":"` + expectedUp + `","arguments":"{\"region\":\"eu-west-1\"}","status":"completed"}],"usage":{"input_tokens":20,"input_tokens_details":{"cached_tokens":0},"output_tokens":10,"output_tokens_details":{"reasoning_tokens":0},"total_tokens":30}}`))
		require.NoError(t, rErr)
		require.Contains(t, string(bufResp), `"name":"`+longName+`"`)
		require.NotContains(t, string(bufResp), expectedUp)
	})

	t.Run("sanitized cross-protocol function name collision fails", func(t *testing.T) {
		req := &ir.Request{
			Model: "claude-sonnet-4-5",
			Tools: []ir.Tool{
				{Kind: ir.ToolFunction, Name: "repo.read", InputSchema: []byte(`{"type":"object"}`)},
				{Kind: ir.ToolFunction, Name: "repo_read", InputSchema: []byte(`{"type":"object"}`)},
			},
			Messages: []ir.Message{{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "read"}}}},
		}
		for _, route := range []Route{
			{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIChatCompletions,
				Dialect:  Dialect{Tools: ToolSupportFunction, ToolNameMaxBytes: defaultOpenAIToolNameMaxBytes},
			},
			{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect:  Dialect{Tools: ToolSupportAllResponses, ToolNameMaxBytes: defaultOpenAIToolNameMaxBytes},
			},
		} {
			work := req.Clone()
			_, _, err := lowerTools(work, newSourceCoords(work), route, nil)
			require.Error(t, err, "colliding sanitized tool names must fail on %s -> %s", route.Client, route.Upstream)
			require.Contains(t, err.Error(), "conflicts")
		}
	})

	t.Run("orphaned defer_loading without tool_search strips on messages_to_responses", func(t *testing.T) {
		body := []byte(`{
			"model":"claude-sonnet-4-5",
			"max_tokens":512,
			"tools":[
				{"name":"Bash","description":"Run shell.","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}}}},
				{"name":"DeferredToolPlaceholder","description":"Placeholder.","defer_loading":true,"input_schema":{"type":"object","properties":{}}}
			],
			"messages":[{"role":"user","content":"ping"}]
		}`)
		ex, err := NewExchange(body, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolOpenAIResponses,
				Dialect: Dialect{
					Tools:            ToolSupportAllResponses,
					ToolNameMaxBytes: defaultOpenAIToolNameMaxBytes,
				},
			},
		})
		require.NoError(t, err)
		upBytes, err := ex.UpstreamRequest()
		require.NoError(t, err)
		require.NotContains(t, string(upBytes), `"defer_loading"`)
		require.Nil(t, ex.ToolMap())
		require.Contains(t, ex.Losses(), ir.Loss{Kind: ir.LossToolLowered, Path: "tools[1]"})
	})

	t.Run("Gemini nameless functionCall synthesis and closed explicit-ID late signature revision", func(t *testing.T) {
		clientBody := minimalClientRequest(protocol.ProtocolAnthropicMessages, true)
		upstreamFrames := []sse.Frame{
			{Data: []byte(`{"responseId":"gem_late_1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gemini-paris","name":"get_weather","args":{"city":"Paris"}}},{"functionCall":{"name":"get_weather","args":{"city":"London"}}}]}}]}`)},
			{Data: []byte(`{"responseId":"gem_late_1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gemini-paris","name":"get_weather","args":{"city":"Paris"}},"thoughtSignature":"original-signature"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":8,"totalTokenCount":20}}`)},
		}

		// Verify streaming Exchange path refreshes CallMemory on late signature and leaves nameless UpstreamID empty.
		streamMem := newMemCallMemory()
		exStream, err := NewExchange(clientBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  Dialect{ReplayScheme: ir.SchemeGemini},
			},
			Runtime: newDeterministicRuntime(),
			Calls:   streamMem,
		})
		require.NoError(t, err)
		st := exStream.Stream()
		var clientFrames []sse.Frame
		for _, f := range upstreamFrames {
			batch, wErr := st.Write(f)
			require.NoError(t, wErr)
			clientFrames = append(clientFrames, batch...)
		}
		tail, err := st.Close(nil)
		require.NoError(t, err)
		clientFrames = append(clientFrames, tail...)

		renderedStream := string(formatSSEFrames(clientFrames))
		require.Equal(t, 1, strings.Count(renderedStream, `"id":"gemini-paris"`))
		require.Contains(t, renderedStream, `"id":"toolu_1"`)

		parisBinding, ok := streamMem.Recall("gemini-paris")
		require.True(t, ok)
		require.Equal(t, "gemini-paris", parisBinding.UpstreamID)
		require.NotNil(t, parisBinding.Token)
		require.Equal(t, ir.SchemeGemini, parisBinding.Token.Scheme)
		require.Equal(t, "original-signature", parisBinding.Token.Data)

		londonBinding, ok := streamMem.Recall("toolu_1")
		require.True(t, ok)
		require.Empty(t, londonBinding.UpstreamID, "nameless upstream Gemini functionCall must record empty UpstreamID")

		// Verify Buffer path also refreshes CallMemory when folding SSE into a buffered client response.
		bufMem := newMemCallMemory()
		exBuf, err := NewExchange(minimalClientRequest(protocol.ProtocolAnthropicMessages, false), Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect:  Dialect{ReplayScheme: ir.SchemeGemini},
			},
			Runtime: newDeterministicRuntime(),
			Calls:   bufMem,
		})
		require.NoError(t, err)
		buf := exBuf.Buffer()
		for _, f := range upstreamFrames {
			require.NoError(t, buf.Write(f))
		}
		bufJSON, err := buf.Close(nil)
		require.NoError(t, err)
		require.Equal(t, 1, strings.Count(string(bufJSON), `"id":"gemini-paris"`))
		require.Contains(t, string(bufJSON), `"id":"toolu_1"`)

		parisBufBinding, ok := bufMem.Recall("gemini-paris")
		require.True(t, ok)
		require.NotNil(t, parisBufBinding.Token)
		require.Equal(t, "original-signature", parisBufBinding.Token.Data)

		londonBufBinding, ok := bufMem.Recall("toolu_1")
		require.True(t, ok)
		require.Empty(t, londonBufBinding.UpstreamID)
	})
}

// TestExchange_AntigravitySchemaCallMemoryIDAndSchemeRules exercises CallMemory
// recall, wire functionCall/functionResponse ID retention, placeholder
// signatures, and scheme isolation under SchemaAntigravity. Both claude-* and
// gemini-* Antigravity dialects must preserve client call IDs on misses or
// nameless bindings while rejecting foreign or cleared replay schemes.
func TestExchange_AntigravitySchemaCallMemoryIDAndSchemeRules(t *testing.T) {
	// antigravityWireView decodes outgoing Gemini contents parts for call ID and signature assertions.
	type antigravityWireView struct {
		Contents []struct {
			Role  string `json:"role"` // outgoing Gemini turn role
			Parts []struct {
				ThoughtSignature string `json:"thoughtSignature"` // optional replay or placeholder token
				FunctionCall     *struct {
					ID   string         `json:"id"`   // wire functionCall identifier
					Name string         `json:"name"` // wire functionCall name
					Args map[string]any `json:"args"` // wire functionCall arguments
				} `json:"functionCall"` // assistant tool invocation payload
				FunctionResponse *struct {
					ID       string         `json:"id"`       // wire functionResponse identifier
					Name     string         `json:"name"`     // wire functionResponse name
					Response map[string]any `json:"response"` // wrapped tool output object
				} `json:"functionResponse"` // user tool result payload
			} `json:"parts"` // ordered Gemini turn parts
		} `json:"contents"` // outgoing Gemini conversation turns
	}

	decodeWire := func(t *testing.T, raw []byte) antigravityWireView {
		t.Helper()
		var doc antigravityWireView
		require.NoError(t, json.Unmarshal(raw, &doc))
		return doc
	}

	msgTwoCallBody := []byte(`{
		"model":"claude-sonnet-4-5",
		"max_tokens":256,
		"tools":[{"name":"lookup","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],
		"messages":[
			{"role":"user","content":"check cities"},
			{"role":"assistant","content":[
				{"type":"tool_use","id":"toolu_client_1","name":"lookup","input":{"city":"Tokyo"}},
				{"type":"tool_use","id":"toolu_client_2","name":"lookup","input":{"city":"Osaka"}}
			]},
			{"role":"user","content":[
				{"type":"tool_result","tool_use_id":"toolu_client_1","content":"Sunny"},
				{"type":"tool_result","tool_use_id":"toolu_client_2","content":"Rain"}
			]}
		]
	}`)

	t.Run("CallMemory miss preserves wire call IDs on SchemaAntigravity claude and gemini dialects", func(t *testing.T) {
		exClaude, err := NewExchange(msgTwoCallBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect: Dialect{
					Name:             "antigravity",
					ReplayScheme:     ir.SchemeAntigravityClaude,
					SchemaDialect:    SchemaAntigravity,
					PlaceholderToken: "",
				},
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		rawClaude, err := exClaude.UpstreamRequest()
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, rawClaude, testkit.RequestValidationOptions{
			DialectRules: []string{"antigravity-schema"},
		}))
		docClaude := decodeWire(t, rawClaude)
		require.Equal(t, "toolu_client_1", docClaude.Contents[1].Parts[0].FunctionCall.ID)
		require.Empty(t, docClaude.Contents[1].Parts[0].ThoughtSignature)
		require.Equal(t, "toolu_client_2", docClaude.Contents[1].Parts[1].FunctionCall.ID)
		require.Empty(t, docClaude.Contents[1].Parts[1].ThoughtSignature)
		require.Equal(t, "toolu_client_1", docClaude.Contents[2].Parts[0].FunctionResponse.ID)
		require.Equal(t, "Sunny", docClaude.Contents[2].Parts[0].FunctionResponse.Response["result"])
		require.Equal(t, "toolu_client_2", docClaude.Contents[2].Parts[1].FunctionResponse.ID)

		exGemini, err := NewExchange(msgTwoCallBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect: Dialect{
					Name:             "antigravity",
					ReplayScheme:     ir.SchemeGemini,
					SchemaDialect:    SchemaAntigravity,
					PlaceholderToken: "skip_thought_signature_validator",
				},
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		rawGemini, err := exGemini.UpstreamRequest()
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, rawGemini, testkit.RequestValidationOptions{
			DialectRules: []string{"antigravity-schema", "gemini-signature"},
		}))
		docGemini := decodeWire(t, rawGemini)
		require.Equal(t, "toolu_client_1", docGemini.Contents[1].Parts[0].FunctionCall.ID)
		require.Equal(t, "skip_thought_signature_validator", docGemini.Contents[1].Parts[0].ThoughtSignature)
		require.Equal(t, "toolu_client_2", docGemini.Contents[1].Parts[1].FunctionCall.ID)
		require.Equal(t, "skip_thought_signature_validator", docGemini.Contents[1].Parts[1].ThoughtSignature)
		require.Equal(t, "toolu_client_1", docGemini.Contents[2].Parts[0].FunctionResponse.ID)
		require.Equal(t, "toolu_client_2", docGemini.Contents[2].Parts[1].FunctionResponse.ID)
	})

	t.Run("CallMemory hit restores matching UpstreamID and preserves synthesized IDs when UpstreamID is empty", func(t *testing.T) {
		mem := newMemCallMemory()
		mem.Remember("toolu_client_1", CallBinding{
			UpstreamID: "up_ag_explicit_1",
			Token:      &ir.ReplayToken{Scheme: ir.SchemeGemini, Data: "sig_step_lead"},
		})
		mem.Remember("toolu_client_2", CallBinding{
			UpstreamID: "",
			Token:      nil,
		})

		exGemHit, err := NewExchange(msgTwoCallBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect: Dialect{
					Name:             "antigravity",
					ReplayScheme:     ir.SchemeGemini,
					SchemaDialect:    SchemaAntigravity,
					PlaceholderToken: "skip_thought_signature_validator",
				},
			},
			Runtime: newDeterministicRuntime(),
			Calls:   mem,
		})
		require.NoError(t, err)
		rawGemHit, err := exGemHit.UpstreamRequest()
		require.NoError(t, err)
		docGemHit := decodeWire(t, rawGemHit)
		require.Equal(t, "up_ag_explicit_1", docGemHit.Contents[1].Parts[0].FunctionCall.ID)
		require.Equal(t, "sig_step_lead", docGemHit.Contents[1].Parts[0].ThoughtSignature)
		// Known-unsigned parallel sibling keeps its client ID without receiving DummyThoughtSignature.
		require.Equal(t, "toolu_client_2", docGemHit.Contents[1].Parts[1].FunctionCall.ID)
		require.Empty(t, docGemHit.Contents[1].Parts[1].ThoughtSignature)
		require.Equal(t, "up_ag_explicit_1", docGemHit.Contents[2].Parts[0].FunctionResponse.ID)
		require.Equal(t, "toolu_client_2", docGemHit.Contents[2].Parts[1].FunctionResponse.ID)

		hasIDRewritten := false
		for _, loss := range exGemHit.Losses() {
			if loss.Kind == ir.LossIDRewritten && loss.Path == "messages[1].parts[0].call_id" {
				hasIDRewritten = true
			}
		}
		require.True(t, hasIDRewritten, "expected LossIDRewritten for rewritten UpstreamID")

		// Mismatched scheme (antigravity_claude reading gemini bindings) ignores foreign UpstreamID and Token cleanly.
		exForeign, err := NewExchange(msgTwoCallBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect: Dialect{
					Name:             "antigravity",
					ReplayScheme:     ir.SchemeAntigravityClaude,
					SchemaDialect:    SchemaAntigravity,
					PlaceholderToken: "",
				},
			},
			Runtime: newDeterministicRuntime(),
			Calls:   mem,
		})
		require.NoError(t, err)
		rawForeign, err := exForeign.UpstreamRequest()
		require.NoError(t, err)
		docForeign := decodeWire(t, rawForeign)
		require.Equal(t, "toolu_client_1", docForeign.Contents[1].Parts[0].FunctionCall.ID)
		require.Empty(t, docForeign.Contents[1].Parts[0].ThoughtSignature)
		require.Equal(t, "toolu_client_1", docForeign.Contents[2].Parts[0].FunctionResponse.ID)
		for _, loss := range exForeign.Losses() {
			require.NotEqual(t, ir.LossReplayTokenDropped, loss.Kind)
		}

		// Cleared ReplayScheme (compatibility retry or count_tokens) ignores CallMemory UpstreamID and Unsigned flags.
		exCleared, err := NewExchange(msgTwoCallBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolGeminiGenerateContent,
				Dialect: Dialect{
					Name:             "antigravity",
					ReplayScheme:     "",
					SchemaDialect:    SchemaAntigravity,
					PlaceholderToken: "skip_thought_signature_validator",
				},
			},
			Runtime: newDeterministicRuntime(),
			Calls:   mem,
		})
		require.NoError(t, err)
		rawCleared, err := exCleared.UpstreamRequest()
		require.NoError(t, err)
		docCleared := decodeWire(t, rawCleared)
		require.Equal(t, "toolu_client_1", docCleared.Contents[1].Parts[0].FunctionCall.ID)
		require.Equal(t, "skip_thought_signature_validator", docCleared.Contents[1].Parts[0].ThoughtSignature)
		require.Equal(t, "toolu_client_2", docCleared.Contents[1].Parts[1].FunctionCall.ID)
		require.Equal(t, "skip_thought_signature_validator", docCleared.Contents[1].Parts[1].ThoughtSignature)
	})
}

// TestExchange_ReencodeUpstreamPreservesOriginalExchangeState verifies that
// ReencodeUpstream unpacks carriers and lowers tools on a detached request copy
// for retry encoding. Cached UpstreamRequest bytes, lowered ToolMap entries,
// and recorded Losses on the original Exchange must remain untouched for
// subsequent response restoration and carrier sealing.
func TestExchange_ReencodeUpstreamPreservesOriginalExchangeState(t *testing.T) {
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAntigravityClaude,
		Data:   "sig_claude_orig",
		Text:   "unpacked reasoning text",
	})
	require.NoError(t, err)

	clientBody := []byte(`{
		"model":"gpt-5",
		"stream":false,
		"reasoning":{"effort":"high"},
		"tools":[{"type":"custom","name":"apply_patch","description":"Apply patch"}],
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"Fix bug"}]},
			{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"` + carrier + `"},
			{"type":"custom_tool_call","call_id":"call_patch_1","name":"apply_patch","input":"*** Begin Patch"},
			{"type":"custom_tool_call_output","call_id":"call_patch_1","output":"ok"}
		]
	}`)

	ex, err := NewExchange(clientBody, Options{
		Route: Route{
			Client:   protocol.ProtocolOpenAIResponses,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: Dialect{
				Name:          "antigravity",
				ReplayScheme:  ir.SchemeAntigravityClaude,
				SchemaDialect: SchemaAntigravity,
				Reasoning: ReasoningCapabilities{
					ForceBudget: true,
				},
			},
		},
		ClientModel: "gpt-5",
		Runtime:     newDeterministicRuntime(),
	})
	require.NoError(t, err)

	origBytes, err := ex.UpstreamRequest()
	require.NoError(t, err)
	origLosses := ex.Losses()
	origToolMap := ex.ToolMap()
	require.NotNil(t, origToolMap)
	require.Contains(t, string(origBytes), `"thought":true`)
	require.Contains(t, string(origBytes), `"thoughtSignature":"sig_claude_orig"`)

	// Unchanged callback short-circuits without encoding.
	noopBytes, changed, err := ex.ReencodeUpstream(func(_ *ir.Request, _ *Dialect) (bool, error) {
		return false, nil
	})
	require.NoError(t, err)
	require.False(t, changed)
	require.Nil(t, noopBytes)

	// Stripping reasoning on the detached copy unpacks trc1 text into PartText and
	// omits thinkingConfig without altering ex's cached UpstreamRequest or ToolMap.
	reencoded, changed, err := ex.ReencodeUpstream(func(req *ir.Request, _ *Dialect) (bool, error) {
		req.Reasoning = nil
		for m := range req.Messages {
			for p := range req.Messages[m].Parts {
				part := &req.Messages[m].Parts[p]
				if part.Kind == ir.PartReasoning && part.Reasoning != nil {
					*part = ir.Part{Kind: ir.PartText, Text: part.Reasoning.Segments[0]}
				}
			}
		}
		return true, nil
	})
	require.NoError(t, err)
	require.True(t, changed)
	require.Contains(t, string(reencoded), `"text":"unpacked reasoning text"`)
	require.NotContains(t, string(reencoded), `"thought":true`)
	require.NotContains(t, string(reencoded), `"thinkingConfig"`)

	afterBytes, err := ex.UpstreamRequest()
	require.NoError(t, err)
	require.Equal(t, origBytes, afterBytes)
	require.Equal(t, origLosses, ex.Losses())

	// Verify response conversion on ex still restores lowered custom tool calls and
	// seals antigravity_claude carriers after ReencodeUpstream ran.
	geminiReply := []byte(`{
		"responseId":"resp_after_retry",
		"modelVersion":"claude-opus-4-6-thinking",
		"candidates":[{
			"index":0,
			"content":{"role":"model","parts":[
				{"text":"new claude thought","thought":true,"thoughtSignature":"sig_new_turn"},
				{"functionCall":{"id":"call_patch_2","name":"apply_patch","args":{"input":"*** End Patch"}}}
			]},
			"finishReason":"STOP"
		}],
		"usageMetadata":{"promptTokenCount":20,"candidatesTokenCount":5,"thoughtsTokenCount":3,"totalTokenCount":28}
	}`)
	clientJSON, err := ex.Response(geminiReply)
	require.NoError(t, err)
	require.Contains(t, string(clientJSON), `"type":"custom_tool_call"`)
	require.Contains(t, string(clientJSON), `"input":"*** End Patch"`)
	require.Contains(t, string(clientJSON), `trc1:antigravity_claude:`)
}

// TestExchangeInlineBase64ImageAndSignedNonThoughtTextParity exercises Gemini
// and Antigravity response, buffer, and stream conversions carrying generated
// inlineData images and non-thought text thoughtSignatures across Messages,
// Responses, and Chat clients. Every client route must render base64 images as
// visible Markdown data URLs, preserve replayable signatures on Messages and
// Responses, and satisfy Laws S3, S4, S6, and S7.
func TestExchangeInlineBase64ImageAndSignedNonThoughtTextParity(t *testing.T) {
	const pngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
	wantMarkdown := "![image](data:image/png;base64," + pngB64 + ")"

	yes := true
	dialects := []struct {
		name   string
		scheme string
		d      Dialect
	}{
		{
			name:   "antigravity_claude",
			scheme: ir.SchemeAntigravityClaude,
			d: Dialect{
				Name:             "antigravity",
				Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
				ToolNameMaxBytes: 64,
				ReplayScheme:     ir.SchemeAntigravityClaude,
				ReasoningHistory: HistoryNone,
				SchemaDialect:    SchemaAntigravity,
				Reasoning:        ReasoningCapabilities{ControlsKnown: true, Supported: &yes, Toggle: true, Budget: true, ForceBudget: true},
			},
		},
		{
			name:   "antigravity_gemini",
			scheme: ir.SchemeGemini,
			d: Dialect{
				Name:             "antigravity",
				Tools:            ToolSupportFunction | ToolSupportHostedWebSearch,
				ToolNameMaxBytes: 64,
				ReplayScheme:     ir.SchemeGemini,
				ReasoningHistory: HistoryNone,
				SchemaDialect:    SchemaAntigravity,
				PlaceholderToken: geminiPlaceholderSignature,
				Reasoning:        ReasoningCapabilities{ControlsKnown: true, Supported: &yes, Toggle: true, Budget: true},
			},
		},
	}

	rawSSE := []byte(
		"data: {\"responseId\":\"ag_img_sig_1\",\"modelVersion\":\"gemini-3-pro-image\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Rendered icon: \"}]}}]}\n\n" +
			"data: {\"responseId\":\"ag_img_sig_1\",\"modelVersion\":\"gemini-3-pro-image\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"\",\"thoughtSignature\":\"sig_plain_text_42\"}]}}]}\n\n" +
			"data: {\"responseId\":\"ag_img_sig_1\",\"modelVersion\":\"gemini-3-pro-image\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"inlineData\":{\"mimeType\":\"image/png\",\"data\":\"" + pngB64 + "\"}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":12,\"candidatesTokenCount\":19,\"totalTokenCount\":31}}\n\n",
	)
	bufferedUpstream := []byte(`{
		"responseId":"ag_img_sig_1",
		"modelVersion":"gemini-3-pro-image",
		"candidates":[{
			"index":0,
			"content":{"role":"model","parts":[
				{"text":"Rendered icon: ","thoughtSignature":"sig_plain_text_42"},
				{"inlineData":{"mimeType":"image/png","data":"` + pngB64 + `"}}
			]},
			"finishReason":"STOP"
		}],
		"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":19,"totalTokenCount":31}
	}`)

	clients := []struct {
		proto protocol.ProtocolID
		model string
	}{
		{proto: protocol.ProtocolAnthropicMessages, model: "claude-sonnet-4-5"},
		{proto: protocol.ProtocolOpenAIResponses, model: "gpt-5"},
		{proto: protocol.ProtocolOpenAIChatCompletions, model: "gpt-4o"},
	}

	for _, dl := range dialects {
		for _, cl := range clients {
			t.Run(dl.name+"_"+string(cl.proto), func(t *testing.T) {
				reqBytes := minimalClientRequest(cl.proto, true)
				route := Route{
					Client:   cl.proto,
					Upstream: protocol.ProtocolGeminiGenerateContent,
					Dialect:  dl.d,
				}
				fixStream := testkit.Fixture{
					Name:     "inline_image_and_signed_text",
					Scenario: "inline_image_and_signed_text",
					Protocol: protocol.ProtocolGeminiGenerateContent,
					Kind:     testkit.FixtureStream,
					Data:     rawSSE,
				}
				rawFrames, err := fixStream.Frames()
				require.NoError(t, err)

				exStream, err := NewExchange(reqBytes, Options{
					Route:       route,
					ClientModel: cl.model,
					Runtime:     newDeterministicRuntime(),
				})
				require.NoError(t, err)
				st := exStream.Stream()
				var outFrames []sse.Frame
				for _, f := range rawFrames {
					batch, wErr := st.Write(f)
					require.NoError(t, wErr)
					outFrames = append(outFrames, batch...)
				}
				tail, err := st.Close(nil)
				require.NoError(t, err)
				outFrames = append(outFrames, tail...)

				// Verify grammar (S3), semantic flags, and summary presence.
				require.Empty(t, testkit.ValidateStreamGrammar(cl.proto, outFrames, testkit.GrammarOptions{}))
				require.True(t, st.Summary().HasContent)
				require.True(t, st.Summary().HasText)
				rawClientSSE := string(formatSSEFrames(outFrames))
				require.Contains(t, rawClientSSE, wantMarkdown)
				if cl.proto != protocol.ProtocolOpenAIChatCompletions {
					require.Contains(t, rawClientSSE, "trc1:"+dl.scheme+":")
				}

				// Verify Law S4 Essence loss accounting.
				_, srcResp := decodeRawStreamToResponse(t, protocol.ProtocolGeminiGenerateContent, rawSSE)
				tagUpstreamResponseTokens(srcResp, exStream.route.Dialect)
				_, dstResp := decodeRawStreamToResponse(t, cl.proto, formatSSEFrames(outFrames))
				essDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(dstResp), st.Losses())
				require.NoError(t, err)
				require.Empty(t, essDiffs, testkit.FormatJSONDiff(essDiffs))

				// Verify Law S6 Stream, Buffer, and buffered Response parity.
				fixResp := testkit.Fixture{
					Name:     "inline_image_and_signed_text",
					Scenario: "inline_image_and_signed_text",
					Protocol: protocol.ProtocolGeminiGenerateContent,
					Kind:     testkit.FixtureResponse,
					Data:     bufferedUpstream,
				}
				spec := routeSpec{slug: dl.name + "_" + string(cl.proto), route: route, clientMdl: cl.model}
				verifyLawS6StreamBufferResponseParity(t, spec, reqBytes, fixStream, fixResp, outFrames, dstResp)
				verifyLawS7PrefixTruncations(t, spec, reqBytes, rawFrames)
			})
		}
	}
}

// TestExchangeEmptySignedNonThoughtTextSuppressionAndReplay exercises upstream
// Gemini responses and SSE streams that emit metadata-free empty PartText blocks
// carrying thoughtSignature values without preceding unsigned text, both after a
// functionCall and standalone. Cross-protocol conversion must omit residual
// empty text blocks, satisfy Laws S3, S4, and S6 across schemed and unschemed
// routes, emit no empty Responses message items, and replay signatures cleanly on Turn 2.
func TestExchangeEmptySignedNonThoughtTextSuppressionAndReplay(t *testing.T) {
	yes := true
	dialects := []struct {
		name   string
		scheme string
		d      Dialect
	}{
		{
			name:   "antigravity_claude",
			scheme: ir.SchemeAntigravityClaude,
			d: Dialect{
				Name:             "antigravity",
				Tools:            ToolSupportFunction,
				ToolNameMaxBytes: 64,
				ReplayScheme:     ir.SchemeAntigravityClaude,
				ReasoningHistory: HistoryNone,
				SchemaDialect:    SchemaAntigravity,
				Reasoning:        ReasoningCapabilities{ControlsKnown: true, Supported: &yes, Toggle: true, Budget: true, ForceBudget: true},
			},
		},
		{
			name:   "gemini",
			scheme: ir.SchemeGemini,
			d: Dialect{
				Name:             "gemini",
				Tools:            ToolSupportFunction,
				ToolNameMaxBytes: 64,
				ReplayScheme:     ir.SchemeGemini,
				ReasoningHistory: HistoryNone,
				SchemaDialect:    SchemaGemini,
				PlaceholderToken: geminiPlaceholderSignature,
				Reasoning:        ReasoningCapabilities{ControlsKnown: true, Supported: &yes, Toggle: true, Budget: true},
			},
		},
		{
			name:   "unschemed",
			scheme: "",
			d: Dialect{
				Name:             "unschemed_gemini_proxy",
				Tools:            ToolSupportFunction,
				ToolNameMaxBytes: 64,
				ReplayScheme:     "",
				ReasoningHistory: HistoryNone,
				SchemaDialect:    SchemaGemini,
			},
		},
	}

	shapes := []struct {
		name    string
		sig     string
		hasCall bool
		rawSSE  []byte
		rawJSON []byte
	}{
		{
			name:    "after_function_call",
			sig:     "sig_after_fc_99",
			hasCall: true,
			rawSSE: []byte(
				"data: {\"responseId\":\"gem_empty_sig_fc\",\"modelVersion\":\"gemini-3-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"call_fc_sig_1\",\"name\":\"get_weather\",\"args\":{\"city\":\"Tokyo\"}}}]}}]}\n\n" +
					"data: {\"responseId\":\"gem_empty_sig_fc\",\"modelVersion\":\"gemini-3-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"\",\"thoughtSignature\":\"sig_after_fc_99\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":8,\"totalTokenCount\":18}}\n\n",
			),
			rawJSON: []byte(`{
				"responseId":"gem_empty_sig_fc",
				"modelVersion":"gemini-3-pro",
				"candidates":[{
					"index":0,
					"content":{"role":"model","parts":[
						{"functionCall":{"id":"call_fc_sig_1","name":"get_weather","args":{"city":"Tokyo"}}},
						{"text":"","thoughtSignature":"sig_after_fc_99"}
					]},
					"finishReason":"STOP"
				}],
				"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":8,"totalTokenCount":18}
			}`),
		},
		{
			name:    "standalone",
			sig:     "sig_standalone_77",
			hasCall: false,
			rawSSE: []byte(
				"data: {\"responseId\":\"gem_empty_sig_only\",\"modelVersion\":\"gemini-3-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"\",\"thoughtSignature\":\"sig_standalone_77\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":9,\"candidatesTokenCount\":1,\"totalTokenCount\":10}}\n\n",
			),
			rawJSON: []byte(`{
				"responseId":"gem_empty_sig_only",
				"modelVersion":"gemini-3-pro",
				"candidates":[{
					"index":0,
					"content":{"role":"model","parts":[
						{"text":"","thoughtSignature":"sig_standalone_77"}
					]},
					"finishReason":"STOP"
				}],
				"usageMetadata":{"promptTokenCount":9,"candidatesTokenCount":1,"totalTokenCount":10}
			}`),
		},
	}

	clients := []struct {
		proto protocol.ProtocolID
		model string
	}{
		{proto: protocol.ProtocolAnthropicMessages, model: "claude-sonnet-4-5"},
		{proto: protocol.ProtocolOpenAIResponses, model: "gpt-5"},
		{proto: protocol.ProtocolOpenAIChatCompletions, model: "gpt-4o"},
	}

	for _, sh := range shapes {
		for _, dl := range dialects {
			for _, cl := range clients {
				t.Run(sh.name+"_"+dl.name+"_"+string(cl.proto), func(t *testing.T) {
					reqBytes := minimalClientRequestWithWeatherTool(cl.proto)
					route := Route{
						Client:   cl.proto,
						Upstream: protocol.ProtocolGeminiGenerateContent,
						Dialect:  dl.d,
					}
					mem := newMemCallMemory()
					exStream, err := NewExchange(reqBytes, Options{
						Route:       route,
						Calls:       mem,
						ClientModel: cl.model,
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)

					fixStream := testkit.Fixture{
						Name:     sh.name,
						Scenario: sh.name,
						Protocol: protocol.ProtocolGeminiGenerateContent,
						Kind:     testkit.FixtureStream,
						Data:     sh.rawSSE,
					}
					if !sh.hasCall && (dl.scheme == "" || cl.proto == protocol.ProtocolOpenAIChatCompletions) {
						fixStream.Scenario = "empty_response"
					}
					rawFrames, err := fixStream.Frames()
					require.NoError(t, err)

					st := exStream.Stream()
					var outFrames []sse.Frame
					for _, f := range rawFrames {
						batch, wErr := st.Write(f)
						require.NoError(t, wErr)
						outFrames = append(outFrames, batch...)
					}
					tail, err := st.Close(nil)
					require.NoError(t, err)
					outFrames = append(outFrames, tail...)

					require.Empty(t, testkit.ValidateStreamGrammar(cl.proto, outFrames, testkit.GrammarOptions{}))
					rawClientSSE := string(formatSSEFrames(outFrames))
					if cl.proto == protocol.ProtocolOpenAIResponses {
						require.NotContains(t, rawClientSSE, `"type":"message"`, "signature-only empty text must not emit an empty Responses message item")
					}

					_, srcResp := decodeRawStreamToResponse(t, protocol.ProtocolGeminiGenerateContent, sh.rawSSE)
					tagUpstreamResponseTokens(srcResp, exStream.route.Dialect)
					_, dstResp := decodeRawStreamToResponse(t, cl.proto, formatSSEFrames(outFrames))
					essDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(dstResp), st.Losses())
					require.NoError(t, err)
					require.Empty(t, essDiffs, testkit.FormatJSONDiff(essDiffs))

					fixResp := testkit.Fixture{
						Name:     sh.name,
						Scenario: fixStream.Scenario,
						Protocol: protocol.ProtocolGeminiGenerateContent,
						Kind:     testkit.FixtureResponse,
						Data:     sh.rawJSON,
					}
					spec := routeSpec{slug: dl.name + "_" + string(cl.proto), route: route, clientMdl: cl.model}
					verifyLawS6StreamBufferResponseParity(t, spec, reqBytes, fixStream, fixResp, outFrames, dstResp)

					// Verify buffered Response also omits empty Responses message items and passes S4.
					exBuf, err := NewExchange(reqBytes, Options{
						Route:       route,
						ClientModel: cl.model,
						Runtime:     newDeterministicRuntime(),
					})
					require.NoError(t, err)
					_, err = exBuf.UpstreamRequest()
					require.NoError(t, err)
					reqLossCount := len(exBuf.Losses())
					clientJSON, err := exBuf.Response(sh.rawJSON)
					require.NoError(t, err)
					if cl.proto == protocol.ProtocolOpenAIResponses {
						require.NotContains(t, string(clientJSON), `"type":"message"`)
					}
					decodedBufResp, err := DecodeResponse(cl.proto, clientJSON)
					require.NoError(t, err)
					bufDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(decodedBufResp), exBuf.Losses()[reqLossCount:])
					require.NoError(t, err)
					require.Empty(t, bufDiffs, testkit.FormatJSONDiff(bufDiffs))

					// Verify Law M1 Turn 2 signature replay without spurious empty {"text":""} parts.
					if dl.scheme != "" && cl.proto != protocol.ProtocolOpenAIChatCompletions {
						turn2ClientReq, err := testkit.SimulateNextTurn(cl.proto, testkit.TurnInput{
							PrevRequest:              reqBytes,
							ResponseFrames:           outFrames,
							SynthesizeMissingOutputs: true,
							FollowUpUserText:         "Continue.",
						})
						require.NoError(t, err)
						ex2, err := NewExchange(turn2ClientReq, Options{
							Route:       route,
							Calls:       mem,
							ClientModel: cl.model,
							Runtime:     newDeterministicRuntime(),
						})
						require.NoError(t, err)
						upReq2, err := ex2.UpstreamRequest()
						require.NoError(t, err)
						require.Contains(t, string(upReq2), `"thoughtSignature":"`+sh.sig+`"`)
						decodedUp2, err := DecodeRequest(protocol.ProtocolGeminiGenerateContent, upReq2)
						require.NoError(t, err)
						wantMsgCount := 3
						if sh.hasCall {
							wantMsgCount = 4
						}
						require.Len(t, decodedUp2.Messages, wantMsgCount)
						for _, mp := range decodedUp2.Messages[1].Parts {
							require.NotEqual(t, ir.PartText, mp.Kind, "Turn 2 assistant turn must not contain a residual empty PartText")
						}
					}
				})
			}
		}
	}
}

// minimalClientRequestWithWeatherTool constructs a minimal valid single-turn
// client request declaring get_weather so tool-call replies replay cleanly.
func minimalClientRequestWithWeatherTool(proto protocol.ProtocolID) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"tools":[{"name":"get_weather","description":"Weather lookup","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"messages":[{"role":"user","content":"Check Tokyo."}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"gpt-5","stream":true,"tools":[{"type":"function","name":"get_weather","description":"Weather lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Check Tokyo."}]}]}`)
	default:
		return []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"get_weather","description":"Weather lookup","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],"messages":[{"role":"user","content":"Check Tokyo."}]}`)
	}
}

// TestDirectForeignCodecsReportPartTextReplayTokenLoss exercises direct calls to
// EncodeRequest, EncodeResponse, and NewStreamEncoder across Chat, Messages, and
// Responses codecs when legal IR carries PartText.Token. Every non-Gemini codec
// must report LossReplayTokenDropped at the exact part coordinate and pass VerifyEssence.
func TestDirectForeignCodecsReportPartTextReplayTokenLoss(t *testing.T) {
	protos := []protocol.ProtocolID{
		protocol.ProtocolOpenAIChatCompletions,
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
	}
	tok := &ir.ReplayToken{
		Scheme: ir.SchemeGemini,
		Data:   "sig_direct_codec_text_token",
	}

	for _, proto := range protos {
		t.Run(string(proto)+"_request_nonempty_and_empty_signed_text", func(t *testing.T) {
			for _, tc := range []struct {
				name string
				text string
			}{
				{name: "nonempty", text: "Signed assistant reply."},
				{name: "empty_with_call", text: ""},
			} {
				t.Run(tc.name, func(t *testing.T) {
					asstParts := []ir.Part{{
						Kind:  ir.PartText,
						Text:  tc.text,
						Token: &ir.ReplayToken{Scheme: tok.Scheme, Data: tok.Data},
					}}
					msgs := []ir.Message{
						{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Hello"}}},
					}
					if tc.text == "" {
						asstParts = append(asstParts, ir.Part{
							Kind: ir.PartToolCall,
							Call: &ir.ToolCall{
								CallID:    "call_1",
								Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: "ping"},
								Arguments: `{}`,
							},
						})
						msgs = append(msgs,
							ir.Message{Role: ir.RoleAssistant, Parts: asstParts},
							ir.Message{Role: ir.RoleTool, Parts: []ir.Part{{
								Kind: ir.PartToolResult,
								Result: &ir.ToolResult{
									CallID:  "call_1",
									Tool:    ir.ToolRef{Kind: ir.ToolFunction, Name: "ping"},
									Content: []ir.Part{{Kind: ir.PartText, Text: "pong"}},
								},
							}}},
						)
					} else {
						msgs = append(msgs,
							ir.Message{Role: ir.RoleAssistant, Parts: asstParts},
							ir.Message{Role: ir.RoleUser, Parts: []ir.Part{{Kind: ir.PartText, Text: "Follow up"}}},
						)
					}
					maxOut := int64(128)
					req := &ir.Request{
						Model:           "test-model",
						MaxOutputTokens: &maxOut,
						Messages:        msgs,
					}
					require.NoError(t, ir.Validate(req))

					wireBytes, losses, err := EncodeRequest(proto, req.Clone(), Dialect{Tools: ToolSupportFunction})
					require.NoError(t, err)
					require.Contains(t, losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: "messages[1].parts[0]"})

					decoded, err := DecodeRequest(proto, wireBytes)
					require.NoError(t, err)
					diffs, err := testkit.VerifyEssence(ir.Essence(req), ir.Essence(decoded), losses)
					require.NoError(t, err)
					require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))
				})
			}
		})

		t.Run(string(proto)+"_response_and_stream_signed_text", func(t *testing.T) {
			for _, textVal := range []string{"Visible answer", ""} {
				name := "nonempty"
				if textVal == "" {
					name = "empty"
				}
				t.Run(name, func(t *testing.T) {
					srcResp := &ir.Response{
						ID:    "resp_direct_tok",
						Model: "test-model",
						Parts: []ir.Part{{
							Kind:  ir.PartText,
							Text:  textVal,
							Token: &ir.ReplayToken{Scheme: tok.Scheme, Data: tok.Data},
						}},
						Finish: ir.Finish{Reason: ir.FinishStop},
					}
					require.NoError(t, ir.ValidateResponse(srcResp))

					view := ClientView{Model: "test-model", Runtime: newDeterministicRuntime()}
					respBytes, respLosses, err := EncodeResponse(proto, srcResp.Clone(), view)
					require.NoError(t, err)
					require.Contains(t, respLosses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: "parts[0]"})

					dstResp, err := DecodeResponse(proto, respBytes)
					require.NoError(t, err)
					diffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(dstResp), respLosses)
					require.NoError(t, err)
					require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

					events := []ir.Event{
						{Kind: ir.EventResponseStart, Response: &ir.ResponseMeta{ID: "resp_direct_tok", Model: "test-model"}},
						{Kind: ir.EventPartStart, Index: 0, Part: &ir.Part{Kind: ir.PartText}},
					}
					if textVal != "" {
						events = append(events, ir.Event{Kind: ir.EventPartDelta, Index: 0, Delta: &ir.Delta{Text: textVal}})
					}
					events = append(events,
						ir.Event{Kind: ir.EventPartEnd, Index: 0, Part: &ir.Part{
							Kind:  ir.PartText,
							Text:  textVal,
							Token: &ir.ReplayToken{Scheme: tok.Scheme, Data: tok.Data},
						}},
						ir.Event{Kind: ir.EventResponseEnd, Finish: &ir.Finish{Reason: ir.FinishStop}},
					)
					require.NoError(t, ir.ValidateStream(events))

					enc, err := NewStreamEncoder(proto, ClientView{Model: "test-model", Runtime: newDeterministicRuntime()})
					require.NoError(t, err)
					var frames []sse.Frame
					var streamLosses []ir.Loss
					for _, ev := range events {
						fBatch, lBatch, encErr := enc.Encode(ev.Clone())
						require.NoError(t, encErr)
						frames = append(frames, fBatch...)
						streamLosses = append(streamLosses, lBatch...)
					}
					require.Contains(t, streamLosses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: "parts[0]"})

					_, decodedStreamResp := decodeRawStreamToResponse(t, proto, formatSSEFrames(frames))
					sDiffs, err := testkit.VerifyEssence(ir.ResponseEssence(srcResp), ir.ResponseEssence(decodedStreamResp), streamLosses)
					require.NoError(t, err)
					require.Empty(t, sDiffs, testkit.FormatJSONDiff(sDiffs))
				})
			}
		})
	}
}

// TestExchangeQoderChatUpstreamRegistrationErrorsStreamAndBuffer exercises
// ProtocolQoderChat upstream registration, client-direction rejection, billing
// header switches, origin-specific stateless rules, and custom tool restoration.
// It verifies that Responses and Cursor input requests enforce *qoder.RequestError
// gates while Chat and Messages requests preserve standalone tool results and
// stream/buffer conversions pass grammar validation.
func TestExchangeQoderChatUpstreamRegistrationErrorsStreamAndBuffer(t *testing.T) {
	require.True(t, HasUpstreamCodec(protocol.ProtocolQoderChat))
	require.False(t, HasClientCodec(protocol.ProtocolQoderChat))

	_, err := NewExchange([]byte(`{"model":"qwen3.8-max"}`), Options{
		Route: Route{
			Client:   protocol.ProtocolQoderChat,
			Upstream: protocol.ProtocolOpenAIResponses,
		},
	})
	require.ErrorContains(t, err, `unsupported client protocol "qoder_chat"`)

	t.Run("AllowBillingHeader preserves or strips x-anthropic-billing-header", func(t *testing.T) {
		msgBody := []byte(`{
			"model":"claude-opus-4-6",
			"max_tokens":512,
			"system":[
				{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.330; cch=a1b2F;"},
				{"type":"text","text":"Be helpful."}
			],
			"metadata":{"user_id":"user_cc_1"},
			"messages":[{"role":"user","content":"hello"}]
		}`)

		exKeep, err := NewExchange(msgBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolQoderChat,
				Dialect:  Dialect{AllowBillingHeader: true},
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upKeep, err := exKeep.UpstreamRequest()
		require.NoError(t, err)
		parsedKeep, err := qoder.DecodePayload(upKeep)
		require.NoError(t, err)
		require.Contains(t, parsedKeep.System, "x-anthropic-billing-header:")
		require.Contains(t, parsedKeep.NormalizedSystem(), "cch=<cch>;")
		require.Equal(t, "user_cc_1", parsedKeep.MetadataUserID)

		exStrip, err := NewExchange(msgBody, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolQoderChat,
				Dialect:  Dialect{AllowBillingHeader: false},
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upStrip, err := exStrip.UpstreamRequest()
		require.NoError(t, err)
		parsedStrip, err := qoder.DecodePayload(upStrip)
		require.NoError(t, err)
		require.NotContains(t, parsedStrip.System, "x-anthropic-billing-header:")
		require.Equal(t, "Be helpful.", parsedStrip.System)
		require.Contains(t, exStrip.Losses(), ir.Loss{
			Kind:   ir.LossClientMarkerDropped,
			Path:   "system[0]",
			Detail: "x-anthropic-billing-header",
		})
	})

	t.Run("stateful Responses and orphan tool_result unwrap as *qoder.RequestError", func(t *testing.T) {
		cases := []struct {
			name     string
			body     string
			wantCode string
		}{
			{
				name:     "store true",
				body:     `{"model":"qwen3.8-max","store":true,"input":"hi"}`,
				wantCode: qoder.CodeUnsupportedResponseState,
			},
			{
				name:     "previous_response_id",
				body:     `{"model":"qwen3.8-max","previous_response_id":"resp_1","input":"hi"}`,
				wantCode: qoder.CodeUnsupportedResponseState,
			},
			{
				name:     "conversation object",
				body:     `{"model":"qwen3.8-max","conversation":{"id":"conv_1"},"input":"hi"}`,
				wantCode: qoder.CodeUnsupportedResponseState,
			},
			{
				name:     "item_reference",
				body:     `{"model":"qwen3.8-max","input":[{"type":"item_reference","id":"msg_1"}]}`,
				wantCode: qoder.CodeUnsupportedResponseState,
			},
			{
				name:     "orphan function_call_output",
				body:     `{"model":"qwen3.8-max","input":[{"type":"function_call_output","call_id":"call_missing","output":"done"}]}`,
				wantCode: qoder.CodeMissingToolCall,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				ex, err := NewExchange([]byte(tc.body), Options{
					Route: Route{
						Client:   protocol.ProtocolOpenAIResponses,
						Upstream: protocol.ProtocolQoderChat,
						Dialect:  Dialect{AllowBillingHeader: true},
					},
				})
				require.NoError(t, err)
				_, upErr := ex.UpstreamRequest()
				require.Error(t, upErr)
				var qErr *qoder.RequestError
				require.True(t, errors.As(upErr, &qErr))
				require.Equal(t, tc.wantCode, qErr.Code)
			})
		}

		for _, cursorBody := range []string{
			`{"model":"qwen3.8-max","store":true,"input":"hi"}`,
			`{"model":"qwen3.8-max","input":[{"type":"function_call_output","call_id":"missing","output":"done"}]}`,
		} {
			exCursor, err := NewExchange([]byte(cursorBody), Options{
				Route: Route{
					Client:   protocol.ProtocolOpenAIChatCompletions,
					Upstream: protocol.ProtocolQoderChat,
				},
			})
			require.NoError(t, err)
			_, upErr := exCursor.UpstreamRequest()
			require.True(t, qoder.IsRequestError(upErr))
		}
	})

	t.Run("Chat and Messages preserve store true, standalone tool results, and ID-less Messages tool_result text", func(t *testing.T) {
		chatBody := []byte(`{
			"model":"auto",
			"store":true,
			"max_tokens":123,
			"messages":[
				{"role":"system","content":"be terse"},
				{"role":"user","content":[{"type":"text","text":"hello"}]},
				{"role":"assistant","content":"hi","tool_calls":[{"id":"call_unanswered","type":"function","function":{"name":"lookup","arguments":"{}"}}]},
				{"role":"tool","tool_call_id":"call_standalone","content":"tool output"}
			],
			"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}]
		}`)
		exChat, err := NewExchange(chatBody, Options{
			Route: Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolQoderChat,
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upChat, err := exChat.UpstreamRequest()
		require.NoError(t, err)
		parsedChat, err := qoder.DecodePayload(upChat)
		require.NoError(t, err)
		require.Len(t, parsedChat.Messages, 3)
		require.Equal(t, "assistant", parsedChat.Messages[1].Role)
		require.Len(t, parsedChat.Messages[1].ToolCalls, 1)
		require.Equal(t, "call_unanswered", parsedChat.Messages[1].ToolCalls[0].ID)
		require.Equal(t, "tool", parsedChat.Messages[2].Role)
		require.Equal(t, "call_standalone", parsedChat.Messages[2].ToolCallID)
		require.Equal(t, "tool output", parsedChat.Messages[2].Text)

		msgStandalone := []byte(`{
			"model":"claude-opus-4-6",
			"max_tokens":456,
			"messages":[
				{"role":"user","content":[{"type":"text","text":"hello"},{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"tool result"}]}]}
			]
		}`)
		exMsgStandalone, err := NewExchange(msgStandalone, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolQoderChat,
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upMsgStandalone, err := exMsgStandalone.UpstreamRequest()
		require.NoError(t, err)
		parsedMsgStandalone, err := qoder.DecodePayload(upMsgStandalone)
		require.NoError(t, err)
		require.Len(t, parsedMsgStandalone.Messages, 2)
		require.Equal(t, "user", parsedMsgStandalone.Messages[0].Role)
		require.Equal(t, "hello", parsedMsgStandalone.Messages[0].Text)
		require.Equal(t, "tool", parsedMsgStandalone.Messages[1].Role)
		require.Equal(t, "t1", parsedMsgStandalone.Messages[1].ToolCallID)
		require.Equal(t, "tool result", parsedMsgStandalone.Messages[1].Text)

		msgMissingID := []byte(`{
			"model":"auto",
			"messages":[
				{"role":"user","content":[{"type":"tool_result","content":"file.txt"}]}
			]
		}`)
		exMsgMissingID, err := NewExchange(msgMissingID, Options{
			Route: Route{
				Client:   protocol.ProtocolAnthropicMessages,
				Upstream: protocol.ProtocolQoderChat,
			},
			Runtime: newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upMsgMissingID, err := exMsgMissingID.UpstreamRequest()
		require.NoError(t, err)
		parsedMsgMissingID, err := qoder.DecodePayload(upMsgMissingID)
		require.NoError(t, err)
		require.Len(t, parsedMsgMissingID.Messages, 1)
		require.Equal(t, "user", parsedMsgMissingID.Messages[0].Role)
		require.Equal(t, "file.txt", parsedMsgMissingID.Messages[0].Text)
		require.Empty(t, parsedMsgMissingID.Messages[0].ToolCallID)
		require.Empty(t, parsedMsgMissingID.Messages[0].ToolCalls)
		require.Contains(t, exMsgMissingID.Losses(), ir.Loss{
			Kind:   ir.LossToolPairingRepaired,
			Path:   "messages[0].parts[0]",
			Detail: "missing_tool_call_id",
		})
		srcMissingIR, err := DecodeRequest(protocol.ProtocolAnthropicMessages, msgMissingID)
		require.NoError(t, err)
		dstMissingIR, err := qoderchat.DecodePayloadRequest(upMsgMissingID)
		require.NoError(t, err)
		missingDiffs, err := testkit.VerifyEssence(ir.Essence(srcMissingIR), ir.Essence(dstMissingIR), exMsgMissingID.Losses())
		require.NoError(t, err)
		require.Empty(t, missingDiffs, testkit.FormatJSONDiff(missingDiffs))

		for _, tc := range []struct {
			body     []byte // Anthropic Messages client JSON payload
			wantText string // expected single merged user message text
		}{
			{
				body:     []byte(`{"model":"auto","messages":[{"role":"user","content":[{"type":"tool_result","content":"lead.txt"},{"type":"text","text":"tail"}]}]}`),
				wantText: "lead.txt\ntail",
			},
			{
				body:     []byte(`{"model":"auto","messages":[{"role":"user","content":[{"type":"text","text":"before"},{"type":"tool_result","content":"file.txt"},{"type":"text","text":"after"}]}]}`),
				wantText: "before\nfile.txt\nafter",
			},
		} {
			exMerged, mErr := NewExchange(tc.body, Options{
				Route: Route{
					Client:   protocol.ProtocolAnthropicMessages,
					Upstream: protocol.ProtocolQoderChat,
				},
				Runtime: newDeterministicRuntime(),
			})
			require.NoError(t, mErr)
			upMerged, mErr := exMerged.UpstreamRequest()
			require.NoError(t, mErr)
			parsedMerged, mErr := qoder.DecodePayload(upMerged)
			require.NoError(t, mErr)
			require.Len(t, parsedMerged.Messages, 1)
			require.Equal(t, "user", parsedMerged.Messages[0].Role)
			require.Equal(t, tc.wantText, parsedMerged.Messages[0].Text)

			srcMergedIR, mErr := DecodeRequest(protocol.ProtocolAnthropicMessages, tc.body)
			require.NoError(t, mErr)
			dstMergedIR, mErr := qoderchat.DecodePayloadRequest(upMerged)
			require.NoError(t, mErr)
			mergedDiffs, mErr := testkit.VerifyEssence(ir.Essence(srcMergedIR), ir.Essence(dstMergedIR), exMerged.Losses())
			require.NoError(t, mErr)
			require.Empty(t, mergedDiffs, testkit.FormatJSONDiff(mergedDiffs))
		}
	})

	t.Run("Responses custom tool lowers to Qoder and restores across Stream and Buffer", func(t *testing.T) {
		respClientBody := []byte(`{
			"model":"qwen3.8-max",
			"stream":true,
			"session_id":"sess_codex",
			"prompt_cache_key":"pck_codex",
			"tools":[{"type":"custom","name":"apply_patch","description":"Patch file"}],
			"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Patch file"}]}]
		}`)

		qoderFrames := []sse.Frame{
			{Data: []byte(`{"body":"{\"choices\":[{\"delta\":{\"reasoning_content\":\"planning patch\"}}]}"}`)},
			{Data: []byte(`{"body":"{\"choices\":[{\"delta\":{\"content\":\"<tool_call>{\\\"name\\\":\\\"apply_patch\\\",\\\"arguments\\\":{\\\"input\\\":\\\"*** Begin Patch\\\"}}\"}</tool_call>\"}}]}"}`)},
		}
		qoderFrames[1] = sse.Frame{
			Data: []byte(`{"body":"{\"choices\":[{\"delta\":{\"content\":\"<tool_call>{\\\"name\\\":\\\"apply_patch\\\",\\\"arguments\\\":{\\\"input\\\":\\\"*** Begin Patch\\\"}}</tool_call>\"}}],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":7,\"prompt_tokens_details\":{\"cached_tokens\":15,\"cacheable_tokens\":3}}}"}`),
		}
		qoderFrames = append(qoderFrames, sse.Frame{Data: []byte(`{"body":"[DONE]"}`)})

		exStream, err := NewExchange(respClientBody, Options{
			Route: Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: protocol.ProtocolQoderChat,
				Dialect:  Dialect{AllowBillingHeader: true},
			},
			ClientModel: "gpt-5-codex",
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		upReq, err := exStream.UpstreamRequest()
		require.NoError(t, err)
		parsedUp, err := qoder.DecodePayload(upReq)
		require.NoError(t, err)
		require.Equal(t, "sess_codex", parsedUp.SessionID)
		require.Equal(t, "pck_codex", parsedUp.CacheKey)
		require.Len(t, parsedUp.Tools, 1)
		require.Equal(t, "apply_patch", parsedUp.Tools[0].Function.Name)

		st := exStream.Stream()
		var outFrames []sse.Frame
		for _, f := range qoderFrames {
			batch, wErr := st.Write(f)
			require.NoError(t, wErr)
			outFrames = append(outFrames, batch...)
		}
		tail, cErr := st.Close(nil)
		require.NoError(t, cErr)
		outFrames = append(outFrames, tail...)
		rawClientSSE := formatSSEFrames(outFrames)
		require.Empty(t, testkit.ValidateResponsesStream(outFrames, testkit.GrammarOptions{}))
		require.Contains(t, string(rawClientSSE), `"type":"custom_tool_call"`)
		require.Contains(t, string(rawClientSSE), `"*** Begin Patch"`)

		summary := st.Summary()
		require.True(t, summary.HasToolCall)
		require.NotNil(t, summary.Usage)
		require.Equal(t, int64(5), summary.Usage.Input)
		require.Equal(t, int64(15), summary.Usage.CacheRead)
		require.Equal(t, int64(0), summary.Usage.CacheWrite)
		require.Equal(t, int64(7), summary.Usage.Output)

		exBuf, err := NewExchange(respClientBody, Options{
			Route: Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: protocol.ProtocolQoderChat,
				Dialect:  Dialect{AllowBillingHeader: true},
			},
			ClientModel: "gpt-5-codex",
			Runtime:     newDeterministicRuntime(),
		})
		require.NoError(t, err)
		buf := exBuf.Buffer()
		for _, f := range qoderFrames {
			require.NoError(t, buf.Write(f))
		}
		bufJSON, err := buf.Close(nil)
		require.NoError(t, err)
		require.Contains(t, string(bufJSON), `"type":"custom_tool_call"`)
		require.Contains(t, string(bufJSON), `"input":"*** Begin Patch"`)
	})
}

// TestExchangeQoderChatLawsQ4AndQ5AcrossWireAndClientCorpus replays every
// shared wire and recorded client corpus request fixture from Anthropic
// Messages, OpenAI Responses, and OpenAI Chat Completions through Exchange into
// ProtocolQoderChat. It verifies that every valid fixture encodes without error,
// produces byte-identical output across repeated runs, and satisfies Law Q4
// VerifyEssence against DecodePayloadRequest.
func TestExchangeQoderChatLawsQ4AndQ5AcrossWireAndClientCorpus(t *testing.T) {
	clientProtos := []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	}
	for _, clientProto := range clientProtos {
		t.Run(string(clientProto), func(t *testing.T) {
			fixtures := allProtocolRequestFixtures(t, clientProto)
			for _, fx := range fixtures {
				t.Run(fx.Path, func(t *testing.T) {
					opts := Options{
						Route: Route{
							Client:   clientProto,
							Upstream: protocol.ProtocolQoderChat,
							Dialect: Dialect{
								Name:               "qoder",
								AllowBillingHeader: true,
							},
						},
						Runtime: newDeterministicRuntime(),
					}
					ex1, err := NewExchange(fx.Data, opts)
					require.NoError(t, err)
					up1, err1 := ex1.UpstreamRequest()
					require.NoError(t, err1)

					ex2, err := NewExchange(fx.Data, opts)
					require.NoError(t, err)
					up2, err2 := ex2.UpstreamRequest()
					require.NoError(t, err2)
					require.Equal(t, up1, up2, "Law Q5 determinism violated for %s", fx.Path)

					srcIR, err := DecodeRequest(clientProto, fx.Data)
					require.NoError(t, err)
					dstIR, err := qoderchat.DecodePayloadRequest(up1)
					require.NoError(t, err)

					diffs, err := testkit.VerifyEssence(ir.Essence(srcIR), ir.Essence(dstIR), ex1.Losses())
					require.NoError(t, err)
					require.Empty(t, diffs, "Law Q4 essence mismatch on %s:\n%s", fx.Path, testkit.FormatJSONDiff(diffs))
				})
			}
		})
	}
}

// TestExchange_MapUpstreamToolNameComposesWithLoweredTools exercises
// Options.MapUpstreamToolName on Qoder streaming, buffered SSE folding, and
// unary Chat response paths carrying lowered client tools. It verifies that
// upstream alias tool names map back to lowered declaration names before
// raiseTools restores custom, namespaced, and tool_search client identities.
func TestExchange_MapUpstreamToolNameComposesWithLoweredTools(t *testing.T) {
	clientBody := []byte(`{
		"model":"auto",
		"stream":true,
		"tools":[
			{"type":"custom","name":"bash","description":"Execute shell commands"},
			{"type":"tool_search"},
			{"type":"namespace","name":"ops","description":"Ops tools","tools":[
				{"type":"function","name":"read_log","description":"Read logs","parameters":{"type":"object","properties":{"path":{"type":"string"}}}},
				{"type":"custom","name":"todo_write","description":"Write todo"}
			]}
		],
		"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Run diagnostics"}]}]
	}`)

	reverseMap := func(name string) string {
		switch name {
		case "Bash":
			return "bash"
		case "Tool_Search":
			return "tool_search"
		case "OPS__READ_LOG":
			return "ops__read_log"
		case "Ops_Todo_Write":
			return "ops__todo_write"
		default:
			return name
		}
	}

	makeExchange := func(t *testing.T, upProto protocol.ProtocolID) *Exchange {
		t.Helper()
		ex, err := NewExchange(clientBody, Options{
			Route: Route{
				Client:   protocol.ProtocolOpenAIResponses,
				Upstream: upProto,
			},
			ClientModel:         "auto",
			Runtime:             newDeterministicRuntime(),
			MapUpstreamToolName: reverseMap,
		})
		require.NoError(t, err)
		_, err = ex.UpstreamRequest()
		require.NoError(t, err)
		return ex
	}

	qoderFrames := []sse.Frame{
		{Data: []byte(`{"body":"{\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_bash_1\",\"type\":\"function\",\"function\":{\"name\":\"shell\",\"arguments\":\"{\\\"input\\\":\\\"pwd\\\"}\"}},{\"index\":1,\"id\":\"call_search_1\",\"type\":\"function\",\"function\":{\"name\":\"Tool_Search\",\"arguments\":\"{\\\"query\\\":\\\"ops\\\"}\"}},{\"index\":2,\"id\":\"call_ns_fn_1\",\"type\":\"function\",\"function\":{\"name\":\"OPS__READ_LOG\",\"arguments\":\"{\\\"path\\\":\\\"/var/log/app.log\\\"}\"}},{\"index\":3,\"id\":\"call_ns_custom_1\",\"type\":\"function\",\"function\":{\"name\":\"Ops_Todo_Write\",\"arguments\":\"{\\\"input\\\":\\\"1. check logs\\\"}\"}}]}}]}"}`)},
		{Data: []byte(`{"body":"[DONE]"}`)},
	}

	// 1. Streaming conversion over Qoder SSE frames.
	exStream := makeExchange(t, protocol.ProtocolQoderChat)
	stream := exStream.Stream()
	var clientFrames []sse.Frame
	for _, f := range qoderFrames {
		batch, err := stream.Write(f)
		require.NoError(t, err)
		clientFrames = append(clientFrames, batch...)
	}
	tail, err := stream.Close(nil)
	require.NoError(t, err)
	clientFrames = append(clientFrames, tail...)

	var rawStream strings.Builder
	for _, f := range clientFrames {
		_, _ = rawStream.Write(sse.Append(nil, f))
	}
	streamText := rawStream.String()
	require.Contains(t, streamText, `"store":false`)
	require.Contains(t, streamText, `"type":"custom_tool_call","status":"completed","call_id":"call_bash_1","name":"bash","input":"pwd"`)
	require.Contains(t, streamText, `"type":"tool_search_call","status":"completed","call_id":"call_search_1","execution":"client","arguments":{"query":"ops"}`)
	require.Contains(t, streamText, `"type":"function_call","status":"completed","call_id":"call_ns_fn_1","namespace":"ops","name":"read_log","arguments":"{\"path\":\"/var/log/app.log\"}"`)
	require.Contains(t, streamText, `"type":"custom_tool_call","status":"completed","call_id":"call_ns_custom_1","namespace":"ops","name":"todo_write","input":"1. check logs"`)

	// 2. Buffered SSE folding over the exact same Qoder SSE frames.
	exBuf := makeExchange(t, protocol.ProtocolQoderChat)
	buf := exBuf.Buffer()
	for _, f := range qoderFrames {
		require.NoError(t, buf.Write(f))
	}
	bufJSON, err := buf.Close(nil)
	require.NoError(t, err)
	bufText := string(bufJSON)
	require.Contains(t, bufText, `"store":false`)
	require.Contains(t, bufText, `"type":"custom_tool_call","status":"completed","call_id":"call_bash_1","name":"bash","input":"pwd"`)
	require.Contains(t, bufText, `"type":"tool_search_call","status":"completed","call_id":"call_search_1","execution":"client","arguments":{"query":"ops"}`)
	require.Contains(t, bufText, `"type":"function_call","status":"completed","call_id":"call_ns_fn_1","namespace":"ops","name":"read_log"`)
	require.Contains(t, bufText, `"type":"custom_tool_call","status":"completed","call_id":"call_ns_custom_1","namespace":"ops","name":"todo_write","input":"1. check logs"`)

	// 3. Unary buffered Response path (using Chat upstream codec since Qoder is stream-only).
	exUnary := makeExchange(t, protocol.ProtocolOpenAIChatCompletions)
	chatReply := []byte(`{
		"id":"chatcmpl-1",
		"object":"chat.completion",
		"created":1700000000,
		"model":"auto",
		"choices":[{
			"index":0,
			"message":{
				"role":"assistant",
				"content":null,
				"tool_calls":[
					{"id":"call_bash_1","type":"function","function":{"name":"Bash","arguments":"{\"input\":\"pwd\"}"}},
					{"id":"call_ns_fn_1","type":"function","function":{"name":"OPS__READ_LOG","arguments":"{\"path\":\"/var/log/app.log\"}"}}
				]
			},
			"finish_reason":"tool_calls"
		}],
		"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}
	}`)
	unaryJSON, err := exUnary.Response(chatReply)
	require.NoError(t, err)
	require.Contains(t, string(unaryJSON), `"type":"custom_tool_call","status":"completed","call_id":"call_bash_1","name":"bash","input":"pwd"`)
	require.Contains(t, string(unaryJSON), `"type":"function_call","status":"completed","call_id":"call_ns_fn_1","namespace":"ops","name":"read_log"`)
}

// TestExchange_DiagnosticRouteLossCountsAndAbnormalClose verifies that Exchange
// exposes detached Route copies, aggregates request and response losses by
// LossKind, replaces request losses cleanly on retry adoption, and retains
// response-side losses when Stream.Close or Buffer.Close terminates with an error.
func TestExchange_DiagnosticRouteLossCountsAndAbnormalClose(t *testing.T) {
	var nilEx *Exchange
	require.Equal(t, Route{}, nilEx.Route())
	require.Nil(t, nilEx.LossCounts())
	require.Nil(t, nilEx.Losses())

	reqBody := []byte(`{
		"model":"claude-sonnet-4-5",
		"max_tokens":256,
		"temperature":0.7,
		"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}]
	}`)
	ex, err := NewExchange(reqBody, Options{
		Route: Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolOpenAIChatCompletions,
		},
	})
	require.NoError(t, err)
	require.Nil(t, ex.LossCounts())

	msgToChat, err := NewExchange(reqBody, Options{
		Route: Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolOpenAIChatCompletions,
			Dialect: Dialect{
				Name:            "openai",
				DropTemperature: true,
			},
		},
		ClientModel: "claude-sonnet-4-5",
		Runtime:     newDeterministicRuntime(),
	})
	require.NoError(t, err)

	gotRoute := msgToChat.Route()
	require.Equal(t, protocol.ProtocolAnthropicMessages, gotRoute.Client)
	require.Equal(t, protocol.ProtocolOpenAIChatCompletions, gotRoute.Upstream)
	require.Equal(t, "openai", gotRoute.Dialect.Name)

	_, err = msgToChat.UpstreamRequest()
	require.NoError(t, err)
	reqCounts := msgToChat.LossCounts()
	require.Equal(t, 1, reqCounts[string(ir.LossSamplingDropped)])
	require.Equal(t, 1, reqCounts[string(ir.LossCacheMarkDropped)])

	// ReplaceRequestLosses swaps request-stage losses without dropping subsequent response losses.
	msgToChat.ReplaceRequestLosses([]ir.Loss{{Kind: ir.LossReplayTokenDropped, Path: "messages[0]"}})
	require.Equal(t, map[string]int{string(ir.LossReplayTokenDropped): 1}, msgToChat.LossCounts())

	// Abnormal Stream.Close(cause) and Buffer.Close(cause) commit sealer losses onto Exchange.
	geminiStreamFrames := []sse.Frame{
		{Data: []byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"answer","thoughtSignature":"sig_gem_1"}]}}]}`)},
	}
	chatToGem, err := NewExchange([]byte(`{"model":"gpt-5","stream":true,"messages":[{"role":"user","content":"hi"}]}`), Options{
		Route: Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: Dialect{
				Name:         "gemini",
				ReplayScheme: ir.SchemeGemini,
			},
		},
		Runtime: newDeterministicRuntime(),
	})
	require.NoError(t, err)
	st := chatToGem.Stream()
	for _, f := range geminiStreamFrames {
		_, wErr := st.Write(f)
		require.NoError(t, wErr)
	}
	_, closeErr := st.Close(errors.New("upstream transport reset"))
	require.NoError(t, closeErr)
	require.GreaterOrEqual(t, chatToGem.LossCounts()[string(ir.LossReplayTokenDropped)], 1)

	// Re-observing the same wire retry must preserve the response-side loss
	// exactly once. An independently re-encoded request replaces all wire counts.
	beforeRetry := chatToGem.LossCounts()[string(ir.LossReplayTokenDropped)]
	chatToGem.SetSignatureRetryLosses(2, 1)
	chatToGem.SetSignatureRetryLosses(2, 1)
	require.Equal(t, beforeRetry+2, chatToGem.LossCounts()[string(ir.LossReplayTokenDropped)])
	require.Equal(t, 1, chatToGem.LossCounts()[string(ir.LossReasoningTextDropped)])
	chatToGem.ReplaceRequestLosses(nil)
	require.Equal(t, beforeRetry, chatToGem.LossCounts()[string(ir.LossReplayTokenDropped)])
	require.Zero(t, chatToGem.LossCounts()[string(ir.LossReasoningTextDropped)])

	// Abnormal Buffer.Close(cause) returns cause while still committing response-side losses.
	chatToGemBuf, err := NewExchange([]byte(`{"model":"gpt-5","stream":false,"messages":[{"role":"user","content":"hi"}]}`), Options{
		Route: Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: Dialect{
				Name:         "gemini",
				ReplayScheme: ir.SchemeGemini,
			},
		},
		Runtime: newDeterministicRuntime(),
	})
	require.NoError(t, err)
	buf := chatToGemBuf.Buffer()
	for _, f := range geminiStreamFrames {
		require.NoError(t, buf.Write(f))
	}
	_, bufErr := buf.Close(errors.New("upstream read timeout"))
	require.Error(t, bufErr)
	require.GreaterOrEqual(t, chatToGemBuf.LossCounts()[string(ir.LossReplayTokenDropped)], 1)

	// ReencodeUpstreamLosses remaps recovery losses via ReencodeEditor and
	// updates source coordinates when parts are removed or replaced before encoding.
	reBytes, reLosses, changed, reErr := chatToGemBuf.ReencodeUpstreamLosses(func(req *ir.Request, _ *Dialect, edit *ReencodeEditor) (bool, error) {
		req.Messages[0].Parts[0].Cache = &ir.CacheMark{}
		edit.RecordPartLoss(0, 0, ir.LossReplayTokenDropped)
		return true, nil
	})
	require.NoError(t, reErr)
	require.True(t, changed)
	require.NotEmpty(t, reBytes)
	require.Contains(t, reLosses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: "messages[0].parts[0]"})
	require.Contains(t, reLosses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: "messages[0].parts[0]"})

	// Removing parts[0] during recovery must shift coordinates so post-mutation
	// encoder losses on the surviving part still point to original parts[1],
	// while synthetic placeholder parts omit synthetic encoder losses.
	encOnlyCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAntigravityClaude,
		Data:   "enc_secret_sig",
	})
	require.NoError(t, err)
	msgWithEncryptedAndCachedText := []byte(`{
		"model":"claude-sonnet-4-5",
		"max_tokens":256,
		"messages":[
			{"role":"user","content":"continue"},
			{"role":"assistant","content":[
				{"type":"thinking","thinking":"","signature":"` + encOnlyCarrier + `"},
				{"type":"text","text":"cached answer","cache_control":{"type":"ephemeral"}}
			]},
			{"role":"user","content":"next"}
		]
	}`)
	shiftedEx, err := NewExchange(msgWithEncryptedAndCachedText, Options{
		Route: Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: Dialect{
				Name:          "antigravity",
				ReplayScheme:  ir.SchemeAntigravityClaude,
				SchemaDialect: SchemaAntigravity,
			},
		},
		Runtime: newDeterministicRuntime(),
	})
	require.NoError(t, err)
	shiftedBytes, shiftedLosses, shiftedChanged, shiftedErr := shiftedEx.ReencodeUpstreamLosses(func(req *ir.Request, _ *Dialect, edit *ReencodeEditor) (bool, error) {
		edit.RecordPartLoss(1, 0, ir.LossReplayTokenDropped)
		edit.SetMessageParts(1, []ir.Part{req.Messages[1].Parts[1]}, []ReencodePartSource{{Part: 1}})
		return true, nil
	})
	require.NoError(t, shiftedErr)
	require.True(t, shiftedChanged)
	require.NotEmpty(t, shiftedBytes)
	require.Contains(t, shiftedLosses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: "messages[1].parts[0]"})
	foundShiftedCache := false
	for _, loss := range shiftedLosses {
		if loss.Kind == ir.LossCacheMarkDropped {
			require.True(t, strings.HasPrefix(loss.Path, "messages[1].parts[1]"), "expected shifted cache_mark_dropped on messages[1].parts[1], got %q", loss.Path)
			foundShiftedCache = true
		}
	}
	require.True(t, foundShiftedCache, "expected cache_mark_dropped loss on surviving cached text part")

	// Synthetic replacement parts tagged with source index -1 must suppress
	// encoder losses rather than emitting negative indices.
	synthBytes, synthLosses, synthChanged, synthErr := shiftedEx.ReencodeUpstreamLosses(func(_ *ir.Request, _ *Dialect, edit *ReencodeEditor) (bool, error) {
		edit.RecordPartLoss(1, 0, ir.LossReplayTokenDropped)
		edit.SetMessageParts(1, []ir.Part{{Kind: ir.PartText, Text: "(content removed)", Cache: &ir.CacheMark{}}}, []ReencodePartSource{{Part: -1}})
		return true, nil
	})
	require.NoError(t, synthErr)
	require.True(t, synthChanged)
	require.NotEmpty(t, synthBytes)
	for _, loss := range synthLosses {
		require.NotContains(t, loss.Path, "[-1]")
		require.NotEqual(t, ir.LossCacheMarkDropped, loss.Kind)
	}
}
