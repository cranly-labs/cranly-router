package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/billing"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit/mockupstream"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// protocol_lab_stage5_scenario_test.go exercises same-protocol OpenAI Responses
// client-tool lowering and response restoration across http_routes.go,
// gateway_middleware.go, gateway_selection.go, gateway_openai_text_http.go,
// gateway_openai_text_execution.go, gateway_openai_attempts.go,
// gateway_response_output.go, gateway_grok_execution.go,
// gateway_openai_ws_http.go, and gateway_ws_connections.go. Function-only
// Responses upstreams (OpenAI API Key passthrough, DeepSeek API Key Responses,
// Grok OAuth and API Key, and the Responses WebSocket HTTP bridge) reject
// freeform custom tools, scoped namespaces, and client tool_search declarations
// unless the gateway lowers them on egress and reconstructs client identities on
// streaming and buffered responses.
//
// End-to-end subtests cover canonical wire scenarios, hosted-tool intermediate
// stream events, Grok Compact turns, Codex alias reversal, multi-turn WebSocket
// warmup continuations, explicit tools:[] session resets, pre-output failover
// isolation, and local HTTP 400 or WebSocket 1008 collision gates without
// provider health penalties.

const (
	// Stage 5 T5 law identifiers classify HTTP client-tool restoration and
	// Responses WebSocket HTTP bridge multi-turn scenario cells.
	lawT5ClientTools = "T5_CLIENT_TOOLS" // HTTP streaming/buffered client-tool adaptation law
	lawT5WSBridge    = "T5_WS_BRIDGE"    // Responses WebSocket HTTP bridge client-tool law

	// Stage 5 synthetic scenario slugs identify targeted client-tool, hosted
	// event, collision, and WebSocket warmup/continuation cells.
	scenarioStage5BufferedTools        = "buffered_custom_namespace_search"        // buffered JSON custom + search + namespace + scoped custom
	scenarioStage5ScopedCustom         = "scoped_custom_in_namespace"              // custom child tool nested inside namespace declaration
	scenarioStage5OmittedArgsDone      = "custom_stream_omitted_args_done"         // lowered custom stream omitting function_call_arguments.done
	scenarioStage5HostedStreamEvents   = "hosted_intermediate_events_passthrough"  // partial_image + web_search_call.searching + mcp_* interleaved
	scenarioStage5ToolCollision400     = "tool_name_collision_local_400"           // colliding tool names rejected locally with 400 and zero failover/penalty
	scenarioStage5WSWarmupContinuation = "ws_warmup_continuation_custom_namespace" // WS generate=false warmup + omitted tools/model + custom/namespace replay
	scenarioStage5WSExplicitToolsReset = "ws_explicit_tools_empty_reset"           // WS explicit tools:[] clears inherited session LoweredTools
	scenarioStage5WSPrebyteFailover    = "ws_prebyte_failover_toolmap_isolation"   // WS Turn 1 additional_tools and Turn 2 HTTP 429 failover isolation
	scenarioStage5WSScopedCustom       = "ws_scoped_custom_in_namespace"           // WS bridge restoration of custom child inside namespace
	scenarioStage5WSNativeCarrier      = "ws_native_carrier_replay"                // WS bridge PrepareNativeReplay same-account unpack and foreign strip
	scenarioStage5WSToolCollision1008  = "ws_tool_conflict_1008_no_penalty"        // WS bridge collision closes with 1008 and zero upstream/health side effects

	// Stage 5 violation rule codes report un-restored client tools, dropped
	// hosted-tool events, unexpected failover, or schedule health penalties.
	ruleClientToolNotRestored      = "client_tool_not_restored"     // lowered custom, namespace, or tool_search call was not restored
	ruleHostedStreamEventDropped   = "hosted_stream_event_dropped"  // partial_image, web_search_call.searching, or mcp_* event disappeared
	ruleUnexpectedCollisionStatus  = "unexpected_collision_status"  // tool collision did not return local HTTP 400 or WS 1008
	ruleCollisionTriggeredFailover = "collision_triggered_failover" // local tool collision contacted upstream or fallback provider
	ruleCollisionPenalizedHealth   = "collision_penalized_health"   // local tool collision recorded provider error or cooldown penalty
	ruleWSStaleToolsLeaked         = "ws_stale_tools_leaked"        // cleared or duplicate WS session tools leaked to upstream request
)

// TestProtocolLabIRStage5ResponsesClientToolsMatrix exercises Stage 5's
// same-protocol OpenAI Responses client-tool lowering and restoration across OpenAI API Key passthrough, DeepSeek API Key
// Responses, Grok OAuth and API Key (including /v1/responses/compact), and the
// Responses WebSocket HTTP bridge. It verifies streaming and buffered tool
// restoration, hosted-tool intermediate event passthrough, DeepSeek field
// preservation, Codex alias reversal, multi-turn WS warmup continuations,
// local 400/1008 collision gates without failover or health penalties, and
// exact client-vs-UsageLog token parity.
func TestProtocolLabIRStage5ResponsesClientToolsMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	httpSpecs := stage5FunctionOnlyResponsesProviders()
	respClient := protocolLabClients[1]

	t.Run("canonical_wire_scenarios_and_usage_across_http_routes", func(t *testing.T) {
		for _, prov := range httpSpecs {
			combo := labRouteCombo{
				slug:     respClient.slug + "_via_" + prov.kind,
				client:   respClient,
				provider: prov,
			}
			t.Run(combo.slug, func(t *testing.T) {
				for _, scenario := range []string{
					"text_basic",
					"text_unicode_split",
					"system_roles",
					"tool_single",
					"tool_parallel",
					"tool_args_fragmented",
					"tool_args_empty",
					"tool_result_image",
					"tool_result_error",
					"reasoning_summary",
					"reasoning_tool_replay",
					"reasoning_carrier_native",
					"tools_custom",
					"tools_namespace",
					"tools_search",
					"tools_hosted_search",
					"image_input",
					"document_input",
					"structured_output",
					"max_tokens",
					"refusal",
					"usage_cache",
					"empty_response",
				} {
					streamEntry, usageEntry := runStage5ResponsesScenarioCell(t, corpus, combo, scenario)
					require.Equal(t, labCellPass, streamEntry.Status,
						"%s/%s stream failed: rules=%v", combo.slug, scenario, streamEntry.Rules)
					require.Equal(t, labCellPass, usageEntry.Status,
						"%s/%s usage failed: rules=%v", combo.slug, scenario, usageEntry.Rules)
				}

				failoverEntry := runLabPreByteFailoverCell(t, corpus, combo)
				require.Equal(t, labCellPass, failoverEntry.Status,
					"%s failover failed: rules=%v", combo.slug, failoverEntry.Rules)

				errEntry := runLabErrorFormatCell(t, corpus, combo)
				require.True(t, errEntry.Status == labCellPass || isStage5NativeNonConvertedBehavior(combo.provider.kind, errEntry),
					"%s error format failed: rules=%v", combo.slug, errEntry.Rules)

				// Client-actionable 429 rate-limit errors format as 4xx client JSON envelopes on all routes.
				rlH := mockupstream.NewWithConfig(mockupstream.Config{
					Script: testkit.Script{
						Name: "s5_rate_limit_envelope",
						Turns: []testkit.Turn{{
							Fault: &testkit.Fault{
								Status:    http.StatusTooManyRequests,
								ErrorBody: formatUpstreamJSONError(protocol.ProtocolOpenAIResponses, http.StatusTooManyRequests, "rate limit reached"),
							},
						}},
					},
					Validation: combo.provider.validation,
				})
				rlSrv := newLabMockServer(combo.provider, rlH)
				defer rlSrv.Close()
				rlLab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(1, 0, rlSrv.URL, combo.provider)},
				})
				rlGW := httptest.NewServer(rlLab)
				defer rlGW.Close()
				rlStatus, _, rlBody := sendLabClientRequest(t, rlGW, combo.client, []byte(`{"model":"gpt-5","stream":true,"input":[{"type":"message","role":"user","content":"ping"}]}`))
				require.Equal(t, http.StatusTooManyRequests, rlStatus)
				require.True(t, hasClientErrorEnvelope(protocol.ProtocolOpenAIResponses, rlBody))
				require.Empty(t, rlLab.UsageLogs())
			})
		}
	})

	t.Run("buffered_scoped_custom_and_omitted_args_done_across_http_routes", func(t *testing.T) {
		for _, prov := range httpSpecs {
			combo := labRouteCombo{
				slug:     respClient.slug + "_via_" + prov.kind,
				client:   respClient,
				provider: prov,
			}
			t.Run(combo.slug, func(t *testing.T) {
				bufEntry := runStage5BufferedToolsCell(t, combo)
				require.Equal(t, labCellPass, bufEntry.Status,
					"%s buffered restoration failed: rules=%v", combo.slug, bufEntry.Rules)

				scopedEntry := runStage5ScopedCustomCell(t, combo)
				require.Equal(t, labCellPass, scopedEntry.Status,
					"%s scoped custom restoration failed: rules=%v", combo.slug, scopedEntry.Rules)

				omitEntry := runStage5OmittedArgsDoneCell(t, combo)
				require.Equal(t, labCellPass, omitEntry.Status,
					"%s omitted arguments.done synthesis failed: rules=%v", combo.slug, omitEntry.Rules)

				hostedEntry := runStage5HostedIntermediateEventsCell(t, combo)
				require.Equal(t, labCellPass, hostedEntry.Status,
					"%s hosted intermediate events failed: rules=%v", combo.slug, hostedEntry.Rules)
			})
		}
	})

	t.Run("deepseek_strips_prompt_cache_options_while_preserving_breakpoints_compaction_and_bare_tokens", func(t *testing.T) {
		foreignCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "EqQBForeignAnthropicSig",
			Text:   "Foreign Anthropic thought.",
		})
		require.NoError(t, err)

		reqBody := []byte(fmt.Sprintf(`{
			"model":"gpt-5",
			"stream":false,
			"store":false,
			"prompt_cache_options":{"ttl":"5m"},
			"x_custom_u64":9007199254740993,
			"tools":[
				{"type":"custom","name":"apply_patch","description":"Freeform patch"},
				{"type":"tool_search","execution":"client"},
				{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}
			],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"inspect cache"}],"prompt_cache_breakpoint":true},
				{"type":"compaction","id":"cmp_keep_1","encrypted_content":"cmp_state_blob"},
				{"type":"reasoning","id":"rs_bare_keep","summary":[{"type":"summary_text","text":"bare summary"}],"encrypted_content":"gAAAA_bare_native_token"},
				{"type":"reasoning","id":"rs_foreign_drop","summary":[],"encrypted_content":%q},
				{"type":"message","id":"msg_prev_1","role":"assistant","content":[{"type":"output_text","text":"prior"}],"prompt_cache_breakpoint":true},
				{"type":"custom_tool_call","id":"ctc_h1","call_id":"call_h1","name":"apply_patch","input":"*** Begin Patch\n*** End Patch"},
				{"type":"custom_tool_call_output","id":"ctco_h1","call_id":"call_h1","output":"ok"}
			]
		}`, foreignCarrier))

		bufferedUpstreamJSON := `{
			"id":"resp_ds_preserve",
			"object":"response",
			"created_at":1760000500,
			"status":"completed",
			"model":"deepseek-chat",
			"output":[
				{"id":"fc_out_1","type":"function_call","status":"completed","call_id":"call_out_1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** Add File: a.txt\\n+ok\\n*** End Patch\"}"},
				{"id":"fc_out_2","type":"function_call","status":"completed","call_id":"call_out_2","name":"db__query","arguments":"{\"sql\":\"select 1\"}"}
			],
			"usage":{"input_tokens":80,"output_tokens":20,"total_tokens":100,"input_tokens_details":{"cached_tokens":30},"output_tokens_details":{"reasoning_tokens":0}}
		}`

		for _, tc := range []struct {
			name                  string              // subtest scenario name
			spec                  labProviderKindSpec // target provider specification under test
			wantPromptCacheOption bool                // whether top-level prompt_cache_options survives upstream
		}{
			{
				name:                  "deepseek_responses_strips_top_level_prompt_cache_options",
				spec:                  stage5DeepSeekResponsesLabSpec(),
				wantPromptCacheOption: false,
			},
			{
				name:                  "openai_apikey_passthrough_keeps_top_level_prompt_cache_options",
				spec:                  stage5OpenAIAPIKeyPassthroughLabSpec(),
				wantPromptCacheOption: true,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var mu sync.Mutex
				var captured [][]byte
				upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, readErr := io.ReadAll(r.Body)
					require.NoError(t, readErr)
					mu.Lock()
					captured = append(captured, raw)
					mu.Unlock()
					violations := testkit.ValidateResponsesRequest(raw, tc.spec.validation)
					require.Empty(t, violations, "function-only violations: %+v", violations)
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, bufferedUpstreamJSON)
				}))
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(301, 0, upSrv.URL, tc.spec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				status, ct, respBytes := sendLabClientRequest(t, gwSrv, respClient, reqBody)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, strings.ToLower(ct), "application/json")

				mu.Lock()
				require.Len(t, captured, 1)
				wireBody := captured[0]
				mu.Unlock()

				require.Equal(t, tc.wantPromptCacheOption, gjson.GetBytes(wireBody, "prompt_cache_options").Exists())
				require.Equal(t, "9007199254740993", gjson.GetBytes(wireBody, "x_custom_u64").Raw)
				require.True(t, gjson.GetBytes(wireBody, "input.0.prompt_cache_breakpoint").Bool())
				require.Equal(t, "compaction", gjson.GetBytes(wireBody, "input.1.type").String())
				require.Equal(t, "cmp_state_blob", gjson.GetBytes(wireBody, "input.1.encrypted_content").String())
				require.Equal(t, "reasoning", gjson.GetBytes(wireBody, "input.2.type").String())
				require.Equal(t, "gAAAA_bare_native_token", gjson.GetBytes(wireBody, "input.2.encrypted_content").String())
				require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, wireBody))
				require.Equal(t, "message", gjson.GetBytes(wireBody, "input.3.type").String())
				require.True(t, gjson.GetBytes(wireBody, "input.3.prompt_cache_breakpoint").Bool())
				require.Equal(t, "function_call", gjson.GetBytes(wireBody, "input.4.type").String())
				require.Equal(t, "function_call_output", gjson.GetBytes(wireBody, "input.5.type").String())

				// Client buffered JSON receives restored custom_tool_call and namespaced function_call.
				require.Equal(t, "custom_tool_call", gjson.GetBytes(respBytes, "output.0.type").String())
				require.Contains(t, gjson.GetBytes(respBytes, "output.0.input").String(), "*** Add File: a.txt")
				require.Equal(t, "function_call", gjson.GetBytes(respBytes, "output.1.type").String())
				require.Equal(t, "db", gjson.GetBytes(respBytes, "output.1.namespace").String())
				require.Equal(t, "query", gjson.GetBytes(respBytes, "output.1.name").String())

				logs := lab.UsageLogs()
				require.Len(t, logs, 1)
				require.Equal(t, 50, logs[0].InputTokens)
				require.Equal(t, 30, logs[0].CacheReadTokens)
				require.Equal(t, 20, logs[0].OutputTokens)
			})
		}
	})

	t.Run("grok_responses_compact_lowers_client_tools_and_wraps_compaction_item", func(t *testing.T) {
		compactReq := []byte(`{
			"model":"gpt-5",
			"tools":[
				{"type":"custom","name":"apply_patch","description":"Edit files"},
				{"type":"tool_search","execution":"client"}
			],
			"input":[
				{"type":"additional_tools","tools":[
					{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}
				]},
				{"type":"message","role":"user","content":"summarize prior patch"},
				{"type":"custom_tool_call","id":"ctc_c1","call_id":"call_c1","name":"apply_patch","input":"*** Begin Patch\n*** End Patch"},
				{"type":"custom_tool_call_output","id":"ctco_c1","call_id":"call_c1","output":"patched"}
			]
		}`)
		grokCompactUpstreamResp := `{
			"id":"resp_grok_cmp",
			"object":"response",
			"model":"grok-4.5",
			"status":"completed",
			"output":[
				{"id":"rs_cmp","type":"reasoning","summary":[],"encrypted_content":"xai_compact_state_blob"},
				{"id":"msg_cmp","type":"message","role":"assistant","content":[{"type":"output_text","text":"<summary>Patched cleanly.</summary>"}]}
			],
			"usage":{"input_tokens":44,"output_tokens":12,"total_tokens":56,"input_tokens_details":{"cached_tokens":14}}
		}`

		for _, grokSpec := range grokLabProviderSpecs() {
			for _, compactPath := range []string{"/v1/responses/compact", "/responses/compact"} {
				t.Run(grokSpec.kind+compactPath, func(t *testing.T) {
					var mu sync.Mutex
					var captured [][]byte
					upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						raw, rErr := io.ReadAll(r.Body)
						require.NoError(t, rErr)
						mu.Lock()
						captured = append(captured, raw)
						mu.Unlock()
						violations := testkit.ValidateResponsesRequest(raw, grokSpec.validation)
						require.Empty(t, violations, "Grok compact function-only violations: %+v", violations)
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						_, _ = io.WriteString(w, grokCompactUpstreamResp)
					}))
					defer upSrv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{makeLabProviderSpec(401, 0, upSrv.URL, grokSpec)},
						AllowedProtocols: []protocol.ProtocolID{
							protocol.ProtocolOpenAIResponses,
							protocol.ProtocolResponsesCompact,
						},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					compactClient := respClient
					compactClient.path = compactPath
					status, _, respBytes := sendLabClientRequest(t, gwSrv, compactClient, compactReq)
					require.Equal(t, http.StatusOK, status, string(respBytes))

					mu.Lock()
					require.Len(t, captured, 1)
					wireBody := captured[0]
					mu.Unlock()

					require.Equal(t, "none", gjson.GetBytes(wireBody, "tool_choice").String())
					require.False(t, gjson.GetBytes(wireBody, "store").Bool())
					require.Equal(t, "function", gjson.GetBytes(wireBody, "tools.0.type").String())
					require.Equal(t, "apply_patch", gjson.GetBytes(wireBody, "tools.0.name").String())
					require.Equal(t, "compaction", gjson.GetBytes(respBytes, "output.0.type").String())
					require.NotEmpty(t, gjson.GetBytes(respBytes, "output.0.encrypted_content").String())

					logs := lab.UsageLogs()
					require.Len(t, logs, 1)
					require.Equal(t, 30, logs[0].InputTokens)
					require.Equal(t, 14, logs[0].CacheReadTokens)
					require.Equal(t, 12, logs[0].OutputTokens)
				})
			}
		}
	})

	t.Run("codex_tool_name_reverse_and_native_namespace_switch_coordination", func(t *testing.T) {
		// OpenAI OAuth with openai_responses_flatten_namespaces=true keeps
		// ToolSupportAllResponses in ConversionDialect so native FlattenOpenAIResponsesNamespaces
		// (exempting image_gen) and AliasOpenAIOAuthReservedToolNames ("python" function ->
		// "python__tokenrouter") compose cleanly without double-flattening.
		oauthSSE := strings.Join([]string{
			`event: response.created`,
			`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_oauth_ns","object":"response","created_at":1760000600,"status":"in_progress","model":"gpt-5.4","output":[]}}`,
			``,
			`event: response.output_item.added`,
			`data: {"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"fc_ns_ws","type":"function_call","status":"in_progress","call_id":"call_ns_ws","name":"nb__python","arguments":""}}`,
			``,
			`event: response.function_call_arguments.done`,
			`data: {"type":"response.function_call_arguments.done","sequence_number":2,"item_id":"fc_ns_ws","output_index":0,"arguments":"{\"code\":\"1+1\"}"}`,
			``,
			`event: response.output_item.done`,
			`data: {"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"fc_ns_ws","type":"function_call","status":"completed","call_id":"call_ns_ws","name":"nb__python","arguments":"{\"code\":\"1+1\"}"}}`,
			``,
			`event: response.output_item.added`,
			`data: {"type":"response.output_item.added","sequence_number":4,"output_index":1,"item":{"id":"fc_top_ws","type":"function_call","status":"in_progress","call_id":"call_top_ws","name":"python__tokenrouter","arguments":""}}`,
			``,
			`event: response.function_call_arguments.done`,
			`data: {"type":"response.function_call_arguments.done","sequence_number":5,"item_id":"fc_top_ws","output_index":1,"arguments":"{\"code\":\"2+2\"}"}`,
			``,
			`event: response.output_item.done`,
			`data: {"type":"response.output_item.done","sequence_number":6,"output_index":1,"item":{"id":"fc_top_ws","type":"function_call","status":"completed","call_id":"call_top_ws","name":"python__tokenrouter","arguments":"{\"code\":\"2+2\"}"}}`,
			``,
			`event: response.completed`,
			`data: {"type":"response.completed","sequence_number":7,"response":{"id":"resp_oauth_ns","object":"response","created_at":1760000600,"status":"completed","model":"gpt-5.4","output":[{"id":"fc_ns_ws","type":"function_call","status":"completed","call_id":"call_ns_ws","name":"nb__python","arguments":"{\"code\":\"1+1\"}"},{"id":"fc_top_ws","type":"function_call","status":"completed","call_id":"call_top_ws","name":"python__tokenrouter","arguments":"{\"code\":\"2+2\"}"}],"usage":{"input_tokens":20,"output_tokens":8,"total_tokens":28,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`,
			``,
			``,
		}, "\n")

		oauthBufResp := `{"id":"resp_oauth_ns_buf","object":"response","created_at":1760000601,"status":"completed","model":"gpt-5.4","output":[{"id":"fc_ns_b1","type":"function_call","status":"completed","call_id":"call_ns_b1","name":"nb__python","arguments":"{\"code\":\"1+1\"}"},{"id":"fc_top_b1","type":"function_call","status":"completed","call_id":"call_top_b1","name":"python__tokenrouter","arguments":"{\"code\":\"2+2\"}"}],"usage":{"input_tokens":18,"output_tokens":6,"total_tokens":24,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}`

		var mu sync.Mutex
		var captured [][]byte
		upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, rErr := io.ReadAll(r.Body)
			require.NoError(t, rErr)
			mu.Lock()
			captured = append(captured, raw)
			serveSSE := len(captured) == 1
			mu.Unlock()
			if serveSSE {
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, oauthSSE)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, oauthBufResp)
		}))
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{{
				ID:               601,
				Name:             "openai-oauth-flatten-ns",
				Platform:         capability.PlatformOpenAI,
				Type:             capability.ProviderTypeOAuth,
				BaseURL:          upSrv.URL,
				Models:           []string{"gpt-5.4"},
				ModelMapping:     map[string]string{"gpt-5": "gpt-5.4"},
				EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
				Credentials: map[string]any{
					"access_token":       "oauth-lab-token",
					"chatgpt_account_id": "acct-lab-601",
				},
				Extra: map[string]any{
					"openai_responses_flatten_namespaces":       true,
					"openai_oauth_responses_websockets_v2_mode": "off",
				},
			}},
			HostOverrides: map[string]string{"chatgpt.com": upSrv.URL},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		oauthReq := []byte(`{
			"model":"gpt-5",
			"stream":true,
			"instructions":"Coordinate namespace flattening and Codex alias reversal.",
			"tools":[
				{"type":"function","name":"python","parameters":{"type":"object","properties":{"code":{"type":"string"}}}},
				{"type":"namespace","name":"nb","tools":[
					{"type":"function","name":"python","parameters":{"type":"object","properties":{"code":{"type":"string"}}}}
				]},
				{"type":"namespace","name":"image_gen","tools":[
					{"type":"function","name":"render","parameters":{"type":"object"}}
				]}
			],
			"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"run both"}]}]
		}`)

		status, ct, respBytes := sendLabClientRequest(t, gwSrv, respClient, oauthReq)
		require.Equal(t, http.StatusOK, status, string(respBytes))
		require.Contains(t, strings.ToLower(ct), "text/event-stream")

		mu.Lock()
		require.Len(t, captured, 1)
		wireBody := captured[0]
		mu.Unlock()

		// Top-level "python" function is aliased for OAuth, "nb" namespace is flattened, and "image_gen" stays exempted.
		require.Contains(t, string(wireBody), `"python__tokenrouter"`)
		require.Contains(t, string(wireBody), `"name":"image_gen"`)

		frames, fErr := readAllSSEFrames(respBytes)
		require.NoError(t, fErr)
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{}))
		require.NotContains(t, string(respBytes), "__tokenrouter")
		require.Contains(t, string(respBytes), `"namespace":"nb"`)
		require.Contains(t, string(respBytes), `"name":"python"`)
		require.Empty(t, compareClientAndSettledUsage(protocol.ProtocolOpenAIResponses, frames, lab.UsageLogs()))

		// Also verify non-streaming (stream=false) OAuth namespace un-flattening and Codex alias reversal
		// (OpenAI OAuth streams SSE upstream and folds the completed response into buffered client JSON).
		oauthBufReq := bytes.Replace(oauthReq, []byte(`"stream":true`), []byte(`"stream":false`), 1)
		bufStatus, bufCT, bufBytes := sendLabClientRequest(t, gwSrv, respClient, oauthBufReq)
		require.Equal(t, http.StatusOK, bufStatus, string(bufBytes))
		require.Contains(t, strings.ToLower(bufCT), "application/json")
		require.NotContains(t, string(bufBytes), "__tokenrouter")
		require.Equal(t, "nb", gjson.GetBytes(bufBytes, "output.0.namespace").String())
		require.Equal(t, "python", gjson.GetBytes(bufBytes, "output.0.name").String())
		require.Equal(t, "python", gjson.GetBytes(bufBytes, "output.1.name").String())
	})

	t.Run("ws_http_bridge_warmup_continuation_reset_failover_and_hosted_events", func(t *testing.T) {
		wsProviders := stage5WSBridgeProviders()
		for _, prov := range wsProviders {
			t.Run(prov.kind, func(t *testing.T) {
				entry1 := runStage5WSWarmupContinuationCell(t, prov)
				require.Equal(t, labCellPass, entry1.Status,
					"ws warmup continuation failed on %s: rules=%v", prov.kind, entry1.Rules)

				entry2 := runStage5WSExplicitToolsResetCell(t, prov)
				require.Equal(t, labCellPass, entry2.Status,
					"ws explicit tools:[] reset failed on %s: rules=%v", prov.kind, entry2.Rules)

				entry3 := runStage5WSPrebyteFailoverIsolationCell(t, prov)
				require.Equal(t, labCellPass, entry3.Status,
					"ws pre-byte failover isolation failed on %s: rules=%v", prov.kind, entry3.Rules)

				entry4 := runStage5WSScopedCustomCell(t, prov)
				require.Equal(t, labCellPass, entry4.Status,
					"ws scoped custom failed on %s: rules=%v", prov.kind, entry4.Rules)

				entry5 := runStage5WSNativeCarrierCell(t, prov)
				require.Equal(t, labCellPass, entry5.Status,
					"ws native carrier replay failed on %s: rules=%v", prov.kind, entry5.Rules)
			})
		}
	})

	t.Run("client_tool_collision_returns_local_400_or_ws_1008_without_failover_or_health_penalty", func(t *testing.T) {
		for _, prov := range httpSpecs {
			combo := labRouteCombo{
				slug:     respClient.slug + "_via_" + prov.kind,
				client:   respClient,
				provider: prov,
			}
			t.Run("http_"+combo.slug, func(t *testing.T) {
				entry := runStage5HTTPToolCollisionCell(t, combo)
				require.Equal(t, labCellPass, entry.Status,
					"HTTP collision gate failed on %s: rules=%v", combo.slug, entry.Rules)
			})
		}
		for _, prov := range stage5WSBridgeProviders() {
			t.Run("ws_"+prov.kind, func(t *testing.T) {
				entry := runStage5WSToolCollisionCell(t, prov)
				require.Equal(t, labCellPass, entry.Status,
					"WS collision gate failed on %s: rules=%v", prov.kind, entry.Rules)
			})
		}
	})
}

// stage5WSBridgeProviders returns the three provider specifications exercised
// over the Responses WebSocket HTTP bridge (OpenAI API Key, Grok OAuth, and Grok API Key).
func stage5WSBridgeProviders() []labProviderKindSpec {
	grokSpecs := grokLabProviderSpecs()
	return []labProviderKindSpec{
		stage5OpenAIAPIKeyPassthroughLabSpec(),
		grokSpecs[0],
		grokSpecs[1],
	}
}

// runStage5ResponsesScenarioCell executes one canonical wire scenario over a
// function-only Responses HTTP route, substituting realistic lowered
// function_call upstream streams on tools_custom, tools_namespace, and
// tools_search. Other scenarios delegate directly to runLabScenarioCell, while
// client-tool scenarios return paired T5_STREAM and T5_USAGE cell results.
func runStage5ResponsesScenarioCell(
	t *testing.T,
	corpus *testkit.Corpus,
	combo labRouteCombo,
	scenario string,
) (labCellResult, labCellResult) {
	t.Helper()
	switch scenario {
	case "tools_custom", "tools_namespace", "tools_search":
	default:
		return runLabScenarioCell(t, corpus, combo, scenario)
	}

	baseStream := labCellResult{
		Client:   combo.client.protocol,
		Upstream: combo.provider.upstreamProtocol,
		Scenario: scenario,
		Law:      lawT5Stream,
	}
	baseUsage := labCellResult{
		Client:   combo.client.protocol,
		Upstream: combo.provider.upstreamProtocol,
		Scenario: scenario,
		Law:      lawT5Usage,
	}

	reqMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolOpenAIResponses,
		Kind:     testkit.FixtureRequest,
		Scenario: scenario,
	})
	require.NoError(t, err)
	require.NotEmpty(t, reqMatches)

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "stage5_" + scenario,
			Default: testkit.Reply{Raw: stage5LoweredUpstreamStreamForScenario(scenario, combo.provider.upstreamModel)},
		},
		Validation: combo.provider.validation,
	})
	upSrv := newLabMockServer(combo.provider, mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, combo.provider)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status, contentType, respBody := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var streamRules []string
	for _, v := range upSrv.Violations() {
		for _, rv := range v.RuleViolations {
			streamRules = appendUniqueLabRule(streamRules, rv.RuleID)
		}
		for range v.CheckErrors {
			streamRules = appendUniqueLabRule(streamRules, "mockupstream_check_error")
		}
	}

	isValidSSE200 := status == http.StatusOK && strings.Contains(strings.ToLower(contentType), "text/event-stream")
	var frames []sse.Frame
	if !isValidSSE200 {
		if len(streamRules) == 0 {
			streamRules = appendUniqueLabRule(streamRules, fmt.Sprintf("gateway_http_%d", status))
		}
	} else {
		parsedFrames, frameErr := readAllSSEFrames(respBody)
		frames = parsedFrames
		if frameErr != nil {
			streamRules = appendUniqueLabRule(streamRules, "client_sse_read_error")
		}
		for _, gv := range testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{}) {
			streamRules = appendUniqueLabRule(streamRules, gv.Rule)
		}
		if !stage5ClientStreamRestoredScenario(scenario, respBody) {
			streamRules = appendUniqueLabRule(streamRules, ruleClientToolNotRestored)
		}
	}

	if len(streamRules) > 0 {
		baseStream.Status = labCellFail
		baseStream.Rules = streamRules
	} else {
		baseStream.Status = labCellPass
	}

	if !isValidSSE200 {
		baseUsage.Status = labCellFail
		baseUsage.Rules = []string{ruleRequestRejectedBeforeStream}
		return baseStream, baseUsage
	}

	usageRules := compareClientAndSettledUsage(combo.client.protocol, frames, lab.UsageLogs())
	if len(usageRules) > 0 {
		baseUsage.Status = labCellFail
		baseUsage.Rules = usageRules
	} else {
		baseUsage.Status = labCellPass
	}
	return baseStream, baseUsage
}

// stage5LoweredUpstreamStreamForScenario returns the lowered function_call
// Responses SSE stream emitted by a function-only upstream for tools_custom,
// tools_namespace, or tools_search. Unrecognized scenario names panic
// immediately so callers cannot evaluate empty synthetic streams.
func stage5LoweredUpstreamStreamForScenario(scenario, model string) string {
	modelJSON, _ := json.Marshal(model)
	switch scenario {
	case "tools_custom":
		return strings.Join([]string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_tools_custom","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_tools_custom","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.output_item.added",
			`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"fc_patch_2","type":"function_call","status":"in_progress","call_id":"call_patch_2","name":"apply_patch","arguments":""}}`,
			"",
			"event: response.function_call_arguments.delta",
			`data: {"type":"response.function_call_arguments.delta","sequence_number":3,"item_id":"fc_patch_2","output_index":0,"delta":"{\"input\":\"*** Begin Patch\\n*** Update File: README.md\\n@@\\n"}`,
			"",
			"event: response.function_call_arguments.delta",
			`data: {"type":"response.function_call_arguments.delta","sequence_number":4,"item_id":"fc_patch_2","output_index":0,"delta":"-Default timeout: 5s\\n+Default timeout: 15s\\n*** End Patch\\n\"}"}`,
			"",
			"event: response.function_call_arguments.done",
			`data: {"type":"response.function_call_arguments.done","sequence_number":5,"item_id":"fc_patch_2","output_index":0,"arguments":"{\"input\":\"*** Begin Patch\\n*** Update File: README.md\\n@@\\n-Default timeout: 5s\\n+Default timeout: 15s\\n*** End Patch\\n\"}"}`,
			"",
			"event: response.output_item.done",
			`data: {"type":"response.output_item.done","sequence_number":6,"output_index":0,"item":{"id":"fc_patch_2","type":"function_call","status":"completed","call_id":"call_patch_2","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** Update File: README.md\\n@@\\n-Default timeout: 5s\\n+Default timeout: 15s\\n*** End Patch\\n\"}"}}`,
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":7,"response":{"id":"resp_tools_custom","object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":"fc_patch_2","type":"function_call","status":"completed","call_id":"call_patch_2","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** Update File: README.md\\n@@\\n-Default timeout: 5s\\n+Default timeout: 15s\\n*** End Patch\\n\"}"}],"usage":{"input_tokens":104,"output_tokens":42,"total_tokens":146,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON),
			"",
			"",
		}, "\n")

	case "tools_namespace":
		return strings.Join([]string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_tools_namespace","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_tools_namespace","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.output_item.added",
			`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"fc_ns_trace_2","type":"function_call","status":"in_progress","call_id":"call_ns_trace_2","name":"enterprise_observability_telemetry_connector__query_di__5375327d","arguments":""}}`,
			"",
			"event: response.function_call_arguments.delta",
			`data: {"type":"response.function_call_arguments.delta","sequence_number":3,"item_id":"fc_ns_trace_2","output_index":0,"delta":"{\"service\":\"checkout\","}`,
			"",
			"event: response.function_call_arguments.delta",
			`data: {"type":"response.function_call_arguments.delta","sequence_number":4,"item_id":"fc_ns_trace_2","output_index":0,"delta":"\"limit\":20}"}`,
			"",
			"event: response.function_call_arguments.done",
			`data: {"type":"response.function_call_arguments.done","sequence_number":5,"item_id":"fc_ns_trace_2","output_index":0,"arguments":"{\"service\":\"checkout\",\"limit\":20}"}`,
			"",
			"event: response.output_item.done",
			`data: {"type":"response.output_item.done","sequence_number":6,"output_index":0,"item":{"id":"fc_ns_trace_2","type":"function_call","status":"completed","call_id":"call_ns_trace_2","name":"enterprise_observability_telemetry_connector__query_di__5375327d","arguments":"{\"service\":\"checkout\",\"limit\":20}"}}`,
			"",
			"event: response.output_item.added",
			`data: {"type":"response.output_item.added","sequence_number":7,"output_index":1,"item":{"id":"fc_ns_spawn_2","type":"function_call","status":"in_progress","call_id":"call_ns_spawn_2","name":"collaboration__spawn_agent","arguments":""}}`,
			"",
			"event: response.function_call_arguments.delta",
			`data: {"type":"response.function_call_arguments.delta","sequence_number":8,"item_id":"fc_ns_spawn_2","output_index":1,"delta":"{\"task\":\"summarize slow spans\"}"}`,
			"",
			"event: response.function_call_arguments.done",
			`data: {"type":"response.function_call_arguments.done","sequence_number":9,"item_id":"fc_ns_spawn_2","output_index":1,"arguments":"{\"task\":\"summarize slow spans\"}"}`,
			"",
			"event: response.output_item.done",
			`data: {"type":"response.output_item.done","sequence_number":10,"output_index":1,"item":{"id":"fc_ns_spawn_2","type":"function_call","status":"completed","call_id":"call_ns_spawn_2","name":"collaboration__spawn_agent","arguments":"{\"task\":\"summarize slow spans\"}"}}`,
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":11,"response":{"id":"resp_tools_namespace","object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":"fc_ns_trace_2","type":"function_call","status":"completed","call_id":"call_ns_trace_2","name":"enterprise_observability_telemetry_connector__query_di__5375327d","arguments":"{\"service\":\"checkout\",\"limit\":20}"},{"id":"fc_ns_spawn_2","type":"function_call","status":"completed","call_id":"call_ns_spawn_2","name":"collaboration__spawn_agent","arguments":"{\"task\":\"summarize slow spans\"}"}],"usage":{"input_tokens":132,"output_tokens":38,"total_tokens":170,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON),
			"",
			"",
		}, "\n")

	case "tools_search":
		return strings.Join([]string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_tools_search","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_tools_search","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.output_item.added",
			`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"fc_slack_2","type":"function_call","status":"in_progress","call_id":"call_search_2","name":"tool_search","arguments":""}}`,
			"",
			"event: response.function_call_arguments.delta",
			`data: {"type":"response.function_call_arguments.delta","sequence_number":3,"item_id":"fc_slack_2","output_index":0,"delta":"{\"query\":\"slack notify\",\"limit\":3}"}`,
			"",
			"event: response.function_call_arguments.done",
			`data: {"type":"response.function_call_arguments.done","sequence_number":4,"item_id":"fc_slack_2","output_index":0,"arguments":"{\"query\":\"slack notify\",\"limit\":3}"}`,
			"",
			"event: response.output_item.done",
			`data: {"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":"fc_slack_2","type":"function_call","status":"completed","call_id":"call_search_2","name":"tool_search","arguments":"{\"query\":\"slack notify\",\"limit\":3}"}}`,
			"",
			"event: response.output_item.added",
			`data: {"type":"response.output_item.added","sequence_number":6,"output_index":1,"item":{"id":"fc_inc_2","type":"function_call","status":"in_progress","call_id":"call_inc_2","name":"pager__list_incidents","arguments":""}}`,
			"",
			"event: response.function_call_arguments.delta",
			`data: {"type":"response.function_call_arguments.delta","sequence_number":7,"item_id":"fc_inc_2","output_index":1,"delta":"{\"severity\":\"sev2\"}"}`,
			"",
			"event: response.function_call_arguments.done",
			`data: {"type":"response.function_call_arguments.done","sequence_number":8,"item_id":"fc_inc_2","output_index":1,"arguments":"{\"severity\":\"sev2\"}"}`,
			"",
			"event: response.output_item.done",
			`data: {"type":"response.output_item.done","sequence_number":9,"output_index":1,"item":{"id":"fc_inc_2","type":"function_call","status":"completed","call_id":"call_inc_2","name":"pager__list_incidents","arguments":"{\"severity\":\"sev2\"}"}}`,
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":10,"response":{"id":"resp_tools_search","object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":"fc_slack_2","type":"function_call","status":"completed","call_id":"call_search_2","name":"tool_search","arguments":"{\"query\":\"slack notify\",\"limit\":3}"},{"id":"fc_inc_2","type":"function_call","status":"completed","call_id":"call_inc_2","name":"pager__list_incidents","arguments":"{\"severity\":\"sev2\"}"}],"usage":{"input_tokens":148,"output_tokens":36,"total_tokens":184,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON),
			"",
			"",
		}, "\n")

	default:
		panic("unexpected scenario: " + scenario)
	}
}

// stage5ClientStreamRestoredScenario reports whether respBody contains the
// restored client tool items expected for tools_custom, tools_namespace, or
// tools_search. Scenarios outside those three client-tool cases return true so
// callers skip tool-specific marker checks.
func stage5ClientStreamRestoredScenario(scenario string, respBody []byte) bool {
	s := string(respBody)
	switch scenario {
	case "tools_custom":
		return strings.Contains(s, `"type":"custom_tool_call"`) &&
			strings.Contains(s, `"type":"response.custom_tool_call_input.done"`) &&
			strings.Contains(s, "Default timeout: 15s")
	case "tools_namespace":
		return strings.Contains(s, `"namespace":"enterprise_observability_telemetry_connector"`) &&
			strings.Contains(s, `"name":"query_distributed_trace_spans_by_service"`) &&
			strings.Contains(s, `"namespace":"collaboration"`) &&
			strings.Contains(s, `"name":"spawn_agent"`)
	case "tools_search":
		return strings.Contains(s, `"type":"tool_search_call"`) &&
			strings.Contains(s, `"namespace":"pager"`) &&
			strings.Contains(s, `"name":"list_incidents"`)
	default:
		return true
	}
}

// runStage5BufferedToolsCell exercises non-streaming (stream=false) lowering
// and buffered JSON response restoration across custom, tool_search,
// namespace function, and scoped namespace custom tools while preserving large
// 64-bit integer metadata. It returns labCellPass when all four output items,
// 64-bit integers, and settled UsageLog counts match expectations, or
// labCellFail with violated rule codes otherwise.
func runStage5BufferedToolsCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolOpenAIResponses,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5BufferedTools,
		Law:      lawT5ClientTools,
	}

	reqBody := []byte(`{
		"model":"gpt-5",
		"stream":false,
		"tools":[
			{"type":"custom","name":"apply_patch","description":"Patch files"},
			{"type":"tool_search","execution":"client"},
			{"type":"namespace","name":"nb","tools":[
				{"type":"function","name":"query","parameters":{"type":"object"}}
			]},
			{"type":"namespace","name":"code","tools":[
				{"type":"custom","name":"exec","description":"Run snippet"}
			]}
		],
		"input":[{"type":"message","role":"user","content":"run buffered tools"}]
	}`)

	upJSON := fmt.Sprintf(`{
		"id":"resp_s5_buf",
		"object":"response",
		"created_at":1760000100,
		"status":"completed",
		"model":%q,
		"x_trace_u64":9007199254740993111,
		"output":[
			{"id":"fc_b1","type":"function_call","status":"completed","call_id":"c_b1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"},
			{"id":"fc_b2","type":"function_call","status":"completed","call_id":"c_b2","name":"tool_search","arguments":"{\"query\":\"nb\"}"},
			{"id":"fc_b3","type":"function_call","status":"completed","call_id":"c_b3","name":"nb__query","arguments":"{}"},
			{"id":"fc_b4","type":"function_call","status":"completed","call_id":"c_b4","name":"code__exec","arguments":"{\"input\":\"pwd\"}"}
		],
		"usage":{"input_tokens":36,"output_tokens":14,"total_tokens":50,"input_tokens_details":{"cached_tokens":6},"output_tokens_details":{"reasoning_tokens":0}}
	}`, combo.provider.upstreamModel)

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "s5_buffered", Default: testkit.Reply{Raw: upJSON}},
		Validation: combo.provider.validation,
	})
	upSrv := newLabMockServer(combo.provider, mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, combo.provider)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	status, _, respBytes := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	for _, v := range upSrv.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
	}
	if status != http.StatusOK {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		if gjson.GetBytes(respBytes, "output.0.type").String() != "custom_tool_call" ||
			gjson.GetBytes(respBytes, "output.1.type").String() != "tool_search_call" ||
			gjson.GetBytes(respBytes, "output.2.namespace").String() != "nb" ||
			gjson.GetBytes(respBytes, "output.2.name").String() != "query" ||
			gjson.GetBytes(respBytes, "output.3.type").String() != "custom_tool_call" ||
			gjson.GetBytes(respBytes, "output.3.namespace").String() != "code" ||
			gjson.GetBytes(respBytes, "output.3.name").String() != "exec" ||
			gjson.GetBytes(respBytes, "output.3.input").String() != "pwd" ||
			gjson.GetBytes(respBytes, "x_trace_u64").Raw != "9007199254740993111" {
			rules = appendUniqueLabRule(rules, ruleClientToolNotRestored)
		}
		logs := lab.UsageLogs()
		if len(logs) != 1 || logs[0].InputTokens != 30 || logs[0].CacheReadTokens != 6 || logs[0].OutputTokens != 14 {
			rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
		}
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5ScopedCustomCell exercises a custom tool nested inside a namespace
// declaration, verifying both function-only lowering and streaming restoration
// of custom_tool_call with its original namespace and name. Upstream rule
// rejections, stream grammar errors, or missing namespace metadata return
// labCellFail with the offending rule codes.
func runStage5ScopedCustomCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolOpenAIResponses,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5ScopedCustom,
		Law:      lawT5ClientTools,
	}

	reqBody := []byte(`{
		"model":"gpt-5",
		"stream":true,
		"tools":[
			{"type":"namespace","name":"code","tools":[
				{"type":"custom","name":"exec","description":"Run raw code snippet"}
			]}
		],
		"input":[{"type":"message","role":"user","content":"run scoped custom"}]
	}`)
	modelJSON, _ := json.Marshal(combo.provider.upstreamModel)
	upSSE := strings.Join([]string{
		"event: response.created",
		fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_sc_1","object":"response","created_at":1760000110,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"fc_sc_1","type":"function_call","status":"in_progress","call_id":"call_sc_1","name":"code__exec","arguments":""}}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":2,"item_id":"fc_sc_1","output_index":0,"arguments":"{\"input\":\"print(42)\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"fc_sc_1","type":"function_call","status":"completed","call_id":"call_sc_1","name":"code__exec","arguments":"{\"input\":\"print(42)\"}"}}`,
		"",
		"event: response.completed",
		fmt.Sprintf(`data: {"type":"response.completed","sequence_number":4,"response":{"id":"resp_sc_1","object":"response","created_at":1760000110,"status":"completed","model":%s,"output":[{"id":"fc_sc_1","type":"function_call","status":"completed","call_id":"call_sc_1","name":"code__exec","arguments":"{\"input\":\"print(42)\"}"}],"usage":{"input_tokens":18,"output_tokens":7,"total_tokens":25,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON),
		"",
		"",
	}, "\n")

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "s5_scoped_custom", Default: testkit.Reply{Raw: upSSE}},
		Validation: combo.provider.validation,
	})
	upSrv := newLabMockServer(combo.provider, mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, combo.provider)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	status, _, respBytes := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	for _, v := range upSrv.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
	}
	if status != http.StatusOK {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		frames, fErr := readAllSSEFrames(respBytes)
		if fErr != nil {
			rules = appendUniqueLabRule(rules, "client_sse_read_error")
		}
		for _, gv := range testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{}) {
			rules = appendUniqueLabRule(rules, gv.Rule)
		}
		s := string(respBytes)
		if !strings.Contains(s, `"type":"custom_tool_call"`) ||
			!strings.Contains(s, `"namespace":"code"`) ||
			!strings.Contains(s, `"name":"exec"`) ||
			!strings.Contains(s, `"input":"print(42)"`) {
			rules = appendUniqueLabRule(rules, ruleClientToolNotRestored)
		}
		rules = append(rules, compareClientAndSettledUsage(protocol.ProtocolOpenAIResponses, frames, lab.UsageLogs())...)
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5OmittedArgsDoneCell exercises a compatible function-only Responses
// stream that emits function_call_arguments.delta followed directly by
// response.output_item.done without response.function_call_arguments.done. The
// returned entry passes only when downstream SSE grammar succeeds and both
// custom_tool_call_input.delta and custom_tool_call_input.done are synthesized.
func runStage5OmittedArgsDoneCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolOpenAIResponses,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5OmittedArgsDone,
		Law:      lawT5ClientTools,
	}

	reqBody := []byte(`{
		"model":"gpt-5",
		"stream":true,
		"tools":[{"type":"custom","name":"apply_patch","description":"Patch"}],
		"input":[{"type":"message","role":"user","content":"patch without args.done"}]
	}`)
	modelJSON, _ := json.Marshal(combo.provider.upstreamModel)
	upSSE := strings.Join([]string{
		"event: response.created",
		fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_omit_1","object":"response","created_at":1760000120,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"fc_om_1","type":"function_call","status":"in_progress","call_id":"call_om_1","name":"apply_patch","arguments":""}}`,
		"",
		"event: response.function_call_arguments.delta",
		`data: {"type":"response.function_call_arguments.delta","sequence_number":2,"item_id":"fc_om_1","output_index":0,"delta":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"fc_om_1","type":"function_call","status":"completed","call_id":"call_om_1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}}`,
		"",
		"event: response.completed",
		fmt.Sprintf(`data: {"type":"response.completed","sequence_number":4,"response":{"id":"resp_omit_1","object":"response","created_at":1760000120,"status":"completed","model":%s,"output":[{"id":"fc_om_1","type":"function_call","status":"completed","call_id":"call_om_1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}],"usage":{"input_tokens":16,"output_tokens":6,"total_tokens":22,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON),
		"",
		"",
	}, "\n")

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "s5_omitted_args_done", Default: testkit.Reply{Raw: upSSE}},
		Validation: combo.provider.validation,
	})
	upSrv := newLabMockServer(combo.provider, mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, combo.provider)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	status, _, respBytes := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	for _, v := range upSrv.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
	}
	if status != http.StatusOK {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		frames, fErr := readAllSSEFrames(respBytes)
		if fErr != nil {
			rules = appendUniqueLabRule(rules, "client_sse_read_error")
		}
		for _, gv := range testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{}) {
			rules = appendUniqueLabRule(rules, gv.Rule)
		}
		s := string(respBytes)
		if !strings.Contains(s, `"type":"response.custom_tool_call_input.delta"`) ||
			!strings.Contains(s, `"type":"response.custom_tool_call_input.done"`) {
			rules = appendUniqueLabRule(rules, ruleClientToolNotRestored)
		}
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5HostedIntermediateEventsCell verifies that partial_image,
// web_search_call.searching, and mcp_* intermediate SSE events pass through
// untouched alongside restored custom_tool_call and namespaced function_call
// items. Dropped intermediate frames, un-restored client tools, or UsageLog
// mismatches record labCellFail with the corresponding rule identifier.
func runStage5HostedIntermediateEventsCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolOpenAIResponses,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5HostedStreamEvents,
		Law:      lawT5ClientTools,
	}

	reqBody := []byte(`{
		"model":"gpt-5",
		"stream":true,
		"tools":[
			{"type":"custom","name":"apply_patch","description":"Patch"},
			{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}
		],
		"input":[{"type":"message","role":"user","content":"search, render preview, call mcp, and patch"}]
	}`)
	upSSE := buildStage5HostedAndClientToolSSE(combo.provider.upstreamModel)

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "s5_hosted_events", Default: testkit.Reply{Raw: upSSE}},
		Validation: combo.provider.validation,
	})
	upSrv := newLabMockServer(combo.provider, mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, combo.provider)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	status, _, respBytes := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	for _, v := range upSrv.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
	}
	if status != http.StatusOK {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		frames, fErr := readAllSSEFrames(respBytes)
		if fErr != nil {
			rules = appendUniqueLabRule(rules, "client_sse_read_error")
		}
		for _, gv := range testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{}) {
			rules = appendUniqueLabRule(rules, gv.Rule)
		}
		s := string(respBytes)
		wantPartialImg := fmt.Sprintf(`"partial_image_b64":%q`, testkit.SyntheticPNGDataBase64)
		if !strings.Contains(s, `"type":"response.image_generation_call.partial_image"`) ||
			!strings.Contains(s, wantPartialImg) ||
			!strings.Contains(s, `"type":"response.web_search_call.searching"`) ||
			!strings.Contains(s, `"type":"response.mcp_call_arguments.delta"`) ||
			!strings.Contains(s, `"type":"response.mcp_call.completed"`) {
			rules = appendUniqueLabRule(rules, ruleHostedStreamEventDropped)
		}
		if !strings.Contains(s, `"type":"custom_tool_call"`) ||
			!strings.Contains(s, `"namespace":"db"`) {
			rules = appendUniqueLabRule(rules, ruleClientToolNotRestored)
		}
		for _, ur := range compareClientAndSettledUsage(protocol.ProtocolOpenAIResponses, frames, lab.UsageLogs()) {
			rules = appendUniqueLabRule(rules, ur)
		}
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// buildStage5HostedAndClientToolSSE constructs a native Responses SSE stream
// interleaving partial_image, web_search_call.searching, and mcp_call events
// with lowered apply_patch and db__query function_call items.
func buildStage5HostedAndClientToolSSE(model string) string {
	modelJSON, _ := json.Marshal(model)
	png1x1 := testkit.SyntheticPNGDataBase64
	return strings.Join([]string{
		"event: response.created",
		fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_host_mix","object":"response","created_at":1760000130,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
		"",
		"event: response.in_progress",
		fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_host_mix","object":"response","created_at":1760000130,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"ws_h1","type":"web_search_call","status":"in_progress"}}`,
		"",
		"event: response.web_search_call.in_progress",
		`data: {"type":"response.web_search_call.in_progress","sequence_number":3,"output_index":0,"item_id":"ws_h1"}`,
		"",
		"event: response.web_search_call.searching",
		`data: {"type":"response.web_search_call.searching","sequence_number":4,"output_index":0,"item_id":"ws_h1"}`,
		"",
		"event: response.web_search_call.completed",
		`data: {"type":"response.web_search_call.completed","sequence_number":5,"output_index":0,"item_id":"ws_h1"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":6,"output_index":0,"item":{"id":"ws_h1","type":"web_search_call","status":"completed"}}`,
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":7,"output_index":1,"item":{"id":"ig_h1","type":"image_generation_call","status":"in_progress"}}`,
		"",
		"event: response.image_generation_call.partial_image",
		fmt.Sprintf(`data: {"type":"response.image_generation_call.partial_image","sequence_number":8,"output_index":1,"item_id":"ig_h1","partial_image_index":0,"partial_image_b64":%q}`, png1x1),
		"",
		"event: response.output_item.done",
		fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":9,"output_index":1,"item":{"id":"ig_h1","type":"image_generation_call","status":"completed","result":%q}}`, png1x1),
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":10,"output_index":2,"item":{"id":"mcp_h1","type":"mcp_call","status":"in_progress","server_label":"git","name":"log"}}`,
		"",
		"event: response.mcp_call_arguments.delta",
		`data: {"type":"response.mcp_call_arguments.delta","sequence_number":11,"output_index":2,"item_id":"mcp_h1","delta":"{\"n\":1}"}`,
		"",
		"event: response.mcp_call_arguments.done",
		`data: {"type":"response.mcp_call_arguments.done","sequence_number":12,"output_index":2,"item_id":"mcp_h1","arguments":"{\"n\":1}"}`,
		"",
		"event: response.mcp_call.completed",
		`data: {"type":"response.mcp_call.completed","sequence_number":13,"output_index":2,"item_id":"mcp_h1"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":14,"output_index":2,"item":{"id":"mcp_h1","type":"mcp_call","status":"completed","server_label":"git","name":"log","arguments":"{\"n\":1}","output":"ok"}}`,
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":15,"output_index":3,"item":{"id":"fc_hp","type":"function_call","status":"in_progress","call_id":"call_hp","name":"apply_patch","arguments":""}}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":16,"item_id":"fc_hp","output_index":3,"arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":17,"output_index":3,"item":{"id":"fc_hp","type":"function_call","status":"completed","call_id":"call_hp","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}}`,
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":18,"output_index":4,"item":{"id":"fc_hdb","type":"function_call","status":"in_progress","call_id":"call_hdb","name":"db__query","arguments":""}}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":19,"item_id":"fc_hdb","output_index":4,"arguments":"{\"sql\":\"select 1\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":20,"output_index":4,"item":{"id":"fc_hdb","type":"function_call","status":"completed","call_id":"call_hdb","name":"db__query","arguments":"{\"sql\":\"select 1\"}"}}`,
		"",
		"event: response.completed",
		fmt.Sprintf(`data: {"type":"response.completed","sequence_number":21,"response":{"id":"resp_host_mix","object":"response","created_at":1760000130,"status":"completed","model":%s,"output":[{"id":"ws_h1","type":"web_search_call","status":"completed"},{"id":"ig_h1","type":"image_generation_call","status":"completed","result":%q},{"id":"mcp_h1","type":"mcp_call","status":"completed","server_label":"git","name":"log","arguments":"{\"n\":1}","output":"ok"},{"id":"fc_hp","type":"function_call","status":"completed","call_id":"call_hp","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"},{"id":"fc_hdb","type":"function_call","status":"completed","call_id":"call_hdb","name":"db__query","arguments":"{\"sql\":\"select 1\"}"}],"usage":{"input_tokens":60,"output_tokens":25,"total_tokens":85,"input_tokens_details":{"cached_tokens":10},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON, png1x1),
		"",
		"",
	}, "\n")
}

// runStage5HTTPToolCollisionCell sends colliding custom/namespace/function tool
// declarations through a two-provider group and checks for a local HTTP 400
// invalid_request_error with param="tools". Any upstream HTTP call, provider
// failover, schedule health penalty, or settled UsageLog row marks the cell
// labCellFail.
func runStage5HTTPToolCollisionCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolOpenAIResponses,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5ToolCollision400,
		Law:      lawT5ClientTools,
	}

	// Combine a custom tool with colliding db__query and namespace db/query
	// declarations so flattening detects the collision during request lowering.
	conflictBody := []byte(`{
		"model":"gpt-5",
		"stream":false,
		"tools":[
			{"type":"custom","name":"apply_patch"},
			{"type":"function","name":"db__query","parameters":{"type":"object"}},
			{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}
		],
		"input":[{"type":"message","role":"user","content":"collide"}]
	}`)

	primaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "unused_primary", Default: testkit.Reply{Raw: `{}`}},
		Validation: combo.provider.validation,
	})
	secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "unused_secondary", Default: testkit.Reply{Raw: `{}`}},
		Validation: combo.provider.validation,
	})
	primarySrv := newLabMockServer(combo.provider, primaryH)
	defer primarySrv.Close()
	secondarySrv := newLabMockServer(combo.provider, secondaryH)
	defer secondarySrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeLabProviderSpec(1, 0, primarySrv.URL, combo.provider),
			makeLabProviderSpec(2, 1, secondarySrv.URL, combo.provider),
		},
		MaxProviderSwitches: 2,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	status, _, respBytes := sendLabClientRequest(t, gwSrv, combo.client, conflictBody)

	var rules []string
	if status != http.StatusBadRequest ||
		gjson.GetBytes(respBytes, "error.type").String() != "invalid_request_error" ||
		gjson.GetBytes(respBytes, "error.param").String() != "tools" {
		rules = appendUniqueLabRule(rules, ruleUnexpectedCollisionStatus)
	}
	if len(primarySrv.Requests()) > 0 || len(secondarySrv.Requests()) > 0 {
		rules = appendUniqueLabRule(rules, ruleCollisionTriggeredFailover)
	}
	if stage5ProviderWasPenalized(lab, 1) || stage5ProviderWasPenalized(lab, 2) {
		rules = appendUniqueLabRule(rules, ruleCollisionPenalizedHealth)
	}
	if len(lab.UsageLogs()) > 0 {
		rules = appendUniqueLabRule(rules, ruleDuplicateUsageLogs)
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5WSWarmupContinuationCell exercises a three-turn Responses WebSocket
// HTTP bridge session: Turn 1 local generate=false warmup with custom,
// tool_search, namespace, and additional_tools; Turn 2 omitting tools and model
// while streaming hosted intermediate events and lowered custom + namespace
// calls; Turn 3 replaying custom_tool_call_output and function_call_output while
// omitting tools and model. Duplicate outbound tool names, un-restored calls,
// or UsageLog drift mark the returned cell labCellFail.
func runStage5WSWarmupContinuationCell(t *testing.T, prov labProviderKindSpec) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolResponsesWebSocket,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5WSWarmupContinuation,
		Law:      lawT5WSBridge,
	}

	modelJSON, _ := json.Marshal(prov.upstreamModel)
	turn2SSE := strings.Join([]string{
		"event: response.created",
		fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_ws_t2","object":"response","created_at":1760000200,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"ws_s1","type":"web_search_call","status":"in_progress"}}`,
		"",
		"event: response.web_search_call.searching",
		`data: {"type":"response.web_search_call.searching","sequence_number":2,"output_index":0,"item_id":"ws_s1"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"ws_s1","type":"web_search_call","status":"completed"}}`,
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":4,"output_index":1,"item":{"id":"fc_wp","type":"function_call","status":"in_progress","call_id":"call_wp","name":"apply_patch","arguments":""}}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":5,"item_id":"fc_wp","output_index":1,"arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":6,"output_index":1,"item":{"id":"fc_wp","type":"function_call","status":"completed","call_id":"call_wp","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}}`,
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":7,"output_index":2,"item":{"id":"fc_wdb","type":"function_call","status":"in_progress","call_id":"call_wdb","name":"db__query","arguments":""}}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":8,"item_id":"fc_wdb","output_index":2,"arguments":"{\"sql\":\"select 1\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":9,"output_index":2,"item":{"id":"fc_wdb","type":"function_call","status":"completed","call_id":"call_wdb","name":"db__query","arguments":"{\"sql\":\"select 1\"}"}}`,
		"",
		"event: response.completed",
		fmt.Sprintf(`data: {"type":"response.completed","sequence_number":10,"response":{"id":"resp_ws_t2","object":"response","created_at":1760000200,"status":"completed","model":%s,"output":[{"id":"ws_s1","type":"web_search_call","status":"completed"},{"id":"fc_wp","type":"function_call","status":"completed","call_id":"call_wp","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"},{"id":"fc_wdb","type":"function_call","status":"completed","call_id":"call_wdb","name":"db__query","arguments":"{\"sql\":\"select 1\"}"}],"usage":{"input_tokens":40,"output_tokens":15,"total_tokens":55,"input_tokens_details":{"cached_tokens":10},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON),
		"",
		"",
	}, "\n")
	turn3SSE := buildUpstreamTextSSE(protocol.ProtocolOpenAIResponses, prov.upstreamModel, "ws_t3", "WS continuation completed")

	zeroResults := 0
	twoResults := 2
	hasTools := true
	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "ws_warmup_cont",
			Turns: []testkit.Turn{
				{
					Match: testkit.Match{ToolResults: &zeroResults, HasTools: &hasTools},
					Reply: testkit.Reply{Raw: turn2SSE},
				},
				{
					Match: testkit.Match{ToolResults: &twoResults, HasTools: &hasTools},
					Reply: testkit.Reply{Raw: turn3SSE},
				},
			},
		},
		Validation: prov.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, prov)},
		AllowedProtocols: []protocol.ProtocolID{
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolOpenAIResponses,
		},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	conn, dialCancel := dialStage5LabWS(t, gwSrv)
	defer dialCancel()
	defer func() { _ = conn.CloseNow() }()

	// Turn 1: generate=false local warmup with custom, tool_search, namespace, and additional_tools.
	warmFrame := []byte(`{
		"type":"response.create",
		"model":"gpt-5",
		"generate":false,
		"tools":[
			{"type":"custom","name":"apply_patch","description":"Patch"},
			{"type":"tool_search","execution":"client"},
			{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}
		],
		"input":[
			{"type":"additional_tools","tools":[
				{"type":"namespace","name":"collab","tools":[{"type":"function","name":"ping","parameters":{"type":"object"}}]}
			]},
			{"type":"message","role":"developer","content":"Warmup instructions"}
		]
	}`)
	warmEvents, warmErr := sendAndReadStage5WSTurn(t.Context(), conn, warmFrame)
	var rules []string
	if warmErr != nil || len(mockH.Requests()) != 0 || len(lab.UsageLogs()) != 0 {
		rules = appendUniqueLabRule(rules, "ws_warmup_unexpected_side_effect")
	}
	warmRespID := extractStage5WSCompletedID(warmEvents)
	if warmRespID == "" {
		rules = appendUniqueLabRule(rules, "ws_warmup_missing_response_id")
		entry.Status = labCellFail
		entry.Rules = rules
		return entry
	}

	// Turn 2: omit BOTH tools and model, referencing previous_response_id from warmup.
	turn2Frame := []byte(fmt.Sprintf(`{
		"type":"response.create",
		"previous_response_id":%q,
		"input":[{"type":"message","role":"user","content":"Execute patch and db query"}]
	}`, warmRespID))
	turn2Events, t2Err := sendAndReadStage5WSTurn(t.Context(), conn, turn2Frame)
	if t2Err != nil {
		rules = appendUniqueLabRule(rules, "ws_turn2_read_error")
	}
	t2Joined := string(bytes.Join(turn2Events, []byte("\n")))
	if !strings.Contains(t2Joined, `"type":"response.web_search_call.searching"`) {
		rules = appendUniqueLabRule(rules, ruleHostedStreamEventDropped)
	}
	if !strings.Contains(t2Joined, `"type":"custom_tool_call"`) ||
		!strings.Contains(t2Joined, `"namespace":"db"`) {
		rules = appendUniqueLabRule(rules, ruleClientToolNotRestored)
	}
	turn2RespID := extractStage5WSCompletedID(turn2Events)

	// Turn 3: send custom_tool_call_output and function_call_output while omitting tools and model.
	turn3Frame := []byte(fmt.Sprintf(`{
		"type":"response.create",
		"previous_response_id":%q,
		"input":[
			{"type":"custom_tool_call_output","call_id":"call_wp","output":"patched"},
			{"type":"function_call_output","call_id":"call_wdb","output":"{\"rows\":1}"}
		]
	}`, turn2RespID))
	turn3Events, t3Err := sendAndReadStage5WSTurn(t.Context(), conn, turn3Frame)
	if t3Err != nil || !strings.Contains(string(bytes.Join(turn3Events, []byte("\n"))), "WS continuation completed") {
		rules = appendUniqueLabRule(rules, "ws_turn3_completion_failed")
	}
	_ = conn.Close(websocket.StatusNormalClosure, "done")

	for _, v := range mockH.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
	}
	// Verify Turn 2 and Turn 3 outbound requests contain no duplicate lowered tool names
	// across all evaluated client-tool runs.
	for _, rec := range mockH.Requests() {
		seenNames := make(map[string]bool)
		for _, tool := range gjson.GetBytes(rec.Body, "tools").Array() {
			name := strings.TrimSpace(tool.Get("name").String())
			if name != "" && seenNames[name] {
				rules = appendUniqueLabRule(rules, ruleWSStaleToolsLeaked)
			}
			seenNames[name] = true
		}
	}
	logs := lab.UsageLogs()
	if len(logs) != 2 {
		rules = appendUniqueLabRule(rules, ruleMissingUsageLog)
	} else if logs[0].InputTokens != 30 || logs[0].CacheReadTokens != 10 || logs[0].OutputTokens != 15 {
		rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5WSExplicitToolsResetCell verifies that sending tools:[] on a later
// WebSocket turn clears inherited LoweredTools so subsequent omitted-tools
// turns do not leak stale lowered declarations. Any reappearance of apply_patch
// or unexpected upstream turn count records labCellFail.
func runStage5WSExplicitToolsResetCell(t *testing.T, prov labProviderKindSpec) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolResponsesWebSocket,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5WSExplicitToolsReset,
		Law:      lawT5WSBridge,
	}

	replySSE := buildUpstreamTextSSE(protocol.ProtocolOpenAIResponses, prov.upstreamModel, "ws_rst", "reset-ok")
	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "ws_reset", Default: testkit.Reply{Raw: replySSE}},
		Validation: prov.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, prov)},
		AllowedProtocols: []protocol.ProtocolID{
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolOpenAIResponses,
		},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	conn, cancel := dialStage5LabWS(t, gwSrv)
	defer cancel()
	defer func() { _ = conn.CloseNow() }()

	// Turn 1 warmup registers apply_patch.
	warmEvents, err := sendAndReadStage5WSTurn(t.Context(), conn, []byte(`{
		"type":"response.create",
		"model":"gpt-5",
		"generate":false,
		"tools":[{"type":"custom","name":"apply_patch"}],
		"input":[{"type":"message","role":"user","content":"warm"}]
	}`))
	var rules []string
	if err != nil {
		rules = appendUniqueLabRule(rules, "ws_warmup_failed")
	}
	warmID := extractStage5WSCompletedID(warmEvents)

	// Turn 2 explicitly resets tools:[].
	t2Events, err := sendAndReadStage5WSTurn(t.Context(), conn, []byte(fmt.Sprintf(`{
		"type":"response.create",
		"previous_response_id":%q,
		"tools":[],
		"input":[{"type":"message","role":"user","content":"reset tools"}]
	}`, warmID)))
	if err != nil {
		rules = appendUniqueLabRule(rules, "ws_reset_turn_failed")
	}
	t2ID := extractStage5WSCompletedID(t2Events)

	// Turn 3 omits tools; apply_patch must NOT reappear on upstream wire.
	_, err = sendAndReadStage5WSTurn(t.Context(), conn, []byte(fmt.Sprintf(`{
		"type":"response.create",
		"previous_response_id":%q,
		"input":[{"type":"message","role":"user","content":"after reset"}]
	}`, t2ID)))
	if err != nil {
		rules = appendUniqueLabRule(rules, "ws_post_reset_turn_failed")
	}
	_ = conn.Close(websocket.StatusNormalClosure, "done")

	for _, v := range mockH.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
	}
	reqs := mockH.Requests()
	if len(reqs) != 2 {
		rules = appendUniqueLabRule(rules, "ws_unexpected_upstream_count")
	} else {
		for _, rec := range reqs {
			for _, tool := range gjson.GetBytes(rec.Body, "tools").Array() {
				if tool.Get("name").String() == "apply_patch" {
					rules = appendUniqueLabRule(rules, ruleWSStaleToolsLeaked)
				}
			}
		}
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5WSPrebyteFailoverIsolationCell exercises pre-output provider
// failover on the WebSocket HTTP bridge across both Turn 1 additional_tools
// requests (preventing duplicate tool promotion on Provider 2) and Turn 2
// omitted-tools HTTP 429 failovers (preserving Turn 1's inherited ToolMap).
// Leaked primary error text, duplicated declarations, or misattributed UsageLog
// entries mark the returned cell failed.
func runStage5WSPrebyteFailoverIsolationCell(t *testing.T, prov labProviderKindSpec) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolResponsesWebSocket,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5WSPrebyteFailover,
		Law:      lawT5WSBridge,
	}

	modelJSON, _ := json.Marshal(prov.upstreamModel)
	patchSSE := strings.Join([]string{
		"event: response.created",
		fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_ws_fo","object":"response","created_at":1760000220,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"fc_fo","type":"function_call","status":"in_progress","call_id":"call_fo","name":"apply_patch","arguments":""}}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":2,"item_id":"fc_fo","output_index":0,"arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"fc_fo","type":"function_call","status":"completed","call_id":"call_fo","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}}`,
		"",
		"event: response.completed",
		fmt.Sprintf(`data: {"type":"response.completed","sequence_number":4,"response":{"id":"resp_ws_fo","object":"response","created_at":1760000220,"status":"completed","model":%s,"output":[{"id":"fc_fo","type":"function_call","status":"completed","call_id":"call_fo","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\\n*** End Patch\"}"}],"usage":{"input_tokens":22,"output_tokens":8,"total_tokens":30,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON),
		"",
		"",
	}, "\n")

	primaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "ws_fo_primary",
			Default: testkit.Reply{Raw: patchSSE},
			Turns: []testkit.Turn{{
				Fault: &testkit.Fault{
					Status:    http.StatusTooManyRequests,
					ErrorBody: formatUpstreamJSONError(protocol.ProtocolOpenAIResponses, http.StatusTooManyRequests, "ws-primary-429-sentinel"),
				},
			}},
		},
		Validation: prov.validation,
	})
	secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "ws_fo_secondary", Default: testkit.Reply{Raw: patchSSE}},
		Validation: prov.validation,
	})
	primarySrv := httptest.NewServer(primaryH)
	defer primarySrv.Close()
	secondarySrv := httptest.NewServer(secondaryH)
	defer secondarySrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeLabProviderSpec(1, 0, primarySrv.URL, prov),
			makeLabProviderSpec(2, 1, secondarySrv.URL, prov),
		},
		AllowedProtocols: []protocol.ProtocolID{
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolOpenAIResponses,
		},
		MaxProviderSwitches: 2,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	var rules []string

	// Subcase A: Turn 1 with additional_tools hits 429 on Provider 1 and fails over to Provider 2
	// without duplicating promoted additional_tools declarations.
	connA, cancelA := dialStage5LabWS(t, gwSrv)
	eventsA, errA := sendAndReadStage5WSTurn(t.Context(), connA, []byte(`{
		"type":"response.create",
		"model":"gpt-5",
		"input":[
			{"type":"additional_tools","tools":[{"type":"custom","name":"apply_patch"}]},
			{"type":"message","role":"user","content":"failover turn 1"}
		]
	}`))
	_ = connA.Close(websocket.StatusNormalClosure, "done")
	cancelA()

	if errA != nil {
		rules = appendUniqueLabRule(rules, "ws_failover_turn1_error")
	}
	joinedA := string(bytes.Join(eventsA, []byte("\n")))
	if strings.Contains(joinedA, "ws-primary-429-sentinel") {
		rules = appendUniqueLabRule(rules, "primary_content_leaked")
	}
	if !strings.Contains(joinedA, `"type":"custom_tool_call"`) {
		rules = appendUniqueLabRule(rules, ruleClientToolNotRestored)
	}
	if len(secondaryH.Requests()) != 1 {
		rules = appendUniqueLabRule(rules, "fallback_not_selected")
	} else {
		applyPatchCount := 0
		for _, tool := range gjson.GetBytes(secondaryH.Requests()[0].Body, "tools").Array() {
			if tool.Get("name").String() == "apply_patch" {
				applyPatchCount++
			}
		}
		if applyPatchCount != 1 {
			rules = appendUniqueLabRule(rules, ruleWSStaleToolsLeaked)
		}
	}

	// Subcase B: Turn 1 local warmup on Provider 1, then Turn 2 omits tools and hits 429 on
	// Provider 1 before failing over to Provider 2 while preserving Turn 1's ToolMap.
	primaryB := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "ws_fo_primary_t2",
			Turns: []testkit.Turn{{
				Fault: &testkit.Fault{
					Status:    http.StatusTooManyRequests,
					ErrorBody: formatUpstreamJSONError(protocol.ProtocolOpenAIResponses, http.StatusTooManyRequests, "ws-turn2-429-sentinel"),
				},
			}},
		},
		Validation: prov.validation,
	})
	secondaryB := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "ws_fo_secondary_t2", Default: testkit.Reply{Raw: patchSSE}},
		Validation: prov.validation,
	})
	pSrvB := httptest.NewServer(primaryB)
	defer pSrvB.Close()
	sSrvB := httptest.NewServer(secondaryB)
	defer sSrvB.Close()

	lab.SetProviders(t, []labProvider{
		makeLabProviderSpec(11, 0, pSrvB.URL, prov),
		makeLabProviderSpec(12, 1, sSrvB.URL, prov),
	})
	lab.ResetUsageLogs()

	connB, cancelB := dialStage5LabWS(t, gwSrv)
	warmEventsB, wErrB := sendAndReadStage5WSTurn(t.Context(), connB, []byte(`{
		"type":"response.create",
		"model":"gpt-5",
		"generate":false,
		"tools":[{"type":"custom","name":"apply_patch"}],
		"input":[{"type":"message","role":"user","content":"warmup before 429"}]
	}`))
	if wErrB != nil {
		rules = appendUniqueLabRule(rules, "ws_warmup_b_error")
	}
	warmIDB := extractStage5WSCompletedID(warmEventsB)
	eventsB, errB := sendAndReadStage5WSTurn(t.Context(), connB, []byte(fmt.Sprintf(`{
		"type":"response.create",
		"previous_response_id":%q,
		"input":[{"type":"message","role":"user","content":"turn 2 failover"}]
	}`, warmIDB)))
	_ = connB.Close(websocket.StatusNormalClosure, "done")
	cancelB()

	if errB != nil {
		rules = appendUniqueLabRule(rules, "ws_failover_turn2_error")
	}
	joinedB := string(bytes.Join(eventsB, []byte("\n")))
	if !strings.Contains(joinedB, `"type":"custom_tool_call"`) {
		rules = appendUniqueLabRule(rules, ruleClientToolNotRestored)
	}
	for _, h := range []*mockupstream.Handler{primaryH, secondaryH, primaryB, secondaryB} {
		for _, v := range h.Violations() {
			for _, rv := range v.RuleViolations {
				rules = appendUniqueLabRule(rules, rv.RuleID)
			}
		}
	}
	if logs := lab.UsageLogs(); len(logs) != 1 || logs[0].ProviderID != 12 {
		rules = appendUniqueLabRule(rules, "failover_usage_attribution_mismatch")
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5WSScopedCustomCell exercises a custom child tool nested inside a
// namespace declaration over the Responses WebSocket HTTP bridge. It returns
// labCellPass only when upstream function-only validation succeeds and client
// frames restore custom_tool_call with namespace="code" and name="exec".
func runStage5WSScopedCustomCell(t *testing.T, prov labProviderKindSpec) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolResponsesWebSocket,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5WSScopedCustom,
		Law:      lawT5WSBridge,
	}

	modelJSON, _ := json.Marshal(prov.upstreamModel)
	upSSE := strings.Join([]string{
		"event: response.created",
		fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_ws_sc","object":"response","created_at":1760000230,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"fc_wsc","type":"function_call","status":"in_progress","call_id":"call_wsc","name":"code__exec","arguments":""}}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":2,"item_id":"fc_wsc","output_index":0,"arguments":"{\"input\":\"ls -la\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"fc_wsc","type":"function_call","status":"completed","call_id":"call_wsc","name":"code__exec","arguments":"{\"input\":\"ls -la\"}"}}`,
		"",
		"event: response.completed",
		fmt.Sprintf(`data: {"type":"response.completed","sequence_number":4,"response":{"id":"resp_ws_sc","object":"response","created_at":1760000230,"status":"completed","model":%s,"output":[{"id":"fc_wsc","type":"function_call","status":"completed","call_id":"call_wsc","name":"code__exec","arguments":"{\"input\":\"ls -la\"}"}],"usage":{"input_tokens":14,"output_tokens":5,"total_tokens":19,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON),
		"",
		"",
	}, "\n")

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "ws_scoped_custom", Default: testkit.Reply{Raw: upSSE}},
		Validation: prov.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, prov)},
		AllowedProtocols: []protocol.ProtocolID{
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolOpenAIResponses,
		},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	conn, cancel := dialStage5LabWS(t, gwSrv)
	defer cancel()
	events, err := sendAndReadStage5WSTurn(t.Context(), conn, []byte(`{
		"type":"response.create",
		"model":"gpt-5",
		"tools":[{"type":"namespace","name":"code","tools":[{"type":"custom","name":"exec"}]}],
		"input":[{"type":"message","role":"user","content":"scoped ws custom"}]
	}`))
	_ = conn.Close(websocket.StatusNormalClosure, "done")

	var rules []string
	if err != nil {
		rules = appendUniqueLabRule(rules, "ws_read_error")
	}
	for _, v := range mockH.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
	}
	joined := string(bytes.Join(events, []byte("\n")))
	if !strings.Contains(joined, `"type":"custom_tool_call"`) ||
		!strings.Contains(joined, `"namespace":"code"`) ||
		!strings.Contains(joined, `"name":"exec"`) {
		rules = appendUniqueLabRule(rules, ruleClientToolNotRestored)
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5WSNativeCarrierCell verifies PrepareNativeReplay coexisting with
// client-tool lowering on the Responses WebSocket HTTP bridge. Matching
// same-account trc1: carriers must unpack to raw ciphertext while foreign
// carriers strip cleanly and user-text trc1: literals stay untouched.
func runStage5WSNativeCarrierCell(t *testing.T, prov labProviderKindSpec) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolResponsesWebSocket,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5WSNativeCarrier,
		Law:      lawT5WSBridge,
	}

	sameScheme := ir.SchemeOpenAI
	wantUnpacked := "gAAAA_ws_same_101"
	if prov.platform == capability.PlatformGrok {
		sameScheme = ir.SchemeXAI
		wantUnpacked = "xai_ws_same_101"
	}
	sameCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     sameScheme,
		Data:       wantUnpacked,
		ProviderID: 101,
	})
	require.NoError(t, err)
	foreignCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "EqQBForeignWS",
		Text:   "Foreign WS thought.",
	})
	require.NoError(t, err)

	replySSE := buildUpstreamTextSSE(protocol.ProtocolOpenAIResponses, prov.upstreamModel, "ws_car", "carrier-ok")
	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "ws_carrier", Default: testkit.Reply{Raw: replySSE}},
		Validation: prov.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, prov)},
		AllowedProtocols: []protocol.ProtocolID{
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolOpenAIResponses,
		},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	conn, cancel := dialStage5LabWS(t, gwSrv)
	defer cancel()
	frame := []byte(fmt.Sprintf(`{
		"type":"response.create",
		"model":"gpt-5",
		"store":false,
		"tools":[{"type":"custom","name":"apply_patch"}],
		"input":[
			{"type":"message","role":"user","content":"keep literal trc1:plain:demo"},
			{"type":"reasoning","id":"rs_same","summary":[{"type":"summary_text","text":"s"}],"encrypted_content":%q},
			{"type":"reasoning","id":"rs_foreign","summary":[],"encrypted_content":%q},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}
		]
	}`, sameCarrier, foreignCarrier))
	_, rErr := sendAndReadStage5WSTurn(t.Context(), conn, frame)
	_ = conn.Close(websocket.StatusNormalClosure, "done")

	var rules []string
	if rErr != nil {
		rules = appendUniqueLabRule(rules, "ws_read_error")
	}
	for _, v := range mockH.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
	}
	for _, rec := range mockH.Requests() {
		if hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, rec.Body) {
			rules = appendUniqueLabRule(rules, ruleUnstrippedTRC1Carrier)
		} else if gjson.GetBytes(rec.Body, "input.1.encrypted_content").String() != wantUnpacked {
			rules = appendUniqueLabRule(rules, ruleUnstrippedTRC1Carrier)
		}
		require.Contains(t, string(rec.Body), "trc1:plain:demo")
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage5WSToolCollisionCell sends colliding tool names over the Responses
// WebSocket HTTP bridge and verifies WebSocket close code 1008 with zero
// upstream requests, zero failover, and zero schedule health penalties. Any
// non-1008 close status, upstream request, or health mutation marks the cell failed.
func runStage5WSToolCollisionCell(t *testing.T, prov labProviderKindSpec) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   protocol.ProtocolResponsesWebSocket,
		Upstream: protocol.ProtocolOpenAIResponses,
		Scenario: scenarioStage5WSToolCollision1008,
		Law:      lawT5WSBridge,
	}

	primaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "ws_unused_primary", Default: testkit.Reply{Raw: ""}},
		Validation: prov.validation,
	})
	secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     testkit.Script{Name: "ws_unused_secondary", Default: testkit.Reply{Raw: ""}},
		Validation: prov.validation,
	})
	primarySrv := httptest.NewServer(primaryH)
	defer primarySrv.Close()
	secondarySrv := httptest.NewServer(secondaryH)
	defer secondarySrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeLabProviderSpec(1, 0, primarySrv.URL, prov),
			makeLabProviderSpec(2, 1, secondarySrv.URL, prov),
		},
		AllowedProtocols: []protocol.ProtocolID{
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolOpenAIResponses,
		},
		MaxProviderSwitches: 2,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	conn, cancel := dialStage5LabWS(t, gwSrv)
	defer cancel()
	defer func() { _ = conn.CloseNow() }()

	readCtx, readCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer readCancel()
	writeErr := conn.Write(readCtx, websocket.MessageText, []byte(`{
		"type":"response.create",
		"model":"gpt-5",
		"tools":[
			{"type":"function","name":"db__query","parameters":{"type":"object"}},
			{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}
		],
		"input":[{"type":"message","role":"user","content":"ws collide"}]
	}`))
	require.NoError(t, writeErr)

	var closeCode websocket.StatusCode
	for range 4 {
		_, _, rErr := conn.Read(readCtx)
		if rErr != nil {
			closeCode = websocket.CloseStatus(rErr)
			break
		}
	}

	var rules []string
	if closeCode != websocket.StatusPolicyViolation {
		rules = appendUniqueLabRule(rules, ruleUnexpectedCollisionStatus)
	}
	if len(primaryH.Requests()) > 0 || len(secondaryH.Requests()) > 0 {
		rules = appendUniqueLabRule(rules, ruleCollisionTriggeredFailover)
	}
	if stage5ProviderWasPenalized(lab, 1) || stage5ProviderWasPenalized(lab, 2) {
		rules = appendUniqueLabRule(rules, ruleCollisionPenalizedHealth)
	}
	if len(lab.UsageLogs()) > 0 {
		rules = appendUniqueLabRule(rules, ruleDuplicateUsageLogs)
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// stage5ProviderWasPenalized reports whether provider id on lab recorded any
// error message, status degradation, rate-limit window, overload cooldown, or
// temporary unschedulable penalty. Missing provider IDs return false without
// failing the caller.
func stage5ProviderWasPenalized(lab *protocolLabRouter, id int64) bool {
	rec, ok := lab.ProviderRecord(id)
	if !ok {
		return false
	}
	return rec.Status != billing.StatusActive ||
		!rec.Schedulable ||
		strings.TrimSpace(rec.ErrorMessage) != "" ||
		rec.TempUnschedulableUntil != nil ||
		rec.RateLimitResetAt != nil ||
		rec.OverloadUntil != nil
}

// dialStage5LabWS upgrades a Responses WebSocket client connection to gwSrv's
// /v1/responses endpoint with synthetic lab bearer credentials under a 10-second
// dial timeout. Callers must invoke the returned CancelFunc and close the
// connection when the turn exchange finishes.
func dialStage5LabWS(t *testing.T, gwSrv *httptest.Server) (*websocket.Conn, context.CancelFunc) {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(gwSrv.URL, "http") + "/v1/responses"
	dialCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	conn, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer lab-api-key"}},
	})
	require.NoError(t, err)
	return conn, cancel
}

// sendAndReadStage5WSTurn writes frame over conn under a 10-second turn budget
// and collects server JSON text frames until a terminal response.completed,
// response.failed, response.incomplete, or error event arrives. Transport read
// failures or standalone error frames return a non-nil error alongside all
// frames received up to that point.
func sendAndReadStage5WSTurn(parentCtx context.Context, conn *websocket.Conn, frame []byte) ([][]byte, error) {
	ctx, cancel := context.WithTimeout(parentCtx, 10*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, frame); err != nil {
		return nil, err
	}
	var events [][]byte
	for {
		_, msg, err := conn.Read(ctx)
		if err != nil {
			return events, err
		}
		events = append(events, bytes.Clone(msg))
		switch gjson.GetBytes(msg, "type").String() {
		case "response.completed", "response.failed", "response.incomplete":
			return events, nil
		case "error":
			return events, errors.New("received websocket error frame: " + string(msg))
		}
	}
}

// extractStage5WSCompletedID scans events in reverse order and returns the
// trimmed response.id from the latest terminal response.completed frame. Slices
// without a completed frame or with a blank identifier return an empty string.
func extractStage5WSCompletedID(events [][]byte) string {
	for _, ev := range slices.Backward(events) {
		if gjson.GetBytes(ev, "type").String() == "response.completed" {
			return strings.TrimSpace(gjson.GetBytes(ev, "response.id").String())
		}
	}
	return ""
}

// isStage5NativeNonConvertedBehavior reports whether entry is one of the known
// native same-protocol HTTP SSE framing or non-OpenAI error-masking behaviors
// on requests without lowered client tools. WebSocket bridge entries and any
// client-tool adaptation failures return false so unresolved defects fail immediately.
func isStage5NativeNonConvertedBehavior(providerKind string, entry labCellResult) bool {
	if entry.Client != protocol.ProtocolOpenAIResponses || entry.Upstream != protocol.ProtocolOpenAIResponses {
		return false
	}
	switch entry.Scenario {
	case "stream_truncated", "stream_error_event", "upstream_sse_quirks":
		return entry.Law == lawT5Stream
	case scenarioDisconnectAfterData:
		return entry.Law == lawT5MidstreamTruncate
	case scenarioUpstreamHTTPError:
		return entry.Law == lawT5ErrorFormat &&
			providerKind != "openai_apikey_passthrough" &&
			len(entry.Rules) == 1 &&
			entry.Rules[0] == "unexpected_error_status_502"
	default:
		return false
	}
}
