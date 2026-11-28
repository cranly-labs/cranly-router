package app

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"maps"
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

	"github.com/TokenFlux/TokenRouter/internal/gateway"
	"github.com/TokenFlux/TokenRouter/internal/gateway/errorpolicy"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit/mockupstream"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream/bedrock"
	"github.com/TokenFlux/TokenRouter/internal/usage"
)

// protocol_lab_scenario_test.go exercises gateway HTTP route registration,
// protocol selection, streaming conversion wrappers, pre-first-byte failover,
// mid-stream disconnects, client error envelopes, usage settlement, and
// multi-turn provider switching across http_routes.go, gateway_middleware.go,
// gateway_selection.go, gateway_openai_text_http.go,
// gateway_openai_text_execution.go, gateway_openai_attempts.go,
// gateway_response_output.go, gateway_messages_http.go,
// gateway_messages_execution.go, gateway_message_attempts.go,
// gateway_google.go, qoder_runtime.go, gateway_count_tokens.go,
// gateway_openai_tokens.go, gateway_openai_ws_http.go, gateway_ws_connections.go,
// and gateway_models.go. Sending real HTTP and WebSocket requests through
// middleware, admission, attempt loops, and completion recorders surfaces
// transport-level framing and accounting bugs alongside pure protocol conversion
// behavior.
//
// Each subtest pairs newProtocolLabRouter with local mockupstream HTTP servers
// so httpclient.UpstreamPool sockets, DownstreamSSEKeepalive heartbeats, strict
// upstream request validators, and protocol stream grammar checks run end to
// end without external services across OpenAI Responses, OpenAI Chat, Anthropic
// Messages, Grok, OpenCode, Gemini GenerateContent, and Antigravity routes.

const (
	// T5 law identifiers partition stream, usage, fault, error, and multi-turn
	// gateway route checks across protocol laboratory scenario suites.
	lawT5Stream            = "T5_STREAM"               // canonical scenario request and SSE grammar law
	lawT5Usage             = "T5_USAGE"                // client-visible vs settled UsageLog bucket law
	lawT5Failover          = "T5_FAILOVER"             // pre-first-byte provider switch law
	lawT5MidstreamTruncate = "T5_MIDSTREAM_DISCONNECT" // post-output mid-stream socket disconnect law
	lawT5ErrorFormat       = "T5_ERROR_FORMAT"         // client-protocol HTTP JSON error envelope law
	lawT5SwitchToNative    = "T5_SWITCH_TO_NATIVE"     // Turn 1 converted -> Turn 2 native replay law
	lawT5SwitchFromNative  = "T5_SWITCH_FROM_NATIVE"   // Turn 1 native -> Turn 2 converted replay law

	// Synthetic scenario slugs identify T5 gateway failover, mid-stream
	// truncation, and upstream HTTP error cells outside the 33 wire fixtures.
	scenarioPreByteFailover     = "failover_pre_first_byte" // primary fails before first byte, secondary succeeds
	scenarioDisconnectAfterData = "disconnect_after_output" // primary truncates stream after emitting partial text
	scenarioUpstreamHTTPError   = "upstream_http_error"     // upstream returns HTTP 400 JSON error body

	// T5 violation codes supplement testkit request-rule and stream-grammar
	// identifiers when gateway-level lifecycle or accounting invariants fail.
	ruleMissingFaultErrorTermination = "missing_fault_error_termination" // faulted stream ended without client error event
	ruleUnstrippedTRC1Carrier        = "unstripped_trc1_carrier"         // trc1: replay carrier leaked to upstream wire
	ruleForeignReplayTokenLeaked     = "foreign_replay_token_leaked"     // foreign signature or ciphertext leaked across families
	ruleRequestRejectedBeforeStream  = "request_rejected_before_stream"  // upstream rejected request before streaming usage
	ruleMissingUsageLog              = "missing_usage_log"               // expected UsageLog row was not settled
	ruleDuplicateUsageLogs           = "duplicate_usage_logs"            // extra UsageLog row was settled unexpectedly
	ruleMissingClientUsage           = "missing_client_usage"            // client SSE stream omitted terminal usage counters
	ruleUsageInputMismatch           = "usage_input_mismatch"            // client input token count disagreed with UsageLog
	ruleUsageCacheReadMismatch       = "usage_cache_read_mismatch"       // client cache-read token count disagreed with UsageLog
	ruleUsageCacheWriteMismatch      = "usage_cache_write_mismatch"      // client cache-write token count disagreed with UsageLog
	ruleUsageOutputMismatch          = "usage_output_mismatch"           // client output token count disagreed with UsageLog
	ruleFallbackContactedAfterOutput = "fallback_contacted_after_output" // secondary provider was called after partial output
	ruleMissingPartialContent        = "missing_partial_content"         // partial text emitted before disconnect disappeared
	ruleWrongClientErrorEnvelope     = "wrong_client_error_envelope"     // HTTP error body did not match client protocol envelope
)

var (
	// protocolLabScenarios lists all 33 canonical section 5.5 wire scenarios in
	// catalog order so T5 visits the exact same scenario set as pure matrix laws.
	protocolLabScenarios = []string{
		"text_basic",
		"text_unicode_split",
		"system_roles",
		"tool_single",
		"tool_parallel",
		"tool_parallel_interleaved",
		"tool_args_fragmented",
		"tool_args_empty",
		"tool_result_image",
		"tool_result_error",
		"tool_ids_missing",
		"reasoning_summary",
		"reasoning_tool_replay",
		"reasoning_token_foreign",
		"reasoning_carrier_roundtrip",
		"reasoning_carrier_native",
		"reasoning_encrypted_only",
		"legacy_history",
		"tools_custom",
		"tools_namespace",
		"tools_search",
		"tools_hosted_search",
		"image_input",
		"document_input",
		"structured_output",
		"stop_sequence",
		"max_tokens",
		"refusal",
		"usage_cache",
		"stream_truncated",
		"stream_error_event",
		"upstream_sse_quirks",
		"empty_response",
	}

	// protocolLabProviders defines the five section 5.6.3 upstream provider kinds:
	// Anthropic Messages, OpenAI Responses, OpenAI Chat Completions, Google Gemini,
	// and DeepSeek's compatible Anthropic Messages endpoint.
	protocolLabProviders = []labProviderKindSpec{
		{
			kind:             "anthropic",
			platform:         capability.PlatformAnthropic,
			providerType:     capability.ProviderTypeAPIKey,
			upstreamProtocol: protocol.ProtocolAnthropicMessages,
			upstreamModel:    "claude-sonnet-4-5-20250929",
			enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
			validation: testkit.RequestValidationOptions{
				StrictTopLevel: true,
			},
			failoverStatus: 529,
		},
		{
			kind:             "openai",
			platform:         capability.PlatformOpenAI,
			providerType:     capability.ProviderTypeAPIKey,
			upstreamProtocol: protocol.ProtocolOpenAIResponses,
			upstreamModel:    "gpt-5.4",
			enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
			failoverStatus:   529,
		},
		{
			kind:             "openai_chat",
			platform:         capability.PlatformOpenAI,
			providerType:     capability.ProviderTypeAPIKey,
			upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
			upstreamModel:    "gpt-4o",
			enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
			extra:            map[string]any{"openai_text_route_mode": "force_chat_completions"},
			failoverStatus:   529,
		},
		{
			kind:             "gemini",
			platform:         capability.PlatformGemini,
			providerType:     capability.ProviderTypeAPIKey,
			upstreamProtocol: protocol.ProtocolGeminiGenerateContent,
			upstreamModel:    "gemini-2.5-pro",
			enabledProtocols: []protocol.ProtocolID{protocol.ProtocolGeminiGenerateContent},
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"gemini-signature"},
			},
			// Gemini internal HTTP exchange sleeps across 429/5xx retries; 401
			// enters outer provider failover immediately without multi-second sleep.
			failoverStatus: http.StatusUnauthorized,
		},
		{
			kind:             "deepseek_anthropic",
			platform:         capability.PlatformDeepseek,
			providerType:     capability.ProviderTypeAPIKey,
			upstreamProtocol: protocol.ProtocolAnthropicMessages,
			upstreamModel:    "deepseek-reasoner",
			enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
			validation: testkit.RequestValidationOptions{
				StrictTopLevel: true,
				DialectRules:   []string{"passback"},
			},
			failoverStatus: 529,
		},
	}

	// protocolLabClients lists the four inbound text client protocols paired
	// with their default fixture models and native passthrough provider kinds.
	protocolLabClients = []labClientSpec{
		{
			slug:         "messages",
			protocol:     protocol.ProtocolAnthropicMessages,
			defaultModel: "claude-sonnet-4-5",
			nativeKind:   "anthropic",
		},
		{
			slug:         "responses",
			protocol:     protocol.ProtocolOpenAIResponses,
			defaultModel: "gpt-5",
			nativeKind:   "openai",
		},
		{
			slug:         "chat",
			protocol:     protocol.ProtocolOpenAIChatCompletions,
			defaultModel: "gpt-4o",
			nativeKind:   "openai_chat",
		},
		{
			slug:         "gemini",
			protocol:     protocol.ProtocolGeminiGenerateContent,
			defaultModel: "gemini-2.5-pro",
			nativeKind:   "gemini",
		},
	}
)

// labProviderKindSpec describes one upstream provider archetype in the T5 matrix.
type labProviderKindSpec struct {
	kind             string                           // stable provider kind identifier
	platform         string                           // capability platform name
	providerType     string                           // capability provider auth type
	upstreamProtocol protocol.ProtocolID              // target wire protocol on upstream
	upstreamModel    string                           // mapped model sent to upstream
	enabledProtocols []protocol.ProtocolID            // explicit upstream_protocols list
	extra            map[string]any                   // optional provider Extra settings
	validation       testkit.RequestValidationOptions // base strict validator options
	failoverStatus   int                              // pre-byte HTTP status triggering immediate switch
}

// labClientSpec describes one inbound client protocol entry point in the T5 matrix.
type labClientSpec struct {
	slug     string              // short route prefix ("messages", "responses", "chat", "gemini")
	protocol protocol.ProtocolID // canonical client protocol identifier
	// path overrides the default per-protocol gateway endpoint in
	// sendLabClientRequest when non-empty, such as /antigravity/v1/messages.
	path         string // optional explicit gateway HTTP path override
	defaultModel string // client model appearing in wire fixtures
	nativeKind   string // provider kind serving native passthrough for this client
}

// labMockServer pairs a local httptest.Server with its underlying
// mockupstream.Handler and optional Antigravity v1internal envelope adapter.
type labMockServer struct {
	*httptest.Server                       // embedded local HTTP test server
	mock             *mockupstream.Handler // inner strict protocol validator and scripted responder
	// ag wraps mock inside the Antigravity v1internal envelope handler when
	// non-nil; nil serves standard upstream requests directly from mock.
	ag *protocolLabAntigravityUpstream // optional v1internal request/response envelope adapter
}

// labRouteCombo pairs one client protocol with one eligible upstream provider kind.
type labRouteCombo struct {
	slug     string              // stable route cell name "<client>_via_<provider_kind>"
	client   labClientSpec       // inbound client protocol spec
	provider labProviderKindSpec // outbound provider kind spec
}

// clientObservedUsage holds token counts extracted from client-visible SSE frames.
type clientObservedUsage struct {
	present       bool // whether any usage payload appeared in client frames
	input         int  // fresh input tokens (Messages/Gemini) or total input (Responses/Chat)
	cacheRead     int  // cached prompt read tokens
	cacheWrite    int  // cache creation tokens when exposed by the client protocol
	hasCacheWrite bool // whether the client wire format exposes a separate cache-write field
	output        int  // output tokens including reasoning
}

// TestProtocolLabRouterEndpointsAndCache exercises newProtocolLabRouter's
// in-memory session cache, group protocol_fallbacks gating, and auxiliary
// endpoint wiring. It protects non-nil route registration across models,
// count_tokens, input_tokens, Antigravity, and Qoder paths.
func TestProtocolLabRouterEndpointsAndCache(t *testing.T) {
	t.Run("in-memory GatewayCache", func(t *testing.T) {
		cache := newProtocolLabGatewayCache()
		ctx := t.Context()

		_, err := cache.GetSessionProviderID(ctx, 42, "sess-1")
		require.Error(t, err)
		require.NoError(t, cache.SetSessionProviderID(ctx, 42, "sess-1", 9, time.Minute))
		gotID, err := cache.GetSessionProviderID(ctx, 42, "sess-1")
		require.NoError(t, err)
		require.Equal(t, int64(9), gotID)
		require.NoError(t, cache.DeleteSessionProviderID(ctx, 42, "sess-1"))
		_, err = cache.GetSessionProviderID(ctx, 42, "sess-1")
		require.Error(t, err)

		claimed, err := cache.SetSessionOwnerGroupID(ctx, 7, "messages", "hash-a", 42, time.Minute)
		require.NoError(t, err)
		require.True(t, claimed)
		claimedAgain, err := cache.SetSessionOwnerGroupID(ctx, 7, "messages", "hash-a", 99, time.Minute)
		require.NoError(t, err)
		require.False(t, claimedAgain)
		ownerGroup, err := cache.GetSessionOwnerGroupID(ctx, 7, "messages", "hash-a")
		require.NoError(t, err)
		require.Equal(t, int64(42), ownerGroup)
	})

	t.Run("models count_tokens input_tokens antigravity and qoder routes", func(t *testing.T) {
		auxUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			_ = r.Body.Close()
			switch {
			case strings.HasSuffix(r.URL.Path, "/messages/count_tokens"):
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"input_tokens":17}`)
			case strings.HasSuffix(r.URL.Path, "/responses/input_tokens"):
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"object":"response.input_tokens","input_tokens":23}`)
			case strings.Contains(r.URL.Path, "v1internal:streamGenerateContent"):
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ag-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":2,\"totalTokenCount\":7}}}\n\n")
			default:
				http.NotFound(w, r)
			}
		}))
		t.Cleanup(auxUpstream.Close)

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{
				{
					ID:               1,
					Platform:         capability.PlatformAnthropic,
					Type:             capability.ProviderTypeAPIKey,
					BaseURL:          auxUpstream.URL,
					Models:           []string{"claude-sonnet-4-5"},
					EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
				},
				{
					ID:               2,
					Platform:         capability.PlatformOpenAI,
					Type:             capability.ProviderTypeAPIKey,
					BaseURL:          auxUpstream.URL,
					Models:           []string{"gpt-5"},
					EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
				},
				{
					ID:               3,
					Platform:         capability.PlatformGemini,
					Type:             capability.ProviderTypeAPIKey,
					BaseURL:          auxUpstream.URL,
					Models:           []string{"gemini-2.5-flash"},
					EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolGeminiGenerateContent},
				},
				{
					ID:       4,
					Platform: capability.PlatformAntigravity,
					Type:     capability.ProviderTypeOAuth,
					BaseURL:  auxUpstream.URL,
					Models:   []string{"claude-sonnet-4-5", "gemini-3-flash"},
					ModelMapping: map[string]string{
						"claude-sonnet-4-5": "claude-sonnet-4-5",
						"gemini-3-flash":    "gemini-3-flash",
					},
				},
				{
					ID:       5,
					Platform: capability.PlatformQoder,
					Type:     capability.ProviderTypeCosy,
					Models:   []string{"ultimate"},
				},
			},
		})
		srv := httptest.NewServer(lab)
		t.Cleanup(srv.Close)
		client := srv.Client()

		for _, modelPath := range []string{"/v1/models", "/models", "/v1beta/models", "/antigravity/models", "/antigravity/v1/models"} {
			resp, err := client.Get(srv.URL + modelPath)
			require.NoError(t, err, modelPath)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, resp.Body.Close())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", modelPath, string(body))
			require.NotEmpty(t, body, modelPath)
		}

		for _, countPath := range []string{"/v1/messages/count_tokens", "/messages/count_tokens"} {
			resp, err := client.Post(
				srv.URL+countPath,
				"application/json",
				strings.NewReader(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"count me"}]}`),
			)
			require.NoError(t, err, countPath)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, resp.Body.Close())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", countPath, string(body))
			require.Equal(t, int64(17), gjson.GetBytes(body, "input_tokens").Int())
		}

		for _, inputTokPath := range []string{"/v1/responses/input_tokens", "/responses/input_tokens"} {
			resp, err := client.Post(
				srv.URL+inputTokPath,
				"application/json",
				strings.NewReader(`{"model":"gpt-5","input":"count responses input"}`),
			)
			require.NoError(t, err, inputTokPath)
			body, err := io.ReadAll(resp.Body)
			require.NoError(t, resp.Body.Close())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, resp.StatusCode, "%s: %s", inputTokPath, string(body))
			require.Greater(t, gjson.GetBytes(body, "input_tokens").Int(), int64(0), string(body))
		}

		agMsgResp, err := client.Post(
			srv.URL+"/antigravity/v1/messages",
			"application/json",
			strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hello antigravity"}]}`),
		)
		require.NoError(t, err)
		agMsgBody, err := io.ReadAll(agMsgResp.Body)
		require.NoError(t, agMsgResp.Body.Close())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, agMsgResp.StatusCode, string(agMsgBody))
		require.Contains(t, string(agMsgBody), "ag-ok")

		agGemResp, err := client.Post(
			srv.URL+"/antigravity/v1beta/models/gemini-3-flash:streamGenerateContent?alt=sse",
			"application/json",
			strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hello gemini antigravity"}]}]}`),
		)
		require.NoError(t, err)
		agGemBody, err := io.ReadAll(agGemResp.Body)
		require.NoError(t, agGemResp.Body.Close())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, agGemResp.StatusCode, string(agGemBody))
		require.Contains(t, string(agGemBody), "ag-ok")

		qoderResp, err := client.Post(
			srv.URL+"/v1/chat/completions",
			"application/json",
			strings.NewReader(`{"model":"ultimate","stream":true,"messages":[{"role":"user","content":"hello qoder"}]}`),
		)
		require.NoError(t, err)
		qoderBody, err := io.ReadAll(qoderResp.Body)
		require.NoError(t, qoderResp.Body.Close())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, qoderResp.StatusCode, string(qoderBody))
		require.Contains(t, string(qoderBody), "ok-qoder")
	})

	t.Run("protocol_fallbacks empty disables conversion and gemini client rejects non-gemini upstream", func(t *testing.T) {
		mockH := mockupstream.New(testkit.Script{
			Name:    "unused",
			Default: testkit.Reply{Raw: "wire/openai_responses/stream/text_basic.sse"},
		})
		upSrv := httptest.NewServer(mockH)
		t.Cleanup(upSrv.Close)

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{{
				ID:               1,
				Platform:         capability.PlatformOpenAI,
				Type:             capability.ProviderTypeAPIKey,
				BaseURL:          upSrv.URL,
				Models:           []string{"gpt-5.4"},
				ModelMapping:     map[string]string{"claude-sonnet-4-5": "gpt-5.4", "gemini-2.5-pro": "gpt-5.4"},
				EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
			}},
			ProtocolFallbacks: map[protocol.ProtocolID][]protocol.ProtocolID{
				protocol.ProtocolAnthropicMessages: {},
			},
		})
		srv := httptest.NewServer(lab)
		t.Cleanup(srv.Close)
		client := srv.Client()

		msgResp, err := client.Post(
			srv.URL+"/v1/messages",
			"application/json",
			strings.NewReader(`{"model":"claude-sonnet-4-5","max_tokens":32,"stream":true,"messages":[{"role":"user","content":"hi"}]}`),
		)
		require.NoError(t, err)
		msgBody, err := io.ReadAll(msgResp.Body)
		require.NoError(t, msgResp.Body.Close())
		require.NoError(t, err)
		require.GreaterOrEqual(t, msgResp.StatusCode, 400)
		require.True(t, hasClientErrorEnvelope(protocol.ProtocolAnthropicMessages, msgBody), string(msgBody))
		require.Empty(t, mockH.Requests())

		gemResp, err := client.Post(
			srv.URL+"/v1beta/models/gemini-2.5-pro:streamGenerateContent?alt=sse",
			"application/json",
			strings.NewReader(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`),
		)
		require.NoError(t, err)
		gemBody, err := io.ReadAll(gemResp.Body)
		require.NoError(t, gemResp.Body.Close())
		require.NoError(t, err)
		require.GreaterOrEqual(t, gemResp.StatusCode, 400)
		require.True(t, hasClientErrorEnvelope(protocol.ProtocolGeminiGenerateContent, gemBody), string(gemBody))
		require.Empty(t, mockH.Requests())
	})

	t.Run("provider health cooldown persists across requests and resets on SetProviders", func(t *testing.T) {
		primaryH := mockupstream.New(testkit.Script{
			Name:    "primary_healthy_when_called",
			Default: testkit.Reply{Raw: "wire/openai_responses/stream/text_basic.sse"},
		})
		secondaryH := mockupstream.New(testkit.Script{
			Name:    "secondary_healthy",
			Default: testkit.Reply{Raw: "wire/openai_responses/stream/text_basic.sse"},
		})
		primarySrv := httptest.NewServer(primaryH)
		t.Cleanup(primarySrv.Close)
		secondarySrv := httptest.NewServer(secondaryH)
		t.Cleanup(secondarySrv.Close)

		specs := []labProvider{
			{
				ID:               1,
				Platform:         capability.PlatformOpenAI,
				Type:             capability.ProviderTypeAPIKey,
				BaseURL:          primarySrv.URL,
				Models:           []string{"gpt-5"},
				EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
				Priority:         0,
			},
			{
				ID:               2,
				Platform:         capability.PlatformOpenAI,
				Type:             capability.ProviderTypeAPIKey,
				BaseURL:          secondarySrv.URL,
				Models:           []string{"gpt-5"},
				EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
				Priority:         1,
			},
		}
		lab := newProtocolLabRouter(t, labOptions{Providers: specs})
		gwSrv := httptest.NewServer(lab)
		t.Cleanup(gwSrv.Close)

		// Mark Provider 1 rate-limited and verify selection skips it immediately.
		require.NoError(t, lab.providers.SetRateLimited(t.Context(), 1, time.Now().Add(time.Hour)))
		status1, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","input":"first","stream":true}`))
		require.Equal(t, http.StatusOK, status1)
		require.Empty(t, primaryH.Requests())
		require.Len(t, secondaryH.Requests(), 1)

		// SetProviders rebuilds fresh records so Provider 1 resumes scheduling.
		lab.SetProviders(t, specs)
		status2, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","input":"second","stream":true}`))
		require.Equal(t, http.StatusOK, status2)
		require.Len(t, primaryH.Requests(), 1)

		// Shared OpenAI RuntimeBlockState skips blocked Provider 1 until SetProviders clears it.
		lab.blocks.Block(1, time.Now().Add(time.Hour), "lab-runtime-block")
		status3, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","input":"third","stream":true}`))
		require.Equal(t, http.StatusOK, status3)
		require.Len(t, primaryH.Requests(), 1)
		require.Len(t, secondaryH.Requests(), 2)

		lab.SetProviders(t, specs)
		status4, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","input":"fourth","stream":true}`))
		require.Equal(t, http.StatusOK, status4)
		require.Len(t, primaryH.Requests(), 2)

		// Verify Gemini thoughtSignature carrier and foreign-token detectors.
		gemSpec := findLabProviderKind("gemini")
		require.True(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolGeminiGenerateContent, []byte(`{"contents":[{"role":"model","parts":[{"thoughtSignature":"trc1:gemini:abc"}]}]}`)))
		require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolGeminiGenerateContent, []byte(`{"contents":[{"role":"user","parts":[{"text":"trc1:literal"}]}]}`)))
		require.True(t, hasLeakedForeignReplayToken(gemSpec, []byte(`{"contents":[{"role":"model","parts":[{"thoughtSignature":"EqQBCgIIAxgB"}]}]}`)))
		require.True(t, hasLeakedForeignReplayToken(gemSpec, []byte(`{"contents":[{"role":"model","parts":[{"thoughtSignature":"gAAAAABn1234"}]}]}`)))
		require.False(t, hasLeakedForeignReplayToken(gemSpec, []byte(`{"contents":[{"role":"model","parts":[{"thoughtSignature":"CpEBCgIIAxgB"}]}]}`)))
	})

	t.Run("Antigravity OAuth in-memory token cache, project backfill, and v1internal local redirect", func(t *testing.T) {
		okSSE := "data: {\"responseId\":\"ag_smoke_1\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"antigravity-fixture-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":11,\"candidatesTokenCount\":4,\"totalTokenCount\":15}}\n\n"
		mockH := mockupstream.NewWithConfig(mockupstream.Config{
			Script: testkit.Script{Name: "ag_smoke", Default: testkit.Reply{Raw: okSSE}},
			Validation: testkit.RequestValidationOptions{
				DialectRules: []string{"gemini-signature", "antigravity-schema"},
			},
		})
		agUp := newProtocolLabAntigravityUpstream(mockH)
		upSrv := httptest.NewServer(agUp)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{{
				ID:           101,
				Name:         "ag-backfill-oauth",
				Platform:     capability.PlatformAntigravity,
				Type:         capability.ProviderTypeOAuth,
				BaseURL:      upSrv.URL,
				Models:       []string{"gemini-2.5-pro"},
				ModelMapping: map[string]string{"gpt-5": "gemini-2.5-pro"},
				Credentials: map[string]any{
					"access_token": "lab-ag-fresh-token",
					"project_id":   "",
				},
				Extra: map[string]any{
					"lab_backfill_project_id": "lab-ag-backfilled-project",
				},
			}},
		})
		require.NotNil(t, lab.antigravityTokens)
		require.NotNil(t, lab.tokenCache)

		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status, _, respBytes := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[1],
			[]byte(`{"model":"gpt-5","stream":true,"input":"verify antigravity fixture"}`),
		)
		require.Equal(t, http.StatusOK, status, string(respBytes))
		require.Contains(t, string(respBytes), "antigravity-fixture-ok")
		agUp.AssertNoViolations(t)

		envs := agUp.Envelopes()
		require.Len(t, envs, 1)
		require.Equal(t, "lab-ag-backfilled-project", envs[0].Project)
		require.Equal(t, "gemini-2.5-pro", envs[0].Model)
		require.Equal(t, "Bearer lab-ag-fresh-token", envs[0].Headers.Get("Authorization"))

		// Request 1 cached the bearer token under the pre-repair row key (ag:provider:101)
		// and persisted project_id into protocolLabProviderStore; Request 2 reads the
		// persisted project_id and caches under ag:lab-ag-backfilled-project.
		preRepairTok, err := lab.tokenCache.GetAccessToken(t.Context(), "ag:provider:101")
		require.NoError(t, err)
		require.Equal(t, "lab-ag-fresh-token", preRepairTok)

		storedRec, err := fixtureRetryStore{lab.providers}.GetByID(t.Context(), 101)
		require.NoError(t, err)
		require.NotNil(t, storedRec)
		require.Equal(t, "lab-ag-backfilled-project", storedRec.GetCredential("project_id"))

		status2, _, respBytes2 := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[1],
			[]byte(`{"model":"gpt-5","stream":true,"input":"second antigravity request"}`),
		)
		require.Equal(t, http.StatusOK, status2, string(respBytes2))
		cachedTok, err := lab.tokenCache.GetAccessToken(t.Context(), provider.AntigravityTokenCacheKey(storedRec))
		require.NoError(t, err)
		require.Equal(t, "lab-ag-fresh-token", cachedTok)
	})
}

// TestProtocolLabIRResponsesUpstreamMatrix exercises Stage 2 Step 3a's merged
// Responses-upstream conversion path (messages_via_openai and chat_via_openai)
// . All 33 canonical scenarios, usage parity,
// pre-byte and pre-semantic SSE failover, mid-stream disconnect block closure,
// error envelopes, mixed-route carrier replay, Cursor input-shaped Chat
// requests, and Claude Code disabled-thinking capability fallbacks must satisfy
// gateway protocol laws without regressions.
func TestProtocolLabIRResponsesUpstreamMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	var targetCombos []labRouteCombo
	for _, combo := range activeLabRouteCombos() {
		if combo.slug == "messages_via_openai" || combo.slug == "chat_via_openai" {
			targetCombos = append(targetCombos, combo)
		}
	}
	require.Len(t, targetCombos, 2)

	for _, combo := range targetCombos {
		t.Run(combo.slug, func(t *testing.T) {
			for _, scenario := range protocolLabScenarios {
				streamEntry, usageEntry := runLabScenarioCell(t, corpus, combo, scenario)
				require.NotEqual(t, labCellFail, streamEntry.Status,
					"IR T5_STREAM failed for %s/%s: rules=%v", combo.slug, scenario, streamEntry.Rules)
				require.NotEqual(t, labCellFail, usageEntry.Status,
					"IR T5_USAGE failed for %s/%s: rules=%v", combo.slug, scenario, usageEntry.Rules)
			}

			failoverEntry := runLabPreByteFailoverCell(t, corpus, combo)
			require.Equal(t, labCellPass, failoverEntry.Status,
				"IR T5_FAILOVER failed for %s: rules=%v", combo.slug, failoverEntry.Rules)

			disconnectEntry := runLabMidstreamDisconnectCell(t, corpus, combo)
			require.Equal(t, labCellPass, disconnectEntry.Status,
				"IR T5_MIDSTREAM_DISCONNECT failed for %s: rules=%v", combo.slug, disconnectEntry.Rules)

			errFmtEntry := runLabErrorFormatCell(t, corpus, combo)
			require.Equal(t, labCellPass, errFmtEntry.Status,
				"IR T5_ERROR_FORMAT failed for %s: rules=%v", combo.slug, errFmtEntry.Rules)

			fromNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, false)
			require.Equal(t, labCellPass, fromNativeEntry.Status,
				"IR T5_SWITCH_FROM_NATIVE failed for %s: rules=%v", combo.slug, fromNativeEntry.Rules)

			toNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, true)
			require.Equal(t, labCellPass, toNativeEntry.Status,
				"IR T5_SWITCH_TO_NATIVE failed for %s: rules=%v", combo.slug, toNativeEntry.Rules)
		})
	}

	t.Run("http200_sse_leading_frame_staging_failover", func(t *testing.T) {
		openAISpec := findLabProviderKind("openai")
		primarySSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_p1\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5.4\",\"output\":[]}}\n\n" +
			"event: response.failed\n" +
			"data: {\"type\":\"response.failed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_p1\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"failed\",\"model\":\"gpt-5.4\",\"output\":[],\"error\":{\"code\":\"server_error\",\"message\":\"primary overloaded\"}}}\n\n"
		secondarySSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_p2\",\"object\":\"response\",\"created_at\":1760000001,\"status\":\"in_progress\",\"model\":\"gpt-5.4\",\"output\":[]}}\n\n" +
			"event: response.output_item.added\n" +
			"data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_p2\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
			"event: response.content_part.added\n" +
			"data: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_p2\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"\"}}\n\n" +
			"event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_p2\",\"output_index\":0,\"content_index\":0,\"delta\":\"fallback-p2-ok\"}\n\n" +
			"event: response.output_text.done\n" +
			"data: {\"type\":\"response.output_text.done\",\"sequence_number\":4,\"item_id\":\"msg_p2\",\"output_index\":0,\"content_index\":0,\"text\":\"fallback-p2-ok\"}}\n\n" +
			"event: response.content_part.done\n" +
			"data: {\"type\":\"response.content_part.done\",\"sequence_number\":5,\"item_id\":\"msg_p2\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"fallback-p2-ok\"}}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0,\"item\":{\"id\":\"msg_p2\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"fallback-p2-ok\"}]}}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_p2\",\"object\":\"response\",\"created_at\":1760000001,\"status\":\"completed\",\"model\":\"gpt-5.4\",\"output\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":3,\"total_tokens\":15,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		primaryH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "p1_sse_fail", Default: testkit.Reply{Raw: primarySSE}},
			Validation: openAISpec.validation,
		})
		secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "p2_sse_ok", Default: testkit.Reply{Raw: secondarySSE}},
			Validation: openAISpec.validation,
		})
		p1Srv := httptest.NewServer(primaryH)
		defer p1Srv.Close()
		p2Srv := httptest.NewServer(secondaryH)
		defer p2Srv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{
				makeLabProviderSpec(1, 0, p1Srv.URL, openAISpec),
				makeLabProviderSpec(2, 1, p2Srv.URL, openAISpec),
			},
			MaxProviderSwitches: 2,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status, ct, respBytes := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[0],
			[]byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"ping"}]}`),
		)
		require.Equal(t, http.StatusOK, status)
		require.Contains(t, strings.ToLower(ct), "text/event-stream")
		require.Contains(t, string(respBytes), "fallback-p2-ok")
		require.NotContains(t, string(respBytes), "resp_p1")
		frames, err := readAllSSEFrames(respBytes)
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolAnthropicMessages, frames, testkit.GrammarOptions{}))
		logs := lab.UsageLogs()
		require.Len(t, logs, 1)
		require.Equal(t, int64(2), logs[0].ProviderID)
	})

	t.Run("cursor_input_shaped_chat_completions", func(t *testing.T) {
		openAISpec := findLabProviderKind("openai")
		replySSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_cur\",\"object\":\"response\",\"created_at\":1760000002,\"status\":\"in_progress\",\"model\":\"gpt-5.4\",\"output\":[]}}\n\n" +
			"event: response.output_item.added\n" +
			"data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_c\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
			"event: response.content_part.added\n" +
			"data: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_c\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"\"}}\n\n" +
			"event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_c\",\"output_index\":0,\"content_index\":0,\"delta\":\"cursor-chat-ok\"}\n\n" +
			"event: response.output_text.done\n" +
			"data: {\"type\":\"response.output_text.done\",\"sequence_number\":4,\"item_id\":\"msg_c\",\"output_index\":0,\"content_index\":0,\"text\":\"cursor-chat-ok\"}}\n\n" +
			"event: response.content_part.done\n" +
			"data: {\"type\":\"response.content_part.done\",\"sequence_number\":5,\"item_id\":\"msg_c\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"cursor-chat-ok\"}}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0,\"item\":{\"id\":\"msg_c\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"cursor-chat-ok\"}]}}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_cur\",\"object\":\"response\",\"created_at\":1760000002,\"status\":\"completed\",\"model\":\"gpt-5.4\",\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":3,\"total_tokens\":13,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		upH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "cursor_resp", Default: testkit.Reply{Raw: replySSE}},
			Validation: openAISpec.validation,
		})
		upSrv := httptest.NewServer(upH)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, openAISpec)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status, ct, respBytes := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[2],
			[]byte(`{"model":"gpt-5.4","stream":true,"stream_options":{"include_usage":true},"input":[{"type":"message","role":"user","content":"hello from cursor"}]}`),
		)
		require.Equal(t, http.StatusOK, status)
		require.Contains(t, strings.ToLower(ct), "text/event-stream")
		require.Contains(t, string(respBytes), "cursor-chat-ok")
		frames, err := readAllSSEFrames(respBytes)
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, frames, testkit.GrammarOptions{}))
		require.Empty(t, compareClientAndSettledUsage(protocol.ProtocolOpenAIChatCompletions, frames, lab.UsageLogs()))
	})

	t.Run("claude_code_corpus_disabled_thinking_capability_guard", func(t *testing.T) {
		corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolAnthropicMessages)
		require.NoError(t, err)
		var claudeReqs []testkit.Fixture
		for _, fx := range corpusReqs {
			if strings.HasPrefix(fx.Path, "corpus/clients/claude-code/") {
				claudeReqs = append(claudeReqs, fx)
			}
		}
		require.Len(t, claudeReqs, 6)

		replySSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_cc\",\"object\":\"response\",\"created_at\":1760000040,\"status\":\"in_progress\",\"model\":\"gpt-5\",\"output\":[]}}\n\n" +
			"event: response.output_item.added\n" +
			"data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_cc\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
			"event: response.content_part.added\n" +
			"data: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_cc\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"\"}}\n\n" +
			"event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_cc\",\"output_index\":0,\"content_index\":0,\"delta\":\"ok\"}}\n\n" +
			"event: response.output_text.done\n" +
			"data: {\"type\":\"response.output_text.done\",\"sequence_number\":4,\"item_id\":\"msg_cc\",\"output_index\":0,\"content_index\":0,\"text\":\"ok\"}}\n\n" +
			"event: response.content_part.done\n" +
			"data: {\"type\":\"response.content_part.done\",\"sequence_number\":5,\"item_id\":\"msg_cc\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"ok\"}}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0,\"item\":{\"id\":\"msg_cc\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"ok\"}]}}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_cc\",\"object\":\"response\",\"created_at\":1760000040,\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":2,\"total_tokens\":14,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		for _, tc := range []struct {
			upstreamModel string
			wantEffort    string
		}{
			{upstreamModel: "gpt-5.4", wantEffort: "none"},
			{upstreamModel: "gpt-5", wantEffort: "minimal"},
			{upstreamModel: "o3", wantEffort: "low"},
			{upstreamModel: "gpt-5-codex", wantEffort: ""},
		} {
			t.Run(tc.upstreamModel, func(t *testing.T) {
				provSpec := findLabProviderKind("openai")
				provSpec.upstreamModel = tc.upstreamModel

				upH := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "cc_resp_" + tc.upstreamModel, Default: testkit.Reply{Raw: replySSE}},
					Validation: provSpec.validation,
				})
				upSrv := httptest.NewServer(upH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, provSpec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				for i, fx := range claudeReqs {
					status, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[0], fx.Data)
					require.Equal(t, http.StatusOK, status, "fixture=%s", fx.Path)
					frames, fErr := readAllSSEFrames(respBytes)
					require.NoError(t, fErr)
					require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolAnthropicMessages, frames, testkit.GrammarOptions{}))

					reqs := upH.Requests()
					require.Len(t, reqs, i+1)
					sent := reqs[i].Body
					if tc.wantEffort == "" {
						require.False(t, gjson.GetBytes(sent, "reasoning").Exists(), "fixture=%s body=%s", fx.Path, string(sent))
					} else {
						require.Equal(t, tc.wantEffort, gjson.GetBytes(sent, "reasoning.effort").String(), "fixture=%s", fx.Path)
						require.False(t, gjson.GetBytes(sent, "reasoning.summary").Exists(), "fixture=%s", fx.Path)
					}
				}
				require.Empty(t, upH.Violations())
			})
		}
	})
}

// TestProtocolLabIRChatUpstreamMatrix exercises Stage 2 Step 3b's merged
// Chat-upstream conversion path (messages_via_openai_chat and
// responses_via_openai_chat). All 33 canonical
// scenarios, usage parity, pre-byte and pre-semantic HTTP 200 SSE failover,
// mid-stream disconnect block closure, error envelopes, mixed-route replay,
// summary-less trc1:plain round-trip recovery without Redis, Cursor
// input-shaped Chat requests, and Claude Code disabled-thinking capability
// fallbacks must satisfy gateway protocol laws.
func TestProtocolLabIRChatUpstreamMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	var targetCombos []labRouteCombo
	for _, combo := range activeLabRouteCombos() {
		if combo.slug == "messages_via_openai_chat" || combo.slug == "responses_via_openai_chat" {
			targetCombos = append(targetCombos, combo)
		}
	}
	require.Len(t, targetCombos, 2)

	for _, combo := range targetCombos {
		t.Run(combo.slug, func(t *testing.T) {
			for _, scenario := range protocolLabScenarios {
				streamEntry, usageEntry := runLabScenarioCell(t, corpus, combo, scenario)
				require.NotEqual(t, labCellFail, streamEntry.Status,
					"IR T5_STREAM failed for %s/%s: rules=%v", combo.slug, scenario, streamEntry.Rules)
				require.NotEqual(t, labCellFail, usageEntry.Status,
					"IR T5_USAGE failed for %s/%s: rules=%v", combo.slug, scenario, usageEntry.Rules)
			}

			failoverEntry := runLabPreByteFailoverCell(t, corpus, combo)
			require.Equal(t, labCellPass, failoverEntry.Status,
				"IR T5_FAILOVER failed for %s: rules=%v", combo.slug, failoverEntry.Rules)

			disconnectEntry := runLabMidstreamDisconnectCell(t, corpus, combo)
			require.Equal(t, labCellPass, disconnectEntry.Status,
				"IR T5_MIDSTREAM_DISCONNECT failed for %s: rules=%v", combo.slug, disconnectEntry.Rules)

			errFmtEntry := runLabErrorFormatCell(t, corpus, combo)
			require.Equal(t, labCellPass, errFmtEntry.Status,
				"IR T5_ERROR_FORMAT failed for %s: rules=%v", combo.slug, errFmtEntry.Rules)

			fromNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, false)
			require.Equal(t, labCellPass, fromNativeEntry.Status,
				"IR T5_SWITCH_FROM_NATIVE failed for %s: rules=%v", combo.slug, fromNativeEntry.Rules)

			toNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, true)
			require.Equal(t, labCellPass, toNativeEntry.Status,
				"IR T5_SWITCH_TO_NATIVE failed for %s: rules=%v", combo.slug, toNativeEntry.Rules)
		})
	}

	t.Run("http200_sse_leading_frame_staging_failover", func(t *testing.T) {
		chatSpec := findLabProviderKind("openai_chat")
		primarySSE := "data: {\"id\":\"chatcmpl_p1\",\"object\":\"chat.completion.chunk\",\"created\":1760000010,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\"},\"finish_reason\":null}]}\n\n" +
			"data: {\"error\":{\"type\":\"server_error\",\"code\":\"rate_limit_exceeded\",\"message\":\"primary chat overloaded\"}}\n\n"
		secondarySSE := "data: {\"id\":\"chatcmpl_p2\",\"object\":\"chat.completion.chunk\",\"created\":1760000011,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"chat-fallback-p2-ok\"},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"chatcmpl_p2\",\"object\":\"chat.completion.chunk\",\"created\":1760000011,\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":14,\"completion_tokens\":4,\"total_tokens\":18}}\n\n" +
			"data: [DONE]\n\n"

		for _, clientIdx := range []int{0, 1} {
			clientSpec := protocolLabClients[clientIdx]
			t.Run(clientSpec.slug, func(t *testing.T) {
				primaryH := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "chat_p1_sse_fail", Default: testkit.Reply{Raw: primarySSE}},
					Validation: chatSpec.validation,
				})
				secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "chat_p2_sse_ok", Default: testkit.Reply{Raw: secondarySSE}},
					Validation: chatSpec.validation,
				})
				p1Srv := httptest.NewServer(primaryH)
				defer p1Srv.Close()
				p2Srv := httptest.NewServer(secondaryH)
				defer p2Srv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{
						makeLabProviderSpec(1, 0, p1Srv.URL, chatSpec),
						makeLabProviderSpec(2, 1, p2Srv.URL, chatSpec),
					},
					MaxProviderSwitches: 2,
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				var reqPayload []byte
				if clientSpec.protocol == protocol.ProtocolAnthropicMessages {
					reqPayload = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"ping"}]}`)
				} else {
					reqPayload = []byte(`{"model":"gpt-5","stream":true,"input":"ping"}`)
				}
				status, ct, respBytes := sendLabClientRequest(t, gwSrv, clientSpec, reqPayload)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, strings.ToLower(ct), "text/event-stream")
				require.Contains(t, string(respBytes), "chat-fallback-p2-ok")
				require.NotContains(t, string(respBytes), "chatcmpl_p1")
				frames, err := readAllSSEFrames(respBytes)
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateStreamGrammar(clientSpec.protocol, frames, testkit.GrammarOptions{}))
				logs := lab.UsageLogs()
				require.Len(t, logs, 1)
				require.Equal(t, int64(2), logs[0].ProviderID)
			})
		}
	})

	t.Run("two_turn_plain_carrier_roundtrip_without_redis", func(t *testing.T) {
		chatSpec := findLabProviderKind("openai_chat")
		turn1SSE := "data: {\"id\":\"chatcmpl_t1\",\"object\":\"chat.completion.chunk\",\"created\":1760000020,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"reasoning_content\":\"Audit inventory table first.\"},\"finish_reason\":null}]}\n\n" +
			"data: {\"id\":\"chatcmpl_t1\",\"object\":\"chat.completion.chunk\",\"created\":1760000020,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call_inv_1\",\"type\":\"function\",\"function\":{\"name\":\"check_stock\",\"arguments\":\"{\\\"sku\\\":\\\"A1\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n" +
			"data: {\"id\":\"chatcmpl_t1\",\"object\":\"chat.completion.chunk\",\"created\":1760000020,\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":10,\"total_tokens\":30,\"completion_tokens_details\":{\"reasoning_tokens\":5}}}\n\n" +
			"data: [DONE]\n\n"
		turn2SSE := "data: {\"id\":\"chatcmpl_t2\",\"object\":\"chat.completion.chunk\",\"created\":1760000021,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Stock confirmed.\"},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"chatcmpl_t2\",\"object\":\"chat.completion.chunk\",\"created\":1760000021,\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":35,\"completion_tokens\":4,\"total_tokens\":39}}\n\n" +
			"data: [DONE]\n\n"

		upH := mockupstream.NewWithConfig(mockupstream.Config{
			Script: testkit.Script{
				Name: "plain_carrier_roundtrip",
				Turns: []testkit.Turn{
					{Reply: testkit.Reply{Raw: turn1SSE}},
					{Reply: testkit.Reply{Raw: turn2SSE}},
				},
			},
			Validation: testkit.RequestValidationOptions{
				DialectRules: []string{"deepseek-thinking"},
			},
		})
		upSrv := httptest.NewServer(upH)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, chatSpec)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		turn1Req := []byte(`{"model":"gpt-5","stream":true,"reasoning":{"effort":"high"},"input":[{"type":"message","role":"user","content":"check sku A1"}],"tools":[{"type":"function","name":"check_stock","parameters":{"type":"object","properties":{"sku":{"type":"string"}}}}]}`)
		status1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], turn1Req)
		require.Equal(t, http.StatusOK, status1)
		frames1, err := readAllSSEFrames(resp1)
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames1, testkit.GrammarOptions{}))

		turn2Raw, simErr := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
			PrevRequest:              turn1Req,
			ResponseFrames:           frames1,
			SynthesizeMissingOutputs: true,
			FollowUpUserText:         "Finalize order.",
		})
		require.NoError(t, simErr)
		// turn2Envelope projects top-level OpenAI Responses request members for Turn 2 replay editing.
		type turn2Envelope struct {
			Model     string            `json:"model"`     // client model identifier
			Stream    bool              `json:"stream"`    // streaming toggle
			Reasoning json.RawMessage   `json:"reasoning"` // preserved reasoning config
			Tools     json.RawMessage   `json:"tools"`     // preserved tool declarations
			Input     []json.RawMessage `json:"input"`     // ordered input items
		}
		// reasoningItemProbe inspects replayed Responses input items for reasoning carrier tokens.
		type reasoningItemProbe struct {
			Type             string `json:"type"`              // item type discriminator
			ID               string `json:"id,omitempty"`      // item identifier
			EncryptedContent string `json:"encrypted_content"` // trc1:plain carrier token
		}
		// strippedReasoningItem serializes a summary-less Responses reasoning item preserving its carrier.
		type strippedReasoningItem struct {
			Type             string   `json:"type"`              // literal "reasoning"
			ID               string   `json:"id,omitempty"`      // item identifier
			Summary          []string `json:"summary"`           // empty summary array
			EncryptedContent string   `json:"encrypted_content"` // retained trc1:plain carrier
		}
		var env turn2Envelope
		require.NoError(t, json.Unmarshal(turn2Raw, &env))
		// Strip visible summary text from the replayed reasoning item via typed
		// projections so Turn 2 relies exclusively on the trc1:plain carrier.
		for idx, rawItem := range env.Input {
			var probe reasoningItemProbe
			if json.Unmarshal(rawItem, &probe) == nil && probe.Type == "reasoning" {
				strippedItem, marshalErr := json.Marshal(strippedReasoningItem{
					Type:             "reasoning",
					ID:               probe.ID,
					Summary:          []string{},
					EncryptedContent: probe.EncryptedContent,
				})
				require.NoError(t, marshalErr)
				env.Input[idx] = strippedItem
			}
		}
		turn2Stripped, err := json.Marshal(env)
		require.NoError(t, err)

		status2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], turn2Stripped)
		require.Equal(t, http.StatusOK, status2)
		require.Empty(t, upH.Violations(), "Turn 2 must pass deepseek-thinking via trc1:plain carrier")
		require.Contains(t, string(resp2), "Stock confirmed.")
		require.Len(t, lab.UsageLogs(), 2)
	})

	t.Run("cursor_input_shaped_chat_completions_via_openai_chat", func(t *testing.T) {
		chatSpec := findLabProviderKind("openai_chat")
		replySSE := "data: {\"id\":\"chatcmpl_cur_c\",\"object\":\"chat.completion.chunk\",\"created\":1760000030,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"cursor-raw-chat-ok\"},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"chatcmpl_cur_c\",\"object\":\"chat.completion.chunk\",\"created\":1760000030,\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":3,\"total_tokens\":13}}\n\n" +
			"data: [DONE]\n\n"

		upH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "cursor_raw_chat", Default: testkit.Reply{Raw: replySSE}},
			Validation: chatSpec.validation,
		})
		upSrv := httptest.NewServer(upH)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, chatSpec)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status, ct, respBytes := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[2],
			[]byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"input":[{"type":"message","role":"user","content":"hello from cursor"}]}`),
		)
		require.Equal(t, http.StatusOK, status)
		require.Contains(t, strings.ToLower(ct), "text/event-stream")
		require.Empty(t, upH.Violations())
		require.Contains(t, string(respBytes), "cursor-raw-chat-ok")
		frames, err := readAllSSEFrames(respBytes)
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, frames, testkit.GrammarOptions{}))
		require.Empty(t, compareClientAndSettledUsage(protocol.ProtocolOpenAIChatCompletions, frames, lab.UsageLogs()))
	})

	t.Run("claude_code_corpus_disabled_thinking_capability_guard", func(t *testing.T) {
		corpusReqs, err := testkit.CorpusRequests(protocol.ProtocolAnthropicMessages)
		require.NoError(t, err)
		var claudeReqs []testkit.Fixture
		for _, fx := range corpusReqs {
			if strings.HasPrefix(fx.Path, "corpus/clients/claude-code/") {
				claudeReqs = append(claudeReqs, fx)
			}
		}
		require.Len(t, claudeReqs, 6)

		replySSE := "data: {\"id\":\"chatcmpl_cc\",\"object\":\"chat.completion.chunk\",\"created\":1760000050,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: {\"id\":\"chatcmpl_cc\",\"object\":\"chat.completion.chunk\",\"created\":1760000050,\"model\":\"gpt-4o\",\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":2,\"total_tokens\":14}}\n\n" +
			"data: [DONE]\n\n"

		for _, tc := range []struct {
			name          string
			platform      string
			upstreamModel string
			wantEffort    string
		}{
			{name: "openai_gpt_5_4_none", platform: capability.PlatformOpenAI, upstreamModel: "gpt-5.4", wantEffort: "none"},
			{name: "openai_gpt_5_minimal", platform: capability.PlatformOpenAI, upstreamModel: "gpt-5", wantEffort: "minimal"},
			{name: "openai_o3_low", platform: capability.PlatformOpenAI, upstreamModel: "o3", wantEffort: "low"},
			{name: "openai_gpt_5_codex_omitted", platform: capability.PlatformOpenAI, upstreamModel: "gpt-5-codex", wantEffort: ""},
			{name: "openai_gpt_4o_omitted", platform: capability.PlatformOpenAI, upstreamModel: "gpt-4o", wantEffort: ""},
			{name: "deepseek_reasoner_omitted", platform: capability.PlatformDeepseek, upstreamModel: "deepseek-reasoner", wantEffort: ""},
			{name: "deepseek_chat_omitted", platform: capability.PlatformDeepseek, upstreamModel: "deepseek-chat", wantEffort: ""},
			{name: "deepseek_v4_pro_omitted", platform: capability.PlatformDeepseek, upstreamModel: "deepseek-v4-pro", wantEffort: ""},
			{name: "kimi_k2_6_omitted", platform: capability.PlatformKimi, upstreamModel: "kimi-k2.6", wantEffort: ""},
			{name: "kimi_k3_low", platform: capability.PlatformKimi, upstreamModel: "kimi-k3", wantEffort: "low"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				provSpec := findLabProviderKind("openai_chat")
				provSpec.platform = tc.platform
				provSpec.upstreamModel = tc.upstreamModel

				upH := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "cc_chat_" + tc.name, Default: testkit.Reply{Raw: replySSE}},
					Validation: provSpec.validation,
				})
				upSrv := httptest.NewServer(upH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, provSpec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				for i, fx := range claudeReqs {
					status, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[0], fx.Data)
					require.Equal(t, http.StatusOK, status, "fixture=%s", fx.Path)
					frames, fErr := readAllSSEFrames(respBytes)
					require.NoError(t, fErr)
					require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolAnthropicMessages, frames, testkit.GrammarOptions{}))

					reqs := upH.Requests()
					require.Len(t, reqs, i+1)
					sent := reqs[i].Body
					effNode := gjson.GetBytes(sent, "reasoning_effort")
					if tc.wantEffort == "" {
						require.False(t, effNode.Exists(), "fixture=%s unexpected reasoning_effort=%q", fx.Path, effNode.String())
					} else {
						require.Equal(t, tc.wantEffort, effNode.String(), "fixture=%s", fx.Path)
					}
				}
				require.Empty(t, upH.Violations())
			})
		}
	})
}

// buildLabEventStreamFrame encodes one binary AWS EventStream frame with typed
// string headers and IEEE CRC32 prelude and message checksums. T5 Bedrock
// tests use the returned bytes to simulate invoke-with-response-stream replies.
func buildLabEventStreamFrame(headers map[string]string, payload []byte) []byte {
	var hdrBuf bytes.Buffer
	for k, v := range headers {
		_ = hdrBuf.WriteByte(byte(len(k)))
		_, _ = hdrBuf.WriteString(k)
		_ = hdrBuf.WriteByte(7)
		_ = binary.Write(&hdrBuf, binary.BigEndian, uint16(len(v)))
		_, _ = hdrBuf.WriteString(v)
	}
	hdrBytes := hdrBuf.Bytes()
	totalLen := uint32(12 + len(hdrBytes) + len(payload) + 4)

	var prelude bytes.Buffer
	_ = binary.Write(&prelude, binary.BigEndian, totalLen)
	_ = binary.Write(&prelude, binary.BigEndian, uint32(len(hdrBytes)))
	preludeCRC := crc32.Checksum(prelude.Bytes(), bedrock.Crc32IEEETable)

	var frame bytes.Buffer
	_, _ = frame.Write(prelude.Bytes())
	_ = binary.Write(&frame, binary.BigEndian, preludeCRC)
	_, _ = frame.Write(hdrBytes)
	_, _ = frame.Write(payload)
	msgCRC := crc32.Checksum(frame.Bytes(), bedrock.Crc32IEEETable)
	_ = binary.Write(&frame, binary.BigEndian, msgCRC)
	return frame.Bytes()
}

// buildLabBedrockChunkStream wraps Claude JSON event strings inside base64
// chunk AWS EventStream frames preceded by an initial-response metadata frame.
// Empty event slices still emit the initial-response preamble frame cleanly.
func buildLabBedrockChunkStream(events ...string) []byte {
	var out bytes.Buffer
	_, _ = out.Write(buildLabEventStreamFrame(map[string]string{
		":message-type": "event",
		":event-type":   "initial-response",
	}, []byte(`{}`)))
	for _, ev := range events {
		b64 := base64.StdEncoding.EncodeToString([]byte(ev))
		payload := []byte(`{"bytes":"` + b64 + `"}`)
		_, _ = out.Write(buildLabEventStreamFrame(map[string]string{
			":message-type": "event",
			":event-type":   "chunk",
			":content-type": "application/json",
		}, payload))
	}
	return out.Bytes()
}

// TestProtocolLabIRMessagesUpstreamMatrix exercises Stage 2 Step 3c's merged
// Messages-upstream conversion path (responses_via_anthropic, chat_via_anthropic,
// responses_via_deepseek_anthropic, chat_via_deepseek_anthropic, Google Vertex
// service_account, and AWS Bedrock EventStream).
// All 33 canonical scenarios, usage parity, pre-byte and HTTP 200 SSE leading-frame
// failover, mid-stream disconnect block closure, cross-Claude-model signature 400
// lossless stripping retries, and Bedrock EventStream decoding must pass.
func TestProtocolLabIRMessagesUpstreamMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	var targetCombos []labRouteCombo
	for _, combo := range activeLabRouteCombos() {
		switch combo.slug {
		case "responses_via_anthropic", "chat_via_anthropic",
			"responses_via_deepseek_anthropic", "chat_via_deepseek_anthropic":
			targetCombos = append(targetCombos, combo)
		}
	}
	require.Len(t, targetCombos, 4)

	for _, combo := range targetCombos {
		t.Run(combo.slug, func(t *testing.T) {
			for _, scenario := range protocolLabScenarios {
				streamEntry, usageEntry := runLabScenarioCell(t, corpus, combo, scenario)
				require.NotEqual(t, labCellFail, streamEntry.Status,
					"IR T5_STREAM failed for %s/%s: rules=%v", combo.slug, scenario, streamEntry.Rules)
				require.NotEqual(t, labCellFail, usageEntry.Status,
					"IR T5_USAGE failed for %s/%s: rules=%v", combo.slug, scenario, usageEntry.Rules)
			}

			failoverEntry := runLabPreByteFailoverCell(t, corpus, combo)
			require.Equal(t, labCellPass, failoverEntry.Status,
				"IR T5_FAILOVER failed for %s: rules=%v", combo.slug, failoverEntry.Rules)

			disconnectEntry := runLabMidstreamDisconnectCell(t, corpus, combo)
			require.Equal(t, labCellPass, disconnectEntry.Status,
				"IR T5_MIDSTREAM_DISCONNECT failed for %s: rules=%v", combo.slug, disconnectEntry.Rules)

			errFmtEntry := runLabErrorFormatCell(t, corpus, combo)
			require.Equal(t, labCellPass, errFmtEntry.Status,
				"IR T5_ERROR_FORMAT failed for %s: rules=%v", combo.slug, errFmtEntry.Rules)

			fromNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, false)
			require.Equal(t, labCellPass, fromNativeEntry.Status,
				"IR T5_SWITCH_FROM_NATIVE failed for %s: rules=%v", combo.slug, fromNativeEntry.Rules)

			toNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, true)
			require.Equal(t, labCellPass, toNativeEntry.Status,
				"IR T5_SWITCH_TO_NATIVE failed for %s: rules=%v", combo.slug, toNativeEntry.Rules)
		})
	}

	t.Run("http200_sse_leading_frame_staging_failover", func(t *testing.T) {
		primarySSE := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_p1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5-20250929\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n" +
			"event: error\n" +
			"data: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"primary messages overloaded\"}}\n\n"
		secondarySSE := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_p2\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5-20250929\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":14,\"output_tokens\":0}}}\n\n" +
			"event: content_block_start\n" +
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\n" +
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"messages-fallback-p2-ok\"}}\n\n" +
			"event: content_block_stop\n" +
			"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\n" +
			"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\",\"stop_sequence\":null},\"usage\":{\"output_tokens\":5}}\n\n" +
			"event: message_stop\n" +
			"data: {\"type\":\"message_stop\"}\n\n"

		for _, provKind := range []string{"anthropic", "deepseek_anthropic"} {
			provSpec := findLabProviderKind(provKind)
			for _, clientIdx := range []int{1, 2} {
				clientSpec := protocolLabClients[clientIdx]
				t.Run(clientSpec.slug+"_via_"+provKind, func(t *testing.T) {
					primaryH := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "msg_p1_sse_fail", Default: testkit.Reply{Raw: primarySSE}},
						Validation: provSpec.validation,
					})
					secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "msg_p2_sse_ok", Default: testkit.Reply{Raw: secondarySSE}},
						Validation: provSpec.validation,
					})
					p1Srv := httptest.NewServer(primaryH)
					defer p1Srv.Close()
					p2Srv := httptest.NewServer(secondaryH)
					defer p2Srv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{
							makeLabProviderSpec(1, 0, p1Srv.URL, provSpec),
							makeLabProviderSpec(2, 1, p2Srv.URL, provSpec),
						},
						MaxProviderSwitches: 2,
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					var reqPayload []byte
					if clientSpec.protocol == protocol.ProtocolOpenAIResponses {
						reqPayload = []byte(`{"model":"gpt-5","stream":true,"input":"ping"}`)
					} else {
						reqPayload = []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"ping"}]}`)
					}
					status, ct, respBytes := sendLabClientRequest(t, gwSrv, clientSpec, reqPayload)
					require.Equal(t, http.StatusOK, status)
					require.Contains(t, strings.ToLower(ct), "text/event-stream")
					require.Contains(t, string(respBytes), "messages-fallback-p2-ok")
					require.NotContains(t, string(respBytes), "msg_p1")
					frames, err := readAllSSEFrames(respBytes)
					require.NoError(t, err)
					require.Empty(t, testkit.ValidateStreamGrammar(clientSpec.protocol, frames, testkit.GrammarOptions{}))
					logs := lab.UsageLogs()
					require.Len(t, logs, 1)
					require.Equal(t, int64(2), logs[0].ProviderID)
				})
			}
		}
	})

	t.Run("cross_claude_model_signature_400_lossless_retry", func(t *testing.T) {
		carrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "EqQBCrossModelSig",
			Text:   "Inspect repository state before answering.",
		})
		require.NoError(t, err)
		reqPayload := []byte(fmt.Sprintf(`{
			"model":"gpt-5",
			"stream":true,
			"reasoning":{"effort":"high"},
			"input":[
				{"type":"message","role":"user","content":"check id"},
				{"type":"reasoning","id":"rs_sig","summary":[],"encrypted_content":%q},
				{"type":"function_call","call_id":"call_sig","name":"lookup","arguments":"{\"id\":9007199254740993}"},
				{"type":"function_call_output","call_id":"call_sig","output":"found"}
			],
			"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}]
		}`, carrier))

		okSSE := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_sig_ok\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-opus-4-6\",\"content\":[],\"usage\":{\"input_tokens\":22,\"output_tokens\":0}}}\n\n" +
			"event: content_block_start\n" +
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\n" +
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"sig-retry-ok\"}}\n\n" +
			"event: content_block_stop\n" +
			"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\n" +
			"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n" +
			"event: message_stop\n" +
			"data: {\"type\":\"message_stop\"}\n\n"

		for _, tc := range []struct {
			name     string
			platform string
		}{
			{name: "native_anthropic", platform: capability.PlatformAnthropic},
			{name: "compat_anthropic", platform: capability.PlatformOpenCode},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var mu sync.Mutex
				var bodies [][]byte
				var record telemetry.RequestRecord
				upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, readErr := io.ReadAll(r.Body)
					if readErr != nil {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					mu.Lock()
					idx := len(bodies)
					bodies = append(bodies, raw)
					mu.Unlock()
					if idx == 0 {
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, `{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0: Invalid `+"`signature`"+` in `+"`thinking`"+` block"}}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, okSSE)
				}))
				defer upSrv.Close()

				prov := labProvider{
					ID:               1,
					Name:             tc.name,
					Platform:         tc.platform,
					Type:             capability.ProviderTypeAPIKey,
					BaseURL:          upSrv.URL,
					Model:            "claude-opus-4-6",
					ModelMapping:     map[string]string{"gpt-5": "claude-opus-4-6"},
					EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
				}
				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{prov},
					OnTelemetry: func(snapshot telemetry.RequestRecord) {
						mu.Lock()
						defer mu.Unlock()
						record = snapshot
					},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				status, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], reqPayload)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, string(respBytes), "sig-retry-ok")

				mu.Lock()
				captured := slices.Clone(bodies)
				mu.Unlock()
				require.Len(t, captured, 2)
				require.Equal(t, "EqQBCrossModelSig", gjson.GetBytes(captured[0], "messages.1.content.0.signature").String())
				require.Equal(t, "tool_use", gjson.GetBytes(captured[1], "messages.1.content.0.type").String())
				require.Equal(t, "9007199254740993", gjson.GetBytes(captured[1], "messages.1.content.0.input.id").Raw)
				logs := lab.UsageLogs()
				require.Len(t, logs, 1)
				mu.Lock()
				final := record
				mu.Unlock()
				require.Len(t, final.Attempts, 2)
				require.NotNil(t, final.Attempts[0].ProtocolConversion)
				require.Zero(t, final.Attempts[0].ProtocolConversion.LossCounts[string(ir.LossReplayTokenDropped)])
				require.NotNil(t, final.Attempts[1].ProtocolConversion)
				require.Equal(t, 1, final.Attempts[1].ProtocolConversion.LossCounts[string(ir.LossReplayTokenDropped)])
				require.Equal(t, final.Attempts[1].ProtocolConversion, logs[0].ProtocolConversion)
			})
		}
	})

	t.Run("vertex_and_bedrock_eventstream_end_to_end", func(t *testing.T) {
		t.Run("vertex_service_account", func(t *testing.T) {
			var mu sync.Mutex
			var paths []string
			vertexSSE := "event: message_start\n" +
				"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_vtx_e2e\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"usage\":{\"input_tokens\":16,\"output_tokens\":0}}}\n\n" +
				"event: content_block_start\n" +
				"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
				"event: content_block_delta\n" +
				"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"vertex-e2e-ok\"}}\n\n" +
				"event: content_block_stop\n" +
				"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
				"event: message_delta\n" +
				"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n" +
				"event: message_stop\n" +
				"data: {\"type\":\"message_stop\"}\n\n"

			vtxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				paths = append(paths, r.URL.Path)
				mu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, vertexSSE)
			}))
			defer vtxSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{{
					ID:           1,
					Name:         "vertex-e2e",
					Platform:     capability.PlatformAnthropic,
					Type:         capability.ProviderTypeServiceAccount,
					BaseURL:      vtxSrv.URL,
					Model:        "claude-sonnet-4-6",
					ModelMapping: map[string]string{"gpt-5": "claude-sonnet-4-6"},
				}},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			status, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","stream":true,"input":"hello vertex"}`))
			require.Equal(t, http.StatusOK, status)
			require.Contains(t, string(respBytes), "vertex-e2e-ok")
			frames, err := readAllSSEFrames(respBytes)
			require.NoError(t, err)
			require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{}))

			mu.Lock()
			gotPaths := slices.Clone(paths)
			mu.Unlock()
			require.Len(t, gotPaths, 1)
			require.Contains(t, gotPaths[0], "/publishers/anthropic/models/")
			require.Contains(t, gotPaths[0], ":streamRawPredict")
			logs := lab.UsageLogs()
			require.Len(t, logs, 1)
			require.Equal(t, 16, logs[0].InputTokens)
			require.Equal(t, 4, logs[0].OutputTokens)
		})

		t.Run("bedrock_sigv4_eventstream_streaming_and_buffered", func(t *testing.T) {
			var mu sync.Mutex
			var authHeaders []string
			var paths []string
			bedrockStream := buildLabBedrockChunkStream(
				`{"type":"message_start","message":{"id":"msg_bdrk_e2e","type":"message","role":"assistant","model":"us.anthropic.claude-sonnet-4-6","content":[],"usage":{"input_tokens":0,"output_tokens":0}}}`,
				`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
				`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"bedrock-e2e-ok"}}`,
				`{"type":"content_block_stop","index":0}`,
				`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"amazon-bedrock-invocationMetrics":{"inputTokenCount":44,"outputTokenCount":9}}`,
				`{"type":"message_stop"}`,
			)

			bdrkSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				authHeaders = append(authHeaders, r.Header.Get("Authorization"))
				paths = append(paths, r.URL.Path)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
				w.Header().Set("X-Amzn-Requestid", "amzn-e2e-req")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(bedrockStream)
			}))
			defer bdrkSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{{
					ID:           1,
					Name:         "bedrock-e2e",
					Platform:     capability.PlatformAnthropic,
					Type:         capability.ProviderTypeBedrock,
					BaseURL:      bdrkSrv.URL,
					Models:       []string{"claude-sonnet-4-6", "us.anthropic.claude-sonnet-4-6"},
					ModelMapping: map[string]string{"gpt-5": "claude-sonnet-4-6", "gpt-4o": "claude-sonnet-4-6"},
				}},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			status1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","stream":true,"input":"hello bedrock"}`))
			require.Equal(t, http.StatusOK, status1)
			require.Contains(t, string(resp1), "bedrock-e2e-ok")
			frames1, err := readAllSSEFrames(resp1)
			require.NoError(t, err)
			require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames1, testkit.GrammarOptions{}))

			status2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[2], []byte(`{"model":"gpt-4o","stream":false,"messages":[{"role":"user","content":"hello bedrock chat"}]}`))
			require.Equal(t, http.StatusOK, status2)
			require.Equal(t, "bedrock-e2e-ok", gjson.GetBytes(resp2, "choices.0.message.content").String())
			require.Equal(t, int64(44), gjson.GetBytes(resp2, "usage.prompt_tokens").Int())
			require.Equal(t, int64(9), gjson.GetBytes(resp2, "usage.completion_tokens").Int())

			mu.Lock()
			gotAuth := slices.Clone(authHeaders)
			gotPaths := slices.Clone(paths)
			mu.Unlock()
			require.Len(t, gotAuth, 2)
			require.True(t, strings.HasPrefix(gotAuth[0], "AWS4-HMAC-SHA256 "))
			require.Contains(t, gotPaths[0], "/invoke-with-response-stream")

			logs := lab.UsageLogs()
			require.Len(t, logs, 2)
			require.Equal(t, 44, logs[0].InputTokens)
			require.Equal(t, 9, logs[0].OutputTokens)
			require.Equal(t, 44, logs[1].InputTokens)
			require.Equal(t, 9, logs[1].OutputTokens)
		})

		t.Run("kimi_passback_unsigned_thinking_replay", func(t *testing.T) {
			plainCarrier, err := ir.FormatCarrier(ir.Carrier{
				Scheme: ir.SchemePlain,
				Text:   "Verify Kimi tool result before replying.",
			})
			require.NoError(t, err)
			kimiSSE := "event: message_start\n" +
				"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_kimi\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"k3\",\"content\":[],\"usage\":{\"input_tokens\":20,\"output_tokens\":0}}}\n\n" +
				"event: content_block_start\n" +
				"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
				"event: content_block_delta\n" +
				"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"kimi-passback-ok\"}}\n\n" +
				"event: content_block_stop\n" +
				"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
				"event: message_delta\n" +
				"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n" +
				"event: message_stop\n" +
				"data: {\"type\":\"message_stop\"}\n\n"

			kimiH := mockupstream.NewWithConfig(mockupstream.Config{
				Script: testkit.Script{Name: "kimi_passback", Default: testkit.Reply{Raw: kimiSSE}},
				Validation: testkit.RequestValidationOptions{
					StrictTopLevel: true,
					DialectRules:   []string{"passback"},
				},
			})
			kimiSrv := httptest.NewServer(kimiH)
			defer kimiSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{{
					ID:               1,
					Name:             "kimi-anthropic",
					Platform:         capability.PlatformKimi,
					Type:             capability.ProviderTypeAPIKey,
					BaseURL:          kimiSrv.URL,
					Model:            "k3",
					ModelMapping:     map[string]string{"gpt-5": "k3"},
					EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
				}},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			reqBody := []byte(fmt.Sprintf(`{
				"model":"gpt-5",
				"stream":true,
				"reasoning":{"effort":"high"},
				"input":[
					{"type":"message","role":"user","content":"check status"},
					{"type":"reasoning","id":"rs_kimi","summary":[],"encrypted_content":%q},
					{"type":"function_call","call_id":"call_k1","name":"ping_service","arguments":"{}"},
					{"type":"function_call_output","call_id":"call_k1","output":"healthy"}
				],
				"tools":[{"type":"function","name":"ping_service","parameters":{"type":"object","properties":{}}}]
			}`, plainCarrier))

			status, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], reqBody)
			require.Equal(t, http.StatusOK, status)
			require.Empty(t, kimiH.Violations())
			require.Contains(t, string(respBytes), "kimi-passback-ok")
			reqs := kimiH.Requests()
			require.Len(t, reqs, 1)
			require.Equal(t, "thinking", gjson.GetBytes(reqs[0].Body, "messages.1.content.0.type").String())
			require.Equal(t, "Verify Kimi tool result before replying.", gjson.GetBytes(reqs[0].Body, "messages.1.content.0.thinking").String())
			require.False(t, gjson.GetBytes(reqs[0].Body, "messages.1.content.0.signature").Exists())
		})

		t.Run("cursor_input_shaped_chat_completions_via_compat_anthropic", func(t *testing.T) {
			carrier, err := ir.FormatCarrier(ir.Carrier{
				Scheme: ir.SchemeAnthropic,
				Data:   "EqQBCursorAnthropicSig",
				Text:   "Plan patch before calling custom tool.",
			})
			require.NoError(t, err)

			cursorReplySSE := "event: message_start\n" +
				"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_cur_anth\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-opus-4-6\",\"content\":[],\"usage\":{\"input_tokens\":18,\"output_tokens\":0}}}\n\n" +
				"event: content_block_start\n" +
				"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_cur_1\",\"name\":\"apply_patch\",\"input\":{}}}\n\n" +
				"event: content_block_delta\n" +
				"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"input\\\":\\\"*** Begin Patch\\\"}\"}}\n\n" +
				"event: content_block_stop\n" +
				"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
				"event: message_delta\n" +
				"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":6}}\n\n" +
				"event: message_stop\n" +
				"data: {\"type\":\"message_stop\"}\n\n"

			anthSpec := findLabProviderKind("anthropic")
			upH := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "cursor_compat_anth", Default: testkit.Reply{Raw: cursorReplySSE}},
				Validation: anthSpec.validation,
			})
			upSrv := httptest.NewServer(upH)
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{{
					ID:               1,
					Name:             "opencode-claude",
					Platform:         capability.PlatformOpenCode,
					Type:             capability.ProviderTypeAPIKey,
					BaseURL:          upSrv.URL,
					Model:            "claude-opus-4-6",
					ModelMapping:     map[string]string{"gpt-4o": "claude-opus-4-6"},
					EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
				}},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			cursorReq := []byte(fmt.Sprintf(`{
				"model":"gpt-4o",
				"stream":true,
				"stream_options":{"include_usage":true},
				"reasoning":{"effort":"high"},
				"input":[
					{"type":"message","role":"user","content":"patch file"},
					{"type":"reasoning","id":"rs_cur","summary":[],"encrypted_content":%q},
					{"type":"custom_tool_call","call_id":"call_c1","name":"apply_patch","input":"*** Prior Patch"},
					{"type":"custom_tool_call_output","call_id":"call_c1","output":"applied"}
				],
				"tools":[{"type":"custom","name":"apply_patch","description":"apply unified patch"}]
			}`, carrier))

			status, ct, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[2], cursorReq)
			require.Equal(t, http.StatusOK, status)
			require.Contains(t, strings.ToLower(ct), "text/event-stream")
			require.Empty(t, upH.Violations())

			// Confirm the single-hop Exchange preserved trc1:anthropic on the wire without an extra Chat hop.
			reqs := upH.Requests()
			require.Len(t, reqs, 1)
			require.Equal(t, "EqQBCursorAnthropicSig", gjson.GetBytes(reqs[0].Body, "messages.1.content.0.signature").String())

			frames, fErr := readAllSSEFrames(respBytes)
			require.NoError(t, fErr)
			require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, frames, testkit.GrammarOptions{}))
			require.Contains(t, string(respBytes), `"apply_patch"`)
		})

		t.Run("ultra_effort_returns_client_400_without_provider_penalty", func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				platform string
			}{
				{name: "native_anthropic", platform: capability.PlatformAnthropic},
				{name: "compat_anthropic", platform: capability.PlatformOpenCode},
			} {
				t.Run(tc.name, func(t *testing.T) {
					upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						t.Errorf("upstream must not be contacted for rejected ultra effort on %s", tc.name)
						w.WriteHeader(http.StatusInternalServerError)
					}))
					defer upSrv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{{
							ID:               1,
							Name:             tc.name,
							Platform:         tc.platform,
							Type:             capability.ProviderTypeAPIKey,
							BaseURL:          upSrv.URL,
							Model:            "claude-opus-4-6",
							ModelMapping:     map[string]string{"gpt-5": "claude-opus-4-6"},
							EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
						}},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					status, _, respBytes := sendLabClientRequest(
						t,
						gwSrv,
						protocolLabClients[1],
						[]byte(`{"model":"gpt-5","stream":false,"reasoning":{"effort":"ultra"},"input":"hello"}`),
					)
					require.Equal(t, http.StatusBadRequest, status)
					require.Equal(t, "invalid_request_error", gjson.GetBytes(respBytes, "error.type").String())
				})
			}
		})
	})
}

// grokLabProviderSpecs returns the OAuth and API Key Grok Responses provider
// specifications shared across Grok T5 and T6 verification suites. Keeping
// these archetypes dedicated leaves the five-provider protocolLabProviders
// table and its 16-route activeLabRouteCombos matrix unchanged.
func grokLabProviderSpecs() []labProviderKindSpec {
	return []labProviderKindSpec{
		{
			kind:             "grok_oauth",
			platform:         capability.PlatformGrok,
			providerType:     capability.ProviderTypeOAuth,
			upstreamProtocol: protocol.ProtocolOpenAIResponses,
			upstreamModel:    "grok-4.5",
			enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"function-only"},
			},
			failoverStatus: 529,
		},
		{
			kind:             "grok_apikey",
			platform:         capability.PlatformGrok,
			providerType:     capability.ProviderTypeAPIKey,
			upstreamProtocol: protocol.ProtocolOpenAIResponses,
			upstreamModel:    "grok-4.5",
			enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"function-only"},
			},
			failoverStatus: 529,
		},
	}
}

// TestProtocolLabIRGrokChatResponsesMatrix exercises Stage 2 Step 3d's Grok
// Responses conversion paths (chat_via_grok_oauth, chat_via_grok_apikey,
// messages_via_grok_oauth, and messages_via_grok_apikey). It verifies all 33 canonical scenarios, usage parity, pre-byte and
// leading-frame SSE failover, mid-stream disconnect closure, OmitStoreAndInclude,
// OAuth vs API Key cache routing, trc1:xai: account binding and lossless HTTP 400
// retry, Cursor input-shaped Chat turns, client 400 validation without scheduler
// penalty, and cross-stage fallback from native Chat to Grok Responses.
func TestProtocolLabIRGrokChatResponsesMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	grokSpecs := grokLabProviderSpecs()
	var targetCombos []labRouteCombo
	for _, client := range []labClientSpec{protocolLabClients[2], protocolLabClients[0]} {
		for _, prov := range grokSpecs {
			targetCombos = append(targetCombos, labRouteCombo{
				slug:     client.slug + "_via_" + prov.kind,
				client:   client,
				provider: prov,
			})
		}
	}
	require.Len(t, targetCombos, 4)

	for _, combo := range targetCombos {
		t.Run(combo.slug, func(t *testing.T) {
			for _, scenario := range protocolLabScenarios {
				streamEntry, usageEntry := runLabScenarioCell(t, corpus, combo, scenario)
				require.NotEqual(t, labCellFail, streamEntry.Status,
					"IR Grok T5_STREAM failed for %s/%s: rules=%v", combo.slug, scenario, streamEntry.Rules)
				require.NotEqual(t, labCellFail, usageEntry.Status,
					"IR Grok T5_USAGE failed for %s/%s: rules=%v", combo.slug, scenario, usageEntry.Rules)
			}

			failoverEntry := runLabPreByteFailoverCell(t, corpus, combo)
			require.Equal(t, labCellPass, failoverEntry.Status,
				"IR Grok T5_FAILOVER failed for %s: rules=%v", combo.slug, failoverEntry.Rules)

			disconnectEntry := runLabMidstreamDisconnectCell(t, corpus, combo)
			require.Equal(t, labCellPass, disconnectEntry.Status,
				"IR Grok T5_MIDSTREAM_DISCONNECT failed for %s: rules=%v", combo.slug, disconnectEntry.Rules)

			errFmtEntry := runLabErrorFormatCell(t, corpus, combo)
			require.Equal(t, labCellPass, errFmtEntry.Status,
				"IR Grok T5_ERROR_FORMAT failed for %s: rules=%v", combo.slug, errFmtEntry.Rules)

			fromNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, false)
			require.Equal(t, labCellPass, fromNativeEntry.Status,
				"IR Grok T5_SWITCH_FROM_NATIVE failed for %s: rules=%v", combo.slug, fromNativeEntry.Rules)

			toNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, true)
			require.Equal(t, labCellPass, toNativeEntry.Status,
				"IR Grok T5_SWITCH_TO_NATIVE failed for %s: rules=%v", combo.slug, toNativeEntry.Rules)
		})
	}

	t.Run("omit_store_and_include_and_cache_routing", func(t *testing.T) {
		okSSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_grok_cache\",\"object\":\"response\",\"created_at\":1760000100,\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_gc\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"grok-cached-ok\"}]}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_grok_cache\",\"object\":\"response\",\"created_at\":1760000100,\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":48,\"output_tokens\":4,\"total_tokens\":52,\"input_tokens_details\":{\"cached_tokens\":32},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		for _, spec := range grokSpecs {
			t.Run(spec.kind, func(t *testing.T) {
				upH := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "grok_cache_" + spec.kind, Default: testkit.Reply{Raw: okSSE}},
					Validation: spec.validation,
				})
				upSrv := httptest.NewServer(upH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, spec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				reqBody := []byte(`{"model":"gpt-4o","stream":false,"instructions":"Always reply concisely","service_tier":"fast","prompt_cache_key":"grok-lab-session","messages":[{"role":"developer","content":"use plain text"},{"role":"user","content":"ping"}]}`)
				status, ct, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[2], reqBody)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, strings.ToLower(ct), "application/json")
				require.Equal(t, "grok-cached-ok", gjson.GetBytes(respBytes, "choices.0.message.content").String())
				require.Equal(t, int64(32), gjson.GetBytes(respBytes, "usage.prompt_tokens_details.cached_tokens").Int())

				reqs := upH.Requests()
				require.Len(t, reqs, 1)
				require.Empty(t, upH.Violations())
				sent := reqs[0].Body
				require.True(t, gjson.GetBytes(sent, "stream").Bool())
				require.False(t, gjson.GetBytes(sent, "store").Exists())
				require.False(t, gjson.GetBytes(sent, "include").Exists())
				require.Equal(t, "Always reply concisely", gjson.GetBytes(sent, "instructions").String())
				require.Equal(t, "priority", gjson.GetBytes(sent, "service_tier").String())
				cacheKey := gjson.GetBytes(sent, "prompt_cache_key").String()
				require.NotEmpty(t, cacheKey)
				require.Equal(t, cacheKey, reqs[0].Headers.Get("X-Grok-Conv-Id"))

				if spec.providerType == capability.ProviderTypeOAuth {
					require.Equal(t, "web_search", gjson.GetBytes(sent, "tools.0.type").String())
					require.Equal(t, "x_search", gjson.GetBytes(sent, "tools.1.type").String())
					require.Equal(t, "none", gjson.GetBytes(sent, "tool_choice").String())
				} else {
					require.False(t, gjson.GetBytes(sent, "tools").Exists())
				}
			})
		}
	})

	t.Run("xai_carrier_roundtrip_provider_binding_and_lossless_400_retry", func(t *testing.T) {
		grokOAuth := grokSpecs[0]
		turn1SSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_xai_t1\",\"object\":\"response\",\"created_at\":1760000110,\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"rs_xai_1\",\"type\":\"reasoning\",\"status\":\"completed\",\"summary\":[{\"type\":\"summary_text\",\"text\":\"checking order\"}],\"encrypted_content\":\"xai_turn1_blob_p301\"}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":2,\"output_index\":1,\"item\":{\"id\":\"fc_xai_1\",\"type\":\"function_call\",\"status\":\"completed\",\"call_id\":\"call_xai_1\",\"name\":\"lookup\",\"arguments\":\"{\\\"order_id\\\":9007199254740993}\"}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":3,\"response\":{\"id\":\"resp_xai_t1\",\"object\":\"response\",\"created_at\":1760000110,\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":20,\"output_tokens\":10,\"total_tokens\":30,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":4}}}}\n\n"
		turn2OKSSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_xai_t2\",\"object\":\"response\",\"created_at\":1760000111,\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_xai_2\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"order-verified\"}]}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_xai_t2\",\"object\":\"response\",\"created_at\":1760000111,\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":35,\"output_tokens\":5,\"total_tokens\":40,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		p301H := mockupstream.NewWithConfig(mockupstream.Config{
			Script: testkit.Script{
				Name: "grok_xai_p301",
				Turns: []testkit.Turn{
					{Reply: testkit.Reply{Raw: turn1SSE}},
					{Reply: testkit.Reply{Raw: turn2OKSSE}},
					{Fault: &testkit.Fault{
						Status:    http.StatusBadRequest,
						ErrorBody: `{"error":{"code":"invalid_encrypted_content","message":"Failed to decrypt encrypted_content"}}`,
					}},
					{Reply: testkit.Reply{Raw: turn2OKSSE}},
				},
			},
			Validation: grokOAuth.validation,
		})
		p302H := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "grok_xai_p302", Default: testkit.Reply{Raw: turn2OKSSE}},
			Validation: grokOAuth.validation,
		})
		p301Srv := httptest.NewServer(p301H)
		defer p301Srv.Close()
		p302Srv := httptest.NewServer(p302H)
		defer p302Srv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(301, 0, p301Srv.URL, grokOAuth)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		turn1Req := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"thinking":{"type":"enabled","budget_tokens":1024},"tools":[{"name":"lookup","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"verify order 9007199254740993"}]}`)
		s1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], turn1Req)
		require.Equal(t, http.StatusOK, s1)
		frames1, err := readAllSSEFrames(resp1)
		require.NoError(t, err)
		require.Contains(t, string(resp1), "trc1:xai:")

		turn2Req, err := testkit.SimulateNextTurn(protocol.ProtocolAnthropicMessages, testkit.TurnInput{
			PrevRequest:              turn1Req,
			ResponseFrames:           frames1,
			SynthesizeMissingOutputs: true,
			FollowUpUserText:         "continue",
		})
		require.NoError(t, err)
		turn2StreamReq := prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, turn2Req)

		// Same ProviderID 301 unpacks xai_turn1_blob_p301 onto the wire.
		s2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], turn2StreamReq)
		require.Equal(t, http.StatusOK, s2)
		require.Contains(t, string(resp2), "order-verified")
		require.Equal(t, "xai_turn1_blob_p301", gjson.GetBytes(p301H.Requests()[1].Body, "input.1.encrypted_content").String())
		require.False(t, gjson.GetBytes(p301H.Requests()[1].Body, "store").Exists())
		require.False(t, gjson.GetBytes(p301H.Requests()[1].Body, "include").Exists())

		// Same ProviderID 301 receiving HTTP 400 invalid_encrypted_content strips
		// encrypted_content losslessly via jsontext while keeping 9007199254740993 intact.
		s3, _, resp3 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], turn2StreamReq)
		require.Equal(t, http.StatusOK, s3)
		require.Contains(t, string(resp3), "order-verified")
		require.Len(t, p301H.Requests(), 4)
		require.Contains(t, string(p301H.Requests()[2].Body), "xai_turn1_blob_p301")
		require.NotContains(t, string(p301H.Requests()[3].Body), "xai_turn1_blob_p301")
		logs := lab.UsageLogs()
		require.NotEmpty(t, logs)
		require.NotNil(t, logs[len(logs)-1].ProtocolConversion)
		require.Positive(t, logs[len(logs)-1].ProtocolConversion.LossCounts[string(ir.LossReplayTokenDropped)])
		require.Contains(t, string(p301H.Requests()[3].Body), `\"order_id\":9007199254740993`)

		// Switching to ProviderID 302 drops ProviderID 301's xai carrier locally.
		lab.SetProviders(t, []labProvider{makeLabProviderSpec(302, 0, p302Srv.URL, grokOAuth)})
		s4, _, resp4 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], turn2StreamReq)
		require.Equal(t, http.StatusOK, s4)
		require.Contains(t, string(resp4), "order-verified")
		require.Len(t, p302H.Requests(), 1)
		require.NotContains(t, string(p302H.Requests()[0].Body), "xai_turn1_blob_p301")
		require.NotContains(t, string(p302H.Requests()[0].Body), "encrypted_content")

		// Also verify Cursor input-shaped /v1/chat/completions HTTP 400 xai retry on ProviderID 302.
		carrier302, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeXAI,
			ProviderID: 302,
			Data:       "xai_cursor_stale_302",
		})
		require.NoError(t, err)
		p302RetryH := mockupstream.NewWithConfig(mockupstream.Config{
			Script: testkit.Script{
				Name: "grok_cursor_xai_retry",
				Turns: []testkit.Turn{
					{Fault: &testkit.Fault{
						Status:    http.StatusBadRequest,
						ErrorBody: `{"error":{"code":"invalid_encrypted_content","message":"Failed to decrypt encrypted_content"}}`,
					}},
					{Reply: testkit.Reply{Raw: turn2OKSSE}},
				},
			},
			Validation: grokSpecs[1].validation,
		})
		p302RetrySrv := httptest.NewServer(p302RetryH)
		defer p302RetrySrv.Close()
		lab.SetProviders(t, []labProvider{makeLabProviderSpec(302, 0, p302RetrySrv.URL, grokSpecs[1])})

		cursorReplay := []byte(fmt.Sprintf(`{
			"model":"gpt-4o",
			"stream":false,
			"input":[
				{"type":"message","role":"user","content":"verify"},
				{"type":"reasoning","id":"rs_c","summary":[],"encrypted_content":%q},
				{"type":"function_call","call_id":"call_big","name":"lookup","arguments":"{\"order_id\":9007199254740993}"},
				{"type":"function_call_output","call_id":"call_big","output":"ok"}
			],
			"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]
		}`, carrier302))
		s5, _, resp5 := sendLabClientRequest(t, gwSrv, protocolLabClients[2], cursorReplay)
		require.Equal(t, http.StatusOK, s5)
		require.Equal(t, "order-verified", gjson.GetBytes(resp5, "choices.0.message.content").String())
		require.Len(t, p302RetryH.Requests(), 2)
		require.Contains(t, string(p302RetryH.Requests()[0].Body), "xai_cursor_stale_302")
		require.NotContains(t, string(p302RetryH.Requests()[1].Body), "xai_cursor_stale_302")
		logs = lab.UsageLogs()
		require.NotEmpty(t, logs)
		require.NotNil(t, logs[len(logs)-1].ProtocolConversion)
		require.Positive(t, logs[len(logs)-1].ProtocolConversion.LossCounts[string(ir.LossReplayTokenDropped)])
		require.Contains(t, string(p302RetryH.Requests()[1].Body), `\"order_id\":9007199254740993`)
	})

	t.Run("cursor_input_shaped_chat_streaming_and_buffered", func(t *testing.T) {
		grokAPIKey := grokSpecs[1]
		replySSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_cur_grok\",\"object\":\"response\",\"created_at\":1760000120,\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
			"event: response.output_item.added\n" +
			"data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_cg\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
			"event: response.content_part.added\n" +
			"data: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_cg\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"\"}}\n\n" +
			"event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_cg\",\"output_index\":0,\"content_index\":0,\"delta\":\"cursor-grok-ok\"}\n\n" +
			"event: response.output_text.done\n" +
			"data: {\"type\":\"response.output_text.done\",\"sequence_number\":4,\"item_id\":\"msg_cg\",\"output_index\":0,\"content_index\":0,\"text\":\"cursor-grok-ok\"}}\n\n" +
			"event: response.content_part.done\n" +
			"data: {\"type\":\"response.content_part.done\",\"sequence_number\":5,\"item_id\":\"msg_cg\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"cursor-grok-ok\"}}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0,\"item\":{\"id\":\"msg_cg\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"cursor-grok-ok\"}]}}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_cur_grok\",\"object\":\"response\",\"created_at\":1760000120,\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":14,\"output_tokens\":4,\"total_tokens\":18,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		upH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "cursor_grok_resp", Default: testkit.Reply{Raw: replySSE}},
			Validation: grokAPIKey.validation,
		})
		upSrv := httptest.NewServer(upH)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, grokAPIKey)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		streamReq := []byte(`{"model":"gpt-4o","stream":true,"prompt_cache_retention":"24h","stream_options":{"include_usage":true},"input":[{"type":"message","role":"user","content":"hello from cursor"}]}`)
		statusS, ctS, bytesS := sendLabClientRequest(t, gwSrv, protocolLabClients[2], streamReq)
		require.Equal(t, http.StatusOK, statusS)
		require.Contains(t, strings.ToLower(ctS), "text/event-stream")
		frames, err := readAllSSEFrames(bytesS)
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, frames, testkit.GrammarOptions{}))
		require.Empty(t, compareClientAndSettledUsage(protocol.ProtocolOpenAIChatCompletions, frames, lab.UsageLogs()))

		lab.ResetUsageLogs()
		bufReq := []byte(`{"model":"gpt-4o","stream":false,"prompt_cache_retention":"24h","stream_options":{"include_usage":true},"input":[{"type":"message","role":"user","content":"hello buffered"}]}`)
		statusB, ctB, bytesB := sendLabClientRequest(t, gwSrv, protocolLabClients[2], bufReq)
		require.Equal(t, http.StatusOK, statusB)
		require.Contains(t, strings.ToLower(ctB), "application/json")
		require.Equal(t, "cursor-grok-ok", gjson.GetBytes(bytesB, "choices.0.message.content").String())
		require.Len(t, lab.UsageLogs(), 1)

		reqs := upH.Requests()
		require.Len(t, reqs, 2)
		for _, rec := range reqs {
			require.True(t, gjson.GetBytes(rec.Body, "stream").Bool())
			require.False(t, gjson.GetBytes(rec.Body, "store").Exists())
			require.False(t, gjson.GetBytes(rec.Body, "include").Exists())
			require.False(t, gjson.GetBytes(rec.Body, "prompt_cache_retention").Exists())
			require.False(t, gjson.GetBytes(rec.Body, "stream_options").Exists())
		}
	})

	t.Run("client_validation_400_leaves_grok_provider_schedulable", func(t *testing.T) {
		grokAPIKey := grokSpecs[1]
		okSSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_after_400\",\"object\":\"response\",\"created_at\":1760000130,\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_a4\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"still-healthy\"}]}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_after_400\",\"object\":\"response\",\"created_at\":1760000130,\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":8,\"output_tokens\":2,\"total_tokens\":10,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		upH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "grok_health_guard", Default: testkit.Reply{Raw: okSSE}},
			Validation: grokAPIKey.validation,
		})
		upSrv := httptest.NewServer(upH)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, grokAPIKey)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status400, _, bytes400 := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[2],
			[]byte(`{"model":"gpt-4o","stream":false,"reasoning_effort":"ultra","messages":[{"role":"user","content":"hi"}]}`),
		)
		require.Equal(t, http.StatusBadRequest, status400)
		require.Equal(t, "invalid_request_error", gjson.GetBytes(bytes400, "error.type").String())
		require.Empty(t, upH.Requests(), "upstream must not be contacted on local 400 validation failure")
		require.Empty(t, lab.UsageLogs())

		// Provider 1 must remain immediately schedulable with zero cooldown penalty.
		statusOK, _, bytesOK := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[2],
			[]byte(`{"model":"gpt-4o","stream":false,"messages":[{"role":"user","content":"follow up"}]}`),
		)
		require.Equal(t, http.StatusOK, statusOK)
		require.Equal(t, "still-healthy", gjson.GetBytes(bytesOK, "choices.0.message.content").String())
		require.Len(t, upH.Requests(), 1)
	})

	t.Run("http200_sse_leading_frame_staging_failover", func(t *testing.T) {
		grokOAuth := grokSpecs[0]
		primarySSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_g1\",\"object\":\"response\",\"created_at\":1760000140,\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
			"event: response.failed\n" +
			"data: {\"type\":\"response.failed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_g1\",\"object\":\"response\",\"created_at\":1760000140,\"status\":\"failed\",\"model\":\"grok-4.5\",\"output\":[],\"error\":{\"code\":\"server_error\",\"message\":\"grok primary overloaded\"}}}\n\n"
		secondarySSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_g2\",\"object\":\"response\",\"created_at\":1760000141,\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
			"event: response.output_item.added\n" +
			"data: {\"type\":\"response.output_item.added\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_g2\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
			"event: response.content_part.added\n" +
			"data: {\"type\":\"response.content_part.added\",\"sequence_number\":2,\"item_id\":\"msg_g2\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"\"}}\n\n" +
			"event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":3,\"item_id\":\"msg_g2\",\"output_index\":0,\"content_index\":0,\"delta\":\"grok-fallback-p2-ok\"}\n\n" +
			"event: response.output_text.done\n" +
			"data: {\"type\":\"response.output_text.done\",\"sequence_number\":4,\"item_id\":\"msg_g2\",\"output_index\":0,\"content_index\":0,\"text\":\"grok-fallback-p2-ok\"}}\n\n" +
			"event: response.content_part.done\n" +
			"data: {\"type\":\"response.content_part.done\",\"sequence_number\":5,\"item_id\":\"msg_g2\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"grok-fallback-p2-ok\"}}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":6,\"output_index\":0,\"item\":{\"id\":\"msg_g2\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"grok-fallback-p2-ok\"}]}}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":7,\"response\":{\"id\":\"resp_g2\",\"object\":\"response\",\"created_at\":1760000141,\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":4,\"total_tokens\":16,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		primaryH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "grok_p1_sse_fail", Default: testkit.Reply{Raw: primarySSE}},
			Validation: grokOAuth.validation,
		})
		secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "grok_p2_sse_ok", Default: testkit.Reply{Raw: secondarySSE}},
			Validation: grokOAuth.validation,
		})
		p1Srv := httptest.NewServer(primaryH)
		defer p1Srv.Close()
		p2Srv := httptest.NewServer(secondaryH)
		defer p2Srv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{
				makeLabProviderSpec(1, 0, p1Srv.URL, grokOAuth),
				makeLabProviderSpec(2, 1, p2Srv.URL, grokOAuth),
			},
			MaxProviderSwitches: 2,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status, ct, respBytes := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[2],
			[]byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"ping"}]}`),
		)
		require.Equal(t, http.StatusOK, status)
		require.Contains(t, strings.ToLower(ct), "text/event-stream")
		require.Contains(t, string(respBytes), "grok-fallback-p2-ok")
		require.NotContains(t, string(respBytes), "resp_g1")
		frames, err := readAllSSEFrames(respBytes)
		require.NoError(t, err)
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, frames, testkit.GrammarOptions{}))
		logs := lab.UsageLogs()
		require.Len(t, logs, 1)
		require.Equal(t, int64(2), logs[0].ProviderID)
	})

	t.Run("stage_fallback_from_native_chat_to_grok_responses", func(t *testing.T) {
		rawChatSpec := findLabProviderKind("openai_chat")
		grokOAuth := grokSpecs[0]

		rawChatFailH := mockupstream.NewWithConfig(mockupstream.Config{
			Script: testkit.Script{
				Name: "raw_chat_529",
				Turns: []testkit.Turn{{
					Fault: &testkit.Fault{
						Status:    529,
						ErrorBody: formatUpstreamJSONError(protocol.ProtocolOpenAIChatCompletions, 529, "chat primary overloaded"),
					},
				}},
			},
			Validation: rawChatSpec.validation,
		})
		grokRespOKSSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_stage2\",\"object\":\"response\",\"created_at\":1760000150,\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_st2\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"grok-stage2-ok\"}]}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_stage2\",\"object\":\"response\",\"created_at\":1760000150,\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":3,\"total_tokens\":13,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"
		grokRespH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "grok_stage2_ok", Default: testkit.Reply{Raw: grokRespOKSSE}},
			Validation: grokOAuth.validation,
		})
		chatSrv := httptest.NewServer(rawChatFailH)
		defer chatSrv.Close()
		grokSrv := httptest.NewServer(grokRespH)
		defer grokSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{
				makeLabProviderSpec(1, 0, chatSrv.URL, rawChatSpec),
				makeLabProviderSpec(2, 1, grokSrv.URL, grokOAuth),
			},
			MaxProviderSwitches: 2,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status, _, respBytes := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[2],
			[]byte(`{"model":"gpt-4o","stream":false,"stop":"END","messages":[{"role":"developer","content":"strict"},{"role":"user","content":"stage fallback"}]}`),
		)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "grok-stage2-ok", gjson.GetBytes(respBytes, "choices.0.message.content").String())
		require.Len(t, rawChatFailH.Requests(), 1)
		require.Len(t, grokRespH.Requests(), 1)
		require.Empty(t, grokRespH.Violations())
		require.False(t, gjson.GetBytes(grokRespH.Requests()[0].Body, "store").Exists())
		require.False(t, gjson.GetBytes(grokRespH.Requests()[0].Body, "include").Exists())
		logs := lab.UsageLogs()
		require.Len(t, logs, 1)
		require.Equal(t, int64(2), logs[0].ProviderID)
	})
}

// TestProtocolLabIRNativeReplayMatrix exercises Stage 2 Step 4's native
// PrepareNativeReplay wiring across all native Messages paths (Anthropic API Key
// passthrough, OAuth, Vertex service_account, AWS Bedrock, compatible OpenCode
// Claude, Kimi/DeepSeek passback, and /v1/messages/count_tokens) and all native
// Responses paths (OpenAI and Grok HTTP Responses, /v1/responses/input_tokens,
// dedicated WebSocket, pooled WebSocket, and WebSocket HTTP bridge). It verifies
// switch-disabled preservation, byte-for-byte identity on non-carrier payloads
// and session.update frames, same-scheme/same-account restoration, and
// foreign/cross-account carrier stripping with zero T5 violations.
func TestProtocolLabIRNativeReplayMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()

	t.Run("canonical_reasoning_carrier_native_scenarios", func(t *testing.T) {
		for _, combo := range activeLabRouteCombos() {
			if combo.client.protocol != combo.provider.upstreamProtocol {
				continue
			}
			if combo.client.protocol != protocol.ProtocolAnthropicMessages &&
				combo.client.protocol != protocol.ProtocolOpenAIResponses {
				continue
			}
			t.Run(combo.slug, func(t *testing.T) {
				streamOn, usageOn := runLabScenarioCell(t, corpus, combo, "reasoning_carrier_native")
				require.Equal(t, labCellPass, streamOn.Status,
					"IR native replay failed for %s: rules=%v", combo.slug, streamOn.Rules)
				require.Equal(t, labCellPass, usageOn.Status,
					"IR native replay usage failed for %s: rules=%v", combo.slug, usageOn.Rules)
			})
		}
	})

	t.Run("mixed_group_converted_then_native_messages_variants", func(t *testing.T) {
		anthCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "EqQBMixedGroupSig",
			Text:   "Canonical Anthropic thought across mixed group.",
		})
		require.NoError(t, err)
		plainCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemePlain,
			Text:   "Plain tool-turn thought from Chat turn 1.",
		})
		require.NoError(t, err)
		foreignOpenAICarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeOpenAI,
			Data:       "gAAAA_mixed_foreign",
			ProviderID: 101,
		})
		require.NoError(t, err)

		msgSSE := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_nat_ok\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"usage\":{\"input_tokens\":14,\"output_tokens\":0}}}\n\n" +
			"event: content_block_start\n" +
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\n" +
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"native-msg-ok\"}}\n\n" +
			"event: content_block_stop\n" +
			"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\n" +
			"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n" +
			"event: message_stop\n" +
			"data: {\"type\":\"message_stop\"}\n\n"

		replayReq := []byte(fmt.Sprintf(`{
			"model":"claude-sonnet-4-5",
			"max_tokens":2048,
			"stream":true,
			"thinking":{"type":"enabled","budget_tokens":1024},
			"messages":[
				{"role":"user","content":"keep literal trc1:plain:demo"},
				{"role":"assistant","content":[
					{"type":"thinking","thinking":"stub","signature":%q},
					{"type":"thinking","thinking":"","signature":%q},
					{"type":"thinking","thinking":"foreign","signature":%q},
					{"type":"text","text":"prior reply"}
				]},
				{"role":"user","content":"continue natively"}
			]
		}`, anthCarrier, plainCarrier, foreignOpenAICarrier))

		for _, tc := range []struct {
			name         string
			platform     string
			providerType string
			model        string
			extra        map[string]any
			passback     bool
			bedrock      bool
		}{
			{name: "anthropic_oauth", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeOAuth, model: "claude-sonnet-4-6"},
			{name: "vertex_service_account", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeServiceAccount, model: "claude-sonnet-4-6"},
			{name: "aws_bedrock", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeBedrock, model: "claude-sonnet-4-6", bedrock: true},
			{name: "opencode_claude", platform: capability.PlatformOpenCode, providerType: capability.ProviderTypeAPIKey, model: "claude-sonnet-4-6"},
			{name: "kimi_passback", platform: capability.PlatformKimi, providerType: capability.ProviderTypeAPIKey, model: "k3", passback: true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				var mu sync.Mutex
				var capturedBodies [][]byte
				var upSrv *httptest.Server
				if tc.bedrock {
					bedrockStream := buildLabBedrockChunkStream(
						`{"type":"message_start","message":{"id":"msg_bdrk_nat","type":"message","role":"assistant","model":"us.anthropic.claude-sonnet-4-6","content":[],"usage":{"input_tokens":0,"output_tokens":0}}}`,
						`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
						`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"native-msg-ok"}}`,
						`{"type":"content_block_stop","index":0}`,
						`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"amazon-bedrock-invocationMetrics":{"inputTokenCount":14,"outputTokenCount":3}}`,
						`{"type":"message_stop"}`,
					)
					upSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						raw, rErr := io.ReadAll(r.Body)
						require.NoError(t, rErr)
						mu.Lock()
						capturedBodies = append(capturedBodies, raw)
						mu.Unlock()
						w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write(bedrockStream)
					}))
				} else {
					upSrv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						raw, rErr := io.ReadAll(r.Body)
						require.NoError(t, rErr)
						mu.Lock()
						capturedBodies = append(capturedBodies, raw)
						mu.Unlock()
						w.Header().Set("Content-Type", "text/event-stream")
						w.WriteHeader(http.StatusOK)
						_, _ = io.WriteString(w, msgSSE)
					}))
				}
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{{
						ID:               201,
						Name:             tc.name,
						Platform:         tc.platform,
						Type:             tc.providerType,
						BaseURL:          upSrv.URL,
						Models:           []string{tc.model, "us.anthropic." + tc.model},
						ModelMapping:     map[string]string{"claude-sonnet-4-5": tc.model},
						EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
						Extra:            tc.extra,
					}},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				status, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[0], replayReq)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, string(respBytes), "native-msg-ok")

				mu.Lock()
				require.Len(t, capturedBodies, 1)
				wireBody := capturedBodies[0]
				mu.Unlock()

				require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolAnthropicMessages, wireBody))
				require.Contains(t, string(wireBody), "trc1:plain:demo")
				if tc.passback {
					require.Equal(t, "Canonical Anthropic thought across mixed group.", gjson.GetBytes(wireBody, "messages.1.content.0.thinking").String())
					require.False(t, gjson.GetBytes(wireBody, "messages.1.content.0.signature").Exists())
					require.Equal(t, "Plain tool-turn thought from Chat turn 1.", gjson.GetBytes(wireBody, "messages.1.content.1.thinking").String())
					require.False(t, gjson.GetBytes(wireBody, "messages.1.content.1.signature").Exists())
					require.Equal(t, "text", gjson.GetBytes(wireBody, "messages.1.content.2.type").String())
				} else {
					// OAuth mimicry may prepend a system block, while messages[1] keeps assistant blocks in order.
					require.Equal(t, "EqQBMixedGroupSig", gjson.GetBytes(wireBody, "messages.1.content.0.signature").String())
					require.Equal(t, "Canonical Anthropic thought across mixed group.", gjson.GetBytes(wireBody, "messages.1.content.0.thinking").String())
					require.Equal(t, "text", gjson.GetBytes(wireBody, "messages.1.content.1.type").String())
				}
			})
		}
	})

	t.Run("mixed_group_responses_same_account_vs_cross_account", func(t *testing.T) {
		oaCarrier101, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeOpenAI,
			Data:       "gAAAA_lab_acct_101",
			ProviderID: 101,
		})
		require.NoError(t, err)
		xaiCarrier301, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeXAI,
			Data:       "xai_lab_acct_301",
			ProviderID: 301,
		})
		require.NoError(t, err)

		respSSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_nat_ok\",\"object\":\"response\",\"created_at\":1760000200,\"status\":\"in_progress\",\"model\":\"gpt-5.4\",\"output\":[]}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_nat\",\"type\":\"message\",\"status\":\"completed\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"annotations\":[],\"text\":\"native-resp-ok\"}]}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_nat_ok\",\"object\":\"response\",\"created_at\":1760000200,\"status\":\"completed\",\"model\":\"gpt-5.4\",\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":2,\"total_tokens\":12,\"input_tokens_details\":{\"cached_tokens\":0},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n"

		reqBody := []byte(fmt.Sprintf(`{
			"model":"gpt-5",
			"stream":true,
			"store":false,
			"input":[
				{"type":"message","role":"user","content":"literal trc1:openai:demo"},
				{"type":"reasoning","id":"rs_oa","summary":[{"type":"summary_text","text":"oa summary"}],"encrypted_content":%q},
				{"type":"reasoning","id":"rs_xai","summary":[{"type":"summary_text","text":"xai summary"}],"encrypted_content":%q},
				{"type":"message","role":"assistant","content":[{"type":"output_text","text":"prior"}]}
			]
		}`, oaCarrier101, xaiCarrier301))

		for _, tc := range []struct {
			name       string
			providerID int64
			spec       labProviderKindSpec
			wantIndex1 string
			wantIndex2 string
		}{
			{name: "openai_same_account", providerID: 101, spec: findLabProviderKind("openai"), wantIndex1: "gAAAA_lab_acct_101", wantIndex2: ""},
			{name: "openai_cross_account", providerID: 202, spec: findLabProviderKind("openai"), wantIndex1: "", wantIndex2: ""},
			{name: "grok_oauth_same_account", providerID: 301, spec: grokLabProviderSpecs()[0], wantIndex1: "", wantIndex2: "xai_lab_acct_301"},
			{name: "grok_apikey_cross_account", providerID: 302, spec: grokLabProviderSpecs()[1], wantIndex1: "", wantIndex2: ""},
		} {
			t.Run(tc.name, func(t *testing.T) {
				upH := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: tc.name, Default: testkit.Reply{Raw: respSSE}},
					Validation: tc.spec.validation,
				})
				upSrv := httptest.NewServer(upH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(tc.providerID, 0, upSrv.URL, tc.spec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				status, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], reqBody)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, string(respBytes), "native-resp-ok")
				require.Empty(t, upH.Violations())
				require.Len(t, upH.Requests(), 1)
				wireBody := upH.Requests()[0].Body
				require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, wireBody))
				require.Equal(t, tc.wantIndex1, gjson.GetBytes(wireBody, "input.1.encrypted_content").String())
				require.Equal(t, "oa summary", gjson.GetBytes(wireBody, "input.1.summary.0.text").String())
				require.Equal(t, tc.wantIndex2, gjson.GetBytes(wireBody, "input.2.encrypted_content").String())
				require.Equal(t, "xai summary", gjson.GetBytes(wireBody, "input.2.summary.0.text").String())
			})
		}
	})

	t.Run("auxiliary_count_tokens_and_input_tokens_preflight", func(t *testing.T) {
		anthCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "EqQBCountSig",
			Text:   "Counted thought.",
		})
		require.NoError(t, err)
		oaCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeOpenAI,
			Data:       "gAAAA_count_101",
			ProviderID: 101,
		})
		require.NoError(t, err)

		var mu sync.Mutex
		var countMsgBodies [][]byte
		var inputTokBodies [][]byte
		upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, rErr := io.ReadAll(r.Body)
			require.NoError(t, rErr)
			mu.Lock()
			if strings.Contains(r.URL.Path, "count_tokens") {
				countMsgBodies = append(countMsgBodies, raw)
				mu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"input_tokens":42}`)
				return
			}
			inputTokBodies = append(inputTokBodies, raw)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"object":"response.input_tokens","input_tokens":37}`)
		}))
		defer upSrv.Close()

		anthSpec := findLabProviderKind("anthropic")
		oaSpec := findLabProviderKind("openai")

		postJSON := func(srv *httptest.Server, path string, payload []byte) (int, []byte) {
			req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+path, bytes.NewReader(payload))
			require.NoError(t, reqErr)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer lab-api-key")
			resp, doErr := srv.Client().Do(req)
			require.NoError(t, doErr)
			raw, readErr := io.ReadAll(resp.Body)
			require.NoError(t, resp.Body.Close())
			require.NoError(t, readErr)
			return resp.StatusCode, raw
		}

		// 1. Anthropic /v1/messages/count_tokens always unpacks matching carriers and drops foreign ones.
		countReqBody := []byte(fmt.Sprintf(`{
			"model":"claude-sonnet-4-5",
			"thinking":{"type":"enabled","budget_tokens":1024},
			"messages":[
				{"role":"user","content":"literal trc1:plain:demo"},
				{"role":"assistant","content":[
					{"type":"thinking","thinking":"stub","signature":%q},
					{"type":"thinking","thinking":"drop","signature":%q},
					{"type":"text","text":"ok"}
				]},
				{"role":"user","content":"count me"}
			]
		}`, anthCarrier, oaCarrier))

		countLab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, anthSpec)},
		})
		countGWSrv := httptest.NewServer(countLab)
		status, _ := postJSON(countGWSrv, "/v1/messages/count_tokens", countReqBody)
		countGWSrv.Close()
		require.Equal(t, http.StatusOK, status)

		mu.Lock()
		require.Len(t, countMsgBodies, 1)
		gotCountOn := countMsgBodies[0]
		mu.Unlock()
		require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolAnthropicMessages, gotCountOn))
		require.Equal(t, "EqQBCountSig", gjson.GetBytes(gotCountOn, "messages.1.content.0.signature").String())
		require.Equal(t, "Counted thought.", gjson.GetBytes(gotCountOn, "messages.1.content.0.thinking").String())
		require.Equal(t, "text", gjson.GetBytes(gotCountOn, "messages.1.content.1.type").String())
		require.Contains(t, gjson.GetBytes(gotCountOn, "messages.0.content").String(), "literal trc1:plain:demo")

		// 2. Official OpenAI /v1/responses/input_tokens: route api.openai.com to upSrv via
		// HostOverrides so ForwardResponsesInputTokens dispatches real upstream HTTP requests.
		inputTokBody := []byte(fmt.Sprintf(`{
			"model":"gpt-5",
			"store":false,
			"input":[
				{"type":"message","role":"user","content":"literal trc1:plain:demo"},
				{"type":"reasoning","id":"rs_same","summary":[{"type":"summary_text","text":"s"}],"encrypted_content":%q},
				{"type":"reasoning","id":"rs_foreign","summary":[],"encrypted_content":%q},
				{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}
			]
		}`, oaCarrier, anthCarrier))

		runOfficialInputTokens := func(providerID int64) []byte {
			lab := newProtocolLabRouter(t, labOptions{
				Providers:     []labProvider{makeLabProviderSpec(providerID, 0, "https://api.openai.com/v1", oaSpec)},
				HostOverrides: map[string]string{"api.openai.com": upSrv.URL},
			})
			srv := httptest.NewServer(lab)
			defer srv.Close()
			status, respRaw := postJSON(srv, "/v1/responses/input_tokens", inputTokBody)
			require.Equal(t, http.StatusOK, status)
			require.Equal(t, int64(37), gjson.GetBytes(respRaw, "input_tokens").Int())
			return respRaw
		}

		runOfficialInputTokens(101)
		runOfficialInputTokens(202)

		mu.Lock()
		require.Len(t, inputTokBodies, 2)
		gotInTokSame := inputTokBodies[0]
		gotInTokCross := inputTokBodies[1]
		mu.Unlock()

		// Same account (ID=101) unpacks oaCarrier, drops empty foreign anthCarrier, and preserves user literal "trc1:".
		require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, gotInTokSame))
		require.Equal(t, "gAAAA_count_101", gjson.GetBytes(gotInTokSame, "input.1.encrypted_content").String())
		require.Equal(t, "message", gjson.GetBytes(gotInTokSame, "input.2.type").String())
		require.Contains(t, gjson.GetBytes(gotInTokSame, "input.0.content").String(), "literal trc1:plain:demo")

		// Cross account (ID=202) under store=false strips encrypted_content and synthetic id while keeping summary.
		require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, gotInTokCross))
		require.Equal(t, "reasoning", gjson.GetBytes(gotInTokCross, "input.1.type").String())
		require.Equal(t, "s", gjson.GetBytes(gotInTokCross, "input.1.summary.0.text").String())
		require.False(t, gjson.GetBytes(gotInTokCross, "input.1.encrypted_content").Exists())
		require.False(t, gjson.GetBytes(gotInTokCross, "input.1.id").Exists())
		require.Equal(t, "message", gjson.GetBytes(gotInTokCross, "input.2.type").String())

		// 3. Custom relay local estimation (non-api.openai.com BaseURL): under store=false on a
		// cross-account provider, stripping the synthetic reasoning item id lowers the local token estimate.
		localEstimateBody := []byte(fmt.Sprintf(`{
			"model":"gpt-5",
			"store":false,
			"input":[
				{"type":"message","role":"user","content":"literal trc1:plain:demo"},
				{"type":"reasoning","id":"rs_synthetic_cross_account_identifier_counted_when_unstripped_0123456789","summary":[{"type":"summary_text","text":"s"}],"encrypted_content":%q},
				{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}
			]
		}`, oaCarrier))
		runRelayEstimate := func(providerID int64) int64 {
			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(providerID, 0, upSrv.URL, oaSpec)},
			})
			srv := httptest.NewServer(lab)
			defer srv.Close()
			status, respRaw := postJSON(srv, "/v1/responses/input_tokens", localEstimateBody)
			require.Equal(t, http.StatusOK, status)
			return gjson.GetBytes(respRaw, "input_tokens").Int()
		}
		tokensSame := runRelayEstimate(101)
		tokensCross := runRelayEstimate(202)
		require.Positive(t, tokensCross)
		require.Less(t, tokensCross, tokensSame)

		// Local relay estimates must not send additional HTTP requests to upSrv.
		mu.Lock()
		require.Len(t, inputTokBodies, 2)
		mu.Unlock()
	})

	t.Run("websocket_dedicated_pooled_and_http_bridge", func(t *testing.T) {
		oaCarrier401, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeOpenAI,
			Data:       "gAAAA_ws_lab_401",
			ProviderID: 401,
		})
		require.NoError(t, err)
		xaiCarrier501, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeXAI,
			Data:       "xai_ws_lab_501",
			ProviderID: 501,
		})
		require.NoError(t, err)

		for _, wsMode := range []string{"per_session", "pooled"} {
			t.Run("ws_"+wsMode, func(t *testing.T) {
				var mu sync.Mutex
				var upFrames [][]byte
				upWSSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, acceptErr := websocket.Accept(w, r, nil)
					if acceptErr != nil {
						return
					}
					defer func() { _ = conn.CloseNow() }()
					for {
						_, data, readErr := conn.Read(r.Context())
						if readErr != nil {
							return
						}
						mu.Lock()
						upFrames = append(upFrames, append([]byte(nil), data...))
						mu.Unlock()
						if gjson.GetBytes(data, "type").String() == "session.update" {
							_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"session.updated","session":{"id":"sess_ws"}}`))
							continue
						}
						_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_ws_1","object":"response","status":"in_progress","model":"gpt-5.4","output":[]}}`))
						_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.completed","sequence_number":1,"response":{"id":"resp_ws_1","object":"response","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":6,"output_tokens":2,"total_tokens":8}}}`))
					}
				}))
				defer upWSSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{{
						ID:               401,
						Name:             "ws-" + wsMode,
						Platform:         capability.PlatformOpenAI,
						Type:             capability.ProviderTypeAPIKey,
						BaseURL:          upWSSrv.URL,
						Models:           []string{"gpt-5.4"},
						ModelMapping:     map[string]string{"gpt-5": "gpt-5.4"},
						EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
						Extra:            map[string]any{"responses_ws_connection_mode": wsMode},
					}},
					AllowedProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				wsURL := "ws" + strings.TrimPrefix(gwSrv.URL, "http") + "/v1/responses"
				dialCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				clientConn, _, dialErr := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
					HTTPHeader: http.Header{"Authorization": []string{"Bearer lab-api-key"}},
				})
				require.NoError(t, dialErr)
				defer func() { _ = clientConn.CloseNow() }()

				turnFrame := []byte(fmt.Sprintf(`{"type":"response.create","model":"gpt-5","store":false,"input":[{"type":"message","role":"user","content":"literal trc1:plain:demo"},{"type":"reasoning","id":"rs_same","summary":[{"type":"summary_text","text":"ws summary"}],"encrypted_content":%q},{"type":"reasoning","id":"rs_foreign","summary":[],"encrypted_content":%q},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`, oaCarrier401, xaiCarrier501))
				require.NoError(t, clientConn.Write(dialCtx, websocket.MessageText, turnFrame))
				for {
					_, msg, rErr := clientConn.Read(dialCtx)
					require.NoError(t, rErr)
					if gjson.GetBytes(msg, "type").String() == "response.completed" {
						break
					}
				}

				if wsMode == "per_session" {
					sessUpdate := []byte("{\n  \"type\": \"session.update\",\n  \"session\": {\"instructions\": \"keep trc1:openai:eyJkIjoiZiJ9 untouched\"}\n}")
					require.NoError(t, clientConn.Write(dialCtx, websocket.MessageText, sessUpdate))
					_, updAck, rErr := clientConn.Read(dialCtx)
					require.NoError(t, rErr)
					require.Equal(t, "session.updated", gjson.GetBytes(updAck, "type").String())
				}
				// Ignore best-effort normal WebSocket closure errors after the terminal response.completed frame has already been verified.
				_ = clientConn.Close(websocket.StatusNormalClosure, "done")

				mu.Lock()
				gotFrames := slices.Clone(upFrames)
				mu.Unlock()
				require.NotEmpty(t, gotFrames)
				require.Equal(t, "gAAAA_ws_lab_401", gjson.GetBytes(gotFrames[0], "input.1.encrypted_content").String())
				require.Equal(t, "message", gjson.GetBytes(gotFrames[0], "input.2.type").String())
				require.Contains(t, gjson.GetBytes(gotFrames[0], "input.0.content").String(), "trc1:plain:demo")
				if wsMode == "per_session" {
					require.Len(t, gotFrames, 2)
					require.Contains(t, string(gotFrames[1]), "trc1:openai:eyJkIjoiZiJ9")
				}
			})
		}

		t.Run("ws_http_bridge_grok", func(t *testing.T) {
			grokSSE := "data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_gbr\",\"object\":\"response\",\"status\":\"in_progress\",\"model\":\"grok-4.5\",\"output\":[]}}\n\n" +
				"data: {\"type\":\"response.completed\",\"sequence_number\":1,\"response\":{\"id\":\"resp_gbr\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":5,\"output_tokens\":2,\"total_tokens\":7}}}\n\n"
			grokSpec := grokLabProviderSpecs()[0]
			upH := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "ws_grok_bridge", Default: testkit.Reply{Raw: grokSSE}},
				Validation: grokSpec.validation,
			})
			upSrv := httptest.NewServer(upH)
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers:        []labProvider{makeLabProviderSpec(501, 0, upSrv.URL, grokSpec)},
				AllowedProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			wsURL := "ws" + strings.TrimPrefix(gwSrv.URL, "http") + "/v1/responses"
			dialCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			clientConn, _, dialErr := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
				HTTPHeader: http.Header{"Authorization": []string{"Bearer lab-api-key"}},
			})
			require.NoError(t, dialErr)
			defer func() { _ = clientConn.CloseNow() }()

			bridgeFrame := []byte(fmt.Sprintf(`{"type":"response.create","model":"gpt-5","input":[{"type":"message","role":"user","content":"literal trc1:xai:demo"},{"type":"reasoning","id":"rs_xai","summary":[{"type":"summary_text","text":"grok ws summary"}],"encrypted_content":%q},{"type":"reasoning","id":"rs_oa","summary":[],"encrypted_content":%q},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`, xaiCarrier501, oaCarrier401))
			require.NoError(t, clientConn.Write(dialCtx, websocket.MessageText, bridgeFrame))
			for {
				_, msg, rErr := clientConn.Read(dialCtx)
				require.NoError(t, rErr)
				if gjson.GetBytes(msg, "type").String() == "response.completed" {
					break
				}
			}
			_ = clientConn.Close(websocket.StatusNormalClosure, "done")

			require.Empty(t, upH.Violations())
			require.Len(t, upH.Requests(), 1)
			wireBody := upH.Requests()[0].Body
			require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, wireBody))
			require.Equal(t, "xai_ws_lab_501", gjson.GetBytes(wireBody, "input.1.encrypted_content").String())
			require.Equal(t, "message", gjson.GetBytes(wireBody, "input.2.type").String())
		})
	})
}

// activeLabRouteCombos enumerates the 16 supported (client protocol x provider kind)
// route pairs across protocolLabClients and protocolLabProviders in deterministic
// order. Gemini GenerateContent clients pair only with Gemini upstreams because
// the gateway intentionally disables cross-protocol conversion for Gemini ingress.
func activeLabRouteCombos() []labRouteCombo {
	var out []labRouteCombo
	for _, client := range protocolLabClients {
		for _, prov := range protocolLabProviders {
			if client.protocol == protocol.ProtocolGeminiGenerateContent && prov.upstreamProtocol != protocol.ProtocolGeminiGenerateContent {
				continue
			}
			out = append(out, labRouteCombo{
				slug:     client.slug + "_via_" + prov.kind,
				client:   client,
				provider: prov,
			})
		}
	}
	return out
}

// findLabProviderKind looks up and returns the labProviderKindSpec matching
// kind inside protocolLabProviders. Passing an unrecognized kind slug panics
// immediately because all T5 provider archetypes are statically registered.
func findLabProviderKind(kind string) labProviderKindSpec {
	for _, p := range protocolLabProviders {
		if p.kind == kind {
			return p
		}
	}
	panic("unknown lab provider kind: " + kind)
}

// runLabScenarioCell executes one canonical wire scenario over combo and returns
// its T5_STREAM and T5_USAGE cell results. Scenarios without matching client
// request or upstream stream fixtures, as well as fault scenarios on T5_USAGE,
// return labCellSkip.
func runLabScenarioCell(t *testing.T, corpus *testkit.Corpus, combo labRouteCombo, scenario string) (labCellResult, labCellResult) {
	t.Helper()
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
		Protocol: combo.client.protocol,
		Kind:     testkit.FixtureRequest,
		Scenario: scenario,
	})
	require.NoError(t, err)
	streamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.provider.upstreamProtocol,
		Kind:     testkit.FixtureStream,
		Scenario: scenario,
	})
	require.NoError(t, err)

	// reasoning_carrier_native applies only to native passthrough routes.
	if scenario == "reasoning_carrier_native" && combo.client.protocol != combo.provider.upstreamProtocol {
		baseStream.Status = labCellSkip
		baseUsage.Status = labCellSkip
		return baseStream, baseUsage
	}
	if len(reqMatches) == 0 || len(streamMatches) == 0 {
		baseStream.Status = labCellSkip
		baseUsage.Status = labCellSkip
		return baseStream, baseUsage
	}

	valOpts := validationOptionsForLabCombo(combo, reqMatches[0].Data)
	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    scenario,
			Default: testkit.Reply{Raw: streamMatches[0].Path},
		},
		Validation: valOpts,
	})
	upSrv := newLabMockServer(combo.provider, mockH)
	defer upSrv.Close()

	providerID := int64(101)
	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(providerID, 0, upSrv.URL, combo.provider)},
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

	if scenario == "reasoning_carrier_native" && combo.client.protocol == combo.provider.upstreamProtocol {
		for _, rec := range upSrv.Requests() {
			if hasUnstrippedCarrierInTokenPaths(combo.provider.upstreamProtocol, rec.Body) {
				streamRules = appendUniqueLabRule(streamRules, ruleUnstrippedTRC1Carrier)
			}
			// Literal "trc1:" inside user prompt text must never be stripped.
			require.Contains(t, string(rec.Body), "trc1:", "user literal trc1: text must remain intact")
		}
	}

	isFaultScenario := scenario == "stream_truncated" || scenario == "stream_error_event"
	var frames []sse.Frame
	isEmptyHTTP200 := status == http.StatusOK && len(respBody) == 0
	isValidSSE200 := status == http.StatusOK && (strings.Contains(strings.ToLower(contentType), "text/event-stream") || isEmptyHTTP200)
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
		grammarViolations := testkit.ValidateStreamGrammar(
			combo.client.protocol,
			frames,
			testkit.GrammarOptions{AllowAbnormal: isFaultScenario},
		)
		for _, gv := range grammarViolations {
			streamRules = appendUniqueLabRule(streamRules, gv.Rule)
		}
		if isFaultScenario && !hasLabAbnormalTerminalErrorFrame(combo.client.protocol, frames) {
			streamRules = appendUniqueLabRule(streamRules, ruleMissingFaultErrorTermination)
		}
	}

	if len(streamRules) > 0 {
		baseStream.Status = labCellFail
		baseStream.Rules = streamRules
	} else {
		baseStream.Status = labCellPass
	}

	if isFaultScenario {
		baseUsage.Status = labCellSkip
		return baseStream, baseUsage
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

// runLabPreByteFailoverCell executes a two-provider failover exchange where
// Provider 1 fails before writing any semantic output. The gateway must switch
// to Provider 2, deliver a valid client SSE stream with zero leaked bytes from
// Provider 1, and settle exactly one UsageLog attributed to Provider 2.
func runLabPreByteFailoverCell(t *testing.T, corpus *testkit.Corpus, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: combo.provider.upstreamProtocol,
		Scenario: scenarioPreByteFailover,
		Law:      lawT5Failover,
	}

	reqMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.client.protocol,
		Kind:     testkit.FixtureRequest,
		Scenario: "text_basic",
	})
	require.NoError(t, err)
	streamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.provider.upstreamProtocol,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)

	sentinel := "provider-one-prebyte-sentinel"
	primaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "failover_primary",
			Turns: []testkit.Turn{{
				Fault: &testkit.Fault{
					Status:    combo.provider.failoverStatus,
					ErrorBody: formatUpstreamJSONError(combo.provider.upstreamProtocol, combo.provider.failoverStatus, sentinel),
				},
			}},
		},
		Validation: combo.provider.validation,
	})
	fallbackH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "failover_secondary",
			Default: testkit.Reply{Raw: streamMatches[0].Path},
		},
		Validation: combo.provider.validation,
	})
	primarySrv := newLabMockServer(combo.provider, primaryH)
	defer primarySrv.Close()
	fallbackSrv := newLabMockServer(combo.provider, fallbackH)
	defer fallbackSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeLabProviderSpec(1, 0, primarySrv.URL, combo.provider),
			makeLabProviderSpec(2, 1, fallbackSrv.URL, combo.provider),
		},
		MaxProviderSwitches: 2,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status, contentType, respBody := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	for _, s := range []*labMockServer{primarySrv, fallbackSrv} {
		for _, v := range s.Violations() {
			for _, rv := range v.RuleViolations {
				rules = appendUniqueLabRule(rules, rv.RuleID)
			}
			for range v.CheckErrors {
				rules = appendUniqueLabRule(rules, "mockupstream_check_error")
			}
		}
	}
	if len(primarySrv.Requests()) == 0 {
		rules = appendUniqueLabRule(rules, "primary_not_contacted")
	}
	if len(fallbackSrv.Requests()) != 1 {
		rules = appendUniqueLabRule(rules, "fallback_not_selected")
	}
	if bytes.Contains(respBody, []byte(sentinel)) {
		rules = appendUniqueLabRule(rules, "primary_content_leaked")
	}
	if status != http.StatusOK || !strings.Contains(strings.ToLower(contentType), "text/event-stream") {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		frames, frameErr := readAllSSEFrames(respBody)
		if frameErr != nil {
			rules = appendUniqueLabRule(rules, "client_sse_read_error")
		}
		for _, gv := range testkit.ValidateStreamGrammar(combo.client.protocol, frames, testkit.GrammarOptions{}) {
			rules = appendUniqueLabRule(rules, gv.Rule)
		}
		logs := lab.UsageLogs()
		if len(logs) != 1 || logs[0].ProviderID != 2 {
			rules = appendUniqueLabRule(rules, "failover_usage_attribution_mismatch")
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

// runLabMidstreamDisconnectCell truncates Provider 1's SSE stream after it
// emits initial usage and partial semantic text. Because bytes were already
// committed downstream, the gateway must not contact Provider 2, must append a
// client-protocol terminal error frame, and must still settle observed partial usage.
func runLabMidstreamDisconnectCell(t *testing.T, corpus *testkit.Corpus, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: combo.provider.upstreamProtocol,
		Scenario: scenarioDisconnectAfterData,
		Law:      lawT5MidstreamTruncate,
	}

	reqMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.client.protocol,
		Kind:     testkit.FixtureRequest,
		Scenario: "text_basic",
	})
	require.NoError(t, err)

	rawStream, cutAfter := partialStreamWithEarlyUsage(combo.provider.upstreamProtocol)
	primaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "midstream_disconnect",
			Turns: []testkit.Turn{{
				Reply: testkit.Reply{Raw: rawStream},
				Fault: &testkit.Fault{TruncateAfterFrames: &cutAfter},
			}},
		},
		Validation: combo.provider.validation,
	})
	fallbackH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "unused_fallback",
			Default: testkit.Reply{Raw: rawStream},
		},
		Validation: combo.provider.validation,
	})
	primarySrv := newLabMockServer(combo.provider, primaryH)
	defer primarySrv.Close()
	fallbackSrv := newLabMockServer(combo.provider, fallbackH)
	defer fallbackSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeLabProviderSpec(1, 0, primarySrv.URL, combo.provider),
			makeLabProviderSpec(2, 1, fallbackSrv.URL, combo.provider),
		},
		MaxProviderSwitches: 2,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status, contentType, respBody := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	for _, v := range primarySrv.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
		for range v.CheckErrors {
			rules = appendUniqueLabRule(rules, "mockupstream_check_error")
		}
	}
	if len(fallbackSrv.Requests()) > 0 {
		rules = appendUniqueLabRule(rules, ruleFallbackContactedAfterOutput)
	}
	if status != http.StatusOK || !strings.Contains(strings.ToLower(contentType), "text/event-stream") {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		if !bytes.Contains(respBody, []byte("partial-midstream-chunk")) {
			rules = appendUniqueLabRule(rules, ruleMissingPartialContent)
		}
		frames, frameErr := readAllSSEFrames(respBody)
		if frameErr != nil {
			rules = appendUniqueLabRule(rules, "client_sse_read_error")
		}
		for _, gv := range testkit.ValidateStreamGrammar(combo.client.protocol, frames, testkit.GrammarOptions{AllowAbnormal: true}) {
			rules = appendUniqueLabRule(rules, gv.Rule)
		}
		if !hasLabAbnormalTerminalErrorFrame(combo.client.protocol, frames) {
			rules = appendUniqueLabRule(rules, ruleMissingFaultErrorTermination)
		}
	}

	logs := lab.UsageLogs()
	if len(logs) == 0 {
		rules = appendUniqueLabRule(rules, ruleMissingUsageLog)
	} else if len(logs) > 1 {
		rules = appendUniqueLabRule(rules, ruleDuplicateUsageLogs)
	} else if logs[0].InputTokens <= 0 {
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

// runLabErrorFormatCell verifies that an upstream HTTP 400 error is formatted
// in the inbound client protocol's JSON error envelope. Failed pre-stream
// exchanges must return a 4xx status without settling any UsageLog rows.
func runLabErrorFormatCell(t *testing.T, corpus *testkit.Corpus, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: combo.provider.upstreamProtocol,
		Scenario: scenarioUpstreamHTTPError,
		Law:      lawT5ErrorFormat,
	}

	reqMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.client.protocol,
		Kind:     testkit.FixtureRequest,
		Scenario: "text_basic",
	})
	require.NoError(t, err)

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "upstream_http_error",
			Turns: []testkit.Turn{{
				Fault: &testkit.Fault{
					Status:    http.StatusBadRequest,
					ErrorBody: formatUpstreamJSONError(combo.provider.upstreamProtocol, http.StatusBadRequest, "upstream rejected request parameter"),
				},
			}},
		},
		Validation: combo.provider.validation,
	})
	upSrv := newLabMockServer(combo.provider, mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(1, 0, upSrv.URL, combo.provider)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status, _, respBody := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	for _, v := range upSrv.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
		for range v.CheckErrors {
			rules = appendUniqueLabRule(rules, "mockupstream_check_error")
		}
	}
	if status < http.StatusBadRequest || status >= http.StatusInternalServerError {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("unexpected_error_status_%d", status))
	}
	if !hasClientErrorEnvelope(combo.client.protocol, respBody) {
		rules = appendUniqueLabRule(rules, ruleWrongClientErrorEnvelope)
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

// runLabSwitchRouteReplayCell executes a two-turn reasoning_tool_replay
// exchange on one protocolLabRouter instance while switching the active
// upstream provider family between Turn 1 and Turn 2. When convertedFirst is
// true, Turn 1 runs on combo.provider and Turn 2 switches to the client's
// native provider; when false, Turn 1 runs natively and Turn 2 switches to
// combo.provider.
//
// Turn 2's request is synthesized from Turn 1's SSE response via
// testkit.SimulateNextTurn so cross-provider signatures, encrypted reasoning
// payloads, and trc1: carriers face strict target-protocol validation.
func runLabSwitchRouteReplayCell(t *testing.T, corpus *testkit.Corpus, combo labRouteCombo, convertedFirst bool) labCellResult {
	t.Helper()
	law := lawT5SwitchFromNative
	if convertedFirst {
		law = lawT5SwitchToNative
	}
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: combo.provider.upstreamProtocol,
		Scenario: "reasoning_tool_replay",
		Law:      law,
	}

	nativeSpec := findLabProviderKind(combo.client.nativeKind)
	firstProvSpec := nativeSpec
	secondProvSpec := combo.provider
	if convertedFirst {
		firstProvSpec = combo.provider
		secondProvSpec = nativeSpec
	}

	reqMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.client.protocol,
		Kind:     testkit.FixtureRequest,
		Scenario: "reasoning_tool_replay",
	})
	require.NoError(t, err)
	firstStreamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: firstProvSpec.upstreamProtocol,
		Kind:     testkit.FixtureStream,
		Scenario: "reasoning_tool_replay",
	})
	require.NoError(t, err)
	secondReplyMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: secondProvSpec.upstreamProtocol,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)

	turn1ValOpts := validationOptionsForProvider(firstProvSpec, reqMatches[0].Data)
	turn2ValOpts := validationOptionsForProvider(secondProvSpec, reqMatches[0].Data)

	firstH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "switch_turn1",
			Default: testkit.Reply{Raw: firstStreamMatches[0].Path},
		},
		Validation: turn1ValOpts,
	})
	secondH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "switch_turn2",
			Default: testkit.Reply{Raw: secondReplyMatches[0].Path},
		},
		Validation: turn2ValOpts,
	})
	firstSrv := newLabMockServer(firstProvSpec, firstH)
	defer firstSrv.Close()
	secondSrv := newLabMockServer(secondProvSpec, secondH)
	defer secondSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, firstSrv.URL, firstProvSpec)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	turn1Body := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status1, ct1, resp1 := sendLabClientRequest(t, gwSrv, combo.client, turn1Body)

	var rules []string
	for _, v := range firstSrv.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, "turn1_"+rv.RuleID)
		}
		for range v.CheckErrors {
			rules = appendUniqueLabRule(rules, "turn1_mockupstream_check_error")
		}
	}
	if status1 != http.StatusOK || !strings.Contains(strings.ToLower(ct1), "text/event-stream") {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("turn1_gateway_http_%d", status1))
		entry.Status = labCellFail
		entry.Rules = rules
		return entry
	}
	turn1Frames, frameErr := readAllSSEFrames(resp1)
	if frameErr != nil {
		rules = appendUniqueLabRule(rules, "turn1_sse_read_error")
	}

	turn2Raw, simErr := testkit.SimulateNextTurn(combo.client.protocol, testkit.TurnInput{
		PrevRequest:              turn1Body,
		ResponseFrames:           turn1Frames,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Confirm the final inventory decision.",
	})
	if simErr != nil {
		rules = appendUniqueLabRule(rules, "simulate_next_turn_error")
		entry.Status = labCellFail
		entry.Rules = rules
		return entry
	}

	// Switch the group's provider from Turn 1's upstream family to Turn 2's family.
	lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, secondSrv.URL, secondProvSpec)})

	turn2Body := prepareLabStreamRequestBody(combo.client.protocol, turn2Raw)
	status2, ct2, resp2 := sendLabClientRequest(t, gwSrv, combo.client, turn2Body)

	for _, v := range secondSrv.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
		for range v.CheckErrors {
			rules = appendUniqueLabRule(rules, "mockupstream_check_error")
		}
	}
	for _, rec := range secondSrv.Requests() {
		if hasUnstrippedCarrierInTokenPaths(secondProvSpec.upstreamProtocol, rec.Body) {
			rules = appendUniqueLabRule(rules, ruleUnstrippedTRC1Carrier)
		}
		if hasLeakedForeignReplayToken(secondProvSpec, rec.Body) {
			rules = appendUniqueLabRule(rules, ruleForeignReplayTokenLeaked)
		}
	}
	if status2 != http.StatusOK || !strings.Contains(strings.ToLower(ct2), "text/event-stream") {
		if len(secondSrv.Violations()) == 0 {
			rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status2))
		}
	} else {
		turn2Frames, err2 := readAllSSEFrames(resp2)
		if err2 != nil {
			rules = appendUniqueLabRule(rules, "client_sse_read_error")
		}
		for _, gv := range testkit.ValidateStreamGrammar(combo.client.protocol, turn2Frames, testkit.GrammarOptions{}) {
			rules = appendUniqueLabRule(rules, gv.Rule)
		}
		if len(lab.UsageLogs()) != 2 {
			rules = appendUniqueLabRule(rules, ruleMissingUsageLog)
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

// makeLabProviderSpec builds a labProvider mapping canonical client fixture
// model names onto prov.upstreamModel and binds explicit payg model protocol
// overrides on OpenCode targets so models outside the embedded catalog route
// cleanly. Antigravity claude-sonnet-4-5 specs whitelist the -thinking suffix
// variant, Anthropic Bedrock specs whitelist us.anthropic.* regional model IDs,
// Gemini OAuth specs default oauth_type to ai_studio, and OpenAI OAuth specs
// disable outbound WS v2 dialing unless overridden in prov.extra.
func makeLabProviderSpec(id int64, priority int, baseURL string, prov labProviderKindSpec) labProvider {
	mapping := map[string]string{
		"claude-sonnet-4-5":          prov.upstreamModel,
		"claude-sonnet-4-5-20250929": prov.upstreamModel,
		"deepseek-reasoner":          prov.upstreamModel,
		"gpt-5":                      prov.upstreamModel,
		"gpt-5.4":                    prov.upstreamModel,
		"gpt-5.5":                    prov.upstreamModel,
		"gpt-4o":                     prov.upstreamModel,
		"gemini-2.5-pro":             prov.upstreamModel,
		"gemini-2.5-flash":           prov.upstreamModel,
		prov.upstreamModel:           prov.upstreamModel,
	}
	var creds map[string]any
	if prov.platform == capability.PlatformOpenCode && prov.upstreamProtocol != "" {
		creds = map[string]any{
			"opencode_model_protocols": map[string]any{
				"payg": map[string]any{
					prov.upstreamModel: string(prov.upstreamProtocol),
				},
			},
		}
	}
	if prov.platform == capability.PlatformGemini && prov.providerType == capability.ProviderTypeOAuth {
		creds = map[string]any{"oauth_type": "ai_studio"}
	}
	models := []string{prov.upstreamModel}
	if prov.platform == capability.PlatformAntigravity && prov.upstreamModel == "claude-sonnet-4-5" {
		// Whitelist claude-sonnet-4-5-thinking alongside claude-sonnet-4-5 because
		// FinalAntigravityModel appends the -thinking suffix before checking
		// model_whitelist, which otherwise rejects thinking requests.
		models = append(models, "claude-sonnet-4-5-thinking")
	}
	if prov.platform == capability.PlatformAnthropic && prov.providerType == capability.ProviderTypeBedrock {
		models = append(models, "us.anthropic."+prov.upstreamModel)
	}
	extra := maps.Clone(prov.extra)
	if prov.platform == capability.PlatformOpenAI && prov.providerType == capability.ProviderTypeOAuth {
		if extra == nil {
			extra = make(map[string]any)
		}
		if _, ok := extra["openai_oauth_responses_websockets_v2_mode"]; !ok {
			extra["openai_oauth_responses_websockets_v2_mode"] = "off"
		}
	}
	return labProvider{
		ID:               id,
		Platform:         prov.platform,
		Type:             prov.providerType,
		BaseURL:          baseURL,
		Models:           models,
		ModelMapping:     mapping,
		EnabledProtocols: prov.enabledProtocols,
		Priority:         priority,
		Concurrency:      4,
		Credentials:      creds,
		Extra:            extra,
	}
}

// newLabMockServer starts an httptest.Server over mockH directly for standard
// providers, or wraps mockH inside protocolLabAntigravityUpstream when prov
// targets Antigravity so v1internal request and response envelopes are adapted.
// Callers must close the returned server when the subtest completes.
func newLabMockServer(prov labProviderKindSpec, mockH *mockupstream.Handler) *labMockServer {
	if prov.platform == capability.PlatformAntigravity {
		ag := newProtocolLabAntigravityUpstream(mockH)
		return &labMockServer{
			Server: httptest.NewServer(ag),
			mock:   mockH,
			ag:     ag,
		}
	}
	return &labMockServer{
		Server: httptest.NewServer(mockH),
		mock:   mockH,
	}
}

// Requests returns deep-cloned inner request snapshots captured by s.mock.
func (s *labMockServer) Requests() []mockupstream.RecordedRequest {
	return s.mock.Requests()
}

// Violations returns strict request violations recorded on s.mock. When s wraps
// an Antigravity v1internal server, outer envelope validation errors from s.ag
// are prepended ahead of inner Gemini rule violations.
func (s *labMockServer) Violations() []mockupstream.ViolationEntry {
	if s.ag != nil {
		return s.ag.Violations()
	}
	return s.mock.Violations()
}

// validationOptionsForLabCombo builds strict request validation options for combo
// and enables deepseek-thinking on Chat upstreams when clientBody enables reasoning.
func validationOptionsForLabCombo(combo labRouteCombo, clientBody []byte) testkit.RequestValidationOptions {
	return validationOptionsForProvider(combo.provider, clientBody)
}

// validationOptionsForProvider returns a copy of prov.validation with a cloned
// DialectRules slice and appends the deepseek-thinking dialect rule for Chat
// upstreams whenever clientBody enables reasoning. Cloning DialectRules keeps
// the shared prov.validation slice immutable across concurrent or sequential subtests.
func validationOptionsForProvider(prov labProviderKindSpec, clientBody []byte) testkit.RequestValidationOptions {
	opts := prov.validation
	opts.DialectRules = slices.Clone(prov.validation.DialectRules)
	if prov.upstreamProtocol == protocol.ProtocolOpenAIChatCompletions && labClientBodyEnablesReasoning(clientBody) {
		if !slices.Contains(opts.DialectRules, "deepseek-thinking") {
			opts.DialectRules = append(opts.DialectRules, "deepseek-thinking")
		}
	}
	return opts
}

// labClientBodyEnablesReasoning reports whether a client wire JSON request
// activates thinking or reasoning controls.
func labClientBodyEnablesReasoning(body []byte) bool {
	if tType := strings.TrimSpace(gjson.GetBytes(body, "thinking.type").String()); tType == "enabled" || tType == "adaptive" {
		return true
	}
	if effort := strings.TrimSpace(gjson.GetBytes(body, "reasoning.effort").String()); effort != "" && effort != "none" {
		return true
	}
	if effort := strings.TrimSpace(gjson.GetBytes(body, "reasoning_effort").String()); effort != "" && effort != "none" {
		return true
	}
	if gjson.GetBytes(body, "generationConfig.thinkingConfig.includeThoughts").Bool() {
		return true
	}
	return gjson.GetBytes(body, "generationConfig.thinkingConfig.thinkingBudget").Int() > 0
}

// prepareLabStreamRequestBody clones raw and sets stream=true on non-Gemini
// JSON requests when absent. Chat Completions requests also receive
// stream_options.include_usage=true when stream_options is absent so client SSE
// streams emit terminal usage chunks for T5_USAGE verification.
func prepareLabStreamRequestBody(clientProto protocol.ProtocolID, raw []byte) []byte {
	out := bytes.Clone(raw)
	if clientProto == protocol.ProtocolGeminiGenerateContent {
		return out
	}
	if !gjson.GetBytes(out, "stream").Exists() {
		out = appendTopLevelJSONMember(out, `"stream":true`)
	}
	if clientProto == protocol.ProtocolOpenAIChatCompletions && !gjson.GetBytes(out, "stream_options").Exists() {
		out = appendTopLevelJSONMember(out, `"stream_options":{"include_usage":true}`)
	}
	return out
}

// appendTopLevelJSONMember inserts memberJSON right before the final closing
// brace of obj while preserving all existing JSON member bytes verbatim. Inputs
// without a closing brace return obj unchanged.
func appendTopLevelJSONMember(obj []byte, memberJSON string) []byte {
	before, _, ok := bytes.CutLast(obj, []byte("}"))
	if !ok {
		return obj
	}
	prefix := bytes.TrimRight(before, " \t\r\n")
	var buf bytes.Buffer
	_, _ = buf.Write(prefix)
	if len(prefix) > 0 && prefix[len(prefix)-1] != '{' {
		_ = buf.WriteByte(',')
	}
	_, _ = buf.WriteString(memberJSON)
	_ = buf.WriteByte('}')
	return buf.Bytes()
}

// sendLabClientRequest dispatches a single HTTP POST to gwSrv using
// clientSpec.path when non-empty or clientSpec's default public gateway route
// and protocol-appropriate API key header. It returns the HTTP status code,
// Content-Type header, and complete response body.
func sendLabClientRequest(t *testing.T, gwSrv *httptest.Server, clientSpec labClientSpec, body []byte) (int, string, []byte) {
	t.Helper()
	reqPath := clientSpec.path
	if reqPath == "" {
		switch clientSpec.protocol {
		case protocol.ProtocolAnthropicMessages:
			reqPath = "/v1/messages"
		case protocol.ProtocolOpenAIResponses:
			reqPath = "/v1/responses"
		case protocol.ProtocolOpenAIChatCompletions:
			reqPath = "/v1/chat/completions"
		case protocol.ProtocolGeminiGenerateContent:
			reqPath = "/v1beta/models/" + clientSpec.defaultModel + ":streamGenerateContent?alt=sse"
		default:
			t.Fatalf("unsupported client protocol %q", clientSpec.protocol)
		}
	}

	client := gwSrv.Client()
	client.Timeout = 10 * time.Second
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, gwSrv.URL+reqPath, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	if clientSpec.protocol == protocol.ProtocolGeminiGenerateContent {
		req.Header.Set("x-goog-api-key", "lab-api-key")
	} else {
		req.Header.Set("Authorization", "Bearer lab-api-key")
	}

	resp, err := client.Do(req)
	require.NoError(t, err)
	respBytes, readErr := io.ReadAll(resp.Body)
	require.NoError(t, resp.Body.Close())
	require.NoError(t, readErr)
	return resp.StatusCode, resp.Header.Get("Content-Type"), respBytes
}

// readAllSSEFrames parses all SSE frames from raw using protocol/sse.NewReader.
func readAllSSEFrames(raw []byte) ([]sse.Frame, error) {
	reader := sse.NewReader(bytes.NewReader(raw))
	var frames []sse.Frame
	for reader.Scan() {
		frames = append(frames, reader.Frame())
	}
	return frames, reader.Err()
}

// compareClientAndSettledUsage compares client-visible token counters from
// frames against the single settled UsageLog row in logs under section 3.3's
// protocol bucket formulas. Anthropic Messages and Gemini compare fresh input
// tokens directly, whereas OpenAI Responses and Chat compare total input tokens
// against InputTokens + CacheReadTokens + CacheCreationTokens. Missing or
// duplicate UsageLog rows, missing client usage payloads, or per-bucket
// discrepancies return T5 violation rule codes.
func compareClientAndSettledUsage(clientProto protocol.ProtocolID, frames []sse.Frame, logs []usage.UsageLog) []string {
	if len(logs) == 0 {
		return []string{ruleMissingUsageLog}
	}
	if len(logs) > 1 {
		return []string{ruleDuplicateUsageLogs}
	}
	settled := logs[0]
	obs := extractClientStreamUsage(clientProto, frames)
	if !obs.present {
		return []string{ruleMissingClientUsage}
	}

	var rules []string
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		if obs.input != settled.InputTokens {
			rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
		}
		if obs.cacheRead != settled.CacheReadTokens {
			rules = appendUniqueLabRule(rules, ruleUsageCacheReadMismatch)
		}
		if obs.cacheWrite != settled.CacheCreationTokens {
			rules = appendUniqueLabRule(rules, ruleUsageCacheWriteMismatch)
		}
		if obs.output != settled.OutputTokens {
			rules = appendUniqueLabRule(rules, ruleUsageOutputMismatch)
		}

	case protocol.ProtocolOpenAIResponses, protocol.ProtocolOpenAIChatCompletions:
		wantTotalInput := settled.InputTokens + settled.CacheReadTokens + settled.CacheCreationTokens
		if obs.input != wantTotalInput {
			rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
		}
		if obs.cacheRead != settled.CacheReadTokens {
			rules = appendUniqueLabRule(rules, ruleUsageCacheReadMismatch)
		}
		if obs.hasCacheWrite && obs.cacheWrite != settled.CacheCreationTokens {
			rules = appendUniqueLabRule(rules, ruleUsageCacheWriteMismatch)
		}
		if obs.output != settled.OutputTokens {
			rules = appendUniqueLabRule(rules, ruleUsageOutputMismatch)
		}

	case protocol.ProtocolGeminiGenerateContent:
		if obs.input != settled.InputTokens {
			rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
		}
		if obs.cacheRead != settled.CacheReadTokens {
			rules = appendUniqueLabRule(rules, ruleUsageCacheReadMismatch)
		}
		if obs.output != settled.OutputTokens {
			rules = appendUniqueLabRule(rules, ruleUsageOutputMismatch)
		}
	}
	return rules
}

// extractClientStreamUsage extracts client-visible token counts from frames
// according to clientProto's streaming usage schema. Anthropic merges
// message_start and message_delta counters, OpenAI Responses and Chat read
// terminal usage objects, and Gemini derives fresh input as promptTokenCount
// minus cachedContentTokenCount and output as candidatesTokenCount plus
// thoughtsTokenCount. Streams without usage payloads return present=false.
func extractClientStreamUsage(clientProto protocol.ProtocolID, frames []sse.Frame) clientObservedUsage {
	var out clientObservedUsage
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		for _, f := range frames {
			evType := f.Event
			if evType == "" {
				evType = gjson.GetBytes(f.Data, "type").String()
			}
			switch evType {
			case "message_start":
				u := gjson.GetBytes(f.Data, "message.usage")
				if u.IsObject() {
					out.present = true
					out.input = int(u.Get("input_tokens").Int())
					out.cacheRead = int(u.Get("cache_read_input_tokens").Int())
					out.cacheWrite = extractAnthropicUsageCacheWrite(u)
					out.hasCacheWrite = true
					out.output = int(u.Get("output_tokens").Int())
				}
			case "message_delta":
				u := gjson.GetBytes(f.Data, "usage")
				if u.IsObject() {
					out.present = true
					if u.Get("output_tokens").Exists() {
						out.output = int(u.Get("output_tokens").Int())
					}
					if v := int(u.Get("input_tokens").Int()); v > 0 || (u.Get("input_tokens").Exists() && out.input == 0) {
						out.input = v
					}
					if v := int(u.Get("cache_read_input_tokens").Int()); v > 0 || (u.Get("cache_read_input_tokens").Exists() && out.cacheRead == 0) {
						out.cacheRead = v
					}
					if cw := extractAnthropicUsageCacheWrite(u); cw > 0 {
						out.cacheWrite = cw
						out.hasCacheWrite = true
					}
				}
			}
		}

	case protocol.ProtocolOpenAIResponses:
		for _, f := range frames {
			evType := f.Event
			if evType == "" {
				evType = gjson.GetBytes(f.Data, "type").String()
			}
			if evType != "response.completed" && evType != "response.incomplete" && evType != "response.done" {
				continue
			}
			u := gjson.GetBytes(f.Data, "response.usage")
			if !u.IsObject() {
				u = gjson.GetBytes(f.Data, "usage")
			}
			if !u.IsObject() {
				continue
			}
			out.present = true
			out.input = int(u.Get("input_tokens").Int())
			out.cacheRead = int(u.Get("input_tokens_details.cached_tokens").Int())
			out.output = int(u.Get("output_tokens").Int())
			if cwField := u.Get("input_tokens_details.cache_write_tokens"); cwField.Exists() {
				out.cacheWrite = int(cwField.Int())
				out.hasCacheWrite = true
			} else if cwTop := u.Get("cache_creation_input_tokens"); cwTop.Exists() {
				out.cacheWrite = int(cwTop.Int())
				out.hasCacheWrite = true
			}
		}

	case protocol.ProtocolOpenAIChatCompletions:
		for _, f := range frames {
			if bytes.Equal(bytes.TrimSpace(f.Data), []byte("[DONE]")) {
				continue
			}
			u := gjson.GetBytes(f.Data, "usage")
			if !u.IsObject() {
				continue
			}
			out.present = true
			out.input = int(u.Get("prompt_tokens").Int())
			if cached := u.Get("prompt_tokens_details.cached_tokens"); cached.Exists() {
				out.cacheRead = int(cached.Int())
			} else {
				out.cacheRead = int(u.Get("cache_read_input_tokens").Int())
			}
			if cw := u.Get("prompt_tokens_details.cache_write_tokens"); cw.Exists() {
				out.cacheWrite = int(cw.Int())
				out.hasCacheWrite = true
			}
			out.output = int(u.Get("completion_tokens").Int())
		}

	case protocol.ProtocolGeminiGenerateContent:
		var prompt, cached, cand, thoughts int
		for _, f := range frames {
			u := gjson.GetBytes(f.Data, "usageMetadata")
			if !u.IsObject() {
				continue
			}
			out.present = true
			if u.Get("promptTokenCount").Exists() {
				prompt = int(u.Get("promptTokenCount").Int())
			}
			if u.Get("cachedContentTokenCount").Exists() {
				cached = int(u.Get("cachedContentTokenCount").Int())
			}
			if u.Get("candidatesTokenCount").Exists() {
				cand = int(u.Get("candidatesTokenCount").Int())
			}
			if u.Get("thoughtsTokenCount").Exists() {
				thoughts = int(u.Get("thoughtsTokenCount").Int())
			}
		}
		out.input = prompt - cached
		out.cacheRead = cached
		out.output = cand + thoughts
	}
	return out
}

// extractAnthropicUsageCacheWrite resolves aggregate cache creation input
// tokens from an Anthropic usage JSON object u. When cache_creation_input_tokens
// is absent or nonpositive, it falls back to summing ephemeral_5m_input_tokens
// and ephemeral_1h_input_tokens under u.cache_creation.
func extractAnthropicUsageCacheWrite(u gjson.Result) int {
	if agg := int(u.Get("cache_creation_input_tokens").Int()); agg > 0 {
		return agg
	}
	return int(u.Get("cache_creation.ephemeral_5m_input_tokens").Int() + u.Get("cache_creation.ephemeral_1h_input_tokens").Int())
}

// partialStreamWithEarlyUsage returns an inline upstream SSE payload together
// with a positive frame index cutAfter that truncates the stream after initial
// usage and "partial-midstream-chunk" have been emitted. Unsupported upstream
// protocol identifiers panic immediately.
func partialStreamWithEarlyUsage(upstreamProto protocol.ProtocolID) (string, int) {
	switch upstreamProto {
	case protocol.ProtocolAnthropicMessages:
		raw := "event: message_start\n" +
			"data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_mid_1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5\",\"content\":[],\"stop_reason\":null,\"stop_sequence\":null,\"usage\":{\"input_tokens\":40,\"cache_creation_input_tokens\":0,\"cache_read_input_tokens\":10,\"output_tokens\":1}}}\n\n" +
			"event: content_block_start\n" +
			"data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
			"event: content_block_delta\n" +
			"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial-midstream-chunk\"}}\n\n" +
			"event: content_block_stop\n" +
			"data: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
			"event: message_delta\n" +
			"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":4}}\n\n" +
			"event: message_stop\n" +
			"data: {\"type\":\"message_stop\"}\n\n"
		return raw, 3

	case protocol.ProtocolOpenAIResponses:
		raw := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_mid_1\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5.4\",\"output\":[]}}\n\n" +
			"event: response.in_progress\n" +
			"data: {\"type\":\"response.in_progress\",\"sequence_number\":1,\"response\":{\"id\":\"resp_mid_1\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"in_progress\",\"model\":\"gpt-5.4\",\"output\":[],\"usage\":{\"input_tokens\":50,\"output_tokens\":1,\"total_tokens\":51,\"input_tokens_details\":{\"cached_tokens\":10},\"output_tokens_details\":{\"reasoning_tokens\":0}}}}\n\n" +
			"event: response.output_item.added\n" +
			"data: {\"type\":\"response.output_item.added\",\"sequence_number\":2,\"output_index\":0,\"item\":{\"id\":\"msg_mid_out\",\"type\":\"message\",\"status\":\"in_progress\",\"role\":\"assistant\",\"content\":[]}}\n\n" +
			"event: response.content_part.added\n" +
			"data: {\"type\":\"response.content_part.added\",\"sequence_number\":3,\"item_id\":\"msg_mid_out\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"annotations\":[],\"text\":\"\"}}\n\n" +
			"event: response.output_text.delta\n" +
			"data: {\"type\":\"response.output_text.delta\",\"sequence_number\":4,\"item_id\":\"msg_mid_out\",\"output_index\":0,\"content_index\":0,\"delta\":\"partial-midstream-chunk\"}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":5,\"response\":{\"id\":\"resp_mid_1\",\"object\":\"response\",\"created_at\":1760000000,\"status\":\"completed\",\"model\":\"gpt-5.4\",\"output\":[]}}\n\n"
		return raw, 5

	case protocol.ProtocolOpenAIChatCompletions:
		raw := "data: {\"id\":\"chat_mid_1\",\"object\":\"chat.completion.chunk\",\"created\":1760000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"partial-midstream-chunk\"}}],\"usage\":{\"prompt_tokens\":50,\"completion_tokens\":3,\"total_tokens\":53,\"prompt_tokens_details\":{\"cached_tokens\":10}}}\n\n" +
			"data: {\"id\":\"chat_mid_1\",\"object\":\"chat.completion.chunk\",\"created\":1760000000,\"model\":\"gpt-4o\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n" +
			"data: [DONE]\n\n"
		return raw, 1

	case protocol.ProtocolGeminiGenerateContent:
		raw := "data: {\"responseId\":\"gem_mid_1\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial-midstream-chunk\"}]}}],\"usageMetadata\":{\"promptTokenCount\":50,\"cachedContentTokenCount\":10,\"candidatesTokenCount\":3,\"totalTokenCount\":53}}\n\n" +
			"data: {\"responseId\":\"gem_mid_1\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"finishReason\":\"STOP\"}]}\n\n"
		return raw, 1

	default:
		panic("unsupported upstream protocol: " + string(upstreamProto))
	}
}

// formatUpstreamJSONError synthesizes a provider-native HTTP error JSON body
// for upstreamProto and status. Server statuses (>=500) emit overload or
// unavailable codes so gateway failover policies treat them as retryable, while
// 400 emits client request-validation error types.
func formatUpstreamJSONError(upstreamProto protocol.ProtocolID, status int, message string) string {
	switch upstreamProto {
	case protocol.ProtocolAnthropicMessages:
		errType := "invalid_request_error"
		switch {
		case status == 529:
			errType = "overloaded_error"
		case status >= 500:
			errType = "api_error"
		case status == http.StatusTooManyRequests:
			errType = "rate_limit_error"
		case status == http.StatusUnauthorized:
			errType = "authentication_error"
		}
		return fmt.Sprintf(`{"type":"error","error":{"type":%q,"message":%q}}`, errType, message)
	case protocol.ProtocolGeminiGenerateContent:
		grpcStatus := "INVALID_ARGUMENT"
		switch {
		case status >= 500:
			grpcStatus = "UNAVAILABLE"
		case status == http.StatusTooManyRequests:
			grpcStatus = "RESOURCE_EXHAUSTED"
		case status == http.StatusUnauthorized:
			grpcStatus = "UNAUTHENTICATED"
		}
		return fmt.Sprintf(`{"error":{"code":%d,"message":%q,"status":%q}}`, status, message, grpcStatus)
	default:
		errType := "invalid_request_error"
		errCode := "invalid_param"
		switch {
		case status >= 500:
			errType = "server_error"
			errCode = "server_overloaded"
		case status == http.StatusTooManyRequests:
			errType = "rate_limit_error"
			errCode = "rate_limit_exceeded"
		case status == http.StatusUnauthorized:
			errType = "authentication_error"
			errCode = "invalid_api_key"
		}
		return fmt.Sprintf(`{"error":{"message":%q,"type":%q,"code":%q}}`, message, errType, errCode)
	}
}

// hasClientErrorEnvelope reports whether body is valid JSON matching
// clientProto's HTTP error envelope. Foreign envelopes (such as Anthropic's
// outer "type":"error" or Gemini's numeric error.code + error.status on OpenAI
// routes) return false.
func hasClientErrorEnvelope(clientProto protocol.ProtocolID, body []byte) bool {
	if !json.Valid(body) {
		return false
	}
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		return gjson.GetBytes(body, "type").String() == "error" &&
			gjson.GetBytes(body, "error.type").String() != "" &&
			gjson.GetBytes(body, "error.message").String() != ""
	case protocol.ProtocolOpenAIResponses:
		// Responses HTTP errors populate error.message plus either error.code
		// (conversion output) or error.type without Anthropic or Google wrappers.
		if gjson.GetBytes(body, "type").String() == "error" {
			return false
		}
		if gjson.GetBytes(body, "error.status").Exists() && gjson.GetBytes(body, "error.code").Type == gjson.Number {
			return false
		}
		return gjson.GetBytes(body, "error.message").String() != "" &&
			(gjson.GetBytes(body, "error.type").String() != "" || gjson.GetBytes(body, "error.code").String() != "")
	case protocol.ProtocolOpenAIChatCompletions:
		// Chat HTTP errors populate top-level error.message and error.type without
		// Anthropic's outer "type":"error" or Google's integer error.code + status.
		if gjson.GetBytes(body, "type").String() == "error" {
			return false
		}
		if gjson.GetBytes(body, "error.status").Exists() && gjson.GetBytes(body, "error.code").Type == gjson.Number {
			return false
		}
		return gjson.GetBytes(body, "error.message").String() != "" &&
			gjson.GetBytes(body, "error.type").String() != ""
	case protocol.ProtocolGeminiGenerateContent:
		return gjson.GetBytes(body, "error.code").Type == gjson.Number &&
			gjson.GetBytes(body, "error.message").String() != ""
	default:
		return false
	}
}

// hasLabAbnormalTerminalErrorFrame reports whether frames end with an explicit
// client-protocol SSE error event rather than normal completion. Chat streams
// inspect the last frame preceding any trailing [DONE] sentinel, and empty
// streams return false.
func hasLabAbnormalTerminalErrorFrame(clientProto protocol.ProtocolID, frames []sse.Frame) bool {
	if len(frames) == 0 {
		return false
	}
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		last := frames[len(frames)-1]
		return last.Event == "error" || gjson.GetBytes(last.Data, "type").String() == "error"
	case protocol.ProtocolOpenAIResponses:
		last := frames[len(frames)-1]
		t := last.Event
		if t == "" {
			t = gjson.GetBytes(last.Data, "type").String()
		}
		return t == "response.failed" || t == "error"
	case protocol.ProtocolOpenAIChatCompletions:
		for _, frame := range slices.Backward(frames) {
			if bytes.Equal(bytes.TrimSpace(frame.Data), []byte("[DONE]")) {
				continue
			}
			return frame.Event == "error" || gjson.GetBytes(frame.Data, "error").IsObject()
		}
		return false
	case protocol.ProtocolGeminiGenerateContent:
		last := frames[len(frames)-1]
		return gjson.GetBytes(last.Data, "error").IsObject()
	default:
		return false
	}
}

// hasUnstrippedCarrierInTokenPaths reports whether any trc1: replay carrier
// survived inside Anthropic thinking, OpenAI reasoning encrypted_content, or
// Gemini part thoughtSignature fields instead of being unwrapped or stripped by
// PrepareNativeReplay. Literal "trc1:" strings inside user or assistant message
// text are ignored.
func hasUnstrippedCarrierInTokenPaths(upstreamProto protocol.ProtocolID, body []byte) bool {
	switch upstreamProto {
	case protocol.ProtocolAnthropicMessages:
		for _, msg := range gjson.GetBytes(body, "messages").Array() {
			if msg.Get("role").String() != "assistant" {
				continue
			}
			for _, part := range msg.Get("content").Array() {
				switch part.Get("type").String() {
				case "thinking":
					if strings.HasPrefix(part.Get("signature").String(), "trc1:") {
						return true
					}
				case "redacted_thinking":
					if strings.HasPrefix(part.Get("data").String(), "trc1:") {
						return true
					}
				}
			}
		}
	case protocol.ProtocolOpenAIResponses:
		for _, item := range gjson.GetBytes(body, "input").Array() {
			if item.Get("type").String() == "reasoning" && strings.HasPrefix(item.Get("encrypted_content").String(), "trc1:") {
				return true
			}
		}
	case protocol.ProtocolGeminiGenerateContent:
		for _, turn := range gjson.GetBytes(body, "contents").Array() {
			for _, part := range turn.Get("parts").Array() {
				if strings.HasPrefix(part.Get("thoughtSignature").String(), "trc1:") ||
					strings.HasPrefix(part.Get("thought_signature").String(), "trc1:") {
					return true
				}
			}
		}
	}
	return false
}

// hasLeakedForeignReplayToken reports whether bare replay tokens issued under
// another upstream scheme leaked into prov's Anthropic signature, OpenAI
// encrypted_content, or Gemini thoughtSignature field. Passback dialects also
// reject Anthropic EqQB cryptographic signatures.
func hasLeakedForeignReplayToken(prov labProviderKindSpec, body []byte) bool {
	switch prov.upstreamProtocol {
	case protocol.ProtocolAnthropicMessages:
		for _, msg := range gjson.GetBytes(body, "messages").Array() {
			if msg.Get("role").String() != "assistant" {
				continue
			}
			for _, part := range msg.Get("content").Array() {
				if part.Get("type").String() != "thinking" {
					continue
				}
				sig := part.Get("signature").String()
				// OpenAI gAAAA ciphertext or Gemini CpEB thoughtSignatures are invalid on Anthropic and passback endpoints.
				if strings.HasPrefix(sig, "gAAAA") || strings.HasPrefix(sig, "CpEB") {
					return true
				}
				if slices.Contains(prov.validation.DialectRules, "passback") && strings.HasPrefix(sig, "EqQB") {
					return true
				}
			}
		}
	case protocol.ProtocolOpenAIResponses:
		for _, item := range gjson.GetBytes(body, "input").Array() {
			if item.Get("type").String() != "reasoning" {
				continue
			}
			enc := item.Get("encrypted_content").String()
			// Anthropic EqQB signatures or Gemini CpEB thoughtSignatures are invalid in OpenAI encrypted_content.
			if strings.HasPrefix(enc, "EqQB") || strings.HasPrefix(enc, "CpEB") {
				return true
			}
		}
	case protocol.ProtocolGeminiGenerateContent:
		for _, turn := range gjson.GetBytes(body, "contents").Array() {
			for _, part := range turn.Get("parts").Array() {
				for _, field := range []string{"thoughtSignature", "thought_signature"} {
					sig := part.Get(field).String()
					// Anthropic EqQB signatures or OpenAI/Grok gAAAA/xai_ ciphertext are invalid in Gemini thoughtSignature.
					if strings.HasPrefix(sig, "EqQB") || strings.HasPrefix(sig, "gAAAA") || strings.HasPrefix(sig, "xai_") {
						return true
					}
				}
			}
		}
	}
	return false
}

// appendUniqueLabRule appends rule to list while preserving insertion order for
// deterministic labCellResult.Rules slices. Empty rule strings and rules
// already present in list return list unchanged.
func appendUniqueLabRule(list []string, rule string) []string {
	if rule == "" || slices.Contains(list, rule) {
		return list
	}
	return append(list, rule)
}

// TestProtocolLabIRStage2MixedGroupAndOpenCodeMatrix exercises all six Stage 2
// OpenCode conversion routes, genuine two-turn converted-then-native replay
// across Bedrock, Vertex, OpenCode, and passback models, and official OpenAI
// input_tokens five-field projection from /v1/messages/count_tokens. Every
// route must satisfy strict upstream validation, client SSE grammar, and
// settled UsageLog parity without leaking trc1: continuity envelopes.
func TestProtocolLabIRStage2MixedGroupAndOpenCodeMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()

	t.Run("opencode_all_six_stage2_conversion_routes", func(t *testing.T) {
		openCodeSpecs := []labProviderKindSpec{
			{
				kind:             "opencode_responses",
				platform:         capability.PlatformOpenCode,
				providerType:     capability.ProviderTypeAPIKey,
				upstreamProtocol: protocol.ProtocolOpenAIResponses,
				upstreamModel:    "gpt-5.4",
				enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
				failoverStatus:   529,
			},
			{
				kind:             "opencode_chat",
				platform:         capability.PlatformOpenCode,
				providerType:     capability.ProviderTypeAPIKey,
				upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
				upstreamModel:    "minimax-m3",
				enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
				failoverStatus:   529,
			},
			{
				kind:             "opencode_messages",
				platform:         capability.PlatformOpenCode,
				providerType:     capability.ProviderTypeAPIKey,
				upstreamProtocol: protocol.ProtocolAnthropicMessages,
				upstreamModel:    "claude-sonnet-4-6",
				enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
				validation:       testkit.RequestValidationOptions{StrictTopLevel: true},
				failoverStatus:   529,
			},
		}

		visited := 0
		for _, spec := range openCodeSpecs {
			for _, client := range protocolLabClients[:3] {
				if client.protocol == spec.upstreamProtocol {
					continue
				}
				visited++
				combo := labRouteCombo{
					slug:     client.slug + "_via_" + spec.kind,
					client:   client,
					provider: spec,
				}
				t.Run(combo.slug, func(t *testing.T) {
					for _, scenario := range []string{"text_basic", "tool_single", "reasoning_tool_replay", "usage_cache"} {
						streamEntry, usageEntry := runLabScenarioCell(t, corpus, combo, scenario)
						require.Equal(t, labCellPass, streamEntry.Status,
							"OpenCode T5_STREAM failed for %s/%s: rules=%v", combo.slug, scenario, streamEntry.Rules)
						require.Equal(t, labCellPass, usageEntry.Status,
							"OpenCode T5_USAGE failed for %s/%s: rules=%v", combo.slug, scenario, usageEntry.Rules)
					}
					failoverEntry := runLabPreByteFailoverCell(t, corpus, combo)
					require.Equal(t, labCellPass, failoverEntry.Status)
				})
			}
		}
		require.Equal(t, 6, visited)
	})

	t.Run("two_turn_converted_then_native_bedrock_vertex_opencode_and_passback", func(t *testing.T) {
		msgTurn1Matches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolAnthropicMessages,
			Kind:     testkit.FixtureRequest,
			Scenario: "reasoning_tool_replay",
		})
		require.NoError(t, err)
		chatStreamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolOpenAIChatCompletions,
			Kind:     testkit.FixtureStream,
			Scenario: "reasoning_tool_replay",
		})
		require.NoError(t, err)
		msgTextMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolAnthropicMessages,
			Kind:     testkit.FixtureStream,
			Scenario: "text_basic",
		})
		require.NoError(t, err)

		chatTurn1Spec := findLabProviderKind("openai_chat")
		for _, tc := range []struct {
			name         string // subtest target label
			platform     string // Turn 2 native provider platform
			providerType string // Turn 2 native provider auth type
			model        string // Turn 2 native upstream model
			passback     bool   // whether Turn 2 expects unsigned thinking passback
			bedrock      bool   // whether Turn 2 serves binary AWS EventStream frames
		}{
			{name: "aws_bedrock", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeBedrock, model: "claude-sonnet-4-6", bedrock: true},
			{name: "vertex_service_account", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeServiceAccount, model: "claude-sonnet-4-6"},
			{name: "opencode_claude", platform: capability.PlatformOpenCode, providerType: capability.ProviderTypeAPIKey, model: "claude-sonnet-4-6"},
			{name: "kimi_passback", platform: capability.PlatformKimi, providerType: capability.ProviderTypeAPIKey, model: "k3", passback: true},
			{name: "deepseek_passback", platform: capability.PlatformDeepseek, providerType: capability.ProviderTypeAPIKey, model: "deepseek-reasoner", passback: true},
		} {
			t.Run("messages_chat_turn1_to_native_"+tc.name, func(t *testing.T) {
				turn1H := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "turn1_chat", Default: testkit.Reply{Raw: chatStreamMatches[0].Path}},
					Validation: validationOptionsForProvider(chatTurn1Spec, msgTurn1Matches[0].Data),
				})
				turn1Srv := httptest.NewServer(turn1H)
				defer turn1Srv.Close()

				var mu sync.Mutex
				var turn2Bodies [][]byte
				var turn2Srv *httptest.Server
				var turn2MockH *mockupstream.Handler

				if tc.bedrock {
					bedrockStream := buildLabBedrockChunkStream(
						`{"type":"message_start","message":{"id":"msg_bdrk_t2","type":"message","role":"assistant","model":"us.anthropic.claude-sonnet-4-6","content":[],"usage":{"input_tokens":0,"output_tokens":0}}}`,
						`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
						`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"bedrock-turn2-ok"}}`,
						`{"type":"content_block_stop","index":0}`,
						`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"amazon-bedrock-invocationMetrics":{"inputTokenCount":25,"outputTokenCount":5}}`,
						`{"type":"message_stop"}`,
					)
					turn2Srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						raw, rErr := io.ReadAll(r.Body)
						require.NoError(t, rErr)
						mu.Lock()
						turn2Bodies = append(turn2Bodies, raw)
						mu.Unlock()
						w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write(bedrockStream)
					}))
				} else if tc.providerType == capability.ProviderTypeServiceAccount {
					turn2Srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						raw, rErr := io.ReadAll(r.Body)
						require.NoError(t, rErr)
						mu.Lock()
						turn2Bodies = append(turn2Bodies, raw)
						mu.Unlock()
						w.Header().Set("Content-Type", "text/event-stream")
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write(msgTextMatches[0].Data)
					}))
				} else {
					valOpts := testkit.RequestValidationOptions{StrictTopLevel: true}
					if tc.passback {
						valOpts.DialectRules = []string{"passback"}
					}
					turn2MockH = mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "turn2_native_msg", Default: testkit.Reply{Raw: msgTextMatches[0].Path}},
						Validation: valOpts,
					})
					turn2Srv = httptest.NewServer(turn2MockH)
				}
				defer turn2Srv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, turn1Srv.URL, chatTurn1Spec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				turn1Body := prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, msgTurn1Matches[0].Data)
				st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], turn1Body)
				require.Equal(t, http.StatusOK, st1)
				require.Empty(t, turn1H.Violations())
				require.Contains(t, string(resp1), "trc1:plain:")

				frames1, err := readAllSSEFrames(resp1)
				require.NoError(t, err)
				turn2Req, err := testkit.SimulateNextTurn(protocol.ProtocolAnthropicMessages, testkit.TurnInput{
					PrevRequest:              turn1Body,
					ResponseFrames:           frames1,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Continue on native provider.",
				})
				require.NoError(t, err)

				nativeProv := labProvider{
					ID:               202,
					Name:             tc.name,
					Platform:         tc.platform,
					Type:             tc.providerType,
					BaseURL:          turn2Srv.URL,
					Models:           []string{tc.model, "us.anthropic." + tc.model},
					ModelMapping:     map[string]string{"claude-sonnet-4-5": tc.model, "claude-sonnet-4-5-20250929": tc.model},
					EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
				}
				lab.SetProviders(t, []labProvider{nativeProv})

				st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, turn2Req))
				require.Equal(t, http.StatusOK, st2, string(resp2))
				frames2, err := readAllSSEFrames(resp2)
				require.NoError(t, err)
				require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolAnthropicMessages, frames2, testkit.GrammarOptions{}))
				require.Len(t, lab.UsageLogs(), 2)

				var wireBody []byte
				if turn2MockH != nil {
					require.Empty(t, turn2MockH.Violations())
					require.Len(t, turn2MockH.Requests(), 1)
					wireBody = turn2MockH.Requests()[0].Body
				} else {
					mu.Lock()
					require.Len(t, turn2Bodies, 1)
					wireBody = turn2Bodies[0]
					mu.Unlock()
				}
				require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolAnthropicMessages, wireBody))
				if tc.passback {
					// Passback retains Turn 1's trc1:plain: reasoning text as an unsigned thinking block.
					lastAssistantIdx := int(gjson.GetBytes(wireBody, "messages.#").Int()) - 2
					require.Equal(t, "thinking", gjson.GetBytes(wireBody, fmt.Sprintf("messages.%d.content.0.type", lastAssistantIdx)).String())
					require.NotEmpty(t, gjson.GetBytes(wireBody, fmt.Sprintf("messages.%d.content.0.thinking", lastAssistantIdx)).String())
					require.False(t, gjson.GetBytes(wireBody, fmt.Sprintf("messages.%d.content.0.signature", lastAssistantIdx)).Exists())
				}
			})
		}

		t.Run("responses_anthropic_turn1_to_native_opencode_and_grok", func(t *testing.T) {
			respTurn1Matches, rErr := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   testkit.SourceWire,
				Protocol: protocol.ProtocolOpenAIResponses,
				Kind:     testkit.FixtureRequest,
				Scenario: "reasoning_tool_replay",
			})
			require.NoError(t, rErr)
			anthStreamMatches, rErr := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   testkit.SourceWire,
				Protocol: protocol.ProtocolAnthropicMessages,
				Kind:     testkit.FixtureStream,
				Scenario: "reasoning_tool_replay",
			})
			require.NoError(t, rErr)
			respTextMatches, rErr := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   testkit.SourceWire,
				Protocol: protocol.ProtocolOpenAIResponses,
				Kind:     testkit.FixtureStream,
				Scenario: "text_basic",
			})
			require.NoError(t, rErr)

			anthSpec := findLabProviderKind("anthropic")
			for _, targetSpec := range []labProviderKindSpec{
				{
					kind:             "opencode_responses",
					platform:         capability.PlatformOpenCode,
					providerType:     capability.ProviderTypeAPIKey,
					upstreamProtocol: protocol.ProtocolOpenAIResponses,
					upstreamModel:    "gpt-5.4",
					enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
				},
				grokLabProviderSpecs()[0],
			} {
				t.Run(targetSpec.kind, func(t *testing.T) {
					t1H := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "t1_anth", Default: testkit.Reply{Raw: anthStreamMatches[0].Path}},
						Validation: anthSpec.validation,
					})
					t1Srv := httptest.NewServer(t1H)
					defer t1Srv.Close()

					t2H := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "t2_resp", Default: testkit.Reply{Raw: respTextMatches[0].Path}},
						Validation: targetSpec.validation,
					})
					t2Srv := httptest.NewServer(t2H)
					defer t2Srv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{makeLabProviderSpec(101, 0, t1Srv.URL, anthSpec)},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					t1Body := prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, respTurn1Matches[0].Data)
					st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Body)
					require.Equal(t, http.StatusOK, st1)
					require.Contains(t, string(resp1), "trc1:anthropic:")

					frames1, fErr := readAllSSEFrames(resp1)
					require.NoError(t, fErr)
					t2Req, sErr := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
						PrevRequest:              t1Body,
						ResponseFrames:           frames1,
						SynthesizeMissingOutputs: true,
						FollowUpUserText:         "Continue on native Responses.",
					})
					require.NoError(t, sErr)

					lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, t2Srv.URL, targetSpec)})
					st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Req))
					require.Equal(t, http.StatusOK, st2, string(resp2))
					require.Empty(t, t2H.Violations())
					require.Len(t, t2H.Requests(), 1)
					require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, t2H.Requests()[0].Body))
					require.Len(t, lab.UsageLogs(), 2)
				})
			}
		})
	})

	t.Run("messages_count_tokens_to_official_openai_projects_five_fields_and_unpacks_carrier", func(t *testing.T) {
		oaCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeOpenAI,
			Data:       "gAAAA_msg_count_official_101",
			ProviderID: 101,
		})
		require.NoError(t, err)

		var mu sync.Mutex
		var bodies [][]byte
		upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, rErr := io.ReadAll(r.Body)
			require.NoError(t, rErr)
			mu.Lock()
			bodies = append(bodies, raw)
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"object":"response.input_tokens","input_tokens":51}`)
		}))
		defer upSrv.Close()

		oaSpec := findLabProviderKind("openai")
		lab := newProtocolLabRouter(t, labOptions{
			Providers:     []labProvider{makeLabProviderSpec(101, 0, "https://api.openai.com/v1", oaSpec)},
			HostOverrides: map[string]string{"api.openai.com": upSrv.URL},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		reqPayload := []byte(fmt.Sprintf(`{
			"model":"claude-sonnet-4-5",
			"max_tokens":2048,
			"stream":true,
			"temperature":0.2,
			"system":"Count system.",
			"messages":[
				{"role":"user","content":"q"},
				{"role":"assistant","content":[
					{"type":"thinking","thinking":"","signature":%q},
					{"type":"text","text":"a"}
				]},
				{"role":"user","content":"count"}
			]
		}`, oaCarrier))
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, gwSrv.URL+"/v1/messages/count_tokens", bytes.NewReader(reqPayload))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer lab-api-key")
		resp, err := gwSrv.Client().Do(req)
		require.NoError(t, err)
		respRaw, err := io.ReadAll(resp.Body)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(respRaw))
		require.Equal(t, int64(51), gjson.GetBytes(respRaw, "input_tokens").Int())

		mu.Lock()
		require.Len(t, bodies, 1)
		wireBody := bodies[0]
		mu.Unlock()

		var top map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(wireBody, &top))
		for k := range top {
			require.Contains(t, []string{"model", "instructions", "input", "tools", "tool_choice"}, k)
		}
		require.Equal(t, "gAAAA_msg_count_official_101", gjson.GetBytes(wireBody, "input.2.encrypted_content").String())
	})
}

// openCodeGeminiLabSpec returns the OpenCode Gemini GenerateContent provider
// archetype shared across Stage 3 T5 and T6 verification suites. Using
// OpenCode's bundled gemini-3-flash offering resolves canonical author
// google/gemini-3-flash-preview without altering protocolLabProviders.
func openCodeGeminiLabSpec() labProviderKindSpec {
	return labProviderKindSpec{
		kind:             "opencode_gemini",
		platform:         capability.PlatformOpenCode,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolGeminiGenerateContent,
		upstreamModel:    "gemini-3-flash",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolGeminiGenerateContent},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"gemini-signature"},
		},
		failoverStatus: 529,
	}
}

// vertexGeminiLabSpec returns the Google Vertex service_account Gemini
// GenerateContent provider archetype shared across Gemini T5 and T6 suites.
// Keeping this specification dedicated leaves the five-provider
// protocolLabProviders table and its 16-route activeLabRouteCombos matrix unchanged.
func vertexGeminiLabSpec() labProviderKindSpec {
	return labProviderKindSpec{
		kind:             "gemini_vertex",
		platform:         capability.PlatformGemini,
		providerType:     capability.ProviderTypeServiceAccount,
		upstreamProtocol: protocol.ProtocolGeminiGenerateContent,
		upstreamModel:    "gemini-2.5-pro",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolGeminiGenerateContent},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"gemini-signature"},
		},
		failoverStatus: http.StatusUnauthorized,
	}
}

// vertexGeminiFlashLabSpec returns a Google Vertex service_account Gemini
// GenerateContent provider archetype targeting toggle-capable gemini-2.5-flash.
// Non-reasoning T6 scenarios that inject disabled thinking controls use this
// specification while reasoning scenarios exercise non-toggle gemini-2.5-pro.
func vertexGeminiFlashLabSpec() labProviderKindSpec {
	spec := vertexGeminiLabSpec()
	spec.kind = "gemini_vertex_flash"
	spec.upstreamModel = "gemini-2.5-flash"
	return spec
}

// TestProtocolLabIRGeminiUpstreamMatrix exercises Stage 3's merged Gemini
// GenerateContent conversion path across Google Gemini (API Key, Vertex
// service_account, and Code Assist OAuth), OpenCode Gemini, and
// /v1/messages/count_tokens. All 33 canonical
// scenarios, client-vs-settled usage parity, 5-second sparse usageMetadata
// tail-frame merging, pre-first-byte and HTTP 200 SSE leading-frame failover,
// mid-stream disconnect block closure, client 400 validation without scheduler
// penalties, same-provider signature-400 compatibility retries, CallMemory
// two-turn bindings, single-hop Cursor input-shaped Chat turns, and mixed-group
// native/converted carrier isolation must satisfy strict protocol laws.
func TestProtocolLabIRGeminiUpstreamMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()

	var targetCombos []labRouteCombo
	for _, combo := range activeLabRouteCombos() {
		if combo.slug == "messages_via_gemini" ||
			combo.slug == "responses_via_gemini" ||
			combo.slug == "chat_via_gemini" {
			targetCombos = append(targetCombos, combo)
		}
	}
	require.Len(t, targetCombos, 3)

	openCodeSpec := openCodeGeminiLabSpec()
	for _, client := range protocolLabClients[:3] {
		targetCombos = append(targetCombos, labRouteCombo{
			slug:     client.slug + "_via_" + openCodeSpec.kind,
			client:   client,
			provider: openCodeSpec,
		})
	}
	require.Len(t, targetCombos, 6)

	for _, combo := range targetCombos {
		t.Run(combo.slug, func(t *testing.T) {
			for _, scenario := range protocolLabScenarios {
				streamEntry, usageEntry := runLabScenarioCell(t, corpus, combo, scenario)
				require.NotEqual(t, labCellFail, streamEntry.Status,
					"IR Gemini T5_STREAM failed for %s/%s: rules=%v", combo.slug, scenario, streamEntry.Rules)
				require.NotEqual(t, labCellFail, usageEntry.Status,
					"IR Gemini T5_USAGE failed for %s/%s: rules=%v", combo.slug, scenario, usageEntry.Rules)
			}

			failoverEntry := runLabPreByteFailoverCell(t, corpus, combo)
			require.Equal(t, labCellPass, failoverEntry.Status,
				"IR Gemini T5_FAILOVER failed for %s: rules=%v", combo.slug, failoverEntry.Rules)

			disconnectEntry := runLabMidstreamDisconnectCell(t, corpus, combo)
			require.Equal(t, labCellPass, disconnectEntry.Status,
				"IR Gemini T5_MIDSTREAM_DISCONNECT failed for %s: rules=%v", combo.slug, disconnectEntry.Rules)

			errFmtEntry := runLabErrorFormatCell(t, corpus, combo)
			require.Equal(t, labCellPass, errFmtEntry.Status,
				"IR Gemini T5_ERROR_FORMAT failed for %s: rules=%v", combo.slug, errFmtEntry.Rules)

			fromNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, false)
			require.Equal(t, labCellPass, fromNativeEntry.Status,
				"IR Gemini T5_SWITCH_FROM_NATIVE failed for %s: rules=%v", combo.slug, fromNativeEntry.Rules)

			toNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, true)
			require.Equal(t, labCellPass, toNativeEntry.Status,
				"IR Gemini T5_SWITCH_TO_NATIVE failed for %s: rules=%v", combo.slug, toNativeEntry.Rules)
		})
	}

	t.Run("opencode_bearer_and_session_header_adaptation", func(t *testing.T) {
		okSSE := "data: {\"responseId\":\"gem_oc_hdr\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"opencode-gemini-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":14,\"candidatesTokenCount\":5,\"totalTokenCount\":19}}\n\n"
		upH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "opencode_gemini_hdr", Default: testkit.Reply{Raw: okSSE}},
			Validation: openCodeSpec.validation,
		})
		upSrv := httptest.NewServer(upH)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, openCodeSpec)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		req, err := http.NewRequestWithContext(
			t.Context(),
			http.MethodPost,
			gwSrv.URL+"/v1/responses",
			strings.NewReader(`{"model":"gpt-5","stream":true,"input":"verify opencode gemini headers"}`),
		)
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer lab-api-key")
		req.Header.Set("session_id", "sess-opencode-gem-77")

		resp, err := gwSrv.Client().Do(req)
		require.NoError(t, err)
		respBytes, err := io.ReadAll(resp.Body)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode, string(respBytes))
		require.Contains(t, string(respBytes), "opencode-gemini-ok")

		reqs := upH.Requests()
		require.Len(t, reqs, 1)
		require.Empty(t, upH.Violations())
		require.Equal(t, "Bearer lab-fixture-key", reqs[0].Headers.Get("Authorization"))
		require.Empty(t, reqs[0].Headers.Get("x-goog-api-key"))
		require.Equal(t, "sess-opencode-gem-77", reqs[0].Headers.Get("X-OpenCode-Session"))
	})

	t.Run("vertex_service_account_and_code_assist_oauth_streaming_and_buffered", func(t *testing.T) {
		t.Run("vertex_service_account", func(t *testing.T) {
			var mu sync.Mutex
			var paths []string
			var auths []string
			vtxSSE := "data: {\"responseId\":\"gem_vtx_s\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"vertex-gemini-stream-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":18,\"cachedContentTokenCount\":6,\"candidatesTokenCount\":5,\"totalTokenCount\":23}}\n\n"
			vtxJSON := `{"responseId":"gem_vtx_b","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"vertex-gemini-buffered-ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":20,"cachedContentTokenCount":4,"candidatesTokenCount":6,"totalTokenCount":26}}`

			vtxSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, raw, testkit.RequestValidationOptions{
					DialectRules: []string{"gemini-signature"},
				}))
				mu.Lock()
				paths = append(paths, r.URL.Path+"?"+r.URL.RawQuery)
				auths = append(auths, r.Header.Get("Authorization"))
				mu.Unlock()
				if strings.Contains(r.URL.Path, ":streamGenerateContent") {
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					_, _ = io.WriteString(w, vtxSSE)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, vtxJSON)
			}))
			defer vtxSrv.Close()

			vtxSpec := vertexGeminiLabSpec()
			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, vtxSrv.URL, vtxSpec)},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			for _, client := range protocolLabClients[:3] {
				lab.ResetUsageLogs()
				var sBody, bBody []byte
				switch client.protocol {
				case protocol.ProtocolAnthropicMessages:
					sBody = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"hello vertex"}]}`)
					bBody = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"messages":[{"role":"user","content":"hello vertex buffered"}]}`)
				case protocol.ProtocolOpenAIResponses:
					sBody = []byte(`{"model":"gpt-5","stream":true,"input":"hello vertex"}`)
					bBody = []byte(`{"model":"gpt-5","stream":false,"input":"hello vertex buffered"}`)
				case protocol.ProtocolOpenAIChatCompletions:
					sBody = []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hello vertex"}]}`)
					bBody = []byte(`{"model":"gpt-4o","stream":false,"messages":[{"role":"user","content":"hello vertex buffered"}]}`)
				}

				stS, ctS, respS := sendLabClientRequest(t, gwSrv, client, sBody)
				require.Equal(t, http.StatusOK, stS, client.slug)
				require.Contains(t, strings.ToLower(ctS), "text/event-stream")
				require.Contains(t, string(respS), "vertex-gemini-stream-ok")
				framesS, fErr := readAllSSEFrames(respS)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(client.protocol, framesS, testkit.GrammarOptions{}))
				require.Empty(t, compareClientAndSettledUsage(client.protocol, framesS, lab.UsageLogs()))

				lab.ResetUsageLogs()
				stB, ctB, respB := sendLabClientRequest(t, gwSrv, client, bBody)
				require.Equal(t, http.StatusOK, stB, client.slug)
				require.Contains(t, strings.ToLower(ctB), "application/json")
				require.Contains(t, string(respB), "vertex-gemini-buffered-ok")
				logsB := lab.UsageLogs()
				require.Len(t, logsB, 1)
				require.Equal(t, 16, logsB[0].InputTokens)
				require.Equal(t, 4, logsB[0].CacheReadTokens)
				require.Equal(t, 6, logsB[0].OutputTokens)
			}

			mu.Lock()
			gotPaths := slices.Clone(paths)
			gotAuths := slices.Clone(auths)
			mu.Unlock()
			require.Len(t, gotPaths, 6)
			for i, p := range gotPaths {
				require.Contains(t, p, "/projects/lab-vertex-project/locations/us-east5/publishers/google/models/gemini-2.5-pro:")
				require.Equal(t, "Bearer lab-vertex-token", gotAuths[i])
			}
		})

		t.Run("code_assist_oauth_wrapping_and_buffered_sse_folding", func(t *testing.T) {
			var mu sync.Mutex
			var envelopes [][]byte
			var paths []string
			caSSE := "data: {\"response\":{\"responseId\":\"gem_ca_1\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"code-assist-folded-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":22,\"cachedContentTokenCount\":2,\"candidatesTokenCount\":7,\"totalTokenCount\":29}}}\n\n"

			caSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				mu.Lock()
				envelopes = append(envelopes, raw)
				paths = append(paths, r.URL.Path+"?"+r.URL.RawQuery)
				mu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, caSSE)
			}))
			defer caSrv.Close()

			caProv := labProvider{
				ID:       101,
				Name:     "gemini-code-assist",
				Platform: capability.PlatformGemini,
				Type:     capability.ProviderTypeOAuth,
				BaseURL:  caSrv.URL,
				Models:   []string{"gemini-2.5-pro"},
				ModelMapping: map[string]string{
					"claude-sonnet-4-5": "gemini-2.5-pro",
					"gpt-5":             "gemini-2.5-pro",
					"gpt-4o":            "gemini-2.5-pro",
				},
				Credentials: map[string]any{
					"access_token": "lab-ca-oauth-token",
					"project_id":   "lab-ca-gcp-project",
					"oauth_type":   "code_assist",
				},
			}
			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{caProv},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			// Buffered Chat request over project-bearing Code Assist OAuth streams SSE upstream and folds via Exchange.Buffer.
			stB, ctB, respB := sendLabClientRequest(
				t,
				gwSrv,
				protocolLabClients[2],
				[]byte(`{"model":"gpt-4o","stream":false,"messages":[{"role":"user","content":"fold code assist"}]}`),
			)
			require.Equal(t, http.StatusOK, stB)
			require.Contains(t, strings.ToLower(ctB), "application/json")
			require.Equal(t, "code-assist-folded-ok", gjson.GetBytes(respB, "choices.0.message.content").String())
			require.Equal(t, int64(22), gjson.GetBytes(respB, "usage.prompt_tokens").Int())
			require.Equal(t, int64(2), gjson.GetBytes(respB, "usage.prompt_tokens_details.cached_tokens").Int())
			require.Equal(t, int64(7), gjson.GetBytes(respB, "usage.completion_tokens").Int())

			mu.Lock()
			require.Len(t, envelopes, 1)
			require.Len(t, paths, 1)
			sentEnv := envelopes[0]
			sentPath := paths[0]
			mu.Unlock()
			require.Equal(t, "/v1internal:streamGenerateContent?alt=sse", sentPath)
			require.Equal(t, "lab-ca-gcp-project", gjson.GetBytes(sentEnv, "project").String())
			require.Equal(t, "gemini-2.5-pro", gjson.GetBytes(sentEnv, "model").String())
			require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, []byte(gjson.GetBytes(sentEnv, "request").Raw), testkit.RequestValidationOptions{
				DialectRules: []string{"gemini-signature"},
			}))
		})
	})

	t.Run("sparse_usage_metadata_tail_window_within_5s", func(t *testing.T) {
		gemSpec := findLabProviderKind("gemini")
		for _, client := range protocolLabClients[:3] {
			t.Run(client.slug, func(t *testing.T) {
				upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, rErr := io.ReadAll(r.Body)
					require.NoError(t, rErr)
					require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, raw, gemSpec.validation))

					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					flusher, ok := w.(http.Flusher)
					require.True(t, ok)

					// Frame 1 delivers content, finishReason STOP, and initial prompt + cached counts.
					_, _ = io.WriteString(w, "data: {\"responseId\":\"gem_tail_1\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"sparse-tail-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":60,\"cachedContentTokenCount\":15,\"candidatesTokenCount\":0,\"totalTokenCount\":60}}\n\n")
					flusher.Flush()

					// Delay the sparse usageMetadata tail frame inside ReadGeminiStream's 5s tail
					// collection window after c.Complete() has stopped downstream keepalives.
					time.Sleep(150 * time.Millisecond)
					_, _ = io.WriteString(w, "data: {\"responseId\":\"gem_tail_1\",\"modelVersion\":\"gemini-2.5-pro\",\"usageMetadata\":{\"candidatesTokenCount\":18,\"thoughtsTokenCount\":7,\"totalTokenCount\":85}}\n\n")
					flusher.Flush()
				}))
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers:         []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, gemSpec)},
					KeepaliveInterval: 45 * time.Millisecond,
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				var reqPayload []byte
				switch client.protocol {
				case protocol.ProtocolAnthropicMessages:
					reqPayload = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"tail usage"}]}`)
				case protocol.ProtocolOpenAIResponses:
					reqPayload = []byte(`{"model":"gpt-5","stream":true,"input":"tail usage"}`)
				case protocol.ProtocolOpenAIChatCompletions:
					reqPayload = []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"tail usage"}]}`)
				}

				status, ct, respBytes := sendLabClientRequest(t, gwSrv, client, reqPayload)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, strings.ToLower(ct), "text/event-stream")
				require.Contains(t, string(respBytes), "sparse-tail-ok")

				frames, fErr := readAllSSEFrames(respBytes)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(client.protocol, frames, testkit.GrammarOptions{}),
					"client stream frames: %s", string(respBytes))

				logs := lab.UsageLogs()
				require.Len(t, logs, 1)
				require.Equal(t, 45, logs[0].InputTokens)
				require.Equal(t, 15, logs[0].CacheReadTokens)
				require.Equal(t, 25, logs[0].OutputTokens)
				require.Empty(t, compareClientAndSettledUsage(client.protocol, frames, logs))
			})
		}
	})

	t.Run("http200_sse_leading_frame_staging_failover", func(t *testing.T) {
		primarySSE := "data: {\"responseId\":\"gem_stage_p1\",\"modelVersion\":\"gemini-2.5-pro\",\"usageMetadata\":{\"promptTokenCount\":12,\"candidatesTokenCount\":0,\"totalTokenCount\":12}}\n\n" +
			"data: {\"error\":{\"code\":503,\"message\":\"primary gemini stream overloaded\",\"status\":\"UNAVAILABLE\"}}\n\n"
		secondarySSE := "data: {\"responseId\":\"gem_stage_p2\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"gemini-fallback-p2-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":16,\"candidatesTokenCount\":5,\"totalTokenCount\":21}}\n\n"

		for _, provSpec := range []labProviderKindSpec{findLabProviderKind("gemini"), openCodeSpec} {
			for _, client := range protocolLabClients[:3] {
				t.Run(client.slug+"_via_"+provSpec.kind, func(t *testing.T) {
					primaryH := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "gem_p1_sse_fail", Default: testkit.Reply{Raw: primarySSE}},
						Validation: provSpec.validation,
					})
					secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "gem_p2_sse_ok", Default: testkit.Reply{Raw: secondarySSE}},
						Validation: provSpec.validation,
					})
					p1Srv := httptest.NewServer(primaryH)
					defer p1Srv.Close()
					p2Srv := httptest.NewServer(secondaryH)
					defer p2Srv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{
							makeLabProviderSpec(1, 0, p1Srv.URL, provSpec),
							makeLabProviderSpec(2, 1, p2Srv.URL, provSpec),
						},
						MaxProviderSwitches: 2,
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					var reqPayload []byte
					switch client.protocol {
					case protocol.ProtocolAnthropicMessages:
						reqPayload = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"ping"}]}`)
					case protocol.ProtocolOpenAIResponses:
						reqPayload = []byte(`{"model":"gpt-5","stream":true,"input":"ping"}`)
					case protocol.ProtocolOpenAIChatCompletions:
						reqPayload = []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"ping"}]}`)
					}

					status, ct, respBytes := sendLabClientRequest(t, gwSrv, client, reqPayload)
					require.Equal(t, http.StatusOK, status)
					require.Contains(t, strings.ToLower(ct), "text/event-stream")
					require.Contains(t, string(respBytes), "gemini-fallback-p2-ok")
					require.NotContains(t, string(respBytes), "gem_stage_p1")
					require.Len(t, primaryH.Requests(), 1)
					require.Len(t, secondaryH.Requests(), 1)

					frames, err := readAllSSEFrames(respBytes)
					require.NoError(t, err)
					require.Empty(t, testkit.ValidateStreamGrammar(client.protocol, frames, testkit.GrammarOptions{}))
					logs := lab.UsageLogs()
					require.Len(t, logs, 1)
					require.Equal(t, int64(2), logs[0].ProviderID)
				})
			}
		}
	})

	t.Run("client_validation_400_without_provider_penalty", func(t *testing.T) {
		okSSE := "data: {\"responseId\":\"gem_after_400\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"gemini-still-healthy\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":9,\"candidatesTokenCount\":3,\"totalTokenCount\":12}}\n\n"

		for _, provSpec := range []labProviderKindSpec{findLabProviderKind("gemini"), vertexGeminiLabSpec(), openCodeSpec} {
			t.Run(provSpec.kind, func(t *testing.T) {
				upH := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "gem_health_guard", Default: testkit.Reply{Raw: okSSE}},
					Validation: provSpec.validation,
				})
				upSrv := httptest.NewServer(upH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, provSpec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				badPayloads := []struct {
					name    string        // rejection case label
					client  labClientSpec // inbound client protocol descriptor
					body    []byte        // invalid client request payload
					wantMsg string        // expected substring inside HTTP 400 error body
				}{
					{
						name:   "messages_ultra_effort",
						client: protocolLabClients[0],
						body:   []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"output_config":{"effort":"ultra"},"messages":[{"role":"user","content":"hi"}]}`),
					},
					{
						name:   "responses_ultra_effort",
						client: protocolLabClients[1],
						body:   []byte(`{"model":"gpt-5","stream":false,"reasoning":{"effort":"ultra"},"input":"hi"}`),
					},
					{
						name:   "chat_ultra_effort",
						client: protocolLabClients[2],
						body:   []byte(`{"model":"gpt-4o","stream":false,"reasoning_effort":"ultra","messages":[{"role":"user","content":"hi"}]}`),
					},
					{
						name:    "messages_disabled_thinking_on_non_toggle_gemini",
						client:  protocolLabClients[0],
						body:    []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`),
						wantMsg: "selected model does not support disabling thinking",
					},
					{
						name:    "messages_zero_budget_enabled_thinking",
						client:  protocolLabClients[0],
						body:    []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"thinking":{"type":"enabled","budget_tokens":0},"messages":[{"role":"user","content":"hi"}]}`),
						wantMsg: "enabled mode requires positive budget_tokens",
					},
					{
						name:    "responses_effort_none_on_non_toggle_gemini",
						client:  protocolLabClients[1],
						body:    []byte(`{"model":"gpt-5","stream":false,"reasoning":{"effort":"none"},"input":"hi"}`),
						wantMsg: "selected model does not support disabling thinking",
					},
					{
						name:    "chat_effort_none_on_non_toggle_gemini",
						client:  protocolLabClients[2],
						body:    []byte(`{"model":"gpt-4o","stream":false,"reasoning_effort":"none","messages":[{"role":"user","content":"hi"}]}`),
						wantMsg: "selected model does not support disabling thinking",
					},
				}

				for _, bp := range badPayloads {
					st, _, errBytes := sendLabClientRequest(t, gwSrv, bp.client, bp.body)
					require.Equal(t, http.StatusBadRequest, st, bp.name)
					require.True(t, hasClientErrorEnvelope(bp.client.protocol, errBytes), "%s: %s", bp.name, string(errBytes))
					if bp.wantMsg != "" {
						require.Contains(t, string(errBytes), bp.wantMsg, bp.name)
					}
				}
				require.Empty(t, upH.Requests(), "upstream must not be contacted on local 400 validation failure")
				require.Empty(t, lab.UsageLogs())

				// Confirm Provider 101 remains immediately schedulable with zero cooldown penalty.
				stOK, _, okBytes := sendLabClientRequest(
					t,
					gwSrv,
					protocolLabClients[1],
					[]byte(`{"model":"gpt-5","stream":true,"input":"healthy check"}`),
				)
				require.Equal(t, http.StatusOK, stOK)
				require.Contains(t, string(okBytes), "gemini-still-healthy")
				require.Len(t, upH.Requests(), 1)
				require.Len(t, lab.UsageLogs(), 1)
			})
		}
	})

	t.Run("signature_400_compatibility_same_provider_retry", func(t *testing.T) {
		turn1SSE := "data: {\"responseId\":\"gem_sig_t1\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Checking order ledger.\",\"thought\":true,\"thoughtSignature\":\"CpEBCgIIAxgB\"},{\"functionCall\":{\"id\":\"call_gem_sig_1\",\"name\":\"lookup\",\"args\":{\"order_id\":9007199254740993}},\"thoughtSignature\":\"CpEBCgQIAxgC\"},{\"functionCall\":{\"id\":\"call_gem_sig_2\",\"name\":\"lookup\",\"args\":{\"order_id\":42}}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":24,\"candidatesTokenCount\":10,\"thoughtsTokenCount\":6,\"totalTokenCount\":40}}\n\n"
		turn2FreshSSE := "data: {\"responseId\":\"gem_sig_t2\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Fresh turn2 thought.\",\"thought\":true,\"thoughtSignature\":\"CpEBCgYIAxgD\"},{\"functionCall\":{\"id\":\"call_gem_fresh_2\",\"name\":\"lookup\",\"args\":{\"order_id\":77}},\"thoughtSignature\":\"CpEBCggIAxgE\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":36,\"candidatesTokenCount\":9,\"thoughtsTokenCount\":5,\"totalTokenCount\":50}}\n\n"
		turn3DoneSSE := "data: {\"responseId\":\"gem_sig_t3\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"signature-retry-sealed-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":48,\"candidatesTokenCount\":5,\"totalTokenCount\":53}}\n\n"

		for _, provSpec := range []labProviderKindSpec{findLabProviderKind("gemini"), openCodeSpec} {
			t.Run(provSpec.kind, func(t *testing.T) {
				var mu sync.Mutex
				var bodies [][]byte
				upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, rErr := io.ReadAll(r.Body)
					require.NoError(t, rErr)
					require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, raw, provSpec.validation),
						"attempt body violated strict Gemini rules: %s", string(raw))

					mu.Lock()
					idx := len(bodies)
					bodies = append(bodies, raw)
					mu.Unlock()

					switch idx {
					case 0:
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, turn1SSE)
					case 1:
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusBadRequest)
						_, _ = io.WriteString(w, `{"error":{"code":400,"message":"Unable to submit request because thought_signature is invalid.","status":"INVALID_ARGUMENT"}}`)
					case 2:
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, turn2FreshSSE)
					default:
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, turn3DoneSSE)
					}
				}))
				defer upSrv.Close()

				prov := makeLabProviderSpec(101, 0, upSrv.URL, provSpec)
				delete(prov.ModelMapping, "gpt-5")
				if prov.Credentials == nil {
					prov.Credentials = make(map[string]any)
				}
				prov.Credentials["effort_model_mapping"] = map[string]any{
					"gpt-5": map[string]any{"high": provSpec.upstreamModel},
				}
				prov.Credentials["effort_model_mapping_strip_effort"] = true
				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{prov},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				turn1Req := []byte(`{"model":"gpt-5","stream":true,"reasoning":{"effort":"high"},"input":[{"type":"message","role":"user","content":"verify order 9007199254740993"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"order_id":{"type":"integer"}}}}]}`)
				st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], turn1Req)
				require.Equal(t, http.StatusOK, st1)
				require.Contains(t, string(resp1), "trc1:gemini:")
				frames1, err := readAllSSEFrames(resp1)
				require.NoError(t, err)

				turn2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
					PrevRequest:              turn1Req,
					ResponseFrames:           frames1,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "continue after tool results",
				})
				require.NoError(t, err)
				turn2Req := prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, turn2Raw)

				st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], turn2Req)
				require.Equal(t, http.StatusOK, st2, string(resp2))
				require.Contains(t, string(resp2), "trc1:gemini:")
				frames2, err := readAllSSEFrames(resp2)
				require.NoError(t, err)

				// Turn 3 replays Turn 2's response to prove the original Exchange sealed the new response tokens.
				turn3Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
					PrevRequest:              turn2Req,
					ResponseFrames:           frames2,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "finalize",
				})
				require.NoError(t, err)
				st3, _, resp3 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, turn3Raw))
				require.Equal(t, http.StatusOK, st3, string(resp3))
				require.Contains(t, string(resp3), "signature-retry-sealed-ok")

				mu.Lock()
				captured := slices.Clone(bodies)
				mu.Unlock()
				require.Len(t, captured, 4)

				// Attempt 1 of Turn 2 sent unpacked CpEB signatures and explicit CallMemory IDs.
				require.Contains(t, string(captured[1]), `"CpEBCgIIAxgB"`)
				require.Contains(t, string(captured[1]), `"CpEBCgQIAxgC"`)
				require.Contains(t, string(captured[1]), `"call_gem_sig_1"`)
				require.False(t, gjson.GetBytes(captured[1], "generationConfig.thinkingConfig").Exists(),
					"StripEffort must omit residual thinkingConfig on Turn 2 attempt 1: %s", string(captured[1]))

				// Compatibility retry (Attempt 2 of Turn 2) stripped CpEB tokens, omitted CallMemory IDs,
				// wrote PlaceholderToken on step head, preserved 64-bit integer precision, and kept StripEffort.
				require.NotContains(t, string(captured[2]), "CpEBCgIIAxgB")
				require.NotContains(t, string(captured[2]), "CpEBCgQIAxgC")
				require.NotContains(t, string(captured[2]), "call_gem_sig_1")
				require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(captured[2], "contents.1.parts.0.thoughtSignature").String())
				require.Equal(t, "9007199254740993", gjson.GetBytes(captured[2], "contents.1.parts.0.functionCall.args.order_id").Raw)
				require.False(t, gjson.GetBytes(captured[2], "generationConfig.thinkingConfig").Exists(),
					"StripEffort must remain active on compatibility retry: %s", string(captured[2]))

				// Turn 3 proves the original Exchange still sealed Turn 2's fresh thoughtSignature and CallMemory binding.
				require.Contains(t, string(captured[3]), `"CpEBCgYIAxgD"`)
				require.Contains(t, string(captured[3]), `"CpEBCggIAxgE"`)
				require.Contains(t, string(captured[3]), `"call_gem_fresh_2"`)
				require.Len(t, lab.UsageLogs(), 3)
			})
		}
	})

	t.Run("call_memory_explicit_nameless_late_and_parallel_unsigned", func(t *testing.T) {
		gemSpec := findLabProviderKind("gemini")
		doneSSE := "data: {\"responseId\":\"gem_cm_done\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"call-memory-verified\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":28,\"candidatesTokenCount\":4,\"totalTokenCount\":32}}\n\n"

		t.Run("explicit_id_and_cross_provider_scope_isolation", func(t *testing.T) {
			turn1SSE := "data: {\"responseId\":\"gem_cm_exp\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"gem_orig_id_1\",\"name\":\"lookup\",\"args\":{\"city\":\"Paris\"}},\"thoughtSignature\":\"CpEBCgIIAxgB\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":16,\"candidatesTokenCount\":6,\"totalTokenCount\":22}}\n\n"

			p1H := mockupstream.NewWithConfig(mockupstream.Config{
				Script: testkit.Script{
					Name: "cm_explicit_p1",
					Turns: []testkit.Turn{
						{Reply: testkit.Reply{Raw: turn1SSE}},
						{Reply: testkit.Reply{Raw: doneSSE}},
					},
				},
				Validation: gemSpec.validation,
			})
			p2H := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "cm_explicit_p2", Default: testkit.Reply{Raw: doneSSE}},
				Validation: gemSpec.validation,
			})
			p1Srv := httptest.NewServer(p1H)
			defer p1Srv.Close()
			p2Srv := httptest.NewServer(p2H)
			defer p2Srv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, p1Srv.URL, gemSpec)},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			t1Req := []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"tools":[{"name":"lookup","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"weather in Paris"}]}`)
			st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], t1Req)
			require.Equal(t, http.StatusOK, st1)
			frames1, err := readAllSSEFrames(resp1)
			require.NoError(t, err)

			t2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolAnthropicMessages, testkit.TurnInput{
				PrevRequest:              t1Req,
				ResponseFrames:           frames1,
				SynthesizeMissingOutputs: true,
				FollowUpUserText:         "summarize",
			})
			require.NoError(t, err)
			t2Req := prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, t2Raw)

			st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], t2Req)
			require.Equal(t, http.StatusOK, st2)
			require.Contains(t, string(resp2), "call-memory-verified")
			require.Empty(t, p1H.Violations())
			sentP1 := p1H.Requests()[1].Body
			require.Equal(t, "gem_orig_id_1", gjson.GetBytes(sentP1, "contents.1.parts.0.functionCall.id").String())
			require.Equal(t, "CpEBCgIIAxgB", gjson.GetBytes(sentP1, "contents.1.parts.0.thoughtSignature").String())
			require.Equal(t, "gem_orig_id_1", gjson.GetBytes(sentP1, "contents.2.parts.0.functionResponse.id").String())

			// Switching to Provider 202 misses Provider 101's scoped CallMemory, omitting foreign IDs and writing PlaceholderToken.
			lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, p2Srv.URL, gemSpec)})
			st3, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[0], t2Req)
			require.Equal(t, http.StatusOK, st3)
			require.Empty(t, p2H.Violations())
			sentP2 := p2H.Requests()[0].Body
			require.False(t, gjson.GetBytes(sentP2, "contents.1.parts.0.functionCall.id").Exists())
			require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(sentP2, "contents.1.parts.0.thoughtSignature").String())
		})

		t.Run("nameless_parallel_same_function_calls", func(t *testing.T) {
			turn1SSE := "data: {\"responseId\":\"gem_cm_nmls\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"name\":\"lookup\",\"args\":{\"city\":\"Tokyo\"}},\"thoughtSignature\":\"CpEBCgIIAxgB\"},{\"functionCall\":{\"name\":\"lookup\",\"args\":{\"city\":\"Osaka\"}}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":16,\"candidatesTokenCount\":8,\"totalTokenCount\":24}}\n\n"

			upH := mockupstream.NewWithConfig(mockupstream.Config{
				Script: testkit.Script{
					Name: "cm_nameless",
					Turns: []testkit.Turn{
						{Reply: testkit.Reply{Raw: turn1SSE}},
						{Reply: testkit.Reply{Raw: doneSSE}},
					},
				},
				Validation: gemSpec.validation,
			})
			upSrv := httptest.NewServer(upH)
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, gemSpec)},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			t1Req := []byte(`{"model":"gpt-5","stream":true,"input":"compare Tokyo and Osaka","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
			st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Req)
			require.Equal(t, http.StatusOK, st1)
			frames1, err := readAllSSEFrames(resp1)
			require.NoError(t, err)

			t2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
				PrevRequest:              t1Req,
				ResponseFrames:           frames1,
				SynthesizeMissingOutputs: true,
				FollowUpUserText:         "compare",
			})
			require.NoError(t, err)
			st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Raw))
			require.Equal(t, http.StatusOK, st2)
			require.Contains(t, string(resp2), "call-memory-verified")
			require.Empty(t, upH.Violations())

			sent := upH.Requests()[1].Body
			require.False(t, gjson.GetBytes(sent, "contents.1.parts.0.functionCall.id").Exists())
			require.False(t, gjson.GetBytes(sent, "contents.1.parts.1.functionCall.id").Exists())
			require.Equal(t, "CpEBCgIIAxgB", gjson.GetBytes(sent, "contents.1.parts.0.thoughtSignature").String())
			require.False(t, gjson.GetBytes(sent, "contents.1.parts.1.thoughtSignature").Exists())
			require.False(t, gjson.GetBytes(sent, "contents.2.parts.0.functionResponse.id").Exists())
			require.False(t, gjson.GetBytes(sent, "contents.2.parts.1.functionResponse.id").Exists())
		})

		t.Run("late_streamed_signature_revision_streaming_and_buffered", func(t *testing.T) {
			lateTurn1SSE := "data: {\"responseId\":\"gem_cm_late\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"gem_late_id_9\",\"name\":\"lookup\",\"args\":{\"city\":\"Berlin\"}}}]}}]}\n\n" +
				"data: {\"responseId\":\"gem_cm_late\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"gem_late_id_9\",\"name\":\"lookup\",\"args\":{\"city\":\"Berlin\"}},\"thoughtSignature\":\"CpEBCgQIAxgC\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":14,\"candidatesTokenCount\":6,\"totalTokenCount\":20}}\n\n"

			// 1. Streaming Chat Completions client.
			upH := mockupstream.NewWithConfig(mockupstream.Config{
				Script: testkit.Script{
					Name: "cm_late_stream",
					Turns: []testkit.Turn{
						{Reply: testkit.Reply{Raw: lateTurn1SSE}},
						{Reply: testkit.Reply{Raw: doneSSE}},
					},
				},
				Validation: gemSpec.validation,
			})
			upSrv := httptest.NewServer(upH)
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, gemSpec)},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			t1Req := []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"weather in Berlin"}]}`)
			st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[2], t1Req)
			require.Equal(t, http.StatusOK, st1)
			frames1, err := readAllSSEFrames(resp1)
			require.NoError(t, err)

			t2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, testkit.TurnInput{
				PrevRequest:              t1Req,
				ResponseFrames:           frames1,
				SynthesizeMissingOutputs: true,
				FollowUpUserText:         "done",
			})
			require.NoError(t, err)
			st2, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[2], prepareLabStreamRequestBody(protocol.ProtocolOpenAIChatCompletions, t2Raw))
			require.Equal(t, http.StatusOK, st2)
			require.Empty(t, upH.Violations())
			sent := upH.Requests()[1].Body
			require.Equal(t, "gem_late_id_9", gjson.GetBytes(sent, "contents.1.parts.0.functionCall.id").String())
			require.Equal(t, "CpEBCgQIAxgC", gjson.GetBytes(sent, "contents.1.parts.0.thoughtSignature").String())

			// 2. Buffered Responses client over Code Assist OAuth SSE (exercises Buffer.patchResponseLateSignatures).
			var caBodies [][]byte
			var caMu sync.Mutex
			caLateSSE := "data: {\"response\":{\"responseId\":\"gem_ca_late\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"gem_ca_late_1\",\"name\":\"lookup\",\"args\":{\"city\":\"Vienna\"}}}]}}]}}\n\n" +
				"data: {\"response\":{\"responseId\":\"gem_ca_late\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"gem_ca_late_1\",\"name\":\"lookup\",\"args\":{\"city\":\"Vienna\"}},\"thoughtSignature\":\"CpEBCgYIAxgD\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":14,\"candidatesTokenCount\":6,\"totalTokenCount\":20}}}\n\n"
			caDoneSSE := "data: {\"response\":{\"responseId\":\"gem_ca_done\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ca-late-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":20,\"candidatesTokenCount\":4,\"totalTokenCount\":24}}}\n\n"
			caSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				caMu.Lock()
				idx := len(caBodies)
				caBodies = append(caBodies, raw)
				caMu.Unlock()
				w.Header().Set("Content-Type", "text/event-stream")
				if idx == 0 {
					_, _ = io.WriteString(w, caLateSSE)
				} else {
					_, _ = io.WriteString(w, caDoneSSE)
				}
			}))
			defer caSrv.Close()

			caLab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{{
					ID:           102,
					Platform:     capability.PlatformGemini,
					Type:         capability.ProviderTypeOAuth,
					BaseURL:      caSrv.URL,
					Models:       []string{"gemini-2.5-pro"},
					ModelMapping: map[string]string{"gpt-5": "gemini-2.5-pro"},
					Credentials: map[string]any{
						"access_token": "lab-ca-token",
						"project_id":   "lab-ca-project",
						"oauth_type":   "code_assist",
					},
				}},
			})
			caGW := httptest.NewServer(caLab)
			defer caGW.Close()

			bufT1 := []byte(`{"model":"gpt-5","stream":false,"input":"weather in Vienna","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
			stB1, _, respB1 := sendLabClientRequest(t, caGW, protocolLabClients[1], bufT1)
			require.Equal(t, http.StatusOK, stB1)
			callID := gjson.GetBytes(respB1, "output.0.call_id").String()
			require.Equal(t, "gem_ca_late_1", callID)

			bufT2 := []byte(fmt.Sprintf(`{"model":"gpt-5","stream":false,"input":[{"type":"message","role":"user","content":"weather in Vienna"},{"type":"function_call","call_id":%q,"name":"lookup","arguments":"{\"city\":\"Vienna\"}"},{"type":"function_call_output","call_id":%q,"output":"sunny"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`, callID, callID))
			stB2, _, respB2 := sendLabClientRequest(t, caGW, protocolLabClients[1], bufT2)
			require.Equal(t, http.StatusOK, stB2)
			require.Contains(t, string(respB2), "ca-late-ok")

			caMu.Lock()
			require.Len(t, caBodies, 2)
			sentCA2 := caBodies[1]
			caMu.Unlock()
			require.Equal(t, "gem_ca_late_1", gjson.GetBytes(sentCA2, "request.contents.1.parts.0.functionCall.id").String())
			require.Equal(t, "CpEBCgYIAxgD", gjson.GetBytes(sentCA2, "request.contents.1.parts.0.thoughtSignature").String())
		})

		t.Run("parallel_unsigned_calls_4b05d79a5", func(t *testing.T) {
			unsignedTurn1SSE := "data: {\"responseId\":\"gem_cm_u\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"gem_u_1\",\"name\":\"lookup\",\"args\":{\"city\":\"Rome\"}}},{\"functionCall\":{\"id\":\"gem_u_2\",\"name\":\"lookup\",\"args\":{\"city\":\"Madrid\"}}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":16,\"candidatesTokenCount\":8,\"totalTokenCount\":24}}\n\n"

			upH := mockupstream.NewWithConfig(mockupstream.Config{
				Script: testkit.Script{
					Name: "cm_parallel_unsigned",
					Turns: []testkit.Turn{
						{Reply: testkit.Reply{Raw: unsignedTurn1SSE}},
						{Reply: testkit.Reply{Raw: doneSSE}},
					},
				},
				Validation: gemSpec.validation,
			})
			upSrv := httptest.NewServer(upH)
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, gemSpec)},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			t1Req := []byte(`{"model":"gpt-5","stream":true,"input":"compare Rome and Madrid","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
			st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Req)
			require.Equal(t, http.StatusOK, st1)
			frames1, err := readAllSSEFrames(resp1)
			require.NoError(t, err)

			t2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
				PrevRequest:              t1Req,
				ResponseFrames:           frames1,
				SynthesizeMissingOutputs: true,
				FollowUpUserText:         "summarize",
			})
			require.NoError(t, err)
			st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Raw))
			require.Equal(t, http.StatusOK, st2)
			require.Contains(t, string(resp2), "call-memory-verified")
			require.Empty(t, upH.Violations())

			// Step head gets PlaceholderToken while the second confirmed-unsigned parallel call stays unsigned (4b05d79a5).
			sent := upH.Requests()[1].Body
			require.Equal(t, "gem_u_1", gjson.GetBytes(sent, "contents.1.parts.0.functionCall.id").String())
			require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(sent, "contents.1.parts.0.thoughtSignature").String())
			require.Equal(t, "gem_u_2", gjson.GetBytes(sent, "contents.1.parts.1.functionCall.id").String())
			require.False(t, gjson.GetBytes(sent, "contents.1.parts.1.thoughtSignature").Exists())
		})
	})

	t.Run("cursor_input_shaped_chat_single_hop_to_gemini", func(t *testing.T) {
		gemCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeGemini,
			Data:   "CpEBCgIIAxgB",
		})
		require.NoError(t, err)

		cursorReplySSE := "data: {\"responseId\":\"gem_cur_1\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"gem_cur_patch_1\",\"name\":\"apply_patch\",\"args\":{\"input\":\"*** Begin Patch\\n*** End Patch\"}},\"thoughtSignature\":\"CpEBCgQIAxgC\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":25,\"candidatesTokenCount\":8,\"totalTokenCount\":33}}\n\n"
		cursorReplyJSON := `{"responseId":"gem_cur_2","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"cursor-gemini-buffered-ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":25,"candidatesTokenCount":5,"totalTokenCount":30}}`

		for _, provSpec := range []labProviderKindSpec{findLabProviderKind("gemini"), openCodeSpec} {
			t.Run(provSpec.kind, func(t *testing.T) {
				var mu sync.Mutex
				var bodies [][]byte
				upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, rErr := io.ReadAll(r.Body)
					require.NoError(t, rErr)
					require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, raw, provSpec.validation))
					mu.Lock()
					bodies = append(bodies, raw)
					mu.Unlock()
					if strings.Contains(r.URL.Path, ":streamGenerateContent") {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, cursorReplySSE)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, cursorReplyJSON)
				}))
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, provSpec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				makeCursorReq := func(stream bool) []byte {
					return []byte(fmt.Sprintf(`{
						"model":"gpt-4o",
						"stream":%v,
						"stream_options":{"include_usage":true},
						"reasoning_effort":"high",
						"input":[
							{"type":"message","role":"user","content":"patch target.go"},
							{"type":"reasoning","id":"rs_cur_gem","summary":[{"type":"summary_text","text":"Plan patch."}],"encrypted_content":%q},
							{"type":"custom_tool_call","call_id":"call_c_prev","name":"apply_patch","input":"*** Prior Patch"},
							{"type":"custom_tool_call_output","call_id":"call_c_prev","output":"applied"}
						],
						"tools":[{"type":"custom","name":"apply_patch","description":"apply unified patch"}]
					}`, stream, gemCarrier))
				}

				stS, ctS, respS := sendLabClientRequest(t, gwSrv, protocolLabClients[2], makeCursorReq(true))
				require.Equal(t, http.StatusOK, stS)
				require.Contains(t, strings.ToLower(ctS), "text/event-stream")
				framesS, fErr := readAllSSEFrames(respS)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, framesS, testkit.GrammarOptions{}))
				require.Contains(t, string(respS), `"apply_patch"`)
				logsS := lab.UsageLogs()
				require.Empty(t, compareClientAndSettledUsage(protocol.ProtocolOpenAIChatCompletions, framesS, logsS))
				require.NotNil(t, logsS[0].ReasoningEffort)
				require.Equal(t, "high", *logsS[0].ReasoningEffort)

				lab.ResetUsageLogs()
				stB, ctB, respB := sendLabClientRequest(t, gwSrv, protocolLabClients[2], makeCursorReq(false))
				require.Equal(t, http.StatusOK, stB)
				require.Contains(t, strings.ToLower(ctB), "application/json")
				require.Equal(t, "cursor-gemini-buffered-ok", gjson.GetBytes(respB, "choices.0.message.content").String())
				require.Len(t, lab.UsageLogs(), 1)

				mu.Lock()
				captured := slices.Clone(bodies)
				mu.Unlock()
				require.Len(t, captured, 2)
				// Single-hop conversion preserves trc1:gemini: directly onto the Gemini thought part without an extra Chat hop.
				for _, b := range captured {
					require.Equal(t, "CpEBCgIIAxgB", gjson.GetBytes(b, "contents.1.parts.0.thoughtSignature").String())
					require.True(t, gjson.GetBytes(b, "contents.1.parts.0.thought").Bool())
					require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(b, "contents.1.parts.1.thoughtSignature").String())
				}
			})
		}
	})

	t.Run("mixed_group_gemini_and_native_carrier_isolation", func(t *testing.T) {
		gemRsnStreamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolGeminiGenerateContent,
			Kind:     testkit.FixtureStream,
			Scenario: "reasoning_tool_replay",
		})
		require.NoError(t, err)
		msgTextMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolAnthropicMessages,
			Kind:     testkit.FixtureStream,
			Scenario: "text_basic",
		})
		require.NoError(t, err)
		respTextMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolOpenAIResponses,
			Kind:     testkit.FixtureStream,
			Scenario: "text_basic",
		})
		require.NoError(t, err)
		gemTextMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolGeminiGenerateContent,
			Kind:     testkit.FixtureStream,
			Scenario: "text_basic",
		})
		require.NoError(t, err)

		gemSpec := findLabProviderKind("gemini")

		t.Run("messages_gemini_turn1_to_native_anthropic_bedrock_vertex_opencode_and_passback", func(t *testing.T) {
			msgTurn1Matches, qErr := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   testkit.SourceWire,
				Protocol: protocol.ProtocolAnthropicMessages,
				Kind:     testkit.FixtureRequest,
				Scenario: "reasoning_tool_replay",
			})
			require.NoError(t, qErr)

			for _, tc := range []struct {
				name         string // subtest target label
				platform     string // Turn 2 native provider platform
				providerType string // Turn 2 native provider auth type
				model        string // Turn 2 native upstream model
				passback     bool   // whether Turn 2 uses passback dialect
				bedrock      bool   // whether Turn 2 serves binary AWS EventStream frames
			}{
				{name: "anthropic_apikey", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeAPIKey, model: "claude-sonnet-4-5-20250929"},
				{name: "aws_bedrock", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeBedrock, model: "claude-sonnet-4-6", bedrock: true},
				{name: "vertex_service_account", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeServiceAccount, model: "claude-sonnet-4-6"},
				{name: "opencode_claude", platform: capability.PlatformOpenCode, providerType: capability.ProviderTypeAPIKey, model: "claude-sonnet-4-6"},
				{name: "kimi_passback", platform: capability.PlatformKimi, providerType: capability.ProviderTypeAPIKey, model: "k3", passback: true},
				{name: "deepseek_passback", platform: capability.PlatformDeepseek, providerType: capability.ProviderTypeAPIKey, model: "deepseek-reasoner", passback: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t1H := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "t1_gem", Default: testkit.Reply{Raw: gemRsnStreamMatches[0].Path}},
						Validation: gemSpec.validation,
					})
					t1Srv := httptest.NewServer(t1H)
					defer t1Srv.Close()

					var mu sync.Mutex
					var t2Bodies [][]byte
					var t2Srv *httptest.Server
					var t2MockH *mockupstream.Handler

					if tc.bedrock {
						bedrockStream := buildLabBedrockChunkStream(
							`{"type":"message_start","message":{"id":"msg_bdrk_gem_t2","type":"message","role":"assistant","model":"us.anthropic.claude-sonnet-4-6","content":[],"usage":{"input_tokens":0,"output_tokens":0}}}`,
							`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
							`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"bedrock-after-gemini-ok"}}`,
							`{"type":"content_block_stop","index":0}`,
							`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"amazon-bedrock-invocationMetrics":{"inputTokenCount":25,"outputTokenCount":5}}`,
							`{"type":"message_stop"}`,
						)
						t2Srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							raw, rErr := io.ReadAll(r.Body)
							require.NoError(t, rErr)
							mu.Lock()
							t2Bodies = append(t2Bodies, raw)
							mu.Unlock()
							w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
							w.WriteHeader(http.StatusOK)
							_, _ = w.Write(bedrockStream)
						}))
					} else if tc.providerType == capability.ProviderTypeServiceAccount {
						t2Srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							raw, rErr := io.ReadAll(r.Body)
							require.NoError(t, rErr)
							mu.Lock()
							t2Bodies = append(t2Bodies, raw)
							mu.Unlock()
							w.Header().Set("Content-Type", "text/event-stream")
							w.WriteHeader(http.StatusOK)
							_, _ = w.Write(msgTextMatches[0].Data)
						}))
					} else {
						valOpts := testkit.RequestValidationOptions{StrictTopLevel: true}
						if tc.passback {
							valOpts.DialectRules = []string{"passback"}
						}
						t2MockH = mockupstream.NewWithConfig(mockupstream.Config{
							Script:     testkit.Script{Name: "t2_native_msg", Default: testkit.Reply{Raw: msgTextMatches[0].Path}},
							Validation: valOpts,
						})
						t2Srv = httptest.NewServer(t2MockH)
					}
					defer t2Srv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{makeLabProviderSpec(101, 0, t1Srv.URL, gemSpec)},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					t1Body := prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, msgTurn1Matches[0].Data)
					st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], t1Body)
					require.Equal(t, http.StatusOK, st1)
					require.Empty(t, t1H.Violations())
					require.Contains(t, string(resp1), "trc1:gemini:")

					frames1, fErr := readAllSSEFrames(resp1)
					require.NoError(t, fErr)
					t2Req, sErr := testkit.SimulateNextTurn(protocol.ProtocolAnthropicMessages, testkit.TurnInput{
						PrevRequest:              t1Body,
						ResponseFrames:           frames1,
						SynthesizeMissingOutputs: true,
						FollowUpUserText:         "Continue on native Messages.",
					})
					require.NoError(t, sErr)

					nativeProv := labProvider{
						ID:               202,
						Name:             tc.name,
						Platform:         tc.platform,
						Type:             tc.providerType,
						BaseURL:          t2Srv.URL,
						Models:           []string{tc.model, "us.anthropic." + tc.model},
						ModelMapping:     map[string]string{"claude-sonnet-4-5": tc.model, "claude-sonnet-4-5-20250929": tc.model},
						EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
					}
					lab.SetProviders(t, []labProvider{nativeProv})

					st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, t2Req))
					require.Equal(t, http.StatusOK, st2, string(resp2))
					frames2, fErr := readAllSSEFrames(resp2)
					require.NoError(t, fErr)
					require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolAnthropicMessages, frames2, testkit.GrammarOptions{}))
					require.Len(t, lab.UsageLogs(), 2)

					var wireBody []byte
					if t2MockH != nil {
						require.Empty(t, t2MockH.Violations())
						require.Len(t, t2MockH.Requests(), 1)
						wireBody = t2MockH.Requests()[0].Body
					} else {
						mu.Lock()
						require.Len(t, t2Bodies, 1)
						wireBody = t2Bodies[0]
						mu.Unlock()
					}
					require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolAnthropicMessages, wireBody))
					require.False(t, hasLeakedForeignReplayToken(findLabProviderKind("anthropic"), wireBody))
				})
			}
		})

		t.Run("responses_gemini_turn1_to_native_openai_grok_opencode_and_websocket", func(t *testing.T) {
			respTurn1Matches, qErr := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   testkit.SourceWire,
				Protocol: protocol.ProtocolOpenAIResponses,
				Kind:     testkit.FixtureRequest,
				Scenario: "reasoning_tool_replay",
			})
			require.NoError(t, qErr)

			for _, targetSpec := range []labProviderKindSpec{
				findLabProviderKind("openai"),
				grokLabProviderSpecs()[0],
				{
					kind:             "opencode_responses",
					platform:         capability.PlatformOpenCode,
					providerType:     capability.ProviderTypeAPIKey,
					upstreamProtocol: protocol.ProtocolOpenAIResponses,
					upstreamModel:    "gpt-5.4",
					enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
				},
			} {
				t.Run(targetSpec.kind, func(t *testing.T) {
					t1H := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "t1_oc_gem", Default: testkit.Reply{Raw: gemRsnStreamMatches[0].Path}},
						Validation: openCodeSpec.validation,
					})
					t1Srv := httptest.NewServer(t1H)
					defer t1Srv.Close()

					t2H := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "t2_native_resp", Default: testkit.Reply{Raw: respTextMatches[0].Path}},
						Validation: targetSpec.validation,
					})
					t2Srv := httptest.NewServer(t2H)
					defer t2Srv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{makeLabProviderSpec(101, 0, t1Srv.URL, openCodeSpec)},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					t1Body := prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, respTurn1Matches[0].Data)
					st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Body)
					require.Equal(t, http.StatusOK, st1)
					require.Contains(t, string(resp1), "trc1:gemini:")

					frames1, fErr := readAllSSEFrames(resp1)
					require.NoError(t, fErr)
					t2Req, sErr := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
						PrevRequest:              t1Body,
						ResponseFrames:           frames1,
						SynthesizeMissingOutputs: true,
						FollowUpUserText:         "Continue on native Responses.",
					})
					require.NoError(t, sErr)

					lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, t2Srv.URL, targetSpec)})
					st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Req))
					require.Equal(t, http.StatusOK, st2, string(resp2))
					require.Empty(t, t2H.Violations())
					require.Len(t, t2H.Requests(), 1)
					wireBody := t2H.Requests()[0].Body
					require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, wireBody))
					require.False(t, hasLeakedForeignReplayToken(targetSpec, wireBody))
					require.Len(t, lab.UsageLogs(), 2)
				})
			}

			t.Run("native_responses_websocket_strips_gemini_carrier", func(t *testing.T) {
				gemCarrier, cErr := ir.FormatCarrier(ir.Carrier{
					Scheme: ir.SchemeGemini,
					Data:   "CpEBCgIIAxgB",
				})
				require.NoError(t, cErr)

				var mu sync.Mutex
				var wsFrames [][]byte
				upWSSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, acceptErr := websocket.Accept(w, r, nil)
					if acceptErr != nil {
						return
					}
					defer func() { _ = conn.CloseNow() }()
					_, data, readErr := conn.Read(r.Context())
					if readErr != nil {
						return
					}
					mu.Lock()
					wsFrames = append(wsFrames, append([]byte(nil), data...))
					mu.Unlock()
					// Ignore write and teardown errors on the local mock WebSocket once the
					// captured request frame is recorded and terminal completion frames flush.
					_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_ws_gem","object":"response","status":"in_progress","model":"gpt-5.4","output":[]}}`))
					_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.completed","sequence_number":1,"response":{"id":"resp_ws_gem","object":"response","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":8,"output_tokens":2,"total_tokens":10}}}`))
				}))
				defer upWSSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{{
						ID:               401,
						Platform:         capability.PlatformOpenAI,
						Type:             capability.ProviderTypeAPIKey,
						BaseURL:          upWSSrv.URL,
						Models:           []string{"gpt-5.4"},
						ModelMapping:     map[string]string{"gpt-5": "gpt-5.4"},
						EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
					}},
					AllowedProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				wsURL := "ws" + strings.TrimPrefix(gwSrv.URL, "http") + "/v1/responses"
				dialCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				clientConn, _, dialErr := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
					HTTPHeader: http.Header{"Authorization": []string{"Bearer lab-api-key"}},
				})
				require.NoError(t, dialErr)
				defer func() { _ = clientConn.CloseNow() }()

				turnFrame := []byte(fmt.Sprintf(`{"type":"response.create","model":"gpt-5","store":false,"input":[{"type":"message","role":"user","content":"ws after gemini"},{"type":"reasoning","id":"rs_gem_syn","summary":[{"type":"summary_text","text":"gemini summary"}],"encrypted_content":%q},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`, gemCarrier))
				require.NoError(t, clientConn.Write(dialCtx, websocket.MessageText, turnFrame))
				for {
					_, msg, rErr := clientConn.Read(dialCtx)
					require.NoError(t, rErr)
					if gjson.GetBytes(msg, "type").String() == "response.completed" {
						break
					}
				}
				// Ignore best-effort normal WebSocket closure errors after the terminal response.completed frame has already been verified.
				_ = clientConn.Close(websocket.StatusNormalClosure, "done")

				mu.Lock()
				got := slices.Clone(wsFrames)
				mu.Unlock()
				require.Len(t, got, 1)
				require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, got[0]))
				require.Equal(t, "gemini summary", gjson.GetBytes(got[0], "input.1.summary.0.text").String())
				require.False(t, gjson.GetBytes(got[0], "input.1.encrypted_content").Exists())
				require.False(t, gjson.GetBytes(got[0], "input.1.id").Exists())
			})
		})

		t.Run("native_messages_and_responses_turn1_to_gemini_and_opencode_gemini", func(t *testing.T) {
			for _, turn2Prov := range []labProviderKindSpec{gemSpec, vertexGeminiLabSpec(), openCodeSpec} {
				for _, client := range protocolLabClients[:2] {
					t.Run(client.slug+"_native_to_"+turn2Prov.kind, func(t *testing.T) {
						nativeSpec := findLabProviderKind(client.nativeKind)
						reqMatches, qErr := corpus.QueryFixtures(testkit.FixtureQuery{
							Source:   testkit.SourceWire,
							Protocol: client.protocol,
							Kind:     testkit.FixtureRequest,
							Scenario: "reasoning_tool_replay",
						})
						require.NoError(t, qErr)
						t1StreamMatches, qErr := corpus.QueryFixtures(testkit.FixtureQuery{
							Source:   testkit.SourceWire,
							Protocol: nativeSpec.upstreamProtocol,
							Kind:     testkit.FixtureStream,
							Scenario: "reasoning_tool_replay",
						})
						require.NoError(t, qErr)

						t1H := mockupstream.NewWithConfig(mockupstream.Config{
							Script:     testkit.Script{Name: "t1_native", Default: testkit.Reply{Raw: t1StreamMatches[0].Path}},
							Validation: nativeSpec.validation,
						})
						t2H := mockupstream.NewWithConfig(mockupstream.Config{
							Script:     testkit.Script{Name: "t2_gem", Default: testkit.Reply{Raw: gemTextMatches[0].Path}},
							Validation: turn2Prov.validation,
						})
						t1Srv := httptest.NewServer(t1H)
						defer t1Srv.Close()
						t2Srv := httptest.NewServer(t2H)
						defer t2Srv.Close()

						lab := newProtocolLabRouter(t, labOptions{
							Providers: []labProvider{makeLabProviderSpec(101, 0, t1Srv.URL, nativeSpec)},
						})
						gwSrv := httptest.NewServer(lab)
						defer gwSrv.Close()

						t1Body := prepareLabStreamRequestBody(client.protocol, reqMatches[0].Data)
						st1, _, resp1 := sendLabClientRequest(t, gwSrv, client, t1Body)
						require.Equal(t, http.StatusOK, st1)
						frames1, fErr := readAllSSEFrames(resp1)
						require.NoError(t, fErr)

						t2Raw, sErr := testkit.SimulateNextTurn(client.protocol, testkit.TurnInput{
							PrevRequest:              t1Body,
							ResponseFrames:           frames1,
							SynthesizeMissingOutputs: true,
							FollowUpUserText:         "Switch to Gemini on Turn 2.",
						})
						require.NoError(t, sErr)

						lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, t2Srv.URL, turn2Prov)})
						st2, _, resp2 := sendLabClientRequest(t, gwSrv, client, prepareLabStreamRequestBody(client.protocol, t2Raw))
						require.Equal(t, http.StatusOK, st2, string(resp2))
						require.Empty(t, t2H.Violations())
						require.Len(t, t2H.Requests(), 1)
						wireBody := t2H.Requests()[0].Body
						require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolGeminiGenerateContent, wireBody))
						require.False(t, hasLeakedForeignReplayToken(turn2Prov, wireBody))
						require.Len(t, lab.UsageLogs(), 2)
					})
				}
			}
		})
	})

	t.Run("messages_count_tokens_to_gemini", func(t *testing.T) {
		gemCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeGemini,
			Data:   "CpEBCgIIAxgB",
		})
		require.NoError(t, err)

		countPayload := []byte(fmt.Sprintf(`{
			"model":"claude-sonnet-4-5",
			"system":"Count Gemini tokens accurately.",
			"tools":[{
				"name":"search_docs",
				"description":"Search technical docs",
				"input_schema":{
					"$schema":"https://json-schema.org/draft/2020-12/schema",
					"type":"object",
					"additionalProperties":false,
					"properties":{"limit":{"type":"integer","exclusiveMinimum":0}}
				}
			}],
			"messages":[
				{"role":"user","content":"find docs"},
				{"role":"assistant","content":[
					{"type":"thinking","thinking":"Plan doc lookup.","signature":%q},
					{"type":"tool_use","id":"toolu_cnt_1","name":"search_docs","input":{"limit":5}}
				]},
				{"role":"user","content":[
					{"type":"tool_result","tool_use_id":"toolu_cnt_1","content":"3 hits"}
				]}
			]
		}`, gemCarrier))

		postCountTokens := func(srv *httptest.Server, body []byte) (int, []byte) {
			req, rErr := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/v1/messages/count_tokens", bytes.NewReader(body))
			require.NoError(t, rErr)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Authorization", "Bearer lab-api-key")
			resp, dErr := srv.Client().Do(req)
			require.NoError(t, dErr)
			raw, readErr := io.ReadAll(resp.Body)
			require.NoError(t, resp.Body.Close())
			require.NoError(t, readErr)
			return resp.StatusCode, raw
		}

		for _, provSpec := range []labProviderKindSpec{findLabProviderKind("gemini"), vertexGeminiLabSpec()} {
			t.Run(provSpec.kind, func(t *testing.T) {
				var mu sync.Mutex
				var bodies [][]byte
				var paths []string
				upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, rErr := io.ReadAll(r.Body)
					require.NoError(t, rErr)
					mu.Lock()
					bodies = append(bodies, raw)
					paths = append(paths, r.URL.Path)
					mu.Unlock()
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"totalTokens":63}`)
				}))
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, provSpec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				status, respRaw := postCountTokens(gwSrv, countPayload)
				require.Equal(t, http.StatusOK, status, string(respRaw))
				require.Equal(t, int64(63), gjson.GetBytes(respRaw, "input_tokens").Int())
				require.Empty(t, lab.UsageLogs(), "count_tokens must never settle UsageLog rows")

				mu.Lock()
				require.Len(t, bodies, 1)
				wireBody := bodies[0]
				gotPath := paths[0]
				mu.Unlock()

				require.Contains(t, gotPath, ":countTokens")
				inner := gjson.GetBytes(wireBody, "generateContentRequest")
				require.True(t, inner.IsObject(), string(wireBody))
				require.Equal(t, "models/gemini-2.5-pro", inner.Get("model").String())
				// ReplayScheme and PlaceholderToken are cleared on count_tokens so no signatures appear.
				require.NotContains(t, inner.Raw, "thoughtSignature")
				require.NotContains(t, inner.Raw, "skip_thought_signature_validator")
				require.NotContains(t, inner.Raw, "CpEBCgIIAxgB")
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, []byte(inner.Raw), testkit.RequestValidationOptions{}))
			})
		}

		t.Run("oauth_insufficient_scope_estimates_locally", func(t *testing.T) {
			upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("WWW-Authenticate", `Bearer error="insufficient_scope"`)
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, `{"error":{"code":403,"message":"Request had insufficient authentication scopes.","status":"PERMISSION_DENIED"}}`)
			}))
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{{
					ID:           101,
					Platform:     capability.PlatformGemini,
					Type:         capability.ProviderTypeOAuth,
					BaseURL:      upSrv.URL,
					Models:       []string{"gemini-2.5-pro"},
					ModelMapping: map[string]string{"claude-sonnet-4-5": "gemini-2.5-pro"},
					Credentials: map[string]any{
						"access_token": "lab-oauth-narrow-scope",
					},
				}},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			status, respRaw := postCountTokens(gwSrv, countPayload)
			require.Equal(t, http.StatusOK, status, string(respRaw))
			require.Greater(t, gjson.GetBytes(respRaw, "input_tokens").Int(), int64(0))
			require.Empty(t, lab.UsageLogs())
		})
	})
}

// antigravityLabProviderSpecs returns the Claude and Gemini Antigravity OAuth
// provider archetypes exercised by TestProtocolLabIRAntigravityUpstreamMatrix.
// Keeping these OAuth archetypes dedicated separates Antigravity v1internal
// routing from the five-provider protocolLabProviders table and its 16-route
// activeLabRouteCombos matrix.
func antigravityLabProviderSpecs() []labProviderKindSpec {
	return []labProviderKindSpec{
		{
			kind:             "antigravity_claude",
			platform:         capability.PlatformAntigravity,
			providerType:     capability.ProviderTypeOAuth,
			upstreamProtocol: protocol.ProtocolGeminiGenerateContent,
			upstreamModel:    "claude-sonnet-4-5",
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"antigravity-schema"},
			},
			// AntigravityRetryLoop sleeps across unstructured 429/5xx retries; 401
			// enters outer provider failover immediately without multi-second sleep during matrix runs.
			failoverStatus: http.StatusUnauthorized,
		},
		{
			kind:             "antigravity_gemini",
			platform:         capability.PlatformAntigravity,
			providerType:     capability.ProviderTypeOAuth,
			upstreamProtocol: protocol.ProtocolGeminiGenerateContent,
			upstreamModel:    "gemini-2.5-pro",
			validation: testkit.RequestValidationOptions{
				DialectRules: []string{"gemini-signature", "antigravity-schema"},
			},
			// AntigravityRetryLoop sleeps across unstructured 429/5xx retries; 401
			// enters outer provider failover immediately without multi-second sleep during matrix runs.
			failoverStatus: http.StatusUnauthorized,
		},
	}
}

// antigravityDedicatedMessagesClient returns the inbound client descriptor for
// the forced-platform /antigravity/v1/messages entry point.
func antigravityDedicatedMessagesClient() labClientSpec {
	return labClientSpec{
		slug:         "antigravity_messages",
		protocol:     protocol.ProtocolAnthropicMessages,
		path:         "/antigravity/v1/messages",
		defaultModel: "claude-sonnet-4-5",
		nativeKind:   "anthropic",
	}
}

// TestProtocolLabIRAntigravityUpstreamMatrix exercises Stage 4's merged
// Antigravity v1internal conversion, response reading, and recovery pipeline
// across Anthropic Messages (/v1/messages and /antigravity/v1/messages), OpenAI
// Responses (/v1/responses), and OpenAI Chat Completions (/v1/chat/completions)
// for both claude-* and gemini-* model families.
// It verifies all 33 canonical scenarios, usage parity, 5-second sparse
// usageMetadata tail-frame merging, pre-byte and HTTP 200 SSE leading-frame
// failover, post-output midstream disconnects, upstream timeouts, CommittedSSE
// terminal errors, ApplyErrorPassthroughRule overrides, client 400 validation
// without scheduler penalties, RecoverClaudeIR and RecoverGemini, CallMemory,
// single-hop Cursor input-shaped Chat turns, live identity_patch_prompt updates,
// and mixed-group native/converted carrier continuity and isolation.
func TestProtocolLabIRAntigravityUpstreamMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	agSpecs := antigravityLabProviderSpecs()
	agDedicatedClient := antigravityDedicatedMessagesClient()
	allClients := []labClientSpec{
		protocolLabClients[0],
		agDedicatedClient,
		protocolLabClients[1],
		protocolLabClients[2],
	}

	t.Run("canonical_scenarios_and_dedicated_antigravity_entry", func(t *testing.T) {
		for _, client := range allClients {
			for _, prov := range agSpecs {
				combo := labRouteCombo{
					slug:     client.slug + "_via_" + prov.kind,
					client:   client,
					provider: prov,
				}
				t.Run(combo.slug, func(t *testing.T) {
					for _, scenario := range protocolLabScenarios {
						streamEntry, usageEntry := runLabScenarioCell(t, corpus, combo, scenario)
						require.NotEqual(t, labCellFail, streamEntry.Status,
							"IR Antigravity T5_STREAM failed for %s/%s: rules=%v", combo.slug, scenario, streamEntry.Rules)
						require.NotEqual(t, labCellFail, usageEntry.Status,
							"IR Antigravity T5_USAGE failed for %s/%s: rules=%v", combo.slug, scenario, usageEntry.Rules)
					}

					failoverEntry := runLabPreByteFailoverCell(t, corpus, combo)
					require.Equal(t, labCellPass, failoverEntry.Status,
						"IR Antigravity T5_FAILOVER failed for %s: rules=%v", combo.slug, failoverEntry.Rules)

					disconnectEntry := runLabMidstreamDisconnectCell(t, corpus, combo)
					require.Equal(t, labCellPass, disconnectEntry.Status,
						"IR Antigravity T5_MIDSTREAM_DISCONNECT failed for %s: rules=%v", combo.slug, disconnectEntry.Rules)

					errFmtEntry := runLabErrorFormatCell(t, corpus, combo)
					require.Equal(t, labCellPass, errFmtEntry.Status,
						"IR Antigravity T5_ERROR_FORMAT failed for %s: rules=%v", combo.slug, errFmtEntry.Rules)

					// Generic entry points also run two-turn mixed-group native switch laws;
					// /antigravity/v1/messages enforces ForcePlatform=antigravity (tested below).
					if client.path == "" {
						fromNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, false)
						require.Equal(t, labCellPass, fromNativeEntry.Status,
							"IR Antigravity T5_SWITCH_FROM_NATIVE failed for %s: rules=%v", combo.slug, fromNativeEntry.Rules)

						toNativeEntry := runLabSwitchRouteReplayCell(t, corpus, combo, true)
						require.Equal(t, labCellPass, toNativeEntry.Status,
							"IR Antigravity T5_SWITCH_TO_NATIVE failed for %s: rules=%v", combo.slug, toNativeEntry.Rules)
					}
				})
			}
		}

		t.Run("buffered_folding_and_forced_antigravity_platform_routing", func(t *testing.T) {
			anthSpec := findLabProviderKind("anthropic")
			anthMock := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "unused_anth", Default: testkit.Reply{Raw: "wire/anthropic_messages/stream/text_basic.sse"}},
				Validation: anthSpec.validation,
			})
			anthSrv := httptest.NewServer(anthMock)
			defer anthSrv.Close()

			for _, prov := range agSpecs {
				t.Run(prov.kind, func(t *testing.T) {
					bufSSE := fmt.Sprintf("data: {\"response\":{\"responseId\":\"ag_buf_1\",\"modelVersion\":%q,\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"antigravity-buffered-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":24,\"cachedContentTokenCount\":6,\"candidatesTokenCount\":5,\"totalTokenCount\":29}}}\n\n", prov.upstreamModel+"-v2")
					agMock := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "ag_buf", Default: testkit.Reply{Raw: bufSSE}},
						Validation: prov.validation,
					})
					agSrv := newLabMockServer(prov, agMock)
					defer agSrv.Close()

					// Give native Anthropic higher priority (0) than Antigravity (1) to prove
					// /antigravity/v1/messages enforces ForcePlatform=antigravity.
					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{
							makeLabProviderSpec(1, 0, anthSrv.URL, anthSpec),
							makeLabProviderSpec(2, 1, agSrv.URL, prov),
						},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					stDedicated, ctDedicated, respDedicated := sendLabClientRequest(
						t,
						gwSrv,
						agDedicatedClient,
						[]byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"messages":[{"role":"user","content":"dedicated antigravity buffered"}]}`),
					)
					require.Equal(t, http.StatusOK, stDedicated, string(respDedicated))
					require.Contains(t, strings.ToLower(ctDedicated), "application/json")
					require.Contains(t, string(respDedicated), "antigravity-buffered-ok")
					require.Empty(t, anthMock.Requests(), "/antigravity/v1/messages must bypass non-Antigravity providers")
					require.Len(t, agSrv.Requests(), 1)
					agSrv.ag.AssertNoViolations(t)

					logs := lab.UsageLogs()
					require.Len(t, logs, 1)
					require.Equal(t, int64(2), logs[0].ProviderID)
					require.Equal(t, 18, logs[0].InputTokens)
					require.Equal(t, 6, logs[0].CacheReadTokens)
					require.Equal(t, 5, logs[0].OutputTokens)
					require.NotNil(t, logs[0].UpstreamResponseModel)
					require.Equal(t, prov.upstreamModel+"-v2", *logs[0].UpstreamResponseModel)

					// Also verify buffered folding on generic /v1/responses and /v1/chat/completions when Antigravity is primary.
					lab.SetProviders(t, []labProvider{makeLabProviderSpec(2, 0, agSrv.URL, prov)})
					for _, cli := range []labClientSpec{protocolLabClients[0], protocolLabClients[1], protocolLabClients[2]} {
						lab.ResetUsageLogs()
						var bBody []byte
						switch cli.protocol {
						case protocol.ProtocolAnthropicMessages:
							bBody = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"messages":[{"role":"user","content":"generic buffered"}]}`)
						case protocol.ProtocolOpenAIResponses:
							bBody = []byte(`{"model":"gpt-5","stream":false,"input":"generic buffered"}`)
						case protocol.ProtocolOpenAIChatCompletions:
							bBody = []byte(`{"model":"gpt-4o","stream":false,"messages":[{"role":"user","content":"generic buffered"}]}`)
						}
						stB, ctB, respB := sendLabClientRequest(t, gwSrv, cli, bBody)
						require.Equal(t, http.StatusOK, stB, cli.slug)
						require.Contains(t, strings.ToLower(ctB), "application/json")
						require.Contains(t, string(respB), "antigravity-buffered-ok")
						require.Len(t, lab.UsageLogs(), 1)
						require.Equal(t, 18, lab.UsageLogs()[0].InputTokens)
						require.Equal(t, 6, lab.UsageLogs()[0].CacheReadTokens)
						require.Equal(t, 5, lab.UsageLogs()[0].OutputTokens)
					}
				})
			}
		})
	})

	t.Run("sparse_usage_metadata_tail_window_within_5s", func(t *testing.T) {
		runAntigravitySparseTailWindowSuite(t, allClients, agSpecs)
	})

	t.Run("pre_byte_and_http200_leading_frame_staging_failover", func(t *testing.T) {
		runAntigravityFailoverSuite(t, allClients, agSpecs)
	})

	t.Run("post_output_disconnect_timeout_committed_sse_and_passthrough_rules", func(t *testing.T) {
		runAntigravityTimeoutAndErrorPolicySuite(t, allClients, agSpecs)
	})

	t.Run("client_validation_400_without_provider_penalty", func(t *testing.T) {
		runAntigravityClientValidationSuite(t, allClients, agSpecs)
	})

	t.Run("claude_ir_and_gemini_recovery_pipelines", func(t *testing.T) {
		runAntigravityRecoveryPipelinesSuite(t, agSpecs)
	})

	t.Run("call_memory_cursor_input_chat_and_live_identity_patch", func(t *testing.T) {
		runAntigravityCallMemoryCursorAndIdentitySuite(t, agSpecs)
	})

	t.Run("mixed_group_carrier_continuity_and_isolation", func(t *testing.T) {
		runAntigravityMixedGroupContinuitySuite(t, corpus, agSpecs)
	})

	t.Run("inline_generated_image_and_non_thought_text_signature_replay", func(t *testing.T) {
		runAntigravityInlineImageAndTextSignatureSuite(t, allClients, agSpecs)
	})
}

// runAntigravitySparseTailWindowSuite verifies that ReadConvertedStreaming
// stops downstream keepalive heartbeats upon finishReason while continuing to
// collect sparse usageMetadata tail frames within 5 seconds. Each client
// protocol and model family must settle merged token buckets matching client SSE.
func runAntigravitySparseTailWindowSuite(t *testing.T, clients []labClientSpec, agSpecs []labProviderKindSpec) {
	t.Helper()
	for _, prov := range agSpecs {
		for _, client := range clients {
			t.Run(client.slug+"_via_"+prov.kind, func(t *testing.T) {
				upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					raw, rErr := io.ReadAll(r.Body)
					require.NoError(t, rErr)
					captured, envErr := unwrapAntigravityV1InternalEnvelope(r.URL.Path, r.URL.RawQuery, r.Header, raw)
					require.NoError(t, envErr)
					require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, captured.InnerBody, prov.validation))

					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					flusher, ok := w.(http.Flusher)
					require.True(t, ok)

					_, _ = fmt.Fprintf(w, "data: {\"responseId\":\"ag_tail_1\",\"modelVersion\":%q,\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ag-sparse-tail-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":60,\"cachedContentTokenCount\":15,\"candidatesTokenCount\":0,\"totalTokenCount\":60}}}\n\n", prov.upstreamModel)
					flusher.Flush()

					// Delay the sparse usageMetadata tail frame inside ReadGeminiStream's 5s tail
					// collection window after c.Complete() has stopped downstream keepalives.
					time.Sleep(120 * time.Millisecond)
					_, _ = io.WriteString(w, "data: {\"responseId\":\"ag_tail_1\",\"response\":{\"usageMetadata\":{\"candidatesTokenCount\":18,\"thoughtsTokenCount\":7,\"totalTokenCount\":85}}}\n\n")
					flusher.Flush()
				}))
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers:         []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, prov)},
					KeepaliveInterval: 40 * time.Millisecond,
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				var reqPayload []byte
				switch client.protocol {
				case protocol.ProtocolAnthropicMessages:
					reqPayload = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"tail usage"}]}`)
				case protocol.ProtocolOpenAIResponses:
					reqPayload = []byte(`{"model":"gpt-5","stream":true,"input":"tail usage"}`)
				case protocol.ProtocolOpenAIChatCompletions:
					reqPayload = []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"tail usage"}]}`)
				}

				status, ct, respBytes := sendLabClientRequest(t, gwSrv, client, reqPayload)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, strings.ToLower(ct), "text/event-stream")
				require.Contains(t, string(respBytes), "ag-sparse-tail-ok")

				frames, fErr := readAllSSEFrames(respBytes)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(client.protocol, frames, testkit.GrammarOptions{}),
					"client stream frames: %s", string(respBytes))

				logs := lab.UsageLogs()
				require.Len(t, logs, 1)
				require.Equal(t, 45, logs[0].InputTokens)
				require.Equal(t, 15, logs[0].CacheReadTokens)
				require.Equal(t, 25, logs[0].OutputTokens)
				require.Empty(t, compareClientAndSettledUsage(client.protocol, frames, logs))
			})
		}
	}
}

// runAntigravityFailoverSuite exercises Antigravity structured 429 RetryInfo
// model-limit switching, Google project configuration 400 failover, and HTTP 200
// SSE leading non-semantic frame staging failover. Primary non-semantic bytes
// must remain hidden from clients while UsageLog attributes Provider 2.
func runAntigravityFailoverSuite(t *testing.T, clients []labClientSpec, agSpecs []labProviderKindSpec) {
	t.Helper()
	secondarySSE := "data: {\"responseId\":\"ag_stage_p2\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"antigravity-fallback-p2-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":16,\"candidatesTokenCount\":5,\"totalTokenCount\":21}}\n\n"

	t.Run("http200_sse_leading_frame_staging_failover", func(t *testing.T) {
		primarySSE := "data: {\"response\":{\"responseId\":\"ag_stage_p1\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[]}}],\"usageMetadata\":{\"promptTokenCount\":12,\"candidatesTokenCount\":0,\"totalTokenCount\":12}}}\n\n" +
			"data: {\"error\":{\"code\":503,\"message\":\"Model capacity exhausted\",\"status\":\"UNAVAILABLE\"}}\n\n"

		for _, provSpec := range agSpecs {
			for _, client := range clients {
				t.Run(client.slug+"_via_"+provSpec.kind, func(t *testing.T) {
					p1Mock := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "ag_p1_sse_fail", Default: testkit.Reply{Raw: primarySSE}},
						Validation: provSpec.validation,
					})
					p2Mock := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "ag_p2_sse_ok", Default: testkit.Reply{Raw: secondarySSE}},
						Validation: provSpec.validation,
					})
					p1Srv := newLabMockServer(provSpec, p1Mock)
					defer p1Srv.Close()
					p2Srv := newLabMockServer(provSpec, p2Mock)
					defer p2Srv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{
							makeLabProviderSpec(1, 0, p1Srv.URL, provSpec),
							makeLabProviderSpec(2, 1, p2Srv.URL, provSpec),
						},
						MaxProviderSwitches: 2,
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					var reqPayload []byte
					switch client.protocol {
					case protocol.ProtocolAnthropicMessages:
						reqPayload = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"ping"}]}`)
					case protocol.ProtocolOpenAIResponses:
						reqPayload = []byte(`{"model":"gpt-5","stream":true,"input":"ping"}`)
					case protocol.ProtocolOpenAIChatCompletions:
						reqPayload = []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"ping"}]}`)
					}

					status, ct, respBytes := sendLabClientRequest(t, gwSrv, client, reqPayload)
					require.Equal(t, http.StatusOK, status)
					require.Contains(t, strings.ToLower(ct), "text/event-stream")
					require.Contains(t, string(respBytes), "antigravity-fallback-p2-ok")
					require.NotContains(t, string(respBytes), "ag_stage_p1")
					require.Len(t, p1Srv.Requests(), 1)
					require.Len(t, p2Srv.Requests(), 1)

					frames, err := readAllSSEFrames(respBytes)
					require.NoError(t, err)
					require.Empty(t, testkit.ValidateStreamGrammar(client.protocol, frames, testkit.GrammarOptions{}))
					logs := lab.UsageLogs()
					require.Len(t, logs, 1)
					require.Equal(t, int64(2), logs[0].ProviderID)
				})
			}
		}
	})

	t.Run("structured_429_model_limit_and_project_config_400_failover", func(t *testing.T) {
		for _, provSpec := range agSpecs {
			for _, errCase := range []struct {
				name   string // failover trigger label
				status int    // upstream HTTP error status
				body   string // upstream Google RPC error payload
			}{
				{
					name:   "smart_retry_429_long_model_limit",
					status: http.StatusTooManyRequests,
					body:   fmt.Sprintf(`{"error":{"code":429,"message":"Quota exceeded","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED","metadata":{"model":%q}},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"30s"}]}}`, provSpec.upstreamModel),
				},
				{
					name:   "google_project_config_400",
					status: http.StatusBadRequest,
					body:   `{"error":{"code":400,"message":"Invalid project resource name projects/bad-proj","status":"INVALID_ARGUMENT"}}`,
				},
			} {
				t.Run(provSpec.kind+"_"+errCase.name, func(t *testing.T) {
					p1Mock := mockupstream.NewWithConfig(mockupstream.Config{
						Script: testkit.Script{
							Name: "ag_p1_" + errCase.name,
							Turns: []testkit.Turn{{
								Fault: &testkit.Fault{Status: errCase.status, ErrorBody: errCase.body},
							}},
						},
						Validation: provSpec.validation,
					})
					p2Mock := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "ag_p2_ok", Default: testkit.Reply{Raw: secondarySSE}},
						Validation: provSpec.validation,
					})
					p1Srv := newLabMockServer(provSpec, p1Mock)
					defer p1Srv.Close()
					p2Srv := newLabMockServer(provSpec, p2Mock)
					defer p2Srv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{
							makeLabProviderSpec(1, 0, p1Srv.URL, provSpec),
							makeLabProviderSpec(2, 1, p2Srv.URL, provSpec),
						},
						MaxProviderSwitches: 2,
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					status, _, respBytes := sendLabClientRequest(
						t,
						gwSrv,
						protocolLabClients[1],
						[]byte(`{"model":"gpt-5","stream":true,"input":"switch provider"}`),
					)
					require.Equal(t, http.StatusOK, status, string(respBytes))
					require.Contains(t, string(respBytes), "antigravity-fallback-p2-ok")
					require.NotEmpty(t, p1Srv.Requests())
					require.Len(t, p2Srv.Requests(), 1)
					require.Len(t, lab.UsageLogs(), 1)
					require.Equal(t, int64(2), lab.UsageLogs()[0].ProviderID)
				})
			}
		}
	})
}

// runAntigravityTimeoutAndErrorPolicySuite verifies upstream header and stream
// idle DeadlineExceeded timeouts, CommittedSSE untimed terminal SSE errors, and
// ApplyErrorPassthroughRule status and message overrides across Antigravity
// routes. Mid-stream idle timeouts must close open blocks without contacting
// secondary providers.
func runAntigravityTimeoutAndErrorPolicySuite(t *testing.T, clients []labClientSpec, agSpecs []labProviderKindSpec) {
	t.Helper()

	t.Run("header_timeout_after_keepalive_commits_sse_writes_terminal_error", func(t *testing.T) {
		slowSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Hold response headers past the 25ms keepalive ping and 90ms HeaderTimeout
			// so the downstream SSE stream is already committed when timeout fires.
			select {
			case <-r.Context().Done():
			case <-time.After(500 * time.Millisecond):
			}
			w.WriteHeader(http.StatusGatewayTimeout)
		}))
		defer slowSrv.Close()

		for _, client := range clients {
			t.Run(client.slug, func(t *testing.T) {
				lab := newProtocolLabRouter(t, labOptions{
					Providers:           []labProvider{makeLabProviderSpec(101, 0, slowSrv.URL, agSpecs[0])},
					MaxProviderSwitches: 1,
					KeepaliveInterval:   25 * time.Millisecond,
					HeaderTimeout:       90 * time.Millisecond,
					StallTimeout:        time.Second,
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				var reqPayload []byte
				switch client.protocol {
				case protocol.ProtocolAnthropicMessages:
					reqPayload = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"timeout after ping"}]}`)
				case protocol.ProtocolOpenAIResponses:
					reqPayload = []byte(`{"model":"gpt-5","stream":true,"input":"timeout after ping"}`)
				case protocol.ProtocolOpenAIChatCompletions:
					reqPayload = []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"timeout after ping"}]}`)
				}

				status, ct, respBytes := sendLabClientRequest(t, gwSrv, client, reqPayload)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, strings.ToLower(ct), "text/event-stream")
				frames, err := readAllSSEFrames(respBytes)
				require.NoError(t, err)
				require.True(t, hasLabAbnormalTerminalErrorFrame(client.protocol, frames),
					"expected terminal SSE error frame after CommittedSSE timeout, got: %s", string(respBytes))
			})
		}
	})

	t.Run("midstream_idle_deadline_exceeded_closes_blocks_and_settles_partial_usage", func(t *testing.T) {
		for _, provSpec := range agSpecs {
			t.Run(provSpec.kind, func(t *testing.T) {
				stallSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(http.StatusOK)
					flusher, ok := w.(http.Flusher)
					require.True(t, ok)

					_, _ = io.WriteString(w, "data: {\"responseId\":\"ag_idle_1\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"partial-before-idle-timeout\"}]}}],\"usageMetadata\":{\"promptTokenCount\":34,\"cachedContentTokenCount\":4,\"candidatesTokenCount\":5,\"totalTokenCount\":39}}}\n\n")
					flusher.Flush()

					// Hold the SSE body open past the 1s Google stream idle timeout while
					// still exiting promptly when r.Context() cancels.
					select {
					case <-r.Context().Done():
					case <-time.After(1300 * time.Millisecond):
					}
				}))
				defer stallSrv.Close()

				unusedMock := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "unused_p2", Default: testkit.Reply{Raw: "wire/gemini_generate_content/stream/text_basic.sse"}},
					Validation: provSpec.validation,
				})
				unusedSrv := newLabMockServer(provSpec, unusedMock)
				defer unusedSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{
						makeLabProviderSpec(1, 0, stallSrv.URL, provSpec),
						makeLabProviderSpec(2, 1, unusedSrv.URL, provSpec),
					},
					MaxProviderSwitches: 2,
					StallTimeout:        time.Second,
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				status, ct, respBytes := sendLabClientRequest(
					t,
					gwSrv,
					protocolLabClients[1],
					[]byte(`{"model":"gpt-5","stream":true,"input":"stall midstream"}`),
				)
				require.Equal(t, http.StatusOK, status)
				require.Contains(t, strings.ToLower(ct), "text/event-stream")
				require.Contains(t, string(respBytes), "partial-before-idle-timeout")
				require.Empty(t, unusedSrv.Requests(), "secondary provider must not be contacted after partial output")

				frames, fErr := readAllSSEFrames(respBytes)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{AllowAbnormal: true}),
					"stream frames: %s", string(respBytes))
				require.True(t, hasLabAbnormalTerminalErrorFrame(protocol.ProtocolOpenAIResponses, frames))

				logs := lab.UsageLogs()
				require.Len(t, logs, 1)
				require.Equal(t, 30, logs[0].InputTokens)
				require.Equal(t, 4, logs[0].CacheReadTokens)
				require.Equal(t, 5, logs[0].OutputTokens)
			})
		}
	})

	t.Run("error_passthrough_rule_uncommitted_json_and_committed_sse", func(t *testing.T) {
		mappedCode := http.StatusTooManyRequests
		mappedMsg := "Mapped by administrator Antigravity passthrough rule"
		rule := &errorpolicy.ErrorPassthroughRule{
			ID:             44,
			Name:           "antigravity-quota-gate",
			Enabled:        true,
			Platforms:      []string{capability.PlatformAntigravity},
			MatchMode:      errorpolicy.MatchModeAll,
			ErrorCodes:     []int{http.StatusBadRequest},
			Keywords:       []string{"vendor_specific_quota_gate"},
			ResponseCode:   &mappedCode,
			CustomMessage:  &mappedMsg,
			SkipMonitoring: true,
		}

		t.Run("uncommitted_http_error", func(t *testing.T) {
			errSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":{"code":400,"message":"vendor_specific_quota_gate: account blocked","status":"INVALID_ARGUMENT"}}`)
			}))
			defer errSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers:  []labProvider{makeLabProviderSpec(101, 0, errSrv.URL, agSpecs[0])},
				ErrorRules: []*errorpolicy.ErrorPassthroughRule{rule},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			for _, client := range []labClientSpec{protocolLabClients[0], antigravityDedicatedMessagesClient()} {
				body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"stream":false,"messages":[{"role":"user","content":"gate"}]}`)
				st, _, respBytes := sendLabClientRequest(t, gwSrv, client, body)
				require.Equal(t, http.StatusTooManyRequests, st, "%s: %s", client.slug, string(respBytes))
				require.True(t, hasClientErrorEnvelope(client.protocol, respBytes), "%s: %s", client.slug, string(respBytes))
				require.Contains(t, string(respBytes), mappedMsg, client.slug)
			}
			for _, client := range []labClientSpec{protocolLabClients[1], protocolLabClients[2]} {
				var body []byte
				if client.protocol == protocol.ProtocolOpenAIResponses {
					body = []byte(`{"model":"gpt-5","stream":false,"input":"gate"}`)
				} else {
					body = []byte(`{"model":"gpt-4o","stream":false,"messages":[{"role":"user","content":"gate"}]}`)
				}
				st, _, respBytes := sendLabClientRequest(t, gwSrv, client, body)
				require.Equal(t, http.StatusBadRequest, st, "%s: %s", client.slug, string(respBytes))
				require.True(t, hasClientErrorEnvelope(client.protocol, respBytes), "%s: %s", client.slug, string(respBytes))
				require.Contains(t, string(respBytes), "Upstream request failed", client.slug)
			}
		})

		t.Run("committed_sse_after_keepalive_heartbeat", func(t *testing.T) {
			delayedErrSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-r.Context().Done():
					return
				case <-time.After(75 * time.Millisecond):
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				_, _ = io.WriteString(w, `{"error":{"code":400,"message":"vendor_specific_quota_gate: delayed block","status":"INVALID_ARGUMENT"}}`)
			}))
			defer delayedErrSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers:         []labProvider{makeLabProviderSpec(101, 0, delayedErrSrv.URL, agSpecs[0])},
				KeepaliveInterval: 20 * time.Millisecond,
				ErrorRules:        []*errorpolicy.ErrorPassthroughRule{rule},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			for _, client := range []labClientSpec{protocolLabClients[0], antigravityDedicatedMessagesClient()} {
				st, ct, respBytes := sendLabClientRequest(
					t,
					gwSrv,
					client,
					[]byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"stream":true,"messages":[{"role":"user","content":"delayed gate"}]}`),
				)
				require.Equal(t, http.StatusOK, st, client.slug)
				require.Contains(t, strings.ToLower(ct), "text/event-stream")
				require.Contains(t, string(respBytes), mappedMsg, client.slug)
				frames, err := readAllSSEFrames(respBytes)
				require.NoError(t, err)
				require.True(t, hasLabAbnormalTerminalErrorFrame(client.protocol, frames), string(respBytes))
			}
		})
	})
}

// runAntigravityClientValidationSuite verifies that client-side HTTP 400
// rejections (including "ultra" effort, explicit disabled thinking on
// non-toggle claude-* and gemini-* models, zero budget_tokens, malformed JSON,
// and missing model) format in the client protocol envelope. Rejected inputs
// must never contact the upstream server or penalize provider scheduler health.
func runAntigravityClientValidationSuite(t *testing.T, clients []labClientSpec, agSpecs []labProviderKindSpec) {
	t.Helper()
	okSSE := "data: {\"responseId\":\"ag_after_400\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"antigravity-still-healthy\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":9,\"candidatesTokenCount\":3,\"totalTokenCount\":12}}\n\n"

	for _, provSpec := range agSpecs {
		t.Run(provSpec.kind, func(t *testing.T) {
			mockH := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "ag_health_guard", Default: testkit.Reply{Raw: okSSE}},
				Validation: provSpec.validation,
			})
			upSrv := newLabMockServer(provSpec, mockH)
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, provSpec)},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			badPayloads := []struct {
				name    string        // rejection case label
				client  labClientSpec // inbound client protocol descriptor
				body    []byte        // invalid client request payload
				wantMsg string        // expected substring inside HTTP 400 error body
			}{
				{
					name:   "messages_ultra_effort",
					client: clients[0],
					body:   []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"output_config":{"effort":"ultra"},"messages":[{"role":"user","content":"hi"}]}`),
				},
				{
					name:   "dedicated_messages_ultra_effort",
					client: clients[1],
					body:   []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"output_config":{"effort":"ultra"},"messages":[{"role":"user","content":"hi"}]}`),
				},
				{
					name:   "responses_ultra_effort",
					client: clients[2],
					body:   []byte(`{"model":"gpt-5","stream":false,"reasoning":{"effort":"ultra"},"input":"hi"}`),
				},
				{
					name:   "chat_ultra_effort",
					client: clients[3],
					body:   []byte(`{"model":"gpt-4o","stream":false,"reasoning_effort":"ultra","messages":[{"role":"user","content":"hi"}]}`),
				},
				{
					name:    "messages_disabled_thinking_on_non_toggle_model",
					client:  clients[0],
					body:    []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`),
					wantMsg: "selected model does not support disabling thinking",
				},
				{
					name:    "dedicated_messages_disabled_thinking_on_non_toggle_model",
					client:  clients[1],
					body:    []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"hi"}]}`),
					wantMsg: "selected model does not support disabling thinking",
				},
				{
					name:    "messages_zero_budget_enabled_thinking",
					client:  clients[0],
					body:    []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"thinking":{"type":"enabled","budget_tokens":0},"messages":[{"role":"user","content":"hi"}]}`),
					wantMsg: "enabled mode requires positive budget_tokens",
				},
				{
					name:    "responses_effort_none_on_non_toggle_model",
					client:  clients[2],
					body:    []byte(`{"model":"gpt-5","stream":false,"reasoning":{"effort":"none"},"input":"hi"}`),
					wantMsg: "selected model does not support disabling thinking",
				},
				{
					name:    "chat_effort_none_on_non_toggle_model",
					client:  clients[3],
					body:    []byte(`{"model":"gpt-4o","stream":false,"reasoning_effort":"none","messages":[{"role":"user","content":"hi"}]}`),
					wantMsg: "selected model does not support disabling thinking",
				},
				{
					name:   "responses_missing_model",
					client: clients[2],
					body:   []byte(`{"stream":false,"input":"hi"}`),
				},
				{
					name:   "chat_malformed_json",
					client: clients[3],
					body:   []byte(`{"model":"gpt-4o","messages":[`),
				},
			}

			for _, bp := range badPayloads {
				st, _, errBytes := sendLabClientRequest(t, gwSrv, bp.client, bp.body)
				require.Equal(t, http.StatusBadRequest, st, "%s: %s", bp.name, string(errBytes))
				require.True(t, hasClientErrorEnvelope(bp.client.protocol, errBytes), "%s: %s", bp.name, string(errBytes))
				if bp.wantMsg != "" {
					require.Contains(t, string(errBytes), bp.wantMsg, bp.name)
				}
			}
			require.Empty(t, upSrv.Requests(), "upstream must not be contacted on local 400 validation failure")
			require.Empty(t, lab.UsageLogs())

			// Confirm Provider 101 has zero error/cooldown penalty and immediately serves a valid request.
			rec, err := fixtureRetryStore{lab.providers}.GetByID(t.Context(), 101)
			require.NoError(t, err)
			require.Nil(t, rec.TempUnschedulableUntil)
			require.Nil(t, rec.RateLimitResetAt)
			require.True(t, rec.Schedulable)

			stOK, _, okBytes := sendLabClientRequest(
				t,
				gwSrv,
				clients[2],
				[]byte(`{"model":"gpt-5","stream":true,"input":"healthy check"}`),
			)
			require.Equal(t, http.StatusOK, stOK, string(okBytes))
			require.Contains(t, string(okBytes), "antigravity-still-healthy")
			require.Len(t, upSrv.Requests(), 1)
			require.Len(t, lab.UsageLogs(), 1)
		})
	}

	t.Run("toggle_capable_gemini_2_5_flash_accepts_disabled_thinking", func(t *testing.T) {
		flashSpec := agSpecs[1]
		flashSpec.upstreamModel = "gemini-2.5-flash"
		mockH := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "ag_flash_toggle", Default: testkit.Reply{Raw: okSSE}},
			Validation: flashSpec.validation,
		})
		upSrv := newLabMockServer(flashSpec, mockH)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, flashSpec)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		st, _, respBytes := sendLabClientRequest(
			t,
			gwSrv,
			protocolLabClients[1],
			[]byte(`{"model":"gpt-5","stream":true,"reasoning":{"effort":"none"},"input":"flash toggle disabled"}`),
		)
		require.Equal(t, http.StatusOK, st, string(respBytes))
		require.Contains(t, string(respBytes), "antigravity-still-healthy")
		require.Len(t, upSrv.Requests(), 1)
		sent := upSrv.Requests()[0].Body
		require.False(t, gjson.GetBytes(sent, "generationConfig.thinkingConfig.includeThoughts").Bool())
		require.Equal(t, int64(0), gjson.GetBytes(sent, "generationConfig.thinkingConfig.thinkingBudget").Int())
	})
}

// runAntigravityRecoveryPipelinesSuite exercises RecoverClaudeIR (thinking-only,
// thinking+tools, and non-adaptive budget rectification) and RecoverGemini
// (same-model signature cleanup, 404 fallback model switch with two-turn
// CallMemory persistence, and cross-family 404 fallback carrier sealing). All
// recovery retries run end to end through the assembled gateway router.
func runAntigravityRecoveryPipelinesSuite(t *testing.T, agSpecs []labProviderKindSpec) {
	t.Helper()

	t.Run("claude_ir_thinking_only_thinking_plus_tools_and_budget_rectification", func(t *testing.T) {
		staleClaudeCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAntigravityClaude,
			Data:   "stale_claude_sig_turn1",
			Text:   "prior claude reasoning",
		})
		require.NoError(t, err)

		reqWithCustomTool := []byte(fmt.Sprintf(`{
			"model":"claude-opus-4-6-thinking",
			"stream":false,
			"reasoning":{"effort":"high"},
			"tools":[{"type":"custom","name":"apply_patch","description":"Apply unified patch"}],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"Fix handler"}]},
				{"type":"reasoning","id":"rs_cl_1","summary":[],"encrypted_content":%q},
				{"type":"custom_tool_call","call_id":"call_cl_1","name":"apply_patch","input":"*** Begin Patch"},
				{"type":"custom_tool_call_output","call_id":"call_cl_1","output":"patched"}
			]
		}`, staleClaudeCarrier))

		recoveredClaudeSSE := "data: {\"responseId\":\"ag_cl_rec\",\"modelVersion\":\"claude-opus-4-6-thinking\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"fresh opus thought\",\"thought\":true,\"thoughtSignature\":\"sig_opus_fresh\"},{\"functionCall\":{\"id\":\"call_cl_2\",\"name\":\"apply_patch\",\"args\":{\"input\":\"*** End Patch\"}}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":30,\"candidatesTokenCount\":7,\"thoughtsTokenCount\":4,\"totalTokenCount\":41}}}\n\n"

		t.Run("stage1_thinking_only_preserves_custom_tool_map_and_seals_carrier", func(t *testing.T) {
			var mu sync.Mutex
			var attempts [][]byte
			upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				captured, envErr := unwrapAntigravityV1InternalEnvelope(r.URL.Path, r.URL.RawQuery, r.Header, raw)
				require.NoError(t, envErr)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, captured.InnerBody, agSpecs[0].validation))

				mu.Lock()
				idx := len(attempts)
				attempts = append(attempts, captured.InnerBody)
				mu.Unlock()

				if idx == 0 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":{"code":400,"message":"Invalid signature in thinking block","status":"INVALID_ARGUMENT"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, recoveredClaudeSSE)
			}))
			defer upSrv.Close()

			prov := makeLabProviderSpec(101, 0, upSrv.URL, agSpecs[0])
			prov.Models = append(prov.Models, "claude-opus-4-6-thinking")
			prov.ModelMapping["claude-opus-4-6-thinking"] = "claude-opus-4-6-thinking"
			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{prov},
				Settings:  map[string]string{gateway.SettingKeyIdentityPatchPrompt: "Operator recovery identity."},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			st, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], reqWithCustomTool)
			require.Equal(t, http.StatusOK, st, string(respBytes))
			require.Contains(t, string(respBytes), `"type":"custom_tool_call"`)
			require.Contains(t, string(respBytes), `"input":"*** End Patch"`)
			require.Contains(t, string(respBytes), "trc1:antigravity_claude:")

			mu.Lock()
			gotAttempts := slices.Clone(attempts)
			mu.Unlock()
			require.Len(t, gotAttempts, 2)
			// Attempt 1 carried unpacked thought + functionCall + thinkingConfig; Stage 1 retry downgraded thought to plain text, cleared thinkingConfig, and kept functionCall at parts[1].
			require.True(t, gjson.GetBytes(gotAttempts[0], "contents.1.parts.0.thought").Bool())
			require.Equal(t, "Operator recovery identity.", gjson.GetBytes(gotAttempts[1], "systemInstruction.parts.0.text").String())
			require.False(t, gjson.GetBytes(gotAttempts[1], "generationConfig.thinkingConfig").Exists())
			require.Equal(t, "prior claude reasoning", gjson.GetBytes(gotAttempts[1], "contents.1.parts.0.text").String())
			require.False(t, gjson.GetBytes(gotAttempts[1], "contents.1.parts.0.thought").Bool())
			require.Equal(t, "apply_patch", gjson.GetBytes(gotAttempts[1], "contents.1.parts.1.functionCall.name").String())
			require.Len(t, lab.UsageLogs(), 1)
		})

		t.Run("stage2_thinking_plus_tools_degrades_historical_tools_to_text", func(t *testing.T) {
			var mu sync.Mutex
			var attempts [][]byte
			upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				captured, envErr := unwrapAntigravityV1InternalEnvelope(r.URL.Path, r.URL.RawQuery, r.Header, raw)
				require.NoError(t, envErr)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, captured.InnerBody, agSpecs[0].validation))

				mu.Lock()
				idx := len(attempts)
				attempts = append(attempts, captured.InnerBody)
				mu.Unlock()

				switch idx {
				case 0:
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":{"code":400,"message":"Invalid signature in thinking block","status":"INVALID_ARGUMENT"}}`)
				case 1:
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":{"code":400,"message":"Expected thinking block before tool_use signature check","status":"INVALID_ARGUMENT"}}`)
				default:
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, recoveredClaudeSSE)
				}
			}))
			defer upSrv.Close()

			prov := makeLabProviderSpec(101, 0, upSrv.URL, agSpecs[0])
			prov.Models = append(prov.Models, "claude-opus-4-6-thinking")
			prov.ModelMapping["claude-opus-4-6-thinking"] = "claude-opus-4-6-thinking"
			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{prov},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			st, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], reqWithCustomTool)
			require.Equal(t, http.StatusOK, st, string(respBytes))
			require.Contains(t, string(respBytes), `"type":"custom_tool_call"`)

			mu.Lock()
			gotAttempts := slices.Clone(attempts)
			mu.Unlock()
			require.Len(t, gotAttempts, 3)
			// Attempt 2 kept functionCall at parts[1]; Attempt 3 degraded historical functionCall/functionResponse into plain text markers while retaining declared tools.
			require.Equal(t, "apply_patch", gjson.GetBytes(gotAttempts[1], "contents.1.parts.1.functionCall.name").String())
			for _, p := range gjson.GetBytes(gotAttempts[2], "contents.1.parts").Array() {
				require.False(t, p.Get("functionCall").Exists())
			}
			require.NotEmpty(t, gjson.GetBytes(gotAttempts[2], "contents.1.parts.0.text").String())
			require.Equal(t, "apply_patch", gjson.GetBytes(gotAttempts[2], "tools.0.functionDeclarations.0.name").String())
		})

		t.Run("budget_constraint_400_rectifies_non_adaptive_budget", func(t *testing.T) {
			var mu sync.Mutex
			var attempts [][]byte
			upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				captured, envErr := unwrapAntigravityV1InternalEnvelope(r.URL.Path, r.URL.RawQuery, r.Header, raw)
				require.NoError(t, envErr)

				mu.Lock()
				idx := len(attempts)
				attempts = append(attempts, captured.InnerBody)
				mu.Unlock()

				if idx == 0 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":{"code":400,"message":"thinking.budget_tokens: Input should be greater than or equal to 1024","status":"INVALID_ARGUMENT"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"responseId\":\"ag_bud_ok\",\"modelVersion\":\"claude-sonnet-4-5-thinking\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"budget-rectified-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":14,\"candidatesTokenCount\":4,\"totalTokenCount\":18}}}\n\n")
			}))
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, agSpecs[0])},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			budgetReq := []byte(`{"model":"claude-sonnet-4-5","max_tokens":4096,"stream":true,"thinking":{"type":"enabled","budget_tokens":2048},"messages":[{"role":"user","content":"rectify budget"}]}`)
			st, _, respBytes := sendLabClientRequest(t, gwSrv, antigravityDedicatedMessagesClient(), budgetReq)
			require.Equal(t, http.StatusOK, st, string(respBytes))
			require.Contains(t, string(respBytes), "budget-rectified-ok")

			mu.Lock()
			gotAttempts := slices.Clone(attempts)
			mu.Unlock()
			require.Len(t, gotAttempts, 2)
			require.Equal(t, int64(2048), gjson.GetBytes(gotAttempts[0], "generationConfig.thinkingConfig.thinkingBudget").Int())
			require.Equal(t, int64(32000), gjson.GetBytes(gotAttempts[1], "generationConfig.thinkingConfig.thinkingBudget").Int())
			require.Equal(t, int64(64000), gjson.GetBytes(gotAttempts[1], "generationConfig.maxOutputTokens").Int())
		})
	})

	t.Run("gemini_signature_cleanup_404_fallback_call_memory_and_cross_family_sealing", func(t *testing.T) {
		t.Run("same_model_signature_400_replaces_thought_signatures_and_retains_exchange", func(t *testing.T) {
			gemCarrier, err := ir.FormatCarrier(ir.Carrier{
				Scheme: ir.SchemeGemini,
				Data:   "stale_gem_sig_turn1",
			})
			require.NoError(t, err)

			var mu sync.Mutex
			var attempts [][]byte
			var record telemetry.RequestRecord
			upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				captured, envErr := unwrapAntigravityV1InternalEnvelope(r.URL.Path, r.URL.RawQuery, r.Header, raw)
				require.NoError(t, envErr)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, captured.InnerBody, agSpecs[1].validation))

				mu.Lock()
				idx := len(attempts)
				attempts = append(attempts, captured.InnerBody)
				mu.Unlock()

				if idx == 0 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusBadRequest)
					_, _ = io.WriteString(w, `{"error":{"code":400,"message":"Corrupted thoughtSignature in request","status":"INVALID_ARGUMENT"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"responseId\":\"ag_gem_sig_ok\",\"modelVersion\":\"gemini-2.5-pro\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"fresh gem thought\",\"thought\":true,\"thoughtSignature\":\"CpEBCgYIAxgD\"},{\"text\":\"gem-sig-recovered\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":20,\"candidatesTokenCount\":5,\"thoughtsTokenCount\":3,\"totalTokenCount\":28}}}\n\n")
			}))
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				OnTelemetry: func(snapshot telemetry.RequestRecord) {
					mu.Lock()
					defer mu.Unlock()
					record = snapshot
				},
				Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, agSpecs[1])},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			reqBody := []byte(fmt.Sprintf(`{
				"model":"gpt-5",
				"stream":true,
				"tools":[{"type":"function","name":"check_id","parameters":{"type":"object","properties":{"n":{"type":"integer"}}}}],
				"input":[
					{"type":"message","role":"user","content":"check 42"},
					{"type":"reasoning","id":"rs_gem_1","summary":[{"type":"summary_text","text":"prior"}],"encrypted_content":%q},
					{"type":"function_call","call_id":"call_g1","name":"check_id","arguments":"{\"n\":42}"},
					{"type":"function_call_output","call_id":"call_g1","output":"ok"}
				]
			}`, gemCarrier))

			st, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], reqBody)
			require.Equal(t, http.StatusOK, st, string(respBytes))
			require.Contains(t, string(respBytes), "gem-sig-recovered")
			require.Contains(t, string(respBytes), "trc1:gemini:")

			mu.Lock()
			gotAttempts := slices.Clone(attempts)
			mu.Unlock()
			require.Len(t, gotAttempts, 2)
			require.Equal(t, "stale_gem_sig_turn1", gjson.GetBytes(gotAttempts[0], "contents.1.parts.0.thoughtSignature").String())
			require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(gotAttempts[1], "contents.1.parts.0.thoughtSignature").String())
			require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(gotAttempts[1], "contents.1.parts.1.thoughtSignature").String())
			mu.Lock()
			final := record
			mu.Unlock()
			require.Len(t, final.Attempts, 2)
			require.NotNil(t, final.Attempts[0].ProtocolConversion)
			require.Zero(t, final.Attempts[0].ProtocolConversion.LossCounts[string(ir.LossReplayTokenDropped)])
			require.NotNil(t, final.Attempts[1].ProtocolConversion)
			require.Equal(t, 1, final.Attempts[1].ProtocolConversion.LossCounts[string(ir.LossReplayTokenDropped)])
			logs := lab.UsageLogs()
			require.Len(t, logs, 1)
			require.Equal(t, final.Attempts[1].ProtocolConversion, logs[0].ProtocolConversion)
		})

		t.Run("model_404_fallback_switches_exchange_and_persists_two_turn_call_memory", func(t *testing.T) {
			var mu sync.Mutex
			var envs []protocolLabAntigravityRequest
			upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				captured, envErr := unwrapAntigravityV1InternalEnvelope(r.URL.Path, r.URL.RawQuery, r.Header, raw)
				require.NoError(t, envErr)
				require.Empty(t, testkit.ValidateRequestRules(protocol.ProtocolGeminiGenerateContent, captured.InnerBody, agSpecs[1].validation))

				mu.Lock()
				idx := len(envs)
				envs = append(envs, captured)
				mu.Unlock()

				if captured.Model == "gemini-3.1-pro-high" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusNotFound)
					_, _ = io.WriteString(w, `{"error":{"code":404,"message":"Model gemini-3.1-pro-high was not found","status":"NOT_FOUND"}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if idx == 1 {
					_, _ = io.WriteString(w, "data: {\"responseId\":\"ag_fb_t1\",\"modelVersion\":\"gemini-2.5-pro\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"up_fb_call_1\",\"name\":\"lookup_order\",\"args\":{\"order\":\"A1\"}},\"thoughtSignature\":\"CpEBCgIIAxgB\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":14,\"candidatesTokenCount\":6,\"totalTokenCount\":20}}}\n\n")
					return
				}
				_, _ = io.WriteString(w, "data: {\"responseId\":\"ag_fb_t2\",\"modelVersion\":\"gemini-2.5-pro\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Order A1 shipped via fallback\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":26,\"candidatesTokenCount\":5,\"totalTokenCount\":31}}}\n\n")
			}))
			defer upSrv.Close()

			prov := makeLabProviderSpec(101, 0, upSrv.URL, agSpecs[1])
			prov.Models = []string{"gemini-3.1-pro-high", "gemini-2.5-pro"}
			prov.ModelMapping["gemini-3.1-pro-high"] = "gemini-3.1-pro-high"
			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{prov},
				Settings: map[string]string{
					routing.SettingKeyEnableModelFallback:      "true",
					routing.SettingKeyFallbackModelAntigravity: "gemini-2.5-pro",
				},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			t1Req := []byte(`{"model":"gemini-3.1-pro-high","stream":true,"tools":[{"type":"function","name":"lookup_order","parameters":{"type":"object","properties":{"order":{"type":"string"}}}}],"input":"Check order A1"}`)
			st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Req)
			require.Equal(t, http.StatusOK, st1, string(resp1))
			frames1, err := readAllSSEFrames(resp1)
			require.NoError(t, err)

			t2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
				PrevRequest:              t1Req,
				ResponseFrames:           frames1,
				SynthesizeMissingOutputs: true,
				FollowUpUserText:         "confirm shipment",
			})
			require.NoError(t, err)
			st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Raw))
			require.Equal(t, http.StatusOK, st2, string(resp2))
			require.Contains(t, string(resp2), "Order A1 shipped via fallback")

			mu.Lock()
			gotEnvs := slices.Clone(envs)
			mu.Unlock()
			require.Len(t, gotEnvs, 4)
			require.Equal(t, "gemini-3.1-pro-high", gotEnvs[0].Model)
			require.Equal(t, "gemini-2.5-pro", gotEnvs[1].Model)
			require.Equal(t, "gemini-3.1-pro-high", gotEnvs[2].Model)
			require.Equal(t, "gemini-2.5-pro", gotEnvs[3].Model)
			// Turn 2's fallback attempt on gemini-2.5-pro must recall up_fb_call_1 and CpEBCgIIAxgB from fbScope CallMemory.
			require.Equal(t, "up_fb_call_1", gjson.GetBytes(gotEnvs[3].InnerBody, "contents.1.parts.0.functionCall.id").String())
			require.Equal(t, "CpEBCgIIAxgB", gjson.GetBytes(gotEnvs[3].InnerBody, "contents.1.parts.0.thoughtSignature").String())
			require.Len(t, lab.UsageLogs(), 2)
		})

		t.Run("model_404_cross_family_fallback_to_claude_seals_antigravity_claude_carrier", func(t *testing.T) {
			upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, rErr := io.ReadAll(r.Body)
				require.NoError(t, rErr)
				captured, envErr := unwrapAntigravityV1InternalEnvelope(r.URL.Path, r.URL.RawQuery, r.Header, raw)
				require.NoError(t, envErr)

				if captured.Model == "gemini-3.1-pro-high" {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusNotFound)
					_, _ = io.WriteString(w, `{"error":{"code":404,"message":"Model gemini-3.1-pro-high was not found","status":"NOT_FOUND"}}`)
					return
				}
				require.Equal(t, "claude-sonnet-4-5", captured.Model)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"responseId\":\"ag_cf_ok\",\"modelVersion\":\"claude-sonnet-4-5\",\"response\":{\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"claude fallback reasoning\",\"thought\":true,\"thoughtSignature\":\"sig_claude_fb_1\"},{\"text\":\"cross-family-fallback-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":15,\"candidatesTokenCount\":5,\"thoughtsTokenCount\":3,\"totalTokenCount\":23}}}\n\n")
			}))
			defer upSrv.Close()

			prov := makeLabProviderSpec(101, 0, upSrv.URL, agSpecs[1])
			prov.Models = []string{"gemini-3.1-pro-high", "claude-sonnet-4-5"}
			prov.ModelMapping["gemini-3.1-pro-high"] = "gemini-3.1-pro-high"
			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{prov},
				Settings: map[string]string{
					routing.SettingKeyEnableModelFallback:      "true",
					routing.SettingKeyFallbackModelAntigravity: "claude-sonnet-4-5",
				},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			st, _, respBytes := sendLabClientRequest(
				t,
				gwSrv,
				protocolLabClients[1],
				[]byte(`{"model":"gemini-3.1-pro-high","stream":false,"input":"cross family fallback"}`),
			)
			require.Equal(t, http.StatusOK, st, string(respBytes))
			require.Contains(t, string(respBytes), "cross-family-fallback-ok")
			carrierStr := gjson.GetBytes(respBytes, "output.0.encrypted_content").String()
			parsed, ok := ir.ParseCarrier(carrierStr)
			require.True(t, ok, "expected valid trc1 carrier, got %q", carrierStr)
			require.Equal(t, ir.SchemeAntigravityClaude, parsed.Scheme)
			require.Equal(t, "sig_claude_fb_1", parsed.Data)
			require.Equal(t, "claude fallback reasoning", parsed.Text)
		})
	})
}

// runAntigravityCallMemoryCursorAndIdentitySuite verifies multi-turn CallMemory
// bindings and cross-account isolation, single-hop Cursor input-shaped Chat
// requests onto Antigravity, and live runtime identity_patch_prompt updates.
// Setting updates must take effect immediately without rebuilding the router.
func runAntigravityCallMemoryCursorAndIdentitySuite(t *testing.T, agSpecs []labProviderKindSpec) {
	t.Helper()
	doneSSE := "data: {\"responseId\":\"ag_cm_done\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ag-call-memory-verified\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":28,\"candidatesTokenCount\":4,\"totalTokenCount\":32}}\n\n"

	t.Run("call_memory_multi_turn_late_signature_parallel_unsigned_and_family_isolation", func(t *testing.T) {
		for _, provSpec := range agSpecs {
			t.Run(provSpec.kind+"_explicit_and_late_signature", func(t *testing.T) {
				lateTurn1SSE := "data: {\"responseId\":\"ag_cm_late\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"ag_late_id_7\",\"name\":\"lookup\",\"args\":{\"city\":\"Zurich\"}}}]}}]}\n\n" +
					"data: {\"responseId\":\"ag_cm_late\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"ag_late_id_7\",\"name\":\"lookup\",\"args\":{\"city\":\"Zurich\"}},\"thoughtSignature\":\"CpEBCgQIAxgC\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":14,\"candidatesTokenCount\":6,\"totalTokenCount\":20}}\n\n"

				mockH := mockupstream.NewWithConfig(mockupstream.Config{
					Script: testkit.Script{
						Name: "ag_cm_late_" + provSpec.kind,
						Turns: []testkit.Turn{
							{Reply: testkit.Reply{Raw: lateTurn1SSE}},
							{Reply: testkit.Reply{Raw: doneSSE}},
						},
					},
					Validation: provSpec.validation,
				})
				upSrv := newLabMockServer(provSpec, mockH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, provSpec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				t1Req := []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"messages":[{"role":"user","content":"weather in Zurich"}]}`)
				st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[2], t1Req)
				require.Equal(t, http.StatusOK, st1)
				frames1, err := readAllSSEFrames(resp1)
				require.NoError(t, err)

				t2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIChatCompletions, testkit.TurnInput{
					PrevRequest:              t1Req,
					ResponseFrames:           frames1,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "summarize",
				})
				require.NoError(t, err)
				st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[2], prepareLabStreamRequestBody(protocol.ProtocolOpenAIChatCompletions, t2Raw))
				require.Equal(t, http.StatusOK, st2, string(resp2))
				require.Contains(t, string(resp2), "ag-call-memory-verified")
				require.Empty(t, upSrv.Violations())

				sent2 := upSrv.Requests()[1].Body
				require.Equal(t, "ag_late_id_7", gjson.GetBytes(sent2, "contents.1.parts.0.functionCall.id").String())
				require.Equal(t, "CpEBCgQIAxgC", gjson.GetBytes(sent2, "contents.1.parts.0.thoughtSignature").String())

				// Switching to another Antigravity provider (ID 202) misses Provider 101's CallMemory:
				// SchemaAntigravity retains the client call ID while omitting foreign signature on claude-*
				// or writing skip_thought_signature_validator on gemini-*.
				p2Mock := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "ag_cm_p2", Default: testkit.Reply{Raw: doneSSE}},
					Validation: provSpec.validation,
				})
				p2Srv := newLabMockServer(provSpec, p2Mock)
				defer p2Srv.Close()
				lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, p2Srv.URL, provSpec)})

				st3, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[2], prepareLabStreamRequestBody(protocol.ProtocolOpenAIChatCompletions, t2Raw))
				require.Equal(t, http.StatusOK, st3)
				require.Empty(t, p2Srv.Violations())
				sentP2 := p2Srv.Requests()[0].Body
				require.Equal(t, "ag_late_id_7", gjson.GetBytes(sentP2, "contents.1.parts.0.functionCall.id").String())
				if strings.HasPrefix(provSpec.upstreamModel, "gemini-") {
					require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(sentP2, "contents.1.parts.0.thoughtSignature").String())
				} else {
					require.False(t, gjson.GetBytes(sentP2, "contents.1.parts.0.thoughtSignature").Exists())
				}
			})
		}

		t.Run("gemini_parallel_unsigned_sibling_calls_4b05d79a5", func(t *testing.T) {
			unsignedTurn1SSE := "data: {\"responseId\":\"ag_cm_u\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"ag_u_1\",\"name\":\"lookup\",\"args\":{\"city\":\"Oslo\"}}},{\"functionCall\":{\"id\":\"ag_u_2\",\"name\":\"lookup\",\"args\":{\"city\":\"Lisbon\"}}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":16,\"candidatesTokenCount\":8,\"totalTokenCount\":24}}\n\n"
			mockH := mockupstream.NewWithConfig(mockupstream.Config{
				Script: testkit.Script{
					Name: "ag_parallel_unsigned",
					Turns: []testkit.Turn{
						{Reply: testkit.Reply{Raw: unsignedTurn1SSE}},
						{Reply: testkit.Reply{Raw: doneSSE}},
					},
				},
				Validation: agSpecs[1].validation,
			})
			upSrv := newLabMockServer(agSpecs[1], mockH)
			defer upSrv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, agSpecs[1])},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			t1Req := []byte(`{"model":"gpt-5","stream":true,"input":"compare Oslo and Lisbon","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
			st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Req)
			require.Equal(t, http.StatusOK, st1)
			frames1, err := readAllSSEFrames(resp1)
			require.NoError(t, err)

			t2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
				PrevRequest:              t1Req,
				ResponseFrames:           frames1,
				SynthesizeMissingOutputs: true,
				FollowUpUserText:         "summarize",
			})
			require.NoError(t, err)
			st2, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Raw))
			require.Equal(t, http.StatusOK, st2)
			require.Empty(t, upSrv.Violations())
			sent := upSrv.Requests()[1].Body
			require.Equal(t, "skip_thought_signature_validator", gjson.GetBytes(sent, "contents.1.parts.0.thoughtSignature").String())
			require.False(t, gjson.GetBytes(sent, "contents.1.parts.1.thoughtSignature").Exists())
		})
	})

	t.Run("cursor_input_shaped_chat_single_hop_to_antigravity", func(t *testing.T) {
		claudeCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAntigravityClaude,
			Data:   "sig_cursor_claude_1",
			Text:   "Plan patch on Claude.",
		})
		require.NoError(t, err)
		gemCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeGemini,
			Data:   "CpEBCgIIAxgB",
		})
		require.NoError(t, err)

		cursorStreamSSE := "data: {\"responseId\":\"ag_cur_s\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":\"ag_cur_patch_1\",\"name\":\"apply_patch\",\"args\":{\"input\":\"*** Begin Patch\\n*** End Patch\"}},\"thoughtSignature\":\"CpEBCgQIAxgC\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":25,\"candidatesTokenCount\":8,\"totalTokenCount\":33}}\n\n"
		cursorBufSSE := "data: {\"responseId\":\"ag_cur_b\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"cursor-antigravity-buffered-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":25,\"candidatesTokenCount\":5,\"totalTokenCount\":30}}\n\n"

		for _, tc := range []struct {
			prov    labProviderKindSpec // target Antigravity model-family archetype
			carrier string              // matching trc1 continuity carrier
			wantSig string              // expected unpacked thoughtSignature on inner Gemini request
		}{
			{prov: agSpecs[0], carrier: claudeCarrier, wantSig: "sig_cursor_claude_1"},
			{prov: agSpecs[1], carrier: gemCarrier, wantSig: "CpEBCgIIAxgB"},
		} {
			t.Run(tc.prov.kind, func(t *testing.T) {
				mockH := mockupstream.NewWithConfig(mockupstream.Config{
					Mode:       mockupstream.ModeReplay,
					Replay:     []testkit.Reply{{Raw: cursorStreamSSE}, {Raw: cursorBufSSE}},
					Validation: tc.prov.validation,
				})
				upSrv := newLabMockServer(tc.prov, mockH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, tc.prov)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				makeCursorReq := func(stream bool) []byte {
					return []byte(fmt.Sprintf(`{
						"model":"gpt-4o",
						"stream":%v,
						"stream_options":{"include_usage":true},
						"reasoning_effort":"high",
						"input":[
							{"type":"message","role":"user","content":"patch target.go"},
							{"type":"reasoning","id":"rs_cur_ag","summary":[{"type":"summary_text","text":"summary"}],"encrypted_content":%q},
							{"type":"custom_tool_call","call_id":"call_c_prev","name":"apply_patch","input":"*** Prior Patch"},
							{"type":"custom_tool_call_output","call_id":"call_c_prev","output":"applied"}
						],
						"tools":[{"type":"custom","name":"apply_patch","description":"apply unified patch"}]
					}`, stream, tc.carrier))
				}

				stS, ctS, respS := sendLabClientRequest(t, gwSrv, protocolLabClients[2], makeCursorReq(true))
				require.Equal(t, http.StatusOK, stS, string(respS))
				require.Contains(t, strings.ToLower(ctS), "text/event-stream")
				framesS, fErr := readAllSSEFrames(respS)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, framesS, testkit.GrammarOptions{}))
				require.Contains(t, string(respS), `"apply_patch"`)
				logsS := lab.UsageLogs()
				require.Empty(t, compareClientAndSettledUsage(protocol.ProtocolOpenAIChatCompletions, framesS, logsS))
				require.NotNil(t, logsS[0].ReasoningEffort)
				require.Equal(t, "high", *logsS[0].ReasoningEffort)

				lab.ResetUsageLogs()
				stB, ctB, respB := sendLabClientRequest(t, gwSrv, protocolLabClients[2], makeCursorReq(false))
				require.Equal(t, http.StatusOK, stB, string(respB))
				require.Contains(t, strings.ToLower(ctB), "application/json")
				require.Equal(t, "cursor-antigravity-buffered-ok", gjson.GetBytes(respB, "choices.0.message.content").String())
				require.Len(t, lab.UsageLogs(), 1)

				require.Empty(t, upSrv.Violations())
				reqs := upSrv.Requests()
				require.Len(t, reqs, 2)
				for _, r := range reqs {
					require.Equal(t, tc.wantSig, gjson.GetBytes(r.Body, "contents.1.parts.0.thoughtSignature").String())
					require.True(t, gjson.GetBytes(r.Body, "contents.1.parts.0.thought").Bool())
				}
			})
		}
	})

	t.Run("admin_identity_patch_prompt_live_updates", func(t *testing.T) {
		okSSE := "data: {\"responseId\":\"ag_id_live\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"identity-live-ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":3,\"totalTokenCount\":13}}\n\n"
		for _, provSpec := range agSpecs {
			t.Run(provSpec.kind, func(t *testing.T) {
				mockH := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "ag_identity_live", Default: testkit.Reply{Raw: okSSE}},
					Validation: provSpec.validation,
				})
				upSrv := newLabMockServer(provSpec, mockH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, provSpec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				reqBody := []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"stream":true,"system":"Keep answers concise.","messages":[{"role":"user","content":"ping"}]}`)

				// 1. Default unset -> built-in Antigravity identity prompt.
				st1, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[0], reqBody)
				require.Equal(t, http.StatusOK, st1)

				// 2. Live admin update v1 -> immediately reflected without restarting router.
				lab.SetSetting(gateway.SettingKeyIdentityPatchPrompt, "Custom admin identity v1.")
				st2, _, _ := sendLabClientRequest(t, gwSrv, antigravityDedicatedMessagesClient(), reqBody)
				require.Equal(t, http.StatusOK, st2)

				// 3. Live admin update v2 -> immediately reflected on OpenAI Responses entry too.
				lab.SetSetting(gateway.SettingKeyIdentityPatchPrompt, "Custom admin identity v2.")
				st3, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","stream":true,"instructions":"Keep answers concise.","input":"ping"}`))
				require.Equal(t, http.StatusOK, st3)

				// 4. Delete setting -> immediately reverts to built-in Antigravity identity prompt.
				lab.DeleteSetting(gateway.SettingKeyIdentityPatchPrompt)
				st4, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[2], []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"system","content":"Keep answers concise."},{"role":"user","content":"ping"}]}`))
				require.Equal(t, http.StatusOK, st4)

				require.Empty(t, upSrv.Violations())
				reqs := upSrv.Requests()
				require.Len(t, reqs, 4)
				require.Contains(t, gjson.GetBytes(reqs[0].Body, "systemInstruction.parts.0.text").String(), "You are Antigravity")
				require.Equal(t, "Keep answers concise.", gjson.GetBytes(reqs[0].Body, "systemInstruction.parts.1.text").String())
				require.Equal(t, "Custom admin identity v1.", gjson.GetBytes(reqs[1].Body, "systemInstruction.parts.0.text").String())
				require.Equal(t, "Custom admin identity v2.", gjson.GetBytes(reqs[2].Body, "systemInstruction.parts.0.text").String())
				require.Contains(t, gjson.GetBytes(reqs[3].Body, "systemInstruction.parts.0.text").String(), "You are Antigravity")
			})
		}
	})
}

// runAntigravityInlineImageAndTextSignatureSuite verifies end-to-end T5 gateway
// delivery of model-generated inlineData base64 images as Markdown data URLs
// across Messages, Responses, and Chat (streaming and buffered) plus Turn 2
// replay preservation of non-thought text thoughtSignature carriers.
func runAntigravityInlineImageAndTextSignatureSuite(t *testing.T, clients []labClientSpec, agSpecs []labProviderKindSpec) {
	t.Helper()
	const pngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
	wantMarkdown := "![image](data:image/png;base64," + pngB64 + ")"

	for _, provSpec := range agSpecs {
		t.Run(provSpec.kind, func(t *testing.T) {
			sigVal := "sig_t5_plain_txt_" + provSpec.kind
			turn1SSE := fmt.Sprintf(
				"data: {\"responseId\":\"ag_t5_img_1\",\"modelVersion\":%q,\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Generated diagram: \",\"thoughtSignature\":%q},{\"inlineData\":{\"mimeType\":\"image/png\",\"data\":%q}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":20,\"candidatesTokenCount\":16,\"totalTokenCount\":36}}\n\n",
				provSpec.upstreamModel, sigVal, pngB64,
			)
			turn2SSE := fmt.Sprintf(
				"data: {\"responseId\":\"ag_t5_img_2\",\"modelVersion\":%q,\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Turn two acknowledged.\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":40,\"candidatesTokenCount\":6,\"totalTokenCount\":46}}\n\n",
				provSpec.upstreamModel,
			)

			for _, cli := range clients {
				t.Run(cli.slug, func(t *testing.T) {
					mockH := mockupstream.NewWithConfig(mockupstream.Config{
						Mode: mockupstream.ModeReplay,
						Replay: []testkit.Reply{
							{Raw: turn1SSE},
							{Raw: turn2SSE},
							{Raw: turn1SSE},
						},
						Validation: provSpec.validation,
					})
					upSrv := newLabMockServer(provSpec, mockH)
					defer upSrv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, provSpec)},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					// 1. Turn 1 streaming request -> Markdown image + trc1 carrier on Messages/Responses.
					var req1 []byte
					switch cli.protocol {
					case protocol.ProtocolAnthropicMessages:
						req1 = []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":true,"messages":[{"role":"user","content":"Draw diagram."}]}`)
					case protocol.ProtocolOpenAIResponses:
						req1 = []byte(`{"model":"gpt-5","stream":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Draw diagram."}]}]}`)
					default:
						req1 = []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"Draw diagram."}]}`)
					}
					st1, _, raw1 := sendLabClientRequest(t, gwSrv, cli, req1)
					require.Equal(t, http.StatusOK, st1, string(raw1))
					require.Contains(t, string(raw1), wantMarkdown)

					frames1, fErr := readAllSSEFrames(raw1)
					require.NoError(t, fErr)
					require.Empty(t, testkit.ValidateStreamGrammar(cli.protocol, frames1, testkit.GrammarOptions{}))

					// 2. Turn 2 replay -> verify signature round-trips on Messages and Responses.
					req2, simErr := testkit.SimulateNextTurn(cli.protocol, testkit.TurnInput{
						PrevRequest:      req1,
						ResponseFrames:   frames1,
						FollowUpUserText: "Refine label.",
					})
					require.NoError(t, simErr)
					st2, _, raw2 := sendLabClientRequest(t, gwSrv, cli, req2)
					require.Equal(t, http.StatusOK, st2, string(raw2))
					require.Contains(t, string(raw2), "Turn two acknowledged.")

					// 3. Buffered mode on generic / dedicated endpoint -> Markdown image preserved.
					reqBuf := bytes.Replace(req1, []byte(`"stream":true`), []byte(`"stream":false`), 1)
					st3, ct3, raw3 := sendLabClientRequest(t, gwSrv, cli, reqBuf)
					require.Equal(t, http.StatusOK, st3, string(raw3))
					require.Contains(t, strings.ToLower(ct3), "application/json")
					require.Contains(t, string(raw3), wantMarkdown)

					require.Empty(t, upSrv.Violations())
					reqs := upSrv.Requests()
					require.Len(t, reqs, 3)
					if cli.protocol != protocol.ProtocolOpenAIChatCompletions {
						require.Contains(t, string(reqs[1].Body), `"thoughtSignature":"`+sigVal+`"`)
					}
				})
			}
		})
	}
}

// runAntigravityMixedGroupContinuitySuite verifies Section 3.10 continuity
// boundaries in mixed groups: trc1:antigravity_claude and native or converted
// Anthropic trc1:anthropic never cross-contaminate, safely stripping on regular
// Messages/Responses or preserving plain text on passback Messages. Meanwhile,
// trc1:gemini reuses signatures across Antigravity gemini-* and standard Gemini
// upstreams and strips cleanly when falling onto native Messages or Responses.
func runAntigravityMixedGroupContinuitySuite(t *testing.T, corpus *testkit.Corpus, agSpecs []labProviderKindSpec) {
	t.Helper()
	gemRsnStreamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolGeminiGenerateContent,
		Kind:     testkit.FixtureStream,
		Scenario: "reasoning_tool_replay",
	})
	require.NoError(t, err)
	msgRsnStreamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolAnthropicMessages,
		Kind:     testkit.FixtureStream,
		Scenario: "reasoning_tool_replay",
	})
	require.NoError(t, err)
	msgTextMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolAnthropicMessages,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)
	respTextMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolOpenAIResponses,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)
	gemTextMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolGeminiGenerateContent,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)

	t.Run("antigravity_claude_and_native_anthropic_strict_mutual_isolation", func(t *testing.T) {
		msgTurn1Matches, qErr := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolAnthropicMessages,
			Kind:     testkit.FixtureRequest,
			Scenario: "reasoning_tool_replay",
		})
		require.NoError(t, qErr)

		// Direction A: Turn 1 on Antigravity claude-* (emits trc1:antigravity_claude) -> Turn 2 on Native Messages variants.
		for _, tc := range []struct {
			name         string // subtest target label
			platform     string // Turn 2 native provider platform
			providerType string // Turn 2 native provider auth type
			model        string // Turn 2 native upstream model
			passback     bool   // whether Turn 2 uses passback dialect
			bedrock      bool   // whether Turn 2 serves binary AWS EventStream frames
		}{
			{name: "anthropic_apikey", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeAPIKey, model: "claude-sonnet-4-5-20250929"},
			{name: "aws_bedrock", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeBedrock, model: "claude-sonnet-4-6", bedrock: true},
			{name: "vertex_service_account", platform: capability.PlatformAnthropic, providerType: capability.ProviderTypeServiceAccount, model: "claude-sonnet-4-6"},
			{name: "opencode_claude", platform: capability.PlatformOpenCode, providerType: capability.ProviderTypeAPIKey, model: "claude-sonnet-4-6"},
			{name: "kimi_passback", platform: capability.PlatformKimi, providerType: capability.ProviderTypeAPIKey, model: "k3", passback: true},
			{name: "deepseek_passback", platform: capability.PlatformDeepseek, providerType: capability.ProviderTypeAPIKey, model: "deepseek-reasoner", passback: true},
		} {
			t.Run("ag_claude_to_"+tc.name, func(t *testing.T) {
				t1Mock := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "t1_ag_claude", Default: testkit.Reply{Raw: gemRsnStreamMatches[0].Path}},
					Validation: agSpecs[0].validation,
				})
				t1Srv := newLabMockServer(agSpecs[0], t1Mock)
				defer t1Srv.Close()

				var mu sync.Mutex
				var t2Bodies [][]byte
				var t2Srv *httptest.Server
				var t2MockH *mockupstream.Handler

				if tc.bedrock {
					bedrockStream := buildLabBedrockChunkStream(
						`{"type":"message_start","message":{"id":"msg_bdrk_ag_t2","type":"message","role":"assistant","model":"us.anthropic.claude-sonnet-4-6","content":[],"usage":{"input_tokens":0,"output_tokens":0}}}`,
						`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
						`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"bedrock-after-ag-claude-ok"}}`,
						`{"type":"content_block_stop","index":0}`,
						`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"amazon-bedrock-invocationMetrics":{"inputTokenCount":25,"outputTokenCount":5}}`,
						`{"type":"message_stop"}`,
					)
					t2Srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						raw, rErr := io.ReadAll(r.Body)
						require.NoError(t, rErr)
						mu.Lock()
						t2Bodies = append(t2Bodies, raw)
						mu.Unlock()
						w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write(bedrockStream)
					}))
				} else if tc.providerType == capability.ProviderTypeServiceAccount {
					t2Srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						raw, rErr := io.ReadAll(r.Body)
						require.NoError(t, rErr)
						mu.Lock()
						t2Bodies = append(t2Bodies, raw)
						mu.Unlock()
						w.Header().Set("Content-Type", "text/event-stream")
						w.WriteHeader(http.StatusOK)
						_, _ = w.Write(msgTextMatches[0].Data)
					}))
				} else {
					valOpts := testkit.RequestValidationOptions{StrictTopLevel: true}
					if tc.passback {
						valOpts.DialectRules = []string{"passback"}
					}
					t2MockH = mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "t2_native_msg", Default: testkit.Reply{Raw: msgTextMatches[0].Path}},
						Validation: valOpts,
					})
					t2Srv = httptest.NewServer(t2MockH)
				}
				defer t2Srv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, t1Srv.URL, agSpecs[0])},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				t1Body := prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, msgTurn1Matches[0].Data)
				st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], t1Body)
				require.Equal(t, http.StatusOK, st1)
				require.Empty(t, t1Srv.Violations())
				require.Contains(t, string(resp1), "trc1:antigravity_claude:")

				frames1, fErr := readAllSSEFrames(resp1)
				require.NoError(t, fErr)
				t2Req, sErr := testkit.SimulateNextTurn(protocol.ProtocolAnthropicMessages, testkit.TurnInput{
					PrevRequest:              t1Body,
					ResponseFrames:           frames1,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Continue on native Anthropic Messages.",
				})
				require.NoError(t, sErr)

				nativeProv := labProvider{
					ID:               202,
					Name:             tc.name,
					Platform:         tc.platform,
					Type:             tc.providerType,
					BaseURL:          t2Srv.URL,
					Models:           []string{tc.model, "us.anthropic." + tc.model},
					ModelMapping:     map[string]string{"claude-sonnet-4-5": tc.model, "claude-sonnet-4-5-20250929": tc.model},
					EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
				}
				lab.SetProviders(t, []labProvider{nativeProv})

				st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[0], prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, t2Req))
				require.Equal(t, http.StatusOK, st2, string(resp2))
				frames2, fErr := readAllSSEFrames(resp2)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolAnthropicMessages, frames2, testkit.GrammarOptions{}))
				require.Len(t, lab.UsageLogs(), 2)

				var wireBody []byte
				if t2MockH != nil {
					require.Empty(t, t2MockH.Violations())
					require.Len(t, t2MockH.Requests(), 1)
					wireBody = t2MockH.Requests()[0].Body
				} else {
					mu.Lock()
					require.Len(t, t2Bodies, 1)
					wireBody = t2Bodies[0]
					mu.Unlock()
				}
				require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolAnthropicMessages, wireBody))
				require.False(t, hasLeakedForeignReplayToken(findLabProviderKind("anthropic"), wireBody))
				// Verify trc1:antigravity_claude was NOT unwrapped as a bare Anthropic signature,
				// while passback models retained the embedded thinking text with an empty signature.
				require.NotContains(t, string(wireBody), "CpEBCocB_gem_inv_thought_turn2")
				if tc.passback {
					require.Contains(t, string(wireBody), `"type":"thinking"`)
					require.Contains(t, string(wireBody), "now query SKU-99 stock.")
				}
			})
		}

		// Direction B: Responses Turn 1 on converted Anthropic (emits trc1:anthropic) -> Turn 2 on Antigravity claude-*.
		t.Run("converted_anthropic_carrier_not_reused_on_antigravity_claude", func(t *testing.T) {
			respTurn1Matches, qErr := corpus.QueryFixtures(testkit.FixtureQuery{
				Source:   testkit.SourceWire,
				Protocol: protocol.ProtocolOpenAIResponses,
				Kind:     testkit.FixtureRequest,
				Scenario: "reasoning_tool_replay",
			})
			require.NoError(t, qErr)

			anthSpec := findLabProviderKind("anthropic")
			t1H := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "t1_conv_anth", Default: testkit.Reply{Raw: msgRsnStreamMatches[0].Path}},
				Validation: anthSpec.validation,
			})
			t1Srv := httptest.NewServer(t1H)
			defer t1Srv.Close()

			t2Mock := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "t2_ag_claude", Default: testkit.Reply{Raw: gemTextMatches[0].Path}},
				Validation: agSpecs[0].validation,
			})
			t2Srv := newLabMockServer(agSpecs[0], t2Mock)
			defer t2Srv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, t1Srv.URL, anthSpec)},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			t1Body := prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, respTurn1Matches[0].Data)
			st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Body)
			require.Equal(t, http.StatusOK, st1)
			require.Contains(t, string(resp1), "trc1:anthropic:")

			frames1, fErr := readAllSSEFrames(resp1)
			require.NoError(t, fErr)
			t2Raw, sErr := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
				PrevRequest:              t1Body,
				ResponseFrames:           frames1,
				SynthesizeMissingOutputs: true,
				FollowUpUserText:         "Switch to Antigravity Claude.",
			})
			require.NoError(t, sErr)

			lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, t2Srv.URL, agSpecs[0])})
			st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Raw))
			require.Equal(t, http.StatusOK, st2, string(resp2))
			require.Empty(t, t2Srv.Violations())
			sent := t2Srv.Requests()[0].Body
			require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolGeminiGenerateContent, sent))
			require.False(t, hasLeakedForeignReplayToken(agSpecs[0], sent))
			require.NotContains(t, string(sent), "EqQBCgIYAiJAc2t1Xzk5X3RoaW5raW5nX3NpZ190dXJuXzAy")
			// Unsigned historical thinking on claude-* clears thinkingConfig on the outbound Gemini request.
			require.False(t, gjson.GetBytes(sent, "generationConfig.thinkingConfig").Exists())
		})
	})

	t.Run("trc1_gemini_reused_between_antigravity_gemini_and_standard_gemini_and_stripped_on_native", func(t *testing.T) {
		respTurn1Matches, qErr := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolOpenAIResponses,
			Kind:     testkit.FixtureRequest,
			Scenario: "reasoning_tool_replay",
		})
		require.NoError(t, qErr)
		stdGemSpec := findLabProviderKind("gemini")

		// 1. Bidirectional trc1:gemini reuse: Antigravity gemini-* <-> Standard Google Gemini.
		for _, dir := range []struct {
			name   string              // Gemini carrier reuse direction label
			first  labProviderKindSpec // Turn 1 Gemini-scheme provider
			second labProviderKindSpec // Turn 2 Gemini-scheme provider
		}{
			{name: "antigravity_gemini_to_standard_gemini", first: agSpecs[1], second: stdGemSpec},
			{name: "standard_gemini_to_antigravity_gemini", first: stdGemSpec, second: agSpecs[1]},
		} {
			t.Run(dir.name, func(t *testing.T) {
				t1Mock := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "t1_" + dir.name, Default: testkit.Reply{Raw: gemRsnStreamMatches[0].Path}},
					Validation: dir.first.validation,
				})
				t2Mock := mockupstream.NewWithConfig(mockupstream.Config{
					Script:     testkit.Script{Name: "t2_" + dir.name, Default: testkit.Reply{Raw: gemTextMatches[0].Path}},
					Validation: dir.second.validation,
				})
				t1Srv := newLabMockServer(dir.first, t1Mock)
				defer t1Srv.Close()
				t2Srv := newLabMockServer(dir.second, t2Mock)
				defer t2Srv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(101, 0, t1Srv.URL, dir.first)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				t1Body := prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, respTurn1Matches[0].Data)
				st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Body)
				require.Equal(t, http.StatusOK, st1)
				require.Contains(t, string(resp1), "trc1:gemini:")

				frames1, fErr := readAllSSEFrames(resp1)
				require.NoError(t, fErr)
				t2Raw, sErr := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
					PrevRequest:              t1Body,
					ResponseFrames:           frames1,
					SynthesizeMissingOutputs: true,
					FollowUpUserText:         "Reuse trc1:gemini across Gemini endpoints.",
				})
				require.NoError(t, sErr)

				lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, t2Srv.URL, dir.second)})
				st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Raw))
				require.Equal(t, http.StatusOK, st2, string(resp2))
				require.Empty(t, t2Srv.Violations())

				// Same ReplayScheme ("gemini") unwraps Turn 1's exact CpEBCocB_gem_inv_thought_turn2 onto the thought part.
				sent2 := t2Srv.Requests()[0].Body
				require.Contains(t, string(sent2), `"CpEBCocB_gem_inv_thought_turn2"`)
				require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolGeminiGenerateContent, sent2))
				require.Len(t, lab.UsageLogs(), 2)
			})
		}

		// 2. Antigravity claude-* does NOT reuse trc1:gemini from Antigravity gemini-*.
		t.Run("antigravity_gemini_carrier_dropped_on_antigravity_claude", func(t *testing.T) {
			t1Mock := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "t1_ag_gem", Default: testkit.Reply{Raw: gemRsnStreamMatches[0].Path}},
				Validation: agSpecs[1].validation,
			})
			t2Mock := mockupstream.NewWithConfig(mockupstream.Config{
				Script:     testkit.Script{Name: "t2_ag_cl", Default: testkit.Reply{Raw: gemTextMatches[0].Path}},
				Validation: agSpecs[0].validation,
			})
			t1Srv := newLabMockServer(agSpecs[1], t1Mock)
			defer t1Srv.Close()
			t2Srv := newLabMockServer(agSpecs[0], t2Mock)
			defer t2Srv.Close()

			lab := newProtocolLabRouter(t, labOptions{
				Providers: []labProvider{makeLabProviderSpec(101, 0, t1Srv.URL, agSpecs[1])},
			})
			gwSrv := httptest.NewServer(lab)
			defer gwSrv.Close()

			t1Body := prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, respTurn1Matches[0].Data)
			st1, _, resp1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], t1Body)
			require.Equal(t, http.StatusOK, st1)
			require.Contains(t, string(resp1), "trc1:gemini:")
			frames1, fErr := readAllSSEFrames(resp1)
			require.NoError(t, fErr)
			t2Raw, err := testkit.SimulateNextTurn(protocol.ProtocolOpenAIResponses, testkit.TurnInput{
				PrevRequest:              t1Body,
				ResponseFrames:           frames1,
				SynthesizeMissingOutputs: true,
				FollowUpUserText:         "Switch from Antigravity Gemini to Antigravity Claude.",
			})
			require.NoError(t, err)

			lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, t2Srv.URL, agSpecs[0])})
			st2, _, resp2 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], prepareLabStreamRequestBody(protocol.ProtocolOpenAIResponses, t2Raw))
			require.Equal(t, http.StatusOK, st2, string(resp2))
			require.Empty(t, t2Srv.Violations())
			sent2 := t2Srv.Requests()[0].Body
			require.NotContains(t, string(sent2), "CpEBCocB_gem_inv_thought_turn2")
		})

		// 3. Antigravity gemini-* and claude-* carriers stripped on Native Responses HTTP & WebSocket.
		t.Run("antigravity_carriers_stripped_on_native_responses_http_and_ws", func(t *testing.T) {
			for _, scheme := range []struct {
				name    string     // continuity carrier scheme label
				carrier ir.Carrier // synthetic continuity carrier payload
			}{
				{name: "antigravity_claude", carrier: ir.Carrier{Scheme: ir.SchemeAntigravityClaude, Data: "sig_cl_ws", Text: "claude thought"}},
				{name: "antigravity_gemini", carrier: ir.Carrier{Scheme: ir.SchemeGemini, Data: "CpEBCgIIAxgB"}},
			} {
				t.Run(scheme.name, func(t *testing.T) {
					encoded, cErr := ir.FormatCarrier(scheme.carrier)
					require.NoError(t, cErr)

					// HTTP native OpenAI Responses check.
					openAISpec := findLabProviderKind("openai")
					httpH := mockupstream.NewWithConfig(mockupstream.Config{
						Script:     testkit.Script{Name: "native_resp_http", Default: testkit.Reply{Raw: respTextMatches[0].Path}},
						Validation: openAISpec.validation,
					})
					httpSrv := httptest.NewServer(httpH)
					defer httpSrv.Close()

					var mu sync.Mutex
					var wsFrames [][]byte
					wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						conn, acceptErr := websocket.Accept(w, r, nil)
						if acceptErr != nil {
							return
						}
						defer func() { _ = conn.CloseNow() }()
						_, data, readErr := conn.Read(r.Context())
						if readErr != nil {
							return
						}
						mu.Lock()
						wsFrames = append(wsFrames, append([]byte(nil), data...))
						mu.Unlock()
						// Ignore write and teardown errors on the local mock WebSocket once the
						// captured request frame is recorded and terminal completion frames flush.
						_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_ws_ag","object":"response","status":"in_progress","model":"gpt-5.4","output":[]}}`))
						_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.completed","sequence_number":1,"response":{"id":"resp_ws_ag","object":"response","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":8,"output_tokens":2,"total_tokens":10}}}`))
					}))
					defer wsSrv.Close()

					lab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{makeLabProviderSpec(301, 0, httpSrv.URL, openAISpec)},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					httpPayload := []byte(fmt.Sprintf(`{"model":"gpt-5","stream":true,"store":false,"input":[{"type":"message","role":"user","content":"http after ag"},{"type":"reasoning","id":"rs_ag_syn","summary":[{"type":"summary_text","text":"ag summary"}],"encrypted_content":%q},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`, encoded))
					stH, _, respH := sendLabClientRequest(t, gwSrv, protocolLabClients[1], httpPayload)
					require.Equal(t, http.StatusOK, stH, string(respH))
					require.Empty(t, httpH.Violations())
					httpSent := httpH.Requests()[0].Body
					require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, httpSent))
					require.False(t, gjson.GetBytes(httpSent, "input.1.encrypted_content").Exists())
					require.False(t, gjson.GetBytes(httpSent, "input.1.id").Exists())
					require.Equal(t, "ag summary", gjson.GetBytes(httpSent, "input.1.summary.0.text").String())

					// WebSocket native OpenAI Responses check.
					wsLab := newProtocolLabRouter(t, labOptions{
						Providers: []labProvider{{
							ID:               401,
							Platform:         capability.PlatformOpenAI,
							Type:             capability.ProviderTypeAPIKey,
							BaseURL:          wsSrv.URL,
							Models:           []string{"gpt-5.4"},
							ModelMapping:     map[string]string{"gpt-5": "gpt-5.4"},
							EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
						}},
						AllowedProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
					})
					wsGW := httptest.NewServer(wsLab)
					defer wsGW.Close()

					wsURL := "ws" + strings.TrimPrefix(wsGW.URL, "http") + "/v1/responses"
					dialCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					defer cancel()
					clientConn, _, dialErr := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
						HTTPHeader: http.Header{"Authorization": []string{"Bearer lab-api-key"}},
					})
					require.NoError(t, dialErr)
					defer func() { _ = clientConn.CloseNow() }()

					wsTurn := []byte(fmt.Sprintf(`{"type":"response.create","model":"gpt-5","store":false,"input":[{"type":"message","role":"user","content":"ws after ag"},{"type":"reasoning","id":"rs_ag_ws","summary":[{"type":"summary_text","text":"ag ws summary"}],"encrypted_content":%q},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`, encoded))
					require.NoError(t, clientConn.Write(dialCtx, websocket.MessageText, wsTurn))
					for {
						_, msg, rErr := clientConn.Read(dialCtx)
						require.NoError(t, rErr)
						if gjson.GetBytes(msg, "type").String() == "response.completed" {
							break
						}
					}
					// Ignore best-effort normal WebSocket closure errors on clientConn after
					// the terminal response.completed frame has already been verified on the stream.
					_ = clientConn.Close(websocket.StatusNormalClosure, "done")

					mu.Lock()
					gotWS := slices.Clone(wsFrames)
					mu.Unlock()
					require.Len(t, gotWS, 1)
					require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, gotWS[0]))
					require.False(t, gjson.GetBytes(gotWS[0], "input.1.encrypted_content").Exists())
					require.False(t, gjson.GetBytes(gotWS[0], "input.1.id").Exists())
				})
			}
		})
	})
}

// TestProtocolLab_ProtocolConversionDiagnostics exercises end-to-end gateway
// collection and isolation of bounded ProtocolConversion metadata across pure
// native passthrough, cross-protocol IR conversion, merged tool_adapted +
// native_replay Responses passthrough, lossless native carrier unpacking, and
// converted-to-native provider failover.
func TestProtocolLab_ProtocolConversionDiagnostics(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	anthStreamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolAnthropicMessages,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)
	require.NotEmpty(t, anthStreamMatches)

	respStreamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolOpenAIResponses,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)
	require.NotEmpty(t, respStreamMatches)

	gemStreamMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: protocol.ProtocolGeminiGenerateContent,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)
	require.NotEmpty(t, gemStreamMatches)

	anthSpec := findLabProviderKind("anthropic")
	openAISpec := findLabProviderKind("openai")
	geminiSpec := findLabProviderKind("gemini")

	anthMock := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{Name: "diag_anth", Default: testkit.Reply{Raw: anthStreamMatches[0].Path}},
	})
	anthSrv := httptest.NewServer(anthMock)
	defer anthSrv.Close()

	openAIMock := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{Name: "diag_openai", Default: testkit.Reply{Raw: respStreamMatches[0].Path}},
	})
	openAISrv := httptest.NewServer(openAIMock)
	defer openAISrv.Close()

	gemMock := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{Name: "diag_gemini", Default: testkit.Reply{Raw: gemStreamMatches[0].Path}},
	})
	gemSrv := httptest.NewServer(gemMock)
	defer gemSrv.Close()

	var recMu sync.Mutex
	var latestRecord telemetry.RequestRecord
	onTelemetry := func(r telemetry.RequestRecord) {
		recMu.Lock()
		latestRecord = r
		recMu.Unlock()
	}
	readLatestRecord := func() telemetry.RequestRecord {
		recMu.Lock()
		defer recMu.Unlock()
		return latestRecord
	}

	t.Run("pure_native_passthrough_and_literal_user_text_stay_nil", func(t *testing.T) {
		lab := newProtocolLabRouter(t, labOptions{
			Providers:   []labProvider{makeLabProviderSpec(11, 0, openAISrv.URL, openAISpec)},
			OnTelemetry: onTelemetry,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		body := []byte(`{"model":"gpt-5","stream":true,"input":[{"type":"message","role":"user","content":"literal trc1:gemini:eyJkIjoiZmFrZSJ9 in user text"}]}`)
		st, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], body)
		require.Equal(t, http.StatusOK, st, string(respBytes))

		logs := lab.UsageLogs()
		require.Len(t, logs, 1)
		require.Nil(t, logs[0].ProtocolConversion)
		rec := readLatestRecord()
		require.NotEmpty(t, rec.Attempts)
		require.Nil(t, rec.Attempts[0].ProtocolConversion)
	})

	t.Run("ir_converted_records_route_and_loss_counts", func(t *testing.T) {
		prov := makeLabProviderSpec(12, 0, gemSrv.URL, geminiSpec)
		prov.Models = []string{"claude-sonnet-4-5", "gemini-2.5-pro"}
		prov.ModelMapping = map[string]string{"claude-sonnet-4-5": "gemini-2.5-pro"}
		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{prov},
			ProtocolFallbacks: map[protocol.ProtocolID][]protocol.ProtocolID{
				protocol.ProtocolAnthropicMessages: {protocol.ProtocolGeminiGenerateContent},
			},
			OnTelemetry: onTelemetry,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"stream":true,"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]}`)
		st, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[0], body)
		require.Equal(t, http.StatusOK, st, string(respBytes))

		logs := lab.UsageLogs()
		require.Len(t, logs, 1)
		require.NotNil(t, logs[0].ProtocolConversion)
		require.Equal(t, telemetry.ConversionModeIRConverted, logs[0].ProtocolConversion.Mode)
		require.Equal(t, string(protocol.ProtocolAnthropicMessages), logs[0].ProtocolConversion.ClientProtocol)
		require.Equal(t, string(protocol.ProtocolGeminiGenerateContent), logs[0].ProtocolConversion.UpstreamProtocol)
		require.GreaterOrEqual(t, logs[0].ProtocolConversion.LossCounts[string(ir.LossCacheMarkDropped)], 1)

		rec := readLatestRecord()
		require.Len(t, rec.Attempts, 1)
		require.Equal(t, logs[0].ProtocolConversion, rec.Attempts[0].ProtocolConversion)
	})

	t.Run("tool_adapted_and_native_replay_merge_on_same_attempt", func(t *testing.T) {
		foreignCarrier, cErr := ir.FormatCarrier(ir.Carrier{Scheme: ir.SchemeGemini, Data: "CpEB_gem"})
		require.NoError(t, cErr)

		prov := makeLabProviderSpec(13, 0, "https://api.openai.com/v1", openAISpec)
		prov.Extra = map[string]any{"openai_passthrough": true}
		lab := newProtocolLabRouter(t, labOptions{
			Providers:     []labProvider{prov},
			HostOverrides: map[string]string{"api.openai.com": openAISrv.URL},
			OnTelemetry:   onTelemetry,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		body := []byte(fmt.Sprintf(`{"model":"gpt-5","stream":true,"store":false,"tools":[{"type":"custom","name":"apply_patch","format":{"type":"text"}}],"input":[{"type":"message","role":"user","content":"hi"},{"type":"reasoning","id":"rs_foreign","summary":[{"type":"summary_text","text":"thought"}],"encrypted_content":%q},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`, foreignCarrier))
		st, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[1], body)
		require.Equal(t, http.StatusOK, st, string(respBytes))

		logs := lab.UsageLogs()
		require.Len(t, logs, 1)
		conv := logs[0].ProtocolConversion
		require.NotNil(t, conv)
		require.Equal(t, telemetry.ConversionModeToolAdapted, conv.Mode)
		require.Equal(t, string(protocol.ProtocolOpenAIResponses), conv.ClientProtocol)
		require.Equal(t, string(protocol.ProtocolOpenAIResponses), conv.UpstreamProtocol)
		require.Equal(t, ir.SchemeOpenAI, conv.ReplayScheme)
		require.Equal(t, 1, conv.LossCounts[string(ir.LossReplayTokenDropped)])
		require.Equal(t, 1, conv.LossCounts[string(ir.LossToolLowered)])

		rec := readLatestRecord()
		require.Len(t, rec.Attempts, 1)
		require.Equal(t, conv, rec.Attempts[0].ProtocolConversion)
	})

	t.Run("lossless_native_replay_unpack_records_native_replay_without_losses", func(t *testing.T) {
		matchingCarrier, cErr := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "sig_anth_valid",
			Text:   "verified thought",
		})
		require.NoError(t, cErr)

		lab := newProtocolLabRouter(t, labOptions{
			Providers:   []labProvider{makeLabProviderSpec(14, 0, anthSrv.URL, anthSpec)},
			OnTelemetry: onTelemetry,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		body := []byte(fmt.Sprintf(`{"model":"claude-sonnet-4-5","max_tokens":128,"stream":true,"messages":[{"role":"user","content":"q1"},{"role":"assistant","content":[{"type":"thinking","thinking":"verified thought","signature":%q},{"type":"text","text":"a1"}]},{"role":"user","content":"q2"}]}`, matchingCarrier))
		st, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[0], body)
		require.Equal(t, http.StatusOK, st, string(respBytes))

		logs := lab.UsageLogs()
		require.Len(t, logs, 1)
		conv := logs[0].ProtocolConversion
		require.NotNil(t, conv)
		require.Equal(t, telemetry.ConversionModeNativeReplay, conv.Mode)
		require.Equal(t, string(protocol.ProtocolAnthropicMessages), conv.ClientProtocol)
		require.Equal(t, string(protocol.ProtocolAnthropicMessages), conv.UpstreamProtocol)
		require.Empty(t, conv.LossCounts)

		rec := readLatestRecord()
		require.Len(t, rec.Attempts, 1)
		require.Equal(t, conv, rec.Attempts[0].ProtocolConversion)
	})

	t.Run("failover_from_converted_failure_to_native_passthrough_isolates_attempts", func(t *testing.T) {
		failGemSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error":{"code":401,"message":"invalid api key","status":"UNAUTHENTICATED"}}`))
		}))
		defer failGemSrv.Close()

		failGemProv := makeLabProviderSpec(15, 1, failGemSrv.URL, geminiSpec)
		failGemProv.Models = []string{"claude-sonnet-4-5", "gemini-2.5-pro"}
		failGemProv.ModelMapping = map[string]string{"claude-sonnet-4-5": "gemini-2.5-pro"}

		nativeAnthProv := makeLabProviderSpec(16, 2, anthSrv.URL, anthSpec)

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{failGemProv, nativeAnthProv},
			ProtocolFallbacks: map[protocol.ProtocolID][]protocol.ProtocolID{
				protocol.ProtocolAnthropicMessages: {protocol.ProtocolGeminiGenerateContent},
			},
			MaxProviderSwitches: 2,
			OnTelemetry:         onTelemetry,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"stream":true,"messages":[{"role":"user","content":[{"type":"text","text":"failover test","cache_control":{"type":"ephemeral"}}]}]}`)
		st, _, respBytes := sendLabClientRequest(t, gwSrv, protocolLabClients[0], body)
		require.Equal(t, http.StatusOK, st, string(respBytes))

		logs := lab.UsageLogs()
		require.Len(t, logs, 1)
		require.Equal(t, int64(16), logs[0].ProviderID)
		require.Nil(t, logs[0].ProtocolConversion, "winning native passthrough usage row must stay nil")

		rec := readLatestRecord()
		require.Len(t, rec.Attempts, 2)
		require.Equal(t, int64(15), rec.Attempts[0].ProviderID)
		require.NotNil(t, rec.Attempts[0].ProtocolConversion, "failed converted attempt must retain ir_converted diagnostics")
		require.Equal(t, telemetry.ConversionModeIRConverted, rec.Attempts[0].ProtocolConversion.Mode)
		require.Equal(t, int64(16), rec.Attempts[1].ProviderID)
		require.Nil(t, rec.Attempts[1].ProtocolConversion, "winning native attempt must stay nil")
	})
}
