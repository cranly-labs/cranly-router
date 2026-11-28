package app

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/billing"
	gatewaytestkit "github.com/TokenFlux/TokenRouter/internal/gateway/testkit"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit/mockupstream"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/usage"
)

// protocol_lab_stage6_scenario_test.go exercises Qoder COSY upstream request
// conversion, session planning, capability gating, response reading, failover,
// and multi-turn carrier continuity across http_routes.go,
// gateway_middleware.go, gateway_selection.go, gateway_openai_text_http.go,
// gateway_openai_text_execution.go, gateway_openai_attempts.go,
// gateway_response_output.go, gateway_completion.go, gateway_messages_http.go,
// gateway_messages_execution.go, gateway_openai_ws_http.go,
// gateway_ws_connections.go, qoder_runtime.go, and provider_qoder.go. Three
// public text entry points (Anthropic Messages, OpenAI Responses, and OpenAI
// Chat Completions, including input-shaped Cursor Chat turns) share one
// convert.Exchange request pipeline and one streaming or buffered Qoder COSY
// response reader.
//
// Subtests verify canonical wire scenarios, XML/JSON/DSML text-encoded tool
// calls, Qoder alias and lowered client-tool restoration, stateless 400 and
// mandatory-thinking capability gates, volatile cch billing-header session
// fingerprinting, pre-semantic SSE failover, mid-stream partial settlement, and
// two-turn trc1:plain carrier replay across Qoder and native providers.

const (
	// Stage 6 T5 law identifiers classify Qoder buffered response folding,
	// text-encoded and lowered tool restoration, capability/stateless gates,
	// and conversation session reuse across Qoder scenario cells.
	lawT5QoderBuffer  = "T5_QODER_BUFFER"  // buffered JSON response folding and usage parity law
	lawT5QoderTools   = "T5_QODER_TOOLS"   // XML/JSON/DSML text tools, alias reversal, and lowered tool law
	lawT5QoderGate    = "T5_QODER_GATE"    // stateless 400, tool capability, thinking, and CommittedSSE gate law
	lawT5QoderSession = "T5_QODER_SESSION" // multi-turn session reuse and cch=<cch>; fingerprint law

	// Stage 6 synthetic scenario slugs identify Qoder buffered, tool-encoding,
	// capability-gate, and session-reuse cells outside the 33 canonical scenarios.
	scenarioStage6BufferedCache     = "buffered_text_and_usage_cache"     // non-streaming text and cached usage projection
	scenarioStage6BufferedReasoning = "buffered_reasoning_and_max_tokens" // non-streaming trc1:plain carrier and length truncation
	scenarioStage6ToolXMLText       = "tool_xml_text"                     // split <tool_call> XML text-encoded tool invocation
	scenarioStage6ToolJSONText      = "tool_json_text"                    // split <tool_call> embedded JSON tool invocation
	scenarioStage6ToolDSMLText      = "tool_dsml_text"                    // split DSML text-encoded tool invocation
	scenarioStage6ToolAliasClient   = "tool_alias_and_client_restoration" // QoderToolNameMapper case alias plus client tool raising
	scenarioStage6AnthropicEnvelope = "anthropic_envelope"                // Anthropic-shaped Qoder SSE event envelope with sparse delta usage
	scenarioStage6StatelessGate     = "stateless_gate_and_controls"       // stateless Responses/Cursor 400 gates and stateful Chat/Messages controls
	scenarioStage6CapabilityGate    = "tool_capability_and_thinking_gate" // attrs.ToolCall=false, mandatory thinking 400, and CommittedSSE error
	scenarioStage6SessionReuseCCH   = "session_reuse_and_cch_fingerprint" // two-turn session_id reuse across volatile cch checksums

	// Stage 6 violation rule codes report Qoder desktop payload defects,
	// un-restored tools, missing trc1:plain carriers, truncation mismatches,
	// capability gate leaks, or health-penalty side effects.
	ruleQoderDesktopWireInvalid      = "qoder_desktop_wire_invalid"      // outbound Qoder desktop JSON payload violated contract
	ruleQoderBufferedJSONInvalid     = "qoder_buffered_json_invalid"     // buffered response status, JSON, or usage disagreed
	ruleQoderPlainCarrierMissing     = "qoder_plain_carrier_missing"     // reasoning response omitted valid trc1:plain carrier or Chat reasoning_content
	ruleQoderFinishLengthMismatch    = "qoder_finish_length_mismatch"    // finish_reason=length did not map to client truncation reason
	ruleQoderTextToolNotDecoded      = "qoder_text_tool_not_decoded"     // XML, JSON, or DSML text tool call failed to decode or leaked markup
	ruleQoderToolNotRestored         = "qoder_tool_not_restored"         // Qoder alias or lowered client tool identity was not restored
	ruleQoderStatelessGateBypassed   = "qoder_stateless_gate_bypassed"   // stateful Responses or Cursor input Chat was not locally rejected with 400
	ruleQoderCursorInputRejected     = "qoder_cursor_input_rejected"     // valid stateless Cursor input-shaped Chat request was rejected
	ruleQoderGateContactedUpstream   = "qoder_gate_contacted_upstream"   // local 400 rejection dispatched upstream or contacted fallback provider
	ruleQoderGatePenalizedHealth     = "qoder_gate_penalized_health"     // local 400 rejection penalized provider status, cooldown, or EWMA health
	ruleQoderCommittedSSEMissingErr  = "qoder_committed_sse_missing_err" // pre-committed keepalive SSE stream lacked terminal SSE error frame
	ruleQoderSessionFingerprintDrift = "qoder_session_fingerprint_drift" // volatile cch checksum broke two-turn Qoder session_id reuse
)

// rawUnnormalizedCCHPattern matches un-normalized 5-hex-digit cch checksums
// inside x-anthropic-billing-header system lines so outbound Qoder payload
// checks verify NormalizeBillingHeader ran before fingerprinting.
var rawUnnormalizedCCHPattern = regexp.MustCompile(`(?i)\bcch=[0-9a-f]{5};`)

// protocolLabQoderUpstream serves local HTTP Qoder COSY inference streams while
// recording submitted desktop payloads and verifying Qoder wire invariants.
type protocolLabQoderUpstream struct {
	// mu, bodies, headers, violations, replySSE, status, errorBody, cutFrames,
	// delay, and custom coordinate thread-safe request capture in ServeHTTP
	// against Bodies and Violations snapshots in test assertions.
	mu         sync.Mutex    // request capture and response config lock
	bodies     [][]byte      // chronological raw Qoder desktop JSON request bodies
	headers    []http.Header // chronological cloned HTTP request headers
	violations []string      // recorded desktop payload invariant violations
	replySSE   []byte        // default wrapped Qoder SSE stream bytes
	// status sets the HTTP status code written by ServeHTTP and defaults to 200
	// OK when left at zero.
	status    int    // HTTP response status code
	errorBody string // non-200 HTTP JSON error body
	// cutFrames truncates replySSE after the given positive SSE frame count and
	// closes the hijacked TCP socket abruptly when greater than zero.
	cutFrames int // positive frame limit before abrupt socket close
	// delay pauses ServeHTTP before writing response headers when positive so
	// keepalive timers can commit downstream SSE headers first.
	delay time.Duration // optional pre-header response pause
	// custom overrides status and replySSE handling when non-nil so tests can
	// synthesize dynamic Qoder responses from captured request payloads.
	custom func(w http.ResponseWriter) // optional custom response writer hook
}

// TestProtocolLabIRQoderUpstreamMatrix exercises Stage 6's merged Qoder COSY
// request conversion, capability gating, conversation planning, response
// streaming and buffered folding, failover, mid-stream partial settlement, and
// mixed-group carrier continuity across Anthropic Messages (/v1/messages),
// OpenAI Responses (/v1/responses and /responses), and OpenAI Chat Completions
// (/v1/chat/completions and /chat/completions). Subtests verify both
// gateway/testkit.QoderClient in-memory capture and protocolLabQoderClient
// multi-provider HTTP transport without regressions.
func TestProtocolLabIRQoderUpstreamMatrix(t *testing.T) {
	corpus := testkit.DefaultCorpus()
	qoderProv := stage6QoderLabProviderSpec()
	clients := protocolLabClients[:3]

	t.Run("canonical_scenarios_streaming_buffered_and_usage_across_three_routes", func(t *testing.T) {
		for _, client := range clients {
			combo := labRouteCombo{
				slug:     client.slug + "_via_qoder",
				client:   client,
				provider: qoderProv,
			}
			t.Run(combo.slug, func(t *testing.T) {
				for _, scenario := range protocolLabScenarios {
					streamEntry, usageEntry := runStage6QoderCanonicalCell(t, corpus, combo, scenario)
					require.NotEqual(t, labCellFail, streamEntry.Status,
						"IR Qoder T5_STREAM failed on %s/%s: rules=%v", combo.slug, scenario, streamEntry.Rules)
					require.NotEqual(t, labCellFail, usageEntry.Status,
						"IR Qoder T5_USAGE failed on %s/%s: rules=%v", combo.slug, scenario, usageEntry.Rules)
				}

				bufCacheEntry := runStage6QoderBufferedCacheCell(t, combo)
				require.Equal(t, labCellPass, bufCacheEntry.Status,
					"IR Qoder buffered cache failed on %s: rules=%v", combo.slug, bufCacheEntry.Rules)

				bufRsnEntry := runStage6QoderBufferedReasoningAndMaxTokensCell(t, combo)
				require.Equal(t, labCellPass, bufRsnEntry.Status,
					"IR Qoder buffered reasoning/max_tokens failed on %s: rules=%v", combo.slug, bufRsnEntry.Rules)

				for _, textToolSc := range []string{
					scenarioStage6ToolXMLText,
					scenarioStage6ToolJSONText,
					scenarioStage6ToolDSMLText,
					scenarioStage6ToolAliasClient,
					scenarioStage6AnthropicEnvelope,
				} {
					toolEntry := runStage6QoderToolScenarioCell(t, combo, textToolSc)
					require.Equal(t, labCellPass, toolEntry.Status,
						"IR Qoder %s failed on %s: rules=%v", textToolSc, combo.slug, toolEntry.Rules)
				}

				for _, extraEntry := range []labCellResult{
					runStage6QoderStatelessGateCell(t, combo),
					runStage6QoderCapabilityGateCell(t, combo),
					runStage6QoderFailoverCell(t, corpus, combo),
					runStage6QoderMidstreamDisconnectCell(t, corpus, combo),
					runStage6QoderErrorFormatCell(t, corpus, combo),
				} {
					require.Equal(t, labCellPass, extraEntry.Status,
						"IR Qoder %s/%s failed on %s: rules=%v", extraEntry.Scenario, extraEntry.Law, combo.slug, extraEntry.Rules)
				}
			})
		}
	})

	t.Run("route_aliases_legacy_responses_ingress_and_protocol_fallbacks_gate", func(t *testing.T) {
		runStage6QoderRoutingAndLegacyIngressSuite(t, qoderProv)
	})

	t.Run("stateless_responses_and_cursor_chat_400_gates_with_zero_failover_and_penalty", func(t *testing.T) {
		runStage6QoderStatelessGatesSuite(t, qoderProv)
	})

	t.Run("tool_capability_thinking_control_and_committed_sse_gates", func(t *testing.T) {
		runStage6QoderCapabilityAndCommittedSSESuite(t, clients, qoderProv)
	})

	t.Run("conversation_planning_and_billing_header_cch_normalization", func(t *testing.T) {
		runStage6QoderSessionAndFingerprintSuite(t, clients, qoderProv)
	})

	t.Run("failover_midstream_disconnect_and_partial_usage_settlement", func(t *testing.T) {
		runStage6QoderFailoverAndDisconnectSuite(t, clients, qoderProv)
	})

	t.Run("mixed_group_converted_and_native_two_turn_continuity", func(t *testing.T) {
		runStage6QoderMixedGroupContinuitySuite(t, corpus, clients, qoderProv)
	})
}

// stage6QoderLabProviderSpec returns the canonical Qoder COSY labProviderKindSpec
// mapped onto qmodel_38max (qwen3.8-max in gatewaytestkit.SetQoderCatalog,
// supporting both multi-level reasoning and explicit thinking disable).
func stage6QoderLabProviderSpec() labProviderKindSpec {
	return labProviderKindSpec{
		kind:             "qoder",
		platform:         capability.PlatformQoder,
		providerType:     capability.ProviderTypeCosy,
		upstreamProtocol: protocol.ProtocolQoderChat,
		upstreamModel:    "qmodel_38max",
		failoverStatus:   http.StatusBadGateway,
	}
}

// makeStage6QoderProviderSpec constructs a labProvider for Qoder with explicit
// whitelisting of both internal route keys and public display aliases from
// gatewaytestkit.SetQoderCatalog. Callers pass an optional local BaseURL to
// route traffic to an httptest.Server or an empty string to use in-memory QoderClient capture.
func makeStage6QoderProviderSpec(id int64, priority int, baseURL string, prov labProviderKindSpec) labProvider {
	spec := makeLabProviderSpec(id, priority, baseURL, prov)
	spec.Models = append(spec.Models, "auto", "ultimate", "qmodel_38max", "qwen3.8-max", "dmodel", "deepseek-v4-pro", "gmodel", "glm-5.3")
	return spec
}

// newStage6QoderServer starts an httptest.Server backed by u and registers
// automatic Close cleanup on t. Zero-valued u.status fields default to
// http.StatusOK before the listener begins serving requests.
func newStage6QoderServer(t *testing.T, u *protocolLabQoderUpstream) *httptest.Server {
	t.Helper()
	if u.status == 0 {
		u.status = http.StatusOK
	}
	srv := httptest.NewServer(u)
	t.Cleanup(srv.Close)
	return srv
}

// ServeHTTP validates incoming Qoder generation requests, records cloned
// headers and desktop payload bytes on u, and writes the configured upstream
// response. Requests targeting non-POST methods or unexpected paths append
// diagnostic entries to u.violations before response dispatch proceeds.
//
// Response parameters are snapshotted under u.mu before optional pre-header
// delays or socket writes so concurrent Bodies and Violations calls never block
// on stalled keepalive or mid-stream truncation tests.
func (u *protocolLabQoderUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, readErr := io.ReadAll(r.Body)
	_ = r.Body.Close()

	var viols []string
	if r.Method != http.MethodPost {
		viols = append(viols, fmt.Sprintf("unexpected HTTP method %s", r.Method))
	}
	if !strings.HasPrefix(r.URL.Path, "/algo/api/v2/service/pro/sse/agent_chat_generation") {
		viols = append(viols, fmt.Sprintf("unexpected Qoder generation path %s", r.URL.Path))
	}
	if readErr != nil {
		viols = append(viols, fmt.Sprintf("read request body: %v", readErr))
	} else {
		viols = append(viols, validateStage6QoderDesktopPayload(raw, r.Header)...)
	}

	// Snapshot response directives and append cloned request artifacts under u.mu
	// before releasing the lock for artificial delays or HTTP socket writes.
	u.mu.Lock()
	u.bodies = append(u.bodies, bytes.Clone(raw))
	u.headers = append(u.headers, r.Header.Clone())
	u.violations = append(u.violations, viols...)
	delay := u.delay
	status := u.status
	errBody := u.errorBody
	replySSE := bytes.Clone(u.replySSE)
	cutFrames := u.cutFrames
	custom := u.custom
	u.mu.Unlock()

	if delay > 0 {
		// Abort the simulated pre-header delay early when the caller cancels the
		// request context so httptest.Server shutdown never hangs.
		select {
		case <-r.Context().Done():
			return
		case <-time.After(delay):
		}
	}

	if custom != nil {
		custom(w)
		return
	}

	if status != http.StatusOK {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if errBody != "" {
			_, _ = io.WriteString(w, errBody)
		}
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	if cutFrames > 0 {
		prefix := truncateSSEBytesAtFrame(replySSE, cutFrames)
		_, _ = w.Write(prefix)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		// Close the underlying TCP socket abruptly after flushing cutFrames SSE
		// events so Qoder's stream scanner observes an authentic mid-stream EOF.
		if hj, ok := w.(http.Hijacker); ok {
			if conn, rw, err := hj.Hijack(); err == nil {
				_ = rw.Flush()
				_ = conn.Close()
				return
			}
		}
		return
	}

	_, _ = w.Write(replySSE)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// Bodies returns detached byte copies of all Qoder desktop request bodies
// received so far by u under u.mu. Mutating the returned slices never alters u's
// recorded history.
func (u *protocolLabQoderUpstream) Bodies() [][]byte {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([][]byte, len(u.bodies))
	for i, b := range u.bodies {
		out[i] = bytes.Clone(b)
	}
	return out
}

// Violations returns a cloned slice of all Qoder desktop wire violations
// observed by u under u.mu. An empty slice indicates every captured request
// satisfied Qoder's desktop execution contract.
func (u *protocolLabQoderUpstream) Violations() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return slices.Clone(u.violations)
}

// truncateSSEBytesAtFrame returns the byte prefix of raw containing at most
// maxFrames non-empty SSE event blocks separated by blank lines. Non-positive
// maxFrames arguments return nil without scanning raw.
func truncateSSEBytesAtFrame(raw []byte, maxFrames int) []byte {
	if maxFrames <= 0 {
		return nil
	}
	parts := bytes.SplitAfter(raw, []byte("\n\n"))
	var out bytes.Buffer
	count := 0
	for _, p := range parts {
		if len(bytes.TrimSpace(p)) == 0 {
			continue
		}
		_, _ = out.Write(p)
		count++
		if count >= maxFrames {
			break
		}
	}
	return out.Bytes()
}

// validateStage6QoderDesktopPayload validates an outbound Qoder COSY desktop
// JSON request body and optional HTTP headers against Qoder's execution
// contract (section 4.5 and docs/interfaces/qoder_upstream.md). Passing a nil
// headers map skips HTTP header cross-checks while returning any JSON wire
// violations as descriptive strings.
func validateStage6QoderDesktopPayload(raw []byte, headers http.Header) []string {
	var viols []string
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return []string{"invalid JSON object: " + err.Error()}
	}
	if _, err := dec.ReadValue(); err != nil {
		viols = append(viols, "jsontext duplicate/syntax error: "+err.Error())
	}

	sessionID, _ := root["session_id"].(string)
	if strings.TrimSpace(sessionID) == "" {
		viols = append(viols, "session_id must be non-empty")
	}
	reqID, _ := root["request_id"].(string)
	chatRecID, _ := root["chat_record_id"].(string)
	if strings.TrimSpace(reqID) == "" || reqID != chatRecID {
		viols = append(viols, "request_id and chat_record_id must be non-empty and identical")
	}
	if userType, ok := root["aliyun_user_type"].(string); !ok || userType != "" {
		viols = append(viols, "aliyun_user_type must be present as empty string")
	}

	modelCfg, _ := root["model_config"].(map[string]any)
	modelKey, _ := modelCfg["key"].(string)
	if strings.TrimSpace(modelKey) == "" {
		viols = append(viols, "model_config.key must be non-empty")
	}
	if headers != nil {
		if gotKey := headers.Get("x-model-key"); gotKey != modelKey {
			viols = append(viols, fmt.Sprintf("x-model-key header %q != model_config.key %q", gotKey, modelKey))
		}
		if gotSrc := headers.Get("x-model-source"); gotSrc != "system" {
			viols = append(viols, fmt.Sprintf("x-model-source header %q != system", gotSrc))
		}
	}
	if maxIn, _ := modelCfg["max_input_tokens"].(float64); maxIn <= 0 {
		viols = append(viols, "model_config.max_input_tokens must be positive")
	}

	params, _ := root["parameters"].(map[string]any)
	if maxTok, _ := params["max_tokens"].(float64); maxTok <= 0 {
		viols = append(viols, "parameters.max_tokens must be positive")
	}

	biz, _ := root["business"].(map[string]any)
	if prod, _ := biz["product"].(string); prod != "app" {
		viols = append(viols, fmt.Sprintf("business.product %q != app", prod))
	}

	toolsSlice, ok := root["tools"].([]any)
	if !ok {
		viols = append(viols, "tools must be a JSON array")
	} else {
		seenTools := make(map[string]bool, len(toolsSlice))
		for i, item := range toolsSlice {
			tm, _ := item.(map[string]any)
			if tType, _ := tm["type"].(string); tType != "function" {
				viols = append(viols, fmt.Sprintf("tools[%d].type %q != function", i, tType))
			}
			fn, _ := tm["function"].(map[string]any)
			name, _ := fn["name"].(string)
			if !isValidStage6QoderIdentifier(name) || len(name) > 64 {
				viols = append(viols, fmt.Sprintf("tools[%d].function.name %q invalid", i, name))
			}
			if seenTools[name] {
				viols = append(viols, fmt.Sprintf("duplicate tools[%d].function.name %q", i, name))
			}
			seenTools[name] = true
			if _, ok := fn["parameters"].(map[string]any); !ok {
				viols = append(viols, fmt.Sprintf("tools[%d].function.parameters must be JSON object", i))
			}
		}
	}

	msgsSlice, ok := root["messages"].([]any)
	if !ok || len(msgsSlice) == 0 {
		viols = append(viols, "messages must be a non-empty JSON array")
	} else {
		for i, rawMsg := range msgsSlice {
			mm, _ := rawMsg.(map[string]any)
			role, _ := mm["role"].(string)
			switch role {
			case "system":
				sysText := extractQoderDesktopMessageText(mm["content"])
				if strings.Contains(strings.ToLower(sysText), "x-anthropic-billing-header:") &&
					rawUnnormalizedCCHPattern.MatchString(sysText) {
					viols = append(viols, "system message contains un-normalized 5-hex cch checksum")
				}
				if strings.Contains(sysText, "trc1:") {
					viols = append(viols, "system message leaked trc1: carrier")
				}
			case "user":
			case "assistant":
				asstText := extractQoderDesktopMessageText(mm["content"])
				if strings.Contains(asstText, "trc1:") {
					viols = append(viols, fmt.Sprintf("messages[%d] assistant content leaked trc1: carrier", i))
				}
				if rawCalls, hasCalls := mm["tool_calls"].([]any); hasCalls {
					for j, rtc := range rawCalls {
						tcm, _ := rtc.(map[string]any)
						id, _ := tcm["id"].(string)
						if strings.TrimSpace(id) == "" {
							viols = append(viols, fmt.Sprintf("messages[%d].tool_calls[%d].id empty", i, j))
						}
						fn, _ := tcm["function"].(map[string]any)
						fnName, _ := fn["name"].(string)
						if !isValidStage6QoderIdentifier(fnName) || len(fnName) > 64 {
							viols = append(viols, fmt.Sprintf("messages[%d].tool_calls[%d].function.name %q invalid", i, j, fnName))
						}
						argStr, _ := fn["arguments"].(string)
						var parsedArgs any
						if json.Unmarshal([]byte(argStr), &parsedArgs) != nil {
							viols = append(viols, fmt.Sprintf("messages[%d].tool_calls[%d].function.arguments invalid JSON", i, j))
						}
					}
				}
			case "tool":
				tcID, _ := mm["tool_call_id"].(string)
				if strings.TrimSpace(tcID) == "" {
					viols = append(viols, fmt.Sprintf("messages[%d].tool_call_id empty", i))
				}
			default:
				viols = append(viols, fmt.Sprintf("messages[%d] unsupported role %q", i, role))
			}
		}
	}

	return viols
}

// extractQoderDesktopMessageText flattens scalar string or block-array Qoder
// message content into a newline-joined string for diagnostic inspection.
// Nil or non-text content values return an empty string without failing.
func extractQoderDesktopMessageText(content any) string {
	if s, ok := content.(string); ok {
		return s
	}
	arr, ok := content.([]any)
	if !ok {
		return ""
	}
	var parts []string
	for _, elem := range arr {
		if m, ok := elem.(map[string]any); ok {
			if txt, ok := m["text"].(string); ok {
				parts = append(parts, txt)
			}
		}
	}
	return strings.Join(parts, "\n")
}

// isValidStage6QoderIdentifier reports whether name is non-empty and consists
// solely of ASCII letters, digits, underscores, or hyphens.
func isValidStage6QoderIdentifier(name string) bool {
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

// wrapStage6QoderSSE formats inner Qoder JSON strings into wrapped COSY SSE
// data frames separated by blank lines. Setting appendDone appends a wrapped
// [DONE] sentinel frame at the end of the returned byte slice.
func wrapStage6QoderSSE(inners []string, appendDone bool) []byte {
	var buf bytes.Buffer
	for _, inner := range inners {
		fmt.Fprintf(&buf, "data: {\"body\":%q}\n\n", inner)
	}
	if appendDone {
		_, _ = buf.WriteString("data: {\"body\":\"[DONE]\"}\n\n")
	}
	return buf.Bytes()
}

// stage6QoderSSEForScenario returns the deterministic wrapped Qoder COSY SSE
// stream paired with scenario in T5 canonical and Qoder-specific cells.
// Scenarios not needing specialized tool, reasoning, or fault payloads fall
// back to a two-chunk "Hello, world!" assistant text stream with usage.
func stage6QoderSSEForScenario(scenario string) []byte {
	switch scenario {
	case "text_unicode_split":
		return wrapStage6QoderSSE([]string{
			`{"model":"qwen3.8-max-2026","choices":[{"delta":{"role":"assistant","content":"こんにちは"}}]}`,
			`{"choices":[{"delta":{"content":"世界 👨‍👩‍👧‍👦 🇯🇵 "}}]}`,
			`{"choices":[{"delta":{"content":"café"}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"usage":{"prompt_tokens":18,"completion_tokens":6}}`,
		}, true)
	case "tool_single", "tool_result_image", "tool_result_error":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_weather_osa_2","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_weather_osa_2","function":{"arguments":"\"Osaka\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":64,"completion_tokens":18}}`,
		}, true)
	case "tool_parallel", "tool_parallel_interleaved":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_weather_tyo_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_weather_tyo_1","function":{"arguments":"\"Tokyo\"}"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_time_tyo_2","type":"function","function":{"name":"get_time","arguments":"{\"timezone\":"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_time_tyo_2","function":{"arguments":"\"Asia/Tokyo\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":96,"completion_tokens":32}}`,
		}, true)
	case "tool_args_fragmented":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_frag_1","type":"function","function":{"name":"get_weather","arguments":"{\"ci"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_frag_1","function":{"arguments":"ty\":\"\\u6771"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_frag_1","function":{"arguments":"\\u4eac\\nJP\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":52,"completion_tokens":20}}`,
		}, true)
	case "tool_args_empty":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_empty_str_1","type":"function","function":{"name":"get_clock","arguments":""}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_empty_obj_2","type":"function","function":{"name":"get_uptime","arguments":"{}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":44,"completion_tokens":20}}`,
		}, true)
	case "tool_ids_missing":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"","function":{"name":"","arguments":"\"Nagoya\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":48,"completion_tokens":16}}`,
		}, true)
	case "reasoning_summary", "reasoning_token_foreign", "reasoning_carrier_roundtrip", "reasoning_encrypted_only", "legacy_history":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","reasoning_content":"Merge sort preserves equal-key order during left-before-right merging. "}}]}`,
			`{"choices":[{"delta":{"reasoning_content":"Quicksort in-place swaps can invert equal keys."}}]}`,
			`{"choices":[{"delta":{"content":"Merge sort is stable while in-place quicksort is not."}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"usage":{"prompt_tokens":42,"completion_tokens":50,"completion_tokens_details":{"reasoning_tokens":24}}}`,
		}, true)
	case "reasoning_tool_replay":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","reasoning_content":"SKU-42 has 4 units below reorder threshold 20; "}}]}`,
			`{"choices":[{"delta":{"reasoning_content":"querying SKU-99 stock next."}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_inv_sku99_2","type":"function","function":{"name":"query_inventory","arguments":"{\"sku\":"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_inv_sku99_2","function":{"arguments":"\"SKU-99\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":96,"completion_tokens":52,"completion_tokens_details":{"reasoning_tokens":28}}}`,
		}, true)
	case "tools_custom":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_patch_2","type":"function","function":{"name":"APPLY_PATCH","arguments":"{\"input\":\"*** Begin Patch\\n*** Update File: README.md\\n"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_patch_2","function":{"arguments":"@@\\n-Default timeout: 5s\\n+Default timeout: 15s\\n*** End Patch\\n\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":104,"completion_tokens":42}}`,
		}, true)
	case "tools_namespace":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_ns_trace_2","type":"function","function":{"name":"ENTERPRISE_OBSERVABILITY_TELEMETRY_CONNECTOR__QUERY_DI__5375327D","arguments":"{\"service\":\"checkout\",\"limit\":20}"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_ns_spawn_2","type":"function","function":{"name":"COLLABORATION__SPAWN_AGENT","arguments":"{\"task\":\"summarize slow spans\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":132,"completion_tokens":38}}`,
		}, true)
	case "tools_search":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_search_2","type":"function","function":{"name":"TOOL_SEARCH","arguments":"{\"query\":\"slack notify\",\"limit\":3}"}}]}}]}`,
			`{"choices":[{"delta":{"tool_calls":[{"index":1,"id":"call_inc_2","type":"function","function":{"name":"PAGER__LIST_INCIDENTS","arguments":"{\"severity\":\"sev2\"}"}}]}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
			`{"usage":{"prompt_tokens":148,"completion_tokens":36}}`,
		}, true)
	case "structured_output":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","content":"{\"summary\":\"All checks passed\","}}]}`,
			`{"choices":[{"delta":{"content":"\"score\":98}"}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"usage":{"prompt_tokens":30,"completion_tokens":12}}`,
		}, true)
	case "max_tokens":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","content":"Truncated at ceiling"}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"length"}]}`,
			`{"usage":{"prompt_tokens":22,"completion_tokens":16}}`,
		}, true)
	case "usage_cache":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","content":"Cached prefix reused."}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"usage":{"prompt_tokens":120,"completion_tokens":24,"prompt_tokens_details":{"cached_tokens":80,"cacheable_tokens":15},"completion_tokens_details":{"reasoning_tokens":6}}}`,
		}, true)
	case "empty_response":
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant"}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"usage":{"prompt_tokens":12,"completion_tokens":0}}`,
		}, true)
	case "stream_truncated":
		return []byte(
			"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"role\\\":\\\"assistant\\\",\\\"content\\\":\\\"Partial output before socket drop\\\"}}]}\"}\n\n" +
				"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"tool_calls\\\":[{\\\"index\\\":0,\\\"id\\\":\\\"call_trunc_1\\\",\\\"type\\\":\\\"function\\\",\\\"function\\\":{\\\"name\\\":\\\"get_weather\\\",\\\"arguments\\\":\\\"{\\\\\\\"city\\\\\\\":\\\"}}]}}]}\"}\n\n",
		)
	case "stream_error_event":
		return []byte(
			"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"role\\\":\\\"assistant\\\",\\\"content\\\":\\\"Partial output before rate limit\\\"}}]}\"}\n\n" +
				"data: {\"statusCode\":\"TOO_MANY_REQUESTS\",\"statusCodeValue\":429,\"body\":\"{\\\"error\\\":{\\\"code\\\":\\\"rate_limit_exceeded\\\",\\\"message\\\":\\\"Too many requests\\\"}}\"}\n\n",
		)
	case "upstream_sse_quirks":
		return []byte(
			": keepalive comment\r\n" +
				"data:{\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"role\\\":\\\"assistant\\\",\\\"content\\\":\\\"Quirky \\\"}},{\\\"delta\\\":{}}]}\"}   \r\n\r\n" +
				"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"Qoder SSE.\\\"}}]}\"}\r\n\r\n" +
				"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{},\\\"finish_reason\\\":\\\"stop\\\"}]}\"}\r\n\r\n" +
				"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":20,\\\"completion_tokens\\\":5}}\"}\r\n\r\n" +
				"data: {\"body\":\"[DONE]\"}",
		)
	case scenarioStage6ToolXMLText:
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"content":"Checking forecast: <tool_cal"}}]}`,
			`{"choices":[{"delta":{"content":"l>get_weather<arg_key>city</arg_key><arg_value>Tokyo</arg_value></tool_call>"}}]}`,
			`{"usage":{"prompt_tokens":36,"completion_tokens":14}}`,
		}, true)
	case scenarioStage6ToolJSONText:
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"content":"Lookup: <tool_call>{\"name\":\"get_weather\","}}]}`,
			`{"choices":[{"delta":{"content":"\"arguments\":{\"city\":\"Osaka\"}}</tool_call>"}}]}`,
			`{"usage":{"prompt_tokens":38,"completion_tokens":15}}`,
		}, true)
	case scenarioStage6ToolDSMLText:
		return wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"content":"DSML call: <｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\"get_weather\">"}}]}`,
			`{"choices":[{"delta":{"content":"<｜｜DSML｜｜parameter name=\"city\">Kyoto</｜｜DSML｜｜parameter></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>"}}]}`,
			`{"usage":{"prompt_tokens":40,"completion_tokens":16}}`,
		}, true)
	case scenarioStage6AnthropicEnvelope:
		return wrapStage6QoderSSE([]string{
			`{"event":"content_block_delta","data":{"type":"thinking_delta","text":"Checking Osaka weather."}}`,
			`{"event":"content_block_delta","data":{"type":"text_delta","text":"Calling weather API."}}`,
			`{"event":"tool_use_start","index":0,"data":{"id":"call_env_1","name":"get_weather","type":"tool_use"}}`,
			`{"event":"tool_use_delta","index":0,"data":{"id":"call_env_1","arguments":"{}"}}`,
			`{"event":"tool_use_delta","index":4,"data":{"id":"call_env_1","arguments":"{\"city\":\"Osaka\"}"}}`,
			`{"usage":{"input_tokens":50,"output_tokens":4,"prompt_tokens_details":{"cached_tokens":20}}}`,
			`{"event":"message_delta","data":{"usage":{"output_tokens":11}}}`,
			`{"event":"message_stop"}`,
		}, false)
	default:
		return wrapStage6QoderSSE([]string{
			`{"model":"qwen3.8-max-2026","choices":[{"delta":{"role":"assistant","content":"Hello, "}}]}`,
			`{"choices":[{"delta":{"content":"world!"}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"usage":{"prompt_tokens":16,"completion_tokens":4}}`,
		}, true)
	}
}

// runStage6QoderCanonicalCell executes one canonical section 5.5 scenario on
// combo against a local Qoder COSY server and returns its paired T5_STREAM and
// T5_USAGE results. Scenarios without a client request wire fixture or native
// passthrough-only scenarios return labCellSkip.
func runStage6QoderCanonicalCell(
	t *testing.T,
	corpus *testkit.Corpus,
	combo labRouteCombo,
	scenario string,
) (labCellResult, labCellResult) {
	t.Helper()
	baseStream := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenario,
		Law:      lawT5Stream,
	}
	baseUsage := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenario,
		Law:      lawT5Usage,
	}

	if scenario == "reasoning_carrier_native" {
		baseStream.Status = labCellSkip
		baseUsage.Status = labCellSkip
		return baseStream, baseUsage
	}

	reqMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.client.protocol,
		Kind:     testkit.FixtureRequest,
		Scenario: scenario,
	})
	require.NoError(t, err)
	if len(reqMatches) == 0 {
		baseStream.Status = labCellSkip
		baseUsage.Status = labCellSkip
		return baseStream, baseUsage
	}

	qUp := &protocolLabQoderUpstream{
		replySSE: stage6QoderSSEForScenario(scenario),
	}
	upSrv := newStage6QoderServer(t, qUp)

	capture := &gatewaytestkit.QoderClient{}
	lab := newProtocolLabRouter(t, labOptions{
		Providers:   []labProvider{makeStage6QoderProviderSpec(101, 0, upSrv.URL, combo.provider)},
		QoderClient: capture,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status, contentType, respBody := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var streamRules []string
	if len(qUp.Violations()) > 0 {
		streamRules = appendUniqueLabRule(streamRules, ruleQoderDesktopWireInvalid)
	}

	isFaultScenario := scenario == "stream_truncated" || scenario == "stream_error_event"
	var frames []sse.Frame
	isValidSSE200 := status == http.StatusOK && strings.Contains(strings.ToLower(contentType), "text/event-stream")
	if !isValidSSE200 {
		streamRules = appendUniqueLabRule(streamRules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		require.Equal(t, 1, capture.BodyCount(), "gateway/testkit.QoderClient must capture submitted payload")
		parsedFrames, frameErr := readAllSSEFrames(respBody)
		frames = parsedFrames
		if frameErr != nil {
			streamRules = appendUniqueLabRule(streamRules, "client_sse_read_error")
		}
		for _, gv := range testkit.ValidateStreamGrammar(
			combo.client.protocol,
			frames,
			testkit.GrammarOptions{AllowAbnormal: isFaultScenario},
		) {
			streamRules = appendUniqueLabRule(streamRules, gv.Rule)
		}
		if isFaultScenario && !hasLabAbnormalTerminalErrorFrame(combo.client.protocol, frames) {
			streamRules = appendUniqueLabRule(streamRules, ruleMissingFaultErrorTermination)
		}
		if combo.client.protocol == protocol.ProtocolOpenAIResponses &&
			(scenario == "tools_custom" || scenario == "tools_namespace" || scenario == "tools_search") &&
			!stage5ClientStreamRestoredScenario(scenario, respBody) {
			streamRules = appendUniqueLabRule(streamRules, ruleQoderToolNotRestored)
		}
		if scenario == "max_tokens" && !clientStreamHasLengthFinish(combo.client.protocol, frames) {
			streamRules = appendUniqueLabRule(streamRules, ruleQoderFinishLengthMismatch)
		}
		if (scenario == "reasoning_summary" || scenario == "reasoning_tool_replay") &&
			!clientStreamHasQoderReasoningContinuity(combo.client.protocol, frames) {
			streamRules = appendUniqueLabRule(streamRules, ruleQoderPlainCarrierMissing)
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

// runStage6QoderBufferedCacheCell evaluates non-streaming (stream=false)
// buffered Qoder response folding and cached token projection (prompt_tokens=120,
// cached_tokens=80, cacheable_tokens=15 -> InputTokens=40, CacheReadTokens=80,
// CacheCreationTokens=0, OutputTokens=24) across client JSON and UsageLog.
// It returns labCellPass only when outbound Qoder wire format, JSON
// content, and settled token buckets all match expectations.
func runStage6QoderBufferedCacheCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenarioStage6BufferedCache,
		Law:      lawT5QoderBuffer,
	}

	capture := &gatewaytestkit.QoderClient{
		Body: string(stage6QoderSSEForScenario("usage_cache")),
	}
	lab := newProtocolLabRouter(t, labOptions{
		Providers:   []labProvider{makeStage6QoderProviderSpec(101, 0, "", combo.provider)},
		QoderClient: capture,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := buildStage6SimpleRequest(combo.client.protocol, false, "buffered cache test")
	status, ct, respBytes := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	if capture.BodyCount() != 1 || len(validateStage6QoderDesktopPayload(capture.BodyAt(0), nil)) > 0 {
		rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
	}
	if status != http.StatusOK || !strings.Contains(strings.ToLower(ct), "application/json") {
		rules = appendUniqueLabRule(rules, ruleQoderBufferedJSONInvalid)
	} else {
		if !strings.Contains(string(respBytes), "Cached prefix reused.") {
			rules = appendUniqueLabRule(rules, ruleQoderBufferedJSONInvalid)
		}
		if combo.client.protocol == protocol.ProtocolOpenAIResponses &&
			!gjson.GetBytes(respBytes, "usage.output_tokens_details.reasoning_tokens").Exists() {
			rules = appendUniqueLabRule(rules, "missing_output_tokens_details")
		}
		rules = append(rules, compareBufferedClientAndSettledUsage(combo.client.protocol, respBytes, lab.UsageLogs(), 40, 80, 24)...)
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = dedupStage6Rules(rules)
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage6QoderBufferedReasoningAndMaxTokensCell evaluates non-streaming
// Qoder buffered conversion on reasoning output plus finish_reason=length
// truncation across all three client protocols. It checks trc1:plain carrier
// sealing on Messages and Responses, reasoning_content preservation on Chat,
// and protocol-native truncation stop-reason mapping.
func runStage6QoderBufferedReasoningAndMaxTokensCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenarioStage6BufferedReasoning,
		Law:      lawT5QoderBuffer,
	}

	sseBody := wrapStage6QoderSSE([]string{
		`{"choices":[{"delta":{"role":"assistant","reasoning_content":"Step 1: inspect ceiling. "}}]}`,
		`{"choices":[{"delta":{"content":"Partial buffered answer"}}]}`,
		`{"choices":[{"delta":{},"finish_reason":"length"}]}`,
		`{"usage":{"prompt_tokens":30,"completion_tokens":18,"completion_tokens_details":{"reasoning_tokens":7}}}`,
	}, true)
	capture := &gatewaytestkit.QoderClient{Body: string(sseBody)}
	lab := newProtocolLabRouter(t, labOptions{
		Providers:   []labProvider{makeStage6QoderProviderSpec(101, 0, "", combo.provider)},
		QoderClient: capture,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := buildStage6SimpleRequest(combo.client.protocol, false, "buffered reasoning truncation")
	status, ct, respBytes := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	if capture.BodyCount() != 1 || len(validateStage6QoderDesktopPayload(capture.BodyAt(0), nil)) > 0 {
		rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
	}
	if status != http.StatusOK || !strings.Contains(strings.ToLower(ct), "application/json") {
		rules = appendUniqueLabRule(rules, ruleQoderBufferedJSONInvalid)
	} else {
		switch combo.client.protocol {
		case protocol.ProtocolAnthropicMessages:
			sig := gjson.GetBytes(respBytes, "content.0.signature").String()
			carrier, ok := ir.ParseCarrier(sig)
			if !ok || carrier.Scheme != ir.SchemePlain || carrier.Text != "Step 1: inspect ceiling. " {
				rules = appendUniqueLabRule(rules, ruleQoderPlainCarrierMissing)
			}
			if gjson.GetBytes(respBytes, "stop_reason").String() != "max_tokens" {
				rules = appendUniqueLabRule(rules, ruleQoderFinishLengthMismatch)
			}
		case protocol.ProtocolOpenAIResponses:
			enc := gjson.GetBytes(respBytes, "output.0.encrypted_content").String()
			carrier, ok := ir.ParseCarrier(enc)
			if !ok || carrier.Scheme != ir.SchemePlain || carrier.Text != "Step 1: inspect ceiling. " {
				rules = appendUniqueLabRule(rules, ruleQoderPlainCarrierMissing)
			}
			if gjson.GetBytes(respBytes, "status").String() != "incomplete" ||
				gjson.GetBytes(respBytes, "incomplete_details.reason").String() != "max_output_tokens" {
				rules = appendUniqueLabRule(rules, ruleQoderFinishLengthMismatch)
			}
		case protocol.ProtocolOpenAIChatCompletions:
			if gjson.GetBytes(respBytes, "choices.0.message.reasoning_content").String() != "Step 1: inspect ceiling. " {
				rules = appendUniqueLabRule(rules, ruleQoderPlainCarrierMissing)
			}
			if gjson.GetBytes(respBytes, "choices.0.finish_reason").String() != "length" {
				rules = appendUniqueLabRule(rules, ruleQoderFinishLengthMismatch)
			}
		}
		rules = append(rules, compareBufferedClientAndSettledUsage(combo.client.protocol, respBytes, lab.UsageLogs(), 30, 0, 18)...)
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = dedupStage6Rules(rules)
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage6QoderToolScenarioCell evaluates streaming and buffered Qoder tool
// decoding, outbound desktop wire validity, usage parity, and client tool
// restoration across XML/JSON/DSML text-encoded tool calls, Anthropic-shaped
// Qoder envelopes (with sparse message_delta usage), and QoderToolNameMapper
// alias reversal stacked with client tool raising. Any wire violation,
// un-decoded markup, invalid SSE grammar, usage mismatch, or un-restored tool
// identity records a failure rule on the returned entry.
func runStage6QoderToolScenarioCell(
	t *testing.T,
	combo labRouteCombo,
	scenario string,
) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenario,
		Law:      lawT5QoderTools,
	}

	var rules []string
	if scenario == scenarioStage6ToolAliasClient {
		rules = runStage6ToolAliasAndClientRestorationChecks(t, combo)
	} else {
		ssePayload := stage6QoderSSEForScenario(scenario)
		wantFresh, wantCacheRead, wantOutput := stage6ExpectedToolScenarioUsage(scenario)
		for _, streamMode := range []bool{true, false} {
			capture := &gatewaytestkit.QoderClient{Body: string(ssePayload)}
			lab := newProtocolLabRouter(t, labOptions{
				Providers:   []labProvider{makeStage6QoderProviderSpec(101, 0, "", combo.provider)},
				QoderClient: capture,
			})
			gwSrv := httptest.NewServer(lab)
			reqBody := buildStage6WeatherToolRequest(combo.client.protocol, streamMode, "get_weather")
			status, ct, respBytes := sendLabClientRequest(t, gwSrv, combo.client, reqBody)
			gwSrv.Close()

			if capture.BodyCount() != 1 || len(validateStage6QoderDesktopPayload(capture.BodyAt(0), nil)) > 0 {
				rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
			}
			if status != http.StatusOK {
				rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
				continue
			}
			if streamMode {
				if !strings.Contains(strings.ToLower(ct), "text/event-stream") {
					rules = appendUniqueLabRule(rules, "missing_sse_content_type")
					continue
				}
				frames, fErr := readAllSSEFrames(respBytes)
				if fErr != nil {
					rules = appendUniqueLabRule(rules, "client_sse_read_error")
				}
				for _, gv := range testkit.ValidateStreamGrammar(combo.client.protocol, frames, testkit.GrammarOptions{}) {
					rules = appendUniqueLabRule(rules, gv.Rule)
				}
				if !clientPayloadHasDecodedWeatherToolCall(combo.client.protocol, true, respBytes, frames) {
					rules = appendUniqueLabRule(rules, ruleQoderTextToolNotDecoded)
				}
				rules = append(rules, compareClientAndSettledUsage(combo.client.protocol, frames, lab.UsageLogs())...)
			} else {
				if !strings.Contains(strings.ToLower(ct), "application/json") {
					rules = appendUniqueLabRule(rules, ruleQoderBufferedJSONInvalid)
					continue
				}
				if !clientPayloadHasDecodedWeatherToolCall(combo.client.protocol, false, respBytes, nil) {
					rules = appendUniqueLabRule(rules, ruleQoderTextToolNotDecoded)
				}
				rules = append(rules, compareBufferedClientAndSettledUsage(
					combo.client.protocol,
					respBytes,
					lab.UsageLogs(),
					wantFresh,
					wantCacheRead,
					wantOutput,
				)...)
			}
		}
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = dedupStage6Rules(rules)
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// stage6ExpectedToolScenarioUsage returns the expected fresh input, cache_read,
// and output token buckets for Qoder tool-decoding scenario fixtures.
// Unrecognized scenario slugs return zeroes across all three token counters.
func stage6ExpectedToolScenarioUsage(scenario string) (int, int, int) {
	switch scenario {
	case scenarioStage6ToolXMLText:
		return 36, 0, 14
	case scenarioStage6ToolJSONText:
		return 38, 0, 15
	case scenarioStage6ToolDSMLText:
		return 40, 0, 16
	case scenarioStage6AnthropicEnvelope:
		return 30, 20, 11
	default:
		return 0, 0, 0
	}
}

// runStage6ToolAliasAndClientRestorationChecks verifies QoderToolNameMapper
// case-alias reversal stacked with overlong identifier sanitization on
// Messages/Chat and custom.exec plus scoped namespace lowering on Responses.
// Both streaming SSE and buffered JSON responses are checked, returning any
// violated rule identifiers.
func runStage6ToolAliasAndClientRestorationChecks(t *testing.T, combo labRouteCombo) []string {
	t.Helper()
	var rules []string

	for _, streamMode := range []bool{true, false} {
		var reqBody []byte
		var wantNames []string
		switch combo.client.protocol {
		case protocol.ProtocolOpenAIResponses:
			streamFlag := "false"
			if streamMode {
				streamFlag = "true"
			}
			reqBody = []byte(fmt.Sprintf(`{
				"model":"gpt-5",
				"stream":%s,
				"tools":[
					{"type":"custom","name":"code.exec","description":"Execute snippet"},
					{"type":"namespace","name":"git_ops","tools":[
						{"type":"function","name":"Diff_Status","parameters":{"type":"object","properties":{}}}
					]}
				],
				"input":"Run code.exec and git_ops Diff_Status."
			}`, streamFlag))
			wantNames = []string{"code.exec", "Diff_Status"}
		case protocol.ProtocolAnthropicMessages:
			overlong := "enterprise_observability_cluster_diagnostic_latency_histogram_report_v2"
			reqBody = buildStage6WeatherToolRequest(combo.client.protocol, streamMode, overlong)
			wantNames = []string{overlong}
		default:
			mixedCase := "Get_Weather_Forecast"
			reqBody = buildStage6WeatherToolRequest(combo.client.protocol, streamMode, mixedCase)
			wantNames = []string{mixedCase}
		}

		// Qoder upstream replies with uppercase aliases of whatever declared function
		// names reached payload["tools"], exercising QoderDeclaredToolNameMapper -> raiseTools.
		qUp := &protocolLabQoderUpstream{}
		qUp.custom = func(w http.ResponseWriter) {
			bodies := qUp.Bodies()
			last := bodies[len(bodies)-1]
			var toolCalls []string
			gjson.GetBytes(last, "tools").ForEach(func(idx, val gjson.Result) bool {
				declName := val.Get("function.name").String()
				upperAlias := strings.ToUpper(declName)
				arg := `{"city":"Sapporo"}`
				if strings.Contains(strings.ToLower(declName), "code_exec") {
					arg = `{"input":"echo hello"}`
				}
				toolCalls = append(toolCalls, fmt.Sprintf(
					`{"index":%d,"id":"call_alias_%d","type":"function","function":{"name":%q,"arguments":%q}}`,
					idx.Int(), idx.Int(), upperAlias, arg,
				))
				return true
			})
			var sseBytes []byte
			if len(toolCalls) == 0 {
				sseBytes = wrapStage6QoderSSE([]string{
					`{"choices":[{"delta":{"role":"assistant","content":"no tools declared"}}]}`,
					`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
					`{"usage":{"prompt_tokens":20,"completion_tokens":4}}`,
				}, true)
			} else {
				sseBytes = wrapStage6QoderSSE([]string{
					fmt.Sprintf(`{"choices":[{"delta":{"role":"assistant","tool_calls":[%s]}}]}`, strings.Join(toolCalls, ",")),
					`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
					`{"usage":{"prompt_tokens":40,"completion_tokens":16}}`,
				}, true)
			}
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(sseBytes)
		}
		upSrv := newStage6QoderServer(t, qUp)

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeStage6QoderProviderSpec(101, 0, upSrv.URL, combo.provider)},
		})
		gwSrv := httptest.NewServer(lab)
		status, _, respBytes := sendLabClientRequest(t, gwSrv, combo.client, reqBody)
		gwSrv.Close()

		if len(qUp.Violations()) > 0 {
			rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
		}
		if status != http.StatusOK {
			rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
			continue
		}
		if streamMode {
			frames, fErr := readAllSSEFrames(respBytes)
			if fErr != nil {
				rules = appendUniqueLabRule(rules, "client_sse_read_error")
			}
			for _, gv := range testkit.ValidateStreamGrammar(combo.client.protocol, frames, testkit.GrammarOptions{}) {
				rules = appendUniqueLabRule(rules, gv.Rule)
			}
		}
		if !verifyStage6RestoredAliasPayload(combo.client.protocol, streamMode, respBytes, wantNames) {
			rules = appendUniqueLabRule(rules, ruleQoderToolNotRestored)
		}
	}
	return rules
}

// runStage6QoderStatelessGateCell evaluates local HTTP 400 stateless rejections
// on combo alongside positive stateful Chat and Messages controls. Rejected
// requests must neither invoke upstream Qoder providers nor penalize scheduler
// health state.
func runStage6QoderStatelessGateCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenarioStage6StatelessGate,
		Law:      lawT5QoderGate,
	}

	var rules []string
	fallbackUp := &protocolLabQoderUpstream{replySSE: stage6QoderSSEForScenario("text_basic")}
	fbSrv := newStage6QoderServer(t, fallbackUp)

	capture := &gatewaytestkit.QoderClient{Body: string(stage6QoderSSEForScenario("text_basic"))}
	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeStage6QoderProviderSpec(1, 0, "", combo.provider),
			makeStage6QoderProviderSpec(2, 1, fbSrv.URL, combo.provider),
		},
		MaxProviderSwitches: 2,
		QoderClient:         capture,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	switch combo.client.protocol {
	case protocol.ProtocolOpenAIResponses:
		for _, badBody := range [][]byte{
			[]byte(`{"model":"gpt-5","stream":true,"store":true,"input":"stateful store"}`),
			[]byte(`{"model":"gpt-5","stream":true,"previous_response_id":"resp_prev_99","input":"stateful prev"}`),
			[]byte(`{"model":"gpt-5","stream":true,"conversation":"conv_abc","input":"stateful conv"}`),
			[]byte(`{"model":"gpt-5","stream":true,"input":[{"type":"item_reference","id":"item_ref_1"}]}`),
			[]byte(`{"model":"gpt-5","stream":true,"input":[{"type":"message","role":"user","content":"hi"},{"type":"function_call_output","call_id":"call_orphan_1","output":"ok"}]}`),
		} {
			st, _, resp := sendLabClientRequest(t, gwSrv, combo.client, badBody)
			if st != http.StatusBadRequest || !hasClientErrorEnvelope(combo.client.protocol, resp) {
				rules = appendUniqueLabRule(rules, ruleQoderStatelessGateBypassed)
			}
		}
		if capture.BodyCount() != 0 || len(fallbackUp.Bodies()) != 0 || len(lab.UsageLogs()) != 0 {
			rules = appendUniqueLabRule(rules, ruleQoderGateContactedUpstream)
		}
		if stage6QoderProviderPenalized(lab, 1) {
			rules = appendUniqueLabRule(rules, ruleQoderGatePenalizedHealth)
		}

	case protocol.ProtocolOpenAIChatCompletions:
		// Reject Cursor input-shaped Chat requests locally with HTTP 400 whenever
		// stateful fields or unpaired function_call_output items violate Qoder's stateless contract.
		for _, badCursor := range [][]byte{
			[]byte(`{"model":"gpt-4o","stream":true,"store":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"cursor store"}]}]}`),
			[]byte(`{"model":"gpt-4o","stream":true,"previous_response_id":"resp_cur_1","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"cursor prev"}]}]}`),
			[]byte(`{"model":"gpt-4o","stream":true,"conversation":"conv_cur_1","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"cursor conv"}]}]}`),
			[]byte(`{"model":"gpt-4o","stream":true,"input":[{"type":"item_reference","id":"item_cur_1"}]}`),
			[]byte(`{"model":"gpt-4o","stream":true,"input":[{"type":"function_call_output","call_id":"call_orphan","output":"ok"}]}`),
		} {
			st, _, resp := sendLabClientRequest(t, gwSrv, combo.client, badCursor)
			if st != http.StatusBadRequest || !hasClientErrorEnvelope(combo.client.protocol, resp) {
				rules = appendUniqueLabRule(rules, ruleQoderStatelessGateBypassed)
			}
		}
		if capture.BodyCount() != 0 || len(fallbackUp.Bodies()) != 0 {
			rules = appendUniqueLabRule(rules, ruleQoderGateContactedUpstream)
		}
		if stage6QoderProviderPenalized(lab, 1) {
			rules = appendUniqueLabRule(rules, ruleQoderGatePenalizedHealth)
		}

		// Allow stateless Cursor input-shaped Chat turns through convert.NewExchange
		// single-hop while preserving user prompt text on Qoder's messages array.
		lab.ResetQoderCapture()
		validCursor := []byte(`{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"valid cursor turn"}]}]}`)
		stCur, _, _ := sendLabClientRequest(t, gwSrv, combo.client, validCursor)
		if stCur != http.StatusOK || capture.BodyCount() != 1 || !strings.Contains(string(capture.BodyAt(0)), "valid cursor turn") {
			rules = appendUniqueLabRule(rules, ruleQoderCursorInputRejected)
		}

		// Allow standard messages-shaped Chat Completions carrying store:true and
		// standalone tool outputs because StatelessResponses applies only to Responses ingress.
		lab.ResetQoderCapture()
		stdChat := []byte(`{"model":"gpt-4o","stream":true,"store":true,"messages":[{"role":"user","content":"check"},{"role":"tool","tool_call_id":"call_standalone_1","content":"tool output ok"}]}`)
		stStd, _, _ := sendLabClientRequest(t, gwSrv, combo.client, stdChat)
		if stStd != http.StatusOK || capture.BodyCount() != 1 {
			rules = appendUniqueLabRule(rules, "chat_store_or_standalone_tool_rejected")
		}

	case protocol.ProtocolAnthropicMessages:
		// Malformed Messages payload rejects locally with 400 and zero upstream/health side effects,
		// while standalone tool_result in user content passes through cleanly.
		stBad, _, respBad := sendLabClientRequest(t, gwSrv, combo.client, []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"messages":"not-an-array"}`))
		if stBad != http.StatusBadRequest || !hasClientErrorEnvelope(combo.client.protocol, respBad) {
			rules = appendUniqueLabRule(rules, ruleQoderStatelessGateBypassed)
		}
		if capture.BodyCount() != 0 || len(fallbackUp.Bodies()) != 0 {
			rules = appendUniqueLabRule(rules, ruleQoderGateContactedUpstream)
		}
		if stage6QoderProviderPenalized(lab, 1) {
			rules = appendUniqueLabRule(rules, ruleQoderGatePenalizedHealth)
		}
		lab.ResetQoderCapture()
		stStandalone, _, _ := sendLabClientRequest(t, gwSrv, combo.client, []byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"stream":true,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_standalone_1","content":"standalone result"},{"type":"text","text":"summarize"}]}]}`))
		if stStandalone != http.StatusOK || capture.BodyCount() != 1 {
			rules = appendUniqueLabRule(rules, "messages_standalone_tool_result_rejected")
		}
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = dedupStage6Rules(rules)
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage6QoderCapabilityGateCell evaluates attrs.ToolCall=false optional tool
// stripping plus mandatory tool_choice 400 rejection, mandatory-thinking model
// disabled-thinking 400 rejection, and pre-committed keepalive CommittedSSE
// terminal SSE error emission. Every local 400 gate must leave upstream call
// counts and provider health unpenalized.
func runStage6QoderCapabilityGateCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenarioStage6CapabilityGate,
		Law:      lawT5QoderGate,
	}

	var rules []string
	fallbackUp := &protocolLabQoderUpstream{replySSE: stage6QoderSSEForScenario("text_basic")}
	fbSrv := newStage6QoderServer(t, fallbackUp)

	// Map client models onto "ultimate" (mandatory thinking in SetQoderCatalog) and disable ToolCall via group rule.
	ultimateProv := combo.provider
	ultimateProv.upstreamModel = "ultimate"
	capture := &gatewaytestkit.QoderClient{Body: string(stage6QoderSSEForScenario("text_basic"))}
	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeStage6QoderProviderSpec(1, 0, "", ultimateProv),
			makeStage6QoderProviderSpec(2, 1, fbSrv.URL, ultimateProv),
		},
		MaxProviderSwitches: 2,
		QoderClient:         capture,
		ModelAttributeRules: []routing.ModelAttributeRule{{
			Models:     []string{"*"},
			Attributes: modelcatalog.Attributes{ToolCall: new(false)},
		}},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	// Strip optional tool declarations to an empty array when attrs.ToolCall is
	// false while preserving historical assistant tool calls and tool outputs.
	optToolReq := buildStage6ToolHistoryRequest(combo.client.protocol, "auto")
	stOpt, _, _ := sendLabClientRequest(t, gwSrv, combo.client, optToolReq)
	if stOpt != http.StatusOK || capture.BodyCount() != 1 {
		rules = appendUniqueLabRule(rules, "optional_tools_gate_failed")
	} else {
		sentPayload := capture.BodyAt(0)
		if gjson.GetBytes(sentPayload, "tools.#").Int() != 0 {
			rules = appendUniqueLabRule(rules, "optional_tools_not_stripped")
		}
		if !strings.Contains(string(sentPayload), "call_hist_1") {
			rules = appendUniqueLabRule(rules, "tool_history_dropped_when_tools_disabled")
		}
	}

	// Reject mandatory tool_choice locally with HTTP 400 when attrs.ToolCall is
	// false without invoking upstream Qoder providers or penalizing health.
	lab.ResetQoderCapture()
	reqToolReq := buildStage6ToolHistoryRequest(combo.client.protocol, "required")
	stReq, _, respReq := sendLabClientRequest(t, gwSrv, combo.client, reqToolReq)
	if stReq != http.StatusBadRequest || !hasClientErrorEnvelope(combo.client.protocol, respReq) {
		rules = appendUniqueLabRule(rules, "required_tool_choice_not_rejected_400")
	}
	if capture.BodyCount() != 0 || len(fallbackUp.Bodies()) != 0 {
		rules = appendUniqueLabRule(rules, ruleQoderGateContactedUpstream)
	}
	if stage6QoderProviderPenalized(lab, 1) {
		rules = appendUniqueLabRule(rules, ruleQoderGatePenalizedHealth)
	}

	// Reject explicit disabled thinking locally with HTTP 400 when targeting
	// Qoder's mandatory-thinking "ultimate" model without failover or health penalties.
	lab.SetModelAttributeRules(nil)
	lab.ResetQoderCapture()
	disableThinkReq := buildStage6DisableThinkingRequest(combo.client.protocol)
	stThink, _, respThink := sendLabClientRequest(t, gwSrv, combo.client, disableThinkReq)
	if stThink != http.StatusBadRequest || !hasClientErrorEnvelope(combo.client.protocol, respThink) {
		rules = appendUniqueLabRule(rules, "mandatory_thinking_disable_not_rejected_400")
	}
	if capture.BodyCount() != 0 || len(fallbackUp.Bodies()) != 0 {
		rules = appendUniqueLabRule(rules, ruleQoderGateContactedUpstream)
	}
	if stage6QoderProviderPenalized(lab, 1) {
		rules = appendUniqueLabRule(rules, ruleQoderGatePenalizedHealth)
	}

	// 4. CommittedSSE already committed by keepalive ping before non-switchable upstream 401 failure:
	// gateway must emit an untimed client-protocol terminal SSE error frame instead of raw JSON.
	slowAuthErrUp := &protocolLabQoderUpstream{
		delay:     70 * time.Millisecond,
		status:    http.StatusUnauthorized,
		errorBody: `{"code":"101","message":"Signature expired"}`,
	}
	slowSrv := newStage6QoderServer(t, slowAuthErrUp)
	sseLab := newProtocolLabRouter(t, labOptions{
		Providers:         []labProvider{makeStage6QoderProviderSpec(11, 0, slowSrv.URL, combo.provider)},
		KeepaliveInterval: 20 * time.Millisecond,
	})
	sseGWSrv := httptest.NewServer(sseLab)
	defer sseGWSrv.Close()

	stSSE, ctSSE, bodySSE := sendLabClientRequest(t, sseGWSrv, combo.client, buildStage6SimpleRequest(combo.client.protocol, true, "keepalive committed error"))
	if stSSE != http.StatusOK || !strings.Contains(strings.ToLower(ctSSE), "text/event-stream") {
		rules = appendUniqueLabRule(rules, ruleQoderCommittedSSEMissingErr)
	} else {
		sseFrames, fErr := readAllSSEFrames(bodySSE)
		if fErr != nil || !hasLabAbnormalTerminalErrorFrame(combo.client.protocol, sseFrames) {
			rules = appendUniqueLabRule(rules, ruleQoderCommittedSSEMissingErr)
		}
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = dedupStage6Rules(rules)
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage6QoderSessionReuseCell evaluates two-turn Qoder conversation planning
// and x-anthropic-billing-header volatile 5-hex cch normalization (cch=1a2b3;
// followed by cch=9f8e7;). Both outbound Qoder payloads must carry normalized
// cch=<cch>; system text and reuse an identical session_id across turns.
func runStage6QoderSessionReuseCell(t *testing.T, combo labRouteCombo) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenarioStage6SessionReuseCCH,
		Law:      lawT5QoderSession,
	}

	capture := &gatewaytestkit.QoderClient{
		Body: string(wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","content":"Turn 1 answer."}}]}`,
			`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
			`{"usage":{"prompt_tokens":24,"completion_tokens":6}}`,
		}, true)),
	}
	lab := newProtocolLabRouter(t, labOptions{
		Providers:   []labProvider{makeStage6QoderProviderSpec(101, 0, "", combo.provider)},
		QoderClient: capture,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	sysTurn1 := "x-anthropic-billing-header: cc_version=2.1.330.975; cc_entrypoint=sdk-cli; cch=1a2b3;\nFollow repository guidelines."
	sysTurn2 := "x-anthropic-billing-header: cc_version=2.1.330.975; cc_entrypoint=sdk-cli; cch=9f8e7;\nFollow repository guidelines."
	t1Req, t2Req := buildStage6TwoTurnCCHRequests(combo.client.protocol, sysTurn1, sysTurn2)

	st1, _, _ := sendLabClientRequest(t, gwSrv, combo.client, t1Req)
	st2, _, _ := sendLabClientRequest(t, gwSrv, combo.client, t2Req)

	var rules []string
	if st1 != http.StatusOK || st2 != http.StatusOK || capture.BodyCount() != 2 {
		rules = appendUniqueLabRule(rules, ruleQoderSessionFingerprintDrift)
	} else {
		b1 := capture.BodyAt(0)
		b2 := capture.BodyAt(1)
		if len(validateStage6QoderDesktopPayload(b1, nil)) > 0 || len(validateStage6QoderDesktopPayload(b2, nil)) > 0 {
			rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
		}
		sid1 := gjson.GetBytes(b1, "session_id").String()
		sid2 := gjson.GetBytes(b2, "session_id").String()
		if sid1 == "" || sid1 != sid2 {
			rules = appendUniqueLabRule(rules, ruleQoderSessionFingerprintDrift)
		}
		if !strings.Contains(gjson.GetBytes(b1, "messages.0.content").String(), "cch=<cch>;") {
			rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
		}
	}

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = dedupStage6Rules(rules)
	} else {
		entry.Status = labCellPass
	}
	return entry
}

// runStage6QoderFailoverCell evaluates two-provider Qoder failover when
// Provider 1 returns HTTP 502 before emitting any semantic output. Provider 2
// must serve a grammar-valid client SSE stream and receive sole UsageLog
// attribution without leaking Provider 1's error sentinel.
func runStage6QoderFailoverCell(
	t *testing.T,
	corpus *testkit.Corpus,
	combo labRouteCombo,
) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
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

	sentinel := "qoder-primary-502-sentinel"
	p1Up := &protocolLabQoderUpstream{
		status:    http.StatusBadGateway,
		errorBody: fmt.Sprintf(`{"code":"500","message":%q}`, sentinel),
	}
	p2Up := &protocolLabQoderUpstream{
		replySSE: stage6QoderSSEForScenario("text_basic"),
	}
	p1Srv := newStage6QoderServer(t, p1Up)
	p2Srv := newStage6QoderServer(t, p2Up)

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeStage6QoderProviderSpec(1, 0, p1Srv.URL, combo.provider),
			makeStage6QoderProviderSpec(2, 1, p2Srv.URL, combo.provider),
		},
		MaxProviderSwitches: 2,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status, ct, respBody := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	if len(p1Up.Violations()) > 0 || len(p2Up.Violations()) > 0 {
		rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
	}
	if len(p1Up.Bodies()) == 0 {
		rules = appendUniqueLabRule(rules, "primary_not_contacted")
	}
	if len(p2Up.Bodies()) != 1 {
		rules = appendUniqueLabRule(rules, "fallback_not_selected")
	}
	if bytes.Contains(respBody, []byte(sentinel)) {
		rules = appendUniqueLabRule(rules, "primary_content_leaked")
	}
	if status != http.StatusOK || !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		frames, fErr := readAllSSEFrames(respBody)
		if fErr != nil {
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

// runStage6QoderMidstreamDisconnectCell truncates Provider 1's Qoder SSE stream
// after emitting early usage, partial text output, and an unclosed tool call.
// Once semantic output is served, Provider 2 must remain untouched while the
// gateway closes open blocks with a terminal SSE error frame and settles partial usage.
func runStage6QoderMidstreamDisconnectCell(
	t *testing.T,
	corpus *testkit.Corpus,
	combo labRouteCombo,
) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: scenarioDisconnectAfterData,
		Law:      lawT5MidstreamTruncate,
	}

	reqMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.client.protocol,
		Kind:     testkit.FixtureRequest,
		Scenario: "tool_single",
	})
	require.NoError(t, err)

	partialSSE := wrapStage6QoderSSE([]string{
		`{"usage":{"prompt_tokens":48,"completion_tokens":7,"prompt_tokens_details":{"cached_tokens":12}}}`,
		`{"choices":[{"delta":{"role":"assistant","content":"partial-midstream-chunk"}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_mid_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":"}}]}}]}`,
		`{"choices":[{"delta":{},"finish_reason":"tool_calls"}]}`,
	}, true)

	p1Up := &protocolLabQoderUpstream{
		replySSE:  partialSSE,
		cutFrames: 3,
	}
	p2Up := &protocolLabQoderUpstream{
		replySSE: stage6QoderSSEForScenario("text_basic"),
	}
	p1Srv := newStage6QoderServer(t, p1Up)
	p2Srv := newStage6QoderServer(t, p2Up)

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeStage6QoderProviderSpec(1, 0, p1Srv.URL, combo.provider),
			makeStage6QoderProviderSpec(2, 1, p2Srv.URL, combo.provider),
		},
		MaxProviderSwitches: 2,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status, ct, respBody := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	if len(p1Up.Violations()) > 0 {
		rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
	}
	if len(p2Up.Bodies()) > 0 {
		rules = appendUniqueLabRule(rules, ruleFallbackContactedAfterOutput)
	}
	if status != http.StatusOK || !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("gateway_http_%d", status))
	} else {
		if !bytes.Contains(respBody, []byte("partial-midstream-chunk")) {
			rules = appendUniqueLabRule(rules, ruleMissingPartialContent)
		}
		frames, fErr := readAllSSEFrames(respBody)
		if fErr != nil {
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
	} else if logs[0].InputTokens != 36 || logs[0].CacheReadTokens != 12 || logs[0].OutputTokens != 7 {
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

// runStage6QoderErrorFormatCell verifies that a non-switchable Qoder HTTP 401
// error formats into a 4xx client-protocol JSON error envelope. Failed pre-byte
// authentication attempts must not settle any UsageLog rows.
func runStage6QoderErrorFormatCell(
	t *testing.T,
	corpus *testkit.Corpus,
	combo labRouteCombo,
) labCellResult {
	t.Helper()
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
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

	errUp := &protocolLabQoderUpstream{
		status:    http.StatusUnauthorized,
		errorBody: `{"code":"101","message":"Invalid Qoder signature"}`,
	}
	upSrv := newStage6QoderServer(t, errUp)

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeStage6QoderProviderSpec(1, 0, upSrv.URL, combo.provider)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	reqBody := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status, _, respBody := sendLabClientRequest(t, gwSrv, combo.client, reqBody)

	var rules []string
	if len(errUp.Violations()) > 0 {
		rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
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

// runStage6QoderSwitchRouteReplayCell executes a two-turn reasoning_tool_replay
// exchange switching between Qoder and combo.client's native provider via
// SetProviders. Setting convertedFirst=true runs Turn 1 on Qoder and Turn 2 on
// the native mock server; setting convertedFirst=false runs Turn 1 on the
// native mock server and Turn 2 on Qoder. Turn 2's request is synthesized via
// testkit.SimulateNextTurn with store:false on Responses so trc1:plain carriers
// and foreign native replay tokens face strict target validation.
func runStage6QoderSwitchRouteReplayCell(
	t *testing.T,
	corpus *testkit.Corpus,
	combo labRouteCombo,
	convertedFirst bool,
) labCellResult {
	t.Helper()
	law := lawT5SwitchFromNative
	if convertedFirst {
		law = lawT5SwitchToNative
	}
	entry := labCellResult{
		Client:   combo.client.protocol,
		Upstream: protocol.ProtocolQoderChat,
		Scenario: "reasoning_tool_replay",
		Law:      law,
	}

	nativeSpec := findLabProviderKind(combo.client.nativeKind)
	reqMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: combo.client.protocol,
		Kind:     testkit.FixtureRequest,
		Scenario: "reasoning_tool_replay",
	})
	require.NoError(t, err)

	nativeTurn1Matches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: nativeSpec.upstreamProtocol,
		Kind:     testkit.FixtureStream,
		Scenario: "reasoning_tool_replay",
	})
	require.NoError(t, err)
	nativeTurn2Matches, err := corpus.QueryFixtures(testkit.FixtureQuery{
		Source:   testkit.SourceWire,
		Protocol: nativeSpec.upstreamProtocol,
		Kind:     testkit.FixtureStream,
		Scenario: "text_basic",
	})
	require.NoError(t, err)

	nativeStreamPath := nativeTurn2Matches[0].Path
	if !convertedFirst {
		nativeStreamPath = nativeTurn1Matches[0].Path
	}
	nativeMock := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "qoder_switch_native",
			Default: testkit.Reply{Raw: nativeStreamPath},
		},
		Validation: validationOptionsForProvider(nativeSpec, reqMatches[0].Data),
	})
	nativeSrv := newLabMockServer(nativeSpec, nativeMock)
	defer nativeSrv.Close()

	qoderSSE := stage6QoderSSEForScenario("text_basic")
	if convertedFirst {
		qoderSSE = stage6QoderSSEForScenario("reasoning_tool_replay")
	}
	qUp := &protocolLabQoderUpstream{replySSE: qoderSSE}
	qSrv := newStage6QoderServer(t, qUp)

	var firstSpecs, secondSpecs []labProvider
	if convertedFirst {
		firstSpecs = []labProvider{makeStage6QoderProviderSpec(101, 0, qSrv.URL, combo.provider)}
		secondSpecs = []labProvider{makeLabProviderSpec(202, 0, nativeSrv.URL, nativeSpec)}
	} else {
		firstSpecs = []labProvider{makeLabProviderSpec(101, 0, nativeSrv.URL, nativeSpec)}
		secondSpecs = []labProvider{makeStage6QoderProviderSpec(202, 0, qSrv.URL, combo.provider)}
	}

	lab := newProtocolLabRouter(t, labOptions{
		Providers: firstSpecs,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	turn1Body := prepareLabStreamRequestBody(combo.client.protocol, reqMatches[0].Data)
	status1, ct1, resp1 := sendLabClientRequest(t, gwSrv, combo.client, turn1Body)

	var rules []string
	if convertedFirst && len(qUp.Violations()) > 0 {
		rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
	}
	if !convertedFirst {
		for _, v := range nativeSrv.Violations() {
			for _, rv := range v.RuleViolations {
				rules = appendUniqueLabRule(rules, "turn1_"+rv.RuleID)
			}
		}
	}
	if status1 != http.StatusOK || !strings.Contains(strings.ToLower(ct1), "text/event-stream") {
		rules = appendUniqueLabRule(rules, fmt.Sprintf("turn1_gateway_http_%d", status1))
		entry.Status = labCellFail
		entry.Rules = rules
		return entry
	}
	turn1Frames, fErr := readAllSSEFrames(resp1)
	if fErr != nil {
		rules = appendUniqueLabRule(rules, "turn1_sse_read_error")
	}
	for _, gv := range testkit.ValidateStreamGrammar(combo.client.protocol, turn1Frames, testkit.GrammarOptions{}) {
		rules = appendUniqueLabRule(rules, "turn1_"+gv.Rule)
	}

	turn2Raw, simErr := testkit.SimulateNextTurn(combo.client.protocol, testkit.TurnInput{
		PrevRequest:              turn1Body,
		ResponseFrames:           turn1Frames,
		SynthesizeMissingOutputs: true,
		FollowUpUserText:         "Confirm final SKU-99 restock decision.",
	})
	if simErr != nil {
		rules = appendUniqueLabRule(rules, "simulate_next_turn_error")
		entry.Status = labCellFail
		entry.Rules = rules
		return entry
	}
	turn2Body := prepareLabStreamRequestBody(combo.client.protocol, turn2Raw)
	if combo.client.protocol == protocol.ProtocolOpenAIResponses {
		// Ensure stateless store:false on Responses Turn 2 so both native Responses
		// PrepareNativeReplay synthetic-ID stripping and stateless Qoder accept the turn.
		turn2Body = forceResponsesStoreFalse(turn2Body)
	}

	lab.SetProviders(t, secondSpecs)
	status2, ct2, resp2 := sendLabClientRequest(t, gwSrv, combo.client, turn2Body)

	if convertedFirst {
		for _, v := range nativeSrv.Violations() {
			for _, rv := range v.RuleViolations {
				rules = appendUniqueLabRule(rules, rv.RuleID)
			}
		}
		for _, rec := range nativeSrv.Requests() {
			if hasUnstrippedCarrierInTokenPaths(nativeSpec.upstreamProtocol, rec.Body) {
				rules = appendUniqueLabRule(rules, ruleUnstrippedTRC1Carrier)
			}
		}
	} else if len(qUp.Violations()) > 0 {
		rules = appendUniqueLabRule(rules, ruleQoderDesktopWireInvalid)
	}

	if status2 != http.StatusOK || !strings.Contains(strings.ToLower(ct2), "text/event-stream") {
		if !convertedFirst || len(nativeSrv.Violations()) == 0 {
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

// runStage6QoderRoutingAndLegacyIngressSuite verifies unprefixed /responses and
// /chat/completions aliases, legacy Responses messages/functions/commands
// ingress normalization, and protocol_fallbacks=[] conversion opt-out on Qoder.
// All subchecks assert that gateway/testkit.QoderClient captures allowed
// payloads and receives zero calls when protocol conversion is disabled.
func runStage6QoderRoutingAndLegacyIngressSuite(t *testing.T, qoderProv labProviderKindSpec) {
	t.Helper()
	capture := &gatewaytestkit.QoderClient{Body: string(stage6QoderSSEForScenario("text_basic"))}
	lab := newProtocolLabRouter(t, labOptions{
		Providers:   []labProvider{makeStage6QoderProviderSpec(101, 0, "", qoderProv)},
		QoderClient: capture,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	// Route unprefixed /responses and /chat/completions aliases through the same
	// Qoder conversion executor and capture fixture as the canonical /v1/* paths.
	for _, alias := range []labClientSpec{
		{slug: "responses_alias", protocol: protocol.ProtocolOpenAIResponses, path: "/responses"},
		{slug: "chat_alias", protocol: protocol.ProtocolOpenAIChatCompletions, path: "/chat/completions"},
	} {
		lab.ResetQoderCapture()
		reqBody := buildStage6SimpleRequest(alias.protocol, false, "alias check")
		st, ct, resp := sendLabClientRequest(t, gwSrv, alias, reqBody)
		require.Equal(t, http.StatusOK, st, string(resp))
		require.Contains(t, strings.ToLower(ct), "application/json")
		require.Equal(t, 1, capture.BodyCount())
		if alias.protocol == protocol.ProtocolOpenAIResponses {
			// Echo omitted top-level store as store:false on stateless Qoder Responses
			// payloads so clients observe that server-side state was not persisted.
			require.True(t, gjson.GetBytes(resp, "store").Exists())
			require.False(t, gjson.GetBytes(resp, "store").Bool())
		}
	}

	// Normalize legacy Responses messages and functions fields into canonical input
	// and tools while stripping deprecated commands before encoding Qoder payloads.
	lab.ResetQoderCapture()
	legacyRespIngress := []byte(`{
		"model":"gpt-5",
		"stream":false,
		"messages":[{"role":"system","content":"Follow strict formatting."},{"role":"user","content":"Legacy messages field on /v1/responses"}],
		"functions":[{"name":"check_status","description":"Check system status","parameters":{"type":"object","properties":{}}}],
		"commands":["/legacy_slash_ignored"]
	}`)
	stLeg, _, respLeg := sendLabClientRequest(t, gwSrv, protocolLabClients[1], legacyRespIngress)
	require.Equal(t, http.StatusOK, stLeg, string(respLeg))
	require.Equal(t, 1, capture.BodyCount())
	sentLeg := capture.BodyAt(0)
	require.Contains(t, string(sentLeg), "Legacy messages field on /v1/responses")
	require.Equal(t, "check_status", gjson.GetBytes(sentLeg, "tools.0.function.name").String())
	require.NotContains(t, string(sentLeg), "legacy_slash_ignored")

	// Reject Responses requests before upstream dispatch when group protocol_fallbacks
	// explicitly maps openai_responses to an empty target slice.
	lab.ResetQoderCapture()
	lab.SetProtocolFallbacks(map[protocol.ProtocolID][]protocol.ProtocolID{
		protocol.ProtocolOpenAIResponses: {},
	})
	defer lab.SetProtocolFallbacks(nil)
	stBlocked, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","stream":false,"input":"blocked"}`))
	require.NotEqual(t, http.StatusOK, stBlocked)
	require.Zero(t, capture.BodyCount())
}

// runStage6QoderStatelessGatesSuite exercises stateless Responses and Cursor
// input-shaped Chat local 400 rejections alongside positive stateful Chat and
// Messages controls using gateway/testkit.QoderClient. Every rejected case
// verifies zero QoderClient calls, zero failover to Provider 2, and zero health penalties.
func runStage6QoderStatelessGatesSuite(t *testing.T, qoderProv labProviderKindSpec) {
	t.Helper()
	fbUp := &protocolLabQoderUpstream{replySSE: stage6QoderSSEForScenario("text_basic")}
	fbSrv := newStage6QoderServer(t, fbUp)
	capture := &gatewaytestkit.QoderClient{Body: string(stage6QoderSSEForScenario("text_basic"))}

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			makeStage6QoderProviderSpec(1, 0, "", qoderProv),
			makeStage6QoderProviderSpec(2, 1, fbSrv.URL, qoderProv),
		},
		MaxProviderSwitches: 2,
		QoderClient:         capture,
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	rejectCases := []struct {
		name     string        // subtest case identifier
		client   labClientSpec // target client entry point
		body     string        // stateful or invalid client payload
		wantCode string        // expected OpenAI error.code
	}{
		{
			name:     "responses_store_true",
			client:   protocolLabClients[1],
			body:     `{"model":"gpt-5","stream":false,"store":true,"input":"hello"}`,
			wantCode: "unsupported_response_state",
		},
		{
			name:     "responses_previous_response_id",
			client:   protocolLabClients[1],
			body:     `{"model":"gpt-5","stream":true,"previous_response_id":"resp_abc","input":"hello"}`,
			wantCode: "",
		},
		{
			name:     "responses_conversation_object",
			client:   protocolLabClients[1],
			body:     `{"model":"gpt-5","stream":false,"conversation":{"id":"conv_99"},"input":"hello"}`,
			wantCode: "unsupported_response_state",
		},
		{
			name:     "responses_item_reference",
			client:   protocolLabClients[1],
			body:     `{"model":"gpt-5","stream":false,"input":[{"type":"item_reference","id":"msg_01"}]}`,
			wantCode: "unsupported_response_state",
		},
		{
			name:     "responses_unpaired_function_call_output",
			client:   protocolLabClients[1],
			body:     `{"model":"gpt-5","stream":false,"input":[{"type":"function_call_output","call_id":"call_missing","output":"done"}]}`,
			wantCode: "",
		},
		{
			name:     "cursor_chat_store_true",
			client:   protocolLabClients[2],
			body:     `{"model":"gpt-4o","stream":false,"store":true,"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`,
			wantCode: "unsupported_response_state",
		},
		{
			name:     "cursor_chat_previous_response_id",
			client:   protocolLabClients[2],
			body:     `{"model":"gpt-4o","stream":false,"previous_response_id":"resp_cur","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`,
			wantCode: "unsupported_response_state",
		},
		{
			name:     "cursor_chat_conversation",
			client:   protocolLabClients[2],
			body:     `{"model":"gpt-4o","stream":false,"conversation":"conv_cur","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"hi"}]}]}`,
			wantCode: "unsupported_response_state",
		},
		{
			name:     "cursor_chat_item_reference",
			client:   protocolLabClients[2],
			body:     `{"model":"gpt-4o","stream":false,"input":[{"type":"item_reference","id":"ref_cur"}]}`,
			wantCode: "unsupported_response_state",
		},
		{
			name:     "cursor_chat_unpaired_function_call_output",
			client:   protocolLabClients[2],
			body:     `{"model":"gpt-4o","stream":false,"input":[{"type":"function_call_output","call_id":"call_cur_orphan","output":"done"}]}`,
			wantCode: "missing_tool_call",
		},
	}

	for _, tc := range rejectCases {
		t.Run(tc.name, func(t *testing.T) {
			lab.ResetQoderCapture()
			lab.ResetUsageLogs()
			st, _, resp := sendLabClientRequest(t, gwSrv, tc.client, []byte(tc.body))
			require.Equal(t, http.StatusBadRequest, st, string(resp))
			require.True(t, hasClientErrorEnvelope(tc.client.protocol, resp))
			require.Equal(t, "invalid_request_error", gjson.GetBytes(resp, "error.type").String())
			if tc.wantCode != "" {
				require.Equal(t, tc.wantCode, gjson.GetBytes(resp, "error.code").String())
			}
			require.Zero(t, capture.BodyCount(), "local 400 gate must never invoke QoderClient")
			require.Empty(t, fbUp.Bodies(), "local 400 gate must never fail over to secondary provider")
			require.Empty(t, lab.UsageLogs(), "local 400 gate must not settle UsageLog")
			require.False(t, stage6QoderProviderPenalized(lab, 1), "local 400 gate must not penalize provider health")
		})
	}

	// Positive controls: standard Chat store:true, Chat standalone tool message,
	// Messages missing-ID tool_result folding, and Cursor stateless input Chat all succeed.
	t.Run("positive_controls_chat_store_and_standalone_tool_results", func(t *testing.T) {
		lab.ResetQoderCapture()
		stChat, _, respChat := sendLabClientRequest(t, gwSrv, protocolLabClients[2],
			[]byte(`{"model":"gpt-4o","stream":false,"store":true,"messages":[{"role":"user","content":"run"},{"role":"tool","tool_call_id":"call_ok_1","content":"ok"}]}`))
		require.Equal(t, http.StatusOK, stChat, string(respChat))
		require.Equal(t, 1, capture.BodyCount())

		lab.ResetQoderCapture()
		stMsg, _, respMsg := sendLabClientRequest(t, gwSrv, protocolLabClients[0],
			[]byte(`{"model":"claude-sonnet-4-5","max_tokens":128,"stream":false,"messages":[{"role":"user","content":[{"type":"tool_result","content":"folded id-less output"},{"type":"text","text":"next"}]}]}`))
		require.Equal(t, http.StatusOK, stMsg, string(respMsg))
		require.Equal(t, 1, capture.BodyCount())
		require.Contains(t, string(capture.BodyAt(0)), "folded id-less output")

		lab.ResetQoderCapture()
		stCur, _, respCur := sendLabClientRequest(t, gwSrv, protocolLabClients[2],
			[]byte(`{"model":"gpt-4o","stream":false,"reasoning_effort":"low","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"cursor stateless ok"}]}]}`))
		require.Equal(t, http.StatusOK, stCur, string(respCur))
		require.Equal(t, 1, capture.BodyCount())
		require.Equal(t, "Hello, world!", gjson.GetBytes(respCur, "choices.0.message.content").String())

		// Owned previous_response_id (produced by Turn 1 on this same API key) must still
		// reject locally with HTTP 400 on stateless Qoder without invoking QoderClient or penalizing health.
		lab.ResetQoderCapture()
		stT1, _, respT1 := sendLabClientRequest(t, gwSrv, protocolLabClients[1], []byte(`{"model":"gpt-5","stream":false,"input":"turn 1"}`))
		require.Equal(t, http.StatusOK, stT1, string(respT1))
		ownedRespID := gjson.GetBytes(respT1, "id").String()
		require.NotEmpty(t, ownedRespID)
		lab.ResetQoderCapture()
		lab.ResetUsageLogs()
		stOwnedPrev, _, respOwnedPrev := sendLabClientRequest(t, gwSrv, protocolLabClients[1],
			fmt.Appendf(nil, `{"model":"gpt-5","stream":false,"previous_response_id":%q,"input":"turn 2 stateful"}`, ownedRespID))
		require.Equal(t, http.StatusBadRequest, stOwnedPrev, string(respOwnedPrev))
		require.True(t, hasClientErrorEnvelope(protocol.ProtocolOpenAIResponses, respOwnedPrev))
		require.Zero(t, capture.BodyCount())
		require.Empty(t, fbUp.Bodies())
		require.Empty(t, lab.UsageLogs())
		require.False(t, stage6QoderProviderPenalized(lab, 1))
	})
}

// runStage6QoderCapabilityAndCommittedSSESuite verifies allowed_tools subset
// ordering, thinking effort promotion, explicit thinking disable on capable
// Qoder models, and CommittedSSE pre-output error handling. It also checks that
// DeferRetryableCommittedError fails over cleanly after a keepalive ping commits headers.
func runStage6QoderCapabilityAndCommittedSSESuite(
	t *testing.T,
	clients []labClientSpec,
	qoderProv labProviderKindSpec,
) {
	t.Helper()

	t.Run("allowed_tools_subset_ordering_and_conditional_tool_call_disable", func(t *testing.T) {
		capture := &gatewaytestkit.QoderClient{Body: string(stage6QoderSSEForScenario("text_basic"))}
		lab := newProtocolLabRouter(t, labOptions{
			Providers:   []labProvider{makeStage6QoderProviderSpec(1, 0, "", qoderProv)},
			QoderClient: capture,
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		// Filter outbound Qoder tools to the allowed_tools subset while preserving
		// original declaration order ("t_alpha" before "t_gamma") rather than choice order.
		allowedReq := []byte(`{
			"model":"gpt-5",
			"stream":false,
			"tools":[
				{"type":"function","name":"t_alpha","parameters":{"type":"object","properties":{}}},
				{"type":"function","name":"t_beta","parameters":{"type":"object","properties":{}}},
				{"type":"function","name":"t_gamma","parameters":{"type":"object","properties":{}}}
			],
			"tool_choice":{"type":"allowed_tools","mode":"required","tools":[{"type":"function","name":"t_gamma"},{"type":"function","name":"t_alpha"}]},
			"input":"pick allowed"
		}`)
		st, _, resp := sendLabClientRequest(t, gwSrv, protocolLabClients[1], allowedReq)
		require.Equal(t, http.StatusOK, st, string(resp))
		sent := capture.BodyAt(0)
		require.Equal(t, int64(2), gjson.GetBytes(sent, "tools.#").Int())
		require.Equal(t, "t_alpha", gjson.GetBytes(sent, "tools.0.function.name").String())
		require.Equal(t, "t_gamma", gjson.GetBytes(sent, "tools.1.function.name").String())
		require.Equal(t, "required", gjson.GetBytes(sent, "parameters.tool_choice").String())

		// Promote requested "high" reasoning_effort onto "xhigh" when qmodel_38max
		// advertises Efforts ["low", "medium", "xhigh"] without a "high" tier.
		lab.ResetQoderCapture()
		stHigh, _, _ := sendLabClientRequest(t, gwSrv, protocolLabClients[2],
			[]byte(`{"model":"gpt-4o","stream":false,"reasoning_effort":"high","messages":[{"role":"user","content":"promote effort"}]}`))
		require.Equal(t, http.StatusOK, stHigh)
		require.Equal(t, "xhigh", gjson.GetBytes(capture.BodyAt(0), "parameters.reasoning_effort").String())

		// Allow explicit thinking disable across all three client protocols when
		// targeting qmodel_38max with ThinkingSpec.CanDisable enabled.
		for _, cli := range clients {
			lab.ResetQoderCapture()
			stDis, _, respDis := sendLabClientRequest(t, gwSrv, cli, buildStage6DisableThinkingRequest(cli.protocol))
			require.Equal(t, http.StatusOK, stDis, "%s: %s", cli.slug, string(respDis))
			require.False(t, gjson.GetBytes(capture.BodyAt(0), "model_config.enable_thinking").Bool(), cli.slug)
		}
	})

	t.Run("keepalive_committed_sse_defers_retryable_error_for_failover_and_writes_terminal_on_exhaustion", func(t *testing.T) {
		for _, cli := range clients {
			t.Run(cli.slug, func(t *testing.T) {
				// 1. Provider 1 delays past keepalive ping (CommittedSSE=true) then returns wrapped 502 code 115;
				// DeferRetryableCommittedError suppresses premature error SSE so Provider 2 completes the stream!
				p1SlowRetryable := &protocolLabQoderUpstream{
					delay: 65 * time.Millisecond,
					replySSE: []byte(
						`data: {"statusCodeValue":502,"body":"{\"code\":\"115\",\"message\":\"Agent limit reached\"}"}` + "\n\n",
					),
				}
				p2OK := &protocolLabQoderUpstream{
					replySSE: stage6QoderSSEForScenario("text_basic"),
				}
				p1Srv := newStage6QoderServer(t, p1SlowRetryable)
				p2Srv := newStage6QoderServer(t, p2OK)

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{
						makeStage6QoderProviderSpec(1, 0, p1Srv.URL, qoderProv),
						makeStage6QoderProviderSpec(2, 1, p2Srv.URL, qoderProv),
					},
					MaxProviderSwitches: 2,
					KeepaliveInterval:   20 * time.Millisecond,
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				st, ct, body := sendLabClientRequest(t, gwSrv, cli, buildStage6SimpleRequest(cli.protocol, true, "keepalive failover"))
				require.Equal(t, http.StatusOK, st)
				require.Contains(t, strings.ToLower(ct), "text/event-stream")
				require.Contains(t, string(body), "Hello, ")
				require.Len(t, p1SlowRetryable.Bodies(), 1)
				require.Len(t, p2OK.Bodies(), 1)
				frames, fErr := readAllSSEFrames(body)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(cli.protocol, frames, testkit.GrammarOptions{}))
				require.Len(t, lab.UsageLogs(), 1)
				require.Equal(t, int64(2), lab.UsageLogs()[0].ProviderID)
				p1Rec, ok := lab.ProviderRecord(1)
				require.True(t, ok)
				require.NotNil(t, p1Rec.RateLimitResetAt, "agent limit code 115 must mark Provider 1 rate-limited")
			})
		}
	})
}

// runStage6QoderSessionAndFingerprintSuite verifies multi-turn conversation
// session_id reuse across volatile 5-hex x-anthropic-billing-header cch
// checksums across all three client protocols. Each protocol subtest requires
// identical Qoder session_id values across Turn 1 and Turn 2.
func runStage6QoderSessionAndFingerprintSuite(
	t *testing.T,
	clients []labClientSpec,
	qoderProv labProviderKindSpec,
) {
	t.Helper()
	for _, cli := range clients {
		t.Run(cli.slug, func(t *testing.T) {
			entry := runStage6QoderSessionReuseCell(t, labRouteCombo{
				slug:     cli.slug + "_via_qoder",
				client:   cli,
				provider: qoderProv,
			})
			require.Equal(t, labCellPass, entry.Status, "rules=%v", entry.Rules)
		})
	}
}

// runStage6QoderFailoverAndDisconnectSuite verifies HTTP 403 entitlement code
// 112 failover, HTTP 200 leading non-semantic frame failover, and unbilled
// midstream disconnects when HasUsage is false. Secondary providers must remain
// untouched once semantic output reaches the client.
func runStage6QoderFailoverAndDisconnectSuite(
	t *testing.T,
	clients []labClientSpec,
	qoderProv labProviderKindSpec,
) {
	t.Helper()

	t.Run("http_403_entitlement_112_and_http200_leading_nonsemantic_frame_failover", func(t *testing.T) {
		for _, cli := range clients {
			t.Run(cli.slug, func(t *testing.T) {
				// Case A: HTTP 403 with code 112 (entitlement denied) switches to Provider 2 without refreshing tokens.
				p1Ent := &protocolLabQoderUpstream{
					status:    http.StatusForbidden,
					errorBody: `{"code":"112","message":"Model entitlement denied"}`,
				}
				p2OK := &protocolLabQoderUpstream{
					replySSE: stage6QoderSSEForScenario("text_basic"),
				}
				s1 := newStage6QoderServer(t, p1Ent)
				s2 := newStage6QoderServer(t, p2OK)
				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{
						makeStage6QoderProviderSpec(1, 0, s1.URL, qoderProv),
						makeStage6QoderProviderSpec(2, 1, s2.URL, qoderProv),
					},
					MaxProviderSwitches: 2,
				})
				gw := httptest.NewServer(lab)
				st, _, body := sendLabClientRequest(t, gw, cli, buildStage6SimpleRequest(cli.protocol, true, "entitlement failover"))
				gw.Close()
				require.Equal(t, http.StatusOK, st)
				require.Contains(t, string(body), "Hello, ")
				require.Len(t, p1Ent.Bodies(), 1)
				require.Len(t, p2OK.Bodies(), 1)

				// Case B: HTTP 200 SSE leading role-only chunk followed by wrapped 503 APIError stages & fails over cleanly.
				p1Leading := &protocolLabQoderUpstream{
					replySSE: []byte(
						"data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"role\\\":\\\"assistant\\\"}}]}\"}\n\n" +
							"data: {\"statusCodeValue\":503,\"body\":\"{\\\"message\\\":\\\"Service overloaded\\\"}\"}\n\n",
					),
				}
				p2OK2 := &protocolLabQoderUpstream{
					replySSE: stage6QoderSSEForScenario("text_basic"),
				}
				s1b := newStage6QoderServer(t, p1Leading)
				s2b := newStage6QoderServer(t, p2OK2)
				lab2 := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{
						makeStage6QoderProviderSpec(1, 0, s1b.URL, qoderProv),
						makeStage6QoderProviderSpec(2, 1, s2b.URL, qoderProv),
					},
					MaxProviderSwitches: 2,
				})
				gw2 := httptest.NewServer(lab2)
				st2, _, body2 := sendLabClientRequest(t, gw2, cli, buildStage6SimpleRequest(cli.protocol, true, "leading frame failover"))
				gw2.Close()
				require.Equal(t, http.StatusOK, st2)
				require.Contains(t, string(body2), "world!")
				require.Len(t, p1Leading.Bodies(), 1)
				require.Len(t, p2OK2.Bodies(), 1)
				frames2, fErr := readAllSSEFrames(body2)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(cli.protocol, frames2, testkit.GrammarOptions{}))
				require.Len(t, lab2.UsageLogs(), 1)
				require.Equal(t, int64(2), lab2.UsageLogs()[0].ProviderID)
				p1Rec, ok := lab2.ProviderRecord(1)
				require.True(t, ok)
				require.NotNil(t, p1Rec.OverloadUntil, "503 wrapped APIError must mark Provider 1 overloaded")
			})
		}
	})

	t.Run("midstream_disconnect_without_usage_skips_partial_settlement", func(t *testing.T) {
		noUsagePartial := wrapStage6QoderSSE([]string{
			`{"choices":[{"delta":{"role":"assistant","content":"partial-no-usage"}}]}`,
			`{"choices":[{"delta":{"content":"never-sent"}}]}`,
		}, true)
		p1 := &protocolLabQoderUpstream{replySSE: noUsagePartial, cutFrames: 1}
		p2 := &protocolLabQoderUpstream{replySSE: stage6QoderSSEForScenario("text_basic")}
		s1 := newStage6QoderServer(t, p1)
		s2 := newStage6QoderServer(t, p2)

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{
				makeStage6QoderProviderSpec(1, 0, s1.URL, qoderProv),
				makeStage6QoderProviderSpec(2, 1, s2.URL, qoderProv),
			},
			MaxProviderSwitches: 2,
		})
		gw := httptest.NewServer(lab)
		defer gw.Close()

		st, _, resp := sendLabClientRequest(t, gw, protocolLabClients[1], []byte(`{"model":"gpt-5","stream":true,"input":"no usage abort"}`))
		require.Equal(t, http.StatusOK, st)
		require.Contains(t, string(resp), "partial-no-usage")
		require.Empty(t, p2.Bodies(), "secondary provider must not run after semantic output")
		require.Empty(t, lab.UsageLogs(), "abort without observed usage must not fabricate a UsageLog")
	})
}

// runStage6QoderMixedGroupContinuitySuite verifies that Turn 1 trc1:plain
// carriers emitted by Qoder continue losslessly or strip safely on Turn 2. It
// exercises continuation across Qoder itself, passback Anthropic Messages,
// standard Anthropic Messages, DeepSeek thinking Chat, and native OpenAI
// Responses over HTTP and WebSocket.
func runStage6QoderMixedGroupContinuitySuite(
	t *testing.T,
	corpus *testkit.Corpus,
	clients []labClientSpec,
	qoderProv labProviderKindSpec,
) {
	t.Helper()

	for _, cli := range clients {
		combo := labRouteCombo{
			slug:     cli.slug + "_via_qoder",
			client:   cli,
			provider: qoderProv,
		}
		toNat := runStage6QoderSwitchRouteReplayCell(t, corpus, combo, true)
		require.Equal(t, labCellPass, toNat.Status, "%s T5_SWITCH_TO_NATIVE failed: %v", combo.slug, toNat.Rules)
		fromNat := runStage6QoderSwitchRouteReplayCell(t, corpus, combo, false)
		require.Equal(t, labCellPass, fromNat.Status, "%s T5_SWITCH_FROM_NATIVE failed: %v", combo.slug, fromNat.Rules)
	}

	// 1. Messages Turn 1 on Qoder -> Turn 2 on passback Messages (deepseek_anthropic) preserves thinking text and clears signature.
	t.Run("qoder_plain_carrier_to_passback_messages_preserves_thinking_text", func(t *testing.T) {
		qUp := &protocolLabQoderUpstream{replySSE: stage6QoderSSEForScenario("reasoning_tool_replay")}
		qSrv := newStage6QoderServer(t, qUp)

		passbackSpec := findLabProviderKind("deepseek_anthropic")
		anthTextMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolAnthropicMessages,
			Kind:     testkit.FixtureStream,
			Scenario: "text_basic",
		})
		require.NoError(t, err)
		pbMock := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "pb_t2", Default: testkit.Reply{Raw: anthTextMatches[0].Path}},
			Validation: passbackSpec.validation,
		})
		pbSrv := newLabMockServer(passbackSpec, pbMock)
		defer pbSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeStage6QoderProviderSpec(101, 0, qSrv.URL, qoderProv)},
		})
		gw := httptest.NewServer(lab)
		defer gw.Close()

		t1Req := []byte(`{"model":"deepseek-reasoner","max_tokens":512,"stream":true,"thinking":{"type":"enabled","budget_tokens":256},"tools":[{"name":"query_inventory","input_schema":{"type":"object","properties":{"sku":{"type":"string"}}}}],"messages":[{"role":"user","content":"Check SKU-99"}]}`)
		st1, _, resp1 := sendLabClientRequest(t, gw, protocolLabClients[0], t1Req)
		require.Equal(t, http.StatusOK, st1)
		require.Contains(t, string(resp1), "trc1:plain:")
		frames1, fErr := readAllSSEFrames(resp1)
		require.NoError(t, fErr)

		t2Raw, sErr := testkit.SimulateNextTurn(protocol.ProtocolAnthropicMessages, testkit.TurnInput{
			PrevRequest:              t1Req,
			ResponseFrames:           frames1,
			SynthesizeMissingOutputs: true,
			FollowUpUserText:         "Continue on passback.",
		})
		require.NoError(t, sErr)

		lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, pbSrv.URL, passbackSpec)})
		st2, _, resp2 := sendLabClientRequest(t, gw, protocolLabClients[0], prepareLabStreamRequestBody(protocol.ProtocolAnthropicMessages, t2Raw))
		require.Equal(t, http.StatusOK, st2, string(resp2))
		require.Empty(t, pbSrv.Violations())
		sentPB := pbSrv.Requests()[0].Body
		require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolAnthropicMessages, sentPB))
		require.Equal(t, "thinking", gjson.GetBytes(sentPB, "messages.1.content.0.type").String())
		require.Contains(t, gjson.GetBytes(sentPB, "messages.1.content.0.thinking").String(), "SKU-42 has 4 units")
		require.Empty(t, gjson.GetBytes(sentPB, "messages.1.content.0.signature").String())
	})

	// 2. Responses Turn 1 on Qoder -> Turn 2 encrypted_content-only replay to Qoder, DeepSeek Chat, and Native Responses WS.
	t.Run("qoder_plain_carrier_responses_to_qoder_deepseek_chat_and_native_ws", func(t *testing.T) {
		qTurn1 := &protocolLabQoderUpstream{replySSE: stage6QoderSSEForScenario("reasoning_tool_replay")}
		qSrv1 := newStage6QoderServer(t, qTurn1)

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeStage6QoderProviderSpec(101, 0, qSrv1.URL, qoderProv)},
		})
		gw := httptest.NewServer(lab)
		defer gw.Close()

		t1Body := []byte(`{"model":"deepseek-reasoner","stream":true,"store":false,"tools":[{"type":"function","name":"query_inventory","parameters":{"type":"object","properties":{"sku":{"type":"string"}}}}],"input":"Check SKU-99"}`)
		st1, _, resp1 := sendLabClientRequest(t, gw, protocolLabClients[1], t1Body)
		require.Equal(t, http.StatusOK, st1)
		require.Contains(t, string(resp1), "trc1:plain:")
		frames1, fErr := readAllSSEFrames(resp1)
		require.NoError(t, fErr)

		var plainEnc string
		for _, f := range frames1 {
			if enc := gjson.GetBytes(f.Data, "item.encrypted_content").String(); strings.HasPrefix(enc, "trc1:plain:") {
				plainEnc = enc
				break
			}
		}
		require.NotEmpty(t, plainEnc)

		// Replay an encrypted_content-only reasoning item (with summary: []) onto
		// Qoder and onto DeepSeek Chat so trc1:plain restores assistant reasoning_content.
		encOnlyTurn2 := []byte(fmt.Sprintf(`{
			"model":"deepseek-reasoner",
			"stream":true,
			"store":false,
			"tools":[{"type":"function","name":"query_inventory","parameters":{"type":"object","properties":{"sku":{"type":"string"}}}}],
			"input":[
				{"type":"message","role":"user","content":"Check SKU-99"},
				{"type":"reasoning","id":"rs_qoder_1","summary":[],"encrypted_content":%q},
				{"type":"function_call","id":"fc_q1","call_id":"call_inv_sku99_2","name":"query_inventory","arguments":"{\"sku\":\"SKU-99\"}"},
				{"type":"function_call_output","call_id":"call_inv_sku99_2","output":"{\"stock\":40}"}
			]
		}`, plainEnc))

		qTurn2 := &protocolLabQoderUpstream{replySSE: stage6QoderSSEForScenario("text_basic")}
		qSrv2 := newStage6QoderServer(t, qTurn2)
		lab.SetProviders(t, []labProvider{makeStage6QoderProviderSpec(202, 0, qSrv2.URL, qoderProv)})
		stQ2, _, respQ2 := sendLabClientRequest(t, gw, protocolLabClients[1], encOnlyTurn2)
		require.Equal(t, http.StatusOK, stQ2, string(respQ2))
		require.Empty(t, qTurn2.Violations())

		chatMatches, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source:   testkit.SourceWire,
			Protocol: protocol.ProtocolOpenAIChatCompletions,
			Kind:     testkit.FixtureStream,
			Scenario: "text_basic",
		})
		require.NoError(t, err)
		dsChatMock := mockupstream.NewWithConfig(mockupstream.Config{
			Script:     testkit.Script{Name: "ds_chat_t2", Default: testkit.Reply{Raw: chatMatches[0].Path}},
			Validation: testkit.RequestValidationOptions{DialectRules: []string{"deepseek-thinking"}},
		})
		dsChatSrv := httptest.NewServer(dsChatMock)
		defer dsChatSrv.Close()
		lab.SetProviders(t, []labProvider{{
			ID:               303,
			Platform:         capability.PlatformDeepseek,
			Type:             capability.ProviderTypeAPIKey,
			BaseURL:          dsChatSrv.URL,
			Models:           []string{"deepseek-reasoner"},
			EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		}})
		stDS, _, respDS := sendLabClientRequest(t, gw, protocolLabClients[1], encOnlyTurn2)
		require.Equal(t, http.StatusOK, stDS, string(respDS))
		require.Empty(t, dsChatMock.Violations(), "trc1:plain must restore reasoning_content for CHAT-DEEPSEEK-THINKING")
		require.Contains(t, gjson.GetBytes(dsChatMock.Requests()[0].Body, "messages.1.reasoning_content").String(), "SKU-42 has 4 units")

		// Strip empty-summary trc1:plain reasoning items via PrepareNativeReplay
		// before forwarding Turn 2 over native OpenAI Responses WebSocket.
		var wsMu sync.Mutex
		var wsCaptured [][]byte
		wsSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, aErr := websocket.Accept(w, r, nil)
			if aErr != nil {
				return
			}
			// Close the accepted mock WebSocket connection on handler exit regardless
			// of whether client closure handshake completed cleanly.
			defer func() { _ = conn.CloseNow() }()
			_, data, rErr := conn.Read(r.Context())
			if rErr != nil {
				return
			}
			// Guard wsCaptured appends under wsMu because httptest serves WebSocket
			// connections on a separate goroutine from test assertions.
			wsMu.Lock()
			wsCaptured = append(wsCaptured, bytes.Clone(data))
			wsMu.Unlock()
			// Ignore local mock WebSocket write errors after recording the outbound
			// frame because assertions inspect wsCaptured once the turn completes.
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_ws_q","object":"response","status":"in_progress","model":"gpt-5.4","output":[]}}`))
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.completed","sequence_number":1,"response":{"id":"resp_ws_q","object":"response","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}`))
		}))
		defer wsSrv.Close()

		wsLab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{{
				ID:               404,
				Platform:         capability.PlatformOpenAI,
				Type:             capability.ProviderTypeAPIKey,
				BaseURL:          wsSrv.URL,
				Models:           []string{"gpt-5.4"},
				ModelMapping:     map[string]string{"deepseek-reasoner": "gpt-5.4"},
				EnabledProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
			}},
			AllowedProtocols: []protocol.ProtocolID{protocol.ProtocolResponsesWebSocket, protocol.ProtocolOpenAIResponses},
		})
		wsGW := httptest.NewServer(wsLab)
		defer wsGW.Close()

		wsConn, cancelWS := dialStage5LabWS(t, wsGW)
		defer cancelWS()
		wsTurn := []byte(fmt.Sprintf(`{"type":"response.create","model":"deepseek-reasoner","store":false,"input":[{"type":"message","role":"user","content":"ws after qoder"},{"type":"reasoning","id":"rs_q_ws","summary":[],"encrypted_content":%q},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`, plainEnc))
		_, wsErr := sendAndReadStage5WSTurn(t.Context(), wsConn, wsTurn)
		require.NoError(t, wsErr)
		_ = wsConn.Close(websocket.StatusNormalClosure, "done")

		wsMu.Lock()
		gotWS := slices.Clone(wsCaptured)
		wsMu.Unlock()
		require.Len(t, gotWS, 1)
		require.False(t, hasUnstrippedCarrierInTokenPaths(protocol.ProtocolOpenAIResponses, gotWS[0]))
		require.Equal(t, int64(2), gjson.GetBytes(gotWS[0], "input.#").Int(), "empty-summary trc1:plain reasoning item must be removed on native WS")
	})
}

// buildStage6SimpleRequest builds a minimal valid client request JSON payload
// for proto with the requested stream flag and prompt text. Streaming Chat
// Completions requests automatically set stream_options.include_usage=true so
// terminal usage chunks emit.
func buildStage6SimpleRequest(proto protocol.ProtocolID, stream bool, prompt string) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return fmt.Appendf(nil, `{"model":"claude-sonnet-4-5","max_tokens":256,"stream":%t,"messages":[{"role":"user","content":%q}]}`, stream, prompt)
	case protocol.ProtocolOpenAIResponses:
		return fmt.Appendf(nil, `{"model":"gpt-5","stream":%t,"input":%q}`, stream, prompt)
	default:
		if stream {
			return fmt.Appendf(nil, `{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":%q}]}`, prompt)
		}
		return fmt.Appendf(nil, `{"model":"gpt-4o","stream":false,"messages":[{"role":"user","content":%q}]}`, prompt)
	}
}

// buildStage6WeatherToolRequest builds a single-tool client request declaring
// toolName with a {"city": string} JSON Schema for proto. Streaming Chat
// Completions requests include stream_options.include_usage=true for usage parity checks.
func buildStage6WeatherToolRequest(proto protocol.ProtocolID, stream bool, toolName string) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return fmt.Appendf(nil, `{"model":"claude-sonnet-4-5","max_tokens":256,"stream":%t,"tools":[{"name":%q,"input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],"messages":[{"role":"user","content":"Check weather"}]}`, stream, toolName)
	case protocol.ProtocolOpenAIResponses:
		return fmt.Appendf(nil, `{"model":"gpt-5","stream":%t,"tools":[{"type":"function","name":%q,"parameters":{"type":"object","properties":{"city":{"type":"string"}}}}],"input":"Check weather"}`, stream, toolName)
	default:
		if stream {
			return fmt.Appendf(nil, `{"model":"gpt-4o","stream":true,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":%q,"parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],"messages":[{"role":"user","content":"Check weather"}]}`, toolName)
		}
		return fmt.Appendf(nil, `{"model":"gpt-4o","stream":false,"tools":[{"type":"function","function":{"name":%q,"parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],"messages":[{"role":"user","content":"Check weather"}]}`, toolName)
	}
}

// buildStage6ToolHistoryRequest builds a client request carrying a get_weather
// tool declaration, a historical tool call and output pair ("call_hist_1"), and
// the requested choiceMode ("auto" or "required"). On Anthropic Messages,
// choiceMode="required" maps onto {"type":"any"}.
func buildStage6ToolHistoryRequest(proto protocol.ProtocolID, choiceMode string) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		tc := `{"type":"auto"}`
		if choiceMode == "required" {
			tc = `{"type":"any"}`
		}
		return fmt.Appendf(nil, `{
			"model":"claude-sonnet-4-5",
			"max_tokens":256,
			"stream":false,
			"tools":[{"name":"get_weather","input_schema":{"type":"object","properties":{"city":{"type":"string"}}}}],
			"tool_choice":%s,
			"messages":[
				{"role":"user","content":"Weather in Tokyo?"},
				{"role":"assistant","content":[{"type":"tool_use","id":"call_hist_1","name":"get_weather","input":{"city":"Tokyo"}}]},
				{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_hist_1","content":"Sunny 22C"},{"type":"text","text":"Summarize."}]}
			]
		}`, tc)
	case protocol.ProtocolOpenAIResponses:
		return fmt.Appendf(nil, `{
			"model":"gpt-5",
			"stream":false,
			"tools":[{"type":"function","name":"get_weather","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}],
			"tool_choice":%q,
			"input":[
				{"type":"message","role":"user","content":"Weather in Tokyo?"},
				{"type":"function_call","id":"fc_hist_1","call_id":"call_hist_1","name":"get_weather","arguments":"{\"city\":\"Tokyo\"}"},
				{"type":"function_call_output","call_id":"call_hist_1","output":"Sunny 22C"},
				{"type":"message","role":"user","content":"Summarize."}
			]
		}`, choiceMode)
	default:
		return fmt.Appendf(nil, `{
			"model":"gpt-4o",
			"stream":false,
			"tools":[{"type":"function","function":{"name":"get_weather","parameters":{"type":"object","properties":{"city":{"type":"string"}}}}}],
			"tool_choice":%q,
			"messages":[
				{"role":"user","content":"Weather in Tokyo?"},
				{"role":"assistant","content":"","tool_calls":[{"id":"call_hist_1","type":"function","function":{"name":"get_weather","arguments":"{\"city\":\"Tokyo\"}"}}]},
				{"role":"tool","tool_call_id":"call_hist_1","content":"Sunny 22C"},
				{"role":"user","content":"Summarize."}
			]
		}`, choiceMode)
	}
}

// buildStage6DisableThinkingRequest builds a client request explicitly
// disabling model thinking in proto's native wire format. It emits
// thinking.type="disabled" on Messages, reasoning.effort="none" on Responses,
// and reasoning_effort="none" on Chat Completions.
func buildStage6DisableThinkingRequest(proto protocol.ProtocolID) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"thinking":{"type":"disabled"},"messages":[{"role":"user","content":"no thinking"}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"gpt-5","stream":false,"reasoning":{"effort":"none"},"input":"no thinking"}`)
	default:
		return []byte(`{"model":"gpt-4o","stream":false,"reasoning_effort":"none","messages":[{"role":"user","content":"no thinking"}]}`)
	}
}

// buildStage6TwoTurnCCHRequests constructs Turn 1 and Turn 2 client requests
// carrying sys1 and sys2 (with differing 5-hex cch checksums) and matching
// conversation history. Each protocol populates its native session key
// (Anthropic metadata.user_id, Responses prompt_cache_key, or Chat session_id)
// so QoderConversationKey enables session_id reuse across turns.
func buildStage6TwoTurnCCHRequests(proto protocol.ProtocolID, sys1, sys2 string) ([]byte, []byte) {
	const claudeMetaUser = "user_0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef_account__session_11111111-2222-3333-4444-555555555555"
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		t1 := fmt.Appendf(nil, `{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"metadata":{"user_id":%q},"system":%q,"messages":[{"role":"user","content":"First turn question."}]}`, claudeMetaUser, sys1)
		t2 := fmt.Appendf(nil, `{"model":"claude-sonnet-4-5","max_tokens":256,"stream":false,"metadata":{"user_id":%q},"system":%q,"messages":[{"role":"user","content":"First turn question."},{"role":"assistant","content":"Turn 1 answer."},{"role":"user","content":"Second turn follow-up."}]}`, claudeMetaUser, sys2)
		return t1, t2
	case protocol.ProtocolOpenAIResponses:
		t1 := fmt.Appendf(nil, `{"model":"gpt-5","stream":false,"prompt_cache_key":"qoder-cch-sess-1","instructions":%q,"input":[{"type":"message","role":"user","content":"First turn question."}]}`, sys1)
		t2 := fmt.Appendf(nil, `{"model":"gpt-5","stream":false,"prompt_cache_key":"qoder-cch-sess-1","instructions":%q,"input":[{"type":"message","role":"user","content":"First turn question."},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Turn 1 answer."}]},{"type":"message","role":"user","content":"Second turn follow-up."}]}`, sys2)
		return t1, t2
	default:
		t1 := fmt.Appendf(nil, `{"model":"gpt-4o","stream":false,"session_id":"qoder-chat-cch-sess-1","messages":[{"role":"system","content":%q},{"role":"user","content":"First turn question."}]}`, sys1)
		t2 := fmt.Appendf(nil, `{"model":"gpt-4o","stream":false,"session_id":"qoder-chat-cch-sess-1","messages":[{"role":"system","content":%q},{"role":"user","content":"First turn question."},{"role":"assistant","content":"Turn 1 answer."},{"role":"user","content":"Second turn follow-up."}]}`, sys2)
		return t1, t2
	}
}

// forceResponsesStoreFalse sets top-level "store": false on a JSON object body
// using jsontext.Value preservation so nested payload values retain exact
// formatting. Invalid JSON inputs return body unchanged.
func forceResponsesStoreFalse(body []byte) []byte {
	var doc map[string]jsontext.Value
	if err := json.Unmarshal(body, &doc); err != nil {
		// Return body unchanged when JSON parsing fails so malformed test probes
		// still reach the gateway's ingress decoder.
		return body
	}
	doc["store"] = jsontext.Value("false")
	out, err := json.Marshal(doc)
	if err != nil {
		// Return the original body bytes if re-marshaling fails so callers always
		// forward a non-empty JSON payload slice downstream.
		return body
	}
	return out
}

// clientStreamHasLengthFinish reports whether frames contain proto's length
// truncation stop reason in downstream SSE output. It matches stop_reason
// "max_tokens" on Messages, "response.incomplete" with "max_output_tokens" on
// Responses, and finish_reason "length" on Chat Completions.
func clientStreamHasLengthFinish(proto protocol.ProtocolID, frames []sse.Frame) bool {
	for _, f := range frames {
		switch proto {
		case protocol.ProtocolAnthropicMessages:
			if gjson.GetBytes(f.Data, "delta.stop_reason").String() == "max_tokens" {
				return true
			}
		case protocol.ProtocolOpenAIResponses:
			evType := f.Event
			if evType == "" {
				evType = gjson.GetBytes(f.Data, "type").String()
			}
			if evType == "response.incomplete" &&
				gjson.GetBytes(f.Data, "response.incomplete_details.reason").String() == "max_output_tokens" {
				return true
			}
		case protocol.ProtocolOpenAIChatCompletions:
			if gjson.GetBytes(f.Data, "choices.0.finish_reason").String() == "length" {
				return true
			}
		}
	}
	return false
}

// clientStreamHasQoderReasoningContinuity reports whether frames carry
// multi-turn reasoning continuity from Qoder. Messages and Responses require a
// non-empty trc1:plain carrier, while Chat Completions requires non-empty
// delta.reasoning_content chunks.
func clientStreamHasQoderReasoningContinuity(proto protocol.ProtocolID, frames []sse.Frame) bool {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		for _, f := range frames {
			if gjson.GetBytes(f.Data, "delta.type").String() == "signature_delta" {
				if c, ok := ir.ParseCarrier(gjson.GetBytes(f.Data, "delta.signature").String()); ok && c.Scheme == ir.SchemePlain && c.Text != "" {
					return true
				}
			}
		}
		return false
	case protocol.ProtocolOpenAIResponses:
		for _, f := range frames {
			for _, path := range []string{"item.encrypted_content", "response.output.0.encrypted_content"} {
				if enc := gjson.GetBytes(f.Data, path).String(); enc != "" {
					if c, ok := ir.ParseCarrier(enc); ok && c.Scheme == ir.SchemePlain && c.Text != "" {
						return true
					}
				}
			}
		}
		return false
	default:
		for _, f := range frames {
			if gjson.GetBytes(f.Data, "choices.0.delta.reasoning_content").String() != "" {
				return true
			}
		}
		return false
	}
}

// clientPayloadHasDecodedWeatherToolCall reports whether a streaming or
// buffered client response decoded a structured get_weather call with a
// non-empty CallID and city JSON argument. Any raw <tool_call> or DSML markup
// remaining in raw causes an immediate false return.
func clientPayloadHasDecodedWeatherToolCall(
	proto protocol.ProtocolID,
	stream bool,
	raw []byte,
	frames []sse.Frame,
) bool {
	if bytes.Contains(raw, []byte("<tool_call>")) || bytes.Contains(raw, []byte("<｜｜DSML｜｜")) {
		return false
	}
	var resp *ir.Response
	var err error
	if stream {
		dec, dErr := convert.NewStreamDecoder(proto)
		if dErr != nil {
			return false
		}
		var evs []ir.Event
		for _, f := range frames {
			if bytes.Equal(bytes.TrimSpace(f.Data), []byte("[DONE]")) {
				continue
			}
			batch, bErr := dec.Decode(f)
			if bErr != nil {
				return false
			}
			evs = append(evs, batch...)
		}
		evs = append(evs, dec.Close(nil)...)
		resp, err = ir.Accumulate(evs)
	} else {
		resp, err = convert.DecodeResponse(proto, raw)
	}
	if err != nil || resp == nil {
		return false
	}
	for _, p := range resp.Parts {
		if p.Kind == ir.PartToolCall && p.Call != nil && p.Call.Tool.Name == "get_weather" && p.Call.CallID != "" {
			return gjson.Get(p.Call.Arguments, "city").String() != ""
		}
	}
	return false
}

// verifyStage6RestoredAliasPayload decodes raw as an SSE stream or buffered
// JSON response and checks that every tool call matches wantNames in order. On
// OpenAI Responses, it also requires ToolCustom with unwrapped input on
// "code.exec" and restored "git_ops" Namespace on "Diff_Status".
func verifyStage6RestoredAliasPayload(
	proto protocol.ProtocolID,
	stream bool,
	raw []byte,
	wantNames []string,
) bool {
	var resp *ir.Response
	var err error
	if stream {
		frames, fErr := readAllSSEFrames(raw)
		if fErr != nil {
			return false
		}
		dec, dErr := convert.NewStreamDecoder(proto)
		if dErr != nil {
			return false
		}
		var evs []ir.Event
		for _, f := range frames {
			if bytes.Equal(bytes.TrimSpace(f.Data), []byte("[DONE]")) {
				continue
			}
			batch, bErr := dec.Decode(f)
			if bErr != nil {
				return false
			}
			evs = append(evs, batch...)
		}
		evs = append(evs, dec.Close(nil)...)
		resp, err = ir.Accumulate(evs)
	} else {
		resp, err = convert.DecodeResponse(proto, raw)
	}
	if err != nil || resp == nil {
		return false
	}

	var calls []*ir.ToolCall
	for _, p := range resp.Parts {
		if p.Kind == ir.PartToolCall && p.Call != nil {
			calls = append(calls, p.Call)
		}
	}
	if len(calls) != len(wantNames) {
		return false
	}
	for i, want := range wantNames {
		if calls[i].Tool.Name != want {
			return false
		}
		if proto == protocol.ProtocolOpenAIResponses && want == "code.exec" {
			if calls[i].Tool.Kind != ir.ToolCustom || calls[i].Arguments != "echo hello" {
				return false
			}
		}
		if proto == protocol.ProtocolOpenAIResponses && want == "Diff_Status" {
			if calls[i].Tool.Namespace != "git_ops" {
				return false
			}
		}
	}
	return true
}

// compareBufferedClientAndSettledUsage checks that a single buffered JSON
// response and settled UsageLog agree on expected fresh input, cache_read,
// zero cache_write, and output token buckets. Missing or duplicate UsageLog
// rows return ruleMissingUsageLog or ruleDuplicateUsageLogs immediately.
func compareBufferedClientAndSettledUsage(
	proto protocol.ProtocolID,
	respBytes []byte,
	logs []usage.UsageLog,
	wantFreshInput, wantCacheRead, wantOutput int,
) []string {
	var rules []string
	if len(logs) == 0 {
		return []string{ruleMissingUsageLog}
	}
	if len(logs) > 1 {
		return []string{ruleDuplicateUsageLogs}
	}
	row := logs[0]
	if row.InputTokens != wantFreshInput {
		rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
	}
	if row.CacheReadTokens != wantCacheRead {
		rules = appendUniqueLabRule(rules, ruleUsageCacheReadMismatch)
	}
	if row.CacheCreationTokens != 0 {
		rules = appendUniqueLabRule(rules, ruleUsageCacheWriteMismatch)
	}
	if row.OutputTokens != wantOutput {
		rules = appendUniqueLabRule(rules, ruleUsageOutputMismatch)
	}

	switch proto {
	case protocol.ProtocolAnthropicMessages:
		if int(gjson.GetBytes(respBytes, "usage.input_tokens").Int()) != wantFreshInput {
			rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
		}
		if int(gjson.GetBytes(respBytes, "usage.cache_read_input_tokens").Int()) != wantCacheRead {
			rules = appendUniqueLabRule(rules, ruleUsageCacheReadMismatch)
		}
		if int(gjson.GetBytes(respBytes, "usage.output_tokens").Int()) != wantOutput {
			rules = appendUniqueLabRule(rules, ruleUsageOutputMismatch)
		}
	case protocol.ProtocolOpenAIResponses:
		if int(gjson.GetBytes(respBytes, "usage.input_tokens").Int()) != wantFreshInput+wantCacheRead {
			rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
		}
		if int(gjson.GetBytes(respBytes, "usage.input_tokens_details.cached_tokens").Int()) != wantCacheRead {
			rules = appendUniqueLabRule(rules, ruleUsageCacheReadMismatch)
		}
		if int(gjson.GetBytes(respBytes, "usage.output_tokens").Int()) != wantOutput {
			rules = appendUniqueLabRule(rules, ruleUsageOutputMismatch)
		}
	case protocol.ProtocolOpenAIChatCompletions:
		if int(gjson.GetBytes(respBytes, "usage.prompt_tokens").Int()) != wantFreshInput+wantCacheRead {
			rules = appendUniqueLabRule(rules, ruleUsageInputMismatch)
		}
		if wantCacheRead > 0 && int(gjson.GetBytes(respBytes, "usage.prompt_tokens_details.cached_tokens").Int()) != wantCacheRead {
			rules = appendUniqueLabRule(rules, ruleUsageCacheReadMismatch)
		}
		if int(gjson.GetBytes(respBytes, "usage.completion_tokens").Int()) != wantOutput {
			rules = appendUniqueLabRule(rules, ruleUsageOutputMismatch)
		}
	}
	return rules
}

// stage6QoderProviderPenalized reports whether provider id on lab incurred any
// persistent status or cooldown mutation or a positive EWMA scheduler
// ErrorRate. Missing provider records return true so tests never mask disappeared accounts.
func stage6QoderProviderPenalized(lab *protocolLabRouter, id int64) bool {
	rec, ok := lab.ProviderRecord(id)
	if !ok {
		return true
	}
	if rec.Status != billing.StatusActive || !rec.Schedulable || strings.TrimSpace(rec.ErrorMessage) != "" ||
		rec.RateLimitResetAt != nil || rec.TempUnschedulableUntil != nil || rec.OverloadUntil != nil {
		return true
	}
	return lab.FeedbackSnapshot(id).ErrorRate > 0
}

// dedupStage6Rules removes duplicate rule strings while preserving first-seen order.
func dedupStage6Rules(in []string) []string {
	var out []string
	for _, r := range in {
		out = appendUniqueLabRule(out, r)
	}
	return out
}

// TestQoderRefreshConversionCounts sends the same custom tool through a
// credential retry. Each rejected HTTP dispatch must record exactly one tool
// adaptation, including when neither response reaches the usage writer.
func TestQoderRefreshConversionCounts(t *testing.T) {
	var mu sync.Mutex
	var record telemetry.RequestRecord
	var requests int
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		first := requests == 1
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if first {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"code":"101","message":"Signature expired"}`)
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"code":"112","message":"Model entitlement denied"}`)
	}))
	defer up.Close()
	spec := makeStage6QoderProviderSpec(1, 0, up.URL, stage6QoderLabProviderSpec())
	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{spec},
		OnTelemetry: func(snapshot telemetry.RequestRecord) {
			mu.Lock()
			defer mu.Unlock()
			record = snapshot
		},
	})
	store := fixtureRetryStore{ExecutionProviderStore: lab.providers}
	lab.unified.QoderRefresh = &provideradapter.QoderRequestRefresh{
		Store:       store,
		Coordinator: provider.NewOAuthRefreshAPI(store, nil, provider.RefreshOptions{}),
	}
	gw := httptest.NewServer(lab)
	defer gw.Close()
	body := []byte(`{"model":"gpt-5","stream":true,"input":"hello","tools":[{"type":"custom","name":"patch","format":{"type":"text"}}]}`)
	sendLabClientRequest(t, gw, protocolLabClients[1], body)
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 2, requests)
	require.Len(t, record.Attempts, 2)
	require.Equal(t, http.StatusUnauthorized, record.Attempts[0].Status)
	require.Equal(t, http.StatusForbidden, record.Attempts[1].Status)
	for _, attempt := range record.Attempts {
		require.NotNil(t, attempt.ProtocolConversion)
		require.Equal(t, 1, attempt.ProtocolConversion.LossCounts[string(ir.LossToolLowered)])
	}
}
