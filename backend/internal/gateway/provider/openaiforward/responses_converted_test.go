package openaiforward

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// fakeConvertedPorts provides controllable in-memory stubs for RunConvertedResponses unit tests.
type fakeConvertedPorts struct {
	msgProfile          MessagesProfile               // returned by PrepareMessages
	msgRoute            Dispatch                      // returned by PrepareMessages
	chatProfile         ChatProfile                   // returned by PrepareChat
	dispatchedRoute     Dispatch                      // recorded DispatchChat/DispatchMessages target
	dispatchedBody      []byte                        // recorded body passed to DispatchChat/DispatchMessages
	cursorConverted     bool                          // true when ConvertCursorResponsesToChat ran
	sentBodies          [][]byte                      // outbound HTTP request bodies sent via Send
	responses           []*http.Response              // queued HTTP responses returned by Send
	sinkBuf             bytes.Buffer                  // downstream bytes captured by Sink
	sinkStatus          int                           // committed HTTP status on Sink
	boundResponseID     string                        // last ResponseID bound after success
	boundDigestChain    string                        // last Anthropic digest chain bound after success
	deletedContinuation bool                          // true when DeleteResponseID ran on recovery
	prevResponseID      string                        // simulated saved previous_response_id
	continuationEnabled bool                          // simulated ContinuationEnabled flag
	replayScheme        string                        // optional replay scheme override for ResolveResponsesDialect
	reasoning           convert.ReasoningCapabilities // optional model reasoning capabilities for ResolveResponsesDialect
}

// testForwardSink implements upstream.OutputSink over fakeConvertedPorts to
// capture status codes and response payload bytes during unit tests.
type testForwardSink struct {
	owner *fakeConvertedPorts // owning test port stub
}

// Begin records the committed HTTP status code on the owning test stub.
func (s *testForwardSink) Begin(head upstream.OutputHead) error {
	s.owner.sinkStatus = head.Status
	return nil
}

// Emit appends non-empty output payload bytes onto the owning stub's buffer.
func (s *testForwardSink) Emit(ev upstream.OutputEvent) error {
	if len(ev.Data) > 0 {
		_, _ = s.owner.sinkBuf.Write(ev.Data)
	}
	return nil
}

// PrepareMessages returns the configured Messages profile and dispatch route without error.
func (f *fakeConvertedPorts) PrepareMessages(context.Context) (MessagesProfile, Dispatch, error) {
	return f.msgProfile, f.msgRoute, nil
}

// DispatchMessages records the delegated route and cloned body on f and returns a stub Result.
func (f *fakeConvertedPorts) DispatchMessages(_ context.Context, r Dispatch, body []byte, _, _ string) (*Result, error) {
	f.dispatchedRoute = r
	f.dispatchedBody = bytes.Clone(body)
	return &Result{Model: "delegated"}, nil
}

// PrepareChat returns the configured Chat attempt profile without error.
func (f *fakeConvertedPorts) PrepareChat(context.Context) (ChatProfile, error) {
	return f.chatProfile, nil
}

// DispatchChat records the delegated Chat route and cloned body on f and returns a stub Result.
func (f *fakeConvertedPorts) DispatchChat(_ context.Context, r Dispatch, body []byte, _, _ string) (*Result, error) {
	f.dispatchedRoute = r
	f.dispatchedBody = bytes.Clone(body)
	return &Result{Model: "delegated"}, nil
}

// DefaultChat returns false so Chat tests select Responses conversion unless RawChat is set.
func (f *fakeConvertedPorts) DefaultChat() bool { return false }

// ClientAllowed admits every incoming Chat test request unconditionally.
func (f *fakeConvertedPorts) ClientAllowed(context.Context, []byte) bool { return true }

// PolicyDenied performs no operation in unit test stubs.
func (f *fakeConvertedPorts) PolicyDenied() {}

// Reject records the local HTTP rejection status code onto f.sinkStatus.
func (f *fakeConvertedPorts) Reject(status int, _, _ string) { f.sinkStatus = status }

// SourceError records the local protocol error HTTP status code onto f.sinkStatus.
func (f *fakeConvertedPorts) SourceError(_ protocol.ProtocolID, status int, _, _ string) {
	f.sinkStatus = status
}

// BillingModel returns model when non-empty or falls back to fallback otherwise.
func (f *fakeConvertedPorts) BillingModel(model, fallback string) string {
	if model != "" {
		return model
	}
	return fallback
}

// UpstreamModel maps claude-sonnet-4-5 onto gpt-5.4 and leaves other model names unchanged.
func (f *fakeConvertedPorts) UpstreamModel(model string) string {
	if model == "claude-sonnet-4-5" {
		return "gpt-5.4"
	}
	return model
}

// APIKeyID returns a deterministic synthetic downstream API key identifier.
func (f *fakeConvertedPorts) APIKeyID() int64 { return 7 }

// IsolateCacheKey prefixes key with the synthetic API key namespace for isolation tests.
func (f *fakeConvertedPorts) IsolateCacheKey(key string) string { return "k7:" + key }

// ClaudeSession returns an empty string so metadata or digest cache derivation executes.
func (f *fakeConvertedPorts) ClaudeSession([]byte) string { return "" }

// FindDigestKey returns empty strings so tests exercise fresh digest key derivation.
func (f *fakeConvertedPorts) FindDigestKey(int64, string) (string, string) { return "", "" }

// ContinuationEnabled reports the configured continuation support flag on f.
func (f *fakeConvertedPorts) ContinuationEnabled(string) bool { return f.continuationEnabled }

// ResponseID returns the simulated saved previous_response_id configured on f.
func (f *fakeConvertedPorts) ResponseID(context.Context, string) string { return f.prevResponseID }

// ContinuationDisabled returns false so continuation stays eligible unless cleared on f.
func (f *fakeConvertedPorts) ContinuationDisabled(context.Context, string) bool { return false }

// ResolveResponsesDialect returns an OpenAI API Key dialect supporting functions, hosted web_search, and f.reasoning.
func (f *fakeConvertedPorts) ResolveResponsesDialect(string, []byte) (convert.Dialect, error) {
	scheme := f.replayScheme
	if scheme == "" {
		scheme = "openai"
	}
	return convert.Dialect{
		Tools:               convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch,
		ReplayScheme:        scheme,
		ProviderID:          11,
		OmitStoreAndInclude: scheme == ir.SchemeXAI,
		Reasoning:           f.reasoning,
	}, nil
}

// ConversionRuntime returns the default wall-clock and random ID runtime for test exchanges.
func (f *fakeConvertedPorts) ConversionRuntime() convert.Runtime { return convert.DefaultRuntime() }

// ConvertCursorResponsesToChat marks cursorConverted and converts body into Chat Completions JSON via Exchange.
func (f *fakeConvertedPorts) ConvertCursorResponsesToChat(body []byte) ([]byte, error) {
	f.cursorConverted = true
	ex, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolOpenAIChatCompletions,
			Upstream: protocol.ProtocolOpenAIChatCompletions,
		},
	})
	if err != nil {
		return nil, err
	}
	return ex.UpstreamRequest()
}

// BetaFast returns false so unit tests omit beta priority tier overrides by default.
func (f *fakeConvertedPorts) BetaFast() bool { return false }

// MessagesEffortValue promotes explicit "max" client effort and returns convertedEffort otherwise.
func (f *fakeConvertedPorts) MessagesEffortValue(clientEffort, _, convertedEffort string) string {
	if strings.EqualFold(strings.TrimSpace(clientEffort), "max") {
		return "max"
	}
	return convertedEffort
}

// ToolNameReverse performs no operation because unit test fixtures omit Codex tool aliases.
func (f *fakeConvertedPorts) ToolNameReverse(map[string]string) {}

// ForcedTemplate returns an empty template string to skip forced Codex instruction rendering.
func (f *fakeConvertedPorts) ForcedTemplate() string { return "" }

// ForcedInstructions reports no template change and nil error in unit test stubs.
func (f *fakeConvertedPorts) ForcedInstructions(map[string]any, string, TemplateData) (bool, error) {
	return false, nil
}

// ProviderIdentity performs no operation on decoded Codex maps in unit test stubs.
func (f *fakeConvertedPorts) ProviderIdentity(map[string]any, int64) {}

// TurnState returns an empty turn-state header string in unit test stubs.
func (f *fakeConvertedPorts) TurnState(context.Context, string) string { return "" }

// ApplyEffort returns body unchanged with false and nil error in unit test stubs.
func (f *fakeConvertedPorts) ApplyEffort(_ context.Context, body []byte) ([]byte, bool, error) {
	return body, false, nil
}

// ApplyFast returns body unchanged without blocking fast-tier requests in unit tests.
func (f *fakeConvertedPorts) ApplyFast(_ context.Context, _ string, body []byte) ([]byte, error) {
	return body, nil
}

// ApplyChatFast returns body unchanged without blocking Chat fast-tier requests in unit tests.
func (f *fakeConvertedPorts) ApplyChatFast(_ context.Context, _ string, body []byte) ([]byte, error) {
	return body, nil
}

// ResolvedServiceTier echoes the supplied outbound service tier pointer unchanged.
func (f *fakeConvertedPorts) ResolvedServiceTier(tier *string) *string { return tier }

// GrokCacheIdentity returns key directly as the resolved Grok cache identity in tests.
func (f *fakeConvertedPorts) GrokCacheIdentity(_ []byte, key, _ string) string { return key }

// PatchGrokBody returns body unchanged with nil error in unit test stubs.
func (f *fakeConvertedPorts) PatchGrokBody(body []byte, _ string) ([]byte, error) { return body, nil }

// GrokFreeToolRoute returns body unchanged with nil error in unit test stubs.
func (f *fakeConvertedPorts) GrokFreeToolRoute(body, _ []byte, _ string) ([]byte, error) {
	return body, nil
}

// Credential returns a fixed synthetic bearer token for Messages test requests.
func (f *fakeConvertedPorts) Credential(context.Context) (string, error) { return "tok", nil }

// AccessToken returns a fixed synthetic bearer token for Chat test requests.
func (f *fakeConvertedPorts) AccessToken(context.Context) (string, error) { return "tok", nil }

// BindMessagesBridge performs no operation in unit test stubs.
func (f *fakeConvertedPorts) BindMessagesBridge(bool) {}

// Build constructs a synthetic HTTP POST request carrying body to the test Responses URL.
func (f *fakeConvertedPorts) Build(_ context.Context, ctx context.Context, body []byte, _ string, _ bool, _, _ string) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, http.MethodPost, "http://upstream.test/v1/responses", bytes.NewReader(body))
}

// BuildChat constructs a synthetic HTTP POST request carrying body for Chat bridge tests.
func (f *fakeConvertedPorts) BuildChat(ctx context.Context, body []byte, _, _ string) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, http.MethodPost, "http://upstream.test/v1/responses", bytes.NewReader(body))
}

// IsolatedSessionID prefixes cache with a deterministic session label for header assertions.
func (f *fakeConvertedPorts) IsolatedSessionID(_ int64, cache string) string { return "sess-" + cache }

// UpstreamSessionKey returns key unchanged as the session seed in unit test stubs.
func (f *fakeConvertedPorts) UpstreamSessionKey(_ int64, key string) string { return key }

// Header returns an empty inbound HTTP header value in unit test stubs.
func (f *fakeConvertedPorts) Header(string) string { return "" }

// PrepareTransport performs no proxy setup in unit test stubs.
func (f *fakeConvertedPorts) PrepareTransport() {}

// Send records the outbound request body bytes and returns the next queued HTTP response on f.
func (f *fakeConvertedPorts) Send(r *http.Request) (*http.Response, error) {
	raw, _ := io.ReadAll(r.Body)
	f.sentBodies = append(f.sentBodies, raw)
	resp := f.responses[0]
	f.responses = f.responses[1:]
	return resp, nil
}

// TransportError returns err unchanged to surface transport failures directly in tests.
func (f *fakeConvertedPorts) TransportError(_ context.Context, err error) error { return err }

// ReadErrorBody reads and returns all bytes from r.Body without closing it.
func (f *fakeConvertedPorts) ReadErrorBody(r *http.Response) []byte {
	b, _ := io.ReadAll(r.Body)
	return b
}

// ReadUpstreamError reads all bytes from r.Body and returns both raw bytes and text.
func (f *fakeConvertedPorts) ReadUpstreamError(r *http.Response) ([]byte, string) {
	b, _ := io.ReadAll(r.Body)
	return b, string(b)
}

// IsAgentIdentity returns false so Agent Identity task recovery stays inactive by default.
func (f *fakeConvertedPorts) IsAgentIdentity(context.Context) bool { return false }

// RecoverAgentTask returns a nil error immediately in unit test stubs.
func (f *fakeConvertedPorts) RecoverAgentTask(context.Context) error { return nil }

// RedactErrorBody returns body unchanged without scrubbing bytes in unit test stubs.
func (f *fakeConvertedPorts) RedactErrorBody(_ context.Context, body []byte) []byte { return body }

// DisableContinuation clears prevResponseID on f to simulate session continuation disablement.
func (f *fakeConvertedPorts) DisableContinuation(context.Context, string) { f.prevResponseID = "" }

// DeleteResponseID clears prevResponseID and records continuation deletion on f.
func (f *fakeConvertedPorts) DeleteResponseID(context.Context, string) {
	f.deletedContinuation = true
	f.prevResponseID = ""
}

// FailoverHTTP returns nil so HTTP errors proceed to SourceErrorResponse in tests.
func (f *fakeConvertedPorts) FailoverHTTP(context.Context, *http.Response, []byte, string, string) error {
	return nil
}

// SourceErrorResponse returns io.ErrUnexpectedEOF to represent terminal upstream HTTP errors.
func (f *fakeConvertedPorts) SourceErrorResponse(_ protocol.ProtocolID, _ *http.Response, _ string) (*Result, error) {
	return nil, io.ErrUnexpectedEOF
}

// UpdateGrokUsage performs no operation on Grok response headers in unit test stubs.
func (f *fakeConvertedPorts) UpdateGrokUsage(context.Context, string, http.Header, int) {}

// BindTurnState performs no operation on turn-state headers in unit test stubs.
func (f *fakeConvertedPorts) BindTurnState(context.Context, string, string) {}

// Sink returns a testForwardSink writing status and payload bytes onto f.
func (f *fakeConvertedPorts) Sink() upstream.OutputSink { return &testForwardSink{owner: f} }

// ConvertedResponseOptions returns zero-valued ConvertedResponseOptions for unit test readers.
func (f *fakeConvertedPorts) ConvertedResponseOptions(*http.Response, protocol.ProtocolID, string, string, string, int) openai.ConvertedResponseOptions {
	return openai.ConvertedResponseOptions{}
}

// CyberPolicy returns false so completions proceed unless cyber rejection is tested directly.
func (f *fakeConvertedPorts) CyberPolicy() bool { return false }

// BindResponseID records the completed response ID bound at turn completion onto f.
func (f *fakeConvertedPorts) BindResponseID(_ context.Context, _, id string) { f.boundResponseID = id }

// BindDigestKey records the Anthropic digest prefix chain bound after completion onto f.
func (f *fakeConvertedPorts) BindDigestKey(_ int64, chain, _, _ string) { f.boundDigestChain = chain }

// UpdateCodexUsage performs no operation on Codex quota headers in unit test stubs.
func (f *fakeConvertedPorts) UpdateCodexUsage(context.Context, http.Header) {}

// TestRunConvertedResponses_MessagesRecoversPreviousResponseIDAndBindsDigest
// sends a Messages request with a stale previous_response_id that receives HTTP
// 400 previous_response_not_found on Attempt 1 and succeeds on Attempt 2. The
// runner deletes the stale ID, replays full history with the todo guard, and
// binds the new ResponseID and digest chain.
func TestRunConvertedResponses_MessagesRecoversPreviousResponseIDAndBindsDigest(t *testing.T) {
	okSSE := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_new_2\",\"status\":\"in_progress\",\"model\":\"gpt-5.4-snap\"}}\n\n" +
		"event: response.output_item.done\n" +
		"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"done\"}]}}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_new_2\",\"status\":\"completed\",\"model\":\"gpt-5.4-snap\",\"output\":[],\"usage\":{\"input_tokens\":15,\"output_tokens\":3}}}\n\n"

	ports := &fakeConvertedPorts{
		msgProfile: MessagesProfile{
			Profile:               Profile{Platform: "openai", Type: "apikey"},
			ID:                    11,
			ContinuationSupported: true,
		},
		msgRoute:            DispatchResponses,
		continuationEnabled: true,
		prevResponseID:      "resp_stale_1",
		responses: []*http.Response{
			{
				StatusCode: http.StatusBadRequest,
				Header:     http.Header{},
				Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"previous_response_not_found","message":"Previous response not found"}}`)),
			},
			{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(okSSE)),
			},
		},
	}

	reqBody := []byte(`{` +
		`"model":"claude-sonnet-4-5",` +
		`"max_tokens":256,` +
		`"stream":false,` +
		`"tools":[{"type":"web_search_20250305","name":"web_search"}],` +
		`"messages":[` +
		`{"role":"user","content":"turn-1"},` +
		`{"role":"assistant","content":"reply-1"},` +
		`{"role":"user","content":"turn-2"}` +
		`]` +
		`}`)

	res, err := RunConvertedResponses(t.Context(), protocol.ProtocolAnthropicMessages, reqBody, "", "", false, ports)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.True(t, ports.deletedContinuation)
	require.Len(t, ports.sentBodies, 2)
	require.Contains(t, string(ports.sentBodies[0]), `"previous_response_id":"resp_stale_1"`)
	require.NotContains(t, string(ports.sentBodies[0]), `"turn-1"`, "Attempt 1 continuation must trim to latest turn")
	require.NotContains(t, string(ports.sentBodies[1]), `"previous_response_id"`)
	require.Contains(t, string(ports.sentBodies[1]), `"turn-1"`, "Attempt 2 fallback must replay full history")
	require.Contains(t, string(ports.sentBodies[1]), `"type":"web_search"`)
	require.Equal(t, "resp_new_2", ports.boundResponseID)
	require.NotEmpty(t, ports.boundDigestChain)
	require.Equal(t, "gpt-5.4-snap", res.UpstreamResponseModel)
}

// TestRunConvertedResponses_CursorInputShapedChatHandlesResponsesAndNonResponses
// sends a non-streaming Cursor /v1/chat/completions payload lacking messages to
// both a Responses upstream and a raw Chat upstream. Targeting Responses forces
// outbound stream=true while folding SSE back into Chat JSON, whereas targeting
// raw Chat converts input into Chat messages via Exchange before delegation.
func TestRunConvertedResponses_CursorInputShapedChatHandlesResponsesAndNonResponses(t *testing.T) {
	cursorBody := []byte(`{` +
		`"model":"gpt-5.4",` +
		`"stream":false,` +
		`"prompt_cache_retention":"24h",` +
		`"stream_options":{"include_usage":true},` +
		`"input":[{"type":"message","role":"user","content":"cursor prompt","custom_cursor":9007199254740993}]` +
		`}`)

	okSSE := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_cur\",\"status\":\"in_progress\",\"model\":\"gpt-5.4\"}}\n\n" +
		"event: response.output_item.done\n" +
		"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"cursor reply\"}]}}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_cur\",\"status\":\"completed\",\"model\":\"gpt-5.4\",\"output\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":2}}}\n\n"

	respPorts := &fakeConvertedPorts{
		chatProfile: ChatProfile{
			MessagesProfile:      MessagesProfile{Profile: Profile{Platform: "openai", Type: "apikey"}, ID: 21},
			Protocol:             protocol.ProtocolOpenAIResponses,
			ProtocolSlotDefaults: true,
			OpenAIAPIKey:         true,
		},
		responses: []*http.Response{{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(okSSE)),
		}},
	}
	res1, err1 := RunConvertedResponses(t.Context(), protocol.ProtocolOpenAIChatCompletions, cursorBody, "", "", false, respPorts)
	require.NoError(t, err1)
	require.NotNil(t, res1)
	require.False(t, respPorts.cursorConverted)
	require.Len(t, respPorts.sentBodies, 1)
	sentStr := string(respPorts.sentBodies[0])
	require.Contains(t, sentStr, `"stream":true`)
	require.Contains(t, sentStr, `"custom_cursor":9007199254740993`)
	require.NotContains(t, sentStr, `"prompt_cache_retention"`)
	require.NotContains(t, sentStr, `"stream_options"`)
	require.Contains(t, respPorts.sinkBuf.String(), `"object":"chat.completion"`)
	require.Contains(t, respPorts.sinkBuf.String(), `"cursor reply"`)

	rawChatPorts := &fakeConvertedPorts{
		chatProfile: ChatProfile{
			MessagesProfile: MessagesProfile{Profile: Profile{Platform: "deepseek", Type: "apikey", RawChat: true}, ID: 22},
			Protocol:        protocol.ProtocolOpenAIChatCompletions,
		},
	}
	res2, err2 := RunConvertedResponses(t.Context(), protocol.ProtocolOpenAIChatCompletions, cursorBody, "", "", false, rawChatPorts)
	require.NoError(t, err2)
	require.NotNil(t, res2)
	require.True(t, rawChatPorts.cursorConverted)
	require.Equal(t, DispatchRawChat, rawChatPorts.dispatchedRoute)
	require.Contains(t, string(rawChatPorts.dispatchedBody), `"messages":[`)
	require.Contains(t, string(rawChatPorts.dispatchedBody), `"cursor prompt"`)
}

// TestRunConvertedResponses_MessagesDisabledThinkingPreservesDialectFallback
// sends an Anthropic Messages request carrying both thinking.type="disabled" and
// output_config.effort="max" to RunConvertedResponses. It verifies that
// MessagesEffortValue reconciliation does not overwrite the dialect's "minimal"
// fallback or reintroduce reasoning.summary on GPT-5 targets.
func TestRunConvertedResponses_MessagesDisabledThinkingPreservesDialectFallback(t *testing.T) {
	okSSE := "event: response.created\n" +
		"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_dis\",\"status\":\"in_progress\",\"model\":\"gpt-5\"}}\n\n" +
		"event: response.output_item.done\n" +
		"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_d\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"ok\"}]}}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_dis\",\"status\":\"completed\",\"model\":\"gpt-5\",\"output\":[],\"usage\":{\"input_tokens\":8,\"output_tokens\":1}}}\n\n"

	ports := &fakeConvertedPorts{
		msgProfile: MessagesProfile{Profile: Profile{Platform: "openai", Type: "apikey"}, ID: 31},
		msgRoute:   DispatchResponses,
		reasoning: convert.ReasoningCapabilities{
			ControlsKnown: true,
			Efforts:       []string{"minimal", "low", "medium", "high"},
		},
		responses: []*http.Response{{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(okSSE)),
		}},
	}
	reqBody := []byte(`{"model":"gpt-5","max_tokens":256,"stream":false,"thinking":{"type":"disabled"},"output_config":{"effort":"max"},"messages":[{"role":"user","content":"ping"}]}`)
	res, err := RunConvertedResponses(t.Context(), protocol.ProtocolAnthropicMessages, reqBody, "", "", false, ports)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, ports.sentBodies, 1)
	sent := string(ports.sentBodies[0])
	require.Contains(t, sent, `"effort":"minimal"`)
	require.NotContains(t, sent, `"summary"`)
}

// TestRunConvertedResponses_GrokChatExplicitRouteAndMessagesLosslessRetry
// exercises explicit Grok Responses Chat delegation, Messages-to-Grok HTTP 400
// invalid-ciphertext recovery, and "ultra" effort rejection. It protects Cursor
// input-shaped delegation to DispatchGrok, lossless 64-bit integer preservation
// during jsontext ciphertext stripping, and 400 MODEL_CAPABILITY_UNSUPPORTED errors.
func TestRunConvertedResponses_GrokChatExplicitRouteAndMessagesLosslessRetry(t *testing.T) {
	t.Run("explicit_grok_responses_route_dispatches_cursor_and_developer_chat", func(t *testing.T) {
		ports := &fakeConvertedPorts{
			chatProfile: ChatProfile{
				MessagesProfile: MessagesProfile{Profile: Profile{Platform: "grok", Type: "apikey"}, ID: 41},
				Protocol:        protocol.ProtocolOpenAIResponses,
			},
		}
		cursorReq := []byte(`{"model":"grok-4.5","stream":false,"input":[{"type":"message","role":"user","content":"cursor grok"}]}`)
		res, err := RunConvertedResponses(t.Context(), protocol.ProtocolOpenAIChatCompletions, cursorReq, "", "", false, ports)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, DispatchGrok, ports.dispatchedRoute)
		require.False(t, ports.cursorConverted)
	})

	t.Run("messages_to_grok_strips_invalid_xai_ciphertext_losslessly", func(t *testing.T) {
		xaiCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme:     ir.SchemeXAI,
			ProviderID: 11,
			Data:       "xai_enc_msg_11",
		})
		require.NoError(t, err)

		okSSE := "event: response.created\n" +
			"data: {\"type\":\"response.created\",\"sequence_number\":0,\"response\":{\"id\":\"resp_grok_ok\",\"status\":\"in_progress\",\"model\":\"grok-4.5\"}}\n\n" +
			"event: response.output_item.done\n" +
			"data: {\"type\":\"response.output_item.done\",\"sequence_number\":1,\"output_index\":0,\"item\":{\"id\":\"msg_g\",\"type\":\"message\",\"role\":\"assistant\",\"status\":\"completed\",\"content\":[{\"type\":\"output_text\",\"text\":\"grok-msg-ok\"}]}}\n\n" +
			"event: response.completed\n" +
			"data: {\"type\":\"response.completed\",\"sequence_number\":2,\"response\":{\"id\":\"resp_grok_ok\",\"status\":\"completed\",\"model\":\"grok-4.5\",\"output\":[],\"usage\":{\"input_tokens\":14,\"output_tokens\":3}}}\n\n"

		ports := &fakeConvertedPorts{
			msgProfile: MessagesProfile{
				Profile:   Profile{Platform: "grok", Type: "oauth"},
				ID:        11,
				GrokOAuth: true,
			},
			msgRoute:     DispatchResponses,
			replayScheme: ir.SchemeXAI,
			responses: []*http.Response{
				{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":"invalid_encrypted_content","message":"Failed to decrypt encrypted_content"}}`)),
				},
				{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(okSSE)),
				},
			},
		}

		msgReq := []byte(fmt.Sprintf(`{
			"model":"grok-4.5",
			"max_tokens":256,
			"stream":false,
			"messages":[
				{"role":"user","content":"check id"},
				{"role":"assistant","content":[
					{"type":"thinking","thinking":"","signature":%q},
					{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"big":9007199254740993}}
				]},
				{"role":"user","content":[
					{"type":"tool_result","tool_use_id":"toolu_1","content":"found"}
				]}
			],
			"tools":[{"name":"lookup","input_schema":{"type":"object"}}]
		}`, xaiCarrier))

		res, err := RunConvertedResponses(t.Context(), protocol.ProtocolAnthropicMessages, msgReq, "seed-1", "", false, ports)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, ports.sentBodies, 2)
		require.Contains(t, string(ports.sentBodies[0]), "xai_enc_msg_11")
		require.NotContains(t, string(ports.sentBodies[0]), `"store"`)
		require.NotContains(t, string(ports.sentBodies[0]), `"include"`)
		require.NotContains(t, string(ports.sentBodies[1]), "xai_enc_msg_11")
		require.NotContains(t, string(ports.sentBodies[1]), `"type":"reasoning"`)
		require.Contains(t, string(ports.sentBodies[1]), `\"big\":9007199254740993`)
	})

	t.Run("ultra_effort_returns_400_model_capability_unsupported", func(t *testing.T) {
		ports := &fakeConvertedPorts{
			chatProfile: ChatProfile{
				MessagesProfile: MessagesProfile{Profile: Profile{Platform: "openai", Type: "apikey"}, ID: 42},
				Protocol:        protocol.ProtocolOpenAIResponses,
			},
		}
		_, err := RunConvertedResponses(t.Context(), protocol.ProtocolOpenAIChatCompletions, []byte(`{"model":"gpt-5.4","reasoning_effort":"ultra","messages":[{"role":"user","content":"hi"}]}`), "", "", false, ports)
		require.Equal(t, http.StatusBadRequest, ports.sinkStatus)
		require.Equal(t, "MODEL_CAPABILITY_UNSUPPORTED", apperror.Reason(err))
	})
}

// TestCursorMixedShapeDetection patches the model in a Responses-shaped Chat
// fixture using sjson. The resulting bytes must retain input roles, content,
// extensions, stream choice, service tier, and effort.
func TestCursorMixedShapeDetection(t *testing.T) {
	cursorBody := []byte(`{
		"user": "85df22e7463ab6c2",
		"model": "gpt-5.4",
		"stream": true,
		"input": [
			{"role":"system","content":"You are GPT-5.4 running as a coding agent."},
			{"role":"user","content":"hello"}
		],
		"service_tier": "auto",
		"reasoning": {"effort": "high"}
	}`)

	hasMessages := gjson.GetBytes(cursorBody, "messages").Exists()
	hasInput := gjson.GetBytes(cursorBody, "input").Exists()
	isResponsesShape := !hasMessages && hasInput

	require.True(t, isResponsesShape,
		"Cursor body must be detected as Responses-shape (has input, no messages)")

	const upstreamModel = "gpt-5.1-codex"
	rewritten, err := sjson.SetBytes(cursorBody, "model", upstreamModel)
	require.NoError(t, err)

	assert.Equal(t, upstreamModel, gjson.GetBytes(rewritten, "model").String())

	inputResult := gjson.GetBytes(rewritten, "input")
	require.True(t, inputResult.Exists(), "input field must still exist after rewrite")
	require.True(t, inputResult.IsArray(), "input must still be an array (not null, not object)")

	items := inputResult.Array()
	require.Len(t, items, 2, "both input items must be preserved")
	assert.Equal(t, "system", items[0].Get("role").String())
	assert.Equal(t, "You are GPT-5.4 running as a coding agent.",
		items[0].Get("content").String())
	assert.Equal(t, "user", items[1].Get("role").String())
	assert.Equal(t, "hello", items[1].Get("content").String())

	assert.Equal(t, "85df22e7463ab6c2", gjson.GetBytes(rewritten, "user").String())
	assert.Equal(t, true, gjson.GetBytes(rewritten, "stream").Bool())
	assert.Equal(t, "auto", gjson.GetBytes(rewritten, "service_tier").String())
	assert.Equal(t, "high", gjson.GetBytes(rewritten, "reasoning.effort").String())

	assert.NotContains(t, string(rewritten), `"input":null`,
		"rewritten body must not collapse input to null")
}

// TestCursorMixedShapeDetection_NormalChatCompletionsUnaffected evaluates the
// shape predicate on ordinary Chat messages. A body containing messages must
// remain outside the Responses-input shortcut used by request preparation.
func TestCursorMixedShapeDetection_NormalChatCompletionsUnaffected(t *testing.T) {
	body := []byte(`{
		"model": "gpt-4o",
		"messages": [{"role":"user","content":"hi"}],
		"stream": true
	}`)

	hasMessages := gjson.GetBytes(body, "messages").Exists()
	hasInput := gjson.GetBytes(body, "input").Exists()
	isResponsesShape := !hasMessages && hasInput

	assert.False(t, isResponsesShape,
		"standard Chat Completions body must NOT be detected as Responses-shape")
}

// TestCursorMixedShapeDetection_BothFieldsPrefersMessages evaluates a body with
// both messages and input. The presence of messages must prevent selection of
// the Responses-shaped input shortcut.
func TestCursorMixedShapeDetection_BothFieldsPrefersMessages(t *testing.T) {
	body := []byte(`{
		"model": "gpt-4o",
		"messages": [{"role":"user","content":"hi"}],
		"input": [{"role":"user","content":"other"}]
	}`)

	hasMessages := gjson.GetBytes(body, "messages").Exists()
	hasInput := gjson.GetBytes(body, "input").Exists()
	isResponsesShape := !hasMessages && hasInput

	assert.False(t, isResponsesShape,
		"when both messages and input are present, must not take the Cursor shortcut")
}

// TestCursorMixedShapeDetection_EmptyBody evaluates a body with model and
// stream but no history fields. It must fail the Responses-shaped predicate
// because input is absent.
func TestCursorMixedShapeDetection_EmptyBody(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","stream":true}`)

	hasMessages := gjson.GetBytes(body, "messages").Exists()
	hasInput := gjson.GetBytes(body, "input").Exists()
	isResponsesShape := !hasMessages && hasInput

	assert.False(t, isResponsesShape,
		"body with neither messages nor input must not be taken as Cursor shape")
}

// TestCursorMixedShape_JSONRoundtrip decodes the bytes after a raw Responses
// model patch. The decoded map must retain the replacement model, stream flag,
// and one array input item.
func TestCursorMixedShape_JSONRoundtrip(t *testing.T) {
	cursorBody := []byte(`{"model":"gpt-5.4","stream":true,"input":[{"role":"user","content":"hi"}]}`)

	rewritten, err := sjson.SetBytes(cursorBody, "model", "gpt-5.1-codex")
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(rewritten, &parsed))

	assert.Equal(t, "gpt-5.1-codex", parsed["model"])
	assert.Equal(t, true, parsed["stream"])

	inputArr, ok := parsed["input"].([]any)
	require.True(t, ok, "input must decode to a Go []any after round-trip")
	require.Len(t, inputArr, 1)
}

// TestCursorMixedShape_StripsUnsupportedFields removes every listed extension
// from a complete raw fixture. The required model, stream choice, and user
// input must remain while the incompatible fields disappear.
func TestCursorMixedShape_StripsUnsupportedFields(t *testing.T) {
	cursorBody := []byte(`{
		"model": "gpt-5.4",
		"stream": true,
		"prompt_cache_retention": "24h",
		"safety_identifier": "cursor-user-xyz",
		"metadata": {"trace_id":"abc","caller":"cursor"},
		"stream_options": {"include_usage": true},
		"input": [{"role":"user","content":"hi"}]
	}`)

	for _, field := range CursorResponsesUnsupportedFields {
		require.True(t, gjson.GetBytes(cursorBody, field).Exists(),
			"test fixture must contain %s", field)
	}

	result := cursorBody
	// Optional deletion failures retain the prior bytes, matching the production
	// removal loop; final assertions detect any fixture extension that remained
	// present.
	for _, field := range CursorResponsesUnsupportedFields {
		if stripped, err := sjson.DeleteBytes(result, field); err == nil {
			result = stripped
		}
	}

	for _, field := range CursorResponsesUnsupportedFields {
		assert.False(t, gjson.GetBytes(result, field).Exists(),
			"%s must be stripped", field)
	}

	assert.Equal(t, "gpt-5.4", gjson.GetBytes(result, "model").String())
	assert.Equal(t, true, gjson.GetBytes(result, "stream").Bool())
	assert.True(t, gjson.GetBytes(result, "input").IsArray())
	assert.Equal(t, "user", gjson.GetBytes(result, "input.0.role").String())
}
