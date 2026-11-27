package openaiforward

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
)

// anthropic_flows_test.go verifies native Anthropic Messages and OpenAI
// conversion request preparation using production cache-control helpers. Direct
// Messages rewrites existing ephemeral cache markers for Claude targets while
// leaving mapped passback models and uncached OpenAI conversions untouched.

// cacheOutputSink retains serialized native-Anthropic response bytes in memory.
type cacheOutputSink struct {
	bytes.Buffer // captured client response bytes
}

// nativeFlowTestPorts binds production Anthropic cache helpers and in-memory
// exchange responses to native Anthropic flow tests.
type nativeFlowTestPorts struct {
	sink           *cacheOutputSink                     // captured client output writer
	provider       *gatewayprovider.ExecutionProvider   // selected execution provider target
	upstreamModel  string                               // mapped vendor model returned by UpstreamModel
	streamResponse bool                                 // whether SendNative returns SSE instead of JSON
	replayFn       func([]byte, string) ([]byte, error) // optional native replay callback
	builtBodies    [][]byte                             // captured request payloads passed to BuildNative
	cacheModels    []string                             // captured model arguments passed to CacheTTL
}

// Begin accepts output metadata without writing synthetic response content.
func (*cacheOutputSink) Begin(upstream.OutputHead) error { return nil }

// Emit retains every serialized payload for final wire-level usage assertions.
func (s *cacheOutputSink) Emit(event upstream.OutputEvent) error {
	_, err := s.Write(event.Data)
	return err
}

// BillingModel selects fallback when non-empty and otherwise retains model.
// Native flow tests use it to verify charge-model separation before upstream resolution.
func (p *nativeFlowTestPorts) BillingModel(model, fallback string) string {
	if fallback != "" {
		return fallback
	}
	return model
}

// Debug ignores structured flow diagnostics during in-memory unit tests.
func (*nativeFlowTestPorts) Debug(string, ...zap.Field) {}

// ErrorResponse panics when unexpected HTTP error presentation runs in success scenarios.
func (*nativeFlowTestPorts) ErrorResponse(*http.Response, string) (*Result, error) {
	panic("unexpected ErrorResponse call")
}

// FailoverHTTP returns nil because test exchanges return HTTP 200 responses.
func (*nativeFlowTestPorts) FailoverHTTP(context.Context, *http.Response, []byte, string, string) error {
	return nil
}

// PrepareTransport satisfies the proxy-preparation hook before outbound request construction.
func (*nativeFlowTestPorts) PrepareTransport() {}

// ReadUpstreamError panics when unexpected error-body capture runs in success scenarios.
func (*nativeFlowTestPorts) ReadUpstreamError(*http.Response) ([]byte, string) {
	panic("unexpected ReadUpstreamError call")
}

// Sink returns the in-memory test buffer collecting client output.
func (p *nativeFlowTestPorts) Sink() upstream.OutputSink { return p.sink }

// UpstreamModel returns the configured test vendor model when present.
// When upstreamModel is empty, it retains the supplied billing model unchanged.
func (p *nativeFlowTestPorts) UpstreamModel(model string) string {
	if p.upstreamModel != "" {
		return p.upstreamModel
	}
	return model
}

// Profile returns a fixed OpenCode provider profile for flow diagnostics.
func (*nativeFlowTestPorts) Profile() MessagesProfile {
	return MessagesProfile{ID: 7, Profile: Profile{Name: "opencode-claude", Platform: "opencode"}}
}

// Error panics on unexpected local validation failures in valid flow scenarios.
func (*nativeFlowTestPorts) Error(_ int, _, message string) {
	panic("unexpected Error: " + message)
}

// PrepareNativeMessagesReplay invokes replayFn when set, or delegates to
// gatewayprovider.PrepareNativeReplay against p.provider when replayFn is nil.
func (p *nativeFlowTestPorts) PrepareNativeMessagesReplay(ctx context.Context, model string, body []byte) ([]byte, error) {
	if p.replayFn != nil {
		return p.replayFn(body, model)
	}
	out, _, err := gatewayprovider.PrepareNativeReplay(ctx, p.provider, model, body, nil, "anthropic_messages", "anthropic_messages")
	return out, err
}

// PrepareModel returns the supplied Messages body without model-attribute rewrites.
func (*nativeFlowTestPorts) PrepareModel(_ context.Context, body []byte, _ string) ([]byte, error) {
	return body, nil
}

// ThinkingFallback returns the supplied reasoning effort pointer without modification.
func (*nativeFlowTestPorts) ThinkingFallback(effort *string, _ []byte, _ string) *string {
	return effort
}

// StripEmpty delegates production empty-text removal on Anthropic Messages request bytes.
func (*nativeFlowTestPorts) StripEmpty(body []byte) []byte {
	return protocolanthropic.StripEmptyTextBlocks(body)
}

// FilterSearch returns the supplied request bytes unchanged in cache-policy tests.
func (*nativeFlowTestPorts) FilterSearch(body []byte, _ string) []byte { return body }

// CacheLimit delegates production Anthropic cache-control validation and breakpoint limiting.
func (*nativeFlowTestPorts) CacheLimit(body []byte) []byte {
	return anthropic.EnforceCacheControlLimit(body)
}

// CacheTTL records model and delegates production per-Key Claude 1h cache
// eligibility and TTL rewriting. Ineligible platforms or passback models keep body unchanged.
func (p *nativeFlowTestPorts) CacheTTL(ctx context.Context, body []byte, model string) []byte {
	p.cacheModels = append(p.cacheModels, model)
	if !gatewayprovider.ShouldForceClaude1hCache(ctx, p.provider, model) {
		return body
	}
	return anthropic.InjectAnthropicCacheControlTTL1h(body)
}

// Log discards formatted diagnostic messages during native flow unit tests.
func (*nativeFlowTestPorts) Log(string, ...any) {}

// ProtocolAPIKey returns a nonblank test credential for native Anthropic exchange.
func (*nativeFlowTestPorts) ProtocolAPIKey() string { return "sk-opencode-test" }

// TargetURL returns a deterministic OpenCode Messages endpoint URL for tests.
func (*nativeFlowTestPorts) TargetURL() (string, error) {
	return "https://opencode.ai/zen/v1/messages", nil
}

// StreamContext returns ctx unchanged alongside a no-op release callback.
func (*nativeFlowTestPorts) StreamContext(ctx context.Context, _ bool) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}

// BuildNative records a copy of body in builtBodies and constructs an outbound
// POST request bound to ctx. Callers inspect builtBodies for wire-level assertions.
func (p *nativeFlowTestPorts) BuildNative(ctx context.Context, body []byte, _, url string) (*http.Request, error) {
	p.builtBodies = append(p.builtBodies, append([]byte(nil), body...))
	return http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
}

// SendNative returns an in-memory SSE stream when streamResponse is set or a
// buffered Messages JSON response otherwise. Callers close the response body after reading.
func (p *nativeFlowTestPorts) SendNative(*http.Request) (*http.Response, error) {
	if p.streamResponse {
		sse := "event: message_start\ndata: " + `{"type":"message_start","message":{"id":"msg_1","model":"claude-sonnet-4-6","role":"assistant","content":[],"usage":{"input_tokens":10,"output_tokens":0}}}` + "\n\n" +
			"event: content_block_start\ndata: " + `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}` + "\n\n" +
			"event: content_block_delta\ndata: " + `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"ok"}}` + "\n\n" +
			"event: content_block_stop\ndata: " + `{"type":"content_block_stop","index":0}` + "\n\n" +
			"event: message_delta\ndata: " + `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}` + "\n\n" +
			"event: message_stop\ndata: " + `{"type":"message_stop"}` + "\n\n"
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(sse)),
		}, nil
	}
	jsonResp := `{"id":"msg_1","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}`
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(jsonResp)),
	}, nil
}

// TransportErrorNative returns err unchanged so tests observe raw exchange errors.
func (*nativeFlowTestPorts) TransportErrorNative(_ context.Context, err error) error { return err }

// DirectOptions builds buffered native Messages reader callbacks for unit tests.
func (*nativeFlowTestPorts) DirectOptions() NativeAnthropicOptions {
	return NativeAnthropicOptions{
		ProviderID:     7,
		UpdateWindow:   func(context.Context, http.Header) {},
		ReadBody:       io.ReadAll,
		InvalidJSON:    func(_ context.Context, _ *http.Response, _ []byte, err error, _ string) error { return err },
		ForceCache:     func(context.Context) bool { return false },
		CopyHeaders:    func(http.Header, http.Header) {},
		ReverseTools:   func(body []byte) []byte { return body },
		MaxLineSize:    func() int { return 1 << 20 },
		StreamInterval: func() time.Duration { return 0 },
		Log:            func(string, ...any) {},
		HandleTimeout:  func(context.Context, string) {},
	}
}

// TestForwardNativeAnthropicFlows_RewritesExistingMessagesCacheControlWithoutSynthesizingConversions
// runs native Messages, Responses, and Chat flows with the real production
// cache-control helpers. Direct Messages rewrites existing ephemeral markers to
// 1h only for Claude models when enabled, while converted OpenAI requests carry
// no synthesized cache_control markers.
func TestForwardNativeAnthropicFlows_RewritesExistingMessagesCacheControlWithoutSynthesizingConversions(t *testing.T) {
	target := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           7,
			Platform:     capability.PlatformOpenCode,
			Type:         capability.ProviderTypeAPIKey,
		},
	}
	enabledCtx := gatewayprovider.WithAPIKeyForceClaude1hCache(context.Background(), true)

	t.Run("messages_claude_versus_glm_and_disabled", func(t *testing.T) {
		msgBody := []byte(`{
			"model":"claude-sonnet-4-6",
			"max_tokens":64,
			"cache_control":{"type":"ephemeral","ttl":"5m"},
			"system":[
				{"type":"text","text":"cached","cache_control":{"type":"ephemeral","ttl":"5m"}},
				{"type":"text","text":"uncached"}
			],
			"tools":[{"name":"bash","cache_control":{"type":"ephemeral"},"input_schema":{"type":"object"}}],
			"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral","ttl":"5m"}}]}]
		}`)

		claudePorts := &nativeFlowTestPorts{sink: &cacheOutputSink{}, provider: target, upstreamModel: "claude-sonnet-4-6"}
		_, err := ForwardNativeMessages(enabledCtx, msgBody, "", claudePorts)
		require.NoError(t, err)
		require.Equal(t, []string{"claude-sonnet-4-6"}, claudePorts.cacheModels)
		require.Equal(t, "1h", gjson.GetBytes(claudePorts.builtBodies[0], "cache_control.ttl").String())
		require.Equal(t, "1h", gjson.GetBytes(claudePorts.builtBodies[0], "system.0.cache_control.ttl").String())
		require.False(t, gjson.GetBytes(claudePorts.builtBodies[0], "system.1.cache_control").Exists())
		require.Equal(t, "1h", gjson.GetBytes(claudePorts.builtBodies[0], "tools.0.cache_control.ttl").String())
		require.Equal(t, "1h", gjson.GetBytes(claudePorts.builtBodies[0], "messages.0.content.0.cache_control.ttl").String())

		glmPorts := &nativeFlowTestPorts{sink: &cacheOutputSink{}, provider: target, upstreamModel: "glm-5"}
		_, err = ForwardNativeMessages(enabledCtx, msgBody, "", glmPorts)
		require.NoError(t, err)
		require.Equal(t, []string{"glm-5"}, glmPorts.cacheModels)
		require.Equal(t, "5m", gjson.GetBytes(glmPorts.builtBodies[0], "cache_control.ttl").String())
		require.Equal(t, "5m", gjson.GetBytes(glmPorts.builtBodies[0], "system.0.cache_control.ttl").String())
		require.Equal(t, "5m", gjson.GetBytes(glmPorts.builtBodies[0], "messages.0.content.0.cache_control.ttl").String())

		disabledPorts := &nativeFlowTestPorts{sink: &cacheOutputSink{}, provider: target, upstreamModel: "claude-sonnet-4-6"}
		_, err = ForwardNativeMessages(context.Background(), msgBody, "", disabledPorts)
		require.NoError(t, err)
		require.Equal(t, "5m", gjson.GetBytes(disabledPorts.builtBodies[0], "system.0.cache_control.ttl").String())
	})

	t.Run("responses_conversion_leaves_uncached_payload_without_cache_control", func(t *testing.T) {
		respBody := []byte(`{"model":"claude-sonnet-4-6","instructions":"be helpful","input":"hello"}`)
		ports := &fakeConvertedAnthropicPorts{
			profile:       MessagesProfile{Profile: Profile{Platform: "opencode"}, ID: 7},
			upstreamModel: "claude-sonnet-4-6",
			dialect:       convert.Dialect{Tools: convert.ToolSupportFunction, ToolNameMaxBytes: 128},
			force1h:       true,
		}
		_, err := ForwardConvertedAnthropic(enabledCtx, protocol.ProtocolOpenAIResponses, respBody, "", ports)
		require.NoError(t, err)
		require.Len(t, ports.sentBodies, 1)
		require.NotContains(t, string(ports.sentBodies[0]), "cache_control")
	})

	t.Run("chat_conversion_leaves_uncached_payload_without_cache_control", func(t *testing.T) {
		chatBody := []byte(`{"model":"claude-sonnet-4-6","messages":[{"role":"system","content":"be helpful"},{"role":"user","content":"hello"}]}`)
		ports := &fakeConvertedAnthropicPorts{
			profile:       MessagesProfile{Profile: Profile{Platform: "opencode"}, ID: 7},
			upstreamModel: "claude-sonnet-4-6",
			dialect:       convert.Dialect{Tools: convert.ToolSupportFunction, ToolNameMaxBytes: 128},
			force1h:       true,
		}
		_, err := ForwardConvertedAnthropic(enabledCtx, protocol.ProtocolOpenAIChatCompletions, chatBody, "", ports)
		require.NoError(t, err)
		require.Len(t, ports.sentBodies, 1)
		require.NotContains(t, string(ports.sentBodies[0]), "cache_control")
	})
}

// TestForwardNativeMessages_PrepareNativeMessagesReplay exercises compatible
// platform ForwardNativeMessages carrier replay across literal user text,
// OpenCode Claude signature restoration, and Kimi passback unsigned thinking.
// Matching envelopes must unpack cleanly while foreign tokens drop without
// altering literal user text.
func TestForwardNativeMessages_PrepareNativeMessagesReplay(t *testing.T) {
	anthCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "EqQBOpenCodeClaudeSig",
		Text:   "OpenCode Claude signed thought.",
	})
	require.NoError(t, err)
	plainCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemePlain,
		Text:   "Kimi passback thought.",
	})
	require.NoError(t, err)

	body := []byte(`{"model":"claude-sonnet-4-6","max_tokens":128,"nonce":9007199254740993,"messages":[{"role":"user","content":"keep trc1:plain:demo"},{"role":"assistant","content":[{"type":"thinking","thinking":"stub","signature":"` + anthCarrier + `"},{"type":"thinking","thinking":"","signature":"` + plainCarrier + `"},{"type":"text","text":"done"}]}]}`)
	literalOnly := []byte(`{"model":"claude-sonnet-4-6","max_tokens":128,"nonce":9007199254740993,"messages":[{"role":"user","content":"keep trc1:plain:demo"}]}`)

	openCodeTarget := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{LoadLocation: time.LoadLocation, ID: 71, Platform: capability.PlatformOpenCode, Type: capability.ProviderTypeAPIKey},
	}
	kimiTarget := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{LoadLocation: time.LoadLocation, ID: 72, Platform: capability.PlatformKimi, Type: capability.ProviderTypeAPIKey},
	}

	// Literal user "trc1:" without token-path carriers remains byte-for-byte identical.
	litPorts := &nativeFlowTestPorts{
		sink:          &cacheOutputSink{},
		provider:      openCodeTarget,
		upstreamModel: "claude-sonnet-4-6",
	}
	_, err = ForwardNativeMessages(context.Background(), literalOnly, "", litPorts)
	require.NoError(t, err)
	require.Equal(t, literalOnly, litPorts.builtBodies[0])

	// OpenCode Claude with canonical author resolution restores trc1:anthropic and strips trc1:plain.
	claudePorts := &nativeFlowTestPorts{
		sink:          &cacheOutputSink{},
		provider:      openCodeTarget,
		upstreamModel: "claude-sonnet-4-6",
		replayFn: func(b []byte, model string) ([]byte, error) {
			res, rErr := gatewayprovider.ResolveModelAttributes(context.Background(), openCodeTarget, model, nil, "anthropic_messages")
			if rErr != nil {
				return nil, rErr
			}
			res.Target.AttributeModel = "anthropic/claude-sonnet-4-6"
			out, _, rErr := gatewayprovider.PrepareNativeReplayWithResolution(context.Background(), openCodeTarget, res, b, "anthropic_messages", "anthropic_messages")
			return out, rErr
		},
	}
	_, err = ForwardNativeMessages(context.Background(), body, "", claudePorts)
	require.NoError(t, err)
	require.Equal(t, "EqQBOpenCodeClaudeSig", gjson.GetBytes(claudePorts.builtBodies[0], "messages.1.content.0.signature").String())
	require.Equal(t, "OpenCode Claude signed thought.", gjson.GetBytes(claudePorts.builtBodies[0], "messages.1.content.0.thinking").String())
	require.Equal(t, "text", gjson.GetBytes(claudePorts.builtBodies[0], "messages.1.content.1.type").String())
	require.Equal(t, "9007199254740993", gjson.GetBytes(claudePorts.builtBodies[0], "nonce").Raw)

	// Kimi passback retains unsigned thinking text from both trc1:anthropic and trc1:plain.
	kimiPorts := &nativeFlowTestPorts{
		sink:          &cacheOutputSink{},
		provider:      kimiTarget,
		upstreamModel: "kimi-k2.5",
	}
	_, err = ForwardNativeMessages(context.Background(), body, "", kimiPorts)
	require.NoError(t, err)
	require.Equal(t, "OpenCode Claude signed thought.", gjson.GetBytes(kimiPorts.builtBodies[0], "messages.1.content.0.thinking").String())
	require.False(t, gjson.GetBytes(kimiPorts.builtBodies[0], "messages.1.content.0.signature").Exists())
	require.Equal(t, "Kimi passback thought.", gjson.GetBytes(kimiPorts.builtBodies[0], "messages.1.content.1.thinking").String())
	require.False(t, gjson.GetBytes(kimiPorts.builtBodies[0], "messages.1.content.1.signature").Exists())
}
