package openaiforward

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	anthropicupstream "github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
)

// fakeConvertedAnthropicPorts implements ConvertedAnthropicPorts to verify
// ForwardConvertedAnthropic phase ordering, source-protocol dispatch, and HTTP
// 400 signature-stripping retries in unit tests.
type fakeConvertedAnthropicPorts struct {
	profile       MessagesProfile            // provider profile snapshot
	upstreamModel string                     // mapped upstream model returned by UpstreamModel
	dialect       convert.Dialect            // target dialect returned by ResolveAnthropicDialect
	passback      bool                       // whether passback signature stripping applies
	force1h       bool                       // whether CacheTTL rewrites ephemeral markers to 1h
	responses     []*http.Response           // HTTP responses returned sequentially by SendNative
	phases        []string                   // chronological preparation and send phase labels
	sentBodies    [][]byte                   // outbound Messages request bodies passed to SendNative
	sigRetries    int                        // count of ObserveSignatureRetry invocations
	errSource     protocol.ProtocolID        // client source passed to SourceError
	errStatus     int                        // HTTP status passed to SourceError
	sink          anthropicConvertedTestSink // recorded output sink bytes and status
}

// Profile returns the configured MessagesProfile snapshot stored on f.
func (f *fakeConvertedAnthropicPorts) Profile() MessagesProfile { return f.profile }

// BillingModel selects fallback when non-empty and model otherwise.
func (*fakeConvertedAnthropicPorts) BillingModel(model, fallback string) string {
	if fallback != "" {
		return fallback
	}
	return model
}

// UpstreamModel returns f.upstreamModel when set or model otherwise.
func (f *fakeConvertedAnthropicPorts) UpstreamModel(model string) string {
	if f.upstreamModel != "" {
		return f.upstreamModel
	}
	return model
}

// ResolveAnthropicDialect returns the configured dialect and passback flag.
func (f *fakeConvertedAnthropicPorts) ResolveAnthropicDialect(context.Context, string, []byte) (convert.Dialect, bool, error) {
	return f.dialect, f.passback, nil
}

// ConversionRuntime returns convert.DefaultRuntime for deterministic exchange IDs and clocks.
func (*fakeConvertedAnthropicPorts) ConversionRuntime() convert.Runtime {
	return convert.DefaultRuntime()
}

// SourceError records the client source protocol and HTTP status code on f.
func (f *fakeConvertedAnthropicPorts) SourceError(source protocol.ProtocolID, status int, _, _ string) {
	f.errSource = source
	f.errStatus = status
}

// Debug ignores structured log entries during unit tests.
func (*fakeConvertedAnthropicPorts) Debug(string, ...zap.Field) {}

// Mimic records OAuth preparation and adds a 5m ephemeral cache breakpoint on tools.
func (f *fakeConvertedAnthropicPorts) Mimic(_ context.Context, body []byte, _ json.RawMessage, _ string) []byte {
	f.phases = append(f.phases, "mimic")
	return anthropicupstream.ApplyToolsLastCacheBreakpoint(body)
}

// FilterSearch records search-history filtering without altering clean bytes.
func (f *fakeConvertedAnthropicPorts) FilterSearch(body []byte, _ string) []byte {
	f.phases = append(f.phases, "filter_search")
	return body
}

// CacheLimit records Anthropic cache breakpoint enforcement and runs EnforceCacheControlLimit.
func (f *fakeConvertedAnthropicPorts) CacheLimit(body []byte) []byte {
	f.phases = append(f.phases, "cache_limit")
	return anthropicupstream.EnforceCacheControlLimit(body)
}

// PrepareModel records model-attribute preparation in f.phases and returns body unchanged.
func (f *fakeConvertedAnthropicPorts) PrepareModel(_ context.Context, body []byte, _ string) ([]byte, error) {
	f.phases = append(f.phases, "prepare_model")
	return body, nil
}

// CacheTTL records 1h cache TTL evaluation and injects 1h when f.force1h is set.
func (f *fakeConvertedAnthropicPorts) CacheTTL(_ context.Context, body []byte, _ string) []byte {
	f.phases = append(f.phases, "cache_ttl")
	if f.force1h {
		return anthropicupstream.InjectAnthropicCacheControlTTL1h(body)
	}
	return body
}

// ProtocolAPIKey returns a synthetic non-empty API credential for test dispatch.
func (*fakeConvertedAnthropicPorts) ProtocolAPIKey() string { return "sk-compat-test" }

// TargetURL returns a local synthetic Anthropic Messages URL.
func (*fakeConvertedAnthropicPorts) TargetURL() (string, error) {
	return "http://127.0.0.1:18080/v1/messages", nil
}

// PrepareTransport accepts proxy preparation as a no-op in unit tests.
func (*fakeConvertedAnthropicPorts) PrepareTransport() {}

// StreamContext returns ctx with a no-op cancel callback.
func (*fakeConvertedAnthropicPorts) StreamContext(ctx context.Context, _ bool) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}

// BuildConvertedNative constructs a POST request over body and returns body as wire bytes.
func (*fakeConvertedAnthropicPorts) BuildConvertedNative(ctx context.Context, body []byte, _, url string) (*http.Request, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	return req, body, err
}

// SendNative records the request payload and returns the next configured HTTP response.
func (f *fakeConvertedAnthropicPorts) SendNative(req *http.Request) (*http.Response, error) {
	f.phases = append(f.phases, "send")
	raw, _ := io.ReadAll(req.Body)
	f.sentBodies = append(f.sentBodies, raw)
	idx := len(f.sentBodies) - 1
	if idx < len(f.responses) {
		return f.responses[idx], nil
	}
	sse := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_ok\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-opus-4-6\",\"content\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":1}}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"done\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"req_ok"}},
		Body:       io.NopCloser(strings.NewReader(sse)),
	}, nil
}

// TransportErrorNative passes network errors through unchanged in unit tests.
func (*fakeConvertedAnthropicPorts) TransportErrorNative(_ context.Context, err error) error {
	return err
}

// ReadUpstreamError captures resp.Body, reinstalls a replayable reader, and extracts message text.
func (*fakeConvertedAnthropicPorts) ReadUpstreamError(resp *http.Response) ([]byte, string) {
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, upstream.ExtractErrorMessage(raw)
}

// IsSignatureError delegates signature matching to anthropicupstream.IsThinkingBlockSignatureError on extracted text.
func (*fakeConvertedAnthropicPorts) IsSignatureError(_ context.Context, body []byte) bool {
	matched, _ := anthropicupstream.IsThinkingBlockSignatureError(upstream.ExtractErrorMessage(body))
	return matched
}

// ObserveSignatureRetry increments sigRetries and records the retry marker.
func (f *fakeConvertedAnthropicPorts) ObserveSignatureRetry(*http.Response, []byte, string) {
	f.sigRetries++
	f.phases = append(f.phases, "observe_sig_retry")
}

// FailoverHTTP declines failover in these single-provider unit tests.
func (*fakeConvertedAnthropicPorts) FailoverHTTP(context.Context, *http.Response, []byte, string, string) error {
	return nil
}

// MapStatus delegates upstream HTTP status mapping to forwardcore.MapStatus.
func (*fakeConvertedAnthropicPorts) MapStatus(status int) int {
	return forwardcore.MapStatus(status)
}

// ResponsesEffort extracts normalized OpenAI Responses effort from raw client bytes.
func (*fakeConvertedAnthropicPorts) ResponsesEffort(body []byte, models ...string) *string {
	return forwardcore.ExtractEffort(body, false, capability.NormalizeRecordedOpenAIEffortForModel, models...)
}

// ChatEffort extracts normalized OpenAI Chat effort from raw client bytes.
func (*fakeConvertedAnthropicPorts) ChatEffort(body []byte, models ...string) *string {
	return forwardcore.ExtractEffort(body, true, capability.NormalizeRecordedOpenAIEffortForModel, models...)
}

// ThinkingFallback returns the supplied effort pointer unchanged during unit tests.
func (*fakeConvertedAnthropicPorts) ThinkingFallback(effort *string, _ []byte, _ string) *string {
	return effort
}

// Sink returns the in-memory test OutputSink on f.
func (f *fakeConvertedAnthropicPorts) Sink() upstream.OutputSink { return &f.sink }

// ConvertedOutputOptions returns minimal ConvertedAnthropicOutputOptions configured for unit tests.
func (*fakeConvertedAnthropicPorts) ConvertedOutputOptions(*http.Response, protocol.ProtocolID, string, string, string, int) ConvertedAnthropicOutputOptions {
	return ConvertedAnthropicOutputOptions{DefaultErrorType: "server_error"}
}

// TestForwardConvertedAnthropic_OrderCacheTTLSigRetryAndCarrierDispatch
// exercises ForwardConvertedAnthropic across Responses and Chat inputs
// alongside strict Mimic -> CacheLimit -> PrepareModel -> CacheTTL ordering.
// It also confirms cross-Claude-model signature 400 lossless stripping retries
// and DropTemperature dialect rules on compatible passback endpoints.
func TestForwardConvertedAnthropic_OrderCacheTTLSigRetryAndCarrierDispatch(t *testing.T) {
	t.Run("oauth_mimic_cache_limit_prepare_model_1h_ttl_and_signature_400_retry", func(t *testing.T) {
		carrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "EqQBSonnetSig",
			Text:   "Check inventory before responding.",
		})
		require.NoError(t, err)

		reqBody := []byte(`{"model":"gpt-5","stream":false,"reasoning":{"effort":"high"},"input":[{"type":"message","role":"user","content":"check"},{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"` + carrier + `"},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"id\":9007199254740993}"},{"type":"function_call_output","call_id":"call_1","output":"ok"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}]}`)

		sig400 := &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0.thinking.signature: Invalid 'signature' in 'thinking' block"}}`)),
		}

		ports := &fakeConvertedAnthropicPorts{
			profile:       MessagesProfile{Profile: Profile{Platform: "opencode", OAuth: true}, ID: 91},
			upstreamModel: "claude-opus-4-6",
			dialect: convert.Dialect{
				Tools:            convert.ToolSupportFunction,
				ToolNameMaxBytes: 128,
				ReplayScheme:     ir.SchemeAnthropic,
				DropTemperature:  true,
			},
			force1h:   true,
			responses: []*http.Response{sig400},
		}

		res, err := ForwardConvertedAnthropic(context.Background(), protocol.ProtocolOpenAIResponses, reqBody, "", ports)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, 1, ports.sigRetries)
		require.Len(t, ports.sentBodies, 2)

		// Verify strict Mimic -> CacheLimit -> PrepareModel -> CacheTTL order on both dispatches.
		require.Equal(t, []string{
			"mimic", "filter_search", "cache_limit", "prepare_model", "cache_ttl", "send",
			"observe_sig_retry",
			"mimic", "filter_search", "cache_limit", "prepare_model", "cache_ttl", "send",
		}, ports.phases)

		// Both dispatches carry the 1h TTL injected onto Mimic's tools breakpoint;
		// Attempt 1 carries the signature while Attempt 2 strips thinking cleanly.
		require.Equal(t, "1h", gjson.GetBytes(ports.sentBodies[0], "tools.0.cache_control.ttl").String())
		require.Equal(t, "EqQBSonnetSig", gjson.GetBytes(ports.sentBodies[0], "messages.1.content.0.signature").String())
		require.Equal(t, "1h", gjson.GetBytes(ports.sentBodies[1], "tools.0.cache_control.ttl").String())
		require.Equal(t, "tool_use", gjson.GetBytes(ports.sentBodies[1], "messages.1.content.0.type").String())
		require.Equal(t, "9007199254740993", gjson.GetBytes(ports.sentBodies[1], "messages.1.content.0.input.id").Raw)
		require.Equal(t, "claude-opus-4-6", res.UpstreamResponseModel)
		require.Equal(t, "gpt-5", gjson.GetBytes(ports.sink.buf.Bytes(), "model").String())
	})

	t.Run("chat_carrier_dispatch_and_passback_signature_strip", func(t *testing.T) {
		chatBody := []byte(`{"model":"gpt-4o","stream":false,"temperature":0.7,"messages":[{"role":"user","content":"hello"}]}`)
		ports := &fakeConvertedAnthropicPorts{
			profile:       MessagesProfile{Profile: Profile{Platform: "deepseek"}, ID: 92},
			upstreamModel: "deepseek-reasoner",
			dialect: convert.Dialect{
				Tools:            convert.ToolSupportFunction,
				ToolNameMaxBytes: 128,
				ReasoningHistory: convert.HistoryToolTurns,
				DropTemperature:  true,
			},
			passback: true,
		}

		res, err := ForwardConvertedAnthropic(context.Background(), protocol.ProtocolOpenAIChatCompletions, chatBody, "", ports)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, ports.sentBodies, 1)
		require.False(t, gjson.GetBytes(ports.sentBodies[0], "temperature").Exists(), "DropTemperature dialect rule must strip temperature")
		require.Equal(t, "gpt-4o", gjson.GetBytes(ports.sink.buf.Bytes(), "model").String())
		require.Equal(t, "done", gjson.GetBytes(ports.sink.buf.Bytes(), "choices.0.message.content").String())
	})
}
