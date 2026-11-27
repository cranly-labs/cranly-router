package openaiforward

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// fakeConvertedRawChatPorts provides controllable in-memory stubs implementing
// ConvertedRawChatPorts for Chat fallback runner unit tests.
type fakeConvertedRawChatPorts struct {
	profile         MessagesProfile     // provider profile snapshot
	dialect         convert.Dialect     // target Chat dialect returned by ResolveChatDialect
	mappedUpstream  string              // model returned by UpstreamModel when non-empty
	preparedCalls   int                 // count of PrepareModel invocations
	grokUsageCalled bool                // true if UpdateGrokUsage was invoked
	cyberMarked     bool                // returned by CyberPolicy
	sentBodies      [][]byte            // outbound Chat request bodies sent via SendCC
	sentStream      []bool              // stream flags passed to SendCC
	observedEndpt   string              // endpoint recorded via Endpoint
	observedModel   string              // upstream model recorded via ObserveModel
	errSource       protocol.ProtocolID // client source passed to SourceError or SourceErrorResponse
	errStatus       int                 // HTTP status passed to SourceError
	failoverErr     error               // optional failover error returned by FailoverHTTP
	resp            *http.Response      // HTTP response returned by SendCC
	sinkBuf         bytes.Buffer        // downstream output bytes captured by Sink
	sinkStatus      int                 // committed HTTP status captured by Sink
}

// rawChatTestSink implements upstream.OutputSink over fakeConvertedRawChatPorts
// to capture status codes and response payload bytes during unit tests.
type rawChatTestSink struct {
	owner *fakeConvertedRawChatPorts // owning test port stub
}

// Begin records the committed HTTP status code onto the owning test stub.
func (s *rawChatTestSink) Begin(head upstream.OutputHead) error {
	s.owner.sinkStatus = head.Status
	return nil
}

// Emit appends non-empty output payload bytes onto the owning test stub buffer.
func (s *rawChatTestSink) Emit(ev upstream.OutputEvent) error {
	if len(ev.Data) > 0 {
		_, _ = s.owner.sinkBuf.Write(ev.Data)
	}
	return nil
}

// Profile returns the configured MessagesProfile snapshot for the attempt.
func (f *fakeConvertedRawChatPorts) Profile() MessagesProfile { return f.profile }

// SourceError records the client source protocol and HTTP status code on f.
func (f *fakeConvertedRawChatPorts) SourceError(source protocol.ProtocolID, status int, _, _ string) {
	f.errSource = source
	f.errStatus = status
}

// BillingModel selects fallback when non-empty and falls back to model when
// no group-mapped override was supplied by the caller. Both string inputs pass
// through unchanged when already canonical in unit tests.
func (f *fakeConvertedRawChatPorts) BillingModel(model, fallback string) string {
	if fallback != "" {
		return fallback
	}
	return model
}

// UpstreamModel returns f.mappedUpstream when configured on the test stub so
// tests can simulate provider-level model remapping. Empty overrides preserve
// the caller-supplied billing model identifier unchanged across requests.
func (f *fakeConvertedRawChatPorts) UpstreamModel(model string) string {
	if f.mappedUpstream != "" {
		return f.mappedUpstream
	}
	return model
}

// ResolveChatDialect returns f.dialect for convert.NewExchange during unit
// tests. Empty ReasoningHistory fields default to HistoryToolTurns so Chat
// tool-call turns retain plaintext reasoning across simulated multi-turn runs.
func (f *fakeConvertedRawChatPorts) ResolveChatDialect(string, []byte) (convert.Dialect, error) {
	d := f.dialect
	if d.ReasoningHistory == "" {
		d.ReasoningHistory = convert.HistoryToolTurns
	}
	return d, nil
}

// ConversionRuntime returns a default Runtime for convert.Exchange test runs.
func (f *fakeConvertedRawChatPorts) ConversionRuntime() convert.Runtime {
	return convert.DefaultRuntime()
}

// MessagesEffortValue preserves explicit Anthropic "max" client effort when
// configured on the stub test request. All other client effort inputs return
// convertedEffort unchanged to mirror production fallback reconciliation.
func (f *fakeConvertedRawChatPorts) MessagesEffortValue(clientEffort, _, convertedEffort string) string {
	if strings.EqualFold(strings.TrimSpace(clientEffort), "max") {
		return "max"
	}
	return convertedEffort
}

// PrepareModel increments f.preparedCalls to record post-encoding model
// attribute preparation during unit tests. Returned byte slices are cloned so
// subsequent caller mutations never alias captured request payloads.
func (f *fakeConvertedRawChatPorts) PrepareModel(_ context.Context, body []byte) ([]byte, error) {
	f.preparedCalls++
	return bytes.Clone(body), nil
}

// ApplyEffort returns body unchanged with changed=false in default unit stubs.
func (f *fakeConvertedRawChatPorts) ApplyEffort(_ context.Context, body []byte) ([]byte, bool, error) {
	return body, false, nil
}

// ApplySourceFast returns body unchanged with a nil error in default unit stubs.
func (f *fakeConvertedRawChatPorts) ApplySourceFast(_ context.Context, _ protocol.ProtocolID, _ string, body []byte) ([]byte, error) {
	return body, nil
}

// Debug discards structured debug log fields during unit tests.
func (f *fakeConvertedRawChatPorts) Debug(string, ...zap.Field) {}

// ObserveModel records the mapped upstream model string onto f.observedModel.
func (f *fakeConvertedRawChatPorts) ObserveModel(m string) { f.observedModel = m }

// Target returns a synthetic bearer key and local Chat Completions endpoint URL.
func (f *fakeConvertedRawChatPorts) Target(context.Context) (string, string, error) {
	return "sk-test", "http://127.0.0.1:8080/v1/chat/completions", nil
}

// Endpoint records the upstream endpoint path string onto f.observedEndpt.
func (f *fakeConvertedRawChatPorts) Endpoint(ep string) { f.observedEndpt = ep }

// SendCC appends an independent copy of body and stream onto the stub's call
// log for outbound request assertions. Callers receive f.resp directly and own
// closing its Body after reading completes.
func (f *fakeConvertedRawChatPorts) SendCC(_ context.Context, _ string, body []byte, stream bool, _ string) (*http.Response, error) {
	f.sentBodies = append(f.sentBodies, bytes.Clone(body))
	f.sentStream = append(f.sentStream, stream)
	return f.resp, nil
}

// ReadUpstreamError consumes and closes resp.Body before installing a fresh
// replayable reader over the captured error bytes. Callers receive both the
// raw error slice and its string representation for classification.
func (f *fakeConvertedRawChatPorts) ReadUpstreamError(resp *http.Response) ([]byte, string) {
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	resp.Body = io.NopCloser(bytes.NewReader(raw))
	return raw, string(raw)
}

// FailoverHTTP returns f.failoverErr to simulate provider health failover classification.
func (f *fakeConvertedRawChatPorts) FailoverHTTP(context.Context, *http.Response, []byte, string, string) error {
	return f.failoverErr
}

// SourceErrorResponse records source on f.errSource and returns a stub Result.
func (f *fakeConvertedRawChatPorts) SourceErrorResponse(_ context.Context, source protocol.ProtocolID, _ *http.Response, _ []byte, model string) (*Result, error) {
	f.errSource = source
	return &Result{BillingModel: model}, nil
}

// UpdateGrokUsage marks f.grokUsageCalled true when invoked on Grok OAuth 2xx replies.
func (f *fakeConvertedRawChatPorts) UpdateGrokUsage(context.Context, string, http.Header, int) {
	f.grokUsageCalled = true
}

// Sink returns a rawChatTestSink writing captured bytes into f.sinkBuf.
func (f *fakeConvertedRawChatPorts) Sink() upstream.OutputSink {
	return &rawChatTestSink{owner: f}
}

// ConvertedRawOptions builds ConvertedRawChatOptions selecting "api_error" for
// Anthropic Messages clients and "upstream_error" for OpenAI Responses clients.
// Supplied serviceTier pointers pass through ResolvedServiceTier so result
// projections retain resolved tier hints.
func (f *fakeConvertedRawChatPorts) ConvertedRawOptions(_ *http.Response, source protocol.ProtocolID, _, _, _ string, serviceTier *string, requestBodyLen int) openai.ConvertedRawChatOptions {
	defaultErr := "upstream_error"
	if source == protocol.ProtocolAnthropicMessages {
		defaultErr = "api_error"
	}
	return openai.ConvertedRawChatOptions{
		RequestBodyLen:      requestBodyLen,
		DefaultErrorType:    defaultErr,
		ResolvedServiceTier: func() *string { return serviceTier },
	}
}

// CyberPolicy reports f.cyberMarked to simulate upstream cyber-policy detection.
func (f *fakeConvertedRawChatPorts) CyberPolicy() bool { return f.cyberMarked }

// TestRunConvertedRawChat_ResponsesRecoversPlainCarrier sends a summary-less
// Responses reasoning item carrying a trc1:plain carrier through
// RunConvertedRawChat. It verifies that reasoning_content reaches the Chat
// upstream, PrepareModel runs after encoding, and Grok prompt_cache_key fields
// are stripped before sending.
func TestRunConvertedRawChat_ResponsesRecoversPlainCarrier(t *testing.T) {
	plainCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemePlain,
		Text:   "Check migration lock order before running DDL.",
	})
	require.NoError(t, err)

	reqBody := `{"model":"gpt-5.4","stream":true,"prompt_cache_key":"pck_1","input":` +
		`[{"type":"message","role":"user","content":"run migration"},` +
		`{"type":"reasoning","id":"rs_99","summary":[],"encrypted_content":"` + plainCarrier + `"},` +
		`{"type":"function_call","id":"fc_1","call_id":"call_1","name":"exec_sql","arguments":"{}"},` +
		`{"type":"function_call_output","call_id":"call_1","output":"ok"}],` +
		`"tools":[{"type":"function","name":"exec_sql","parameters":{"type":"object"}}]}`

	sseReply := "data: {\"id\":\"chatcmpl_1\",\"object\":\"chat.completion.chunk\",\"created\":1760001000,\"model\":\"deepseek-reasoner\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"Migration complete.\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":40,\"completion_tokens\":5,\"total_tokens\":45}}\n\ndata: [DONE]\n\n"
	ports := &fakeConvertedRawChatPorts{
		profile:        MessagesProfile{Profile: Profile{Platform: "grok", Grok: true}, GrokOAuth: true, ID: 7},
		mappedUpstream: "grok-4",
		resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}, "X-Request-Id": []string{"req_ir_1"}},
			Body:       io.NopCloser(strings.NewReader(sseReply)),
		},
	}

	res, err := RunConvertedRawChat(t.Context(), protocol.ProtocolOpenAIResponses, []byte(reqBody), "", ports)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, ports.preparedCalls, "RunConvertedRawChat must call PrepareModel after encoding")
	require.True(t, ports.grokUsageCalled)
	require.Equal(t, RawChatEndpoint, res.UpstreamEndpoint)
	require.Equal(t, "deepseek-reasoner", res.UpstreamResponseModel)
	require.Len(t, ports.sentBodies, 1)

	sentStr := string(ports.sentBodies[0])
	require.Contains(t, sentStr, `"model":"grok-4"`)
	require.Contains(t, sentStr, `"reasoning_content":"Check migration lock order before running DDL."`)
	require.Contains(t, sentStr, `"include_usage":true`)
	require.NotContains(t, sentStr, `"prompt_cache_key"`, "Grok Chat must strip Responses prompt_cache_key")
	require.Contains(t, ports.sinkBuf.String(), "Migration complete.")
}

// TestRunConvertedRawChat_MessagesPreservesPreStrippedEffortAndPreparesModel
// sends an Anthropic Messages request with StripEffort=true through
// RunConvertedRawChat. It verifies post-encoding PrepareModel execution,
// pre-stripped ReasoningEffort retention on Result, and source-protocol HTTP >= 400 error routing.
func TestRunConvertedRawChat_MessagesPreservesPreStrippedEffortAndPreparesModel(t *testing.T) {
	reqBody := `{"model":"claude-sonnet-4-5","max_tokens":512,"stream":false,"output_config":{"effort":"high"},"messages":[{"role":"user","content":"summarize"}]}`
	jsonReply := `{"id":"chatcmpl_m1","object":"chat.completion","created":1760001100,"model":"glm-5","choices":[{"index":0,"message":{"role":"assistant","content":"Summary ready."},"finish_reason":"stop"}],"usage":{"prompt_tokens":18,"completion_tokens":6,"total_tokens":24}}`

	ports := &fakeConvertedRawChatPorts{
		profile:        MessagesProfile{Profile: Profile{Platform: "zhipu", OpenAI: false}, ID: 12},
		dialect:        convert.Dialect{StripEffort: true},
		mappedUpstream: "glm-5-high",
		resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(jsonReply)),
		},
	}

	res, err := RunConvertedRawChat(t.Context(), protocol.ProtocolAnthropicMessages, []byte(reqBody), "claude-sonnet-4-5", ports)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, ports.preparedCalls)
	require.Equal(t, "glm-5", res.UpstreamResponseModel)
	require.NotNil(t, res.ReasoningEffort)
	require.Equal(t, "high", *res.ReasoningEffort)
	require.Len(t, ports.sentBodies, 1)
	require.NotContains(t, string(ports.sentBodies[0]), `"reasoning_effort"`)
	require.Contains(t, ports.sinkBuf.String(), `"Summary ready."`)

	errPorts := &fakeConvertedRawChatPorts{
		profile:        MessagesProfile{Profile: Profile{Platform: "openai", OpenAI: true}, ID: 13},
		mappedUpstream: "gpt-4o",
		resp: &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"invalid parameter"}}`)),
		},
	}
	_, err = RunConvertedRawChat(t.Context(), protocol.ProtocolAnthropicMessages, []byte(reqBody), "claude-sonnet-4-5", errPorts)
	require.NoError(t, err)
	require.Equal(t, protocol.ProtocolAnthropicMessages, errPorts.errSource)

	disabledMaxReq := `{"model":"claude-sonnet-4-5","max_tokens":512,"stream":false,"thinking":{"type":"disabled"},"output_config":{"effort":"max"},"messages":[{"role":"user","content":"ping"}]}`
	minPorts := &fakeConvertedRawChatPorts{
		profile: MessagesProfile{Profile: Profile{Platform: "openai", OpenAI: true}, ID: 14},
		dialect: convert.Dialect{
			Reasoning: convert.ReasoningCapabilities{
				ControlsKnown: true,
				Efforts:       []string{"minimal", "low", "medium", "high"},
			},
		},
		mappedUpstream: "gpt-5",
		resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(jsonReply)),
		},
	}
	_, err = RunConvertedRawChat(t.Context(), protocol.ProtocolAnthropicMessages, []byte(disabledMaxReq), "claude-sonnet-4-5", minPorts)
	require.NoError(t, err)
	require.Len(t, minPorts.sentBodies, 1)
	require.Contains(t, string(minPorts.sentBodies[0]), `"reasoning_effort":"minimal"`)
}
