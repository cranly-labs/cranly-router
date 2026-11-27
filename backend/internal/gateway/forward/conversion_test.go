package forward

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	protocolcore "github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// asConvertedTestPorts implements ConvertedMessagesPorts to record phase order,
// built request bodies, signature-retry telemetry, and response cleanup across
// AsConverted test scenarios.
type asConvertedTestPorts struct {
	mappedModel string          // resolved target model returned by ResolveModel
	resolveErr  error           // optional ResolveModel error simulating Bedrock route rejection
	dialect     convert.Dialect // target IR dialect returned by ResolveDialect
	passback    bool            // whether passback signature stripping applies
	wireSpeed   string          // optional speed field injected during Build
	statuses    []int           // HTTP statuses returned sequentially by Send
	errBody     []byte          // error response payload returned by ReadErrorBody
	readErr     error           // optional error returned alongside errBody by ReadErrorBody
	sigMatch    bool            // whether IsSignatureError matches errBody
	decision    ErrorDecision   // provider health decision for HTTP >= 400
	failoverErr error           // failover sentinel returned by FailoverError

	// events, builtBodies, closeCount, sigRetryCount, readEffort, clientStatus,
	// and clientMsg record callback order, request bodies, closed responses, and
	// final client outcomes across initial and signature-retry dispatches.
	events        []string // chronological callback names
	builtBodies   [][]byte // request bodies passed to Build
	closeCount    int      // number of Response.Close invocations
	sigRetryCount int      // number of ObserveSignatureRetry invocations
	readEffort    *string  // reasoningEffort received by ReadConverted
	clientStatus  int      // HTTP status written by WriteClientError
	clientMsg     string   // error message written by WriteClientError
}

// TestExtractCCReasoningEffortFromBody checks nested and flat Chat effort with
// casing and x-high aliases. The fixtures also retain max for DeepSeek or
// mapped Kimi and return nil when effort is absent.
func TestExtractCCReasoningEffortFromBody(t *testing.T) {
	t.Parallel()

	t.Run("nested reasoning.effort", func(t *testing.T) {
		got := chatEffortFixture([]byte(`{"reasoning":{"effort":"HIGH"}}`))
		require.NotNil(t, got)
		require.Equal(t, "high", *got)
	})

	t.Run("flat reasoning_effort", func(t *testing.T) {
		got := chatEffortFixture([]byte(`{"reasoning_effort":"x-high"}`))
		require.NotNil(t, got)
		require.Equal(t, "xhigh", *got)
	})

	t.Run("DeepSeek V4 preserves max", func(t *testing.T) {
		got := chatEffortFixture([]byte(`{"model":"deepseek-v4-flash","reasoning_effort":"Max"}`))
		require.NotNil(t, got)
		require.Equal(t, "max", *got)
	})

	t.Run("mapped Kimi alias preserves max", func(t *testing.T) {
		got := chatEffortFixture(
			[]byte(`{"model":"public-alias","reasoning_effort":"max"}`),
			"kimi-k3",
			"public-alias",
		)
		require.NotNil(t, got)
		require.Equal(t, "max", *got)
	})

	t.Run("missing effort", func(t *testing.T) {
		require.Nil(t, chatEffortFixture([]byte(`{"model":"gpt-5"}`)))
	})
}

// TestExtractResponsesReasoningEffortFromBody checks nested effort for Claude,
// DeepSeek, and a mapped GLM alias. Casing normalizes, eligible max survives
// model selection, and a request without effort returns nil.
func TestExtractResponsesReasoningEffortFromBody(t *testing.T) {
	t.Parallel()

	got := responsesEffortFixture([]byte(`{"model":"claude-sonnet-4.5","reasoning":{"effort":"HIGH"}}`))
	require.NotNil(t, got)
	require.Equal(t, "high", *got)

	maxGot := responsesEffortFixture([]byte(`{"model":"deepseek-v4-pro","reasoning":{"effort":"max"}}`))
	require.NotNil(t, maxGot)
	require.Equal(t, "max", *maxGot)

	mappedMax := responsesEffortFixture(
		[]byte(`{"model":"public-alias","reasoning":{"effort":"max"}}`),
		"provider/glm-5.2",
		"public-alias",
	)
	require.NotNil(t, mappedMax)
	require.Equal(t, "max", *mappedMax)

	require.Nil(t, responsesEffortFixture([]byte(`{"model":"claude-sonnet-4.5"}`)))
}

// TestAsConverted_SignatureRetryAndOrderInvariants exercises OAuth Mimic ->
// CacheLimit -> Build phase ordering and cross-Claude-model HTTP 400 signature
// stripping with leak-proof response cleanup. It also confirms StripEffort
// billing preservation, wire fast-speed fallback, and early Bedrock routing
// error exits without contacting credentials or transport.
func TestAsConverted_SignatureRetryAndOrderInvariants(t *testing.T) {
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "EqQBSonnetSignature",
		Text:   "Verify sonnet plan before calling tool.",
	})
	require.NoError(t, err)

	reqBody := []byte(`{"model":"gpt-5","stream":true,"reasoning":{"effort":"max"},"input":[{"type":"message","role":"user","content":"start"},{"type":"reasoning","id":"rs_1","summary":[],"encrypted_content":"` + carrier + `"},{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"id\":9007199254740993}"},{"type":"function_call_output","call_id":"call_1","output":"ok"}],"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}]}`)

	ports := &asConvertedTestPorts{
		mappedModel: "claude-opus-4-6",
		dialect: convert.Dialect{
			Tools:            convert.ToolSupportFunction,
			ToolNameMaxBytes: 128,
			ReplayScheme:     ir.SchemeAnthropic,
		},
		wireSpeed: "fast",
		statuses:  []int{400, 200},
		errBody:   []byte(`{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0.thinking.signature: Invalid signature"}}`),
		sigMatch:  true,
	}

	res, err := AsConverted(context.Background(), protocolcore.ProtocolOpenAIResponses, ports, ConversionInput{OAuth: true}, reqBody)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, ports.sigRetryCount)
	require.Equal(t, 2, len(ports.builtBodies))
	require.Equal(t, 2, ports.closeCount, "both initial 400 response and final 200 response must close")
	require.Equal(t, "fast", res.Usage.Speed)
	require.NotNil(t, res.ReasoningEffort)
	require.Equal(t, "max", *res.ReasoningEffort)

	// Attempt 1 carries the unpacked Anthropic thinking block and signature,
	// whereas Attempt 2 strips thinking and removes active-tool-loop top-level
	// thinking while keeping the 64-bit tool input integer literal intact.
	require.Equal(t, "thinking", gjson.GetBytes(ports.builtBodies[0], "messages.1.content.0.type").String())
	require.Equal(t, "EqQBSonnetSignature", gjson.GetBytes(ports.builtBodies[0], "messages.1.content.0.signature").String())
	require.Equal(t, "tool_use", gjson.GetBytes(ports.builtBodies[1], "messages.1.content.0.type").String())
	require.False(t, gjson.GetBytes(ports.builtBodies[1], "thinking").Exists())
	require.Equal(t, "9007199254740993", gjson.GetBytes(ports.builtBodies[1], "messages.1.content.0.input.id").Raw)

	// Verify Mimic -> CacheLimit -> Build ordering ran on both dispatches.
	require.Equal(t, []string{
		"normalize", "resolve_model", "resolve_dialect", "model_notice", "credential",
		"mimic", "cache_limit", "build", "send",
		"read_error_body", "observe_sig_retry", "close",
		"mimic", "cache_limit", "build", "send",
		"read_converted", "close",
	}, ports.events)

	// Verify Bedrock ResolveModel routing errors abort before credentials or Build.
	routeErr := errors.New("bedrock region unsupported")
	errPorts := &asConvertedTestPorts{resolveErr: routeErr}
	nilRes, gotErr := AsConverted(context.Background(), protocolcore.ProtocolOpenAIChatCompletions, errPorts, ConversionInput{Bedrock: true}, []byte(`{"model":"claude-opus-4-6","messages":[{"role":"user","content":"hi"}]}`))
	require.ErrorIs(t, gotErr, routeErr)
	require.Nil(t, nilRes)
	require.Equal(t, []string{"resolve_model"}, errPorts.events)

	// Verify ultra effort and invalid client JSON write HTTP 400 via WriteClientError
	// and return MODEL_CAPABILITY_UNSUPPORTED without contacting credentials or Build.
	ultraPorts := &asConvertedTestPorts{
		mappedModel: "claude-opus-4-6",
		dialect: convert.Dialect{
			Tools:            convert.ToolSupportFunction,
			ToolNameMaxBytes: 128,
		},
	}
	ultraRes, ultraErr := AsConverted(
		context.Background(),
		protocolcore.ProtocolOpenAIResponses,
		ultraPorts,
		ConversionInput{},
		[]byte(`{"model":"gpt-5","reasoning":{"effort":"ultra"},"input":"hi"}`),
	)
	require.Error(t, ultraErr)
	require.Nil(t, ultraRes)
	require.Equal(t, 400, ultraPorts.clientStatus)
	require.Equal(t, "MODEL_CAPABILITY_UNSUPPORTED", apperror.Reason(ultraErr))
	require.Equal(t, []string{"normalize", "resolve_model", "resolve_dialect", "write_client_error"}, ultraPorts.events)
}

// TestAsConverted_HTTPErrorDecisions sends upstream error statuses through
// AsConverted for both OpenAI Responses and Chat Completions clients. It
// protects Generic precedence, failover notices, status mapping, and response closure.
func TestAsConverted_HTTPErrorDecisions(t *testing.T) {
	t.Parallel()

	sources := []struct {
		name   string
		source protocolcore.ProtocolID
		body   []byte
	}{
		{
			name:   "Responses",
			source: protocolcore.ProtocolOpenAIResponses,
			body:   []byte(`{"model":"client-model","input":"hello"}`),
		},
		{
			name:   "Chat",
			source: protocolcore.ProtocolOpenAIChatCompletions,
			body:   []byte(`{"model":"client-model","messages":[{"role":"user","content":"hello"}],"stream":true}`),
		},
	}

	cases := []struct {
		name         string
		status       int
		decision     ErrorDecision
		readErr      error
		clientStatus int
		clientMsg    string
		errText      string
		wantFailover bool
	}{
		{
			name:         "generic_precedes_failover_on_partial_read",
			status:       503,
			decision:     ErrorDecision{Generic: true, Failover: true, RetrySameProvider: true},
			readErr:      io.ErrUnexpectedEOF,
			clientStatus: 500,
			clientMsg:    "Upstream gateway error",
			errText:      "upstream error: 503 (not in custom error codes)",
		},
		{
			name:         "failover_switches_provider_on_partial_read",
			status:       429,
			decision:     ErrorDecision{Failover: true, RetrySameProvider: true},
			readErr:      io.ErrUnexpectedEOF,
			wantFailover: true,
		},
		{
			name:         "server_status_maps_to_502",
			status:       503,
			readErr:      io.ErrUnexpectedEOF,
			clientStatus: 502,
			clientMsg:    "invalid signature in thinking block",
			errText:      "upstream error: 503 invalid signature in thinking block",
		},
		{
			name:         "client_status_is_retained",
			status:       429,
			clientStatus: 429,
			clientMsg:    "invalid signature in thinking block",
			errText:      "upstream error: 429 invalid signature in thinking block",
		},
	}

	for _, src := range sources {
		t.Run(src.name, func(t *testing.T) {
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					failoverSentinel := errors.New("fixture failover sentinel")
					ports := &asConvertedTestPorts{
						mappedModel: "claude-sonnet-4-5",
						dialect: convert.Dialect{
							Tools:            convert.ToolSupportFunction,
							ToolNameMaxBytes: 128,
						},
						statuses:    []int{tc.status},
						errBody:     []byte("available upstream body"),
						readErr:     tc.readErr,
						decision:    tc.decision,
						failoverErr: failoverSentinel,
					}

					res, err := AsConverted(t.Context(), src.source, ports, ConversionInput{}, src.body)
					require.Nil(t, res)
					require.Equal(t, 1, ports.closeCount)
					if tc.wantFailover {
						require.ErrorIs(t, err, failoverSentinel)
						require.Zero(t, ports.clientStatus)
					} else {
						require.EqualError(t, err, tc.errText)
						require.Equal(t, tc.clientStatus, ports.clientStatus)
						require.Equal(t, tc.clientMsg, ports.clientMsg)
					}
				})
			}
		})
	}
}

// chatEffortFixture reads Chat effort through capability-sensitive
// normalization for supplied model candidates. Missing usable effort returns
// nil; candidate precedence and the flat Chat alias follow ExtractEffort rather
// than separate fixture logic.
func chatEffortFixture(body []byte, models ...string) *string {
	return ExtractEffort(body, true, capability.NormalizeRecordedOpenAIEffortForModel, models...)
}

// responsesEffortFixture reads nested Responses effort with
// capability-sensitive model normalization. Supplied candidates follow
// ExtractEffort precedence, and absent or rejected effort returns nil without
// enabling the flat Chat alias.
func responsesEffortFixture(body []byte, models ...string) *string {
	return ExtractEffort(body, false, capability.NormalizeRecordedOpenAIEffortForModel, models...)
}

// NormalizeResponses records legacy-ingress normalization checks without changing bytes.
func (p *asConvertedTestPorts) NormalizeResponses(body []byte) ([]byte, bool, error) {
	p.events = append(p.events, "normalize")
	return body, false, nil
}

// ResolveModel returns p.mappedModel or p.resolveErr while recording phase order.
func (p *asConvertedTestPorts) ResolveModel(_ context.Context, _ string) (string, error) {
	p.events = append(p.events, "resolve_model")
	return p.mappedModel, p.resolveErr
}

// ResolveDialect returns the configured target dialect and passback flag.
func (p *asConvertedTestPorts) ResolveDialect(_ context.Context, _ string, _ []byte) (convert.Dialect, bool, error) {
	p.events = append(p.events, "resolve_dialect")
	return p.dialect, p.passback, nil
}

// ConversionRuntime returns convert.DefaultRuntime for deterministic exchange IDs and clocks.
func (*asConvertedTestPorts) ConversionRuntime() convert.Runtime {
	return convert.DefaultRuntime()
}

// Effort extracts normalized OpenAI effort from client bytes using capability rules.
func (*asConvertedTestPorts) Effort(body []byte, chat bool, models ...string) *string {
	return ExtractEffort(body, chat, capability.NormalizeRecordedOpenAIEffortForModel, models...)
}

// ThinkingFallback returns the supplied effort pointer unchanged in unit fixtures.
func (*asConvertedTestPorts) ThinkingFallback(effort *string, _ []byte, _ string) *string {
	return effort
}

// ModelNotice records model mapping notice emission in p.events.
func (p *asConvertedTestPorts) ModelNotice(_, _, _ string, _ bool) {
	p.events = append(p.events, "model_notice")
}

// Mimic records OAuth preparation in p.events and returns body unchanged.
func (p *asConvertedTestPorts) Mimic(_ context.Context, body []byte, _ json.RawMessage, _ string) []byte {
	p.events = append(p.events, "mimic")
	return body
}

// CacheLimit records Anthropic cache breakpoint enforcement in p.events.
func (p *asConvertedTestPorts) CacheLimit(body []byte) []byte {
	p.events = append(p.events, "cache_limit")
	return body
}

// Credential records provider credential resolution in p.events and returns nil.
func (p *asConvertedTestPorts) Credential(context.Context) error {
	p.events = append(p.events, "credential")
	return nil
}

// Build records the prepared Messages body and optionally injects a wire speed hint.
func (p *asConvertedTestPorts) Build(_ context.Context, body []byte, _ string, _ bool, _ bool) ([]byte, error) {
	p.events = append(p.events, "build")
	cp := append([]byte(nil), body...)
	p.builtBodies = append(p.builtBodies, cp)
	if p.wireSpeed != "" {
		return []byte(`{"speed":"` + p.wireSpeed + `"}`), nil
	}
	return cp, nil
}

// Send returns the next configured HTTP status and tracks response closure.
func (p *asConvertedTestPorts) Send(context.Context) (Response, error) {
	p.events = append(p.events, "send")
	idx := len(p.builtBodies) - 1
	status := 200
	if idx >= 0 && idx < len(p.statuses) {
		status = p.statuses[idx]
	}
	return Response{
		StatusCode: status,
		Close: func() {
			p.events = append(p.events, "close")
			p.closeCount++
		},
	}, nil
}

// ReadErrorBody returns the configured error payload and optional read error.
func (p *asConvertedTestPorts) ReadErrorBody() ([]byte, error) {
	p.events = append(p.events, "read_error_body")
	return p.errBody, p.readErr
}

// ErrorMessage returns a fixed summary extracted from the error payload.
func (p *asConvertedTestPorts) ErrorMessage([]byte) string {
	return "invalid signature in thinking block"
}

// IsSignatureError reports whether the fixture marks the HTTP 400 body as a signature failure.
func (p *asConvertedTestPorts) IsSignatureError(context.Context, []byte) bool {
	return p.sigMatch
}

// ObserveSignatureRetry increments sigRetryCount and records the retry notice.
func (p *asConvertedTestPorts) ObserveSignatureRetry(int, []byte, string) {
	p.sigRetryCount++
	p.events = append(p.events, "observe_sig_retry")
}

// Health returns the configured provider error decision and records health evaluation.
func (p *asConvertedTestPorts) Health(context.Context, int, []byte, string) ErrorDecision {
	p.events = append(p.events, "health")
	return p.decision
}

// FailoverNotice appends failover_notice to p.events for provider switch telemetry checks.
func (p *asConvertedTestPorts) FailoverNotice(int, string) {
	p.events = append(p.events, "failover_notice")
}

// FailoverError records failover_error and returns the configured failover sentinel error.
func (p *asConvertedTestPorts) FailoverError(int, []byte, bool) error {
	p.events = append(p.events, "failover_error")
	return p.failoverErr
}

// WriteClientError records the status and message passed to final client error presentation.
func (p *asConvertedTestPorts) WriteClientError(status int, _, message string) {
	p.events = append(p.events, "write_client_error")
	p.clientStatus = status
	p.clientMsg = message
}

// ReadConverted captures the resolved reasoning effort and returns a minimal Result.
func (p *asConvertedTestPorts) ReadConverted(_ context.Context, _ *convert.Exchange, stream bool, original, mapped string, effort *string, _ time.Time) (*Result, error) {
	p.events = append(p.events, "read_converted")
	p.readEffort = effort
	return &Result{
		Model:           original,
		UpstreamModel:   mapped,
		ReasoningEffort: effort,
		Stream:          stream,
	}, nil
}
