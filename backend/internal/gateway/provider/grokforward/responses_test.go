package grokforward

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/grok"
)

// stubGrokPorts records request patching, ToolMap publication, and stream
// output while satisfying Ports for Grok Responses unit tests.
type stubGrokPorts struct {
	sentBody          []byte           // request body captured at Build
	upstreamSSE       string           // scripted SSE response payload
	streamedOut       string           // bytes read by ReadStream after body wrapping
	irToolMap         *convert.ToolMap // published IR ToolMap
	irMapCalls        int              // ClientToolMap call count
	invalidReqMessage string           // captured InvalidRequest message
	invalidReqParam   string           // captured InvalidRequest parameter
}

// BillingModel echoes the caller model name directly as the charge identity for stubbed Forward tests.
func (*stubGrokPorts) BillingModel(m string) string { return m }

// UpstreamModel echoes the billing model name directly as the upstream vendor model for stubbed tests.
func (*stubGrokPorts) UpstreamModel(m string) string { return m }

// InvalidRequest records the rejected parameter name and message reported by Forward during validation failures.
func (s *stubGrokPorts) InvalidRequest(msg, param string) {
	s.invalidReqMessage = msg
	s.invalidReqParam = param
}

// SetError ignores diagnostic upstream status and error message recording during stubbed Forward execution.
func (*stubGrokPorts) SetError(int, string, string) {}

// ClientToolMap counts IR ToolMap publications and stores a detached clone of tm on stubGrokPorts.
func (s *stubGrokPorts) ClientToolMap(tm *convert.ToolMap) {
	s.irMapCalls++
	s.irToolMap = tm.Clone()
}

// CacheIdentity returns a fixed prompt-cache key string for deterministic Grok request construction in tests.
func (*stubGrokPorts) CacheIdentity([]byte, string) string { return "cache-1" }

// FreeCacheRoute returns the prepared request bytes unchanged without injecting companion Grok search tools.
func (*stubGrokPorts) FreeCacheRoute(body, _ []byte, _ string) ([]byte, error) { return body, nil }

// Credential succeeds without external token resolution so Forward proceeds directly to request construction.
func (*stubGrokPorts) Credential(context.Context) error { return nil }

// Detach returns the supplied context alongside a no-op release callback for synchronous test execution.
func (*stubGrokPorts) Detach(ctx context.Context) (context.Context, func()) {
	return ctx, func() {}
}

// ResolveProxy leaves transport proxy configuration unset during in-memory Grok forwarding unit tests.
func (*stubGrokPorts) ResolveProxy() {}

// Build clones the outgoing request payload onto s.sentBody and constructs an HTTP POST request.
func (s *stubGrokPorts) Build(ctx context.Context, body []byte, _ string, _ bool) (*http.Request, error) {
	s.sentBody = bytes.Clone(body)
	return http.NewRequestWithContext(ctx, http.MethodPost, "https://api.x.ai/v1/responses", bytes.NewReader(body))
}

// Do returns a synthetic HTTP 200 event-stream response backed by s.upstreamSSE without network I/O.
func (s *stubGrokPorts) Do(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(s.upstreamSSE)),
	}, nil
}

// ReadError reads and returns all remaining diagnostic bytes from an upstream error response body.
func (*stubGrokPorts) ReadError(resp *http.Response) []byte {
	b, _ := io.ReadAll(resp.Body)
	return b
}

// Latency discards upstream exchange latency measurements during stubbed Grok forwarding unit tests.
func (*stubGrokPorts) Latency(int64) {}

// TransportError returns the supplied transport failure unchanged to the caller for assertion checks.
func (*stubGrokPorts) TransportError(_ context.Context, err error) error { return err }

// ReplayNotice discards provider-local decryption replay notifications during stubbed Grok forwarding tests.
func (*stubGrokPorts) ReplayNotice(bool) {}

// ErrorMessage converts raw upstream error response bytes directly into a diagnostic string value.
func (*stubGrokPorts) ErrorMessage(b []byte) string { return string(b) }

// Health classifies upstream HTTP errors as generic non-failover responses during stubbed Forward tests.
func (*stubGrokPorts) Health(context.Context, int, http.Header, []byte, string, bool) Decision {
	return Decision{Generic: true}
}

// Observe discards structured upstream failure notices emitted by Forward during stubbed unit tests.
func (*stubGrokPorts) Observe(Notice) {}

// HandleError returns nil result and nil error placeholders after generic upstream failure classification.
func (*stubGrokPorts) HandleError(context.Context, *http.Response, []byte, string) (*forwardcore.OpenAIResult, error) {
	return nil, nil
}

// ShouldMarkTeam reports false so stubbed Grok forwarding tests never apply team-model quota blocks.
func (*stubGrokPorts) ShouldMarkTeam(int, []byte) bool { return false }

// MarkTeam is a no-op team-model cooldown hook for stubbed Grok forwarding unit tests.
func (*stubGrokPorts) MarkTeam(string) {}

// RetryMetadata returns zero same-provider retry guidance for stubbed Grok error responses.
func (*stubGrokPorts) RetryMetadata(int, []byte) Retry { return Retry{} }

// Failure returns a sentinel unexpected-EOF error when Forward constructs a provider failover error.
func (*stubGrokPorts) Failure(Failure) error { return io.ErrUnexpectedEOF }

// ObserveSuccess discards successful upstream status and header health observations in unit tests.
func (*stubGrokPorts) ObserveSuccess(context.Context, http.Header, int, string) {}

// ReadStream drains the wrapped SSE response body into s.streamedOut and returns positive token usage.
func (s *stubGrokPorts) ReadStream(_ context.Context, resp *http.Response, _ time.Time, _, _ string) (upstream.ResponsesObservation, error) {
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return upstream.ResponsesObservation{}, err
	}
	s.streamedOut = string(b)
	return upstream.ResponsesObservation{
		ResponseID: "resp_stub",
		Usage:      &wire.ForwardUsage{InputTokens: 3, OutputTokens: 2},
		HasUsage:   true,
		Served:     true,
	}, nil
}

// ReadNonStream drains buffered response bytes from resp.Body and returns synthetic token usage observations.
func (*stubGrokPorts) ReadNonStream(_ context.Context, resp *http.Response, _, _ string) (upstream.ResponsesObservation, error) {
	_, err := io.ReadAll(resp.Body)
	return upstream.ResponsesObservation{
		ResponseID: "resp_stub",
		Usage:      &wire.ForwardUsage{InputTokens: 3, OutputTokens: 2},
		HasUsage:   true,
		Served:     true,
	}, err
}

// Sink returns a nil OutputSink because stubGrokPorts captures stream bytes directly inside ReadStream.
func (*stubGrokPorts) Sink() upstream.OutputSink { return nil }

// Effort returns a nil reasoning-effort pointer for stubbed Grok result projection tests.
func (*stubGrokPorts) Effort([]byte, string) *string { return nil }

// ReadBody reads all bytes from resp.Body for auxiliary Composer image description helpers.
func (*stubGrokPorts) ReadBody(resp *http.Response) ([]byte, error) { return io.ReadAll(resp.Body) }

// HasTokens reports whether the observed usage struct carries a positive input or output token count.
func (*stubGrokPorts) HasTokens(u *wire.ForwardUsage) bool {
	return u != nil && (u.InputTokens+u.OutputTokens) > 0
}

// TestForward_ClientToolLoweringAndStreamRestorer runs Grok Responses streaming
// requests through Forward with custom tools and native replay preparation. It
// verifies PatchGrokResponsesBodyWithToolMap lowering, ToolMap publication, and
// NewResponsesToolStreamBody restoration of custom_tool_call SSE events.
func TestForward_ClientToolLoweringAndStreamRestorer(t *testing.T) {
	sse := strings.Join([]string{
		`: ping`,
		`data: {"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"c1","name":"exec","status":"in_progress"}}`,
		``,
		`data: {"type":"response.function_call_arguments.done","sequence_number":1,"output_index":0,"item_id":"fc_1","name":"exec","arguments":"{\"input\":\"ls\"}"}`,
		``,
		`data: {"type":"response.output_item.done","sequence_number":2,"output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"c1","name":"exec","arguments":"{\"input\":\"ls\"}","status":"completed"}}`,
		``,
		`data: {"type":"response.completed","sequence_number":3,"response":{"id":"resp_stub","status":"completed","output":[{"type":"function_call","id":"fc_1","call_id":"c1","name":"exec","arguments":"{\"input\":\"ls\"}"}]}}`,
		``,
	}, "\n")
	body := []byte(`{"model":"grok-4.5","stream":true,"tools":[{"type":"custom","name":"exec"}],"input":"run ls"}`)
	in := Input{
		ProviderID:    7,
		ProviderName:  "grok",
		ProviderType:  "oauth",
		Platform:      "grok",
		OAuth:         true,
		HTTPPresent:   true,
		Body:          body,
		OriginalModel: "grok-4.5",
		Stream:        true,
		StartedAt:     time.Now(),
	}

	ports := &stubGrokPorts{upstreamSSE: sse}
	var replayCalled bool
	_, err := Forward(context.Background(), ports, Options{
		Codec: grok.BodyCodec{NewID: uuid.NewString},
		ResolveDialect: func(context.Context, string, []byte) convert.Dialect {
			return convert.Dialect{Tools: ir.ToolSupportFunction | ir.ToolSupportHostedWebSearch}
		},
		PrepareNativeReplay: func(_ context.Context, _ string, b []byte) ([]byte, error) {
			replayCalled = true
			return b, nil
		},
	}, in)
	require.NoError(t, err)
	require.True(t, replayCalled)
	require.Equal(t, 2, ports.irMapCalls) // initial clear + published ToolMap
	require.NotNil(t, ports.irToolMap)
	require.True(t, ports.irToolMap.HasRestorations())
	require.Equal(t, "function", gjson.GetBytes(ports.sentBody, "tools.0.type").String())
	require.Contains(t, ports.streamedOut, `"type":"custom_tool_call"`)
	require.Contains(t, ports.streamedOut, `"input":"ls"`)
}
