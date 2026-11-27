package ws

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
)

// streamReplayTestLease records pooled WebSocket request writes and replays
// scripted upstream event frames for RelayTurn unit tests.
type streamReplayTestLease struct {
	written [][]byte // outbound request payloads passed to WriteRequest
	events  [][]byte // scripted upstream event frames returned by ReadEvent
	idx     int      // next unread event index in events
	broken  bool     // whether MarkBroken was invoked on this lease
}

// streamReplayNoopImages satisfies ImageCounter when pooled turns produce no images.
type streamReplayNoopImages struct{}

// streamReplayNoopCollector satisfies ReplayCollector when tests inspect outbound requests.
type streamReplayNoopCollector struct{}

// streamReplayTestPort stubs StreamPort and nativeReplayDialectResolver so
// RelayTurn unit tests verify outbound carrier preparation and digest lineage.
type streamReplayTestPort struct {
	dialect         convert.Dialect // target dialect returned by ResolveNativeReplayDialect
	observedDigests [][]byte        // payloads inspected by EncryptedDigests on invalid_encrypted_content
	markedDigests   []string        // digests passed to MarkEncrypted
	clientWrites    [][]byte        // downstream frames delivered to WriteClient
}

// ConnID returns a stable pooled connection identifier for diagnostic formatting.
func (*streamReplayTestLease) ConnID() string { return "pool_conn_1" }

// Reused reports false so RelayTurn treats each test lease as newly established.
func (*streamReplayTestLease) Reused() bool { return false }

// IdleDuration returns zero idle age for freshly acquired unit-test leases.
func (*streamReplayTestLease) IdleDuration() time.Duration { return 0 }

// PingHealthy returns nil because preflight ping checks run outside RelayTurn.
func (*streamReplayTestLease) PingHealthy(context.Context, time.Duration) error { return nil }

// MarkBroken sets l.broken so tests can observe when RelayTurn invalidates a
// pooled connection. Repeated calls leave the broken marker set without
// affecting recorded request or event slices.
func (l *streamReplayTestLease) MarkBroken() { l.broken = true }

// Release performs no external cleanup on in-memory unit-test leases.
func (*streamReplayTestLease) Release() {}

// SupportsIdlePingWithoutReader reports false because idle pings are unused in unit tests.
func (*streamReplayTestLease) SupportsIdlePingWithoutReader() bool { return false }

// PingWithTimeout returns nil immediately on in-memory unit-test connection leases.
func (*streamReplayTestLease) PingWithTimeout(time.Duration) error { return nil }

// WriteRequest clones body into l.written and returns nil to simulate a
// completed upstream socket send. Cloning preserves exact wire bytes even if the
// caller mutates its local slice afterward.
func (l *streamReplayTestLease) WriteRequest(_ context.Context, body []byte, _ time.Duration) error {
	l.written = append(l.written, append([]byte(nil), body...))
	return nil
}

// ReadEvent advances l.idx and returns the next scripted upstream frame for
// RelayTurn consumption. Once all scripted events are consumed, subsequent reads
// return io.EOF so unexpected missing-terminal loops surface immediately.
func (l *streamReplayTestLease) ReadEvent(context.Context, time.Duration) ([]byte, error) {
	if l.idx >= len(l.events) {
		return nil, io.EOF
	}
	ev := l.events[l.idx]
	l.idx++
	return ev, nil
}

// Headers returns empty handshake headers for unit-test leases.
func (*streamReplayTestLease) Headers() map[string][]string { return nil }

// AddSSEData ignores SSE events when no generated images are present.
func (streamReplayNoopImages) AddSSEData([]byte) {}

// Count returns zero generated images for text-only pooled turns.
func (streamReplayNoopImages) Count() int { return 0 }

// FrameCount returns zero animation frame units for text-only pooled turns.
func (streamReplayNoopImages) FrameCount() int { return 0 }

// Sizes returns nil output image size labels for text-only pooled turns.
func (streamReplayNoopImages) Sizes() []string { return nil }

// AddEvent ignores output items in outbound-request unit tests.
func (streamReplayNoopCollector) AddEvent(string, []byte) {}

// Items returns nil retained replay items when continuation history is unused.
func (streamReplayNoopCollector) Items() []json.RawMessage { return nil }

// ResolveNativeReplayDialect returns the configured convert.Dialect for pooled WS replay tests.
func (p *streamReplayTestPort) ResolveNativeReplayDialect(context.Context, string, []byte) (convert.Dialect, error) {
	return p.dialect, nil
}

// BeginObservation resets per-turn model and service-tier observation state before execution.
func (*streamReplayTestPort) BeginObservation() {}

// ObserveModel ignores upstream model metadata in outbound-replay unit tests.
func (*streamReplayTestPort) ObserveModel([]byte, string) {}

// ResponseTier returns an empty upstream service tier when none was observed.
func (*streamReplayTestPort) ResponseTier() string { return "" }

// ResolvedTier returns nil resolved service tier when request tiers are absent.
func (*streamReplayTestPort) ResolvedTier([]byte) *string { return nil }

// Reasoning returns nil recorded effort attribution for unit-test turns.
func (*streamReplayTestPort) Reasoning([]byte, string, string) *string { return nil }

// ImageCounter returns a no-op image collector for text-only pooled turns.
func (*streamReplayTestPort) ImageCounter() ImageCounter { return streamReplayNoopImages{} }

// ReplayCollector returns a no-op replay item collector for single-turn tests.
func (*streamReplayTestPort) ReplayCollector() ReplayCollector { return streamReplayNoopCollector{} }

// Streaming reports true so RelayTurn records standard streaming WebSocket metadata.
func (*streamReplayTestPort) Streaming([]byte) bool { return true }

// StoreDisabled reports whether body explicitly sets top-level store to boolean
// false. Omitted, null, or true store values return false so RelayTurn keeps
// standard stored-session continuation behavior during pooled execution.
func (*streamReplayTestPort) StoreDisabled(body []byte) bool {
	res := gjson.GetBytes(body, "store")
	return res.Exists() && res.Type == gjson.False
}

// ClassifyPrevious returns an empty kind when previous_response_id is absent.
func (*streamReplayTestPort) ClassifyPrevious(string) string { return "" }

// HasToolOutput reports false for turns without function_call_output items.
func (*streamReplayTestPort) HasToolOutput([]byte) bool { return false }

// MappedModel echoes routingModel unchanged when no provider alias applies.
func (*streamReplayTestPort) MappedModel(routingModel string) string { return routingModel }

// NormalizeCompleted returns body unchanged with false when terminal frames need no repair.
func (*streamReplayTestPort) NormalizeCompleted(body []byte) ([]byte, bool) { return body, false }

// Envelope extracts top-level event type and nested response.id strings from
// body for turn correlation. It always reports false for the auxiliary third
// flag because RelayTurn ignores adapter-specific envelope markers.
func (*streamReplayTestPort) Envelope(body []byte) (string, string, bool) {
	return gjson.GetBytes(body, "type").String(), gjson.GetBytes(body, "response.id").String(), false
}

// ShouldParseUsage reports whether event equals response.completed so RelayTurn
// updates turn token counters only on terminal completion frames. Other event
// labels return false and leave cumulative turn usage untouched.
func (*streamReplayTestPort) ShouldParseUsage(event string) bool {
	return event == "response.completed"
}

// ParseUsage writes fixed positive input and output token counts into u when
// RelayTurn observes a terminal usage frame. Callers pass a non-nil ForwardUsage
// pointer owned by the active turn.
func (*streamReplayTestPort) ParseUsage(_ []byte, u *wire.ForwardUsage) {
	u.InputTokens = 5
	u.OutputTokens = 2
}

// MarkCyber performs no risk updates in unit tests.
func (*streamReplayTestPort) MarkCyber([]byte, *wire.ForwardUsage) {}

// SchedulingModel echoes model unchanged as the canonical health scheduling key.
func (*streamReplayTestPort) SchedulingModel(model string) string { return model }

// ErrorDecision requests pre-output failover on bare error events.
func (*streamReplayTestPort) ErrorDecision(context.Context, string, map[string][]string, []byte) ErrorPolicy {
	return ErrorPolicy{Failover: true}
}

// TerminalDecision extracts the top-level event type from body and returns an
// HTTP 200 TerminalPolicy. Zero ErrorPolicy flags let RelayTurn finish normal
// response.completed delivery without generic error replacement.
func (*streamReplayTestPort) TerminalDecision(_ context.Context, _ string, _ map[string][]string, body []byte) TerminalPolicy {
	return TerminalPolicy{TerminalEvent: gjson.GetBytes(body, "type").String(), StatusCode: 200}
}

// ErrorFields extracts error.code, error.type, and error.message strings from
// an upstream error event payload. Absent or non-string fields return empty
// strings for downstream recovery classification and diagnostic formatting.
func (*streamReplayTestPort) ErrorFields(body []byte) (string, string, string) {
	return gjson.GetBytes(body, "error.code").String(),
		gjson.GetBytes(body, "error.type").String(),
		gjson.GetBytes(body, "error.message").String()
}

// ErrorStatus returns HTTP status 400 whenever body contains a top-level error
// object. Frames without an error member return zero so RelayTurn skips all
// HTTP error-status policy branches.
func (*streamReplayTestPort) ErrorStatus(body []byte) int {
	if gjson.GetBytes(body, "error").Exists() {
		return 400
	}
	return 0
}

// ClassifyError maps upstream error code invalid_encrypted_content to its
// matching recovery reason and true. All other codes return an empty reason and
// false so RelayTurn skips ciphertext lineage marking.
func (*streamReplayTestPort) ClassifyError(code, _, _ string) (string, bool) {
	if code == "invalid_encrypted_content" {
		return "invalid_encrypted_content", true
	}
	return "", false
}

// EncryptedDigests clones body into p.observedDigests and collects every
// non-empty input[*].encrypted_content string in encounter order. Recording the
// inspected slice lets unit tests verify RelayTurn passes post-replay outbound
// bytes rather than pre-replay ClientPayload bytes.
func (p *streamReplayTestPort) EncryptedDigests(body []byte) []string {
	p.observedDigests = append(p.observedDigests, append([]byte(nil), body...))
	var out []string
	for _, item := range gjson.GetBytes(body, "input").Array() {
		if enc := item.Get("encrypted_content").String(); enc != "" {
			out = append(out, enc)
		}
	}
	return out
}

// MarkEncrypted appends rejected ciphertext digests to p.markedDigests in
// invocation order for session lineage assertions. Tests inspect the recorded
// slice after RelayTurn handles an invalid_encrypted_content upstream error
// event before client output begins.
func (p *streamReplayTestPort) MarkEncrypted(digests []string) {
	p.markedDigests = append(p.markedDigests, digests...)
}

// SummarizeError returns code, typ, and msg unchanged for diagnostic logging.
func (*streamReplayTestPort) SummarizeError(code, typ, msg string) (string, string, string) {
	return code, typ, msg
}

// GenericError returns a generic policy error for status.
func (*streamReplayTestPort) GenericError(int) error { return errors.New("generic policy error") }

// RawFailure returns a failover error when bare error events escape on Turn 1.
func (*streamReplayTestPort) RawFailure(_ int, _ map[string][]string, _ []byte, _ bool) error {
	return errors.New("upstream error failover")
}

// Failure returns a terminal failure error with msg.
func (*streamReplayTestPort) Failure(_ int, _ map[string][]string, _ []byte, msg string, _ bool) error {
	return errors.New(msg)
}

// Warning returns nil because upstream risk warnings are inactive in these unit tests.
func (*streamReplayTestPort) Warning(string, []byte) *forwardcore.UpstreamWarning { return nil }

// IsToken reports false because scripted responses emit terminal events directly.
func (*streamReplayTestPort) IsToken(string) bool { return false }

// IsTerminal reports whether event equals response.completed so RelayTurn ends
// its event loop on terminal frames. Non-terminal event labels return false and
// keep reading subsequent events from the lease.
func (*streamReplayTestPort) IsTerminal(event string) bool { return event == "response.completed" }

// NormalizeTerminal returns response.completed unchanged as the canonical terminal label.
func (*streamReplayTestPort) NormalizeTerminal(event string) string { return event }

// Message extracts top-level error.message text from upstream error frames for
// terminal failure presentation. Payloads without that string field return an
// empty string so RelayTurn applies its fallback error message.
func (*streamReplayTestPort) Message(body []byte) string {
	return gjson.GetBytes(body, "error.message").String()
}

// GenericEvent returns a fixed JSON error payload for generic stream failures.
func (*streamReplayTestPort) GenericEvent() []byte {
	return []byte(`{"type":"error","error":{"message":"generic"}}`)
}

// MayContainModel reports false because model restoration is unused in these unit tests.
func (*streamReplayTestPort) MayContainModel(string) bool { return false }

// ReplaceModel returns downstream response bytes without substituting model names.
func (*streamReplayTestPort) ReplaceModel(body []byte, _, _ string) []byte { return body }

// MayContainTools reports false because tool-name restoration is unused in these unit tests.
func (*streamReplayTestPort) MayContainTools(string) bool { return false }

// LikelyTools reports false because scripted responses contain no tool calls.
func (*streamReplayTestPort) LikelyTools([]byte) bool { return false }

// CorrectTools returns downstream response bytes unchanged with false.
func (*streamReplayTestPort) CorrectTools(body []byte) ([]byte, bool) { return body, false }

// CapacityShed returns downstream response bytes unchanged without overload rewriting.
func (*streamReplayTestPort) CapacityShed(body []byte) ([]byte, bool) { return body, false }

// WriteClient clones body into p.clientWrites and returns nil to record
// successful downstream frame delivery. Allocating a separate copy keeps each
// captured client event independent of later buffer mutations.
func (p *streamReplayTestPort) WriteClient(body []byte) error {
	p.clientWrites = append(p.clientWrites, append([]byte(nil), body...))
	return nil
}

// IsDisconnect reports false because WriteClient succeeds in these tests.
func (*streamReplayTestPort) IsDisconnect(error) bool { return false }

// SummarizeClose returns empty status and reason strings for client disconnects.
func (*streamReplayTestPort) SummarizeClose(error) (string, string) { return "", "" }

// Truncate returns s unchanged without applying diagnostic length truncation.
func (*streamReplayTestPort) Truncate(s string, _ int) string { return s }

// NormalizeLog returns s unchanged as an already safe diagnostic label.
func (*streamReplayTestPort) NormalizeLog(s string) string { return s }

// Log discards informational relay diagnostics during in-memory unit tests.
func (*streamReplayTestPort) Log(string) {}

// Debug discards verbose per-frame diagnostics during in-memory unit tests.
func (*streamReplayTestPort) Debug(string) {}

// TestRelayTurn_PrepareNativeReplay exercises pooled WebSocket RelayTurn native
// continuity-carrier replay. Literal user "trc1:" payloads stay byte-identical,
// same-account turns unpack encrypted_content (and feed unpacked bytes into
// invalid_encrypted_content digest lineage), and cross-account turns strip
// mismatched envelopes while keeping ClientPayload.PayloadRaw intact for failover.
func TestRelayTurn_PrepareNativeReplay(t *testing.T) {
	carrierAcct42, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_ws_stream_42",
		ProviderID: 42,
	})
	require.NoError(t, err)
	foreignCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeXAI,
		Data:       "xai_foreign",
		ProviderID: 42,
	})
	require.NoError(t, err)

	completedFrame := []byte(`{"type":"response.completed","response":{"id":"resp_pool_1","status":"completed"}}`)
	invalidEncFrame := []byte(`{"type":"error","error":{"type":"invalid_request_error","code":"invalid_encrypted_content","message":"cannot decrypt"}}`)

	carrierPayload := []byte(`{"type":"response.create","model":"gpt-5.4","store":false,"nonce":9007199254740993,"input":[{"type":"message","role":"user","content":"literal trc1:plain:demo"},{"type":"reasoning","id":"rs_keep","summary":[{"type":"summary_text","text":"pooled summary"}],"encrypted_content":"` + carrierAcct42 + `"},{"type":"reasoning","id":"rs_drop","summary":[],"encrypted_content":"` + foreignCarrier + `"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`)
	literalPayload := []byte("{\n  \"type\": \"response.create\",\n  \"model\": \"gpt-5.4\",\n  \"nonce\": 9007199254740993,\n  \"input\": [{\"type\": \"message\", \"role\": \"user\", \"content\": \"literal trc1:plain:demo\"}]\n}")

	// 1. Literal user "trc1:" without token-path carriers remains byte-for-byte identical.
	leaseLit := &streamReplayTestLease{events: [][]byte{completedFrame}}
	portLit := &streamReplayTestPort{dialect: convert.Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 42}}
	_, err = RelayTurn(context.Background(), portLit, leaseLit, ClientPayload{
		PayloadRaw:    literalPayload,
		PayloadBytes:  len(literalPayload),
		OriginalModel: "gpt-5.4",
		RoutingModel:  "gpt-5.4",
	}, 1, StreamOptions{ProviderID: 42, WriteTimeout: time.Second, ReadTimeout: time.Second}, nil)
	require.NoError(t, err)
	require.Equal(t, literalPayload, leaseLit.written[0])

	// 2. Same account (ID=42) unpacks carrierAcct42, drops empty foreign item, and passes unpacked outboundPayload to EncryptedDigests on invalid_encrypted_content.
	leaseSame := &streamReplayTestLease{events: [][]byte{invalidEncFrame}}
	portSame := &streamReplayTestPort{dialect: convert.Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 42}}
	inSame := ClientPayload{
		PayloadRaw:    append([]byte(nil), carrierPayload...),
		PayloadBytes:  len(carrierPayload),
		OriginalModel: "gpt-5.4",
		RoutingModel:  "gpt-5.4",
	}
	_, err = RelayTurn(context.Background(), portSame, leaseSame, inSame, 1, StreamOptions{ProviderID: 42, WriteTimeout: time.Second, ReadTimeout: time.Second}, nil)
	require.Error(t, err)
	require.Equal(t, "gAAAA_ws_stream_42", gjson.GetBytes(leaseSame.written[0], "input.1.encrypted_content").String())
	require.Equal(t, "message", gjson.GetBytes(leaseSame.written[0], "input.2.type").String())
	require.Equal(t, "9007199254740993", gjson.GetBytes(leaseSame.written[0], "nonce").Raw)
	require.Equal(t, []string{"gAAAA_ws_stream_42"}, portSame.markedDigests)
	// ClientPayload.PayloadRaw must retain intact pre-replay trc1: envelope for failover.
	require.Equal(t, carrierAcct42, gjson.GetBytes(inSame.PayloadRaw, "input.1.encrypted_content").String())

	// 3. Cross account (ID=99) strips mismatched encrypted_content and synthetic store=false id while retaining summary.
	leaseCross := &streamReplayTestLease{events: [][]byte{completedFrame}}
	portCross := &streamReplayTestPort{dialect: convert.Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: 99}}
	_, err = RelayTurn(context.Background(), portCross, leaseCross, ClientPayload{
		PayloadRaw:    carrierPayload,
		PayloadBytes:  len(carrierPayload),
		OriginalModel: "gpt-5.4",
		RoutingModel:  "gpt-5.4",
	}, 1, StreamOptions{ProviderID: 99, WriteTimeout: time.Second, ReadTimeout: time.Second}, nil)
	require.NoError(t, err)
	require.Equal(t, "pooled summary", gjson.GetBytes(leaseCross.written[0], "input.1.summary.0.text").String())
	require.False(t, gjson.GetBytes(leaseCross.written[0], "input.1.encrypted_content").Exists())
	require.False(t, gjson.GetBytes(leaseCross.written[0], "input.1.id").Exists())
	require.Equal(t, "message", gjson.GetBytes(leaseCross.written[0], "input.2.type").String())
}
