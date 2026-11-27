package ws

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// passthroughReplayClientSocket feeds scripted client WebSocket frames to
// PassthroughSession.Run while discarding best-effort policy rejection writes.
type passthroughReplayClientSocket struct {
	frames [][]byte // scripted inbound text frames read sequentially
	idx    int      // next unread frame index in frames
}

// passthroughReplayUpstreamConn records frames written to the dedicated upstream
// WebSocket connection during PassthroughSession.Run.
type passthroughReplayUpstreamConn struct {
	writes [][]byte // outbound frame payloads captured in send order
}

// passthroughReplayTestPort stubs PassthroughPort and nativeReplayDialectResolver
// so unit tests isolate dedicated WebSocket continuity-carrier preparation.
type passthroughReplayTestPort struct {
	RequestUsageDecoder                                // shared request effort and service-tier extractors
	dialect             convert.Dialect                // target dialect returned by ResolveNativeReplayDialect
	prepareDialBodies   [][]byte                       // pre-replay bodies observed by PrepareDial
	upstream            *passthroughReplayUpstreamConn // dedicated upstream frame recorder
	relayReadPayloads   [][]byte                       // post-filter later-turn payloads read by RunRelay
}

// Read advances c.idx and returns the next scripted text frame for client
// relay consumption. Once all scripted frames have been read, subsequent calls
// return io.EOF so the client reader exits cleanly.
func (c *passthroughReplayClientSocket) Read(context.Context) (int, []byte, error) {
	if c.idx >= len(c.frames) {
		return 0, nil, io.EOF
	}
	frame := c.frames[c.idx]
	c.idx++
	return TextFrame, frame, nil
}

// Write accepts synchronous client error writes without side effects in unit tests.
func (*passthroughReplayClientSocket) Write(context.Context, int, []byte) error {
	return nil
}

// IsNormalClose reports whether err represents clean end-of-script termination
// on the client test socket. Only io.EOF matches as normal closure, so any
// other transport failure still surfaces as an abnormal session exit.
func (*passthroughReplayClientSocket) IsNormalClose(err error) bool {
	return errors.Is(err, io.EOF)
}

// Close records no transport side effects on scripted client sockets.
func (*passthroughReplayClientSocket) Close(int, string) error { return nil }

// CloseNow terminates no external resources on scripted client sockets.
func (*passthroughReplayClientSocket) CloseNow() error { return nil }

// ReadFrame returns io.EOF immediately because RunRelay drives turn completion
// callbacks directly without reading upstream socket frames. Callers treat that
// EOF as an idle stub stream during unit tests.
func (*passthroughReplayUpstreamConn) ReadFrame(context.Context) (int, []byte, error) {
	return 0, nil, io.EOF
}

// WriteFrame clones data and appends the copy to u.writes in outbound dispatch
// order. Allocating a detached byte slice isolates captured Turn 1 wire payloads
// from any later buffer reuse during PassthroughSession assertions.
func (u *passthroughReplayUpstreamConn) WriteFrame(_ context.Context, _ int, data []byte) error {
	u.writes = append(u.writes, append([]byte(nil), data...))
	return nil
}

// Close releases no external resources on the in-memory upstream recorder.
func (*passthroughReplayUpstreamConn) Close() error { return nil }

// ResolveNativeReplayDialect returns the configured convert.Dialect for native replay tests.
func (p *passthroughReplayTestPort) ResolveNativeReplayDialect(context.Context, string, []byte) (convert.Dialect, error) {
	return p.dialect, nil
}

// ModelAttributes returns body unchanged so byte-identity assertions remain exact.
func (*passthroughReplayTestPort) ModelAttributes(_ context.Context, _ int, _ string, body []byte) ([]byte, error) {
	return body, nil
}

// IsLite reports false for standard Responses WebSocket test frames.
func (*passthroughReplayTestPort) IsLite([]byte) bool { return false }

// NormalizeLite returns body unchanged because Lite mode is inactive in these tests.
func (*passthroughReplayTestPort) NormalizeLite(body []byte) ([]byte, error) { return body, nil }

// Reasoning returns body unchanged with no group effort overrides applied.
func (*passthroughReplayTestPort) Reasoning(body []byte, _ string) ([]byte, error) { return body, nil }

// Models echoes model as both routing and upstream model without rewriting bytes.
func (*passthroughReplayTestPort) Models(_ int, model string, _ []byte) (string, string, error) {
	return model, model, nil
}

// AliasTools returns body unchanged when OAuth tool aliasing runs.
func (*passthroughReplayTestPort) AliasTools(body []byte) ([]byte, error) { return body, nil }

// Compatibility reports no byte modifications so pre-replay bytes remain intact.
func (*passthroughReplayTestPort) Compatibility(body []byte, _ bool) ([]byte, bool, error) {
	return body, false, nil
}

// ScopeIdentity reports no metadata changes so literal payload bytes stay untouched.
func (*passthroughReplayTestPort) ScopeIdentity(body []byte) ([]byte, bool, error) {
	return body, false, nil
}

// FastPolicy permits every turn without modifying service-tier bytes.
func (*passthroughReplayTestPort) FastPolicy(_ context.Context, _ int, _ string, body []byte, _ bool) ([]byte, *PolicyBlocked, error) {
	return body, nil, nil
}

// PromptReplace returns body unchanged for later client frames.
func (*passthroughReplayTestPort) PromptReplace(_ context.Context, body []byte) []byte { return body }

// BlockedEvent returns nil because FastPolicy never blocks in these tests.
func (*passthroughReplayTestPort) BlockedEvent(*PolicyBlocked) []byte { return nil }

// PolicyDenied performs no observation updates in unit tests.
func (*passthroughReplayTestPort) PolicyDenied() {}

// SetUpstreamModel ignores first-turn model observation in unit tests.
func (*passthroughReplayTestPort) SetUpstreamModel(string) {}

// PrepareDial appends a cloned copy of body to p.prepareDialBodies before
// returning nil. Recording this snapshot lets tests verify that handshake
// preparation still sees pre-replay Turn 1 bytes while upstream writes receive
// unpacked or stripped carriers.
func (p *passthroughReplayTestPort) PrepareDial(_ context.Context, body []byte, _ string) error {
	p.prepareDialBodies = append(p.prepareDialBodies, append([]byte(nil), body...))
	return nil
}

// DialOnce returns HTTP status 101 with p.upstream attached as the dedicated
// upstream frame connection. PassthroughSession adopts ownership of that
// recorder for the initial outbound write and final session close.
func (p *passthroughReplayTestPort) DialOnce(context.Context) (DialResult, error) {
	return DialResult{Status: 101, Conn: p.upstream}, nil
}

// CanRecover reports false because DialOnce succeeds immediately in unit tests.
func (*passthroughReplayTestPort) CanRecover(context.Context, DialResult, error) bool { return false }

// Recover returns nil and is unused when DialOnce succeeds.
func (*passthroughReplayTestPort) Recover(context.Context) error { return nil }

// DialFailure returns err unchanged if handshake failure is ever simulated.
func (*passthroughReplayTestPort) DialFailure(_ context.Context, _ string, _ DialResult, err error) error {
	return err
}

// RunRelay completes Turn 1 terminal write hooks to reopen turn admission and
// drains remaining client frames through in.ClientConn until EOF or error.
// Each filtered frame is cloned into p.relayReadPayloads, later response.create
// turns trigger another terminal completion cycle, and the relay exits
// gracefully with WroteDownstream set.
func (p *passthroughReplayTestPort) RunRelay(in RelayInput) (RelayResult, *RelayExit) {
	completed := []byte(`{"type":"response.completed","response":{"id":"resp_t","status":"completed"}}`)
	completeActive := func() {
		if in.Options.BeforeClientWrite != nil {
			in.Options.BeforeClientWrite(TextFrame, completed)
		}
		if in.Options.AfterClientWrite != nil {
			in.Options.AfterClientWrite(TextFrame, completed, nil)
		}
	}
	completeActive()
	for {
		_, payload, err := in.ClientConn.ReadFrame(in.Ctx)
		if err != nil {
			break
		}
		p.relayReadPayloads = append(p.relayReadPayloads, append([]byte(nil), payload...))
		if gjson.GetBytes(payload, "type").String() == "response.create" {
			completeActive()
		}
	}
	return RelayResult{TerminalEventType: "response.completed"}, &RelayExit{Stage: "read_client", Graceful: true, WroteDownstream: true}
}

// NormalizeCompleted reports no terminal repairs for downstream frames.
func (*passthroughReplayTestPort) NormalizeCompleted(body []byte) ([]byte, bool) { return body, false }

// RestoreTools returns downstream response bytes without rewriting tool names.
func (*passthroughReplayTestPort) RestoreTools(body []byte) []byte { return body }

// EventType extracts the top-level JSON type string from body for downstream
// event routing. Payloads with malformed JSON or an omitted type field return
// an empty string without raising an error.
func (*passthroughReplayTestPort) EventType(body []byte) string {
	return gjson.GetBytes(body, "type").String()
}

// MayContainModel reports false because model restoration is outside this test's scope.
func (*passthroughReplayTestPort) MayContainModel(string) bool { return false }

// ReplaceModel returns downstream response bytes without substituting model identifiers.
func (*passthroughReplayTestPort) ReplaceModel(body []byte, _, _ string) []byte { return body }

// IsTerminal reports whether event equals response.completed for downstream
// turn tracking. Non-matching event names return false so queue consumption and
// turn-lifecycle completion callbacks stay idle until terminal delivery.
func (*passthroughReplayTestPort) IsTerminal(event string) bool { return event == "response.completed" }

// NormalizeTerminal maps response.completed to itself as the canonical terminal
// event label. Unrecognized or non-terminal event names return an empty string
// so the active turn remains open for subsequent frames.
func (*passthroughReplayTestPort) NormalizeTerminal(event string) string {
	if event == "response.completed" {
		return event
	}
	return ""
}

// NormalizeTier returns the observed service tier string without remapping.
func (*passthroughReplayTestPort) NormalizeTier(tier string) string { return tier }

// Warning returns nil because no upstream error frames are emitted in this suite.
func (*passthroughReplayTestPort) Warning(string, []byte) *forwardcore.UpstreamWarning { return nil }

// BeforeWrite permits every downstream text frame without triggering provider failover.
func (*passthroughReplayTestPort) BeforeWrite(context.Context, string, []byte, bool, map[string][]string) error {
	return nil
}

// RelayClose declines custom close mapping on clean client EOF exits.
func (*passthroughReplayTestPort) RelayClose(RelayExit, int) (int, string, bool) {
	return 0, "", false
}

// CloseError returns the non-nil underlying err when supplied by policy checks.
// Nil causes fall back to a fresh error constructed from reason so close
// rejections always return a non-nil error.
func (*passthroughReplayTestPort) CloseError(_ int, reason string, err error) error {
	if err != nil {
		return err
	}
	return errors.New(reason)
}

// FirstOutputFailure returns a descriptive timeout error if armed deadlines expire.
func (*passthroughReplayTestPort) FirstOutputFailure(context.Context, Deadline, map[string][]string) error {
	return errors.New("first output timeout")
}

// FirstOutputTimeout returns one minute so DeadlineConn stays armed safely.
func (*passthroughReplayTestPort) FirstOutputTimeout(string) time.Duration { return time.Minute }

// Log discards informational passthrough logs during unit tests.
func (*passthroughReplayTestPort) Log(string) {}

// Truncate returns diagnostic text unchanged without applying byte truncation.
func (*passthroughReplayTestPort) Truncate(message string, _ int) string { return message }

// TruncateReason returns close-frame reason text unchanged without byte truncation.
func (*passthroughReplayTestPort) TruncateReason(message string, _ int) string { return message }

// ClosedError returns io.ErrClosedPipe as the transport closed sentinel.
func (*passthroughReplayTestPort) ClosedError() error { return io.ErrClosedPipe }

// TestPassthroughSession_PrepareNativeReplay exercises dedicated WebSocket
// PassthroughSession.Run across Turn 1 response.create, inter-turn session.update,
// and Turn 2 response.create frames. Literal user "trc1:" and session.update
// frames stay byte-identical, same-account sessions unpack encrypted_content,
// and cross-account sessions strip it.
func TestPassthroughSession_PrepareNativeReplay(t *testing.T) {
	carrierAcct42, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_ws_passthrough_42",
		ProviderID: 42,
	})
	require.NoError(t, err)
	foreignCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "EqQBForeignSig",
		Text:   "Foreign thought.",
	})
	require.NoError(t, err)

	turn1Carrier := []byte(`{"type":"response.create","model":"gpt-5.4","store":false,"nonce":9007199254740993,"input":[{"type":"message","role":"user","content":"literal trc1:plain:demo"},{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"turn1 summary"}],"encrypted_content":"` + carrierAcct42 + `"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`)
	sessionUpdate := []byte("{\n  \"type\": \"session.update\",\n  \"session\": {\"instructions\": \"keep trc1:openai:eyJkIjoiZiJ9 untouched\"},\n  \"seq\": 9007199254740993\n}")
	turn2Carrier := []byte(`{"type":"response.create","model":"gpt-5.4","input":[{"type":"message","role":"user","content":"turn2"},{"type":"reasoning","id":"rs_2","summary":[],"encrypted_content":"` + carrierAcct42 + `"},{"type":"reasoning","id":"rs_foreign","summary":[],"encrypted_content":"` + foreignCarrier + `"},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}]}`)
	turn1Literal := []byte("{\n  \"type\": \"response.create\",\n  \"model\": \"gpt-5.4\",\n  \"nonce\": 9007199254740993,\n  \"input\": [{\"type\": \"message\", \"role\": \"user\", \"content\": \"literal trc1:plain:demo\"}]\n}")

	runSession := func(providerID int64, first []byte, rest ...[]byte) (*passthroughReplayTestPort, error) {
		up := &passthroughReplayUpstreamConn{}
		port := &passthroughReplayTestPort{
			dialect:  convert.Dialect{ReplayScheme: ir.SchemeOpenAI, ProviderID: providerID},
			upstream: up,
		}
		sess := PassthroughSession{
			Port: port,
			Options: PassthroughOptions{
				ProviderID:           providerID,
				WriteTimeout:         time.Second,
				IdleTimeout:          time.Minute,
				InterTurnIdleTimeout: time.Minute,
			},
		}
		client := &passthroughReplayClientSocket{frames: rest}
		runErr := sess.Run(context.Background(), client, first)
		return port, runErr
	}

	// 1. Literal "trc1:" in user text and session.update remains byte-for-byte identical.
	portLit, err := runSession(42, turn1Literal, sessionUpdate)
	require.NoError(t, err)
	require.Equal(t, turn1Literal, portLit.upstream.writes[0])
	require.Equal(t, sessionUpdate, portLit.relayReadPayloads[0])

	// 2. Same account (ID=42) unpacks trc1:openai on Turn 1 and Turn 2 while PrepareDial still sees pre-replay Turn 1 bytes.
	portSame, err := runSession(42, turn1Carrier, sessionUpdate, turn2Carrier)
	require.NoError(t, err)
	require.Equal(t, carrierAcct42, gjson.GetBytes(portSame.prepareDialBodies[0], "input.1.encrypted_content").String())
	require.Equal(t, "gAAAA_ws_passthrough_42", gjson.GetBytes(portSame.upstream.writes[0], "input.1.encrypted_content").String())
	require.Equal(t, "9007199254740993", gjson.GetBytes(portSame.upstream.writes[0], "nonce").Raw)
	require.Equal(t, sessionUpdate, portSame.relayReadPayloads[0])
	require.Equal(t, "gAAAA_ws_passthrough_42", gjson.GetBytes(portSame.relayReadPayloads[1], "input.1.encrypted_content").String())
	require.Equal(t, "message", gjson.GetBytes(portSame.relayReadPayloads[1], "input.2.type").String())

	// 3. Cross account (ID=99) strips mismatched encrypted_content while preserving Turn 1 summary and dropping empty Turn 2 items.
	portCross, err := runSession(99, turn1Carrier, sessionUpdate, turn2Carrier)
	require.NoError(t, err)
	require.Equal(t, "turn1 summary", gjson.GetBytes(portCross.upstream.writes[0], "input.1.summary.0.text").String())
	require.False(t, gjson.GetBytes(portCross.upstream.writes[0], "input.1.encrypted_content").Exists())
	require.False(t, gjson.GetBytes(portCross.upstream.writes[0], "input.1.id").Exists())
	require.Equal(t, sessionUpdate, portCross.relayReadPayloads[0])
	require.Equal(t, "message", gjson.GetBytes(portCross.relayReadPayloads[1], "input.1.type").String())
}
