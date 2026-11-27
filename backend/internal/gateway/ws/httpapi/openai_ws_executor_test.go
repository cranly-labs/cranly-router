package httpapi

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	gatewayws "github.com/TokenFlux/TokenRouter/internal/gateway/ws"
)

// TestNewOpenAIGatewayService_InitializesOpenAIWSResolver constructs a WS
// executor from the shared fixture bindings. Resolving a nil provider returns
// HTTP/SSE and the protocol_http reason, confirming that construction retains
// the bound selector.
func TestNewOpenAIGatewayService_InitializesOpenAIWSResolver(t *testing.T) {
	options := &wsFixtureOptions{}
	fixture := newWSFixture(wsFixtureInputs{options: options})
	svc := NewOpenAIWebSocketExecutor(fixture.OpenAIWSDependencies)

	decision := svc.Selection.ResolveTransport(nil)
	require.Equal(t, egress.OpenAIUpstreamTransportHTTPSSE, decision.Transport)
	require.Equal(t, "protocol_http", decision.Reason)
}

// TestOpenAIWebSocketExecutor_ForTurnSnapshotsRuntimeParameters constructs a
// WebSocket executor bound to a live Runtime, then snapshots turn parameters
// via forTurn. The copied turn executor must apply the updated
// ReadTimeoutSeconds snapshot while sharing the bound Selection dependency.
func TestOpenAIWebSocketExecutor_ForTurnSnapshotsRuntimeParameters(t *testing.T) {
	defaults := gatewayws.DefaultParameters()
	defaults.ReadTimeoutSeconds = 42
	runtime := gatewayws.NewRuntime(defaults, nil, nil)

	fixture := newWSFixture(wsFixtureInputs{options: &wsFixtureOptions{}})
	deps := fixture.OpenAIWSDependencies
	deps.Runtime = runtime

	svc := NewOpenAIWebSocketExecutor(deps)
	turn := svc.forTurn()
	require.NotSame(t, svc, turn)
	require.Same(t, svc.Selection, turn.Selection)
	require.NotNil(t, turn.Options)
	require.Equal(t, 42, turn.Options.ReadTimeoutSeconds)
}
