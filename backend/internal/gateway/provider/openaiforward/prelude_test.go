package openaiforward

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/egress"
)

// stubPreludePorts records IR client-tool prelude invocations while satisfying
// PreludePorts for unit tests.
type stubPreludePorts struct {
	mappedModel    string // MappedModel override when non-empty
	needsCalled    int    // NeedsClientToolAdaptation call count
	adaptCalled    int    // AdaptClientTools call count
	lastModel      string // model passed to NeedsClientToolAdaptation and AdaptClientTools
	adaptedPayload []byte // replacement payload returned by adaptation
}

// BlockGroupImages returns false so test payloads skip local group image blocking.
func (*stubPreludePorts) BlockGroupImages() bool { return false }

// Begin is a no-op attempt observation hook for prelude unit tests.
func (*stubPreludePorts) Begin() {}

// ClearMappings is a no-op mapping reset hook for prelude unit tests.
func (*stubPreludePorts) ClearMappings() {}

// PrepareIdentity succeeds immediately without mutating request Codex identity state in tests.
func (*stubPreludePorts) PrepareIdentity(context.Context) error { return nil }

// MatchTLS returns an empty TLS fingerprint router match result for prelude tests.
func (*stubPreludePorts) MatchTLS() egress.TLSFingerprintRouterMatchResult {
	return egress.TLSFingerprintRouterMatchResult{}
}

// ClientAllowed admits every incoming test request without returning a client policy error.
func (*stubPreludePorts) ClientAllowed(context.Context, egress.TLSFingerprintRouterMatchResult, []byte) (bool, string) {
	return true, ""
}

// Reject is a no-op local rejection writer for valid prelude test inputs.
func (*stubPreludePorts) Reject(Rejection) {}

// LiteHeader disables Responses Lite header normalization in prelude unit tests.
func (*stubPreludePorts) LiteHeader() bool { return false }

// LitePayload returns the supplied request bytes unchanged with no rejection parameter.
func (*stubPreludePorts) LitePayload(b []byte) ([]byte, bool, string, error) {
	return b, false, "", nil
}

// Transport selects standard HTTP SSE transport for prelude unit test preparation.
func (*stubPreludePorts) Transport() TransportDecision {
	return TransportDecision{Transport: "http_sse", Reason: "test"}
}

// CompactPath reports false so PreparePrelude runs ordinary non-Compact tool preparation.
func (*stubPreludePorts) CompactPath() bool { return false }

// FlattenRequired disables OAuth namespace flattening in API Key prelude tests.
func (*stubPreludePorts) FlattenRequired(TransportDecision, bool, bool) bool { return false }

// Flatten returns the supplied request bytes unchanged when namespace flattening is skipped.
func (*stubPreludePorts) Flatten(b []byte) ([]byte, error) { return b, nil }

// StripNamespacesRequired disables historical input namespace stripping in DeepSeek prelude tests.
func (*stubPreludePorts) StripNamespacesRequired(TransportDecision, bool) bool { return false }

// KeepNamespaces reports false when historical input namespace stripping is disabled.
func (*stubPreludePorts) KeepNamespaces(TransportDecision, bool, bool) bool { return false }

// NeedsClientToolAdaptation increments needsCalled, records model, and reports true.
func (s *stubPreludePorts) NeedsClientToolAdaptation(_ context.Context, model string, _ []byte) bool {
	s.needsCalled++
	s.lastModel = model
	return true
}

// AdaptClientTools increments adaptCalled, records model, and returns s.adaptedPayload.
func (s *stubPreludePorts) AdaptClientTools(_ context.Context, model string, _ []byte) ([]byte, error) {
	s.adaptCalled++
	s.lastModel = model
	return s.adaptedPayload, nil
}

// BindMessagesBridge ignores Messages-bridge context markers in prelude unit tests.
func (*stubPreludePorts) BindMessagesBridge(bool) {}

// CodexClient reports false so PreparePrelude treats the caller as a generic API Key client.
func (*stubPreludePorts) CodexClient() bool { return false }

// ImageToolPolicy returns an empty explicit image-generation tool policy string for tests.
func (*stubPreludePorts) ImageToolPolicy() string { return "" }

// ObserveTransport ignores resolved transport mode diagnostics in prelude unit tests.
func (*stubPreludePorts) ObserveTransport(TransportDecision, string, bool) {}

// MappedModel returns s.mappedModel when configured, or echoes model unchanged otherwise.
// PreparePrelude passes this resolved upstream model name into IR client-tool lowering.
func (s *stubPreludePorts) MappedModel(model string) string {
	if s.mappedModel != "" {
		return s.mappedModel
	}
	return model
}

// TestPreparePrelude_AdaptsClientToolsWithMappedModel prepares a compatible API
// Key Responses request that requires client-tool encoding. It verifies that
// PreparePrelude resolves MappedModel for NeedsClientToolAdaptation and
// AdaptClientTools and invalidates SanitizedToolSchemasPlatform after lowering.
func TestPreparePrelude_AdaptsClientToolsWithMappedModel(t *testing.T) {
	body := []byte(`{"model":"ds-alias","tools":[{"type":"custom","name":"exec"}],"input":"hi"}`)
	adapted := []byte(`{"model":"ds-alias","tools":[{"type":"function","name":"exec","parameters":{"type":"object"}}],"input":"hi"}`)
	profile := Profile{
		Platform:                   "deepseek",
		Type:                       "apikey",
		APIKey:                     true,
		EncodeResponsesClientTools: true,
	}

	ports := &stubPreludePorts{mappedModel: "deepseek-chat", adaptedPayload: adapted}
	got, err := PreparePrelude(context.Background(), body, profile, ports)
	require.NoError(t, err)
	require.Equal(t, 1, ports.needsCalled)
	require.Equal(t, 1, ports.adaptCalled)
	require.Equal(t, "deepseek-chat", ports.lastModel)
	require.Equal(t, adapted, got.Body)
	require.Empty(t, got.SanitizedToolSchemasPlatform)
}
