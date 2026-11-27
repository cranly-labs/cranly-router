package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	openaiexecution "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/provider"
)

// TestOpenAIConvertedRawChatAdapter_DialectAndSourceErrors checks raw-byte Chat
// dialect resolution and pre-committed SSE error formatting on
// openAIConvertedRawChatAdapter. High-effort GPT-5.4 requests must drop
// prohibited sampling parameters, and SourceError on an already committed
// Responses SSE context must emit synthetic response.failed frames.
func TestOpenAIConvertedRawChatAdapter_DialectAndSourceErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	prov := &gatewayprovider.ExecutionProvider{
		Record: provider.Record{
			ID:       88,
			Platform: "openai",
			Type:     "apikey",
		},
	}
	requests := newAuxiliaryFixture(auxiliaryFixtureInputs{}).Requests

	exec := &OpenAITextExecutor{Requests: requests}
	adapter := exec.newConvertedRawChatAdapter(c, prov, openaiexecution.NativeResponses, nil)
	require.NotNil(t, adapter)

	dialect, err := adapter.ResolveChatDialect(
		"gpt-5.4",
		[]byte(`{"model":"gpt-5.4","reasoning":{"effort":"high"},"input":"hello"}`),
	)
	require.NoError(t, err)
	require.Equal(t, convert.HistoryToolTurns, dialect.ReasoningHistory)
	require.Equal(t, convert.ToolSupportFunction, dialect.Tools)
	require.True(t, dialect.DropTemperature, "high-effort gpt-5.4 Chat dialect must drop temperature")

	// Commit HTTP 200 text/event-stream headers beforehand to simulate an early
	// waiting keepalive heartbeat before SourceError formats the client failure.
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.WriteHeader(http.StatusOK)
	c.Writer.WriteHeaderNow()
	adapter.SourceError(protocol.ProtocolOpenAIResponses, http.StatusBadRequest, "invalid_request_error", "bad input")
	require.True(t, IsResponseCommitted(c))
	require.True(t, TerminalSSEDelivered(c))
	require.Contains(t, rec.Body.String(), "event: response.failed\n")

	// Verify UpdateGrokUsage tolerates nil Grok health services and nil adapters
	// without panicking when unit or partial executor fixtures omit Grok wiring.
	require.NotPanics(t, func() {
		adapter.UpdateGrokUsage(c.Request.Context(), "grok-4", http.Header{}, http.StatusOK)
		var nilAdapter *openAIConvertedRawChatAdapter
		nilAdapter.UpdateGrokUsage(c.Request.Context(), "grok-4", http.Header{}, http.StatusOK)
	})
}
