package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/provider"
)

// TestOpenAIConvertedResponsesAdapter_DialectAndCursorConversion checks
// dialect-aware conversion of input-shaped Cursor Chat requests on
// openAIConvertedResponsesAdapter. Responses dialects preserve hosted
// web_search support and apply catalog sampling rules such as dropping
// temperature and top_p on high-effort GPT-5.4 requests.
func TestOpenAIConvertedResponsesAdapter_DialectAndCursorConversion(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	prov := &gatewayprovider.ExecutionProvider{
		Record: provider.Record{
			ID:       77,
			Platform: "openai",
			Type:     "apikey",
		},
	}
	requests := newAuxiliaryFixture(auxiliaryFixtureInputs{}).Requests

	exec := &OpenAITextExecutor{Requests: requests}
	adapter := exec.newConvertedResponsesAdapter(c, prov, nil)
	require.NotNil(t, adapter)

	dialect, err := adapter.ResolveResponsesDialect("gpt-5.4", []byte(`{"model":"claude-sonnet-4-5","messages":[{"role":"user","content":"hi"}]}`))
	require.NoError(t, err)
	require.True(t, dialect.Tools.Has(convert.ToolSupportHostedWebSearch))

	chatBytes, err := adapter.ConvertCursorResponsesToChat([]byte(`{"model":"gpt-5.4","temperature":0.7,"top_p":0.8,"reasoning_effort":"high","input":[{"type":"message","role":"user","content":"cursor input"}]}`))
	require.NoError(t, err)
	require.Contains(t, string(chatBytes), `"messages":[`)
	require.Contains(t, string(chatBytes), `"cursor input"`)
	require.Contains(t, string(chatBytes), `"reasoning_effort":"high"`)
	require.NotContains(t, string(chatBytes), `"temperature"`, "ConvertCursorResponsesToChat must apply modelResolutionFromBytes DropTemperature rules")
	require.NotContains(t, string(chatBytes), `"top_p"`, "ConvertCursorResponsesToChat must apply modelResolutionFromBytes DropTopP rules")
}
