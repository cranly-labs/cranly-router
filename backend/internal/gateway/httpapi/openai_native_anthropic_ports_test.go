package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	openaiexecution "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/provider"
)

// openai_native_anthropic_ports_test.go verifies dialect passback detection,
// signature-error matching, Anthropic SSE error.type classification, native
// continuity carrier replay, and CommittedSSE terminal error promotion on both
// compatible and native Messages boundaries.

// anthropicHealthRecordStub counts overload, auth-error, and model-cooldown
// updates triggered by StreamFailure during compatible Anthropic stream error tests.
type anthropicHealthRecordStub struct {
	gatewayprovider.ExecutionProviderStore     // unused repository methods
	overloadedCalls                        int // number of SetOverloaded invocations
	errorCalls                             int // number of SetError invocations
	modelRateLimitCalls                    int // number of SetModelRateLimit invocations
}

// TestOpenAINativeAnthropicAdapter_DialectAndCommittedSSE verifies that
// openAINativeAnthropicAdapter resolves Claude and DeepSeek passback dialects
// accurately. Pre-semantic errors after SSE header commitment must promote to
// response.failed terminal SSE frames on both compatible and Messages boundaries.
func TestOpenAINativeAnthropicAdapter_DialectAndCommittedSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

	requests := newAuxiliaryFixture(auxiliaryFixtureInputs{}).Requests
	claudeProv := &gatewayprovider.ExecutionProvider{
		Record: provider.Record{
			ID:       91,
			Platform: "openai",
			Type:     "apikey",
		},
	}

	adapter := &openAINativeAnthropicAdapter{
		openAIMessagesExecutionAdapter: &openAIMessagesExecutionAdapter{
			s:        &OpenAITextExecutor{Requests: requests},
			c:        c,
			provider: claudeProv,
		},
		kind: openaiexecution.NativeResponses,
	}

	claudeDialect, claudePassback, err := adapter.ResolveAnthropicDialect(
		context.Background(),
		"claude-opus-4-6",
		[]byte(`{"model":"claude-opus-4-6","input":"hi"}`),
	)
	require.NoError(t, err)
	assert.False(t, claudePassback)
	assert.Equal(t, ir.SchemeAnthropic, claudeDialect.ReplayScheme)

	dsAdapter := &openAINativeAnthropicAdapter{
		openAIMessagesExecutionAdapter: &openAIMessagesExecutionAdapter{
			s: &OpenAITextExecutor{Requests: requests},
			c: c,
			provider: &gatewayprovider.ExecutionProvider{
				Record: provider.Record{ID: 92, Platform: "deepseek", Type: "apikey"},
			},
		},
		kind: openaiexecution.NativeResponses,
	}
	dsDialect, dsPassback, err := dsAdapter.ResolveAnthropicDialect(
		context.Background(),
		"deepseek-reasoner",
		[]byte(`{"model":"deepseek-reasoner","input":"hi"}`),
	)
	require.NoError(t, err)
	assert.True(t, dsPassback)
	assert.Equal(t, convert.HistoryToolTurns, dsDialect.ReasoningHistory)
	assert.Empty(t, dsDialect.ReplayScheme)

	assert.True(t, adapter.IsSignatureError(
		context.Background(),
		[]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"messages.1.content.0: Invalid `+"`signature`"+` in `+"`thinking`"+` block"}}`),
	))
	assert.False(t, adapter.IsSignatureError(
		context.Background(),
		[]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"max_tokens must be positive"}}`),
	))

	// Commit HTTP 200 text/event-stream headers first (simulating a waiting keepalive
	// heartbeat) and verify both SourceError and MessageForwardBoundary promote to response.failed SSE.
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.WriteHeader(http.StatusOK)
	c.Writer.WriteHeaderNow()
	require.True(t, CommittedSSE(c))

	adapter.SourceError(protocol.ProtocolOpenAIResponses, http.StatusBadGateway, "upstream_error", "stream broke after keepalive")
	assert.Contains(t, rec.Body.String(), "event: response.failed")
	assert.Contains(t, rec.Body.String(), "stream broke after keepalive")

	rec2 := httptest.NewRecorder()
	c2, _ := gin.CreateTestContext(rec2)
	c2.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c2.Writer.Header().Set("Content-Type", "text/event-stream")
	c2.Writer.WriteHeader(http.StatusOK)
	c2.Writer.WriteHeaderNow()

	boundary := NewMessageForwardBoundary(c2, nil)
	require.True(t, boundary.CommittedSSE())
	boundary.WriteConvertedError(protocol.ProtocolOpenAIResponses, "claude-sonnet-4-6", http.StatusBadGateway, "server_error", "", "messages boundary promoted")
	assert.Contains(t, rec2.Body.String(), "event: response.failed")
	assert.Contains(t, rec2.Body.String(), "messages boundary promoted")
}

// SetOverloaded increments overloadedCalls and returns nil for unit assertions.
func (s *anthropicHealthRecordStub) SetOverloaded(_ context.Context, _ int64, _ time.Time) error {
	s.overloadedCalls++
	return nil
}

// SetError increments errorCalls and returns nil when 401 or 403 health policies run.
func (s *anthropicHealthRecordStub) SetError(_ context.Context, _ int64, _ string) error {
	s.errorCalls++
	return nil
}

// SetTempUnschedulable accepts temporary cooldown persistence as a no-op in memory.
func (*anthropicHealthRecordStub) SetTempUnschedulable(_ context.Context, _ int64, _ time.Time, _ string) error {
	return nil
}

// SetModelRateLimit increments modelRateLimitCalls when 404 model cooldowns run.
func (s *anthropicHealthRecordStub) SetModelRateLimit(_ context.Context, _ int64, _ string, _ time.Time, _ ...string) error {
	s.modelRateLimitCalls++
	return nil
}

// SetRateLimited accepts provider-level 429 rate-limit persistence as a no-op in memory.
func (*anthropicHealthRecordStub) SetRateLimited(_ context.Context, _ int64, _ time.Time) error {
	return nil
}

// TestConvertedAnthropicOptions_StreamFailureClassifiesAnthropicErrorTypes
// supplies standard Anthropic overloaded_error, permission_error, not_found_error,
// and invalid_request_error SSE payloads to ConvertedAnthropicOptions.StreamFailure.
// Pre-output retryable errors must return Failover with exact 529 or 403 status
// codes and overload cooldowns, while 404, 400, and post-output errors skip failover.
func TestConvertedAnthropicOptions_StreamFailureClassifiesAnthropicErrorTypes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	store := &anthropicHealthRecordStub{}
	upHealth := newUpstreamHealthForTest(store, nil, nil, provider.HealthOptions{}, nil)
	svc := newWSFixture(wsFixtureInputs{health: upHealth})
	out := svc.Output
	prov := &gatewayprovider.ExecutionProvider{
		Record: provider.Record{
			LoadLocation: time.LoadLocation,
			ID:           301,
			Name:         "compat-anthropic",
			Platform:     "deepseek",
			Type:         "apikey",
		},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"X-Request-Id": []string{"req_anth_err"}},
	}

	newOpts := func() openaiexecution.ConvertedAnthropicOutputOptions {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		return out.ConvertedAnthropicOptions(c, prov, resp, protocol.ProtocolOpenAIResponses, "gpt-5", "deepseek-reasoner", "deepseek-reasoner", 64)
	}

	// 1. Pre-semantic overloaded_error ("Overloaded") maps to 529, triggers SetOverloaded, and fails over with StatusCode 529.
	overloadedPayload := []byte(`{"type":"error","error":{"type":"overloaded_error","message":"Overloaded"}}`)
	fOverload := newOpts().StreamFailure(context.Background(), overloadedPayload, "Overloaded", false)
	require.NotNil(t, fOverload.Failover)
	var foErr *forwardcore.UpstreamFailoverError
	require.True(t, errors.As(fOverload.Failover, &foErr))
	assert.Equal(t, 529, foErr.StatusCode)
	assert.Equal(t, 1, store.overloadedCalls)

	// 2. Pre-semantic permission_error maps to 403 and fails over cleanly.
	permPayload := []byte(`{"type":"error","error":{"type":"permission_error","message":"Forbidden"}}`)
	fPerm := newOpts().StreamFailure(context.Background(), permPayload, "Forbidden", false)
	require.NotNil(t, fPerm.Failover)
	require.True(t, errors.As(fPerm.Failover, &foErr))
	assert.Equal(t, http.StatusForbidden, foErr.StatusCode)

	// 3a. Pre-semantic not_found_error on a general missing resource maps to 404, declines failover, and preserves not_found_error.
	notFoundResource := []byte(`{"type":"error","error":{"type":"not_found_error","message":"requested resource was not found"}}`)
	fNotFound := newOpts().StreamFailure(context.Background(), notFoundResource, "requested resource was not found", false)
	assert.Nil(t, fNotFound.Failover)
	assert.Equal(t, http.StatusNotFound, fNotFound.Status)
	assert.Equal(t, "not_found_error", fNotFound.Type)
	assert.Equal(t, "not_found_error", fNotFound.Code)

	// 3b. Pre-semantic model-scoped not_found_error records SetModelRateLimit and fails over with exact StatusCode 404 (not 502).
	notFoundModel := []byte(`{"type":"error","error":{"type":"not_found_error","message":"model not found"}}`)
	fModelNotFound := newOpts().StreamFailure(context.Background(), notFoundModel, "model not found", false)
	require.NotNil(t, fModelNotFound.Failover)
	require.True(t, errors.As(fModelNotFound.Failover, &foErr))
	assert.Equal(t, http.StatusNotFound, foErr.StatusCode)
	assert.Equal(t, 1, store.modelRateLimitCalls)

	// 4. Pre-semantic invalid_request_error maps to 400, declines failover, and preserves invalid_request_error type/code.
	badReqPayload := []byte(`{"type":"error","error":{"type":"invalid_request_error","message":"bad parameter"}}`)
	fBadReq := newOpts().StreamFailure(context.Background(), badReqPayload, "bad parameter", false)
	assert.Nil(t, fBadReq.Failover)
	assert.Equal(t, http.StatusBadRequest, fBadReq.Status)
	assert.Equal(t, "invalid_request_error", fBadReq.Type)
	assert.Equal(t, "invalid_request_error", fBadReq.Code)

	// 5. Post-semantic overloaded_error (started=true) skips health persistence and declines failover.
	beforeOverloads := store.overloadedCalls
	fStarted := newOpts().StreamFailure(context.Background(), overloadedPayload, "Overloaded", true)
	assert.Nil(t, fStarted.Failover)
	assert.Equal(t, beforeOverloads, store.overloadedCalls, "post-output stream errors must not mutate provider health")
	assert.Equal(t, http.StatusBadGateway, fStarted.Status)
	assert.Equal(t, "overloaded_error", fStarted.Type)

	// 6. PrepareNativeMessagesReplay preserves literal user text and unpacks passback carriers.
	plainCarrier, err := ir.FormatCarrier(ir.Carrier{Scheme: ir.SchemePlain, Text: "Passback adapter thought."})
	require.NoError(t, err)
	msgWithCarrier := []byte(`{"model":"deepseek-reasoner","max_tokens":64,"messages":[{"role":"user","content":"literal trc1:plain:demo"},{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":"` + plainCarrier + `"},{"type":"text","text":"ok"}]}]}`)
	msgLiteralOnly := []byte(`{"model":"deepseek-reasoner","max_tokens":64,"messages":[{"role":"user","content":"literal trc1:plain:demo"}]}`)

	dsCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	dsCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	dsNative := &openAINativeAnthropicAdapter{
		openAIMessagesExecutionAdapter: &openAIMessagesExecutionAdapter{
			s:        &OpenAITextExecutor{Requests: newAuxiliaryFixture(auxiliaryFixtureInputs{}).Requests},
			c:        dsCtx,
			provider: prov,
		},
		kind: openaiexecution.NativeMessages,
	}

	unchangedLit, err := dsNative.PrepareNativeMessagesReplay(context.Background(), "deepseek-reasoner", msgLiteralOnly)
	require.NoError(t, err)
	assert.Equal(t, msgLiteralOnly, unchangedLit)

	rewrittenDS, err := dsNative.PrepareNativeMessagesReplay(context.Background(), "deepseek-reasoner", msgWithCarrier)
	require.NoError(t, err)
	assert.Equal(t, "Passback adapter thought.", gjson.GetBytes(rewrittenDS, "messages.1.content.0.thinking").String())
	assert.False(t, gjson.GetBytes(rewrittenDS, "messages.1.content.0.signature").Exists())
}
