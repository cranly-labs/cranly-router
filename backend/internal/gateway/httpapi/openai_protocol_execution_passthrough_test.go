package httpapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/billing"
	"github.com/TokenFlux/TokenRouter/internal/gateway/compact"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// TestOpenAIPassthroughCompactFallbackSecondStreamFailureUsesStandardErrorPath
// returns context-window failures from both initial and Compact retry streams.
// It checks two requests, final HTTP 400, no remaining Compact signal and
// ordered retry/error Ops events.
func TestOpenAIPassthroughCompactFallbackSecondStreamFailureUsesStandardErrorPath(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","stream":true,"input":[{"type":"compaction_trigger"}]}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	c.Request.Header.Set("Content-Type", "application/json")
	MarkOpenAINativeCompactionV2(c)

	failed := "event: response.failed\n" +
		`data: {"type":"response.failed","response":{"status":"failed","error":{"code":"context_length_exceeded","message":"context window exceeded"}}}` + "\n\n"
	upstream := &auxiliaryHTTPRecorder{responses: []*http.Response{
		{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(failed))},
		{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(failed))},
	}}
	svc := newResponsesFixture(responsesFixtureInputs{compactModel: "gpt-5.4", transport: upstream})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation, ID: 1, Name: "openai-oauth", Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeOAuth, Concurrency: 1,
			Credentials: map[string]any{"access_token": "oauth-token", "chatgpt_account_id": "chatgpt-provider"},
			Status:      billing.StatusActive, Schedulable: true,
		},
	}

	result, err := svc.Text.Passthrough(
		context.Background(), c, provider, body, body, "gpt-5.5", false, nil, true, time.Now(),
	)

	require.Error(t, err)
	require.Nil(t, result)
	require.Len(t, upstream.bodies, 2)
	var compactSignal *compact.Failure
	require.False(t, errors.As(err, &compactSignal))
	require.Equal(t, http.StatusBadRequest, recorder.Code)
	require.Contains(t, recorder.Body.String(), "context window exceeded")
	rawEvents, ok := c.Get(OpsUpstreamErrorsKey)
	require.True(t, ok)
	events, ok := rawEvents.([]*ops.OpsUpstreamErrorEvent)
	require.True(t, ok)
	require.Len(t, events, 2)
	require.Equal(t, "retry", events[0].Kind)
	require.Equal(t, "compact_model_fallback", events[0].Reason)
	require.Equal(t, "http_error", events[1].Kind)
	require.True(t, events[1].Passthrough)
}

// TestOpenAIPassthroughAPIKeyRestoresClientToolsNonStreaming lowers client
// custom tools for a native API-key response. It checks outgoing declarations
// and restored custom call types and inputs in buffered JSON.
func TestOpenAIPassthroughAPIKeyRestoresClientToolsNonStreaming(t *testing.T) {
	body := openAIClientToolsRequest(false)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(`{"id":"resp_tools","status":"completed","output":[
			{"type":"function_call","id":"i1","call_id":"c1","name":"exec","arguments":"{\"input\":\"pwd\"}"},
			{"type":"function_call","id":"i2","call_id":"c2","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\"}"}],"usage":{}}`)),
	}}
	svc := openAIClientToolsTestService(upstream)
	provider := &gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, ID: 5659, Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey, Credentials: map[string]any{"api_key": "test-key"}}}

	result, err := svc.Text.Passthrough(context.Background(), c, provider, body, body, "gpt-5.4", false, nil, false, time.Now())

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result.UpstreamResponseModel)
	assertOpenAIClientToolsLowered(t, upstream.lastBody)
	require.Equal(t, "custom_tool_call", gjson.Get(recorder.Body.String(), "output.0.type").String())
	require.Equal(t, "pwd", gjson.Get(recorder.Body.String(), "output.0.input").String())
	require.Equal(t, "custom_tool_call", gjson.Get(recorder.Body.String(), "output.1.type").String())
	require.Equal(t, "*** Begin Patch", gjson.Get(recorder.Body.String(), "output.1.input").String())
}

// TestOpenAIPassthroughAPIKeyPreservesCustomToolOutputContentParts supplies
// text and file parts in a custom tool result. It checks conversion to
// function_call_output with the upstream output array and its field values
// retained.
func TestOpenAIPassthroughAPIKeyPreservesCustomToolOutputContentParts(t *testing.T) {
	body := []byte(`{"model":"gpt-5.4","stream":false,"tools":[{"type":"custom","name":"exec"}],"input":[{"type":"custom_tool_call_output","call_id":"call_1","output":[{"type":"input_text","text":"result"},{"type":"input_file","file_id":"file_123"}]}]}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))

	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"id":"resp_tools","status":"completed","output":[],"usage":{}}`)),
	}}
	svc := openAIClientToolsTestService(upstream)
	provider := &gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, ID: 6240, Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey, Credentials: map[string]any{"api_key": "test-key"}}}

	result, err := svc.Text.Passthrough(context.Background(), c, provider, body, body, "gpt-5.4", false, nil, false, time.Now())

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "function_call_output", gjson.GetBytes(upstream.lastBody, "input.0.type").String())
	output := gjson.GetBytes(upstream.lastBody, "input.0.output")
	require.True(t, output.IsArray(), "native Responses content parts must reach the upstream as an array")
	require.Equal(t, "input_text", output.Get("0.type").String())
	require.Equal(t, "result", output.Get("0.text").String())
	require.Equal(t, "input_file", output.Get("1.type").String())
	require.Equal(t, "file_123", output.Get("1.file_id").String())
}

// TestOpenAIPassthroughAPIKeyRestoresClientToolsStreaming returns
// function-call stream events for a lowered custom tool. It checks restored
// custom-call events and string input, plus the final observed runtime model.
func TestOpenAIPassthroughAPIKeyRestoresClientToolsStreaming(t *testing.T) {
	body := openAIClientToolsRequest(true)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))

	sse := strings.Join([]string{
		`data: {"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"type":"function_call","id":"i1","call_id":"c1","name":"apply_patch","status":"in_progress"}}`,
		`data: {"type":"response.function_call_arguments.done","sequence_number":1,"item_id":"i1","call_id":"c1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\"}"}`,
		`data: {"type":"response.output_item.done","sequence_number":2,"output_index":0,"item":{"type":"function_call","id":"i1","call_id":"c1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\"}","status":"completed"}}`,
		`data: {"type":"response.completed","sequence_number":3,"response":{"id":"resp_stream_tools","model":"runtime-version","status":"completed","output":[{"type":"function_call","id":"i1","call_id":"c1","name":"apply_patch","arguments":"{\"input\":\"*** Begin Patch\"}"}],"usage":{"input_tokens":1,"output_tokens":1}}}`,
	}, "\n\n") + "\n\n"
	upstream := &auxiliaryHTTPRecorder{resp: &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}}
	svc := openAIClientToolsTestService(upstream)
	provider := &gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, ID: 5660, Platform: capability.PlatformOpenAI, Type: capability.ProviderTypeAPIKey, Credentials: map[string]any{"api_key": "test-key"}}}

	result, err := svc.Text.Passthrough(context.Background(), c, provider, body, body, "gpt-5.4", false, nil, true, time.Now())

	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "runtime-version", result.UpstreamResponseModel)
	assertOpenAIClientToolsLowered(t, upstream.lastBody)
	output := recorder.Body.String()
	require.Contains(t, output, `"type":"custom_tool_call"`)
	require.Contains(t, output, `"type":"response.custom_tool_call_input.done"`)
	require.Contains(t, output, `"input":"*** Begin Patch"`)
	require.NotContains(t, output, `"input":{`)
}

// TestOpenAIUpstreamAccessStateClassification compares structured access-state
// codes with the listed free-form messages. It checks accepted cases'
// credential failure scope, next-provider action and client presentation
// alongside rejected classifications.
func TestOpenAIUpstreamAccessStateClassification(t *testing.T) {
	tests := []struct {
		name string // fixture case name
		body string // upstream error payload
		want bool   // expected access-state classification
	}{
		{"workspace_code", `{"detail":{"code":"deactivated_workspace"}}`, true},
		{"disabled_provider_message", `{"error":{"message":"Your provider is disabled"}}`, false},
		{"suspended_workspace_message", `{"response":{"error":{"message":"This workspace has been suspended"}}}`, false},
		{"deactivated_organization_message", `{"detail":{"message":"The organization is deactivated"}}`, false},
		{"scalar_detail", `{"detail":"This workspace has been disabled"}`, false},
		{"suspended_org_code", `{"error":{"code":"org_suspended"}}`, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body := []byte(tt.body)
			require.Equal(t, tt.want, gatewayprovider.IsOpenAIUpstreamAccessStateError("", body))
			if !tt.want {
				return
			}
			require.True(t, gatewayprovider.ShouldFailoverOpenAIResponse(http.StatusForbidden, "", body))
			require.True(t, shouldFailoverOpenAIPassthroughResponse(&gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Type: capability.ProviderTypeOAuth}}, http.StatusForbidden, body))

			err := gatewayprovider.NewOpenAIUpstreamFailure(http.StatusForbidden, nil, body, "", true)
			require.True(t, err.IsCredentialFailure())
			require.Equal(t, forwardcore.GatewayFailureScopeProvider, err.Scope)
			require.Equal(t, forwardcore.OpenAIUpstreamAccessStateReason, err.Reason)
			require.Equal(t, forwardcore.NextProviderRetry, err.NextProviderAction)
			require.False(t, err.RetryableOnSameProvider)
			require.False(t, err.RequestScopedTransient)
			require.Equal(t, http.StatusBadGateway, err.ClientStatusCode)
			require.Equal(t, "Upstream access is temporarily unavailable, please retry later", err.ClientMessage)
		})
	}
}

// TestOpenAIUpstreamAccessStateDoesNotScanEchoedJSON puts disabled-provider
// wording in an echo beside an ordinary invalid-input error. It checks that
// access-state and passthrough failover classification reject that unrelated
// text.
func TestOpenAIUpstreamAccessStateDoesNotScanEchoedJSON(t *testing.T) {
	body := []byte(`{"error":{"code":"invalid_request_error","message":"Invalid input"},"echo":{"prompt":"my provider is disabled"}}`)
	require.False(t, gatewayprovider.IsOpenAIUpstreamAccessStateError("", body))
	require.False(t, shouldFailoverOpenAIPassthroughResponse(&gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Type: capability.ProviderTypeOAuth}}, http.StatusBadRequest, body))
}

// TestOpenAIHTTPAccessStateDoesNotTrustBadRequestMessage mentions account
// disabling inside an unknown-parameter HTTP 400. It checks absence of
// access-state, failover and credential-failure classification for this
// request rejection.
func TestOpenAIHTTPAccessStateDoesNotTrustBadRequestMessage(t *testing.T) {
	body := []byte(`{"error":{"type":"invalid_request_error","code":"unknown_parameter","message":"Unknown parameter: account disabled"}}`)

	require.False(t, gatewayprovider.IsOpenAIUpstreamAccessStateError("", body), "free-form stream messages are not durable provider evidence")
	require.False(t, gatewayprovider.IsOpenAIHTTPUpstreamAccessStateError(http.StatusBadRequest, "", body))
	require.False(t, gatewayprovider.ShouldFailoverOpenAIResponse(http.StatusBadRequest, "", body))
	require.False(t, shouldFailoverOpenAIPassthroughResponse(&gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Type: capability.ProviderTypeOAuth}}, http.StatusBadRequest, body))

	err := gatewayprovider.NewOpenAIUpstreamFailure(http.StatusBadRequest, nil, body, "", false)
	require.False(t, err.IsCredentialFailure())
}

// TestOpenAICyberPolicyWrapped5xxNeverFailsOver wraps a cyber-policy code
// inside an HTTP 502 failure. It checks that both general Responses and OAuth
// passthrough classifiers decline provider failover.
func TestOpenAICyberPolicyWrapped5xxNeverFailsOver(t *testing.T) {
	body := []byte(`{"error":{"code":"cyber_policy","message":"blocked"}}`)

	require.False(t, gatewayprovider.ShouldFailoverOpenAIResponse(http.StatusBadGateway, "wrapped upstream failure", body))
	require.False(t, shouldFailoverOpenAIPassthroughResponse(&gatewayprovider.ExecutionProvider{Record: providercore.Record{LoadLocation: time.LoadLocation, Type: capability.ProviderTypeOAuth}}, http.StatusBadGateway, body))
}

// TestOpenAIPassthroughAPIKey_IRClientToolsStreamingBufferedAndLocalRejection
// runs OpenAI API Key HTTP passthrough requests across buffered and streaming
// responses. It verifies PrepareNativeReplay carrier unpacking,
// CodexToolNameReverse alias restoration, and local HTTP 400 tool-conflict
// responses that skip failover and health penalties.
func TestOpenAIPassthroughAPIKey_IRClientToolsStreamingBufferedAndLocalRejection(t *testing.T) {
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_pass_acct_5659",
		ProviderID: 5659,
	})
	require.NoError(t, err)

	reqBody := func(stream bool) []byte {
		streamText := "false"
		if stream {
			streamText = "true"
		}
		return []byte(`{
			"model":"gpt-5.4",
			"stream":` + streamText + `,
			"store":false,
			"nonce":9007199254740993,
			"tools":[
				{"type":"custom","name":"exec","description":"Execute command"},
				{"type":"tool_search","execution":"client"},
				{"type":"namespace","name":"nb","tools":[{"type":"function","name":"python__tokenrouter","parameters":{"type":"object"}}]}
			],
			"input":[
				{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"plan"}],"encrypted_content":"` + carrier + `"},
				{"type":"message","role":"user","content":"run tools"}
			]
		}`)
	}

	makeProvider := func() *gatewayprovider.ExecutionProvider {
		return &gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				LoadLocation: time.LoadLocation,
				ID:           5659,
				Name:         "openai-apikey-pass",
				Platform:     capability.PlatformOpenAI,
				Type:         capability.ProviderTypeAPIKey,
				Concurrency:  1,
				Status:       billing.StatusActive,
				Schedulable:  true,
				Credentials:  map[string]any{"api_key": "sk-test", "base_url": "https://api.openai.com/v1"},
				Extra:        map[string]any{"openai_passthrough": true},
			},
		}
	}

	t.Run("buffered", func(t *testing.T) {
		body := reqBody(false)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
		SetCodexToolNameReverse(c, map[string]string{"python__tokenrouter": "python"})

		upResp := `{"id":"resp_buf","status":"completed","trace":9007199254740993111,"output":[` +
			`{"type":"function_call","id":"fc_1","call_id":"c1","name":"exec","arguments":"{\"input\":\"ls\"}"},` +
			`{"type":"function_call","id":"fc_2","call_id":"c2","name":"tool_search","arguments":"{\"query\":\"nb\"}"},` +
			`{"type":"function_call","id":"fc_3","call_id":"c3","name":"nb__python__tokenrouter","arguments":"{}"}` +
			`],"usage":{"input_tokens":5,"output_tokens":3}}`
		up := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(upResp)),
		}}
		svc := openAIClientToolsTestService(up)

		res, pErr := svc.Text.Passthrough(context.Background(), c, makeProvider(), body, body, "gpt-5.4", false, nil, false, time.Now())
		require.NoError(t, pErr)
		require.NotNil(t, res)
		require.Equal(t, "function", gjson.GetBytes(up.lastBody, "tools.0.type").String())
		require.Equal(t, "exec", gjson.GetBytes(up.lastBody, "tools.0.name").String())
		require.Equal(t, "tool_search", gjson.GetBytes(up.lastBody, "tools.1.name").String())
		require.Equal(t, "nb__python__tokenrouter", gjson.GetBytes(up.lastBody, "tools.2.name").String())
		require.Equal(t, "9007199254740993", gjson.GetBytes(up.lastBody, "nonce").Raw)
		require.Equal(t, "gAAAA_pass_acct_5659", gjson.GetBytes(up.lastBody, "input.0.encrypted_content").String())
		require.Equal(t, "9007199254740993111", gjson.Get(rec.Body.String(), "trace").Raw)
		require.Equal(t, "custom_tool_call", gjson.Get(rec.Body.String(), "output.0.type").String())
		require.Equal(t, "ls", gjson.Get(rec.Body.String(), "output.0.input").String())
		require.Equal(t, "tool_search_call", gjson.Get(rec.Body.String(), "output.1.type").String())
		require.Equal(t, "nb", gjson.Get(rec.Body.String(), "output.2.namespace").String())
		require.Equal(t, "python", gjson.Get(rec.Body.String(), "output.2.name").String())
	})

	t.Run("streaming", func(t *testing.T) {
		body := reqBody(true)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(body))
		SetCodexToolNameReverse(c, map[string]string{"python__tokenrouter": "python"})

		sse := strings.Join([]string{
			`: keepalive`,
			`event: ping`,
			``,
			`data: {"type":"response.output_item.added","sequence_number":0,"output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"c1","name":"exec","status":"in_progress"}}`,
			``,
			`data: {"type":"response.function_call_arguments.delta","sequence_number":1,"output_index":0,"item_id":"fc_1","delta":"{\"input\":"}`,
			``,
			`data: {"type":"response.function_call_arguments.done","sequence_number":2,"output_index":0,"item_id":"fc_1","name":"exec","arguments":"{\"input\":\"pwd\"}"}`,
			``,
			`data: {"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"c1","name":"exec","arguments":"{\"input\":\"pwd\"}","status":"completed"}}`,
			``,
			`data: {"type":"response.output_item.done","sequence_number":4,"output_index":1,"item":{"type":"function_call","id":"fc_2","call_id":"c2","name":"nb__python__tokenrouter","arguments":"{}","status":"completed"}}`,
			``,
			`data: {"type":"response.completed","sequence_number":5,"response":{"id":"resp_stream","model":"gpt-5.4-2026","status":"completed","output":[{"type":"function_call","id":"fc_1","call_id":"c1","name":"exec","arguments":"{\"input\":\"pwd\"}"},{"type":"function_call","id":"fc_2","call_id":"c2","name":"nb__python__tokenrouter","arguments":"{}"}],"usage":{"input_tokens":4,"output_tokens":2}}}`,
			``,
		}, "\n")
		up := &auxiliaryHTTPRecorder{resp: &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body:       io.NopCloser(strings.NewReader(sse)),
		}}
		svc := openAIClientToolsTestService(up)

		res, pErr := svc.Text.Passthrough(context.Background(), c, makeProvider(), body, body, "gpt-5.4", false, nil, true, time.Now())
		require.NoError(t, pErr)
		require.NotNil(t, res)
		require.Equal(t, "gpt-5.4-2026", res.UpstreamResponseModel)
		out := rec.Body.String()
		require.NotContains(t, out, "event: ping")
		require.Contains(t, out, `"type":"custom_tool_call"`)
		require.Contains(t, out, `"type":"response.custom_tool_call_input.done"`)
		require.Contains(t, out, `"input":"pwd"`)
		require.Contains(t, out, `"namespace":"nb"`)
		require.Contains(t, out, `"name":"python"`)
	})

	t.Run("ir_tool_conflict_returns_local_400_without_failover_or_penalty", func(t *testing.T) {
		conflictBody := []byte(`{
			"model":"gpt-5.4",
			"stream":false,
			"tools":[
				{"type":"function","name":"db__query","parameters":{"type":"object"}},
				{"type":"namespace","name":"db","tools":[{"type":"function","name":"query","parameters":{"type":"object"}}]}
			],
			"input":"hi"
		}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(conflictBody))

		up := &auxiliaryHTTPRecorder{}
		svc := openAIClientToolsTestService(up)

		res, pErr := svc.Text.Passthrough(context.Background(), c, makeProvider(), conflictBody, conflictBody, "gpt-5.4", false, nil, false, time.Now())
		require.Error(t, pErr)
		require.Nil(t, res)
		require.True(t, routing.IsModelAttributeError(pErr), "tool conflict must be marked as a local model-capability error to skip schedule penalty")
		var failoverErr *forwardcore.UpstreamFailoverError
		require.False(t, errors.As(pErr, &failoverErr))
		require.True(t, IsResponseCommitted(c))
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Equal(t, "invalid_request_error", gjson.Get(rec.Body.String(), "error.type").String())
		require.Equal(t, "tools", gjson.Get(rec.Body.String(), "error.param").String())
		require.Empty(t, up.bodies)
	})
}
