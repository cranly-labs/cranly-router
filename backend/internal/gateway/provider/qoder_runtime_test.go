package provider

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/qoder"
)

// qoderRuntimeStubClient adapts a function into a qoder.StreamClient for runtime unit tests.
type qoderRuntimeStubClient func(context.Context, *qoder.SessionContext, string, []byte, map[string]string) (*http.Response, error)

// qoderNoopOutputSink discards output headers and payload bytes during runtime target tests.
type qoderNoopOutputSink struct{}

// TestQoderRuntime_PrepareQoderTarget exercises QoderRuntime target preparation
// with model attributes and effort_model_mapping configured. It verifies
// ConversionDialect AllowBillingHeader and StripEffort injection and legacy
// Responses messages ingress normalization.
func TestQoderRuntime_PrepareQoderTarget(t *testing.T) {
	t.Run("injects ConversionDialect with pre-mapped StripEffort", func(t *testing.T) {
		row := qoderAvailabilityRecord(17, "global", []map[string]any{
			{
				"key":              "ultimate",
				"alias":            "ultimate",
				"name":             "Ultimate",
				"enabled":          true,
				"reasoning":        true,
				"can_disable":      true,
				"default_thinking": true,
				"default_effort":   "high",
				"efforts":          []string{"low", "medium", "high"},
				"input_limit":      1000000,
			},
		})
		row.Credentials[providercore.EffortModelMappingCredentialKey] = map[string]any{
			"gpt-5": map[string]any{
				"high": "ultimate",
			},
		}
		row.Credentials[providercore.EffortModelMappingStripEffortCredentialKey] = true
		catalogExtra, ok := row.Extra[providercore.QoderCatalogExtraKey].(map[string]any)
		require.True(t, ok)
		catalogExtra["identity"] = providercore.QoderCatalogIdentity(&row)
		row.PrepareQoderCatalog()

		var attrConfig routing.ModelAttributeConfig
		require.NoError(t, json.Unmarshal([]byte(`{
			"status":"active",
			"rules":[{
				"models":["ultimate"],
				"match":{"upstream_protocol":"qoder_chat"},
				"attributes":{"temperature":false}
			}]
		}`), &attrConfig))
		service := &routing.ModelAttributeService{
			Repo: &mediaAttributeRepo{config: &attrConfig},
		}
		tokens := provideradapter.NewQoderTokenProvider(qoder.SessionBuilder{})
		tokens.Core.Sessions = map[int64]providercore.QoderSessionCacheEntry[*qoder.SessionContext]{
			row.ID: {
				CredentialsHash: providercore.QoderCredentialsHash(row.Credentials),
				Session:         &qoder.SessionContext{Identity: &qoder.AuthIdentity{SecurityOauthToken: "tok"}},
			},
		}

		var sentPayload map[string]any
		runtime := NewQoderRuntime(QoderRuntimeOptions{
			Attributes: service,
			Tokens:     tokens,
			Client: qoderRuntimeStubClient(func(_ context.Context, _ *qoder.SessionContext, _ string, body []byte, _ map[string]string) (*http.Response, error) {
				require.NoError(t, json.Unmarshal(body, &sentPayload))
				sseBody := "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"ok\\\"}}]}\"}\n\ndata: {\"body\":\"[DONE]\"}\n\n"
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(sseBody))}, nil
			}),
		})

		ctx := requeststate.WithGroup(t.Context(), &routing.Group{ID: 7})
		body := []byte(`{
			"model":"gpt-5",
			"reasoning":{"effort":"high"},
			"temperature":0.7,
			"stream":true,
			"instructions":"x-anthropic-billing-header: cc_version=2.1.0; cch=abc12;\nKeep billing header.",
			"input":"hello"
		}`)

		exec, input := runtime.PrepareQoderTarget(qoder.RequestMetadata{}, &row, body, protocol.ProtocolOpenAIResponses, "gpt-5")
		target, ok := input.Target.(*qoder.Target)
		require.True(t, ok)
		require.NotNil(t, target.Dialect)

		dialect, err := target.Dialect(ctx, "ultimate", protocol.ProtocolOpenAIResponses)
		require.NoError(t, err)
		require.True(t, dialect.AllowBillingHeader)
		require.True(t, dialect.StripEffort, "effort consumed by effort_model_mapping must set StripEffort")
		require.True(t, dialect.DropTemperature)

		res, err := exec.Execute(ctx, input, qoderNoopOutputSink{})
		require.NoError(t, err)
		require.True(t, res.Served)
		require.Equal(t, "ultimate", res.UpstreamModel)

		messages, _ := sentPayload["messages"].([]any)
		require.NotEmpty(t, messages)
		firstMsg, _ := messages[0].(map[string]any)
		require.Equal(t, "system", firstMsg["role"])
		require.Contains(t, firstMsg["content"], "cch=<cch>;")
	})

	t.Run("normalizes legacy Responses messages ingress and surfaces invalid legacy messages as RequestError", func(t *testing.T) {
		row := qoderAvailabilityRecord(18, "global", []map[string]any{
			{"key": "auto", "alias": "auto", "name": "Auto", "enabled": true, "input_limit": 200000},
		})
		row.PrepareQoderCatalog()

		tokens := provideradapter.NewQoderTokenProvider(qoder.SessionBuilder{})
		tokens.Core.Sessions = map[int64]providercore.QoderSessionCacheEntry[*qoder.SessionContext]{
			row.ID: {
				CredentialsHash: providercore.QoderCredentialsHash(row.Credentials),
				Session:         &qoder.SessionContext{Identity: &qoder.AuthIdentity{SecurityOauthToken: "tok"}},
			},
		}

		var sentPayload map[string]any
		runtime := NewQoderRuntime(QoderRuntimeOptions{
			Tokens: tokens,
			Client: qoderRuntimeStubClient(func(_ context.Context, _ *qoder.SessionContext, _ string, body []byte, _ map[string]string) (*http.Response, error) {
				require.NoError(t, json.Unmarshal(body, &sentPayload))
				sseBody := "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"ok\\\"}}]}\"}\n\ndata: {\"body\":\"[DONE]\"}\n\n"
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(sseBody))}, nil
			}),
		})

		legacyBody := []byte(`{"model":"auto","messages":[{"role":"user","content":"legacy responses chat"}],"commands":["noop"]}`)
		exec, input := runtime.PrepareQoderTarget(qoder.RequestMetadata{}, &row, legacyBody, protocol.ProtocolOpenAIResponses, "auto")
		res, err := exec.Execute(t.Context(), input, qoderNoopOutputSink{})
		require.NoError(t, err)
		require.True(t, res.Served)
		chatCtx, _ := sentPayload["chat_context"].(map[string]any)
		require.Equal(t, "legacy responses chat", chatCtx["text"])

		badLegacy := []byte(`{"model":"auto","messages":"not-an-array"}`)
		execBad, inputBad := runtime.PrepareQoderTarget(qoder.RequestMetadata{}, &row, badLegacy, protocol.ProtocolOpenAIResponses, "auto")
		_, badErr := execBad.Execute(t.Context(), inputBad, qoderNoopOutputSink{})
		var reqErr *qoder.RequestError
		require.ErrorAs(t, badErr, &reqErr)
	})
}

// StreamRequestContext invokes f directly with the supplied Qoder session, path, payload, and headers.
func (f qoderRuntimeStubClient) StreamRequestContext(ctx context.Context, s *qoder.SessionContext, path string, body []byte, headers map[string]string) (*http.Response, error) {
	return f(ctx, s, path, body, headers)
}

// Begin accepts downstream response metadata without performing I/O during runtime target tests.
func (qoderNoopOutputSink) Begin(upstream.OutputHead) error { return nil }

// Emit discards downstream response payload bytes without performing I/O during runtime target tests.
func (qoderNoopOutputSink) Emit(upstream.OutputEvent) error { return nil }
