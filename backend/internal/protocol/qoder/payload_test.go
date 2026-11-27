package qoder_test

import (
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
)

// TestRequestErrorUnwrapAndClassification wraps a *qoder.RequestError across
// multiple fmt.Errorf layers. It verifies that errors.As, IsRequestError, and
// AsRequestError preserve both the rejection Code and diagnostic Message while
// nil or unrelated errors stay distinct.
func TestRequestErrorUnwrapAndClassification(t *testing.T) {
	base := &qoder.RequestError{
		Code:    qoder.CodeUnsupportedResponseState,
		Message: "Qoder Responses requires stateless input.",
	}
	wrapped := fmt.Errorf("convert exchange: %w", fmt.Errorf("encode qoder: %w", base))

	require.True(t, qoder.IsRequestError(wrapped))
	extracted, ok := qoder.AsRequestError(wrapped)
	require.True(t, ok)
	require.Equal(t, qoder.CodeUnsupportedResponseState, extracted.Code)
	require.Equal(t, "Qoder Responses requires stateless input.", extracted.Error())

	var viaStd *qoder.RequestError
	require.ErrorAs(t, wrapped, &viaStd)
	require.Equal(t, base.Code, viaStd.Code)

	require.False(t, qoder.IsRequestError(errors.New("network timeout")))
	var nilReqErr *qoder.RequestError
	require.Empty(t, nilReqErr.Error())
	require.Equal(t, "qoder request error: missing_tool_call", (&qoder.RequestError{Code: qoder.CodeMissingToolCall}).Error())
	require.Equal(t, "qoder request error", (&qoder.RequestError{}).Error())
}

// TestAPIErrorClassificationFormattingAndRedaction constructs agent-limit and
// entitlement-denied *qoder.APIError instances with reset timestamps and wrapped
// causes. It verifies Asia/Shanghai quota reset formatting, code 112/115
// predicates, unwrap helpers, and credential masking that retains numeric codes.
func TestAPIErrorClassificationFormattingAndRedaction(t *testing.T) {
	resetMillis := time.Date(2026, time.November, 25, 4, 30, 0, 0, time.UTC).UnixMilli()
	agentErr := &qoder.APIError{
		StatusCode:          429,
		Code:                qoder.VendorCodeAgentLimit,
		Message:             "quota exceeded",
		AgentLimitResetTime: resetMillis,
	}
	require.True(t, agentErr.IsAgentLimit())
	require.False(t, agentErr.IsEntitlementDenied())
	require.Equal(t, "Qoder agent limit reached; resets at 2026-11-25 12:30:00 Asia/Shanghai", agentErr.Error())

	entErr := &qoder.APIError{
		StatusCode: 403,
		Code:       qoder.VendorCodeEntitlementDenied,
		Message:    "plan denied",
	}
	require.True(t, entErr.IsEntitlementDenied())
	require.False(t, entErr.IsAgentLimit())
	require.Equal(t, "Qoder upstream error 112: plan denied", entErr.Error())

	wrapped := fmt.Errorf("stream decode: %w", entErr)
	got, ok := qoder.AsAPIError(wrapped)
	require.True(t, ok)
	require.Equal(t, 403, got.StatusCode)

	rawSecret := `{"code":"115","message":"Authorization: Bearer secret-jwt-token cosy-key=mykey","securityOauthToken":"tok_123"}`
	redacted := qoder.RedactErrorText(rawSecret)
	require.NotContains(t, redacted, "secret-jwt-token")
	require.NotContains(t, redacted, "mykey")
	require.NotContains(t, redacted, "tok_123")
	require.Contains(t, redacted, `"code":"115"`)
}

// TestRedactErrorTextCoversAllSensitiveKeysEscapedQuotesAndNumericCodes feeds
// structured JSON, escaped string secrets, non-string secret containers, and
// plain/query key-value text into RedactErrorText. It verifies that every shared
// OAuth and Qoder credential field is masked while one-to-eight-digit vendor
// error codes remain intact across JSON and plain representations.
func TestRedactErrorTextCoversAllSensitiveKeysEscapedQuotesAndNumericCodes(t *testing.T) {
	nestedBody := `{"code":"101","message":"Authorization: Bearer secret-token securityOauthToken=dt-secret refreshToken=drt-secret cookie=session=abc uid=user-secret aid=provider-secret","data":{"securityOauthToken":"nested-secret","refreshToken":"nested-refresh","uid":"nested-user","aid":"nested-account","cosy_user":"cu-secret","cosy-user":"cu2-secret"}}`
	redactedNested := qoder.RedactErrorText(nestedBody)
	for _, secret := range []string{
		"secret-token", "dt-secret", "drt-secret", "session=abc",
		"user-secret", "provider-secret", "nested-secret", "nested-refresh",
		"nested-user", "nested-account", "cu-secret", "cu2-secret",
	} {
		require.NotContains(t, redactedNested, secret)
	}
	require.Contains(t, redactedNested, `"code":"101"`)

	for _, cnInput := range []string{
		`{"token":"cn-json-secret","message":"invalid"}`,
		`token=cn-plain-secret uid=plain-uid aid=plain-aid message=invalid`,
		`token: cn-colon-secret`,
	} {
		got := qoder.RedactErrorText(cnInput)
		require.NotContains(t, got, "secret")
		require.NotContains(t, got, "plain-uid")
		require.NotContains(t, got, "plain-aid")
		require.Contains(t, got, "***")
	}

	oauthAndEscaped := `{
		"code": 115,
		"access_token": "acc_\"quoted\"_secret",
		"id_token": "idt_secret",
		"client_secret": "cli_\"escaped\"_secret",
		"password": "pw_\"quote\"_secret",
		"authorization_code": "auth_code_secret",
		"code_verifier": "pkce_verifier_secret",
		"uid": 987654321,
		"aid": {"nested": "obj_secret"},
		"detail": "GOCSPX-abcdefghijklmnopqrstuvwx1234 and AIzaSyA12345678901234567890123456789012"
	}`
	redactedOAuth := qoder.RedactErrorText(oauthAndEscaped)
	for _, secret := range []string{
		"quoted", "idt_secret", "escaped", "pw_", "auth_code_secret",
		"pkce_verifier_secret", "987654321", "obj_secret",
		"abcdefghijklmnopqrstuvwx1234", "SyA12345678901234567890123456789012",
	} {
		require.NotContains(t, redactedOAuth, secret)
	}
	require.Contains(t, redactedOAuth, `"code":115`)
	require.Contains(t, redactedOAuth, "GOCSPX-***")
	require.Contains(t, redactedOAuth, "AIza***")

	nonJSONEscaped := `upstream error: {"password":"my\"escaped\"secret","code":"oauth_secret_code"} code=115 access_token=tok_xyz`
	redactedFrag := qoder.RedactErrorText(nonJSONEscaped)
	require.NotContains(t, redactedFrag, "escaped")
	require.NotContains(t, redactedFrag, "oauth_secret_code")
	require.NotContains(t, redactedFrag, "tok_xyz")
	require.Contains(t, redactedFrag, `"code":"***"`)
	require.Contains(t, redactedFrag, "code=115")

	require.Contains(t, qoder.RedactErrorText("code: 112 token=secret"), "code: 112")
	require.NotContains(t, qoder.RedactErrorText("code: 112 token=secret"), "secret")
}

// TestNormalizeBillingHeaderReplacesFiveHexCCH passes Anthropic billing header
// system strings and ordinary prompts containing cch= substrings into
// NormalizeBillingHeader. It verifies that five-hex billing checksums normalize
// to cch=<cch>; while surrounding metadata and non-header prompts stay intact.
func TestNormalizeBillingHeaderReplacesFiveHexCCH(t *testing.T) {
	input := "x-anthropic-billing-header: cc_version=2.1.330; cc_entrypoint=cli; cch=a1b2F;\nYou are Claude Code."
	want := "x-anthropic-billing-header: cc_version=2.1.330; cc_entrypoint=cli; cch=<cch>;\nYou are Claude Code."
	require.Equal(t, want, qoder.NormalizeBillingHeader(input))

	req := &qoder.PayloadRequest{Model: "claude-opus-4-6", System: input}
	require.Equal(t, want, req.NormalizedSystem())

	nonHeader := "ordinary prompt with cch=12345; outside billing header"
	require.Equal(t, nonHeader, qoder.NormalizeBillingHeader(nonHeader))
}

// TestDeriveThinkingDirectivePrecedence evaluates combinations of reasoning
// modes, effort aliases, token budgets, and unknown effort labels through
// DeriveThinkingDirective. It verifies that explicit disable signals override
// enabling inputs and recognized active efforts precede budget fallbacks.
func TestDeriveThinkingDirectivePrecedence(t *testing.T) {
	i64 := func(v int64) *int64 { return &v }
	cases := []struct {
		name   string                  // subtest scenario label
		mode   string                  // input reasoning mode string
		effort string                  // input reasoning effort string
		budget *int64                  // optional explicit thinking token budget
		want   qoder.ThinkingDirective // expected derived thinking directive
	}{
		{name: "empty unspecified", want: qoder.ThinkingDirective{}},
		{name: "minimal maps low", effort: "minimal", want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "low"}},
		{name: "low case insensitive", effort: "LOW", want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "low"}},
		{name: "medium preserved", effort: "medium", want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "medium"}},
		{name: "high preserved", effort: "high", want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "high"}},
		{name: "very_high maps xhigh", effort: "very_high", want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "xhigh"}},
		{name: "extra-high maps xhigh", effort: "extra-high", want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "xhigh"}},
		{name: "positive budget maps max", budget: i64(1), want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "max"}},
		{name: "zero budget stays unspecified", budget: i64(0), want: qoder.ThinkingDirective{}},
		{name: "enabled mode uses default", mode: "enabled", want: qoder.ThinkingDirective{Specified: true, Enabled: true}},
		{name: "adaptive mode uses default", mode: "adaptive", want: qoder.ThinkingDirective{Specified: true, Enabled: true}},
		{name: "effort beats positive budget", mode: "enabled", effort: "low", budget: i64(32768), want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "low"}},
		{name: "disabled mode beats max effort and budget", mode: "disabled", effort: "max", budget: i64(32768), want: qoder.ThinkingDirective{Specified: true}},
		{name: "none effort beats enabled mode and budget", mode: "enabled", effort: "none", budget: i64(32768), want: qoder.ThinkingDirective{Specified: true}},
		{name: "unknown effort falls back to positive budget", effort: "banana", budget: i64(8), want: qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "max"}},
		{name: "ultra effort alone stays unspecified", effort: "ultra", want: qoder.ThinkingDirective{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := qoder.DeriveThinkingDirective(tc.mode, tc.effort, tc.budget)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestRestrictToolsAndWireValue applies valid and invalid allowed_tools subsets
// onto declared Qoder functions via RestrictAllowedTools and RestrictTools. It
// verifies declaration-order filtering without caller mutation, desktop
// WireValue formatting, and *qoder.RequestError rejection on undeclared names.
func TestRestrictToolsAndWireValue(t *testing.T) {
	tools := []qoder.Tool{
		{Type: "function", Function: qoder.ToolFunction{Name: "first", Parameters: jsontext.Value(`{"type":"object"}`)}},
		{Type: "function", Function: qoder.ToolFunction{Name: "second", Parameters: jsontext.Value(`{"type":"object"}`)}},
		{Type: "function", Function: qoder.ToolFunction{Name: "third", Parameters: jsontext.Value(`{"type":"object"}`)}},
	}

	req := &qoder.PayloadRequest{
		Model: "qwen3.8-max",
		Tools: tools,
		ToolChoice: &qoder.ToolChoice{
			Mode:         qoder.ToolChoiceRequired,
			AllowedTools: []string{"third", "first"},
		},
	}
	restricted, err := req.RestrictAllowedTools()
	require.NoError(t, err)
	require.Len(t, restricted.Tools, 2)
	require.Equal(t, "first", restricted.Tools[0].Function.Name)
	require.Equal(t, "third", restricted.Tools[1].Function.Name)
	require.Equal(t, "required", restricted.ToolChoice.WireValue())
	require.Len(t, req.Tools, 3, "RestrictAllowedTools must not mutate original PayloadRequest")

	fnChoice := &qoder.ToolChoice{Mode: qoder.ToolChoiceFunction, FunctionName: "second"}
	require.Equal(t, map[string]any{
		"type":     "function",
		"function": map[string]any{"name": "second"},
	}, fnChoice.WireValue())

	_, _, err = qoder.RestrictTools(tools, &qoder.ToolChoice{
		Mode:         qoder.ToolChoiceAuto,
		AllowedTools: []string{"missing_fn"},
	})
	require.True(t, qoder.IsRequestError(err))

	_, _, err = qoder.RestrictTools(tools, &qoder.ToolChoice{
		Mode:         qoder.ToolChoiceNone,
		AllowedTools: []string{"first"},
	})
	require.True(t, qoder.IsRequestError(err))
}

// TestPayloadRequestDeterministicMarshalDecodeAndClone serializes, decodes, and
// clones a populated PayloadRequest carrying session hints, cache breakpoints,
// tool calls, and schemas. It verifies byte-identical marshaling, session ID
// precedence, and deep-copy isolation when mutating nested slices and pointers.
func TestPayloadRequestDeterministicMarshalDecodeAndClone(t *testing.T) {
	strict := true
	orig := &qoder.PayloadRequest{
		Model:          "claude-opus-4-6",
		System:         "be concise",
		SessionID:      "  sess_primary  ",
		ConversationID: "conv_secondary",
		CacheKey:       "pck_1",
		MetadataUserID: "user_42",
		MaxTokens:      2048,
		Thinking:       qoder.ThinkingDirective{Specified: true, Enabled: true, Effort: "high"},
		Messages: []qoder.Message{
			{
				Role: "user",
				Text: "hello",
				TextBlocks: []qoder.TextBlock{{
					Type:         "text",
					Text:         "hello",
					CacheControl: &qoder.CacheControl{Type: "ephemeral", TTL: "5m"},
				}},
			},
			{
				Role: "assistant",
				ToolCalls: []qoder.ToolCall{{
					ID:       "call_1",
					Type:     "function",
					Function: qoder.ToolCallFunction{Name: "Read", Arguments: `{"path":"a.go"}`},
				}},
			},
			{
				Role:         "tool",
				Text:         "(empty)",
				ToolCallID:   "call_1",
				Name:         "Read",
				EmptyContent: true,
			},
		},
		Tools: []qoder.Tool{{
			Type: "function",
			Function: qoder.ToolFunction{
				Name:        "Read",
				Description: "Read file",
				Parameters:  jsontext.Value(`{"type":"object","properties":{"path":{"type":"string"}}}`),
				Strict:      &strict,
			},
		}},
		ToolChoice: &qoder.ToolChoice{Mode: qoder.ToolChoiceAuto, AllowedTools: []string{"Read"}},
	}

	require.Equal(t, "sess_primary", orig.ExplicitSession())
	onlyConv := &qoder.PayloadRequest{Model: "m", ConversationID: "  conv_only  "}
	require.Equal(t, "conv_only", onlyConv.ExplicitSession())

	bytesA, err := orig.MarshalJSONBytes()
	require.NoError(t, err)
	bytesB, err := orig.MarshalJSONBytes()
	require.NoError(t, err)
	require.Equal(t, bytesA, bytesB)

	decoded, err := qoder.DecodePayload(bytesA)
	require.NoError(t, err)
	require.Equal(t, orig, decoded)

	cloned := orig.Clone()
	cloned.Messages[0].TextBlocks[0].CacheControl.TTL = "1h"
	cloned.Tools[0].Function.Parameters[0] = '['
	*cloned.Tools[0].Function.Strict = false
	cloned.ToolChoice.AllowedTools[0] = "Other"

	require.Equal(t, "5m", orig.Messages[0].TextBlocks[0].CacheControl.TTL)
	require.Equal(t, byte('{'), orig.Tools[0].Function.Parameters[0])
	require.True(t, *orig.Tools[0].Function.Strict)
	require.Equal(t, "Read", orig.ToolChoice.AllowedTools[0])
}
