package antigravity

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// decodedRecoveryEnvelope captures v1internal request fields sent during
// RecoverClaudeIR retries without gjson or map[string]any.
type decodedRecoveryEnvelope struct {
	Project     string                 `json:"project"`     // v1internal GCP project ID
	Model       string                 `json:"model"`       // mapped Antigravity model ID
	UserAgent   string                 `json:"userAgent"`   // fixed antigravity userAgent
	RequestType string                 `json:"requestType"` // fixed agent requestType
	Request     decodedRecoveryRequest `json:"request"`     // inner Gemini request payload
}

// decodedRecoveryRequest decodes inner Gemini fields inspected by recovery tests.
type decodedRecoveryRequest struct {
	SystemInstruction jsontext.Value `json:"systemInstruction"` // injected systemInstruction JSON
	Contents          jsontext.Value `json:"contents"`          // serialized conversation contents
	Tools             jsontext.Value `json:"tools"`             // serialized tool declarations
	GenerationConfig  jsontext.Value `json:"generationConfig"`  // serialized generationConfig JSON
}

// decodeTestRecoveryEnvelope unmarshals raw v1internal request JSON bytes into decodedRecoveryEnvelope.
func decodeTestRecoveryEnvelope(t *testing.T, raw []byte) decodedRecoveryEnvelope {
	t.Helper()
	var env decodedRecoveryEnvelope
	dec := jsontext.NewDecoder(bytes.NewReader(raw))
	require.NoError(t, json.UnmarshalDecode(dec, &env))
	return env
}

// TestRecoverClaudeIR_ThinkingOnlyStagePreservesToolsExchangeAndCarrierSealing
// triggers an HTTP 400 signature error on an OpenAI Responses request carrying a
// custom apply_patch tool, an encrypted-only trc1:antigravity_claude carrier,
// and a 64-bit integer argument. Exchange.ReencodeUpstream must preserve the
// 64-bit integer literal before v1internal envelope wrapping, while stage-one
// RecoverClaudeIR unpacks reasoning text into plain text, omits thinkingConfig,
// retains structured tool calls without text degradation, honors custom
// IdentityPatchPrompt, and keeps the original Exchange intact for response conversion.
func TestRecoverClaudeIR_ThinkingOnlyStagePreservesToolsExchangeAndCarrierSealing(t *testing.T) {
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAntigravityClaude,
		Data:   "stale_sonnet_sig",
		Text:   "recovered full claude reasoning",
	})
	require.NoError(t, err)

	clientBody := []byte(`{
		"model":"claude-opus-4-6-thinking",
		"stream":false,
		"reasoning":{"effort":"high"},
		"tools":[
			{"type":"custom","name":"apply_patch","description":"Apply patch"},
			{"type":"function","name":"check_seq","description":"Check seq","parameters":{"type":"object","properties":{"seq":{"type":"integer"}}}}
		],
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"Patch file"}]},
			{"type":"reasoning","id":"rs_prev","summary":[],"encrypted_content":"` + carrier + `"},
			{"type":"custom_tool_call","call_id":"call_patch_prev","name":"apply_patch","input":"*** Begin Patch"},
			{"type":"function_call","call_id":"call_seq_prev","name":"check_seq","arguments":"{\"seq\":9007199254740993}"},
			{"type":"custom_tool_call_output","call_id":"call_patch_prev","output":"patched"},
			{"type":"function_call_output","call_id":"call_seq_prev","output":"seq_ok"}
		]
	}`)

	mem := &testScopedCallMemory{}
	ex := newTestAntigravityExchange(t, protocol.ProtocolOpenAIResponses, clientBody, "claude-opus-4-6-thinking", true, mem)
	initialGemini, err := ex.UpstreamRequest()
	require.NoError(t, err)
	require.Contains(t, string(initialGemini), `"thought":true`)
	require.Contains(t, string(initialGemini), `"thoughtSignature":"stale_sonnet_sig"`)
	require.Contains(t, string(initialGemini), `9007199254740993`)

	// Verify Exchange.ReencodeUpstream preserves >53-bit integer literals verbatim
	// prior to CleanGeminiRequest and WrapV1InternalRequest post-processing.
	stage1Reencoded, stage1Changed, stage1Err := ex.ReencodeUpstream(stripIRThinkingForClaudeRecovery)
	require.NoError(t, stage1Err)
	require.True(t, stage1Changed)
	require.Contains(t, string(stage1Reencoded), `9007199254740993`)

	initialResp := &http.Response{
		StatusCode: http.StatusBadRequest,
		Header:     http.Header{"X-Request-Id": []string{"req_sig_400"}},
		Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"messages.1.content.0: Invalid signature in thinking block"}}`)),
	}

	var retriedBodies [][]byte
	var observations []RetryObservation
	successSSE := `data: {"responseId":"resp_recovered_1","modelVersion":"claude-opus-4-6-thinking","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"fresh thought","thought":true,"thoughtSignature":"sig_fresh_opus"},{"functionCall":{"id":"up_new_patch","name":"apply_patch","args":{"input":"*** End Patch"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":40,"candidatesTokenCount":9,"thoughtsTokenCount":5}}}` + "\n\n"

	recoveredResp := RecoverClaudeIR(t.Context(), ClaudeIRRecoveryInput{
		ProviderID:          9,
		ProviderName:        "ag-claude-acct",
		Prefix:              "[test]",
		ProjectID:           "proj-ag-9",
		Model:               "claude-opus-4-6-thinking",
		IdentityPatchPrompt: "Operator custom Antigravity identity.",
		Exchange:            ex,
		InitialGeminiBody:   initialGemini,
	}, initialResp, ClaudeIRRecoveryOptions{
		SignatureEnabled: func(context.Context) bool { return true },
		BudgetEnabled:    func(context.Context) bool { return true },
		LogConfig:        func() (bool, int) { return true, 512 },
		ErrorDetail:      func(b []byte) string { return string(b) },
		ReadErrorBody: func(r *http.Response) []byte {
			raw, _ := io.ReadAll(r.Body)
			return raw
		},
		Observe: func(obs RetryObservation) {
			observations = append(observations, obs)
		},
		Retry: func(body []byte) (*http.Response, error) {
			retriedBodies = append(retriedBodies, bytes.Clone(body))
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"X-Request-Id": []string{"req_sig_ok"}},
				Body:       io.NopCloser(strings.NewReader(successSSE)),
			}, nil
		},
	})

	require.Equal(t, http.StatusOK, recoveredResp.StatusCode)
	require.Len(t, retriedBodies, 1, "thinking-only stage must succeed without invoking stage 2")
	require.Len(t, observations, 1)
	require.Equal(t, "signature_error", observations[0].Kind)

	sent := decodeTestRecoveryEnvelope(t, retriedBodies[0])
	require.Equal(t, "proj-ag-9", sent.Project)
	require.Equal(t, "claude-opus-4-6-thinking", sent.Model)
	require.Contains(t, string(sent.Request.SystemInstruction), "Operator custom Antigravity identity.")
	require.Contains(t, string(sent.Request.Contents), "recovered full claude reasoning")
	require.NotContains(t, string(sent.Request.Contents), `"thought":true`)
	require.NotContains(t, string(sent.Request.Contents), "stale_sonnet_sig")
	require.Contains(t, string(sent.Request.Contents), `"functionCall"`)
	require.Contains(t, string(sent.Request.Contents), `"name":"check_seq"`)
	require.Contains(t, string(sent.Request.Contents), `"functionResponse"`)
	require.NotContains(t, string(sent.Request.Contents), "(tool_use)")
	require.NotContains(t, string(sent.Request.GenerationConfig), "thinkingConfig")

	// Feed the recovered upstream response through ReadConvertedBuffered using the
	// untouched original Exchange and verify custom tool raising + carrier sealing.
	sink := &convertedTestSink{}
	outCtx := upstream.NewDeferredOutputContext(sink)
	adapter := &ResponseAdapter{Options: ResponseOptions{Context: t.Context()}}
	res, readErr := adapter.ReadConvertedBuffered(outCtx, recoveredResp, ConvertedOptions{
		Exchange:  ex,
		RequestID: "req_sig_ok",
	})
	require.NoError(t, readErr)
	require.NoError(t, recoveredResp.Body.Close())
	require.Equal(t, 40, res.Usage.InputTokens)
	require.Equal(t, 14, res.Usage.OutputTokens)
	require.Contains(t, sink.body.String(), `"type":"custom_tool_call"`)
	require.Contains(t, sink.body.String(), `"input":"*** End Patch"`)
	require.Contains(t, sink.body.String(), "trc1:antigravity_claude:")
}

// TestRecoverClaudeIR_ThinkingPlusToolsStageAndBudgetRectification feeds
// rejected signature and budget-constraint HTTP 400 responses into RecoverClaudeIR.
// Stage-two must downgrade tool history to text, non-adaptive requests must
// rectify thinkingBudget and maxOutputTokens, and adaptive requests must skip
// explicit budget overrides.
func TestRecoverClaudeIR_ThinkingPlusToolsStageAndBudgetRectification(t *testing.T) {
	t.Run("stage2_downgrades_tools_then_falls_through_to_budget_rectification", func(t *testing.T) {
		msgBody := []byte(`{
			"model":"claude-sonnet-4-5-thinking",
			"max_tokens":2048,
			"thinking":{"type":"enabled","budget_tokens":512},
			"tools":[{"name":"run_check","description":"Run check","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}}}}],
			"messages":[
				{"role":"user","content":"Run diagnostic"},
				{"role":"assistant","content":[
					{"type":"redacted_thinking","data":"enc_redacted"},
					{"type":"tool_use","id":"toolu_9","name":"run_check","input":{"cmd":"uptime"}}
				]},
				{"role":"user","content":[
					{"type":"tool_result","tool_use_id":"toolu_9","is_error":true,"content":"timeout"}
				]}
			]
		}`)

		ex := newTestAntigravityExchange(t, protocol.ProtocolAnthropicMessages, msgBody, "claude-sonnet-4-5-thinking", true, nil)
		initialGemini, err := ex.UpstreamRequest()
		require.NoError(t, err)

		initialResp := &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"Expected thinking or redacted_thinking, but found tool_use"}}`)),
		}

		var retriedBodies [][]byte
		var obsKinds []string
		callIdx := 0
		finalResp := RecoverClaudeIR(t.Context(), ClaudeIRRecoveryInput{
			ProviderID:        5,
			ProviderName:      "ag-p5",
			Prefix:            "[ag]",
			ProjectID:         "proj-5",
			Model:             "claude-sonnet-4-5-thinking",
			Exchange:          ex,
			InitialGeminiBody: initialGemini,
		}, initialResp, ClaudeIRRecoveryOptions{
			SignatureEnabled: func(context.Context) bool { return true },
			BudgetEnabled:    func(context.Context) bool { return true },
			IsBudgetConstraint: func(msg string) bool {
				lower := strings.ToLower(msg)
				return strings.Contains(lower, "budget_tokens") && strings.Contains(lower, "1024")
			},
			BudgetTokens:   32000,
			MinMaxTokens:   32001,
			MaxTokens:      64000,
			LogConfig:      func() (bool, int) { return true, 256 },
			ErrorDetail:    func(b []byte) string { return string(b) },
			TruncateString: func(s string, _ int) string { return s },
			ReadErrorBody: func(r *http.Response) []byte {
				raw, _ := io.ReadAll(r.Body)
				return raw
			},
			Observe: func(obs RetryObservation) {
				obsKinds = append(obsKinds, obs.Kind)
			},
			Retry: func(body []byte) (*http.Response, error) {
				callIdx++
				retriedBodies = append(retriedBodies, bytes.Clone(body))
				switch callIdx {
				case 1:
					// Stage 1 (thinking-only) still hits signature rejection -> triggers Stage 2.
					return &http.Response{
						StatusCode: http.StatusBadRequest,
						Header:     http.Header{},
						Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"Expected thinking block before tool_use"}}`)),
					}, nil
				case 2:
					// Stage 2 (thinking+tools) leaves signature classification with a budget constraint 400!
					return &http.Response{
						StatusCode: http.StatusBadRequest,
						Header:     http.Header{},
						Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"thinking.budget_tokens: Input should be greater than or equal to 1024"}}`)),
					}, nil
				default:
					// Stage 3 (budget rectification) succeeds!
					return &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{},
						Body:       io.NopCloser(strings.NewReader("data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"done\"}]},\"finishReason\":\"STOP\"}]}}\n\n")),
					}, nil
				}
			},
		})
		require.Equal(t, http.StatusOK, finalResp.StatusCode)
		require.NoError(t, finalResp.Body.Close())
		require.Len(t, retriedBodies, 3)
		require.Equal(t, []string{
			"signature_error",
			"signature_retry_thinking-only",
			"signature_retry_thinking_tools",
			"budget_constraint_error",
		}, obsKinds)

		// Stage 2 downgraded tool_use and tool_result to readable text parts.
		stage2Env := decodeTestRecoveryEnvelope(t, retriedBodies[1])
		require.NotContains(t, string(stage2Env.Request.Contents), `"functionCall"`)
		require.NotContains(t, string(stage2Env.Request.Contents), `"functionResponse"`)
		require.Contains(t, string(stage2Env.Request.Contents), "(tool_use) name=run_check id=toolu_9")
		require.Contains(t, string(stage2Env.Request.Contents), "(tool_result) tool_use_id=toolu_9 is_error=true")

		// Stage 3 applied rectified thinkingBudget=32000 and maxOutputTokens=64000.
		stage3Env := decodeTestRecoveryEnvelope(t, retriedBodies[2])
		require.Contains(t, string(stage3Env.Request.GenerationConfig), `"thinkingBudget":32000`)
		require.Contains(t, string(stage3Env.Request.GenerationConfig), `"maxOutputTokens":64000`)
	})

	t.Run("adaptive_thinking_skips_budget_rectification", func(t *testing.T) {
		adaptiveBody := []byte(`{"model":"claude-opus-4-6-thinking","max_tokens":4096,"thinking":{"type":"adaptive"},"messages":[{"role":"user","content":"hi"}]}`)
		ex := newTestAntigravityExchange(t, protocol.ProtocolAnthropicMessages, adaptiveBody, "claude-opus-4-6-thinking", true, nil)
		initialResp := &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"thinking.budget_tokens: Input should be >= 1024"}}`)),
		}

		retried := false
		outResp := RecoverClaudeIR(t.Context(), ClaudeIRRecoveryInput{
			ProviderID: 7,
			ProjectID:  "proj-7",
			Model:      "claude-opus-4-6-thinking",
			Exchange:   ex,
		}, initialResp, ClaudeIRRecoveryOptions{
			BudgetEnabled: func(context.Context) bool { return true },
			IsBudgetConstraint: func(msg string) bool {
				lower := strings.ToLower(msg)
				return strings.Contains(lower, "budget_tokens") && strings.Contains(lower, "1024")
			},
			BudgetTokens: 32000,
			MinMaxTokens: 32001,
			MaxTokens:    64000,
			Retry: func([]byte) (*http.Response, error) {
				retried = true
				return nil, nil
			},
		})
		require.False(t, retried, "adaptive thinking must skip explicit budget rectification")
		require.Equal(t, http.StatusBadRequest, outResp.StatusCode)
		restoredBody, err := io.ReadAll(outResp.Body)
		require.NoError(t, err)
		require.NoError(t, outResp.Body.Close())
		require.Contains(t, string(restoredBody), ">= 1024")
	})
}
