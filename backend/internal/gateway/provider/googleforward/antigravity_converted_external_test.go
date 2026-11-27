package googleforward_test

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/apikey"
	"github.com/TokenFlux/TokenRouter/internal/billing"
	"github.com/TokenFlux/TokenRouter/internal/gateway"
	"github.com/TokenFlux/TokenRouter/internal/gateway/errorpolicy"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayhttp "github.com/TokenFlux/TokenRouter/internal/gateway/httpapi"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/googleforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	gatewaytestkit "github.com/TokenFlux/TokenRouter/internal/gateway/testkit"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	settingscore "github.com/TokenFlux/TokenRouter/internal/settings"
	"github.com/TokenFlux/TokenRouter/internal/upstream/antigravity"
)

// antigravity_converted_external_test.go verifies Antigravity.ForwardConverted
// and UnifiedTextExecutor Antigravity dispatch across
// Anthropic Messages, OpenAI Responses, and OpenAI Chat Completions. Scenarios
// cover claude-* and gemini-* dialect and CallMemory isolation, streaming and
// buffered merged response reading, trc1 carrier sealing, pre-semantic leading
// frame failover, CommittedSSE terminal errors, mid-stream truncation and
// client disconnect draining, RecoverClaudeIR, and RecoverGemini.

// capturedAntigravityEnvelope decodes a v1internal wrapped Antigravity request
// in external tests without gjson or map[string]any.
type capturedAntigravityEnvelope struct {
	Project     string                `json:"project"`     // GCP project identifier
	RequestID   string                `json:"requestId"`   // generated agent request identifier
	UserAgent   string                `json:"userAgent"`   // fixed protocol userAgent
	RequestType string                `json:"requestType"` // fixed v1internal requestType value
	Model       string                `json:"model"`       // mapped upstream model
	Request     capturedGeminiRequest `json:"request"`     // inner GenerateContent payload
}

// capturedGeminiRequest decodes top-level Gemini GenerateContent fields for assertions.
type capturedGeminiRequest struct {
	SystemInstruction capturedGeminiContent `json:"systemInstruction"` // injected systemInstruction object
	Contents          []capturedGeminiTurn  `json:"contents"`          // ordered conversation turns
	Tools             []capturedGeminiTools `json:"tools"`             // tool declaration groups
	GenerationConfig  jsontext.Value        `json:"generationConfig"`  // raw generationConfig JSON
}

// capturedGeminiContent decodes role and ordered instruction parts inside a Gemini systemInstruction object.
type capturedGeminiContent struct {
	Role  string               `json:"role"`  // optional role field
	Parts []capturedGeminiPart `json:"parts"` // ordered instruction parts
}

// capturedGeminiTurn decodes one role-annotated user or model turn inside Gemini contents.
type capturedGeminiTurn struct {
	Role  string               `json:"role"`  // "user" or "model"
	Parts []capturedGeminiPart `json:"parts"` // ordered turn content parts
}

// capturedGeminiPart decodes text, thought, signature, and tool fields of a Gemini part.
type capturedGeminiPart struct {
	Text             string                      `json:"text"`             // text or thought text
	Thought          bool                        `json:"thought"`          // reasoning thought part indicator
	ThoughtSignature string                      `json:"thoughtSignature"` // upstream thought replay signature
	FunctionCall     *capturedGeminiFunctionCall `json:"functionCall"`     // model tool call
	FunctionResponse *capturedGeminiFuncResp     `json:"functionResponse"` // user tool result
}

// capturedGeminiFunctionCall decodes the identifier, tool name, and raw arguments of a Gemini functionCall.
type capturedGeminiFunctionCall struct {
	ID   string         `json:"id"`   // optional call ID
	Name string         `json:"name"` // tool function name
	Args jsontext.Value `json:"args"` // raw JSON arguments
}

// capturedGeminiFuncResp decodes the identifier, tool name, and raw payload of a Gemini functionResponse.
type capturedGeminiFuncResp struct {
	ID       string         `json:"id"`       // optional call ID
	Name     string         `json:"name"`     // tool function name
	Response jsontext.Value `json:"response"` // raw response object
}

// capturedGeminiTools decodes one Gemini tool entry containing declared function schemas.
type capturedGeminiTools struct {
	FunctionDeclarations []capturedGeminiFuncDecl `json:"functionDeclarations"` // declared Gemini tool functions
}

// capturedGeminiFuncDecl decodes the name, description, and cleaned parameter schema of one Gemini function.
type capturedGeminiFuncDecl struct {
	Name        string         `json:"name"`        // declared tool function name
	Description string         `json:"description"` // declared tool function description
	Parameters  jsontext.Value `json:"parameters"`  // cleaned schema JSON
}

// capturedResponsesOutputItem decodes one output entry from a buffered OpenAI Responses reply.
type capturedResponsesOutputItem struct {
	Type             string `json:"type"`              // Responses output item category
	CallID           string `json:"call_id"`           // client tool call identifier
	Name             string `json:"name"`              // invoked tool function name
	EncryptedContent string `json:"encrypted_content"` // sealed continuity carrier token
}

// capturedResponsesEnvelope decodes output items from a buffered OpenAI Responses reply.
type capturedResponsesEnvelope struct {
	Output []capturedResponsesOutputItem `json:"output"` // ordered Responses output items
}

// antigravityCustomPromptSettingStub overrides configured gateway and routing
// runtime settings keys while delegating unset keys to antigravitySettingRepoStub.
type antigravityCustomPromptSettingStub struct {
	antigravitySettingRepoStub                   // fallback missing-setting repository
	prompt                     string            // configured identity_patch_prompt value
	values                     map[string]string // additional runtime setting overrides
}

// TestAntigravityForwardConverted_AllProtocolsAndModelFamilies exercises
// Anthropic Messages, OpenAI Responses, and OpenAI Chat Completions across
// claude-* and gemini-* targets through UnifiedTextExecutor. Every route must emit a wrapped v1internal Gemini payload with the
// Antigravity identity patch, CleanAntigravitySchema transformations, and
// family-scoped CallMemory replay without cross-family token leakage.
func TestAntigravityForwardConverted_AllProtocolsAndModelFamilies(t *testing.T) {
	claudeCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAntigravityClaude,
		Data:   "sig-claude-turn1",
		Text:   "claude prior reasoning",
	})
	require.NoError(t, err)
	geminiCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeGemini,
		Data:   "sig-gemini-turn1",
	})
	require.NoError(t, err)

	models := []struct {
		model    string // target Antigravity model name
		isClaude bool   // whether model uses antigravity_claude replay rules
	}{
		{model: "claude-opus-4-6-thinking", isClaude: true},
		{model: "gemini-3.1-pro-high", isClaude: false},
	}
	sources := []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	}

	for _, m := range models {
		for _, source := range sources {
			t.Run(string(source)+"/"+m.model, func(t *testing.T) {
				upstreamStub := &queuedHTTPUpstreamStub{
					responses: []*http.Response{antigravityCompatSuccessResponse()},
				}
				svc := newAntigravityCompatibilityFixture(
					googleforward.Options{
						MaxLineSize: 500 * 1024 * 1024,
					},
					upstreamStub,
				)
				unified := &gatewayhttp.UnifiedTextExecutor{
					Antigravity: &gatewayhttp.AntigravityExecutor{Runtime: svc},
				}
				prov := newAntigravityCompatProvider(capability.ProviderTypeOAuth)

				// Pre-populate CallMemory with one signed call whose UpstreamID differs
				// from the client ID and one known-unsigned parallel sibling call, plus
				// a foreign-scheme call that must not leak across model families.
				scope := session.GeminiToolScope{
					APIKeyID:   77,
					ProviderID: prov.Record.ID,
					Model:      m.model,
				}
				scheme := ir.SchemeGemini
				foreignScheme := ir.SchemeAntigravityClaude
				if m.isClaude {
					scheme = ir.SchemeAntigravityClaude
					foreignScheme = ir.SchemeGemini
				}
				mem := svc.CallMemory(scope)
				require.NotNil(t, mem)
				mem.Remember("call_signed", convert.CallBinding{
					UpstreamID: "up_call_signed",
					Token:      &ir.ReplayToken{Scheme: scheme, Data: "call-sig-1"},
				})
				mem.Remember("call_unsigned_sibling", convert.CallBinding{
					UpstreamID: "up_call_unsigned",
					Token:      nil,
				})
				mem.Remember("call_foreign", convert.CallBinding{
					UpstreamID: "up_foreign_should_not_apply",
					Token:      &ir.ReplayToken{Scheme: foreignScheme, Data: "foreign-sig"},
				})

				reqBody := buildAntigravityMultiTurnFixtureBody(source, m.model, m.isClaude, claudeCarrier, geminiCarrier)
				path := antigravityPathForProtocol(source)
				c, rec := newAntigravityCompatContext(http.MethodPost, path, reqBody)
				c.Set("api_key", &apikey.APIKey{ID: 77})

				var res *forwardcore.OpenAIResult
				var execErr error
				switch source {
				case protocol.ProtocolAnthropicMessages:
					res, execErr = unified.Messages(t.Context(), c, prov, reqBody, "", m.model)
				case protocol.ProtocolOpenAIResponses:
					res, execErr = unified.Responses(t.Context(), c, prov, reqBody)
				default:
					res, execErr = unified.Chat(t.Context(), c, prov, reqBody, "", m.model)
				}
				require.NoError(t, execErr)
				require.NotNil(t, res)
				require.Equal(t, gatewayhttp.EndpointAntigravityGenerateContent, res.UpstreamEndpoint)
				require.Equal(t, m.model, res.Model)
				require.Equal(t, m.model, res.UpstreamModel)
				require.Equal(t, http.StatusOK, rec.Code)

				require.Len(t, upstreamStub.requestBodies, 1)
				sent := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[0])
				require.Equal(t, "project-3757", sent.Project)
				require.Equal(t, "antigravity", sent.UserAgent)
				require.Equal(t, "agent", sent.RequestType)
				require.Equal(t, m.model, sent.Model)
				require.True(t, strings.HasPrefix(sent.RequestID, "agent-"))

				require.NotEmpty(t, sent.Request.SystemInstruction.Parts)
				require.Contains(t, sent.Request.SystemInstruction.Parts[0].Text, "You are Antigravity")

				// Verify CleanAntigravitySchema injected placeholder reason onto empty
				// object schema and migrated minLength into description.
				require.Len(t, sent.Request.Tools, 1)
				require.Len(t, sent.Request.Tools[0].FunctionDeclarations, 2)
				pingDecl := sent.Request.Tools[0].FunctionDeclarations[0]
				require.Equal(t, "ping", pingDecl.Name)
				require.Contains(t, string(pingDecl.Parameters), `"reason"`)
				lookupDecl := sent.Request.Tools[0].FunctionDeclarations[1]
				require.Equal(t, "lookup", lookupDecl.Name)
				require.Contains(t, string(lookupDecl.Parameters), "[Constraint: minLen: 2]")

				// Inspect model turn parts for CallMemory ID/signature restoration and
				// family-specific unsigned sibling handling.
				var modelParts []capturedGeminiPart
				var userFuncRespParts []capturedGeminiPart
				for _, content := range sent.Request.Contents {
					if content.Role == "model" {
						modelParts = append(modelParts, content.Parts...)
					}
					for _, p := range content.Parts {
						if p.FunctionResponse != nil {
							userFuncRespParts = append(userFuncRespParts, p)
						}
					}
				}
				require.NotEmpty(t, modelParts)

				var calls []capturedGeminiPart
				for _, p := range modelParts {
					if p.FunctionCall != nil {
						calls = append(calls, p)
					}
				}
				require.Len(t, calls, 3)
				require.Equal(t, "up_call_signed", calls[0].FunctionCall.ID)
				require.Equal(t, "call-sig-1", calls[0].ThoughtSignature)

				require.Equal(t, "up_call_unsigned", calls[1].FunctionCall.ID)
				require.Empty(t, calls[1].ThoughtSignature, "known-unsigned sibling must not receive dummy signature")

				// Foreign-scheme CallMemory entry must be ignored, preserving client ID.
				require.Equal(t, "call_foreign", calls[2].FunctionCall.ID)
				if m.isClaude {
					require.Empty(t, calls[2].ThoughtSignature, "claude-* must never receive dummy thoughtSignature")
				} else {
					require.Equal(t, "skip_thought_signature_validator", calls[2].ThoughtSignature, "unknown unsigned call on gemini-* receives placeholder")
				}

				require.Len(t, userFuncRespParts, 3)
				require.Equal(t, "up_call_signed", userFuncRespParts[0].FunctionResponse.ID)
				require.Equal(t, "up_call_unsigned", userFuncRespParts[1].FunctionResponse.ID)
				require.Equal(t, "call_foreign", userFuncRespParts[2].FunctionResponse.ID)
				require.Contains(t, string(userFuncRespParts[0].FunctionResponse.Response), `"result":"pong"`)
			})
		}
	}
}

// TestAntigravityForwardConverted_CursorInputChatAndLegacyResponsesIngress
// submits a Cursor /v1/chat/completions payload lacking messages with top-level
// input, a legacy /v1/responses payload carrying messages, and a Chat tool named
// sessions_lookup. Single-hop conversion must preserve Cursor stream usage and
// reasoning effort, normalize legacy Responses messages, and retain raw tool names.
func TestAntigravityForwardConverted_CursorInputChatAndLegacyResponsesIngress(t *testing.T) {
	t.Run("cursor_input_chat_single_hop", func(t *testing.T) {
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{antigravityCompatSuccessResponse()},
		}
		svc := newAntigravityCompatibilityFixture(
			googleforward.Options{MaxLineSize: 500 * 1024 * 1024},
			upstreamStub,
		)
		cursorBody := []byte(`{
			"model":"gemini-3.1-pro-high",
			"stream":true,
			"stream_options":{"include_usage":true},
			"reasoning_effort":"high",
			"stop":"END_NOW",
			"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Cursor input payload"}]}]
		}`)
		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", cursorBody)

		res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolOpenAIChatCompletions, cursorBody, false)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.Stream)
		require.NotNil(t, res.ReasoningEffort)
		require.Equal(t, "high", *res.ReasoningEffort)
		require.Contains(t, rec.Body.String(), `"usage":`)
		require.Contains(t, rec.Body.String(), "data: [DONE]")

		require.Len(t, upstreamStub.requestBodies, 1)
		sent := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[0])
		require.Len(t, sent.Request.Contents, 1)
		require.Equal(t, "Cursor input payload", sent.Request.Contents[0].Parts[0].Text)
	})

	t.Run("legacy_responses_messages_ingress", func(t *testing.T) {
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{antigravityCompatSuccessResponse()},
		}
		svc := newAntigravityCompatibilityFixture(
			googleforward.Options{MaxLineSize: 500 * 1024 * 1024},
			upstreamStub,
		)
		legacyBody := []byte(`{
			"model":"claude-sonnet-4-5",
			"messages":[{"role":"user","content":"Legacy messages on /v1/responses"}]
		}`)
		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/responses", legacyBody)

		res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolOpenAIResponses, legacyBody, false)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Len(t, upstreamStub.requestBodies, 1)
		sent := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[0])
		require.Equal(t, "claude-sonnet-4-5", sent.Model)
		require.Equal(t, "Legacy messages on /v1/responses", sent.Request.Contents[0].Parts[0].Text)
	})

	t.Run("sessions_tool_preserved_without_oauth_obfuscation", func(t *testing.T) {
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{antigravityCompatSuccessResponse()},
		}
		svc := newAntigravityCompatibilityFixture(
			googleforward.Options{MaxLineSize: 500 * 1024 * 1024},
			upstreamStub,
		)
		chatBody := []byte(`{
			"model":"gemini-3.1-pro-high",
			"messages":[{"role":"user","content":"Check session"}],
			"tools":[{"type":"function","function":{"name":"sessions_lookup","description":"Lookup session","parameters":{"type":"object","properties":{}}}}]
		}`)
		c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", chatBody)

		res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolOpenAIChatCompletions, chatBody, false)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, upstreamStub.requestBodies, 1)
		sent := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[0])
		require.Len(t, sent.Request.Tools, 1)
		require.Len(t, sent.Request.Tools[0].FunctionDeclarations, 1)
		require.Equal(t, "sessions_lookup", sent.Request.Tools[0].FunctionDeclarations[0].Name)
	})
}

// TestAntigravityForwardConverted_ClientValidationAndWhitelistSkipSchedulerPenalty
// submits malformed JSON, blank models, "ultra" reasoning effort, non-OAuth
// providers, and base-only model whitelists across uncommitted JSON and
// pre-committed SSE modes. Every local rejection must emit a protocol-formatted
// 400 or 403 error and return MODEL_CAPABILITY_UNSUPPORTED.
func TestAntigravityForwardConverted_ClientValidationAndWhitelistSkipSchedulerPenalty(t *testing.T) {
	cases := []struct {
		name           string              // table subtest scenario label
		source         protocol.ProtocolID // inbound client protocol
		body           []byte              // inbound request payload
		providerType   string              // provider type override
		whitelist      []string            // optional provider model_whitelist
		wantHTTPStatus int                 // expected uncommitted HTTP status
		wantSubstr     string              // expected client error message substring
	}{
		{
			name:           "invalid_json_messages",
			source:         protocol.ProtocolAnthropicMessages,
			body:           []byte(`{"model":`),
			providerType:   capability.ProviderTypeOAuth,
			wantHTTPStatus: http.StatusBadRequest,
			wantSubstr:     "Failed to parse request body",
		},
		{
			name:           "blank_model_responses",
			source:         protocol.ProtocolOpenAIResponses,
			body:           []byte(`{"model":"   ","input":"hi"}`),
			providerType:   capability.ProviderTypeOAuth,
			wantHTTPStatus: http.StatusBadRequest,
			wantSubstr:     "model is required",
		},
		{
			name:           "ultra_effort_messages",
			source:         protocol.ProtocolAnthropicMessages,
			body:           []byte(`{"model":"claude-opus-4-6-thinking","max_tokens":64,"output_config":{"effort":"ultra"},"messages":[{"role":"user","content":"hi"}]}`),
			providerType:   capability.ProviderTypeOAuth,
			wantHTTPStatus: http.StatusBadRequest,
			wantSubstr:     "is not supported; use",
		},
		{
			name:           "ultra_effort_responses",
			source:         protocol.ProtocolOpenAIResponses,
			body:           []byte(`{"model":"gemini-3.1-pro-high","reasoning":{"effort":"ultra"},"input":"hi"}`),
			providerType:   capability.ProviderTypeOAuth,
			wantHTTPStatus: http.StatusBadRequest,
			wantSubstr:     "is not supported; use",
		},
		{
			name:           "ultra_effort_chat",
			source:         protocol.ProtocolOpenAIChatCompletions,
			body:           []byte(`{"model":"gemini-3.1-pro-high","reasoning_effort":"ultra","messages":[{"role":"user","content":"hi"}]}`),
			providerType:   capability.ProviderTypeOAuth,
			wantHTTPStatus: http.StatusBadRequest,
			wantSubstr:     "is not supported; use",
		},
		{
			name:           "non_oauth_provider_rejected",
			source:         protocol.ProtocolOpenAIChatCompletions,
			body:           []byte(`{"model":"gemini-3.1-pro-high","messages":[{"role":"user","content":"hi"}]}`),
			providerType:   capability.ProviderTypeSetupToken,
			wantHTTPStatus: http.StatusBadRequest,
			wantSubstr:     "native OAuth provider required",
		},
		{
			name:           "thinking_suffix_denied_by_base_only_whitelist",
			source:         protocol.ProtocolAnthropicMessages,
			body:           []byte(`{"model":"claude-sonnet-4-5","max_tokens":1024,"thinking":{"type":"enabled","budget_tokens":512},"messages":[{"role":"user","content":"hi"}]}`),
			providerType:   capability.ProviderTypeOAuth,
			whitelist:      []string{"claude-sonnet-4-5"},
			wantHTTPStatus: http.StatusForbidden,
			wantSubstr:     "not in whitelist",
		},
	}

	for _, tc := range cases {
		for _, committedSSE := range []bool{false, true} {
			mode := "json"
			if committedSSE {
				mode = "committed_sse"
			}
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				upstreamStub := &queuedHTTPUpstreamStub{}
				svc := newAntigravityCompatibilityFixture(
					googleforward.Options{},
					upstreamStub,
				)
				prov := newAntigravityCompatProvider(tc.providerType)
				if len(tc.whitelist) > 0 {
					prov.Record.Credentials["model_whitelist"] = tc.whitelist
				}

				path := antigravityPathForProtocol(tc.source)
				c, rec := newAntigravityCompatContext(http.MethodPost, path, tc.body)
				if committedSSE {
					c.Header("Content-Type", "text/event-stream")
					c.Status(http.StatusOK)
					c.Writer.WriteHeaderNow()
				}

				boundary := gatewayhttp.NewGoogleBoundary(c, svc.Options, true)
				var res *forwardcore.MessagesResult
				var err error
				switch tc.source {
				case protocol.ProtocolAnthropicMessages:
					res, err = svc.ForwardConverted(t.Context(), boundary, prov, protocol.ProtocolAnthropicMessages, tc.body, false)
				case protocol.ProtocolOpenAIResponses:
					res, err = svc.ForwardConverted(t.Context(), boundary, prov, protocol.ProtocolOpenAIResponses, tc.body, false)
				default:
					res, err = svc.ForwardConverted(t.Context(), boundary, prov, protocol.ProtocolOpenAIChatCompletions, tc.body, false)
				}

				require.Nil(t, res)
				require.Error(t, err)
				require.Equal(t, "MODEL_CAPABILITY_UNSUPPORTED", apperror.Reason(err))
				require.True(t, routing.IsModelAttributeError(err))
				require.Empty(t, upstreamStub.requestBodies, "local validation failure must not contact upstream")

				out := rec.Body.String()
				require.Contains(t, out, tc.wantSubstr)
				if committedSSE {
					require.Equal(t, http.StatusOK, rec.Code)
					if tc.source == protocol.ProtocolOpenAIResponses {
						require.Contains(t, out, "event: response.failed")
					} else {
						require.Contains(t, out, "event: error")
					}
				} else {
					require.Equal(t, tc.wantHTTPStatus, rec.Code)
				}
			})
		}
	}
}

// TestAntigravityForwardConverted_UpstreamErrorsStickyFailoverAndStripEffort
// exercises Messages PromptTooLongError, Google project-config retryable 400
// failover, OpenAI 401 credential rejection, sticky rate-limit failover,
// pre-committed SSE passthrough rules, custom identity_patch_prompt settings,
// and StripEffort. Upstream error classification,
// runtime settings, and billing attribution must match Antigravity platform rules.
func TestAntigravityForwardConverted_UpstreamErrorsStickyFailoverAndStripEffort(t *testing.T) {
	t.Run("messages_prompt_too_long", func(t *testing.T) {
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusBadRequest,
				Header:     http.Header{"X-Request-Id": {"req-ptl-42"}},
				Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"Prompt is too long: 250000 tokens"}}`)),
			}},
		}
		svc := newAntigravityCompatibilityFixture(googleforward.Options{}, upstreamStub)
		body := []byte(`{"model":"claude-opus-4-6-thinking","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`)
		c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/messages", body)

		res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolAnthropicMessages, body, false)
		require.Nil(t, res)
		var promptErr *antigravity.PromptTooLongError
		require.ErrorAs(t, err, &promptErr)
		require.Equal(t, http.StatusBadRequest, promptErr.StatusCode)
		require.Equal(t, "req-ptl-42", promptErr.RequestID)

		rawEvents, exists := c.Get(gatewayhttp.OpsUpstreamErrorsKey)
		require.True(t, exists)
		events, ok := rawEvents.([]*ops.OpsUpstreamErrorEvent)
		require.True(t, ok)
		require.Len(t, events, 1)
		require.Equal(t, "prompt_too_long", events[0].Kind)
	})

	t.Run("google_project_config_400_failover", func(t *testing.T) {
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusBadRequest,
				Header:     http.Header{"X-Request-Id": {"req-cfg-400"}},
				Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"Invalid project resource name projects/bad-proj"}}`)),
			}},
		}
		svc := newAntigravityCompatibilityFixture(googleforward.Options{}, upstreamStub)
		body := []byte(`{"model":"gemini-3.1-pro-high","input":"hello"}`)
		c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/responses", body)

		res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolOpenAIResponses, body, false)
		require.Nil(t, res)
		var failoverErr *forwardcore.UpstreamFailoverError
		require.ErrorAs(t, err, &failoverErr)
		require.True(t, failoverErr.RetryableOnSameProvider)
	})

	t.Run("openai_401_credential_rejected", func(t *testing.T) {
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusUnauthorized,
				Header:     http.Header{"X-Request-Id": {"req-auth-401"}},
				Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"Invalid authentication credentials"}}`)),
			}},
		}
		svc := newAntigravityCompatibilityFixture(googleforward.Options{}, upstreamStub)
		body := []byte(`{"model":"gemini-3.1-pro-high","messages":[{"role":"user","content":"hello"}]}`)
		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)

		res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolOpenAIChatCompletions, body, false)
		require.Nil(t, res)
		var failoverErr *forwardcore.UpstreamFailoverError
		require.ErrorAs(t, err, &failoverErr)
		require.Equal(t, forwardcore.GatewayFailureStageProviderAuth, failoverErr.Stage)
		require.Equal(t, forwardcore.AntigravityCredentialRejectedReason, failoverErr.Reason)
		require.Equal(t, forwardcore.NextProviderRetry, failoverErr.NextProviderAction)
		require.Empty(t, rec.Body.String())
	})

	t.Run("sticky_model_cooldown_sets_force_cache_billing", func(t *testing.T) {
		svc := newAntigravityCompatibilityFixture(googleforward.Options{}, &queuedHTTPUpstreamStub{})
		prov := newAntigravityCompatProvider(capability.ProviderTypeOAuth)
		prov.Record.Extra = map[string]any{
			"model_rate_limits": map[string]any{
				"claude-opus-4-6-thinking": map[string]any{
					"rate_limit_reset_at": time.Now().Add(30 * time.Second).Format(time.RFC3339),
				},
			},
		}
		body := []byte(`{"model":"claude-opus-4-6-thinking","max_tokens":32,"messages":[{"role":"user","content":"hi"}]}`)
		c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/messages", body)
		ctx := requeststate.WithPrefetchedStickySession(t.Context(), prov.Record.ID, 1)

		unified := &gatewayhttp.UnifiedTextExecutor{
			Antigravity: &gatewayhttp.AntigravityExecutor{Runtime: svc},
		}
		res, err := unified.Messages(ctx, c, prov, body, "", "claude-opus-4-6-thinking")
		require.Nil(t, res)
		var failoverErr *forwardcore.UpstreamFailoverError
		require.ErrorAs(t, err, &failoverErr)
		require.Equal(t, http.StatusServiceUnavailable, failoverErr.StatusCode)
		require.True(t, failoverErr.ForceCacheBilling)
	})

	t.Run("committed_sse_upstream_400_writes_terminal_event", func(t *testing.T) {
		for _, source := range []protocol.ProtocolID{protocol.ProtocolAnthropicMessages, protocol.ProtocolOpenAIResponses} {
			upstreamStub := &queuedHTTPUpstreamStub{
				responses: []*http.Response{{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"X-Request-Id": {"req-400-sse"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"invalid tool parameter"}}`)),
				}},
			}
			svc := newAntigravityCompatibilityFixture(googleforward.Options{}, upstreamStub)
			body := []byte(`{"model":"gemini-3.1-pro-high","stream":true,"input":"hello"}`)
			if source == protocol.ProtocolAnthropicMessages {
				body = []byte(`{"model":"gemini-3.1-pro-high","max_tokens":32,"stream":true,"messages":[{"role":"user","content":"hello"}]}`)
			}
			c, rec := newAntigravityCompatContext(http.MethodPost, antigravityPathForProtocol(source), body)
			c.Header("Content-Type", "text/event-stream")
			c.Status(http.StatusOK)
			c.Writer.WriteHeaderNow()

			_, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), source, body, false)
			require.Error(t, err)
			if source == protocol.ProtocolOpenAIResponses {
				require.Contains(t, rec.Body.String(), "event: response.failed")
			} else {
				require.Contains(t, rec.Body.String(), "event: error")
			}
		}
	})

	t.Run("strip_effort_maps_alias_and_keeps_billing_effort", func(t *testing.T) {
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{antigravityCompatSuccessResponse()},
		}
		svc := newAntigravityCompatibilityFixture(
			googleforward.Options{MaxLineSize: 500 * 1024 * 1024},
			upstreamStub,
		)
		prov := newAntigravityCompatProvider(capability.ProviderTypeOAuth)
		prov.Record.Credentials[providercore.EffortModelMappingCredentialKey] = map[string]any{
			"gemini-tier-alias": map[string]any{"high": "gemini-3-flash"},
		}
		prov.Record.Credentials[providercore.EffortModelMappingStripEffortCredentialKey] = true

		body := []byte(`{"model":"gemini-tier-alias","max_tokens":64,"output_config":{"effort":"high"},"messages":[{"role":"user","content":"strip check"}]}`)
		c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/messages", body)

		res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), prov, protocol.ProtocolAnthropicMessages, body, false)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, "gemini-tier-alias", res.Model)
		require.Equal(t, "gemini-3-flash", res.UpstreamModel)
		require.NotNil(t, res.ReasoningEffort)
		require.Equal(t, "high", *res.ReasoningEffort)

		require.Len(t, upstreamStub.requestBodies, 1)
		sent := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[0])
		require.Equal(t, "gemini-3-flash", sent.Model)
		require.NotContains(t, string(sent.Request.GenerationConfig), "thinkingLevel")
	})

	t.Run("custom_identity_patch_prompt_honored_across_families", func(t *testing.T) {
		for _, model := range []string{"claude-sonnet-4-5", "gemini-2.5-flash"} {
			upstreamStub := &queuedHTTPUpstreamStub{
				responses: []*http.Response{antigravityCompatSuccessResponse()},
			}
			tokenProvider := newAntigravityTokenSourceForTest(&antigravityCompatTokenCache{token: "fresh-oauth-token"})
			svc := newAntigravityFixture(antigravityDependencies{
				tokenProvider: tokenProvider,
				httpUpstream:  upstreamStub,
				settingService: newExecutionReadersFixture(&antigravityCustomPromptSettingStub{
					prompt: "Custom operator Antigravity identity prompt.",
				}),
				options: fixtureOptions(&googleforward.Options{
					MaxLineSize: 500 * 1024 * 1024,
				}),
			})
			prov := newAntigravityCompatProvider(capability.ProviderTypeOAuth)
			body := []byte(`{"model":"` + model + `","messages":[{"role":"user","content":"hello"}]}`)
			c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", body)

			res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), prov, protocol.ProtocolOpenAIChatCompletions, body, false)
			require.NoError(t, err)
			require.NotNil(t, res)
			require.Len(t, upstreamStub.requestBodies, 1)
			sent := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[0])
			require.NotEmpty(t, sent.Request.SystemInstruction.Parts)
			require.Equal(t, "Custom operator Antigravity identity prompt.", sent.Request.SystemInstruction.Parts[0].Text)
		}
	})

	t.Run("committed_sse_messages_applies_passthrough_rule_and_error_detail", func(t *testing.T) {
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusBadRequest,
				Header:     http.Header{"X-Request-Id": {"req-pt-rule"}},
				Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"vendor_specific_quota_gate: blocked"}}`)),
			}},
		}
		svc := newAntigravityCompatibilityFixture(
			googleforward.Options{
				LogErrorBody:         true,
				LogErrorBodyMaxBytes: 512,
			},
			upstreamStub,
		)
		body := []byte(`{"model":"claude-sonnet-4-5","max_tokens":32,"stream":true,"messages":[{"role":"user","content":"hello"}]}`)
		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/messages", body)
		respCode := http.StatusTooManyRequests
		customMsg := "Mapped by administrator passthrough rule"
		gatewayhttp.BindErrorPassthroughService(c, gatewaytestkit.ErrorRules([]*errorpolicy.ErrorPassthroughRule{{
			ID:             11,
			Name:           "antigravity-gate-rule",
			Enabled:        true,
			Platforms:      []string{capability.PlatformAntigravity},
			MatchMode:      errorpolicy.MatchModeAll,
			ErrorCodes:     []int{http.StatusBadRequest},
			Keywords:       []string{"vendor_specific_quota_gate"},
			ResponseCode:   &respCode,
			CustomMessage:  &customMsg,
			SkipMonitoring: true,
		}}))
		c.Header("Content-Type", "text/event-stream")
		c.Status(http.StatusOK)
		c.Writer.WriteHeaderNow()

		_, err := svc.ForwardConverted(
			t.Context(),
			gatewayhttp.NewGoogleBoundary(c, svc.Options, true),
			newAntigravityCompatProvider(capability.ProviderTypeOAuth),
			protocol.ProtocolAnthropicMessages,
			body,
			false,
		)
		require.Error(t, err)
		out := rec.Body.String()
		require.Contains(t, out, "event: error")
		require.Contains(t, out, `"type":"upstream_error"`)
		require.Contains(t, out, `"message":"Mapped by administrator passthrough rule"`)

		streamErr, hasStreamErr := gatewayhttp.GetOpsStreamError(c)
		require.True(t, hasStreamErr)
		require.Equal(t, http.StatusTooManyRequests, streamErr.IntendedStatus)
		require.True(t, streamErr.SkipMonitoring)

		rawEvents, exists := c.Get(gatewayhttp.OpsUpstreamErrorsKey)
		require.True(t, exists)
		events, ok := rawEvents.([]*ops.OpsUpstreamErrorEvent)
		require.True(t, ok)
		require.Len(t, events, 1)
		require.Equal(t, "http_error", events[0].Kind)
		require.Contains(t, events[0].Detail, "vendor_specific_quota_gate")
	})
}

// GetValue looks up key in s.values first and then returns s.prompt when key
// matches SettingKeyIdentityPatchPrompt. Unconfigured setting keys fall back to
// settingscore.ErrSettingNotFound so callers apply runtime defaults.
func (s *antigravityCustomPromptSettingStub) GetValue(_ context.Context, key string) (string, error) {
	if s.values != nil {
		if val, ok := s.values[key]; ok {
			return val, nil
		}
	}
	if key == gateway.SettingKeyIdentityPatchPrompt {
		return s.prompt, nil
	}
	return "", settingscore.ErrSettingNotFound
}

// decodeCapturedAntigravityEnvelope parses raw v1internal JSON bytes into a
// typed capturedAntigravityEnvelope using encoding/json/v2.
func decodeCapturedAntigravityEnvelope(t *testing.T, raw []byte) capturedAntigravityEnvelope {
	t.Helper()
	var env capturedAntigravityEnvelope
	dec := jsontext.NewDecoder(bytes.NewReader(raw))
	require.NoError(t, json.UnmarshalDecode(dec, &env))
	return env
}

// antigravityPathForProtocol returns the canonical gateway HTTP path for source.
func antigravityPathForProtocol(source protocol.ProtocolID) string {
	switch source {
	case protocol.ProtocolAnthropicMessages:
		return "/v1/messages"
	case protocol.ProtocolOpenAIResponses:
		return "/v1/responses"
	default:
		return "/v1/chat/completions"
	}
}

// buildAntigravityMultiTurnFixtureBody constructs a multi-turn client request
// containing system text, empty-object and constrained-string tool schemas,
// prior reasoning carrier, three parallel tool calls, and three corresponding
// tool results for source. Claude models select claudeCarrier while Gemini
// models select geminiCarrier so matching-scheme replay unpacking is exercised.
func buildAntigravityMultiTurnFixtureBody(
	source protocol.ProtocolID,
	model string,
	isClaude bool,
	claudeCarrier, geminiCarrier string,
) []byte {
	carrier := geminiCarrier
	if isClaude {
		carrier = claudeCarrier
	}
	switch source {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{
			"model":"` + model + `",
			"max_tokens":2048,
			"system":[{"type":"text","text":"Follow repo instructions."}],
			"thinking":{"type":"adaptive"},
			"tools":[
				{"name":"ping","description":"Ping host","input_schema":{"type":"object","properties":{}}},
				{"name":"lookup","description":"Lookup item","input_schema":{"type":"object","properties":{"q":{"type":"string","minLength":2}},"required":["q"]}}
			],
			"messages":[
				{"role":"user","content":"Check all three"},
				{"role":"assistant","content":[
					{"type":"thinking","thinking":"prior reasoning","signature":"` + carrier + `"},
					{"type":"tool_use","id":"call_signed","name":"ping","input":{}},
					{"type":"tool_use","id":"call_unsigned_sibling","name":"lookup","input":{"q":"go"}},
					{"type":"tool_use","id":"call_foreign","name":"lookup","input":{"q":"ir"}}
				]},
				{"role":"user","content":[
					{"type":"tool_result","tool_use_id":"call_signed","content":"pong"},
					{"type":"tool_result","tool_use_id":"call_unsigned_sibling","content":"found go"},
					{"type":"tool_result","tool_use_id":"call_foreign","content":"found ir"}
				]}
			]
		}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{
			"model":"` + model + `",
			"instructions":"Follow repo instructions.",
			"reasoning":{"effort":"high"},
			"tools":[
				{"type":"function","name":"ping","description":"Ping host","parameters":{"type":"object","properties":{}}},
				{"type":"function","name":"lookup","description":"Lookup item","parameters":{"type":"object","properties":{"q":{"type":"string","minLength":2}},"required":["q"]}}
			],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"Check all three"}]},
				{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"prior reasoning"}],"encrypted_content":"` + carrier + `"},
				{"type":"function_call","call_id":"call_signed","name":"ping","arguments":"{}"},
				{"type":"function_call","call_id":"call_unsigned_sibling","name":"lookup","arguments":"{\"q\":\"go\"}"},
				{"type":"function_call","call_id":"call_foreign","name":"lookup","arguments":"{\"q\":\"ir\"}"},
				{"type":"function_call_output","call_id":"call_signed","output":"pong"},
				{"type":"function_call_output","call_id":"call_unsigned_sibling","output":"found go"},
				{"type":"function_call_output","call_id":"call_foreign","output":"found ir"}
			]
		}`)
	default:
		return []byte(`{
			"model":"` + model + `",
			"reasoning_effort":"high",
			"tools":[
				{"type":"function","function":{"name":"ping","description":"Ping host","parameters":{"type":"object","properties":{}}}},
				{"type":"function","function":{"name":"lookup","description":"Lookup item","parameters":{"type":"object","properties":{"q":{"type":"string","minLength":2}},"required":["q"]}}}
			],
			"messages":[
				{"role":"system","content":"Follow repo instructions."},
				{"role":"user","content":"Check all three"},
				{"role":"assistant","reasoning_content":"prior reasoning","tool_calls":[
					{"id":"call_signed","type":"function","function":{"name":"ping","arguments":"{}"}},
					{"id":"call_unsigned_sibling","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"go\"}"}},
					{"id":"call_foreign","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"ir\"}"}}
				]},
				{"role":"tool","tool_call_id":"call_signed","content":"pong"},
				{"role":"tool","tool_call_id":"call_unsigned_sibling","content":"found go"},
				{"role":"tool","tool_call_id":"call_foreign","content":"found ir"}
			]
		}`)
	}
}

// readExternalSSEFrames parses all SSE frames emitted into raw using sse.NewReader.
func readExternalSSEFrames(t *testing.T, raw []byte) []sse.Frame {
	t.Helper()
	r := sse.NewReader(bytes.NewReader(raw))
	var frames []sse.Frame
	for r.Scan() {
		frames = append(frames, r.Frame())
	}
	require.NoError(t, r.Err())
	return frames
}

// buildExternalResponseCellRequest builds a streaming or buffered client request
// declaring lookup_order (and custom apply_patch on Responses) for source.
func buildExternalResponseCellRequest(source protocol.ProtocolID, model string, stream bool) []byte {
	streamVal := "false"
	if stream {
		streamVal = "true"
	}
	switch source {
	case protocol.ProtocolAnthropicMessages:
		return []byte(`{"model":"` + model + `","max_tokens":512,"stream":` + streamVal + `,"tools":[{"name":"lookup_order","description":"Lookup order","input_schema":{"type":"object","properties":{"id":{"type":"integer"}}}}],"messages":[{"role":"user","content":"Check order"}]}`)
	case protocol.ProtocolOpenAIResponses:
		return []byte(`{"model":"` + model + `","stream":` + streamVal + `,"tools":[{"type":"custom","name":"apply_patch","description":"Apply patch"},{"type":"function","name":"lookup_order","description":"Lookup order","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}],"input":"Check order"}`)
	default:
		return []byte(`{"model":"` + model + `","stream":` + streamVal + `,"stream_options":{"include_usage":true},"tools":[{"type":"function","function":{"name":"lookup_order","description":"Lookup order","parameters":{"type":"object","properties":{"id":{"type":"integer"}}}}}],"messages":[{"role":"user","content":"Check order"}]}`)
	}
}

// TestAntigravityForwardConverted_StreamingAndBufferedResponsesAllCells runs
// Messages, Responses, and Chat in both streaming and buffered modes across
// claude-* and gemini-* targets. Every cell must satisfy strict stream grammar,
// seal family-scoped trc1 carriers, write CallMemory bindings, record raw
// UpstreamResponseModel, and merge sparse tail usageMetadata counters.
func TestAntigravityForwardConverted_StreamingAndBufferedResponsesAllCells(t *testing.T) {
	models := []struct {
		model      string // target Antigravity model name
		isClaude   bool   // whether model uses antigravity_claude replay rules
		wantScheme string // expected trc1 carrier scheme
	}{
		{model: "claude-opus-4-6-thinking", isClaude: true, wantScheme: ir.SchemeAntigravityClaude},
		{model: "gemini-3.1-pro-high", isClaude: false, wantScheme: ir.SchemeGemini},
	}
	sources := []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	}

	v1SSE := strings.Join([]string{
		`data: {"responseId":"outer_ag_id","modelVersion":"runtime-model-v3","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"deep thought","thought":true,"thoughtSignature":"sig_r_88"},{"functionCall":{"id":"up_tool_88","name":"lookup_order","args":{"id":9007199254740993}},"thoughtSignature":"sig_call_88"},{"text":"Done."}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":25}}}`,
		"",
		`data: {"response":{"usageMetadata":{"candidatesTokenCount":12,"thoughtsTokenCount":8,"totalTokenCount":120}}}`,
		"",
	}, "\n")

	for _, m := range models {
		for _, source := range sources {
			for _, isStream := range []bool{true, false} {
				modeLabel := "buffered"
				if isStream {
					modeLabel = "stream"
				}
				t.Run(string(source)+"/"+m.model+"/"+modeLabel, func(t *testing.T) {
					upstreamStub := &queuedHTTPUpstreamStub{
						responses: []*http.Response{{
							StatusCode: http.StatusOK,
							Header: http.Header{
								"Content-Type": {"text/event-stream"},
								"X-Request-Id": {"ag-cell-req"},
							},
							Body: io.NopCloser(strings.NewReader(v1SSE)),
						}},
					}
					svc := newAntigravityCompatibilityFixture(
						googleforward.Options{
							MaxLineSize: 500 * 1024 * 1024,
						},
						upstreamStub,
					)
					prov := newAntigravityCompatProvider(capability.ProviderTypeOAuth)
					reqBody := buildExternalResponseCellRequest(source, m.model, isStream)
					c, rec := newAntigravityCompatContext(http.MethodPost, antigravityPathForProtocol(source), reqBody)
					c.Set("api_key", &apikey.APIKey{ID: 88})

					res, err := svc.ForwardConverted(
						t.Context(),
						gatewayhttp.NewGoogleBoundary(c, svc.Options, true),
						prov,
						source,
						reqBody,
						false,
					)
					require.NoError(t, err)
					require.NotNil(t, res)
					require.Equal(t, isStream, res.Stream)
					require.Equal(t, "ag-cell-req", res.RequestID)
					require.Equal(t, "runtime-model-v3", res.UpstreamResponseModel)
					require.Equal(t, 75, res.Usage.InputTokens)
					require.Equal(t, 25, res.Usage.CacheReadInputTokens)
					require.Equal(t, 20, res.Usage.OutputTokens)

					outStr := rec.Body.String()
					require.Contains(t, outStr, "9007199254740993")
					if isStream {
						frames := readExternalSSEFrames(t, rec.Body.Bytes())
						require.Empty(t, testkit.ValidateStreamGrammar(source, frames, testkit.GrammarOptions{}))
					} else {
						require.Contains(t, strings.ToLower(rec.Header().Get("Content-Type")), "application/json")
					}

					if source != protocol.ProtocolOpenAIChatCompletions {
						require.Contains(t, outStr, "trc1:"+m.wantScheme+":")
					}

					// Verify CallMemory wrote the tool call binding under (APIKeyID=88, ProviderID, m.model).
					mem := svc.CallMemory(session.GeminiToolScope{
						APIKeyID:   88,
						ProviderID: prov.Record.ID,
						Model:      m.model,
					})
					require.NotNil(t, mem)
					binding, ok := mem.Recall("up_tool_88")
					require.True(t, ok)
					require.Equal(t, "up_tool_88", binding.UpstreamID)
					require.NotNil(t, binding.Token)
					require.Equal(t, m.wantScheme, binding.Token.Scheme)
					require.Equal(t, "sig_call_88", binding.Token.Data)
				})
			}
		}
	}
}

// TestAntigravityForwardConverted_StreamControlFailoverCommittedSSEAndDisconnect
// feeds pre-semantic inline 503 errors, mid-stream truncations, and client
// write failures into ForwardConverted. Pre-semantic failures must failover
// with one Ops event, truncated Responses streams must emit continuous
// response.failed frames, and disconnected clients must drain trailing usage.
func TestAntigravityForwardConverted_StreamControlFailoverCommittedSSEAndDisconnect(t *testing.T) {
	t.Run("leading_non_semantic_frame_then_inline_503_triggers_failover_with_single_ops_event", func(t *testing.T) {
		inlineFailSSE := strings.Join([]string{
			`data: {"response":{"responseId":"ag_lead_1","candidates":[{"index":0,"content":{"role":"model","parts":[]}}],"usageMetadata":{"promptTokenCount":10}}}`,
			"",
			`data: {"error":{"code":503,"message":"Model capacity exhausted","status":"UNAVAILABLE"}}`,
			"",
		}, "\n")
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"req-lead-503"}},
				Body:       io.NopCloser(strings.NewReader(inlineFailSSE)),
			}},
		}
		svc := newAntigravityCompatibilityFixture(
			googleforward.Options{MaxLineSize: 500 * 1024 * 1024},
			upstreamStub,
		)
		reqBody := buildExternalResponseCellRequest(protocol.ProtocolOpenAIResponses, "claude-sonnet-4-5", true)
		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/responses", reqBody)

		res, err := svc.ForwardConverted(
			t.Context(),
			gatewayhttp.NewGoogleBoundary(c, svc.Options, true),
			newAntigravityCompatProvider(capability.ProviderTypeOAuth),
			protocol.ProtocolOpenAIResponses,
			reqBody,
			false,
		)
		require.Nil(t, res)
		var failoverErr *forwardcore.UpstreamFailoverError
		require.ErrorAs(t, err, &failoverErr)
		require.Equal(t, http.StatusServiceUnavailable, failoverErr.StatusCode)
		require.Empty(t, rec.Body.String(), "staged leading non-semantic frame must never flush before failover")

		rawEvents, exists := c.Get(gatewayhttp.OpsUpstreamErrorsKey)
		require.True(t, exists)
		events, ok := rawEvents.([]*ops.OpsUpstreamErrorEvent)
		require.True(t, ok)
		require.Len(t, events, 1, "inline SSE failover must record exactly one Ops event without double penalty")
		require.Equal(t, "failover", events[0].Kind)
	})

	t.Run("mid_stream_truncation_emits_continuous_response_failed_and_returns_partial_usage", func(t *testing.T) {
		truncatedSSE := `data: {"responseId":"resp_mid_trunc","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"partial-midstream-chunk"}]}}],"usageMetadata":{"promptTokenCount":30,"candidatesTokenCount":6}}}` + "\n\n"
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"req-trunc"}},
				Body:       io.NopCloser(strings.NewReader(truncatedSSE)),
			}},
		}
		svc := newAntigravityCompatibilityFixture(
			googleforward.Options{MaxLineSize: 500 * 1024 * 1024},
			upstreamStub,
		)
		reqBody := buildExternalResponseCellRequest(protocol.ProtocolOpenAIResponses, "gemini-3.1-pro-high", true)
		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/responses", reqBody)

		res, err := svc.ForwardConverted(
			t.Context(),
			gatewayhttp.NewGoogleBoundary(c, svc.Options, true),
			newAntigravityCompatProvider(capability.ProviderTypeOAuth),
			protocol.ProtocolOpenAIResponses,
			reqBody,
			false,
		)
		require.Error(t, err)
		require.NotNil(t, res, "partial result must return for billing settlement after semantic output")
		require.Equal(t, 30, res.Usage.InputTokens)
		require.Equal(t, 6, res.Usage.OutputTokens)
		require.Contains(t, rec.Body.String(), "partial-midstream-chunk")
		require.Contains(t, rec.Body.String(), "event: response.failed")
		require.Contains(t, rec.Body.String(), `"id":"resp_mid_trunc"`)

		frames := readExternalSSEFrames(t, rec.Body.Bytes())
		require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{
			AllowAbnormal: true,
		}))
	})

	t.Run("client_disconnect_drains_trailing_usage_for_billing", func(t *testing.T) {
		drainSSE := strings.Join([]string{
			`data: {"response":{"responseId":"resp_disc_drain","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"chunk-1"}]}}],"usageMetadata":{"promptTokenCount":18,"candidatesTokenCount":2}}}`,
			"",
			`data: {"response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"chunk-2"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":18,"candidatesTokenCount":24,"thoughtsTokenCount":6}}}`,
			"",
		}, "\n")
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(drainSSE)),
			}},
		}
		svc := newAntigravityCompatibilityFixture(
			googleforward.Options{MaxLineSize: 500 * 1024 * 1024},
			upstreamStub,
		)
		reqBody := buildExternalResponseCellRequest(protocol.ProtocolOpenAIChatCompletions, "claude-sonnet-4-5", true)
		c, _ := newAntigravityCompatContext(http.MethodPost, "/v1/chat/completions", reqBody)
		c.Writer = &antigravityFailingWriter{ResponseWriter: c.Writer, failAfter: 1}

		res, err := svc.ForwardConverted(
			t.Context(),
			gatewayhttp.NewGoogleBoundary(c, svc.Options, true),
			newAntigravityCompatProvider(capability.ProviderTypeOAuth),
			protocol.ProtocolOpenAIChatCompletions,
			reqBody,
			false,
		)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.True(t, res.ClientDisconnect)
		require.Equal(t, 18, res.Usage.InputTokens)
		require.Equal(t, 30, res.Usage.OutputTokens)
	})
}

// TestAntigravityForwardConverted_ClaudeIRAndGeminiRecoveryPipelines triggers
// signature, budget-constraint, and model-not-found errors across claude-* and
// gemini-* Antigravity exchanges. RecoverClaudeIR and RecoverGemini must retry
// cleanly while preserving lowered tool restoration and trc1 carrier sealing.
func TestAntigravityForwardConverted_ClaudeIRAndGeminiRecoveryPipelines(t *testing.T) {
	t.Run("claude_signature_400_recovers_and_preserves_exchange_custom_tool_and_carrier", func(t *testing.T) {
		staleCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAntigravityClaude,
			Data:   "stale_sig_from_other_claude",
			Text:   "prior claude reasoning",
		})
		require.NoError(t, err)

		reqBody := []byte(`{
			"model":"claude-opus-4-6-thinking",
			"stream":false,
			"reasoning":{"effort":"high"},
			"tools":[{"type":"custom","name":"apply_patch","description":"Apply patch"}],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"Fix handler"}]},
				{"type":"reasoning","id":"rs_prev","summary":[],"encrypted_content":"` + staleCarrier + `"},
				{"type":"custom_tool_call","call_id":"call_p1","name":"apply_patch","input":"*** Begin Patch"},
				{"type":"custom_tool_call_output","call_id":"call_p1","output":"patched"}
			]
		}`)

		recoveredSSE := `data: {"responseId":"resp_claude_rec","modelVersion":"claude-opus-4-6-thinking","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"fresh opus thought","thought":true,"thoughtSignature":"sig_opus_new"},{"functionCall":{"id":"call_p2","name":"apply_patch","args":{"input":"*** End Patch"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":32,"candidatesTokenCount":7,"thoughtsTokenCount":4}}}` + "\n\n"
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{
				{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"X-Request-Id": {"req-sig-1"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"Invalid signature in thinking block"}}`)),
				},
				{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"req-sig-2"}},
					Body:       io.NopCloser(strings.NewReader(recoveredSSE)),
				},
			},
		}
		tokenProvider := newAntigravityTokenSourceForTest(&antigravityCompatTokenCache{token: "fresh-oauth-token"})
		svc := newAntigravityFixture(antigravityDependencies{
			tokenProvider: tokenProvider,
			httpUpstream:  upstreamStub,
			settingService: newExecutionReadersFixture(&antigravityCustomPromptSettingStub{
				prompt: "Custom operator identity.",
			}),
			options: fixtureOptions(&googleforward.Options{
				MaxLineSize: 500 * 1024 * 1024,
			}),
		})

		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/responses", reqBody)
		res, execErr := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolOpenAIResponses, reqBody, false)
		require.NoError(t, execErr)
		require.NotNil(t, res)
		require.Equal(t, "req-sig-2", res.RequestID)
		require.Len(t, upstreamStub.requestBodies, 2)

		retryEnv := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[1])
		require.Equal(t, "Custom operator identity.", retryEnv.Request.SystemInstruction.Parts[0].Text)
		require.NotContains(t, string(retryEnv.Request.GenerationConfig), "thinkingConfig")

		outJSON := rec.Body.String()
		require.Contains(t, outJSON, `"type":"custom_tool_call"`)
		require.Contains(t, outJSON, `"input":"*** End Patch"`)
		require.Contains(t, outJSON, "trc1:antigravity_claude:")
	})

	t.Run("claude_budget_constraint_400_rectifies_thinking_budget", func(t *testing.T) {
		reqBody := []byte(`{
			"model":"claude-sonnet-4-5-thinking",
			"max_tokens":2048,
			"thinking":{"type":"enabled","budget_tokens":512},
			"messages":[{"role":"user","content":"Check budget"}]
		}`)
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{
				{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"X-Request-Id": {"req-bud-1"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"thinking.budget_tokens: Input should be greater than or equal to 1024"}}`)),
				},
				antigravityCompatSuccessResponse(),
			},
		}
		tokenProvider := newAntigravityTokenSourceForTest(&antigravityCompatTokenCache{token: "fresh-oauth-token"})
		svc := newAntigravityFixture(antigravityDependencies{
			tokenProvider:  tokenProvider,
			httpUpstream:   upstreamStub,
			settingService: newExecutionReadersFixture(&antigravityCustomPromptSettingStub{}),
			options: fixtureOptions(&googleforward.Options{
				MaxLineSize: 500 * 1024 * 1024,
			}),
		})

		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/messages", reqBody)
		res, err := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolAnthropicMessages, reqBody, false)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Len(t, upstreamStub.requestBodies, 2)

		retryEnv := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[1])
		require.Contains(t, string(retryEnv.Request.GenerationConfig), `"thinkingBudget":32000`)
		require.Contains(t, string(retryEnv.Request.GenerationConfig), `"maxOutputTokens":64000`)
	})

	t.Run("gemini_model_404_fallback_reencodes_and_persists_fallback_call_memory_across_turns", func(t *testing.T) {
		turn1Req := []byte(`{
			"model":"gemini-3.1-pro-high",
			"stream":false,
			"tools":[{"type":"function","name":"lookup_order","description":"Lookup order","parameters":{"type":"object","properties":{"order":{"type":"string"}}}}],
			"input":"Check order A1"
		}`)
		turn1FallbackSSE := `data: {"responseId":"gem_fb_t1","modelVersion":"gemini-2.5-pro","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"up_fb_call_1","name":"lookup_order","args":{"order":"A1"}},"thoughtSignature":"sig_fb_25_pro_turn1"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":6}}}` + "\n\n"
		turn2FallbackSSE := `data: {"responseId":"gem_fb_t2","modelVersion":"gemini-2.5-pro","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Order A1 shipped"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":24,"candidatesTokenCount":4}}}` + "\n\n"

		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{
				{
					StatusCode: http.StatusNotFound,
					Header:     http.Header{"X-Request-Id": {"req-t1-404"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":404,"message":"Model gemini-3.1-pro-high was not found","status":"NOT_FOUND"}}`)),
				},
				{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"req-t1-fb-200"}},
					Body:       io.NopCloser(strings.NewReader(turn1FallbackSSE)),
				},
				{
					StatusCode: http.StatusNotFound,
					Header:     http.Header{"X-Request-Id": {"req-t2-404"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":404,"message":"Model gemini-3.1-pro-high was not found","status":"NOT_FOUND"}}`)),
				},
				{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"req-t2-fb-200"}},
					Body:       io.NopCloser(strings.NewReader(turn2FallbackSSE)),
				},
			},
		}
		tokenProvider := newAntigravityTokenSourceForTest(&antigravityCompatTokenCache{token: "fresh-oauth-token"})
		svc := newAntigravityFixture(antigravityDependencies{
			tokenProvider: tokenProvider,
			httpUpstream:  upstreamStub,
			settingService: newExecutionReadersFixture(&antigravityCustomPromptSettingStub{
				values: map[string]string{
					routing.SettingKeyEnableModelFallback:      "true",
					routing.SettingKeyFallbackModelAntigravity: "gemini-2.5-pro",
				},
			}),
			options: fixtureOptions(&googleforward.Options{
				MaxLineSize: 500 * 1024 * 1024,
			}),
		})
		prov := newAntigravityCompatProvider(capability.ProviderTypeOAuth)

		c1, rec1 := newAntigravityCompatContext(http.MethodPost, "/v1/responses", turn1Req)
		c1.Set("api_key", &apikey.APIKey{ID: 91})
		res1, execErr1 := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c1, svc.Options, true), prov, protocol.ProtocolOpenAIResponses, turn1Req, false)
		require.NoError(t, execErr1)
		require.NotNil(t, res1)
		require.Equal(t, http.StatusOK, rec1.Code)
		require.Len(t, upstreamStub.requestBodies, 2)

		var turn1Reply capturedResponsesEnvelope
		require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(bytes.NewReader(rec1.Body.Bytes())), &turn1Reply))
		var clientCallID string
		for _, item := range turn1Reply.Output {
			if item.Type == "function_call" {
				clientCallID = item.CallID
			}
		}
		require.NotEmpty(t, clientCallID)

		// Verify CallMemory recorded Turn 1's signature under gemini-2.5-pro (fbScope)
		// rather than the missing initial model scope.
		fbMem := svc.CallMemory(session.GeminiToolScope{
			APIKeyID:   91,
			ProviderID: prov.Record.ID,
			Model:      "gemini-2.5-pro",
		})
		fbBinding, fbHit := fbMem.Recall(clientCallID)
		require.True(t, fbHit, "fallback model CallMemory scope must store Turn 1 tool call binding")
		require.Equal(t, "up_fb_call_1", fbBinding.UpstreamID)
		require.NotNil(t, fbBinding.Token)
		require.Equal(t, "sig_fb_25_pro_turn1", fbBinding.Token.Data)

		origMem := svc.CallMemory(session.GeminiToolScope{
			APIKeyID:   91,
			ProviderID: prov.Record.ID,
			Model:      "gemini-3.1-pro-high",
		})
		_, origHit := origMem.Recall(clientCallID)
		require.False(t, origHit, "missing initial model scope must not receive fallback response bindings")

		// Turn 2: omit reasoning items so Prepare on the second 404 fallback must
		// recall sig_fb_25_pro_turn1 and up_fb_call_1 from fbScope CallMemory.
		turn2Req := []byte(`{
			"model":"gemini-3.1-pro-high",
			"stream":false,
			"tools":[{"type":"function","name":"lookup_order","description":"Lookup order","parameters":{"type":"object","properties":{"order":{"type":"string"}}}}],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"Check order A1"}]},
				{"type":"function_call","call_id":"` + clientCallID + `","name":"lookup_order","arguments":"{\"order\":\"A1\"}"},
				{"type":"function_call_output","call_id":"` + clientCallID + `","output":"shipped"}
			]
		}`)
		c2, rec2 := newAntigravityCompatContext(http.MethodPost, "/v1/responses", turn2Req)
		c2.Set("api_key", &apikey.APIKey{ID: 91})
		res2, execErr2 := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c2, svc.Options, true), prov, protocol.ProtocolOpenAIResponses, turn2Req, false)
		require.NoError(t, execErr2)
		require.NotNil(t, res2)
		require.Equal(t, http.StatusOK, rec2.Code)
		require.Len(t, upstreamStub.requestBodies, 4)

		turn2FallbackEnv := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[3])
		require.Equal(t, "gemini-2.5-pro", turn2FallbackEnv.Model)
		var recalledCall *capturedGeminiPart
		for _, content := range turn2FallbackEnv.Request.Contents {
			for i := range content.Parts {
				if content.Parts[i].FunctionCall != nil {
					recalledCall = &content.Parts[i]
				}
			}
		}
		require.NotNil(t, recalledCall)
		require.Equal(t, "up_fb_call_1", recalledCall.FunctionCall.ID)
		require.Equal(t, "sig_fb_25_pro_turn1", recalledCall.ThoughtSignature)
		require.Contains(t, rec2.Body.String(), "Order A1 shipped")
	})

	t.Run("gemini_model_404_cross_family_fallback_seals_antigravity_claude_carrier", func(t *testing.T) {
		reqBody := []byte(`{"model":"gemini-3.1-pro-high","stream":false,"input":"Cross family fallback"}`)
		claudeFallbackSSE := `data: {"responseId":"cl_fb_ok","modelVersion":"claude-sonnet-4-5","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"claude fallback thought","thought":true,"thoughtSignature":"sig_claude_fb"},{"text":"claude fallback answer"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":14,"candidatesTokenCount":5,"thoughtsTokenCount":3}}}` + "\n\n"
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{
				{
					StatusCode: http.StatusNotFound,
					Header:     http.Header{"X-Request-Id": {"req-cf-404"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":404,"message":"Model gemini-3.1-pro-high was not found","status":"NOT_FOUND"}}`)),
				},
				{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"req-cf-200"}},
					Body:       io.NopCloser(strings.NewReader(claudeFallbackSSE)),
				},
			},
		}
		tokenProvider := newAntigravityTokenSourceForTest(&antigravityCompatTokenCache{token: "fresh-oauth-token"})
		svc := newAntigravityFixture(antigravityDependencies{
			tokenProvider: tokenProvider,
			httpUpstream:  upstreamStub,
			settingService: newExecutionReadersFixture(&antigravityCustomPromptSettingStub{
				values: map[string]string{
					routing.SettingKeyEnableModelFallback:      "true",
					routing.SettingKeyFallbackModelAntigravity: "claude-sonnet-4-5",
				},
			}),
			options: fixtureOptions(&googleforward.Options{
				MaxLineSize: 500 * 1024 * 1024,
			}),
		})

		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/responses", reqBody)
		res, execErr := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolOpenAIResponses, reqBody, false)
		require.NoError(t, execErr)
		require.NotNil(t, res)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Contains(t, rec.Body.String(), "trc1:antigravity_claude:")

		var reply capturedResponsesEnvelope
		require.NoError(t, json.UnmarshalDecode(jsontext.NewDecoder(bytes.NewReader(rec.Body.Bytes())), &reply))
		var carrierStr string
		for _, item := range reply.Output {
			if item.Type == "reasoning" {
				carrierStr = item.EncryptedContent
			}
		}
		parsedCarrier, ok := ir.ParseCarrier(carrierStr)
		require.True(t, ok)
		require.Equal(t, ir.SchemeAntigravityClaude, parsedCarrier.Scheme)
		require.Equal(t, "claude fallback thought", parsedCarrier.Text)
	})

	t.Run("gemini_signature_400_replaces_thought_signatures_and_retains_exchange", func(t *testing.T) {
		gemCarrier, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeGemini,
			Data:   "stale_gemini_thought_sig",
		})
		require.NoError(t, err)

		reqBody := []byte(`{
			"model":"gemini-3.1-pro-high",
			"stream":false,
			"tools":[{"type":"function","name":"check_id","description":"Check ID","parameters":{"type":"object","properties":{"n":{"type":"integer"}}}}],
			"input":[
				{"type":"message","role":"user","content":[{"type":"input_text","text":"Check ID"}]},
				{"type":"reasoning","id":"rs_gem","summary":[{"type":"summary_text","text":"gem summary"}],"encrypted_content":"` + gemCarrier + `"},
				{"type":"function_call","call_id":"call_g1","name":"check_id","arguments":"{\"n\":42}"},
				{"type":"function_call_output","call_id":"call_g1","output":"ok"}
			]
		}`)

		gemSuccessSSE := `data: {"responseId":"gem_rec_ok","modelVersion":"gemini-3.1-pro-high","response":{"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"fresh gem thought","thought":true,"thoughtSignature":"sig_gem_fresh"},{"text":"recovered"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":22,"candidatesTokenCount":4,"thoughtsTokenCount":3}}}` + "\n\n"
		upstreamStub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{
				{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"X-Request-Id": {"req-400-sig"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"Corrupted thoughtSignature in request"}}`)),
				},
				{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Request-Id": {"req-200-ok"}},
					Body:       io.NopCloser(strings.NewReader(gemSuccessSSE)),
				},
			},
		}
		tokenProvider := newAntigravityTokenSourceForTest(&antigravityCompatTokenCache{token: "fresh-oauth-token"})
		svc := newAntigravityFixture(antigravityDependencies{
			tokenProvider:  tokenProvider,
			httpUpstream:   upstreamStub,
			settingService: newExecutionReadersFixture(&antigravityCustomPromptSettingStub{}),
			options: fixtureOptions(&googleforward.Options{
				MaxLineSize: 500 * 1024 * 1024,
			}),
		})

		c, rec := newAntigravityCompatContext(http.MethodPost, "/v1/responses", reqBody)
		res, execErr := svc.ForwardConverted(t.Context(), gatewayhttp.NewGoogleBoundary(c, svc.Options, true), newAntigravityCompatProvider(capability.ProviderTypeOAuth), protocol.ProtocolOpenAIResponses, reqBody, false)
		require.NoError(t, execErr)
		require.NotNil(t, res)
		require.Equal(t, http.StatusOK, rec.Code)
		require.Len(t, upstreamStub.requestBodies, 2)

		cleanedEnv := decodeCapturedAntigravityEnvelope(t, upstreamStub.requestBodies[1])
		var modelTurnParts []capturedGeminiPart
		for _, turn := range cleanedEnv.Request.Contents {
			if turn.Role == "model" {
				modelTurnParts = append(modelTurnParts, turn.Parts...)
			}
		}
		require.NotEmpty(t, modelTurnParts)
		for _, p := range modelTurnParts {
			if p.ThoughtSignature != "" {
				require.Equal(t, "skip_thought_signature_validator", p.ThoughtSignature)
			}
		}
		require.Contains(t, rec.Body.String(), "trc1:gemini:")
	})
}

// TestReplaceGeminiThoughtSignaturesLossless_PreservesInt64AndMemberOrder feeds
// inner Gemini request JSON carrying >53-bit integer literals, nested objects,
// and exact "thoughtSignature" fields into ReplaceThoughtSignaturesLosslessForTest.
// Signature values must change to the placeholder while 64-bit integer literals,
// member ordering, and malformed fallback bytes remain untouched.
func TestReplaceGeminiThoughtSignaturesLossless_PreservesInt64AndMemberOrder(t *testing.T) {
	raw := []byte(`{"zFirst":1,"contents":[{"role":"model","parts":[{"text":"t","thought":true,"thoughtSignature":"stale_sig_1"},{"functionCall":{"name":"check_id","args":{"seq":9007199254740993,"note":"thoughtSignature stays literal inside string"}},"thoughtSignature":"stale_sig_2"}]}],"aLast":true}`)
	got := googleforward.ReplaceThoughtSignaturesLosslessForTest(raw, "skip_thought_signature_validator")
	want := `{"zFirst":1,"contents":[{"role":"model","parts":[{"text":"t","thought":true,"thoughtSignature":"skip_thought_signature_validator"},{"functionCall":{"name":"check_id","args":{"seq":9007199254740993,"note":"thoughtSignature stays literal inside string"}},"thoughtSignature":"skip_thought_signature_validator"}]}],"aLast":true}`
	require.Equal(t, want, string(got))

	malformed := []byte(`{"contents":[{"thoughtSignature":`)
	require.Equal(t, malformed, googleforward.ReplaceThoughtSignaturesLosslessForTest(malformed, "skip"))
	require.Nil(t, googleforward.ReplaceThoughtSignaturesLosslessForTest(nil, "skip"))
}

// newAntigravityCompatProvider creates a test target with the supplied provider
// kind. It carries a stale record token, project, and selected identity
// mappings so compatibility fixtures can observe cached-token replacement.
func newAntigravityCompatProvider(providerType string) *gatewayprovider.ExecutionProvider {
	return &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           3757,
			Name:         "antigravity-compat",
			Platform:     capability.PlatformAntigravity,
			Type:         providerType,
			Status:       billing.StatusActive,
			Concurrency:  1,
			Credentials: map[string]any{
				"access_token": "stale-provider-token",
				"project_id":   "project-3757",
				"model_mapping": map[string]any{
					"gemini-3.1-pro-high":      "gemini-3.1-pro-high",
					"claude-sonnet-4-5":        "claude-sonnet-4-5",
					"claude-opus-4-6-thinking": "claude-opus-4-6-thinking",
				},
			},
		},
	}
}

// antigravityCompatSuccessResponse creates a fresh stream response containing
// text, terminal reason, and usage. Its reader is handed to platform execution
// for response consumption and closure during each fixture call.
func antigravityCompatSuccessResponse() *http.Response {
	body := `data: {"response":{"responseId":"resp_3757","candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":3}}}` + "\n\n"
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{"text/event-stream"},
			"X-Request-Id": []string{"request-3757"},
		},
		Body: io.NopCloser(strings.NewReader(body)),
	}
}
