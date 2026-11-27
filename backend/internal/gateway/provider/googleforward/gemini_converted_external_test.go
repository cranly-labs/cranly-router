package googleforward_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/apikey"
	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/httpapi"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/googleforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// TestGeminiForwardConverted_CallMemoryReplayAndSignatureRetryWithStripEffort
// exercises two-turn tool-call replay, late streamed thoughtSignature updates,
// scope isolation, and HTTP 400 signature-rejection compatibility retry across
// Messages, Responses, and Chat Completions. Retries must drop replayed tokens
// and CallMemory IDs, write skip_thought_signature_validator on the step's
// first call, preserve StripEffort, and still seal new response tokens onto
// the successful reply.
func TestGeminiForwardConverted_CallMemoryReplayAndSignatureRetryWithStripEffort(t *testing.T) {
	const model = "gemini-2.5-flash"
	const answerWithNewThought = `{"candidates":[{"content":{"parts":[{"text":"fresh thought","thought":true,"thoughtSignature":"fresh-sig-turn2"},{"text":"done"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":12,"candidatesTokenCount":4}}`

	for _, proto := range []string{"messages", "responses", "chat"} {
		for _, mode := range []string{"buffered", "stream", "oauth_collected"} {
			t.Run(proto+"/"+mode, func(t *testing.T) {
				isStream := mode == "stream"
				isOAuth := mode == "oauth_collected"

				stub := &queuedHTTPUpstreamStub{}
				svc := newGeminiFixture(geminiDependencies{
					httpUpstream:  stub,
					cfg:           &googleforward.Options{},
					tokenProvider: newGeminiTokenSourceForTest(),
				})
				executor := &httpapi.GeminiExecutor{Runtime: svc}

				provider := &gatewayprovider.ExecutionProvider{
					Record: providercore.Record{
						ID:       201,
						Platform: capability.PlatformGemini,
						Type:     capability.ProviderTypeAPIKey,
						Credentials: map[string]any{
							"api_key": "fixture-key",
							providercore.EffortModelMappingCredentialKey: map[string]any{
								"strip-alias": map[string]any{"high": model},
							},
							providercore.EffortModelMappingStripEffortCredentialKey: true,
						},
						LoadLocation: time.LoadLocation,
					},
				}

				firstFrame := `{"candidates":[{"content":{"parts":[{"functionCall":{"id":"gemini-paris","name":"lookup","args":{"city":"Paris"}}},{"functionCall":{"id":"gemini-london","name":"lookup","args":{"city":"London"}}}]}}]}`
				secondFrame := `{"candidates":[{"content":{"parts":[{"functionCall":{"id":"gemini-paris","name":"lookup","args":{"city":"Paris"}},"thoughtSignature":"original-signature"},{"functionCall":{"id":"gemini-london","name":"lookup","args":{"city":"London"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":3}}`

				contentType := "application/json"
				rawTurn1 := secondFrame
				rawTurn2 := answerWithNewThought
				if isStream {
					contentType = "text/event-stream"
					rawTurn1 = "data: " + firstFrame + "\n\ndata: " + secondFrame + "\n\n"
					rawTurn2 = "data: " + answerWithNewThought + "\n\n"
				} else if isOAuth {
					provider.Record.Type = capability.ProviderTypeOAuth
					provider.Record.Credentials["access_token"] = "ya29.fixture"
					provider.Record.Credentials["project_id"] = "proj-ir-1"
					contentType = "text/event-stream"
					frag1 := `{"candidates":[{"content":{"parts":[{"functionCall":{"id":"gemini-paris","name":"lookup","args":"{\"city\":"}}]}}]}`
					frag2 := `{"candidates":[{"content":{"parts":[{"functionCall":{"id":"gemini-paris","name":"lookup","args":"\"Paris\"}"},"thoughtSignature":"original-signature"},{"functionCall":{"id":"gemini-london","name":"lookup","args":{"city":"London"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":3}}`
					rawTurn1 = "data: {\"response\":" + frag1 + "}\n\ndata: {\"response\":" + frag2 + "}\n\n"
					rawTurn2 = "data: {\"response\":" + answerWithNewThought + "}\n\n"
				}

				stub.responses = []*http.Response{{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": {contentType}},
					Body:       io.NopCloser(strings.NewReader(rawTurn1)),
				}}

				turn1Body := fmt.Sprintf(`{"model":%q,"max_tokens":128,"stream":%t,"messages":[{"role":"user","content":"weather"}]}`, model, isStream)
				path := "/v1/messages"
				switch proto {
				case "responses":
					turn1Body = fmt.Sprintf(`{"model":%q,"stream":%t,"input":"weather"}`, model, isStream)
					path = "/v1/responses"
				case "chat":
					turn1Body = fmt.Sprintf(`{"model":%q,"stream":%t,"messages":[{"role":"user","content":"weather"}]}`, model, isStream)
					path = "/v1/chat/completions"
				}

				rec1 := httptest.NewRecorder()
				c1, _ := gin.CreateTestContext(rec1)
				c1.Set("api_key", &apikey.APIKey{ID: 31})
				c1.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(turn1Body))

				switch proto {
				case "messages":
					_, err := executor.Forward(t.Context(), c1, provider, []byte(turn1Body))
					require.NoError(t, err)
				case "responses":
					_, err := executor.ForwardConverted(t.Context(), c1, provider, protocol.ProtocolOpenAIResponses, []byte(turn1Body))
					require.NoError(t, err)
				case "chat":
					_, err := executor.ForwardConverted(t.Context(), c1, provider, protocol.ProtocolOpenAIChatCompletions, []byte(turn1Body))
					require.NoError(t, err)
				}

				ids := extractClientToolIDsForTest(rec1.Body.String(), proto, isStream)
				require.Len(t, ids, 2)
				require.NotEqual(t, ids[0], ids[1])

				// Replay under same scope, mismatched key, and signature-rejection 400 with StripEffort active.
				for _, tc := range []struct {
					name       string // subtest label
					keyID      int64  // API key ID scoping CallMemory
					reqModel   string // client requested model
					stripCheck bool   // whether to exercise StripEffort on effort-mapped alias
					reject     bool   // whether upstream rejects first attempt with signature 400
					wantMatch  bool   // whether first attempt restores CallMemory IDs and signature
				}{
					{name: "same_scope", keyID: 31, reqModel: model, wantMatch: true},
					{name: "different_key_miss", keyID: 99, reqModel: model, wantMatch: false},
					{name: "signature_rejected_with_strip_effort", keyID: 31, reqModel: "strip-alias", stripCheck: true, reject: true, wantMatch: true},
				} {
					t.Run(tc.name, func(t *testing.T) {
						carrier, cErr := ir.FormatCarrier(ir.Carrier{Scheme: ir.SchemeGemini, Data: "stale-thought-sig"})
						require.NoError(t, cErr)

						turn2Body := buildReplayTurnBodyForTest(proto, tc.reqModel, isStream, carrier, ids[0], ids[1], tc.stripCheck)
						firstIdx := stub.callCount
						if tc.reject {
							stub.responses = append(stub.responses, &http.Response{
								StatusCode: http.StatusBadRequest,
								Header:     http.Header{"X-Goog-Request-Id": {"sig-400"}},
								Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"message":"Corrupted thought signature."}}`)),
							})
						}
						stub.responses = append(stub.responses, &http.Response{
							StatusCode: http.StatusOK,
							Header:     http.Header{"Content-Type": {contentType}},
							Body:       io.NopCloser(strings.NewReader(rawTurn2)),
						})

						rec2 := httptest.NewRecorder()
						c2, _ := gin.CreateTestContext(rec2)
						c2.Set("api_key", &apikey.APIKey{ID: tc.keyID})
						reqCtx := t.Context()
						if tc.stripCheck {
							reqCtx = requeststate.WithRoutingReasoningEffort(reqCtx, "high")
						}
						c2.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(turn2Body)).WithContext(reqCtx)

						res, err := executor.ForwardConverted(reqCtx, c2, provider, clientProtoFromSlug(proto), []byte(turn2Body))
						require.NoError(t, err)
						require.NotNil(t, res)

						prefix := "contents"
						genCfgPrefix := "generationConfig"
						if isOAuth {
							prefix = "request.contents"
							genCfgPrefix = "request.generationConfig"
						}

						wire1 := stub.requestBodies[firstIdx]
						if tc.stripCheck {
							// StripEffort must prevent residual effort-derived thinkingConfig on both initial and retry payloads.
							require.False(t, gjson.GetBytes(wire1, genCfgPrefix+".thinkingConfig.thinkingLevel").Exists())
							require.NotNil(t, res.ReasoningEffort)
							require.Equal(t, "high", *res.ReasoningEffort)
						}

						firstCallIdx := 0
						if proto != "chat" {
							// First assistant part is the replayed Gemini thought block carrying stale-thought-sig.
							require.Equal(t, "stale-thought-sig", gjson.GetBytes(wire1, prefix+".1.parts.0.thoughtSignature").String())
							firstCallIdx = 1
						}
						firstCall := gjson.GetBytes(wire1, fmt.Sprintf("%s.1.parts.%d", prefix, firstCallIdx))
						secondCall := gjson.GetBytes(wire1, fmt.Sprintf("%s.1.parts.%d", prefix, firstCallIdx+1))
						if tc.wantMatch {
							require.Equal(t, "gemini-paris", firstCall.Get("functionCall.id").String())
							require.Equal(t, "original-signature", firstCall.Get("thoughtSignature").String())
							require.Equal(t, "gemini-london", secondCall.Get("functionCall.id").String())
							require.False(t, secondCall.Get("thoughtSignature").Exists())
						} else {
							require.False(t, firstCall.Get("functionCall.id").Exists())
							require.Equal(t, "skip_thought_signature_validator", firstCall.Get("thoughtSignature").String())
						}

						if tc.reject {
							require.Equal(t, firstIdx+2, stub.callCount)
							wireRetry := stub.requestBodies[firstIdx+1]
							require.NotContains(t, string(wireRetry), "stale-thought-sig")
							require.NotContains(t, string(wireRetry), "original-signature")
							retryFirst := gjson.GetBytes(wireRetry, prefix+".1.parts.0")
							retrySecond := gjson.GetBytes(wireRetry, prefix+".1.parts.1")
							require.False(t, retryFirst.Get("functionCall.id").Exists())
							require.Equal(t, "skip_thought_signature_validator", retryFirst.Get("thoughtSignature").String())
							require.False(t, retrySecond.Get("functionCall.id").Exists())
							if tc.stripCheck {
								require.False(t, gjson.GetBytes(wireRetry, genCfgPrefix+".thinkingConfig.thinkingLevel").Exists())
							}
							// Successful retry response must still seal new Gemini thought signatures for Messages and Responses clients.
							if proto != "chat" {
								require.Contains(t, rec2.Body.String(), "trc1:gemini:")
							}
						} else {
							require.Equal(t, firstIdx+1, stub.callCount)
						}
					})
				}
			})
		}
	}
}

// TestGeminiForwardConverted_CursorInputShapedChatSingleHopPreservesToolMapAndCarrier
// sends a Cursor /v1/chat/completions payload carrying top-level input (no
// messages), a trc1:gemini reasoning carrier, and a custom free-form tool.
// ForwardConverted must convert in a single hop without losing the Gemini
// thought signature or custom tool unwrapping, while returning Chat framing.
func TestGeminiForwardConverted_CursorInputShapedChatSingleHopPreservesToolMapAndCarrier(t *testing.T) {
	carrier, err := ir.FormatCarrier(ir.Carrier{Scheme: ir.SchemeGemini, Data: "cursor-gem-sig"})
	require.NoError(t, err)

	stub := &queuedHTTPUpstreamStub{
		responses: []*http.Response{{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": {"text/event-stream"}},
			Body: io.NopCloser(strings.NewReader(
				"data: {\"responseId\":\"gem_cur_1\",\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"name\":\"apply_patch\",\"args\":{\"input\":\"*** Begin Patch\\n+ok\\n*** End Patch\"}}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":22,\"candidatesTokenCount\":6}}\n\n",
			)),
		}},
	}
	svc := newGeminiFixture(geminiDependencies{
		httpUpstream: stub,
		cfg:          &googleforward.Options{},
	})
	provider := &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			ID:           301,
			Platform:     capability.PlatformGemini,
			Type:         capability.ProviderTypeAPIKey,
			Credentials:  map[string]any{"api_key": "gem-key"},
			LoadLocation: time.LoadLocation,
		},
	}

	cursorBody := fmt.Sprintf(`{
		"model":"gemini-2.5-pro",
		"stream":true,
		"stream_options":{"include_usage":true},
		"reasoning_effort":"high",
		"tools":[{"type":"custom","name":"apply_patch","description":"patch file"}],
		"input":[
			{"role":"user","content":"update main.go"},
			{"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"plan"}],"encrypted_content":%q},
			{"role":"assistant","content":[{"type":"output_text","text":"ready"}]},
			{"role":"user","content":"go ahead"}
		]
	}`, carrier)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Set("api_key", &apikey.APIKey{ID: 44})
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(cursorBody))

	res, err := svc.ForwardConverted(t.Context(), httpapi.NewGoogleBoundary(c, svc.Options, false), provider, protocol.ProtocolOpenAIChatCompletions, []byte(cursorBody))
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.ReasoningEffort)
	require.Equal(t, "high", *res.ReasoningEffort)
	require.Equal(t, 1, stub.callCount)

	sent := stub.requestBodies[0]
	// Single-hop conversion must unpack trc1:gemini onto the model thought part and lower custom apply_patch.
	require.Equal(t, "cursor-gem-sig", gjson.GetBytes(sent, "contents.1.parts.0.thoughtSignature").String())
	require.Equal(t, "apply_patch", gjson.GetBytes(sent, "tools.0.functionDeclarations.0.name").String())

	// Response must be Chat Completions SSE with unwrapped free-text custom tool arguments and usage chunk.
	out := rec.Body.String()
	require.Contains(t, out, `"object":"chat.completion.chunk"`)
	require.Contains(t, out, `"name":"apply_patch"`)
	require.Contains(t, out, "*** Begin Patch")
	require.Contains(t, out, `"prompt_tokens":22,"completion_tokens":6,"total_tokens":28`)
	require.Contains(t, out, "data: [DONE]")
}

// TestGeminiForwardConverted_AIStudioSearchNormalizationAndCodeAssistWrapping
// sends a Responses hosted web_search request through ForwardConverted onto
// AI Studio API-key and project-bearing Code Assist OAuth Gemini targets. AI
// Studio REST requests must normalize googleSearch to google_search, whereas
// Code Assist OAuth requests must wrap v1internal SSE without snake-case tool
// renaming.
func TestGeminiForwardConverted_AIStudioSearchNormalizationAndCodeAssistWrapping(t *testing.T) {
	reqBody := `{"model":"gemini-2.5-flash","stream":false,"tools":[{"type":"web_search"}],"input":"latest go release"}`

	t.Run("ai_studio_api_key_normalizes_google_search", func(t *testing.T) {
		var capturedURL string
		stub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"Go 1.27"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":3}}`)),
			}},
			onCall: func(req *http.Request, _ *queuedHTTPUpstreamStub) {
				capturedURL = req.URL.String()
			},
		}
		svc := newGeminiFixture(geminiDependencies{
			httpUpstream: stub,
			cfg:          &googleforward.Options{},
		})
		provider := &gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				ID:           601,
				Platform:     capability.PlatformGemini,
				Type:         capability.ProviderTypeAPIKey,
				Credentials:  map[string]any{"api_key": "ai-studio-key"},
				LoadLocation: time.LoadLocation,
			},
		}

		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(reqBody))

		res, err := svc.ForwardConverted(t.Context(), httpapi.NewGoogleBoundary(c, svc.Options, false), provider, protocol.ProtocolOpenAIResponses, []byte(reqBody))
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, capturedURL, "/v1beta/models/gemini-2.5-flash:generateContent")

		wire := stub.requestBodies[0]
		require.True(t, gjson.GetBytes(wire, "tools.0.google_search").Exists())
		require.False(t, gjson.GetBytes(wire, "tools.0.googleSearch").Exists())
	})

	t.Run("code_assist_oauth_wraps_v1internal_without_ai_studio_rename", func(t *testing.T) {
		var capturedURL string
		stub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader("data: {\"response\":{\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Go 1.27\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":3}}}\n\n")),
			}},
			onCall: func(req *http.Request, _ *queuedHTTPUpstreamStub) {
				capturedURL = req.URL.String()
			},
		}
		svc := newGeminiFixture(geminiDependencies{
			httpUpstream:  stub,
			cfg:           &googleforward.Options{},
			tokenProvider: newGeminiTokenSourceForTest(),
		})
		provider := &gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				ID:           602,
				Platform:     capability.PlatformGemini,
				Type:         capability.ProviderTypeOAuth,
				Credentials:  map[string]any{"access_token": "ya29.codeassist", "project_id": "proj-ca-1"},
				LoadLocation: time.LoadLocation,
			},
		}

		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(reqBody))

		res, err := svc.ForwardConverted(t.Context(), httpapi.NewGoogleBoundary(c, svc.Options, false), provider, protocol.ProtocolOpenAIResponses, []byte(reqBody))
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, capturedURL, "/v1internal:streamGenerateContent?alt=sse")

		wire := stub.requestBodies[0]
		require.Equal(t, "proj-ca-1", gjson.GetBytes(wire, "project").String())
		require.True(t, gjson.GetBytes(wire, "request.tools.0.googleSearch").Exists())
		require.False(t, gjson.GetBytes(wire, "request.tools.0.google_search").Exists())
	})
}

// TestGeminiForwardConverted_InvalidRequestsReturn400ModelCapabilityUnsupported
// submits malformed JSON, missing models, and unsupported "ultra" reasoning
// effort across Messages, Responses, and Chat Completions.
// Every case must return MODEL_CAPABILITY_UNSUPPORTED without upstream calls or
// health writes, rendering HTTP 400 JSON or untimed terminal SSE when pre-committed.
func TestGeminiForwardConverted_InvalidRequestsReturn400ModelCapabilityUnsupported(t *testing.T) {
	for _, proto := range []protocol.ProtocolID{
		protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
	} {
		for _, tc := range []struct {
			name         string // invalid case label
			body         string // client request JSON bytes
			preCommitSSE bool   // whether to pre-commit HTTP 200 text/event-stream headers
		}{
			{name: "malformed_json", body: `{"model":`},
			{name: "empty_model", body: `{"model":"   ","messages":[{"role":"user","content":"hi"}],"input":"hi"}`},
			{name: "ultra_effort_json", body: invalidUltraBodyForProto(proto)},
			{name: "ultra_effort_precommitted_sse", body: invalidUltraBodyForProto(proto), preCommitSSE: true},
		} {
			t.Run(string(proto)+"/"+tc.name, func(t *testing.T) {
				trace := &geminiFailureTrace{}
				store := &geminiFailureStore{trace: trace}
				stub := &queuedHTTPUpstreamStub{}
				cfg := &googleforward.Options{Configured: true}
				svc := newGeminiFixture(geminiDependencies{
					httpUpstream:   stub,
					providerRepo:   store,
					cfg:            cfg,
					healthObserver: newUpstreamHealthForTest(store, cfg, nil, providercore.HealthOptions{}, nil),
				})
				provider := &gatewayprovider.ExecutionProvider{
					Record: providercore.Record{
						ID:           401,
						Platform:     capability.PlatformGemini,
						Type:         capability.ProviderTypeAPIKey,
						Credentials:  map[string]any{"api_key": "k"},
						LoadLocation: time.LoadLocation,
					},
				}

				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				path := "/v1/chat/completions"
				switch proto {
				case protocol.ProtocolAnthropicMessages:
					path = "/v1/messages"
				case protocol.ProtocolOpenAIResponses:
					path = "/v1/responses"
				}
				c.Request = httptest.NewRequest(http.MethodPost, path, strings.NewReader(tc.body))
				if tc.preCommitSSE {
					c.Header("Content-Type", "text/event-stream")
					c.Writer.WriteHeader(http.StatusOK)
					c.Writer.Flush()
				}

				res, err := svc.ForwardConverted(t.Context(), httpapi.NewGoogleBoundary(c, svc.Options, false), provider, proto, []byte(tc.body))
				require.Nil(t, res)
				require.Error(t, err)
				require.Equal(t, "MODEL_CAPABILITY_UNSUPPORTED", apperror.Reason(err))
				require.Zero(t, stub.callCount)
				require.Empty(t, trace.steps)

				if tc.preCommitSSE {
					require.Equal(t, http.StatusOK, rec.Code)
					if proto == protocol.ProtocolOpenAIResponses {
						require.Contains(t, rec.Body.String(), "event: response.failed")
					} else {
						require.Contains(t, rec.Body.String(), "event: error")
					}
				} else {
					require.Equal(t, http.StatusBadRequest, rec.Code)
				}
			})
		}
	}
}

// TestGeminiForwardConverted_SinglePassHealthEvaluationOnHTTPAndStreamFailures
// exercises both HTTP >= 400 responses and HTTP 200 in-stream error frames
// through ForwardConverted. Provider health policy
// must persist at most once per attempt and never duplicate writes across stages.
func TestGeminiForwardConverted_SinglePassHealthEvaluationOnHTTPAndStreamFailures(t *testing.T) {
	t.Run("http_401_and_503_temp_rule_write_health_once", func(t *testing.T) {
		for _, proto := range []protocol.ProtocolID{
			protocol.ProtocolAnthropicMessages,
			protocol.ProtocolOpenAIResponses,
			protocol.ProtocolOpenAIChatCompletions,
		} {
			trace := &geminiFailureTrace{}
			store := &geminiFailureStore{trace: trace}
			cfg := &googleforward.Options{Configured: true, LogErrorBody: true}
			transport := &geminiCompatHTTPUpstreamStub{
				response: &http.Response{
					StatusCode: http.StatusUnauthorized,
					Header:     http.Header{"X-Goog-Request-Id": {"auth-fail-ir"}},
					Body:       io.NopCloser(strings.NewReader(`{"error":{"code":401,"message":"bad token"}}`)),
				},
			}
			svc := newGeminiFixture(geminiDependencies{
				httpUpstream:   transport,
				tokenProvider:  newGeminiTokenSourceForTest(),
				providerRepo:   store,
				cfg:            cfg,
				healthObserver: newUpstreamHealthForTest(store, cfg, nil, providercore.HealthOptions{}, nil),
			})
			provider := &gatewayprovider.ExecutionProvider{
				Record: providercore.Record{
					ID:           501,
					Name:         "ir-health-fixture",
					Platform:     capability.PlatformGemini,
					Type:         capability.ProviderTypeOAuth,
					Credentials:  map[string]any{"access_token": "tok", "oauth_type": "ai_studio"},
					LoadLocation: time.LoadLocation,
				},
			}
			c, _ := newGeminiNativeTestContext(t)
			output := &geminiFailureOutput{Output: httpapi.NewGoogleBoundary(c, svc.Options, false), trace: trace}
			res, err := svc.ForwardConverted(t.Context(), output, provider, proto, validBodyForProto(proto, true))
			require.Nil(t, res)
			var fo *forward.UpstreamFailoverError
			require.ErrorAs(t, err, &fo)
			require.Equal(t, []string{"health:error", "observe:failover"}, trace.steps)
		}
	})

	t.Run("http_200_stream_error_pre_and_post_semantic_write_health_once", func(t *testing.T) {
		for _, started := range []bool{false, true} {
			t.Run(fmt.Sprintf("started=%t", started), func(t *testing.T) {
				trace := &geminiFailureTrace{}
				store := &geminiFailureStore{trace: trace}
				cfg := &googleforward.Options{Configured: true}
				ssePayload := "data: {\"error\":{\"code\":401,\"message\":\"revoked key\"}}\n\n"
				if started {
					ssePayload = "data: {\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"hello\"}]}}],\"usageMetadata\":{\"promptTokenCount\":5,\"candidatesTokenCount\":1}}\n\n" + ssePayload
				}
				transport := &geminiCompatHTTPUpstreamStub{
					response: &http.Response{
						StatusCode: http.StatusOK,
						Header:     http.Header{"Content-Type": {"text/event-stream"}, "X-Goog-Request-Id": {"stream-err-ir"}},
						Body:       io.NopCloser(strings.NewReader(ssePayload)),
					},
				}
				svc := newGeminiFixture(geminiDependencies{
					httpUpstream:   transport,
					providerRepo:   store,
					cfg:            cfg,
					healthObserver: newUpstreamHealthForTest(store, cfg, nil, providercore.HealthOptions{}, nil),
				})
				provider := &gatewayprovider.ExecutionProvider{
					Record: providercore.Record{
						ID:           502,
						Name:         "stream-health-fixture",
						Platform:     capability.PlatformGemini,
						Type:         capability.ProviderTypeAPIKey,
						Credentials:  map[string]any{"api_key": "k"},
						LoadLocation: time.LoadLocation,
					},
				}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
				output := &geminiFailureOutput{Output: httpapi.NewGoogleBoundary(c, svc.Options, false), trace: trace}

				res, err := svc.ForwardConverted(t.Context(), output, provider, protocol.ProtocolOpenAIResponses, validBodyForProto(protocol.ProtocolOpenAIResponses, true))
				require.Error(t, err)
				healthWrites := 0
				for _, step := range trace.steps {
					if strings.HasPrefix(step, "health:") {
						healthWrites++
					}
				}
				require.Equal(t, 1, healthWrites, "stream error must evaluate provider health exactly once")
				if !started {
					require.Nil(t, res)
					var fo *forward.UpstreamFailoverError
					require.ErrorAs(t, err, &fo)
					require.Zero(t, rec.Body.Len())
				} else {
					require.NotNil(t, res)
					require.Equal(t, 5, res.Usage.InputTokens)
					require.Contains(t, rec.Body.String(), "event: response.failed")
				}
			})
		}
	})

	t.Run("committed_sse_upstream_http_400_emits_typed_terminal_error_once", func(t *testing.T) {
		for _, proto := range []protocol.ProtocolID{
			protocol.ProtocolAnthropicMessages,
			protocol.ProtocolOpenAIResponses,
			protocol.ProtocolOpenAIChatCompletions,
		} {
			t.Run(string(proto), func(t *testing.T) {
				stub := &queuedHTTPUpstreamStub{
					responses: []*http.Response{{
						StatusCode: http.StatusBadRequest,
						Header:     http.Header{"X-Goog-Request-Id": {"req-400-sse"}},
						Body:       io.NopCloser(strings.NewReader(`{"error":{"code":400,"status":"INVALID_ARGUMENT","message":"unsupported parameter value"}}`)),
					}},
				}
				svc := newGeminiFixture(geminiDependencies{
					httpUpstream: stub,
					cfg:          &googleforward.Options{Configured: true},
				})
				provider := &gatewayprovider.ExecutionProvider{
					Record: providercore.Record{
						ID:           503,
						Name:         "committed-sse-400",
						Platform:     capability.PlatformGemini,
						Type:         capability.ProviderTypeAPIKey,
						Credentials:  map[string]any{"api_key": "k"},
						LoadLocation: time.LoadLocation,
					},
				}

				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				path := "/v1/chat/completions"
				switch proto {
				case protocol.ProtocolAnthropicMessages:
					path = "/v1/messages"
				case protocol.ProtocolOpenAIResponses:
					path = "/v1/responses"
				}
				c.Request = httptest.NewRequest(http.MethodPost, path, nil)
				c.Header("Content-Type", "text/event-stream")
				c.Writer.WriteHeader(http.StatusOK)
				c.Writer.Flush()

				boundary := httpapi.NewGoogleBoundary(c, svc.Options, false)
				res, err := svc.ForwardConverted(t.Context(), boundary, provider, proto, validBodyForProto(proto, true))
				require.Nil(t, res)
				require.Error(t, err)
				require.Equal(t, http.StatusOK, rec.Code)

				bodyBeforeDup := rec.Body.String()
				// Second error attempt must be suppressed by beginGoogleError deduplication.
				_ = boundary.ClaudeError(http.StatusBadGateway, "api_error", "duplicate")
				require.Equal(t, bodyBeforeDup, rec.Body.String())

				switch proto {
				case protocol.ProtocolAnthropicMessages:
					require.Contains(t, bodyBeforeDup, "event: error\n")
					require.Contains(t, bodyBeforeDup, `"type":"invalid_request_error"`)
					require.Contains(t, bodyBeforeDup, "unsupported parameter value")
				case protocol.ProtocolOpenAIResponses:
					require.Contains(t, bodyBeforeDup, "event: response.failed\n")
					require.Contains(t, bodyBeforeDup, `"code":"invalid_request_error"`)
					require.Contains(t, bodyBeforeDup, "Invalid request")
				case protocol.ProtocolOpenAIChatCompletions:
					require.Contains(t, bodyBeforeDup, "event: error\n")
					require.Contains(t, bodyBeforeDup, `"type":"invalid_request_error"`)
					require.Contains(t, bodyBeforeDup, "Invalid request")
				}
			})
		}
	})

	t.Run("legacy_responses_messages_ingress_uses_lossless_ir_normalizer", func(t *testing.T) {
		stub := &queuedHTTPUpstreamStub{
			responses: []*http.Response{{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": {"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"candidates":[{"content":{"parts":[{"text":"ok"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":8,"candidatesTokenCount":2}}`)),
			}},
		}
		svc := newGeminiFixture(geminiDependencies{
			httpUpstream: stub,
			cfg:          &googleforward.Options{Configured: true},
		})
		provider := &gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				ID:           504,
				Platform:     capability.PlatformGemini,
				Type:         capability.ProviderTypeAPIKey,
				Credentials:  map[string]any{"api_key": "k"},
				LoadLocation: time.LoadLocation,
			},
		}
		legacyResponsesBody := []byte(`{
			"model":"gemini-2.5-flash",
			"tools":[{"type":"function","name":"lookup","parameters":{"type":"object","properties":{"seq":{"type":"integer","minimum":9007199254740993}}}}],
			"messages":[{"role":"user","content":"check seq 9007199254740993"}]
		}`)
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)

		res, err := svc.ForwardConverted(t.Context(), httpapi.NewGoogleBoundary(c, svc.Options, false), provider, protocol.ProtocolOpenAIResponses, legacyResponsesBody)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Contains(t, string(stub.requestBodies[0]), "9007199254740993")
	})
}

// extractClientToolIDsForTest extracts non-empty emitted tool call IDs from
// buffered JSON or streaming SSE Messages, Responses, or Chat client responses.
// Streaming inputs scan data lines in emission order and ignore non-tool events.
func extractClientToolIDsForTest(rawOut, proto string, stream bool) []string {
	var payloads []string
	if stream {
		for line := range strings.SplitSeq(rawOut, "\n") {
			if payload, ok := strings.CutPrefix(line, "data: "); ok {
				payloads = append(payloads, payload)
			}
		}
	} else {
		payloads = []string{rawOut}
	}
	var ids []string
	for _, payload := range payloads {
		var found []gjson.Result
		switch {
		case proto == "messages" && stream:
			if gjson.Get(payload, "type").String() == "content_block_start" &&
				gjson.Get(payload, "content_block.type").String() == "tool_use" {
				found = []gjson.Result{gjson.Get(payload, "content_block.id")}
			}
		case proto == "messages":
			found = gjson.Get(payload, "content.#.id").Array()
		case proto == "responses" && stream:
			if gjson.Get(payload, "type").String() == "response.output_item.added" &&
				gjson.Get(payload, "item.type").String() == "function_call" {
				found = []gjson.Result{gjson.Get(payload, "item.call_id")}
			}
		case proto == "responses":
			found = gjson.Get(payload, "output.#.call_id").Array()
		case stream:
			found = gjson.Get(payload, "choices.0.delta.tool_calls.#.id").Array()
		default:
			found = gjson.Get(payload, "choices.0.message.tool_calls.#.id").Array()
		}
		for _, item := range found {
			if item.String() != "" {
				ids = append(ids, item.String())
			}
		}
	}
	return ids
}

// buildReplayTurnBodyForTest constructs a second-turn request body carrying
// replayed tool calls and reversed tool results for proto. Messages and
// Responses payloads include the supplied trc1:gemini carrier, and
// withHighEffort appends protocol-specific high effort controls.
func buildReplayTurnBodyForTest(proto, model string, stream bool, carrier, id0, id1 string, withHighEffort bool) string {
	switch proto {
	case "messages":
		effortField := ""
		if withHighEffort {
			effortField = `,"output_config":{"effort":"high"}`
		}
		return fmt.Sprintf(
			`{"model":%q,"max_tokens":128,"stream":%t%s,"messages":[{"role":"user","content":"weather"},{"role":"assistant","content":[{"type":"thinking","thinking":"old thought","signature":%q},{"type":"tool_use","id":%q,"name":"lookup","input":{"city":"Paris"}},{"type":"tool_use","id":%q,"name":"lookup","input":{"city":"London"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":%q,"content":"London result"},{"type":"tool_result","tool_use_id":%q,"content":"Paris result"}]}]}`,
			model, stream, effortField, carrier, id0, id1, id1, id0,
		)
	case "responses":
		effortField := ""
		if withHighEffort {
			effortField = `,"reasoning":{"effort":"high"}`
		}
		return fmt.Sprintf(
			`{"model":%q,"stream":%t%s,"input":[{"role":"user","content":"weather"},{"type":"reasoning","id":"rs_prev","summary":[{"type":"summary_text","text":"old thought"}],"encrypted_content":%q},{"type":"function_call","call_id":%q,"name":"lookup","arguments":"{\"city\":\"Paris\"}"},{"type":"function_call","call_id":%q,"name":"lookup","arguments":"{\"city\":\"London\"}"},{"type":"function_call_output","call_id":%q,"output":"London result"},{"type":"function_call_output","call_id":%q,"output":"Paris result"}]}`,
			model, stream, effortField, carrier, id0, id1, id1, id0,
		)
	default:
		effortField := ""
		if withHighEffort {
			effortField = `,"reasoning_effort":"high"`
		}
		return fmt.Sprintf(
			`{"model":%q,"stream":%t%s,"messages":[{"role":"user","content":"weather"},{"role":"assistant","tool_calls":[{"type":"function","id":%q,"function":{"name":"lookup","arguments":"{\"city\":\"Paris\"}"}},{"type":"function","id":%q,"function":{"name":"lookup","arguments":"{\"city\":\"London\"}"}}]},{"role":"tool","tool_call_id":%q,"content":"London result"},{"role":"tool","tool_call_id":%q,"content":"Paris result"}]}`,
			model, stream, effortField, id0, id1, id1, id0,
		)
	}
}

// clientProtoFromSlug maps "messages", "responses", and "chat" slugs to ProtocolIDs.
func clientProtoFromSlug(slug string) protocol.ProtocolID {
	switch slug {
	case "messages":
		return protocol.ProtocolAnthropicMessages
	case "responses":
		return protocol.ProtocolOpenAIResponses
	default:
		return protocol.ProtocolOpenAIChatCompletions
	}
}

// invalidUltraBodyForProto returns a minimal request carrying unsupported
// "ultra" reasoning effort in proto's wire format.
func invalidUltraBodyForProto(proto protocol.ProtocolID) string {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return `{"model":"gemini-2.5-pro","max_tokens":64,"output_config":{"effort":"ultra"},"messages":[{"role":"user","content":"hi"}]}`
	case protocol.ProtocolOpenAIResponses:
		return `{"model":"gemini-2.5-pro","reasoning":{"effort":"ultra"},"input":"hi"}`
	default:
		return `{"model":"gemini-2.5-pro","reasoning_effort":"ultra","messages":[{"role":"user","content":"hi"}]}`
	}
}

// validBodyForProto returns a minimal valid request body for proto.
func validBodyForProto(proto protocol.ProtocolID, stream bool) []byte {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return []byte(fmt.Sprintf(`{"model":"gemini-2.5-flash","max_tokens":64,"stream":%t,"messages":[{"role":"user","content":"hi"}]}`, stream))
	case protocol.ProtocolOpenAIResponses:
		return []byte(fmt.Sprintf(`{"model":"gemini-2.5-flash","stream":%t,"input":"hi"}`, stream))
	default:
		return []byte(fmt.Sprintf(`{"model":"gemini-2.5-flash","stream":%t,"messages":[{"role":"user","content":"hi"}]}`, stream))
	}
}
