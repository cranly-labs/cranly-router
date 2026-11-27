package messageforward

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient/tlsfingerprint"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/upstream/bedrock"
)

// conversion_ir_test.go exercises IR-based Chat and Responses conversion across
// AWS Bedrock (SigV4 and API-key modes over binary AWS EventStream frames),
// Google Vertex service accounts, and Anthropic OAuth. Assertions cover
// regional routing validation, 1h cache TTL capability rules, cross-model
// signature stripping with SigV4 re-signing, pre-semantic EventStream exception
// failover, and OAuth tool-name restoration composed with IR tool raising.

// convertedBoundaryFixture extends privateHTTPBoundary with ConvertedHTTPBoundary
// error and notice capture for unit assertions on Runtime.Converted.
type convertedBoundaryFixture struct {
	*privateHTTPBoundary                      // embedded HTTP recording fixture
	committedSSE         bool                 // simulated downstream HTTP 200 SSE commitment state
	notices              []forwardcore.Notice // recorded diagnostic and failover notices
	convertedErrStatus   int                  // last status passed to WriteConvertedError
	convertedErrType     string               // last error type passed to WriteConvertedError
	convertedErrMsg      string               // last message passed to WriteConvertedError
	terminalErrStatus    int                  // last status passed to WriteLocalTerminalError
	terminalErrType      string               // last error type passed to WriteLocalTerminalError
	terminalErrMsg       string               // last message passed to WriteLocalTerminalError
	rawTerminalFrames    [][]byte             // pre-encoded terminal SSE frames written via WriteRawTerminalSSE
}

// convertedRoundTripCapture records each outbound HTTP request and dispatches
// queued in-memory HTTP responses during messageforward unit tests.
type convertedRoundTripCapture struct {
	requests []*http.Request                                                       // cloned outbound HTTP requests in call order
	bodies   [][]byte                                                              // captured outbound HTTP request bodies in call order
	respond  func(idx int, req *http.Request, body []byte) (*http.Response, error) // per-call response factory
}

// newConvertedBoundaryFixture constructs a convertedBoundaryFixture backed by
// rec and a POST inbound request carrying optional headers.
func newConvertedBoundaryFixture(t *testing.T, rec *httptest.ResponseRecorder, path string) *convertedBoundaryFixture {
	t.Helper()
	base, err := newPrivateHTTPFixture(rec)
	require.NoError(t, err)
	base.Request = httptest.NewRequest(http.MethodPost, path, nil)
	return &convertedBoundaryFixture{privateHTTPBoundary: base}
}

// Observe appends n to f.notices for test inspection.
func (f *convertedBoundaryFixture) Observe(n forwardcore.Notice) {
	f.notices = append(f.notices, n)
}

// CommittedSSE reports whether simulated HTTP 200 SSE headers were committed.
func (f *convertedBoundaryFixture) CommittedSSE() bool {
	return f.committedSSE || f.Written()
}

// WriteConvertedError records uncommitted or SSE-promoted converted error fields
// and writes JSON or SSE error output onto the underlying recorder.
func (f *convertedBoundaryFixture) WriteConvertedError(_ protocol.ProtocolID, _ string, status int, errType, _, message string) {
	f.convertedErrStatus = status
	f.convertedErrType = errType
	f.convertedErrMsg = message
	if f.CommittedSSE() {
		f.WriteLocalTerminalError(protocol.ProtocolOpenAIResponses, "", status, errType, "", message)
		return
	}
	f.Writer.recorder.WriteHeader(status)
	_, _ = fmt.Fprintf(f.Writer.recorder, `{"error":{"type":%q,"message":%q}}`, errType, message)
	f.Writer.written = true
}

// WriteLocalTerminalError records terminal SSE error metadata and writes a
// synthetic error frame onto the underlying recorder.
func (f *convertedBoundaryFixture) WriteLocalTerminalError(_ protocol.ProtocolID, _ string, status int, errType, _, message string) {
	f.terminalErrStatus = status
	f.terminalErrType = errType
	f.terminalErrMsg = message
	_, _ = fmt.Fprintf(f.Writer.recorder, "event: error\ndata: {\"type\":%q,\"message\":%q}\n\n", errType, message)
	f.Writer.written = true
}

// WriteRawTerminalSSE records pre-encoded terminal SSE frame bytes and writes
// them onto the underlying recorder.
func (f *convertedBoundaryFixture) WriteRawTerminalSSE(data []byte) bool {
	f.rawTerminalFrames = append(f.rawTerminalFrames, bytes.Clone(data))
	_, _ = f.Writer.recorder.Write(data)
	f.Writer.written = true
	return true
}

// Do delegates to DoWithTLS with a nil TLS profile.
func (c *convertedRoundTripCapture) Do(req *http.Request, proxyURL string, providerID int64, concurrency int) (*http.Response, error) {
	return c.DoWithTLS(req, proxyURL, providerID, concurrency, nil)
}

// DoWithTLS clones req headers and body bytes, appends them to c, and invokes
// c.respond for the current zero-based call index.
func (c *convertedRoundTripCapture) DoWithTLS(req *http.Request, _ string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		raw, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		body = raw
	}
	cloned := req.Clone(req.Context())
	cloned.Header = req.Header.Clone()
	idx := len(c.requests)
	c.requests = append(c.requests, cloned)
	c.bodies = append(c.bodies, body)
	return c.respond(idx, cloned, body)
}

// buildTestEventStreamFrame encodes one binary AWS EventStream frame with typed
// string headers and IEEE CRC32 prelude and message checksums.
func buildTestEventStreamFrame(headers map[string]string, payload []byte) []byte {
	var hdrBuf bytes.Buffer
	for k, v := range headers {
		_ = hdrBuf.WriteByte(byte(len(k)))
		_, _ = hdrBuf.WriteString(k)
		_ = hdrBuf.WriteByte(7)
		_ = binary.Write(&hdrBuf, binary.BigEndian, uint16(len(v)))
		_, _ = hdrBuf.WriteString(v)
	}
	hdrBytes := hdrBuf.Bytes()
	totalLen := uint32(12 + len(hdrBytes) + len(payload) + 4)

	var prelude bytes.Buffer
	_ = binary.Write(&prelude, binary.BigEndian, totalLen)
	_ = binary.Write(&prelude, binary.BigEndian, uint32(len(hdrBytes)))
	preludeCRC := crc32.Checksum(prelude.Bytes(), bedrock.Crc32IEEETable)

	var frame bytes.Buffer
	_, _ = frame.Write(prelude.Bytes())
	_ = binary.Write(&frame, binary.BigEndian, preludeCRC)
	_, _ = frame.Write(hdrBytes)
	_, _ = frame.Write(payload)
	msgCRC := crc32.Checksum(frame.Bytes(), bedrock.Crc32IEEETable)
	_ = binary.Write(&frame, binary.BigEndian, msgCRC)
	return frame.Bytes()
}

// buildTestBedrockChunkStream wraps Claude JSON event strings in base64 chunk
// EventStream frames preceded by an initial-response metadata frame.
func buildTestBedrockChunkStream(events ...string) []byte {
	var out bytes.Buffer
	_, _ = out.Write(buildTestEventStreamFrame(map[string]string{
		":message-type": "event",
		":event-type":   "initial-response",
	}, []byte(`{}`)))
	for _, ev := range events {
		b64 := base64.StdEncoding.EncodeToString([]byte(ev))
		payload := []byte(`{"bytes":"` + b64 + `"}`)
		_, _ = out.Write(buildTestEventStreamFrame(map[string]string{
			":message-type": "event",
			":event-type":   "chunk",
			":content-type": "application/json",
		}, payload))
	}
	return out.Bytes()
}

// buildTestSigV4BedrockProvider constructs an IAM SigV4 Bedrock ExecutionProvider
// fixture configured for region.
func buildTestSigV4BedrockProvider(region string) *gatewayprovider.ExecutionProvider {
	return &gatewayprovider.ExecutionProvider{
		Record: providercore.Record{
			LoadLocation: time.LoadLocation,
			ID:           811,
			Name:         "bedrock-sigv4",
			Platform:     capability.PlatformAnthropic,
			Type:         capability.ProviderTypeBedrock,
			Credentials: map[string]any{
				"aws_access_key_id":     "AKIAIOSFODNN7EXAMPLE",
				"aws_secret_access_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				"aws_region":            region,
			},
		},
	}
}

// TestRuntimeConverted_BedrockSigV4AndAPIKeyEventStream verifies that Runtime
// Chat and Responses delegate to Bedrock regional routing, SigV4 or bearer
// signing, and AWS EventStream decoding. It also confirms
// amazon-bedrock-invocationMetrics populate normalized token usage.
func TestRuntimeConverted_BedrockSigV4AndAPIKeyEventStream(t *testing.T) {
	streamBytes := buildTestBedrockChunkStream(
		`{"type":"message_start","message":{"id":"msg_bdrk_1","type":"message","role":"assistant","model":"us.anthropic.claude-sonnet-4-6","content":[],"usage":{"input_tokens":0,"output_tokens":0}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello from Bedrock"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"amazon-bedrock-invocationMetrics":{"inputTokenCount":44,"outputTokenCount":9}}`,
		`{"type":"message_stop"}`,
	)

	t.Run("SigV4 streaming Responses", func(t *testing.T) {
		capture := &convertedRoundTripCapture{
			respond: func(_ int, _ *http.Request, _ []byte) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header: http.Header{
						"Content-Type":     []string{"application/vnd.amazon.eventstream"},
						"X-Amzn-Requestid": []string{"amzn-req-sigv4"},
					},
					Body: io.NopCloser(bytes.NewReader(streamBytes)),
				}, nil
			},
		}
		svc := NewRuntime(Dependencies{Transport: capture}, Options{})
		rec := httptest.NewRecorder()
		boundary := newConvertedBoundaryFixture(t, rec, "/v1/responses")
		provider := buildTestSigV4BedrockProvider("us-east-1")

		reqBody := []byte(`{"model":"claude-sonnet-4-6","stream":true,"service_tier":"priority","input":"greet me"}`)
		res, err := svc.Converted(context.Background(), boundary, provider, protocol.ProtocolOpenAIResponses, reqBody)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, capture.requests, 1)

		sentReq := capture.requests[0]
		assert.Contains(t, sentReq.URL.Host, "bedrock-runtime.us-east-1.amazonaws.com")
		assert.Contains(t, sentReq.URL.Path, "/invoke-with-response-stream")
		assert.True(t, strings.HasPrefix(sentReq.Header.Get("Authorization"), "AWS4-HMAC-SHA256 "))

		var sentMap map[string]any
		require.NoError(t, json.Unmarshal(capture.bodies[0], &sentMap))
		assert.Equal(t, "bedrock-2023-05-31", sentMap["anthropic_version"])
		assert.Nil(t, sentMap["model"])
		assert.Nil(t, sentMap["stream"])
		assert.Nil(t, sentMap["speed"], "Bedrock Invoke payload must strip unsupported top-level speed")

		assert.Equal(t, "amzn-req-sigv4", res.RequestID)
		assert.Equal(t, "claude-sonnet-4-6", res.Model)
		assert.Equal(t, "us.anthropic.claude-sonnet-4-6", res.UpstreamModel)
		assert.Equal(t, "us.anthropic.claude-sonnet-4-6", res.UpstreamResponseModel)
		assert.Empty(t, res.Usage.Speed, "Bedrock conversion must not record fast-mode speed billing")
		assert.Equal(t, 44, res.Usage.InputTokens)
		assert.Equal(t, 9, res.Usage.OutputTokens)
		assert.Contains(t, rec.Body.String(), "Hello from Bedrock")
		assert.Contains(t, rec.Body.String(), "event: response.completed")
	})

	t.Run("APIKey buffered Chat Completions", func(t *testing.T) {
		capture := &convertedRoundTripCapture{
			respond: func(_ int, _ *http.Request, _ []byte) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header: http.Header{
						"Content-Type":     []string{"application/vnd.amazon.eventstream"},
						"X-Amzn-Requestid": []string{"amzn-req-apikey"},
					},
					Body: io.NopCloser(bytes.NewReader(streamBytes)),
				}, nil
			},
		}
		svc := NewRuntime(Dependencies{Transport: capture}, Options{})
		rec := httptest.NewRecorder()
		boundary := newConvertedBoundaryFixture(t, rec, "/v1/chat/completions")
		provider := &gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				LoadLocation: time.LoadLocation,
				ID:           812,
				Name:         "bedrock-bearer",
				Platform:     capability.PlatformAnthropic,
				Type:         capability.ProviderTypeBedrock,
				Credentials: map[string]any{
					"auth_mode":  "apikey",
					"api_key":    "ABSK-bedrock-bearer-token",
					"aws_region": "us-west-2",
				},
			},
		}

		reqBody := []byte(`{"model":"claude-sonnet-4-6","stream":false,"messages":[{"role":"user","content":"greet me"}]}`)
		res, err := svc.Converted(context.Background(), boundary, provider, protocol.ProtocolOpenAIChatCompletions, reqBody)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, capture.requests, 1)

		sentReq := capture.requests[0]
		assert.Contains(t, sentReq.URL.Host, "bedrock-runtime.us-west-2.amazonaws.com")
		assert.Equal(t, "Bearer ABSK-bedrock-bearer-token", sentReq.Header.Get("Authorization"))
		assert.Equal(t, 44, res.Usage.InputTokens)
		assert.Equal(t, 9, res.Usage.OutputTokens)

		var chatResp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &chatResp))
		assert.Equal(t, "chat.completion", chatResp["object"])
		assert.Contains(t, rec.Body.String(), "Hello from Bedrock")
	})
}

// TestRuntimeConverted_BedrockRegionalRoutingError supplies an unsupported
// Bedrock source-region and model combination to Runtime.Responses. ResolveModel
// must reject the route immediately without invoking credential signers or transport.
func TestRuntimeConverted_BedrockRegionalRoutingError(t *testing.T) {
	capture := &convertedRoundTripCapture{
		respond: func(_ int, _ *http.Request, _ []byte) (*http.Response, error) {
			t.Fatal("transport should not be invoked when BedrockRoute rejects unsupported region")
			return nil, nil
		},
	}
	svc := NewRuntime(Dependencies{Transport: capture}, Options{})
	rec := httptest.NewRecorder()
	boundary := newConvertedBoundaryFixture(t, rec, "/v1/responses")
	// claude-sonnet-4-6 is restricted to BedrockCommercialSources, so us-gov-west-1 is rejected.
	provider := buildTestSigV4BedrockProvider("us-gov-west-1")

	_, err := svc.Converted(context.Background(), boundary, provider, protocol.ProtocolOpenAIResponses, []byte(`{"model":"claude-sonnet-4-6","input":"hi"}`))
	require.Error(t, err)
	assert.Empty(t, capture.requests)
}

// TestRuntimeConverted_BedrockSignature400StrippingReSignsPayload replays a
// foreign trc1:anthropic signature against a SigV4 Bedrock provider that returns
// HTTP 400 on the first dispatch. The retry must strip the signature losslessly,
// preserve 64-bit integer arguments, and re-compute the SigV4 Authorization hash.
func TestRuntimeConverted_BedrockSignature400StrippingReSignsPayload(t *testing.T) {
	carrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme: ir.SchemeAnthropic,
		Data:   "sig_foreign_model",
		Text:   "prior thought",
	})
	require.NoError(t, err)
	reqBody := fmt.Sprintf(`{
		"model":"claude-sonnet-4-6",
		"stream":false,
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"step 1"}]},
			{"type":"reasoning","id":"rs_1","encrypted_content":%q,"summary":[]},
			{"type":"function_call","call_id":"call_1","name":"lookup","arguments":"{\"id\":9007199254740993}"},
			{"type":"function_call_output","call_id":"call_1","output":"done"}
		]
	}`, carrier)

	okStream := buildTestBedrockChunkStream(
		`{"type":"message_start","message":{"id":"msg_retry_ok","type":"message","role":"assistant","model":"us.anthropic.claude-sonnet-4-6","content":[],"usage":{"input_tokens":15,"output_tokens":0}}}`,
		`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Recovered"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":4}}`,
		`{"type":"message_stop"}`,
	)

	capture := &convertedRoundTripCapture{
		respond: func(idx int, _ *http.Request, _ []byte) (*http.Response, error) {
			if idx == 0 {
				return &http.Response{
					StatusCode: http.StatusBadRequest,
					Header:     http.Header{"Content-Type": []string{"application/json"}},
					Body:       io.NopCloser(strings.NewReader(`{"message":"messages.1.content.0: Invalid ` + "`signature`" + ` in ` + "`thinking`" + ` block"}`)),
				}, nil
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/vnd.amazon.eventstream"}},
				Body:       io.NopCloser(bytes.NewReader(okStream)),
			}, nil
		},
	}

	svc := NewRuntime(Dependencies{Transport: capture}, Options{ResponseReadLimit: 1 << 20})
	rec := httptest.NewRecorder()
	boundary := newConvertedBoundaryFixture(t, rec, "/v1/responses")
	provider := buildTestSigV4BedrockProvider("us-east-1")

	res, err := svc.Converted(context.Background(), boundary, provider, protocol.ProtocolOpenAIResponses, []byte(reqBody))
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, capture.requests, 2)

	assert.Contains(t, string(capture.bodies[0]), `"sig_foreign_model"`)
	assert.NotContains(t, string(capture.bodies[1]), `"sig_foreign_model"`)
	// Precision of the 64-bit integer tool argument must survive lossless stripping.
	assert.Contains(t, string(capture.bodies[1]), `9007199254740993`)
	// SigV4 Authorization signature must change when the payload body changes.
	assert.NotEqual(t, capture.requests[0].Header.Get("Authorization"), capture.requests[1].Header.Get("Authorization"))
	require.Len(t, boundary.notices, 1)
	assert.Equal(t, "signature_error", boundary.notices[0].Kind)
}

// TestRuntimeConverted_BedrockPreSemanticExceptionFailover injects a Bedrock
// EventStream throttlingException immediately after message_start before any
// semantic content block arrives. Runtime.Responses must return an HTTP 429
// UpstreamFailoverError and leave the downstream recorder completely untouched.
func TestRuntimeConverted_BedrockPreSemanticExceptionFailover(t *testing.T) {
	var streamBuf bytes.Buffer
	msgStart := `{"type":"message_start","message":{"id":"msg_fail_1","type":"message","role":"assistant","model":"us.anthropic.claude-sonnet-4-6","content":[],"usage":{"input_tokens":10,"output_tokens":0}}}`
	b64 := base64.StdEncoding.EncodeToString([]byte(msgStart))
	_, _ = streamBuf.Write(buildTestEventStreamFrame(map[string]string{
		":message-type": "event",
		":event-type":   "chunk",
	}, []byte(`{"bytes":"`+b64+`"}`)))
	_, _ = streamBuf.Write(buildTestEventStreamFrame(map[string]string{
		":message-type":   "exception",
		":exception-type": "throttlingException",
	}, []byte(`{"message":"Too many requests"}`)))

	capture := &convertedRoundTripCapture{
		respond: func(_ int, _ *http.Request, _ []byte) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/vnd.amazon.eventstream"}},
				Body:       io.NopCloser(bytes.NewReader(streamBuf.Bytes())),
			}, nil
		},
	}

	svc := NewRuntime(Dependencies{Transport: capture}, Options{})
	rec := httptest.NewRecorder()
	boundary := newConvertedBoundaryFixture(t, rec, "/v1/responses")
	provider := buildTestSigV4BedrockProvider("us-east-1")

	res, err := svc.Converted(context.Background(), boundary, provider, protocol.ProtocolOpenAIResponses, []byte(`{"model":"claude-sonnet-4-6","stream":true,"input":"hi"}`))
	require.Error(t, err)
	assert.Nil(t, res)

	var foErr *forwardcore.UpstreamFailoverError
	require.True(t, errors.As(err, &foErr))
	assert.Equal(t, http.StatusTooManyRequests, foErr.StatusCode)
	assert.False(t, boundary.Written())
	assert.Empty(t, rec.Body.String())
}

// TestRuntimeConverted_BedrockForce1hCacheByCapability runs convertedAttempt.Build
// with forced 1h Claude cache lifetimes enabled across Claude 4.6 and Claude 3.5
// Bedrock models. Claude 4.6 must retain ttl:"1h" on ephemeral markers while
// older Claude 3.5 routes strip unsupported ttl fields during Bedrock sanitization.
func TestRuntimeConverted_BedrockForce1hCacheByCapability(t *testing.T) {
	ctx := gatewayprovider.WithAPIKeyForceClaude1hCache(context.Background(), true)
	svc := NewRuntime(Dependencies{}, Options{})
	provider := buildTestSigV4BedrockProvider("us-east-1")

	makePrepared := func(model string) []byte {
		return []byte(`{
			"model":"` + model + `",
			"max_tokens":64,
			"stream":true,
			"cache_control":{"type":"ephemeral","ttl":"5m"},
			"system":[{"type":"text","text":"sys","cache_control":{"type":"ephemeral","ttl":"5m"}}],
			"tools":[{"name":"bash","cache_control":{"type":"ephemeral","ttl":"5m"},"input_schema":{"type":"object"}}],
			"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral","ttl":"5m"}}]}]
		}`)
	}

	buildAndReadBedrock := func(logicalModel, mappedModel string) string {
		boundary := newConvertedBoundaryFixture(t, httptest.NewRecorder(), "/v1/responses")
		attempt := &convertedAttempt{
			s:             svc,
			c:             boundary,
			provider:      provider,
			source:        protocol.ProtocolOpenAIResponses,
			state:         &AttemptState{},
			originalModel: logicalModel,
			bedrockRegion: "us-east-1",
		}
		require.NoError(t, attempt.Credential(ctx))
		_, err := attempt.Build(ctx, makePrepared(mappedModel), mappedModel, true, false)
		require.NoError(t, err)
		require.NotNil(t, attempt.request)
		raw, err := io.ReadAll(attempt.request.Body)
		require.NoError(t, err)
		return string(raw)
	}

	opusWire := buildAndReadBedrock("claude-opus-4-6", "us.anthropic.claude-opus-4-6-v1")
	oldWire := buildAndReadBedrock("anthropic.claude-3-5-sonnet-20241022-v2:0", "anthropic.claude-3-5-sonnet-20241022-v2:0")

	assert.Contains(t, opusWire, `"ttl":"1h"`)
	assert.NotContains(t, oldWire, `"ttl"`)
}

// TestRuntimeConverted_VertexAndOAuthToolRestoration exercises Google Vertex
// service_account streamRawPredict routing and Anthropic OAuth conversion.
// OAuth attempts must preserve Mimic -> CacheLimit -> 1h TTL ordering and
// restore rewritten session tool names before IR custom_tool_call raising.
func TestRuntimeConverted_VertexAndOAuthToolRestoration(t *testing.T) {
	t.Run("Vertex service_account Responses conversion", func(t *testing.T) {
		sseStream := strings.Join([]string{
			"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_vtx\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"usage\":{\"input_tokens\":12,\"output_tokens\":0}}}\n\n",
			"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
			"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Vertex OK\"}}\n\n",
			"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n",
			"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
		}, "")

		capture := &convertedRoundTripCapture{
			respond: func(_ int, _ *http.Request, _ []byte) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(sseStream)),
				}, nil
			},
		}
		creds := &providercore.MessageCredentialSource{
			Claude: func(_ context.Context, p *providercore.Record) (string, error) {
				return p.GetCredential("access_token"), nil
			},
		}
		svc := NewRuntime(Dependencies{Transport: capture, Credentials: creds}, Options{})
		rec := httptest.NewRecorder()
		boundary := newConvertedBoundaryFixture(t, rec, "/v1/responses")
		provider := &gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				LoadLocation: time.LoadLocation,
				ID:           821,
				Name:         "vertex-sa",
				Platform:     capability.PlatformAnthropic,
				Type:         capability.ProviderTypeServiceAccount,
				Credentials: map[string]any{
					"access_token": "ya29.vertex-token",
					"project_id":   "demo-vertex-proj",
					"location":     "us-east5",
				},
			},
		}

		res, err := svc.Converted(context.Background(), boundary, provider, protocol.ProtocolOpenAIResponses, []byte(`{"model":"claude-sonnet-4-6","stream":true,"service_tier":"priority","input":"hello vertex"}`))
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, capture.requests, 1)
		assert.Contains(t, capture.requests[0].URL.Host, "us-east5-aiplatform.googleapis.com")
		assert.Contains(t, capture.requests[0].URL.Path, "projects/demo-vertex-proj/locations/us-east5/publishers/anthropic/models/")
		assert.Contains(t, capture.requests[0].URL.Path, ":streamRawPredict")
		assert.Equal(t, "Bearer ya29.vertex-token", anthropic.GetHeaderRaw(capture.requests[0].Header, "authorization"))
		assert.NotContains(t, string(capture.bodies[0]), `"speed"`)
		assert.Empty(t, res.Usage.Speed, "Vertex conversion must not record fast-mode speed billing")
		assert.Contains(t, rec.Body.String(), "Vertex OK")
	})

	t.Run("OAuth Mimic 1h TTL order and custom_tool_call name restoration", func(t *testing.T) {
		capture := &convertedRoundTripCapture{
			respond: func(_ int, _ *http.Request, body []byte) (*http.Response, error) {
				var sent struct {
					Tools []struct {
						Name string `json:"name"`
					} `json:"tools"`
				}
				if err := json.Unmarshal(body, &sent); err != nil || len(sent.Tools) == 0 {
					return nil, fmt.Errorf("missing tools in request: %s", string(body))
				}
				wireToolName := sent.Tools[0].Name
				sseStream := strings.Join([]string{
					"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_oauth\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-6\",\"content\":[],\"usage\":{\"input_tokens\":18,\"output_tokens\":0}}}\n\n",
					fmt.Sprintf("event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":%q,\"input\":{}}}\n\n", wireToolName),
					"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"input\\\":\\\"ls -la\\\"}\"}}\n\n",
					"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
					"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":7}}\n\n",
					"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
				}, "")
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
					Body:       io.NopCloser(strings.NewReader(sseStream)),
				}, nil
			},
		}
		creds := &providercore.MessageCredentialSource{
			Claude: func(_ context.Context, p *providercore.Record) (string, error) {
				return p.GetCredential("access_token"), nil
			},
		}
		svc := NewRuntime(Dependencies{Transport: capture, Credentials: creds}, Options{})
		rec := httptest.NewRecorder()
		boundary := newConvertedBoundaryFixture(t, rec, "/v1/responses")
		provider := &gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				LoadLocation: time.LoadLocation,
				ID:           822,
				Name:         "claude-oauth",
				Platform:     capability.PlatformAnthropic,
				Type:         capability.ProviderTypeOAuth,
				Credentials: map[string]any{
					"access_token": "sk-ant-oat01-test-token",
				},
			},
		}

		// Use a Responses custom tool named "session_exec" (which StaticToolNameRewrites
		// aliases to "cc_ses_exec" on the wire) alongside forced 1h Claude caching to
		// verify Mimic -> CacheLimit -> InjectAnthropicCacheControlTTL1h and RestoreToolNames + IR ToolMap.
		ctx := gatewayprovider.WithAPIKeyForceClaude1hCache(context.Background(), true)
		reqBody := []byte(`{
			"model":"claude-sonnet-4-6",
			"stream":false,
			"input":"list files",
			"tools":[{"type":"custom","name":"session_exec","description":"run shell command"}]
		}`)
		res, err := svc.Converted(ctx, boundary, provider, protocol.ProtocolOpenAIResponses, reqBody)
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Len(t, capture.bodies, 1)
		assert.Contains(t, string(capture.bodies[0]), `"name":"cc_ses_exec"`)
		assert.Contains(t, string(capture.bodies[0]), `"ttl":"1h"`)

		var respObj struct {
			Output []struct {
				Type  string `json:"type"`
				Name  string `json:"name"`
				Input string `json:"input"`
			} `json:"output"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &respObj))
		require.Len(t, respObj.Output, 1)
		assert.Equal(t, "custom_tool_call", respObj.Output[0].Type)
		assert.Equal(t, "session_exec", respObj.Output[0].Name)
		assert.Equal(t, "ls -la", respObj.Output[0].Input)
	})
}
