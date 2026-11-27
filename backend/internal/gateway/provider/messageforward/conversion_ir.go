package messageforward

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/modeltrace"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient/tlsfingerprint"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/timing"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/upstream/bedrock"
)

// conversion_ir.go executes IR-based OpenAI Responses and Chat Completions
// conversion across Anthropic (OAuth, Setup Token, API Key), Google Vertex
// service accounts, and AWS Bedrock. All five provider variants share
// forward.AsConverted and the merged openaiforward response readers so request
// preparation, signature recovery, and leading-frame failover staging behave
// identically across transports.
//
// Bedrock conversion resolves regional inference IDs via BedrockRoute, applies
// model attributes and 1h cache TTL rules before PrepareBedrockRequestBodyWithTokens,
// signs each HTTP dispatch with SigV4 or bearer credentials, and bridges binary
// AWS EventStream chunks into normalized Anthropic SSE frames for the shared reader.

// ConvertedHTTPBoundary optionally extends HTTPBoundary with CommittedSSE state
// and untimed terminal SSE error writers for IR-converted responses.
type ConvertedHTTPBoundary interface {
	// CommittedSSE reports whether downstream HTTP 200 text/event-stream headers
	// have already been committed, including by waiting keepalive heartbeats.
	CommittedSSE() bool
	// WriteConvertedError writes an uncommitted client-protocol HTTP JSON error
	// or promotes to an untimed terminal SSE error when CommittedSSE is true.
	// Uncommitted responses mark semantic commitment before writing HTTP JSON.
	WriteConvertedError(source protocol.ProtocolID, originalModel string, status int, errType, code, message string)
	// WriteLocalTerminalError writes an untimed terminal SSE error frame in
	// source's client protocol, emitting response.failed on Responses clients.
	// Callers invoke it only on streams whose HTTP 200 SSE headers are committed.
	WriteLocalTerminalError(source protocol.ProtocolID, originalModel string, status int, errType, code, message string)
	// WriteRawTerminalSSE delivers pre-encoded terminal SSE bytes through the
	// untimed response barrier and reports whether delivery was handled. False
	// returns let callers fall back to WriteLocalTerminalError.
	WriteRawTerminalSSE(data []byte) bool
}

// convertedAttempt implements forward.ConvertedMessagesPorts for one Anthropic,
// Vertex, or Bedrock cross-protocol conversion attempt.
type convertedAttempt struct {
	s              *Runtime                           // shared Messages forwarding runtime
	c              HTTPBoundary                       // active HTTP boundary adapter
	provider       *gatewayprovider.ExecutionProvider // selected provider attempt target
	source         protocol.ProtocolID                // inbound client protocol (Responses or Chat)
	requestBodyLen int                                // raw client request byte length
	state          *AttemptState                      // per-attempt tool and beta preparation state

	// ResolveModel populates originalModel, mappedModel, and bedrockRegion before
	// dialect resolution, request construction, or error callbacks read them.
	originalModel string // logical client model extracted before provider mapping
	mappedModel   string // resolved upstream or regional Bedrock model ID
	bedrockRegion string // resolved AWS source region for Bedrock Invoke URLs

	// Credential populates token, tokenType, bedrockAPIKey, and bedrockSigner,
	// while Build resolves proxyURL so Send dispatches with consistent auth state.
	token         string                 // OAuth, Setup Token, API Key, or Vertex bearer token
	tokenType     string                 // resolved credential category label
	proxyURL      string                 // provider proxy URL when configured
	bedrockAPIKey string                 // bearer API key for Bedrock API-key mode
	bedrockSigner *bedrock.BedrockSigner // SigV4 request signer for Bedrock IAM mode

	request  *http.Request  // latest prepared HTTP request awaiting Send
	response *http.Response // latest HTTP response received by Send
}

// bedrockEventStreamSSEReader adapts a binary AWS Bedrock EventStream response
// body into normalized Anthropic Messages SSE frames on demand without spawning
// an extra background goroutine.
type bedrockEventStreamSSEReader struct {
	raw     io.ReadCloser                      // underlying HTTP response body for Close
	decoder *bedrock.BedrockEventStreamDecoder // binary AWS EventStream frame decoder
	pending bytes.Buffer                       // buffered SSE bytes for the current decoded chunk
	eof     bool                               // set after translating a terminal EventStream exception frame
}

// newBedrockEventStreamSSEReader wraps raw in a synchronous AWS EventStream-to-SSE
// io.ReadCloser adapter. Closing the returned reader closes raw directly so
// idle-timeout body closures unblock in-flight reads immediately.
func newBedrockEventStreamSSEReader(raw io.ReadCloser) io.ReadCloser {
	return &bedrockEventStreamSSEReader{
		raw:     raw,
		decoder: bedrock.NewBedrockEventStreamDecoder(raw),
	}
}

// Read decodes binary EventStream chunks until at least one non-empty Anthropic
// SSE frame is buffered in r.pending, normalizing amazon-bedrock-invocationMetrics
// into standard Messages usage counters. EventStream exception frames become one
// Anthropic error SSE frame followed by io.EOF on the next Read call.
func (r *bedrockEventStreamSSEReader) Read(p []byte) (int, error) {
	for r.pending.Len() == 0 {
		if r.eof {
			return 0, io.EOF
		}
		payload, err := r.decoder.Decode()
		if err != nil {
			if errFrame, ok := translateBedrockStreamExceptionSSE(err); ok {
				_, _ = r.pending.Write(errFrame)
				r.eof = true
				break
			}
			return 0, err
		}
		sseData := bedrock.ExtractBedrockChunkData(payload)
		if len(sseData) == 0 {
			continue
		}
		// Normalize Bedrock invocation metrics before framing so both billing
		// usage collection and IR stream decoding observe standard usage counters.
		sseData = bedrock.TransformBedrockInvocationMetrics(sseData)
		evType := extractBedrockChunkEventTypeBytes(sseData)
		framed := sse.Append(nil, sse.Frame{Event: evType, Data: sseData})
		_, _ = r.pending.Write(framed)
	}
	return r.pending.Read(p)
}

// Close closes the underlying HTTP response body to release network sockets and
// unblock any concurrent reader waiting on upstream EventStream bytes. Nil raw
// readers return nil without error.
func (r *bedrockEventStreamSSEReader) Close() error {
	if r.raw == nil {
		return nil
	}
	return r.raw.Close()
}

// NormalizeResponses delegates legacy Responses ingress normalization to the
// shared provider helper so Chat-shaped messages fields convert into input
// items while preserving unmodeled top-level fields. Valid objects without
// legacy fields return body unchanged with false, while malformed JSON returns
// an error.
func (*convertedAttempt) NormalizeResponses(body []byte) ([]byte, bool, error) {
	return gatewayprovider.NormalizeOpenAIResponsesLegacyIngress(body)
}

// ResolveModel resolves Bedrock regional inference routes or standard Anthropic
// and Vertex wire model IDs for model. Bedrock routing errors return directly
// without contacting upstream or marking credentials invalid.
func (a *convertedAttempt) ResolveModel(ctx context.Context, model string) (string, error) {
	a.originalModel = model
	if a.provider != nil && a.provider.View().IsBedrock() {
		route, err := gatewayprovider.ExecutionModelPolicy(a.provider).WithContext(ctx).BedrockRoute(model)
		if err != nil {
			logging.LegacyPrintf("service.gateway", "[Bedrock] %s", bedrock.BedrockRoutingDiagnostic(err))
			return "", err
		}
		a.bedrockRegion = route.SourceRegion
		a.mappedModel = route.ModelID
		modeltrace.RegisterStage(ctx, route.ModelID)
		if route.ModelID != model {
			logging.LegacyPrintf("service.gateway", "[Bedrock] Model mapping: %s -> %s (provider: %s)", model, route.ModelID, a.provider.Record.Name)
		}
		return route.ModelID, nil
	}

	mapped := gatewayprovider.ExecutionModelPolicy(a.provider).UpstreamModel(ctx, model)
	if mapped == "" {
		a.mappedModel = model
	} else {
		a.mappedModel = mapped
	}
	return mapped, nil
}

// ResolveDialect resolves model attributes and execution rules for mappedModel
// and clientBody, publishing base capabilities onto a.provider. It returns the
// target convert.Dialect and passback mode, or stops conversion on lookup error.
func (a *convertedAttempt) ResolveDialect(ctx context.Context, mappedModel string, clientBody []byte) (convert.Dialect, bool, error) {
	res, err := gatewayprovider.ResolveModelAttributes(ctx, a.provider, mappedModel, a.s.dependencies.Attributes, "anthropic_messages")
	if err != nil {
		return convert.Dialect{}, false, err
	}
	if a.provider != nil {
		base := res
		a.provider.Capabilities = &base
	}
	adjusted := res.ForRequest(gatewayprovider.ModelRequestState(clientBody))
	dialect := gatewayprovider.ConversionDialect(ctx, a.provider, adjusted, protocol.ProtocolAnthropicMessages)
	return dialect, adjusted.Rules.SignatureMode == "passback", nil
}

// ConversionRuntime returns the default wall-clock and crypto/rand ID runtime.
func (*convertedAttempt) ConversionRuntime() convert.Runtime {
	return convert.DefaultRuntime()
}

// Effort extracts normalized OpenAI reasoning effort from client bytes.
func (*convertedAttempt) Effort(body []byte, chat bool, models ...string) *string {
	return forwardcore.ExtractEffort(body, chat, capability.NormalizeRecordedOpenAIEffortForModel, models...)
}

// ThinkingFallback derives display effort when thinking is enabled on model.
func (*convertedAttempt) ThinkingFallback(effort *string, body []byte, model string) *string {
	return gatewayprovider.ApplyThinkingEnabledFallback(effort, body, model)
}

// ModelNotice logs structured provider, logical model, mapped model, and client
// streaming fields at debug level. Nil providers record provider_id zero
// without panicking during synthetic or unit attempts.
func (a *convertedAttempt) ModelNotice(message, original, mapped string, stream bool) {
	var id int64
	if a.provider != nil {
		id = a.provider.Record.ID
	}
	logging.L().Debug(message,
		zap.Int64("provider_id", id),
		zap.String("original_model", original),
		zap.String("mapped_model", mapped),
		zap.Bool("client_stream", stream),
	)
}

// Mimic applies Anthropic OAuth system, metadata, cache, and tool-name
// preparation using a.state so response readers restore rewritten tool names.
func (a *convertedAttempt) Mimic(ctx context.Context, body []byte, system json.RawMessage, model string) []byte {
	return a.s.mimic(ctx, a.c, a.state, a.provider, body, system, model)
}

// CacheLimit enforces Anthropic cache-control validity and breakpoint limits.
func (*convertedAttempt) CacheLimit(body []byte) []byte {
	return anthropic.EnforceCacheControlLimit(body)
}

// Credential resolves Bedrock API-key or SigV4 credentials on Bedrock targets
// and delegates to MessageCredentialSource on Anthropic and Vertex targets.
// Successful lookups populate a.bedrockAPIKey, a.bedrockSigner, or a.token
// before Build constructs the HTTP request.
func (a *convertedAttempt) Credential(ctx context.Context) error {
	if a.provider != nil && a.provider.View().IsBedrock() {
		if a.provider.View().IsBedrockAPIKey() {
			a.bedrockAPIKey = a.provider.View().GetCredential("api_key")
			if a.bedrockAPIKey == "" {
				return errors.New("api_key not found in bedrock credentials")
			}
			return nil
		}
		signer, err := provideradapter.NewBedrockSignerFromProvider(gatewayprovider.ExecutionRecord(a.provider))
		if err != nil {
			return fmt.Errorf("create bedrock signer: %w", err)
		}
		a.bedrockSigner = signer
		return nil
	}

	token, kind, err := a.s.dependencies.Credentials.Resolve(ctx, gatewayprovider.ExecutionRecord(a.provider))
	if err == nil {
		a.token, a.tokenType = token, kind
	}
	return err
}

// Build prepares model attributes, wire effort stripping, and 1h cache TTL
// injection before constructing an Anthropic, Vertex, or signed Bedrock HTTP
// request. Bedrock and Vertex targets strip unsupported top-level speed hints
// from both outbound payloads and returned wire bytes, and Bedrock Invoke
// requests re-sign on every Build call so signature retries carry valid hashes.
func (a *convertedAttempt) Build(ctx context.Context, body []byte, model string, stream, mimic bool) ([]byte, error) {
	if a.provider != nil && a.provider.Record.ProxyID != nil && a.provider.Record.Proxy != nil {
		a.proxyURL = a.provider.Record.Proxy.URL()
	}

	if a.provider != nil && a.provider.View().IsBedrock() {
		attributes, err := gatewayprovider.ResolveModelAttributes(ctx, a.provider, model, a.s.dependencies.Attributes, "anthropic_messages")
		if err != nil {
			return nil, err
		}
		body, err = gatewayprovider.ApplyModelAttributeParameters(body, attributes)
		if err != nil {
			return nil, err
		}
		body = stripAnthropicWireEffortIfMatched(ctx, a.provider, body, a.originalModel)
		// Strip top-level speed hints via jsontext before Bedrock preparation so
		// Invoke payloads avoid ValidationException and billing omits fast-mode rates.
		body = stripTopLevelMessagesMemberBytes(body, "speed")
		// Apply 1h cache lifetimes before Bedrock capability sanitization so Claude
		// 4.5+ routes retain 1h while older Bedrock models strip unsupported TTLs.
		if a.s.injectTTL(ctx, a.provider, model, a.originalModel) {
			body = anthropic.InjectAnthropicCacheControlTTL1h(body)
		}

		betaHeader := ""
		if a.c != nil && a.c.RequestPresent() {
			betaHeader = a.c.RequestHeaders().Get("anthropic-beta")
		}
		betaTokens, err := a.s.bedrockBetaTokens(ctx, a.provider, betaHeader, body, model, attributes.Rules)
		if err != nil {
			return nil, err
		}
		bedrockBody, err := bedrock.PrepareBedrockRequestBodyWithTokens(body, model, betaTokens, false, attributes.Rules)
		if err != nil {
			return nil, fmt.Errorf("prepare bedrock request body: %w", err)
		}

		upstreamCtx, release := detachedStreamContext(ctx, stream)
		var req *http.Request
		if a.provider.View().IsBedrockAPIKey() {
			req, err = bedrock.BuildRequestAPIKey(upstreamCtx, bedrockBody, model, a.bedrockRegion, stream, a.bedrockAPIKey)
		} else {
			req, err = bedrock.BuildRequest(upstreamCtx, bedrockBody, model, a.bedrockRegion, stream, a.bedrockSigner)
		}
		release()
		if err != nil {
			return nil, err
		}
		a.request = req
		return bedrockBody, nil
	}

	isVertex := a.provider != nil &&
		a.provider.Record.Platform == capability.PlatformAnthropic &&
		a.provider.Record.Type == capability.ProviderTypeServiceAccount
	if isVertex {
		// Remove converted fast-tier speed hints on Vertex service accounts so
		// returned wire bytes match Vertex's sanitized non-fast billing behavior.
		body = stripTopLevelMessagesMemberBytes(body, "speed")
	}

	upstreamCtx, release := detachedStreamContext(ctx, stream)
	request, wire, err := a.s.buildRequest(upstreamCtx, a.c, a.state, a.provider, body, a.token, a.tokenType, model, stream, mimic)
	release()
	if isVertex {
		wire = stripTopLevelMessagesMemberBytes(wire, "speed")
	}
	a.request = request
	return wire, err
}

// Send dispatches a.request through the runtime transport, normalizes Bedrock
// request IDs, and wraps HTTP 2xx Bedrock bodies in newBedrockEventStreamSSEReader.
// Transport failures close any returned response body before delegating classification,
// whereas successful callers invoke the returned Close callback when finished.
func (a *convertedAttempt) Send(ctx context.Context) (forwardcore.Response, error) {
	if a.state != nil && !a.state.PrepareRecorded && !a.state.PrepareStartedAt.IsZero() && a.request != nil {
		a.state.PrepareRecorded = true
		timing.ObserveProviderPrepare(a.request.Context(), time.Since(a.state.PrepareStartedAt))
	}

	isBedrock := a.provider != nil && a.provider.View().IsBedrock()
	var tlsProfile *tlsfingerprint.Profile
	if !isBedrock {
		tlsProfile = a.s.requestTLS(a.provider)
	}
	resp, err := a.s.dependencies.Transport.DoWithTLS(
		a.request,
		a.proxyURL,
		a.provider.Record.ID,
		a.provider.Record.Concurrency,
		tlsProfile,
	)
	if err != nil {
		// Close any non-nil response body returned alongside a transport error before
		// delegating failure classification so failed attempts never leak open sockets.
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return forwardcore.Response{}, a.s.transportError(ctx, a.c, a.provider, err, forwardcore.Notice{
			UpstreamURL: logredact.SafeUpstreamURL(a.request.URL.String()),
		})
	}

	if isBedrock {
		if id := resp.Header.Get("x-amzn-requestid"); id != "" && resp.Header.Get("x-request-id") == "" {
			resp.Header.Set("x-request-id", id)
		}
		if resp.StatusCode < http.StatusBadRequest && resp.Body != nil {
			resp.Body = newBedrockEventStreamSSEReader(resp.Body)
		}
	}

	a.response = resp
	return forwardcore.Response{
		StatusCode: resp.StatusCode,
		Close:      func() { _ = resp.Body.Close() },
		RequestID:  resp.Header.Get("x-request-id"),
		Headers:    resp.Header,
	}, nil
}

// ReadErrorBody captures bounded diagnostic bytes from a.response, closes the
// consumed network body, and installs a replayable reader. Downstream health
// checks inspect the same captured slice even when reading reported an error.
func (a *convertedAttempt) ReadErrorBody() ([]byte, error) {
	body, err := a.s.readErrorBody(a.response)
	_ = a.response.Body.Close()
	a.response.Body = io.NopCloser(bytes.NewReader(body))
	return body, err
}

// ErrorMessage extracts and query-sanitizes upstream error text from body.
func (*convertedAttempt) ErrorMessage(body []byte) string {
	return logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(body)))
}

// IsSignatureError reports whether captured HTTP 400 bytes match standard
// thinking-signature errors or configured API-key signature patterns.
func (a *convertedAttempt) IsSignatureError(ctx context.Context, body []byte) bool {
	if thinkingSignatureError(body) {
		return true
	}
	return a.s.signaturePattern(ctx, a.provider, body)
}

// ObserveSignatureRetry records an Ops signature_error notice before AsConverted
// re-dispatches a stripped-signature retry on the same provider. Nil boundaries
// or providers return without recording, and body details obey LogErrorBodyMaxBytes.
func (a *convertedAttempt) ObserveSignatureRetry(status int, body []byte, message string) {
	if a.c == nil || a.provider == nil {
		return
	}
	var reqID, reqURL string
	if a.response != nil {
		reqID = a.response.Header.Get("x-request-id")
	}
	if a.request != nil && a.request.URL != nil {
		reqURL = logredact.SafeUpstreamURL(a.request.URL.String())
	}
	detail := ""
	if a.s.options.LogErrorBody {
		maxBytes := a.s.options.LogErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		detail = logredact.TruncateUTF8(string(body), maxBytes)
	}
	a.c.Observe(forwardcore.Notice{
		Platform:           a.provider.Record.Platform,
		ProviderID:         a.provider.Record.ID,
		ProviderName:       a.provider.Record.Name,
		UpstreamStatusCode: status,
		UpstreamRequestID:  reqID,
		UpstreamURL:        reqURL,
		Kind:               "signature_error",
		Message:            message,
		Detail:             detail,
	})
}

// Health evaluates provider error policy and optional persisted health
// observation for HTTP >= 400 responses. Absent Health dependencies compute
// Generic, Failover, and RetrySameProvider decisions without persistence writes.
func (a *convertedAttempt) Health(ctx context.Context, status int, body []byte, model string) forwardcore.ErrorDecision {
	decision := providercore.ErrorDecisionWithoutPersistence(gatewayprovider.ExecutionErrorPolicy(a.provider), status)
	if a.s.dependencies.Health != nil {
		var headers http.Header
		if a.response != nil {
			headers = a.response.Header
		}
		decision = gatewayprovider.ApplyExecutionHealth(
			ctx,
			a.s.dependencies.Health,
			a.provider,
			gatewayprovider.HealthObservationFromContext(ctx, status, headers, body, []string{model}),
		)
	}
	return forwardcore.ErrorDecision{
		Generic:           decision.ShouldReturnGenericError(),
		Failover:          decision.ShouldFailover(gatewayprovider.ExecutionErrorPolicy(a.provider), status, forwardcore.ShouldFailover(status)),
		RetrySameProvider: decision.RetryableOnSameProvider(gatewayprovider.ExecutionErrorPolicy(a.provider), status),
	}
}

// FailoverNotice records provider failover telemetry on the active HTTP
// boundary before outer provider switching runs. Nil boundaries or providers
// return immediately without recording diagnostic events.
func (a *convertedAttempt) FailoverNotice(status int, message string) {
	if a.c == nil || a.provider == nil {
		return
	}
	var reqID string
	if a.response != nil {
		reqID = a.response.Header.Get("x-request-id")
	}
	a.c.Observe(forwardcore.Notice{
		Platform:           a.provider.Record.Platform,
		ProviderID:         a.provider.Record.ID,
		ProviderName:       a.provider.Record.Name,
		UpstreamStatusCode: status,
		UpstreamRequestID:  reqID,
		Kind:               "failover",
		Message:            message,
	})
}

// FailoverError returns an UpstreamFailoverError for the outer provider loop.
func (*convertedAttempt) FailoverError(status int, body []byte, retry bool) error {
	return &forwardcore.UpstreamFailoverError{
		StatusCode:              status,
		ResponseBody:            body,
		RetryableOnSameProvider: retry,
	}
}

// WriteClientError presents an unrecoverable HTTP error in a.source's client
// envelope, promoting to an untimed terminal SSE error when CommittedSSE is
// true. Boundaries without ConvertedHTTPBoundary fall back to MessageError.
func (a *convertedAttempt) WriteClientError(status int, kind, message string) {
	if cb, ok := a.c.(ConvertedHTTPBoundary); ok {
		cb.WriteConvertedError(a.source, a.originalModel, status, kind, "", message)
		return
	}
	if a.c != nil {
		a.c.MessageError(status, kind, message)
	}
}

// ReadConverted delegates streaming or buffered response conversion to the
// shared openaiforward readers over NewDeferredOutputContext. Nil reader results
// return nil with err, while non-nil results project native token buckets into forward.Result.
func (a *convertedAttempt) ReadConverted(
	ctx context.Context,
	exchange *convert.Exchange,
	clientStream bool,
	originalModel, mappedModel string,
	reasoningEffort *string,
	startTime time.Time,
) (*forwardcore.Result, error) {
	outCtx := upstream.NewDeferredOutputContext(a.c.Sink())
	opts := a.convertedOutputOptions(originalModel, mappedModel)

	var res *openaiforward.Result
	var err error
	if clientStream {
		res, err = openaiforward.ReadConvertedAnthropicStreaming(
			ctx, a.response, outCtx, exchange, opts,
			originalModel, mappedModel, mappedModel, reasoningEffort, startTime,
		)
	} else {
		res, err = openaiforward.ReadConvertedAnthropicBuffered(
			ctx, a.response, outCtx, exchange, opts,
			originalModel, mappedModel, mappedModel, reasoningEffort, startTime,
		)
	}
	if res == nil {
		return nil, err
	}
	var nativeUsage upstream.TokenUsage
	if res.NativeUsage != nil {
		nativeUsage = *res.NativeUsage
	}
	return &forwardcore.Result{
		RequestID:                   res.RequestID,
		UpstreamHeaders:             res.Headers,
		Usage:                       nativeUsage,
		Model:                       res.Model,
		UpstreamModel:               res.UpstreamModel,
		UpstreamResponseServiceTier: a.c.ServiceTier(),
		UpstreamResponseModel:       res.UpstreamResponseModel,
		ReasoningEffort:             res.ReasoningEffort,
		Stream:                      res.Stream,
		Duration:                    res.Duration,
		ClientDisconnect:            res.ClientDisconnect,
	}, err
}

// convertedOutputOptions builds openaiforward.ConvertedAnthropicOutputOptions
// with single-pass stream failure classification, OAuth tool-name restoration,
// cache TTL bucket overrides, and untimed terminal SSE writers. Non-positive
// MaxLineSize settings default to 500 MiB, and boundaries lacking
// ConvertedHTTPBoundary fall back to MessageError.
func (a *convertedAttempt) convertedOutputOptions(originalModel, mappedModel string) openaiforward.ConvertedAnthropicOutputOptions {
	maxFrameBytes := 500 * 1024 * 1024
	if a.s.options.MaxLineSize > 0 {
		maxFrameBytes = a.s.options.MaxLineSize
	}
	var providerID int64
	if a.provider != nil {
		providerID = a.provider.Record.ID
	}

	convBoundary, hasConvBoundary := a.c.(ConvertedHTTPBoundary)

	opts := openaiforward.ConvertedAnthropicOutputOptions{
		ProviderID:       providerID,
		MaxFrameBytes:    maxFrameBytes,
		DefaultErrorType: "server_error",
		StreamInterval: func() time.Duration {
			return a.s.options.StreamInterval
		},
		UpdateWindow: func(ctx context.Context, headers http.Header) {
			gatewayprovider.ObserveExecutionSessionWindow(ctx, a.s.dependencies.Health, a.provider, headers)
		},
		CopyHeaders: func(dst, src http.Header) {
			if a.c != nil && a.c.HasHeaderFilter() {
				a.c.WriteHeaders(dst, src, false)
			}
		},
		RestoreToolNames: func(body []byte) []byte {
			var rw *anthropic.ToolNameRewrite
			if a.state != nil {
				rw = a.state.ToolNames
			}
			return anthropic.RestoreToolNamesInBytes(body, rw)
		},
		OverrideCache: func(ctx context.Context) (string, bool) {
			return a.s.cacheUsageOverride(ctx, a.provider)
		},
		CommittedSSE: func() bool {
			if hasConvBoundary {
				return convBoundary.CommittedSSE()
			}
			return a.c != nil && a.c.Written()
		},
		WriteError: func(status int, errType, code, message string) {
			if hasConvBoundary {
				convBoundary.WriteConvertedError(a.source, originalModel, status, errType, code, message)
				return
			}
			if a.c != nil {
				a.c.MessageError(status, errType, message)
			}
		},
		WriteLocalTerminalError: func(status int, errType, code, message string) {
			if hasConvBoundary {
				convBoundary.WriteLocalTerminalError(a.source, originalModel, status, errType, code, message)
				return
			}
			if a.c != nil {
				a.c.MessageError(status, errType, message)
			}
		},
		WriteRawTerminalSSE: func(data []byte) bool {
			if hasConvBoundary {
				return convBoundary.WriteRawTerminalSSE(data)
			}
			return false
		},
		StreamFailure: func(ctx context.Context, payload []byte, message string, started bool) openaiforward.ConvertedAnthropicFailure {
			return a.classifyStreamFailure(ctx, payload, message, mappedModel, started)
		},
		PreOutputReadFailure: func(readErr error) error {
			// Convert pre-semantic stream truncation or Bedrock EventStream exceptions
			// into retryable failover signals before any client bytes are flushed.
			return &forwardcore.UpstreamFailoverError{
				StatusCode:              http.StatusBadGateway,
				ResponseBody:            buildSyntheticStreamFailureBytes(readErr),
				RetryableOnSameProvider: true,
			}
		},
	}

	if a.s.dependencies.Health != nil && a.provider != nil {
		opts.HandleTimeout = func(ctx context.Context, model string) {
			a.s.dependencies.Health.Core.HandleStreamTimeout(ctx, gatewayprovider.ExecutionRecord(a.provider), model)
		}
	}
	return opts
}

// classifyStreamFailure evaluates an upstream Anthropic SSE error event once and
// selects either pre-semantic provider failover or client error metadata.
// Persisted health policy runs only when clientOutputStarted is false so a
// single upstream stream error frame never triggers duplicate health penalties.
//
// When failover is declined or output has already begun, Ops records the
// sanitized failure and configured error passthrough rules can override the
// client status or message unless health policy requested a generic 500 error.
func (a *convertedAttempt) classifyStreamFailure(
	ctx context.Context,
	payload []byte,
	message, mappedModel string,
	clientOutputStarted bool,
) openaiforward.ConvertedAnthropicFailure {
	rawErrType := extractAnthropicErrorTypeBytes(payload)
	semanticStatus := mapAnthropicStreamErrorStatus(rawErrType)
	cleanMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(message))
	if cleanMsg == "" {
		cleanMsg = "Upstream stream error"
	}

	decision := providercore.ErrorDecisionWithoutPersistence(gatewayprovider.ExecutionErrorPolicy(a.provider), semanticStatus)
	if !clientOutputStarted && a.s.dependencies.Health != nil {
		var headers http.Header
		if a.response != nil {
			headers = a.response.Header
		}
		decision = gatewayprovider.ApplyExecutionHealth(
			ctx,
			a.s.dependencies.Health,
			a.provider,
			gatewayprovider.HealthObservationFromContext(ctx, semanticStatus, headers, payload, []string{mappedModel}),
		)
	}

	if !clientOutputStarted && !decision.ShouldReturnGenericError() &&
		decision.ShouldFailover(gatewayprovider.ExecutionErrorPolicy(a.provider), semanticStatus, forwardcore.ShouldFailover(semanticStatus)) {
		a.FailoverNotice(semanticStatus, cleanMsg)
		return openaiforward.ConvertedAnthropicFailure{
			Failover: a.FailoverError(
				semanticStatus,
				payload,
				decision.RetryableOnSameProvider(gatewayprovider.ExecutionErrorPolicy(a.provider), semanticStatus),
			),
		}
	}

	if a.c != nil && a.provider != nil {
		var reqID string
		if a.response != nil {
			reqID = a.response.Header.Get("x-request-id")
		}
		a.c.SetError(semanticStatus, cleanMsg, "")
		a.c.Observe(forwardcore.Notice{
			Platform:           a.provider.Record.Platform,
			ProviderID:         a.provider.Record.ID,
			ProviderName:       a.provider.Record.Name,
			UpstreamStatusCode: semanticStatus,
			UpstreamRequestID:  reqID,
			Kind:               "stream_error",
			Message:            cleanMsg,
		})
	}

	if decision.ShouldReturnGenericError() {
		return openaiforward.ConvertedAnthropicFailure{
			Status:  http.StatusInternalServerError,
			Type:    "server_error",
			Code:    "server_error",
			Message: "Upstream gateway error",
		}
	}

	outStatus := forwardcore.MapStatus(semanticStatus)
	outType := rawErrType
	if outType == "" {
		outType = "server_error"
	}
	if a.c != nil && a.provider != nil && len(payload) > 0 {
		if rule := a.c.MatchRule(a.provider.Record.Platform, semanticStatus, payload); rule != nil {
			if !rule.PassthroughCode && rule.ResponseCode != nil {
				outStatus = *rule.ResponseCode
			} else if rule.PassthroughCode {
				outStatus = semanticStatus
			}
			if !rule.PassthroughBody && rule.CustomMessage != nil {
				cleanMsg = *rule.CustomMessage
			}
			if rule.SkipMonitoring {
				a.c.SkipMonitoring()
			}
			if a.c != nil {
				a.c.Commit()
			}
		}
	}

	return openaiforward.ConvertedAnthropicFailure{
		Status:  outStatus,
		Type:    outType,
		Code:    rawErrType,
		Message: cleanMsg,
	}
}

// mapAnthropicStreamErrorStatus maps an Anthropic SSE error.type identifier to
// its corresponding HTTP status code for health and failover classification.
// Unrecognized or blank error types default to HTTP 403 Forbidden.
func mapAnthropicStreamErrorStatus(errType string) int {
	switch strings.ToLower(strings.TrimSpace(errType)) {
	case "overloaded_error":
		return 529
	case "rate_limit_error":
		return http.StatusTooManyRequests
	case "authentication_error":
		return http.StatusUnauthorized
	case "permission_error":
		return http.StatusForbidden
	case "invalid_request_error":
		return http.StatusBadRequest
	case "not_found_error":
		return http.StatusNotFound
	case "timeout_error", "gateway_timeout":
		return http.StatusGatewayTimeout
	case "api_error", "internal_server_error", "server_error":
		return http.StatusInternalServerError
	default:
		return http.StatusForbidden
	}
}

// extractBedrockChunkEventTypeBytes extracts top-level "type" from decoded
// Bedrock chunk JSON bytes via jsontext without map or gjson allocation.
// Non-object payloads, missing keys, or non-string values return an empty string.
func extractBedrockChunkEventTypeBytes(data []byte) string {
	dec := jsontext.NewDecoder(bytes.NewReader(data), jsontext.AllowInvalidUTF8(true))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return ""
	}
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return ""
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return ""
		}
		if key == "type" {
			var typ string
			if jsonv2.Unmarshal(val, &typ, jsontext.AllowInvalidUTF8(true)) == nil {
				return strings.TrimSpace(typ)
			}
			return ""
		}
	}
	return ""
}

// extractAnthropicErrorTypeBytes extracts nested error.type from an Anthropic
// SSE error JSON object via jsontext without map or gjson decoding. Malformed
// tokens, missing error objects, or non-string types return an empty string.
func extractAnthropicErrorTypeBytes(payload []byte) string {
	dec := jsontext.NewDecoder(bytes.NewReader(payload), jsontext.AllowInvalidUTF8(true))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return ""
	}
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return ""
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return ""
		}
		if key != "error" || len(val) == 0 || val.Kind() != '{' {
			continue
		}
		subDec := jsontext.NewDecoder(bytes.NewReader(val), jsontext.AllowInvalidUTF8(true))
		if subTok, subErr := subDec.ReadToken(); subErr != nil || subTok.Kind() != '{' {
			return ""
		}
		for subDec.PeekKind() != '}' {
			skTok, skErr := subDec.ReadToken()
			if skErr != nil {
				return ""
			}
			sKey := skTok.String()
			sVal, svErr := subDec.ReadValue()
			if svErr != nil {
				return ""
			}
			if sKey == "type" {
				var typ string
				if jsonv2.Unmarshal(sVal, &typ, jsontext.AllowInvalidUTF8(true)) == nil {
					return strings.TrimSpace(typ)
				}
				return ""
			}
		}
	}
	return ""
}

// buildSyntheticStreamFailureBytes encodes a minimal Anthropic JSON error
// payload via jsonv2 for pre-output read failure failover diagnostics. Nil or
// blank cause strings use a default interruption message with queries sanitized.
func buildSyntheticStreamFailureBytes(cause error) []byte {
	// synthErrInner represents the inner Anthropic error object on synthetic failover payloads.
	type synthErrInner struct {
		Type    string `json:"type"`    // fixed "upstream_stream_error" tag
		Message string `json:"message"` // sanitized transport error description
	}
	// synthErrEnvelope wraps synthErrInner in an Anthropic error envelope.
	type synthErrEnvelope struct {
		Type  string        `json:"type"`  // literal "error" event tag
		Error synthErrInner `json:"error"` // nested error details
	}
	msg := "Upstream stream interrupted before semantic output"
	if cause != nil {
		if clean := logredact.SanitizeUpstreamQueries(strings.TrimSpace(cause.Error())); clean != "" {
			msg = clean
		}
	}
	encoded, err := jsonv2.Marshal(synthErrEnvelope{
		Type:  "error",
		Error: synthErrInner{Type: "upstream_stream_error", Message: msg},
	})
	if err != nil {
		return []byte(`{"type":"error","error":{"type":"upstream_stream_error","message":"stream read failure"}}`)
	}
	return encoded
}

// stripTopLevelMessagesMemberBytes removes every top-level member named
// targetKey from a JSON object slice via jsontext while preserving invalid
// UTF-8, key order, and 64-bit integer literals. Non-object inputs or unchanged
// objects return body unmodified.
func stripTopLevelMessagesMemberBytes(body []byte, targetKey string) []byte {
	if len(body) == 0 || targetKey == "" {
		return body
	}
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return body
	}
	// rawMember pairs one top-level object key with its verbatim JSON value slice.
	type rawMember struct {
		name  string         // decoded object member key
		value jsontext.Value // cloned raw JSON value bytes
	}
	var kept []rawMember
	removed := false
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return body
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return body
		}
		if key == targetKey {
			removed = true
			continue
		}
		kept = append(kept, rawMember{name: key, value: val.Clone()})
	}
	if !removed {
		return body
	}
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if enc.WriteToken(jsontext.BeginObject) != nil {
		return body
	}
	for _, m := range kept {
		if enc.WriteToken(jsontext.String(m.name)) != nil || enc.WriteValue(m.value) != nil {
			return body
		}
	}
	if enc.WriteToken(jsontext.EndObject) != nil {
		return body
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// translateBedrockStreamExceptionSSE converts an AWS EventStream exception or
// error frame returned by BedrockEventStreamDecoder.Decode into a normalized
// Anthropic SSE error frame. Non-exception transport errors return nil and
// false so callers propagate socket resets and CRC corruption directly.
func translateBedrockStreamExceptionSSE(err error) ([]byte, bool) {
	if err == nil {
		return nil, false
	}
	text := err.Error()
	var excType, rawPayload string
	switch {
	case strings.HasPrefix(text, "bedrock exception: "):
		rest := strings.TrimPrefix(text, "bedrock exception: ")
		if head, tail, ok := strings.Cut(rest, ": "); ok {
			excType = strings.TrimSpace(head)
			rawPayload = strings.TrimSpace(tail)
		} else {
			excType = strings.TrimSpace(rest)
		}
	case strings.HasPrefix(text, "bedrock error: "):
		excType = "internalServerException"
		rawPayload = strings.TrimSpace(strings.TrimPrefix(text, "bedrock error: "))
	default:
		return nil, false
	}

	anthropicErrType := mapBedrockExceptionToAnthropicType(excType)
	msg := strings.TrimSpace(upstream.ExtractErrorMessage([]byte(rawPayload)))
	if msg == "" {
		msg = rawPayload
	}
	if msg == "" {
		msg = excType
	}

	// bedrockSSEErrInner encodes the nested Anthropic error payload for a Bedrock EventStream exception.
	type bedrockSSEErrInner struct {
		Type    string `json:"type"`    // mapped Anthropic error category
		Message string `json:"message"` // extracted Bedrock exception message
	}
	// bedrockSSEErrEnvelope wraps bedrockSSEErrInner in an Anthropic SSE error object.
	type bedrockSSEErrEnvelope struct {
		Type  string             `json:"type"`  // literal "error" event tag
		Error bedrockSSEErrInner `json:"error"` // nested mapped error details
	}
	encoded, mErr := jsonv2.Marshal(bedrockSSEErrEnvelope{
		Type:  "error",
		Error: bedrockSSEErrInner{Type: anthropicErrType, Message: msg},
	})
	if mErr != nil {
		return nil, false
	}
	return sse.Append(nil, sse.Frame{Event: "error", Data: encoded}), true
}

// mapBedrockExceptionToAnthropicType maps AWS Bedrock EventStream exception
// identifiers onto standard Anthropic Messages SSE error.type categories.
// Unknown or internal exception names default to "api_error" for 5xx classification.
func mapBedrockExceptionToAnthropicType(excType string) string {
	switch strings.ToLower(strings.TrimSpace(excType)) {
	case "throttlingexception":
		return "rate_limit_error"
	case "serviceunavailableexception", "modelnotreadyexception":
		return "overloaded_error"
	case "validationexception":
		return "invalid_request_error"
	case "accessdeniedexception":
		return "permission_error"
	case "resourcenotfoundexception":
		return "not_found_error"
	case "modeltimeoutexception":
		return "timeout_error"
	default:
		return "api_error"
	}
}
