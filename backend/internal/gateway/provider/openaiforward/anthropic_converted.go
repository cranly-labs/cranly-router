package openaiforward

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// anthropic_converted.go executes merged IR protocol conversion onto compatible
// Anthropic Messages endpoints for OpenAI Responses and Chat Completions
// clients. Both client protocols share one convert.Exchange request pipeline
// and the merged ReadConvertedAnthropicStreaming and
// ReadConvertedAnthropicBuffered response readers without round-tripping
// requests through lossy subset structs or dropping unknown Extra members.
//
// Pre-conversion continuation guards trim latest-turn Responses histories and
// preserve Claude Code todo guards on raw bytes before IR decoding. Post-encoding
// preparation runs PrepareModelAttributeParameters alongside Mimic ->
// EnforceCacheControlLimit -> InjectAnthropicCacheControlTTL1h in strict order,
// while cross-model thinking signature HTTP 400 rejections trigger one lossless
// jsontext stripping retry before provider health and failover evaluation run.

// ConvertedAnthropicPorts supplies dialect resolution, lossless preparation,
// signature recovery, and merged reader bindings for ForwardConvertedAnthropic.
type ConvertedAnthropicPorts interface {
	// Profile returns the selected provider's platform, credential, and OAuth
	// capability snapshot for this attempt.
	Profile() MessagesProfile
	// BillingModel resolves the effective billing model from caller model and
	// fallback. Non-empty fallback values take precedence when group or key model
	// mapping applies, whereas empty fallbacks retain the caller's model name.
	BillingModel(model, fallback string) string
	// UpstreamModel normalizes the billing model into the wire model sent upstream.
	UpstreamModel(model string) string
	// ResolveAnthropicDialect resolves catalog attributes and model rules from
	// raw client bytes and returns the target convert.Dialect plus passback mode.
	// Attribute lookup errors stop execution before request conversion begins.
	ResolveAnthropicDialect(ctx context.Context, model string, body []byte) (convert.Dialect, bool, error)
	// ConversionRuntime returns the wall-clock and cryptographic ID runtime passed to convert.NewExchange.
	ConversionRuntime() convert.Runtime
	// SourceError writes a client-protocol rejection, promoting to an untimed
	// terminal SSE error when CommittedSSE is already true on the active stream.
	// Uncommitted responses mark semantic commitment before writing HTTP JSON.
	SourceError(source protocol.ProtocolID, status int, kind, message string)
	// Debug logs structured attempt diagnostics through the execution logger.
	Debug(msg string, fields ...zap.Field)
	// Mimic applies optional OAuth system, metadata, cache, and tool preparation
	// to converted Anthropic Messages bytes before cache breakpoint limiting.
	// Ineligible API-key routes return the supplied body slice unchanged.
	Mimic(ctx context.Context, body []byte, system json.RawMessage, model string) []byte
	// FilterSearch removes incompatible web-search history blocks for the target
	// model before cache-control limiting runs. Models supporting web-search
	// history return the supplied body slice unchanged.
	FilterSearch(body []byte, model string) []byte
	// CacheLimit enforces Anthropic cache-control structural validity and total
	// breakpoint limits after Mimic and FilterSearch run. Payloads already within
	// the breakpoint cap return unchanged before PrepareModel executes.
	CacheLimit(body []byte) []byte
	// PrepareModel applies selected-model attribute parameters to prepared
	// Anthropic Messages request bytes before 1h cache TTL injection. Returned
	// errors abort the attempt before building or dispatching the HTTP request.
	PrepareModel(ctx context.Context, body []byte, model string) ([]byte, error)
	// CacheTTL rewrites existing ephemeral cache_control markers to a 1h lifetime
	// when the API Key and target Claude model qualify for forced 1h caching.
	// Ineligible targets return body unchanged.
	CacheTTL(ctx context.Context, body []byte, model string) []byte
	// ProtocolAPIKey returns the configured Anthropic-compatible API credential
	// for this attempt. Blank credentials abort ForwardConvertedAnthropic before
	// TargetURL validation or outbound network dispatch begins.
	ProtocolAPIKey() string
	// TargetURL resolves and URL-policy validates the provider's Anthropic
	// Messages endpoint URL. Validation errors abort the attempt before transport
	// setup or HTTP request construction runs.
	TargetURL() (string, error)
	// PrepareTransport resolves provider proxy configuration on the attempt state before request dispatch.
	PrepareTransport()
	// StreamContext detaches client cancellation for streaming upstream requests
	// and returns its release callback invoked right after request construction.
	// Non-streaming contexts remain attached to caller deadlines.
	StreamContext(ctx context.Context, stream bool) (context.Context, context.CancelFunc)
	// BuildConvertedNative constructs an authenticated Anthropic HTTP request and
	// returns its adopted sanitized wire body bytes alongside the request.
	// Construction errors stop forwarding before SendNative is called.
	BuildConvertedNative(ctx context.Context, body []byte, key, url string) (*http.Request, []byte, error)
	// SendNative dispatches req through the selected provider transport and proxy
	// pool. Transport errors return nil or closed bodies, while callers close the
	// returned response body after successful reading or HTTP error handling ends.
	SendNative(req *http.Request) (*http.Response, error)
	// TransportErrorNative classifies network transport failures into cancellation
	// or sanitized provider failover errors for outer retry handling.
	TransportErrorNative(ctx context.Context, err error) error
	// ReadUpstreamError reads bounded error bytes, closes the network body, and
	// installs a replayable reader for downstream health checks. It returns
	// captured bytes alongside a query-sanitized summary string.
	ReadUpstreamError(resp *http.Response) ([]byte, string)
	// IsSignatureError reports whether captured HTTP 400 bytes indicate a thinking
	// signature rejection eligible for one lossless signature-stripping retry.
	IsSignatureError(ctx context.Context, body []byte) bool
	// ObserveSignatureRetry records diagnostic telemetry before re-dispatching a
	// stripped-signature retry on the same provider.
	ObserveSignatureRetry(resp *http.Response, body []byte, msg string)
	// FailoverHTTP evaluates provider failure policy on HTTP >= 400 and returns
	// a non-nil failover error when outer provider switching is permitted. Nil
	// returns proceed to final client error presentation.
	FailoverHTTP(ctx context.Context, resp *http.Response, body []byte, msg, model string) error
	// MapStatus maps upstream HTTP statuses >= 500 to 502 while preserving 4xx statuses for client error output.
	MapStatus(status int) int
	// ResponsesEffort extracts normalized OpenAI Responses reasoning effort from
	// client bytes for the first recognized model candidate. Absent or disabled
	// effort controls return nil.
	ResponsesEffort(body []byte, models ...string) *string
	// ChatEffort extracts normalized OpenAI Chat Completions reasoning effort
	// from client bytes for the first recognized model candidate. Absent effort
	// fields return nil.
	ChatEffort(body []byte, models ...string) *string
	// ThinkingFallback derives display effort when model thinking is enabled on
	// body without an explicit recorded effort observation. Non-nil effort
	// pointers return unchanged, and disabled thinking leaves nil pointers nil.
	ThinkingFallback(effort *string, body []byte, model string) *string
	// Sink returns the synchronous OutputSink backing NewDeferredOutputContext on this attempt.
	Sink() upstream.OutputSink
	// ConvertedOutputOptions binds merged Anthropic reader callbacks, single-pass
	// stream failure policy, and untimed terminal SSE writers to this attempt.
	// Callers keep the HTTP context and response alive until reading finishes.
	ConvertedOutputOptions(resp *http.Response, source protocol.ProtocolID, original, billing, model string, requestBodyLen int) ConvertedAnthropicOutputOptions
}

// ForwardConvertedAnthropic converts an OpenAI Responses or Chat Completions
// request into one Anthropic Messages exchange via convert.Exchange and reads
// streaming or buffered client output through ReadConvertedAnthropicStreaming
// or ReadConvertedAnthropicBuffered. Callers supply non-empty model JSON and an
// active ConvertedAnthropicPorts implementation, and ForwardConvertedAnthropic
// closes the retained upstream response body when reading or error handling ends.
//
// Outgoing preparation preserves Mimic -> CacheLimit -> PrepareModel -> CacheTTL
// ordering across both initial sends and signature retries. When an upstream
// rejects replayed cross-model thinking signatures with HTTP 400, the first
// response body is closed and one lossless jsontext signature-stripping retry
// runs on the same provider before FailoverHTTP evaluates health policy.
func ForwardConvertedAnthropic(
	ctx context.Context,
	source protocol.ProtocolID,
	body []byte,
	defaultMappedModel string,
	p ConvertedAnthropicPorts,
) (*Result, error) {
	if source != protocol.ProtocolOpenAIResponses && source != protocol.ProtocolOpenAIChatCompletions {
		return nil, fmt.Errorf("unsupported converted anthropic source protocol %q", source)
	}

	profile := p.Profile()
	startTime := time.Now()

	if source == protocol.ProtocolOpenAIResponses {
		normalizedBody, normalized, err := gatewayprovider.NormalizeOpenAIResponsesLegacyIngress(body)
		if err != nil {
			p.SourceError(source, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
			return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(err)
		}
		if normalized {
			body = normalizedBody
		}
	}

	clientInfo, ok := inspectConvertedClientBody(body)
	if !ok {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		parseErr := fmt.Errorf("parse %s request: invalid JSON object", source)
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "Failed to parse request body").WithCause(parseErr)
	}

	originalModel := clientInfo.model
	if strings.TrimSpace(originalModel) == "" {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", "model is required")
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", "model is required")
	}

	if err := requeststate.ValidateOpenAIReasoningEffort(body, originalModel); err != nil {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(err)
	}

	billingModel := p.BillingModel(originalModel, defaultMappedModel)
	upstreamModel := p.UpstreamModel(billingModel)

	dialect, passback, err := p.ResolveAnthropicDialect(ctx, upstreamModel, body)
	if err != nil {
		return nil, fmt.Errorf("resolve anthropic dialect: %w", err)
	}

	exchange, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   source,
			Upstream: protocol.ProtocolAnthropicMessages,
			Dialect:  dialect,
		},
		ClientModel: originalModel,
		Runtime:     p.ConversionRuntime(),
	})
	if err != nil {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("convert %s to anthropic: %w", source, err))
	}

	baseAnthropicBody, err := exchange.UpstreamRequest()
	if err != nil {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, apperror.BadRequest("MODEL_CAPABILITY_UNSUPPORTED", err.Error()).WithCause(fmt.Errorf("encode converted anthropic request: %w", err))
	}
	upstream.ObserveExchangeConversion(ctx, exchange)

	baseAnthropicBody, err = patchConvertedAnthropicBodyBytes(baseAnthropicBody, upstreamModel)
	if err != nil {
		return nil, fmt.Errorf("patch converted anthropic request: %w", err)
	}

	clientStream := exchange.Info().Stream
	debugLabel := "openai chat_completions (ir): forwarding via native anthropic endpoint"
	if source == protocol.ProtocolOpenAIResponses {
		debugLabel = "openai responses (ir): forwarding via native anthropic endpoint"
	}
	p.Debug(debugLabel,
		zap.Int64("provider_id", profile.ID),
		zap.String("original_model", originalModel),
		zap.String("billing_model", billingModel),
		zap.String("upstream_model", upstreamModel),
		zap.Bool("client_stream", clientStream),
	)

	apiKey := strings.TrimSpace(p.ProtocolAPIKey())
	if apiKey == "" {
		return nil, fmt.Errorf("provider %d missing api_key", profile.ID)
	}
	targetURL, err := p.TargetURL()
	if err != nil {
		return nil, err
	}
	p.PrepareTransport()

	sendOnce := func(candidateBase []byte, losses protocolanthropic.SignatureRetryLosses) (*http.Response, []byte, error) {
		prepared := candidateBase
		if profile.OAuth {
			prepared = p.Mimic(ctx, prepared, extractConvertedAnthropicSystemBytes(candidateBase), upstreamModel)
		}
		prepared = p.FilterSearch(prepared, upstreamModel)
		prepared = p.CacheLimit(prepared)
		var prepErr error
		prepared, prepErr = p.PrepareModel(ctx, prepared, upstreamModel)
		if prepErr != nil {
			return nil, nil, prepErr
		}
		prepared = p.CacheTTL(ctx, prepared, upstreamModel)

		upstreamCtx, releaseUpstreamCtx := p.StreamContext(ctx, true)
		upstreamReq, wireBody, buildErr := p.BuildConvertedNative(upstreamCtx, prepared, apiKey, targetURL)
		releaseUpstreamCtx()
		if buildErr != nil {
			return nil, nil, fmt.Errorf("build upstream request: %w", buildErr)
		}
		if losses.Tokens > 0 || losses.Reasoning > 0 {
			// Prepare the diagnostic snapshot only after the replacement payload
			// builds successfully and before the transport assigns its dispatch ordinal.
			upstream.StageSignatureRetry(ctx, exchange, losses.Tokens, losses.Reasoning)
		}
		sentResp, sendErr := p.SendNative(upstreamReq)
		if sendErr != nil {
			if sentResp != nil && sentResp.Body != nil {
				_ = sentResp.Body.Close()
			}
			return nil, nil, p.TransportErrorNative(ctx, sendErr)
		}
		return sentResp, wireBody, nil
	}

	resp, wireBody, err := sendOnce(baseAnthropicBody, protocolanthropic.SignatureRetryLosses{})
	if err != nil {
		return nil, err
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
	}()

	var preReadBody []byte
	var preReadMsg string
	preReadDone := false
	if resp.StatusCode == http.StatusBadRequest {
		preReadBody, preReadMsg = p.ReadUpstreamError(resp)
		preReadDone = true
		if p.IsSignatureError(ctx, preReadBody) {
			// Strip rejected historical thinking signatures losslessly via jsontext
			// and retry once on the same provider before falling back to HTTP 400.
			if strippedBase, stripped, losses := protocolanthropic.StripMessagesSignatureBlocksWithLosses(baseAnthropicBody, passback); stripped {
				p.ObserveSignatureRetry(resp, preReadBody, preReadMsg)
				_ = resp.Body.Close()
				resp = nil
				retryResp, retryWire, retryErr := sendOnce(strippedBase, losses)
				if retryErr != nil {
					return nil, retryErr
				}
				resp = retryResp
				wireBody = retryWire
				baseAnthropicBody = strippedBase
				preReadBody = nil
				preReadMsg = ""
				preReadDone = false
			}
		}
	}

	if resp.StatusCode >= http.StatusBadRequest {
		if !preReadDone {
			preReadBody, preReadMsg = p.ReadUpstreamError(resp)
		}
		if foErr := p.FailoverHTTP(ctx, resp, preReadBody, preReadMsg, upstreamModel); foErr != nil {
			return nil, foErr
		}
		p.SourceError(source, p.MapStatus(resp.StatusCode), "server_error", preReadMsg)
		return nil, fmt.Errorf("upstream error: %d %s", resp.StatusCode, preReadMsg)
	}

	var reasoningEffort *string
	if source == protocol.ProtocolOpenAIResponses || (!clientInfo.hasMessages && clientInfo.hasInput) {
		reasoningEffort = p.ResponsesEffort(body, upstreamModel, billingModel, originalModel)
	} else {
		reasoningEffort = p.ChatEffort(body, upstreamModel, billingModel, originalModel)
	}
	if convertedEffort := extractConvertedAnthropicEffortBytes(baseAnthropicBody); convertedEffort != nil {
		reasoningEffort = convertedEffort
	}
	// Retain pre-stripping client effort for billing when StripEffort cleared
	// outgoing wire effort fields during IR request encoding.
	if reasoningEffort == nil && dialect.StripEffort {
		if preStrip := protocol.NormalizeClaudeOutputEffort(exchange.Info().Effort); preStrip != nil {
			reasoningEffort = preStrip
		}
	}
	reasoningEffort = p.ThinkingFallback(reasoningEffort, body, billingModel)

	output := upstream.NewDeferredOutputContext(p.Sink())
	outOpts := p.ConvertedOutputOptions(resp, source, originalModel, billingModel, upstreamModel, len(body))

	var result *Result
	var readErr error
	if clientStream {
		result, readErr = ReadConvertedAnthropicStreaming(ctx, resp, output, exchange, outOpts, originalModel, billingModel, upstreamModel, reasoningEffort, startTime)
	} else {
		result, readErr = ReadConvertedAnthropicBuffered(ctx, resp, output, exchange, outOpts, originalModel, billingModel, upstreamModel, reasoningEffort, startTime)
	}

	if result != nil && result.NativeUsage != nil && strings.TrimSpace(result.NativeUsage.Speed) == "" &&
		strings.EqualFold(extractConvertedAnthropicSpeedBytes(wireBody), "fast") {
		result.NativeUsage.Speed = "fast"
	}
	return result, readErr
}

// patchConvertedAnthropicBodyBytes sets top-level model to upstreamModel (when
// non-empty) and stream to true on an encoded Anthropic Messages JSON object via
// ordered jsontext members. Unknown Extra fields and 64-bit integer literals
// remain intact; malformed JSON objects return a decoding error.
func patchConvertedAnthropicBodyBytes(body []byte, upstreamModel string) ([]byte, error) {
	members, err := readConvertedObjectMembers(body)
	if err != nil {
		return nil, err
	}
	if upstreamModel != "" {
		rawModel, mErr := jsonv2.Marshal(upstreamModel, jsontext.AllowInvalidUTF8(true))
		if mErr != nil {
			return nil, mErr
		}
		members = upsertConvertedMember(members, "model", rawModel)
	}
	members = upsertConvertedMember(members, "stream", jsontext.Value("true"))
	return encodeConvertedObjectMembers(members)
}

// extractConvertedAnthropicSystemBytes returns a cloned raw JSON value of
// top-level "system" from body via jsontext. Absent system fields or malformed
// JSON objects return nil so OAuth mimicry skips empty system extraction.
func extractConvertedAnthropicSystemBytes(body []byte) json.RawMessage {
	members, err := readConvertedObjectMembers(body)
	if err != nil {
		return nil
	}
	if rawSys, ok := findConvertedMember(members, "system"); ok {
		return json.RawMessage(bytes.Clone(rawSys))
	}
	return nil
}

// extractConvertedAnthropicEffortBytes extracts and normalizes top-level
// output_config.effort from body via jsontext. Missing output_config objects,
// non-string effort fields, or unrecognized effort levels return nil so caller
// effort observations remain intact.
func extractConvertedAnthropicEffortBytes(body []byte) *string {
	members, err := readConvertedObjectMembers(body)
	if err != nil {
		return nil
	}
	rawOC, ok := findConvertedMember(members, "output_config")
	if !ok || len(rawOC) == 0 || rawOC[0] != '{' {
		return nil
	}
	ocMembers, err := readConvertedObjectMembers(rawOC)
	if err != nil {
		return nil
	}
	rawEffort, ok := findConvertedMember(ocMembers, "effort")
	if !ok {
		return nil
	}
	var effort string
	if jsonv2.Unmarshal(rawEffort, &effort, jsontext.AllowInvalidUTF8(true)) != nil {
		return nil
	}
	return protocol.NormalizeClaudeOutputEffort(effort)
}

// extractConvertedAnthropicSpeedBytes reads top-level "speed" from wireBody via
// jsontext and returns its trimmed string value. Missing fields, non-string
// values, or malformed objects return an empty string.
func extractConvertedAnthropicSpeedBytes(wireBody []byte) string {
	members, err := readConvertedObjectMembers(wireBody)
	if err != nil {
		return ""
	}
	rawSpeed, ok := findConvertedMember(members, "speed")
	if !ok {
		return ""
	}
	var speed string
	if jsonv2.Unmarshal(rawSpeed, &speed, jsontext.AllowInvalidUTF8(true)) != nil {
		return ""
	}
	return strings.TrimSpace(speed)
}
