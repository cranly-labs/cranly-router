package openaiforward

import (
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.uber.org/zap"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	protocolopenai "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// raw_fallback_converted.go executes merged IR protocol conversion onto OpenAI-
// compatible Chat Completions upstreams for Anthropic Messages and OpenAI
// Responses clients. Both client protocols share one convert.Exchange request
// pipeline and the merged ReadRawChatConvertedStreaming and
// ReadRawChatConvertedBuffered response readers without round-tripping payloads
// through subset structs or consulting external reasoning caches.
//
// Plaintext reasoning continuity travels inside trc1:plain carrier envelopes
// across turns, while post-encoding model-attribute adjustments run uniformly
// across both client sources before group effort caps and fast-tier policy.
// Outgoing JSON member edits use ordered jsontext slices so unmodeled Extra
// fields and 64-bit integer literals survive intact.

// ConvertedRawChatPorts supplies client-protocol-parameterized dialect
// resolution, model preparation, transport execution, and merged response
// reading callbacks for RunConvertedRawChat.
type ConvertedRawChatPorts interface {
	// Profile returns the selected provider's platform, credential, and protocol
	// capability snapshot for this Chat attempt.
	Profile() MessagesProfile
	// SourceError marks response commitment and writes a local error in source's
	// client protocol envelope. Already committed SSE streams receive an untimed
	// terminal SSE error event instead of raw JSON.
	SourceError(source protocol.ProtocolID, status int, kind, message string)
	// BillingModel selects the charge model from requested and group-mapped names.
	// Non-empty group mappings or caller fallbacks override the client model when
	// no explicit provider billing override applies.
	BillingModel(model, fallback string) string
	// UpstreamModel normalizes the charge model for the selected upstream provider.
	UpstreamModel(model string) string
	// ResolveChatDialect resolves target Chat Completions model rules from body
	// and builds the convert.Dialect for the selected provider attempt. Successful
	// resolution publishes capabilities onto the attempt provider before returning.
	ResolveChatDialect(model string, body []byte) (convert.Dialect, error)
	// ConversionRuntime returns the wall-clock and random ID source for convert.Exchange.
	ConversionRuntime() convert.Runtime
	// MessagesEffortValue reconciles explicit Anthropic max client effort against
	// resolved catalog attributes on the selected provider, retaining "max" when
	// supported or unknown and downgrading converted "max" to "xhigh" when
	// explicitly unsupported. Other client effort values return convertedEffort
	// unchanged without inspecting catalog capabilities.
	MessagesEffortValue(clientEffort, model, convertedEffort string) string
	// PrepareModel applies selected-provider Chat model attributes to outgoing
	// request bytes before group effort or fast-tier policies run. Capability or
	// parameter errors return before target resolution or upstream HTTP exchange proceeds.
	PrepareModel(ctx context.Context, body []byte) ([]byte, error)
	// ApplyEffort applies admitted group reasoning-effort caps to converted
	// Messages-source Chat request bytes. Over-limit errors mark local policy
	// denial, write an Anthropic 403 rejection, and stop upstream forwarding.
	ApplyEffort(ctx context.Context, body []byte) ([]byte, bool, error)
	// ApplySourceFast applies provider fast-tier policy to outgoing Chat bytes and
	// formats any BlockedError in source's client protocol envelope. Callers stop
	// request preparation whenever a non-nil policy error is returned.
	ApplySourceFast(ctx context.Context, source protocol.ProtocolID, model string, body []byte) ([]byte, error)
	// Debug writes structured diagnostic fields through the execution logger.
	Debug(msg string, fields ...zap.Field)
	// ObserveModel records the mapped upstream model on request diagnostics.
	ObserveModel(model string)
	// Target resolves the Chat-compatible credential and validated endpoint URL
	// for the selected provider attempt. Credential or URL validation errors stop
	// request preparation before any upstream network exchange begins.
	Target(ctx context.Context) (string, string, error)
	// Endpoint records the actual upstream request path on request observations.
	Endpoint(endpoint string)
	// SendCC exchanges a prepared Chat request body using the provider transport
	// and TLS routing state. Callers close successful response bodies after
	// response reading or final HTTP error presentation completes.
	SendCC(ctx context.Context, targetURL string, body []byte, stream bool, apiKey string) (*http.Response, error)
	// ReadUpstreamError captures bounded upstream error bytes, closes the consumed
	// network body, and installs a replayable reader on resp. Returned message
	// strings sanitize query parameters before failure classification runs.
	ReadUpstreamError(resp *http.Response) ([]byte, string)
	// FailoverHTTP evaluates provider health policy on HTTP >= 400 upstream
	// responses. Non-nil errors return recovery guidance to the outer provider
	// loop, while nil keeps resp for terminal client error formatting.
	FailoverHTTP(ctx context.Context, resp *http.Response, body []byte, msg, model string) error
	// SourceErrorResponse formats terminal HTTP >= 400 upstream rejections in
	// source's client error envelope using the replayable body on resp. Callers
	// retain ownership of closing resp.Body after handling finishes.
	SourceErrorResponse(ctx context.Context, source protocol.ProtocolID, resp *http.Response, requestBody []byte, model string) (*Result, error)
	// UpdateGrokUsage records parsed Grok OAuth quota-header snapshots on HTTP 2xx
	// Chat responses and clears recoverable OAuth rate-limit state when quota
	// headers are absent. Nil executors, providers, or Grok health services return
	// safely without mutating account state.
	UpdateGrokUsage(ctx context.Context, model string, headers http.Header, status int)
	// Sink returns the synchronous downstream OutputSink for response writing.
	Sink() upstream.OutputSink
	// ConvertedRawOptions builds client-protocol-aware reader, observation, and
	// terminal error callbacks for ReadRawChatConvertedStreaming and
	// ReadRawChatConvertedBuffered on the live HTTP response. Callers keep request
	// state and resp alive until reading finishes and close resp.Body afterward.
	ConvertedRawOptions(resp *http.Response, source protocol.ProtocolID, original, billing, model string, serviceTier *string, requestBodyLen int) openai.ConvertedRawChatOptions
	// CyberPolicy reports whether upstream cyber-policy rejection was recorded on this attempt.
	CyberPolicy() bool
}

// convertedChatBodyPatch specifies lossless top-level JSON edits applied to
// Chat Completions request bytes produced by convert.Exchange.UpstreamRequest.
type convertedChatBodyPatch struct {
	// Model replaces the top-level "model" member when non-empty and leaves any
	// existing model member untouched when empty.
	Model string
	// EnsureIncludeUsage sets top-level stream=true and merges include_usage=true
	// into stream_options when true, leaving non-streaming bodies unchanged otherwise.
	EnsureIncludeUsage bool
	// DeleteFields lists top-level JSON member names removed before model and
	// stream_options updates run; empty slices remove nothing.
	DeleteFields []string
	// ReconcileEffort rewrites an existing string "reasoning_effort" member when
	// non-nil and removes the member when the callback returns an empty string.
	ReconcileEffort func(converted string) string
}

// RunConvertedRawChat converts an inbound Anthropic Messages or OpenAI
// Responses request body into one Chat Completions exchange via convert.Exchange
// and reads streaming or buffered client output through ReadRawChatConvertedStreaming
// or ReadRawChatConvertedBuffered.
//
// Reasoning continuity derives from trc1:plain carriers without external cache
// callbacks, and PrepareModel runs after encoding for every client protocol so
// clamping, DropFields, and platform reasoning normalizations take effect
// before group effort caps, fast-tier policy, and effort attribution. Response
// bodies are closed by this runner after reading or terminal error formatting.
func RunConvertedRawChat(
	ctx context.Context,
	source protocol.ProtocolID,
	body []byte,
	defaultMappedModel string,
	p ConvertedRawChatPorts,
) (*Result, error) {
	if source != protocol.ProtocolAnthropicMessages && source != protocol.ProtocolOpenAIResponses {
		return nil, fmt.Errorf("unsupported converted raw chat source protocol %q", source)
	}

	profile := p.Profile()
	startTime := time.Now()

	clientInfo, ok := inspectConvertedClientBody(body)
	if !ok {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", "Failed to parse request body")
		return nil, fmt.Errorf("parse %s request: invalid JSON object", source)
	}

	originalModel := clientInfo.model
	if source == protocol.ProtocolOpenAIResponses {
		originalModel = strings.TrimSpace(originalModel)
	}
	if strings.TrimSpace(originalModel) == "" {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", "model is required")
		return nil, errors.New("missing model in request")
	}

	if err := requeststate.ValidateOpenAIReasoningEffort(body, originalModel); err != nil {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, err
	}

	clientStream := clientInfo.stream
	billingModel := p.BillingModel(originalModel, defaultMappedModel)
	upstreamModel := p.UpstreamModel(billingModel)

	dialect, err := p.ResolveChatDialect(upstreamModel, body)
	if err != nil {
		return nil, fmt.Errorf("resolve chat dialect: %w", err)
	}

	exchange, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   source,
			Upstream: protocol.ProtocolOpenAIChatCompletions,
			Dialect:  dialect,
		},
		ClientModel: originalModel,
		Runtime:     p.ConversionRuntime(),
	})
	if err != nil {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, fmt.Errorf("convert %s to chat completions: %w", source, err)
	}
	chatBody, err := exchange.UpstreamRequest()
	if err != nil {
		p.SourceError(source, http.StatusBadRequest, "invalid_request_error", err.Error())
		return nil, fmt.Errorf("encode converted chat completions request: %w", err)
	}
	upstream.ObserveExchangeConversion(ctx, exchange)

	var deleteFields []string
	// Grok Chat Completions endpoints reject Responses-only prompt_cache_key
	// members; strip them losslessly before model-attribute and transport preparation.
	if profile.Grok {
		deleteFields = []string{"prompt_cache_key"}
	}
	var reconcileEffort func(string) string
	// Skip MessagesEffortValue reconciliation when thinking is explicitly disabled
	// so an accompanying output_config.effort cannot override the dialect fallback.
	if source == protocol.ProtocolAnthropicMessages && !clientInfo.thinkingDisabled && !dialect.StripEffort {
		reconcileEffort = func(converted string) string {
			return p.MessagesEffortValue(clientInfo.clientEffort, upstreamModel, converted)
		}
	}
	chatBody, err = patchConvertedChatBody(chatBody, convertedChatBodyPatch{
		Model:              upstreamModel,
		EnsureIncludeUsage: clientStream,
		DeleteFields:       deleteFields,
		ReconcileEffort:    reconcileEffort,
	})
	if err != nil {
		return nil, fmt.Errorf("patch converted chat completions request: %w", err)
	}

	// Apply selected-provider model attributes uniformly across Messages and
	// Responses sources so clamping, DropFields, and platform effort aliases take
	// effect before group policy and effort attribution inspect chatBody.
	chatBody, err = p.PrepareModel(ctx, chatBody)
	if err != nil {
		return nil, err
	}

	// Re-evaluate OpenAI group reasoning-effort caps on converted Messages bodies
	// because default Messages-to-Chat conversion injects medium effort when omitted.
	if source == protocol.ProtocolAnthropicMessages && profile.OpenAI {
		policyBody, changed, policyErr := p.ApplyEffort(ctx, chatBody)
		if policyErr != nil {
			return nil, policyErr
		}
		if changed {
			chatBody = policyBody
		}
	}

	serviceTier := requeststate.ExtractOpenAIServiceTierFromBody(body)
	chatBody, err = p.ApplySourceFast(ctx, source, upstreamModel, chatBody)
	if err != nil {
		return nil, err
	}
	if serviceTier == nil {
		serviceTier = requeststate.ExtractOpenAIServiceTierFromBody(chatBody)
	}

	reasoningEffort := requeststate.ExtractOpenAIReasoningEffortFromBody(chatBody)
	// Preserve pre-stripping client effort for billing and telemetry when
	// effort_model_mapping_strip_effort removed outgoing wire effort fields from chatBody.
	if reasoningEffort == nil && dialect.StripEffort {
		if norm := protocolopenai.NormalizeRecordedReasoningEffort(exchange.Info().Effort); norm != "" {
			reasoningEffort = &norm
		}
	}
	if source == protocol.ProtocolAnthropicMessages {
		reasoningEffort = gatewayprovider.ApplyThinkingEnabledFallback(reasoningEffort, body, billingModel)
	} else {
		reasoningEffort = gatewayprovider.ApplyThinkingEnabledFallback(reasoningEffort, chatBody, billingModel)
	}

	debugMsg := "openai responses (ir): forwarding via raw chat completions"
	if source == protocol.ProtocolAnthropicMessages {
		debugMsg = "openai messages (ir): forwarding via raw chat completions"
	}
	p.Debug(debugMsg,
		zap.Int64("provider_id", profile.ID),
		zap.String("original_model", originalModel),
		zap.String("billing_model", billingModel),
		zap.String("upstream_model", upstreamModel),
		zap.Bool("stream", clientStream),
	)
	p.ObserveModel(upstreamModel)

	apiKey, targetURL, err := p.Target(ctx)
	if err != nil {
		return nil, err
	}
	p.Endpoint(RawChatEndpoint)
	resp, err := p.SendCC(ctx, targetURL, chatBody, clientStream, apiKey)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, upstreamMsg := p.ReadUpstreamError(resp)
		if foErr := p.FailoverHTTP(ctx, resp, respBody, upstreamMsg, upstreamModel); foErr != nil {
			return nil, foErr
		}
		return p.SourceErrorResponse(ctx, source, resp, chatBody, billingModel)
	}

	if profile.Grok && profile.GrokOAuth && !profile.Shadow {
		p.UpdateGrokUsage(ctx, upstreamModel, resp.Header, resp.StatusCode)
	}

	output := upstream.NewDeferredOutputContext(p.Sink())
	convOpts := p.ConvertedRawOptions(resp, source, originalModel, billingModel, upstreamModel, serviceTier, len(body))
	var observed *openai.CompatResponseResult
	var readErr error
	if clientStream {
		observed, readErr = openai.ReadRawChatConvertedStreaming(output, resp, exchange, convOpts, originalModel, upstreamModel, reasoningEffort, startTime)
	} else {
		observed, readErr = openai.ReadRawChatConvertedBuffered(output, resp, exchange, convOpts, originalModel, upstreamModel, reasoningEffort, startTime)
	}
	result := FromCompatResult(observed, billingModel)
	// Suppress partial usage settlement when upstream cyber-policy moderation
	// rejected the response mid-stream or inside an HTTP 200 error envelope.
	if p.CyberPolicy() {
		if readErr == nil {
			readErr = forwardcore.ErrCyberPolicyForwarded
		}
		return nil, readErr
	}
	if result != nil {
		result.UpstreamEndpoint = RawChatEndpoint
	}
	return result, readErr
}

// patchConvertedChatBody applies model, stream_options.include_usage,
// reasoning_effort, and field-deletion edits onto ordered raw JSON object
// members via jsontext. Non-object inputs return nil alongside the parse error.
//
// Existing stream_options members are preserved while merging include_usage,
// and empty ReconcileEffort results delete reasoning_effort rather than
// emitting an invalid empty string. Unmodeled Extra fields and 64-bit integer
// literals survive intact without map round-trips.
func patchConvertedChatBody(body []byte, patch convertedChatBodyPatch) ([]byte, error) {
	members, err := readConvertedObjectMembers(body)
	if err != nil {
		return nil, err
	}

	for _, delKey := range patch.DeleteFields {
		members = deleteConvertedMember(members, delKey)
	}

	if patch.Model != "" {
		rawModel, err := jsonv2.Marshal(patch.Model, jsontext.AllowInvalidUTF8(true))
		if err != nil {
			return nil, err
		}
		members = upsertConvertedMember(members, "model", rawModel)
	}

	if patch.EnsureIncludeUsage {
		members = upsertConvertedMember(members, "stream", jsontext.Value("true"))
		streamOptsValue := jsontext.Value(`{"include_usage":true}`)
		if rawExisting, ok := findConvertedMember(members, "stream_options"); ok {
			if subMembers, subErr := readConvertedObjectMembers(rawExisting); subErr == nil {
				subMembers = upsertConvertedMember(subMembers, "include_usage", jsontext.Value("true"))
				if rebuilt, encErr := encodeConvertedObjectMembers(subMembers); encErr == nil {
					streamOptsValue = rebuilt
				}
			}
		}
		members = upsertConvertedMember(members, "stream_options", streamOptsValue)
	}

	if patch.ReconcileEffort != nil {
		if rawEff, ok := findConvertedMember(members, "reasoning_effort"); ok {
			var curEffort string
			if jsonv2.Unmarshal(rawEff, &curEffort, jsontext.AllowInvalidUTF8(true)) == nil {
				nextEffort := patch.ReconcileEffort(curEffort)
				if nextEffort != curEffort {
					if nextEffort == "" {
						members = deleteConvertedMember(members, "reasoning_effort")
					} else {
						rawNext, err := jsonv2.Marshal(nextEffort, jsontext.AllowInvalidUTF8(true))
						if err != nil {
							return nil, err
						}
						members = upsertConvertedMember(members, "reasoning_effort", rawNext)
					}
				}
			}
		}
	}

	return encodeConvertedObjectMembers(members)
}
