package openaiforward

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// anthropic_flows.go forwards native Messages requests through compatible
// Anthropic endpoints and defines client-protocol tags shared by compatible
// Anthropic adapters. Direct Messages retains the client stream choice and
// always runs PrepareNativeMessagesReplay ahead of model-attribute preparation
// so active tool loops unpack matching trc1 continuity envelopes or downgrade
// incompatible thinking blocks before effort and cache inspection.
//
// Billing and vendor models stay separate from the client response identity.
// Each attempt closes its upstream response body and evaluates provider
// failover before client error presentation; native output readers retain
// observed usage and the vendor-declared model on their result.

// NativeAnthropicKind values select the client error writer and output conversion
// used by adapters sharing one selected native Anthropic provider attempt.
const (
	NativeMessages  NativeAnthropicKind = iota // direct Messages output
	NativeResponses                            // Responses conversion output
	NativeChat                                 // Chat conversion output
)

// NativeAnthropicKind identifies the client protocol for a native Anthropic attempt.
type NativeAnthropicKind uint8

// ForwardNativeMessages executes a direct Messages attempt against the selected
// Anthropic-compatible endpoint. Nonblank model and protocol credentials are
// required; PrepareNativeMessagesReplay carrier rewriting runs ahead of
// model-attribute preparation, empty-text stripping, search-history cleanup,
// and 1h cache TTL injection, and the client stream flag selects native reading.
//
// The runner closes the upstream response body after output or error handling.
// Output effort observed after model preparation is recorded on
// RequestedReasoningEffort separately from its thinking-derived display
// fallback, and partial reader observations can accompany a stream error.
func ForwardNativeMessages(ctx context.Context, body []byte, defaultMappedModel string, p NativeAnthropicPorts) (*Result, error) {
	profile := p.Profile()
	startTime := time.Now()

	originalModel := strings.TrimSpace(gjson.GetBytes(body, "model").String())
	if originalModel == "" {
		p.Error(http.StatusBadRequest, "invalid_request_error", "model is required")
		return nil, fmt.Errorf("missing model in request")
	}
	clientStream := gjson.GetBytes(body, "stream").Bool()

	// Retain the caller model for output while patching the mapped vendor model
	// into Messages bytes; native preparation then evaluates controls for that
	// selected upstream identity.
	billingModel := p.BillingModel(originalModel, defaultMappedModel)
	upstreamModel := p.UpstreamModel(billingModel)
	if upstreamModel != originalModel {
		rewritten, err := sjson.SetBytes(body, "model", upstreamModel)
		if err != nil {
			return nil, fmt.Errorf("rewrite model: %w", err)
		}
		body = rewritten
	}
	// Unpack matching trc1 signatures or retain passback unsigned thinking
	// ahead of PrepareModel and ThinkingFallback so downgraded active tool
	// loops update top-level thinking before attribute and effort inspection.
	rewritten, err := p.PrepareNativeMessagesReplay(ctx, upstreamModel, body)
	if err != nil {
		return nil, err
	}
	body = rewritten

	body, err = p.PrepareModel(ctx, body, upstreamModel)
	if err != nil {
		return nil, err
	}

	// Capture output_config.effort after model preparation. When no effort is
	// captured, enabled thinking can select high for passback families that
	// lack native effort; the result keeps that display default separate from
	// the captured request control.
	requestedReasoningEffort := protocol.NormalizeClaudeOutputEffort(gjson.GetBytes(body, "output_config.effort").String())
	reasoningEffort := p.ThinkingFallback(
		requestedReasoningEffort,
		body,
		billingModel,
	)

	// Apply the Messages passthrough filters: empty text and unsupported search history
	// can cause compatible upstreams to reject the request.
	body = p.StripEmpty(body)
	body = p.FilterSearch(body, upstreamModel)
	body = p.CacheTTL(ctx, body, upstreamModel)

	p.Log("[Messages passthrough] provider=%d(%s) platform=%s model=%s upstream=%s stream=%v",
		profile.ID, profile.Name, profile.Platform, originalModel, upstreamModel, clientStream)

	apiKey := strings.TrimSpace(p.ProtocolAPIKey())
	if apiKey == "" {
		return nil, fmt.Errorf("provider %d missing api_key", profile.ID)
	}
	targetURL, err := p.TargetURL()
	if err != nil {
		return nil, err
	}

	p.PrepareTransport()

	upstreamCtx, releaseUpstreamCtx := p.StreamContext(ctx, clientStream)
	upstreamReq, err := p.BuildNative(upstreamCtx, body, apiKey, targetURL)
	releaseUpstreamCtx()
	if err != nil {
		return nil, err
	}

	resp, err := p.SendNative(upstreamReq)
	if err != nil {
		return nil, p.TransportErrorNative(ctx, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		respBody, upstreamMsg := p.ReadUpstreamError(resp)
		if foErr := p.FailoverHTTP(ctx, resp, respBody, upstreamMsg, upstreamModel); foErr != nil {
			return nil, foErr
		}
		// Use the shared Anthropic error formatter, passthrough rules, Ops observations,
		// and cyber-policy detection when failover is not requested.
		return p.ErrorResponse(resp, billingModel)
	}

	if clientStream {
		result, err := NativeAnthropicStreaming(ctx, resp, upstream.NewDeferredOutputContext(p.Sink()), p.DirectOptions(), originalModel, billingModel, upstreamModel, reasoningEffort, startTime)
		// Retain post-preparation output_config.effort separately from thinking-derived
		// display fallback so billing attribution sees only explicit request effort.
		if result != nil {
			result.RequestedReasoningEffort = requestedReasoningEffort
		}
		return result, err
	}
	result, err := NativeAnthropicBuffered(ctx, resp, upstream.NewDeferredOutputContext(p.Sink()), p.DirectOptions(), originalModel, billingModel, upstreamModel, reasoningEffort, startTime)
	if result != nil {
		result.RequestedReasoningEffort = requestedReasoningEffort
	}
	return result, err
}
