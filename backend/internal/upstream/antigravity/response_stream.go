package antigravity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	protocolgemini "github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// response_stream.go reads Antigravity v1internal SSE for native Gemini
// streaming or buffered collection and exposes shared envelope unwrapping and
// request image-size helpers.
//
// Antigravity generation arrives as Gemini data inside v1internal SSE frames,
// even for buffered client requests. Native and converted readers share
// validated frame reading and the bounded post-completion usage tail, closing
// the response body and joining the scanner worker before returning so sparse
// tail fields and client disconnects never erase observed cache counts.

// ResponseOptions supplies stream limits, raw observation, and tool-name
// reversal callbacks.
type ResponseOptions struct {
	// Context supplies the selected attempt cancellation. Nil permits
	// standalone codec use with a background context in the shared reader.
	Context    context.Context
	Candidates int // expected native candidate count

	// ObserveRaw receives normalized payloads synchronously before client
	// conversion/output; Executor chains it with attempt model and usage
	// observation. Callbacks leave supplied bytes unchanged and copy retained
	// mutable data before the owner continues synchronous processing.
	ObserveRaw func([]byte)
	// MaxLineSize bounds complete SSE frame bytes, with nonpositive selecting the
	// default. StreamDataIntervalTimeout supplies upstream idle-timeout seconds;
	// nonpositive values disable idle timing while retaining the bounded usage tail.
	MaxLineSize, StreamDataIntervalTimeout int
	// ReverseTools restores provider-mapped tool names in converted serialized
	// output when non-nil.
	ReverseTools func([]byte) []byte
}

type (
	// ResponseAdapter uses Options to adapt one upstream stream and leaves final
	// response-body closing to the exchange owner.
	ResponseAdapter struct{ Options ResponseOptions } // stream adaptation callbacks
	// StreamResult carries normalized adapter usage and observed client-write
	// observations for attempt completion.
	StreamResult struct {
		// Usage references collected adapter observations; streaming paths can
		// return this shared state after disconnect, while Executor separately
		// observes raw usage.
		Usage *upstream.TokenUsage

		// ClientDisconnect reports a failed client write or a handled
		// cancelled/read-failure path according to the selected response adapter.
		ClientDisconnect bool
		// ProtocolConversion carries bounded protocol transformation diagnostics
		// when this stream result came from an IR-converted exchange.
		ProtocolConversion *telemetry.ProtocolConversion
	}
)

// HandleGeminiStreamingResponse unwraps and normalizes native Gemini SSE frames.
// Shared consumption validates completion and provider errors before delivery.
// Partial usage accompanies failures, and the reader owns idempotent body cleanup.
//
// A complete frame carries its whole-response terminal flag into the output sink,
// so heartbeat shutdown does not depend on inspecting a bounded JSON prefix.
// Transport-only DONE becomes a notification without a native SDK data event.
// Payload observers retain upstream metadata independently of downstream formatting.
func (s *ResponseAdapter) HandleGeminiStreamingResponse(c *upstream.OutputContext, resp *http.Response, startTime time.Time) (*StreamResult, error) {
	c.Status(resp.StatusCode)
	c.Header("Content-Type", "text/event-stream; charset=utf-8")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	result := &StreamResult{Usage: &upstream.TokenUsage{}}
	observed, err := s.readGeminiStream(resp, s.UnwrapV1InternalResponse, func(event upstream.GeminiStreamEvent) error {
		if len(event.Data) == 0 {
			if event.Terminal {
				c.Complete()
			}
			return nil
		}
		s.observeRaw(event.Data)
		if usage := protocolgemini.NativeUsageProjection(protocolgemini.NativeExtractGeminiUsage(event.Data)); usage != nil {
			result.Usage = usage
		}
		if c.Err() != nil {
			return nil
		}
		c.NextEvent(protocolgemini.ObservePayload(event.Data).Semantic, event.Terminal)
		if _, err := fmt.Fprintf(c.Writer, "data: %s\n\n", event.Data); err != nil {
			return nil
		}
		c.Writer.Flush()
		return nil
	})
	if usage := protocolgemini.NativeUsageProjection(protocolgemini.NativeExtractGeminiUsage(observed.UsageJSON)); usage != nil {
		result.Usage = usage
	}
	result.ClientDisconnect = c.Err() != nil || errors.Is(err, context.Canceled) || (s.Options.Context != nil && s.Options.Context.Err() != nil)
	if result.ClientDisconnect {
		return result, nil
	}
	return result, err
}

// readGeminiStream binds the selected envelope extraction to shared validation.
// Native output keeps inner bytes, while converted output retains wrapper identity.
// Both use the same limits, completion expectation, and trailing-usage policy.
func (s *ResponseAdapter) readGeminiStream(resp *http.Response, unwrap func([]byte) ([]byte, error), consume func(upstream.GeminiStreamEvent) error) (upstream.GeminiStreamResult, error) {
	return upstream.ReadGeminiStream(s.Options.Context, resp, upstream.GeminiStreamOptions{
		IdleTimeout:   time.Duration(s.Options.StreamDataIntervalTimeout) * time.Second,
		MaxFrameBytes: s.Options.MaxLineSize, Candidates: s.Options.Candidates,
		Unwrap: unwrap,
	}, consume)
}

// HandleGeminiStreamToNonStreaming collects a validated native Gemini response.
// Candidate indices retain their own parts and metadata through usage-only tails.
// Failed consumption returns partial usage without emitting successful JSON; shared
// consumption closes the response body and joins its worker before returning.
func (s *ResponseAdapter) HandleGeminiStreamToNonStreaming(c *upstream.OutputContext, resp *http.Response, startTime time.Time) (*StreamResult, error) {
	response, observed, err := upstream.CollectGeminiStream(s.Options.Context, resp, upstream.GeminiStreamOptions{
		IdleTimeout:   time.Duration(s.Options.StreamDataIntervalTimeout) * time.Second,
		MaxFrameBytes: s.Options.MaxLineSize, Candidates: s.Options.Candidates, Unwrap: s.UnwrapV1InternalResponse,
	}, s.observeRaw)
	usage := protocolgemini.NativeUsageProjection(protocolgemini.NativeExtractGeminiUsage(observed.UsageJSON))
	if usage == nil {
		usage = &upstream.TokenUsage{}
	}
	result := &StreamResult{Usage: usage}
	if err != nil {
		return result, err
	}
	body, err := json.Marshal(response)
	if err != nil {
		return result, err
	}
	c.Data(http.StatusOK, "application/json", body)
	return result, c.Err()
}

// ExtractImageInputSize returns trimmed imageSize from decoded Gemini
// generation image settings. Invalid JSON, absent generation/image settings,
// and blank size return empty; the helper follows the shared Gemini wire field
// spelling.
func (s *ResponseAdapter) ExtractImageInputSize(body []byte) string {
	// A failed complete request decode supplies no selected image size. Return
	// empty text so callers retain their ordinary image-size default or policy.
	var req protocolgemini.GeminiRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return ""
	}

	if req.GenerationConfig != nil && req.GenerationConfig.ImageConfig != nil {
		return strings.TrimSpace(req.GenerationConfig.ImageConfig.ImageSize)
	}

	return ""
}

// observeRaw forwards data synchronously to the optional callback without
// copying bytes. A missing callback has no effect; retaining callbacks follow
// supplied storage ownership and synchronize their own shared state. Callbacks
// leave the supplied bytes unchanged before subsequent parsing and output.
func (s *ResponseAdapter) observeRaw(data []byte) {
	if s.Options.ObserveRaw != nil {
		s.Options.ObserveRaw(data)
	}
}

// UnwrapV1InternalResponse extracts raw JSON for any existing response field,
// including scalar or null values. Absent fields return supplied bytes by
// reference; this helper performs no full JSON validation and returns no
// error. Callers keep the supplied body stable while reading a borrowed
// return value; extracted response bytes have their own allocation.
func (s *ResponseAdapter) UnwrapV1InternalResponse(body []byte) ([]byte, error) {
	result := gjson.GetBytes(body, "response")
	if result.Exists() {
		return []byte(result.Raw), nil
	}
	return body, nil
}
