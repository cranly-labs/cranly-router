package gemini

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	logger "github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	geminiwire "github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// response.go reads Gemini responses, collects streamed content, and writes
// native output while exposing shared usage and envelope helpers.
// Gemini output may arrive as ordinary JSON or one JSON payload per SSE data
// line, with an optional Code Assist response wrapper. Native output retains
// the original REST payload while observers receive raw payloads before output
// is emitted.
//
// Shared stream consumption validates complete frames, candidate termination,
// upstream errors, and read deadlines before output delivery. Buffered clients
// retain each candidate independently and combine text only when its remaining
// metadata matches. Sparse usage observations survive failed reads and final
// usage-only frames; callers still decide whether failed requests qualify for
// settlement. The reader closes its idempotent body and joins its worker before
// the executor releases the attempt.

// ResponseOptions supplies payload observers, response readers, header
// handling, and native error mapping.
type ResponseOptions struct {
	// Context controls read cancellation for this attempt. Direct codec callers
	// may leave it nil to use an uncancelled background context.
	Context context.Context
	Stream  upstream.GeminiStreamOptions // shared framing and idle limits

	// ObserveRaw receives upstream payload bytes synchronously before output.
	// Its callback leaves borrowed input unchanged and copies bytes retained after returning.
	ObserveRaw func([]byte) // raw payload observation callback

	GoogleError  func(int, string) error         // native response error mapping
	ReadBody     func(io.Reader) ([]byte, error) // caller-bounded response body reader
	WriteHeaders func(http.Header, http.Header)  // response header forwarding callback
	// DebugHeaders selects rate-header logging; HasHeaderFilter selects streaming
	// header forwarding.
	DebugHeaders, HasHeaderFilter bool
	ObserveImages                 func([]byte)        // inline image observation callback
	ReverseTools                  func([]byte) []byte // upstream tool-name reversal callback
}

// ResponseAdapter reads Gemini payloads and delivers native or converted client
// output through its options.
type ResponseAdapter struct {
	Options ResponseOptions // conversion and output callbacks
}

// NativeStreamResult holds normalized usage collected from Gemini stream payloads.
type NativeStreamResult struct {
	Usage *upstream.TokenUsage // observed normalized token usage
}

// CompleteNative selects native streaming, buffered SSE collection, or
// buffered payload forwarding from the two stream flags. Callers pass the
// OAuth wrapper choice and close resp.Body; a missing usage result becomes
// zero counters.
//
// Collected SSE is reencoded as one native JSON response and observed for
// images before output. Direct buffered forwarding retains upstream status,
// whereas collection emits a successful combined response. Stream adapters
// supply parsed usage for the execution owner.
// Read and conversion failures return before a completed result is supplied.
func (s *ResponseAdapter) CompleteNative(c *upstream.OutputContext, resp *http.Response, startTime time.Time, stream, useUpstreamStream, isOAuth bool) (*upstream.TokenUsage, error) {
	var usage *upstream.TokenUsage

	if stream {
		streamRes, err := s.HandleNativeStreamingResponse(c, resp, startTime, isOAuth)
		if err != nil {
			return streamRes.Usage, err
		}
		usage = streamRes.Usage
	} else {
		if useUpstreamStream {
			collected, usageObj, err := s.collectSSE(resp, isOAuth)
			if err != nil {
				return usageObj, err
			}
			// Collection supplies ordinary JSON-decoded maps, so serialization errors
			// are discarded on this combined native path; raw observers have already
			// recorded each participating payload.
			b, _ := json.Marshal(collected)
			s.Options.ObserveImages(b)
			c.NextEvent(geminiwire.ObservePayload(b).Semantic, true)
			c.Data(http.StatusOK, "application/json", b)
			usage = usageObj
		} else {
			usageResp, err := s.HandleNativeNonStreamingResponse(c, resp, isOAuth)
			if err != nil {
				return nil, err
			}
			usage = usageResp
		}
	}

	if usage == nil {
		usage = &upstream.TokenUsage{}
	}

	return usage, nil
}

// UnwrapIfNeeded returns an extracted response only for OAuth payloads and
// otherwise returns raw. Paths returning raw share its byte slice; an
// extracted object or array uses the bytes produced by UnwrapGeminiResponse.
func UnwrapIfNeeded(isOAuth bool, raw []byte) []byte {
	if !isOAuth {
		return raw
	}
	inner, err := UnwrapGeminiResponse(raw)
	if err != nil {
		return raw
	}
	return inner
}

// collectSSE delegates candidate-preserving collection for a buffered client response.
// Shared consumption enforces cancellation, framing, completion, and idle limits.
// Partial content and accumulated usage remain available beside failures for diagnostics.
func (s *ResponseAdapter) collectSSE(resp *http.Response, isOAuth bool) (map[string]any, *upstream.TokenUsage, error) {
	options := s.Options.Stream
	if isOAuth {
		options.Unwrap = UnwrapGeminiResponse
	}
	collected, observed, err := upstream.CollectGeminiStream(s.Options.Context, resp, options, s.observeRaw)
	usage := ExtractGeminiUsage(observed.UsageJSON)
	if usage == nil {
		usage = &upstream.TokenUsage{}
	}
	return collected, usage, err
}

// readStream applies this platform's envelope decoder to shared Gemini consumption.
// OAuth and Code Assist readers unwrap their response field before validation;
// native REST readers retain their JSON object. The shared consumer owns body cleanup.
func (s *ResponseAdapter) readStream(resp *http.Response, wrapped bool, consume func(upstream.GeminiStreamEvent) error) (upstream.GeminiStreamResult, error) {
	options := s.Options.Stream
	if wrapped {
		options.Unwrap = UnwrapGeminiResponse
	}
	return upstream.ReadGeminiStream(s.Options.Context, resp, options, consume)
}

// HandleNativeNonStreamingResponse reads through the caller body limiter,
// optionally unwraps OAuth data, and forwards status, content type, and
// filtered headers. It observes raw/image data before output; absent usage
// returns zero counters, and callers inspect output errors separately.
func (s *ResponseAdapter) HandleNativeNonStreamingResponse(c *upstream.OutputContext, resp *http.Response, isOAuth bool) (*upstream.TokenUsage, error) {
	if s.Options.DebugHeaders {
		logger.LegacyPrintf("service.gemini_messages_compat", "[GeminiAPI] ========== Response Headers ==========")
		for key, values := range resp.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-ratelimit") {
				logger.LegacyPrintf("service.gemini_messages_compat", "[GeminiAPI] %s: %v", key, values)
			}
		}
		logger.LegacyPrintf("service.gemini_messages_compat", "[GeminiAPI] ========================================")
	}

	respBody, err := s.Options.ReadBody(resp.Body)
	if err != nil {
		return nil, err
	}

	if isOAuth {
		unwrappedBody, uwErr := UnwrapGeminiResponse(respBody)
		if uwErr == nil {
			respBody = unwrappedBody
		}
	}
	s.observeRaw(respBody)
	s.Options.ObserveImages(respBody)

	s.Options.WriteHeaders(c.Writer.Header(), resp.Header)

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	c.NextEvent(geminiwire.ObservePayload(respBody).Semantic, true)
	c.Data(resp.StatusCode, contentType, respBody)

	if u := ExtractGeminiUsage(respBody); u != nil {
		return u, nil
	}
	return &upstream.TokenUsage{}, nil
}

// HandleNativeStreamingResponse delivers complete Gemini JSON events and partial usage.
// Upstream comments and DONE never reach the native SDK. The shared reader owns
// response-body cleanup, and all output failures propagate to the execution owner.
//
// Candidate completion is attached to the exact normalized frame that establishes
// it, allowing the HTTP boundary to stop heartbeats atomically with delivery.
// Provider errors and missing completion return failures instead of manufacturing
// a successful EOF; already observed usage remains available on those returns.
func (s *ResponseAdapter) HandleNativeStreamingResponse(c *upstream.OutputContext, resp *http.Response, startTime time.Time, isOAuth bool) (*NativeStreamResult, error) {
	if s.Options.DebugHeaders {
		for key, values := range resp.Header {
			if strings.HasPrefix(strings.ToLower(key), "x-ratelimit") {
				logger.LegacyPrintf("service.gemini_messages_compat", "[GeminiAPI] %s: %v", key, values)
			}
		}
	}
	if s.Options.HasHeaderFilter {
		s.Options.WriteHeaders(c.Writer.Header(), resp.Header)
	}
	c.Status(resp.StatusCode)
	c.Header("Content-Type", "text/event-stream; charset=utf-8")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	result := &NativeStreamResult{Usage: &upstream.TokenUsage{}}
	observed, err := s.readStream(resp, isOAuth, func(event upstream.GeminiStreamEvent) error {
		if len(event.Data) == 0 {
			if event.Terminal {
				c.Complete()
			}
			return nil
		}
		if usage := ExtractGeminiUsage(event.Data); usage != nil {
			result.Usage = usage
		}
		s.observeRaw(event.Data)
		s.Options.ObserveImages(event.Data)
		if c.Err() != nil {
			return nil
		}
		c.NextEvent(geminiwire.ObservePayload(event.Data).Semantic, event.Terminal)
		if _, err := fmt.Fprintf(c.Writer, "data: %s\n\n", event.Data); err != nil {
			return nil
		}
		c.Writer.Flush()
		return nil
	})
	if usage := ExtractGeminiUsage(observed.UsageJSON); usage != nil {
		result.Usage = usage
	}
	if c.Err() != nil {
		return result, nil
	}
	return result, err
}

// UnwrapGeminiResponse extracts the raw object or array in response as new
// bytes, otherwise returning the supplied slice with nil error. Field
// extraction preserves nested JSON spelling and does not validate the complete
// outer document.
func UnwrapGeminiResponse(raw []byte) ([]byte, error) {
	result := gjson.GetBytes(raw, "response")
	if result.Exists() && result.Type == gjson.JSON {
		return []byte(result.Raw), nil
	}
	return raw, nil
}

// ExtractGeminiUsage copies native Gemini token buckets into upstream usage,
// returning nil for absent metadata. Explicit null or zero metadata yields
// zero counters; cached input is separated from prompt input and thinking
// joins candidate output.
func ExtractGeminiUsage(data []byte) *upstream.TokenUsage {
	return LegacyNativeGeminiUsage(geminiwire.NativeExtractGeminiUsage(data))
}

// LegacyNativeGeminiUsage copies native Gemini token counters into a fresh
// upstream usage value. Nil input returns nil, and the four integer buckets
// retain their supplied accounting values.
func LegacyNativeGeminiUsage(usage *geminiwire.NativeGeminiUsage) *upstream.TokenUsage {
	return geminiwire.NativeUsageProjection(usage)
}

// observeRaw synchronously invokes the optional raw observer with the supplied
// payload bytes.
func (s *ResponseAdapter) observeRaw(payload []byte) {
	if s.Options.ObserveRaw != nil {
		s.Options.ObserveRaw(payload)
	}
}
