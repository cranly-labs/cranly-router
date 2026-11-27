package antigravity

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// response_converted.go reads streaming SSE and buffered Antigravity v1internal
// responses through convert.Exchange for Anthropic Messages, OpenAI Responses,
// and OpenAI Chat Completions while preserving ReadGeminiStream validation,
// wirejson-guarded billing usage extraction, pre-conversion model observation,
// and pre-semantic failover staging. OutputContext.NextEvent marks every
// non-empty payload write as CommitForRetry, so flushing a leading non-semantic
// frame such as message_start, response.created, or an opening assistant role
// chunk would prematurely close outer provider failover before real model
// output arrives.
//
// ReadConvertedStreaming buffers non-semantic client frames locally until the
// first Semantic==true frame or a validated completion terminal arrives, calls
// OutputContext.Complete upon observing finishReason so downstream heartbeats
// stop during the five-second sparse usage tail window, and invokes
// Stream.Close(nil) only after ReadGeminiStream returns so final merged usage
// and response_end frames carry trailing counters. Early-committed HTTP 200 SSE
// responses receive untimed client-protocol terminal SSE errors, started
// streams close open blocks via Stream.Close(cause) and prefer
// WriteRawTerminalSSE for continuous sequence numbers and response IDs, and
// disconnected clients stop downstream writes while continuing to drain
// upstream usage for billing.

var (
	// ErrAmbiguousUsagePayload marks upstream response frames rejected by wirejson
	// duplicate-key or case-colliding usageMetadata validation so failure policy
	// treats them as non-retryable parse errors rather than empty streams.
	ErrAmbiguousUsagePayload = errors.New("ambiguous upstream usage payload")

	// jsonDecoderPool reuses jsontext.Decoder instances across SSE frame scans so
	// v1internal unwrapping validates nested duplicate keys without per-frame
	// decoder buffer allocations.
	jsonDecoderPool = sync.Pool{
		New: func() any {
			p := &pooledJSONDecoder{}
			p.dec = jsontext.NewDecoder(&p.reader, jsontext.AllowInvalidUTF8(true))
			return p
		},
	}
)

// ConvertedStreamFailure carries either a pre-semantic failover or fallback
// error or the resolved client error metadata from single-pass stream failure
// classification.
type ConvertedStreamFailure struct {
	// Failover takes precedence over Status, Type, Code, and Message when
	// non-nil so pre-semantic retryable or typed fallback errors return directly
	// to the outer loop without touching the output sink.
	Failover error  // pre-semantic failover or fallback error
	Status   int    // intended HTTP status for uncommitted JSON or Ops diagnostics
	Type     string // client-protocol error category
	Code     string // optional machine-readable error code
	Message  string // client-safe failure message
	Err      error  // optional companion error joined onto the stream failure return
}

// ConvertedOptions binds a convert.Exchange, single-pass failure policy, and
// untimed SSE error writers for ReadConvertedStreaming and ReadConvertedBuffered.
type ConvertedOptions struct {
	Exchange         *convert.Exchange // per-attempt IR request and response converter
	RequestID        string            // upstream request correlation ID for headers and logs
	DefaultErrorType string            // fallback error category when classification leaves Type blank

	// CommittedSSE reports whether downstream HTTP 200 text/event-stream headers
	// have already been committed, including by waiting keepalive heartbeats. Nil
	// falls back to inspecting c.Writer.Written directly.
	CommittedSSE func() bool
	// WriteError writes an uncommitted HTTP JSON error or promotes to an untimed
	// terminal SSE error frame when CommittedSSE is already true. Nil falls back
	// to writeConvertedAntigravityUnstartedError's standalone JSON envelope.
	WriteError func(status int, errType, code, message string)
	// WriteLocalTerminalError writes an untimed terminal SSE error frame in the
	// inbound client protocol, emitting response.failed on Responses streams. Nil
	// falls back to WriteError when stream output has not started.
	WriteLocalTerminalError func(status int, errType, code, message string)
	// WriteRawTerminalSSE delivers pre-encoded terminal SSE frame bytes from
	// Stream.Close(cause) through the gateway's untimed terminal barrier and
	// reports whether delivery ran. Returning false falls back to c.Writer.
	WriteRawTerminalSSE func(status int, errType, code, message string, data []byte) bool
	// ClassifyStreamFailure evaluates provider error policy at most once for an
	// upstream stream or inline error and returns either Failover or client error
	// fields. Nil uses streamErr's status and sanitized message directly.
	ClassifyStreamFailure func(ctx context.Context, err *upstream.GeminiStreamError, clientOutputStarted bool) ConvertedStreamFailure
}

// pooledJSONDecoder pairs a resettable bytes.Reader with a reusable
// jsontext.Decoder for pooled single-pass object scanning.
type pooledJSONDecoder struct {
	reader bytes.Reader      // resettable byte reader backing dec
	dec    *jsontext.Decoder // reusable streaming JSON token scanner
}

// rawJSONObjectMember preserves exact JSON member order and raw value bytes
// during lossless jsontext v1internal unwrapping and sparse usage merging.
type rawJSONObjectMember struct {
	name  string         // decoded object member key
	value jsontext.Value // validated raw JSON member slice
}

// antigravityRawUsageAccumulator merges present non-null usageMetadata fields
// across unwrapped SSE frames using jsontext after wirejson validation succeeds.
type antigravityRawUsageAccumulator struct {
	// fields retains ordered non-null usageMetadata members observed across
	// sparse frames so output-only tail updates never erase earlier prompt or
	// cachedContentTokenCount counters when read failures return partial usage.
	fields []rawJSONObjectMember // accumulated non-null usageMetadata members
}

// ReadConvertedBuffered consumes an Antigravity v1internal SSE stream through
// ReadGeminiStream, folds unwrapped Gemini frames via conv.Exchange.Buffer, and
// writes one buffered client JSON response. Callers retain ownership of closing
// resp.Body after this method returns.
//
// ReadGeminiStream collects up to five seconds of sparse trailing usageMetadata
// frames after finishReason before Buffer.Close(nil) serializes the accumulated
// client response, sealing trc1:antigravity_claude or trc1:gemini carriers and
// storing tool-call bindings into CallMemory. Billing usage passes wirejson
// uniqueness validation before native bucket projection, while stream or decode
// failures route once through handleBufferedStreamFailure alongside partial usage.
func (s *ResponseAdapter) ReadConvertedBuffered(
	c *upstream.OutputContext,
	resp *http.Response,
	conv ConvertedOptions,
) (*StreamResult, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("antigravity upstream response body is nil")
	}
	if conv.Exchange == nil {
		return nil, errors.New("antigravity conversion exchange is nil")
	}
	ctx := s.Options.Context
	if ctx == nil {
		ctx = context.Background()
	}

	buf := conv.Exchange.Buffer()
	sawSemantic := false

	observed, readErr := s.readGeminiStream(resp, unwrapV1InternalConvertedPayload, func(event upstream.GeminiStreamEvent) error {
		if len(event.Data) == 0 {
			return nil
		}
		s.observeRaw(event.Data)
		sawSemantic = sawSemantic || gemini.ObservePayload(event.Data).Semantic
		payload := event.Data
		if s.Options.ReverseTools != nil {
			payload = s.Options.ReverseTools(payload)
		}
		if wErr := buf.Write(sse.Frame{Data: payload}); wErr != nil {
			return newAntigravityDecodeStreamError("Failed to parse upstream stream", wErr)
		}
		return nil
	})

	billingUsage := extractAntigravityBillingUsage(observed.UsageJSON)
	if billingUsage == nil {
		billingUsage = &upstream.TokenUsage{}
	}
	result := &StreamResult{Usage: billingUsage}
	if readErr != nil {
		return result, s.handleBufferedStreamFailure(ctx, c, conv, readErr)
	}

	clientJSON, closeErr := buf.Close(nil)
	if closeErr != nil {
		return result, s.handleBufferedStreamFailure(
			ctx,
			c,
			conv,
			newAntigravityDecodeStreamError("Failed to convert upstream response", closeErr),
		)
	}
	logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)

	writeBufferedResponseHeaders(c, conv.RequestID)
	c.NextEvent(sawSemantic, true)
	c.Data(http.StatusOK, "application/json; charset=utf-8", clientJSON)
	return result, nil
}

// ReadConvertedStreaming consumes an Antigravity v1internal SSE body through
// ReadGeminiStream and conv.Exchange.Stream, staging non-semantic leading
// frames locally until semantic content or a validated completion terminal
// arrives so pre-semantic failures remain eligible for outer provider failover.
//
// Observing finishReason invokes c.Complete to stop downstream keepalive
// heartbeats while ReadGeminiStream continues collecting up to five seconds of
// sparse usageMetadata tail frames, after which Stream.Close(nil) emits the
// final merged client usage and completion frames. Disconnected clients stop
// further downstream writes while continuing to drain upstream usage for
// billing, pre-committed HTTP 200 SSE streams receive untimed terminal error
// frames, and started streams close open blocks via Stream.Close(cause) before
// delivering their terminal error frame through WriteRawTerminalSSE.
func (s *ResponseAdapter) ReadConvertedStreaming(
	c *upstream.OutputContext,
	resp *http.Response,
	conv ConvertedOptions,
) (*StreamResult, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("antigravity upstream response body is nil")
	}
	if conv.Exchange == nil {
		return nil, errors.New("antigravity conversion exchange is nil")
	}
	ctx := s.Options.Context
	if ctx == nil {
		ctx = context.Background()
	}

	// Guard SSE header emission with sync.Once so staged preamble flushes, empty
	// completions, and mid-stream error closers initialize HTTP 200 headers once.
	var headersOnce sync.Once
	writeStreamHeaders := func() {
		headersOnce.Do(func() {
			if conv.RequestID != "" {
				c.Header("x-request-id", conv.RequestID)
			}
			c.Header("Content-Type", "text/event-stream")
			c.Header("Cache-Control", "no-cache")
			c.Header("Connection", "keep-alive")
			c.Header("X-Accel-Buffering", "no")
			c.Status(http.StatusOK)
		})
	}

	stream := conv.Exchange.Stream()
	var stagedFrames []sse.Frame
	stagedHasSemantic := false
	clientOutputStarted := false
	clientDisconnected := false
	completedKeepalive := false

	isSSECommitted := func() bool {
		return isConvertedSSECommitted(c, conv)
	}

	writeSingleClientFrame := func(f sse.Frame) bool {
		if clientDisconnected {
			return false
		}
		writeStreamHeaders()
		raw := sse.Append(nil, f)
		c.NextEvent(f.Semantic, f.Terminal)
		if _, err := c.Writer.Write(raw); err != nil {
			clientDisconnected = true
			logging.L().Info("antigravity converted stream: client disconnected, continuing to drain upstream for billing",
				zap.String("request_id", conv.RequestID),
			)
			return false
		}
		clientOutputStarted = true
		return true
	}

	flushStagedFrames := func() bool {
		if clientDisconnected || len(stagedFrames) == 0 {
			return !clientDisconnected
		}
		for _, f := range stagedFrames {
			if !writeSingleClientFrame(f) {
				stagedFrames = stagedFrames[:0]
				stagedHasSemantic = false
				return false
			}
		}
		stagedFrames = stagedFrames[:0]
		stagedHasSemantic = false
		return true
	}

	stageOrWriteClientFrames := func(frames []sse.Frame) {
		if len(frames) == 0 || clientDisconnected {
			return
		}
		wroteDirectly := false
		for _, f := range frames {
			if !clientOutputStarted {
				stagedFrames = append(stagedFrames, f)
				if f.Semantic {
					stagedHasSemantic = true
				}
				if stagedHasSemantic || f.Terminal {
					if flushStagedFrames() {
						wroteDirectly = true
					}
				}
				continue
			}
			if writeSingleClientFrame(f) {
				wroteDirectly = true
			}
		}
		if wroteDirectly && !clientDisconnected {
			c.Writer.Flush()
		}
	}

	observed, readErr := s.readGeminiStream(resp, unwrapV1InternalConvertedPayload, func(event upstream.GeminiStreamEvent) error {
		if len(event.Data) > 0 {
			s.observeRaw(event.Data)
			// Keep draining ReadGeminiStream after client disconnect so billing usage
			// and model observations complete without writing further client frames.
			if !clientDisconnected && c.Err() == nil {
				payload := event.Data
				if s.Options.ReverseTools != nil {
					payload = s.Options.ReverseTools(payload)
				}
				frames, wErr := stream.Write(sse.Frame{Data: payload})
				if wErr != nil {
					return newAntigravityDecodeStreamError("Failed to parse upstream stream", wErr)
				}
				stageOrWriteClientFrames(frames)
			}
		}
		if event.Terminal && !completedKeepalive && !clientDisconnected && c.Err() == nil {
			// Commit SSE headers or flush staged preamble frames on a valid empty
			// completion before c.Complete() flushes, stopping downstream keepalives
			// cleanly while ReadGeminiStream collects trailing usage for up to 5s.
			if !clientOutputStarted && !isSSECommitted() {
				if len(stagedFrames) > 0 {
					_ = flushStagedFrames()
				} else {
					writeStreamHeaders()
					c.Writer.WriteHeaderNow()
				}
			}
			c.Complete()
			completedKeepalive = true
		}
		if c.Err() != nil {
			clientDisconnected = true
		}
		return nil
	})

	billingUsage := extractAntigravityBillingUsage(observed.UsageJSON)
	if billingUsage == nil {
		billingUsage = &upstream.TokenUsage{}
	}

	if clientDisconnected || c.Err() != nil {
		// Discard terminal error frames on a disconnected client while still closing
		// the converter stream so conversion losses finalize cleanly.
		_, _ = stream.Close(errors.New("client disconnected"))
		logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
		return &StreamResult{Usage: billingUsage, ClientDisconnect: true}, nil
	}

	if readErr != nil {
		if ctx.Err() != nil || errors.Is(readErr, context.Canceled) {
			if !clientDisconnected && clientOutputStarted {
				// Close open client blocks and deliver an untimed terminal SSE error when
				// execution context cancellation or deadline interrupts an active stream.
				s.emitStartedConvertedAbortFrames(c, conv, stream, &ir.StreamError{
					Type:    defaultConvertedAntigravityErrorType(conv.DefaultErrorType, http.StatusBadGateway),
					Code:    "upstream_stream_error",
					Message: "Stream interrupted before completion",
					Status:  http.StatusBadGateway,
				}, writeStreamHeaders)
			} else {
				// Finalize unstarted converter state on caller cancellation or execution
				// timeout, emitting one untimed terminal SSE error when HTTP 200 SSE
				// headers were already committed on an active client connection.
				_, _ = stream.Close(readErr)
				logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
				if !clientDisconnected && isSSECommitted() && conv.WriteLocalTerminalError != nil {
					conv.WriteLocalTerminalError(
						http.StatusBadGateway,
						defaultConvertedAntigravityErrorType(conv.DefaultErrorType, http.StatusBadGateway),
						"upstream_stream_error",
						"Stream interrupted before completion",
					)
				}
			}
			return &StreamResult{Usage: billingUsage}, readErr
		}
		streamErr := coerceAntigravityStreamError(readErr)
		return &StreamResult{Usage: billingUsage}, s.deliverConvertedStreamFailure(
			ctx,
			c,
			conv,
			stream,
			streamErr,
			readErr,
			clientOutputStarted,
			isSSECommitted,
			writeStreamHeaders,
		)
	}

	tailFrames, closeErr := stream.Close(nil)
	if closeErr != nil {
		synthErr := newAntigravityDecodeStreamError("Failed to finalize upstream stream", closeErr)
		return &StreamResult{Usage: billingUsage}, s.deliverConvertedStreamFailure(
			ctx,
			c,
			conv,
			stream,
			synthErr,
			synthErr,
			clientOutputStarted,
			isSSECommitted,
			writeStreamHeaders,
		)
	}
	logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
	stageOrWriteClientFrames(tailFrames)
	if !clientOutputStarted && len(stagedFrames) > 0 {
		if flushStagedFrames() && !clientDisconnected {
			c.Writer.Flush()
		}
	}
	return &StreamResult{
		Usage:            billingUsage,
		ClientDisconnect: clientDisconnected || c.Err() != nil,
	}, nil
}

// deliverConvertedStreamFailure classifies streamErr once and either returns a
// pre-semantic failover error without touching c.Writer or writes the resolved
// terminal error frame or uncommitted HTTP JSON error. Started streams close
// open blocks via stream.Close(cause) and prefer WriteRawTerminalSSE for the
// final error frame so sequence numbers and response IDs stay continuous.
func (s *ResponseAdapter) deliverConvertedStreamFailure(
	ctx context.Context,
	c *upstream.OutputContext,
	conv ConvertedOptions,
	stream *convert.Stream,
	streamErr *upstream.GeminiStreamError,
	rawErr error,
	clientOutputStarted bool,
	isSSECommitted func() bool,
	writeStreamHeaders func(),
) error {
	classified := s.classifyConvertedFailure(ctx, conv, streamErr, clientOutputStarted)
	if classified.Failover != nil {
		// Discard closing frames during pre-semantic failover so the sink remains
		// uncommitted for the next candidate provider attempt.
		_, _ = stream.Close(classified.Failover)
		logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
		return classified.Failover
	}

	cause := &ir.StreamError{
		Type:    classified.Type,
		Code:    classified.Code,
		Message: classified.Message,
		Status:  classified.Status,
	}

	if clientOutputStarted {
		s.emitStartedConvertedAbortFrames(c, conv, stream, cause, writeStreamHeaders)
		return errors.Join(rawErr, classified.Err)
	}

	// Finalize unstarted converter state without flushing staged frames so the
	// response renders either one untimed SSE error or one HTTP JSON error.
	_, _ = stream.Close(cause)
	logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
	if isSSECommitted() && conv.WriteLocalTerminalError != nil {
		conv.WriteLocalTerminalError(cause.Status, cause.Type, cause.Code, cause.Message)
		return errors.Join(rawErr, classified.Err)
	}
	writeConvertedAntigravityUnstartedError(c, conv, cause.Status, cause.Type, cause.Code, cause.Message)
	return errors.Join(rawErr, classified.Err)
}

// emitStartedConvertedAbortFrames closes open blocks on stream with cause and
// writes the resulting closing frames, routing any final terminal error frame
// through conv.WriteRawTerminalSSE when available. Already-completed streams
// return quietly, while empty or failed closes on unfinished streams fall back
// to conv.WriteLocalTerminalError.
func (s *ResponseAdapter) emitStartedConvertedAbortFrames(
	c *upstream.OutputContext,
	conv ConvertedOptions,
	stream *convert.Stream,
	cause *ir.StreamError,
	writeStreamHeaders func(),
) {
	closingFrames, closeErr := stream.Close(cause)
	logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
	// Suppress fallback terminal error frames when Stream.Close reports a clean
	// prior finish without error so completed responses never append a second terminal.
	if closeErr == nil && len(closingFrames) == 0 && stream.Summary().Finish.Reason != "" && stream.Summary().Err == nil {
		return
	}
	// Fall back to WriteLocalTerminalError when Stream.Close was already consumed
	// or failed on an unfinished stream so started client streams terminate cleanly.
	if closeErr != nil || len(closingFrames) == 0 {
		if conv.WriteLocalTerminalError != nil {
			conv.WriteLocalTerminalError(cause.Status, cause.Type, cause.Code, cause.Message)
		}
		return
	}
	writeStreamHeaders()
	for i, f := range closingFrames {
		raw := sse.Append(nil, f)
		isLastTerminal := i == len(closingFrames)-1 && f.Terminal
		if isLastTerminal &&
			conv.WriteRawTerminalSSE != nil &&
			conv.WriteRawTerminalSSE(cause.Status, cause.Type, cause.Code, cause.Message, raw) {
			continue
		}
		if c == nil || c.Writer == nil {
			return
		}
		c.NextEvent(f.Semantic, f.Terminal)
		if _, wErr := c.Writer.Write(raw); wErr != nil {
			return
		}
	}
	if c != nil && c.Writer != nil {
		c.Writer.Flush()
	}
}

// handleBufferedStreamFailure classifies readErr once for buffered Antigravity
// SSE collection, returning pre-output failover errors directly or presenting
// the resolved client error response. Context cancellations or execution
// deadlines emit an untimed terminal SSE error when SSE headers were already
// committed, while upstream read timeouts with an active ctx enter single-pass
// stream failure classification.
func (s *ResponseAdapter) handleBufferedStreamFailure(
	ctx context.Context,
	c *upstream.OutputContext,
	conv ConvertedOptions,
	readErr error,
) error {
	if ctx.Err() != nil || errors.Is(readErr, context.Canceled) {
		logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
		if isConvertedSSECommitted(c, conv) && conv.WriteLocalTerminalError != nil {
			conv.WriteLocalTerminalError(
				http.StatusBadGateway,
				defaultConvertedAntigravityErrorType(conv.DefaultErrorType, http.StatusBadGateway),
				"upstream_stream_error",
				"Stream interrupted before completion",
			)
		}
		return readErr
	}
	var streamErr *upstream.GeminiStreamError
	if !errors.As(readErr, &streamErr) {
		logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
		return readErr
	}
	classified := s.classifyConvertedFailure(ctx, conv, streamErr, false)
	logAntigravityExchangeLossesDebug(conv.Exchange, conv.RequestID)
	if classified.Failover != nil {
		return classified.Failover
	}
	if isConvertedSSECommitted(c, conv) && conv.WriteLocalTerminalError != nil {
		conv.WriteLocalTerminalError(classified.Status, classified.Type, classified.Code, classified.Message)
		return errors.Join(readErr, classified.Err)
	}
	writeConvertedAntigravityUnstartedError(c, conv, classified.Status, classified.Type, classified.Code, classified.Message)
	return errors.Join(readErr, classified.Err)
}

// isConvertedSSECommitted reports whether downstream HTTP 200 SSE headers were
// already committed via conv.CommittedSSE or direct OutputContext writer writes.
func isConvertedSSECommitted(c *upstream.OutputContext, conv ConvertedOptions) bool {
	if conv.CommittedSSE != nil && conv.CommittedSSE() {
		return true
	}
	return c != nil && c.Writer != nil && c.Writer.Written()
}

// classifyConvertedFailure invokes conv.ClassifyStreamFailure when configured
// and fills safe default status, type, and message fields for standalone callers.
// Non-nil Failover outcomes return immediately without synthesizing client fields.
func (s *ResponseAdapter) classifyConvertedFailure(
	ctx context.Context,
	conv ConvertedOptions,
	streamErr *upstream.GeminiStreamError,
	clientOutputStarted bool,
) ConvertedStreamFailure {
	var classified ConvertedStreamFailure
	if conv.ClassifyStreamFailure != nil {
		classified = conv.ClassifyStreamFailure(ctx, streamErr, clientOutputStarted)
		if classified.Failover != nil {
			return classified
		}
	} else {
		classified.Status = streamErr.Status
		classified.Message = strings.TrimSpace(streamErr.Message)
	}
	if classified.Status <= 0 {
		classified.Status = http.StatusBadGateway
	}
	if classified.Type == "" {
		classified.Type = defaultConvertedAntigravityErrorType(conv.DefaultErrorType, classified.Status)
	}
	if classified.Message == "" {
		classified.Message = strings.TrimSpace(streamErr.Message)
	}
	if classified.Message == "" {
		classified.Message = "Upstream stream failed"
	}
	return classified
}

// writeBufferedResponseHeaders attaches requestID when non-empty and sets JSON
// Content-Type before buffered client output. Callers invoke this helper
// immediately ahead of c.NextEvent and c.Data so deferred output stays closed
// until conversion succeeds.
func writeBufferedResponseHeaders(c *upstream.OutputContext, requestID string) {
	if requestID != "" {
		c.Header("x-request-id", requestID)
	}
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
}

// unwrapV1InternalConvertedPayload unwraps a v1internal {"response": ...}
// envelope via lossless jsontext token scanning while validating wirejson usage
// uniqueness on both outer and inner objects. Non-empty outer responseId values
// override inner responseId fields, and outer modelVersion values backfill
// missing inner modelVersion fields without altering numeric precision or
// unknown JSON members.
func unwrapV1InternalConvertedPayload(body []byte) ([]byte, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return body, nil
	}
	if err := wirejson.ValidateUsageEnvelope(trimmed); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAmbiguousUsagePayload, err)
	}

	var outerStack [8]rawJSONObjectMember
	outerMembers, parseErr := appendOrderedJSONObjectSpans(outerStack[:0], trimmed)
	if parseErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrAmbiguousUsagePayload, parseErr)
	}
	if err := validateUsageMetadataMember(outerMembers); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAmbiguousUsagePayload, err)
	}

	respIdx := -1
	outerResponseID := ""
	outerModelVersion := ""
	// Ignore non-string decode errors on optional outer metadata fields because
	// appendOrderedJSONObjectSpans already verified JSON syntax and non-string
	// values leave outerResponseID and outerModelVersion unset.
	for i, m := range outerMembers {
		switch m.name {
		case "response":
			respIdx = i
		case "responseId":
			_ = unmarshalJSONString(m.value, &outerResponseID)
		case "modelVersion":
			_ = unmarshalJSONString(m.value, &outerModelVersion)
		}
	}
	if respIdx < 0 {
		return bytes.Clone(trimmed), nil
	}

	innerRaw := bytes.TrimSpace(outerMembers[respIdx].value)
	if len(innerRaw) == 0 || innerRaw[0] != '{' {
		return bytes.Clone(innerRaw), nil
	}
	if err := wirejson.ValidateUsageEnvelope(innerRaw); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAmbiguousUsagePayload, err)
	}

	var innerStack [8]rawJSONObjectMember
	innerMembers, innerErr := appendOrderedJSONObjectSpans(innerStack[:0], innerRaw)
	if innerErr != nil {
		return nil, fmt.Errorf("%w: %w", ErrAmbiguousUsagePayload, innerErr)
	}
	if err := validateUsageMetadataMember(innerMembers); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAmbiguousUsagePayload, err)
	}

	if outerResponseID == "" && outerModelVersion == "" {
		return bytes.Clone(innerRaw), nil
	}

	changed := false
	innerRespIDIdx := -1
	innerModelIdx := -1
	innerModelVal := ""
	// Ignore non-string inner modelVersion decode errors so null or non-string
	// placeholders are treated as blank and backfilled by outerModelVersion.
	for i, m := range innerMembers {
		switch m.name {
		case "responseId":
			innerRespIDIdx = i
		case "modelVersion":
			innerModelIdx = i
			_ = unmarshalJSONString(m.value, &innerModelVal)
		}
	}

	if outerResponseID != "" {
		encID, err := json.Marshal(outerResponseID, jsontext.AllowInvalidUTF8(true))
		if err != nil {
			return nil, err
		}
		if innerRespIDIdx >= 0 {
			if !bytes.Equal(bytes.TrimSpace(innerMembers[innerRespIDIdx].value), encID) {
				innerMembers[innerRespIDIdx].value = jsontext.Value(encID)
				changed = true
			}
		} else {
			innerMembers = append(innerMembers, rawJSONObjectMember{
				name:  "responseId",
				value: jsontext.Value(encID),
			})
			changed = true
		}
	}

	if outerModelVersion != "" && innerModelVal == "" {
		encModel, err := json.Marshal(outerModelVersion, jsontext.AllowInvalidUTF8(true))
		if err != nil {
			return nil, err
		}
		if innerModelIdx >= 0 {
			innerMembers[innerModelIdx].value = jsontext.Value(encModel)
		} else {
			innerMembers = append(innerMembers, rawJSONObjectMember{
				name:  "modelVersion",
				value: jsontext.Value(encModel),
			})
		}
		changed = true
	}

	if !changed {
		return bytes.Clone(innerRaw), nil
	}
	return encodeOrderedJSONObject(innerMembers, len(innerRaw)+64)
}

// validateAntigravityUsagePayload enforces wirejson envelope uniqueness on
// payload, verifies nested duplicate-key absence via jsontext, and validates
// any present non-null usageMetadata object for case-colliding counter keys.
// Non-object inputs return nil, whereas ambiguous keys return an error.
func validateAntigravityUsagePayload(payload []byte) error {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil
	}
	if err := wirejson.ValidateUsageEnvelope(trimmed); err != nil {
		return err
	}
	var stack [8]rawJSONObjectMember
	members, err := appendOrderedJSONObjectSpans(stack[:0], trimmed)
	if err != nil {
		return err
	}
	return validateUsageMetadataMember(members)
}

// validateUsageMetadataMember checks any non-null usageMetadata member in
// members with wirejson.ValidateUsageObject to reject case-colliding counters.
// Objects without usageMetadata or with an explicit null value return nil.
func validateUsageMetadataMember(members []rawJSONObjectMember) error {
	for _, m := range members {
		if m.name == "usageMetadata" {
			val := bytes.TrimSpace(m.value)
			if len(val) > 0 && !bytes.Equal(val, []byte("null")) {
				if err := wirejson.ValidateUsageObject(val); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// extractAntigravityBillingUsage validates data with wirejson and projects
// Gemini usageMetadata counters into billing TokenUsage via the existing native
// projection path. Empty, invalid, or ambiguous payloads return nil so tainted
// frames never contribute billing counts.
func extractAntigravityBillingUsage(data []byte) *upstream.TokenUsage {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	if err := validateAntigravityUsagePayload(data); err != nil {
		return nil
	}
	return gemini.NativeUsageProjection(gemini.NativeExtractGeminiUsage(data))
}

// observe merges non-null usageMetadata fields from unwrappedBody after wirejson
// validation and returns the cumulative native billing TokenUsage snapshot.
// Ambiguous or non-usage payloads return (nil, false) without mutating a.fields.
func (a *antigravityRawUsageAccumulator) observe(unwrappedBody []byte) (*upstream.TokenUsage, bool) {
	trimmed := bytes.TrimSpace(unwrappedBody)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, false
	}
	if err := wirejson.ValidateUsageEnvelope(trimmed); err != nil {
		return nil, false
	}
	var topStack [8]rawJSONObjectMember
	topMembers, err := appendOrderedJSONObjectSpans(topStack[:0], trimmed)
	if err != nil {
		return nil, false
	}
	var rawUsage jsontext.Value
	for _, m := range topMembers {
		if m.name == "usageMetadata" {
			rawUsage = bytes.TrimSpace(m.value)
			break
		}
	}
	if len(rawUsage) == 0 || bytes.Equal(rawUsage, []byte("null")) {
		return nil, false
	}
	if err := wirejson.ValidateUsageObject(rawUsage); err != nil {
		return nil, false
	}
	var usageStack [8]rawJSONObjectMember
	usageMembers, err := appendOrderedJSONObjectSpans(usageStack[:0], rawUsage)
	if err != nil {
		return nil, false
	}

	for _, um := range usageMembers {
		val := bytes.TrimSpace(um.value)
		if len(val) == 0 || bytes.Equal(val, []byte("null")) {
			continue
		}
		cloned := jsontext.Value(bytes.Clone(val))
		replaced := false
		for i := range a.fields {
			if a.fields[i].name == um.name {
				a.fields[i].value = cloned
				replaced = true
				break
			}
		}
		if !replaced {
			a.fields = append(a.fields, rawJSONObjectMember{
				name:  um.name,
				value: cloned,
			})
		}
	}

	usageObjBytes, err := encodeOrderedJSONObject(a.fields, 192)
	if err != nil {
		return nil, true
	}
	envelopeBytes, err := encodeOrderedJSONObject([]rawJSONObjectMember{{
		name:  "usageMetadata",
		value: jsontext.Value(usageObjBytes),
	}}, len(usageObjBytes)+24)
	if err != nil {
		return nil, true
	}
	return gemini.NativeUsageProjection(gemini.NativeExtractGeminiUsage(envelopeBytes)), true
}

// parseOrderedJSONObject scans a top-level JSON object from raw into detached
// rawJSONObjectMember entries while tolerating invalid UTF-8 in string values
// and enforcing jsontext duplicate-key rejection across nested values.
// Non-object roots, malformed syntax, duplicate keys, or trailing tokens return
// an error without partial results.
func parseOrderedJSONObject(raw []byte) ([]rawJSONObjectMember, error) {
	members, err := appendOrderedJSONObjectSpans(nil, raw)
	if err != nil {
		return nil, err
	}
	for i := range members {
		members[i].value = members[i].value.Clone()
	}
	return members, nil
}

// appendOrderedJSONObjectSpans scans a top-level JSON object from raw into dst
// while tolerating invalid UTF-8 in string values and enforcing jsontext
// duplicate-key rejection across nested values. Member value slices borrow raw
// directly, and known envelope keys map to static constants without heap allocation.
func appendOrderedJSONObjectSpans(dst []rawJSONObjectMember, raw []byte) ([]rawJSONObjectMember, error) {
	pooled, ok := jsonDecoderPool.Get().(*pooledJSONDecoder)
	if !ok || pooled == nil {
		pooled = &pooledJSONDecoder{}
		pooled.dec = jsontext.NewDecoder(&pooled.reader, jsontext.AllowInvalidUTF8(true))
	}
	pooled.reader.Reset(raw)
	pooled.dec.Reset(&pooled.reader, jsontext.AllowInvalidUTF8(true))
	defer func() {
		pooled.reader.Reset(nil)
		jsonDecoderPool.Put(pooled)
	}()

	tok, err := pooled.dec.ReadToken()
	if err != nil {
		return nil, err
	}
	if tok.Kind() != '{' {
		return nil, errors.New("expected JSON object")
	}
	members := dst
	for pooled.dec.PeekKind() != '}' {
		kTok, kErr := pooled.dec.ReadToken()
		if kErr != nil {
			return nil, kErr
		}
		key := internAntigravityEnvelopeKey(kTok.String())
		vStart := int(pooled.dec.InputOffset())
		if vErr := pooled.dec.SkipValue(); vErr != nil {
			return nil, vErr
		}
		vEnd := int(pooled.dec.InputOffset())
		span := bytes.TrimSpace(raw[vStart:vEnd])
		if len(span) > 0 && span[0] == ':' {
			span = bytes.TrimSpace(span[1:])
		}
		members = append(members, rawJSONObjectMember{
			name:  key,
			value: jsontext.Value(span),
		})
	}
	if _, err := pooled.dec.ReadToken(); err != nil {
		return nil, err
	}
	if _, err := pooled.dec.ReadToken(); err != io.EOF {
		return nil, errors.New("unexpected trailing JSON token")
	}
	return members, nil
}

// internAntigravityEnvelopeKey maps standard Gemini and v1internal envelope keys
// to static string constants and clones unrecognized keys via strings.Clone.
// Keeping rawKey non-escaping lets inlined jsontext.Token.String calls compare
// the thirteen common envelope keys on the stack with zero heap allocations.
func internAntigravityEnvelopeKey(rawKey string) string {
	switch rawKey {
	case "response":
		return "response"
	case "responseId":
		return "responseId"
	case "modelVersion":
		return "modelVersion"
	case "candidates":
		return "candidates"
	case "usageMetadata":
		return "usageMetadata"
	case "error":
		return "error"
	case "promptFeedback":
		return "promptFeedback"
	case "promptTokenCount":
		return "promptTokenCount"
	case "candidatesTokenCount":
		return "candidatesTokenCount"
	case "cachedContentTokenCount":
		return "cachedContentTokenCount"
	case "thoughtsTokenCount":
		return "thoughtsTokenCount"
	case "totalTokenCount":
		return "totalTokenCount"
	case "candidatesTokensDetails":
		return "candidatesTokensDetails"
	default:
		return strings.Clone(rawKey)
	}
}

// encodeOrderedJSONObject serializes validated rawJSONObjectMember entries into
// a compact JSON object while preserving each member's exact raw value bytes and
// tolerating invalid UTF-8 byte sequences. Positive sizeHint values preallocate
// the output buffer, while nonpositive values use default buffer growth.
func encodeOrderedJSONObject(members []rawJSONObjectMember, sizeHint int) ([]byte, error) {
	out := make([]byte, 0, max(sizeHint, 2))
	out = append(out, '{')
	for i, m := range members {
		if i > 0 {
			out = append(out, ',')
		}
		keyBytes, err := json.Marshal(m.name, jsontext.AllowInvalidUTF8(true))
		if err != nil {
			return nil, err
		}
		out = append(out, keyBytes...)
		out = append(out, ':')
		out = append(out, m.value...)
	}
	out = append(out, '}')
	return out, nil
}

// unmarshalJSONString decodes a JSON string token from raw into dst using
// encoding/json/v2 while tolerating invalid UTF-8 byte sequences. Non-string
// JSON tokens or malformed syntax return a decode error without overwriting *dst.
func unmarshalJSONString(raw jsontext.Value, dst *string) error {
	return json.Unmarshal(raw, dst, jsontext.AllowInvalidUTF8(true))
}

// coerceAntigravityStreamError extracts an existing GeminiStreamError from err
// or wraps err into an HTTP 502 stream_read_error classification. Structured
// stream errors return unchanged so upstream status and body bytes survive.
func coerceAntigravityStreamError(err error) *upstream.GeminiStreamError {
	if existing, ok := errors.AsType[*upstream.GeminiStreamError](err); ok && existing != nil {
		return existing
	}
	return &upstream.GeminiStreamError{
		Status:  http.StatusBadGateway,
		Kind:    "stream_read_error",
		Message: "Failed to read upstream stream",
		Cause:   err,
	}
}

// newAntigravityDecodeStreamError wraps a stream frame or completion conversion
// failure into an HTTP 502 GeminiStreamError for single-pass policy handling.
func newAntigravityDecodeStreamError(message string, cause error) *upstream.GeminiStreamError {
	return &upstream.GeminiStreamError{
		Status:  http.StatusBadGateway,
		Kind:    "stream_invalid_json",
		Message: message,
		Cause:   cause,
	}
}

// defaultConvertedAntigravityErrorType selects the client error category when
// stream failure classification leaves Type blank. Non-empty fallbacks win
// directly; empty fallbacks map HTTP 400 onto "invalid_request_error" and every
// other status onto "upstream_error".
func defaultConvertedAntigravityErrorType(fallback string, status int) string {
	if fallback != "" {
		return fallback
	}
	if status == http.StatusBadRequest {
		return "invalid_request_error"
	}
	return "upstream_error"
}

// writeConvertedAntigravityUnstartedError delegates uncommitted client error
// presentation to conv.WriteError or writes a minimal JSON error envelope via
// encoding/json/v2 for standalone callers. Nil output contexts without
// WriteError return safely without panicking.
func writeConvertedAntigravityUnstartedError(
	c *upstream.OutputContext,
	conv ConvertedOptions,
	status int,
	errType, errCode, message string,
) {
	if conv.WriteError != nil {
		conv.WriteError(status, errType, errCode, message)
		return
	}
	if c == nil || c.Writer == nil {
		return
	}
	// standaloneErrorBody formats fallback JSON error fields when WriteError is nil.
	type standaloneErrorBody struct {
		Type    string `json:"type"`          // client error category
		Code    string `json:"code,omitzero"` // optional machine-readable code
		Message string `json:"message"`       // client-safe error message
	}
	// standaloneErrorEnvelope wraps standaloneErrorBody under a top-level "error" key.
	type standaloneErrorEnvelope struct {
		Error standaloneErrorBody `json:"error"` // top-level error object
	}
	encoded, err := json.Marshal(standaloneErrorEnvelope{
		Error: standaloneErrorBody{
			Type:    errType,
			Code:    errCode,
			Message: message,
		},
	})
	if err != nil {
		encoded = []byte(`{"error":{"type":"upstream_error","message":"Upstream request failed"}}`)
	}
	c.Data(status, "application/json; charset=utf-8", encoded)
}

// logAntigravityExchangeLossesDebug records the total conversion Loss count
// from exchange at debug level alongside requestID whenever at least one loss
// occurred. Nil exchanges and lossless conversions return immediately without
// emitting log entries.
func logAntigravityExchangeLossesDebug(exchange *convert.Exchange, requestID string) {
	if exchange == nil {
		return
	}
	losses := exchange.Losses()
	if len(losses) == 0 {
		return
	}
	logging.L().Debug("antigravity converted response: recorded conversion losses",
		zap.String("request_id", requestID),
		zap.Int("loss_count", len(losses)),
	)
}
