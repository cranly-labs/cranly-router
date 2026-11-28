package openai

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	logger "github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// response_converted.go reads streaming and buffered OpenAI Responses SSE
// streams through convert.Exchange for any client protocol while defining
// shared CompatResponseResult metadata and serialized SSE header writers.
// OutputContext.NextEvent marks every non-empty payload write as CommitForRetry,
// so writing a non-semantic leading frame (such as message_start or an opening
// assistant role chunk) immediately would close provider failover before actual
// content arrives.
//
// ReadConvertedStreaming holds non-semantic converted frames in a local staging
// slice until the first Semantic==true frame (cleared by ChatSilentRefusalDetector
// when active) or a non-retryable terminal arrives, running upstream events
// through responseStreamLifecycleTracker so Codex standalone error frames can
// still recover on a subsequent response.completed event and empty completions
// remain eligible for retry. Response headers stay deferred via OutputContext
// until actual client frames flush, while early-committed HTTP 200 SSE streams
// receive untimed client-protocol terminal error events on failure or timeout.

// CompatResponseResult carries reader-specific usage and upstream/request
// metadata for inbound result adaptation.
type CompatResponseResult struct {
	// ReasoningEffort is the optional request reasoning level. ResolvedTier is the
	// optional request tier. Both can share caller-owned string storage; readers
	// lacking these annotations leave them nil, and consumers keep pointed-to values stable.
	ReasoningEffort, ResolvedTier *string
	// ResponseID is a Responses turn ID when the selected reader records it;
	// readers omitting turn identity leave this field empty.
	ResponseID string
	// ClientDisconnect is populated by stream readers when downstream delivery
	// fails or policy forces output halt, while buffered readers leave it zero.
	ClientDisconnect bool
	// RequestID is the upstream HTTP request identity. Model is the client model.
	// UpstreamModel is the selected upstream request model. UpstreamResponseModel
	// is the raw model echoed by the upstream response before client rewriting.
	// ServiceTier is the observed upstream response tier. These describe the same
	// attempt independently of ResponseID and the inbound handler's separately
	// selected billing model.
	RequestID, Model, UpstreamModel, UpstreamResponseModel, ServiceTier string
	// UpstreamHeaders shares the response header map with the executor; callers
	// clone it before edits that would alter another consumer's observation.
	UpstreamHeaders    http.Header
	Usage              wire.ForwardUsage             // observed upstream billing usage
	Stream             bool                          // client streaming response mode
	Duration           time.Duration                 // total response handling duration
	ProtocolConversion *telemetry.ProtocolConversion // bounded protocol transformation diagnostics

	SearchCount int // observed native search calls
}

// ConvertedFailure describes either a pre-output failover error or the client
// status, type, code, and message selected for an unrecoverable stream failure.
type ConvertedFailure struct {
	// Failover takes precedence over Status, Type, Code, and Message when non-nil
	// so pre-output retryable errors return directly to the provider loop.
	Failover error
	Status   int    // HTTP status used when no SSE framing has been committed
	Type     string // client error category
	Code     string // optional machine-readable error code
	Message  string // client-safe failure message
}

// ConvertedResponseOptions binds gateway failure policies, header filters,
// search counters, and untimed SSE error writers around one Responses attempt.
type ConvertedResponseOptions struct {
	RequestContext context.Context // logger context for unexpected scanner errors
	RequestBodyLen int             // client request byte size for Chat silent-refusal gating
	// CodexFailureTerminal holds standalone error frames until stream end so a
	// subsequent response.completed frame can supersede transient Codex errors.
	CodexFailureTerminal bool
	// NativeOpenAI enables EmptyCompleted failover classification when a native
	// OpenAI stream finishes completed without output items or usage tokens.
	NativeOpenAI bool
	// AppendDoneOnCyber appends a trailing data: [DONE] SSE terminator after
	// cyber-policy error frames when the inbound client uses Chat Completions.
	AppendDoneOnCyber bool
	// DefaultErrorType supplies the fallback client error category when failure
	// classification or WriteError receives an empty errType string.
	DefaultErrorType string

	Scanner        func(io.Reader) *bufio.Scanner // line scanner constructor with configured max line size
	StreamInterval func() time.Duration           // per-attempt upstream read-idle timeout getter

	RestoreToolNames func([]byte) []byte                   // reverses Codex OAuth tool aliases before decoding
	Observe          func([]byte, string)                  // pre-conversion raw payload model and tier observer
	ObserveFinal     func(*wire.ResponsesResponse)         // buffered terminal response model and tier observer
	ServiceTier      func() string                         // request-scoped observed upstream service tier getter
	Headers          func(http.Header, http.Header)        // copies approved upstream headers onto pending output
	CountSearch      bool                                  // enables Grok native search call counting
	JSONSearch       func([]byte) int                      // counts search calls in buffered terminal JSON
	StreamSearch     func([]byte, map[string]struct{}) int // deduplicates search calls across stream events
	CommittedSSE     func() bool                           // reports whether HTTP 200 SSE headers are already committed

	// WriteError writes an uncommitted HTTP JSON error or promotes to an untimed
	// terminal SSE error when waiting heartbeats already committed 200 SSE.
	WriteError func(status int, errType, code, message string)
	// WriteLocalTerminalError writes an untimed client-protocol terminal SSE error
	// (including synthetic response.failed for Responses clients) onto an active stream.
	WriteLocalTerminalError func(status int, errType, code, message string)
	// WriteRawTerminalSSE delivers pre-encoded terminal SSE frame bytes through
	// the gateway's untimed terminal barrier and reports whether delivery ran.
	WriteRawTerminalSSE func(data []byte) bool

	StreamFailure        func(payload []byte, message string, outputStarted, bareError bool) ConvertedFailure                               // classifies stream error/failed frames
	TerminalSideEffects  func(payload []byte, message string, headers http.Header, upstreamModel string)                                    // records deferred post-output failure side effects
	BufferedFailure      func(*wire.ResponsesResponse, wire.ForwardUsage) error                                                             // classifies failed buffered terminal objects
	BufferedReadFailure  func(error) error                                                                                                  // classifies buffered SSE transport read errors
	ObserveTerminalUsage func(final *wire.ResponsesResponse, usage *wire.ForwardUsage, terminalEvent string, clientDisconnected bool) error // validates or logs terminal usage

	EmptyCompleted        func(requestID string) error // builds failover error for empty native OpenAI completions
	SilentRefusal         func() error                 // builds failover error for large-request Chat silent refusals
	MissingTerminal       func(message string) error   // classifies pre-output missing-terminal stream truncation
	RecordMissingTerminal func(message string)         // records post-output missing-terminal diagnostics in Ops
	ReadFailure           func(error) error            // classifies mid-stream or pre-output scanner read failures

	CyberForwarded error                    // sentinel returned after cyber-policy rejection delivery
	MarkCyber      func(CyberObservation)   // records cyber-policy moderation telemetry before delivery
	Truncate       func(string, int) string // UTF-8-safe byte truncation helper for diagnostics
}

// convertedResponseModelTracker records raw upstream response.model values
// observed before client-model rewriting, giving terminal events precedence
// over non-terminal frames and clearing conflicting non-terminal candidates.
type convertedResponseModelTracker struct {
	nonTerminal         string // unanimous non-terminal response.model candidate
	nonTerminalConflict bool   // true when non-terminal frames disagreed on model
	terminal            string // authoritative terminal response.model value
}

// NewCompatStreamHeaderWriter returns a closure that commits filtered HTTP 200
// SSE headers on its first invocation. Construction retains headers and the
// required filter without writing. Serialize closure calls: its ordinary
// boolean is set before filtering/writing, and repeated calls perform no
// further header work.
func NewCompatStreamHeaderWriter(c *upstream.OutputContext, headers http.Header, filter func(http.Header, http.Header)) func() {
	written := false
	return func() {
		if written {
			return
		}
		// Claim this serialized closure before filtering and committing headers,
		// so later conversion chunks cannot repeat the first-output header side effects.
		written = true
		filter(c.Writer.Header(), headers)
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("X-Accel-Buffering", "no")
		c.Writer.WriteHeader(http.StatusOK)
	}
}

// ReadConvertedBuffered reads an upstream Responses SSE body through
// exchange.Buffer and writes a single converted client JSON response on HTTP 200
// while accumulating billing usage and raw upstream response model observations.
//
// Standalone Codex error frames wait for a possible recovering response.completed
// event when CodexFailureTerminal is enabled, and failed terminals delegate to
// BufferedFailure before any client JSON bytes are written. The caller retains
// ownership of closing resp.Body except on interval timeout, which closes the
// body early to unblock a blocked scanner goroutine.
func ReadConvertedBuffered(
	resp *http.Response,
	c *upstream.OutputContext,
	exchange *convert.Exchange,
	options ConvertedResponseOptions,
	originalModel, upstreamModel string,
	startTime time.Time,
) (result *CompatResponseResult, err error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("upstream response body is nil")
	}
	if exchange == nil {
		return nil, errors.New("conversion exchange is nil")
	}

	reqCtx := convertedRequestContext(options.RequestContext, resp)
	requestID := firstNonEmptyConvertedHeader(resp.Header, "x-request-id", "xai-request-id")
	buf := exchange.Buffer()
	defer func() {
		snap := upstream.FinalizeExchangeConversion(reqCtx, exchange)
		if result != nil {
			result.ProtocolConversion = snap
		}
	}()
	defer func() {
		if err != nil {
			// Ensure buffered sealer losses commit onto exchange before
			// FinalizeExchangeConversion snapshots cumulative LossCounts.
			_, _ = buf.Close(err)
		}
	}()
	lifecycle := newResponseStreamLifecycleTracker(options.CodexFailureTerminal)
	var modelTracker convertedResponseModelTracker
	var usage wire.ForwardUsage
	var usageAccumulator wire.UsageAccumulator
	var finalResponse *wire.ResponsesResponse
	responseID := ""
	sawTerminal := false
	searchCount := 0
	streamSearchSeen := make(map[string]struct{})

	scanner := buildConvertedScanner(resp.Body, options.Scanner)
	streamInterval := readConvertedStreamInterval(options.StreamInterval)

	var timeoutCh <-chan time.Time
	var timeoutTimer *time.Timer
	resetTimeout := func() {
		if streamInterval <= 0 {
			return
		}
		if timeoutTimer == nil {
			timeoutTimer = time.NewTimer(streamInterval)
			timeoutCh = timeoutTimer.C
			return
		}
		// Drain an unread expiry tick after a false Stop return so resetting the
		// idle budget never triggers an immediate false timeout on the next select.
		if !timeoutTimer.Stop() {
			select {
			case <-timeoutTimer.C:
			default:
			}
		}
		timeoutTimer.Reset(streamInterval)
	}
	stopTimeout := func() {
		if timeoutTimer == nil {
			return
		}
		if !timeoutTimer.Stop() {
			select {
			case <-timeoutTimer.C:
			default:
			}
		}
	}
	resetTimeout()
	defer stopTimeout()

	// scanEvent pairs one raw SSE line or scanner read error from the producer goroutine.
	type scanEvent struct {
		line string // single scanned SSE line without trailing newline
		err  error  // scanner read failure when non-nil
	}
	events := make(chan scanEvent, 16)
	done := make(chan struct{})
	// Run body scanning on a producer goroutine so interval timeout selection can
	// close resp.Body and return promptly while network reads are blocked.
	go func() {
		defer close(events)
		for scanner.Scan() {
			select {
			case events <- scanEvent{line: scanner.Text()}:
			case <-done:
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case events <- scanEvent{err: err}:
			case <-done:
			}
		}
	}()
	defer close(done)

	resultWithUsage := func() *CompatResponseResult {
		out := &CompatResponseResult{
			RequestID:             requestID,
			UpstreamHeaders:       resp.Header,
			ResponseID:            responseID,
			Usage:                 usage,
			Model:                 originalModel,
			UpstreamModel:         upstreamModel,
			UpstreamResponseModel: modelTracker.Model(),
			ServiceTier:           readConvertedServiceTier(options.ServiceTier, ""),
			Stream:                false,
			Duration:              time.Since(startTime),
		}
		if searchCount > 0 {
			out.SearchCount = searchCount
		}
		return out
	}

	var failedImmediateErr error
	processFrame := func(frame wire.OpenAICompatSSEFrame) bool {
		rawPayload := []byte(frame.Data)
		rawType := wire.EffectiveOpenAISSEEventType(rawPayload, frame.EventType)
		observeConvertedRawPayload(rawPayload, rawType, &modelTracker, options.Observe)

		restored := restoreConvertedToolNames(rawPayload, options.RestoreToolNames)
		normalizedData, eventType, duplicateTerminal := lifecycle.NormalizeData(restored, rawType)
		// Backfill a missing JSON type field after lifecycle normalization wraps bare
		// response objects so non-lifecycle SSE event headers populate event.Type cleanly.
		normalizedData = []byte(wire.OpenAICompatPayloadWithEventType(string(normalizedData), eventType))
		lifecycle.ResolveNormalizedEvent(eventType)
		if duplicateTerminal {
			return false
		}
		if options.CountSearch && options.StreamSearch != nil {
			searchCount += options.StreamSearch(normalizedData, streamSearchSeen)
		}

		var event wire.ResponsesStreamEvent
		if err := json.Unmarshal(normalizedData, &event); err != nil {
			logger.L().Warn("openai converted buffered: failed to parse event",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
			return false
		}
		wire.ParseSSEUsageBytesWithType(normalizedData, eventType, &usage)
		if snapshot := usageAccumulator.ObserveResponsesEvent(normalizedData, &event); snapshot != nil {
			usage = usageAccumulator.ForwardUsage()
		}
		if event.Response != nil {
			if id := strings.TrimSpace(event.Response.ID); id != "" {
				responseID = id
			}
		}

		if eventType == "response.failed" || eventType == "error" {
			_ = lifecycle.BeginFailureEvent(eventType, normalizedData)
			// Hold standalone Codex error frames inside lifecycle and buf so a
			// subsequent response.completed can recover the turn without failing early.
			if options.CodexFailureTerminal && eventType == "error" {
				lifecycle.CommitFailureEvent(eventType, normalizedData)
				_ = buf.Write(sse.Frame{Event: eventType, Data: normalizedData})
				return false
			}
			termResp := CompatTerminalResponse(&event, normalizedData)
			if termResp != nil && options.ObserveFinal != nil {
				options.ObserveFinal(termResp)
			}
			if options.BufferedFailure != nil {
				failedImmediateErr = options.BufferedFailure(termResp, usage)
			} else {
				failedImmediateErr = fmt.Errorf("upstream response failed: %s", lifecycle.failedMessage)
			}
			return true
		}

		if err := buf.Write(sse.Frame{Event: eventType, Data: normalizedData}); err != nil {
			logger.L().Warn("openai converted buffered: failed to fold frame",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
		}

		if IsCompatResponsesTerminalEvent(eventType) {
			sawTerminal = true
			if termResp := CompatTerminalResponse(&event, normalizedData); termResp != nil {
				finalResponse = termResp
				if finalResponse.Usage == nil {
					finalResponse.Usage = event.Usage
				}
				if options.ObserveFinal != nil {
					options.ObserveFinal(finalResponse)
				}
			}
			return true
		}
		return false
	}

	var heartbeatFilter upstream.SSEHeartbeatLineFilter
	var parser wire.OpenAICompatSSEFrameParser
	feedLine := func(line string) (bool, bool) {
		for _, filtered := range heartbeatFilter.Push([]byte(line + "\n")) {
			clean := strings.TrimSuffix(string(filtered), "\n")
			if IsCompatDoneSentinelLine(clean) {
				return true, false
			}
			frame, ok := parser.AddLine(clean)
			if !ok {
				continue
			}
			if strings.TrimSpace(frame.Data) == "[DONE]" {
				return true, false
			}
			if processFrame(frame) {
				return false, true
			}
		}
		return false, false
	}
	flushEOF := func() bool {
		for _, filtered := range heartbeatFilter.Flush() {
			clean := strings.TrimSuffix(string(filtered), "\n")
			if IsCompatDoneSentinelLine(clean) {
				return false
			}
			if frame, ok := parser.AddLine(clean); ok && strings.TrimSpace(frame.Data) != "[DONE]" {
				if processFrame(frame) {
					return true
				}
			}
		}
		if frame, ok := parser.Finish(); ok && strings.TrimSpace(frame.Data) != "[DONE]" {
			if processFrame(frame) {
				return true
			}
		}
		return false
	}

scanLoop:
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				flushEOF()
				break scanLoop
			}
			resetTimeout()
			if ev.err != nil {
				if !errors.Is(ev.err, context.Canceled) && !errors.Is(ev.err, context.DeadlineExceeded) {
					logger.L().Warn("openai converted buffered: read error",
						zap.Error(ev.err),
						zap.String("request_id", requestID),
					)
				}
				wrapped := &CompatBufferedReadError{cause: ev.err}
				if options.BufferedReadFailure != nil {
					return resultWithUsage(), options.BufferedReadFailure(wrapped)
				}
				return resultWithUsage(), ev.err
			}
			doneSentinel, stop := feedLine(ev.line)
			if stop || doneSentinel {
				break scanLoop
			}

		case <-timeoutCh:
			// Close resp.Body before returning so a scanner goroutine blocked in
			// network Read wakes immediately when the idle interval expires.
			_ = resp.Body.Close()
			logger.L().Warn("openai converted buffered: data interval timeout",
				zap.String("request_id", requestID),
				zap.Duration("interval", streamInterval),
			)
			timeoutErr := fmt.Errorf("stream data interval timeout")
			if options.BufferedReadFailure != nil {
				return resultWithUsage(), options.BufferedReadFailure(timeoutErr)
			}
			return resultWithUsage(), timeoutErr
		}
	}

	if failedImmediateErr != nil {
		return nil, failedImmediateErr
	}
	if lifecycle.HasUnresolvedBareError() {
		synth := CompatTerminalResponse(&wire.ResponsesStreamEvent{Type: "error"}, lifecycle.bareErrorPayload)
		if synth != nil && options.ObserveFinal != nil {
			options.ObserveFinal(synth)
		}
		if options.BufferedFailure != nil {
			return nil, options.BufferedFailure(synth, usage)
		}
		return nil, fmt.Errorf("upstream response failed: %s", lifecycle.failedMessage)
	}
	if !sawTerminal {
		errType := defaultConvertedErrorType(options.DefaultErrorType, exchange.Info())
		writeConvertedUnstartedError(c, options, http.StatusBadGateway, errType, "", "Upstream stream ended without a terminal response event")
		return nil, fmt.Errorf("upstream stream ended without terminal event")
	}

	if options.ObserveTerminalUsage != nil {
		termEvent := "response.completed"
		if finalResponse != nil && strings.TrimSpace(finalResponse.Status) != "" && strings.TrimSpace(finalResponse.Status) != "completed" {
			termEvent = "response." + strings.TrimSpace(finalResponse.Status)
		}
		if err := options.ObserveTerminalUsage(finalResponse, &usage, termEvent, false); err != nil {
			return nil, err
		}
	}

	clientJSON, err := buf.Close(nil)
	if err != nil {
		errType := defaultConvertedErrorType(options.DefaultErrorType, exchange.Info())
		writeConvertedUnstartedError(c, options, http.StatusBadGateway, errType, "", "Failed to convert upstream response")
		return nil, err
	}
	logExchangeLossesDebug(exchange, requestID)

	if options.CountSearch && searchCount == 0 && options.JSONSearch != nil && finalResponse != nil {
		if rawFinal, marshalErr := json.Marshal(finalResponse); marshalErr == nil {
			if n := options.JSONSearch(rawFinal); n > 0 {
				searchCount = n
			}
		}
	}

	if options.Headers != nil {
		options.Headers(c.Writer.Header(), resp.Header)
	}
	// Overwrite any copied upstream text/event-stream Content-Type before emitting
	// buffered JSON so downstream clients parse the body as unary JSON.
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.Data(http.StatusOK, "application/json; charset=utf-8", clientJSON)

	return resultWithUsage(), nil
}

// ReadConvertedStreaming reads an upstream Responses SSE stream through
// exchange.Stream, holds non-semantic leading client frames until semantic
// progress or a non-retryable terminal arrives, and returns billing usage and
// raw response model observations across normal and error exits.
//
// Pre-output retryable failures return a nil result whenever no content bytes
// were committed to c.Writer so outer attempt loops settle usage once on the
// winning retry. When DownstreamSSEKeepalive already committed HTTP 200 SSE
// before semantic output, non-retryable errors and timeouts emit untimed
// client-protocol terminal SSE error frames; after client output starts,
// abnormal truncations and failures close open blocks through stream.Close
// before delivering the terminal error frame.
func ReadConvertedStreaming(
	resp *http.Response,
	c *upstream.OutputContext,
	exchange *convert.Exchange,
	options ConvertedResponseOptions,
	originalModel, upstreamModel string,
	startTime time.Time,
) (result *CompatResponseResult, err error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("upstream response body is nil")
	}
	if exchange == nil {
		return nil, errors.New("conversion exchange is nil")
	}

	reqCtx := convertedRequestContext(options.RequestContext, resp)
	requestID := firstNonEmptyConvertedHeader(resp.Header, "x-request-id", "xai-request-id")
	writeStreamHeaders := NewCompatStreamHeaderWriter(c, resp.Header, func(dst, src http.Header) {
		if options.Headers != nil {
			options.Headers(dst, src)
		}
	})

	stream := exchange.Stream()
	defer func() {
		snap := upstream.FinalizeExchangeConversion(reqCtx, exchange)
		if result != nil {
			result.ProtocolConversion = snap
		}
	}()
	defer func() {
		if err != nil {
			// Flush any unclosed stream sealer losses onto exchange before
			// FinalizeExchangeConversion snapshots cumulative LossCounts.
			_, _ = stream.Close(err)
		}
	}()
	lifecycle := newResponseStreamLifecycleTracker(options.CodexFailureTerminal)
	var modelTracker convertedResponseModelTracker
	var usage wire.ForwardUsage
	var usageAccumulator wire.UsageAccumulator

	var refusalDetector *ChatSilentRefusalDetector
	if options.SilentRefusal != nil {
		refusalDetector = NewChatSilentRefusalDetector(options.RequestBodyLen)
	}

	var stagedFrames []sse.Frame
	stagedHasSemantic := false
	semanticOutputSeen := false
	clientOutputStarted := false
	clientDisconnected := false
	responseID := ""
	terminalEventType := ""
	var finalResponse *wire.ResponsesResponse
	searchCount := 0
	streamSearchSeen := make(map[string]struct{})

	var cyberPolicyErr error
	var streamFailoverErr error
	var streamNonFailoverErr error

	resultWithUsage := func() *CompatResponseResult {
		summaryTier := stream.Summary().ServiceTier
		out := &CompatResponseResult{
			RequestID:             requestID,
			UpstreamHeaders:       resp.Header,
			ResponseID:            responseID,
			Usage:                 usage,
			Model:                 originalModel,
			UpstreamModel:         upstreamModel,
			UpstreamResponseModel: modelTracker.Model(),
			ServiceTier:           readConvertedServiceTier(options.ServiceTier, summaryTier),
			Stream:                true,
			Duration:              time.Since(startTime),
			ClientDisconnect:      clientDisconnected,
		}
		if searchCount > 0 {
			out.SearchCount = searchCount
		}
		return out
	}

	isSSECommitted := func() bool {
		if options.CommittedSSE != nil && options.CommittedSSE() {
			return true
		}
		return c != nil && c.Writer != nil && c.Writer.Written()
	}

	isContentWritten := func() bool {
		return clientOutputStarted || (c != nil && c.Writer != nil && c.Writer.ContentWritten())
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
			logger.L().Info("openai converted stream: client disconnected, continuing to drain upstream for billing",
				zap.String("request_id", requestID),
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

	emitStartedAbortFrames := func(cause *ir.StreamError) {
		if clientDisconnected {
			return
		}
		closingFrames, closeErr := stream.Close(cause)
		logExchangeLossesDebug(exchange, requestID)
		// Fall back to WriteLocalTerminalError when stream.Close was already
		// consumed or failed so an active client stream still receives an error terminal.
		if closeErr != nil || len(closingFrames) == 0 {
			if options.WriteLocalTerminalError != nil {
				options.WriteLocalTerminalError(cause.Status, cause.Type, cause.Code, cause.Message)
			}
			return
		}
		writeStreamHeaders()
		for i, f := range closingFrames {
			raw := sse.Append(nil, f)
			isLastTerminal := i == len(closingFrames)-1 && f.Terminal
			if isLastTerminal && options.WriteRawTerminalSSE != nil && options.WriteRawTerminalSSE(raw) {
				clientOutputStarted = true
				continue
			}
			c.NextEvent(f.Semantic, f.Terminal)
			if _, err := c.Writer.Write(raw); err != nil {
				clientDisconnected = true
				return
			}
			clientOutputStarted = true
		}
		if !clientDisconnected {
			c.Writer.Flush()
		}
	}

	deliverFailure := func(status int, errType, errCode, errMsg string) {
		if clientDisconnected {
			return
		}
		if errType == "" {
			errType = defaultConvertedErrorType(options.DefaultErrorType, exchange.Info())
		}
		if status <= 0 {
			status = http.StatusBadGateway
		}
		if clientOutputStarted {
			emitStartedAbortFrames(&ir.StreamError{
				Type:    errType,
				Code:    errCode,
				Message: errMsg,
				Status:  status,
			})
			return
		}
		_, _ = stream.Close(&ir.StreamError{
			Type:    errType,
			Code:    errCode,
			Message: errMsg,
			Status:  status,
		})
		logExchangeLossesDebug(exchange, requestID)
		if isSSECommitted() && options.WriteLocalTerminalError != nil {
			options.WriteLocalTerminalError(status, errType, errCode, errMsg)
			clientOutputStarted = true
			return
		}
		writeConvertedUnstartedError(c, options, status, errType, errCode, errMsg)
		clientOutputStarted = true
	}

	stageOrWriteClientFrames := func(frames []sse.Frame) {
		if len(frames) == 0 || clientDisconnected {
			return
		}
		wroteDirectly := false
		for _, f := range frames {
			if f.Semantic {
				semanticOutputSeen = true
				refusalDetector.MarkSemanticOutput()
			}
			if !clientOutputStarted {
				stagedFrames = append(stagedFrames, f)
				if f.Semantic {
					stagedHasSemantic = true
				}
				if stagedHasSemantic && refusalDetector.ShouldReleaseClientOutput() {
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

	deliverCyberFailure := func(code, clientMsg string) {
		if clientDisconnected {
			return
		}
		if !clientOutputStarted && !isSSECommitted() {
			// Commit HTTP 200 SSE headers explicitly on deferred writers before
			// emitting cyber-policy rejection frames so unstarted streams stay SSE.
			writeStreamHeaders()
			c.Writer.WriteHeaderNow()
		}
		cause := &ir.StreamError{
			Type:    "invalid_request_error",
			Code:    code,
			Message: clientMsg,
			Status:  http.StatusBadRequest,
		}
		closingFrames, _ := stream.Close(cause)
		logExchangeLossesDebug(exchange, requestID)

		var terminalRaw []byte
		for i, f := range closingFrames {
			raw := sse.Append(nil, f)
			if i == len(closingFrames)-1 && f.Terminal {
				terminalRaw = raw
				continue
			}
			if clientOutputStarted {
				c.NextEvent(f.Semantic, false)
				if _, err := c.Writer.Write(raw); err != nil {
					clientDisconnected = true
					return
				}
			}
		}
		if options.AppendDoneOnCyber {
			if len(terminalRaw) == 0 {
				terminalRaw = []byte("data: {\"error\":{\"type\":\"invalid_request_error\",\"message\":\"Request blocked by upstream cyber-security policy\"}}\n\n")
			}
			terminalRaw = append(terminalRaw, "data: [DONE]\n\n"...)
			if options.WriteRawTerminalSSE == nil || !options.WriteRawTerminalSSE(terminalRaw) {
				c.NextEvent(false, true)
				_, _ = c.Writer.Write(terminalRaw)
				c.Writer.Flush()
			}
			clientOutputStarted = true
			return
		}
		if options.WriteLocalTerminalError != nil {
			options.WriteLocalTerminalError(http.StatusBadRequest, "invalid_request_error", code, clientMsg)
			clientOutputStarted = true
			return
		}
		if len(terminalRaw) > 0 {
			if options.WriteRawTerminalSSE == nil || !options.WriteRawTerminalSSE(terminalRaw) {
				c.NextEvent(false, true)
				_, _ = c.Writer.Write(terminalRaw)
				c.Writer.Flush()
			}
		}
		clientOutputStarted = true
	}

	processPayload := func(rawPayload []byte, frameEventType string) bool {
		rawType := wire.EffectiveOpenAISSEEventType(rawPayload, frameEventType)
		observeConvertedRawPayload(rawPayload, rawType, &modelTracker, options.Observe)

		restored := restoreConvertedToolNames(rawPayload, options.RestoreToolNames)
		normalizedData, eventType, duplicateTerminal := lifecycle.NormalizeData(restored, rawType)
		// Backfill a missing JSON type field after lifecycle normalization wraps bare
		// response objects so header-only delta or error frames retain eventType.
		normalizedData = []byte(wire.OpenAICompatPayloadWithEventType(string(normalizedData), eventType))
		lifecycle.ResolveNormalizedEvent(eventType)
		if duplicateTerminal {
			return false
		}
		if options.CountSearch && options.StreamSearch != nil {
			searchCount += options.StreamSearch(normalizedData, streamSearchSeen)
		}

		var event wire.ResponsesStreamEvent
		if err := json.Unmarshal(normalizedData, &event); err != nil {
			logger.L().Warn("openai converted stream: failed to parse event",
				zap.Error(err),
				zap.String("request_id", requestID),
			)
			return false
		}
		refusalDetector.ObservePayload(normalizedData)
		wire.ParseSSEUsageBytesWithType(normalizedData, eventType, &usage)
		if snapshot := usageAccumulator.ObserveResponsesEvent(normalizedData, &event); snapshot != nil {
			usage = usageAccumulator.ForwardUsage()
		}

		isBareError := eventType == "error"
		isTerminal := IsCompatResponsesTerminalEvent(eventType)
		if isTerminal {
			terminalEventType = eventType
			if event.Response != nil {
				finalResponse = event.Response
				if id := strings.TrimSpace(event.Response.ID); id != "" {
					responseID = id
				}
			}
		}

		if eventType == "response.failed" || isBareError {
			failedMessage := lifecycle.BeginFailureEvent(eventType, normalizedData)
			if hit, code, msg := DetectOpenAICyberPolicy(normalizedData); hit {
				// Record up to 4 KiB of cyber-policy evidence with observed token counts
				// before client delivery so write failures never drop moderation telemetry.
				if options.MarkCyber != nil {
					options.MarkCyber(CyberObservation{
						Code:           code,
						Message:        msg,
						Body:           truncateConvertedUTF8(string(normalizedData), 4096, options.Truncate),
						UpstreamStatus: http.StatusOK,
						UpstreamInTok:  usage.InputTokens,
						UpstreamOutTok: usage.OutputTokens,
					})
				}
				clientMsg := msg
				if clientMsg == "" {
					clientMsg = "Request blocked by upstream cyber-security policy"
				}
				if !clientDisconnected {
					deliverCyberFailure(code, clientMsg)
					clientDisconnected = true
				}
				if options.CyberForwarded != nil {
					cyberPolicyErr = options.CyberForwarded
				} else {
					cyberPolicyErr = errors.New("openai cyber_policy forwarded")
				}
				return true
			}

			// In Codex terminal mode, hold post-output standalone error frames and
			// non-retryable pre-output standalone error frames until response.completed
			// recovers the turn or EOF synthesizes the terminal failure.
			if options.CodexFailureTerminal && isBareError {
				if !isContentWritten() && options.StreamFailure != nil {
					probe := options.StreamFailure(normalizedData, failedMessage, false, true)
					if probe.Failover != nil {
						lifecycle.MarkEarlyFailure()
						streamFailoverErr = probe.Failover
						return true
					}
				}
				lifecycle.RecordFailureSideEffects(eventType, isContentWritten(), false, func() {
					if options.TerminalSideEffects != nil {
						options.TerminalSideEffects(normalizedData, failedMessage, resp.Header, upstreamModel)
					}
				})
				lifecycle.CommitFailureEvent(eventType, normalizedData)
				_, _ = stream.Write(sse.Frame{Event: eventType, Data: normalizedData})
				return false
			}

			lifecycle.RecordFailureSideEffects(eventType, isContentWritten(), false, func() {
				if options.TerminalSideEffects != nil {
					options.TerminalSideEffects(normalizedData, failedMessage, resp.Header, upstreamModel)
				}
			})
			var failure ConvertedFailure
			if options.StreamFailure != nil {
				failure = options.StreamFailure(normalizedData, failedMessage, isContentWritten(), isBareError)
			} else {
				failure = ConvertedFailure{
					Status:  http.StatusBadGateway,
					Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					Code:    event.Code,
					Message: failedMessage,
				}
			}
			if failure.Failover != nil {
				lifecycle.MarkEarlyFailure()
				streamFailoverErr = failure.Failover
				return true
			}
			lifecycle.CommitFailureEvent(eventType, normalizedData)
			deliverFailure(failure.Status, failure.Type, failure.Code, failure.Message)
			streamNonFailoverErr = fmt.Errorf("upstream response failed: %s", failure.Message)
			return true
		}

		// Check empty native OpenAI completions before flushing staged leading
		// frames so empty terminal streams still fail over cleanly.
		if options.NativeOpenAI &&
			options.EmptyCompleted != nil &&
			(eventType == "response.completed" || eventType == "response.done") &&
			!lifecycle.sawFailedEvent &&
			!semanticOutputSeen &&
			!isContentWritten() &&
			wire.OpenAIResponsesCompletedEventIsEmpty(normalizedData, &usage) {
			streamFailoverErr = options.EmptyCompleted(requestID)
			return true
		}

		clientFrames, convErr := stream.Write(sse.Frame{Event: eventType, Data: normalizedData})
		if convErr != nil {
			logger.L().Warn("openai converted stream: failed to convert event",
				zap.Error(convErr),
				zap.String("request_id", requestID),
			)
			return isTerminal
		}
		stageOrWriteClientFrames(clientFrames)
		return isTerminal
	}

	finalizeStream := func() (*CompatResponseResult, error) {
		if cyberPolicyErr != nil {
			return resultWithUsage(), cyberPolicyErr
		}
		if streamFailoverErr != nil {
			if !isContentWritten() {
				return nil, streamFailoverErr
			}
			return resultWithUsage(), streamFailoverErr
		}
		if streamNonFailoverErr != nil {
			return resultWithUsage(), streamNonFailoverErr
		}

		if lifecycle.HasUnresolvedBareError() {
			lifecycle.FlushBareErrorSideEffects(func(payload []byte, message string) {
				if options.TerminalSideEffects != nil {
					options.TerminalSideEffects(payload, message, resp.Header, upstreamModel)
				}
			})
			var failure ConvertedFailure
			if options.StreamFailure != nil {
				failure = options.StreamFailure(lifecycle.bareErrorPayload, lifecycle.failedMessage, isContentWritten(), true)
			} else {
				failure = ConvertedFailure{
					Status:  http.StatusBadGateway,
					Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					Message: lifecycle.failedMessage,
				}
			}
			if failure.Failover != nil {
				if !isContentWritten() {
					return nil, failure.Failover
				}
				return resultWithUsage(), failure.Failover
			}
			lifecycle.MarkBareErrorSynthesized()
			deliverFailure(failure.Status, failure.Type, failure.Code, failure.Message)
			return resultWithUsage(), fmt.Errorf("upstream response failed: %s", failure.Message)
		}

		closingFrames, closeErr := stream.Close(nil)
		logExchangeLossesDebug(exchange, requestID)
		if closeErr != nil {
			logger.L().Warn("openai converted stream: close error",
				zap.Error(closeErr),
				zap.String("request_id", requestID),
			)
		}
		if len(closingFrames) > 0 && !clientDisconnected {
			if !clientOutputStarted {
				stagedFrames = append(stagedFrames, closingFrames...)
			} else {
				for _, f := range closingFrames {
					if !writeSingleClientFrame(f) {
						break
					}
				}
			}
		}

		// Evaluate large-request Chat silent refusal before flushing held non-semantic
		// frames so an empty stop completion can still switch providers cleanly.
		if !clientDisconnected && !isContentWritten() && !semanticOutputSeen && refusalDetector.IsSilentRefusal() && options.SilentRefusal != nil {
			return nil, options.SilentRefusal()
		}

		if !clientDisconnected && len(stagedFrames) > 0 {
			if flushStagedFrames() {
				c.Writer.Flush()
			}
		} else if !clientDisconnected && clientOutputStarted {
			c.Writer.Flush()
		}
		if responseID == "" {
			responseID = stream.Summary().ID
		}
		if options.ObserveTerminalUsage != nil {
			_ = options.ObserveTerminalUsage(finalResponse, &usage, terminalEventType, clientDisconnected)
		}
		return resultWithUsage(), nil
	}

	handleScanErr := func(scanErr error) (*CompatResponseResult, error) {
		if scanErr != nil && !errors.Is(scanErr, context.Canceled) && !errors.Is(scanErr, context.DeadlineExceeded) {
			logger.FromContext(options.RequestContext).Warn("openai converted stream: read error",
				zap.Error(scanErr),
				zap.String("upstream_request_id", requestID),
			)
		}
		if clientDisconnected || errors.Is(scanErr, context.Canceled) || errors.Is(scanErr, context.DeadlineExceeded) {
			if clientOutputStarted && !clientDisconnected {
				emitStartedAbortFrames(&ir.StreamError{
					Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					Message: "Stream interrupted before completion",
					Status:  http.StatusBadGateway,
				})
			} else if !clientOutputStarted && isSSECommitted() && !clientDisconnected && options.WriteLocalTerminalError != nil {
				options.WriteLocalTerminalError(
					http.StatusBadGateway,
					defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					"",
					"Stream interrupted before completion",
				)
			}
			return resultWithUsage(), fmt.Errorf("stream usage incomplete: %w", scanErr)
		}

		var classified error
		if options.ReadFailure != nil {
			classified = options.ReadFailure(scanErr)
		} else {
			classified = NewUpstreamStreamReadError(scanErr)
		}
		if !isContentWritten() {
			return nil, classified
		}
		if options.RecordMissingTerminal != nil {
			options.RecordMissingTerminal("OpenAI converted stream read error after output started")
		}
		emitStartedAbortFrames(&ir.StreamError{
			Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
			Message: "Upstream stream disconnected before completion",
			Status:  http.StatusBadGateway,
		})
		return resultWithUsage(), classified
	}

	missingTerminalErr := func() (*CompatResponseResult, error) {
		if lifecycle.HasUnresolvedBareError() {
			return finalizeStream()
		}
		if clientDisconnected {
			return resultWithUsage(), fmt.Errorf("stream usage incomplete: missing terminal event")
		}
		message := "OpenAI converted stream ended before a terminal event"
		if !isContentWritten() {
			if options.MissingTerminal != nil {
				return nil, options.MissingTerminal(message)
			}
			if isSSECommitted() && options.WriteLocalTerminalError != nil {
				options.WriteLocalTerminalError(
					http.StatusBadGateway,
					defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					"",
					message,
				)
			}
			return resultWithUsage(), fmt.Errorf("stream usage incomplete: missing terminal event")
		}
		if options.RecordMissingTerminal != nil {
			options.RecordMissingTerminal(message)
		}
		emitStartedAbortFrames(&ir.StreamError{
			Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
			Message: message,
			Status:  http.StatusBadGateway,
		})
		return resultWithUsage(), fmt.Errorf("stream usage incomplete: missing terminal event")
	}

	scanner := buildConvertedScanner(resp.Body, options.Scanner)
	streamInterval := readConvertedStreamInterval(options.StreamInterval)

	var heartbeatFilter upstream.SSEHeartbeatLineFilter
	var parser wire.OpenAICompatSSEFrameParser
	feedLine := func(line string) (bool, bool) {
		for _, filtered := range heartbeatFilter.Push([]byte(line + "\n")) {
			clean := strings.TrimSuffix(string(filtered), "\n")
			if IsCompatDoneSentinelLine(clean) {
				return true, false
			}
			frame, ok := parser.AddLine(clean)
			if !ok {
				continue
			}
			if strings.TrimSpace(frame.Data) == "[DONE]" {
				return true, false
			}
			if processPayload([]byte(frame.Data), frame.EventType) {
				return false, true
			}
		}
		return false, false
	}
	flushEOF := func() (bool, bool) {
		for _, filtered := range heartbeatFilter.Flush() {
			clean := strings.TrimSuffix(string(filtered), "\n")
			if IsCompatDoneSentinelLine(clean) {
				return true, false
			}
			if frame, ok := parser.AddLine(clean); ok {
				if strings.TrimSpace(frame.Data) == "[DONE]" {
					return true, false
				}
				if processPayload([]byte(frame.Data), frame.EventType) {
					return false, true
				}
			}
		}
		if frame, ok := parser.Finish(); ok {
			if strings.TrimSpace(frame.Data) == "[DONE]" {
				return true, false
			}
			if processPayload([]byte(frame.Data), frame.EventType) {
				return false, true
			}
		}
		return false, false
	}

	// Synchronous scanning avoids spawning a read-ahead producer goroutine when
	// per-line stream interval timeouts are disabled.
	if streamInterval <= 0 {
		for scanner.Scan() {
			doneSentinel, reachedTerminal := feedLine(scanner.Text())
			if reachedTerminal {
				return finalizeStream()
			}
			if doneSentinel {
				return missingTerminalErr()
			}
		}
		if err := scanner.Err(); err != nil {
			return handleScanErr(err)
		}
		_, reachedTerminal := flushEOF()
		if reachedTerminal {
			return finalizeStream()
		}
		return missingTerminalErr()
	}

	intervalTicker := time.NewTicker(streamInterval)
	defer intervalTicker.Stop()
	intervalCh := intervalTicker.C

	// scanEvent delivers one scanned SSE line or read error to the timed consumer.
	type scanEvent struct {
		line string // scanned upstream SSE line
		err  error  // scanner read error
	}
	events := make(chan scanEvent, 16)
	done := make(chan struct{})
	var lastReadAt int64
	atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
	sendEvent := func(ev scanEvent) bool {
		select {
		case events <- ev:
			return true
		case <-done:
			return false
		}
	}
	// Update lastReadAt upon upstream line arrival before channel queueing so slow
	// downstream client writes never cause false upstream idle-timeout aborts.
	go func() {
		defer close(events)
		for scanner.Scan() {
			atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
			if !sendEvent(scanEvent{line: scanner.Text()}) {
				return
			}
		}
		if err := scanner.Err(); err != nil {
			_ = sendEvent(scanEvent{err: err})
		}
	}()
	defer close(done)

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				_, reachedTerminal := flushEOF()
				if reachedTerminal {
					return finalizeStream()
				}
				return missingTerminalErr()
			}
			if ev.err != nil {
				return handleScanErr(ev.err)
			}
			doneSentinel, reachedTerminal := feedLine(ev.line)
			if reachedTerminal {
				return finalizeStream()
			}
			if doneSentinel {
				return missingTerminalErr()
			}

		case <-intervalCh:
			lastRead := time.Unix(0, atomic.LoadInt64(&lastReadAt))
			if time.Since(lastRead) < streamInterval {
				continue
			}
			if clientDisconnected {
				return resultWithUsage(), fmt.Errorf("stream usage incomplete after timeout")
			}
			logger.L().Warn("openai converted stream: data interval timeout",
				zap.String("request_id", requestID),
				zap.String("model", originalModel),
				zap.Duration("interval", streamInterval),
			)
			timeoutMsg := "Upstream stream timed out waiting for data"
			if clientOutputStarted {
				emitStartedAbortFrames(&ir.StreamError{
					Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					Message: timeoutMsg,
					Status:  http.StatusGatewayTimeout,
				})
			} else if isSSECommitted() && options.WriteLocalTerminalError != nil {
				options.WriteLocalTerminalError(
					http.StatusGatewayTimeout,
					defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					"",
					timeoutMsg,
				)
			}
			return resultWithUsage(), fmt.Errorf("stream data interval timeout")
		}
	}
}

// Observe records a raw response.model candidate from payload before client
// model rewriting runs. Nil receivers, empty payloads, unparseable JSON, and
// blank model fields leave existing tracker state unchanged.
//
// Non-error terminal lifecycle events establish an authoritative model that
// overrides any earlier non-terminal value, while conflicting non-terminal
// models clear the candidate until a terminal frame settles it.
func (m *convertedResponseModelTracker) Observe(payload []byte, eventType string) {
	if m == nil || len(payload) == 0 {
		return
	}
	var envelope struct {
		Model    string `json:"model"` // top-level model field when present
		Response *struct {
			Model string `json:"model"` // nested response.model field
		} `json:"response"` // nested response lifecycle object
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		// Unparseable frames contribute no model candidate while leaving earlier
		// non-terminal and terminal model observations intact.
		return
	}
	candidate := ""
	if envelope.Response != nil {
		candidate = strings.TrimSpace(envelope.Response.Model)
	}
	if candidate == "" {
		candidate = strings.TrimSpace(envelope.Model)
	}
	if candidate == "" {
		return
	}
	if IsCompatResponsesTerminalEvent(eventType) && strings.TrimSpace(eventType) != "error" {
		m.terminal = candidate
		return
	}
	if m.nonTerminalConflict {
		return
	}
	if m.nonTerminal == "" {
		m.nonTerminal = candidate
		return
	}
	if m.nonTerminal != candidate {
		m.nonTerminal = ""
		m.nonTerminalConflict = true
	}
}

// Model returns the authoritative terminal response model when observed or the
// unanimous non-terminal candidate otherwise. Nil receivers and conflicting
// non-terminal observations without a terminal event return an empty string.
func (m *convertedResponseModelTracker) Model() string {
	if m == nil {
		return ""
	}
	if m.terminal != "" {
		return m.terminal
	}
	return m.nonTerminal
}

// buildConvertedScanner constructs an SSE line scanner using custom when
// non-nil. When custom is nil, it returns NewCompatSSEScanner with a 2 MiB
// maximum line size while leaving reader closure to the caller.
func buildConvertedScanner(r io.Reader, custom func(io.Reader) *bufio.Scanner) *bufio.Scanner {
	if custom != nil {
		return custom(r)
	}
	// Default to a 2 MiB max line size when callers omit a custom scanner factory.
	return NewCompatSSEScanner(r, 2*1024*1024)
}

// readConvertedStreamInterval evaluates getter when non-nil to obtain the
// per-attempt upstream read-idle timeout. Nil getters return zero, disabling
// background interval timers and producer goroutines in stream readers.
func readConvertedStreamInterval(getter func() time.Duration) time.Duration {
	if getter != nil {
		return getter()
	}
	return 0
}

// restoreConvertedToolNames applies restorer when non-nil to reverse Codex
// OAuth tool aliases on raw SSE payloads before event decoding. Nil restorers
// return payload unchanged without allocating a duplicate byte slice.
func restoreConvertedToolNames(payload []byte, restorer func([]byte) []byte) []byte {
	if restorer != nil {
		return restorer(payload)
	}
	return payload
}

// observeConvertedRawPayload notifies both the attempt-local model tracker and
// any caller-supplied raw payload observer before protocol conversion runs. Nil
// observers are skipped after tracker updates complete.
func observeConvertedRawPayload(
	rawPayload []byte,
	rawEventType string,
	tracker *convertedResponseModelTracker,
	observer func([]byte, string),
) {
	tracker.Observe(rawPayload, rawEventType)
	if observer != nil {
		observer(rawPayload, rawEventType)
	}
}

// readConvertedServiceTier prefers a trimmed non-empty request-context tier
// from getter and falls back to the trimmed stream summary tier otherwise. Nil
// getters return the trimmed fallback string directly.
func readConvertedServiceTier(getter func() string, fallback string) string {
	if getter != nil {
		if tier := strings.TrimSpace(getter()); tier != "" {
			return tier
		}
	}
	return strings.TrimSpace(fallback)
}

// truncateConvertedUTF8 delegates diagnostic truncation to custom when non-nil.
// When custom is nil, non-positive limits or short strings return s unchanged,
// while longer inputs back up to a valid UTF-8 rune start at or below maxBytes.
func truncateConvertedUTF8(s string, maxBytes int, custom func(string, int) string) string {
	if custom != nil {
		return custom(s, maxBytes)
	}
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}
	for maxBytes > 0 && !utf8.RuneStart(s[maxBytes]) {
		maxBytes--
	}
	return s[:maxBytes]
}

// defaultConvertedErrorType returns trimmed configured when non-empty for
// client error envelopes. Blank configured values fall back to "api_error" so
// downstream error frames always carry a non-empty category.
func defaultConvertedErrorType(configured string, _ convert.RequestInfo) string {
	if strings.TrimSpace(configured) != "" {
		return strings.TrimSpace(configured)
	}
	return "api_error"
}

// writeConvertedUnstartedError delegates unstarted error presentation to
// options.WriteError when configured. When WriteError is nil, non-nil writers
// receive a fallback JSON error envelope carrying status, errType, errCode, and message.
func writeConvertedUnstartedError(
	c *upstream.OutputContext,
	options ConvertedResponseOptions,
	status int,
	errType, errCode, message string,
) {
	if options.WriteError != nil {
		options.WriteError(status, errType, errCode, message)
		return
	}
	if c == nil || c.Writer == nil {
		return
	}
	type jsonErrBody struct {
		Type    string `json:"type"`           // client error category
		Code    string `json:"code,omitempty"` // optional error code
		Message string `json:"message"`        // client-safe error message
	}
	type jsonErrEnvelope struct {
		Error jsonErrBody `json:"error"` // top-level error wrapper
	}
	c.JSON(status, jsonErrEnvelope{
		Error: jsonErrBody{
			Type:    errType,
			Code:    errCode,
			Message: message,
		},
	})
}

// logExchangeLossesDebug emits a debug log entry recording the total conversion
// loss count accumulated on exchange for requestID. Nil exchanges and
// zero-loss conversions skip debug logging without allocating slices.
func logExchangeLossesDebug(exchange *convert.Exchange, requestID string) {
	if exchange == nil {
		return
	}
	losses := exchange.Losses()
	if len(losses) == 0 {
		return
	}
	logger.L().Debug("openai converted response: recorded conversion losses",
		zap.String("request_id", requestID),
		zap.Int("loss_count", len(losses)),
	)
}

// convertedRequestContext prefers resp.Request.Context() when the dispatched
// upstream request carries an active attempt reference or conversion scope so
// recursive retries finalize response losses on the actual retry attempt rather
// than a stale outer Gin context. Otherwise it selects ctx when non-nil,
// falling back to resp's request context or context.Background.
func convertedRequestContext(ctx context.Context, resp *http.Response) context.Context {
	if resp != nil && resp.Request != nil {
		if reqCtx := resp.Request.Context(); telemetry.RequestAttemptFromContext(reqCtx).Number > 0 ||
			telemetry.ProtocolConversionFromContext(reqCtx) != nil {
			return reqCtx
		}
	}
	if ctx != nil {
		return ctx
	}
	if resp != nil && resp.Request != nil {
		return resp.Request.Context()
	}
	return context.Background()
}

// firstNonEmptyConvertedHeader returns the first trimmed non-empty header value
// found under keys in h, or "" when h is nil or every candidate is blank.
func firstNonEmptyConvertedHeader(h http.Header, keys ...string) string {
	if h == nil {
		return ""
	}
	for _, k := range keys {
		if v := strings.TrimSpace(h.Get(k)); v != "" {
			return v
		}
	}
	return ""
}
