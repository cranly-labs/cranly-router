package openaiforward

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
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// anthropic_converted_output.go reads Anthropic Messages SSE streams (including
// Vertex and EventStream-bridged Bedrock responses) through convert.Exchange for
// any client protocol while preserving native billing usage and failover windows.
// OutputContext.NextEvent marks every non-empty payload write as CommitForRetry,
// so flushing a non-semantic leading frame such as response.created or an
// initial assistant role chunk immediately would close provider failover before
// upstream content blocks arrive.
//
// ReadConvertedAnthropicStreaming stages non-semantic client frames in memory
// until the first Semantic==true frame or a clean terminal event arrives,
// evaluates provider health at most once per upstream error frame through
// StreamFailure, and keeps draining upstream usage after downstream disconnects.
// Response headers stay deferred via NewDeferredOutputContext, and committed
// HTTP 200 SSE streams receive untimed terminal client error frames on failure.

// ConvertedAnthropicFailure describes either a pre-output failover error or the
// client status, type, code, and message selected for a stream failure.
type ConvertedAnthropicFailure struct {
	// Failover takes precedence over Status, Type, Code, and Message when non-nil
	// so pre-semantic retryable upstream errors return to the outer provider loop.
	Failover error
	Status   int    // HTTP status used when no SSE framing has been committed
	Type     string // client-protocol error category
	Code     string // optional machine-readable error code
	Message  string // sanitized client-facing failure description
}

// ConvertedAnthropicOutputOptions binds header copying, tool restoration,
// single-pass failure classification, and untimed terminal SSE writers to one
// converted Messages response attempt.
type ConvertedAnthropicOutputOptions struct {
	ProviderID       int64  // selected provider identifier for diagnostics
	MaxFrameBytes    int    // per-frame SSE accumulation byte limit; <=0 uses default
	DefaultErrorType string // fallback client error type when classification omits one

	StreamInterval   func() time.Duration                 // upstream idle interval getter; <=0 disables timeout
	UpdateWindow     func(context.Context, http.Header)   // optional passive session-window header observer
	CopyHeaders      func(http.Header, http.Header)       // copies approved upstream headers onto pending output
	RestoreToolNames func([]byte) []byte                  // restores OAuth-rewritten tool names before IR decoding
	OverrideCache    func(context.Context) (string, bool) // optional 5m/1h billing cache bucket reclassification
	Observe          func(protocolanthropic.Observation)  // optional pre-conversion raw event observer
	CommittedSSE     func() bool                          // reports whether HTTP 200 SSE headers are already committed

	// WriteError writes an uncommitted HTTP JSON error or promotes to an untimed
	// terminal SSE error when header-wait heartbeats already committed 200 SSE.
	WriteError func(status int, errType, code, message string)
	// WriteLocalTerminalError writes an untimed client-protocol terminal SSE error
	// (including synthetic response.failed for Responses clients) onto an active stream.
	WriteLocalTerminalError func(status int, errType, code, message string)
	// WriteRawTerminalSSE delivers pre-encoded terminal SSE frame bytes through
	// the gateway's untimed terminal barrier and reports whether delivery ran.
	WriteRawTerminalSSE func(data []byte) bool

	// StreamFailure classifies an upstream Anthropic SSE error frame and evaluates
	// provider health policy at most once for that error event.
	StreamFailure func(ctx context.Context, payload []byte, message string, clientOutputStarted bool) ConvertedAnthropicFailure
	// PreOutputReadFailure optionally converts a pre-semantic transport or
	// truncation error into an UpstreamFailoverError for outer provider switching.
	PreOutputReadFailure func(err error) error
	// HandleTimeout records provider stream-timeout health when an upstream stream
	// exceeds StreamInterval while the downstream client remains connected.
	HandleTimeout func(ctx context.Context, model string)
	// Warn writes structured non-fatal reader diagnostics when non-nil; a nil
	// callback falls back to logging.L().Warn so reader anomalies remain visible.
	Warn func(msg string, fields ...zap.Field)
}

// convertedAnthropicFrameInfo summarizes top-level type, stop_reason, and error
// fields scanned from one Anthropic SSE frame via jsontext.
type convertedAnthropicFrameInfo struct {
	eventType     string // resolved event type from JSON "type" or SSE event header
	errorType     string // nested error.type when eventType is "error"
	errorMessage  string // nested error.message when eventType is "error"
	sawStopReason bool   // true on message_stop or non-empty delta.stop_reason
}

// convertedAnthropicUsageCollector accumulates wirejson-guarded native billing
// usage and raw pre-conversion vendor model declarations across SSE frames.
type convertedAnthropicUsageCollector struct {
	usage     upstream.TokenUsage            // normalized native token buckets
	models    protocol.ResponseModelObserver // raw pre-conversion model observer
	identity  Result                         // static attempt metadata copied into built results
	startTime time.Time                      // attempt start timestamp for Duration
}

// ReadConvertedAnthropicBuffered folds an upstream Anthropic Messages SSE
// stream (or EventStream-bridged Bedrock stream) through exchange.Buffer into a
// single client-protocol JSON body while accumulating native billing usage and
// raw pre-conversion model observations.
//
// Upstream SSE error events run through StreamFailure once before any JSON
// bytes are written, returning Failover directly when eligible. Idle timeouts
// close resp.Body early to unblock blocked reads, while normal exits leave
// resp.Body closure to the caller.
func ReadConvertedAnthropicBuffered(
	ctx context.Context,
	resp *http.Response,
	c *upstream.OutputContext,
	exchange *convert.Exchange,
	options ConvertedAnthropicOutputOptions,
	originalModel, billingModel, upstreamModel string,
	reasoningEffort *string,
	startTime time.Time,
) (*Result, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("upstream response body is nil")
	}
	if exchange == nil {
		return nil, errors.New("conversion exchange is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if options.UpdateWindow != nil {
		options.UpdateWindow(ctx, resp.Header)
	}

	requestID := anthropicResponseRequestID(resp.Header)
	errTypeDefault := defaultAnthropicConvertedErrorType(options.DefaultErrorType, exchange.Info())
	collector := newConvertedAnthropicUsageCollector(resp, requestID, originalModel, billingModel, upstreamModel, reasoningEffort, startTime, false)
	buf := exchange.Buffer()

	sawMessageStart := false
	sawStop := false
	var immediateErr error
	var immediateFailover bool

	processFrame := func(frame sse.Frame) bool {
		trimmed := bytes.TrimSpace(frame.Data)
		info := inspectAnthropicSSEFrame(frame.Event, trimmed)

		if len(trimmed) > 0 {
			collector.models.ObserveAnthropic(trimmed)
			if options.Observe != nil {
				options.Observe(protocolanthropic.ObserveEvent(string(trimmed)))
			}
		}

		if info.eventType != "error" && (len(trimmed) == 0 || info.eventType == "ping" || upstream.IsSSEHeartbeatEvent(frame.Event, string(trimmed))) {
			return false
		}

		restoredPayload := trimmed
		if options.RestoreToolNames != nil && len(restoredPayload) > 0 {
			restoredPayload = options.RestoreToolNames(restoredPayload)
		}

		collector.Observe(ctx, frame.Event, restoredPayload, options.OverrideCache)

		if info.eventType == "error" {
			msg := strings.TrimSpace(info.errorMessage)
			if msg == "" {
				msg = strings.TrimSpace(upstream.ExtractErrorMessage(restoredPayload))
			}
			if msg == "" {
				msg = "Upstream stream error"
			}
			failure := classifyConvertedAnthropicStreamFailure(ctx, options, errTypeDefault, restoredPayload, info.errorType, msg, false)
			if failure.Failover != nil {
				immediateErr = failure.Failover
				immediateFailover = true
				return true
			}
			writeConvertedAnthropicUnstartedError(c, options, failure.Status, failure.Type, failure.Code, failure.Message)
			immediateErr = fmt.Errorf("upstream stream error: %s", failure.Message)
			return true
		}

		if writeErr := buf.Write(sse.Frame{Event: frame.Event, Data: restoredPayload}); writeErr != nil {
			warnConvertedAnthropic(options.Warn, "converted anthropic buffered: failed to fold event",
				zap.Error(writeErr),
				zap.String("request_id", requestID),
				zap.String("event_type", info.eventType),
			)
			return false
		}
		if info.eventType == "message_start" {
			sawMessageStart = true
		}
		if info.sawStopReason {
			sawStop = true
		}
		return false
	}

	scanErr := scanAnthropicSSEFrames(resp.Body, options.MaxFrameBytes, readAnthropicStreamInterval(options.StreamInterval), processFrame)
	if immediateErr != nil {
		if immediateFailover {
			return nil, immediateErr
		}
		return collector.PartialResult(), immediateErr
	}

	if errors.Is(scanErr, ErrAnthropicStreamIdle) {
		if options.HandleTimeout != nil {
			options.HandleTimeout(ctx, upstreamModel)
		}
		warnConvertedAnthropic(options.Warn, "converted anthropic buffered: data interval timeout",
			zap.String("request_id", requestID),
			zap.Duration("interval", readAnthropicStreamInterval(options.StreamInterval)),
		)
		writeConvertedAnthropicUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, "server_error", "Upstream stream data interval timeout")
		return collector.PartialResult(), scanErr
	}

	if scanErr != nil {
		if !errors.Is(scanErr, context.Canceled) && !errors.Is(scanErr, context.DeadlineExceeded) {
			warnConvertedAnthropic(options.Warn, "converted anthropic buffered: read error",
				zap.Error(scanErr),
				zap.String("request_id", requestID),
			)
		}
		// Retain a completed reply when stop_reason already arrived before a trailing
		// transport error, unless a frame exceeded the configured size cap.
		if !sawMessageStart || !sawStop || errors.Is(scanErr, sse.ErrFrameTooLarge) {
			if !errors.Is(scanErr, context.Canceled) && !errors.Is(scanErr, context.DeadlineExceeded) && options.PreOutputReadFailure != nil {
				if foErr := options.PreOutputReadFailure(scanErr); foErr != nil {
					return nil, foErr
				}
			}
			writeConvertedAnthropicUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, "upstream_stream_error", "Upstream stream could not be read completely")
			return collector.PartialResult(), scanErr
		}
	}

	if !sawMessageStart {
		if options.PreOutputReadFailure != nil {
			emptyErr := errors.New("upstream stream ended without response")
			if foErr := options.PreOutputReadFailure(emptyErr); foErr != nil {
				return nil, foErr
			}
		}
		writeConvertedAnthropicUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, "server_error", "Upstream stream ended without a response")
		return nil, errors.New("upstream stream ended without response")
	}

	clientJSON, closeErr := buf.Close(nil)
	logAnthropicExchangeLossesDebug(exchange, requestID)
	if closeErr != nil {
		if scanErr == nil && options.PreOutputReadFailure != nil {
			if foErr := options.PreOutputReadFailure(closeErr); foErr != nil {
				return nil, foErr
			}
		}
		writeConvertedAnthropicUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, "upstream_stream_error", "Upstream stream could not be read completely")
		if scanErr != nil {
			return collector.PartialResult(), scanErr
		}
		return collector.PartialResult(), closeErr
	}

	if c != nil && c.Writer != nil {
		if options.CopyHeaders != nil {
			options.CopyHeaders(c.Writer.Header(), resp.Header)
		}
		// Override copied upstream text/event-stream headers so downstream buffered
		// clients parse the reconstructed Responses or Chat payload as unary JSON.
		c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		c.Data(http.StatusOK, "application/json; charset=utf-8", clientJSON)
	}

	return collector.Result(false), scanErr
}

// ReadConvertedAnthropicStreaming converts an upstream Anthropic Messages SSE
// stream (or EventStream-bridged Bedrock stream) into client-protocol SSE frames
// via exchange.Stream while preserving leading-frame failover staging, deferred
// header commitment, and native billing usage collection.
//
// Leading non-semantic client frames stay buffered until the first Semantic==true
// frame or clean terminal arrives, letting pre-semantic upstream error frames
// switch providers without interleaving partial streams. After client write
// failures, upstream reading continues until completion or StreamInterval so
// trailing message_delta usage counters still reach billing.
func ReadConvertedAnthropicStreaming(
	ctx context.Context,
	resp *http.Response,
	c *upstream.OutputContext,
	exchange *convert.Exchange,
	options ConvertedAnthropicOutputOptions,
	originalModel, billingModel, upstreamModel string,
	reasoningEffort *string,
	startTime time.Time,
) (*Result, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("upstream response body is nil")
	}
	if exchange == nil {
		return nil, errors.New("conversion exchange is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if options.UpdateWindow != nil {
		options.UpdateWindow(ctx, resp.Header)
	}

	requestID := anthropicResponseRequestID(resp.Header)
	errTypeDefault := defaultAnthropicConvertedErrorType(options.DefaultErrorType, exchange.Info())
	collector := newConvertedAnthropicUsageCollector(resp, requestID, originalModel, billingModel, upstreamModel, reasoningEffort, startTime, true)
	stream := exchange.Stream()

	headersPrepared := false
	prepareStreamHeaders := func() {
		if headersPrepared || c == nil || c.Writer == nil {
			return
		}
		headersPrepared = true
		if options.CopyHeaders != nil {
			options.CopyHeaders(c.Writer.Header(), resp.Header)
		}
		c.Header("Content-Type", "text/event-stream")
		if c.Writer.Header().Get("Cache-Control") == "" {
			c.Header("Cache-Control", "no-cache")
		}
		if c.Writer.Header().Get("Connection") == "" {
			c.Header("Connection", "keep-alive")
		}
		c.Header("X-Accel-Buffering", "no")
		if requestID != "" && c.Writer.Header().Get("x-request-id") == "" {
			c.Header("x-request-id", requestID)
		}
		c.Writer.WriteHeader(http.StatusOK)
	}

	var stagedFrames []sse.Frame
	clientOutputStarted := false
	clientDisconnected := false
	var streamFailoverErr error
	var streamNonFailoverErr error

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
		if clientDisconnected || c == nil || c.Writer == nil {
			return false
		}
		prepareStreamHeaders()
		raw := sse.Append(nil, f)
		c.NextEvent(f.Semantic, f.Terminal)
		if _, err := c.Writer.Write(raw); err != nil {
			// Stop writing downstream after a socket failure while allowing the outer
			// scanner loop to keep draining trailing message_delta token counts.
			clientDisconnected = true
			logging.L().Debug("converted anthropic stream: client disconnected, continuing to drain upstream for billing",
				zap.Error(err),
				zap.String("request_id", requestID),
				zap.Int64("provider_id", options.ProviderID),
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
				return false
			}
		}
		stagedFrames = stagedFrames[:0]
		return true
	}

	stageOrWriteClientFrames := func(frames []sse.Frame) {
		if len(frames) == 0 || clientDisconnected {
			return
		}
		wroteAny := false
		for _, f := range frames {
			if !clientOutputStarted {
				stagedFrames = append(stagedFrames, f)
				if f.Semantic || (f.Terminal && stream.Summary().Err == nil) {
					if flushStagedFrames() {
						wroteAny = true
					}
				}
				continue
			}
			if writeSingleClientFrame(f) {
				wroteAny = true
			}
		}
		if wroteAny && !clientDisconnected && c != nil && c.Writer != nil {
			c.Writer.Flush()
		}
	}

	writeClosingErrorFrames := func(closingFrames []sse.Frame, fallback *ir.StreamError) {
		if clientDisconnected {
			return
		}
		if len(closingFrames) == 0 {
			if options.WriteLocalTerminalError != nil && fallback != nil {
				options.WriteLocalTerminalError(fallback.Status, fallback.Type, fallback.Code, fallback.Message)
			}
			return
		}
		prepareStreamHeaders()
		for i, f := range closingFrames {
			raw := sse.Append(nil, f)
			isLastTerminal := i == len(closingFrames)-1 && f.Terminal
			// Route the final terminal error frame through WriteRawTerminalSSE so
			// gateway-synthesized failures stay untimed while preserving sequence_number.
			if isLastTerminal && options.WriteRawTerminalSSE != nil && options.WriteRawTerminalSSE(raw) {
				clientOutputStarted = true
				continue
			}
			if c == nil || c.Writer == nil {
				return
			}
			c.NextEvent(f.Semantic, f.Terminal)
			if _, err := c.Writer.Write(raw); err != nil {
				clientDisconnected = true
				return
			}
			clientOutputStarted = true
		}
		if !clientDisconnected && c != nil && c.Writer != nil {
			c.Writer.Flush()
		}
	}

	emitStartedAbortFrames := func(cause *ir.StreamError) {
		if clientDisconnected {
			return
		}
		closingFrames, closeErr := stream.Close(cause)
		logAnthropicExchangeLossesDebug(exchange, requestID)
		// Suppress fallback terminal error frames when stream.Close reports a
		// clean prior finish without error so completed responses never append
		// a duplicate response.failed or error chunk after [DONE].
		if closeErr == nil && len(closingFrames) == 0 && stream.Summary().Finish.Reason != "" && stream.Summary().Err == nil {
			return
		}
		// Fall back to WriteLocalTerminalError when stream.Close failed or emitted
		// no frames on an unfinished stream so started clients receive one error frame.
		if closeErr != nil || len(closingFrames) == 0 {
			if options.WriteLocalTerminalError != nil {
				options.WriteLocalTerminalError(cause.Status, cause.Type, cause.Code, cause.Message)
			}
			return
		}
		writeClosingErrorFrames(closingFrames, cause)
	}

	deliverFailure := func(status int, errType, errCode, errMsg string) {
		if clientDisconnected {
			return
		}
		if errType == "" {
			errType = errTypeDefault
		}
		if status <= 0 {
			status = http.StatusBadGateway
		}
		cause := &ir.StreamError{
			Type:    errType,
			Code:    errCode,
			Message: errMsg,
			Status:  status,
		}
		if clientOutputStarted {
			emitStartedAbortFrames(cause)
			return
		}
		stagedFrames = stagedFrames[:0]
		_, _ = stream.Close(cause)
		logAnthropicExchangeLossesDebug(exchange, requestID)
		if isSSECommitted() && options.WriteLocalTerminalError != nil {
			options.WriteLocalTerminalError(status, errType, errCode, errMsg)
			clientOutputStarted = true
			return
		}
		writeConvertedAnthropicUnstartedError(c, options, status, errType, errCode, errMsg)
		clientOutputStarted = true
	}

	deliverDecoderCloseError := func(closingFrames []sse.Frame, sumErr *ir.StreamError) {
		errType := errTypeDefault
		if sumErr.Type != "" && sumErr.Type != "stream_error" {
			errType = sumErr.Type
		}
		status := sumErr.Status
		if status <= 0 {
			status = http.StatusBadGateway
		}
		fallback := &ir.StreamError{
			Type:    errType,
			Code:    sumErr.Code,
			Message: sumErr.Message,
			Status:  status,
		}
		if clientOutputStarted {
			writeClosingErrorFrames(closingFrames, fallback)
			return
		}
		stagedFrames = stagedFrames[:0]
		if isSSECommitted() && options.WriteLocalTerminalError != nil {
			options.WriteLocalTerminalError(status, errType, sumErr.Code, sumErr.Message)
			clientOutputStarted = true
			return
		}
		writeConvertedAnthropicUnstartedError(c, options, status, errType, sumErr.Code, sumErr.Message)
		clientOutputStarted = true
	}

	sawMessageStart := false
	sawStop := false

	processFrame := func(frame sse.Frame) bool {
		trimmed := bytes.TrimSpace(frame.Data)
		info := inspectAnthropicSSEFrame(frame.Event, trimmed)

		if len(trimmed) > 0 {
			collector.models.ObserveAnthropic(trimmed)
			if options.Observe != nil {
				options.Observe(protocolanthropic.ObserveEvent(string(trimmed)))
			}
		}

		if info.eventType != "error" && (len(trimmed) == 0 || info.eventType == "ping" || upstream.IsSSEHeartbeatEvent(frame.Event, string(trimmed))) {
			return false
		}

		restoredPayload := trimmed
		if options.RestoreToolNames != nil && len(restoredPayload) > 0 {
			restoredPayload = options.RestoreToolNames(restoredPayload)
		}

		collector.Observe(ctx, frame.Event, restoredPayload, options.OverrideCache)

		if info.eventType == "error" {
			if clientDisconnected {
				// End post-disconnect usage draining quietly without attributing an
				// upstream error penalty after the downstream client already left.
				return true
			}
			msg := strings.TrimSpace(info.errorMessage)
			if msg == "" {
				msg = strings.TrimSpace(upstream.ExtractErrorMessage(restoredPayload))
			}
			if msg == "" {
				msg = "Upstream stream error"
			}
			failure := classifyConvertedAnthropicStreamFailure(ctx, options, errTypeDefault, restoredPayload, info.errorType, msg, isContentWritten())
			if failure.Failover != nil {
				stagedFrames = stagedFrames[:0]
				streamFailoverErr = failure.Failover
				return true
			}
			deliverFailure(failure.Status, failure.Type, failure.Code, failure.Message)
			streamNonFailoverErr = fmt.Errorf("upstream stream error: %s", failure.Message)
			return true
		}

		clientFrames, convErr := stream.Write(sse.Frame{Event: frame.Event, Data: restoredPayload})
		if convErr != nil {
			warnConvertedAnthropic(options.Warn, "converted anthropic stream: failed to decode event",
				zap.Error(convErr),
				zap.String("request_id", requestID),
				zap.String("event_type", info.eventType),
			)
			return false
		}
		if info.eventType == "message_start" {
			sawMessageStart = true
		}
		if info.sawStopReason {
			sawStop = true
		}
		if clientDisconnected {
			return false
		}
		stageOrWriteClientFrames(clientFrames)
		return false
	}

	scanErr := scanAnthropicSSEFrames(resp.Body, options.MaxFrameBytes, readAnthropicStreamInterval(options.StreamInterval), processFrame)

	if streamFailoverErr != nil {
		return nil, streamFailoverErr
	}
	if streamNonFailoverErr != nil {
		return collector.Result(clientDisconnected), streamNonFailoverErr
	}

	if errors.Is(scanErr, ErrAnthropicStreamIdle) {
		if clientDisconnected {
			return collector.Result(true), nil
		}
		if options.HandleTimeout != nil {
			options.HandleTimeout(ctx, upstreamModel)
		}
		warnConvertedAnthropic(options.Warn, "converted anthropic stream: data interval timeout",
			zap.String("request_id", requestID),
			zap.Duration("interval", readAnthropicStreamInterval(options.StreamInterval)),
		)
		if !isContentWritten() && options.PreOutputReadFailure != nil {
			if foErr := options.PreOutputReadFailure(scanErr); foErr != nil {
				return nil, foErr
			}
		}
		deliverFailure(http.StatusBadGateway, errTypeDefault, "upstream_stream_error", "Upstream stream data interval timeout")
		return collector.Result(clientDisconnected), scanErr
	}

	if scanErr != nil {
		if clientDisconnected {
			// A transport read error during post-disconnect usage draining finishes
			// with already accumulated tokens rather than reporting an upstream error.
			return collector.Result(true), nil
		}
		if !errors.Is(scanErr, context.Canceled) && !errors.Is(scanErr, context.DeadlineExceeded) {
			warnConvertedAnthropic(options.Warn, "converted anthropic stream: read error",
				zap.Error(scanErr),
				zap.String("request_id", requestID),
			)
		}
		// Retain a completed or stop_reason-finalized reply when a trailing socket
		// error arrives before EOF, unless a frame exceeded the configured size cap.
		if !sawMessageStart || !sawStop || errors.Is(scanErr, sse.ErrFrameTooLarge) {
			if errors.Is(scanErr, context.Canceled) || errors.Is(scanErr, context.DeadlineExceeded) {
				if clientOutputStarted {
					emitStartedAbortFrames(&ir.StreamError{
						Type:    errTypeDefault,
						Code:    "upstream_stream_error",
						Message: "Stream interrupted before completion",
						Status:  http.StatusBadGateway,
					})
				} else if isSSECommitted() && options.WriteLocalTerminalError != nil {
					_, _ = stream.Close(scanErr)
					logAnthropicExchangeLossesDebug(exchange, requestID)
					options.WriteLocalTerminalError(http.StatusBadGateway, errTypeDefault, "upstream_stream_error", "Stream interrupted before completion")
				}
				return collector.Result(clientDisconnected), scanErr
			}
			if !isContentWritten() && !errors.Is(scanErr, sse.ErrFrameTooLarge) && options.PreOutputReadFailure != nil {
				if foErr := options.PreOutputReadFailure(scanErr); foErr != nil {
					return nil, foErr
				}
			}
			deliverFailure(http.StatusBadGateway, errTypeDefault, "upstream_stream_error", "Upstream stream could not be read completely")
			return collector.Result(clientDisconnected), scanErr
		}
	}

	closingFrames, closeErr := stream.Close(nil)
	logAnthropicExchangeLossesDebug(exchange, requestID)
	if closeErr != nil {
		warnConvertedAnthropic(options.Warn, "converted anthropic stream: close error",
			zap.Error(closeErr),
			zap.String("request_id", requestID),
		)
	}
	if sumErr := stream.Summary().Err; sumErr != nil {
		if clientDisconnected {
			return collector.Result(true), nil
		}
		truncErr := fmt.Errorf("upstream stream incomplete: %s", sumErr.Message)
		if !isContentWritten() && options.PreOutputReadFailure != nil {
			if foErr := options.PreOutputReadFailure(truncErr); foErr != nil {
				return nil, foErr
			}
		}
		deliverDecoderCloseError(closingFrames, sumErr)
		return collector.Result(clientDisconnected), truncErr
	}

	if !clientDisconnected {
		if !clientOutputStarted {
			stagedFrames = append(stagedFrames, closingFrames...)
			if flushStagedFrames() && c != nil && c.Writer != nil {
				c.Writer.Flush()
			}
		} else if len(closingFrames) > 0 {
			wroteClosing := false
			for _, f := range closingFrames {
				if !writeSingleClientFrame(f) {
					break
				}
				wroteClosing = true
			}
			if wroteClosing && !clientDisconnected && c != nil && c.Writer != nil {
				c.Writer.Flush()
			}
		}
	}

	if clientDisconnected {
		return collector.Result(true), nil
	}
	return collector.Result(false), scanErr
}

// scanAnthropicSSEFrames scans body via sse.NewReaderLimit and invokes onFrame
// for each complete SSE frame. Positive intervals spawn a reader goroutine and
// close body on idle expiry so blocked network or EventStream reads wake promptly.
func scanAnthropicSSEFrames(
	body io.ReadCloser,
	maxFrameBytes int,
	interval time.Duration,
	onFrame func(sse.Frame) bool,
) error {
	reader := sse.NewReaderLimit(body, maxFrameBytes)
	// Scan synchronously when upstream idle polling is disabled to avoid
	// allocating channels and spawning a goroutine on unpolled streams.
	if interval <= 0 {
		for reader.Scan() {
			if onFrame(reader.Frame()) {
				return nil
			}
		}
		return reader.Err()
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// scannedSSEEvent pairs one complete SSE frame or terminal scanner error.
	type scannedSSEEvent struct {
		frame sse.Frame // complete scanned SSE frame
		err   error     // non-EOF reader error when non-nil
	}
	events := make(chan scannedSSEEvent, 16)
	done := make(chan struct{})
	var lastReadAt atomic.Int64
	lastReadAt.Store(time.Now().UnixNano())

	sendEvent := func(ev scannedSSEEvent) bool {
		select {
		case events <- ev:
			return true
		case <-done:
			return false
		}
	}

	// Stamp lastReadAt when each upstream frame finishes scanning so slow
	// downstream client writes never trigger a false upstream idle timeout.
	go func() {
		defer close(events)
		for reader.Scan() {
			lastReadAt.Store(time.Now().UnixNano())
			if !sendEvent(scannedSSEEvent{frame: reader.Frame()}) {
				return
			}
		}
		if err := reader.Err(); err != nil {
			_ = sendEvent(scannedSSEEvent{err: err})
		}
	}()
	defer close(done)

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return nil
			}
			if ev.err != nil {
				return ev.err
			}
			if onFrame(ev.frame) {
				return nil
			}
		case <-ticker.C:
			lastRead := time.Unix(0, lastReadAt.Load())
			if time.Since(lastRead) < interval {
				continue
			}
			// Close the upstream body to unblock a goroutine waiting inside Read
			// before returning the idle timeout sentinel to the caller.
			_ = body.Close()
			return ErrAnthropicStreamIdle
		}
	}
}

// newConvertedAnthropicUsageCollector initializes native usage and raw model
// tracking for one converted Messages response attempt.
func newConvertedAnthropicUsageCollector(
	resp *http.Response,
	requestID string,
	originalModel, billingModel, upstreamModel string,
	reasoningEffort *string,
	startTime time.Time,
	stream bool,
) *convertedAnthropicUsageCollector {
	var headers http.Header
	if resp != nil {
		headers = resp.Header
	}
	return &convertedAnthropicUsageCollector{
		identity: Result{
			RequestID:        requestID,
			Headers:          headers,
			Model:            originalModel,
			BillingModel:     billingModel,
			UpstreamModel:    upstreamModel,
			UpstreamEndpoint: "/v1/messages",
			ReasoningEffort:  reasoningEffort,
			Stream:           stream,
		},
		startTime: startTime,
	}
}

// Observe validates payload with wirejson.ValidateUsageEnvelope and merges
// normalized Anthropic token buckets into c.usage, applying overrideCache when
// present. Invalid JSON or ambiguous duplicate usage keys leave c.usage unchanged.
func (c *convertedAnthropicUsageCollector) Observe(
	ctx context.Context,
	eventHeader string,
	payload []byte,
	overrideCache func(context.Context) (string, bool),
) {
	if len(payload) == 0 || wirejson.ValidateUsageEnvelope(payload) != nil {
		return
	}
	var event protocolanthropic.AnthropicStreamEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return
	}
	if event.Type == "" {
		event.Type = strings.TrimSpace(eventHeader)
	}
	if protocolanthropic.NormalizeStreamUsage(payload, &event, &c.usage) && overrideCache != nil {
		if target, ok := overrideCache(ctx); ok {
			protocol.ApplyCacheTTLOverride(&c.usage, target)
		}
	}
}

// Result builds a detached Result snapshot carrying inclusive OpenAI Usage, a
// separate NativeUsage copy, and the pre-conversion UpstreamResponseModel.
// Later collector updates do not mutate previously returned Result values.
func (c *convertedAnthropicUsageCollector) Result(clientDisconnect bool) *Result {
	out := c.identity
	usageCopy := c.usage
	out.Usage = AnthropicUsageToOpenAI(&usageCopy)
	out.NativeUsage = &usageCopy
	out.UpstreamResponseModel = c.models.Model()
	out.Duration = time.Since(c.startTime)
	out.ClientDisconnect = clientDisconnect
	return &out
}

// PartialResult returns c.Result(false) when positive token counters or a raw
// upstream model declaration were observed prior to buffered failure. Absent
// usage and model observations return nil so unstarted failures record nothing.
func (c *convertedAnthropicUsageCollector) PartialResult() *Result {
	if c.usage == (upstream.TokenUsage{}) && c.models.Model() == "" {
		return nil
	}
	return c.Result(false)
}

// inspectAnthropicSSEFrame scans eventHeader and raw JSON payload bytes via
// jsontext to extract the effective event type, nested error details, and
// stop_reason presence without map or gjson decoding. Empty or non-object
// payloads retain the trimmed SSE eventHeader with empty error metadata.
func inspectAnthropicSSEFrame(eventHeader string, payload []byte) convertedAnthropicFrameInfo {
	out := convertedAnthropicFrameInfo{
		eventType: strings.TrimSpace(eventHeader),
	}
	if len(payload) == 0 {
		return out
	}
	dec := jsontext.NewDecoder(bytes.NewReader(payload), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return out
	}
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			break
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			break
		}
		switch key {
		case "type":
			var typ string
			if jsonv2.Unmarshal(val, &typ, jsontext.AllowInvalidUTF8(true)) == nil && strings.TrimSpace(typ) != "" {
				out.eventType = strings.TrimSpace(typ)
			}
		case "error":
			out.errorType, out.errorMessage = parseAnthropicNestedErrorObject(val)
		case "delta":
			if parseAnthropicDeltaHasStopReason(val) {
				out.sawStopReason = true
			}
		}
	}
	if out.eventType == "message_stop" {
		out.sawStopReason = true
	}
	return out
}

// parseAnthropicNestedErrorObject extracts trimmed type and message strings
// from a raw Anthropic error JSON object via jsontext. Empty, non-object, or
// malformed values return empty strings for any unread fields.
func parseAnthropicNestedErrorObject(raw jsontext.Value) (string, string) {
	if len(raw) == 0 || raw.Kind() != '{' {
		return "", ""
	}
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return "", ""
	}
	var errType, errMsg string
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			break
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			break
		}
		switch key {
		case "type":
			_ = jsonv2.Unmarshal(val, &errType, jsontext.AllowInvalidUTF8(true))
		case "message":
			_ = jsonv2.Unmarshal(val, &errMsg, jsontext.AllowInvalidUTF8(true))
		}
	}
	return strings.TrimSpace(errType), strings.TrimSpace(errMsg)
}

// parseAnthropicDeltaHasStopReason reports whether a raw message_delta.delta
// JSON object contains a non-empty string stop_reason field. Empty slices,
// non-object values, null stop reasons, or malformed tokens return false without error.
func parseAnthropicDeltaHasStopReason(raw jsontext.Value) bool {
	if len(raw) == 0 || raw.Kind() != '{' {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return false
	}
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return false
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return false
		}
		if key == "stop_reason" {
			var reason string
			if jsonv2.Unmarshal(val, &reason, jsontext.AllowInvalidUTF8(true)) == nil && strings.TrimSpace(reason) != "" {
				return true
			}
		}
	}
	return false
}

// classifyConvertedAnthropicStreamFailure delegates to options.StreamFailure
// when present and fills non-positive status, empty type, or empty message
// defaults on non-failover results. Absent callbacks return HTTP 502 with
// rawErrType or defaultErrType so unit callers still emit structured errors.
func classifyConvertedAnthropicStreamFailure(
	ctx context.Context,
	options ConvertedAnthropicOutputOptions,
	defaultErrType string,
	payload []byte,
	rawErrType, message string,
	clientOutputStarted bool,
) ConvertedAnthropicFailure {
	if options.StreamFailure != nil {
		out := options.StreamFailure(ctx, payload, message, clientOutputStarted)
		if out.Failover != nil {
			return out
		}
		if out.Status <= 0 {
			out.Status = http.StatusBadGateway
		}
		if out.Type == "" {
			out.Type = defaultErrType
		}
		if out.Message == "" {
			out.Message = message
		}
		return out
	}
	errType := strings.TrimSpace(rawErrType)
	if errType == "" {
		errType = defaultErrType
	}
	return ConvertedAnthropicFailure{
		Status:  http.StatusBadGateway,
		Type:    errType,
		Code:    strings.TrimSpace(rawErrType),
		Message: message,
	}
}

// writeConvertedAnthropicUnstartedError writes a client error before semantic
// content starts, promoting to WriteLocalTerminalError when CommittedSSE is
// true. When neither callback is installed on an unwritten writer, it encodes a
// typed JSON error envelope directly via jsonv2.
func writeConvertedAnthropicUnstartedError(
	c *upstream.OutputContext,
	options ConvertedAnthropicOutputOptions,
	status int,
	errType, errCode, errMsg string,
) {
	if options.CommittedSSE != nil && options.CommittedSSE() && options.WriteLocalTerminalError != nil {
		options.WriteLocalTerminalError(status, errType, errCode, errMsg)
		return
	}
	if options.WriteError != nil {
		options.WriteError(status, errType, errCode, errMsg)
		return
	}
	if c != nil && c.Writer != nil && !c.Writer.Written() {
		// fallbackErrorDetail serializes the inner OpenAI error object when no WriteError callback is installed.
		type fallbackErrorDetail struct {
			Type    string `json:"type"`          // error category string
			Code    string `json:"code,omitzero"` // optional machine-readable error code
			Message string `json:"message"`       // client-facing error message
		}
		// fallbackErrorEnvelope wraps fallbackErrorDetail under the top-level OpenAI JSON "error" key.
		type fallbackErrorEnvelope struct {
			Error fallbackErrorDetail `json:"error"` // top-level OpenAI error envelope
		}
		payload, err := jsonv2.Marshal(fallbackErrorEnvelope{
			Error: fallbackErrorDetail{
				Type:    errType,
				Code:    errCode,
				Message: errMsg,
			},
		})
		if err == nil {
			c.Data(status, "application/json; charset=utf-8", payload)
		}
	}
}

// anthropicResponseRequestID returns trimmed x-request-id from headers or falls
// back to Bedrock's x-amzn-requestid correlation header when x-request-id is
// absent. Nil header maps return an empty string.
func anthropicResponseRequestID(headers http.Header) string {
	if headers == nil {
		return ""
	}
	if id := strings.TrimSpace(headers.Get("x-request-id")); id != "" {
		return id
	}
	return strings.TrimSpace(headers.Get("x-amzn-requestid"))
}

// readAnthropicStreamInterval invokes fn when non-nil and returns positive idle
// timeout durations for SSE frame polling. Nil callbacks or non-positive
// durations return zero so scanAnthropicSSEFrames runs synchronously without timers.
func readAnthropicStreamInterval(fn func() time.Duration) time.Duration {
	if fn == nil {
		return 0
	}
	if d := fn(); d > 0 {
		return d
	}
	return 0
}

// defaultAnthropicConvertedErrorType returns trimmed configured when non-empty
// for client error envelopes. Blank or whitespace-only configuration defaults
// to "server_error" across OpenAI Responses and Chat Completions client protocols.
func defaultAnthropicConvertedErrorType(configured string, _ convert.RequestInfo) string {
	if trimmed := strings.TrimSpace(configured); trimmed != "" {
		return trimmed
	}
	return "server_error"
}

// warnConvertedAnthropic routes structured reader warnings to customWarn when
// provided by caller options. Nil callbacks fall back to logging.L().Warn so
// unexpected stream anomalies still surface in gateway logs.
func warnConvertedAnthropic(customWarn func(string, ...zap.Field), msg string, fields ...zap.Field) {
	if customWarn != nil {
		customWarn(msg, fields...)
		return
	}
	logging.L().Warn(msg, fields...)
}

// logAnthropicExchangeLossesDebug logs total and per-kind IR conversion Loss
// counts at debug level after response conversion completes. Nil exchanges or
// zero recorded losses return immediately without allocating a summary map.
func logAnthropicExchangeLossesDebug(exchange *convert.Exchange, requestID string) {
	if exchange == nil {
		return
	}
	losses := exchange.Losses()
	if len(losses) == 0 {
		return
	}
	counts := make(map[string]int, 4)
	for _, loss := range losses {
		counts[string(loss.Kind)]++
	}
	logging.L().Debug("anthropic protocol conversion losses recorded",
		zap.String("request_id", requestID),
		zap.Int("loss_count", len(losses)),
		zap.Any("losses_by_kind", counts),
	)
}
