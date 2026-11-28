package qoder

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// response_converted.go reads streaming and buffered Qoder COSY SSE responses
// through convert.Exchange for Anthropic Messages, OpenAI Responses, and OpenAI
// Chat Completions while preserving raw billing usage extraction, pre-conversion
// upstream model observation, wrapped APIError classification, and pre-semantic
// failover staging. Flushing a leading non-semantic frame such as message_start,
// response.created, or an opening assistant role chunk before model content
// arrives would close outer provider failover prematurely if the next frame
// carries a retryable upstream error.
//
// ReadConvertedStreaming holds leading non-semantic client frames in memory
// until the first Semantic==true frame or normal completion terminal arrives,
// commits HTTP 200 SSE headers lazily via NewDeferredOutputContext, emits
// untimed terminal SSE errors when early keepalive heartbeats already committed
// SSE headers, closes open blocks via Stream.Close(cause) on mid-stream aborts,
// and drains trailing upstream usage frames after client disconnection.

// ConvertedResponseOptions binds a convert.Exchange and gateway SSE commitment
// callbacks for ReadConvertedStreaming and ReadConvertedBuffered.
type ConvertedResponseOptions struct {
	Exchange  *convert.Exchange // per-attempt IR request and response converter
	RequestID string            // correlation identifier for headers and debug logs

	// CommittedSSE reports whether downstream HTTP 200 text/event-stream headers
	// were already committed before semantic output began. Nil falls back to
	// inspecting c.Writer.Written directly.
	CommittedSSE func() bool
	// DeferRetryableCommittedError leaves pre-semantic refreshable or switchable
	// upstream errors unwritten on an early keepalive-committed SSE stream so
	// UnifiedTextExecutor can refresh credentials or fail over to another provider.
	DeferRetryableCommittedError bool
	// WriteLocalTerminalError writes an untimed terminal SSE error frame in the
	// inbound client protocol when an unstarted stream fails after SSE commitment.
	WriteLocalTerminalError func(status int, errType, code, message string)
	// WriteRawTerminalSSE delivers pre-encoded terminal SSE frame bytes from
	// Stream.Close(cause) through the gateway's untimed terminal SSE barrier.
	WriteRawTerminalSSE func(data []byte) bool
}

// QoderConvertedResult carries observed native billing usage, service flags,
// pre-conversion model declarations, and client disconnect evidence from one
// converted Qoder response read.
type QoderConvertedResult struct {
	Usage                 upstream.TokenUsage           // normalized native billing token buckets
	HasUsage              bool                          // true when at least one usage envelope arrived
	Served                bool                          // true when semantic output or buffered completion succeeded
	UpstreamResponseModel string                        // first non-empty raw model declared by Qoder before conversion
	ClientDisconnect      bool                          // true when downstream client writes failed during delivery
	ProtocolConversion    *telemetry.ProtocolConversion // bounded protocol transformation diagnostics
}

// wireConvertedSSEWrapper decodes the outer Qoder COSY SSE JSON envelope via
// json/v2 without coercing body payloads through map[string]any.
type wireConvertedSSEWrapper struct {
	Body string `json:"body,omitzero"` // stringified inner JSON payload or "[DONE]"
	// StatusCodeValue, StatusCode, and Status carry wrapper HTTP status codes
	// across Qoder gateway versions; parseConvertedWrapperStatus inspects them in
	// declaration order and selects the first positive integer status.
	StatusCodeValue jsontext.Value `json:"statusCodeValue,omitzero"` // primary wrapper HTTP status
	StatusCode      jsontext.Value `json:"statusCode,omitzero"`      // fallback wrapper status code or label
	Status          jsontext.Value `json:"status,omitzero"`          // secondary wrapper status code or label
}

// wireConvertedSSEInner inspects raw inner Qoder SSE JSON before conversion for
// declared model identity, event labels, and wirejson-validated usage envelopes.
type wireConvertedSSEInner struct {
	Model string         `json:"model,omitzero"` // optional vendor-declared response model
	Event string         `json:"event,omitzero"` // Anthropic-like inner event discriminator
	Type  string         `json:"type,omitzero"`  // fallback inner event type discriminator
	Data  jsontext.Value `json:"data,omitzero"`  // nested event payload for message_delta usage
	Usage jsontext.Value `json:"usage,omitzero"` // top-level inner usage object
}

// wireConvertedMessageDelta inspects nested message_delta data objects for
// trailing Qoder usage updates.
type wireConvertedMessageDelta struct {
	Usage jsontext.Value `json:"usage,omitzero"` // nested message_delta usage object
}

// wireConvertedUsage decodes Qoder prompt, completion, and cached token fields
// using raw JSON values so string, integer, and float tokens parse losslessly.
type wireConvertedUsage struct {
	// PromptTokens, InputTokens, CompletionTokens, and OutputTokens carry OpenAI
	// and Anthropic token count spellings; mergeUsageJSON prefers PromptTokens
	// and CompletionTokens when positive before falling back to InputTokens and OutputTokens.
	PromptTokens        jsontext.Value `json:"prompt_tokens,omitzero"`         // OpenAI-style inclusive prompt token count
	InputTokens         jsontext.Value `json:"input_tokens,omitzero"`          // Anthropic-style fallback input token count
	CompletionTokens    jsontext.Value `json:"completion_tokens,omitzero"`     // OpenAI-style completion token count
	OutputTokens        jsontext.Value `json:"output_tokens,omitzero"`         // Anthropic-style fallback output token count
	PromptTokensDetails jsontext.Value `json:"prompt_tokens_details,omitzero"` // nested cached prompt token breakdown
}

// wireConvertedPromptDetails decodes Qoder cached prompt token counts inside
// prompt_tokens_details.
type wireConvertedPromptDetails struct {
	CachedTokens jsontext.Value `json:"cached_tokens,omitzero"` // cached prompt token subset
}

// qoderRawFrameObserver accumulates raw native billing usage and captures the
// first non-empty upstream-declared model string before IR conversion runs.
type qoderRawFrameObserver struct {
	// usage and sawUsage track merged native billing buckets independently of
	// nonzero token totals so an explicit zero-count usage envelope still reports HasUsage.
	usage         upstream.TokenUsage // merged disjoint billing token buckets
	sawUsage      bool                // whether at least one non-null usage object arrived
	upstreamModel string              // first non-empty raw model declared by Qoder
}

// qoderScanStep carries one SSE frame read or terminal scanner status from
// qoderFrameScanner to its owning reader goroutine.
type qoderScanStep struct {
	// frame, err, and ok encode one reader.Scan outcome: ok is true with nil err
	// when frame holds a scanned SSE event, whereas ok is false at clean EOF or
	// alongside a non-nil scanner err.
	frame sse.Frame // scanned SSE frame when ok is true
	err   error     // scanner I/O or line-limit failure
	ok    bool      // true when frame holds a newly scanned event
}

// qoderFrameScanner steps sse.Reader one frame at a time on demand so normal
// stream completion and frame-level errors stop scanning before resp.Body closes
// while blocked reads still unblock on context cancellation.
type qoderFrameScanner struct {
	// wantNext, out, stop, done, and inFlight coordinate single-step SSE reads:
	// the owner sends on wantNext, sets inFlight until out delivers, and closes
	// stop during close while the scanner goroutine closes done on exit.
	wantNext chan struct{}      // unbuffered request token for one scan step
	out      chan qoderScanStep // unbuffered channel delivering the completed step
	stop     chan struct{}      // owner-closed signal terminating a parked scanner
	done     chan struct{}      // scanner-closed signal confirming goroutine exit
	inFlight bool               // true while a requested Scan step awaits out
}

// ReadConvertedStreaming reads a Qoder COSY SSE response stream through
// opts.Exchange.Stream, stages leading non-semantic frames until semantic
// progress or normal completion arrives, and writes client SSE frames through c.
// The supplied resp.Body is always closed before ReadConvertedStreaming returns.
//
// When downstream client writes fail mid-stream, ReadConvertedStreaming stops
// further client emission and continues draining upstream frames within ctx to
// capture final billing usage. Pre-semantic errors discard staged frames and
// preserve wrapped *APIError classifications for refresh or failover, while
// post-semantic failures close open client blocks and route their terminal SSE
// error frame through opts.WriteRawTerminalSSE when configured.
func ReadConvertedStreaming(
	ctx context.Context,
	c *upstream.OutputContext,
	resp *http.Response,
	opts ConvertedResponseOptions,
) (result *QoderConvertedResult, err error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("qoder response body is nil")
	}
	if opts.Exchange == nil {
		CloseQoderResponse(resp)
		return nil, errors.New("qoder conversion exchange is nil")
	}
	if ctx == nil {
		if resp.Request != nil {
			ctx = resp.Request.Context()
		} else {
			ctx = context.Background()
		}
	}

	scanner := newQoderFrameScanner(resp.Body)
	defer scanner.close(resp)
	defer func() {
		snap := upstream.FinalizeExchangeConversion(ctx, opts.Exchange)
		if result != nil {
			result.ProtocolConversion = snap
		}
	}()

	var headersOnce sync.Once
	writeStreamHeaders := func() {
		headersOnce.Do(func() {
			if opts.RequestID != "" {
				c.Header("x-request-id", opts.RequestID)
			}
			c.Header("Content-Type", "text/event-stream")
			c.Header("Cache-Control", "no-cache")
			c.Header("Connection", "keep-alive")
			c.Header("X-Accel-Buffering", "no")
			c.Status(http.StatusOK)
		})
	}

	isCommittedSSE := func() bool {
		if opts.CommittedSSE != nil && opts.CommittedSSE() {
			return true
		}
		return c != nil && c.Writer != nil && c.Writer.Written()
	}

	stream := opts.Exchange.Stream()
	var rawObs qoderRawFrameObserver
	var stagedFrames []sse.Frame
	stagedHasSemantic := false
	clientOutputStarted := false
	clientDisconnected := false

	writeSingleClientFrame := func(f sse.Frame) bool {
		if clientDisconnected {
			return false
		}
		writeStreamHeaders()
		raw := sse.Append(nil, f)
		c.NextEvent(f.Semantic, f.Terminal)
		if _, err := c.Writer.Write(raw); err != nil {
			clientDisconnected = true
			logging.L().Info("qoder converted stream: client disconnected, continuing to drain upstream for billing",
				zap.String("request_id", opts.RequestID),
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

	deliverStartedAbortFrames := func(closingFrames []sse.Frame) {
		if clientDisconnected || len(closingFrames) == 0 {
			return
		}
		wroteAny := false
		for i, f := range closingFrames {
			isFinalTerminal := f.Terminal && i == len(closingFrames)-1
			if isFinalTerminal && opts.WriteRawTerminalSSE != nil {
				writeStreamHeaders()
				c.Writer.WriteHeaderNow()
				raw := sse.Append(nil, f)
				// Route the terminal SSE error frame through the gateway's untimed
				// terminal barrier so idle keepalive heartbeats stop without skewing
				// streaming throughput timing; fall back to c.Writer when unavailable.
				if opts.WriteRawTerminalSSE(raw) {
					return
				}
			}
			if writeSingleClientFrame(f) {
				wroteAny = true
			}
		}
		if wroteAny && !clientDisconnected {
			c.Writer.Flush()
		}
	}

	emitPreOutputCommittedTerminalError := func(cause error) {
		if !isCommittedSSE() || opts.WriteLocalTerminalError == nil {
			return
		}
		// Leave early keepalive-committed SSE streams untouched on pre-semantic
		// refreshable or switchable errors when outer gateway orchestration will
		// attempt credential refresh or provider failover first.
		if opts.DeferRetryableCommittedError && ctx.Err() == nil && (MayRefreshAttempt(cause) || MaySwitchAttempt(cause)) {
			return
		}
		status, errType, code, msg := classifyQoderLocalTerminalError(cause)
		opts.WriteLocalTerminalError(status, errType, code, msg)
	}

	var readErr error
	for {
		step, ctxErr := scanner.next(ctx)
		if ctxErr != nil {
			readErr = ctxErr
			break
		}
		if step.err != nil {
			readErr = step.err
			break
		}
		if !step.ok {
			break
		}

		if obsErr := rawObs.observe(step.frame); obsErr != nil {
			readErr = translateProtocolQoderError(obsErr)
			break
		}

		// Keep draining upstream SSE frames after downstream client disconnect so
		// final billing usage settles without writing further client frames.
		if !clientDisconnected {
			batch, wErr := stream.Write(step.frame)
			if wErr != nil {
				readErr = translateProtocolQoderError(wErr)
				break
			}
			stageOrWriteClientFrames(batch)
		}

		if isQoderExplicitDoneFrame(step.frame) {
			break
		}
	}

	if clientDisconnected {
		// Finalize converter loss accounting on a disconnected client while
		// suppressing downstream terminal error writes.
		_, _ = stream.Close(errors.New("client disconnected"))
		logQoderExchangeLossesDebug(opts.Exchange, opts.RequestID)
		if readErr != nil {
			return &QoderConvertedResult{
				Usage:                 rawObs.usage,
				HasUsage:              rawObs.sawUsage,
				Served:                clientOutputStarted || stream.Summary().HasContent,
				UpstreamResponseModel: rawObs.upstreamModel,
				ClientDisconnect:      true,
			}, readErr
		}
		return &QoderConvertedResult{
			Usage:                 rawObs.usage,
			HasUsage:              rawObs.sawUsage,
			Served:                true,
			UpstreamResponseModel: rawObs.upstreamModel,
			ClientDisconnect:      true,
		}, nil
	}

	if readErr != nil {
		if clientOutputStarted {
			// Ignore secondary Close errors after a started stream read failure so open
			// blocks flush and accumulated Losses commit onto opts.Exchange while preserving readErr.
			closingFrames, _ := stream.Close(readErr)
			logQoderExchangeLossesDebug(opts.Exchange, opts.RequestID)
			deliverStartedAbortFrames(closingFrames)
			return &QoderConvertedResult{
				Usage:                 rawObs.usage,
				HasUsage:              rawObs.sawUsage,
				Served:                true,
				UpstreamResponseModel: rawObs.upstreamModel,
				ClientDisconnect:      clientDisconnected,
			}, readErr
		}
		// Discard buffered non-semantic preamble frames before downstream headers commit
		// and ignore secondary Close errors so pre-output failures stay eligible for failover.
		stagedFrames = nil
		_, _ = stream.Close(readErr)
		logQoderExchangeLossesDebug(opts.Exchange, opts.RequestID)
		emitPreOutputCommittedTerminalError(readErr)
		return &QoderConvertedResult{
			UpstreamResponseModel: rawObs.upstreamModel,
		}, readErr
	}

	tailFrames, closeErr := stream.Close(nil)
	logQoderExchangeLossesDebug(opts.Exchange, opts.RequestID)
	if closeErr != nil {
		translated := translateProtocolQoderError(closeErr)
		if clientOutputStarted {
			return &QoderConvertedResult{
				Usage:                 rawObs.usage,
				HasUsage:              rawObs.sawUsage,
				Served:                true,
				UpstreamResponseModel: rawObs.upstreamModel,
				ClientDisconnect:      clientDisconnected,
			}, translated
		}
		// Drop buffered non-semantic preamble frames when stream finalization fails
		// before semantic output so unstarted streams never emit partial envelopes.
		stagedFrames = nil
		emitPreOutputCommittedTerminalError(translated)
		return &QoderConvertedResult{
			UpstreamResponseModel: rawObs.upstreamModel,
		}, translated
	}

	summary := stream.Summary()
	if summary.Err != nil {
		truncErr := errors.New(FirstNonEmptyQoder(summary.Err.Message, "qoder stream ended before terminal marker"))
		if clientOutputStarted {
			deliverStartedAbortFrames(tailFrames)
			return &QoderConvertedResult{
				Usage:                 rawObs.usage,
				HasUsage:              rawObs.sawUsage,
				Served:                true,
				UpstreamResponseModel: rawObs.upstreamModel,
				ClientDisconnect:      clientDisconnected,
			}, truncErr
		}
		// Drop buffered non-semantic preamble frames on pre-semantic silent
		// truncation so unstarted streams remain uncommitted for outer failover.
		stagedFrames = nil
		emitPreOutputCommittedTerminalError(truncErr)
		return &QoderConvertedResult{
			UpstreamResponseModel: rawObs.upstreamModel,
		}, truncErr
	}

	stageOrWriteClientFrames(tailFrames)
	summary = stream.Summary()
	return &QoderConvertedResult{
		Usage:                 rawObs.usage,
		HasUsage:              rawObs.sawUsage,
		Served:                summary.HasContent,
		UpstreamResponseModel: rawObs.upstreamModel,
		ClientDisconnect:      clientDisconnected,
	}, nil
}

// ReadConvertedBuffered reads a Qoder COSY SSE response stream through
// opts.Exchange.Buffer, folds all decoded events into a single client JSON
// response body, and emits HTTP 200 application/json through sink. The supplied
// resp.Body is always closed before ReadConvertedBuffered returns.
//
// Upstream wrapped *APIError frames, malformed SSE payloads, silent truncations
// before a terminal marker, or context cancellations return an error before
// committing downstream JSON headers. Once upstream SSE folding and client JSON
// encoding succeed, downstream sink write failures mark ClientDisconnect while
// keeping Served=true so completed upstream generation settles accurately.
func ReadConvertedBuffered(
	ctx context.Context,
	sink upstream.OutputSink,
	resp *http.Response,
	opts ConvertedResponseOptions,
) (result *QoderConvertedResult, err error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("qoder response body is nil")
	}
	if opts.Exchange == nil {
		CloseQoderResponse(resp)
		return nil, errors.New("qoder conversion exchange is nil")
	}
	if ctx == nil {
		if resp.Request != nil {
			ctx = resp.Request.Context()
		} else {
			ctx = context.Background()
		}
	}

	scanner := newQoderFrameScanner(resp.Body)
	defer scanner.close(resp)
	defer func() {
		snap := upstream.FinalizeExchangeConversion(ctx, opts.Exchange)
		if result != nil {
			result.ProtocolConversion = snap
		}
	}()

	buf := opts.Exchange.Buffer()
	var rawObs qoderRawFrameObserver
	abortBuffered := func(cause error) (*QoderConvertedResult, error) {
		// Ignore secondary Buffer.Close errors during abort cleanup so conversion
		// losses commit onto opts.Exchange without masking the primary stream failure.
		_, _ = buf.Close(cause)
		logQoderExchangeLossesDebug(opts.Exchange, opts.RequestID)
		return &QoderConvertedResult{
			UpstreamResponseModel: rawObs.upstreamModel,
		}, cause
	}

	for {
		step, ctxErr := scanner.next(ctx)
		if ctxErr != nil {
			return abortBuffered(ctxErr)
		}
		if step.err != nil {
			return abortBuffered(step.err)
		}
		if !step.ok {
			break
		}

		if obsErr := rawObs.observe(step.frame); obsErr != nil {
			return abortBuffered(translateProtocolQoderError(obsErr))
		}

		if wErr := buf.Write(step.frame); wErr != nil {
			return abortBuffered(translateProtocolQoderError(wErr))
		}

		if isQoderExplicitDoneFrame(step.frame) {
			break
		}
	}

	responseBody, err := buf.Close(nil)
	logQoderExchangeLossesDebug(opts.Exchange, opts.RequestID)
	if err != nil {
		return &QoderConvertedResult{
			UpstreamResponseModel: rawObs.upstreamModel,
		}, translateProtocolQoderError(err)
	}

	out := &QoderConvertedResult{
		Usage:                 rawObs.usage,
		HasUsage:              rawObs.sawUsage,
		Served:                true,
		UpstreamResponseModel: rawObs.upstreamModel,
	}
	header := http.Header{
		"Content-Type": []string{"application/json; charset=utf-8"},
	}
	if opts.RequestID != "" {
		header.Set("x-request-id", opts.RequestID)
	}
	// Preserve Served and observed billing usage when downstream JSON emission
	// fails after upstream generation and encoding have already completed.
	if writeErr := sink.Begin(upstream.OutputHead{Status: http.StatusOK, Header: header}); writeErr != nil {
		out.ClientDisconnect = true
	} else if writeErr = sink.Emit(upstream.OutputEvent{
		Data:           responseBody,
		Semantic:       true,
		CommitForRetry: true,
		Terminal:       true,
	}); writeErr != nil {
		out.ClientDisconnect = true
	}
	return out, nil
}

// newQoderFrameScanner starts a background step-driven SSE frame reader over
// body using defaultMaxLineSize. Callers own shutting down the goroutine and
// closing the response body through close when reading finishes.
func newQoderFrameScanner(body io.Reader) *qoderFrameScanner {
	s := &qoderFrameScanner{
		wantNext: make(chan struct{}),
		out:      make(chan qoderScanStep),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
	// Park between reader.Scan steps on wantNext and stop so frame-level errors
	// and [DONE] markers halt reads before close invokes resp.Body.Close.
	go func() {
		defer close(s.done)
		reader := sse.NewReaderLimit(body, defaultMaxLineSize)
		for {
			select {
			case <-s.stop:
				return
			case <-s.wantNext:
			}
			if reader.Scan() {
				f := reader.Frame()
				step := qoderScanStep{
					frame: sse.Frame{
						Event: f.Event,
						Data:  bytes.Clone(f.Data),
					},
					ok: true,
				}
				select {
				case s.out <- step:
				case <-s.stop:
					return
				}
				continue
			}
			var scanErr error
			if err := reader.Err(); err != nil {
				if errors.Is(err, sse.ErrFrameTooLarge) {
					scanErr = fmt.Errorf("SSE event exceeds max line size (%d bytes): %w", defaultMaxLineSize, err)
				} else {
					scanErr = err
				}
			}
			select {
			case s.out <- qoderScanStep{err: scanErr, ok: false}:
			case <-s.stop:
			}
			return
		}
	}()
	return s
}

// next requests one SSE frame scan step from the background reader goroutine
// and returns the scanned frame, EOF status, or context cancellation error.
// Callers invoke next serially from the owning response-reader goroutine until
// step.ok is false, step.err is non-nil, or an explicit Qoder done marker arrives.
//
// Sending on wantNext marks inFlight true until the matching step arrives from
// out. When ctx cancels while reader.Scan is blocked on a live network or pipe
// stream, inFlight remains true so close closes resp.Body first to unblock the
// read instead of waiting on a blocked scanner goroutine.
func (s *qoderFrameScanner) next(ctx context.Context) (qoderScanStep, error) {
	select {
	case <-ctx.Done():
		return qoderScanStep{}, ctx.Err()
	case s.wantNext <- struct{}{}:
		s.inFlight = true
	}
	select {
	case step := <-s.out:
		s.inFlight = false
		return step, nil
	case <-ctx.Done():
		return qoderScanStep{}, ctx.Err()
	}
}

// close terminates the scanner goroutine and closes resp.Body once. When no
// scan step is in flight, close waits for the parked goroutine to exit before
// closing resp.Body so synchronous non-thread-safe test readers never race.
func (s *qoderFrameScanner) close(resp *http.Response) {
	close(s.stop)
	if s.inFlight {
		// Close resp.Body first when next exited via ctx.Done while reader.Scan was
		// blocked on a pipe or network socket so the read unblocks immediately.
		CloseQoderResponse(resp)
		return
	}
	<-s.done
	CloseQoderResponse(resp)
}

// observe inspects one raw Qoder SSE frame before IR conversion to capture the
// first vendor-declared model string and accumulate wirejson-validated native
// billing token usage. Wrapper HTTP error frames (>= 400) skip usage extraction
// so StreamDecoder surfaces the canonical *APIError.
func (o *qoderRawFrameObserver) observe(frame sse.Frame) error {
	trimmed := bytes.TrimSpace(frame.Data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("[DONE]")) {
		return nil
	}
	var wrapper wireConvertedSSEWrapper
	// Ignore malformed outer or inner JSON in the pre-conversion observer so
	// StreamDecoder surfaces the single authoritative wire parse error next.
	if err := unmarshalConvertedPermissive(trimmed, &wrapper); err != nil {
		return nil
	}
	if parseConvertedWrapperStatus(wrapper) >= http.StatusBadRequest {
		return nil
	}
	if wrapper.Body == "" || wrapper.Body == "[DONE]" {
		return nil
	}

	bodyBytes := []byte(wrapper.Body)
	var inner wireConvertedSSEInner
	// Skip pre-conversion model and usage inspection when inner JSON is malformed
	// so StreamDecoder reports the canonical inner wire error on the same frame.
	if err := unmarshalConvertedPermissive(bodyBytes, &inner); err != nil {
		return nil
	}
	if o.upstreamModel == "" {
		if m := strings.TrimSpace(inner.Model); m != "" {
			o.upstreamModel = m
		}
	}

	// Validate usage envelopes and usage objects with wirejson before merging so
	// duplicate or case-colliding usage keys fail consistently with codec/qoderchat.
	trimmedTop := bytes.TrimSpace(inner.Usage)
	if len(trimmedTop) > 0 && !bytes.Equal(trimmedTop, []byte("null")) {
		if err := wirejson.ValidateUsageEnvelope(bodyBytes); err != nil {
			return fmt.Errorf("qoder invalid SSE usage JSON: %w", err)
		}
		if err := wirejson.ValidateUsageObject(trimmedTop); err != nil {
			return fmt.Errorf("qoder invalid SSE usage JSON: %w", err)
		}
		o.mergeUsageJSON(trimmedTop)
	}

	evType := FirstNonEmptyQoder(inner.Event, inner.Type)
	if evType == "message_delta" {
		trimmedData := bytes.TrimSpace(inner.Data)
		if len(trimmedData) > 0 && trimmedData[0] == '{' {
			var delta wireConvertedMessageDelta
			if unmarshalConvertedPermissive(trimmedData, &delta) == nil {
				trimmedDeltaUsage := bytes.TrimSpace(delta.Usage)
				if len(trimmedDeltaUsage) > 0 && !bytes.Equal(trimmedDeltaUsage, []byte("null")) {
					if err := wirejson.ValidateUsageEnvelope(bodyBytes); err != nil {
						return fmt.Errorf("qoder invalid SSE usage JSON: %w", err)
					}
					if err := wirejson.ValidateUsageObject(trimmedDeltaUsage); err != nil {
						return fmt.Errorf("qoder invalid SSE usage JSON: %w", err)
					}
					o.mergeUsageJSON(trimmedDeltaUsage)
				}
			}
		}
	}
	return nil
}

// mergeUsageJSON merges one non-null Qoder usage JSON object into o.usage while
// splitting cached_tokens out of inclusive prompt_tokens and keeping CacheWrite
// at zero. Malformed usage objects still mark sawUsage true while leaving prior
// token counters intact, matching QoderSSEUsage tolerance.
//
// Sparse trailing message_delta updates that report completion output tokens
// alone retain earlier input and cache_read counts, while summary frames that
// repeat total prompt_tokens without prompt_tokens_details preserve an already
// observed cache_read split.
func (o *qoderRawFrameObserver) mergeUsageJSON(raw []byte) {
	hadPrior := o.sawUsage
	prev := o.usage
	o.sawUsage = true

	var u wireConvertedUsage
	// Retain previously merged token counts while keeping sawUsage true when a
	// non-null usage token fails object decoding, matching QoderSSEUsage.UnmarshalJSON.
	if unmarshalConvertedPermissive(raw, &u) != nil {
		return
	}

	prompt := parseConvertedFlexibleInt(u.PromptTokens)
	if prompt == 0 {
		prompt = parseConvertedFlexibleInt(u.InputTokens)
	}
	completion := parseConvertedFlexibleInt(u.CompletionTokens)
	if completion == 0 {
		completion = parseConvertedFlexibleInt(u.OutputTokens)
	}
	prompt = max(prompt, 0)
	completion = max(completion, 0)

	hasPromptDetails := false
	cached := 0
	ptTrim := bytes.TrimSpace(u.PromptTokensDetails)
	if len(ptTrim) > 0 && !bytes.Equal(ptTrim, []byte("null")) {
		hasPromptDetails = true
		var pt wireConvertedPromptDetails
		if unmarshalConvertedPermissive(ptTrim, &pt) == nil {
			cached = max(parseConvertedFlexibleInt(pt.CachedTokens), 0)
		}
	}

	cur := upstream.TokenUsage{
		OutputTokens: completion,
	}
	if hasPromptDetails {
		// Qoder prompt_tokens includes cached_tokens while cacheable_tokens is an
		// informational hint rather than a cache write counter.
		cur.InputTokens = max(prompt-cached, 0)
		cur.CacheReadInputTokens = cached
	} else {
		cur.InputTokens = prompt
	}

	if !hadPrior {
		o.usage = cur
		return
	}

	if prompt == 0 && (!hasPromptDetails || cached == 0) && cur.OutputTokens > 0 {
		// Retain earlier input and cache_read counts when a trailing message_delta
		// reports completion output tokens alone.
		cur.InputTokens = prev.InputTokens
		cur.CacheReadInputTokens = prev.CacheReadInputTokens
	} else if prev.CacheReadInputTokens > 0 && cur.CacheReadInputTokens == 0 && !hasPromptDetails && (cur.InputTokens > 0 || cur.OutputTokens > 0) {
		if cur.InputTokens >= prev.CacheReadInputTokens {
			cur.InputTokens -= prev.CacheReadInputTokens
		}
		cur.CacheReadInputTokens = prev.CacheReadInputTokens
	}
	if cur.OutputTokens == 0 && prev.OutputTokens > 0 && (cur.InputTokens > 0 || cur.CacheReadInputTokens > 0) {
		cur.OutputTokens = prev.OutputTokens
	}
	o.usage = cur
}

// isQoderExplicitDoneFrame reports whether frame carries Qoder's explicit
// [DONE], {"body":"[DONE]"}, or inner message_stop termination marker. Blank
// heartbeats, ordinary content frames, and malformed JSON payloads return false
// so the caller continues scanning or surfaces decode errors normally.
func isQoderExplicitDoneFrame(frame sse.Frame) bool {
	trimmed := bytes.TrimSpace(frame.Data)
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		return true
	}
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	var wrapper wireConvertedSSEWrapper
	// Treat malformed wrapper or inner JSON as non-terminal here so the caller's
	// converter step surfaces the decode failure instead of stopping early at EOF.
	if unmarshalConvertedPermissive(trimmed, &wrapper) != nil {
		return false
	}
	if wrapper.Body == "[DONE]" {
		return true
	}
	if wrapper.Body == "" || wrapper.Body[0] != '{' {
		return false
	}
	var inner wireConvertedSSEInner
	if unmarshalConvertedPermissive([]byte(wrapper.Body), &inner) != nil {
		return false
	}
	return FirstNonEmptyQoder(inner.Event, inner.Type) == "message_stop"
}

// unmarshalConvertedPermissive decodes Qoder upstream JSON bytes into dst with
// duplicate object member names and invalid UTF-8 string bytes permitted. This
// matches codec/qoderchat's upstream tolerance while structural syntax errors
// still return a non-nil error.
func unmarshalConvertedPermissive(data []byte, dst any) error {
	return json.Unmarshal(
		data,
		dst,
		jsontext.AllowDuplicateNames(true),
		jsontext.AllowInvalidUTF8(true),
	)
}

// parseConvertedWrapperStatus extracts the first positive HTTP status code from
// wrapper's StatusCodeValue, StatusCode, or Status fields in priority order.
// Missing, null, non-numeric, or non-positive status tokens return zero so
// normal 200 wrapper frames proceed to inner payload inspection.
func parseConvertedWrapperStatus(wrapper wireConvertedSSEWrapper) int {
	for _, raw := range []jsontext.Value{wrapper.StatusCodeValue, wrapper.StatusCode, wrapper.Status} {
		if code := parseConvertedFlexibleInt(raw); code > 0 {
			return code
		}
	}
	return 0
}

// parseConvertedFlexibleInt parses a JSON integer, floating-point number, or
// quoted numeric string into an int. Empty, null, or unparseable tokens return
// zero, and floating-point values truncate toward zero to match Qoder usage counters.
func parseConvertedFlexibleInt(raw jsontext.Value) int {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return 0
	}
	if trimmed[0] == '"' {
		var s string
		if unmarshalConvertedPermissive(trimmed, &s) != nil {
			return 0
		}
		s = strings.TrimSpace(s)
		if n, err := strconv.Atoi(s); err == nil {
			return n
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int(f)
		}
		return 0
	}
	var n int
	if unmarshalConvertedPermissive(trimmed, &n) == nil {
		return n
	}
	var f float64
	if unmarshalConvertedPermissive(trimmed, &f) == nil {
		return int(f)
	}
	return 0
}

// classifyQoderLocalTerminalError maps a pre-semantic Qoder failure onto an
// intended HTTP status, client error type, optional error code, and sanitized
// message for untimed terminal SSE error emission. Nil or unrecognized errors
// fall back to a generic 502 upstream_error classification, while deadlines map
// to 504 timeout.
func classifyQoderLocalTerminalError(err error) (int, string, string, string) {
	if err == nil {
		return http.StatusBadGateway, "upstream_error", "", "Upstream request failed"
	}
	if reqErr, ok := errors.AsType[*RequestError](err); ok {
		return http.StatusBadRequest, "invalid_request_error", reqErr.Code, FirstNonEmptyQoder(reqErr.Message, "Invalid request")
	}
	if apiErr, ok := errors.AsType[*APIError](err); ok {
		msg := FirstNonEmptyQoder(apiErr.Message, "Qoder upstream request failed")
		switch {
		case apiErr.IsAgentLimit() || apiErr.StatusCode == http.StatusTooManyRequests:
			return http.StatusTooManyRequests, "rate_limit_error", apiErr.Code, msg
		case apiErr.IsEntitlementDenied() || apiErr.StatusCode == http.StatusForbidden:
			return http.StatusForbidden, "permission_error", apiErr.Code, msg
		case apiErr.StatusCode == http.StatusUnauthorized:
			return http.StatusBadGateway, "upstream_error", apiErr.Code, msg
		case apiErr.StatusCode >= 400 && apiErr.StatusCode < 500:
			return apiErr.StatusCode, "invalid_request_error", apiErr.Code, msg
		default:
			return http.StatusBadGateway, "upstream_error", apiErr.Code, msg
		}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "upstream_error", "timeout", "Upstream request timed out"
	}
	return http.StatusBadGateway, "upstream_error", "", "Upstream request failed"
}

// logQoderExchangeLossesDebug emits one debug-level log entry summarizing the
// cumulative request and response ir.Loss count recorded on exchange. Nil
// exchanges or conversions with zero recorded losses return immediately without logging.
func logQoderExchangeLossesDebug(exchange *convert.Exchange, requestID string) {
	if exchange == nil {
		return
	}
	losses := exchange.Losses()
	if len(losses) == 0 {
		return
	}
	logging.L().Debug("qoder converted exchange losses",
		zap.String("request_id", requestID),
		zap.Int("loss_count", len(losses)),
	)
}
