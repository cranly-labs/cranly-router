package gemini

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	geminiwire "github.com/TokenFlux/TokenRouter/internal/protocol/gemini"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// response_converted.go reads streaming SSE and buffered Gemini responses
// through convert.Exchange for any client protocol while preserving native
// ReadGeminiStream validation, billing usage extraction, pre-conversion model
// observation, and pre-semantic failover staging. OutputContext.NextEvent marks
// every non-empty payload write as CommitForRetry, so flushing a leading
// non-semantic frame (such as message_start, response.created, or an opening
// assistant role chunk) would prematurely close provider failover before real
// model output arrives.
//
// ReadConvertedStreaming buffers non-semantic client frames locally until the
// first Semantic==true frame or a validated completion terminal arrives, calls
// OutputContext.Complete upon observing finishReason so downstream heartbeats
// stop during the five-second sparse usage tail window, and invokes
// Stream.Close(nil) only after ReadGeminiStream returns so final merged usage
// and response_end frames carry trailing counters. Early-committed HTTP 200 SSE
// responses receive untimed client-protocol terminal SSE errors, started
// streams close open blocks via Stream.Close(cause) before emitting their
// terminal error frame, and disconnected clients stop downstream writes while
// continuing to drain upstream usage for billing.

const (
	// OpenAI-compatible protocol values select the error representation used by
	// OpenAI-compatible Gemini error mappers.
	OpenAICompatChatCompletions OpenAICompatProtocol = iota // Chat Completions output
	OpenAICompatResponses                                   // Responses protocol output

	// defaultBufferedGeminiLimit caps unary buffered JSON reads when callers do
	// not supply a bounded ReadBody callback on ConvertedOptions or ResponseOptions.
	defaultBufferedGeminiLimit = 8 << 20 // fallback eight-MiB unary JSON read cap
)

// OpenAICompatProtocol identifies Chat Completions or Responses formatting for
// an OpenAI-compatible Gemini error or response.
type OpenAICompatProtocol int

// ConvertedStreamFailure carries either a pre-semantic failover error or the
// resolved client error metadata from single-pass stream failure classification.
type ConvertedStreamFailure struct {
	// Failover takes precedence over Status, Type, Code, and Message when
	// non-nil so pre-semantic retryable failures return directly to the outer loop.
	Failover error  // retryable failover error returned before semantic output
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
	// to writeConvertedUnstartedError's standalone JSON envelope.
	WriteError func(status int, errType, code, message string)
	// WriteLocalTerminalError writes an untimed terminal SSE error frame in the
	// inbound client protocol, emitting response.failed on Responses streams. Nil
	// falls back to WriteError when stream output has not started.
	WriteLocalTerminalError func(status int, errType, code, message string)
	// WriteRawTerminalSSE delivers pre-encoded terminal SSE frame bytes from
	// Stream.Close(cause) through the gateway's untimed terminal barrier and
	// reports whether delivery ran. Returning false falls back to c.Writer.
	WriteRawTerminalSSE func(status int, errType, code, message string, data []byte) bool
	// ReadBody reads bounded unary JSON bytes and reports whether an oversize
	// client error response has already been written to the active HTTP output.
	// Nil falls back to ResponseOptions.ReadBody or defaultBufferedGeminiLimit.
	ReadBody func(io.Reader) ([]byte, bool, error)
	// ClassifyStreamFailure evaluates provider error policy at most once for an
	// upstream stream or inline error and returns either Failover or client error
	// fields. Nil uses streamErr's status and sanitized message directly.
	ClassifyStreamFailure func(ctx context.Context, err *upstream.GeminiStreamError, clientOutputStarted bool) ConvertedStreamFailure
}

// ReadConvertedBuffered reads either a collected upstream Gemini SSE stream
// (when useUpstreamStream is true, such as project-bearing Code Assist OAuth) or
// a unary Gemini JSON body through conv.Exchange and writes a single client JSON
// response. Callers retain ownership of closing resp.Body after this call returns.
//
// Collected SSE streams pass through ReadGeminiStream and fold into client JSON
// via Exchange.Buffer after collecting up to five seconds of sparse trailing
// usage frames, whereas unary JSON bodies unwrap optional OAuth envelopes and
// convert via Exchange.Response. Both paths run ObserveRaw and ObserveImages
// before conversion, evaluate inline or stream failures once via
// ClassifyStreamFailure, and return native merged billing token counters.
func (s *ResponseAdapter) ReadConvertedBuffered(
	c *upstream.OutputContext,
	resp *http.Response,
	conv ConvertedOptions,
	useUpstreamStream bool,
	isOAuth bool,
) (*upstream.TokenUsage, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("gemini upstream response body is nil")
	}
	if conv.Exchange == nil {
		return nil, errors.New("gemini conversion exchange is nil")
	}
	ctx := s.Options.Context
	if ctx == nil {
		ctx = context.Background()
	}

	if useUpstreamStream {
		return s.readConvertedBufferedSSE(ctx, c, resp, conv, isOAuth)
	}
	return s.readConvertedBufferedJSON(ctx, c, resp, conv, isOAuth)
}

// readConvertedBufferedSSE consumes an upstream Gemini SSE body through
// ReadGeminiStream, folds frames via conv.Exchange.Buffer, and writes buffered
// client JSON while returning native merged billing usage. Stream or buffer
// failures route once through handleBufferedStreamFailure alongside partial usage.
func (s *ResponseAdapter) readConvertedBufferedSSE(
	ctx context.Context,
	c *upstream.OutputContext,
	resp *http.Response,
	conv ConvertedOptions,
	isOAuth bool,
) (*upstream.TokenUsage, error) {
	buf := conv.Exchange.Buffer()
	sawSemantic := false

	observed, readErr := s.readStream(resp, isOAuth, func(event upstream.GeminiStreamEvent) error {
		if len(event.Data) == 0 {
			return nil
		}
		s.observeRaw(event.Data)
		if s.Options.ObserveImages != nil {
			s.Options.ObserveImages(event.Data)
		}
		sawSemantic = sawSemantic || geminiwire.ObservePayload(event.Data).Semantic
		payload := event.Data
		if s.Options.ReverseTools != nil {
			payload = s.Options.ReverseTools(payload)
		}
		if wErr := buf.Write(sse.Frame{Data: payload}); wErr != nil {
			return newConvertedDecodeStreamError("Failed to parse upstream stream", wErr)
		}
		return nil
	})

	billingUsage := ExtractGeminiUsage(observed.UsageJSON)
	if billingUsage == nil {
		billingUsage = &upstream.TokenUsage{}
	}
	if readErr != nil {
		return billingUsage, s.handleBufferedStreamFailure(ctx, c, conv, readErr)
	}

	clientJSON, closeErr := buf.Close(nil)
	if closeErr != nil {
		return billingUsage, s.handleBufferedStreamFailure(
			ctx,
			c,
			conv,
			newConvertedDecodeStreamError("Failed to convert upstream response", closeErr),
		)
	}
	logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)

	s.writeBufferedResponseHeaders(c, resp.Header, conv.RequestID)
	c.NextEvent(sawSemantic, true)
	c.Data(http.StatusOK, "application/json; charset=utf-8", clientJSON)
	return billingUsage, nil
}

// readConvertedBufferedJSON reads a bounded unary Gemini JSON response body,
// unwraps OAuth envelopes when applicable, converts client JSON via
// conv.Exchange.Response, and returns native billing usage counters. Oversize
// or cancelled reads suppress duplicate error envelopes, while HTTP 200 inline
// error bodies classify once through handleBufferedStreamFailure.
func (s *ResponseAdapter) readConvertedBufferedJSON(
	ctx context.Context,
	c *upstream.OutputContext,
	resp *http.Response,
	conv ConvertedOptions,
	isOAuth bool,
) (*upstream.TokenUsage, error) {
	respBody, alreadyPresented, err := s.readBoundedUnaryBody(resp.Body, conv)
	if err != nil {
		if alreadyPresented || ctx.Err() != nil || errors.Is(err, context.Canceled) {
			return nil, err
		}
		errType := defaultConvertedGeminiErrorType(conv.DefaultErrorType, http.StatusBadGateway)
		writeConvertedUnstartedError(c, conv, http.StatusBadGateway, errType, "", "Failed to read upstream response")
		return nil, err
	}
	// Strip the Code Assist {"response": ...} wrapper on OAuth targets before
	// inline error detection, raw observation, and IR response decoding run.
	respBody = UnwrapIfNeeded(isOAuth, respBody)

	// Classify HTTP 200 inline Google error envelopes through the single-pass
	// stream failure callback so provider health and failover rules still apply.
	if inlineErr := extractInlineGeminiStreamError(respBody); inlineErr != nil {
		return nil, s.handleBufferedStreamFailure(ctx, c, conv, inlineErr)
	}

	s.observeRaw(respBody)
	if s.Options.ObserveImages != nil {
		s.Options.ObserveImages(respBody)
	}
	payload := respBody
	if s.Options.ReverseTools != nil {
		payload = s.Options.ReverseTools(payload)
	}

	clientJSON, convErr := conv.Exchange.Response(payload)
	if convErr != nil {
		errType := defaultConvertedGeminiErrorType(conv.DefaultErrorType, http.StatusBadGateway)
		writeConvertedUnstartedError(c, conv, http.StatusBadGateway, errType, "", "Failed to parse upstream response")
		return nil, convErr
	}
	logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)

	s.writeBufferedResponseHeaders(c, resp.Header, conv.RequestID)
	c.NextEvent(geminiwire.ObservePayload(respBody).Semantic, true)
	c.Data(http.StatusOK, "application/json; charset=utf-8", clientJSON)

	if u := ExtractGeminiUsage(respBody); u != nil {
		return u, nil
	}
	return &upstream.TokenUsage{}, nil
}

// ReadConvertedStreaming consumes an upstream Gemini SSE body through
// ReadGeminiStream and conv.Exchange.Stream, staging non-semantic leading
// frames locally until semantic content or a validated completion terminal
// arrives so pre-semantic failures remain eligible for outer provider failover.
//
// Observing finishReason invokes c.Complete to stop downstream keepalive
// heartbeats while ReadGeminiStream continues collecting up to five seconds of
// sparse usage tail frames, after which Stream.Close(nil) emits the final
// merged client usage and completion frames. Disconnected clients stop further
// downstream writes while continuing to drain upstream usage for billing,
// pre-committed HTTP 200 SSE streams receive untimed terminal error frames, and
// started streams close open blocks via Stream.Close(cause) before delivering
// their terminal error frame.
func (s *ResponseAdapter) ReadConvertedStreaming(
	c *upstream.OutputContext,
	resp *http.Response,
	conv ConvertedOptions,
	isOAuth bool,
) (*upstream.TokenUsage, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("gemini upstream response body is nil")
	}
	if conv.Exchange == nil {
		return nil, errors.New("gemini conversion exchange is nil")
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
			if s.Options.HasHeaderFilter && s.Options.WriteHeaders != nil {
				s.Options.WriteHeaders(c.Writer.Header(), resp.Header)
			}
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
		if conv.CommittedSSE != nil && conv.CommittedSSE() {
			return true
		}
		return c != nil && c.Writer != nil && c.Writer.Written()
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
			logging.L().Info("gemini converted stream: client disconnected, continuing to drain upstream for billing",
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

	observed, readErr := s.readStream(resp, isOAuth, func(event upstream.GeminiStreamEvent) error {
		if len(event.Data) > 0 {
			s.observeRaw(event.Data)
			if s.Options.ObserveImages != nil {
				s.Options.ObserveImages(event.Data)
			}
			// Keep draining ReadGeminiStream after client disconnect so billing usage
			// and model observations complete without writing further client frames.
			if !clientDisconnected && c.Err() == nil {
				payload := event.Data
				if s.Options.ReverseTools != nil {
					payload = s.Options.ReverseTools(payload)
				}
				frames, wErr := stream.Write(sse.Frame{Data: payload})
				if wErr != nil {
					return newConvertedDecodeStreamError("Failed to parse upstream stream", wErr)
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

	billingUsage := ExtractGeminiUsage(observed.UsageJSON)
	if billingUsage == nil {
		billingUsage = &upstream.TokenUsage{}
	}

	if clientDisconnected || c.Err() != nil {
		// Discard terminal error frames on a disconnected client while still closing
		// the converter stream so conversion losses finalize cleanly.
		_, _ = stream.Close(errors.New("client disconnected"))
		logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
		return billingUsage, nil
	}

	if readErr != nil {
		if ctx.Err() != nil || errors.Is(readErr, context.Canceled) {
			// Finalize open converter state on caller cancellation while suppressing
			// synthetic SSE error frames onto the cancelled client connection.
			_, _ = stream.Close(readErr)
			logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
			return billingUsage, readErr
		}
		streamErr := coerceGeminiStreamError(readErr)
		return billingUsage, s.deliverConvertedStreamFailure(
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
		synthErr := newConvertedDecodeStreamError("Failed to finalize upstream stream", closeErr)
		return billingUsage, s.deliverConvertedStreamFailure(
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
	logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
	stageOrWriteClientFrames(tailFrames)
	if !clientOutputStarted && len(stagedFrames) > 0 {
		if flushStagedFrames() && !clientDisconnected {
			c.Writer.Flush()
		}
	}
	return billingUsage, nil
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
		logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
		return classified.Failover
	}

	cause := &ir.StreamError{
		Type:    classified.Type,
		Code:    classified.Code,
		Message: classified.Message,
		Status:  classified.Status,
	}

	if clientOutputStarted {
		closingFrames, closeErr := stream.Close(cause)
		logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
		// Fall back to WriteLocalTerminalError when Stream.Close was already
		// consumed or failed so started client streams always terminate cleanly.
		if closeErr != nil || len(closingFrames) == 0 {
			if conv.WriteLocalTerminalError != nil {
				conv.WriteLocalTerminalError(cause.Status, cause.Type, cause.Code, cause.Message)
			}
			return errors.Join(rawErr, classified.Err)
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
			c.NextEvent(f.Semantic, f.Terminal)
			if _, wErr := c.Writer.Write(raw); wErr != nil {
				return errors.Join(rawErr, classified.Err)
			}
		}
		c.Writer.Flush()
		return errors.Join(rawErr, classified.Err)
	}

	// Finalize unstarted converter state without flushing staged frames so the
	// response renders either one untimed SSE error or one HTTP JSON error.
	_, _ = stream.Close(cause)
	logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
	if isSSECommitted() && conv.WriteLocalTerminalError != nil {
		conv.WriteLocalTerminalError(cause.Status, cause.Type, cause.Code, cause.Message)
		return errors.Join(rawErr, classified.Err)
	}
	writeConvertedUnstartedError(c, conv, cause.Status, cause.Type, cause.Code, cause.Message)
	return errors.Join(rawErr, classified.Err)
}

// handleBufferedStreamFailure classifies readErr once for buffered Gemini SSE
// or inline HTTP 200 error bodies, returning pre-output failover errors directly
// or presenting the resolved client error response. Cancelled requests and
// non-GeminiStreamError causes bypass client presentation and return readErr.
func (s *ResponseAdapter) handleBufferedStreamFailure(
	ctx context.Context,
	c *upstream.OutputContext,
	conv ConvertedOptions,
	readErr error,
) error {
	if ctx.Err() != nil || errors.Is(readErr, context.Canceled) {
		logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
		return readErr
	}
	var streamErr *upstream.GeminiStreamError
	if !errors.As(readErr, &streamErr) {
		logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
		return readErr
	}
	classified := s.classifyConvertedFailure(ctx, conv, streamErr, false)
	logConvertedExchangeLossesDebug(conv.Exchange, conv.RequestID)
	if classified.Failover != nil {
		return classified.Failover
	}
	if conv.CommittedSSE != nil && conv.CommittedSSE() && conv.WriteLocalTerminalError != nil {
		conv.WriteLocalTerminalError(classified.Status, classified.Type, classified.Code, classified.Message)
		return errors.Join(readErr, classified.Err)
	}
	writeConvertedUnstartedError(c, conv, classified.Status, classified.Type, classified.Code, classified.Message)
	return errors.Join(readErr, classified.Err)
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
		classified.Type = defaultConvertedGeminiErrorType(conv.DefaultErrorType, classified.Status)
	}
	if classified.Message == "" {
		classified.Message = strings.TrimSpace(streamErr.Message)
	}
	if classified.Message == "" {
		classified.Message = "Upstream stream failed"
	}
	return classified
}

// readBoundedUnaryBody reads a unary response body using conv.ReadBody,
// s.Options.ReadBody, or defaultBufferedGeminiLimit in that order. The boolean
// result reports whether conv.ReadBody already presented an oversize HTTP error.
func (s *ResponseAdapter) readBoundedUnaryBody(body io.Reader, conv ConvertedOptions) ([]byte, bool, error) {
	if conv.ReadBody != nil {
		return conv.ReadBody(body)
	}
	if s.Options.ReadBody != nil {
		raw, err := s.Options.ReadBody(body)
		return raw, false, err
	}
	// Cap standalone unary reads at eight MiB when no gateway body reader is wired.
	raw, err := io.ReadAll(io.LimitReader(body, defaultBufferedGeminiLimit))
	return raw, false, err
}

// writeBufferedResponseHeaders copies filtered upstream headers, attaches
// requestID when non-empty, and sets JSON Content-Type before buffered output.
// Callers invoke this helper immediately ahead of c.NextEvent and c.Data.
func (s *ResponseAdapter) writeBufferedResponseHeaders(
	c *upstream.OutputContext,
	src http.Header,
	requestID string,
) {
	if s.Options.WriteHeaders != nil {
		s.Options.WriteHeaders(c.Writer.Header(), src)
	}
	if requestID != "" {
		c.Header("x-request-id", requestID)
	}
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
}

// extractInlineGeminiStreamError inspects a unary HTTP 200 JSON body via
// jsontext for an embedded top-level "error" member and converts it into a
// GeminiStreamError. Valid generation payloads, non-object JSON, or null error
// fields return nil; out-of-range error codes normalize to HTTP 502 with
// sanitized upstream message text.
func extractInlineGeminiStreamError(body []byte) *upstream.GeminiStreamError {
	if len(body) == 0 {
		return nil
	}
	// inlineGoogleErrorCode decodes an optional numeric HTTP status code inside a Google error object.
	type inlineGoogleErrorCode struct {
		Code int `json:"code"` // optional numeric status code
	}
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	if tok, err := dec.ReadToken(); err != nil || tok.Kind() != '{' {
		return nil
	}
	foundError := false
	status := 0
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return nil
		}
		key := kTok.String()
		val, vErr := dec.ReadValue()
		if vErr != nil {
			return nil
		}
		if key != "error" || len(val) == 0 || val.Kind() == 'n' {
			continue
		}
		foundError = true
		if val.Kind() == '{' {
			var parsed inlineGoogleErrorCode
			if json.Unmarshal(val, &parsed, jsontext.AllowInvalidUTF8(true)) == nil {
				status = parsed.Code
			}
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil
	}
	if _, err := dec.ReadToken(); err != io.EOF || !foundError {
		return nil
	}
	// Map missing or non-HTTP Google error codes onto 502 Bad Gateway so policy
	// classifiers always receive a valid 4xx/5xx upstream status code.
	if status < http.StatusBadRequest || status > 599 {
		status = http.StatusBadGateway
	}
	msg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(body)))
	if msg == "" {
		msg = "Upstream request failed"
	}
	return &upstream.GeminiStreamError{
		Status:  status,
		Kind:    "upstream_error",
		Message: msg,
		Body:    bytes.Clone(body),
	}
}

// coerceGeminiStreamError extracts an existing GeminiStreamError from err or
// wraps err into an HTTP 502 stream_read_error classification. Existing
// structured stream errors are returned unchanged so status and body survive.
func coerceGeminiStreamError(err error) *upstream.GeminiStreamError {
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

// newConvertedDecodeStreamError wraps a stream frame or completion conversion
// error as an HTTP 502 GeminiStreamError for single-pass policy handling.
func newConvertedDecodeStreamError(message string, cause error) *upstream.GeminiStreamError {
	return &upstream.GeminiStreamError{
		Status:  http.StatusBadGateway,
		Kind:    "stream_invalid_json",
		Message: message,
		Cause:   cause,
	}
}

// defaultConvertedGeminiErrorType selects the client error category when stream
// failure classification leaves Type blank. Non-empty fallback strings win
// directly; empty fallbacks map HTTP 400 onto "invalid_request_error" and every
// other status onto "upstream_error".
func defaultConvertedGeminiErrorType(fallback string, status int) string {
	if fallback != "" {
		return fallback
	}
	if status == http.StatusBadRequest {
		return "invalid_request_error"
	}
	return "upstream_error"
}

// writeConvertedUnstartedError delegates uncommitted client error presentation
// to conv.WriteError or writes a minimal JSON error envelope for standalone
// tests. Nil output contexts without WriteError are ignored safely.
func writeConvertedUnstartedError(
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
		Type    string `json:"type"`           // client error category
		Code    string `json:"code,omitempty"` // optional machine-readable code
		Message string `json:"message"`        // client-safe error message
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

// logConvertedExchangeLossesDebug records the total conversion Loss count from
// exchange at debug level alongside requestID whenever at least one loss
// occurred. Nil exchanges and lossless conversions return immediately without
// emitting log entries.
func logConvertedExchangeLossesDebug(exchange *convert.Exchange, requestID string) {
	if exchange == nil {
		return
	}
	losses := exchange.Losses()
	if len(losses) == 0 {
		return
	}
	logging.L().Debug("gemini converted response: recorded conversion losses",
		zap.String("request_id", requestID),
		zap.Int("loss_count", len(losses)),
	)
}
