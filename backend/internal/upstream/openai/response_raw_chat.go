package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	logger "github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// response_raw_chat.go forwards raw Chat Completions responses and converts
// Chat output into client protocols through merged IR exchanges. Raw Chat
// streams observe usage, service tier, refusal evidence, and terminal signals
// before delivery repairs change the client line. Large requests can hold lines
// before headers, while downstream write failures suppress delivery and keep
// scanning active so trailing usage chunks settle.
//
// Merged IR readers (ReadRawChatConvertedStreaming and ReadRawChatConvertedBuffered)
// translate Chat SSE and JSON replies for any client protocol through
// convert.Exchange while retaining raw model and billing usage observations.
// Non-semantic leading client frames stay buffered until semantic progress or a
// non-retryable failure arrives so retryable error frames, empty-stop silent
// refusals, and pre-output truncations still fail over cleanly, while early
// keepalive-committed SSE streams receive untimed terminal client error frames.

const (
	// openAIRawChatConvertedTruncatedMessage describes a Chat Completions stream
	// that ended without [DONE] or a nonblank finish_reason chunk.
	openAIRawChatConvertedTruncatedMessage = "Upstream Chat Completions stream ended before any terminal chunk"
)

// RawResponseOptions binds raw Chat delivery to attempt-specific observation
// and policy.
type RawResponseOptions struct {
	// ProviderID identifies the selected provider in raw-stream truncation
	// diagnostics; a nil gateway provider leaves it zero without disabling bound
	// output callbacks.
	ProviderID int64
	// Scanner supplies raw Chat line reading under caller limits.
	Scanner func(io.Reader) *bufio.Scanner
	// ReadBody consumes the raw buffered response under caller size/error policy;
	// response-body closure remains with the executor after this callback returns.
	ReadBody func(io.Reader) ([]byte, error)
	// BodyLimitError identifies an already presented size rejection via errors.Is;
	// other raw buffered read failures invoke the generic WriteError callback.
	BodyLimitError error
	// Headers copies approved upstream headers before raw forwarding; content
	// type is then selected by the particular reader.
	Headers func(http.Header, http.Header)
	// WriteError presents raw buffered read failures except the configured size
	// sentinel; its void result does not report downstream delivery success.
	WriteError func(int, string, string)
	// Observe receives native payloads before raw delivery repairs; gateway binding
	// observes service tier independently of usage parsing, and buffered JSON uses
	// the completed-event label.
	Observe func([]byte, string)
	// ObserveSSE receives buffered bodies with explicit or sniffed SSE framing
	// before transformation; the gateway inspects their response-tier observations.
	ObserveSSE func(string)
	// TransformLine repairs a raw Chat line after upstream observation; the reader
	// then strips empty tool-call identity fields from the delivery copy.
	TransformLine func(string) string
	// TransformBody repairs buffered bytes after upstream usage/tier and
	// MissingUsage policy, keeping those observations tied to the pre-repair body.
	TransformBody func([]byte) []byte
	// MissingUsage can reject raw buffered output before transformation; it
	// receives the top-level model and accumulated usage observed from the upstream
	// bytes.
	MissingUsage func(string, wire.ForwardUsage) error
	// ServiceTier supplies request-local observed tier for raw/buffered results.
	ServiceTier func() string
	// ResolvedServiceTier supplies an optional request tier pointer for result
	// adaptation; callers keep pointed-to text stable while downstream accounting
	// consumes it.
	ResolvedServiceTier func() *string
	// TruncatedFailover supplies recovery classification when a non-aborted raw
	// Chat stream lacks terminal evidence before client output has begun.
	TruncatedFailover func(error) error
	// RecordTruncation records non-aborted raw Chat truncation after output; the
	// reader then returns typed upstream read failure with observed usage.
	RecordTruncation func(error)
	// SilentRefusal supplies recovery for cleanly read held raw Chat refusal
	// candidates before output, under the detector's large-request eligibility
	// rule.
	SilentRefusal func() error
}

// ConvertedRawChatOptions binds gateway failure policies, header filters,
// truncation classifiers, and untimed SSE error writers around one IR-converted
// Chat Completions response attempt.
type ConvertedRawChatOptions struct {
	RequestContext context.Context // logger context for unexpected scanner read errors
	// ProviderID identifies the selected provider in truncation warning logs;
	// zero leaves bound gateway callbacks active when no provider ID is supplied.
	ProviderID     int64
	RequestBodyLen int // inbound client request byte length for silent-refusal gating
	// MaxFrameBytes bounds accumulated bytes in one SSE frame via sse.NewReaderLimit;
	// nonpositive values select sse.DefaultMaxFrameBytes.
	MaxFrameBytes int
	// AppendDoneOnCyber appends data: [DONE] after cyber-policy error frames
	// when the inbound client protocol expects Chat Completions stream framing.
	AppendDoneOnCyber bool
	// DefaultErrorType supplies the fallback client error category when failure
	// classification or WriteError receives an empty errType string.
	DefaultErrorType string

	StreamInterval func() time.Duration            // per-attempt upstream read-idle timeout getter
	ReadBody       func(io.Reader) ([]byte, error) // bounded buffered response body reader
	// BodyLimitError designates a read error whose HTTP rejection was already
	// presented by ReadBody so buffered readers skip duplicate WriteError calls.
	BodyLimitError error

	Observe              func([]byte, string)                  // pre-conversion raw payload model and tier observer
	ServiceTier          func() string                         // request-scoped observed upstream service tier getter
	ResolvedServiceTier  func() *string                        // outbound or resolved service tier pointer getter
	NormalizeServiceTier func(string) string                   // normalizes observed upstream service tier strings
	Headers              func(http.Header, http.Header)        // copies approved upstream headers onto pending output
	MissingUsage         func(string, wire.ForwardUsage) error // validates billable usage on buffered Chat replies
	CommittedSSE         func() bool                           // reports whether HTTP 200 SSE headers are already committed

	// WriteError writes an uncommitted HTTP JSON error or promotes to an untimed
	// terminal SSE error when waiting heartbeats already committed 200 SSE.
	WriteError func(status int, errType, code, message string)
	// WriteLocalTerminalError writes an untimed client-protocol terminal SSE error
	// (including synthetic response.failed for Responses clients) onto an active stream.
	WriteLocalTerminalError func(status int, errType, code, message string)
	// WriteRawTerminalSSE delivers pre-encoded terminal SSE frame bytes through
	// the gateway's untimed terminal barrier and reports whether delivery ran.
	WriteRawTerminalSSE func(data []byte) bool

	StreamFailure func(payload []byte, message string, outputStarted bool) ConvertedFailure // classifies Chat stream error payloads

	TruncatedFailover func(error) error // builds failover error for pre-output stream truncation or read errors
	RecordTruncation  func(error)       // records post-output stream truncation in Ops
	SilentRefusal     func() error      // builds failover error for large-request empty stop completions
	ReadFailure       func(error) error // classifies mid-stream or pre-output transport read errors

	CyberForwarded error                    // sentinel returned after cyber-policy rejection delivery
	MarkCyber      func(CyberObservation)   // records cyber-policy moderation telemetry before delivery
	Truncate       func(string, int) string // UTF-8-safe byte truncation helper for diagnostics
}

// rawChatPayloadInfo summarizes top-level metadata, usage, finish, and error
// fields scanned from one raw Chat Completions JSON object via jsontext.
type rawChatPayloadInfo struct {
	valid           bool           // whether payload is a syntactically valid JSON object
	model           string         // trimmed top-level "model" string when present
	serviceTier     string         // trimmed top-level "service_tier" string when present
	usageObj        jsontext.Value // cloned top-level "usage" JSON object bytes when present
	hasChoices      bool           // whether top-level "choices" array is present
	sawFinishReason bool           // whether any choice carried a nonblank finish_reason string
	hasError        bool           // whether top-level non-null "error" member is present
	errType         string         // extracted error category string when present
	errCode         string         // extracted string or numeric error code when present
	errMessage      string         // extracted error message string when present
	errStatus       int            // extracted HTTP status integer inside error when present
}

// ReadRawChatStreaming forwards Chat lines after observing upstream usage,
// tier, refusal evidence, and terminal state. Supply scanner, transformation,
// header, tier, and selected failure callbacks; the executor owns body closure.
// Reasoning/tier pointers and headers remain shared in the returned result.
//
// Held refusal candidates settle before final delivery. Non-aborted truncation fails over
// before output and returns typed failure with usage afterward. Client write
// failure or scanner cancellation suppresses that truncation classification;
// those exits can return observed usage with nil error, and ClientDisconnect
// remains unpopulated.
func ReadRawChatStreaming(c *upstream.OutputContext, resp *http.Response, options RawResponseOptions, originalModel, upstreamModel string, reasoningEffort *string, startTime time.Time, requestBodyLen int) (*CompatResponseResult, error) {
	requestID := resp.Header.Get("x-request-id")
	writeStreamHeaders := NewCompatStreamHeaderWriter(c, resp.Header, options.Headers)
	scanner := options.Scanner(resp.Body)

	var usage wire.ForwardUsage

	clientDisconnected := false
	clientOutputStarted := false
	pendingLines := make([]string, 0, 8)
	refusalDetector := NewChatSilentRefusalDetector(requestBodyLen)
	var terminal wire.RawStreamTerminalState

	writeLine := func(line string) {
		if clientDisconnected {
			return
		}
		// Hold native lines while the large-request detector lacks release evidence,
		// keeping a terminal refusal eligible for recovery before semantic client output.
		if !clientOutputStarted && !refusalDetector.ShouldReleaseClientOutput() {
			pendingLines = append(pendingLines, line)
			return
		}
		if !clientOutputStarted {
			writeStreamHeaders()
			for _, pending := range pendingLines {
				if _, werr := io.WriteString(c.Writer, pending+"\n"); werr != nil {
					clientDisconnected = true
					logger.L().Debug("openai chat_completions raw: client disconnected, continuing to drain upstream for billing",
						zap.Error(werr),
						zap.String("request_id", requestID),
					)
					return
				}
			}
			pendingLines = pendingLines[:0]
			clientOutputStarted = true
		}
		if _, werr := io.WriteString(c.Writer, line+"\n"); werr != nil {
			clientDisconnected = true
			logger.L().Debug("openai chat_completions raw: client disconnected, continuing to drain upstream for billing",
				zap.Error(werr),
				zap.String("request_id", requestID),
			)
		}
	}

	var heartbeatFilter upstream.SSEHeartbeatLineFilter
	// Preserve incomplete candidates at EOF, while complete heartbeat frames never
	// enter protocol output or establish the reader's client-content commitment.
	for {
		more := scanner.Scan()
		var lines [][]byte
		if more {
			lines = heartbeatFilter.Push([]byte(scanner.Text() + "\n"))
		} else {
			lines = heartbeatFilter.Flush()
		}
		for _, rawLine := range lines {
			line := strings.TrimSuffix(string(rawLine), "\n")
			refusalDetector.ObserveSSELine(line)
			if payload, ok := wire.ExtractSSEDataLine(line); ok {
				trimmedPayload := strings.TrimSpace(payload)
				terminal.ObserveDataLine(trimmedPayload)
				if trimmedPayload != "[DONE]" {
					options.Observe([]byte(trimmedPayload), wire.OpenAIChatCompletionServiceTierEventType([]byte(trimmedPayload)))
					if u := wire.ExtractCCStreamUsage(payload); u != nil {
						usage = *u
					}
				}
			}
			// Observe upstream evidence first, then repair the client copy and remove
			// empty tool identity fields that would interfere with delta accumulation downstream.
			line = options.TransformLine(line)
			line = wire.StripEmptyChatToolCallIdentityFromSSELine(line)

			writeLine(line)
			if line == "" {
				if !clientDisconnected && clientOutputStarted {
					c.Writer.Flush()
				}
				continue
			}
			if !clientDisconnected && clientOutputStarted {
				c.Writer.Flush()
			}
		}
		if !more {
			break
		}
	}

	resultWithUsage := func() *CompatResponseResult {
		return &CompatResponseResult{
			RequestID:       requestID,
			UpstreamHeaders: resp.Header,
			Usage:           usage,
			Model:           originalModel,
			UpstreamModel:   upstreamModel,
			ServiceTier:     options.ServiceTier(),
			ReasoningEffort: reasoningEffort,
			ResolvedTier:    options.ResolvedServiceTier(),
			Stream:          true,
			Duration:        time.Since(startTime),
		}
	}

	scanErr := scanner.Err()
	if scanErr != nil && !errors.Is(scanErr, context.Canceled) && !errors.Is(scanErr, context.DeadlineExceeded) {
		logger.L().Warn("openai chat_completions raw: stream read error",
			zap.Error(scanErr),
			zap.String("request_id", requestID),
		)
	}

	// Delivery failure or canceled/deadline reads suppress truncation attribution;
	// observed usage remains returnable and these aborted paths can finish with nil error.
	clientAborted := clientDisconnected ||
		errors.Is(scanErr, context.Canceled) ||
		errors.Is(scanErr, context.DeadlineExceeded)

	// The terminal tracker requires DONE, usage-object, or finish-reason evidence
	// after SSE data or before any output; an already delivered non-SSE body is exempt.
	if !clientAborted && terminal.IsTruncated(clientOutputStarted) {
		cause := scanErr
		if cause == nil {
			cause = ErrOpenAIUpstreamStreamTruncated
		}
		logger.L().Warn("openai chat_completions raw: upstream stream truncated before terminal chunk",
			zap.Error(cause),
			zap.String("request_id", requestID),
			zap.Int64("provider_id", options.ProviderID),
			zap.String("upstream_model", upstreamModel),
			zap.Bool("saw_sse_data", terminal.SawDataLine()),
			zap.Bool("client_output_started", clientOutputStarted),
		)
		if !clientOutputStarted {
			// No semantic client output has been delivered, so classify missing
			// terminal evidence as recoverable through the caller's selected provider retry policy.
			return nil, options.TruncatedFailover(cause)
		}
		// Retain observed usage after delivered content while recording truncation;
		// the typed read error lets the handler present an incomplete stream and account failure.
		options.RecordTruncation(cause)
		return resultWithUsage(), NewUpstreamStreamReadError(cause)
	}

	if scanErr == nil && !clientDisconnected && !clientOutputStarted {
		if refusalDetector.IsSilentRefusal() {
			return nil, options.SilentRefusal()
		}
		if len(pendingLines) > 0 {
			writeStreamHeaders()
			for _, pending := range pendingLines {
				if _, werr := io.WriteString(c.Writer, pending+"\n"); werr != nil {
					clientDisconnected = true
					logger.L().Debug("openai chat_completions raw: client disconnected during final flush",
						zap.Error(werr),
						zap.String("request_id", requestID),
					)
					break
				}
			}
			if !clientDisconnected {
				c.Writer.Flush()
				clientOutputStarted = true
			}
		}
	}

	return resultWithUsage(), nil
}

// ReadRawChatBuffered observes usage and service tier before repairing and
// forwarding a buffered Chat body with HTTP 200. Supply ReadBody, observation,
// usage, transformation, headers, and tier callbacks; the executor closes the
// response body. A configured body-limit error suppresses duplicate generic
// error presentation.
//
// Explicit or sniffed SSE is inspected frame by frame for usage and tier, but
// forwarding still uses the returned transformed bytes and upstream content
// type. MissingUsage can prevent delivery. Writer errors are discarded by this
// reader, and request reasoning/tier pointers remain available in the result
// independently of the upstream body's metadata.
func ReadRawChatBuffered(c *upstream.OutputContext, resp *http.Response, options RawResponseOptions, originalModel, upstreamModel string, reasoningEffort *string, startTime time.Time) (*CompatResponseResult, error) {
	requestID := resp.Header.Get("x-request-id")

	respBody, err := options.ReadBody(resp.Body)
	if err != nil {
		// The configured size sentinel already has caller presentation; other
		// read failures receive this generic error before the wrapped cause is returned.
		if !errors.Is(err, options.BodyLimitError) {
			options.WriteError(http.StatusBadGateway, "api_error", "Failed to read upstream response")
		}
		return nil, fmt.Errorf("read upstream body: %w", err)
	}

	var usage wire.ForwardUsage
	if parsedUsage, ok := wire.ExtractOpenAIUsageFromJSONBytes(respBody); ok {
		usage = parsedUsage
	}
	if IsEventStreamResponse(resp.Header) || wire.BodyHasSSEFraming(respBody) {
		// Compatible upstreams can return SSE for a buffered request; observe
		// framed usage/tier before raw forwarding, with later recognized snapshots
		// replacing earlier ones.
		options.ObserveSSE(string(respBody))
		wire.ForEachOpenAISSEFrame(string(respBody), func(_ string, payload []byte) {
			if parsed, ok := wire.ExtractOpenAIUsageFromJSONBytes(payload); ok {
				usage = parsed
			}
			if parsed := wire.ExtractCCStreamUsage(string(payload)); parsed != nil {
				usage = *parsed
			}
		})
	} else {
		options.Observe(respBody, "response.completed")
	}
	responseModel := gjson.GetBytes(respBody, "model").String()
	if err := options.MissingUsage(responseModel, usage); err != nil {
		return nil, err
	}

	respBody = options.TransformBody(respBody)

	options.Headers(c.Writer.Header(), resp.Header)
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		c.Writer.Header().Set("Content-Type", ct)
	} else {
		c.Writer.Header().Set("Content-Type", "application/json")
	}
	c.Writer.WriteHeader(http.StatusOK)
	// Raw buffered delivery does not surface sink write errors through this
	// return; the result still contains usage observed before representation repair.
	_, _ = c.Writer.Write(respBody)

	return &CompatResponseResult{
		RequestID:       requestID,
		UpstreamHeaders: resp.Header,
		Usage:           usage,
		Model:           originalModel,
		UpstreamModel:   upstreamModel,
		ServiceTier:     options.ServiceTier(),
		ReasoningEffort: reasoningEffort,
		ResolvedTier:    options.ResolvedServiceTier(),
		Stream:          false,
		Duration:        time.Since(startTime),
	}, nil
}

// ReadRawChatConvertedBuffered reads a buffered Chat Completions response body
// (or a compatible upstream SSE stream returned for a non-streaming request)
// through convert.Exchange and writes one converted client JSON response on
// HTTP 200 while preserving native billing usage and raw model observations.
//
// Pre-conversion model declarations are captured before client model rewriting,
// HTTP-200 JSON or SSE error envelopes run through cyber-policy and failover
// classification before any client bytes are written, and MissingUsage runs
// before conversion output commits. The caller retains ownership of closing
// resp.Body after this reader returns.
func ReadRawChatConvertedBuffered(
	c *upstream.OutputContext,
	resp *http.Response,
	exchange *convert.Exchange,
	options ConvertedRawChatOptions,
	originalModel, upstreamModel string,
	reasoningEffort *string,
	startTime time.Time,
) (*CompatResponseResult, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("upstream response body is nil")
	}
	if exchange == nil {
		return nil, errors.New("conversion exchange is nil")
	}

	requestID := resp.Header.Get("x-request-id")
	errTypeDefault := defaultConvertedErrorType(options.DefaultErrorType, exchange.Info())

	readBody := options.ReadBody
	if readBody == nil {
		readBody = io.ReadAll
	}
	respBody, err := readBody(resp.Body)
	if err != nil {
		// Skip duplicate client error output when ReadBody already presented its
		// configured size-limit rejection sentinel on the downstream writer.
		if options.BodyLimitError == nil || !errors.Is(err, options.BodyLimitError) {
			writeConvertedRawChatUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, "", "Failed to read upstream response")
		}
		return nil, fmt.Errorf("read upstream body: %w", err)
	}

	var modelObserver protocol.ResponseModelObserver
	var usage wire.ForwardUsage
	var summaryServiceTier string

	resultWithUsage := func() *CompatResponseResult {
		tier := readConvertedServiceTier(options.ServiceTier, summaryServiceTier)
		if options.NormalizeServiceTier != nil && tier != "" {
			tier = options.NormalizeServiceTier(tier)
		}
		var resolved *string
		if options.ResolvedServiceTier != nil {
			resolved = options.ResolvedServiceTier()
		}
		return &CompatResponseResult{
			RequestID:             requestID,
			UpstreamHeaders:       resp.Header,
			Usage:                 usage,
			Model:                 originalModel,
			UpstreamModel:         upstreamModel,
			UpstreamResponseModel: modelObserver.Model(),
			ServiceTier:           tier,
			ReasoningEffort:       reasoningEffort,
			ResolvedTier:          resolved,
			Stream:                false,
			Duration:              time.Since(startTime),
		}
	}

	var clientJSON []byte
	if IsEventStreamResponse(resp.Header) || wire.BodyHasSSEFraming(respBody) {
		// Compatible Chat relays occasionally return text/event-stream framing on
		// stream=false requests; fold non-heartbeat frames through exchange.Buffer.
		buf := exchange.Buffer()
		var usageAccumulator wire.UsageAccumulator
		reader := sse.NewReaderLimit(bytes.NewReader(respBody), options.MaxFrameBytes)
		sawDone := false
		sawFinishReason := false

		for reader.Scan() {
			frame := reader.Frame()
			if upstream.IsSSEHeartbeatEvent(frame.Event, string(frame.Data)) {
				continue
			}
			trimmed := bytes.TrimSpace(frame.Data)
			if len(trimmed) == 0 {
				continue
			}
			if bytes.Equal(trimmed, []byte("[DONE]")) {
				sawDone = true
				_ = buf.Write(sse.Frame{Event: frame.Event, Data: trimmed})
				break
			}

			info := inspectRawChatPayload(trimmed)
			normalizedPayload, normalizedInfo := backfillHeaderOnlyChatErrorPayload(frame.Event, trimmed, info)
			if normalizedInfo.valid && normalizedInfo.model != "" {
				modelObserver.Observe(normalizedInfo.model, false)
			}
			if normalizedInfo.valid && normalizedInfo.serviceTier != "" {
				summaryServiceTier = normalizedInfo.serviceTier
			}
			if options.Observe != nil {
				options.Observe(trimmed, wire.OpenAIChatCompletionServiceTierEventType(trimmed))
			}
			// Enforce top-level usage envelope uniqueness before accumulating billing
			// counts so colliding keys match the client stream decoder's rejection rule.
			if len(normalizedInfo.usageObj) > 0 && wirejson.ValidateUsageEnvelope(trimmed) == nil {
				if usageAccumulator.Observe(normalizedInfo.usageObj) != nil {
					usage = usageAccumulator.ForwardUsage()
				}
			}

			if normalizedInfo.hasError && !normalizedInfo.hasChoices {
				if cyberErr, handled := handleBufferedRawChatCyberPolicy(c, options, normalizedPayload, usage); handled {
					return nil, cyberErr
				}
				failure := classifyRawChatStreamFailure(options, exchange, normalizedPayload, normalizedInfo, false)
				if failure.Failover != nil {
					return nil, failure.Failover
				}
				writeConvertedRawChatUnstartedError(c, options, failure.Status, failure.Type, failure.Code, failure.Message)
				return nil, fmt.Errorf("upstream response failed: %s", failure.Message)
			}

			if writeErr := buf.Write(sse.Frame{Event: frame.Event, Data: normalizedPayload}); writeErr != nil {
				logger.L().Warn("openai converted raw chat buffered: failed to fold frame",
					zap.Error(writeErr),
					zap.String("request_id", requestID),
				)
				continue
			}
			if normalizedInfo.sawFinishReason {
				sawFinishReason = true
			}
		}
		if scanErr := reader.Err(); scanErr != nil {
			if options.TruncatedFailover != nil {
				return nil, options.TruncatedFailover(scanErr)
			}
			writeConvertedRawChatUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, "", "Failed to read upstream response")
			return nil, fmt.Errorf("read buffered chat sse stream: %w", scanErr)
		}
		if !sawDone && !sawFinishReason {
			if options.TruncatedFailover != nil {
				return nil, options.TruncatedFailover(ErrOpenAIUpstreamStreamTruncated)
			}
			writeConvertedRawChatUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, OpenAIUpstreamStreamTruncatedCode, openAIRawChatConvertedTruncatedMessage)
			return nil, NewUpstreamStreamReadError(ErrOpenAIUpstreamStreamTruncated)
		}
		if options.MissingUsage != nil {
			if err := options.MissingUsage(modelObserver.Model(), usage); err != nil {
				return nil, err
			}
		}
		var closeErr error
		clientJSON, closeErr = buf.Close(nil)
		if closeErr != nil {
			writeConvertedRawChatUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, "", "Failed to parse upstream response")
			return nil, fmt.Errorf("convert buffered chat sse response: %w", closeErr)
		}
	} else {
		info := inspectRawChatPayload(respBody)
		if info.valid && info.model != "" {
			modelObserver.Observe(info.model, false)
		}
		if info.valid && info.serviceTier != "" {
			summaryServiceTier = info.serviceTier
		}
		if info.hasError && !info.hasChoices {
			if cyberErr, handled := handleBufferedRawChatCyberPolicy(c, options, respBody, usage); handled {
				return nil, cyberErr
			}
			failure := classifyRawChatStreamFailure(options, exchange, respBody, info, false)
			if failure.Failover != nil {
				return nil, failure.Failover
			}
			writeConvertedRawChatUnstartedError(c, options, failure.Status, failure.Type, failure.Code, failure.Message)
			return nil, fmt.Errorf("upstream response failed: %s", failure.Message)
		}

		if options.Observe != nil {
			options.Observe(respBody, "response.completed")
		}
		if parsedUsage, ok := wire.ExtractOpenAIUsageFromJSONBytes(respBody); ok {
			usage = parsedUsage
		}
		if options.MissingUsage != nil {
			if err := options.MissingUsage(info.model, usage); err != nil {
				return nil, err
			}
		}

		var convErr error
		clientJSON, convErr = exchange.Response(respBody)
		if convErr != nil {
			writeConvertedRawChatUnstartedError(c, options, http.StatusBadGateway, errTypeDefault, "", "Failed to parse upstream response")
			return nil, fmt.Errorf("parse chat completions response: %w", convErr)
		}
	}

	logExchangeLossesDebug(exchange, requestID)

	if options.Headers != nil {
		options.Headers(c.Writer.Header(), resp.Header)
	}
	// Force unary JSON Content-Type so downstream clients decode converted bodies
	// as JSON even when a compatible relay labeled the upstream response as SSE.
	c.Writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	c.Data(http.StatusOK, "application/json; charset=utf-8", clientJSON)

	return resultWithUsage(), nil
}

// ReadRawChatConvertedStreaming reads an upstream Chat Completions SSE stream
// through exchange.Stream, holds non-semantic leading client frames until
// semantic output or an unrecoverable terminal state arrives, and returns
// native billing usage and raw model observations across normal and error exits.
//
// Pre-output retryable stream errors, empty-stop silent refusals on large
// requests, and pre-output truncations return nil results alongside their
// failover error so outer provider loops switch cleanly without double billing.
// Early keepalive-committed HTTP 200 SSE streams receive untimed terminal
// client error frames on failure or timeout, while post-output truncations
// close open blocks via stream.Close before delivering the terminal error frame.
// Callers retain ownership of closing resp.Body except on interval timeout,
// which closes the body early to unblock a blocked scanner goroutine.
func ReadRawChatConvertedStreaming(
	c *upstream.OutputContext,
	resp *http.Response,
	exchange *convert.Exchange,
	options ConvertedRawChatOptions,
	originalModel, upstreamModel string,
	reasoningEffort *string,
	startTime time.Time,
) (*CompatResponseResult, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("upstream response body is nil")
	}
	if exchange == nil {
		return nil, errors.New("conversion exchange is nil")
	}

	requestID := resp.Header.Get("x-request-id")
	writeStreamHeaders := NewCompatStreamHeaderWriter(c, resp.Header, func(dst, src http.Header) {
		if options.Headers != nil {
			options.Headers(dst, src)
		}
	})

	stream := exchange.Stream()
	var modelObserver protocol.ResponseModelObserver
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
	sawDone := false
	sawFinishReason := false

	var cyberPolicyErr error
	var streamFailoverErr error
	var streamNonFailoverErr error

	resultWithUsage := func() *CompatResponseResult {
		summaryTier := stream.Summary().ServiceTier
		tier := readConvertedServiceTier(options.ServiceTier, summaryTier)
		if options.NormalizeServiceTier != nil && tier != "" {
			tier = options.NormalizeServiceTier(tier)
		}
		var resolved *string
		if options.ResolvedServiceTier != nil {
			resolved = options.ResolvedServiceTier()
		}
		return &CompatResponseResult{
			RequestID:             requestID,
			UpstreamHeaders:       resp.Header,
			Usage:                 usage,
			Model:                 originalModel,
			UpstreamModel:         upstreamModel,
			UpstreamResponseModel: modelObserver.Model(),
			ServiceTier:           tier,
			ReasoningEffort:       reasoningEffort,
			ResolvedTier:          resolved,
			Stream:                true,
			Duration:              time.Since(startTime),
			ClientDisconnect:      clientDisconnected,
		}
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
			logger.L().Debug("openai converted raw chat: client disconnected, continuing to drain upstream for billing",
				zap.Error(err),
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

	emitStartedAbortFrames := func(cause *ir.StreamError) {
		if clientDisconnected {
			return
		}
		closingFrames, closeErr := stream.Close(cause)
		logExchangeLossesDebug(exchange, requestID)
		// Fall back to WriteLocalTerminalError when stream.Close already ran or
		// failed so an active client stream always receives a terminal error event.
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
			errType = defaultConvertedErrorType(options.DefaultErrorType, exchange.Info())
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
		_, _ = stream.Close(cause)
		logExchangeLossesDebug(exchange, requestID)
		if isSSECommitted() && options.WriteLocalTerminalError != nil {
			options.WriteLocalTerminalError(status, errType, errCode, errMsg)
			clientOutputStarted = true
			return
		}
		writeConvertedRawChatUnstartedError(c, options, status, errType, errCode, errMsg)
		clientOutputStarted = true
	}

	deliverStreamDecoderError := func(frames []sse.Frame, sumErr *ir.StreamError) {
		errType := defaultConvertedErrorType(options.DefaultErrorType, exchange.Info())
		if sumErr.Type != "" && sumErr.Type != "stream_error" {
			errType = sumErr.Type
		}
		status := sumErr.Status
		if status <= 0 {
			status = http.StatusBadGateway
		}
		if clientOutputStarted {
			writeClosingErrorFrames(frames, &ir.StreamError{
				Type:    errType,
				Code:    sumErr.Code,
				Message: sumErr.Message,
				Status:  status,
			})
			return
		}
		// Discard buffered non-semantic frames when tool validation fails before
		// semantic output so the caller receives a clean JSON or terminal SSE error.
		stagedFrames = stagedFrames[:0]
		stagedHasSemantic = false
		if isSSECommitted() && options.WriteLocalTerminalError != nil {
			options.WriteLocalTerminalError(status, errType, sumErr.Code, sumErr.Message)
			clientOutputStarted = true
			return
		}
		writeConvertedRawChatUnstartedError(c, options, status, errType, sumErr.Code, sumErr.Message)
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
		// Preserve stream.Close's terminal frame once downstream output has started
		// so Responses streams keep continuous sequence numbers and matching response.id.
		if clientOutputStarted && len(terminalRaw) > 0 {
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

	processFrame := func(frame sse.Frame) bool {
		if upstream.IsSSEHeartbeatEvent(frame.Event, string(frame.Data)) {
			return false
		}
		trimmedPayload := bytes.TrimSpace(frame.Data)
		if len(trimmedPayload) == 0 {
			return false
		}
		if bytes.Equal(trimmedPayload, []byte("[DONE]")) {
			sawDone = true
			doneFrames, convErr := stream.Write(sse.Frame{Event: frame.Event, Data: trimmedPayload})
			if convErr != nil {
				logger.L().Warn("openai converted raw chat: failed to finalize [DONE] frame",
					zap.Error(convErr),
					zap.String("request_id", requestID),
				)
				return true
			}
			if sumErr := stream.Summary().Err; sumErr != nil {
				_, _ = stream.Close(nil)
				logExchangeLossesDebug(exchange, requestID)
				deliverStreamDecoderError(doneFrames, sumErr)
				streamNonFailoverErr = fmt.Errorf("upstream response failed: %s", sumErr.Message)
				return true
			}
			stageOrWriteClientFrames(doneFrames)
			return true
		}

		info := inspectRawChatPayload(trimmedPayload)
		normalizedPayload, normalizedInfo := backfillHeaderOnlyChatErrorPayload(frame.Event, trimmedPayload, info)

		// Capture pre-conversion upstream model and service-tier observations and
		// accumulate native billing usage before typed decoding or client rewriting.
		if normalizedInfo.valid && normalizedInfo.model != "" {
			modelObserver.Observe(normalizedInfo.model, false)
		}
		if options.Observe != nil {
			options.Observe(trimmedPayload, wire.OpenAIChatCompletionServiceTierEventType(trimmedPayload))
		}
		// Enforce top-level usage envelope uniqueness before accumulating billing
		// counts so colliding keys match the client stream decoder's rejection rule.
		if len(normalizedInfo.usageObj) > 0 && wirejson.ValidateUsageEnvelope(trimmedPayload) == nil {
			if usageAccumulator.Observe(normalizedInfo.usageObj) != nil {
				usage = usageAccumulator.ForwardUsage()
			}
		}

		if normalizedInfo.hasError && !normalizedInfo.hasChoices {
			if hit, code, msg := DetectOpenAICyberPolicy(normalizedPayload); hit {
				// Record bounded cyber-policy evidence with observed token counts
				// before client delivery so write errors never drop moderation marks.
				if options.MarkCyber != nil {
					options.MarkCyber(CyberObservation{
						Code:           code,
						Message:        msg,
						Body:           truncateConvertedUTF8(string(normalizedPayload), 4096, options.Truncate),
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

			failure := classifyRawChatStreamFailure(options, exchange, normalizedPayload, normalizedInfo, isContentWritten())
			if failure.Failover != nil {
				streamFailoverErr = failure.Failover
				return true
			}
			deliverFailure(failure.Status, failure.Type, failure.Code, failure.Message)
			streamNonFailoverErr = fmt.Errorf("upstream response failed: %s", failure.Message)
			return true
		}

		refusalDetector.ObservePayload(trimmedPayload)
		clientFrames, convErr := stream.Write(sse.Frame{Event: frame.Event, Data: normalizedPayload})
		if convErr != nil {
			logger.L().Warn("openai converted raw chat: failed to parse chat stream chunk",
				zap.Error(convErr),
				zap.String("request_id", requestID),
			)
			return false
		}
		if normalizedInfo.sawFinishReason {
			sawFinishReason = true
		}
		stageOrWriteClientFrames(clientFrames)
		return false
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

		closingFrames, closeErr := stream.Close(nil)
		logExchangeLossesDebug(exchange, requestID)
		if closeErr != nil {
			logger.L().Warn("openai converted raw chat: close error",
				zap.Error(closeErr),
				zap.String("request_id", requestID),
			)
		}
		if sumErr := stream.Summary().Err; sumErr != nil {
			deliverStreamDecoderError(closingFrames, sumErr)
			return resultWithUsage(), fmt.Errorf("upstream response failed: %s", sumErr.Message)
		}
		if len(closingFrames) > 0 && !clientDisconnected {
			if !clientOutputStarted {
				for _, f := range closingFrames {
					if f.Semantic {
						semanticOutputSeen = true
						refusalDetector.MarkSemanticOutput()
					}
					stagedFrames = append(stagedFrames, f)
				}
			} else {
				for _, f := range closingFrames {
					if !writeSingleClientFrame(f) {
						break
					}
				}
			}
		}

		// Evaluate large-request silent refusal before flushing staged non-semantic
		// frames so an empty stop stream can still switch providers cleanly.
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
		if !sawDone {
			LogCCStreamMissingDoneSentinel("openai converted raw chat", requestID)
		}
		return resultWithUsage(), nil
	}

	handleScanErr := func(scanErr error) (*CompatResponseResult, error) {
		if scanErr != nil && !errors.Is(scanErr, context.Canceled) && !errors.Is(scanErr, context.DeadlineExceeded) {
			logger.FromContext(options.RequestContext).Warn("openai converted raw chat: stream read error",
				zap.Error(scanErr),
				zap.String("request_id", requestID),
			)
		}
		// Downstream disconnects and caller context cancellations suppress upstream
		// truncation penalties while closing any active downstream SSE stream cleanly.
		if clientDisconnected || errors.Is(scanErr, context.Canceled) || errors.Is(scanErr, context.DeadlineExceeded) {
			if clientOutputStarted && !clientDisconnected {
				emitStartedAbortFrames(&ir.StreamError{
					Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					Message: "Stream interrupted before completion",
					Status:  http.StatusBadGateway,
				})
			} else if !clientOutputStarted && isSSECommitted() && !clientDisconnected && options.WriteLocalTerminalError != nil {
				_, _ = stream.Close(scanErr)
				logExchangeLossesDebug(exchange, requestID)
				options.WriteLocalTerminalError(
					http.StatusBadGateway,
					defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					"",
					"Stream interrupted before completion",
				)
			}
			return resultWithUsage(), fmt.Errorf("stream usage incomplete: %w", scanErr)
		}

		if !isContentWritten() {
			if options.TruncatedFailover != nil {
				return nil, options.TruncatedFailover(scanErr)
			}
			if options.ReadFailure != nil {
				return nil, options.ReadFailure(scanErr)
			}
			return nil, NewUpstreamStreamReadError(scanErr)
		}

		if options.RecordTruncation != nil {
			options.RecordTruncation(scanErr)
		}
		var classified error
		if options.ReadFailure != nil {
			classified = options.ReadFailure(scanErr)
		} else {
			classified = NewUpstreamStreamReadError(scanErr)
		}
		code, _ := ClassifyUpstreamStreamReadError(scanErr)
		emitStartedAbortFrames(&ir.StreamError{
			Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
			Code:    code,
			Message: "Upstream stream disconnected before completion",
			Status:  http.StatusBadGateway,
		})
		return resultWithUsage(), classified
	}

	finishAtEOF := func() (*CompatResponseResult, error) {
		if cyberPolicyErr != nil || streamFailoverErr != nil || streamNonFailoverErr != nil {
			return finalizeStream()
		}
		if !sawDone && !sawFinishReason {
			if clientDisconnected {
				return resultWithUsage(), fmt.Errorf("stream usage incomplete: %w", ErrOpenAIUpstreamStreamTruncated)
			}
			logger.L().Warn("openai converted raw chat: upstream stream truncated before terminal chunk",
				zap.Error(ErrOpenAIUpstreamStreamTruncated),
				zap.String("request_id", requestID),
				zap.Int64("provider_id", options.ProviderID),
				zap.String("upstream_model", upstreamModel),
				zap.Bool("client_output_started", isContentWritten()),
			)
			if !isContentWritten() {
				if options.TruncatedFailover != nil {
					return nil, options.TruncatedFailover(ErrOpenAIUpstreamStreamTruncated)
				}
				deliverFailure(
					http.StatusBadGateway,
					defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
					OpenAIUpstreamStreamTruncatedCode,
					openAIRawChatConvertedTruncatedMessage,
				)
				return nil, NewUpstreamStreamReadError(ErrOpenAIUpstreamStreamTruncated)
			}
			if options.RecordTruncation != nil {
				options.RecordTruncation(ErrOpenAIUpstreamStreamTruncated)
			}
			emitStartedAbortFrames(&ir.StreamError{
				Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
				Code:    OpenAIUpstreamStreamTruncatedCode,
				Message: openAIRawChatConvertedTruncatedMessage,
				Status:  http.StatusBadGateway,
			})
			var classified error
			if options.ReadFailure != nil {
				classified = options.ReadFailure(ErrOpenAIUpstreamStreamTruncated)
			} else {
				classified = NewUpstreamStreamReadError(ErrOpenAIUpstreamStreamTruncated)
			}
			return resultWithUsage(), classified
		}
		return finalizeStream()
	}

	reader := sse.NewReaderLimit(resp.Body, options.MaxFrameBytes)
	streamInterval := readConvertedStreamInterval(options.StreamInterval)

	// Scan synchronously when per-chunk idle interval timeouts are disabled so
	// ordinary Chat streams avoid spawning a background producer goroutine.
	if streamInterval <= 0 {
		for reader.Scan() {
			if processFrame(reader.Frame()) {
				return finalizeStream()
			}
		}
		if err := reader.Err(); err != nil {
			return handleScanErr(err)
		}
		return finishAtEOF()
	}

	intervalTicker := time.NewTicker(streamInterval)
	defer intervalTicker.Stop()
	intervalCh := intervalTicker.C

	// rawChatFrameEvent pairs one scanned SSE frame or reader error from the producer goroutine.
	type rawChatFrameEvent struct {
		frame sse.Frame // scanned SSE frame
		err   error     // non-EOF reader failure when non-nil
	}
	events := make(chan rawChatFrameEvent, 16)
	done := make(chan struct{})
	var lastReadAt int64
	atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
	sendEvent := func(ev rawChatFrameEvent) bool {
		select {
		case events <- ev:
			return true
		case <-done:
			return false
		}
	}
	// Refresh lastReadAt as soon as a complete SSE frame arrives from upstream so
	// slow downstream writes do not trigger false upstream idle timeouts.
	go func() {
		defer close(events)
		for reader.Scan() {
			atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
			if !sendEvent(rawChatFrameEvent{frame: reader.Frame()}) {
				return
			}
		}
		if err := reader.Err(); err != nil {
			_ = sendEvent(rawChatFrameEvent{err: err})
		}
	}()
	defer close(done)

	for {
		select {
		case ev, ok := <-events:
			if !ok {
				return finishAtEOF()
			}
			if ev.err != nil {
				return handleScanErr(ev.err)
			}
			if processFrame(ev.frame) {
				return finalizeStream()
			}

		case <-intervalCh:
			lastRead := time.Unix(0, atomic.LoadInt64(&lastReadAt))
			if time.Since(lastRead) < streamInterval {
				continue
			}
			// Close resp.Body before returning so a producer goroutine blocked in
			// network Read wakes immediately when the idle interval expires.
			_ = resp.Body.Close()
			if clientDisconnected {
				return resultWithUsage(), fmt.Errorf("stream usage incomplete after timeout")
			}
			logger.L().Warn("openai converted raw chat: data interval timeout",
				zap.String("request_id", requestID),
				zap.String("model", originalModel),
				zap.Duration("interval", streamInterval),
			)
			timeoutMsg := "Upstream stream timed out waiting for data"
			timeoutCause := &ir.StreamError{
				Type:    defaultConvertedErrorType(options.DefaultErrorType, exchange.Info()),
				Message: timeoutMsg,
				Status:  http.StatusGatewayTimeout,
			}
			if clientOutputStarted {
				emitStartedAbortFrames(timeoutCause)
			} else if isSSECommitted() && options.WriteLocalTerminalError != nil {
				_, _ = stream.Close(timeoutCause)
				logExchangeLossesDebug(exchange, requestID)
				options.WriteLocalTerminalError(
					http.StatusGatewayTimeout,
					timeoutCause.Type,
					"",
					timeoutMsg,
				)
			}
			return resultWithUsage(), fmt.Errorf("stream data interval timeout")
		}
	}
}

// inspectRawChatPayload scans top-level model, service_tier, usage, choices,
// finish_reason, and error fields from raw Chat Completions JSON bytes via
// jsontext without round-tripping through untyped maps or losing numeric
// precision. Non-object inputs, malformed tokens, or trailing data after the
// root object return an invalid summary with any fields already visited before
// the syntax error.
func inspectRawChatPayload(payload []byte) rawChatPayloadInfo {
	dec := jsontext.NewDecoder(bytes.NewReader(payload), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return rawChatPayloadInfo{}
	}

	var info rawChatPayloadInfo
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil || kTok.Kind() != '"' {
			return info
		}
		key := kTok.String()
		val, err := dec.ReadValue()
		if err != nil {
			return info
		}
		switch key {
		case "model":
			var modelStr string
			if jsonv2.Unmarshal(val, &modelStr, jsontext.AllowInvalidUTF8(true)) == nil {
				info.model = strings.TrimSpace(modelStr)
			}
		case "service_tier":
			var tierStr string
			if jsonv2.Unmarshal(val, &tierStr, jsontext.AllowInvalidUTF8(true)) == nil {
				info.serviceTier = strings.TrimSpace(tierStr)
			}
		case "usage":
			trimmedUsage := bytes.TrimSpace(val)
			if len(trimmedUsage) > 0 && trimmedUsage[0] == '{' {
				info.usageObj = bytes.Clone(trimmedUsage)
			}
		case "choices":
			trimmedChoices := bytes.TrimSpace(val)
			if len(trimmedChoices) > 0 && trimmedChoices[0] == '[' {
				info.hasChoices = true
				info.sawFinishReason = scanRawChatChoicesFinishReason(trimmedChoices)
			}
		case "error":
			parseRawChatErrorValue(val, &info)
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return info
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return info
	}
	info.valid = true
	return info
}

// scanRawChatChoicesFinishReason reports whether any choice object in rawArray
// carries a nonblank string finish_reason member. Malformed choice JSON arrays,
// null finish_reason fields, and whitespace-only strings all return false.
func scanRawChatChoicesFinishReason(rawArray jsontext.Value) bool {
	// choiceFinishProbe decodes nullable finish_reason strings from each choices array element.
	type choiceFinishProbe struct {
		FinishReason *string `json:"finish_reason"` // nullable completion boundary reason
	}
	var choices []choiceFinishProbe
	if jsonv2.Unmarshal(rawArray, &choices, jsontext.AllowInvalidUTF8(true)) != nil {
		return false
	}
	for _, ch := range choices {
		if ch.FinishReason != nil && strings.TrimSpace(*ch.FinishReason) != "" {
			return true
		}
	}
	return false
}

// parseRawChatErrorValue extracts error type, code, message, and HTTP status
// from a non-null Chat error JSON value into info. Empty or JSON null values
// leave info unchanged, whereas scalar strings and non-object values still set
// hasError so upstream error frames never slip past failure classification.
func parseRawChatErrorValue(rawErr jsontext.Value, info *rawChatPayloadInfo) {
	trimmed := bytes.TrimSpace(rawErr)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return
	}
	info.hasError = true
	if trimmed[0] == '"' {
		var msg string
		if jsonv2.Unmarshal(trimmed, &msg, jsontext.AllowInvalidUTF8(true)) == nil {
			info.errMessage = strings.TrimSpace(msg)
		}
		return
	}
	// rawChatErrorFields decodes structured error message, category, code, and status members.
	type rawChatErrorFields struct {
		Message string         `json:"message"` // human-readable error message
		Type    string         `json:"type"`    // error category label
		Code    jsontext.Value `json:"code"`    // string or numeric error code
		Status  int            `json:"status"`  // optional HTTP status code
	}
	var parsed rawChatErrorFields
	if jsonv2.Unmarshal(trimmed, &parsed, jsontext.AllowInvalidUTF8(true)) != nil {
		return
	}
	info.errMessage = strings.TrimSpace(parsed.Message)
	info.errType = strings.TrimSpace(parsed.Type)
	info.errStatus = parsed.Status
	codeTrimmed := bytes.TrimSpace(parsed.Code)
	if len(codeTrimmed) > 0 && !bytes.Equal(codeTrimmed, []byte("null")) {
		if codeTrimmed[0] == '"' {
			var codeStr string
			if jsonv2.Unmarshal(codeTrimmed, &codeStr, jsontext.AllowInvalidUTF8(true)) == nil {
				info.errCode = strings.TrimSpace(codeStr)
			}
		} else {
			info.errCode = string(codeTrimmed)
		}
	}
}

// backfillHeaderOnlyChatErrorPayload wraps flat event: error JSON payloads that
// omit a top-level "error" envelope into {"error": ...} so downstream error
// classifiers and codecs inspect a uniform Chat error shape. Ordinary chunks
// and frames that already carry "error" or "choices" return unchanged.
func backfillHeaderOnlyChatErrorPayload(eventHeader string, rawPayload []byte, info rawChatPayloadInfo) ([]byte, rawChatPayloadInfo) {
	if info.hasError || info.hasChoices || !strings.EqualFold(strings.TrimSpace(eventHeader), "error") {
		return rawPayload, info
	}
	// wrappedChatError nests bare event: error payloads inside a top-level error member.
	type wrappedChatError struct {
		Error jsontext.Value `json:"error"` // wrapped raw error value
	}
	// Wrap bare event: error objects or strings into {"error": <payload>} so
	// OpenAIStreamFailedEventShouldFailover and DetectOpenAICyberPolicy read them.
	if info.valid || (len(rawPayload) > 0 && rawPayload[0] == '"') {
		wrapped, err := jsonv2.Marshal(wrappedChatError{Error: jsontext.Value(bytes.Clone(rawPayload))}, jsontext.AllowInvalidUTF8(true))
		if err == nil {
			reinspected := inspectRawChatPayload(wrapped)
			if reinspected.hasError {
				return wrapped, reinspected
			}
		}
	}
	rawMsg, err := jsonv2.Marshal(string(rawPayload), jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return rawPayload, info
	}
	// wrappedMsg packages plain-text error lines into a structured message object.
	type wrappedMsg struct {
		Message jsontext.Value `json:"message"` // escaped plain-text error message
	}
	msgObj, err := jsonv2.Marshal(wrappedMsg{Message: rawMsg}, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return rawPayload, info
	}
	wrapped, err := jsonv2.Marshal(wrappedChatError{Error: msgObj}, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return rawPayload, info
	}
	return wrapped, inspectRawChatPayload(wrapped)
}

// handleBufferedRawChatCyberPolicy checks payload for OpenAI cyber-policy
// markers during buffered Chat conversion, recording moderation telemetry and
// writing an unstarted client rejection when matched. Payloads without cyber
// markers return nil and false without invoking moderation or error callbacks.
func handleBufferedRawChatCyberPolicy(
	c *upstream.OutputContext,
	options ConvertedRawChatOptions,
	payload []byte,
	usage wire.ForwardUsage,
) (error, bool) {
	hit, code, msg := DetectOpenAICyberPolicy(payload)
	if !hit {
		return nil, false
	}
	// Record up to 4 KiB of cyber-policy evidence with observed token counts
	// before client rejection delivery so write errors never drop moderation marks.
	if options.MarkCyber != nil {
		options.MarkCyber(CyberObservation{
			Code:           code,
			Message:        msg,
			Body:           truncateConvertedUTF8(string(payload), 4096, options.Truncate),
			UpstreamStatus: http.StatusOK,
			UpstreamInTok:  usage.InputTokens,
			UpstreamOutTok: usage.OutputTokens,
		})
	}
	clientMsg := msg
	if clientMsg == "" {
		clientMsg = "Request blocked by upstream cyber-security policy"
	}
	writeConvertedRawChatUnstartedError(c, options, http.StatusBadRequest, "invalid_request_error", code, clientMsg)
	if options.CyberForwarded != nil {
		return options.CyberForwarded, true
	}
	return errors.New("openai cyber_policy forwarded"), true
}

// classifyRawChatStreamFailure delegates Chat error-frame classification to
// options.StreamFailure when configured and falls back to building a 502
// ConvertedFailure from info's parsed error metadata otherwise. Blank error
// messages default to a generic Chat stream error label before classification.
func classifyRawChatStreamFailure(
	options ConvertedRawChatOptions,
	exchange *convert.Exchange,
	payload []byte,
	info rawChatPayloadInfo,
	outputStarted bool,
) ConvertedFailure {
	msg := info.errMessage
	if msg == "" {
		msg = "Upstream chat stream error"
	}
	if options.StreamFailure != nil {
		return options.StreamFailure(payload, msg, outputStarted)
	}
	errType := info.errType
	if errType == "" {
		errType = defaultConvertedErrorType(options.DefaultErrorType, exchange.Info())
	}
	status := info.errStatus
	if status < http.StatusBadRequest {
		status = http.StatusBadGateway
	}
	return ConvertedFailure{
		Status:  status,
		Type:    errType,
		Code:    info.errCode,
		Message: msg,
	}
}

// writeConvertedRawChatUnstartedError delegates unstarted error presentation to
// options.WriteError when configured, falling back to a plain JSON error
// envelope on c when WriteError is nil. Nil output contexts or writers are
// ignored safely when no custom WriteError callback is supplied.
func writeConvertedRawChatUnstartedError(
	c *upstream.OutputContext,
	options ConvertedRawChatOptions,
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
	// jsonErrBody formats the inner error object when no gateway WriteError callback is bound.
	type jsonErrBody struct {
		Type    string `json:"type"`           // client error category
		Code    string `json:"code,omitempty"` // optional error code
		Message string `json:"message"`        // client-safe error message
	}
	// jsonErrEnvelope wraps jsonErrBody in a standard top-level error object.
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
