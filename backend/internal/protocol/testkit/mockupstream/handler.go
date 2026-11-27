package mockupstream

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// handler.go implements the scripted upstream HTTP handler, strict request
// validation gate, IR-backed turn selector, synthetic reply encoder, fault
// injector, and thread-safe request and violation logs for protocol conversion
// testing. Gateway route tests (T5) and real client subprocess tests (T6) point
// upstream provider URLs at Handler to verify converted wire requests and
// exercise failover, keepalive, and mid-stream disconnects without external
// network dependencies.
//
// Every recognized generation request runs testkit.ValidateRequestRules before
// turn matching, decodes registered protocols into ir.Request values for
// Turn.Expect predicates, and synthesizes Reply.Events or Reply.Response wire
// payloads through convert when Reply.Raw is absent. Turn selection matches
// conversation history counts rather than a global request counter so client
// background requests fall through to Script.Default without shifting turn
// alignment, and completed records settle under the handler mutex before
// response bytes or connection closes flush to downstream readers.

const (
	// Mode constants select whether Handler evaluates declarative Script turns,
	// replays ordered raw fixtures, or reverse-proxies to a live upstream.
	ModeScripted    Mode = "scripted"     // match requests by conversation shape against Script
	ModeReplay      Mode = "replay"       // emit configured replies in arrival order
	ModeProxyRecord Mode = "proxy-record" // forward to TargetURL and record bidirectional bytes

	// defaultSyntheticUnix fixes response created_at timestamps when Handler
	// synthesizes Reply.Events or Reply.Response payloads without a custom clock.
	defaultSyntheticUnix = 1760000000 // deterministic UTC Unix timestamp in seconds
)

// utf8BOM marks the optional three-byte UTF-8 byte-order mark stripped from
// the start of raw SSE streams during content-type and frame-boundary checks.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// Mode identifies the operating mode of a Handler instance.
type Mode string

// CaptureOptions configures optional persistence of IncomingCapture JSON
// bundles (*.capture.json) for later sanitization via cmd/corpus import.
type CaptureOptions struct {
	// Dir specifies the destination _incoming directory for capture bundles;
	// leaving Dir empty or whitespace-only disables disk capture writes.
	Dir string
	// FilePrefix overrides the sanitized filename stem for written capture
	// bundles; empty values fall back to Meta.Scenario and then "capture".
	FilePrefix string
	// Meta supplies base provenance metadata for written capture bundles, while
	// empty Protocol and Model fields fill from the observed request and endpoint.
	Meta testkit.CorpusMeta
	// RecordRequests and RecordStreams select which payload directions
	// maybeWriteCaptureBundle writes into each capture bundle. Leaving both
	// false when Dir is non-empty captures both JSON request bodies and SSE
	// response streams by default.
	RecordRequests bool // include raw JSON request body in capture bundle
	RecordStreams  bool // include raw SSE response stream in capture bundle
}

// Config bundles execution mode, turn scripts, corpus lookup, strict validation
// options, proxy target settings, and disk capture settings for NewWithConfig.
type Config struct {
	// Mode selects scripted matching, sequential replay, or reverse-proxy
	// recording; an empty Mode defaults to ModeScripted in NewWithConfig.
	Mode Mode
	// Script defines turn matchers, Expect checks, faults, and the Default
	// fallback reply; NewWithConfig clones its Turns and Expect slices.
	Script testkit.Script
	// Replay supplies ordered replies for ModeReplay and takes precedence over
	// Script.Turns; leaving Replay empty falls back to Script.Turns in order.
	Replay []testkit.Reply
	// Corpus resolves relative testdata paths referenced by Reply.Raw; nil
	// defaults to testkit.DefaultCorpus in NewWithConfig.
	Corpus *testkit.Corpus
	// Validation configures strict wire-request rules and dialect checks run
	// before turn matching in ModeScripted and ModeReplay.
	Validation testkit.RequestValidationOptions
	// TargetURL specifies the upstream base URL for ModeProxyRecord; blank or
	// hostless values fail proxy requests with HTTP 502.
	TargetURL string
	// HTTPClient executes outbound requests in ModeProxyRecord; nil defaults to
	// a standard http.Client with transparent gzip decompression enabled.
	HTTPClient *http.Client
	Capture    CaptureOptions // optional _incoming capture bundle writer settings
}

// ViolationEntry records strict request validation failures or failed Turn
// expectations observed on one handled request.
type ViolationEntry struct {
	RequestIndex   int                     // zero-based position in Handler.Requests
	Method         string                  // HTTP request method
	Path           string                  // HTTP request URL path
	Protocol       string                  // canonical protocol ID, or empty when unrecognized
	RuleViolations []testkit.RuleViolation // strict wire-request rule violations
	CheckErrors    []string                // Turn.Expect, unmatched-turn, or reply errors
}

// RecordedRequest stores a complete snapshot of an incoming HTTP request, its
// turn-matching classification, any validation errors, and the emitted reply.
type RecordedRequest struct {
	Index           int                     // zero-based arrival sequence number
	Method          string                  // HTTP method such as POST
	Path            string                  // URL path without query parameters
	RawQuery        string                  // raw URL query string
	Headers         http.Header             // cloned incoming HTTP headers
	Body            []byte                  // cloned incoming request body bytes
	Protocol        string                  // canonical protocol ID, or empty when unrecognized
	Decoded         *ir.Request             // decoded IR request when codec is registered, or nil
	Summary         RequestSummary          // wire-JSON or IR-derived conversation summary
	MatchedTurn     int                     // matched Script.Turns or Replay index, or -1
	UsedDefault     bool                    // true when Script.Default served the request
	RuleViolations  []testkit.RuleViolation // strict request validator violations
	CheckErrors     []string                // failed Expect predicates or reply resolution errors
	ResponseStatus  int                     // HTTP status code written to the caller, or 0 on cancel
	ResponseHeaders http.Header             // cloned response headers sent to the caller
	ResponseBody    []byte                  // cloned response body bytes sent before return
}

// Handler implements http.Handler as a configurable scripted, replay, or
// reverse-proxy upstream for protocol conversion tests.
type Handler struct {
	cfg Config // immutable configuration resolved by NewWithConfig

	// mu serializes concurrent reads and updates across turnHits, replayCursor,
	// captureSeq, synthSeq, requests, and violations so HTTP handler goroutines record
	// completed exchanges before test goroutines inspect cloned snapshots or reset state.
	mu           sync.Mutex        // guards all mutable state below
	turnHits     []int             // invocation counts per Script.Turns index
	replayCursor int               // next sequential reply index in ModeReplay
	captureSeq   int               // one-based counter for written capture files
	synthSeq     int               // monotonic counter for synthesized response IDs
	requests     []RecordedRequest // chronological log of handled HTTP requests
	violations   []ViolationEntry  // subset of requests with rule or check errors
}

// resolvedReply holds the raw wire bytes and HTTP Content-Type resolved from a Reply.
type resolvedReply struct {
	data        []byte // verbatim response payload bytes
	contentType string // HTTP Content-Type header value
}

// anthropicErrorEnvelope serializes an outer Anthropic Messages HTTP JSON error envelope.
type anthropicErrorEnvelope struct {
	Type  string               `json:"type"`  // fixed "error" envelope discriminator
	Error anthropicErrorDetail `json:"error"` // inner Anthropic error detail object
}

// anthropicErrorDetail holds the type and message of an Anthropic error response.
type anthropicErrorDetail struct {
	Type    string `json:"type"`    // Anthropic error category such as "invalid_request_error"
	Message string `json:"message"` // human-readable error description
}

// openAIErrorEnvelope serializes an OpenAI Responses or Chat HTTP error payload.
type openAIErrorEnvelope struct {
	Error openAIErrorDetail `json:"error"` // inner OpenAI error detail object
}

// openAIErrorDetail holds message, type, param, and code fields for OpenAI errors.
type openAIErrorDetail struct {
	Message string `json:"message"`        // human-readable error description
	Type    string `json:"type"`           // OpenAI error type such as "invalid_request_error"
	Param   string `json:"param,omitzero"` // offending JSON path or parameter name
	Code    string `json:"code"`           // machine-readable error code
}

// geminiErrorEnvelope serializes an outer Google Gemini HTTP JSON error envelope.
type geminiErrorEnvelope struct {
	Error geminiErrorDetail `json:"error"` // inner Google RPC status detail object
}

// geminiErrorDetail holds numeric code, message, and canonical status for Gemini errors.
type geminiErrorDetail struct {
	Code    int    `json:"code"`    // HTTP status code
	Message string `json:"message"` // human-readable error description
	Status  string `json:"status"`  // canonical gRPC status string such as "INVALID_ARGUMENT"
}

// New constructs a ModeScripted Handler for script backed by
// testkit.DefaultCorpus and default strict request validation options. Callers
// needing custom validation dialect rules, alternate corpora, replay mode, or
// proxy recording should call NewWithConfig instead.
func New(script testkit.Script) *Handler {
	return NewWithConfig(Config{Script: script})
}

// NewWithConfig constructs a Handler from cfg, defaulting Mode to ModeScripted,
// Corpus to testkit.DefaultCorpus, and HTTPClient to a standard http.Client
// when ModeProxyRecord is selected. Caller-owned slices inside cfg.Script and
// cfg.Replay are cloned so subsequent caller edits cannot mutate Handler state.
func NewWithConfig(cfg Config) *Handler {
	if cfg.Mode == "" {
		cfg.Mode = ModeScripted
	}
	if cfg.Corpus == nil {
		cfg.Corpus = testkit.DefaultCorpus()
	}
	if cfg.Mode == ModeProxyRecord && cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	cfg.Script.Turns = slices.Clone(cfg.Script.Turns)
	for i := range cfg.Script.Turns {
		cfg.Script.Turns[i].Expect = slices.Clone(cfg.Script.Turns[i].Expect)
	}
	cfg.Replay = slices.Clone(cfg.Replay)
	cfg.Validation.DialectRules = slices.Clone(cfg.Validation.DialectRules)
	return &Handler{
		cfg:      cfg,
		turnHits: make([]int, len(cfg.Script.Turns)),
	}
}

// ServeHTTP dispatches incoming upstream HTTP requests according to Handler's
// Mode while recording every exchange in arrival order. Unrecognized paths and
// non-POST generation calls in ModeScripted or ModeReplay return 404 and 405
// without recording strict protocol violations.
//
// Recognized POST generation requests run strict wire-request validation before
// turn matching and return a provider-native HTTP 400 payload whenever rules
// fail. ModeProxyRecord forwards requests directly to Config.TargetURL so live
// upstream providers perform their own validation and streaming responses.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, readErr := io.ReadAll(r.Body)
	_ = r.Body.Close()

	baseRec := RecordedRequest{
		Method:      r.Method,
		Path:        r.URL.Path,
		RawQuery:    r.URL.RawQuery,
		Headers:     r.Header.Clone(),
		Body:        bytes.Clone(body),
		MatchedTurn: -1,
	}
	endpoint, hasEndpoint := IdentifyEndpoint(r.URL.Path, r.URL.RawQuery)
	if hasEndpoint {
		baseRec.Protocol = endpoint.Protocol
	}

	if readErr != nil {
		baseRec.CheckErrors = []string{fmt.Sprintf("read request body: %v", readErr)}
		errProtocol := endpoint.Protocol
		if errProtocol == "" {
			errProtocol = protocolOpenAIResponses
		}
		respHeaders, respBody := prepareProviderError(errProtocol, http.StatusBadRequest, "$", baseRec.CheckErrors[0])
		baseRec.ResponseStatus = http.StatusBadRequest
		baseRec.ResponseHeaders = respHeaders.Clone()
		baseRec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(baseRec)
		writePreparedResponse(w, http.StatusBadRequest, respHeaders, respBody)
		return
	}

	if h.cfg.Mode == ModeProxyRecord {
		var summary RequestSummary
		if hasEndpoint && r.Method == http.MethodPost && len(body) > 0 {
			// Ignore JSON inspection errors in ModeProxyRecord so malformed payloads
			// still reach the live target server and return authentic upstream errors.
			if parsed, err := InspectRequest(endpoint, body); err == nil {
				summary = parsed
			}
		}
		h.serveProxyRecord(w, r, endpoint, hasEndpoint, body, summary, baseRec)
		return
	}

	if !hasEndpoint {
		respHeaders, respBody := prepareProviderError(protocolAnthropicMessages, http.StatusNotFound, "", fmt.Sprintf("unrecognized upstream path %q", r.URL.Path))
		baseRec.ResponseStatus = http.StatusNotFound
		baseRec.ResponseHeaders = respHeaders.Clone()
		baseRec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(baseRec)
		writePreparedResponse(w, http.StatusNotFound, respHeaders, respBody)
		return
	}

	if r.Method != http.MethodPost {
		respHeaders, respBody := prepareProviderError(endpoint.Protocol, http.StatusMethodNotAllowed, "", fmt.Sprintf("method %s not allowed on %s", r.Method, r.URL.Path))
		baseRec.ResponseStatus = http.StatusMethodNotAllowed
		baseRec.ResponseHeaders = respHeaders.Clone()
		baseRec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(baseRec)
		writePreparedResponse(w, http.StatusMethodNotAllowed, respHeaders, respBody)
		return
	}

	if violations := h.validateIncomingRequest(endpoint, body); len(violations) > 0 {
		baseRec.RuleViolations = violations
		msgs := make([]string, 0, len(violations))
		for _, v := range violations {
			msgs = append(msgs, v.String())
		}
		respHeaders, respBody := prepareProviderError(endpoint.Protocol, http.StatusBadRequest, violations[0].Path, strings.Join(msgs, "; "))
		baseRec.ResponseStatus = http.StatusBadRequest
		baseRec.ResponseHeaders = respHeaders.Clone()
		baseRec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(baseRec)
		writePreparedResponse(w, http.StatusBadRequest, respHeaders, respBody)
		return
	}

	decodedReq, summary, err := DecodeAndSummarizeRequest(endpoint, body)
	if err != nil {
		baseRec.CheckErrors = []string{fmt.Sprintf("inspect request JSON: %v", err)}
		respHeaders, respBody := prepareProviderError(endpoint.Protocol, http.StatusBadRequest, "$", baseRec.CheckErrors[0])
		baseRec.ResponseStatus = http.StatusBadRequest
		baseRec.ResponseHeaders = respHeaders.Clone()
		baseRec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(baseRec)
		writePreparedResponse(w, http.StatusBadRequest, respHeaders, respBody)
		return
	}
	baseRec.Decoded = decodedReq
	baseRec.Summary = summary

	if h.cfg.Mode == ModeReplay {
		h.serveReplay(w, r, endpoint, body, summary, baseRec)
		return
	}
	h.serveScripted(w, r, endpoint, body, summary, baseRec)
}

// Requests returns deep-cloned snapshots of every HTTP request handled so far
// in chronological arrival order. Callers may freely mutate the returned
// slices, headers, and byte buffers without affecting Handler state.
func (h *Handler) Requests() []RecordedRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]RecordedRequest, len(h.requests))
	for i, rec := range h.requests {
		out[i] = cloneRecordedRequest(rec)
	}
	return out
}

// Violations returns deep-cloned snapshots of all recorded strict validation
// failures, failed Turn.Expect assertions, and unmatched-turn errors. An empty
// slice indicates every handled request satisfied validation and script checks.
func (h *Handler) Violations() []ViolationEntry {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]ViolationEntry, len(h.violations))
	for i, v := range h.violations {
		out[i] = cloneViolationEntry(v)
	}
	return out
}

// AssertNoViolations fails tb via Fatalf with a formatted per-request
// diagnostic summary whenever Handler recorded any strict request rule
// violation or check failure. When Violations is empty, AssertNoViolations
// returns without side effects.
func (h *Handler) AssertNoViolations(tb testing.TB) {
	tb.Helper()
	entries := h.Violations()
	if len(entries) == 0 {
		return
	}
	var b strings.Builder
	fmt.Fprintf(&b, "mockupstream recorded %d request violation(s):", len(entries))
	for _, entry := range entries {
		fmt.Fprintf(&b, "\n  request[%d] %s %s (%s):", entry.RequestIndex, entry.Method, entry.Path, entry.Protocol)
		for _, rv := range entry.RuleViolations {
			fmt.Fprintf(&b, "\n    - %s", rv.String())
		}
		for _, ce := range entry.CheckErrors {
			fmt.Fprintf(&b, "\n    - check failed: %s", ce)
		}
	}
	tb.Fatalf("%s", b.String())
}

// Reset clears all recorded requests, violations, turn hit counts, capture
// counters, and replay cursors under Handler's mutex. Configured Script turns,
// validation rules, and Corpus references remain intact for subsequent subtests.
func (h *Handler) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	clear(h.turnHits)
	h.replayCursor = 0
	h.captureSeq = 0
	h.synthSeq = 0
	h.requests = nil
	h.violations = nil
}

// serveScripted matches an inspected request against Script.Turns or
// Script.Default, evaluates Turn.Expect predicates, and delivers the reply.
// Unmatched requests when Script.Default is empty log a violation and return a
// provider-shaped HTTP 400 diagnostic.
func (h *Handler) serveScripted(w http.ResponseWriter, r *http.Request, endpoint Endpoint, body []byte, summary RequestSummary, rec RecordedRequest) {
	turnIdx, turn, matched := h.selectScriptTurn(summary, body)
	if !matched {
		if !hasConfiguredReply(h.cfg.Script.Default) {
			msg := fmt.Sprintf(
				"no script turn matched request (assistant_messages=%d, tool_results=%d, model=%q, has_tools=%v) and Script.Default is empty",
				summary.AssistantMessages,
				summary.ToolResults,
				summary.Model,
				summary.HasTools,
			)
			rec.CheckErrors = []string{msg}
			respHeaders, respBody := prepareProviderError(endpoint.Protocol, http.StatusBadRequest, "$", msg)
			rec.ResponseStatus = http.StatusBadRequest
			rec.ResponseHeaders = respHeaders.Clone()
			rec.ResponseBody = bytes.Clone(respBody)
			h.recordExchange(rec)
			writePreparedResponse(w, http.StatusBadRequest, respHeaders, respBody)
			return
		}
		rec.UsedDefault = true
		h.deliverReply(w, r, endpoint, summary, h.cfg.Script.Default, nil, rec)
		return
	}

	rec.MatchedTurn = turnIdx
	if checkErrs := evaluateChecks(turn.Expect, rec.Decoded, body); len(checkErrs) > 0 {
		rec.CheckErrors = checkErrs
		msg := strings.Join(checkErrs, "; ")
		respHeaders, respBody := prepareProviderError(endpoint.Protocol, http.StatusBadRequest, "$", msg)
		rec.ResponseStatus = http.StatusBadRequest
		rec.ResponseHeaders = respHeaders.Clone()
		rec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(rec)
		writePreparedResponse(w, http.StatusBadRequest, respHeaders, respBody)
		return
	}

	h.deliverReply(w, r, endpoint, summary, turn.Reply, turn.Fault, rec)
}

// validateIncomingRequest maps endpoint.Protocol to its canonical protocol ID
// and runs strict request validation over body using Handler's Validation
// options. Unrecognized protocol strings return nil immediately without
// reporting any rule violation.
func (h *Handler) validateIncomingRequest(endpoint Endpoint, body []byte) []testkit.RuleViolation {
	proto, ok := testkit.ParseProtocolDirName(endpoint.Protocol)
	if !ok {
		return nil
	}
	return testkit.ValidateRequestRules(proto, body, h.cfg.Validation)
}

// selectScriptTurn locates the matching Turn in Script.Turns by conversation
// shape under h.mu and returns its index, Turn value, and whether any turn
// matched. Unmatched requests leave turnHits unchanged and return false so
// callers can fall back to Script.Default.
//
// Distinct turns normally differ in assistant message or tool result counts, so
// repeating a turn with a unique Match reuses that turn idempotently. When a
// Script declares multiple turns with identical Match predicates to model
// retry-then-succeed sequences on one server, selection advances to the first
// unused twin and remains on the final twin once every twin has been hit.
func (h *Handler) selectScriptTurn(summary RequestSummary, rawBody []byte) (int, testkit.Turn, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	firstIdx := -1
	for i, turn := range h.cfg.Script.Turns {
		if turn.Match.MatchesObserved(summary.AssistantMessages, summary.ToolResults, summary.Model, summary.HasTools, rawBody) {
			firstIdx = i
			break
		}
	}
	if firstIdx < 0 {
		return -1, testkit.Turn{}, false
	}

	chosenIdx := firstIdx
	firstMatch := h.cfg.Script.Turns[firstIdx].Match
	if h.turnHits[firstIdx] > 0 {
		for j := firstIdx + 1; j < len(h.cfg.Script.Turns); j++ {
			if !equalMatch(h.cfg.Script.Turns[j].Match, firstMatch) {
				continue
			}
			chosenIdx = j
			if h.turnHits[j] == 0 {
				break
			}
		}
	}
	h.turnHits[chosenIdx]++
	return chosenIdx, h.cfg.Script.Turns[chosenIdx], true
}

// deliverReply executes optional Fault delays, pre-stream HTTP error statuses,
// Reply.Raw lookup, IR reply synthesis, and frame-level SSE truncation for one
// matched or default turn. Intentional Fault injections leave Violations
// unchanged while missing Raw fixtures, unregistered synthesis codecs, or
// invalid IR reply payloads record check errors.
//
// Request cancellation during DelayMs records the aborted attempt with zero
// ResponseStatus and returns before writing headers. On completed or truncated
// replies, deliverReply records rec under Handler's mutex before writing bytes
// or closing hijacked sockets so client readers observe updated logs immediately.
func (h *Handler) deliverReply(w http.ResponseWriter, r *http.Request, endpoint Endpoint, summary RequestSummary, reply testkit.Reply, fault *testkit.Fault, rec RecordedRequest) {
	if fault != nil && fault.DelayMs > 0 {
		if !waitFaultDelay(r.Context(), fault.DelayMs) {
			// Record canceled exchanges before returning so tests inspecting
			// Handler.Requests see the aborted attempt alongside completed turns.
			h.recordExchange(rec)
			return
		}
	}

	if fault != nil && fault.Status > 0 && fault.Status != http.StatusOK {
		var respHeaders http.Header
		var respBody []byte
		if fault.ErrorBody != "" {
			respHeaders = make(http.Header)
			respBody = []byte(fault.ErrorBody)
			trimmed := strings.TrimSpace(fault.ErrorBody)
			if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
				respHeaders.Set("Content-Type", "application/json")
			} else {
				respHeaders.Set("Content-Type", "text/plain; charset=utf-8")
			}
		} else {
			respHeaders, respBody = prepareProviderError(
				endpoint.Protocol,
				fault.Status,
				"",
				fmt.Sprintf("injected upstream fault status %d", fault.Status),
			)
		}
		rec.ResponseStatus = fault.Status
		rec.ResponseHeaders = respHeaders.Clone()
		rec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(rec)
		writePreparedResponse(w, fault.Status, respHeaders, respBody)
		return
	}

	resolved, err := h.resolveReply(endpoint, rec.Decoded, summary, reply)
	if err != nil {
		rec.CheckErrors = append(rec.CheckErrors, err.Error())
		respHeaders, respBody := prepareProviderError(endpoint.Protocol, http.StatusNotImplemented, "", err.Error())
		rec.ResponseStatus = http.StatusNotImplemented
		rec.ResponseHeaders = respHeaders.Clone()
		rec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(rec)
		writePreparedResponse(w, http.StatusNotImplemented, respHeaders, respBody)
		return
	}

	if fault != nil && fault.TruncateAfterFrames != nil {
		cutData := truncateRawSSEFrames(resolved.data, *fault.TruncateAfterFrames)
		respHeaders := make(http.Header)
		respHeaders.Set("Content-Type", "text/event-stream")
		respHeaders.Set("Cache-Control", "no-cache")
		rec.ResponseStatus = http.StatusOK
		rec.ResponseHeaders = respHeaders.Clone()
		rec.ResponseBody = bytes.Clone(cutData)
		// Persist the request log before flushing and closing the hijacked socket
		// so clients unblocking on ErrUnexpectedEOF see the completed record immediately.
		h.recordExchange(rec)
		writeTruncatedSSEResponse(w, respHeaders, cutData)
		return
	}

	respHeaders := make(http.Header)
	respHeaders.Set("Content-Type", resolved.contentType)
	if resolved.contentType == "text/event-stream" {
		respHeaders.Set("Cache-Control", "no-cache")
	}
	rec.ResponseStatus = http.StatusOK
	rec.ResponseHeaders = respHeaders.Clone()
	rec.ResponseBody = bytes.Clone(resolved.data)
	if capErr := h.maybeWriteCaptureBundle(rec); capErr != nil {
		rec.CheckErrors = append(rec.CheckErrors, fmt.Sprintf("write capture bundle: %v", capErr))
	}
	// Record the completed exchange before writing response bytes so client
	// goroutines reading the body observe synchronized Handler.Requests state.
	h.recordExchange(rec)
	writePreparedResponse(w, http.StatusOK, respHeaders, resolved.data)
}

// resolveReply resolves wire bytes and Content-Type for reply, preferring
// verbatim Reply.Raw fixtures when present and otherwise synthesizing SSE
// streams or JSON bodies from Reply.Events or Reply.Response through convert's
// registered protocol encoders.
func (h *Handler) resolveReply(
	endpoint Endpoint,
	decodedReq *ir.Request,
	summary RequestSummary,
	reply testkit.Reply,
) (resolvedReply, error) {
	if reply.Raw != "" {
		return h.resolveRawReply(reply, summary.Stream)
	}
	if len(reply.Events) == 0 && reply.Response == nil {
		return resolvedReply{}, errors.New("mockupstream Reply has empty Raw, Events, and Response payloads")
	}

	proto, ok := testkit.ParseProtocolDirName(endpoint.Protocol)
	if !ok || !convert.HasClientCodec(proto) {
		return resolvedReply{}, fmt.Errorf(
			"mockupstream protocol %q has no registered response encoder for Reply.Events or Reply.Response",
			endpoint.Protocol,
		)
	}

	view := convert.ClientView{
		Model:   summary.Model,
		Request: decodedReq.Clone(),
		Runtime: h.newSyntheticRuntime(),
	}

	if len(reply.Events) > 0 {
		if err := ir.ValidateStream(reply.Events); err != nil {
			return resolvedReply{}, fmt.Errorf("invalid Reply.Events stream: %w", err)
		}
		if summary.Stream {
			rawSSE, err := encodeEventsToSSE(proto, view, reply.Events)
			if err != nil {
				return resolvedReply{}, err
			}
			return resolvedReply{
				data:        rawSSE,
				contentType: "text/event-stream",
			}, nil
		}
		if reply.Response != nil {
			if err := ir.ValidateResponse(reply.Response); err != nil {
				return resolvedReply{}, fmt.Errorf("invalid Reply.Response: %w", err)
			}
			rawJSON, _, err := convert.EncodeResponse(proto, reply.Response, view)
			if err != nil {
				return resolvedReply{}, err
			}
			return resolvedReply{
				data:        rawJSON,
				contentType: "application/json",
			}, nil
		}
		var acc ir.Accumulator
		acc.Add(reply.Events...)
		if streamErr := acc.StreamError(); streamErr != nil {
			return resolvedReply{}, fmt.Errorf("cannot synthesize buffered JSON from error stream: %s", streamErr.Message)
		}
		rawJSON, _, err := convert.EncodeResponse(proto, acc.Response(), view)
		if err != nil {
			return resolvedReply{}, err
		}
		return resolvedReply{
			data:        rawJSON,
			contentType: "application/json",
		}, nil
	}

	if err := ir.ValidateResponse(reply.Response); err != nil {
		return resolvedReply{}, fmt.Errorf("invalid Reply.Response: %w", err)
	}
	if !summary.Stream {
		rawJSON, _, err := convert.EncodeResponse(proto, reply.Response, view)
		if err != nil {
			return resolvedReply{}, err
		}
		return resolvedReply{
			data:        rawJSON,
			contentType: "application/json",
		}, nil
	}
	synthEvents := synthesizeEventsFromResponse(reply.Response)
	rawSSE, err := encodeEventsToSSE(proto, view, synthEvents)
	if err != nil {
		return resolvedReply{}, err
	}
	return resolvedReply{
		data:        rawSSE,
		contentType: "text/event-stream",
	}, nil
}

// newSyntheticRuntime builds a deterministic Runtime for one synthesized reply.
func (h *Handler) newSyntheticRuntime() convert.Runtime {
	h.mu.Lock()
	h.synthSeq++
	base := h.synthSeq
	h.mu.Unlock()

	sub := 0
	return convert.Runtime{
		Now: func() time.Time { return time.Unix(defaultSyntheticUnix, 0).UTC() },
		NewID: func(prefix string) string {
			sub++
			if prefix == "" {
				return fmt.Sprintf("mock_%d_%d", base, sub)
			}
			if strings.HasSuffix(prefix, "_") || strings.HasSuffix(prefix, "-") {
				return fmt.Sprintf("%s%d_%d", prefix, base, sub)
			}
			return fmt.Sprintf("%s_%d_%d", prefix, base, sub)
		},
	}
}

// encodeEventsToSSE encodes events through proto's StreamEncoder and formats wire SSE bytes.
func encodeEventsToSSE(proto protocol.ProtocolID, view convert.ClientView, events []ir.Event) ([]byte, error) {
	enc, err := convert.NewStreamEncoder(proto, view)
	if err != nil {
		return nil, err
	}
	var out []byte
	for _, ev := range events {
		frames, _, encErr := enc.Encode(ev)
		if encErr != nil {
			return nil, encErr
		}
		for _, f := range frames {
			out = sse.Append(out, f)
		}
	}
	return out, nil
}

// synthesizeEventsFromResponse converts a buffered ir.Response into a canonical
// sequential ir.Event slice suitable for StreamEncoder synthesis.
func synthesizeEventsFromResponse(resp *ir.Response) []ir.Event {
	events := []ir.Event{{
		Kind: ir.EventResponseStart,
		Response: &ir.ResponseMeta{
			ID:          resp.ID,
			Model:       resp.Model,
			CreatedAt:   resp.CreatedAt,
			ServiceTier: resp.ServiceTier,
		},
	}}
	for i, part := range resp.Parts {
		skel := partSkeletonForStream(part)
		events = append(events, ir.Event{
			Kind:  ir.EventPartStart,
			Index: i,
			Part:  &skel,
		})
		switch part.Kind {
		case ir.PartText, ir.PartRefusal:
			if part.Text != "" {
				events = append(events, ir.Event{
					Kind:  ir.EventPartDelta,
					Index: i,
					Delta: &ir.Delta{Text: part.Text},
				})
			}
		case ir.PartReasoning:
			if part.Reasoning != nil && !part.Reasoning.Redacted {
				for _, seg := range part.Reasoning.Segments {
					if seg != "" {
						events = append(events, ir.Event{
							Kind:  ir.EventPartDelta,
							Index: i,
							Delta: &ir.Delta{Text: seg},
						})
					}
				}
			}
		case ir.PartToolCall:
			if part.Call != nil && part.Call.Arguments != "" && part.Call.Tool.Kind != ir.ToolClientSearch {
				events = append(events, ir.Event{
					Kind:  ir.EventPartDelta,
					Index: i,
					Delta: &ir.Delta{Arguments: part.Call.Arguments},
				})
			}
		}
		endCopy := part.Clone()
		events = append(events, ir.Event{
			Kind:  ir.EventPartEnd,
			Index: i,
			Part:  &endCopy,
		})
	}
	if resp.Usage != nil {
		events = append(events, ir.Event{
			Kind:  ir.EventUsage,
			Usage: resp.Usage.Clone(),
		})
	}
	fin := resp.Finish
	if fin.Reason == "" {
		fin.Reason = ir.FinishStop
	}
	events = append(events, ir.Event{
		Kind:        ir.EventResponseEnd,
		Finish:      &fin,
		ServiceTier: resp.ServiceTier,
	})
	return events
}

// partSkeletonForStream builds an EventPartStart skeleton from a completed ir.Part.
func partSkeletonForStream(part ir.Part) ir.Part {
	skel := part.Clone()
	switch skel.Kind {
	case ir.PartText, ir.PartRefusal:
		skel.Text = ""
	case ir.PartReasoning:
		if skel.Reasoning != nil {
			skel.Reasoning.Segments = nil
			if !skel.Reasoning.Redacted {
				skel.Reasoning.Token = nil
			}
		}
	case ir.PartToolCall:
		if skel.Call != nil {
			skel.Call.Arguments = ""
			skel.Call.Token = nil
		}
	}
	return skel
}

// resolveRawReply loads verbatim wire bytes from inline Reply.Raw text or from
// Handler's Corpus.
func (h *Handler) resolveRawReply(reply testkit.Reply, requestStream bool) (resolvedReply, error) {
	if reply.Raw == "" {
		return resolvedReply{}, errors.New("mockupstream Reply has empty Raw payload")
	}

	if isInlineRawPayload(reply.Raw) {
		data := []byte(reply.Raw)
		return resolvedReply{
			data:        data,
			contentType: inferRawContentType("", data, requestStream),
		}, nil
	}

	data, err := h.cfg.Corpus.ReadFile(reply.Raw)
	if err != nil {
		return resolvedReply{}, fmt.Errorf("load Reply.Raw %q from corpus: %w", reply.Raw, err)
	}
	return resolvedReply{
		data:        data,
		contentType: inferRawContentType(reply.Raw, data, requestStream),
	}, nil
}

// recordExchange appends a deep copy of rec to Handler's request log under h.mu
// and assigns its zero-based arrival Index. Whenever rec carries RuleViolations
// or CheckErrors, recordExchange also appends a matching ViolationEntry.
func (h *Handler) recordExchange(rec RecordedRequest) {
	h.mu.Lock()
	defer h.mu.Unlock()

	idx := len(h.requests)
	rec.Index = idx
	h.requests = append(h.requests, cloneRecordedRequest(rec))
	if len(rec.RuleViolations) > 0 || len(rec.CheckErrors) > 0 {
		h.violations = append(h.violations, ViolationEntry{
			RequestIndex:   idx,
			Method:         rec.Method,
			Path:           rec.Path,
			Protocol:       rec.Protocol,
			RuleViolations: slices.Clone(rec.RuleViolations),
			CheckErrors:    slices.Clone(rec.CheckErrors),
		})
	}
}

// evaluateChecks runs each configured testkit.Check against a cloned decoded
// ir.Request (when available) and raw JSON bytes, returning formatted
// descriptions for any failed predicates.
func evaluateChecks(checks []testkit.Check, decoded *ir.Request, body []byte) []string {
	var failures []string
	for i, chk := range checks {
		if err := chk.Evaluate(decoded.Clone(), body); err != nil {
			label := chk.Description
			if label == "" {
				label = fmt.Sprintf("expect[%d]", i)
			}
			failures = append(failures, fmt.Sprintf("%s: %v", label, err))
		}
	}
	return failures
}

// cloneRecordedRequest returns an independent deep copy of rec, cloning request
// and response header maps alongside body, decoded IR, violation, and error slices.
// Nil header maps and nil Decoded pointers remain nil on the returned copy.
func cloneRecordedRequest(rec RecordedRequest) RecordedRequest {
	out := rec
	if rec.Headers != nil {
		out.Headers = rec.Headers.Clone()
	}
	out.Body = bytes.Clone(rec.Body)
	out.Decoded = rec.Decoded.Clone()
	out.RuleViolations = slices.Clone(rec.RuleViolations)
	out.CheckErrors = slices.Clone(rec.CheckErrors)
	if rec.ResponseHeaders != nil {
		out.ResponseHeaders = rec.ResponseHeaders.Clone()
	}
	out.ResponseBody = bytes.Clone(rec.ResponseBody)
	return out
}

// cloneViolationEntry returns an independent deep copy of v with cloned
// RuleViolations and CheckErrors slices. Callers can freely modify the returned
// slices without mutating Handler's internal violation history.
func cloneViolationEntry(v ViolationEntry) ViolationEntry {
	out := v
	out.RuleViolations = slices.Clone(v.RuleViolations)
	out.CheckErrors = slices.Clone(v.CheckErrors)
	return out
}

// equalMatch reports whether two testkit.Match values specify identical turn predicates.
func equalMatch(a, b testkit.Match) bool {
	return equalOptionalInt(a.AssistantMessages, b.AssistantMessages) &&
		equalOptionalInt(a.ToolResults, b.ToolResults) &&
		a.ModelPrefix == b.ModelPrefix &&
		a.ContainsText == b.ContainsText &&
		equalOptionalBool(a.HasTools, b.HasTools)
}

// equalOptionalInt reports whether two optional int pointers are both nil or point to equal values.
func equalOptionalInt(a, b *int) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// equalOptionalBool reports whether two optional bool pointers are both nil or point to equal values.
func equalOptionalBool(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// hasConfiguredReply reports whether reply defines any Raw or IR reply source.
func hasConfiguredReply(reply testkit.Reply) bool {
	return reply.Raw != "" || len(reply.Events) > 0 || reply.Response != nil
}

// isInlineRawPayload reports whether raw holds literal JSON or SSE content
// rather than a relative file path inside testkit/testdata. Multiline text,
// leading JSON delimiters, and SSE field prefixes classify as inline payloads.
func isInlineRawPayload(raw string) bool {
	if strings.ContainsAny(raw, "\r\n") {
		return true
	}
	trimmed := strings.TrimSpace(strings.TrimPrefix(raw, string(utf8BOM)))
	if trimmed == "" {
		return false
	}
	if trimmed[0] == '{' || trimmed[0] == '[' {
		return true
	}
	return strings.HasPrefix(trimmed, "data:") ||
		strings.HasPrefix(trimmed, "event:") ||
		strings.HasPrefix(trimmed, ":")
}

// inferRawContentType selects text/event-stream or application/json from a
// corpus path extension, payload prefix, or request stream mode. Explicit .sse
// and .json extensions take precedence over payload byte inspection.
func inferRawContentType(rawPath string, data []byte, requestStream bool) string {
	lowerPath := strings.ToLower(strings.TrimSpace(rawPath))
	switch {
	case strings.HasSuffix(lowerPath, ".sse"):
		return "text/event-stream"
	case strings.HasSuffix(lowerPath, ".json"):
		return "application/json"
	}
	trimmed := bytes.TrimSpace(bytes.TrimPrefix(data, utf8BOM))
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[') {
		return "application/json"
	}
	if bytes.HasPrefix(trimmed, []byte("data:")) ||
		bytes.HasPrefix(trimmed, []byte("event:")) ||
		bytes.HasPrefix(trimmed, []byte(":")) {
		return "text/event-stream"
	}
	if requestStream {
		return "text/event-stream"
	}
	return "application/json"
}

// waitFaultDelay pauses execution for delayMs milliseconds and returns true
// once the timer expires. When ctx is canceled before expiration,
// waitFaultDelay stops the timer and returns false immediately.
func waitFaultDelay(ctx context.Context, delayMs int) bool {
	timer := time.NewTimer(time.Duration(delayMs) * time.Millisecond)
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		// Stop the pending timer on request cancellation so aborted fault-delay
		// turns release timer resources without waiting for delayMs to expire.
		timer.Stop()
		return false
	}
}

// truncateRawSSEFrames returns a cloned verbatim byte prefix of raw ending
// immediately after maxFrames dispatched SSE frames. Nonpositive maxFrames or
// empty raw slices return nil without scanning.
//
// Line scanning mirrors sse.Reader's frame boundary rules: an optional UTF-8
// BOM is stripped on the first physical line, comments and empty single data
// lines do not dispatch a frame, and original CRLF/LF delimiters stay intact
// so quirk fixtures replay byte-for-byte up to the cut point.
func truncateRawSSEFrames(raw []byte, maxFrames int) []byte {
	if maxFrames <= 0 || len(raw) == 0 {
		return nil
	}
	emitted := 0
	hasData := false
	dataLen := 0
	started := false
	offset := 0

	for offset < len(raw) {
		nlRel := bytes.IndexByte(raw[offset:], '\n')
		var rawLine []byte
		var nextOffset int
		terminated := false
		if nlRel >= 0 {
			rawLine = raw[offset : offset+nlRel]
			nextOffset = offset + nlRel + 1
			terminated = true
		} else {
			rawLine = raw[offset:]
			nextOffset = len(raw)
		}

		line := rawLine
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		if !started {
			started = true
			line = bytes.TrimPrefix(line, utf8BOM)
		}

		if len(line) == 0 {
			if terminated && dataLen > 0 {
				emitted++
				if emitted >= maxFrames {
					return bytes.Clone(raw[:nextOffset])
				}
			}
			hasData = false
			dataLen = 0
			offset = nextOffset
			continue
		}

		if line[0] != ':' {
			if field, content, hasColon := bytes.Cut(line, []byte(":")); hasColon && string(field) == "data" {
				content = bytes.TrimPrefix(content, []byte(" "))
				if hasData {
					dataLen++
				}
				dataLen += len(content)
				hasData = true
			}
		}
		offset = nextOffset
	}
	return bytes.Clone(raw)
}

// writeTruncatedSSEResponse writes HTTP 200 SSE headers and cutData, flushes the
// chunked stream, and closes the underlying TCP connection before sending a
// terminating zero-length chunk when w supports http.Hijacker. In-memory
// recorders that do not implement Hijacker retain cutData without panicking.
func writeTruncatedSSEResponse(w http.ResponseWriter, headers http.Header, cutData []byte) {
	for k, vals := range headers {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(http.StatusOK)
	if len(cutData) > 0 {
		_, _ = w.Write(cutData)
	}
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	// Hijack and close the raw TCP socket after flushing chunked SSE bytes so
	// real HTTP clients observe io.ErrUnexpectedEOF mid-stream, while in-memory
	// httptest.ResponseRecorder callers without Hijacker return without panicking.
	if hj, ok := w.(http.Hijacker); ok {
		if conn, rw, err := hj.Hijack(); err == nil {
			_ = rw.Flush()
			_ = conn.Close()
		}
	}
}

// writePreparedResponse copies headers onto w, writes status and body, and
// flushes text/event-stream responses when w implements http.Flusher. Empty
// body slices write only the status line and headers.
func writePreparedResponse(w http.ResponseWriter, status int, headers http.Header, body []byte) {
	for k, vals := range headers {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(status)
	if len(body) > 0 {
		_, _ = w.Write(body)
	}
	if flusher, ok := w.(http.Flusher); ok && headers.Get("Content-Type") == "text/event-stream" {
		flusher.Flush()
	}
}

// prepareProviderError constructs an application/json header map and a
// provider-native JSON error payload matching protocolID and HTTP status.
// Callers receive independent header and body values ready to record and send.
func prepareProviderError(protocolID string, status int, param, message string) (http.Header, []byte) {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	return headers, formatProviderErrorBody(protocolID, status, param, message)
}

// formatProviderErrorBody serializes status and message using the error JSON
// envelope native to Anthropic, Gemini, or OpenAI endpoints. Unknown protocol
// identifiers fall back to the OpenAI error envelope shape.
func formatProviderErrorBody(protocolID string, status int, param, message string) []byte {
	switch protocolID {
	case protocolAnthropicMessages:
		errType := "invalid_request_error"
		switch {
		case status == http.StatusUnauthorized:
			errType = "authentication_error"
		case status == http.StatusForbidden:
			errType = "permission_error"
		case status == http.StatusNotFound:
			errType = "not_found_error"
		case status == http.StatusTooManyRequests:
			errType = "rate_limit_error"
		case status == 529:
			errType = "overloaded_error"
		case status >= http.StatusInternalServerError:
			errType = "api_error"
		}
		raw, err := json.Marshal(anthropicErrorEnvelope{
			Type:  "error",
			Error: anthropicErrorDetail{Type: errType, Message: message},
		})
		if err == nil {
			return raw
		}

	case protocolGeminiGenerateContent:
		grpcStatus := "INVALID_ARGUMENT"
		switch {
		case status == http.StatusUnauthorized:
			grpcStatus = "UNAUTHENTICATED"
		case status == http.StatusForbidden:
			grpcStatus = "PERMISSION_DENIED"
		case status == http.StatusNotFound:
			grpcStatus = "NOT_FOUND"
		case status == http.StatusTooManyRequests:
			grpcStatus = "RESOURCE_EXHAUSTED"
		case status == http.StatusServiceUnavailable || status == 529:
			grpcStatus = "UNAVAILABLE"
		case status >= http.StatusInternalServerError:
			grpcStatus = "INTERNAL"
		}
		raw, err := json.Marshal(geminiErrorEnvelope{
			Error: geminiErrorDetail{Code: status, Message: message, Status: grpcStatus},
		})
		if err == nil {
			return raw
		}

	default:
		errType := "invalid_request_error"
		code := "invalid_request"
		switch {
		case status == http.StatusUnauthorized:
			errType = "authentication_error"
			code = "invalid_api_key"
		case status == http.StatusNotFound:
			errType = "invalid_request_error"
			code = "not_found"
		case status == http.StatusTooManyRequests:
			errType = "rate_limit_error"
			code = "rate_limit_exceeded"
		case status >= http.StatusInternalServerError:
			errType = "server_error"
			code = "server_error"
		}
		raw, err := json.Marshal(openAIErrorEnvelope{
			Error: openAIErrorDetail{
				Message: message,
				Type:    errType,
				Param:   param,
				Code:    code,
			},
		})
		if err == nil {
			return raw
		}
	}
	// Return a static JSON error envelope if struct marshaling fails so HTTP
	// error responses always carry valid JSON for downstream error parsers.
	return []byte(`{"error":{"type":"invalid_request_error","message":"mockupstream request error"}}`)
}
