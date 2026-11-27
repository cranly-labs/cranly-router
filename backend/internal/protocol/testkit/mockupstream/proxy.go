package mockupstream

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/textproto"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// proxy.go implements sequential fixture replay (ModeReplay), live upstream
// reverse-proxying (ModeProxyRecord), and raw capture bundle persistence into
// _incoming/ directories for client-laboratory corpus collection (section 5.7).
// Live canary runs need incremental SSE delivery to the client subprocess while
// still retaining exact request and stream bytes for structural assertions and
// later sanitization through cmd/corpus import.
//
// ModeProxyRecord strips RFC 9110 hop-by-hop headers and incoming
// Accept-Encoding so outbound http.Transport transparently decompresses gzip
// SSE streams into UTF-8 plaintext, deduplicates trailing /v1 and /v1beta base
// path segments when joining TargetURL, and flushes each upstream chunk to the
// downstream ResponseWriter while teeing bytes into an in-memory buffer.
// Recording and capture file writes finish before ServeHTTP returns so chunked
// transfer termination happens after Handler logs settle.

// hopByHopHeaderNames lists canonical HTTP/1.1 hop-by-hop header keys omitted
// when forwarding requests and responses in ModeProxyRecord.
var hopByHopHeaderNames = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Trailers",
	"Transfer-Encoding",
	"Upgrade",
}

// serveReplay responds to an inspected request using the next sequential entry
// in Config.Replay or Config.Script.Turns, falling back to Script.Default once
// all ordered entries have been consumed. Exhausted sequences without a
// configured Default reply record a violation and return HTTP 400.
func (h *Handler) serveReplay(w http.ResponseWriter, r *http.Request, endpoint Endpoint, body []byte, summary RequestSummary, rec RecordedRequest) {
	idx, expect, reply, fault, usedDefault, ok := h.nextReplayStep()
	if !ok {
		msg := fmt.Sprintf("replay sequence exhausted on %s %s and Script.Default is empty", r.Method, r.URL.Path)
		rec.CheckErrors = []string{msg}
		respHeaders, respBody := prepareProviderError(endpoint.Protocol, http.StatusBadRequest, "$", msg)
		rec.ResponseStatus = http.StatusBadRequest
		rec.ResponseHeaders = respHeaders.Clone()
		rec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(rec)
		writePreparedResponse(w, http.StatusBadRequest, respHeaders, respBody)
		return
	}

	rec.MatchedTurn = idx
	rec.UsedDefault = usedDefault
	if checkErrs := evaluateChecks(expect, rec.Decoded, body); len(checkErrs) > 0 {
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

	h.deliverReply(w, r, endpoint, summary, reply, fault, rec)
}

// nextReplayStep advances the sequential replay cursor under h.mu and returns
// the selected index, optional Turn checks, Reply, optional Fault, whether
// Script.Default was used, and whether a reply was available. Explicit
// Config.Replay entries take precedence over Config.Script.Turns.
func (h *Handler) nextReplayStep() (int, []testkit.Check, testkit.Reply, *testkit.Fault, bool, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if len(h.cfg.Replay) > 0 {
		if h.replayCursor < len(h.cfg.Replay) {
			idx := h.replayCursor
			h.replayCursor++
			return idx, nil, h.cfg.Replay[idx], nil, false, true
		}
	} else if h.replayCursor < len(h.cfg.Script.Turns) {
		idx := h.replayCursor
		h.replayCursor++
		turn := h.cfg.Script.Turns[idx]
		return idx, turn.Expect, turn.Reply, turn.Fault, false, true
	}

	if hasConfiguredReply(h.cfg.Script.Default) {
		return -1, nil, h.cfg.Script.Default, nil, true, true
	}
	return -1, nil, testkit.Reply{}, nil, false, false
}

// serveProxyRecord forwards r to Config.TargetURL without local request
// validation so live upstream providers evaluate converted payloads directly.
// Invalid TargetURL configuration or outbound dial failures return HTTP 502
// JSON responses and record the failed exchange.
//
// Outbound requests strip hop-by-hop headers and incoming Accept-Encoding so
// Go's http.Transport negotiates and decompresses gzip streams into plaintext
// UTF-8 before incremental chunks flush to w and buffer in memory. Capture
// bundle files and request logs commit before ServeHTTP returns so chunked
// stream completion orders after Handler state updates.
func (h *Handler) serveProxyRecord(w http.ResponseWriter, r *http.Request, endpoint Endpoint, hasEndpoint bool, body []byte, summary RequestSummary, rec RecordedRequest) {
	rec.Summary = summary
	errProtocol := endpoint.Protocol
	if errProtocol == "" {
		errProtocol = protocolOpenAIResponses
	}

	targetURL, err := buildProxyTargetURL(h.cfg.TargetURL, r.URL)
	if err != nil {
		msg := fmt.Sprintf("invalid proxy-record TargetURL: %v", err)
		rec.CheckErrors = []string{msg}
		respHeaders, respBody := prepareProviderError(errProtocol, http.StatusBadGateway, "", msg)
		rec.ResponseStatus = http.StatusBadGateway
		rec.ResponseHeaders = respHeaders.Clone()
		rec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(rec)
		writePreparedResponse(w, http.StatusBadGateway, respHeaders, respBody)
		return
	}

	outReq, err := http.NewRequestWithContext(r.Context(), r.Method, targetURL, bytes.NewReader(body))
	if err != nil {
		msg := fmt.Sprintf("build proxy-record request: %v", err)
		rec.CheckErrors = []string{msg}
		respHeaders, respBody := prepareProviderError(errProtocol, http.StatusBadGateway, "", msg)
		rec.ResponseStatus = http.StatusBadGateway
		rec.ResponseHeaders = respHeaders.Clone()
		rec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(rec)
		writePreparedResponse(w, http.StatusBadGateway, respHeaders, respBody)
		return
	}
	copyForwardHeaders(outReq.Header, r.Header, false)
	// Remove incoming Accept-Encoding so outbound http.Transport negotiates gzip
	// itself and transparently decompresses resp.Body into UTF-8 plaintext for capture.
	outReq.Header.Del("Accept-Encoding")
	outReq.ContentLength = int64(len(body))

	client := h.cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(outReq)
	if err != nil {
		respHeaders, respBody := prepareProviderError(errProtocol, http.StatusBadGateway, "", fmt.Sprintf("proxy-record upstream request failed: %v", err))
		rec.ResponseStatus = http.StatusBadGateway
		rec.ResponseHeaders = respHeaders.Clone()
		rec.ResponseBody = bytes.Clone(respBody)
		h.recordExchange(rec)
		writePreparedResponse(w, http.StatusBadGateway, respHeaders, respBody)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	respHeaders := make(http.Header)
	// Omit upstream Content-Length when relaying through incremental chunk
	// flushes so chunked framing governs stream completion cleanly.
	copyForwardHeaders(respHeaders, resp.Header, true)
	for k, vals := range respHeaders {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)

	flusher, canFlush := w.(http.Flusher)
	if canFlush {
		flusher.Flush()
	}

	var captured bytes.Buffer
	buf := make([]byte, 4096)
	for {
		n, readErr := resp.Body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			_, _ = captured.Write(chunk)
			_, _ = w.Write(chunk)
			if canFlush {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if !errors.Is(readErr, io.EOF) && r.Context().Err() == nil {
				rec.CheckErrors = append(rec.CheckErrors, fmt.Sprintf("read upstream proxy stream: %v", readErr))
			}
			break
		}
	}

	rec.ResponseStatus = resp.StatusCode
	rec.ResponseHeaders = respHeaders.Clone()
	rec.ResponseBody = bytes.Clone(captured.Bytes())
	if hasEndpoint && resp.StatusCode == http.StatusOK {
		if capErr := h.maybeWriteCaptureBundle(rec); capErr != nil {
			rec.CheckErrors = append(rec.CheckErrors, fmt.Sprintf("write capture bundle: %v", capErr))
		}
	}
	h.recordExchange(rec)
}

// maybeWriteCaptureBundle persists a *.capture.json IncomingCapture file into
// Capture.Dir when disk capture is enabled and rec completed with HTTP 200.
// Leaving both RecordRequests and RecordStreams false captures both JSON
// request payloads and SSE response streams by default.
func (h *Handler) maybeWriteCaptureBundle(rec RecordedRequest) error {
	dir := strings.TrimSpace(h.cfg.Capture.Dir)
	if dir == "" || rec.Protocol == "" || rec.ResponseStatus != http.StatusOK {
		return nil
	}
	proto, ok := testkit.ParseProtocolDirName(rec.Protocol)
	if !ok {
		return nil
	}

	wantReq := h.cfg.Capture.RecordRequests
	wantStream := h.cfg.Capture.RecordStreams
	if !wantReq && !wantStream {
		wantReq = true
		wantStream = true
	}

	isSSE := strings.Contains(strings.ToLower(rec.ResponseHeaders.Get("Content-Type")), "text/event-stream")
	meta := h.cfg.Capture.Meta
	if meta.Protocol == "" {
		meta.Protocol = proto
	}
	if meta.Model == "" && rec.Summary.Model != "" {
		meta.Model = rec.Summary.Model
	}
	if isSSE {
		meta.Stream = true
	}

	var reqValue jsontext.Value
	if wantReq && len(rec.Body) > 0 {
		reqValue = jsontext.Value(bytes.Clone(rec.Body))
	}
	upstreamSSE := ""
	if wantStream && isSSE && len(rec.ResponseBody) > 0 {
		upstreamSSE = string(rec.ResponseBody)
	}
	if len(reqValue) == 0 && upstreamSSE == "" {
		return nil
	}

	h.mu.Lock()
	h.captureSeq++
	seq := h.captureSeq
	h.mu.Unlock()

	bundle := testkit.IncomingCapture{
		Meta:        meta,
		RawHeaders:  flattenCaptureHeaders(rec.Headers),
		Turn:        seq,
		RequestBody: reqValue,
		UpstreamSSE: upstreamSSE,
	}
	encoded, err := testkit.MarshalDeterministicIndent(bundle)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	prefix := h.cfg.Capture.FilePrefix
	if prefix == "" {
		prefix = meta.Scenario
	}
	fileName := fmt.Sprintf("%s-%03d.capture.json", sanitizeCapturePrefix(prefix), seq)
	return os.WriteFile(filepath.Join(dir, fileName), encoded, 0o600)
}

// sanitizeCapturePrefix converts prefix into a safe capture filename stem by
// replacing characters outside ASCII letters, digits, hyphens, and underscores
// with underscores. Empty inputs or strings containing only separators fall
// back to "capture".
func sanitizeCapturePrefix(prefix string) string {
	trimmed := strings.TrimSpace(prefix)
	if trimmed == "" {
		return "capture"
	}
	var b strings.Builder
	for i := range len(trimmed) {
		c := trimmed[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' {
			_ = b.WriteByte(c)
		} else {
			_ = b.WriteByte('_')
		}
	}
	clean := strings.Trim(b.String(), "_")
	if clean == "" {
		return "capture"
	}
	return clean
}

// buildProxyTargetURL joins targetBase with reqURL's path and query while
// deduplicating trailing /v1 or /v1beta version segments even when targetBase
// carries leading path prefixes such as /api/v1 or /proxy/v1beta. Empty
// targetBase values or URLs missing scheme or host return an error.
func buildProxyTargetURL(targetBase string, reqURL *url.URL) (string, error) {
	trimmed := strings.TrimSpace(targetBase)
	if trimmed == "" {
		return "", errors.New("empty TargetURL")
	}
	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("TargetURL %q must include scheme and host", targetBase)
	}

	basePath := strings.TrimRight(parsed.Path, "/")
	if basePath != "" && !strings.HasPrefix(basePath, "/") {
		basePath = "/" + basePath
	}
	reqPath := reqURL.Path
	if reqPath != "" && !strings.HasPrefix(reqPath, "/") {
		reqPath = "/" + reqPath
	}

	switch {
	case basePath == "":
		parsed.Path = reqPath
	case reqPath == "":
		parsed.Path = basePath
	case reqPath == basePath || strings.HasPrefix(reqPath, basePath+"/"):
		parsed.Path = reqPath
	default:
		joined := basePath + reqPath
		for _, ver := range []string{"/v1beta", "/v1"} {
			if strings.HasSuffix(basePath, ver) && (reqPath == ver || strings.HasPrefix(reqPath, ver+"/")) {
				joined = basePath + strings.TrimPrefix(reqPath, ver)
				break
			}
		}
		parsed.Path = joined
	}
	parsed.RawPath = ""

	switch {
	case parsed.RawQuery != "" && reqURL.RawQuery != "":
		parsed.RawQuery = parsed.RawQuery + "&" + reqURL.RawQuery
	case reqURL.RawQuery != "":
		parsed.RawQuery = reqURL.RawQuery
	}
	return parsed.String(), nil
}

// copyForwardHeaders copies HTTP header values from src to dst while excluding
// standard hop-by-hop headers and any header keys listed inside Connection.
// Setting omitContentLength also skips Content-Length for streamed responses.
func copyForwardHeaders(dst, src http.Header, omitContentLength bool) {
	var connNominated []string
	for _, raw := range src.Values("Connection") {
		for token := range strings.SplitSeq(raw, ",") {
			canonical := textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(token))
			if canonical != "" {
				connNominated = append(connNominated, canonical)
			}
		}
	}
	for k, vals := range src {
		canonical := textproto.CanonicalMIMEHeaderKey(k)
		if omitContentLength && canonical == "Content-Length" {
			continue
		}
		if slices.Contains(hopByHopHeaderNames, canonical) || slices.Contains(connNominated, canonical) {
			continue
		}
		for _, v := range vals {
			dst.Add(canonical, v)
		}
	}
}

// flattenCaptureHeaders converts an http.Header map into single-string header
// entries joined with ", " for IncomingCapture.RawHeaders JSON serialization.
// Nil or empty header maps return nil so omitzero omits raw_headers from disk.
func flattenCaptureHeaders(h http.Header) map[string]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string]string, len(h))
	for k, vals := range h {
		if len(vals) > 0 {
			out[k] = strings.Join(vals, ", ")
		}
	}
	return out
}
