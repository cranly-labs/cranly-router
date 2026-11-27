package qoder

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// stream_conversion.go scans wrapped Qoder SSE streams, normalizes text-encoded
// tool calls, merges observed prompt and completion usage, and extracts latest
// prompt text and history metadata shared across Qoder request planning and
// response reading.
//
// Scanner goroutines deliver parsed SSE batches over a bounded channel so
// context cancellation can release blocked sends without waiting on upstream
// network reads, while StreamQoderEvents flushes buffered text-tool fragments
// on clean EOF and closes the upstream body on every exit path.

// Execution defaults govern stream lifetime, saved conversations, and SSE line
// limits before Qoder events reach client protocol output.
const (
	// QoderDefaultMaxTokens, QoderStreamTimeout, and QoderConversationTTL govern
	// output budget, execution lifetime, and saved-history lifetime used by their respective owners.
	QoderDefaultMaxTokens = 32768
	QoderStreamTimeout    = 15 * time.Minute
	QoderConversationTTL  = 2 * time.Hour

	// defaultMaxLineSize supplies ScanQoderEvents and qoderFrameScanner with the
	// upstream package's shared SSE line cap in bytes. StreamEvents uses its
	// separate one-megabyte limit for the standalone helper.
	defaultMaxLineSize = upstream.DefaultSSELineLimit
)

// QoderModelInfo carries the selected Gateway model key and request source.
type QoderModelInfo struct {
	Key    string `json:"key"`    // Gateway model key
	Source string `json:"source"` // model source label
}

// QoderMessage retains visible history text and raw call/result identity for
// request conversion.
type QoderMessage struct {
	Role string // outgoing history role
	Text string // visible history text
	// ToolCallID overrides raw result-ID aliases when nonblank. Payload conversion
	// mirrors that trimmed identity into both Gateway fields; empty text permits
	// lookup through Raw.
	ToolCallID string // tool result identifier
	// Raw retains mutable caller metadata used by tool enrichment and payload
	// conversion. Callers coordinate mutation before sharing a request with
	// conversation planning.
	Raw map[string]any // raw history metadata
	// TextContent retains mutable block maps shared with outgoing payloads.
	// Cache-control insertion can update the last eligible map before request
	// serialization.
	TextContent []map[string]any // structured text blocks
}

// QoderToolNameMapper maps an upstream function name into the client's declared
// spelling.
type QoderToolNameMapper func(string) string

// QoderEventResult carries one scanner batch or terminal failure to its
// consumer.
type QoderEventResult struct {
	// ScanQoderEvents supplies events for a parsed batch or err for a failure.
	// StreamQoderEvents checks err before dispatching events, retaining terminal
	// failure precedence at the consumer.
	events []SSEEvent // parsed event batch
	err    error      // scanner or parser failure
}

// QoderMessageHasToolCalls reports whether raw history contains identifiable
// named tool calls.
func QoderMessageHasToolCalls(message QoderMessage) bool {
	return len(QoderToolCallNamesFromRaw(message.Raw)) > 0
}

// QoderMessageToolCallID selects explicit message identity before recognized raw
// result-ID aliases. Candidates are trimmed, and no usable value returns empty
// text for prompt and result enrichment callers.
func QoderMessageToolCallID(message QoderMessage) string {
	return FirstNonEmptyQoder(
		message.ToolCallID,
		QoderStringField(message.Raw, "tool_call_id"),
		QoderStringField(message.Raw, "tool_call_call_id"),
		QoderStringField(message.Raw, "call_id"),
	)
}

// QoderToolCallNamesFromRaw indexes named calls from modern history and
// Anthropic tool-use blocks. Only entries with both ID and name contribute;
// later matching IDs replace earlier names, and absent raw history returns an
// empty map.
func QoderToolCallNamesFromRaw(raw map[string]any) map[string]string {
	names := map[string]string{}
	if len(raw) == 0 {
		return names
	}
	for _, rawTool := range QoderAnySlice(raw["tool_calls"]) {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			continue
		}
		function, _ := tool["function"].(map[string]any)
		id := FirstNonEmptyQoder(
			QoderStringField(tool, "id"),
			QoderStringField(tool, "tool_call_id"),
			QoderStringField(tool, "call_id"),
		)
		name := FirstNonEmptyQoder(
			QoderStringField(function, "name"),
			QoderStringField(tool, "name"),
			QoderStringField(tool, "tool_name"),
		)
		if id != "" && name != "" {
			names[id] = name
		}
	}
	for _, rawBlock := range QoderAnySlice(raw["content"]) {
		block, ok := rawBlock.(map[string]any)
		if !ok || block["type"] != "tool_use" {
			continue
		}
		id := QoderStringField(block, "id")
		name := QoderStringField(block, "name")
		if id != "" && name != "" {
			names[id] = name
		}
	}
	return names
}

// QoderToolArgumentsString returns argument text from strings, raw JSON, or
// encoded values. Nil returns empty text; values that cannot be JSON marshaled
// use formatted text so later protocol validation can report their
// representation.
func QoderToolArgumentsString(raw any) string {
	switch v := raw.(type) {
	case nil:
		return ""
	case string:
		return v
	case json.RawMessage:
		return string(v)
	default:
		body, err := json.Marshal(v)
		if err != nil {
			// Unsupported Go arguments cannot be JSON marshaled. Keep their formatted
			// text so the protocol's tool-input validator can report the resulting
			// unusable representation.
			return fmt.Sprint(v)
		}
		return string(body)
	}
}

// ReadQoderSSEEventsContext collects normalized events while the shared stream
// reader owns body closure. Absent response bodies return an error. Stream or
// callback failure discards the collected slice and returns that error.
func ReadQoderSSEEventsContext(ctx context.Context, resp *http.Response) ([]SSEEvent, error) {
	if resp == nil || resp.Body == nil {
		return nil, errors.New("qoder response body is nil")
	}
	events := make([]SSEEvent, 0)
	if err := StreamQoderEvents(ctx, resp, func(event SSEEvent) error {
		events = append(events, event)
		return nil
	}); err != nil {
		return nil, err
	}
	return events, nil
}

// QoderResponsesID returns a fresh resp_ identifier derived from a random UUID.
func QoderResponsesID() string {
	return "resp_" + strings.ReplaceAll(uuid.NewString(), "-", "")
}

// QoderSendEventResult sends one scanner batch or stops on context
// cancellation. Blocking sends select against ctx.Done, and the returned
// boolean tells ScanQoderEvents whether to continue producing results, while the
// stream owner separately closes the response body to interrupt reads.
func QoderSendEventResult(ctx context.Context, results chan<- QoderEventResult, result QoderEventResult) bool {
	select {
	case results <- result:
		return true
	case <-ctx.Done():
		return false
	}
}

// ScanQoderEvents parses bounded SSE lines and closes its output channel when
// scanning ends. The stream owner closes the response body; this scanner
// observes context cancellation at result sends. Parse errors and scanner errors
// are delivered through the same channel, and completion events stop further
// scanning.
func ScanQoderEvents(ctx context.Context, resp *http.Response, results chan<- QoderEventResult) {
	defer close(results)
	scanner := bufio.NewScanner(resp.Body)
	// Wrapped argument JSON can exceed the initial allocation. Permit growth up to
	// the shared SSE line cap before reporting an oversized representation.
	scanner.Buffer(make([]byte, 0, 64*1024), defaultMaxLineSize)
	for scanner.Scan() {
		parsed, err := ParseSSELine(scanner.Text())
		if err != nil {
			QoderSendEventResult(ctx, results, QoderEventResult{err: err})
			return
		}
		if len(parsed) > 0 && !QoderSendEventResult(ctx, results, QoderEventResult{events: parsed}) {
			return
		}
		for _, event := range parsed {
			if event.IsDone {
				return
			}
		}
	}
	if err := scanner.Err(); err != nil {
		// A line-size failure has no parsed event to diagnose. Report the configured
		// maximum together with the initial buffer size in the scanner error's
		// diagnostic text.
		if errors.Is(err, bufio.ErrTooLong) {
			QoderSendEventResult(ctx, results, QoderEventResult{
				err: fmt.Errorf("SSE event exceeds max line size (%d bytes, buffer limit %d): %w", defaultMaxLineSize, 64*1024, err),
			})
			return
		}
		QoderSendEventResult(ctx, results, QoderEventResult{err: err})
	}
}

// StreamQoderEvents coordinates scanner results, transformed callbacks,
// and context cancellation. Nil context selects Background; callers
// supply a usable body and handler, and Close must release blocked reads for
// prompt scanner cleanup. A child context lets result sends stop when
// consumption ends. Handlers run serially and need to return; cancellation
// cannot interrupt a callback already executing.
//
// The scanner runs separately so cancellation can progress while
// upstream reads wait. Completion, EOF, cancellation, callback error, or scan
// error ends consumption; EOF flushes buffered text, while an error returns
// immediately. Exit cancels sends and closes the body,
// without joining the scanner goroutine.
func StreamQoderEvents(ctx context.Context, resp *http.Response, handle func(SSEEvent) error) error {
	if resp == nil || resp.Body == nil {
		return errors.New("qoder response body is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Give result sends their own cancellation signal. Consumption can end before
	// scanning, so exit cancellation releases a producer blocked behind the
	// callback owner.
	ctx, cancel := context.WithCancel(ctx)
	defer func() { _ = resp.Body.Close() }()
	defer cancel()

	results := make(chan QoderEventResult, 1)
	transformer := NewQoderTextToolCallTransformer()
	// Upstream reads can wait while cancellation needs to progress.
	// Run scanning separately and transfer batches through the bounded result
	// channel to this owner.
	go ScanQoderEvents(ctx, resp, results)
	for {
		select {
		case result, ok := <-results:
			if !ok {
				for _, event := range transformer.Flush() {
					if err := handle(event); err != nil {
						return err
					}
				}
				return nil
			}
			if result.err != nil {
				return result.err
			}
			for _, event := range result.events {
				for _, transformed := range transformer.Append(event) {
					if err := handle(transformed); err != nil {
						return err
					}
					if transformed.IsDone {
						return nil
					}
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// CloseQoderResponse closes a present response body and ignores its cleanup
// error. Nil response or body is accepted, allowing protocol writers to use the
// same deferred cleanup after partial setup.
func CloseQoderResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		// Read or conversion results already determine this attempt's outcome. Ignore
		// cleanup failure here so a deferred close cannot replace that result.
		_ = resp.Body.Close()
	}
}

// QoderUsageFromEvents returns the token snapshot supplied by the last observed
// usage event. Each HasUsage event replaces current counts through
// MergeQoderUsageEvent, including explicit zero observations; no usage returns
// zero values.
func QoderUsageFromEvents(events []SSEEvent) upstream.TokenUsage {
	usage := upstream.TokenUsage{}
	for _, event := range events {
		if event.HasUsage {
			MergeQoderUsageEvent(&usage, event)
		}
	}
	return usage
}

// MergeQoderUsageEvent replaces token counts from an observed usage payload and
// splits cached input. Nil target or an event without HasUsage is ignored.
// Prompt detail presence selects uncached input as max(prompt minus cached,
// zero), while cache-read count retains the reported cached value.
//
// Cached input can exceed prompt input; the clamp affects uncached input alone.
// Detail-bearing events clear cache creation. Absent prompt details restore
// prompt input, clear cache reads, and retain existing cache creation; completion
// replaces output. Observed prompt/output categories replace prior counts rather
// than adding repeated upstream observations.
func MergeQoderUsageEvent(usage *upstream.TokenUsage, event SSEEvent) {
	if usage == nil || !event.HasUsage {
		return
	}
	cachedTokens := 0
	if event.UsageDetails.PromptTokensDetails != nil {
		// Upstream prompt counts include reported cache reads. Split those categories
		// while clamping uncached input alone; cache-read evidence remains the
		// upstream observation.
		cachedTokens = event.UsageDetails.PromptTokensDetails.CachedTokens
		usage.InputTokens = max(event.PromptTokens-cachedTokens, 0)
		usage.CacheReadInputTokens = cachedTokens
		usage.CacheCreationInputTokens = 0
	} else {
		usage.InputTokens = event.PromptTokens
		usage.CacheReadInputTokens = 0
	}
	usage.OutputTokens = event.CompletionTokens
}

// LatestQoderUserText returns the last nonblank user text while retaining its
// spelling. Roles match user exactly, and an empty history or one lacking
// usable user text produces empty output.
func LatestQoderUserText(messages []QoderMessage) string {
	for _, message := range slices.Backward(messages) {
		if message.Role == "user" && strings.TrimSpace(message.Text) != "" {
			return message.Text
		}
	}
	return ""
}

// LatestQoderPromptText returns the last message whose prompt conversion
// yields usable text. Empty history or entirely blank messages return empty
// output so payload selection can consider another history source.
func LatestQoderPromptText(messages []QoderMessage) string {
	for _, message := range slices.Backward(messages) {
		if text := QoderPromptTextForMessage(message); text != "" {
			return text
		}
	}
	return ""
}

// QoderPromptTextForMessage returns text with a tool-result wrapper when a tool
// ID is available. Blank text returns empty, ordinary roles retain their text,
// and the wrapper escapes call identity for markup.
func QoderPromptTextForMessage(message QoderMessage) string {
	text := strings.TrimSpace(message.Text)
	if text == "" {
		return ""
	}
	if message.Role != "tool" {
		return message.Text
	}
	toolCallID := QoderMessageToolCallID(message)
	if toolCallID == "" {
		return message.Text
	}
	return fmt.Sprintf("<tool_result id=\"%s\">\n%s\n</tool_result>", html.EscapeString(toolCallID), message.Text)
}

// LatestQoderPayloadPromptText selects trailing tool continuation, then latest
// user, then latest usable message text. The includeTools argument is currently
// unused, so prompt selection follows message history regardless of
// tool-definition inclusion.
func LatestQoderPayloadPromptText(messages []QoderMessage, includeTools bool) string {
	if text := QoderToolContinuationPromptText(messages); text != "" {
		return text
	}
	if text := LatestQoderUserText(messages); text != "" {
		return text
	}
	return LatestQoderPromptText(messages)
}

// QoderToolContinuationPromptText joins the trailing run of tool results into
// a current prompt. A history containing only tools qualifies directly. When
// earlier history exists, the immediately preceding turn needs identifiable
// named calls before the tool run can replace ordinary user-text selection.
//
// The adjacency check identifies a call-bearing turn; it does not compare each
// result ID with that turn's call IDs. QoderPromptTextForMessage wraps
// available result identity and preserves readable result text. The request
// parser separately owns Responses pairing before this prompt selection runs.
func QoderToolContinuationPromptText(messages []QoderMessage) string {
	toolStart := len(messages)
	for toolStart > 0 && messages[toolStart-1].Role == "tool" {
		toolStart--
	}
	if toolStart == len(messages) {
		return ""
	}

	// A supplied history can consist entirely of tool results. When earlier history
	// exists, require an adjacent assistant call turn so unrelated old results
	// cannot replace the current prompt merely because they end the message
	// sequence.
	if toolStart > 0 && !QoderMessageHasToolCalls(messages[toolStart-1]) {
		return ""
	}

	parts := make([]string, 0, len(messages)-toolStart)
	for _, message := range messages[toolStart:] {
		if text := QoderPromptTextForMessage(message); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n\n")
}

// QoderAnySlice returns a supplied []any directly or an empty slice for other
// values. The returned input slice remains shared; callers copy it before
// changing elements when they need independent ownership.
func QoderAnySlice(raw any) []any {
	if raw == nil {
		return []any{}
	}
	if values, ok := raw.([]any); ok {
		return values
	}
	return []any{}
}

// QoderStringField returns trimmed string or formatted text for a present map
// value. Missing map/key or nil value returns empty text, letting protocol
// callers apply their own precedence and defaults.
func QoderStringField(values map[string]any, key string) string {
	if len(values) == 0 {
		return ""
	}
	value, ok := values[key]
	if !ok || value == nil {
		return ""
	}
	if text, ok := value.(string); ok {
		return strings.TrimSpace(text)
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

// TruncateRunes limits text by Unicode code-point count and returns empty for
// nonpositive limits. A short input retains its supplied string; truncation can
// divide a grapheme that contains multiple runes.
func TruncateRunes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	// Truncation counts Unicode code points, leaving the preceding complete runes.
	return string(runes[:limit])
}

// FirstNonEmptyQoder selects the first candidate containing non-whitespace
// text and returns its trimmed value. No candidates or an entirely blank list
// produces empty text for the caller's next default.
func FirstNonEmptyQoder(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
