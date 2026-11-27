package provider

import (
	"bufio"
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// test_streams.go converts platform test streams into content, error, and
// completion events. Provider test streams expose platform content through one
// typed event sink, but their completion rules differ. Gemini and ordinary
// Anthropic accept EOF as success, Qoder accepts EOF after a parsed event, and
// Responses requires a terminal response event.
//
// Chat tracks both parsed JSON and finish reasons so a stream ending after a
// finish reason can succeed while unfinished JSON fails. Gemini emits a frame's
// text and images before recognizing its finish reason, preserving content
// carried by the terminating frame. Completion suppression lets adaptive probes
// share these parsers without publishing intermediate whole-test success. Sink
// failures are logged here and retained by TestRun, which cancels execution and
// combines them with the executor's final result.

// sseDataPrefix recognizes data fields with optional following whitespace for
// parsers that accept both compact and spaced SSE framing.
var sseDataPrefix = regexp.MustCompile(`^data:\s*`)

// TestStreamOutput converts platform stream data into synchronous provider test
// events.
type TestStreamOutput struct{}

// wireQoderProbeChatDelta extracts visible delta.content from converted Chat
// SSE chunks during Qoder connection probe streaming.
type wireQoderProbeChatDelta struct {
	Content string `json:"content,omitzero"` // incremental visible assistant text
}

// wireQoderProbeChatChoice extracts one choice delta from a converted Chat SSE chunk.
type wireQoderProbeChatChoice struct {
	Delta wireQoderProbeChatDelta `json:"delta,omitzero"` // choice delta payload
}

// wireQoderProbeChatChunk decodes converted Chat Completions SSE chunks emitted
// by TestStreamOutput.Qoder without unmarshaling through map[string]any.
type wireQoderProbeChatChunk struct {
	Choices []wireQoderProbeChatChoice `json:"choices,omitzero"` // converted Chat choices array
}

// Gemini reads newline-delimited SSE data and emits content from the first
// candidate. EOF, DONE, or a nonempty finish reason emits successful
// completion; malformed JSON frames are skipped, and other read failures or
// error objects emit failures.
//
// AI Studio candidates can appear at the root, while CLI responses wrap them in
// response. After unwrapping, text and inline image parts are emitted before
// checking the candidate's finish reason, so content in a terminal frame
// reaches the sink. The caller owns the reader's response-body cleanup. Only
// newline-terminated data is parsed; an EOF fragment is discarded.
func (s TestStreamOutput) Gemini(c *TestRun, body io.Reader) error {
	reader := bufio.NewReader(body)

	for {
		line, err := reader.ReadString('\n')
		// Handle EOF before parsing returned bytes: an unterminated final fragment is
		// discarded, and Gemini completes through its separate EOF rule.
		if err != nil {
			if err == io.EOF {
				s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
				return nil
			}
			return s.Error(c, fmt.Sprintf("Stream read error: %s", err.Error()))
		}

		line = strings.TrimSpace(line)
		if line == "" || !strings.HasPrefix(line, "data: ") {
			continue
		}

		jsonStr := strings.TrimPrefix(line, "data: ")
		if jsonStr == "[DONE]" {
			s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
			return nil
		}

		var data map[string]any
		// Ignore malformed frame data and continue looking for a usable platform
		// event. Completion still follows this parser's separate terminal or EOF rule.
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			continue
		}

		// AI Studio places candidates at the root; Gemini CLI wraps the same
		// candidate structure in response. Unwrap once so both forms share content
		// and termination handling.
		if resp, ok := data["response"].(map[string]any); ok && resp != nil {
			data = resp
		}
		if candidates, ok := data["candidates"].([]any); ok && len(candidates) > 0 {
			if candidate, ok := candidates[0].(map[string]any); ok {
				// A terminating candidate can also carry its final text or image parts.
				// Emit those parts before finishReason returns so the preview retains that content.
				if content, ok := candidate["content"].(map[string]any); ok {
					if parts, ok := content["parts"].([]any); ok {
						for _, part := range parts {
							if partMap, ok := part.(map[string]any); ok {
								if text, ok := partMap["text"].(string); ok && text != "" {
									s.SendEvent(c, providercore.TestEvent{Type: "content", Text: text})
								}
								if inlineData, ok := partMap["inlineData"].(map[string]any); ok {
									mimeType, _ := inlineData["mimeType"].(string)
									data, _ := inlineData["data"].(string)
									if strings.HasPrefix(strings.ToLower(mimeType), "image/") && data != "" {
										s.SendEvent(c, providercore.TestEvent{
											Type:     "image",
											ImageURL: fmt.Sprintf("data:%s;base64,%s", mimeType, data),
											MimeType: mimeType,
										})
									}
								}
							}
						}
					}
				}

				if finishReason, ok := candidate["finishReason"].(string); ok && finishReason != "" {
					s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
					return nil
				}
			}
		}

		if errData, ok := data["error"].(map[string]any); ok {
			// An error event can omit usable message details. Keep the event-specific
			// diagnostic until a string message is available from the expected platform shape.
			errorMsg := "Unknown error"
			if msg, ok := errData["message"].(string); ok {
				errorMsg = msg
			}
			return s.Error(c, errorMsg)
		}
	}
}

// Anthropic emits text deltas from newline-delimited Claude SSE data. EOF,
// DONE, and message_stop emit completion, malformed JSON frames are skipped,
// and upstream or read errors emit failure; the caller closes the response
// body. Only newline-terminated data is parsed; an EOF fragment is discarded.
func (s TestStreamOutput) Anthropic(c *TestRun, body io.Reader) error {
	reader := bufio.NewReader(body)

	for {
		line, err := reader.ReadString('\n')
		// Handle EOF before parsing returned bytes: an unterminated final fragment is
		// discarded, and ordinary Anthropic completes through its separate EOF rule.
		if err != nil {
			if err == io.EOF {
				s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
				return nil
			}
			return s.Error(c, fmt.Sprintf("Stream read error: %s", err.Error()))
		}

		line = strings.TrimSpace(line)
		if line == "" || !sseDataPrefix.MatchString(line) {
			continue
		}

		jsonStr := sseDataPrefix.ReplaceAllString(line, "")
		if jsonStr == "[DONE]" {
			s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
			return nil
		}

		var data map[string]any
		// Ignore malformed frame data and continue looking for a usable platform
		// event. Completion still follows this parser's separate terminal or EOF rule.
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			continue
		}

		eventType, _ := data["type"].(string)

		switch eventType {
		case "content_block_delta":
			if delta, ok := data["delta"].(map[string]any); ok {
				if text, ok := delta["text"].(string); ok {
					s.SendEvent(c, providercore.TestEvent{Type: "content", Text: text})
				}
			}
		case "message_stop":
			s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
			return nil
		case "error":
			// An error event can omit usable message details. Keep the event-specific
			// diagnostic until a string message is available from the expected platform shape.
			errorMsg := "Unknown error"
			if errData, ok := data["error"].(map[string]any); ok {
				if msg, ok := errData["message"].(string); ok {
					errorMsg = msg
				}
			}
			return s.Error(c, errorMsg)
		}
	}
}

// Qoder decodes a Qoder COSY SSE response stream through protocol/sse and
// protocol/convert, emits visible text deltas as probe content events, and
// closes body on exit. Nil bodies, wrapped upstream API errors, malformed SSE
// payloads, reader errors, and EOF before any parsed event return a probe error.
//
// Explicit [DONE] or message_stop termination completes immediately, while
// clean EOF after at least one parsed Qoder content, reasoning, tool, or usage
// frame also completes after flushing any buffered text suffix.
func (s TestStreamOutput) Qoder(c *TestRun, body io.ReadCloser) error {
	if body == nil {
		return s.Error(c, "Qoder response body is nil")
	}
	// Ignore response-body close errors on exit because stream event parsing and
	// scanner state determine the connection probe outcome.
	defer func() { _ = body.Close() }()

	exchange, err := convert.NewExchange(
		[]byte(`{"model":"qoder-probe","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"probe"}]}`),
		convert.Options{
			Route: convert.Route{
				Client:   protocol.ProtocolOpenAIChatCompletions,
				Upstream: protocol.ProtocolQoderChat,
			},
			Runtime: convert.DefaultRuntime(),
		},
	)
	if err != nil {
		return s.Error(c, err.Error())
	}
	stream := exchange.Stream()
	reader := sse.NewReaderLimit(body, upstream.DefaultSSELineLimit)
	seenEvent := false

	emitChatChunkDeltas := func(frames []sse.Frame) (sawTerminal bool) {
		for _, f := range frames {
			if f.Event == "error" {
				continue
			}
			trimmed := bytes.TrimSpace(f.Data)
			if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("[DONE]")) {
				if f.Terminal {
					sawTerminal = true
				}
				continue
			}
			var chunk wireQoderProbeChatChunk
			if jsonv2.Unmarshal(trimmed, &chunk, jsontext.AllowInvalidUTF8(true)) == nil {
				for _, ch := range chunk.Choices {
					if ch.Delta.Content != "" {
						seenEvent = true
						s.SendEvent(c, providercore.TestEvent{Type: "content", Text: ch.Delta.Content})
					}
				}
			}
			if f.Terminal {
				sawTerminal = true
			}
		}
		return sawTerminal
	}

	for reader.Scan() {
		frame := reader.Frame()
		if len(bytes.TrimSpace(frame.Data)) == 0 {
			continue
		}
		batch, wErr := stream.Write(frame)
		if wErr != nil {
			return s.Error(c, wErr.Error())
		}
		summary := stream.Summary()
		if len(batch) > 0 || summary.HasContent || summary.Usage != nil {
			seenEvent = true
		}
		if emitChatChunkDeltas(batch) {
			s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
			return nil
		}
	}
	if err := reader.Err(); err != nil {
		return s.Error(c, fmt.Sprintf("Qoder stream read error: %s", err.Error()))
	}
	// Flush any buffered text suffix at clean EOF while ignoring missing-terminal
	// Close errors so Qoder probes still succeed after at least one parsed event.
	if tail, closeErr := stream.Close(nil); closeErr == nil {
		_ = emitChatChunkDeltas(tail)
	}
	if seenEvent {
		s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
		return nil
	}
	return s.Error(c, "Qoder stream ended before any response")
}

// ChatCompletions converts OpenAI-compatible SSE chunks into probe content and
// completion. A DONE marker succeeds immediately, and malformed data JSON or
// explicit error objects produce a failure; callers close the supplied response
// body.
//
// The parser separately records parsed JSON and nonempty finish reasons. EOF
// after a finish reason is accepted, but EOF after unfinished JSON reports a
// missing terminal marker, and EOF without JSON reports an invalid response.
// Content is read from both delta and message forms before finish state is
// recorded. Only newline-terminated data is parsed; an EOF fragment is
// discarded.
func (s TestStreamOutput) ChatCompletions(c *TestRun, body io.Reader) error {
	reader := bufio.NewReader(body)
	seenJSON := false
	seenFinish := false

	for {
		line, err := reader.ReadString('\n')
		// A read error takes precedence over returned fragments. EOF can complete
		// only from a finish reason recorded on an earlier newline-terminated chunk.
		if err != nil {
			if err == io.EOF {
				if seenFinish {
					s.SendEvent(c, providercore.TestEvent{Type: "status", Text: "Verified through /v1/chat/completions"})
					s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
					return nil
				}
				if seenJSON {
					return s.Error(c, "Chat Completions stream from /v1/chat/completions ended before [DONE]")
				}
				return s.Error(c, "Invalid Chat Completions response from /v1/chat/completions: expected SSE JSON data")
			}
			return s.Error(c, fmt.Sprintf("Chat Completions stream read error from /v1/chat/completions: %s", err.Error()))
		}

		line = strings.TrimSpace(line)
		if line == "" || !sseDataPrefix.MatchString(line) {
			continue
		}

		jsonStr := sseDataPrefix.ReplaceAllString(line, "")
		if jsonStr == "[DONE]" {
			s.SendEvent(c, providercore.TestEvent{Type: "status", Text: "Verified through /v1/chat/completions"})
			s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
			return nil
		}

		var data map[string]any
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			return s.Error(c, "Invalid Chat Completions response from /v1/chat/completions: expected JSON data")
		}
		seenJSON = true

		if errData, ok := data["error"].(map[string]any); ok {
			errorMsg := "Chat Completions API (/v1/chat/completions) returned an error"
			if msg, ok := errData["message"].(string); ok && msg != "" {
				errorMsg = msg
			}
			return s.Error(c, fmt.Sprintf("Chat Completions API (/v1/chat/completions) error: %s", errorMsg))
		}

		choices, ok := data["choices"].([]any)
		if !ok {
			continue
		}
		for _, choiceValue := range choices {
			choice, ok := choiceValue.(map[string]any)
			if !ok {
				continue
			}
			if delta, ok := choice["delta"].(map[string]any); ok {
				if text, ok := delta["content"].(string); ok && text != "" {
					s.SendEvent(c, providercore.TestEvent{Type: "content", Text: text})
				}
			}
			if message, ok := choice["message"].(map[string]any); ok {
				if text, ok := message["content"].(string); ok && text != "" {
					s.SendEvent(c, providercore.TestEvent{Type: "content", Text: text})
				}
			}
			if finishReason, ok := choice["finish_reason"].(string); ok && finishReason != "" {
				seenFinish = true
			}
		}
	}
}

// Responses emits output-text deltas and requires response.completed or
// response.done for success. EOF and DONE before those events fail, malformed
// JSON frames are skipped, and error or response.failed events emit a failure;
// callers close the reader. Only newline-terminated data is parsed; an EOF
// fragment is discarded.
func (s TestStreamOutput) Responses(c *TestRun, body io.Reader) error {
	reader := bufio.NewReader(body)
	for {
		line, err := reader.ReadString('\n')
		// An EOF fragment is discarded before parsing. Success therefore requires a
		// completed or done event from an earlier newline-terminated frame; otherwise
		// this call fails.
		if err != nil {
			if err == io.EOF {
				return s.Error(c, "Stream ended before response.completed")
			}
			return s.Error(c, fmt.Sprintf("Stream read error: %s", err.Error()))
		}

		line = strings.TrimSpace(line)
		if line == "" || !sseDataPrefix.MatchString(line) {
			continue
		}

		jsonStr := sseDataPrefix.ReplaceAllString(line, "")
		if jsonStr == "[DONE]" {
			return s.Error(c, "Stream ended before response.completed")
		}

		var data map[string]any
		// Ignore malformed frame data and continue looking for a usable platform
		// event. Completion still follows this parser's separate terminal or EOF rule.
		if err := json.Unmarshal([]byte(jsonStr), &data); err != nil {
			continue
		}

		eventType, _ := data["type"].(string)

		switch eventType {
		case "response.output_text.delta":
			if delta, ok := data["delta"].(string); ok && delta != "" {
				s.SendEvent(c, providercore.TestEvent{Type: "content", Text: delta})
			}
		case "response.completed", "response.done":
			s.SendEvent(c, providercore.TestEvent{Type: "test_complete", Success: true})
			return nil
		case "response.failed":
			// An error event can omit usable message details. Keep the event-specific
			// diagnostic until a string message is available from the expected platform shape.
			errorMsg := "OpenAI response failed"
			if responseData, ok := data["response"].(map[string]any); ok {
				if errData, ok := responseData["error"].(map[string]any); ok {
					if msg, ok := errData["message"].(string); ok && msg != "" {
						errorMsg = msg
					}
				}
			}
			return s.Error(c, errorMsg)
		case "error":
			// An error event can omit usable message details. Keep the event-specific
			// diagnostic until a string message is available from the expected platform shape.
			errorMsg := "Unknown error"
			if errData, ok := data["error"].(map[string]any); ok {
				if msg, ok := errData["message"].(string); ok {
					errorMsg = msg
				}
			}
			return s.Error(c, errorMsg)
		}
	}
}

// SendEvent emits a typed event unless an intermediate completion is
// suppressed. Sink failures are logged and remain retained by TestRun for
// Result, while content, status, and error events remain eligible during
// suppression.
func (s TestStreamOutput) SendEvent(c *TestRun, event providercore.TestEvent) {
	if event.Type == "test_complete" && c.SuppressCompletion() {
		return
	}
	// SendEvent cannot return a write failure to every parser call site. Log it
	// while TestRun retains the first error and cancels cooperative upstream execution.
	if err := c.Emit(event); err != nil {
		logging.LegacyPrintf("provider.test_streams", "failed to write SSE event: %v", err)
	}
}

// Error logs and emits the supplied probe failure, then returns an error with
// that message. A sink failure stays in TestRun for final Result handling
// rather than replacing this diagnostic message.
func (s TestStreamOutput) Error(c *TestRun, errorMsg string) error {
	logging.LegacyPrintf("provider.test_streams", "Provider test error: %s", errorMsg)
	s.SendEvent(c, providercore.TestEvent{Type: "error", Error: errorMsg})
	return fmt.Errorf("%s", errorMsg)
}
