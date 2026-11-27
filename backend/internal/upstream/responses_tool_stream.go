package upstream

import (
	"bufio"
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// responses_tool_stream.go wraps upstream Responses SSE byte streams around
// convert.ResponsesToolStream so HTTP passthrough, compatible API Key relays,
// Grok Responses, and WebSocket HTTP bridges receive restored client tool items
// as standard Server-Sent Events frames.
//
// Function-only Responses upstreams stream lowered custom_tool_call and
// tool_search_call items as ordinary function_call deltas that buffer or expand
// into multiple client events while shifting sequence_number values. Running
// stateful restoration in a pipe-backed worker flushes each completed event
// batch before EOF, drops comment and empty-payload heartbeat frames without
// creating sequence gaps, compacts multiline JSON objects onto single data
// lines for downstream line scanners, and propagates scan or restoration errors
// while guaranteeing source closure on both reader and worker teardown.

// DefaultSSELineLimit names the five-hundred-MiB scanner limit used by
// upstream SSE consumers unless a caller supplies another value.
const DefaultSSELineLimit = 500 * 1024 * 1024

// responsesClientToolStreamBody exposes transformed pipe reads and owns
// closure of the source.
type responsesClientToolStreamBody struct {
	*io.PipeReader // transformed SSE byte reader
	// source is closed by both body.Close and worker cleanup. Its Close method
	// needs to tolerate repeated, potentially concurrent closes and interrupt its
	// reads when closure is used to stop transformation.
	source io.Closer // owned upstream response closer
}

// NewResponsesToolStreamBody starts a pipe-backed SSE transformation of source
// driven by convert.NewResponsesToolStream and a detached clone of tools.
// Callers must close the returned ReadCloser when reading finishes; nonpositive
// maxLineSize values select DefaultSSELineLimit, and closing the returned body
// wakes any blocked pipe write before closing source.
func NewResponsesToolStreamBody(
	source io.ReadCloser,
	tools *convert.ToolMap,
	maxLineSize int,
) io.ReadCloser {
	reader, writer := io.Pipe()
	body := &responsesClientToolStreamBody{PipeReader: reader, source: source}
	// Run stateful SSE restoration in a dedicated pipe worker so live frames
	// flush incrementally while reader closure interrupts blocked pipe writes.
	go transformResponsesToolStream(source, writer, tools, maxLineSize)
	return body
}

// Close releases the transformed reader before closing the upstream source.
// That order wakes a worker blocked on pipe output before source.Close can
// wait for upstream I/O; source-read interruption still depends on the source's
// close behavior. io.PipeReader.Close returns nil, leaving the source close
// error as this method's result.
//
// Worker cleanup also closes the source, so its closer needs to tolerate
// repeated, potentially concurrent calls. Close returns after those two close
// operations and does not join the transformation worker.
func (b *responsesClientToolStreamBody) Close() error {
	// A transformer can be blocked while flushing pipe output. Closing the
	// reader first wakes that writer before source closure potentially waits on I/O.
	readerErr := b.PipeReader.Close()
	sourceErr := b.source.Close()
	if readerErr != nil {
		return readerErr
	}
	return sourceErr
}

// transformResponsesToolStream scans SSE lines from source, restores lowered
// client tool events through convert.ResponsesToolStream, and flushes canonical
// SSE frames to destination. Every exit path closes source, and scan, line-size,
// restoration, or pipe-write failures close destination with the causative error.
//
// Leading comment lines and empty-payload ping or keepalive frames are dropped
// so upstream liveness traffic never emits blank protocol events or advances
// sequence numbers. Multiline JSON data payloads accumulate up to maxLineSize
// and compact onto single data lines without float64 re-encoding, while non-event
// SSE fields attach to the first emitted frame of their source message.
//
// Suppressed proxy argument deltas consume their source frame's pending event
// header without emitting downstream bytes. Unterminated tail fields at EOF
// flush ahead of any underlying scanner error so partial progress reaches
// callers before failure classification runs.
func transformResponsesToolStream(
	source io.ReadCloser,
	destination *io.PipeWriter,
	tools *convert.ToolMap,
	maxLineSize int,
) {
	// Always close the upstream body when the worker exits so early reader
	// disconnects and midstream restoration errors never leak open sockets.
	defer func() { _ = source.Close() }()
	if maxLineSize <= 0 {
		maxLineSize = DefaultSSELineLimit
	}

	scanner := bufio.NewScanner(source)
	// Return the pooled 64 KiB scanner buffer only after scanning finishes;
	// Scanner.Buffer retains the backing slice across incremental network reads.
	scanBuf := httpclient.GetSSEScannerBuf64K()
	defer httpclient.PutSSEScannerBuf64K(scanBuf)
	scanner.Buffer(scanBuf[:0], maxLineSize)
	documents := wire.NewSSEJSONDocumentScanner(scanner)
	restorer := convert.NewResponsesToolStream(tools)
	buffered := bufio.NewWriterSize(destination, 4*1024)

	nonEventFields := make([]string, 0, 2)
	var multilineData []byte
	var frameBuf []byte
	pendingEvent := ""
	frameHadEvent := false
	frameEmitted := false

	writeLine := func(line string) error {
		if _, err := buffered.WriteString(line); err != nil {
			return err
		}
		return buffered.WriteByte('\n')
	}

	resetFrameMetadata := func() {
		nonEventFields = nonEventFields[:0]
		multilineData = multilineData[:0]
		pendingEvent = ""
		frameHadEvent = false
	}

	// dispatchPayload restores one complete data payload through restorer and
	// flushes resulting SSE frames immediately for live downstream readers.
	dispatchPayload := func(payload []byte) error {
		inEvent := ""
		if frameHadEvent {
			inEvent = pendingEvent
		}
		// Filter non-error JSON or empty-payload heartbeat frames before entering
		// restorer so keepalive traffic never emits frames or shifts sequence numbers.
		if len(nonEventFields) == 0 && strings.TrimSpace(inEvent) != "error" && IsSSEHeartbeatEvent(inEvent, string(payload)) {
			resetFrameMetadata()
			frameEmitted = true
			return nil
		}
		frames, err := restorer.Write(sse.Frame{
			Event: inEvent,
			Data:  payload,
		})
		if err != nil {
			// Flush already buffered earlier frames best-effort before reporting
			// the restoration failure as the pipe reader's terminal error.
			_ = buffered.Flush()
			_, _ = restorer.Close(err)
			return fmt.Errorf("restore Responses client tool event: %w", err)
		}
		if len(frames) == 0 {
			// Buffered proxy deltas suppress output intentionally; clear pending
			// frame headers so the next upstream frame starts with clean metadata.
			resetFrameMetadata()
			frameEmitted = true
			return nil
		}
		for i, outFrame := range frames {
			if i == 0 {
				for _, field := range nonEventFields {
					if err := writeLine(field); err != nil {
						return err
					}
				}
			}
			outFrame.Data = compactSingleLineJSON(outFrame.Data)
			frameBuf = sse.Append(frameBuf[:0], outFrame)
			if _, err := buffered.Write(frameBuf); err != nil {
				return err
			}
		}
		resetFrameMetadata()
		frameEmitted = true
		return buffered.Flush()
	}

	for documents.Scan() {
		line := documents.Text()
		// Discard SSE comment lines immediately so keepalive comments never
		// allocate metadata entries or emit blank separators downstream.
		if strings.HasPrefix(line, ":") {
			continue
		}
		if data, isData := wire.ExtractSSEDataLine(line); isData {
			if len(multilineData) > 0 {
				if len(multilineData)+1+len(data) > maxLineSize {
					_, _ = restorer.Close(bufio.ErrTooLong)
					_ = destination.CloseWithError(bufio.ErrTooLong)
					return
				}
				multilineData = append(multilineData, '\n')
				multilineData = append(multilineData, data...)
				if !isCompleteJSONObjectBytes(multilineData) {
					continue
				}
				payload := bytes.Clone(multilineData)
				if err := dispatchPayload(payload); err != nil {
					_ = destination.CloseWithError(err)
					return
				}
				continue
			}

			trimmed := strings.TrimSpace(data)
			if trimmed != "" && trimmed[0] == '{' && !isCompleteJSONObjectBytes([]byte(trimmed)) {
				if len(data) > maxLineSize {
					_, _ = restorer.Close(bufio.ErrTooLong)
					_ = destination.CloseWithError(bufio.ErrTooLong)
					return
				}
				multilineData = append(multilineData[:0], data...)
				continue
			}

			if err := dispatchPayload([]byte(data)); err != nil {
				_ = destination.CloseWithError(err)
				return
			}
			continue
		}

		if line == "" {
			if len(multilineData) > 0 {
				payload := bytes.Clone(multilineData)
				if err := dispatchPayload(payload); err != nil {
					_ = destination.CloseWithError(err)
					return
				}
			} else if !frameEmitted && (frameHadEvent || len(nonEventFields) > 0) {
				// Drop empty-data heartbeat events (such as event: ping) while
				// preserving control frames like bare event: error and id/retry lines.
				if !isEmptyDataHeartbeatFrame(nonEventFields, frameHadEvent, pendingEvent) {
					for _, field := range nonEventFields {
						if err := writeLine(field); err != nil {
							_ = destination.CloseWithError(err)
							return
						}
					}
					if frameHadEvent {
						if err := writeLine("event: " + pendingEvent); err != nil {
							_ = destination.CloseWithError(err)
							return
						}
					}
					if err := writeLine(""); err != nil {
						_ = destination.CloseWithError(err)
						return
					}
					if err := buffered.Flush(); err != nil {
						_ = destination.CloseWithError(err)
						return
					}
				}
			}
			resetFrameMetadata()
			frameEmitted = false
			continue
		}

		if evName, isEvent := wire.ExtractSSEEventLine(line); isEvent {
			pendingEvent = evName
			frameHadEvent = true
			continue
		}
		nonEventFields = append(nonEventFields, line)
	}

	if len(multilineData) > 0 {
		payload := bytes.Clone(multilineData)
		if err := dispatchPayload(payload); err != nil {
			_ = destination.CloseWithError(err)
			return
		}
	} else if !frameEmitted && (frameHadEvent || len(nonEventFields) > 0) {
		// Flush unterminated non-heartbeat tail metadata before reporting any
		// underlying scanner failure so downstream readers observe trailing
		// non-comment SSE control lines.
		if !isEmptyDataHeartbeatFrame(nonEventFields, frameHadEvent, pendingEvent) {
			for _, field := range nonEventFields {
				if err := writeLine(field); err != nil {
					_ = destination.CloseWithError(err)
					return
				}
			}
			if frameHadEvent {
				if err := writeLine("event: " + pendingEvent); err != nil {
					_ = destination.CloseWithError(err)
					return
				}
			}
		}
	}

	scanErr := documents.Err()
	tailFrames, closeErr := restorer.Close(scanErr)
	for _, tailFrame := range tailFrames {
		tailFrame.Data = compactSingleLineJSON(tailFrame.Data)
		frameBuf = sse.Append(frameBuf[:0], tailFrame)
		if _, err := buffered.Write(frameBuf); err != nil {
			_ = destination.CloseWithError(err)
			return
		}
	}
	if err := buffered.Flush(); err != nil {
		_ = destination.CloseWithError(err)
		return
	}
	if scanErr != nil {
		_ = destination.CloseWithError(scanErr)
		return
	}
	if closeErr != nil {
		_ = destination.CloseWithError(closeErr)
		return
	}
	// Close the pipe writer with EOF after buffered frames flush so downstream
	// readers observe clean stream completion without losing trailing events.
	_ = destination.Close()
}

// isEmptyDataHeartbeatFrame reports whether a data-free SSE frame holds only a
// non-error heartbeat event header with no id or retry metadata lines. Bare
// event: error frames and frames carrying id or retry fields return false so
// control metadata still flushes downstream.
func isEmptyDataHeartbeatFrame(nonEventFields []string, frameHadEvent bool, pendingEvent string) bool {
	return len(nonEventFields) == 0 &&
		frameHadEvent &&
		strings.TrimSpace(pendingEvent) != "error" &&
		IsSSEHeartbeatEvent(pendingEvent, "")
}

// isCompleteJSONObjectBytes reports whether raw contains one syntactically
// complete JSON object while tolerating invalid UTF-8 bytes and duplicate keys.
// Truncated objects or trailing tokens return false, whereas duplicate-key
// objects complete here so restorer validation surfaces the duplicate-key error.
func isCompleteJSONObjectBytes(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true), jsontext.AllowDuplicateNames(true))
	if err := dec.SkipValue(); err != nil {
		return false
	}
	_, err := dec.ReadToken()
	return errors.Is(err, io.EOF)
}

// compactSingleLineJSON removes physical CR and LF line breaks outside JSON
// string literals while preserving exact integer digits and invalid UTF-8 bytes.
// Single-line inputs and non-JSON sentinels return data without copying.
func compactSingleLineJSON(data []byte) []byte {
	if !bytes.ContainsAny(data, "\r\n") {
		return data
	}
	cloned := jsontext.Value(bytes.Clone(data))
	if cloned.Compact() == nil {
		return []byte(cloned)
	}
	// Permissive token re-serialization compacts multiline JSON carrying raw
	// invalid UTF-8 string bytes when strict Compact fails, keeping exact integer digits.
	var out bytes.Buffer
	out.Grow(len(data))
	dec := jsontext.NewDecoder(bytes.NewReader(data), jsontext.AllowInvalidUTF8(true))
	enc := jsontext.NewEncoder(&out, jsontext.AllowInvalidUTF8(true))
	for {
		tok, err := dec.ReadToken()
		if errors.Is(err, io.EOF) {
			return bytes.TrimSuffix(out.Bytes(), []byte("\n"))
		}
		if err != nil {
			return data
		}
		if err := enc.WriteToken(tok); err != nil {
			return data
		}
	}
}
