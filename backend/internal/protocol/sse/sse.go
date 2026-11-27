package sse

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
)

// sse.go parses bounded Server-Sent Events frames from incremental byte streams
// and formats outgoing frames. Upstream event streams can split UTF-8 sequences,
// JSON payloads, and CRLF delimiters across arbitrary network read boundaries,
// omit event headers or final blank lines, and interleave comments or multiline
// data fields. Assembling complete frames before protocol decoding keeps split
// lines and multiline payloads intact regardless of transport chunking.
//
// Each nonblank physical line in a pending frame, including comments and ignored
// fields, consumes per-frame byte budget before its contents are buffered, and
// blank frame delimiters reset that budget. Normal EOF dispatches any buffered
// nonempty data without requiring a trailing blank line, whereas a non-EOF read
// failure dispatches an unterminated final frame only when its joined data is
// valid JSON while retaining the read error in Err. Append normalizes embedded
// line breaks into canonical LF data lines and strips CR/LF from event headers.

const (
	// DefaultMaxFrameBytes bounds accumulated bytes in one SSE frame at 500 MiB
	// when callers construct a Reader without a positive limit, matching the
	// gateway's shared conversion frame budget.
	DefaultMaxFrameBytes = 500 * 1024 * 1024

	// readerBufferSize and minReaderBufferSize bound the internal bufio.Reader
	// chunk window independently of DefaultMaxFrameBytes so ordinary streams
	// allocate a compact fixed read buffer while small test limits avoid large
	// prefetches.
	readerBufferSize    = 32 * 1024
	minReaderBufferSize = 16
)

var (
	// ErrFrameTooLarge reports that one SSE frame exceeded the configured byte
	// budget, allowing stream adapters to reject truncated output even if an
	// earlier frame already carried a stop signal.
	ErrFrameTooLarge = errors.New("sse: frame exceeds size limit")

	// utf8BOM marks the optional leading byte-order mark stripped once at the
	// start of an SSE stream under the WHATWG event-stream specification.
	utf8BOM = []byte{0xEF, 0xBB, 0xBF}
)

// Frame represents one logical Server-Sent Events message and its stream
// delivery classification.
type Frame struct {
	Event string // SSE event field label, or empty for data-only frames
	Data  []byte // SSE data payload, joined with LF across multiline data fields

	// Semantic and Terminal carry conversion-layer stream observations for
	// gateway prelude buffering and OutputContext.NextEvent. Reader leaves both
	// false on decoded wire frames, and Append omits them from wire output.
	Semantic bool // whether the frame carries client-visible content or tool calls
	Terminal bool // whether the frame ends the client-facing protocol stream
}

// Reader incrementally scans bounded SSE frames from an io.Reader on a single
// goroutine. Its zero value is exhausted and returns false from Scan and nil
// from Err.
type Reader struct {
	// br and maxBytes hold the buffered source stream and per-frame byte limit.
	// Termination clears br so completed or failed readers release buffer memory
	// and never read from the underlying stream again.
	br       *bufio.Reader // buffered source byte stream
	maxBytes int           // maximum accumulated bytes per frame

	// started, line, event, data, hasData, and frameBytes track the pending frame
	// across chunk boundaries. Blank delimiters reset event, data, hasData, and
	// frameBytes together, while overflow clears all buffered state immediately.
	started    bool   // whether the first line's optional UTF-8 BOM was checked
	line       []byte // partial physical line spanning internal buffer fills
	event      string // last declared event label in the pending frame
	data       []byte // joined data lines accumulated in the pending frame
	hasData    bool   // whether the pending frame saw at least one data field
	frameBytes int    // bytes charged to the pending frame so far

	// current, err, and finished expose the outcome of the latest Scan call.
	// Each true Scan populates current with an owned Data slice, while false
	// clears current and leaves err holding any non-EOF read or size failure.
	current  Frame // latest complete frame selected by Scan
	err      error // retained non-EOF read error or ErrFrameTooLarge
	finished bool  // whether scanning has reached EOF or a terminal failure
}

// Append formats f as a complete LF-delimited SSE frame appended to dst and
// returns the extended slice. Nonempty Event values emit an event header with
// any CR or LF bytes removed, and Data bytes split across CR, LF, or CRLF emit
// one data line per segment followed by a blank frame delimiter.
//
// Empty Data still emits an empty data field and blank delimiter, while f.Data
// is never mutated and Semantic and Terminal remain in-memory metadata excluded
// from wire bytes. When dst has sufficient spare capacity for single-line data,
// Append writes directly without intermediate allocations.
func Append(dst []byte, f Frame) []byte {
	hasEvent, cleanEvent := sanitizeEvent(f.Event)
	if len(f.Data) == 0 {
		needed := len("data: \n\n")
		if hasEvent {
			needed += len("event: ") + len(cleanEvent) + 1
		}
		dst = slices.Grow(dst, needed)
		if hasEvent {
			dst = append(dst, "event: "...)
			dst = append(dst, cleanEvent...)
			dst = append(dst, '\n')
		}
		return append(dst, "data: \n\n"...)
	}

	if bytes.IndexByte(f.Data, '\n') < 0 && bytes.IndexByte(f.Data, '\r') < 0 {
		needed := len("data: ") + len(f.Data) + len("\n\n")
		if hasEvent {
			needed += len("event: ") + len(cleanEvent) + 1
		}
		dst = slices.Grow(dst, needed)
		if hasEvent {
			dst = append(dst, "event: "...)
			dst = append(dst, cleanEvent...)
			dst = append(dst, '\n')
		}
		dst = append(dst, "data: "...)
		dst = append(dst, f.Data...)
		return append(dst, "\n\n"...)
	}

	return appendMultilineFrame(dst, hasEvent, cleanEvent, f.Data)
}

// sanitizeEvent removes CR and LF characters so an event label cannot inject
// synthetic SSE fields or premature frame delimiters. It reports false when the
// input is empty or contains only line-break characters.
func sanitizeEvent(event string) (bool, string) {
	if event == "" {
		return false, ""
	}
	if !strings.ContainsAny(event, "\r\n") {
		return true, event
	}
	buf := make([]byte, 0, len(event))
	for i := range len(event) {
		if event[i] != '\r' && event[i] != '\n' {
			buf = append(buf, event[i])
		}
	}
	if len(buf) == 0 {
		return false, ""
	}
	return true, string(buf)
}

// appendMultilineFrame encodes nonempty data containing CR or LF breaks as one
// canonical LF data field per line segment followed by a blank line. Treating
// CRLF as a single break and bare CR as its own break keeps downstream SSE
// parsers from truncating or misframing payloads.
func appendMultilineFrame(dst []byte, hasEvent bool, cleanEvent string, data []byte) []byte {
	if hasEvent {
		dst = append(dst, "event: "...)
		dst = append(dst, cleanEvent...)
		dst = append(dst, '\n')
	}
	rest := data
	for {
		idx := bytes.IndexAny(rest, "\r\n")
		if idx < 0 {
			dst = append(dst, "data: "...)
			dst = append(dst, rest...)
			return append(dst, "\n\n"...)
		}
		dst = append(dst, "data: "...)
		dst = append(dst, rest[:idx]...)
		dst = append(dst, '\n')
		if rest[idx] == '\r' && idx+1 < len(rest) && rest[idx+1] == '\n' {
			rest = rest[idx+2:]
		} else {
			rest = rest[idx+1:]
		}
	}
}

// NewReader constructs a Reader over r using DefaultMaxFrameBytes as the
// per-frame byte limit. A nil r yields an exhausted Reader whose Scan returns
// false and Err returns nil; callers retain responsibility for closing r.
func NewReader(r io.Reader) *Reader {
	return NewReaderLimit(r, DefaultMaxFrameBytes)
}

// NewReaderLimit constructs a Reader over r that rejects any single frame
// exceeding maxFrameBytes with ErrFrameTooLarge. Nonpositive maxFrameBytes
// values select DefaultMaxFrameBytes, and a nil r yields an exhausted Reader;
// callers serialize access and close r when done.
func NewReaderLimit(r io.Reader, maxFrameBytes int) *Reader {
	if maxFrameBytes <= 0 {
		maxFrameBytes = DefaultMaxFrameBytes
	}
	if r == nil {
		return &Reader{maxBytes: maxFrameBytes, finished: true}
	}
	// Cap the read buffer near small frame limits so tests and tight bounds do
	// not prefetch far past an overflowing frame while keeping bufio's minimum.
	bufSize := readerBufferSize
	if maxFrameBytes < readerBufferSize-2 {
		bufSize = max(maxFrameBytes+2, minReaderBufferSize)
	}
	return &Reader{
		br:       bufio.NewReaderSize(r, bufSize),
		maxBytes: maxFrameBytes,
	}
}

// Scan advances to the next nonempty SSE frame and reports whether Frame holds
// a delivered message. Blank lines dispatch accumulated data lines joined with
// LF and reset both the event label and per-frame byte counter; frames with
// only comments, ignored fields, or empty data produce no Frame and still reset
// any pending event label at the blank boundary.
//
// Both LF and CRLF line endings are accepted within the same stream, and a
// single UTF-8 BOM at the start of the first line is removed. Every nonblank
// line, including comments and unrecognized fields, counts toward maxFrameBytes
// before buffering; exceeding the limit clears buffered state, sets
// ErrFrameTooLarge, and stops further reads.
//
// At normal EOF, Scan dispatches a trailing nonempty frame even without a final
// blank line. When a non-EOF read error interrupts an unterminated final frame,
// Scan dispatches that frame only if its joined Data is valid JSON while still
// recording the read error for Err, matching gateway usage recovery on dropped
// connections.
func (r *Reader) Scan() bool {
	if r.finished || r.br == nil {
		r.current = Frame{}
		return false
	}

	for {
		fragment, readErr := r.br.ReadSlice('\n')
		if errors.Is(readErr, bufio.ErrBufferFull) {
			if !r.appendPendingFragment(fragment) {
				return false
			}
			continue
		}

		rawLine := fragment
		if len(r.line) > 0 {
			r.line = append(r.line, fragment...)
			rawLine = r.line
		}

		if readErr == nil {
			frame, emitted, withinLimit := r.consumeTerminatedLine(rawLine)
			r.line = r.line[:0]
			if !withinLimit {
				return false
			}
			if emitted {
				r.current = frame
				return true
			}
			continue
		}

		return r.finishAtReadEnd(rawLine, readErr)
	}
}

// Frame returns the complete frame selected by the most recent true Scan call.
// Its Data slice is an independent copy safe to retain or mutate across later
// Scan calls; before the first Scan or after Scan returns false, Frame returns
// a zero Frame.
func (r *Reader) Frame() Frame {
	return r.current
}

// Err returns the first non-EOF read error or ErrFrameTooLarge encountered by
// Scan, or nil after normal EOF. Because a complete JSON frame lacking a final
// blank delimiter can still be delivered when a read error follows it, callers
// must inspect Err after the Scan loop ends even when frames were processed.
func (r *Reader) Err() error {
	return r.err
}

// appendPendingFragment buffers an incomplete physical line chunk when it can
// still fit within the remaining frame budget. When the chunk already exceeds
// that budget plus delimiter slack, it marks the reader failed with
// ErrFrameTooLarge and returns false.
func (r *Reader) appendPendingFragment(fragment []byte) bool {
	remaining := r.maxBytes - r.frameBytes
	// Allow one byte for a trailing CR that may pair with the next chunk's LF,
	// plus three bytes on the first line before UTF-8 BOM stripping runs.
	slack := 1
	if !r.started {
		slack += len(utf8BOM)
	}
	if len(r.line)+len(fragment) > remaining+slack {
		r.failTooLarge()
		return false
	}
	r.line = append(r.line, fragment...)
	return true
}

// consumeTerminatedLine normalizes one LF-terminated physical line, enforces
// the per-frame byte budget, and updates or dispatches the pending frame. It
// returns false for withinLimit after recording ErrFrameTooLarge when the line
// overflows the remaining frame budget.
func (r *Reader) consumeTerminatedLine(rawLine []byte) (Frame, bool, bool) {
	line := r.normalizeLine(rawLine[:len(rawLine)-1])
	if len(line) == 0 {
		r.frameBytes = 0
		frame, ok := r.dispatch()
		return frame, ok, true
	}

	// Charge one byte for the normalized line delimiter so many short data or
	// comment lines cannot accumulate past the frame limit before a blank line.
	if len(line) >= r.maxBytes-r.frameBytes {
		r.failTooLarge()
		return Frame{}, false, false
	}
	r.frameBytes += len(line) + 1
	r.consumeField(line)
	return Frame{}, false, true
}

// finishAtReadEnd processes any unterminated trailing bytes at EOF or read
// failure and releases internal reader buffers. It delivers a final frame at
// normal EOF or when a connection failure leaves complete JSON in Data, while
// retaining non-EOF read errors in r.err.
func (r *Reader) finishAtReadEnd(rawLine []byte, readErr error) bool {
	if len(rawLine) > 0 {
		line := r.normalizeLine(rawLine)
		if len(line) > 0 {
			if len(line) > r.maxBytes-r.frameBytes {
				r.failTooLarge()
				return false
			}
			r.frameBytes += len(line)
			r.consumeField(line)
		}
	}

	frame, ok := r.dispatch()
	r.finished = true
	r.line = nil
	r.data = nil
	r.br = nil
	// Normal io.EOF ends the stream cleanly without reporting a reader error,
	// whereas transport failures stay in r.err even if a final JSON frame ships.
	if !errors.Is(readErr, io.EOF) {
		r.err = readErr
	}
	// Connection drops can arrive after a complete final JSON event before its
	// blank line; deliver valid JSON for usage/stop capture and drop truncated data.
	if !ok || (r.err != nil && !json.Valid(frame.Data)) {
		r.current = Frame{}
		return false
	}
	r.current = frame
	return true
}

// normalizeLine strips one optional trailing CR from lineWithoutLF and removes
// a single leading UTF-8 BOM on the stream's first physical line. Later lines
// leave any leading BOM bytes untouched in the field content.
func (r *Reader) normalizeLine(lineWithoutLF []byte) []byte {
	line := lineWithoutLF
	if len(line) > 0 && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if !r.started {
		r.started = true
		line = bytes.TrimPrefix(line, utf8BOM)
	}
	return line
}

// consumeField updates pending event or data state from one normalized nonblank
// line. Leading-colon comments, colon-free lines, and unrecognized SSE fields
// are ignored after their bytes have been counted toward the frame limit.
func (r *Reader) consumeField(line []byte) {
	if line[0] == ':' {
		return
	}
	field, content, hasColon := bytes.Cut(line, []byte(":"))
	if !hasColon {
		return
	}
	content = bytes.TrimPrefix(content, []byte(" "))
	switch string(field) {
	case "data":
		if r.hasData {
			r.data = append(r.data, '\n')
		}
		r.data = append(r.data, content...)
		r.hasData = true
	case "event":
		r.event = string(content)
	}
}

// dispatch emits a Frame with a cloned Data slice when accumulated data is
// nonempty, and returns false otherwise. Every call resets event, data, and
// hasData so an empty or comment-only frame never leaks its event label into
// the next frame.
func (r *Reader) dispatch() (Frame, bool) {
	var frame Frame
	ok := len(r.data) > 0
	if ok {
		frame = Frame{
			Event: r.event,
			Data:  bytes.Clone(r.data),
		}
	}
	r.event = ""
	if r.data != nil {
		r.data = r.data[:0]
	}
	r.hasData = false
	return frame, ok
}

// failTooLarge records ErrFrameTooLarge, marks the reader finished, and clears
// all buffered frame state and the underlying bufio.Reader reference. Later
// Scan calls return false immediately without reading more bytes.
func (r *Reader) failTooLarge() {
	r.err = ErrFrameTooLarge
	r.finished = true
	r.line = nil
	r.event = ""
	r.data = nil
	r.hasData = false
	r.frameBytes = 0
	r.current = Frame{}
	r.br = nil
}
