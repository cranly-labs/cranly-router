package sse

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// splitReader slices raw bytes into arbitrary caller-controlled Read chunks,
// including zero-byte reads and optional combined data-plus-EOF returns.
type splitReader struct {
	// data and offset track the unread suffix of the byte stream across Read
	// calls until offset reaches len(data) and signals io.EOF.
	data   []byte // complete input stream
	offset int    // next unread byte index

	// cuts, cutIdx, emptyRun, and inlineEOF control chunk boundaries, cap
	// consecutive zero-byte reads below bufio's progress limit, and toggle
	// whether the final byte chunk carries io.EOF immediately.
	cuts      []byte // fuzz-supplied step pattern
	cutIdx    int    // current step index in cuts
	emptyRun  int    // consecutive zero-byte reads emitted so far
	inlineEOF bool   // whether to return io.EOF alongside the final byte
}

// errTailReader serves all bytes from prefix and then fails with err, modeling
// an upstream connection drop mid-stream or right after an unterminated frame.
type errTailReader struct {
	// prefix, offset, err, reads, and inline deliver deterministic bytes before
	// surfacing err either alongside the final chunk or on the next Read, while
	// counting total Read calls after failure.
	prefix []byte // bytes delivered before the terminal error
	offset int    // next unread byte index in prefix
	err    error  // terminal non-EOF failure returned once prefix is exhausted
	reads  int    // total Read invocations observed
	inline bool   // whether to return err together with the final prefix chunk
}

// Read returns the next fuzz-sliced chunk of s.data and caps consecutive
// zero-byte reads at two so bufio.Reader does not report io.ErrNoProgress. When
// inlineEOF is set, the final byte chunk returns io.EOF in the same call.
func (s *splitReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if s.offset >= len(s.data) {
		return 0, io.EOF
	}

	step := 1
	if len(s.cuts) > 0 {
		op := s.cuts[s.cutIdx%len(s.cuts)]
		s.cutIdx++
		if op == 0 && s.emptyRun < 2 {
			s.emptyRun++
			return 0, nil
		}
		if op > 0 {
			step = int(op)
		}
	}
	s.emptyRun = 0

	remaining := len(s.data) - s.offset
	n := min(step, len(p), remaining)
	copy(p, s.data[s.offset:s.offset+n])
	s.offset += n
	if s.offset == len(s.data) && s.inlineEOF {
		return n, io.EOF
	}
	return n, nil
}

// Read copies unread bytes from r.prefix into p and increments r.reads on every
// invocation. Once r.prefix is exhausted, it returns r.err either inline with
// the final chunk or on the subsequent Read call.
func (r *errTailReader) Read(p []byte) (int, error) {
	r.reads++
	if r.offset >= len(r.prefix) {
		return 0, r.err
	}
	n := copy(p, r.prefix[r.offset:])
	r.offset += n
	if r.offset >= len(r.prefix) && r.inline {
		return n, r.err
	}
	return n, nil
}

// collectFrames drains rd until Scan returns false and returns all emitted
// frames together with rd.Err(). It also asserts that rd.Frame() resets to a
// zero Frame once scanning stops.
func collectFrames(t testing.TB, rd *Reader) ([]Frame, error) {
	t.Helper()
	var frames []Frame
	for rd.Scan() {
		frames = append(frames, rd.Frame())
	}
	require.Equal(t, Frame{}, rd.Frame())
	return frames, rd.Err()
}

// TestAppend formats single-line, multiline, empty, and CR/LF-tainted frames.
// It verifies canonical LF output, prefix preservation, zero-mutation of input
// slices, and exclusion of Semantic and Terminal flags from wire bytes.
func TestAppend(t *testing.T) {
	cases := []struct {
		name  string // scenario label
		frame Frame  // input frame to encode
		want  string // expected wire output
	}{
		{
			name:  "data only json",
			frame: Frame{Data: []byte(`{"id":"1"}`), Semantic: true, Terminal: false},
			want:  "data: {\"id\":\"1\"}\n\n",
		},
		{
			name:  "done sentinel",
			frame: Frame{Data: []byte("[DONE]"), Terminal: true},
			want:  "data: [DONE]\n\n",
		},
		{
			name:  "named event",
			frame: Frame{Event: "message_start", Data: []byte(`{"type":"message_start"}`)},
			want:  "event: message_start\ndata: {\"type\":\"message_start\"}\n\n",
		},
		{
			name:  "empty data nil and nonnil",
			frame: Frame{Event: "ping", Data: nil},
			want:  "event: ping\ndata: \n\n",
		},
		{
			name:  "multiline lf crlf and bare cr",
			frame: Frame{Event: "delta", Data: []byte("line1\nline2\r\nline3\rline4")},
			want:  "event: delta\ndata: line1\ndata: line2\ndata: line3\ndata: line4\n\n",
		},
		{
			name:  "leading and trailing line breaks",
			frame: Frame{Data: []byte("\nmiddle\r\n")},
			want:  "data: \ndata: middle\ndata: \n\n",
		},
		{
			name:  "single newline payload",
			frame: Frame{Data: []byte("\n")},
			want:  "data: \ndata: \n\n",
		},
		{
			name:  "event header strips cr and lf injection",
			frame: Frame{Event: "message_delta\r\ndata: injected\n", Data: []byte(`{"ok":true}`)},
			want:  "event: message_deltadata: injected\ndata: {\"ok\":true}\n\n",
		},
		{
			name:  "event header with only crlf is omitted",
			frame: Frame{Event: "\r\n\n", Data: []byte(`{"ok":true}`)},
			want:  "data: {\"ok\":true}\n\n",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			origData := bytes.Clone(tc.frame.Data)
			prefix := []byte("prefix:")
			got := Append(bytes.Clone(prefix), tc.frame)
			require.Equal(t, "prefix:"+tc.want, string(got))
			require.Equal(t, origData, tc.frame.Data)
		})
	}

	// Verify spare destination capacity writes single-line frames in place
	// without allocating a new backing array.
	buf := make([]byte, 0, 128)
	frame := Frame{Event: "message_stop", Data: []byte(`{"type":"message_stop"}`)}
	allocs := testing.AllocsPerRun(100, func() {
		out := Append(buf[:0], frame)
		if len(out) == 0 {
			t.Fatal("empty output")
		}
	})
	require.Zero(t, allocs)
}

// TestReaderQuirksAndFields feeds a mixed-quirk SSE stream containing BOM,
// comments, colon-spacing variants, multiline data, and CRLF/LF delimiters. It
// verifies that empty or comment frames are skipped without leaking event
// labels and that the final frame at EOF ships with independent Data slices.
func TestReaderQuirksAndFields(t *testing.T) {
	stream := "\xef\xbb\xbf" +
		": leading comment\r\n" +
		"id: ignored-id\n" +
		"retry: 1500\r\n" +
		"event: orphan_before_blank\n" +
		"\r\n" +
		"data: {\"only\":\"data\"}\n\n" +
		"event:first_label\r\n" +
		"event: message_start\n" +
		" data: ignored-leading-space-field\n" +
		"unrecognized line without colon\r\n" +
		"data:{\r\n" +
		": comment inside multiline frame\n" +
		"data:   \"a\": 1\r\n" +
		"data: }\r\n\n" +
		"event: will_be_cleared\n" +
		"event:\n" +
		"data:\n" +
		"data: second_line\n" +
		"data:\n\n" +
		"data:\n\n" +
		"data: \r\n\r\n" +
		"data: \xef\xbb\xbf[DONE]"

	rd := NewReader(strings.NewReader(stream))
	require.Equal(t, Frame{}, rd.Frame())

	frames, err := collectFrames(t, rd)
	require.NoError(t, err)
	require.Equal(t, []Frame{
		{Event: "", Data: []byte(`{"only":"data"}`)},
		{Event: "message_start", Data: []byte("{\n  \"a\": 1\n}")},
		{Event: "", Data: []byte("\nsecond_line\n")},
		{Event: "", Data: []byte("\xef\xbb\xbf[DONE]")},
	}, frames)

	// Mutating an earlier Frame.Data slice must not affect neighboring frames.
	frames[0].Data[0] = 'X'
	require.Equal(t, byte('{'), frames[1].Data[0])
}

// TestReaderNilZeroAndExactBufferEOF exercises nil and zero-value readers
// alongside unterminated EOF lines matching readerBufferSize or ending in CR.
// Empty readers must report clean exhaustion, and exact-buffer or CR-suffixed
// final frames must arrive intact.
func TestReaderNilZeroAndExactBufferEOF(t *testing.T) {
	var zero Reader
	require.False(t, zero.Scan())
	require.NoError(t, zero.Err())
	require.Equal(t, Frame{}, zero.Frame())

	nilReader := NewReader(nil)
	require.False(t, nilReader.Scan())
	require.NoError(t, nilReader.Err())
	require.Equal(t, Frame{}, nilReader.Frame())

	// Build a single unterminated line whose total length equals readerBufferSize
	// so ReadSlice hits ErrBufferFull followed by an empty EOF read.
	prefix := "data: "
	payload := strings.Repeat("a", readerBufferSize-len(prefix))
	frames, err := collectFrames(t, NewReaderLimit(strings.NewReader(prefix+payload), 0))
	require.NoError(t, err)
	require.Equal(t, []Frame{{Data: []byte(payload)}}, frames)

	// Also check a trailing CR on an unterminated final frame at EOF.
	frames, err = collectFrames(t, NewReader(strings.NewReader("data: {\"done\":true}\r")))
	require.NoError(t, err)
	require.Equal(t, []Frame{{Data: []byte(`{"done":true}`)}}, frames)
}

// TestReaderReadError reproduces connection drops after complete and truncated
// final frames without blank delimiters. Complete JSON must still be delivered
// while Err retains the read error, and truncated data must be discarded.
func TestReaderReadError(t *testing.T) {
	readErr := errors.New("upstream connection reset")
	firstFrame := "data: {\"type\":\"message_start\"}\n\n"

	for _, inline := range []bool{false, true} {
		for _, tc := range []struct {
			name        string // subcase label
			tail        string // bytes following firstFrame before readErr
			wantDeliver bool   // whether tail should emit a second frame
			wantData    string // expected data of the second frame when delivered
		}{
			{
				name:        "complete json without newline",
				tail:        `data: {"type":"message_delta","usage":{"output_tokens":25}}`,
				wantDeliver: true,
				wantData:    `{"type":"message_delta","usage":{"output_tokens":25}}`,
			},
			{
				name:        "complete json with single lf",
				tail:        "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":25}}\n",
				wantDeliver: true,
				wantData:    `{"type":"message_delta","usage":{"output_tokens":25}}`,
			},
			{
				name:        "complete json followed by interrupted crlf",
				tail:        "data: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":25}}\n\r",
				wantDeliver: true,
				wantData:    `{"type":"message_delta","usage":{"output_tokens":25}}`,
			},
			{
				name:        "truncated json without newline",
				tail:        `data: {"type":"message_delta","usage":{"output_tokens":`,
				wantDeliver: false,
			},
			{
				name:        "truncated json followed by interrupted crlf",
				tail:        "data: {\"type\":\"message_delta\"\n\r",
				wantDeliver: false,
			},
			{
				name:        "non json sentinel without blank line",
				tail:        "data: [DON",
				wantDeliver: false,
			},
		} {
			mode := "separate_err"
			if inline {
				mode = "inline_err"
			}
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				src := &errTailReader{
					prefix: []byte(firstFrame + tc.tail),
					err:    readErr,
					inline: inline,
				}
				rd := NewReader(src)

				// The completed first frame arrives before the read failure surfaces.
				require.True(t, rd.Scan())
				require.Equal(t, Frame{Data: []byte(`{"type":"message_start"}`)}, rd.Frame())
				require.NoError(t, rd.Err())

				require.Equal(t, tc.wantDeliver, rd.Scan())
				if tc.wantDeliver {
					require.Equal(t, Frame{Data: []byte(tc.wantData)}, rd.Frame())
				} else {
					require.Equal(t, Frame{}, rd.Frame())
				}
				require.ErrorIs(t, rd.Err(), readErr)

				readsAfterFailure := src.reads
				require.False(t, rd.Scan())
				require.Equal(t, Frame{}, rd.Frame())
				require.ErrorIs(t, rd.Err(), readErr)
				require.Equal(t, readsAfterFailure, src.reads)
			})
		}
	}
}

// TestReaderFrameLimit feeds oversized multi-line and single-line frames plus
// back-to-back exact-budget frames separated by LF and split CRLF delimiters.
// Overflow must return ErrFrameTooLarge and stop reading, while blank lines
// reset the per-frame budget for subsequent frames.
func TestReaderFrameLimit(t *testing.T) {
	for _, line := range []string{"data: x", ": comment", "id: 123", "event: msg"} {
		t.Run("accumulated/"+line, func(t *testing.T) {
			raw := strings.Repeat(line+"\n", 5)
			src := &errTailReader{prefix: []byte(raw), err: io.EOF}
			rd := NewReaderLimit(src, 2*(len(line)+1))

			require.False(t, rd.Scan())
			require.ErrorIs(t, rd.Err(), ErrFrameTooLarge)
			require.Equal(t, Frame{}, rd.Frame())

			readsAfterOverflow := src.reads
			require.False(t, rd.Scan())
			require.Equal(t, readsAfterOverflow, src.reads)
		})
	}

	// Two exact-budget frames separated by LF or split CRLF blank delimiters must
	// both succeed because each blank line resets the per-frame byte counter.
	exactLimit := len("data: {}\n")
	for _, body := range []string{
		"data: {}\n\ndata: {}\n\n",
		"data: {}\r\n\r\ndata: {}\r\n\r\n",
	} {
		rd := NewReaderLimit(&splitReader{data: []byte(body), cuts: []byte{1}}, exactLimit)
		frames, err := collectFrames(t, rd)
		require.NoError(t, err)
		require.Equal(t, []Frame{
			{Data: []byte("{}")},
			{Data: []byte("{}")},
		}, frames)
	}

	// An exact-budget unterminated final frame at EOF fits when maxFrameBytes
	// equals its raw length and fails when one byte smaller.
	frames, err := collectFrames(t, NewReaderLimit(strings.NewReader("data: {}"), len("data: {}")))
	require.NoError(t, err)
	require.Equal(t, []Frame{{Data: []byte("{}")}}, frames)

	frames, err = collectFrames(t, NewReaderLimit(strings.NewReader("data: {}"), len("data: {}")-1))
	require.ErrorIs(t, err, ErrFrameTooLarge)
	require.Empty(t, frames)

	// A single newline-free line exceeding a multi-buffer limit must abort early
	// without reading the entire remaining stream, and ErrFrameTooLarge must take
	// precedence over a trailing transport error.
	oversized := "data: {\"ok\":1}\n\n" + "data: " + strings.Repeat("x", 3*readerBufferSize)
	src := &errTailReader{prefix: []byte(oversized), err: io.ErrUnexpectedEOF}
	rd := NewReaderLimit(src, readerBufferSize+512)
	require.True(t, rd.Scan())
	require.Equal(t, Frame{Data: []byte(`{"ok":1}`)}, rd.Frame())
	require.False(t, rd.Scan())
	require.ErrorIs(t, rd.Err(), ErrFrameTooLarge)
	require.Less(t, src.offset, len(oversized))
}

// FuzzReaderArbitrarySplits feeds arbitrary SSE byte streams and cut schedules
// through Reader and Append. It checks that chunk boundaries never cause panics
// or alter parsed frames or ErrFrameTooLarge outcomes, and that valid frames
// round-trip through Append under the same split schedule.
func FuzzReaderArbitrarySplits(f *testing.F) {
	f.Add([]byte(""), []byte{1}, uint16(0))
	f.Add([]byte(": keepalive\n\n"), []byte{1, 2, 0}, uint16(64))
	f.Add([]byte("data: {\"delta\":\"你好🎉\"}\n\ndata: [DONE]\n\n"), []byte{1, 0, 2, 3, 5}, uint16(0))
	f.Add([]byte("\xef\xbb\xbfevent: message_start\r\ndata: {\"a\":1}\ndata: {\"b\":2}\r\n\n"), []byte{1, 1, 2}, uint16(0))
	f.Add([]byte("data:\ndata: second\r\ndata:\r\n\r\ndata: tail_without_blank"), []byte{1, 3, 0, 4}, uint16(0))
	f.Add([]byte("event: drop\n\ndata: "+strings.Repeat("x", readerBufferSize+17)+"\n\n"), []byte{7, 13, 64, 255}, uint16(0))
	f.Add([]byte("data: ok\n\ndata: "+strings.Repeat("y", 120)+"\n\n"), []byte{1, 2, 4, 8}, uint16(32))

	f.Fuzz(func(t *testing.T, raw []byte, cuts []byte, limitSeed uint16) {
		limit := DefaultMaxFrameBytes
		if limitSeed > 0 {
			limit = int(limitSeed)
		}

		wantFrames, wantErr := collectFrames(t, NewReaderLimit(bytes.NewReader(raw), limit))

		for _, inlineEOF := range []bool{false, true} {
			sr := &splitReader{
				data:      raw,
				cuts:      cuts,
				inlineEOF: inlineEOF,
			}
			gotFrames, gotErr := collectFrames(t, NewReaderLimit(sr, limit))
			require.Equal(t, wantFrames, gotFrames)
			require.Equal(t, errors.Is(wantErr, ErrFrameTooLarge), errors.Is(gotErr, ErrFrameTooLarge))
			require.Equal(t, wantErr == nil, gotErr == nil)
		}

		if wantErr != nil || len(wantFrames) == 0 {
			return
		}

		// Append strips CR/LF from Event and normalizes CRLF or bare CR in Data into
		// canonical LF line breaks; CR-free frames round-trip unchanged.
		var encoded []byte
		expectedRoundTrip := make([]Frame, len(wantFrames))
		for i, frame := range wantFrames {
			encoded = Append(encoded, frame)
			_, cleanEvent := sanitizeEvent(frame.Event)
			cleanData := bytes.ReplaceAll(frame.Data, []byte("\r\n"), []byte("\n"))
			cleanData = bytes.ReplaceAll(cleanData, []byte("\r"), []byte("\n"))
			expectedRoundTrip[i] = Frame{
				Event: cleanEvent,
				Data:  cleanData,
			}
		}
		roundTripReader := &splitReader{
			data:      encoded,
			cuts:      cuts,
			inlineEOF: len(cuts)%2 == 1,
		}
		roundTripFrames, roundTripErr := collectFrames(t, NewReader(roundTripReader))
		require.NoError(t, roundTripErr)
		require.Equal(t, expectedRoundTrip, roundTripFrames)
	})
}
