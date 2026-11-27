package upstream

import (
	"bufio"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
)

// countingReadCloser wraps an io.Reader and counts idempotent Close invocations.
type countingReadCloser struct {
	io.Reader              // underlying stream reader
	closed    atomic.Int32 // number of Close calls observed
}

// Close increments the close counter and closes the underlying reader when it
// implements io.Closer.
func (c *countingReadCloser) Close() error {
	c.closed.Add(1)
	if closer, ok := c.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// TestNewResponsesToolStreamBody_FlushesLiveFrameBeforeEOF writes one complete
// SSE frame to an open upstream pipe before closing the writer. It verifies
// that NewResponsesToolStreamBody flushes the restored frame to downstream
// readers without waiting for upstream EOF.
func TestNewResponsesToolStreamBody_FlushesLiveFrameBeforeEOF(t *testing.T) {
	sourceReader, sourceWriter := io.Pipe()
	body := NewResponsesToolStreamBody(sourceReader, &convert.ToolMap{
		CustomTools: map[string]bool{"apply_patch": true},
	}, DefaultSSELineLimit)
	defer func() { _ = body.Close() }()
	defer func() { _ = sourceWriter.Close() }()

	// readResult captures the first complete SSE frame or a stream read error.
	type readResult struct {
		frame string // delivered SSE frame text
		err   error  // read error if any
	}
	read := make(chan readResult, 1)
	go func() {
		reader := bufio.NewReader(body)
		var frame strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				read <- readResult{err: err}
				return
			}
			_, _ = frame.WriteString(line)
			if strings.TrimSpace(line) == "" {
				read <- readResult{frame: frame.String()}
				return
			}
		}
	}()

	firstFrame := "event: response.created\n" +
		`data: {"type":"response.created","sequence_number":0,"response":{"id":"flush-ir-before-eof"}}` + "\n\n"
	_, err := sourceWriter.Write([]byte(firstFrame))
	require.NoError(t, err)

	select {
	case result := <-read:
		require.NoError(t, result.err)
		require.Contains(t, result.frame, "flush-ir-before-eof")
		require.Contains(t, result.frame, "event: response.created")
	case <-time.After(3 * time.Second):
		t.Fatal("first transformed IR SSE frame did not flush while upstream connection remained open")
	}
}

// TestNewResponsesToolStreamBody_RestoresToolsHeartbeatsMultilineAndHostedEvents
// streams custom, tool_search, and namespaced function calls interleaved with
// keepalive comments, unnumbered ping events, multiline JSON carrying >53-bit
// integers, hosted web_search intermediate events, and [DONE]. All client tool
// identities must restore with contiguous sequence_number values and intact precision.
func TestNewResponsesToolStreamBody_RestoresToolsHeartbeatsMultilineAndHostedEvents(t *testing.T) {
	tm := &convert.ToolMap{
		CustomTools: map[string]bool{"code_exec": true},
		CustomNames: map[string]convert.NamespacedName{
			"code_exec": {Name: "code.exec"},
		},
		ToolSearch: true,
		NamespaceTools: map[string]convert.NamespacedName{
			"git__status": {Namespace: "git", Name: "status"},
		},
	}

	rawStream := strings.Join([]string{
		": keepalive comment before stream start",
		"event: ping",
		"",
		"id: evt-0",
		"event: response.created",
		"data: {",
		`data:   "type": "response.created",`,
		`data:   "sequence_number": 0,`,
		`data:   "big_trace_id": 9007199254740993,`,
		`data:   "response": {"id": "resp_1", "status": "in_progress", "output": []}`,
		"data: }",
		"",
		"event: ping",
		`data: {"type":"ping"}`,
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"in_progress"}}`,
		"",
		"event: response.web_search_call.searching",
		`data: {"type":"response.web_search_call.searching","sequence_number":2,"output_index":0,"item_id":"ws_1"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":3,"output_index":0,"item":{"id":"ws_1","type":"web_search_call","status":"completed"}}`,
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":4,"output_index":1,"item":{"id":"fc_exec","type":"function_call","call_id":"call_exec","name":"code_exec","arguments":""}}`,
		"",
		"event: response.function_call_arguments.delta",
		`data: {"type":"response.function_call_arguments.delta","sequence_number":5,"output_index":1,"item_id":"fc_exec","delta":"{\"input\":\"echo "}`,
		"",
		"event: response.function_call_arguments.delta",
		`data: {"type":"response.function_call_arguments.delta","sequence_number":6,"output_index":1,"item_id":"fc_exec","delta":"ok\"}"}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":7,"output_index":1,"item_id":"fc_exec","arguments":"{\"input\":\"echo ok\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":8,"output_index":1,"item":{"id":"fc_exec","type":"function_call","call_id":"call_exec","name":"code_exec","arguments":"{\"input\":\"echo ok\"}"}}`,
		"",
		"event: response.output_item.added",
		`data: {"type":"response.output_item.added","sequence_number":9,"output_index":2,"item":{"id":"fc_search","type":"function_call","call_id":"call_s1","name":"tool_search","arguments":""}}`,
		"",
		"event: response.function_call_arguments.done",
		`data: {"type":"response.function_call_arguments.done","sequence_number":10,"output_index":2,"item_id":"fc_search","arguments":"{\"query\":\"git\"}"}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":11,"output_index":2,"item":{"id":"fc_search","type":"function_call","call_id":"call_s1","name":"tool_search","arguments":"{\"query\":\"git\"}"}}`,
		"",
		"event: response.output_item.done",
		`data: {"type":"response.output_item.done","sequence_number":12,"output_index":3,"item":{"id":"fc_git","type":"function_call","call_id":"call_g1","name":"git__status","arguments":"{}"}}`,
		"",
		"event: response.completed",
		`data: {"type":"response.completed","sequence_number":13,"big_trace_id":9007199254740993,"response":{"id":"resp_1","status":"completed","output":[{"id":"ws_1","type":"web_search_call","status":"completed"},{"id":"fc_exec","type":"function_call","call_id":"call_exec","name":"code_exec","arguments":"{\"input\":\"echo ok\"}"},{"id":"fc_search","type":"function_call","call_id":"call_s1","name":"tool_search","arguments":"{\"query\":\"git\"}"},{"id":"fc_git","type":"function_call","call_id":"call_g1","name":"git__status","arguments":"{}"}]}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")

	src := &countingReadCloser{Reader: strings.NewReader(rawStream)}
	body := NewResponsesToolStreamBody(src, tm, DefaultSSELineLimit)
	outBytes, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.GreaterOrEqual(t, src.closed.Load(), int32(1))

	out := string(outBytes)
	require.NotContains(t, out, "keepalive comment")
	require.NotContains(t, out, "event: ping\n\n")
	require.Contains(t, out, "id: evt-0\nevent: response.created\ndata: {")
	require.Contains(t, out, `"big_trace_id":9007199254740993`)
	require.Contains(t, out, "event: response.web_search_call.searching")
	require.Contains(t, out, "event: response.custom_tool_call_input.delta")
	require.Contains(t, out, "event: response.custom_tool_call_input.done")
	require.Contains(t, out, `"id":"ctc_exec"`)
	require.Contains(t, out, `"name":"code.exec"`)
	require.Contains(t, out, `"input":"echo ok"`)
	require.Contains(t, out, `"id":"tsc_search"`)
	require.Contains(t, out, `"type":"tool_search_call"`)
	require.Contains(t, out, `"namespace":"git"`)
	require.Contains(t, out, `"name":"status"`)
	// Sequence numbers: 0 (created), 1..3 (web_search), 4 (custom added),
	// 5 (custom delta), 6 (custom done), 7 (custom item done), 8 (search added),
	// 9 (search item done), 10 (git item done), 11 (completed).
	require.Contains(t, out, `"type":"response.completed","sequence_number":11`)
	require.Contains(t, out, "data: [DONE]\n\n")
}

// TestNewResponsesToolStreamBody_ErrorPropagationAndEarlyClose feeds oversized
// multiline payloads, duplicate-key JSON objects, bare error events, and
// unconsumed large chunks into NewResponsesToolStreamBody. It verifies
// bufio.ErrTooLong reporting, restoration error propagation, control-frame
// pass-through, and early reader-close wakeup of blocked upstream writers.
func TestNewResponsesToolStreamBody_ErrorPropagationAndEarlyClose(t *testing.T) {
	t.Run("multiline overflow reports bufio.ErrTooLong", func(t *testing.T) {
		tm := &convert.ToolMap{CustomTools: map[string]bool{"exec": true}}
		raw := "data: {\ndata: \"" + strings.Repeat("x", 128) + "\": 1\ndata: }\n\n"
		body := NewResponsesToolStreamBody(io.NopCloser(strings.NewReader(raw)), tm, 64)
		defer func() { _ = body.Close() }()
		_, err := io.ReadAll(body)
		require.ErrorIs(t, err, bufio.ErrTooLong)
	})

	t.Run("duplicate key inside JSON event surfaces restoration error", func(t *testing.T) {
		tm := &convert.ToolMap{CustomTools: map[string]bool{"exec": true}}
		raw := "data: {\"type\":\"response.created\",\"type\":\"response.completed\"}\n\n"
		body := NewResponsesToolStreamBody(io.NopCloser(strings.NewReader(raw)), tm, DefaultSSELineLimit)
		defer func() { _ = body.Close() }()
		_, err := io.ReadAll(body)
		require.Error(t, err)
		require.Contains(t, err.Error(), "restore Responses client tool event")
	})

	t.Run("bare event error metadata passes through", func(t *testing.T) {
		tm := &convert.ToolMap{CustomTools: map[string]bool{"exec": true}}
		raw := "event: error\n\n"
		body := NewResponsesToolStreamBody(io.NopCloser(strings.NewReader(raw)), tm, DefaultSSELineLimit)
		defer func() { _ = body.Close() }()
		got, err := io.ReadAll(body)
		require.NoError(t, err)
		require.Equal(t, "event: error\n\n", string(got))
	})

	t.Run("early reader Close wakes blocked worker and closes source", func(t *testing.T) {
		srcReader, srcWriter := io.Pipe()
		tracked := &countingReadCloser{Reader: srcReader}
		body := NewResponsesToolStreamBody(tracked, &convert.ToolMap{
			CustomTools: map[string]bool{"exec": true},
		}, DefaultSSELineLimit)

		done := make(chan error, 1)
		go func() {
			// Write more than the buffered writer's 4 KiB window so the worker
			// blocks on the unconsumed PipeWriter until body.Close runs.
			chunk := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"" + strings.Repeat("a", 8192) + "\"}\n\n"
			_, err := srcWriter.Write([]byte(chunk))
			done <- err
		}()

		require.NoError(t, body.Close())
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("closing transformed body did not unblock upstream writer")
		}
		_ = srcWriter.Close()
		require.GreaterOrEqual(t, tracked.closed.Load(), int32(1))
	})
}
