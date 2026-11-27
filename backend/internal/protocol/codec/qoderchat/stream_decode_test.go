package qoderchat_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/qoderchat"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestStreamDecodeParallelCallsPlaceholdersEnvelopesAndUsage feeds Anthropic
// envelopes, replaceable "{}" placeholders, drifted slot indices, huge sparse
// indices, and cache-hit usage frames into StreamDecoder and DecodeResponse. It
// verifies sequential IR stream validity, disjoint cache-read usage buckets, and
// parity between streaming accumulation and buffered decoding.
func TestStreamDecodeParallelCallsPlaceholdersEnvelopesAndUsage(t *testing.T) {
	require.Equal(t, `{"cmd":"pwd"}`, qoderchat.MergeToolArguments(`{}`, `{"cmd":"pwd"}`))
	require.Equal(t, `{"cmd":"pwd"}`, qoderchat.MergeToolArguments(`{"cmd":"pwd"}`, `{}`))
	require.Equal(t, `{"cmd":"pwd"}`, qoderchat.MergeToolArguments(`{"cmd":`, `"pwd"}`))

	inners := []string{
		`{"event":"content_block_delta","data":{"type":"thinking_delta","text":"plan first"}}`,
		`{"event":"content_block_delta","data":{"type":"text_delta","text":"Running checks"}}`,
		`{"choices":[{"delta":{"tool_calls":[{"type":"function"}]}}]}`,
		`{"event":"tool_use_start","index":0,"data":{"id":"call_drift","name":"Read","type":"tool_use"}}`,
		`{"event":"tool_use_delta","index":0,"data":{"id":"call_drift","arguments":"{}"}}`,
		`{"event":"tool_use_delta","index":5,"data":{"id":"call_drift","arguments":"{\"path\":"}}`,
		`{"event":"tool_use_delta","index":9,"data":{"id":"call_drift","arguments":"\"go.mod\"}"}}`,
		`{"choices":[{"delta":{"tool_calls":[{"index":2000000000,"name":"Bash","arguments":"{\"cmd\":\"pwd\"}"}]}}]}`,
		`{"choices":[{"delta":{"tool_calls":[{"arguments":"{\"ignored\":\"ambiguous\"}"}]}}]}`,
		`{"usage":{"prompt_tokens":66637,"completion_tokens":"6","prompt_tokens_details":{"cached_tokens":66612,"cacheable_tokens":19},"completion_tokens_details":{"reasoning_tokens":2}}}`,
		`{"event":"message_delta","data":{"usage":{"output_tokens":8}}}`,
		`{"event":"message_stop"}`,
	}

	var rawSSE bytes.Buffer
	_, _ = rawSSE.WriteString("data: \n\n")
	for _, inner := range inners {
		fmt.Fprintf(&rawSSE, "data: {\"body\":%q}\n\n", inner)
	}

	dec := qoderchat.NewStreamDecoder()
	reader := sse.NewReader(bytes.NewReader(rawSSE.Bytes()))
	var events []ir.Event
	for reader.Scan() {
		batch, err := dec.Decode(reader.Frame())
		require.NoError(t, err)
		events = append(events, batch...)
	}
	require.NoError(t, reader.Err())
	events = append(events, dec.Close(nil)...)
	require.NoError(t, ir.ValidateStream(events))

	var acc ir.Accumulator
	acc.Add(events...)
	resp := acc.Response()
	require.NoError(t, ir.ValidateResponse(resp))
	require.Equal(t, ir.FinishToolCalls, resp.Finish.Reason)
	require.Len(t, resp.Parts, 4)

	require.Equal(t, ir.PartReasoning, resp.Parts[0].Kind)
	require.Equal(t, []string{"plan first"}, resp.Parts[0].Reasoning.Segments)
	require.Equal(t, ir.PartText, resp.Parts[1].Kind)
	require.Equal(t, "Running checks", resp.Parts[1].Text)
	require.Equal(t, ir.PartToolCall, resp.Parts[2].Kind)
	require.Equal(t, "call_drift", resp.Parts[2].Call.CallID)
	require.Equal(t, "Read", resp.Parts[2].Call.Tool.Name)
	require.JSONEq(t, `{"path":"go.mod"}`, resp.Parts[2].Call.Arguments)
	require.Equal(t, ir.PartToolCall, resp.Parts[3].Kind)
	require.Equal(t, "Bash", resp.Parts[3].Call.Tool.Name)
	require.JSONEq(t, `{"command":"pwd"}`, resp.Parts[3].Call.Arguments)

	require.NotNil(t, resp.Usage)
	require.Equal(t, int64(25), resp.Usage.Input)
	require.Equal(t, int64(66612), resp.Usage.CacheRead)
	require.Equal(t, int64(0), resp.Usage.CacheWrite)
	require.Equal(t, int64(8), resp.Usage.Output)
	require.Equal(t, int64(2), resp.Usage.Reasoning)

	bufferedResp, err := qoderchat.DecodeResponse(rawSSE.Bytes())
	require.NoError(t, err)
	require.Equal(t, ir.ResponseEssence(resp), ir.ResponseEssence(bufferedResp))
	require.Equal(t, resp.Usage, bufferedResp.Usage)
}

// TestStreamDecodeWrappedAPIErrorsAndUsageValidation feeds wrapped 4xx/5xx SSE
// and buffered error envelopes, non-usage data sub-objects, and spoofed usage
// fields into StreamDecoder and DecodeResponse. It verifies that failed wrappers
// return redacted *qoder.APIError values even without body fields while usage
// validation runs selectively on usage-bearing frames.
func TestStreamDecodeWrappedAPIErrorsAndUsageValidation(t *testing.T) {
	dec := qoderchat.NewStreamDecoder()
	_, err := dec.Decode(sse.Frame{
		Data: []byte(`{"statusCode":"TOO_MANY_REQUESTS","body":"{\"code\":115,\"message\":\"Authorization: Bearer tok_secret\",\"agentLimitResetTime\":1764045000000}"}`),
	})
	require.Error(t, err)
	apiErr, ok := qoder.AsAPIError(err)
	require.True(t, ok)
	require.Equal(t, 429, apiErr.StatusCode)
	require.True(t, apiErr.IsAgentLimit())
	require.NotContains(t, apiErr.Message, "tok_secret")

	tail := dec.Close(err)
	require.NoError(t, ir.ValidateStream(tail))

	decSpoof := qoderchat.NewStreamDecoder()
	_, err = decSpoof.Decode(sse.Frame{
		Data: []byte(`{"body":"{\"usage\":{\"prompt_tokens\":1,\"Prompt_Tokens\":999}}"}`),
	})
	require.Error(t, err)
	require.NoError(t, ir.ValidateStream(decSpoof.Close(err)))

	decMsgDeltaSpoof := qoderchat.NewStreamDecoder()
	_, err = decMsgDeltaSpoof.Decode(sse.Frame{
		Data: []byte(`{"body":"{\"event\":\"message_delta\",\"data\":{\"usage\":{\"input_tokens\":1,\"Input_Tokens\":2}}}"}`),
	})
	require.Error(t, err)
	require.NoError(t, ir.ValidateStream(decMsgDeltaSpoof.Close(err)))

	decNonUsageData := qoderchat.NewStreamDecoder()
	evs, err := decNonUsageData.Decode(sse.Frame{
		Data: []byte(`{"body":"{\"event\":\"content_block_delta\",\"data\":{\"type\":\"text_delta\",\"text\":\"ok\",\"InputTokens\":7}}"}`),
	})
	require.NoError(t, err)
	doneEvs, err := decNonUsageData.Decode(sse.Frame{Data: []byte(`{"body":"[DONE]"}`)})
	require.NoError(t, err)
	evs = append(evs, doneEvs...)
	evs = append(evs, decNonUsageData.Close(nil)...)
	require.NoError(t, ir.ValidateStream(evs))

	for _, rawErrBody := range [][]byte{
		[]byte(`{"statusCodeValue":403,"statusCode":"FORBIDDEN"}`),
		[]byte(`{"statusCodeValue":500,"body":null}`),
		[]byte(`{"statusCode":"BAD_GATEWAY"}`),
	} {
		_, bErr := qoderchat.DecodeResponse(rawErrBody)
		require.Error(t, bErr)
		gotAPI, isAPI := qoder.AsAPIError(bErr)
		require.True(t, isAPI, "expected *qoder.APIError for %s, got %v", rawErrBody, bErr)
		require.GreaterOrEqual(t, gotAPI.StatusCode, 400)
	}

	for _, rawEmptyOK := range [][]byte{
		[]byte(`{"statusCodeValue":200,"body":null}`),
		[]byte(`{"statusCodeValue":200,"body":""}`),
		[]byte(`{"statusCodeValue":200}`),
	} {
		okResp, okErr := qoderchat.DecodeResponse(rawEmptyOK)
		require.NoError(t, okErr)
		require.NoError(t, ir.ValidateResponse(okResp))
		require.Equal(t, ir.FinishStop, okResp.Finish.Reason)
		require.Empty(t, okResp.Parts)
	}
}

// TestStreamDecodeTruncatedStreamsAndMalformedToolArguments feeds prematurely
// closed SSE streams, mid-call transport errors, and truncated JSON tool
// arguments into StreamDecoder and DecodeResponse. It verifies that every
// abnormal stream ends with a valid EventError and fails buffered decoding.
func TestStreamDecodeTruncatedStreamsAndMalformedToolArguments(t *testing.T) {
	t.Run("silent EOF before DONE", func(t *testing.T) {
		dec := qoderchat.NewStreamDecoder()
		evs, err := dec.Decode(sse.Frame{
			Data: []byte(`{"body":"{\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}"}`),
		})
		require.NoError(t, err)
		evs = append(evs, dec.Close(nil)...)
		require.NoError(t, ir.ValidateStream(evs))
		require.Equal(t, ir.EventError, evs[len(evs)-1].Kind)
	})

	t.Run("transport error mid tool call", func(t *testing.T) {
		dec := qoderchat.NewStreamDecoder()
		evs, err := dec.Decode(sse.Frame{
			Data: []byte(`{"body":"{\"choices\":[{\"delta\":{\"tool_calls\":[{\"id\":\"call_1\",\"function\":{\"name\":\"Read\",\"arguments\":\"{\\\"path\\\":\"}}]}}]}"}`),
		})
		require.NoError(t, err)
		evs = append(evs, dec.Close(io.ErrUnexpectedEOF)...)
		require.NoError(t, ir.ValidateStream(evs))
		require.Equal(t, ir.EventError, evs[len(evs)-1].Kind)
	})

	t.Run("malformed tool JSON with DONE", func(t *testing.T) {
		raw := "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"tool_calls\\\":[{\\\"id\\\":\\\"call_1\\\",\\\"function\\\":{\\\"name\\\":\\\"bash\\\",\\\"arguments\\\":\\\"{\\\\\\\"cmd\\\\\\\":\\\"}}]}}]}\"}\n\n" +
			"data: {\"body\":\"[DONE]\"}\n\n"
		_, err := qoderchat.DecodeResponse([]byte(raw))
		require.Error(t, err)
		require.Contains(t, err.Error(), "malformed qoder tool arguments")
	})

	t.Run("single wrapped JSON object DecodeResponse", func(t *testing.T) {
		single := []byte(`{"statusCodeValue":200,"body":"{\"choices\":[{\"message\":{\"content\":[{\"type\":\"text\",\"text\":\"line1\"},{\"type\":\"text\",\"text\":\"line2\"}]},\"finish_reason\":\"stop\"}],\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}"}`)
		resp, err := qoderchat.DecodeResponse(single)
		require.NoError(t, err)
		require.Equal(t, ir.FinishStop, resp.Finish.Reason)
		require.Len(t, resp.Parts, 1)
		require.Equal(t, "line1\nline2", resp.Parts[0].Text)
		require.Equal(t, int64(5), resp.Usage.Input)
		require.Equal(t, int64(2), resp.Usage.Output)
	})
}

// FuzzStreamDecode feeds arbitrary SSE byte sequences into sse.Reader and
// qoderchat.StreamDecoder across normal and interrupted termination paths. It
// verifies zero panics alongside full ir.ValidateStream and ir.ValidateResponse
// compliance for every emitted event sequence.
func FuzzStreamDecode(f *testing.F) {
	seeds := [][]byte{
		[]byte("data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"hi\\\"}}]}\"}\n\ndata: {\"body\":\"[DONE]\"}\n\n"),
		[]byte("data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"reasoning_content\\\":\\\"think\\\",\\\"content\\\":\\\"<tool_call>Read<arg_key>path</arg_key><arg_value>a&amp;b</arg_value></tool_call>\\\"}}],\\\"usage\\\":{\\\"prompt_tokens\\\":10,\\\"completion_tokens\\\":4,\\\"prompt_tokens_details\\\":{\\\"cached_tokens\\\":6,\\\"cacheable_tokens\\\":2}}}\"}\n\ndata: [DONE]\n\n"),
		[]byte("data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"<tool_call>{\\\\\\\"name\\\\\\\":\\\\\\\"shell\\\\\\\",\\\\\\\"cmd\\\\\\\":\\\\\\\"pwd\\\\\\\"}</tool_call>\\\"}}]}\"}\n\ndata: {\"body\":\"[DONE]\"}\n\n"),
		[]byte("data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"<｜｜DSML｜｜tool_calls><｜｜DSML｜｜invoke name=\\\\\\\"Bash\\\\\\\"><｜｜DSML｜｜parameter name=\\\\\\\"command\\\\\\\">ls</｜｜DSML｜｜parameter></｜｜DSML｜｜invoke></｜｜DSML｜｜tool_calls>\\\"}}]}\"}\n\ndata: {\"body\":\"[DONE]\"}\n\n"),
		[]byte("data: {\"body\":\"{\\\"event\\\":\\\"tool_use_start\\\",\\\"index\\\":0,\\\"data\\\":{\\\"id\\\":\\\"c1\\\",\\\"name\\\":\\\"Read\\\"}}\"}\n\ndata: {\"body\":\"{\\\"event\\\":\\\"tool_use_delta\\\",\\\"index\\\":0,\\\"data\\\":{\\\"arguments\\\":\\\"{}\\\"}}\"}\n\ndata: {\"body\":\"{\\\"event\\\":\\\"tool_use_delta\\\",\\\"index\\\":2,\\\"data\\\":{\\\"id\\\":\\\"c1\\\",\\\"arguments\\\":\\\"{\\\\\\\"p\\\\\\\":1}\\\"}}\"}\n\ndata: {\"body\":\"{\\\"event\\\":\\\"message_stop\\\"}\"}\n\n"),
		[]byte("data: {\"statusCodeValue\":429,\"body\":\"{\\\"code\\\":115,\\\"message\\\":\\\"limit\\\",\\\"agentLimitResetTime\\\":1700000000000}\"}\n\n"),
		[]byte("data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"tool_calls\\\":[{\\\"index\\\":999999999,\\\"name\\\":\\\"Bash\\\",\\\"arguments\\\":\\\"{\\\\\\\"cmd\\\\\\\":\\\"}}]}}]}\"}\n\n"),
		[]byte(""),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		reader := sse.NewReader(bytes.NewReader(data))
		dec := qoderchat.NewStreamDecoder()
		var events []ir.Event
		var streamCause error

		for reader.Scan() {
			batch, err := dec.Decode(reader.Frame())
			if err != nil {
				streamCause = err
				break
			}
			events = append(events, batch...)
		}
		if streamCause == nil {
			if err := reader.Err(); err != nil && !errors.Is(err, io.EOF) {
				streamCause = err
			}
		}
		events = append(events, dec.Close(streamCause)...)

		if err := ir.ValidateStream(events); err != nil {
			t.Fatalf("ir.ValidateStream failed: %v\nevents=%+v", err, events)
		}

		var acc ir.Accumulator
		acc.Add(events...)
		if acc.StreamError() == nil {
			resp := acc.Response()
			if err := ir.ValidateResponse(resp); err != nil {
				t.Fatalf("ir.ValidateResponse failed: %v", err)
			}
		}
	})
}

// FuzzResponseDecode feeds arbitrary buffered JSON and SSE byte sequences into
// qoderchat.DecodeResponse and qoderchat.DecodePayloadRequest. It verifies zero
// panics and confirms that every successful decode satisfies ir.ValidateResponse
// or ir.Validate invariants.
func FuzzResponseDecode(f *testing.F) {
	seeds := [][]byte{
		[]byte(`{"statusCodeValue":200,"body":"{\"choices\":[{\"message\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}"}`),
		[]byte(`{"choices":[{"message":{"content":[{"type":"text","text":"hello"}]},"finish_reason":"stop"}]}`),
		[]byte("data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"hi\\\"}}]}\"}\n\ndata: {\"body\":\"[DONE]\"}\n\n"),
		[]byte(`{"statusCode":"BAD_REQUEST","body":"{\"code\":\"112\",\"message\":\"denied\"}"}`),
		[]byte(`{"model":"qwen3.8-max","system":"sys","messages":[{"role":"user","text":"hi"}],"max_tokens":512}`),
		[]byte(""),
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if resp, err := qoderchat.DecodeResponse(data); err == nil {
			if vErr := ir.ValidateResponse(resp); vErr != nil {
				t.Fatalf("DecodeResponse produced invalid ir.Response: %v", vErr)
			}
		}
		if req, err := qoderchat.DecodePayloadRequest(data); err == nil {
			if vErr := ir.Validate(req); vErr != nil {
				t.Fatalf("DecodePayloadRequest produced invalid ir.Request: %v", vErr)
			}
		}
	})
}
