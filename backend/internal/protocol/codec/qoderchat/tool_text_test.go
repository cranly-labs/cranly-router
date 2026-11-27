package qoderchat_test

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/qoderchat"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestTextToolCallFormatsAndChunkBoundaries streams XML, embedded JSON, and
// DSML text-encoded tool calls both as a single frame and split into one-rune
// SSE deltas. It verifies that deterministic call IDs, entity decoding, Bash
// alias normalization, and ir.ValidateStream invariants match across splits.
func TestTextToolCallFormatsAndChunkBoundaries(t *testing.T) {
	const (
		xmlSegment  = `<tool_call>Read<arg_value><arg_key>file_path</arg_key><arg_value>/workspace/a&amp;b&#47;README.md</arg_value></tool_call>`
		jsonSegment = `<tool_call>{"name":"shell","arguments":{"cmd":"pwd &amp;&amp; ls","description":"Check dir"}}</tool_call>`
		dsmlSegment = "<｜｜DSML｜｜tool_calls>\n" +
			`<｜｜DSML｜｜invoke name="execute_bash">` +
			`<｜｜DSML｜｜parameter name="command" string="true">echo &quot;hi&quot; &amp;&amp; true</｜｜DSML｜｜parameter>` +
			`</｜｜DSML｜｜invoke>` +
			`<｜｜DSML｜｜invoke name="empty_skipped"></｜｜DSML｜｜invoke>` +
			"</｜｜DSML｜｜tool_calls>"
	)

	fullText := "Preface " + xmlSegment + " middle " + jsonSegment + " tail " + dsmlSegment + " end."

	decWhole := qoderchat.NewStreamDecoder()
	wholeEvents := decodeWrappedTextChunks(t, decWhole, []string{fullText})
	require.NoError(t, ir.ValidateStream(wholeEvents))

	singleRuneChunks := make([]string, 0, len(fullText))
	for _, r := range fullText {
		singleRuneChunks = append(singleRuneChunks, string(r))
	}
	decSplit := qoderchat.NewStreamDecoder()
	splitEvents := decodeWrappedTextChunks(t, decSplit, singleRuneChunks)
	require.NoError(t, ir.ValidateStream(splitEvents))

	var wholeAcc, splitAcc ir.Accumulator
	wholeAcc.Add(wholeEvents...)
	splitAcc.Add(splitEvents...)
	wholeResp := wholeAcc.Response()
	splitResp := splitAcc.Response()

	require.Equal(t, ir.FinishToolCalls, wholeResp.Finish.Reason)
	require.Equal(t, ir.ResponseEssence(wholeResp), ir.ResponseEssence(splitResp))

	var toolCalls []*ir.ToolCall
	var visible strings.Builder
	for _, p := range splitResp.Parts {
		switch p.Kind {
		case ir.PartText:
			_, _ = visible.WriteString(p.Text)
		case ir.PartToolCall:
			toolCalls = append(toolCalls, p.Call)
		}
	}

	require.Equal(t, "Preface  middle  tail  end.", visible.String())
	require.Len(t, toolCalls, 3)

	require.Equal(t, "Read", toolCalls[0].Tool.Name)
	require.JSONEq(t, `{"file_path":"/workspace/a&b/README.md"}`, toolCalls[0].Arguments)
	require.True(t, strings.HasPrefix(toolCalls[0].CallID, "call_"))
	require.Len(t, toolCalls[0].CallID, 29)

	require.Equal(t, "Bash", toolCalls[1].Tool.Name)
	require.JSONEq(t, `{"command":"pwd && ls","description":"Check dir"}`, toolCalls[1].Arguments)

	require.Equal(t, "Bash", toolCalls[2].Tool.Name)
	require.JSONEq(t, `{"command":"echo \"hi\" && true"}`, toolCalls[2].Arguments)
}

// TestNormalizeToolCallNameAndArgumentsVariants passes shell aliases, cmd
// argument keys, raw shell command strings, and incomplete JSON prefixes into
// NormalizeToolCallNameAndArguments. It verifies that complete shell payloads
// normalize onto Bash {"command": ...} while partial JSON deltas stay intact.
func TestNormalizeToolCallNameAndArgumentsVariants(t *testing.T) {
	name, args := qoderchat.NormalizeToolCallNameAndArguments("shell", `{"cmd":"ls -la"}`)
	require.Equal(t, "Bash", name)
	require.JSONEq(t, `{"command":"ls -la"}`, args)

	name, args = qoderchat.NormalizeToolCallNameAndArguments("EXECUTE_BASH", "git status")
	require.Equal(t, "Bash", name)
	require.JSONEq(t, `{"command":"git status"}`, args)

	name, args = qoderchat.NormalizeToolCallNameAndArguments("Bash", `{"command":"partial`)
	require.Equal(t, "Bash", name)
	require.Equal(t, `{"command":"partial`, args)

	name, args = qoderchat.NormalizeToolCallNameAndArguments("  Read  ", `{"file_path":"a.go"}`)
	require.Equal(t, "Read", name)
	require.Equal(t, `{"file_path":"a.go"}`, args)
}

// TestUnclosedOrInvalidMarkupFlushesAsLiteralText streams an empty tool_call
// block followed by an unclosed opening marker prefix before stream completion.
// It verifies that both segments flush verbatim as visible assistant text rather
// than dropping user-visible prose at EOF.
func TestUnclosedOrInvalidMarkupFlushesAsLiteralText(t *testing.T) {
	dec := qoderchat.NewStreamDecoder()
	events := decodeWrappedTextChunks(t, dec, []string{
		"Literal example: <tool_call></tool_call> and trailing <tool_cal",
	})
	require.NoError(t, ir.ValidateStream(events))

	var acc ir.Accumulator
	acc.Add(events...)
	resp := acc.Response()
	require.Equal(t, ir.FinishStop, resp.Finish.Reason)
	require.Len(t, resp.Parts, 1)
	require.Equal(t, "Literal example: <tool_call></tool_call> and trailing <tool_cal", resp.Parts[0].Text)
}

// decodeWrappedTextChunks wraps each string in chunks inside a Qoder SSE choice
// delta envelope, appends a wrapped [DONE] frame, and closes dec with a nil
// cause. Any unexpected frame marshal or decode failure aborts t immediately.
func decodeWrappedTextChunks(t *testing.T, dec *qoderchat.StreamDecoder, chunks []string) []ir.Event {
	t.Helper()
	// deltaPayload encodes the inner delta.content field for one choice chunk.
	type deltaPayload struct {
		Content string `json:"content"` // raw or split UTF-8 chunk bytes
	}
	// choicePayload wraps deltaPayload inside a single Qoder choices array entry.
	type choicePayload struct {
		Delta deltaPayload `json:"delta"` // incremental choice delta
	}
	// innerPayload serializes the stringified inner JSON body of a Qoder SSE frame.
	type innerPayload struct {
		Choices []choicePayload `json:"choices"` // single-choice delta list
	}
	// wrapperPayload serializes the outer Qoder SSE JSON envelope around Body.
	type wrapperPayload struct {
		Body string `json:"body"` // stringified inner SSE body
	}

	var all []ir.Event
	for _, chunk := range chunks {
		innerBytes, err := json.Marshal(innerPayload{
			Choices: []choicePayload{{Delta: deltaPayload{Content: chunk}}},
		}, jsontext.AllowInvalidUTF8(true))
		require.NoError(t, err)
		frameBytes, err := json.Marshal(wrapperPayload{
			Body: string(innerBytes),
		}, jsontext.AllowInvalidUTF8(true))
		require.NoError(t, err)
		evs, err := dec.Decode(sse.Frame{Data: frameBytes})
		require.NoError(t, err)
		all = append(all, evs...)
	}
	doneEvs, err := dec.Decode(sse.Frame{Data: []byte(`{"body":"[DONE]"}`)})
	require.NoError(t, err)
	all = append(all, doneEvs...)
	all = append(all, dec.Close(nil)...)
	return all
}
