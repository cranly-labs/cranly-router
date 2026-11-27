package messages

import (
	"encoding/json/jsontext"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestStopReasonMapping verifies bidirectional Anthropic Messages stop_reason
// and stop_sequence normalization across blank tool-use inference, context-window
// limits, safety refusals, and server pause_turn boundaries. Unknown vendor stop
// reasons must map to FinishOther on decode and collapse to "end_turn" on encode.
func TestStopReasonMapping(t *testing.T) {
	seq := "</stop>"
	require.Equal(t, ir.Finish{Reason: ir.FinishStop}, decodeStopReason("", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishToolCalls}, decodeStopReason("", nil, true))
	require.Equal(t, ir.Finish{Reason: ir.FinishStop}, decodeStopReason("end_turn", nil, true))
	require.Equal(t, ir.Finish{Reason: ir.FinishLength}, decodeStopReason("max_tokens", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishLength}, decodeStopReason("model_context_window_exceeded", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishStopSequence, StopSequence: "</stop>"}, decodeStopReason("stop_sequence", &seq, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishStopSequence}, decodeStopReason("stop_sequence", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishToolCalls}, decodeStopReason("tool_use", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishContentFilter}, decodeStopReason("refusal", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishPause}, decodeStopReason("pause_turn", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishOther}, decodeStopReason("vendor_custom", nil, false))

	reason, outSeq := encodeStopReason(ir.Finish{}, false)
	require.Equal(t, "end_turn", reason)
	require.Nil(t, outSeq)

	reason, outSeq = encodeStopReason(ir.Finish{}, true)
	require.Equal(t, "tool_use", reason)
	require.Nil(t, outSeq)

	reason, outSeq = encodeStopReason(ir.Finish{Reason: ir.FinishLength}, false)
	require.Equal(t, "max_tokens", reason)
	require.Nil(t, outSeq)

	reason, outSeq = encodeStopReason(ir.Finish{Reason: ir.FinishToolCalls}, false)
	require.Equal(t, "tool_use", reason)
	require.Nil(t, outSeq)

	reason, outSeq = encodeStopReason(ir.Finish{Reason: ir.FinishContentFilter}, false)
	require.Equal(t, "refusal", reason)
	require.Nil(t, outSeq)

	reason, outSeq = encodeStopReason(ir.Finish{Reason: ir.FinishPause}, false)
	require.Equal(t, "pause_turn", reason)
	require.Nil(t, outSeq)

	reason, outSeq = encodeStopReason(ir.Finish{Reason: ir.FinishStopSequence, StopSequence: "</stop>"}, false)
	require.Equal(t, "stop_sequence", reason)
	require.NotNil(t, outSeq)
	require.Equal(t, "</stop>", *outSeq)

	reason, outSeq = encodeStopReason(ir.Finish{Reason: ir.FinishStopSequence}, false)
	require.Equal(t, "stop_sequence", reason)
	require.Nil(t, outSeq)

	reason, outSeq = encodeStopReason(ir.Finish{Reason: ir.FinishOther}, false)
	require.Equal(t, "end_turn", reason)
	require.Nil(t, outSeq)
}

// TestMessagesStreamErrorDecodeAndEncode checks string, object, numeric-code,
// blank-message, and truncated Messages SSE error payloads. Every abnormal
// termination shape must yield a non-empty diagnostic message and deterministic JSON.
func TestMessagesStreamErrorDecodeAndEncode(t *testing.T) {
	require.Nil(t, decodeStreamError(nil))
	require.Nil(t, decodeStreamError(jsontext.Value("null")))

	strErr := decodeStreamError(jsontext.Value(`"rate limited"`))
	require.NotNil(t, strErr)
	require.Equal(t, "api_error", strErr.Type)
	require.Equal(t, "rate limited", strErr.Message)

	blankStrErr := decodeStreamError(jsontext.Value(`"   "`))
	require.NotNil(t, blankStrErr)
	require.Equal(t, defaultStreamErrorMessage, blankStrErr.Message)

	numCodeErr := decodeStreamError(jsontext.Value(`{"type":"overloaded_error","code":529,"message":"overloaded","status":529}`))
	require.NotNil(t, numCodeErr)
	require.Equal(t, "overloaded_error", numCodeErr.Type)
	require.Equal(t, "529", numCodeErr.Code)
	require.Equal(t, "overloaded", numCodeErr.Message)
	require.Equal(t, 529, numCodeErr.Status)

	blankMsgErr := decodeStreamError(jsontext.Value(`{"type":"","code":"bad_req","message":"  "}`))
	require.NotNil(t, blankMsgErr)
	require.Equal(t, "api_error", blankMsgErr.Type)
	require.Equal(t, "bad_req", blankMsgErr.Code)
	require.Equal(t, defaultStreamErrorMessage, blankMsgErr.Message)

	scalarFallback := decodeStreamError(jsontext.Value(`123`))
	require.NotNil(t, scalarFallback)
	require.Equal(t, defaultStreamErrorMessage, scalarFallback.Message)

	encoded, err := encodeStreamError(numCodeErr)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"error","error":{"type":"overloaded_error","code":"529","message":"overloaded","status":529}}`, string(encoded))

	nilEncoded, err := encodeStreamError(nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"type":"error","error":{"type":"api_error","message":"upstream messages stream error"}}`, string(nilEncoded))

	trunc := truncatedStreamError(nil)
	require.Equal(t, "stream_truncated", trunc.Code)
	require.NotEmpty(t, trunc.Message)

	interrupted := truncatedStreamError(errors.New("unexpected EOF"))
	require.Equal(t, "stream_interrupted", interrupted.Code)
	require.Equal(t, "unexpected EOF", interrupted.Message)
}
