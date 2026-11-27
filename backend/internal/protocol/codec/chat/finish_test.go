package chat

import (
	"encoding/json/jsontext"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestFinishReasonMapping verifies bidirectional finish_reason normalization
// across empty tool-call inference and legacy function_call aliases. Distinct
// IR stop_sequence and pause boundaries must fold cleanly into Chat "stop".
func TestFinishReasonMapping(t *testing.T) {
	require.Equal(t, ir.Finish{Reason: ir.FinishStop}, decodeFinishReason("", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishToolCalls}, decodeFinishReason("", true))
	require.Equal(t, ir.Finish{Reason: ir.FinishStop}, decodeFinishReason("stop", true))
	require.Equal(t, ir.Finish{Reason: ir.FinishLength}, decodeFinishReason("length", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishToolCalls}, decodeFinishReason("tool_calls", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishToolCalls}, decodeFinishReason("function_call", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishContentFilter}, decodeFinishReason("content_filter", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishOther}, decodeFinishReason("vendor_custom", false))

	require.Equal(t, "stop", encodeFinishReason(ir.Finish{Reason: ir.FinishStop}))
	require.Equal(t, "stop", encodeFinishReason(ir.Finish{Reason: ir.FinishStopSequence, StopSequence: "END"}))
	require.Equal(t, "stop", encodeFinishReason(ir.Finish{Reason: ir.FinishPause}))
	require.Equal(t, "length", encodeFinishReason(ir.Finish{Reason: ir.FinishLength}))
	require.Equal(t, "tool_calls", encodeFinishReason(ir.Finish{Reason: ir.FinishToolCalls}))
	require.Equal(t, "content_filter", encodeFinishReason(ir.Finish{Reason: ir.FinishContentFilter}))
	require.Equal(t, "stop", encodeFinishReason(ir.Finish{Reason: ir.FinishOther}))
}

// TestStreamErrorDecodeAndEncode checks string, object, numeric-code, blank,
// and truncated stream error envelopes. Every abnormal termination shape must
// yield a non-empty diagnostic message and deterministic JSON output.
func TestStreamErrorDecodeAndEncode(t *testing.T) {
	require.Nil(t, decodeStreamError(nil))
	require.Nil(t, decodeStreamError(jsontext.Value("null")))

	strErr := decodeStreamError(jsontext.Value(`"rate limited"`))
	require.NotNil(t, strErr)
	require.Equal(t, "rate limited", strErr.Message)

	numCodeErr := decodeStreamError(jsontext.Value(`{"type":"api_error","code":503,"message":"overloaded","status":503}`))
	require.NotNil(t, numCodeErr)
	require.Equal(t, "503", numCodeErr.Code)
	require.Equal(t, "overloaded", numCodeErr.Message)
	require.Equal(t, 503, numCodeErr.Status)

	blankMsgErr := decodeStreamError(jsontext.Value(`{"type":"server_error","message":"   "}`))
	require.NotNil(t, blankMsgErr)
	require.Equal(t, defaultStreamErrorMessage, blankMsgErr.Message)

	encoded, err := encodeStreamError(numCodeErr)
	require.NoError(t, err)
	require.JSONEq(t, `{"error":{"type":"api_error","code":"503","message":"overloaded","status":503}}`, string(encoded))

	nilEncoded, err := encodeStreamError(nil)
	require.NoError(t, err)
	require.JSONEq(t, `{"error":{"type":"server_error","message":"upstream chat stream error"}}`, string(nilEncoded))

	trunc := truncatedStreamError(nil)
	require.Equal(t, "stream_truncated", trunc.Code)
	require.NotEmpty(t, trunc.Message)

	interrupted := truncatedStreamError(errors.New("unexpected EOF"))
	require.Equal(t, "stream_interrupted", interrupted.Code)
	require.Equal(t, "unexpected EOF", interrupted.Message)
}
