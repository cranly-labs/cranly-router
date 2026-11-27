package generatecontent

import (
	"encoding/json/jsontext"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestFinishReasonAndErrorEnvelopeMapping exercises bidirectional FinishReason
// translation across STOP, tool-call STOP, MAX_TOKENS, safety filters, and
// unrecognized vendor labels. It also checks that Google RPC error envelopes
// preserve numeric HTTP status codes alongside string status identifiers.
func TestFinishReasonAndErrorEnvelopeMapping(t *testing.T) {
	require.Equal(t, ir.Finish{Reason: ir.FinishStop}, decodeFinishReason("STOP", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishToolCalls}, decodeFinishReason("STOP", true))
	require.Equal(t, ir.Finish{Reason: ir.FinishLength}, decodeFinishReason("MAX_TOKENS", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishContentFilter}, decodeFinishReason("RECITATION", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishContentFilter}, decodeFinishReason("IMAGE_SAFETY", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishOther}, decodeFinishReason("MALFORMED_FUNCTION_CALL", false))
	require.Equal(t, ir.Finish{Reason: ir.FinishOther}, decodeFinishReason("FUTURE_REASON", false))

	require.Equal(t, "STOP", encodeFinishReason(ir.Finish{Reason: ir.FinishToolCalls}, nil))
	require.Equal(t, "MAX_TOKENS", encodeFinishReason(ir.Finish{Reason: ir.FinishLength}, nil))
	require.Equal(t, "SAFETY", encodeFinishReason(ir.Finish{Reason: ir.FinishContentFilter}, nil))
	require.Equal(t, "OTHER", encodeFinishReason(ir.Finish{Reason: ir.FinishOther}, nil))

	rpcRaw := jsontext.Value(`{"code":503,"message":"Capacity exhausted","status":"UNAVAILABLE"}`)
	se := decodeStreamError(rpcRaw)
	require.NotNil(t, se)
	require.Equal(t, 503, se.Status)
	require.Equal(t, "UNAVAILABLE", se.Code)
	require.Equal(t, "Capacity exhausted", se.Message)

	encodedErr, err := encodeStreamError(se)
	require.NoError(t, err)
	var wrapper struct {
		Error jsontext.Value `json:"error"`
	}
	require.NoError(t, unmarshalJSONObject(encodedErr, &wrapper))
	roundTripSE := decodeStreamError(wrapper.Error)
	require.Equal(t, se, roundTripSE)

	require.Nil(t, decodeStreamError(jsontext.Value("null")))
	scalarSE := decodeStreamError(jsontext.Value(`"plain error"`))
	require.Equal(t, "plain error", scalarSE.Message)

	structuredCause := &ir.StreamError{Type: "overloaded", Code: "RESOURCE_EXHAUSTED", Message: "quota", Status: 429}
	require.Equal(t, structuredCause, truncatedStreamError(structuredCause))
	require.Equal(t, "stream_interrupted", truncatedStreamError(errors.New("broken pipe")).Code)
	require.Equal(t, "stream_truncated", truncatedStreamError(nil).Code)
}
