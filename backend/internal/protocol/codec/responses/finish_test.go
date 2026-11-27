package responses

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestDecodeAndEncodeResponsesFinish checks finish-reason mapping across
// completed, incomplete, failed, and cancelled Responses statuses. Callable tool
// presence on completed responses must select FinishToolCalls.
func TestDecodeAndEncodeResponsesFinish(t *testing.T) {
	require.Equal(t, ir.Finish{Reason: ir.FinishStop}, decodeResponsesFinish("completed", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishToolCalls}, decodeResponsesFinish("completed", nil, true))
	require.Equal(t, ir.Finish{Reason: ir.FinishLength}, decodeResponsesFinish("incomplete", &wireIncompleteDetails{Reason: "max_output_tokens"}, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishContentFilter}, decodeResponsesFinish("incomplete", &wireIncompleteDetails{Reason: "content_filter"}, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishPause}, decodeResponsesFinish("incomplete", &wireIncompleteDetails{Reason: "pause_turn"}, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishError}, decodeResponsesFinish("failed", nil, false))
	require.Equal(t, ir.Finish{Reason: ir.FinishOther}, decodeResponsesFinish("cancelled", nil, false))

	status, details, ev := encodeResponsesFinish(ir.Finish{Reason: ir.FinishLength})
	require.Equal(t, "incomplete", status)
	require.Equal(t, &wireIncompleteDetails{Reason: "max_output_tokens"}, details)
	require.Equal(t, "response.incomplete", ev)

	status, details, ev = encodeResponsesFinish(ir.Finish{Reason: ir.FinishContentFilter})
	require.Equal(t, "incomplete", status)
	require.Equal(t, &wireIncompleteDetails{Reason: "content_filter"}, details)
	require.Equal(t, "response.incomplete", ev)

	status, details, ev = encodeResponsesFinish(ir.Finish{Reason: ir.FinishError})
	require.Equal(t, "failed", status)
	require.Nil(t, details)
	require.Equal(t, "response.failed", ev)

	status, details, ev = encodeResponsesFinish(ir.Finish{Reason: ir.FinishToolCalls})
	require.Equal(t, "completed", status)
	require.Nil(t, details)
	require.Equal(t, "response.completed", ev)
}

// TestDecodeResponsesErrorAndTruncationError checks nested, flat, string, and
// numeric-code error envelopes alongside truncated stream error helpers. Every
// non-null error payload must yield a non-empty StreamError.Message.
func TestDecodeResponsesErrorAndTruncationError(t *testing.T) {
	require.Nil(t, decodeResponsesError(nil))
	require.Nil(t, decodeResponsesError([]byte("null")))

	strErr := decodeResponsesError([]byte(`"rate limit exceeded"`))
	require.NotNil(t, strErr)
	require.Equal(t, "rate limit exceeded", strErr.Message)

	nestedErr := decodeResponsesError([]byte(`{"type":"error","error":{"type":"invalid_request_error","code":429,"message":"quota reached","status":429}}`))
	require.NotNil(t, nestedErr)
	require.Equal(t, "invalid_request_error", nestedErr.Type)
	require.Equal(t, "429", nestedErr.Code)
	require.Equal(t, "quota reached", nestedErr.Message)
	require.Equal(t, 429, nestedErr.Status)

	emptyMsgErr := decodeResponsesError([]byte(`{"message":"   "}`))
	require.NotNil(t, emptyMsgErr)
	require.Equal(t, defaultResponsesErrorMessage, emptyMsgErr.Message)

	eofErr := truncatedResponsesStreamError(nil)
	require.Equal(t, "stream_truncated", eofErr.Code)
	require.NotEmpty(t, eofErr.Message)

	causeErr := truncatedResponsesStreamError(errors.New("unexpected EOF"))
	require.Equal(t, "stream_interrupted", causeErr.Code)
	require.Equal(t, "unexpected EOF", causeErr.Message)
}
