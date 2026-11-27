package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// TestValidateResponsesStream feeds valid OpenAI Responses SSE streams and
// reproduces missing created_at (6b6987483), stuck content_index (ac38d88f0),
// and stray argument delta names (091ec1f41). Valid streams and response.failed
// truncations must pass, while each regression shape must report its rule code.
func TestValidateResponsesStream(t *testing.T) {
	validFrames := []sse.Frame{
		{Event: "response.created", Data: []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","object":"response","created_at":1760000000,"status":"in_progress","model":"gpt-5","output":[]}}`)},
		{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","content":[]}}`)},
		{Event: "response.content_part.added", Data: []byte(`{"type":"response.content_part.added","sequence_number":2,"output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}`)},
		{Event: "response.output_text.delta", Data: []byte(`{"type":"response.output_text.delta","sequence_number":3,"output_index":0,"content_index":0,"delta":"Hello"}`)},
		{Event: "response.content_part.done", Data: []byte(`{"type":"response.content_part.done","sequence_number":4,"output_index":0,"content_index":0,"part":{"type":"output_text","text":"Hello"}}`)},
		{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","content":[{"type":"output_text","text":"Hello"}]}}`)},
		{Event: "response.output_item.added", Data: []byte(`{"type":"response.output_item.added","sequence_number":6,"output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":""}}`)},
		{Event: "response.function_call_arguments.delta", Data: []byte(`{"type":"response.function_call_arguments.delta","sequence_number":7,"output_index":1,"delta":"{}"}`)},
		{Event: "response.output_item.done", Data: []byte(`{"type":"response.output_item.done","sequence_number":8,"output_index":1,"item":{"id":"fc_1","type":"function_call","call_id":"call_1","name":"lookup","arguments":"{}"}}`)},
		{Event: "response.completed", Data: []byte(`{"type":"response.completed","sequence_number":9,"response":{"id":"resp_1","object":"response","created_at":1760000000,"status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":0}}}}`)},
	}
	require.Empty(t, ValidateResponsesStream(validFrames, GrammarOptions{}))

	// Missing created_at (6b6987483) must report missing_created_at.
	missingCreatedAt := append([]sse.Frame(nil), validFrames...)
	missingCreatedAt[0] = sse.Frame{Event: "response.created", Data: []byte(`{"type":"response.created","sequence_number":0,"response":{"id":"resp_1","object":"response","status":"in_progress","model":"gpt-5","output":[]}}`)}
	v := ValidateResponsesStream(missingCreatedAt, GrammarOptions{})
	require.NotEmpty(t, v)
	require.Equal(t, "missing_created_at", v[0].Rule)

	// Stray "name":"" on function_call_arguments.delta (091ec1f41) must fail.
	badDeltaName := append([]sse.Frame(nil), validFrames...)
	badDeltaName[7] = sse.Frame{Event: "response.function_call_arguments.delta", Data: []byte(`{"type":"response.function_call_arguments.delta","sequence_number":7,"output_index":1,"name":"","delta":"{}"}`)}
	v = ValidateResponsesStream(badDeltaName, GrammarOptions{})
	require.NotEmpty(t, v)
	require.Equal(t, "unexpected_name_on_arguments_delta", v[0].Rule)

	// Non-advancing content_index (ac38d88f0) must fail.
	stuckIdx := []sse.Frame{
		validFrames[0],
		validFrames[1],
		validFrames[2],
		{Event: "response.content_part.done", Data: []byte(`{"type":"response.content_part.done","sequence_number":3,"output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}`)},
		{Event: "response.content_part.added", Data: []byte(`{"type":"response.content_part.added","sequence_number":4,"output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}`)},
	}
	v = ValidateResponsesStream(stuckIdx, GrammarOptions{})
	require.NotEmpty(t, v)
	require.Equal(t, "non_contiguous_content_index", v[0].Rule)

	// Abnormal termination after response.created requires response.failed rather than bare error.
	failedStream := []sse.Frame{
		validFrames[0],
		{Event: "response.failed", Data: []byte(`{"type":"response.failed","sequence_number":1,"response":{"id":"resp_1","object":"response","created_at":1760000000,"status":"failed","model":"gpt-5","output":[]}}`)},
	}
	require.Empty(t, ValidateResponsesStream(failedStream, GrammarOptions{AllowAbnormal: true}))
}
