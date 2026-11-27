package openai

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	wire "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// usage_accounting_scenario_test.go covers response_converted.go and the shared
// protocol/openai usage accumulator across converted Messages and Chat streams.
// Client completion events and returned billing usage must preserve image token
// accounting across sparse, omitted, and reset terminal usage observations.

// accountingStreamSink captures converted stream payloads for terminal assertions.
type accountingStreamSink struct {
	bytes.Buffer // captured client stream bytes
}

// Begin accepts response metadata without changing captured client payloads.
func (s *accountingStreamSink) Begin(upstream.OutputHead) error { return nil }

// Emit copies each synchronous payload into the retained client stream.
func (s *accountingStreamSink) Emit(event upstream.OutputEvent) error {
	_, err := s.Write(event.Data)
	return err
}

// TestConvertedResponsesStreamsPreserveImageAccounting drives ReadConvertedStreaming
// for Messages and Chat clients through sparse, absent, and reset terminal usage.
// The emitted completion and returned billing buckets must reflect the same
// response without dropping images.
func TestConvertedResponsesStreamsPreserveImageAccounting(t *testing.T) {
	for _, client := range []string{"messages", "chat"} {
		for _, fixture := range []struct {
			name  string            // terminal fixture description
			usage string            // optional terminal usage member
			want  wire.ForwardUsage // expected final billing buckets
		}{
			{
				name: "absent usage",
				want: wire.ForwardUsage{InputTokens: 100, OutputTokens: 3, CacheReadInputTokens: 20, ImageInputTokens: 40, ImageOutputTokens: 2},
			},
			{
				name:  "sparse usage",
				usage: `,"usage":{"output_tokens":10}`,
				want:  wire.ForwardUsage{InputTokens: 100, OutputTokens: 10, CacheReadInputTokens: 20, ImageInputTokens: 40, ImageOutputTokens: 2},
			},
			{
				name:  "explicit zero images",
				usage: `,"usage":{"input_tokens_details":{"image_tokens":0},"output_tokens_details":{"image_tokens":0}}`,
				want:  wire.ForwardUsage{InputTokens: 100, OutputTokens: 3, CacheReadInputTokens: 20},
			},
			{
				name:  "complete zero reset",
				usage: `,"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}`,
			},
		} {
			t.Run(client+"/"+fixture.name, func(t *testing.T) {
				body := "data: " + `{"type":"response.created","response":{"id":"resp_images","model":"model","usage":{"input_tokens":100,"output_tokens":3,"input_tokens_details":{"cached_tokens":20,"image_tokens":40},"output_tokens_details":{"image_tokens":2}}}}` + "\n\n" +
					"data: " + `{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"OK"}` + "\n\n" +
					"data: " + `{"type":"response.completed","response":{"id":"resp_images","status":"completed","output":[]` + fixture.usage + `}}` + "\n\n"
				response := &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
				sink := &accountingStreamSink{}
				output := upstream.NewDeferredOutputContext(sink)

				clientProto := protocol.ProtocolAnthropicMessages
				reqBody := []byte(`{"model":"model","max_tokens":64,"stream":true,"messages":[{"role":"user","content":"hi"}]}`)
				if client == "chat" {
					clientProto = protocol.ProtocolOpenAIChatCompletions
					reqBody = []byte(`{"model":"model","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`)
				}
				ex, exErr := convert.NewExchange(reqBody, convert.Options{
					Route: convert.Route{
						Client:   clientProto,
						Upstream: protocol.ProtocolOpenAIResponses,
					},
					ClientModel: "model",
				})
				require.NoError(t, exErr)

				result, err := ReadConvertedStreaming(response, output, ex, ConvertedResponseOptions{
					RequestContext:   t.Context(),
					Headers:          func(http.Header, http.Header) {},
					ServiceTier:      func() string { return "" },
					StreamInterval:   func() time.Duration { return 0 },
					RestoreToolNames: func(payload []byte) []byte { return payload },
					Observe:          func([]byte, string) {},
				}, "model", "upstream-model", time.Now())
				require.NoError(t, err)
				if client == "messages" {
					require.Contains(t, sink.String(), `"type":"message_stop"`)
				} else {
					require.Contains(t, sink.String(), "data: [DONE]")
				}
				require.Equal(t, fixture.want, result.Usage)
				require.Contains(t, sink.String(), "OK")
			})
		}
	}
}
