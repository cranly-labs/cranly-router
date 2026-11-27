package antigravity

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// cache_usage_scenario_test.go covers response_stream.go and
// response_converted.go across Anthropic Messages, OpenAI Chat, and OpenAI
// Responses clients. When a terminal candidate arrives before sparse cache
// metadata, every client protocol must postpone final usage emission until the
// shared stream reader finishes collecting trailing counters.

// cacheClientSink retains serialized protocol output without network effects.
type cacheClientSink struct {
	bytes.Buffer // captured client output bytes
}

// Begin accepts headers without altering the captured protocol response bytes.
func (s *cacheClientSink) Begin(upstream.OutputHead) error { return nil }

// Emit captures synchronous payloads in their protocol delivery order.
func (s *cacheClientSink) Emit(event upstream.OutputEvent) error {
	_, err := s.Write(event.Data)
	return err
}

// newAntigravityCacheExchangeForTest builds a convert.Exchange for clientKind
// and includeUsage so ReadConvertedStreaming and ReadConvertedBuffered can
// format client-facing usage alongside internal billing observations.
func newAntigravityCacheExchangeForTest(t *testing.T, clientKind string, includeUsage bool) *convert.Exchange {
	t.Helper()
	var proto protocol.ProtocolID
	var reqBody []byte
	switch clientKind {
	case "messages":
		proto = protocol.ProtocolAnthropicMessages
		reqBody = []byte(`{"model":"model","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`)
	case "chat":
		proto = protocol.ProtocolOpenAIChatCompletions
		if includeUsage {
			reqBody = []byte(`{"model":"model","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`)
		} else {
			reqBody = []byte(`{"model":"model","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
		}
	default:
		proto = protocol.ProtocolOpenAIResponses
		reqBody = []byte(`{"model":"model","input":"hi"}`)
	}
	ex, err := convert.NewExchange(reqBody, convert.Options{
		Route: convert.Route{
			Client:   proto,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect: convert.Dialect{
				Name:          "antigravity",
				SchemaDialect: convert.SchemaAntigravity,
			},
		},
		ClientModel: "model",
	})
	require.NoError(t, err)
	return ex
}

// TestAntigravityCacheTailAcrossClientProtocols supplies cache metadata after
// candidate completion. All six output paths must agree with internal usage
// and emit only one successful terminal event after the tail is consumed.
func TestAntigravityCacheTailAcrossClientProtocols(t *testing.T) {
	body := "data: " + `{"responseId":"outer_cache","response":{"candidates":[{"content":{"parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100}}}` + "\n\n" +
		"data: " + `{"response":{"usageMetadata":{"cachedContentTokenCount":60,"candidatesTokenCount":10}}}` + "\n\ndata: [DONE]\n\n"
	for _, clientKind := range []string{"messages", "chat", "responses"} {
		for _, stream := range []bool{false, true} {
			t.Run(clientKind+map[bool]string{false: "/json", true: "/sse"}[stream], func(t *testing.T) {
				response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
				sink := &cacheClientSink{}
				output := upstream.NewDeferredOutputContext(sink)
				adapter := ResponseAdapter{Options: ResponseOptions{
					ReverseTools: func(data []byte) []byte { return data },
				}}
				convOpts := ConvertedOptions{
					Exchange: newAntigravityCacheExchangeForTest(t, clientKind, true),
				}
				var result *StreamResult
				var err error
				if stream {
					result, err = adapter.ReadConvertedStreaming(output, response, convOpts)
				} else {
					result, err = adapter.ReadConvertedBuffered(output, response, convOpts)
				}
				require.NoError(t, err)
				require.Equal(t, 40, result.Usage.InputTokens)
				require.Equal(t, 60, result.Usage.CacheReadInputTokens)
				require.Equal(t, 10, result.Usage.OutputTokens)
				visible := gjson.Get(sink.String(), "usage")
				if stream {
					for line := range strings.SplitSeq(sink.String(), "\n") {
						if !strings.HasPrefix(line, "data: ") {
							continue
						}
						payload := strings.TrimPrefix(line, "data: ")
						if u := gjson.Get(payload, "usage"); u.IsObject() {
							visible = u
						}
						if u := gjson.Get(payload, "response.usage"); u.IsObject() {
							visible = u
						}
					}
				}
				require.True(t, visible.IsObject(), sink.String())
				switch clientKind {
				case "messages":
					require.Contains(t, sink.String(), `"id":"outer_cache"`)
					require.Equal(t, int64(40), visible.Get("input_tokens").Int())
					require.Equal(t, int64(60), visible.Get("cache_read_input_tokens").Int())
					if stream {
						require.Equal(t, 1, strings.Count(sink.String(), `"type":"message_stop"`))
					}
				case "chat":
					require.Equal(t, int64(100), visible.Get("prompt_tokens").Int())
					require.Equal(t, int64(60), visible.Get("prompt_tokens_details.cached_tokens").Int())
				case "responses":
					require.Equal(t, int64(100), visible.Get("input_tokens").Int())
					require.Equal(t, int64(60), visible.Get("input_tokens_details.cached_tokens").Int())
				}
			})
		}
	}
}

// TestAntigravityCacheTailFailureCannotEmitSuccess places an explicit provider
// error after completed content and cache usage. Accounting survives the failure,
// but Messages must not emit a successful message_stop after that error.
func TestAntigravityCacheTailFailureCannotEmitSuccess(t *testing.T) {
	body := "data: " + `{"response":{"candidates":[{"content":{"parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":60}}}` + "\n\n" +
		"data: " + `{"error":{"code":500,"message":"fixture failure"}}` + "\n\n"
	sink := &cacheClientSink{}
	adapter := ResponseAdapter{}
	response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
	result, err := adapter.ReadConvertedStreaming(
		upstream.NewDeferredOutputContext(sink),
		response,
		ConvertedOptions{Exchange: newAntigravityCacheExchangeForTest(t, "messages", true)},
	)
	require.Error(t, err)
	require.Equal(t, 60, result.Usage.CacheReadInputTokens)
	require.Contains(t, sink.String(), "event: error")
	require.NotContains(t, sink.String(), `"type":"message_stop"`)
}

// TestAntigravityChatUsageOptOutRetainsAccounting disables optional Chat usage
// while the upstream reports cached input. Internal accounting must retain
// cache counts even though the client requested content-only stream chunks.
func TestAntigravityChatUsageOptOutRetainsAccounting(t *testing.T) {
	body := "data: " + `{"response":{"candidates":[{"content":{"parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":60,"candidatesTokenCount":10}}}` + "\n\n"
	sink := &cacheClientSink{}
	adapter := ResponseAdapter{Options: ResponseOptions{ReverseTools: func(data []byte) []byte { return data }}}
	response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
	result, err := adapter.ReadConvertedStreaming(
		upstream.NewDeferredOutputContext(sink),
		response,
		ConvertedOptions{Exchange: newAntigravityCacheExchangeForTest(t, "chat", false)},
	)
	require.NoError(t, err)
	require.Equal(t, 60, result.Usage.CacheReadInputTokens)
	require.NotContains(t, sink.String(), `"usage"`)
}
