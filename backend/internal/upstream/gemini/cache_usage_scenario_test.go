package gemini

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

// cache_usage_scenario_test.go covers response.go and response_converted.go
// across Anthropic Messages, OpenAI Chat, and OpenAI Responses clients. It
// verifies wire usage and internal billing accounting when sparse cache token
// counts arrive after Gemini candidate completion.

// cacheWireSink captures serialized client payloads for protocol-level assertions.
type cacheWireSink struct {
	bytes.Buffer // captured client payload bytes
}

// Begin accepts response metadata without adding bytes to the fixture.
func (s *cacheWireSink) Begin(upstream.OutputHead) error { return nil }

// Emit retains each synchronous client payload in its original emission order.
func (s *cacheWireSink) Emit(event upstream.OutputEvent) error {
	_, err := s.Write(event.Data)
	return err
}

// newGeminiCacheExchangeForTest initializes a convert.Exchange for clientKind
// and includeUsage so ReadConvertedStreaming and ReadConvertedBuffered can
// format client-facing usage alongside billing accounting.
func newGeminiCacheExchangeForTest(t *testing.T, clientKind string, includeUsage bool) *convert.Exchange {
	t.Helper()
	var proto protocol.ProtocolID
	var reqBody []byte
	switch clientKind {
	case "messages":
		proto = protocol.ProtocolAnthropicMessages
		reqBody = []byte(`{"model":"gemini","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`)
	case "chat":
		proto = protocol.ProtocolOpenAIChatCompletions
		if includeUsage {
			reqBody = []byte(`{"model":"gemini","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`)
		} else {
			reqBody = []byte(`{"model":"gemini","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
		}
	default:
		proto = protocol.ProtocolOpenAIResponses
		reqBody = []byte(`{"model":"gemini","input":"hi"}`)
	}
	ex, err := convert.NewExchange(reqBody, convert.Options{
		Route: convert.Route{
			Client:   proto,
			Upstream: protocol.ProtocolGeminiGenerateContent,
		},
		ClientModel: "gemini",
	})
	require.NoError(t, err)
	return ex
}

// TestGeminiClientCacheUsageMatrix converts the same cached prompt into each
// supported client protocol. JSON and sparse-tail SSE must expose the cache
// count while preserving disjoint accounting and inclusive OpenAI input totals.
func TestGeminiClientCacheUsageMatrix(t *testing.T) {
	jsonBody := `{"candidates":[{"content":{"parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":60,"candidatesTokenCount":10}}`
	sseBody := "data: " + `{"candidates":[{"content":{"parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100}}` + "\n\n" +
		"data: " + `{"usageMetadata":{"cachedContentTokenCount":60,"candidatesTokenCount":10}}` + "\n\ndata: [DONE]\n\n"
	for _, clientKind := range []string{"messages", "chat", "responses"} {
		for _, stream := range []bool{false, true} {
			t.Run(clientKind+map[bool]string{false: "/json", true: "/sse"}[stream], func(t *testing.T) {
				sink := &cacheWireSink{}
				output := upstream.NewDeferredOutputContext(sink)
				adapter := ResponseAdapter{Options: ResponseOptions{
					ObserveImages: func([]byte) {},
					ReadBody:      io.ReadAll,
					WriteHeaders:  func(http.Header, http.Header) {},
					ReverseTools:  func(body []byte) []byte { return body },
				}}
				body := jsonBody
				if stream {
					body = sseBody
				}
				response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
				convOpts := ConvertedOptions{
					Exchange: newGeminiCacheExchangeForTest(t, clientKind, true),
				}
				var usage *upstream.TokenUsage
				var err error
				if stream {
					usage, err = adapter.ReadConvertedStreaming(output, response, convOpts, false)
				} else {
					usage, err = adapter.ReadConvertedBuffered(output, response, convOpts, false, false)
				}
				require.NoError(t, err)
				require.Equal(t, 40, usage.InputTokens)
				require.Equal(t, 60, usage.CacheReadInputTokens)
				require.Equal(t, 10, usage.OutputTokens)
				visible := gjson.Get(sink.String(), "usage")
				if stream {
					// Select the final serialized usage rather than converter state, so
					// tests detect fields omitted from actual client-facing terminal events.
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
					require.Equal(t, int64(40), visible.Get("input_tokens").Int())
					require.Equal(t, int64(60), visible.Get("cache_read_input_tokens").Int())
				case "chat":
					require.Equal(t, int64(100), visible.Get("prompt_tokens").Int())
					require.Equal(t, int64(60), visible.Get("prompt_tokens_details.cached_tokens").Int())
					require.Equal(t, int64(110), visible.Get("total_tokens").Int())
				case "responses":
					require.Equal(t, int64(100), visible.Get("input_tokens").Int())
					require.Equal(t, int64(60), visible.Get("input_tokens_details.cached_tokens").Int())
					require.Equal(t, int64(110), visible.Get("total_tokens").Int())
				}
			})
		}
	}
}

// TestGeminiChatUsageOptOutRetainsAccounting disables optional Chat usage
// while the upstream reports cached input. Client chunks must omit usage
// without removing the internal cache observation used for settlement.
func TestGeminiChatUsageOptOutRetainsAccounting(t *testing.T) {
	body := "data: " + `{"candidates":[{"content":{"parts":[{"text":"OK"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":60,"candidatesTokenCount":10}}` + "\n\n"
	sink := &cacheWireSink{}
	adapter := ResponseAdapter{Options: ResponseOptions{ReverseTools: func(data []byte) []byte { return data }}}
	response := &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
	usage, err := adapter.ReadConvertedStreaming(
		upstream.NewDeferredOutputContext(sink),
		response,
		ConvertedOptions{Exchange: newGeminiCacheExchangeForTest(t, "chat", false)},
		false,
	)
	require.NoError(t, err)
	require.Equal(t, 60, usage.CacheReadInputTokens)
	require.NotContains(t, sink.String(), `"usage"`)
}
