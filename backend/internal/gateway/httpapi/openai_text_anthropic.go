package httpapi

import (
	"context"

	"github.com/gin-gonic/gin"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	openaiexecution "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// openai_text_anthropic.go dispatches Messages, Chat, and Responses requests to
// compatible Anthropic execution.
// A selected Anthropic-compatible upstream serves Messages directly or receives
// requests converted from Chat or Responses. Direct Messages unpacks or strips
// trc1 continuity envelopes via PrepareNativeReplay, while converted OpenAI
// flows record raw UpstreamResponseModel observations before returning.

// NativeMessages executes one selected provider through its native Anthropic
// endpoint, unpacking or stripping trc1 continuity envelopes before model
// preparation.
func (s *OpenAITextExecutor) NativeMessages(
	ctx context.Context,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	body []byte,
	defaultMappedModel string,
) (*forwardcore.OpenAIResult, error) {
	adapter := &openAINativeAnthropicAdapter{
		openAIMessagesExecutionAdapter: &openAIMessagesExecutionAdapter{s: s, c: c, provider: provider},
		kind:                           openaiexecution.NativeMessages,
	}
	result, err := openaiexecution.ForwardNativeMessages(ctx, body, defaultMappedModel, adapter)
	return openaiexecution.ToForwardResult(result), err
}

// NativeChat executes Chat input through the selected compatible Anthropic
// endpoint via ForwardConvertedAnthropic and records raw UpstreamResponseModel
// observations.
func (s *OpenAITextExecutor) NativeChat(
	ctx context.Context,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	body []byte,
	defaultMappedModel string,
) (*forwardcore.OpenAIResult, error) {
	adapter := &openAINativeAnthropicAdapter{
		openAIMessagesExecutionAdapter: &openAIMessagesExecutionAdapter{s: s, c: c, provider: provider},
		kind:                           openaiexecution.NativeChat,
	}
	result, err := openaiexecution.ForwardConvertedAnthropic(ctx, protocol.ProtocolOpenAIChatCompletions, body, defaultMappedModel, adapter)
	out := openaiexecution.ToForwardResult(result)
	captureResponseModel(c, out)
	return out, err
}

// NativeResponses executes Responses input through the selected compatible
// Anthropic endpoint via ForwardConvertedAnthropic and records raw
// UpstreamResponseModel observations.
func (s *OpenAITextExecutor) NativeResponses(
	ctx context.Context,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	body []byte,
	defaultMappedModel string,
) (*forwardcore.OpenAIResult, error) {
	adapter := &openAINativeAnthropicAdapter{
		openAIMessagesExecutionAdapter: &openAIMessagesExecutionAdapter{s: s, c: c, provider: provider},
		kind:                           openaiexecution.NativeResponses,
	}
	result, err := openaiexecution.ForwardConvertedAnthropic(ctx, protocol.ProtocolOpenAIResponses, body, defaultMappedModel, adapter)
	out := openaiexecution.ToForwardResult(result)
	captureResponseModel(c, out)
	return out, err
}
