package httpapi

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	openaiexecution "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
)

// openai_protocol_execution.go defines OpenAITextExecutor and dispatches Chat
// Completions and Anthropic Messages execution through the merged IR Responses
// runner and shared request services. Each entry constructs a request-local
// attempt adapter around the selected provider, delegates conversion and
// response recovery to RunConvertedResponses, and captures observed upstream
// response models onto the Gin context before returning results and errors.

// OpenAITextExecutor binds app-owned protocol execution and shared session
// services. Each entry creates request-local adapter state while these service
// references remain available throughout synchronous forwarding and response
// observation.
type OpenAITextExecutor struct {
	Compact  *CompactExecutor      // shared Compact recovery executor
	Requests *OpenAIRequests       // shared upstream request builder
	Output   *OpenAIResponseOutput // shared response output bindings
	Grok     *GrokExecutor         // shared Grok execution adapter
	// text execution credential resolver
	Credentials *gatewayprovider.RequestCredentials
	// current fast-tier policy reader
	FastPolicy *gatewayprovider.ExecutionFastPolicy
	// Continuation and PromptCache retain session identities across attempts with
	// independent expiry rules. Continuation needs caller coordination for
	// same-key updates; PromptCache publishes the new prefix before separately
	// deleting its prior chain, so complete same-namespace updates also need coordination.
	// provider/key/prompt continuation storage
	Continuation *session.CompatResponses
	// Anthropic digest-prefix cache
	PromptCache *session.AnthropicPromptCache
	// Codex quota-header observation service
	CodexUsage *provideradapter.CodexUsageObserver
	// ForcedTemplate supplies optional Codex instructions for Messages conversion;
	// RunConvertedResponses skips template rendering when this configured text is empty.
	ForcedTemplate string
	// ResponseTTL supplies PromptCache.Bind expiry. Configure this callback before
	// Messages execution can publish a new digest-chain cache binding.
	ResponseTTL func() time.Duration
}

// Chat executes Chat Completions using the selected attempt provider and
// protocol policy. It starts with an unisolated compatibility key; callers
// retain the HTTP context until conversion, reading and model capture finish,
// then inspect both result and error.
func (s *OpenAITextExecutor) Chat(
	ctx context.Context,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	body []byte,
	promptCacheKey string,
	defaultMappedModel string,
	tlsRouterMatch ...egress.TLSFingerprintRouterMatchResult,
) (*forwardcore.OpenAIResult, error) {
	return s.ChatWithCacheIsolation(ctx, c, provider, body, promptCacheKey, defaultMappedModel, false, tlsRouterMatch...)
}

// ChatWithCacheIsolation executes Chat using a new adapter for this call.
// compatPromptCacheTenantIsolated records whether the supplied key already
// carries API-key isolation, preventing RunConvertedResponses from isolating
// that seed again; returned partial observations can accompany an error.
func (s *OpenAITextExecutor) ChatWithCacheIsolation(ctx context.Context, c *gin.Context, provider *gatewayprovider.ExecutionProvider, body []byte, promptCacheKey, defaultMappedModel string, compatPromptCacheTenantIsolated bool, tlsRouterMatch ...egress.TLSFingerprintRouterMatchResult) (*forwardcore.OpenAIResult, error) {
	p := s.newConvertedResponsesAdapter(c, provider, tlsRouterMatch)
	result, err := openaiexecution.RunConvertedResponses(ctx, protocol.ProtocolOpenAIChatCompletions, body, promptCacheKey, defaultMappedModel, compatPromptCacheTenantIsolated, p)
	out := openaiexecution.ToForwardResult(result)
	captureResponseModel(c, out)
	return out, err
}

// chatError reads and handles an upstream HTTP rejection using Chat error
// writers. The caller retains response closure and can supply the canonical
// scheduling model; CompatError may return a failover signal instead of
// writing a final error.
func (s *OpenAITextExecutor) chatError(
	resp *http.Response,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	requestedModel ...string,
) (*forwardcore.OpenAIResult, error) {
	return s.Output.CompatError(resp, c, provider, WriteForwardChatError, WriteForwardChatErrorBody, requestedModel...)
}

// Messages executes one selected provider attempt through RunConvertedResponses
// for Anthropic Messages input. The HTTP context and bound services remain live
// through synchronous output and model capture; callers inspect both returned
// observation and error before deciding completion or another provider.
func (s *OpenAITextExecutor) Messages(ctx context.Context, c *gin.Context, provider *gatewayprovider.ExecutionProvider, body []byte, promptCacheKey, defaultMappedModel string, tlsRouterMatch ...egress.TLSFingerprintRouterMatchResult) (*forwardcore.OpenAIResult, error) {
	ports := s.newConvertedResponsesAdapter(c, provider, tlsRouterMatch)
	result, err := openaiexecution.RunConvertedResponses(ctx, protocol.ProtocolAnthropicMessages, body, promptCacheKey, defaultMappedModel, false, ports)
	out := openaiexecution.ToForwardResult(result)
	captureResponseModel(c, out)
	return out, err
}

// messagesError handles an upstream HTTP rejection using Anthropic envelope
// and body writers. The caller owns response closure and may supply a
// scheduling model; CompatError can return a failover signal before final
// client error output.
func (s *OpenAITextExecutor) messagesError(
	resp *http.Response,
	c *gin.Context,
	provider *gatewayprovider.ExecutionProvider,
	requestedModel ...string,
) (*forwardcore.OpenAIResult, error) {
	return s.Output.CompatError(resp, c, provider, WriteForwardAnthropicError, WriteForwardAnthropicErrorBody, requestedModel...)
}
