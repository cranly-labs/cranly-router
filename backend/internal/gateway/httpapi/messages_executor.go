package httpapi

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayadapter "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/messageforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// messages_executor.go creates synchronous HTTP output adapters for calls to
// the shared Messages forwarding runtime.
// Native Messages, Bedrock compatibility, Count Tokens, and converted OpenAI
// entry adapters share the bound runtime and response header filter, keeping
// platform attempts in that runtime while HTTP writers remain request-local.

// MessagesExecutor creates an HTTP output adapter for each runtime call.
type MessagesExecutor struct {
	runtime *messageforward.Runtime // shared Messages forwarding runtime
	// filter is shared across request-local output adapters. Nil selects shared
	// header filtering defaults, with passthrough defaults handled by MessageForwardBoundary.
	filter *egress.CompiledHeaderFilter
}

// NewMessagesExecutor retains the shared forwarding runtime and response
// header filter. It allocates no request state; callers supply a runtime
// before forwarding, and each method creates output for its active Gin
// request.
func NewMessagesExecutor(runtime *messageforward.Runtime, filter *egress.CompiledHeaderFilter) *MessagesExecutor {
	return &MessagesExecutor{runtime: runtime, filter: filter}
}

// ApplyBedrockCCCompat delegates Bedrock client compatibility using the
// current request context and live headers. It returns the prepared body;
// provider/model/group data select the runtime's applicable compatibility
// behavior for this attempt.
func (e *MessagesExecutor) ApplyBedrockCCCompat(c *gin.Context, body []byte, model string, target *gatewayadapter.ExecutionProvider, groupID *int64) []byte {
	return e.runtime.PrepareBedrockCompatibility(c.Request.Context(), c.Request.Header, body, model, target, groupID)
}

// Forward executes the selected provider with parsed Messages input and a new
// HTTP output adapter. The runtime owns platform preparation and response-body
// lifetime; outer orchestration interprets the returned result and error.
func (e *MessagesExecutor) Forward(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, parsed *requeststate.ParsedRequest) (*forward.MessagesResult, error) {
	return e.runtime.Execute(ctx, NewMessageForwardBoundary(c, e.filter), target, parsed)
}

// ForwardCountTokens delegates selected-provider counting with parsed input
// and request-local HTTP output. It returns the runtime error after response
// handling; counting does not create a normal text completion in this adapter.
func (e *MessagesExecutor) ForwardCountTokens(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, parsed *requeststate.ParsedRequest) error {
	return e.runtime.Count(ctx, NewMessageForwardBoundary(c, e.filter), target, parsed)
}

// ForwardConverted delegates IR-based OpenAI Chat or Responses conversion across
// Anthropic, Vertex, and Bedrock providers through e.runtime.Converted with a
// fresh request-local MessageForwardBoundary.
func (e *MessagesExecutor) ForwardConverted(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, source protocol.ProtocolID, body []byte) (*forward.MessagesResult, error) {
	return e.runtime.Converted(ctx, NewMessageForwardBoundary(c, e.filter), target, source, body)
}
