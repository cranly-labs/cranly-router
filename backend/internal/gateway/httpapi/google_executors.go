package httpapi

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayadapter "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/googleforward"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// google_executors.go creates request-local HTTP output adapters for Gemini
// and Antigravity runtime calls.
// Google platform preparation receives synchronous HTTP output for each
// attempt. Gemini and Antigravity retain their own runtime instances, while
// these entry adapters choose the client protocol and create the corresponding
// request-local error output before delegation.

// GeminiExecutor creates per-request synchronous output for shared Gemini request preparation.
type GeminiExecutor struct {
	Runtime *googleforward.Gemini // bound Gemini execution
}

// AntigravityExecutor connects Antigravity requests to HTTP output and error handling.
type AntigravityExecutor struct {
	Runtime *googleforward.Antigravity // bound Antigravity execution
}

// ForwardConverted delegates protocol-agnostic IR conversion for source onto
// the bound Gemini runtime with a fresh request-local GoogleBoundary. The
// caller retains provider failover and completion ownership, including any
// partial usage result returned alongside an error.
func (s *GeminiExecutor) ForwardConverted(
	ctx context.Context,
	c *gin.Context,
	a *gatewayadapter.ExecutionProvider,
	source protocol.ProtocolID,
	body []byte,
) (*forward.MessagesResult, error) {
	return s.Runtime.ForwardConverted(ctx, NewGoogleBoundary(c, s.Runtime.Options, false), a, source, body)
}

// Forward delegates Anthropic Messages execution to the bound Gemini runtime's
// ForwardConverted entry with request-local HTTP output. The caller retains
// provider selection and completion ownership, including any partial result
// returned with an error.
func (s *GeminiExecutor) Forward(ctx context.Context, c *gin.Context, a *gatewayadapter.ExecutionProvider, body []byte) (*forward.MessagesResult, error) {
	return s.Runtime.ForwardConverted(ctx, NewGoogleBoundary(c, s.Runtime.Options, false), a, protocol.ProtocolAnthropicMessages, body)
}

// ForwardNative delegates one Gemini model action with its explicit stream
// flag and body. The runtime uses Google-format output, and the caller
// interprets observed usage and any failure for completion.
func (s *GeminiExecutor) ForwardNative(ctx context.Context, c *gin.Context, a *gatewayadapter.ExecutionProvider, model, action string, stream bool, body []byte) (*forward.MessagesResult, error) {
	return s.Runtime.ForwardNative(ctx, NewGoogleBoundary(c, s.Runtime.Options, false), a, model, action, stream, body)
}

// ForwardConverted delegates protocol-agnostic IR conversion for source onto
// the bound Antigravity runtime with a fresh request-local GoogleBoundary and
// sticky-session flag. The caller retains provider selection, failover, and
// completion ownership, including any partial usage result returned with an error.
func (s *AntigravityExecutor) ForwardConverted(
	ctx context.Context,
	c *gin.Context,
	a *gatewayadapter.ExecutionProvider,
	source protocol.ProtocolID,
	body []byte,
	sticky bool,
) (*forward.MessagesResult, error) {
	return s.Runtime.ForwardConverted(ctx, NewGoogleBoundary(c, s.Runtime.Options, true), a, source, body, sticky)
}

// Forward delegates Anthropic Messages execution to the bound Antigravity
// runtime's ForwardConverted entry with request-local HTTP output. The caller
// retains provider selection and completion ownership, including any partial
// result returned with an error.
func (s *AntigravityExecutor) Forward(ctx context.Context, c *gin.Context, a *gatewayadapter.ExecutionProvider, body []byte, sticky bool) (*forward.MessagesResult, error) {
	return s.Runtime.ForwardConverted(ctx, NewGoogleBoundary(c, s.Runtime.Options, true), a, protocol.ProtocolAnthropicMessages, body, sticky)
}

// ForwardGemini delegates a Gemini-format attempt to Antigravity with sticky
// session state and optional Gemini session settings. Runtime output uses the
// native client format; outer orchestration owns provider switching and
// completion.
func (s *AntigravityExecutor) ForwardGemini(ctx context.Context, c *gin.Context, a *gatewayadapter.ExecutionProvider, model, action string, stream bool, body []byte, sticky bool, options ...forward.GeminiSessionOption) (*forward.MessagesResult, error) {
	return s.Runtime.ForwardGemini(ctx, NewGoogleBoundary(c, s.Runtime.Options, true), a, model, action, stream, body, sticky, options...)
}

// WriteMappedClaudeError presents the supplied Antigravity upstream failure as
// a Messages error. It creates request-local output using runtime options,
// records diagnostics there and returns the presentation error to its caller.
func (s *AntigravityExecutor) WriteMappedClaudeError(c *gin.Context, a *gatewayadapter.ExecutionProvider, status int, id string, body []byte) error {
	return NewGoogleBoundary(c, s.Runtime.Options, true).MappedClaudeError(a, status, id, body)
}
