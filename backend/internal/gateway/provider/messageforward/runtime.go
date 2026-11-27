package messageforward

import (
	"context"
	"time"

	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayadapter "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// runtime.go starts native Messages, token counting, and converted OpenAI
// flows with fresh attempt state. Each native Messages entry creates its own
// preparation state before invoking gateway/forward, Converted executes OpenAI
// Chat or Responses requests across Anthropic, Vertex, and Bedrock targets, and
// Count uses its retained HTTP response to finish telemetry without a usage writer.

// Execute forwards one parsed Messages request using fresh attempt state for
// the selected provider. gateway/forward controls preparation and partial
// results; the outer gateway selects any replacement provider.
// @project-doc docs/architecture/gateway_request_lifecycle.md#protocol_conversion_boundary
func (r *Runtime) Execute(ctx context.Context, output HTTPBoundary, target *gatewayadapter.ExecutionProvider, parsed *requeststate.ParsedRequest) (*forward.MessagesResult, error) {
	var preStripEffort *string
	if parsed != nil && gatewayadapter.ExecutionModelPolicy(target).ShouldStripWireEffort(ctx, parsed.Model) {
		preStripEffort = protocol.NormalizeClaudeOutputEffort(parsed.OutputEffort)
	}
	adapter := newAttemptWithContext(ctx, r, output, target)
	result, err := forward.Messages(ctx, adapter, adapter.input(), parsed)
	out := messagesResult(result)
	// Preserve pre-strip Claude effort for usage recording when wire effort
	// stripping cleared output_config.effort on the adopted outbound payload.
	if out != nil && out.ReasoningEffort == nil && preStripEffort != nil {
		out.ReasoningEffort = preStripEffort
	}
	return out, err
}

// Count runs token counting with fresh preparation state for the selected
// provider. The counting flow owns its response format and reports unsupported
// routes separately from ordinary generation errors. The latest retained HTTP
// response identifies the final dispatch after signature repair; prior HTTP
// failures and failed sends keep their already observed outcomes.
func (r *Runtime) Count(ctx context.Context, output HTTPBoundary, target *gatewayadapter.ExecutionProvider, parsed *requeststate.ParsedRequest) (failure error) {
	adapter := &countAttempt{attempt: newAttempt(r, output, target)}
	defer func() {
		telemetry.ObserveRequestResult(ctx, telemetry.RequestAttemptFromResponse(adapter.response), failure)
	}()
	return forward.CountTokens(ctx, adapter, adapter.input(), parsed)
}

// Converted executes one IR-based OpenAI Chat or Responses conversion attempt
// across Anthropic, Vertex, or AWS Bedrock targets via forward.AsConverted.
// Optional application attempt admission wraps preparation, exchange, and
// response reading; returned partial observations remain paired with any error.
func (r *Runtime) Converted(ctx context.Context, output HTTPBoundary, target *gatewayadapter.ExecutionProvider, source protocol.ProtocolID, body []byte) (*forward.MessagesResult, error) {
	if r != nil && r.dependencies.Enter != nil {
		done, err := r.dependencies.Enter()
		if err != nil {
			return nil, err
		}
		if done != nil {
			defer done()
		}
	}
	adapter := &convertedAttempt{
		s:              r,
		c:              output,
		provider:       target,
		source:         source,
		requestBodyLen: len(body),
		state:          &AttemptState{PrepareStartedAt: time.Now()},
	}
	in := forward.ConversionInput{
		OAuth:   target != nil && target.View().IsOAuth(),
		Bedrock: target != nil && target.View().IsBedrock(),
	}
	result, err := forward.AsConverted(ctx, source, adapter, in, body)
	return messagesResult(result), err
}

// messagesResult returns a new outer MessagesResult value when a forwarding
// result exists. Reference-valued fields remain shared with the source result;
// nil input returns nil for absent observations.
func messagesResult(result *forward.Result) *forward.MessagesResult {
	if result == nil {
		return nil
	}
	copy := forward.MessagesResult(*result)
	return &copy
}
