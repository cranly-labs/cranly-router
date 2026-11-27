package httpapi

import (
	"context"

	"github.com/gin-gonic/gin"

	"github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayadapter "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/upstream/qoder"
)

// qoder_attempt.go executes Qoder attempts and retains usage-bearing partial
// results.
// A native Qoder attempt can fail after serving measurable usage. The HTTP
// adapter returns that partial result only when execution reports both Served
// and HasUsage; a failure with either observation absent returns the error
// alone. The outer request loop decides refresh, provider switching and
// completion from this qualified result.
//
// Converted client output uses the first nonblank response-model override or
// the trimmed body model when no override exists. Request metadata clones
// headers and records detected client identity before target preparation,
// keeping this attempt's input available to platform execution. Result fields
// carry observed tokens, duration and disconnect state into completion, with
// no synthesized first-token timing.

// forwardQoderAttempt executes one selected Qoder target, binds gateway
// terminal SSE error callbacks onto Target, and writes through the current Gin
// response. Setting deferRetryableCommitted leaves pre-semantic refreshable or
// switchable errors unwritten on keepalive-committed SSE streams so
// UnifiedTextExecutor can run same-target credential refresh and outer provider
// failover first.
//
// The first nonblank explicit response model wins, falling back to the trimmed
// body model; errors are observed and retain a partial result only when
// execution reports both Served and HasUsage.
// @project-doc docs/architecture/gateway_request_lifecycle.md#qoder_gateway_execution
func forwardQoderAttempt(
	ctx context.Context,
	c *gin.Context,
	runtime *gatewayadapter.QoderRuntime,
	value *provider.Record,
	body []byte,
	wire protocol.ProtocolID,
	deferRetryableCommitted bool,
	responseModels ...string,
) (*forward.MessagesResult, error) {
	responseModel := qoder.FirstNonEmptyQoder(responseModels...)
	// Preserve an explicit response-model override when supplied; body identity is
	// the fallback so converted output can still name the requested client model.
	if responseModel == "" {
		responseModel, _ = qoder.ExtractTopLevelModelJSON(body)
	}
	executor, input := runtime.PrepareQoderTarget(QoderRequestMetadata(c), value, body, wire, responseModel)
	if target, ok := input.Target.(*qoder.Target); ok && target != nil {
		target.CommittedSSE = func() bool {
			return CommittedSSE(c)
		}
		target.DeferRetryableCommittedError = deferRetryableCommitted
		target.WriteLocalTerminalError = func(status int, errType, code, message string) {
			writeQoderCommittedTerminalError(c, wire, status, errType, code, message, responseModel)
		}
		target.WriteRawTerminalSSE = func(data []byte) bool {
			if c == nil {
				return false
			}
			return emitLocalTerminalSSE(c, data)
		}
	}
	result, err := executor.Execute(ctx, input, NewResponseSink(c))
	if err != nil {
		runtime.ObserveQoderFailure(ctx, value, err)
		// Without both service and usage evidence, expose only the error to outer
		// orchestration rather than a completion result that could record unobserved work.
		if !result.Served || !result.HasUsage {
			return nil, err
		}
	}
	return &forward.MessagesResult{
		RequestID:             result.RequestID,
		UpstreamHeaders:       result.UpstreamHeaders,
		Model:                 result.Model,
		UpstreamModel:         result.UpstreamModel,
		UpstreamResponseModel: result.UpstreamResponseModel,
		Usage:                 result.Usage,
		Stream:                result.Stream,
		Duration:              result.Duration,
		ClientDisconnect:      result.ClientDisconnect,
	}, err
}

// writeQoderCommittedTerminalError emits an untimed terminal SSE error frame in
// the inbound client wire protocol when HTTP 200 text/event-stream headers were
// already committed before Qoder produced semantic output. Nil contexts or
// streams that already delivered a terminal SSE frame return immediately, and
// every emitted frame marks the Gin response committed so outer attempt loops
// skip duplicate error fallbacks and local-capability health penalties.
func writeQoderCommittedTerminalError(
	c *gin.Context,
	wire protocol.ProtocolID,
	status int,
	errType string,
	code string,
	message string,
	fallbackModel string,
) {
	if c == nil || TerminalSSEDelivered(c) {
		return
	}
	switch wire {
	case protocol.ProtocolAnthropicMessages:
		DefaultOpenAIErrorOutput().WriteAnthropicStreamingError(c, status, errType, message, true)
	case protocol.ProtocolOpenAIResponses:
		status, message = modelAttributeError(c, status, message)
		MarkOpsStreamError(c, errType, message, status)
		model := qoder.FirstNonEmptyQoder(ErrorRequestModel(c), fallbackModel)
		if !WriteResponsesFailedSSE(c, errType, code, message, ErrorRequestID(c), model) {
			writeOpenAIGenericErrorSSE(c, errType, code, message)
		}
	default:
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, errType, code, message, true, false)
	}
	MarkResponseCommitted(c)
}

// QoderRequestMetadata snapshots request headers and detected client identity
// for one Qoder attempt. Headers are cloned when a request exists; missing
// request state leaves them absent, while the API-key identity reader supplies
// its own fallback.
func QoderRequestMetadata(c *gin.Context) qoder.RequestMetadata {
	result := qoder.RequestMetadata{APIKeyID: APIKeyIDFromContext(c)}
	if c != nil && c.Request != nil {
		result.Headers = c.Request.Header.Clone()
		result.ClaudeCode = requeststate.IsClaudeCodeClient(c.Request.Context())
	}
	return result
}
