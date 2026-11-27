package qoder

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/TokenFlux/TokenRouter/internal/pkg/requestcontext"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// execute.go executes Qoder requests with conversation planning, credential
// acquisition, protocol conversion, and completion evidence.
// Qoder execution converts three client protocols into one signed upstream
// stream while retaining conversation identity. The original request context
// controls preparation and credential waits. Streaming inference uses a
// detached context with a separate execution budget so trailing usage can
// arrive after client disconnection.
//
// Transport acceptance publishes a versioned history snapshot. Stream or
// conversion failures roll it back and return partial evidence when output and
// usage were both observed. Completed non-streaming work and streams reporting
// output commit history after conversion. The output observer records semantic timing before
// writes and marks disconnects, letting completion distinguish upstream
// service from downstream delivery. The request owner releases provider and
// user leases.

// StreamClient sends the prepared inference request and returns its live
// response body.
type StreamClient interface {
	// StreamRequestContext sends an inference request using the supplied context
	// and session. An empty path selects the generation endpoint in Client; callers
	// consume and close successful response bodies through their stream reader.
	StreamRequestContext(context.Context, *SessionContext, string, []byte, map[string]string) (*http.Response, error)
}

// streamClientWithDoer supports provider-specific transport selection for an
// inference request.
type streamClientWithDoer interface {
	// StreamRequestContextWithDoer sends inference through a caller-selected
	// request transport. Callers consume and close successful response bodies; the
	// supplied doer carries the provider proxy and TLS selection for this attempt.
	StreamRequestContextWithDoer(context.Context, *SessionContext, string, []byte, map[string]string, RequestDoer) (*http.Response, error)
}

// Target binds site metadata and provider callbacks needed by one Qoder attempt.
type Target struct {
	// Attributes resolves the mapped route against its captured account catalog.
	// Nil retains execution defaults without inferring site-wide model capabilities.
	Attributes func(context.Context, string, protocol.ProtocolID) (modelrules.Resolution, error) // route attribute resolver
	ProviderID int64                                                                             // selected provider identifier
	Site       Site                                                                              // selected credential site
	Metadata   RequestMetadata                                                                   // inbound session metadata
	// Session obtains credentials with the original request context. The callback
	// is required and cancellation can stop the wait before inference starts.
	Session func(context.Context) (*SessionContext, error) // credential session getter
	// Client constructs the inference transport after credentials are ready. The
	// required callback returns a usable StreamClient on success; its error
	// prevents upstream dispatch.
	Client func() (StreamClient, error) // inference client factory
	// Doer supplies the provider transport when Client implements
	// streamClientWithDoer. Other clients use StreamRequestContext and the
	// transport retained by their client implementation.
	Doer RequestDoer // provider request transport

	// IngressError carries any pre-conversion legacy ingress normalization failure
	// recorded before Execute so IR conversion reports it as a local RequestError.
	IngressError error
	// Dialect resolves the protocol/convert Dialect for a validated request model
	// and client wire protocol. Nil uses default ProtocolQoderChat dialect
	// capabilities.
	Dialect func(context.Context, string, protocol.ProtocolID) (convert.Dialect, error)
	// CommittedSSE reports whether HTTP 200 text/event-stream headers were already
	// committed before semantic output began, such as by a waiting keepalive beat.
	CommittedSSE func() bool
	// DeferRetryableCommittedError suppresses immediate terminal SSE emission inside
	// ReadConvertedStreaming on pre-semantic refreshable or switchable errors when
	// outer gateway orchestration owns same-target refresh and provider failover.
	DeferRetryableCommittedError bool
	// WriteLocalTerminalError writes an untimed terminal SSE error frame in the
	// inbound client protocol when pre-semantic failure occurs on a committed SSE stream.
	WriteLocalTerminalError func(status int, errType, code, message string)
	// WriteRawTerminalSSE delivers pre-encoded terminal SSE error bytes produced by
	// convert.Stream.Close through the gateway's untimed terminal SSE barrier.
	WriteRawTerminalSSE func(data []byte) bool
}

// ExecuteOptions supplies shared conversation storage, timeout, and attempt
// activity registration.
type ExecuteOptions struct {
	// Conversations is shared across attempts to match full-history fingerprints. Nil in NewExecutor allocates a store with the default TTL.
	Conversations *QoderConversationStore // shared conversation store
	// Timeout bounds the execution context created before protocol parsing.
	// Credential waits still use the original request context, so preparation can
	// consume this upstream budget before dispatch.
	Timeout time.Duration // upstream execution duration
	// Enter registers active execution. A successful call supplies a nonnil
	// release function that Execute invokes on exit; a nil Enter callback skips
	// activity registration.
	Enter func() (func(), error) // attempt activity registrar
}

// Executor converts client requests and coordinates upstream output with
// conversation state.
type Executor struct {
	// conversations retains the store selected by NewExecutor. Concurrent requests
	// coordinate fingerprint and usage observations through that store's mutex and snapshot rules.
	conversations *QoderConversationStore // shared conversation store
	timeout       time.Duration           // upstream execution duration
	// enter retains the activity callback selected at construction. Execute invokes
	// it before preparation and releases the returned activity registration when
	// the attempt exits.
	enter func() (func(), error) // attempt activity registrar
}

// TargetID returns the provider identifier carried by this attempt target.
// A nil target returns zero so diagnostic formatting can identify absent
// configuration before execution validates it.
func (t *Target) TargetID() int64 {
	if t == nil {
		return 0
	}
	return t.ProviderID
}

// String formats the target's provider identifier for diagnostic output and
// logging. A nil receiver uses TargetID's zero result, allowing failed
// configuration paths to format the target safely.
func (t *Target) String() string { return fmt.Sprintf("qoder target provider=%d", t.TargetID()) }

// NewExecutor creates an executor with conversation storage and an execution
// timeout. Nil storage allocates the default store, nonpositive timeout selects
// QoderStreamTimeout, and the optional Enter callback is retained for attempt
// activity registration.
func NewExecutor(options ExecuteOptions) *Executor {
	if options.Conversations == nil {
		options.Conversations = NewQoderConversationStore(QoderConversationTTL)
	}
	if options.Timeout <= 0 {
		options.Timeout = QoderStreamTimeout
	}
	return &Executor{conversations: options.Conversations, timeout: options.Timeout, enter: options.Enter}
}

// PrepareConversation builds a payload and a conversation plan for one selected
// target. Target site replaces request metadata before session-key
// selection. Clients send complete input history with each request. The caller publishes acceptance and completion
// after transport and output outcomes are known.
func (e *Executor) PrepareConversation(target *Target, wireProtocol protocol.ProtocolID, request QoderPayloadRequest) (map[string]any, string, *QoderConversationPlan, error) {
	request.Site = target.Site
	request, err := request.withEffectiveControls()
	if err != nil {
		return nil, "", nil, err
	}
	key, source := QoderConversationKey(target.Metadata, target.ProviderID, request)
	plan := e.conversations.Plan(key, request.System, request.Tools, request.Messages)
	payload, model := buildQoderPayload(request, plan.SessionID, plan.MessagesToSend, plan.IncludeSystem, plan.IncludeTools)
	plan.Log(target.Metadata, target.ProviderID, string(wireProtocol), request.Model, source, request, payload)
	return payload, model, plan, nil
}

// Execute serves one Qoder attempt and returns observed service and usage
// evidence. The caller supplies a configured Target and usable synchronous
// sink. Preparation and credential waits observe the original context;
// streaming inference detaches parent cancellation while retaining request
// values and the internal abort signal. The execution timeout starts before
// parsing and bounds the upstream context.
//
// A returned response publishes conversation acceptance. Stream failures roll
// it back and return partial evidence when the converter observed both output
// and usage. Completed non-streaming work stays served when the sink fails.
// History commits for non-streaming completion or when a stream converter
// reports output; Responses IDs identify the current result. The execution owner closes the response through
// stream reading, while the request owner releases provider and user leases.
// @project-doc docs/interfaces/qoder_upstream.md#qoder_execution_boundary
func (e *Executor) Execute(ctx context.Context, input upstream.AttemptInput, sink upstream.OutputSink) (result upstream.AttemptResult, executionErr error) {
	defer func() { result.Finish(ctx, executionErr) }()
	if e.enter != nil {
		done, err := e.enter()
		if err != nil {
			return upstream.AttemptResult{}, err
		}
		defer done()
	}
	start := time.Now()
	observed := &executionOutput{sink: sink}
	sink = observed
	defer func() {
		result.ClientDisconnect = result.ClientDisconnect || observed.disconnected

		if ctx != nil && ctx.Err() != nil {
			result.Cancelled = true
			result.ClientDisconnect = result.ClientDisconnect || result.Stream
		}
		if executionErr != nil {
			switch {
			case IsRequestError(executionErr):
				result.FailureClass = "invalid_request"
			case errors.Is(executionErr, context.Canceled):
				result.FailureClass = "cancelled"
			case errors.Is(executionErr, context.DeadlineExceeded):
				result.FailureClass = "timeout"
			default:
				result.FailureClass = "upstream"
			}
		}
	}()
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return upstream.AttemptResult{}, err
	}
	target, ok := input.Target.(*Target)
	if !ok || target == nil || target.Session == nil || target.Client == nil {
		return upstream.AttemptResult{}, errors.New("qoder gateway service is not configured")
	}
	executionCtx := ctx
	// A disconnected client can precede the final usage event. Detach its stop
	// signal while retaining the server abort signal, then bound collection with
	// this timeout.
	if input.Stream {
		executionCtx = requestcontext.Detach(ctx)
	}
	executionCtx, cancel := context.WithTimeout(executionCtx, e.timeout)
	defer cancel()
	return e.executeConverted(ctx, executionCtx, start, target, input, sink)
}
