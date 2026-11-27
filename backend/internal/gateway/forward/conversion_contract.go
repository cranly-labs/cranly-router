package forward

import (
	"context"
	"encoding/json"
	"time"

	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
)

// conversion_contract.go defines adapter operations and upstream response
// handles for sending OpenAI client requests through Anthropic, Vertex, and
// Bedrock Messages targets. Conversion always requests an upstream stream, then
// selects event output or buffered JSON using the client's original stream
// setting while delegating transport, credentials, and health checks to ports.

// ConversionInput selects provider preparation rules before the converted request is exchanged.
type ConversionInput struct {
	OAuth   bool // OAuth mimic eligibility
	Bedrock bool // AWS Bedrock route and EventStream mode
}

// ErrorDecision holds provider policy choices consumed before client error display.
type ErrorDecision struct {
	// Generic selects the generic envelope before recovery in conversion/count
	// handling; Failover permits recovery, and RetrySameProvider accompanies its
	// signal. Status-qualified Messages HTTP paths use their own failover predicate.
	Generic, Failover, RetrySameProvider bool // generic error selection; provider failover permission; same-provider retry permission
}

// Response holds upstream HTTP response metadata and the transport close callback.
type Response struct {
	StatusCode int // upstream HTTP status
	// Close releases the exchange response resource. Successful exchange adapters
	// provide this callback because conversion defers it before consuming the response.
	Close     func()
	RequestID string // upstream exchange request identifier
	// Headers contains direct upstream response metadata read by output adapters.
	// Copying Response retains its map and slices, so mutation needs an independent clone.
	Headers map[string][]string
}

// ConvertedMessagesPorts supplies IR dialect resolution, request preparation,
// signature recovery, and merged response reading for AsConverted.
type ConvertedMessagesPorts interface {
	// NormalizeResponses applies optional legacy Responses ingress normalization
	// and reports whether returned bytes replaced the supplied body. AsConverted
	// calls it only on ProtocolOpenAIResponses input and stops before dialect
	// resolution when an error is returned.
	NormalizeResponses([]byte) ([]byte, bool, error)
	// ResolveModel resolves provider model mapping, Vertex date formatting, or
	// Bedrock regional inference routing for the supplied logical model. A non-nil
	// error stops conversion without contacting upstream or invalidating credentials,
	// whereas an empty string with nil error retains the caller's logical model.
	ResolveModel(context.Context, string) (string, error)
	// ResolveDialect resolves model capabilities and execution rules for the
	// mapped model and client bytes, returning the target convert.Dialect and
	// passback mode. Attribute lookup errors abort conversion before encoding.
	ResolveDialect(context.Context, string, []byte) (convert.Dialect, bool, error)
	// ConversionRuntime returns the wall-clock and cryptographic ID runtime passed
	// to convert.NewExchange for this conversion attempt.
	ConversionRuntime() convert.Runtime
	// Effort extracts recorded OpenAI reasoning effort from client bytes for the
	// first nonblank model candidate. The boolean distinguishes Chat top-level
	// aliases from nested Responses reasoning configuration.
	Effort([]byte, bool, ...string) *string
	// ThinkingFallback retains a present effort pointer and otherwise derives
	// display effort when model-specific thinking is enabled. Absent thinking
	// configuration leaves a nil effort pointer nil.
	ThinkingFallback(*string, []byte, string) *string
	// ModelNotice logs logical and mapped model identities alongside the client
	// streaming flag for diagnostic tracing.
	ModelNotice(string, string, string, bool)
	// Mimic applies Anthropic OAuth system, metadata, cache, and tool-name
	// preparation to converted Messages bytes when OAuth mimicry is enabled.
	// Implementations retain tool-name restoration state for ReadConverted.
	Mimic(context.Context, []byte, json.RawMessage, string) []byte
	// CacheLimit enforces Anthropic total cache-control breakpoint limits after
	// optional OAuth mimicry and before Build injects one-hour cache lifetimes.
	// In-limit payloads return unchanged, and returned bytes feed Build directly.
	CacheLimit([]byte) []byte
	// Credential resolves and stores provider authentication (OAuth, Setup Token,
	// API Key, Vertex token, or Bedrock SigV4/API-key credentials) before Build.
	// Resolution errors stop conversion before building or sending the request.
	Credential(context.Context) error
	// Build applies final model-attribute parameters, wire effort stripping, and
	// 1h cache TTL injection before constructing the Anthropic, Vertex, or Bedrock
	// HTTP request. Returned wire bytes supply fast-tier speed attribution only
	// when the target platform supports Anthropic fast mode.
	Build(context.Context, []byte, string, bool, bool) ([]byte, error)
	// Send dispatches the request retained by Build and returns response status
	// and cleanup handles. Transport errors close any returned body before
	// returning so callers own Close only on nil error.
	Send(context.Context) (Response, error)
	// ReadErrorBody captures bounded upstream error bytes, closes the network
	// reader, and installs a replayable body reader. Health classification and
	// diagnostics inspect the same captured slice even after partial read errors.
	ReadErrorBody() ([]byte, error)
	// ErrorMessage extracts and query-sanitizes the upstream error message from
	// captured HTTP error bytes.
	ErrorMessage([]byte) string
	// IsSignatureError reports whether an upstream HTTP 400 error body represents
	// a rejected thinking signature or malformed reasoning block. Matching
	// responses trigger at most one lossless signature-stripping retry on the same provider.
	IsSignatureError(context.Context, []byte) bool
	// ObserveSignatureRetry records diagnostic telemetry when an HTTP 400
	// signature rejection triggers a lossless stripped-thinking retry on the same provider.
	ObserveSignatureRetry(int, []byte, string)
	// Health evaluates provider error policy from upstream status, captured error
	// bytes, and mapped model. Returned Generic, Failover, and RetrySameProvider
	// flags govern final client error or outer provider switching.
	Health(context.Context, int, []byte, string) ErrorDecision
	// FailoverNotice records provider failover telemetry before returning a
	// recovery error to the outer provider selection loop.
	FailoverNotice(int, string)
	// FailoverError constructs a gateway UpstreamFailoverError carrying source
	// status, captured error bytes, and same-provider retry eligibility. Outer
	// attempt loops inspect this signal to select a replacement provider.
	FailoverError(int, []byte, bool) error
	// WriteClientError presents an unrecoverable HTTP error in the active client
	// protocol, promoting to an untimed terminal SSE error when CommittedSSE is
	// true. Uncommitted responses mark semantic commitment before writing JSON.
	WriteClientError(int, string, string)
	// ReadConverted reads the accepted upstream response stream through the shared
	// merged streaming or buffered converter and returns usage and model
	// observations. Partial usage observations can accompany returned stream errors.
	ReadConverted(context.Context, *convert.Exchange, bool, string, string, *string, time.Time) (*Result, error)
}

// MapStatus returns 502 for source statuses at least 500 and otherwise retains
// the supplied status. Conversion uses this for final client error output after
// provider health and recovery have used the original upstream status.
func MapStatus(status int) int {
	// Present server failures as gateway errors after health policy has used
	// the source status, keeping client display separate from that recovery decision.
	if status >= 500 {
		return 502
	}
	return status
}
