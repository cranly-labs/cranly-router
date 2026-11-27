package openaiforward

import (
	"context"
	"net/http"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// responses_converted_ports.go defines provider capability snapshots, template
// inputs, and client-protocol-parameterized ports consumed by RunConvertedResponses.
// Merging Messages-to-Responses and Chat-to-Responses execution behind a single
// conversion runner keeps dialect resolution, lossless prompt-cache seeding,
// Codex OAuth adaptation, Grok cache routing, and merged response reading on
// one interface without coupling to legacy subset request structs.

type (
	// TemplateData supplies current instructions and model identities to forced template rendering.
	TemplateData struct {
		// ExistingInstructions is the current instruction text; OriginalModel is the
		// client model; NormalizedModel is the pre-mapping Messages model;
		// BillingModel is the charge model; UpstreamModel is the vendor model.
		ExistingInstructions, OriginalModel, NormalizedModel, BillingModel, UpstreamModel string
	}
	// MessagesProfile identifies provider credentials, shadow mode, and continuation capability for conversion.
	MessagesProfile struct {
		Profile       // platform and credential capabilities
		ID      int64 // selected provider identifier
		// GrokOAuth identifies Grok OAuth credentials; Shadow identifies a shadow
		// provider; ContinuationSupported records saved Responses continuation support.
		GrokOAuth, Shadow, ContinuationSupported bool
	}
	// ChatProfile combines selected provider capabilities with resolved Chat dispatch observations.
	ChatProfile struct {
		MessagesProfile // selected provider capability snapshot
		// resolved upstream protocol identifier
		Protocol protocol.ProtocolID
		// Adaptive selects request-shape dispatch; NativeResponsesSupported permits
		// Responses-shaped input without Chat conversion. ProtocolSlotDefaults keeps
		// endpoint selection with configured slots instead of the generic Chat default.
		// OpenAIAPIKey identifies the OpenAI credential path used for cache isolation.
		Adaptive, NativeResponsesSupported, ProtocolSlotDefaults, OpenAIAPIKey bool
	}
)

// ConvertedResponsesPorts supplies provider preparation, IR dialect resolution,
// transport execution, and merged response-reading callbacks for one attempt.
type ConvertedResponsesPorts interface {
	// PrepareMessages resolves the selected provider attempt, resets request
	// observations, and returns its Messages capability profile and dispatch
	// target. Identity or route errors stop execution before parsing client bytes.
	PrepareMessages(context.Context) (MessagesProfile, Dispatch, error)
	// DispatchMessages delegates non-Responses Messages routes using the raw
	// client body, prompt-cache seed, and fallback model. Returned results and
	// errors pass directly back to the outer caller.
	DispatchMessages(context.Context, Dispatch, []byte, string, string) (*Result, error)
	// PrepareChat resolves the selected provider attempt, resets request
	// observations, and returns its Chat capability and protocol profile.
	// Identity preparation errors abort the attempt before body inspection.
	PrepareChat(context.Context) (ChatProfile, error)
	// DispatchChat delegates non-Responses Chat routes using the prepared
	// request bytes, prompt-cache seed, and fallback model. Returned results and
	// errors pass directly back to the outer caller.
	DispatchChat(context.Context, Dispatch, []byte, string, string) (*Result, error)
	// DefaultChat reports whether dynamic Chat attempt policy selects raw Chat execution.
	DefaultChat() bool
	// ClientAllowed evaluates Codex official-client restrictions on Chat ingress requests.
	ClientAllowed(ctx context.Context, body []byte) bool
	// PolicyDenied records local business-policy denial telemetry in request Ops.
	PolicyDenied()
	// Reject writes an uncommitted OpenAI rejection response on Chat ingress.
	Reject(status int, kind, message string)
	// SourceError marks response commitment and writes a local error in source's
	// client protocol envelope. Already committed SSE streams receive an untimed
	// terminal SSE error event instead of raw JSON.
	SourceError(source protocol.ProtocolID, status int, kind, message string)

	// BillingModel selects the charge model from requested and group-mapped names.
	BillingModel(model, fallback string) string
	// UpstreamModel normalizes the billing model for the selected upstream provider.
	UpstreamModel(model string) string
	// APIKeyID returns the authenticated downstream API key identifier for isolation.
	APIKeyID() int64
	// IsolateCacheKey namespaces a derived Chat prompt-cache key by API key ID.
	IsolateCacheKey(key string) string
	// ClaudeSession extracts Claude Code session identity from request headers or body.
	ClaudeSession(body []byte) string
	// FindDigestKey looks up a reusable prompt-cache key by Anthropic digest prefix.
	FindDigestKey(key int64, chain string) (string, string)
	// ContinuationEnabled reports whether automatic previous_response_id reuse is active.
	ContinuationEnabled(model string) bool
	// ResponseID reads any saved previous_response_id for key from process-local
	// continuation state. Missing, blank, or expired session bindings return an
	// empty string without mutating stored turn-state metadata.
	ResponseID(ctx context.Context, key string) string
	// ContinuationDisabled reports whether previous_response_id reuse was disabled
	// for key in process-local session state. Missing or expired entries return
	// false so automatic continuation remains eligible.
	ContinuationDisabled(ctx context.Context, key string) bool

	// ResolveResponsesDialect resolves model capabilities from body and builds the
	// target convert.Dialect for the selected provider and Responses upstream.
	// Successful capability lookup updates attempt provider state before returning.
	ResolveResponsesDialect(model string, body []byte) (convert.Dialect, error)
	// ConversionRuntime returns the wall-clock and random ID source for convert.Exchange.
	ConversionRuntime() convert.Runtime
	// ConvertCursorResponsesToChat resolves target Chat model rules and converts an
	// input-shaped Cursor Chat request into Chat Completions JSON bytes via Exchange.
	// Syntax or capability errors return before delegating to the target route.
	ConvertCursorResponsesToChat(body []byte) ([]byte, error)

	// BetaFast reports whether inbound Anthropic beta headers requested fast mode.
	BetaFast() bool
	// MessagesEffortValue reconciles explicit Messages max effort with catalog support.
	MessagesEffortValue(clientEffort, model, convertedEffort string) string
	// ToolNameReverse publishes copied reverse Codex tool aliases into request
	// state for response restoration and clears stale session entries. Callers
	// keep value immutable while copying completes.
	ToolNameReverse(value map[string]string)
	// ForcedTemplate returns the configured Messages-to-Codex instruction template text,
	// or an empty string when template injection is disabled.
	ForcedTemplate() string
	// ForcedInstructions renders forced Codex instructions into a writable request
	// map and reports whether text was installed. Template rendering errors abort
	// request preparation before outgoing serialization.
	ForcedInstructions(body map[string]any, text string, data TemplateData) (bool, error)
	// ProviderIdentity injects Codex provider namespace and API key metadata into
	// a writable request map before serialization. Subsequent session header
	// builders read the same prepared attempt identity.
	ProviderIdentity(body map[string]any, key int64)
	// TurnState reads saved opaque x-codex-turn-state metadata for key from session state.
	TurnState(ctx context.Context, key string) string
	// ApplyEffort applies admitted group reasoning-effort caps to outgoing
	// Messages-bridge request bytes. Over-limit errors mark local policy denial,
	// write an Anthropic 403 rejection, and abort request forwarding.
	ApplyEffort(ctx context.Context, body []byte) ([]byte, bool, error)
	// ApplyFast applies provider fast-tier policy to outgoing Messages-bridge
	// request bytes. Blocked tiers mark local policy denial, write an Anthropic
	// 403 rejection, and return the policy error.
	ApplyFast(ctx context.Context, model string, body []byte) ([]byte, error)
	// ApplyChatFast applies provider fast-tier policy to outgoing Chat-bridge
	// request bytes. Blocked tiers mark local policy denial, write a Chat 403
	// rejection, and return the policy error.
	ApplyChatFast(ctx context.Context, model string, body []byte) ([]byte, error)
	// ResolvedServiceTier resolves the final completion service tier pointer from observations.
	ResolvedServiceTier(tier *string) *string

	// GrokCacheIdentity derives scoped Grok prompt-cache identity for Messages bridges.
	GrokCacheIdentity(body []byte, key, model string) string
	// PatchGrokBody applies the selected Grok model's Responses compatibility rules
	// to outgoing request bytes. Validation or serialization errors return before
	// credentials or upstream HTTP requests are constructed.
	PatchGrokBody(body []byte, model string) ([]byte, error)
	// GrokFreeToolRoute applies Free-tier Grok native search companion cache
	// routing to body using retained tool intent. Ineligible providers or tool
	// groups return body unchanged with a nil error.
	GrokFreeToolRoute(body, intent []byte, key string) ([]byte, error)

	// Credential resolves upstream authentication credentials for Messages attempts
	// under the active request credential budget. Token refresh or lookup failures
	// return before building the outbound HTTP request.
	Credential(ctx context.Context) (string, error)
	// AccessToken resolves upstream authentication credentials for Chat attempts
	// against the selected provider record. Credential lookup or refresh failures
	// return immediately before building the outbound HTTP request.
	AccessToken(ctx context.Context) (string, error)
	// BindMessagesBridge records an active Messages-to-Codex bridge marker on the
	// request context when v is true. Passing false leaves existing true markers
	// on the request context intact.
	BindMessagesBridge(v bool)
	// Build constructs the outgoing Responses HTTP request for Messages attempts
	// using prepared bytes, credentials, and session identities. The leading
	// context parameter is unused while ctx governs upstream cancellation.
	Build(_ context.Context, ctx context.Context, body []byte, token string, stream bool, key, grokIdentity string) (*http.Request, error)
	// BuildChat constructs a streaming Responses HTTP request for Chat attempts
	// using detached upstream context, prepared bytes, and credentials. Header
	// construction borrows request-local state synchronously during invocation.
	BuildChat(ctx context.Context, body []byte, token, key string) (*http.Request, error)
	// IsolatedSessionID formats a deterministic UUID session_id header for Messages.
	IsolatedSessionID(key int64, cache string) string
	// UpstreamSessionKey formats the isolated session seed hashed for Chat session_id.
	UpstreamSessionKey(id int64, key string) string
	// Header reads an inbound client HTTP header value by name from the request.
	Header(key string) string

	// PrepareTransport initializes proxy routing state for the selected provider attempt.
	PrepareTransport()
	// Send resets response-model observation and executes r through the provider
	// transport with concurrency and TLS controls. Callers invoke PrepareTransport
	// beforehand and close successful response bodies after reading finishes.
	Send(r *http.Request) (*http.Response, error)
	// TransportError records sanitized transport failure diagnostics in Ops and
	// returns provider failover or client cancellation guidance. Client-canceled
	// contexts preserve their cause without triggering provider penalties.
	TransportError(ctx context.Context, err error) error
	// ReadErrorBody reads bounded upstream error bytes while suppressing transport
	// read errors. Callers close or replace r.Body after inspecting the captured
	// error payload for retry eligibility.
	ReadErrorBody(r *http.Response) []byte
	// ReadUpstreamError captures bounded upstream error bytes, closes the consumed
	// network body, and installs a replay reader on r. Returned message strings
	// are query-sanitized for downstream failure classification.
	ReadUpstreamError(r *http.Response) ([]byte, string)
	// IsAgentIdentity reports whether the provider uses recoverable Agent Identity tasks.
	IsAgentIdentity(ctx context.Context) bool
	// RecoverAgentTask refreshes an invalid Agent Identity task credential on the
	// selected provider before retry. Callers guard recursion with request-context
	// state so task recovery runs at most once per request.
	RecoverAgentTask(ctx context.Context) error
	// RedactErrorBody scrubs sensitive provider identifiers from captured upstream error bytes.
	RedactErrorBody(ctx context.Context, body []byte) []byte
	// DisableContinuation marks previous_response_id continuation unsupported for
	// key in process-local session state before full-history replay. Existing
	// opaque turn-state metadata remains available under the renewed TTL.
	DisableContinuation(ctx context.Context, key string)
	// DeleteResponseID removes a stale saved previous_response_id entry for key
	// before rebuilding full request history. Opaque turn-state metadata and
	// disablement flags in the same session entry remain intact.
	DeleteResponseID(ctx context.Context, key string)
	// FailoverHTTP evaluates provider health policy on HTTP >= 400 upstream
	// responses. Non-nil errors request provider switching in the outer loop,
	// while nil keeps the response for terminal client error formatting.
	FailoverHTTP(ctx context.Context, r *http.Response, body []byte, msg, model string) error
	// SourceErrorResponse presents terminal HTTP >= 400 upstream rejections in
	// source's client error format using the replayable body on r. Callers keep
	// ownership of closing r.Body after handling finishes.
	SourceErrorResponse(source protocol.ProtocolID, r *http.Response, model string) (*Result, error)

	// UpdateGrokUsage records Grok OAuth quota headers on HTTP 2xx responses.
	UpdateGrokUsage(ctx context.Context, model string, h http.Header, status int)
	// BindTurnState stores non-blank opaque x-codex-turn-state header text for
	// key in process-local continuation storage and renews its expiration window.
	// Blank state values are ignored by the caller before binding.
	BindTurnState(ctx context.Context, key, state string)
	// Sink returns the synchronous downstream OutputSink for response writing.
	Sink() upstream.OutputSink
	// ConvertedResponseOptions builds client-protocol-aware callbacks for
	// ReadConvertedStreaming and ReadConvertedBuffered on the live HTTP response.
	// Callers retain request state and close r.Body after reading returns.
	ConvertedResponseOptions(r *http.Response, source protocol.ProtocolID, original, billing, model string, requestBodyLen int) openai.ConvertedResponseOptions
	// CyberPolicy reports whether upstream cyber-policy rejection was recorded on this attempt.
	CyberPolicy() bool
	// BindResponseID saves a completed response ID for subsequent continuation turns.
	BindResponseID(ctx context.Context, key, id string)
	// BindDigestKey binds an Anthropic digest prefix chain to promptCacheKey in cache storage.
	BindDigestKey(id int64, chain, key, matched string)
	// UpdateCodexUsage records Codex OAuth rate-limit headers after successful completion.
	UpdateCodexUsage(ctx context.Context, h http.Header)
}
