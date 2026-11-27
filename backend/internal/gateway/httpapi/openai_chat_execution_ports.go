package httpapi

import (
	"context"
	"errors"
	"net/http"

	"github.com/TokenFlux/TokenRouter/internal/egress"
	protocolforward "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	openaiexecution "github.com/TokenFlux/TokenRouter/internal/gateway/provider/openaiforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/tierpolicy"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
)

// openai_chat_execution_ports.go binds selected Chat attempts to protocol
// dispatch, client policy, and request preparation callbacks.
// Chat requests can execute as native Chat, converted Responses, Anthropic, or
// Grok depending on the selected attempt profile. The adapter keeps that
// attempt's provider and request state together while the shared runner
// chooses a route and owns the network-response lifetime.

// openAIChatExecutionAdapter binds Chat routing to the shared Messages attempt
// state.
type openAIChatExecutionAdapter struct {
	// openAIMessagesExecutionAdapter shares the current attempt provider, context
	// and services with RunConvertedResponses; mutations during preparation remain visible here.
	*openAIMessagesExecutionAdapter
}

// PrepareChat resolves the attempt provider and resets model, endpoint and
// tool observations before preparing Codex identity. The resolved provider
// replaces adapter state even if later identity preparation fails; callers use
// the returned profile for dispatch.
func (p *openAIChatExecutionAdapter) PrepareChat(ctx context.Context) (openaiexecution.ChatProfile, error) {
	provider, err := gatewayprovider.ProviderForProtocolAttempt(ctx, p.provider)
	if err != nil {
		return openaiexecution.ChatProfile{}, err
	}
	p.provider = provider
	BeginUpstreamResponseModelObservation(p.c)
	ClearActualOpenAIUpstreamEndpoint(p.c)
	if gatewayprovider.ExecutionModelPolicy(provider).RawChat() {
		SetActualOpenAIUpstreamEndpoint(p.c, "/v1/chat/completions")
	}
	SetCodexToolNameReverse(p.c, nil)
	if _, err := PrepareCodexIdentity(ctx, p.c, p.s.Requests.Providers, provider); err != nil {
		return openaiexecution.ChatProfile{}, err
	}
	return openaiexecution.ChatProfile{MessagesProfile: openaiexecution.MessagesProfile{Profile: openAIForwardProfile(provider), ID: provider.Record.ID, GrokOAuth: provider.View().IsGrokOAuth(), Shadow: provider.View().IsShadow()}, Protocol: provider.Route.Protocol(), Adaptive: gatewayprovider.ExecutionProtocolTarget(provider).IsAdaptiveAPIProtocol(), NativeResponsesSupported: provider.View().SupportsAPIKeyResponses(), ProtocolSlotDefaults: provider.View().HasProtocolEndpointDefaults(), OpenAIAPIKey: provider.View().IsOpenAIApiKey()}, nil
}

// DispatchChat delegates the selected route and converts its forward result
// for RunConvertedResponses. An unhandled Grok conversion with no error falls back to
// RawChat using the same body, model, key and TLS selection.
func (p *openAIChatExecutionAdapter) DispatchChat(ctx context.Context, route openaiexecution.Dispatch, body []byte, key, model string) (*openaiexecution.Result, error) {
	var r *protocolforward.OpenAIResult
	var err error
	switch route {
	case openaiexecution.DispatchAnthropic:
		r, err = p.s.NativeChat(ctx, p.c, p.provider, body, model)
	case openaiexecution.DispatchRawChat:
		r, err = p.s.RawChat(ctx, p.c, p.provider, body, model, p.tls...)
	case openaiexecution.DispatchGrok:
		var handled bool
		r, handled, err = p.s.Grok.ChatResponses(ctx, p.c, p.provider, body, key, model, p.tls...)
		// An unhandled Grok bridge leaves the same attempt eligible for native Chat;
		// a bridge error is returned instead of opening another response.
		if !handled && err == nil {
			r, err = p.s.RawChat(ctx, p.c, p.provider, body, model, p.tls...)
		}
	}
	return openaiexecution.FromForwardResult(r), err
}

// ClientAllowed evaluates and logs client admission using the first supplied
// TLS match or a newly matched route. A disabled restriction admits the
// request; callers handle rejection when an enabled restriction does not
// match.
func (p *openAIChatExecutionAdapter) ClientAllowed(ctx context.Context, body []byte) bool {
	var match egress.TLSFingerprintRouterMatchResult
	if len(p.tls) > 0 {
		match = p.tls[0]
	} else {
		match = p.s.Requests.MatchTLS(p.c, p.provider)
	}
	restriction := p.s.Requests.DetectClient(p.c, p.provider, match)
	LogCodexCLIOnlyDetection(ctx, p.c, p.provider, APIKeyIDFromContext(p.c), restriction, body)
	return !restriction.Enabled || restriction.Matched
}

// PolicyDenied marks this request as locally limited for Ops reporting.
func (p *openAIChatExecutionAdapter) PolicyDenied() {
	MarkOpsClientBusinessLimited(p.c, OpsClientBusinessLimitedReasonLocalPolicyDenied)
}

// Reject writes a protocol rejection using this attempt's HTTP context.
func (p *openAIChatExecutionAdapter) Reject(status int, kind, message string) {
	WriteOpenAIForwardRejection(p.c, status, kind, message, "")
}

// ChatError commits response state before writing the Chat protocol rejection.
// Callers terminate the failed preparation path afterward so the outer attempt
// handler does not replace this dedicated error.
func (p *openAIChatExecutionAdapter) ChatError(status int, kind, message string) {
	MarkResponseCommitted(p.c)
	WriteOpenAIForwardRejection(p.c, status, kind, message, "")
}

// DefaultChat reports whether attempt protocol policy resolves to Chat
// Completions.
func (p *openAIChatExecutionAdapter) DefaultChat() bool {
	return resolveOpenAITextProtocolForAttempt(p.c, p.provider, providercore.TextProtocolChatCompletions) == providercore.TextProtocolChatCompletions
}

// IsolateCacheKey namespaces the compatibility seed with the authenticated
// API-key identifier.
func (p *openAIChatExecutionAdapter) IsolateCacheKey(key string) string {
	return upstream.IsolateSessionID(APIKeyIDFromContext(p.c), key)
}

// ApplyChatFast applies the provider's current fast-tier policy to the
// outgoing body. A BlockedError marks local business limitation and writes a
// committed permission error; callers stop forwarding on the returned error.
func (p *openAIChatExecutionAdapter) ApplyChatFast(ctx context.Context, model string, body []byte) ([]byte, error) {
	updated, err := tierpolicy.ApplyBody(body, p.s.FastPolicy.Input(ctx, p.provider, model))
	if blocked, ok := errors.AsType[*tierpolicy.BlockedError](err); ok {
		p.PolicyDenied()
		p.ChatError(403, "permission_error", blocked.Message)
	}
	return updated, err
}

// AccessToken resolves execution credentials for the adapter's current
// provider record.
func (p *openAIChatExecutionAdapter) AccessToken(ctx context.Context) (string, error) {
	token, _, err := p.s.Requests.Credentials.Resolve(ctx, gatewayprovider.ExecutionRecord(p.provider))
	return token, err
}

// BuildChat constructs a streaming upstream request from the prepared body and
// credentials. It borrows the active Gin context for header construction and
// passes this attempt's TLS matches to OpenAIRequests.Build.
func (p *openAIChatExecutionAdapter) BuildChat(ctx context.Context, body []byte, token, key string) (*http.Request, error) {
	return p.s.Requests.Build(ctx, p.c, p.provider, body, token, true, key, false, p.tls...)
}

// UpstreamSessionKey isolates the prompt seed by API key and Codex identity
// namespace.
func (p *openAIChatExecutionAdapter) UpstreamSessionKey(id int64, key string) string {
	return openai.IsolateOpenAIUpstreamSessionID(id, provideradapter.CodexIdentityNamespace(CodexIdentityRecord(p.c, p.provider.View())), key)
}

// ChatErrorResponse applies Chat HTTP-error handling to the supplied response
// and model. It returns the handler error with no result; RunConvertedResponses has already
// installed the captured error body for this handler to read.
func (p *openAIChatExecutionAdapter) ChatErrorResponse(r *http.Response, model string) (*openaiexecution.Result, error) {
	_, err := p.s.chatError(r, p.c, p.provider, model)
	return nil, err
}
