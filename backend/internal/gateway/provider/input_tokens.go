package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/tokenestimate"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// input_tokens.go prepares counting requests while retaining client, billing,
// and upstream model names. Input-token queries can count a provider-mapped
// model while the caller still needs the client's requested identity, so
// preparation retains both names before replacing the counting request's model.
//
// PrepareAnthropicInputTokensWithAttributes and PrepareNativeInputTokens convert
// and project the five official /v1/responses/input_tokens fields {model,
// instructions, input, tools, tool_choice} through protocol/convert and jsontext
// with trc1 continuity-carrier unpacking. Provider type and endpoint then
// determine whether counting stays local, including malformed custom URLs that
// cannot establish the official OpenAI counting host.

// InputTokensPrepared combines counting input with the model names used during
// preparation.
type InputTokensPrepared struct {
	Request tokenestimate.Request // projected upstream counting view for callers inspecting fields
	// Payload holds the lossless projected {model, instructions, input, tools,
	// tool_choice} fields populated by IR and jsontext preparation; callers that
	// construct Request directly leave Payload zero so MarshalBody and EstimateLocal
	// fall back to Request.
	Payload tokenestimate.CountingPayload
	// OriginalModel and NormalizedModel describe ingress, while BillingModel and
	// UpstreamModel record provider mapping and wire normalization. The counting
	// request uses UpstreamModel, leaving ingress identity available to its caller.
	OriginalModel   string // client model before mapping
	NormalizedModel string // preparation input model spelling
	BillingModel    string // provider-mapped billing model name
	UpstreamModel   string // final counting request model
}

// MarshalBody serializes the prepared counting payload into compact JSON bytes
// for upstream /v1/responses/input_tokens dispatch. Prepared Payload values
// serialize directly to preserve raw tool schemas and 64-bit integer literals,
// whereas manually constructed Request values fall back to wirejson.Marshal.
func (p *InputTokensPrepared) MarshalBody() ([]byte, error) {
	if p == nil {
		return nil, errors.New("prepared input_tokens request is nil")
	}
	if !p.Payload.IsZero() {
		return tokenestimate.MarshalCountingPayload(p.Payload)
	}
	return wirejson.Marshal(p.Request)
}

// EstimateLocal computes a local tiktoken input-token approximation for p. Nil
// receivers return an error, Payload-backed values delegate to
// tokenestimate.EstimateCountingPayload, and zero-Payload values fall back to
// tokenestimate.Responses(p.Request).
func (p *InputTokensPrepared) EstimateLocal() (int, error) {
	if p == nil {
		return 0, errors.New("prepared input_tokens request is nil")
	}
	if !p.Payload.IsZero() {
		return tokenestimate.EstimateCountingPayload(p.Payload)
	}
	return tokenestimate.Responses(p.Request)
}

// PrepareAnthropicInputTokens converts an Anthropic Messages count_tokens
// request into the five official /v1/responses/input_tokens fields {model,
// instructions, input, tools, tool_choice} via protocol/convert and jsontext
// using provider metadata rules. Matching OpenAI trc1 continuity envelopes
// inside input unpack to raw encrypted_content while cross-account and foreign
// envelopes are stripped.
//
// OriginalModel and NormalizedModel preserve the untrimmed Anthropic model
// string, BillingModel records provider mapping, and UpstreamModel records
// OpenAI wire normalization. Both Payload and Request receive UpstreamModel.
func PrepareAnthropicInputTokens(
	body []byte,
	provider *ExecutionProvider,
	defaultMappedModel string,
) (*InputTokensPrepared, error) {
	return PrepareAnthropicInputTokensWithAttributes(context.Background(), body, provider, defaultMappedModel, nil)
}

// PrepareAnthropicInputTokensWithAttributes converts an Anthropic Messages
// count_tokens request into the five official /v1/responses/input_tokens fields
// {model, instructions, input, tools, tool_choice} using optional catalog
// attributes from service. Nil contexts select context.Background(), and a nil
// service resolves target model rules from provider record metadata. Malformed
// JSON, duplicate object keys, or conversion errors abort before returning a
// prepared counting request.
//
// OriginalModel and NormalizedModel retain the untrimmed Anthropic model string,
// while BillingModel records provider mapping and UpstreamModel records OpenAI
// wire normalization. Matching OpenAI trc1 continuity envelopes inside input
// unpack to raw encrypted_content, whereas cross-account and foreign envelopes
// strip during conversion so upstream counting never receives internal wrappers.
func PrepareAnthropicInputTokensWithAttributes(
	ctx context.Context,
	body []byte,
	provider *ExecutionProvider,
	defaultMappedModel string,
	service *routing.ModelAttributeService,
) (*InputTokensPrepared, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	originalModel, err := extractCountingModelRaw(body)
	if err != nil {
		return nil, fmt.Errorf("parse anthropic count_tokens request: %w", err)
	}

	normalizedModel := originalModel
	effort := requeststate.ExtractRoutingReasoningEffort(body, protocol.ProtocolAnthropicMessages)
	billingModel := ExecutionModelPolicy(provider).WithEffort(effort).ForwardModel(normalizedModel, strings.TrimSpace(defaultMappedModel))
	upstreamModel := ExecutionModelPolicy(provider).NormalizeOpenAI(billingModel)

	dialect, err := resolveAnthropicInputTokensDialect(ctx, provider, upstreamModel, body, service)
	if err != nil {
		return nil, fmt.Errorf("resolve input_tokens dialect: %w", err)
	}

	ex, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect:  dialect,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("parse anthropic count_tokens request: %w", err)
	}
	convertedBytes, err := ex.UpstreamRequest()
	if err != nil {
		return nil, fmt.Errorf("convert anthropic request to responses: %w", err)
	}
	upstream.ObserveExchangeConversion(ctx, ex)

	payload, err := tokenestimate.ProjectCountingPayload(convertedBytes)
	if err != nil {
		return nil, fmt.Errorf("project converted input_tokens request: %w", err)
	}
	payload.Model = upstreamModel

	return &InputTokensPrepared{
		Request:         countingPayloadToLegacyRequest(payload),
		Payload:         payload,
		OriginalModel:   originalModel,
		NormalizedModel: normalizedModel,
		BillingModel:    billingModel,
		UpstreamModel:   upstreamModel,
	}, nil
}

// PrepareNativeInputTokens validates a native OpenAI Responses input_tokens
// request via jsontext and projects {model, instructions, input, tools,
// tool_choice} onto both Payload and Request. Non-object JSON roots, duplicate
// object keys, or blank trimmed models return an error before provider mapping
// finishes.
//
// Preparation invokes PrepareNativeReplay on the full request body before
// ProjectCountingPayload removes top-level store=false and sibling generation
// controls. Running replay preparation first allows store=false counting
// requests to unpack matching trc1:openai: carriers or strip cross-account and
// foreign envelopes while retaining 64-bit integer literals and custom tool
// grammars.
func PrepareNativeInputTokens(body []byte, provider *ExecutionProvider) (*InputTokensPrepared, error) {
	rawModel, err := extractCountingModelRaw(body)
	if err != nil {
		return nil, fmt.Errorf("parse responses input_tokens request: %w", err)
	}
	originalModel := strings.TrimSpace(rawModel)
	if originalModel == "" {
		return nil, fmt.Errorf("parse responses input_tokens request: model is required")
	}

	effort := requeststate.ExtractRoutingReasoningEffort(body, protocol.ProtocolOpenAIResponses)
	billingModel := ExecutionModelPolicy(provider).WithEffort(effort).ForwardModel(originalModel, "")
	upstreamModel := ExecutionModelPolicy(provider).NormalizeOpenAI(billingModel)

	// Unpack matching trc1 envelopes or strip cross-account and foreign items
	// before projecting the five counting fields removes top-level store=false.
	rewritten, _, err := PrepareNativeReplay(
		context.Background(),
		provider,
		upstreamModel,
		body,
		nil,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIResponses,
	)
	if err != nil {
		return nil, fmt.Errorf("parse responses input_tokens request: %w", err)
	}

	payload, err := tokenestimate.ProjectCountingPayload(rewritten)
	if err != nil {
		return nil, fmt.Errorf("parse responses input_tokens request: %w", err)
	}
	payload.Model = upstreamModel

	return &InputTokensPrepared{
		Request:         countingPayloadToLegacyRequest(payload),
		Payload:         payload,
		OriginalModel:   originalModel,
		NormalizedModel: originalModel,
		BillingModel:    billingModel,
		UpstreamModel:   upstreamModel,
	}, nil
}

// EstimateInputTokensLocally selects local counting for absent targets,
// compatible platforms other than OpenAI, upstream-type targets, or custom API
// Key targets. Non-API-Key targets and API Keys with blank or official
// api.openai.com hosts use the upstream path; malformed custom URLs choose
// local counting.
func EstimateInputTokensLocally(provider *ExecutionProvider) bool {
	if provider == nil || provider.View().IsOpenAICompatible() && !provider.View().IsOpenAI() || provider.Record.Type == capability.ProviderTypeUpstream {
		return true
	}
	if provider.Record.Type != capability.ProviderTypeAPIKey {
		return false
	}
	baseURL := strings.TrimSpace(provider.View().GetCredential("base_url"))
	if baseURL == "" {
		return false
	}
	// A malformed custom URL cannot establish the official counting endpoint
	// identity, so parse failure selects local estimation along with other custom hosts.
	parsed, err := url.Parse(baseURL)
	if err != nil {
		return true
	}
	return !strings.EqualFold(parsed.Hostname(), "api.openai.com")
}

// resolveAnthropicInputTokensDialect builds the OpenAI Responses convert.Dialect
// for counting an Anthropic Messages request against provider and upstreamModel.
// OpenAI targets always retain ir.SchemeOpenAI so matching trc1:openai: carriers
// unpack inside input[] even when custom base URLs leave ServiceID blank.
func resolveAnthropicInputTokensDialect(
	ctx context.Context,
	provider *ExecutionProvider,
	upstreamModel string,
	body []byte,
	service *routing.ModelAttributeService,
) (convert.Dialect, error) {
	var res modelrules.Resolution
	if service != nil {
		var err error
		res, err = ResolveModelAttributes(ctx, provider, upstreamModel, service, string(protocol.ProtocolOpenAIResponses))
		if err != nil {
			return convert.Dialect{}, err
		}
	} else {
		target := ExecutionProtocolRecord(provider).ModelAttributeTarget(upstreamModel, string(protocol.ProtocolOpenAIResponses))
		if target.Platform == "" {
			target.Platform = capability.PlatformOpenAI
		}
		if target.ProviderType == "" {
			target.ProviderType = capability.ProviderTypeAPIKey
		}
		res = modelrules.Resolution{
			Target:     target,
			Attributes: ExecutionModelAttributes(provider, upstreamModel),
		}
	}
	res = res.ForRequest(ModelRequestState(body))
	dialect := ConversionDialect(ctx, provider, res, protocol.ProtocolOpenAIResponses)
	// Ensure OpenAI input_tokens targets unpack same-account trc1:openai: carriers
	// even when defaultMappedModel is blank on a custom relay URL.
	if dialect.ReplayScheme == "" && (provider == nil || provider.View().IsOpenAI()) {
		dialect.ReplayScheme = ir.SchemeOpenAI
	}
	return dialect, nil
}

// extractCountingModelRaw scans the top-level "model" string from a JSON object
// with invalid-UTF-8 tolerance while leaving full structural validation to
// downstream exchange or projection decoders. Non-object roots or non-string
// non-null model values return an error.
func extractCountingModelRaw(body []byte) (string, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return "", errors.New("request body must be a JSON object")
	}
	var modelOnly struct {
		Model   string                    `json:"model"`  // top-level request model string
		Unknown map[string]jsontext.Value `json:",embed"` // unmodeled sibling request members
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	if err := jsonv2.UnmarshalDecode(dec, &modelOnly); err != nil {
		return "", err
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		if err == nil {
			return "", errors.New("unexpected trailing JSON token")
		}
		return "", err
	}
	return modelOnly.Model, nil
}

// countingPayloadToLegacyRequest populates a tokenestimate.Request view from p
// so callers inspecting Request fields stay compatible. Input and ToolChoice
// receive independent byte copies while Payload owns lossless upstream
// serialization and local estimation.
func countingPayloadToLegacyRequest(p tokenestimate.CountingPayload) tokenestimate.Request {
	req := tokenestimate.Request{
		Model:        p.Model,
		Instructions: p.Instructions,
	}
	if len(p.Input) > 0 {
		req.Input = json.RawMessage(bytes.Clone(p.Input))
	}
	if len(p.Tools) > 0 {
		rawTools, err := jsonv2.Marshal(p.Tools, jsontext.AllowInvalidUTF8(true))
		if err == nil {
			// Best-effort Request view population skips tool shapes that ResponsesTool
			// cannot decode because MarshalBody and EstimateLocal read p.Payload directly.
			_ = json.Unmarshal(rawTools, &req.Tools)
		}
	}
	if len(p.ToolChoice) > 0 {
		req.ToolChoice = json.RawMessage(bytes.Clone(p.ToolChoice))
	}
	return req
}
