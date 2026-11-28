package provider

import (
	"github.com/TokenFlux/TokenRouter/internal/apikey"
	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
	"github.com/TokenFlux/TokenRouter/internal/gateway/tierpolicy"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
)

// marketplace_tiers.go determines which non-standard token rates a public
// listing can quote for a resolved provider route. A provider may serve
// several protocols with different completion data. Embeddings and standalone
// searches can use the same provider as Chat while supplying no service tier
// to the token calculator. Both the client entry and the upstream route matter.
//
// OpenAI-compatible text execution selects an outbound tier after group and
// system policy. Direct Anthropic Messages instead uses the Fast beta header
// and speed, subject to its own beta policy and provider header overrides.
// Marketplace quotes describe the shared group configuration, before any
// individual payer or API key selects its own overrides.

// MarketplaceTierPolicy binds immutable OpenAI and Anthropic settings for one
// price listing. Nil settings apply no dynamic restrictions, matching the
// respective executors after a failed settings read. The caller supplies the
// actual upstream model and the resolved protocols for each route.
//
// A Messages client can express Fast through speed. Chat and Responses clients
// can also request Flex or Ultrafast. Media and auxiliary entries contribute
// no accelerated token pricing even when their transport uses Responses.
func MarketplaceTierPolicy(settings *tierpolicy.OpenAIFastPolicySettings, beta *anthropic.BetaPolicySettings) routing.MarketplaceTierPolicy {
	return func(target modelrules.Target, groupPolicy string) pricing.ServiceTierSupport {
		source := capability.ProtocolID(target.ClientProtocol)
		wire := capability.ProtocolID(target.UpstreamProtocol)
		if source == "" {
			// A caller describing only a wire protocol supplies a native route.
			// Production catalogue targets retain both ends of each protocol conversion.
			source = wire
		}
		switch source {
		case capability.ProtocolAnthropicMessages, capability.ProtocolOpenAIResponses, capability.ProtocolOpenAIChatCompletions,
			capability.ProtocolResponsesCompact, capability.ProtocolResponsesWebSocket:
		default:
			return pricing.ServiceTierSupport{}
		}
		switch target.Platform {
		case capability.PlatformAnthropic:
			return pricing.ServiceTierSupport{Fast: wire == capability.ProtocolAnthropicMessages && marketplaceMessagesFast(target, beta)}
		case capability.PlatformOpenCode:
			if wire == capability.ProtocolAnthropicMessages {
				// OpenCode's native Messages builder sanitizes speed against its final
				// header and does not run the direct Anthropic provider's beta policy.
				return pricing.ServiceTierSupport{Fast: !target.FastBetaHeaderSet || target.FastBetaHeaderEnabled}
			}
		case capability.PlatformOpenAI, capability.PlatformGrok:
		default:
			return pricing.ServiceTierSupport{}
		}
		switch wire {
		case capability.ProtocolOpenAIResponses, capability.ProtocolOpenAIChatCompletions, capability.ProtocolResponsesWebSocket, capability.ProtocolResponsesCompact:
			return marketplaceOpenAITiers(settings, target, groupPolicy, source)
		default:
			return pricing.ServiceTierSupport{}
		}
	}
}

// marketplaceMessagesFast checks whether the outbound beta can retain Fast
// for a Messages route. Native Anthropic applies its model-scoped block before
// accepting a provider override. That override controls final body sanitation
// even when an earlier header filter removed the client token.
//
// Bedrock and Vertex use separate transports. On the direct routes, a fixed
// header can restore a filtered beta or omit an otherwise permitted token.
// With no override, clients can supply the token on a transparent request.
func marketplaceMessagesFast(target modelrules.Target, beta *anthropic.BetaPolicySettings) bool {
	switch target.ProviderType {
	case capability.ProviderTypeAPIKey, capability.ProviderTypeOAuth, capability.ProviderTypeSetupToken:
	default:
		return false
	}
	oauth := target.ProviderType != capability.ProviderTypeAPIKey
	decision := anthropic.EvaluateBetaPolicy(beta, anthropic.BetaFastMode, oauth, false, target.Model)
	if decision.BlockErr != nil {
		return false
	}
	if target.FastBetaHeaderSet {
		return target.FastBetaHeaderEnabled
	}
	_, filtered := decision.FilterSet[anthropic.BetaFastMode]
	return !filtered
}

// marketplaceOpenAITiers evaluates the tiers a client can request through the
// public group, using the same decision as the HTTP and WebSocket entrypoints.
// The neutral key and zero payer ID select only shared rules. A default
// request represents standard labels in an all-tier rule; selective global
// rules distinguish only the three non-standard tiers.
//
// Keep each accepted output, since a rule for one input tier can force a
// different one. A blocked or removed field contributes no non-standard rate.
func marketplaceOpenAITiers(settings *tierpolicy.OpenAIFastPolicySettings, target modelrules.Target, groupPolicy string, source capability.ProtocolID) pricing.ServiceTierSupport {
	openAI := target.Platform == capability.PlatformOpenAI
	if !openAI {
		// The request executor reads the group's acceleration setting only for
		// OpenAI providers. Compatible targets still participate in the global rules.
		groupPolicy = routing.GroupOpenAIFastPolicyFollowRequest
	}
	oauth := target.ProviderType == capability.ProviderTypeOAuth || target.ProviderType == capability.ProviderTypeSetupToken
	input := tierpolicy.DecisionInput{
		Model:       target.Model,
		GroupPolicy: groupPolicy,
		OpenAI:      openAI,
		KeyPolicy:   func() string { return apikey.APIKeyFastModePolicyFollowRequest },
		Evaluate: func(tier string) (string, string) {
			return tierpolicy.Evaluate(settings, 0, oauth, false, target.Model, tier)
		},
	}
	tiers := []string{"", tierpolicy.OpenAIFastTierPriority}
	if source != capability.ProtocolAnthropicMessages {
		tiers = append(tiers, "default", tierpolicy.OpenAIFastTierFlex, tierpolicy.OpenAIFastTierUltrafast)
	}
	var allowed pricing.ServiceTierSupport
	for _, tier := range tiers {
		decision := tierpolicy.Resolve(input, tier, tier != "")
		switch decision.Tier {
		case tierpolicy.OpenAIFastTierPriority:
			allowed.Fast = true
		case tierpolicy.OpenAIFastTierFlex:
			allowed.Flex = true
		case tierpolicy.OpenAIFastTierUltrafast:
			allowed.Ultrafast = true
		}
	}
	return allowed
}
