package provider

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
	"github.com/TokenFlux/TokenRouter/internal/gateway/tierpolicy"
	"github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
)

// TestMarketplaceTierPolicyUsesPublicExecutionRules quotes multiple routes
// against the same settings snapshot. Global force, provider scope, model
// fallback, and payer-only exceptions must agree with the execution decision.
func TestMarketplaceTierPolicyUsesPublicExecutionRules(t *testing.T) {
	policy := MarketplaceTierPolicy(&tierpolicy.OpenAIFastPolicySettings{Rules: []tierpolicy.OpenAIFastPolicyRule{
		{UserIDs: []int64{7}, Scope: "all", ServiceTier: "all", Action: "block"},
		{Scope: "oauth", ServiceTier: "priority", Action: "force_priority", ModelWhitelist: []string{"upstream-*"}, FallbackAction: "filter"},
		{Scope: "all", ServiceTier: "flex", Action: "block"},
	}}, nil)
	target := modelrules.Target{
		Platform: capability.PlatformOpenAI, ProviderType: capability.ProviderTypeOAuth,
		Model: "upstream-text", UpstreamProtocol: string(capability.ProtocolOpenAIResponses),
	}
	require.Equal(t, pricing.ServiceTierSupport{Fast: true}, policy(target, routing.GroupOpenAIFastPolicyForceOff))
	target.Model = "unmatched"
	require.Equal(t, pricing.ServiceTierSupport{}, policy(target, routing.GroupOpenAIFastPolicyForceOff))
	target.ProviderType = capability.ProviderTypeAPIKey
	require.Equal(t, pricing.ServiceTierSupport{Fast: true, Ultrafast: true}, policy(target, routing.GroupOpenAIFastPolicyFollowRequest))
	target.Platform = capability.PlatformGrok
	require.Equal(t, pricing.ServiceTierSupport{Fast: true, Ultrafast: true}, policy(target, routing.GroupOpenAIFastPolicyForceOff))
	target.ClientProtocol = string(capability.ProtocolAlphaSearch)
	require.Equal(t, pricing.ServiceTierSupport{}, policy(target, routing.GroupOpenAIFastPolicyFollowRequest))
}

// TestMarketplaceAnthropicFastUsesFinalBeta projects real provider overrides
// into a catalogue target. A provider header can restore a filtered beta, while
// explicit blocking still prevents Fast from reaching the upstream request.
func TestMarketplaceAnthropicFastUsesFinalBeta(t *testing.T) {
	makeTarget := func(header string) modelrules.Target {
		record := &provider.Record{
			Platform: capability.PlatformAnthropic, Type: capability.ProviderTypeAPIKey,
			Credentials: map[string]any{"header_override_enabled": true, "header_overrides": map[string]any{"anthropic-beta": header}},
		}
		rules, ok := (ModelPolicy{Record: record}).CatalogueRules().(catalogueRules)
		require.True(t, ok)
		return rules.AttributeTarget("claude", "claude", capability.ProtocolAnthropicMessages, capability.ProtocolAnthropicMessages)
	}
	defaults := anthropic.DefaultBetaPolicySettings()
	route := makeTarget(anthropic.BetaFastMode)
	require.True(t, MarketplaceTierPolicy(nil, defaults)(route, "").Fast)
	require.False(t, MarketplaceTierPolicy(nil, nil)(makeTarget("another-beta"), "").Fast)
	block := &anthropic.BetaPolicySettings{Rules: []anthropic.BetaPolicyRule{
		{BetaToken: anthropic.BetaFastMode, Action: anthropic.BetaPolicyActionBlock, Scope: "all"},
	}}
	require.False(t, MarketplaceTierPolicy(nil, block)(route, "").Fast)
	route = makeTarget("")
	route.ProviderType = capability.ProviderTypeOAuth
	require.False(t, MarketplaceTierPolicy(nil, defaults)(route, "").Fast)
	require.True(t, MarketplaceTierPolicy(nil, nil)(route, "").Fast)
}
