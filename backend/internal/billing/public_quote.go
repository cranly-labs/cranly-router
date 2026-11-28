package billing

import (
	"context"

	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
)

// public_quote.go resolves public marketplace model quotes and applies optional
// route service-tier restrictions. Marketplace listings quote the user charge
// for a group and model while hiding non-standard service tiers that no active
// group route can execute. PublicQuote restricts a local copy of resolved
// token pricing before formatting display amounts, keeping shared catalog and
// card records unchanged for settlement.

// PublicQuoteInput selects a model quote, user rate multiplier, and optional
// route-level service-tier filter.
type PublicQuoteInput struct {
	PricingInput           // model and group selection
	RateMultiplier float64 // user pricing multiplier
	// ServiceTiers restricts displayed Fast, Flex, and Ultrafast token quotes to
	// tiers supported by the caller's routes. Nil leaves all resolved tiers
	// visible.
	ServiceTiers *pricing.ServiceTierSupport
}

// PublicQuote resolves model prices and returns display amounts scaled by the
// requested rate multiplier. Call with a resolver that has a calculator; a
// non-nil ServiceTiers filter removes unsupported token service tiers from a
// copied resolution before display formatting.
func (r *PriceResolver) PublicQuote(ctx context.Context, input PublicQuoteInput) ModelDisplayPricing {
	resolved := r.Resolve(ctx, input.PricingInput)
	if input.ServiceTiers != nil && resolved != nil {
		resolved = resolved.RestrictServiceTiers(*input.ServiceTiers)
	}
	return r.calculator.DisplayPricingWithResolvedMultipliers(input.Model, input.RateMultiplier, resolved)
}
