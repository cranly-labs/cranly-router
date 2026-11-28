package billing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
)

// TestPublicQuotePreservesExactImagePrices quotes explicit catalog image
// prices, including zero, with a user multiplier. It checks quote, unit
// price, and calculated charge agreement, and keeps an unknown image
// model distinguishable as unpriced.
func TestPublicQuotePreservesExactImagePrices(t *testing.T) {
	calculator := NewCalculator(defaultCatalogStub{entries: map[string]*pricing.CatalogModelPricing{
		"grok-imagine-image-quality": {CatalogRules: pricing.CatalogRules{ImagePrices: map[string]float64{"1K": 0.05}}, TokenPricingAbsent: true},
		"custom-image":               {OutputCostPerImage: 0.1, ImagePricePresent: true, TokenPricingAbsent: true, Mode: "image_generation"},
		"free-image":                 {ImagePricePresent: true, TokenPricingAbsent: true, Mode: "image_generation"},
	}}, CalculatorOptions{})
	resolver := NewPriceResolver(nil, calculator, nil, nil)
	for _, tc := range []struct {
		model string  // quoted image model
		price float64 // expected USD unit price
	}{
		{"grok-imagine-image-quality", 0.05},
		{"custom-image", 0.1},
		{"free-image", 0},
	} {
		input := PricingInput{Model: tc.model}
		quote := resolver.PublicQuote(context.Background(), PublicQuoteInput{PricingInput: input, RateMultiplier: 2})
		require.Equal(t, "priced", quote.PriceStatus, tc.model)
		require.Equal(t, "image", quote.PricingMode, tc.model)
		// Compare multiplied display amounts with a small tolerance for binary
		// floating-point noise while requiring the unit price and calculated
		// charge to retain their separately asserted values.
		require.InDelta(t, tc.price*2, quote.ImagePrice1K, 1e-12)
		unit, err := resolver.ResolveImageUnitPrice(context.Background(), input, "1K")
		require.NoError(t, err)
		require.Equal(t, tc.price, unit)
		cost, err := calculator.CalculateImageCost(tc.model, "1K", 1, 2)
		require.NoError(t, err)
		require.Equal(t, quote.ImagePrice1K, cost.ActualCost)
	}
	quote := resolver.PublicQuote(context.Background(), PublicQuoteInput{PricingInput: PricingInput{Model: "unknown-image"}, RateMultiplier: 1})
	require.Equal(t, "unpriced", quote.PriceStatus)
}

// TestPricingManagementFreeImageOverridesCatalog applies a free configured
// image card over a catalog with positive size prices. It checks that
// user unit prices become zero while the catalog prices remain available
// for each displayed size.
func TestPricingManagementFreeImageOverridesCatalog(t *testing.T) {
	zero := 0.0
	groupID := int64(1)
	catalog := defaultCatalogStub{entries: map[string]*pricing.CatalogModelPricing{
		"gemini-3-pro-image": {CatalogRules: pricing.CatalogRules{ImagePrices: pricing.MediaPrices{"1K": 0.134, "2K": 0.134, "4K": 0.24}}, TokenPricingAbsent: true},
	}}
	calculator := NewCalculator(catalog, CalculatorOptions{})
	resolver := NewPriceResolver(mediaPriceCards{card: &ModelPricingEntry{BillingMode: pricing.BillingModeImage, PerRequestPrice: &zero}}, calculator, nil, nil)
	input := PricingInput{Model: "gemini-3-pro-image", GroupID: &groupID}
	shown := resolver.PublicQuote(context.Background(), PublicQuoteInput{PricingInput: input, RateMultiplier: 2})
	require.Equal(t, "priced", shown.PriceStatus)
	require.ElementsMatch(t, []string{"1K", "2K", "4K"}, shown.ImagePriceSizes)
	for _, size := range shown.ImagePriceSizes {
		unit, err := resolver.ResolveImageUnitPrice(context.Background(), input, size)
		require.NoError(t, err)
		require.Zero(t, unit)
		official, err := calculator.DefaultImagePrice(input.Model, size)
		require.NoError(t, err)
		require.Positive(t, official, "用户售价不能覆盖目录及提供商成本的基础数据")
	}
}

// TestPublicVideoQuoteMatchesCatalogCharges compares custom catalog rates with
// long-video charges. Free dimensions remain present, unknown dimensions remain
// unpriced, and the administrator catalog exposes every declared resolution.
func TestPublicVideoQuoteMatchesCatalogCharges(t *testing.T) {
	calculator := NewCalculator(defaultCatalogStub{entries: map[string]*pricing.CatalogModelPricing{
		"custom-video": {CatalogRules: pricing.CatalogRules{VideoPrices: pricing.MediaPrices{"768P": 0.12, "2K": 0}}, TokenPricingAbsent: true},
	}}, CalculatorOptions{})
	resolver := NewPriceResolver(nil, calculator, nil, nil)
	quote := resolver.PublicQuote(context.Background(), PublicQuoteInput{PricingInput: PricingInput{Model: "custom-video"}, RateMultiplier: 1.5})
	require.Equal(t, "video", quote.PricingMode)
	require.Equal(t, "priced", quote.PriceStatus)
	require.Len(t, quote.VideoPrices, 2)
	require.Nil(t, quote.VideoPricePerSecond)
	for _, resolution := range []string{"768p", "2k"} {
		cost, err := calculator.CalculateVideoCost("custom-video", resolution, 2, 60, 1.5)
		require.NoError(t, err)
		require.InDelta(t, quote.VideoPrices[resolution]*120, cost.ActualCost, 1e-12)
	}
	_, err := calculator.CalculateVideoCost("custom-video", "480p", 1, 30, 1)
	require.ErrorIs(t, err, pricing.ErrModelPricingUnavailable)
	defaults := calculator.DefaultModelPrice("custom-video", "", "video")
	require.Len(t, defaults.Prices, 2)
	require.Equal(t, "2k", defaults.Prices[0].Key)
	require.Zero(t, *defaults.Prices[0].Value)
}

// TestConfiguredVideoQuoteKeepsMissingDimensionsUnavailable selects a partial
// video card over a catalog containing other resolutions. Unified video dispatch
// rejects a missing tier while free and paid controls settle at their quoted
// rates, proving that the missing-price assertion exercises the video path.
func TestConfiguredVideoQuoteKeepsMissingDimensionsUnavailable(t *testing.T) {
	groupID := int64(1)
	zero := 0.0
	paid := 0.25
	calculator := NewCalculator(defaultCatalogStub{entries: map[string]*pricing.CatalogModelPricing{
		"custom-video": {CatalogRules: pricing.CatalogRules{VideoPrices: pricing.MediaPrices{"480p": 0.1, "768p": 0.2}}, TokenPricingAbsent: true},
	}}, CalculatorOptions{})
	resolver := NewPriceResolver(mediaPriceCards{card: &ModelPricingEntry{
		BillingMode: pricing.BillingModeVideo,
		Intervals: []pricing.PricingInterval{
			{TierLabel: "2K", PerRequestPrice: &zero},
			{TierLabel: "1080p", PerRequestPrice: &paid},
		},
	}}, calculator, nil, nil)
	input := PricingInput{Model: "custom-video", GroupID: &groupID}
	quote := resolver.PublicQuote(context.Background(), PublicQuoteInput{PricingInput: input, RateMultiplier: 1.5})
	require.Equal(t, map[string]float64{"2k": 0, "1080p": 0.375}, quote.VideoPrices)
	require.Nil(t, quote.VideoPricePerSecond)
	resolved := resolver.Resolve(context.Background(), input)
	costInput := CostInput{
		Ctx:            context.Background(),
		Model:          input.Model,
		GroupID:        &groupID,
		Resolver:       resolver,
		Resolved:       resolved,
		UsageUnits:     60,
		SizeTier:       "768p",
		RateMultiplier: 1.5,
	}
	_, err := calculator.CalculateCostUnified(costInput)
	require.ErrorIs(t, err, pricing.ErrModelPricingUnavailable)
	for resolution, unitPrice := range map[string]float64{"2k": zero, "1080p": paid} {
		t.Run(resolution, func(t *testing.T) {
			costInput.SizeTier = resolution
			cost, err := calculator.CalculateCostUnified(costInput)
			require.NoError(t, err)
			require.Equal(t, string(pricing.BillingModeVideo), cost.BillingMode)
			// Allow binary rounding when comparing the same per-second amounts
			// through base-cost calculation and group-scaled public quotation.
			require.InDelta(t, unitPrice*60, cost.TotalCost, 1e-12)
			require.InDelta(t, quote.VideoPrices[resolution]*60, cost.ActualCost, 1e-12)
		})
	}
}

// TestPublicQuoteRestrictsDisallowedServiceTiers quotes catalog and card-backed
// token models under nil and selective route service-tier filters. Disallowed
// Fast, Flex, and Ultrafast quotes are omitted while standard prices, calendar
// adjustments, and settlement resolutions remain intact.
func TestPublicQuoteRestrictsDisallowedServiceTiers(t *testing.T) {
	fastFactor := 2.0
	flexFactor := 0.5
	ultrafastFactor := 4.0
	calendarFactor := 1.25
	catalog := defaultCatalogStub{entries: map[string]*pricing.CatalogModelPricing{
		"tiered-chat": {
			Source:              "models.dev",
			InputCostPerToken:   2e-6,
			OutputCostPerToken:  8e-6,
			SupportsServiceTier: true,
			CatalogRules: pricing.CatalogRules{
				ServiceTierPrices: []pricing.CatalogServicePrice{
					{Tier: "", After: "2020-01-01", Zone: "UTC", Factor: &calendarFactor},
					{Tier: "priority", Factor: &fastFactor},
					{Tier: "flex", Factor: &flexFactor},
					{Tier: "ultrafast", Factor: &ultrafastFactor},
				},
			},
		},
	}}
	calculator := NewCalculator(catalog, CalculatorOptions{})
	resolver := NewPriceResolver(nil, calculator, nil, nil)
	input := PricingInput{Model: "tiered-chat"}

	unrestricted := resolver.PublicQuote(context.Background(), PublicQuoteInput{PricingInput: input, RateMultiplier: 1})
	require.InDelta(t, 2.5e-6, unrestricted.InputPricePerToken, 1e-12)
	require.InDelta(t, 5e-6, unrestricted.FastInputPricePerToken, 1e-12)
	require.NotNil(t, unrestricted.Flex)
	require.InDelta(t, 1.25e-6, unrestricted.Flex.Input, 1e-12)
	require.NotNil(t, unrestricted.Ultrafast)
	require.InDelta(t, 10e-6, unrestricted.Ultrafast.Input, 1e-12)

	none := resolver.PublicQuote(context.Background(), PublicQuoteInput{
		PricingInput:   input,
		RateMultiplier: 1,
		ServiceTiers:   &pricing.ServiceTierSupport{},
	})
	require.InDelta(t, 2.5e-6, none.InputPricePerToken, 1e-12)
	require.Zero(t, none.FastInputPricePerToken)
	require.Zero(t, none.FastOutputPricePerToken)
	require.Nil(t, none.Flex)
	require.Nil(t, none.Ultrafast)

	flexOnly := resolver.PublicQuote(context.Background(), PublicQuoteInput{
		PricingInput:   input,
		RateMultiplier: 1,
		ServiceTiers:   &pricing.ServiceTierSupport{Flex: true},
	})
	require.Zero(t, flexOnly.FastInputPricePerToken)
	require.NotNil(t, flexOnly.Flex)
	require.InDelta(t, 1.25e-6, flexOnly.Flex.Input, 1e-12)
	require.Nil(t, flexOnly.Ultrafast)

	// Card-configured Fast and Flex multipliers on interval cards must also be
	// cleared on a restricted copy without mutating the stored card.
	groupID := int64(7)
	stdIn := 3e-6
	stdOut := 9e-6
	boundary := 100000
	cardFast := 3.0
	cardFlex := 0.4
	card := &ModelPricingEntry{
		BillingMode:    pricing.BillingModeToken,
		InputPrice:     &stdIn,
		OutputPrice:    &stdOut,
		FastMultiplier: &cardFast,
		FlexMultiplier: &cardFlex,
		Intervals: []pricing.PricingInterval{
			{MinTokens: 0, MaxTokens: &boundary, InputPrice: &stdIn, OutputPrice: &stdOut},
			{MinTokens: boundary, InputPrice: &stdIn, OutputPrice: &stdOut},
		},
	}
	cardResolver := NewPriceResolver(mediaPriceCards{card: card}, calculator, nil, nil)
	cardInput := PricingInput{Model: "tiered-chat", GroupID: &groupID}
	restrictedCard := cardResolver.PublicQuote(context.Background(), PublicQuoteInput{
		PricingInput:   cardInput,
		RateMultiplier: 1,
		ServiceTiers:   &pricing.ServiceTierSupport{},
	})
	require.Empty(t, restrictedCard.ContextIntervals)
	require.InDelta(t, stdIn, restrictedCard.InputPricePerToken, 1e-12)
	require.Zero(t, restrictedCard.FastInputPricePerToken)
	require.Nil(t, restrictedCard.Flex)
	require.NotNil(t, card.FastMultiplier)
	require.NotNil(t, card.FlexMultiplier)
}
