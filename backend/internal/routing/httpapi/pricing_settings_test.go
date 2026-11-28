package httpapi

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
)

// TestBillingSettingsRequestPreservesNullablePrices decodes omitted, null,
// and zero operation prices into a settings patch. It checks supplied-field
// presence and nullable values, then verifies that explicit false, true, and
// zero creation settings override defaults.
func TestBillingSettingsRequestPreservesNullablePrices(t *testing.T) {
	for _, tc := range []struct {
		body     string   // serialized settings request body
		supplied bool     // expected price presence flag
		price    *float64 // expected optional price amount
	}{
		{`{}`, false, nil}, {`{"web_search_price_per_call":null}`, true, nil}, {`{"web_search_price_per_call":0}`, true, new(float64)},
	} {
		var req updatePricingConfigRequest
		require.NoError(t, json.Unmarshal([]byte(tc.body), &req))
		patch := req.patch()
		require.Equal(t, tc.supplied, patch.WebSearchPricePerCall.Set)
		require.Equal(t, tc.price, patch.WebSearchPricePerCall.Value)
	}
	var req createPricingConfigRequest
	require.NoError(t, json.Unmarshal([]byte(`{"peak_rate_enabled":true,"peak_start":"09:00","peak_end":"17:00","long_context_pricing_enabled":false,"batch_image_discount_multiplier":0}`), &req))
	settings := pricing.DefaultBillingSettings()
	require.NoError(t, req.patch().Apply(&settings))
	require.True(t, settings.PeakRateEnabled)
	require.False(t, settings.LongContextPricingEnabled)
	require.Zero(t, settings.BatchImageDiscountMultiplier)
}
