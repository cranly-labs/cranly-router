package googleforward

import (
	"context"
	"net/http"
	"strings"

	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	gemininative "github.com/TokenFlux/TokenRouter/internal/upstream/gemini"
)

// gemini_exchange.go binds retry policy and native Google error presentation
// for native Gemini exchanges. Provider policy, status retry checks, and
// default health observation stay bound to the selected attempt while external
// relays delegate retry budgets to the outer gateway loop.

// geminiExchangeOptions binds retry, policy, health, and diagnostic callbacks
// for one native Gemini provider attempt. Missing-project build errors present
// HTTP 400 Google errors, while external relays return failover signals on
// final transport exhaustion.
func (s *Gemini) geminiExchangeOptions(c *attempt, provider *gatewayprovider.ExecutionProvider, model string) gemininative.ExchangeOptions {
	options := gemininative.ExchangeOptions{
		ProviderID:   provider.Record.ID,
		ProviderName: provider.Record.Name,
		Platform:     provider.Record.Platform,
		MaxRetries:   geminiMaxRetries,
		ReadError:    s.readUpstreamErrorBody,
		Sanitize:     logredact.SanitizeUpstreamQueries,
		Message:      upstream.ExtractErrorMessage,
		CheckPolicy: func(ctx context.Context, resp *http.Response) (bool, *http.Response) {
			return s.checkErrorPolicyInLoop(ctx, provider, resp, model)
		},
		ShouldRetry: func(code int) bool { return s.shouldRetryGeminiUpstreamError(provider, code) },
		OnStatus: func(ctx context.Context, status int, header http.Header, body []byte) {
			s.observeHealth(ctx, provider, status, header, body)
		},
		Observe: func(value gemininative.ExchangeNotice) {
			c.Observe(ops.OpsUpstreamErrorEvent{
				Platform:           value.Platform,
				ProviderID:         value.ProviderID,
				ProviderName:       value.ProviderName,
				UpstreamStatusCode: value.UpstreamStatusCode,
				UpstreamRequestID:  value.UpstreamRequestID,
				Kind:               value.Kind,
				Message:            value.Message,
				Detail:             value.Detail,
			})
		},
		SetError: func(code int, message, detail string) { c.SetError(code, message, detail) },
		Detail:   s.upstreamErrorDetail,
	}
	if !isGoogleAccount(provider) {
		// The outer gateway owns a relay's ordinary attempts and retry budget.
		options.ExternalRetry = true
	}
	options.BuildError = func(err error) error {
		status := http.StatusBadGateway
		if strings.Contains(err.Error(), "missing project_id") {
			status = http.StatusBadRequest
		}
		return c.GoogleError(status, err.Error())
	}
	options.FinalError = func(message string) error {
		// A relay's final transport failure becomes a failover signal so the
		// outer gateway can choose another provider under its retry budget.
		if !isGoogleAccount(provider) {
			return &forwardcore.UpstreamFailoverError{
				Platform:                provider.Record.Platform,
				StatusCode:              http.StatusBadGateway,
				RetryableOnSameProvider: provider.View().IsPoolMode() && provider.View().IsPoolModeRetryableStatus(http.StatusBadGateway),
			}
		}
		return c.GoogleError(http.StatusBadGateway, message)
	}
	return options
}
