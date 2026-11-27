package gemini

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	logger "github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
)

// exchange_native.go retries native Gemini requests and selects final
// responses or eligible local token estimates.
// Native Gemini retries can return a local count estimate after transport or
// eligible status attempts are exhausted. ExternalRetry instead returns the
// first failure for caller-owned provider retry and count policy. Error policy
// and recognized scope failures stop local retries, leaving the caller to
// handle their selected response.
//
// Every attempt rebuilds the request and its ID-header selection. Retried
// error bodies close after capture and final error responses are reconstructed
// from captured bytes. Status 429 is reported before retry delay so caller
// health state can affect concurrent requests. A count estimate carries its
// own result pointer, while normal final responses transfer body ownership to
// the caller.

// ExchangeNotice identifies an upstream exchange observation and its selected
// diagnostic fields.
type ExchangeNotice struct {
	// Platform identifies the selected platform; ProviderName identifies its
	// display name.
	Platform, ProviderName string
	ProviderID             int64 // selected provider audit identifier
	UpstreamStatusCode     int   // upstream HTTP status; zero for transport failure
	// UpstreamRequestID identifies the server request; Kind classifies
	// observation; Message supplies public-safe text; Detail supplies diagnostic
	// text.
	UpstreamRequestID, Kind, Message, Detail string
}

// ExchangeOptions supplies synchronous construction, retry policy,
// diagnostics, and body-recovery callbacks.
type ExchangeOptions struct {
	ProviderID int64 // selected provider audit identifier
	// ProviderName labels provider; Platform labels platform; RequestIDHeader
	// selects the initial request-ID header.
	ProviderName, Platform, RequestIDHeader string
	// MaxRetries is the local attempt count when ExternalRetry is false. Zero
	// performs no exchange and returns a result with no response.
	MaxRetries int // total request attempt budget
	// ExternalRetry selects one ordinary attempt and leaves retryable HTTP or
	// transport failures to the caller.
	ExternalRetry bool // caller-owned ordinary retry policy
	// CountFallback enables an estimate after native retry exhaustion.
	// EstimateCount is required when this flag is used, and the estimate remains
	// separate from usage.
	CountFallback bool // native token estimate selection
	// Build and Do are required on every attempt. Other callback fields are
	// called synchronously on their applicable error, policy, throttle, or
	// recovery path and callers supply those used by their selected exchange.
	Build func(context.Context) (*http.Request, string, error) // request and ID-header construction
	// Do returns a nonnil response and body on success. It owns cleanup for any
	// response returned together with an error because these retry branches do
	// not close that pair.
	Do            func(*http.Request) (*http.Response, error) // upstream HTTP exchange callback
	BuildError    func(error) error                           // request construction error mapping
	FinalError    func(string) error                          // terminal transport error mapping
	EstimateCount func() int                                  // native fallback token estimation
	ReadError     func(*http.Response) []byte                 // error response body consumption
	// CheckPolicy returns the readable response used for subsequent status
	// handling, including a nonnil replacement when matched. It owns any original
	// stream consumed or replaced during evaluation.
	CheckPolicy func(context.Context, *http.Response) (bool, *http.Response) // error-policy evaluation and response replacement
	ShouldRetry func(int) bool                                               // HTTP status retry eligibility
	OnStatus    func(context.Context, int, http.Header, []byte)              // early throttle observation callback
	Observe     func(ExchangeNotice)                                         // exchange diagnostic observation callback
	SetError    func(int, string, string)                                    // terminal error state callback
	// Message extracts error text; Detail extracts diagnostic body text.
	Message, Detail func([]byte) string
	Sanitize        func(string) string // diagnostic text redaction callback
	// RepairToolReplay rebuilds converted request history without replay
	// signatures after a signature rejection. True permits one repair.
	RepairToolReplay func() bool
}

// ExchangeResult carries a selected response or a separate native token
// estimate.
type ExchangeResult struct {
	CandidateCount int // final request candidate count

	// Response and EstimatedTokens represent alternative outcomes. A nonnil
	// response body belongs to the caller; a native estimate outcome has no
	// response body to close.
	Response        *http.Response // caller-owned selected HTTP response
	RequestIDHeader string         // selected upstream ID-header name
	EstimatedTokens *int           // independent native token estimate
}

// ExchangeNative returns the selected upstream response and request-ID header
// after its total attempt budget. Callers supply callbacks for the selected
// ExchangeOptions paths and close the returned body.
//
// Policy matches stop before status retry, scope-rejected 403 responses are
// rebuilt and returned, and 429 invokes caller observation before waiting.
// Transport retries use the same attempt count and a cancellable backoff
// wait. Native fallback can instead return an independent estimated token
// count after eligible retry exhaustion; that result is separate from server
// usage. ExternalRetry disables local retries and count estimates.
func ExchangeNative(ctx context.Context, options ExchangeOptions) (ExchangeResult, error) {
	maxAttempts := options.MaxRetries
	// External retry ownership limits this exchange to one dispatch, returning
	// failures for the caller's provider budget and count policy to handle.
	if options.ExternalRetry {
		maxAttempts = 1
	}
	requestIDHeader := options.RequestIDHeader
	var resp *http.Response
	candidateCount := 1
	// Rebuild the request and its ID-header selection for every attempt,
	// including after caller body replacement. Captured HTTP error bytes survive
	// stream closing before another request or final error mapping.
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return ExchangeResult{}, err
		}
		upstreamReq, idHeader, err := options.Build(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ExchangeResult{}, ctx.Err()
			}
			return ExchangeResult{}, options.BuildError(err)
		}
		requestIDHeader = idHeader
		candidateCount = requestCandidateCount(upstreamReq)

		resp, err = options.Do(upstreamReq)
		if err != nil {
			// Caller cancellation ends the exchange. Transport-local deadlines leave
			// the caller alive and must reach retry or final-error classification.
			if ctx.Err() != nil {
				return ExchangeResult{}, ctx.Err()
			}
			safeErr := options.Sanitize(err.Error())
			options.Observe(ExchangeNotice{
				Platform:           options.Platform,
				ProviderID:         options.ProviderID,
				ProviderName:       options.ProviderName,
				UpstreamStatusCode: 0,
				Kind:               "request_error",
				Message:            safeErr,
			})
			// Transport failures share the total attempt budget with HTTP retries;
			// wait before rebuilding so repeated failures cannot form an immediate
			// dispatch loop.
			if attempt < maxAttempts {
				logger.LegacyPrintf("service.gemini_messages_compat", "Gemini provider %d: upstream request failed, retry %d/%d: %v", options.ProviderID, attempt, maxAttempts, err)
				if !SleepGeminiBackoff(ctx, attempt) {
					return ExchangeResult{}, ctx.Err()
				}
				continue
			}
			// After the retry budget ends, a native count fallback returns its local
			// estimate separately from usage and avoids exposing the final transport
			// failure as generation output.
			if options.CountFallback && !options.ExternalRetry {
				estimated := options.EstimateCount()
				return ExchangeResult{EstimatedTokens: &estimated}, nil
			}
			options.SetError(0, safeErr, "")
			return ExchangeResult{}, options.FinalError("Upstream request failed after retries: " + safeErr)
		}

		// Apply error policy before ordinary status retry so a matched rule keeps
		// control over the chosen response and prevents another provider-local
		// attempt.
		matched, rebuilt := options.CheckPolicy(ctx, resp)
		resp = rebuilt
		if matched {
			break
		}

		if resp.StatusCode >= 400 && !options.ExternalRetry && options.ShouldRetry(resp.StatusCode) {
			respBody := options.ReadError(resp)
			_ = resp.Body.Close()
			// Rebuild the consumed scope-rejection body and return it because another
			// retry with the same bearer permissions cannot resolve the authorization
			// requirement.
			if resp.StatusCode == http.StatusForbidden && IsGeminiInsufficientScope(resp.Header, respBody) {
				resp = &http.Response{
					StatusCode: resp.StatusCode,
					Header:     resp.Header.Clone(),
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}
				break
			}
			// Publish the throttle observation before waiting so other caller requests
			// can react to this provider cooldown while the current attempt is delayed.
			if resp.StatusCode == http.StatusTooManyRequests {
				options.OnStatus(ctx, resp.StatusCode, resp.Header, respBody)
			}
			// Retry this classified HTTP failure while attempt slots remain; its body
			// is already captured and closed before waiting and constructing another
			// request.
			if attempt < maxAttempts {
				upstreamReqID := resp.Header.Get(requestIDHeader)
				if upstreamReqID == "" {
					upstreamReqID = resp.Header.Get("x-goog-request-id")
				}
				upstreamMsg := strings.TrimSpace(options.Message(respBody))
				upstreamMsg = options.Sanitize(upstreamMsg)
				upstreamDetail := options.Detail(respBody)
				options.Observe(ExchangeNotice{
					Platform:           options.Platform,
					ProviderID:         options.ProviderID,
					ProviderName:       options.ProviderName,
					UpstreamStatusCode: resp.StatusCode,
					UpstreamRequestID:  upstreamReqID,
					Kind:               "retry",
					Message:            upstreamMsg,
					Detail:             upstreamDetail,
				})

				logger.LegacyPrintf("service.gemini_messages_compat", "Gemini provider %d: upstream status %d, retry %d/%d", options.ProviderID, resp.StatusCode, attempt, maxAttempts)
				if !SleepGeminiBackoff(ctx, attempt) {
					return ExchangeResult{}, ctx.Err()
				}
				continue
			}
			// After the eligible HTTP retry budget ends, return the configured count
			// estimate separately from usage; policy matches and recognized scope
			// responses already selected their own earlier outcomes.
			if options.CountFallback && !options.ExternalRetry {
				estimated := options.EstimateCount()
				return ExchangeResult{EstimatedTokens: &estimated}, nil
			}
			// Reconstruct the final error response from captured bytes because its
			// original body was consumed and closed during retry handling; native
			// callers still need readable error content.
			resp = &http.Response{
				StatusCode: resp.StatusCode,
				Header:     resp.Header.Clone(),
				Body:       io.NopCloser(bytes.NewReader(respBody)),
			}
			break
		}

		break
	}

	return ExchangeResult{Response: resp, RequestIDHeader: requestIDHeader, CandidateCount: candidateCount}, nil
}
