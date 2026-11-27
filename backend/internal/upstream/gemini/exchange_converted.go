package gemini

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
)

// exchange_converted.go executes Gemini HTTP dispatches for protocol-agnostic
// IR conversions while coordinating provider retry budgets and single-pass
// compatibility signature recovery. Google Gemini accounts retry transient
// transport and quota statuses within a local attempt budget, whereas relays
// borrowing Gemini wire encoding delegate ordinary retries to the outer gateway
// failover loop through ExternalRetry while still retaining bounded signature
// repair inside the active attempt.
//
// Replayed tool or thinking signatures cached from an earlier turn can expire or
// fail upstream validation on the same provider. When an upstream HTTP 400
// response reports a signature error and RepairToolReplay produces a stripped
// compatibility request body, ExchangeConverted records a signature_error
// diagnostic and retries once on the same provider without consuming an
// ordinary retry slot. Every discarded error body is captured and closed before
// rebuilding or retrying, and caller cancellation aborts backoff waits immediately.

// ExchangeConverted dispatches a converted Gemini request within its configured
// retry budget and returns the selected HTTP response alongside its request-ID
// header name and wire candidate count. Callers supply Build and Do callbacks
// on options and take ownership of closing a non-nil returned Response.Body.
//
// ExternalRetry limits ordinary dispatches to one attempt and runs CheckPolicy
// ahead of signature repair so explicit administrator or pool rules stop the
// exchange immediately. On Google accounts, HTTP 400 signature rejections run
// RepairToolReplay once before CheckPolicy evaluates general status rules.
// Scope-rejected 403 responses return without backoff, 429 responses notify
// OnStatus before sleeping, and caller context cancellation aborts immediately.
func ExchangeConverted(ctx context.Context, options ExchangeOptions) (ExchangeResult, error) {
	maxAttempts := options.MaxRetries
	// External retry ownership delegates ordinary HTTP and transport retries to the
	// outer gateway budget while keeping bounded signature repair inside this attempt.
	if options.ExternalRetry {
		maxAttempts = 1
	}
	requestIDHeader := options.RequestIDHeader
	var resp *http.Response
	candidateCount := 1
	repairedReplay := false

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
			// Caller cancellation terminates the exchange directly, whereas transport
			// header deadlines leave ctx active for retry or FinalError classification.
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
			// Transport errors share maxAttempts with HTTP status retries and sleep
			// before rebuilding so repeated network faults do not spin tightly.
			if attempt < maxAttempts {
				logging.LegacyPrintf(
					"service.gemini_converted",
					"Gemini provider %d: upstream request failed, retry %d/%d: %v",
					options.ProviderID,
					attempt,
					maxAttempts,
					err,
				)
				if !SleepGeminiBackoff(ctx, attempt) {
					return ExchangeResult{}, ctx.Err()
				}
				continue
			}
			options.SetError(0, safeErr, "")
			return ExchangeResult{}, options.FinalError("Upstream request failed after retries: " + safeErr)
		}

		// Evaluate external provider policy before signature repair on compatible
		// relays so explicit custom-code or pool rules keep control of the response.
		if options.ExternalRetry && options.CheckPolicy != nil {
			matched, rebuilt := options.CheckPolicy(ctx, resp)
			resp = rebuilt
			if matched {
				break
			}
		}

		// Rebuild one compatibility request without replayed tokens or CallMemory
		// bindings when upstream rejects a cached or replayed signature with HTTP 400.
		if resp.StatusCode == http.StatusBadRequest && !repairedReplay && options.RepairToolReplay != nil {
			respBody := options.ReadError(resp)
			_ = resp.Body.Close()
			if IsGeminiSignatureRelatedError(respBody) && options.RepairToolReplay() {
				upstreamReqID := resp.Header.Get(requestIDHeader)
				if upstreamReqID == "" {
					upstreamReqID = resp.Header.Get("x-goog-request-id")
				}
				upstreamMsg := ""
				if options.Message != nil {
					upstreamMsg = strings.TrimSpace(options.Message(respBody))
				}
				if options.Sanitize != nil {
					upstreamMsg = options.Sanitize(upstreamMsg)
				}
				upstreamDetail := ""
				if options.Detail != nil {
					upstreamDetail = options.Detail(respBody)
				}
				if options.Observe != nil {
					options.Observe(ExchangeNotice{
						Platform:           options.Platform,
						ProviderID:         options.ProviderID,
						ProviderName:       options.ProviderName,
						UpstreamStatusCode: resp.StatusCode,
						UpstreamRequestID:  upstreamReqID,
						Kind:               "signature_error",
						Message:            upstreamMsg,
						Detail:             upstreamDetail,
					})
				}
				logging.LegacyPrintf(
					"service.gemini_converted",
					"Gemini provider %d: detected signature-related 400, retrying with compatibility replay format",
					options.ProviderID,
				)
				repairedReplay = true
				attempt--
				continue
			}
			// Restore a readable HTTP 400 body when signature recovery is inapplicable
			// so subsequent policy checks and client error mappers inspect the payload.
			resp = &http.Response{
				StatusCode: http.StatusBadRequest,
				Header:     resp.Header.Clone(),
				Body:       io.NopCloser(bytes.NewReader(respBody)),
			}
		}

		// Run policy checks once for Google accounts after signature repair has
		// either skipped or already been spent on an earlier iteration.
		if !options.ExternalRetry && options.CheckPolicy != nil {
			matched, rebuilt := options.CheckPolicy(ctx, resp)
			resp = rebuilt
			if matched {
				break
			}
		}

		if resp.StatusCode >= http.StatusBadRequest &&
			!options.ExternalRetry &&
			options.ShouldRetry != nil &&
			options.ShouldRetry(resp.StatusCode) {
			respBody := options.ReadError(resp)
			_ = resp.Body.Close()
			// Insufficient OAuth scopes require credential reauthorization rather than
			// retry delays; reconstruct the consumed 403 body and return immediately.
			if resp.StatusCode == http.StatusForbidden && IsGeminiInsufficientScope(resp.Header, respBody) {
				resp = &http.Response{
					StatusCode: resp.StatusCode,
					Header:     resp.Header.Clone(),
					Body:       io.NopCloser(bytes.NewReader(respBody)),
				}
				break
			}
			// Report 429 rate-limit headers before sleeping so concurrent requests
			// observe the updated provider cooldown while this attempt waits.
			if resp.StatusCode == http.StatusTooManyRequests && options.OnStatus != nil {
				options.OnStatus(ctx, resp.StatusCode, resp.Header, respBody)
			}
			if attempt < maxAttempts {
				upstreamReqID := resp.Header.Get(requestIDHeader)
				if upstreamReqID == "" {
					upstreamReqID = resp.Header.Get("x-goog-request-id")
				}
				upstreamMsg := ""
				if options.Message != nil {
					upstreamMsg = strings.TrimSpace(options.Message(respBody))
				}
				if options.Sanitize != nil {
					upstreamMsg = options.Sanitize(upstreamMsg)
				}
				upstreamDetail := ""
				if options.Detail != nil {
					upstreamDetail = options.Detail(respBody)
				}
				if options.Observe != nil {
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
				}
				logging.LegacyPrintf(
					"service.gemini_converted",
					"Gemini provider %d: upstream status %d, retry %d/%d",
					options.ProviderID,
					resp.StatusCode,
					attempt,
					maxAttempts,
				)
				if !SleepGeminiBackoff(ctx, attempt) {
					return ExchangeResult{}, ctx.Err()
				}
				continue
			}
			// Reconstruct a readable error body on retry exhaustion so the caller's
			// BeforeResponse handler can classify and render the final HTTP failure.
			resp = &http.Response{
				StatusCode: resp.StatusCode,
				Header:     resp.Header.Clone(),
				Body:       io.NopCloser(bytes.NewReader(respBody)),
			}
			break
		}

		break
	}

	return ExchangeResult{
		Response:        resp,
		RequestIDHeader: requestIDHeader,
		CandidateCount:  candidateCount,
	}, nil
}
