package gemini

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// TestExchangeConverted_SignatureRejectionRetriesOnceAcrossGoogleAndExternalModes
// models an upstream HTTP 400 thought_signature rejection followed by a second
// failure across both Google local-retry and ExternalRetry modes. Signature
// recovery must rebuild and retry at most once per attempt while preserving
// readable error bytes and external ordinary retry limits.
func TestExchangeConverted_SignatureRejectionRetriesOnceAcrossGoogleAndExternalModes(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprintf("external=%t", external), func(t *testing.T) {
			options := exchangeTestOptions()
			options.Platform = "gemini"
			options.ProviderID = 42
			options.ProviderName = "gemini-fixture"
			options.ExternalRetry = external
			options.MaxRetries = 4
			options.ShouldRetry = func(code int) bool { return code == http.StatusServiceUnavailable }

			var notices []ExchangeNotice
			options.Observe = func(n ExchangeNotice) { notices = append(notices, n) }

			calls := 0
			repairs := 0
			options.RepairToolReplay = func() bool {
				repairs++
				return true
			}
			options.Do = func(*http.Request) (*http.Response, error) {
				calls++
				if calls <= 2 {
					return &http.Response{
						StatusCode: http.StatusBadRequest,
						Header:     http.Header{"X-Goog-Request-Id": {"sig-req-1"}},
						Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"Invalid thought_signature"}}`)),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader(`{}`)),
				}, nil
			}

			res, err := ExchangeConverted(context.Background(), options)
			require.NoError(t, err)
			require.NotNil(t, res.Response)
			defer func() {
				require.NoError(t, res.Response.Body.Close())
			}()

			require.Equal(t, 2, calls)
			require.Equal(t, 1, repairs)
			require.Equal(t, http.StatusBadRequest, res.Response.StatusCode)
			bodyBytes, readErr := io.ReadAll(res.Response.Body)
			require.NoError(t, readErr)
			require.Contains(t, string(bodyBytes), "Invalid thought_signature")
			require.Len(t, notices, 1)
			require.Equal(t, "signature_error", notices[0].Kind)
			require.Equal(t, "sig-req-1", notices[0].UpstreamRequestID)
		})
	}
}

// TestExchangeConverted_ExternalPolicyStopsBeforeSignatureRepair models an
// ExternalRetry provider whose CheckPolicy matches an HTTP 400 response. The
// exchange must stop immediately and return the policy-rebuilt response without
// invoking RepairToolReplay.
func TestExchangeConverted_ExternalPolicyStopsBeforeSignatureRepair(t *testing.T) {
	options := exchangeTestOptions()
	options.ExternalRetry = true
	options.MaxRetries = 5
	repaired := false
	options.RepairToolReplay = func() bool {
		repaired = true
		return true
	}
	options.CheckPolicy = func(_ context.Context, resp *http.Response) (bool, *http.Response) {
		_ = resp.Body.Close()
		rebuilt := &http.Response{
			StatusCode: resp.StatusCode,
			Header:     http.Header{"X-Policy": {"matched"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"rebuilt"}}`)),
		}
		return true, rebuilt
	}
	options.Do = func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadRequest,
			Header:     http.Header{},
			Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"thought_signature invalid"}}`)),
		}, nil
	}

	res, err := ExchangeConverted(context.Background(), options)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, res.Response.Body.Close())
	}()
	require.False(t, repaired)
	require.Equal(t, "matched", res.Response.Header.Get("X-Policy"))
}

// TestExchangeConverted_CancellationStopsTransportBackoff models a transport
// error followed by caller cancellation during backoff. ExchangeConverted must
// wake promptly and return context.Canceled without dispatching a second attempt.
func TestExchangeConverted_CancellationStopsTransportBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	options := exchangeTestOptions()
	options.MaxRetries = 4
	calls := 0
	options.Do = func(*http.Request) (*http.Response, error) {
		calls++
		// Cancel shortly after transport failure so ExchangeConverted is interrupted
		// inside SleepGeminiBackoff rather than at the top-of-loop check.
		time.AfterFunc(10*time.Millisecond, cancel)
		return nil, io.ErrUnexpectedEOF
	}
	started := time.Now()
	_, err := ExchangeConverted(ctx, options)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
	require.Less(t, time.Since(started), 500*time.Millisecond)
}

// TestExchangeConverted_InsufficientScopeSkips403BackoffAnd429NotifiesOnStatus
// exercises scope-rejected 403 responses and retried 429 responses under
// virtual time. Insufficient-scope 403 must return immediately without retry,
// whereas 429 must notify OnStatus before sleeping and succeeding on attempt 2.
func TestExchangeConverted_InsufficientScopeSkips403BackoffAnd429NotifiesOnStatus(t *testing.T) {
	t.Run("insufficient_scope_403_no_retry", func(t *testing.T) {
		options := exchangeTestOptions()
		options.MaxRetries = 3
		options.ShouldRetry = func(code int) bool { return code == http.StatusForbidden }
		calls := 0
		options.Do = func(*http.Request) (*http.Response, error) {
			calls++
			return &http.Response{
				StatusCode: http.StatusForbidden,
				Header:     http.Header{"Www-Authenticate": {`Bearer error="insufficient_scope"`}},
				Body:       io.NopCloser(strings.NewReader(`{"error":{"message":"insufficient authentication scopes"}}`)),
			}, nil
		}
		res, err := ExchangeConverted(context.Background(), options)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, res.Response.Body.Close())
		}()
		require.Equal(t, 1, calls)
		require.Equal(t, http.StatusForbidden, res.Response.StatusCode)
	})

	t.Run("rate_limit_429_notifies_before_retry", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			options := exchangeTestOptions()
			options.MaxRetries = 2
			options.ShouldRetry = func(code int) bool { return code == http.StatusTooManyRequests }
			calls := 0
			throttled := 0
			options.OnStatus = func(_ context.Context, status int, _ http.Header, _ []byte) {
				if status == http.StatusTooManyRequests {
					throttled++
				}
			}
			options.Do = func(*http.Request) (*http.Response, error) {
				calls++
				if calls == 1 {
					return &http.Response{
						StatusCode: http.StatusTooManyRequests,
						Header:     http.Header{"Retry-After": {"2"}},
						Body:       io.NopCloser(strings.NewReader(`{"error":{"code":429,"message":"quota"}}`)),
					}, nil
				}
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     http.Header{},
					Body:       io.NopCloser(strings.NewReader(`{"candidates":[]}`)),
				}, nil
			}
			res, err := ExchangeConverted(context.Background(), options)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, res.Response.Body.Close())
			}()
			require.Equal(t, 2, calls)
			require.Equal(t, 1, throttled)
			require.Equal(t, http.StatusOK, res.Response.StatusCode)
		})
	})

	t.Run("transport_timeout_maps_final_error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			mapped := errors.New("mapped final transport failure")
			options := exchangeTestOptions()
			options.MaxRetries = 2
			calls := 0
			options.Do = func(*http.Request) (*http.Response, error) {
				calls++
				return nil, fmt.Errorf("header timeout: %w", context.DeadlineExceeded)
			}
			options.FinalError = func(msg string) error {
				require.Contains(t, msg, "header timeout")
				return mapped
			}
			_, err := ExchangeConverted(context.Background(), options)
			require.ErrorIs(t, err, mapped)
			require.Equal(t, 2, calls)
		})
	})
}
