package gemini

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// TestExchangeCancellationStopsBackoff exercises native and converted exchange
// modes after a transport failure. Cancelling the shared request must end the
// wait promptly and prevent another dispatch within the remaining local budget.
func TestExchangeCancellationStopsBackoff(t *testing.T) {
	for name, exchange := range map[string]func(context.Context, ExchangeOptions) (ExchangeResult, error){
		"native":    ExchangeNative,
		"converted": ExchangeConverted,
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			options := exchangeTestOptions()
			options.MaxRetries = 5
			options.Do = func(*http.Request) (*http.Response, error) {
				calls++
				// Schedule cancellation after the exchange enters its wait, exercising
				// interruption rather than only its check before request construction.
				time.AfterFunc(10*time.Millisecond, cancel)
				return nil, io.ErrUnexpectedEOF
			}
			started := time.Now()
			_, err := exchange(ctx, options)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 1, calls)
			require.Less(t, time.Since(started), 500*time.Millisecond)
		})
	}
}

// TestExchangeTransportDeadlineKeepsCallerLive models an upstream header timeout
// while the request context remains active. Both native and converted exchanges
// retry local Google budgets and classify external retry failures via FinalError.
func TestExchangeTransportDeadlineKeepsCallerLive(t *testing.T) {
	for name, exchange := range map[string]func(context.Context, ExchangeOptions) (ExchangeResult, error){
		"native":    ExchangeNative,
		"converted": ExchangeConverted,
	} {
		for _, external := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/external=%t", name, external), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					ctx := context.Background()
					calls, observed := 0, 0
					mapped := errors.New("classified transport failure")
					options := exchangeTestOptions()
					options.MaxRetries = 2
					options.ExternalRetry = external
					options.Do = func(*http.Request) (*http.Response, error) {
						calls++
						return nil, fmt.Errorf("response header timeout: %w", context.DeadlineExceeded)
					}
					options.Observe = func(ExchangeNotice) { observed++ }
					options.FinalError = func(message string) error {
						require.Contains(t, message, "response header timeout")
						return mapped
					}
					_, err := exchange(ctx, options)
					require.ErrorIs(t, err, mapped)
					require.NoError(t, ctx.Err())
					expected := 2
					if external {
						expected = 1
					}
					require.Equal(t, expected, calls)
					require.Equal(t, expected, observed)
				})
			})
		}
	}
}
