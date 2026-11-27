package gemini

import (
	"context"
	"io"
	"net/http"
	"sync/atomic"
)

// executeBody wraps a response stream and counts explicit fixture close
// invocations.
type executeBody struct {
	io.ReadCloser // wrapped upstream response body
	// closes points to the nonnil shared atomic counter supplied by the fixture;
	// every Close invocation updates it before the wrapped body is closed.
	closes *atomic.Int32 // shared close invocation counter
}

// Close increments the shared close counter before delegating to the wrapped
// body. It counts every invocation, including repeated closes, and returns the
// wrapped close error to the fixture caller.
func (b *executeBody) Close() error { b.closes.Add(1); return b.ReadCloser.Close() }

// exchangeTestOptions supplies deterministic error readers and inert callbacks
// for exchange tests. Individual cases replace transport and retry inputs;
// returned response bodies remain owned by the test invoking the exchange.
func exchangeTestOptions() ExchangeOptions {
	return ExchangeOptions{
		Build: func(ctx context.Context) (*http.Request, string, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://fixture.invalid", nil)
			return req, "x-request-id", err
		},
		Sanitize: func(value string) string { return value },
		Observe:  func(ExchangeNotice) {},
		SetError: func(int, string, string) {},
		Message:  func(body []byte) string { return string(body) },
		Detail:   func(body []byte) string { return string(body) },
		ReadError: func(resp *http.Response) []byte {
			body, _ := io.ReadAll(resp.Body)
			return body
		},
		CheckPolicy: func(_ context.Context, resp *http.Response) (bool, *http.Response) { return false, resp },
	}
}
