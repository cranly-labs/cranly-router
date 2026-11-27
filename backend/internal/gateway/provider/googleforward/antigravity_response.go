package googleforward

import (
	"github.com/TokenFlux/TokenRouter/internal/upstream/antigravity"
)

// antigravity_response.go binds Antigravity response adapters to attempt image
// observations, stream limits, and tool restoration. Configured options supply
// SSE line and interval limits, while a nil attempt supports pure request
// image-size extraction and v1internal response unwrapping.

// antigravityResponseAdapter binds response presentation to the current attempt
// image counter, stream limits, and tool restoration map. A nil attempt leaves
// request context unset while keeping pure extraction helpers available.
func (s *Antigravity) antigravityResponseAdapter(c *attempt) *antigravity.ResponseAdapter {
	opts := antigravity.ResponseOptions{
		ObserveRaw: func(body []byte) {
			if c != nil {
				c.observeImages(body)
			}
		},
		ReverseTools: func(body []byte) []byte { return c.reverseTools(body) },
	}
	if c != nil && c.Output != nil {
		opts.Context = c.RequestContext()
	}
	if s.Options.Configured {
		opts.MaxLineSize = s.Options.MaxLineSize
		opts.StreamDataIntervalTimeout = s.Options.StreamInterval
	}
	return &antigravity.ResponseAdapter{Options: opts}
}

// extractImageInputSize delegates request image-size extraction through the
// platform response adapter.
func (s *Antigravity) extractImageInputSize(body []byte) string {
	return s.antigravityResponseAdapter(nil).ExtractImageInputSize(body)
}

// unwrapV1InternalResponse delegates single-envelope extraction to the platform
// response adapter.
func (s *Antigravity) unwrapV1InternalResponse(body []byte) ([]byte, error) {
	return s.antigravityResponseAdapter(nil).UnwrapV1InternalResponse(body)
}
