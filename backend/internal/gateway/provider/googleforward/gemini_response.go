package googleforward

import (
	"io"
	"net/http"
	"time"

	"github.com/TokenFlux/TokenRouter/internal/upstream"
	gemininative "github.com/TokenFlux/TokenRouter/internal/upstream/gemini"
)

// gemini_response.go binds Gemini response readers to the selected attempt's
// HTTP output, stream limits, and image observations. Bounded body reading and
// header filtering remain HTTP boundary operations, while the attempt records
// inline image counts and restores OAuth tool names during response adaptation.

// geminiResponseAdapter binds response reading to this attempt's image, tool,
// and HTTP output state alongside configured stream idle and frame limits.
func (s *Gemini) geminiResponseAdapter(c *attempt) *gemininative.ResponseAdapter {
	options := gemininative.ResponseOptions{
		Context: c.RequestContext(),
		Stream: upstream.GeminiStreamOptions{
			IdleTimeout:   time.Duration(s.Options.StreamInterval) * time.Second,
			MaxFrameBytes: s.Options.MaxLineSize,
		},
		GoogleError: func(status int, message string) error { return c.GoogleError(status, message) },
		ReadBody: func(body io.Reader) ([]byte, error) {
			return c.ReadBody(body, s.Options.ResponseReadLimit)
		},
		WriteHeaders:    func(dst, src http.Header) { c.WriteHeaders(dst, src, s.HeaderFilter) },
		DebugHeaders:    s.Options.Configured && s.Options.DebugHeaders,
		HasHeaderFilter: s.HeaderFilter != nil,
		ObserveImages:   func(body []byte) { c.observeImages(body) },
		ReverseTools:    func(body []byte) []byte { return c.reverseTools(body) },
	}
	return &gemininative.ResponseAdapter{Options: options}
}
