package googleforward_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
	gatewayhttp "github.com/TokenFlux/TokenRouter/internal/gateway/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/googleforward"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// antigravityFailingWriter rejects output after a configurable count of
// forwarded fixture writes.
type antigravityFailingWriter struct {
	// ResponseWriter forwards headers and other writer operations while
	// Write enforces the fixture failure threshold; callers keep the
	// underlying recorder available through response handling.
	gin.ResponseWriter // borrowed HTTP response writer
	// failAfter limits calls forwarded through ResponseWriter; writes
	// counts them before delegation. Nonpositive allowance rejects the
	// first call, and injected failures leave the counter unchanged.
	failAfter int // forwarded write call allowance
	// writes advances before delegated Write, so it counts forwarded
	// attempts even when the underlying response writer returns its own
	// error.
	writes int // forwarded fixture write count
}

// TestHandleGeminiStreamingResponse_NormalComplete feeds native partial content
// followed by terminal usage with cache reads. The result reports final
// input/output/cache buckets, and client output contains both text fragments.
func TestHandleGeminiStreamingResponse_NormalComplete(t *testing.T) {
	svc := newAntigravityStreamFixture(&googleforward.Options{MaxLineSize: 500 * 1024 * 1024})

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)

	pr, pw := io.Pipe()
	resp := &http.Response{StatusCode: http.StatusOK, Body: pr, Header: http.Header{}}

	// Feed partial and terminal native payloads through the blocking pipe,
	// then close the producer so final usage replaces earlier cumulative
	// observations before assertions.
	go func() {
		defer func() { _ = pw.Close() }()
		fmt.Fprintln(pw, `data: {"candidates":[{"content":{"parts":[{"text":"Hello"}]}}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":3}}`)
		fmt.Fprintln(pw, "")
		fmt.Fprintln(pw, `data: {"candidates":[{"content":{"parts":[{"text":" world"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":8,"cachedContentTokenCount":2}}`)
		fmt.Fprintln(pw, "")
	}()

	result, err := googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{Output: gatewayhttp.NewGoogleBoundary(c, svc.Options, true)}).HandleGeminiStreamingResponse(upstream.NewOutputContext(gatewayhttp.NewGoogleBoundary(c, svc.Options, true).Sink()), resp, time.Now())
	_ = pr.Close()

	require.NoError(t, err)
	require.NotNil(t, result)
	require.False(t, result.ClientDisconnect, "normal completion should not set clientDisconnect")
	require.NotNil(t, result.Usage)
	require.Equal(t, 8, result.Usage.InputTokens)
	require.Equal(t, 8, result.Usage.OutputTokens)
	require.Equal(t, 2, result.Usage.CacheReadInputTokens)

	body := rec.Body.String()
	require.Contains(t, body, "Hello")
	require.Contains(t, body, "world")
	require.NotContains(t, body, "event: error")
}

// TestUnwrapV1InternalResponse compares wrapped, unwrapped, scalar/null,
// malformed, nested, and large input. The adapter returns the exact expected
// single-envelope bytes for each exercised fixture after trimming exterior
// whitespace.
func TestUnwrapV1InternalResponse(t *testing.T) {
	svc := newAntigravityFixture(antigravityDependencies{})

	// Use a long pad string under the same response wrapper so the large case
	// exercises payload size while retaining the unwrap shape.
	largePadding := strings.Repeat("x", 50*1024)
	largeInput := []byte(fmt.Sprintf(`{"response":{"id":"big","pad":"%s"}}`, largePadding))
	largeExpected := fmt.Sprintf(`{"id":"big","pad":"%s"}`, largePadding)

	tests := []struct {
		name     string // table scenario name
		input    []byte // wrapped response fixture bytes
		expected string // expected single-envelope output
		wantErr  bool   // expected unwrap failure flag
	}{
		{
			name:     "正常 response 包装",
			input:    []byte(`{"response":{"id":"123","content":"hello"}}`),
			expected: `{"id":"123","content":"hello"}`,
		},
		{
			name:     "无 response 透传",
			input:    []byte(`{"id":"456"}`),
			expected: `{"id":"456"}`,
		},
		{
			name:     "空 JSON",
			input:    []byte(`{}`),
			expected: `{}`,
		},
		{
			name:     "response 为 null",
			input:    []byte(`{"response":null}`),
			expected: `null`,
		},
		{
			name:     "response 为基础类型 string",
			input:    []byte(`{"response":"hello"}`),
			expected: `"hello"`,
		},
		{
			name:     "非法 JSON",
			input:    []byte(`not json`),
			expected: `not json`,
		},
		{
			name:     "嵌套 response 只解一层",
			input:    []byte(`{"response":{"response":{"inner":true}}}`),
			expected: `{"response":{"inner":true}}`,
		},
		{
			name:     "大型 JSON >50KB",
			input:    largeInput,
			expected: largeExpected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).UnwrapV1InternalResponse(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.expected, strings.TrimSpace(string(got)))
		})
	}
}

// BenchmarkUnwrapV1Internal_Old_Small repeatedly decodes the small wrapper and
// remarshals its response value through the comparison helper. Setup is
// excluded from timing, and returned values are discarded in the loop.
func BenchmarkUnwrapV1Internal_Old_Small(b *testing.B) {
	body := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"hello world"}]}}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5}}}`)
	b.ResetTimer()
	for range b.N {
		_, _ = unwrapV1InternalResponseOld(body)
	}
}

// BenchmarkUnwrapV1Internal_New_Small repeatedly unwraps the small response
// fixture through the platform adapter. Setup is excluded from timing, and
// returned values are discarded during the benchmark loop.
func BenchmarkUnwrapV1Internal_New_Small(b *testing.B) {
	body := []byte(`{"response":{"candidates":[{"content":{"parts":[{"text":"hello world"}]}}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5}}}`)
	svc := newAntigravityFixture(antigravityDependencies{})
	b.ResetTimer()
	for range b.N {
		_, _ = googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).UnwrapV1InternalResponse(body)
	}
}

// BenchmarkUnwrapV1Internal_Old_Large repeatedly decodes the approximate large
// wrapper and remarshals its response value through the comparison helper.
// Fixture setup is excluded from timing, and loop results are discarded.
func BenchmarkUnwrapV1Internal_Old_Large(b *testing.B) {
	// Use an approximate 10 KiB target for comparison with the small unwrap
	// fixtures measured by the companion benchmark.
	body := generateLargeUnwrapJSON(10 * 1024)
	b.ResetTimer()
	for range b.N {
		_, _ = unwrapV1InternalResponseOld(body)
	}
}

// BenchmarkUnwrapV1Internal_New_Large repeatedly unwraps the approximate large
// fixture through the platform adapter. Fixture construction is excluded from
// timing, and returned values are discarded during the benchmark loop.
func BenchmarkUnwrapV1Internal_New_Large(b *testing.B) {
	// Use an approximate 10 KiB target for comparison with the small unwrap
	// fixtures measured by the companion benchmark.
	body := generateLargeUnwrapJSON(10 * 1024)
	svc := newAntigravityFixture(antigravityDependencies{})
	b.ResetTimer()
	for range b.N {
		_, _ = googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).UnwrapV1InternalResponse(body)
	}
}

// TestExtractImageSize_ValidSizes supplies supported one-, two-, and four-K
// image-size hints to extraction and billing normalization. The selected tier
// matches each exercised request value after both operations.
func TestExtractImageSize_ValidSizes(t *testing.T) {
	svc := newAntigravityFixture(antigravityDependencies{})

	body := []byte(`{"generationConfig":{"imageConfig":{"imageSize":"1K"}}}`)
	require.Equal(t, "1K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"generationConfig":{"imageConfig":{"imageSize":"2K"}}}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"generationConfig":{"imageConfig":{"imageSize":"4K"}}}`)
	require.Equal(t, "4K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))
}

// TestExtractImageSize_CaseInsensitive supplies lowercase image-size hints to
// extraction and billing normalization. The returned tiers use the expected
// uppercase labels for both exercised request size variants.
func TestExtractImageSize_CaseInsensitive(t *testing.T) {
	svc := newAntigravityFixture(antigravityDependencies{})

	body := []byte(`{"generationConfig":{"imageConfig":{"imageSize":"1k"}}}`)
	require.Equal(t, "1K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"generationConfig":{"imageConfig":{"imageSize":"4k"}}}`)
	require.Equal(t, "4K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))
}

// TestExtractImageSize_Default omits generation configuration, image
// configuration, and imageSize in separate inputs. Extraction plus billing
// normalization chooses the two-K default for every absent-hint fixture.
func TestExtractImageSize_Default(t *testing.T) {
	svc := newAntigravityFixture(antigravityDependencies{})

	body := []byte(`{"contents":[]}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"generationConfig":{"temperature":0.7}}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"generationConfig":{"imageConfig":{}}}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))
}

// TestExtractImageSize_InvalidJSON supplies malformed text and truncated JSON
// to extraction and billing normalization. Both inputs select the two-K default
// used when no valid image hint is available.
func TestExtractImageSize_InvalidJSON(t *testing.T) {
	svc := newAntigravityFixture(antigravityDependencies{})

	body := []byte(`not valid json`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"broken":`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))
}

// TestExtractImageSize_EmptySize supplies empty and whitespace-only imageSize
// strings to extraction and billing normalization. Both request variants select
// the two-K default after their hint values are interpreted.
func TestExtractImageSize_EmptySize(t *testing.T) {
	svc := newAntigravityFixture(antigravityDependencies{})

	body := []byte(`{"generationConfig":{"imageConfig":{"imageSize":""}}}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"generationConfig":{"imageConfig":{"imageSize":"   "}}}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))
}

// TestExtractImageSize_InvalidSize supplies unsupported three-, eight-K, and
// arbitrary size hints. Extraction plus billing normalization chooses the two-K
// default for each exercised unsupported request value.
func TestExtractImageSize_InvalidSize(t *testing.T) {
	svc := newAntigravityFixture(antigravityDependencies{})

	body := []byte(`{"generationConfig":{"imageConfig":{"imageSize":"3K"}}}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"generationConfig":{"imageConfig":{"imageSize":"8K"}}}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))

	body = []byte(`{"generationConfig":{"imageConfig":{"imageSize":"invalid"}}}`)
	require.Equal(t, "2K", pricing.NormalizeImageBillingTierOrDefault(googleforward.AntigravityResponseForTest(svc, &googleforward.AttemptForTest{}).ExtractImageInputSize(body)))
}

// Write forwards calls until writes reaches failAfter, then returns zero with a
// disconnect error. The counter advances before delegated output, allowing
// response tests to choose the failing client-write point.
func (w *antigravityFailingWriter) Write(p []byte) (int, error) {
	if w.writes >= w.failAfter {
		return 0, errors.New("write failed: client disconnected")
	}
	w.writes++
	return w.ResponseWriter.Write(p)
}

// unwrapV1InternalResponseOld decodes and remarshals a present response value
// for benchmark comparison. Invalid JSON returns its decoding error, while an
// absent response property returns the supplied body bytes.
func unwrapV1InternalResponseOld(body []byte) ([]byte, error) {
	var outer map[string]any
	if err := json.Unmarshal(body, &outer); err != nil {
		return nil, err
	}
	if resp, ok := outer["response"]; ok {
		return json.Marshal(resp)
	}
	return body, nil
}

// generateLargeUnwrapJSON constructs a wrapped fixture using approximate per-
// part encoding overhead until minSize is reached. The result is benchmark
// input, and callers must not treat minSize as its exact encoded length.
func generateLargeUnwrapJSON(minSize int) []byte {
	parts := make([]map[string]string, 0)
	current := 0
	for current < minSize {
		text := fmt.Sprintf("这是第 %d 段内容，用于填充 JSON 到目标大小。", len(parts)+1)
		parts = append(parts, map[string]string{"text": text})
		// The added overhead approximates per-part JSON syntax, so
		// minSize drives payload construction without promising an
		// exact encoded byte length.
		current += len(text) + 20
	}
	inner := map[string]any{
		"candidates": []map[string]any{
			{"content": map[string]any{"parts": parts}},
		},
		"usageMetadata": map[string]any{
			"promptTokenCount":     100,
			"candidatesTokenCount": 50,
		},
	}
	outer := map[string]any{"response": inner}
	b, _ := json.Marshal(outer)
	return b
}
