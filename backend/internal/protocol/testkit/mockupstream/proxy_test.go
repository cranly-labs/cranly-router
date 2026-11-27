package mockupstream

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestHandlerModeReplay sends sequential requests to a ModeReplay Handler with
// and without a configured Script.Default fallback. Ordered replies must emit in
// arrival sequence before falling back to Default or logging exhaustion errors.
func TestHandlerModeReplay(t *testing.T) {
	t.Run("replays ordered replies then falls back to Default", func(t *testing.T) {
		h := NewWithConfig(Config{
			Mode: ModeReplay,
			Replay: []testkit.Reply{
				{Raw: "data: {\"id\":\"turn-0\",\"choices\":[]}\n\ndata: [DONE]\n\n"},
				{Raw: "data: {\"id\":\"turn-1\",\"choices\":[]}\n\ndata: [DONE]\n\n"},
			},
			Script: testkit.Script{
				Default: testkit.Reply{Raw: `{"id":"default-fallback","choices":[]}`},
			},
		})

		sendChat := func() (int, string) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"gpt-5.4","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
			h.ServeHTTP(rec, req)
			return rec.Code, rec.Body.String()
		}

		code0, body0 := sendChat()
		require.Equal(t, http.StatusOK, code0)
		require.Contains(t, body0, "turn-0")

		code1, body1 := sendChat()
		require.Equal(t, http.StatusOK, code1)
		require.Contains(t, body1, "turn-1")

		code2, body2 := sendChat()
		require.Equal(t, http.StatusOK, code2)
		require.Contains(t, body2, "default-fallback")

		h.AssertNoViolations(t)
		reqs := h.Requests()
		require.Len(t, reqs, 3)
		require.Equal(t, 0, reqs[0].MatchedTurn)
		require.Equal(t, 1, reqs[1].MatchedTurn)
		require.Equal(t, -1, reqs[2].MatchedTurn)
		require.True(t, reqs[2].UsedDefault)
	})

	t.Run("exhausted replay without Default logs violation", func(t *testing.T) {
		h := NewWithConfig(Config{
			Mode:   ModeReplay,
			Replay: []testkit.Reply{{Raw: `{"id":"only-one"}`}},
		})
		rec1 := httptest.NewRecorder()
		h.ServeHTTP(rec1, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.4","input":"first"}`)))
		require.Equal(t, http.StatusOK, rec1.Code)

		rec2 := httptest.NewRecorder()
		h.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.4","input":"second"}`)))
		require.Equal(t, http.StatusBadRequest, rec2.Code)
		require.Len(t, h.Violations(), 1)
		require.Contains(t, h.Violations()[0].CheckErrors[0], "replay sequence exhausted")
	})
}

// TestHandlerModeProxyRecordAndCorpusImport fronts a gzip-compressing SSE test
// upstream with ModeProxyRecord while incoming requests carry Accept-Encoding
// and capture bundles write to _incoming. Outbound Transport must decompress
// gzip into UTF-8 plaintext so capture serialization, AssertNoViolations, and
// ImportCorpusDir all succeed cleanly.
func TestHandlerModeProxyRecordAndCorpusImport(t *testing.T) {
	var upstreamSawPath string
	var upstreamSawQuery string
	var upstreamSawKeepAlive string
	var upstreamSawAuth string
	var upstreamSawAcceptEncoding string

	upstreamStream := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_live\"}}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_live\",\"output\":[]}}\n\n"
	realUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamSawPath = r.URL.Path
		upstreamSawQuery = r.URL.RawQuery
		upstreamSawKeepAlive = r.Header.Get("Keep-Alive")
		upstreamSawAuth = r.Header.Get("Authorization")
		upstreamSawAcceptEncoding = r.Header.Get("Accept-Encoding")
		_, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Connection", "X-Internal-Hop")
		w.Header().Set("X-Internal-Hop", "drop-me")
		w.Header().Set("X-Request-Id", "req-live-1")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		require.True(t, ok)

		gz := gzip.NewWriter(w)
		_, err := io.WriteString(gz, upstreamStream[:60])
		require.NoError(t, err)
		require.NoError(t, gz.Flush())
		flusher.Flush()

		_, err = io.WriteString(gz, upstreamStream[60:])
		require.NoError(t, err)
		require.NoError(t, gz.Close())
		flusher.Flush()
	}))
	t.Cleanup(realUpstream.Close)

	tempRoot := t.TempDir()
	incomingDir := filepath.Join(tempRoot, "_incoming")
	destCorpusDir := filepath.Join(tempRoot, "corpus")

	proxyHandler := NewWithConfig(Config{
		Mode:       ModeProxyRecord,
		TargetURL:  realUpstream.URL + "/api/v1?base_flag=1",
		HTTPClient: realUpstream.Client(),
		Capture: CaptureOptions{
			Dir:        incomingDir,
			FilePrefix: "codex-canary",
			Meta: testkit.CorpusMeta{
				Client:     "codex",
				Version:    "0.169.0",
				Provider:   "openai",
				Scenario:   "lab_text",
				CapturedAt: "2026-11-21",
			},
		},
	})
	proxySrv := httptest.NewServer(proxyHandler)
	t.Cleanup(proxySrv.Close)

	// Disable automatic decompression on the downstream test client to prove
	// proxySrv itself emits decompressed UTF-8 SSE rather than raw gzip bytes.
	rawDownstreamClient := &http.Client{
		Transport: &http.Transport{DisableCompression: true},
	}
	t.Cleanup(rawDownstreamClient.CloseIdleConnections)

	req, err := http.NewRequest(
		http.MethodPost,
		proxySrv.URL+"/v1/responses?trace=abc",
		strings.NewReader(`{"model":"gpt-5.4","input":"hello from /Users/alice","stream":true}`),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip, br, zstd")
	req.Header.Set("Authorization", "Bearer sk-proj-1234567890abcdefghijklmnop")
	req.Header.Set("User-Agent", "codex-cli/0.169.0")
	req.Header.Set("OpenAI-Beta", "responses=v1")
	req.Header.Set("Keep-Alive", "timeout=5")

	resp, err := rawDownstreamClient.Do(req)
	require.NoError(t, err)
	gotBody, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	require.NoError(t, err)

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Empty(t, resp.Header.Get("Content-Encoding"))
	require.Equal(t, upstreamStream, string(gotBody))
	require.Equal(t, "/api/v1/responses", upstreamSawPath)
	require.Equal(t, "base_flag=1&trace=abc", upstreamSawQuery)
	require.Equal(t, "gzip", upstreamSawAcceptEncoding, "caller br/zstd must be stripped so Go Transport manages gzip")
	require.Empty(t, upstreamSawKeepAlive, "hop-by-hop Keep-Alive header must not be forwarded")
	require.Contains(t, upstreamSawAuth, "Bearer sk-proj-")
	require.Empty(t, resp.Header.Get("X-Internal-Hop"), "Connection-nominated response header must be stripped")
	require.Equal(t, "req-live-1", resp.Header.Get("X-Request-Id"))

	proxyHandler.AssertNoViolations(t)
	reqs := proxyHandler.Requests()
	require.Len(t, reqs, 1)
	require.Equal(t, protocolOpenAIResponses, reqs[0].Protocol)
	require.Equal(t, "gpt-5.4", reqs[0].Summary.Model)
	require.Equal(t, upstreamStream, string(reqs[0].ResponseBody))

	// Import written *.capture.json through testkit.ImportCorpusDir and verify secret scrubbing.
	importRep, err := testkit.ImportCorpusDir(incomingDir, destCorpusDir)
	require.NoError(t, err)
	require.Len(t, importRep.ImportedFiles, 4)

	verifyRep, err := testkit.VerifyCorpusDir(destCorpusDir)
	require.NoError(t, err)
	require.Equal(t, 4, verifyRep.CheckedFiles)

	importedCorpus := testkit.NewCorpus(os.DirFS(filepath.Dir(destCorpusDir)))
	clientFixtures, err := importedCorpus.QueryFixtures(testkit.FixtureQuery{Source: testkit.SourceCorpusClient})
	require.NoError(t, err)
	require.Len(t, clientFixtures, 1)
	require.Contains(t, string(clientFixtures[0].Data), testkit.RedactedHomePlaceholder)
	require.NotContains(t, string(clientFixtures[0].Data), "/Users/alice")
	require.Equal(t, "codex-cli/0.169.0", clientFixtures[0].Meta.Headers["user-agent"])
	require.Equal(t, "responses=v1", clientFixtures[0].Meta.Headers["openai-beta"])
	require.NotContains(t, clientFixtures[0].Meta.Headers, "authorization")

	upstreamFixtures, err := importedCorpus.QueryFixtures(testkit.FixtureQuery{Source: testkit.SourceCorpusUpstream})
	require.NoError(t, err)
	require.Len(t, upstreamFixtures, 1)
	require.Equal(t, upstreamStream, string(upstreamFixtures[0].Data))
}

// TestBuildProxyTargetURLVersionDeduplication checks URL path joining across
// root, versioned, multi-segment versioned, and non-versioned base URLs.
// Trailing /v1 and /v1beta segments must deduplicate without corrupting
// non-segment suffixes such as /apiv1.
func TestBuildProxyTargetURLVersionDeduplication(t *testing.T) {
	cases := []struct {
		name       string // subtest scenario name
		targetBase string // configured TargetURL base string
		reqRawURL  string // incoming request path and query
		wantURL    string // expected joined outbound URL
	}{
		{
			name:       "bare origin with v1 request",
			targetBase: "https://api.openai.com",
			reqRawURL:  "/v1/responses",
			wantURL:    "https://api.openai.com/v1/responses",
		},
		{
			name:       "root v1 base with v1 request",
			targetBase: "https://api.openai.com/v1/",
			reqRawURL:  "/v1/chat/completions",
			wantURL:    "https://api.openai.com/v1/chat/completions",
		},
		{
			name:       "prefixed api/v1 base with v1 chat request",
			targetBase: "https://openrouter.ai/api/v1",
			reqRawURL:  "/v1/chat/completions",
			wantURL:    "https://openrouter.ai/api/v1/chat/completions",
		},
		{
			name:       "prefixed anthropic/v1 base with v1 messages request",
			targetBase: "https://api.minimaxi.com/anthropic/v1",
			reqRawURL:  "/v1/messages?beta=true",
			wantURL:    "https://api.minimaxi.com/anthropic/v1/messages?beta=true",
		},
		{
			name:       "prefixed proxy/v1beta base with v1beta gemini request",
			targetBase: "https://example.com/proxy/v1beta",
			reqRawURL:  "/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
			wantURL:    "https://example.com/proxy/v1beta/models/gemini-2.5-flash:streamGenerateContent?alt=sse",
		},
		{
			name:       "non-segment apiv1 prefix retains v1 request segment",
			targetBase: "https://example.com/apiv1",
			reqRawURL:  "/v1/chat/completions",
			wantURL:    "https://example.com/apiv1/v1/chat/completions",
		},
		{
			name:       "unversioned custom prefix appends v1 request",
			targetBase: "https://example.com/custom",
			reqRawURL:  "/v1/responses",
			wantURL:    "https://example.com/custom/v1/responses",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			parsedReq, err := url.Parse(tc.reqRawURL)
			require.NoError(t, err)
			got, err := buildProxyTargetURL(tc.targetBase, parsedReq)
			require.NoError(t, err)
			require.Equal(t, tc.wantURL, got)
		})
	}
}
