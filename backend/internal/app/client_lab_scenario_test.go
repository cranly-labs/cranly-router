package app

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit/mockupstream"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// client_lab_scenario_test.go drives real Codex CLI, Claude Code, and pi coding
// agent subprocesses against registered gateway routes in http_routes.go,
// gateway_middleware.go, gateway_selection.go, gateway_openai_text_http.go,
// gateway_openai_text_execution.go, gateway_openai_attempts.go,
// gateway_response_output.go, gateway_grok_execution.go,
// gateway_openai_ws_http.go, gateway_ws_connections.go,
// gateway_messages_http.go, gateway_messages_execution.go,
// gateway_message_attempts.go, gateway_google.go, gateway_count_tokens.go,
// gateway_openai_tokens.go, and gateway_models.go. Pure self-consistency laws
// cannot expose shared protocol misunderstandings between an encoder and
// decoder, so T6 pairs real CLI request builders and SSE consumers with strict
// mockupstream validators.
//
// Scripted runs execute inside isolated temporary directories with synthetic
// lab API keys and deterministic or per-run random markers, verifying native
// passthrough controls and cross-protocol IR conversion routes end-to-end.
// Record mode captures untouched client requests at the gateway ingress into
// testdata/corpus/_incoming/ for cmd/corpus sanitization, while live mode
// proxies to real providers only when TOKENROUTER_CLIENTLAB_CONTAINER=1 proves
// execution inside the disposable tools/clientlab/Dockerfile container.

const (
	// Environment variable names gate T6 subprocess execution, select scripted,
	// record, or live mode, and enforce the live-mode container boundary.
	clientLabEnableEnv    = "TOKENROUTER_CLIENTLAB"              // enables T6 CLI subprocess suite when set to 1
	clientLabModeEnv      = "TOKENROUTER_CLIENTLAB_MODE"         // selects scripted, record, or live mode
	clientLabContainerEnv = "TOKENROUTER_CLIENTLAB_CONTAINER"    // confirms execution inside disposable Dockerfile container
	clientLabIncomingEnv  = "TOKENROUTER_CLIENTLAB_INCOMING_DIR" // overrides destination _incoming capture directory
	clientLabTestdataDir  = "../protocol/testkit/testdata"       // relative path to testkit/testdata for record-mode _incoming captures

	// ClientLab operating modes control whether mockupstream serves scripted
	// SSE fixtures, persists raw client captures, or proxies to live providers.
	clientLabModeScripted clientLabMode = "scripted" // scripted fake upstream without disk capture
	clientLabModeRecord   clientLabMode = "record"   // scripted fake upstream with ingress request capture
	clientLabModeLive     clientLabMode = "live"     // live provider proxy-record inside disposable container

	// Pinned npm package versions match tools/clientlab/package.json so corpus
	// metadata and template assertions stay aligned with the lockfile.
	pinnedCodexVersion      = "0.171.0" // locked @openai/codex version
	pinnedClaudeCodeVersion = "2.1.330" // locked @anthropic-ai/claude-code version
	pinnedPiVersion         = "1.6.0"   // locked @earendil-works/pi-coding-agent version

	// T6 law, scenario slugs, and synthetic auth constants identify client
	// laboratory cells and local gateway credentials.
	lawT6Client                     = "T6_CLIENT"                    // law label for T6 real-client cells
	clientLabScenarioText           = "lab_text"                     // single-turn text verification scenario slug
	clientLabScenarioTool           = "lab_tool"                     // two-turn shell tool execution scenario slug
	clientLabScenarioToolParallel   = "lab_tool_parallel"            // single-turn parallel shell tool execution scenario slug
	clientLabScenarioReasoningTool  = "lab_reasoning_tool"           // two-turn reasoning token plus tool execution scenario slug
	clientLabScenarioSwitchProvider = "lab_switch_provider"          // two-turn cross-provider protocol switch scenario slug
	clientLabScenarioFailover       = "lab_failover"                 // pre-first-byte HTTP 529 provider failover scenario slug
	clientLabScenarioTruncated      = "lab_truncated"                // mid-stream upstream disconnect scenario slug
	clientLabScenarioCustomTool     = "lab_custom_tool"              // Codex apply_patch freeform custom tool scenario slug
	clientLabScenarioLongStream     = "lab_long_stream"              // multi-chunk long text plus large tool args scenario slug
	clientLabAPIKeyEnvName          = "TOKENROUTER_LAB_API_KEY"      // env var carrying the synthetic gateway key
	clientLabSyntheticAPIKey        = "lab-api-key"                  // synthetic bearer/API key value for local routes
	clientLabRecordedTokenSeed      = "tr_clientlab_recorded_marker" // deterministic marker prefix in record mode
	clientLabCaptureDate            = "2026-11-21"                   // capture date recorded in corpus meta.json

	// Subprocess execution budgets bound scripted and live CLI invocations so
	// malformed streams fail promptly instead of hanging test suites.
	clientLabScriptedTimeout = 45 * time.Second  // per-invocation timeout for scripted and record runs
	clientLabLiveTimeout     = 240 * time.Second // per-invocation timeout for live provider canaries

	// T6 failure rule codes classify gateway status, subprocess exit, client
	// event-stream errors, file side effects, turn-2 replay, and usage counts.
	ruleT6Gateway5xx              = "t6_gateway_5xx"               // gateway returned one or more HTTP 5xx responses
	ruleT6ClientExitNonZero       = "t6_client_exit_nonzero"       // CLI subprocess exited with non-zero status or error
	ruleT6ClientErrorEvent        = "t6_client_error_event"        // CLI JSONL stream reported an error or missing completion
	ruleT6MissingExpectedText     = "t6_missing_expected_text"     // final assistant text omitted expected marker string
	ruleT6ToolFileMismatch        = "t6_tool_file_mismatch"        // shell tool did not write expected marker to result.txt
	ruleT6ToolExecCountMismatch   = "t6_tool_exec_count_mismatch"  // CLI JSONL stream did not record exactly one tool run
	ruleT6MissingTurn2ToolResult  = "t6_missing_turn2_tool_result" // upstream did not receive a Turn 2 tool result request
	ruleT6UsageSettlementMismatch = "t6_usage_settlement_mismatch" // settled UsageLog count disagreed with HTTP 200 turns

	// Client driver identifiers map each clientLabProfile to its template
	// renderer, subprocess launcher, and JSONL output parser.
	clientDriverCodex      clientDriverKind = "codex"       // OpenAI Codex CLI adapter
	clientDriverClaudeCode clientDriverKind = "claude-code" // Anthropic Claude Code CLI adapter
	clientDriverPi         clientDriverKind = "pi"          // pi coding agent CLI adapter
)

var (
	// clientLabScenarios lists the core single-turn text and two-turn shell
	// tool T6 scenarios executed across every client profile and upstream route.
	clientLabScenarios = []string{
		clientLabScenarioText,
		clientLabScenarioTool,
	}

	// safeInheritedEnvKeys lists host environment variables preserved when
	// spawning isolated CLI subprocesses so provider keys and proxies never leak.
	safeInheritedEnvKeys = []string{
		"PATH",
		"TMPDIR",
		"TMP",
		"TEMP",
		"LANG",
		"LC_ALL",
		"TERM",
		"USER",
		"LOGNAME",
		"SHELL",
		"SYSTEMROOT",
		"COMSPEC",
		"PATHEXT",
		"WINDIR",
	}

	// clientLabProfiles defines the seven CLI configurations covering Codex,
	// Claude Code (--bare and default), and pi across its four wire APIs.
	clientLabProfiles = []clientLabProfile{
		{
			slug:                 "codex",
			driver:               clientDriverCodex,
			corpusClient:         "codex",
			version:              pinnedCodexVersion,
			clientProtocol:       protocol.ProtocolOpenAIResponses,
			clientModel:          "gpt-5.5",
			shellToolName:        "exec_command",
			shellCommandField:    "cmd",
			corpusScenarioSuffix: "",
			nativeProviderKind:   "openai",
			providerKinds:        []string{"openai", "openai_chat", "anthropic", "gemini"},
		},
		{
			slug:                 "claude_bare",
			driver:               clientDriverClaudeCode,
			claudeBare:           true,
			corpusClient:         "claude-code",
			version:              pinnedClaudeCodeVersion,
			clientProtocol:       protocol.ProtocolAnthropicMessages,
			clientModel:          "claude-sonnet-4-5",
			shellToolName:        "Bash",
			shellCommandField:    "command",
			corpusScenarioSuffix: "_bare",
			nativeProviderKind:   "anthropic",
			providerKinds:        []string{"anthropic", "openai", "openai_chat", "gemini"},
		},
		{
			slug:                 "claude_default",
			driver:               clientDriverClaudeCode,
			claudeBare:           false,
			corpusClient:         "claude-code",
			version:              pinnedClaudeCodeVersion,
			clientProtocol:       protocol.ProtocolAnthropicMessages,
			clientModel:          "claude-sonnet-4-5",
			shellToolName:        "Bash",
			shellCommandField:    "command",
			corpusScenarioSuffix: "",
			nativeProviderKind:   "anthropic",
			providerKinds:        []string{"anthropic", "openai", "openai_chat", "gemini"},
		},
		{
			slug:                 "pi_chat",
			driver:               clientDriverPi,
			piAPI:                "openai-completions",
			corpusClient:         "pi",
			version:              pinnedPiVersion,
			clientProtocol:       protocol.ProtocolOpenAIChatCompletions,
			clientModel:          "gpt-4o",
			shellToolName:        "bash",
			shellCommandField:    "command",
			corpusScenarioSuffix: "_chat",
			nativeProviderKind:   "openai_chat",
			providerKinds:        []string{"openai_chat", "openai", "anthropic", "gemini"},
		},
		{
			slug:                 "pi_responses",
			driver:               clientDriverPi,
			piAPI:                "openai-responses",
			corpusClient:         "pi",
			version:              pinnedPiVersion,
			clientProtocol:       protocol.ProtocolOpenAIResponses,
			clientModel:          "gpt-5",
			shellToolName:        "bash",
			shellCommandField:    "command",
			corpusScenarioSuffix: "_responses",
			nativeProviderKind:   "openai",
			providerKinds:        []string{"openai", "openai_chat", "anthropic", "gemini"},
		},
		{
			slug:                 "pi_messages",
			driver:               clientDriverPi,
			piAPI:                "anthropic-messages",
			corpusClient:         "pi",
			version:              pinnedPiVersion,
			clientProtocol:       protocol.ProtocolAnthropicMessages,
			clientModel:          "claude-sonnet-4-5",
			shellToolName:        "bash",
			shellCommandField:    "command",
			corpusScenarioSuffix: "_messages",
			nativeProviderKind:   "anthropic",
			providerKinds:        []string{"anthropic", "openai", "openai_chat", "gemini"},
		},
		{
			slug:                 "pi_gemini",
			driver:               clientDriverPi,
			piAPI:                "google-generative-ai",
			corpusClient:         "pi",
			version:              pinnedPiVersion,
			clientProtocol:       protocol.ProtocolGeminiGenerateContent,
			clientModel:          "gemini-2.5-pro",
			shellToolName:        "bash",
			shellCommandField:    "command",
			corpusScenarioSuffix: "_gemini",
			nativeProviderKind:   "gemini",
			providerKinds:        []string{"gemini"},
		},
	}
)

// clientLabMode selects whether T6 scenarios use scripted upstreams, record
// raw client captures, or proxy live provider traffic.
type clientLabMode string

// clientDriverKind selects the CLI binary adapter used to render configs,
// launch subprocesses, and parse structured JSONL stdout streams.
type clientDriverKind string

// clientLabProfile describes one CLI agent configuration and its target upstreams.
type clientLabProfile struct {
	slug   string           // unique route prefix such as "claude_bare" or "pi_chat"
	driver clientDriverKind // CLI driver implementation kind
	// claudeBare adds --bare when driver is clientDriverClaudeCode so T6 covers
	// both minimal-tool and full-beta Claude Code request surfaces.
	claudeBare bool // enables Claude Code minimal --bare mode
	// piAPI names the model-level wire API selected in models.json when driver
	// is clientDriverPi; non-pi profiles leave it empty.
	piAPI                string              // pi model API identifier in models.json
	corpusClient         string              // corpus/clients/<client> directory name
	version              string              // pinned npm package version
	clientProtocol       protocol.ProtocolID // wire protocol spoken to the gateway
	clientModel          string              // model ID sent by the CLI client
	shellToolName        string              // built-in shell tool name declared by this CLI
	shellCommandField    string              // JSON property name carrying the shell command string
	corpusScenarioSuffix string              // scenario slug suffix distinguishing recorded variants
	nativeProviderKind   string              // provider kind serving native passthrough control
	providerKinds        []string            // ordered provider kinds tested with this profile
}

// clientLabRoute pairs one clientLabProfile with one upstream labProviderKindSpec.
type clientLabRoute struct {
	slug     string              // stable route cell name "<profile>_via_<provider>"
	profile  clientLabProfile    // CLI client configuration
	provider labProviderKindSpec // target upstream provider specification
	isNative bool                // whether client and upstream protocols match natively
}

// clientLabInvocation bundles rendered config directories, working directory,
// command arguments, and sanitized environment variables for one CLI run.
type clientLabInvocation struct {
	executable string   // absolute path to the CLI binary under node_modules/.bin
	args       []string // command-line arguments passed to executable
	env        []string // complete sanitized environment slice
	workDir    string   // isolated working directory for file side effects
	configDir  string   // isolated client config directory path
}

// clientLabProcessResult holds raw exit status, stdout/stderr buffers, and
// parsed JSONL stream observations from one CLI subprocess execution.
type clientLabProcessResult struct {
	exitCode       int      // OS process exit status, or -1 on spawn/cancel failure
	runErr         error    // error returned by exec.Cmd.Run, or nil on exit 0
	stdout         string   // complete UTF-8 stdout stream
	stderr         string   // complete UTF-8 stderr stream
	finalText      string   // final assistant message text extracted from JSONL events
	toolExecutions int      // count of completed tool executions observed in JSONL events
	errorMessages  []string // client-reported error events or terminal error reasons
	completed      bool     // whether the client emitted its normal completion event
}

// gatewayObservedExchange records status and classification for one HTTP
// request handled at the gateway ingress server.
type gatewayObservedExchange struct {
	method       string // HTTP request method
	path         string // request URL path
	status       int    // committed HTTP status code
	isGeneration bool   // whether request targeted a generation POST route
}

// clientLabGatewayRecorder wraps the Gin gateway handler to observe HTTP status
// codes for 5xx/settlement assertions and to persist untouched client request
// captures in record mode.
type clientLabGatewayRecorder struct {
	next http.Handler // underlying protocolLabRouter Gin engine
	// captureDir enables raw client request bundle persistence into the named
	// _incoming directory when non-empty; empty values disable disk writes.
	captureDir string             // destination _incoming directory path
	baseMeta   testkit.CorpusMeta // provenance metadata for written capture bundles

	// mu serializes concurrent reads and updates of exchanges, captureTurn, and
	// captureErrors across gateway ServeHTTP goroutines and test summary
	// inspections after subprocess exit.
	mu            sync.Mutex                // protects exchanges, captureTurn, and captureErrors
	exchanges     []gatewayObservedExchange // chronological gateway HTTP exchanges
	captureTurn   int                       // one-based turn counter for generation captures
	captureErrors []string                  // disk capture write failures in record mode
}

// statusCapturingResponseWriter wraps http.ResponseWriter to track the committed
// HTTP status code while forwarding Flush and Hijack capabilities.
type statusCapturingResponseWriter struct {
	http.ResponseWriter // underlying HTTP response writer

	// status and wroteHeader record the first HTTP status code committed by
	// WriteHeader or implicit 200 Write so later calls cannot overwrite it.
	status      int  // committed HTTP response status code
	wroteHeader bool // whether status has already been committed
}

// liveProviderEndpoint holds base URL, credential, and optional model override
// read from environment variables for one live-canary upstream family.
type liveProviderEndpoint struct {
	baseURL string // live provider base URL
	apiKey  string // live provider bearer or API key secret
	// model overrides labProviderKindSpec.upstreamModel when non-empty; empty
	// values retain the default upstream model configured for that provider kind.
	model string // upstream model override from environment
}

// liveCanaryConfig collects environment-supplied endpoints across all five
// upstream provider families required by Stage 7 Step 4 live canaries.
type liveCanaryConfig struct {
	openai    liveProviderEndpoint // native OpenAI Responses endpoint and key
	anthropic liveProviderEndpoint // Anthropic Messages endpoint and key
	gemini    liveProviderEndpoint // Google Gemini GenerateContent endpoint and key
	grok      liveProviderEndpoint // xAI Grok Responses endpoint and key
	deepseek  liveProviderEndpoint // DeepSeek Chat Completions endpoint and key
}

// liveCanaryPairSpec binds one CLI profile to its Turn 1 IR-converted or
// dialect-lowered provider and its Turn 2 native passthrough provider.
type liveCanaryPairSpec struct {
	slug          string               // stable pair identifier within its canary group
	profile       clientLabProfile     // CLI agent profile invoked against the gateway
	turn1Provider labProviderKindSpec  // Turn 1 converted or tool-lowered upstream spec
	turn1Endpoint liveProviderEndpoint // live endpoint backing Turn 1
	turn2Native   labProviderKindSpec  // Turn 2 native-protocol upstream spec
	turn2Endpoint liveProviderEndpoint // live endpoint backing Turn 2 native replay
	carrierScheme string               // expected Turn 1 trc1 carrier scheme label
}

// liveCanaryGroupSpec groups the route pairs belonging to one Section 6.8 Step 4
// live canary family.
type liveCanaryGroupSpec struct {
	groupSlug   string               // Section 6.8 Step 4 group identifier
	description string               // human-readable canary purpose summary
	pairs       []liveCanaryPairSpec // ordered CLI-to-upstream canary pairs
}

// liveCanaryRetryTransport wraps an outbound http.RoundTripper used by
// mockupstream's ModeProxyRecord during live canary runs so transient upstream
// HTTP 429 rate-limit windows, HTTP 503 demand spikes, and immediate SSE
// account-concurrency rejections back off cleanly before returning to
// single-provider lab routers.
type liveCanaryRetryTransport struct {
	// base, defaultWait, maxWait, and maxRetries configure retry behavior at
	// the live test-proxy boundary: a nil base falls back to
	// http.DefaultTransport, non-positive defaultWait falls back to 15s,
	// non-positive maxWait leaves explicit retry delays uncapped, and
	// non-positive maxRetries disables retries.
	base        http.RoundTripper // underlying outbound HTTP transport
	defaultWait time.Duration     // fallback 429 wait when upstream omits retry hints
	maxWait     time.Duration     // upper bound on a single retry backoff wait
	maxRetries  int               // maximum retry attempts on transient upstream limits
}

// liveCanaryPrefixReadCloser replays pre-inspected leading SSE bytes ahead of
// the remaining upstream response body while delegating Close to the original stream.
type liveCanaryPrefixReadCloser struct {
	io.Reader // combined prefix buffer and live upstream stream
	io.Closer // underlying HTTP response body closer
}

// TestClientLabConfigTemplatesAndGuards runs on every go test invocation without
// requiring TOKENROUTER_CLIENTLAB=1. It protects pinned npm versions, template
// rendering, .dockerignore exclusions, Dockerfile container entrypoint rules,
// live-mode safety rejection, client JSONL parsers, committed corpus fixtures,
// and client_lab route-table invariants.
func TestClientLabConfigTemplatesAndGuards(t *testing.T) {
	labRoot := resolveClientLabDir(t)

	t.Run("package manifest and lockfile pin exact client versions", func(t *testing.T) {
		pkgBytes, err := os.ReadFile(filepath.Join(labRoot, "package.json"))
		require.NoError(t, err)
		require.Equal(t, pinnedCodexVersion, gjson.GetBytes(pkgBytes, `dependencies.@openai/codex`).String())
		require.Equal(t, pinnedClaudeCodeVersion, gjson.GetBytes(pkgBytes, `dependencies.@anthropic-ai/claude-code`).String())
		require.Equal(t, pinnedPiVersion, gjson.GetBytes(pkgBytes, `dependencies.@earendil-works/pi-coding-agent`).String())

		lockBytes, err := os.ReadFile(filepath.Join(labRoot, "package-lock.json"))
		require.NoError(t, err)
		require.Equal(t, int64(3), gjson.GetBytes(lockBytes, "lockfileVersion").Int())
		require.Equal(t, pinnedCodexVersion, gjson.GetBytes(lockBytes, `packages.node_modules/@openai/codex.version`).String())
		require.Equal(t, pinnedClaudeCodeVersion, gjson.GetBytes(lockBytes, `packages.node_modules/@anthropic-ai/claude-code.version`).String())
		require.Equal(t, pinnedPiVersion, gjson.GetBytes(lockBytes, `packages.node_modules/@earendil-works/pi-coding-agent.version`).String())
	})

	t.Run("templates render cleanly without unreplaced placeholders", func(t *testing.T) {
		gwURL := "http://127.0.0.1:18080"
		for _, prof := range clientLabProfiles {
			inv := prepareClientLabInvocation(t, labRoot, prof, gwURL, "Sample prompt", false)
			require.DirExists(t, inv.workDir)
			require.DirExists(t, inv.configDir)

			err := filepath.WalkDir(inv.configDir, func(p string, d os.DirEntry, walkErr error) error {
				if walkErr != nil || d.IsDir() {
					return walkErr
				}
				raw, readErr := os.ReadFile(p)
				require.NoError(t, readErr)
				require.NotContains(t, string(raw), "{{", "unreplaced template token in %s", p)
				require.NotContains(t, string(raw), "}}", "unreplaced template token in %s", p)
				require.NotContains(t, string(raw), clientLabSyntheticAPIKey, "rendered config %s should reference env var rather than literal key", p)
				return nil
			})
			require.NoError(t, err)
			if prof.driver == clientDriverPi {
				modelsRaw, readErr := os.ReadFile(filepath.Join(inv.configDir, "models.json"))
				require.NoError(t, readErr)
				query := fmt.Sprintf(`providers.lab.models.#(id==%q).api`, prof.clientModel)
				require.Equal(t, prof.piAPI, gjson.GetBytes(modelsRaw, query).String())
			}
		}
	})

	t.Run("mode parser and live container guard reject unsafe live runs", func(t *testing.T) {
		mode, err := parseClientLabMode("")
		require.NoError(t, err)
		require.Equal(t, clientLabModeScripted, mode)

		for _, raw := range []string{"scripted", "record", "live", "  RECORD "} {
			parsed, parseErr := parseClientLabMode(raw)
			require.NoError(t, parseErr)
			require.NotEmpty(t, parsed)
		}

		_, err = parseClientLabMode("production")
		require.ErrorContains(t, err, "unsupported")

		require.NoError(t, verifyClientLabLiveContainerGate(clientLabModeScripted, ""))
		require.NoError(t, verifyClientLabLiveContainerGate(clientLabModeRecord, ""))
		require.ErrorContains(t, verifyClientLabLiveContainerGate(clientLabModeLive, ""), clientLabContainerEnv)
		require.ErrorContains(t, verifyClientLabLiveContainerGate(clientLabModeLive, "0"), clientLabContainerEnv)
		require.NoError(t, verifyClientLabLiveContainerGate(clientLabModeLive, "1"))

		dockerfileBytes, err := os.ReadFile(filepath.Join(labRoot, "Dockerfile"))
		require.NoError(t, err)
		dockerfileText := string(dockerfileBytes)
		require.Contains(t, dockerfileText, "TOKENROUTER_CLIENTLAB_CONTAINER=1")
		require.Contains(t, dockerfileText, "ENTRYPOINT")
		require.Contains(t, dockerfileText, "USER clientlab")
		require.Contains(t, dockerfileText, "COPY --chown=clientlab:clientlab .dockerignore ./.dockerignore")
		require.Contains(t, dockerfileText, "COPY --chown=clientlab:clientlab tools/clientlab/Dockerfile ./tools/clientlab/Dockerfile")

		repoRoot := filepath.Clean(filepath.Join(labRoot, "..", ".."))
		dockerignoreBytes, err := os.ReadFile(filepath.Join(repoRoot, ".dockerignore"))
		require.NoError(t, err)
		var dockerignoreLines []string
		for line := range strings.SplitSeq(string(dockerignoreBytes), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed != "" && !strings.HasPrefix(trimmed, "#") {
				dockerignoreLines = append(dockerignoreLines, trimmed)
			}
		}
		require.Contains(t, dockerignoreLines, "tools/clientlab/node_modules/")
		require.Contains(t, dockerignoreLines, "backend/internal/protocol/testkit/testdata/corpus/_incoming/")

		routes := buildClientLabRoutes()
		toolScript, _, _ := buildClientLabScenarioScript(routes[0], clientLabScenarioTool, "tok_guard")
		require.Len(t, toolScript.Turns, 2)
		require.NotNil(t, toolScript.Turns[0].Match.HasTools)
		require.True(t, *toolScript.Turns[0].Match.HasTools)
		require.False(t, toolScript.Turns[0].Match.MatchesObserved(0, 0, "gpt-5.4", false, []byte(`{"model":"gpt-5.4","input":"title"}`)))
		require.True(t, toolScript.Turns[0].Match.MatchesObserved(0, 0, "gpt-5.4", true, []byte(`{"model":"gpt-5.4","input":"run","tools":[{"type":"function","name":"exec_command"}]}`)))
	})

	t.Run("client JSONL event parsers detect tool calls completions and errors", func(t *testing.T) {
		codexOK := parseCodexJSONLOutput(strings.Join([]string{
			`{"type":"thread.started","thread_id":"t1"}`,
			`{"type":"turn.started"}`,
			`{"type":"item.completed","item":{"id":"i1","type":"command_execution","command":"printf ok","exit_code":0,"status":"completed"}}`,
			`{"type":"item.completed","item":{"id":"i1b","type":"file_change","status":"completed"}}`,
			`{"type":"item.completed","item":{"id":"i2","type":"agent_message","text":"done token_abc"}}`,
			`{"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":4}}`,
		}, "\n"))
		require.True(t, codexOK.completed)
		require.Equal(t, 2, codexOK.toolExecutions)
		require.Equal(t, "done token_abc", codexOK.finalText)
		require.Empty(t, codexOK.errorMessages)

		codexErr := parseCodexJSONLOutput(`{"type":"turn.failed","error":{"message":"upstream 400"}}`)
		require.False(t, codexErr.completed)
		require.NotEmpty(t, codexErr.errorMessages)

		claudeOK := parseClaudeStreamJSONOutput(strings.Join([]string{
			`{"type":"system","subtype":"init","tools":["Bash"]}`,
			`{"type":"assistant","message":{"role":"assistant","content":[{"type":"tool_use","id":"u1","name":"Bash","input":{"command":"true"}}]}}`,
			`{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"u1","content":"ok","is_error":false}]}}`,
			`{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"done claude_xyz"}]}}`,
			`{"type":"result","subtype":"success","is_error":false,"result":"done claude_xyz"}`,
		}, "\n"))
		require.True(t, claudeOK.completed)
		require.Equal(t, 1, claudeOK.toolExecutions)
		require.Equal(t, "done claude_xyz", claudeOK.finalText)
		require.Empty(t, claudeOK.errorMessages)

		piSilentErr := parsePiJSONOutput(strings.Join([]string{
			`{"type":"session","version":3,"id":"s1"}`,
			`{"type":"message_end","message":{"role":"assistant","content":[],"stopReason":"error","errorMessage":"400 invalid schema"}}`,
			`{"type":"agent_settled","aborted":false}`,
		}, "\n"))
		require.True(t, piSilentErr.completed)
		require.NotEmpty(t, piSilentErr.errorMessages)
		require.Contains(t, piSilentErr.errorMessages[0], "400 invalid schema")
	})

	t.Run("gateway ingress recorder captures client requests without 5xx", func(t *testing.T) {
		incDir := filepath.Join(t.TempDir(), "_incoming")
		destDir := filepath.Join(t.TempDir(), "corpus")
		inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/models" {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"object":"list","data":[]}`)
				return
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
		})
		rec := &clientLabGatewayRecorder{
			next:       inner,
			captureDir: incDir,
			baseMeta: testkit.CorpusMeta{
				Protocol:   protocol.ProtocolOpenAIChatCompletions,
				Client:     "pi",
				Version:    pinnedPiVersion,
				Model:      "gpt-4o",
				CapturedAt: clientLabCaptureDate,
				Stream:     true,
				Scenario:   "lab_text_chat",
			},
		}
		srv := httptest.NewServer(rec)
		t.Cleanup(srv.Close)

		modelsResp, err := srv.Client().Get(srv.URL + "/v1/models")
		require.NoError(t, err)
		require.NoError(t, modelsResp.Body.Close())

		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hello from /Users/alice"}]}`))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer sk-proj-1234567890abcdefghijklmnop")
		req.Header.Set("User-Agent", "pi/1.6.0")
		chatResp, err := srv.Client().Do(req)
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, chatResp.Body)
		require.NoError(t, chatResp.Body.Close())

		gw5xx, gen200, capErrs := rec.summary()
		require.Zero(t, gw5xx)
		require.Equal(t, 1, gen200)
		require.Empty(t, capErrs)

		impRep, err := testkit.ImportCorpusDir(incDir, destDir)
		require.NoError(t, err)
		require.Len(t, impRep.ImportedFiles, 2)
		_, err = testkit.VerifyCorpusDir(destDir)
		require.NoError(t, err)
	})

	t.Run("embedded client corpus and client_lab route table satisfy invariants", func(t *testing.T) {
		corpus := testkit.DefaultCorpus()
		clientFixtures, err := corpus.QueryFixtures(testkit.FixtureQuery{
			Source: testkit.SourceCorpusClient,
		})
		require.NoError(t, err)
		require.NotEmpty(t, clientFixtures, "corpus/clients must contain recorded client requests")

		seenClients := make(map[string]bool)
		for _, f := range clientFixtures {
			require.NotNil(t, f.Meta, f.Path)
			seenClients[f.Meta.Client] = true
			require.Empty(t, testkit.DetectSecretViolations(f.Data), "secret violation in %s", f.Path)
			valOpts := testkit.RequestValidationOptions{
				StrictTopLevel: f.Protocol == protocol.ProtocolAnthropicMessages,
			}
			require.Empty(t, testkit.ValidateRequestRules(f.Protocol, f.Data, valOpts), "recorded client request %s violated strict rules", f.Path)
		}
		for _, wantClient := range []string{"codex", "claude-code", "pi"} {
			require.True(t, seenClients[wantClient], "corpus/clients must include %s recordings", wantClient)
		}

		routes := buildClientLabRoutes()
		require.Len(t, routes, 25)
		require.Equal(t, 50, len(routes)*len(clientLabScenarios))
		seenSlugs := make(map[string]bool, len(routes))
		nativeCount := 0
		conversionCount := 0
		for _, r := range routes {
			require.NotEmpty(t, r.slug)
			require.False(t, seenSlugs[r.slug], "duplicate client_lab route slug %s", r.slug)
			seenSlugs[r.slug] = true
			if r.isNative {
				nativeCount++
				require.Equal(t, r.profile.clientProtocol, r.provider.upstreamProtocol, r.slug)
			} else {
				conversionCount++
				require.NotEqual(t, r.profile.clientProtocol, r.provider.upstreamProtocol, r.slug)
			}
		}
		require.Equal(t, 7, nativeCount)
		require.Equal(t, 18, conversionCount)
	})

	t.Run("stage 7 live canary config parser and router assembly validate all five groups", func(t *testing.T) {
		emptyCfg := parseLiveCanaryConfigFromEnv(func(string) string { return "" })
		emptyGroups := buildStage7LiveCanaryGroups(emptyCfg)
		require.Len(t, emptyGroups, 5)
		for _, g := range emptyGroups {
			for _, p := range g.pairs {
				require.False(t, p.canRunReasoningTool())
				require.False(t, p.canRunSwitchProvider())
			}
		}

		syntheticEnv := map[string]string{
			"TOKENROUTER_CLIENTLAB_LIVE_OPENAI_BASE_URL":    "http://127.0.0.1:19001/v1",
			"TOKENROUTER_CLIENTLAB_LIVE_OPENAI_API_KEY":     "synthetic-openai-key",
			"TOKENROUTER_CLIENTLAB_LIVE_OPENAI_MODEL":       "gpt-5.4-canary",
			"TOKENROUTER_CLIENTLAB_LIVE_ANTHROPIC_BASE_URL": "http://127.0.0.1:19002",
			"TOKENROUTER_CLIENTLAB_LIVE_ANTHROPIC_API_KEY":  "synthetic-anthropic-key",
			"TOKENROUTER_CLIENTLAB_LIVE_ANTHROPIC_MODEL":    "claude-sonnet-4-5-canary",
			"TOKENROUTER_CLIENTLAB_LIVE_GEMINI_BASE_URL":    "http://127.0.0.1:19003",
			"TOKENROUTER_CLIENTLAB_LIVE_GEMINI_API_KEY":     "synthetic-gemini-key",
			"TOKENROUTER_CLIENTLAB_LIVE_GEMINI_MODEL":       "gemini-2.5-pro-canary",
			"TOKENROUTER_CLIENTLAB_LIVE_GROK_BASE_URL":      "http://127.0.0.1:19004/v1",
			"TOKENROUTER_CLIENTLAB_LIVE_GROK_API_KEY":       "synthetic-grok-key",
			"TOKENROUTER_CLIENTLAB_LIVE_GROK_MODEL":         "grok-4.5-canary",
			"TOKENROUTER_CLIENTLAB_LIVE_DEEPSEEK_BASE_URL":  "http://127.0.0.1:19005/v1",
			"TOKENROUTER_CLIENTLAB_LIVE_DEEPSEEK_API_KEY":   "synthetic-deepseek-key",
			"TOKENROUTER_CLIENTLAB_LIVE_DEEPSEEK_MODEL":     "deepseek-reasoner-canary",
		}
		fullCfg := parseLiveCanaryConfigFromEnv(func(k string) string { return syntheticEnv[k] })
		groups := buildStage7LiveCanaryGroups(fullCfg)
		require.Len(t, groups, 5)

		wantGroupSlugs := []string{
			"group1_codex_to_anthropic",
			"group2_claude_to_openai",
			"group3_claude_and_codex_to_gemini",
			"group4_claude_and_codex_to_grok",
			"group5_codex_to_deepseek_chat",
		}
		var gotGroupSlugs []string
		totalPairs := 0
		for _, grp := range groups {
			gotGroupSlugs = append(gotGroupSlugs, grp.groupSlug)
			require.NotEmpty(t, grp.pairs)
			for _, pair := range grp.pairs {
				totalPairs++
				require.True(t, pair.canRunReasoningTool(), pair.slug)
				require.True(t, pair.canRunSwitchProvider(), pair.slug)
				// Turn 2 must always land on the client's native protocol path.
				require.Equal(t, pair.profile.clientProtocol, pair.turn2Native.upstreamProtocol, pair.slug)

				// Verify router, dynamic Turn 2 pricing, and provider switching
				// against a local loopback fake upstream.
				turn2SSE := buildUpstreamTextSSE(pair.turn2Native.upstreamProtocol, pair.turn2Native.upstreamModel, "canary_guard", "guard_ok")
				turn2Mock := mockupstream.NewWithConfig(mockupstream.Config{
					Script: testkit.Script{
						Name:    "canary_turn2_guard",
						Default: testkit.Reply{Raw: turn2SSE},
					},
				})
				turn2Srv := httptest.NewServer(turn2Mock)
				turn1Lab := buildLiveCanaryProviderSpec(101, "http://127.0.0.1:19901", pair.turn1Provider, pair.turn1Endpoint.apiKey)
				turn2Lab := buildLiveCanaryProviderSpec(202, turn2Srv.URL, pair.turn2Native, pair.turn2Endpoint.apiKey)
				router := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{turn1Lab},
					Pricing:   liveCanaryPricingMap(turn1Lab, turn2Lab),
				})
				router.SetProviders(t, []labProvider{turn2Lab})
				gwSrv := httptest.NewServer(router)
				clientSpec := stage7ClientSpecForProtocol(pair.profile.clientProtocol)
				reqBody := stage7SampleClientStreamRequest(pair.profile.clientProtocol, pair.profile.clientModel, "guard ping")
				status, _, respBody := sendLabClientRequest(t, gwSrv, clientSpec, reqBody)
				gwSrv.Close()
				turn2Srv.Close()
				require.Equal(t, http.StatusOK, status, "%s Turn 2 pricing guard failed: %s", pair.slug, string(respBody))
				require.Len(t, router.UsageLogs(), 1, pair.slug)
			}
		}
		require.Equal(t, wantGroupSlugs, gotGroupSlugs)
		require.Equal(t, 7, totalPairs, "five Stage 7 canary groups must expand to all seven required client-to-upstream pairs")

		// Verify liveCanaryRetryTransport honors RetryInfo/Retry-After on 429 and 503.
		attempts := 0
		rateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw, _ := io.ReadAll(r.Body)
			_ = r.Body.Close()
			require.Equal(t, `{"ping":"ok"}`, string(raw))
			attempts++
			switch attempts {
			case 1:
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = io.WriteString(w, `{"error":{"code":429,"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"5ms"}]}}`)
			case 2:
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{"error":{"code":503,"status":"UNAVAILABLE"}}`)
			case 3:
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\"}\n\nevent: error\ndata: {\"error\":{\"message\":\"Concurrency limit exceeded for account, please retry later\"}}\n\n")
			default:
				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\"}\n\n")
			}
		}))
		defer rateSrv.Close()
		retryClient := &http.Client{
			Transport: &liveCanaryRetryTransport{
				base:        rateSrv.Client().Transport,
				defaultWait: 5 * time.Millisecond,
				maxWait:     20 * time.Millisecond,
				maxRetries:  4,
			},
		}
		retryReq, reqErr := http.NewRequestWithContext(t.Context(), http.MethodPost, rateSrv.URL, strings.NewReader(`{"ping":"ok"}`))
		require.NoError(t, reqErr)
		retryResp, doErr := retryClient.Do(retryReq)
		require.NoError(t, doErr)
		retryBody, readErr := io.ReadAll(retryResp.Body)
		require.NoError(t, readErr)
		require.NoError(t, retryResp.Body.Close())
		require.Equal(t, http.StatusOK, retryResp.StatusCode)
		require.Contains(t, string(retryBody), "response.output_item.added")
		require.Equal(t, 4, attempts)

		// Partial credentials enable reasoning_tool on the configured target while
		// withholding switch_provider when the native Turn 2 endpoint is absent.
		partialCfg := parseLiveCanaryConfigFromEnv(func(k string) string {
			if strings.Contains(k, "_ANTHROPIC_") {
				return syntheticEnv[k]
			}
			return ""
		})
		partialGroups := buildStage7LiveCanaryGroups(partialCfg)
		require.True(t, partialGroups[0].pairs[0].canRunReasoningTool())
		require.False(t, partialGroups[0].pairs[0].canRunSwitchProvider())

		// Verify group1_codex_to_anthropic (claude-haiku-4-5) gateway upstream
		// request bytes are byte-for-byte identical before and after cross-protocol
		// Mode="enabled" activation because claude-haiku-4-5 already resolves as a
		// budget-only model (Budget=true, len(Efforts)==0) in the embedded catalog.
		haikuCfg := parseLiveCanaryConfigFromEnv(func(k string) string {
			switch k {
			case "TOKENROUTER_CLIENTLAB_LIVE_ANTHROPIC_BASE_URL":
				return "http://127.0.0.1:19911"
			case "TOKENROUTER_CLIENTLAB_LIVE_ANTHROPIC_API_KEY":
				return "synthetic-anthropic-key"
			case "TOKENROUTER_CLIENTLAB_LIVE_ANTHROPIC_MODEL":
				return "claude-haiku-4-5"
			default:
				return ""
			}
		})
		group1Pair := buildStage7LiveCanaryGroups(haikuCfg)[0].pairs[0]
		require.Equal(t, "codex_to_anthropic", group1Pair.slug)
		require.Equal(t, "claude-haiku-4-5", group1Pair.turn1Provider.upstreamModel)

		anthCarrier, carrErr := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemeAnthropic,
			Data:   "EqQBCgIYAhIMc2lnX2hhaWt1XzAx",
			Text:   "Need to inspect workspace status first.",
		})
		require.NoError(t, carrErr)

		codexTurn1Body := []byte(`{"model":"gpt-5.4","stream":true,"reasoning":{"effort":"medium","summary":"auto"},"include":["reasoning.encrypted_content"],"tools":[{"type":"function","name":"exec_command","description":"Run shell command","parameters":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Check status."}]}]}`)
		codexTurn2Body := []byte(`{"model":"gpt-5.4","stream":true,"reasoning":{"effort":"medium","summary":"auto"},"include":["reasoning.encrypted_content"],"tools":[{"type":"function","name":"exec_command","description":"Run shell command","parameters":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}}],"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Check status."}]},{"id":"rs_haiku_1","type":"reasoning","summary":[{"type":"summary_text","text":"Need to inspect workspace status first."}],"encrypted_content":"` + anthCarrier + `"},{"id":"fc_haiku_1","type":"function_call","call_id":"call_haiku_1","name":"exec_command","arguments":"{\"cmd\":\"pwd\"}"},{"type":"function_call_output","call_id":"call_haiku_1","output":"/workspace"}]}`)

		haikuSSE := buildUpstreamTextSSE(protocol.ProtocolAnthropicMessages, "claude-haiku-4-5", "haiku_guard", "status_ok")
		haikuMock := mockupstream.NewWithConfig(mockupstream.Config{
			Validation: testkit.RequestValidationOptions{StrictTopLevel: true},
			Script: testkit.Script{
				Name:    "group1_haiku_byte_invariance",
				Default: testkit.Reply{Raw: haikuSSE},
			},
		})
		haikuSrv := httptest.NewServer(haikuMock)
		haikuLab := buildLiveCanaryProviderSpec(301, haikuSrv.URL, group1Pair.turn1Provider, group1Pair.turn1Endpoint.apiKey)
		haikuRouter := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{haikuLab},
			Pricing:   liveCanaryPricingMap(haikuLab),
		})
		haikuGwSrv := httptest.NewServer(haikuRouter)
		codexSpec := stage7ClientSpecForProtocol(protocol.ProtocolOpenAIResponses)
		st1, _, rb1 := sendLabClientRequest(t, haikuGwSrv, codexSpec, codexTurn1Body)
		st2, _, rb2 := sendLabClientRequest(t, haikuGwSrv, codexSpec, codexTurn2Body)
		haikuGwSrv.Close()
		haikuSrv.Close()
		require.Equal(t, http.StatusOK, st1, string(rb1))
		require.Equal(t, http.StatusOK, st2, string(rb2))
		require.Empty(t, haikuMock.Violations())
		recordedReqs := haikuMock.Requests()
		require.Len(t, recordedReqs, 2)

		wantTurn1GatewayBody := `{"model":"claude-haiku-4-5","max_tokens":8192,"messages":[{"role":"user","content":[{"type":"text","text":"Check status."}]}],"tools":[{"name":"exec_command","description":"Run shell command","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}}],"thinking":{"type":"enabled","budget_tokens":4096},"stream":true}`
		wantTurn2GatewayBody := `{"model":"claude-haiku-4-5","max_tokens":8192,"messages":[{"role":"user","content":[{"type":"text","text":"Check status."}]},{"role":"assistant","content":[{"type":"thinking","thinking":"Need to inspect workspace status first.","signature":"EqQBCgIYAhIMc2lnX2hhaWt1XzAx"},{"type":"tool_use","id":"call_haiku_1","name":"exec_command","input":{"cmd":"pwd"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call_haiku_1","content":"/workspace"}]}],"tools":[{"name":"exec_command","description":"Run shell command","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}}],"thinking":{"type":"enabled","budget_tokens":4096},"stream":true}`
		require.Equal(t, wantTurn1GatewayBody, string(recordedReqs[0].Body))
		require.Equal(t, wantTurn2GatewayBody, string(recordedReqs[1].Body))
	})

	t.Run("scripted T6 route inventory covers every non-Antigravity non-Qoder route_table row", func(t *testing.T) {
		rawRoutes, err := os.ReadFile(filepath.Join("testdata", "golden", "routes.txt"))
		require.NoError(t, err)
		routeRows, _, err := parseConversionRoutesGolden(string(rawRoutes))
		require.NoError(t, err)

		coveredKeys := collectAllScriptedClientLabRouteKeys()
		checked := 0
		for _, row := range routeRows {
			if row.Platform == capability.PlatformAntigravity || row.Platform == capability.PlatformQoder {
				continue
			}
			checked++
			key := formatStage7RouteKey(row.ClientProtocol, row.Platform, row.ProviderType, row.UpstreamProtocol)
			require.True(t, coveredKeys[key], "missing T6 scripted clientlab route for routes.txt row %s", key)
		}
		require.Equal(t, 58, checked)
	})
}

// TestClientLabScenarios executes real Codex CLI, Claude Code, and pi
// subprocesses across lab_text and lab_tool when TOKENROUTER_CLIENTLAB=1 is set.
// Both native passthrough controls and cross-protocol IR conversion routes must
// pass 100% with zero gateway 5xx responses and exact usage settlement parity.
func TestClientLabScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	require.NoError(t, verifyClientLabLiveContainerGate(mode, os.Getenv(clientLabContainerEnv)))
	if mode == clientLabModeLive {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	incomingDir := resolveClientLabIncomingDir(mode)
	if mode == clientLabModeRecord {
		require.NoError(t, os.RemoveAll(incomingDir))
		require.NoError(t, os.MkdirAll(incomingDir, 0o755))
	}

	routes := buildClientLabRoutes()
	var nativeRoutes []clientLabRoute
	var conversionRoutes []clientLabRoute
	for _, r := range routes {
		if r.isNative {
			nativeRoutes = append(nativeRoutes, r)
		} else {
			conversionRoutes = append(conversionRoutes, r)
		}
	}

	// Phase 1: Native passthrough controls must succeed across every client and scenario.
	t.Run("native_controls", func(t *testing.T) {
		for _, route := range nativeRoutes {
			for _, scenario := range clientLabScenarios {
				cellName := route.slug + "/" + scenario
				t.Run(cellName, func(t *testing.T) {
					captureDir := ""
					if mode == clientLabModeRecord {
						captureDir = incomingDir
					}
					entry, diag := executeClientLabScriptedCell(t, labRoot, route, scenario, mode, captureDir)
					require.Equal(t, labCellPass, entry.Status,
						"native control %s failed (rules=%v): %s", cellName, entry.Rules, diag)
				})
			}
		}
	})

	// Phase 2: Cross-protocol conversion routes run on the IR engine and must pass 100%.
	t.Run("conversion_routes", func(t *testing.T) {
		for _, route := range conversionRoutes {
			for _, scenario := range clientLabScenarios {
				cellName := route.slug + "/" + scenario
				t.Run(cellName, func(t *testing.T) {
					entry, diag := executeClientLabScriptedCell(t, labRoot, route, scenario, mode, "")
					require.Equal(t, labCellPass, entry.Status,
						"conversion route %s failed (rules=%v): %s", cellName, entry.Rules, diag)
				})
			}
		}
	})
}

// TestClientLabIRResponsesUpstreamScenarios runs real Claude Code (--bare and
// default) and pi (Chat and Messages) CLI subprocesses against the OpenAI
// Responses upstream family when TOKENROUTER_CLIENTLAB=1 is set. Every
// Responses-upstream conversion cell must pass with zero gateway 5xx responses
// and exact usage settlement parity.
func TestClientLabIRResponsesUpstreamScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeScripted {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	for _, route := range buildClientLabRoutes() {
		if route.isNative || route.provider.kind != "openai" {
			continue
		}
		for _, scenario := range clientLabScenarios {
			cellName := route.slug + "/" + scenario
			t.Run(cellName, func(t *testing.T) {
				entry, diag := executeClientLabScriptedCell(t, labRoot, route, scenario, mode, "")
				require.Equal(t, labCellPass, entry.Status,
					"IR T6 cell %s failed (rules=%v): %s", cellName, entry.Rules, diag)
			})
		}
	}
}

// TestClientLabIRChatUpstreamScenarios runs real Codex CLI, Claude Code
// (--bare and default), and pi (Responses and Messages) CLI subprocesses
// against the OpenAI Chat Completions upstream family when
// TOKENROUTER_CLIENTLAB=1 is set. Every Chat-upstream conversion cell must pass
// with zero gateway 5xx responses and exact usage settlement parity.
func TestClientLabIRChatUpstreamScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeScripted {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	for _, route := range buildClientLabRoutes() {
		if route.isNative || route.provider.kind != "openai_chat" {
			continue
		}
		for _, scenario := range clientLabScenarios {
			cellName := route.slug + "/" + scenario
			t.Run(cellName, func(t *testing.T) {
				entry, diag := executeClientLabScriptedCell(t, labRoot, route, scenario, mode, "")
				require.Equal(t, labCellPass, entry.Status,
					"IR Chat T6 cell %s failed (rules=%v): %s", cellName, entry.Rules, diag)
			})
		}
	}
}

// TestClientLabIRMessagesUpstreamScenarios runs real Codex CLI and pi (Chat and
// Responses) CLI subprocesses against the Anthropic Messages upstream family
// (codex_via_anthropic, pi_chat_via_anthropic, pi_responses_via_anthropic) when
// TOKENROUTER_CLIENTLAB=1 is set. Every cell, including codex_via_anthropic, must pass.
func TestClientLabIRMessagesUpstreamScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeScripted {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	visitedCodex := 0
	for _, route := range buildClientLabRoutes() {
		if route.isNative || route.provider.kind != "anthropic" {
			continue
		}
		if route.slug == "codex_via_anthropic" {
			visitedCodex++
		}
		for _, scenario := range clientLabScenarios {
			cellName := route.slug + "/" + scenario
			t.Run(cellName, func(t *testing.T) {
				entry, diag := executeClientLabScriptedCell(t, labRoot, route, scenario, mode, "")
				require.Equal(t, labCellPass, entry.Status,
					"IR Messages T6 cell %s failed (rules=%v): %s", cellName, entry.Rules, diag)
			})
		}
	}
	require.Equal(t, 1, visitedCodex, "codex_via_anthropic must be exercised")
}

// TestClientLabIRGrokResponsesUpstreamScenarios runs real pi (Chat and Messages)
// and Claude Code (--bare and default) CLI subprocesses against OAuth and API
// Key Grok Responses upstreams when TOKENROUTER_CLIENTLAB=1 is set. Every cell
// must pass with zero gateway 5xx responses, strict function-only request
// compliance, and exact usage parity.
func TestClientLabIRGrokResponsesUpstreamScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeScripted {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	grokSpecs := grokLabProviderSpecs()
	var routes []clientLabRoute
	for _, prof := range clientLabProfiles {
		if prof.clientProtocol != protocol.ProtocolOpenAIChatCompletions &&
			prof.clientProtocol != protocol.ProtocolAnthropicMessages {
			continue
		}
		for _, spec := range grokSpecs {
			routes = append(routes, clientLabRoute{
				slug:     prof.slug + "_via_" + spec.kind,
				profile:  prof,
				provider: spec,
			})
		}
	}
	require.Len(t, routes, 8)

	for _, route := range routes {
		for _, scenario := range clientLabScenarios {
			cellName := route.slug + "/" + scenario
			t.Run(cellName, func(t *testing.T) {
				entry, diag := executeClientLabScriptedCell(t, labRoot, route, scenario, mode, "")
				require.Equal(t, labCellPass, entry.Status,
					"IR Grok Responses T6 cell %s failed (rules=%v): %s", cellName, entry.Rules, diag)
			})
		}
	}
}

// TestClientLabIRStage2ExtendedScenarios exercises all seven Section 5.7
// extended T6 scenarios (lab_tool_parallel, lab_reasoning_tool,
// lab_switch_provider, lab_failover, lab_truncated, lab_custom_tool, and
// lab_long_stream) with real Codex CLI, Claude Code, and pi subprocesses across
// Stage 2's three upstream protocol families plus native passthrough controls
// when TOKENROUTER_CLIENTLAB=1 is enabled in scripted mode. Every non-truncated
// scenario must complete with zero gateway 5xx responses, zero upstream
// validation violations, and exact per-turn UsageLog settlement parity.
func TestClientLabIRStage2ExtendedScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeScripted {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	deepseekChatSpec := labProviderKindSpec{
		kind:             "deepseek_chat",
		platform:         capability.PlatformDeepseek,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "deepseek-reasoner",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"deepseek-thinking"},
		},
		failoverStatus: 529,
	}
	kimiPassbackSpec := labProviderKindSpec{
		kind:             "kimi_passback",
		platform:         capability.PlatformKimi,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "k3",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation: testkit.RequestValidationOptions{
			StrictTopLevel: true,
			DialectRules:   []string{"passback"},
		},
		failoverStatus: 529,
	}
	grokOAuthSpec := grokLabProviderSpecs()[0]

	stage2Routes := stage2ClientLabExtendedRoutes()

	t.Run(clientLabScenarioToolParallel, func(t *testing.T) {
		for _, route := range stage2Routes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedParallelToolCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioReasoningTool, func(t *testing.T) {
		reasoningRoutes := append(slices.Clone(stage2Routes),
			makeCustomClientLabRoute("codex", deepseekChatSpec),
			makeCustomClientLabRoute("codex", findLabProviderKind("deepseek_anthropic")),
			makeCustomClientLabRoute("claude_default", grokOAuthSpec),
			makeCustomClientLabRoute("claude_bare", deepseekChatSpec),
			makeCustomClientLabRoute("pi_responses", deepseekChatSpec),
			makeCustomClientLabRoute("pi_chat", grokOAuthSpec),
			makeCustomClientLabRoute("pi_messages", deepseekChatSpec),
		)
		for _, route := range reasoningRoutes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedReasoningToolCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioSwitchProvider, func(t *testing.T) {
		for _, route := range stage2Routes {
			turn2Spec := findLabProviderKind(route.profile.nativeProviderKind)
			if route.isNative {
				turn2Spec = stage2AlternateSwitchProvider(route.provider.upstreamProtocol)
			}
			caseName := route.slug + "_to_" + turn2Spec.kind
			t.Run(caseName, func(t *testing.T) {
				runExtendedSwitchProviderCell(t, labRoot, route.profile, route.provider, turn2Spec)
			})
		}

		extraDialectSwitches := []struct {
			name        string              // subtest scenario name
			profileSlug string              // CLI profile slug
			turn1Spec   labProviderKindSpec // Turn 1 upstream provider specification
			turn2Spec   labProviderKindSpec // Turn 2 upstream provider specification
		}{
			{
				name:        "codex_deepseek_chat_to_native_openai",
				profileSlug: "codex",
				turn1Spec:   deepseekChatSpec,
				turn2Spec:   findLabProviderKind("openai"),
			},
			{
				name:        "codex_anthropic_to_deepseek_chat",
				profileSlug: "codex",
				turn1Spec:   findLabProviderKind("anthropic"),
				turn2Spec:   deepseekChatSpec,
			},
			{
				name:        "claude_bare_openai_chat_to_grok_oauth",
				profileSlug: "claude_bare",
				turn1Spec:   findLabProviderKind("openai_chat"),
				turn2Spec:   grokOAuthSpec,
			},
			{
				name:        "pi_messages_openai_chat_to_native_kimi_passback",
				profileSlug: "pi_messages",
				turn1Spec:   findLabProviderKind("openai_chat"),
				turn2Spec:   kimiPassbackSpec,
			},
			{
				name:        "pi_chat_openai_to_anthropic",
				profileSlug: "pi_chat",
				turn1Spec:   findLabProviderKind("openai"),
				turn2Spec:   findLabProviderKind("anthropic"),
			},
			{
				name:        "pi_responses_openai_chat_to_anthropic",
				profileSlug: "pi_responses",
				turn1Spec:   findLabProviderKind("openai_chat"),
				turn2Spec:   findLabProviderKind("anthropic"),
			},
		}
		for _, sc := range extraDialectSwitches {
			t.Run(sc.name, func(t *testing.T) {
				prof := findClientLabProfile(sc.profileSlug)
				runExtendedSwitchProviderCell(t, labRoot, prof, sc.turn1Spec, sc.turn2Spec)
			})
		}
	})

	t.Run(clientLabScenarioFailover, func(t *testing.T) {
		failoverRoutes := append(slices.Clone(stage2Routes),
			makeCustomClientLabRoute("claude_default", grokOAuthSpec),
			makeCustomClientLabRoute("pi_chat", grokOAuthSpec),
		)
		for _, route := range failoverRoutes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedFailoverCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioTruncated, func(t *testing.T) {
		for _, route := range stage2Routes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedTruncatedCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioCustomTool, func(t *testing.T) {
		for _, provSpec := range []labProviderKindSpec{
			findLabProviderKind("openai"),
			findLabProviderKind("openai_chat"),
			findLabProviderKind("anthropic"),
			findLabProviderKind("deepseek_anthropic"),
		} {
			route := makeCustomClientLabRoute("codex", provSpec)
			t.Run(route.slug, func(t *testing.T) {
				runExtendedCustomToolCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioLongStream, func(t *testing.T) {
		for _, route := range stage2Routes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedLongStreamCell(t, labRoot, route)
			})
		}
	})
}

// TestClientLabIRGeminiUpstreamScenarios runs real Codex CLI, Claude Code
// (--bare and default), and pi (Chat, Responses, Messages, and native Gemini)
// CLI subprocesses against Google Gemini, Vertex Gemini, and OpenCode Gemini
// GenerateContent upstreams when
// TOKENROUTER_CLIENTLAB=1 is set. Every base lab_text and lab_tool cell must
// pass with zero gateway 5xx responses, zero upstream validation violations,
// and exact usage settlement parity.
func TestClientLabIRGeminiUpstreamScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeScripted {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	geminiRoutes := stage3ClientLabGeminiRoutes()
	require.Len(t, geminiRoutes, 7)

	// Pair non-reasoning CLI runs that inject disabled-thinking controls (Codex
	// and Claude Code) with toggle-capable Vertex gemini-2.5-flash, and pair pi
	// profiles (which omit disabled-thinking fields) with OpenCode gemini-3-flash.
	openCodeSpec := openCodeGeminiLabSpec()
	vertexFlashSpec := vertexGeminiFlashLabSpec()
	routes := append(slices.Clone(geminiRoutes),
		makeCustomClientLabRoute("codex", vertexFlashSpec),
		makeCustomClientLabRoute("claude_bare", vertexFlashSpec),
		makeCustomClientLabRoute("pi_chat", openCodeSpec),
		makeCustomClientLabRoute("pi_responses", openCodeSpec),
		makeCustomClientLabRoute("pi_messages", openCodeSpec),
	)

	for _, route := range routes {
		for _, scenario := range clientLabScenarios {
			cellName := route.slug + "/" + scenario
			t.Run(cellName, func(t *testing.T) {
				entry, diag := executeClientLabScriptedCell(t, labRoot, route, scenario, mode, "")
				require.Equal(t, labCellPass, entry.Status,
					"IR Gemini T6 cell %s failed (rules=%v): %s", cellName, entry.Rules, diag)
			})
		}
	}
}

// TestClientLabIRStage3GeminiExtendedScenarios exercises all seven Section 5.7
// extended T6 scenarios (lab_tool_parallel, lab_reasoning_tool with
// gemini-signature and CallMemory verification, bidirectional
// lab_switch_provider with Stage 2 providers, lab_failover, lab_truncated,
// lab_custom_tool Codex apply_patch lowering/raising, and lab_long_stream)
// across all seven Gemini upstream CLI profiles plus Vertex and OpenCode Gemini
// when TOKENROUTER_CLIENTLAB=1 is enabled in scripted mode. Every applicable
// Gemini cell runs without skipping claude_bare, claude_default, or pi_responses.
func TestClientLabIRStage3GeminiExtendedScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeScripted {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	deepseekChatSpec := labProviderKindSpec{
		kind:             "deepseek_chat",
		platform:         capability.PlatformDeepseek,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "deepseek-reasoner",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"deepseek-thinking"},
		},
		failoverStatus: 529,
	}
	kimiPassbackSpec := labProviderKindSpec{
		kind:             "kimi_passback",
		platform:         capability.PlatformKimi,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "k3",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation: testkit.RequestValidationOptions{
			StrictTopLevel: true,
			DialectRules:   []string{"passback"},
		},
		failoverStatus: 529,
	}
	grokOAuthSpec := grokLabProviderSpecs()[0]
	geminiProSpec := findLabProviderKind("gemini")
	vertexProSpec := vertexGeminiLabSpec()
	vertexFlashSpec := vertexGeminiFlashLabSpec()
	openCodeSpec := openCodeGeminiLabSpec()

	stage3Routes := stage3ClientLabGeminiRoutes()
	require.Len(t, stage3Routes, 7)

	t.Run(clientLabScenarioToolParallel, func(t *testing.T) {
		for _, route := range stage3Routes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedParallelToolCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioReasoningTool, func(t *testing.T) {
		reasoningRoutes := append(slices.Clone(stage3Routes),
			makeCustomClientLabRoute("codex", vertexProSpec),
			makeCustomClientLabRoute("claude_default", vertexProSpec),
			makeCustomClientLabRoute("codex", openCodeSpec),
			makeCustomClientLabRoute("claude_bare", openCodeSpec),
			makeCustomClientLabRoute("pi_chat", openCodeSpec),
		)
		for _, route := range reasoningRoutes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedReasoningToolCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioSwitchProvider, func(t *testing.T) {
		for _, route := range stage3Routes {
			if route.isNative {
				t.Run("pi_gemini_via_gemini_to_gemini_vertex", func(t *testing.T) {
					runExtendedSwitchProviderCell(t, labRoot, route.profile, route.provider, vertexProSpec)
				})
				continue
			}
			nativeSpec := findLabProviderKind(route.profile.nativeProviderKind)
			t.Run(route.slug+"_to_"+nativeSpec.kind, func(t *testing.T) {
				runExtendedSwitchProviderCell(t, labRoot, route.profile, route.provider, nativeSpec)
			})
			t.Run(route.profile.slug+"_via_"+nativeSpec.kind+"_to_"+route.provider.kind, func(t *testing.T) {
				runExtendedSwitchProviderCell(t, labRoot, route.profile, nativeSpec, route.provider)
			})
		}

		extraGeminiSwitches := []struct {
			name        string              // subtest scenario name
			profileSlug string              // CLI profile slug
			turn1Spec   labProviderKindSpec // Turn 1 upstream provider specification
			turn2Spec   labProviderKindSpec // Turn 2 upstream provider specification
		}{
			{
				name:        "codex_gemini_pro_to_deepseek_chat",
				profileSlug: "codex",
				turn1Spec:   geminiProSpec,
				turn2Spec:   deepseekChatSpec,
			},
			{
				name:        "claude_bare_grok_oauth_to_gemini_pro",
				profileSlug: "claude_bare",
				turn1Spec:   grokOAuthSpec,
				turn2Spec:   geminiProSpec,
			},
			{
				name:        "claude_default_gemini_vertex_pro_to_anthropic",
				profileSlug: "claude_default",
				turn1Spec:   vertexProSpec,
				turn2Spec:   findLabProviderKind("anthropic"),
			},
			{
				name:        "codex_opencode_gemini_to_openai",
				profileSlug: "codex",
				turn1Spec:   openCodeSpec,
				turn2Spec:   findLabProviderKind("openai"),
			},
			{
				name:        "pi_messages_gemini_pro_to_kimi_passback",
				profileSlug: "pi_messages",
				turn1Spec:   geminiProSpec,
				turn2Spec:   kimiPassbackSpec,
			},
			{
				name:        "pi_responses_opencode_gemini_to_anthropic",
				profileSlug: "pi_responses",
				turn1Spec:   openCodeSpec,
				turn2Spec:   findLabProviderKind("anthropic"),
			},
		}
		for _, sc := range extraGeminiSwitches {
			t.Run(sc.name, func(t *testing.T) {
				prof := findClientLabProfile(sc.profileSlug)
				runExtendedSwitchProviderCell(t, labRoot, prof, sc.turn1Spec, sc.turn2Spec)
			})
		}
	})

	t.Run(clientLabScenarioFailover, func(t *testing.T) {
		failoverRoutes := append(slices.Clone(stage3Routes),
			makeCustomClientLabRoute("pi_responses", openCodeSpec),
			makeCustomClientLabRoute("claude_default", vertexFlashSpec),
		)
		for _, route := range failoverRoutes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedFailoverCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioTruncated, func(t *testing.T) {
		for _, route := range stage3Routes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedTruncatedCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioCustomTool, func(t *testing.T) {
		for _, provSpec := range []labProviderKindSpec{
			stage3ClientLabGeminiRoutes()[0].provider,
			vertexFlashSpec,
		} {
			route := makeCustomClientLabRoute("codex", provSpec)
			t.Run(route.slug, func(t *testing.T) {
				runExtendedCustomToolCell(t, labRoot, route)
			})
		}
	})

	t.Run(clientLabScenarioLongStream, func(t *testing.T) {
		for _, route := range stage3Routes {
			t.Run(route.slug, func(t *testing.T) {
				runExtendedLongStreamCell(t, labRoot, route)
			})
		}
	})
}

// TestClientLabIRStage5ResponsesCustomToolScenarios exercises Section 6.6 Step 4's
// Codex CLI lab_custom_tool apply_patch workflow across same-protocol function-only
// OpenAI Responses upstreams (OpenAI API Key passthrough, Grok OAuth and API Key,
// and DeepSeek API Key Responses) as well as the Responses WebSocket HTTP bridge
// when TOKENROUTER_CLIENTLAB=1 is enabled. Scripted runs verify client-tool
// adaptation with full function-only validation, while record mode captures raw
// two-turn Codex apply_patch requests into _incoming/.
func TestClientLabIRStage5ResponsesCustomToolScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode == clientLabModeLive {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	if mode == clientLabModeRecord {
		incomingDir := resolveClientLabIncomingDir(mode)
		require.NoError(t, os.MkdirAll(incomingDir, 0o755))
		nativeRoute := makeCustomClientLabRoute("codex", findLabProviderKind("openai"))
		runStage5CodexCustomToolCell(t, labRoot, nativeRoute, mode, incomingDir, false)
		return
	}

	httpProviders := stage5FunctionOnlyResponsesProviders()
	for _, provSpec := range httpProviders {
		route := makeCustomClientLabRoute("codex", provSpec)
		t.Run(route.slug, func(t *testing.T) {
			runStage5CodexCustomToolCell(t, labRoot, route, mode, "", false)
		})
	}

	wsBridgeProviders := []labProviderKindSpec{
		stage5OpenAIAPIKeyPassthroughLabSpec(),
		grokLabProviderSpecs()[0],
		grokLabProviderSpecs()[1],
	}
	for _, provSpec := range wsBridgeProviders {
		route := clientLabRoute{
			slug:     "codex_ws_via_" + provSpec.kind,
			profile:  findClientLabProfile("codex"),
			provider: provSpec,
		}
		t.Run(route.slug, func(t *testing.T) {
			runStage5CodexCustomToolCell(t, labRoot, route, mode, "", true)
		})
	}
}

// TestClientLabIRStage7RouteMatrixScenarios runs real Codex CLI, Claude Code,
// and pi subprocesses across every remaining non-Antigravity, non-Qoder
// [route_table] entry in testdata/golden/routes.txt (including Anthropic OAuth,
// Setup Token, AWS Bedrock EventStream, and Vertex Claude, OpenAI OAuth, Gemini
// OAuth and Vertex Chat, Grok Chat, Kimi, Zhipu, DeepSeek, and OpenCode) when
// TOKENROUTER_CLIENTLAB=1 is set in scripted mode.
// Every route executes a full two-turn lab_tool exchange and must pass with
// zero gateway 5xx responses, zero strict request violations, and exact usage parity.
func TestClientLabIRStage7RouteMatrixScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeScripted {
		return
	}

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	routes := stage7RemainingClientLabRoutes()
	require.Len(t, routes, 37)
	for _, route := range routes {
		cellName := route.slug + "/" + clientLabScenarioTool
		t.Run(cellName, func(t *testing.T) {
			entry, diag := executeClientLabScriptedCell(t, labRoot, route, clientLabScenarioTool, mode, "")
			require.Equal(t, labCellPass, entry.Status,
				"IR Stage 7 route matrix cell %s failed (rules=%v): %s", cellName, entry.Rules, diag)
		})
	}
}

// TestClientLabIRLiveCanaryScenarios executes Section 6.8 Step 4's five live
// canary groups when TOKENROUTER_CLIENTLAB=1 and
// TOKENROUTER_CLIENTLAB_MODE=live are set inside the disposable container.
// Non-live modes return immediately without reading live provider environment
// variables or initiating outbound network connections.
func TestClientLabIRLiveCanaryScenarios(t *testing.T) {
	if !clientLabEnabled() {
		return
	}

	mode, err := parseClientLabMode(os.Getenv(clientLabModeEnv))
	require.NoError(t, err)
	if mode != clientLabModeLive {
		return
	}
	require.NoError(t, verifyClientLabLiveContainerGate(mode, os.Getenv(clientLabContainerEnv)))

	labRoot := resolveClientLabDir(t)
	verifyInstalledClientBinaries(t, labRoot)

	runStage7LiveCanaries(t, labRoot)
}

// clientLabEnabled reports whether TOKENROUTER_CLIENTLAB=1 is set in the
// process environment to authorize real CLI subprocess execution.
func clientLabEnabled() bool {
	return os.Getenv(clientLabEnableEnv) == "1"
}

// parseClientLabMode normalizes raw by trimming whitespace and lowercasing into
// a supported clientLabMode constant. Empty inputs default to
// clientLabModeScripted, while unrecognized mode names return a descriptive error.
func parseClientLabMode(raw string) (clientLabMode, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", string(clientLabModeScripted):
		return clientLabModeScripted, nil
	case string(clientLabModeRecord):
		return clientLabModeRecord, nil
	case string(clientLabModeLive):
		return clientLabModeLive, nil
	default:
		return "", fmt.Errorf("unsupported %s value %q (want scripted, record, or live)", clientLabModeEnv, raw)
	}
}

// verifyClientLabLiveContainerGate checks Section 5.7's container boundary before
// live upstream providers can choose shell commands executed by CLI agents.
// Scripted and record modes return nil immediately, whereas live mode requires
// TOKENROUTER_CLIENTLAB_CONTAINER=1 from tools/clientlab/Dockerfile.
func verifyClientLabLiveContainerGate(mode clientLabMode, containerEnv string) error {
	if mode != clientLabModeLive {
		return nil
	}
	if strings.TrimSpace(containerEnv) != "1" {
		return fmt.Errorf("refusing clientlab live mode outside disposable container: %s=1 is required", clientLabContainerEnv)
	}
	return nil
}

// resolveClientLabDir resolves the repository's tools/clientlab directory from
// this test source file's runtime caller path. Missing paths or non-directory
// entries fail tb immediately before template or binary lookups proceed.
func resolveClientLabDir(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	dir := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", "..", "tools", "clientlab"))
	info, err := os.Stat(dir)
	require.NoError(tb, err, "tools/clientlab directory must exist")
	require.True(tb, info.IsDir())
	return dir
}

// resolveClientLabIncomingDir resolves the destination _incoming directory used
// when TOKENROUTER_CLIENTLAB_MODE=record captures raw client requests. Non-record
// modes return an empty string so ingress capture stays disabled, whereas record
// mode prefers TOKENROUTER_CLIENTLAB_INCOMING_DIR before testdata/corpus/_incoming.
func resolveClientLabIncomingDir(mode clientLabMode) string {
	if mode != clientLabModeRecord {
		return ""
	}
	if custom := strings.TrimSpace(os.Getenv(clientLabIncomingEnv)); custom != "" {
		return custom
	}
	return filepath.Join(clientLabTestdataDir, "corpus", "_incoming")
}

// verifyInstalledClientBinaries checks that npm ci has populated codex, claude,
// and pi executables under tools/clientlab/node_modules/.bin before running T6.
// Missing executables or directory collisions fail tb immediately with the
// required installation command.
func verifyInstalledClientBinaries(tb testing.TB, labRoot string) {
	tb.Helper()
	binDir := filepath.Join(labRoot, "node_modules", ".bin")
	for _, binName := range []string{"codex", "claude", "pi"} {
		binPath := clientBinaryPath(binDir, binName)
		info, err := os.Stat(binPath)
		if err != nil || info.IsDir() {
			tb.Fatalf("missing clientlab binary %s; run `cd tools/clientlab && npm ci` first", binPath)
		}
	}
}

// clientBinaryPath joins binDir and name into the platform-specific npm shim
// executable path. Windows environments append a .cmd suffix, while POSIX
// platforms return the bare executable name inside binDir.
func clientBinaryPath(binDir, name string) string {
	if runtime.GOOS == "windows" {
		return filepath.Join(binDir, name+".cmd")
	}
	return filepath.Join(binDir, name)
}

// buildClientLabRoutes expands clientLabProfiles across their configured
// target provider kinds in deterministic order. Gemini routes target
// toggle-capable gemini-2.5-flash so non-reasoning CLI templates encoding
// effort="none" or MAX_THINKING_TOKENS=0 map cleanly to thinkingBudget=0.
func buildClientLabRoutes() []clientLabRoute {
	var out []clientLabRoute
	for _, prof := range clientLabProfiles {
		for _, provKind := range prof.providerKinds {
			provSpec := findLabProviderKind(provKind)
			if provSpec.upstreamProtocol == protocol.ProtocolGeminiGenerateContent {
				provSpec.upstreamModel = "gemini-2.5-flash"
			}
			out = append(out, clientLabRoute{
				slug:     prof.slug + "_via_" + provSpec.kind,
				profile:  prof,
				provider: provSpec,
				isNative: provSpec.kind == prof.nativeProviderKind,
			})
		}
	}
	return out
}

// executeClientLabScriptedCell runs one (route, scenario) cell against an
// in-process gateway and scripted mockupstream server, returning its cell
// result and diagnostic summary string. Non-empty captureDir enables raw client
// request capture at the gateway ingress, and protocol or subprocess failures
// populate entry.Rules with labCellFail instead of aborting t directly.
func executeClientLabScriptedCell(
	t *testing.T,
	labRoot string,
	route clientLabRoute,
	scenario string,
	mode clientLabMode,
	captureDir string,
) (labCellResult, string) {
	t.Helper()
	entry := labCellResult{
		Client:   route.profile.clientProtocol,
		Upstream: route.provider.upstreamProtocol,
		Scenario: scenario,
		Law:      lawT6Client,
	}

	token := generateClientLabMarker(t, mode, route.slug, scenario)
	script, prompt, expectedFinalToken := buildClientLabScenarioScript(route, scenario, token)

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     script,
		Validation: route.provider.validation,
	})
	var cloudViolationsMu sync.Mutex
	var cloudViolations []string
	upSrv := newClientLabScriptedUpstreamServer(route.provider, mockH, func(ruleID string) {
		cloudViolationsMu.Lock()
		cloudViolations = append(cloudViolations, ruleID)
		cloudViolationsMu.Unlock()
	})
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers:     []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, route.provider)},
		StallTimeout:  15 * time.Second,
		HeaderTimeout: 15 * time.Second,
	})

	corpusScenario := scenario + route.profile.corpusScenarioSuffix
	gwRec := &clientLabGatewayRecorder{
		next:       lab,
		captureDir: captureDir,
		baseMeta: testkit.CorpusMeta{
			Protocol:   route.profile.clientProtocol,
			Client:     route.profile.corpusClient,
			Version:    route.profile.version,
			Model:      route.profile.clientModel,
			CapturedAt: clientLabCaptureDate,
			Stream:     true,
			Scenario:   corpusScenario,
		},
	}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	inv := prepareClientLabInvocation(t, labRoot, route.profile, gwSrv.URL, prompt, false)
	procRes := runClientLabSubprocess(t.Context(), inv, clientLabScriptedTimeout)

	var rules []string
	cloudViolationsMu.Lock()
	for _, cid := range cloudViolations {
		rules = appendUniqueLabRule(rules, cid)
	}
	cloudViolationsMu.Unlock()
	for _, v := range mockH.Violations() {
		for _, rv := range v.RuleViolations {
			rules = appendUniqueLabRule(rules, rv.RuleID)
		}
		for range v.CheckErrors {
			rules = appendUniqueLabRule(rules, "mockupstream_check_error")
		}
	}

	gw5xx, gen200, capErrs := gwRec.summary()
	require.Empty(t, capErrs, "record-mode ingress capture failed")
	if gw5xx > 0 {
		rules = appendUniqueLabRule(rules, ruleT6Gateway5xx)
	}
	if procRes.exitCode != 0 || procRes.runErr != nil {
		rules = appendUniqueLabRule(rules, ruleT6ClientExitNonZero)
	}
	if len(procRes.errorMessages) > 0 || !procRes.completed {
		rules = appendUniqueLabRule(rules, ruleT6ClientErrorEvent)
	}
	if !strings.Contains(procRes.finalText, expectedFinalToken) {
		rules = appendUniqueLabRule(rules, ruleT6MissingExpectedText)
	}

	if scenario == clientLabScenarioTool {
		resultBytes, readErr := os.ReadFile(filepath.Join(inv.workDir, "result.txt"))
		if readErr != nil || strings.TrimSpace(string(resultBytes)) != token {
			rules = appendUniqueLabRule(rules, ruleT6ToolFileMismatch)
		}
		if procRes.toolExecutions != 1 {
			rules = appendUniqueLabRule(rules, ruleT6ToolExecCountMismatch)
		}
		if !hasTurn2ToolResultRequest(mockH.Requests()) {
			rules = appendUniqueLabRule(rules, ruleT6MissingTurn2ToolResult)
		}
	}

	settledCount := len(lab.UsageLogs())
	if gen200 == 0 || settledCount != gen200 {
		rules = appendUniqueLabRule(rules, ruleT6UsageSettlementMismatch)
	}

	diag := fmt.Sprintf(
		"exit=%d completed=%v tools=%d errs=%v final=%q gen200=%d settled=%d gw5xx=%d stderr=%q",
		procRes.exitCode,
		procRes.completed,
		procRes.toolExecutions,
		procRes.errorMessages,
		truncateForDiag(procRes.finalText, 120),
		gen200,
		settledCount,
		gw5xx,
		truncateForDiag(procRes.stderr, 200),
	)

	if len(rules) > 0 {
		entry.Status = labCellFail
		entry.Rules = rules
	} else {
		entry.Status = labCellPass
	}
	return entry, diag
}

// newClientLabScriptedUpstreamServer starts an httptest.Server over mockH
// directly for standard HTTP wire endpoints, or wraps mockH for Vertex Claude
// (:streamRawPredict) and AWS Bedrock (/invoke-with-response-stream) so cloud
// envelope rules are verified before delegating Messages validation to mockH.
// Callers must close the returned server when the cell finishes.
func newClientLabScriptedUpstreamServer(
	prov labProviderKindSpec,
	mockH *mockupstream.Handler,
	recordCloudViolation func(string),
) *httptest.Server {
	isVertexClaude := prov.platform == capability.PlatformAnthropic && prov.providerType == capability.ProviderTypeServiceAccount
	isBedrock := prov.platform == capability.PlatformAnthropic && prov.providerType == capability.ProviderTypeBedrock
	if !isVertexClaude && !isBedrock {
		return httptest.NewServer(mockH)
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rawBody, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, "read body failed", http.StatusBadRequest)
			return
		}

		wantVer := "vertex-2023-10-16"
		isStream := strings.Contains(r.URL.Path, ":streamRawPredict")
		if isBedrock {
			wantVer = "bedrock-2023-05-31"
			isStream = strings.HasSuffix(r.URL.Path, "/invoke-with-response-stream")
		}
		if gjson.GetBytes(rawBody, "anthropic_version").String() != wantVer {
			if recordCloudViolation != nil {
				recordCloudViolation("cloud_anthropic_version_mismatch")
			}
		}
		if gjson.GetBytes(rawBody, "model").Exists() || (isBedrock && gjson.GetBytes(rawBody, "stream").Exists()) {
			if recordCloudViolation != nil {
				recordCloudViolation("cloud_anthropic_forbidden_model_or_stream")
			}
		}

		// Rehydrate canonical /v1/messages top-level fields so mockupstream runs
		// full strict Messages request validation and Turn 1/Turn 2 script matching.
		var root map[string]json.RawMessage
		if err := json.Unmarshal(rawBody, &root); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		delete(root, "anthropic_version")
		delete(root, "anthropic_beta")
		modelJSON, _ := json.Marshal(prov.upstreamModel)
		streamJSON, _ := json.Marshal(isStream)
		root["model"] = modelJSON
		root["stream"] = streamJSON
		canonical, _ := json.Marshal(root)

		r2 := r.Clone(r.Context())
		r2.URL.Path = "/v1/messages"
		r2.URL.RawQuery = ""
		r2.Body = io.NopCloser(bytes.NewReader(canonical))
		r2.ContentLength = int64(len(canonical))

		if !isBedrock || !isStream {
			mockH.ServeHTTP(w, r2)
			return
		}

		rec := httptest.NewRecorder()
		mockH.ServeHTTP(rec, r2)
		if rec.Code != http.StatusOK {
			for k, vals := range rec.Header() {
				for _, v := range vals {
					w.Header().Add(k, v)
				}
			}
			w.WriteHeader(rec.Code)
			_, _ = w.Write(rec.Body.Bytes())
			return
		}

		rdr := sse.NewReader(bytes.NewReader(rec.Body.Bytes()))
		var chunkJSONs []string
		for rdr.Scan() {
			f := rdr.Frame()
			if len(f.Data) > 0 {
				chunkJSONs = append(chunkJSONs, string(f.Data))
			}
		}
		eventStream := buildLabBedrockChunkStream(chunkJSONs...)
		w.Header().Set("Content-Type", "application/vnd.amazon.eventstream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(eventStream)
	}))
}

// stage7RemainingClientLabRoutes constructs the 37 scripted T6 routes that
// complete 100% real-CLI coverage of every non-Antigravity, non-Qoder
// [route_table] entry in testdata/golden/routes.txt.
func stage7RemainingClientLabRoutes() []clientLabRoute {
	anthropicOAuth := labProviderKindSpec{
		kind:             "anthropic_oauth",
		platform:         capability.PlatformAnthropic,
		providerType:     capability.ProviderTypeOAuth,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "claude-sonnet-4-5-20250929",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation:       testkit.RequestValidationOptions{StrictTopLevel: true},
		failoverStatus:   529,
	}
	anthropicSetupToken := labProviderKindSpec{
		kind:             "anthropic_setup_token",
		platform:         capability.PlatformAnthropic,
		providerType:     capability.ProviderTypeSetupToken,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "claude-sonnet-4-5-20250929",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation:       testkit.RequestValidationOptions{StrictTopLevel: true},
		failoverStatus:   529,
	}
	anthropicBedrock := labProviderKindSpec{
		kind:             "anthropic_bedrock",
		platform:         capability.PlatformAnthropic,
		providerType:     capability.ProviderTypeBedrock,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "claude-sonnet-4-6",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation:       testkit.RequestValidationOptions{StrictTopLevel: true},
		failoverStatus:   529,
	}
	anthropicVertex := labProviderKindSpec{
		kind:             "anthropic_vertex",
		platform:         capability.PlatformAnthropic,
		providerType:     capability.ProviderTypeServiceAccount,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "claude-sonnet-4-6",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation:       testkit.RequestValidationOptions{StrictTopLevel: true},
		failoverStatus:   529,
	}
	openAIOAuth := labProviderKindSpec{
		kind:             "openai_oauth",
		platform:         capability.PlatformOpenAI,
		providerType:     capability.ProviderTypeOAuth,
		upstreamProtocol: protocol.ProtocolOpenAIResponses,
		upstreamModel:    "gpt-5.4",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
		failoverStatus:   529,
	}
	geminiOAuth := labProviderKindSpec{
		kind:             "gemini_oauth",
		platform:         capability.PlatformGemini,
		providerType:     capability.ProviderTypeOAuth,
		upstreamProtocol: protocol.ProtocolGeminiGenerateContent,
		upstreamModel:    "gemini-2.5-flash",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolGeminiGenerateContent},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"gemini-signature"},
		},
		failoverStatus: http.StatusUnauthorized,
	}
	geminiVertexFlash := vertexGeminiFlashLabSpec()

	grokOAuthChat := labProviderKindSpec{
		kind:             "grok_oauth_chat",
		platform:         capability.PlatformGrok,
		providerType:     capability.ProviderTypeOAuth,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "grok-4.5",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		failoverStatus:   529,
	}
	grokAPIKeyChat := labProviderKindSpec{
		kind:             "grok_apikey_chat",
		platform:         capability.PlatformGrok,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "grok-4.5",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		failoverStatus:   529,
	}

	kimiResponses := labProviderKindSpec{
		kind:             "kimi_responses",
		platform:         capability.PlatformKimi,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIResponses,
		upstreamModel:    "k3",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		},
		failoverStatus: 529,
	}
	kimiChat := labProviderKindSpec{
		kind:             "kimi_chat",
		platform:         capability.PlatformKimi,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "k3",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		failoverStatus:   529,
	}
	kimiMessages := labProviderKindSpec{
		kind:             "kimi_messages",
		platform:         capability.PlatformKimi,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "k3",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation: testkit.RequestValidationOptions{
			StrictTopLevel: true,
			DialectRules:   []string{"passback"},
		},
		failoverStatus: 529,
	}

	zhipuChat := labProviderKindSpec{
		kind:             "zhipu_chat",
		platform:         capability.PlatformZhipu,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "glm-5",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		failoverStatus:   529,
	}
	zhipuMessages := labProviderKindSpec{
		kind:             "zhipu_messages",
		platform:         capability.PlatformZhipu,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "glm-5",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation: testkit.RequestValidationOptions{
			StrictTopLevel: true,
			DialectRules:   []string{"passback"},
		},
		failoverStatus: 529,
	}

	deepseekResponses := stage5DeepSeekResponsesLabSpec()
	deepseekMessages := findLabProviderKind("deepseek_anthropic")

	openCodeResponses := labProviderKindSpec{
		kind:             "opencode_responses",
		platform:         capability.PlatformOpenCode,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIResponses,
		upstreamModel:    "gpt-5.4",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		},
		failoverStatus: 529,
	}
	openCodeChat := labProviderKindSpec{
		kind:             "opencode_chat",
		platform:         capability.PlatformOpenCode,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "gpt-5.4",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		failoverStatus:   529,
	}
	openCodeMessages := labProviderKindSpec{
		kind:             "opencode_messages",
		platform:         capability.PlatformOpenCode,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolAnthropicMessages,
		upstreamModel:    "claude-sonnet-4-6",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolAnthropicMessages},
		validation:       testkit.RequestValidationOptions{StrictTopLevel: true},
		failoverStatus:   529,
	}

	return []clientLabRoute{
		makeCustomClientLabRoute("codex", anthropicOAuth),
		makeCustomClientLabRoute("pi_chat", anthropicOAuth),
		makeCustomClientLabRoute("codex", anthropicSetupToken),
		makeCustomClientLabRoute("pi_chat", anthropicSetupToken),
		makeCustomClientLabRoute("codex", anthropicBedrock),
		makeCustomClientLabRoute("pi_chat", anthropicBedrock),
		makeCustomClientLabRoute("codex", anthropicVertex),
		makeCustomClientLabRoute("pi_chat", anthropicVertex),
		makeCustomClientLabRoute("claude_bare", openAIOAuth),
		makeCustomClientLabRoute("pi_chat", openAIOAuth),
		makeCustomClientLabRoute("claude_bare", geminiOAuth),
		makeCustomClientLabRoute("codex", geminiOAuth),
		makeCustomClientLabRoute("pi_chat", geminiOAuth),
		makeCustomClientLabRoute("pi_chat", geminiVertexFlash),
		makeCustomClientLabRoute("claude_bare", grokOAuthChat),
		makeCustomClientLabRoute("codex", grokOAuthChat),
		makeCustomClientLabRoute("claude_bare", grokAPIKeyChat),
		makeCustomClientLabRoute("codex", grokAPIKeyChat),
		makeCustomClientLabRoute("pi_messages", kimiResponses),
		makeCustomClientLabRoute("pi_chat", kimiResponses),
		makeCustomClientLabRoute("pi_messages", kimiChat),
		makeCustomClientLabRoute("pi_responses", kimiChat),
		makeCustomClientLabRoute("pi_responses", kimiMessages),
		makeCustomClientLabRoute("pi_chat", kimiMessages),
		makeCustomClientLabRoute("pi_messages", zhipuChat),
		makeCustomClientLabRoute("pi_responses", zhipuChat),
		makeCustomClientLabRoute("pi_responses", zhipuMessages),
		makeCustomClientLabRoute("pi_chat", zhipuMessages),
		makeCustomClientLabRoute("pi_messages", deepseekResponses),
		makeCustomClientLabRoute("pi_chat", deepseekResponses),
		makeCustomClientLabRoute("pi_chat", deepseekMessages),
		makeCustomClientLabRoute("pi_messages", openCodeResponses),
		makeCustomClientLabRoute("pi_chat", openCodeResponses),
		makeCustomClientLabRoute("pi_messages", openCodeChat),
		makeCustomClientLabRoute("pi_responses", openCodeChat),
		makeCustomClientLabRoute("pi_responses", openCodeMessages),
		makeCustomClientLabRoute("pi_chat", openCodeMessages),
	}
}

// formatStage7RouteKey formats a four-part [route_table] lookup key matching
// testdata/golden/routes.txt columns 1 through 4.
func formatStage7RouteKey(clientProto protocol.ProtocolID, platform, providerType string, upstreamProto protocol.ProtocolID) string {
	return fmt.Sprintf("%s|%s|%s|%s", clientProto, platform, providerType, upstreamProto)
}

// buildStage7ScriptedT6RouteIndex constructs a deterministic map from each
// four-part [route_table] key to the ordered list of "internal/app:<TestFunc>"
// scripted T6 suites that actually execute real CLI subprocesses on that route.
// Native passthrough control routes are excluded so only cross-protocol cells appear.
func buildStage7ScriptedT6RouteIndex() map[string][]string {
	index := make(map[string][]string)
	bind := func(testRef string, r clientLabRoute) {
		if r.isNative {
			return
		}
		key := formatStage7RouteKey(
			r.profile.clientProtocol,
			r.provider.platform,
			r.provider.providerType,
			r.provider.upstreamProtocol,
		)
		if !slices.Contains(index[key], testRef) {
			index[key] = append(index[key], testRef)
		}
	}

	for _, r := range buildClientLabRoutes() {
		switch r.provider.kind {
		case "openai":
			bind("internal/app:TestClientLabIRResponsesUpstreamScenarios", r)
		case "openai_chat":
			bind("internal/app:TestClientLabIRChatUpstreamScenarios", r)
		case "anthropic":
			bind("internal/app:TestClientLabIRMessagesUpstreamScenarios", r)
		}
	}

	for _, prof := range clientLabProfiles {
		if prof.clientProtocol != protocol.ProtocolOpenAIChatCompletions &&
			prof.clientProtocol != protocol.ProtocolAnthropicMessages {
			continue
		}
		for _, spec := range grokLabProviderSpecs() {
			bind("internal/app:TestClientLabIRGrokResponsesUpstreamScenarios", makeCustomClientLabRoute(prof.slug, spec))
		}
	}

	deepseekChatSpec := labProviderKindSpec{
		kind:             "deepseek_chat",
		platform:         capability.PlatformDeepseek,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "deepseek-reasoner",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
	}
	grokOAuthSpec := grokLabProviderSpecs()[0]
	for _, r := range stage2ClientLabExtendedRoutes() {
		bind("internal/app:TestClientLabIRStage2ExtendedScenarios", r)
	}
	for _, r := range []clientLabRoute{
		makeCustomClientLabRoute("codex", deepseekChatSpec),
		makeCustomClientLabRoute("codex", findLabProviderKind("deepseek_anthropic")),
		makeCustomClientLabRoute("claude_default", grokOAuthSpec),
		makeCustomClientLabRoute("claude_bare", deepseekChatSpec),
		makeCustomClientLabRoute("pi_responses", deepseekChatSpec),
		makeCustomClientLabRoute("pi_chat", grokOAuthSpec),
		makeCustomClientLabRoute("pi_messages", deepseekChatSpec),
	} {
		bind("internal/app:TestClientLabIRStage2ExtendedScenarios", r)
	}

	vertexFlash := vertexGeminiFlashLabSpec()
	vertexPro := vertexGeminiLabSpec()
	openCodeGem := openCodeGeminiLabSpec()
	for _, r := range stage3ClientLabGeminiRoutes() {
		bind("internal/app:TestClientLabIRGeminiUpstreamScenarios", r)
		bind("internal/app:TestClientLabIRStage3GeminiExtendedScenarios", r)
	}
	for _, r := range []clientLabRoute{
		makeCustomClientLabRoute("codex", vertexFlash),
		makeCustomClientLabRoute("claude_bare", vertexFlash),
		makeCustomClientLabRoute("pi_chat", openCodeGem),
		makeCustomClientLabRoute("pi_responses", openCodeGem),
		makeCustomClientLabRoute("pi_messages", openCodeGem),
	} {
		bind("internal/app:TestClientLabIRGeminiUpstreamScenarios", r)
	}
	for _, r := range []clientLabRoute{
		makeCustomClientLabRoute("codex", vertexPro),
		makeCustomClientLabRoute("claude_default", vertexPro),
		makeCustomClientLabRoute("codex", openCodeGem),
		makeCustomClientLabRoute("claude_bare", openCodeGem),
		makeCustomClientLabRoute("pi_chat", openCodeGem),
		makeCustomClientLabRoute("pi_responses", openCodeGem),
	} {
		bind("internal/app:TestClientLabIRStage3GeminiExtendedScenarios", r)
	}

	for _, r := range stage7RemainingClientLabRoutes() {
		bind("internal/app:TestClientLabIRStage7RouteMatrixScenarios", r)
	}
	return index
}

// collectAllScriptedClientLabRouteKeys returns the set of four-part [route_table]
// keys exercised across all IR scripted T6 test suites in this file.
func collectAllScriptedClientLabRouteKeys() map[string]bool {
	index := buildStage7ScriptedT6RouteIndex()
	keys := make(map[string]bool, len(index))
	for k := range index {
		keys[k] = true
	}
	return keys
}

// configured reports whether both baseURL and apiKey are non-empty on e.
func (e liveProviderEndpoint) configured() bool {
	return e.baseURL != "" && e.apiKey != ""
}

// canRunReasoningTool reports whether p's Turn 1 live endpoint has credentials configured.
func (p liveCanaryPairSpec) canRunReasoningTool() bool {
	return p.turn1Endpoint.configured()
}

// canRunSwitchProvider reports whether both Turn 1 and Turn 2 native live
// endpoints have credentials configured on p.
func (p liveCanaryPairSpec) canRunSwitchProvider() bool {
	return p.turn1Endpoint.configured() && p.turn2Endpoint.configured()
}

// parseLiveCanaryConfigFromEnv reads trimmed live-canary base URLs, API keys,
// and optional model overrides through getenv without touching host files.
// Callers supply os.Getenv only inside verified disposable-container live runs
// and pass synthetic lookup closures during unit tests.
func parseLiveCanaryConfigFromEnv(getenv func(string) string) liveCanaryConfig {
	read := func(prefix string) liveProviderEndpoint {
		return liveProviderEndpoint{
			baseURL: strings.TrimSpace(getenv("TOKENROUTER_CLIENTLAB_LIVE_" + prefix + "_BASE_URL")),
			apiKey:  strings.TrimSpace(getenv("TOKENROUTER_CLIENTLAB_LIVE_" + prefix + "_API_KEY")),
			model:   strings.TrimSpace(getenv("TOKENROUTER_CLIENTLAB_LIVE_" + prefix + "_MODEL")),
		}
	}
	return liveCanaryConfig{
		openai:    read("OPENAI"),
		anthropic: read("ANTHROPIC"),
		gemini:    read("GEMINI"),
		grok:      read("GROK"),
		deepseek:  read("DEEPSEEK"),
	}
}

// applyLiveEndpointModel returns a shallow copy of base with its target model
// overridden when ep supplies a non-empty model string from the live canary
// environment. Blank model overrides leave the default upstream model configured
// on base untouched for downstream provider assembly.
func applyLiveEndpointModel(base labProviderKindSpec, ep liveProviderEndpoint) labProviderKindSpec {
	if ep.model != "" {
		base.upstreamModel = ep.model
	}
	return base
}

// buildStage7LiveCanaryGroups constructs all five Section 6.8 Step 4 live
// canary groups, binding each converted or dialect-lowered Turn 1 route to its
// native Turn 2 continuation target.
func buildStage7LiveCanaryGroups(cfg liveCanaryConfig) []liveCanaryGroupSpec {
	codexProf := findClientLabProfile("codex")
	claudeProf := findClientLabProfile("claude_default")

	openAISpec := applyLiveEndpointModel(findLabProviderKind("openai"), cfg.openai)
	anthropicSpec := applyLiveEndpointModel(findLabProviderKind("anthropic"), cfg.anthropic)
	geminiSpec := applyLiveEndpointModel(findLabProviderKind("gemini"), cfg.gemini)
	grokAPIKeySpec := applyLiveEndpointModel(grokLabProviderSpecs()[1], cfg.grok)
	deepseekChatSpec := applyLiveEndpointModel(labProviderKindSpec{
		kind:             "deepseek_chat",
		platform:         capability.PlatformDeepseek,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIChatCompletions,
		upstreamModel:    "deepseek-reasoner",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIChatCompletions},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"deepseek-thinking"},
		},
		failoverStatus: 529,
	}, cfg.deepseek)

	return []liveCanaryGroupSpec{
		{
			groupSlug:   "group1_codex_to_anthropic",
			description: "Codex to Anthropic Messages (anthropic carrier) and Turn 2 native OpenAI Responses",
			pairs: []liveCanaryPairSpec{{
				slug:          "codex_to_anthropic",
				profile:       codexProf,
				turn1Provider: anthropicSpec,
				turn1Endpoint: cfg.anthropic,
				turn2Native:   openAISpec,
				turn2Endpoint: cfg.openai,
				carrierScheme: ir.SchemeAnthropic,
			}},
		},
		{
			groupSlug:   "group2_claude_to_openai",
			description: "Claude Code to OpenAI Responses (openai carrier) and Turn 2 native Anthropic Messages",
			pairs: []liveCanaryPairSpec{{
				slug:          "claude_to_openai",
				profile:       claudeProf,
				turn1Provider: openAISpec,
				turn1Endpoint: cfg.openai,
				turn2Native:   anthropicSpec,
				turn2Endpoint: cfg.anthropic,
				carrierScheme: ir.SchemeOpenAI,
			}},
		},
		{
			groupSlug:   "group3_claude_and_codex_to_gemini",
			description: "Claude Code and Codex to Gemini GenerateContent (gemini carrier + CallMemory) and Turn 2 native paths",
			pairs: []liveCanaryPairSpec{
				{
					slug:          "claude_to_gemini",
					profile:       claudeProf,
					turn1Provider: geminiSpec,
					turn1Endpoint: cfg.gemini,
					turn2Native:   anthropicSpec,
					turn2Endpoint: cfg.anthropic,
					carrierScheme: ir.SchemeGemini,
				},
				{
					slug:          "codex_to_gemini",
					profile:       codexProf,
					turn1Provider: geminiSpec,
					turn1Endpoint: cfg.gemini,
					turn2Native:   openAISpec,
					turn2Endpoint: cfg.openai,
					carrierScheme: ir.SchemeGemini,
				},
			},
		},
		{
			groupSlug:   "group4_claude_and_codex_to_grok",
			description: "Claude Code to Grok and Codex via dialect tool lowering to Grok (xai carrier) and Turn 2 native paths",
			pairs: []liveCanaryPairSpec{
				{
					slug:          "claude_to_grok",
					profile:       claudeProf,
					turn1Provider: grokAPIKeySpec,
					turn1Endpoint: cfg.grok,
					turn2Native:   anthropicSpec,
					turn2Endpoint: cfg.anthropic,
					carrierScheme: ir.SchemeXAI,
				},
				{
					slug:          "codex_dialect_lowered_to_grok",
					profile:       codexProf,
					turn1Provider: grokAPIKeySpec,
					turn1Endpoint: cfg.grok,
					turn2Native:   openAISpec,
					turn2Endpoint: cfg.openai,
					carrierScheme: ir.SchemeXAI,
				},
			},
		},
		{
			groupSlug:   "group5_codex_to_deepseek_chat",
			description: "Codex to DeepSeek Chat Completions (plain carrier with reasoning_content) and Turn 2 native OpenAI Responses",
			pairs: []liveCanaryPairSpec{{
				slug:          "codex_to_deepseek_chat",
				profile:       codexProf,
				turn1Provider: deepseekChatSpec,
				turn1Endpoint: cfg.deepseek,
				turn2Native:   openAISpec,
				turn2Endpoint: cfg.openai,
				carrierScheme: ir.SchemePlain,
			}},
		},
	}
}

// buildLiveCanaryProviderSpec constructs a labProvider record pointing at
// proxyURL and carrying apiKey alongside any multi-slot protocol credentials.
// Nil credential maps are allocated before storing api_key so live auth never panics.
func buildLiveCanaryProviderSpec(id int64, proxyURL string, provSpec labProviderKindSpec, apiKey string) labProvider {
	spec := makeLabProviderSpec(id, 0, proxyURL, provSpec)
	if spec.Credentials == nil {
		spec.Credentials = make(map[string]any)
	}
	spec.Credentials["api_key"] = apiKey
	return spec
}

// liveCanaryPricingMap builds synthetic per-token ModelPricing entries for
// every model and mapping target across specs so live canary model overrides
// remain priced both at router startup and after dynamic SetProviders switches.
// Blank model strings and duplicate entries are skipped without overwriting
// earlier pricing records.
func liveCanaryPricingMap(specs ...labProvider) map[string]*pricing.ModelPricing {
	out := make(map[string]*pricing.ModelPricing)
	add := func(model string) {
		trimmed := strings.TrimSpace(model)
		if trimmed == "" || out[trimmed] != nil {
			return
		}
		out[trimmed] = &pricing.ModelPricing{
			InputPricePerToken:         1e-6,
			OutputPricePerToken:        2e-6,
			CacheCreationPricePerToken: 1.25e-6,
			CacheReadPricePerToken:     0.1e-6,
			CacheCreation5mPrice:       1.25e-6,
			CacheCreation1hPrice:       2e-6,
			SupportsCacheBreakdown:     true,
			SupportsServiceTier:        true,
		}
	}
	for _, s := range specs {
		add(s.Model)
		for _, m := range s.Models {
			add(m)
		}
		for src, dst := range s.ModelMapping {
			add(src)
			add(dst)
		}
	}
	return out
}

// newLiveCanaryHTTPClient returns an *http.Client whose transport backs off on
// transient live-upstream HTTP 429 rate-limit windows, HTTP 503 demand spikes,
// and immediate SSE concurrency rejections before returning to mockupstream's
// single-provider proxy. Callers pass defaultWait to govern 429 backoff when
// upstream responses omit explicit retry-delay hints.
func newLiveCanaryHTTPClient(defaultWait time.Duration) *http.Client {
	return &http.Client{
		Transport: &liveCanaryRetryTransport{
			base:        http.DefaultTransport,
			defaultWait: defaultWait,
			maxWait:     65 * time.Second,
			maxRetries:  3,
		},
	}
}

// RoundTrip buffers req.Body and retries transient HTTP 429, HTTP 503, and
// pre-output SSE concurrency rejections up to rt.maxRetries times while
// honoring Retry-After headers, Google rpc.RetryInfo durations, and caller
// context cancellation. Non-retryable statuses and healthy streams return
// immediately without adding extra latency.
//
// Inspecting the leading SSE bytes before returning HTTP 200 prevents aborted
// account-concurrency error streams from committing downstream status headers
// or skewing settled UsageLog parity on single-provider canary routers.
func (rt *liveCanaryRetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := rt.base
	if base == nil {
		base = http.DefaultTransport
	}
	var bodyBytes []byte
	if req.Body != nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return nil, err
		}
	}

	maxRetries := max(rt.maxRetries, 0)
	for attempt := 0; ; attempt++ {
		cloned := req.Clone(req.Context())
		if bodyBytes != nil {
			cloned.Body = io.NopCloser(bytes.NewReader(bodyBytes))
			cloned.ContentLength = int64(len(bodyBytes))
		}
		resp, err := base.RoundTrip(cloned)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusOK && attempt < maxRetries &&
			strings.Contains(strings.ToLower(resp.Header.Get("Content-Type")), "text/event-stream") {
			peeked, retrySSE := inspectLeadingLiveCanarySSE(resp.Body)
			if retrySSE {
				_ = resp.Body.Close()
				// Back off at the proxy boundary when an SSE stream aborts with a
				// concurrency error before any output item arrives so downstream HTTP 200 stays uncommitted.
				wait := min(3*time.Second*time.Duration(attempt+1), max(rt.defaultWait, 3*time.Second))
				if rt.maxWait > 0 && wait > rt.maxWait {
					wait = rt.maxWait
				}
				timer := time.NewTimer(wait)
				select {
				case <-req.Context().Done():
					timer.Stop()
					return nil, req.Context().Err()
				case <-timer.C:
				}
				continue
			}
			if len(peeked) > 0 {
				resp.Body = liveCanaryPrefixReadCloser{
					Reader: io.MultiReader(bytes.NewReader(peeked), resp.Body),
					Closer: resp.Body,
				}
			}
			return resp, nil
		}
		if (resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable) || attempt >= maxRetries {
			return resp, nil
		}

		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
		_ = resp.Body.Close()

		fallback := rt.defaultWait
		if resp.StatusCode == http.StatusServiceUnavailable {
			fallback = min(3*time.Second*time.Duration(attempt+1), rt.defaultWait)
		}
		wait := parseLiveCanaryRetryDelay(resp.Header, errBody, fallback)
		if rt.maxWait > 0 && wait > rt.maxWait {
			wait = rt.maxWait
		}
		// Wait out transient upstream RPM/TPM windows at the test-proxy boundary so
		// single-provider lab routers do not mark the sole canary credential unschedulable.
		timer := time.NewTimer(wait)
		select {
		case <-req.Context().Done():
			timer.Stop()
			return nil, req.Context().Err()
		case <-timer.C:
		}
	}
}

// parseLiveCanaryRetryDelay extracts a positive backoff wait from HTTP
// Retry-After seconds or Google rpc.RetryInfo.retryDelay JSON fields, adding a
// small safety margin when explicit hints are present. Missing or unparseable
// hints return fallback, or 15 seconds when fallback is non-positive.
func parseLiveCanaryRetryDelay(header http.Header, errBody []byte, fallback time.Duration) time.Duration {
	if rawAfter := strings.TrimSpace(header.Get("Retry-After")); rawAfter != "" {
		if secs, err := strconv.Atoi(rawAfter); err == nil && secs > 0 {
			return time.Duration(secs)*time.Second + time.Second
		}
	}
	for _, detail := range gjson.GetBytes(errBody, "error.details").Array() {
		if strings.Contains(detail.Get("@type").String(), "RetryInfo") {
			if rawDelay := strings.TrimSpace(detail.Get("retryDelay").String()); rawDelay != "" {
				if parsed, err := time.ParseDuration(rawDelay); err == nil && parsed > 0 {
					return parsed + min(parsed/4, 2*time.Second)
				}
			}
		}
	}
	if fallback <= 0 {
		return 15 * time.Second
	}
	return fallback
}

// inspectLeadingLiveCanarySSE reads up to 8 KiB from body until the first real
// output event marker arrives or EOF is reached, reporting retrySSE=true when
// the stream aborted immediately with an upstream concurrency or rate-limit
// error. Healthy streams return all inspected prefix bytes with retrySSE=false
// so callers replay them ahead of the remaining live body.
func inspectLeadingLiveCanarySSE(body io.Reader) ([]byte, bool) {
	var buf bytes.Buffer
	var scratch [1024]byte
	// Bound prefix inspection to 8 KiB so large preamble frames never buffer unboundedly.
	for buf.Len() < 8192 {
		n, err := body.Read(scratch[:])
		if n > 0 {
			_, _ = buf.Write(scratch[:n])
			cur := buf.Bytes()
			if bytes.Contains(cur, []byte("output_item.added")) ||
				bytes.Contains(cur, []byte("content_block_start")) ||
				bytes.Contains(cur, []byte("chat.completion.chunk")) ||
				bytes.Contains(cur, []byte(`"candidates"`)) ||
				bytes.Contains(cur, []byte("response.completed")) ||
				bytes.Contains(cur, []byte("[DONE]")) {
				return bytes.Clone(cur), false
			}
		}
		if err != nil {
			break
		}
	}
	collected := bytes.Clone(buf.Bytes())
	lower := bytes.ToLower(collected)
	if (bytes.Contains(lower, []byte("concurrency limit exceeded")) ||
		bytes.Contains(lower, []byte("rate_limit_exceeded")) ||
		bytes.Contains(lower, []byte("too many requests"))) &&
		(bytes.Contains(lower, []byte(`"error"`)) || bytes.Contains(lower, []byte("response.failed"))) {
		return collected, true
	}
	return collected, false
}

// runStage7LiveCanaries executes Section 6.8 Step 4's five live canary groups
// via mockupstream's ModeProxyRecord inside the
// disposable container. Each configured pair runs lab_reasoning_tool and, when
// its native Turn 2 endpoint is also configured, lab_switch_provider landing
// Turn 2 on the client's native path; invoking live mode with zero configured
// provider credentials fails t immediately.
func runStage7LiveCanaries(t *testing.T, labRoot string) {
	t.Helper()
	cfg := parseLiveCanaryConfigFromEnv(os.Getenv)
	groups := buildStage7LiveCanaryGroups(cfg)

	incomingDir := strings.TrimSpace(os.Getenv(clientLabIncomingEnv))
	if incomingDir == "" {
		incomingDir = filepath.Join(clientLabTestdataDir, "corpus", "_incoming")
	}

	ranAny := false
	for _, grp := range groups {
		var runnablePairs []liveCanaryPairSpec
		for _, p := range grp.pairs {
			if p.canRunReasoningTool() {
				runnablePairs = append(runnablePairs, p)
			}
		}
		if len(runnablePairs) == 0 {
			continue
		}
		ranAny = true
		t.Run(grp.groupSlug, func(t *testing.T) {
			for _, pair := range runnablePairs {
				t.Run(pair.slug+"/"+clientLabScenarioReasoningTool, func(t *testing.T) {
					runLiveReasoningToolCanary(t, labRoot, incomingDir, pair)
				})
				if pair.canRunSwitchProvider() {
					t.Run(pair.slug+"/"+clientLabScenarioSwitchProvider, func(t *testing.T) {
						runLiveSwitchProviderCanary(t, labRoot, incomingDir, pair)
					})
				}
			}
		})
	}
	require.True(t, ranAny, "live mode requires at least one TOKENROUTER_CLIENTLAB_LIVE_<PROVIDER>_BASE_URL and *_API_KEY pair")
}

// runLiveReasoningToolCanary runs Section 6.8 Step 4's lab_reasoning_tool live
// canary for pair through ModeProxyRecord. It
// asserts clean two-turn shell tool completion, Turn 2 tool-result replay, zero
// leaked trc1: carriers on outbound requests, and exact UsageLog settlement parity.
func runLiveReasoningToolCanary(t *testing.T, labRoot, incomingDir string, pair liveCanaryPairSpec) {
	t.Helper()
	token := generateClientLabMarker(t, clientLabModeLive, pair.slug, clientLabScenarioReasoningTool)
	doneToken := "LIVE_RSN_DONE_" + token

	proxyH := mockupstream.NewWithConfig(mockupstream.Config{
		Mode:       mockupstream.ModeProxyRecord,
		TargetURL:  pair.turn1Endpoint.baseURL,
		HTTPClient: newLiveCanaryHTTPClient(22 * time.Second),
		Capture: mockupstream.CaptureOptions{
			Dir:            incomingDir,
			FilePrefix:     "live_" + pair.slug + "_" + clientLabScenarioReasoningTool,
			RecordRequests: false,
			RecordStreams:  true,
			Meta: testkit.CorpusMeta{
				Protocol:   pair.turn1Provider.upstreamProtocol,
				Provider:   pair.turn1Provider.kind,
				Model:      pair.turn1Provider.upstreamModel,
				CapturedAt: time.Now().UTC().Format("2006-01-02"),
				Stream:     true,
				Scenario:   clientLabScenarioReasoningTool,
			},
		},
	})
	upSrv := httptest.NewServer(proxyH)
	defer upSrv.Close()

	turn1Lab := buildLiveCanaryProviderSpec(101, upSrv.URL, pair.turn1Provider, pair.turn1Endpoint.apiKey)
	lab := newProtocolLabRouter(t, labOptions{
		Providers:     []labProvider{turn1Lab},
		Pricing:       liveCanaryPricingMap(turn1Lab),
		StallTimeout:  150 * time.Second,
		HeaderTimeout: 150 * time.Second,
	})
	gwRec := &clientLabGatewayRecorder{next: lab}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	prompt := "Think carefully, use your shell tool to run `tr -d '\\r\\n' < seed.txt > result.txt && cat result.txt` in the current working directory, and then reply including LIVE_RSN_DONE_ followed immediately by the exact contents of result.txt."
	inv := disableLiveCodexHostedWebSearch(
		prepareClientLabInvocation(t, labRoot, pair.profile, gwSrv.URL, prompt, true),
		pair.profile,
	)
	require.NoError(t, os.WriteFile(filepath.Join(inv.workDir, "seed.txt"), []byte(token+"\n"), 0o600))
	res := runClientLabSubprocess(t.Context(), inv, clientLabLiveTimeout)
	// Drain active gateway and proxy handlers after CLI exit so detached upstream
	// streams finish closing and commit RecordedRequest and UsageLog entries before inspection.
	gwSrv.Close()
	upSrv.Close()

	gw5xx, gen200, _ := gwRec.summary()
	proxyDiag := summarizeLiveProxyExchanges(proxyH.Requests())
	require.Zero(t, gw5xx, "gateway returned 5xx during live reasoning_tool canary on %s (%s)", pair.slug, proxyDiag)
	require.NoError(t, res.runErr, "errs=%v upstream=%s stderr=%s", res.errorMessages, proxyDiag, truncateForDiag(res.stderr, 300))
	require.Zero(t, res.exitCode, "errs=%v upstream=%s stderr=%s", res.errorMessages, proxyDiag, truncateForDiag(res.stderr, 300))
	require.True(t, res.completed, "errs=%v upstream=%s", res.errorMessages, proxyDiag)
	require.Empty(t, res.errorMessages, "client reported errors on %s: %v (%s)", pair.slug, res.errorMessages, proxyDiag)
	require.GreaterOrEqual(t, res.toolExecutions, 1)
	require.Contains(t, res.finalText, doneToken)

	gotFile, err := os.ReadFile(filepath.Join(inv.workDir, "result.txt"))
	require.NoError(t, err)
	require.Contains(t, string(gotFile), token)

	for _, rec := range proxyH.Requests() {
		require.False(t, hasUnstrippedCarrierInTokenPaths(pair.turn1Provider.upstreamProtocol, rec.Body),
			"unstripped trc1: carrier leaked upstream on %s", pair.slug)
	}
	require.True(t, hasTurn2ToolResultRequest(proxyH.Requests()), "Turn 2 tool result request not observed on %s (%s)", pair.slug, proxyDiag)
	require.GreaterOrEqual(t, gen200, 2)
	require.Equal(t, gen200, len(lab.UsageLogs()))
}

// runLiveSwitchProviderCanary runs Section 6.8 Step 4's lab_switch_provider
// live canary for pair. Turn 1 executes on
// pair.turn1Provider and Turn 2 dynamically switches the active provider to
// pair.turn2Native as soon as the client sends its tool result, verifying that
// PrepareNativeReplay strips foreign trc1: carriers cleanly on the native path.
func runLiveSwitchProviderCanary(t *testing.T, labRoot, incomingDir string, pair liveCanaryPairSpec) {
	t.Helper()
	token := generateClientLabMarker(t, clientLabModeLive, pair.slug, clientLabScenarioSwitchProvider)
	doneToken := "LIVE_SW_DONE_" + token

	firstProxy := mockupstream.NewWithConfig(mockupstream.Config{
		Mode:       mockupstream.ModeProxyRecord,
		TargetURL:  pair.turn1Endpoint.baseURL,
		HTTPClient: newLiveCanaryHTTPClient(22 * time.Second),
		Capture: mockupstream.CaptureOptions{
			Dir:            incomingDir,
			FilePrefix:     "live_" + pair.slug + "_switch_turn1",
			RecordRequests: false,
			RecordStreams:  true,
			Meta: testkit.CorpusMeta{
				Protocol:   pair.turn1Provider.upstreamProtocol,
				Provider:   pair.turn1Provider.kind,
				Model:      pair.turn1Provider.upstreamModel,
				CapturedAt: time.Now().UTC().Format("2006-01-02"),
				Stream:     true,
				Scenario:   clientLabScenarioSwitchProvider,
			},
		},
	})
	secondProxy := mockupstream.NewWithConfig(mockupstream.Config{
		Mode:       mockupstream.ModeProxyRecord,
		TargetURL:  pair.turn2Endpoint.baseURL,
		HTTPClient: newLiveCanaryHTTPClient(22 * time.Second),
		Capture: mockupstream.CaptureOptions{
			Dir:            incomingDir,
			FilePrefix:     "live_" + pair.slug + "_switch_turn2_native",
			RecordRequests: false,
			RecordStreams:  true,
			Meta: testkit.CorpusMeta{
				Protocol:   pair.turn2Native.upstreamProtocol,
				Provider:   pair.turn2Native.kind,
				Model:      pair.turn2Native.upstreamModel,
				CapturedAt: time.Now().UTC().Format("2006-01-02"),
				Stream:     true,
				Scenario:   clientLabScenarioSwitchProvider,
			},
		},
	})
	firstSrv := httptest.NewServer(firstProxy)
	defer firstSrv.Close()
	secondSrv := httptest.NewServer(secondProxy)
	defer secondSrv.Close()

	turn1Lab := buildLiveCanaryProviderSpec(101, firstSrv.URL, pair.turn1Provider, pair.turn1Endpoint.apiKey)
	turn2Lab := buildLiveCanaryProviderSpec(202, secondSrv.URL, pair.turn2Native, pair.turn2Endpoint.apiKey)
	lab := newProtocolLabRouter(t, labOptions{
		Providers:     []labProvider{turn1Lab},
		Pricing:       liveCanaryPricingMap(turn1Lab, turn2Lab),
		StallTimeout:  150 * time.Second,
		HeaderTimeout: 150 * time.Second,
	})

	var switched sync.Once
	switcher := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.Body != nil {
			if _, isGen := mockupstream.IdentifyEndpoint(r.URL.Path, r.URL.RawQuery); isGen {
				rawBody, _ := io.ReadAll(r.Body)
				_ = r.Body.Close()
				r.Body = io.NopCloser(bytes.NewReader(rawBody))
				if clientRequestCarriesToolResult(pair.profile.clientProtocol, rawBody) {
					// Switch the group's active provider to the native target once
					// before forwarding Turn 2 so PrepareNativeReplay strips Turn 1's carrier.
					switched.Do(func() {
						lab.SetProviders(t, []labProvider{turn2Lab})
					})
				}
			}
		}
		lab.ServeHTTP(w, r)
	})

	gwRec := &clientLabGatewayRecorder{next: switcher}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	prompt := "Think carefully, use your shell tool to run `tr -d '\\r\\n' < seed.txt > result.txt && cat result.txt` in the current working directory, and then reply including LIVE_SW_DONE_ followed immediately by the exact contents of result.txt."
	inv := disableLiveCodexHostedWebSearch(
		prepareClientLabInvocation(t, labRoot, pair.profile, gwSrv.URL, prompt, true),
		pair.profile,
	)
	require.NoError(t, os.WriteFile(filepath.Join(inv.workDir, "seed.txt"), []byte(token+"\n"), 0o600))
	res := runClientLabSubprocess(t.Context(), inv, clientLabLiveTimeout)
	// Drain active gateway and proxy handlers after CLI exit so detached upstream
	// streams finish closing and commit RecordedRequest and UsageLog entries before inspection.
	gwSrv.Close()
	firstSrv.Close()
	secondSrv.Close()

	gw5xx, gen200, _ := gwRec.summary()
	turn1Diag := summarizeLiveProxyExchanges(firstProxy.Requests())
	turn2Diag := summarizeLiveProxyExchanges(secondProxy.Requests())
	require.Zero(t, gw5xx, "gateway returned 5xx during live switch_provider canary on %s (t1=%s t2=%s)", pair.slug, turn1Diag, turn2Diag)
	require.NoError(t, res.runErr, "errs=%v t1=%s t2=%s stderr=%s", res.errorMessages, turn1Diag, turn2Diag, truncateForDiag(res.stderr, 300))
	require.Zero(t, res.exitCode, "errs=%v t1=%s t2=%s stderr=%s", res.errorMessages, turn1Diag, turn2Diag, truncateForDiag(res.stderr, 300))
	require.True(t, res.completed, "errs=%v t1=%s t2=%s", res.errorMessages, turn1Diag, turn2Diag)
	require.Empty(t, res.errorMessages, "client reported errors on %s: %v (t1=%s t2=%s)", pair.slug, res.errorMessages, turn1Diag, turn2Diag)
	require.NotEmpty(t, firstProxy.Requests(), "Turn 1 provider was not contacted on %s", pair.slug)
	require.True(t, hasTurn2ToolResultRequest(secondProxy.Requests()), "Turn 2 did not land on native provider for %s (t1=%s t2=%s)", pair.slug, turn1Diag, turn2Diag)
	for _, rec := range secondProxy.Requests() {
		require.False(t, hasUnstrippedCarrierInTokenPaths(pair.turn2Native.upstreamProtocol, rec.Body),
			"unstripped trc1: carrier leaked on native Turn 2 (%s)", pair.slug)
		require.False(t, hasLeakedForeignReplayToken(pair.turn2Native, rec.Body),
			"foreign replay token leaked on native Turn 2 (%s)", pair.slug)
	}

	gotFile, err := os.ReadFile(filepath.Join(inv.workDir, "result.txt"))
	require.NoError(t, err)
	require.Contains(t, string(gotFile), token)
	require.Contains(t, res.finalText, doneToken)
	require.GreaterOrEqual(t, gen200, 2)
	require.Equal(t, gen200, len(lab.UsageLogs()))
}

// disableLiveCodexHostedWebSearch inserts `-c web_search="disabled"` into a
// live Codex CLI invocation while preserving custom, function, and tool_search
// client tools. Disabling server-side web_search keeps scripted corpus routes
// untouched while avoiding Google AI Studio zero-quota Search Grounding 429s.
func disableLiveCodexHostedWebSearch(inv clientLabInvocation, prof clientLabProfile) clientLabInvocation {
	if prof.driver != clientDriverCodex || len(inv.args) == 0 {
		return inv
	}
	// Insert right after the leading "exec" subcommand so trailing positional prompt stays last.
	updated := make([]string, 0, len(inv.args)+2)
	updated = append(updated, inv.args[0], "-c", `web_search="disabled"`)
	updated = append(updated, inv.args[1:]...)
	inv.args = updated
	return inv
}

// generateClientLabMarker returns a deterministic marker string in record mode
// so imported corpus files stay byte-reproducible. Scripted and live modes
// generate a fresh crypto/rand hex marker so stale state cannot mask broken execution.
func generateClientLabMarker(tb testing.TB, mode clientLabMode, routeSlug, scenario string) string {
	tb.Helper()
	if mode == clientLabModeRecord {
		return clientLabRecordedTokenSeed + "_" + scenario
	}
	var raw [8]byte
	_, err := rand.Read(raw[:])
	require.NoError(tb, err)
	return fmt.Sprintf("tr_lab_%s_%s", strings.ReplaceAll(routeSlug, "_", ""), hex.EncodeToString(raw[:]))
}

// buildClientLabScenarioScript constructs the declarative mockupstream.Script,
// CLI user prompt, and expected final assistant substring for scenario. Text
// scenarios emit one assistant reply, whereas tool scenarios require HasTools
// on Turn 1 so toolless background requests fall through to Script.Default.
func buildClientLabScenarioScript(route clientLabRoute, scenario, token string) (testkit.Script, string, string) {
	upProto := route.provider.upstreamProtocol
	upModel := route.provider.upstreamModel

	if scenario == clientLabScenarioText {
		replyText := "Verified marker: " + token
		sse := buildUpstreamTextSSE(upProto, upModel, "lab_text_1", replyText)
		zeroTools := 0
		return testkit.Script{
			Name: "clientlab_text",
			Turns: []testkit.Turn{{
				Match: testkit.Match{ToolResults: &zeroTools},
				Reply: testkit.Reply{Raw: sse},
			}},
			Default: testkit.Reply{Raw: sse},
		}, "Reply briefly to acknowledge this ping.", token
	}

	shellCmd := fmt.Sprintf("printf '%%s' '%s' > result.txt", token)
	argsObj := map[string]string{
		route.profile.shellCommandField: shellCmd,
	}
	argsBytes, err := json.Marshal(argsObj)
	if err != nil {
		panic(fmt.Sprintf("marshal shell tool args: %v", err))
	}

	doneToken := "TOOL_DONE_" + token
	turn1SSE := buildUpstreamToolCallSSE(upProto, upModel, "call_clientlab_1", route.profile.shellToolName, string(argsBytes))
	turn2SSE := buildUpstreamTextSSE(upProto, upModel, "lab_tool_2", "Completed write: "+doneToken)
	bgSSE := buildUpstreamTextSSE(upProto, upModel, "lab_bg", "Background acknowledgment")

	zeroResults := 0
	oneResult := 1
	hasTools := true
	return testkit.Script{
		Name: "clientlab_tool",
		Turns: []testkit.Turn{
			{
				Match: testkit.Match{ToolResults: &zeroResults, HasTools: &hasTools},
				Reply: testkit.Reply{Raw: turn1SSE},
			},
			{
				Match: testkit.Match{ToolResults: &oneResult},
				Reply: testkit.Reply{Raw: turn2SSE},
			},
		},
		Default: testkit.Reply{Raw: bgSSE},
	}, "Use your shell tool to run the command requested by the assistant and then confirm.", doneToken
}

// buildUpstreamTextSSE synthesizes a valid single-turn streaming text SSE
// payload in upProto's native wire format with deterministic token usage.
// Unsupported upstream protocol identifiers panic immediately because T6 routes
// are restricted to the four registered text generation protocols.
func buildUpstreamTextSSE(upProto protocol.ProtocolID, upModel, idSuffix, text string) string {
	textJSON, _ := json.Marshal(text)
	modelJSON, _ := json.Marshal(upModel)

	switch upProto {
	case protocol.ProtocolAnthropicMessages:
		return strings.Join([]string{
			"event: message_start",
			fmt.Sprintf(`data: {"type":"message_start","message":{"id":"msg_%s","type":"message","role":"assistant","model":%s,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":24,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}`, idSuffix, modelJSON),
			"",
			"event: content_block_start",
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			"",
			"event: content_block_delta",
			fmt.Sprintf(`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":%s}}`, textJSON),
			"",
			"event: content_block_stop",
			`data: {"type":"content_block_stop","index":0}`,
			"",
			"event: message_delta",
			`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":12}}`,
			"",
			"event: message_stop",
			`data: {"type":"message_stop"}`,
			"",
			"",
		}, "\n")

	case protocol.ProtocolOpenAIResponses:
		msgID := "msg_" + idSuffix
		respID := "resp_" + idSuffix
		return strings.Join([]string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":%q,"object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, respID, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":%q,"object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, respID, modelJSON),
			"",
			"event: response.output_item.added",
			fmt.Sprintf(`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":%q,"type":"message","status":"in_progress","role":"assistant","content":[]}}`, msgID),
			"",
			"event: response.content_part.added",
			fmt.Sprintf(`data: {"type":"response.content_part.added","sequence_number":3,"item_id":%q,"output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"text":""}}`, msgID),
			"",
			"event: response.output_text.delta",
			fmt.Sprintf(`data: {"type":"response.output_text.delta","sequence_number":4,"item_id":%q,"output_index":0,"content_index":0,"delta":%s}`, msgID, textJSON),
			"",
			"event: response.output_text.done",
			fmt.Sprintf(`data: {"type":"response.output_text.done","sequence_number":5,"item_id":%q,"output_index":0,"content_index":0,"text":%s}`, msgID, textJSON),
			"",
			"event: response.content_part.done",
			fmt.Sprintf(`data: {"type":"response.content_part.done","sequence_number":6,"item_id":%q,"output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"text":%s}}`, msgID, textJSON),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":7,"output_index":0,"item":{"id":%q,"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","annotations":[],"text":%s}]}}`, msgID, textJSON),
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":8,"response":{"id":%q,"object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":%q,"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","annotations":[],"text":%s}]}],"usage":{"input_tokens":24,"output_tokens":12,"total_tokens":36,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, respID, modelJSON, msgID, textJSON),
			"",
			"",
		}, "\n")

	case protocol.ProtocolOpenAIChatCompletions:
		chatID := "chatcmpl_" + idSuffix
		return strings.Join([]string{
			fmt.Sprintf(`data: {"id":%q,"object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{"role":"assistant","content":%s},"finish_reason":null}]}`, chatID, modelJSON, textJSON),
			"",
			fmt.Sprintf(`data: {"id":%q,"object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, chatID, modelJSON),
			"",
			fmt.Sprintf(`data: {"id":%q,"object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[],"usage":{"prompt_tokens":24,"completion_tokens":12,"total_tokens":36,"prompt_tokens_details":{"cached_tokens":0},"completion_tokens_details":{"reasoning_tokens":0}}}`, chatID, modelJSON),
			"",
			"data: [DONE]",
			"",
			"",
		}, "\n")

	case protocol.ProtocolGeminiGenerateContent:
		return fmt.Sprintf(
			"data: {\"responseId\":%q,\"modelVersion\":%s,\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":%s}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":24,\"candidatesTokenCount\":12,\"totalTokenCount\":36}}\n\n",
			"gem_"+idSuffix,
			modelJSON,
			textJSON,
		)

	default:
		panic("unsupported upstream protocol: " + string(upProto))
	}
}

// buildUpstreamToolCallSSE synthesizes a streaming single-tool-call SSE payload
// in upProto's native wire format, quoting argsJSON on Messages, Responses, and
// Chat while embedding raw object args plus thoughtSignature on Gemini.
// Unsupported upstream protocol identifiers panic immediately.
func buildUpstreamToolCallSSE(upProto protocol.ProtocolID, upModel, callID, toolName, argsJSON string) string {
	argsQuoted, _ := json.Marshal(argsJSON)
	modelJSON, _ := json.Marshal(upModel)
	nameJSON, _ := json.Marshal(toolName)

	switch upProto {
	case protocol.ProtocolAnthropicMessages:
		return strings.Join([]string{
			"event: message_start",
			fmt.Sprintf(`data: {"type":"message_start","message":{"id":"msg_tool_1","type":"message","role":"assistant","model":%s,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":48,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}`, modelJSON),
			"",
			"event: content_block_start",
			fmt.Sprintf(`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":%q,"name":%s,"input":{}}}`, callID, nameJSON),
			"",
			"event: content_block_delta",
			fmt.Sprintf(`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":%s}}`, argsQuoted),
			"",
			"event: content_block_stop",
			`data: {"type":"content_block_stop","index":0}`,
			"",
			"event: message_delta",
			`data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":16}}`,
			"",
			"event: message_stop",
			`data: {"type":"message_stop"}`,
			"",
			"",
		}, "\n")

	case protocol.ProtocolOpenAIResponses:
		itemID := "fc_" + callID
		return strings.Join([]string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_tool_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_tool_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.output_item.added",
			fmt.Sprintf(`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":%q,"type":"function_call","status":"in_progress","call_id":%q,"name":%s,"arguments":""}}`, itemID, callID, nameJSON),
			"",
			"event: response.function_call_arguments.delta",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.delta","sequence_number":3,"item_id":%q,"output_index":0,"delta":%s}`, itemID, argsQuoted),
			"",
			"event: response.function_call_arguments.done",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.done","sequence_number":4,"item_id":%q,"output_index":0,"arguments":%s}`, itemID, argsQuoted),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}}`, itemID, callID, nameJSON, argsQuoted),
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":6,"response":{"id":"resp_tool_1","object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}],"usage":{"input_tokens":48,"output_tokens":16,"total_tokens":64,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON, itemID, callID, nameJSON, argsQuoted),
			"",
			"",
		}, "\n")

	case protocol.ProtocolOpenAIChatCompletions:
		return strings.Join([]string{
			fmt.Sprintf(`data: {"id":"chatcmpl_tool_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":%q,"type":"function","function":{"name":%s,"arguments":""}}]},"finish_reason":null}]}`, modelJSON, callID, nameJSON),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_tool_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":%s}}]},"finish_reason":null}]}`, modelJSON, argsQuoted),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_tool_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`, modelJSON),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_tool_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[],"usage":{"prompt_tokens":48,"completion_tokens":16,"total_tokens":64,"prompt_tokens_details":{"cached_tokens":0},"completion_tokens_details":{"reasoning_tokens":0}}}`, modelJSON),
			"",
			"data: [DONE]",
			"",
			"",
		}, "\n")

	case protocol.ProtocolGeminiGenerateContent:
		return fmt.Sprintf(
			"data: {\"responseId\":\"gem_tool_1\",\"modelVersion\":%s,\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":%q,\"name\":%s,\"args\":%s},\"thoughtSignature\":\"sig_clientlab_tool_1\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":48,\"candidatesTokenCount\":16,\"totalTokenCount\":64}}\n\n",
			modelJSON,
			callID,
			nameJSON,
			argsJSON,
		)

	default:
		panic("unsupported upstream protocol: " + string(upProto))
	}
}

// prepareClientLabInvocation renders configuration templates from labRoot into
// isolated home, work, and config subdirectories under tb.TempDir() and returns
// the command specification for prof. Thinking controls remain disabled unless
// enableReasoning is true so text and tool scenarios isolate non-reasoning paths.
func prepareClientLabInvocation(
	tb testing.TB,
	labRoot string,
	prof clientLabProfile,
	gatewayBaseURL string,
	prompt string,
	enableReasoning bool,
) clientLabInvocation {
	tb.Helper()
	runRoot := tb.TempDir()
	homeDir := filepath.Join(runRoot, "home")
	workDir := filepath.Join(runRoot, "work")
	configDir := filepath.Join(runRoot, "config")
	for _, d := range []string{homeDir, workDir, configDir} {
		require.NoError(tb, os.MkdirAll(d, 0o755))
	}

	binDir := filepath.Join(labRoot, "node_modules", ".bin")
	tmplRoot := filepath.Join(labRoot, "templates")
	env := buildIsolatedClientEnv(homeDir)

	switch prof.driver {
	case clientDriverCodex:
		effort := "none"
		if enableReasoning {
			effort = "medium"
		}
		rendered := renderClientTemplateFile(tb, filepath.Join(tmplRoot, "codex", "config.toml.tmpl"), map[string]string{
			"{{MODEL}}":            prof.clientModel,
			"{{REASONING_EFFORT}}": effort,
			"{{BASE_URL}}":         strings.TrimRight(gatewayBaseURL, "/"),
			"{{API_KEY_ENV}}":      clientLabAPIKeyEnvName,
		})
		require.NoError(tb, os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(rendered), 0o600))
		env = append(env,
			"CODEX_HOME="+configDir,
			clientLabAPIKeyEnvName+"="+clientLabSyntheticAPIKey,
		)
		return clientLabInvocation{
			executable: clientBinaryPath(binDir, "codex"),
			args: []string{
				"exec",
				"--json",
				"--disable", "plugins",
				"--disable", "remote_plugin",
				"--skip-git-repo-check",
				"--ephemeral",
				"--dangerously-bypass-approvals-and-sandbox",
				"-C", workDir,
				prompt,
			},
			env:       env,
			workDir:   workDir,
			configDir: configDir,
		}

	case clientDriverClaudeCode:
		rendered := renderClientTemplateFile(tb, filepath.Join(tmplRoot, "claude-code", "settings.json.tmpl"), map[string]string{
			"{{BASE_URL}}": strings.TrimRight(gatewayBaseURL, "/"),
			"{{MODEL}}":    prof.clientModel,
		})
		settingsPath := filepath.Join(configDir, "settings.json")
		require.NoError(tb, os.WriteFile(settingsPath, []byte(rendered), 0o600))
		env = append(env,
			"ANTHROPIC_BASE_URL="+strings.TrimRight(gatewayBaseURL, "/"),
			"ANTHROPIC_API_KEY="+clientLabSyntheticAPIKey,
			"ANTHROPIC_MODEL="+prof.clientModel,
			"ANTHROPIC_DEFAULT_HAIKU_MODEL="+prof.clientModel,
			"CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1",
			"DISABLE_AUTOUPDATER=1",
			"DISABLE_TELEMETRY=1",
			"DISABLE_ERROR_REPORTING=1",
			"CLAUDE_CONFIG_DIR="+configDir,
		)
		if !enableReasoning {
			env = append(env, "MAX_THINKING_TOKENS=0")
		}
		var args []string
		if prof.claudeBare {
			args = append(args, "--bare")
		}
		args = append(args,
			"-p", prompt,
			"--output-format", "stream-json",
			"--verbose",
			"--no-session-persistence",
			"--permission-mode", "bypassPermissions",
		)
		return clientLabInvocation{
			executable: clientBinaryPath(binDir, "claude"),
			args:       args,
			env:        env,
			workDir:    workDir,
			configDir:  configDir,
		}

	case clientDriverPi:
		thinkingLevel := "off"
		reasoningBool := "false"
		if enableReasoning {
			thinkingLevel = "medium"
			reasoningBool = "true"
		}
		modelsJSON := renderClientTemplateFile(tb, filepath.Join(tmplRoot, "pi", "models.json.tmpl"), map[string]string{
			"{{API_KEY_ENV}}":       clientLabAPIKeyEnvName,
			"{{BASE_URL}}":          strings.TrimRight(gatewayBaseURL, "/"),
			"{{CHAT_MODEL}}":        "gpt-4o",
			"{{RESPONSES_MODEL}}":   "gpt-5",
			"{{MESSAGES_MODEL}}":    "claude-sonnet-4-5",
			"{{GEMINI_MODEL}}":      "gemini-2.5-pro",
			"{{REASONING_ENABLED}}": reasoningBool,
		})
		settingsJSON := renderClientTemplateFile(tb, filepath.Join(tmplRoot, "pi", "settings.json.tmpl"), map[string]string{
			"{{THINKING_LEVEL}}": thinkingLevel,
		})
		require.NoError(tb, os.WriteFile(filepath.Join(configDir, "models.json"), []byte(modelsJSON), 0o600))
		require.NoError(tb, os.WriteFile(filepath.Join(configDir, "settings.json"), []byte(settingsJSON), 0o600))
		env = append(env,
			"PI_CODING_AGENT_DIR="+configDir,
			"PI_TELEMETRY=0",
			"PI_OFFLINE=1",
			clientLabAPIKeyEnvName+"="+clientLabSyntheticAPIKey,
		)
		args := []string{
			"-p",
			"--mode", "json",
			"--model", "lab/" + prof.clientModel,
			"--no-session",
			"--offline",
			"--no-extensions",
			"--no-skills",
			"--no-context-files",
		}
		if enableReasoning {
			args = append(args, "--thinking", "medium")
		}
		args = append(args, prompt)
		return clientLabInvocation{
			executable: clientBinaryPath(binDir, "pi"),
			args:       args,
			env:        env,
			workDir:    workDir,
			configDir:  configDir,
		}

	default:
		tb.Fatalf("unsupported client driver %q", prof.driver)
		return clientLabInvocation{}
	}
}

// renderClientTemplateFile reads the template file at tmplPath and substitutes
// every placeholder key from replacements in deterministic lexical order.
// Missing or unreadable template files fail tb immediately before returning.
func renderClientTemplateFile(tb testing.TB, tmplPath string, replacements map[string]string) string {
	tb.Helper()
	raw, err := os.ReadFile(tmplPath)
	require.NoError(tb, err)
	keys := make([]string, 0, len(replacements))
	for k := range replacements {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	out := string(raw)
	for _, k := range keys {
		out = strings.ReplaceAll(out, k, replacements[k])
	}
	return out
}

// buildIsolatedClientEnv constructs a minimal environment slice preserving only
// safe OS variables while pointing HOME and USERPROFILE at homeDir. Ambient
// provider API keys and outbound proxy variables are omitted, and NO_PROXY
// forces loopback gateway traffic to connect directly.
func buildIsolatedClientEnv(homeDir string) []string {
	env := make([]string, 0, len(safeInheritedEnvKeys)+4)
	for _, key := range safeInheritedEnvKeys {
		if val, ok := os.LookupEnv(key); ok && val != "" {
			env = append(env, key+"="+val)
		}
	}
	env = append(env,
		"HOME="+homeDir,
		"USERPROFILE="+homeDir,
		"NO_PROXY=127.0.0.1,localhost",
		"no_proxy=127.0.0.1,localhost",
	)
	return env
}

// runClientLabSubprocess executes inv under timeout, captures stdout and stderr,
// and parses the structured JSONL event stream according to the executable kind.
// Startup failures or context cancellations that produce no OS ExitError report
// exitCode -1 alongside runErr.
func runClientLabSubprocess(parentCtx context.Context, inv clientLabInvocation, timeout time.Duration) clientLabProcessResult {
	// Bound each CLI subprocess with an independent context deadline so truncated
	// SSE streams or unexpected prompts terminate instead of hanging the test binary.
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, inv.executable, inv.args...)
	cmd.Dir = inv.workDir
	cmd.Env = inv.env

	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf

	runErr := cmd.Run()
	exitCode := 0
	if runErr != nil {
		if exitErr, ok := errors.AsType[*exec.ExitError](runErr); ok {
			exitCode = exitErr.ExitCode()
		} else {
			// Distinguish spawn failures and context-cancellation kills that never
			// produced an OS wait status from a clean exit code zero.
			exitCode = -1
		}
	}

	stdoutStr := stdoutBuf.String()
	baseName := strings.ToLower(filepath.Base(inv.executable))
	var parsed clientLabProcessResult
	switch {
	case strings.HasPrefix(baseName, "codex"):
		parsed = parseCodexJSONLOutput(stdoutStr)
	case strings.HasPrefix(baseName, "claude"):
		parsed = parseClaudeStreamJSONOutput(stdoutStr)
	case strings.HasPrefix(baseName, "pi"):
		parsed = parsePiJSONOutput(stdoutStr)
	}
	parsed.exitCode = exitCode
	parsed.runErr = runErr
	parsed.stdout = stdoutStr
	parsed.stderr = stderrBuf.String()
	return parsed
}

// parseCodexJSONLOutput extracts final agent message text, command execution
// counts, completion status, and error events from `codex exec --json` stdout.
// Blank or non-JSON lines are ignored, and failed command exits or error items
// append entries to errorMessages.
func parseCodexJSONLOutput(stdout string) clientLabProcessResult {
	var out clientLabProcessResult
	for line := range strings.SplitSeq(stdout, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || trimmed[0] != '{' {
			continue
		}
		evType := gjson.Get(trimmed, "type").String()
		switch evType {
		case "item.completed":
			itemType := gjson.Get(trimmed, "item.type").String()
			switch itemType {
			case "agent_message":
				if txt := gjson.Get(trimmed, "item.text").String(); txt != "" {
					out.finalText = txt
				}
			case "command_execution":
				out.toolExecutions++
				status := gjson.Get(trimmed, "item.status").String()
				code := gjson.Get(trimmed, "item.exit_code").Int()
				if status == "failed" || code != 0 {
					out.errorMessages = append(out.errorMessages, fmt.Sprintf("command_execution status=%s exit=%d", status, code))
				}
			case "file_change":
				out.toolExecutions++
				status := gjson.Get(trimmed, "item.status").String()
				if status == "failed" {
					out.errorMessages = append(out.errorMessages, "file_change status=failed")
				}
			case "error":
				msg := gjson.Get(trimmed, "item.message").String()
				out.errorMessages = append(out.errorMessages, "item.error: "+msg)
			}
		case "turn.completed":
			out.completed = true
		case "turn.failed", "error":
			msg := gjson.Get(trimmed, "error.message").String()
			if msg == "" {
				msg = gjson.Get(trimmed, "message").String()
			}
			if msg == "" {
				msg = evType
			}
			out.errorMessages = append(out.errorMessages, msg)
		}
	}
	return out
}

// parseClaudeStreamJSONOutput extracts final result text, tool_result counts,
// completion status, and error flags from `claude --output-format stream-json`.
// Blank or non-JSON lines are skipped, and only a terminal result event with
// subtype "success" and is_error false marks completed true.
func parseClaudeStreamJSONOutput(stdout string) clientLabProcessResult {
	var out clientLabProcessResult
	for line := range strings.SplitSeq(stdout, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || trimmed[0] != '{' {
			continue
		}
		evType := gjson.Get(trimmed, "type").String()
		switch evType {
		case "assistant":
			if apiErr := gjson.Get(trimmed, "error").String(); apiErr != "" {
				out.errorMessages = append(out.errorMessages, "assistant.error: "+apiErr)
			}
			gjson.Get(trimmed, "message.content").ForEach(func(_, blk gjson.Result) bool {
				if blk.Get("type").String() == "text" {
					if txt := blk.Get("text").String(); txt != "" {
						out.finalText = txt
					}
				}
				return true
			})
		case "user":
			gjson.Get(trimmed, "message.content").ForEach(func(_, blk gjson.Result) bool {
				if blk.Get("type").String() == "tool_result" {
					out.toolExecutions++
					if blk.Get("is_error").Bool() {
						out.errorMessages = append(out.errorMessages, "tool_result is_error: "+blk.Get("content").String())
					}
				}
				return true
			})
		case "result":
			subtype := gjson.Get(trimmed, "subtype").String()
			isErr := gjson.Get(trimmed, "is_error").Bool()
			resTxt := gjson.Get(trimmed, "result").String()
			if resTxt != "" {
				out.finalText = resTxt
			}
			if !isErr && subtype == "success" {
				out.completed = true
			} else {
				out.errorMessages = append(out.errorMessages, fmt.Sprintf("result subtype=%s is_error=%v result=%s", subtype, isErr, resTxt))
			}
		}
	}
	return out
}

// parsePiJSONOutput extracts final assistant text, tool_execution_end counts,
// agent_settled status, and assistant/retry errors from `pi --mode json`.
// Because pi can exit with status 0 after a stream failure, error or aborted
// stopReason values on assistant messages record explicit errorMessages entries.
func parsePiJSONOutput(stdout string) clientLabProcessResult {
	var out clientLabProcessResult
	for line := range strings.SplitSeq(stdout, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || trimmed[0] != '{' {
			continue
		}
		evType := gjson.Get(trimmed, "type").String()
		switch evType {
		case "tool_execution_end":
			out.toolExecutions++
			if gjson.Get(trimmed, "isError").Bool() {
				out.errorMessages = append(out.errorMessages, "pi tool_execution_end isError=true")
			}
		case "message_end":
			if gjson.Get(trimmed, "message.role").String() != "assistant" {
				continue
			}
			stopReason := gjson.Get(trimmed, "message.stopReason").String()
			errMsg := gjson.Get(trimmed, "message.errorMessage").String()
			if stopReason == "error" || stopReason == "aborted" || errMsg != "" {
				out.errorMessages = append(out.errorMessages, fmt.Sprintf("stopReason=%s error=%s", stopReason, errMsg))
			}
			var textParts strings.Builder
			gjson.Get(trimmed, "message.content").ForEach(func(_, blk gjson.Result) bool {
				if blk.Get("type").String() == "text" {
					_, _ = textParts.WriteString(blk.Get("text").String())
				}
				return true
			})
			if textParts.Len() > 0 {
				out.finalText = textParts.String()
			}
		case "auto_retry_end":
			if !gjson.Get(trimmed, "success").Bool() {
				out.errorMessages = append(out.errorMessages, "auto_retry_end: "+gjson.Get(trimmed, "finalError").String())
			}
		case "agent_settled":
			if gjson.Get(trimmed, "aborted").Bool() {
				out.errorMessages = append(out.errorMessages, "agent_settled aborted=true")
			} else {
				out.completed = true
			}
		}
	}
	return out
}

// ServeHTTP buffers incoming request bytes for optional _incoming capture,
// restores r.Body for downstream Gin handlers, and records HTTP status codes.
// Successful HTTP 200 generation requests also persist raw client capture
// bundles when g.captureDir is non-empty.
func (g *clientLabGatewayRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var rawBody []byte
	if r.Body != nil {
		rawBody, _ = io.ReadAll(r.Body)
		_ = r.Body.Close()
		r.Body = io.NopCloser(bytes.NewReader(rawBody))
	}
	rawHeaders := r.Header.Clone()
	_, hasGenEndpoint := mockupstream.IdentifyEndpoint(r.URL.Path, r.URL.RawQuery)
	isGen := hasGenEndpoint && r.Method == http.MethodPost

	sw := &statusCapturingResponseWriter{ResponseWriter: w, status: http.StatusOK}
	g.next.ServeHTTP(sw, r)

	// Record the exchange and claim a monotonic turn number under g.mu before
	// releasing the lock so disk I/O does not block concurrent HTTP requests.
	g.mu.Lock()
	g.exchanges = append(g.exchanges, gatewayObservedExchange{
		method:       r.Method,
		path:         r.URL.Path,
		status:       sw.status,
		isGeneration: isGen,
	})
	turn := 0
	if g.captureDir != "" && isGen && sw.status == http.StatusOK && len(rawBody) > 0 {
		g.captureTurn++
		turn = g.captureTurn
	}
	g.mu.Unlock()

	if turn > 0 {
		if err := g.writeIngressCaptureBundle(turn, rawHeaders, rawBody); err != nil {
			// Retain disk capture failures under g.mu so caller assertions fail
			// explicitly instead of silently missing recorded corpus turns.
			g.mu.Lock()
			g.captureErrors = append(g.captureErrors, err.Error())
			g.mu.Unlock()
		}
	}
}

// summary acquires g.mu and returns the HTTP 5xx count, HTTP 200 generation
// turn count, and an independent copy of recorded capture errors. Callers can
// inspect or mutate captureErrs without racing later ServeHTTP calls.
func (g *clientLabGatewayRecorder) summary() (gateway5xx int, generation200 int, captureErrs []string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, ex := range g.exchanges {
		if ex.status >= 500 {
			gateway5xx++
		}
		if ex.isGeneration && ex.status == http.StatusOK {
			generation200++
		}
	}
	return gateway5xx, generation200, slices.Clone(g.captureErrors)
}

// writeIngressCaptureBundle writes one IncomingCapture JSON file containing the
// untouched client request body and raw HTTP headers into g.captureDir. Any
// JSON marshaling, directory creation, or file write failure returns an error.
func (g *clientLabGatewayRecorder) writeIngressCaptureBundle(turn int, headers http.Header, body []byte) error {
	flatHeaders := make(map[string]string, len(headers))
	for k, vals := range headers {
		if len(vals) > 0 {
			flatHeaders[k] = strings.Join(vals, ", ")
		}
	}
	bundle := testkit.IncomingCapture{
		Meta:        g.baseMeta,
		RawHeaders:  flatHeaders,
		Turn:        turn,
		RequestBody: jsontext.Value(bytes.Clone(body)),
	}
	encoded, err := testkit.MarshalDeterministicIndent(bundle)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(g.captureDir, 0o755); err != nil {
		return err
	}
	fileName := fmt.Sprintf("%s_%s_%03d.capture.json", g.baseMeta.Client, g.baseMeta.Scenario, turn)
	return os.WriteFile(filepath.Join(g.captureDir, fileName), encoded, 0o600)
}

// WriteHeader records statusCode on the first call before delegating to the
// underlying http.ResponseWriter. Subsequent calls leave the recorded status
// unchanged while still forwarding the call to the wrapped writer.
func (w *statusCapturingResponseWriter) WriteHeader(statusCode int) {
	if !w.wroteHeader {
		w.status = statusCode
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(statusCode)
}

// Write marks HTTP 200 committed when WriteHeader has not been called yet and
// delegates byte writing to the underlying http.ResponseWriter. Later writes
// preserve the already committed status code without overwriting it.
func (w *statusCapturingResponseWriter) Write(p []byte) (int, error) {
	if !w.wroteHeader {
		w.status = http.StatusOK
		w.wroteHeader = true
	}
	return w.ResponseWriter.Write(p)
}

// Flush forwards incremental SSE flushes to the underlying http.ResponseWriter
// whenever it implements http.Flusher. Writers that do not implement
// http.Flusher retain their existing buffering behavior without error.
func (w *statusCapturingResponseWriter) Flush() {
	if fl, ok := w.ResponseWriter.(http.Flusher); ok {
		fl.Flush()
	}
}

// Hijack delegates raw TCP socket hijacking to w.ResponseWriter through
// http.NewResponseController for WebSocket upgrades and simulated mid-stream
// disconnects. Underlying response writers that do not support hijacking
// return http.ErrNotSupported without altering recorded status state.
func (w *statusCapturingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

// hasTurn2ToolResultRequest reports whether reqs contains at least one HTTP 200
// turn carrying one or more historical tool results. Requests served by
// Script.Default or rejected before 200 completion do not satisfy Turn 2.
func hasTurn2ToolResultRequest(reqs []mockupstream.RecordedRequest) bool {
	for _, rec := range reqs {
		if !rec.UsedDefault && rec.Summary.ToolResults >= 1 && rec.ResponseStatus == http.StatusOK {
			return true
		}
	}
	return false
}

// truncateForDiag trims leading and trailing whitespace from s and bounds its
// byte length at maxBytes with an appended ellipsis. Inputs already within
// maxBytes return the trimmed string unchanged.
func truncateForDiag(s string, maxBytes int) string {
	trimmed := strings.TrimSpace(s)
	// Cap diagnostic snippets so verbose CLI logs stay readable in failure output.
	if len(trimmed) <= maxBytes {
		return trimmed
	}
	return trimmed[:maxBytes] + "..."
}

// summarizeLiveProxyExchanges formats recorded live proxy exchanges as compact
// path-and-status tokens without including upstream base URLs, headers, or
// credentials. Non-2xx error bodies pass through testkit.SanitizeSSEPayload
// before truncation so failure diagnostics never leak provider secrets.
func summarizeLiveProxyExchanges(reqs []mockupstream.RecordedRequest) string {
	if len(reqs) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(reqs))
	for i, rec := range reqs {
		token := fmt.Sprintf("#%d:%s->%d(asst=%d,tr=%d)", i+1, rec.Path, rec.ResponseStatus, rec.Summary.AssistantMessages, rec.Summary.ToolResults)
		if rec.Summary.ToolResults == 0 && rec.Protocol != "" && len(rec.Body) > 0 {
			if _, inspErr := mockupstream.InspectRequest(mockupstream.Endpoint{Protocol: rec.Protocol}, rec.Body); inspErr != nil {
				token += fmt.Sprintf("[inspectErr=%s]", truncateForDiag(inspErr.Error(), 80))
			}
		}
		if rec.ResponseStatus >= http.StatusBadRequest && len(rec.ResponseBody) > 0 {
			scrubbed, err := testkit.SanitizeSSEPayload(rec.ResponseBody)
			if err == nil {
				token += "(" + truncateForDiag(strings.ReplaceAll(string(scrubbed), "\n", " "), 160) + ")"
			}
		}
		parts = append(parts, token)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// stage2ClientLabExtendedRoutes returns all 18 Stage 2 client-to-upstream
// matrix cells (6 native controls and 12 cross-protocol conversion routes)
// from buildClientLabRoutes(). Gemini client and Gemini upstream cells are
// covered separately by TestClientLabIRStage3GeminiExtendedScenarios.
func stage2ClientLabExtendedRoutes() []clientLabRoute {
	var out []clientLabRoute
	for _, r := range buildClientLabRoutes() {
		if r.profile.clientProtocol == protocol.ProtocolGeminiGenerateContent {
			continue
		}
		if !isStage2TextUpstream(r.provider.upstreamProtocol) {
			continue
		}
		out = append(out, r)
	}
	return out
}

// isStage2TextUpstream reports whether proto belongs to Stage 2's three
// upstream text protocol families (Messages, Responses, and Chat Completions).
func isStage2TextUpstream(proto protocol.ProtocolID) bool {
	switch proto {
	case protocol.ProtocolAnthropicMessages,
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions:
		return true
	default:
		return false
	}
}

// stage3ClientLabGeminiRoutes returns all seven Gemini GenerateContent T6
// cells (six cross-protocol conversion routes plus the pi_gemini_via_gemini
// native control) from buildClientLabRoutes() in deterministic profile order.
// Each cell targets toggle-capable gemini-2.5-flash so non-reasoning CLI runs
// configured with effort="none" or MAX_THINKING_TOKENS=0 encode thinkingBudget=0
// without mutating protocolLabProviders' default Gemini model.
func stage3ClientLabGeminiRoutes() []clientLabRoute {
	var out []clientLabRoute
	for _, r := range buildClientLabRoutes() {
		if r.provider.upstreamProtocol == protocol.ProtocolGeminiGenerateContent {
			r.provider.upstreamModel = "gemini-2.5-flash"
			out = append(out, r)
		}
	}
	return out
}

// stage2AlternateSwitchProvider picks a distinct Stage 2 labProviderKindSpec
// whose upstream protocol differs from turn1Proto so native control cells in
// lab_switch_provider switch from protocol A to protocol B on Turn 2.
// Unsupported protocol identifiers panic immediately.
func stage2AlternateSwitchProvider(turn1Proto protocol.ProtocolID) labProviderKindSpec {
	switch turn1Proto {
	case protocol.ProtocolOpenAIResponses:
		return findLabProviderKind("anthropic")
	case protocol.ProtocolAnthropicMessages, protocol.ProtocolOpenAIChatCompletions:
		return findLabProviderKind("openai")
	default:
		panic("unsupported Stage 2 upstream protocol: " + string(turn1Proto))
	}
}

// findClientLabProfile looks up the static clientLabProfile entry matching slug
// in clientLabProfiles. Passing an unregistered profile slug panics immediately
// because every supported T6 CLI profile is statically declared in this file.
func findClientLabProfile(slug string) clientLabProfile {
	for _, p := range clientLabProfiles {
		if p.slug == slug {
			return p
		}
	}
	panic("unknown clientLabProfile: " + slug)
}

// makeCustomClientLabRoute pairs profileSlug with provSpec to construct a
// targeted Stage 2 or Stage 3 T6 route value. The returned clientLabRoute sets
// isNative true whenever the client wire protocol matches provSpec.upstreamProtocol.
func makeCustomClientLabRoute(profileSlug string, provSpec labProviderKindSpec) clientLabRoute {
	prof := findClientLabProfile(profileSlug)
	return clientLabRoute{
		slug:     prof.slug + "_via_" + provSpec.kind,
		profile:  prof,
		provider: provSpec,
		isNative: prof.clientProtocol == provSpec.upstreamProtocol,
	}
}

// runExtendedParallelToolCell executes Section 5.7's lab_tool_parallel scenario
// on route. Turn 1 streams two parallel shell tool
// calls writing markers to part_a.txt and part_b.txt, and Turn 2 verifies both
// files, zero gateway 5xx responses, and single usage settlement per request.
func runExtendedParallelToolCell(t *testing.T, labRoot string, route clientLabRoute) {
	t.Helper()
	token := generateClientLabMarker(t, clientLabModeScripted, route.slug, clientLabScenarioToolParallel)
	doneToken := "PAR_DONE_" + token

	argsA, err := json.Marshal(map[string]string{
		route.profile.shellCommandField: fmt.Sprintf("printf '%%s' '%s_a' > part_a.txt", token),
	})
	require.NoError(t, err)
	argsB, err := json.Marshal(map[string]string{
		route.profile.shellCommandField: fmt.Sprintf("printf '%%s' '%s_b' > part_b.txt", token),
	})
	require.NoError(t, err)

	upProto := route.provider.upstreamProtocol
	upModel := route.provider.upstreamModel
	turn1SSE := buildUpstreamParallelToolCallsSSE(
		upProto,
		upModel,
		"call_par_1",
		"call_par_2",
		route.profile.shellToolName,
		string(argsA),
		string(argsB),
	)
	turn2SSE := buildUpstreamTextSSE(upProto, upModel, "lab_par_2", "Parallel complete: "+doneToken)
	bgSSE := buildUpstreamTextSSE(upProto, upModel, "lab_par_bg", "Background acknowledgment")

	zeroResults := 0
	twoResults := 2
	hasTools := true
	script := testkit.Script{
		Name: "clientlab_tool_parallel",
		Turns: []testkit.Turn{
			{
				Match: testkit.Match{ToolResults: &zeroResults, HasTools: &hasTools},
				Reply: testkit.Reply{Raw: turn1SSE},
			},
			{
				Match: testkit.Match{ToolResults: &twoResults},
				Reply: testkit.Reply{Raw: turn2SSE},
			},
		},
		Default: testkit.Reply{Raw: bgSSE},
	}

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     script,
		Validation: route.provider.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, route.provider)},
	})
	gwRec := &clientLabGatewayRecorder{next: lab}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	inv := prepareClientLabInvocation(
		t,
		labRoot,
		route.profile,
		gwSrv.URL,
		"Execute both shell commands requested by the assistant and then confirm.",
		false,
	)
	res := runClientLabSubprocess(t.Context(), inv, clientLabScriptedTimeout)

	gw5xx, gen200, _ := gwRec.summary()
	require.Empty(t, mockH.Violations(), "upstream violations on %s: %+v", route.slug, mockH.Violations())
	require.Zero(t, gw5xx)
	require.Zero(t, res.exitCode, "stderr: %s stdout: %s", res.stderr, res.stdout)
	require.True(t, res.completed, "stdout: %s", res.stdout)
	require.Empty(t, res.errorMessages, "client errors: %v (upstreamReqs=%d stderr=%s stdout=%s)", res.errorMessages, len(mockH.Requests()), res.stderr, res.stdout)
	require.Equal(t, 2, res.toolExecutions)
	require.Contains(t, res.finalText, doneToken)

	gotA, err := os.ReadFile(filepath.Join(inv.workDir, "part_a.txt"))
	require.NoError(t, err)
	require.Equal(t, token+"_a", strings.TrimSpace(string(gotA)))
	gotB, err := os.ReadFile(filepath.Join(inv.workDir, "part_b.txt"))
	require.NoError(t, err)
	require.Equal(t, token+"_b", strings.TrimSpace(string(gotB)))

	require.GreaterOrEqual(t, gen200, 2)
	require.Equal(t, gen200, len(lab.UsageLogs()))
}

// runExtendedReasoningToolCell executes Section 5.7's lab_reasoning_tool
// scenario on route. Turn 1 streams reasoning with
// the upstream's continuity token plus a shell tool call, and Turn 2 enforces
// strict dialect validation, zero leaked trc1: carriers, and single usage
// settlement per request.
func runExtendedReasoningToolCell(t *testing.T, labRoot string, route clientLabRoute) {
	t.Helper()
	token := generateClientLabMarker(t, clientLabModeScripted, route.slug, clientLabScenarioReasoningTool)
	doneToken := "RSN_DONE_" + token

	argsBytes, err := json.Marshal(map[string]string{
		route.profile.shellCommandField: fmt.Sprintf("printf '%%s' '%s' > result.txt", token),
	})
	require.NoError(t, err)

	turn1SSE := buildUpstreamReasoningToolCallSSE(route.provider, "call_rsn_1", route.profile.shellToolName, string(argsBytes))
	turn2SSE := buildUpstreamTextSSE(route.provider.upstreamProtocol, route.provider.upstreamModel, "lab_rsn_2", "Reasoning verified: "+doneToken)
	bgSSE := buildUpstreamTextSSE(route.provider.upstreamProtocol, route.provider.upstreamModel, "lab_rsn_bg", "Background acknowledgment")

	zeroResults := 0
	oneResult := 1
	hasTools := true
	script := testkit.Script{
		Name: "clientlab_reasoning_tool",
		Turns: []testkit.Turn{
			{
				Match: testkit.Match{ToolResults: &zeroResults, HasTools: &hasTools},
				Reply: testkit.Reply{Raw: turn1SSE},
			},
			{
				Match: testkit.Match{ToolResults: &oneResult},
				Reply: testkit.Reply{Raw: turn2SSE},
			},
		},
		Default: testkit.Reply{Raw: bgSSE},
	}

	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script:     script,
		Validation: route.provider.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, route.provider)},
	})
	gwRec := &clientLabGatewayRecorder{next: lab}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	inv := prepareClientLabInvocation(
		t,
		labRoot,
		route.profile,
		gwSrv.URL,
		"Think carefully, run the requested shell command, and confirm completion.",
		true,
	)
	res := runClientLabSubprocess(t.Context(), inv, clientLabScriptedTimeout)

	gw5xx, gen200, _ := gwRec.summary()
	require.Empty(t, mockH.Violations(), "upstream dialect violations on %s: %+v", route.slug, mockH.Violations())
	for _, rec := range mockH.Requests() {
		require.False(t, hasUnstrippedCarrierInTokenPaths(route.provider.upstreamProtocol, rec.Body),
			"unstripped trc1: carrier leaked on %s", route.slug)
	}
	require.Zero(t, gw5xx)
	require.Zero(t, res.exitCode, "stderr: %s stdout: %s", res.stderr, res.stdout)
	require.True(t, res.completed, "stdout: %s", res.stdout)
	require.Empty(t, res.errorMessages, "client errors: %v (upstreamReqs=%d stderr=%s stdout=%s)", res.errorMessages, len(mockH.Requests()), res.stderr, res.stdout)
	require.Equal(t, 1, res.toolExecutions)
	require.Contains(t, res.finalText, doneToken)

	gotFile, err := os.ReadFile(filepath.Join(inv.workDir, "result.txt"))
	require.NoError(t, err)
	require.Equal(t, token, strings.TrimSpace(string(gotFile)))
	require.True(t, hasTurn2ToolResultRequest(mockH.Requests()))
	if route.provider.upstreamProtocol == protocol.ProtocolGeminiGenerateContent {
		sawRecalledCallSignature := false
		for _, rec := range mockH.Requests() {
			if !rec.UsedDefault && rec.Summary.ToolResults >= 1 && rec.ResponseStatus == http.StatusOK {
				bodyStr := string(rec.Body)
				require.NotContains(t, bodyStr, "skip_thought_signature_validator",
					"Gemini Turn 2 must replay real CallMemory signature instead of placeholder on %s", route.slug)
				if strings.Contains(bodyStr, "CpEBCgQIAxgC") {
					sawRecalledCallSignature = true
				}
			}
		}
		require.True(t, sawRecalledCallSignature, "Gemini Turn 2 did not carry Turn 1 CallMemory signature CpEBCgQIAxgC on %s", route.slug)
	}
	require.GreaterOrEqual(t, gen200, 2)
	require.Equal(t, gen200, len(lab.UsageLogs()))
}

// runExtendedSwitchProviderCell executes Section 5.7's lab_switch_provider
// scenario across prof, turn1Spec, and turn2Spec. Turn 1 emits reasoning plus a
// tool call on turn1Spec, and Turn 2 switches the group's provider to turn2Spec
// before verifying dialect compliance, carrier isolation, and single usage
// settlement per request.
func runExtendedSwitchProviderCell(
	t *testing.T,
	labRoot string,
	prof clientLabProfile,
	turn1Spec labProviderKindSpec,
	turn2Spec labProviderKindSpec,
) {
	t.Helper()
	slug := prof.slug + "_" + turn1Spec.kind + "_to_" + turn2Spec.kind
	token := generateClientLabMarker(t, clientLabModeScripted, slug, clientLabScenarioSwitchProvider)
	doneToken := "SW_DONE_" + token

	argsBytes, err := json.Marshal(map[string]string{
		prof.shellCommandField: fmt.Sprintf("printf '%%s' '%s' > result.txt", token),
	})
	require.NoError(t, err)

	turn1SSE := buildUpstreamReasoningToolCallSSE(turn1Spec, "call_sw_1", prof.shellToolName, string(argsBytes))
	turn1Bg := buildUpstreamTextSSE(turn1Spec.upstreamProtocol, turn1Spec.upstreamModel, "sw_bg1", "Background")
	turn2SSE := buildUpstreamTextSSE(turn2Spec.upstreamProtocol, turn2Spec.upstreamModel, "sw_t2", "Switched provider ok: "+doneToken)
	turn2Bg := buildUpstreamTextSSE(turn2Spec.upstreamProtocol, turn2Spec.upstreamModel, "sw_bg2", "Background")

	zeroResults := 0
	oneResult := 1
	hasTools := true
	firstH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "switch_turn1",
			Turns: []testkit.Turn{{
				Match: testkit.Match{ToolResults: &zeroResults, HasTools: &hasTools},
				Reply: testkit.Reply{Raw: turn1SSE},
			}},
			Default: testkit.Reply{Raw: turn1Bg},
		},
		Validation: turn1Spec.validation,
	})
	secondH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "switch_turn2",
			Turns: []testkit.Turn{{
				Match: testkit.Match{ToolResults: &oneResult},
				Reply: testkit.Reply{Raw: turn2SSE},
			}},
			Default: testkit.Reply{Raw: turn2Bg},
		},
		Validation: turn2Spec.validation,
	})
	firstSrv := httptest.NewServer(firstH)
	defer firstSrv.Close()
	secondSrv := httptest.NewServer(secondH)
	defer secondSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, firstSrv.URL, turn1Spec)},
	})

	var switched sync.Once
	switcher := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.Body != nil {
			if _, isGen := mockupstream.IdentifyEndpoint(r.URL.Path, r.URL.RawQuery); isGen {
				rawBody, _ := io.ReadAll(r.Body)
				_ = r.Body.Close()
				r.Body = io.NopCloser(bytes.NewReader(rawBody))
				if clientRequestCarriesToolResult(prof.clientProtocol, rawBody) {
					switched.Do(func() {
						lab.SetProviders(t, []labProvider{makeLabProviderSpec(202, 0, secondSrv.URL, turn2Spec)})
					})
				}
			}
		}
		lab.ServeHTTP(w, r)
	})

	gwRec := &clientLabGatewayRecorder{next: switcher}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	inv := prepareClientLabInvocation(
		t,
		labRoot,
		prof,
		gwSrv.URL,
		"Run the requested shell command and confirm completion.",
		true,
	)
	res := runClientLabSubprocess(t.Context(), inv, clientLabScriptedTimeout)

	gw5xx, gen200, _ := gwRec.summary()
	require.Empty(t, firstH.Violations(), "Turn 1 violations on %s: %+v", slug, firstH.Violations())
	require.Empty(t, secondH.Violations(), "Turn 2 violations on %s: %+v", slug, secondH.Violations())
	require.NotEmpty(t, firstH.Requests())
	require.True(t, hasTurn2ToolResultRequest(secondH.Requests()), "Provider B did not serve Turn 2 on %s", slug)
	for _, rec := range secondH.Requests() {
		require.False(t, hasUnstrippedCarrierInTokenPaths(turn2Spec.upstreamProtocol, rec.Body),
			"unstripped trc1: carrier leaked on Provider B (%s)", slug)
		require.False(t, hasLeakedForeignReplayToken(turn2Spec, rec.Body),
			"foreign replay token leaked on Provider B (%s)", slug)
	}
	require.Zero(t, gw5xx)
	require.Zero(t, res.exitCode, "stderr: %s stdout: %s", res.stderr, res.stdout)
	require.True(t, res.completed, "stdout: %s", res.stdout)
	require.Empty(t, res.errorMessages, "client errors: %v", res.errorMessages)
	require.Contains(t, res.finalText, doneToken)

	gotFile, err := os.ReadFile(filepath.Join(inv.workDir, "result.txt"))
	require.NoError(t, err)
	require.Equal(t, token, strings.TrimSpace(string(gotFile)))
	require.GreaterOrEqual(t, gen200, 2)
	require.Equal(t, gen200, len(lab.UsageLogs()))
}

// runExtendedFailoverCell executes Section 5.7's lab_failover scenario on route
// with two configured providers. Provider 1 returns HTTP 529 before emitting
// semantic bytes, and the gateway fails over transparently to Provider 2 with
// zero gateway 5xx responses and one settled usage log per HTTP 200 generation.
func runExtendedFailoverCell(t *testing.T, labRoot string, route clientLabRoute) {
	t.Helper()
	token := generateClientLabMarker(t, clientLabModeScripted, route.slug, clientLabScenarioFailover)
	upProto := route.provider.upstreamProtocol
	upModel := route.provider.upstreamModel

	primaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "clientlab_failover_primary",
			Default: testkit.Reply{Raw: formatUpstreamJSONError(upProto, 529, "primary overloaded")},
			Turns: []testkit.Turn{{
				Fault: &testkit.Fault{
					Status:    529,
					ErrorBody: formatUpstreamJSONError(upProto, 529, "primary overloaded"),
				},
			}},
		},
		Validation: route.provider.validation,
	})
	secondarySSE := buildUpstreamTextSSE(upProto, upModel, "lab_fo_2", "Failover marker: "+token)
	secondaryH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "clientlab_failover_secondary",
			Default: testkit.Reply{Raw: secondarySSE},
		},
		Validation: route.provider.validation,
	})
	p1Srv := httptest.NewServer(primaryH)
	defer p1Srv.Close()
	p2Srv := httptest.NewServer(secondaryH)
	defer p2Srv.Close()

	p1Spec := makeLabProviderSpec(101, 0, p1Srv.URL, route.provider)
	if route.provider.platform == capability.PlatformGemini {
		// Configure a 529 temporary-unschedulable rule on Google Gemini primary so
		// checkErrorPolicyInLoop exits internal 15s exponential backoff on attempt 1
		// and hands immediate pre-byte failover to Provider 2.
		if p1Spec.Credentials == nil {
			p1Spec.Credentials = make(map[string]any)
		}
		p1Spec.Credentials["temp_unschedulable_enabled"] = true
		p1Spec.Credentials["temp_unschedulable_rules"] = []any{
			map[string]any{
				"error_code":       529,
				"keywords":         []any{"overloaded"},
				"duration_minutes": 5,
			},
		}
	}
	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{
			p1Spec,
			makeLabProviderSpec(102, 1, p2Srv.URL, route.provider),
		},
		MaxProviderSwitches: 2,
	})
	gwRec := &clientLabGatewayRecorder{next: lab}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	inv := prepareClientLabInvocation(t, labRoot, route.profile, gwSrv.URL, "Reply briefly to acknowledge this ping.", false)
	res := runClientLabSubprocess(t.Context(), inv, clientLabScriptedTimeout)

	gw5xx, gen200, _ := gwRec.summary()
	require.Empty(t, primaryH.Violations())
	require.Empty(t, secondaryH.Violations())
	require.NotEmpty(t, primaryH.Requests(), "primary provider was never contacted")
	require.NotEmpty(t, secondaryH.Requests(), "secondary provider was never contacted")
	require.Zero(t, gw5xx)
	require.Zero(t, res.exitCode, "stderr: %s stdout: %s", res.stderr, res.stdout)
	require.True(t, res.completed)
	require.Empty(t, res.errorMessages)
	require.Contains(t, res.finalText, token)
	require.GreaterOrEqual(t, gen200, 1)
	logs := lab.UsageLogs()
	require.Len(t, logs, gen200)
	for _, lg := range logs {
		require.Equal(t, int64(102), lg.ProviderID)
	}
}

// runExtendedTruncatedCell executes Section 5.7's lab_truncated scenario on
// route by truncating the first main generation stream mid-output while pairing
// an identical-Match twin Turn and Default recovery reply for client retries.
// The CLI must either report an error or complete via its own retry policy
// without hanging, gateway 5xx responses, or duplicate per-request usage logs.
func runExtendedTruncatedCell(t *testing.T, labRoot string, route clientLabRoute) {
	t.Helper()
	token := generateClientLabMarker(t, clientLabModeScripted, route.slug, clientLabScenarioTruncated)
	rawStream, cutAfter := partialStreamWithEarlyUsage(route.provider.upstreamProtocol)
	recoveryReply := testkit.Reply{
		Response: &ir.Response{
			ID:     "resp_trunc_recover",
			Model:  route.provider.upstreamModel,
			Finish: ir.Finish{Reason: ir.FinishStop},
			Parts:  []ir.Part{{Kind: ir.PartText, Text: "Recovered after truncation: " + token}},
			Usage:  &ir.Usage{Input: 20, Output: 6},
		},
	}

	zeroResults := 0
	hasTools := true
	mainMatch := testkit.Match{ToolResults: &zeroResults, HasTools: &hasTools}
	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "clientlab_truncated",
			Turns: []testkit.Turn{
				{
					Match: mainMatch,
					Reply: testkit.Reply{Raw: rawStream},
					Fault: &testkit.Fault{TruncateAfterFrames: &cutAfter},
				},
				{
					Match: mainMatch,
					Reply: recoveryReply,
				},
			},
			Default: recoveryReply,
		},
		Validation: route.provider.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, route.provider)},
	})
	gwRec := &clientLabGatewayRecorder{next: lab}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	inv := prepareClientLabInvocation(t, labRoot, route.profile, gwSrv.URL, "Stream a detailed reply.", false)
	res := runClientLabSubprocess(t.Context(), inv, 20*time.Second)

	gw5xx, gen200, _ := gwRec.summary()
	require.Empty(t, mockH.Violations())
	require.Zero(t, gw5xx, "gateway must not return 5xx on mid-stream truncation")
	require.GreaterOrEqual(t, gen200, 1)

	hitTruncatedTurn := false
	for _, rec := range mockH.Requests() {
		if rec.MatchedTurn == 0 {
			hitTruncatedTurn = true
			break
		}
	}
	require.True(t, hitTruncatedTurn, "main client turn never exercised truncated upstream stream on %s", route.slug)

	require.NotEqual(t, -1, res.exitCode, "CLI hung until context timeout on truncated stream: stderr=%s", res.stderr)
	reportedFailure := res.exitCode != 0 || len(res.errorMessages) > 0 || !res.completed
	retriedCleanly := gen200 >= 2 && res.exitCode == 0 && res.completed && strings.Contains(res.finalText, token)
	require.True(t, reportedFailure || retriedCleanly,
		"CLI neither reported stream error nor retried to clean completion on %s: %+v", route.slug, res)

	if route.isNative {
		require.LessOrEqual(t, len(lab.UsageLogs()), gen200,
			"native route settled usage more than once per generation request on %s", route.slug)
	} else {
		require.Equal(t, gen200, len(lab.UsageLogs()),
			"converted route must settle usage once per generation request on %s", route.slug)
	}
}

// runExtendedCustomToolCell executes Section 5.7's lab_custom_tool scenario on
// Codex CLI. Turn 1 lowers apply_patch to a JSON
// function tool on Chat, Messages, Gemini GenerateContent, and function-only
// Responses upstreams and restores a freeform custom_tool_call so Codex patches
// target.txt before completing Turn 2.
func runExtendedCustomToolCell(t *testing.T, labRoot string, route clientLabRoute) {
	t.Helper()
	runStage5CodexCustomToolCell(t, labRoot, route, clientLabModeScripted, "", false)
}

// stage5OpenAIAPIKeyPassthroughLabSpec returns an OpenAI API Key Responses
// provider specification with openai_passthrough enabled and strict function-only
// request validation for Stage 5 client-tool adaptation tests.
func stage5OpenAIAPIKeyPassthroughLabSpec() labProviderKindSpec {
	return labProviderKindSpec{
		kind:             "openai_apikey_passthrough",
		platform:         capability.PlatformOpenAI,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIResponses,
		upstreamModel:    "gpt-5.4",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
		extra:            map[string]any{"openai_passthrough": true},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		},
		failoverStatus: 529,
	}
}

// stage5DeepSeekResponsesLabSpec returns a DeepSeek API Key compatible Responses
// provider specification with strict function-only request validation for Stage 5
// prelude client-tool lowering and response restoration tests.
func stage5DeepSeekResponsesLabSpec() labProviderKindSpec {
	return labProviderKindSpec{
		kind:             "deepseek_responses",
		platform:         capability.PlatformDeepseek,
		providerType:     capability.ProviderTypeAPIKey,
		upstreamProtocol: protocol.ProtocolOpenAIResponses,
		upstreamModel:    "deepseek-chat",
		enabledProtocols: []protocol.ProtocolID{protocol.ProtocolOpenAIResponses},
		validation: testkit.RequestValidationOptions{
			DialectRules: []string{"function-only"},
		},
		failoverStatus: 529,
	}
}

// stage5FunctionOnlyResponsesProviders returns all four Stage 5 HTTP Responses
// provider specifications that require same-protocol client-tool lowering.
func stage5FunctionOnlyResponsesProviders() []labProviderKindSpec {
	grokSpecs := grokLabProviderSpecs()
	return []labProviderKindSpec{
		stage5OpenAIAPIKeyPassthroughLabSpec(),
		grokSpecs[0],
		grokSpecs[1],
		stage5DeepSeekResponsesLabSpec(),
	}
}

// runStage5CodexCustomToolCell drives one Codex CLI apply_patch exchange over
// either HTTP Responses or the Responses WebSocket HTTP bridge. It asserts
// clean subprocess completion, zero gateway 5xx responses, zero mockupstream
// validation violations, patched file contents, Turn 2 tool-output replay, and
// exact UsageLog settlement parity.
func runStage5CodexCustomToolCell(
	t *testing.T,
	labRoot string,
	route clientLabRoute,
	mode clientLabMode,
	captureDir string,
	useWebSocket bool,
) {
	t.Helper()
	token := generateClientLabMarker(t, mode, route.slug, clientLabScenarioCustomTool)
	doneToken := "PATCH_DONE_" + token
	patchBody := fmt.Sprintf("*** Begin Patch\n*** Update File: target.txt\n@@\n-placeholder\n+%s\n*** End Patch\n", token)

	turn1SSE := buildUpstreamCustomPatchSSEForProvider(route.provider, "call_patch_1", patchBody)
	turn2SSE := buildUpstreamTextSSE(route.provider.upstreamProtocol, route.provider.upstreamModel, "lab_patch_2", "Applied patch: "+doneToken)
	bgSSE := buildUpstreamTextSSE(route.provider.upstreamProtocol, route.provider.upstreamModel, "lab_patch_bg", "Background")

	zeroResults := 0
	oneResult := 1
	// Both HTTP turns and WebSocket HTTP bridge continuations after local
	// generate=false warmups carry lowered tool declarations on the outbound HTTP request.
	hasTools := true
	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "clientlab_custom_tool",
			Turns: []testkit.Turn{
				{
					Match: testkit.Match{ToolResults: &zeroResults, HasTools: &hasTools},
					Reply: testkit.Reply{Raw: turn1SSE},
				},
				{
					Match: testkit.Match{ToolResults: &oneResult},
					Reply: testkit.Reply{Raw: turn2SSE},
				},
			},
			Default: testkit.Reply{Raw: bgSSE},
		},
		Validation: route.provider.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	var allowed []protocol.ProtocolID
	if useWebSocket {
		allowed = []protocol.ProtocolID{
			protocol.ProtocolResponsesWebSocket,
			protocol.ProtocolOpenAIResponses,
		}
	}
	lab := newProtocolLabRouter(t, labOptions{
		Providers:        []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, route.provider)},
		AllowedProtocols: allowed,
	})
	gwRec := &clientLabGatewayRecorder{
		next:       lab,
		captureDir: captureDir,
		baseMeta: testkit.CorpusMeta{
			Protocol:   route.profile.clientProtocol,
			Client:     route.profile.corpusClient,
			Version:    route.profile.version,
			Model:      route.profile.clientModel,
			CapturedAt: clientLabCaptureDate,
			Stream:     true,
			Scenario:   clientLabScenarioCustomTool + route.profile.corpusScenarioSuffix,
		},
	}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	inv := prepareClientLabInvocation(
		t,
		labRoot,
		route.profile,
		gwSrv.URL,
		"Use apply_patch to update target.txt as instructed and confirm.",
		false,
	)
	if useWebSocket {
		// Override supports_websockets via CLI -c flag so the pinned template
		// remains unchanged for standard HTTP Responses client laboratory runs.
		inv.args = slices.Insert(inv.args, 1, "-c", "model_providers.lab.supports_websockets=true")
	}
	require.NoError(t, os.WriteFile(filepath.Join(inv.workDir, "target.txt"), []byte("placeholder\n"), 0o600))

	res := runClientLabSubprocess(t.Context(), inv, clientLabScriptedTimeout)

	gw5xx, gen200, capErrs := gwRec.summary()
	require.Empty(t, capErrs, "ingress capture errors: %v", capErrs)
	require.Empty(t, mockH.Violations(), "upstream violations on %s: %+v", route.slug, mockH.Violations())
	require.Zero(t, gw5xx)
	require.Zero(t, res.exitCode, "stderr: %s stdout: %s", res.stderr, res.stdout)
	require.True(t, res.completed, "stdout: %s", res.stdout)
	require.Empty(t, res.errorMessages, "client errors: %v (upstreamReqs=%d stderr=%s stdout=%s)", res.errorMessages, len(mockH.Requests()), res.stderr, res.stdout)
	require.Contains(t, res.finalText, doneToken)

	patchedBytes, err := os.ReadFile(filepath.Join(inv.workDir, "target.txt"))
	require.NoError(t, err)
	require.Equal(t, token, strings.TrimSpace(string(patchedBytes)))
	require.True(t, hasTurn2ToolResultRequest(mockH.Requests()))

	settledCount := len(lab.UsageLogs())
	if useWebSocket {
		// WebSocket upgrades return HTTP 101 once at ingress while each real
		// upstream HTTP bridge turn settles exactly one UsageLog entry (warmup=0).
		require.Equal(t, len(mockH.Requests()), settledCount)
		require.GreaterOrEqual(t, settledCount, 2)
	} else {
		require.GreaterOrEqual(t, gen200, 2)
		require.Equal(t, gen200, settledCount)
	}
}

// runExtendedLongStreamCell executes Section 5.7's lab_long_stream scenario on
// route. Turn 1 streams a 2 KiB shell payload
// fragmented across 16 argument deltas and Turn 2 streams 32 text delta chunks,
// verifying intact disk output and single usage settlement per request.
func runExtendedLongStreamCell(t *testing.T, labRoot string, route clientLabRoute) {
	t.Helper()
	token := generateClientLabMarker(t, clientLabModeScripted, route.slug, clientLabScenarioLongStream)
	doneToken := "LONG_DONE_" + token
	longPrefix := strings.Repeat("LONGSEG_", 256)
	expectedFilePayload := longPrefix + token

	argsBytes, err := json.Marshal(map[string]string{
		route.profile.shellCommandField: fmt.Sprintf("printf '%%s' '%s' > large.txt", expectedFilePayload),
	})
	require.NoError(t, err)

	var textChunks []string
	for i := range 32 {
		textChunks = append(textChunks, fmt.Sprintf("BLOCK_%02d_%s ", i, strings.Repeat("x", 96)))
	}
	textChunks = append(textChunks, doneToken)

	upProto := route.provider.upstreamProtocol
	upModel := route.provider.upstreamModel
	turn1SSE := buildUpstreamFragmentedToolCallSSE(upProto, upModel, "call_long_1", route.profile.shellToolName, string(argsBytes), 16)
	turn2SSE := buildUpstreamFragmentedTextSSE(upProto, upModel, "lab_long_2", textChunks)
	bgSSE := buildUpstreamTextSSE(upProto, upModel, "lab_long_bg", "Background")

	zeroResults := 0
	oneResult := 1
	hasTools := true
	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name: "clientlab_long_stream",
			Turns: []testkit.Turn{
				{
					Match: testkit.Match{ToolResults: &zeroResults, HasTools: &hasTools},
					Reply: testkit.Reply{Raw: turn1SSE},
				},
				{
					Match: testkit.Match{ToolResults: &oneResult},
					Reply: testkit.Reply{Raw: turn2SSE},
				},
			},
			Default: testkit.Reply{Raw: bgSSE},
		},
		Validation: route.provider.validation,
	})
	upSrv := httptest.NewServer(mockH)
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(101, 0, upSrv.URL, route.provider)},
	})
	gwRec := &clientLabGatewayRecorder{next: lab}
	gwSrv := httptest.NewServer(gwRec)
	defer gwSrv.Close()

	inv := prepareClientLabInvocation(
		t,
		labRoot,
		route.profile,
		gwSrv.URL,
		"Run the shell command with its full payload and then return the complete reply.",
		false,
	)
	res := runClientLabSubprocess(t.Context(), inv, clientLabScriptedTimeout)

	gw5xx, gen200, _ := gwRec.summary()
	require.Empty(t, mockH.Violations(), "upstream violations on %s: %+v", route.slug, mockH.Violations())
	require.Zero(t, gw5xx)
	require.Zero(t, res.exitCode, "stderr: %s stdout: %s", res.stderr, res.stdout)
	require.True(t, res.completed, "stdout: %s", res.stdout)
	require.Empty(t, res.errorMessages, "client errors: %v (upstreamReqs=%d stderr=%s stdout=%s)", res.errorMessages, len(mockH.Requests()), res.stderr, res.stdout)
	require.Contains(t, res.finalText, "BLOCK_00_")
	require.Contains(t, res.finalText, "BLOCK_31_")
	require.Contains(t, res.finalText, doneToken)

	largeBytes, err := os.ReadFile(filepath.Join(inv.workDir, "large.txt"))
	require.NoError(t, err)
	require.Equal(t, expectedFilePayload, string(largeBytes))
	require.GreaterOrEqual(t, gen200, 2)
	require.Equal(t, gen200, len(lab.UsageLogs()))
}

// clientRequestCarriesToolResult reports whether rawBody in clientProto carries
// at least one historical tool result item. Unrecognized protocol identifiers or
// malformed JSON payloads return false so provider switching waits until Turn 2.
func clientRequestCarriesToolResult(clientProto protocol.ProtocolID, rawBody []byte) bool {
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		found := false
		gjson.GetBytes(rawBody, "messages").ForEach(func(_, msg gjson.Result) bool {
			msg.Get("content").ForEach(func(_, blk gjson.Result) bool {
				if blk.Get("type").String() == "tool_result" {
					found = true
					return false
				}
				return true
			})
			return !found
		})
		return found
	case protocol.ProtocolOpenAIResponses:
		found := false
		gjson.GetBytes(rawBody, "input").ForEach(func(_, item gjson.Result) bool {
			switch item.Get("type").String() {
			case "function_call_output", "custom_tool_call_output", "tool_search_output", "tool_search_call_output",
				"local_shell_call_output", "shell_call_output", "computer_call_output", "mcp_tool_call_output":
				found = true
				return false
			}
			return true
		})
		return found
	case protocol.ProtocolOpenAIChatCompletions:
		found := false
		gjson.GetBytes(rawBody, "messages").ForEach(func(_, msg gjson.Result) bool {
			if msg.Get("role").String() == "tool" {
				found = true
				return false
			}
			return true
		})
		return found
	case protocol.ProtocolGeminiGenerateContent:
		found := false
		gjson.GetBytes(rawBody, "contents").ForEach(func(_, turn gjson.Result) bool {
			turn.Get("parts").ForEach(func(_, part gjson.Result) bool {
				if part.Get("functionResponse").IsObject() {
					found = true
					return false
				}
				return true
			})
			return !found
		})
		return found
	default:
		return false
	}
}

// buildUpstreamParallelToolCallsSSE synthesizes a single streaming assistant
// turn containing two parallel tool calls in upProto's native SSE wire format.
// Unsupported upstream protocol identifiers panic immediately instead of
// returning malformed frames.
func buildUpstreamParallelToolCallsSSE(
	upProto protocol.ProtocolID,
	upModel, callID1, callID2, toolName, argsJSON1, argsJSON2 string,
) string {
	args1Q, _ := json.Marshal(argsJSON1)
	args2Q, _ := json.Marshal(argsJSON2)
	modelJSON, _ := json.Marshal(upModel)
	nameJSON, _ := json.Marshal(toolName)

	switch upProto {
	case protocol.ProtocolAnthropicMessages:
		return strings.Join([]string{
			"event: message_start",
			fmt.Sprintf(`data: {"type":"message_start","message":{"id":"msg_par_1","type":"message","role":"assistant","model":%s,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":52,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}`, modelJSON),
			"",
			"event: content_block_start",
			fmt.Sprintf(`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":%q,"name":%s,"input":{}}}`, callID1, nameJSON),
			"",
			"event: content_block_delta",
			fmt.Sprintf(`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":%s}}`, args1Q),
			"",
			"event: content_block_stop",
			`data: {"type":"content_block_stop","index":0}`,
			"",
			"event: content_block_start",
			fmt.Sprintf(`data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":%q,"name":%s,"input":{}}}`, callID2, nameJSON),
			"",
			"event: content_block_delta",
			fmt.Sprintf(`data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":%s}}`, args2Q),
			"",
			"event: content_block_stop",
			`data: {"type":"content_block_stop","index":1}`,
			"",
			"event: message_delta",
			`data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":24}}`,
			"",
			"event: message_stop",
			`data: {"type":"message_stop"}`,
			"",
			"",
		}, "\n")

	case protocol.ProtocolOpenAIResponses:
		fc1 := "fc_" + callID1
		fc2 := "fc_" + callID2
		return strings.Join([]string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_par_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_par_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.output_item.added",
			fmt.Sprintf(`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":%q,"type":"function_call","status":"in_progress","call_id":%q,"name":%s,"arguments":""}}`, fc1, callID1, nameJSON),
			"",
			"event: response.function_call_arguments.delta",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.delta","sequence_number":3,"item_id":%q,"output_index":0,"delta":%s}`, fc1, args1Q),
			"",
			"event: response.function_call_arguments.done",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.done","sequence_number":4,"item_id":%q,"output_index":0,"arguments":%s}`, fc1, args1Q),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}}`, fc1, callID1, nameJSON, args1Q),
			"",
			"event: response.output_item.added",
			fmt.Sprintf(`data: {"type":"response.output_item.added","sequence_number":6,"output_index":1,"item":{"id":%q,"type":"function_call","status":"in_progress","call_id":%q,"name":%s,"arguments":""}}`, fc2, callID2, nameJSON),
			"",
			"event: response.function_call_arguments.delta",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.delta","sequence_number":7,"item_id":%q,"output_index":1,"delta":%s}`, fc2, args2Q),
			"",
			"event: response.function_call_arguments.done",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.done","sequence_number":8,"item_id":%q,"output_index":1,"arguments":%s}`, fc2, args2Q),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":9,"output_index":1,"item":{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}}`, fc2, callID2, nameJSON, args2Q),
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":10,"response":{"id":"resp_par_1","object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s},{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}],"usage":{"input_tokens":52,"output_tokens":24,"total_tokens":76,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON, fc1, callID1, nameJSON, args1Q, fc2, callID2, nameJSON, args2Q),
			"",
			"",
		}, "\n")

	case protocol.ProtocolOpenAIChatCompletions:
		return strings.Join([]string{
			fmt.Sprintf(`data: {"id":"chatcmpl_par_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":%q,"type":"function","function":{"name":%s,"arguments":%s}},{"index":1,"id":%q,"type":"function","function":{"name":%s,"arguments":%s}}]},"finish_reason":null}]}`, modelJSON, callID1, nameJSON, args1Q, callID2, nameJSON, args2Q),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_par_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`, modelJSON),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_par_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[],"usage":{"prompt_tokens":52,"completion_tokens":24,"total_tokens":76,"prompt_tokens_details":{"cached_tokens":0},"completion_tokens_details":{"reasoning_tokens":0}}}`, modelJSON),
			"",
			"data: [DONE]",
			"",
			"",
		}, "\n")

	case protocol.ProtocolGeminiGenerateContent:
		// Only the first functionCall in a parallel Gemini step carries a base64
		// thoughtSignature per commit 4b05d79a5 while the second remains unsigned.
		return fmt.Sprintf(
			"data: {\"responseId\":\"gem_par_1\",\"modelVersion\":%s,\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"id\":%q,\"name\":%s,\"args\":%s},\"thoughtSignature\":\"CpEBCgIIAxgB\"},{\"functionCall\":{\"id\":%q,\"name\":%s,\"args\":%s}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":52,\"candidatesTokenCount\":24,\"totalTokenCount\":76}}\n\n",
			modelJSON,
			callID1,
			nameJSON,
			argsJSON1,
			callID2,
			nameJSON,
			argsJSON2,
		)

	default:
		panic("unsupported upstream protocol: " + string(upProto))
	}
}

// buildUpstreamReasoningToolCallSSE synthesizes a streaming reasoning and tool
// call SSE payload carrying the continuity token required by provSpec's
// upstream dialect. Unsupported upstream protocol identifiers panic immediately
// instead of returning empty text.
func buildUpstreamReasoningToolCallSSE(provSpec labProviderKindSpec, callID, toolName, argsJSON string) string {
	argsQ, _ := json.Marshal(argsJSON)
	modelJSON, _ := json.Marshal(provSpec.upstreamModel)
	nameJSON, _ := json.Marshal(toolName)
	thought := "Inspecting workspace before invoking tool."
	thoughtQ, _ := json.Marshal(thought)

	switch provSpec.upstreamProtocol {
	case protocol.ProtocolAnthropicMessages:
		isPassback := slices.Contains(provSpec.validation.DialectRules, "passback")
		lines := []string{
			"event: message_start",
			fmt.Sprintf(`data: {"type":"message_start","message":{"id":"msg_rsn_1","type":"message","role":"assistant","model":%s,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":56,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}`, modelJSON),
			"",
			"event: content_block_start",
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`,
			"",
			"event: content_block_delta",
			fmt.Sprintf(`data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":%s}}`, thoughtQ),
			"",
		}
		if !isPassback {
			lines = append(lines,
				"event: content_block_delta",
				`data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"EqQBLabReasoningSig1"}}`,
				"",
			)
		}
		lines = append(lines,
			"event: content_block_stop",
			`data: {"type":"content_block_stop","index":0}`,
			"",
			"event: content_block_start",
			fmt.Sprintf(`data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":%q,"name":%s,"input":{}}}`, callID, nameJSON),
			"",
			"event: content_block_delta",
			fmt.Sprintf(`data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":%s}}`, argsQ),
			"",
			"event: content_block_stop",
			`data: {"type":"content_block_stop","index":1}`,
			"",
			"event: message_delta",
			`data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":22}}`,
			"",
			"event: message_stop",
			`data: {"type":"message_stop"}`,
			"",
			"",
		)
		return strings.Join(lines, "\n")

	case protocol.ProtocolOpenAIResponses:
		encToken := "gAAAA_lab_reason_1"
		if provSpec.platform == capability.PlatformGrok {
			encToken = "xai_lab_reason_1"
		}
		fcID := "fc_" + callID
		return strings.Join([]string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_rsn_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_rsn_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.output_item.added",
			`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":"rs_lab_1","type":"reasoning","summary":[]}}`,
			"",
			"event: response.reasoning_summary_part.added",
			`data: {"type":"response.reasoning_summary_part.added","sequence_number":3,"item_id":"rs_lab_1","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":""}}`,
			"",
			"event: response.reasoning_summary_text.delta",
			fmt.Sprintf(`data: {"type":"response.reasoning_summary_text.delta","sequence_number":4,"item_id":"rs_lab_1","output_index":0,"summary_index":0,"delta":%s}`, thoughtQ),
			"",
			"event: response.reasoning_summary_text.done",
			fmt.Sprintf(`data: {"type":"response.reasoning_summary_text.done","sequence_number":5,"item_id":"rs_lab_1","output_index":0,"summary_index":0,"text":%s}`, thoughtQ),
			"",
			"event: response.reasoning_summary_part.done",
			fmt.Sprintf(`data: {"type":"response.reasoning_summary_part.done","sequence_number":6,"item_id":"rs_lab_1","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":%s}}`, thoughtQ),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":7,"output_index":0,"item":{"id":"rs_lab_1","type":"reasoning","summary":[{"type":"summary_text","text":%s}],"encrypted_content":%q}}`, thoughtQ, encToken),
			"",
			"event: response.output_item.added",
			fmt.Sprintf(`data: {"type":"response.output_item.added","sequence_number":8,"output_index":1,"item":{"id":%q,"type":"function_call","status":"in_progress","call_id":%q,"name":%s,"arguments":""}}`, fcID, callID, nameJSON),
			"",
			"event: response.function_call_arguments.delta",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.delta","sequence_number":9,"item_id":%q,"output_index":1,"delta":%s}`, fcID, argsQ),
			"",
			"event: response.function_call_arguments.done",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.done","sequence_number":10,"item_id":%q,"output_index":1,"arguments":%s}`, fcID, argsQ),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":11,"output_index":1,"item":{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}}`, fcID, callID, nameJSON, argsQ),
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":12,"response":{"id":"resp_rsn_1","object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":"rs_lab_1","type":"reasoning","summary":[{"type":"summary_text","text":%s}],"encrypted_content":%q},{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}],"usage":{"input_tokens":56,"output_tokens":22,"total_tokens":78,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":8}}}}`, modelJSON, thoughtQ, encToken, fcID, callID, nameJSON, argsQ),
			"",
			"",
		}, "\n")

	case protocol.ProtocolOpenAIChatCompletions:
		return strings.Join([]string{
			fmt.Sprintf(`data: {"id":"chatcmpl_rsn_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":%s},"finish_reason":null}]}`, modelJSON, thoughtQ),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_rsn_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":%q,"type":"function","function":{"name":%s,"arguments":%s}}]},"finish_reason":null}]}`, modelJSON, callID, nameJSON, argsQ),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_rsn_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`, modelJSON),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_rsn_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[],"usage":{"prompt_tokens":56,"completion_tokens":22,"total_tokens":78,"prompt_tokens_details":{"cached_tokens":0},"completion_tokens_details":{"reasoning_tokens":8}}}`, modelJSON),
			"",
			"data: [DONE]",
			"",
			"",
		}, "\n")

	case protocol.ProtocolGeminiGenerateContent:
		// Use valid 12-character base64 CpEB prefixes so native pi_gemini retains
		// thoughtSignature while foreign-token leak detectors catch unstripped leaks.
		return strings.Join([]string{
			fmt.Sprintf(`data: {"responseId":"gem_rsn_1","modelVersion":%s,"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":%s,"thought":true,"thoughtSignature":"CpEBCgIIAxgB"}]}}]}`, modelJSON, thoughtQ),
			"",
			fmt.Sprintf(`data: {"responseId":"gem_rsn_1","modelVersion":%s,"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":%q,"name":%s,"args":%s},"thoughtSignature":"CpEBCgQIAxgC"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":56,"candidatesTokenCount":14,"thoughtsTokenCount":8,"totalTokenCount":78}}`, modelJSON, callID, nameJSON, argsJSON),
			"",
			"",
		}, "\n")

	default:
		panic("unsupported upstream protocol: " + string(provSpec.upstreamProtocol))
	}
}

// buildUpstreamCustomPatchSSEForProvider synthesizes a native custom_tool_call
// only when provSpec accepts full OpenAI Responses client tools natively, and
// emits a lowered {"input": patchText} function call on function-only Responses,
// Chat, Messages, and Gemini GenerateContent upstreams so gateway response
// restorers reconstruct apply_patch. Unsupported upstream protocol identifiers
// panic immediately in buildUpstreamCustomPatchSSE.
func buildUpstreamCustomPatchSSEForProvider(provSpec labProviderKindSpec, callID, patchText string) string {
	if provSpec.upstreamProtocol == protocol.ProtocolOpenAIResponses &&
		slices.Contains(provSpec.validation.DialectRules, "function-only") {
		wrappedArgs, _ := json.Marshal(map[string]string{"input": patchText})
		return buildUpstreamToolCallSSE(provSpec.upstreamProtocol, provSpec.upstreamModel, callID, "apply_patch", string(wrappedArgs))
	}
	return buildUpstreamCustomPatchSSE(provSpec.upstreamProtocol, provSpec.upstreamModel, callID, patchText)
}

// buildUpstreamCustomPatchSSE synthesizes a native custom_tool_call on OpenAI
// Responses and a lowered {"input": patchText} function, tool_use, or
// functionCall payload on Chat, Messages, and Gemini GenerateContent upstreams
// so convert.raiseTools reconstructs apply_patch. Unsupported upstream protocol
// identifiers panic immediately.
func buildUpstreamCustomPatchSSE(upProto protocol.ProtocolID, upModel, callID, patchText string) string {
	if upProto == protocol.ProtocolOpenAIResponses {
		patchQ, _ := json.Marshal(patchText)
		modelJSON, _ := json.Marshal(upModel)
		itemID := "ctc_" + callID
		return strings.Join([]string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":0,"response":{"id":"resp_patch_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":1,"response":{"id":"resp_patch_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, modelJSON),
			"",
			"event: response.output_item.added",
			fmt.Sprintf(`data: {"type":"response.output_item.added","sequence_number":2,"output_index":0,"item":{"id":%q,"type":"custom_tool_call","status":"in_progress","call_id":%q,"name":"apply_patch","input":""}}`, itemID, callID),
			"",
			"event: response.custom_tool_call_input.delta",
			fmt.Sprintf(`data: {"type":"response.custom_tool_call_input.delta","sequence_number":3,"item_id":%q,"output_index":0,"delta":%s}`, itemID, patchQ),
			"",
			"event: response.custom_tool_call_input.done",
			fmt.Sprintf(`data: {"type":"response.custom_tool_call_input.done","sequence_number":4,"item_id":%q,"output_index":0,"input":%s}`, itemID, patchQ),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":5,"output_index":0,"item":{"id":%q,"type":"custom_tool_call","status":"completed","call_id":%q,"name":"apply_patch","input":%s}}`, itemID, callID, patchQ),
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":6,"response":{"id":"resp_patch_1","object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":%q,"type":"custom_tool_call","status":"completed","call_id":%q,"name":"apply_patch","input":%s}],"usage":{"input_tokens":48,"output_tokens":18,"total_tokens":66,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, modelJSON, itemID, callID, patchQ),
			"",
			"",
		}, "\n")
	}

	wrappedArgs, _ := json.Marshal(map[string]string{"input": patchText})
	return buildUpstreamToolCallSSE(upProto, upModel, callID, "apply_patch", string(wrappedArgs))
}

// buildUpstreamFragmentedToolCallSSE splits argsJSON across numFragments SSE
// delta frames in upProto's wire format to exercise multi-chunk tool argument
// reassembly. Unsupported upstream protocol identifiers panic immediately
// instead of returning partial output.
func buildUpstreamFragmentedToolCallSSE(upProto protocol.ProtocolID, upModel, callID, toolName, argsJSON string, numFragments int) string {
	fragments := splitStringEvenly(argsJSON, numFragments)
	modelJSON, _ := json.Marshal(upModel)
	nameJSON, _ := json.Marshal(toolName)
	fullArgsQ, _ := json.Marshal(argsJSON)

	switch upProto {
	case protocol.ProtocolAnthropicMessages:
		lines := []string{
			"event: message_start",
			fmt.Sprintf(`data: {"type":"message_start","message":{"id":"msg_frag_1","type":"message","role":"assistant","model":%s,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":64,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}`, modelJSON),
			"",
			"event: content_block_start",
			fmt.Sprintf(`data: {"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":%q,"name":%s,"input":{}}}`, callID, nameJSON),
			"",
		}
		for _, frag := range fragments {
			fragQ, _ := json.Marshal(frag)
			lines = append(lines,
				"event: content_block_delta",
				fmt.Sprintf(`data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":%s}}`, fragQ),
				"",
			)
		}
		lines = append(lines,
			"event: content_block_stop",
			`data: {"type":"content_block_stop","index":0}`,
			"",
			"event: message_delta",
			`data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":64}}`,
			"",
			"event: message_stop",
			`data: {"type":"message_stop"}`,
			"",
			"",
		)
		return strings.Join(lines, "\n")

	case protocol.ProtocolOpenAIResponses:
		itemID := "fc_" + callID
		seq := 0
		lines := []string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":%d,"response":{"id":"resp_frag_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, seq, modelJSON),
			"",
		}
		seq++
		lines = append(lines,
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":%d,"response":{"id":"resp_frag_1","object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, seq, modelJSON),
			"",
		)
		seq++
		lines = append(lines,
			"event: response.output_item.added",
			fmt.Sprintf(`data: {"type":"response.output_item.added","sequence_number":%d,"output_index":0,"item":{"id":%q,"type":"function_call","status":"in_progress","call_id":%q,"name":%s,"arguments":""}}`, seq, itemID, callID, nameJSON),
			"",
		)
		seq++
		for _, frag := range fragments {
			fragQ, _ := json.Marshal(frag)
			lines = append(lines,
				"event: response.function_call_arguments.delta",
				fmt.Sprintf(`data: {"type":"response.function_call_arguments.delta","sequence_number":%d,"item_id":%q,"output_index":0,"delta":%s}`, seq, itemID, fragQ),
				"",
			)
			seq++
		}
		lines = append(lines,
			"event: response.function_call_arguments.done",
			fmt.Sprintf(`data: {"type":"response.function_call_arguments.done","sequence_number":%d,"item_id":%q,"output_index":0,"arguments":%s}`, seq, itemID, fullArgsQ),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":%d,"output_index":0,"item":{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}}`, seq+1, itemID, callID, nameJSON, fullArgsQ),
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":%d,"response":{"id":"resp_frag_1","object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":%q,"type":"function_call","status":"completed","call_id":%q,"name":%s,"arguments":%s}],"usage":{"input_tokens":64,"output_tokens":64,"total_tokens":128,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, seq+2, modelJSON, itemID, callID, nameJSON, fullArgsQ),
			"",
			"",
		)
		return strings.Join(lines, "\n")

	case protocol.ProtocolOpenAIChatCompletions:
		lines := []string{
			fmt.Sprintf(`data: {"id":"chatcmpl_frag_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":%q,"type":"function","function":{"name":%s,"arguments":""}}]},"finish_reason":null}]}`, modelJSON, callID, nameJSON),
			"",
		}
		for _, frag := range fragments {
			fragQ, _ := json.Marshal(frag)
			lines = append(lines,
				fmt.Sprintf(`data: {"id":"chatcmpl_frag_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":%s}}]},"finish_reason":null}]}`, modelJSON, fragQ),
				"",
			)
		}
		lines = append(lines,
			fmt.Sprintf(`data: {"id":"chatcmpl_frag_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`, modelJSON),
			"",
			fmt.Sprintf(`data: {"id":"chatcmpl_frag_1","object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[],"usage":{"prompt_tokens":64,"completion_tokens":64,"total_tokens":128,"prompt_tokens_details":{"cached_tokens":0},"completion_tokens_details":{"reasoning_tokens":0}}}`, modelJSON),
			"",
			"data: [DONE]",
			"",
			"",
		)
		return strings.Join(lines, "\n")

	case protocol.ProtocolGeminiGenerateContent:
		// Gemini represents functionCall.args as an atomic JSON object per Part, so
		// stream multi-chunk preamble text frames ahead of the 2 KiB functionCall frame.
		var lines []string
		for i := range min(numFragments, 8) {
			preQ, _ := json.Marshal(fmt.Sprintf("Preparing large command segment %02d. ", i))
			lines = append(lines,
				fmt.Sprintf(`data: {"responseId":"gem_frag_1","modelVersion":%s,"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":%s}]}}]}`, modelJSON, preQ),
				"",
			)
		}
		lines = append(lines,
			fmt.Sprintf(`data: {"responseId":"gem_frag_1","modelVersion":%s,"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":%q,"name":%s,"args":%s},"thoughtSignature":"CpEBCggIAxgE"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":64,"candidatesTokenCount":64,"totalTokenCount":128}}`, modelJSON, callID, nameJSON, argsJSON),
			"",
			"",
		)
		return strings.Join(lines, "\n")

	default:
		panic("unsupported upstream protocol: " + string(upProto))
	}
}

// buildUpstreamFragmentedTextSSE emits chunks as individual streaming text
// delta frames in upProto's native SSE wire format alongside terminal usage.
// Unsupported upstream protocol identifiers panic immediately instead of
// returning empty payloads.
func buildUpstreamFragmentedTextSSE(upProto protocol.ProtocolID, upModel, idSuffix string, chunks []string) string {
	fullText := strings.Join(chunks, "")
	fullQ, _ := json.Marshal(fullText)
	modelJSON, _ := json.Marshal(upModel)

	switch upProto {
	case protocol.ProtocolAnthropicMessages:
		lines := []string{
			"event: message_start",
			fmt.Sprintf(`data: {"type":"message_start","message":{"id":"msg_%s","type":"message","role":"assistant","model":%s,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":80,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}`, idSuffix, modelJSON),
			"",
			"event: content_block_start",
			`data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
			"",
		}
		for _, ch := range chunks {
			chQ, _ := json.Marshal(ch)
			lines = append(lines,
				"event: content_block_delta",
				fmt.Sprintf(`data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":%s}}`, chQ),
				"",
			)
		}
		lines = append(lines,
			"event: content_block_stop",
			`data: {"type":"content_block_stop","index":0}`,
			"",
			"event: message_delta",
			`data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":128}}`,
			"",
			"event: message_stop",
			`data: {"type":"message_stop"}`,
			"",
			"",
		)
		return strings.Join(lines, "\n")

	case protocol.ProtocolOpenAIResponses:
		msgID := "msg_" + idSuffix
		respID := "resp_" + idSuffix
		seq := 0
		lines := []string{
			"event: response.created",
			fmt.Sprintf(`data: {"type":"response.created","sequence_number":%d,"response":{"id":%q,"object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, seq, respID, modelJSON),
			"",
			"event: response.in_progress",
			fmt.Sprintf(`data: {"type":"response.in_progress","sequence_number":%d,"response":{"id":%q,"object":"response","created_at":1760000000,"status":"in_progress","model":%s,"output":[]}}`, seq+1, respID, modelJSON),
			"",
			"event: response.output_item.added",
			fmt.Sprintf(`data: {"type":"response.output_item.added","sequence_number":%d,"output_index":0,"item":{"id":%q,"type":"message","status":"in_progress","role":"assistant","content":[]}}`, seq+2, msgID),
			"",
			"event: response.content_part.added",
			fmt.Sprintf(`data: {"type":"response.content_part.added","sequence_number":%d,"item_id":%q,"output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"text":""}}`, seq+3, msgID),
			"",
		}
		seq = 4
		for _, ch := range chunks {
			chQ, _ := json.Marshal(ch)
			lines = append(lines,
				"event: response.output_text.delta",
				fmt.Sprintf(`data: {"type":"response.output_text.delta","sequence_number":%d,"item_id":%q,"output_index":0,"content_index":0,"delta":%s}`, seq, msgID, chQ),
				"",
			)
			seq++
		}
		lines = append(lines,
			"event: response.output_text.done",
			fmt.Sprintf(`data: {"type":"response.output_text.done","sequence_number":%d,"item_id":%q,"output_index":0,"content_index":0,"text":%s}`, seq, msgID, fullQ),
			"",
			"event: response.content_part.done",
			fmt.Sprintf(`data: {"type":"response.content_part.done","sequence_number":%d,"item_id":%q,"output_index":0,"content_index":0,"part":{"type":"output_text","annotations":[],"text":%s}}`, seq+1, msgID, fullQ),
			"",
			"event: response.output_item.done",
			fmt.Sprintf(`data: {"type":"response.output_item.done","sequence_number":%d,"output_index":0,"item":{"id":%q,"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","annotations":[],"text":%s}]}}`, seq+2, msgID, fullQ),
			"",
			"event: response.completed",
			fmt.Sprintf(`data: {"type":"response.completed","sequence_number":%d,"response":{"id":%q,"object":"response","created_at":1760000000,"status":"completed","model":%s,"output":[{"id":%q,"type":"message","status":"completed","role":"assistant","content":[{"type":"output_text","annotations":[],"text":%s}]}],"usage":{"input_tokens":80,"output_tokens":128,"total_tokens":208,"input_tokens_details":{"cached_tokens":0},"output_tokens_details":{"reasoning_tokens":0}}}}`, seq+3, respID, modelJSON, msgID, fullQ),
			"",
			"",
		)
		return strings.Join(lines, "\n")

	case protocol.ProtocolOpenAIChatCompletions:
		chatID := "chatcmpl_" + idSuffix
		var lines []string
		for i, ch := range chunks {
			chQ, _ := json.Marshal(ch)
			roleField := ""
			if i == 0 {
				roleField = `"role":"assistant",`
			}
			lines = append(lines,
				fmt.Sprintf(`data: {"id":%q,"object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{%s"content":%s},"finish_reason":null}]}`, chatID, modelJSON, roleField, chQ),
				"",
			)
		}
		lines = append(lines,
			fmt.Sprintf(`data: {"id":%q,"object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, chatID, modelJSON),
			"",
			fmt.Sprintf(`data: {"id":%q,"object":"chat.completion.chunk","created":1760000000,"model":%s,"choices":[],"usage":{"prompt_tokens":80,"completion_tokens":128,"total_tokens":208,"prompt_tokens_details":{"cached_tokens":0},"completion_tokens_details":{"reasoning_tokens":0}}}`, chatID, modelJSON),
			"",
			"data: [DONE]",
			"",
			"",
		)
		return strings.Join(lines, "\n")

	case protocol.ProtocolGeminiGenerateContent:
		respID := "gem_" + idSuffix
		var lines []string
		for i, ch := range chunks {
			chQ, _ := json.Marshal(ch)
			if i == len(chunks)-1 {
				// Attach complete usageMetadata to the terminal STOP frame because native
				// pi google-generative-ai overwrites output.usage from the latest chunk.
				lines = append(lines,
					fmt.Sprintf(`data: {"responseId":%q,"modelVersion":%s,"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":%s}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":80,"candidatesTokenCount":128,"totalTokenCount":208}}`, respID, modelJSON, chQ),
					"",
				)
				continue
			}
			lines = append(lines,
				fmt.Sprintf(`data: {"responseId":%q,"modelVersion":%s,"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":%s}]}}]}`, respID, modelJSON, chQ),
				"",
			)
		}
		lines = append(lines, "")
		return strings.Join(lines, "\n")

	default:
		panic("unsupported upstream protocol: " + string(upProto))
	}
}

// splitStringEvenly partitions s into at most parts non-empty substrings along
// ASCII byte boundaries when s is ASCII JSON. Non-positive or unit part counts
// and single-byte inputs return a one-element slice containing s.
func splitStringEvenly(s string, parts int) []string {
	if parts <= 1 || len(s) <= 1 {
		return []string{s}
	}
	step := max(len(s)/parts, 1)
	var out []string
	for offset := 0; offset < len(s); offset += step {
		if len(out) == parts-1 || offset+step >= len(s) {
			out = append(out, s[offset:])
			break
		}
		out = append(out, s[offset:offset+step])
	}
	return out
}
