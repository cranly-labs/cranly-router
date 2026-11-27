package app

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit/mockupstream"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
)

// stage7_verification_scenario_test.go verifies route-matrix execution and
// line-by-line test coverage across http_routes.go,
// gateway_openai_text_execution.go, gateway_response_output.go,
// gateway_messages_execution.go, gateway_google.go, gateway_grok_execution.go,
// gateway_openai_ws_http.go, qoder_runtime.go, gateway_count_tokens.go, and
// gateway_openai_tokens.go. Every active conversion route and off-table
// normalization entry in testdata/golden/routes.txt must run end to end through
// the IR conversion pipeline and stay bound to AST-verified test suites.
//
// TestProtocolLabStage7RouteMatrixComplete executes all 64 [route_table] entries
// and all 13 [off_table] mechanisms against in-process gateway routers,
// checking target request framing, client stream grammar, and usage or
// count_tokens accounting. TestStage7RoutesLineByLineCoverage parses all 77
// effective lines in routes.txt, verifies via Go AST inspection that every
// referenced T3, T5, and T6 test entry point exists in its package, and locks
// testdata/golden/stage7_routes_coverage.json.

const (
	// Stage 7 golden filename and T6 status labels anchor line-by-line route
	// coverage snapshots under testdata/golden/stage7_routes_coverage.json.
	stage7RoutesCoverageOut = "stage7_routes_coverage.json" // golden line-by-line T3/T5/T6 coverage manifest
	stage7T6Covered         = "covered"                     // exercised by real CLI subprocesses in scripted T6
	stage7T6Exempt          = "exempt"                      // exempted by non-CLI platform or auxiliary endpoint scope
)

// stage7ParsedRouteLine represents one effective non-comment line from
// testdata/golden/routes.txt with its 1-based file line number preserved.
type stage7ParsedRouteLine struct {
	LineNumber int      // 1-based line number in testdata/golden/routes.txt
	Section    string   // "[route_table]" or "[off_table]"
	RawLine    string   // exact tab-separated line text
	Columns    []string // six tab-separated column values
}

// stage7RouteCoverageEntry binds one line of testdata/golden/routes.txt to its
// AST-verified T3 law tests, T5 gateway tests, and T6 clientlab tests.
type stage7RouteCoverageEntry struct {
	LineNumber     int      `json:"line_number"`               // 1-based line number in testdata/golden/routes.txt
	Section        string   `json:"section"`                   // "[route_table]" or "[off_table]"
	RouteSpec      string   `json:"route_spec"`                // canonical tab-joined six-column route specification
	T3Tests        []string `json:"t3_tests"`                  // AST-verified "<pkg>:<TestFunc>" T3 law tests
	T5Tests        []string `json:"t5_tests"`                  // AST-verified "internal/app:<TestFunc>" T5 scenario tests
	T6Status       string   `json:"t6_status"`                 // "covered" or "exempt"
	T6Tests        []string `json:"t6_tests,omitzero"`         // AST-verified "internal/app:<TestFunc>" T6 scripted tests
	T6ExemptReason string   `json:"t6_exempt_reason,omitzero"` // concrete technical exemption reason when T6Status is "exempt"
}

// stage7RoutesCoverageReport is the deterministic JSON manifest written to
// testdata/golden/stage7_routes_coverage.json.
type stage7RoutesCoverageReport struct {
	TotalLines      int                        `json:"total_lines"`       // total effective lines in routes.txt (77)
	RouteTableLines int                        `json:"route_table_lines"` // count of [route_table] lines (64)
	OffTableLines   int                        `json:"off_table_lines"`   // count of [off_table] lines (13)
	T6CoveredLines  int                        `json:"t6_covered_lines"`  // count of lines with real CLI T6 coverage (63)
	T6ExemptLines   int                        `json:"t6_exempt_lines"`   // count of lines with documented T6 exemption (14)
	Entries         []stage7RouteCoverageEntry `json:"entries"`           // line-ordered coverage bindings
}

// stage7PlatformTypePair pairs one platform name with one provider_type name
// expanded from a semicolon-and-comma scope string in testdata/golden/routes.txt.
type stage7PlatformTypePair struct {
	platform     string // provider platform identifier
	providerType string // provider auth/deployment type identifier
}

// TestProtocolLabStage7RouteMatrixComplete executes real in-process gateway
// exchanges across all 64 [route_table] entries
// and all 13 [off_table] mechanisms (covering every platform and provider_type
// scope listed in testdata/golden/routes.txt). Every cell must complete with
// valid target request framing, valid client output grammar, and exact usage
// or count_tokens accounting.
func TestProtocolLabStage7RouteMatrixComplete(t *testing.T) {
	rawRoutes, err := os.ReadFile(filepath.Join("testdata", "golden", "routes.txt"))
	require.NoError(t, err)
	routeRows, offRows, err := parseConversionRoutesGolden(string(rawRoutes))
	require.NoError(t, err)
	require.Len(t, routeRows, 64)
	require.Len(t, offRows, 13)

	t.Run("route_table_all_64_rows", func(t *testing.T) {
		for idx, row := range routeRows {
			subName := fmt.Sprintf("%02d_%s_to_%s_%s_%s", idx+1, row.ClientProtocol, row.Platform, row.ProviderType, row.UpstreamProtocol)
			t.Run(subName, func(t *testing.T) {
				runStage7RouteTableGatewayCell(t, row)
			})
		}
	})

	t.Run("off_table_all_13_rows_and_scopes", func(t *testing.T) {
		runStage7OffTableClientToolLoweringScopes(t, offRows)
		runStage7OffTableCursorInputScopes(t, offRows)
		runStage7OffTableCountTokensAndEstimationScopes(t, offRows)
		runStage7OffTableLegacyResponsesIngressScopes(t, offRows)
	})
}

// TestStage7RoutesLineByLineCoverage parses every effective line of
// testdata/golden/routes.txt and binds all 77 lines to AST-verified T3 law
// tests, T5 gateway scenario tests, and T6 scripted clientlab tests. Each
// route-table line's T5 and T6 test lists derive directly from the exact route
// constructors of those suites, and the verified manifest must match
// testdata/golden/stage7_routes_coverage.json.
func TestStage7RoutesLineByLineCoverage(t *testing.T) {
	backendRoot := resolveStage7BackendRoot(t)
	rawBytes, err := os.ReadFile(filepath.Join(backendRoot, "internal", "app", "testdata", "golden", "routes.txt"))
	require.NoError(t, err)

	routeRows, _, err := parseConversionRoutesGolden(string(rawBytes))
	require.NoError(t, err)
	parsedLines := parseStage7RoutesWithLineNumbers(t, string(rawBytes))
	require.Len(t, parsedLines, 77)

	testPackages := map[string]map[string]bool{
		"internal/protocol/convert":      scanPackageTopLevelDecls(t, filepath.Join(backendRoot, "internal", "protocol", "convert"), true),
		"internal/gateway/provider":      scanPackageTopLevelDecls(t, filepath.Join(backendRoot, "internal", "gateway", "provider"), true),
		"internal/gateway/tokenestimate": scanPackageTopLevelDecls(t, filepath.Join(backendRoot, "internal", "gateway", "tokenestimate"), true),
		"internal/app":                   scanPackageTopLevelDecls(t, filepath.Join(backendRoot, "internal", "app"), true),
	}

	assertTestRefsExist := func(lineNo int, tier string, refs []string) {
		require.NotEmpty(t, refs, "routes.txt line %d has empty %s test bindings", lineNo, tier)
		for _, ref := range refs {
			pkgPath, fnName, ok := strings.Cut(ref, ":")
			require.True(t, ok, "malformed test ref %q on line %d", ref, lineNo)
			decls, pkgLoaded := testPackages[pkgPath]
			require.True(t, pkgLoaded, "unindexed test package %q in ref %q on line %d", pkgPath, ref, lineNo)
			require.True(t, strings.HasPrefix(fnName, "Test"), "test ref %q on line %d must name a Test* entry point", ref, lineNo)
			require.True(t, decls[fnName], "referenced %s test %s does not exist on routes.txt line %d", tier, ref, lineNo)
		}
	}

	t5RouteIndex := buildStage7T5RouteIndex(routeRows)
	t6RouteIndex := buildStage7ScriptedT6RouteIndex()

	var (
		entries        = make([]stage7RouteCoverageEntry, 0, len(parsedLines))
		routeLineCount int
		offLineCount   int
		t6CoveredCount int
		t6ExemptCount  int
	)
	for _, pl := range parsedLines {
		cov := buildStage7CoverageForLine(t, pl, t5RouteIndex, t6RouteIndex)
		assertTestRefsExist(pl.LineNumber, "T3", cov.T3Tests)
		assertTestRefsExist(pl.LineNumber, "T5", cov.T5Tests)
		switch cov.T6Status {
		case stage7T6Covered:
			t6CoveredCount++
			assertTestRefsExist(pl.LineNumber, "T6", cov.T6Tests)
			require.Empty(t, cov.T6ExemptReason, "covered T6 line %d must not carry exemption reason", pl.LineNumber)
		case stage7T6Exempt:
			t6ExemptCount++
			require.Empty(t, cov.T6Tests, "exempt T6 line %d must not list T6Tests", pl.LineNumber)
			require.NotEmpty(t, cov.T6ExemptReason, "exempt T6 line %d must document concrete reason", pl.LineNumber)
		default:
			t.Fatalf("unexpected T6Status %q on line %d", cov.T6Status, pl.LineNumber)
		}

		if pl.Section == routesSectionRouteTable {
			routeLineCount++
			routeKey := formatStage7RouteKey(
				protocol.ProtocolID(pl.Columns[0]),
				pl.Columns[1],
				pl.Columns[2],
				protocol.ProtocolID(pl.Columns[3]),
			)
			require.Equal(t, t5RouteIndex[routeKey], cov.T5Tests, "T5 index mismatch on line %d (%s)", pl.LineNumber, routeKey)
			if cov.T6Status == stage7T6Covered {
				require.Equal(t, t6RouteIndex[routeKey], cov.T6Tests, "T6 index mismatch on line %d (%s)", pl.LineNumber, routeKey)
			}
		} else {
			offLineCount++
		}
		entries = append(entries, cov)
	}

	require.Equal(t, 64, routeLineCount)
	require.Equal(t, 13, offLineCount)
	require.Equal(t, 63, t6CoveredCount, "58 non-Antigravity/non-Qoder [route_table] rows + 5 Stage 5 [off_table] rows must have T6 coverage")
	require.Equal(t, 14, t6ExemptCount, "3 Antigravity + 3 Qoder + 8 non-standalone-CLI [off_table] rows are exempt")

	report := stage7RoutesCoverageReport{
		TotalLines:      len(entries),
		RouteTableLines: routeLineCount,
		OffTableLines:   offLineCount,
		T6CoveredLines:  t6CoveredCount,
		T6ExemptLines:   t6ExemptCount,
		Entries:         entries,
	}
	testkit.GoldenJSON(t, stage7RoutesCoverageOut, report)
}

// resolveStage7BackendRoot locates the repository's backend/ directory from this
// test file's runtime caller path. Missing runtime caller metadata or absent
// protocol/convert directories fail tb immediately before source scanning begins.
func resolveStage7BackendRoot(tb testing.TB) string {
	tb.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(tb, ok)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
	require.DirExists(tb, filepath.Join(root, "internal", "protocol", "convert"))
	return root
}

// scanPackageTopLevelDecls parses Go files directly inside dir and returns the
// set of top-level function, method, type, const, and var names. When testsOnly
// is true only *_test.go files are scanned; otherwise only non-test .go files are scanned.
func scanPackageTopLevelDecls(tb testing.TB, dir string, testsOnly bool) map[string]bool {
	tb.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(tb, err)

	fset := token.NewFileSet()
	decls := make(map[string]bool)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") {
			continue
		}
		isTest := strings.HasSuffix(entry.Name(), "_test.go")
		if isTest != testsOnly {
			continue
		}
		f, parseErr := parser.ParseFile(fset, filepath.Join(dir, entry.Name()), nil, 0)
		require.NoError(tb, parseErr)
		for _, d := range f.Decls {
			switch node := d.(type) {
			case *ast.FuncDecl:
				decls[node.Name.Name] = true
			case *ast.GenDecl:
				for _, sp := range node.Specs {
					switch spec := sp.(type) {
					case *ast.TypeSpec:
						decls[spec.Name.Name] = true
					case *ast.ValueSpec:
						for _, n := range spec.Names {
							decls[n.Name] = true
						}
					}
				}
			}
		}
	}
	return decls
}

// runStage7RouteTableGatewayCell executes one streaming generation turn across
// newProtocolLabRouter for a single [route_table]
// row from testdata/golden/routes.txt. It asserts HTTP 200 status, zero strict
// upstream request violations, valid client SSE stream grammar, and one settled UsageLog.
func runStage7RouteTableGatewayCell(t *testing.T, row conversionRouteEntry) {
	t.Helper()
	clientSpec := stage7ClientSpecForProtocol(row.ClientProtocol)
	reqBody := stage7SampleClientStreamRequest(row.ClientProtocol, clientSpec.defaultModel, "Stage 7 ping")

	if row.Platform == capability.PlatformQoder {
		qoderUp := &protocolLabQoderUpstream{
			status: http.StatusOK,
			replySSE: wrapStage6QoderSSE([]string{
				`{"choices":[{"delta":{"role":"assistant","content":"stage7-qoder-ok"}}]}`,
				`{"choices":[{"delta":{},"finish_reason":"stop"}]}`,
				`{"usage":{"prompt_tokens":14,"completion_tokens":4}}`,
			}, true),
		}
		qoderSrv := httptest.NewServer(qoderUp)
		defer qoderSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{{
				ID:           701,
				Platform:     capability.PlatformQoder,
				Type:         capability.ProviderTypeCosy,
				BaseURL:      qoderSrv.URL,
				Models:       []string{"auto"},
				ModelMapping: map[string]string{clientSpec.defaultModel: "auto"},
				Credentials: map[string]any{
					"access_token": "lab-qoder-token",
					"machine_id":   "lab-qoder-machine",
				},
			}},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status, _, respBytes := sendLabClientRequest(t, gwSrv, clientSpec, reqBody)
		require.Equal(t, http.StatusOK, status, string(respBytes))
		require.Empty(t, qoderUp.Violations())
		require.Contains(t, string(respBytes), "stage7-qoder-ok")
		frames, fErr := readAllSSEFrames(respBytes)
		require.NoError(t, fErr)
		require.Empty(t, testkit.ValidateStreamGrammar(row.ClientProtocol, frames, testkit.GrammarOptions{}))
		require.Len(t, qoderUp.Bodies(), 1)
		require.Len(t, lab.UsageLogs(), 1)
		return
	}

	provSpec := stage7ProviderSpecForRouteRow(row)
	replySSE := buildUpstreamTextSSE(row.UpstreamProtocol, provSpec.upstreamModel, "s7_1", "stage7-route-ok")
	mockH := mockupstream.NewWithConfig(mockupstream.Config{
		Script: testkit.Script{
			Name:    "stage7_route_cell",
			Default: testkit.Reply{Raw: replySSE},
		},
		Validation: provSpec.validation,
	})

	if row.Platform == capability.PlatformAntigravity {
		upSrv := newLabMockServer(provSpec, mockH)
		defer upSrv.Close()

		lab := newProtocolLabRouter(t, labOptions{
			Providers: []labProvider{makeLabProviderSpec(701, 0, upSrv.URL, provSpec)},
		})
		gwSrv := httptest.NewServer(lab)
		defer gwSrv.Close()

		status, _, respBytes := sendLabClientRequest(t, gwSrv, clientSpec, reqBody)
		require.Equal(t, http.StatusOK, status, string(respBytes))
		require.Empty(t, upSrv.Violations())
		require.Contains(t, string(respBytes), "stage7-route-ok")
		frames, fErr := readAllSSEFrames(respBytes)
		require.NoError(t, fErr)
		require.Empty(t, testkit.ValidateStreamGrammar(row.ClientProtocol, frames, testkit.GrammarOptions{}))
		require.Len(t, lab.UsageLogs(), 1)
		return
	}

	var cloudErrs []string
	var cloudMu sync.Mutex
	upSrv := newClientLabScriptedUpstreamServer(provSpec, mockH, func(code string) {
		cloudMu.Lock()
		cloudErrs = append(cloudErrs, code)
		cloudMu.Unlock()
	})
	defer upSrv.Close()

	lab := newProtocolLabRouter(t, labOptions{
		Providers: []labProvider{makeLabProviderSpec(701, 0, upSrv.URL, provSpec)},
	})
	gwSrv := httptest.NewServer(lab)
	defer gwSrv.Close()

	status, _, respBytes := sendLabClientRequest(t, gwSrv, clientSpec, reqBody)
	require.Equal(t, http.StatusOK, status, string(respBytes))
	cloudMu.Lock()
	gotCloudErrs := slices.Clone(cloudErrs)
	cloudMu.Unlock()
	require.Empty(t, gotCloudErrs)
	require.Empty(t, mockH.Violations(), "violations: %+v", mockH.Violations())
	require.Contains(t, string(respBytes), "stage7-route-ok")
	frames, fErr := readAllSSEFrames(respBytes)
	require.NoError(t, fErr)
	require.Empty(t, testkit.ValidateStreamGrammar(row.ClientProtocol, frames, testkit.GrammarOptions{}))
	require.Len(t, lab.UsageLogs(), 1)
}

// runStage7OffTableClientToolLoweringScopes exercises every HTTP and WebSocket
// scope listed on routes.txt [off_table] lines 75 through 79. Each scope sends an apply_patch custom tool request and asserts
// function-only lowering upstream, custom_tool_call restoration downstream, and
// single UsageLog settlement.
func runStage7OffTableClientToolLoweringScopes(t *testing.T, offRows []offTableConversionEntry) {
	t.Helper()
	respClient := stage7ClientSpecForProtocol(protocol.ProtocolOpenAIResponses)
	patchJSON, _ := json.Marshal(map[string]string{"input": "*** Begin Patch\n*** End Patch"})
	loweredSSE := buildUpstreamToolCallSSE(protocol.ProtocolOpenAIResponses, "gpt-5.4", "call_s7_ct", "apply_patch", string(patchJSON))

	for _, entry := range offRows {
		if entry.Category != offTableResponsesClientTools {
			continue
		}
		isWS := entry.ClientProtocol == protocol.ProtocolResponsesWebSocket
		for _, pair := range expandStage7PlatformTypeScopes(t, entry.Scope) {
			subName := fmt.Sprintf("client_tool_lowering_%s_%s_%s", entry.ClientProtocol, pair.platform, pair.providerType)
			t.Run(subName, func(t *testing.T) {
				provSpec := stage7ProviderSpecForRouteRow(conversionRouteEntry{
					ClientProtocol:   protocol.ProtocolOpenAIResponses,
					Platform:         pair.platform,
					ProviderType:     pair.providerType,
					UpstreamProtocol: protocol.ProtocolOpenAIResponses,
				})
				provSpec.validation.DialectRules = []string{"function-only"}
				if pair.platform == capability.PlatformOpenAI && pair.providerType == capability.ProviderTypeAPIKey {
					provSpec.extra = map[string]any{"openai_passthrough": true}
				}

				mockH := mockupstream.NewWithConfig(mockupstream.Config{
					Script: testkit.Script{
						Name:    "stage7_client_tool_lowering",
						Default: testkit.Reply{Raw: loweredSSE},
					},
					Validation: provSpec.validation,
				})
				upSrv := httptest.NewServer(mockH)
				defer upSrv.Close()

				var allowed []protocol.ProtocolID
				if isWS {
					allowed = []protocol.ProtocolID{
						protocol.ProtocolResponsesWebSocket,
						protocol.ProtocolOpenAIResponses,
					}
				}
				lab := newProtocolLabRouter(t, labOptions{
					Providers:        []labProvider{makeLabProviderSpec(705, 0, upSrv.URL, provSpec)},
					AllowedProtocols: allowed,
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				if !isWS {
					httpReq := []byte(`{"model":"gpt-5","stream":true,"tools":[{"type":"custom","name":"apply_patch"}],"input":"patch file"}`)
					status, _, respBytes := sendLabClientRequest(t, gwSrv, respClient, httpReq)
					require.Equal(t, http.StatusOK, status, string(respBytes))
					require.Empty(t, mockH.Violations(), "HTTP tool lowering violations: %+v", mockH.Violations())
					require.Contains(t, string(respBytes), `"custom_tool_call"`)
					require.Len(t, lab.UsageLogs(), 1)
					return
				}

				wsURL := "ws" + strings.TrimPrefix(gwSrv.URL, "http") + "/v1/responses"
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				conn, _, dErr := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
					HTTPHeader: http.Header{"Authorization": []string{"Bearer lab-api-key"}},
				})
				require.NoError(t, dErr)
				defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()

				wsCreate := `{"type":"response.create","model":"gpt-5","tools":[{"type":"custom","name":"apply_patch"}],"input":"patch ws"}`
				require.NoError(t, conn.Write(ctx, websocket.MessageText, []byte(wsCreate)))
				var sawCustomCall, sawCompleted bool
				for !sawCompleted {
					_, msgBytes, rErr := conn.Read(ctx)
					require.NoError(t, rErr)
					if strings.Contains(string(msgBytes), `"custom_tool_call"`) {
						sawCustomCall = true
					}
					if gjson.GetBytes(msgBytes, "type").String() == "response.completed" {
						sawCompleted = true
					}
				}
				_ = conn.Close(websocket.StatusNormalClosure, "")
				require.True(t, sawCustomCall)
				require.Empty(t, mockH.Violations(), "WS tool lowering violations: %+v", mockH.Violations())
				require.Eventually(t, func() bool {
					return len(lab.UsageLogs()) == 1
				}, time.Second, 5*time.Millisecond)
			})
		}
	}
}

// runStage7OffTableCursorInputScopes exercises every platform and provider_type
// scope listed on routes.txt [off_table] lines 80 through 82 by sending a
// Cursor input-shaped /v1/chat/completions request.
// Each scope asserts HTTP 200 Chat SSE output, zero target violations, and one UsageLog.
func runStage7OffTableCursorInputScopes(t *testing.T, offRows []offTableConversionEntry) {
	t.Helper()
	cursorReq := []byte(`{
		"model":"gpt-4o",
		"stream":true,
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"Cursor off-table check"}]}
		]
	}`)
	chatClient := stage7ClientSpecForProtocol(protocol.ProtocolOpenAIChatCompletions)

	for _, entry := range offRows {
		if entry.Category != offTableCursorChatInput {
			continue
		}
		targetProto := entry.TargetProtocol
		for _, pair := range expandStage7PlatformTypeScopes(t, entry.Scope) {
			subName := fmt.Sprintf("cursor_input_to_%s_%s_%s", targetProto, pair.platform, pair.providerType)
			t.Run(subName, func(t *testing.T) {
				provSpec := stage7ProviderSpecForRouteRow(conversionRouteEntry{
					ClientProtocol:   protocol.ProtocolOpenAIChatCompletions,
					Platform:         pair.platform,
					ProviderType:     pair.providerType,
					UpstreamProtocol: targetProto,
				})
				replySSE := buildUpstreamTextSSE(targetProto, provSpec.upstreamModel, "cur_s7", "cursor-scope-ok")
				mockH := mockupstream.NewWithConfig(mockupstream.Config{
					Script: testkit.Script{
						Name:    "stage7_cursor_scope",
						Default: testkit.Reply{Raw: replySSE},
					},
					Validation: provSpec.validation,
				})
				upSrv := httptest.NewServer(mockH)
				defer upSrv.Close()

				lab := newProtocolLabRouter(t, labOptions{
					Providers: []labProvider{makeLabProviderSpec(710, 0, upSrv.URL, provSpec)},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				status, _, respBytes := sendLabClientRequest(t, gwSrv, chatClient, cursorReq)
				require.Equal(t, http.StatusOK, status, string(respBytes))
				require.Empty(t, mockH.Violations(), "cursor violations: %+v", mockH.Violations())
				require.Contains(t, string(respBytes), "cursor-scope-ok")
				frames, fErr := readAllSSEFrames(respBytes)
				require.NoError(t, fErr)
				require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIChatCompletions, frames, testkit.GrammarOptions{}))
				require.Len(t, lab.UsageLogs(), 1)
			})
		}
	}
}

// runStage7OffTableCountTokensAndEstimationScopes exercises every platform and
// provider_type scope listed on routes.txt [off_table] lines 83 through 85 across
// /v1/messages/count_tokens. Each cell verifies
// HTTP 200 positive input_tokens counts, carrier stripping, and zero UsageLogs.
func runStage7OffTableCountTokensAndEstimationScopes(t *testing.T, offRows []offTableConversionEntry) {
	t.Helper()
	oaCarrier, err := ir.FormatCarrier(ir.Carrier{
		Scheme:     ir.SchemeOpenAI,
		Data:       "gAAAA_stage7_count",
		ProviderID: 720,
	})
	require.NoError(t, err)
	countBody := []byte(fmt.Sprintf(`{
		"model":"claude-sonnet-4-5",
		"system":"Count tokens accurately.",
		"messages":[
			{"role":"user","content":"estimate this"},
			{"role":"assistant","content":[
				{"type":"thinking","thinking":"Reasoning step.","signature":%q},
				{"type":"text","text":"Ack."}
			]},
			{"role":"user","content":"follow up"}
		]
	}`, oaCarrier))

	postCount := func(gwSrv *httptest.Server) (int, []byte) {
		req, rErr := http.NewRequestWithContext(t.Context(), http.MethodPost, gwSrv.URL+"/v1/messages/count_tokens", bytes.NewReader(countBody))
		require.NoError(t, rErr)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer lab-api-key")
		resp, dErr := gwSrv.Client().Do(req)
		require.NoError(t, dErr)
		raw, readErr := io.ReadAll(resp.Body)
		require.NoError(t, resp.Body.Close())
		require.NoError(t, readErr)
		return resp.StatusCode, raw
	}

	for _, entry := range offRows {
		switch entry.Category {
		case offTableMessagesCountTokens:
			targetProto := entry.TargetProtocol
			for _, pair := range expandStage7PlatformTypeScopes(t, entry.Scope) {
				subName := fmt.Sprintf("messages_count_tokens_%s_%s_%s", targetProto, pair.platform, pair.providerType)
				t.Run(subName, func(t *testing.T) {
					var mu sync.Mutex
					var captured [][]byte
					upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						raw, rErr := io.ReadAll(r.Body)
						require.NoError(t, rErr)
						mu.Lock()
						captured = append(captured, raw)
						mu.Unlock()
						w.Header().Set("Content-Type", "application/json")
						if targetProto == protocol.ProtocolGeminiGenerateContent {
							_, _ = io.WriteString(w, `{"totalTokens":47}`)
						} else {
							_, _ = io.WriteString(w, `{"object":"response.input_tokens","input_tokens":49}`)
						}
					}))
					defer upSrv.Close()

					provSpec := stage7ProviderSpecForRouteRow(conversionRouteEntry{
						ClientProtocol:   protocol.ProtocolAnthropicMessages,
						Platform:         pair.platform,
						ProviderType:     pair.providerType,
						UpstreamProtocol: targetProto,
					})
					lab := newProtocolLabRouter(t, labOptions{
						Providers:     []labProvider{makeLabProviderSpec(720, 0, upSrv.URL, provSpec)},
						HostOverrides: map[string]string{"api.openai.com": upSrv.URL},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					status, respRaw := postCount(gwSrv)
					require.Equal(t, http.StatusOK, status, string(respRaw))
					require.Greater(t, gjson.GetBytes(respRaw, "input_tokens").Int(), int64(40))
					require.Empty(t, lab.UsageLogs())

					mu.Lock()
					require.Len(t, captured, 1)
					wire := captured[0]
					mu.Unlock()
					if targetProto == protocol.ProtocolOpenAIResponses {
						require.NotContains(t, string(wire), "trc1:")
						require.Contains(t, string(wire), "gAAAA_stage7_count")
					} else {
						require.NotContains(t, string(wire), "thoughtSignature")
					}
				})
			}

		case offTableLocalTokenEstimation:
			for _, pair := range expandStage7PlatformTypeScopes(t, entry.Scope) {
				subName := fmt.Sprintf("local_input_token_estimation_%s_%s", pair.platform, pair.providerType)
				t.Run(subName, func(t *testing.T) {
					upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						// Return OAuth scope/endpoint rejection for OpenAI and Gemini OAuth
						// so their local estimator fallback executes; compatible and Grok
						// providers estimate locally without contacting upSrv.
						w.Header().Set("Content-Type", "application/json")
						w.Header().Set("WWW-Authenticate", `Bearer error="insufficient_scope"`)
						w.WriteHeader(http.StatusForbidden)
						_, _ = io.WriteString(w, `{"error":{"code":"missing_scope","message":"Missing scopes: api.responses.write","status":"PERMISSION_DENIED"}}`)
					}))
					defer upSrv.Close()

					targetProto := protocol.ProtocolOpenAIChatCompletions
					switch pair.platform {
					case capability.PlatformOpenAI, capability.PlatformGrok:
						targetProto = protocol.ProtocolOpenAIResponses
					case capability.PlatformGemini:
						targetProto = protocol.ProtocolGeminiGenerateContent
					}
					provSpec := stage7ProviderSpecForRouteRow(conversionRouteEntry{
						ClientProtocol:   protocol.ProtocolAnthropicMessages,
						Platform:         pair.platform,
						ProviderType:     pair.providerType,
						UpstreamProtocol: targetProto,
					})
					lab := newProtocolLabRouter(t, labOptions{
						Providers:     []labProvider{makeLabProviderSpec(721, 0, upSrv.URL, provSpec)},
						HostOverrides: map[string]string{"api.openai.com": upSrv.URL},
					})
					gwSrv := httptest.NewServer(lab)
					defer gwSrv.Close()

					status, respRaw := postCount(gwSrv)
					require.Equal(t, http.StatusOK, status, string(respRaw))
					require.Greater(t, gjson.GetBytes(respRaw, "input_tokens").Int(), int64(0))
					require.Empty(t, lab.UsageLogs())
				})
			}
		}
	}
}

// runStage7OffTableLegacyResponsesIngressScopes exercises every HTTP and
// WebSocket scope on routes.txt [off_table] lines 86 and 87 by sending
// Responses payloads carrying legacy Chat-shaped messages arrays. Each scope verifies normalized upstream execution,
// valid Responses client events, and single UsageLog settlement.
func runStage7OffTableLegacyResponsesIngressScopes(t *testing.T, offRows []offTableConversionEntry) {
	t.Helper()
	legacyHTTPBody := []byte(`{
		"model":"gpt-5",
		"stream":true,
		"messages":[
			{"role":"system","content":"Normalize legacy messages."},
			{"role":"user","content":"Hello legacy ingress"}
		]
	}`)
	respClient := stage7ClientSpecForProtocol(protocol.ProtocolOpenAIResponses)

	for _, entry := range offRows {
		if entry.Category != offTableResponsesLegacyMessages {
			continue
		}
		isWS := entry.ClientProtocol == protocol.ProtocolResponsesWebSocket
		for _, pair := range expandStage7PlatformTypeScopes(t, entry.Scope) {
			subName := fmt.Sprintf("legacy_messages_ingress_%s_%s_%s", entry.ClientProtocol, pair.platform, pair.providerType)
			t.Run(subName, func(t *testing.T) {
				upProto := protocol.ProtocolOpenAIResponses
				if pair.platform == capability.PlatformAnthropic {
					upProto = protocol.ProtocolAnthropicMessages
				}
				provSpec := stage7ProviderSpecForRouteRow(conversionRouteEntry{
					ClientProtocol:   protocol.ProtocolOpenAIResponses,
					Platform:         pair.platform,
					ProviderType:     pair.providerType,
					UpstreamProtocol: upProto,
				})
				if pair.platform == capability.PlatformOpenAI && pair.providerType == capability.ProviderTypeOAuth {
					provSpec.extra = map[string]any{
						"openai_passthrough":                        true,
						"openai_oauth_responses_websockets_v2_mode": "off",
					}
				}
				replySSE := buildUpstreamTextSSE(upProto, provSpec.upstreamModel, "leg_s7", "legacy-ingress-ok")
				mockH := mockupstream.NewWithConfig(mockupstream.Config{
					Script: testkit.Script{
						Name:    "stage7_legacy_ingress",
						Default: testkit.Reply{Raw: replySSE},
					},
					Validation: provSpec.validation,
				})
				upSrv := newClientLabScriptedUpstreamServer(provSpec, mockH, nil)
				defer upSrv.Close()

				var allowed []protocol.ProtocolID
				if isWS {
					allowed = []protocol.ProtocolID{
						protocol.ProtocolResponsesWebSocket,
						protocol.ProtocolOpenAIResponses,
					}
				}
				lab := newProtocolLabRouter(t, labOptions{
					Providers:        []labProvider{makeLabProviderSpec(730, 0, upSrv.URL, provSpec)},
					AllowedProtocols: allowed,
					HostOverrides:    map[string]string{"chatgpt.com": upSrv.URL},
				})
				gwSrv := httptest.NewServer(lab)
				defer gwSrv.Close()

				if !isWS {
					status, _, respBytes := sendLabClientRequest(t, gwSrv, respClient, legacyHTTPBody)
					require.Equal(t, http.StatusOK, status, string(respBytes))
					require.Empty(t, mockH.Violations(), "legacy HTTP violations: %+v", mockH.Violations())
					require.Contains(t, string(respBytes), "legacy-ingress-ok")
					frames, fErr := readAllSSEFrames(respBytes)
					require.NoError(t, fErr)
					require.Empty(t, testkit.ValidateStreamGrammar(protocol.ProtocolOpenAIResponses, frames, testkit.GrammarOptions{}))
					require.Len(t, lab.UsageLogs(), 1)
					return
				}

				wsURL := "ws" + strings.TrimPrefix(gwSrv.URL, "http") + "/v1/responses"
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				conn, _, dErr := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
					HTTPHeader: http.Header{"Authorization": []string{"Bearer lab-api-key"}},
				})
				require.NoError(t, dErr)
				defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()

				wsCreate := `{"type":"response.create","model":"gpt-5","messages":[{"role":"user","content":"Hello WS legacy ingress"}]}`
				require.NoError(t, conn.Write(ctx, websocket.MessageText, []byte(wsCreate)))

				sawCompleted := false
				for !sawCompleted {
					_, msgBytes, rErr := conn.Read(ctx)
					require.NoError(t, rErr)
					evType := gjson.GetBytes(msgBytes, "type").String()
					if evType == "response.completed" {
						sawCompleted = true
						require.Contains(t, string(msgBytes), "legacy-ingress-ok")
					}
				}
				_ = conn.Close(websocket.StatusNormalClosure, "")
				require.Empty(t, mockH.Violations(), "legacy WS violations: %+v", mockH.Violations())
				require.Eventually(t, func() bool {
					return len(lab.UsageLogs()) == 1
				}, time.Second, 5*time.Millisecond)
			})
		}
	}
}

// expandStage7PlatformTypeScopes splits semicolon-separated platform groups and
// comma-separated provider_type lists from testdata/golden/routes.txt into
// ordered pairs. Segments lacking a colon separator or containing empty
// platform or provider_type names fail tb immediately.
func expandStage7PlatformTypeScopes(tb testing.TB, scope string) []stage7PlatformTypePair {
	tb.Helper()
	var out []stage7PlatformTypePair
	for group := range strings.SplitSeq(scope, ";") {
		plat, rawTypes, ok := strings.Cut(strings.TrimSpace(group), ":")
		require.True(tb, ok, "malformed off-table scope segment %q", group)
		for pt := range strings.SplitSeq(rawTypes, ",") {
			trimmed := strings.TrimSpace(pt)
			require.NotEmpty(tb, trimmed)
			out = append(out, stage7PlatformTypePair{
				platform:     strings.TrimSpace(plat),
				providerType: trimmed,
			})
		}
	}
	require.NotEmpty(tb, out)
	return out
}

// stage7ClientSpecForProtocol looks up the canonical labClientSpec descriptor
// matching clientProto inside protocolLabClients for T5 route execution.
// Passing an unregistered client protocol identifier panics immediately so
// unsupported ingress protocols never fall back to silent zero values.
func stage7ClientSpecForProtocol(clientProto protocol.ProtocolID) labClientSpec {
	for _, c := range protocolLabClients {
		if c.protocol == clientProto {
			return c
		}
	}
	panic("unsupported Stage 7 client protocol: " + string(clientProto))
}

// stage7SampleClientStreamRequest builds a minimal valid streaming client
// request JSON payload in clientProto for model and text. Unsupported client
// protocol identifiers panic immediately instead of returning empty bytes.
func stage7SampleClientStreamRequest(clientProto protocol.ProtocolID, model, text string) []byte {
	switch clientProto {
	case protocol.ProtocolAnthropicMessages:
		raw, _ := json.Marshal(map[string]any{
			"model":      model,
			"max_tokens": 256,
			"stream":     true,
			"messages": []map[string]any{
				{"role": "user", "content": text},
			},
		})
		return raw
	case protocol.ProtocolOpenAIResponses:
		raw, _ := json.Marshal(map[string]any{
			"model":  model,
			"stream": true,
			"input":  text,
		})
		return raw
	case protocol.ProtocolOpenAIChatCompletions:
		raw, _ := json.Marshal(map[string]any{
			"model":  model,
			"stream": true,
			"messages": []map[string]any{
				{"role": "user", "content": text},
			},
		})
		return raw
	default:
		panic("unsupported Stage 7 sample request protocol: " + string(clientProto))
	}
}

// stage7ProviderSpecForRouteRow constructs a labProviderKindSpec with strict
// request validation matching row's platform, provider_type, and upstream_protocol.
// Antigravity rows omit EnabledProtocols while OpenAI raw Chat rows set force_chat_completions.
func stage7ProviderSpecForRouteRow(row conversionRouteEntry) labProviderKindSpec {
	model := "gpt-5.4"
	var val testkit.RequestValidationOptions
	switch row.UpstreamProtocol {
	case protocol.ProtocolAnthropicMessages:
		model = "claude-sonnet-4-5-20250929"
		if row.ProviderType == capability.ProviderTypeBedrock || row.ProviderType == capability.ProviderTypeServiceAccount || row.Platform == capability.PlatformOpenCode {
			model = "claude-sonnet-4-6"
		}
		val.StrictTopLevel = true
		switch row.Platform {
		case capability.PlatformKimi:
			model = "k3"
			val.DialectRules = []string{"passback"}
		case capability.PlatformZhipu:
			model = "glm-5"
			val.DialectRules = []string{"passback"}
		case capability.PlatformDeepseek:
			model = "deepseek-reasoner"
			val.DialectRules = []string{"passback"}
		}
	case protocol.ProtocolOpenAIResponses:
		switch row.Platform {
		case capability.PlatformGrok:
			model = "grok-4.5"
			val.DialectRules = []string{"function-only"}
		case capability.PlatformKimi:
			model = "k3"
			val.DialectRules = []string{"function-only"}
		case capability.PlatformDeepseek:
			model = "deepseek-chat"
			val.DialectRules = []string{"function-only"}
		case capability.PlatformOpenCode:
			model = "gpt-5.4"
			val.DialectRules = []string{"function-only"}
		}
	case protocol.ProtocolOpenAIChatCompletions:
		model = "gpt-4o"
		switch row.Platform {
		case capability.PlatformGrok:
			model = "grok-4.5"
		case capability.PlatformKimi:
			model = "k3"
		case capability.PlatformZhipu:
			model = "glm-5"
		case capability.PlatformDeepseek:
			model = "deepseek-chat"
		case capability.PlatformOpenCode:
			model = "gpt-5.4"
		}
	case protocol.ProtocolGeminiGenerateContent:
		model = "gemini-2.5-pro"
		val.DialectRules = []string{"gemini-signature"}
		switch row.Platform {
		case capability.PlatformAntigravity:
			model = "claude-sonnet-4-5"
			val.DialectRules = []string{"antigravity-schema"}
		case capability.PlatformOpenCode:
			model = "gemini-3-flash"
		}
	}

	var extra map[string]any
	if row.Platform == capability.PlatformOpenAI && row.UpstreamProtocol == protocol.ProtocolOpenAIChatCompletions {
		extra = map[string]any{"openai_text_route_mode": "force_chat_completions"}
	}
	var enabled []protocol.ProtocolID
	if row.Platform != capability.PlatformAntigravity {
		enabled = []protocol.ProtocolID{row.UpstreamProtocol}
	}
	return labProviderKindSpec{
		kind:             fmt.Sprintf("s7_%s_%s_%s", row.Platform, row.ProviderType, row.UpstreamProtocol),
		platform:         row.Platform,
		providerType:     row.ProviderType,
		upstreamProtocol: row.UpstreamProtocol,
		upstreamModel:    model,
		enabledProtocols: enabled,
		extra:            extra,
		validation:       val,
		failoverStatus:   529,
	}
}

// parseStage7RoutesWithLineNumbers parses testdata/golden/routes.txt while
// retaining each effective non-comment row's 1-based line number. Lines before
// any section header or rows without six tab-separated columns fail tb immediately.
func parseStage7RoutesWithLineNumbers(tb testing.TB, content string) []stage7ParsedRouteLine {
	tb.Helper()
	var (
		section string
		out     []stage7ParsedRouteLine
	)
	for idx, rawLine := range strings.Split(content, "\n") {
		lineNo := idx + 1
		trimmed := strings.TrimSpace(rawLine)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if trimmed == routesSectionRouteTable || trimmed == routesSectionOffTable {
			section = trimmed
			continue
		}
		require.NotEmpty(tb, section, "line %d precedes section header", lineNo)
		cols := strings.Split(trimmed, "\t")
		require.Len(tb, cols, 6, "line %d must contain 6 tab-separated columns", lineNo)
		out = append(out, stage7ParsedRouteLine{
			LineNumber: lineNo,
			Section:    section,
			RawLine:    trimmed,
			Columns:    cols,
		})
	}
	return out
}

// buildStage7T5RouteIndex builds a deterministic reverse index mapping each
// four-part [route_table] key to the exact "internal/app:<TestFunc>" T5 suites
// whose route constructors execute that conversion cell.
// Native passthrough control combinations are omitted from the returned map.
func buildStage7T5RouteIndex(routeRows []conversionRouteEntry) map[string][]string {
	index := make(map[string][]string, len(routeRows))
	bindKey := func(testRef, key string) {
		if !slices.Contains(index[key], testRef) {
			index[key] = append(index[key], testRef)
		}
	}
	bindCombo := func(testRef string, combo labRouteCombo) {
		if combo.client.protocol == combo.provider.upstreamProtocol {
			return
		}
		bindKey(testRef, formatStage7RouteKey(
			combo.client.protocol,
			combo.provider.platform,
			combo.provider.providerType,
			combo.provider.upstreamProtocol,
		))
	}

	for _, row := range routeRows {
		bindKey("internal/app:TestProtocolLabStage7RouteMatrixComplete", formatStage7RouteKey(
			row.ClientProtocol,
			row.Platform,
			row.ProviderType,
			row.UpstreamProtocol,
		))
	}

	for _, combo := range activeLabRouteCombos() {
		switch combo.slug {
		case "messages_via_openai", "chat_via_openai":
			bindCombo("internal/app:TestProtocolLabIRResponsesUpstreamMatrix", combo)
		case "messages_via_openai_chat", "responses_via_openai_chat":
			bindCombo("internal/app:TestProtocolLabIRChatUpstreamMatrix", combo)
		case "responses_via_anthropic", "chat_via_anthropic",
			"responses_via_deepseek_anthropic", "chat_via_deepseek_anthropic":
			bindCombo("internal/app:TestProtocolLabIRMessagesUpstreamMatrix", combo)
		case "messages_via_gemini", "responses_via_gemini", "chat_via_gemini":
			bindCombo("internal/app:TestProtocolLabIRGeminiUpstreamMatrix", combo)
		}
	}
	// TestProtocolLabIRMessagesUpstreamMatrix also runs explicit Vertex Claude
	// (Responses) and AWS Bedrock (Responses and Chat) conversion subtests.
	bindKey("internal/app:TestProtocolLabIRMessagesUpstreamMatrix", formatStage7RouteKey(
		protocol.ProtocolOpenAIResponses, capability.PlatformAnthropic, capability.ProviderTypeServiceAccount, protocol.ProtocolAnthropicMessages,
	))
	bindKey("internal/app:TestProtocolLabIRMessagesUpstreamMatrix", formatStage7RouteKey(
		protocol.ProtocolOpenAIResponses, capability.PlatformAnthropic, capability.ProviderTypeBedrock, protocol.ProtocolAnthropicMessages,
	))
	bindKey("internal/app:TestProtocolLabIRMessagesUpstreamMatrix", formatStage7RouteKey(
		protocol.ProtocolOpenAIChatCompletions, capability.PlatformAnthropic, capability.ProviderTypeBedrock, protocol.ProtocolAnthropicMessages,
	))

	for _, client := range protocolLabClients {
		if client.protocol != protocol.ProtocolAnthropicMessages && client.protocol != protocol.ProtocolOpenAIChatCompletions {
			continue
		}
		for _, spec := range grokLabProviderSpecs() {
			bindCombo("internal/app:TestProtocolLabIRGrokChatResponsesMatrix", labRouteCombo{
				client:   client,
				provider: spec,
			})
		}
	}

	for _, upProto := range []protocol.ProtocolID{
		protocol.ProtocolOpenAIResponses,
		protocol.ProtocolOpenAIChatCompletions,
		protocol.ProtocolAnthropicMessages,
	} {
		for _, client := range protocolLabClients[:3] {
			if client.protocol == upProto {
				continue
			}
			bindKey("internal/app:TestProtocolLabIRStage2MixedGroupAndOpenCodeMatrix", formatStage7RouteKey(
				client.protocol, capability.PlatformOpenCode, capability.ProviderTypeAPIKey, upProto,
			))
		}
	}

	vertexSpec := vertexGeminiLabSpec()
	openCodeGem := openCodeGeminiLabSpec()
	for _, client := range protocolLabClients[:3] {
		bindCombo("internal/app:TestProtocolLabIRGeminiUpstreamMatrix", labRouteCombo{client: client, provider: vertexSpec})
		bindCombo("internal/app:TestProtocolLabIRGeminiUpstreamMatrix", labRouteCombo{client: client, provider: openCodeGem})
	}
	bindKey("internal/app:TestProtocolLabIRGeminiUpstreamMatrix", formatStage7RouteKey(
		protocol.ProtocolOpenAIResponses, capability.PlatformGemini, capability.ProviderTypeOAuth, protocol.ProtocolGeminiGenerateContent,
	))

	for _, client := range protocolLabClients[:3] {
		for _, agSpec := range antigravityLabProviderSpecs() {
			bindCombo("internal/app:TestProtocolLabIRAntigravityUpstreamMatrix", labRouteCombo{client: client, provider: agSpec})
		}
		bindCombo("internal/app:TestProtocolLabIRQoderUpstreamMatrix", labRouteCombo{client: client, provider: stage6QoderLabProviderSpec()})
	}

	return index
}

// buildStage7CoverageForLine maps one parsed routes.txt line to its T3, T5, and
// T6 test entry points using t5RouteIndex and t6RouteIndex. Unhandled off-table
// categories or missing T5/T6 index entries fail tb immediately.
func buildStage7CoverageForLine(
	tb testing.TB,
	pl stage7ParsedRouteLine,
	t5RouteIndex map[string][]string,
	t6RouteIndex map[string][]string,
) stage7RouteCoverageEntry {
	tb.Helper()
	if pl.Section == routesSectionRouteTable {
		clientProto := protocol.ProtocolID(pl.Columns[0])
		platform := pl.Columns[1]
		providerType := pl.Columns[2]
		upstreamProto := protocol.ProtocolID(pl.Columns[3])
		routeKey := formatStage7RouteKey(clientProto, platform, providerType, upstreamProto)

		t3 := []string{
			"internal/protocol/convert:TestLawsQ3Q4Q5_CrossProtocolRequestsAndLossGoldens",
			"internal/protocol/convert:TestLawsS3S4S5S6S7S8_CrossProtocolResponsesAndStreams",
			"internal/protocol/convert:TestLawM1_MultiTurnReplayAcrossAllRoutesSchemesAndNative",
		}
		if platform == capability.PlatformGrok || platform == capability.PlatformDeepseek {
			t3 = append(t3, "internal/protocol/convert:TestStage7OffTableAndDialectLaws")
		}
		if platform == capability.PlatformAntigravity {
			t3 = append(t3, "internal/protocol/convert:TestLawM1_AntigravityClaudeAndGeminiContinuityAndCrossFamilyIsolation")
		} else if upstreamProto == protocol.ProtocolGeminiGenerateContent {
			t3 = append(t3, "internal/protocol/convert:TestLawM1_GeminiContinuityCallMemoryLateSignatureAndUnsignedStep")
		}
		if platform == capability.PlatformQoder {
			t3 = append(t3,
				"internal/protocol/convert:TestQoderStatelessAndCursorRequestInvariants",
				"internal/protocol/convert:TestLawM1_QoderPlainCarrierLoweredToolsAndUpstreamAliasReplay",
			)
		}

		t5 := t5RouteIndex[routeKey]
		require.NotEmpty(tb, t5, "routes.txt line %d (%s) missing from T5 route index", pl.LineNumber, routeKey)

		switch platform {
		case capability.PlatformAntigravity:
			return stage7RouteCoverageEntry{
				LineNumber:     pl.LineNumber,
				Section:        pl.Section,
				RouteSpec:      pl.RawLine,
				T3Tests:        t3,
				T5Tests:        t5,
				T6Status:       stage7T6Exempt,
				T6ExemptReason: "Section 6.5 Step 4 and Section 6.8 Step 2 exempt Antigravity OAuth v1internal from CLI T6; verified via T3, T4, and T5.",
			}
		case capability.PlatformQoder:
			return stage7RouteCoverageEntry{
				LineNumber:     pl.LineNumber,
				Section:        pl.Section,
				RouteSpec:      pl.RawLine,
				T3Tests:        t3,
				T5Tests:        t5,
				T6Status:       stage7T6Exempt,
				T6ExemptReason: "Section 6.7 Step 4 and Section 6.8 Step 2 exempt Qoder COSY desktop sessions from CLI T6; verified via T3, T4, and T5.",
			}
		}

		t6 := t6RouteIndex[routeKey]
		require.NotEmpty(tb, t6, "routes.txt line %d (%s) missing from scripted T6 route index", pl.LineNumber, routeKey)
		return stage7RouteCoverageEntry{
			LineNumber: pl.LineNumber,
			Section:    pl.Section,
			RouteSpec:  pl.RawLine,
			T3Tests:    t3,
			T5Tests:    t5,
			T6Status:   stage7T6Covered,
			T6Tests:    t6,
		}
	}

	category := pl.Columns[0]
	switch category {
	case offTableResponsesClientTools:
		return stage7RouteCoverageEntry{
			LineNumber: pl.LineNumber,
			Section:    pl.Section,
			RouteSpec:  pl.RawLine,
			T3Tests: []string{
				"internal/protocol/convert:TestAdaptAndRestoreResponsesTools_CustomNamespaceSearchAndWSWarmup",
				"internal/protocol/convert:TestRestoreResponsesToolsAndStream_HostedEventsEnvelopesAndQuirks",
				"internal/protocol/convert:TestRecordedResponsesCorpus_LosslessPrecheckAndFieldPreservation",
			},
			T5Tests: []string{
				"internal/app:TestProtocolLabStage7RouteMatrixComplete",
				"internal/app:TestProtocolLabIRStage5ResponsesClientToolsMatrix",
			},
			T6Status: stage7T6Covered,
			T6Tests: []string{
				"internal/app:TestClientLabIRStage5ResponsesCustomToolScenarios",
			},
		}

	case offTableCursorChatInput:
		return stage7RouteCoverageEntry{
			LineNumber: pl.LineNumber,
			Section:    pl.Section,
			RouteSpec:  pl.RawLine,
			T3Tests: []string{
				"internal/protocol/convert:TestStage7OffTableAndDialectLaws",
				"internal/protocol/convert:TestQoderStatelessAndCursorRequestInvariants",
			},
			T5Tests: []string{
				"internal/app:TestProtocolLabStage7RouteMatrixComplete",
			},
			T6Status:       stage7T6Exempt,
			T6ExemptReason: "Cursor IDE closed-source desktop client sends input-shaped /v1/chat/completions payloads not emitted by standalone Codex, Claude Code, or pi CLIs; verified end-to-end in T3 and T5.",
		}

	case offTableMessagesCountTokens:
		if pl.Columns[3] == string(protocol.ProtocolGeminiGenerateContent) {
			return stage7RouteCoverageEntry{
				LineNumber: pl.LineNumber,
				Section:    pl.Section,
				RouteSpec:  pl.RawLine,
				T3Tests: []string{
					"internal/protocol/convert:TestStage7OffTableAndDialectLaws",
				},
				T5Tests: []string{
					"internal/app:TestProtocolLabStage7RouteMatrixComplete",
					"internal/app:TestProtocolLabIRGeminiUpstreamMatrix",
				},
				T6Status:       stage7T6Exempt,
				T6ExemptReason: "Auxiliary /v1/messages/count_tokens preflight endpoint is not deterministically invoked as an isolated CLI generation cell; verified across all Gemini auth types in T3 and T5.",
			}
		}
		return stage7RouteCoverageEntry{
			LineNumber: pl.LineNumber,
			Section:    pl.Section,
			RouteSpec:  pl.RawLine,
			T3Tests: []string{
				"internal/gateway/provider:TestPrepareAnthropicInputTokens_FiveFieldProjectionAndCarrierUnpacking",
				"internal/gateway/provider:TestPrepareNativeInputTokensAndEstimateLocally",
				"internal/gateway/provider:TestPrepareInputTokens_CorpusAndWireValidation",
			},
			T5Tests: []string{
				"internal/app:TestProtocolLabStage7RouteMatrixComplete",
			},
			T6Status:       stage7T6Exempt,
			T6ExemptReason: "Auxiliary /v1/messages/count_tokens -> /v1/responses/input_tokens preflight endpoint is not deterministically invoked by standalone CLIs; verified across OpenAI OAuth and API Key in T3 and T5.",
		}

	case offTableLocalTokenEstimation:
		return stage7RouteCoverageEntry{
			LineNumber: pl.LineNumber,
			Section:    pl.Section,
			RouteSpec:  pl.RawLine,
			T3Tests: []string{
				"internal/gateway/tokenestimate:TestCountingPayloadAndResponsesBytes_LosslessProjectionAndExactParity",
				"internal/gateway/tokenestimate:TestAnthropicAndResponsesBytes_CorpusValidation",
			},
			T5Tests: []string{
				"internal/app:TestProtocolLabStage7RouteMatrixComplete",
			},
			T6Status:       stage7T6Exempt,
			T6ExemptReason: "Local tiktoken input estimation backs auxiliary /v1/messages/count_tokens without a dedicated CLI invocation mode; verified across all eight platform/auth scopes in T3 and T5.",
		}

	case offTableResponsesLegacyMessages:
		return stage7RouteCoverageEntry{
			LineNumber: pl.LineNumber,
			Section:    pl.Section,
			RouteSpec:  pl.RawLine,
			T3Tests: []string{
				"internal/gateway/provider:TestNormalizeOpenAIResponsesLegacyIngress_LosslessHybridAndUint64Preservation",
				"internal/gateway/provider:TestNormalizeOpenAIResponsesLegacyIngress_CorpusValidation",
			},
			T5Tests: []string{
				"internal/app:TestProtocolLabStage7RouteMatrixComplete",
			},
			T6Status:       stage7T6Exempt,
			T6ExemptReason: "Non-standard Responses payloads carrying legacy Chat messages[] come from custom SDK shims rather than Codex, Claude Code, or pi CLIs; verified over HTTP and WebSocket in T3 and T5.",
		}

	default:
		tb.Fatalf("unhandled [off_table] category %q on line %d", category, pl.LineNumber)
		return stage7RouteCoverageEntry{}
	}
}
