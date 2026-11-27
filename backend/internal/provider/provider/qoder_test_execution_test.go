package provider

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	"github.com/TokenFlux/TokenRouter/internal/provider/modelrules"
	"github.com/TokenFlux/TokenRouter/internal/provider/qodermodel"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/upstream/qoder"
)

// qoderProbeEventSink captures provider TestEvent emissions in memory during
// Qoder COSY connection probe tests.
type qoderProbeEventSink struct {
	events []providercore.TestEvent // ordered probe events emitted so far
}

// qoderProbeSessionStub implements qoderProviderTestSessionProvider and
// qoderProviderTestSessionInvalidator for PAT invalidation assertions.
type qoderProbeSessionStub struct {
	session     *qoder.SessionContext // session returned by GetSession
	invalidated []int64               // provider IDs passed to Invalidate
}

// qoderProbeUserInfoStub implements qoderProviderTestOAuthClient for Global
// site user-info verification tests.
type qoderProbeUserInfoStub struct {
	calls int // count of GetUserInfo invocations
	// info and err configure the fixed GetUserInfo response: a non-nil err fails
	// the Global user-info probe before stream dispatch, whereas info fills absent identity hints.
	info *qoder.UserInfo // user identity returned on success
	err  error           // error returned when non-nil
}

// qoderProbeStreamClientStub records outbound payload bytes and headers while
// returning a fresh SSE body per probe run.
type qoderProbeStreamClientStub struct {
	sseBody string // raw Qoder SSE stream body returned for each call
	// bodies and headers append one cloned entry at matching indices on each
	// StreamRequestContext call so assertions inspect captured request state.
	bodies  [][]byte            // captured JSON request payloads
	headers []map[string]string // captured request header maps
}

// Begin accepts probe stream framing without error for the in-memory event sink.
func (*qoderProbeEventSink) Begin(context.Context, bool) error { return nil }

// Emit appends event onto s.events in arrival order for test assertions.
func (s *qoderProbeEventSink) Emit(_ context.Context, event providercore.TestEvent) error {
	s.events = append(s.events, event)
	return nil
}

// GetSession returns s.session directly without performing live Qoder credential network calls.
func (s *qoderProbeSessionStub) GetSession(context.Context, *providercore.Record) (*qoder.SessionContext, error) {
	return s.session, nil
}

// Invalidate records providerID on s.invalidated so PAT probes can assert cache eviction.
func (s *qoderProbeSessionStub) Invalidate(providerID int64) {
	s.invalidated = append(s.invalidated, providerID)
}

// GetUserInfo increments s.calls and returns the configured UserInfo and error values.
func (s *qoderProbeUserInfoStub) GetUserInfo(context.Context, string) (*qoder.UserInfo, error) {
	s.calls++
	return s.info, s.err
}

// StreamRequestContext records body and headers and returns a fresh HTTP 200 SSE response.
func (s *qoderProbeStreamClientStub) StreamRequestContext(_ context.Context, _ *qoder.SessionContext, _ string, body []byte, headers map[string]string) (*http.Response, error) {
	s.bodies = append(s.bodies, append([]byte(nil), body...))
	clonedHeaders := make(map[string]string, len(headers))
	maps.Copy(clonedHeaders, headers)
	s.headers = append(s.headers, clonedHeaders)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader(s.sseBody)),
	}, nil
}

// newQoderTestRecord constructs an active COSY provider record populated with a
// prepared Qoder model catalog snapshot and public-alias mapping for probe
// tests. Non-empty pat inputs attach a PAT credential so callers can exercise
// pre-probe session cache invalidation.
func newQoderTestRecord(id int64, site string, pat string) *providercore.Record {
	creds := map[string]any{
		"site": site,
		"model_mapping": map[string]any{
			"public-alias": "ultimate",
		},
	}
	if pat != "" {
		creds["pat"] = pat
	}
	rec := &providercore.Record{
		ID:          id,
		Name:        "qoder-probe",
		Platform:    capability.PlatformQoder,
		Type:        capability.ProviderTypeCosy,
		Status:      providercore.StatusActive,
		Schedulable: true,
		Credentials: creds,
	}
	rec.Extra = map[string]any{
		providercore.QoderCatalogExtraKey: map[string]any{
			"identity":   providercore.QoderCatalogIdentity(rec),
			"version":    "v1",
			"fetched_at": time.Now(),
			"models": []map[string]any{
				{
					"key":              "ultimate",
					"alias":            "ultimate",
					"name":             "Ultimate Reasoning",
					"enabled":          true,
					"default":          true,
					"reasoning":        true,
					"can_disable":      true,
					"default_thinking": true,
					"default_effort":   "high",
					"efforts":          []string{"low", "medium", "high"},
					"input_limit":      1000000,
					"context_windows":  []int{200000, 1000000},
					"vision":           true,
				},
			},
		},
	}
	rec.PrepareQoderCatalog()
	return rec
}

// TestBuildQoderTestPayload builds COSY connection-probe payloads through
// BuildQoderTestPayload across Global and CN sites. It verifies resolved model
// header keys, user prompt placement, and catalog thinking capabilities.
func TestBuildQoderTestPayload(t *testing.T) {
	catalog := qodermodel.NewCatalog("v1", []qodermodel.Model{
		{
			Key:             "ultimate",
			Alias:           "ultimate",
			Name:            "Ultimate Reasoning",
			Enabled:         true,
			Reasoning:       new(true),
			CanDisable:      new(true),
			DefaultThinking: true,
			DefaultEffort:   "high",
			Efforts:         []string{"low", "medium", "high"},
			InputLimit:      1000000,
			ContextWindows:  []int{200000, 1000000},
			Vision:          new(true),
		},
	})

	for _, site := range []qoder.Site{qoder.SiteGlobal, qoder.SiteCN} {
		t.Run(string(site), func(t *testing.T) {
			resolution := modelrules.View{}.Resolve(modelrules.Target{
				Model:          "ultimate",
				ClientProtocol: string(capability.ProtocolOpenAIChatCompletions),
				ProviderModels: catalog,
			}, modelcatalog.Attributes{})

			prompt := "Write a Python function add(a, b) that returns a + b."
			payload, modelKey, err := BuildQoderTestPayload("ultimate", prompt, site, resolution)
			require.NoError(t, err)
			require.Equal(t, "ultimate", modelKey)
			require.NotEmpty(t, payload["request_id"])

			raw, err := json.Marshal(payload)
			require.NoError(t, err)
			require.Contains(t, string(raw), prompt)
		})
	}
}

// TestQoderProviderTest_Execute exercises QoderProviderTest.Execute across
// Global and CN sites, PAT session invalidation, and varied SSE termination
// shapes. It verifies emitted probe events, outbound model headers, user-info
// identity hint filling, and secret redaction on upstream errors.
func TestQoderProviderTest_Execute(t *testing.T) {
	cases := []struct {
		name           string
		site           string
		pat            string
		modelID        string
		sseBody        string
		wantErr        bool
		wantUserInfo   int
		wantInvalidate int
		wantModelStart string
		wantText       []string
	}{
		{
			name:           "global site with PAT invalidation userinfo probe and DONE stream",
			site:           "global",
			pat:            "QODER_PAT_TEST",
			modelID:        "public-alias",
			sseBody:        "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"def add\\\"}}]}\"}\n\ndata: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"(a, b): return a + b\\\"}}]}\"}\n\ndata: {\"body\":\"[DONE]\"}\n\n",
			wantErr:        false,
			wantUserInfo:   1,
			wantInvalidate: 1,
			wantModelStart: "public-alias",
			wantText:       []string{"def add", "(a, b): return a + b"},
		},
		{
			name:           "cn site skips userinfo probe and accepts EOF after parsed usage or text event without DONE",
			site:           "cn",
			pat:            "",
			modelID:        "",
			sseBody:        "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"ok\\\"}}]}\"}\n\ndata: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":5,\\\"completion_tokens\\\":1}}\"}\n\n",
			wantErr:        false,
			wantUserInfo:   0,
			wantInvalidate: 0,
			wantModelStart: "ultimate",
			wantText:       []string{"ok"},
		},
		{
			name:           "wrapped 112 entitlement error redacts secrets",
			site:           "cn",
			pat:            "",
			modelID:        "ultimate",
			sseBody:        "data: {\"statusCodeValue\":403,\"body\":\"{\\\"code\\\":\\\"112\\\",\\\"message\\\":\\\"Denied for bearer secret-abc-123\\\"}\"}\n\n",
			wantErr:        true,
			wantUserInfo:   0,
			wantInvalidate: 0,
			wantModelStart: "ultimate",
		},
		{
			name:           "empty stream before any event fails",
			site:           "cn",
			pat:            "",
			modelID:        "ultimate",
			sseBody:        "",
			wantErr:        true,
			wantUserInfo:   0,
			wantInvalidate: 0,
			wantModelStart: "ultimate",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newQoderTestRecord(91, tc.site, tc.pat)
			sessStub := &qoderProbeSessionStub{
				session: &qoder.SessionContext{
					Identity: &qoder.AuthIdentity{SecurityOauthToken: "oauth-tok"},
				},
			}
			userStub := &qoderProbeUserInfoStub{
				info: &qoder.UserInfo{ID: "uid-filled", Name: "Probe User"},
			}
			clientStub := &qoderProbeStreamClientStub{sseBody: tc.sseBody}
			probe := &QoderProviderTest{
				Sessions: sessStub,
				Client:   clientStub,
				UserInfo: userStub,
			}
			sink := &qoderProbeEventSink{}
			testRun := NewTestRun(t.Context(), nil, sink)
			defer testRun.Cancel()

			execErr := probe.Execute(testRun, rec, tc.modelID, "Write add")
			require.Len(t, clientStub.bodies, 1)
			require.Equal(t, "ultimate", clientStub.headers[0]["x-model-key"])
			require.Equal(t, tc.wantErr, execErr != nil)
			if tc.wantErr {
				require.NotContains(t, execErr.Error(), "secret-abc-123")
			} else {
				var contentChunks []string
				var sawComplete bool
				for _, ev := range sink.events {
					if ev.Type == "content" {
						contentChunks = append(contentChunks, ev.Text)
					}
					if ev.Type == "test_complete" && ev.Success {
						sawComplete = true
					}
				}
				require.Equal(t, tc.wantText, contentChunks)
				require.True(t, sawComplete)
			}
			require.NotEmpty(t, sink.events)
			require.Equal(t, "test_start", sink.events[0].Type)
			require.Equal(t, tc.wantModelStart, sink.events[0].Model)
			require.Len(t, sessStub.invalidated, tc.wantInvalidate)
			require.Equal(t, tc.wantUserInfo, userStub.calls)
			if tc.wantUserInfo > 0 {
				require.Equal(t, "uid-filled", sessStub.session.Identity.UID)
				require.Equal(t, "Probe User", sessStub.session.Identity.Name)
			}
		})
	}
}
