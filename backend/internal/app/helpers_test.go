package app

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/TokenFlux/TokenRouter/internal/apikey"
	keyhttp "github.com/TokenFlux/TokenRouter/internal/apikey/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/apikey/httpapi/dto"
	keytestkit "github.com/TokenFlux/TokenRouter/internal/apikey/testkit"
	"github.com/TokenFlux/TokenRouter/internal/app/lifecycle"
	routeaudit "github.com/TokenFlux/TokenRouter/internal/audit/httpapi"
	routebackup "github.com/TokenFlux/TokenRouter/internal/backup/httpapi"
	batchhttp "github.com/TokenFlux/TokenRouter/internal/batchimage/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/billing"
	routebilling "github.com/TokenFlux/TokenRouter/internal/billing/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/billing/pricing"
	pricingprovider "github.com/TokenFlux/TokenRouter/internal/billing/provider"
	billingtestkit "github.com/TokenFlux/TokenRouter/internal/billing/testkit"
	"github.com/TokenFlux/TokenRouter/internal/config"
	routecreative "github.com/TokenFlux/TokenRouter/internal/creative/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/egress"
	routeegress "github.com/TokenFlux/TokenRouter/internal/egress/httpapi"
	egressadapter "github.com/TokenFlux/TokenRouter/internal/egress/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway"
	"github.com/TokenFlux/TokenRouter/internal/gateway/admission"
	"github.com/TokenFlux/TokenRouter/internal/gateway/completion"
	"github.com/TokenFlux/TokenRouter/internal/gateway/errorpolicy"
	forwardcore "github.com/TokenFlux/TokenRouter/internal/gateway/forward"
	gatewayhttp "github.com/TokenFlux/TokenRouter/internal/gateway/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/gateway/httpapi/mediaentry"
	"github.com/TokenFlux/TokenRouter/internal/gateway/httpapi/openaiattempt"
	httptestkit "github.com/TokenFlux/TokenRouter/internal/gateway/httpapi/testkit"
	"github.com/TokenFlux/TokenRouter/internal/gateway/httpapi/textattempt"
	"github.com/TokenFlux/TokenRouter/internal/gateway/moderationflow"
	"github.com/TokenFlux/TokenRouter/internal/gateway/promptpolicy"
	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/googleforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/messageforward"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/selection"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/searchtools"
	"github.com/TokenFlux/TokenRouter/internal/gateway/session"
	"github.com/TokenFlux/TokenRouter/internal/gateway/telemetry"
	gatewaytestkit "github.com/TokenFlux/TokenRouter/internal/gateway/testkit"
	textflow "github.com/TokenFlux/TokenRouter/internal/gateway/text"
	"github.com/TokenFlux/TokenRouter/internal/gateway/ws"
	wshttp "github.com/TokenFlux/TokenRouter/internal/gateway/ws/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/identity"
	routeidentity "github.com/TokenFlux/TokenRouter/internal/identity/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/identity/httpapi/authctx"
	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient"
	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient/tlsfingerprint"
	redisinfra "github.com/TokenFlux/TokenRouter/internal/infra/redis"
	infratelemetry "github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	catalogtest "github.com/TokenFlux/TokenRouter/internal/modelcatalog/testkit"
	"github.com/TokenFlux/TokenRouter/internal/moderation"
	routemoderation "github.com/TokenFlux/TokenRouter/internal/moderation/httpapi"
	moderationadapter "github.com/TokenFlux/TokenRouter/internal/moderation/provider"
	routenotification "github.com/TokenFlux/TokenRouter/internal/notification/httpapi"
	opscore "github.com/TokenFlux/TokenRouter/internal/ops"
	routeops "github.com/TokenFlux/TokenRouter/internal/ops/httpapi"
	paymenthttp "github.com/TokenFlux/TokenRouter/internal/payment/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/pkg/apperror"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/pkg/pagination"
	"github.com/TokenFlux/TokenRouter/internal/pkg/timezone"
	routepromotion "github.com/TokenFlux/TokenRouter/internal/promotion/httpapi"
	protocolcore "github.com/TokenFlux/TokenRouter/internal/protocol"
	protocolqoder "github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit/mockupstream"
	providercore "github.com/TokenFlux/TokenRouter/internal/provider"
	routeprovider "github.com/TokenFlux/TokenRouter/internal/provider/httpapi"
	provideradapter "github.com/TokenFlux/TokenRouter/internal/provider/provider"
	"github.com/TokenFlux/TokenRouter/internal/routing"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	routerouting "github.com/TokenFlux/TokenRouter/internal/routing/httpapi"
	routingdto "github.com/TokenFlux/TokenRouter/internal/routing/httpapi/dto"
	routingtestkit "github.com/TokenFlux/TokenRouter/internal/routing/testkit"
	"github.com/TokenFlux/TokenRouter/internal/scheduler"
	routescheduler "github.com/TokenFlux/TokenRouter/internal/scheduler/httpapi"
	"github.com/TokenFlux/TokenRouter/internal/scheduler/rediscache/codec"
	"github.com/TokenFlux/TokenRouter/internal/search"
	routesearch "github.com/TokenFlux/TokenRouter/internal/search/httpapi"
	serverhttp "github.com/TokenFlux/TokenRouter/internal/server/httpapi"
	servermiddleware "github.com/TokenFlux/TokenRouter/internal/server/middleware"
	settingscore "github.com/TokenFlux/TokenRouter/internal/settings"
	routesettings "github.com/TokenFlux/TokenRouter/internal/settings/httpapi"
	routesite "github.com/TokenFlux/TokenRouter/internal/site/httpapi"
	teamhttpapi "github.com/TokenFlux/TokenRouter/internal/team/httpapi"
	testassert "github.com/TokenFlux/TokenRouter/internal/testutil/assertion"
	claude "github.com/TokenFlux/TokenRouter/internal/upstream/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/upstream/antigravity"
	"github.com/TokenFlux/TokenRouter/internal/upstream/gemini"
	"github.com/TokenFlux/TokenRouter/internal/upstream/openai"
	openaiws "github.com/TokenFlux/TokenRouter/internal/upstream/openai/ws"
	"github.com/TokenFlux/TokenRouter/internal/upstream/qoder"
	"github.com/TokenFlux/TokenRouter/internal/usage"
	usagehttp "github.com/TokenFlux/TokenRouter/internal/usage/httpapi"
	routeusageadmin "github.com/TokenFlux/TokenRouter/internal/usage/httpapi/admin"
)

// helpers_test.go supplies HTTP assembly fixtures using production selection and
// forwarding rules while controlling repository results, credentials, and upstream
// responses. The fixtures bind those components through application providers, then expose
// narrowly implemented stores whose observations let tests distinguish provider
// selection, credential mutation, forwarding, and completion recording.
//
// Fixture copies have different ownership rules: some reads return shared
// pointers, others copy outer records, and execution-provider conversions use
// the provider module's supported-container copying. Authentication and
// scheduler tests depend on those distinctions when later mutations occur. Grok
// mutation fixtures compare credential and proxy snapshots before changing
// provider state, while membership adapters fill missing group data and retain
// existing memberships. Structured-log capture holds a process-wide lock until
// the caller restores its sink; moderation cleanup stops runtime workers before
// joining the separately registered background callbacks.

const (
	// labCellPass, labCellFail, and labCellSkip classify T5 gateway route and T6
	// client laboratory scenario cell evaluations across protocol lab suites.
	labCellPass = "pass" // scenario cell satisfied all protocol and accounting laws
	labCellFail = "fail" // scenario cell violated one or more validation rules
	labCellSkip = "skip" // scenario lacks applicable wire fixtures or usage semantics
)

// The fixture interface assertion verifies task submission compatibility, while
// handlerStructuredLogCaptureMu and handlerRefresherStarted coordinate
// process-wide logging capture and per-router cancellation observations.
var (
	// The assignment checks fixtureCyberTasks against the production Tasks interface
	// at compile time so moderation callback binding keeps its required signature.
	_ moderationflow.Tasks = fixtureCyberTasks{}

	// handlerStructuredLogCaptureMu is held from logger initialization
	// through sink restoration, serializing tests that replace the
	// process-wide structured log sink.
	handlerStructuredLogCaptureMu sync.Mutex
	// handlerRefresherStarted maps each fixture router to its refresh-entry
	// channel; cleanup removes the registration after requests finish so
	// later lookups cannot retain that router.
	handlerRefresherStarted sync.Map
)

// mcpSettingsFixture implements the setting operations used by OAuth configuration.
type mcpSettingsFixture struct {
	settingscore.Repository                   // Unused storage methods reject accidental calls.
	values                  map[string]string // Current persisted setting values.
	err                     error             // Injected storage read or write failure.
}

// messageExecutionFixture holds routing, session, cooldown, and completion
// inputs for message-compatible handlers.
type messageExecutionFixture struct {
	// Routes, Cache, Digest, Cooldown, and Recorder are retained component
	// references passed to message handler bindings; callers keep those
	// supplied components usable throughout request execution.
	Routes   *gatewayprovider.RoutePlanner // Group model route planner.
	Cache    session.GatewayCache          // Sticky session cache storage.
	Digest   *session.DigestSessionStore   // Gemini session digest storage.
	Cooldown *providercore.RetryCooldown   // Provider retry cooldown state.
	Recorder *completion.Recorder          // Attempt completion recording component.
}

// fixtureRetryStore adapts execution-provider rereads to the provider cooldown
// repository interface.
type fixtureRetryStore struct {
	// ExecutionProviderStore supplies provider rereads for cooldown; the
	// adapter overrides GetByID and inherits the remaining execution-store
	// methods from this supplied source.
	gatewayprovider.ExecutionProviderStore
}

// gatewayHTTPFixtureInput supplies mutable handler dependencies and
// provider-switch options for HTTP tests.
type gatewayHTTPFixtureInput struct {
	// Request credential execution component.
	Credentials *gatewayhttp.RequestCredentialExecutor
	// Model availability diagnostic component.
	Availability *gatewayModelAvailability
	// Compatible provider selection component.
	Choices *selection.Compatible
	// Native takes precedence over the OpenAI-only UnifiedTextExecutor that
	// the fixture otherwise constructs when binding each handler callback.
	Native  *gatewayhttp.UnifiedTextExecutor
	Generic *selection.Generic // Generic provider selection component.
	// Source supplies captured response and socket executors at
	// construction; replacing this pointer later does not replace those
	// captured execution components.
	Source *gatewayExecutionFixture
	// Funding admission check component.
	Funding *admission.FundingAdmission
	Keys    *apikey.APIKeyService // API key validation service.
	// Worker receives completion jobs when supplied; a nil worker selects
	// the submission helper's inline completion behavior for the fixture.
	Worker *completion.UsageRecordWorkerPool
	// Upstream error passthrough policy.
	Rules *errorpolicy.ErrorPassthroughService
	// Content moderation check component.
	Moderator *moderation.ContentModerationService
	// Ops and Queue enable cyber-policy Ops submission when both are
	// supplied; the fixture binds that pair together so each queued input
	// names its recording service.
	Ops *opscore.OpsService // Cyber-policy Ops recording service.
	// Cyber-policy Ops submission queue.
	Queue gatewayhttp.OpsErrorLogQueue
	// Config is read when callbacks build handler options and image
	// admission settings; tests serialize changes to the retained
	// configuration pointer.
	Config *config.Config
	// Request prompt transformation component.
	Prompts *promptpolicy.Service
	// Shared request concurrency helper.
	Concurrency *gatewayhttp.ConcurrencyHelper
	// Shared image capacity limiter.
	Images      *scheduler.ImageConcurrencyLimiter
	MaxSwitches int // Provider switch count budget.
	// Recorder overrides Source.Recorder when supplied; each callback
	// binding rereads this retained field before constructing completion
	// and cyber-policy support.
	Recorder *completion.Recorder
}

// gatewayHTTPEndpointsFixture exposes bound HTTP entry points and retained
// inputs for assembly tests.
type gatewayHTTPEndpointsFixture struct {
	// textHandler rebuilds the production receiver from retained fixture inputs.
	// Route tests register this receiver directly to exercise the full middleware chain.
	textHandler func() *gatewayhttp.OpenAITextHandler
	// Input points to the constructor's retained input copy; serial tests
	// change its options before invoking callbacks that rebuild handler
	// bindings.
	Input *gatewayHTTPFixtureInput
	// Responses HTTP handler entry.
	Responses gin.HandlerFunc
	// Messages HTTP handler entry.
	Messages gin.HandlerFunc
	// Chat Completions handler entry.
	ChatCompletions gin.HandlerFunc
	// Responses WebSocket handler entry.
	ResponsesWebSocket gin.HandlerFunc
	// Image request handler entry.
	Images gin.HandlerFunc
	// Grok video generation entry.
	GrokVideoGeneration gin.HandlerFunc
	// Grok video status entry.
	GrokVideoStatus gin.HandlerFunc
	// httpResources builds a fresh wrapper over retained concurrency and
	// image limiter pointers, so separate handler bindings share their
	// occupied capacity.
	httpResources func() *gatewayhttp.OpenAIHTTPResources
	// openAIAttemptSupport rebuilds attempt bindings from retained inputs,
	// including the current recorder choice used by direct error and
	// warning assertions.
	openAIAttemptSupport func() *openaiattempt.Support
	// rejectIfCyberSessionBlocked copies the supplied key before delegating
	// to the current cyber handler, exposing group-scoped session block
	// decisions to tests.
	rejectIfCyberSessionBlocked func(*gin.Context, *apikey.APIKey, []byte, string, gatewayhttp.CyberBlockFormat) bool
	// enqueueCyberSessionBlockedOpsEntry copies the supplied key and
	// delegates blocked-session recording through the current cyber handler
	// and retained Ops queue.
	enqueueCyberSessionBlockedOpsEntry func(*gin.Context, *apikey.APIKey, string, string)
}

// fixtureCyberTasks adapts a fixture background callback to moderation task
// submission.
type fixtureCyberTasks struct {
	// source supplies Background when configured; otherwise Go launches the
	// callback directly, leaving its completion ownership with the invoking
	// test.
	source *gatewayExecutionFixture
}

// fixtureCyberOps binds an Ops service and queue for cyber-policy recording.
type fixtureCyberOps struct {
	// service and queue are retained together after both fixture inputs are
	// supplied; Enqueue passes each cyber-policy input to queue with that
	// same service reference.
	service *opscore.OpsService          // Retained Ops recording service.
	queue   gatewayhttp.OpsErrorLogQueue // Retained Ops submission queue.
}

// httpFixtureBalances supplies fixed spendable balances to production
// funding-admission tests.
type httpFixtureBalances struct{}

// gatewayExecutionPricingConfigRows supplies pricing configurations and
// per-group platforms for route planning.
type gatewayExecutionPricingConfigRows struct {
	// PricingConfigRepository supplies unimplemented methods through
	// embedding; fixture consumers use only the configuration and
	// group-platform reads overridden below.
	routing.PricingConfigRepository

	// modelConfigs supplies outer copies through ListAll; nested maps and
	// slices stay shared with the fixture configurations and need stable
	// test inputs.
	modelConfigs   []routingtestkit.Configuration
	groupPlatforms map[int64]string // Stored group platform names.
}

// messageEndpointsFixture exposes Messages HTTP handling and group attempt
// preparation for tests.
type messageEndpointsFixture struct {
	// Messages HTTP handler entry.
	Messages gin.HandlerFunc
	// prepareGatewayAttemptRequest delegates group mapping to the same
	// retained route planner as the handlers, keeping request body and
	// mapped model together.
	prepareGatewayAttemptRequest func(context.Context, *requeststate.ParsedRequest, []byte, *apikey.APIKey, string) (*requeststate.ParsedRequest, routing.GroupMappingResult, error)
}

// fakeSchedulerCache supplies snapshots from stored provider pointers and fixed
// scheduler acknowledgments.
type fakeSchedulerCache struct {
	// providers supplies snapshot inputs converted through ExecutionRecord;
	// GetProvider skips nil entries and matches IDs, while GetSnapshot
	// converts every stored entry.
	providers []*gatewayprovider.ExecutionProvider
}

// fakeGroupRepo returns one shared group record and fixed repository
// acknowledgments.
type fakeGroupRepo struct {
	// group is returned directly for every requested ID by both group
	// lookups; tests keep this shared record stable during handler use.
	group *routing.Group
}

// fakeConcurrencyCache grants slot and waiter admission while returning zero
// load observations.
type fakeConcurrencyCache struct{}

// grokCredentialTokenReader converts fixture execution records for the
// production Grok token source.
type grokCredentialTokenReader struct {
	// source supplies the execution-provider rereads converted for Grok
	// token lookup; callers provide a usable repository before invoking
	// this adapter.
	source *grokCredentialHandlerRepo
}

// handlerInMemoryLogSink stores copied event records for concurrent
// structured-log assertions.
type handlerInMemoryLogSink struct {
	// mu protects events append and inspection. Stored events and their
	// Fields maps are copied before publication, while values nested within
	// Fields remain shared and need stable producer inputs.
	mu     sync.Mutex          // Event observation access lock.
	events []*logging.LogEvent // Captured structured event records.
}

// grokCredentialHandlerRepo records credential and health mutations while
// serving selectable provider fixtures.
type grokCredentialHandlerRepo struct {
	// ExecutionProviderStore supplies unimplemented methods through
	// embedding; tests use the selection, reread, and credential-state
	// mutation methods overridden by this fixture.
	gatewayprovider.ExecutionProviderStore

	// mu protects providers, setErrorIDs, setTempIDs, rateLimitIDs,
	// updateExtraIDs, selectionCalls, setErrorErr, setTempErr, and
	// missingOnGet while methods record attempts, inspect snapshots, or
	// mutate provider state.
	mu sync.Mutex // Provider fixture access lock.
	// providers holds records mutated under mu; returned outer copies keep
	// nested references unless GetByID copies the outer credentials map.
	providers      []gatewayprovider.ExecutionProvider
	setErrorIDs    []int64 // Attempted provider error updates.
	setTempIDs     []int64 // Attempted provider cooldown updates.
	rateLimitIDs   []int64 // Attempted provider rate limits.
	updateExtraIDs []int64 // Attempted provider extra updates.
	selectionCalls int     // Observed selection invocation count.
	// setErrorErr is returned after an error-update ID is observed and
	// before provider state changes; nil allows the scripted mutation to
	// proceed.
	setErrorErr error // configured error-update failure
	// setTempErr is returned after a cooldown-update ID is observed and
	// before expiry assignment; nil allows the scripted cooldown mutation
	// to proceed.
	setTempErr error // configured cooldown-update failure
	// missingOnGet makes selected IDs return nil during rereads even when
	// their records remain selectable, reproducing a row disappearing after
	// provider selection.
	missingOnGet map[int64]bool // missing reread provider identities
}

// grokCredentialHandlerTokenCache scripts token misses and configurable
// invalidation errors for refresh tests.
type grokCredentialHandlerTokenCache struct {
	// AccessTokenCache supplies unimplemented cache methods through
	// embedding; the fixture overrides token misses, invalidation, and
	// refresh lock acknowledgments used by recovery.
	providercore.AccessTokenCache
	// mu serializes DeleteAccessToken reads of deleteErr; tests initialize
	// deleteErr before refresh runs, and the fixture retains no token
	// entries or real lock ownership.
	mu sync.Mutex // Invalidation error access lock.
	// deleteErr controls the result of DeleteAccessToken for every key;
	// tests configure it before refresh to expose credential-cache cleanup
	// failure.
	deleteErr error // configured token invalidation error
}

// grokCredentialHandlerRefresher scripts OAuth failures and a cancellable
// refresh-entry signal.
type grokCredentialHandlerRefresher struct {
	mode string // Scripted token refresh mode.
	// started publishes refresh entry and once guards its closure in cancel
	// mode; repeated refresh calls share the closed entry signal while
	// still waiting on their own contexts.
	//
	// started must be initialized for cancel mode because closing a nil
	// entry channel panics before Refresh can observe ctx cancellation.
	started chan struct{} // refresh entry signal
	once    sync.Once     // Refresh entry signal guard.
}

// grokCredentialHandlerUpstream records request identities and returns scripted
// Grok failures or successful responses.
type grokCredentialHandlerUpstream struct {
	// UpstreamTransport supplies unimplemented methods through embedding;
	// ordinary and fingerprinted execution are overridden to return this
	// fixture's scripted responses.
	httpclient.
		UpstreamTransport
	// mu protects hits, requestURLs, authorization, failProviderID,
	// rateLimitIDs, failureStatus, and cancelRequest during Do's
	// observation and control snapshot. Tests use the same mutex when
	// changing controls after construction.
	mu            sync.Mutex // Upstream observation access lock.
	hits          []int64    // Observed provider request IDs.
	requestURLs   []string   // Observed upstream request URLs.
	authorization []string   // Observed Authorization header values.
	// failProviderID selects payment-required output for a matching
	// provider after rateLimitIDs and failureStatus overrides have been
	// considered for that request.
	failProviderID int64 // payment-failure provider identity
	// rateLimitIDs selects HTTP 429 for true entries before other scripted
	// failure controls; the response includes the fixture's sixty-second
	// Retry-After value.
	rateLimitIDs map[int64]bool // rate-limited provider identities
	// failureStatus selects a positive per-provider HTTP status after
	// rate-limit checks and before the payment-failure branch, allowing
	// mixed-status retry fixtures.
	failureStatus map[int64]int // configured provider response status
	// cancelRequest runs when the payment-failure branch is selected and a
	// callback is present, after controls have been snapshotted and mu
	// released.
	cancelRequest context.CancelFunc // payment-failure cancellation callback
}

// contentModerationHandlerSettingRepo implements mutable string settings for
// moderation and gateway fixtures.
type contentModerationHandlerSettingRepo struct {
	// values holds mutable runtime setting strings with unsynchronized map
	// access; tests finish configuration setup or serialize changes before
	// moderation reads.
	values map[string]string
}

// contentModerationHandlerTestRepo stores outer moderation records for
// synchronized test observations.
type contentModerationHandlerTestRepo struct {
	// mu protects logs and cyberWarnings append, reset, and snapshot reads.
	// Outer record copies preserve observations, while nested record values
	// remain shared with submitted inputs and need stable producers.
	mu sync.Mutex // Moderation record access lock.
	// Captured moderation log records.
	logs []moderation.ContentModerationLog
	// Captured cyber warning records.
	cyberWarnings []moderation.ContentModerationCyberWarning
}

// openAIHandlerTestWarningError exposes a forwarding cause and upstream warning
// through separate interfaces.
type openAIHandlerTestWarningError struct {
	// warning supplies upstream policy details and err supplies the
	// forwarding cause through separate interfaces; callers keep both
	// retained objects stable while error-output code inspects the fixture.
	// Attached upstream warning details.
	warning *forwardcore.UpstreamWarning
	// err supplies Error and Unwrap when present; a missing cause makes
	// Error return the fixed fixture message while warning inspection
	// remains separate.
	err error // retained forwarding error
}

// openAIWSFailoverHandlerProviderRepoStub supplies selectable providers and
// records their rate-limit reset updates.
type openAIWSFailoverHandlerProviderRepoStub struct {
	// ExecutionProviderStore supplies unimplemented methods through
	// embedding; fixture consumers use the selection, ID reread, and
	// rate-limit writes overridden below.
	gatewayprovider.ExecutionProviderStore

	// providers supplies selection and reread records, while rateLimitedIDs
	// records every attempted reset update. Both are mutated without
	// synchronization, so tests finish handler work before inspecting
	// fixture state.
	// Stored failover provider records.
	providers      []gatewayprovider.ExecutionProvider
	rateLimitedIDs []int64 // Observed rate-limit update IDs.
}

// opsErrorLogJob pairs an Ops service with its queued error-log input.
type opsErrorLogJob struct {
	// ops selects the recording service for entry; the sanitized entry
	// pointer and its service travel together as one captured submission
	// for later Ops assertions.
	ops *opscore.OpsService // Selected Ops recording service.
	// entry is the same input pointer sanitized before enqueue; callers
	// keep it stable while the captured job is queued or inspected.
	entry *opscore.OpsInsertErrorLogInput // sanitized error-log input
}

// captureOpsErrorQueue captures sanitized Ops submissions and admission
// counters for serial test assertions.
type captureOpsErrorQueue struct {
	// health accumulates admission counters for jobs, including drops and
	// sanitized inputs. Reading jobs does not decrement health.Length, and
	// tests submit and inspect this unsynchronized fixture serially.
	// Accumulated queue admission counters.
	health opscore.ErrorLogQueueHealth
	// jobs carries sanitized input pointers with their Ops service; Enqueue
	// uses nonblocking sends and the fixture leaves this channel open for
	// assertions.
	jobs chan opsErrorLogJob
}

// modelCatalogueEmptyPrices supplies an empty configuration read for model
// catalogue tests.
type modelCatalogueEmptyPrices struct {
	// PricingConfigRepository supplies unimplemented methods through
	// embedding; catalogue fixtures use only the empty ListAll read
	// overridden by this type.
	routing.PricingConfigRepository
}

// gatewayExecutionFixture retains assembled OpenAI and Grok execution,
// recording, and session components.
type gatewayExecutionFixture struct {
	// newOpenAIExecutionAndSelectionFixture sets Requests to Text.Requests
	// and binds Responses, WebSockets, Grok, and Auxiliary to that shared
	// execution setup. Hand-built fixtures supply these references themselves.
	// OpenAI text execution component.
	Text *gatewayhttp.OpenAITextExecutor
	// OpenAI request preparation component.
	Requests *gatewayhttp.OpenAIRequests
	// OpenAI Responses execution component.
	Responses *gatewayhttp.OpenAIResponsesExecutor
	// OpenAI WebSocket execution component.
	WebSockets *wshttp.OpenAIWebSocketExecutor
	// Grok protocol execution component.
	Grok *gatewayhttp.GrokExecutor
	// OpenAI auxiliary execution component.
	Auxiliary *gatewayhttp.OpenAIAuxiliary
	// Attempt completion recording component.
	Recorder *completion.Recorder
	Blocks   *session.CyberBlocks          // Cyber session block component.
	Cache    session.GatewayCache          // Shared gateway cache storage.
	Planner  *gatewayprovider.RoutePlanner // Group model route planner.
	// Background optionally controls task acceptance through
	// fixtureCyberTasks; when absent, that adapter launches a goroutine and
	// leaves completion ownership with the test.
	Background func(string, func()) bool
}

// mixedHTTPProviders serves copied provider records with stored group
// membership for mixed-platform selection.
type mixedHTTPProviders struct {
	// ExecutionProviderStore supplies unimplemented methods through
	// embedding; mixed-platform tests use the group-aware selection and
	// record rereads overridden by this fixture.
	gatewayprovider.ExecutionProviderStore
	// values stores complete group memberships; selection requires those
	// memberships, and NewExecutionProvider converts each returned record
	// using provider-module copying rules.
	values []gatewayprovider.ExecutionProvider
}

// routeTestAdminHandlers supplies administrator endpoint receivers to route
// registration and inventory tests.
type routeTestAdminHandlers struct {
	// Administrator API key routes.
	APIKey *keyhttp.AdminAPIKeyHandler[routingdto.Group]
	// Provider archive management routes.
	ProviderArchive *routeprovider.ArchiveHandler
	// Provider CRS route receiver.
	ProviderCRS *routeprovider.CRSHandler
	// Provider Codex import routes.
	ProviderCodexImport *routeprovider.CodexImportHandler
	// Provider record management routes.
	ProviderManagement *routeprovider.ManagementHandler
	// Provider OAuth usage routes.
	ProviderOAuthUsage *routeprovider.OAuthUsageHandler
	// Provider Ollama usage routes.
	ProviderOllama *routeprovider.OllamaUsageHandler
	// Provider execution test routes.
	ProviderTests *routeprovider.TestHandler
	// Referral rebate administration routes.
	Affiliate *routepromotion.AffiliateHandler
	// Site announcement administration routes.
	Announcement *routesite.AdminAnnouncementHandler
	// Antigravity OAuth administration routes.
	AntigravityOAuth *routeprovider.AntigravityOAuthHandler
	// Audit log administration routes.
	AuditLog *routeaudit.AuditLogHandler
	// Backup archive administration routes.
	Backup *routebackup.BackupHandler
	// Pricing configuration administration routes.
	PricingConfig *routerouting.PricingHandler
	// Codex invitation reset routes.
	CodexInviteReset *routeprovider.CodexInviteResetHandler
	// Content moderation administration routes.
	ContentModeration *routemoderation.ContentModerationHandler
	// Usage dashboard administration routes.
	Dashboard *routeusageadmin.DashboardHandler
	// Data maintenance administration routes.
	DataManagement *routebackup.DataManagementHandler
	// Error policy administration routes.
	ErrorPassthrough *gatewayhttp.ErrorPassthroughHandler
	// Gemini OAuth administration routes.
	GeminiOAuth *routeprovider.GeminiOAuthHandler
	// Grok OAuth administration routes.
	GrokOAuth *routeprovider.GrokOAuthHandler
	// Routing group administration routes.
	Group *routerouting.GroupHandler
	// Claude OAuth administration routes.
	OAuth *routeprovider.ClaudeOAuthHandler
	// OpenAI OAuth administration routes.
	OpenAIOAuth *routeprovider.OpenAIOAuthHandler
	// Ops diagnostics administration routes.
	Ops *routeops.OpsHandler
	// Payment order administration routes.
	Payment *paymenthttp.AdminHandler
	// Promotion code administration routes.
	Promo *routepromotion.PromoHandler
	// Egress proxy administration routes.
	Proxy *routeegress.ProxyHandler
	// Qoder OAuth administration routes.
	QoderOAuth *routeprovider.QoderOAuthHandler
	// Redemption code administration routes.
	Redeem *routebilling.AdminRedeemHandler
	// Scheduled provider test routes.
	ScheduledTest *routeprovider.ScheduledTestHandler
	// Provider scheduler diagnostic routes.
	SchedulerDiagnostics *routescheduler.DiagnosticsHandler
	// Subscription plan administration routes.
	Subscription *routebilling.AdminSubscriptionHandler
	// System runtime administration routes.
	System *routeops.SystemHandler
	// TLS profile administration routes.
	TLSFingerprintProfile *routeegress.TLSFingerprintProfileHandler
	// TLS router administration routes.
	TLSFingerprintRouter *routeegress.TLSFingerprintRouterHandler
	// Team membership administration routes.
	Team *teamhttpapi.AdminHandler
	// Upstream provider usage routes.
	UpstreamUsage *routeprovider.UpstreamUsageHandler
	// Usage record administration routes.
	Usage *routeusageadmin.UsageHandler
	// User account administration routes.
	User *routeidentity.AdminUserHandler[dto.APIKey[routingdto.Group]]
	// User attribute administration routes.
	UserAttribute *routeidentity.UserAttributeHandler
}

// routeTestHandlers supplies route receivers and flags that request default
// gateway HTTP fixtures.
type routeTestHandlers struct {
	// User API key routes.
	APIKey *keyhttp.APIKeyHandler[routingdto.Group]
	// Administrator route receiver collection.
	Admin *routeTestAdminHandlers
	// Public announcement route receiver.
	Announcement *routesite.AnnouncementHandler
	Auth         interface {
		// Authentication and session methods.
		routeidentity.AuthEndpoints
		// WeChat authentication route methods.
		paymenthttp.WeChatAuthEndpoints
	} // Authentication and WeChat endpoints.
	// AuxiliaryHTTP overrides the OpenAI auxiliary fixture; a nil receiver
	// is constructed from the shared media runtime when OpenAIEnabled is
	// true.
	AuxiliaryHTTP *gatewayhttp.AuxiliaryHandler
	// Batch image gateway receiver.
	BatchImage *batchhttp.BatchImageHandler
	// CountTokensHTTP overrides token counting; when it is nil and
	// TextEnabled is true, registration reads body limits from nonnil cfg
	// and constructs the unavailable-funding fixture.
	CountTokensHTTP *gatewayhttp.CountTokensHandler
	// Creative task route receiver.
	Creative *routecreative.CreativeHandler
	// TextEnabled constructs shared message activity and missing
	// text-related gateway receivers, allowing route tests to exercise
	// admission with minimal execution dependencies.
	TextEnabled bool
	// GeminiNativeHTTP overrides Gemini generation; a nil receiver is
	// constructed with shared message bindings and request activity when
	// TextEnabled is true.
	GeminiNativeHTTP *gatewayhttp.GeminiNativeHandler
	// LiveHTTP overrides the live gateway receiver; a nil receiver is
	// constructed with minimal application dependencies when OpenAIEnabled
	// is true.
	LiveHTTP *gatewayhttp.LiveHandler
	// MediaHTTP overrides OpenAI media entry points; a nil receiver is
	// constructed from the shared media runtime when OpenAIEnabled is true.
	MediaHTTP *gatewayhttp.MediaHandler
	// MessagesHTTP overrides Messages entry points; a nil receiver is
	// constructed with shared message bindings and request activity when
	// TextEnabled is true.
	MessagesHTTP *gatewayhttp.MessagesHandler
	// Public model marketplace receiver.
	ModelMarketplace *routerouting.MarketplaceHandler
	// ModelsHTTP overrides model listing; a nil receiver is constructed
	// with minimal model dependencies when TextEnabled is true for gateway
	// route registration.
	ModelsHTTP *gatewayhttp.ModelsHandler
	// Notification and unsubscribe receiver.
	Notification *routenotification.Handler
	// OpenAIEnabled constructs missing OpenAI text, WebSocket, media,
	// auxiliary, and live receivers for the route fixture using their
	// application providers.
	OpenAIEnabled bool
	// OpenAITextHTTP overrides OpenAI text entry points; a nil receiver is
	// constructed with shared OpenAI attempt bindings when OpenAIEnabled is
	// true.
	OpenAITextHTTP *gatewayhttp.OpenAITextHandler
	// OpenAITokensHTTP overrides OpenAI token counting; a nil receiver is
	// constructed unconditionally through the application provider before
	// gateway routes are registered.
	OpenAITokensHTTP *gatewayhttp.OpenAITokensHandler
	// Passkey authentication route receiver.
	Passkey *routeidentity.PasskeyHandler
	// User payment route receiver.
	Payment *paymenthttp.PaymentHandler
	// Payment callback route receiver.
	PaymentWebhook *paymenthttp.PaymentWebhookHandler
	// Public plan route receiver.
	Plans *routebilling.PlanHandler
	// Public settings route receiver.
	PublicSettings *routesite.PublicHandler
	// PublicUsage overrides the public gateway usage endpoint; a nil
	// receiver uses a minimal context-reading handler with the
	// process-local calendar.
	PublicUsage *usagehttp.PublicUsageHandler
	// User redemption route receiver.
	Redeem *routebilling.RedeemHandler
	// ResponsesWSHTTP overrides Responses WebSocket ingress; a nil receiver
	// is constructed with shared OpenAI attempt bindings when OpenAIEnabled
	// is true.
	ResponsesWSHTTP *wshttp.ResponsesWSHandler
	// Search settings route receiver.
	Search *routesearch.Handler
	// SearchHTTP overrides gateway search; a nil receiver is constructed
	// with empty search callbacks when TextEnabled is true for route
	// registration.
	SearchHTTP *gatewayhttp.SearchHandler
	// User subscription route receiver.
	Subscription *routebilling.SubscriptionHandler
	Team         *teamhttpapi.UserHandler // User team receiver.
	// User TOTP route receiver.
	Totp  *routeidentity.TotpHandler
	Usage *usagehttp.UsageHandler // User usage receiver.
	// User promotion route receiver.
	PromotionUser *routepromotion.UserHandler
	// User account route receiver.
	User *routeidentity.UserHandler
}

// protocolGateTrackingReader records attempted body reads before returning end
// of input.
type protocolGateTrackingReader struct {
	read bool // Body read observation flag.
}

// routeCountUnavailable supplies an unavailable funding check to token-count
// route fixtures.
type routeCountUnavailable struct{}

// selectionGroupFixture fills missing group membership in single-platform
// provider fixtures. Stored memberships are retained so tests can still reject
// a candidate belonging to another group.
type selectionGroupFixture struct {
	// ExecutionProviderStore supplies the single-platform reads adapted for
	// group selection; its other methods remain available through this
	// retained source.
	gatewayprovider.ExecutionProviderStore
	// mu protects groups writes during missing-membership completion and
	// reads during GetByID; only the remembered association is locked,
	// while the supplied store owns its own synchronization.
	mu sync.Mutex // Membership association access lock.
	// groups remembers the last filled membership by provider ID for
	// GetByID; selecting the same provider for another missing group
	// replaces that saved association.
	groups map[int64][]int64 // remembered provider memberships
}

// readerStoreProbe counts uncached settings lookups by returning a
// missing-setting result.
type readerStoreProbe struct {
	// Repository supplies unimplemented settings methods through embedding;
	// the probe overrides GetValue so tests can count uncached dynamic
	// setting reads.
	settingscore.Repository
	reads int // Observed uncached setting reads.
}

// settingHandlerRepoStub stores setting values and the last bulk update for
// handler assertions.
type settingHandlerRepoStub struct {
	// values stores mutable strings merged by SetMultiple and read by value
	// lookups; the fixture expects callers to serialize repository access.
	values map[string]string
	// lastUpdates is replaced with a new input copy on each SetMultiple
	// call, including an empty map for empty updates, so assertions observe
	// that call alone.
	lastUpdates map[string]string
}

// openCodeHTTPKeys supplies one credential to real gateway authentication.
type openCodeHTTPKeys struct {
	apikey.APIKeyRepository                // unused key management operations
	key                     *apikey.APIKey // configured authenticated key view
}

// openCodeHTTPExchange captures one local upstream request for synchronous assertions.
type openCodeHTTPExchange struct {
	path    string      // observed upstream request path
	headers http.Header // copied upstream request headers
	body    []byte      // consumed upstream request bytes
}

// mixedHTTPTransport records selected provider IDs before forwarding requests
// with the default HTTP client.
type mixedHTTPTransport struct {
	// failure optionally injects a transport error before network access. Set it
	// before serving requests; the callback keeps its captured state immutable.
	failure func(*http.Request, int64) error
	// mu protects providers appends and cloned observations; requests release
	// mu before network forwarding so call recording does not serialize HTTP exchanges.
	mu        sync.Mutex // Provider observation access lock.
	providers []int64    // Observed provider request order.
}

// heartbeatRouteOptions adjusts model, transport budgets, and telemetry hooks
// for keepalive and Gemini SDK route fixtures.
type heartbeatRouteOptions struct {
	FallbackURL   string                             // optional second provider URL
	Model         string                             // optional fixture model
	Stall         time.Duration                      // upstream idle budget
	HeaderTimeout time.Duration                      // upstream header budget
	Ops           *opscore.OpsService                // optional Ops logger service
	OpsQueue      gatewayhttp.OpsErrorLogQueue       // optional synchronous Ops capture queue
	OnTelemetry   func(infratelemetry.RequestRecord) // optional request record observer
}

// labProvider declares one provider candidate for newProtocolLabRouter, which
// fills missing IDs, names, auth types, and concurrency slots in buildProtocolLabProviderRecord.
type labProvider struct {
	ID               int64                     // optional explicit provider ID
	Name             string                    // optional provider display name
	Platform         string                    // capability platform identifier
	Type             string                    // capability provider auth type
	BaseURL          string                    // local upstream server base URL
	Model            string                    // single whitelisted model name
	Models           []string                  // whitelisted model identifiers
	ModelMapping     map[string]string         // optional client-to-upstream model mapping
	EnabledProtocols []protocolcore.ProtocolID // optional explicit upstream_protocols list
	Priority         int                       // scheduler priority (lower values win)
	Concurrency      int                       // maximum concurrent request slots
	Credentials      map[string]any            // optional extra credential entries
	Extra            map[string]any            // optional provider Extra settings
}

// labOptions configures providers, group protocol routing, and timeouts for
// newProtocolLabRouter.
type labOptions struct {
	Providers           []labProvider                                         // ordered provider candidates bound to the lab group
	AllowedProtocols    []protocolcore.ProtocolID                             // optional group allowed_protocols override
	ProtocolFallbacks   map[protocolcore.ProtocolID][]protocolcore.ProtocolID // optional group protocol_fallbacks map
	MaxProviderSwitches int                                                   // optional failover provider switch cap
	KeepaliveInterval   time.Duration                                         // downstream SSE keepalive ping interval
	StallTimeout        time.Duration                                         // downstream SSE data idle timeout
	HeaderTimeout       time.Duration                                         // upstream HTTP response header timeout
	Ops                 *opscore.OpsService                                   // optional Ops error service
	OpsQueue            gatewayhttp.OpsErrorLogQueue                          // optional Ops error capture queue
	OnTelemetry         func(infratelemetry.RequestRecord)                    // optional request record capture hook
	Pricing             map[string]*pricing.ModelPricing                      // optional model price overrides
	// HostOverrides redirects outbound HTTP requests matching lowercase target
	// hostnames to local test server URLs while keeping official vendor BaseURLs
	// intact on provider credentials.
	HostOverrides map[string]string // optional target hostname to local URL map
	// Settings seeds the in-memory runtime settings repository before router
	// assembly; tests can mutate values live afterward via SetSetting.
	Settings map[string]string // optional initial runtime setting values
	// ErrorRules supplies administrator error-passthrough rules bound across
	// Messages and OpenAI attempt runtimes when non-empty; nil leaves rules unset.
	ErrorRules []*errorpolicy.ErrorPassthroughRule // optional error-passthrough rules
	// QoderClient binds a caller-owned gateway/testkit QoderClient inside
	// protocolLabQoderClient when non-nil; nil allocates a fresh capture client.
	QoderClient *gatewaytestkit.QoderClient // optional Qoder capture client fixture
	// ModelAttributeRules seeds active group ModelAttributeRule overrides for
	// group 42 when non-empty; nil leaves group overrides absent until SetModelAttributeRules runs.
	ModelAttributeRules []routing.ModelAttributeRule // optional initial group model-attribute rules
}

// labCellResult records the evaluated outcome and violated rule identifiers of
// one T5 gateway route or T6 client laboratory cell.
type labCellResult struct {
	Client   protocolcore.ProtocolID // client-facing wire protocol
	Upstream protocolcore.ProtocolID // upstream-facing wire protocol
	Scenario string                  // evaluated canonical or synthetic scenario slug
	Law      string                  // evaluated T5 or T6 law identifier
	Status   string                  // labCellPass, labCellFail, or labCellSkip
	// Rules lists deduplicated violated rule or error codes when Status is
	// labCellFail; passing and skipped cells leave Rules empty.
	Rules []string
}

// protocolLabRouter bundles the assembled Gin gateway engine with in-memory
// session, turn-origin, provider, and usage-recording handles for T5 and T6 tests.
type protocolLabRouter struct {
	*gin.Engine // embedded HTTP handler for httptest.NewServer

	Origins *session.CodexTurnOrigins // shared Codex turn-state ownership tracker
	Cache   *protocolLabGatewayCache  // in-memory sticky session and owner cache
	APIKey  string                    // bearer credential accepted by lab routes

	// groupMu serializes concurrent reads and dynamic replacements of group
	// across HTTP request admission, group policy lookups, and multi-turn
	// provider or fallback reconfigurations.
	groupMu           sync.RWMutex                         // active routing group lock
	group             *routing.Group                       // active routing group configuration
	settings          *protocolLabSettingsStore            // thread-safe runtime settings repository
	attributeRepo     *protocolLabAttributeRepo            // thread-safe group model-attribute profile repository
	tokenCache        *protocolLabAccessTokenCache         // in-memory OAuth access-token cache
	antigravityTokens *providercore.AntigravityTokenSource // assembled Antigravity OAuth token source
	providers         *protocolLabProviderStore            // mutable in-memory provider repository
	blocks            *providercore.RuntimeBlockState      // shared OpenAI and Grok runtime block state
	modelTransient    *providercore.ModelTransientState    // shared OpenAI model-transient cooldown state
	feedback          *scheduler.RuntimeStats              // shared in-memory EWMA scheduling feedback tracker
	usageStore        *protocolLabUsageStore               // thread-safe captured UsageLog store
	settlements       *protocolLabSettlementStore          // thread-safe captured billing command store
	qoderClient       *protocolLabQoderClient              // Qoder stream client bound to lab pool
	qoderTokens       *provideradapter.QoderTokenProvider  // in-memory Qoder session cache
	unified           *gatewayhttp.UnifiedTextExecutor     // assembled unified text executor
	wsExecutor        *wshttp.OpenAIWebSocketExecutor      // assembled Responses WebSocket executor
	countTokens       *gatewayhttp.CountTokensHandler      // assembled Messages count_tokens handler
}

// protocolLabTransport executes outbound upstream HTTP requests through a real
// httpclient.UpstreamPool with a bounded response-header timeout.
type protocolLabTransport struct {
	httpclient.UpstreamTransport // unimplemented interface methods remain unused
	// pool owns local HTTP keep-alive connections and closes idle sockets in test cleanup.
	pool          *httpclient.UpstreamPool  // real outbound connection pool
	headerTimeout time.Duration             // response header wait budget
	providers     *protocolLabProviderStore // optional provider store for local endpoint overrides
	hostOverrides map[string]string         // optional lowercase target hostname to local URL overrides
}

// protocolLabNoSearch disables external web searchers while keeping search-tool
// emulation wired as in production.
type protocolLabNoSearch struct{}

// protocolLabKeyRepo supplies the active laboratory APIKey and cloned group
// policies to APIKeyService.GetByKey and Reauthenticate across multi-turn
// Responses WebSocket sessions.
type protocolLabKeyRepo struct {
	apikey.APIKeyRepository                    // unused management methods remain nil
	lab                     *protocolLabRouter // owning lab router holding APIKey and live group
	keyID                   int64              // authenticated API key identifier
	userID                  int64              // authenticated user identifier
	groupID                 int64              // bound routing group identifier
	user                    *identity.User     // active user summary with positive balance
}

// protocolLabGatewayCache implements session.GatewayCache in memory with mutex
// synchronization.
type protocolLabGatewayCache struct {
	// mu serializes concurrent reads, SetNX ownership claims, and writes across
	// sessionBindings and ownerGroups during parallel HTTP requests and stream
	// completions.
	mu              sync.Mutex       // cache map access lock
	sessionBindings map[string]int64 // sticky provider IDs by group and hash
	ownerGroups     map[string]int64 // owning group IDs by user, source, and hash
}

// protocolLabGroupRepo serves cloned snapshots of protocolLabRouter's active
// routing group to generic and Gemini selectors.
type protocolLabGroupRepo struct {
	routing.GroupRepository                    // unused repository mutation methods
	router                  *protocolLabRouter // owning lab router holding groupMu and group
}

// protocolLabProviderStore serves thread-safe provider queries, model-list
// catalogue reads, and live health or rate-limit mutations in memory.
type protocolLabProviderStore struct {
	gatewayprovider.ExecutionProviderStore // unused persistence methods

	// mu serializes concurrent candidate reads, retry cooldown lookups, live
	// health or cooldown mutations, and SetProviders replacements of values
	// across parallel gateway requests.
	mu     sync.RWMutex                        // provider slice and health lock
	values []gatewayprovider.ExecutionProvider // active execution providers with live health state
}

// protocolLabUsageStore records deep-copied usage.UsageLog entries across both
// Forward and OpenAI completion recorders under a mutex.
type protocolLabUsageStore struct {
	usage.UsageLogRepository // unused query methods

	// mu serializes concurrent completion appends to logs, ResetUsageLogs slice
	// clears, and detached snapshot reads from UsageLogs across Forward and
	// OpenAI recorders.
	mu   sync.Mutex       // settled usage slice lock
	logs []usage.UsageLog // chronological settled usage logs
}

// protocolLabSettlementStore records cloned billing.UsageBillingCommand entries
// and synthesizes applied balance or subscription outcomes under a mutex.
type protocolLabSettlementStore struct {
	completion.Store // unused persistence methods

	// mu serializes concurrent settlement appends to commands during Apply and
	// slice resets of commands during ResetUsageLogs across parallel HTTP
	// completion paths.
	mu       sync.Mutex                    // billing command slice lock
	commands []billing.UsageBillingCommand // chronological applied billing commands
}

// protocolLabQoderClient records every submitted Qoder payload through a bound
// gateway/testkit.QoderClient and dispatches per-provider HTTP streams when a
// provider BaseURL is configured, or returns an in-memory SSE stream when empty.
type protocolLabQoderClient struct {
	pool    *httpclient.UpstreamPool // shared outbound HTTP connection pool
	timeout time.Duration            // response header timeout
	// mu, capture, baseURLBySession, and fallbackBaseURL coordinate thread-safe
	// request capture and per-provider endpoint lookup across StreamRequestContext,
	// StreamRequestContextWithDoer, SetProviders, and protocolLabRouter Qoder
	// capture accessors.
	mu               sync.Mutex                       // capture and endpoint map lock
	capture          *gatewaytestkit.QoderClient      // underlying gateway/testkit Qoder capture fixture
	baseURLBySession map[*qoder.SessionContext]string // provider-scoped Qoder upstream base URLs
	fallbackBaseURL  string                           // first configured Qoder base URL fallback
}

// protocolLabAttributeRepo implements routing.ModelAttributeRepository in
// memory under RWMutex synchronization so runtime resolution observes live
// group ModelAttributeRule overrides without PostgreSQL.
type protocolLabAttributeRepo struct {
	routing.ModelAttributeRepository // unused administrative persistence methods

	groupID int64 // owning laboratory group identifier
	// mu and rules synchronize concurrent ForGroup and ForGroups profile reads
	// against initial seeding in newProtocolLabAttributeRepo and live slice
	// replacements in setRules when callers invoke protocolLabRouter.SetModelAttributeRules.
	mu    sync.RWMutex                 // rule slice access lock
	rules []routing.ModelAttributeRule // active group model-attribute rules
}

// protocolLabSettingsStore implements settingscore.Repository in memory with
// RWMutex synchronization so runtime readers observe live test setting updates.
type protocolLabSettingsStore struct {
	settingscore.Repository // unused repository methods

	// mu serializes concurrent runtime lookups of values in Get, GetValue,
	// GetMultiple, and GetAll against live writes to values in Set,
	// SetMultiple, and Delete.
	mu     sync.RWMutex      // setting map access lock
	values map[string]string // stored runtime setting key-value entries
}

// protocolLabCachedToken pairs an OAuth access token with its expiration deadline.
type protocolLabCachedToken struct {
	token string // cached OAuth bearer access token
	// expiresAt bounds token validity in GetAccessToken; a zero Time leaves
	// the cached bearer token valid without expiration.
	expiresAt time.Time // UTC expiration deadline, or zero when unbounded
}

// protocolLabAccessTokenCache implements providercore.AccessTokenCache in
// memory with mutex synchronization across token entries and refresh locks.
type protocolLabAccessTokenCache struct {
	// mu serializes concurrent GetAccessToken, SetAccessToken, DeleteAccessToken,
	// AcquireRefreshLock, ReleaseRefreshLock, and Reset operations across both
	// tokens and locks maps during parallel provider credential lookups.
	mu     sync.Mutex                        // cache and refresh-lock access lock
	tokens map[string]protocolLabCachedToken // cached access tokens keyed by provider token hash
	locks  map[string]time.Time              // active refresh lock deadlines keyed by cache key
}

// protocolLabAntigravityRequest captures the outer v1internal request envelope
// and unwrapped inner Gemini JSON payload sent to a local Antigravity upstream.
type protocolLabAntigravityRequest struct {
	Path        string      // incoming HTTP URL path such as /v1internal:streamGenerateContent
	RawQuery    string      // incoming raw URL query string such as alt=sse
	Headers     http.Header // cloned incoming HTTP headers including Authorization
	RawEnvelope []byte      // verbatim outer v1internal JSON request payload
	Project     string      // outer v1internal GCP project identifier
	Model       string      // outer v1internal target model identifier
	RequestID   string      // outer v1internal agent request correlation ID
	UserAgent   string      // outer v1internal userAgent string
	RequestType string      // outer v1internal requestType string
	InnerBody   []byte      // verbatim inner Gemini GenerateContent JSON object bytes
}

// protocolLabAntigravityUpstream adapts a scripted mockupstream.Handler to the
// Antigravity v1internal wire contract by unwrapping incoming {"project",
// "model", "request"} envelopes and wrapping outgoing Gemini payloads into
// {"response": ...} envelopes.
type protocolLabAntigravityUpstream struct {
	mock *mockupstream.Handler // underlying scripted Gemini validator and responder

	// mu serializes concurrent ServeHTTP and recordEnvelopeError appends to
	// envelopes and envelopeErrors against Envelopes, Violations,
	// AssertNoViolations, and Reset reads or clears from test goroutines.
	mu             sync.Mutex                      // envelope log access lock
	envelopes      []protocolLabAntigravityRequest // chronological captured v1internal envelopes
	envelopeErrors []string                        // outer v1internal envelope validation errors
}

// protocolLabHijackRecorder buffers an HTTP response from mockupstream.Handler
// in memory while recording whether Fault.TruncateAfterFrames requested socket
// hijacking to simulate an abrupt mid-stream disconnect.
type protocolLabHijackRecorder struct {
	// header, body, status, wroteHeader, and hijacked capture one synchronous
	// mockupstream ServeHTTP exchange: wroteHeader latches the first committed
	// status across WriteHeader, Write, and Flush, while hijacked tells the
	// outer v1internal handler to close the live TCP socket after wrapping body.
	header      http.Header  // buffered HTTP response header map
	body        bytes.Buffer // buffered HTTP response body bytes
	status      int          // committed HTTP status code, or default 200 before WriteHeader
	wroteHeader bool         // whether WriteHeader has already latched status
	hijacked    bool         // whether Hijack was invoked to truncate an SSE stream
}

// GetValue reads one fixture setting and distinguishes absence from storage failure.
// Missing keys return the production absence sentinel, allowing the assembled
// browser-origin reader to skip unconfigured URLs while keeping saved addresses.
func (s *mcpSettingsFixture) GetValue(_ context.Context, key string) (string, error) {
	if s.err != nil {
		return "", s.err
	}
	if value, ok := s.values[key]; ok {
		return value, nil
	}
	return "", settingscore.ErrSettingNotFound
}

// newGenericExecutionAndSelectionFixture binds generic selection and Messages
// forwarding to the supplied repositories and runtime components. A provider
// store enables cooldown rereads and error writes; absent settings leave search
// settings unset. Window prefetch is enabled only when both cost cache and
// usage repository are supplied. It returns separate execution inputs, choices,
// and forwarding components for callers to bind and record.
func newGenericExecutionAndSelectionFixture(
	providerRepo gatewayprovider.ExecutionProviderStore,
	groupRepo routing.GroupRepository,
	usageLogRepo usage.UsageLogRepository,
	cache session.GatewayCache,
	cfg *config.Config,
	schedulerSnapshot *scheduler.SnapshotService,
	concurrencyService *scheduler.ConcurrencyService,
	healthObserver *provideradapter.UpstreamHealth,
	identityService *claude.RequestFingerprint,
	httpUpstream httpclient.UpstreamTransport,
	deferredService *providercore.DeferredService,
	messageCredentials *providercore.MessageCredentialSource,
	sessionLimitCache scheduler.SessionLimitCache,
	windowCostCache billing.WindowCostCache,
	rpmCache scheduler.RPMCache,
	digestStore *session.DigestSessionStore,
	settingService *gatewayprovider.RuntimeReaders,
	tlsFPProfileService *egressadapter.TLSProfiles,
	pricingConfigService *routing.PricingConfigService,
	resolver *billing.PriceResolver,
	headerFilter *egress.CompiledHeaderFilter,
) (*messageExecutionFixture, *selection.Generic, *gatewayhttp.MessagesExecutor) {
	var retryStore providercore.RetryCooldownStore
	if providerRepo != nil {
		retryStore = fixtureRetryStore{providerRepo}
	}
	source := &messageExecutionFixture{
		Routes: gatewayprovider.NewRoutePlanner(pricingConfigService), Cache: cache, Digest: digestStore,
		Cooldown: providercore.NewRetryCooldown(retryStore, providercore.RetryCooldownOptions{}),
	}
	feedback := scheduler.NewRuntimeStats(time.Now)
	window := billing.NewWindowCostGuard(windowCostCache, gatewaytestkit.WindowCosts(usageLogRepo), billing.WindowCostGuardOptions{Now: time.Now, Stats: billing.SharedWindowCostMetrics(), Log: func(format string, args ...any) {
		logging.LegacyPrintf("service.gateway", format, args...)
	}, Debug: slog.Debug})
	var write func(context.Context, int64, string) error
	if providerRepo != nil {
		write = providerRepo.SetError
	}
	choices := selection.NewGeneric(selection.GenericDependencies{
		Reads: selection.Reads{
			Providers: withSelectionGroupFixture(providerRepo),
			Groups:    groupRepo,
			Snapshot:  provideSelectionSnapshots(schedulerSnapshot),
		},
		Shared: selection.Shared{
			Cache:         cache,
			Concurrency:   concurrencyService,
			Health:        healthObserver,
			GroupPolicies: pricingConfigService,

			Feedback: feedback,
		},
		Window:                  window,
		WindowPrefetchAvailable: windowCostCache != nil && usageLogRepo != nil,
		RPM:                     rpmCache,

		Sessions:         sessionLimitCache,
		SetProviderError: write,
	}, selectionOptions(cfg))
	var searchSettings *search.ConfigService
	if settingService != nil {
		searchSettings = settingService.Search
	}
	searchTools := ProvideGatewaySearchTools(searchSettings, pricingConfigService)
	messages := provideMessagesExecution(messageCredentials, identityService, httpUpstream, healthObserver, tlsFPProfileService, settingService, resolver, searchTools, nil, nil, providerRepo, deferredService, cfg, headerFilter, pricingConfigService)
	return source, choices, messages
}

// GetByID rereads an execution provider and converts it to a provider record
// through ExecutionRecord. The repository error passes through; nil records
// follow that conversion helper's nil handling.
func (s fixtureRetryStore) GetByID(ctx context.Context, id int64) (*providercore.Record, error) {
	value, err := s.ExecutionProviderStore.GetByID(ctx, id)
	return gatewayprovider.ExecutionRecord(value), err
}

// newOpenAIImageChatRejectionHandlerWithPricingConfig constructs the
// image-rejection fixture with the supplied group model mapping. It binds
// completion recording and default concurrency hooks so mapped-model validation
// can be inspected before provider execution.
func newOpenAIImageChatRejectionHandlerWithPricingConfig(t *testing.T, pricingConfigService *routing.PricingConfigService) *gatewayHTTPEndpointsFixture {
	t.Helper()
	gatewayService, gatewayServiceChoices, _ := newOpenAIExecutionAndSelectionFixture(
		nil, nil, nil, nil, nil, nil,
		nil, nil, nil, newOpenAIExecutionCredentialsForTest(nil,
			nil), nil, nil, pricingConfigService, nil, nil, responseHeaderFilterForTest(nil), nil, nil, nil,
	)
	gatewayService.Recorder = newHTTPCompletionFixture(nil, nil, nil,
		nil, nil, pricingConfigService, nil, true)

	return newOpenAIImageChatRejectionHandlerWithService(t, &httptestkit.ConcurrencyHooks{}, gatewayService, newExecutionAvailabilityForTest(nil,

		pricingConfigService, nil), gatewayServiceChoices,
	)
}

// newOpenAIImageChatRejectionHandlerWithService constructs a Chat rejection
// fixture from supplied execution and selection dependencies. Funding and key
// components are minimal, while the caller's concurrency hooks provide
// admission observations.
func newOpenAIImageChatRejectionHandlerWithService(t *testing.T, cache *httptestkit.ConcurrencyHooks, gatewayService *gatewayExecutionFixture, availability *gatewayModelAvailability, choices *selection.Compatible) *gatewayHTTPEndpointsFixture {
	t.Helper()

	return newGatewayHTTPEndpoints(gatewayHTTPFixtureInput{
		Source: gatewayService, Availability: availability, Choices: choices,
		Funding: &admission.FundingAdmission{},
		Keys:    &apikey.APIKeyService{},
		Concurrency: gatewayhttp.NewConcurrencyHelper(scheduler.NewConcurrencyService(cache, scheduler.Diagnostics{
			Logf:  logging.LegacyPrintf,
			Event: logging.Event,
		},
		), gatewayhttp.SSEPingFormatNone, time.Second),
	})
}

// newHTTPCompletionFixture binds a recorder to supplied logs, pricing, funding
// cache, activity, and health observers. A nil config selects multiplier one;
// otherwise the configured multiplier is used, and openAI chooses the recording
// mode.
func newHTTPCompletionFixture(cfg *config.Config, logs usage.UsageLogRepository, calculator *billing.Calculator, eligibility *billing.Eligibility, activity completion.ProviderActivity, modelConfigs *routing.PricingConfigService, health completion.HealthObserver, openAI bool) *completion.Recorder {
	f := gatewaytestkit.NewRecording(logs, &gatewaytestkit.SettlementStore{}, nil, false)
	f.Dependencies.Calculator = calculator
	f.Dependencies.Health = health
	f.GroupPolicies = modelConfigs
	f.Effects.Funds.Cache = eligibility
	f.Effects.Activity = activity
	f.Options.DefaultMultiplier = 1
	if cfg != nil {
		f.Options.DefaultMultiplier = cfg.Billing.FallbackRateMultiplier
	}
	return f.Core(nil, openAI)
}

// Go submits work through Source.Background when that callback is configured,
// returning its acceptance result. Otherwise it launches fn directly in a
// goroutine and reports acceptance, leaving completion ownership with the test.
func (t fixtureCyberTasks) Go(name string, fn func()) bool {
	if t.source != nil && t.source.Background != nil {
		return t.source.Background(name, fn)
	}
	// When no source callback owns admission, run the task independently
	// and leave its completion observation with the invoking test.
	go fn()
	return true
}

// Enqueue submits the input with the retained Ops service through the retained
// queue. Callers provide a usable queue; sanitization, admission, and
// observation follow that queue's implementation.
func (w fixtureCyberOps) Enqueue(value *opscore.OpsInsertErrorLogInput) {
	w.queue.Enqueue(w.service, value)
}

// newGatewayHTTPEndpoints builds handler callbacks around a retained copy of
// input and fills missing execution pieces on a supplied Source. A nil Source
// leaves execution interfaces absent for admission-rejection tests; media
// callbacks require a Source when invoked.
//
// Resources and handler bindings are rebuilt at callback use, sharing the
// supplied concurrency and image limiters while reading mutable fixture
// options. Response and socket executors are captured during construction;
// replacing Source later does not replace those captured executors. An explicit
// input Recorder takes precedence over Source.Recorder, and an explicit Native
// executor takes precedence over the OpenAI-only executor assembled here. The
// fixture exposes its retained Input for serial test adjustment.
func newGatewayHTTPEndpoints(input gatewayHTTPFixtureInput) *gatewayHTTPEndpointsFixture {
	// Admission rejection tests leave Source nil, so their execution interfaces remain unset.
	if input.Source != nil && input.Source.Text == nil {
		// Fill the minimal request executor on the retained Source so
		// early rejection fixtures can bind handlers while still
		// lacking usable upstream dependencies.
		input.Source.Text = &gatewayhttp.OpenAITextExecutor{Requests: &gatewayhttp.OpenAIRequests{}, CodexUsage: &provideradapter.CodexUsageObserver{}}
		input.Source.Requests = input.Source.Text.Requests
	}

	var responses *gatewayhttp.OpenAIResponsesExecutor
	var sockets *wshttp.OpenAIWebSocketExecutor
	planner := gatewayprovider.NewRoutePlanner(nil)
	var cache session.GatewayCache
	if input.Source != nil {
		// Give a missing Responses executor and its socket executor the
		// same lineage store so continuation validation observes the
		// fixture's shared response ownership.
		if input.Source.Responses == nil {
			input.Source.Responses = &gatewayhttp.OpenAIResponsesExecutor{Requests: input.Source.Requests, Text: input.Source.Text, Lineage: &gatewayhttp.OpenAIEncryptedLineage{Store: session.NewOpenAIWSStateStore(input.Source.Cache, gatewayprovider.LogOpenAIWSModeInfo), TTL: func() time.Duration { return time.Hour }}}
		}
		if input.Source.WebSockets == nil {
			input.Source.WebSockets = &wshttp.OpenAIWebSocketExecutor{OpenAIWSDependencies: wshttp.OpenAIWSDependencies{
				State: input.Source.Responses.Lineage.Store,
			}}
		}
		responses, sockets, cache = input.Source.Responses, input.Source.WebSockets, input.Source.Cache
		if input.Source.Planner != nil {
			planner = input.Source.Planner
		}
	}
	var images *gatewayhttp.OpenAIImagesExecutor
	if input.Source != nil {
		images = provideOpenAIImages(input.Source.Text, &gatewayRequestActivity{Operations: lifecycle.NewOperations("image-execution-fixture")})
	}
	f := &gatewayHTTPEndpointsFixture{Input: &input}
	resources := func() *gatewayhttp.OpenAIHTTPResources {
		return &gatewayhttp.OpenAIHTTPResources{Concurrency: input.Concurrency, Images: input.Images, ImageOptions: openAIImageAdmissionOptions(input.Config)}
	}
	base := func() (openaiattempt.Bindings, *gatewayhttp.CyberHandler, *session.CyberBlocks) {
		recorder := input.Recorder
		var blocks *session.CyberBlocks
		if input.Source != nil {
			// Use the Source recorder when the explicit input is absent
			// so warning support and handler completion resolve the same
			// recorder choice.
			if recorder == nil {
				recorder = input.Source.Recorder
			}
			blocks = input.Source.Blocks
		}
		var moderator gatewayhttp.ModerationPort
		if input.Moderator != nil {
			moderator = input.Moderator
		}
		runtime := moderationflow.Runtime{Recorder: recorder, Blocks: blocks, Tasks: fixtureCyberTasks{input.Source}}
		if input.Ops != nil && input.Queue != nil {
			runtime.Ops = fixtureCyberOps{input.Ops, input.Queue}
		}
		cyber := gatewayhttp.NewBoundCyberHandler(blocks, moderator, runtime)
		unified := input.Native
		// Bind the captured OpenAI Responses executor when no
		// cross-platform executor is supplied, keeping ordinary OpenAI
		// fixtures usable through unified text entry points.
		if unified == nil {
			unified = &gatewayhttp.UnifiedTextExecutor{
				OpenAI: responses,
			}
		}
		bindings := provideOpenAIAttemptBindings(responses, input.Keys, resources(), cyber, input.Rules, input.Moderator, GatewayCompletionRecorders{OpenAI: recorder}, input.Worker, input.Availability, input.Choices, unified, input.Generic, input.Funding, nil, planner, cache)
		return bindings, cyber, blocks
	}
	text := func() *gatewayhttp.OpenAITextHandler {
		common, cyber, _ := base()
		options := openAITextOptions(input.Config)
		options.MaxSwitches = input.MaxSwitches
		bindings := openAITextBindings(responses, input.Funding, input.Keys, resources(), cyber, input.Rules, input.Moderator, planner, cache, nil)
		executor := textflow.NewResponsesExecutor(provideOpenAITextAttemptRuntime(common), textflow.ResponseOptions{MaxSwitches: input.MaxSwitches}, textflow.ResponseOptions{MaxSwitches: input.MaxSwitches, FirstOutputBudget: true})
		return gatewayhttp.NewBoundOpenAITextHandler(options, bindings, input.Prompts, executor)
	}
	ws := func() *wshttp.ResponsesWSHandler {
		common, _, blocks := base()
		options := responsesWSOptions(input.Config)
		options.MaxProviderSwitches = input.MaxSwitches
		return wshttp.New(options, responsesWSBindings(sockets, input.Credentials, input.Funding, input.Keys, common, input.Prompts, blocks, input.Choices, planner, nil))
	}
	media := func() *mediaentry.Runtime {
		common, _, _ := base()
		bindings := mediaBindings(responses, input.Credentials, input.Keys, input.Funding, common, resources(), nil, input.Config, input.Source.Grok, provideGrokVideoTasks(nil, input.Config), input.Source.Auxiliary, images, planner, cache, nil)
		bindings.Options.MaxSwitches = input.MaxSwitches
		bindings.EligibilityProber = nil
		return mediaentry.New(bindings)
	}
	f.textHandler = text
	f.Responses = func(c *gin.Context) { text().Responses(c) }
	f.Messages = func(c *gin.Context) { text().Messages(c) }
	f.ChatCompletions = func(c *gin.Context) { text().ChatCompletions(c) }
	f.ResponsesWebSocket = func(c *gin.Context) { ws().ResponsesWebSocket(c) }
	f.Images = func(c *gin.Context) { media().MediaHTTPHandler().Images(c) }
	f.GrokVideoGeneration = func(c *gin.Context) { media().MediaHTTPHandler().GrokVideoGeneration(c) }
	f.GrokVideoStatus = func(c *gin.Context) { media().MediaHTTPHandler().GrokVideoStatus(c) }
	f.httpResources = resources
	f.openAIAttemptSupport = func() *openaiattempt.Support { common, _, _ := base(); return common.Support }
	f.rejectIfCyberSessionBlocked = func(c *gin.Context, key *apikey.APIKey, body []byte, model string, format gatewayhttp.CyberBlockFormat) bool {
		_, cyber, _ := base()
		return cyber.RejectSession(c, apikey.CopyAPIKey(key), body, model, format)
	}
	f.enqueueCyberSessionBlockedOpsEntry = func(c *gin.Context, key *apikey.APIKey, model, block string) {
		_, cyber, _ := base()
		cyber.EnqueueBlocked(c, apikey.CopyAPIKey(key), model, block)
	}
	return f
}

// newGatewayHTTPEndpointsFromDeps assembles handler inputs with shared
// concurrency and image resources. The first provided resource object is used
// when nonnil; otherwise application providers construct it, and additional
// variadic objects are ignored.
func newGatewayHTTPEndpointsFromDeps(source *gatewayExecutionFixture, credentials *gatewayhttp.RequestCredentialExecutor, concurrency *scheduler.ConcurrencyService, funding *admission.FundingAdmission, keys *apikey.APIKeyService, worker *completion.UsageRecordWorkerPool, rules *errorpolicy.ErrorPassthroughService, moderator *moderation.ContentModerationService, opsService *opscore.OpsService, cfg *config.Config, prompts *promptpolicy.Service, availability *gatewayModelAvailability, choices *selection.Compatible, provided ...*gatewayhttp.OpenAIHTTPResources) *gatewayHTTPEndpointsFixture {
	var resources *gatewayhttp.OpenAIHTTPResources
	if len(provided) > 0 {
		resources = provided[0]
	}
	if resources == nil {
		resources = provideOpenAIHTTPResources(concurrency, cfg)
	}
	return newGatewayHTTPEndpoints(gatewayHTTPFixtureInput{Source: source, Credentials: credentials, Availability: availability, Choices: choices, Funding: funding, Keys: keys, Worker: worker, Rules: rules, Moderator: moderator, Ops: opsService, Config: cfg, Prompts: prompts, Concurrency: resources.Concurrency, Images: resources.Images, MaxSwitches: openAITextOptions(cfg).MaxSwitches})
}

// newFundingAdmissionFixture binds the supplied funding eligibility service
// with an absent RPM backend. The cfg argument is unused; service configuration
// and lifecycle remain with the funding fixture supplied by the caller.
func newFundingAdmissionFixture(funds *billing.Eligibility, cfg *config.Config) *admission.FundingAdmission {
	return admission.NewFundingAdmission(funds, nil)
}

// billingEligibilityFixtureOptions reads reserve and circuit-breaker settings
// from the supplied config. The config must be nonnil when this helper runs,
// including later reads through the eligibility fixture's options callback.
func billingEligibilityFixtureOptions(c *config.Config) billing.EligibilityOptions {
	return billing.EligibilityOptions{Billing: billing.BillingOptions{MinimumBalanceReserve: c.Billing.MinimumBalanceReserve, CircuitBreaker: billing.CircuitBreakerOptions{Enabled: c.Billing.BalanceCheckCircuitBreaker.Enabled, FailureThreshold: c.Billing.BalanceCheckCircuitBreaker.FailureThreshold, ResetTimeoutSeconds: c.Billing.BalanceCheckCircuitBreaker.ResetTimeoutSeconds, HalfOpenRequests: c.Billing.BalanceCheckCircuitBreaker.HalfOpenRequests}}}
}

// newBillingEligibilityFixture constructs eligibility with fixed balance reads
// and a callback that rereads cfg. Callers supply a nonnil configuration, start
// worker processing when needed, and stop started workers after producers have
// finished.
func newBillingEligibilityFixture(cfg *config.Config) *billing.Eligibility {
	// The injected runner launches independent goroutines; eligibility Stop
	// joins its worker queue separately, so tests finish callback producers
	// before stopping the fixture.
	return billing.NewEligibility(nil, httpFixtureBalances{}, nil, func() billing.EligibilityOptions { return billingEligibilityFixtureOptions(cfg) }, nil, func(_ string, fn func()) { go fn() })
}

// GetByID returns the requested user ID with a fixed balance of 1000.
func (httpFixtureBalances) GetByID(_ context.Context, id int64) (*billing.UserSummary, error) {
	return &billing.UserSummary{ID: id, Balance: 1000}, nil
}

// ListAll returns a new slice containing outer pricing configuration copies.
// Nested maps and slices remain shared; nil stored input produces a nonnil
// empty result because allocation uses its length.
func (s *gatewayExecutionPricingConfigRows) ListAll(ctx context.Context) ([]routingtestkit.Configuration, error) {
	modelConfigs := make([]routingtestkit.Configuration, len(s.modelConfigs))
	copy(modelConfigs, s.modelConfigs)
	return modelConfigs, nil
}

// GetGroupPlatforms builds a new map for requested IDs present in the fixture.
// Stored platform strings pass through verbatim, including blanks, and unknown
// groups are omitted from the result.
func (s *gatewayExecutionPricingConfigRows) GetGroupPlatforms(ctx context.Context, groupIDs []int64) (map[int64]string, error) {
	platforms := make(map[int64]string, len(groupIDs))
	for _, groupID := range groupIDs {
		if platform, ok := s.groupPlatforms[groupID]; ok {
			platforms[groupID] = platform
		}
	}
	return platforms, nil
}

// newGatewayExecutionHandlerWithPricingConfigForTest binds message-compatible
// handlers to the provider store and group pricing configuration. It uses
// default body limits with zero switch budgets and supplies model-availability
// diagnostics from the same inputs.
func newGatewayExecutionHandlerWithPricingConfigForTest(repo gatewayprovider.ExecutionProviderStore, pricingConfigService *routing.PricingConfigService) *messageEndpointsFixture {
	source, choices, messages := newGenericExecutionAndSelectionFixture(repo, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, pricingConfigService, nil, responseHeaderFilterForTest(nil))
	return newMessageEndpointsFixture(source, messages, nil, nil, gatewayhttp.MessagesHTTPOptions{MaxBodyBytes: openAITextOptions(nil).MaxBodyBytes, MaxSwitches: 0, MaxGeminiSwitches: 0}, newExecutionAvailabilityForTest(repo, pricingConfigService, nil), choices)
}

// newGatewayExecutionPricingConfigServiceForTest replaces the configuration's
// group IDs with the supplied group and binds its platform. The service
// receives that outer configuration value; nested configuration data retains
// the fixture's copying rules.
func newGatewayExecutionPricingConfigServiceForTest(groupID int64, platform string, pricingConfig routingtestkit.Configuration) *routing.PricingConfigService {
	pricingConfig.GroupIDs = []int64{groupID}
	repo := &gatewayExecutionPricingConfigRows{
		modelConfigs:   []routingtestkit.Configuration{pricingConfig},
		groupPlatforms: map[int64]string{groupID: platform},
	}
	return routingtestkit.NewPricingConfigService(repo, nil, routing.PricingConfigOptions{
		Warn: slog.Warn,
		Now:  time.Now, LoadLocation: pricingprovider.
			LoadPricingLocation,
	},
	)
}

// newMessageEndpointsFixture binds Messages and compatible text handlers to the
// supplied selection, forwarding, and completion components. Nil execution
// inputs permit early validation tests, while route planning and execution
// require source with Routes and usable generic choices.
//
// Messages uses partial-failure completion, whereas compatible Responses and
// Chat enable cancellation stopping. The returned preparation callback
// delegates group mapping to the same route planner used by the handlers,
// keeping mapped model and request data associated with their attempt. Optional
// availability diagnostics are bound only when supplied. Completion submission
// runs inline because the worker pool is absent.
func newMessageEndpointsFixture(source *messageExecutionFixture, messages *gatewayhttp.MessagesExecutor, funding *admission.FundingAdmission, concurrency *gatewayhttp.ConcurrencyHelper, options gatewayhttp.MessagesHTTPOptions, availability *gatewayModelAvailability, choices *selection.Generic) *messageEndpointsFixture {
	plan := func(ctx context.Context, key *apikey.APIKey, model string) routing.RoutePlan {
		var group *routing.Group
		var id *int64
		if key != nil {
			group, id = key.Group, key.GroupID
		}
		return source.Routes.PlanRoute(ctx, group, id, model)
	}
	b := textattempt.Bindings{
		PlanRoute: plan, Concurrency: concurrency,
		Submission: gatewayhttp.NewCompletionSubmission(nil, false),
	}
	if availability != nil {
		b.Diagnoser = availability.Messages
	}
	if source != nil {
		b.Recorder = source.Recorder
		b.Selection = textattempt.SelectionPorts{
			SelectProvider:      choices.SelectProviderWithLoadAwareness,
			TrackSession:        choices.TrackSessionAttempt,
			NewSessionAttempts:  choices.NewSessionAttempts,
			SingleProviderGroup: choices.IsSingleAntigravityProviderGroup,
			ReportSchedule:      choices.ReportAdvancedProviderScheduleResult,
			IncrementRPM:        choices.IncrementProviderRPM,
			BindSticky:          choices.BindStickySession,
			CachedSession:       choices.GetCachedSessionProviderID,
			ProviderSwitched:    choices.RecordAdvancedProviderSwitch,
			TempUnschedule:      messageRetryCooldown(source.Cooldown),
		}
		b.Forward = textattempt.ForwardPorts{
			SaveGeminiSession: messageDigestSave(source.Digest),
		}
	}
	if messages != nil {
		b.Forward.BedrockCompat = messages.ApplyBedrockCCCompat
		b.Forward.ForwardMessages = messages.Forward
	}
	var settings *gateway.RuntimeSettings
	bindings := gatewayhttp.MessagesBindings{
		PlanRoute: plan, ClientVersions: settings.GetClaudeCodeVersionBounds, Funding: funding,
		ObserveCompatibility: func(log *zap.Logger) {
			gatewayhttp.LogCompatibilityFallback(log, func() gatewayhttp.CompatibilityLogSnapshot { return gatewayhttp.CompatibilityLogSnapshot{} })
		},
	}
	if source != nil {
		bindings.IsolateSession = messageSessionIsolation(source.Cache)
		bindings.CachedSession = choices.GetCachedSessionProviderID
	}
	runtime := textattempt.New(b)
	var prompts *promptpolicy.Service
	messageHandler := gatewayhttp.NewBoundMessagesHandler(options, bindings, prompts, concurrency, textflow.NewMessagesExecutor(runtime, textflow.MessageOptions{MaxSwitches: options.MaxSwitches, CompletePartialFailure: true, Observe: telemetry.Failover}, textflow.MessageOptions{MaxSwitches: options.MaxGeminiSwitches, Observe: telemetry.Failover}))
	return &messageEndpointsFixture{
		Messages: messageHandler.Messages,
		prepareGatewayAttemptRequest: func(ctx context.Context, parsed *requeststate.ParsedRequest, body []byte, key *apikey.APIKey, model string) (*requeststate.ParsedRequest, routing.GroupMappingResult, error) {
			return gatewayhttp.PrepareGroupAttempt(ctx, parsed, body, key, model, plan)
		},
	}
}

// GetSnapshot converts every stored provider through ExecutionRecord and
// codec.WrapRecord and reports a cache hit. A nil provider slice returns nil
// with a hit, so absence of fixture data is not a cache miss.
func (f *fakeSchedulerCache) GetSnapshot(_ context.Context, _ scheduler.SchedulerBucket) ([]scheduler.SnapshotProvider, bool, error) {
	if f.providers == nil {
		return nil, true, nil
	}
	values := make([]scheduler.SnapshotProvider, len(f.providers))
	for i, value := range f.providers {
		values[i] = codec.WrapRecord(gatewayprovider.ExecutionRecord(value))
	}
	return values, true, nil
}

// CaptureBucketWriteToken returns an epoch-one token for the requested bucket.
func (f *fakeSchedulerCache) CaptureBucketWriteToken(_ context.Context, bucket scheduler.SchedulerBucket) (scheduler.SchedulerBucketWriteToken, error) {
	return scheduler.SchedulerBucketWriteToken{Bucket: bucket, Epoch: 1}, nil
}

// SetSnapshot acknowledges the write without storing providers or checking
// tokens.
func (f *fakeSchedulerCache) SetSnapshot(_ context.Context, _ scheduler.SchedulerBucket, _ scheduler.SchedulerBucketWriteToken, _ []scheduler.SnapshotProvider) error {
	return nil
}

// RetireBucket acknowledges retirement without changing this fixture's snapshot
// data.
func (f *fakeSchedulerCache) RetireBucket(_ context.Context, _ scheduler.SchedulerBucket) error {
	return nil
}

// ReopenBucket returns an epoch-one token for the requested bucket.
func (f *fakeSchedulerCache) ReopenBucket(_ context.Context, bucket scheduler.SchedulerBucket) (scheduler.SchedulerBucketWriteToken, error) {
	return scheduler.SchedulerBucketWriteToken{Bucket: bucket, Epoch: 1}, nil
}

// TryAcquireGroupLifecycleLease declines every claim with an empty lease value.
func (f *fakeSchedulerCache) TryAcquireGroupLifecycleLease(_ context.Context, _ int64, _ time.Duration) (scheduler.SchedulerGroupLifecycleLease, bool, error) {
	return scheduler.SchedulerGroupLifecycleLease{}, false, nil
}

// ReleaseGroupLifecycleLease acknowledges release without tracking any fixture
// lease state.
func (f *fakeSchedulerCache) ReleaseGroupLifecycleLease(_ context.Context, _ scheduler.SchedulerGroupLifecycleLease) error {
	return nil
}

// GetProvider converts the matching nonnil provider through ExecutionRecord and
// codec.WrapRecord. Missing IDs return nil without error, and nil entries are
// skipped during the fixture lookup.
func (f *fakeSchedulerCache) GetProvider(_ context.Context, id int64) (scheduler.SnapshotProvider, error) {
	for _, provider := range f.providers {
		if provider != nil && provider.Record.ID == id {
			return codec.WrapRecord(gatewayprovider.ExecutionRecord(provider)), nil
		}
	}
	return nil, nil
}

// SetProvider acknowledges a provider write without modifying stored snapshot
// pointers.
func (f *fakeSchedulerCache) SetProvider(_ context.Context, _ scheduler.SnapshotProvider) error {
	return nil
}

// DeleteProvider acknowledges deletion without removing any stored fixture
// provider.
func (f *fakeSchedulerCache) DeleteProvider(_ context.Context, _ int64) error { return nil }

// UpdateLastUsed acknowledges timestamps without changing stored fixture
// provider records.
func (f *fakeSchedulerCache) UpdateLastUsed(_ context.Context, _ map[int64]time.Time) error {
	return nil
}

// TryLockBucket grants every lock request without retaining bucket ownership.
func (f *fakeSchedulerCache) TryLockBucket(_ context.Context, _ scheduler.SchedulerBucket, _ time.Duration) (bool, error) {
	return true, nil
}

// UnlockBucket acknowledges release without changing any fixture lock state.
func (f *fakeSchedulerCache) UnlockBucket(_ context.Context, _ scheduler.SchedulerBucket) error {
	return nil
}

// ListBuckets returns an empty bucket inventory for this snapshot fixture.
func (f *fakeSchedulerCache) ListBuckets(_ context.Context) ([]scheduler.SchedulerBucket, error) {
	return nil, nil
}

// GetOutboxWatermark returns a fixed zero watermark for this fixture.
func (f *fakeSchedulerCache) GetOutboxWatermark(_ context.Context) (int64, error) { return 0, nil }

// SetOutboxWatermark acknowledges writes without retaining the supplied
// watermark value.
func (f *fakeSchedulerCache) SetOutboxWatermark(_ context.Context, _ int64) error { return nil }

// Create acknowledges group creation without changing this fixture's stored
// record.
func (f *fakeGroupRepo) Create(context.Context, *routing.Group) error { return nil }

// GetByID returns the fixture's shared group pointer regardless of the
// requested ID. Nil stored input returns nil without error; callers serialize
// mutations of the returned shared record.
func (f *fakeGroupRepo) GetByID(context.Context, int64) (*routing.Group, error) {
	return f.group, nil
}

// GetByIDLite returns the same shared group pointer as the full fixture lookup.
// It ignores the requested ID and leaves copy or synchronization requirements
// with callers that mutate the record.
func (f *fakeGroupRepo) GetByIDLite(context.Context, int64) (*routing.Group, error) {
	return f.group, nil
}

// Update acknowledges a group write without replacing the stored fixture
// record.
func (f *fakeGroupRepo) Update(context.Context, *routing.Group) error { return nil }

// Delete acknowledges group deletion without clearing the stored fixture
// pointer.
func (f *fakeGroupRepo) Delete(context.Context, int64) error { return nil }

// DeleteCascade returns no affected IDs and leaves the stored group available.
func (f *fakeGroupRepo) DeleteCascade(context.Context, int64) ([]int64, error) { return nil, nil }

// List returns no group records, pagination metadata, or repository error.
func (f *fakeGroupRepo) List(context.Context, pagination.PaginationParams) ([]routing.Group, *pagination.PaginationResult, error) {
	return nil, nil, nil
}

// ListWithFilters returns no records or pagination metadata for every filter.
func (f *fakeGroupRepo) ListWithFilters(context.Context, pagination.PaginationParams, string, string, string, *bool) ([]routing.Group, *pagination.PaginationResult, error) {
	return nil, nil, nil
}

// ListActive returns no active group records for this fixture.
func (f *fakeGroupRepo) ListActive(context.Context) ([]routing.Group, error) { return nil, nil }

// ListActiveByPlatform returns no active records regardless of platform input.
func (f *fakeGroupRepo) ListActiveByPlatform(context.Context, string) ([]routing.Group, error) {
	return nil, nil
}

// ListActiveByPlatformLite delegates the fixed empty active-group result by
// platform.
func (f *fakeGroupRepo) ListActiveByPlatformLite(ctx context.Context, platform string) ([]routing.Group, error) {
	return f.ListActiveByPlatform(ctx, platform)
}

// ExistsByName reports that every requested group name is absent.
func (f *fakeGroupRepo) ExistsByName(context.Context, string) (bool, error) { return false, nil }

// GetProviderCount returns zero values for both fixture provider counts.
func (f *fakeGroupRepo) GetProviderCount(context.Context, int64) (int64, int64, error) {
	return 0, 0, nil
}

// DeleteProviderGroupsByGroupID reports zero deleted memberships without
// changing fixture data.
func (f *fakeGroupRepo) DeleteProviderGroupsByGroupID(context.Context, int64) (int64, error) {
	return 0, nil
}

// GetProviderIDsByGroupIDs returns no provider IDs for the requested groups.
func (f *fakeGroupRepo) GetProviderIDsByGroupIDs(context.Context, []int64) ([]int64, error) {
	return nil, nil
}

// BindProvidersToGroup acknowledges binding without retaining any provider
// membership data.
func (f *fakeGroupRepo) BindProvidersToGroup(context.Context, int64, []int64) error { return nil }

// UpdateSortOrders acknowledges updates without storing any fixture group
// ordering.
func (f *fakeGroupRepo) UpdateSortOrders(context.Context, []routing.GroupSortOrderUpdate) error {
	return nil
}

// AcquireProviderSlot grants every provider claim without tracking occupied
// slots.
func (f *fakeConcurrencyCache) AcquireProviderSlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}

// ReleaseProviderSlot acknowledges release without tracking any provider slot
// occupancy.
func (f *fakeConcurrencyCache) ReleaseProviderSlot(context.Context, int64, string) error { return nil }

// GetProviderConcurrency reports zero occupied slots for every provider ID.
func (f *fakeConcurrencyCache) GetProviderConcurrency(context.Context, int64) (int, error) {
	return 0, nil
}

// IncrementProviderWaitCount grants waiter admission without retaining a
// provider count.
func (f *fakeConcurrencyCache) IncrementProviderWaitCount(context.Context, int64, int) (bool, error) {
	return true, nil
}

// DecrementProviderWaitCount acknowledges removal without changing any fixture
// waiter count.
func (f *fakeConcurrencyCache) DecrementProviderWaitCount(context.Context, int64) error { return nil }

// GetProviderWaitingCount reports zero waiting requests for every provider ID.
func (f *fakeConcurrencyCache) GetProviderWaitingCount(context.Context, int64) (int, error) {
	return 0, nil
}

// AcquireUserSlot grants every user claim without tracking occupied slots.
func (f *fakeConcurrencyCache) AcquireUserSlot(context.Context, int64, int, string) (bool, error) {
	return true, nil
}

// ReleaseUserSlot acknowledges release without tracking any user slot
// occupancy.
func (f *fakeConcurrencyCache) ReleaseUserSlot(context.Context, int64, string) error { return nil }

// GetUserConcurrency reports zero occupied slots for every fixture user.
func (f *fakeConcurrencyCache) GetUserConcurrency(context.Context, int64) (int, error) { return 0, nil }

// IncrementWaitCount grants user waiter admission without retaining a count.
func (f *fakeConcurrencyCache) IncrementWaitCount(context.Context, int64, int) (bool, error) {
	return true, nil
}

// DecrementWaitCount acknowledges removal without changing any user waiter
// count.
func (f *fakeConcurrencyCache) DecrementWaitCount(context.Context, int64) error { return nil }

// GetProvidersLoadBatch returns a nonnil empty provider load map for every
// request.
func (f *fakeConcurrencyCache) GetProvidersLoadBatch(context.Context, []scheduler.ProviderWithConcurrency) (map[int64]*scheduler.ProviderLoadInfo, error) {
	return map[int64]*scheduler.ProviderLoadInfo{}, nil
}

// GetUsersLoadBatch returns a nonnil empty user load map for every request.
func (f *fakeConcurrencyCache) GetUsersLoadBatch(context.Context, []scheduler.UserWithConcurrency) (map[int64]*scheduler.UserLoadInfo, error) {
	return map[int64]*scheduler.UserLoadInfo{}, nil
}

// GetProviderConcurrencyBatch allocates a map with zero occupancy for every
// requested provider ID. Duplicate IDs share one map entry, and empty input
// still returns a nonnil map for fixture consumers.
func (f *fakeConcurrencyCache) GetProviderConcurrencyBatch(_ context.Context, providerIDs []int64) (map[int64]int, error) {
	result := make(map[int64]int, len(providerIDs))
	for _, id := range providerIDs {
		result[id] = 0
	}
	return result, nil
}

// CleanupExpiredProviderSlots acknowledges cleanup without storing or expiring
// provider slots.
func (f *fakeConcurrencyCache) CleanupExpiredProviderSlots(context.Context, int64) error { return nil }

// CleanupExpiredProviderSlotKeys acknowledges cleanup without tracking any
// provider slot keys.
func (f *fakeConcurrencyCache) CleanupExpiredProviderSlotKeys(context.Context) error { return nil }

// CleanupStaleProcessSlots acknowledges cleanup without tracking any
// process-owned fixture slots.
func (f *fakeConcurrencyCache) CleanupStaleProcessSlots(context.Context, string) error { return nil }

// AcquireBucketLease wraps a successful TryLockBucket result in a release
// handle calling UnlockBucket. The caller releases the returned lease; denied
// or failed lock acquisition passes through without a handle.
func (f *fakeSchedulerCache) AcquireBucketLease(ctx context.Context, bucket scheduler.SchedulerBucket, ttl time.Duration) (*scheduler.BucketLease, bool, error) {
	ok, err := f.TryLockBucket(ctx, bucket, ttl)
	if err != nil || !ok {
		return nil, ok, err
	}
	return scheduler.NewBucketLease(func(cleanup context.Context) error { return f.UnlockBucket(cleanup, bucket) }), true, nil
}

// GetByID converts a provider reread from the retained Grok fixture repository
// through ExecutionRecord. The source must be usable, and its error passes
// through with the converted record.
func (r grokCredentialTokenReader) GetByID(ctx context.Context, id int64) (*providercore.Record, error) {
	v, err := r.source.GetByID(ctx, id)
	return gatewayprovider.ExecutionRecord(v), err
}

// newHTTPModeration constructs moderation with a local audit client and
// registers cleanup with the test. Callers start its runtime when worker
// processing is needed; Stop failures become test errors.
//
// Runtime workers and injected Background callbacks have separate joins. The
// helper registers background.Wait first and Stop second, so reverse cleanup
// order seals and joins runtime work before waiting for callbacks. Background
// uses WaitGroup.Go to register each callback, allowing cleanup to observe
// callbacks accepted during runtime shutdown. Tests finish synchronous
// producers before cleanup because the wait does not itself prevent later
// callback submission.
func newHTTPModeration(t *testing.T, settings moderation.SettingRepository, repo moderation.ContentModerationRepository) *moderation.ContentModerationService {
	t.Helper()
	var background sync.WaitGroup
	// Cleanup runs in reverse order; register the callback join first so
	// Stop can finish runtime workers before the separate Background wait
	// begins.
	t.Cleanup(background.Wait)
	core := moderation.NewContentModerationService(settings, repo, nil, nil, nil, nil, nil, moderation.Runtime{
		Audit:         moderationadapter.NewAuditClient(),
		SnapshotMedia: moderationadapter.SnapshotMedia,
		Background:    func(_ string, fn func()) bool { background.Go(fn); return true },
		CyberText:     openai.IsOpenAICyberWarningText,
		CyberPolicy:   openai.DetectOpenAICyberPolicy,
		MissingRow:    func(err error) bool { return errors.Is(err, sql.ErrNoRows) },
		MissingUser:   func(err error) bool { return errors.Is(err, identity.ErrUserNotFound) },
	})
	t.Cleanup(func() {
		if err := core.Stop(); err != nil {
			t.Errorf("停止审核运行时: %v", err)
		}
	})
	return core
}

// newOpenAIExecutionCredentialsForTest binds provider rereads and the supplied
// Grok token source when present. Nil inputs leave their callbacks unset,
// letting credential tests distinguish an absent capability from an
// unsuccessful repository read.
func newOpenAIExecutionCredentialsForTest(repo gatewayprovider.ExecutionProviderStore, grok *providercore.GrokTokenSource) *providercore.OpenAIExecutionCredentials {
	out := &providercore.OpenAIExecutionCredentials{}
	if repo != nil {
		out.Parent = func(ctx context.Context, id int64) (*providercore.Record, error) {
			value, err := repo.GetByID(ctx, id)
			return gatewayprovider.ExecutionRecord(value), err
		}
	}
	if grok != nil {
		out.Grok = grok.GetAccessToken
	}
	return out
}

// WriteLogEvent stores an outer event copy and a separate Fields map before
// appending under the sink mutex. Nil events are ignored; mutable values nested
// inside Fields remain shared with the caller.
func (s *handlerInMemoryLogSink) WriteLogEvent(event *logging.LogEvent) {
	if event == nil {
		return
	}
	// Copy the outer event and field map before publication so later caller
	// replacement of field entries cannot change a captured log
	// observation.
	cloned := *event
	if event.Fields != nil {
		cloned.Fields = make(map[string]any, len(event.Fields))
		maps.Copy(cloned.Fields, event.Fields)
	}
	// Publish the copied event under the reader mutex so message and field
	// assertions see a complete appended record while other events arrive.
	s.mu.Lock()
	s.events = append(s.events, &cloned)
	s.mu.Unlock()
}

// ContainsMessageAtLevel searches stored message substrings under the sink
// mutex with trimmed, case-insensitive level comparison. Nil events are
// skipped, and an empty substring matches any message at the requested level.
func (s *handlerInMemoryLogSink) ContainsMessageAtLevel(substr, level string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	wantLevel := strings.ToLower(strings.TrimSpace(level))
	for _, ev := range s.events {
		if ev == nil {
			continue
		}
		if strings.Contains(ev.Message, substr) && strings.ToLower(strings.TrimSpace(ev.Level)) == wantLevel {
			return true
		}
	}
	return false
}

// ContainsFieldValue searches a named field's fmt.Sprint representation for the
// requested substring under the mutex. Missing fields and nil events are
// skipped; an empty substring matches any present field value.
func (s *handlerInMemoryLogSink) ContainsFieldValue(field, substr string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ev := range s.events {
		if ev == nil || ev.Fields == nil {
			continue
		}
		if v, ok := ev.Fields[field]; ok && strings.Contains(fmt.Sprint(v), substr) {
			return true
		}
	}
	return false
}

// FieldValueForMessage returns the first present field on an event whose
// message matches exactly. A present nil value returns nil with true, while
// missing matches return nil with false; nested values retain shared ownership.
func (s *handlerInMemoryLogSink) FieldValueForMessage(message, field string) (any, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, event := range s.events {
		if event == nil || event.Message != message || event.Fields == nil {
			continue
		}
		if value, ok := event.Fields[field]; ok {
			return value, true
		}
	}
	return nil, false
}

// captureHandlerStructuredLog installs a debug JSON logger and a fresh event
// sink while holding the global capture mutex. The caller must invoke the
// returned restoration function once to remove the sink and release that mutex.
//
// Captures serialize process-wide logger mutation for their full observation
// interval, so another fixture cannot replace the sink while assertions are
// still reading it. Restoration clears the sink; it does not restore the prior
// logger configuration. Initialization failure stops the test before the
// returned cleanup function is available.
func captureHandlerStructuredLog(t *testing.T) (*handlerInMemoryLogSink, func()) {
	t.Helper()
	// Hold the capture mutex until restoration so another test cannot
	// replace the global sink while this test is still observing handler
	// events.
	handlerStructuredLogCaptureMu.Lock()

	err := logging.Init(logging.InitOptions{
		Level:       "debug",
		Format:      "json",
		ServiceName: "tokenrouter",
		Environment: "test",
		Output: logging.OutputOptions{
			ToStdout: true,
			ToFile:   false,
		},
		Sampling: logging.SamplingOptions{Enabled: false},
	})
	require.NoError(t, err)

	sink := &handlerInMemoryLogSink{}
	logging.SetSink(sink)
	return sink, func() {
		logging.SetSink(nil)
		handlerStructuredLogCaptureMu.Unlock()
	}
}

// ListSchedulableByPlatform increments the selection counter and returns outer
// provider copies under the fixture mutex. It applies platform and
// IsSchedulable checks; empty platform admits every matching schedulable
// fixture provider.
func (r *grokCredentialHandlerRepo) ListSchedulableByPlatform(_ context.Context, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	// Count selection and collect its candidates under one lock so
	// credential mutations cannot split a locked reader's count observation
	// from this provider view.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.selectionCalls++
	out := make([]gatewayprovider.ExecutionProvider, 0, len(r.providers))
	for _, provider := range r.providers {
		if (platform == "" || provider.Record.Platform == platform) && provider.View().IsSchedulable() {
			out = append(out, provider)
		}
	}
	return out, nil
}

// ListSchedulableByGroupIDAndPlatform delegates schedulability and platform
// filtering while ignoring the group ID. It increments the same selection
// counter and returns the delegate's outer-copy provider values.
func (r *grokCredentialHandlerRepo) ListSchedulableByGroupIDAndPlatform(ctx context.Context, _ int64, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

// ListSchedulableUngroupedByPlatform delegates schedulability and platform
// filtering without checking membership. Selection counting and shared nested
// provider references follow ListSchedulableByPlatform, allowing the
// group-completion wrapper to supply test memberships.
func (r *grokCredentialHandlerRepo) ListSchedulableUngroupedByPlatform(ctx context.Context, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	return r.ListSchedulableByPlatform(ctx, platform)
}

// GetByID returns an outer provider copy with a separate outer credentials map
// while holding the fixture mutex. A scripted missing row or unknown ID returns
// nil; nested credential values and other provider references remain shared.
func (r *grokCredentialHandlerRepo) GetByID(_ context.Context, id int64) (*gatewayprovider.ExecutionProvider, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.missingOnGet[id] {
		return nil, nil
	}
	for _, provider := range r.providers {
		if provider.Record.ID == id {
			copy := provider
			// Isolate edits to the outer credential map in the
			// returned record while retaining nested values,
			// matching the refresh fixture's shallow
			// credential-copy behavior.
			copy.Record.Credentials = cloneCredentialMap(provider.Record.Credentials)
			return &copy, nil
		}
	}
	return nil, nil
}

// SetError records the attempted provider ID before returning a configured
// write error or updating matching records. Successful updates set error
// status, disable scheduling, and store the message under the fixture mutex.
func (r *grokCredentialHandlerRepo) SetError(_ context.Context, id int64, message string) error {
	// Keep attempted-ID recording and error-state changes under one lock so
	// locked readers cannot observe a successful mutation without its
	// recorded attempt.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setErrorIDs = append(r.setErrorIDs, id)
	if r.setErrorErr != nil {
		return r.setErrorErr
	}
	for i := range r.providers {
		if r.providers[i].Record.ID == id {
			r.providers[i].Record.Status = providercore.StatusError
			r.providers[i].Record.Schedulable = false
			r.providers[i].Record.ErrorMessage = message
		}
	}
	return nil
}

// SetTempUnschedulable records the attempted ID before any configured write
// error. On success it copies until into matching providers under the mutex;
// unknown IDs are still observed as attempted writes.
func (r *grokCredentialHandlerRepo) SetTempUnschedulable(_ context.Context, id int64, until time.Time, _ string) error {
	// Publish attempted-ID recording and cooldown assignment under one lock
	// so locked provider rereads see either the preceding state or the
	// completed fixture update.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.setTempIDs = append(r.setTempIDs, id)
	if r.setTempErr != nil {
		return r.setTempErr
	}
	for i := range r.providers {
		if r.providers[i].Record.ID == id {
			value := until
			r.providers[i].Record.TempUnschedulableUntil = &value
		}
	}
	return nil
}

// SetRateLimited records the ID and installs current rate-limit time plus a
// copied reset time under the mutex. Every matching record is updated, and
// unknown IDs still appear in the observation slice.
func (r *grokCredentialHandlerRepo) SetRateLimited(_ context.Context, id int64, resetAt time.Time) error {
	// Install observation, rate-limit start, and reset together under the
	// reader mutex so locked rereads cannot see a partly updated rate-limit
	// interval.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rateLimitIDs = append(r.rateLimitIDs, id)
	for i := range r.providers {
		if r.providers[i].Record.ID != id {
			continue
		}
		now := time.Now()
		r.providers[i].Record.RateLimitedAt = &now
		value := resetAt
		r.providers[i].Record.RateLimitResetAt = &value
	}
	return nil
}

// SetRateLimitedIfLater skips a reset that is no later than an existing reset
// for the matching provider. Otherwise it unlocks and delegates to
// SetRateLimited, which records the attempted update.
//
// The comparison and write use separate lock intervals. Another caller can
// change the reset between them, so this fixture's method does not guarantee an
// atomic later-reset update under concurrency. Tests relying on its ordering
// serialize these calls; an unknown ID still reaches SetRateLimited and records
// an observation.
func (r *grokCredentialHandlerRepo) SetRateLimitedIfLater(ctx context.Context, id int64, resetAt time.Time) error {
	r.mu.Lock()
	for i := range r.providers {
		if r.providers[i].Record.ID == id && r.providers[i].Record.RateLimitResetAt != nil && !resetAt.After(*r.providers[i].Record.RateLimitResetAt) {
			r.mu.Unlock()
			return nil
		}
	}
	// Release the check lock before SetRateLimited acquires it again; a
	// concurrent reset can intervene, so callers serialize this fixture's
	// conditional updates.
	r.mu.Unlock()
	return r.SetRateLimited(ctx, id, resetAt)
}

// SetGrokCredentialErrorIfMatch marks a provider errored only when its current
// state matches snapshot. It returns false for a missing or stale candidate,
// and a configured write failure records the attempted ID before returning
// false and that error.
//
// The mutex covers snapshot comparison and the successful status,
// schedulability, and message update. Matching includes credentials, proxy ID,
// provider kind, and current scheduling eligibility, so credentials replaced
// since selection cannot be marked through an older snapshot. A successful
// update returns true with the observation recorded in the same locked
// operation.
func (r *grokCredentialHandlerRepo) SetGrokCredentialErrorIfMatch(
	_ context.Context,
	id int64,
	snapshot providercore.CredentialMutationSnapshot,
	message string,
) (bool, error) {
	// Hold provider state stable through credential-and-proxy comparison
	// and mutation so a stale selected snapshot cannot mark replacement
	// credentials through this fixture.
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.providers {
		provider := &r.providers[i]
		if provider.Record.ID != id || !handlerGrokCredentialSnapshotMatches(provider, snapshot) {
			continue
		}
		r.setErrorIDs = append(r.setErrorIDs, id)
		if r.setErrorErr != nil {
			return false, r.setErrorErr
		}
		provider.Record.Status = providercore.StatusError
		provider.Record.Schedulable = false
		provider.Record.ErrorMessage = message
		return true, nil
	}
	return false, nil
}

// SetGrokCredentialTempUnschedulableIfMatch sets a copied cooldown time only
// for a provider whose state matches snapshot. Missing or stale candidates
// return false, and a configured write failure records the attempted ID before
// returning false and that error.
//
// The mutex covers both credential-and-proxy comparison and cooldown
// assignment, keeping the mutation tied to the selected state. Matching also
// requires a schedulable Grok OAuth record. Success returns true and records
// the ID; the reason string is unused by this fixture, so tests observe the
// cooldown and attempted write rather than retained reason text.
func (r *grokCredentialHandlerRepo) SetGrokCredentialTempUnschedulableIfMatch(
	_ context.Context,
	id int64,
	snapshot providercore.CredentialMutationSnapshot,
	until time.Time,
	_ string,
) (bool, error) {
	// Hold provider state stable through snapshot comparison and cooldown
	// assignment so a selected credential snapshot cannot apply its delay
	// after an intervening replacement.
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.providers {
		provider := &r.providers[i]
		if provider.Record.ID != id || !handlerGrokCredentialSnapshotMatches(provider, snapshot) {
			continue
		}
		r.setTempIDs = append(r.setTempIDs, id)
		if r.setTempErr != nil {
			return false, r.setTempErr
		}
		value := until
		provider.Record.TempUnschedulableUntil = &value
		return true, nil
	}
	return false, nil
}

// handlerGrokCredentialSnapshotMatches checks whether a provider still matches
// a credential-mutation snapshot. It requires a Grok OAuth provider that
// remains schedulable, successful credential JSON encoding, exact encoded
// credentials, and matching optional proxy IDs.
//
// The credential map and proxy selection jointly identify the state that
// authorized mutation. A nil provider or encoding failure rejects the
// comparison, and nil proxy differs from an ID-valued proxy. Repository
// conditional mutations perform this comparison while holding their mutex, so a
// matching snapshot and the following fixture state update use one locked
// provider view.
func handlerGrokCredentialSnapshotMatches(provider *gatewayprovider.ExecutionProvider, snapshot providercore.CredentialMutationSnapshot) bool {
	if provider == nil {
		return false
	}
	// Reject unencodable credentials as a mismatch so conditional mutations
	// cannot treat an incomparable record as the state selected by the request.
	credentialsJSON, err := json.Marshal(provider.Record.Credentials)
	return err == nil && provider.View().IsGrokOAuth() && provider.View().IsSchedulable() && string(credentialsJSON) == snapshot.CredentialsJSON &&
		handlerGrokCredentialProxyIDsEqual(provider.Record.ProxyID, snapshot.ProxyID)
}

// handlerGrokCredentialProxyIDsEqual compares optional proxy IDs by presence
// and value. Two nil pointers match; one nil pointer differs from an ID
// pointer, even when that pointed value is zero.
func handlerGrokCredentialProxyIDsEqual(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

// UpdateExtra records the ID and merges supplied entries into matching
// providers under the mutex. It allocates a missing Extra map; copied entries
// retain their nested references, and unknown IDs still record the attempt.
func (r *grokCredentialHandlerRepo) UpdateExtra(_ context.Context, id int64, updates map[string]any) error {
	// Merge extra entries and record their attempted provider ID under the
	// reader mutex so locked rereads observe the completed update with its
	// corresponding call.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.updateExtraIDs = append(r.updateExtraIDs, id)
	for i := range r.providers {
		if r.providers[i].Record.ID != id {
			continue
		}
		if r.providers[i].Record.Extra == nil {
			r.providers[i].Record.Extra = map[string]any{}
		}
		maps.Copy(r.providers[i].Record.Extra, updates)
	}
	return nil
}

// errorIDs returns a copied slice of attempted error-state provider IDs under
// the repository mutex. Callers can modify its slice storage without changing
// the fixture's observations, including attempts that returned configured write
// errors.
func (r *grokCredentialHandlerRepo) errorIDs() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.setErrorIDs...)
}

// selectorCalls reads the selection invocation count while holding the
// repository mutex. The count includes delegated group and ungrouped reads
// because those methods use ListSchedulableByPlatform for their fixture
// selection.
func (r *grokCredentialHandlerRepo) selectorCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.selectionCalls
}

// rateLimitedProviderIDs returns a copied slice of observed rate-limit provider
// IDs under the mutex. The result isolates slice edits from stored observations
// and includes unknown-ID attempts recorded by SetRateLimited.
func (r *grokCredentialHandlerRepo) rateLimitedProviderIDs() []int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int64(nil), r.rateLimitIDs...)
}

// GetAccessToken returns an empty token and a fixed cache-miss error.
func (c *grokCredentialHandlerTokenCache) GetAccessToken(context.Context, string) (string, error) {
	return "", errors.New("not cached")
}

// SetAccessToken acknowledges writes without retaining a token or expiration.
func (c *grokCredentialHandlerTokenCache) SetAccessToken(context.Context, string, string, time.Duration) error {
	return nil
}

// DeleteAccessToken returns the configured invalidation error while holding the
// cache mutex. It ignores the cache key and stores no tokens, allowing tests to
// expose cleanup failure during credential recovery.
func (c *grokCredentialHandlerTokenCache) DeleteAccessToken(context.Context, string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deleteErr
}

// AcquireRefreshLock grants every refresh claim without retaining lock
// ownership.
func (c *grokCredentialHandlerTokenCache) AcquireRefreshLock(context.Context, string, time.Duration) (bool, error) {
	return true, nil
}

// ReleaseRefreshLock acknowledges release without tracking any fixture refresh
// lock.
func (c *grokCredentialHandlerTokenCache) ReleaseRefreshLock(context.Context, string) error {
	return nil
}

// cloneCredentialMap creates a map with copied key-value entries, returning a
// nonnil empty map for nil input. Nested map, slice, and pointer values remain
// shared, so the result isolates outer credential edits alone.
func cloneCredentialMap(source map[string]any) map[string]any {
	cloned := make(map[string]any, len(source))
	maps.Copy(cloned, source)
	return cloned
}

// CacheKey delegates the provider cache identity to GrokTokenCacheKey.
func (r *grokCredentialHandlerRefresher) CacheKey(provider *providercore.Record) string {
	return providercore.GrokTokenCacheKey(provider)
}

// CanRefresh reports whether a nonnil provider is Grok OAuth.
func (r *grokCredentialHandlerRefresher) CanRefresh(provider *providercore.Record) bool {
	return provider != nil && provider.IsGrokOAuth()
}

// NeedsRefresh requests refresh for provider 801 or every nonnil provider in
// all_revoked mode. The supplied refresh window is ignored, so fixture mode and
// provider identity determine this decision.
func (r *grokCredentialHandlerRefresher) NeedsRefresh(provider *providercore.Record, _ time.Duration) bool {
	return provider != nil && (provider.ID == 801 || r.mode == "all_revoked")
}

// Refresh returns scripted OAuth or transport failures according to mode.
// Cancellation mode requires an initialized started channel, publishes refresh
// entry once, then waits for ctx cancellation; unrecognized modes return nil
// credentials and nil error.
//
// The entry signal lets the HTTP test cancel after refresh has reached its
// wait, keeping pre-cancelled ingress distinct from in-flight cancellation.
// sync.Once protects channel closure across repeated calls, while each call
// still waits on its own request context. Revoked modes produce invalid_grant,
// provider configuration produces invalid_client, and transient modes return
// the transport error used by cooldown tests.
func (r *grokCredentialHandlerRefresher) Refresh(ctx context.Context, _ *providercore.Record) (map[string]any, error) {
	switch r.mode {
	case "revoked", "all_revoked", "mutation_set_error", "mutation_cache":
		return nil, apperror.New(http.StatusBadGateway, "GROK_OAUTH_TOKEN_REFRESH_FAILED", "invalid_grant")
	case "provider":
		return nil, apperror.New(http.StatusBadGateway, "GROK_OAUTH_TOKEN_REFRESH_FAILED", "invalid_client")
	case "cancel":
		// Publish entry once before waiting for cancellation so the
		// test can interrupt an active refresh rather than accidentally
		// cancelling before that path starts.
		r.once.Do(func() { close(r.started) })
		<-ctx.Done()
		return nil, ctx.Err()
	case "transient", "mutation_temp":
		return nil, errors.New("temporary refresh transport failure")
	default:
		return nil, nil
	}
}

// Do records provider ID, URL, and Authorization, then returns the configured
// Grok response. Rate-limit IDs take precedence over per-provider
// failureStatus, which takes precedence over failProviderID's payment failure;
// otherwise the literal `"stream":true` fragment and request path select
// success shape.
//
// The mutex publishes one observation and snapshots failure controls before
// unlocking. A payment-failure callback can cancel the request after that
// snapshot while the scripted response still returns, exposing the gateway's
// post-forward cancellation handling. Request-body read errors are discarded
// with any bytes retained. Successful callers own the returned response body,
// and fixture control changes use the same mutex as this snapshot.
func (u *grokCredentialHandlerUpstream) Do(req *http.Request, _ string, providerID int64, _ int) (*http.Response, error) {
	var requestBody []byte
	if req.Body != nil {
		// Keep bytes obtained before any fixture body-read error
		// because they still determine the scripted response shape; the
		// outer handler assertions observe exchange success.
		requestBody, _ = io.ReadAll(req.Body)
	}
	// Record provider, URL, and authorization together and snapshot all
	// failure controls before unlocking so each observed attempt keeps its
	// request identity and scripted outcome.
	u.mu.Lock()
	u.hits = append(u.hits, providerID)
	u.requestURLs = append(u.requestURLs, req.URL.String())
	u.authorization = append(u.authorization, req.Header.Get("Authorization"))
	failProviderID := u.failProviderID
	rateLimited := u.rateLimitIDs[providerID]
	failureStatus := u.failureStatus[providerID]
	cancelRequest := u.cancelRequest
	u.mu.Unlock()
	if rateLimited {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header: http.Header{
				"Content-Type": []string{"application/json"},
				"Retry-After":  []string{"60"},
			},
			Body: io.NopCloser(bytes.NewBufferString(`{"error":{"message":"rate limited"}}`)),
		}, nil
	}
	if failureStatus > 0 {
		return &http.Response{
			StatusCode: failureStatus,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewBufferString(`{"error":{"message":"upstream unavailable"}}`)),
		}, nil
	}
	if providerID == failProviderID {
		if cancelRequest != nil {
			// Cancel after the failure controls have been
			// snapshotted and unlocked, then return the scripted
			// response so the gateway exercises post-forward
			// cancellation handling.
			cancelRequest()
		}
		return &http.Response{
			StatusCode: http.StatusPaymentRequired,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewBufferString(`{"error":{"message":"payment required"}}`)),
		}, nil
	}
	if bytes.Contains(requestBody, []byte(`"stream":true`)) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
			Body: io.NopCloser(bytes.NewBufferString(
				"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_healthy\",\"model\":\"grok-4.5\",\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
			)),
		}, nil
	}
	if strings.Contains(req.URL.Path, "/chat/completions") {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(bytes.NewBufferString(
				`{"id":"chatcmpl_healthy","object":"chat.completion","model":"grok-4.5","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
			)),
		}, nil
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(bytes.NewBufferString(
			`{"id":"resp_healthy","object":"response","model":"grok-4.5","status":"completed","output":[{"type":"message","id":"msg_healthy","status":"completed","role":"assistant","content":[{"type":"output_text","text":"ok","annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":1}}`,
		)),
	}, nil
}

// DoWithTLS delegates to Do and ignores the supplied TLS fingerprint profile.
// Request observation, scripted failure precedence, and response-body ownership
// follow the ordinary transport method used by these fixtures.
func (u *grokCredentialHandlerUpstream) DoWithTLS(
	req *http.Request,
	proxyURL string,
	providerID int64,
	providerConcurrency int,
	_ *tlsfingerprint.Profile,
) (*http.Response, error) {
	return u.Do(req, proxyURL, providerID, providerConcurrency)
}

// providerHits returns a copied provider-ID sequence under the transport mutex.
// Callers inspect the separate slice after request execution without changing
// the recorded order of upstream attempts.
func (u *grokCredentialHandlerUpstream) providerHits() []int64 {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]int64(nil), u.hits...)
}

// requests returns copied URL and Authorization sequences from one locked
// observation snapshot. Corresponding positions describe the same Do
// invocation, so callers can compare credential use with its forwarded
// endpoint.
//
// Do appends the URL and header value together under this mutex. Copying the
// sequences in separate lock intervals could admit another call between
// snapshots and produce mismatched lengths or positions. Taking both copies
// while locked retains each request's pairing; after return, callers can modify
// the two slice containers without changing stored observations.
func (u *grokCredentialHandlerUpstream) requests() ([]string, []string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.requestURLs...), append([]string(nil), u.authorization...)
}

// findHandlerRefresherStarted returns the refresh-entry channel registered for
// router by newGrokCredentialFailoverHandler. The router must still have that
// registration; a missing or incorrectly typed value fails the asserted type
// conversion.
func findHandlerRefresherStarted(router *gin.Engine) <-chan struct{} {
	value, _ := handlerRefresherStarted.Load(router)
	return testassert.MustType[chan struct{}](value)
}

// newGrokCredentialFailoverHandler assembles Grok HTTP and WebSocket endpoints
// with scripted credentials, repository failures, and upstream results selected
// by mode. It returns observations and a cleanup callback; callers finish
// requests before invoking cleanup, which removes the router's refresh signal
// and stops billing workers.
//
// Expired credentials exercise refresh failures, while rate-limit and payment
// modes give the first provider valid credentials so forwarding reaches the
// scripted response. Some modes add a third provider to expose bounded retry
// behavior; the API-key mode changes the second provider's credential type. All
// records receive group membership and wildcard model admission so credential
// recovery is isolated from catalogue rejection. The cancellation mode
// publishes refresh entry before waiting on the request context.
func newGrokCredentialFailoverHandler(t *testing.T, mode string) (*gatewayHTTPEndpointsFixture, *grokCredentialHandlerRepo, *grokCredentialHandlerUpstream, *gin.Engine, func()) {
	t.Helper()
	groupID := int64(901)
	providers := []gatewayprovider.ExecutionProvider{
		{
			Record: providercore.Record{
				LoadLocation: time.LoadLocation, ID: 801, Name: "revoked", Platform: capability.PlatformGrok, Type: capability.ProviderTypeOAuth,
				Status: billing.StatusActive, Schedulable: true, Concurrency: 1, Priority: 1,
				Credentials: map[string]any{
					"access_token": "expired", "refresh_token": "revoked-refresh",
					"expires_at": time.Now().Add(-time.Minute).UTC().Format(time.RFC3339),
				},
				Extra: map[string]any{providercore.GrokMediaEligibleExtraKey: true},
			},
		},
		{
			Record: providercore.Record{
				LoadLocation: time.LoadLocation, ID: 802, Name: "healthy", Platform: capability.PlatformGrok, Type: capability.ProviderTypeOAuth,
				Status: billing.StatusActive, Schedulable: true, Concurrency: 1, Priority: 2,
				Credentials: map[string]any{
					"access_token": "healthy-access", "refresh_token": "healthy-refresh",
					"expires_at": time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339),
				},
				Extra: map[string]any{providercore.GrokMediaEligibleExtraKey: true},
			},
		},
	}
	if mode == "postmap_cancel" || mode == "first_402" || mode == "first_429" || mode == "all_429" || mode == "mixed_429_500" || mode == "mixed_500_429" || mode == "oauth_429_apikey_500" {
		providers[0].Record.Credentials["expires_at"] = time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339)
	}
	if mode == "all_429" || mode == "mixed_429_500" || mode == "mixed_500_429" || mode == "oauth_429_apikey_500" {
		providers = append(providers, gatewayprovider.ExecutionProvider{
			Record: providercore.Record{
				LoadLocation: time.LoadLocation, ID: 803, Name: "untried-healthy", Platform: capability.PlatformGrok, Type: capability.ProviderTypeOAuth,
				Status: billing.StatusActive, Schedulable: true, Concurrency: 1, Priority: 3,
				Credentials: map[string]any{
					"access_token": "untried-healthy-access", "refresh_token": "untried-healthy-refresh",
					"expires_at": time.Now().Add(2 * time.Hour).UTC().Format(time.RFC3339),
				},
				Extra: map[string]any{providercore.GrokMediaEligibleExtraKey: true},
			},
		})
	}
	if mode == "oauth_429_apikey_500" {
		providers[1].Record.Type = capability.ProviderTypeAPIKey
		providers[1].Record.Credentials = map[string]any{"api_key": "third-party-key"}
	}
	if mode == "all_revoked" {
		providers[1].Record.Credentials["expires_at"] = time.Now().Add(-time.Minute).UTC().Format(time.RFC3339)
	}
	// Give every credential fixture wildcard model admission and the test
	// group before selection so model policy does not obscure
	// credential-recovery observations.
	for i := range providers {
		providers[i].Record.Credentials["model_whitelist"] = []string{"*"}
		providers[i].Record.GroupIDs = []int64{groupID}
	}
	repo := &grokCredentialHandlerRepo{providers: providers, missingOnGet: map[int64]bool{}}
	if mode == "missing_row" {
		repo.missingOnGet[801] = true
	}
	if mode == "mutation_set_error" {
		repo.setErrorErr = errors.New("database write failed")
	}
	if mode == "mutation_temp" {
		repo.setTempErr = errors.New("database write failed")
	}
	refresher := &grokCredentialHandlerRefresher{mode: mode, started: make(chan struct{})}
	tokenCache := &grokCredentialHandlerTokenCache{}
	if mode == "mutation_cache" {
		tokenCache.deleteErr = errors.New("cache delete failed")
	}
	var provider *providercore.GrokTokenSource
	if mode != "nil_provider" {
		refresh := providercore.NewOAuthRefreshAPI(grokCredentialTokenReader{repo}, tokenCache, providercore.RefreshOptions{Now: time.Now, Warn: slog.Warn, Info: slog.Info, Error: slog.Error, Platform: providercore.ProviderRefreshPlatformPolicy()})
		provider = &providercore.GrokTokenSource{
			Repository: grokCredentialTokenReader{repo}, Cache: tokenCache,
			Policy: providercore.GrokProviderRefreshPolicy(),
			Refresh: func(ctx context.Context, record *providercore.Record, window time.Duration) (*providercore.OAuthRefreshResult, error) {
				return refresh.RefreshIfNeeded(ctx, record, refresher, window)
			},
		}
	}
	upstream := &grokCredentialHandlerUpstream{}
	switch mode {
	case "first_402":
		upstream.failProviderID = 801
	case "first_429":
		upstream.rateLimitIDs = map[int64]bool{801: true}
	case "all_429":
		upstream.rateLimitIDs = map[int64]bool{801: true, 802: true}
	case "mixed_429_500":
		upstream.rateLimitIDs = map[int64]bool{801: true}
		upstream.failureStatus = map[int64]int{802: http.StatusInternalServerError}
	case "mixed_500_429":
		upstream.failureStatus = map[int64]int{801: http.StatusInternalServerError}
		upstream.rateLimitIDs = map[int64]bool{802: true}
	case "oauth_429_apikey_500":
		upstream.rateLimitIDs = map[int64]bool{801: true}
		upstream.failureStatus = map[int64]int{802: http.StatusInternalServerError}
	}
	cfg := &config.Config{}
	cfg.Gateway.Failover.MaxProviderSwitches = 3
	billingCache := newBillingEligibilityFixture(cfg)
	billingCache.Start()
	completionInput2 := billingtestkit.Calculator(nil, nil)
	completionInput3 := &providercore.DeferredService{}
	gateway, gatewayChoices, gatewayCredentialPort := newOpenAIExecutionAndSelectionFixture(
		repo, nil, cfg, nil, nil, nil, upstream,
		nil, completionInput3, newOpenAIExecutionCredentialsForTest(repo,
			provider), provider, nil, nil, nil, nil, responseHeaderFilterForTest(cfg), nil, nil, nil,
	)
	gateway.Recorder = newHTTPCompletionFixture(cfg, nil, completionInput2, billingCache, completionInput3, nil, nil, true)

	cache := &httptestkit.ConcurrencyHooks{
		AcquireUserSlotFn:     func(context.Context, int64, int, string) (bool, error) { return true, nil },
		AcquireProviderSlotFn: func(context.Context, int64, int, string) (bool, error) { return true, nil },
	}
	h := newGatewayHTTPEndpointsFromDeps(gateway, gatewayCredentialPort, scheduler.NewConcurrencyService(cache, scheduler.Diagnostics{
		Logf:  logging.LegacyPrintf,
		Event: logging.Event,
	},
	), newFundingAdmissionFixture(billingCache, cfg), &apikey.APIKeyService{}, nil, nil, nil, nil, cfg, nil, newExecutionAvailabilityForTest(repo,

		nil, cfg), gatewayChoices,
	)
	apiKey := &apikey.APIKey{
		ID: 902, GroupID: &groupID,
		User: &identity.User{ID: 903, Status: billing.StatusActive},
		Group: &routing.Group{
			ID:                   groupID,
			Status:               billing.StatusActive,
			AllowImageGeneration: true,
			AllowedProtocols: []protocolcore.ProtocolID{
				protocolcore.ProtocolAnthropicMessages,
				protocolcore.ProtocolOpenAIResponses,
				protocolcore.ProtocolOpenAIChatCompletions,
			},
		},
	}
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set(string(keyhttp.ContextKeyAPIKey), apiKey)
		c.Set(string(authctx.ContextKeyUser), authctx.AuthSubject{UserID: apiKey.User.ID, Concurrency: 1})
		c.Next()
	})
	router.POST("/openai/v1/responses", h.Responses)
	router.GET("/openai/v1/responses", h.ResponsesWebSocket)
	router.POST("/openai/v1/messages", h.Messages)
	router.POST("/openai/v1/chat/completions", h.ChatCompletions)
	router.POST("/openai/v1/videos/generations", h.GrokVideoGeneration)
	router.GET("/openai/v1/videos/:request_id", h.GrokVideoStatus)
	// Register refresh entry by router before returning it to the test;
	// cleanup removes that exact association after requests finish and
	// stops billing workers.
	handlerRefresherStarted.Store(router, refresher.started)
	cleanup := func() {
		handlerRefresherStarted.Delete(router)
		billingCache.Stop()
	}
	return h, repo, upstream, router, cleanup
}

// Get returns a fresh Setting record when the requested key exists in the
// fixture. Missing keys return ErrSettingNotFound, preserving a distinct
// absent-setting result for moderation configuration reads.
func (r *contentModerationHandlerSettingRepo) Get(ctx context.Context, key string) (*settingscore.Setting, error) {
	if value, ok := r.values[key]; ok {
		return &settingscore.Setting{Key: key, Value: value}, nil
	}
	return nil, settingscore.ErrSettingNotFound
}

// GetValue returns the stored string for the requested key, including empty
// values. Missing keys return ErrSettingNotFound; this mutable fixture expects
// callers to serialize writes and reads.
func (r *contentModerationHandlerSettingRepo) GetValue(ctx context.Context, key string) (string, error) {
	if value, ok := r.values[key]; ok {
		return value, nil
	}
	return "", settingscore.ErrSettingNotFound
}

// Set stores the supplied string, allocating the fixture map when needed. It
// has no synchronization or persistence backend, so callers serialize updates
// with moderation or gateway configuration reads.
func (r *contentModerationHandlerSettingRepo) Set(ctx context.Context, key, value string) error {
	if r.values == nil {
		r.values = map[string]string{}
	}
	r.values[key] = value
	return nil
}

// GetMultiple allocates a map containing requested keys present in the fixture.
// Missing keys are omitted and empty values remain present, preserving the
// distinction used by settings consumers.
func (r *contentModerationHandlerSettingRepo) GetMultiple(ctx context.Context, keys []string) (map[string]string, error) {
	out := map[string]string{}
	for _, key := range keys {
		if value, ok := r.values[key]; ok {
			out[key] = value
		}
	}
	return out, nil
}

// SetMultiple merges supplied entries into the fixture map, allocating it when
// needed. Existing unrelated settings remain available, and callers serialize
// this mutable map with any concurrent runtime reads.
func (r *contentModerationHandlerSettingRepo) SetMultiple(ctx context.Context, settings map[string]string) error {
	if r.values == nil {
		r.values = map[string]string{}
	}
	maps.Copy(r.values, settings)
	return nil
}

// GetAll returns a new map containing every fixture setting value. Strings are
// copied by value, so callers can change the returned map without modifying
// stored settings.
func (r *contentModerationHandlerSettingRepo) GetAll(ctx context.Context) (map[string]string, error) {
	out := make(map[string]string, len(r.values))
	maps.Copy(out, r.values)
	return out, nil
}

// Delete removes the requested fixture setting and succeeds when it is absent.
// Map access is unsynchronized, so callers serialize deletion with moderation
// and gateway runtime reads.
func (r *contentModerationHandlerSettingRepo) Delete(ctx context.Context, key string) error {
	delete(r.values, key)
	return nil
}

// CreateLog appends an outer log copy under the repository mutex when log is
// nonnil. Nil is accepted, and nested log values remain shared with the
// supplied record for later assertions.
func (r *contentModerationHandlerTestRepo) CreateLog(ctx context.Context, log *moderation.ContentModerationLog) error {
	if log != nil {
		// Append the outer log copy under the snapshot mutex so polling
		// assertions can observe completed records while moderation
		// workers continue to submit logs.
		r.mu.Lock()
		defer r.mu.Unlock()
		r.logs = append(r.logs, *log)
	}
	return nil
}

// resetLogs clears the stored log slice while holding the repository mutex.
// Callers use it after setup work has been observed so later HTTP assertions
// identify newly recorded requests.
func (r *contentModerationHandlerTestRepo) resetLogs() {
	// Clear captured logs under the snapshot mutex after setup has
	// completed so the next observation identifies handler work rather than
	// the direct warm-up check.
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logs = nil
}

// logSnapshot returns an outer copy of the recorded log slice under the mutex.
// Nested record data remains shared, so callers use the snapshot for
// observation and serialize any nested mutation.
func (r *contentModerationHandlerTestRepo) logSnapshot() []moderation.ContentModerationLog {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]moderation.ContentModerationLog(nil), r.logs...)
}

// ListLogs returns no records or pagination metadata for every fixture filter.
func (r *contentModerationHandlerTestRepo) ListLogs(ctx context.Context, filter moderation.ContentModerationLogFilter) ([]moderation.ContentModerationLog, *pagination.PaginationResult, error) {
	return nil, nil, nil
}

// CountFlaggedByUserSince returns zero prior violations for every user and
// time.
func (r *contentModerationHandlerTestRepo) CountFlaggedByUserSince(ctx context.Context, userID int64, since time.Time) (int, error) {
	return 0, nil
}

// CreateCyberWarning appends an outer warning copy under the mutex when warning
// is nonnil. Nil is accepted, while nested warning data remains shared with the
// supplied record.
func (r *contentModerationHandlerTestRepo) CreateCyberWarning(ctx context.Context, warning *moderation.ContentModerationCyberWarning) error {
	if warning != nil {
		// Append the warning under the snapshot mutex so concurrent
		// handler callbacks publish completed outer records before
		// polling assertions inspect the warning list.
		r.mu.Lock()
		defer r.mu.Unlock()
		r.cyberWarnings = append(r.cyberWarnings, *warning)
	}
	return nil
}

// CreateCyberWarningAndApplyUserBan sets a nonpositive incoming violation count
// to one before storing an outer warning copy under the mutex. It always
// reports no applied ban, ignores policy, and accepts nil warnings.
func (r *contentModerationHandlerTestRepo) CreateCyberWarningAndApplyUserBan(ctx context.Context, warning *moderation.ContentModerationCyberWarning, policy moderation.ContentModerationCyberWarningPolicy) (bool, error) {
	if warning != nil {
		if warning.ViolationCount <= 0 {
			warning.ViolationCount = 1
		}
		// Publish the normalized warning under the snapshot mutex so
		// polling assertions see its final fixture violation count
		// together with the captured outer record.
		r.mu.Lock()
		defer r.mu.Unlock()
		r.cyberWarnings = append(r.cyberWarnings, *warning)
	}
	return false, nil
}

// ListCyberWarnings returns no warning records or pagination metadata for every
// filter.
func (r *contentModerationHandlerTestRepo) ListCyberWarnings(ctx context.Context, filter moderation.ContentModerationCyberWarningFilter) ([]moderation.ContentModerationCyberWarning, *pagination.PaginationResult, error) {
	return nil, nil, nil
}

// CountCyberWarningsByUserSince returns zero prior warnings for every user and
// time.
func (r *contentModerationHandlerTestRepo) CountCyberWarningsByUserSince(ctx context.Context, userID int64, since time.Time) (int, error) {
	return 0, nil
}

// GetCyberSummary returns a new zero-valued summary for every fixture filter.
func (r *contentModerationHandlerTestRepo) GetCyberSummary(ctx context.Context, filter moderation.ContentModerationCyberWarningFilter) (*moderation.ContentModerationCyberSummary, error) {
	return &moderation.ContentModerationCyberSummary{}, nil
}

// MarkCyberWarningEmailSent acknowledges the marker without changing any stored
// warning.
func (r *contentModerationHandlerTestRepo) MarkCyberWarningEmailSent(ctx context.Context, id int64) error {
	return nil
}

// CleanupExpiredLogs returns a new zero-valued cleanup result without removing
// records.
func (r *contentModerationHandlerTestRepo) CleanupExpiredLogs(ctx context.Context, hitBefore time.Time, nonHitBefore time.Time) (*moderation.ContentModerationCleanupResult, error) {
	return &moderation.ContentModerationCleanupResult{}, nil
}

// Error returns the wrapped cause's message when both receiver and cause are
// present. A nil receiver or cause returns the fixed test warning message for
// error-output assertions.
func (e *openAIHandlerTestWarningError) Error() string {
	if e == nil || e.err == nil {
		return "test warning error"
	}
	return e.err.Error()
}

// Unwrap returns the retained forwarding cause for standard error-chain
// inspection. A nil receiver returns nil, and the returned error remains the
// same object supplied to the fixture.
func (e *openAIHandlerTestWarningError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.err
}

// OpenAIUpstreamWarning returns the retained upstream warning pointer for
// error-output detection. A nil receiver returns nil; callers keep warning data
// stable while the forwarding error is inspected.
func (e *openAIHandlerTestWarningError) OpenAIUpstreamWarning() *forwardcore.UpstreamWarning {
	if e == nil {
		return nil
	}
	return e.warning
}

// newOpenAIHandlerForPreviousResponseIDValidation binds minimal OpenAI
// execution and funding fixtures for continuation and ingress checks. A nil
// cache supplies successful user and provider slot hooks; a supplied cache
// retains its scripted admission behavior.
func newOpenAIHandlerForPreviousResponseIDValidation(t *testing.T, cache *httptestkit.ConcurrencyHooks) *gatewayHTTPEndpointsFixture {
	t.Helper()
	if cache == nil {
		cache = &httptestkit.ConcurrencyHooks{
			AcquireUserSlotFn: func(ctx context.Context, userID int64, maxConcurrency int, requestID string) (bool, error) {
				return true, nil
			},
			AcquireProviderSlotFn: func(ctx context.Context, providerID int64, maxConcurrency int, requestID string) (bool, error) {
				return true, nil
			},
		}
	}
	return newGatewayHTTPEndpoints(gatewayHTTPFixtureInput{
		Source:  &gatewayExecutionFixture{},
		Funding: &admission.FundingAdmission{},
		Keys:    &apikey.APIKeyService{},
		Concurrency: gatewayhttp.NewConcurrencyHelper(scheduler.NewConcurrencyService(cache, scheduler.Diagnostics{
			Logf: logging.LegacyPrintf,

			Event: logging.Event,
		},
		), gatewayhttp.SSEPingFormatNone, time.Second), Availability: newExecutionAvailabilityForTest(nil, nil, nil), Choices: newEmptyCompatibleSelectionFixture(),
	})
}

// ListSchedulableByPlatform returns outer provider copies passing platform and
// IsSchedulable checks. Empty platform includes all schedulable fixture
// records, and nested references remain shared; tests serialize fixture
// mutation and observation.
func (s *openAIWSFailoverHandlerProviderRepoStub) ListSchedulableByPlatform(ctx context.Context, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	out := make([]gatewayprovider.ExecutionProvider, 0, len(s.providers))
	for _, provider := range s.providers {
		if (platform == "" || provider.Record.Platform == platform) && provider.View().IsSchedulable() {
			out = append(out, provider)
		}
	}
	return out, nil
}

// ListSchedulableByGroupIDAndPlatform delegates platform and schedulability
// filtering while ignoring group membership. Outer provider copies and their
// shared nested values follow ListSchedulableByPlatform for the fixture
// selection reads.
func (s *openAIWSFailoverHandlerProviderRepoStub) ListSchedulableByGroupIDAndPlatform(ctx context.Context, groupID int64, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	return s.ListSchedulableByPlatform(ctx, platform)
}

// ListSchedulableUngroupedByPlatform delegates platform and schedulability
// filtering without restricting group membership. Its copied outer provider
// values retain nested references just like the other selection reads in this
// fixture.
func (s *openAIWSFailoverHandlerProviderRepoStub) ListSchedulableUngroupedByPlatform(ctx context.Context, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	return s.ListSchedulableByPlatform(ctx, platform)
}

// GetByID returns an outer copy of the first fixture provider matching id.
// Unknown IDs return nil without error, while nested maps and pointers retain
// shared ownership.
func (s *openAIWSFailoverHandlerProviderRepoStub) GetByID(ctx context.Context, id int64) (*gatewayprovider.ExecutionProvider, error) {
	for _, provider := range s.providers {
		if provider.Record.ID == id {
			acc := provider
			return &acc, nil
		}
	}
	return nil, nil
}

// SetRateLimited appends the observed ID and replaces the first matching
// provider's reset-time pointer with a copied value. Unknown IDs are still
// recorded, and callers serialize this fixture's unsynchronized mutation.
func (s *openAIWSFailoverHandlerProviderRepoStub) SetRateLimited(ctx context.Context, id int64, resetAt time.Time) error {
	// Observe every attempted ID before updating the first matching reset
	// so tests can distinguish a requested mutation from the presence of a
	// fixture record.
	s.rateLimitedIDs = append(s.rateLimitedIDs, id)
	for i := range s.providers {
		if s.providers[i].Record.ID == id {
			reset := resetAt
			s.providers[i].Record.RateLimitResetAt = &reset
			break
		}
	}
	return nil
}

// cyberWarningSnapshot returns an outer copy of the stored warning slice under
// the mutex. Nested warning data remains shared, so concurrent callers use this
// snapshot for observation rather than mutation.
func (r *contentModerationHandlerTestRepo) cyberWarningSnapshot() []moderation.ContentModerationCyberWarning {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]moderation.ContentModerationCyberWarning(nil), r.cyberWarnings...)
}

// newOpsCaptureQueue allocates a serial capture queue with the requested
// channel capacity. Enqueue drops a job when capacity is unavailable, including
// an unbuffered queue with no ready receiver; negative size panics.
func newOpsCaptureQueue(size int) *captureOpsErrorQueue {
	return &captureOpsErrorQueue{jobs: make(chan opsErrorLogJob, size)}
}

// Enqueue sanitizes a nonnil error input paired with a nonnil Ops service, then
// attempts a nonblocking channel send. The original input pointer is queued
// after sanitization, and callers keep it stable for assertions.
//
// Preparation errors and full queues increase Dropped, while successful sends
// increase Length and Enqueued. Sanitized counts preparation changes even if
// the job is later dropped. Nil inputs have no effect. The fixture's counters
// are unsynchronized and Length is not decremented when tests read jobs, so
// callers use serial submission and inspection rather than treating it as a
// live production queue depth.
func (q *captureOpsErrorQueue) Enqueue(s *opscore.OpsService, e *opscore.OpsInsertErrorLogInput) {
	if s == nil || e == nil {
		return
	}
	// Prepare the original pointer before queue admission so captured jobs
	// contain sanitized fields, and preparation changes remain counted even
	// if admission later fails.
	sanitized, err := opscore.PrepareErrorLogInput(e)
	if sanitized {
		q.health.Sanitized++
	}
	if err != nil {
		q.health.Dropped++
		return
	}
	// Attempt admission without blocking the middleware; a full or unready
	// channel records a drop so serial tests can inspect both accepted jobs
	// and overflow.
	select {
	case q.jobs <- opsErrorLogJob{ops: s, entry: e}:
		q.health.Length++
		q.health.Enqueued++
	default:
		q.health.Dropped++
	}
}

// responseHeaderFilterForTest compiles response-header settings from cfg for
// forwarding fixtures. A nil config returns no filter, while nonnil config
// supplies enabled state, added allowed names, and forced removals.
func responseHeaderFilterForTest(cfg *config.Config) *egress.CompiledHeaderFilter {
	if cfg == nil {
		return nil
	}
	return egress.CompileHeaderFilter(egress.ResponseHeaderOptions{Enabled: cfg.Gateway.ResponseHeaders.Enabled, AdditionalAllowed: cfg.Gateway.ResponseHeaders.AdditionalAllowed, ForceRemove: cfg.Gateway.ResponseHeaders.ForceRemove})
}

// newExecutionAvailabilityForTest binds the supplied store and model policies
// to general and compatible model diagnostics. Nil storage leaves availability
// reads absent; cfg is unused, and the resolved diagnostic uses compatible
// routing checks.
func newExecutionAvailabilityForTest(store gatewayprovider.ExecutionProviderStore, modelConfigs *routing.PricingConfigService, cfg *config.Config) *gatewayModelAvailability {
	var source gatewayprovider.AvailabilityProviders
	if store != nil {
		source = gatewaytestkit.AvailabilityStore{Source: store}
	}

	general := gatewayprovider.NewModelAvailability(source, modelConfigs, false)
	compatible := gatewayprovider.NewModelAvailability(source, modelConfigs, true)
	return &gatewayModelAvailability{
		Messages:   routing.ModelAvailabilityDiagnoserFunc(general.DiagnoseGeneral),
		Compatible: routing.ModelAvailabilityDiagnoserFunc(compatible.DiagnoseCompatible),
		Resolved:   routing.ModelAvailabilityDiagnoserFunc(compatible.DiagnoseCompatibleRouting),
	}
}

// ListAll returns no pricing configurations for model catalogue fixture reads.
func (modelCatalogueEmptyPrices) ListAll(context.Context) ([]routing.PricingConfig, error) {
	return nil, nil
}

// newOpenAIExecutionAndSelectionFixture assembles OpenAI and Grok execution
// together with compatible provider selection. Nil model-transient,
// proxy-circuit, and response-state inputs receive application defaults;
// supplied objects are reused, and only the first optional TLS router is
// selected.
//
// Selection and forwarding share runtime blocks, model-transient state, proxy
// circuit, and response ownership storage. Request credentials use the same
// runtime blocks, while response output receives selection feedback and lineage
// callbacks, so a failure's state is visible to the next attempt. The helper
// constructs connection and request-activity components and returns execution
// inputs, choices, and credential execution. It does not register a complete
// application lifecycle for those constructed components; callers own any
// runtime resources used by their tests.
func newOpenAIExecutionAndSelectionFixture(
	providerRepo gatewayprovider.ExecutionProviderStore,
	cache session.GatewayCache,
	cfg *config.Config,
	schedulerSnapshot *scheduler.SnapshotService,
	concurrencyService *scheduler.ConcurrencyService,

	healthObserver *provideradapter.UpstreamHealth,
	httpUpstream httpclient.UpstreamTransport,
	tlsFPProfileService *egressadapter.TLSProfiles,
	deferredService *providercore.DeferredService,
	executionCredentials *providercore.OpenAIExecutionCredentials,
	grokTokenProvider *providercore.GrokTokenSource,
	resolver *billing.PriceResolver,
	pricingConfigService *routing.PricingConfigService,

	settingService *gatewayprovider.RuntimeReaders,
	prompts *promptpolicy.Service, headerFilter *egress.CompiledHeaderFilter, stateStore session.OpenAIWSStateStore, modelTransient *providercore.ModelTransientState, proxyCircuit *egress.ProxyStreamCircuit,
	tlsFPRouterServices ...*egress.TLSFingerprintRouterService,
) (*gatewayExecutionFixture, *selection.Compatible, *gatewayhttp.RequestCredentialExecutor) {
	if modelTransient ==
		nil {
		modelTransient = provideSelectionModelTransient()
	}
	if proxyCircuit ==
		nil {
		proxyCircuit = provideSelectionProxyCircuit(cfg)
	}
	if stateStore == nil {
		stateStore = session.NewOpenAIWSStateStore(cache, gatewayprovider.LogOpenAIWSModeInfo)
	}
	blocks := providercore.NewRuntimeBlockState(time.Now)
	feedback := scheduler.NewRuntimeStats(time.Now)
	sticky := &scheduler.StickyStats{}
	var quota *providercore.QuotaSettingsCache
	if settingService != nil {
		quota = settingService.Quota
	}
	choices := selection.NewCompatible(selection.CompatibleDependencies{
		Reads: selection.Reads{Providers: withSelectionGroupFixture(providerRepo), Snapshot: provideSelectionSnapshots(schedulerSnapshot)},
		Shared: selection.Shared{
			Cache: cache,

			Concurrency:   concurrencyService,
			Health:        healthObserver,
			GroupPolicies: pricingConfigService,

			Feedback: feedback,
		},
		Responses:      stateStore,
		QuotaSettings:  quota,
		RuntimeBlocks:  blocks,
		ModelTransient: modelTransient,

		ProxyCircuit: proxyCircuit,
		StickyStats:  sticky,
	}, selectionOptions(cfg))
	credentials := gatewaytestkit.RequestCredentials(providerRepo, executionCredentials, grokTokenProvider, blocks)
	executionCredentials = credentials.Source

	turnHeaders := provideCodexTurnStateHeaders(choices)
	// Use a thirty-second write throttle in the Grok health fixture so
	// repeated failures pass through the production rate-limited health
	// observation component.
	grokHealth := &provideradapter.GrokHealth{Store: providerRepo, Health: healthObserver, Runtime: blocks, ModelTransient: modelTransient, Throttle: providercore.NewWriteThrottle(30 * time.Second), NormalizeModel: func(value *providercore.Record, model string) string {
		return (gatewayprovider.ModelPolicy{Record: value}).NormalizeOpenAI(model)
	}}
	connections := openaiws.NewOpenAIWSConnections(openAIWSPoolOptions(cfg), nil)
	identity := gatewayprovider.NewExecutionAgentIdentity(&providercore.OpenAITaskCoordinator{}, providerRepo, nil, connections.InvalidateProvider)
	output := provideOpenAIResponseOutput(cfg, provideOpenAIResponseHealth(healthObserver, blocks, modelTransient, deferredService), grokHealth, healthObserver, headerFilter, turnHeaders, proxyCircuit, stateStore, choices, identity)
	activity := &gatewayRequestActivity{Operations: lifecycle.NewOperations("GatewayRequestsAndAttempts")}
	grokExecutor := provideGrokExecutor(cfg, credentials, httpUpstream, output, grokHealth, tlsFPProfileService, settingService, blocks, deferredService, providerRepo, activity, resolver, connections)
	var routers *egress.TLSFingerprintRouterService
	if len(tlsFPRouterServices) > 0 {
		routers = tlsFPRouterServices[0]
	}
	text := openAITextExecution(cfg, providerRepo, identity, executionCredentials, httpUpstream, tlsFPProfileService, routers, settingService, grokExecutor, output, provideAnthropicPromptCache(), choices.OpenAIHTTPResponseStickyTTL, provideCompactExecutor(cfg))
	lineage := provideOpenAIEncryptedLineage(stateStore, choices)
	imagePolicy := provideOpenAIImageBridgePolicy(cfg)
	sockets := provideOpenAIWebSockets(nil, cfg, connections, text, prompts, choices, lineage, imagePolicy, cache)
	responses := provideOpenAIResponses(text, choices, lineage, imagePolicy)
	var read func(context.Context) (bool, time.Duration)
	if settingService != nil {
		read = settingService.Moderation.GetCyberSessionBlockRuntime
	}
	source := &gatewayExecutionFixture{Text: text, Requests: text.Requests, Responses: responses, WebSockets: sockets, Grok: grokExecutor, Cache: cache, Planner: gatewayprovider.NewRoutePlanner(pricingConfigService), Blocks: session.NewCyberBlocks(session.AdaptCyberSessionBlockStore(cache), read, func(format string, args ...any) { logging.LegacyPrintf("service.openai_gateway", format, args...) })}
	source.Auxiliary = provideOpenAIAuxiliary(text, nil, activity)

	return source, choices, &gatewayhttp.RequestCredentialExecutor{Runtime: credentials}
}

// newEmptyCompatibleSelectionFixture constructs compatible selection with
// default options and absent provider dependencies.
func newEmptyCompatibleSelectionFixture() *selection.Compatible {
	return selection.NewCompatible(selection.CompatibleDependencies{}, selection.DefaultOptions())
}

// completeGroupProjection marks stored membership as complete for selection
// fixture wrapping.
func (*mixedHTTPProviders) completeGroupProjection() {}

// GetByID converts the matching stored record through NewExecutionProvider for
// a new execution target. Unknown IDs return nil; request-specific Route and
// Capabilities start unset according to that constructor's copying rules.
func (s *mixedHTTPProviders) GetByID(_ context.Context, id int64) (*gatewayprovider.ExecutionProvider, error) {
	for _, value := range s.values {
		if value.Record.ID == id {
			return gatewayprovider.NewExecutionProvider(&value.Record), nil
		}
	}
	return nil, nil
}

// GetByIDs rereads requested IDs in input order and omits missing records.
// Duplicate IDs produce separate converted targets, while empty or wholly
// missing input leaves the result nil.
func (s *mixedHTTPProviders) GetByIDs(ctx context.Context, ids []int64) ([]*gatewayprovider.ExecutionProvider, error) {
	var out []*gatewayprovider.ExecutionProvider
	for _, id := range ids {
		// The fixture GetByID cannot return an error; omit missing rows
		// while keeping requested order and separate converted targets
		// for duplicate IDs.
		value, _ := s.GetByID(ctx, id)
		if value != nil {
			out = append(out, value)
		}
	}
	return out, nil
}

// ListSchedulableByGroupIDAndPlatforms converts records matching both stored
// group membership and a requested platform. The fixture does not itself test
// schedulability; production selection applies eligibility to the converted
// execution targets.
func (s *mixedHTTPProviders) ListSchedulableByGroupIDAndPlatforms(_ context.Context, id int64, platforms []string) ([]gatewayprovider.ExecutionProvider, error) {
	var out []gatewayprovider.ExecutionProvider
	for _, value := range s.values {
		if slices.Contains(value.Record.GroupIDs, id) && slices.Contains(platforms, value.Record.Platform) {
			out = append(out, *gatewayprovider.NewExecutionProvider(&value.Record))
		}
	}
	return out, nil
}

// ListSchedulableByGroupIDAndPlatform delegates with a singleton platform list
// and the supplied group. Unlike the membership-completing wrapper, this
// fixture requires stored membership to contain that group before returning
// candidates.
func (s *mixedHTTPProviders) ListSchedulableByGroupIDAndPlatform(ctx context.Context, id int64, p string) ([]gatewayprovider.ExecutionProvider, error) {
	return s.ListSchedulableByGroupIDAndPlatforms(ctx, id, []string{p})
}

// SetError acknowledges marking without changing any stored mixed-platform
// provider.
func (s *mixedHTTPProviders) SetError(context.Context, int64, string) error { return nil }

// SetTempUnschedulable acknowledges cooldown without changing stored provider
// scheduling state.
func (s *mixedHTTPProviders) SetTempUnschedulable(context.Context, int64, time.Time, string) error {
	return nil
}

// UpdateLastUsed acknowledges activity without changing any stored provider
// timestamp.
func (s *mixedHTTPProviders) UpdateLastUsed(context.Context, int64) error { return nil }

// BatchUpdateLastUsed acknowledges activity writes without changing stored
// provider timestamps.
func (s *mixedHTTPProviders) BatchUpdateLastUsed(context.Context, map[int64]time.Time) error {
	return nil
}

// newOAuthSettingsFixture binds repository settings through the application
// OAuth settings provider. Startup configuration and identity runtime readers
// follow that provider's defaults; the returned object retains the
// repository-backed settings service.
func newOAuthSettingsFixture(repo settingscore.Repository, cfg *config.Config) *identity.OAuthSettings {
	return provideOAuthSettings(settingscore.New(repo), cfg)
}

// testEndpoints builds a new map of protocol IDs to HTTP endpoint paths.
func testEndpoints() map[protocolcore.ProtocolID]string {
	out := map[protocolcore.ProtocolID]string{}
	for _, e := range gatewayhttp.ProtocolEndpoints() {
		out[e.ID] = e.Endpoint
	}
	return out
}

// newAppHealthObserverFixture binds provider health observation to the supplied
// store. A nil config uses zero option values; otherwise OAuth unauthorized,
// overload, and Kimi, Zhipu, and DeepSeek-provider intervals come from that
// configuration.
func newAppHealthObserverFixture(store gatewayprovider.ExecutionProviderStore, cfg *config.Config) *provideradapter.UpstreamHealth {
	options := providercore.HealthOptions{}
	if cfg != nil {
		options.UnauthorizedCooldownMinutes = cfg.Provider.Cooldown.OAuth401Minutes
		options.OverloadMinutes = cfg.Provider.Cooldown.OverloadMinutes
		options.UsageMonitorIntervalMinutes = cfg.Provider.UsageMonitor.IntervalMinutes
	}
	return gatewaytestkit.NewHealthObserver(gatewaytestkit.HealthInput{Store: store, Options: options})
}

// newAuthRoutesTestRouter registers authentication and session routes with
// empty endpoint receivers and pass-through JWT and audit middleware. Redis
// supplies fixed-window limits, letting route tests inspect HTTP admission
// without a complete identity service.
func newAuthRoutesTestRouter(redisClient *redis.Client) *gin.Engine {
	router := gin.New()
	v1 := router.Group("/api/v1")

	RegisterAuthRoutes(
		v1,
		&routeTestHandlers{
			Auth:    &identityHTTP{AuthenticationHandler: &routeidentity.AuthenticationHandler{}, WeChatPaymentHandler: &paymenthttp.WeChatPaymentHandler{}},
			Passkey: &routeidentity.PasskeyHandler{},
		},
		routeidentity.JWTAuthMiddleware(func(c *gin.Context) {
			c.Next()
		}),
		servermiddleware.AuditLogMiddleware(func(c *gin.Context) {
			c.Next()
		}),
		servermiddleware.NewRateLimiter(redisinfra.NewFixedWindowLimiter(redisClient, "rate_limit:")),
		nil,
		nil,
	)

	return router
}

// newGatewayRoutesTestRouter constructs gateway route fixtures using an empty
// configuration and optional platform. It delegates platform defaults and
// protocol-set construction to newGatewayRoutesTestRouterWithOptions, using
// only the first platform argument.
func newGatewayRoutesTestRouter(platform ...string) *gin.Engine {
	return newGatewayRoutesTestRouterWithOptions(&config.Config{}, platform...)
}

// newGatewayRoutesTestRouterWithOptions creates gateway routes with the
// ordinary text protocols enabled. Empty or absent platform selects OpenAI;
// Gemini and Antigravity additionally enable Gemini generation, and extra
// platform arguments are ignored.
func newGatewayRoutesTestRouterWithOptions(cfg *config.Config, platform ...string) *gin.Engine {
	groupPlatform := capability.PlatformOpenAI
	if len(platform) > 0 && platform[0] != "" {
		groupPlatform = platform[0]
	}
	groupID := int64(1)
	// Enable the ordinary text protocol set for general route tests so
	// their requests can reach registered handlers; dedicated admission
	// cases provide nil or empty sets.
	protocols := []protocolcore.ProtocolID{
		protocolcore.ProtocolAnthropicMessages,
		protocolcore.ProtocolOpenAIResponses,
		protocolcore.ProtocolOpenAIChatCompletions,
	}
	if groupPlatform == capability.PlatformGemini || groupPlatform == capability.PlatformAntigravity {
		protocols = append(protocols, protocolcore.ProtocolGeminiGenerateContent)
	}
	return newGatewayRoutesTestRouterWithGroup(cfg, &routing.Group{
		ID:               groupID,
		AllowedProtocols: protocols,
	})
}

// newGatewayRoutesTestRouterWithGroup registers gateway routes using the
// supplied group and first optional models handler. Supply nonnil cfg for the
// default token-count receiver and nonnil group for authentication's ID read;
// nil and empty AllowedProtocols retain their distinct admission behavior.
func newGatewayRoutesTestRouterWithGroup(cfg *config.Config, group *routing.Group, models ...*gatewayhttp.ModelsHandler) *gin.Engine {
	router := gin.New()

	var modelsHTTP *gatewayhttp.ModelsHandler
	if len(models) > 0 {
		modelsHTTP = models[0]
	}
	RegisterGatewayRoutes(
		router,
		&routeTestHandlers{
			TextEnabled:   true,
			ModelsHTTP:    modelsHTTP,
			OpenAIEnabled: true,
		},
		keyhttp.APIKeyAuthMiddleware(func(c *gin.Context) {
			groupID := group.ID
			c.Set(string(keyhttp.ContextKeyAPIKey), &apikey.APIKey{
				User:    &identity.User{ID: 1, Status: billing.StatusActive, Concurrency: 1},
				GroupID: &groupID,
				Group:   group,
			})
			c.Set(string(authctx.ContextKeyUser), authctx.AuthSubject{UserID: 1, Concurrency: 1})
			c.Next()
		}),
		nil,
		nil,
		nil,
		nil,
		cfg,
	)

	return router
}

// Read marks the fixture body as read and immediately returns EOF. The marker
// lets admission tests distinguish rejection before body consumption from a
// handler that attempted any read.
func (r *protocolGateTrackingReader) Read(_ []byte) (int, error) {
	r.read = true
	return 0, io.EOF
}

// routeInventoryValue creates reflection values for registration inventories
// from the requested type. Pointer inputs receive allocated receivers; settable
// anonymous struct fields are populated while depth is below three, and struct
// function fields receive zero-result callbacks.
//
// Generated function callbacks return a zero value for every declared result
// without invoking application dependencies. Pointer allocation still occurs at
// the depth limit, but its embedded fields remain unset. Other kinds receive
// reflect.Zero, so these values establish method bindings and middleware
// signatures for inventory collection rather than usable production services.
// Callers use compatible factory types whose registration paths accept those
// fixture values.
func routeInventoryValue(kind reflect.Type, depth int) reflect.Value {
	if kind.Kind() == reflect.Pointer {
		value := reflect.New(kind.Elem())
		// Populate anonymous receiver pointers for inventory
		// registration through depth two; the next allocated receiver
		// stays empty so recursive embedding does not expand
		// indefinitely.
		if kind.Elem().Kind() == reflect.Struct && depth < 3 {
			for i := range value.Elem().NumField() {
				field := value.Elem().Field(i)
				if kind.Elem().Field(i).Anonymous && field.CanSet() {
					field.Set(routeInventoryValue(field.Type(), depth+1))
				}
			}
		}
		return value
	}
	if kind.Kind() == reflect.Func {
		return reflect.MakeFunc(kind, func([]reflect.Value) []reflect.Value {
			result := make([]reflect.Value, kind.NumOut())
			for i := range result {
				result[i] = reflect.Zero(kind.Out(i))
			}
			return result
		})
	}
	if kind.Kind() == reflect.Struct {
		value := reflect.New(kind).Elem()
		for _, field := range value.Fields() {
			if field.CanSet() && field.Kind() == reflect.Func {
				field.Set(routeInventoryValue(field.Type(), depth+1))
			}
		}
		return value
	}
	return reflect.Zero(kind)
}

// routeInventoryMount calls a registration factory with values from
// routeInventoryValue and asserts its first result has type T. The factory must
// be callable with a first result; generated receivers and callbacks support
// inventory construction, not service execution.
func routeInventoryMount[T any](t *testing.T, factory any) T {
	value := reflect.ValueOf(factory)
	args := make([]reflect.Value, value.Type().NumIn())
	for i := range args {
		args[i] = routeInventoryValue(value.Type().In(i), 0)
	}
	result, ok := reflect.TypeAssert[T](value.Call(args)[0])
	require.True(t, ok, "生产注册函数的返回类型不匹配")
	return result
}

// RegisterAdminRoutes mounts administrator fixture routes under the supplied
// version group. Authentication, panel limits, and auditing run in that order;
// route modules receive the supplied endpoint receivers, and selected mutations
// receive step-up middleware.
func RegisterAdminRoutes(
	v1 *gin.RouterGroup,
	h *routeTestHandlers,
	adminAuth routeidentity.AdminAuthMiddleware,
	auditLog servermiddleware.AuditLogMiddleware,
	stepUpAuth routeidentity.StepUpAuthMiddleware,
	panelRateLimiter *servermiddleware.PanelRateLimiter,
	protocolCatalog gin.HandlerFunc,
) {
	admin := v1.Group("/admin")
	admin.Use(gin.HandlerFunc(adminAuth))
	// Install panel admission between authentication and auditing so limits
	// can inspect the authenticated user before administrative handlers and
	// audit collection run.
	admin.Use(panelRateLimiter.Global())
	// Run auditing after authentication and panel admission so
	// administrative request recording uses the authenticated context and
	// the request accepted by panel limits.
	admin.Use(gin.HandlerFunc(auditLog))
	{
		// Provider and group forms share this read-only capability catalogue.
		admin.GET("/protocol-capabilities", protocolCatalog)

		registerDashboardRoutes(admin, h)

		registerUserManagementRoutes(admin, h)

		registerGroupRoutes(admin, h)

		registerProviderRoutes(admin, h, stepUpAuth)

		registerAnnouncementRoutes(admin, h)

		registerOpenAIOAuthRoutes(admin, h)

		registerGeminiOAuthRoutes(admin, h)

		registerAntigravityOAuthRoutes(admin, h)

		registerQoderOAuthRoutes(admin, h)

		registerGrokOAuthRoutes(admin, h)

		registerProxyRoutes(admin, h, stepUpAuth)

		registerRedeemCodeRoutes(admin, h)

		registerPromoCodeRoutes(admin, h)

		registerSettingsRoutes(admin, h)

		registerDataManagementRoutes(admin, h, stepUpAuth)

		registerBackupRoutes(admin, h, stepUpAuth)

		registerOpsRoutes(admin, h)

		registerSystemRoutes(admin, h)

		registerSubscriptionRoutes(admin, h)

		registerUsageRoutes(admin, h)

		registerUserAttributeRoutes(admin, h)

		registerErrorPassthroughRoutes(admin, h)

		registerTLSFingerprintProfileRoutes(admin, h)

		registerTLSFingerprintRouterRoutes(admin, h)

		registerAdminAPIKeyRoutes(admin, h)

		registerScheduledTestRoutes(admin, h)

		registerPricingConfigRoutes(admin, h)

		registerContentModerationRoutes(admin, h)

		registerAffiliateRoutes(admin, h)

		registerAuditLogRoutes(admin, h, stepUpAuth)

		teams := admin.Group("/teams")
		{
			teams.GET("", h.Admin.Team.List)
			teams.POST("", h.Admin.Team.Create)
			teams.GET("/:id", h.Admin.Team.Get)
			teams.GET("/:id/members", h.Admin.Team.ListMembers)
			teams.GET("/:id/usage", h.Admin.Team.GetUsage)
			teams.PATCH("/:id", h.Admin.Team.Update)
			teams.POST("/:id/force-transfer", gin.HandlerFunc(stepUpAuth), h.Admin.Team.ForceTransfer)
			teams.DELETE("/:id", gin.HandlerFunc(stepUpAuth), h.Admin.Team.Dissolve)
		}
	}
}

// registerAuditLogRoutes delegates audit-log registration with the fixture
// receiver; its step-up argument is unused.
func registerAuditLogRoutes(admin *gin.RouterGroup, h *routeTestHandlers, _ routeidentity.StepUpAuthMiddleware) {
	routeaudit.RegisterAuditLogRoutes(admin, h.Admin.AuditLog)
}

// registerAffiliateRoutes mounts referral rebate administration using the
// fixture's affiliate receiver.
func registerAffiliateRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routepromotion.RegisterAffiliateRoutes(admin, h.Admin.Affiliate)
}

// registerContentModerationRoutes mounts audit and moderation administration
// using the fixture receiver.
func registerContentModerationRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routemoderation.RegisterContentModerationRoutes(admin, h.Admin.ContentModeration)
}

// registerAdminAPIKeyRoutes mounts key administration using the fixture's
// administrator receiver.
func registerAdminAPIKeyRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	keyhttp.RegisterAdminAPIKeyRoutes(admin, h.Admin.APIKey)
}

// registerOpsRoutes mounts Ops administration endpoints using the fixture
// receiver.
func registerOpsRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeops.RegisterOpsRoutes(admin, h.Admin.Ops)
}

// registerDashboardRoutes mounts usage dashboard endpoints using the
// administrator fixture receiver.
func registerDashboardRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeusageadmin.RegisterDashboardRoutes(admin, h.Admin.Dashboard)
}

// registerUserManagementRoutes mounts user and user-attribute administration
// with both fixture receivers.
func registerUserManagementRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeidentity.RegisterUserManagementRoutes(admin, h.Admin.User, h.Admin.UserAttribute)
}

// registerGroupRoutes mounts routing group administration using the fixture
// receiver.
func registerGroupRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routerouting.RegisterGroupRoutes(admin, h.Admin.Group)
}

// registerProviderRoutes mounts provider administration with the fixture's
// management, import, OAuth, usage, and test receivers. It passes step-up
// middleware and adds scheduler diagnostics within the provider route group.
func registerProviderRoutes(admin *gin.RouterGroup, h *routeTestHandlers, stepUpAuth routeidentity.StepUpAuthMiddleware) {
	routeprovider.RegisterProviderRoutes(admin, routeprovider.ProviderRouteEndpoints{
		ProviderArchive:     h.Admin.ProviderArchive,
		ProviderCRS:         h.Admin.ProviderCRS,
		ProviderCodexImport: h.Admin.ProviderCodexImport,
		ProviderManagement:  h.Admin.ProviderManagement,
		ProviderOAuthUsage:  h.Admin.ProviderOAuthUsage,
		ProviderOllama:      h.Admin.ProviderOllama,
		ProviderTests:       h.Admin.ProviderTests,
		CodexInviteReset:    h.Admin.CodexInviteReset,
		OAuth:               h.Admin.OAuth,
		OpenAIOAuth:         h.Admin.OpenAIOAuth,
		UpstreamUsage:       h.Admin.UpstreamUsage,
	}, gin.HandlerFunc(stepUpAuth), func(providers *gin.RouterGroup) {
		routescheduler.RegisterProviderDiagnostics(providers, h.Admin.SchedulerDiagnostics)
	})
}

// registerAnnouncementRoutes mounts announcement administration using the
// fixture's site receiver.
func registerAnnouncementRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routesite.RegisterAnnouncementRoutes(admin, h.Admin.Announcement)
}

// registerOpenAIOAuthRoutes mounts OpenAI OAuth administration using the
// fixture receiver.
func registerOpenAIOAuthRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeprovider.RegisterOpenAIOAuthRoutes(admin, h.Admin.OpenAIOAuth)
}

// registerGeminiOAuthRoutes mounts Gemini OAuth administration using the
// fixture receiver.
func registerGeminiOAuthRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeprovider.RegisterGeminiOAuthRoutes(admin, h.Admin.GeminiOAuth)
}

// registerAntigravityOAuthRoutes mounts Antigravity OAuth administration using
// the fixture receiver.
func registerAntigravityOAuthRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeprovider.RegisterAntigravityOAuthRoutes(admin, h.Admin.AntigravityOAuth)
}

// registerQoderOAuthRoutes mounts Qoder OAuth administration using the fixture
// receiver.
func registerQoderOAuthRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeprovider.RegisterQoderOAuthRoutes(admin, h.Admin.QoderOAuth)
}

// registerGrokOAuthRoutes mounts Grok OAuth administration using the fixture
// receiver.
func registerGrokOAuthRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeprovider.RegisterGrokOAuthRoutes(admin, h.Admin.GrokOAuth)
}

// registerProxyRoutes mounts proxy administration with the fixture receiver and
// converted step-up middleware.
func registerProxyRoutes(admin *gin.RouterGroup, h *routeTestHandlers, stepUpAuth routeidentity.StepUpAuthMiddleware) {
	routeegress.RegisterProxyRoutes(admin, h.Admin.Proxy, gin.HandlerFunc(stepUpAuth))
}

// registerRedeemCodeRoutes mounts redemption administration using the fixture's
// billing receiver.
func registerRedeemCodeRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routebilling.RegisterRedeemCodeRoutes(admin, h.Admin.Redeem)
}

// registerPromoCodeRoutes mounts promotional-code administration using the
// fixture's promotion receiver.
func registerPromoCodeRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routepromotion.RegisterPromoCodeRoutes(admin, h.Admin.Promo)
}

// registerDataManagementRoutes mounts data administration with the fixture
// receiver and converted step-up middleware.
func registerDataManagementRoutes(admin *gin.RouterGroup, h *routeTestHandlers, stepUpAuth routeidentity.StepUpAuthMiddleware) {
	routebackup.RegisterDataManagementRoutes(admin, h.Admin.DataManagement, gin.HandlerFunc(stepUpAuth))
}

// registerBackupRoutes mounts backup administration with the fixture receiver
// and converted step-up middleware.
func registerBackupRoutes(admin *gin.RouterGroup, h *routeTestHandlers, stepUpAuth routeidentity.StepUpAuthMiddleware) {
	routebackup.RegisterBackupRoutes(admin, h.Admin.Backup, gin.HandlerFunc(stepUpAuth))
}

// registerSystemRoutes mounts system administration endpoints using the fixture
// receiver.
func registerSystemRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeops.RegisterSystemRoutes(admin, h.Admin.System)
}

// registerSubscriptionRoutes mounts subscription administration using the
// fixture's billing receiver.
func registerSubscriptionRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routebilling.RegisterSubscriptionRoutes(admin, h.Admin.Subscription)
}

// registerUsageRoutes mounts usage administration endpoints using the fixture
// receiver.
func registerUsageRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeusageadmin.RegisterUsageRoutes(admin, h.Admin.Usage)
}

// registerUserAttributeRoutes mounts user-attribute administration using the
// fixture's identity receiver.
func registerUserAttributeRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeidentity.RegisterUserAttributeRoutes(admin, h.Admin.UserAttribute)
}

// registerScheduledTestRoutes mounts scheduled provider tests using the fixture
// receiver.
func registerScheduledTestRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeprovider.RegisterScheduledTestRoutes(admin, h.Admin.ScheduledTest)
}

// registerErrorPassthroughRoutes mounts error-policy administration using the
// fixture's gateway receiver.
func registerErrorPassthroughRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	gatewayhttp.RegisterErrorPassthroughRoutes(admin, h.Admin.ErrorPassthrough)
}

// registerTLSFingerprintProfileRoutes mounts TLS profile administration using
// the fixture's egress receiver.
func registerTLSFingerprintProfileRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeegress.RegisterTLSFingerprintProfileRoutes(admin, h.Admin.TLSFingerprintProfile)
}

// registerTLSFingerprintRouterRoutes mounts TLS router administration using the
// fixture's egress receiver.
func registerTLSFingerprintRouterRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routeegress.RegisterTLSFingerprintRouterRoutes(admin, h.Admin.TLSFingerprintRouter)
}

// registerPricingConfigRoutes mounts pricing configuration administration using
// the fixture's routing receiver.
func registerPricingConfigRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	routerouting.RegisterPricingRoutes(admin, h.Admin.PricingConfig)
}

// registerSettingsRoutes mounts settings endpoints using empty module receivers
// plus the fixture's notification and search handlers. These registrations
// support route inventory assertions; their receivers require real dependencies
// for successful settings operations.
func registerSettingsRoutes(admin *gin.RouterGroup, h *routeTestHandlers) {
	group := admin.Group("/settings")
	routesettings.RegisterSettingsSettingsRoutes(group, &routesettings.Handler{}, &routesettings.PreAggregationHandler{})
	routecreative.RegisterCreativeSettingsRoutes(group, &routecreative.SettingsHandler{})
	routeidentity.RegisterIdentitySettingsRoutes(group, &routeidentity.AdminKeySettingsHandler{})
	routeprovider.RegisterProviderSettingsRoutes(group, &routeprovider.RuntimeSettingsHandler{})
	serverhttp.RegisterPanelSettingsRoutes(group, &serverhttp.PanelSettingsHandler{})
	gatewayhttp.RegisterGatewaySettingsRoutes(group, &gatewayhttp.RuntimeSettingsHandler{})
	routenotification.RegisterSettingsRoutes(group, h.Notification)
	routesearch.RegisterSettingsRoutes(group, h.Search)
}

// RegisterAuthRoutes mounts authentication, sessions, public settings,
// unsubscribe, and marketplace fixture routes. It combines JWT and audit
// middleware with backend-mode guards, panel limits, and fail-closed endpoint
// limits, then adds WeChat authentication through the shared identity receiver.
// @project-doc docs/domains/identity_and_tenancy.md#authentication_boundaries
func RegisterAuthRoutes(
	v1 *gin.RouterGroup,
	h *routeTestHandlers,
	jwtAuth routeidentity.JWTAuthMiddleware,
	auditLog servermiddleware.AuditLogMiddleware,
	rateLimiter *servermiddleware.RateLimiter,
	settingService *admission.BackendMode,
	panelRateLimiter *servermiddleware.PanelRateLimiter,
) {
	guards := routeidentity.AuthRouteMiddleware{JWT: gin.HandlerFunc(jwtAuth), Audit: gin.HandlerFunc(auditLog), BackendAuth: routeidentity.BackendModeAuthGuard(legacyBackendModeReader(settingService)), BackendUser: routeidentity.BackendModeUserGuard(legacyBackendModeReader(settingService)), Panel: panelRateLimiter.Global(), Limit: func(key string, n int, window time.Duration) gin.HandlerFunc {
		return rateLimiter.LimitWithOptions(key, n, window, servermiddleware.RateLimitOptions{FailureMode: servermiddleware.RateLimitFailClose})
	}}
	routeidentity.RegisterAuthenticationRoutes(v1, h.Auth, h.Passkey, guards, func(group *gin.RouterGroup) { paymenthttp.RegisterWeChatAuthRoutes(group, h.Auth) })
	public := v1.Group("/settings")
	public.Use(panelRateLimiter.PublicIP())
	routesite.RegisterPublicSettingsRoutes(public, h.PublicSettings)
	routenotification.RegisterUnsubscribeRoute(public, h.Notification)
	routerouting.RegisterPublicMarketplaceRoutes(v1, h.ModelMarketplace)
	routeidentity.RegisterSessionRoutes(v1, h.Auth, guards)
}

// legacyBackendModeReader converts an optional BackendMode pointer to the route
// reader interface. Nil returns a nil interface so middleware can recognize an
// absent reader; a nonnil pointer is passed through.
func legacyBackendModeReader(s *admission.BackendMode) routeidentity.BackendModeReader {
	if s == nil {
		return nil
	}
	return s
}

// legacyRouteMiddleware binds gateway middleware through application providers,
// then replaces Anthropic and Google group-assignment checks for the fixture.
// Settings is unused; the checks inspect the loaded key, its group assignment,
// and the composite no-group marker, and record client-business and ingress
// rejection observations.
func legacyRouteMiddleware(auth keyhttp.APIKeyAuthMiddleware, keys *apikey.APIKeyService, subscriptions *billing.SubscriptionService, ops *opscore.OpsService, settings *routing.RuntimeSettings, cfg *config.Config) gatewayhttp.RouteMiddleware {
	var native *apikey.APIKeyService
	if keys != nil {
		native = keys
	}
	value := provideGatewayRouteMiddleware(auth, native, subscriptions, ops, cfg, nil, nil, nil, nil)
	options := gatewayhttp.GroupAssignmentOptions{Access: func(c *gin.Context) gatewayhttp.GroupAssignmentAccess {
		key, ok := keyhttp.GetAPIKeyFromContext(c)
		if !ok || key == nil {
			return gatewayhttp.GroupAssignmentAccess{}
		}
		_, noGroup := c.Get(gatewayhttp.CompositeKeyNoGroupContextKey)
		return gatewayhttp.GroupAssignmentAccess{Loaded: true, Assigned: key.GroupID != nil, CompositeNoGroup: key.IsComposite && noGroup}
	}, Rejected: func(c *gin.Context) {
		gatewayhttp.MarkOpsClientBusinessLimited(c, gatewayhttp.OpsClientBusinessLimitedReasonAPIKeyGroupUnassigned)
		servermiddleware.MarkIngressRejected(c, servermiddleware.IngressRejectGroupUnassigned)
	}}
	options.WriteError = gatewayhttp.AnthropicErrorWriter
	value.RequireGroupAnthropic = gatewayhttp.RequireGroupAssignment(options)
	options.WriteError = gatewayhttp.GoogleErrorWriter
	value.RequireGroupGoogle = gatewayhttp.RequireGroupAssignment(options)
	return value
}

// RegisterGatewayRoutes registers gateway fixture endpoints with supplied
// receivers or constructed defaults. TextEnabled and OpenAIEnabled control
// their corresponding defaults; token, Qoder-compatible, and public-usage
// fixtures also have unconditional fallback construction. Supply nonnil cfg
// when TextEnabled requests the default CountTokensHTTP receiver because its
// body limit is read directly.
//
// Supplied endpoint receivers take precedence, while enabled missing handlers
// use application providers with minimal dependencies for rejection and route
// tests. Text routes share one request activity and message runtime, and OpenAI
// text and media share their attempt bindings. The function passes method
// bindings and authentication middleware to the production route registrar,
// including batch-image routes; successful forwarding still requires usable
// execution dependencies.
func RegisterGatewayRoutes(
	r *gin.Engine,
	h *routeTestHandlers,
	apiKeyAuth keyhttp.APIKeyAuthMiddleware,
	apiKeyService *apikey.APIKeyService,
	subscriptionService *billing.SubscriptionService,
	opsService *opscore.OpsService,
	settingService *routing.RuntimeSettings,
	cfg *config.Config,
) {
	// Build enabled text receivers around one request activity so route
	// admission and shutdown use the same fixture lifetime before minimal
	// dependency rejection.
	var shared *messageHTTPBindings
	var runtime *textattempt.Runtime
	var activity *gatewayRequestActivity
	if h.TextEnabled {
		shared = provideMessageHTTPBindings(gatewayprovider.NewRoutePlanner(nil), nil, provideSchedulerSharedState(nil, nil), nil, nil, nil, nil, nil, nil, cfg, nil, nil, nil)
		runtime = textattempt.New(textattempt.Bindings{})
		activity = &gatewayRequestActivity{Operations: lifecycle.NewOperations("route-fixture")}
	}
	openAITokensHTTP := h.OpenAITokensHTTP
	if openAITokensHTTP == nil {
		openAITokensHTTP = provideOpenAITokensHTTP(nil, nil, nil, nil, nil, cfg, nil, nil, nil, nil, nil)
	}
	countTokensHTTP := h.CountTokensHTTP
	if countTokensHTTP == nil && h.TextEnabled {
		countTokensHTTP = gatewayhttp.NewCountTokensHandler(cfg.Gateway.MaxRequestBodyBytes, 0, gatewayhttp.CountHTTPPorts{ReadAccess: keyhttp.GetAPIKeyFromContext, Funding: routeCountUnavailable{}, ObserveCompatibility: func(*zap.Logger) {}}, (*promptpolicy.Service)(nil))
	}
	geminiNativeHTTP := h.GeminiNativeHTTP
	if geminiNativeHTTP == nil && h.TextEnabled {
		geminiNativeHTTP = provideGeminiNativeHTTP(shared, nil, runtime, activity, nil)
	}
	commonOpenAI := provideOpenAIAttemptBindings(nil, nil, nil, nil, nil, nil, GatewayCompletionRecorders{}, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	openAIRuntime := provideOpenAITextAttemptRuntime(commonOpenAI)
	mediaRuntime := provideMediaRuntime(nil, nil, nil, nil, nil, commonOpenAI, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	openAITextHTTP := h.OpenAITextHTTP
	if openAITextHTTP == nil && h.OpenAIEnabled {
		openAITextHTTP = provideOpenAITextHTTP(nil, nil, nil, nil, nil, nil, nil, nil, nil, openAIRuntime, activity, nil, nil, nil, nil)
	}
	responsesWSHTTP := h.ResponsesWSHTTP
	if responsesWSHTTP == nil && h.OpenAIEnabled {
		responsesWSHTTP = provideResponsesWSHTTP(nil, nil, nil, nil, commonOpenAI, nil, nil, nil, activity, nil, nil, nil, nil)
	}
	modelsHTTP := h.ModelsHTTP
	if modelsHTTP == nil && h.TextEnabled {
		modelsHTTP = provideModelsHTTP(nil, nil, nil, nil, nil, nil)
	}
	messagesHTTP := h.MessagesHTTP
	if messagesHTTP == nil && h.TextEnabled {
		messagesHTTP = provideMessagesHTTP(shared, runtime, activity)
	}

	mediaHTTP, auxiliaryHTTP, liveHTTP, searchHTTP := h.MediaHTTP, h.AuxiliaryHTTP, h.LiveHTTP, h.SearchHTTP
	if h.OpenAIEnabled {
		if mediaHTTP == nil {
			mediaHTTP = provideMediaHTTP(mediaRuntime, activity)
		}
		if auxiliaryHTTP == nil {
			auxiliaryHTTP = provideAuxiliaryHTTP(mediaRuntime, activity)
		}
		if liveHTTP == nil {
			liveHTTP = provideLiveHTTP(nil, nil, nil, nil, nil)
		}
	}
	if searchHTTP == nil && h.TextEnabled {
		searchHTTP = gatewayhttp.NewSearchHandler(gatewayhttp.SearchPorts{})
	}

	publicUsage := usagehttp.NewPublicUsageHandler(nil, nil, nil, nil, usagehttp.PublicUsageContext{
		Key: keyhttp.GetAPIKeyFromContext,
		Billing: func(c *gin.Context) (*billing.APIKeyBillingContext, bool) {
			return gatewayhttp.GetAPIKeyBillingContext(c)
		},
		Subscription: gatewayhttp.SubscriptionFromContext,
	}, timezone.NewCalendar(time.Local)).Usage
	if h.PublicUsage != nil {
		publicUsage = h.PublicUsage.Usage
	}
	gatewayhttp.RegisterGatewayRoutes(r, gatewayhttp.RouteEndpoints{CountTokens: countTokensHTTP, GeminiNative: geminiNativeHTTP, OpenAIText: openAITextHTTP, OpenAITokens: openAITokensHTTP, ResponsesWS: responsesWSHTTP.ResponsesWebSocket, Models: modelsHTTP, Messages: messagesHTTP, Media: mediaHTTP, Auxiliary: auxiliaryHTTP, Live: liveHTTP, Search: searchHTTP, PublicUsage: publicUsage}, legacyRouteMiddleware(apiKeyAuth, apiKeyService, subscriptionService, opsService, settingService, cfg), func(group *gin.RouterGroup) { batchhttp.RegisterGatewayRoutes(group, h.BatchImage) })
}

// CheckKey returns billing-service unavailability for every token-count fixture
// request.
func (routeCountUnavailable) CheckKey(context.Context, *apikey.APIKey, *billing.UserSubscription, string, bool) error {
	return billing.ErrBillingServiceUnavailable
}

// withSelectionGroupFixture adds membership completion to an execution store
// unless it declares completeGroupProjection. Nil remains nil; otherwise a
// wrapper remembers memberships filled during batch selection for later
// provider rereads.
func withSelectionGroupFixture(source gatewayprovider.ExecutionProviderStore) gatewayprovider.ExecutionProviderStore {
	if _, ok := source.(interface {
		// completeGroupProjection marks stores whose group memberships need no fixture completion.
		completeGroupProjection()
	}); ok {
		return source
	}
	if source == nil {
		return nil
	}
	return &selectionGroupFixture{ExecutionProviderStore: source, groups: make(map[int64][]int64)}
}

// ListSchedulableByGroupIDAndPlatforms filters a single-platform fixture read
// and fills empty group membership with id. Existing memberships pass through
// intact, allowing production selection to reject an out-of-group candidate.
//
// Each returned record is converted through NewExecutionProvider before
// membership completion. Filled membership is saved under the wrapper mutex by
// provider ID, and another missing-group selection for that ID replaces the
// saved association. GetByID therefore uses the last remembered membership
// rather than a per-request association. Tests serialize those selections when
// they need the filled group to survive a reread. Delegate errors pass through,
// and empty platform lists produce no candidates.
func (s *selectionGroupFixture) ListSchedulableByGroupIDAndPlatforms(ctx context.Context, id int64, platforms []string) ([]gatewayprovider.ExecutionProvider, error) {
	values, err := s.ListSchedulableByGroupIDAndPlatform(ctx, id, "")
	if err != nil {
		return nil, err
	}
	var result []gatewayprovider.ExecutionProvider
	for _, value := range values {
		if !slices.Contains(platforms, value.Record.Platform) {
			continue
		}
		copy := *gatewayprovider.NewExecutionProvider(&value.Record)
		// Fill membership only when the returned record has no groups;
		// retaining existing memberships lets the real selector detect
		// candidates belonging to another group.
		if len(copy.Record.GroupIDs) == 0 {
			copy.Record.GroupIDs = []int64{id}
			// Save the filled association under its reader mutex so
			// later GetByID can recover the last group remembered
			// for that provider without a map race.
			s.mu.Lock()
			s.groups[copy.Record.ID] = []int64{id}
			s.mu.Unlock()
		}
		result = append(result, copy)
	}
	return result, nil
}

// ListSchedulableByGroupID delegates membership completion with the fixture's
// fixed nine-platform list. It includes Anthropic, OpenAI, Gemini, Antigravity,
// Qoder, Grok, Kimi, Zhipu, and DeepSeek for generic selection tests.
func (s *selectionGroupFixture) ListSchedulableByGroupID(ctx context.Context, id int64) ([]gatewayprovider.ExecutionProvider, error) {
	return s.ListSchedulableByGroupIDAndPlatforms(ctx, id, []string{"anthropic", "openai", "gemini", "antigravity", "qoder", "grok", "kimi", "zhipu", "deepseek"})
}

// GetByID rereads and converts the provider record, then fills absent
// membership from the wrapper's remembered provider-ID entry. Delegate errors
// and nil values pass through, and existing membership takes precedence over
// the saved test association.
func (s *selectionGroupFixture) GetByID(ctx context.Context, id int64) (*gatewayprovider.ExecutionProvider, error) {
	value, err := s.ExecutionProviderStore.GetByID(ctx, id)
	if value == nil || err != nil {
		return value, err
	}
	value = gatewayprovider.NewExecutionProvider(&value.Record)
	if len(value.Record.GroupIDs) == 0 {
		s.mu.Lock()
		// Clone the remembered membership while holding its mutex so a
		// later selected-group replacement cannot modify the slice
		// already returned for this provider reread.
		value.Record.GroupIDs = slices.Clone(s.groups[id])
		s.mu.Unlock()
	}
	return value, nil
}

// GetValue increments the settings-read observation and returns
// ErrSettingNotFound for every key. Callers use this serial fixture to
// distinguish initial repository reads from later cached settings access.
func (s *readerStoreProbe) GetValue(context.Context, string) (string, error) {
	s.reads++
	return "", settingscore.ErrSettingNotFound
}

// Get panics to reject unexpected record lookup in the settings fixture.
func (s *settingHandlerRepoStub) Get(ctx context.Context, key string) (*settingscore.Setting, error) {
	panic("unexpected Get call")
}

// GetValue returns a stored fixture string or an empty string without error
// when absent. This differs from ErrSettingNotFound readers, and callers
// serialize access to the mutable settings map.
func (s *settingHandlerRepoStub) GetValue(ctx context.Context, key string) (string, error) {
	if s.values != nil {
		if value, ok := s.values[key]; ok {
			return value, nil
		}
	}
	return "", nil
}

// Set panics to reject unexpected single-key writes in the fixture.
func (s *settingHandlerRepoStub) Set(ctx context.Context, key, value string) error {
	panic("unexpected Set call")
}

// GetMultiple returns a new map of requested stored values, omitting absent
// keys. Present empty values remain in the result, and input order does not
// change that map representation.
func (s *settingHandlerRepoStub) GetMultiple(ctx context.Context, keys []string) (map[string]string, error) {
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		if value, ok := s.values[key]; ok {
			out[key] = value
		}
	}
	return out, nil
}

// SetMultiple copies the input into a fresh lastUpdates map and merges the same
// strings into values. Empty input still replaces lastUpdates with a nonnil
// empty map; unrelated stored values remain available.
func (s *settingHandlerRepoStub) SetMultiple(ctx context.Context, settings map[string]string) error {
	s.lastUpdates = make(map[string]string, len(settings))
	for key, value := range settings {
		s.lastUpdates[key] = value
		if s.values == nil {
			s.values = map[string]string{}
		}
		s.values[key] = value
	}
	return nil
}

// GetAll returns a new map containing every stored fixture string. Callers can
// change this outer result independently, while reads and writes of the stored
// map remain serial fixture operations.
func (s *settingHandlerRepoStub) GetAll(ctx context.Context) (map[string]string, error) {
	out := make(map[string]string, len(s.values))
	maps.Copy(out, s.values)
	return out, nil
}

// Delete panics to reject unexpected deletion from the settings fixture.
func (s *settingHandlerRepoStub) Delete(ctx context.Context, key string) error {
	panic("unexpected Delete call")
}

// doUpdateSettings marshals the supplied values into an administrator settings
// PUT request, optionally invokes prepare, and calls UpdateSettings
// synchronously. The callback can install authorization context before the
// handler runs; the returned recorder exposes its response.
func doUpdateSettings(t *testing.T, h *routesettings.Handler, body map[string]any, prepare func(c *gin.Context)) *httptest.ResponseRecorder {
	t.Helper()
	rawBody, err := json.Marshal(body)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPut, "/api/v1/admin/settings", bytes.NewReader(rawBody))
	c.Request.Header.Set("Content-Type", "application/json")
	if prepare != nil {
		prepare(c)
	}

	h.UpdateSettings(c)
	return rec
}

// GetByKeyForAuth returns a detached key only for the configured credential.
// Unknown credentials retain the production repository's missing-key result, so
// HTTP authentication still owns rejection and identity publication.
func (r *openCodeHTTPKeys) GetByKeyForAuth(_ context.Context, credential string) (*apikey.APIKey, error) {
	if credential != r.key.Key {
		return nil, apikey.ErrAPIKeyNotFound
	}
	return apikey.CopyAPIKey(r.key), nil
}

// UpdateLastUsed accepts diagnostic touches after successful fixture authentication.
func (*openCodeHTTPKeys) UpdateLastUsed(context.Context, int64, time.Time) error { return nil }

// Do records the provider ID under its mutex, then forwards the request with
// http.DefaultClient. Proxy and concurrency arguments are ignored; successful
// responses retain the default client's body-cleanup obligation.
func (s *mixedHTTPTransport) Do(req *http.Request, _ string, id int64, _ int) (*http.Response, error) {
	// Record selection before forwarding, then unlock so independent HTTP
	// exchanges can progress while assertions read a consistent provider call sequence.
	s.mu.Lock()
	s.providers = append(s.providers, id)
	s.mu.Unlock()
	if s.failure != nil {
		if err := s.failure(req, id); err != nil {
			return nil, err
		}
	}
	return http.DefaultClient.Do(req)
}

// DoWithTLS delegates to Do while ignoring the fingerprint profile supplied by
// execution. Provider observations and default-client forwarding remain the
// same, and callers close successful response bodies.
func (s *mixedHTTPTransport) DoWithTLS(req *http.Request, p string, id int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return s.Do(req, p, id, n)
}

// calls returns a cloned provider-ID sequence while holding its mutex. The
// result keeps nil for an uninitialized sequence and gives callers separate
// slice storage for observation.
func (s *mixedHTTPTransport) calls() []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.providers)
}

// mixedUpstreamResponse returns fixed successful bodies selected by the
// upstream URL and stream request. failAnthropic makes Messages URLs return
// HTTP 502 first; response-write errors are left to the outer HTTP-result
// assertions.
func mixedUpstreamResponse(w http.ResponseWriter, r *http.Request, failAnthropic bool) {
	// Use any bytes read from the fixture request to choose its stream
	// shape; the outer endpoint assertions detect an unusable exchange after read failure.
	body, _ := io.ReadAll(r.Body)
	if failAnthropic && strings.Contains(r.URL.Path, "/messages") {
		w.WriteHeader(http.StatusBadGateway)
		// Send the scripted protocol body; the outer HTTP result and
		// completion assertions expose unsuccessful delivery, so
		// fixture writes discard their errors.
		_, _ = io.WriteString(w, `{"error":{"type":"overloaded_error","message":"retry another provider"}}`)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if strings.Contains(r.URL.Path, "/messages") {
		if gjson.GetBytes(body, "stream").Bool() {
			w.Header().Set("Content-Type", "text/event-stream")
			// Send the scripted protocol body; the outer HTTP
			// result and completion assertions expose unsuccessful
			// delivery, so fixture writes discard their errors.
			_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-mixed\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"claude-sonnet-4-5-20250929\",\"content\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok-mixed\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
		} else {
			// Send the scripted protocol body; the outer HTTP
			// result and completion assertions expose unsuccessful
			// delivery, so fixture writes discard their errors.
			_, _ = io.WriteString(w, `{"id":"msg-mixed","type":"message","role":"assistant","model":"claude-sonnet-4-5-20250929","content":[{"type":"text","text":"ok-mixed"}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":3}}`)
		}
	} else if strings.Contains(r.URL.Path, "models/") {
		response := `{"candidates":[{"content":{"role":"model","parts":[{"text":"ok-mixed"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":3,"totalTokenCount":13}}`
		if strings.Contains(r.URL.Path, "streamGenerateContent") {
			w.Header().Set("Content-Type", "text/event-stream")
			// Send the scripted protocol body; the outer HTTP
			// result and completion assertions expose unsuccessful
			// delivery, so fixture writes discard their errors.
			_, _ = fmt.Fprintf(w, "data: %s\n\n", response)
		} else {
			// Send the scripted protocol body; the outer HTTP
			// result and completion assertions expose unsuccessful
			// delivery, so fixture writes discard their errors.
			_, _ = io.WriteString(w, response)
		}
	} else if strings.Contains(r.URL.Path, "chat/completions") {
		// Send the scripted protocol body; the outer HTTP result and
		// completion assertions expose unsuccessful delivery, so
		// fixture writes discard their errors.
		_, _ = io.WriteString(w, `{"id":"chat-mixed","object":"chat.completion","model":"gpt-5.4","choices":[{"index":0,"message":{"role":"assistant","content":"ok-mixed"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":3,"total_tokens":13}}`)
	} else {
		response := `{"id":"resp-mixed","object":"response","status":"completed","model":"gpt-5.4","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok-mixed"}]}],"usage":{"input_tokens":10,"output_tokens":3,"total_tokens":13}}`
		if gjson.GetBytes(body, "stream").Bool() {
			w.Header().Set("Content-Type", "text/event-stream")
			// Send the scripted protocol body; the outer HTTP
			// result and completion assertions expose unsuccessful
			// delivery, so fixture writes discard their errors.
			_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-mixed\",\"model\":\"gpt-5.4\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok-mixed\"}\n\ndata: {\"type\":\"response.completed\",\"response\":%s}\n\n", response)
		} else {
			// Send the scripted protocol body; the outer HTTP
			// result and completion assertions expose unsuccessful
			// delivery, so fixture writes discard their errors.
			_, _ = io.WriteString(w, response)
		}
	}
}

// Do executes req through the lab UpstreamPool with the configured header timeout.
// Matching hostOverrides or provider-scoped Bedrock, Vertex, and Anthropic OAuth
// base_url overrides redirect outbound scheme and host to local httptest servers
// while preserving signed Host and Authorization headers.
func (t *protocolLabTransport) Do(req *http.Request, proxy string, id int64, _ int) (*http.Response, error) {
	if req != nil && req.URL != nil {
		base := ""
		if len(t.hostOverrides) > 0 {
			base = strings.TrimSpace(t.hostOverrides[strings.ToLower(req.URL.Hostname())])
		}
		if base == "" && id > 0 && t.providers != nil {
			base = t.localEndpointOverride(id)
		}
		if base != "" {
			if parsed, err := url.Parse(base); err == nil && parsed.Host != "" {
				cloned := req.Clone(req.Context())
				if cloned.Host == "" {
					cloned.Host = req.URL.Host
				}
				cloned.URL.Scheme = parsed.Scheme
				cloned.URL.Host = parsed.Host
				req = cloned
			}
		}
	}
	return t.pool.Do(req, httpclient.UpstreamRequestOptions{
		ProxyURL:   proxy,
		ProviderID: id,
		Settings:   httpclient.UpstreamSettings{ResponseHeaderTimeout: t.headerTimeout},
	})
}

// localEndpointOverride returns a non-empty base_url credential for Bedrock,
// Vertex service_account, Qoder cosy, or OpenAI, Anthropic, Gemini, and
// Antigravity OAuth providers whose request builders otherwise target fixed
// cloud hostnames. Providers without a configured base_url or outside those
// auth types return an empty string.
func (t *protocolLabTransport) localEndpointOverride(id int64) string {
	t.providers.mu.RLock()
	defer t.providers.mu.RUnlock()
	for i := range t.providers.values {
		rec := &t.providers.values[i].Record
		if rec.ID != id {
			continue
		}
		if rec.Type == capability.ProviderTypeBedrock ||
			rec.Type == capability.ProviderTypeServiceAccount ||
			(rec.Platform == capability.PlatformQoder && rec.Type == capability.ProviderTypeCosy) ||
			(rec.Platform == capability.PlatformOpenAI && rec.Type == capability.ProviderTypeOAuth) ||
			(rec.Platform == capability.PlatformGemini && rec.Type == capability.ProviderTypeOAuth) ||
			(rec.Platform == capability.PlatformAntigravity && rec.Type == capability.ProviderTypeOAuth) ||
			(rec.Platform == capability.PlatformAnthropic && (rec.Type == capability.ProviderTypeOAuth || rec.Type == capability.ProviderTypeSetupToken)) {
			return strings.TrimSpace(rec.GetCredential("base_url"))
		}
		return ""
	}
	return ""
}

// DoWithTLS delegates fingerprinted outbound requests to Do over local HTTP test servers.
func (t *protocolLabTransport) DoWithTLS(req *http.Request, proxy string, id int64, n int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return t.Do(req, proxy, id, n)
}

// Current reports that no external search provider is configured in the lab fixture.
func (protocolLabNoSearch) Current() searchtools.Searcher { return nil }

// newProtocolLabGatewayCache constructs an empty concurrency-safe in-memory
// session.GatewayCache for protocol lab routers.
func newProtocolLabGatewayCache() *protocolLabGatewayCache {
	return &protocolLabGatewayCache{
		sessionBindings: make(map[string]int64),
		ownerGroups:     make(map[string]int64),
	}
}

// GetSessionProviderID looks up the sticky provider ID bound to (groupID, sessionHash)
// under c.mu. Unbound session hashes return a non-nil error so selectors fall
// back to normal candidate scheduling.
func (c *protocolLabGatewayCache) GetSessionProviderID(_ context.Context, groupID int64, sessionHash string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%d\x00%s", groupID, sessionHash)
	if id, ok := c.sessionBindings[key]; ok {
		return id, nil
	}
	return 0, errors.New("session binding not found")
}

// SetSessionProviderID binds (groupID, sessionHash) to providerID in memory under c.mu.
func (c *protocolLabGatewayCache) SetSessionProviderID(_ context.Context, groupID int64, sessionHash string, providerID int64, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sessionBindings[fmt.Sprintf("%d\x00%s", groupID, sessionHash)] = providerID
	return nil
}

// RefreshSessionTTL accepts sticky-session TTL refresh calls as a no-op in memory.
func (c *protocolLabGatewayCache) RefreshSessionTTL(context.Context, int64, string, time.Duration) error {
	return nil
}

// DeleteSessionProviderID removes any sticky provider binding recorded for (groupID, sessionHash).
func (c *protocolLabGatewayCache) DeleteSessionProviderID(_ context.Context, groupID int64, sessionHash string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sessionBindings, fmt.Sprintf("%d\x00%s", groupID, sessionHash))
	return nil
}

// SetSessionOwnerGroupID claims (userID, source, sessionHash) for groupID with
// SetNX semantics under c.mu. Subsequent claims for an already-owned session
// return false without overwriting the first owner.
func (c *protocolLabGatewayCache) SetSessionOwnerGroupID(_ context.Context, userID int64, source, sessionHash string, groupID int64, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%d\x00%s\x00%s", userID, source, sessionHash)
	if _, exists := c.ownerGroups[key]; exists {
		return false, nil
	}
	c.ownerGroups[key] = groupID
	return true, nil
}

// GetSessionOwnerGroupID reads the owning group ID recorded for (userID, source, sessionHash)
// under c.mu. Unclaimed composite keys return a non-nil error so session-owner
// admission guards observe a cache miss.
func (c *protocolLabGatewayCache) GetSessionOwnerGroupID(_ context.Context, userID int64, source, sessionHash string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := fmt.Sprintf("%d\x00%s\x00%s", userID, source, sessionHash)
	if id, exists := c.ownerGroups[key]; exists {
		return id, nil
	}
	return 0, errors.New("session owner not found")
}

// RefreshSessionOwnerTTL accepts session-owner TTL refresh calls as a no-op in memory.
func (c *protocolLabGatewayCache) RefreshSessionOwnerTTL(context.Context, int64, string, string, time.Duration) error {
	return nil
}

// GetByID returns a detached copy of the lab router's active routing group when
// id matches the configured group ID or is nonpositive. Mismatched positive
// group IDs return (nil, nil) without error.
func (g protocolLabGroupRepo) GetByID(_ context.Context, id int64) (*routing.Group, error) {
	return g.router.cloneGroup(id), nil
}

// GetByIDLite returns a detached copy of the lab router's active routing group
// when id matches the configured group ID or is nonpositive. Mismatched
// positive group IDs return (nil, nil) without error.
func (g protocolLabGroupRepo) GetByIDLite(_ context.Context, id int64) (*routing.Group, error) {
	return g.router.cloneGroup(id), nil
}

// completeGroupProjection marks protocolLabProviderStore as carrying explicit
// group memberships so withSelectionGroupFixture does not wrap it.
func (*protocolLabProviderStore) completeGroupProjection() {}

// GetByID returns a detached ExecutionProvider copy reflecting live health and
// cooldown fields when id exists in s.values. Unknown provider IDs return
// (nil, nil) so retry cooldown and selection callers treat them as absent.
func (s *protocolLabProviderStore) GetByID(_ context.Context, id int64) (*gatewayprovider.ExecutionProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for i := range s.values {
		if s.values[i].Record.ID == id {
			return gatewayprovider.NewExecutionProvider(&s.values[i].Record), nil
		}
	}
	return nil, nil
}

// GetByIDs returns detached ExecutionProvider copies in caller slice order for
// every requested ID present in s.values. Unknown IDs are omitted, and wholly
// absent or empty inputs return (nil, nil).
func (s *protocolLabProviderStore) GetByIDs(ctx context.Context, ids []int64) ([]*gatewayprovider.ExecutionProvider, error) {
	var out []*gatewayprovider.ExecutionProvider
	for _, id := range ids {
		// GetByID returns a nil error in memory; skip absent IDs while keeping found entries in order.
		val, _ := s.GetByID(ctx, id)
		if val != nil {
			out = append(out, val)
		}
	}
	return out, nil
}

// ListSchedulable returns detached ExecutionProvider copies for every record in
// s.values that currently satisfies Record.IsSchedulable.
func (s *protocolLabProviderStore) ListSchedulable(_ context.Context) ([]gatewayprovider.ExecutionProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []gatewayprovider.ExecutionProvider
	for i := range s.values {
		if s.values[i].Record.IsSchedulable() {
			out = append(out, *gatewayprovider.NewExecutionProvider(&s.values[i].Record))
		}
	}
	return out, nil
}

// ListSchedulableByGroupID returns detached ExecutionProvider copies belonging
// to groupID that currently satisfy Record.IsSchedulable.
func (s *protocolLabProviderStore) ListSchedulableByGroupID(_ context.Context, groupID int64) ([]gatewayprovider.ExecutionProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []gatewayprovider.ExecutionProvider
	for i := range s.values {
		rec := &s.values[i].Record
		if rec.IsSchedulable() && slices.Contains(rec.GroupIDs, groupID) {
			out = append(out, *gatewayprovider.NewExecutionProvider(rec))
		}
	}
	return out, nil
}

// catalogueRecords returns cloned schedulable provider.Record values filtered
// by optional groupID for model-list catalogue queries. Nil groupID includes
// all currently schedulable records regardless of group membership.
func (s *protocolLabProviderStore) catalogueRecords(groupID *int64) []providercore.Record {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []providercore.Record
	for i := range s.values {
		rec := &s.values[i].Record
		if !rec.IsSchedulable() {
			continue
		}
		if groupID != nil && !slices.Contains(rec.GroupIDs, *groupID) {
			continue
		}
		out = append(out, *providercore.CloneRecord(rec))
	}
	return out
}

// ListSchedulableByGroupIDAndPlatforms returns detached ExecutionProvider
// copies belonging to groupID whose Platform appears in platforms and that
// currently satisfy Record.IsSchedulable.
func (s *protocolLabProviderStore) ListSchedulableByGroupIDAndPlatforms(_ context.Context, groupID int64, platforms []string) ([]gatewayprovider.ExecutionProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []gatewayprovider.ExecutionProvider
	for i := range s.values {
		rec := &s.values[i].Record
		if rec.IsSchedulable() && slices.Contains(rec.GroupIDs, groupID) && slices.Contains(platforms, rec.Platform) {
			out = append(out, *gatewayprovider.NewExecutionProvider(rec))
		}
	}
	return out, nil
}

// ListSchedulableByGroupIDAndPlatform delegates a single platform query for
// groupID to ListSchedulableByGroupIDAndPlatforms.
func (s *protocolLabProviderStore) ListSchedulableByGroupIDAndPlatform(ctx context.Context, groupID int64, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	return s.ListSchedulableByGroupIDAndPlatforms(ctx, groupID, []string{platform})
}

// ListByPlatform returns detached ExecutionProvider copies matching platform regardless of group or cooldown state.
func (s *protocolLabProviderStore) ListByPlatform(_ context.Context, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []gatewayprovider.ExecutionProvider
	for i := range s.values {
		if s.values[i].Record.Platform == platform {
			out = append(out, *gatewayprovider.NewExecutionProvider(&s.values[i].Record))
		}
	}
	return out, nil
}

// ListSchedulableByPlatform delegates a single platform schedulability query to ListSchedulableByPlatforms.
func (s *protocolLabProviderStore) ListSchedulableByPlatform(ctx context.Context, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	return s.ListSchedulableByPlatforms(ctx, []string{platform})
}

// ListSchedulableByPlatforms returns detached ExecutionProvider copies for
// stored records that satisfy Record.IsSchedulable and match any platform in
// platforms. Passing an empty platforms slice returns (nil, nil) immediately
// without acquiring the provider read lock.
func (s *protocolLabProviderStore) ListSchedulableByPlatforms(_ context.Context, platforms []string) ([]gatewayprovider.ExecutionProvider, error) {
	if len(platforms) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []gatewayprovider.ExecutionProvider
	for i := range s.values {
		rec := &s.values[i].Record
		if rec.IsSchedulable() && slices.Contains(platforms, rec.Platform) {
			out = append(out, *gatewayprovider.NewExecutionProvider(rec))
		}
	}
	return out, nil
}

// ListSchedulableUngroupedByPlatform delegates a single platform ungrouped query to ListSchedulableUngroupedByPlatforms.
func (s *protocolLabProviderStore) ListSchedulableUngroupedByPlatform(ctx context.Context, platform string) ([]gatewayprovider.ExecutionProvider, error) {
	return s.ListSchedulableUngroupedByPlatforms(ctx, []string{platform})
}

// ListSchedulableUngroupedByPlatforms returns detached ExecutionProvider copies
// with empty GroupIDs that satisfy Record.IsSchedulable and match any platform
// in platforms. Passing an empty platforms slice returns (nil, nil) immediately
// without acquiring the provider read lock.
func (s *protocolLabProviderStore) ListSchedulableUngroupedByPlatforms(_ context.Context, platforms []string) ([]gatewayprovider.ExecutionProvider, error) {
	if len(platforms) == 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []gatewayprovider.ExecutionProvider
	for i := range s.values {
		rec := &s.values[i].Record
		if rec.IsSchedulable() && len(rec.GroupIDs) == 0 && slices.Contains(platforms, rec.Platform) {
			out = append(out, *gatewayprovider.NewExecutionProvider(rec))
		}
	}
	return out, nil
}

// ListModelAvailabilityCandidates returns active schedulable ExecutionProvider
// entries filtered by groupID and platforms while ignoring transient rate-limit
// or cooldown windows for selection-failure diagnostics. When groupID is nil
// and includeGrouped is false, grouped providers are excluded.
func (s *protocolLabProviderStore) ListModelAvailabilityCandidates(_ context.Context, groupID *int64, platforms []string, includeGrouped bool) ([]gatewayprovider.ExecutionProvider, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []gatewayprovider.ExecutionProvider
	for i := range s.values {
		rec := &s.values[i].Record
		if rec.Status != billing.StatusActive || !rec.Schedulable {
			continue
		}
		if len(platforms) > 0 && !slices.Contains(platforms, rec.Platform) {
			continue
		}
		if groupID != nil {
			if !slices.Contains(rec.GroupIDs, *groupID) {
				continue
			}
		} else if !includeGrouped && len(rec.GroupIDs) > 0 {
			continue
		}
		out = append(out, *gatewayprovider.NewExecutionProvider(rec))
	}
	return out, nil
}

// Update accepts provider record persistence calls as a no-op in memory.
func (s *protocolLabProviderStore) Update(context.Context, *gatewayprovider.ExecutionProvider) error {
	return nil
}

// ClearError restores active status and clears ErrorMessage on the stored
// provider matching id under s.mu without re-enabling Schedulable. Unknown
// provider IDs return nil without mutating s.values.
func (s *protocolLabProviderStore) ClearError(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID == id {
			s.values[i].Record.Status = billing.StatusActive
			s.values[i].Record.ErrorMessage = ""
		}
	}
	return nil
}

// SetModelRateLimit writes a UTC RFC3339 model_rate_limits scope entry into the
// matching provider's Extra map under s.mu. Empty scopes or unknown provider
// IDs return nil without modifying stored state.
func (s *protocolLabProviderStore) SetModelRateLimit(_ context.Context, id int64, scope string, resetAt time.Time, reason ...string) error {
	if scope == "" {
		return nil
	}
	entry := map[string]any{
		"rate_limited_at":     time.Now().UTC().Format(time.RFC3339),
		"rate_limit_reset_at": resetAt.UTC().Format(time.RFC3339),
	}
	if len(reason) > 0 {
		if trimmed := strings.TrimSpace(reason[0]); trimmed != "" {
			entry["reason"] = trimmed
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID != id {
			continue
		}
		if s.values[i].Record.Extra == nil {
			s.values[i].Record.Extra = make(map[string]any)
		}
		scopes, ok := s.values[i].Record.Extra["model_rate_limits"].(map[string]any)
		if !ok {
			scopes = make(map[string]any)
			s.values[i].Record.Extra["model_rate_limits"] = scopes
		}
		scopes[scope] = entry
	}
	return nil
}

// SetOverloaded sets OverloadUntil on the stored provider matching id under
// s.mu so Record.IsSchedulable excludes it until the deadline passes. Unknown
// provider IDs return nil without mutating s.values.
func (s *protocolLabProviderStore) SetOverloaded(_ context.Context, id int64, until time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID == id {
			deadline := until
			s.values[i].Record.OverloadUntil = &deadline
		}
	}
	return nil
}

// ClearRateLimit clears RateLimitedAt, RateLimitResetAt, and OverloadUntil on
// the stored provider record matching id under s.mu while preserving manual
// Schedulable flags. Unknown provider IDs return nil without mutating s.values.
func (s *protocolLabProviderStore) ClearRateLimit(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID == id {
			s.values[i].Record.RateLimitedAt = nil
			s.values[i].Record.RateLimitResetAt = nil
			s.values[i].Record.OverloadUntil = nil
		}
	}
	return nil
}

// ClearAntigravityQuotaScopes accepts Antigravity quota-scope resets as a no-op in memory.
func (s *protocolLabProviderStore) ClearAntigravityQuotaScopes(context.Context, int64) error {
	return nil
}

// ClearModelRateLimits removes the model_rate_limits scope map from Extra on
// the stored provider record matching id under s.mu while preserving other
// Extra keys. Unknown provider IDs or nil Extra maps return nil cleanly.
func (s *protocolLabProviderStore) ClearModelRateLimits(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID == id && s.values[i].Record.Extra != nil {
			delete(s.values[i].Record.Extra, "model_rate_limits")
		}
	}
	return nil
}

// UpdateSessionWindow accepts session-window metadata updates as a no-op in memory.
func (s *protocolLabProviderStore) UpdateSessionWindow(context.Context, int64, *time.Time, *time.Time, string) error {
	return nil
}

// UpdateSessionWindowEnd accepts session-window end timestamp updates as a no-op in memory.
func (s *protocolLabProviderStore) UpdateSessionWindowEnd(context.Context, int64, time.Time) error {
	return nil
}

// UpdateExtra accepts provider Extra map persistence calls as a no-op in memory.
func (s *protocolLabProviderStore) UpdateExtra(context.Context, int64, map[string]any) error {
	return nil
}

// IncrementQuotaUsed accepts provider quota usage increments as a no-op in memory.
func (s *protocolLabProviderStore) IncrementQuotaUsed(context.Context, int64, float64) error {
	return nil
}

// BulkUpdate reports len(ids) affected rows without mutating in-memory provider records.
func (s *protocolLabProviderStore) BulkUpdate(_ context.Context, ids []int64, _ providercore.ProviderBulkUpdate) (int64, error) {
	return int64(len(ids)), nil
}

// SetRateLimited sets RateLimitedAt and RateLimitResetAt on the stored provider
// matching id under s.mu so subsequent candidate queries observe the cooldown.
// Unknown provider IDs return nil without mutating s.values.
func (s *protocolLabProviderStore) SetRateLimited(_ context.Context, id int64, resetAt time.Time) error {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID == id {
			observed := now
			reset := resetAt
			s.values[i].Record.RateLimitedAt = &observed
			s.values[i].Record.RateLimitResetAt = &reset
		}
	}
	return nil
}

// SetError marks the stored provider matching id with error status, stores msg,
// and disables Schedulable under s.mu so subsequent selection skips that
// candidate. Unknown provider IDs return nil without mutating s.values.
func (s *protocolLabProviderStore) SetError(_ context.Context, id int64, msg string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID == id {
			s.values[i].Record.Status = providercore.StatusError
			s.values[i].Record.ErrorMessage = msg
			s.values[i].Record.Schedulable = false
		}
	}
	return nil
}

// SetTempUnschedulable advances TempUnschedulableUntil and records reason on
// the stored provider matching id under s.mu when until exceeds any existing
// suspension deadline. Earlier deadlines or unknown provider IDs leave stored
// state unchanged and return nil.
func (s *protocolLabProviderStore) SetTempUnschedulable(_ context.Context, id int64, until time.Time, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID != id {
			continue
		}
		cur := s.values[i].Record.TempUnschedulableUntil
		if cur == nil || cur.Before(until) {
			deadline := until
			s.values[i].Record.TempUnschedulableUntil = &deadline
			s.values[i].Record.TempUnschedulableReason = reason
		}
	}
	return nil
}

// ClearTempUnschedulable clears TempUnschedulableUntil and TempUnschedulableReason
// on the stored provider record matching id under s.mu so temporary cooldown
// suspension ends immediately. Unknown provider IDs return nil without mutating s.values.
func (s *protocolLabProviderStore) ClearTempUnschedulable(_ context.Context, id int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID == id {
			s.values[i].Record.TempUnschedulableUntil = nil
			s.values[i].Record.TempUnschedulableReason = ""
		}
	}
	return nil
}

// UpdateLastUsed accepts provider activity timestamp updates as a no-op in memory.
func (s *protocolLabProviderStore) UpdateLastUsed(context.Context, int64) error { return nil }

// BatchUpdateLastUsed accepts batched provider activity timestamp updates as a no-op in memory.
func (s *protocolLabProviderStore) BatchUpdateLastUsed(context.Context, map[int64]time.Time) error {
	return nil
}

// Create appends a detached copy of log to s.logs under s.mu and reports
// whether a UsageLog row was inserted. Passing a nil log pointer returns
// (false, nil) immediately without mutating s.logs.
func (s *protocolLabUsageStore) Create(_ context.Context, log *usage.UsageLog) (bool, error) {
	if log == nil {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs, cloneLabUsageLog(*log))
	return true, nil
}

// CreateBestEffort appends a detached copy of log to s.logs under s.mu for
// best-effort completion recording paths. Nil log pointers are ignored without
// mutating s.logs and always return a nil error.
func (s *protocolLabUsageStore) CreateBestEffort(_ context.Context, log *usage.UsageLog) error {
	if log != nil {
		s.mu.Lock()
		s.logs = append(s.logs, cloneLabUsageLog(*log))
		s.mu.Unlock()
	}
	return nil
}

// Apply records a copy of cmd under s.mu and returns an applied settlement
// result. Subscription billing routes BillableAmountUSD to SubscriptionAmountUSD,
// while all other modes populate BalanceAmountUSD.
func (s *protocolLabSettlementStore) Apply(_ context.Context, cmd *billing.UsageBillingCommand) (*billing.UsageBillingApplyResult, error) {
	res := &billing.UsageBillingApplyResult{Applied: true}
	if cmd != nil {
		s.mu.Lock()
		s.commands = append(s.commands, *cmd)
		s.mu.Unlock()
		if cmd.BillingType == usage.BillingTypeSubscription {
			res.SubscriptionAmountUSD = cmd.BillableAmountUSD
		} else {
			res.BalanceAmountUSD = cmd.BillableAmountUSD
		}
	}
	return res, nil
}

// ResolveUsableSubscriptionForGroup reports no active subscription so lab balance billing applies.
func (s *protocolLabSettlementStore) ResolveUsableSubscriptionForGroup(context.Context, int64, int64) (*billing.UserSubscription, error) {
	return nil, nil
}

// StreamRequestContext captures Qoder inference payloads in c.capture and
// delegates transport execution to StreamRequestContextWithDoer with a nil doer.
// Callers consume and close successful HTTP response bodies returned by the stream.
func (c *protocolLabQoderClient) StreamRequestContext(ctx context.Context, session *qoder.SessionContext, reqPath string, body []byte, headers map[string]string) (*http.Response, error) {
	return c.StreamRequestContextWithDoer(ctx, session, reqPath, body, headers, nil)
}

// StreamRequestContextWithDoer records every submitted Qoder payload, cloned
// header map, and constructed HTTP request onto c.capture under c.mu before
// dispatching to the selected provider's local BaseURL or returning an
// in-memory SSE stream. Nil contexts default to context.Background, and empty
// reqPath values select qoder.GenerationPath.
//
// Holding c.mu only around c.capture mutation while releasing it across stream
// synthesis and outbound HTTP I/O prevents stalled keepalive or failover streams
// from blocking concurrent provider reconfigurations. Non-200 HTTP responses
// parse into *qoder.APIError values with redacted diagnostic fields matching
// production qoder.Client.StreamRequestContextWithDoer.
func (c *protocolLabQoderClient) StreamRequestContextWithDoer(
	ctx context.Context,
	session *qoder.SessionContext,
	reqPath string,
	body []byte,
	headers map[string]string,
	targetDoer qoder.RequestDoer,
) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	clonedHeaders := maps.Clone(headers)

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.capture == nil {
		c.capture = &gatewaytestkit.QoderClient{}
	}
	base, hasSessionBase := c.baseURLBySession[session]
	if !hasSessionBase {
		// Fall back to fallbackBaseURL when callers pass a custom or unseeded Qoder
		// SessionContext so single-provider tests still reach the configured server.
		base = c.fallbackBaseURL
	}
	presetBody := c.capture.Body

	return c.capture.StreamRequestContextWithDoer(ctx, session, reqPath, body, clonedHeaders, func(_ *http.Request) (*http.Response, error) {
		// Release c.mu before performing in-memory stream synthesis or blocking
		// outbound HTTP I/O so stalled keepalive or failover tests never contend.
		c.mu.Unlock()
		defer c.mu.Lock()

		if base == "" {
			sseText := presetBody
			if sseText == "" {
				// Synthesize a minimal Qoder SSE stream when neither BaseURL nor
				// QoderClient.Body was supplied so endpoint smoke tests verify routing offline.
				sseText = "data: {\"body\":\"{\\\"choices\\\":[{\\\"delta\\\":{\\\"content\\\":\\\"ok-qoder\\\"}}]}\"}\n\n" +
					"data: {\"body\":\"{\\\"usage\\\":{\\\"prompt_tokens\\\":8,\\\"completion_tokens\\\":2,\\\"total_tokens\\\":10}}\"}\n\n" +
					"data: {\"body\":\"[DONE]\"}\n\n"
			}
			respHeaders := make(http.Header)
			respHeaders.Set("Content-Type", "text/event-stream")
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     respHeaders,
				Body:       io.NopCloser(strings.NewReader(sseText)),
			}, nil
		}

		path := strings.TrimSpace(reqPath)
		if path == "" {
			path = qoder.GenerationPath
		}
		// Preserve Qoder's canonical api1.qoder.sh origin when targetDoer is provided
		// so provideradapter.QoderRequestDoer rewrites the request to the selected
		// provider's local base_url via protocolLabTransport.localEndpointOverride;
		// direct pool fallback dials base directly when targetDoer is nil.
		targetURL := "https://api1.qoder.sh/" + strings.TrimLeft(path, "/")
		if targetDoer == nil {
			targetURL = strings.TrimRight(base, "/") + "/" + strings.TrimLeft(path, "/")
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Cache-Control", "no-cache")
		for k, v := range clonedHeaders {
			req.Header.Set(k, v)
		}

		var resp *http.Response
		if targetDoer != nil {
			resp, err = targetDoer(req)
		} else {
			resp, err = c.pool.Do(req, httpclient.UpstreamRequestOptions{
				Settings: httpclient.UpstreamSettings{ResponseHeaderTimeout: c.timeout},
			})
		}
		if err != nil {
			return nil, fmt.Errorf("qoder: request failed: %w", err)
		}
		if resp.StatusCode != http.StatusOK {
			// Bound rejected HTTP response bodies to 4096 bytes and convert them into
			// *qoder.APIError values matching production qoder.Client transport semantics.
			errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
			_ = resp.Body.Close()
			apiErr := qoder.ParseAPIErrorBody(resp.StatusCode, string(errBody))
			if apiErr == nil {
				apiErr = &qoder.APIError{}
			}
			apiErr.StatusCode = resp.StatusCode
			apiErr.Body = protocolqoder.RedactErrorText(string(errBody))
			if strings.TrimSpace(apiErr.Message) == "" {
				apiErr.Message = fmt.Sprintf("Qoder upstream returned HTTP %d", resp.StatusCode)
			}
			apiErr.Message = protocolqoder.RedactErrorText(apiErr.Message)
			return nil, apiErr
		}
		return resp, nil
	})
}

// newProtocolLabAttributeRepo constructs an in-memory ModelAttributeRepository
// bound to groupID with an initial deep-copied slice of rules. Passing an empty
// or nil rules slice leaves group overrides inactive until SetModelAttributeRules runs.
func newProtocolLabAttributeRepo(groupID int64, rules []routing.ModelAttributeRule) *protocolLabAttributeRepo {
	repo := &protocolLabAttributeRepo{groupID: groupID}
	repo.setRules(rules)
	return repo
}

// setRules replaces r.rules with a deep copy under r.mu so subsequent ForGroup
// and ForGroups queries observe the updated attribute profile. Nil receivers are
// ignored safely, and empty slices clear active group overrides.
func (r *protocolLabAttributeRepo) setRules(rules []routing.ModelAttributeRule) {
	if r == nil {
		return
	}
	var cloned []routing.ModelAttributeRule
	if len(rules) > 0 {
		// Round-trip rules through JSON to detach nested pointer and slice fields
		// from caller slices, falling back to slices.Clone only if marshaling fails.
		raw, err := json.Marshal(rules)
		if err == nil {
			_ = json.Unmarshal(raw, &cloned)
		} else {
			cloned = slices.Clone(rules)
		}
	}
	r.mu.Lock()
	r.rules = cloned
	r.mu.Unlock()
}

// ForGroup returns an active ModelAttributeConfig carrying a cloned copy of
// r.rules when groupID matches r.groupID and at least one rule is configured.
// Nil receivers, non-matching group IDs, or empty rule slices return (nil, nil).
func (r *protocolLabAttributeRepo) ForGroup(_ context.Context, groupID int64) (*routing.ModelAttributeConfig, error) {
	if r == nil || groupID != r.groupID {
		return nil, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.rules) == 0 {
		return nil, nil
	}
	return &routing.ModelAttributeConfig{
		ID:       1,
		Name:     "protocol-lab-attributes",
		Status:   routing.StatusActive,
		Rules:    slices.Clone(r.rules),
		GroupIDs: []int64{r.groupID},
	}, nil
}

// ForGroups returns a single-entry profile map keyed by r.groupID when
// r.groupID appears in ids and r.rules is non-empty. Missing group IDs or empty
// rule slices return an allocated empty map with a nil error.
func (r *protocolLabAttributeRepo) ForGroups(ctx context.Context, ids []int64) (map[int64]*routing.ModelAttributeConfig, error) {
	out := make(map[int64]*routing.ModelAttributeConfig)
	if !slices.Contains(ids, r.groupID) {
		return out, nil
	}
	cfg, err := r.ForGroup(ctx, r.groupID)
	if err != nil || cfg == nil {
		return out, err
	}
	out[r.groupID] = cfg
	return out, nil
}

// UsageLogs returns detached copies of all settled UsageLog rows recorded so
// far in chronological order under usageStore.mu. Mutating the returned slice
// or its optional pointers does not affect stored recorder entries.
func (r *protocolLabRouter) UsageLogs() []usage.UsageLog {
	r.usageStore.mu.Lock()
	defer r.usageStore.mu.Unlock()
	out := make([]usage.UsageLog, len(r.usageStore.logs))
	for i, row := range r.usageStore.logs {
		out[i] = cloneLabUsageLog(row)
	}
	return out
}

// ResetUsageLogs clears all previously recorded UsageLog and settlement command
// slices under their respective mutexes. Subsequent UsageLogs calls observe an
// empty history while preserving provider and session state.
func (r *protocolLabRouter) ResetUsageLogs() {
	r.usageStore.mu.Lock()
	r.usageStore.logs = nil
	r.usageStore.mu.Unlock()
	r.settlements.mu.Lock()
	r.settlements.commands = nil
	r.settlements.mu.Unlock()
}

// ProviderRecord returns a snapshot of the stored provider.Record matching id
// under r.providers.mu together with a presence flag, or a zero Record and
// false when r or id is absent. Callers inspect Status, Schedulable,
// ErrorMessage, RateLimitResetAt, and TempUnschedulableUntil to verify
// health-penalty isolation without racing live handler goroutines.
func (r *protocolLabRouter) ProviderRecord(id int64) (providercore.Record, bool) {
	if r == nil || r.providers == nil {
		return providercore.Record{}, false
	}
	r.providers.mu.Lock()
	defer r.providers.mu.Unlock()
	for i := range r.providers.values {
		if r.providers.values[i].Record.ID == id {
			return r.providers.values[i].Record, true
		}
	}
	return providercore.Record{}, false
}

// FeedbackSnapshot samples EWMA scheduling feedback for provider id from r's
// shared RuntimeStats tracker. Nil routers, uninitialized trackers, or unseen
// provider IDs return a zero FeedbackSnapshot.
func (r *protocolLabRouter) FeedbackSnapshot(id int64) scheduler.FeedbackSnapshot {
	if r == nil || r.feedback == nil {
		return scheduler.FeedbackSnapshot{}
	}
	return r.feedback.FeedbackSnapshot(id)
}

// SetModelAttributeRules replaces the active group ModelAttributeRule slice on
// r's in-memory attribute repository so subsequent requests observe updated
// capability or effort overrides immediately. Passing nil clears active group
// rules, and nil routers return as a no-op.
func (r *protocolLabRouter) SetModelAttributeRules(rules []routing.ModelAttributeRule) {
	if r == nil || r.attributeRepo == nil {
		return
	}
	r.attributeRepo.setRules(rules)
}

// QoderBodies returns detached byte copies of all Qoder desktop payload JSON
// bodies captured so far by r.qoderClient.capture under qoderClient.mu. Nil
// routers or unconfigured Qoder clients return nil without panicking.
func (r *protocolLabRouter) QoderBodies() [][]byte {
	if r == nil || r.qoderClient == nil {
		return nil
	}
	r.qoderClient.mu.Lock()
	defer r.qoderClient.mu.Unlock()
	count := r.qoderClient.capture.BodyCount()
	out := make([][]byte, 0, count)
	for i := range count {
		out = append(out, r.qoderClient.capture.BodyAt(i))
	}
	return out
}

// QoderHeaders returns a detached clone of the most recent Qoder request header
// map captured on r.qoderClient.capture under qoderClient.mu. Nil routers or
// absent header snapshots return nil.
func (r *protocolLabRouter) QoderHeaders() map[string]string {
	if r == nil || r.qoderClient == nil {
		return nil
	}
	r.qoderClient.mu.Lock()
	defer r.qoderClient.mu.Unlock()
	if r.qoderClient.capture == nil || r.qoderClient.capture.Headers == nil {
		return nil
	}
	return maps.Clone(r.qoderClient.capture.Headers)
}

// ResetQoderCapture clears all captured Qoder HTTP requests, copied payload
// bodies, and header snapshots on r.qoderClient.capture under qoderClient.mu.
// Preset Body stream fixtures and injected Err values on c.capture remain intact.
func (r *protocolLabRouter) ResetQoderCapture() {
	if r == nil || r.qoderClient == nil {
		return
	}
	r.qoderClient.mu.Lock()
	defer r.qoderClient.mu.Unlock()
	if r.qoderClient.capture == nil {
		return
	}
	r.qoderClient.capture.Request = nil
	r.qoderClient.capture.Requests = nil
	r.qoderClient.capture.Bodies = nil
	r.qoderClient.capture.Headers = nil
}

// SetProviders replaces the active provider list on r's group with fresh
// schedulable records, seeds per-provider Qoder session caches and BaseURL
// mappings, and clears runtime block, model-transient cooldown, and cached
// OAuth access-token state for old and new provider IDs. Multi-turn tests call
// SetProviders between turns to switch upstream protocols or reset degraded
// accounts cleanly.
func (r *protocolLabRouter) SetProviders(tb testing.TB, specs []labProvider) {
	tb.Helper()
	r.groupMu.RLock()
	groupID := r.group.ID
	r.groupMu.RUnlock()

	execProviders := make([]gatewayprovider.ExecutionProvider, 0, len(specs))
	qoderBases := make(map[*qoder.SessionContext]string)
	var qoderFallbackBase string
	var resetModels []string
	var resetIDs []int64
	for i, spec := range specs {
		rec := buildProtocolLabProviderRecord(tb, i, groupID, spec)
		resetIDs = append(resetIDs, rec.ID)
		if spec.Model != "" {
			resetModels = append(resetModels, spec.Model)
		}
		resetModels = append(resetModels, spec.Models...)
		for src, dst := range spec.ModelMapping {
			resetModels = append(resetModels, src, dst)
		}
		if rec.Platform == capability.PlatformQoder {
			provBase := strings.TrimSpace(rec.GetCredential("base_url"))
			if qoderFallbackBase == "" && provBase != "" {
				qoderFallbackBase = provBase
			}
			sess := &qoder.SessionContext{
				Identity: &qoder.AuthIdentity{
					SecurityOauthToken: fmt.Sprintf("lab-qoder-token-%d", rec.ID),
				},
			}
			qoderBases[sess] = provBase
			r.qoderTokens.Core.Sessions[rec.ID] = providercore.QoderSessionCacheEntry[*qoder.SessionContext]{
				CredentialsHash: providercore.QoderCredentialsHash(rec.Credentials),
				Session:         sess,
			}
		}
		execProviders = append(execProviders, *gatewayprovider.NewExecutionProvider(rec))
	}

	// Publish updated Qoder session-to-BaseURL mappings under qoderClient.mu
	// before swapping provider records so concurrent Qoder attempts target the new endpoints.
	r.qoderClient.mu.Lock()
	r.qoderClient.baseURLBySession = qoderBases
	r.qoderClient.fallbackBaseURL = qoderFallbackBase
	r.qoderClient.mu.Unlock()

	// Swap in freshly built provider records under providers.mu while collecting
	// outgoing IDs so runtime block and model-transient state can be cleared too.
	r.providers.mu.Lock()
	for i := range r.providers.values {
		resetIDs = append(resetIDs, r.providers.values[i].Record.ID)
	}
	r.providers.values = execProviders
	r.providers.mu.Unlock()

	if r.blocks != nil {
		for _, id := range resetIDs {
			r.blocks.ClearProviderSchedulingBlock(id)
			r.blocks.ResetRetry(id)
		}
	}
	if r.modelTransient != nil {
		for _, id := range resetIDs {
			for _, m := range resetModels {
				r.modelTransient.RecordSuccess(id, m)
			}
		}
	}
	if r.tokenCache != nil {
		// Clear cached OAuth tokens when provider specifications change so reused
		// provider IDs never inherit credentials from an outgoing provider record.
		r.tokenCache.Reset()
	}
}

// SetSetting stores or replaces key in r's in-memory settings repository under
// its write lock so live requests observe the new value immediately. Calling
// SetSetting on a router without an initialized settings store is a no-op.
func (r *protocolLabRouter) SetSetting(key, value string) {
	if r.settings == nil {
		return
	}
	_ = r.settings.Set(context.Background(), key, value)
}

// DeleteSetting removes key from r's in-memory settings repository under its
// write lock so subsequent runtime lookups revert to built-in defaults. Calling
// DeleteSetting when key is absent or settings is nil succeeds as a no-op.
func (r *protocolLabRouter) DeleteSetting(key string) {
	if r.settings == nil {
		return
	}
	_ = r.settings.Delete(context.Background(), key)
}

// SetProtocolFallbacks replaces the active group's protocol_fallbacks map with
// a deep copy under groupMu. Passing nil restores default capability fallback
// order, while an explicit empty slice disables conversion for that client protocol.
func (r *protocolLabRouter) SetProtocolFallbacks(fallbacks map[protocolcore.ProtocolID][]protocolcore.ProtocolID) {
	r.groupMu.Lock()
	defer r.groupMu.Unlock()
	r.group.ProtocolFallbacks = cloneProtocolFallbacksMap(fallbacks)
}

// cloneGroup returns a detached copy of r.group with cloned AllowedProtocols
// and ProtocolFallbacks when id matches r.group.ID or is nonpositive.
// Mismatched positive group IDs or a nil group return nil.
func (r *protocolLabRouter) cloneGroup(id int64) *routing.Group {
	r.groupMu.RLock()
	defer r.groupMu.RUnlock()
	if r.group == nil || (id > 0 && id != r.group.ID) {
		return nil
	}
	cp := *r.group
	cp.AllowedProtocols = slices.Clone(r.group.AllowedProtocols)
	cp.ProtocolFallbacks = cloneProtocolFallbacksMap(r.group.ProtocolFallbacks)
	return &cp
}

// newProtocolLabRouter assembles the production gateway HTTP route graph over
// in-memory stores, a real httpclient.UpstreamPool, and configurable provider
// candidates. All twelve RouteEndpoints (including ModelsHandler,
// CountTokensHandler, OpenAITokensHandler, MessagesHandler, GeminiNativeHandler,
// OpenAITextHandler, Antigravity, and Qoder) plus an in-memory
// session.GatewayCache are wired so client protocols and real CLI subprocesses
// exercise authentic routing, SSE framing, keepalive, failover, and usage
// recording without PostgreSQL or Redis.
//
// Zero-valued labOptions fields default to all four text protocols, a 10s
// keepalive interval, 5s stall and header timeouts, max(len(Providers), 1)
// failover switches, a fresh gatewaytestkit.QoderClient capture fixture, and
// empty runtime settings, model-attribute rules, and error-passthrough rules,
// while background workers register tb.Cleanup hooks.
func newProtocolLabRouter(tb testing.TB, opts labOptions) *protocolLabRouter {
	tb.Helper()

	const (
		groupID = int64(42)
		userID  = int64(7)
		keyID   = int64(8)
	)

	allowed := opts.AllowedProtocols
	if allowed == nil {
		allowed = []protocolcore.ProtocolID{
			protocolcore.ProtocolAnthropicMessages,
			protocolcore.ProtocolOpenAIResponses,
			protocolcore.ProtocolOpenAIChatCompletions,
			protocolcore.ProtocolGeminiGenerateContent,
		}
	} else {
		allowed = slices.Clone(allowed)
	}

	stall := opts.StallTimeout
	if stall <= 0 {
		stall = 5 * time.Second
	}
	headerTimeout := opts.HeaderTimeout
	if headerTimeout <= 0 {
		headerTimeout = 5 * time.Second
	}
	keepaliveInterval := opts.KeepaliveInterval
	if keepaliveInterval == 0 {
		keepaliveInterval = 10 * time.Second
	}
	maxSwitches := opts.MaxProviderSwitches
	if maxSwitches <= 0 {
		maxSwitches = max(len(opts.Providers), 1)
	}

	cfg := &config.Config{}
	cfg.Billing.FallbackRateMultiplier = 1
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	cfg.Security.URLAllowlist.AllowPrivateHosts = true
	cfg.Gateway.Failover.MaxProviderSwitches = maxSwitches
	cfg.Gateway.Failover.MaxProviderSwitchesGemini = maxSwitches
	cfg.Gateway.MaxRequestBodyBytes = 4 << 20
	cfg.Gateway.TextMaxRequestBodyBytes = 4 << 20
	cfg.Gateway.UpstreamResponseMaxBytes = 16 << 20
	// Convert stall into whole seconds for DataIdleTimeoutSeconds and clamp sub-second test budgets to one second.
	cfg.Gateway.Streaming.DataIdleTimeoutSeconds = max(int(stall/time.Second), 1)
	wsDefaults := ws.DefaultParameters()
	cfg.Gateway.OpenAI.ResponsesWS.MaxConnsPerProvider = wsDefaults.MaxConnsPerProvider
	cfg.Gateway.OpenAI.ResponsesWS.MinIdlePerProvider = wsDefaults.MinIdlePerProvider
	cfg.Gateway.OpenAI.ResponsesWS.MaxIdlePerProvider = wsDefaults.MaxIdlePerProvider
	cfg.Gateway.OpenAI.ResponsesWS.DynamicMaxConnsByProviderConcurrencyEnabled = wsDefaults.DynamicMaxConnsByProviderConcurrencyEnabled
	cfg.Gateway.OpenAI.ResponsesWS.OAuthMaxConnsFactor = wsDefaults.OAuthMaxConnsFactor
	cfg.Gateway.OpenAI.ResponsesWS.APIKeyMaxConnsFactor = wsDefaults.APIKeyMaxConnsFactor
	cfg.Gateway.OpenAI.ResponsesWS.QueueLimitPerConn = wsDefaults.QueueLimitPerConn
	cfg.Gateway.OpenAI.ResponsesWS.PoolTargetUtilization = wsDefaults.PoolTargetUtilization
	cfg.Gateway.OpenAI.ResponsesWS.PrewarmCooldownMS = wsDefaults.PrewarmCooldownMS
	cfg.Gateway.OpenAI.ResponsesWS.ClientFirstMessageTimeoutSeconds = wsDefaults.ClientFirstMessageTimeoutSeconds
	cfg.Gateway.OpenAI.ResponsesWS.IngressInterTurnIdleTimeoutSeconds = wsDefaults.IngressInterTurnIdleTimeoutSeconds
	// Leave MaxIngressConnectionsPerAPIKey at zero because fakeConcurrencyCache
	// does not emulate Redis WebSocket ingress connection lease scripts.
	cfg.Gateway.OpenAI.ResponsesWS.MaxIngressConnectionsPerAPIKey = 0
	cfg.Gateway.OpenAI.ResponsesWS.ClientReadLimitBytes = wsDefaults.ClientReadLimitBytes
	cfg.Gateway.OpenAI.ResponsesWS.HTTPBridgeThresholdBytes = wsDefaults.HTTPBridgeThresholdBytes
	cfg.Gateway.OpenAI.ResponsesWS.DialTimeoutSeconds = wsDefaults.DialTimeoutSeconds
	cfg.Gateway.OpenAI.ResponsesWS.ReadTimeoutSeconds = wsDefaults.ReadTimeoutSeconds
	cfg.Gateway.OpenAI.ResponsesWS.WriteTimeoutSeconds = wsDefaults.WriteTimeoutSeconds
	cfg.Gateway.OpenAI.ResponsesWS.IngressPreviousResponseRecoveryEnabled = wsDefaults.IngressPreviousResponseRecoveryEnabled

	group := &routing.Group{
		ID:                groupID,
		Hydrated:          true,
		Name:              "protocol-lab",
		Status:            billing.StatusActive,
		RateMultiplier:    1,
		AllowedProtocols:  allowed,
		ProtocolFallbacks: cloneProtocolFallbacksMap(opts.ProtocolFallbacks),
	}

	cache := newProtocolLabGatewayCache()
	settingsStore := newProtocolLabSettingsStore(opts.Settings)
	tokenCache := newProtocolLabAccessTokenCache()
	providerStore := &protocolLabProviderStore{}
	usageStore := &protocolLabUsageStore{}
	settlementStore := &protocolLabSettlementStore{}

	var hostOverrides map[string]string
	if len(opts.HostOverrides) > 0 {
		hostOverrides = make(map[string]string, len(opts.HostOverrides))
		for host, targetURL := range opts.HostOverrides {
			hostOverrides[strings.ToLower(strings.TrimSpace(host))] = targetURL
		}
	}

	transport := &protocolLabTransport{
		pool:          httpclient.NewUpstreamPool(),
		headerTimeout: headerTimeout,
		providers:     providerStore,
		hostOverrides: hostOverrides,
	}
	tb.Cleanup(transport.pool.CloseIdleConnections)

	qoderTokens := provideradapter.NewQoderTokenProvider(qoder.SessionBuilder{})
	qoderTokens.Core.Sessions = make(map[int64]providercore.QoderSessionCacheEntry[*qoder.SessionContext])
	qoderCapture := opts.QoderClient
	if qoderCapture == nil {
		qoderCapture = &gatewaytestkit.QoderClient{}
	}
	qoderClient := &protocolLabQoderClient{
		pool:    transport.pool,
		timeout: headerTimeout,
		capture: qoderCapture,
	}

	attributeRepo := newProtocolLabAttributeRepo(groupID, opts.ModelAttributeRules)
	deferred := &providercore.DeferredService{}
	blocks := providercore.NewRuntimeBlockState(time.Now)
	modelTransient := provideSelectionModelTransient()
	proxyCircuit := provideSelectionProxyCircuit(cfg)

	lab := &protocolLabRouter{
		Cache:          cache,
		APIKey:         "lab-api-key",
		group:          group,
		settings:       settingsStore,
		attributeRepo:  attributeRepo,
		tokenCache:     tokenCache,
		providers:      providerStore,
		blocks:         blocks,
		modelTransient: modelTransient,
		usageStore:     usageStore,
		settlements:    settlementStore,
		qoderClient:    qoderClient,
		qoderTokens:    qoderTokens,
	}
	lab.SetProviders(tb, opts.Providers)

	groupRepo := protocolLabGroupRepo{router: lab}
	readers := gatewaytestkit.RuntimeReaders(settingsStore)
	readers.Attributes.Repo = attributeRepo
	var errorRules *errorpolicy.ErrorPassthroughService
	if len(opts.ErrorRules) > 0 {
		errorRules = gatewaytestkit.ErrorRules(opts.ErrorRules)
	}
	health := newAppHealthObserverFixture(providerStore, cfg)
	concurrency := scheduler.NewConcurrencyService(&fakeConcurrencyCache{})

	pricingConfigs := routing.NewPricingConfigService(modelCatalogueEmptyPrices{}, nil, routing.PricingConfigOptions{
		ReadGroup:    groupRepo.GetByID,
		Warn:         slog.Warn,
		Now:          time.Now,
		LoadLocation: pricingprovider.LoadPricingLocation,
	})
	calculator := billingtestkit.Calculator(nil, buildProtocolLabPriceMap(opts))
	prices := billingtestkit.PriceResolver(pricingConfigs, calculator)
	planner := gatewayprovider.NewRoutePlanner(pricingConfigs)
	stateStore := session.NewOpenAIWSStateStore(cache, gatewayprovider.LogOpenAIWSModeInfo)
	schedulerState := &schedulerSharedState{
		Sticky:   &scheduler.StickyStats{},
		Feedback: scheduler.NewRuntimeStats(time.Now),
	}
	lab.feedback = schedulerState.Feedback
	selectionReads := selection.Reads{Providers: providerStore, Groups: groupRepo}
	selectionShared := selection.Shared{
		Cache:         cache,
		Concurrency:   concurrency,
		Health:        health,
		GroupPolicies: pricingConfigs,
		Feedback:      schedulerState.Feedback,
	}
	genericChoices := selection.NewGeneric(selection.GenericDependencies{
		Reads:            selectionReads,
		Shared:           selectionShared,
		SetProviderError: providerStore.SetError,
	}, selectionOptions(cfg))
	geminiChoices := provideGeminiSelection(selectionReads, selectionShared, cfg, nil)
	compatChoices := selection.NewCompatible(selection.CompatibleDependencies{
		Generic:        genericChoices,
		Gemini:         geminiChoices,
		Reads:          selectionReads,
		Shared:         selectionShared,
		Responses:      stateStore,
		QuotaSettings:  readers.Quota,
		RuntimeBlocks:  blocks,
		ModelTransient: modelTransient,
		ProxyCircuit:   proxyCircuit,
		StickyStats:    schedulerState.Sticky,
	}, selectionOptions(cfg))

	origins := session.NewCodexTurnOrigins(time.Now)
	lab.Origins = origins
	turns := &gatewayhttp.CodexTurnStateHeaders{Origins: origins}

	activity := &gatewayRequestActivity{Operations: lifecycle.NewOperations("protocol-lab-activity")}
	credSource := &providercore.OpenAIExecutionCredentials{
		Grok: func(_ context.Context, rec *providercore.Record) (string, error) {
			if rec == nil {
				return "", errors.New("provider record is nil")
			}
			tok := strings.TrimSpace(rec.GetCredential("access_token"))
			if tok == "" {
				return "", errors.New("access_token not found in credentials")
			}
			return tok, nil
		},
	}
	credRuntime := gatewaytestkit.RequestCredentials(providerStore, credSource, nil, blocks)
	credRuntime.HasGrokTokenSource = true
	credentials := &gatewayhttp.RequestCredentialExecutor{Runtime: credRuntime}
	// Use a thirty-second write throttle in the Grok health observer so repeated
	// upstream failures exercise the production throttled health observation path.
	grokHealth := &provideradapter.GrokHealth{
		Store:          providerStore,
		Health:         health,
		Runtime:        blocks,
		ModelTransient: modelTransient,
		Throttle:       providercore.NewWriteThrottle(30 * time.Second),
		NormalizeModel: func(value *providercore.Record, model string) string {
			return (gatewayprovider.ModelPolicy{Record: value}).NormalizeOpenAI(model)
		},
	}
	connections := openaiws.NewOpenAIWSConnections(openAIWSPoolOptions(cfg), nil)
	tb.Cleanup(connections.Close)
	agentIdentity := gatewayprovider.NewExecutionAgentIdentity(&providercore.OpenAITaskCoordinator{}, providerStore, nil, connections.InvalidateProvider)
	output := provideOpenAIResponseOutput(
		cfg,
		provideOpenAIResponseHealth(health, blocks, modelTransient, deferred),
		grokHealth,
		health,
		nil,
		turns,
		proxyCircuit,
		stateStore,
		compatChoices,
		agentIdentity,
	)
	grokExecutor := provideGrokExecutor(cfg, credRuntime, transport, output, grokHealth, nil, readers, blocks, deferred, providerStore, activity, prices, connections)
	text := openAITextExecution(cfg, providerStore, agentIdentity, credRuntime.Source, transport, nil, nil, readers, grokExecutor, output, provideAnthropicPromptCache(), compatChoices.OpenAIHTTPResponseStickyTTL, provideCompactExecutor(cfg))
	lineage := provideOpenAIEncryptedLineage(stateStore, compatChoices)
	imagePolicy := provideOpenAIImageBridgePolicy(cfg)
	sockets := provideOpenAIWebSockets(nil, cfg, connections, text, nil, compatChoices, lineage, imagePolicy, cache)
	responses := provideOpenAIResponses(text, compatChoices, lineage, imagePolicy)
	source := &gatewayExecutionFixture{
		Text:       text,
		Requests:   text.Requests,
		Responses:  responses,
		WebSockets: sockets,
		Grok:       grokExecutor,
		Cache:      cache,
		Planner:    planner,
		Blocks: session.NewCyberBlocks(
			session.AdaptCyberSessionBlockStore(cache),
			readers.Moderation.GetCyberSessionBlockRuntime,
			func(format string, args ...any) { logging.LegacyPrintf("service.openai_gateway", format, args...) },
		),
	}
	source.Auxiliary = provideOpenAIAuxiliary(text, nil, activity)

	recording := gatewaytestkit.NewRecording(usageStore, settlementStore, nil, false)
	recording.Options.DefaultMultiplier = 1
	recording.Dependencies.Calculator = calculator
	recording.Dependencies.Prices = prices
	recording.GroupPolicies = pricingConfigs
	recorders := GatewayCompletionRecorders{
		Forward: recording.Core(nil, false),
		OpenAI:  recording.Core(nil, true),
	}
	source.Recorder = recorders.OpenAI

	msgOptions := messageExecutionOptions(cfg)
	msgOptions.StreamInterval = stall
	messagesExec := gatewayhttp.NewMessagesExecutor(messageforward.NewRuntime(messageforward.Dependencies{
		Credentials: &providercore.MessageCredentialSource{
			Claude: func(_ context.Context, p *providercore.Record) (string, error) {
				if p == nil {
					return "", errors.New("provider record is nil")
				}
				tok := p.GetCredential("access_token")
				if tok == "" {
					return "", errors.New("access_token not found in credentials")
				}
				return tok, nil
			},
		},
		Transport:     transport,
		Health:        health,
		Prices:        prices,
		GroupPolicies: pricingConfigs,
		Settings:      readers.Gateway,
		Attributes:    readers.Attributes,
		ProviderState: providerStore,
		Deferred:      deferred,
		Search:        searchtools.NewEmulator(protocolLabNoSearch{}, nil, nil, nil, nil, nil),
		Enter:         activity.Enter,
	}, msgOptions), nil)

	dailyReset := func() *int64 {
		val := time.Now().Add(24 * time.Hour).Unix()
		return &val
	}
	googleOpts := googleForwardOptions(cfg)
	// Truncate stall to whole seconds for Google stream idle polling and clamp sub-second budgets to one second.
	googleOpts.StreamInterval = max(int(stall/time.Second), 1)
	googleToolCalls := provideGeminiToolCalls()
	geminiRuntime := &googleforward.Gemini{
		Attributes: readers.Attributes,
		Options:    googleOpts,
		Tokens: &providercore.GeminiTokenSource{
			Options: providercore.GeminiTokenOptions{
				Vertex: func(_ context.Context, p *providercore.Record) (string, error) {
					if p == nil {
						return "", errors.New("provider record is nil")
					}
					tok := strings.TrimSpace(p.GetCredential("access_token"))
					if tok == "" {
						return "", errors.New("access_token not found in credentials")
					}
					return tok, nil
				},
			},
		},
		Health:    health,
		Transport: transport,
		ToolCalls: googleToolCalls,
		Enter:     activity.Enter,
		Errors: &provideradapter.GeminiErrorObserver{
			Other:          health,
			SetRateLimited: providerStore.SetRateLimited,
			DailyReset:     dailyReset,
			ResetTime:      func(body []byte) *int64 { return gemini.ParseGeminiRateLimitResetTime(body, dailyReset) },
		},
	}
	geminiExec := provideGeminiExecutor(geminiRuntime)

	antigravityExec, antigravityTokens := buildProtocolLabAntigravityExecutor(providerStore, tokenCache, transport, health, readers, cache, googleOpts, dailyReset, activity, googleToolCalls)
	lab.antigravityTokens = antigravityTokens
	qoderRuntime := gatewayprovider.NewQoderRuntime(gatewayprovider.QoderRuntimeOptions{
		Attributes:    readers.Attributes,
		Tokens:        qoderTokens,
		Client:        qoderClient,
		Transport:     transport,
		Health:        providerStore,
		Conversations: qoder.NewQoderConversationStore(qoder.QoderConversationTTL),
	})
	qoderRuntime.BindAttemptActivity(activity.Enter)

	unified := provideUnifiedTextExecutor(
		source.Responses, messagesExec, geminiExec, antigravityExec,
		qoderRuntime, nil, nil, nil, cfg, prices,
	)
	lab.unified = unified
	lab.wsExecutor = sockets

	user := &identity.User{ID: userID, Balance: 1000, Status: billing.StatusActive, Concurrency: 16}
	eligibility := newBillingEligibilityFixture(cfg)
	tb.Cleanup(eligibility.Stop)
	funding := newFundingAdmissionFixture(eligibility, cfg)
	keyRepo := &protocolLabKeyRepo{
		lab:     lab,
		keyID:   keyID,
		userID:  userID,
		groupID: groupID,
		user:    user,
	}
	keys := keytestkit.NewService(keyRepo, nil, groupRepo, nil, nil, nil, cfg)
	tb.Cleanup(keys.Stop)

	availability := newExecutionAvailabilityForTest(providerStore, pricingConfigs, nil)
	resources := provideOpenAIHTTPResources(concurrency, cfg)
	endpoints := newGatewayHTTPEndpoints(gatewayHTTPFixtureInput{
		Source:       source,
		Native:       unified,
		Generic:      genericChoices,
		Credentials:  credentials,
		Availability: availability,
		Choices:      compatChoices,
		Config:       cfg,
		Funding:      funding,
		Keys:         keys,
		Rules:        errorRules,
		Concurrency:  resources.Concurrency,
		Images:       resources.Images,
		MaxSwitches:  maxSwitches,
		Recorder:     recorders.OpenAI,
		Ops:          opts.Ops,
		Queue:        opts.OpsQueue,
	})

	retryCooldown := providercore.NewRetryCooldown(fixtureRetryStore{providerStore}, providercore.RetryCooldownOptions{})
	msgBindings := provideMessageHTTPBindings(planner, cache, schedulerState, funding, errorRules, nil, readers.Gateway, nil, concurrency, cfg, genericChoices, keys, nil)
	digest := session.NewDigestSessionStore()
	msgRuntime := provideMessageAttemptRuntime(
		retryCooldown, digest, messagesExec, antigravityExec, geminiExec,
		funding, keys, recorders, msgBindings, errorRules, nil, nil, cfg,
		availability, genericChoices, nil, cache, prices,
	)
	messagesHTTP := provideMessagesHTTP(msgBindings, msgRuntime, activity)
	geminiNativeHTTP := provideGeminiNativeHTTP(msgBindings, digest, msgRuntime, activity, genericChoices)
	countTokensHTTP := provideCountTokensHTTP(
		planner, messagesExec, schedulerState, funding, nil, cfg, activity,
		nil, availability, genericChoices, retryCooldown, source.Auxiliary, geminiExec, msgBindings,
	)
	lab.countTokens = countTokensHTTP
	openAITokensHTTP := provideOpenAITokensHTTP(
		source.Auxiliary, funding, keys, concurrency, nil, cfg, activity,
		nil, availability, compatChoices, planner,
	)

	catalogue := &routing.RequestableCatalogue{
		Read: func(_ context.Context, id *int64) ([]routing.CatalogueProvider, error) {
			return gatewayprovider.CatalogueProviders(providerStore.catalogueRecords(id)), nil
		},
		Resolver: routing.RequestableResolver{
			Attributes:    readers.Attributes,
			GroupPolicies: pricingConfigs,
			Warn:          slog.Warn,
		},
		Warn: slog.Warn,
	}
	modelsCatalog := catalogtest.New(
		"claude-sonnet-4-5", "claude-sonnet-4-5-20250929", "claude-opus-4-6",
		"gpt-5", "gpt-5.4", "gpt-5.5", "gpt-4o",
		"gemini-2.5-pro", "gemini-2.5-flash", "gemini-3-flash",
		"deepseek-chat", "deepseek-reasoner", "deepseek-v4-pro",
		"grok-4.5", "ultimate", "auto", "qwen3.8-max", "glm-5.3",
	)
	modelsHTTP := modelsHTTP(modelsCatalog, catalogue, geminiRuntime, activity, geminiChoices, readers.Attributes)

	commonOpenAI := provideOpenAIAttemptBindings(
		source.Responses, keys, resources, gatewayhttp.NewBoundCyberHandler(source.Blocks, nil, moderationflow.Runtime{Recorder: recorders.OpenAI, Blocks: source.Blocks, Tasks: fixtureCyberTasks{source}}),
		errorRules, nil, recorders, nil, availability, compatChoices, unified, genericChoices, funding, nil, planner, cache,
	)
	mediaRuntime := mediaentry.New(mediaBindings(
		source.Responses, credentials, keys, funding, commonOpenAI, resources, nil, cfg,
		source.Grok, provideGrokVideoTasks(cache, cfg), source.Auxiliary,
		provideOpenAIImages(source.Text, activity), planner, cache, nil,
	))
	mediaHTTP := provideMediaHTTP(mediaRuntime, activity)
	auxiliaryHTTP := provideAuxiliaryHTTP(mediaRuntime, activity)
	liveHTTP := provideLiveHTTP(nil, nil, nil, nil, nil)
	searchHTTP := gatewayhttp.NewSearchHandler(gatewayhttp.SearchPorts{})
	publicUsage := usagehttp.NewPublicUsageHandler(nil, nil, nil, nil, usagehttp.PublicUsageContext{
		Key: keyhttp.GetAPIKeyFromContext,
		Billing: func(c *gin.Context) (*billing.APIKeyBillingContext, bool) {
			return gatewayhttp.GetAPIKeyBillingContext(c)
		},
		Subscription: gatewayhttp.SubscriptionFromContext,
	}, timezone.NewCalendar(time.UTC)).Usage

	auth := keyhttp.APIKeyAuthMiddleware(func(c *gin.Context) {
		grp := lab.cloneGroup(groupID)
		gid := groupID
		key := &apikey.APIKey{
			ID:          keyID,
			Key:         lab.APIKey,
			Status:      billing.StatusActive,
			UserID:      userID,
			User:        user,
			GroupID:     &gid,
			Group:       grp,
			BillingMode: apikey.APIKeyBillingModeBalance,
		}
		c.Set(string(keyhttp.ContextKeyAPIKey), key)
		c.Set(string(gatewayhttp.ContextKeyAPIKeyBilling), &billing.APIKeyBillingContext{
			Mode:      apikey.APIKeyBillingModeBalance,
			Source:    "balance",
			Available: true,
		})
		authctx.SetPrincipal(c, identity.Principal{UserID: userID}, 0, "")
		c.Next()
	})
	// Leave legacyRouteMiddleware's Ops argument nil so its typed-nil concrete
	// ErrorLogQueue stays inactive while router-level OpsErrorLoggerMiddleware
	// dispatches captured entries to the caller's OpsQueue interface.
	middleware := legacyRouteMiddleware(auth, keys, nil, nil, readers.Routing, cfg)
	middleware.GoogleAPIKeyAuth = gin.HandlerFunc(auth)
	// Bind readers.Attributes into request context so group ModelAttributeRules
	// and IR ConversionDialect share the active catalog and profile view.
	middleware.ModelAttributes = func(c *gin.Context) {
		c.Request = c.Request.WithContext(routing.WithModelAttributes(c.Request.Context(), readers.Attributes))
		c.Next()
	}
	if keepaliveInterval > 0 {
		middleware.DownstreamKeepalive = gatewayhttp.DownstreamSSEKeepalive(keepaliveInterval, stall)
	}

	router := gin.New()
	if opts.OnTelemetry != nil {
		router.Use(func(c *gin.Context) {
			reqID := infratelemetry.RequestIDValue(c.Request.Context())
			if reqID == "" {
				reqID = infratelemetry.NewRequestID()
			}
			ctx := context.WithValue(c.Request.Context(), infratelemetry.RequestID, reqID)
			ctx = infratelemetry.WithRequestCapture(ctx, infratelemetry.RequestRecord{RequestID: reqID, State: "completed"}, opts.OnTelemetry)
			c.Request = c.Request.WithContext(ctx)
			c.Next()
		})
	}
	if opts.Ops != nil && opts.OpsQueue != nil {
		router.Use(gatewayhttp.OpsErrorLoggerMiddleware(opts.Ops, opts.OpsQueue, gatewayhttp.OpsObservationAccess{}))
	}

	gatewayhttp.RegisterGatewayRoutes(router, gatewayhttp.RouteEndpoints{
		CountTokens:  countTokensHTTP,
		GeminiNative: geminiNativeHTTP,
		OpenAIText:   endpoints.textHandler(),
		OpenAITokens: openAITokensHTTP,
		ResponsesWS:  endpoints.ResponsesWebSocket,
		Models:       modelsHTTP,
		Messages:     messagesHTTP,
		Media:        mediaHTTP,
		Auxiliary:    auxiliaryHTTP,
		Live:         liveHTTP,
		Search:       searchHTTP,
		PublicUsage:  publicUsage,
	}, middleware, func(*gin.RouterGroup) {})

	lab.Engine = router
	return lab
}

// newHeartbeatRouteRouter preserves the single-kind keepalive and SDK route
// helper as a thin wrapper around newProtocolLabRouter. Synthetic kind values
// such as "openai_passthrough" and "openai_chat" populate provider Extra flags,
// and an optional FallbackURL appends a priority-1 secondary candidate.
func newHeartbeatRouteRouter(t *testing.T, url, kind string, interval time.Duration, overrides ...heartbeatRouteOptions) (*gin.Engine, *session.CodexTurnOrigins) {
	t.Helper()
	platform := kind
	if strings.HasPrefix(kind, "openai_") {
		platform = capability.PlatformOpenAI
	}
	defaultModel := map[string]string{
		"anthropic": "claude-sonnet-4-5-20250929",
		"openai":    "gpt-5.4",
		"gemini":    "gemini-2.5-flash",
		"grok":      "grok-4.5",
	}[platform]
	limits := heartbeatRouteOptions{
		Model:         defaultModel,
		Stall:         time.Second,
		HeaderTimeout: 2 * time.Second,
	}
	if len(overrides) > 0 {
		limits.FallbackURL = overrides[0].FallbackURL
		limits.Ops = overrides[0].Ops
		limits.OpsQueue = overrides[0].OpsQueue
		limits.OnTelemetry = overrides[0].OnTelemetry
		if overrides[0].Model != "" {
			limits.Model = overrides[0].Model
		}
		if overrides[0].Stall > 0 {
			limits.Stall = overrides[0].Stall
		}
		if overrides[0].HeaderTimeout > 0 {
			limits.HeaderTimeout = overrides[0].HeaderTimeout
		}
	}

	var extra map[string]any
	switch kind {
	case "openai_passthrough":
		extra = map[string]any{"openai_passthrough": true}
	case "openai_chat":
		extra = map[string]any{"openai_text_route_mode": "force_chat_completions"}
	}

	providers := []labProvider{{
		ID:          1,
		Name:        platform,
		Platform:    platform,
		Type:        capability.ProviderTypeAPIKey,
		BaseURL:     url,
		Model:       limits.Model,
		Priority:    0,
		Concurrency: 1,
		Extra:       extra,
	}}
	if limits.FallbackURL != "" {
		providers = append(providers, labProvider{
			ID:          2,
			Name:        platform + "-fallback",
			Platform:    platform,
			Type:        capability.ProviderTypeAPIKey,
			BaseURL:     limits.FallbackURL,
			Model:       limits.Model,
			Priority:    1,
			Concurrency: 1,
			Extra:       maps.Clone(extra),
		})
	}

	lab := newProtocolLabRouter(t, labOptions{
		Providers:           providers,
		MaxProviderSwitches: 1,
		KeepaliveInterval:   interval,
		StallTimeout:        limits.Stall,
		HeaderTimeout:       limits.HeaderTimeout,
		Ops:                 limits.Ops,
		OpsQueue:            limits.OpsQueue,
		OnTelemetry:         limits.OnTelemetry,
	})
	return lab.Engine, lab.Origins
}

// buildProtocolLabAntigravityExecutor constructs a local-HTTP-capable
// AntigravityExecutor and its in-memory AntigravityTokenSource wired to store,
// tokenCache, transport, health, cache, and shared toolCalls. Provider records
// with a non-empty base_url credential route v1internal requests (including
// multi-URL and model-fallback attempts) to local httptest servers instead of
// live Antigravity endpoints.
func buildProtocolLabAntigravityExecutor(
	store *protocolLabProviderStore,
	tokenCache *protocolLabAccessTokenCache,
	transport httpclient.UpstreamTransport,
	health *provideradapter.UpstreamHealth,
	readers *gatewayprovider.RuntimeReaders,
	cache session.GatewayCache,
	opts googleforward.Options,
	dailyReset func() *int64,
	activity *gatewayRequestActivity,
	toolCalls *session.GeminiToolCalls,
) (*gatewayhttp.AntigravityExecutor, *providercore.AntigravityTokenSource) {
	agHealth := &providercore.AntigravityHealth{
		Store:     store,
		ModelKeys: provideradapter.AntigravityModelLimitKeys,
		Error:     slog.Error,
		Warn:      slog.Warn,
		Info:      slog.Info,
		Logf: func(format string, args ...any) {
			logging.LegacyPrintf("service.antigravity_gateway", format, args...)
		},
	}
	resolveBase := func(value *providercore.Record) string {
		// Prefer an explicit local base_url credential so tests intercept v1internal
		// calls without dialing production Antigravity cloud endpoints.
		if value != nil {
			if custom := strings.TrimSpace(value.GetCredential("base_url")); custom != "" {
				return custom
			}
		}
		return antigravity.ResolveAntigravityForwardBaseURL("auto", provideradapter.AntigravityPaidTier(value))
	}
	resolveBaseURLs := func(value *providercore.Record) []string {
		// Accept both typed string slices and JSON-decoded []any slices in base_urls
		// before falling back to the single endpoint selected by resolveBase.
		if value != nil {
			if rawURLs, ok := value.Credentials["base_urls"]; ok {
				switch typed := rawURLs.(type) {
				case []string:
					var urls []string
					for _, u := range typed {
						if trimmed := strings.TrimSpace(u); trimmed != "" {
							urls = append(urls, trimmed)
						}
					}
					if len(urls) > 0 {
						return urls
					}
				case []any:
					var urls []string
					for _, item := range typed {
						if s, sOK := item.(string); sOK {
							if trimmed := strings.TrimSpace(s); trimmed != "" {
								urls = append(urls, trimmed)
							}
						}
					}
					if len(urls) > 0 {
						return urls
					}
				}
			}
		}
		return []string{resolveBase(value)}
	}
	tokens := &providercore.AntigravityTokenSource{
		Options: providercore.AntigravityTokenOptions{
			Cache:                tokenCache,
			Repository:           fixtureRetryStore{store},
			Policy:               providercore.AntigravityProviderRefreshPolicy(),
			SetTempUnschedulable: store.SetTempUnschedulable,
			FillProject: func(_ context.Context, value *providercore.Record, _ string) (string, error) {
				if value == nil {
					return "", nil
				}
				// Read lab_backfill_project_id from Extra when project_id is missing
				// so tests exercise project resolution without calling Google Cloud.
				if backfill := strings.TrimSpace(value.GetExtraString("lab_backfill_project_id")); backfill != "" {
					return backfill, nil
				}
				return "", nil
			},
			Persist: func(_ context.Context, value *providercore.Record, creds map[string]any) error {
				if value == nil {
					return nil
				}
				// Write refreshed tokens or backfilled project_id maps back into store
				// so subsequent provider lookups observe the updated credentials.
				store.persistCredentials(value.ID, creds)
				return nil
			},
			Warn:  slog.Warn,
			Debug: slog.Debug,
		},
	}
	// Cap upstream error-response reads at 512 KiB so structured Google error JSON
	// parses intact without unbounded memory allocation.
	retry := &provideradapter.AntigravityRetry{
		Health:         agHealth,
		Policy:         health.Core,
		Do:             transport.Do,
		BaseURL:        resolveBase,
		BaseURLs:       resolveBaseURLs,
		BodyLimit:      func() int64 { return 512 << 10 },
		LogConfig:      opts.LogConfig,
		TruncateString: logredact.TruncateUTF8,
		SafeURL:        logredact.SafeUpstreamURL,
		Capacity:       antigravity.NewCapacityTracker(),
	}
	observer := &provideradapter.AntigravityErrorObserver{
		Health:          agHealth,
		Other:           health,
		LogConfig:       opts.LogConfig,
		TruncateString:  logredact.TruncateUTF8,
		ResetTime:       func(body []byte) *int64 { return gemini.ParseGeminiRateLimitResetTime(body, dailyReset) },
		DefaultDuration: func() time.Duration { return provideradapter.AntigravityFallbackDuration(0) },
		SetRateLimited:  store.SetRateLimited,
	}
	return provideAntigravityExecutor(&googleforward.Antigravity{
		Attributes: readers.Attributes,
		Options:    opts,
		Tokens:     tokens,
		Retry:      retry,
		Errors:     observer,
		Store:      store,
		Transport:  transport,
		Gateway:    readers.Gateway,
		Routing:    readers.Routing,
		Sticky:     cache,
		ToolCalls:  toolCalls,
		Enter:      activity.Enter,
	}), tokens
}

// buildProtocolLabProviderRecord normalizes one labProvider specification into
// an active schedulable provider.Record bound to groupID. Missing IDs, names,
// auth types, credentials, model whitelists, and multi-slot api_base_urls
// entries receive deterministic lab defaults, and empty Platform values fail tb.
func buildProtocolLabProviderRecord(tb testing.TB, index int, groupID int64, spec labProvider) *providercore.Record {
	tb.Helper()
	if strings.TrimSpace(spec.Platform) == "" {
		tb.Fatalf("labProvider[%d] has empty Platform", index)
	}
	id := spec.ID
	if id <= 0 {
		id = int64(index + 1)
	}
	providerType := spec.Type
	if providerType == "" {
		switch spec.Platform {
		case capability.PlatformAntigravity:
			providerType = capability.ProviderTypeOAuth
		case capability.PlatformQoder:
			providerType = capability.ProviderTypeCosy
		default:
			providerType = capability.ProviderTypeAPIKey
		}
	}
	name := spec.Name
	if name == "" {
		name = fmt.Sprintf("%s-%d", spec.Platform, id)
	}
	conc := spec.Concurrency
	if conc <= 0 {
		conc = 4
	}

	creds := make(map[string]any)
	maps.Copy(creds, spec.Credentials)
	if spec.BaseURL != "" {
		creds["base_url"] = spec.BaseURL
	}
	switch providerType {
	case capability.ProviderTypeAPIKey, capability.ProviderTypeUpstream:
		if _, ok := creds["api_key"]; !ok {
			creds["api_key"] = "lab-fixture-key"
		}
	case capability.ProviderTypeOAuth:
		switch spec.Platform {
		case capability.PlatformAntigravity:
			if _, ok := creds["access_token"]; !ok {
				creds["access_token"] = "lab-antigravity-token"
			}
			if _, ok := creds["project_id"]; !ok {
				creds["project_id"] = "lab-antigravity-project"
			}
			if _, ok := creds["expires_at"]; !ok {
				creds["expires_at"] = time.Now().Add(24 * time.Hour).Format(time.RFC3339)
			}
		case capability.PlatformGrok:
			if _, ok := creds["access_token"]; !ok {
				creds["access_token"] = "lab-grok-oauth-token"
			}
			if _, ok := creds["refresh_token"]; !ok {
				creds["refresh_token"] = "lab-grok-refresh-token"
			}
			if _, ok := creds["expires_at"]; !ok {
				creds["expires_at"] = time.Now().Add(24 * time.Hour).UTC().Format(time.RFC3339)
			}
		default:
			if _, ok := creds["access_token"]; !ok {
				creds["access_token"] = "lab-oauth-token"
			}
		}
	case capability.ProviderTypeSetupToken:
		if _, ok := creds["access_token"]; !ok {
			creds["access_token"] = "lab-setup-token"
		}
	case capability.ProviderTypeServiceAccount:
		if _, ok := creds["access_token"]; !ok {
			creds["access_token"] = "lab-vertex-token"
		}
		if _, ok := creds["project_id"]; !ok {
			creds["project_id"] = "lab-vertex-project"
		}
		if _, ok := creds["location"]; !ok {
			creds["location"] = "us-east5"
		}
	case capability.ProviderTypeBedrock:
		if authMode, _ := creds["auth_mode"].(string); strings.EqualFold(strings.TrimSpace(authMode), "apikey") {
			if _, ok := creds["api_key"]; !ok {
				creds["api_key"] = "lab-bedrock-apikey"
			}
		} else {
			if _, ok := creds["aws_access_key_id"]; !ok {
				creds["aws_access_key_id"] = "AKIAIOSFODNN7EXAMPLE"
			}
			if _, ok := creds["aws_secret_access_key"]; !ok {
				creds["aws_secret_access_key"] = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
			}
		}
		if _, ok := creds["aws_region"]; !ok {
			creds["aws_region"] = "us-east-1"
		}
	}

	var whitelist []string
	if spec.Model != "" {
		whitelist = append(whitelist, spec.Model)
	}
	for _, m := range spec.Models {
		if m != "" && !slices.Contains(whitelist, m) {
			whitelist = append(whitelist, m)
		}
	}
	if len(spec.ModelMapping) > 0 {
		mapping := make(map[string]any, len(spec.ModelMapping))
		for src, dst := range spec.ModelMapping {
			mapping[src] = dst
			if dst != "" && !slices.Contains(whitelist, dst) {
				whitelist = append(whitelist, dst)
			}
		}
		creds["model_mapping"] = mapping
	}
	if _, hasWhitelist := creds["model_whitelist"]; !hasWhitelist && len(whitelist) > 0 {
		creds["model_whitelist"] = whitelist
	}

	if spec.EnabledProtocols != nil {
		rawProtos := make([]string, len(spec.EnabledProtocols))
		for i, p := range spec.EnabledProtocols {
			rawProtos[i] = string(p)
		}
		creds[providercore.UpstreamProtocolsKey] = rawProtos
		// Unified protocol records on multi-slot platforms resolve protocol roots
		// from api_base_urls before falling back to live internet defaults.
		if spec.BaseURL != "" {
			if _, hasSlots := creds["api_base_urls"]; !hasSlots {
				switch spec.Platform {
				case capability.PlatformKimi, capability.PlatformZhipu, capability.PlatformDeepseek, capability.PlatformOpenCode:
					creds["api_base_urls"] = map[string]any{
						providercore.APIProtocolChatCompletions: spec.BaseURL,
						providercore.APIProtocolAnthropic:       spec.BaseURL,
						providercore.APIProtocolResponses:       spec.BaseURL,
						"gemini":                                spec.BaseURL,
						"systemone":                             spec.BaseURL,
					}
				}
			}
		}
	}

	rec := &providercore.Record{
		ID:           id,
		Name:         name,
		Platform:     spec.Platform,
		Type:         providerType,
		Status:       billing.StatusActive,
		Schedulable:  true,
		Priority:     spec.Priority,
		Concurrency:  conc,
		GroupIDs:     []int64{groupID},
		Credentials:  creds,
		Extra:        maps.Clone(spec.Extra),
		LoadLocation: time.LoadLocation,
	}
	if spec.Platform == capability.PlatformQoder {
		if rec.Extra == nil || rec.Extra[providercore.QoderCatalogExtraKey] == nil {
			gatewaytestkit.SetQoderCatalog(rec)
		}
	}
	return rec
}

// buildProtocolLabPriceMap merges embedded historical prices with default token
// rates for canonical fixture models and declared provider models. Explicit
// entries in opts.Pricing override both historical and default rates with
// detached ModelPricing copies.
func buildProtocolLabPriceMap(opts labOptions) map[string]*pricing.ModelPricing {
	prices := billingtestkit.HistoricalPrices()
	defaultRate := &pricing.ModelPricing{
		InputPricePerToken:         1e-6,
		OutputPricePerToken:        2e-6,
		CacheCreationPricePerToken: 1.25e-6,
		CacheReadPricePerToken:     0.1e-6,
		CacheCreation5mPrice:       1.25e-6,
		CacheCreation1hPrice:       2e-6,
		SupportsCacheBreakdown:     true,
		SupportsServiceTier:        true,
	}
	canonicalModels := []string{
		"claude-sonnet-4-5", "claude-sonnet-4-5-thinking", "claude-sonnet-4-5-20250929", "claude-sonnet-4-6", "us.anthropic.claude-sonnet-4-6", "claude-opus-4-6", "claude-opus-4-6-thinking", "claude-fable-5",
		"gpt-5", "gpt-5.4", "gpt-5.5", "gpt-4o",
		"gemini-2.5-pro", "gemini-2.5-flash", "gemini-3-flash", "gemini-3.1-pro-high", "gemini-4-argon",
		"deepseek-chat", "deepseek-reasoner", "deepseek-v4-pro",
		"kimi-k2", "k3", "k3-256k", "glm-5", "glm-5.3", "grok-4.5", "grok-4.6",
		"auto", "ultimate", "qwen3.8-max", "qmodel_38max", "dmodel", "gmodel",
	}
	for _, name := range canonicalModels {
		if _, exists := prices[name]; !exists {
			cp := *defaultRate
			prices[name] = &cp
		}
	}
	for _, p := range opts.Providers {
		if p.Model != "" {
			if _, exists := prices[p.Model]; !exists {
				cp := *defaultRate
				prices[p.Model] = &cp
			}
		}
		for _, m := range p.Models {
			if m != "" {
				if _, exists := prices[m]; !exists {
					cp := *defaultRate
					prices[m] = &cp
				}
			}
		}
		for src, dst := range p.ModelMapping {
			for _, m := range []string{src, dst} {
				if m != "" {
					if _, exists := prices[m]; !exists {
						cp := *defaultRate
						prices[m] = &cp
					}
				}
			}
		}
	}
	for name, rate := range opts.Pricing {
		if rate != nil {
			cp := *rate
			prices[name] = &cp
		}
	}
	return prices
}

// cloneProtocolFallbacksMap returns a deep copy of in with cloned protocol
// target slices. Nil outer maps and nil target slices remain nil, whereas
// non-nil empty target slices stay non-nil so explicit conversion disables survive.
func cloneProtocolFallbacksMap(in map[protocolcore.ProtocolID][]protocolcore.ProtocolID) map[protocolcore.ProtocolID][]protocolcore.ProtocolID {
	if in == nil {
		return nil
	}
	out := make(map[protocolcore.ProtocolID][]protocolcore.ProtocolID, len(in))
	for k, v := range in {
		if v == nil {
			out[k] = nil
		} else {
			out[k] = slices.Clone(v)
		}
	}
	return out
}

// cloneLabValuePtr allocates and returns a detached copy of the value pointed
// to by p when p is non-nil. Passing a nil pointer returns nil so optional
// UsageLog metadata fields preserve absence without aliasing recorder memory.
func cloneLabValuePtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

// cloneLabUsageLog returns a copy of row with detached copies of all optional
// scalar pointers, CacheWriteAnomaly, BillingAllocations, and ImageSizeBreakdown.
// Query-only display relation pointers (User, APIKey, Provider, Group,
// Subscription) are copied by value because completion recorders leave them nil.
func cloneLabUsageLog(row usage.UsageLog) usage.UsageLog {
	out := row
	out.TeamID = cloneLabValuePtr(row.TeamID)
	out.UpstreamModel = cloneLabValuePtr(row.UpstreamModel)
	out.UpstreamResponseModel = cloneLabValuePtr(row.UpstreamResponseModel)
	out.UpstreamModelMismatch = cloneLabValuePtr(row.UpstreamModelMismatch)
	out.PricingConfigID = cloneLabValuePtr(row.PricingConfigID)
	out.ModelMappingChain = cloneLabValuePtr(row.ModelMappingChain)
	out.BillingTier = cloneLabValuePtr(row.BillingTier)
	out.BillingMode = cloneLabValuePtr(row.BillingMode)
	out.ServiceTier = cloneLabValuePtr(row.ServiceTier)
	out.ReasoningEffort = cloneLabValuePtr(row.ReasoningEffort)
	out.RequestedReasoningEffort = cloneLabValuePtr(row.RequestedReasoningEffort)
	out.InboundEndpoint = cloneLabValuePtr(row.InboundEndpoint)
	out.UpstreamEndpoint = cloneLabValuePtr(row.UpstreamEndpoint)
	out.GroupID = cloneLabValuePtr(row.GroupID)
	out.SubscriptionID = cloneLabValuePtr(row.SubscriptionID)
	out.CacheCreation30mTokens = cloneLabValuePtr(row.CacheCreation30mTokens)
	out.CacheCreation12hTokens = cloneLabValuePtr(row.CacheCreation12hTokens)
	if row.CacheWriteAnomaly != nil {
		anom := *row.CacheWriteAnomaly
		anom.Reported30m = cloneLabValuePtr(row.CacheWriteAnomaly.Reported30m)
		anom.Reported12h = cloneLabValuePtr(row.CacheWriteAnomaly.Reported12h)
		out.CacheWriteAnomaly = &anom
	}
	out.ProtocolConversion = row.ProtocolConversion.Clone()
	if row.BillingAllocations != nil {
		allocs := make([]billing.BillingAllocation, len(row.BillingAllocations))
		for i, a := range row.BillingAllocations {
			a.SubscriptionID = cloneLabValuePtr(a.SubscriptionID)
			a.PlanID = cloneLabValuePtr(a.PlanID)
			allocs[i] = a
		}
		out.BillingAllocations = allocs
	}
	out.ProviderRateMultiplier = cloneLabValuePtr(row.ProviderRateMultiplier)
	out.ProviderStatsCost = cloneLabValuePtr(row.ProviderStatsCost)
	out.DurationMs = cloneLabValuePtr(row.DurationMs)
	out.UserAgent = cloneLabValuePtr(row.UserAgent)
	out.IPAddress = cloneLabValuePtr(row.IPAddress)
	out.SessionID = cloneLabValuePtr(row.SessionID)
	out.UpstreamRequestID = cloneLabValuePtr(row.UpstreamRequestID)
	out.ImageSize = cloneLabValuePtr(row.ImageSize)
	out.ImageInputSize = cloneLabValuePtr(row.ImageInputSize)
	out.ImageOutputSize = cloneLabValuePtr(row.ImageOutputSize)
	out.ImageSizeSource = cloneLabValuePtr(row.ImageSizeSource)
	out.ImageSizeBreakdown = maps.Clone(row.ImageSizeBreakdown)
	out.MediaType = cloneLabValuePtr(row.MediaType)
	out.VideoResolution = cloneLabValuePtr(row.VideoResolution)
	out.VideoDurationSeconds = cloneLabValuePtr(row.VideoDurationSeconds)
	return out
}

// persistCredentials replaces the Credentials map on the stored provider record
// matching id with a shallow copy of creds under s.mu so subsequent GetByID
// and selection reads observe backfilled credentials. Unknown provider IDs
// leave s.values unchanged.
func (s *protocolLabProviderStore) persistCredentials(id int64, creds map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.values {
		if s.values[i].Record.ID == id {
			s.values[i].Record.Credentials = maps.Clone(creds)
		}
	}
}

// newProtocolLabSettingsStore constructs a concurrency-safe in-memory settings
// repository seeded with a cloned copy of initial. Passing a nil initial map
// starts with an empty setting table, and mutating initial after construction
// does not alter stored runtime settings.
func newProtocolLabSettingsStore(initial map[string]string) *protocolLabSettingsStore {
	values := make(map[string]string, len(initial))
	maps.Copy(values, initial)
	return &protocolLabSettingsStore{values: values}
}

// Get reads a Setting record for key under s.mu when key is present in s.values.
// Absent keys return nil and settingscore.ErrSettingNotFound so callers
// distinguish missing entries from empty setting strings.
func (s *protocolLabSettingsStore) Get(_ context.Context, key string) (*settingscore.Setting, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, ok := s.values[key]; ok {
		return &settingscore.Setting{Key: key, Value: val}, nil
	}
	return nil, settingscore.ErrSettingNotFound
}

// GetValue returns the stored string value for key under s.mu when key exists
// in s.values. Absent keys return an empty string and
// settingscore.ErrSettingNotFound so runtime readers fall back to built-in defaults.
func (s *protocolLabSettingsStore) GetValue(_ context.Context, key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if val, ok := s.values[key]; ok {
		return val, nil
	}
	return "", settingscore.ErrSettingNotFound
}

// Set stores or overwrites value under key in s.values while holding s.mu,
// allocating the backing map when nil so live runtime readers observe updated
// setting strings immediately. It always returns a nil error in memory.
func (s *protocolLabSettingsStore) Set(_ context.Context, key, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.values == nil {
		s.values = make(map[string]string)
	}
	s.values[key] = value
	return nil
}

// GetMultiple returns a newly allocated map containing every requested key
// currently present in s.values under s.mu. Absent keys are omitted from the
// returned map, and mutating the result does not affect s.values.
func (s *protocolLabSettingsStore) GetMultiple(_ context.Context, keys []string) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		if val, ok := s.values[key]; ok {
			out[key] = val
		}
	}
	return out, nil
}

// SetMultiple copies all key-value entries from updates into s.values
// atomically under s.mu, allocating s.values when nil. Existing keys omitted
// from updates remain intact, and empty updates succeed with a nil error.
func (s *protocolLabSettingsStore) SetMultiple(_ context.Context, updates map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.values == nil {
		s.values = make(map[string]string, len(updates))
	}
	maps.Copy(s.values, updates)
	return nil
}

// GetAll returns a detached copy of all stored setting entries under s.mu.
// Callers can mutate the returned map freely without affecting s.values, and a
// nil backing map returns nil with a nil error.
func (s *protocolLabSettingsStore) GetAll(_ context.Context) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.values), nil
}

// Delete removes key from s.values while holding s.mu so subsequent Get and
// GetValue calls return settingscore.ErrSettingNotFound. Deleting an absent key
// or calling Delete when s.values is nil succeeds with a nil error.
func (s *protocolLabSettingsStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, key)
	return nil
}

// newProtocolLabAccessTokenCache constructs an empty concurrency-safe OAuth
// access-token and refresh-lock cache.
func newProtocolLabAccessTokenCache() *protocolLabAccessTokenCache {
	return &protocolLabAccessTokenCache{
		tokens: make(map[string]protocolLabCachedToken),
		locks:  make(map[string]time.Time),
	}
}

// GetAccessToken returns a non-expired cached bearer token for key under c.mu.
// Missing or expired entries return an empty string and nil error after
// evicting any stale entry from c.tokens.
func (c *protocolLabAccessTokenCache) GetAccessToken(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.tokens[key]
	if !ok {
		return "", nil
	}
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		// Evict expired bearer tokens on read so token sources trigger fresh OAuth
		// resolution instead of replaying stale credentials.
		delete(c.tokens, key)
		return "", nil
	}
	return entry.token, nil
}

// SetAccessToken stores token under key with the supplied positive ttl under
// c.mu, replacing any prior cached token for that key. Nonpositive durations
// keep the entry cached without an expiration deadline.
func (c *protocolLabAccessTokenCache) SetAccessToken(_ context.Context, key, token string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	c.tokens[key] = protocolLabCachedToken{token: token, expiresAt: exp}
	return nil
}

// DeleteAccessToken removes any cached bearer token stored under key while
// holding c.mu without disturbing active refresh locks. Absent keys succeed as
// a no-op and return a nil error.
func (c *protocolLabAccessTokenCache) DeleteAccessToken(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.tokens, key)
	return nil
}

// AcquireRefreshLock claims refresh admission for key under c.mu when no
// unexpired lock exists and returns true. Nonpositive ttl values default to a
// thirty-second lease window. Active locks return false and a nil error so
// competing callers wait for the holder's refresh result.
func (c *protocolLabAccessTokenCache) AcquireRefreshLock(_ context.Context, key string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if exp, held := c.locks[key]; held && now.Before(exp) {
		return false, nil
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	c.locks[key] = now.Add(ttl)
	return true, nil
}

// ReleaseRefreshLock removes any refresh lock entry held for key under c.mu so
// subsequent refresh attempts can acquire the lock immediately. Releasing an
// unheld key succeeds without error.
func (c *protocolLabAccessTokenCache) ReleaseRefreshLock(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.locks, key)
	return nil
}

// Reset clears all cached OAuth bearer tokens and active refresh locks while
// holding c.mu. Subsequent token lookups miss the cache, re-read current
// provider credentials, and can acquire refresh locks immediately.
func (c *protocolLabAccessTokenCache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	clear(c.tokens)
	clear(c.locks)
}

// Header returns the buffered HTTP response header map on r, allocating an
// empty http.Header on first access. Subsequent mutations on the returned map
// update r.header in place before ServeHTTP copies headers to the outer writer.
func (r *protocolLabHijackRecorder) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

// WriteHeader latches statusCode onto r.status on the first call and marks
// r.wroteHeader true. Subsequent calls to WriteHeader are ignored so r matches
// standard http.ResponseWriter single-commit header semantics.
func (r *protocolLabHijackRecorder) WriteHeader(statusCode int) {
	if r.wroteHeader {
		return
	}
	r.status = statusCode
	r.wroteHeader = true
}

// Write appends data into r.body and commits HTTP 200 status when WriteHeader
// has not yet been called. It returns the byte count written to r.body
// alongside a nil error.
func (r *protocolLabHijackRecorder) Write(data []byte) (int, error) {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
	return r.body.Write(data)
}

// Flush satisfies http.Flusher on r while keeping mockupstream SSE chunks
// buffered in r.body until ServeHTTP wraps them. When headers have not yet
// been committed, Flush latches HTTP 200 status.
func (r *protocolLabHijackRecorder) Flush() {
	if !r.wroteHeader {
		r.WriteHeader(http.StatusOK)
	}
}

// Hijack records that mockupstream requested an abrupt mid-stream TCP disconnect
// by setting r.hijacked true. It returns a sentinel error so mockupstream skips
// closing a nil net.Conn while ServeHTTP hijacks the real httptest socket after
// wrapping buffered SSE frames.
func (r *protocolLabHijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	r.hijacked = true
	return nil, nil, errors.New("hijack captured by protocolLabHijackRecorder")
}

// newProtocolLabAntigravityUpstream wraps mock inside an HTTP handler speaking
// the Antigravity v1internal request and response envelope contract.
func newProtocolLabAntigravityUpstream(mock *mockupstream.Handler) *protocolLabAntigravityUpstream {
	return &protocolLabAntigravityUpstream{mock: mock}
}

// ServeHTTP validates and unwraps incoming Antigravity v1internal POST requests
// (/v1internal:streamGenerateContent and /v1internal:generateContent) and
// delegates inner Gemini GenerateContent validation and scripted reply selection
// to u.mock. HTTP 200 SSE and JSON Gemini payloads from u.mock are wrapped into
// {"response": ...} envelopes before writing to w.
//
// Unrecognized URL paths, non-POST methods, or malformed outer envelopes return
// Google-shaped JSON errors directly, and outer envelope errors are recorded on
// u for Violations and AssertNoViolations checks. Non-200 upstream error
// statuses from u.mock pass through unwrapped with their original headers.
//
// Abrupt mid-stream truncations requested by Fault.TruncateAfterFrames are
// intercepted via protocolLabHijackRecorder so buffered SSE events still receive
// {"response": ...} wrapping. After flushing wrapped chunks, ServeHTTP hijacks
// and closes w's underlying TCP connection so downstream readers observe an
// authentic io.ErrUnexpectedEOF disconnect.
func (u *protocolLabAntigravityUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rawBody, readErr := io.ReadAll(r.Body)
	_ = r.Body.Close()

	cleanPath := strings.TrimRight(strings.TrimSpace(r.URL.Path), "/")
	var action string
	switch {
	case strings.HasSuffix(cleanPath, "/v1internal:streamGenerateContent"):
		action = "streamGenerateContent"
	case strings.HasSuffix(cleanPath, "/v1internal:generateContent"):
		action = "generateContent"
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, `{"error":{"code":404,"message":"unrecognized antigravity v1internal path %s","status":"NOT_FOUND"}}`, r.URL.Path)
		return
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = fmt.Fprintf(w, `{"error":{"code":405,"message":"method %s not allowed","status":"INVALID_ARGUMENT"}}`, r.Method)
		return
	}
	if readErr != nil {
		u.recordEnvelopeError(fmt.Sprintf("read v1internal body: %v", readErr))
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `{"error":{"code":400,"message":"read body failed","status":"INVALID_ARGUMENT"}}`)
		return
	}

	captured, envErr := unwrapAntigravityV1InternalEnvelope(r.URL.Path, r.URL.RawQuery, r.Header, rawBody)
	u.mu.Lock()
	u.envelopes = append(u.envelopes, captured)
	if envErr != nil {
		u.envelopeErrors = append(u.envelopeErrors, envErr.Error())
	}
	u.mu.Unlock()

	if envErr != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprintf(w, `{"error":{"code":400,"message":%q,"status":"INVALID_ARGUMENT"}}`, envErr.Error())
		return
	}

	innerReq := r.Clone(r.Context())
	innerURL := *r.URL
	innerURL.Path = "/v1beta/models/" + url.PathEscape(captured.Model) + ":" + action
	innerReq.URL = &innerURL
	innerReq.Body = io.NopCloser(bytes.NewReader(captured.InnerBody))
	innerReq.ContentLength = int64(len(captured.InnerBody))

	rec := &protocolLabHijackRecorder{status: http.StatusOK}
	u.mock.ServeHTTP(rec, innerReq)

	// Omit inner Content-Length headers because wrapping HTTP 200 payloads into
	// {"response": ...} envelopes increases the serialized response byte length.
	for k, vals := range rec.Header() {
		if strings.EqualFold(k, "Content-Length") {
			continue
		}
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}

	outBytes := rec.body.Bytes()
	ct := strings.ToLower(rec.Header().Get("Content-Type"))
	if rec.status == http.StatusOK {
		switch {
		case strings.Contains(ct, "text/event-stream"):
			outBytes = wrapAntigravityV1InternalSSE(outBytes)
		case strings.Contains(ct, "application/json"):
			outBytes = wrapAntigravityV1InternalJSON(outBytes)
		}
	}

	w.WriteHeader(rec.status)
	if len(outBytes) > 0 {
		_, _ = w.Write(outBytes)
	}
	if flusher, ok := w.(http.Flusher); ok && (strings.Contains(ct, "text/event-stream") || rec.hijacked) {
		flusher.Flush()
	}
	if rec.hijacked {
		// Propagate Fault.TruncateAfterFrames' raw socket closure to the live
		// httptest connection after wrapped SSE chunks have been flushed.
		if hj, ok := w.(http.Hijacker); ok {
			if conn, rw, err := hj.Hijack(); err == nil {
				_ = rw.Flush()
				_ = conn.Close()
			}
		}
	}
}

// recordEnvelopeError appends msg to u.envelopeErrors while holding u.mu for later inspection.
func (u *protocolLabAntigravityUpstream) recordEnvelopeError(msg string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.envelopeErrors = append(u.envelopeErrors, msg)
}

// Envelopes returns detached copies of all outer v1internal request envelopes
// received by u in arrival order under u.mu. Header maps, RawEnvelope slices,
// and InnerBody slices are cloned so caller mutations cannot alter captured logs.
func (u *protocolLabAntigravityUpstream) Envelopes() []protocolLabAntigravityRequest {
	u.mu.Lock()
	defer u.mu.Unlock()
	out := make([]protocolLabAntigravityRequest, len(u.envelopes))
	for i, env := range u.envelopes {
		cp := env
		if env.Headers != nil {
			cp.Headers = env.Headers.Clone()
		}
		cp.RawEnvelope = bytes.Clone(env.RawEnvelope)
		cp.InnerBody = bytes.Clone(env.InnerBody)
		out[i] = cp
	}
	return out
}

// Requests returns deep-cloned inner Gemini request records captured by u.mock.
func (u *protocolLabAntigravityUpstream) Requests() []mockupstream.RecordedRequest {
	return u.mock.Requests()
}

// Violations returns all strict Gemini request violations from u.mock combined
// with any outer v1internal envelope validation errors recorded by u. Outer
// envelope errors are prepended in a synthetic RequestIndex -1 ViolationEntry.
func (u *protocolLabAntigravityUpstream) Violations() []mockupstream.ViolationEntry {
	entries := u.mock.Violations()
	u.mu.Lock()
	envErrs := slices.Clone(u.envelopeErrors)
	u.mu.Unlock()
	if len(envErrs) > 0 {
		entries = append([]mockupstream.ViolationEntry{{
			RequestIndex: -1,
			Method:       http.MethodPost,
			Path:         "/v1internal",
			Protocol:     string(protocolcore.ProtocolGeminiGenerateContent),
			CheckErrors:  envErrs,
		}}, entries...)
	}
	return entries
}

// AssertNoViolations fails tb immediately if u recorded any outer v1internal
// envelope error under u.mu or any inner strict Gemini request violation on
// u.mock. Clean runs return without side effects.
func (u *protocolLabAntigravityUpstream) AssertNoViolations(tb testing.TB) {
	tb.Helper()
	u.mu.Lock()
	envErrs := slices.Clone(u.envelopeErrors)
	u.mu.Unlock()
	if len(envErrs) > 0 {
		tb.Fatalf("antigravity v1internal envelope violation(s): %s", strings.Join(envErrs, "; "))
	}
	u.mock.AssertNoViolations(tb)
}

// Reset clears all captured outer v1internal envelopes and envelope errors
// under u.mu, then resets u.mock's recorded inner requests, violations, and
// reply cursors. Subsequent subtests reuse u from a clean initial state.
func (u *protocolLabAntigravityUpstream) Reset() {
	u.mu.Lock()
	u.envelopes = nil
	u.envelopeErrors = nil
	u.mu.Unlock()
	u.mock.Reset()
}

// unwrapAntigravityV1InternalEnvelope parses rawBody with a streaming
// jsontext.Decoder, extracting outer project, model, requestId, userAgent, and
// requestType strings alongside verbatim inner request object bytes. Reading
// the inner request via ReadValue preserves 64-bit integer literals and member
// order without map[string]any float64 coercion before strict Gemini request
// validation runs.
//
// Non-object payloads, duplicate top-level keys, or missing project, model, or
// object request fields return a descriptive error alongside any partial header
// and RawEnvelope snapshot already populated on rec.
func unwrapAntigravityV1InternalEnvelope(
	urlPath, rawQuery string,
	headers http.Header,
	rawBody []byte,
) (protocolLabAntigravityRequest, error) {
	rec := protocolLabAntigravityRequest{
		Path:        urlPath,
		RawQuery:    rawQuery,
		Headers:     headers.Clone(),
		RawEnvelope: bytes.Clone(rawBody),
	}

	dec := jsontext.NewDecoder(bytes.NewReader(rawBody), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return rec, errors.New("v1internal payload must be a JSON object")
	}

	seenKeys := make(map[string]bool)
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return rec, fmt.Errorf("read v1internal key: %w", kErr)
		}
		key := kTok.String()
		if seenKeys[key] {
			return rec, fmt.Errorf("duplicate v1internal top-level key %q", key)
		}
		seenKeys[key] = true

		val, vErr := dec.ReadValue()
		if vErr != nil {
			return rec, fmt.Errorf("read v1internal %q value: %w", key, vErr)
		}
		// Non-string outer metadata tokens leave string fields empty so required
		// project and model checks below reject mistyped outer values cleanly.
		switch key {
		case "project":
			_ = json.Unmarshal(val, &rec.Project)
		case "model":
			_ = json.Unmarshal(val, &rec.Model)
		case "requestId":
			_ = json.Unmarshal(val, &rec.RequestID)
		case "userAgent":
			_ = json.Unmarshal(val, &rec.UserAgent)
		case "requestType":
			_ = json.Unmarshal(val, &rec.RequestType)
		case "request":
			trimmed := bytes.TrimSpace(val)
			if len(trimmed) > 0 && trimmed[0] == '{' {
				rec.InnerBody = bytes.Clone(trimmed)
			}
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return rec, fmt.Errorf("close v1internal object: %w", err)
	}
	if strings.TrimSpace(rec.Project) == "" {
		return rec, errors.New("v1internal project is required")
	}
	if strings.TrimSpace(rec.Model) == "" {
		return rec, errors.New("v1internal model is required")
	}
	if len(rec.InnerBody) == 0 {
		return rec, errors.New("v1internal request object is required")
	}
	return rec, nil
}

// wrapAntigravityV1InternalJSON wraps a buffered Gemini JSON object into a
// {"response": ...} envelope for HTTP 200 v1internal:generateContent replies.
// Empty, non-object, or invalid JSON slices, as well as objects already
// carrying a top-level "response" or "error" field, are returned unchanged.
func wrapAntigravityV1InternalJSON(raw []byte) []byte {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
		return raw
	}
	if gjson.GetBytes(trimmed, "response").Exists() || gjson.GetBytes(trimmed, "error").Exists() {
		return raw
	}
	var out bytes.Buffer
	out.Grow(len(trimmed) + 14)
	_, _ = out.WriteString(`{"response":`)
	_, _ = out.Write(trimmed)
	_ = out.WriteByte('}')
	return out.Bytes()
}

// wrapAntigravityV1InternalSSE wraps each Gemini JSON object event inside raw
// SSE bytes into a {"response": ...} envelope while preserving wire-level SSE
// framing quirks. Physical line terminators (\r\n, \n, or trailing EOF without
// newline), leading UTF-8 BOM bytes, SSE comment lines, spaceless data:
// prefixes, multi-line data: splits, and trailing horizontal whitespace after
// closing braces all survive verbatim so T5 SSE-quirk tests exercise the
// production Antigravity scanner faithfully.
//
// Non-JSON payloads such as [DONE] and events that already carry top-level
// "response" or "error" keys remain untouched. Empty raw slices return raw
// directly without allocating.
func wrapAntigravityV1InternalSSE(raw []byte) []byte {
	if len(raw) == 0 {
		return raw
	}

	// ssePhysicalLine retains one physical line body alongside its original CR/LF terminator.
	type ssePhysicalLine struct {
		body   []byte // line bytes excluding trailing \r\n or \n
		ending []byte // verbatim "\r\n", "\n", or empty EOF slice
	}

	var lines []ssePhysicalLine
	for offset := 0; offset < len(raw); {
		nlRel := bytes.IndexByte(raw[offset:], '\n')
		if nlRel < 0 {
			lines = append(lines, ssePhysicalLine{body: bytes.Clone(raw[offset:])})
			break
		}
		endIdx := offset + nlRel
		if endIdx > offset && raw[endIdx-1] == '\r' {
			lines = append(lines, ssePhysicalLine{
				body:   bytes.Clone(raw[offset : endIdx-1]),
				ending: []byte("\r\n"),
			})
		} else {
			lines = append(lines, ssePhysicalLine{
				body:   bytes.Clone(raw[offset:endIdx]),
				ending: []byte("\n"),
			})
		}
		offset = endIdx + 1
	}

	wrapBlock := func(start, end int) {
		var dataLineIndices []int
		var joined bytes.Buffer
		for idx := start; idx < end; idx++ {
			line := lines[idx].body
			if idx == 0 {
				// Strip a leading UTF-8 BOM only while inspecting field names so
				// line[0] remains preserved in the serialized physical output.
				line = bytes.TrimPrefix(line, []byte{0xEF, 0xBB, 0xBF})
			}
			if len(line) == 0 || line[0] == ':' {
				continue
			}
			field, content, hasColon := bytes.Cut(line, []byte(":"))
			if !hasColon || string(field) != "data" {
				continue
			}
			content = bytes.TrimPrefix(content, []byte(" "))
			if len(dataLineIndices) > 0 {
				_ = joined.WriteByte('\n')
			}
			_, _ = joined.Write(content)
			dataLineIndices = append(dataLineIndices, idx)
		}
		if len(dataLineIndices) == 0 {
			return
		}
		trimmed := bytes.TrimSpace(joined.Bytes())
		if len(trimmed) == 0 || trimmed[0] != '{' || !json.Valid(trimmed) {
			return
		}
		if gjson.GetBytes(trimmed, "response").Exists() || gjson.GetBytes(trimmed, "error").Exists() {
			return
		}

		// Splice {"response": before the opening brace on the first data: line
		// and } after the closing brace on the last data: line so multi-line
		// splits and trailing horizontal whitespace remain intact.
		firstIdx := dataLineIndices[0]
		firstBody := lines[firstIdx].body
		if bracePos := bytes.IndexByte(firstBody, '{'); bracePos >= 0 {
			var b bytes.Buffer
			b.Grow(len(firstBody) + 12)
			_, _ = b.Write(firstBody[:bracePos])
			_, _ = b.WriteString(`{"response":`)
			_, _ = b.Write(firstBody[bracePos:])
			lines[firstIdx].body = b.Bytes()
		}

		lastIdx := dataLineIndices[len(dataLineIndices)-1]
		lastBody := lines[lastIdx].body
		if closePos := bytes.LastIndexByte(lastBody, '}'); closePos >= 0 {
			var b bytes.Buffer
			b.Grow(len(lastBody) + 1)
			_, _ = b.Write(lastBody[:closePos+1])
			_ = b.WriteByte('}')
			_, _ = b.Write(lastBody[closePos+1:])
			lines[lastIdx].body = b.Bytes()
		}
	}

	blockStart := -1
	for i := range lines {
		checkLine := lines[i].body
		if i == 0 {
			checkLine = bytes.TrimPrefix(checkLine, []byte{0xEF, 0xBB, 0xBF})
		}
		if len(checkLine) == 0 {
			if blockStart >= 0 {
				wrapBlock(blockStart, i)
				blockStart = -1
			}
			continue
		}
		if blockStart < 0 {
			blockStart = i
		}
	}
	if blockStart >= 0 {
		wrapBlock(blockStart, len(lines))
	}

	var out bytes.Buffer
	out.Grow(len(raw) + 64)
	for _, ln := range lines {
		_, _ = out.Write(ln.body)
		_, _ = out.Write(ln.ending)
	}
	return out.Bytes()
}

// GetByKeyForAuth returns a fresh APIKey copy bound to r.lab's current group
// configuration when credential matches r.lab.APIKey, or ErrAPIKeyNotFound
// otherwise. Multi-turn WebSocket sessions call Reauthenticate through this
// method before admitting Turn 2 and later frames.
func (r *protocolLabKeyRepo) GetByKeyForAuth(_ context.Context, credential string) (*apikey.APIKey, error) {
	if r == nil || r.lab == nil || credential != r.lab.APIKey {
		return nil, apikey.ErrAPIKeyNotFound
	}
	grp := r.lab.cloneGroup(r.groupID)
	gid := r.groupID
	return &apikey.APIKey{
		ID:          r.keyID,
		Key:         r.lab.APIKey,
		Status:      billing.StatusActive,
		UserID:      r.userID,
		User:        r.user,
		GroupID:     &gid,
		Group:       grp,
		BillingMode: apikey.APIKeyBillingModeBalance,
	}, nil
}

// UpdateLastUsed accepts fire-and-forget key usage timestamp touches in tests.
func (*protocolLabKeyRepo) UpdateLastUsed(context.Context, int64, time.Time) error { return nil }
