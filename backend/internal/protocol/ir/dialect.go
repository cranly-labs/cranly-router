package ir

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
)

// dialect.go defines shared upstream endpoint capabilities, client response
// view context, and deterministic runtime callbacks consumed by protocol codecs
// and the conversion engine. Placing Dialect, ClientView, and Runtime inside ir
// lets leaf protocol codecs accept rich target capabilities and client echo
// metadata without importing convert and creating a package dependency cycle.

const (
	// ToolSupport flags describe which tool declaration and invocation kinds an
	// upstream endpoint accepts natively without function-proxy lowering.
	ToolSupportNone            ToolSupport = 0                                                                                                                     // no native tool support configured
	ToolSupportFunction        ToolSupport = 1 << 0                                                                                                                // standard JSON-schema function tools
	ToolSupportCustom          ToolSupport = 1 << 1                                                                                                                // free-form custom grammar tools
	ToolSupportNamespace       ToolSupport = 1 << 2                                                                                                                // scoped Responses tool namespaces
	ToolSupportClientSearch    ToolSupport = 1 << 3                                                                                                                // client-executed tool_search discovery
	ToolSupportHostedWebSearch ToolSupport = 1 << 4                                                                                                                // provider-hosted web search declarations
	ToolSupportAllResponses                = ToolSupportFunction | ToolSupportCustom | ToolSupportNamespace | ToolSupportClientSearch | ToolSupportHostedWebSearch // combined Responses tool mask

	// HistoryMode values control when unsigned plaintext reasoning is written
	// back onto assistant turns in outgoing upstream requests.
	HistoryNone      HistoryMode = "none"       // omit unsigned plaintext reasoning on all turns
	HistoryToolTurns HistoryMode = "tool_turns" // retain plaintext reasoning on tool-calling turns
	HistoryAll       HistoryMode = "all"        // retain plaintext reasoning on every assistant turn

	// SchemaDialect values select which JSON Schema subset an upstream encoder
	// enforces on function tool parameters and structured output schemas.
	SchemaJSONSchema  SchemaDialect = "json_schema" // standard JSON Schema pass-through
	SchemaGemini      SchemaDialect = "gemini"      // Google Gemini uppercase schema subset
	SchemaAntigravity SchemaDialect = "antigravity" // Antigravity lowercase seven-key cleaned schema subset with constraint hints
)

// ToolSupport is a bitmask of tool capabilities supported natively by an upstream endpoint.
type ToolSupport uint16

// HistoryMode specifies how an upstream encoder handles plaintext reasoning in conversation history.
type HistoryMode string

// SchemaDialect identifies the target JSON Schema normalization rule set for an upstream endpoint.
type SchemaDialect string

// Runtime supplies wall-clock and response-side identifier callbacks so codecs
// stay deterministic in tests while generating unique metadata in production.
type Runtime struct {
	Now   func() time.Time           // wall clock for response created_at synthesis
	NewID func(prefix string) string // response-side identifier generator
}

// ClientView carries client-facing model naming, original decoded request echo
// fields, and response-side Runtime callbacks into response and stream encoders.
type ClientView struct {
	Model   string   // model identifier presented back to the client
	Request *Request // decoded client request for echo fields and include_usage
	Runtime Runtime  // clock and response-side ID generator
}

// ReasoningCapabilities carries model-catalog thinking controls, supported
// effort levels, and token budget bounds used to resolve ReasoningConfig.
type ReasoningCapabilities struct {
	ControlsKnown  bool     // whether catalog explicitly defines reasoning controls
	Supported      *bool    // explicit model thinking support when known
	Toggle         bool     // whether thinking mode can be toggled on or off
	Efforts        []string // supported canonical effort labels in ascending order
	Budget         bool     // whether explicit thinking token budgets are supported
	BudgetMin      int      // minimum thinking token budget
	BudgetMax      int      // maximum thinking token budget
	ForceBudget    bool     // whether thinking budget must always be emitted
	AdaptiveBudget int      // fallback budget when adaptive mode is lowered
	OutputLimit    *int     // maximum output tokens cap from model attributes
	BudgetPadding  int      // minimum output token headroom above thinking budget
}

// Dialect describes endpoint-specific capabilities, replay continuity rules,
// and parameter restrictions beyond the base wire protocol for an upstream attempt.
type Dialect struct {
	Name                string                // diagnostic label such as "openai" or "deepseek"
	ProviderID          int64                 // selected provider ID for account-bound replay tokens
	Tools               ToolSupport           // bitmask of natively supported tool categories
	ToolNameMaxBytes    int                   // maximum function name byte length; 0 means unlimited
	ReplayScheme        string                // continuity scheme issued and accepted by endpoint
	ReasoningHistory    HistoryMode           // plaintext reasoning history retention mode
	SchemaDialect       SchemaDialect         // JSON Schema cleaning dialect
	Reasoning           ReasoningCapabilities // model reasoning effort and budget capabilities
	StripEffort         bool                  // clear request effort before upstream encoding
	DropTemperature     bool                  // omit temperature per model attributes
	DropTopP            bool                  // omit top_p per model attributes
	DropTopK            bool                  // omit top_k per model attributes
	OmitStopSequences   bool                  // omit stop sequences per target rules
	OmitStoreAndInclude bool                  // omit cross-protocol store and include defaults
	AllowBillingHeader  bool                  // retain x-anthropic-billing-header in system text
	// StatelessResponses activates Responses-origin statelessness and strict
	// tool-pairing gates when encoding requests for stateless upstreams such as
	// Qoder, leaving Chat Completions and Anthropic Messages requests exempt.
	StatelessResponses bool   // enforce Responses statelessness and tool-pairing gates
	PlaceholderToken   string // Gemini fallback thoughtSignature for unsigned calls
}

// DefaultRuntime constructs a production Runtime backed by time.Now and
// twelve random hexadecimal bytes from crypto/rand. Non-empty prefixes that do
// not already end with '-' or '_' receive an '_' separator before the hex suffix.
func DefaultRuntime() Runtime {
	return Runtime{
		Now: time.Now,
		NewID: func(prefix string) string {
			var raw [12]byte
			// Best-effort random read fills zero-initialized stack bytes; crypto/rand
			// uses OS entropy and leaves raw deterministic zeros only if OS RNG fails.
			_, _ = rand.Read(raw[:])
			suffix := hex.EncodeToString(raw[:])
			if prefix == "" {
				return suffix
			}
			if strings.HasSuffix(prefix, "-") || strings.HasSuffix(prefix, "_") {
				return prefix + suffix
			}
			return prefix + "_" + suffix
		},
	}
}

// NowUnix returns the Unix timestamp in seconds from r.Now. When r.Now is nil,
// NowUnix returns zero so response encoders never panic on zero-valued Runtime structs.
func (r Runtime) NowUnix() int64 {
	if r.Now == nil {
		return 0
	}
	return r.Now().Unix()
}

// NextID invokes r.NewID with prefix when configured and returns fallback when
// r.NewID is nil or returns an empty string. Callers supply a deterministic
// fallback for zero-Runtime unit tests.
func (r Runtime) NextID(prefix, fallback string) string {
	if r.NewID != nil {
		if id := r.NewID(prefix); id != "" {
			return id
		}
	}
	return fallback
}

// Has reports whether s includes every capability bit set in flag.
func (s ToolSupport) Has(flag ToolSupport) bool {
	return s&flag == flag
}

// IsValid reports whether m is empty or one of the three defined HistoryMode constants.
func (m HistoryMode) IsValid() bool {
	switch m {
	case "", HistoryNone, HistoryToolTurns, HistoryAll:
		return true
	default:
		return false
	}
}

// IsValid reports whether d is empty or one of the three defined SchemaDialect constants.
func (d SchemaDialect) IsValid() bool {
	switch d {
	case "", SchemaJSONSchema, SchemaGemini, SchemaAntigravity:
		return true
	default:
		return false
	}
}

// Clone returns an independent copy of rc with detached Supported, Efforts, and
// OutputLimit backing storage. Callers can mutate capability slices or pointer
// bounds on the returned copy without aliasing the source value.
func (rc ReasoningCapabilities) Clone() ReasoningCapabilities {
	out := rc
	out.Supported = cloneBoolPtr(rc.Supported)
	out.Efforts = cloneNonEmpty(rc.Efforts)
	if rc.OutputLimit != nil {
		limit := *rc.OutputLimit
		out.OutputLimit = &limit
	}
	return out
}

// Clone returns an independent copy of d with a deep-copied Reasoning struct.
// Retry attempts can clear ReplayScheme or toggle StripEffort on the clone
// without mutating shared provider dialect descriptors.
func (d Dialect) Clone() Dialect {
	out := d
	out.Reasoning = d.Reasoning.Clone()
	return out
}
