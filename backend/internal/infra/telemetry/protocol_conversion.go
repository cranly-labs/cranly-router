package telemetry

import (
	"context"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"
)

// protocol_conversion.go defines bounded protocol-conversion diagnostics and
// coordinates attempt-scoped staging across request records and usage completion.
// Gateway requests can traverse native carrier replay, same-protocol Responses
// client-tool adaptation, or full cross-protocol IR conversion, and a single
// logical request may fail over from a converted provider attempt to a native
// passthrough attempt before succeeding.
//
// Attaching an isolated conversion scope per selected provider attempt lets
// pre-dispatch preparation record request-side transformations immediately for
// failed HTTP exchanges while keeping subsequent failover attempts clean. When a
// converted response reader finishes, ReplaceProtocolConversion swaps in the
// combined request-and-response loss summary on the active non-error attempt
// without overwriting any earlier HTTP >= 400 attempt already marked failed.

const (
	// ConversionMode values classify the highest-depth protocol transformation
	// applied during one upstream provider attempt.
	ConversionModeIRConverted  = "ir_converted"  // cross-protocol IR request and response conversion
	ConversionModeToolAdapted  = "tool_adapted"  // same-protocol Responses client-tool lowering and restoration
	ConversionModeNativeReplay = "native_replay" // native Messages or Responses trc1 continuity envelope replay

	// requestConversionScopeKey stores the mutable per-attempt conversion slot on a context.
	requestConversionScopeKey ContextKey = "request_conversion_scope" // per-attempt *protocolConversionScope

	// Field and map bounds cap persisted diagnostic JSON size and prevent
	// unbounded or untrusted strings from entering storage.
	maxConversionFieldBytes = 64 // maximum UTF-8 byte length for protocol, dialect, and scheme labels
	maxConversionLossKinds  = 32 // maximum distinct LossKind keys retained in LossCounts
)

// ProtocolConversion summarizes bounded, redacted protocol transformation
// metadata for administrator usage logs and request-lookup attempt details.
// Pure lossless native passthrough requests leave *ProtocolConversion nil so
// database rows and JSON responses incur zero extra storage overhead.
type ProtocolConversion struct {
	Mode             string         `json:"mode"`                    // ir_converted, tool_adapted, or native_replay
	ClientProtocol   string         `json:"client_protocol"`         // inbound client wire protocol ID
	UpstreamProtocol string         `json:"upstream_protocol"`       // outbound upstream wire protocol ID
	Dialect          string         `json:"dialect,omitempty"`       // resolved target endpoint dialect name
	ReplayScheme     string         `json:"replay_scheme,omitempty"` // active continuity token scheme when applicable
	LossCounts       map[string]int `json:"loss_counts,omitempty"`   // positive occurrence counts keyed by LossKind
}

// protocolConversionScope holds mutable conversion diagnostics for one selected
// provider attempt so outer request preparation, net/http trace dispatch, and
// post-response completion share state without leaking across failover attempts.
type protocolConversionScope struct {
	// mu serializes updates to value and lastRef across preparation, transport
	// attempt registration, and response-reader finalization.
	mu      sync.Mutex          // guards value and lastRef
	value   *ProtocolConversion // normalized snapshot staged for this provider selection
	lastRef RequestAttemptRef   // latest HTTP dispatch ordinal registered under this scope
}

// NewProtocolConversion constructs and normalizes a ProtocolConversion value
// from raw route labels and aggregated loss counts. Unrecognized modes or blank
// protocol identifiers return nil so invalid partial inputs never persist.
func NewProtocolConversion(
	mode string,
	clientProtocol string,
	upstreamProtocol string,
	dialect string,
	replayScheme string,
	lossCounts map[string]int,
) *ProtocolConversion {
	return NormalizeProtocolConversion(&ProtocolConversion{
		Mode:             mode,
		ClientProtocol:   clientProtocol,
		UpstreamProtocol: upstreamProtocol,
		Dialect:          dialect,
		ReplayScheme:     replayScheme,
		LossCounts:       lossCounts,
	})
}

// Clone returns an independent deep copy of p with detached LossCounts storage.
// Nil receivers return nil so absent conversion observations stay nil.
func (p *ProtocolConversion) Clone() *ProtocolConversion {
	if p == nil {
		return nil
	}
	out := *p
	if len(p.LossCounts) > 0 {
		out.LossCounts = maps.Clone(p.LossCounts)
	} else {
		out.LossCounts = nil
	}
	return &out
}

// NormalizeProtocolConversion validates, trims, and bounds in into a newly
// allocated ProtocolConversion snapshot. Nil inputs, unknown modes, or empty
// client/upstream protocol identifiers return nil, and nonpositive or malformed
// LossCounts entries are discarded so empty maps serialize as omitted fields.
func NormalizeProtocolConversion(in *ProtocolConversion) *ProtocolConversion {
	if in == nil {
		return nil
	}
	mode := strings.TrimSpace(in.Mode)
	if conversionModeRank(mode) == 0 {
		return nil
	}
	clientProto := sanitizeConversionLabel(in.ClientProtocol)
	upstreamProto := sanitizeConversionLabel(in.UpstreamProtocol)
	if clientProto == "" || upstreamProto == "" {
		return nil
	}
	out := &ProtocolConversion{
		Mode:             mode,
		ClientProtocol:   clientProto,
		UpstreamProtocol: upstreamProto,
		Dialect:          sanitizeConversionLabel(in.Dialect),
		ReplayScheme:     sanitizeConversionLabel(in.ReplayScheme),
	}
	if len(in.LossCounts) > 0 {
		var counts map[string]int
		for rawKey, count := range in.LossCounts {
			if count <= 0 {
				continue
			}
			key := strings.TrimSpace(rawKey)
			if !isValidLossKindKey(key) {
				continue
			}
			if counts == nil {
				counts = make(map[string]int, min(len(in.LossCounts), maxConversionLossKinds))
			}
			counts[key] += count
		}
		out.LossCounts = capLossCountsDeterministic(counts)
	}
	return out
}

// MergeProtocolConversion combines two observations from the same provider
// attempt, selecting the higher-precedence Mode (ir_converted > tool_adapted >
// native_replay), retaining non-empty Dialect and ReplayScheme metadata, and
// summing LossCounts into independent map storage.
func MergeProtocolConversion(base, next *ProtocolConversion) *ProtocolConversion {
	normBase := NormalizeProtocolConversion(base)
	normNext := NormalizeProtocolConversion(next)
	if normBase == nil {
		return normNext
	}
	if normNext == nil {
		return normBase
	}

	winner := normBase
	other := normNext
	if conversionModeRank(normNext.Mode) >= conversionModeRank(normBase.Mode) {
		winner = normNext
		other = normBase
	}
	out := winner.Clone()
	if out.Dialect == "" {
		out.Dialect = other.Dialect
	}
	if out.ReplayScheme == "" {
		out.ReplayScheme = other.ReplayScheme
	}
	if len(other.LossCounts) > 0 {
		if out.LossCounts == nil {
			out.LossCounts = make(map[string]int, len(other.LossCounts))
		}
		for k, v := range other.LossCounts {
			out.LossCounts[k] += v
		}
		out.LossCounts = capLossCountsDeterministic(out.LossCounts)
	}
	return out
}

// capLossCountsDeterministic trims counts exceeding maxConversionLossKinds in
// lexicographical key order so bounded snapshots never depend on map hash seeds.
func capLossCountsDeterministic(counts map[string]int) map[string]int {
	if len(counts) == 0 {
		return nil
	}
	if len(counts) <= maxConversionLossKinds {
		return counts
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	trimmed := make(map[string]int, maxConversionLossKinds)
	for _, k := range keys[:maxConversionLossKinds] {
		trimmed[k] = counts[k]
	}
	return trimmed
}

// WithProtocolConversionScope attaches a fresh per-attempt conversion staging
// slot to ctx so observations recorded on the returned context remain isolated
// from earlier or subsequent provider failover attempts. Nil ctx defaults to
// context.Background.
func WithProtocolConversionScope(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requestConversionScopeKey, &protocolConversionScope{})
}

// CopyProtocolConversionScope attaches source's conversion staging slot to
// destination when present. Nil or unscoped sources return destination
// unchanged, allowing per-turn WebSocket callbacks to share one attempt slot.
func CopyProtocolConversionScope(destination, source context.Context) context.Context {
	if source == nil {
		return destination
	}
	if scope, ok := source.Value(requestConversionScopeKey).(*protocolConversionScope); ok && scope != nil {
		return context.WithValue(destination, requestConversionScopeKey, scope)
	}
	return destination
}

// CopyProtocolConversionSnapshot attaches an isolated conversion scope seeded
// with source's current ProtocolConversion clone to destination when non-nil.
// Detached completion contexts retain the finished attempt's conversion
// observation without sharing mutable state with subsequent WebSocket turns.
func CopyProtocolConversionSnapshot(destination, source context.Context) context.Context {
	if destination == nil {
		destination = context.Background()
	}
	snap := ProtocolConversionFromContext(source)
	if snap == nil {
		return destination
	}
	return context.WithValue(destination, requestConversionScopeKey, &protocolConversionScope{
		value: snap,
	})
}

// ObserveProtocolConversion merges next into ctx's active attempt conversion
// scope and updates any running RequestAttempt already registered under that
// scope. Nil contexts, invalid snapshots, or contexts lacking both a scope and a
// RequestCapture are ignored safely.
func ObserveProtocolConversion(ctx context.Context, next *ProtocolConversion) {
	normalized := NormalizeProtocolConversion(next)
	if ctx == nil || normalized == nil {
		return
	}
	scope := resolveConversionScope(ctx)
	if scope == nil {
		return
	}
	scope.mu.Lock()
	scope.value = MergeProtocolConversion(scope.value, normalized)
	merged := scope.value.Clone()
	ref := scope.lastRef
	scope.mu.Unlock()

	if ref.Number <= 0 {
		ref = RequestAttemptFromContext(ctx)
	}
	syncRecordedAttemptConversion(ctx, ref, merged)
}

// ReplaceProtocolConversion overwrites ctx's active attempt conversion snapshot
// with next after response conversion finishes or before a same-provider retry
// dispatches a replacement request body. Already-failed HTTP >= 400 attempts keep
// their previously recorded snapshot so pre-retry diagnostics survive intact.
func ReplaceProtocolConversion(ctx context.Context, next *ProtocolConversion) {
	normalized := NormalizeProtocolConversion(next)
	if ctx == nil {
		return
	}
	scope := resolveConversionScope(ctx)
	if scope == nil {
		return
	}
	scope.mu.Lock()
	scope.value = normalized.Clone()
	current := scope.value.Clone()
	ref := scope.lastRef
	scope.mu.Unlock()

	if ref.Number <= 0 {
		ref = RequestAttemptFromContext(ctx)
	}
	syncRecordedAttemptConversion(ctx, ref, current)
}

// ProtocolConversionFromContext returns a detached copy of the conversion
// diagnostics staged on ctx's current provider attempt, or nil when the attempt
// used pure lossless native passthrough.
func ProtocolConversionFromContext(ctx context.Context) *ProtocolConversion {
	if ctx == nil {
		return nil
	}
	scope := resolveConversionScope(ctx)
	if scope == nil {
		return nil
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	return scope.value.Clone()
}

// bindConversionScopeAttempt records ref as the latest HTTP dispatch under ctx's
// conversion scope and returns the currently staged ProtocolConversion snapshot
// for immediate attachment to the newly registered RequestAttempt.
func bindConversionScopeAttempt(ctx context.Context, ref RequestAttemptRef) *ProtocolConversion {
	scope := resolveConversionScope(ctx)
	if scope == nil {
		return nil
	}
	scope.mu.Lock()
	defer scope.mu.Unlock()
	if ref.Number > 0 {
		scope.lastRef = ref
	}
	return scope.value.Clone()
}

// resolveConversionScope returns the explicit per-attempt scope on ctx, falling
// back to the RequestCapture's default scope when callers did not install an
// attempt-specific scope in unit tests or single-attempt adapters.
func resolveConversionScope(ctx context.Context) *protocolConversionScope {
	if ctx == nil {
		return nil
	}
	if scope, ok := ctx.Value(requestConversionScopeKey).(*protocolConversionScope); ok && scope != nil {
		return scope
	}
	if capture, ok := ctx.Value(requestCaptureKey).(*RequestCapture); ok && capture != nil {
		return capture.ensureFallbackConversionScope()
	}
	return nil
}

// syncRecordedAttemptConversion updates the matching non-HTTP-error
// RequestAttempt on ctx's RequestCapture with snap. Attempts already marked with
// HTTP status >= 400 retain their pre-failure snapshot when a same-provider
// retry stages replacement request diagnostics prior to the next dispatch.
func syncRecordedAttemptConversion(ctx context.Context, ref RequestAttemptRef, snap *ProtocolConversion) {
	if ctx == nil || ref.Number <= 0 {
		return
	}
	UpdateRequest(ctx, func(record *RequestRecord) {
		for i := range record.Attempts {
			attempt := &record.Attempts[i]
			if attempt.Number != ref.Number || attempt.ProviderID != ref.ProviderID {
				continue
			}
			// Preserve the exact request diagnostics of an already rejected HTTP
			// exchange when a same-provider recovery retry prepares its next payload.
			if attempt.Status >= http.StatusBadRequest {
				return
			}
			attempt.ProtocolConversion = snap.Clone()
			return
		}
	})
}

// conversionModeRank orders conversion modes by transformation depth, returning
// zero for unrecognized mode labels.
func conversionModeRank(mode string) int {
	switch mode {
	case ConversionModeNativeReplay:
		return 1
	case ConversionModeToolAdapted:
		return 2
	case ConversionModeIRConverted:
		return 3
	default:
		return 0
	}
}

// sanitizeConversionLabel trims value, strips ASCII control characters and
// invalid UTF-8 sequences, and truncates to maxConversionFieldBytes on a rune boundary.
func sanitizeConversionLabel(value string) string {
	cleaned := strings.ToValidUTF8(strings.TrimSpace(value), "")
	if cleaned == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(min(len(cleaned), maxConversionFieldBytes))
	for _, r := range cleaned {
		if r < 0x20 || r == 0x7f {
			continue
		}
		if b.Len()+utf8.RuneLen(r) > maxConversionFieldBytes {
			break
		}
		_, _ = b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

// isValidLossKindKey reports whether key is a non-empty snake_case ASCII
// identifier within maxConversionFieldBytes.
func isValidLossKindKey(key string) bool {
	if key == "" || len(key) > maxConversionFieldBytes {
		return false
	}
	for i := range len(key) {
		ch := key[i]
		if (ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '_' {
			continue
		}
		return false
	}
	return true
}
