package session

import (
	"container/list"
	"sync"
	"time"

	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// gemini_tool_calls.go retains scoped upstream tool-call identifiers and replay
// tokens behind client-visible call IDs across multi-turn conversions. Anthropic
// Messages, OpenAI Responses, and OpenAI Chat Completions clients return tool
// results keyed by client call IDs without carrying per-call thoughtSignature
// fields, while compatible Gemini and Antigravity endpoints require original
// call IDs and issuer tokens on subsequent turns. Scoping every entry by
// authenticated API Key, selected provider, and mapped upstream model prevents
// another key, account, or model from inheriting incompatible call signatures.
//
// A mutex-guarded FIFO bounds both live entry count and total retained string
// bytes for a one-hour replay window without retaining tool arguments or
// results. Recording an observed call with no signature still creates a live
// entry whose nil token distinguishes known unsigned calls from cache misses
// during placeholder injection, and late stream signatures update live entries
// without discarding a previously recorded upstream ID.

const (
	// geminiToolCallTTL, geminiToolCallMaxEntries, and geminiToolCallMaxBytes
	// bound replay binding lifetime to one hour and cap process-local entry count
	// and retained string bytes.
	geminiToolCallTTL        = time.Hour // live duration for each recorded call binding
	geminiToolCallMaxEntries = 16384     // maximum live call bindings retained at once
	geminiToolCallMaxBytes   = 32 << 20  // maximum retained string bytes across live bindings
)

// GeminiToolScope identifies the authenticated API key, upstream provider, and
// mapped upstream model authorized to record and replay tool-call bindings.
type GeminiToolScope struct {
	// APIKeyID, ProviderID, and Model must all hold positive identifiers and a
	// non-empty model name before GeminiToolCalls records or recalls bindings.
	APIKeyID   int64  // authenticated gateway API key identifier
	ProviderID int64  // selected upstream provider identifier
	Model      string // mapped upstream wire model identifier
}

// GeminiToolCalls stores process-local, destination-scoped tool-call replay
// bindings for Gemini and Antigravity multi-turn CallMemory sessions.
type GeminiToolCalls struct {
	// mu serializes access to entries, order, bytes, and now together so
	// expiration, FIFO eviction, and late-signature replacement keep index
	// membership and byte accounting consistent.
	mu      sync.Mutex                      // guards entries, order, bytes, and now
	entries map[geminiToolKey]*list.Element // scoped call keys to live FIFO elements
	order   list.List                       // geminiToolEntry values in record order
	bytes   int                             // sum of retained string bytes across entries
	// now supplies wall-clock timestamps while holding mu and defaults to
	// time.Now when nil so zero-value stores work without initialization.
	now func() time.Time // optional test clock read under mu
}

// geminiToolKey addresses one client-visible tool call ID within its permitted GeminiToolScope.
type geminiToolKey struct {
	scope    GeminiToolScope // authenticated key, provider, and model scope
	clientID string          // client-visible tool invocation identifier
}

// geminiToolEntry retains the upstream call identity, optional replay token,
// expiration deadline, and accounted byte size for one cached tool call.
type geminiToolEntry struct {
	key             geminiToolKey // map index key for FIFO eviction
	id              string        // original upstream call ID, or empty when omitted
	scheme          string        // replay token scheme such as gemini or antigravity_claude
	tokenProviderID int64         // optional account-bound issuing provider ID
	signature       string        // verbatim upstream call replay token data
	// hasToken reports whether the upstream call carried non-empty token data;
	// false preserves an observed unsigned call whose Recall returns Token == nil.
	hasToken bool      // whether signature holds a recorded replay token
	expires  time.Time // exclusive timestamp when replay validity ends
	size     int       // accounted string bytes contributed to GeminiToolCalls.bytes
}

// geminiCallMemory binds a GeminiToolCalls store to a single validated
// GeminiToolScope to implement convert.CallMemory for one upstream attempt.
type geminiCallMemory struct {
	store *GeminiToolCalls // backing process-local scoped tool-call cache
	scope GeminiToolScope  // fixed key, provider, and upstream model scope
}

// CallMemory returns a convert.CallMemory view over s isolated to scope. Nil
// receivers or scopes missing a positive APIKeyID, positive ProviderID, or
// non-empty Model return nil so conversion skips call-binding state cleanly,
// while active views share s's one-hour TTL and capacity budgets.
func (s *GeminiToolCalls) CallMemory(scope GeminiToolScope) convert.CallMemory {
	if s == nil || !scope.valid() {
		return nil
	}
	return &geminiCallMemory{
		store: s,
		scope: scope,
	}
}

// Remember records binding under clientCallID within m's bound GeminiToolScope.
// Nil receivers, empty clientCallID values, or bindings exceeding the 32 MiB
// string budget leave the store unchanged. Non-empty binding.Token values are
// copied by value, nil or empty-data tokens record an observed unsigned call,
// and late token updates with an empty UpstreamID preserve a live UpstreamID.
func (m *geminiCallMemory) Remember(clientCallID string, binding convert.CallBinding) {
	if m == nil {
		return
	}
	m.store.rememberBinding(m.scope, clientCallID, binding)
}

// Recall retrieves a live convert.CallBinding recorded for clientCallID within
// m's bound GeminiToolScope. Signed calls return a freshly allocated
// ir.ReplayToken preserving Scheme, ProviderID, and Data; observed unsigned
// calls return a nil Token with ok == true; and misses or expired entries return false.
func (m *geminiCallMemory) Recall(clientCallID string) (convert.CallBinding, bool) {
	if m == nil {
		return convert.CallBinding{}, false
	}
	entry, ok := m.store.lookupEntry(m.scope, clientCallID)
	if !ok {
		return convert.CallBinding{}, false
	}
	var tok *ir.ReplayToken
	if entry.hasToken {
		tok = &ir.ReplayToken{
			Scheme:     entry.scheme,
			ProviderID: entry.tokenProviderID,
			Data:       entry.signature,
		}
	}
	return convert.CallBinding{
		UpstreamID: entry.id,
		Token:      tok,
	}, true
}

// Remember stores the original Gemini call ID and thought signature under
// (scope, clientID) for direct string-keyed callers. Nil receivers, invalid
// scopes, empty clientID values, and entries exceeding the 32 MiB string
// budget leave the store unchanged.
//
// Recording evicts expired or oldest FIFO entries while holding s.mu so
// concurrent lookups never observe an entry after it leaves the index. Empty
// upstreamID and signature strings still record an observed unsigned call so
// Recall and Lookup distinguish known unsigned calls from cache misses.
func (s *GeminiToolCalls) Remember(scope GeminiToolScope, clientID, upstreamID, signature string) {
	if s == nil {
		return
	}
	size := len(scope.Model) + len(clientID) + len(upstreamID) + len(signature)
	if !scope.valid() || clientID == "" || size > geminiToolCallMaxBytes {
		return
	}
	key := geminiToolKey{scope: scope, clientID: clientID}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.currentTime()
	s.insertLocked(now, geminiToolEntry{
		key:       key,
		id:        upstreamID,
		signature: signature,
		hasToken:  signature != "",
		expires:   now.Add(geminiToolCallTTL),
		size:      size,
	})
}

// Lookup returns the original Gemini call ID and signature for (scope, clientID)
// while the entry remains unexpired. Nil receivers, invalid scopes, misses, and
// expired entries return empty strings and false after retiring any expired record.
func (s *GeminiToolCalls) Lookup(scope GeminiToolScope, clientID string) (string, string, bool) {
	entry, ok := s.lookupEntry(scope, clientID)
	if !ok {
		return "", "", false
	}
	return entry.id, entry.signature, true
}

// rememberBinding stores a convert.CallBinding under (scope, clientID) inside s's
// shared FIFO cache. Nil receivers, invalid scopes, empty clientID values, and
// entries larger than geminiToolCallMaxBytes leave existing cache state intact,
// while nil or empty-data tokens record hasToken == false for unsigned calls.
//
// When a subsequent Remember supplies a non-empty token with an empty UpstreamID
// for an unexpired entry under the same key, rememberBinding retains the live
// entry's non-empty UpstreamID. It rechecks the byte budget under s.mu before
// replacing the entry so late thoughtSignature frames never lose explicit call IDs.
func (s *GeminiToolCalls) rememberBinding(scope GeminiToolScope, clientID string, binding convert.CallBinding) {
	if s == nil || !scope.valid() || clientID == "" {
		return
	}
	var (
		scheme          string
		tokenProviderID int64
		signature       string
		hasToken        bool
	)
	if binding.Token != nil && binding.Token.Data != "" {
		scheme = binding.Token.Scheme
		tokenProviderID = binding.Token.ProviderID
		signature = binding.Token.Data
		hasToken = true
	}
	baseSize := len(scope.Model) + len(clientID) + len(signature) + len(scheme)
	if baseSize+len(binding.UpstreamID) > geminiToolCallMaxBytes {
		return
	}

	key := geminiToolKey{scope: scope, clientID: clientID}
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.currentTime()
	upstreamID := binding.UpstreamID
	if upstreamID == "" && hasToken {
		// Retain a live explicit UpstreamID when a late stream frame amends the
		// call's thoughtSignature without repeating the earlier upstream ID.
		if existing := s.entries[key]; existing != nil {
			if prev, ok := existing.Value.(geminiToolEntry); ok && now.Before(prev.expires) && prev.id != "" {
				upstreamID = prev.id
			}
		}
	}
	size := baseSize + len(upstreamID)
	if size > geminiToolCallMaxBytes {
		return
	}
	s.insertLocked(now, geminiToolEntry{
		key:             key,
		id:              upstreamID,
		scheme:          scheme,
		tokenProviderID: tokenProviderID,
		signature:       signature,
		hasToken:        hasToken,
		expires:         now.Add(geminiToolCallTTL),
		size:            size,
	})
}

// lookupEntry returns the unexpired geminiToolEntry stored at (scope, clientID).
// Nil receivers, invalid scopes, empty clientID values, misses, and expired
// entries return false, with expired records removed under s.mu while live
// entries remain available for repeated multi-turn replay.
func (s *GeminiToolCalls) lookupEntry(scope GeminiToolScope, clientID string) (geminiToolEntry, bool) {
	if s == nil || !scope.valid() || clientID == "" {
		return geminiToolEntry{}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	element := s.entries[geminiToolKey{scope: scope, clientID: clientID}]
	if element == nil {
		return geminiToolEntry{}, false
	}
	entry, _ := element.Value.(geminiToolEntry)
	if !s.currentTime().Before(entry.expires) {
		// Reclaim index slots and byte budget on read once the one-hour TTL
		// elapses so stale bindings do not block newer entries.
		s.remove(element)
		return geminiToolEntry{}, false
	}
	return entry, true
}

// insertLocked replaces any prior entry at entry.key, evicts expired or
// over-budget front elements from s.order, and appends entry at the tail.
// Callers must hold s.mu and ensure entry.size does not exceed geminiToolCallMaxBytes.
func (s *GeminiToolCalls) insertLocked(now time.Time, entry geminiToolEntry) {
	if s.entries == nil {
		s.entries = make(map[geminiToolKey]*list.Element)
	}
	// Remove an existing binding for the same key before checking capacity so
	// in-place signature updates do not double-count their previous string bytes.
	if existing := s.entries[entry.key]; existing != nil {
		s.remove(existing)
	}
	// Oldest entries expire first because every append uses the same one-hour TTL;
	// keep evicting front elements until count and byte limits fit the new entry.
	for oldest := s.order.Front(); oldest != nil; oldest = s.order.Front() {
		front, _ := oldest.Value.(geminiToolEntry)
		if now.Before(front.expires) && len(s.entries) < geminiToolCallMaxEntries && s.bytes+entry.size <= geminiToolCallMaxBytes {
			break
		}
		s.remove(oldest)
	}
	s.entries[entry.key] = s.order.PushBack(entry)
	s.bytes += entry.size
}

// remove deletes element's key from s.entries, unlinks element from s.order, and
// subtracts its accounted string bytes from s.bytes. Callers must hold s.mu and
// pass an element currently belonging to s.order.
func (s *GeminiToolCalls) remove(element *list.Element) {
	entry, _ := element.Value.(geminiToolEntry)
	delete(s.entries, entry.key)
	s.order.Remove(element)
	s.bytes -= entry.size
}

// currentTime returns the wall-clock instant from s.now when configured for
// deterministic tests, or time.Now when s.now is nil. Callers must hold s.mu
// before reading s.now.
func (s *GeminiToolCalls) currentTime() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

// valid reports whether scope specifies positive APIKeyID and ProviderID values
// alongside a non-empty mapped upstream Model string. Scopes missing any
// dimension cannot isolate bindings safely and disable storage and lookup.
func (scope GeminiToolScope) valid() bool {
	return scope.APIKeyID > 0 && scope.ProviderID > 0 && scope.Model != ""
}
