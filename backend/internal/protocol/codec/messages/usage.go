package messages

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
)

// usage.go validates, reconciles, and serializes Anthropic Messages token usage
// counters across buffered responses and sparse streaming events. Anthropic
// Messages reports fresh uncached input_tokens separately from
// cache_read_input_tokens and cache_creation_input_tokens, matching IR Usage's
// disjoint bucket layout directly while also carrying optional 5m/1h
// cache_creation TTL breakdowns and speed labels.
//
// Incoming envelopes and usage objects first pass wirejson.ValidateUsageEnvelope
// and wirejson.ValidateUsageObject so duplicate or case-ambiguous counters are
// rejected before accounting. Validated observations then merge through
// anthropic.MergeAnthropicUsage so compatible provider prompt_tokens and
// hit/miss aliases resolve identically to gateway billing while sparse
// message_delta events preserve prior prompt, cache, TTL, and speed fields.

// wireUsage decodes canonical Anthropic Messages usage counters alongside
// compatible prompt-total and cache hit/miss aliases.
type wireUsage struct {
	InputTokens              *int64                   `json:"input_tokens,omitzero"`                // uncached input tokens
	OutputTokens             *int64                   `json:"output_tokens,omitzero"`               // generated output tokens
	CacheCreationInputTokens *int64                   `json:"cache_creation_input_tokens,omitzero"` // aggregate cache-write tokens
	CacheReadInputTokens     *int64                   `json:"cache_read_input_tokens,omitzero"`     // cache-read prompt tokens
	CacheCreation            *wireCacheCreation       `json:"cache_creation,omitzero"`              // optional 5m/1h TTL breakdown
	Speed                    string                   `json:"speed,omitzero"`                       // processing mode such as "fast"
	PromptTokens             *int64                   `json:"prompt_tokens,omitzero"`               // compatible inclusive prompt total
	CachedTokens             *int64                   `json:"cached_tokens,omitzero"`               // compatible top-level cache reads
	PromptTokensDetails      *wirePromptTokensDetails `json:"prompt_tokens_details,omitzero"`       // compatible nested prompt details
	PromptCacheHitTokens     *int64                   `json:"prompt_cache_hit_tokens,omitzero"`     // compatible cache-hit tokens
	PromptCacheMissTokens    *int64                   `json:"prompt_cache_miss_tokens,omitzero"`    // compatible uncached-miss tokens
}

// wireCacheCreation decodes optional 5m and 1h cache-write token counters inside usage.cache_creation.
type wireCacheCreation struct {
	Ephemeral5mInputTokens *int64 `json:"ephemeral_5m_input_tokens,omitzero"` // five-minute cache writes
	Ephemeral1hInputTokens *int64 `json:"ephemeral_1h_input_tokens,omitzero"` // one-hour cache writes
}

// wirePromptTokensDetails decodes nested cached_tokens reported by compatible providers.
type wirePromptTokensDetails struct {
	CachedTokens *int64 `json:"cached_tokens,omitzero"` // cached prompt tokens
}

// wireUsageWrite serializes canonical disjoint Anthropic Messages usage counters.
type wireUsageWrite struct {
	InputTokens              int64                   `json:"input_tokens"`                // fresh uncached prompt tokens
	CacheCreationInputTokens int64                   `json:"cache_creation_input_tokens"` // aggregate cache-write tokens
	CacheReadInputTokens     int64                   `json:"cache_read_input_tokens"`     // cache-read prompt tokens
	CacheCreation            *wireCacheCreationWrite `json:"cache_creation,omitzero"`     // optional 5m/1h TTL breakdown
	OutputTokens             int64                   `json:"output_tokens"`               // generated output tokens
	Speed                    string                  `json:"speed,omitzero"`              // optional processing speed mode
}

// wireCacheCreationWrite serializes optional 5m and 1h cache-write TTL buckets.
type wireCacheCreationWrite struct {
	Ephemeral5mInputTokens *int64 `json:"ephemeral_5m_input_tokens,omitzero"` // five-minute TTL cache writes
	Ephemeral1hInputTokens *int64 `json:"ephemeral_1h_input_tokens,omitzero"` // one-hour TTL cache writes
}

// streamUsageTracker merges initial message_start and sparse message_delta
// usage observations into cumulative disjoint IR Usage snapshots.
type streamUsageTracker struct {
	tokens protocol.TokenUsage // cumulative disjoint billing counters
	has5m  bool                // whether ephemeral_5m_input_tokens was observed
	has1h  bool                // whether ephemeral_1h_input_tokens was observed
	val5m  int64               // latest five-minute cache-write token count
	val1h  int64               // latest one-hour cache-write token count
}

// decodeMessagesUsage validates envelopeBytes and rawUsage with wirejson and
// converts rawUsage into a disjoint ir.Usage snapshot. Absent or null rawUsage
// returns (nil, nil), whereas negative counters or ambiguous keys return an error.
func decodeMessagesUsage(envelopeBytes []byte, rawUsage jsontext.Value) (*ir.Usage, error) {
	trimmed := bytes.TrimSpace(rawUsage)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	var tracker streamUsageTracker
	u, ok, err := tracker.observe(envelopeBytes, trimmed, true)
	if err != nil || !ok {
		return nil, err
	}
	return u, nil
}

// observe validates frameBytes and rawUsage via wirejson, merges the decoded
// usage observation into t, and returns a detached cumulative ir.Usage snapshot.
// Absent or null rawUsage inputs return (nil, false, nil), whereas duplicate
// usage keys or negative token counts return an error.
//
// Initial observations (isStart=true) initialize both 5m and 1h TTL buckets
// together when either field is supplied. Sparse message_delta updates
// (isStart=false) update positive counters through anthropic.MergeAnthropicUsage
// and inherit prior cache buckets before splitting compatible prompt_tokens totals
// so gateway NormalizeStreamUsage and IR accounting stay identical.
func (t *streamUsageTracker) observe(frameBytes []byte, rawUsage jsontext.Value, isStart bool) (*ir.Usage, bool, error) {
	trimmed := bytes.TrimSpace(rawUsage)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, false, nil
	}
	if err := wirejson.ValidateUsageEnvelope(frameBytes); err != nil {
		return nil, false, err
	}
	if err := wirejson.ValidateUsageObject(trimmed); err != nil {
		return nil, false, err
	}

	var parsed wireUsage
	if err := unmarshalJSONObject(trimmed, &parsed); err != nil {
		return nil, false, err
	}
	if err := validateNonNegativeWireUsage(parsed); err != nil {
		return nil, false, err
	}

	anthUsage := toAnthropicUsage(parsed)
	// Inherit established cache buckets before splitting compatible prompt_tokens
	// on sparse message_delta frames that omit cache details, matching
	// anthropic.NormalizeAnthropicCompatiblePromptUsage without double-counting.
	if !isStart && (anthUsage.PromptTokens > 0 || anthUsage.PromptCacheHitTokens != nil || anthUsage.PromptCacheMissTokens != nil) {
		if parsed.CacheReadInputTokens == nil &&
			parsed.CachedTokens == nil &&
			(parsed.PromptTokensDetails == nil || parsed.PromptTokensDetails.CachedTokens == nil) &&
			parsed.PromptCacheHitTokens == nil {
			anthUsage.CacheReadInputTokens = t.tokens.CacheReadInputTokens
		}
		if parsed.CacheCreationInputTokens == nil &&
			(parsed.CacheCreation == nil ||
				(parsed.CacheCreation.Ephemeral5mInputTokens == nil && parsed.CacheCreation.Ephemeral1hInputTokens == nil)) {
			anthUsage.CacheCreationInputTokens = t.tokens.CacheCreationInputTokens
		}
	}
	anthropic.MergeAnthropicUsage(&t.tokens, anthUsage)

	if parsed.CacheCreation != nil {
		if isStart || (!t.has5m && !t.has1h) {
			if parsed.CacheCreation.Ephemeral5mInputTokens != nil || parsed.CacheCreation.Ephemeral1hInputTokens != nil {
				t.has5m = true
				t.has1h = true
				t.val5m = derefInt64(parsed.CacheCreation.Ephemeral5mInputTokens)
				t.val1h = derefInt64(parsed.CacheCreation.Ephemeral1hInputTokens)
			}
		} else {
			if parsed.CacheCreation.Ephemeral5mInputTokens != nil {
				t.has5m = true
				t.val5m = *parsed.CacheCreation.Ephemeral5mInputTokens
			}
			if parsed.CacheCreation.Ephemeral1hInputTokens != nil {
				t.has1h = true
				t.val1h = *parsed.CacheCreation.Ephemeral1hInputTokens
			}
		}
	}

	out := &ir.Usage{
		Input:      max(int64(t.tokens.InputTokens), 0),
		CacheRead:  max(int64(t.tokens.CacheReadInputTokens), 0),
		CacheWrite: max(int64(t.tokens.CacheCreationInputTokens), 0),
		Output:     max(int64(t.tokens.OutputTokens), 0),
		Speed:      strings.TrimSpace(t.tokens.Speed),
	}
	if t.has5m {
		v5 := t.val5m
		out.CacheWrite5m = &v5
	}
	if t.has1h {
		v1 := t.val1h
		out.CacheWrite1h = &v1
	}
	return out, true, nil
}

// validateNonNegativeWireUsage checks every canonical and compatible token
// counter in w before int conversion or ir.ValidateUsage checks run. Any
// negative token count returns a descriptive validation error immediately.
func validateNonNegativeWireUsage(w wireUsage) error {
	for _, ptr := range []*int64{
		w.InputTokens,
		w.OutputTokens,
		w.CacheCreationInputTokens,
		w.CacheReadInputTokens,
		w.PromptTokens,
		w.CachedTokens,
		w.PromptCacheHitTokens,
		w.PromptCacheMissTokens,
	} {
		if ptr != nil && *ptr < 0 {
			return errors.New("usage token counters must be nonnegative")
		}
	}
	if w.CacheCreation != nil {
		if (w.CacheCreation.Ephemeral5mInputTokens != nil && *w.CacheCreation.Ephemeral5mInputTokens < 0) ||
			(w.CacheCreation.Ephemeral1hInputTokens != nil && *w.CacheCreation.Ephemeral1hInputTokens < 0) {
			return errors.New("usage.cache_creation counters must be nonnegative")
		}
	}
	if w.PromptTokensDetails != nil && w.PromptTokensDetails.CachedTokens != nil && *w.PromptTokensDetails.CachedTokens < 0 {
		return errors.New("usage.prompt_tokens_details.cached_tokens must be nonnegative")
	}
	return nil
}

// toAnthropicUsage converts a validated wireUsage struct into anthropic.AnthropicUsage
// for shared prompt/cache alias normalization via anthropic.MergeAnthropicUsage.
func toAnthropicUsage(w wireUsage) anthropic.AnthropicUsage {
	out := anthropic.AnthropicUsage{
		InputTokens:              int(derefInt64(w.InputTokens)),
		OutputTokens:             int(derefInt64(w.OutputTokens)),
		CacheCreationInputTokens: int(derefInt64(w.CacheCreationInputTokens)),
		CacheReadInputTokens:     int(derefInt64(w.CacheReadInputTokens)),
		Speed:                    strings.TrimSpace(w.Speed),
		PromptTokens:             int(derefInt64(w.PromptTokens)),
		CachedTokens:             int(derefInt64(w.CachedTokens)),
	}
	if w.CacheCreation != nil {
		cc := &anthropic.AnthropicCacheCreation{}
		if w.CacheCreation.Ephemeral5mInputTokens != nil {
			v5 := int(*w.CacheCreation.Ephemeral5mInputTokens)
			cc.Ephemeral5mInputTokens = &v5
		}
		if w.CacheCreation.Ephemeral1hInputTokens != nil {
			v1 := int(*w.CacheCreation.Ephemeral1hInputTokens)
			cc.Ephemeral1hInputTokens = &v1
		}
		out.CacheCreation = cc
	}
	if w.PromptTokensDetails != nil && w.PromptTokensDetails.CachedTokens != nil {
		out.PromptTokensDetails = &anthropic.AnthropicPromptTokensDetails{
			CachedTokens: int(*w.PromptTokensDetails.CachedTokens),
		}
	}
	if w.PromptCacheHitTokens != nil {
		hit := int(*w.PromptCacheHitTokens)
		out.PromptCacheHitTokens = &hit
	}
	if w.PromptCacheMissTokens != nil {
		miss := int(*w.PromptCacheMissTokens)
		out.PromptCacheMissTokens = &miss
	}
	return out
}

// derefInt64 dereferences an optional int64 token counter pointer from a
// decoded wire usage struct. Nil pointers return zero so omitted JSON fields
// contribute zero tokens without requiring caller nil checks.
func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// encodeMessagesUsage projects disjoint IR Usage counters into canonical
// Anthropic Messages usage fields without adding CacheRead or CacheWrite onto
// input_tokens. Nil Usage pointers serialize as all-zero counters.
func encodeMessagesUsage(u *ir.Usage) wireUsageWrite {
	if u == nil {
		return wireUsageWrite{}
	}
	out := wireUsageWrite{
		InputTokens:              max(u.Input, 0),
		CacheCreationInputTokens: u.EffectiveCacheWrite(),
		CacheReadInputTokens:     max(u.CacheRead, 0),
		OutputTokens:             max(u.Output, 0),
		Speed:                    u.Speed,
	}
	if u.CacheWrite5m != nil || u.CacheWrite1h != nil {
		v5 := max(derefInt64(u.CacheWrite5m), 0)
		v1 := max(derefInt64(u.CacheWrite1h), 0)
		out.CacheCreation = &wireCacheCreationWrite{
			Ephemeral5mInputTokens: &v5,
			Ephemeral1hInputTokens: &v1,
		}
	}
	return out
}
