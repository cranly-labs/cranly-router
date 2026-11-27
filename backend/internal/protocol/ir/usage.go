package ir

import (
	"math"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// usage.go defines disjoint prompt, cache, and completion token buckets on IR
// responses and stream events alongside conversions to and from protocol.TokenUsage.
// Anthropic Messages reports uncached input separately from cache reads and
// cache writes, whereas OpenAI Responses and Chat Completions include all three
// categories inside input_tokens and prompt_tokens.
//
// Storing fresh input, cache reads, and cache writes in non-overlapping fields
// lets each client encoder derive inclusive or exclusive wire totals once
// without double-counting cached tokens across multi-hop routes. Optional TTL
// pointer fields distinguish an unobserved breakdown from an explicit zero
// bucket while keeping aggregate cache writes authoritative when present.

// Usage stores disjoint prompt and completion token buckets. Messages input_tokens
// equals Input directly, whereas Chat prompt_tokens and Responses input_tokens
// equal Input plus CacheRead plus CacheWrite.
type Usage struct {
	Input      int64 `json:"input"`       // fresh uncached prompt tokens
	CacheRead  int64 `json:"cache_read"`  // prompt tokens served from cache
	CacheWrite int64 `json:"cache_write"` // aggregate prompt tokens written to cache

	// CacheWrite5m, CacheWrite30m, CacheWrite1h, and CacheWrite12h record
	// optional per-TTL breakdowns beneath CacheWrite; nil means the upstream did
	// not report that TTL bucket.
	CacheWrite5m  *int64 `json:"cache_write_5m,omitzero"`  // five-minute TTL cache writes
	CacheWrite30m *int64 `json:"cache_write_30m,omitzero"` // thirty-minute TTL cache writes
	CacheWrite1h  *int64 `json:"cache_write_1h,omitzero"`  // one-hour TTL cache writes
	CacheWrite12h *int64 `json:"cache_write_12h,omitzero"` // twelve-hour TTL cache writes

	Output             int64  `json:"output"`                       // total generated tokens including reasoning
	Reasoning          int64  `json:"reasoning,omitzero"`           // reasoning tokens included within Output
	ImageInput         int64  `json:"image_input,omitzero"`         // prompt image tokens
	ImageOutput        int64  `json:"image_output,omitzero"`        // generated image tokens
	AudioInput         int64  `json:"audio_input,omitzero"`         // prompt audio tokens
	AudioOutput        int64  `json:"audio_output,omitzero"`        // generated audio tokens
	AcceptedPrediction int64  `json:"accepted_prediction,omitzero"` // accepted predicted output tokens
	RejectedPrediction int64  `json:"rejected_prediction,omitzero"` // rejected predicted output tokens
	Speed              string `json:"speed,omitzero"`               // Anthropic usage.speed mode such as "fast"
}

// UsageFromTokenUsage converts disjoint protocol.TokenUsage counters into an IR
// Usage value. Negative counts clamp to zero, positive 5m or 1h breakdowns
// populate their respective TTL pointers, and missing aggregate cache writes
// backfill from positive TTL detail sums.
func UsageFromTokenUsage(src protocol.TokenUsage) Usage {
	five := max(int64(src.CacheCreation5mTokens), 0)
	hour := max(int64(src.CacheCreation1hTokens), 0)
	cacheWrite := max(int64(src.CacheCreationInputTokens), 0)
	if cacheWrite == 0 && (five > 0 || hour > 0) {
		// Detail-only observations omit the outer aggregate; backfill from the
		// nonnegative TTL breakdown so inclusive prompt totals stay complete.
		cacheWrite = five + hour
	}
	out := Usage{
		Input:       max(int64(src.InputTokens), 0),
		CacheRead:   max(int64(src.CacheReadInputTokens), 0),
		CacheWrite:  cacheWrite,
		Output:      max(int64(src.OutputTokens), 0),
		ImageOutput: max(int64(src.ImageOutputTokens), 0),
		Speed:       src.Speed,
	}
	if five > 0 || hour > 0 {
		out.CacheWrite5m = &five
		out.CacheWrite1h = &hour
	}
	return out
}

// Clone returns an independent deep copy of u, including its per-TTL cache-write
// pointer fields. Nil receivers return nil, and mutating returned TTL pointers
// leaves the original Usage snapshot unchanged.
func (u *Usage) Clone() *Usage {
	if u == nil {
		return nil
	}
	out := *u
	out.CacheWrite5m = cloneInt64Ptr(u.CacheWrite5m)
	out.CacheWrite30m = cloneInt64Ptr(u.CacheWrite30m)
	out.CacheWrite1h = cloneInt64Ptr(u.CacheWrite1h)
	out.CacheWrite12h = cloneInt64Ptr(u.CacheWrite12h)
	return &out
}

// ToTokenUsage projects u into gateway billing protocol.TokenUsage counters.
// Nil receivers return a zero TokenUsage value, and negative or out-of-range
// int64 counts clamp into nonnegative int bounds.
func (u *Usage) ToTokenUsage() protocol.TokenUsage {
	if u == nil {
		return protocol.TokenUsage{}
	}
	out := protocol.TokenUsage{
		InputTokens:              clampUsageInt(u.Input),
		OutputTokens:             clampUsageInt(u.Output),
		CacheCreationInputTokens: clampUsageInt(u.EffectiveCacheWrite()),
		CacheReadInputTokens:     clampUsageInt(u.CacheRead),
		ImageOutputTokens:        clampUsageInt(u.ImageOutput),
		Speed:                    u.Speed,
	}
	if u.CacheWrite5m != nil {
		out.CacheCreation5mTokens = clampUsageInt(*u.CacheWrite5m)
	}
	if u.CacheWrite1h != nil {
		out.CacheCreation1hTokens = clampUsageInt(*u.CacheWrite1h)
	}
	return out
}

// EffectiveCacheWrite returns CacheWrite when positive, or the sum of positive
// per-TTL cache-write buckets when CacheWrite is zero. Nil receivers and
// nonpositive buckets without positive details return zero.
func (u *Usage) EffectiveCacheWrite() int64 {
	if u == nil {
		return 0
	}
	if u.CacheWrite > 0 {
		return u.CacheWrite
	}
	var sum int64
	for _, bucket := range []*int64{u.CacheWrite5m, u.CacheWrite30m, u.CacheWrite1h, u.CacheWrite12h} {
		if bucket != nil && *bucket > 0 {
			sum += *bucket
		}
	}
	return sum
}

// InclusiveInputTokens returns Input plus CacheRead plus EffectiveCacheWrite,
// matching the inclusive prompt total reported by OpenAI Responses and Chat
// Completions. Nil receivers return zero, and negative buckets clamp to zero.
func (u *Usage) InclusiveInputTokens() int64 {
	if u == nil {
		return 0
	}
	return max(u.Input, 0) + max(u.CacheRead, 0) + u.EffectiveCacheWrite()
}

// TotalTokens returns InclusiveInputTokens plus nonnegative Output for OpenAI
// Responses and Chat Completions wire totals. Nil receivers return zero, and
// negative Output counts clamp to zero before addition.
func (u *Usage) TotalTokens() int64 {
	if u == nil {
		return 0
	}
	return u.InclusiveInputTokens() + max(u.Output, 0)
}

// HasObservedTokens reports whether any prompt, cache, output, reasoning, or
// modality token counter on u is positive. Nil receivers, all-zero snapshots,
// and speed-only records return false without inventing billable work.
func (u *Usage) HasObservedTokens() bool {
	if u == nil {
		return false
	}
	return u.Input > 0 || u.CacheRead > 0 || u.EffectiveCacheWrite() > 0 ||
		u.Output > 0 || u.Reasoning > 0 ||
		u.ImageInput > 0 || u.ImageOutput > 0 ||
		u.AudioInput > 0 || u.AudioOutput > 0 ||
		u.AcceptedPrediction > 0 || u.RejectedPrediction > 0
}

// clampUsageInt bounds v into [0, math.MaxInt] when projecting int64 IR usage
// buckets onto int fields of protocol.TokenUsage. Nonpositive values return
// zero and values above math.MaxInt saturate at math.MaxInt.
func clampUsageInt(v int64) int {
	if v <= 0 {
		return 0
	}
	// Saturate int64 counts above platform math.MaxInt so conversion on 32-bit
	// builds never wraps large token totals into negative integers.
	if v > int64(math.MaxInt) {
		return math.MaxInt
	}
	return int(v)
}
