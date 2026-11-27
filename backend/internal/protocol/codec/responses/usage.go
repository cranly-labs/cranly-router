package responses

import (
	"bytes"
	"encoding/json/jsontext"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
)

// usage.go validates, accumulates, and serializes OpenAI Responses token usage
// objects across buffered JSON replies and streaming SSE lifecycle envelopes.
// OpenAI Responses reports inclusive input_tokens covering fresh prompt input,
// cache reads, and cache writes together, whereas IR Usage separates those three
// prompt categories into disjoint buckets so cross-protocol conversions add each
// category at most once.
//
// Incoming envelopes pass wirejson.ValidateUsageEnvelope and ValidateUsageObject
// before openai.UsageAccumulator extracts counters, preserving zero divergence
// from gateway billing accounting. Accumulation handles outer-zero cache-write
// backfill, 30m/12h and compatible 5m/1h TTL breakdowns, paired hosted
// tool_usage.image_gen counters, and sparse stream merges, while outbound
// encoding always populates input_tokens_details.cached_tokens and
// output_tokens_details.reasoning_tokens for strict Codex deserializers.

// wireUsageWrite serializes an inclusive OpenAI Responses usage object.
type wireUsageWrite struct {
	InputTokens              int64                        `json:"input_tokens"`                         // inclusive prompt token total
	OutputTokens             int64                        `json:"output_tokens"`                        // inclusive generated token total
	TotalTokens              int64                        `json:"total_tokens"`                         // input_tokens plus output_tokens
	CacheCreationInputTokens int64                        `json:"cache_creation_input_tokens,omitzero"` // positive aggregate cache-write alias
	InputTokensDetails       wireInputTokensDetailsWrite  `json:"input_tokens_details"`                 // mandatory cached_tokens detail wrapper
	OutputTokensDetails      wireOutputTokensDetailsWrite `json:"output_tokens_details"`                // mandatory reasoning_tokens detail wrapper
}

// wireInputTokensDetailsWrite serializes prompt cache, TTL write, audio, and image details inside input_tokens_details.
type wireInputTokensDetailsWrite struct {
	CachedTokens      int64                             `json:"cached_tokens"`                       // prompt tokens served from cache
	CacheWriteTokens  int64                             `json:"cache_write_tokens,omitzero"`         // aggregate prompt tokens written to cache
	CacheWriteDetails *wireCacheWriteTokensDetailsWrite `json:"cache_write_tokens_details,omitzero"` // optional 30m/12h split matching total
	AudioTokens       int64                             `json:"audio_tokens,omitzero"`               // prompt audio input tokens
	ImageTokens       int64                             `json:"image_tokens,omitzero"`               // prompt image input tokens
}

// wireCacheWriteTokensDetailsWrite serializes OpenAI 30m and 12h cache-write lifetime buckets.
type wireCacheWriteTokensDetailsWrite struct {
	TTL30mTokens int64 `json:"ttl_30m_tokens"` // thirty-minute cache-write token count
	TTL12hTokens int64 `json:"ttl_12h_tokens"` // twelve-hour cache-write token count
}

// wireOutputTokensDetailsWrite serializes reasoning, audio, image, and prediction details inside output_tokens_details.
type wireOutputTokensDetailsWrite struct {
	ReasoningTokens          int64 `json:"reasoning_tokens"`                    // reasoning tokens included within output_tokens
	AudioTokens              int64 `json:"audio_tokens,omitzero"`               // generated audio output tokens
	ImageTokens              int64 `json:"image_tokens,omitzero"`               // generated image output tokens
	AcceptedPredictionTokens int64 `json:"accepted_prediction_tokens,omitzero"` // accepted predicted output tokens
	RejectedPredictionTokens int64 `json:"rejected_prediction_tokens,omitzero"` // rejected predicted output tokens
}

// wireAnthropicCacheCreationFields decodes 5m and 1h cache_creation token counts on compatible usage objects.
type wireAnthropicCacheCreationFields struct {
	Ephemeral5m *int64 `json:"ephemeral_5m_input_tokens,omitzero"` // optional five-minute cache writes
	Ephemeral1h *int64 `json:"ephemeral_1h_input_tokens,omitzero"` // optional one-hour cache writes
}

// wireAnthropicCacheCreation decodes compatible 5m/1h cache_creation fields on upstream usage objects.
type wireAnthropicCacheCreation struct {
	CacheCreation *wireAnthropicCacheCreationFields `json:"cache_creation,omitzero"` // compatible Anthropic-style cache TTL breakdown
}

// wireNestedResponseUsage locates a nested response.usage JSON slice inside lifecycle events.
type wireNestedResponseUsage struct {
	Usage jsontext.Value `json:"usage,omitzero"` // nested response.usage object
}

// wireEnvelopeUsageProbe locates raw top-level and nested usage slices for 5m/1h compatibility extraction.
type wireEnvelopeUsageProbe struct {
	Usage    jsontext.Value           `json:"usage,omitzero"`    // top-level usage object
	Response *wireNestedResponseUsage `json:"response,omitzero"` // nested lifecycle response wrapper
}

// decodeResponsesUsage validates envelopeBytes and rawUsage with wirejson, runs
// ObserveResponsesEvent on a fresh openai.UsageAccumulator so paired
// tool_usage.image_gen counters merge identically to billing, and returns
// disjoint IR Usage buckets. Absent or null rawUsage returns (nil, nil), whereas
// duplicate or case-ambiguous usage fields return a validation error.
func decodeResponsesUsage(envelopeBytes []byte, rawUsage jsontext.Value) (*ir.Usage, error) {
	trimmed := bytes.TrimSpace(rawUsage)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if err := wirejson.ValidateUsageEnvelope(envelopeBytes); err != nil {
		return nil, err
	}
	if err := wirejson.ValidateUsageObject(trimmed); err != nil {
		return nil, err
	}
	var acc openai.UsageAccumulator
	snap := acc.ObserveResponsesEvent(envelopeBytes, nil)
	if snap == nil {
		snap = acc.Observe(trimmed)
	}
	if snap == nil {
		return nil, nil
	}
	return projectAccumulatorUsage(&acc, snap, trimmed), nil
}

// observeResponsesStreamUsage validates an SSE event's raw JSON envelope and
// merges any top-level or nested response.usage object into acc alongside paired
// tool_usage.image_gen details. When neither location carries a usage object,
// observeResponsesStreamUsage returns (nil, false, nil) unless forceSnapshot
// requests the previously accumulated snapshot on a terminal event.
func observeResponsesStreamUsage(
	acc *openai.UsageAccumulator,
	prev *ir.Usage,
	rawEventBytes []byte,
	repairedBytes []byte,
	forceSnapshot bool,
) (*ir.Usage, bool, error) {
	var probe wireEnvelopeUsageProbe
	// Repaired lifecycle frames may wrap bare responses into {"response": ...};
	// ignoring probe unmarshal failures lets callers validate malformed JSON earlier.
	_ = unmarshalJSONValue(repairedBytes, &probe)

	topTrim := bytes.TrimSpace(probe.Usage)
	var nestedTrim []byte
	if probe.Response != nil {
		nestedTrim = bytes.TrimSpace(probe.Response.Usage)
	}
	hasTop := len(topTrim) > 0 && !bytes.Equal(topTrim, []byte("null"))
	hasNested := len(nestedTrim) > 0 && !bytes.Equal(nestedTrim, []byte("null"))

	if !hasTop && !hasNested {
		if forceSnapshot {
			if snap := acc.Snapshot(); snap != nil {
				cur := projectAccumulatorUsage(acc, snap, nil)
				mergeMonotonicUsage(prev, cur)
				return cur, true, nil
			}
		}
		return nil, false, nil
	}

	if err := wirejson.ValidateUsageEnvelope(rawEventBytes); err != nil {
		return nil, false, err
	}
	if hasTop {
		if err := wirejson.ValidateUsageObject(topTrim); err != nil {
			return nil, false, err
		}
	}
	if hasNested {
		if err := wirejson.ValidateUsageObject(nestedTrim); err != nil {
			return nil, false, err
		}
	}

	snap := acc.ObserveResponsesEvent(repairedBytes, nil)
	if snap == nil {
		return nil, false, nil
	}
	compatRaw := topTrim
	if hasNested {
		compatRaw = nestedTrim
	}
	cur := projectAccumulatorUsage(acc, snap, compatRaw)
	mergeMonotonicUsage(prev, cur)
	return cur, true, nil
}

// projectAccumulatorUsage converts an accumulated openai.ResponsesUsage
// snapshot and ForwardUsage accounting view into disjoint nonnegative ir.Usage
// counters.
//
// OpenAI Responses includes cached reads and cache writes inside input_tokens,
// so projectAccumulatorUsage subtracts both cached buckets once and clamps at
// zero to guard against inconsistent compatible totals. Raw 30m/12h splits from
// ForwardUsage (including outer-zero splits backfilled by EffectiveCacheWrite)
// and compatible 5m/1h buckets populate their respective TTL pointers.
func projectAccumulatorUsage(acc *openai.UsageAccumulator, snap *openai.ResponsesUsage, rawUsage []byte) *ir.Usage {
	fwd := acc.ForwardUsage()
	inclusiveInput := max(int64(snap.InputTokens), 0)
	var cacheRead int64
	var audioInput int64
	if snap.InputTokensDetails != nil {
		cacheRead = max(int64(snap.InputTokensDetails.CachedTokens), 0)
		audioInput = max(int64(snap.InputTokensDetails.AudioTokens), 0)
	}
	cacheWrite := max(int64(snap.CacheCreationInputTokens), 0)

	// Subtract cached read and write buckets once from inclusive input_tokens so
	// Usage.Input holds only fresh uncached prompt tokens.
	freshInput := max(inclusiveInput-cacheRead-cacheWrite, 0)

	out := &ir.Usage{
		Input:       freshInput,
		CacheRead:   cacheRead,
		CacheWrite:  cacheWrite,
		Output:      max(int64(snap.OutputTokens), 0),
		ImageInput:  max(int64(fwd.ImageInputTokens), 0),
		ImageOutput: max(int64(fwd.ImageOutputTokens), 0),
		AudioInput:  audioInput,
	}
	if snap.OutputTokensDetails != nil {
		out.Reasoning = max(int64(snap.OutputTokensDetails.ReasoningTokens), 0)
		out.AudioOutput = max(int64(snap.OutputTokensDetails.AudioTokens), 0)
		out.AcceptedPrediction = max(int64(snap.OutputTokensDetails.AcceptedPredictionTokens), 0)
		out.RejectedPrediction = max(int64(snap.OutputTokensDetails.RejectedPredictionTokens), 0)
	}
	if fwd.CacheWriteDetails != nil {
		t30 := max(int64(fwd.CacheWriteDetails.Tokens30m), 0)
		t12 := max(int64(fwd.CacheWriteDetails.Tokens12h), 0)
		if t30 > 0 || t12 > 0 {
			out.CacheWrite30m = &t30
			out.CacheWrite12h = &t12
		}
	}
	if len(rawUsage) > 0 {
		var compatTTL wireAnthropicCacheCreation
		// Compatible providers may send non-object cache_creation fields; ignoring
		// unmarshal errors preserves validated primary token totals.
		if unmarshalJSONValue(rawUsage, &compatTTL) == nil && compatTTL.CacheCreation != nil {
			if compatTTL.CacheCreation.Ephemeral5m != nil && *compatTTL.CacheCreation.Ephemeral5m >= 0 {
				v := *compatTTL.CacheCreation.Ephemeral5m
				out.CacheWrite5m = &v
			}
			if compatTTL.CacheCreation.Ephemeral1h != nil && *compatTTL.CacheCreation.Ephemeral1h >= 0 {
				v := *compatTTL.CacheCreation.Ephemeral1h
				out.CacheWrite1h = &v
			}
		}
	}
	return out
}

// mergeMonotonicUsage reconciles cur against prev across cumulative Responses
// stream usage observations so sparse output-only tail events satisfy
// ir.ValidateStream monotonicity while explicit all-zero resets still clear state.
//
// Compatible providers occasionally emit prompt and cache counters on an early
// lifecycle event and send only output_tokens on response.completed. When cur
// still reports observed tokens without reclassifying prompt counts into larger
// cache buckets, mergeMonotonicUsage retains prior CacheRead, CacheWrite, and
// Input values so cumulative snapshots never regress.
func mergeMonotonicUsage(prev, cur *ir.Usage) {
	if prev == nil || cur == nil || !cur.HasObservedTokens() {
		return
	}
	if prev.CacheRead > 0 && cur.CacheRead == 0 {
		cur.CacheRead = prev.CacheRead
	}
	if prev.EffectiveCacheWrite() > 0 && cur.EffectiveCacheWrite() == 0 {
		cur.CacheWrite = prev.CacheWrite
		cloned := prev.Clone()
		cur.CacheWrite5m = cloned.CacheWrite5m
		cur.CacheWrite30m = cloned.CacheWrite30m
		cur.CacheWrite1h = cloned.CacheWrite1h
		cur.CacheWrite12h = cloned.CacheWrite12h
	}
	if prev.Input > 0 && cur.Input == 0 &&
		cur.CacheRead <= prev.CacheRead &&
		cur.EffectiveCacheWrite() <= prev.EffectiveCacheWrite() {
		cur.Input = prev.Input
	}
}

// encodeResponsesUsage projects disjoint IR Usage counters into an inclusive
// OpenAI Responses wire usage object. Nil inputs return nil, while non-nil
// Usage values sum Input, CacheRead, and EffectiveCacheWrite once into
// input_tokens, always emit cached_tokens and reasoning_tokens detail subobjects,
// and emit cache_write_tokens_details only when 30m + 12h equals EffectiveCacheWrite.
func encodeResponsesUsage(u *ir.Usage) *wireUsageWrite {
	if u == nil {
		return nil
	}
	inputTokens := u.InclusiveInputTokens()
	outputTokens := max(u.Output, 0)
	effectiveWrite := u.EffectiveCacheWrite()

	inDetails := wireInputTokensDetailsWrite{
		CachedTokens:     max(u.CacheRead, 0),
		CacheWriteTokens: effectiveWrite,
		AudioTokens:      max(u.AudioInput, 0),
		ImageTokens:      max(u.ImageInput, 0),
	}
	if effectiveWrite > 0 && (u.CacheWrite30m != nil || u.CacheWrite12h != nil) {
		var t30, t12 int64
		validSplit := true
		if u.CacheWrite30m != nil {
			if *u.CacheWrite30m < 0 {
				validSplit = false
			}
			t30 = *u.CacheWrite30m
		}
		if u.CacheWrite12h != nil {
			if *u.CacheWrite12h < 0 {
				validSplit = false
			}
			t12 = *u.CacheWrite12h
		}
		if validSplit && t30+t12 == effectiveWrite {
			inDetails.CacheWriteDetails = &wireCacheWriteTokensDetailsWrite{
				TTL30mTokens: t30,
				TTL12hTokens: t12,
			}
		}
	}

	outDetails := wireOutputTokensDetailsWrite{
		ReasoningTokens:          max(u.Reasoning, 0),
		AudioTokens:              max(u.AudioOutput, 0),
		ImageTokens:              max(u.ImageOutput, 0),
		AcceptedPredictionTokens: max(u.AcceptedPrediction, 0),
		RejectedPredictionTokens: max(u.RejectedPrediction, 0),
	}

	return &wireUsageWrite{
		InputTokens:              inputTokens,
		OutputTokens:             outputTokens,
		TotalTokens:              inputTokens + outputTokens,
		CacheCreationInputTokens: effectiveWrite,
		InputTokensDetails:       inDetails,
		OutputTokensDetails:      outDetails,
	}
}
