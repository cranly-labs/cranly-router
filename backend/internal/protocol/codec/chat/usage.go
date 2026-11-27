package chat

import (
	"bytes"
	"encoding/json/jsontext"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
)

// usage.go decodes, merges, and encodes OpenAI Chat Completions token usage
// counters while guarding incoming payloads with wirejson validation. OpenAI
// Chat Completions reports inclusive prompt_tokens covering fresh input, cache
// reads, and cache writes together, whereas IR Usage stores those three prompt
// categories in disjoint buckets so cross-protocol encoders never double-count
// cached tokens.
//
// Incoming buffered and streamed usage objects first pass ValidateUsageEnvelope
// and ValidateUsageObject so duplicate or non-canonically cased counters cannot
// diverge from gateway billing accounting. Validated objects then flow through
// openai.UsageAccumulator to honor nested detail precedence, DeepSeek hit/miss
// aliases, 30m/12h and 5m/1h cache-write breakdowns, audio/image/prediction
// details, and xAI independent reasoning-token total gaps.

// wireUsageWrite serializes inclusive Chat Completions usage counters and details.
type wireUsageWrite struct {
	PromptTokens            int64                             `json:"prompt_tokens"`                      // inclusive prompt token total
	CompletionTokens        int64                             `json:"completion_tokens"`                  // inclusive completion token total
	TotalTokens             int64                             `json:"total_tokens"`                       // prompt plus completion total
	PromptTokensDetails     *wirePromptTokensDetailsWrite     `json:"prompt_tokens_details,omitzero"`     // prompt cache, audio, and image details
	CompletionTokensDetails *wireCompletionTokensDetailsWrite `json:"completion_tokens_details,omitzero"` // reasoning, audio, and prediction details
}

// wirePromptTokensDetailsWrite serializes cached, cache-write, audio, and image sub-counters inside prompt_tokens_details.
type wirePromptTokensDetailsWrite struct {
	CachedTokens     int64 `json:"cached_tokens"`               // prompt tokens served from cache
	CacheWriteTokens int64 `json:"cache_write_tokens,omitzero"` // prompt tokens written to cache
	AudioTokens      int64 `json:"audio_tokens,omitzero"`       // prompt audio input tokens
	ImageTokens      int64 `json:"image_tokens,omitzero"`       // prompt image input tokens
}

// wireCompletionTokensDetailsWrite serializes reasoning, audio, image, and prediction sub-counters inside completion_tokens_details.
type wireCompletionTokensDetailsWrite struct {
	ReasoningTokens          int64 `json:"reasoning_tokens"`                    // reasoning tokens within completion
	AudioTokens              int64 `json:"audio_tokens,omitzero"`               // generated audio output tokens
	ImageTokens              int64 `json:"image_tokens,omitzero"`               // generated image output tokens
	AcceptedPredictionTokens int64 `json:"accepted_prediction_tokens,omitzero"` // accepted predicted tokens
	RejectedPredictionTokens int64 `json:"rejected_prediction_tokens,omitzero"` // rejected predicted tokens
}

// wireAnthropicCacheCreation captures optional 5m/1h cache_creation details on compatible responses.
type wireAnthropicCacheCreation struct {
	CacheCreation *struct {
		Ephemeral5m *int64 `json:"ephemeral_5m_input_tokens,omitzero"` // optional five-minute cache writes
		Ephemeral1h *int64 `json:"ephemeral_1h_input_tokens,omitzero"` // optional one-hour cache writes
	} `json:"cache_creation,omitzero"` // compatible Anthropic-style cache TTL breakdown
}

// decodeChatUsage validates envelopeBytes and rawUsage with wirejson, feeds
// rawUsage through a fresh openai.UsageAccumulator, and returns disjoint IR
// Usage counters. Absent or null rawUsage returns (nil, nil), whereas invalid
// or case-ambiguous usage envelopes return a validation error.
func decodeChatUsage(envelopeBytes []byte, rawUsage jsontext.Value) (*ir.Usage, error) {
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
	snap := acc.Observe(trimmed)
	if snap == nil {
		return nil, nil
	}
	return projectAccumulatorUsage(&acc, snap, trimmed), nil
}

// observeStreamUsage validates an incoming stream chunk's usage envelope and
// merges rawUsage into acc, preserving previously observed prompt and cache
// buckets when a sparse non-zero tail chunk omits or zeroes prompt fields.
// Ambiguous usage envelopes return an error so invalid counts never emit.
func observeStreamUsage(acc *openai.UsageAccumulator, prev *ir.Usage, chunkBytes []byte, rawUsage jsontext.Value) (*ir.Usage, bool, error) {
	trimmed := bytes.TrimSpace(rawUsage)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, false, nil
	}
	if err := wirejson.ValidateUsageEnvelope(chunkBytes); err != nil {
		return nil, false, err
	}
	if err := wirejson.ValidateUsageObject(trimmed); err != nil {
		return nil, false, err
	}
	snap := acc.Observe(trimmed)
	if snap == nil {
		return nil, false, nil
	}
	cur := projectAccumulatorUsage(acc, snap, trimmed)
	mergeMonotonicUsage(prev, cur)
	return cur, true, nil
}

// projectAccumulatorUsage converts an accumulated openai.ResponsesUsage
// snapshot and ForwardUsage accounting view into disjoint nonnegative ir.Usage
// buckets.
//
// OpenAI Chat Completions includes cached reads and cache writes inside
// prompt_tokens, so projectAccumulatorUsage subtracts both cached buckets once
// and clamps at zero to prevent negative fresh Input counts when compatible
// gateways report inconsistent totals. Optional 30m/12h and 5m/1h TTL breakdowns
// are populated alongside audio, image, reasoning, and prediction sub-counters.
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

	// Subtract cached read and write buckets once from inclusive prompt_tokens
	// so Usage.Input holds only fresh uncached prompt tokens.
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
	var compatTTL wireAnthropicCacheCreation
	// Compatible gateways occasionally send non-object cache_creation metadata;
	// ignoring unmarshal errors here preserves validated primary token totals.
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
	return out
}

// mergeMonotonicUsage reconciles cur against prev across cumulative Chat stream
// usage chunks so partial tail updates satisfy ir.ValidateStream monotonicity.
//
// Some OpenAI-compatible providers emit prompt_tokens on an initial chunk and
// send a trailing completion-only chunk with prompt_tokens omitted or zeroed.
// When cur still reports non-zero observed tokens without reclassifying prompt
// tokens into larger cache buckets, mergeMonotonicUsage retains prior CacheRead,
// CacheWrite, and Input counts while allowing an explicit all-zero snapshot reset.
func mergeMonotonicUsage(prev, cur *ir.Usage) {
	if prev == nil || cur == nil || !cur.HasObservedTokens() {
		return
	}
	if prev.CacheRead > 0 && cur.CacheRead == 0 {
		cur.CacheRead = prev.CacheRead
	}
	if prev.EffectiveCacheWrite() > 0 && cur.EffectiveCacheWrite() == 0 {
		cur.CacheWrite = prev.CacheWrite
		cur.CacheWrite5m = prev.Clone().CacheWrite5m
		cur.CacheWrite30m = prev.Clone().CacheWrite30m
		cur.CacheWrite1h = prev.Clone().CacheWrite1h
		cur.CacheWrite12h = prev.Clone().CacheWrite12h
	}
	if prev.Input > 0 && cur.Input == 0 &&
		cur.CacheRead <= prev.CacheRead &&
		cur.EffectiveCacheWrite() <= prev.EffectiveCacheWrite() {
		cur.Input = prev.Input
	}
}

// encodeChatUsage projects disjoint IR Usage counters into inclusive OpenAI
// Chat Completions prompt_tokens, completion_tokens, total_tokens, and detail
// objects. Nil inputs return nil, while non-nil Usage values sum Input,
// CacheRead, and EffectiveCacheWrite once into prompt_tokens.
func encodeChatUsage(u *ir.Usage) *wireUsageWrite {
	if u == nil {
		return nil
	}
	promptTokens := u.InclusiveInputTokens()
	completionTokens := max(u.Output, 0)
	return &wireUsageWrite{
		PromptTokens:     promptTokens,
		CompletionTokens: completionTokens,
		TotalTokens:      promptTokens + completionTokens,
		PromptTokensDetails: &wirePromptTokensDetailsWrite{
			CachedTokens:     max(u.CacheRead, 0),
			CacheWriteTokens: u.EffectiveCacheWrite(),
			AudioTokens:      max(u.AudioInput, 0),
			ImageTokens:      max(u.ImageInput, 0),
		},
		CompletionTokensDetails: &wireCompletionTokensDetailsWrite{
			ReasoningTokens:          max(u.Reasoning, 0),
			AudioTokens:              max(u.AudioOutput, 0),
			ImageTokens:              max(u.ImageOutput, 0),
			AcceptedPredictionTokens: max(u.AcceptedPrediction, 0),
			RejectedPredictionTokens: max(u.RejectedPrediction, 0),
		},
	}
}
