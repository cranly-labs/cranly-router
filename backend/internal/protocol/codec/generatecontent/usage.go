package generatecontent

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
)

// usage.go validates, merges, and serializes Google Gemini usageMetadata token
// counters across buffered responses and sparse streaming SSE frames. Gemini
// reports inclusive promptTokenCount (covering both fresh prompt tokens and
// cachedContentTokenCount) and separates candidatesTokenCount from
// thoughtsTokenCount, whereas IR Usage stores disjoint fresh Input, CacheRead,
// and inclusive Output (candidates plus thoughts) buckets.
//
// Incoming envelopes and usageMetadata objects pass wirejson duplicate-key and
// case-collision checks before counter extraction. Streaming observations merge
// non-null usageMetadata fields across early prompt frames and post-finishReason
// tail frames so sparse output-only updates preserve previously observed prompt
// and cache counts without double-counting or regressing cumulative buckets.

// wireUsageMetadata decodes canonical Gemini usageMetadata token counters and
// per-modality detail arrays.
type wireUsageMetadata struct {
	PromptTokenCount        *int64              `json:"promptTokenCount,omitzero"`        // inclusive prompt tokens (fresh + cached)
	CachedContentTokenCount *int64              `json:"cachedContentTokenCount,omitzero"` // prompt tokens served from cachedContent
	CandidatesTokenCount    *int64              `json:"candidatesTokenCount,omitzero"`    // visible generated candidate tokens
	ThoughtsTokenCount      *int64              `json:"thoughtsTokenCount,omitzero"`      // generated reasoning tokens billed as output
	TotalTokenCount         *int64              `json:"totalTokenCount,omitzero"`         // upstream total token sum
	PromptTokensDetails     []wireModalityCount `json:"promptTokensDetails,omitzero"`     // per-modality prompt token breakdown
	CandidatesTokensDetails []wireModalityCount `json:"candidatesTokensDetails,omitzero"` // per-modality output token breakdown
}

// wireModalityCount decodes one modality token count inside promptTokensDetails or candidatesTokensDetails.
type wireModalityCount struct {
	Modality   string `json:"modality,omitzero"`   // "TEXT", "IMAGE", "AUDIO", or "VIDEO"
	TokenCount *int64 `json:"tokenCount,omitzero"` // nonnegative tokens attributed to Modality
}

// wireUsageMetadataWrite serializes canonical Gemini usageMetadata fields on
// outgoing buffered responses and terminal SSE frames.
type wireUsageMetadataWrite struct {
	PromptTokenCount        int64                    `json:"promptTokenCount"`                 // inclusive prompt tokens (Input + CacheRead + CacheWrite)
	CachedContentTokenCount int64                    `json:"cachedContentTokenCount,omitzero"` // positive cached prompt tokens
	CandidatesTokenCount    int64                    `json:"candidatesTokenCount"`             // non-thought generated output tokens
	ThoughtsTokenCount      int64                    `json:"thoughtsTokenCount,omitzero"`      // positive generated reasoning tokens
	TotalTokenCount         int64                    `json:"totalTokenCount"`                  // promptTokenCount + candidatesTokenCount + thoughtsTokenCount
	PromptTokensDetails     []wireModalityCountWrite `json:"promptTokensDetails,omitzero"`     // optional image/audio prompt breakdown
	CandidatesTokensDetails []wireModalityCountWrite `json:"candidatesTokensDetails,omitzero"` // optional image/audio output breakdown
}

// wireModalityCountWrite serializes one modality token entry inside usageMetadata detail slices.
type wireModalityCountWrite struct {
	Modality   string `json:"modality"`   // uppercase Gemini modality enum label
	TokenCount int64  `json:"tokenCount"` // positive token count for Modality
}

// streamUsageTracker merges non-null Gemini usageMetadata observations across
// sparse SSE frames into cumulative disjoint IR Usage snapshots.
type streamUsageTracker struct {
	// hasPrompt through hasCandDetails track whether each optional usageMetadata
	// field has arrived on any frame so later sparse frames omit unmentioned
	// fields without erasing earlier prompt, cache, or modality observations.
	hasPrompt     bool                // whether promptTokenCount was observed
	hasCached     bool                // whether cachedContentTokenCount was observed
	hasCandidates bool                // whether candidatesTokenCount was observed
	hasThoughts   bool                // whether thoughtsTokenCount was observed
	hasPromptDets bool                // whether promptTokensDetails was observed
	hasCandDets   bool                // whether candidatesTokensDetails was observed
	promptTokens  int64               // latest observed promptTokenCount
	cachedTokens  int64               // latest observed cachedContentTokenCount
	candTokens    int64               // latest observed candidatesTokenCount
	thoughtTokens int64               // latest observed thoughtsTokenCount
	promptDetails []wireModalityCount // latest observed promptTokensDetails
	candDetails   []wireModalityCount // latest observed candidatesTokensDetails
	prevSnapshot  *ir.Usage           // previous emitted Usage snapshot for monotonicity
}

// decodeGeminiUsage validates envelopeBytes and rawUsage before projecting
// Gemini usageMetadata counters into disjoint ir.Usage buckets. Absent or null
// rawUsage returns (nil, nil), whereas duplicate keys, case-folded key
// collisions, or negative token counts return a validation error.
func decodeGeminiUsage(envelopeBytes []byte, rawUsage jsontext.Value) (*ir.Usage, error) {
	trimmed := bytes.TrimSpace(rawUsage)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	var tracker streamUsageTracker
	u, ok, err := tracker.observe(envelopeBytes, trimmed)
	if err != nil || !ok {
		return nil, err
	}
	return u, nil
}

// observe validates frameBytes and rawUsage, merges non-null usageMetadata
// fields into t, and returns an independent cumulative ir.Usage snapshot.
//
// Gemini streaming endpoints frequently emit promptTokenCount and
// cachedContentTokenCount on the initial frame and send only
// candidatesTokenCount on a trailing frame after finishReason. Retaining prior
// non-null fields before computing Input = max(prompt - cached, 0) and
// Output = candidates + thoughts matches NativeExtractGeminiUsage while
// mergeMonotonicUsage prevents malformed mid-stream resets from violating
// ir.ValidateStream.
func (t *streamUsageTracker) observe(frameBytes []byte, rawUsage jsontext.Value) (*ir.Usage, bool, error) {
	trimmed := bytes.TrimSpace(rawUsage)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, false, nil
	}
	if err := wirejson.ValidateUsageEnvelope(frameBytes); err != nil {
		return nil, false, err
	}
	if err := validateGeminiUsageObject(frameBytes, trimmed); err != nil {
		return nil, false, err
	}

	var parsed wireUsageMetadata
	if err := unmarshalJSONObject(trimmed, &parsed); err != nil {
		return nil, false, err
	}
	if err := validateNonNegativeGeminiUsage(parsed); err != nil {
		return nil, false, err
	}

	if parsed.PromptTokenCount != nil {
		t.hasPrompt = true
		t.promptTokens = *parsed.PromptTokenCount
	}
	if parsed.CachedContentTokenCount != nil {
		t.hasCached = true
		t.cachedTokens = *parsed.CachedContentTokenCount
	}
	if parsed.CandidatesTokenCount != nil {
		t.hasCandidates = true
		t.candTokens = *parsed.CandidatesTokenCount
	}
	if parsed.ThoughtsTokenCount != nil {
		t.hasThoughts = true
		t.thoughtTokens = *parsed.ThoughtsTokenCount
	}
	if len(parsed.PromptTokensDetails) > 0 {
		t.hasPromptDets = true
		t.promptDetails = parsed.PromptTokensDetails
	}
	if len(parsed.CandidatesTokensDetails) > 0 {
		t.hasCandDets = true
		t.candDetails = parsed.CandidatesTokensDetails
	}

	// Subtract cachedContentTokenCount once from inclusive promptTokenCount and
	// clamp at zero so inconsistent compatible totals never yield negative Input.
	freshInput := max(t.promptTokens-t.cachedTokens, 0)
	imageIn, audioIn := sumModalityTokens(t.promptDetails)
	imageOut, audioOut := sumModalityTokens(t.candDetails)

	cur := &ir.Usage{
		Input:       freshInput,
		CacheRead:   max(t.cachedTokens, 0),
		Output:      max(t.candTokens, 0) + max(t.thoughtTokens, 0),
		Reasoning:   max(t.thoughtTokens, 0),
		ImageInput:  imageIn,
		ImageOutput: imageOut,
		AudioInput:  audioIn,
		AudioOutput: audioOut,
	}
	mergeMonotonicUsage(t.prevSnapshot, cur)
	t.prevSnapshot = cur.Clone()
	return cur, true, nil
}

// validateGeminiUsageObject checks envelopeBytes for case-colliding
// usageMetadata sibling keys and checks rawUsage recursively for exact or
// case-folded duplicate member names. wirejson.ValidateUsageEnvelope skips
// camelCase usageMetadata scopes, so dedicated token scanning guards billing
// counters here without reparsing numbers.
func validateGeminiUsageObject(envelopeBytes, rawUsage []byte) error {
	if err := wirejson.ValidateUsageObject(rawUsage); err != nil {
		return err
	}
	if err := checkCaseInsensitiveObjectKeys(envelopeBytes, true); err != nil {
		return err
	}
	return checkCaseInsensitiveObjectKeys(rawUsage, false)
}

// checkCaseInsensitiveObjectKeys scans JSON objects for case-folded key
// collisions. When rootOnlyUsageMeta is true, only top-level keys folding to
// "usagemetadata" are checked; otherwise every nested object inside raw is checked.
func checkCaseInsensitiveObjectKeys(raw []byte, rootOnlyUsageMeta bool) error {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("usage JSON payload must be an object")
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	if _, err := dec.ReadToken(); err != nil {
		return err
	}
	if rootOnlyUsageMeta {
		seenUsageMeta := false
		for dec.PeekKind() != '}' {
			kTok, err := dec.ReadToken()
			if err != nil {
				return err
			}
			if strings.EqualFold(kTok.String(), "usageMetadata") {
				if seenUsageMeta {
					return errors.New("ambiguous case-colliding usageMetadata keys in envelope")
				}
				seenUsageMeta = true
			}
			if _, err := dec.ReadValue(); err != nil {
				return err
			}
		}
		return nil
	}
	return scanNestedCaseInsensitiveObject(dec)
}

// scanNestedCaseInsensitiveObject recursively verifies that an already-opened
// JSON object in dec contains no case-folded duplicate keys at any depth. It
// consumes the closing '}' token and returns an error on syntax or key collisions.
func scanNestedCaseInsensitiveObject(dec *jsontext.Decoder) error {
	seen := make(map[string]struct{})
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil {
			return err
		}
		folded := strings.ToLower(kTok.String())
		if _, dup := seen[folded]; dup {
			return fmt.Errorf("ambiguous usageMetadata key %q", kTok.String())
		}
		seen[folded] = struct{}{}
		if err := skipOrScanNestedUsageValue(dec); err != nil {
			return err
		}
	}
	_, err := dec.ReadToken()
	return err
}

// skipOrScanNestedUsageValue advances dec past one complete JSON value while
// recursing into nested objects and arrays. Scalar tokens are consumed directly,
// whereas nested objects enforce case-folded key uniqueness.
func skipOrScanNestedUsageValue(dec *jsontext.Decoder) error {
	switch dec.PeekKind() {
	case '{':
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		return scanNestedCaseInsensitiveObject(dec)
	case '[':
		if _, err := dec.ReadToken(); err != nil {
			return err
		}
		for dec.PeekKind() != ']' {
			if err := skipOrScanNestedUsageValue(dec); err != nil {
				return err
			}
		}
		_, err := dec.ReadToken()
		return err
	default:
		_, err := dec.ReadValue()
		return err
	}
}

// validateNonNegativeGeminiUsage checks scalar and per-modality token counters
// inside w before disjoint bucket subtraction runs. Any negative counter
// returns an error, while nil counter pointers are ignored.
func validateNonNegativeGeminiUsage(w wireUsageMetadata) error {
	for _, ptr := range []*int64{
		w.PromptTokenCount,
		w.CachedContentTokenCount,
		w.CandidatesTokenCount,
		w.ThoughtsTokenCount,
		w.TotalTokenCount,
	} {
		if ptr != nil && *ptr < 0 {
			return errors.New("usageMetadata token counters must be nonnegative")
		}
	}
	for _, d := range w.PromptTokensDetails {
		if d.TokenCount != nil && *d.TokenCount < 0 {
			return errors.New("usageMetadata.promptTokensDetails tokenCount must be nonnegative")
		}
	}
	for _, d := range w.CandidatesTokensDetails {
		if d.TokenCount != nil && *d.TokenCount < 0 {
			return errors.New("usageMetadata.candidatesTokensDetails tokenCount must be nonnegative")
		}
	}
	return nil
}

// sumModalityTokens totals positive IMAGE and AUDIO token counts across details.
// Entries with nil or non-positive TokenCount pointers and non-media modalities
// such as TEXT are skipped without error.
func sumModalityTokens(details []wireModalityCount) (imageTokens, audioTokens int64) {
	for _, d := range details {
		if d.TokenCount == nil || *d.TokenCount <= 0 {
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(d.Modality)) {
		case "IMAGE":
			imageTokens += *d.TokenCount
		case "AUDIO":
			audioTokens += *d.TokenCount
		}
	}
	return imageTokens, audioTokens
}

// mergeMonotonicUsage reconciles cur against prev across cumulative Gemini
// stream usage observations so sparse output-only tail events satisfy
// ir.ValidateStream monotonicity while explicit all-zero resets still clear state.
//
// Compatible Gemini relays occasionally emit promptTokenCount and
// cachedContentTokenCount on an early frame and send explicit zero prompt fields
// alongside candidatesTokenCount on the final frame. Retaining prior CacheRead
// and Input values when cur still reports positive token activity prevents
// cumulative snapshots from regressing mid-stream.
func mergeMonotonicUsage(prev, cur *ir.Usage) {
	if prev == nil || cur == nil || !cur.HasObservedTokens() {
		return
	}
	if prev.CacheRead > 0 && cur.CacheRead == 0 {
		cur.CacheRead = prev.CacheRead
	}
	if prev.Input > 0 && cur.Input == 0 &&
		cur.CacheRead <= prev.CacheRead &&
		cur.EffectiveCacheWrite() <= prev.EffectiveCacheWrite() {
		cur.Input = prev.Input
	}
}

// encodeGeminiUsage projects disjoint IR Usage buckets into Gemini's inclusive
// usageMetadata wire object. Nil Usage pointers return nil, whereas non-nil
// Usage values reconstruct promptTokenCount = Input + CacheRead + CacheWrite
// and candidatesTokenCount = max(Output - Reasoning, 0).
func encodeGeminiUsage(u *ir.Usage) *wireUsageMetadataWrite {
	if u == nil {
		return nil
	}
	promptTotal := u.InclusiveInputTokens()
	cacheRead := max(u.CacheRead, 0)
	thoughts := max(u.Reasoning, 0)
	// Subtract Reasoning once from inclusive IR Output so candidatesTokenCount
	// + thoughtsTokenCount equals Output on re-decoding without double-counting.
	candidates := max(max(u.Output, 0)-thoughts, 0)
	total := promptTotal + candidates + thoughts

	var promptDetails []wireModalityCountWrite
	if u.ImageInput > 0 {
		promptDetails = append(promptDetails, wireModalityCountWrite{
			Modality:   "IMAGE",
			TokenCount: u.ImageInput,
		})
	}
	if u.AudioInput > 0 {
		promptDetails = append(promptDetails, wireModalityCountWrite{
			Modality:   "AUDIO",
			TokenCount: u.AudioInput,
		})
	}

	var candDetails []wireModalityCountWrite
	if u.ImageOutput > 0 {
		candDetails = append(candDetails, wireModalityCountWrite{
			Modality:   "IMAGE",
			TokenCount: u.ImageOutput,
		})
	}
	if u.AudioOutput > 0 {
		candDetails = append(candDetails, wireModalityCountWrite{
			Modality:   "AUDIO",
			TokenCount: u.AudioOutput,
		})
	}

	return &wireUsageMetadataWrite{
		PromptTokenCount:        promptTotal,
		CachedContentTokenCount: cacheRead,
		CandidatesTokenCount:    candidates,
		ThoughtsTokenCount:      thoughts,
		TotalTokenCount:         total,
		PromptTokensDetails:     promptDetails,
		CandidatesTokensDetails: candDetails,
	}
}
