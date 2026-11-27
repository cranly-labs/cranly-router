package gemini

import (
	"github.com/tidwall/gjson"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// observation.go extracts independent usage, content, and completion evidence
// and projects native usageMetadata token buckets from Gemini response payloads.
// Gemini execution can report usage without candidate content, and candidate
// output can arrive frames ahead of a finishReason or trailing usageMetadata
// update. Independent observations let stream and buffered completion loops
// distinguish those states: explicit zero counters establish usage presence,
// while semantic output requires a recognized non-empty part value and terminal
// completion requires a candidate's non-empty finishReason.
//
// Native billing extraction partitions promptTokenCount into uncached input and
// cachedContentTokenCount, sums candidatesTokenCount with thoughtsTokenCount for
// output, and selects the first uppercase IMAGE candidatesTokensDetails entry
// as an overlapping output subset without clamping or range-filtering observed
// integers.

// Observation records independent usage, output, and termination evidence.
type Observation struct {
	// HasUsage, Semantic, and Terminal describe independent fields in one
	// payload. A candidate finish reason can appear without content or usage,
	// and reported usage does not imply a completed candidate.
	//
	// HasUsage records any recognized nonnull usage field, including an explicit
	// zero count. The extractor does not require the field to contain a valid number.
	//
	// Semantic records recognized nonempty candidate parts. Text, tool names,
	// inline image data, and file URIs can establish it independently of a finish reason.
	//
	// Terminal records a candidate's nonempty finish reason. It establishes
	// payload-level completion evidence; delivery and error handling belong to the caller.
	HasUsage, Semantic, Terminal bool /* HasUsage: nonnull counter presence;
	Semantic: recognized content presence; Terminal: candidate finish evidence */
}

// NativeGeminiUsage holds observed Gemini response token counts in disjoint
// input/cache buckets plus inclusive candidate+thought output and image subset counts.
type NativeGeminiUsage struct {
	// InputTokens and CacheReadInputTokens partition promptTokenCount into
	// uncached and cached prompt tokens, while OutputTokens sums candidate and
	// thinking tokens and ImageOutputTokens records the first exact IMAGE
	// candidatesTokensDetails entry as an overlapping output subset.
	InputTokens          int // uncached prompt tokens (promptTokenCount - cachedContentTokenCount)
	OutputTokens         int // candidate plus thinking tokens
	CacheReadInputTokens int // cached prompt tokens (cachedContentTokenCount)
	ImageOutputTokens    int // first IMAGE modality candidate token count
}

// ObservePayload reads usage and candidate evidence from a top-level Gemini
// response payload. Each result describes this payload alone; callers accumulate
// observations across stream events when they need evidence for a whole attempt.
// This path-based extractor can read fields from malformed JSON. Callers remove
// internal Google wrappers to expose the expected top-level fields and validate
// the complete document separately when their completion rules require it.
func ObservePayload(payload []byte) Observation {
	value := gjson.ParseBytes(payload)
	observed := Observation{}
	usage := value.Get("usageMetadata")
	for _, field := range []string{"promptTokenCount", "candidatesTokenCount", "totalTokenCount", "cachedContentTokenCount", "thoughtsTokenCount"} {
		v := usage.Get(field)
		if v.Exists() && v.Type != gjson.Null {
			observed.HasUsage = true
		}
	}
	value.Get("candidates").ForEach(func(_, candidate gjson.Result) bool {
		if candidate.Get("finishReason").String() != "" {
			observed.Terminal = true
		}
		candidate.Get("content.parts").ForEach(func(_, part gjson.Result) bool {
			if part.Get("text").String() != "" || part.Get("functionCall.name").String() != "" || part.Get("inlineData.data").String() != "" || part.Get("inline_data.data").String() != "" || part.Get("fileData.fileUri").String() != "" {
				observed.Semantic = true
			}
			return true
		})
		return true
	})
	return observed
}

// NativeExtractGeminiUsage reads usageMetadata from data and returns disjoint
// prompt/cache and inclusive output token counts. Missing usageMetadata returns
// nil, whereas present metadata (including explicit zero counters) returns a
// non-nil NativeGeminiUsage populated via gjson integer conversion.
//
// Cached content tokens are subtracted from promptTokenCount so InputTokens
// holds uncached input, thoughtsTokenCount is added to candidatesTokenCount,
// and the first exact "IMAGE" candidatesTokensDetails entry populates
// ImageOutputTokens without being added a second time to OutputTokens.
func NativeExtractGeminiUsage(data []byte) *NativeGeminiUsage {
	usage := gjson.GetBytes(data, "usageMetadata")
	if !usage.Exists() {
		return nil
	}
	prompt := int(usage.Get("promptTokenCount").Int())
	cand := int(usage.Get("candidatesTokenCount").Int())
	cached := int(usage.Get("cachedContentTokenCount").Int())
	thoughts := int(usage.Get("thoughtsTokenCount").Int())

	// Record the first IMAGE modality detail as an overlapping subset of
	// candidate output while keeping OutputTokens equal to candidates + thoughts.
	imageTokens := 0
	candidateDetails := usage.Get("candidatesTokensDetails")
	if candidateDetails.Exists() {
		candidateDetails.ForEach(func(_, detail gjson.Result) bool {
			if detail.Get("modality").String() == "IMAGE" {
				imageTokens = int(detail.Get("tokenCount").Int())
				return false
			}
			return true
		})
	}

	// Subtract cached prompt tokens from inclusive promptTokenCount and fold
	// thinking tokens into OutputTokens to match shared gateway billing buckets.
	return &NativeGeminiUsage{
		InputTokens:          prompt - cached,
		OutputTokens:         cand + thoughts,
		CacheReadInputTokens: cached,
		ImageOutputTokens:    imageTokens,
	}
}

// NativeUsageProjection copies usage into a fresh protocol.TokenUsage value for
// gateway and upstream billing consumers. Nil input returns nil, and non-nil
// input preserves all four integer token buckets verbatim.
func NativeUsageProjection(usage *NativeGeminiUsage) *protocol.TokenUsage {
	if usage == nil {
		return nil
	}
	return &protocol.TokenUsage{
		InputTokens:          usage.InputTokens,
		OutputTokens:         usage.OutputTokens,
		CacheReadInputTokens: usage.CacheReadInputTokens,
		ImageOutputTokens:    usage.ImageOutputTokens,
	}
}
