package ir

import (
	"encoding/json/jsontext"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
)

// TestValidateRequestValidAndInvalidCases runs Validate on a valid multimodal,
// tool, reasoning, and opaque Request (including empty Gemini Model and -1
// dynamic thinking budget). It then verifies rejection across union branch
// mismatches, role placement rules, tool schemas, sampling bounds, and Extra invariants.
func TestValidateRequestValidAndInvalidCases(t *testing.T) {
	require.Error(t, Validate(nil))

	dynBudget := int64(-1)
	candOne := int64(1)
	maxOut := int64(512)
	startIdx := int64(0)
	endIdx := int64(4)

	validBase := func() *Request {
		return &Request{
			// Empty Model is valid because Gemini carries model in the URL path.
			System: []Part{
				{Kind: PartText, Text: "Follow policy.", Cache: &CacheMark{TTL: "5m"}},
				{
					Kind: PartOpaque,
					Opaque: &OpaqueItem{
						Origin: protocol.ProtocolAnthropicMessages,
						Type:   "custom_sys",
						Raw:    jsontext.Value(`{"type":"custom_sys"}`),
					},
				},
			},
			Messages: []Message{
				{
					Role: RoleUser,
					Parts: []Part{
						{
							Kind: PartText,
							Text: "Inspect",
							Annotations: []Annotation{{
								Kind:       "url_citation",
								URL:        "https://example.com",
								StartIndex: &startIdx,
								EndIndex:   &endIdx,
							}},
						},
						{
							Kind:  PartMedia,
							Media: &Media{Type: MediaImage, MIME: "image/png", Source: MediaSourceBase64, Data: "iVBORw=="},
						},
					},
				},
				{
					Role: RoleAssistant,
					Parts: []Part{
						{
							Kind: PartReasoning,
							Reasoning: &Reasoning{
								Segments: []string{"plan"},
								Token:    &ReplayToken{Scheme: SchemeAnthropic, Data: "sig1"},
							},
						},
						{
							Kind: PartToolCall,
							Call: &ToolCall{
								CallID:    "c1",
								Tool:      ToolRef{Kind: ToolFunction, Namespace: "ns", Name: "fn"},
								Arguments: `{"a":1}`,
							},
						},
					},
				},
				{
					Role: RoleTool,
					Parts: []Part{{
						Kind: PartToolResult,
						Result: &ToolResult{
							CallID: "c1",
							Tool:   ToolRef{Kind: ToolFunction, Namespace: "ns", Name: "fn"},
							Content: []Part{
								{Kind: PartText, Text: "done"},
								{Kind: PartMedia, Media: &Media{Type: MediaImage, Source: MediaSourceURL, Data: "https://example.com/a.png"}},
							},
						},
					}},
				},
			},
			Namespaces: []Namespace{{Name: "ns"}},
			Tools: []Tool{
				{Kind: ToolFunction, Namespace: "ns", Name: "fn", InputSchema: jsontext.Value(`{"type":"object"}`)},
				{Kind: ToolCustom, Name: "patch", Format: jsontext.Value(`{"type":"grammar"}`)},
				{Kind: ToolClientSearch},
				{
					Kind: ToolHosted,
					Name: "web_search",
					Hosted: &OpaqueItem{
						Origin: protocol.ProtocolOpenAIResponses,
						Type:   "web_search",
						Raw:    jsontext.Value(`{"type":"web_search"}`),
					},
				},
			},
			ToolChoice:      &ToolChoice{Mode: ToolChoiceTool, Tool: ToolRef{Kind: ToolFunction, Name: "fn"}},
			Sampling:        Sampling{CandidateCount: &candOne},
			MaxOutputTokens: &maxOut,
			Reasoning:       &ReasoningConfig{Mode: "adaptive", BudgetTokens: &dynBudget, Summary: "auto"},
			Output:          &OutputFormat{Kind: "json_schema", Name: "out", Schema: jsontext.Value(`{"type":"object"}`)},
			Extra: Extra{
				Origin:  protocol.ProtocolOpenAIResponses,
				Members: map[string]jsontext.Value{"safety_identifier": jsontext.Value("\"u_\xff\"")},
			},
		}
	}

	require.NoError(t, Validate(validBase()))

	cases := []struct {
		name   string         // invalid mutation label
		mutate func(*Request) // mutation applied to validBase()
	}{
		{"invalid message role", func(r *Request) { r.Messages[0].Role = "invalid" }},
		{"empty tool message", func(r *Request) { r.Messages[2].Parts = nil }},
		{"text inside tool message", func(r *Request) { r.Messages[2].Parts = []Part{{Kind: PartText, Text: "bad"}} }},
		{"tool_result inside user message", func(r *Request) {
			r.Messages[0].Parts = append(r.Messages[0].Parts, Part{Kind: PartToolResult, Result: &ToolResult{CallID: "c1"}})
		}},
		{"tool_call inside user message", func(r *Request) {
			r.Messages[0].Parts = append(r.Messages[0].Parts, Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "c2", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}},
			})
		}},
		{"tool_call marked incomplete in request", func(r *Request) {
			r.Messages[1].Parts[1].Call.Incomplete = true
		}},
		{"tool_call in system instructions", func(r *Request) {
			r.System = append(r.System, Part{
				Kind: PartToolCall,
				Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}},
			})
		}},
		{"nested reasoning inside tool_result content", func(r *Request) {
			r.Messages[2].Parts[0].Result.Content = append(r.Messages[2].Parts[0].Result.Content, Part{
				Kind:      PartReasoning,
				Reasoning: &Reasoning{Segments: []string{"no"}},
			})
		}},
		{"text part with call branch", func(r *Request) {
			r.Messages[0].Parts[0].Call = &ToolCall{Tool: ToolRef{Kind: ToolFunction, Name: "fn"}}
		}},
		{"refusal part with annotations", func(r *Request) {
			r.Messages[1].Parts = append(r.Messages[1].Parts, Part{
				Kind:        PartRefusal,
				Text:        "no",
				Annotations: []Annotation{{URL: "https://example.com"}},
			})
		}},
		{"inverted annotation span", func(r *Request) {
			s := int64(10)
			e := int64(2)
			r.Messages[0].Parts[0].Annotations[0].StartIndex = &s
			r.Messages[0].Parts[0].Annotations[0].EndIndex = &e
		}},
		{"reasoning part nil branch", func(r *Request) { r.Messages[1].Parts[0].Reasoning = nil }},
		{"replay token invalid scheme", func(r *Request) { r.Messages[1].Parts[0].Reasoning.Token.Scheme = "bogus" }},
		{"replay token empty data", func(r *Request) { r.Messages[1].Parts[0].Reasoning.Token.Data = "" }},
		{"media invalid type", func(r *Request) { r.Messages[0].Parts[1].Media.Type = "hologram" }},
		{"media invalid source", func(r *Request) { r.Messages[0].Parts[1].Media.Source = "ftp" }},
		{"media empty data", func(r *Request) { r.Messages[0].Parts[1].Media.Data = "" }},
		{"opaque missing origin", func(r *Request) { r.System[1].Opaque.Origin = "" }},
		{"opaque invalid raw JSON", func(r *Request) { r.System[1].Opaque.Raw = jsontext.Value(`{bad`) }},
		{"invalid cache ttl", func(r *Request) { r.System[0].Cache.TTL = "24h" }},
		{"empty namespace name", func(r *Request) { r.Namespaces[0].Name = "  " }},
		{"function tool empty name", func(r *Request) { r.Tools[0].Name = "" }},
		{"function tool with custom format", func(r *Request) { r.Tools[0].Format = jsontext.Value(`{}`) }},
		{"function tool duplicate schema keys", func(r *Request) { r.Tools[0].InputSchema = jsontext.Value(`{"a":1,"a":2}`) }},
		{"custom tool with input_schema", func(r *Request) { r.Tools[1].InputSchema = jsontext.Value(`{}`) }},
		{"hosted tool nil payload", func(r *Request) { r.Tools[3].Hosted = nil }},
		{"auto tool_choice with target tool", func(r *Request) { r.ToolChoice.Mode = ToolChoiceAuto }},
		{"tool_choice tool mode missing name", func(r *Request) { r.ToolChoice.Tool.Name = "" }},
		{"negative max_output_tokens", func(r *Request) { neg := int64(-5); r.MaxOutputTokens = &neg }},
		{"zero candidate_count", func(r *Request) { z := int64(0); r.Sampling.CandidateCount = &z }},
		{"invalid reasoning mode", func(r *Request) { r.Reasoning.Mode = "turbo" }},
		{"budget_tokens below -1", func(r *Request) { bad := int64(-2); r.Reasoning.BudgetTokens = &bad }},
		{"schema with text output kind", func(r *Request) { r.Output.Kind = "text" }},
		{"extra missing origin", func(r *Request) { r.Extra.Origin = "" }},
		{"extra trailing JSON token", func(r *Request) { r.Extra.Members["bad"] = jsontext.Value(`1 2`) }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validBase()
			tc.mutate(req)
			require.Error(t, Validate(req))
		})
	}
}

// TestValidateResponseValidAndInvalidCases runs ValidateResponse on normal and
// error responses. Nil responses, invalid finish reasons, tool_result parts,
// normal-finish incomplete calls, and negative usage buckets must all return errors.
func TestValidateResponseValidAndInvalidCases(t *testing.T) {
	require.Error(t, ValidateResponse(nil))

	valid := &Response{
		ID:     "r1",
		Parts:  []Part{{Kind: PartText, Text: "ok"}},
		Finish: Finish{Reason: FinishStop},
		Usage:  &Usage{Input: 10, Output: 5},
	}
	require.NoError(t, ValidateResponse(valid))

	// FinishError allows an Incomplete tool_call snapshot.
	errResp := &Response{
		Parts: []Part{{
			Kind: PartToolCall,
			Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}, Incomplete: true},
		}},
		Finish: Finish{Reason: FinishError},
	}
	require.NoError(t, ValidateResponse(errResp))

	// Normal FinishToolCalls rejects Incomplete=true.
	badIncomplete := errResp.Clone()
	badIncomplete.Finish.Reason = FinishToolCalls
	require.Error(t, ValidateResponse(badIncomplete))

	badFinish := valid.Clone()
	badFinish.Finish.Reason = "bad"
	require.Error(t, ValidateResponse(badFinish))

	badPart := valid.Clone()
	badPart.Parts = []Part{{Kind: PartToolResult, Result: &ToolResult{CallID: "c1"}}}
	require.Error(t, ValidateResponse(badPart))

	negTTL := int64(-1)
	badUsage := valid.Clone()
	badUsage.Usage = &Usage{Input: 5, CacheWrite5m: &negTTL}
	require.Error(t, ValidateResponse(badUsage))
}

// TestValidateStreamInvariants checks all seven canonical stream invariants in
// ValidateStream across valid complete, 100% cache-reclassified, all-zero reset,
// and error streams. Ordering, delta compatibility, snapshot completeness,
// sparse usage resets, and terminal closure violations must all return errors.
func TestValidateStreamInvariants(t *testing.T) {
	validStream := []Event{
		{Kind: EventResponseStart, Response: &ResponseMeta{ID: "s1", Model: "gpt-5"}},
		{Kind: EventUsage, Usage: &Usage{Input: 80, CacheRead: 20, Output: 0}},
		{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartReasoning, Reasoning: &Reasoning{Summary: true}}},
		{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "plan"}},
		{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartReasoning, Reasoning: &Reasoning{Segments: []string{"plan"}, Summary: true}}},
		{Kind: EventPartStart, Index: 1, Part: &Part{Kind: PartText}},
		{Kind: EventPartDelta, Index: 1, Delta: &Delta{Text: "hel"}},
		{Kind: EventPartDelta, Index: 1, Delta: &Delta{Text: "lo"}},
		{Kind: EventPartEnd, Index: 1, Part: &Part{Kind: PartText, Text: "hello"}},
		{
			Kind:  EventPartStart,
			Index: 2,
			Part:  &Part{Kind: PartToolCall, Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}}},
		},
		{Kind: EventPartDelta, Index: 2, Delta: &Delta{Arguments: `{"x":1}`}},
		{
			Kind:  EventPartEnd,
			Index: 2,
			Part:  &Part{Kind: PartToolCall, Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}, Arguments: `{"x":1}`}},
		},
		{Kind: EventUsage, Usage: &Usage{Input: 80, CacheRead: 20, Output: 15}},
		{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishToolCalls}},
	}
	require.NoError(t, ValidateStream(validStream))

	// Explicit all-zero usage reset is permitted.
	zeroReset := []Event{
		{Kind: EventResponseStart, Response: &ResponseMeta{ID: "s_zero"}},
		{Kind: EventUsage, Usage: &Usage{Input: 50, CacheRead: 10, Output: 5}},
		{Kind: EventUsage, Usage: &Usage{}},
		{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
	}
	require.NoError(t, ValidateStream(zeroReset))

	// Tail usage frame reclassifying 100% of earlier unsplit Input into CacheRead or CacheWrite is permitted.
	fullCacheHit := []Event{
		{Kind: EventResponseStart, Response: &ResponseMeta{ID: "s_cache_hit"}},
		{Kind: EventUsage, Usage: &Usage{Input: 100, CacheRead: 0, Output: 0}},
		{Kind: EventUsage, Usage: &Usage{Input: 0, CacheRead: 100, Output: 12}},
		{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
	}
	require.NoError(t, ValidateStream(fullCacheHit))

	fullCacheWrite := []Event{
		{Kind: EventResponseStart, Response: &ResponseMeta{ID: "s_cache_write"}},
		{Kind: EventUsage, Usage: &Usage{Input: 100, CacheWrite: 0, Output: 0}},
		{Kind: EventUsage, Usage: &Usage{Input: 0, CacheWrite: 100, Output: 12}},
		{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
	}
	require.NoError(t, ValidateStream(fullCacheWrite))

	invalidCases := []struct {
		name   string  // stream violation case label
		events []Event // candidate invalid stream
	}{
		{"empty stream", nil},
		{"missing leading response_start", []Event{
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"duplicate response_start", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{ID: "1"}},
			{Kind: EventResponseStart, Response: &ResponseMeta{ID: "2"}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"interleaved open parts", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartText}},
			{Kind: EventPartStart, Index: 1, Part: &Part{Kind: PartText}},
			{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartText}},
			{Kind: EventPartEnd, Index: 1, Part: &Part{Kind: PartText}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"skipped part index", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 1, Part: &Part{Kind: PartText}},
			{Kind: EventPartEnd, Index: 1, Part: &Part{Kind: PartText}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"delta with no open part", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "hi"}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"arguments delta on text part", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartText}},
			{Kind: EventPartDelta, Index: 0, Delta: &Delta{Arguments: "{}"}},
			{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartText}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"text delta on tool_call part", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartToolCall, Call: &ToolCall{Tool: ToolRef{Kind: ToolFunction, Name: "fn"}}}},
			{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "bad"}},
			{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartToolCall, Call: &ToolCall{Tool: ToolRef{Kind: ToolFunction, Name: "fn"}}}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishToolCalls}},
		}},
		{"delta on opaque part", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{
				Kind:  EventPartStart,
				Index: 0,
				Part: &Part{
					Kind:   PartOpaque,
					Opaque: &OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: "x", Raw: jsontext.Value(`{}`)},
				},
			},
			{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "bad"}},
			{
				Kind:  EventPartEnd,
				Index: 0,
				Part: &Part{
					Kind:   PartOpaque,
					Opaque: &OpaqueItem{Origin: protocol.ProtocolOpenAIResponses, Type: "x", Raw: jsontext.Value(`{}`)},
				},
			},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"part_end lost accumulated text", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartText}},
			{Kind: EventPartDelta, Index: 0, Delta: &Delta{Text: "hello"}},
			{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartText, Text: ""}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"part_end lost accumulated tool arguments", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartToolCall, Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}}}},
			{Kind: EventPartDelta, Index: 0, Delta: &Delta{Arguments: `{"a":1}`}},
			{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartToolCall, Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}, Arguments: ""}}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishToolCalls}},
		}},
		{"sparse tail usage cleared prior input", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventUsage, Usage: &Usage{Input: 120, CacheRead: 40, Output: 0}},
			{Kind: EventUsage, Usage: &Usage{Input: 0, CacheRead: 40, Output: 18}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"response_end while part still open", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartText}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
		}},
		{"error while part still open", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartText}},
			{Kind: EventError, Err: &StreamError{Message: "cut"}},
		}},
		{"incomplete call ended with normal response_end", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartToolCall, Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}}}},
			{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartToolCall, Call: &ToolCall{CallID: "c1", Tool: ToolRef{Kind: ToolFunction, Name: "fn"}, Incomplete: true}}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishToolCalls}},
		}},
		{"event after terminal response_end", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventResponseEnd, Finish: &Finish{Reason: FinishStop}},
			{Kind: EventUsage, Usage: &Usage{Input: 1, Output: 1}},
		}},
		{"missing terminal event", []Event{
			{Kind: EventResponseStart, Response: &ResponseMeta{}},
			{Kind: EventPartStart, Index: 0, Part: &Part{Kind: PartText}},
			{Kind: EventPartEnd, Index: 0, Part: &Part{Kind: PartText}},
		}},
	}

	for _, tc := range invalidCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, ValidateStream(tc.events))
		})
	}
}
