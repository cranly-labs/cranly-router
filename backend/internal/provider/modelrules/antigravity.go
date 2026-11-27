package modelrules

import (
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
)

// antigravity.go defines output limits, thinking budgets, and parameter
// restrictions for converted Antigravity requests. Claude identities and Chat
// requests cap output at 64,000 tokens while other converted routes cap at
// 65,000 tokens, every converted request reserves 1,000 budget-padding tokens,
// and registered Opus and Gemini identities supply adaptive budgets and field
// omissions shared by preview resolution and ConversionDialect.

// antigravityPatch supplies limits and thinking encodings for converted
// Antigravity requests. The Gemini native client protocol leaves these
// conversion limits unset and continues with its catalog capabilities.
//
// Claude identities and Chat requests use the registered 64,000-token limit;
// other converted routes use 65,000 tokens, retaining any smaller known
// catalog limit. Budget padding reserves output room above thinking. Registered
// adaptive Claude models use their fixed thinking budget, and selected Gemini
// thinking identities also restrict sampling, tool configuration, and stop
// sequences for converted Gemini payloads.
func (r *Resolution) antigravityPatch(patch *modelcatalog.Attributes) {
	model := normalize(r.Target.Model)
	if r.Target.ClientProtocol == "gemini_generate_content" {
		return
	}
	limit := 65000
	if strings.HasPrefix(model, "claude-") || r.Target.ClientProtocol == "openai_chat_completions" {
		limit = AntigravityDefaultOutputLimit
	}
	patch.OutputLimit = modelcatalog.MinimumLimit(r.Attributes.OutputLimit, &limit)
	// Reserve 1,000 output tokens above positive thinking budgets so
	// ConversionDialect and codec encoders leave room for visible reply tokens
	// beneath the converted output cap.
	r.Rules.BudgetPadding = 1000
	if slices.Contains([]string{"claude-opus-4-6", "claude-opus-4-6-thinking", "claude-opus-4-7", "claude-opus-4-7-thinking", "claude-opus-4-8", "claude-opus-4-8-thinking"}, model) {
		r.Rules.AdaptiveThinkingBudget = AntigravityAdaptiveBudget
	}
	if slices.Contains([]string{"gemini-2.5-flash-thinking", "gemini-3-pro-high", "gemini-3.1-pro-high", "gemini-3.6-flash-high", "gemini-3.6-flash-low", "gemini-3.6-flash-medium", "gemini-3.6-flash-tiered"}, model) {
		// Restrict sampling, empty toolConfig, and stop sequences on these Gemini
		// thinking identities so ConversionDialect omits controls rejected by
		// Antigravity v1internal endpoints.
		patch.Temperature, patch.TopP = new(false), new(false)
		r.Rules.DropFields = append(r.Rules.DropFields, "top_k")
		r.Rules.OmitEmptyToolConfig = true
		r.Rules.OmitStopSequences = true
	}
}
