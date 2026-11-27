package antigravity

import (
	"strings"
	"testing"
)

// TestDefaultMappingIsEmpty checks the built-in mapping supplied to routing and
// the admin endpoint. An allocated empty map lets the endpoint return an empty
// JSON object.
func TestDefaultMappingIsEmpty(t *testing.T) {
	if DefaultAntigravityModelMapping == nil || len(DefaultAntigravityModelMapping) != 0 {
		t.Fatalf("expected an allocated empty mapping, got %#v", DefaultAntigravityModelMapping)
	}
}

// TestApplyThinkingModelSuffix compares enabled and disabled thinking across
// base, suffixed, and unrelated models. Only the enabled Sonnet base name
// gains a suffix in these fixtures.
func TestApplyThinkingModelSuffix(t *testing.T) {
	tests := []struct {
		name            string // human-readable table case name
		mappedModel     string // input mapped model name
		thinkingEnabled bool   // input thinking selection flag
		expected        string // expected selected model name
	}{
		{
			name:            "thinking disabled - claude-sonnet-4-5 unchanged",
			mappedModel:     "claude-sonnet-4-5",
			thinkingEnabled: false,
			expected:        "claude-sonnet-4-5",
		},
		{
			name:            "thinking disabled - other model unchanged",
			mappedModel:     "claude-opus-4-6-thinking",
			thinkingEnabled: false,
			expected:        "claude-opus-4-6-thinking",
		},

		{
			name:            "thinking enabled - claude-sonnet-4-5 becomes thinking version",
			mappedModel:     "claude-sonnet-4-5",
			thinkingEnabled: true,
			expected:        "claude-sonnet-4-5-thinking",
		},

		{
			name:            "thinking enabled - claude-sonnet-4-5-thinking unchanged",
			mappedModel:     "claude-sonnet-4-5-thinking",
			thinkingEnabled: true,
			expected:        "claude-sonnet-4-5-thinking",
		},
		{
			name:            "thinking enabled - claude-opus-4-6-thinking unchanged",
			mappedModel:     "claude-opus-4-6-thinking",
			thinkingEnabled: true,
			expected:        "claude-opus-4-6-thinking",
		},
		{
			name:            "thinking enabled - gemini model unchanged",
			mappedModel:     "gemini-3-flash",
			thinkingEnabled: true,
			expected:        "gemini-3-flash",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyThinkingModelSuffix(tt.mappedModel, tt.thinkingEnabled)
			if result != tt.expected {
				t.Errorf("applyThinkingModelSuffix(%q, %v) = %q, want %q",
					tt.mappedModel, tt.thinkingEnabled, result, tt.expected)
			}
		})
	}
}

// TestInjectIdentityPatchToGeminiRequestWithPrompt exercises custom prompt
// replacement, blank fallback to default identity, pre-marked request bypass,
// and lossless 64-bit integer retention. Every non-marked input must prepend
// exactly one identity part while preserving trailing user instructions and raw numbers.
func TestInjectIdentityPatchToGeminiRequestWithPrompt(t *testing.T) {
	raw := []byte(`{"systemInstruction":{"parts":[{"text":"Follow repo rules."}]},"contents":[{"role":"user","parts":[{"functionCall":{"name":"check","args":{"id":9007199254740993}}}]}]}`)

	patchedCustom, err := InjectIdentityPatchToGeminiRequestWithPrompt(raw, "  Custom admin Antigravity prompt.  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	sCustom := string(patchedCustom)
	if !strings.Contains(sCustom, `"Custom admin Antigravity prompt."`) {
		t.Fatalf("expected custom prompt in %s", sCustom)
	}
	if !strings.Contains(sCustom, `"Follow repo rules."`) {
		t.Fatalf("expected existing instruction retained in %s", sCustom)
	}
	if !strings.Contains(sCustom, "9007199254740993") {
		t.Fatalf("expected 64-bit integer preserved in %s", sCustom)
	}

	patchedDefault, err := InjectIdentityPatchToGeminiRequestWithPrompt(raw, "   ")
	if err != nil {
		t.Fatalf("unexpected error on blank custom prompt: %v", err)
	}
	if !strings.Contains(string(patchedDefault), "You are Antigravity") {
		t.Fatalf("expected default identity patch on blank prompt, got %s", string(patchedDefault))
	}

	premarked := []byte(`{"systemInstruction":{"parts":[{"text":"You are Antigravity already."}]},"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)
	unchanged, err := InjectIdentityPatchToGeminiRequestWithPrompt(premarked, "Custom replacement")
	if err != nil {
		t.Fatalf("unexpected error on premarked payload: %v", err)
	}
	if string(unchanged) != string(premarked) {
		t.Fatalf("expected premarked payload unchanged, got %s", string(unchanged))
	}
}
