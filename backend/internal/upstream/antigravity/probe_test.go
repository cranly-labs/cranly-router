package antigravity

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// decodedProbeEnvelope decodes a v1internal probe payload built by
// BuildClaudeTestRequest and BuildGeminiTestRequest for validation assertions.
type decodedProbeEnvelope struct {
	Project     string              `json:"project"`     // GCP project identifier
	RequestID   string              `json:"requestId"`   // generated agent-* request ID
	UserAgent   string              `json:"userAgent"`   // fixed antigravity client label
	RequestType string              `json:"requestType"` // fixed agent requestType
	Model       string              `json:"model"`       // mapped probe model identifier
	Request     decodedProbeRequest `json:"request"`     // inner Gemini probe request
}

// decodedProbeRequest decodes inner Gemini fields emitted by probe builders.
type decodedProbeRequest struct {
	SystemInstruction decodedProbeContent       `json:"systemInstruction"` // injected platform identity
	Contents          []decodedProbeTurn        `json:"contents"`          // single user probe turn
	GenerationConfig  decodedProbeGenCfg        `json:"generationConfig"`  // probe generation settings
	Raw               map[string]jsontext.Value `json:",unknown"`          // remaining top-level members
}

// decodedProbeContent decodes systemInstruction parts inside a probe request.
type decodedProbeContent struct {
	Parts []decodedProbePart `json:"parts"` // ordered text instruction parts
}

// decodedProbeTurn decodes one role-tagged Gemini turn inside probe contents.
type decodedProbeTurn struct {
	Role  string             `json:"role"`  // conversation turn role
	Parts []decodedProbePart `json:"parts"` // turn text parts
}

// decodedProbePart decodes the text member of a probe content or system part.
type decodedProbePart struct {
	Text string `json:"text"` // probe prompt or identity text
}

// decodedProbeGenCfg decodes the maxOutputTokens limit inside a probe payload's generationConfig object.
type decodedProbeGenCfg struct {
	MaxOutputTokens int `json:"maxOutputTokens"` // expected 1-token probe cap
}

// decodeProbeEnvelopeBytes unmarshals raw v1internal probe request JSON bytes into decodedProbeEnvelope.
func decodeProbeEnvelopeBytes(t *testing.T, raw []byte) decodedProbeEnvelope {
	t.Helper()
	var env decodedProbeEnvelope
	dec := jsontext.NewDecoder(bytes.NewReader(raw))
	require.NoError(t, json.UnmarshalDecode(dec, &env))
	return env
}

// TestBuildClaudeTestRequest_EnvelopeAndValidation exercises BuildClaudeTestRequest
// across default, blank, trimmed, and multi-arg prompts for claude-* and
// gemini-* models. Each generated payload must carry the v1internal envelope,
// default identity patch, and satisfy strict Gemini request rules while
// rejecting blank project identifiers.
func TestBuildClaudeTestRequest_EnvelopeAndValidation(t *testing.T) {
	t.Run("missing_project_returns_ErrProjectIDRequired", func(t *testing.T) {
		_, err := BuildClaudeTestRequest("   ", "claude-sonnet-4-5", "ping")
		require.ErrorIs(t, err, ErrProjectIDRequired)
	})

	cases := []struct {
		name       string   // subtest scenario label
		model      string   // target mapped model identifier
		prompts    []string // variadic prompt arguments
		wantPrompt string   // expected trimmed user prompt text
	}{
		{
			name:       "default_dot_when_no_prompts",
			model:      "claude-sonnet-4-5",
			prompts:    nil,
			wantPrompt: ".",
		},
		{
			name:       "default_dot_when_first_prompt_blank",
			model:      "claude-opus-4-6-thinking",
			prompts:    []string{"   ", "ignored-second"},
			wantPrompt: ".",
		},
		{
			name:       "trims_first_prompt_and_ignores_later_prompts",
			model:      "claude-sonnet-4-5",
			prompts:    []string{"  Reply with OK  ", "extra"},
			wantPrompt: "Reply with OK",
		},
		{
			name:       "gemini_mapped_model_in_claude_probe",
			model:      "gemini-2.5-flash",
			prompts:    []string{"healthcheck"},
			wantPrompt: "healthcheck",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			geminiBytes, err := BuildGeminiTestRequest("  proj-probe-42  ", tc.model, tc.prompts...)
			require.NoError(t, err)
			claudeBytes, err := BuildClaudeTestRequest("  proj-probe-42  ", tc.model, tc.prompts...)
			require.NoError(t, err)

			geminiEnv := decodeProbeEnvelopeBytes(t, geminiBytes)
			claudeEnv := decodeProbeEnvelopeBytes(t, claudeBytes)

			require.Equal(t, "proj-probe-42", claudeEnv.Project)
			require.Equal(t, geminiEnv.Project, claudeEnv.Project)
			require.Equal(t, "antigravity", claudeEnv.UserAgent)
			require.Equal(t, "agent", claudeEnv.RequestType)
			require.Equal(t, tc.model, claudeEnv.Model)
			require.True(t, strings.HasPrefix(claudeEnv.RequestID, "agent-"))

			require.Len(t, claudeEnv.Request.Contents, 1)
			require.Equal(t, "user", claudeEnv.Request.Contents[0].Role)
			require.Len(t, claudeEnv.Request.Contents[0].Parts, 1)
			require.Equal(t, tc.wantPrompt, claudeEnv.Request.Contents[0].Parts[0].Text)
			require.Equal(t, 1, claudeEnv.Request.GenerationConfig.MaxOutputTokens)

			require.NotEmpty(t, claudeEnv.Request.SystemInstruction.Parts)
			require.Equal(t, GetDefaultIdentityPatch(), claudeEnv.Request.SystemInstruction.Parts[0].Text)

			// Extract the raw inner "request" object and verify strict Gemini request rules.
			outerMembers, parseErr := parseOrderedJSONObject(claudeBytes)
			require.NoError(t, parseErr)
			var innerRaw []byte
			for _, m := range outerMembers {
				if m.name == "request" {
					innerRaw = m.value
					break
				}
			}
			require.NotEmpty(t, innerRaw)
			require.Empty(t, testkit.ValidateRequestRules(
				protocol.ProtocolGeminiGenerateContent,
				innerRaw,
				testkit.RequestValidationOptions{
					DialectRules: []string{"antigravity-schema"},
				},
			))
		})
	}
}
