package testkit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSanitizeAndVerifyCorpusBoundaries imports structured captures containing
// camelCase apiKey fields, PDF document attachments, and inline images. Valid
// captures must preserve application/pdf and scrub secrets, while failed
// batches must leave zero partial files in the destination directory.
func TestSanitizeAndVerifyCorpusBoundaries(t *testing.T) {
	oversized := `{"text":"` + strings.Repeat("a", MaxCorpusFileBytes) + `"}`
	_, err := SanitizeJSONPayload([]byte(oversized))
	require.ErrorIs(t, err, ErrCorpusFileTooLarge)

	incoming := t.TempDir()
	dest := filepath.Join(t.TempDir(), "corpus")
	srcDir := filepath.Join(incoming, "clients", "claude-code", "2.1.323", "document_input")
	require.NoError(t, os.MkdirAll(srcDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "meta.json"), []byte(`{
		"protocol": "anthropic_messages",
		"headers": {
			"Anthropic-Version": "2023-06-01",
			"User-Agent": "claude-cli/2.1.323",
			"Authorization": "Bearer sk-1234567890abcdefghijklmnop"
		}
	}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "1.request.json"), []byte(`{
		"model": "claude-sonnet-4-5",
		"max_tokens": 1024,
		"metadata": {"apiKey": "custom-secret-token", "accessToken": "opaque-session-secret"},
		"messages": [{
			"role": "user",
			"content": [
				{
					"type": "document",
					"source": {
						"type": "base64",
						"media_type": "application/pdf",
						"data": "JVBERi0xLjQK"
					}
				},
				{
					"type": "image",
					"source": {
						"type": "base64",
						"media_type": "image/jpeg",
						"data": "QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo="
					}
				},
				{
					"type": "text",
					"text": "check C:\\Users\\bob\\notes.txt and data:application/pdf;base64,QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVo="
				}
			]
		}]
	}`), 0o644))

	rep, err := ImportCorpusDir(incoming, dest)
	require.NoError(t, err)
	require.Len(t, rep.ImportedFiles, 1)

	importedBytes, err := os.ReadFile(filepath.Join(dest, "clients", "claude-code", "2.1.323", "document_input", "1.request.json"))
	require.NoError(t, err)
	importedText := string(importedBytes)
	require.NotContains(t, importedText, "custom-secret-token")
	require.NotContains(t, importedText, "opaque-session-secret")
	require.Contains(t, importedText, `"apiKey": "<REDACTED_KEY>"`)
	require.Contains(t, importedText, `"accessToken": "<REDACTED_KEY>"`)
	require.Contains(t, importedText, `"media_type": "application/pdf"`)
	require.Contains(t, importedText, SyntheticPDFDataBase64)
	require.Contains(t, importedText, `"media_type": "image/png"`)
	require.Contains(t, importedText, SyntheticPNGDataBase64)
	require.Contains(t, importedText, "data:application/pdf;base64,"+SyntheticPDFDataBase64)
	require.Contains(t, importedText, RedactedHomePlaceholder)

	// Verify failed batch import writes zero files to a clean destination directory.
	atomicIncoming := t.TempDir()
	atomicDest := filepath.Join(t.TempDir(), "clean_corpus")
	require.NoError(t, os.WriteFile(filepath.Join(atomicIncoming, "01_good.capture.json"), []byte(`{
		"meta": {"protocol": "openai_responses", "client": "codex", "version": "0.169.0", "scenario": "lab_text"},
		"turn": 1,
		"request_body": {"model": "gpt-5", "input": "ok"}
	}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(atomicIncoming, "02_too_large.capture.json"), []byte(`{
		"meta": {"protocol": "openai_responses", "client": "codex", "version": "0.169.0", "scenario": "lab_long"},
		"turn": 1,
		"request_body": {"model": "gpt-5", "input": "`+strings.Repeat("x", MaxCorpusFileBytes)+`"}
	}`), 0o644))
	_, err = ImportCorpusDir(atomicIncoming, atomicDest)
	require.ErrorIs(t, err, ErrCorpusFileTooLarge)
	_, statErr := os.Stat(atomicDest)
	require.True(t, os.IsNotExist(statErr), "failed ImportCorpusDir must not leave partial files in dest")

	// Placing a .go file inside corpus must fail VerifyCorpusDir.
	badGo := filepath.Join(dest, "helper.go")
	require.NoError(t, os.WriteFile(badGo, []byte("package bad\n"), 0o644))
	_, err = VerifyCorpusDir(dest)
	require.Error(t, err)
	require.Contains(t, err.Error(), ".go files are forbidden")
	require.NoError(t, os.Remove(badGo))

	// Writing a disallowed header key into meta.json must fail VerifyCorpusDir.
	metaFile := filepath.Join(dest, "clients", "claude-code", "2.1.323", "document_input", "meta.json")
	require.NoError(t, os.WriteFile(metaFile, []byte(`{"protocol":"anthropic_messages","client":"claude-code","headers":{"cookie":"abc"}}`), 0o644))
	_, err = VerifyCorpusDir(dest)
	require.Error(t, err)
	require.Contains(t, err.Error(), `disallowed or non-lowercase header "cookie"`)
}

// TestDetectAndSanitizeGoogleAQKeyPrefix feeds JSON and SSE payloads carrying
// an AQ.-prefixed Google API key alongside a trc1:gemini continuity carrier.
// DetectSecretViolations must flag the raw key, and sanitization must redact
// the key while preserving the continuity carrier verbatim.
func TestDetectAndSanitizeGoogleAQKeyPrefix(t *testing.T) {
	syntheticAQKey := "AQ.SyntheticGoogleKeyForUnitTesting0123456789"
	rawJSON := []byte(`{"note":"key=` + syntheticAQKey + `","signature":"trc1:gemini:eyJkIjoiQ3BFQkNnSUlBeGdCIn0"}`)
	findings := DetectSecretViolations(rawJSON)
	require.NotEmpty(t, findings)

	sanitizedJSON, err := SanitizeJSONPayload(rawJSON)
	require.NoError(t, err)
	require.NotContains(t, string(sanitizedJSON), syntheticAQKey)
	require.Contains(t, string(sanitizedJSON), RedactedKeyPlaceholder)
	require.Contains(t, string(sanitizedJSON), "trc1:gemini:eyJkIjoiQ3BFQkNnSUlBeGdCIn0")
	require.Empty(t, DetectSecretViolations(sanitizedJSON))

	sanitizedSSE, err := SanitizeSSEPayload([]byte("data: " + string(rawJSON) + "\n\n"))
	require.NoError(t, err)
	require.NotContains(t, string(sanitizedSSE), syntheticAQKey)
	require.Empty(t, DetectSecretViolations(sanitizedSSE))
}
