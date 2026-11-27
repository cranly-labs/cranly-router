package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestRunImportAndVerify writes a raw capture bundle containing live-shaped API
// keys, email addresses, local user paths, disallowed HTTP headers, and inline
// base64 media into _incoming/, then runs the import and verify subcommands. It
// verifies exit codes, header filtering, secret scrubbing, and failure on
// injected secret leaks.
func TestRunImportAndVerify(t *testing.T) {
	root := t.TempDir()
	corpusDir := filepath.Join(root, "corpus")
	incomingDir := filepath.Join(corpusDir, "_incoming")
	require.NoError(t, os.MkdirAll(incomingDir, 0o755))

	bundle := `{
  "meta": {
    "protocol": "anthropic_messages",
    "client": "claude-code",
    "version": "2.1.323",
    "provider": "anthropic",
    "model": "claude-sonnet-4-5",
    "captured_at": "2026-11-21",
    "scenario": "lab_text",
    "notes": "captured by alice@company.io in /Users/alice/work"
  },
  "raw_headers": {
    "Authorization": "Bearer sk-ant-api03-secretvalue1234567890",
    "X-Api-Key": "sk-ant-api03-secretvalue1234567890",
    "User-Agent": "claude-cli/2.1.323",
    "Anthropic-Beta": "interleaved-thinking-2025-05-14",
    "Anthropic-Version": "2023-06-01",
    "Cookie": "session=topsecret"
  },
  "turn": 1,
  "request_body": {
    "model": "claude-sonnet-4-5",
    "max_tokens": 1024,
    "messages": [
      {
        "role": "user",
        "content": [
          {"type": "text", "text": "Inspect /Users/alice/repo for bob@corp.example with sk-proj-abcdefghijklmnopqrstuv"},
          {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"}}
        ]
      }
    ]
  },
  "upstream_sse": "event: message_start\r\ndata: {\"type\":\"message_start\",\"note\":\"/home/alice/secret\"}\r\n\r\n"
}`
	require.NoError(t, os.WriteFile(filepath.Join(incomingDir, "01.capture.json"), []byte(bundle), 0o644))

	var stdout, stderr bytes.Buffer
	code := run([]string{"import", incomingDir}, &stdout, &stderr)
	require.Equal(t, 0, code, stderr.String())
	require.Contains(t, stdout.String(), "imported 4 files")

	reqPath := filepath.Join(corpusDir, "clients", "claude-code", "2.1.323", "lab_text", "1.request.json")
	reqBytes, err := os.ReadFile(reqPath)
	require.NoError(t, err)
	require.NotContains(t, string(reqBytes), "alice")
	require.NotContains(t, string(reqBytes), "bob@corp.example")
	require.NotContains(t, string(reqBytes), "sk-proj-")
	require.Contains(t, string(reqBytes), testkit.RedactedHomePlaceholder+"/repo")
	require.Contains(t, string(reqBytes), testkit.RedactedEmailPlaceholder)
	require.Contains(t, string(reqBytes), testkit.RedactedKeyPlaceholder)
	require.Contains(t, string(reqBytes), testkit.SyntheticPNGDataBase64)

	metaPath := filepath.Join(corpusDir, "clients", "claude-code", "2.1.323", "lab_text", "meta.json")
	metaBytes, err := os.ReadFile(metaPath)
	require.NoError(t, err)
	require.Contains(t, string(metaBytes), `"user-agent": "claude-cli/2.1.323"`)
	require.Contains(t, string(metaBytes), `"anthropic-beta": "interleaved-thinking-2025-05-14"`)
	require.NotContains(t, string(metaBytes), "authorization")
	require.NotContains(t, string(metaBytes), "cookie")

	ssePath := filepath.Join(corpusDir, "upstreams", "anthropic", "claude-sonnet-4-5", "lab_text", "1.stream.sse")
	sseBytes, err := os.ReadFile(ssePath)
	require.NoError(t, err)
	require.Contains(t, string(sseBytes), "\r\n")
	require.Contains(t, string(sseBytes), testkit.RedactedHomePlaceholder+"/secret")

	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 0, run([]string{"verify", corpusDir}, &stdout, &stderr), stderr.String())
	require.Contains(t, stdout.String(), "verified 4 files")

	// Inject an unredacted key into a committed file and confirm verify fails with code 1.
	require.NoError(t, os.WriteFile(reqPath, []byte(`{"leaked":"sk-ant-api03-unredactedsecretvalue999"}`), 0o644))
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, run([]string{"verify", corpusDir}, &stdout, &stderr))
	require.Contains(t, stderr.String(), "corpus verification failed")
}

// TestRunUsageAndUnknownSubcommand invokes run with empty arguments, --help,
// and an unrecognized subcommand name. It verifies that help exits with status
// zero on stdout while usage errors write to stderr and return exit code two.
func TestRunUsageAndUnknownSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 2, run(nil, &stdout, &stderr))
	require.Contains(t, stderr.String(), "Usage:")

	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 0, run([]string{"--help"}, &stdout, &stderr))
	require.Contains(t, stdout.String(), "corpus import")

	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 2, run([]string{"bogus"}, &stdout, &stderr))
	require.Contains(t, stderr.String(), `unknown subcommand "bogus"`)
}
