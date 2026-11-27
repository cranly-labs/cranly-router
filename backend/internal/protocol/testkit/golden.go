package testkit

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// golden.go manages golden snapshot assertions and controlled write-backs for
// protocol test packages. Pure packages under internal/protocol delegate file
// reads and updates here so their own test suites stay free of os imports, and
// update mode reads TOKENROUTER_UPDATE_GOLDEN=1 rather than registering a
// package-local CLI test flag that would break sibling packages during
// wildcard go test runs.

const (
	// GoldenUpdateEnv names the environment variable that enables writing actual
	// bytes back to testdata/golden/ files when set to "1".
	GoldenUpdateEnv = "TOKENROUTER_UPDATE_GOLDEN"

	// maxGoldenDiffLines caps line-by-line text mismatch output so large snapshot
	// regressions print a focused excerpt rather than flooding test logs.
	maxGoldenDiffLines = 40
)

// Golden compares got with testdata/golden/<name> relative to the caller test's
// working directory, or writes got to that path when TOKENROUTER_UPDATE_GOLDEN=1.
// Any path traversal outside testdata/golden, missing file in read mode, or
// byte difference fails t immediately.
func Golden(t testing.TB, name string, got []byte) {
	t.Helper()
	GoldenInDir(t, filepath.Join("testdata", "golden"), name, got)
}

// GoldenJSON marshals v deterministically with two-space indentation and a
// trailing newline, then delegates comparison or update to Golden. Serialization
// errors or byte mismatches fail t immediately.
func GoldenJSON(t testing.TB, name string, v any) {
	t.Helper()
	formatted, err := MarshalDeterministicIndent(v)
	if err != nil {
		t.Fatalf("marshal golden JSON %s: %v", name, err)
	}
	Golden(t, name, formatted)
}

// MarshalDeterministicIndent encodes v with sorted object keys, two-space
// indentation, and a single trailing LF for stable golden files. Values that
// fail JSON serialization return nil alongside the marshal error.
func MarshalDeterministicIndent(v any) ([]byte, error) {
	raw, err := json.Marshal(v, json.Deterministic(true), jsontext.WithIndent("  "))
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 || raw[len(raw)-1] != '\n' {
		raw = append(raw, '\n')
	}
	return raw, nil
}

// GoldenInDir compares got with <baseDir>/<name>, writing got back when
// TOKENROUTER_UPDATE_GOLDEN=1. Callers supply a clean relative snapshot name
// that stays within baseDir; missing files or byte differences fail t.
func GoldenInDir(t testing.TB, baseDir, name string, got []byte) {
	t.Helper()
	targetPath, err := resolveGoldenPath(baseDir, name)
	if err != nil {
		t.Fatalf("invalid golden name %q: %v", name, err)
	}

	if os.Getenv(GoldenUpdateEnv) == "1" {
		if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
			t.Fatalf("create golden directory for %s: %v", targetPath, err)
		}
		if err := os.WriteFile(targetPath, got, 0o644); err != nil {
			t.Fatalf("write golden file %s: %v", targetPath, err)
		}
		return
	}

	want, err := os.ReadFile(targetPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("golden file %s does not exist; run with %s=1 to create it", targetPath, GoldenUpdateEnv)
		}
		t.Fatalf("read golden file %s: %v", targetPath, err)
	}
	if bytes.Equal(want, got) {
		return
	}

	detail := describeGoldenMismatch(name, want, got)
	t.Fatalf("golden mismatch for %s (set %s=1 to update):\n%s", targetPath, GoldenUpdateEnv, detail)
}

// resolveGoldenPath joins baseDir and slash-separated name while rejecting
// empty names, absolute paths, and parent-directory escapes.
func resolveGoldenPath(baseDir, name string) (string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", errors.New("empty snapshot name")
	}
	rel := filepath.FromSlash(trimmed)
	if filepath.IsAbs(rel) {
		return "", errors.New("snapshot name must be relative")
	}
	cleanBase := filepath.Clean(baseDir)
	joined := filepath.Join(cleanBase, rel)
	relCheck, err := filepath.Rel(cleanBase, joined)
	if err != nil {
		return "", err
	}
	if relCheck == "." || relCheck == ".." || strings.HasPrefix(relCheck, ".."+string(filepath.Separator)) {
		return "", errors.New("snapshot name escapes golden directory")
	}
	return joined, nil
}

// describeGoldenMismatch builds a JSON-path summary when both snapshots parse
// as JSON documents. It also appends a bounded line diff so byte formatting or
// key-order differences remain visible.
func describeGoldenMismatch(name string, want, got []byte) string {
	var sections []string
	if strings.HasSuffix(strings.ToLower(name), ".json") {
		if diffs, err := JSONDiff(want, got, WithAllowInvalidUTF8(true)); err == nil {
			if len(diffs) > 0 {
				sections = append(sections, "semantic JSON diff:\n"+FormatJSONDiff(diffs))
			} else {
				sections = append(sections, "JSON values are semantically equal; byte formatting or key order differs.")
			}
		}
	}
	sections = append(sections, formatBoundedLineDiff(string(want), string(got)))
	return strings.Join(sections, "\n\n")
}

// formatBoundedLineDiff compares want and got line by line up to
// maxGoldenDiffLines mismatched lines. Large snapshot failures stop at that cap
// with a truncation note.
func formatBoundedLineDiff(want, got string) string {
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	limit := max(len(wantLines), len(gotLines))

	var b strings.Builder
	_, _ = b.WriteString("line diff (-want +got):")
	reported := 0
	for i := range limit {
		var wLine, gLine string
		hasW := i < len(wantLines)
		hasG := i < len(gotLines)
		if hasW {
			wLine = wantLines[i]
		}
		if hasG {
			gLine = gotLines[i]
		}
		if hasW && hasG && wLine == gLine {
			continue
		}
		if reported >= maxGoldenDiffLines {
			fmt.Fprintf(&b, "\n  ... (truncated after %d differing lines)", maxGoldenDiffLines)
			break
		}
		if hasW {
			fmt.Fprintf(&b, "\n  %4d - %s", i+1, wLine)
		}
		if hasG {
			fmt.Fprintf(&b, "\n  %4d + %s", i+1, gLine)
		}
		reported++
	}
	return b.String()
}
