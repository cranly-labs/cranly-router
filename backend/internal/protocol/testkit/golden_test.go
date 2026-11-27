package testkit

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// recordingTB wraps testing.TB so negative GoldenInDir assertions can capture
// Fatalf messages without failing the outer test run.
type recordingTB struct {
	testing.TB        // embedded fallback test interface
	failed     bool   // whether Fatalf or Errorf was triggered
	message    string // formatted failure text
}

// Helper marks recordingTB methods as test helpers during failure capture.
func (r *recordingTB) Helper() {}

// Fatalf records the formatted failure message and aborts the inner goroutine
// via runtime.Goexit, matching standard testing.T.Fatalf control flow.
func (r *recordingTB) Fatalf(format string, args ...any) {
	r.failed = true
	r.message = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// runRecordingTB executes fn in a child goroutine so Fatalf's runtime.Goexit
// returns cleanly to the caller with captured failure output.
func runRecordingTB(t *testing.T, fn func(tb testing.TB)) (bool, string) {
	t.Helper()
	rec := &recordingTB{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(rec)
	}()
	<-done
	return rec.failed, rec.message
}

// TestGoldenWriteCompareAndPathSafety runs GoldenInDir across missing files,
// TOKENROUTER_UPDATE_GOLDEN=1 writes, JSON value mismatches, and "../" path
// escapes. Update mode must persist snapshots inside baseDir, while read
// mismatches and directory escapes must fail the supplied testing.TB.
func TestGoldenWriteCompareAndPathSafety(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "golden")

	// Missing file in read mode must fail and mention TOKENROUTER_UPDATE_GOLDEN.
	t.Setenv(GoldenUpdateEnv, "")
	failed, msg := runRecordingTB(t, func(tb testing.TB) {
		GoldenInDir(tb, dir, "ir/sample.json", []byte("{\n  \"a\": 1\n}\n"))
	})
	require.True(t, failed)
	require.Contains(t, msg, GoldenUpdateEnv)

	// Update mode creates nested directories and persists exact bytes.
	t.Setenv(GoldenUpdateEnv, "1")
	want := []byte("{\n  \"a\": 1,\n  \"b\": \"hello\"\n}\n")
	GoldenInDir(t, dir, "ir/sample.json", want)
	diskBytes, err := os.ReadFile(filepath.Join(dir, "ir", "sample.json"))
	require.NoError(t, err)
	require.Equal(t, want, diskBytes)

	// Read mode succeeds on exact match and reports JSON path diffs on mismatch.
	t.Setenv(GoldenUpdateEnv, "")
	GoldenInDir(t, dir, "ir/sample.json", want)

	failed, msg = runRecordingTB(t, func(tb testing.TB) {
		GoldenInDir(tb, dir, "ir/sample.json", []byte("{\n  \"a\": 2,\n  \"b\": \"hello\"\n}\n"))
	})
	require.True(t, failed)
	require.Contains(t, msg, "$.a: want 1, got 2")

	// Traversal outside golden base directory is rejected even in update mode.
	t.Setenv(GoldenUpdateEnv, "1")
	failed, msg = runRecordingTB(t, func(tb testing.TB) {
		GoldenInDir(tb, dir, "../escape.txt", []byte("nope"))
	})
	require.True(t, failed)
	require.Contains(t, msg, "escapes golden directory")
}
