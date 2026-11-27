package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// main.go provides the command-line entrypoint for importing sanitized traffic
// captures from _incoming/ into testdata/corpus/ and verifying committed corpus
// directories against secret leaks and size limits. Diagnostic output writes
// directly to caller-supplied io.Writer streams rather than standard library log
// in compliance with internal package logging rules.

const (
	// defaultBackendCorpusDir and defaultRepoCorpusDir locate testdata/corpus
	// whether the tool is invoked from backend/ or from the repository root.
	defaultBackendCorpusDir = "internal/protocol/testkit/testdata/corpus"
	defaultRepoCorpusDir    = "backend/internal/protocol/testkit/testdata/corpus"
)

// main parses command-line arguments and exits with the status code returned by run.
func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the "import", "verify", or "help" corpus subcommand and writes
// human-readable summaries or diagnostics to stdout and stderr. It returns exit
// code 0 on success, 1 on import or verification errors, and 2 on invalid CLI usage.
func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stderr)
		return 2
	}

	switch args[0] {
	case "import":
		if err := runImport(args[1:], stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "corpus import error: %v\n", err)
			return 1
		}
		return 0
	case "verify":
		if err := runVerify(args[1:], stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "corpus verify error: %v\n", err)
			return 1
		}
		return 0
	case "-h", "--help", "help":
		printUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown subcommand %q\n\n", args[0])
		printUsage(stderr)
		return 2
	}
}

// runImport parses flags for the import subcommand, imports sanitized captures,
// and prints a completion summary to stdout. Omitting incoming_dir defaults to
// testdata/corpus/_incoming, and omitting -dest defaults to the parent directory
// when incoming_dir is named _incoming.
func runImport(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("corpus import", flag.ContinueOnError)
	fs.SetOutput(stderr)
	destFlag := fs.String("dest", "", "destination corpus directory (defaults to parent of _incoming)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: corpus import [-dest <corpus_dir>] [<incoming_dir>]")
	}

	incomingDir := ""
	if fs.NArg() == 1 {
		incomingDir = fs.Arg(0)
	} else {
		incomingDir = filepath.Join(resolveDefaultCorpusDir(), "_incoming")
	}

	destDir := *destFlag
	if destDir == "" {
		cleanedIncoming := filepath.Clean(incomingDir)
		if filepath.Base(cleanedIncoming) == "_incoming" {
			destDir = filepath.Dir(cleanedIncoming)
		} else {
			destDir = resolveDefaultCorpusDir()
		}
	}

	report, err := testkit.ImportCorpusDir(incomingDir, destDir)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "imported %d files into %s (verified total %d bytes)\n", len(report.ImportedFiles), destDir, report.TotalBytes)
	return nil
}

// runVerify parses flags for the verify subcommand and checks all corpus
// invariants on the target directory. Omitting corpus_dir resolves the default
// testdata/corpus path from either backend/ or the repository root.
func runVerify(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("corpus verify", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 1 {
		return errors.New("usage: corpus verify [<corpus_dir>]")
	}

	corpusDir := resolveDefaultCorpusDir()
	if fs.NArg() == 1 {
		corpusDir = fs.Arg(0)
	}

	report, err := testkit.VerifyCorpusDir(corpusDir)
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "verified %d files in %s (%d bytes)\n", report.CheckedFiles, corpusDir, report.TotalBytes)
	return nil
}

// resolveDefaultCorpusDir selects defaultBackendCorpusDir when the current
// working directory is backend/ and falls back to defaultRepoCorpusDir when
// invoked from the repository root. If neither parent exists on disk, it
// returns defaultBackendCorpusDir so error messages cite a consistent path.
func resolveDefaultCorpusDir() string {
	if _, err := os.Stat(filepath.Dir(defaultBackendCorpusDir)); err == nil {
		return defaultBackendCorpusDir
	}
	if _, err := os.Stat(filepath.Dir(defaultRepoCorpusDir)); err == nil {
		return defaultRepoCorpusDir
	}
	return defaultBackendCorpusDir
}

// printUsage writes command synopsis lines for import and verify to w.
func printUsage(w io.Writer) {
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  corpus import [-dest <corpus_dir>] [<incoming_dir>]")
	fmt.Fprintln(w, "  corpus verify [<corpus_dir>]")
}
