package testkit

import (
	"bytes"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// corpus_import.go sanitizes raw clientlab captures for corpus import and
// verifies committed corpus trees against secret leaks, unredacted headers, and
// size budgets. Raw captures written to testdata/corpus/_incoming can contain
// live bearer tokens, developer email addresses, local home directory paths,
// and multi-megabyte inline attachments that must never enter version control.
//
// Import stages all sanitized payloads in memory first, strips raw HTTP headers
// down to four protocol metadata keys on meta.json, replaces inline base64
// images and PDFs with compact MIME-preserving synthetic payloads, and validates
// per-file 512 KiB and total 15 MiB budgets before writing any file into the
// destination corpus directory.

const (
	// MaxCorpusFileBytes and MaxCorpusTotalBytes bound individual corpus files
	// at 512 KiB and the entire committed corpus directory at 15 MiB.
	MaxCorpusFileBytes  = 512 * 1024
	MaxCorpusTotalBytes = 15 * 1024 * 1024

	// SyntheticPNGDataBase64, SyntheticPDFDataBase64, and SyntheticBinaryDataBase64
	// supply compact valid base64 payloads substituted for captured images, PDF
	// documents, and other binary attachments while preserving wire MIME kinds.
	SyntheticPNGDataBase64    = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
	SyntheticPDFDataBase64    = "JVBERi0xLjQKMSAwIG9iago8PC9UeXBlL0NhdGFsb2cvUGFnZXMgMiAwIFI+PgplbmRvYmoKMiAwIG9iago8PC9UeXBlL1BhZ2VzL0NvdW50IDEvS2lkc1szIDAgUl0+PgplbmRvYmoKMyAwIG9iago8PC9UeXBlL1BhZ2UvUGFyZW50IDIgMCBSL01lZGlhQm94WzAgMCAxIDFdPj4KZW5kb2JqCnhyZWYKMCA0CjAwMDAwMDAwMDAgNjU1MzUgZiAKMDAwMDAwMDAwOSAwMDAwMCBuIAowMDAwMDAwMDUzIDAwMDAwIG4gCjAwMDAwMDAxMDQgMDAwMDAgbiAKdHJhaWxlcgo8PC9TaXplIDQvUm9vdCAxIDAgUj4+CnN0YXJ0eHJlZgoxNTUKJSVFT0YK"
	SyntheticBinaryDataBase64 = "dGVzdGtpdC1zeW50aGV0aWMtYmluYXJ5"

	// RedactedKeyPlaceholder, RedactedEmailPlaceholder, and RedactedHomePlaceholder
	// replace scrubbed secrets, email addresses, and user home directories.
	RedactedKeyPlaceholder   = "<REDACTED_KEY>"
	RedactedEmailPlaceholder = "user@example.com"
	RedactedHomePlaceholder  = "/home/user"
)

var (
	// ErrCorpusFileTooLarge and ErrCorpusTotalTooLarge classify single-file 512 KiB
	// overflows and cumulative 15 MiB corpus directory budget overflows.
	ErrCorpusFileTooLarge  = errors.New("corpus file exceeds 512 KiB limit; trim conversation history before importing")
	ErrCorpusTotalTooLarge = errors.New("total corpus size exceeds 15 MB limit")

	// allowlistedMetaHeaders lists the only four lowercase HTTP header names
	// permitted inside CorpusMeta.Headers when persisting capture metadata.
	allowlistedMetaHeaders = map[string]bool{
		"user-agent":        true,
		"anthropic-beta":    true,
		"anthropic-version": true,
		"openai-beta":       true,
	}

	// normalizedCredentialKeys lists lowercase separator-stripped JSON key names
	// whose string values must always be scrubbed and verified as credentials.
	normalizedCredentialKeys = map[string]bool{
		"apikey":        true,
		"xapikey":       true,
		"authorization": true,
		"accesstoken":   true,
		"refreshtoken":  true,
		"clientsecret":  true,
	}

	// secretValuePatterns match live API keys, bearer credentials, cloud access
	// keys, and PEM private key blocks during sanitization and verification.
	secretValuePatterns = buildSecretValuePatterns()

	// credentialFieldPattern matches JSON string properties in raw SSE or JSON
	// text whose key names an authorization header or credential secret.
	credentialFieldPattern = regexp.MustCompile(`(?i)("(?:x[-_]?api[-_]?key|api[-_]?key|authorization|access[-_]?token|refresh[-_]?token|client[-_]?secret)"\s*:\s*")([^"]+)(")`)

	// dataURLBase64Pattern matches inline base64 data URLs inside JSON strings
	// and SSE frames while capturing the declared MIME type in submatch one.
	dataURLBase64Pattern = regexp.MustCompile(`data:([a-zA-Z0-9.+/-]+);base64,[A-Za-z0-9+/=_-]{16,}`)

	// emailPattern matches standard RFC-like email addresses in captured text so
	// sanitization replaces developer addresses with RedactedEmailPlaceholder.
	emailPattern = regexp.MustCompile(`\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b`)

	// unixHomePattern, winSlashHomePattern, winBackslashHomePattern, and
	// winEscapedHomePattern match macOS, Linux, and Windows user home directory
	// prefixes in both decoded strings and raw JSON-escaped SSE text.
	unixHomePattern         = regexp.MustCompile(`(?:/Users|/home)/[A-Za-z0-9._-]+`)
	winSlashHomePattern     = regexp.MustCompile(`[A-Za-z]:/Users/[A-Za-z0-9._-]+`)
	winBackslashHomePattern = regexp.MustCompile(`[A-Za-z]:\\Users\\[A-Za-z0-9._-]+`)
	winEscapedHomePattern   = regexp.MustCompile(`[A-Za-z]:\\\\Users\\\\[A-Za-z0-9._-]+`)
)

// IncomingCapture describes a single raw capture bundle JSON file written to
// _incoming/ when recording a client request or upstream stream.
type IncomingCapture struct {
	Meta        CorpusMeta        `json:"meta"`                  // capture metadata and raw headers
	RawHeaders  map[string]string `json:"raw_headers,omitzero"`  // optional full request headers to filter
	Turn        int               `json:"turn,omitzero"`         // one-based turn number
	RequestBody jsontext.Value    `json:"request_body,omitzero"` // raw client request JSON
	UpstreamSSE string            `json:"upstream_sse,omitzero"` // raw upstream SSE text
}

// ImportReport summarizes destination file paths and verified byte totals after ImportCorpusDir.
type ImportReport struct {
	ImportedFiles []string // slash-separated paths written under destDir
	TotalBytes    int64    // verified byte count across destDir after import
}

// VerifyReport summarizes files and total bytes checked by VerifyCorpusDir.
type VerifyReport struct {
	CheckedFiles int   // number of non-ignored files validated
	TotalBytes   int64 // cumulative byte size across checked files
}

// stagedCorpusFile holds one sanitized file buffered in memory before ImportCorpusDir
// commits validated files to disk.
type stagedCorpusFile struct {
	relSlash  string // slash path relative to destCorpusDir
	destPath  string // filesystem destination path
	data      []byte // sanitized file contents
	isMeta    bool   // true for meta.json files
	isPayload bool   // true for *.request.json and *.stream.sse payloads
}

// buildSecretValuePatterns compiles the regular expression table for provider
// API keys (including sk-*, AIza*, AQ.*, and xai-* prefixes), bearer tokens,
// cloud access keys, and PEM private keys during package initialization.
func buildSecretValuePatterns() []*regexp.Regexp {
	return []*regexp.Regexp{
		regexp.MustCompile(`sk-ant-[A-Za-z0-9_-]{12,}`),
		regexp.MustCompile(`sk-proj-[A-Za-z0-9_-]{12,}`),
		regexp.MustCompile(`\bsk-[A-Za-z0-9_-]{20,}\b`),
		regexp.MustCompile(`\bAIza[0-9A-Za-z_-]{30,}\b`),
		regexp.MustCompile(`\bAQ\.[0-9A-Za-z_-]{20,}\b`),
		regexp.MustCompile(`\bxai-[A-Za-z0-9_-]{16,}\b`),
		regexp.MustCompile(`\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{20,}\b`),
		regexp.MustCompile(`\bglpat-[A-Za-z0-9_-]{16,}\b`),
		regexp.MustCompile(`\bAKIA[0-9A-Z]{16}\b`),
		regexp.MustCompile(`(?i)\bBearer\s+[A-Za-z0-9._~+/=-]{16,}`),
		regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----`),
	}
}

// isCredentialKey reports whether key matches a credential field name after
// lowercasing and removing '-' and '_' separators. It covers apiKey, api_key,
// x-api-key, accessToken, refreshToken, clientSecret, and authorization variants.
func isCredentialKey(key string) bool {
	if key == "" {
		return false
	}
	var b strings.Builder
	for i := range len(key) {
		c := key[i]
		if c == '-' || c == '_' {
			continue
		}
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		_ = b.WriteByte(c)
	}
	return normalizedCredentialKeys[b.String()]
}

// FilterMetaHeaders extracts only user-agent, anthropic-beta, anthropic-version,
// and openai-beta from rawHeaders in lowercase form with scrubbed values.
// Keys are processed in sorted order and empty results return nil.
func FilterMetaHeaders(rawHeaders map[string]string) map[string]string {
	if len(rawHeaders) == 0 {
		return nil
	}
	keys := make([]string, 0, len(rawHeaders))
	for k := range rawHeaders {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	var filtered map[string]string
	for _, k := range keys {
		lower := strings.ToLower(strings.TrimSpace(k))
		if !allowlistedMetaHeaders[lower] {
			continue
		}
		trimmed := strings.TrimSpace(scrubString(rawHeaders[k]))
		if trimmed == "" {
			continue
		}
		if filtered == nil {
			filtered = make(map[string]string)
		}
		filtered[lower] = trimmed
	}
	return filtered
}

// SanitizeJSONPayload replaces inline base64 attachments with compact
// MIME-preserving synthetic payloads, scrubs secrets, emails, and home paths
// across all JSON strings, and enforces MaxCorpusFileBytes. Payloads exceeding
// 512 KiB after sanitization return ErrCorpusFileTooLarge.
func SanitizeJSONPayload(raw []byte) ([]byte, error) {
	node, err := parseJSONNode(raw, true)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON payload: %w", err)
	}
	sanitizeJSONNode(&node, "")

	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true), jsontext.WithIndent("  "))
	if err := writeSanitizedNode(enc, node); err != nil {
		return nil, err
	}
	out := buf.Bytes()
	if len(out) == 0 || out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	if len(out) > MaxCorpusFileBytes {
		return nil, fmt.Errorf("%w (%d bytes)", ErrCorpusFileTooLarge, len(out))
	}
	return out, nil
}

// SanitizeSSEPayload scrubs secrets, emails, home paths, and inline base64 media
// inside SSE lines while preserving original CRLF or LF line endings and
// trailing whitespace. Streams exceeding MaxCorpusFileBytes after sanitization
// return ErrCorpusFileTooLarge without mutating raw.
func SanitizeSSEPayload(raw []byte) ([]byte, error) {
	text := scrubString(string(raw))
	text = replaceDataURLBase64(text)
	// Replace long base64 payloads inside Gemini inlineData or Anthropic source
	// JSON lines while keeping surrounding CRLF/LF framing untouched for SSE tests.
	text = replaceInlineJSONBase64InSSE(text)
	out := []byte(text)
	if len(out) > MaxCorpusFileBytes {
		return nil, fmt.Errorf("%w (%d bytes)", ErrCorpusFileTooLarge, len(out))
	}
	return out, nil
}

// DetectSecretViolations scans content and returns human-readable descriptions
// of any unredacted secret value or credential JSON field found. Valid JSON
// payloads are also walked structurally using isCredentialKey.
func DetectSecretViolations(content []byte) []string {
	var findings []string
	for _, pat := range secretValuePatterns {
		if loc := pat.FindIndex(content); loc != nil {
			sample := string(content[loc[0]:min(loc[1], loc[0]+12)]) + "..."
			findings = append(findings, fmt.Sprintf("matched secret pattern %q (%s)", pat.String(), sample))
		}
	}
	if matches := credentialFieldPattern.FindAllSubmatch(content, -1); len(matches) > 0 {
		for _, m := range matches {
			val := string(m[2])
			if val != RedactedKeyPlaceholder {
				findings = append(findings, fmt.Sprintf("unredacted credential field %q", string(m[1])))
			}
		}
	}
	if node, err := parseJSONNode(content, true); err == nil {
		detectJSONCredentialViolations(node, "$", &findings)
	}
	return findings
}

// detectJSONCredentialViolations recursively inspects parsed JSON objects with
// isCredentialKey so camelCase keys or escaped values cannot bypass regex checks.
func detectJSONCredentialViolations(n jsonNode, path string, findings *[]string) {
	switch n.kind {
	case nodeArray:
		for i, elem := range n.arrVal {
			detectJSONCredentialViolations(elem, fmt.Sprintf("%s[%d]", path, i), findings)
		}
	case nodeObject:
		for _, k := range n.objKeys {
			val := n.objVal[k]
			childPath := appendJSONMemberPath(path, k)
			if isCredentialKey(k) && val.kind == nodeString && val.strVal != "" && val.strVal != RedactedKeyPlaceholder {
				*findings = append(*findings, fmt.Sprintf("unredacted credential field at %s", childPath))
			}
			detectJSONCredentialViolations(val, childPath, findings)
		}
	}
}

// ImportCorpusDir reads raw captures from incomingDir, stages and verifies all
// sanitized files and size budgets in memory before touching destCorpusDir, and
// writes canonical files only after every check succeeds. Any malformed capture,
// oversized payload, or total-budget overflow aborts with zero partial writes.
func ImportCorpusDir(incomingDir, destCorpusDir string) (ImportReport, error) {
	info, err := os.Stat(incomingDir)
	if err != nil {
		return ImportReport{}, fmt.Errorf("stat incoming directory %s: %w", incomingDir, err)
	}
	if !info.IsDir() {
		return ImportReport{}, fmt.Errorf("incoming path %s is not a directory", incomingDir)
	}

	stagedByRel := make(map[string]stagedCorpusFile)
	var payloadPaths []string

	err = filepath.WalkDir(incomingDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if p != incomingDir && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		name := d.Name()
		switch {
		case strings.HasSuffix(name, ".capture.json"):
			files, err := stageCaptureBundleFile(p, destCorpusDir)
			if err != nil {
				return fmt.Errorf("import %s: %w", p, err)
			}
			for _, f := range files {
				stagedByRel[f.relSlash] = f
				payloadPaths = append(payloadPaths, filepath.ToSlash(f.destPath))
			}
		case strings.HasSuffix(name, ".request.json"):
			files, err := stageStructuredFile(incomingDir, p, destCorpusDir, true)
			if err != nil {
				return fmt.Errorf("import %s: %w", p, err)
			}
			for _, f := range files {
				stagedByRel[f.relSlash] = f
				if f.isPayload {
					payloadPaths = append(payloadPaths, filepath.ToSlash(f.destPath))
				}
			}
		case strings.HasSuffix(name, ".stream.sse"):
			files, err := stageStructuredFile(incomingDir, p, destCorpusDir, false)
			if err != nil {
				return fmt.Errorf("import %s: %w", p, err)
			}
			for _, f := range files {
				stagedByRel[f.relSlash] = f
				if f.isPayload {
					payloadPaths = append(payloadPaths, filepath.ToSlash(f.destPath))
				}
			}
		}
		return nil
	})
	if err != nil {
		return ImportReport{}, err
	}

	if err := validateStagedCorpusWrites(destCorpusDir, stagedByRel); err != nil {
		return ImportReport{}, err
	}

	relKeys := make([]string, 0, len(stagedByRel))
	for k := range stagedByRel {
		relKeys = append(relKeys, k)
	}
	slices.Sort(relKeys)
	for _, k := range relKeys {
		item := stagedByRel[k]
		if err := os.MkdirAll(filepath.Dir(item.destPath), 0o755); err != nil {
			return ImportReport{}, err
		}
		if err := os.WriteFile(item.destPath, item.data, 0o644); err != nil {
			return ImportReport{}, err
		}
	}

	slices.Sort(payloadPaths)
	payloadPaths = slices.Compact(payloadPaths)
	verifyRep, err := VerifyCorpusDir(destCorpusDir)
	if err != nil {
		return ImportReport{}, err
	}
	return ImportReport{
		ImportedFiles: payloadPaths,
		TotalBytes:    verifyRep.TotalBytes,
	}, nil
}

// validateStagedCorpusWrites checks every staged file's secret, header, and
// 512 KiB size invariants before any disk write occurs. It also verifies that
// non-overwritten files in destCorpusDir plus staged files fit within 15 MiB.
func validateStagedCorpusWrites(destCorpusDir string, staged map[string]stagedCorpusFile) error {
	var stagedTotal int64
	relKeys := make([]string, 0, len(staged))
	for k := range staged {
		relKeys = append(relKeys, k)
	}
	slices.Sort(relKeys)

	for _, k := range relKeys {
		f := staged[k]
		if len(f.data) > MaxCorpusFileBytes {
			return fmt.Errorf("%s: %w (%d bytes)", f.relSlash, ErrCorpusFileTooLarge, len(f.data))
		}
		if findings := DetectSecretViolations(f.data); len(findings) > 0 {
			return fmt.Errorf("%s: secret check failed: %s", f.relSlash, strings.Join(findings, "; "))
		}
		if f.isMeta {
			if err := verifyMetaBytes(f.data); err != nil {
				return fmt.Errorf("%s: %w", f.relSlash, err)
			}
		}
		stagedTotal += int64(len(f.data))
	}

	var existingKeptBytes int64
	if _, statErr := os.Stat(destCorpusDir); statErr == nil {
		walkErr := filepath.WalkDir(destCorpusDir, func(p string, d fs.DirEntry, wErr error) error {
			if wErr != nil {
				return wErr
			}
			if d.IsDir() {
				if p != destCorpusDir && (d.Name() == "_incoming" || strings.HasPrefix(d.Name(), ".")) {
					return filepath.SkipDir
				}
				return nil
			}
			if strings.HasPrefix(d.Name(), ".") {
				return nil
			}
			rel, err := filepath.Rel(destCorpusDir, p)
			if err != nil {
				return err
			}
			relSlash := filepath.ToSlash(rel)
			if _, overwritten := staged[relSlash]; overwritten {
				return nil
			}
			info, err := d.Info()
			if err != nil {
				return err
			}
			existingKeptBytes += info.Size()
			return nil
		})
		if walkErr != nil {
			return walkErr
		}
	}

	if existingKeptBytes+stagedTotal > MaxCorpusTotalBytes {
		return fmt.Errorf("%w (%d bytes)", ErrCorpusTotalTooLarge, existingKeptBytes+stagedTotal)
	}
	return nil
}

// VerifyCorpusDir validates that corpusDir contains no .go files, no unredacted
// secrets, no disallowed header keys in meta.json, no single file above 512 KiB,
// and no more than 15 MiB in total. Missing corpusDir returns a zero report.
func VerifyCorpusDir(corpusDir string) (VerifyReport, error) {
	if _, err := os.Stat(corpusDir); err != nil {
		// Return an empty report when testdata/corpus has not been created yet so
		// early verification runs succeed before clientlab captures are imported.
		if errors.Is(err, os.ErrNotExist) {
			return VerifyReport{}, nil
		}
		return VerifyReport{}, err
	}

	var report VerifyReport
	var violations []string
	recordingDirs := make(map[string]bool)

	err := filepath.WalkDir(corpusDir, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(corpusDir, p)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		if d.IsDir() {
			if p != corpusDir && (d.Name() == "_incoming" || strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasPrefix(d.Name(), ".") {
			return nil
		}
		if strings.HasSuffix(d.Name(), ".go") {
			violations = append(violations, fmt.Sprintf("%s: .go files are forbidden inside testdata", relSlash))
			return nil
		}

		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		report.CheckedFiles++
		report.TotalBytes += int64(len(data))
		if len(data) > MaxCorpusFileBytes {
			violations = append(violations, fmt.Sprintf("%s: size %d exceeds %d bytes", relSlash, len(data), MaxCorpusFileBytes))
		}
		for _, finding := range DetectSecretViolations(data) {
			violations = append(violations, fmt.Sprintf("%s: %s", relSlash, finding))
		}

		if strings.HasSuffix(d.Name(), ".request.json") || strings.HasSuffix(d.Name(), ".stream.sse") {
			recordingDirs[filepath.Dir(p)] = true
		}
		if d.Name() == "meta.json" {
			if err := verifyMetaBytes(data); err != nil {
				violations = append(violations, fmt.Sprintf("%s: %v", relSlash, err))
			}
		} else if strings.HasSuffix(d.Name(), ".json") {
			if _, err := parseJSONNode(data, true); err != nil {
				violations = append(violations, fmt.Sprintf("%s: invalid JSON: %v", relSlash, err))
			}
		}
		return nil
	})
	if err != nil {
		return report, err
	}

	dirs := make([]string, 0, len(recordingDirs))
	for dir := range recordingDirs {
		dirs = append(dirs, dir)
	}
	slices.Sort(dirs)
	for _, dir := range dirs {
		metaPath := filepath.Join(dir, "meta.json")
		if _, err := os.Stat(metaPath); err != nil {
			relDir, _ := filepath.Rel(corpusDir, dir)
			violations = append(violations, fmt.Sprintf("%s: missing required meta.json", filepath.ToSlash(relDir)))
		}
	}
	if report.TotalBytes > MaxCorpusTotalBytes {
		violations = append(violations, fmt.Sprintf("total corpus size %d exceeds %d bytes (%v)", report.TotalBytes, MaxCorpusTotalBytes, ErrCorpusTotalTooLarge))
	}
	if len(violations) > 0 {
		slices.Sort(violations)
		return report, errors.New("corpus verification failed:\n  - " + strings.Join(violations, "\n  - "))
	}
	return report, nil
}

// verifyMetaBytes checks required fields and header allowlist compliance on
// meta.json bytes. Disallowed or uppercase header keys fail deterministically.
func verifyMetaBytes(data []byte) error {
	meta, err := parseCorpusMeta(data)
	if err != nil {
		return err
	}
	if meta.Protocol == "" {
		return errors.New("meta.json missing protocol")
	}
	if meta.Client == "" && meta.Provider == "" {
		return errors.New("meta.json must specify client or provider")
	}
	headerKeys := make([]string, 0, len(meta.Headers))
	for k := range meta.Headers {
		headerKeys = append(headerKeys, k)
	}
	slices.Sort(headerKeys)
	for _, k := range headerKeys {
		if !allowlistedMetaHeaders[strings.ToLower(k)] || k != strings.ToLower(k) {
			return fmt.Errorf("disallowed or non-lowercase header %q in meta.json", k)
		}
	}
	return nil
}

// stageCaptureBundleFile sanitizes one *.capture.json bundle into in-memory
// stagedCorpusFile entries for clients/ and upstreams/ without writing to disk.
func stageCaptureBundleFile(bundlePath, destCorpusDir string) ([]stagedCorpusFile, error) {
	raw, err := os.ReadFile(bundlePath)
	if err != nil {
		return nil, err
	}
	var capBundle IncomingCapture
	if err := json.Unmarshal(raw, &capBundle, jsontext.AllowInvalidUTF8(true)); err != nil {
		return nil, err
	}
	turn := max(capBundle.Turn, 1)
	meta := sanitizeMeta(capBundle.Meta, capBundle.RawHeaders)
	if meta.Protocol == "" {
		return nil, errors.New("capture meta.protocol is required")
	}
	scenario := cleanPathSegment(meta.Scenario, "default")

	var out []stagedCorpusFile
	if len(capBundle.RequestBody) > 0 {
		client := cleanPathSegment(meta.Client, "unknown-client")
		version := cleanPathSegment(meta.Version, "0.0.0")
		clientMeta := meta
		clientMeta.Client = client
		clientMeta.Version = version
		clientMeta.Scenario = scenario
		sanitizedReq, err := SanitizeJSONPayload(capBundle.RequestBody)
		if err != nil {
			return nil, err
		}
		metaBytes, err := MarshalDeterministicIndent(clientMeta)
		if err != nil {
			return nil, err
		}
		relDir := filepath.ToSlash(filepath.Join("clients", client, version, scenario))
		reqRel := relDir + fmt.Sprintf("/%d.request.json", turn)
		metaRel := relDir + "/meta.json"
		out = append(out,
			stagedCorpusFile{
				relSlash:  reqRel,
				destPath:  filepath.Join(destCorpusDir, filepath.FromSlash(reqRel)),
				data:      sanitizedReq,
				isPayload: true,
			},
			stagedCorpusFile{
				relSlash: metaRel,
				destPath: filepath.Join(destCorpusDir, filepath.FromSlash(metaRel)),
				data:     metaBytes,
				isMeta:   true,
			},
		)
	}

	if capBundle.UpstreamSSE != "" {
		provider := cleanPathSegment(meta.Provider, "unknown-provider")
		model := cleanPathSegment(meta.Model, "unknown-model")
		upMeta := meta
		upMeta.Provider = provider
		upMeta.Model = model
		upMeta.Scenario = scenario
		upMeta.Stream = true
		sanitizedSSE, err := SanitizeSSEPayload([]byte(capBundle.UpstreamSSE))
		if err != nil {
			return nil, err
		}
		metaBytes, err := MarshalDeterministicIndent(upMeta)
		if err != nil {
			return nil, err
		}
		relDir := filepath.ToSlash(filepath.Join("upstreams", provider, model, scenario))
		sseRel := relDir + fmt.Sprintf("/%d.stream.sse", turn)
		metaRel := relDir + "/meta.json"
		out = append(out,
			stagedCorpusFile{
				relSlash:  sseRel,
				destPath:  filepath.Join(destCorpusDir, filepath.FromSlash(sseRel)),
				data:      sanitizedSSE,
				isPayload: true,
			},
			stagedCorpusFile{
				relSlash: metaRel,
				destPath: filepath.Join(destCorpusDir, filepath.FromSlash(metaRel)),
				data:     metaBytes,
				isMeta:   true,
			},
		)
	}
	return out, nil
}

// stageStructuredFile sanitizes a pre-laid-out clients/.../*.request.json or
// upstreams/.../*.stream.sse file alongside its sibling meta.json in memory.
func stageStructuredFile(incomingRoot, srcFile, destCorpusDir string, isRequest bool) ([]stagedCorpusFile, error) {
	rel, err := filepath.Rel(incomingRoot, srcFile)
	if err != nil {
		return nil, err
	}
	relSlash := filepath.ToSlash(rel)
	parts := strings.Split(relSlash, "/")
	expectedPrefix := "upstreams"
	if isRequest {
		expectedPrefix = "clients"
	}
	if len(parts) != 5 || parts[0] != expectedPrefix {
		return nil, fmt.Errorf("structured file %s must match %s/<owner>/<ver_or_model>/<scenario>/<file>", relSlash, expectedPrefix)
	}

	metaSrc := filepath.Join(filepath.Dir(srcFile), "meta.json")
	metaRaw, err := os.ReadFile(metaSrc)
	if err != nil {
		return nil, fmt.Errorf("read sibling meta.json: %w", err)
	}
	var rawMeta CorpusMeta
	if err := json.Unmarshal(metaRaw, &rawMeta); err != nil {
		return nil, fmt.Errorf("parse sibling meta.json: %w", err)
	}
	cleanMeta := sanitizeMeta(rawMeta, nil)
	if isRequest {
		if cleanMeta.Client == "" {
			cleanMeta.Client = parts[1]
		}
		if cleanMeta.Version == "" {
			cleanMeta.Version = parts[2]
		}
	} else {
		if cleanMeta.Provider == "" {
			cleanMeta.Provider = parts[1]
		}
		if cleanMeta.Model == "" {
			cleanMeta.Model = parts[2]
		}
	}
	if cleanMeta.Scenario == "" {
		cleanMeta.Scenario = parts[3]
	}

	rawData, err := os.ReadFile(srcFile)
	if err != nil {
		return nil, err
	}
	var sanitized []byte
	if isRequest {
		sanitized, err = SanitizeJSONPayload(rawData)
	} else {
		sanitized, err = SanitizeSSEPayload(rawData)
	}
	if err != nil {
		return nil, err
	}
	metaBytes, err := MarshalDeterministicIndent(cleanMeta)
	if err != nil {
		return nil, err
	}

	metaRel := strings.Join(parts[:4], "/") + "/meta.json"
	return []stagedCorpusFile{
		{
			relSlash:  relSlash,
			destPath:  filepath.Join(destCorpusDir, filepath.FromSlash(relSlash)),
			data:      sanitized,
			isPayload: true,
		},
		{
			relSlash: metaRel,
			destPath: filepath.Join(destCorpusDir, filepath.FromSlash(metaRel)),
			data:     metaBytes,
			isMeta:   true,
		},
	}, nil
}

// sanitizeMeta canonicalizes protocol IDs, scrubs notes, and filters headers.
func sanitizeMeta(meta CorpusMeta, extraHeaders map[string]string) CorpusMeta {
	if canonical, ok := ParseProtocolDirName(string(meta.Protocol)); ok {
		meta.Protocol = canonical
	}
	merged := make(map[string]string, len(meta.Headers)+len(extraHeaders))
	maps.Copy(merged, extraHeaders)
	maps.Copy(merged, meta.Headers)
	meta.Headers = FilterMetaHeaders(merged)
	meta.Notes = scrubString(meta.Notes)
	return meta
}

// cleanPathSegment restricts directory components to safe path characters.
func cleanPathSegment(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	var b strings.Builder
	for i := range len(trimmed) {
		c := trimmed[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			_ = b.WriteByte(c)
		} else {
			_ = b.WriteByte('_')
		}
	}
	res := strings.Trim(b.String(), ".")
	if res == "" {
		return fallback
	}
	return res
}

// sanitizeJSONNode recursively scrubs string fields and replaces inline base64
// media objects inside n using isCredentialKey for credential property matching.
func sanitizeJSONNode(n *jsonNode, parentKey string) {
	switch n.kind {
	case nodeString:
		if isCredentialKey(parentKey) {
			n.strVal = RedactedKeyPlaceholder
			return
		}
		s := replaceDataURLBase64(n.strVal)
		n.strVal = scrubString(s)
	case nodeArray:
		for i := range n.arrVal {
			sanitizeJSONNode(&n.arrVal[i], parentKey)
		}
	case nodeObject:
		replaceObjectBase64Media(n.objVal)
		for k, v := range n.objVal {
			sanitizeJSONNode(&v, k)
			n.objVal[k] = v
		}
	}
}

// syntheticReplacementForMIME returns the normalized MIME string and compact
// valid base64 payload appropriate for origMIME. Image inputs map to a 1x1 PNG,
// while application/pdf and other explicit MIMEs preserve their declared type.
func syntheticReplacementForMIME(origMIME string) (string, string) {
	trimmed := strings.TrimSpace(origMIME)
	lower := strings.ToLower(trimmed)
	switch {
	case lower == "" || strings.HasPrefix(lower, "image/"):
		return "image/png", SyntheticPNGDataBase64
	case lower == "application/pdf":
		return trimmed, SyntheticPDFDataBase64
	default:
		return trimmed, SyntheticBinaryDataBase64
	}
}

// replaceDataURLBase64 substitutes compact MIME-appropriate synthetic base64
// payloads into data:<mime>;base64,... URLs while preserving PDF and non-image MIMEs.
func replaceDataURLBase64(s string) string {
	return dataURLBase64Pattern.ReplaceAllStringFunc(s, func(match string) string {
		sub := dataURLBase64Pattern.FindStringSubmatch(match)
		if len(sub) < 2 {
			return "data:image/png;base64," + SyntheticPNGDataBase64
		}
		newMIME, payload := syntheticReplacementForMIME(sub[1])
		return "data:" + newMIME + ";base64," + payload
	})
}

// replaceObjectBase64Media shrinks inline base64 attachments inside Anthropic,
// Gemini, and OpenAI media objects in place. Image blocks normalize to
// image/png while PDF documents and audio clips retain valid MIME metadata.
func replaceObjectBase64Media(obj map[string]jsonNode) {
	// Anthropic Messages image and document blocks encode inline bytes under
	// source{type:"base64", media_type, data}; preserve application/pdf on documents.
	if typ, ok := obj["type"]; ok && typ.kind == nodeString && typ.strVal == "base64" {
		if dataNode, hasData := obj["data"]; hasData && dataNode.kind == nodeString && dataNode.strVal != "" {
			origMIME, hasMT, _ := nodeStringField(obj, "media_type")
			newMIME, payload := syntheticReplacementForMIME(origMIME)
			obj["data"] = jsonNode{kind: nodeString, strVal: payload}
			if hasMT {
				obj["media_type"] = jsonNode{kind: nodeString, strVal: newMIME}
			}
			return
		}
	}
	// Gemini inlineData{mimeType, data} carries images, PDFs, or audio clips;
	// shrink the base64 blob while preserving non-image mimeType values.
	if mimeVal, hasMime, okMime := nodeStringField(obj, "mimeType"); hasMime && okMime {
		if dataNode, hasData := obj["data"]; hasData && dataNode.kind == nodeString && dataNode.strVal != "" {
			newMIME, payload := syntheticReplacementForMIME(mimeVal)
			obj["data"] = jsonNode{kind: nodeString, strVal: payload}
			obj["mimeType"] = jsonNode{kind: nodeString, strVal: newMIME}
			return
		}
	}
	// OpenAI Chat input_audio{data, format} embeds raw base64 audio without a
	// data: prefix; replace long audio clips so fixtures fit the 512 KiB ceiling.
	if _, hasFmt := obj["format"]; hasFmt {
		if dataNode, hasData := obj["data"]; hasData && dataNode.kind == nodeString && len(dataNode.strVal) > 64 {
			obj["data"] = jsonNode{kind: nodeString, strVal: SyntheticBinaryDataBase64}
		}
	}
	// OpenAI Chat and Responses file inputs can pass bare base64 in file_data
	// instead of a data: URL; replace large payloads with compact valid PDF bytes.
	if fd, hasFD := obj["file_data"]; hasFD && fd.kind == nodeString && !strings.HasPrefix(fd.strVal, "data:") && len(fd.strVal) > 64 {
		if _, err := base64.StdEncoding.DecodeString(SyntheticPDFDataBase64); err == nil {
			obj["file_data"] = jsonNode{kind: nodeString, strVal: SyntheticPDFDataBase64}
		}
	}
}

// replaceInlineJSONBase64InSSE shrinks base64 data fields inside Gemini and
// Anthropic SSE JSON events. Non-image application/pdf payloads substitute
// SyntheticPDFDataBase64 while leaving surrounding line endings unchanged.
func replaceInlineJSONBase64InSSE(text string) string {
	inlinePattern := regexp.MustCompile(`("(?:inlineData|source)"\s*:\s*\{[^{}]*?"data"\s*:\s*")([A-Za-z0-9+/=]{96,})(")`)
	return inlinePattern.ReplaceAllStringFunc(text, func(segment string) string {
		lower := strings.ToLower(segment)
		replacement := SyntheticPNGDataBase64
		if strings.Contains(lower, "application/pdf") {
			replacement = SyntheticPDFDataBase64
		}
		return inlinePattern.ReplaceAllString(segment, "${1}"+replacement+"${3}")
	})
}

// scrubString applies secret, email, and user-home regex replacements to s.
func scrubString(s string) string {
	out := credentialFieldPattern.ReplaceAllString(s, "${1}"+RedactedKeyPlaceholder+"${3}")
	for _, pat := range secretValuePatterns {
		out = pat.ReplaceAllString(out, RedactedKeyPlaceholder)
	}
	out = emailPattern.ReplaceAllString(out, RedactedEmailPlaceholder)
	out = unixHomePattern.ReplaceAllString(out, RedactedHomePlaceholder)
	out = winSlashHomePattern.ReplaceAllString(out, RedactedHomePlaceholder)
	out = winEscapedHomePattern.ReplaceAllString(out, RedactedHomePlaceholder)
	out = winBackslashHomePattern.ReplaceAllString(out, RedactedHomePlaceholder)
	return out
}

// writeSanitizedNode encodes n preserving original numeric token spellings where
// available while sorting object keys deterministically.
func writeSanitizedNode(enc *jsontext.Encoder, n jsonNode) error {
	switch n.kind {
	case nodeNumber:
		if n.numVal.raw != "" {
			return enc.WriteValue(jsontext.Value(n.numVal.raw))
		}
		return enc.WriteValue(jsontext.Value(n.numVal.canonicalString()))
	case nodeArray:
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for _, elem := range n.arrVal {
			if err := writeSanitizedNode(enc, elem); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndArray)
	case nodeObject:
		if err := enc.WriteToken(jsontext.BeginObject); err != nil {
			return err
		}
		keys := make([]string, 0, len(n.objVal))
		for k := range n.objVal {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		for _, k := range keys {
			if err := enc.WriteToken(jsontext.String(k)); err != nil {
				return err
			}
			if err := writeSanitizedNode(enc, n.objVal[k]); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndObject)
	default:
		return writeCanonicalNode(enc, n)
	}
}
