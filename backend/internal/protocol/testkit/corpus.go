package testkit

import (
	"bytes"
	"embed"
	"encoding/json/v2"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// corpus.go embeds testkit/testdata and indexes hand-written wire fixtures and
// sanitized client/upstream traffic recordings by protocol, fixture kind,
// scenario, and metadata tags. Pure packages under internal/protocol query
// fixtures and source files through these helpers instead of importing os or
// embed directly.
//
// Corpus accepts any fs.FS so unit tests can exercise indexing against in-memory
// filesystems while DefaultCorpus serves the compiled-in testdata tree. Missing
// optional subdirectories return empty slices rather than errors so early stages
// work cleanly before every fixture category is populated.

const (
	// FixtureKind constants classify whether a fixture holds a client request,
	// an SSE event stream, or a buffered JSON response.
	FixtureRequest  FixtureKind = "request"  // client request JSON payload
	FixtureStream   FixtureKind = "stream"   // upstream or client SSE byte stream
	FixtureResponse FixtureKind = "response" // non-streaming response JSON payload

	// FixtureSource constants distinguish hand-written wire fixtures from
	// recorded client requests and recorded upstream streams.
	SourceWire           FixtureSource = "wire"            // testdata/wire/<protocol>/<kind>/*
	SourceCorpusClient   FixtureSource = "corpus_client"   // testdata/corpus/clients/...
	SourceCorpusUpstream FixtureSource = "corpus_upstream" // testdata/corpus/upstreams/...
)

var (
	// embeddedTestdata bundles the testdata directory tree into the testkit
	// binary so pure-package tests can load fixtures without filesystem paths.
	//go:embed testdata
	embeddedTestdata embed.FS

	// defaultCorpusOnce and defaultCorpusInst cache the lazily rooted testdata
	// Corpus shared by package-level query helpers.
	defaultCorpusOnce sync.Once
	defaultCorpusInst *Corpus
)

// FixtureKind identifies the payload direction and framing of a Fixture.
type FixtureKind string

// FixtureSource identifies which subtree of testdata owns a Fixture.
type FixtureSource string

// CorpusMeta holds provenance, protocol, tag, and allowlisted header metadata
// stored in a recording directory's meta.json file.
type CorpusMeta struct {
	Protocol   protocol.ProtocolID `json:"protocol"`             // wire protocol identifier
	Client     string              `json:"client,omitzero"`      // recorded client binary name
	Provider   string              `json:"provider,omitzero"`    // recorded upstream provider name
	Version    string              `json:"version,omitzero"`     // client version string
	Model      string              `json:"model,omitzero"`       // upstream model identifier
	CapturedAt string              `json:"captured_at,omitzero"` // capture date YYYY-MM-DD or timestamp
	Stream     bool                `json:"stream,omitzero"`      // whether exchange used streaming SSE
	Scenario   string              `json:"scenario,omitzero"`    // recorded scenario slug
	Tags       []string            `json:"tags,omitzero"`        // optional filter tags
	Headers    map[string]string   `json:"headers,omitzero"`     // allowlisted request metadata headers
	Notes      string              `json:"notes,omitzero"`       // human-authored capture notes
}

// Fixture represents one loaded request, stream, or response payload together
// with its resolved metadata.
type Fixture struct {
	Name     string              // stable display name within its source tree
	Path     string              // slash-separated path relative to testdata/
	Source   FixtureSource       // owning testdata subtree
	Protocol protocol.ProtocolID // wire protocol of Data
	Kind     FixtureKind         // request, stream, or response
	Scenario string              // scenario name derived from filename or directory
	Tags     []string            // merged tags from Meta and fixture location
	Meta     *CorpusMeta         // parsed directory meta.json when present
	Data     []byte              // raw fixture bytes
}

// FixtureQuery filters fixtures returned by Corpus.QueryFixtures. Zero-valued
// fields match all entries, while Tags requires every listed tag to appear.
type FixtureQuery struct {
	Protocol protocol.ProtocolID // exact protocol filter when non-empty
	Kind     FixtureKind         // exact kind filter when non-empty
	Source   FixtureSource       // exact source filter when non-empty
	Scenario string              // exact scenario slug when non-empty
	Client   string              // CorpusMeta.Client or directory client segment
	Provider string              // CorpusMeta.Provider or directory provider segment
	Tags     []string            // required tag subset
}

// GoSourceFile holds the path and raw bytes of one handwritten production Go
// source file loaded for static anti-abuse checks in pure test suites.
type GoSourceFile struct {
	Path string // slash-separated file path
	Data []byte // raw Go source bytes
}

// Corpus indexes wire fixtures, recorded traffic, and scenarios inside an fs.FS
// rooted at the testdata directory.
type Corpus struct {
	fsys fs.FS // filesystem rooted at testdata/
}

// DefaultCorpus returns the shared Corpus backed by embedded testkit/testdata.
func DefaultCorpus() *Corpus {
	defaultCorpusOnce.Do(func() {
		sub, err := fs.Sub(embeddedTestdata, "testdata")
		if err != nil {
			panic(fmt.Sprintf("testkit: sub embedded testdata: %v", err))
		}
		defaultCorpusInst = NewCorpus(sub)
	})
	return defaultCorpusInst
}

// NewCorpus constructs a Corpus reading from fsys, which callers root at a
// testdata-compatible directory layout containing wire/, corpus/, or scenarios/.
// Subsequent fixture queries and file reads traverse fsys without mutating it.
func NewCorpus(fsys fs.FS) *Corpus {
	return &Corpus{fsys: fsys}
}

// FS returns the read-only filesystem backing this Corpus instance.
func (c *Corpus) FS() fs.FS {
	return c.fsys
}

// ReadFile returns an independent cloned byte slice for relPath inside c's
// testdata root. Empty paths and parent-directory traversal segments return
// errors before reading from the underlying filesystem.
func (c *Corpus) ReadFile(relPath string) ([]byte, error) {
	cleaned := path.Clean(strings.TrimPrefix(filepath.ToSlash(relPath), "/"))
	if cleaned == "." || cleaned == ".." || strings.HasPrefix(cleaned, "../") {
		return nil, fmt.Errorf("invalid testdata path %q", relPath)
	}
	data, err := fs.ReadFile(c.fsys, cleaned)
	if err != nil {
		return nil, err
	}
	return bytes.Clone(data), nil
}

// QueryFixtures scans wire and corpus subtrees of c and returns all fixtures
// matching q in deterministic lexical path order. Zero-valued fields on q act
// as wildcards, while q.Tags requires every requested tag to appear on each match.
func (c *Corpus) QueryFixtures(q FixtureQuery) ([]Fixture, error) {
	var all []Fixture
	if q.Source == "" || q.Source == SourceWire {
		wireItems, err := c.scanWireFixtures()
		if err != nil {
			return nil, err
		}
		all = append(all, wireItems...)
	}
	if q.Source == "" || q.Source == SourceCorpusClient {
		clientItems, err := c.scanCorpusClients()
		if err != nil {
			return nil, err
		}
		all = append(all, clientItems...)
	}
	if q.Source == "" || q.Source == SourceCorpusUpstream {
		upstreamItems, err := c.scanCorpusUpstreams()
		if err != nil {
			return nil, err
		}
		all = append(all, upstreamItems...)
	}

	filtered := make([]Fixture, 0, len(all))
	for _, item := range all {
		if matchesQuery(item, q) {
			filtered = append(filtered, item)
		}
	}
	slices.SortFunc(filtered, func(a, b Fixture) int {
		return strings.Compare(a.Path, b.Path)
	})
	return filtered, nil
}

// Frames parses f.Data with sse.NewReader when f holds a stream fixture and
// returns all decoded frames. Request and response fixtures return an error
// rather than attempting SSE line scanning on JSON documents.
func (f Fixture) Frames() ([]sse.Frame, error) {
	if f.Kind != FixtureStream {
		return nil, fmt.Errorf("fixture %s has kind %q, want %q", f.Path, f.Kind, FixtureStream)
	}
	return ParseSSEBytes(f.Data)
}

// ParseSSEBytes scans raw through sse.NewReader and returns every emitted Frame
// in arrival order. Oversized frames or underlying reader errors return nil
// alongside the scanner error.
func ParseSSEBytes(raw []byte) ([]sse.Frame, error) {
	rd := sse.NewReader(bytes.NewReader(raw))
	var frames []sse.Frame
	for rd.Scan() {
		frames = append(frames, rd.Frame())
	}
	if err := rd.Err(); err != nil {
		return nil, err
	}
	return frames, nil
}

// WireFixtures queries DefaultCorpus for hand-written wire fixtures matching
// proto, kind, and all optional tags in deterministic lexical path order.
// Filesystem or metadata parsing failures inside DefaultCorpus return an error.
func WireFixtures(proto protocol.ProtocolID, kind FixtureKind, tags ...string) ([]Fixture, error) {
	return DefaultCorpus().QueryFixtures(FixtureQuery{
		Protocol: proto,
		Kind:     kind,
		Source:   SourceWire,
		Tags:     tags,
	})
}

// CorpusRequests queries DefaultCorpus for sanitized client request recordings
// matching proto and all optional tags in deterministic lexical path order.
// Malformed capture metadata or read failures return an error.
func CorpusRequests(proto protocol.ProtocolID, tags ...string) ([]Fixture, error) {
	return DefaultCorpus().QueryFixtures(FixtureQuery{
		Protocol: proto,
		Kind:     FixtureRequest,
		Source:   SourceCorpusClient,
		Tags:     tags,
	})
}

// CorpusStreams queries DefaultCorpus for sanitized upstream stream recordings
// matching proto and all optional tags in deterministic lexical path order.
// Malformed capture metadata or read failures return an error.
func CorpusStreams(proto protocol.ProtocolID, tags ...string) ([]Fixture, error) {
	return DefaultCorpus().QueryFixtures(FixtureQuery{
		Protocol: proto,
		Kind:     FixtureStream,
		Source:   SourceCorpusUpstream,
		Tags:     tags,
	})
}

// ReadTestdataFile reads relPath from DefaultCorpus's embedded testdata tree and
// returns an independent cloned byte slice. Empty strings, parent-directory
// traversal segments, or missing files return a non-nil error.
func ReadTestdataFile(relPath string) ([]byte, error) {
	return DefaultCorpus().ReadFile(relPath)
}

// ParseProtocolDirName maps a canonical protocol ID or short codec directory
// alias to its protocol.ProtocolID constant after trimming whitespace.
// Unrecognized directory labels return an empty ProtocolID and false.
func ParseProtocolDirName(name string) (protocol.ProtocolID, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case string(protocol.ProtocolAnthropicMessages), "messages", "anthropic":
		return protocol.ProtocolAnthropicMessages, true
	case string(protocol.ProtocolOpenAIResponses), "responses":
		return protocol.ProtocolOpenAIResponses, true
	case string(protocol.ProtocolOpenAIChatCompletions), "chat", "chat_completions", "chatcompletions":
		return protocol.ProtocolOpenAIChatCompletions, true
	case string(protocol.ProtocolGeminiGenerateContent), "generatecontent", "gemini":
		return protocol.ProtocolGeminiGenerateContent, true
	case string(protocol.ProtocolQoderChat), "qoderchat", "qoder":
		return protocol.ProtocolQoderChat, true
	default:
		return "", false
	}
}

// ReadGoProductionSources walks each supplied directory on disk and returns all
// handwritten non-test Go source files sorted by path. Pure packages call this
// helper from tests to enforce source-level invariants such as forbidding direct
// Extra.Members map indexing without importing os themselves.
func ReadGoProductionSources(dirs ...string) ([]GoSourceFile, error) {
	var out []GoSourceFile
	fset := token.NewFileSet()
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(p string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				name := entry.Name()
				if p != dir && (strings.HasPrefix(name, ".") || name == "testdata" || name == "vendor") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				return nil
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			astFile, err := parser.ParseFile(fset, p, data, parser.PackageClauseOnly|parser.ParseComments)
			if err != nil {
				return fmt.Errorf("parse %s: %w", p, err)
			}
			if ast.IsGenerated(astFile) {
				return nil
			}
			out = append(out, GoSourceFile{
				Path: filepath.ToSlash(p),
				Data: data,
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(out, func(a, b GoSourceFile) int {
		return strings.Compare(a.Path, b.Path)
	})
	return out, nil
}

// ReadJSONFixtures walks each supplied directory on disk and returns all .json
// files sorted by slash-separated path. Pure codec test suites call this helper
// to load saved IR golden snapshots across sibling directories without importing
// os directly, and unreadable paths return a non-nil error.
func ReadJSONFixtures(dirs ...string) ([]GoSourceFile, error) {
	var out []GoSourceFile
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(p string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if p != dir && isIgnoredEntry(entry.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(entry.Name(), ".json") || isIgnoredEntry(entry.Name()) {
				return nil
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			out = append(out, GoSourceFile{
				Path: filepath.ToSlash(p),
				Data: bytes.Clone(data),
			})
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	slices.SortFunc(out, func(a, b GoSourceFile) int {
		return strings.Compare(a.Path, b.Path)
	})
	return out, nil
}

// scanWireFixtures walks hand-written request, stream, and response folders
// beneath wire/ inside the backing filesystem.
func (c *Corpus) scanWireFixtures() ([]Fixture, error) {
	protoEntries, err := fs.ReadDir(c.fsys, "wire")
	if err != nil {
		// Return an empty slice when wire/ has not been populated yet so early
		// refactor stages can query DefaultCorpus without directory-missing errors.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	var out []Fixture
	for _, pEntry := range protoEntries {
		if !pEntry.IsDir() || isIgnoredEntry(pEntry.Name()) {
			continue
		}
		protoID, ok := ParseProtocolDirName(pEntry.Name())
		if !ok {
			return nil, fmt.Errorf("unrecognized protocol directory wire/%s", pEntry.Name())
		}
		for _, kind := range []FixtureKind{FixtureRequest, FixtureStream, FixtureResponse} {
			kindDir := path.Join("wire", pEntry.Name(), string(kind))
			files, err := fs.ReadDir(c.fsys, kindDir)
			if err != nil {
				// Skip absent per-kind subdirectories so protocols that define only request
				// or stream fixtures do not require empty placeholder folders.
				if errors.Is(err, fs.ErrNotExist) {
					continue
				}
				return nil, err
			}
			var dirMeta *CorpusMeta
			if metaBytes, metaErr := fs.ReadFile(c.fsys, path.Join(kindDir, "meta.json")); metaErr == nil {
				parsed, err := parseCorpusMeta(metaBytes)
				if err != nil {
					return nil, fmt.Errorf("parse %s/meta.json: %w", kindDir, err)
				}
				dirMeta = &parsed
			}
			for _, fEntry := range files {
				if fEntry.IsDir() || isIgnoredEntry(fEntry.Name()) || fEntry.Name() == "meta.json" {
					continue
				}
				if !matchesWireExtension(kind, fEntry.Name()) {
					continue
				}
				fullPath := path.Join(kindDir, fEntry.Name())
				data, err := fs.ReadFile(c.fsys, fullPath)
				if err != nil {
					return nil, err
				}
				scenario := trimFixtureExt(fEntry.Name())
				var tags []string
				if dirMeta != nil {
					tags = slices.Clone(dirMeta.Tags)
				}
				out = append(out, Fixture{
					Name:     scenario,
					Path:     fullPath,
					Source:   SourceWire,
					Protocol: protoID,
					Kind:     kind,
					Scenario: scenario,
					Tags:     tags,
					Meta:     dirMeta,
					Data:     bytes.Clone(data),
				})
			}
		}
	}
	return out, nil
}

// scanCorpusClients walks recorded client request files and directory metadata
// beneath corpus/clients inside the backing filesystem.
func (c *Corpus) scanCorpusClients() ([]Fixture, error) {
	const root = "corpus/clients"
	if _, err := fs.Stat(c.fsys, root); err != nil {
		// Return an empty slice when no client recordings have been imported yet so
		// law suites run cleanly before clientlab recordings land.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []Fixture
	err := fs.WalkDir(c.fsys, root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if p != root && isIgnoredEntry(d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".request.json") {
			return nil
		}
		rel := strings.TrimPrefix(p, root+"/")
		parts := strings.Split(rel, "/")
		if len(parts) != 4 {
			return fmt.Errorf("unexpected client corpus path %s (want corpus/clients/<client>/<version>/<scenario>/<n>.request.json)", p)
		}
		metaPath := path.Join(path.Dir(p), "meta.json")
		metaBytes, err := fs.ReadFile(c.fsys, metaPath)
		if err != nil {
			return fmt.Errorf("read %s for %s: %w", metaPath, p, err)
		}
		meta, err := parseCorpusMeta(metaBytes)
		if err != nil {
			return fmt.Errorf("parse %s: %w", metaPath, err)
		}
		if meta.Client == "" {
			meta.Client = parts[0]
		}
		if meta.Version == "" {
			meta.Version = parts[1]
		}
		if meta.Scenario == "" {
			meta.Scenario = parts[2]
		}
		data, err := fs.ReadFile(c.fsys, p)
		if err != nil {
			return err
		}
		turnStem := strings.TrimSuffix(d.Name(), ".request.json")
		out = append(out, Fixture{
			Name:     path.Join(parts[0], parts[1], parts[2], turnStem),
			Path:     p,
			Source:   SourceCorpusClient,
			Protocol: meta.Protocol,
			Kind:     FixtureRequest,
			Scenario: meta.Scenario,
			Tags:     slices.Clone(meta.Tags),
			Meta:     &meta,
			Data:     bytes.Clone(data),
		})
		return nil
	})
	return out, err
}

// scanCorpusUpstreams walks recorded upstream SSE stream files and directory
// metadata beneath corpus/upstreams inside the backing filesystem.
func (c *Corpus) scanCorpusUpstreams() ([]Fixture, error) {
	const root = "corpus/upstreams"
	if _, err := fs.Stat(c.fsys, root); err != nil {
		// Return an empty slice when no upstream stream recordings exist yet so
		// corpus queries succeed before live capture imports run.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []Fixture
	err := fs.WalkDir(c.fsys, root, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if p != root && isIgnoredEntry(d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".stream.sse") {
			return nil
		}
		rel := strings.TrimPrefix(p, root+"/")
		parts := strings.Split(rel, "/")
		if len(parts) != 4 {
			return fmt.Errorf("unexpected upstream corpus path %s (want corpus/upstreams/<provider>/<model>/<scenario>/<n>.stream.sse)", p)
		}
		metaPath := path.Join(path.Dir(p), "meta.json")
		metaBytes, err := fs.ReadFile(c.fsys, metaPath)
		if err != nil {
			return fmt.Errorf("read %s for %s: %w", metaPath, p, err)
		}
		meta, err := parseCorpusMeta(metaBytes)
		if err != nil {
			return fmt.Errorf("parse %s: %w", metaPath, err)
		}
		if meta.Provider == "" {
			meta.Provider = parts[0]
		}
		if meta.Model == "" {
			meta.Model = parts[1]
		}
		if meta.Scenario == "" {
			meta.Scenario = parts[2]
		}
		data, err := fs.ReadFile(c.fsys, p)
		if err != nil {
			return err
		}
		turnStem := strings.TrimSuffix(d.Name(), ".stream.sse")
		out = append(out, Fixture{
			Name:     path.Join(parts[0], parts[1], parts[2], turnStem),
			Path:     p,
			Source:   SourceCorpusUpstream,
			Protocol: meta.Protocol,
			Kind:     FixtureStream,
			Scenario: meta.Scenario,
			Tags:     slices.Clone(meta.Tags),
			Meta:     &meta,
			Data:     bytes.Clone(data),
		})
		return nil
	})
	return out, err
}

// parseCorpusMeta unmarshals raw meta.json bytes and normalizes Protocol aliases.
func parseCorpusMeta(raw []byte) (CorpusMeta, error) {
	var meta CorpusMeta
	if err := json.Unmarshal(raw, &meta); err != nil {
		return CorpusMeta{}, err
	}
	if meta.Protocol != "" {
		if canonical, ok := ParseProtocolDirName(string(meta.Protocol)); ok {
			meta.Protocol = canonical
		}
	}
	return meta, nil
}

// matchesQuery reports whether item satisfies all populated filter fields in q.
func matchesQuery(item Fixture, q FixtureQuery) bool {
	if q.Protocol != "" && item.Protocol != q.Protocol {
		return false
	}
	if q.Kind != "" && item.Kind != q.Kind {
		return false
	}
	if q.Source != "" && item.Source != q.Source {
		return false
	}
	if q.Scenario != "" && item.Scenario != q.Scenario {
		return false
	}
	if q.Client != "" && (item.Meta == nil || item.Meta.Client != q.Client) {
		return false
	}
	if q.Provider != "" && (item.Meta == nil || item.Meta.Provider != q.Provider) {
		return false
	}
	for _, wantTag := range q.Tags {
		if !slices.Contains(item.Tags, wantTag) {
			return false
		}
	}
	return true
}

// isIgnoredEntry reports whether a directory or file name starts with '.' or '_'
// such as _incoming or .DS_Store.
func isIgnoredEntry(name string) bool {
	return strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
}

// matchesWireExtension reports whether filename carries the expected extension for kind.
func matchesWireExtension(kind FixtureKind, filename string) bool {
	switch kind {
	case FixtureRequest, FixtureResponse:
		return strings.HasSuffix(filename, ".json")
	case FixtureStream:
		return strings.HasSuffix(filename, ".sse")
	default:
		return false
	}
}

// trimFixtureExt strips compound or single fixture extensions from filename.
func trimFixtureExt(filename string) string {
	for _, suffix := range []string{".request.json", ".response.json", ".stream.sse", ".json", ".sse"} {
		if stem, ok := strings.CutSuffix(filename, suffix); ok {
			return stem
		}
	}
	return filename
}
