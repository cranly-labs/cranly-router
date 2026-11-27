package testkit

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
)

// jsondiff.go compares JSON documents semantically and formats JSON-path
// differences without routing numbers through float64. Protocol requests and
// tool schemas can carry 64-bit integer identifiers and arbitrary decimal bounds
// that float64 decoding rounds silently, while deterministic encoding requires
// object-key order independence during structural assertions.
//
// Decoding reads tokens directly from jsontext.Decoder into a lossless value
// tree and normalizes JSON numbers into sign, trimmed significand digits, and
// base-10 exponent so numerically equal forms such as 1, 1.0, and 1e0 match
// while distinct 64-bit integers stay distinct. Diff traversal visits object
// keys in lexical order and supports caller-supplied ignored paths for volatile
// response fields.

const (
	// DiffKind constants classify a single structural mismatch reported at a
	// JSON path during semantic comparison.
	DiffAdded        DiffKind = "added"         // member or element present only in got
	DiffRemoved      DiffKind = "removed"       // member or element present only in want
	DiffChanged      DiffKind = "changed"       // scalar value differs at the same type
	DiffTypeMismatch DiffKind = "type_mismatch" // JSON value kind differs at path

	// jsonNodeKind constants tag the active decoded JSON scalar or container
	// category stored inside a lossless jsonNode value.
	nodeNull jsonNodeKind = iota
	nodeBool
	nodeNumber
	nodeString
	nodeArray
	nodeObject
)

// DiffKind identifies what changed at a JSONDiffItem path.
type DiffKind string

// jsonNodeKind tags the JSON type stored in a parsed jsonNode.
type jsonNodeKind uint8

// JSONDiffItem describes one semantic difference between expected and actual JSON.
type JSONDiffItem struct {
	Path string   `json:"path"` // JSONPath-style location starting at $
	Kind DiffKind `json:"kind"` // structural mismatch classification
	Want string   `json:"want"` // compact rendering of expected value, or empty
	Got  string   `json:"got"`  // compact rendering of actual value, or empty
}

// JSONDiffOption configures parsing or path filtering during JSONDiff.
type JSONDiffOption func(*jsonDiffConfig)

// jsonDiffConfig holds resolved options for one JSONDiff invocation.
type jsonDiffConfig struct {
	allowInvalidUTF8 bool            // permit invalid UTF-8 in JSON strings
	ignorePaths      map[string]bool // exact JSON paths skipped during comparison
}

// canonNumber stores an arbitrary-precision decimal representation of a valid
// RFC 8259 number token so equality avoids float64 rounding.
type canonNumber struct {
	zero   bool   // true when the numeric value equals zero
	neg    bool   // true for strictly negative numbers
	digits string // significand digits with leading and trailing zeros removed
	exp    int    // base-10 exponent applied to digits
	raw    string // original token spelling for readable diff output
}

// jsonNode represents one lossless decoded JSON scalar, array, or object value.
type jsonNode struct {
	kind    jsonNodeKind        // active JSON value category
	boolVal bool                // decoded boolean scalar payload
	numVal  canonNumber         // normalized numeric payload
	strVal  string              // decoded string payload
	arrVal  []jsonNode          // ordered array elements
	objVal  map[string]jsonNode // object members keyed by exact decoded name
	objKeys []string            // object member names in wire scan order
}

// WithAllowInvalidUTF8 configures JSONDiff and JSONEqual to accept invalid
// UTF-8 byte sequences inside JSON strings when comparing tolerant client
// request payloads. By default, invalid UTF-8 string bytes fail decoding.
func WithAllowInvalidUTF8(allow bool) JSONDiffOption {
	return func(cfg *jsonDiffConfig) {
		cfg.allowInvalidUTF8 = allow
	}
}

// WithIgnorePaths configures JSONDiff and JSONEqual to skip exact JSONPath
// locations such as "$.id" or "$.created_at" during structural comparison.
// Multiple WithIgnorePaths options merge their ignored path sets additively.
func WithIgnorePaths(paths ...string) JSONDiffOption {
	return func(cfg *jsonDiffConfig) {
		if cfg.ignorePaths == nil {
			cfg.ignorePaths = make(map[string]bool, len(paths))
		}
		for _, p := range paths {
			cfg.ignorePaths[p] = true
		}
	}
}

// JSONDiff parses want and got as single JSON values and returns all semantic
// differences ordered deterministically by JSON path. Duplicate object keys in
// either input return a decode error unless absent.
func JSONDiff(want, got []byte, opts ...JSONDiffOption) ([]JSONDiffItem, error) {
	var cfg jsonDiffConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	wantNode, err := parseJSONNode(want, cfg.allowInvalidUTF8)
	if err != nil {
		return nil, fmt.Errorf("parse want JSON: %w", err)
	}
	gotNode, err := parseJSONNode(got, cfg.allowInvalidUTF8)
	if err != nil {
		return nil, fmt.Errorf("parse got JSON: %w", err)
	}
	var diffs []JSONDiffItem
	diffNodes("$", wantNode, gotNode, &cfg, &diffs)
	return diffs, nil
}

// JSONEqual reports whether want and got represent semantically identical JSON
// values under opts and returns a formatted diff summary when they differ.
// Malformed inputs or duplicate object keys return a non-nil error.
func JSONEqual(want, got []byte, opts ...JSONDiffOption) (bool, string, error) {
	diffs, err := JSONDiff(want, got, opts...)
	if err != nil {
		return false, "", err
	}
	if len(diffs) == 0 {
		return true, "", nil
	}
	return false, FormatJSONDiff(diffs), nil
}

// FormatJSONDiff renders items as one human-readable diagnostic line per JSON
// path difference in slice order. Empty or nil diff slices return an empty
// string without trailing newlines.
func FormatJSONDiff(items []JSONDiffItem) string {
	if len(items) == 0 {
		return ""
	}
	var b strings.Builder
	for i, item := range items {
		if i > 0 {
			_ = b.WriteByte('\n')
		}
		switch item.Kind {
		case DiffAdded:
			fmt.Fprintf(&b, "%s: added %s", item.Path, item.Got)
		case DiffRemoved:
			fmt.Fprintf(&b, "%s: removed %s", item.Path, item.Want)
		case DiffTypeMismatch:
			fmt.Fprintf(&b, "%s: type mismatch: want %s, got %s", item.Path, item.Want, item.Got)
		default:
			fmt.Fprintf(&b, "%s: want %s, got %s", item.Path, item.Want, item.Got)
		}
	}
	return b.String()
}

// CanonicalJSON parses raw and re-encodes it with sorted object keys and
// normalized decimal numbers so semantically equal JSON yields identical bytes.
// Invalid JSON syntax or duplicate object member names return an error.
func CanonicalJSON(raw []byte) ([]byte, error) {
	node, err := parseJSONNode(raw, true)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := writeCanonicalNode(enc, node); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// CanonicalJSONHash returns the lowercase SHA-256 hex digest of
// CanonicalJSON(raw) for order-independent JSON equality checks. Malformed JSON
// or duplicate object member names return an empty string and an error.
func CanonicalJSONHash(raw []byte) (string, error) {
	canon, err := CanonicalJSON(raw)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:]), nil
}

// parseJSONNode decodes data into a complete jsonNode and rejects trailing tokens.
func parseJSONNode(data []byte, allowInvalidUTF8 bool) (jsonNode, error) {
	decOpts := []jsontext.Options{jsontext.AllowInvalidUTF8(allowInvalidUTF8)}
	dec := jsontext.NewDecoder(bytes.NewReader(data), decOpts...)
	node, err := readNode(dec)
	if err != nil {
		return jsonNode{}, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		if err == nil {
			return jsonNode{}, fmt.Errorf("unexpected trailing JSON token at offset %d", dec.InputOffset())
		}
		return jsonNode{}, err
	}
	return node, nil
}

// readNode recursively consumes one JSON value from dec, rejecting duplicate
// object member names within the same object.
func readNode(dec *jsontext.Decoder) (jsonNode, error) {
	tok, err := dec.ReadToken()
	if err != nil {
		return jsonNode{}, err
	}
	switch tok.Kind() {
	case 'n':
		return jsonNode{kind: nodeNull}, nil
	case 'f', 't':
		return jsonNode{kind: nodeBool, boolVal: tok.Bool()}, nil
	case '0':
		num, err := normalizeJSONNumber(tok.String())
		if err != nil {
			return jsonNode{}, err
		}
		return jsonNode{kind: nodeNumber, numVal: num}, nil
	case '"':
		return jsonNode{kind: nodeString, strVal: tok.String()}, nil
	case '[':
		var items []jsonNode
		for dec.PeekKind() != ']' {
			elem, err := readNode(dec)
			if err != nil {
				return jsonNode{}, err
			}
			items = append(items, elem)
		}
		if _, err := dec.ReadToken(); err != nil {
			return jsonNode{}, err
		}
		return jsonNode{kind: nodeArray, arrVal: items}, nil
	case '{':
		members := make(map[string]jsonNode)
		var keys []string
		for dec.PeekKind() != '}' {
			keyTok, err := dec.ReadToken()
			if err != nil {
				return jsonNode{}, err
			}
			key := keyTok.String()
			if _, exists := members[key]; exists {
				return jsonNode{}, fmt.Errorf("duplicate JSON object key %q", key)
			}
			val, err := readNode(dec)
			if err != nil {
				return jsonNode{}, err
			}
			members[key] = val
			keys = append(keys, key)
		}
		if _, err := dec.ReadToken(); err != nil {
			return jsonNode{}, err
		}
		return jsonNode{kind: nodeObject, objVal: members, objKeys: keys}, nil
	default:
		return jsonNode{}, fmt.Errorf("unexpected JSON token kind %q", tok.Kind())
	}
}

// normalizeJSONNumber converts a syntactically valid JSON number string into a
// canonical decimal significand and exponent for exact value comparison.
func normalizeJSONNumber(raw string) (canonNumber, error) {
	s := raw
	neg := false
	if strings.HasPrefix(s, "-") {
		neg = true
		s = s[1:]
	}
	expPart := 0
	if idx := strings.IndexAny(s, "eE"); idx >= 0 {
		parsedExp, err := strconv.Atoi(s[idx+1:])
		if err != nil {
			return canonNumber{}, fmt.Errorf("invalid exponent in %q: %w", raw, err)
		}
		expPart = parsedExp
		s = s[:idx]
	}

	intPart, fracPart, hasDot := strings.Cut(s, ".")
	combined := intPart + fracPart
	if hasDot {
		expPart -= len(fracPart)
	}
	trimmedLead := strings.TrimLeft(combined, "0")
	if trimmedLead == "" {
		return canonNumber{zero: true, raw: raw}, nil
	}
	trimmed := strings.TrimRight(trimmedLead, "0")
	expPart += len(trimmedLead) - len(trimmed)
	return canonNumber{
		neg:    neg,
		digits: trimmed,
		exp:    expPart,
		raw:    raw,
	}, nil
}

// equal reports whether two canonNumber values represent the exact same real number.
func (a canonNumber) equal(b canonNumber) bool {
	if a.zero || b.zero {
		return a.zero && b.zero
	}
	return a.neg == b.neg && a.digits == b.digits && a.exp == b.exp
}

// canonicalString formats a canonNumber into a unique JSON number spelling so
// CanonicalJSON hashes 1, 1.0, and 1e0 identically.
func (a canonNumber) canonicalString() string {
	if a.zero {
		return "0"
	}
	var b strings.Builder
	if a.neg {
		_ = b.WriteByte('-')
	}
	// Format modest exponents in plain fixed-point form and switch to scientific
	// notation outside [-6, 20] so large exponents do not allocate huge zero runs.
	pointPos := len(a.digits) + a.exp
	if pointPos > 0 && pointPos <= 21 {
		if pointPos >= len(a.digits) {
			_, _ = b.WriteString(a.digits)
			_, _ = b.WriteString(strings.Repeat("0", pointPos-len(a.digits)))
		} else {
			_, _ = b.WriteString(a.digits[:pointPos])
			_ = b.WriteByte('.')
			_, _ = b.WriteString(a.digits[pointPos:])
		}
		return b.String()
	}
	if pointPos <= 0 && pointPos >= -5 {
		_, _ = b.WriteString("0.")
		_, _ = b.WriteString(strings.Repeat("0", -pointPos))
		_, _ = b.WriteString(a.digits)
		return b.String()
	}
	_ = b.WriteByte(a.digits[0])
	if len(a.digits) > 1 {
		_ = b.WriteByte('.')
		_, _ = b.WriteString(a.digits[1:])
	}
	sciExp := len(a.digits) - 1 + a.exp
	fmt.Fprintf(&b, "e%d", sciExp)
	return b.String()
}

// diffNodes appends structural and scalar mismatches between want and got to out.
func diffNodes(path string, want, got jsonNode, cfg *jsonDiffConfig, out *[]JSONDiffItem) {
	if cfg.ignorePaths[path] {
		return
	}
	if want.kind != got.kind {
		*out = append(*out, JSONDiffItem{
			Path: path,
			Kind: DiffTypeMismatch,
			Want: renderNodeSummary(want),
			Got:  renderNodeSummary(got),
		})
		return
	}

	switch want.kind {
	case nodeNull:
		return
	case nodeBool:
		if want.boolVal != got.boolVal {
			*out = append(*out, JSONDiffItem{
				Path: path,
				Kind: DiffChanged,
				Want: strconv.FormatBool(want.boolVal),
				Got:  strconv.FormatBool(got.boolVal),
			})
		}
	case nodeNumber:
		if !want.numVal.equal(got.numVal) {
			*out = append(*out, JSONDiffItem{
				Path: path,
				Kind: DiffChanged,
				Want: want.numVal.raw,
				Got:  got.numVal.raw,
			})
		}
	case nodeString:
		if want.strVal != got.strVal {
			*out = append(*out, JSONDiffItem{
				Path: path,
				Kind: DiffChanged,
				Want: strconv.Quote(want.strVal),
				Got:  strconv.Quote(got.strVal),
			})
		}
	case nodeArray:
		shared := min(len(want.arrVal), len(got.arrVal))
		for i := range shared {
			elemPath := fmt.Sprintf("%s[%d]", path, i)
			diffNodes(elemPath, want.arrVal[i], got.arrVal[i], cfg, out)
		}
		for i := shared; i < len(want.arrVal); i++ {
			elemPath := fmt.Sprintf("%s[%d]", path, i)
			if !cfg.ignorePaths[elemPath] {
				*out = append(*out, JSONDiffItem{
					Path: elemPath,
					Kind: DiffRemoved,
					Want: renderNodeSummary(want.arrVal[i]),
				})
			}
		}
		for i := shared; i < len(got.arrVal); i++ {
			elemPath := fmt.Sprintf("%s[%d]", path, i)
			if !cfg.ignorePaths[elemPath] {
				*out = append(*out, JSONDiffItem{
					Path: elemPath,
					Kind: DiffAdded,
					Got:  renderNodeSummary(got.arrVal[i]),
				})
			}
		}
	case nodeObject:
		keys := make([]string, 0, len(want.objVal)+len(got.objVal))
		for k := range want.objVal {
			keys = append(keys, k)
		}
		for k := range got.objVal {
			if _, exists := want.objVal[k]; !exists {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		for _, k := range keys {
			childPath := appendJSONMemberPath(path, k)
			if cfg.ignorePaths[childPath] {
				continue
			}
			wVal, wOK := want.objVal[k]
			gVal, gOK := got.objVal[k]
			switch {
			case wOK && gOK:
				diffNodes(childPath, wVal, gVal, cfg, out)
			case wOK:
				*out = append(*out, JSONDiffItem{
					Path: childPath,
					Kind: DiffRemoved,
					Want: renderNodeSummary(wVal),
				})
			default:
				*out = append(*out, JSONDiffItem{
					Path: childPath,
					Kind: DiffAdded,
					Got:  renderNodeSummary(gVal),
				})
			}
		}
	}
}

// appendJSONMemberPath appends key to base using dot notation for simple ASCII
// identifiers and bracket-quoted notation otherwise.
func appendJSONMemberPath(base, key string) string {
	if isSimpleIdentifier(key) {
		return base + "." + key
	}
	return fmt.Sprintf("%s[%s]", base, strconv.Quote(key))
}

// isSimpleIdentifier reports whether key can be written as an unquoted JSONPath segment.
func isSimpleIdentifier(key string) bool {
	if key == "" {
		return false
	}
	for i := range len(key) {
		c := key[i]
		alpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
		digit := c >= '0' && c <= '9'
		if i == 0 && !alpha {
			return false
		}
		if !alpha && !digit {
			return false
		}
	}
	return true
}

// renderNodeSummary formats a compact one-line representation of n for diff diagnostics.
func renderNodeSummary(n jsonNode) string {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := writeCanonicalNode(enc, n); err != nil {
		return "<invalid>"
	}
	s := strings.TrimRight(buf.String(), "\n")
	// Cap rendered container previews at 160 bytes so a single mismatched array or
	// object does not flood test failure output.
	if len(s) > 160 {
		return s[:157] + "..."
	}
	return s
}

// writeCanonicalNode writes n to enc with sorted object keys and canonical numbers.
func writeCanonicalNode(enc *jsontext.Encoder, n jsonNode) error {
	switch n.kind {
	case nodeNull:
		return enc.WriteToken(jsontext.Null)
	case nodeBool:
		return enc.WriteToken(jsontext.Bool(n.boolVal))
	case nodeNumber:
		return enc.WriteValue(jsontext.Value(n.numVal.canonicalString()))
	case nodeString:
		return enc.WriteToken(jsontext.String(n.strVal))
	case nodeArray:
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for _, elem := range n.arrVal {
			if err := writeCanonicalNode(enc, elem); err != nil {
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
			if err := writeCanonicalNode(enc, n.objVal[k]); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndObject)
	default:
		return fmt.Errorf("unknown jsonNodeKind %d", n.kind)
	}
}
