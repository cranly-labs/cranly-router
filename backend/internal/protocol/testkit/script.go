package testkit

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"slices"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// script.go defines protocol-independent scenario descriptors and scripted
// upstream turn specifications together with embedded scenario loaders.
// Scenarios express multi-turn conversation inputs, capability requirements,
// and expected upstream replies once in IR JSON, while Script and Turn drive
// mockupstream without relying on fragile global request counters.

// Script describes a multi-turn scripted upstream behavior for one test scenario.
type Script struct {
	Name    string `json:"name"`             // scenario or script identifier
	Turns   []Turn `json:"turns"`            // ordered turn matchers and replies
	Default Reply  `json:"default,omitzero"` // fallback reply for unmatched client sidecar requests
}

// Turn binds one turn-recognition predicate to request assertions, a synthetic
// or recorded Reply, and optional fault injection.
type Turn struct {
	Match  Match   `json:"match,omitzero"`  // conversation-shape turn selector
	Expect []Check `json:"expect,omitzero"` // predicates checked against upstream request
	Reply  Reply   `json:"reply"`           // upstream stream or response to emit
	Fault  *Fault  `json:"fault,omitzero"`  // optional HTTP error, truncation, or delay
}

// Match identifies a conversation turn by historical assistant message and tool
// result counts plus optional model and text filters, avoiding global request
// counters that client background requests would desynchronize.
type Match struct {
	AssistantMessages *int   `json:"assistant_messages,omitzero"` // required count of assistant messages in history
	ToolResults       *int   `json:"tool_results,omitzero"`       // required count of tool result items in history
	ModelPrefix       string `json:"model_prefix,omitzero"`       // optional model name prefix filter
	ContainsText      string `json:"contains_text,omitzero"`      // optional substring required in request body
	HasTools          *bool  `json:"has_tools,omitzero"`          // optional requirement on declared tools presence
}

// Check defines either a declarative JSON assertion or a Go callback predicate
// verified when a scripted Turn matches an incoming upstream request.
type Check struct {
	Description string                                  `json:"description,omitzero"` // human-readable check label
	JSONPath    string                                  `json:"json_path,omitzero"`   // dot/index path such as $.model
	Equals      string                                  `json:"equals,omitzero"`      // expected compact JSON or string value
	Contains    string                                  `json:"contains,omitzero"`    // substring expected in raw body or path value
	Func        func(req *ir.Request, raw []byte) error `json:"-"`                    // optional programmatic assertion
}

// Reply specifies how a scripted upstream responds to a matched Turn, preferring
// Raw corpus streams when present and otherwise synthesizing from Events or Response.
type Reply struct {
	Events   []ir.Event   `json:"events,omitzero"`   // canonical IR events for stream synthesis
	Response *ir.Response `json:"response,omitzero"` // buffered IR response for non-stream synthesis
	Raw      string       `json:"raw,omitzero"`      // relative testdata path replayed verbatim
}

// Fault injects deterministic upstream HTTP errors, mid-stream frame cuts, or
// response delays on a matched Turn.
type Fault struct {
	Status              int    `json:"status,omitzero"`                // non-200 HTTP status returned before streaming
	ErrorBody           string `json:"error_body,omitzero"`            // optional raw error response payload
	TruncateAfterFrames *int   `json:"truncate_after_frames,omitzero"` // cut SSE output after N emitted frames
	DelayMs             int    `json:"delay_ms,omitzero"`              // artificial pre-response delay in milliseconds
}

// ScenarioCapabilities declares which protocol or dialect capabilities a
// Scenario requires so matrix runners skip inapplicable routes explicitly.
type ScenarioCapabilities struct {
	Tools            bool                  `json:"tools,omitzero"`             // requires function tool declarations and calls
	CustomTools      bool                  `json:"custom_tools,omitzero"`      // requires Responses custom grammar tools
	Namespaces       bool                  `json:"namespaces,omitzero"`        // requires Responses tool namespaces
	ClientSearch     bool                  `json:"client_search,omitzero"`     // requires client tool_search support
	HostedSearch     bool                  `json:"hosted_search,omitzero"`     // requires hosted web search declarations
	Reasoning        bool                  `json:"reasoning,omitzero"`         // requires reasoning blocks or summaries
	ImageInput       bool                  `json:"image_input,omitzero"`       // requires image input parts
	DocumentInput    bool                  `json:"document_input,omitzero"`    // requires PDF or document attachments
	StructuredOutput bool                  `json:"structured_output,omitzero"` // requires json_schema or json_object output
	StreamOnly       bool                  `json:"stream_only,omitzero"`       // skips buffered non-streaming assertions
	Clients          []protocol.ProtocolID `json:"clients,omitzero"`           // optional allowlist of client protocols
	Upstreams        []protocol.ProtocolID `json:"upstreams,omitzero"`         // optional allowlist of upstream protocols
}

// Scenario bundles a protocol-independent IR test case loaded from testdata/scenarios/.
type Scenario struct {
	Name         string                `json:"name"`                  // unique scenario slug such as "tool_single"
	Description  string                `json:"description,omitzero"`  // concise scenario purpose
	Capabilities ScenarioCapabilities  `json:"capabilities,omitzero"` // route eligibility requirements
	Request      *ir.Request           `json:"request,omitzero"`      // first-turn canonical IR request
	Events       []ir.Event            `json:"events,omitzero"`       // first-turn canonical IR stream events
	Response     *ir.Response          `json:"response,omitzero"`     // optional first-turn buffered IR response
	ToolOutputs  []SimulatedToolOutput `json:"tool_outputs,omitzero"` // tool results for multi-turn replay laws
	Script       *Script               `json:"script,omitzero"`       // optional multi-turn upstream script
}

// MatchesObserved reports whether m matches a request with the given historical
// assistant turn count, tool result count, model name, tool presence, and raw
// body. Nil pointer fields and empty strings on m act as wildcards.
func (m Match) MatchesObserved(assistantCount, toolResultCount int, model string, hasTools bool, rawBody []byte) bool {
	if m.AssistantMessages != nil && *m.AssistantMessages != assistantCount {
		return false
	}
	if m.ToolResults != nil && *m.ToolResults != toolResultCount {
		return false
	}
	if m.ModelPrefix != "" && !strings.HasPrefix(model, m.ModelPrefix) {
		return false
	}
	if m.HasTools != nil && *m.HasTools != hasTools {
		return false
	}
	if m.ContainsText != "" && !strings.Contains(string(rawBody), m.ContainsText) {
		return false
	}
	return true
}

// Evaluate runs c against decoded req and raw JSON bytes, returning a
// descriptive error when any configured assertion fails. Programmatic Func
// checks run before JSONPath and raw substring assertions.
func (c Check) Evaluate(req *ir.Request, raw []byte) error {
	if c.Func != nil {
		if err := c.Func(req, raw); err != nil {
			return err
		}
	}
	if c.JSONPath != "" {
		val, ok, err := lookupJSONPathSummary(raw, c.JSONPath)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("JSON path %s not found", c.JSONPath)
		}
		if c.Equals != "" && val != c.Equals {
			return fmt.Errorf("JSON path %s = %q, want %q", c.JSONPath, val, c.Equals)
		}
		if c.Contains != "" && !strings.Contains(val, c.Contains) {
			return fmt.Errorf("JSON path %s = %q, does not contain %q", c.JSONPath, val, c.Contains)
		}
		return nil
	}
	if c.Contains != "" && !strings.Contains(string(raw), c.Contains) {
		return fmt.Errorf("request body does not contain %q", c.Contains)
	}
	return nil
}

// SupportsRoute reports whether s is applicable to the client->upstream protocol pair.
func (s Scenario) SupportsRoute(client, upstream protocol.ProtocolID) bool {
	if len(s.Capabilities.Clients) > 0 && !slices.Contains(s.Capabilities.Clients, client) {
		return false
	}
	if len(s.Capabilities.Upstreams) > 0 && !slices.Contains(s.Capabilities.Upstreams, upstream) {
		return false
	}
	return true
}

// LoadScenarios reads all *.json scenario definitions from DefaultCorpus's
// scenarios/ directory in deterministic name order. Missing scenarios/ returns
// nil without error, whereas malformed JSON files return an error.
func LoadScenarios() ([]Scenario, error) {
	return DefaultCorpus().LoadScenarios()
}

// LoadScenario reads a single named scenario ("<name>.json") from
// DefaultCorpus's scenarios/ directory and populates its Name stem. Empty
// names, missing files, or JSON syntax errors return a non-nil error.
func LoadScenario(name string) (Scenario, error) {
	return DefaultCorpus().LoadScenario(name)
}

// LoadScenarios reads all *.json scenario definitions from c's scenarios/
// directory in deterministic name order. Missing scenarios/ returns nil without
// an error so early test stages run cleanly before scenarios are populated.
func (c *Corpus) LoadScenarios() ([]Scenario, error) {
	entries, err := fs.ReadDir(c.fsys, "scenarios")
	if err != nil {
		// Return nil without error when scenarios/ is absent so early test batches
		// can initialize DefaultCorpus before scenario JSON files are added.
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var out []Scenario
	for _, entry := range entries {
		if entry.IsDir() || isIgnoredEntry(entry.Name()) || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		sc, err := c.LoadScenario(strings.TrimSuffix(entry.Name(), ".json"))
		if err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	slices.SortFunc(out, func(a, b Scenario) int {
		return strings.Compare(a.Name, b.Name)
	})
	return out, nil
}

// LoadScenario reads scenarios/<name>.json from c and populates Scenario.Name
// from the filename stem when omitted in JSON. Empty names, missing scenario
// files, or JSON unmarshaling failures return a non-nil error.
func (c *Corpus) LoadScenario(name string) (Scenario, error) {
	trimmed := strings.TrimSuffix(strings.TrimSpace(name), ".json")
	if trimmed == "" {
		return Scenario{}, errors.New("empty scenario name")
	}
	filePath := path.Join("scenarios", trimmed+".json")
	raw, err := c.ReadFile(filePath)
	if err != nil {
		return Scenario{}, err
	}
	var sc Scenario
	if err := json.Unmarshal(raw, &sc); err != nil {
		return Scenario{}, fmt.Errorf("parse %s: %w", filePath, err)
	}
	if sc.Name == "" {
		sc.Name = trimmed
	}
	return sc, nil
}

// lookupJSONPathSummary navigates simple $.a.b[0] paths inside raw JSON and
// returns unquoted strings or compact canonical JSON scalars.
func lookupJSONPathSummary(raw []byte, expr string) (string, bool, error) {
	node, err := parseJSONNode(raw, true)
	if err != nil {
		return "", false, err
	}
	rest := strings.TrimPrefix(strings.TrimSpace(expr), "$")
	cur := node
	for rest != "" {
		rest = strings.TrimPrefix(rest, ".")
		if rest == "" {
			break
		}
		if rest[0] == '[' {
			closeIdx := strings.IndexByte(rest, ']')
			if closeIdx < 0 {
				return "", false, fmt.Errorf("unclosed bracket in JSON path %q", expr)
			}
			idxToken := rest[1:closeIdx]
			rest = rest[closeIdx+1:]
			var idx int
			if _, scanErr := fmt.Sscanf(idxToken, "%d", &idx); scanErr != nil || idx < 0 {
				return "", false, fmt.Errorf("invalid array index %q in %q", idxToken, expr)
			}
			if cur.kind != nodeArray || idx >= len(cur.arrVal) {
				return "", false, nil
			}
			cur = cur.arrVal[idx]
			continue
		}
		end := len(rest)
		for i := range len(rest) {
			if rest[i] == '.' || rest[i] == '[' {
				end = i
				break
			}
		}
		key := rest[:end]
		rest = rest[end:]
		if cur.kind != nodeObject {
			return "", false, nil
		}
		next, exists := cur.objVal[key]
		if !exists {
			return "", false, nil
		}
		cur = next
	}
	if cur.kind == nodeString {
		return cur.strVal, true, nil
	}
	return renderNodeSummary(cur), true, nil
}
