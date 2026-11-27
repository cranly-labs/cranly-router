package generatecontent

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"maps"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// wire.go defines private Google Gemini GenerateContent JSON wire structures,
// deterministic JSON v2 serialization helpers, and Extra extension preservation
// routines for the generatecontent codec package. Keeping wire structures
// unexported inside this package isolates camelCase Gemini request, candidate,
// part-union, tool-declaration, and grounding envelopes from ir and sibling codecs.
//
// Wire structs backed by IR Extra maps embed a map[string]jsontext.Value field
// tagged with json:",embed" so unmodeled top-level fields (such as
// safetySettings, cachedContent, and labels), sub-object extensions (inside
// systemInstruction, toolConfig, generationConfig, and thinkingConfig), and
// part-level metadata survive same-protocol round trips without float64
// rounding or HTML escaping. Decoders enable jsontext.AllowInvalidUTF8(true)
// while keeping json/v2 duplicate-key rejection active across every nested object.

const (
	// geminiRoleUser and geminiRoleModel name the two speaker roles accepted inside
	// Gemini GenerateContent contents turns and candidate replies.
	geminiRoleUser  = "user"  // user or functionResponse turn speaker role
	geminiRoleModel = "model" // assistant model turn speaker role

	// namelessCallIDPrefix prefixes deterministic CallID values synthesized when
	// incoming Gemini request functionCall parts omit wire call identifiers.
	namelessCallIDPrefix = "gemcall_" // prefix marking derived nameless request call IDs
)

// Codec implements client-side and upstream-side protocol encoding and decoding
// for Google Gemini GenerateContent (protocol.ProtocolGeminiGenerateContent).
type Codec struct{}

// wireInboundRequest decodes an incoming Gemini GenerateContent JSON request body.
type wireInboundRequest struct {
	SystemInstruction jsontext.Value            `json:"systemInstruction,omitzero"` // optional system instruction Content object
	Contents          *[]jsontext.Value         `json:"contents,omitzero"`          // required non-empty conversation Content array
	Tools             []jsontext.Value          `json:"tools,omitzero"`             // optional callable and hosted tool declarations
	ToolConfig        jsontext.Value            `json:"toolConfig,omitzero"`        // optional function calling and server-tool config
	GenerationConfig  jsontext.Value            `json:"generationConfig,omitzero"`  // optional sampling, thinking, and schema config
	Unknown           map[string]jsontext.Value `json:",embed"`                     // unmodeled top-level fields such as safetySettings
}

// wireOutRequest serializes a legalized outgoing Gemini GenerateContent request body.
type wireOutRequest struct {
	SystemInstruction *wireOutSystemInstruction `json:"systemInstruction,omitzero"` // merged system and developer instructions
	Contents          []wireOutContent          `json:"contents"`                   // non-empty user and model conversation turns
	Tools             []jsontext.Value          `json:"tools,omitzero"`             // serialized functionDeclarations and hosted tools
	ToolConfig        *wireToolConfig           `json:"toolConfig,omitzero"`        // serialized functionCallingConfig and server flags
	GenerationConfig  *wireGenerationConfig     `json:"generationConfig,omitzero"`  // serialized sampling, thinking, and output config
	Unknown           map[string]jsontext.Value `json:",embed"`                     // preserved same-origin top-level Extra members
}

// wireOutSystemInstruction serializes the top-level systemInstruction Content
// object on outgoing Gemini requests without pre-marshaling individual parts.
type wireOutSystemInstruction struct {
	Parts   []wireOutPart             `json:"parts"`  // ordered text, media, or opaque instruction parts
	Unknown map[string]jsontext.Value `json:",embed"` // optional role or vendor systemInstruction members
}

// wireOutContent serializes one user or model turn inside an outgoing Gemini
// request contents array without intermediate per-part JSON buffers.
type wireOutContent struct {
	Role    string                    `json:"role,omitzero"` // "user" or "model" speaker role
	Parts   []wireOutPart             `json:"parts"`         // non-empty ordered Gemini Part entries
	Unknown map[string]jsontext.Value `json:",embed"`        // preserved same-origin Content Extra members
}

// wireOutPart holds either a typed wirePart or a compacted raw JSON object for
// same-origin opaque parts so EncodeRequest serializes typed parts in one pass.
type wireOutPart struct {
	// part and raw form a two-branch representation: raw is populated only for
	// same-origin PartOpaque values, while all modeled branches populate part.
	part wirePart       // typed Gemini part union fields when raw is empty
	raw  jsontext.Value // verbatim compacted JSON object for same-origin opaque parts
}

// wireSystemInstruction decodes the top-level Gemini systemInstruction Content
// object on incoming GenerateContent requests.
type wireSystemInstruction struct {
	Parts   []jsontext.Value          `json:"parts"`  // ordered text, media, or opaque instruction parts
	Unknown map[string]jsontext.Value `json:",embed"` // optional role or vendor systemInstruction members
}

// wireInboundContent decodes one conversation turn inside a Gemini request contents array.
type wireInboundContent struct {
	Role    string                    `json:"role,omitzero"`  // "user" or "model" turn speaker role
	Parts   *[]jsontext.Value         `json:"parts,omitzero"` // non-empty ordered part union objects
	Unknown map[string]jsontext.Value `json:",embed"`         // unmodeled Content-level members
}

// wireContent serializes one model turn inside buffered or streamed candidate content.
type wireContent struct {
	Role    string                    `json:"role,omitzero"` // "user" or "model" speaker role
	Parts   []jsontext.Value          `json:"parts"`         // ordered serialized Gemini Part objects
	Unknown map[string]jsontext.Value `json:",embed"`        // preserved same-origin Content Extra members
}

// wirePart decodes or serializes one Gemini Part union object across requests,
// buffered responses, and streaming candidate deltas.
type wirePart struct {
	// Text, Thought, InlineData, FileData, FunctionCall, and FunctionResponse
	// represent the modeled Gemini Part union branches; at most one primary
	// content branch is populated on valid parts while Thought qualifies Text.
	Text             *string                   `json:"text,omitzero"`             // plain or thought text payload
	Thought          bool                      `json:"thought,omitzero"`          // true when Text holds model reasoning
	ThoughtSignature string                    `json:"thoughtSignature,omitzero"` // continuity token on thought, call, or text
	InlineData       *wireInlineData           `json:"inlineData,omitzero"`       // inline base64 media attachment
	FileData         *wireFileData             `json:"fileData,omitzero"`         // URI or uploaded file media reference
	FunctionCall     *wireFunctionCall         `json:"functionCall,omitzero"`     // model tool invocation payload
	FunctionResponse *wireFunctionResponse     `json:"functionResponse,omitzero"` // user tool execution result payload
	Unknown          map[string]jsontext.Value `json:",embed"`                    // unmodeled part members or opaque branches
}

// wireInlineData represents inline base64-encoded media bytes on a Gemini Part.
type wireInlineData struct {
	MimeType string                    `json:"mimeType"` // IANA media MIME type label
	Data     string                    `json:"data"`     // base64-encoded binary media payload
	Unknown  map[string]jsontext.Value `json:",embed"`   // unmodeled inlineData fields such as displayName
}

// wireFileData represents URI-referenced or uploaded file media on a Gemini Part.
type wireFileData struct {
	MimeType string                    `json:"mimeType,omitzero"` // optional media MIME type label
	FileURI  string                    `json:"fileUri"`           // HTTPS, Cloud Storage, or Files API URI
	Unknown  map[string]jsontext.Value `json:",embed"`            // unmodeled fileData fields such as displayName
}

// wireFunctionCall represents a model tool call inside a Gemini Part.
type wireFunctionCall struct {
	ID      string                    `json:"id,omitzero"`   // optional call correlation identifier
	Name    string                    `json:"name"`          // invoked FunctionDeclaration name
	Args    jsontext.Value            `json:"args,omitzero"` // JSON object arguments or stream fragment
	Unknown map[string]jsontext.Value `json:",embed"`        // unmodeled functionCall fields
}

// wireFunctionResponse represents a user tool result inside a Gemini Part.
type wireFunctionResponse struct {
	ID       string                    `json:"id,omitzero"` // optional correlated functionCall identifier
	Name     string                    `json:"name"`        // invoked FunctionDeclaration name
	Response jsontext.Value            `json:"response"`    // required JSON object result payload
	Unknown  map[string]jsontext.Value `json:",embed"`      // unmodeled functionResponse fields
}

// wireToolEntry decodes or serializes one element of the top-level Gemini tools array.
type wireToolEntry struct {
	FunctionDeclarations *[]jsontext.Value         `json:"functionDeclarations,omitzero"` // callable client function declarations
	GoogleSearch         jsontext.Value            `json:"googleSearch,omitzero"`         // camelCase Google Search grounding config
	GoogleSearchSnake    jsontext.Value            `json:"google_search,omitzero"`        // compatible AI Studio snake_case search config
	CodeExecution        jsontext.Value            `json:"codeExecution,omitzero"`        // built-in Python code execution config
	URLContext           jsontext.Value            `json:"urlContext,omitzero"`           // built-in URL context retrieval config
	Unknown              map[string]jsontext.Value `json:",embed"`                        // other hosted Gemini tool declarations
}

// wireFunctionDeclaration decodes or serializes one Gemini FunctionDeclaration entry.
type wireFunctionDeclaration struct {
	Name                 string                    `json:"name"`                          // function identifier matching Gemini syntax
	Description          string                    `json:"description,omitzero"`          // human-readable tool behavior summary
	Parameters           jsontext.Value            `json:"parameters,omitzero"`           // Gemini OpenAPI-subset parameter schema
	ParametersJSONSchema jsontext.Value            `json:"parametersJsonSchema,omitzero"` // standard JSON Schema parameter definition
	Unknown              map[string]jsontext.Value `json:",embed"`                        // unmodeled declaration fields such as response
}

// wireToolConfig decodes or serializes the top-level Gemini toolConfig object.
type wireToolConfig struct {
	FunctionCallingConfig            jsontext.Value            `json:"functionCallingConfig,omitzero"`            // raw or serialized function calling mode config
	IncludeServerSideToolInvocations *bool                     `json:"includeServerSideToolInvocations,omitzero"` // enables mixed client and server tool calls
	Unknown                          map[string]jsontext.Value `json:",embed"`                                    // unmodeled toolConfig fields such as retrievalConfig
}

// wireFunctionCallingConfig decodes or serializes the nested toolConfig.functionCallingConfig JSON object.
type wireFunctionCallingConfig struct {
	Mode                 string                    `json:"mode,omitzero"`                 // "AUTO", "ANY", "NONE", or "VALIDATED"
	AllowedFunctionNames []string                  `json:"allowedFunctionNames,omitzero"` // restricted callable function whitelist
	Unknown              map[string]jsontext.Value `json:",embed"`                        // unmodeled functionCallingConfig fields
}

// wireGenerationConfig decodes or serializes the top-level Gemini generationConfig object.
type wireGenerationConfig struct {
	Temperature        *float64                  `json:"temperature,omitzero"`        // randomness sampling temperature scalar
	TopP               *float64                  `json:"topP,omitzero"`               // nucleus sampling probability cutoff
	TopK               *int64                    `json:"topK,omitzero"`               // top-k token sampling cutoff
	CandidateCount     *int64                    `json:"candidateCount,omitzero"`     // requested response candidate count
	MaxOutputTokens    *int64                    `json:"maxOutputTokens,omitzero"`    // maximum generated output token cap
	StopSequences      []string                  `json:"stopSequences,omitzero"`      // custom generation stop strings
	PresencePenalty    *float64                  `json:"presencePenalty,omitzero"`    // token presence repetition penalty
	FrequencyPenalty   *float64                  `json:"frequencyPenalty,omitzero"`   // token frequency repetition penalty
	Seed               *int64                    `json:"seed,omitzero"`               // deterministic sampling seed integer
	ResponseMimeType   string                    `json:"responseMimeType,omitzero"`   // output MIME type such as "application/json"
	ResponseSchema     jsontext.Value            `json:"responseSchema,omitzero"`     // Gemini-subset structured output schema
	ResponseJSONSchema jsontext.Value            `json:"responseJsonSchema,omitzero"` // standard JSON Schema structured output
	ThinkingConfig     jsontext.Value            `json:"thinkingConfig,omitzero"`     // nested reasoning budget or level settings
	Unknown            map[string]jsontext.Value `json:",embed"`                      // unmodeled generationConfig fields
}

// wireThinkingConfig decodes or serializes the nested generationConfig.thinkingConfig JSON object.
type wireThinkingConfig struct {
	ThinkingBudget  *int64                    `json:"thinkingBudget,omitzero"`  // token budget (-1 dynamic, 0 off, >0 cap)
	ThinkingLevel   string                    `json:"thinkingLevel,omitzero"`   // effort tier on Gemini 3 models
	IncludeThoughts *bool                     `json:"includeThoughts,omitzero"` // whether thought summaries stream back
	Unknown         map[string]jsontext.Value `json:",embed"`                   // unmodeled thinkingConfig fields
}

// wireResponse decodes a buffered Gemini GenerateContentResponse or single SSE frame.
type wireResponse struct {
	ResponseID    string                    `json:"responseId,omitzero"`    // upstream response identifier
	ModelVersion  string                    `json:"modelVersion,omitzero"`  // upstream model version string
	Candidates    []wireCandidate           `json:"candidates,omitzero"`    // ranked completion candidates (index 0 active)
	UsageMetadata jsontext.Value            `json:"usageMetadata,omitzero"` // raw token accounting object
	Error         jsontext.Value            `json:"error,omitzero"`         // inline upstream error envelope when present
	Unknown       map[string]jsontext.Value `json:",embed"`                 // unmodeled top-level response fields
}

// wireResponseWrite serializes an outgoing buffered or streamed GenerateContentResponse.
type wireResponseWrite struct {
	ResponseID    string                    `json:"responseId,omitzero"`    // presented response identifier
	ModelVersion  string                    `json:"modelVersion,omitzero"`  // presented model version identifier
	Candidates    []wireCandidateWrite      `json:"candidates,omitzero"`    // single-element candidate slice when non-empty
	UsageMetadata *wireUsageMetadataWrite   `json:"usageMetadata,omitzero"` // serialized Gemini token counters
	Unknown       map[string]jsontext.Value `json:",embed"`                 // preserved same-origin response Extra members
}

// wireCandidate decodes one completion candidate object inside the GenerateContentResponse.candidates array.
type wireCandidate struct {
	Index             int                       `json:"index,omitzero"`             // zero-based candidate ordinal
	Content           *wireInboundCandidateBody `json:"content,omitzero"`           // model output role and parts container
	FinishReason      string                    `json:"finishReason,omitzero"`      // Gemini FinishReason enum label
	GroundingMetadata *wireGroundingMetadata    `json:"groundingMetadata,omitzero"` // optional web search queries and citations
	Unknown           map[string]jsontext.Value `json:",embed"`                     // unmodeled candidate fields such as safetyRatings
}

// wireInboundCandidateBody decodes candidates[0].content on responses and stream frames.
type wireInboundCandidateBody struct {
	Role    string                    `json:"role,omitzero"`  // "model" speaker role
	Parts   []jsontext.Value          `json:"parts,omitzero"` // ordered candidate output parts
	Unknown map[string]jsontext.Value `json:",embed"`         // unmodeled candidate content members
}

// wireCandidateWrite serializes candidates[0] on outgoing responses and stream frames.
type wireCandidateWrite struct {
	Index        int                       `json:"index"`                 // zero-based candidate ordinal (always 0)
	Content      *wireContent              `json:"content,omitzero"`      // model content wrapper carrying parts
	FinishReason string                    `json:"finishReason,omitzero"` // terminal Gemini FinishReason enum string
	Unknown      map[string]jsontext.Value `json:",embed"`                // preserved same-origin candidate Extra members
}

// wireGroundingMetadata decodes search queries and citation chunks from candidates[0].groundingMetadata.
type wireGroundingMetadata struct {
	WebSearchQueries []string             `json:"webSearchQueries,omitzero"` // executed Google Search query strings
	GroundingChunks  []wireGroundingChunk `json:"groundingChunks,omitzero"`  // cited web sources returned by grounding
}

// wireGroundingChunk decodes one cited grounding source entry inside groundingMetadata.groundingChunks.
type wireGroundingChunk struct {
	Web *wireGroundingWeb `json:"web,omitzero"` // web document URI and title metadata
}

// wireGroundingWeb decodes the URI and human-readable title of a grounded web source.
type wireGroundingWeb struct {
	URI   string `json:"uri,omitzero"`   // cited web page URL
	Title string `json:"title,omitzero"` // cited web page title
}

// DecodeRequest parses a raw Gemini GenerateContent JSON request body into a
// validated ir.Request. Malformed JSON, duplicate object keys, or invalid
// Gemini request fields return an error.
func (Codec) DecodeRequest(body []byte) (*ir.Request, error) {
	return DecodeRequest(body)
}

// EncodeRequest serializes req into deterministic Gemini GenerateContent JSON
// bytes under dialect and returns all incurred information-loss records. Nil
// requests, unsupported thinking controls, or invalid schemas return an error.
func (Codec) EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	return EncodeRequest(req, dialect)
}

// DecodeResponse parses a buffered Gemini GenerateContentResponse JSON body
// into a validated ir.Response. Inline Google error envelopes, duplicate keys,
// or malformed candidate parts return an error.
func (Codec) DecodeResponse(body []byte) (*ir.Response, error) {
	return DecodeResponse(body)
}

// EncodeResponse serializes resp into deterministic Gemini
// GenerateContentResponse JSON bytes using view and returns all incurred
// information-loss records. Nil responses or invalid completed tool arguments
// return an error.
func (Codec) EncodeResponse(resp *ir.Response, view ir.ClientView) ([]byte, []ir.Loss, error) {
	return EncodeResponse(resp, view)
}

// NewStreamDecoder allocates a fresh stateful StreamDecoder for upstream Gemini
// SSE frames so per-stream Sequencer and sparse usage counters stay isolated.
// Callers must allocate a separate decoder instance per upstream stream attempt.
func (Codec) NewStreamDecoder() *StreamDecoder {
	return NewStreamDecoder()
}

// NewStreamEncoder allocates a fresh stateful StreamEncoder configured with
// view for client-facing Gemini GenerateContent SSE response frames. Callers
// must allocate a separate encoder instance per downstream client stream.
func (Codec) NewStreamEncoder(view ir.ClientView) *StreamEncoder {
	return NewStreamEncoder(view)
}

// MarshalJSONTo writes p directly into enc so EncodeRequest avoids allocating
// and reparsing intermediate jsontext.Value buffers for every conversation part.
// Opaque raw parts write verbatim via WriteValue while typed parts encode with
// deterministic map ordering and invalid-UTF-8 tolerance matching marshalDeterministic.
func (p *wireOutPart) MarshalJSONTo(enc *jsontext.Encoder) error {
	if len(p.raw) > 0 {
		return enc.WriteValue(p.raw)
	}
	return json.MarshalEncode(enc, &p.part, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
}

// UnmarshalJSONFrom decodes a single JSON part object from dec into both p.raw
// and p.part so package tests inspecting wireOutRequest can round-trip parts.
// Non-object JSON values, syntax errors, or duplicate object keys return an error.
func (p *wireOutPart) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	val, err := dec.ReadValue()
	if err != nil {
		return err
	}
	p.raw = val.Clone()
	return unmarshalJSONObject(p.raw, &p.part)
}

// marshalDeterministic encodes v with sorted map keys so identical IR inputs
// produce byte-for-byte identical JSON outputs. Invalid UTF-8 sequences inside
// caller strings survive serialization without error.
func marshalDeterministic(v any) ([]byte, error) {
	return json.Marshal(v, json.Deterministic(true), jsontext.AllowInvalidUTF8(true))
}

// unmarshalJSONObject decodes data into dst after checking that data begins
// with a top-level '{' brace and contains a single valid JSON object without
// duplicate keys. Invalid UTF-8 inside JSON string values is tolerated.
func unmarshalJSONObject(data []byte, dst any) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return errors.New("empty JSON payload")
	}
	if trimmed[0] != '{' {
		return errors.New("JSON payload must be an object")
	}
	return json.Unmarshal(trimmed, dst, jsontext.AllowInvalidUTF8(true))
}

// unmarshalJSONValue decodes raw into dst using json/v2 while tolerating
// invalid UTF-8 sequences inside JSON string values. Duplicate object member
// names, syntax errors, or trailing tokens return an error.
func unmarshalJSONValue(raw []byte, dst any) error {
	return json.Unmarshal(raw, dst, jsontext.AllowInvalidUTF8(true))
}

// isValidSingleJSON reports whether raw encodes exactly one syntactically valid
// JSON value with no duplicate object keys and no trailing tokens.
func isValidSingleJSON(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return false
	}
	dec := jsontext.NewDecoder(bytes.NewReader(trimmed), jsontext.AllowInvalidUTF8(true))
	if _, err := dec.ReadValue(); err != nil {
		return false
	}
	_, err := dec.ReadToken()
	return err == io.EOF
}

// isValidSingleJSONObject reports whether raw encodes one syntactically valid
// JSON object beginning with '{' and containing no duplicate object keys.
func isValidSingleJSONObject(raw []byte) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && trimmed[0] == '{' && isValidSingleJSON(trimmed)
}

// compactRawJSON normalizes raw JSON bytes through json/v2's value encoder
// without reparsing numbers into float64 or reordering object member keys.
// Normalized bytes match post-marshal output on fixed-point round trips even
// when input strings contained invalid UTF-8 or non-canonical escapes.
func compactRawJSON(raw jsontext.Value) jsontext.Value {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return nil
	}
	v := jsontext.Value(bytes.Clone(trimmed))
	// Inputs come from json/v2 decoding; if re-marshaling ever rejects a fragment,
	// retaining the cloned trimmed bytes preserves the original wire value.
	normalized, err := marshalDeterministic(v)
	if err != nil {
		return v
	}
	return jsontext.Value(normalized)
}

// cloneValueMap returns a shallow map copy of src so callers can delete or add
// keys without mutating decoded Unknown maps. Empty or nil maps return nil so
// structs without extensions avoid allocating empty map headers.
func cloneValueMap(src map[string]jsontext.Value) map[string]jsontext.Value {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]jsontext.Value, len(src))
	maps.Copy(out, src)
	return out
}

// insertRawMember writes a compacted copy of raw under key in dst and returns
// the updated map. Nil destination maps are allocated automatically so callers
// can chain insertions without pre-allocating an Unknown map.
func insertRawMember(dst map[string]jsontext.Value, key string, raw jsontext.Value) map[string]jsontext.Value {
	if dst == nil {
		dst = make(map[string]jsontext.Value, 1)
	}
	dst[key] = compactRawJSON(raw)
	return dst
}

// marshalUnknownMap serializes a non-empty unknown-member map into deterministic
// JSON object bytes for storage in Extra. Empty or nil maps return (nil, nil)
// so modeled sub-objects without extra fields leave Extra untouched.
func marshalUnknownMap(m map[string]jsontext.Value) (jsontext.Value, error) {
	if len(m) == 0 {
		return nil, nil
	}
	normalized := make(map[string]jsontext.Value, len(m))
	for k, v := range m {
		normalized[k] = compactRawJSON(v)
	}
	raw, err := marshalDeterministic(normalized)
	if err != nil {
		return nil, err
	}
	return jsontext.Value(raw), nil
}

// extractSubObjectUnknown parses a preserved JSON object stored under key in
// topUnknown and deletes modeled stripKeys from the resulting map. Missing keys,
// non-object values, or maps left empty after stripping return nil, while
// wasPresent reports whether topUnknown originally carried an object at key.
func extractSubObjectUnknown(
	topUnknown map[string]jsontext.Value,
	key string,
	stripKeys ...string,
) (map[string]jsontext.Value, bool) {
	if len(topUnknown) == 0 {
		return nil, false
	}
	raw, ok := topUnknown[key]
	if !ok {
		return nil, false
	}
	var parsed map[string]jsontext.Value
	// Ignore non-object values in topUnknown[key] so scalar vendor overrides
	// cannot corrupt typed sub-object serialization.
	if unmarshalJSONObject(raw, &parsed) != nil {
		return nil, false
	}
	for _, sk := range stripKeys {
		delete(parsed, sk)
	}
	if len(parsed) == 0 {
		return nil, true
	}
	return parsed, true
}

// toExtra wraps unknown JSON object members decoded from a Gemini wire struct
// into an ir.Extra tagged with ProtocolGeminiGenerateContent. Empty maps return
// a zero ir.Extra so structs without extensions remain comparable.
func toExtra(unknown map[string]jsontext.Value) ir.Extra {
	if len(unknown) == 0 {
		return ir.Extra{}
	}
	cloned := make(map[string]jsontext.Value, len(unknown))
	for k, v := range unknown {
		cloned[k] = compactRawJSON(v)
	}
	return ir.Extra{
		Origin:  protocol.ProtocolGeminiGenerateContent,
		Members: cloned,
	}
}

// restoreOrDropExtra returns deep-copied Extra members when extra originated
// from Gemini GenerateContent. Non-zero Extra maps from other protocols are
// omitted and recorded as an extra_dropped Loss at path.
func restoreOrDropExtra(extra ir.Extra, path string, losses *[]ir.Loss) map[string]jsontext.Value {
	if extra.IsZero() {
		return nil
	}
	if extra.Origin == protocol.ProtocolGeminiGenerateContent {
		out := make(map[string]jsontext.Value, len(extra.Members))
		maps.Copy(out, extra.Members)
		for k, v := range out {
			out[k] = jsontext.Value(bytes.Clone(v))
		}
		return out
	}
	*losses = append(*losses, ir.Loss{
		Kind: ir.LossExtraDropped,
		Path: path,
	})
	return nil
}

// resolveClientModel selects view.Model when non-empty and falls back to
// upstreamModel when the caller left view.Model blank. When both model strings
// are empty, resolveClientModel returns "gemini-2.5-pro" so encoded responses
// always populate modelVersion.
func resolveClientModel(view ir.ClientView, upstreamModel string) string {
	if view.Model != "" {
		return view.Model
	}
	if upstreamModel != "" {
		return upstreamModel
	}
	return "gemini-2.5-pro"
}
