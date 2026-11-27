package tokenestimate

import (
	"bytes"
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/tiktoken-go/tokenizer"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	protocolopenai "github.com/TokenFlux/TokenRouter/internal/protocol/openai"
	"github.com/TokenFlux/TokenRouter/internal/protocol/wirejson"
)

// estimate.go estimates Responses and Anthropic input tokens using
// model-selected text encodings. Responses messages add fixed item and part
// overheads, while tools and unfamiliar JSON shapes contribute their compact
// serialized representation. Anthropic converts Messages JSON through
// protocol/convert and projects the five Responses counting fields through
// jsontext without subset wire types.
//
// These totals are estimates of request content, including metadata such as
// roles and call identifiers. Image URLs contribute text; a data URL
// contributes its header before the comma instead of its encoded payload.
// Instructions and structured item text are trimmed, while a plain string input
// is counted as decoded. Model-prefix rules choose between two encodings, with
// unfamiliar names using o200k_base; Anthropic additionally requires a positive
// minimum.

// These local overheads approximate Responses item and content framing; the
// Anthropic counting path applies a positive minimum after summing estimated
// content.
const (
	openAIResponsesInputItemTokenOverhead = 3  // token overhead per input item
	openAIResponsesContentPartOverhead    = 1  // token overhead per content part
	openAIInputTokensFallbackMinimum      = 1  // minimum Anthropic input token estimate
	defaultCountingToolNameMaxBytes       = 64 // OpenAI Responses function name byte limit
)

// Request contains the Responses input portions counted by the local
// estimator.
type Request struct {
	Model        string                         `json:"model"`                  // model name selecting tokenizer encoding
	Instructions string                         `json:"instructions,omitempty"` // instructions counted after trimming whitespace
	Input        json.RawMessage                `json:"input,omitempty"`        // JSON input string or items
	Tools        []protocolopenai.ResponsesTool `json:"tools,omitempty"`        // serialized tool definition inputs
	ToolChoice   json.RawMessage                `json:"tool_choice,omitempty"`  // JSON tool selection input
}

// CountingPayload holds the five Responses counting fields as raw JSON values
// so preflight projection and local estimation retain 64-bit integer literals,
// custom tool grammars, and hosted tool declarations without subset decoding.
type CountingPayload struct {
	Model        string           `json:"model"`                 // model name selecting upstream route or tokenizer encoding
	Instructions string           `json:"instructions,omitzero"` // instructions counted after trimming whitespace
	Input        jsontext.Value   `json:"input,omitzero"`        // raw JSON input string or item array
	Tools        []jsontext.Value `json:"tools,omitzero"`        // raw JSON tool declarations
	ToolChoice   jsontext.Value   `json:"tool_choice,omitzero"`  // raw JSON tool selection value
}

// wireCountingProjection captures the five counting fields from a JSON request
// object while validating sibling fields and duplicate keys through json/v2.
type wireCountingProjection struct {
	Model        string                    `json:"model"`        // top-level request model string
	Instructions string                    `json:"instructions"` // optional top-level instructions text
	Input        jsontext.Value            `json:"input"`        // raw input member value
	Tools        jsontext.Value            `json:"tools"`        // raw tools array or null member
	ToolChoice   jsontext.Value            `json:"tool_choice"`  // raw tool_choice member value
	Unknown      map[string]jsontext.Value `json:",embed"`       // unprojected sibling members validated during decode
}

// wireCountingInputItem captures the recognized metadata and content members
// counted for one structured Responses input item.
type wireCountingInputItem struct {
	Type      string                    `json:"type"`      // item kind discriminator
	Role      string                    `json:"role"`      // message speaker role
	Name      string                    `json:"name"`      // tool or speaker name
	Arguments string                    `json:"arguments"` // serialized function argument text
	Output    jsontext.Value            `json:"output"`    // raw string or structured tool output value
	CallID    string                    `json:"call_id"`   // tool result correlation identifier
	ID        string                    `json:"id"`        // upstream item identifier
	Content   jsontext.Value            `json:"content"`   // raw string or part-array message content
	Unknown   map[string]jsontext.Value `json:",embed"`    // unmodeled item fields ignored by local counting
}

// wireCountingContentPart captures the recognized fields counted for one
// structured content part inside a Responses message item.
type wireCountingContentPart struct {
	Type     string                    `json:"type"`      // content part kind discriminator
	Text     string                    `json:"text"`      // input or output text string
	ImageURL string                    `json:"image_url"` // image URL or data URI
	Unknown  map[string]jsontext.Value `json:",embed"`    // unmodeled part fields ignored by local counting
}

// IsZero reports whether p carries no model, instructions, input, tools, or
// tool_choice bytes.
func (p CountingPayload) IsZero() bool {
	return p.Model == "" &&
		p.Instructions == "" &&
		len(p.Input) == 0 &&
		len(p.Tools) == 0 &&
		len(p.ToolChoice) == 0
}

// ProjectCountingPayload decodes one JSON request object with invalid-UTF-8
// tolerance and duplicate-key rejection, projecting only the five Responses
// counting fields {model, instructions, input, tools, tool_choice}. Non-object
// roots, duplicate member names, trailing tokens, or malformed tools arrays
// return an error.
func ProjectCountingPayload(body []byte) (CountingPayload, error) {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return CountingPayload{}, errors.New("counting payload must be a JSON object")
	}
	var raw wireCountingProjection
	if err := unmarshalCountingSingleValue(trimmed, &raw); err != nil {
		return CountingPayload{}, err
	}

	out := CountingPayload{
		Model:        raw.Model,
		Instructions: raw.Instructions,
	}
	if len(raw.Input) > 0 {
		compacted, err := compactCountingRawValue(raw.Input)
		if err != nil {
			return CountingPayload{}, err
		}
		out.Input = compacted
	}
	if len(raw.Tools) > 0 && !bytes.Equal(bytes.TrimSpace(raw.Tools), []byte("null")) {
		var tools []jsontext.Value
		if err := unmarshalCountingSingleValue(raw.Tools, &tools); err != nil {
			return CountingPayload{}, fmt.Errorf("decode tools array: %w", err)
		}
		if len(tools) > 0 {
			out.Tools = make([]jsontext.Value, len(tools))
			for i, elem := range tools {
				compacted, err := compactCountingRawValue(elem)
				if err != nil {
					return CountingPayload{}, fmt.Errorf("compact tools[%d]: %w", i, err)
				}
				out.Tools[i] = compacted
			}
		}
	}
	if len(raw.ToolChoice) > 0 {
		compacted, err := compactCountingRawValue(raw.ToolChoice)
		if err != nil {
			return CountingPayload{}, err
		}
		out.ToolChoice = compacted
	}
	return out, nil
}

// MarshalCountingPayload serializes p as compact UTF-8 JSON with literal HTML
// characters and field declaration order {model, instructions, input, tools,
// tool_choice}. Invalid UTF-8 byte sequences retained inside raw string values
// are replaced with U+FFFD, while structural encoding errors return without a
// trailing newline.
func MarshalCountingPayload(p CountingPayload) ([]byte, error) {
	return jsonv2.Marshal(p, jsontext.AllowInvalidUTF8(true))
}

// Anthropic estimates request input tokens by converting Anthropic Messages
// JSON to OpenAI Responses via protocol/convert and counting the projected
// fields through EstimateCountingPayload. Missing models, duplicate JSON keys,
// and conversion failures return an error; completed estimates below one clamp
// to one.
func Anthropic(body []byte) (int, error) {
	ex, err := convert.NewExchange(body, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolOpenAIResponses,
			Dialect: convert.Dialect{
				Name:             "openai",
				Tools:            convert.ToolSupportFunction | convert.ToolSupportHostedWebSearch,
				ToolNameMaxBytes: defaultCountingToolNameMaxBytes,
				ReplayScheme:     ir.SchemeOpenAI,
			},
		},
	})
	if err != nil {
		return 0, fmt.Errorf("parse anthropic count_tokens request: %w", err)
	}
	origModel := ex.Info().Model
	if strings.TrimSpace(origModel) == "" {
		return 0, fmt.Errorf("parse anthropic count_tokens request: model is required")
	}

	convertedBytes, err := ex.UpstreamRequest()
	if err != nil {
		return 0, fmt.Errorf("convert anthropic request to responses: %w", err)
	}
	payload, err := ProjectCountingPayload(convertedBytes)
	if err != nil {
		return 0, fmt.Errorf("project converted responses counting fields: %w", err)
	}
	payload.Model = origModel

	estimated, err := EstimateCountingPayload(payload)
	if err != nil {
		return 0, fmt.Errorf("estimate input tokens: %w", err)
	}
	// Represent an accepted empty conversation with a positive estimate.
	if estimated < openAIInputTokensFallbackMinimum {
		estimated = openAIInputTokensFallbackMinimum
	}
	return estimated, nil
}

// ResponsesBytes projects the five counting fields from a raw Responses JSON
// request body via jsontext and returns their local token estimate. Malformed
// JSON, duplicate object keys, or tokenizer failures return an error.
func ResponsesBytes(body []byte) (int, error) {
	payload, err := ProjectCountingPayload(body)
	if err != nil {
		return 0, err
	}
	return EstimateCountingPayload(payload)
}

// EstimateCountingPayload sums estimated tokens for instructions, input, raw
// tool declarations, and raw tool_choice using the model-selected tokenizer
// codec. Tools and tool_choice retain lossless jsontext representations without
// subset struct re-serialization, while negative final accumulators return zero.
func EstimateCountingPayload(p CountingPayload) (int, error) {
	codec, err := openAIInputTokensCodecForModel(p.Model)
	if err != nil {
		return 0, err
	}

	total := 0
	addCount := func(text string) error {
		text = strings.TrimSpace(text)
		if text == "" {
			return nil
		}
		n, err := codec.Count(text)
		if err != nil {
			return err
		}
		total += n
		return nil
	}

	if err := addCount(p.Instructions); err != nil {
		return 0, err
	}
	inputTokens, err := estimateCountingInputValue(codec, p.Input)
	if err != nil {
		return 0, err
	}
	total += inputTokens

	for _, tool := range p.Tools {
		compacted, err := compactCountingJSONString(tool)
		if err != nil {
			return 0, err
		}
		if err := addCount(compacted); err != nil {
			return 0, err
		}
	}
	if len(p.ToolChoice) > 0 {
		compacted, err := compactCountingJSONString(p.ToolChoice)
		if err != nil {
			return 0, err
		}
		if err := addCount(compacted); err != nil {
			return 0, err
		}
	}

	// Suppress a negative final accumulator as zero so callers do not report
	// a negative estimate; earlier positive overflow cases have no separate check here.
	if total < 0 {
		return 0, nil
	}
	return total, nil
}

// estimateCountingInputValue counts raw Responses input JSON as plain string
// text, structured input items, or compacted unfamiliar JSON without decoding
// through subset protocolopenai structs. Malformed JSON returns an error.
func estimateCountingInputValue(codec tokenizer.Codec, raw jsontext.Value) (int, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return 0, nil
	}
	if trimmed[0] == '"' {
		var plainText string
		if err := unmarshalCountingSingleValue(trimmed, &plainText); err == nil {
			return codec.Count(plainText)
		}
	}
	if trimmed[0] == '[' {
		var items []wireCountingInputItem
		if err := unmarshalCountingSingleValue(trimmed, &items); err == nil {
			return estimateCountingInputItems(codec, items)
		}
	}

	// Count unfamiliar valid JSON in its compact textual form so accepted input
	// shapes still contribute content while malformed JSON remains a reported error.
	compacted, err := compactCountingJSONString(trimmed)
	if err != nil {
		return 0, err
	}
	return codec.Count(compacted)
}

// estimateCountingInputItems sums fixed item framing overheads with recognized
// metadata and content fields across decoded wireCountingInputItem records.
// Non-string tool outputs contribute their raw JSON representation, while JSON
// or tokenizer errors terminate estimation immediately.
func estimateCountingInputItems(codec tokenizer.Codec, items []wireCountingInputItem) (int, error) {
	total := 0
	countText := func(text string) error {
		text = strings.TrimSpace(text)
		if text == "" {
			return nil
		}
		n, err := codec.Count(text)
		if err != nil {
			return err
		}
		total += n
		return nil
	}

	for _, item := range items {
		// Add framing cost even when an input item has no countable text.
		total += openAIResponsesInputItemTokenOverhead
		if err := countText(item.Role); err != nil {
			return 0, err
		}
		if item.Type != "" && item.Type != "message" {
			if err := countText(item.Type); err != nil {
				return 0, err
			}
		}
		if err := countText(item.Name); err != nil {
			return 0, err
		}
		if err := countText(item.Arguments); err != nil {
			return 0, err
		}
		outputText, err := decodeCountingItemOutputText(item.Output)
		if err != nil {
			return 0, err
		}
		if err := countText(outputText); err != nil {
			return 0, err
		}
		if err := countText(item.CallID); err != nil {
			return 0, err
		}
		if err := countText(item.ID); err != nil {
			return 0, err
		}

		contentTrimmed := bytes.TrimSpace(item.Content)
		if len(contentTrimmed) == 0 || bytes.Equal(contentTrimmed, []byte("null")) {
			continue
		}
		if contentTrimmed[0] == '"' {
			var contentText string
			if err := unmarshalCountingSingleValue(contentTrimmed, &contentText); err == nil {
				if err := countText(contentText); err != nil {
					return 0, err
				}
				continue
			}
		}
		if contentTrimmed[0] == '[' {
			var parts []wireCountingContentPart
			if err := unmarshalCountingSingleValue(contentTrimmed, &parts); err == nil {
				for _, part := range parts {
					// Count each part's framing before its recognized textual contribution.
					total += openAIResponsesContentPartOverhead
					switch part.Type {
					case "input_text", "output_text", "text":
						if err := countText(part.Text); err != nil {
							return 0, err
						}
					case "input_image":
						if err := countText(estimateOpenAIInputImageText(part.ImageURL)); err != nil {
							return 0, err
						}
					default:
						if err := countText(part.Type); err != nil {
							return 0, err
						}
					}
				}
				continue
			}
		}

		// Retain unfamiliar valid content as compact JSON text; dropping the whole
		// value would omit its contribution merely because it uses another shape.
		compacted, err := compactCountingJSONString(contentTrimmed)
		if err != nil {
			return 0, err
		}
		if err := countText(compacted); err != nil {
			return 0, err
		}
	}

	return total, nil
}

// decodeCountingItemOutputText extracts string tool output or retains non-null
// structured output JSON text for counting parity with ResponsesInputItem.
// Empty or null outputs return an empty string without error, while malformed
// JSON returns the decoder error.
func decodeCountingItemOutputText(raw jsontext.Value) (string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return "", nil
	}
	if trimmed[0] == '"' {
		var text string
		if err := unmarshalCountingSingleValue(trimmed, &text); err != nil {
			return "", err
		}
		return text, nil
	}
	compacted, err := compactCountingJSONString(trimmed)
	if err != nil {
		return "", err
	}
	return compacted, nil
}

// unmarshalCountingSingleValue decodes a single complete JSON document from data
// into dst with invalid-UTF-8 tolerance and duplicate-key rejection. Any
// trailing token or syntax error after the first value returns an error.
func unmarshalCountingSingleValue(data []byte, dst any) error {
	dec := jsontext.NewDecoder(bytes.NewReader(data), jsontext.AllowInvalidUTF8(true))
	if err := jsonv2.UnmarshalDecode(dec, dst); err != nil {
		return err
	}
	if _, err := dec.ReadToken(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON token")
		}
		return err
	}
	return nil
}

// compactCountingRawValue validates raw as a single JSON value while allowing
// invalid UTF-8 inside string literals and returns a detached compacted copy.
// Duplicate object keys, trailing tokens, or syntax failures return an error.
func compactCountingRawValue(raw []byte) (jsontext.Value, error) {
	var val jsontext.Value
	if err := unmarshalCountingSingleValue(raw, &val); err != nil {
		return nil, err
	}
	if err := val.Compact(); err != nil {
		return nil, err
	}
	return val, nil
}

// compactCountingJSONString compacts valid JSON bytes via jsontext without
// rounding 64-bit integer literals. Blank input returns an empty string with a
// nil error, while malformed JSON returns the decoder error.
func compactCountingJSONString(raw []byte) (string, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", nil
	}
	val, err := compactCountingRawValue(raw)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// Responses sums estimated tokens for instructions, input, tool definitions,
// and tool choice using the model-selected codec. Tools use their wire JSON,
// nonempty tool choice must be valid JSON, and tokenizer or encoding errors
// stop the estimate.
//
// Structured input adds fixed framing costs and counts its recognized text and
// identity fields, while other valid JSON is compacted before counting.
// Instructions and serialized additions are trimmed before tokenization. The
// result is a local approximation rather than an upstream usage observation; a
// negative final accumulator returns zero, with no broader overflow guarantee.
func Responses(req Request) (int, error) {
	codec, err := openAIInputTokensCodecForModel(req.Model)
	if err != nil {
		return 0, err
	}

	total := 0
	addCount := func(text string) error {
		text = strings.TrimSpace(text)
		if text == "" {
			return nil
		}
		n, err := codec.Count(text)
		if err != nil {
			return err
		}
		total += n
		return nil
	}

	if err := addCount(req.Instructions); err != nil {
		return 0, err
	}
	inputTokens, err := estimateOpenAIInputTokensForInput(codec, req.Input)
	if err != nil {
		return 0, err
	}
	total += inputTokens

	for _, tool := range req.Tools {
		raw, err := wirejson.Marshal(tool)
		if err != nil {
			return 0, err
		}
		if err := addCount(string(raw)); err != nil {
			return 0, err
		}
	}
	if len(req.ToolChoice) > 0 {
		compacted, err := compactOpenAIInputTokensJSON(req.ToolChoice)
		if err != nil {
			return 0, err
		}
		if err := addCount(compacted); err != nil {
			return 0, err
		}
	}

	// Suppress a negative final accumulator as zero so callers do not report
	// a negative estimate; earlier positive overflow cases have no separate check here.
	if total < 0 {
		return 0, nil
	}
	return total, nil
}

// estimateOpenAIInputTokensForInput selects the counting representation for
// one raw input value. Blank input contributes zero, a decoded string is
// counted directly, and a decoded input-item array uses the structured
// estimator.
//
// When neither recognized shape decodes, compact valid JSON contributes its
// textual token count. Invalid JSON returns the compaction error. The
// plain-string path retains its decoded whitespace, whereas item text is
// trimmed by its own counting helper; the representation therefore determines
// both framing overhead and the whitespace counted.
func estimateOpenAIInputTokensForInput(codec tokenizer.Codec, raw json.RawMessage) (int, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return 0, nil
	}

	var plainText string
	if err := json.Unmarshal(raw, &plainText); err == nil {
		return codec.Count(plainText)
	}

	var items []protocolopenai.ResponsesInputItem
	if err := json.Unmarshal(raw, &items); err == nil {
		return estimateOpenAIInputTokensForInputItems(codec, items)
	}

	// Count unfamiliar valid JSON in its compact textual form so accepted input
	// shapes still contribute content while malformed JSON remains a reported error.
	compacted, err := compactOpenAIInputTokensJSON(raw)
	if err != nil {
		return 0, err
	}
	return codec.Count(compacted)
}

// estimateOpenAIInputTokensForInputItems sums fixed item overhead with
// recognized metadata and content text. Roles, non-message types, names,
// arguments, outputs, and call or item identifiers contribute trimmed text
// under the supplied codec.
//
// String content adds text directly; decoded part arrays additionally add each
// part's overhead and choose text, image URL header, or an unfamiliar type
// label. Other valid content JSON is compacted and counted. These
// representation choices retain a deterministic local approximation for tool
// and multimodal inputs, while tokenizer or JSON errors end the estimate.
func estimateOpenAIInputTokensForInputItems(codec tokenizer.Codec, items []protocolopenai.ResponsesInputItem) (int, error) {
	total := 0
	countText := func(text string) error {
		text = strings.TrimSpace(text)
		if text == "" {
			return nil
		}
		n, err := codec.Count(text)
		if err != nil {
			return err
		}
		total += n
		return nil
	}

	for _, item := range items {
		// Add framing cost even when an input item has no countable text.
		total += openAIResponsesInputItemTokenOverhead
		if err := countText(item.Role); err != nil {
			return 0, err
		}
		if item.Type != "" && item.Type != "message" {
			if err := countText(item.Type); err != nil {
				return 0, err
			}
		}
		if err := countText(item.Name); err != nil {
			return 0, err
		}
		if err := countText(item.Arguments); err != nil {
			return 0, err
		}
		if err := countText(item.Output); err != nil {
			return 0, err
		}
		if err := countText(item.CallID); err != nil {
			return 0, err
		}
		if err := countText(item.ID); err != nil {
			return 0, err
		}

		if len(bytes.TrimSpace(item.Content)) == 0 {
			continue
		}

		var contentText string
		if err := json.Unmarshal(item.Content, &contentText); err == nil {
			if err := countText(contentText); err != nil {
				return 0, err
			}
			continue
		}

		var parts []protocolopenai.ResponsesContentPart
		if err := json.Unmarshal(item.Content, &parts); err == nil {
			for _, part := range parts {
				// Count each part's framing before its recognized textual contribution.
				total += openAIResponsesContentPartOverhead
				switch part.Type {
				case "input_text", "output_text", "text":
					if err := countText(part.Text); err != nil {
						return 0, err
					}
				case "input_image":
					if err := countText(estimateOpenAIInputImageText(part.ImageURL)); err != nil {
						return 0, err
					}
				default:
					if err := countText(part.Type); err != nil {
						return 0, err
					}
				}
			}
			continue
		}

		// Retain unfamiliar valid content as compact JSON text; dropping the whole
		// value would omit its contribution merely because it uses another shape.
		compacted, err := compactOpenAIInputTokensJSON(item.Content)
		if err != nil {
			return 0, err
		}
		if err := countText(compacted); err != nil {
			return 0, err
		}
	}

	return total, nil
}

// estimateOpenAIInputImageText returns trimmed URL text for image estimation.
// A data URL with a comma contributes its header alone; ordinary URLs and
// comma-free data values retain their trimmed text.
func estimateOpenAIInputImageText(imageURL string) string {
	trimmed := strings.TrimSpace(imageURL)
	if trimmed == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(trimmed), "data:") {
		// Exclude the encoded payload from this text approximation.
		if comma := strings.Index(trimmed, ","); comma > 0 {
			return trimmed[:comma]
		}
	}
	return trimmed
}

// compactOpenAIInputTokensJSON returns valid JSON with insignificant
// whitespace removed for textual counting. Blank input returns an empty string
// successfully; nonblank invalid JSON returns the decoder's compaction error.
func compactOpenAIInputTokensJSON(raw json.RawMessage) (string, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return "", nil
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// openAIInputTokensCodecForModel loads the codec chosen by the estimator's
// model-prefix rules. EncodingForModel supplies one of its two supported
// encodings, and tokenizer construction errors propagate to the caller.
func openAIInputTokensCodecForModel(model string) (tokenizer.Codec, error) {
	switch EncodingForModel(model) {
	case tokenizer.Cl100kBase:
		return tokenizer.Get(tokenizer.Cl100kBase)
	default:
		return tokenizer.Get(tokenizer.O200kBase)
	}
}

// EncodingForModel chooses cl100k_base for GPT-3.5, older GPT-4 names, and
// text embeddings after trimming and lowercasing. GPT-4o, GPT-4.1, and every
// other prefix use o200k_base, including unknown model names.
func EncodingForModel(model string) tokenizer.Encoding {
	normalized := strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.HasPrefix(normalized, "gpt-3.5"),
		strings.HasPrefix(normalized, "gpt-4") &&
			!strings.HasPrefix(normalized, "gpt-4o") &&
			!strings.HasPrefix(normalized, "gpt-4.1"),
		strings.HasPrefix(normalized, "text-embedding-"):
		return tokenizer.Cl100kBase
	default:
		return tokenizer.O200kBase
	}
}
