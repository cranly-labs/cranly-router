package convert

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// output_meaning.go derives Semantic and Terminal frame annotations and
// stream/response content observations directly from structured IR events and
// typed Responses wire event envelopes without re-parsing output JSON via gjson.
// Gateway streaming loops buffer non-semantic leading frames so retryable
// upstream failures before visible content or tool calls still failover cleanly,
// and rely on Terminal to stop SSE keepalives and mark stream completion.
//
// Computing delivery flags from structured event kinds covers function, custom,
// and client_search tool calls, visible text, reasoning, and refusal deltas,
// and all normal or error terminal frames ([DONE], message_stop,
// response.completed, response.incomplete, response.failed, error, and Chat
// error chunks) across converted and tool-adapted streams.

// wireResponsesStreamProbe decodes minimal top-level fields from a native
// OpenAI Responses SSE JSON payload for same-protocol tool stream classification.
type wireResponsesStreamProbe struct {
	Type            string                `json:"type"`                       // Responses SSE event discriminator
	Object          string                `json:"object,omitzero"`            // top-level object tag on bare response envelopes
	OutputIndex     *int                  `json:"output_index,omitzero"`      // item slot index on item and delta events
	ItemID          string                `json:"item_id,omitzero"`           // target item identifier on incremental delta events
	Delta           string                `json:"delta,omitzero"`             // incremental text or argument delta
	PartialImageB64 string                `json:"partial_image_b64,omitzero"` // incremental base64 image preview on partial_image events
	Item            *wireResponsesItemTag `json:"item,omitzero"`              // added or completed output item header
	Error           jsontext.Value        `json:"error,omitzero"`             // top-level error object on bare error frames
}

// wireResponsesItemTag captures the discriminator, item ID, and content blocks
// of an item on response.output_item.added or response.output_item.done.
type wireResponsesItemTag struct {
	ID               string                   `json:"id,omitzero"`                // output item identifier
	Type             string                   `json:"type"`                       // function_call, custom_tool_call, tool_search_call, message, or reasoning
	Summary          []wireResponsesTextBlock `json:"summary,omitzero"`           // reasoning summary blocks when present
	Content          []wireResponsesTextBlock `json:"content,omitzero"`           // message or reasoning content blocks when present
	EncryptedContent string                   `json:"encrypted_content,omitzero"` // continuity ciphertext on summary-less reasoning items
}

// wireResponsesTextBlock captures text and refusal strings inside a completed
// message or reasoning block on response.output_item.done.
type wireResponsesTextBlock struct {
	Type    string `json:"type,omitzero"`    // output_text, refusal, summary_text, or reasoning_text
	Text    string `json:"text,omitzero"`    // visible assistant or reasoning text
	Refusal string `json:"refusal,omitzero"` // assistant refusal message string
}

// outputObserver tracks cumulative content flags and validates Semantic and
// Terminal frame flags across a sequential IR event stream.
type outputObserver struct {
	hasContent  bool // true once visible text, reasoning, refusal, or callable tool call appeared
	hasText     bool // true once non-empty visible PartText content appeared
	hasToolCall bool // true once at least one non-hosted PartToolCall appeared
}

// observeAndAnnotate updates o from ev and enforces structural Semantic and
// Terminal invariants on frames emitted by the client StreamEncoder for ev.
// Non-terminal events clear premature Terminal flags, while terminal events
// mark the final frame Terminal so gateway keepalive loops stop cleanly.
func (o *outputObserver) observeAndAnnotate(ev ir.Event, frames []sse.Frame) {
	switch ev.Kind {
	case ir.EventPartStart:
		if ev.Part != nil {
			o.observePart(*ev.Part)
		}
		clearPrematureTerminal(frames)

	case ir.EventPartDelta:
		if ev.Delta != nil {
			if ev.Delta.Text != "" {
				o.hasContent = true
			}
			if ev.Delta.Arguments != "" {
				o.hasContent = true
			}
		}
		clearPrematureTerminal(frames)

	case ir.EventPartEnd:
		if ev.Part != nil {
			o.observePart(*ev.Part)
		}
		clearPrematureTerminal(frames)

	case ir.EventResponseStart, ir.EventUsage:
		for i := range frames {
			frames[i].Semantic = false
			frames[i].Terminal = false
		}

	case ir.EventResponseEnd:
		for i := range frames {
			frames[i].Terminal = i == len(frames)-1
		}

	case ir.EventError:
		for i := range frames {
			frames[i].Semantic = false
			frames[i].Terminal = i == len(frames)-1
		}
	}
}

// observePart updates cumulative content, text, and callable tool flags from p.
func (o *outputObserver) observePart(p ir.Part) {
	switch p.Kind {
	case ir.PartText:
		if p.Text != "" {
			o.hasContent = true
			o.hasText = true
		}
	case ir.PartRefusal:
		if p.Text != "" {
			o.hasContent = true
		}
	case ir.PartReasoning:
		if p.Reasoning != nil {
			if strings.Join(p.Reasoning.Segments, "") != "" ||
				(p.Reasoning.Token != nil && p.Reasoning.Token.Data != "") {
				o.hasContent = true
			}
		}
	case ir.PartToolCall:
		if p.Call != nil && p.Call.Tool.Kind != ir.ToolHosted {
			o.hasContent = true
			o.hasToolCall = true
		}
	}
}

// clearPrematureTerminal ensures non-terminal IR events never emit frames with Terminal=true.
func clearPrematureTerminal(frames []sse.Frame) {
	for i := range frames {
		frames[i].Terminal = false
	}
}

// ResponseHasSemanticContent reports whether resp carries any non-empty visible
// text, refusal message, reasoning text or continuity token, or non-hosted tool
// invocation. Nil responses and empty completions return false.
func ResponseHasSemanticContent(resp *ir.Response) bool {
	if resp == nil {
		return false
	}
	var obs outputObserver
	for _, p := range resp.Parts {
		obs.observePart(p)
		if obs.hasContent {
			return true
		}
	}
	return false
}

// classifyResponsesWireFrameWithEvent inspects a single native Responses SSE
// payload and optional SSE event header, falling back to fallbackEvent when the
// JSON payload omits a top-level type discriminator. Empty payloads or non-JSON
// lines other than [DONE] return ("", false, false).
//
// Non-empty text, reasoning, callable tool, and hosted-tool preview or argument
// deltas mark Semantic=true so gateway streaming loops flush live progress,
// while lifecycle completion, cancellation, error events, and [DONE] sentinels
// mark Terminal=true so keepalive timers terminate cleanly.
func classifyResponsesWireFrameWithEvent(payload []byte, fallbackEvent string) (string, bool, bool) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return "", false, false
	}
	if bytes.Equal(trimmed, []byte("[DONE]")) {
		return "", false, true
	}

	var probe wireResponsesStreamProbe
	if err := json.Unmarshal(trimmed, &probe, jsontext.AllowInvalidUTF8(true)); err != nil {
		// Treat non-JSON or malformed upstream lines as passive frames rather than
		// aborting stream classification on vendor keepalive or comment payloads.
		return "", false, false
	}
	evType := strings.TrimSpace(probe.Type)
	if evType == "" && strings.TrimSpace(fallbackEvent) != "" {
		evType = strings.TrimSpace(fallbackEvent)
	}
	switch evType {
	case "response.completed", "response.done", "response.incomplete",
		"response.failed", "response.cancelled", "response.canceled", "error":
		return evType, false, true

	case "response.output_text.delta", "response.refusal.delta",
		"response.reasoning_summary_text.delta", "response.reasoning_text.delta",
		"response.function_call_arguments.delta", "response.custom_tool_call_input.delta",
		"response.mcp_call_arguments.delta", "response.code_interpreter_call_code.delta",
		"response.audio.delta", "response.audio_transcript.delta":
		return evType, probe.Delta != "", false

	case "response.image_generation_call.partial_image":
		return evType, probe.PartialImageB64 != "", false

	case "response.output_item.added":
		if probe.Item != nil {
			switch strings.TrimSpace(probe.Item.Type) {
			case "function_call", "custom_tool_call", "tool_search_call":
				return evType, true, false
			}
		}
		return evType, false, false

	case "response.output_item.done":
		if probe.Item != nil && strings.TrimSpace(probe.Item.Type) == "reasoning" &&
			probe.Item.EncryptedContent != "" && len(probe.Item.Summary) == 0 && len(probe.Item.Content) == 0 {
			return evType, true, false
		}
		return evType, false, false

	case "":
		if len(probe.Error) > 0 && probe.Error.Kind() != 'n' {
			return "", false, true
		}
		return "", false, false

	default:
		return evType, false, false
	}
}

// inspectResponsesStreamFrameItem extracts item correlation keys (output_index,
// hasOutputIndex, itemID) alongside whether a response.output_item.done payload
// delivers semantic message, reasoning, or callable tool content on its own.
// Compatible upstreams that emit one-shot output_item.done frames without prior
// output_item.added or delta events rely on this check to mark Semantic=true.
func inspectResponsesStreamFrameItem(payload []byte) (int, bool, string, bool) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return 0, false, "", false
	}
	var probe wireResponsesStreamProbe
	if err := json.Unmarshal(trimmed, &probe, jsontext.AllowInvalidUTF8(true)); err != nil {
		return 0, false, "", false
	}
	outIdx := 0
	hasOutIdx := false
	if probe.OutputIndex != nil {
		outIdx = *probe.OutputIndex
		hasOutIdx = true
	}
	itemID := strings.TrimSpace(probe.ItemID)
	if itemID == "" && probe.Item != nil {
		itemID = strings.TrimSpace(probe.Item.ID)
	}
	if probe.Item == nil {
		return outIdx, hasOutIdx, itemID, false
	}
	switch strings.TrimSpace(probe.Item.Type) {
	case "function_call", "custom_tool_call", "tool_search_call":
		return outIdx, hasOutIdx, itemID, true
	case "message":
		for _, blk := range probe.Item.Content {
			if blk.Text != "" || blk.Refusal != "" {
				return outIdx, hasOutIdx, itemID, true
			}
		}
	case "reasoning":
		if probe.Item.EncryptedContent != "" {
			return outIdx, hasOutIdx, itemID, true
		}
		for _, blk := range probe.Item.Summary {
			if blk.Text != "" {
				return outIdx, hasOutIdx, itemID, true
			}
		}
		for _, blk := range probe.Item.Content {
			if blk.Text != "" {
				return outIdx, hasOutIdx, itemID, true
			}
		}
	}
	return outIdx, hasOutIdx, itemID, false
}
