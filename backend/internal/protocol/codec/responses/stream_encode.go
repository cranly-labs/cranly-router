package responses

import (
	"bytes"
	"encoding/json/jsontext"
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// stream_encode.go encodes canonical sequential ir.Event streams into client
// OpenAI Responses SSE frames with strictly contiguous sequence_number values
// and Semantic/Terminal delivery markers.
//
// Consecutive PartText and PartRefusal events coalesce into one assistant
// message output item with advancing content_index lifecycles (ac38d88f0),
// reasoning parts open and close summary_index lifecycles before emitting
// output_item.done with encrypted_content, and tool argument deltas omit name
// fields (091ec1f41) while guarding EventPartEnd suffix flushes with
// strings.HasPrefix. Runtime.NextID calls execute in the exact same order as
// EncodeResponse so Law S6 buffered-vs-streaming parity holds across all shapes,
// and mid-stream aborts close every open nested lifecycle before response.failed.

// wireLifecycleEventWrite serializes response.created, response.in_progress,
// response.completed, response.incomplete, and response.failed SSE events.
type wireLifecycleEventWrite struct {
	Type           string            `json:"type"`            // lifecycle event discriminator
	SequenceNumber int64             `json:"sequence_number"` // contiguous zero-based sequence counter
	Response       wireResponseWrite `json:"response"`        // response envelope snapshot
}

// wireOutputItemEventWrite serializes outgoing response.output_item.added and response.output_item.done SSE JSON frames.
type wireOutputItemEventWrite struct {
	Type           string         `json:"type"`            // output_item.added or output_item.done
	SequenceNumber int64          `json:"sequence_number"` // contiguous sequence counter
	OutputIndex    int            `json:"output_index"`    // zero-based output item index
	Item           jsontext.Value `json:"item"`            // serialized item object
}

// wireContentPartEventWrite serializes outgoing response.content_part.added and response.content_part.done SSE JSON frames.
type wireContentPartEventWrite struct {
	Type           string         `json:"type"`            // content_part.added or content_part.done
	SequenceNumber int64          `json:"sequence_number"` // contiguous sequence counter
	ItemID         string         `json:"item_id"`         // owning message item identifier
	OutputIndex    int            `json:"output_index"`    // owning message output_index
	ContentIndex   int            `json:"content_index"`   // zero-based message content index
	Part           jsontext.Value `json:"part"`            // content block skeleton or final snapshot
}

// wireContentDeltaEventWrite serializes outgoing response.output_text.delta and response.refusal.delta SSE JSON frames.
type wireContentDeltaEventWrite struct {
	Type           string `json:"type"`            // output_text.delta or refusal.delta
	SequenceNumber int64  `json:"sequence_number"` // contiguous sequence counter
	ItemID         string `json:"item_id"`         // owning message item identifier
	OutputIndex    int    `json:"output_index"`    // owning message output_index
	ContentIndex   int    `json:"content_index"`   // open message content_index
	Delta          string `json:"delta"`           // incremental text or refusal fragment
}

// wireTextDoneEventWrite serializes outgoing response.output_text.done SSE JSON completion frames for message text blocks.
type wireTextDoneEventWrite struct {
	Type           string `json:"type"`            // literal "response.output_text.done" discriminator
	SequenceNumber int64  `json:"sequence_number"` // contiguous sequence counter
	ItemID         string `json:"item_id"`         // owning message item identifier
	OutputIndex    int    `json:"output_index"`    // owning message output_index
	ContentIndex   int    `json:"content_index"`   // open message content_index
	Text           string `json:"text"`            // full accumulated output_text string
}

// wireRefusalDoneEventWrite serializes outgoing response.refusal.done SSE JSON completion frames for refusal blocks.
type wireRefusalDoneEventWrite struct {
	Type           string `json:"type"`            // literal "response.refusal.done" discriminator
	SequenceNumber int64  `json:"sequence_number"` // contiguous sequence counter
	ItemID         string `json:"item_id"`         // owning message item identifier
	OutputIndex    int    `json:"output_index"`    // owning message output_index
	ContentIndex   int    `json:"content_index"`   // open message content_index
	Refusal        string `json:"refusal"`         // full accumulated refusal string
}

// wireSummaryPartEventWrite serializes outgoing response.reasoning_summary_part.added and .done SSE JSON frames.
type wireSummaryPartEventWrite struct {
	Type           string          `json:"type"`            // reasoning_summary_part.added or .done
	SequenceNumber int64           `json:"sequence_number"` // contiguous sequence counter
	ItemID         string          `json:"item_id"`         // owning reasoning item identifier
	OutputIndex    int             `json:"output_index"`    // owning reasoning output_index
	SummaryIndex   int             `json:"summary_index"`   // zero-based summary segment index
	Part           wireSummaryText `json:"part"`            // summary_text segment object
}

// wireSummaryTextDeltaEventWrite serializes outgoing response.reasoning_summary_text.delta SSE JSON frames for summaries.
type wireSummaryTextDeltaEventWrite struct {
	Type           string `json:"type"`            // literal "response.reasoning_summary_text.delta" discriminator
	SequenceNumber int64  `json:"sequence_number"` // contiguous sequence counter
	ItemID         string `json:"item_id"`         // owning reasoning item identifier
	OutputIndex    int    `json:"output_index"`    // owning reasoning output_index
	SummaryIndex   int    `json:"summary_index"`   // open summary segment index
	Delta          string `json:"delta"`           // incremental reasoning fragment
}

// wireSummaryTextDoneEventWrite serializes outgoing response.reasoning_summary_text.done SSE JSON completion frames.
type wireSummaryTextDoneEventWrite struct {
	Type           string `json:"type"`            // literal "response.reasoning_summary_text.done" discriminator
	SequenceNumber int64  `json:"sequence_number"` // contiguous sequence counter
	ItemID         string `json:"item_id"`         // owning reasoning item identifier
	OutputIndex    int    `json:"output_index"`    // owning reasoning output_index
	SummaryIndex   int    `json:"summary_index"`   // open summary segment index
	Text           string `json:"text"`            // full summary segment text
}

// wireToolDeltaEventWrite serializes function_call_arguments.delta and custom_tool_call_input.delta without a name field.
type wireToolDeltaEventWrite struct {
	Type           string `json:"type"`            // *_arguments.delta or *_input.delta
	SequenceNumber int64  `json:"sequence_number"` // contiguous sequence counter
	ItemID         string `json:"item_id"`         // owning call item identifier
	OutputIndex    int    `json:"output_index"`    // owning call output_index
	Delta          string `json:"delta"`           // incremental argument or input fragment
}

// wireFunctionArgsDoneEventWrite serializes outgoing response.function_call_arguments.done SSE JSON completion frames.
type wireFunctionArgsDoneEventWrite struct {
	Type           string `json:"type"`            // literal "response.function_call_arguments.done" discriminator
	SequenceNumber int64  `json:"sequence_number"` // contiguous sequence counter
	ItemID         string `json:"item_id"`         // owning function_call item identifier
	OutputIndex    int    `json:"output_index"`    // owning function_call output_index
	Arguments      string `json:"arguments"`       // complete JSON argument string
}

// wireCustomInputDoneEventWrite serializes outgoing response.custom_tool_call_input.done SSE JSON completion frames.
type wireCustomInputDoneEventWrite struct {
	Type           string `json:"type"`            // literal "response.custom_tool_call_input.done" discriminator
	SequenceNumber int64  `json:"sequence_number"` // contiguous sequence counter
	ItemID         string `json:"item_id"`         // owning custom_tool_call item identifier
	OutputIndex    int    `json:"output_index"`    // owning custom_tool_call output_index
	Input          string `json:"input"`           // complete custom free-form input
}

// wireOpaqueAddedWrite serializes a minimal in-progress item wrapper for same-origin opaque output items.
type wireOpaqueAddedWrite struct {
	ID     string `json:"id"`     // opaque item identifier
	Type   string `json:"type"`   // opaque item type discriminator
	Status string `json:"status"` // literal "in_progress" status
}

// StreamEncoder encodes canonical sequential ir.Event values into OpenAI Responses SSE frames.
type StreamEncoder struct {
	view             ir.ClientView      // client model, echo request, and runtime callbacks
	echo             responseEchoFields // precomputed request echo metadata
	started          bool               // whether response.created and in_progress emitted
	terminated       bool               // whether terminal lifecycle frame emitted
	seqNum           int64              // next contiguous sequence_number
	id               string             // locked response identifier
	model            string             // locked client-visible model identifier
	createdAt        int64              // locked Unix creation timestamp
	serviceTier      string             // latest non-empty service_tier
	usage            *ir.Usage          // latest cumulative Usage snapshot
	nextOutputIdx    int                // next contiguous output_index
	completedOutputs []jsontext.Value   // completed output items for response.completed
	openMsgActive    bool               // whether an assistant message item is currently open
	openMsgOutputIdx int                // output_index of open assistant message
	openMsgID        string             // item.id of open assistant message
	openMsgExplicit  bool               // whether openMsgID came from an explicit Part.ID
	openMsgContent   []jsontext.Value   // closed content parts inside open message
	nextContentIdx   int                // next contiguous content_index inside open message
	openContentIdx   int                // currently open content_index, or -1
	openPartKind     ir.PartKind        // currently open canonical PartKind, or ""
	openSkipped      bool               // true when current open part was dropped at start
	activeOutputIdx  int                // output_index of active non-message item
	activeItemID     string             // item.id of active non-message item
	activeCallID     string             // call_id of active tool_call item
	activeToolKind   ir.ToolKind        // ToolKind of active tool_call item
	activeToolNS     string             // namespace of active tool_call item
	activeToolName   string             // resolved name of active tool_call item
	activeOpaqueRaw  jsontext.Value     // raw JSON of active same-origin opaque item
	summaryPartOpen  bool               // whether reasoning summary_index 0 is open
	emittedBuf       strings.Builder    // text or arguments already emitted via deltas
}

// NewStreamEncoder allocates a stateful OpenAI Responses StreamEncoder seeded
// with client model naming, echoed request fields, and ID callbacks from view.
// Each encoder tracks sequence numbers and open output indices for a single stream.
func NewStreamEncoder(view ir.ClientView) *StreamEncoder {
	return &StreamEncoder{
		view:           view,
		echo:           buildResponseEchoFields(view.Request),
		openContentIdx: -1,
	}
}

// NewStreamEncoder returns a fresh StreamEncoder bound to view so Codec
// satisfies the streaming encoder factory contract. Callers must construct one
// encoder per downstream client SSE response stream.
func (Codec) NewStreamEncoder(view ir.ClientView) *StreamEncoder {
	return NewStreamEncoder(view)
}

// Encode converts one canonical ir.Event into zero or more OpenAI Responses
// sse.Frame values alongside any incurred ir.Loss entries. Events supplied
// after a terminal EventResponseEnd or EventError return nil slices so callers
// can safely drain buffered events after stream termination.
//
// Initial response.created and response.in_progress envelopes emit lazily on
// the first non-error event with Frame.Semantic set to false, whereas visible
// text, reasoning, and tool-call openings mark Frame.Semantic true and terminal
// completion or failure envelopes mark Frame.Terminal true.
func (e *StreamEncoder) Encode(ev ir.Event) ([]sse.Frame, []ir.Loss, error) {
	if e.terminated {
		return nil, nil, nil
	}

	var frames []sse.Frame
	var losses []ir.Loss

	switch ev.Kind {
	case ir.EventResponseStart:
		if err := e.ensureStarted(ev.Response, &frames); err != nil {
			return nil, nil, err
		}

	case ir.EventPartStart:
		if err := e.ensureStarted(nil, &frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartStart(ev, &frames); err != nil {
			return nil, nil, err
		}

	case ir.EventPartDelta:
		if err := e.ensureStarted(nil, &frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartDelta(ev, &frames); err != nil {
			return nil, nil, err
		}

	case ir.EventPartEnd:
		if err := e.ensureStarted(nil, &frames); err != nil {
			return nil, nil, err
		}
		if err := e.handlePartEnd(ev, &frames, &losses); err != nil {
			return nil, nil, err
		}

	case ir.EventUsage:
		if ev.Usage != nil {
			e.usage = ev.Usage.Clone()
		}

	case ir.EventResponseEnd:
		if ev.ServiceTier != "" {
			e.serviceTier = ev.ServiceTier
		}
		if err := e.ensureStarted(nil, &frames); err != nil {
			return nil, nil, err
		}
		if err := e.handleResponseEnd(ev, &frames); err != nil {
			return nil, nil, err
		}

	case ir.EventError:
		if err := e.handleStreamError(ev.Err, &frames); err != nil {
			return nil, nil, err
		}
	}

	return frames, losses, nil
}

// nextSeq returns the next contiguous sequence_number and advances e.seqNum.
func (e *StreamEncoder) nextSeq() int64 {
	seq := e.seqNum
	e.seqNum++
	return seq
}

// appendEvent marshals payload and appends an sse.Frame with matching Event header and flags.
func (e *StreamEncoder) appendEvent(frames *[]sse.Frame, evType string, payload any, semantic, terminal bool) error {
	data, err := marshalDeterministic(payload)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", evType, err)
	}
	*frames = append(*frames, sse.Frame{
		Event:    evType,
		Data:     data,
		Semantic: semantic,
		Terminal: terminal,
	})
	return nil
}

// initEnvelopeMeta locks response ID, created_at, presented model, and initial
// service_tier once from meta and view.Runtime defaults. Repeated calls after id
// has been assigned return immediately without overwriting established fields.
func (e *StreamEncoder) initEnvelopeMeta(meta *ir.ResponseMeta) {
	if e.id != "" {
		return
	}
	var rawID, rawModel string
	var rawCreated int64
	if meta != nil {
		rawID = meta.ID
		rawModel = meta.Model
		rawCreated = meta.CreatedAt
		if meta.ServiceTier != "" {
			e.serviceTier = meta.ServiceTier
		}
	}
	if rawID == "" {
		rawID = e.view.Runtime.NextID("resp_", "resp_0")
	}
	if rawCreated <= 0 {
		rawCreated = e.view.Runtime.NowUnix()
	}
	e.id = rawID
	e.createdAt = rawCreated
	e.model = resolveClientModel(e.view, rawModel)
}

// buildEnvelope constructs a wireResponseWrite snapshot with status, output, and optional details/error/usage.
func (e *StreamEncoder) buildEnvelope(
	status string,
	output []jsontext.Value,
	incDetails *wireIncompleteDetails,
	errBody *wireErrorBodyWrite,
	usage *wireUsageWrite,
) wireResponseWrite {
	if output == nil {
		output = []jsontext.Value{}
	}
	return wireResponseWrite{
		ID:                 e.id,
		Object:             responsesObjectTag,
		CreatedAt:          e.createdAt,
		Status:             status,
		IncompleteDetails:  incDetails,
		Error:              errBody,
		ServiceTier:        e.serviceTier,
		Model:              e.model,
		Instructions:       e.echo.instructions,
		MaxOutputTokens:    e.echo.maxOutputTokens,
		ParallelToolCalls:  e.echo.parallelToolCalls,
		PreviousResponseID: e.echo.previousResponseID,
		Store:              e.echo.store,
		Temperature:        e.echo.temperature,
		TopP:               e.echo.topP,
		Reasoning:          e.echo.reasoning,
		Text:               e.echo.text,
		Tools:              e.echo.tools,
		ToolChoice:         e.echo.toolChoice,
		Output:             output,
		Usage:              usage,
	}
}

// ensureStarted initializes response envelope metadata from meta and emits
// response.created followed by response.in_progress once at stream start.
// Subsequent calls preserve the established envelope and adopt a non-empty
// meta.ServiceTier only when serviceTier is still unset.
func (e *StreamEncoder) ensureStarted(meta *ir.ResponseMeta, frames *[]sse.Frame) error {
	if e.started {
		if meta != nil && meta.ServiceTier != "" && e.serviceTier == "" {
			e.serviceTier = meta.ServiceTier
		}
		return nil
	}
	e.initEnvelopeMeta(meta)
	e.started = true

	createdEnv := e.buildEnvelope("in_progress", []jsontext.Value{}, nil, nil, nil)
	if err := e.appendEvent(frames, "response.created", wireLifecycleEventWrite{
		Type:           "response.created",
		SequenceNumber: e.nextSeq(),
		Response:       createdEnv,
	}, false, false); err != nil {
		return err
	}
	inProgEnv := e.buildEnvelope("in_progress", []jsontext.Value{}, nil, nil, nil)
	return e.appendEvent(frames, "response.in_progress", wireLifecycleEventWrite{
		Type:           "response.in_progress",
		SequenceNumber: e.nextSeq(),
		Response:       inProgEnv,
	}, false, false)
}

// closeOpenMessage emits response.output_item.done for an active assistant
// message item, records the completed item in completedOutputs, and resets
// open message tracking fields. Calls when no assistant message item is open
// return nil without emitting frames.
func (e *StreamEncoder) closeOpenMessage(frames *[]sse.Frame) error {
	if !e.openMsgActive {
		return nil
	}
	content := e.openMsgContent
	if content == nil {
		content = []jsontext.Value{}
	}
	rawMsg, err := marshalDeterministic(wireResponseMessageWrite{
		ID:      e.openMsgID,
		Type:    "message",
		Status:  "completed",
		Role:    "assistant",
		Content: content,
	})
	if err != nil {
		return err
	}
	itemVal := jsontext.Value(rawMsg)
	if err := e.appendEvent(frames, "response.output_item.done", wireOutputItemEventWrite{
		Type:           "response.output_item.done",
		SequenceNumber: e.nextSeq(),
		OutputIndex:    e.openMsgOutputIdx,
		Item:           itemVal,
	}, false, false); err != nil {
		return err
	}
	e.completedOutputs = append(e.completedOutputs, itemVal)
	e.openMsgActive = false
	e.openMsgID = ""
	e.openMsgExplicit = false
	e.openMsgContent = nil
	e.nextContentIdx = 0
	e.openContentIdx = -1
	return nil
}

// handlePartStart opens the Responses SSE output_item or content_part lifecycle
// for ev.Part, reusing an open assistant message across adjacent text and
// refusal parts unless explicit Part.IDs differ. Reasoning, callable tool, and
// same-origin opaque parts first close any open message item, whereas hosted
// tools and foreign opaque parts mark openSkipped without emitting frames.
func (e *StreamEncoder) handlePartStart(ev ir.Event, frames *[]sse.Frame) error {
	if ev.Part == nil {
		return nil
	}
	p := *ev.Part
	e.emittedBuf.Reset()
	e.openPartKind = p.Kind
	e.openSkipped = false
	e.summaryPartOpen = false

	switch p.Kind {
	case ir.PartText, ir.PartRefusal:
		if e.openMsgActive && p.ID != "" && e.openMsgExplicit && p.ID != e.openMsgID {
			if err := e.closeOpenMessage(frames); err != nil {
				return err
			}
		}
		if !e.openMsgActive {
			outIdx := e.nextOutputIdx
			e.nextOutputIdx++
			msgID := p.ID
			explicit := true
			if msgID == "" {
				msgID = e.view.Runtime.NextID("msg_", fmt.Sprintf("msg_%d", outIdx))
				explicit = false
			}
			rawAdded, err := marshalDeterministic(wireResponseMessageWrite{
				ID:      msgID,
				Type:    "message",
				Status:  "in_progress",
				Role:    "assistant",
				Content: []jsontext.Value{},
			})
			if err != nil {
				return err
			}
			if err := e.appendEvent(frames, "response.output_item.added", wireOutputItemEventWrite{
				Type:           "response.output_item.added",
				SequenceNumber: e.nextSeq(),
				OutputIndex:    outIdx,
				Item:           jsontext.Value(rawAdded),
			}, false, false); err != nil {
				return err
			}
			e.openMsgActive = true
			e.openMsgOutputIdx = outIdx
			e.openMsgID = msgID
			e.openMsgExplicit = explicit
			e.nextContentIdx = 0
		}

		cIdx := e.nextContentIdx
		e.openContentIdx = cIdx
		var skelRaw []byte
		var err error
		if p.Kind == ir.PartRefusal {
			skelRaw, err = marshalDeterministic(wireRefusalPartWrite{
				Type:    "refusal",
				Refusal: "",
			})
		} else {
			skelRaw, err = marshalDeterministic(wireOutputTextPartWrite{
				Type:        "output_text",
				Text:        "",
				Annotations: []wireAnnotation{},
			})
		}
		if err != nil {
			return err
		}
		if err := e.appendEvent(frames, "response.content_part.added", wireContentPartEventWrite{
			Type:           "response.content_part.added",
			SequenceNumber: e.nextSeq(),
			ItemID:         e.openMsgID,
			OutputIndex:    e.openMsgOutputIdx,
			ContentIndex:   cIdx,
			Part:           jsontext.Value(skelRaw),
		}, false, false); err != nil {
			return err
		}
		if p.Text != "" {
			return e.emitContentDelta(p.Text, frames)
		}
		return nil

	case ir.PartReasoning:
		if err := e.closeOpenMessage(frames); err != nil {
			return err
		}
		outIdx := e.nextOutputIdx
		e.nextOutputIdx++
		itemID := p.ID
		if itemID == "" {
			itemID = e.view.Runtime.NextID("rs_", fmt.Sprintf("rs_%d", outIdx))
		}
		e.activeOutputIdx = outIdx
		e.activeItemID = itemID

		rawAdded, err := marshalDeterministic(wireReasoningItem{
			ID:      itemID,
			Type:    "reasoning",
			Summary: []wireSummaryText{},
		})
		if err != nil {
			return err
		}
		return e.appendEvent(frames, "response.output_item.added", wireOutputItemEventWrite{
			Type:           "response.output_item.added",
			SequenceNumber: e.nextSeq(),
			OutputIndex:    outIdx,
			Item:           jsontext.Value(rawAdded),
		}, false, false)

	case ir.PartToolCall:
		if err := e.closeOpenMessage(frames); err != nil {
			return err
		}
		if p.Call == nil || p.Call.Tool.Kind == ir.ToolHosted {
			e.openSkipped = true
			return nil
		}
		outIdx := e.nextOutputIdx
		e.nextOutputIdx++
		kind := p.Call.Tool.Kind
		itemID := p.ID
		if itemID == "" {
			prefix := callItemIDPrefix(kind)
			itemID = e.view.Runtime.NextID(prefix, fmt.Sprintf("%s%d", prefix, outIdx))
		}
		callID := p.Call.CallID
		if callID == "" {
			callID = e.view.Runtime.NextID("call_", fmt.Sprintf("call_%d", outIdx))
		}
		e.activeOutputIdx = outIdx
		e.activeItemID = itemID
		e.activeCallID = callID
		e.activeToolKind = kind
		e.activeToolNS = p.Call.Tool.Namespace
		callName, _ := resolveResponseToolCallName(p.Call.Tool.Name)
		e.activeToolName = callName

		var rawAdded []byte
		var err error
		switch kind {
		case ir.ToolCustom:
			rawAdded, err = marshalDeterministic(wireResponseCustomCallWrite{
				ID:        itemID,
				Type:      "custom_tool_call",
				Status:    "in_progress",
				CallID:    callID,
				Namespace: e.activeToolNS,
				Name:      callName,
				Input:     "",
			})
		case ir.ToolClientSearch:
			rawAdded, err = marshalDeterministic(wireResponseToolSearchCallWrite{
				ID:        itemID,
				Type:      "tool_search_call",
				Status:    "in_progress",
				CallID:    callID,
				Execution: "client",
				Arguments: jsontext.Value("{}"),
			})
		default:
			rawAdded, err = marshalDeterministic(wireResponseFunctionCallWrite{
				ID:        itemID,
				Type:      "function_call",
				Status:    "in_progress",
				CallID:    callID,
				Namespace: e.activeToolNS,
				Name:      callName,
				Arguments: "",
			})
		}
		if err != nil {
			return err
		}
		return e.appendEvent(frames, "response.output_item.added", wireOutputItemEventWrite{
			Type:           "response.output_item.added",
			SequenceNumber: e.nextSeq(),
			OutputIndex:    outIdx,
			Item:           jsontext.Value(rawAdded),
		}, true, false)

	case ir.PartOpaque:
		if err := e.closeOpenMessage(frames); err != nil {
			return err
		}
		if p.Opaque == nil || p.Opaque.Origin != protocol.ProtocolOpenAIResponses {
			e.openSkipped = true
			return nil
		}
		outIdx := e.nextOutputIdx
		e.nextOutputIdx++
		itemID := p.ID
		if itemID == "" {
			itemID = e.view.Runtime.NextID("item_", fmt.Sprintf("item_%d", outIdx))
		}
		e.activeOutputIdx = outIdx
		e.activeItemID = itemID
		e.activeOpaqueRaw = jsontext.Value(bytes.Clone(p.Opaque.Raw))

		rawAdded, err := marshalDeterministic(wireOpaqueAddedWrite{
			ID:     itemID,
			Type:   p.Opaque.Type,
			Status: "in_progress",
		})
		if err != nil {
			return err
		}
		return e.appendEvent(frames, "response.output_item.added", wireOutputItemEventWrite{
			Type:           "response.output_item.added",
			SequenceNumber: e.nextSeq(),
			OutputIndex:    outIdx,
			Item:           jsontext.Value(rawAdded),
		}, false, false)

	default:
		if err := e.closeOpenMessage(frames); err != nil {
			return err
		}
		e.openSkipped = true
		return nil
	}
}

// handlePartDelta emits incremental output_text, refusal, reasoning summary, or
// tool argument deltas for the active part. Nil deltas, empty fragments, skipped
// parts, and client tool_search calls (which deliver structured JSON arguments
// on output_item.done) return nil without emitting frames.
func (e *StreamEncoder) handlePartDelta(ev ir.Event, frames *[]sse.Frame) error {
	if ev.Delta == nil || e.openSkipped {
		return nil
	}
	switch e.openPartKind {
	case ir.PartText, ir.PartRefusal:
		if ev.Delta.Text == "" {
			return nil
		}
		return e.emitContentDelta(ev.Delta.Text, frames)

	case ir.PartReasoning:
		if ev.Delta.Text == "" {
			return nil
		}
		return e.emitReasoningDelta(ev.Delta.Text, frames)

	case ir.PartToolCall:
		if ev.Delta.Arguments == "" || e.activeToolKind == ir.ToolClientSearch {
			return nil
		}
		return e.emitToolDelta(ev.Delta.Arguments, frames)

	default:
		return nil
	}
}

// emitContentDelta emits an output_text.delta or refusal.delta frame and updates emittedBuf.
func (e *StreamEncoder) emitContentDelta(text string, frames *[]sse.Frame) error {
	evType := "response.output_text.delta"
	if e.openPartKind == ir.PartRefusal {
		evType = "response.refusal.delta"
	}
	_, _ = e.emittedBuf.WriteString(text)
	return e.appendEvent(frames, evType, wireContentDeltaEventWrite{
		Type:           evType,
		SequenceNumber: e.nextSeq(),
		ItemID:         e.openMsgID,
		OutputIndex:    e.openMsgOutputIdx,
		ContentIndex:   e.openContentIdx,
		Delta:          text,
	}, true, false)
}

// emitReasoningDelta lazily opens summary_index 0 via
// response.reasoning_summary_part.added on the first non-empty fragment and
// appends a response.reasoning_summary_text.delta frame. Summary-less reasoning
// items that never invoke emitReasoningDelta omit summary_part wrappers.
func (e *StreamEncoder) emitReasoningDelta(text string, frames *[]sse.Frame) error {
	if !e.summaryPartOpen {
		if err := e.appendEvent(frames, "response.reasoning_summary_part.added", wireSummaryPartEventWrite{
			Type:           "response.reasoning_summary_part.added",
			SequenceNumber: e.nextSeq(),
			ItemID:         e.activeItemID,
			OutputIndex:    e.activeOutputIdx,
			SummaryIndex:   0,
			Part:           wireSummaryText{Type: "summary_text", Text: ""},
		}, false, false); err != nil {
			return err
		}
		e.summaryPartOpen = true
	}
	_, _ = e.emittedBuf.WriteString(text)
	return e.appendEvent(frames, "response.reasoning_summary_text.delta", wireSummaryTextDeltaEventWrite{
		Type:           "response.reasoning_summary_text.delta",
		SequenceNumber: e.nextSeq(),
		ItemID:         e.activeItemID,
		OutputIndex:    e.activeOutputIdx,
		SummaryIndex:   0,
		Delta:          text,
	}, true, false)
}

// emitToolDelta emits function_call_arguments.delta or custom_tool_call_input.delta without a name field.
func (e *StreamEncoder) emitToolDelta(chunk string, frames *[]sse.Frame) error {
	evType := "response.function_call_arguments.delta"
	if e.activeToolKind == ir.ToolCustom {
		evType = "response.custom_tool_call_input.delta"
	}
	_, _ = e.emittedBuf.WriteString(chunk)
	return e.appendEvent(frames, evType, wireToolDeltaEventWrite{
		Type:           evType,
		SequenceNumber: e.nextSeq(),
		ItemID:         e.activeItemID,
		OutputIndex:    e.activeOutputIdx,
		Delta:          chunk,
	}, true, false)
}

// handlePartEnd flushes any unstreamed suffix guarded by strings.HasPrefix,
// closes the open content_part or output_item lifecycle, and records matching
// ir.Loss entries for dropped metadata or synthesized identifiers. Nil Part
// pointers return nil immediately without emitting SSE frames.
//
// Completed non-custom function calls with blank arguments normalize to "{}" so
// streamed and buffered next-turn replays agree under Law S6. Comparing the
// authoritative EventPartEnd ID and CallID against the values announced at
// EventPartStart ensures synthesized identifiers always record LossIDRewritten.
func (e *StreamEncoder) handlePartEnd(ev ir.Event, frames *[]sse.Frame, losses *[]ir.Loss) error {
	if ev.Part == nil {
		return nil
	}
	p := *ev.Part
	pPath := fmt.Sprintf("parts[%d]", ev.Index)

	switch p.Kind {
	case ir.PartText, ir.PartRefusal:
		emitted := e.emittedBuf.String()
		if strings.HasPrefix(p.Text, emitted) && len(p.Text) > len(emitted) {
			if err := e.emitContentDelta(p.Text[len(emitted):], frames); err != nil {
				return err
			}
		}
		if p.ID != e.openMsgID {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		completedBlock, err := marshalResponseContentPartWrite(p, pPath, losses)
		if err != nil {
			return err
		}

		if p.Kind == ir.PartRefusal {
			if err := e.appendEvent(frames, "response.refusal.done", wireRefusalDoneEventWrite{
				Type:           "response.refusal.done",
				SequenceNumber: e.nextSeq(),
				ItemID:         e.openMsgID,
				OutputIndex:    e.openMsgOutputIdx,
				ContentIndex:   e.openContentIdx,
				Refusal:        p.Text,
			}, false, false); err != nil {
				return err
			}
		} else {
			if err := e.appendEvent(frames, "response.output_text.done", wireTextDoneEventWrite{
				Type:           "response.output_text.done",
				SequenceNumber: e.nextSeq(),
				ItemID:         e.openMsgID,
				OutputIndex:    e.openMsgOutputIdx,
				ContentIndex:   e.openContentIdx,
				Text:           p.Text,
			}, false, false); err != nil {
				return err
			}
		}
		if err := e.appendEvent(frames, "response.content_part.done", wireContentPartEventWrite{
			Type:           "response.content_part.done",
			SequenceNumber: e.nextSeq(),
			ItemID:         e.openMsgID,
			OutputIndex:    e.openMsgOutputIdx,
			ContentIndex:   e.openContentIdx,
			Part:           completedBlock,
		}, false, false); err != nil {
			return err
		}
		e.openMsgContent = append(e.openMsgContent, completedBlock)
		e.openContentIdx = -1
		e.nextContentIdx++
		e.openPartKind = ""
		return nil

	case ir.PartReasoning:
		var finalText string
		if p.Reasoning != nil {
			finalText = strings.Join(p.Reasoning.Segments, "")
		}
		emitted := e.emittedBuf.String()
		if strings.HasPrefix(finalText, emitted) && len(finalText) > len(emitted) {
			if err := e.emitReasoningDelta(finalText[len(emitted):], frames); err != nil {
				return err
			}
		}
		if e.summaryPartOpen {
			doneSummaryText := e.emittedBuf.String()
			if err := e.appendEvent(frames, "response.reasoning_summary_text.done", wireSummaryTextDoneEventWrite{
				Type:           "response.reasoning_summary_text.done",
				SequenceNumber: e.nextSeq(),
				ItemID:         e.activeItemID,
				OutputIndex:    e.activeOutputIdx,
				SummaryIndex:   0,
				Text:           doneSummaryText,
			}, false, false); err != nil {
				return err
			}
			if err := e.appendEvent(frames, "response.reasoning_summary_part.done", wireSummaryPartEventWrite{
				Type:           "response.reasoning_summary_part.done",
				SequenceNumber: e.nextSeq(),
				ItemID:         e.activeItemID,
				OutputIndex:    e.activeOutputIdx,
				SummaryIndex:   0,
				Part:           wireSummaryText{Type: "summary_text", Text: doneSummaryText},
			}, false, false); err != nil {
				return err
			}
			e.summaryPartOpen = false
		}
		if p.ID != e.activeItemID {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		completedRaw, err := buildCompletedReasoningOutputJSON(e.activeItemID, p, pPath, losses)
		if err != nil {
			return err
		}
		hasToken := p.Reasoning != nil && p.Reasoning.Token != nil && p.Reasoning.Token.Data != ""
		if err := e.appendEvent(frames, "response.output_item.done", wireOutputItemEventWrite{
			Type:           "response.output_item.done",
			SequenceNumber: e.nextSeq(),
			OutputIndex:    e.activeOutputIdx,
			Item:           completedRaw,
		}, hasToken && e.emittedBuf.Len() == 0, false); err != nil {
			return err
		}
		e.completedOutputs = append(e.completedOutputs, completedRaw)
		e.openPartKind = ""
		return nil

	case ir.PartToolCall:
		if p.Call == nil {
			e.openPartKind = ""
			e.openSkipped = false
			return nil
		}
		if e.openSkipped || p.Call.Tool.Kind == ir.ToolHosted {
			if p.Cache != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
			}
			if p.Call.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath})
			}
			_ = restoreOrDropExtra(p.Extra, pPath, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
			e.openPartKind = ""
			e.openSkipped = false
			return nil
		}

		if p.ID != e.activeItemID {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".id"})
		}
		if p.Call.CallID != e.activeCallID {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: pPath + ".call_id"})
		}

		emitted := e.emittedBuf.String()
		switch e.activeToolKind {
		case ir.ToolCustom:
			finalInput := p.Call.Arguments
			if strings.HasPrefix(finalInput, emitted) && len(finalInput) > len(emitted) {
				if err := e.emitToolDelta(finalInput[len(emitted):], frames); err != nil {
					return err
				}
			}
			if err := e.appendEvent(frames, "response.custom_tool_call_input.done", wireCustomInputDoneEventWrite{
				Type:           "response.custom_tool_call_input.done",
				SequenceNumber: e.nextSeq(),
				ItemID:         e.activeItemID,
				OutputIndex:    e.activeOutputIdx,
				Input:          finalInput,
			}, false, false); err != nil {
				return err
			}

		case ir.ToolClientSearch:
			// Client search calls deliver structured JSON object arguments on output_item.done.

		default:
			finalArgs := p.Call.Arguments
			if strings.TrimSpace(finalArgs) == "" && !p.Call.Incomplete {
				finalArgs = "{}"
			}
			if strings.HasPrefix(finalArgs, emitted) && len(finalArgs) > len(emitted) {
				if err := e.emitToolDelta(finalArgs[len(emitted):], frames); err != nil {
					return err
				}
			}
			if err := e.appendEvent(frames, "response.function_call_arguments.done", wireFunctionArgsDoneEventWrite{
				Type:           "response.function_call_arguments.done",
				SequenceNumber: e.nextSeq(),
				ItemID:         e.activeItemID,
				OutputIndex:    e.activeOutputIdx,
				Arguments:      finalArgs,
			}, false, false); err != nil {
				return err
			}
		}

		completedRaw, err := buildCompletedToolCallOutputJSON(e.activeItemID, e.activeCallID, p, pPath, losses)
		if err != nil {
			return err
		}
		if err := e.appendEvent(frames, "response.output_item.done", wireOutputItemEventWrite{
			Type:           "response.output_item.done",
			SequenceNumber: e.nextSeq(),
			OutputIndex:    e.activeOutputIdx,
			Item:           completedRaw,
		}, false, false); err != nil {
			return err
		}
		e.completedOutputs = append(e.completedOutputs, completedRaw)
		e.openPartKind = ""
		return nil

	case ir.PartOpaque:
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		if e.openSkipped || p.Opaque == nil || p.Opaque.Origin != protocol.ProtocolOpenAIResponses {
			detail := ""
			if p.Opaque != nil {
				detail = p.Opaque.Type
			}
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath, Detail: detail})
			e.openPartKind = ""
			e.openSkipped = false
			return nil
		}
		completedRaw := jsontext.Value(bytes.Clone(p.Opaque.Raw))
		if err := e.appendEvent(frames, "response.output_item.done", wireOutputItemEventWrite{
			Type:           "response.output_item.done",
			SequenceNumber: e.nextSeq(),
			OutputIndex:    e.activeOutputIdx,
			Item:           completedRaw,
		}, false, false); err != nil {
			return err
		}
		e.completedOutputs = append(e.completedOutputs, completedRaw)
		e.openPartKind = ""
		return nil

	case ir.PartMedia:
		if p.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: pPath})
		}
		_ = restoreOrDropExtra(p.Extra, pPath, losses)
		*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: pPath, Detail: "response"})
		e.openPartKind = ""
		e.openSkipped = false
		return nil

	default:
		e.openPartKind = ""
		e.openSkipped = false
		return nil
	}
}

// handleResponseEnd closes any open assistant message item before emitting the
// terminal response.completed, response.incomplete, or response.failed frame
// with Terminal=true. Marking terminated prevents any subsequent Encode calls
// from emitting trailing SSE frames.
func (e *StreamEncoder) handleResponseEnd(ev ir.Event, frames *[]sse.Frame) error {
	if err := e.closeOpenMessage(frames); err != nil {
		return err
	}
	var finish ir.Finish
	if ev.Finish != nil {
		finish = *ev.Finish
	}
	status, incDetails, termEvent := encodeResponsesFinish(finish)
	var errBody *wireErrorBodyWrite
	if status == "failed" {
		errBody = encodeErrorBody(nil)
	}
	env := e.buildEnvelope(status, e.completedOutputs, incDetails, errBody, encodeResponsesUsage(e.usage))
	e.terminated = true
	return e.appendEvent(frames, termEvent, wireLifecycleEventWrite{
		Type:           termEvent,
		SequenceNumber: e.nextSeq(),
		Response:       env,
	}, false, true)
}

// handleStreamError initializes fallback envelope metadata when no event has
// started, closes all open summary, content, and output_item lifecycles, and
// emits a terminal response.failed frame with Terminal=true. Marking terminated
// causes any subsequent Encode calls to return nil slices immediately.
func (e *StreamEncoder) handleStreamError(se *ir.StreamError, frames *[]sse.Frame) error {
	e.initEnvelopeMeta(nil)
	if err := e.closeOpenLifecyclesForAbort(frames); err != nil {
		return err
	}
	errBody := encodeErrorBody(se)
	failedEnv := e.buildEnvelope("failed", e.completedOutputs, nil, errBody, encodeResponsesUsage(e.usage))
	e.terminated = true
	return e.appendEvent(frames, "response.failed", wireLifecycleEventWrite{
		Type:           "response.failed",
		SequenceNumber: e.nextSeq(),
		Response:       failedEnv,
	}, false, true)
}

// closeOpenLifecyclesForAbort cleanly terminates any open summary_part,
// content_part, or output_item before response.failed is emitted. Downstream
// Responses SSE parsers therefore never encounter dangling open indices on mid-stream aborts.
func (e *StreamEncoder) closeOpenLifecyclesForAbort(frames *[]sse.Frame) error {
	if e.openPartKind != "" && !e.openSkipped {
		switch e.openPartKind {
		case ir.PartText, ir.PartRefusal:
			if e.openContentIdx >= 0 {
				txt := e.emittedBuf.String()
				var doneRaw []byte
				var err error
				if e.openPartKind == ir.PartRefusal {
					if err := e.appendEvent(frames, "response.refusal.done", wireRefusalDoneEventWrite{
						Type:           "response.refusal.done",
						SequenceNumber: e.nextSeq(),
						ItemID:         e.openMsgID,
						OutputIndex:    e.openMsgOutputIdx,
						ContentIndex:   e.openContentIdx,
						Refusal:        txt,
					}, false, false); err != nil {
						return err
					}
					doneRaw, err = marshalDeterministic(wireRefusalPartWrite{Type: "refusal", Refusal: txt})
				} else {
					if err := e.appendEvent(frames, "response.output_text.done", wireTextDoneEventWrite{
						Type:           "response.output_text.done",
						SequenceNumber: e.nextSeq(),
						ItemID:         e.openMsgID,
						OutputIndex:    e.openMsgOutputIdx,
						ContentIndex:   e.openContentIdx,
						Text:           txt,
					}, false, false); err != nil {
						return err
					}
					doneRaw, err = marshalDeterministic(wireOutputTextPartWrite{Type: "output_text", Text: txt, Annotations: []wireAnnotation{}})
				}
				if err != nil {
					return err
				}
				doneBlock := jsontext.Value(doneRaw)
				if err := e.appendEvent(frames, "response.content_part.done", wireContentPartEventWrite{
					Type:           "response.content_part.done",
					SequenceNumber: e.nextSeq(),
					ItemID:         e.openMsgID,
					OutputIndex:    e.openMsgOutputIdx,
					ContentIndex:   e.openContentIdx,
					Part:           doneBlock,
				}, false, false); err != nil {
					return err
				}
				e.openMsgContent = append(e.openMsgContent, doneBlock)
				e.openContentIdx = -1
				e.nextContentIdx++
			}

		case ir.PartReasoning:
			txt := e.emittedBuf.String()
			if e.summaryPartOpen {
				if err := e.appendEvent(frames, "response.reasoning_summary_text.done", wireSummaryTextDoneEventWrite{
					Type:           "response.reasoning_summary_text.done",
					SequenceNumber: e.nextSeq(),
					ItemID:         e.activeItemID,
					OutputIndex:    e.activeOutputIdx,
					SummaryIndex:   0,
					Text:           txt,
				}, false, false); err != nil {
					return err
				}
				if err := e.appendEvent(frames, "response.reasoning_summary_part.done", wireSummaryPartEventWrite{
					Type:           "response.reasoning_summary_part.done",
					SequenceNumber: e.nextSeq(),
					ItemID:         e.activeItemID,
					OutputIndex:    e.activeOutputIdx,
					SummaryIndex:   0,
					Part:           wireSummaryText{Type: "summary_text", Text: txt},
				}, false, false); err != nil {
					return err
				}
				e.summaryPartOpen = false
			}
			summaries := []wireSummaryText{}
			if txt != "" {
				summaries = []wireSummaryText{{Type: "summary_text", Text: txt}}
			}
			rawDone, err := marshalDeterministic(wireReasoningItem{
				ID:      e.activeItemID,
				Type:    "reasoning",
				Summary: summaries,
			})
			if err != nil {
				return err
			}
			if err := e.appendEvent(frames, "response.output_item.done", wireOutputItemEventWrite{
				Type:           "response.output_item.done",
				SequenceNumber: e.nextSeq(),
				OutputIndex:    e.activeOutputIdx,
				Item:           jsontext.Value(rawDone),
			}, false, false); err != nil {
				return err
			}

		case ir.PartToolCall:
			partial := e.emittedBuf.String()
			var rawDone []byte
			var err error
			switch e.activeToolKind {
			case ir.ToolCustom:
				if err := e.appendEvent(frames, "response.custom_tool_call_input.done", wireCustomInputDoneEventWrite{
					Type:           "response.custom_tool_call_input.done",
					SequenceNumber: e.nextSeq(),
					ItemID:         e.activeItemID,
					OutputIndex:    e.activeOutputIdx,
					Input:          partial,
				}, false, false); err != nil {
					return err
				}
				rawDone, err = marshalDeterministic(wireResponseCustomCallWrite{
					ID:        e.activeItemID,
					Type:      "custom_tool_call",
					Status:    "incomplete",
					CallID:    e.activeCallID,
					Namespace: e.activeToolNS,
					Name:      e.activeToolName,
					Input:     partial,
				})
			case ir.ToolClientSearch:
				rawDone, err = marshalDeterministic(wireResponseToolSearchCallWrite{
					ID:        e.activeItemID,
					Type:      "tool_search_call",
					Status:    "incomplete",
					CallID:    e.activeCallID,
					Execution: "client",
					Arguments: jsontext.Value("{}"),
				})
			default:
				if err := e.appendEvent(frames, "response.function_call_arguments.done", wireFunctionArgsDoneEventWrite{
					Type:           "response.function_call_arguments.done",
					SequenceNumber: e.nextSeq(),
					ItemID:         e.activeItemID,
					OutputIndex:    e.activeOutputIdx,
					Arguments:      partial,
				}, false, false); err != nil {
					return err
				}
				rawDone, err = marshalDeterministic(wireResponseFunctionCallWrite{
					ID:        e.activeItemID,
					Type:      "function_call",
					Status:    "incomplete",
					CallID:    e.activeCallID,
					Namespace: e.activeToolNS,
					Name:      e.activeToolName,
					Arguments: partial,
				})
			}
			if err != nil {
				return err
			}
			if err := e.appendEvent(frames, "response.output_item.done", wireOutputItemEventWrite{
				Type:           "response.output_item.done",
				SequenceNumber: e.nextSeq(),
				OutputIndex:    e.activeOutputIdx,
				Item:           jsontext.Value(rawDone),
			}, false, false); err != nil {
				return err
			}

		case ir.PartOpaque:
			rawDone := e.activeOpaqueRaw
			if len(rawDone) == 0 {
				var err error
				rawDone, err = marshalDeterministic(wireOpaqueAddedWrite{
					ID:     e.activeItemID,
					Type:   "opaque",
					Status: "incomplete",
				})
				if err != nil {
					return err
				}
			}
			if err := e.appendEvent(frames, "response.output_item.done", wireOutputItemEventWrite{
				Type:           "response.output_item.done",
				SequenceNumber: e.nextSeq(),
				OutputIndex:    e.activeOutputIdx,
				Item:           rawDone,
			}, false, false); err != nil {
				return err
			}
		}
		e.openPartKind = ""
	}
	return e.closeOpenMessage(frames)
}
