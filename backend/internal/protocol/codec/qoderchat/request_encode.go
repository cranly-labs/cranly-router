package qoderchat

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/qoder"
)

// request_encode.go encodes canonical IR generation requests into deterministic
// qoder.PayloadRequest JSON bytes while enforcing origin-appropriate Qoder
// continuation and tool-pairing rules alongside original-coordinate ir.Loss logs.
//
// Qoder's COSY inference endpoint maintains conversation continuity through
// gateway-planned session prefixes rather than OpenAI server-side response IDs
// or item_reference pointers. When Dialect.StatelessResponses is set for
// Responses or input-shaped Cursor requests, stateful continuation flags and
// unpaired tool_result turns fail closed with typed *qoder.RequestError values
// while unanswered calls prune cleanly; Chat Completions and Anthropic Messages
// requests instead preserve chronological tool history and fold ID-less
// tool_result text into user turns without inventing identifiers.

const (
	// Default schemas and proxy names support standalone EncodeRequest calls
	// executed outside convert.Exchange's tool-lowering pass.
	defaultEmptyObjectSchemaJSON = `{"type":"object","properties":{}}`
	standaloneCustomSchemaJSON   = `{"type":"object","properties":{"input":{"type":"string","description":"The raw input for this tool, passed through verbatim."}},"required":["input"]}`
	standaloneSearchProxyName    = "tool_search"
	standaloneSearchSchemaJSON   = `{"type":"object","properties":{"query":{"type":"string","description":"Search query for tools or connectors to load."},"limit":{"type":"integer","description":"Maximum number of tool groups to return."}},"required":["query"]}`
	defaultQoderToolNameMaxBytes = 64
)

// trackedReqPart pairs an IR Part with its original source coordinate path.
type trackedReqPart struct {
	part      ir.Part // source IR part value
	path      string  // original IR coordinate path
	origOrder int     // zero-based part index inside its source Message
}

// trackedCallCandidate holds a validated assistant tool call awaiting reply pairing.
type trackedCallCandidate struct {
	callID    string // non-empty trimmed correlation identifier
	name      string // normalized target function name
	arguments string // validated JSON object argument string
	path      string // original IR part coordinate path
	origOrder int    // zero-based part index inside its source Message
}

// trackedResultSlot holds one tool_result item split from an ir.RoleTool turn.
type trackedResultSlot struct {
	partPath string        // original IR path of the PartToolResult
	msgPath  string        // original IR path of the owning RoleTool message
	msgIdx   int           // original Message index
	partIdx  int           // original Part index inside owning RoleTool message
	callID   string        // trimmed correlation identifier
	result   ir.ToolResult // cloned tool result payload
	claimed  bool          // true once paired to a preceding assistant tool call
	dropped  bool          // true when invalidated by a malformed call
}

// encodeStageTurn represents one non-system turn during tool pairing and encoding.
type encodeStageTurn struct {
	role            ir.Role                // RoleUser, RoleAssistant, or RoleTool
	path            string                 // original IR message coordinate path
	id              string                 // original wire message identifier
	extra           ir.Extra               // original message Extra members
	originallyEmpty bool                   // true when source Message.Parts was empty
	parts           []trackedReqPart       // non-tool-result parts on the turn
	candidates      []trackedCallCandidate // validated assistant tool calls
	toolSlot        *trackedResultSlot     // populated when role is RoleTool
}

// partRankEntry records an assistant part's original slice position and target
// serialization rank (0=text/refusal, 1=tool_calls) to detect reordering.
type partRankEntry struct {
	origOrder int // original part index on source assistant Message
	rank      int // target Qoder message field group rank
}

// EncodeRequest serializes req into deterministic qoder.PayloadRequest JSON
// bytes under dialect and returns all incurred original-coordinate ir.Loss
// records without mutating req. Nil requests return an error immediately.
//
// PreviousResponseID, ConversationSet, item_reference opaque items, and
// undeclared allowed_tools subsets always fail closed with *qoder.RequestError.
// When dialect.StatelessResponses is true, Store=true and unpaired tool_result
// turns also return *qoder.RequestError while unanswered calls prune; otherwise
// Store is ignored, chronological tool turns survive, and ID-less tool_result
// text folds into user turns.
func EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	if req == nil {
		return nil, nil, errors.New("qoderchat: nil request")
	}
	if strings.TrimSpace(req.Model) == "" {
		return nil, nil, &qoder.RequestError{
			Code:    qoder.CodeInvalidRequest,
			Message: "model is required",
		}
	}
	if strings.TrimSpace(req.PreviousResponseID) != "" ||
		(dialect.StatelessResponses && req.Store != nil && *req.Store) ||
		req.ConversationSet {
		return nil, nil, &qoder.RequestError{
			Code:    qoder.CodeUnsupportedResponseState,
			Message: "Qoder Responses requires stateless input. Set store to false, omit previous_response_id and conversation, and include the full history.",
		}
	}
	if hasItemReferenceOpaque(req) {
		return nil, nil, &qoder.RequestError{
			Code:    qoder.CodeUnsupportedResponseState,
			Message: "Include the complete input item instead of an item_reference for Qoder Responses.",
		}
	}

	work := req.Clone()
	ir.Link(work)

	var losses []ir.Loss
	if !work.Extra.IsZero() {
		losses = append(losses, ir.Loss{Kind: ir.LossExtraDropped, Path: ""})
	}
	recordDroppedSamplingAndControls(work, &losses)

	maxNameBytes := defaultQoderToolNameMaxBytes
	if dialect.ToolNameMaxBytes > 0 {
		maxNameBytes = dialect.ToolNameMaxBytes
	}

	for i, ns := range work.Namespaces {
		nsPath := fmt.Sprintf("namespaces[%d]", i)
		if ns.Cache != nil {
			losses = append(losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: nsPath + ".cache"})
		}
		if !ns.Extra.IsZero() {
			losses = append(losses, ir.Loss{Kind: ir.LossExtraDropped, Path: nsPath + ".extra"})
		}
	}

	tools, declaredTools := encodeQoderTools(work.Tools, maxNameBytes, &losses)
	toolChoice, err := encodeQoderToolChoice(work.ToolChoice, len(work.Tools) > 0, len(tools) > 0, declaredTools, maxNameBytes, &losses)
	if err != nil {
		return nil, nil, err
	}

	sysText := extractQoderSystem(work, &losses)
	messages, err := encodeQoderMessages(work.Messages, maxNameBytes, dialect.StatelessResponses, &losses)
	if err != nil {
		return nil, nil, err
	}

	maxTokens := qoder.DefaultMaxTokens
	if work.MaxOutputTokens != nil && *work.MaxOutputTokens > 0 {
		// Clamp int64 token budgets above math.MaxInt before narrowing to int so
		// 32-bit builds never wrap large client limits into negative budgets.
		if *work.MaxOutputTokens > int64(math.MaxInt) {
			maxTokens = math.MaxInt
		} else {
			maxTokens = int(*work.MaxOutputTokens)
		}
	}

	var thinking qoder.ThinkingDirective
	if work.Reasoning != nil {
		effort := work.Reasoning.Effort
		if dialect.StripEffort {
			effort = ""
		}
		thinking = qoder.DeriveThinkingDirective(work.Reasoning.Mode, effort, work.Reasoning.BudgetTokens)
	}

	payload := &qoder.PayloadRequest{
		Model:          work.Model,
		System:         sysText,
		Messages:       messages,
		Tools:          tools,
		ToolChoice:     toolChoice,
		MaxTokens:      maxTokens,
		Thinking:       thinking,
		SessionID:      work.SessionID,
		ConversationID: work.ConversationID,
		CacheKey:       work.CacheKey,
		MetadataUserID: work.MetadataUserID,
	}
	encoded, err := payload.MarshalJSONBytes()
	if err != nil {
		return nil, nil, err
	}
	return encoded, losses, nil
}

// EncodeRequest delegates Qoder request serialization to the package-level
// EncodeRequest function so Codec satisfies convert's upstreamCodec interface.
// Nil requests, stateful Responses flags, or undeclared allowed_tools subsets
// return an error without mutating req.
func (Codec) EncodeRequest(req *ir.Request, dialect ir.Dialect) ([]byte, []ir.Loss, error) {
	return EncodeRequest(req, dialect)
}

// DecodePayloadRequest parses serialized qoder.PayloadRequest JSON bytes and
// reconstructs a validated ir.Request for Law Q4 semantic essence verification.
// Empty inputs, duplicate JSON keys, or structurally invalid payloads return an error.
func DecodePayloadRequest(body []byte) (*ir.Request, error) {
	payload, err := qoder.DecodePayload(body)
	if err != nil {
		return nil, err
	}
	return PayloadToIR(payload)
}

// PayloadToIR converts a normalized qoder.PayloadRequest back into a validated
// ir.Request preserving discrete text blocks, cache markers, tool schemas,
// allowed-tool subsets, and correlated tool results. Nil inputs or unsupported
// message roles and tool-choice modes return a descriptive error.
func PayloadToIR(payload *qoder.PayloadRequest) (*ir.Request, error) {
	if payload == nil {
		return nil, errors.New("qoderchat: nil PayloadRequest")
	}
	req := &ir.Request{
		Model:          payload.Model,
		Messages:       make([]ir.Message, 0, len(payload.Messages)),
		SessionID:      payload.SessionID,
		ConversationID: payload.ConversationID,
		CacheKey:       payload.CacheKey,
		MetadataUserID: payload.MetadataUserID,
	}
	if payload.MaxTokens > 0 {
		mt := int64(payload.MaxTokens)
		req.MaxOutputTokens = &mt
	}
	if payload.System != "" {
		req.System = []ir.Part{{Kind: ir.PartText, Text: payload.System}}
	}

	for _, m := range payload.Messages {
		irMsg, err := payloadMessageToIR(m)
		if err != nil {
			return nil, err
		}
		req.Messages = append(req.Messages, irMsg)
	}

	if len(payload.Tools) > 0 {
		req.Tools = make([]ir.Tool, len(payload.Tools))
		for i, t := range payload.Tools {
			var strict *bool
			if t.Function.Strict != nil {
				v := *t.Function.Strict
				strict = &v
			}
			var schema jsontext.Value
			if len(t.Function.Parameters) > 0 {
				schema = jsontext.Value(bytes.Clone(t.Function.Parameters))
			}
			req.Tools[i] = ir.Tool{
				Kind:        ir.ToolFunction,
				Name:        t.Function.Name,
				Description: t.Function.Description,
				InputSchema: schema,
				Strict:      strict,
			}
		}
	}

	if payload.ToolChoice != nil {
		tc, err := payloadToolChoiceToIR(payload.ToolChoice)
		if err != nil {
			return nil, err
		}
		req.ToolChoice = tc
	}

	ir.Link(req)
	if err := ir.Validate(req); err != nil {
		return nil, fmt.Errorf("validate reconstructed qoder IR: %w", err)
	}
	return req, nil
}

// hasItemReferenceOpaque reports whether req.System or req.Messages contains an
// OpaqueItem with Type "item_reference".
func hasItemReferenceOpaque(req *ir.Request) bool {
	for _, p := range req.System {
		if p.Kind == ir.PartOpaque && p.Opaque != nil && p.Opaque.Type == "item_reference" {
			return true
		}
	}
	for _, m := range req.Messages {
		for _, p := range m.Parts {
			if p.Kind == ir.PartOpaque && p.Opaque != nil && p.Opaque.Type == "item_reference" {
				return true
			}
		}
	}
	return false
}

// recordDroppedSamplingAndControls appends ir.Loss records onto losses for
// sampling, candidate count, stop sequence, and structured output parameters
// omitted by Qoder's PayloadRequest. Unset optional controls record no loss entries.
func recordDroppedSamplingAndControls(req *ir.Request, losses *[]ir.Loss) {
	if req.Sampling.Temperature != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.temperature", Detail: "temperature"})
	}
	if req.Sampling.TopP != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.top_p", Detail: "top_p"})
	}
	if req.Sampling.TopK != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.top_k", Detail: "top_k"})
	}
	if req.Sampling.Seed != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.seed", Detail: "seed"})
	}
	if req.Sampling.FrequencyPenalty != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.frequency_penalty", Detail: "frequency_penalty"})
	}
	if req.Sampling.PresencePenalty != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossSamplingDropped, Path: "sampling.presence_penalty", Detail: "presence_penalty"})
	}
	if req.Sampling.CandidateCount != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCandidateCountIgnored, Path: "sampling.candidate_count"})
	}
	if len(req.Stop) > 0 {
		*losses = append(*losses, ir.Loss{Kind: ir.LossStopDropped, Path: "stop"})
	}
	if req.Output != nil && (req.Output.Kind != "" || req.Output.Name != "" ||
		req.Output.Description != "" || len(req.Output.Schema) > 0 || req.Output.Strict != nil) {
		*losses = append(*losses, ir.Loss{Kind: ir.LossOutputFormatApproximated, Path: "output"})
	}
}

// encodeQoderTools converts IR tool declarations into normalized qoder.Tool
// function entries and returns the set of declared function names. Empty tool
// slices return (nil, nil) without allocating output storage.
//
// Provider-hosted tools drop with LossHostedToolDropped, omitted schemas fill
// an empty JSON object with LossSchemaApproximated, and standalone custom,
// deduplicated client_search, or namespaced declarations lower into flat
// functions with LossToolLowered so Qoder's function-only schema contract holds.
func encodeQoderTools(tools []ir.Tool, maxNameBytes int, losses *[]ir.Loss) ([]qoder.Tool, map[string]bool) {
	if len(tools) == 0 {
		return nil, nil
	}
	out := make([]qoder.Tool, 0, len(tools))
	declared := make(map[string]bool, len(tools))
	searchEmitted := false

	for i, t := range tools {
		tPath := fmt.Sprintf("tools[%d]", i)
		if t.Cache != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: tPath + ".cache"})
		}
		if !t.Extra.IsZero() {
			*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: tPath + ".extra"})
		}

		switch t.Kind {
		case ir.ToolHosted:
			detail := t.Name
			if t.Hosted != nil && t.Hosted.Type != "" {
				detail = t.Hosted.Type
			}
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossHostedToolDropped,
				Path:   tPath,
				Detail: detail,
			})

		case ir.ToolFunction:
			name := sanitizeQoderToolName(flattenToolNamespace(t.Namespace, t.Name, maxNameBytes), maxNameBytes)
			lowered := t.DeferLoading != nil || t.Namespace != "" || name != t.Name
			if lowered {
				*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			}
			var schema jsontext.Value
			if len(bytes.TrimSpace(t.InputSchema)) > 0 {
				schema = jsontext.Value(bytes.Clone(t.InputSchema))
			} else {
				schema = jsontext.Value(defaultEmptyObjectSchemaJSON)
				if !lowered {
					*losses = append(*losses, ir.Loss{
						Kind:   ir.LossSchemaApproximated,
						Path:   tPath + ".input_schema",
						Detail: "default_empty_object",
					})
				}
			}
			var strict *bool
			if t.Strict != nil {
				v := *t.Strict
				strict = &v
			}
			declared[name] = true
			out = append(out, qoder.Tool{
				Type: "function",
				Function: qoder.ToolFunction{
					Name:        name,
					Description: t.Description,
					Parameters:  schema,
					Strict:      strict,
				},
			})

		case ir.ToolCustom:
			name := sanitizeQoderToolName(flattenToolNamespace(t.Namespace, t.Name, maxNameBytes), maxNameBytes)
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			declared[name] = true
			out = append(out, qoder.Tool{
				Type: "function",
				Function: qoder.ToolFunction{
					Name:        name,
					Description: t.Description,
					Parameters:  jsontext.Value(standaloneCustomSchemaJSON),
				},
			})

		case ir.ToolClientSearch:
			if searchEmitted {
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossOpaqueItemDropped,
					Path:   tPath,
					Detail: "duplicate_tool_search",
				})
				continue
			}
			searchEmitted = true
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: tPath})
			declared[standaloneSearchProxyName] = true
			out = append(out, qoder.Tool{
				Type: "function",
				Function: qoder.ToolFunction{
					Name:        standaloneSearchProxyName,
					Description: "Search and load tools, plugins, connectors, and MCP namespaces for the current task.",
					Parameters:  jsontext.Value(standaloneSearchSchemaJSON),
				},
			})
		}
	}
	if len(out) == 0 {
		return nil, declared
	}
	return out, declared
}

// encodeQoderToolChoice translates tc into a normalized *qoder.ToolChoice and
// validates any AllowedTools subset against declaredTools. Nil choices or
// unrecognized modes without allowed subsets return (nil, nil).
//
// Undeclared allowed functions or allowed_tools modes outside auto and required
// fail closed with *qoder.RequestError rather than silently widening execution
// permissions to every declared tool. When source tools existed exclusively as
// dropped hosted declarations, non-none choices omit tool_choice and record
// LossToolChoiceApproximated so Qoder never receives a dangling required mode.
func encodeQoderToolChoice(
	tc *ir.ToolChoice,
	hadSourceTools bool,
	hasSurvivingTools bool,
	declaredTools map[string]bool,
	maxNameBytes int,
	losses *[]ir.Loss,
) (*qoder.ToolChoice, error) {
	if tc == nil {
		return nil, nil
	}
	invalidAllowed := &qoder.RequestError{
		Code:    qoder.CodeUnsupportedToolChoice,
		Message: "Select declared Qoder functions and an auto or required allowed_tools mode.",
	}

	if len(tc.AllowedTools) > 0 {
		if tc.Mode != ir.ToolChoiceAuto && tc.Mode != ir.ToolChoiceRequired {
			return nil, invalidAllowed
		}
		allowedNames := make([]string, 0, len(tc.AllowedTools))
		approx := false
		for _, ref := range tc.AllowedTools {
			name, changed, ok := resolveQoderToolChoiceRef(ref, maxNameBytes)
			if !ok || !declaredTools[name] {
				return nil, invalidAllowed
			}
			if changed {
				approx = true
			}
			allowedNames = append(allowedNames, name)
		}
		if approx {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
		}
		return &qoder.ToolChoice{
			Mode:         string(tc.Mode),
			AllowedTools: allowedNames,
		}, nil
	}

	if hadSourceTools && !hasSurvivingTools && tc.Mode != ir.ToolChoiceNone {
		// Omit dangling non-none tool_choice selectors when every hosted declaration was
		// dropped so Qoder never receives a required selector with zero callable tools.
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
		return nil, nil
	}

	switch tc.Mode {
	case ir.ToolChoiceAuto:
		return &qoder.ToolChoice{Mode: qoder.ToolChoiceAuto}, nil
	case ir.ToolChoiceNone:
		return &qoder.ToolChoice{Mode: qoder.ToolChoiceNone}, nil
	case ir.ToolChoiceRequired:
		return &qoder.ToolChoice{Mode: qoder.ToolChoiceRequired}, nil
	case ir.ToolChoiceTool:
		name, changed, ok := resolveQoderToolChoiceRef(tc.Tool, maxNameBytes)
		if !ok {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
			return &qoder.ToolChoice{Mode: qoder.ToolChoiceAuto}, nil
		}
		if changed {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolChoiceApproximated, Path: "tool_choice"})
		}
		return &qoder.ToolChoice{
			Mode:         qoder.ToolChoiceFunction,
			FunctionName: name,
		}, nil
	default:
		return nil, nil
	}
}

// resolveQoderToolChoiceRef maps an ir.ToolRef inside ToolChoice onto its flat
// Qoder function identifier and reports whether lowering modified the reference.
// Hosted tool references or blank function names return ("", false, false) so
// callers can reject invalid allowed_tools entries or approximate single-tool selectors.
func resolveQoderToolChoiceRef(ref ir.ToolRef, maxNameBytes int) (string, bool, bool) {
	switch ref.Kind {
	case ir.ToolFunction:
		flat := flattenToolNamespace(ref.Namespace, ref.Name, maxNameBytes)
		sanitized := sanitizeQoderToolName(flat, maxNameBytes)
		if strings.TrimSpace(sanitized) == "" {
			return "", false, false
		}
		changed := ref.Namespace != "" || sanitized != ref.Name
		return sanitized, changed, true
	case ir.ToolCustom:
		flat := flattenToolNamespace(ref.Namespace, ref.Name, maxNameBytes)
		sanitized := sanitizeQoderToolName(flat, maxNameBytes)
		if strings.TrimSpace(sanitized) == "" {
			return "", false, false
		}
		return sanitized, true, true
	case ir.ToolClientSearch:
		return standaloneSearchProxyName, true, true
	default:
		return "", false, false
	}
}

// extractQoderSystem folds req.System and any RoleSystem or RoleDeveloper turns
// in req.Messages into a single newline-joined system instruction string. Empty
// system inputs return an empty string without emitting LossSystemRelocated.
//
// Because PayloadRequest stores system instructions as one scalar string rather
// than a part slice, extractQoderSystem emits LossSystemRelocated whenever
// in-band control turns, multiple system parts, empty text blocks, or part IDs
// collapse into that scalar representation while recording dropped media and
// metadata on their original coordinates.
func extractQoderSystem(req *ir.Request, losses *[]ir.Loss) string {
	var collected []string
	sawControlTurn := false
	sawEmptySysText := false
	sawSysPartID := false

	for i, p := range req.System {
		pPath := fmt.Sprintf("system[%d]", i)
		if p.ID != "" {
			sawSysPartID = true
		}
		recordPartMetadataLosses(p, pPath, true, losses)
		switch p.Kind {
		case ir.PartText:
			if p.Text != "" {
				collected = append(collected, p.Text)
			} else {
				sawEmptySysText = true
			}
		case ir.PartMedia:
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: pPath})
		case ir.PartOpaque:
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
		}
	}

	for mIdx, msg := range req.Messages {
		if msg.Role != ir.RoleSystem && msg.Role != ir.RoleDeveloper {
			continue
		}
		sawControlTurn = true
		mPath := fmt.Sprintf("messages[%d]", mIdx)
		if msg.ID != "" {
			*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: mPath + ".id"})
		}
		if !msg.Extra.IsZero() {
			*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: mPath + ".extra"})
		}
		for pIdx, p := range msg.Parts {
			pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
			recordPartMetadataLosses(p, pPath, true, losses)
			switch p.Kind {
			case ir.PartText:
				if p.Text != "" {
					collected = append(collected, p.Text)
				}
			case ir.PartMedia:
				*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: pPath})
			case ir.PartOpaque:
				*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: pPath})
			case ir.PartReasoning:
				recordDroppedReasoningPart(p, pPath, losses)
			case ir.PartRefusal:
				*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: pPath})
				if p.Text != "" {
					collected = append(collected, p.Text)
				}
			}
		}
	}

	if sawControlTurn || len(req.System) > 1 || len(collected) > 1 || sawEmptySysText || sawSysPartID {
		*losses = append(*losses, ir.Loss{Kind: ir.LossSystemRelocated, Path: "system"})
	}
	return strings.Join(collected, "\n")
}

// encodeQoderMessages converts non-system conversation turns into ordered
// qoder.Message entries according to statelessResponses. When statelessResponses
// is true, unpaired tool_result turns return *qoder.RequestError and unanswered
// calls prune; when false, chronological Chat and Messages tool turns remain intact.
func encodeQoderMessages(msgs []ir.Message, maxNameBytes int, statelessResponses bool, losses *[]ir.Loss) ([]qoder.Message, error) {
	if !statelessResponses {
		return encodeChronologicalQoderMessages(msgs, maxNameBytes, losses), nil
	}

	var stages []encodeStageTurn
	invalidCallIDs := make(map[string]bool)

	for mIdx, msg := range msgs {
		if msg.Role == ir.RoleSystem || msg.Role == ir.RoleDeveloper {
			continue
		}
		mPath := fmt.Sprintf("messages[%d]", mIdx)

		if msg.Role == ir.RoleTool {
			if msg.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: mPath + ".id"})
			}
			if !msg.Extra.IsZero() {
				*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: mPath + ".extra"})
			}
			for pIdx, p := range msg.Parts {
				if p.Kind != ir.PartToolResult || p.Result == nil {
					continue
				}
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				recordPartMetadataLosses(p, pPath, true, losses)
				callID := strings.TrimSpace(p.Result.CallID)
				if callID == "" {
					return nil, &qoder.RequestError{
						Code:    qoder.CodeMissingToolCall,
						Message: "Include the function_call for each function_call_output in the Responses input.",
					}
				}
				slot := &trackedResultSlot{
					partPath: pPath,
					msgPath:  mPath,
					msgIdx:   mIdx,
					partIdx:  pIdx,
					callID:   callID,
					result:   *p.Clone().Result,
				}
				if invalidCallIDs[callID] {
					slot.dropped = true
					*losses = append(*losses, ir.Loss{
						Kind:   ir.LossToolPairingRepaired,
						Path:   pPath,
						Detail: "invalid_call_dropped",
					})
				}
				stages = append(stages, encodeStageTurn{
					role:     ir.RoleTool,
					path:     mPath,
					toolSlot: slot,
				})
			}
			continue
		}

		st := encodeStageTurn{
			role:            msg.Role,
			path:            mPath,
			id:              msg.ID,
			extra:           msg.Extra,
			originallyEmpty: len(msg.Parts) == 0,
		}
		for pIdx, p := range msg.Parts {
			pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
			if msg.Role == ir.RoleAssistant && p.Kind == ir.PartToolCall && p.Call != nil {
				cand, ok := prepareQoderCallCandidate(p, pPath, pIdx, maxNameBytes, invalidCallIDs, losses)
				if ok {
					st.candidates = append(st.candidates, cand)
				}
				continue
			}
			st.parts = append(st.parts, trackedReqPart{
				part:      p,
				path:      pPath,
				origOrder: pIdx,
			})
		}
		stages = append(stages, st)
	}

	return pairAndEncodeStages(stages, maxNameBytes, losses)
}

// encodeChronologicalQoderMessages converts Chat Completions and Anthropic
// Messages turns in arrival order without enforcing Responses-only pairing
// rejections. Empty surviving histories return nil without allocating slices.
//
// Identified tool_result items and valid assistant tool calls stay in place even
// when one side of the pair is absent from client history. When a Messages
// tool_result block omits tool_use_id, its readable text folds into the
// surrounding user turn with LossToolPairingRepaired while keeping ordinary
// consecutive Chat user messages separate.
func encodeChronologicalQoderMessages(msgs []ir.Message, maxNameBytes int, losses *[]ir.Loss) []qoder.Message {
	invalidCallIDs := make(map[string]bool)
	var out []qoder.Message
	lastUserFromMissingID := false

	for mIdx, msg := range msgs {
		if msg.Role == ir.RoleSystem || msg.Role == ir.RoleDeveloper {
			continue
		}
		mPath := fmt.Sprintf("messages[%d]", mIdx)

		if msg.Role == ir.RoleTool {
			if msg.ID != "" {
				*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: mPath + ".id"})
			}
			if !msg.Extra.IsZero() {
				*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: mPath + ".extra"})
			}
			for pIdx, p := range msg.Parts {
				if p.Kind != ir.PartToolResult || p.Result == nil {
					continue
				}
				pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
				recordPartMetadataLosses(p, pPath, true, losses)
				callID := strings.TrimSpace(p.Result.CallID)
				if callID == "" {
					// Keep readable text from an ID-less tool_result inside a user turn
					// rather than inventing a synthetic tool_call_id for Qoder's gateway.
					*losses = append(*losses, ir.Loss{
						Kind:   ir.LossToolPairingRepaired,
						Path:   pPath,
						Detail: "missing_tool_call_id",
					})
					joined := extractToolResultPlainText(p.Result.Content, pPath, losses)
					appendMissingIDToolResultAsUser(&out, joined)
					lastUserFromMissingID = true
					continue
				}
				if invalidCallIDs[callID] {
					*losses = append(*losses, ir.Loss{
						Kind:   ir.LossToolPairingRepaired,
						Path:   pPath,
						Detail: "invalid_call_dropped",
					})
					continue
				}
				slot := &trackedResultSlot{
					partPath: pPath,
					msgPath:  mPath,
					msgIdx:   mIdx,
					partIdx:  pIdx,
					callID:   callID,
					result:   *p.Clone().Result,
				}
				out = append(out, encodeToolReplyTurn(slot, maxNameBytes, losses))
				lastUserFromMissingID = false
			}
			continue
		}

		st := encodeStageTurn{
			role:            msg.Role,
			path:            mPath,
			id:              msg.ID,
			extra:           msg.Extra,
			originallyEmpty: len(msg.Parts) == 0,
		}
		for pIdx, p := range msg.Parts {
			pPath := fmt.Sprintf("%s.parts[%d]", mPath, pIdx)
			if msg.Role == ir.RoleAssistant && p.Kind == ir.PartToolCall && p.Call != nil {
				cand, ok := prepareQoderCallCandidate(p, pPath, pIdx, maxNameBytes, invalidCallIDs, losses)
				if ok {
					st.candidates = append(st.candidates, cand)
				}
				continue
			}
			st.parts = append(st.parts, trackedReqPart{
				part:      p,
				path:      pPath,
				origOrder: pIdx,
			})
		}

		switch msg.Role {
		case ir.RoleAssistant:
			if asstMsg, keep := encodeAssistantTurn(st, st.candidates, losses); keep {
				out = append(out, asstMsg)
				lastUserFromMissingID = false
			}
		case ir.RoleUser:
			if userMsg, keep := encodeUserTurn(st, losses); keep {
				if lastUserFromMissingID && len(out) > 0 && out[len(out)-1].Role == "user" {
					mergeUserMessageIntoLast(&out[len(out)-1], userMsg)
				} else {
					out = append(out, userMsg)
				}
				lastUserFromMissingID = false
			}
		}
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

// mergeUserMessageIntoLast appends next.TextBlocks and non-empty next.Text onto
// dst when an ID-less tool_result split a single Anthropic user turn across IR
// messages. Empty trailing user segments leave dst's existing text unchanged.
func mergeUserMessageIntoLast(dst *qoder.Message, next qoder.Message) {
	if len(next.TextBlocks) > 0 {
		dst.TextBlocks = append(dst.TextBlocks, next.TextBlocks...)
	}
	if next.Text != "" {
		if dst.Text == "" {
			dst.Text = next.Text
		} else {
			dst.Text += "\n" + next.Text
		}
	}
}

// extractToolResultPlainText concatenates non-empty text parts inside content
// while recording metadata and unsupported media losses under partPath. Empty or
// media-only content slices return an empty string.
func extractToolResultPlainText(content []ir.Part, partPath string, losses *[]ir.Loss) string {
	var texts []string
	for cIdx, child := range content {
		cPath := fmt.Sprintf("%s.result.content[%d]", partPath, cIdx)
		recordPartMetadataLosses(child, cPath, true, losses)
		switch child.Kind {
		case ir.PartText:
			if child.Text != "" {
				texts = append(texts, child.Text)
			}
		case ir.PartMedia:
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: cPath})
		}
	}
	return strings.Join(texts, "")
}

// appendMissingIDToolResultAsUser folds readable text from an ID-less
// tool_result into an adjacent preceding user message or appends a new user
// message. Blank text appends an empty user message when no user turn precedes it.
func appendMissingIDToolResultAsUser(out *[]qoder.Message, text string) {
	if text == "" {
		if len(*out) == 0 || (*out)[len(*out)-1].Role != "user" {
			*out = append(*out, qoder.Message{Role: "user"})
		}
		return
	}
	block := qoder.TextBlock{Type: "text", Text: text}
	if len(*out) > 0 && (*out)[len(*out)-1].Role == "user" {
		prev := &(*out)[len(*out)-1]
		prev.TextBlocks = append(prev.TextBlocks, block)
		if prev.Text == "" {
			prev.Text = text
		} else {
			prev.Text += "\n" + text
		}
		return
	}
	*out = append(*out, qoder.Message{
		Role:       "user",
		Text:       text,
		TextBlocks: []qoder.TextBlock{block},
	})
}

// prepareQoderCallCandidate validates one assistant PartToolCall and lowers any
// standalone custom, search, or namespaced call into a function candidate.
// Hosted or malformed JSON calls drop with LossToolPairingRepaired.
func prepareQoderCallCandidate(
	p ir.Part,
	pPath string,
	origOrder int,
	maxNameBytes int,
	invalidCallIDs map[string]bool,
	losses *[]ir.Loss,
) (trackedCallCandidate, bool) {
	call := p.Call
	callID := strings.TrimSpace(call.CallID)
	if callID == "" || call.Tool.Kind == ir.ToolHosted {
		recordPartMetadataLosses(p, pPath, true, losses)
		if call.Token != nil {
			*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath + ".call.token"})
		}
		if callID != "" {
			invalidCallIDs[callID] = true
		}
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolPairingRepaired, Path: pPath})
		return trackedCallCandidate{}, false
	}

	var name string
	var args string
	lowered := false

	switch call.Tool.Kind {
	case ir.ToolFunction:
		name = sanitizeQoderToolName(flattenToolNamespace(call.Tool.Namespace, call.Tool.Name, maxNameBytes), maxNameBytes)
		if call.Tool.Namespace != "" || name != call.Tool.Name {
			lowered = true
		}
		args = call.Arguments
		if strings.TrimSpace(args) == "" {
			args = "{}"
		} else if err := validateSingleJSONValue([]byte(args)); err != nil {
			recordPartMetadataLosses(p, pPath, true, losses)
			if call.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath + ".call.token"})
			}
			invalidCallIDs[callID] = true
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolPairingRepaired, Path: pPath, Detail: "malformed_arguments"})
			return trackedCallCandidate{}, false
		}

	case ir.ToolCustom:
		name = sanitizeQoderToolName(flattenToolNamespace(call.Tool.Namespace, call.Tool.Name, maxNameBytes), maxNameBytes)
		wrapped, _ := marshalDeterministic(map[string]string{"input": call.Arguments})
		args = string(wrapped)
		lowered = true

	case ir.ToolClientSearch:
		name = standaloneSearchProxyName
		args = call.Arguments
		if strings.TrimSpace(args) == "" {
			args = "{}"
		}
		lowered = true
	}

	recordPartMetadataLosses(p, pPath, true, losses)
	if call.Token != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: pPath + ".call.token"})
	}
	if lowered {
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: pPath})
	}
	delete(invalidCallIDs, callID)
	return trackedCallCandidate{
		callID:    callID,
		name:      name,
		arguments: args,
		path:      pPath,
		origOrder: origOrder,
	}, true
}

// pairAndEncodeStages binds assistant tool calls to subsequent tool_result
// slots, rejects unclaimed orphan tool outputs with *qoder.RequestError, and
// serializes surviving turns into ordered qoder.Message values. Empty surviving
// histories return (nil, nil).
//
// Unanswered assistant tool calls drop with LossToolPairingRepaired while
// preserving sibling visible text on the turn, and parallel replies claimed out
// of source slice order within a single RoleTool message emit LossPartsReordered
// so Law Q4 reconciles split tool messages accurately.
func pairAndEncodeStages(stages []encodeStageTurn, maxNameBytes int, losses *[]ir.Loss) ([]qoder.Message, error) {
	// resolvedAssistant pairs kept assistant calls with their matched reply slots.
	type resolvedAssistant struct {
		keptCalls []trackedCallCandidate // calls that matched a reply
		replies   []*trackedResultSlot   // matched replies in keptCalls order
	}
	resolved := make(map[int]resolvedAssistant)
	reorderedToolMsgs := make(map[string]bool)

	for i := range stages {
		if stages[i].role != ir.RoleAssistant || len(stages[i].candidates) == 0 {
			continue
		}
		var ra resolvedAssistant
		for _, cand := range stages[i].candidates {
			matched := claimReplySlot(stages, i+1, cand.callID, losses)
			if matched == nil {
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossToolPairingRepaired,
					Path:   cand.path,
					Detail: "unanswered_tool_call",
				})
				continue
			}
			if len(ra.replies) > 0 {
				prev := ra.replies[len(ra.replies)-1]
				if prev.msgIdx == matched.msgIdx && prev.partIdx > matched.partIdx && !reorderedToolMsgs[matched.msgPath] {
					reorderedToolMsgs[matched.msgPath] = true
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossPartsReordered,
						Path: matched.msgPath,
					})
				}
			}
			ra.keptCalls = append(ra.keptCalls, cand)
			ra.replies = append(ra.replies, matched)
		}
		resolved[i] = ra
	}

	for i := range stages {
		if stages[i].role == ir.RoleTool && stages[i].toolSlot != nil {
			slot := stages[i].toolSlot
			if !slot.claimed && !slot.dropped {
				return nil, &qoder.RequestError{
					Code:    qoder.CodeMissingToolCall,
					Message: "Include the function_call for each function_call_output in the Responses input.",
				}
			}
		}
	}

	out := make([]qoder.Message, 0, len(stages))
	for i := range stages {
		st := stages[i]
		switch st.role {
		case ir.RoleTool:
			continue
		case ir.RoleAssistant:
			ra := resolved[i]
			asstMsg, keep := encodeAssistantTurn(st, ra.keptCalls, losses)
			if keep {
				out = append(out, asstMsg)
			}
			for _, reply := range ra.replies {
				out = append(out, encodeToolReplyTurn(reply, maxNameBytes, losses))
			}
		case ir.RoleUser:
			userMsg, keep := encodeUserTurn(st, losses)
			if keep {
				out = append(out, userMsg)
			}
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

// claimReplySlot scans forward from startIdx up to the next assistant turn
// redeclaring callID and claims matching unclaimed tool_result slots. When
// duplicate replies match one call, earlier slots record LossToolPairingRepaired
// and the final reply is returned; absent matches return nil.
func claimReplySlot(stages []encodeStageTurn, startIdx int, callID string, losses *[]ir.Loss) *trackedResultSlot {
	var matches []*trackedResultSlot
	for j := startIdx; j < len(stages); j++ {
		if stages[j].role == ir.RoleAssistant {
			redeclares := false
			for _, c := range stages[j].candidates {
				if c.callID == callID {
					redeclares = true
					break
				}
			}
			if redeclares {
				break
			}
		}
		if stages[j].role != ir.RoleTool || stages[j].toolSlot == nil {
			continue
		}
		slot := stages[j].toolSlot
		if slot.claimed || slot.dropped || slot.callID != callID {
			continue
		}
		slot.claimed = true
		matches = append(matches, slot)
	}
	if len(matches) == 0 {
		return nil
	}
	for k := range len(matches) - 1 {
		*losses = append(*losses, ir.Loss{
			Kind:   ir.LossToolPairingRepaired,
			Path:   matches[k].partPath,
			Detail: "duplicate_tool_result",
		})
	}
	return matches[len(matches)-1]
}

// encodeUserTurn converts an ir.RoleUser stage into a qoder.Message preserving
// discrete text blocks and cache_control breakpoints. Source turns that started
// with zero parts keep an empty user message, whereas turns emptied by dropping
// unsupported media or opaque parts return keep=false.
func encodeUserTurn(st encodeStageTurn, losses *[]ir.Loss) (qoder.Message, bool) {
	if st.id != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: st.path + ".id"})
	}
	if !st.extra.IsZero() {
		*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: st.path + ".extra"})
	}
	if st.originallyEmpty {
		return qoder.Message{Role: "user"}, true
	}

	var blocks []qoder.TextBlock
	var nonEmptyTexts []string
	for _, tp := range st.parts {
		p := tp.part
		switch p.Kind {
		case ir.PartText:
			recordPartMetadataLosses(p, tp.path, false, losses)
			blocks = append(blocks, qoder.TextBlock{
				Type:         "text",
				Text:         p.Text,
				CacheControl: cacheControlFromIR(p.Cache),
			})
			if p.Text != "" {
				nonEmptyTexts = append(nonEmptyTexts, p.Text)
			}
		case ir.PartRefusal:
			recordPartMetadataLosses(p, tp.path, false, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: tp.path})
			blocks = append(blocks, qoder.TextBlock{
				Type:         "text",
				Text:         p.Text,
				CacheControl: cacheControlFromIR(p.Cache),
			})
			if p.Text != "" {
				nonEmptyTexts = append(nonEmptyTexts, p.Text)
			}
		case ir.PartMedia:
			recordPartMetadataLosses(p, tp.path, true, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: tp.path})
		case ir.PartOpaque:
			recordPartMetadataLosses(p, tp.path, true, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: tp.path})
		case ir.PartReasoning:
			recordPartMetadataLosses(p, tp.path, true, losses)
			recordDroppedReasoningPart(p, tp.path, losses)
		}
	}

	if len(blocks) == 0 {
		return qoder.Message{}, false
	}
	return qoder.Message{
		Role:       "user",
		Text:       strings.Join(nonEmptyTexts, "\n"),
		TextBlocks: blocks,
	}, true
}

// encodeAssistantTurn converts an ir.RoleAssistant stage and its surviving tool
// calls into a qoder.Message. When visible text or refusal parts appeared after
// a surviving tool call on the source turn, LossPartsReordered is recorded;
// non-empty turns whose parts were all dropped return keep=false.
func encodeAssistantTurn(st encodeStageTurn, keptCalls []trackedCallCandidate, losses *[]ir.Loss) (qoder.Message, bool) {
	if st.id != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: st.path + ".id"})
	}
	if !st.extra.IsZero() {
		*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: st.path + ".extra"})
	}
	if st.originallyEmpty {
		return qoder.Message{Role: "assistant"}, true
	}

	var blocks []qoder.TextBlock
	var nonEmptyTexts []string
	var ranks []partRankEntry

	for _, tp := range st.parts {
		p := tp.part
		switch p.Kind {
		case ir.PartText:
			recordPartMetadataLosses(p, tp.path, true, losses)
			if p.Token != nil {
				*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: tp.path + ".token"})
			}
			blocks = append(blocks, qoder.TextBlock{Type: "text", Text: p.Text})
			if p.Text != "" {
				nonEmptyTexts = append(nonEmptyTexts, p.Text)
			}
			ranks = append(ranks, partRankEntry{origOrder: tp.origOrder, rank: 0})

		case ir.PartRefusal:
			recordPartMetadataLosses(p, tp.path, true, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossRefusalInlined, Path: tp.path})
			blocks = append(blocks, qoder.TextBlock{Type: "text", Text: p.Text})
			if p.Text != "" {
				nonEmptyTexts = append(nonEmptyTexts, p.Text)
			}
			ranks = append(ranks, partRankEntry{origOrder: tp.origOrder, rank: 0})

		case ir.PartReasoning:
			recordPartMetadataLosses(p, tp.path, true, losses)
			recordDroppedReasoningPart(p, tp.path, losses)

		case ir.PartMedia:
			recordPartMetadataLosses(p, tp.path, true, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: tp.path})

		case ir.PartOpaque:
			recordPartMetadataLosses(p, tp.path, true, losses)
			*losses = append(*losses, ir.Loss{Kind: ir.LossOpaqueItemDropped, Path: tp.path})
		}
	}

	var outCalls []qoder.ToolCall
	if len(keptCalls) > 0 {
		outCalls = make([]qoder.ToolCall, len(keptCalls))
		for i, c := range keptCalls {
			outCalls[i] = qoder.ToolCall{
				ID:   c.callID,
				Type: "function",
				Function: qoder.ToolCallFunction{
					Name:      c.name,
					Arguments: c.arguments,
				},
			}
			ranks = append(ranks, partRankEntry{origOrder: c.origOrder, rank: 1})
		}
	}

	if len(blocks) == 0 && len(outCalls) == 0 {
		return qoder.Message{}, false
	}
	if hasCrossedPartRanks(ranks) {
		*losses = append(*losses, ir.Loss{Kind: ir.LossPartsReordered, Path: st.path})
	}

	return qoder.Message{
		Role:       "assistant",
		Text:       strings.Join(nonEmptyTexts, "\n"),
		TextBlocks: blocks,
		ToolCalls:  outCalls,
	}, true
}

// encodeToolReplyTurn converts one trackedResultSlot into a qoder.Message with
// role "tool" and records LossMediaUnsupported for nested media parts. When no
// text parts survive on the result, Text receives "(empty)" alongside
// EmptyContent=true so wire serialization and PayloadToIR stay consistent.
func encodeToolReplyTurn(slot *trackedResultSlot, maxNameBytes int, losses *[]ir.Loss) qoder.Message {
	res := slot.result
	var blocks []qoder.TextBlock
	var texts []string

	for cIdx, child := range res.Content {
		cPath := fmt.Sprintf("%s.result.content[%d]", slot.partPath, cIdx)
		recordPartMetadataLosses(child, cPath, true, losses)
		switch child.Kind {
		case ir.PartText:
			blocks = append(blocks, qoder.TextBlock{Type: "text", Text: child.Text})
			texts = append(texts, child.Text)
		case ir.PartMedia:
			*losses = append(*losses, ir.Loss{Kind: ir.LossMediaUnsupported, Path: cPath})
		}
	}

	toolName := resolveCorrelatedToolName(res.Tool, slot.partPath, maxNameBytes, losses)
	joined := strings.Join(texts, "")
	emptyContent := len(blocks) == 0
	if joined == "" {
		joined = "(empty)"
	}

	return qoder.Message{
		Role:         "tool",
		Text:         joined,
		TextBlocks:   blocks,
		ToolCallID:   slot.callID,
		Name:         toolName,
		IsError:      res.IsError,
		EmptyContent: emptyContent,
	}
}

// resolveCorrelatedToolName returns the sanitized flat function name for a
// correlated ToolResult.Tool and appends LossToolLowered when standalone
// namespace, custom, or search lowering applies. Unlinked or hosted references
// return an empty string so standalone tool results omit the wire name field.
func resolveCorrelatedToolName(ref ir.ToolRef, partPath string, maxNameBytes int, losses *[]ir.Loss) string {
	switch ref.Kind {
	case ir.ToolFunction:
		if ref.Name == "" {
			return ""
		}
		name := sanitizeQoderToolName(flattenToolNamespace(ref.Namespace, ref.Name, maxNameBytes), maxNameBytes)
		if ref.Namespace != "" || name != ref.Name {
			*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: partPath})
		}
		return name
	case ir.ToolCustom:
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: partPath})
		return sanitizeQoderToolName(flattenToolNamespace(ref.Namespace, ref.Name, maxNameBytes), maxNameBytes)
	case ir.ToolClientSearch:
		*losses = append(*losses, ir.Loss{Kind: ir.LossToolLowered, Path: partPath})
		return standaloneSearchProxyName
	default:
		return ""
	}
}

// recordPartMetadataLosses appends LossIDRewritten, LossExtraDropped, and
// LossAnnotationDropped for p at path. When dropCache is true, a non-nil p.Cache
// also appends LossCacheMarkDropped because only user text blocks retain cache markers.
func recordPartMetadataLosses(p ir.Part, path string, dropCache bool, losses *[]ir.Loss) {
	if p.ID != "" {
		*losses = append(*losses, ir.Loss{Kind: ir.LossIDRewritten, Path: path + ".id"})
	}
	if dropCache && p.Cache != nil {
		*losses = append(*losses, ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path + ".cache"})
	}
	if !p.Extra.IsZero() {
		*losses = append(*losses, ir.Loss{Kind: ir.LossExtraDropped, Path: path + ".extra"})
	}
	if len(p.Annotations) > 0 {
		*losses = append(*losses, ir.Loss{Kind: ir.LossAnnotationDropped, Path: path + ".annotations"})
	}
}

// recordDroppedReasoningPart appends LossReplayTokenDropped and
// LossReasoningTextDropped for an omitted historical PartReasoning item at path,
// inspecting un-unpacked trc1 carriers so plain text carriers report text drops
// rather than token drops. Parts with a nil Reasoning branch return immediately.
func recordDroppedReasoningPart(p ir.Part, path string, losses *[]ir.Loss) {
	if p.Reasoning == nil {
		return
	}
	var carrier ir.Carrier
	var isCarrier bool
	if p.Reasoning.Token != nil {
		carrier, isCarrier = ir.ParseCarrier(p.Reasoning.Token.Data)
	}
	hasToken := p.Reasoning.Redacted || (p.Reasoning.Token != nil && (!isCarrier || carrier.Scheme != ir.SchemePlain))
	hasText := len(p.Reasoning.Segments) > 0 || (isCarrier && carrier.Text != "") || (p.Reasoning.Token == nil && !p.Reasoning.Redacted)
	if hasToken {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReplayTokenDropped, Path: path + ".reasoning.token"})
	}
	if hasText {
		*losses = append(*losses, ir.Loss{Kind: ir.LossReasoningTextDropped, Path: path + ".reasoning"})
	}
}

// cacheControlFromIR converts an optional ir.CacheMark into an ephemeral
// *qoder.CacheControl preserving mark.TTL. Nil cache marks return nil so text
// blocks without cache breakpoints omit cache_control on the wire.
func cacheControlFromIR(mark *ir.CacheMark) *qoder.CacheControl {
	if mark == nil {
		return nil
	}
	return &qoder.CacheControl{
		Type: "ephemeral",
		TTL:  mark.TTL,
	}
}

// hasCrossedPartRanks reports whether sorting entries by target group rank
// alters any relative ordering present in origOrder.
func hasCrossedPartRanks(entries []partRankEntry) bool {
	for i := range entries {
		for j := i + 1; j < len(entries); j++ {
			if (entries[i].origOrder < entries[j].origOrder && entries[i].rank > entries[j].rank) ||
				(entries[i].origOrder > entries[j].origOrder && entries[i].rank < entries[j].rank) {
				return true
			}
		}
	}
	return false
}

// flattenToolNamespace joins namespace and name with "__" within maxBytes,
// keeping empty and default "functions" scopes bare. Oversized flattened names
// truncate on UTF-8 rune boundaries with an eight-hex SHA-256 suffix.
func flattenToolNamespace(namespace, name string, maxBytes int) string {
	if maxBytes <= 0 {
		maxBytes = defaultQoderToolNameMaxBytes
	}
	if namespace == "" || namespace == "functions" {
		return name
	}
	full := namespace + "__" + name
	if len(full) <= maxBytes {
		return full
	}
	// Reserve ten bytes for "__" plus an eight-hex SHA-256 digest so distinct
	// long namespaced tools remain collision-free after UTF-8 rune truncation.
	sum := sha256.Sum256([]byte(full))
	suffix := "__" + hex.EncodeToString(sum[:4])
	if maxBytes <= len(suffix) {
		return suffix[len(suffix)-maxBytes:]
	}
	budget := maxBytes - len(suffix)
	var b strings.Builder
	for _, r := range full {
		if b.Len()+len(string(r)) > budget {
			break
		}
		_, _ = b.WriteRune(r)
	}
	return b.String() + suffix
}

// sanitizeQoderToolName replaces characters outside [a-zA-Z0-9_-] with '_' and
// bounds the resulting identifier to maxBytes using a SHA-256 hex suffix. Empty
// inputs default to "tool" so every function declaration carries a non-empty name.
func sanitizeQoderToolName(name string, maxBytes int) string {
	if maxBytes <= 0 {
		maxBytes = defaultQoderToolNameMaxBytes
	}
	mapped := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, name)
	if mapped == "" {
		mapped = "tool"
	}
	if len(mapped) <= maxBytes {
		return mapped
	}
	// Hash the original unsanitized tool name into an eight-hex suffix so long
	// identifiers differing only past maxBytes never collide on the wire.
	sum := sha256.Sum256([]byte(name))
	suffix := "__" + hex.EncodeToString(sum[:4])
	if maxBytes <= len(suffix) {
		return suffix[len(suffix)-maxBytes:]
	}
	return mapped[:maxBytes-len(suffix)] + suffix
}

// payloadMessageToIR converts one normalized qoder.Message back into a
// validated ir.Message preserving discrete text blocks, cache breakpoints, and
// empty tool-result content slices. Unrecognized Role strings return an error.
func payloadMessageToIR(m qoder.Message) (ir.Message, error) {
	switch m.Role {
	case "user":
		parts := make([]ir.Part, 0, max(len(m.TextBlocks), 1))
		if len(m.TextBlocks) > 0 {
			for _, b := range m.TextBlocks {
				parts = append(parts, ir.Part{
					Kind:  ir.PartText,
					Text:  b.Text,
					Cache: cacheMarkFromQoder(b.CacheControl),
				})
			}
		} else if m.Text != "" {
			parts = append(parts, ir.Part{Kind: ir.PartText, Text: m.Text})
		}
		return ir.Message{Role: ir.RoleUser, Parts: parts}, nil

	case "assistant":
		parts := make([]ir.Part, 0, len(m.TextBlocks)+len(m.ToolCalls)+1)
		if len(m.TextBlocks) > 0 {
			for _, b := range m.TextBlocks {
				parts = append(parts, ir.Part{Kind: ir.PartText, Text: b.Text})
			}
		} else if m.Text != "" {
			parts = append(parts, ir.Part{Kind: ir.PartText, Text: m.Text})
		}
		for _, tc := range m.ToolCalls {
			args := tc.Function.Arguments
			if strings.TrimSpace(args) == "" {
				args = "{}"
			}
			parts = append(parts, ir.Part{
				Kind: ir.PartToolCall,
				Call: &ir.ToolCall{
					CallID:    tc.ID,
					Tool:      ir.ToolRef{Kind: ir.ToolFunction, Name: tc.Function.Name},
					Arguments: args,
				},
			})
		}
		return ir.Message{Role: ir.RoleAssistant, Parts: parts}, nil

	case "tool":
		content := make([]ir.Part, 0, max(len(m.TextBlocks), 1))
		if !m.EmptyContent {
			if len(m.TextBlocks) > 0 {
				for _, b := range m.TextBlocks {
					content = append(content, ir.Part{Kind: ir.PartText, Text: b.Text})
				}
			} else if m.Text != "" {
				content = append(content, ir.Part{Kind: ir.PartText, Text: m.Text})
			}
		}
		var ref ir.ToolRef
		if strings.TrimSpace(m.Name) != "" {
			ref = ir.ToolRef{Kind: ir.ToolFunction, Name: m.Name}
		}
		return ir.Message{
			Role: ir.RoleTool,
			Parts: []ir.Part{{
				Kind: ir.PartToolResult,
				Result: &ir.ToolResult{
					CallID:  m.ToolCallID,
					Tool:    ref,
					Content: content,
					IsError: m.IsError,
				},
			}},
		}, nil

	default:
		return ir.Message{}, fmt.Errorf("unsupported qoder message role %q", m.Role)
	}
}

// cacheMarkFromQoder converts an optional *qoder.CacheControl back into an
// *ir.CacheMark preserving cc.TTL for reconstructed IR user text blocks. Nil
// pointers return nil without allocating so unannotated blocks stay unmarked.
func cacheMarkFromQoder(cc *qoder.CacheControl) *ir.CacheMark {
	if cc == nil {
		return nil
	}
	return &ir.CacheMark{TTL: cc.TTL}
}

// payloadToolChoiceToIR converts a normalized *qoder.ToolChoice back into an
// *ir.ToolChoice while preserving AllowedTools function subsets for Law Q4
// comparison. Unrecognized Qoder tool_choice modes return a descriptive error.
func payloadToolChoiceToIR(tc *qoder.ToolChoice) (*ir.ToolChoice, error) {
	var allowed []ir.ToolRef
	if len(tc.AllowedTools) > 0 {
		allowed = make([]ir.ToolRef, len(tc.AllowedTools))
		for i, name := range tc.AllowedTools {
			allowed[i] = ir.ToolRef{Kind: ir.ToolFunction, Name: name}
		}
	}
	switch tc.Mode {
	case qoder.ToolChoiceAuto:
		return &ir.ToolChoice{Mode: ir.ToolChoiceAuto, AllowedTools: allowed}, nil
	case qoder.ToolChoiceNone:
		return &ir.ToolChoice{Mode: ir.ToolChoiceNone}, nil
	case qoder.ToolChoiceRequired:
		return &ir.ToolChoice{Mode: ir.ToolChoiceRequired, AllowedTools: allowed}, nil
	case qoder.ToolChoiceFunction:
		return &ir.ToolChoice{
			Mode: ir.ToolChoiceTool,
			Tool: ir.ToolRef{Kind: ir.ToolFunction, Name: tc.FunctionName},
		}, nil
	default:
		return nil, fmt.Errorf("unsupported qoder tool_choice mode %q", tc.Mode)
	}
}
