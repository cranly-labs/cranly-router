package convert

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// exchange.go coordinates one client-to-upstream protocol conversion attempt,
// encapsulating request decoding, continuity carrier unpacking, client marker
// stripping, tool lowering, dialect normalization, target request encoding, and
// response/stream/buffer factory construction behind a narrow byte-oriented API.
//
// Retaining an untouched snapshot of the decoded client ir.Request alongside
// the mutated upstream working copy lets response and stream encoders echo
// original client tool declarations, response formatting, and include_usage
// flags accurately. UpstreamRequest runs the transformation pipeline once via
// sync.Once, caching deterministic upstream wire bytes and remapped
// original-coordinate ir.Loss records for subsequent inspection.

// Route pairs the client-facing wire protocol, upstream wire protocol, and
// target endpoint Dialect for one conversion attempt.
type Route struct {
	Client   protocol.ProtocolID // client ingress wire protocol
	Upstream protocol.ProtocolID // target upstream wire protocol
	Dialect  Dialect             // upstream endpoint capabilities and parameter rules
}

// Options configures client model echoing, deterministic runtime callbacks,
// stateful CallMemory bindings, WebSocket inherited tool maps, and upstream
// tool-name reverse mapping for NewExchange.
type Options struct {
	Route       Route      // client protocol, upstream protocol, and target dialect
	ClientModel string     // model name presented back to the client; empty uses request model
	Runtime     Runtime    // wall-clock and response-side ID callbacks
	Calls       CallMemory // optional stateful upstream call identity memory
	Tools       *ToolMap   // optional inherited tool map from an earlier WS turn
	// MapUpstreamToolName optionally rewrites decoded upstream non-hosted tool
	// call names back to the lowered declaration spelling before ToolMap
	// restoration runs. Nil or empty return values leave the decoded name intact.
	MapUpstreamToolName func(string) string
}

// RequestInfo exposes pre-stripping client request metadata inspected by
// gateway routing, billing, and telemetry before upstream dispatch.
type RequestInfo struct {
	Model        string // model name parsed from the client request
	Stream       bool   // whether the client requested streaming SSE delivery
	IncludeUsage bool   // whether Chat stream_options.include_usage was enabled
	Effort       string // raw client reasoning effort recorded before StripEffort
}

// Exchange owns the decoded client request, lowered ToolMap, cached upstream
// wire bytes, and response conversion state for a single upstream attempt.
type Exchange struct {
	route               Route               // resolved route with protocol-defaulted Dialect
	clientCodec         clientCodec         // registered client codec for response/stream encoding
	upCodec             upstreamCodec       // registered upstream codec for request/response handling
	clientModel         string              // model name echoed to the client
	runtime             Runtime             // response-side clock and ID callbacks
	calls               CallMemory          // optional upstream tool-call binding memory
	inheritedMap        *ToolMap            // cloned inherited ToolMap from Options.Tools
	mapUpstreamToolName func(string) string // optional pre-raise upstream tool-name reverse mapper
	origReq             *ir.Request         // immutable decoded client request for ClientView echoing
	info                RequestInfo         // pre-stripping client request metadata

	// reqOnce initializes upstreamBytes, upstreamErr, and toolMap at most once
	// during ensureUpstreamPrepared, while mu guards appending response or buffer
	// loss records onto recordedLosses after initial request preparation finishes.
	reqOnce        sync.Once  // ensures UpstreamRequest encodes at most once
	upstreamBytes  []byte     // cached deterministic upstream request JSON bytes
	upstreamErr    error      // cached error from upstream request preparation
	toolMap        *ToolMap   // lowered ToolMap produced during request preparation
	mu             sync.Mutex // guards recordedLosses across request and response stages
	recordedLosses []ir.Loss  // cumulative Loss records in original coordinates
}

// cursorChatControls holds Chat Completions top-level controls extracted from
// an input-shaped Cursor request before delegating to the Responses decoder.
type cursorChatControls struct {
	includeUsage    *bool    // extracted stream_options.include_usage toggle
	reasoningEffort string   // extracted top-level reasoning_effort value
	stop            []string // extracted scalar or array stop sequences
}

// NeedsConversion reports whether route requires cross-protocol IR conversion.
func NeedsConversion(route Route) bool {
	return route.Client != route.Upstream
}

// NewExchange decodes and validates clientBody for one upstream conversion
// attempt configured by opts. Cursor requests arriving on Chat Completions
// without "messages" but with top-level "input" decode through OpenAI Responses
// while keeping Chat Completions as the client response view.
// @project-doc docs/architecture/protocol_conversion.md#conversion_pipeline
func NewExchange(clientBody []byte, opts Options) (*Exchange, error) {
	cCodec, err := lookupClientCodec(opts.Route.Client)
	if err != nil {
		return nil, err
	}
	uCodec, err := lookupUpstreamCodec(opts.Route.Upstream)
	if err != nil {
		return nil, err
	}

	decodeCodec := cCodec
	decodeBody := clientBody
	var cursorControls cursorChatControls
	cursorChatInput := false
	if opts.Route.Client == protocol.ProtocolOpenAIChatCompletions &&
		isResponsesShapedChatBody(clientBody) {
		// Decode Cursor's input-shaped /v1/chat/completions requests via the
		// Responses request decoder after extracting and stripping Chat-only
		// top-level controls so consumed keys neither leak nor trigger extra_dropped.
		respCodec, lookupErr := lookupClientCodec(protocol.ProtocolOpenAIResponses)
		if lookupErr != nil {
			return nil, lookupErr
		}
		strippedBody, extracted, prepErr := extractAndStripCursorChatControls(clientBody)
		if prepErr != nil {
			return nil, prepErr
		}
		decodeCodec = respCodec
		decodeBody = strippedBody
		cursorControls = extracted
		cursorChatInput = true
	}

	decoded, err := decodeCodec.DecodeRequest(decodeBody)
	if err != nil {
		return nil, err
	}
	if cursorChatInput {
		applyCursorChatControls(decoded, cursorControls)
	}
	if err := ir.Validate(decoded); err != nil {
		return nil, err
	}

	info := RequestInfo{
		Model:        decoded.Model,
		Stream:       decoded.Stream,
		IncludeUsage: decoded.IncludeUsage != nil && *decoded.IncludeUsage,
	}
	if decoded.Reasoning != nil {
		info.Effort = strings.TrimSpace(decoded.Reasoning.Effort)
	}

	effRoute := opts.Route
	effRoute.Dialect = resolveEffectiveDialect(opts.Route)
	if effRoute.Upstream == protocol.ProtocolQoderChat {
		// Enforce Qoder's stateless Responses and strict tool-pairing rules only when
		// the client request originated as OpenAI Responses or input-shaped Cursor Chat.
		effRoute.Dialect.StatelessResponses = opts.Route.Client == protocol.ProtocolOpenAIResponses || cursorChatInput
		if opts.Route.Client == protocol.ProtocolOpenAIResponses && decoded.Store == nil {
			// Qoder never persists server-side Responses state, so omitted ingress store
			// echoes back as explicit false on client response and lifecycle envelopes.
			decoded.Store = new(false)
		}
	}

	clientModel := opts.ClientModel
	if clientModel == "" {
		clientModel = decoded.Model
	}

	return &Exchange{
		route:               effRoute,
		clientCodec:         cCodec,
		upCodec:             uCodec,
		clientModel:         clientModel,
		runtime:             opts.Runtime,
		calls:               opts.Calls,
		inheritedMap:        opts.Tools.Clone(),
		mapUpstreamToolName: opts.MapUpstreamToolName,
		origReq:             decoded,
		info:                info,
	}, nil
}

// UpstreamRequest runs the request IR transformation pipeline once and encodes
// deterministic upstream wire JSON bytes. Every invocation returns an
// independent cloned byte slice so caller mutations never corrupt cached state.
func (x *Exchange) UpstreamRequest() ([]byte, error) {
	x.ensureUpstreamPrepared()
	if x.upstreamErr != nil {
		return nil, x.upstreamErr
	}
	return bytes.Clone(x.upstreamBytes), nil
}

// ReencodeUpstream prepares a fresh clone of x's decoded client request through
// carrier unpacking, marker stripping, tool lowering, token selection, and
// dialect adjustments, then invokes mutate before encoding deterministic
// upstream wire JSON bytes. Returning changed == false from mutate short-circuits
// without running target request encoding and returns (nil, false, nil).
//
// x's cached UpstreamRequest bytes, lowered ToolMap, and recorded Losses remain
// untouched so response, stream, and buffer converters on x continue restoring
// client tool identities and sealing continuity carriers after a retry succeeds.
// Nil receivers, nil mutate callbacks, or pipeline and encoder failures return
// an error alongside changed == false.
func (x *Exchange) ReencodeUpstream(mutate func(req *ir.Request, dialect *Dialect) (bool, error)) ([]byte, bool, error) {
	if x == nil {
		return nil, false, errors.New("convert: nil Exchange")
	}
	if mutate == nil {
		return nil, false, errors.New("convert: nil ReencodeUpstream mutate callback")
	}
	x.ensureUpstreamPrepared()
	if x.upstreamErr != nil {
		return nil, false, x.upstreamErr
	}

	work := x.origReq.Clone()
	coords := newSourceCoords(work)
	_, recalledSlots := openCarriers(work, coords, x.route, x.calls)
	_ = stripClientMarkers(work, coords, x.route)
	if _, _, err := lowerTools(work, coords, x.route, x.inheritedMap); err != nil {
		return nil, false, err
	}
	_ = selectReplayTokens(work, coords, x.route, recalledSlots)
	if _, err := applyDialectRequestAdjustments(work, coords, x.route); err != nil {
		return nil, false, err
	}
	ir.Link(work)

	dialect := x.route.Dialect
	dialect.Reasoning = dialect.Reasoning.Clone()
	changed, err := mutate(work, &dialect)
	if err != nil || !changed {
		return nil, false, err
	}
	ir.Link(work)

	encoded, _, err := x.upCodec.EncodeRequest(work, dialect)
	if err != nil {
		return nil, false, err
	}
	return encoded, true, nil
}

// Losses returns a detached copy of all ir.Loss entries recorded so far by
// UpstreamRequest and any completed Response, Stream, or Buffer conversions on x.
// Invoking Losses triggers lazy request preparation when UpstreamRequest has not
// yet run, and mutating the returned slice never alters x's internal loss log.
func (x *Exchange) Losses() []ir.Loss {
	x.ensureUpstreamPrepared()
	x.mu.Lock()
	defer x.mu.Unlock()
	return slices.Clone(x.recordedLosses)
}

// ToolMap returns an independent deep copy of the lowered ToolMap constructed
// during request preparation on x, or nil when no client tool lowering occurred.
// Invoking ToolMap triggers lazy request preparation before cloning state so
// WebSocket sessions can persist the returned value safely across turns.
func (x *Exchange) ToolMap() *ToolMap {
	x.ensureUpstreamPrepared()
	return x.toolMap.Clone()
}

// Info returns pre-stripping Model, Stream, IncludeUsage, and Effort metadata
// captured immediately after decoding the client request.
func (x *Exchange) Info() RequestInfo {
	return x.info
}

// Response converts a complete buffered upstream JSON response body into client
// wire JSON bytes, applying any configured upstream tool-name reverse mapper
// before restoring lowered tool identities and sealing trc1 continuity carriers.
// Incurred response-side ir.Loss entries append onto Exchange.Losses for
// post-conversion inspection.
func (x *Exchange) Response(upstreamBody []byte) ([]byte, error) {
	x.ensureUpstreamPrepared()
	if x.upstreamErr != nil {
		return nil, x.upstreamErr
	}

	resp, err := x.upCodec.DecodeResponse(upstreamBody)
	if err != nil {
		return nil, err
	}
	tagUpstreamResponseTokens(resp, x.route.Dialect)
	mapUpstreamResponseToolNames(resp, x.mapUpstreamToolName)
	raiseResponseTools(resp, x.toolMap)

	var respLosses []ir.Loss
	outPartToOrig, err := sealResponseCarriers(resp, x.route, x.calls, x.runtime, &respLosses)
	if err != nil {
		return nil, err
	}
	encoded, encLosses, err := x.clientCodec.EncodeResponse(resp, x.clientView())
	if err != nil {
		return nil, err
	}
	respLosses = remapResponseLosses(respLosses, encLosses, outPartToOrig)
	x.appendLosses(respLosses)
	return encoded, nil
}

// Stream opens a stateful streaming converter that translates upstream SSE
// frames into client SSE frames for this Exchange. Invoking Stream ensures
// request preparation has completed so lowered ToolMap restorations apply.
func (x *Exchange) Stream() *Stream {
	x.ensureUpstreamPrepared()
	return newStream(x)
}

// Buffer opens a streaming-to-buffered accumulator that folds upstream SSE
// frames into a single client JSON response body for this Exchange. Invoking
// Buffer ensures request preparation has completed before frame accumulation.
func (x *Exchange) Buffer() *Buffer {
	x.ensureUpstreamPrepared()
	return newBuffer(x)
}

// ensureUpstreamPrepared executes x's request IR transformation and upstream
// encoding pipeline at most once on a detached clone of origReq. Cached
// upstreamBytes, upstreamErr, toolMap, and initial recordedLosses persist for
// all subsequent UpstreamRequest, Response, Stream, Buffer, and Losses calls.
func (x *Exchange) ensureUpstreamPrepared() {
	x.reqOnce.Do(func() {
		work := x.origReq.Clone()
		coords := newSourceCoords(work)

		var pipelineLosses []ir.Loss
		carrierLosses, recalledSlots := openCarriers(work, coords, x.route, x.calls)
		pipelineLosses = append(pipelineLosses, carrierLosses...)

		markerLosses := stripClientMarkers(work, coords, x.route)
		pipelineLosses = append(pipelineLosses, markerLosses...)

		tm, toolLosses, err := lowerTools(work, coords, x.route, x.inheritedMap)
		if err != nil {
			x.upstreamErr = err
			return
		}
		x.toolMap = tm
		pipelineLosses = append(pipelineLosses, toolLosses...)

		tokenLosses := selectReplayTokens(work, coords, x.route, recalledSlots)
		pipelineLosses = append(pipelineLosses, tokenLosses...)

		dialectLosses, err := applyDialectRequestAdjustments(work, coords, x.route)
		if err != nil {
			x.upstreamErr = err
			return
		}
		pipelineLosses = append(pipelineLosses, dialectLosses...)

		ir.Link(work)

		encoded, encLosses, err := x.upCodec.EncodeRequest(work, x.route.Dialect)
		if err != nil {
			x.upstreamErr = err
			return
		}

		combined := remapEncoderLosses(pipelineLosses, encLosses, coords)
		x.upstreamBytes = encoded
		x.mu.Lock()
		x.recordedLosses = combined
		x.mu.Unlock()
	})
}

// appendLosses appends additional response or buffer losses under x.mu.
func (x *Exchange) appendLosses(extra []ir.Loss) {
	if len(extra) == 0 {
		return
	}
	x.mu.Lock()
	x.recordedLosses = append(x.recordedLosses, extra...)
	x.mu.Unlock()
}

// clientView constructs the ClientView passed to response and stream encoders.
func (x *Exchange) clientView() ClientView {
	return ClientView{
		Model:   x.clientModel,
		Request: x.origReq,
		Runtime: x.runtime,
	}
}

// isResponsesShapedChatBody reports whether body is a JSON object containing a
// top-level "input" member without a top-level "messages" member.
func isResponsesShapedChatBody(body []byte) bool {
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return false
	}
	sawMessages := false
	sawInput := false
	for dec.PeekKind() != '}' {
		kTok, err := dec.ReadToken()
		if err != nil {
			return false
		}
		switch kTok.String() {
		case "messages":
			sawMessages = true
		case "input":
			sawInput = true
		}
		if err := dec.SkipValue(); err != nil {
			return false
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return false
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return false
	}
	return !sawMessages && sawInput
}

// extractAndStripCursorChatControls extracts Chat-only stream_options.include_usage,
// reasoning_effort, and stop fields from clientBody and strips consumed members
// from the returned JSON bytes before Responses request decoding runs. Malformed
// JSON objects fall back to returning clientBody untouched so DecodeRequest
// surfaces its standard syntax or duplicate-key error.
func extractAndStripCursorChatControls(clientBody []byte) ([]byte, cursorChatControls, error) {
	members, err := parseOrderedObject(clientBody)
	if err != nil {
		return clientBody, cursorChatControls{}, nil
	}

	var out cursorChatControls
	changed := false

	if rawSO, ok := findRawMember(members, "stream_options"); ok {
		trimmedSO := bytes.TrimSpace(rawSO)
		switch {
		case bytes.Equal(trimmedSO, []byte("null")):
			members = deleteRawMember(members, "stream_options")
			changed = true
		case len(trimmedSO) > 0 && trimmedSO[0] == '{':
			if soMembers, soErr := parseOrderedObject(trimmedSO); soErr == nil {
				if inc := readMemberBoolPtr(soMembers, "include_usage"); inc != nil {
					out.includeUsage = inc
				}
				if _, hasInc := findRawMember(soMembers, "include_usage"); hasInc {
					soMembers = deleteRawMember(soMembers, "include_usage")
					if len(soMembers) == 0 {
						members = deleteRawMember(members, "stream_options")
					} else {
						rebuiltSO, encErr := encodeOrderedObject(soMembers)
						if encErr != nil {
							return nil, cursorChatControls{}, encErr
						}
						members = setRawMember(members, "stream_options", rebuiltSO)
					}
					changed = true
				}
			}
		}
	}

	if rawEff, ok := findRawMember(members, "reasoning_effort"); ok {
		trimmedEff := bytes.TrimSpace(rawEff)
		switch {
		case bytes.Equal(trimmedEff, []byte("null")):
			members = deleteRawMember(members, "reasoning_effort")
			changed = true
		case len(trimmedEff) > 0 && trimmedEff[0] == '"':
			var effStr string
			if err := unmarshalPermissive(trimmedEff, &effStr); err != nil {
				return nil, cursorChatControls{}, err
			}
			effStr = strings.TrimSpace(effStr)
			if norm := protocol.NormalizeReasoningEffort(effStr); norm != "" {
				effStr = norm
			}
			out.reasoningEffort = effStr
			members = deleteRawMember(members, "reasoning_effort")
			changed = true
		}
	}

	if rawStop, ok := findRawMember(members, "stop"); ok {
		stops, stopErr := parseCursorChatStop(rawStop)
		if stopErr != nil {
			return nil, cursorChatControls{}, stopErr
		}
		out.stop = stops
		members = deleteRawMember(members, "stop")
		changed = true
	}

	if !changed {
		return clientBody, out, nil
	}
	rebuilt, err := encodeOrderedObject(members)
	if err != nil {
		return nil, cursorChatControls{}, err
	}
	return rebuilt, out, nil
}

// parseCursorChatStop parses a Chat Completions scalar string, string array, or
// null stop field into a normalized string slice.
func parseCursorChatStop(raw jsontext.Value) ([]string, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, nil
	}
	if trimmed[0] == '"' {
		var single string
		if err := unmarshalPermissive(trimmed, &single); err != nil {
			return nil, err
		}
		return []string{single}, nil
	}
	var list []string
	if err := unmarshalPermissive(trimmed, &list); err != nil {
		return nil, err
	}
	if len(list) == 0 {
		return nil, nil
	}
	return list, nil
}

// applyCursorChatControls merges extracted Chat Completions controls onto a
// Responses-decoded ir.Request, giving nested reasoning.effort precedence over
// top-level reasoning_effort to match Chat Completions decoding rules.
func applyCursorChatControls(decoded *ir.Request, controls cursorChatControls) {
	if decoded == nil {
		return
	}
	if decoded.IncludeUsage == nil && controls.includeUsage != nil {
		v := *controls.includeUsage
		decoded.IncludeUsage = &v
	}
	if (decoded.Reasoning == nil || decoded.Reasoning.Effort == "") && controls.reasoningEffort != "" {
		if decoded.Reasoning == nil {
			decoded.Reasoning = &ir.ReasoningConfig{}
		}
		decoded.Reasoning.Effort = controls.reasoningEffort
	}
	if len(decoded.Stop) == 0 && len(controls.stop) > 0 {
		decoded.Stop = slices.Clone(controls.stop)
	}
}
