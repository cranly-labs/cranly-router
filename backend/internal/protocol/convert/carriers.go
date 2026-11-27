package convert

import (
	"fmt"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// carriers.go manages cross-protocol reasoning continuity envelopes ("trc1:"),
// stateful upstream tool-call bindings, and non-Gemini client response part
// expansions across request and response stages. Upstream providers issue
// scheme-specific cryptographic thinking signatures, plain-text replay
// signatures, generated inlineData images, and account-bound ciphertexts that
// foreign client or upstream protocols cannot carry directly on matching wire fields.
//
// Request-side openCarriers unpacks gateway envelopes and recalls CallMemory
// bindings before selectReplayTokens retains matching tokens, unpacks plain
// reasoning text under the target HistoryMode, or drops incompatible tokens
// with LossReplayTokenDropped. Response-side sealResponseCarriers and
// sealEvents render model-generated base64 images as Markdown data URLs, split
// text-attached replay signatures into trailing signed reasoning blocks, record
// call tokens into CallMemory, and wrap reasoning tokens into trc1 envelopes.

// CallMemory stores upstream tool-call identifiers and call-bound replay tokens
// that client wire protocols cannot carry inside assistant tool_call items.
type CallMemory interface {
	// Remember records the upstream call identifier and optional replay token in
	// binding under clientCallID. A nil binding.Token marks an observed call that
	// carried no upstream signature so later turns skip placeholder injection.
	Remember(clientCallID string, binding CallBinding)
	// Recall looks up a previously recorded upstream CallBinding for clientCallID.
	// It returns (CallBinding{}, false) when clientCallID was not observed in the
	// current scoped session or has already expired.
	Recall(clientCallID string) (CallBinding, bool)
}

// CallBinding holds the upstream call identifier and optional call-bound replay
// token associated with one client-visible tool call ID.
type CallBinding struct {
	UpstreamID string          // original upstream call ID, or empty when upstream omitted IDs
	Token      *ir.ReplayToken // call-bound replay token, or nil when observed unsigned
}

// streamCarrierSealer tracks synthesized client call IDs, cross-protocol inline
// media/signature expansions, and part index mappings across EventPartStart and
// EventPartEnd for one streaming response session.
type streamCarrierSealer struct {
	route             Route          // active client-to-upstream conversion route
	calls             CallMemory     // optional stateful call binding store
	runtime           Runtime        // response-side identifier generator
	callIDs           map[int]string // source part index -> synthesized client CallID
	sealedCallIDs     map[int]string // closed source part index -> emitted client CallID
	sealedUpstreamIDs map[int]string // closed source part index -> original upstream ID
	origToOutIdx      map[int]int    // source part index -> output part index
	outPartToOrig     []int          // output part index -> source part index (-1 for synthesized)
	nextOutIdx        int            // next sequential output part index
	activeOrigIdx     int            // currently open source part index, or -1
	activeOutIdx      int            // currently open output part index, or -1
	activeInlineImg   bool           // true when open PartMedia renders as Markdown data URL text
	// deferredText and deferredTextOrig buffer a metadata-free empty PartText
	// start on foreign non-Gemini routes until non-empty text arrives or
	// EventPartEnd resolves signature-only suppression; deferredTextOrig is -1 when inactive.
	deferredText     *ir.Part // buffered empty PartText start skeleton
	deferredTextOrig int      // source part index of deferredText, or -1
	callSeq          int      // monotonic fallback counter for synthesized CallIDs
}

// newStreamCarrierSealer constructs a per-stream sealer for route, calls, and runtime.
func newStreamCarrierSealer(route Route, calls CallMemory, runtime Runtime) *streamCarrierSealer {
	return &streamCarrierSealer{
		route:             route,
		calls:             calls,
		runtime:           runtime,
		callIDs:           make(map[int]string),
		sealedCallIDs:     make(map[int]string),
		sealedUpstreamIDs: make(map[int]string),
		origToOutIdx:      make(map[int]int),
		activeOrigIdx:     -1,
		activeOutIdx:      -1,
		deferredTextOrig:  -1,
	}
}

// openCarriers unpacks trc1 continuity envelopes on historical reasoning,
// assistant text, and tool_call parts inside req, classifies bare non-envelope
// tokens as SchemeUnknown, and recalls upstream call IDs and call tokens from
// calls. Returned recalledSlots marks [msgIdx][partIdx] positions whose
// Call.Token came from CallMemory rather than incoming wire JSON.
//
// When CallMemory recalls an original UpstreamID that differs from the client's
// CallID under a matching active replay scheme, correlated downstream
// ToolResult.CallID references rewrite in lockstep and report LossIDRewritten
// against original source coordinates. Cross-protocol requests targeting
// standard Gemini omit client-originated wire call IDs unless CallMemory
// recalls a non-empty Gemini UpstreamID, whereas SchemaAntigravity preserves
// client call IDs across claude-* and gemini-* families when CallMemory misses
// or records an empty UpstreamID so v1internal tool pairing stays intact.
func openCarriers(
	req *ir.Request,
	coords *sourceCoords,
	route Route,
	calls CallMemory,
) ([]ir.Loss, map[int]map[int]bool) {
	if req == nil {
		return nil, nil
	}

	crossGemini := route.Client != protocol.ProtocolGeminiGenerateContent &&
		route.Upstream == protocol.ProtocolGeminiGenerateContent &&
		route.Dialect.SchemaDialect != ir.SchemaAntigravity

	var losses []ir.Loss
	var recalledSlots map[int]map[int]bool
	omitSeq := 0

	for m := range req.Messages {
		var seenOmittedInMsg map[string]string
		for p := range req.Messages[m].Parts {
			part := &req.Messages[m].Parts[p]
			switch part.Kind {
			case ir.PartText:
				if part.Token != nil {
					unpackTextCarrier(part)
				}

			case ir.PartReasoning:
				if part.Reasoning != nil {
					unpackReasoningCarrier(part.Reasoning)
				}

			case ir.PartToolCall:
				if part.Call == nil {
					continue
				}
				unpackCallCarrier(part.Call)

				var binding CallBinding
				var recalled bool
				if calls != nil && part.Call.CallID != "" {
					binding, recalled = calls.Recall(part.Call.CallID)
				}
				bindingMatches := recalled && callBindingMatchesRoute(binding, route.Dialect)
				if bindingMatches && part.Call.Token == nil {
					if binding.Token != nil && binding.Token.Data != "" {
						cp := *binding.Token
						part.Call.Token = &cp
						if recalledSlots == nil {
							recalledSlots = make(map[int]map[int]bool)
						}
						if recalledSlots[m] == nil {
							recalledSlots[m] = make(map[int]bool)
						}
						recalledSlots[m][p] = true
					} else {
						part.Call.Unsigned = true
					}
				}

				if crossGemini {
					// Preserve FunctionCall.id on cross-protocol Gemini requests only
					// when CallMemory confirms a non-empty Gemini UpstreamID under an
					// active Gemini replay scheme; otherwise tag with a gemcall_ sentinel
					// so generatecontent omits client-invented wire IDs while keeping
					// invocation-order pairing intact.
					keepExplicitID := bindingMatches &&
						binding.UpstreamID != "" &&
						schemeAndProviderMatch(ir.SchemeGemini, 0, route.Dialect)
					if keepExplicitID {
						if binding.UpstreamID != part.Call.CallID {
							oldID := part.Call.CallID
							part.Call.CallID = binding.UpstreamID
							rewriteCorrelatedResultCallID(req.Messages, m, p, oldID, binding.UpstreamID)
							losses = append(losses, ir.Loss{
								Kind: ir.LossIDRewritten,
								Path: coords.origPartPath(m, p) + ".call_id",
							})
						}
					} else if !strings.HasPrefix(part.Call.CallID, "gemcall_") {
						oldID := part.Call.CallID
						placeholderID, dup := seenOmittedInMsg[oldID]
						if !dup || oldID == "" {
							omitSeq++
							placeholderID = fmt.Sprintf("gemcall_client_%d", omitSeq)
							if oldID != "" {
								if seenOmittedInMsg == nil {
									seenOmittedInMsg = make(map[string]string)
								}
								seenOmittedInMsg[oldID] = placeholderID
								rewriteCorrelatedResultCallID(req.Messages, m+1, -1, oldID, placeholderID)
							}
						}
						part.Call.CallID = placeholderID
					}
					continue
				}

				// Under SchemaAntigravity (and non-Gemini upstreams), preserve client
				// call IDs when CallMemory misses or records an empty UpstreamID, and
				// restore a distinct UpstreamID only when the binding matches route.Dialect.
				if bindingMatches && binding.UpstreamID != "" && binding.UpstreamID != part.Call.CallID {
					oldID := part.Call.CallID
					part.Call.CallID = binding.UpstreamID
					rewriteCorrelatedResultCallID(req.Messages, m, p, oldID, binding.UpstreamID)
					losses = append(losses, ir.Loss{
						Kind: ir.LossIDRewritten,
						Path: coords.origPartPath(m, p) + ".call_id",
					})
				}
			}
		}
	}
	return losses, recalledSlots
}

// callBindingMatchesRoute reports whether a recalled CallBinding applies to
// dialect. Routes with an empty ReplayScheme ignore CallMemory state for
// count_tokens and compatibility retries, and signed bindings must match
// dialect's ReplayScheme and account-bound ProviderID rules.
func callBindingMatchesRoute(binding CallBinding, dialect Dialect) bool {
	if dialect.ReplayScheme == "" {
		return false
	}
	if binding.Token != nil && binding.Token.Scheme != "" {
		return schemeAndProviderMatch(binding.Token.Scheme, binding.Token.ProviderID, dialect)
	}
	return true
}

// unpackReasoningCarrier parses r.Token.Data through ir.ParseCarrier, restoring
// exact signed Anthropic text or plain reasoning text onto r.Segments and
// marking non-envelope strings as ir.SchemeUnknown.
func unpackReasoningCarrier(r *ir.Reasoning) {
	if r.Token == nil || r.Token.Data == "" {
		r.Token = nil
		return
	}
	carrier, ok := ir.ParseCarrier(r.Token.Data)
	if !ok {
		r.Token = &ir.ReplayToken{
			Scheme: ir.SchemeUnknown,
			Data:   r.Token.Data,
		}
		return
	}

	r.Redacted = r.Redacted || carrier.Redacted
	switch carrier.Scheme {
	case ir.SchemePlain:
		if strings.Join(r.Segments, "") == "" && carrier.Text != "" {
			r.Segments = []string{carrier.Text}
		}
		r.Token = &ir.ReplayToken{Scheme: ir.SchemePlain}

	case ir.SchemeAnthropic, ir.SchemeAntigravityClaude:
		if !r.Redacted && carrier.Text != "" {
			// Anthropic thinking signatures validate exact original thinking text
			// rather than abbreviated client summaries returned on later turns.
			r.Segments = []string{carrier.Text}
		}
		r.Token = &ir.ReplayToken{
			Scheme:     carrier.Scheme,
			ProviderID: carrier.ProviderID,
			Data:       carrier.Data,
		}

	default:
		r.Token = &ir.ReplayToken{
			Scheme:     carrier.Scheme,
			ProviderID: carrier.ProviderID,
			Data:       carrier.Data,
		}
	}
}

// unpackCallCarrier parses call.Token.Data as a trc1 carrier or marks bare strings as SchemeUnknown.
func unpackCallCarrier(call *ir.ToolCall) {
	if call.Token == nil || call.Token.Data == "" {
		call.Token = nil
		return
	}
	if carrier, ok := ir.ParseCarrier(call.Token.Data); ok {
		if carrier.Scheme == ir.SchemePlain {
			call.Token = nil
			return
		}
		call.Token = &ir.ReplayToken{
			Scheme:     carrier.Scheme,
			ProviderID: carrier.ProviderID,
			Data:       carrier.Data,
		}
		return
	}
	call.Token = &ir.ReplayToken{
		Scheme: ir.SchemeUnknown,
		Data:   call.Token.Data,
	}
}

// unpackTextCarrier parses p.Token.Data on an assistant text part through
// ir.ParseCarrier in place. Plain carriers and blank token strings clear
// p.Token to nil, whereas non-envelope wire signatures receive ir.SchemeUnknown.
func unpackTextCarrier(p *ir.Part) {
	if p.Token == nil || p.Token.Data == "" {
		p.Token = nil
		return
	}
	if carrier, ok := ir.ParseCarrier(p.Token.Data); ok {
		if carrier.Scheme == ir.SchemePlain {
			p.Token = nil
			return
		}
		p.Token = &ir.ReplayToken{
			Scheme:     carrier.Scheme,
			ProviderID: carrier.ProviderID,
			Data:       carrier.Data,
		}
		return
	}
	p.Token = &ir.ReplayToken{
		Scheme: ir.SchemeUnknown,
		Data:   p.Token.Data,
	}
}

// rewriteCorrelatedResultCallID updates matching ToolResult.CallID fields
// following (startMsg, startPart) until another call reuses oldID.
func rewriteCorrelatedResultCallID(msgs []ir.Message, startMsg, startPart int, oldID, newID string) {
	if oldID == "" || oldID == newID {
		return
	}
	for m := startMsg; m < len(msgs); m++ {
		pStart := 0
		if m == startMsg {
			pStart = startPart + 1
		}
		for p := pStart; p < len(msgs[m].Parts); p++ {
			part := &msgs[m].Parts[p]
			if part.Call != nil && part.Call.CallID == oldID {
				return
			}
			if part.Result != nil && part.Result.CallID == oldID {
				part.Result.CallID = newID
			}
		}
	}
}

// selectReplayTokens evaluates unpacked reasoning, assistant text, and
// tool_call tokens against route, retaining matching issuer tokens and clearing
// plain token markers so target encoders apply HistoryMode. Token-only
// reasoning blocks emptied by foreign or cross-account stripping are removed
// while updating coords and reporting LossReplayTokenDropped against original
// client coordinates.
//
// Tokens recalled from CallMemory that do not match the current target dialect
// clear silently without reporting LossReplayTokenDropped because the incoming
// client wire request never carried those server-side bindings.
func selectReplayTokens(
	req *ir.Request,
	coords *sourceCoords,
	route Route,
	recalledSlots map[int]map[int]bool,
) []ir.Loss {
	if req == nil || len(req.Messages) == 0 {
		return nil
	}

	sameProtocol := route.Client == route.Upstream
	var losses []ir.Loss
	keptMsgs := make([]ir.Message, 0, len(req.Messages))
	var keptMsgIdx []int
	var keptPartIdx [][]int

	for m, msg := range req.Messages {
		origEmpty := len(msg.Parts) == 0
		keptParts := make([]ir.Part, 0, len(msg.Parts))
		var pIndices []int

		for p, part := range msg.Parts {
			origPath := coords.origPartPath(m, p)
			switch part.Kind {
			case ir.PartToolCall:
				if part.Call != nil && part.Call.Token != nil {
					if tokenMatchesRoute(*part.Call.Token, route.Dialect, sameProtocol) {
						part.Call.Token = &ir.ReplayToken{Data: part.Call.Token.Data}
					} else {
						hadData := part.Call.Token.Data != ""
						part.Call.Token = nil
						if hadData && !isRecalledSlot(recalledSlots, m, p) {
							losses = append(losses, ir.Loss{
								Kind: ir.LossReplayTokenDropped,
								Path: origPath,
							})
						}
					}
				}
				keptParts = append(keptParts, part)
				if coords != nil && m < len(coords.partIdx) && p < len(coords.partIdx[m]) {
					pIndices = append(pIndices, coords.partIdx[m][p])
				}

			case ir.PartReasoning:
				if part.Reasoning == nil {
					continue
				}
				droppedToken := false
				if tok := part.Reasoning.Token; tok != nil {
					switch {
					case tok.Scheme == ir.SchemePlain:
						part.Reasoning.Token = nil

					case tok.Data != "" && tokenMatchesRoute(*tok, route.Dialect, sameProtocol):
						part.Reasoning.Token = &ir.ReplayToken{Data: tok.Data}

					default:
						part.Reasoning.Token = nil
						part.Reasoning.Redacted = false
						if tok.Data != "" {
							droppedToken = true
							losses = append(losses, ir.Loss{
								Kind: ir.LossReplayTokenDropped,
								Path: origPath,
							})
						}
					}
				}

				joined := strings.Join(part.Reasoning.Segments, "")
				if part.Reasoning.Token == nil && joined == "" && !part.Reasoning.Redacted {
					if droppedToken {
						// Omit token-only reasoning items whose foreign or bare unknown token
						// was stripped so target codecs do not serialize empty shells.
						continue
					}
					if !sameProtocol {
						losses = append(losses, ir.Loss{
							Kind: ir.LossReasoningTextDropped,
							Path: origPath,
						})
						continue
					}
				}

				keptParts = append(keptParts, part)
				if coords != nil && m < len(coords.partIdx) && p < len(coords.partIdx[m]) {
					pIndices = append(pIndices, coords.partIdx[m][p])
				}

			case ir.PartText:
				if part.Token != nil {
					if part.Token.Data != "" &&
						route.Upstream == protocol.ProtocolGeminiGenerateContent &&
						tokenMatchesRoute(*part.Token, route.Dialect, sameProtocol) {
						part.Token = &ir.ReplayToken{Data: part.Token.Data}
					} else {
						hadData := part.Token.Data != ""
						part.Token = nil
						if hadData {
							losses = append(losses, ir.Loss{
								Kind: ir.LossReplayTokenDropped,
								Path: origPath,
							})
						}
						if part.Text == "" && len(part.Annotations) == 0 && part.Cache == nil && part.Extra.IsZero() {
							continue
						}
					}
				}
				keptParts = append(keptParts, part)
				if coords != nil && m < len(coords.partIdx) && p < len(coords.partIdx[m]) {
					pIndices = append(pIndices, coords.partIdx[m][p])
				}

			default:
				keptParts = append(keptParts, part)
				if coords != nil && m < len(coords.partIdx) && p < len(coords.partIdx[m]) {
					pIndices = append(pIndices, coords.partIdx[m][p])
				}
			}
		}

		if len(keptParts) == 0 && !origEmpty {
			continue
		}
		msg.Parts = keptParts
		keptMsgs = append(keptMsgs, msg)
		if coords != nil && m < len(coords.msgIdx) {
			keptMsgIdx = append(keptMsgIdx, coords.msgIdx[m])
			keptPartIdx = append(keptPartIdx, pIndices)
		}
	}

	req.Messages = keptMsgs
	if coords != nil {
		coords.msgIdx = keptMsgIdx
		coords.partIdx = keptPartIdx
	}
	return losses
}

// isRecalledSlot reports whether recalledSlots marks [m][p] as populated from CallMemory.
func isRecalledSlot(recalledSlots map[int]map[int]bool, m, p int) bool {
	if recalledSlots == nil {
		return false
	}
	return recalledSlots[m][p]
}

// tokenMatchesRoute reports whether tok may be written back as a bare wire
// token under dialect and sameProtocol.
func tokenMatchesRoute(tok ir.ReplayToken, dialect Dialect, sameProtocol bool) bool {
	if tok.Scheme == ir.SchemeUnknown {
		return sameProtocol
	}
	return schemeAndProviderMatch(tok.Scheme, tok.ProviderID, dialect)
}

// schemeAndProviderMatch reports whether scheme and providerID match dialect's
// ReplayScheme and account-bound ProviderID rules.
func schemeAndProviderMatch(scheme string, providerID int64, dialect Dialect) bool {
	if scheme == "" || dialect.ReplayScheme == "" || scheme != dialect.ReplayScheme {
		return false
	}
	if ir.IsAccountBoundScheme(scheme) && providerID > 0 && dialect.ProviderID > 0 {
		return providerID == dialect.ProviderID
	}
	return true
}

// tagUpstreamResponseTokens stamps dialect.ReplayScheme and account-bound
// ProviderID onto bare upstream reasoning, text, and tool-call tokens in resp.
func tagUpstreamResponseTokens(resp *ir.Response, dialect Dialect) {
	if resp == nil {
		return
	}
	for i := range resp.Parts {
		tagPartUpstreamTokens(&resp.Parts[i], dialect)
	}
}

// tagUpstreamEventTokens stamps dialect.ReplayScheme and account-bound
// ProviderID onto bare upstream tokens inside events.
func tagUpstreamEventTokens(events []ir.Event, dialect Dialect) {
	for i := range events {
		if events[i].Part != nil {
			tagPartUpstreamTokens(events[i].Part, dialect)
		}
	}
}

// tagPartUpstreamTokens stamps dialect.ReplayScheme and ProviderID onto p's tokens.
func tagPartUpstreamTokens(p *ir.Part, dialect Dialect) {
	pid := int64(0)
	if ir.IsAccountBoundScheme(dialect.ReplayScheme) && dialect.ProviderID > 0 {
		pid = dialect.ProviderID
	}
	if p.Token != nil && p.Token.Data != "" {
		p.Token.Scheme = dialect.ReplayScheme
		p.Token.ProviderID = pid
	}
	if p.Reasoning != nil && p.Reasoning.Token != nil && p.Reasoning.Token.Data != "" {
		p.Reasoning.Token.Scheme = dialect.ReplayScheme
		p.Reasoning.Token.ProviderID = pid
	}
	if p.Call != nil && p.Call.Token != nil && p.Call.Token.Data != "" {
		p.Call.Token.Scheme = dialect.ReplayScheme
		p.Call.Token.ProviderID = pid
	}
}

// rendersForeignResponseParts reports whether route targets a non-Gemini client
// protocol that cannot represent assistant PartMedia or text-attached
// thoughtSignature fields natively.
func rendersForeignResponseParts(route Route) bool {
	return route.Client != route.Upstream &&
		route.Client != protocol.ProtocolGeminiGenerateContent
}

// isInlineBase64ImageStart reports whether m opens a base64-encoded image part
// with a non-empty MIME type suitable for Markdown data URL rendering.
func isInlineBase64ImageStart(m *ir.Media) bool {
	return m != nil &&
		m.Type == ir.MediaImage &&
		m.Source == ir.MediaSourceBase64 &&
		strings.TrimSpace(m.MIME) != ""
}

// isInlineBase64ImageComplete reports whether m carries a complete base64 image
// payload ready to render as a client-visible Markdown data URL.
func isInlineBase64ImageComplete(m *ir.Media) bool {
	return isInlineBase64ImageStart(m) && strings.TrimSpace(m.Data) != ""
}

// formatInlineImageMarkdown formats m as a Markdown image with an inline base64
// data URL for non-Gemini client protocols.
func formatInlineImageMarkdown(m ir.Media) string {
	return fmt.Sprintf("![image](data:%s;base64,%s)", m.MIME, m.Data)
}

// isMetadataFreeEmptyText reports whether p is an empty PartText carrying no
// wire ID, annotations, cache directive, or Extra members after Token removal.
func isMetadataFreeEmptyText(p ir.Part) bool {
	return p.Kind == ir.PartText &&
		p.ID == "" &&
		p.Text == "" &&
		len(p.Annotations) == 0 &&
		p.Cache == nil &&
		p.Extra.IsZero()
}

// sealResponseCarriers records tool-call identities into calls, renders
// cross-protocol inline base64 images as Markdown data URLs, preserves
// text-attached replay signatures in trailing reasoning blocks, and wraps
// cross-protocol reasoning tokens into trc1 carrier envelopes. Nil responses
// return (nil, nil) immediately, while non-nil responses return the
// output-to-source part index table for remapping downstream encoder losses.
//
// Metadata-free empty text parts carrying a replay token omit the empty PartText
// from client output so signature-only Gemini parts produce only the signed
// reasoning carrier or LossReplayTokenDropped. Endpoints whose
// Dialect.ReplayScheme is empty drop bare upstream tokens before sealing, and
// Chat clients drop text-attached and reasoning tokens because Chat Completions
// exposes no opaque reasoning field.
func sealResponseCarriers(
	resp *ir.Response,
	route Route,
	calls CallMemory,
	runtime Runtime,
	losses *[]ir.Loss,
) ([]int, error) {
	if resp == nil {
		return nil, nil
	}
	foreignNonGemini := rendersForeignResponseParts(route)
	outParts := make([]ir.Part, 0, len(resp.Parts))
	outPartToOrig := make([]int, 0, len(resp.Parts))
	callSeq := 0

	for origIdx, p := range resp.Parts {
		origPath := fmt.Sprintf("parts[%d]", origIdx)
		if foreignNonGemini && p.Kind == ir.PartMedia && isInlineBase64ImageComplete(p.Media) {
			// Render model-generated base64 inlineData images as visible Markdown
			// data URLs for Messages, Responses, and Chat clients lacking assistant
			// media blocks, recording markdown_data_url for Essence verification.
			*losses = append(*losses, ir.Loss{
				Kind:   ir.LossMediaUnsupported,
				Path:   origPath,
				Detail: "markdown_data_url",
			})
			outParts = append(outParts, ir.Part{
				Kind:  ir.PartText,
				ID:    p.ID,
				Text:  formatInlineImageMarkdown(*p.Media),
				Cache: p.Cache,
				Extra: p.Extra,
			})
			outPartToOrig = append(outPartToOrig, origIdx)
			continue
		}

		switch p.Kind {
		case ir.PartToolCall:
			if p.Call != nil && p.Call.Tool.Kind != ir.ToolHosted {
				callSeq++
				rememberResponseCall(p.Call, route, calls, runtime, callSeq, origPath, losses)
			}
			outParts = append(outParts, p)
			outPartToOrig = append(outPartToOrig, origIdx)

		case ir.PartReasoning:
			if p.Reasoning != nil {
				if err := sealReasoningPart(p.Reasoning, route, origPath, losses); err != nil {
					return nil, err
				}
			}
			outParts = append(outParts, p)
			outPartToOrig = append(outPartToOrig, origIdx)

		case ir.PartText:
			if foreignNonGemini && p.Token != nil && p.Token.Data != "" {
				tok := p.Token
				p.Token = nil
				synthOrig := origIdx
				if !isMetadataFreeEmptyText(p) {
					outParts = append(outParts, p)
					outPartToOrig = append(outPartToOrig, origIdx)
					synthOrig = -1
				}
				// Preserve non-thought text thoughtSignatures by emitting a trailing
				// empty signed reasoning carrier on Messages and Responses while
				// dropping tokens cleanly on Chat or unschemed endpoints.
				if !ir.IsCarrierScheme(route.Dialect.ReplayScheme) ||
					route.Client == protocol.ProtocolOpenAIChatCompletions {
					*losses = append(*losses, ir.Loss{
						Kind: ir.LossReplayTokenDropped,
						Path: origPath,
					})
					continue
				}
				synthReasoning := ir.Reasoning{
					Summary: true,
					Token:   tok,
				}
				if err := sealReasoningPart(&synthReasoning, route, origPath, losses); err != nil {
					return nil, err
				}
				outParts = append(outParts, ir.Part{
					Kind:      ir.PartReasoning,
					Reasoning: &synthReasoning,
				})
				outPartToOrig = append(outPartToOrig, synthOrig)
				continue
			}
			outParts = append(outParts, p)
			outPartToOrig = append(outPartToOrig, origIdx)

		default:
			outParts = append(outParts, p)
			outPartToOrig = append(outPartToOrig, origIdx)
		}
	}

	resp.Parts = outParts
	return outPartToOrig, nil
}

// openDeferredText allocates the next sequential output slot for s.deferredText,
// appends its EventPartStart onto out, and activates s.activeOutIdx for
// subsequent deltas. Calling openDeferredText when s.deferredText is nil is a
// no-op that leaves stream indices unchanged.
func (s *streamCarrierSealer) openDeferredText(out *[]ir.Event) {
	if s.deferredText == nil {
		return
	}
	origIdx := s.deferredTextOrig
	startPart := s.deferredText.Clone()
	s.deferredText = nil
	s.deferredTextOrig = -1

	outIdx := s.nextOutIdx
	s.nextOutIdx++
	s.activeOrigIdx = origIdx
	s.activeOutIdx = outIdx
	s.activeInlineImg = false
	s.origToOutIdx[origIdx] = outIdx
	s.outPartToOrig = append(s.outPartToOrig, origIdx)
	*out = append(*out, ir.Event{
		Kind:  ir.EventPartStart,
		Index: outIdx,
		Part:  &startPart,
	})
}

// sealEvents transforms sequential upstream IR events for s.route, synthesizing
// missing client CallIDs when CallMemory is configured, recording upstream
// CallBindings on EventPartEnd, and sealing cross-protocol reasoning tokens
// into trc1 envelopes. It returns the re-indexed event slice alongside any
// carrier formatting error while appending incurred losses onto losses.
//
// On non-Gemini cross-protocol client targets, base64 image parts convert into
// streamed Markdown data URL text deltas with LossMediaUnsupported, and
// metadata-free empty PartText skeletons defer EventPartStart until non-empty
// text arrives so signature-only text parts emit only the synthesized signed
// reasoning lifecycle (or LossReplayTokenDropped on Chat and unschemed routes)
// while s.outPartToOrig preserves original part coordinates for encoder loss remapping.
func (s *streamCarrierSealer) sealEvents(events []ir.Event, losses *[]ir.Loss) ([]ir.Event, error) {
	foreignNonGemini := rendersForeignResponseParts(s.route)
	out := make([]ir.Event, 0, len(events))

	for i := range events {
		ev := events[i]
		switch ev.Kind {
		case ir.EventPartStart:
			if s.deferredText != nil {
				s.openDeferredText(&out)
			}
			origIdx := ev.Index
			origPath := fmt.Sprintf("parts[%d]", origIdx)
			if foreignNonGemini && ev.Part != nil && ev.Part.Kind == ir.PartText {
				cp := ev.Part.Clone()
				cp.Token = nil
				if isMetadataFreeEmptyText(cp) {
					s.deferredText = &cp
					s.deferredTextOrig = origIdx
					s.activeOrigIdx = -1
					s.activeOutIdx = -1
					s.activeInlineImg = false
					continue
				}
				ev.Part = &cp
			}

			outIdx := s.nextOutIdx
			s.nextOutIdx++
			s.activeOrigIdx = origIdx
			s.activeOutIdx = outIdx
			s.activeInlineImg = false
			s.origToOutIdx[origIdx] = outIdx
			s.outPartToOrig = append(s.outPartToOrig, origIdx)

			if ev.Part != nil {
				if foreignNonGemini && ev.Part.Kind == ir.PartMedia && isInlineBase64ImageStart(ev.Part.Media) {
					s.activeInlineImg = true
					startPart := ir.Part{
						Kind:  ir.PartText,
						ID:    ev.Part.ID,
						Cache: ev.Part.Cache,
						Extra: ev.Part.Extra,
					}
					out = append(out, ir.Event{
						Kind:  ir.EventPartStart,
						Index: outIdx,
						Part:  &startPart,
					})
					continue
				}
				if ev.Part.Kind == ir.PartToolCall && ev.Part.Call != nil && ev.Part.Call.Tool.Kind != ir.ToolHosted && s.calls != nil {
					if ev.Part.Call.CallID == "" {
						s.callSeq++
						prefix := clientCallIDPrefix(s.route.Client)
						synth := s.runtime.NextID(prefix, fmt.Sprintf("%s%d", prefix, s.callSeq))
						s.callIDs[origIdx] = synth
						ev.Part.Call.CallID = synth
						*losses = append(*losses, ir.Loss{
							Kind: ir.LossIDRewritten,
							Path: origPath + ".call_id",
						})
					}
				}
			}
			ev.Index = outIdx
			out = append(out, ev)

		case ir.EventPartDelta:
			if s.deferredText != nil {
				if ev.Delta == nil || ev.Delta.Text == "" {
					continue
				}
				s.openDeferredText(&out)
			}
			if s.activeOutIdx >= 0 {
				ev.Index = s.activeOutIdx
			}
			out = append(out, ev)

		case ir.EventPartEnd:
			origIdx := ev.Index
			origPath := fmt.Sprintf("parts[%d]", origIdx)
			if s.deferredText != nil && origIdx == s.deferredTextOrig {
				if ev.Part != nil && ev.Part.Kind == ir.PartText && ev.Part.Token != nil && ev.Part.Token.Data != "" {
					tok := ev.Part.Token
					cp := ev.Part.Clone()
					cp.Token = nil
					synthOrig := origIdx
					if !isMetadataFreeEmptyText(cp) {
						s.openDeferredText(&out)
						endIdx := s.activeOutIdx
						s.activeOrigIdx = -1
						s.activeOutIdx = -1
						out = append(out, ir.Event{
							Kind:  ir.EventPartEnd,
							Index: endIdx,
							Part:  &cp,
						})
						synthOrig = -1
					} else {
						s.deferredText = nil
						s.deferredTextOrig = -1
					}
					if !ir.IsCarrierScheme(s.route.Dialect.ReplayScheme) ||
						s.route.Client == protocol.ProtocolOpenAIChatCompletions {
						*losses = append(*losses, ir.Loss{
							Kind: ir.LossReplayTokenDropped,
							Path: origPath,
						})
						continue
					}
					synthIdx := s.nextOutIdx
					s.nextOutIdx++
					s.outPartToOrig = append(s.outPartToOrig, synthOrig)
					synthReasoning := ir.Reasoning{
						Summary: true,
						Token:   tok,
					}
					if err := sealReasoningPart(&synthReasoning, s.route, origPath, losses); err != nil {
						return nil, err
					}
					startSynth := ir.Part{
						Kind:      ir.PartReasoning,
						Reasoning: &ir.Reasoning{Summary: true},
					}
					endSynth := ir.Part{
						Kind:      ir.PartReasoning,
						Reasoning: &synthReasoning,
					}
					out = append(out,
						ir.Event{
							Kind:  ir.EventPartStart,
							Index: synthIdx,
							Part:  &startSynth,
						},
						ir.Event{
							Kind:  ir.EventPartEnd,
							Index: synthIdx,
							Part:  &endSynth,
						},
					)
					continue
				}
				// Unsigned or metadata-bearing deferred text parts materialize at
				// EventPartEnd so empty unsigned text behavior remains unchanged.
				s.openDeferredText(&out)
			}

			outIdx := s.activeOutIdx
			if outIdx < 0 {
				if mapped, ok := s.origToOutIdx[origIdx]; ok {
					outIdx = mapped
				} else {
					outIdx = origIdx
				}
			}
			inlineImg := s.activeInlineImg
			s.activeOrigIdx = -1
			s.activeOutIdx = -1
			s.activeInlineImg = false

			if inlineImg && ev.Part != nil && isInlineBase64ImageComplete(ev.Part.Media) {
				markdown := formatInlineImageMarkdown(*ev.Part.Media)
				*losses = append(*losses, ir.Loss{
					Kind:   ir.LossMediaUnsupported,
					Path:   origPath,
					Detail: "markdown_data_url",
				})
				endPart := ir.Part{
					Kind:  ir.PartText,
					ID:    ev.Part.ID,
					Text:  markdown,
					Cache: ev.Part.Cache,
					Extra: ev.Part.Extra,
				}
				out = append(out,
					ir.Event{
						Kind:  ir.EventPartDelta,
						Index: outIdx,
						Delta: &ir.Delta{Text: markdown},
					},
					ir.Event{
						Kind:  ir.EventPartEnd,
						Index: outIdx,
						Part:  &endPart,
					},
				)
				continue
			}

			if ev.Part != nil {
				switch ev.Part.Kind {
				case ir.PartToolCall:
					if ev.Part.Call != nil && ev.Part.Call.Tool.Kind != ir.ToolHosted && s.calls != nil {
						origUpstreamID := ev.Part.Call.CallID
						if synth, ok := s.callIDs[origIdx]; ok {
							origUpstreamID = ""
							ev.Part.Call.CallID = synth
							delete(s.callIDs, origIdx)
						} else if ev.Part.Call.CallID == "" {
							s.callSeq++
							prefix := clientCallIDPrefix(s.route.Client)
							ev.Part.Call.CallID = s.runtime.NextID(prefix, fmt.Sprintf("%s%d", prefix, s.callSeq))
							*losses = append(*losses, ir.Loss{
								Kind: ir.LossIDRewritten,
								Path: origPath + ".call_id",
							})
						}
						s.sealedCallIDs[origIdx] = ev.Part.Call.CallID
						s.sealedUpstreamIDs[origIdx] = origUpstreamID
						recordCallBinding(s.calls, ev.Part.Call, origUpstreamID, s.route.Dialect)
					}

				case ir.PartReasoning:
					if ev.Part.Reasoning != nil {
						if err := sealReasoningPart(ev.Part.Reasoning, s.route, origPath, losses); err != nil {
							return nil, err
						}
					}

				case ir.PartText:
					if foreignNonGemini && ev.Part.Token != nil && ev.Part.Token.Data != "" {
						tok := ev.Part.Token
						cp := ev.Part.Clone()
						cp.Token = nil
						ev.Part = &cp
						ev.Index = outIdx
						out = append(out, ev)
						if !ir.IsCarrierScheme(s.route.Dialect.ReplayScheme) ||
							s.route.Client == protocol.ProtocolOpenAIChatCompletions {
							*losses = append(*losses, ir.Loss{
								Kind: ir.LossReplayTokenDropped,
								Path: origPath,
							})
							continue
						}
						synthIdx := s.nextOutIdx
						s.nextOutIdx++
						s.outPartToOrig = append(s.outPartToOrig, -1)
						synthReasoning := ir.Reasoning{
							Summary: true,
							Token:   tok,
						}
						if err := sealReasoningPart(&synthReasoning, s.route, origPath, losses); err != nil {
							return nil, err
						}
						startSynth := ir.Part{
							Kind:      ir.PartReasoning,
							Reasoning: &ir.Reasoning{Summary: true},
						}
						endSynth := ir.Part{
							Kind:      ir.PartReasoning,
							Reasoning: &synthReasoning,
						}
						out = append(out,
							ir.Event{
								Kind:  ir.EventPartStart,
								Index: synthIdx,
								Part:  &startSynth,
							},
							ir.Event{
								Kind:  ir.EventPartEnd,
								Index: synthIdx,
								Part:  &endSynth,
							},
						)
						continue
					}
				}
			}

			ev.Index = outIdx
			out = append(out, ev)

		case ir.EventError:
			if s.deferredText != nil {
				s.openDeferredText(&out)
			}
			out = append(out, ev)

		case ir.EventResponseEnd:
			s.deferredText = nil
			s.deferredTextOrig = -1
			out = append(out, ev)

		default:
			out = append(out, ev)
		}
	}
	return out, nil
}

// applyLateToolSignatures refreshes previously recorded CallMemory bindings
// when a later upstream SSE frame supplies a non-empty signature for an
// already-closed explicit-ID tool call. Nil CallMemory stores or unknown part
// indices are ignored without mutating client SSE frames.
func (s *streamCarrierSealer) applyLateToolSignatures(revs []lateToolSignature) {
	if s.calls == nil || len(revs) == 0 {
		return
	}
	for _, rev := range revs {
		clientID, ok := s.sealedCallIDs[rev.partIndex]
		if !ok || rev.signature == "" {
			continue
		}
		origUpstreamID := s.sealedUpstreamIDs[rev.partIndex]
		if origUpstreamID == "" {
			origUpstreamID = rev.upstreamID
		}
		recordCallBinding(
			s.calls,
			&ir.ToolCall{
				CallID: clientID,
				Token:  &ir.ReplayToken{Data: rev.signature},
			},
			origUpstreamID,
			s.route.Dialect,
		)
	}
}

// rememberResponseCall synthesizes a missing client CallID when calls is
// non-nil and records the upstream call binding into calls.
func rememberResponseCall(
	call *ir.ToolCall,
	route Route,
	calls CallMemory,
	runtime Runtime,
	seq int,
	pPath string,
	losses *[]ir.Loss,
) {
	if calls == nil {
		return
	}
	origUpstreamID := call.CallID
	if call.CallID == "" {
		prefix := clientCallIDPrefix(route.Client)
		call.CallID = runtime.NextID(prefix, fmt.Sprintf("%s%d", prefix, seq))
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossIDRewritten,
			Path: pPath + ".call_id",
		})
	}
	recordCallBinding(calls, call, origUpstreamID, route.Dialect)
}

// recordCallBinding stores call's upstream ID and optional call-bound token in calls.
func recordCallBinding(calls CallMemory, call *ir.ToolCall, origUpstreamID string, dialect Dialect) {
	var tokCopy *ir.ReplayToken
	if call.Token != nil && call.Token.Data != "" && ir.IsCarrierScheme(dialect.ReplayScheme) {
		pid := int64(0)
		if ir.IsAccountBoundScheme(dialect.ReplayScheme) && dialect.ProviderID > 0 {
			pid = dialect.ProviderID
		}
		tokCopy = &ir.ReplayToken{
			Scheme:     dialect.ReplayScheme,
			ProviderID: pid,
			Data:       call.Token.Data,
		}
	}
	calls.Remember(call.CallID, CallBinding{
		UpstreamID: origUpstreamID,
		Token:      tokCopy,
	})
}

// clientCallIDPrefix returns the response-side tool call ID prefix expected by clientProto.
func clientCallIDPrefix(clientProto protocol.ProtocolID) string {
	if clientProto == protocol.ProtocolAnthropicMessages {
		return "toolu_"
	}
	return "call_"
}

// sealReasoningPart wraps r.Token or tokenless reasoning text into a trc1
// carrier envelope for cross-protocol client targets that carry opaque fields.
// Tokens emitted by endpoints with no configured ReplayScheme are dropped and
// reported via LossReplayTokenDropped before plaintext carrier fallback runs.
func sealReasoningPart(r *ir.Reasoning, route Route, pPath string, losses *[]ir.Loss) error {
	if route.Client == route.Upstream {
		return nil
	}

	scheme := route.Dialect.ReplayScheme
	if r.Token != nil && r.Token.Data != "" && !ir.IsCarrierScheme(scheme) {
		// Drop tokens emitted by upstreams with no recognized ReplayScheme so
		// unattributed bare strings never enter client envelopes.
		r.Token = nil
		r.Redacted = false
		*losses = append(*losses, ir.Loss{
			Kind: ir.LossReplayTokenDropped,
			Path: pPath,
		})
	}

	if route.Client == protocol.ProtocolOpenAIChatCompletions {
		return nil
	}

	joined := strings.Join(r.Segments, "")
	if r.Token != nil && r.Token.Data != "" {
		carrier := ir.Carrier{
			Scheme:   scheme,
			Data:     r.Token.Data,
			Redacted: r.Redacted,
		}
		if ir.IsTextCarryingScheme(scheme) && !r.Redacted {
			carrier.Text = joined
		}
		if ir.IsAccountBoundScheme(scheme) && route.Dialect.ProviderID > 0 {
			carrier.ProviderID = route.Dialect.ProviderID
		}
		formatted, err := ir.FormatCarrier(carrier)
		if err != nil {
			return fmt.Errorf("seal %s reasoning carrier at %s: %w", scheme, pPath, err)
		}
		r.Token = &ir.ReplayToken{
			Scheme:     scheme,
			ProviderID: carrier.ProviderID,
			Data:       formatted,
		}
		return nil
	}

	if !r.Redacted && joined != "" {
		formatted, err := ir.FormatCarrier(ir.Carrier{
			Scheme: ir.SchemePlain,
			Text:   joined,
		})
		if err != nil {
			return fmt.Errorf("seal plain reasoning carrier at %s: %w", pPath, err)
		}
		r.Token = &ir.ReplayToken{
			Scheme: ir.SchemePlain,
			Data:   formatted,
		}
	}
	return nil
}
