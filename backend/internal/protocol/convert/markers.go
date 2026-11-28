package convert

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// markers.go strips protocol-specific client markers from system instructions
// during cross-protocol conversion and remaps request, response, and stream
// Loss.Path coordinates back to decoded source indices. Removing leading client
// markers, dropping incompatible reasoning blocks, relocating system prompts, or
// promoting discovered search tools shifts slice positions inside the working
// ir.Request before EncodeRequest runs, while response carrier sealing can split
// signed text parts or suppress signature-only empty text parts before client
// encoding.
//
// Request-side sourceCoords records origin indices for System parts, Messages,
// Message.Parts, and Tools so remapEncoderLosses translates codec-emitted paths
// back to decoded client coordinates and omits losses on promoted discovery
// tools (toolIdx == -1) absent from the incoming request. Response-side and
// stream-side remapResponseLosses translates client encoder parts[i] paths
// through outPartToOrig back to decoded upstream response coordinates and drops
// internal losses on synthesized trailing reasoning blocks (origIdx < 0).

const (
	// anthropicBillingHeaderPrefix marks Claude Code billing fingerprint blocks
	// stripped when converting to non-Anthropic targets without AllowBillingHeader.
	anthropicBillingHeaderPrefix = "x-anthropic-billing-header: " // leading Claude Code system billing prefix

	// relocatedSystemMsgSentinel marks a leading developer message inserted from
	// req.System so encoder part losses remap back to original system[i] slots.
	relocatedSystemMsgSentinel = -2 // msgIdx sentinel for System parts moved into Messages[0]
)

// sourceCoords maps current working-request slice positions back to their
// original indices in the freshly decoded client ir.Request.
type sourceCoords struct {
	// editedPartPaths supplies complete source paths for rebuilt recovery
	// messages. Empty entries denote synthesized parts with no client source.
	editedPartPaths map[int][]string
	systemIdx       []int   // working System[i] -> original System index
	msgIdx          []int   // working Messages[m] -> original Messages index
	partIdx         [][]int // working Messages[m].Parts[p] -> original Parts index
	// toolIdx maps working Tools[t] to its original Tools index, or -1 when a
	// tool declaration was promoted from historical tool_search_output items.
	toolIdx []int
}

// newSourceCoords initializes identity coordinate tables matching req's initial
// System, Messages, Parts, and Tools slice lengths.
func newSourceCoords(req *ir.Request) *sourceCoords {
	if req == nil {
		return &sourceCoords{}
	}
	sc := &sourceCoords{
		systemIdx: makeIdentityIndices(len(req.System)),
		msgIdx:    makeIdentityIndices(len(req.Messages)),
		partIdx:   make([][]int, len(req.Messages)),
		toolIdx:   makeIdentityIndices(len(req.Tools)),
	}
	for m := range req.Messages {
		sc.partIdx[m] = makeIdentityIndices(len(req.Messages[m].Parts))
	}
	return sc
}

// makeIdentityIndices returns [0, 1, ..., n-1] or nil when n is zero.
func makeIdentityIndices(n int) []int {
	if n <= 0 {
		return nil
	}
	out := make([]int, n)
	for i := range n {
		out[i] = i
	}
	return out
}

// origSystemPath formats the original IR path for current working System index i.
func (sc *sourceCoords) origSystemPath(i int) string {
	orig := i
	if sc != nil && i >= 0 && i < len(sc.systemIdx) {
		orig = sc.systemIdx[i]
	}
	return fmt.Sprintf("system[%d]", orig)
}

// origPartPath formats the original IR path for a working message part.
// Recovery edits can point into a nested tool result. An edited slot without
// an origin returns an empty path so callers omit synthetic losses.
func (sc *sourceCoords) origPartPath(m, p int) string {
	if sc != nil {
		if paths, edited := sc.editedPartPaths[m]; edited {
			if p < 0 || p >= len(paths) {
				return ""
			}
			return paths[p]
		}
	}
	origM, origP := m, p
	if sc != nil && m >= 0 && m < len(sc.msgIdx) {
		origM = sc.msgIdx[m]
		if origM < 0 && origM != relocatedSystemMsgSentinel {
			return ""
		}
		if p >= 0 && p < len(sc.partIdx[m]) {
			origP = sc.partIdx[m][p]
		}
		if origP < 0 {
			return ""
		}
		if origM == relocatedSystemMsgSentinel {
			return fmt.Sprintf("system[%d]", origP)
		}
	}
	return fmt.Sprintf("messages[%d].parts[%d]", origM, origP)
}

// prependSystemAsDeveloper moves req.System into a leading RoleDeveloper
// message on req.Messages while updating sc so encoder part losses remap back
// to original system[i] coordinates.
func prependSystemAsDeveloper(req *ir.Request, sc *sourceCoords) bool {
	if req == nil || len(req.System) == 0 {
		return false
	}
	devMsg := ir.Message{
		Role:  ir.RoleDeveloper,
		Parts: req.System,
	}
	req.System = nil
	req.Messages = append([]ir.Message{devMsg}, req.Messages...)
	if sc != nil {
		sc.msgIdx = append([]int{relocatedSystemMsgSentinel}, sc.msgIdx...)
		sc.partIdx = append([][]int{sc.systemIdx}, sc.partIdx...)
		sc.systemIdx = nil
	}
	return true
}

// origToolIndex returns the original Tools index for working Tools[t], or -1 for promoted tools.
func (sc *sourceCoords) origToolIndex(t int) int {
	if sc == nil || t < 0 || t >= len(sc.toolIdx) {
		return t
	}
	return sc.toolIdx[t]
}

// stripClientMarkers removes source-only client markers such as Anthropic
// billing headers from req.System and system/developer messages when converting
// across protocols without AllowBillingHeader, updating coords in lockstep.
// Same-protocol routes and dialects with AllowBillingHeader leave req untouched.
func stripClientMarkers(req *ir.Request, coords *sourceCoords, route Route) []ir.Loss {
	if req == nil || route.Client == route.Upstream || route.Dialect.AllowBillingHeader {
		return nil
	}

	var losses []ir.Loss
	if len(req.System) > 0 {
		keptSys := make([]ir.Part, 0, len(req.System))
		var keptIdx []int
		for i, p := range req.System {
			if label, isMarker := matchClientMarkerPart(p); isMarker {
				losses = append(losses, ir.Loss{
					Kind:   ir.LossClientMarkerDropped,
					Path:   coords.origSystemPath(i),
					Detail: label,
				})
				continue
			}
			keptSys = append(keptSys, p)
			if coords != nil && i < len(coords.systemIdx) {
				keptIdx = append(keptIdx, coords.systemIdx[i])
			}
		}
		if len(keptSys) == 0 {
			req.System = nil
		} else {
			req.System = keptSys
		}
		if coords != nil {
			coords.systemIdx = keptIdx
		}
	}

	if len(req.Messages) > 0 {
		keptMsgs := make([]ir.Message, 0, len(req.Messages))
		var keptMsgIdx []int
		var keptPartIdx [][]int
		for m, msg := range req.Messages {
			if msg.Role != ir.RoleSystem && msg.Role != ir.RoleDeveloper {
				keptMsgs = append(keptMsgs, msg)
				if coords != nil && m < len(coords.msgIdx) {
					keptMsgIdx = append(keptMsgIdx, coords.msgIdx[m])
					keptPartIdx = append(keptPartIdx, coords.partIdx[m])
				}
				continue
			}
			var parts []ir.Part
			var pIndices []int
			droppedAny := false
			for p, part := range msg.Parts {
				if label, isMarker := matchClientMarkerPart(part); isMarker {
					droppedAny = true
					losses = append(losses, ir.Loss{
						Kind:   ir.LossClientMarkerDropped,
						Path:   coords.origPartPath(m, p),
						Detail: label,
					})
					continue
				}
				parts = append(parts, part)
				if coords != nil && m < len(coords.partIdx) && p < len(coords.partIdx[m]) {
					pIndices = append(pIndices, coords.partIdx[m][p])
				}
			}
			if len(parts) == 0 && droppedAny {
				continue
			}
			msg.Parts = parts
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
	}
	return losses
}

// matchClientMarkerPart reports whether p is a client-only marker text part and returns its diagnostic label.
func matchClientMarkerPart(p ir.Part) (string, bool) {
	if p.Kind == ir.PartText && strings.HasPrefix(p.Text, anthropicBillingHeaderPrefix) {
		return "x-anthropic-billing-header", true
	}
	return "", false
}

// remapEncoderLosses translates working-request slice indices inside encLosses
// back to original decoded client coordinates using coords. Losses recorded
// against promoted discovery tools (toolIdx == -1) are omitted because those
// declarations had no source slot, and duplicate (Kind, Path, Detail) entries
// already emitted by earlier IR passes are suppressed.
func remapEncoderLosses(prior []ir.Loss, encLosses []ir.Loss, coords *sourceCoords) []ir.Loss {
	if len(encLosses) == 0 {
		return prior
	}
	seen := make(map[ir.Loss]bool, len(prior)+len(encLosses))
	out := make([]ir.Loss, 0, len(prior)+len(encLosses))
	for _, l := range prior {
		seen[l] = true
		out = append(out, l)
	}
	for _, l := range encLosses {
		remapped, keep := remapSingleLossPath(l.Path, coords)
		if !keep {
			continue
		}
		l.Path = remapped
		if seen[l] {
			continue
		}
		seen[l] = true
		out = append(out, l)
	}
	return out
}

// remapSingleLossPath translates system[i], messages[m].parts[p], and tools[t]
// prefixes in path through coords. Promoted discovery tools and synthesized
// parts have no source slot and return keep=false. Recovery media can resolve
// to individual result.content indices, retaining suffixes such as .id.
func remapSingleLossPath(path string, coords *sourceCoords) (string, bool) {
	if coords == nil || path == "" {
		return path, true
	}
	trimmed := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(path), "$"), ".")
	switch {
	case strings.HasPrefix(trimmed, "system["):
		idx, rest, ok := parseLeadingBracketIndex(trimmed[len("system"):])
		if !ok || idx < 0 || idx >= len(coords.systemIdx) {
			return path, true
		}
		return fmt.Sprintf("system[%d]%s", coords.systemIdx[idx], rest), true

	case strings.HasPrefix(trimmed, "tools["):
		idx, rest, ok := parseLeadingBracketIndex(trimmed[len("tools"):])
		if !ok || idx < 0 || idx >= len(coords.toolIdx) {
			return path, true
		}
		orig := coords.toolIdx[idx]
		if orig < 0 {
			// Skip internal encoder metadata losses on promoted tool_search_output
			// discoveries because those declarations have no slot on the source EssenceDoc.
			return "", false
		}
		return fmt.Sprintf("tools[%d]%s", orig, rest), true

	case strings.HasPrefix(trimmed, "messages["):
		mIdx, afterMsg, ok := parseLeadingBracketIndex(trimmed[len("messages"):])
		if !ok || mIdx < 0 || mIdx >= len(coords.msgIdx) {
			return path, true
		}
		origM := coords.msgIdx[mIdx]
		if origM < 0 && origM != relocatedSystemMsgSentinel {
			return "", false
		}
		if partSuffix, ok := strings.CutPrefix(afterMsg, ".parts"); ok {
			if pIdx, rest, parsed := parseLeadingBracketIndex(partSuffix); parsed {
				source := coords.origPartPath(mIdx, pIdx)
				if source == "" {
					// Recovery placeholders have no corresponding client part, so their
					// encoder losses cannot be applied to the source semantic summary.
					return "", false
				}
				return source + rest, true
			}
		}
		if origM == relocatedSystemMsgSentinel {
			return "system", true
		}
		return fmt.Sprintf("messages[%d]%s", origM, afterMsg), true
	}
	return path, true
}

// remapResponseLosses translates output response part indices inside encLosses
// back to original upstream response coordinates via outPartToOrig. Internal
// encoder metadata losses on newly synthesized trailing reasoning blocks
// (origIdx < 0) are omitted, and duplicate Loss entries are suppressed.
func remapResponseLosses(prior []ir.Loss, encLosses []ir.Loss, outPartToOrig []int) []ir.Loss {
	if len(encLosses) == 0 {
		return prior
	}
	seen := make(map[ir.Loss]bool, len(prior)+len(encLosses))
	out := make([]ir.Loss, 0, len(prior)+len(encLosses))
	for _, l := range prior {
		seen[l] = true
		out = append(out, l)
	}
	for _, l := range encLosses {
		remapped, keep := remapResponsePartLossPath(l.Path, outPartToOrig)
		if !keep {
			continue
		}
		l.Path = remapped
		if seen[l] {
			continue
		}
		seen[l] = true
		out = append(out, l)
	}
	return out
}

// remapResponsePartLossPath translates a leading "parts[i]" coordinate through
// outPartToOrig back to its source response index. Negative table entries return
// keep=false so synthetic carrier blocks do not report unmapped losses, while
// empty tables or non-part paths return path unchanged.
func remapResponsePartLossPath(path string, outPartToOrig []int) (string, bool) {
	if len(outPartToOrig) == 0 || path == "" {
		return path, true
	}
	trimmed := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(path), "$"), ".")
	if !strings.HasPrefix(trimmed, "parts[") {
		return path, true
	}
	idx, rest, ok := parseLeadingBracketIndex(trimmed[len("parts"):])
	if !ok || idx < 0 || idx >= len(outPartToOrig) {
		return path, true
	}
	orig := outPartToOrig[idx]
	if orig < 0 {
		return "", false
	}
	return fmt.Sprintf("parts[%d]%s", orig, rest), true
}

// parseLeadingBracketIndex parses a leading "[<nonnegative-int>]" index prefix
// and returns the parsed integer, remaining suffix, and match status.
func parseLeadingBracketIndex(s string) (int, string, bool) {
	if !strings.HasPrefix(s, "[") {
		return 0, "", false
	}
	closePos := strings.IndexByte(s, ']')
	if closePos <= 1 {
		return 0, "", false
	}
	idx, err := strconv.Atoi(s[1:closePos])
	if err != nil || idx < 0 {
		return 0, "", false
	}
	return idx, s[closePos+1:], true
}
