package convert

import (
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// tools_raise.go restores lowered custom, client_search, and namespaced tool
// identities across sequential IR stream events and buffered IR responses using
// the attempt's ToolMap.
//
// Function-only upstreams stream lowered custom inputs as incremental JSON
// string fragments such as {"input":"*** Begin Patch..."} that can split inside
// escape sequences across chunks. Stream raising switches PartToolCall back to
// ToolCustom at EventPartStart, buffers wrapped JSON deltas until EventPartEnd
// delivers the complete wrapper, unwraps the raw input string, and emits one
// clean custom input delta before closing the part.

const (
	// raiseModeNone through raiseModeNamespace classify how toolStreamRaiser
	// transforms the currently open PartToolCall slot.
	raiseModeNone      raiseCallMode = iota // ordinary unmapped tool call
	raiseModeCustom                         // lowered custom tool requiring wrapper unwrapping
	raiseModeSearch                         // lowered tool_search proxy call
	raiseModeNamespace                      // flattened namespaced function call
)

// raiseCallMode identifies the active restoration mode on toolStreamRaiser.
type raiseCallMode uint8

// toolStreamRaiser restores lowered client tool identities across one sequential IR event stream.
type toolStreamRaiser struct {
	tools      *ToolMap        // immutable ToolMap captured when Exchange opened
	mode       raiseCallMode   // active restoration mode for the currently open part
	customName string          // original custom tool name during raiseModeCustom
	nsIdent    NamespacedName  // original (namespace, name) during raiseModeCustom or raiseModeNamespace
	argBuf     strings.Builder // buffered JSON wrapper fragments during raiseModeCustom
}

// newToolStreamRaiser constructs a stateful raiser backed by tools.
func newToolStreamRaiser(tools *ToolMap) *toolStreamRaiser {
	return &toolStreamRaiser{tools: tools}
}

// Raise transforms sequential upstream IR events in order, restoring custom,
// search, and namespaced tool identities when tools carries lowered mappings.
// Nil receivers, empty event slices, or tool maps without active restorations
// return events unchanged without allocating a replacement slice.
//
// Lowered custom tool calls switch back to ToolCustom at EventPartStart while
// suppressing raw JSON wrapper deltas until EventPartEnd unwraps the inner
// input string and emits a single restored text-argument delta before closure.
func (r *toolStreamRaiser) Raise(events []ir.Event) []ir.Event {
	if r == nil || !r.tools.HasRestorations() || len(events) == 0 {
		return events
	}

	out := make([]ir.Event, 0, len(events)+1)
	for _, ev := range events {
		switch ev.Kind {
		case ir.EventPartStart:
			r.resetActive()
			if ev.Part != nil && ev.Part.Kind == ir.PartToolCall && ev.Part.Call != nil {
				ev = r.raisePartStart(ev)
			}
			out = append(out, ev)

		case ir.EventPartDelta:
			if r.mode == raiseModeCustom {
				if ev.Delta != nil && ev.Delta.Arguments != "" {
					_, _ = r.argBuf.WriteString(ev.Delta.Arguments)
				}
				// Suppress incremental JSON wrapper fragments until EventPartEnd unwraps input.
				continue
			}
			if r.mode == raiseModeSearch {
				// Client search calls carry complete JSON arguments on output_item.done.
				continue
			}
			out = append(out, ev)

		case ir.EventPartEnd:
			if r.mode != raiseModeNone {
				out = append(out, r.raisePartEnd(ev)...)
				r.resetActive()
				continue
			}
			if ev.Part != nil && ev.Part.Kind == ir.PartToolCall && ev.Part.Call != nil {
				cp := ev.Clone()
				raiseSingleToolCallPart(cp.Part, r.tools)
				out = append(out, cp)
				continue
			}
			out = append(out, ev)

		default:
			out = append(out, ev)
		}
	}
	return out
}

// resetActive clears active open-call tracking state on r.
func (r *toolStreamRaiser) resetActive() {
	r.mode = raiseModeNone
	r.customName = ""
	r.nsIdent = NamespacedName{}
	r.argBuf.Reset()
}

// raisePartStart classifies an opening PartToolCall against r.tools and
// rewrites its skeleton identity on a cloned event before client stream
// encoding begins. Unmapped function or hosted tool calls return ev unchanged.
func (r *toolStreamRaiser) raisePartStart(ev ir.Event) ir.Event {
	ns := ev.Part.Call.Tool.Namespace
	name := ev.Part.Call.Tool.Name
	if orig, ok := r.tools.LookupCustomCall(ns, name); ok {
		cp := ev.Clone()
		r.mode = raiseModeCustom
		r.nsIdent = orig
		r.customName = orig.Name
		if cp.Part.Call.Arguments != "" {
			_, _ = r.argBuf.WriteString(cp.Part.Call.Arguments)
			cp.Part.Call.Arguments = ""
		}
		cp.Part.ID = retypeToolCallItemID(cp.Part.ID, "custom_tool_call")
		cp.Part.Call.Tool = ir.ToolRef{
			Kind:      ir.ToolCustom,
			Namespace: orig.Namespace,
			Name:      orig.Name,
		}
		return cp
	}
	if r.tools.IsToolSearchProxyCall(ns, name) {
		cp := ev.Clone()
		r.mode = raiseModeSearch
		cp.Part.ID = retypeToolCallItemID(cp.Part.ID, "tool_search_call")
		cp.Part.Call.Tool = ir.ToolRef{Kind: ir.ToolClientSearch}
		cp.Part.Call.Arguments = ""
		return cp
	}
	trimmedNS := strings.TrimSpace(ns)
	if ev.Part.Call.Tool.Kind == ir.ToolFunction && (trimmedNS == "" || trimmedNS == defaultFunctionsNamespace) {
		if pair, ok := r.tools.LookupNamespaceTool(name); ok {
			cp := ev.Clone()
			r.mode = raiseModeNamespace
			r.nsIdent = pair
			cp.Part.Call.Tool = ir.ToolRef{
				Kind:      ir.ToolFunction,
				Namespace: pair.Namespace,
				Name:      pair.Name,
			}
			return cp
		}
	}
	return ev
}

// raisePartEnd finalizes an active lowered PartToolCall on EventPartEnd,
// unwrapping custom JSON wrappers and emitting the restored delta + end pair.
func (r *toolStreamRaiser) raisePartEnd(ev ir.Event) []ir.Event {
	cp := ev.Clone()
	if cp.Part == nil || cp.Part.Call == nil {
		return []ir.Event{cp}
	}

	switch r.mode {
	case raiseModeCustom:
		wrapped := cp.Part.Call.Arguments
		if wrapped == "" {
			wrapped = r.argBuf.String()
		}
		unwrapped := unwrapCustomToolInput(wrapped)
		cp.Part.ID = retypeToolCallItemID(cp.Part.ID, "custom_tool_call")
		cp.Part.Call.Tool = ir.ToolRef{
			Kind:      ir.ToolCustom,
			Namespace: r.nsIdent.Namespace,
			Name:      r.customName,
		}
		cp.Part.Call.Arguments = unwrapped
		if unwrapped != "" {
			deltaEv := ir.Event{
				Kind:  ir.EventPartDelta,
				Index: cp.Index,
				Delta: &ir.Delta{Arguments: unwrapped},
			}
			return []ir.Event{deltaEv, cp}
		}
		return []ir.Event{cp}

	case raiseModeSearch:
		args := cp.Part.Call.Arguments
		if strings.TrimSpace(args) == "" && !cp.Part.Call.Incomplete {
			args = "{}"
		}
		cp.Part.ID = retypeToolCallItemID(cp.Part.ID, "tool_search_call")
		cp.Part.Call.Tool = ir.ToolRef{Kind: ir.ToolClientSearch}
		cp.Part.Call.Arguments = args
		return []ir.Event{cp}

	case raiseModeNamespace:
		cp.Part.Call.Tool = ir.ToolRef{
			Kind:      ir.ToolFunction,
			Namespace: r.nsIdent.Namespace,
			Name:      r.nsIdent.Name,
		}
		return []ir.Event{cp}

	default:
		return []ir.Event{cp}
	}
}

// raiseResponseTools restores lowered custom, search, and namespaced tool calls
// across all output parts of a buffered ir.Response in place.
func raiseResponseTools(resp *ir.Response, tools *ToolMap) {
	if resp == nil || !tools.HasRestorations() {
		return
	}
	for i := range resp.Parts {
		if resp.Parts[i].Kind == ir.PartToolCall && resp.Parts[i].Call != nil {
			raiseSingleToolCallPart(&resp.Parts[i], tools)
		}
	}
}

// raiseSingleToolCallPart restores p's ToolCall identity, item ID prefix, and
// unwrapped custom or default search arguments in place against tools. Nil
// parts, non-call parts, or unmapped tool names return without modifying p.
func raiseSingleToolCallPart(p *ir.Part, tools *ToolMap) {
	if p == nil || p.Call == nil || !tools.HasRestorations() {
		return
	}
	ns := p.Call.Tool.Namespace
	name := p.Call.Tool.Name
	if orig, ok := tools.LookupCustomCall(ns, name); ok {
		p.ID = retypeToolCallItemID(p.ID, "custom_tool_call")
		p.Call.Tool = ir.ToolRef{
			Kind:      ir.ToolCustom,
			Namespace: orig.Namespace,
			Name:      orig.Name,
		}
		p.Call.Arguments = unwrapCustomToolInput(p.Call.Arguments)
		return
	}
	if tools.IsToolSearchProxyCall(ns, name) {
		p.ID = retypeToolCallItemID(p.ID, "tool_search_call")
		p.Call.Tool = ir.ToolRef{Kind: ir.ToolClientSearch}
		if strings.TrimSpace(p.Call.Arguments) == "" && !p.Call.Incomplete {
			p.Call.Arguments = "{}"
		}
		return
	}
	trimmedNS := strings.TrimSpace(ns)
	if p.Call.Tool.Kind == ir.ToolFunction && (trimmedNS == "" || trimmedNS == defaultFunctionsNamespace) {
		if pair, ok := tools.LookupNamespaceTool(name); ok {
			p.Call.Tool = ir.ToolRef{
				Kind:      ir.ToolFunction,
				Namespace: pair.Namespace,
				Name:      pair.Name,
			}
		}
	}
}

// mapUpstreamEventToolNames rewrites non-hosted PartToolCall names on
// EventPartStart and EventPartEnd through mapFn before lowered ToolMap
// restoration runs. Nil callbacks, empty event slices, or callbacks returning
// blank or unchanged names return events without allocating a replacement slice.
func mapUpstreamEventToolNames(events []ir.Event, mapFn func(string) string) []ir.Event {
	if mapFn == nil || len(events) == 0 {
		return events
	}
	var out []ir.Event
	for i, ev := range events {
		if (ev.Kind != ir.EventPartStart && ev.Kind != ir.EventPartEnd) ||
			ev.Part == nil ||
			ev.Part.Kind != ir.PartToolCall ||
			ev.Part.Call == nil ||
			ev.Part.Call.Tool.Kind == ir.ToolHosted {
			if out != nil {
				out = append(out, ev)
			}
			continue
		}
		orig := strings.TrimSpace(ev.Part.Call.Tool.Name)
		if orig == "" {
			if out != nil {
				out = append(out, ev)
			}
			continue
		}
		mapped := strings.TrimSpace(mapFn(orig))
		if mapped == "" || mapped == ev.Part.Call.Tool.Name {
			if out != nil {
				out = append(out, ev)
			}
			continue
		}
		if out == nil {
			out = make([]ir.Event, 0, len(events))
			out = append(out, events[:i]...)
		}
		cp := ev.Clone()
		cp.Part.Call.Tool.Name = mapped
		out = append(out, cp)
	}
	if out != nil {
		return out
	}
	return events
}

// mapUpstreamResponseToolNames rewrites non-hosted PartToolCall names across
// resp.Parts in place through mapFn before buffered ToolMap restoration runs.
// Nil responses, nil callbacks, or blank return values leave tool names intact.
func mapUpstreamResponseToolNames(resp *ir.Response, mapFn func(string) string) {
	if resp == nil || mapFn == nil {
		return
	}
	for i := range resp.Parts {
		p := &resp.Parts[i]
		if p.Kind != ir.PartToolCall || p.Call == nil || p.Call.Tool.Kind == ir.ToolHosted {
			continue
		}
		orig := strings.TrimSpace(p.Call.Tool.Name)
		if orig == "" {
			continue
		}
		if mapped := strings.TrimSpace(mapFn(orig)); mapped != "" && mapped != p.Call.Tool.Name {
			p.Call.Tool.Name = mapped
		}
	}
}
