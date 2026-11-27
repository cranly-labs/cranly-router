package ir

// link.go correlates ToolResult parts inside a Request with their originating
// assistant ToolCall parts by CallID. Anthropic Messages and OpenAI Chat
// Completions omit the invoked tool name and custom-vs-function kind on wire
// tool result items, whereas Gemini functionResponse requires the function name
// and OpenAI Responses distinguishes function_call_output from custom_tool_call_output.
//
// Link walks conversation turns in chronological order so each tool result
// binds to the most recent preceding call carrying the same CallID even when
// compatible clients reuse short call identifiers across later turns. Results
// whose call appears later in malformed history fall back to a full-request
// call index, while genuinely unmatched orphan results retain their existing
// Tool field for downstream encoder pairing repair.

// Link populates ToolResult.Tool on every PartToolResult in req.Messages from
// the matching ToolCall sharing the same non-empty CallID. Nil requests are
// ignored without error.
//
// Sequential turn scanning binds each result to the nearest preceding ToolCall
// with that CallID so reused identifiers across multi-turn histories never
// cross-wire earlier results to later calls. When no preceding call matches,
// Link consults a fallback table of later calls in req.Messages; orphan results
// with no matching call anywhere in req keep their existing Tool value untouched.
func Link(req *Request) {
	if req == nil || len(req.Messages) == 0 {
		return
	}

	var fallback map[string]ToolRef
	seen := make(map[string]ToolRef)
	for mIdx := range req.Messages {
		parts := req.Messages[mIdx].Parts
		for pIdx := range parts {
			part := &parts[pIdx]
			if part.Kind == PartToolCall && part.Call != nil && part.Call.CallID != "" {
				if isNonZeroToolRef(part.Call.Tool) {
					seen[part.Call.CallID] = part.Call.Tool
				}
				continue
			}
			if part.Kind != PartToolResult || part.Result == nil || part.Result.CallID == "" {
				continue
			}
			if ref, ok := seen[part.Result.CallID]; ok {
				part.Result.Tool = ref
				continue
			}
			if fallback == nil {
				// Lazily index remaining messages only when a result precedes its call
				// so well-ordered histories avoid a second pass over Messages.
				fallback = collectFirstCallsByID(req.Messages)
			}
			if ref, ok := fallback[part.Result.CallID]; ok {
				part.Result.Tool = ref
			}
		}
	}
}

// collectFirstCallsByID indexes the first non-zero ToolRef observed for each
// non-empty CallID across msgs to resolve out-of-order tool results. Later
// calls reusing the same CallID do not overwrite earlier entries in the map.
func collectFirstCallsByID(msgs []Message) map[string]ToolRef {
	out := make(map[string]ToolRef)
	for mIdx := range msgs {
		for pIdx := range msgs[mIdx].Parts {
			p := &msgs[mIdx].Parts[pIdx]
			if p.Kind != PartToolCall || p.Call == nil || p.Call.CallID == "" {
				continue
			}
			if _, exists := out[p.Call.CallID]; !exists && isNonZeroToolRef(p.Call.Tool) {
				out[p.Call.CallID] = p.Call.Tool
			}
		}
	}
	return out
}

// isNonZeroToolRef reports whether ref carries either a ToolKind or a tool Name.
func isNonZeroToolRef(ref ToolRef) bool {
	return ref.Kind != "" || ref.Namespace != "" || ref.Name != ""
}
