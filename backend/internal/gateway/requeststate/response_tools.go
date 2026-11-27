package requeststate

import (
	"maps"
	"sync"

	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/protocol/openai"
)

// response_tools.go stores tool restoration mappings for active requests, later
// session turns, and WebSocket-to-HTTP bridging. Replacing session names leaves
// the active-turn slot intact, so response restoration can continue using the
// current turn's names while future session declarations change concurrently.
//
// The mutex synchronizes reference publication and retrieval across request and
// session readers. Namespace maps remain caller-immutable after publication,
// Codex-name publication clones its string map into either the active or
// session slot, and IR ToolMap publication and retrieval return detached deep
// copies so callers can inspect or replace per-turn state without mutating
// active output restoration.

// ResponseTools holds separate request, active-turn, session, and HTTP-bridge
// tool restoration state. Its lock protects slot access while callers treat
// shared string and namespace maps returned from those slots as immutable.
type ResponseTools struct {
	// mu protects openaiToolMap, grokToolMap, namespaces, activeNames,
	// sessionNames, bridgeToolMap, and bridgeToolMapPresent during publication
	// and retrieval. Shared map contents stay immutable after unlocking.
	mu                        sync.RWMutex                             // mapping reference publication lock
	openaiToolMap             *convert.ToolMap                         // OpenAI IR lowered-tool identity map
	grokToolMap               *convert.ToolMap                         // Grok IR lowered-tool identity map
	namespaces                map[string]openai.ResponsesNamespaceName // shared flattened namespace reverse map
	activeNames, sessionNames map[string]string                        // active-turn name restoration and future-session name restoration
	bridgeToolMap             *convert.ToolMap                         // captured next HTTP-bridge IR tool state
	bridgeToolMapPresent      bool                                     // IR bridge tool state publication presence flag
}

// Namespaces returns the captured namespace-name map under a read lock, or nil
// for nil state. The map remains shared after unlocking; callers read it
// immutably and publish a replacement to change names.
func (s *ResponseTools) Namespaces() map[string]openai.ResponsesNamespaceName {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.namespaces
}

// SetNamespaces publishes the supplied namespace map and ignores nil state. It
// retains that map rather than cloning it; callers leave its contents unchanged
// while request output may still read its names.
func (s *ResponseTools) SetNamespaces(names map[string]openai.ResponsesNamespaceName) {
	if s == nil {
		return
	}
	// Replace the namespace reference through the same lock used by retrieval;
	// output readers then capture either the earlier or the newly published
	// immutable name map.
	s.mu.Lock()
	s.namespaces = names
	s.mu.Unlock()
}

// CodexNames returns active-turn names or future-session names according to
// session. Nil state returns nil, and the captured map stays shared after
// unlocking, so callers treat its string entries as immutable.
func (s *ResponseTools) CodexNames(session bool) map[string]string {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if session {
		return s.sessionNames
	}
	return s.activeNames
}

// SetCodexNames publishes a copied string map to either active-turn or
// future-session storage. Nil state is ignored, and nil input produces an empty
// map rather than an uninitialized slot.
//
// The copy is prepared before taking the publication lock, so callers keep
// reverse stable during copying. Session publication changes only sessionNames;
// active output can continue reading its existing activeNames map. A later
// active-turn publication replaces that separate slot. Retrieval shares each
// published map, requiring consumers to treat its entries as immutable after
// the setter returns.
func (s *ResponseTools) SetCodexNames(session bool, reverse map[string]string) {
	if s == nil {
		return
	}
	// Publish a stable name set so later caller edits cannot change an active
	// turn while session-name updates proceed through their separate slot.
	copyMap := make(map[string]string, len(reverse))
	maps.Copy(copyMap, reverse)
	s.mu.Lock()
	defer s.mu.Unlock()
	if session {
		s.sessionNames = copyMap
	} else {
		s.activeNames = copyMap
	}
}

// ClientToolMap returns an independent deep copy of the selected OpenAI or Grok
// IR tool map under a read lock. Nil state or an unset slot returns nil so
// callers can inspect HasRestorations safely without aliasing internal storage.
func (s *ResponseTools) ClientToolMap(grok bool) *convert.ToolMap {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	var selected *convert.ToolMap
	if grok {
		selected = s.grokToolMap
	} else {
		selected = s.openaiToolMap
	}
	s.mu.RUnlock()
	return selected.Clone()
}

// SetClientToolMap publishes a detached deep copy of tools to the OpenAI or
// Grok IR restoration slot and ignores nil state. Maps without active
// restorations clear the slot to nil so non-lowered attempts leave no stale state.
func (s *ResponseTools) SetClientToolMap(grok bool, tools *convert.ToolMap) {
	if s == nil {
		return
	}
	var cloned *convert.ToolMap
	if tools.HasRestorations() {
		cloned = tools.Clone()
	}
	// Replace the selected platform's IR tool map atomically under the write
	// lock so concurrent stream and buffered readers observe one coherent snapshot.
	s.mu.Lock()
	if grok {
		s.grokToolMap = cloned
	} else {
		s.openaiToolMap = cloned
	}
	s.mu.Unlock()
}

// BridgeToolMap returns an independent deep copy of the captured HTTP-bridge IR
// tool map and its presence flag under a read lock. Nil state returns (nil, false),
// while an explicit nil publication returns (nil, true) after a tools reset turn.
func (s *ResponseTools) BridgeToolMap() (*convert.ToolMap, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.RLock()
	selected := s.bridgeToolMap
	present := s.bridgeToolMapPresent
	s.mu.RUnlock()
	return selected.Clone(), present
}

// SetBridgeToolMap publishes a detached deep copy of tools into the HTTP-bridge
// IR session slot and marks it present, ignoring nil state. Cloning happens
// outside the write lock, and nil tools record an explicit reset while keeping
// the presence marker true for subsequent continuation turns.
func (s *ResponseTools) SetBridgeToolMap(tools *convert.ToolMap) {
	if s == nil {
		return
	}
	cloned := tools.Clone()
	// Update the cloned session ToolMap and presence flag under one lock so the
	// next WebSocket bridge turn observes atomic declaration and mapping state.
	s.mu.Lock()
	s.bridgeToolMap, s.bridgeToolMapPresent = cloned, true
	s.mu.Unlock()
}
