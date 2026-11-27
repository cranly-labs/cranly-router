package testkit

import (
	"bytes"
	"encoding/json/jsontext"
	"fmt"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// clientsim.go defines multi-turn client replay simulation contracts, variant
// catalogs, and protocol dispatchers for law M1. Each client simulator builds
// the next-turn wire request directly from the previous wire request, the
// current wire response or SSE frames, and simulated tool outputs without
// routing through protocol codecs so codec symmetry bugs cannot hide replay defects.

const (
	// ClientSimVariant constants model real SDK history-construction habits
	// across Anthropic Messages, OpenAI Responses, Chat Completions, and Gemini.
	SimVariantDefault                 ClientSimVariant = "default"            // protocol default preserving full continuity
	SimVariantMessagesDropThinking    ClientSimVariant = "drop_thinking"      // Messages client omitting historical thinking blocks
	SimVariantResponsesPreserveAll    ClientSimVariant = "preserve_all"       // Responses client echoing all output items verbatim
	SimVariantResponsesStripEncrypted ClientSimVariant = "strip_encrypted"    // Responses client stripping encrypted_content
	SimVariantResponsesEncryptedOnly  ClientSimVariant = "encrypted_only"     // Responses client echoing encrypted_content with empty summary
	SimVariantChatPreserveReasoning   ClientSimVariant = "preserve_reasoning" // Chat client echoing assistant reasoning_content
	SimVariantChatDropUnknown         ClientSimVariant = "drop_unknown"       // Chat client dropping non-standard assistant fields
	SimVariantGeminiPreserveThoughts  ClientSimVariant = "preserve_thoughts"  // Gemini client echoing thought parts and signatures
	SimVariantGeminiDropThoughts      ClientSimVariant = "drop_thoughts"      // Gemini client omitting thought text parts
)

// ClientSimVariant selects one client SDK history-replay habit for a protocol.
type ClientSimVariant string

// SimulatedToolOutput supplies one tool execution result to append when
// constructing a follow-up conversation turn.
type SimulatedToolOutput struct {
	CallID  string `json:"call_id,omitzero"`  // target tool call identifier when matched by ID
	Name    string `json:"name,omitzero"`     // target function name fallback
	Output  string `json:"output"`            // textual or JSON-encoded tool output
	IsError bool   `json:"is_error,omitzero"` // whether tool execution returned an error
}

// TurnInput bundles the previous client request, the current response (either
// streamed frames or buffered JSON), and tool results for SimulateNextTurn.
type TurnInput struct {
	PrevRequest              []byte                // previous turn's client wire request JSON
	ResponseFrames           []sse.Frame           // current turn's client SSE frames when streamed
	ResponseBody             []byte                // current turn's client JSON body when buffered
	ToolOutputs              []SimulatedToolOutput // tool results paired by CallID, Name, or position
	SynthesizeMissingOutputs bool                  // fill {"ok":true} when ToolOutputs omits a call
	FollowUpUserText         string                // optional user message text for non-tool or mixed turns
	Variant                  ClientSimVariant      // client habit variant; empty selects default
}

// ClientSimVariants returns the supported replay variants for proto in deterministic order.
func ClientSimVariants(proto protocol.ProtocolID) []ClientSimVariant {
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return []ClientSimVariant{SimVariantDefault, SimVariantMessagesDropThinking}
	case protocol.ProtocolOpenAIResponses:
		return []ClientSimVariant{
			SimVariantResponsesPreserveAll,
			SimVariantResponsesStripEncrypted,
			SimVariantResponsesEncryptedOnly,
		}
	case protocol.ProtocolOpenAIChatCompletions:
		return []ClientSimVariant{SimVariantChatPreserveReasoning, SimVariantChatDropUnknown}
	case protocol.ProtocolGeminiGenerateContent:
		return []ClientSimVariant{SimVariantGeminiPreserveThoughts, SimVariantGeminiDropThoughts}
	default:
		return nil
	}
}

// SimulateNextTurn constructs the next-turn client wire request JSON for proto
// from in without invoking any protocol codec. Empty PrevRequest payloads or
// inputs missing both ResponseFrames and ResponseBody return errors immediately,
// and unsupported protocol identifiers fail before mutating caller data.
func SimulateNextTurn(proto protocol.ProtocolID, in TurnInput) ([]byte, error) {
	if len(in.PrevRequest) == 0 {
		return nil, fmt.Errorf("SimulateNextTurn: PrevRequest is empty")
	}
	if len(in.ResponseFrames) == 0 && len(in.ResponseBody) == 0 {
		return nil, fmt.Errorf("SimulateNextTurn: both ResponseFrames and ResponseBody are empty")
	}
	switch proto {
	case protocol.ProtocolAnthropicMessages:
		return SimulateMessagesNextTurn(in)
	case protocol.ProtocolOpenAIResponses:
		return SimulateResponsesNextTurn(in)
	case protocol.ProtocolOpenAIChatCompletions:
		return SimulateChatNextTurn(in)
	case protocol.ProtocolGeminiGenerateContent:
		return SimulateGeminiNextTurn(in)
	default:
		return nil, fmt.Errorf("SimulateNextTurn: unsupported protocol %q", proto)
	}
}

// resolveToolOutput matches a response tool call against in.ToolOutputs by
// exact CallID first, then unused Name, then unused positional slot, marking
// the chosen entry in used. When no candidate matches, SynthesizeMissingOutputs
// returns a synthetic {"ok":true} output and false returns an error.
func resolveToolOutput(pos int, callID, name string, in TurnInput, used []bool) (SimulatedToolOutput, error) {
	if callID != "" {
		for i, cand := range in.ToolOutputs {
			if !used[i] && cand.CallID != "" && cand.CallID == callID {
				used[i] = true
				return cand, nil
			}
		}
	}
	if name != "" {
		for i, cand := range in.ToolOutputs {
			if !used[i] && cand.CallID == "" && cand.Name != "" && cand.Name == name {
				used[i] = true
				return cand, nil
			}
		}
	}
	if pos < len(in.ToolOutputs) && !used[pos] && in.ToolOutputs[pos].CallID == "" {
		used[pos] = true
		return in.ToolOutputs[pos], nil
	}
	for i, cand := range in.ToolOutputs {
		if !used[i] && cand.CallID == "" {
			used[i] = true
			return cand, nil
		}
	}
	if in.SynthesizeMissingOutputs {
		return SimulatedToolOutput{
			CallID: callID,
			Name:   name,
			Output: `{"ok":true}`,
		}, nil
	}
	return SimulatedToolOutput{}, fmt.Errorf("no SimulatedToolOutput provided for tool call %d (id=%q name=%q)", pos, callID, name)
}

// encodeLosslessNode serializes n with sorted object keys while preserving
// original numeric token spellings from PrevRequest and response fragments.
func encodeLosslessNode(n jsonNode) ([]byte, error) {
	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsontext.AllowInvalidUTF8(true))
	if err := writeSanitizedNode(enc, n); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// makeStringNode constructs a string-typed jsonNode holding s verbatim.
func makeStringNode(s string) jsonNode {
	return jsonNode{kind: nodeString, strVal: s}
}

// makeBoolNode constructs a boolean-typed jsonNode holding b verbatim.
func makeBoolNode(b bool) jsonNode {
	return jsonNode{kind: nodeBool, boolVal: b}
}
