package generatecontent

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestDecodeAndEncodeResponseGroundingMediaAndRejections decodes and re-encodes
// buffered Gemini responses containing inline media, grounding web citations,
// and structured token usage. It also verifies that inline error envelopes,
// duplicate argument keys, and non-object functionCall arguments return errors.
func TestDecodeAndEncodeResponseGroundingMediaAndRejections(t *testing.T) {
	raw := []byte(`{
		"responseId": "gem_resp_1",
		"modelVersion": "gemini-2.5-pro",
		"candidates": [
			{
				"index": 0,
				"content": {
					"role": "model",
					"parts": [
						{"inlineData": {"mimeType": "image/png", "data": "iVBORw0KGgo="}},
						{"text": "Generated diagram."}
					]
				},
				"groundingMetadata": {
					"webSearchQueries": ["diagram spec"],
					"groundingChunks": [{"web": {"uri": "https://example.com/spec", "title": "Spec Doc"}}]
				},
				"finishReason": "STOP"
			}
		],
		"usageMetadata": {
			"promptTokenCount": 40,
			"candidatesTokenCount": 15,
			"totalTokenCount": 55
		}
	}`)

	resp, err := DecodeResponse(raw)
	require.NoError(t, err)
	require.NoError(t, ir.ValidateResponse(resp))
	require.Len(t, resp.Parts, 2)
	require.Equal(t, ir.PartMedia, resp.Parts[0].Kind)
	require.Contains(t, resp.Parts[1].Text, "\n\n---\nWeb search queries: diagram spec\n\nSources:\n[1] [Spec Doc](https://example.com/spec)")
	require.Equal(t, []ir.Annotation{{
		Kind:  "url_citation",
		URL:   "https://example.com/spec",
		Title: "Spec Doc",
	}}, resp.Parts[1].Annotations)

	encoded, losses, err := EncodeResponse(resp, ir.ClientView{
		Model:   "gemini-2.5-pro",
		Runtime: newDeterministicRuntime(),
	})
	require.NoError(t, err)
	require.Equal(t, []ir.Loss{{
		Kind: ir.LossAnnotationDropped,
		Path: "parts[1].annotations",
	}}, losses)

	redecoded, err := DecodeResponse(encoded)
	require.NoError(t, err)
	diffs, err := testkit.VerifyEssence(ir.ResponseEssence(resp), ir.ResponseEssence(redecoded), losses)
	require.NoError(t, err)
	require.Empty(t, diffs, testkit.FormatJSONDiff(diffs))

	// Reject inline error envelopes, duplicate tool argument keys, and non-object args.
	for _, bad := range [][]byte{
		[]byte(`{"error":{"code":500,"message":"boom","status":"INTERNAL"}}`),
		[]byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"fn","args":{"a":1,"a":2}}}]},"finishReason":"STOP"}]}`),
		[]byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"fn","args":[1,2]}}]},"finishReason":"STOP"}]}`),
	} {
		_, decErr := DecodeResponse(bad)
		require.Error(t, decErr)
	}
}

// TestDecodeResponseNamelessCallsSplitExplicitIDMergeAndSignedBlocks exercises
// buffered DecodeResponse on omitted call IDs, split explicit-ID string-arg
// functionCall parts, sparse grounding chunks, and distinct adjacent signatures.
// Nameless calls must retain empty CallIDs, split explicit-ID calls must merge
// arguments and late signatures, and distinct signed blocks must not coalesce.
func TestDecodeResponseNamelessCallsSplitExplicitIDMergeAndSignedBlocks(t *testing.T) {
	raw := []byte(`{
		"responseId": "gem_resp_merge",
		"modelVersion": "gemini-2.5-pro",
		"candidates": [
			{
				"index": 0,
				"content": {
					"role": "model",
					"parts": [
						{"text": "First thought.", "thought": true, "thoughtSignature": "sig_th_1"},
						{"text": "Second thought.", "thought": true, "thoughtSignature": "sig_th_2"},
						{"functionCall": {"id": "call_split_1", "name": "lookup_city", "args": "{\"city\":"}},
						{"functionCall": {"id": "call_split_1", "name": "lookup_city", "args": "\"Paris\"}"}, "thoughtSignature": "sig_call_late"},
						{"functionCall": {"id": "call_split_1", "name": "lookup_city", "args": "{\"city\":\"Paris\"}"}},
						{"functionCall": {"name": "nameless_fn", "args": {"ok": true}}},
						{"text": "Summary."}
					]
				},
				"groundingMetadata": {
					"groundingChunks": [
						{},
						{"web": {"uri": "   ", "title": "   "}}
					]
				},
				"finishReason": "STOP"
			}
		]
	}`)

	resp, err := DecodeResponse(raw)
	require.NoError(t, err)
	require.NoError(t, ir.ValidateResponse(resp))
	require.Len(t, resp.Parts, 5)

	// Adjacent signed thought blocks keep distinct signatures without coalescing.
	require.Equal(t, []string{"First thought."}, resp.Parts[0].Reasoning.Segments)
	require.Equal(t, "sig_th_1", resp.Parts[0].Reasoning.Token.Data)
	require.Equal(t, []string{"Second thought."}, resp.Parts[1].Reasoning.Segments)
	require.Equal(t, "sig_th_2", resp.Parts[1].Reasoning.Token.Data)

	// Split explicit-ID functionCall merges string args, stops after full object, and keeps late signature.
	require.Equal(t, "call_split_1", resp.Parts[2].Call.CallID)
	require.Equal(t, `{"city":"Paris"}`, resp.Parts[2].Call.Arguments)
	require.NotNil(t, resp.Parts[2].Call.Token)
	require.Equal(t, "sig_call_late", resp.Parts[2].Call.Token.Data)

	// Nameless functionCall leaves CallID empty so convert's sealCarriers synthesizes client IDs.
	require.Empty(t, resp.Parts[3].Call.CallID)
	require.Equal(t, "nameless_fn", resp.Parts[3].Call.Tool.Name)

	// Grounding with no WebSearchQueries omits "---" and numbers chunks by 1-based original index with fallbacks.
	require.Equal(t, "Summary.\n\nSources:\n[2] [Source](#)", resp.Parts[4].Text)
}

// FuzzResponseDecode feeds arbitrary byte slices into DecodeResponse across all
// Gemini wire response seeds and boundary cases. Every successfully decoded
// response must pass ir.ValidateResponse, carry valid JSON object tool arguments,
// and re-encode deterministically.
func FuzzResponseDecode(f *testing.F) {
	respFixes, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureResponse)
	require.NoError(f, err)
	for _, fix := range respFixes {
		f.Add(fix.Data)
	}
	f.Add([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"fn","args":{"a":1,"a":2}}}]},"finishReason":"STOP"}]}`))
	f.Add([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"":1}]}],"finishReason":"STOP"}`))
	f.Add([]byte(`{"candidates":[{"content":{"role":"model","parts":[{"text":"hi","thought":true}]},"groundingMetadata":{"groundingChunks":[{"web":{"uri":"https://go.dev"}}]},"finishReason":"STOP"}]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		resp, err := DecodeResponse(data)
		if err != nil {
			return
		}
		if err := ir.ValidateResponse(resp); err != nil {
			t.Fatalf("DecodeResponse succeeded but ir.ValidateResponse failed: %v", err)
		}
		for _, p := range resp.Parts {
			if p.Kind == ir.PartToolCall && p.Call != nil && strings.TrimSpace(p.Call.Arguments) != "" {
				if !isValidSingleJSONObject([]byte(p.Call.Arguments)) {
					t.Fatalf("DecodeResponse accepted invalid functionCall.args %q", p.Call.Arguments)
				}
			}
		}
		enc1, _, err := EncodeResponse(resp, ir.ClientView{Model: "gemini-2.5-pro", Runtime: newDeterministicRuntime()})
		if err != nil {
			t.Fatalf("EncodeResponse failed: %v", err)
		}
		enc2, _, err := EncodeResponse(resp, ir.ClientView{Model: "gemini-2.5-pro", Runtime: newDeterministicRuntime()})
		if err != nil {
			t.Fatalf("second EncodeResponse failed: %v", err)
		}
		if !bytesEqual(enc1, enc2) {
			t.Fatalf("non-deterministic EncodeResponse output")
		}
	})
}
