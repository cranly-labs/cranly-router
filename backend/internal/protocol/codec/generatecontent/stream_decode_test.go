package generatecontent

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
	"github.com/TokenFlux/TokenRouter/internal/protocol/testkit"
)

// TestStreamDecoderDeferredResponseEndBatchReplayAndFragmentedArgs feeds
// post-finishReason sparse usage tail frames and string-fragmented functionCall
// arguments through StreamDecoder. Decode must defer EventResponseEnd until
// Close(nil), assemble valid JSON object fragments, and emit EventError on
// duplicate argument keys.
func TestStreamDecoderDeferredResponseEndBatchReplayAndFragmentedArgs(t *testing.T) {
	dec := NewStreamDecoder()

	// Frame 1 carries finishReason="STOP", yet Decode must NOT emit EventResponseEnd yet!
	f1Batch, err := dec.Decode(sse.Frame{
		Data: []byte(`{"responseId":"r1","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Done."}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"cachedContentTokenCount":40}}`),
	})
	require.NoError(t, err)
	for _, ev := range f1Batch {
		require.NotEqual(t, ir.EventResponseEnd, ev.Kind, "Decode must defer EventResponseEnd until Close(nil)")
	}

	// Frame 2 arrives during the 5s tail window with sparse output usageMetadata.
	f2Batch, err := dec.Decode(sse.Frame{
		Data: []byte(`{"responseId":"r1","modelVersion":"gemini-2.5-pro","usageMetadata":{"candidatesTokenCount":12,"thoughtsTokenCount":5}}`),
	})
	require.NoError(t, err)
	require.Len(t, f2Batch, 1)
	require.Equal(t, ir.EventUsage, f2Batch[0].Kind)
	require.Equal(t, int64(60), f2Batch[0].Usage.Input)
	require.Equal(t, int64(40), f2Batch[0].Usage.CacheRead)
	require.Equal(t, int64(17), f2Batch[0].Usage.Output)
	require.Equal(t, int64(5), f2Batch[0].Usage.Reasoning)

	allEvents := append(append([]ir.Event{}, f1Batch...), f2Batch...)
	closeBatch := dec.Close(nil)
	allEvents = append(allEvents, closeBatch...)
	require.NoError(t, ir.ValidateStream(allEvents))
	require.Equal(t, ir.EventResponseEnd, allEvents[len(allEvents)-1].Kind)

	// String-fragmented functionCall.args assemble cleanly when valid and emit EventError when invalid.
	validFragSSE := []byte(
		`data: {"responseId":"r2","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"c1","name":"fn","args":"{\"x\":"}}]}}]}` + "\n\n" +
			`data: {"responseId":"r2","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"c1","name":"fn","args":"1}"}}]},"finishReason":"STOP"}]}` + "\n\n",
	)
	fragEvents := decodeFullSSEStream(t, validFragSSE)
	require.NoError(t, ir.ValidateStream(fragEvents))
	foldedFrag, err := ir.Accumulate(fragEvents)
	require.NoError(t, err)
	require.Len(t, foldedFrag.Parts, 1)
	require.Equal(t, `{"x":1}`, foldedFrag.Parts[0].Call.Arguments)

	badFragSSE := []byte(
		`data: {"responseId":"r3","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"c1","name":"fn","args":"{\"a\":1,\"a\":2}"}}]},"finishReason":"STOP"}]}` + "\n\n",
	)
	badFragEvents := decodeFullSSEStream(t, badFragSSE)
	require.NoError(t, ir.ValidateStream(badFragEvents))
	require.Equal(t, ir.EventError, badFragEvents[len(badFragEvents)-1].Kind)
}

// TestStreamDecoderToolReplayWindowLateSignaturesAndSignedBlocks exercises
// StreamDecoder on intervening-text tool replay resets (2435ab3f5), closed
// explicit-ID late signatures (4b05d79a5), same-frame nameless string-arg calls
// (d91aacd8e), short duplicate text prefixes, and adjacent signed thought parts.
// All decoded streams must satisfy ir.ValidateStream and match buffered S2 parity.
func TestStreamDecoderToolReplayWindowLateSignaturesAndSignedBlocks(t *testing.T) {
	// 1. Closed explicit-ID calls suppress duplicate emission and queue late signatures.
	decLate := NewStreamDecoder()
	b1, err := decLate.Decode(sse.Frame{
		Data: []byte(`{"responseId":"r_late","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gemini-paris","name":"get_weather","args":{"city":"Paris"}}},{"functionCall":{"id":"gemini-london","name":"get_weather","args":{"city":"London"}}}]}}]}`),
	})
	require.NoError(t, err)
	b2, err := decLate.Decode(sse.Frame{
		Data: []byte(`{"responseId":"r_late","modelVersion":"gemini-2.5-pro","candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"id":"gemini-paris","name":"get_weather","args":{"city":"Paris"}},"thoughtSignature":"original-signature"}]},"finishReason":"STOP"}]}`),
	})
	require.NoError(t, err)
	revs := decLate.DrainLateToolSignatures()
	require.Equal(t, []LateToolSignature{{
		PartIndex:  0,
		UpstreamID: "gemini-paris",
		Signature:  "original-signature",
	}}, revs)
	require.Empty(t, decLate.DrainLateToolSignatures())
	lateEvents := append(append(b1, b2...), decLate.Close(nil)...)
	require.NoError(t, ir.ValidateStream(lateEvents))
	foldedLate, err := ir.Accumulate(lateEvents)
	require.NoError(t, err)
	require.Len(t, foldedLate.Parts, 2, "closed explicit-ID replay must not emit a duplicate third tool call")

	// 2. Intervening text frame resets prevToolBatch so a later identical nameless call is NOT dropped;
	// nameless calls also preserve empty CallID == "".
	repeatedAfterTextSSE := []byte(
		`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"name":"poll_job","args":{"id":1}}}]}}]}` + "\n\n" +
			`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Still running..."}]}}]}` + "\n\n" +
			`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"name":"poll_job","args":{"id":1}}}]},"finishReason":"STOP"}]}` + "\n\n",
	)
	repEvents := decodeFullSSEStream(t, repeatedAfterTextSSE)
	require.NoError(t, ir.ValidateStream(repEvents))
	foldedRep, err := ir.Accumulate(repEvents)
	require.NoError(t, err)
	require.Len(t, foldedRep.Parts, 3)
	require.Empty(t, foldedRep.Parts[0].Call.CallID)
	require.Equal(t, "Still running...", foldedRep.Parts[1].Text)
	require.Empty(t, foldedRep.Parts[2].Call.CallID)

	// 3. Same-frame nameless string-arg parallel calls stay separate instead of concatenating into "{...}{...}".
	sameFrameNamelessStrSSE := []byte(
		`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"functionCall":{"name":"fn","args":"{\"a\":1}"}},{"functionCall":{"name":"fn","args":"{\"b\":2}"}}]},"finishReason":"STOP"}]}` + "\n\n",
	)
	strParallelEvents := decodeFullSSEStream(t, sameFrameNamelessStrSSE)
	require.NoError(t, ir.ValidateStream(strParallelEvents))
	foldedStrParallel, err := ir.Accumulate(strParallelEvents)
	require.NoError(t, err)
	require.Len(t, foldedStrParallel.Parts, 2)
	require.Equal(t, `{"a":1}`, foldedStrParallel.Parts[0].Call.Arguments)
	require.Equal(t, `{"b":2}`, foldedStrParallel.Parts[1].Call.Arguments)

	// 4. Short (<16 byte) duplicate prefix replay is suppressed and distinct signed thought blocks split cleanly.
	shortPrefixAndSignedSSE := []byte(
		`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Plan A.","thought":true,"thoughtSignature":"sig_a"}]}}]}` + "\n\n" +
			`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Plan B.","thought":true,"thoughtSignature":"sig_b"}]}}]}` + "\n\n" +
			`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Hello"}]}}]}` + "\n\n" +
			`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Hel"}]},"finishReason":"STOP"}]}` + "\n\n",
	)
	spEvents := decodeFullSSEStream(t, shortPrefixAndSignedSSE)
	require.NoError(t, ir.ValidateStream(spEvents))
	foldedSP, err := ir.Accumulate(spEvents)
	require.NoError(t, err)
	require.Len(t, foldedSP.Parts, 3)
	require.Equal(t, "sig_a", foldedSP.Parts[0].Reasoning.Token.Data)
	require.Equal(t, "sig_b", foldedSP.Parts[1].Reasoning.Token.Data)
	require.Equal(t, "Hello", foldedSP.Parts[2].Text)
}

// FuzzStreamDecode feeds arbitrary SSE byte streams through sse.NewReader and
// StreamDecoder across all Gemini wire stream seeds. Whenever every frame
// decodes without error, the resulting event sequence must pass ir.ValidateStream
// without panicking.
func FuzzStreamDecode(f *testing.F) {
	streamFixes, err := testkit.WireFixtures(protocol.ProtocolGeminiGenerateContent, testkit.FixtureStream)
	require.NoError(f, err)
	for _, fix := range streamFixes {
		f.Add(fix.Data)
	}
	f.Add([]byte("data: [DONE]\n\n"))
	f.Add([]byte("data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"\":1}]}}]}\n\n"))
	f.Add([]byte("data: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"functionCall\":{\"name\":\"fn\",\"args\":\"{\\\"a\\\":1,\\\"a\\\":2}\"}}]},\"finishReason\":\"STOP\"}]}\n\n"))
	f.Add([]byte("data: {\"usageMetadata\":{\"promptTokenCount\":100,\"cachedContentTokenCount\":30}}\n\ndata: {\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":0,\"cachedContentTokenCount\":0,\"candidatesTokenCount\":5}}\n\n"))

	f.Fuzz(func(t *testing.T, raw []byte) {
		rd := sse.NewReader(bytes.NewReader(raw))
		dec := NewStreamDecoder()
		var events []ir.Event
		decodeFailed := false

		for rd.Scan() {
			batch, err := dec.Decode(rd.Frame())
			if err != nil {
				decodeFailed = true
				break
			}
			events = append(events, batch...)
		}
		if decodeFailed {
			_ = dec.Close(errors.New("decode aborted"))
			return
		}
		events = append(events, dec.Close(rd.Err())...)
		if err := ir.ValidateStream(events); err != nil {
			t.Fatalf("StreamDecoder produced invalid canonical event stream: %v\nevents=%+v", err, events)
		}
	})
}
