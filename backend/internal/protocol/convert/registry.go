package convert

import (
	"fmt"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/chat"
	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/generatecontent"
	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/messages"
	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/qoderchat"
	"github.com/TokenFlux/TokenRouter/internal/protocol/codec/responses"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// registry.go defines the consumer-owned clientCodec and upstreamCodec
// interfaces and binds protocol identifiers to concrete codec implementations
// through explicit lookup tables without package init side effects. Keeping
// interface ownership inside convert lets leaf codec packages return their own
// concrete StreamDecoder and StreamEncoder structs without circular imports.

var (
	// clientCodecs and upstreamCodecs register the active text, Gemini, and
	// upstream-only Qoder protocol codecs explicitly so Exchange and mockupstream
	// resolve codecs without package init side effects.
	clientCodecs = map[protocol.ProtocolID]clientCodec{
		protocol.ProtocolAnthropicMessages:     messagesCodecAdapter{},
		protocol.ProtocolOpenAIResponses:       responsesCodecAdapter{},
		protocol.ProtocolOpenAIChatCompletions: chatCodecAdapter{},
		protocol.ProtocolGeminiGenerateContent: generatecontentCodecAdapter{},
	}
	upstreamCodecs = map[protocol.ProtocolID]upstreamCodec{
		protocol.ProtocolAnthropicMessages:     messagesCodecAdapter{},
		protocol.ProtocolOpenAIResponses:       responsesCodecAdapter{},
		protocol.ProtocolOpenAIChatCompletions: chatCodecAdapter{},
		protocol.ProtocolGeminiGenerateContent: generatecontentCodecAdapter{},
		protocol.ProtocolQoderChat:             qoderchatCodecAdapter{},
	}
)

// StreamDecoder incrementally decodes upstream SSE frames into canonical sequential IR events.
type StreamDecoder interface {
	// Decode parses one upstream SSE frame into zero or more sequential IR events.
	// Blank keepalive frames or post-terminal sentinels return (nil, nil), whereas
	// malformed wire payloads return an error.
	Decode(frame sse.Frame) ([]ir.Event, error)
	// Close flushes any buffered trailing events when the upstream SSE reader
	// reaches EOF with a nil cause. When cause is non-nil or no protocol
	// termination arrived, Close closes open parts and appends a terminal EventError.
	Close(cause error) []ir.Event
}

// StreamEncoder incrementally encodes canonical sequential IR events into client SSE frames.
type StreamEncoder interface {
	// Encode converts one sequential IR event into zero or more client SSE frames
	// alongside any incurred information-loss records. Events supplied after a
	// terminal response or error event return nil slices without emitting frames.
	Encode(event ir.Event) ([]sse.Frame, []ir.Loss, error)
}

// clientCodec defines the request-decoding and response-encoding half required
// for a wire protocol to act on the client-facing side of an Exchange.
type clientCodec interface {
	// DecodeRequest parses a raw client request JSON body into a validated
	// ir.Request structure. Malformed JSON syntax, duplicate object keys, or IR
	// invariant violations return a non-nil error.
	DecodeRequest(body []byte) (*ir.Request, error)
	// EncodeResponse serializes a buffered ir.Response into client wire JSON bytes
	// using view for model naming and echoed fields, returning all incurred losses.
	// Nil responses or serialization failures return an error.
	EncodeResponse(resp *ir.Response, view ClientView) ([]byte, []ir.Loss, error)
	// NewStreamEncoder allocates a fresh per-stream encoder configured with model
	// naming and request echo fields from view. Callers must allocate an
	// independent StreamEncoder for each downstream client SSE stream.
	NewStreamEncoder(view ClientView) StreamEncoder
}

// upstreamCodec defines the request-encoding and response-decoding half required
// for a wire protocol to act on the upstream-facing side of an Exchange.
type upstreamCodec interface {
	// EncodeRequest serializes req into deterministic upstream wire JSON bytes
	// under dialect and returns all incurred information-loss records. Nil requests
	// or unsupported dialect settings return an error without mutating req.
	EncodeRequest(req *ir.Request, dialect Dialect) ([]byte, []ir.Loss, error)
	// DecodeResponse parses a buffered upstream JSON response body into a validated
	// ir.Response structure. Malformed JSON, upstream error envelopes, or invalid
	// tool-call arguments return a non-nil error.
	DecodeResponse(body []byte) (*ir.Response, error)
	// NewStreamDecoder allocates a fresh stateful per-stream decoder for upstream
	// SSE frames. Callers must allocate an independent StreamDecoder for each
	// upstream stream attempt so Sequencer and usage state stay isolated.
	NewStreamDecoder() StreamDecoder
}

// messagesCodecAdapter bridges messages.Codec concrete stream types to convert interfaces.
type messagesCodecAdapter struct {
	messages.Codec // embedded stateless Anthropic Messages codec
}

// responsesCodecAdapter bridges responses.Codec concrete stream types to convert interfaces.
type responsesCodecAdapter struct {
	responses.Codec // embedded stateless OpenAI Responses codec
}

// chatCodecAdapter bridges chat.Codec concrete stream types to convert interfaces.
type chatCodecAdapter struct {
	chat.Codec // embedded stateless OpenAI Chat Completions codec
}

// generatecontentCodecAdapter bridges generatecontent.Codec concrete stream types to convert interfaces.
type generatecontentCodecAdapter struct {
	generatecontent.Codec // embedded stateless Google Gemini GenerateContent codec
}

// qoderchatCodecAdapter bridges qoderchat.Codec concrete stream types to convert's upstreamCodec interface.
type qoderchatCodecAdapter struct {
	qoderchat.Codec // embedded stateless Qoder chat upstream codec
}

// lateToolSignature records a late thoughtSignature revision delivered on an
// already-closed explicit-ID tool call during upstream SSE decoding.
type lateToolSignature struct {
	partIndex  int    // zero-based sequential IR part index
	upstreamID string // non-empty upstream functionCall.id
	signature  string // revised non-empty thoughtSignature token
}

// lateToolSignatureDrainer is implemented by upstream stream decoders that can
// observe late continuity tokens on already-closed explicit-ID tool calls.
type lateToolSignatureDrainer interface {
	// drainLateToolSignatures returns and clears pending late tool-call signature
	// revisions accumulated since the previous drain call. Empty pending queues
	// return nil without allocating, and callers must drain after Decode and Close.
	drainLateToolSignatures() []lateToolSignature
}

// generatecontentStreamDecoderAdapter exposes generatecontent.StreamDecoder's
// late tool-call signature revisions to Stream and Buffer via lateToolSignatureDrainer.
type generatecontentStreamDecoderAdapter struct {
	*generatecontent.StreamDecoder // embedded stateful Gemini SSE stream decoder
}

// NewStreamDecoder allocates a fresh Anthropic Messages upstream StreamDecoder
// wrapped to satisfy convert.StreamDecoder. Callers must construct a separate
// decoder instance for each upstream Anthropic SSE stream attempt.
func (messagesCodecAdapter) NewStreamDecoder() StreamDecoder {
	return messages.NewStreamDecoder()
}

// NewStreamEncoder allocates a fresh Anthropic Messages client StreamEncoder
// bound to view and wrapped as convert.StreamEncoder. Callers must construct a
// separate encoder for each downstream client SSE stream.
func (messagesCodecAdapter) NewStreamEncoder(view ClientView) StreamEncoder {
	return messages.NewStreamEncoder(view)
}

// NewStreamDecoder allocates a fresh OpenAI Responses upstream StreamDecoder
// wrapped to satisfy convert.StreamDecoder. Callers must construct a separate
// decoder instance for each upstream Responses SSE stream attempt.
func (responsesCodecAdapter) NewStreamDecoder() StreamDecoder {
	return responses.NewStreamDecoder()
}

// NewStreamEncoder allocates a fresh OpenAI Responses client StreamEncoder
// bound to view and wrapped as convert.StreamEncoder. Callers must construct a
// separate encoder for each downstream client SSE stream.
func (responsesCodecAdapter) NewStreamEncoder(view ClientView) StreamEncoder {
	return responses.NewStreamEncoder(view)
}

// NewStreamDecoder allocates a fresh OpenAI Chat Completions upstream
// StreamDecoder wrapped as convert.StreamDecoder. Callers must construct a
// separate decoder for each upstream Chat SSE stream attempt.
func (chatCodecAdapter) NewStreamDecoder() StreamDecoder {
	return chat.NewStreamDecoder()
}

// NewStreamEncoder allocates a fresh OpenAI Chat Completions client
// StreamEncoder bound to view and wrapped as convert.StreamEncoder. Callers
// must construct a separate encoder for each downstream client SSE stream.
func (chatCodecAdapter) NewStreamEncoder(view ClientView) StreamEncoder {
	return chat.NewStreamEncoder(view)
}

// NewStreamDecoder allocates a fresh Google Gemini GenerateContent upstream
// StreamDecoder wrapped as convert.StreamDecoder. Callers must construct a
// separate decoder for each upstream Gemini SSE stream attempt.
func (generatecontentCodecAdapter) NewStreamDecoder() StreamDecoder {
	return generatecontentStreamDecoderAdapter{
		StreamDecoder: generatecontent.NewStreamDecoder(),
	}
}

// NewStreamEncoder allocates a fresh Google Gemini GenerateContent client
// StreamEncoder bound to view and wrapped as convert.StreamEncoder. Callers
// must construct a separate encoder for each downstream client SSE stream.
func (generatecontentCodecAdapter) NewStreamEncoder(view ClientView) StreamEncoder {
	return generatecontent.NewStreamEncoder(view)
}

// NewStreamDecoder allocates a fresh Qoder chat upstream StreamDecoder instance
// wrapped to satisfy convert.StreamDecoder. Callers must construct an
// independent decoder for each upstream Qoder SSE stream attempt so buffered
// text-tool prefixes and parallel slot state stay isolated.
func (qoderchatCodecAdapter) NewStreamDecoder() StreamDecoder {
	return qoderchat.NewStreamDecoder()
}

// drainLateToolSignatures drains pending late tool-call thoughtSignature
// revisions from the wrapped Gemini StreamDecoder and converts them into
// package-private lateToolSignature records. Empty pending queues return nil
// without allocating a new slice.
func (a generatecontentStreamDecoderAdapter) drainLateToolSignatures() []lateToolSignature {
	raw := a.DrainLateToolSignatures()
	if len(raw) == 0 {
		return nil
	}
	out := make([]lateToolSignature, len(raw))
	for i, r := range raw {
		out[i] = lateToolSignature{
			partIndex:  r.PartIndex,
			upstreamID: r.UpstreamID,
			signature:  r.Signature,
		}
	}
	return out
}

// HasClientCodec reports whether proto has a registered client-side codec in convert.
func HasClientCodec(proto protocol.ProtocolID) bool {
	_, ok := clientCodecs[proto]
	return ok
}

// HasUpstreamCodec reports whether proto has a registered upstream-side codec in convert.
func HasUpstreamCodec(proto protocol.ProtocolID) bool {
	_, ok := upstreamCodecs[proto]
	return ok
}

// DecodeRequest parses a raw JSON request body into a validated ir.Request
// using the registered client codec for proto. Unregistered protocol
// identifiers or malformed wire payloads return a descriptive error.
func DecodeRequest(proto protocol.ProtocolID, body []byte) (*ir.Request, error) {
	c, err := lookupClientCodec(proto)
	if err != nil {
		return nil, err
	}
	return c.DecodeRequest(body)
}

// DecodeUpstreamRequest parses a converted upstream request JSON body back into
// a validated ir.Request for Law Q4 semantic verification. Bidirectional
// protocols delegate to DecodeRequest, while ProtocolQoderChat decodes via
// qoderchat.DecodePayloadRequest without exposing leaf codec packages.
func DecodeUpstreamRequest(proto protocol.ProtocolID, body []byte) (*ir.Request, error) {
	if proto == protocol.ProtocolQoderChat {
		return qoderchat.DecodePayloadRequest(body)
	}
	return DecodeRequest(proto, body)
}

// EncodeRequest serializes req into deterministic target wire JSON bytes using
// the registered upstream codec for proto under dialect. Unregistered protocol
// identifiers or invalid IR trees return an error without partial output.
func EncodeRequest(proto protocol.ProtocolID, req *ir.Request, dialect Dialect) ([]byte, []ir.Loss, error) {
	u, err := lookupUpstreamCodec(proto)
	if err != nil {
		return nil, nil, err
	}
	return u.EncodeRequest(req, dialect)
}

// DecodeResponse parses a buffered upstream JSON response body into a validated
// ir.Response using the registered upstream codec for proto. Unregistered
// protocol identifiers or malformed response payloads return an error.
func DecodeResponse(proto protocol.ProtocolID, body []byte) (*ir.Response, error) {
	u, err := lookupUpstreamCodec(proto)
	if err != nil {
		return nil, err
	}
	return u.DecodeResponse(body)
}

// EncodeResponse serializes resp into deterministic client wire JSON bytes
// using the registered client codec for proto and view. Unregistered protocol
// identifiers or nil responses return a descriptive error.
func EncodeResponse(proto protocol.ProtocolID, resp *ir.Response, view ClientView) ([]byte, []ir.Loss, error) {
	c, err := lookupClientCodec(proto)
	if err != nil {
		return nil, nil, err
	}
	return c.EncodeResponse(resp, view)
}

// NewStreamDecoder allocates a fresh stateful upstream StreamDecoder instance
// for proto so callers can incrementally decode SSE frames. Unregistered
// protocol identifiers return a descriptive lookup error instead of a nil
// interface value.
func NewStreamDecoder(proto protocol.ProtocolID) (StreamDecoder, error) {
	u, err := lookupUpstreamCodec(proto)
	if err != nil {
		return nil, err
	}
	return u.NewStreamDecoder(), nil
}

// NewStreamEncoder allocates a fresh stateful client StreamEncoder for proto
// configured with view. Unregistered protocol identifiers return a descriptive
// lookup error instead of a nil interface value.
func NewStreamEncoder(proto protocol.ProtocolID, view ClientView) (StreamEncoder, error) {
	c, err := lookupClientCodec(proto)
	if err != nil {
		return nil, err
	}
	return c.NewStreamEncoder(view), nil
}

// lookupClientCodec retrieves the registered clientCodec for proto or returns an error.
func lookupClientCodec(proto protocol.ProtocolID) (clientCodec, error) {
	c, ok := clientCodecs[proto]
	if !ok {
		return nil, fmt.Errorf("convert: unsupported client protocol %q", proto)
	}
	return c, nil
}

// lookupUpstreamCodec retrieves the registered upstreamCodec for proto or returns an error.
func lookupUpstreamCodec(proto protocol.ProtocolID) (upstreamCodec, error) {
	u, ok := upstreamCodecs[proto]
	if !ok {
		return nil, fmt.Errorf("convert: unsupported upstream protocol %q", proto)
	}
	return u, nil
}
