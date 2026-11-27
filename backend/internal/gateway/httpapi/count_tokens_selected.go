package httpapi

import (
	"bytes"
	"context"
	"encoding/json/jsontext"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"

	gatewayadapter "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/requeststate"
	"github.com/TokenFlux/TokenRouter/internal/gateway/tokenestimate"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	gemininative "github.com/TokenFlux/TokenRouter/internal/upstream/gemini"
)

// count_tokens_selected.go dispatches token counts to the selected platform
// and translates Gemini results into Messages count responses.
// Messages count clients receive input_tokens even when the selected provider
// uses Gemini's native totalTokens response. Provider dispatch can instead write
// a local estimate or an unsupported response, so a handled nil error does not
// imply that a network count exchange occurred for every platform.
//
// Grok estimates Anthropic input locally via tokenestimate.Anthropic, while
// Gemini preparation converts the Messages body via protocol/convert with
// ReplayScheme and PlaceholderToken cleared, resolves its upstream model, and
// wraps the result under generateContentRequest for native countTokens. The
// output adapter reads successful totalTokens events and retains a local
// estimate for the runtime's OAuth-scope rejection path. Both paths write the
// Messages count shape, while other native failures use the shared Google error
// mapping translated back into an Anthropic error.

// messagesCountGoogleOutput translates native count output through the shared Google error adapter.
type messagesCountGoogleOutput struct {
	GoogleBoundary              // shared Google error mapping
	context        *gin.Context // client HTTP output context
	// fallback is estimated from the converted generation body before forwarding.
	// Count uses this value when the runtime substitutes counting after an OAuth-scope rejection.
	fallback int
}

// messagesCountSink validates native count data before writing the Messages count response.
type messagesCountSink struct {
	output *messagesCountGoogleOutput // Messages count output adapter
	// status is installed by Begin and checked by Emit. Callers begin the
	// exchange before emitting data so error-status payloads cannot be accepted as successful counts.
	status int
}

// ForwardSelectedCountTokens dispatches to the selected provider's count path without generation slots or usage
// submission.
// Missing target or parsed input returns an error. Qoder and Antigravity write
// unsupported output with nil error; Grok estimates locally, and other
// compatible platforms use the auxiliary count path. Remaining platforms
// require their matching bound executor, and missing execution support writes
// unavailable output with a terminal error.
func ForwardSelectedCountTokens(ctx context.Context, c *gin.Context, target *gatewayadapter.ExecutionProvider, parsed *requeststate.ParsedRequest, messages *MessagesExecutor, auxiliary *OpenAIAuxiliary, gemini *GeminiExecutor) error {
	if target == nil || parsed == nil {
		return errors.New("count_tokens target is unavailable")
	}
	switch platform := target.Record.Platform; {
	case platform == "qoder" || platform == "antigravity":
		WriteAnthropicError(c, http.StatusNotFound, "not_found_error", "", "count_tokens endpoint is not supported for this provider")
		return nil
	case platform == "grok":
		// This provider path supplies a local Messages estimate. Return the same
		// client count shape while leaving network execution and generation resources out of this branch.
		count, err := tokenestimate.Anthropic(parsed.Body.Bytes())
		if err != nil {
			WriteAnthropicError(c, http.StatusBadRequest, "invalid_request_error", "", "Failed to parse request body")
			return err
		}
		c.JSON(http.StatusOK, gin.H{"input_tokens": count})
		return nil
	case target.View().IsOpenAICompatible():
		if auxiliary != nil {
			return auxiliary.ForwardCountTokensAsAnthropic(ctx, c, target, parsed.Body.Bytes(), parsed.Model)
		}
	case platform == "anthropic":
		if messages != nil {
			return messages.ForwardCountTokens(ctx, c, target, parsed)
		}
	case platform == "gemini":
		if gemini != nil && gemini.Runtime != nil {
			return forwardGeminiMessagesCount(ctx, c, target, parsed, gemini)
		}
	}
	// An absent matching executor cannot supply a provider count. Write a local
	// unavailable response before returning the terminal dispatch error to the count loop.
	WriteAnthropicError(c, http.StatusServiceUnavailable, "api_error", "", "Token count service is unavailable")
	return errors.New("count_tokens executor is unavailable")
}

// forwardGeminiMessagesCount translates an Anthropic Messages count_tokens
// body into a Gemini generateContentRequest payload through convert.Exchange
// with ReplayScheme and PlaceholderToken cleared. Request conversion errors
// write an Anthropic HTTP 400 response, while valid payloads delegate native
// countTokens execution and OAuth scope fallback estimation to ForwardNative.
func forwardGeminiMessagesCount(
	ctx context.Context,
	c *gin.Context,
	target *gatewayadapter.ExecutionProvider,
	parsed *requeststate.ParsedRequest,
	executor *GeminiExecutor,
) error {
	rawClient := parsed.Body.Bytes()
	upstreamModel := gatewayadapter.ExecutionModelPolicy(target).UpstreamModel(ctx, parsed.Model)

	attributes, err := gatewayadapter.ResolveModelAttributes(
		ctx,
		target,
		upstreamModel,
		executor.Runtime.Attributes,
		"gemini_generate_content",
	)
	if err != nil {
		return err
	}
	attributes.Target.Endpoint = "count_tokens"
	adjusted := attributes.ForRequest(gatewayadapter.ModelRequestState(rawClient))
	dialect := gatewayadapter.ConversionDialect(ctx, target, adjusted, protocol.ProtocolGeminiGenerateContent)
	// Clear ReplayScheme and PlaceholderToken on count_tokens preflight payloads
	// so historical signatures and dummy placeholders never inflate token counts.
	dialect.ReplayScheme = ""
	dialect.PlaceholderToken = ""
	if !dialect.StripEffort && gatewayadapter.ExecutionModelPolicy(target).ShouldStripWireEffort(ctx, parsed.Model) {
		dialect.StripEffort = true
	}

	exchange, err := convert.NewExchange(rawClient, convert.Options{
		Route: convert.Route{
			Client:   protocol.ProtocolAnthropicMessages,
			Upstream: protocol.ProtocolGeminiGenerateContent,
			Dialect:  dialect,
		},
		ClientModel: parsed.Model,
		Runtime:     convert.DefaultRuntime(),
	})
	if err != nil {
		WriteAnthropicError(c, http.StatusBadRequest, "invalid_request_error", "", "Failed to parse request body")
		return err
	}
	geminiBody, err := exchange.UpstreamRequest()
	if err != nil {
		WriteAnthropicError(c, http.StatusBadRequest, "invalid_request_error", "", err.Error())
		return err
	}

	patchedBody, request, err := wrapGeminiCountTokensRequestBytes(geminiBody, upstreamModel)
	if err != nil {
		return err
	}
	output := &messagesCountGoogleOutput{
		GoogleBoundary: NewGoogleBoundary(c, executor.Runtime.Options, false),
		context:        c,
		fallback:       gemininative.EstimateGeminiCountTokens(patchedBody),
	}
	_, err = executor.Runtime.ForwardNative(ctx, output, target, parsed.Model, "countTokens", false, request)
	return err
}

// wrapGeminiCountTokensRequestBytes inserts "model":"models/<upstreamModel>"
// into geminiBody via ordered jsontext tokens and wraps the resulting object
// under {"generateContentRequest": ...} without float64 re-decoding. Both the
// patched inner object (used for scope-fallback token estimation) and the outer
// countTokens request bytes are returned.
func wrapGeminiCountTokensRequestBytes(geminiBody []byte, upstreamModel string) ([]byte, []byte, error) {
	// rawMember pairs one top-level JSON key with its verbatim raw value bytes.
	type rawMember struct {
		key string         // decoded member name
		val jsontext.Value // cloned raw JSON value
	}
	dec := jsontext.NewDecoder(bytes.NewReader(geminiBody), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return nil, nil, errors.New("gemini count_tokens payload must be a JSON object")
	}
	var members []rawMember
	for dec.PeekKind() != '}' {
		kTok, kErr := dec.ReadToken()
		if kErr != nil {
			return nil, nil, kErr
		}
		key := kTok.String()
		vRaw, vErr := dec.ReadValue()
		if vErr != nil {
			return nil, nil, vErr
		}
		if key == "model" {
			continue
		}
		members = append(members, rawMember{
			key: key,
			val: bytes.Clone(vRaw),
		})
	}
	if _, err := dec.ReadToken(); err != nil {
		return nil, nil, err
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return nil, nil, errors.New("trailing tokens after gemini count_tokens object")
	}

	qualifiedModel := "models/" + strings.TrimPrefix(strings.TrimSpace(upstreamModel), "models/")

	var innerBuf bytes.Buffer
	innerEnc := jsontext.NewEncoder(&innerBuf, jsontext.AllowInvalidUTF8(true))
	if err := innerEnc.WriteToken(jsontext.BeginObject); err != nil {
		return nil, nil, err
	}
	if err := innerEnc.WriteToken(jsontext.String("model")); err != nil {
		return nil, nil, err
	}
	if err := innerEnc.WriteToken(jsontext.String(qualifiedModel)); err != nil {
		return nil, nil, err
	}
	for _, m := range members {
		if err := innerEnc.WriteToken(jsontext.String(m.key)); err != nil {
			return nil, nil, err
		}
		if err := innerEnc.WriteValue(m.val); err != nil {
			return nil, nil, err
		}
	}
	if err := innerEnc.WriteToken(jsontext.EndObject); err != nil {
		return nil, nil, err
	}
	patchedBody := bytes.TrimSpace(innerBuf.Bytes())

	var outerBuf bytes.Buffer
	outerEnc := jsontext.NewEncoder(&outerBuf, jsontext.AllowInvalidUTF8(true))
	if err := outerEnc.WriteToken(jsontext.BeginObject); err != nil {
		return nil, nil, err
	}
	if err := outerEnc.WriteToken(jsontext.String("generateContentRequest")); err != nil {
		return nil, nil, err
	}
	if err := outerEnc.WriteValue(jsontext.Value(patchedBody)); err != nil {
		return nil, nil, err
	}
	if err := outerEnc.WriteToken(jsontext.EndObject); err != nil {
		return nil, nil, err
	}
	return patchedBody, bytes.TrimSpace(outerBuf.Bytes()), nil
}

// Count writes the estimate captured before native Gemini forwarding.
// The runtime-supplied count argument is ignored. Scope-error fallback therefore
// uses the adapter's converted generation-body estimate and writes HTTP 200
// with the Messages input_tokens field.
func (o *messagesCountGoogleOutput) Count(_ int) {
	o.context.JSON(http.StatusOK, gin.H{"input_tokens": o.fallback})
}

// GoogleError translates a native status and message into an Anthropic upstream error.
func (o *messagesCountGoogleOutput) GoogleError(status int, message string) error {
	return o.ClaudeError(status, "upstream_error", message)
}

// GeminiNativeUpstreamError maps the native error after optional OAuth envelope unwrapping.
// The supplied response provides its status and request ID identifies upstream
// evidence. GoogleBoundary handles error classification and returns the mapped failure to native execution.
func (o *messagesCountGoogleOutput) GeminiNativeUpstreamError(target *gatewayadapter.ExecutionProvider, response *http.Response, body []byte, requestID string, oauth bool) error {
	return o.GeminiMappedError(target, response.StatusCode, requestID, gemininative.UnwrapIfNeeded(oauth, body))
}

// Sink creates a native-output sink bound to this Messages count response adapter.
func (o *messagesCountGoogleOutput) Sink() upstream.OutputSink { return &messagesCountSink{output: o} }

// Begin records upstream status for subsequent native count data validation.
func (s *messagesCountSink) Begin(head upstream.OutputHead) error { s.status = head.Status; return nil }

// Emit writes nonempty native count data as a Messages input_tokens response.
// Empty data is ignored. HTTP error status, absent or nonnumeric totalTokens,
// and negative Int results write a bad-gateway error; other numeric values
// use gjson.Int conversion before output.
func (s *messagesCountSink) Emit(event upstream.OutputEvent) error {
	if len(event.Data) == 0 {
		return nil
	}
	// Native count output uses a different field name from Messages. Check its
	// status and numeric shape before the integer accessor supplies the client-facing count.
	count := gjson.GetBytes(event.Data, "totalTokens")
	if s.status >= http.StatusBadRequest || !count.Exists() || count.Type != gjson.Number || count.Int() < 0 {
		return s.output.ClaudeError(http.StatusBadGateway, "upstream_error", "Invalid token count response")
	}
	s.output.context.JSON(http.StatusOK, gin.H{"input_tokens": count.Int()})
	return nil
}
