package httpapi

import (
	"bytes"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	gatewayprovider "github.com/TokenFlux/TokenRouter/internal/gateway/provider"
	"github.com/TokenFlux/TokenRouter/internal/gateway/provider/googleforward"
	"github.com/TokenFlux/TokenRouter/internal/infra/httpclient"
	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/ops"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/routing/capability"
	"github.com/TokenFlux/TokenRouter/internal/server/httpx"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
	gemininative "github.com/TokenFlux/TokenRouter/internal/upstream/gemini"
)

// google_gemini_errors.go records Gemini upstream failures and selects native,
// Messages, or OpenAI-compatible error output.
// Gemini failures reach clients through native Google, Messages or OpenAI-
// compatible envelopes. Error observation retains the upstream status and
// request identity before final presentation. Native output can deliver an
// unwrapped upstream body, while compatibility output combines Google error
// categories, HTTP status and configured response rules.
//
// Messages mapping lets recognized Google categories supply selected status
// overrides before HTTP defaults fill missing fields. OpenAI compatibility has
// its own status precedence and permits a nonempty upstream message for pool
// providers. Commit markers and route-specific Responses output keep outer
// error handling from appending another response after a final error has
// already been presented.

// googleErrorWrittenKey deduplicates final errors after retries have ended, without
// treating a successful response terminal as a previously delivered error.
const googleErrorWrittenKey = "google_final_error_written"

// GeminiOutput holds the current HTTP request and fixed output options.
type GeminiOutput struct {
	GoogleOutput                       // synchronous request output
	Options      googleforward.Options // fixed error presentation settings
}

// GeminiCustomCodeSkippedError records the rejected upstream status, request
// identity and sanitized diagnostics, then invokes write once. The caller
// supplies its generic response writer and remains responsible for that
// writer's commit behavior.
func (s *GeminiOutput) GeminiCustomCodeSkippedError(provider *gatewayprovider.ExecutionProvider, upstreamStatus int, upstreamRequestID string, body []byte, write func()) error {
	c := s.Context

	upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(body)))
	upstreamDetail := s.Options.ErrorDetail(body)
	SetOpsUpstreamError(c, upstreamStatus, upstreamMsg, upstreamDetail)
	AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
		Platform: provider.Record.Platform,

		ProviderID: provider.Record.ID,

		ProviderName: provider.Record.Name,

		UpstreamStatusCode: upstreamStatus,

		UpstreamRequestID: upstreamRequestID,

		Kind: "http_error",

		Message: upstreamMsg,

		Detail: upstreamDetail,
	})
	write()
	if upstreamMsg == "" {
		return fmt.Errorf("gemini upstream error: %d (not in custom error codes)", upstreamStatus)
	}
	return fmt.Errorf("gemini upstream error: %d (not in custom error codes) message=%s", upstreamStatus, upstreamMsg)
}

// GeminiNativeUpstreamError unwraps an OAuth body when needed and writes it
// with the upstream status. Diagnostics are recorded before committing the
// response, and absent Content-Type defaults to JSON; the response remains
// caller-owned.
func (s *GeminiOutput) GeminiNativeUpstreamError(provider *gatewayprovider.ExecutionProvider, resp *http.Response, respBody []byte, requestID string, isOAuth bool) error {
	c := s.Context

	respBody = gemininative.UnwrapIfNeeded(isOAuth, respBody)
	upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(respBody)))
	upstreamDetail := s.Options.ErrorDetail(respBody)
	SetOpsUpstreamError(c, resp.StatusCode, upstreamMsg, upstreamDetail)
	AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
		Platform: provider.Record.Platform,

		ProviderID: provider.Record.ID,

		ProviderName: provider.Record.Name,

		UpstreamStatusCode: resp.StatusCode,

		UpstreamRequestID: requestID,

		Kind: "http_error",

		Message: upstreamMsg,

		Detail: upstreamDetail,
	})
	contentType := resp.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	MarkResponseCommitted(c)
	if !c.Writer.Written() {
		if beginGoogleError(c) {
			c.Data(resp.StatusCode, contentType, respBody)
		}
	} else {
		writeGoogleErrorBody(c, resp.StatusCode, respBody)
	}
	if upstreamMsg == "" {
		return fmt.Errorf("gemini upstream error: %d", resp.StatusCode)
	}
	return fmt.Errorf("gemini upstream error: %d message=%s", resp.StatusCode, upstreamMsg)
}

// GeminiMappedError presents a Gemini failure through the Messages envelope
// after recording sanitized upstream diagnostics. The provider and Gin context
// are required, and the final response is marked committed before output. A
// matching administrator rule supplies the client status, type and message
// before built-in mapping applies.
//
// Without a rule, recognized Google status fields supply an error type and
// selected status overrides. HTTP-status defaults fill the remaining fields;
// HTTP 400 can expose the sanitized upstream message, while authentication and
// permission failures use configured generic text. Committed event streams
// receive a terminal Anthropic SSE error frame instead of JSON, and the
// returned error retains diagnostic status and message for the caller.
func (s *GeminiOutput) GeminiMappedError(provider *gatewayprovider.ExecutionProvider, upstreamStatus int, upstreamRequestID string, body []byte) error {
	c := s.Context
	MarkResponseCommitted(c)
	statusCode, errType, errMsg, retErr := s.resolveMessagesMappedError(provider, upstreamStatus, upstreamRequestID, body)
	WriteAnthropicStreamError(c, statusCode, errType, "", errMsg, CommittedSSE(c), MarkOpsStreamError)
	return retErr
}

// resolveMessagesMappedError records Gemini Ops diagnostics and resolves the
// Anthropic Messages status, type, message, and returned diagnostic error
// without writing HTTP output. Administrator passthrough rules take precedence
// over body-derived Google category overrides and HTTP status defaults.
func (s *GeminiOutput) resolveMessagesMappedError(
	provider *gatewayprovider.ExecutionProvider,
	upstreamStatus int,
	upstreamRequestID string,
	body []byte,
) (int, string, string, error) {
	c := s.Context

	upstreamMsg := strings.TrimSpace(upstream.ExtractErrorMessage(body))
	upstreamMsg = logredact.SanitizeUpstreamQueries(upstreamMsg)
	upstreamDetail := ""
	if s.Options.LogErrorBody {
		maxBytes := s.Options.LogErrorBodyMaxBytes
		if maxBytes <= 0 {
			maxBytes = 2048
		}
		// Bound diagnostic retention when configured logging is enabled; the full
		// supplied body still participates in error-rule matching and category mapping.
		upstreamDetail = logredact.TruncateUTF8(string(body), maxBytes)
	}
	SetOpsUpstreamError(c, upstreamStatus, upstreamMsg, upstreamDetail)
	if provider != nil {
		AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
			Platform:           provider.Record.Platform,
			ProviderID:         provider.Record.ID,
			ProviderName:       provider.Record.Name,
			UpstreamStatusCode: upstreamStatus,
			UpstreamRequestID:  upstreamRequestID,
			Kind:               "http_error",
			Message:            upstreamMsg,
			Detail:             upstreamDetail,
		})
	}

	if s.Options.LogErrorBody {
		logging.LegacyPrintf("service.gemini_messages_compat", "[Gemini] upstream error %d: %s", upstreamStatus, logredact.TruncateLine(body, s.Options.LogErrorBodyMaxBytes))
	}

	platform := capability.PlatformGemini
	if provider != nil {
		platform = provider.Record.Platform
	}
	if status, errType, errMsg, matched := ApplyErrorPassthroughRule(
		c,
		platform,
		upstreamStatus,
		body,
		http.StatusBadGateway,
		"upstream_error",
		"Upstream request failed",
	); matched {
		diagMsg := upstreamMsg
		if diagMsg == "" {
			diagMsg = errMsg
		}
		if diagMsg == "" {
			return status, errType, errMsg, fmt.Errorf("upstream error: %d (passthrough rule matched)", upstreamStatus)
		}
		return status, errType, errMsg, fmt.Errorf("upstream error: %d (passthrough rule matched) message=%s", upstreamStatus, diagMsg)
	}

	var statusCode int
	var errType, errMsg string

	// Messages mapping fills body-derived category and status first; HTTP defaults
	// below complete fields left unset while preserving those supplied overrides.
	if mapped := MapGeminiErrorBodyToClaudeError(body); mapped != nil {
		errType = mapped.Type
		if mapped.Message != "" {
			errMsg = mapped.Message
		}
		if mapped.StatusCode > 0 {
			statusCode = mapped.StatusCode
		}
	}

	switch upstreamStatus {
	case 400:
		if statusCode == 0 {
			statusCode = http.StatusBadRequest
		}
		if errType == "" {
			errType = "invalid_request_error"
		}
		if errMsg == "" {
			if upstreamMsg != "" {
				errMsg = upstreamMsg
			} else {
				errMsg = "Invalid request"
			}
		}
	case 401:
		if statusCode == 0 {
			statusCode = http.StatusBadGateway
		}
		if errType == "" {
			errType = "authentication_error"
		}
		if errMsg == "" {
			errMsg = "Upstream authentication failed, please contact administrator"
		}
	case 403:
		if statusCode == 0 {
			statusCode = http.StatusBadGateway
		}
		if errType == "" {
			errType = "permission_error"
		}
		if errMsg == "" {
			errMsg = "Upstream access forbidden, please contact administrator"
		}
	case 404:
		if statusCode == 0 {
			statusCode = http.StatusNotFound
		}
		if errType == "" {
			errType = "not_found_error"
		}
		if errMsg == "" {
			errMsg = "Resource not found"
		}
	case 429:
		if statusCode == 0 {
			statusCode = http.StatusTooManyRequests
		}
		if errType == "" {
			errType = "rate_limit_error"
		}
		if errMsg == "" {
			errMsg = "Upstream rate limit exceeded, please retry later"
		}
	case 529:
		if statusCode == 0 {
			statusCode = http.StatusServiceUnavailable
		}
		if errType == "" {
			errType = "overloaded_error"
		}
		if errMsg == "" {
			errMsg = "Upstream service overloaded, please retry later"
		}
	case 500, 502, 503, 504:
		if statusCode == 0 {
			statusCode = http.StatusBadGateway
		}
		if errType == "" {
			switch upstreamStatus {
			case 504:
				errType = "timeout_error"
			case 503:
				errType = "overloaded_error"
			default:
				errType = "api_error"
			}
		}
		if errMsg == "" {
			errMsg = "Upstream service temporarily unavailable"
		}
	default:
		if statusCode == 0 {
			statusCode = http.StatusBadGateway
		}
		if errType == "" {
			errType = "upstream_error"
		}
		if errMsg == "" {
			errMsg = "Upstream request failed"
		}
	}

	if upstreamMsg == "" {
		return statusCode, errType, errMsg, fmt.Errorf("upstream error: %d", upstreamStatus)
	}
	return statusCode, errType, errMsg, fmt.Errorf("upstream error: %d message=%s", upstreamStatus, upstreamMsg)
}

// ClaudeError commits and writes the supplied Messages error envelope, then
// returns a message error. Callers invoke it for final presentation after
// classifying whether this attempt can still be retried.
func (s *GeminiOutput) ClaudeError(status int, errType, message string) error {
	c := s.Context
	if beginGoogleError(c) {
		WriteAnthropicStreamError(c, status, errType, "", message, c.Writer.Written(), MarkOpsStreamError)
	}
	return fmt.Errorf("%s", message)
}

// GoogleError commits a native Google envelope with the shared status-name
// mapping. It returns the supplied message as an error after writing, allowing
// execution to report the same failure to its caller.
func (s *GeminiOutput) GoogleError(status int, message string) error {
	WriteGoogleError(s.Context, status, message)
	return fmt.Errorf("%s", message)
}

// GeminiOpenAICompatMappedError records Gemini upstream diagnostics and
// chooses an OpenAI-compatible client error. A nonnil provider additionally
// contributes an Ops event and supplies the rule-matching platform. Missing
// provider metadata uses Gemini as the platform default. Administrator response
// rules take precedence over built-in message and status selection.
//
// Google error fields seed the compatible category and selected status
// overrides, then recognized HTTP statuses apply this path's own defaults. A
// nonempty upstream message can replace the generic default, and a pool
// provider exposes that message for every status at or above 400. The protocol
// argument selects Responses or Chat presentation after this decision.
func (s *GeminiOutput) GeminiOpenAICompatMappedError(
	provider *gatewayprovider.ExecutionProvider,
	upstreamStatus int,
	upstreamRequestID string,
	body []byte,
	compatProtocol gemininative.OpenAICompatProtocol,
) error {
	statusCode, errType, errMsg := s.resolveOpenAICompatMappedError(provider, upstreamStatus, upstreamRequestID, body)
	return s.GeminiOpenAICompatError(compatProtocol, statusCode, errType, errMsg)
}

// resolveOpenAICompatMappedError records Gemini Ops diagnostics and resolves
// the OpenAI-compatible status, type, and message without writing HTTP output.
// Administrator passthrough rules win first, while pool-mode providers expose
// sanitized upstream error messages for every HTTP status at or above 400.
func (s *GeminiOutput) resolveOpenAICompatMappedError(
	provider *gatewayprovider.ExecutionProvider,
	upstreamStatus int,
	upstreamRequestID string,
	body []byte,
) (int, string, string) {
	c := s.Context

	upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(body)))
	SetOpsUpstreamError(c, upstreamStatus, upstreamMsg, "")
	platform := capability.PlatformGemini
	if provider != nil {
		platform = provider.Record.Platform
		AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
			Platform:           provider.Record.Platform,
			ProviderID:         provider.Record.ID,
			ProviderName:       provider.Record.Name,
			UpstreamStatusCode: upstreamStatus,
			UpstreamRequestID:  upstreamRequestID,
			Kind:               "http_error",
			Message:            upstreamMsg,
		})
	}

	if status, errType, errMsg, matched := ApplyErrorPassthroughRule(
		c,
		platform,
		upstreamStatus,
		body,
		http.StatusBadGateway,
		"upstream_error",
		"Upstream request failed",
	); matched {
		return status, errType, errMsg
	}

	statusCode := http.StatusBadGateway
	errType := "upstream_error"
	errMsg := "Upstream request failed"
	// Compatible mapping starts from the body category; the HTTP branch below
	// has its own precedence for 404, 429 and overloaded upstream responses.
	if mapped := MapGeminiErrorBodyToClaudeError(body); mapped != nil {
		if mapped.Type != "" {
			errType = mapped.Type
		}
		if mapped.Message != "" {
			errMsg = mapped.Message
		}
		if mapped.StatusCode > 0 {
			statusCode = mapped.StatusCode
		}
	}

	switch upstreamStatus {
	case http.StatusBadRequest:
		if statusCode == http.StatusBadGateway {
			statusCode = http.StatusBadRequest
		}
		if errType == "upstream_error" {
			errType = "invalid_request_error"
		}
		if errMsg == "Upstream request failed" {
			errMsg = "Invalid request"
		}
	case http.StatusNotFound:
		statusCode = http.StatusNotFound
		if errType == "upstream_error" {
			errType = "not_found_error"
		}
		if errMsg == "Upstream request failed" {
			errMsg = "Resource not found"
		}
	case http.StatusTooManyRequests:
		statusCode = http.StatusTooManyRequests
		if errType == "upstream_error" {
			errType = "rate_limit_error"
		}
		if errMsg == "Upstream request failed" {
			errMsg = "Upstream rate limit exceeded, please retry later"
		}
	case 529:
		statusCode = http.StatusServiceUnavailable
		if errType == "upstream_error" {
			errType = "overloaded_error"
		}
		if errMsg == "Upstream request failed" {
			errMsg = "Upstream service overloaded, please retry later"
		}
	}

	if upstreamMsg != "" && errMsg == "Upstream request failed" {
		errMsg = upstreamMsg
	}
	// Pool failures expose their sanitized upstream message after built-in mapping,
	// helping clients distinguish pooled provider errors when no response rule matched.
	if provider != nil && provider.View().IsPoolMode() && upstreamStatus >= http.StatusBadRequest && upstreamMsg != "" {
		errMsg = upstreamMsg
	}
	return statusCode, errType, errMsg
}

// GeminiOpenAICompatError writes the envelope selected by the compatible
// client protocol. Responses writes JSON or a streaming error frame once
// beginGoogleError claims final error output; other values use ChatError, with
// the message returned as an error.
func (s *GeminiOutput) GeminiOpenAICompatError(
	protocol gemininative.OpenAICompatProtocol,
	status int,
	errType string,
	message string,
) error {
	c := s.Context

	if protocol == gemininative.OpenAICompatResponses {
		if beginGoogleError(c) {
			if c.Writer.Written() {
				DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, errType, errType, message, true, true)
			} else {
				c.JSON(status, gin.H{"error": gin.H{"code": errType, "message": message}})
			}
		}
		return fmt.Errorf("%s", message)
	}
	return s.ChatError(status, errType, message)
}

// ChatError writes the supplied OpenAI Chat error type and message as JSON. It
// returns a message error after presentation; this method itself does not
// install a response-commit marker.
func (s *GeminiOutput) ChatError(status int, errType, message string) error {
	c := s.Context
	if beginGoogleError(c) {
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, errType, "", message, c.Writer.Written(), true)
	}
	return fmt.Errorf("%s", message)
}

// beginGoogleError selects one final error for the active Google-backed response.
// Cancellation suppresses output to a departed client. A previous successful
// terminal does not prevent a later explicit provider error from being reported.
func beginGoogleError(c *gin.Context) bool {
	if c.GetBool(googleErrorWrittenKey) || (c.Request != nil && c.Request.Context().Err() != nil) {
		return false
	}
	c.Set(googleErrorWrittenKey, true)
	MarkResponseCommitted(c)
	return true
}

// writeGoogleErrorBody chooses JSON or native data-only SSE for final Google errors.
// Existing JSON error fields survive normalization; non-JSON responses become a
// safe Google envelope. A committed SSE response retains its HTTP status.
//
// Local error delivery excludes upstream timing and carries a terminal marker
// through the complete writer chain. This stops idle heartbeats atomically with
// accepted bytes, including errors that arrive after heartbeat-only HTTP commitment.
// Deduplication happens before delivery so a failed client write cannot trigger
// another attempt to present the same terminal from an outer error handler.
func writeGoogleErrorBody(c *gin.Context, status int, body []byte) {
	if !beginGoogleError(c) {
		return
	}
	if !CommittedSSE(c) {
		c.Data(status, "application/json; charset=utf-8", body)
		return
	}
	message := logredact.SanitizeUpstreamQueries(upstream.ExtractErrorMessage(body))
	if message == "" {
		message = http.StatusText(status)
	}
	value := gjson.GetBytes(body, "error")
	if !json.Valid(body) || !value.IsObject() {
		body, _ = json.Marshal(gin.H{"error": gin.H{"code": status, "message": message, "status": HTTPStatusToGoogleStatus(status)}})
	}
	// Complete the native error envelope without discarding provider details.
	// Sanitized messages prevent upstream query credentials from entering client errors.
	body, _ = sjson.SetBytes(body, "error.message", message)
	code := int(gjson.GetBytes(body, "error.code").Int())
	if code < 400 || code > 599 {
		code = status
	}
	body, _ = sjson.SetBytes(body, "error.code", code)
	if name := gjson.GetBytes(body, "error.status"); name.Type != gjson.String || name.String() == "" {
		body, _ = sjson.SetBytes(body, "error.status", HTTPStatusToGoogleStatus(code))
	}
	var compact bytes.Buffer
	_ = json.Compact(&compact, body)
	MarkOpsStreamFailure(c, "upstream_error", "", message, status)
	sink := NewResponseSink(c)
	sink.timing = nil
	if err := sink.Emit(upstream.OutputEvent{Data: []byte("data: " + compact.String() + "\n\n"), Terminal: true, Flush: true}); err != nil {
		_ = c.Error(err)
	}
}

// HTTPStatusToGoogleStatus returns the shared Google status name for an HTTP
// status.
func HTTPStatusToGoogleStatus(status int) string { return httpx.HTTPStatusToGoogleStatus(status) }

// CommittedSSE reports whether downstream HTTP 200 text/event-stream headers
// have already been committed on s.Context, including by waiting heartbeats.
// Nil receivers or absent Gin contexts return false safely.
func (s *GeminiOutput) CommittedSSE() bool {
	return s != nil && s.Context != nil && CommittedSSE(s.Context)
}

// ReadConvertedBody reads bounded upstream bytes using source's oversize error
// envelope and reports whether an oversize error response was already written.
// Callers retain ownership of closing body after this method returns.
func (s *GeminiOutput) ReadConvertedBody(body io.Reader, limit int64, source protocol.ProtocolID) ([]byte, bool, error) {
	tooLarge := OpenAIResponseTooLarge
	if source == protocol.ProtocolAnthropicMessages {
		tooLarge = AnthropicResponseTooLarge
	}
	raw, err := ReadUpstreamResponseBody(body, limit, s.Context, func(c *gin.Context) {
		if beginGoogleError(c) {
			tooLarge(c)
		}
	})
	return raw, errors.Is(err, httpclient.ErrResponseBodyTooLarge), err
}

// MapGeminiStreamFailure records upstream failure diagnostics once on s.Context
// and resolves client status, type, code, message, and diagnostic error without
// writing HTTP output. Custom-code misses record http_error diagnostics and
// resolve to a generic 500 api_error message.
func (s *GeminiOutput) MapGeminiStreamFailure(
	provider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	upstreamStatus int,
	upstreamRequestID string,
	body []byte,
	customSkipped bool,
) (int, string, string, string, error) {
	c := s.Context
	if customSkipped {
		upstreamMsg := logredact.SanitizeUpstreamQueries(strings.TrimSpace(upstream.ExtractErrorMessage(body)))
		upstreamDetail := s.Options.ErrorDetail(body)
		SetOpsUpstreamError(c, upstreamStatus, upstreamMsg, upstreamDetail)
		if provider != nil {
			AppendOpsUpstreamError(c, ops.OpsUpstreamErrorEvent{
				Platform:           provider.Record.Platform,
				ProviderID:         provider.Record.ID,
				ProviderName:       provider.Record.Name,
				UpstreamStatusCode: upstreamStatus,
				UpstreamRequestID:  upstreamRequestID,
				Kind:               "http_error",
				Message:            upstreamMsg,
				Detail:             upstreamDetail,
			})
		}
		code := ""
		if source == protocol.ProtocolOpenAIResponses {
			code = "api_error"
		}
		var retErr error
		if upstreamMsg == "" {
			retErr = fmt.Errorf("gemini upstream error: %d (not in custom error codes)", upstreamStatus)
		} else {
			retErr = fmt.Errorf("gemini upstream error: %d (not in custom error codes) message=%s", upstreamStatus, upstreamMsg)
		}
		return http.StatusInternalServerError, "api_error", code, "Upstream gateway error", retErr
	}

	if source == protocol.ProtocolAnthropicMessages {
		st, tp, msg, retErr := s.resolveMessagesMappedError(provider, upstreamStatus, upstreamRequestID, body)
		return st, tp, "", msg, retErr
	}
	st, tp, msg := s.resolveOpenAICompatMappedError(provider, upstreamStatus, upstreamRequestID, body)
	code := ""
	if source == protocol.ProtocolOpenAIResponses {
		code = tp
	}
	return st, tp, code, msg, fmt.Errorf("%s", msg)
}

// MapAntigravityFailure delegates single-pass Antigravity diagnostic recording,
// optional body logging, and Messages passthrough-rule resolution to an
// AntigravityOutput sharing s.GoogleOutput and s.Options. Callers invoke this
// before writing terminal SSE errors so CommittedSSE branches match uncommitted rules.
func (s *GeminiOutput) MapAntigravityFailure(
	provider *gatewayprovider.ExecutionProvider,
	source protocol.ProtocolID,
	upstreamStatus int,
	upstreamRequestID string,
	body []byte,
) (int, string, string, string, error) {
	ag := AntigravityOutput{GoogleOutput: s.GoogleOutput, Options: s.Options}
	return ag.MapAntigravityFailure(provider, source, upstreamStatus, upstreamRequestID, body)
}

// WriteConvertedError writes an uncommitted client-protocol HTTP JSON error on
// s.Context or promotes to an untimed terminal SSE error when CommittedSSE is
// true. Deduplication through beginGoogleError prevents duplicate error writes.
func (s *GeminiOutput) WriteConvertedError(
	source protocol.ProtocolID,
	originalModel string,
	status int,
	errType, code, message string,
) {
	if s == nil || s.Context == nil {
		return
	}
	if CommittedSSE(s.Context) {
		s.WriteLocalTerminalError(source, originalModel, status, errType, code, message)
		return
	}
	if !beginGoogleError(s.Context) {
		return
	}
	switch source {
	case protocol.ProtocolAnthropicMessages:
		WriteAnthropicStreamError(s.Context, status, errType, "", message, false, MarkOpsStreamError)
	case protocol.ProtocolOpenAIResponses:
		errCode := code
		if errCode == "" {
			errCode = errType
		}
		// responsesErrorBody formats the code and message fields of an uncommitted Responses HTTP error.
		type responsesErrorBody struct {
			Code    string `json:"code"`    // Responses error code
			Message string `json:"message"` // client-safe error message
		}
		// responsesErrorEnvelope wraps responsesErrorBody under a top-level "error" key.
		type responsesErrorEnvelope struct {
			Error responsesErrorBody `json:"error"` // top-level Responses error object
		}
		encoded, err := jsonv2.Marshal(responsesErrorEnvelope{
			Error: responsesErrorBody{Code: errCode, Message: message},
		})
		if err != nil {
			encoded = []byte(`{"error":{"code":"upstream_error","message":"Upstream request failed"}}`)
		}
		s.Context.Data(status, "application/json; charset=utf-8", encoded)
	default:
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(s.Context, status, errType, code, message, false, true)
	}
}

// WriteLocalTerminalError writes an untimed client-protocol terminal SSE error
// onto s.Context, emitting synthetic response.failed frames on Responses
// streams. Deduplication via beginGoogleError ensures at most one final error
// reaches the active client connection.
func (s *GeminiOutput) WriteLocalTerminalError(
	source protocol.ProtocolID,
	originalModel string,
	status int,
	errType, code, message string,
) {
	if s == nil || s.Context == nil || !beginGoogleError(s.Context) {
		return
	}
	c := s.Context
	switch {
	case source == protocol.ProtocolAnthropicMessages:
		DefaultOpenAIErrorOutput().WriteAnthropicStreamingError(c, status, errType, message, true)
	case source == protocol.ProtocolOpenAIResponses || InboundIsResponses(c):
		MarkOpsStreamFailure(c, errType, code, message, status)
		if WriteResponsesFailedSSE(c, errType, code, message, ErrorRequestID(c), firstNonBlankModel(originalModel, ErrorRequestModel(c))) {
			return
		}
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, errType, code, message, true, false)
	default:
		DefaultOpenAIErrorOutput().WriteStreamingErrorWithCode(c, status, errType, code, message, true, true)
	}
}

// WriteRawTerminalSSE records stream error diagnostics once and delivers
// pre-encoded terminal SSE frame bytes through the untimed terminal barrier on
// s.Context. Nil contexts, cancelled requests, or writers without http.Flusher
// return false.
func (s *GeminiOutput) WriteRawTerminalSSE(
	source protocol.ProtocolID,
	status int,
	errType, code, message string,
	data []byte,
) bool {
	if s == nil || s.Context == nil || !beginGoogleError(s.Context) {
		return false
	}
	if source == protocol.ProtocolAnthropicMessages {
		MarkOpsStreamError(s.Context, errType, message, status)
	} else {
		MarkOpsStreamFailure(s.Context, errType, code, message, status)
	}
	return emitLocalTerminalSSE(s.Context, data)
}
