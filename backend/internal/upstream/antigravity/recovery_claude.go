package antigravity

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/infra/telemetry"
	logger "github.com/TokenFlux/TokenRouter/internal/infra/telemetry/logging"
	"github.com/TokenFlux/TokenRouter/internal/pkg/logredact"
	"github.com/TokenFlux/TokenRouter/internal/protocol/convert"
	googlewire "github.com/TokenFlux/TokenRouter/internal/protocol/google"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
	"github.com/TokenFlux/TokenRouter/internal/upstream"
)

// recovery_claude.go retries rejected Antigravity Claude exchanges with staged
// signature degradation and thinking-budget rectification.
// A rejected Claude conversion can retry with less signature-sensitive content
// before attempting a separate budget correction. Each signature stage starts
// from a detached IR clone of the original Exchange so tool degradation does
// not mutate the original ToolMap or ReplayScheme used for response conversion.
//
// Thinking-only recovery precedes thinking-and-tool recovery, and an error that
// leaves signature classification stops further degradation so budget
// rectification can inspect the latest HTTP 400 failure. Budget rectification
// skips adaptive thinking requests and raises undersized output caps.

// ClaudeIRRecoveryInput supplies the attempt Exchange, routing identity, and
// wire preparation inputs for IR-backed Claude signature and budget recovery.
type ClaudeIRRecoveryInput struct {
	ProviderID   int64  // selected recovery provider identifier
	ProviderName string // provider display name for diagnostics
	Prefix       string // gateway log prefix string
	// ProjectID supplies the v1internal GCP project identifier required by
	// WrapV1InternalRequest; blank values skip retry wrapping with ErrProjectIDRequired.
	ProjectID string
	Model     string // mapped upstream Claude model identifier
	// IdentityPatchPrompt supplies the operator identity_patch_prompt override
	// passed to InjectIdentityPatchToGeminiRequestWithPrompt; empty strings use
	// the default Antigravity identity prompt.
	IdentityPatchPrompt string
	// Exchange holds the original per-attempt converter whose cached ToolMap and
	// Dialect remain untouched for response tool restoration and carrier sealing.
	Exchange *convert.Exchange
	// InitialGeminiBody optionally supplies the initial unpatched Gemini request
	// bytes from Exchange.UpstreamRequest so unchanged stages skip duplicate sends.
	InitialGeminiBody []byte
}

// ClaudeIRRecoveryOptions supplies recovery predicates, retry dispatch, budget
// rectification bounds, and diagnostic callbacks for RecoverClaudeIR.
type ClaudeIRRecoveryOptions struct {
	// Retry dispatches a rebuilt v1internal request body and returns an open
	// response on nil error; failed responses are captured and closed by recovery.
	Retry func([]byte) (*http.Response, error)
	// SignatureEnabled reports whether staged signature degradation is permitted
	// for ctx; nil disables both thinking-removal and tool-downgrade retries.
	SignatureEnabled func(context.Context) bool
	// BudgetEnabled reports whether thinking budget rectification is permitted for
	// ctx after non-signature HTTP 400 errors; nil disables budget retries.
	BudgetEnabled func(context.Context) bool
	// LogConfig reports whether diagnostic error-body logging is enabled alongside
	// its UTF-8 byte cap; nil leaves diagnostic body logging disabled.
	LogConfig func() (bool, int)
	// ErrorDetail formats bounded diagnostic body text for RetryObservation events
	// when present; nil leaves observation Detail fields empty.
	ErrorDetail func([]byte) string
	// ReadErrorBody captures error bytes from an unsuccessful HTTP response; nil
	// falls back to a bounded 8 KiB read from resp.Body.
	ReadErrorBody func(*http.Response) []byte
	// Observe records synchronous RetryObservation events for Ops diagnostics when
	// configured; nil skips observation callbacks without affecting retry selection.
	Observe func(RetryObservation)
	// IsBudgetConstraint reports whether extracted upstream error wording matches
	// an eligible thinking budget constraint failure; nil disables budget matching.
	IsBudgetConstraint func(string) bool
	// BudgetTokens, MinMaxTokens, and MaxTokens configure thinking budget
	// rectification: BudgetTokens sets the replacement thinkingBudget, while output
	// caps missing or below MinMaxTokens rise to MaxTokens.
	BudgetTokens int // rectified thinkingBudget token allowance
	MinMaxTokens int // output token floor triggering MaxTokens override
	MaxTokens    int // replacement maxOutputTokens cap when below floor
	// TruncateForLog formats bounded HTTP 429 retry error bodies for log lines
	// when body logging is active; nil falls back to raw string conversion.
	TruncateForLog func([]byte, int) string
	// TruncateString bounds UTF-8 detail text under LogConfig's byte limit when
	// body logging is active; nil leaves extracted detail text unbounded.
	TruncateString func(string, int) string
}

// RecoverClaudeIR retries rejected Antigravity Claude exchanges by re-encoding
// detached IR clones through input.Exchange,
// InjectIdentityPatchToGeminiRequestWithPrompt, CleanGeminiRequest, and
// WrapV1InternalRequest. Successful replacement responses return with their
// body open while input.Exchange retains its original ToolMap and ReplayScheme
// for downstream response conversion and CallMemory sealing.
//
// Signature-related HTTP 400 responses try thinking-only degradation before
// thinking-and-tool text degradation, stopping early if a retry leaves
// signature classification so subsequent non-signature HTTP 400 budget
// constraint checks can inspect the latest failure. Budget rectification skips
// adaptive thinking requests and raises undersized output limits to MaxTokens.
func RecoverClaudeIR(
	ctx context.Context,
	input ClaudeIRRecoveryInput,
	resp *http.Response,
	options ClaudeIRRecoveryOptions,
) *http.Response {
	if resp == nil || resp.StatusCode < http.StatusBadRequest || input.Exchange == nil {
		return resp
	}
	var respBody []byte
	if options.ReadErrorBody != nil {
		respBody = options.ReadErrorBody(resp)
	} else if resp.Body != nil {
		respBody, _ = io.ReadAll(io.LimitReader(resp.Body, 8<<10))
	}
	if resp.Body != nil {
		// Close the consumed network body immediately before reconstructing an
		// in-memory reader so replaced failure responses never leak connections.
		_ = resp.Body.Close()
	}
	resp.Body = io.NopCloser(bytes.NewReader(respBody))

	// Select staged IR degradation on signature-related HTTP 400 responses while
	// keeping unrelated bad requests untouched for budget or terminal handling.
	if resp.StatusCode == http.StatusBadRequest &&
		isClaudeRecoverySignatureError(respBody) &&
		options.SignatureEnabled != nil &&
		options.SignatureEnabled(ctx) {
		upstreamMsg := logredact.SanitizeUpstreamQueries(extractClaudeRecoveryErrorMessage(respBody))
		var logBody bool
		var maxBytes int
		if options.LogConfig != nil {
			logBody, maxBytes = options.LogConfig()
		}
		upstreamDetail := ""
		if options.ErrorDetail != nil {
			upstreamDetail = options.ErrorDetail(respBody)
		}
		if options.Observe != nil {
			options.Observe(RetryObservation{
				ProviderID:         input.ProviderID,
				ProviderName:       input.ProviderName,
				UpstreamStatusCode: resp.StatusCode,
				UpstreamRequestID:  resp.Header.Get("x-request-id"),
				Kind:               "signature_error",
				Message:            upstreamMsg,
				Detail:             upstreamDetail,
			})
		}

		// Try thinking-only removal ahead of tool-block downgrading so structured
		// functionCall and functionResponse turns survive whenever possible.
		stages := []struct {
			name   string                                                                     // diagnostic recovery stage label
			mutate func(*ir.Request, *convert.Dialect, *convert.ReencodeEditor) (bool, error) // detached IR mutation stage
		}{
			{name: "thinking-only", mutate: stripIRThinkingForClaudeRecovery},
			{name: "thinking+tools", mutate: stripIRThinkingAndToolsForClaudeRecovery},
		}

		lastGeminiBody := input.InitialGeminiBody
		for _, stage := range stages {
			reencodedGemini, stageLosses, changed, txErr := input.Exchange.ReencodeUpstreamLosses(stage.mutate)
			if txErr != nil || !changed || (len(lastGeminiBody) > 0 && bytes.Equal(reencodedGemini, lastGeminiBody)) {
				continue
			}
			lastGeminiBody = reencodedGemini

			wrappedRetryBody, wrapErr := wrapClaudeRecoveryGeminiBody(
				input.ProjectID,
				input.Model,
				input.IdentityPatchPrompt,
				reencodedGemini,
			)
			if wrapErr != nil || options.Retry == nil {
				continue
			}

			// Adopt the degraded request's remapped losses before dispatching the
			// replacement attempt so retry attempts and final usage reflect the
			// actually accepted wire payload without double-counting prior stages.
			input.Exchange.ReplaceRequestLosses(stageLosses)
			telemetry.ReplaceProtocolConversion(ctx, upstream.ExchangeConversion(input.Exchange))

			logger.LegacyPrintf("service.antigravity_gateway", "Antigravity provider %d: detected signature-related 400, retrying once (%s)", input.ProviderID, stage.name)

			retryResp, retryErr := options.Retry(wrappedRetryBody)
			if retryErr != nil {
				if options.Observe != nil {
					options.Observe(RetryObservation{
						ProviderID:         input.ProviderID,
						ProviderName:       input.ProviderName,
						UpstreamStatusCode: 0,
						Kind:               "signature_retry_request_error",
						Message:            logredact.SanitizeUpstreamQueries(retryErr.Error()),
					})
				}
				logger.LegacyPrintf("service.antigravity_gateway", "Antigravity provider %d: signature retry request failed (%s): %v", input.ProviderID, stage.name, retryErr)
				continue
			}

			if retryResp.StatusCode < http.StatusBadRequest {
				_ = resp.Body.Close()
				return retryResp
			}

			// Bound failed signature-retry body reads to eight KiB and close the
			// retry network stream before rebuilding an in-memory failure response.
			retryBody, _ := io.ReadAll(io.LimitReader(retryResp.Body, 8<<10))
			_ = retryResp.Body.Close()
			if retryResp.StatusCode == http.StatusTooManyRequests && options.TruncateForLog != nil {
				retryBaseURL := ""
				if retryResp.Request != nil && retryResp.Request.URL != nil {
					retryBaseURL = retryResp.Request.URL.Scheme + "://" + retryResp.Request.URL.Host
				}
				logger.LegacyPrintf("service.antigravity_gateway", "%s status=429 rate_limited base_url=%s retry_stage=%s body=%s", input.Prefix, retryBaseURL, stage.name, options.TruncateForLog(retryBody, 200))
			}

			kind := "signature_retry"
			if strings.TrimSpace(stage.name) != "" {
				kind = "signature_retry_" + strings.ReplaceAll(stage.name, "+", "_")
			}
			retryUpstreamMsg := logredact.SanitizeUpstreamQueries(extractClaudeRecoveryErrorMessage(retryBody))
			retryUpstreamDetail := ""
			if logBody && options.TruncateString != nil {
				retryUpstreamDetail = options.TruncateString(string(retryBody), maxBytes)
			}
			if options.Observe != nil {
				options.Observe(RetryObservation{
					ProviderID:         input.ProviderID,
					ProviderName:       input.ProviderName,
					UpstreamStatusCode: retryResp.StatusCode,
					UpstreamRequestID:  retryResp.Header.Get("x-request-id"),
					Kind:               kind,
					Message:            retryUpstreamMsg,
					Detail:             retryUpstreamDetail,
				})
			}

			_ = resp.Body.Close()
			respBody = retryBody
			resp = &http.Response{
				StatusCode: retryResp.StatusCode,
				Header:     retryResp.Header.Clone(),
				Body:       io.NopCloser(bytes.NewReader(retryBody)),
			}

			// Stop degrading history once the retry leaves signature classification so
			// budget rectification below can inspect a newly surfaced constraint 400.
			if retryResp.StatusCode != http.StatusBadRequest || !isClaudeRecoverySignatureError(retryBody) {
				break
			}
		}
	}

	// Evaluate non-adaptive thinking budget rectification on non-signature HTTP 400
	// responses, including constraint errors surfaced after signature recovery.
	if resp.StatusCode == http.StatusBadRequest && respBody != nil && !isClaudeRecoverySignatureError(respBody) {
		errMsg := extractClaudeRecoveryErrorMessage(respBody)
		if options.IsBudgetConstraint != nil &&
			options.IsBudgetConstraint(errMsg) &&
			options.BudgetEnabled != nil &&
			options.BudgetEnabled(ctx) {
			detail := ""
			if options.ErrorDetail != nil {
				detail = options.ErrorDetail(respBody)
			}
			if options.Observe != nil {
				options.Observe(RetryObservation{
					ProviderID:         input.ProviderID,
					ProviderName:       input.ProviderName,
					UpstreamStatusCode: resp.StatusCode,
					UpstreamRequestID:  resp.Header.Get("x-request-id"),
					Kind:               "budget_constraint_error",
					Message:            errMsg,
					Detail:             detail,
				})
			}

			reencodedBudget, budgetLosses, changed, txErr := input.Exchange.ReencodeUpstreamLosses(func(req *ir.Request, dialect *convert.Dialect, _ *convert.ReencodeEditor) (bool, error) {
				return applyIRBudgetRectification(req, dialect, options.BudgetTokens, options.MinMaxTokens, options.MaxTokens), nil
			})
			if txErr == nil && changed && options.Retry != nil {
				wrappedBudget, wrapErr := wrapClaudeRecoveryGeminiBody(
					input.ProjectID,
					input.Model,
					input.IdentityPatchPrompt,
					reencodedBudget,
				)
				if wrapErr == nil {
					// Adopt the budget-rectified request losses before dispatching the
					// retry so final conversion diagnostics match the sent payload.
					input.Exchange.ReplaceRequestLosses(budgetLosses)
					telemetry.ReplaceProtocolConversion(ctx, upstream.ExchangeConversion(input.Exchange))
					logger.LegacyPrintf("service.antigravity_gateway", "Antigravity provider %d: detected budget_tokens constraint error, retrying with rectified budget (budget_tokens=%d, max_tokens=%d)", input.ProviderID, options.BudgetTokens, options.MaxTokens)
					retryResp, retryErr := options.Retry(wrappedBudget)
					if retryErr == nil {
						if retryResp.StatusCode < http.StatusBadRequest {
							_ = resp.Body.Close()
							return retryResp
						}
						var retryBody []byte
						if options.ReadErrorBody != nil {
							retryBody = options.ReadErrorBody(retryResp)
						} else if retryResp.Body != nil {
							retryBody, _ = io.ReadAll(io.LimitReader(retryResp.Body, 8<<10))
						}
						_ = retryResp.Body.Close()
						_ = resp.Body.Close()
						respBody = retryBody
						resp = &http.Response{
							StatusCode: retryResp.StatusCode,
							Header:     retryResp.Header.Clone(),
							Body:       io.NopCloser(bytes.NewReader(retryBody)),
						}
					} else {
						logger.LegacyPrintf("service.antigravity_gateway", "Antigravity provider %d: budget rectifier retry failed: %v", input.ProviderID, retryErr)
					}
				}
			}
		}
	}

	if resp.StatusCode >= http.StatusBadRequest {
		resp.Body = io.NopCloser(bytes.NewReader(respBody))
	}
	return resp
}

// stripIRThinkingForClaudeRecovery disables top-level reasoning, clears any
// tool-call replay tokens, converts readable historical reasoning segments to
// plain text parts, removes redacted reasoning parts, and records recovery-stage
// ir.Loss entries while keeping source part coordinates aligned through edit.
// Messages emptied by reasoning removal receive a synthetic "(content removed)"
// placeholder part.
func stripIRThinkingForClaudeRecovery(req *ir.Request, _ *convert.Dialect, edit *convert.ReencodeEditor) (bool, error) {
	if req == nil {
		return false, nil
	}
	changed := false
	if req.Reasoning != nil {
		req.Reasoning = nil
		changed = true
	}

	for m := range req.Messages {
		msg := &req.Messages[m]
		if len(msg.Parts) == 0 {
			continue
		}
		filtered := make([]ir.Part, 0, len(msg.Parts))
		sourceParts := make([]convert.ReencodePartSource, 0, len(msg.Parts))
		modifiedMsg := false
		for p, part := range msg.Parts {
			switch part.Kind {
			case ir.PartReasoning:
				modifiedMsg = true
				if part.Cache != nil {
					// Record cache_mark_dropped when removing or inlining a cached
					// reasoning block so the dropped breakpoint remains accounted for.
					edit.RecordPartLoss(m, p, ir.LossCacheMarkDropped)
				}
				if part.Reasoning != nil {
					hasToken := part.Reasoning.Token != nil && part.Reasoning.Token.Data != ""
					if hasToken || part.Reasoning.Redacted {
						edit.RecordPartLoss(m, p, ir.LossReplayTokenDropped)
					}
					if !part.Reasoning.Redacted {
						if text := strings.Join(part.Reasoning.Segments, ""); text != "" {
							filtered = append(filtered, ir.Part{
								Kind: ir.PartText,
								Text: text,
							})
							sourceParts = append(sourceParts, convert.ReencodePartSource{Part: p})
							edit.RecordPartLoss(m, p, ir.LossReasoningTextInlined)
						} else if !hasToken {
							edit.RecordPartLoss(m, p, ir.LossReasoningTextDropped)
						}
					}
				}
			case ir.PartToolCall:
				if part.Call != nil && part.Call.Token != nil {
					if part.Call.Token.Data != "" {
						edit.RecordPartLoss(m, p, ir.LossReplayTokenDropped)
					}
					callCopy := *part.Call
					callCopy.Token = nil
					part.Call = &callCopy
					// Mark the message modified so tool-only turns write back the cleared
					// call copy rather than leaving the rejected signature on msg.Parts.
					modifiedMsg = true
				}
				filtered = append(filtered, part)
				sourceParts = append(sourceParts, convert.ReencodePartSource{Part: p})
			default:
				filtered = append(filtered, part)
				sourceParts = append(sourceParts, convert.ReencodePartSource{Part: p})
			}
		}
		if !modifiedMsg {
			continue
		}
		if len(filtered) == 0 {
			// Supply non-empty placeholder text when every block in the turn was
			// removed so Gemini contents legalization retains turn structure.
			filtered = append(filtered, ir.Part{
				Kind: ir.PartText,
				Text: "(content removed)",
			})
			sourceParts = append(sourceParts, convert.ReencodePartSource{Part: -1})
		}
		if edit != nil {
			edit.SetMessageParts(m, filtered, sourceParts)
		} else {
			msg.Parts = filtered
		}
		changed = true
	}
	return changed, nil
}

// stripIRThinkingAndToolsForClaudeRecovery disables top-level reasoning,
// downgrades historical reasoning, tool calls, and tool results into readable
// text parts for the second Claude signature recovery stage, and records
// recovery-stage ir.Loss entries while keeping source part coordinates aligned
// through edit. Dedicated RoleTool messages convert to RoleUser while retaining
// embedded media attachments.
func stripIRThinkingAndToolsForClaudeRecovery(req *ir.Request, _ *convert.Dialect, edit *convert.ReencodeEditor) (bool, error) {
	if req == nil {
		return false, nil
	}
	changed := false
	if req.Reasoning != nil {
		req.Reasoning = nil
		changed = true
	}

	for m := range req.Messages {
		msg := &req.Messages[m]
		if len(msg.Parts) == 0 {
			continue
		}
		filtered := make([]ir.Part, 0, len(msg.Parts))
		sourceParts := make([]convert.ReencodePartSource, 0, len(msg.Parts))
		modifiedMsg := false
		for p, part := range msg.Parts {
			switch part.Kind {
			case ir.PartReasoning:
				modifiedMsg = true
				if part.Cache != nil {
					// Record cache_mark_dropped when removing or inlining a cached
					// reasoning block so the dropped breakpoint remains accounted for.
					edit.RecordPartLoss(m, p, ir.LossCacheMarkDropped)
				}
				if part.Reasoning != nil {
					hasToken := part.Reasoning.Token != nil && part.Reasoning.Token.Data != ""
					if hasToken || part.Reasoning.Redacted {
						edit.RecordPartLoss(m, p, ir.LossReplayTokenDropped)
					}
					if !part.Reasoning.Redacted {
						if text := strings.Join(part.Reasoning.Segments, ""); text != "" {
							filtered = append(filtered, ir.Part{
								Kind: ir.PartText,
								Text: text,
							})
							sourceParts = append(sourceParts, convert.ReencodePartSource{Part: p})
							edit.RecordPartLoss(m, p, ir.LossReasoningTextInlined)
						} else if !hasToken {
							edit.RecordPartLoss(m, p, ir.LossReasoningTextDropped)
						}
					}
				}
			case ir.PartToolCall:
				if part.Call == nil {
					continue
				}
				if part.Call.Token != nil && part.Call.Token.Data != "" {
					edit.RecordPartLoss(m, p, ir.LossReplayTokenDropped)
				}
				recordInlinedToolHistoryLosses(edit, m, p, part)
				modifiedMsg = true
				filtered = append(filtered, ir.Part{
					Kind: ir.PartText,
					Text: formatDowngradedIRToolCall(part.Call),
				})
				sourceParts = append(sourceParts, convert.ReencodePartSource{Part: p})
			case ir.PartToolResult:
				if part.Result == nil {
					continue
				}
				recordInlinedToolHistoryLosses(edit, m, p, part)
				modifiedMsg = true
				filtered = append(filtered, ir.Part{Kind: ir.PartText, Text: formatDowngradedIRToolResult(part.Result)})
				sourceParts = append(sourceParts, convert.ReencodePartSource{Part: p})
				for i, child := range part.Result.Content {
					suffix := fmt.Sprintf(".result.content[%d]", i)
					if child.Kind == ir.PartMedia {
						// Keep the attachment and its nested source address together so
						// later encoder losses on separate images retain distinct paths.
						filtered = append(filtered, child)
						sourceParts = append(sourceParts, convert.ReencodePartSource{Part: p, Suffix: suffix})
						continue
					}
					// Only the child's text survives in the joined header. Record any
					// discarded metadata before the original content slots disappear.
					path := fmt.Sprintf("messages[%d].parts[%d]%s", m, p, suffix)
					if child.Cache != nil {
						edit.RecordLoss(ir.Loss{Kind: ir.LossCacheMarkDropped, Path: path})
					}
					if !child.Extra.IsZero() {
						edit.RecordLoss(ir.Loss{Kind: ir.LossExtraDropped, Path: path})
					}
				}
			default:
				filtered = append(filtered, part)
				sourceParts = append(sourceParts, convert.ReencodePartSource{Part: p})
			}
		}
		if !modifiedMsg {
			continue
		}
		if msg.Role == ir.RoleTool {
			// Switch downgraded RoleTool turns to RoleUser because IR RoleTool
			// messages only carry structured PartToolResult entries.
			msg.Role = ir.RoleUser
		}
		if len(filtered) == 0 {
			// Insert placeholder text when removing redacted reasoning empties the turn
			// so Gemini contents legalization preserves turn alternation.
			filtered = append(filtered, ir.Part{
				Kind: ir.PartText,
				Text: "(content removed)",
			})
			sourceParts = append(sourceParts, convert.ReencodePartSource{Part: -1})
		}
		if edit != nil {
			edit.SetMessageParts(m, filtered, sourceParts)
		} else {
			msg.Parts = filtered
		}
		changed = true
	}
	return changed, nil
}

// recordInlinedToolHistoryLosses attributes the replacement of a structured
// history block to its source slot. The rendered text contains the correlation
// ID in its header, while optional item IDs, cache breakpoints and provider
// extensions on the original block have no representation in that text.
func recordInlinedToolHistoryLosses(edit *convert.ReencodeEditor, m, p int, part ir.Part) {
	// A past call may no longer have a declaration. Its resolved name still
	// belongs to the conversion evidence so verification can check the rendered header.
	loss := ir.Loss{Kind: ir.LossToolHistoryInlined, Path: fmt.Sprintf("messages[%d].parts[%d]", m, p)}
	if part.Call != nil {
		loss.Detail = part.Call.Tool.Name
	}
	edit.RecordLoss(loss)
	if part.Cache != nil {
		edit.RecordPartLoss(m, p, ir.LossCacheMarkDropped)
	}
	if !part.Extra.IsZero() {
		edit.RecordPartLoss(m, p, ir.LossExtraDropped)
	}
	if part.ID != "" {
		edit.RecordLoss(ir.Loss{Kind: ir.LossIDRewritten, Path: fmt.Sprintf("messages[%d].parts[%d].id", m, p)})
	}
}

// formatDowngradedIRToolCall renders a structured IR ToolCall into the readable
// "(tool_use)" text format used by stage-two Claude signature recovery. Blank
// tool names, blank call IDs, and empty or "null" JSON arguments are omitted.
func formatDowngradedIRToolCall(call *ir.ToolCall) string {
	text := "(tool_use)"
	if call.Tool.Name != "" {
		text += " name=" + call.Tool.Name
	}
	if call.CallID != "" {
		text += " id=" + call.CallID
	}
	args := strings.TrimSpace(call.Arguments)
	if args != "" && args != "null" {
		text += " input=" + args
	}
	return text
}

// formatDowngradedIRToolResult joins the tool correlation header and its
// nonempty text children into the visible user message used during recovery.
// The caller emits media as separate parts in their original order.
func formatDowngradedIRToolResult(res *ir.ToolResult) string {
	header := "(tool_result)"
	if res.CallID != "" {
		header += " tool_use_id=" + res.CallID
	}
	if res.IsError {
		header += " is_error=true"
	}

	lines := []string{header}
	for _, child := range res.Content {
		if child.Kind == ir.PartText && child.Text != "" {
			lines = append(lines, child.Text)
		}
	}
	return strings.Join(lines, "\n")
}

// applyIRBudgetRectification configures explicit enabled thinking with
// budgetTokens on req and dialect while raising missing or sub-threshold
// maxOutputTokens below minMaxTokens up to maxTokens. Nil inputs, non-positive
// budgets, and adaptive thinking requests return false without mutating req or dialect.
func applyIRBudgetRectification(
	req *ir.Request,
	dialect *convert.Dialect,
	budgetTokens, minMaxTokens, maxTokens int,
) bool {
	if req == nil || dialect == nil || budgetTokens <= 0 {
		return false
	}
	// Skip explicit budget rectification when the inbound request requested
	// adaptive thinking, whose budget follows model-rule adaptive defaults.
	if req.Reasoning != nil && req.Reasoning.Mode == "adaptive" {
		return false
	}

	b := int64(budgetTokens)
	req.Reasoning = &ir.ReasoningConfig{
		Mode:         "enabled",
		BudgetTokens: &b,
	}
	dialect.Reasoning.ForceBudget = true
	dialect.Reasoning.Budget = true
	dialect.Reasoning.BudgetMin = 0
	dialect.Reasoning.BudgetMax = 0
	dialect.Reasoning.Supported = nil

	if maxTokens > 0 && (req.MaxOutputTokens == nil || *req.MaxOutputTokens < int64(minMaxTokens)) {
		m := int64(maxTokens)
		req.MaxOutputTokens = &m
	}
	return true
}

// wrapClaudeRecoveryGeminiBody injects the Antigravity identity prompt into
// reencodedGemini, runs CleanGeminiRequest schema normalization, and wraps the
// resulting payload inside a v1internal envelope for mappedModel. Empty
// identityPatchPrompt values use the default Antigravity prompt, CleanGeminiRequest
// errors fall back to the patched Gemini bytes, and blank projectID values
// return ErrProjectIDRequired.
func wrapClaudeRecoveryGeminiBody(
	projectID, mappedModel, identityPatchPrompt string,
	reencodedGemini []byte,
) ([]byte, error) {
	patched, err := InjectIdentityPatchToGeminiRequestWithPrompt(reencodedGemini, identityPatchPrompt)
	if err != nil {
		return nil, err
	}
	// Keep the identity-patched Gemini bytes emitted by codec/generatecontent when
	// CleanGeminiRequest fails so recovery envelope wrapping still proceeds.
	if cleaned, cleanErr := CleanGeminiRequest(patched); cleanErr == nil {
		patched = cleaned
	}
	return WrapV1InternalRequest(projectID, mappedModel, patched)
}

// isClaudeRecoverySignatureError reports whether respBody or its unwrapped
// v1internal response object matches Claude signature or expected-thinking
// error wording. Unwrap failures fall back to the direct raw payload check.
func isClaudeRecoverySignatureError(respBody []byte) bool {
	if IsSignatureRelatedError(respBody) {
		return true
	}
	if unwrapped, err := unwrapV1InternalConvertedPayload(respBody); err == nil && len(unwrapped) > 0 {
		return IsSignatureRelatedError(unwrapped)
	}
	return false
}

// extractClaudeRecoveryErrorMessage extracts trimmed Google platform error text
// from respBody, falling back to an unwrapped v1internal response object when
// the outer wrapper carries the error nested under "response". Bodies without
// an extractable platform error message return an empty string.
func extractClaudeRecoveryErrorMessage(respBody []byte) string {
	msg := strings.TrimSpace(googlewire.ExtractPlatformMessage(respBody))
	if msg != "" {
		return msg
	}
	if unwrapped, err := unwrapV1InternalConvertedPayload(respBody); err == nil && len(unwrapped) > 0 {
		return strings.TrimSpace(googlewire.ExtractPlatformMessage(unwrapped))
	}
	return ""
}
