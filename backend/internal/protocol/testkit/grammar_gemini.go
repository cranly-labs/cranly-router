package testkit

import (
	"bytes"
	"fmt"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/sse"
)

// grammar_gemini.go validates Gemini GenerateContent SSE streams against
// GenerateContentResponse candidate framing, part union validity, object-typed
// functionCall arguments, finishReason placement, and post-finish usage tail
// rules. Unlike OpenAI Chat, Gemini SSE emits self-contained JSON response
// envelopes on every frame and forbids [DONE] sentinels.
//
// Candidate content frames must carry role "model" and non-empty part unions
// with object-valued functionCall.args, and no new content or functionCall
// parts may appear after finishReason while trailing usageMetadata-only frames
// remain accepted during the upstream usage tail window. Abnormal stream
// termination accepts a single JSON error envelope when no finishReason arrived.

// ValidateGeminiStream checks frames against the Gemini GenerateContent SSE
// stream grammar and returns all detected violations in frame order.
//
// Normal completion requires at least one candidate frame carrying a recognized
// finishReason enum, object-typed functionCall.args values, and no [DONE]
// sentinel; trailing usageMetadata frames after finishReason are accepted.
//
// When opts.AllowAbnormal is true, a terminal JSON error frame satisfies
// stream completion, while silent EOF without finishReason or error fails.
func ValidateGeminiStream(frames []sse.Frame, opts GrammarOptions) []Violation {
	var violations []Violation
	add := func(idx int, event, rule, path, msg string) {
		violations = append(violations, Violation{
			Protocol:   protocol.ProtocolGeminiGenerateContent,
			Rule:       rule,
			FrameIndex: idx,
			Event:      event,
			Path:       path,
			Message:    msg,
		})
	}

	if len(frames) == 0 {
		add(-1, "", "empty_stream", "", "Gemini stream ended without emitting any frames")
		return violations
	}

	sawFinish := false
	sawError := false

	for i, frame := range frames {
		trimmed := bytes.TrimSpace(frame.Data)
		if bytes.Equal(trimmed, []byte("[DONE]")) {
			add(i, frame.Event, "unexpected_done_sentinel", "", "Gemini GenerateContent SSE must not emit [DONE]")
			continue
		}
		if sawError {
			add(i, frame.Event, "frame_after_error", "", "no frames may follow a Gemini error frame")
			continue
		}

		obj, err := parseFrameJSONObject(trimmed)
		if err != nil {
			add(i, frame.Event, "invalid_json", "$", err.Error())
			continue
		}

		if errObj, hasErr, okErr := nodeObjectField(obj, "error"); hasErr {
			if !opts.AllowAbnormal {
				add(i, frame.Event, "unexpected_error_frame", "$.error", "error frame is invalid in normal completion mode")
			}
			if !okErr {
				add(i, frame.Event, "invalid_error_object", "$.error", "error field must be a JSON object")
			} else if msg, _, _ := nodeStringField(errObj, "message"); msg == "" {
				add(i, frame.Event, "missing_error_message", "$.error.message", "error.message must be a non-empty string")
			}
			sawError = true
			continue
		}

		candidates, hasCand, okCand := nodeArrayField(obj, "candidates")
		_, hasUsage, okUsage := nodeObjectField(obj, "usageMetadata")
		if hasUsage && !okUsage {
			add(i, frame.Event, "invalid_usage_metadata", "$.usageMetadata", "usageMetadata must be a JSON object")
		}
		if !hasCand && !hasUsage {
			add(i, frame.Event, "empty_response_envelope", "$", "GenerateContentResponse frame must contain candidates or usageMetadata")
			continue
		}
		if hasCand && (!okCand || len(candidates) == 0) {
			add(i, frame.Event, "empty_candidates", "$.candidates", "candidates must be a non-empty array when present")
			continue
		}
		if !hasCand {
			continue
		}

		if candidates[0].kind != nodeObject {
			add(i, frame.Event, "invalid_candidate", "$.candidates[0]", "candidates[0] must be a JSON object")
			continue
		}
		candObj := candidates[0].objVal
		contentObj, hasContent, okContent := nodeObjectField(candObj, "content")
		if hasContent {
			if !okContent {
				add(i, frame.Event, "invalid_candidate_content", "$.candidates[0].content", "candidate content must be a JSON object")
			} else {
				if role, hasRole, okRole := nodeStringField(contentObj, "role"); hasRole && (!okRole || role != "model") {
					add(i, frame.Event, "invalid_candidate_role", "$.candidates[0].content.role", fmt.Sprintf("candidate content.role = %q, want \"model\"", role))
				}
				parts, hasParts, okParts := nodeArrayField(contentObj, "parts")
				if hasParts && !okParts {
					add(i, frame.Event, "invalid_candidate_parts", "$.candidates[0].content.parts", "content.parts must be an array")
				}
				if sawFinish && len(parts) > 0 {
					add(i, frame.Event, "parts_after_finish_reason", "$.candidates[0].content.parts", "candidate content parts emitted after finishReason")
				}
				for pIdx, pNode := range parts {
					validateGeminiResponsePart(i, frame.Event, pIdx, pNode, add)
				}
			}
		}

		if fr, hasFR, okFR := nodeStringField(candObj, "finishReason"); hasFR {
			if !okFR || !isValidGeminiFinishReason(fr) {
				add(i, frame.Event, "invalid_finish_reason", "$.candidates[0].finishReason", fmt.Sprintf("unrecognized Gemini finishReason %q", fr))
			}
			sawFinish = true
		}
	}

	if !opts.AllowAbnormal && !sawFinish {
		add(-1, "", "missing_finish_reason", "", "Gemini stream ended without candidates[0].finishReason")
	} else if opts.AllowAbnormal && !sawFinish && !sawError {
		add(-1, "", "missing_terminal_or_error", "", "truncated Gemini stream ended without finishReason or error frame")
	}
	return violations
}

// validateGeminiResponsePart checks part union fields and object functionCall arguments inside candidates[0].
func validateGeminiResponsePart(frameIdx int, event string, partIdx int, pNode jsonNode, add func(int, string, string, string, string)) {
	basePath := fmt.Sprintf("$.candidates[0].content.parts[%d]", partIdx)
	if pNode.kind != nodeObject {
		add(frameIdx, event, "invalid_part", basePath, "part must be a JSON object")
		return
	}
	pObj := pNode.objVal
	branches := 0
	if _, hasText, okText := nodeStringField(pObj, "text"); hasText {
		branches++
		if !okText {
			add(frameIdx, event, "invalid_part_text", basePath+".text", "part.text must be a string")
		}
	}
	if fcObj, hasFC, okFC := nodeObjectField(pObj, "functionCall"); hasFC {
		branches++
		if !okFC {
			add(frameIdx, event, "invalid_function_call", basePath+".functionCall", "functionCall must be a JSON object")
		} else {
			if name, _, _ := nodeStringField(fcObj, "name"); name == "" {
				add(frameIdx, event, "missing_function_call_name", basePath+".functionCall.name", "functionCall.name must be a non-empty string")
			}
			if argsVal, hasArgs := fcObj["args"]; hasArgs && argsVal.kind != nodeObject {
				add(frameIdx, event, "non_object_function_call_args", basePath+".functionCall.args", "functionCall.args must be a JSON object")
			}
		}
	}
	if inlineObj, hasIn, okIn := nodeObjectField(pObj, "inlineData"); hasIn {
		branches++
		if !okIn {
			add(frameIdx, event, "invalid_inline_data", basePath+".inlineData", "inlineData must be a JSON object")
		} else {
			if mt, _, _ := nodeStringField(inlineObj, "mimeType"); mt == "" {
				add(frameIdx, event, "missing_inline_mime", basePath+".inlineData.mimeType", "inlineData.mimeType must be non-empty")
			}
			if d, _, _ := nodeStringField(inlineObj, "data"); d == "" {
				add(frameIdx, event, "missing_inline_payload", basePath+".inlineData.data", "inlineData.data must be non-empty")
			}
		}
	}
	if fileObj, hasFile, okFile := nodeObjectField(pObj, "fileData"); hasFile {
		branches++
		if !okFile {
			add(frameIdx, event, "invalid_file_data", basePath+".fileData", "fileData must be a JSON object")
		} else if uri, _, _ := nodeStringField(fileObj, "fileUri"); uri == "" {
			add(frameIdx, event, "missing_file_uri", basePath+".fileData.fileUri", "fileData.fileUri must be non-empty")
		}
	}
	if _, hasExec := pObj["executableCode"]; hasExec {
		branches++
	}
	if _, hasExecRes := pObj["codeExecutionResult"]; hasExecRes {
		branches++
	}
	if branches == 0 {
		add(frameIdx, event, "empty_part_union", basePath, "Gemini part contains no recognized content branch")
	}
}

// isValidGeminiFinishReason reports whether fr is a known Gemini FinishReason enum value.
func isValidGeminiFinishReason(fr string) bool {
	switch fr {
	case "STOP", "MAX_TOKENS", "SAFETY", "RECITATION", "LANGUAGE", "OTHER",
		"BLOCKLIST", "PROHIBITED_CONTENT", "SPII", "MALFORMED_FUNCTION_CALL",
		"IMAGE_SAFETY", "UNEXPECTED_TOOL_CALL":
		return true
	default:
		return false
	}
}
