package provider

import (
	"bytes"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"
	"strings"

	"github.com/TokenFlux/TokenRouter/internal/modelcatalog"
	protocolanthropic "github.com/TokenFlux/TokenRouter/internal/protocol/anthropic"
)

// responses_effort.go reconciles explicit Messages max effort with converted
// effort and final-model capability.
// A Messages request can explicitly ask for max effort even when conversion
// produced another spelling. Reconciliation considers that client intent
// together with final-model capability, so a converter default does not erase
// supported max and a known unsupported max does not reach the outbound request.
//
// Missing capability evidence permits the explicit client value. Known
// unsupported max keeps a different converted effort, or changes converted max
// to xhigh when conversion has not supplied an alternative. Other client effort
// values pass through as converted. The first supplied attributes control the
// decision; upstreamModel remains a compatibility argument with no effect.

// OpenAICompatAnthropicReasoningEffort resolves explicit Messages max effort
// against the first optional attribute set. Requests without an explicit max
// return convertedEffort unchanged; client comparison trims text and ignores
// case.
//
// Unknown or supported max capability returns the client's canonical max,
// even when conversion selected another spelling. Known unsupported max keeps
// convertedEffort unless it also spells max, in which case xhigh supplies the
// compatible result. Later attribute arguments and upstreamModel do not affect
// this decision.
func OpenAICompatAnthropicReasoningEffort(req *protocolanthropic.AnthropicRequest, upstreamModel, convertedEffort string, attributes ...modelcatalog.Attributes) string {
	if req == nil || req.OutputConfig == nil {
		return convertedEffort
	}
	return OpenAICompatAnthropicReasoningEffortValue(req.OutputConfig.Effort, upstreamModel, convertedEffort, attributes...)
}

// OpenAICompatAnthropicReasoningEffortValue reconciles an explicit Messages
// output_config.effort string with convertedEffort and the first optional model
// attribute set. Non-max client efforts return convertedEffort unchanged; unknown
// or supported max returns canonical "max", while known unsupported max maps
// converted "max" to "xhigh".
func OpenAICompatAnthropicReasoningEffortValue(clientEffort, upstreamModel, convertedEffort string, attributes ...modelcatalog.Attributes) string {
	_ = upstreamModel
	if !strings.EqualFold(strings.TrimSpace(clientEffort), "max") {
		return convertedEffort
	}
	var attrs modelcatalog.Attributes
	if len(attributes) > 0 {
		attrs = attributes[0]
	}
	if supported := attrs.EffortSupport("max"); supported == nil || *supported {
		return "max"
	}
	// A known unsupported max cannot be forwarded as max; xhigh supplies the
	// compatibility spelling when the converter has not already selected another effort.
	if strings.EqualFold(strings.TrimSpace(convertedEffort), "max") {
		return "xhigh"
	}
	return convertedEffort
}

// ExtractAnthropicOutputEffortBytes reads output_config.effort from raw
// Anthropic Messages JSON bytes using jsontext without decoding unrelated
// request fields or rounding large integers. Empty, non-object, malformed, or
// non-string effort fields return an empty string.
func ExtractAnthropicOutputEffortBytes(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	dec := jsontext.NewDecoder(bytes.NewReader(body), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return ""
	}
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil {
			return ""
		}
		if keyTok.String() != "output_config" {
			if err := dec.SkipValue(); err != nil {
				return ""
			}
			continue
		}
		subTok, err := dec.ReadToken()
		if err != nil || subTok.Kind() != '{' {
			return ""
		}
		var effort string
		for dec.PeekKind() != '}' {
			subKeyTok, err := dec.ReadToken()
			if err != nil {
				return ""
			}
			subKey := subKeyTok.String()
			val, err := dec.ReadValue()
			if err != nil {
				return ""
			}
			if subKey == "effort" {
				_ = jsonv2.Unmarshal(val, &effort, jsontext.AllowInvalidUTF8(true))
			}
		}
		return effort
	}
	return ""
}
