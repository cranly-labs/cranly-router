package ir

import (
	"bytes"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// token.go formats and parses gateway continuity carrier envelopes ("trc1:")
// for cross-protocol reasoning signatures, account-bound encrypted reasoning,
// and tokenless plaintext reasoning blocks. Client protocols expose different
// opaque fields across turns, and bare provider tokens give no reliable signal
// of which issuer or account produced them.
//
// Wrapping tokens inside a versioned, unpadded base64url JSON envelope records
// the issuing scheme, verbatim Anthropic thinking text when signatures bind to
// exact text, redaction state, and optional account-bound provider IDs.
// ParseCarrier accepts only the six gateway schemes with canonical base64url
// object payloads so bare upstream tokens lacking a valid trc1 envelope
// classify as SchemeUnknown.

const (
	// CarrierPrefix identifies version-1 gateway continuity carrier envelopes on
	// protocol signature and encrypted_content wire paths.
	CarrierPrefix = "trc1:" // leading prefix before "<scheme>:<payload>"
)

// Carrier represents the unpacked fields of a trc1:<scheme>:<payload> continuity envelope.
type Carrier struct {
	Scheme     string // anthropic, antigravity_claude, openai, xai, gemini, or plain
	Data       string // verbatim issuer token ("d")
	Text       string // verbatim reasoning text ("t") for anthropic, antigravity_claude, or plain
	Redacted   bool   // whether the reasoning block originated from redacted_thinking ("r")
	ProviderID int64  // optional positive issuing provider ID ("p") for openai or xai
}

// carrierWire defines the exact d, t, r, p JSON member order inside encoded trc1 payloads.
type carrierWire struct {
	Data       string `json:"d,omitzero"` // issuer token string
	Text       string `json:"t,omitzero"` // verbatim reasoning text
	Redacted   bool   `json:"r,omitzero"` // redacted thinking marker
	ProviderID int64  `json:"p,omitzero"` // positive issuing provider ID
}

// IsCarrierScheme reports whether scheme is one of the six continuity schemes
// accepted inside trc1 carrier envelopes. SchemeUnknown, empty strings, and
// unrecognized vendor labels return false so bare tokens never pass as envelopes.
func IsCarrierScheme(scheme string) bool {
	switch scheme {
	case SchemeAnthropic, SchemeAntigravityClaude, SchemeOpenAI,
		SchemeXAI, SchemeGemini, SchemePlain:
		return true
	default:
		return false
	}
}

// IsAccountBoundScheme reports whether scheme binds encrypted continuity tokens
// to the specific provider account recorded in Carrier.ProviderID.
func IsAccountBoundScheme(scheme string) bool {
	return scheme == SchemeOpenAI || scheme == SchemeXAI
}

// IsTextCarryingScheme reports whether scheme stores verbatim reasoning text
// inside Carrier.Text across client round trips.
func IsTextCarryingScheme(scheme string) bool {
	switch scheme {
	case SchemeAnthropic, SchemeAntigravityClaude, SchemePlain:
		return true
	default:
		return false
	}
}

// FormatCarrier serializes c into a "trc1:<scheme>:<payload>" string with an
// unpadded base64url JSON payload. Invalid schemes or scheme-incompatible field
// combinations return an error rather than emitting malformed envelopes.
//
// Anthropic and Antigravity Claude non-redacted carriers preserve verbatim Text
// alongside Data, plain carriers carry Text without Data, and OpenAI or xAI
// carriers include positive ProviderID values while omitting Text. Invalid UTF-8
// bytes inside Text or Data coerce to UTF-8 replacement runes without failing.
func FormatCarrier(c Carrier) (string, error) {
	if err := validateCarrier(c); err != nil {
		return "", err
	}
	wire := carrierWire{
		Data:       c.Data,
		Text:       c.Text,
		Redacted:   c.Redacted,
		ProviderID: c.ProviderID,
	}
	rawJSON, err := json.Marshal(wire, jsontext.AllowInvalidUTF8(true))
	if err != nil {
		return "", fmt.Errorf("marshal carrier payload: %w", err)
	}
	encoded := base64.RawURLEncoding.EncodeToString(rawJSON)
	return CarrierPrefix + c.Scheme + ":" + encoded, nil
}

// ParseCarrier decodes raw when it is a well-formed "trc1:<scheme>:<payload>"
// continuity envelope and returns false for any other string. Bare provider
// tokens, unknown schemes, padded or non-canonical base64url strings, duplicate
// or unknown JSON keys, non-positive "p" fields, and scheme-invalid payloads
// all return false without error side effects.
//
// Callers use the boolean result to separate authentic gateway envelopes from
// bare client-supplied tokens lacking a trc1 header, which convert classifies
// as SchemeUnknown for same-protocol passthrough or cross-protocol pruning.
func ParseCarrier(raw string) (Carrier, bool) {
	if !strings.HasPrefix(raw, CarrierPrefix) {
		return Carrier{}, false
	}
	rest := raw[len(CarrierPrefix):]
	scheme, payloadB64, hasSep := strings.Cut(rest, ":")
	if !hasSep || strings.ContainsRune(payloadB64, ':') {
		return Carrier{}, false
	}
	if !IsCarrierScheme(scheme) || payloadB64 == "" || strings.ContainsRune(payloadB64, '=') {
		return Carrier{}, false
	}

	decoded, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return Carrier{}, false
	}
	// Reject non-canonical base64url spellings with non-zero trailing pad bits or
	// embedded whitespace so one payload has a single valid wire string.
	if base64.RawURLEncoding.EncodeToString(decoded) != payloadB64 {
		return Carrier{}, false
	}

	out, ok := decodeCarrierJSON(scheme, decoded)
	if !ok {
		return Carrier{}, false
	}
	if validateCarrier(out) != nil {
		return Carrier{}, false
	}
	return out, true
}

// decodeCarrierJSON parses raw as a single JSON object containing only unique
// d, t, r, and p members of their exact scalar types. Duplicate keys, unknown
// members, nonpositive p values, or trailing tokens return false.
func decodeCarrierJSON(scheme string, raw []byte) (Carrier, bool) {
	dec := jsontext.NewDecoder(bytes.NewReader(raw), jsontext.AllowInvalidUTF8(true))
	tok, err := dec.ReadToken()
	if err != nil || tok.Kind() != '{' {
		return Carrier{}, false
	}

	out := Carrier{Scheme: scheme}
	var seenD, seenT, seenR, seenP bool
	for dec.PeekKind() != '}' {
		keyTok, err := dec.ReadToken()
		if err != nil || keyTok.Kind() != '"' {
			return Carrier{}, false
		}
		key := keyTok.String()
		valTok, err := dec.ReadToken()
		if err != nil {
			return Carrier{}, false
		}
		switch key {
		case "d":
			if seenD || valTok.Kind() != '"' {
				return Carrier{}, false
			}
			seenD = true
			out.Data = valTok.String()
		case "t":
			if seenT || valTok.Kind() != '"' {
				return Carrier{}, false
			}
			seenT = true
			out.Text = valTok.String()
		case "r":
			if seenR || (valTok.Kind() != 't' && valTok.Kind() != 'f') {
				return Carrier{}, false
			}
			seenR = true
			out.Redacted = valTok.Bool()
		case "p":
			if seenP || valTok.Kind() != '0' {
				return Carrier{}, false
			}
			seenP = true
			pid, parseErr := strconv.ParseInt(valTok.String(), 10, 64)
			if parseErr != nil || pid <= 0 {
				return Carrier{}, false
			}
			out.ProviderID = pid
		default:
			return Carrier{}, false
		}
	}
	if _, err := dec.ReadToken(); err != nil {
		return Carrier{}, false
	}
	if _, err := dec.ReadToken(); err != io.EOF {
		return Carrier{}, false
	}
	return out, true
}

// validateCarrier checks scheme-specific field invariants shared by
// FormatCarrier and ParseCarrier. Unsupported schemes or forbidden combinations
// of Data, Text, Redacted, and ProviderID return an error before encoding or acceptance.
func validateCarrier(c Carrier) error {
	switch c.Scheme {
	case SchemePlain:
		if c.Data != "" {
			return errors.New("plain carrier cannot include issuer token d")
		}
		if c.Text == "" {
			return errors.New("plain carrier requires non-empty reasoning text t")
		}
		if c.Redacted {
			return errors.New("plain carrier cannot set redacted flag r")
		}
		if c.ProviderID != 0 {
			return errors.New("plain carrier cannot include provider ID p")
		}
		return nil

	case SchemeAnthropic, SchemeAntigravityClaude:
		if c.Data == "" {
			return fmt.Errorf("%s carrier requires non-empty issuer token d", c.Scheme)
		}
		if c.ProviderID != 0 {
			return fmt.Errorf("%s carrier cannot include provider ID p", c.Scheme)
		}
		if c.Redacted && c.Text != "" {
			return fmt.Errorf("redacted %s carrier cannot include reasoning text t", c.Scheme)
		}
		return nil

	case SchemeOpenAI, SchemeXAI:
		if c.Data == "" {
			return fmt.Errorf("%s carrier requires non-empty issuer token d", c.Scheme)
		}
		if c.Text != "" {
			return fmt.Errorf("%s carrier cannot include reasoning text t", c.Scheme)
		}
		if c.ProviderID < 0 {
			return fmt.Errorf("%s carrier provider ID p must be nonnegative", c.Scheme)
		}
		return nil

	case SchemeGemini:
		if c.Data == "" {
			return errors.New("gemini carrier requires non-empty issuer token d")
		}
		if c.Text != "" {
			return errors.New("gemini carrier cannot include reasoning text t")
		}
		if c.Redacted {
			return errors.New("gemini carrier cannot set redacted flag r")
		}
		if c.ProviderID != 0 {
			return errors.New("gemini carrier cannot include provider ID p")
		}
		return nil

	default:
		return fmt.Errorf("unsupported carrier scheme %q", c.Scheme)
	}
}
