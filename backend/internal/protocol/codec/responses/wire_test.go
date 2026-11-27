package responses

import (
	"encoding/json/jsontext"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/protocol"
	"github.com/TokenFlux/TokenRouter/internal/protocol/ir"
)

// TestParseAndFormatBase64DataURI checks round-trip parsing and formatting of
// base64 data URIs alongside remote HTTPS URLs and malformed data: prefixes.
// Remote URLs must report isDataURI=false while broken base64 prefixes report valid=false.
func TestParseAndFormatBase64DataURI(t *testing.T) {
	mime, payload, isData, valid := parseBase64DataURI("data:Image/PNG;Base64,YWJjZA==")
	require.True(t, isData)
	require.True(t, valid)
	require.Equal(t, "image/png", mime)
	require.Equal(t, "YWJjZA==", payload)
	require.Equal(t, "data:image/png;base64,YWJjZA==", formatBase64DataURI(mime, "application/octet-stream", payload))
	require.Equal(t, "data:application/pdf;base64,YWJj", formatBase64DataURI("", "application/pdf", "YWJj"))

	_, _, isData, valid = parseBase64DataURI("https://example.com/chart.png")
	require.False(t, isData)
	require.False(t, valid)

	for _, bad := range []string{"data:no_comma", "data:image/png,raw", "data:;base64,abc", "data:image/png;base64,"} {
		_, _, isData, valid = parseBase64DataURI(bad)
		require.True(t, isData, "input %q must be recognized as data URI", bad)
		require.False(t, valid, "input %q must be rejected as malformed", bad)
	}
}

// TestSanitizeAndFlattenToolName checks character replacement, 64-byte SHA-256
// suffix truncation, and namespace flattening on OpenAI Responses tool names.
// Distinct overlong names sharing a 64-byte prefix must produce distinct valid names.
func TestSanitizeAndFlattenToolName(t *testing.T) {
	unchanged, modified := sanitizeResponsesToolName("read_file-v2", 64)
	require.False(t, modified)
	require.Equal(t, "read_file-v2", unchanged)

	invalidChars, modified := sanitizeResponsesToolName("mcp.server/search!", 64)
	require.True(t, modified)
	require.Regexp(t, `^[a-zA-Z0-9_-]{1,64}$`, invalidChars)

	ns := "enterprise_observability_telemetry_connector"
	nameA := flattenNamespaceToolName(ns, "query_distributed_trace_spans_by_service_alpha", 64)
	nameB := flattenNamespaceToolName(ns, "query_distributed_trace_spans_by_service_bravo", 64)
	require.LessOrEqual(t, len(nameA), 64)
	require.LessOrEqual(t, len(nameB), 64)
	require.NotEqual(t, nameA, nameB)
	require.False(t, strings.Contains(nameA, "."))

	shortCap, modified := sanitizeResponsesToolName("very_long_tool_identifier", 8)
	require.True(t, modified)
	require.Len(t, shortCap, 8)
}

// TestIsValidSingleJSONAndRestoreExtra checks duplicate-key and trailing-token
// rejection in isValidSingleJSON plus same-origin versus foreign Extra handling.
// Foreign Extra maps must be dropped and reported via LossExtraDropped.
func TestIsValidSingleJSONAndRestoreExtra(t *testing.T) {
	require.True(t, isValidSingleJSON([]byte(`{"a":1,"b":[true,null]}`)))
	require.False(t, isValidSingleJSON(nil))
	require.False(t, isValidSingleJSON([]byte(`{"a":1,"a":2}`)))
	require.False(t, isValidSingleJSON([]byte(`{"a":1} {"b":2}`)))

	sameOrigin := ir.Extra{
		Origin:  protocol.ProtocolOpenAIResponses,
		Members: map[string]jsontext.Value{"vendor_flag": jsontext.Value(`true`)},
	}
	var losses []ir.Loss
	restored := restoreOrDropExtra(sameOrigin, "messages[0]", &losses)
	require.Empty(t, losses)
	require.Equal(t, jsontext.Value(`true`), restored["vendor_flag"])

	foreignOrigin := ir.Extra{
		Origin:  protocol.ProtocolAnthropicMessages,
		Members: map[string]jsontext.Value{"citations": jsontext.Value(`[]`)},
	}
	restoredForeign := restoreOrDropExtra(foreignOrigin, "messages[0].parts[0]", &losses)
	require.Nil(t, restoredForeign)
	require.Equal(t, []ir.Loss{{Kind: ir.LossExtraDropped, Path: "messages[0].parts[0]"}}, losses)
}
