package ir

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRuntimeCallbacksAndFallbacks verifies zero-value safety on Runtime
// helpers alongside custom and DefaultRuntime prefix formatting. Missing or
// empty callbacks must return deterministic fallbacks without panicking.
func TestRuntimeCallbacksAndFallbacks(t *testing.T) {
	var zero Runtime
	require.Equal(t, int64(0), zero.NowUnix())
	require.Equal(t, "chatcmpl-fallback", zero.NextID("chatcmpl-", "chatcmpl-fallback"))

	emptyID := Runtime{
		Now:   func() time.Time { return time.Unix(1760000123, 0) },
		NewID: func(string) string { return "" },
	}
	require.Equal(t, int64(1760000123), emptyID.NowUnix())
	require.Equal(t, "call_fallback", emptyID.NextID("call_", "call_fallback"))

	seq := 0
	custom := Runtime{
		NewID: func(prefix string) string {
			seq++
			return fmt.Sprintf("%s%d", prefix, seq)
		},
	}
	require.Equal(t, "call_1", custom.NextID("call_", "unused"))
	require.Equal(t, "call_2", custom.NextID("call_", "unused"))

	prod := DefaultRuntime()
	require.Positive(t, prod.NowUnix())
	dashID := prod.NextID("chatcmpl-", "")
	require.True(t, strings.HasPrefix(dashID, "chatcmpl-"))
	require.Len(t, dashID, len("chatcmpl-")+24)

	wordID := prod.NextID("item", "")
	require.True(t, strings.HasPrefix(wordID, "item_"))
	require.Len(t, wordID, len("item_")+24)

	bareID := prod.NextID("", "")
	require.Len(t, bareID, 24)
}

// TestDialectEnumsBitmaskAndClone checks ToolSupport bitmask queries alongside
// HistoryMode and SchemaDialect validity predicates. Mutating a cloned Dialect
// or its nested ReasoningCapabilities must leave the original descriptor intact.
func TestDialectEnumsBitmaskAndClone(t *testing.T) {
	require.True(t, ToolSupportAllResponses.Has(ToolSupportFunction))
	require.True(t, ToolSupportAllResponses.Has(ToolSupportCustom|ToolSupportNamespace))
	require.False(t, ToolSupportFunction.Has(ToolSupportCustom))
	require.True(t, ToolSupportFunction.Has(ToolSupportNone))

	require.True(t, HistoryMode("").IsValid())
	require.True(t, HistoryNone.IsValid())
	require.True(t, HistoryToolTurns.IsValid())
	require.True(t, HistoryAll.IsValid())
	require.False(t, HistoryMode("bogus").IsValid())

	require.True(t, SchemaDialect("").IsValid())
	require.True(t, SchemaJSONSchema.IsValid())
	require.True(t, SchemaGemini.IsValid())
	require.True(t, SchemaAntigravity.IsValid())
	require.False(t, SchemaDialect("bogus").IsValid())

	supported := true
	outLimit := 8192
	orig := Dialect{
		Name:             "deepseek",
		ProviderID:       42,
		Tools:            ToolSupportFunction,
		ToolNameMaxBytes: 64,
		ReplayScheme:     SchemeOpenAI,
		ReasoningHistory: HistoryToolTurns,
		SchemaDialect:    SchemaJSONSchema,
		Reasoning: ReasoningCapabilities{
			ControlsKnown: true,
			Supported:     &supported,
			Efforts:       []string{"low", "medium", "high"},
			OutputLimit:   &outLimit,
		},
	}

	cp := orig.Clone()
	*cp.Reasoning.Supported = false
	cp.Reasoning.Efforts[0] = "minimal"
	*cp.Reasoning.OutputLimit = 4096
	cp.ReplayScheme = ""

	require.True(t, *orig.Reasoning.Supported)
	require.Equal(t, "low", orig.Reasoning.Efforts[0])
	require.Equal(t, 8192, *orig.Reasoning.OutputLimit)
	require.Equal(t, SchemeOpenAI, orig.ReplayScheme)
}
