package testkit

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestJSONDiffPrecisionAndEquivalence compares JSON documents with 64-bit
// integers beyond float64 precision, equivalent decimal notations, reordered
// keys, ignored volatile paths, and duplicate object keys. It verifies exact
// numeric distinction without rounding and deterministic path-sorted diffs.
func TestJSONDiffPrecisionAndEquivalence(t *testing.T) {
	eq, summary, err := JSONEqual(
		[]byte(`{"b":1.00,"a":1e0,"zero":-0.0,"big":9007199254740993}`),
		[]byte(`{"a":1,"b":0.1e1,"zero":0,"big":9007199254740993}`),
	)
	require.NoError(t, err)
	require.True(t, eq, summary)

	// Adjacent integers above 2^53 would collide under float64 decoding.
	diffs, err := JSONDiff(
		[]byte(`{"id":9007199254740992}`),
		[]byte(`{"id":9007199254740993}`),
	)
	require.NoError(t, err)
	require.Equal(t, []JSONDiffItem{{
		Path: "$.id",
		Kind: DiffChanged,
		Want: "9007199254740992",
		Got:  "9007199254740993",
	}}, diffs)

	// Ignored paths skip volatile IDs while still reporting nested mismatches.
	diffs, err = JSONDiff(
		[]byte(`{"id":"resp_1","items":[{"text":"ok"}],"old":true}`),
		[]byte(`{"id":"resp_2","items":[{"text":"changed"}],"extra":1}`),
		WithIgnorePaths("$.id"),
	)
	require.NoError(t, err)
	require.Equal(t, []JSONDiffItem{
		{Path: "$.extra", Kind: DiffAdded, Got: "1"},
		{Path: "$.items[0].text", Kind: DiffChanged, Want: `"ok"`, Got: `"changed"`},
		{Path: "$.old", Kind: DiffRemoved, Want: "true"},
	}, diffs)

	// Duplicate object member names must fail parsing rather than silently winning last.
	_, err = JSONDiff([]byte(`{"a":1,"a":2}`), []byte(`{"a":2}`))
	require.Error(t, err)

	h1, err := CanonicalJSONHash([]byte(`{"z":10.0,"a":[1e0,-0]}`))
	require.NoError(t, err)
	h2, err := CanonicalJSONHash([]byte(`{"a":[1,0],"z":1e1}`))
	require.NoError(t, err)
	require.Equal(t, h1, h2)
}
