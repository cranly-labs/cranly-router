package ir

import (
	"encoding/json/v2"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestLossKindsAndJSONSerialization checks all twenty-three canonical LossKind
// constants for distinct non-empty wire spellings. It also verifies that Loss
// omits empty Detail strings when marshaled and preserves non-empty Detail on round-trip.
func TestLossKindsAndJSONSerialization(t *testing.T) {
	kinds := []LossKind{
		LossExtraDropped,
		LossOpaqueItemDropped,
		LossReplayTokenDropped,
		LossReasoningTextDropped,
		LossReasoningTextInlined,
		LossRefusalInlined,
		LossCacheMarkDropped,
		LossMediaUnsupported,
		LossMediaRelocated,
		LossToolLowered,
		LossToolChoiceApproximated,
		LossHostedToolDropped,
		LossSchemaApproximated,
		LossSystemRelocated,
		LossPartsReordered,
		LossToolPairingRepaired,
		LossSamplingDropped,
		LossStopDropped,
		LossAnnotationDropped,
		LossOutputFormatApproximated,
		LossClientMarkerDropped,
		LossCandidateCountIgnored,
		LossIDRewritten,
	}
	require.Len(t, kinds, 23)

	seen := make(map[LossKind]bool, len(kinds))
	for _, kind := range kinds {
		require.NotEmpty(t, kind)
		require.False(t, seen[kind], "duplicate LossKind %q", kind)
		seen[kind] = true
	}

	withoutDetail := Loss{Kind: LossCacheMarkDropped, Path: "messages[0].parts[0]"}
	raw, err := json.Marshal(withoutDetail, json.Deterministic(true))
	require.NoError(t, err)
	require.Equal(t, `{"kind":"cache_mark_dropped","path":"messages[0].parts[0]"}`, string(raw))

	withDetail := Loss{Kind: LossSamplingDropped, Path: "sampling.top_k", Detail: "top_k"}
	rawWithDetail, err := json.Marshal(withDetail, json.Deterministic(true))
	require.NoError(t, err)
	var decoded Loss
	require.NoError(t, json.Unmarshal(rawWithDetail, &decoded))
	require.Equal(t, withDetail, decoded)
}

// TestLossKindIsValidAndString checks LossKind.IsValid across valid and
// unrecognized kinds alongside Loss.String formatting on root, indexed, and
// detailed records. Empty paths must render as "<root>" and unrecognized kinds must fail.
func TestLossKindIsValidAndString(t *testing.T) {
	require.True(t, LossExtraDropped.IsValid())
	require.True(t, LossIDRewritten.IsValid())
	require.False(t, LossKind("").IsValid())
	require.False(t, LossKind("invented_loss").IsValid())

	require.Equal(t, "extra_dropped at <root>", (Loss{Kind: LossExtraDropped}).String())
	require.Equal(t, "cache_mark_dropped at messages[0].parts[1]", (Loss{
		Kind: LossCacheMarkDropped,
		Path: "messages[0].parts[1]",
	}).String())
	require.Equal(t, "sampling_dropped at sampling.top_k (top_k)", (Loss{
		Kind:   LossSamplingDropped,
		Path:   "sampling.top_k",
		Detail: "top_k",
	}).String())
}
