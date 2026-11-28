package migrations

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestMigration320DropsPricingConfigFreeOpenAIFast inspects the retirement SQL
// for the shared pricing configuration free-Fast toggle. It verifies that the
// migration drops the free_openai_fast column from pricing_configs idempotently.
func TestMigration320DropsPricingConfigFreeOpenAIFast(t *testing.T) {
	content, err := FS.ReadFile("320_drop_pricing_config_free_openai_fast.sql")
	require.NoError(t, err)

	sql := string(content)
	require.Contains(t, sql, "ALTER TABLE pricing_configs")
	require.Contains(t, sql, "DROP COLUMN IF EXISTS free_openai_fast")
}
