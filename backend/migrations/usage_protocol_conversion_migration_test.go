package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// usage_protocol_conversion_migration_test.go verifies the embedded SQL
// migration that adds the nullable protocol_conversion JSONB column to usage_logs.
// Tests check idempotent ADD COLUMN syntax and ensure existing rows are neither
// rewritten nor backfilled with synthetic default payloads.

// TestUsageProtocolConversionMigrationAddsNullableJSONBColumn loads migration
// 317 from the embedded filesystem and verifies that usage_logs receives an
// idempotent nullable JSONB column without a non-null constraint or table rewrite.
func TestUsageProtocolConversionMigrationAddsNullableJSONBColumn(t *testing.T) {
	content, err := FS.ReadFile("317_usage_protocol_conversion.sql")
	require.NoError(t, err)

	sql := strings.ToUpper(string(content))
	require.Contains(t, sql, "ALTER TABLE USAGE_LOGS")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS PROTOCOL_CONVERSION JSONB")
	require.NotContains(t, sql, "NOT NULL")
	require.NotContains(t, sql, "DEFAULT")
	require.NotContains(t, sql, "UPDATE USAGE_LOGS")
}
