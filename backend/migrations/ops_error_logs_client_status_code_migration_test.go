package migrations

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ops_error_logs_client_status_code_migration_test.go verifies the embedded SQL
// migration that adds the nullable client_status_code INT column to
// ops_error_logs. Tests enforce idempotent ADD COLUMN syntax without rewriting
// existing rows or synthesizing wire statuses for historical records.

// TestOpsErrorLogsClientStatusCodeMigrationAddsNullableIntColumn loads
// migration 318 from the embedded filesystem and verifies that ops_error_logs
// adds an idempotent nullable INT column without defaults or table updates.
func TestOpsErrorLogsClientStatusCodeMigrationAddsNullableIntColumn(t *testing.T) {
	content, err := FS.ReadFile("318_ops_error_logs_client_status_code.sql")
	require.NoError(t, err)

	sql := strings.ToUpper(string(content))
	require.Contains(t, sql, "ALTER TABLE OPS_ERROR_LOGS")
	require.Contains(t, sql, "ADD COLUMN IF NOT EXISTS CLIENT_STATUS_CODE INT")
	require.NotContains(t, sql, "NOT NULL")
	require.NotContains(t, sql, "DEFAULT")
	require.NotContains(t, sql, "UPDATE OPS_ERROR_LOGS")
}
