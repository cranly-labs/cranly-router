//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/TokenFlux/TokenRouter/internal/ops"
)

// TestOpsRequestIDConditionUsesIndexes builds analyzed temporary log tables
// with twenty thousand rows and a client/local-ID collision. It protects
// exact-local precedence, client-ID lookup, missing results, and absence of
// sequential scans for the tested fixture plans in both log tables.
func TestOpsRequestIDConditionUsesIndexes(t *testing.T) {
	ctx := t.Context()
	tx, err := integrationDB.BeginTx(ctx, nil)
	require.NoError(t, err)
	// Keep the temporary tables and fixture writes inside one transaction.
	// Deferred rollback removes them after assertions, suppressing cleanup errors
	// while retaining test failures.
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `
CREATE TEMP TABLE usage_logs (LIKE public.usage_logs INCLUDING ALL) ON COMMIT DROP;
CREATE TEMP TABLE request_records (LIKE public.request_records INCLUDING ALL) ON COMMIT DROP;
CREATE TEMP TABLE ops_error_logs (LIKE public.ops_error_logs INCLUDING ALL) ON COMMIT DROP;
CREATE TEMP TABLE ops_system_logs (LIKE public.ops_system_logs INCLUDING ALL) ON COMMIT DROP;
INSERT INTO ops_error_logs(request_id,client_request_id,error_phase,error_type,status_code)
SELECT 'request-'||g,'client-'||g,'request','api_error',500 FROM generate_series(1,20000) g;
INSERT INTO ops_system_logs(request_id,client_request_id,level,message)
SELECT 'request-'||g,'client-'||g,'error','fixture' FROM generate_series(1,20000) g;
UPDATE ops_error_logs SET client_request_id='request-1' WHERE request_id='request-2';
UPDATE ops_system_logs SET client_request_id='request-1' WHERE request_id='request-2';
INSERT INTO request_records(request_id,started_at,updated_at,revision,record)
VALUES('request-1',now(),now(),1,'{"request_id":"request-1","state":"failed"}');
ANALYZE usage_logs;
ANALYZE request_records;
ANALYZE ops_error_logs;
ANALYZE ops_system_logs;`)
	require.NoError(t, err)
	for _, table := range []string{"ops_error_logs", "ops_system_logs"} {
		for _, test := range []struct {
			search string // requested correlation lookup text
			want   string // expected stored request identity
		}{
			{"request-1", "request-1"},
			{"client-3", "request-3"},
			{"missing", ""},
		} {
			t.Run(table+"/"+test.search, func(t *testing.T) {
				query := fmt.Sprintf("SELECT l.request_id FROM %s l WHERE l.id>$1 AND %s", table, opsRequestIDCondition("l", 2))
				rows, err := tx.QueryContext(ctx, query, 0, test.search)
				require.NoError(t, err)
				var got []string
				for rows.Next() {
					var id string
					require.NoError(t, rows.Scan(&id))
					got = append(got, id)
				}
				require.NoError(t, rows.Err())
				// Close each query cursor before the next EXPLAIN on the same transaction,
				// so result resources do not remain active during plan inspection.
				require.NoError(t, rows.Close())
				if test.want == "" {
					require.Empty(t, got)
				} else {
					require.Equal(t, []string{test.want}, got)
				}
				var raw []byte
				require.NoError(t, tx.QueryRowContext(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+query, 0, test.search).Scan(&raw))
				// planNode retains the plan operation and relation needed to inspect nested
				// PostgreSQL scan nodes.
				type planNode struct {
					Type     string     `json:"Node Type"`     // PostgreSQL plan operation name
					Relation string     `json:"Relation Name"` // scanned PostgreSQL relation name
					Plans    []planNode `json:"Plans"`         // nested child plan operations
				}
				var plans []struct {
					Plan planNode `json:"Plan"` // root PostgreSQL plan node
				}
				// Decode EXPLAIN's nested JSON tree and inspect matching log-table nodes,
				// so the assertion tests their chosen scan operations rather than another
				// relation's plan.
				require.NoError(t, json.Unmarshal(raw, &plans))
				seen := false
				var inspect func(planNode)
				inspect = func(node planNode) {
					if node.Relation == table {
						seen = true
						require.NotEqual(t, "Seq Scan", node.Type, "%s", raw)
					}
					for _, child := range node.Plans {
						inspect(child)
					}
				}
				inspect(plans[0].Plan)
				require.True(t, seen)
			})
		}
	}
}

// TestGetErrorLogByID_APIKeyPrefixAndUpstreamStatus stores missing and present
// saved key prefixes plus a credential failure with upstream zero. It protects
// returned prefix snapshots and a nonnil zero status through real PostgreSQL
// reads.
func TestGetErrorLogByID_APIKeyPrefixAndUpstreamStatus(t *testing.T) {
	ctx := context.Background()
	// Clear the shared error fixture before these round trips. Setup truncation
	// errors are suppressed, while the subsequent required inserts and reads
	// determine this test.
	_, _ = integrationDB.ExecContext(ctx, "TRUNCATE ops_error_logs RESTART IDENTITY CASCADE")
	repo := NewOpsRepository(integrationDB)
	var plainID int64
	err := integrationDB.QueryRowContext(ctx, `
		INSERT INTO ops_error_logs (
			error_phase, error_type, severity, status_code, created_at
		) VALUES (
			'upstream', 'upstream_error', 'error', 500, NOW()
		) RETURNING id`,
	).Scan(&plainID)
	require.NoError(t, err)

	plain, err := repo.GetErrorLogByID(ctx, plainID)
	require.NoError(t, err)
	require.Empty(t, plain.APIKeyPrefix)
	validID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase:       "request",
		ErrorType:        "api_error",
		Severity:         "error",
		StatusCode:       402,
		ClientStatusCode: new(402),
		CreatedAt:        time.Now(),
		APIKeyPrefix:     "sk-valid",
	})
	require.NoError(t, err)

	valid, err := repo.GetErrorLogByID(ctx, validID)
	require.NoError(t, err)
	require.Equal(t, "sk-valid", valid.APIKeyPrefix)
	zero := 0
	credentialFailureID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase:         "provider_auth",
		ErrorType:          "upstream_error",
		Severity:           "error",
		StatusCode:         503,
		ClientStatusCode:   new(503),
		UpstreamStatusCode: &zero,
		CreatedAt:          time.Now(),
	})
	require.NoError(t, err)

	credentialFailure, err := repo.GetErrorLogByID(ctx, credentialFailureID)
	require.NoError(t, err)
	require.Equal(t, 503, credentialFailure.StatusCode)
	require.NotNil(t, credentialFailure.UpstreamStatusCode)
	require.Zero(t, *credentialFailure.UpstreamStatusCode)

	upstreamForbidden := 403
	failedUpstreamID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase:         "upstream",
		ErrorType:          "upstream_error",
		ErrorOwner:         "provider",
		Severity:           "error",
		StatusCode:         502,
		ClientStatusCode:   new(502),
		UpstreamStatusCode: &upstreamForbidden,
		CreatedAt:          time.Now(),
	})
	require.NoError(t, err)

	failedUpstream, err := repo.GetErrorLogByID(ctx, failedUpstreamID)
	require.NoError(t, err)
	require.Equal(t, 502, failedUpstream.StatusCode)
	require.NotNil(t, failedUpstream.UpstreamStatusCode)
	require.Equal(t, 403, *failedUpstream.UpstreamStatusCode)

	upstreamRateLimit := 429
	recoveredID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase:         "upstream",
		ErrorType:          "upstream_error",
		ErrorOwner:         "provider",
		Severity:           "warning",
		StatusCode:         200,
		ClientStatusCode:   new(200),
		UpstreamStatusCode: &upstreamRateLimit,
		CreatedAt:          time.Now(),
	})
	require.NoError(t, err)

	recoveredList, err := repo.ListErrorLogs(ctx, &ops.OpsErrorLogFilter{
		Phase:                    "upstream",
		IncludeRecoveredUpstream: true,
		StatusCodes:              []int{200},
		UpstreamStatusCodes:      []int{429},
		View:                     "all",
	})
	require.NoError(t, err)
	require.Len(t, recoveredList.Errors, 1)
	require.Equal(t, recoveredID, recoveredList.Errors[0].ID)
	require.Equal(t, 200, recoveredList.Errors[0].StatusCode)
	require.NotNil(t, recoveredList.Errors[0].UpstreamStatusCode)
	require.Equal(t, 429, *recoveredList.Errors[0].UpstreamStatusCode)

	// Other buckets must exclude client 200 on recovered rows and upstream 0 on
	// credential failures while retaining genuinely unlisted status codes.
	clientTeapotID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase:       "request",
		ErrorType:        "invalid_request_error",
		Severity:         "warning",
		StatusCode:       418,
		ClientStatusCode: new(418),
		CreatedAt:        time.Now(),
	})
	require.NoError(t, err)

	upstreamTimeout524 := 524
	upstreamOtherID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase:         "upstream",
		ErrorType:          "upstream_error",
		ErrorOwner:         "provider",
		Severity:           "error",
		StatusCode:         502,
		ClientStatusCode:   new(502),
		UpstreamStatusCode: &upstreamTimeout524,
		CreatedAt:          time.Now(),
	})
	require.NoError(t, err)

	clientOtherList, err := repo.ListErrorLogs(ctx, &ops.OpsErrorLogFilter{
		Phase:                    "upstream",
		IncludeRecoveredUpstream: true,
		StatusCodesOther:         true,
		View:                     "all",
	})
	require.NoError(t, err)
	for _, row := range clientOtherList.Errors {
		require.NotEqual(t, recoveredID, row.ID, "recovered client 200 must not match client Other")
	}

	allClientOtherList, err := repo.ListErrorLogs(ctx, &ops.OpsErrorLogFilter{
		StatusCodesOther: true,
		View:             "all",
	})
	require.NoError(t, err)
	clientOtherIDs := make([]int64, 0, len(allClientOtherList.Errors))
	for _, row := range allClientOtherList.Errors {
		clientOtherIDs = append(clientOtherIDs, row.ID)
	}
	require.Contains(t, clientOtherIDs, clientTeapotID)
	require.Contains(t, clientOtherIDs, validID)

	upstreamOtherList, err := repo.ListErrorLogs(ctx, &ops.OpsErrorLogFilter{
		UpstreamStatusCodesOther: true,
		View:                     "all",
	})
	require.NoError(t, err)
	upstreamOtherIDs := make([]int64, 0, len(upstreamOtherList.Errors))
	for _, row := range upstreamOtherList.Errors {
		upstreamOtherIDs = append(upstreamOtherIDs, row.ID)
	}
	require.NotContains(t, upstreamOtherIDs, credentialFailureID, "provider_auth upstream 0 must not match upstream Other")
	require.Contains(t, upstreamOtherIDs, upstreamOtherID)
}

// TestListAndDetailErrorLogs_StreamClientStatusAndHistoricalCyberPolicyFallback
// stores an SSE terminal error with wire status 200 and logical 400, a legacy
// stream error lacking client_status_code, and new/legacy/local cyber_policy
// rows. It protects client 200 filtering on SLA-admitted stream errors, legacy
// status fallback, and upstream 403 matching on upstream cyber_policy rows.
func TestListAndDetailErrorLogs_StreamClientStatusAndHistoricalCyberPolicyFallback(t *testing.T) {
	ctx := context.Background()
	var migrationCount int
	require.NoError(
		t,
		integrationDB.QueryRowContext(
			ctx,
			"SELECT COUNT(*) FROM schema_migrations WHERE filename = '318_ops_error_logs_client_status_code.sql'",
		).Scan(&migrationCount),
	)
	require.Equal(t, 1, migrationCount)

	// Reset shared error rows before inserting stream and policy fixtures.
	// Truncation errors are suppressed while subsequent inserts and queries govern
	// the test outcome.
	_, _ = integrationDB.ExecContext(ctx, "TRUNCATE ops_error_logs RESTART IDENTITY CASCADE")
	repo := NewOpsRepository(integrationDB)

	wireOK := 200
	logicalBadRequest := 400
	sseErrorID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase:         "request",
		ErrorType:          "invalid_request_error",
		ErrorOwner:         "provider",
		ErrorSource:        "upstream_http",
		Severity:           "P2",
		StatusCode:         logicalBadRequest,
		ClientStatusCode:   &wireOK,
		UpstreamStatusCode: &wireOK,
		Stream:             true,
		ErrorMessage:       "input exceeds the context window",
		CreatedAt:          time.Now().UTC(),
	})
	require.NoError(t, err)

	// An upgraded WebSocket returns a close frame. The stored logical 403
	// must stay available without matching a filter for a client HTTP 403.
	wsID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase: "request", ErrorType: "cyber_policy_session_blocked", Severity: "P3",
		StatusCode: 403, IsBusinessLimited: true, CreatedAt: time.Now().UTC(),
	})
	require.NoError(t, err)
	ws, err := repo.GetErrorLogByID(ctx, wsID)
	require.NoError(t, err)
	require.Nil(t, ws.ClientStatusCode)
	require.Zero(t, ws.StatusCode)
	require.Equal(t, 403, ws.LogicalStatusCode)
	clientForbidden, err := repo.ListErrorLogs(ctx, &ops.OpsErrorLogFilter{StatusCodes: []int{403}, View: "all"})
	require.NoError(t, err)
	require.Empty(t, clientForbidden.Errors)

	var legacyStreamID int64
	err = integrationDB.QueryRowContext(ctx, `
		INSERT INTO ops_error_logs (
			error_phase, error_type, error_owner, error_source, severity,
			status_code, upstream_status_code, stream, error_message, created_at
		) VALUES (
			'request', 'invalid_request_error', 'provider', 'upstream_http', 'P2',
			400, 400, true, 'legacy stream failure', NOW()
		) RETURNING id`,
	).Scan(&legacyStreamID)
	require.NoError(t, err)

	sseDetail, err := repo.GetErrorLogByID(ctx, sseErrorID)
	require.NoError(t, err)
	require.Equal(t, 200, sseDetail.StatusCode)
	require.NotNil(t, sseDetail.ClientStatusCode)
	require.Equal(t, 200, *sseDetail.ClientStatusCode)
	require.Equal(t, 400, sseDetail.LogicalStatusCode)
	require.NotNil(t, sseDetail.UpstreamStatusCode)
	require.Equal(t, 200, *sseDetail.UpstreamStatusCode)

	legacyDetail, err := repo.GetErrorLogByID(ctx, legacyStreamID)
	require.NoError(t, err)
	require.Equal(t, 400, legacyDetail.StatusCode)
	require.Nil(t, legacyDetail.ClientStatusCode)
	require.Equal(t, 400, legacyDetail.LogicalStatusCode)

	client200Errors, err := repo.ListErrorLogs(ctx, &ops.OpsErrorLogFilter{
		StatusCodes: []int{200},
		View:        "errors",
	})
	require.NoError(t, err)
	require.Len(t, client200Errors.Errors, 1)
	require.Equal(t, sseErrorID, client200Errors.Errors[0].ID)
	require.Equal(t, 200, client200Errors.Errors[0].StatusCode)
	require.Equal(t, 400, client200Errors.Errors[0].LogicalStatusCode)

	upstreamForbidden := 403
	newCyberID, err := repo.InsertErrorLog(ctx, &ops.OpsInsertErrorLogInput{
		ErrorPhase:         "request",
		ErrorType:          "cyber_policy",
		ErrorOwner:         "provider",
		ErrorSource:        "upstream_http",
		Severity:           "P3",
		StatusCode:         403,
		ClientStatusCode:   &wireOK,
		UpstreamStatusCode: &upstreamForbidden,
		IsBusinessLimited:  true,
		Stream:             true,
		ErrorMessage:       "cyber_policy: streamed block",
		CreatedAt:          time.Now().UTC(),
	})
	require.NoError(t, err)

	var legacyCyberID int64
	err = integrationDB.QueryRowContext(ctx, `
		INSERT INTO ops_error_logs (
			error_phase, error_type, error_owner, error_source, severity,
			status_code, is_business_limited, error_message, created_at
		) VALUES (
			'request', 'cyber_policy', 'provider', 'upstream_http', 'P3',
			403, true, 'cyber_policy: legacy block', NOW()
		) RETURNING id`,
	).Scan(&legacyCyberID)
	require.NoError(t, err)

	var localSessionBlockID int64
	err = integrationDB.QueryRowContext(ctx, `
		INSERT INTO ops_error_logs (
			error_phase, error_type, error_owner, error_source, severity,
			status_code, is_business_limited, error_message, created_at
		) VALUES (
			'request', 'cyber_policy_session_blocked', 'platform', 'gateway_local', 'P3',
			403, true, 'cyber_policy_session_blocked: local block', NOW()
		) RETURNING id`,
	).Scan(&localSessionBlockID)
	require.NoError(t, err)

	legacyCyberDetail, err := repo.GetErrorLogByID(ctx, legacyCyberID)
	require.NoError(t, err)
	require.Equal(t, 403, legacyCyberDetail.StatusCode)
	require.NotNil(t, legacyCyberDetail.UpstreamStatusCode)
	require.Equal(t, 403, *legacyCyberDetail.UpstreamStatusCode)

	localBlockDetail, err := repo.GetErrorLogByID(ctx, localSessionBlockID)
	require.NoError(t, err)
	require.Equal(t, 403, localBlockDetail.StatusCode)
	require.Nil(t, localBlockDetail.UpstreamStatusCode)

	excludedUpstream403, err := repo.ListErrorLogs(ctx, &ops.OpsErrorLogFilter{
		UpstreamStatusCodes: []int{403},
		View:                "excluded",
	})
	require.NoError(t, err)
	matchedIDs := make([]int64, 0, len(excludedUpstream403.Errors))
	for _, row := range excludedUpstream403.Errors {
		matchedIDs = append(matchedIDs, row.ID)
		require.NotNil(t, row.UpstreamStatusCode)
		require.Equal(t, 403, *row.UpstreamStatusCode)
	}
	require.ElementsMatch(t, []int64{newCyberID, legacyCyberID}, matchedIDs)
}

// TestOpsRepositoryBatchInsertErrorLogs inserts two prepared observations with
// distinct request IDs and phases. It protects the successful transactional
// batch count and two stored matching rows in PostgreSQL.
func TestOpsRepositoryBatchInsertErrorLogs(t *testing.T) {
	ctx := context.Background()
	// Reset shared error rows before the batch fixture. Setup truncation errors
	// are suppressed; assertions count the two distinct request IDs submitted
	// here.
	_, _ = integrationDB.ExecContext(ctx, "TRUNCATE ops_error_logs RESTART IDENTITY")

	repo := NewOpsRepository(integrationDB)
	now := time.Now().UTC()
	inserted, err := repo.BatchInsertErrorLogs(ctx, []*ops.OpsInsertErrorLogInput{
		{
			RequestID:    "batch-ops-1",
			ErrorPhase:   "upstream",
			ErrorType:    "upstream_error",
			Severity:     "error",
			StatusCode:   429,
			ErrorMessage: "rate limited",
			CreatedAt:    now,
		},
		{
			RequestID:    "batch-ops-2",
			ErrorPhase:   "internal",
			ErrorType:    "api_error",
			Severity:     "error",
			StatusCode:   500,
			ErrorMessage: "internal error",
			CreatedAt:    now.Add(time.Millisecond),
		},
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, inserted)

	var count int
	require.NoError(t, integrationDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM ops_error_logs WHERE request_id IN ('batch-ops-1', 'batch-ops-2')").Scan(&count))
	require.Equal(t, 2, count)
}
