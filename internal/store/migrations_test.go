package store

import (
	"strings"
	"testing"
)

func TestMigrationStatementsAreIdempotent(t *testing.T) {
	for index, statement := range migrationStatements {
		normalized := strings.ToLower(strings.Join(strings.Fields(statement), " "))
		idempotent := strings.Contains(normalized, "if not exists") ||
			strings.Contains(normalized, "on conflict") ||
			strings.HasPrefix(normalized, "insert into schema_migrations") ||
			strings.HasPrefix(normalized, "alter table ") ||
			(strings.HasPrefix(normalized, "update ") && strings.Contains(normalized, " is null"))
		if !idempotent {
			t.Fatalf("migration statement %d is not obviously idempotent: %s", index+1, statement)
		}
	}
}

func TestMigrationRepairsImportJobAndTokenDefaults(t *testing.T) {
	joined := strings.ToLower(strings.Join(migrationStatements, "\n"))
	required := []string{
		"alter table codex_tokens alter column type set default 'codex'",
		"alter table token_import_jobs alter column processed_count set default 0",
		"alter table token_import_jobs alter column created_count set default 0",
		"alter table token_import_jobs alter column updated_count set default 0",
		"alter table token_import_jobs alter column skipped_count set default 0",
		"alter table token_import_jobs alter column failed_count set default 0",
		"alter table token_import_jobs add column if not exists yielded_to_response_traffic_count integer",
		"alter table token_import_jobs alter column yielded_to_response_traffic_count set default 0",
		"alter table token_import_jobs alter column response_traffic_timeout_count set default 0",
		"alter table token_import_items alter column status set default 'queued'",
	}
	for _, fragment := range required {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("missing migration repair fragment %q", fragment)
		}
	}
}

func TestOnlineMigrationStatementsUseConcurrentIndexes(t *testing.T) {
	if len(onlineMigrationStatements) == 0 {
		t.Fatal("online migration statements are empty")
	}
	for index, statement := range onlineMigrationStatements {
		normalized := strings.ToLower(strings.Join(strings.Fields(statement), " "))
		if !strings.Contains(normalized, "create index concurrently if not exists") {
			t.Fatalf("online migration statement %d is not concurrent and idempotent: %s", index+1, statement)
		}
	}
}

func TestSharedTokenIndexRunsAfterShareColumnsExist(t *testing.T) {
	for index, statement := range migrationStatements {
		normalized := strings.ToLower(strings.Join(strings.Fields(statement), " "))
		if strings.Contains(normalized, "ix_codex_tokens_shared_ready") {
			t.Fatalf("migration statement %d creates shared token index before share columns are guaranteed to exist", index+1)
		}
	}
	joinedOnline := strings.ToLower(strings.Join(onlineMigrationStatements, "\n"))
	if !strings.Contains(joinedOnline, "ix_codex_tokens_shared_ready") {
		t.Fatal("online migrations must create shared token readiness index")
	}
}

func TestMigrationDropsLegacyHourlyStatsConstraints(t *testing.T) {
	joined := strings.ToLower(strings.Join(migrationStatements, "\n"))
	required := []string{
		"alter table gateway_request_hourly_stats drop constraint if exists gateway_request_hourly_stats_bucket_start_model_name_key",
		"alter table gateway_request_hourly_stats drop constraint if exists uq_gateway_request_hourly_stats_bucket_model",
		"create unique index if not exists ux_gateway_request_hourly_stats_owner_bucket_model",
	}
	for _, fragment := range required {
		if !strings.Contains(joined, fragment) {
			t.Fatalf("missing hourly stats migration fragment %q", fragment)
		}
	}
}

func TestDownMigrationIsExplicitlyDestructive(t *testing.T) {
	if len(downMigrationStatements) == 0 {
		t.Fatal("down migration statements are empty")
	}
	foundSchemaDelete := false
	for _, statement := range downMigrationStatements {
		if strings.Contains(strings.ToLower(statement), "schema_migrations") {
			foundSchemaDelete = true
			break
		}
	}
	if !foundSchemaDelete {
		t.Fatal("down migration must remove schema_migrations marker")
	}
}
