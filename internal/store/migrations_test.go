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
			strings.HasPrefix(normalized, "insert into schema_migrations")
		if !idempotent {
			t.Fatalf("migration statement %d is not obviously idempotent: %s", index+1, statement)
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
