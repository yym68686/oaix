package store

import (
	"strings"
	"testing"
)

func TestImportItemSelectColumnsMatchesScanner(t *testing.T) {
	parts := importItemSelectColumnList("i")
	if len(parts) != 28 {
		t.Fatalf("import item scanner expects 28 columns, got %d: %v", len(parts), parts)
	}
	columns := importItemSelectColumns("i")
	required := []string{
		"i.matched_existing_token_id",
		"i.publish_attempted",
		"i.publish_skipped_reason",
		"i.reactivated",
		"i.previous_is_active",
		"i.next_is_active",
		"i.previous_disabled_at",
		"i.next_disabled_at",
		"i.refresh_error_code",
		"i.refresh_error_message_excerpt",
	}
	for _, fragment := range required {
		if !strings.Contains(columns, fragment) {
			t.Fatalf("missing import item column %q in %s", fragment, columns)
		}
	}
}
