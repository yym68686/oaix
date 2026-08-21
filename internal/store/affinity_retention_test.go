package store

import (
	"os"
	"strings"
	"testing"
)

func TestAffinityRetentionIsBoundedAndExpiredOnly(t *testing.T) {
	source, err := os.ReadFile("affinity_retention.go")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(string(source))
	for _, fragment := range []string{
		"expires_at <= now()",
		"for update skip locked",
		"limit $1",
		"prompt_affinity_lanes",
		"response_owner_bindings",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("affinity retention is missing bounded fragment %q", fragment)
		}
	}
}

func TestRequestAttemptRetentionRequiresCompletedRowsAndUsesTTL(t *testing.T) {
	source, err := os.ReadFile("affinity_retention.go")
	if err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(string(source))
	for _, fragment := range []string{
		"gateway_request_attempts",
		"started_at < now() - ($1::int * interval '1 day')",
		"finished_at is not null",
		"order by started_at asc, id asc",
		"for update skip locked",
	} {
		if !strings.Contains(text, fragment) {
			t.Fatalf("request attempt retention is missing %q", fragment)
		}
	}
}
