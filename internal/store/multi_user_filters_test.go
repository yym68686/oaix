package store

import (
	"strings"
	"testing"
)

func TestRequestLogWhereScopedSupportsOwnerAndAPIKeyFilters(t *testing.T) {
	ownerID := int64(17)
	where, args := requestLogWhereScoped(RequestLogListOptions{OwnerUserID: ownerID, APIKeyID: 23}, AllResources())
	if len(args) != 2 || args[0] != ownerID || args[1] != int64(23) {
		t.Fatalf("args = %#v", args)
	}
	if !strings.Contains(where, "owner_user_id = $1") || !strings.Contains(where, "api_key_id = $2") {
		t.Fatalf("expected owner/api key filters in where: %s", where)
	}
}

func TestPlatformUserWhereSupportsActivityFilters(t *testing.T) {
	where, args := platformUserWhere(PlatformUserListOptions{
		Query:             "alice",
		Role:              "user",
		Status:            "active",
		Plan:              "pro",
		ActiveWithinHours: 24,
		InactiveForHours:  168,
	})
	if len(args) != 7 {
		t.Fatalf("args length = %d, args=%#v", len(args), args)
	}
	for _, fragment := range []string{
		"lower(coalesce(email, '')) like $1",
		"role = $3",
		"status = $4",
		"coalesce(plan, '') = $5",
		"last_seen_at >= now()",
		"last_seen_at is null",
	} {
		if !strings.Contains(where, fragment) {
			t.Fatalf("missing %q in where: %s", fragment, where)
		}
	}
}
