package store

import (
	"strings"
	"testing"
)

func TestSaveQuotaSnapshotPersistsTokenOwner(t *testing.T) {
	sql := compactSQL(saveQuotaSnapshotSQL)
	for _, fragment := range []string{
		"insert into token_quota_snapshots(token_id, owner_user_id",
		"select id, owner_user_id",
		"from codex_tokens",
		"where id = $1",
	} {
		if !strings.Contains(sql, fragment) {
			t.Fatalf("quota snapshot write missing owner fragment %q: %s", fragment, sql)
		}
	}
}

func TestTokenModelCapabilityScopeIsExactAndBounded(t *testing.T) {
	if got := TokenModelCapabilityScope(" GPT-5.6-SOL "); got != "gpt-5.6-sol:model-compatibility" {
		t.Fatalf("scope = %q", got)
	}
	if got := TokenModelCapabilityScope(""); got != "" {
		t.Fatalf("empty model scope = %q", got)
	}
	if got := TokenModelCapabilityScope(strings.Repeat("x", 128)); got != "" {
		t.Fatalf("oversized model scope = %q", got)
	}
}
