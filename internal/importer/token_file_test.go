package importer

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadAccessTokenFileSkipsRedactedCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tokens.txt")
	if err := os.WriteFile(path, []byte("...\n.\naccess-one\n{\"access_token\":\"***\"}\n{\"access_token\":\"access-two\"}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	tokens, err := ReadAccessTokenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2 || tokens[0] != "access-one" || tokens[1] != "access-two" {
		t.Fatalf("tokens = %#v", tokens)
	}
}
