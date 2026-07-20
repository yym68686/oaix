package proxy

import "testing"

func TestBuildSearchSessionContextHashesIDForObservability(t *testing.T) {
	body := []byte(`{"id":"session-contract-001","model":"gpt-5.4","commands":{"search_query":[{"q":"news"}]}}`)
	intent := RequestIntent{Endpoint: alphaSearchEndpoint}

	first := buildSearchSessionContext(intent, body)
	second := buildSearchSessionContext(intent, body)
	if first == nil || second == nil {
		t.Fatal("expected search session context")
	}
	if first.IDHash == "" || first.IDHash != second.IDHash {
		t.Fatalf("search session hash is not stable: %+v vs %+v", first, second)
	}
	if first.IDHash == "session-contract-001" {
		t.Fatal("raw search id must not be stored in observability metadata")
	}

	otherBody := []byte(`{"id":"session-contract-002","model":"gpt-5.4"}`)
	if got := buildSearchSessionContext(intent, otherBody); got == nil || got.IDHash == first.IDHash {
		t.Fatal("different search ids must have different observability hashes")
	}
	if got := buildSearchSessionContext(RequestIntent{Endpoint: "/v1/responses"}, body); got != nil {
		t.Fatalf("non-search request created search session context: %+v", got)
	}
}
