package proxy

import (
	"net/http"
	"testing"
)

func TestBuildSearchAffinityContextIsStableAndScoped(t *testing.T) {
	body := []byte(`{"id":"session-contract-001","model":"gpt-5.4","commands":{"search_query":[{"q":"news"}]}}`)
	headers := http.Header{}
	headers.Set("Authorization", "Bearer caller-a")
	apiKeyID := int64(101)
	intent := RequestIntent{Endpoint: alphaSearchEndpoint, OwnerUserID: 10, APIKeyID: &apiKeyID}

	first := buildSearchAffinityContext(headers, intent, body)
	second := buildSearchAffinityContext(headers, intent, body)
	if first == nil || second == nil {
		t.Fatal("expected search affinity context")
	}
	if first.AffinityKey == "" || first.IDHash == "" || first.AffinityKey != second.AffinityKey {
		t.Fatalf("search affinity is not stable: %+v vs %+v", first, second)
	}
	if first.AffinityKey == "session-contract-001" || first.IDHash == "session-contract-001" {
		t.Fatal("raw search id must not be stored in affinity metadata")
	}

	otherOwner := intent
	otherOwner.OwnerUserID = 11
	if got := buildSearchAffinityContext(headers, otherOwner, body); got == nil || got.AffinityKey == first.AffinityKey {
		t.Fatal("owner scope did not change search affinity key")
	}
	otherHeaders := headers.Clone()
	otherHeaders.Set("Authorization", "Bearer caller-b")
	if got := buildSearchAffinityContext(otherHeaders, intent, body); got == nil || got.AffinityKey == first.AffinityKey {
		t.Fatal("caller scope did not change search affinity key")
	}
	if got := buildSearchAffinityContext(headers, RequestIntent{Endpoint: "/v1/responses"}, body); got != nil {
		t.Fatalf("non-search request created search affinity: %+v", got)
	}
}
