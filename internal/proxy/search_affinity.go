package proxy

import (
	"encoding/json"
	"net/http"
	"strings"
)

const alphaSearchEndpoint = "/v1/alpha/search"

type SearchAffinityContext struct {
	AffinityKey string
	IDHash      string
}

func isAlphaSearchEndpoint(intent RequestIntent) bool {
	return intent.Endpoint == alphaSearchEndpoint || intent.UpstreamEndpoint == alphaSearchEndpoint
}

func buildSearchAffinityContext(headers http.Header, intent RequestIntent, body []byte) *SearchAffinityContext {
	if !isAlphaSearchEndpoint(intent) {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil || payload == nil {
		return nil
	}
	id, _ := payload["id"].(string)
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	seed := map[string]any{
		"kind":   "alpha_search",
		"client": clientScopeHash(headers),
		"id":     id,
	}
	if intent.OwnerUserID > 0 {
		seed["owner_user_id"] = intent.OwnerUserID
	}
	if intent.APIKeyID != nil && *intent.APIKeyID > 0 {
		seed["api_key_id"] = *intent.APIKeyID
	}
	return &SearchAffinityContext{
		AffinityKey: "oaix:alpha-search:" + shortHash(mustSeedJSON(seed), 64),
		IDHash:      shortHash(id, 32),
	}
}
