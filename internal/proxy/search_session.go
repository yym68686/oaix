package proxy

import (
	"encoding/json"
	"strings"
)

const alphaSearchEndpoint = "/v1/alpha/search"

type SearchSessionContext struct {
	IDHash string
}

func isAlphaSearchEndpoint(intent RequestIntent) bool {
	return intent.Endpoint == alphaSearchEndpoint || intent.UpstreamEndpoint == alphaSearchEndpoint
}

func buildSearchSessionContext(intent RequestIntent, body []byte) *SearchSessionContext {
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
	return &SearchSessionContext{
		IDHash: shortHash(id, 32),
	}
}
