package proxy

import "strings"

const alphaSearchEndpoint = "/v1/alpha/search"

type SearchSessionContext struct {
	IDHash string
}

func isAlphaSearchEndpoint(intent RequestIntent) bool {
	return intent.Endpoint == alphaSearchEndpoint || intent.UpstreamEndpoint == alphaSearchEndpoint
}

func buildSearchSessionContext(intent RequestIntent, body []byte) *SearchSessionContext {
	return buildSearchSessionDocument(intent, newRequestDocument(body, ""))
}

func buildSearchSessionDocument(intent RequestIntent, document *RequestDocument) *SearchSessionContext {
	if !isAlphaSearchEndpoint(intent) {
		return nil
	}
	payload, err := document.StrictObject()
	if err != nil {
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
