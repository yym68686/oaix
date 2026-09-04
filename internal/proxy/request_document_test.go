package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

func TestRequestDocumentPreservesNumbersDuplicateKeysAndUnknownFields(t *testing.T) {
	body := []byte(`{
		"model":"ignored",
		"model":"gpt-5.5",
		"large":9007199254740993,
		"future":{"enabled":true,"nested":{"keep":"yes"}},
		"prompt_cache_key":"explicit-key",
		"input":[{"role":"user","content":"hello","reasoning_content":"remove"}],
		"stream":true
	}`)
	document := newRequestDocument(body, fmt.Sprintf("%x", sha256.Sum256(body)))
	payload, err := document.Object()
	if err != nil {
		t.Fatal(err)
	}
	if payload["model"] != "gpt-5.5" {
		t.Fatalf("duplicate model did not use the final value: %v", payload["model"])
	}
	large, ok := payload["large"].(json.Number)
	if !ok || large.String() != "9007199254740993" {
		t.Fatalf("large number lost precision or type: %T %v", payload["large"], payload["large"])
	}

	intent := normalizeIntentDocument(RequestIntent{Endpoint: "/v1/responses"}, document)
	if intent.Model != "gpt-5.5" || !intent.Stream {
		t.Fatalf("intent did not reuse document fields: %+v", intent)
	}
	if err := prepareResponsesDocument(document, &intent); err != nil {
		t.Fatal(err)
	}
	cache := buildPromptCacheRoutingContext(http.Header{}, intent, document, defaultPromptCacheConfig())
	if cache == nil || cache.PromptCacheKey != "explicit-key" {
		t.Fatalf("prompt cache routing did not reuse document: %+v", cache)
	}
	if err := finalizePromptCacheContext(cache, document); err != nil {
		t.Fatal(err)
	}
	ids := &codexFingerprintIDs{
		InstallationID: "install", SessionID: "session", ThreadID: "thread",
		TurnID: "turn", WindowID: "window", TurnStartedAt: 123,
	}
	if err := applyCodexFingerprintDocument(document, ids); err != nil {
		t.Fatal(err)
	}
	encoded, err := document.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	second, err := document.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, second) || document.encodes != 1 {
		t.Fatalf("document encoded %d times", document.encodes)
	}

	decoded := newRequestDocument(encoded, "")
	upstream, err := decoded.Object()
	if err != nil {
		t.Fatal(err)
	}
	if number, ok := upstream["large"].(json.Number); !ok || number.String() != large.String() {
		t.Fatalf("encoded number changed: %T %v", upstream["large"], upstream["large"])
	}
	future, _ := upstream["future"].(map[string]any)
	if nested, _ := future["nested"].(map[string]any); future["enabled"] != true || nested["keep"] != "yes" {
		t.Fatalf("unknown fields were not preserved: %v", future)
	}
	if containsReasoningContent(upstream) {
		t.Fatalf("reasoning content was not sanitized: %v", upstream)
	}
	metadata, _ := upstream["client_metadata"].(map[string]any)
	if metadata["thread_id"] != "thread" {
		t.Fatalf("fingerprint was not injected: %v", metadata)
	}
}

func TestRequestDocumentCanonicalHashMatchesExistingSeedEncoding(t *testing.T) {
	document := newRequestDocument([]byte(`{"z":"<html>","n":9007199254740993,"a":[true,null]}`), "")
	payload, err := document.Object()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := document.CanonicalHash(), hashPayload(payload); got != want {
		t.Fatalf("canonical hash = %s, want %s", got, want)
	}
}

func TestPromptCacheUpstreamHashUsesEncodedWireBytes(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","prompt_cache_key":"cache","input":"hello"}`)
	ctx, upstream := buildPromptCacheContext(http.Header{}, RequestIntent{Endpoint: "/v1/responses", Model: "gpt-5.5"}, body, defaultPromptCacheConfig())
	if ctx == nil {
		t.Fatal("expected prompt cache context")
	}
	if got, want := ctx.UpstreamPayloadHash, sha256Bytes(upstream); got != want {
		t.Fatalf("upstream payload hash=%s, want wire-byte hash=%s", got, want)
	}
}

func TestRequestDocumentStrictAndStreamingConsumersPreserveTrailingJSONSemantics(t *testing.T) {
	document := newRequestDocument([]byte(`{"model":"gpt-5.5"} {"extra":true}`), "")
	if payload, err := document.Object(); err != nil || payload["model"] != "gpt-5.5" {
		t.Fatalf("streaming object consumer changed behavior: payload=%v err=%v", payload, err)
	}
	if _, err := document.StrictObject(); err == nil {
		t.Fatal("strict object consumer accepted trailing JSON")
	}
	intent := normalizeIntentDocument(RequestIntent{Endpoint: "/v1/responses"}, document)
	if intent.Model != "" {
		t.Fatalf("strict typed intent unexpectedly accepted trailing JSON: %+v", intent)
	}
	if serviceIntent := normalizeIntentDocument(RequestIntent{Endpoint: "/v1/responses"}, newRequestDocument([]byte(`{"service_tier":"priority"} trailing`), "")); !serviceIntent.RequireFast {
		t.Fatalf("streaming service-tier extraction changed behavior: %+v", serviceIntent)
	}
}

func TestReadProxyRequestBodyComputesDecodedSHAWhileReading(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","input":"hello"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(zstdEncodeForTest(t, body)))
	request.Header.Set("Content-Encoding", "zstd")
	result, status, message, err := readProxyRequestBodyWithDigest(request, 1<<20)
	if err != nil {
		t.Fatalf("read body: status=%d message=%q err=%v", status, message, err)
	}
	if !bytes.Equal(result.Bytes, body) || result.SHA256 != sha256Bytes(body) {
		t.Fatalf("decoded body/hash mismatch: body=%q hash=%s", result.Bytes, result.SHA256)
	}
}

func TestProxyNoTokenSkipsDeferredCompatibilityForExplicitPromptCacheKey(t *testing.T) {
	var upstreamCalls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	pipeline := newProxyPipelineTestHarness(t, upstream.URL, 1, &fakeProxyStore{})
	pipeline.cfg.PromptCache = defaultPromptCacheConfig()

	// This payload is valid for routing but its gpt-image-2 compact compatibility
	// rewrite would fail. With no token, that non-routing work must not run.
	body := `{"model":"gpt-image-2","prompt_cache_key":"explicit","input":"draw","stream":false}`
	request := httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	pipeline.Proxy(response, request, RequestIntent{Endpoint: "/v1/responses/compact", Compact: true})

	if response.Code != http.StatusServiceUnavailable || !strings.Contains(response.Body.String(), "no available token") {
		t.Fatalf("response=%d body=%q, want no-token response before compatibility", response.Code, response.Body.String())
	}
	if upstreamCalls != 0 {
		t.Fatalf("upstream calls=%d, want 0", upstreamCalls)
	}
}

func TestExplicitPromptCacheRoutingMatchesPostCompatibilityContext(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		compact  bool
		model    string
		body     string
	}{
		{
			name:     "responses strips previous response",
			endpoint: "/v1/responses",
			body:     `{"model":"gpt-5.5","prompt_cache_key":"cache","previous_response_id":"resp","input":"hello"}`,
		},
		{
			name:     "image compatibility preserves previous response",
			endpoint: "/v1/responses",
			model:    "gpt-5.5",
			body:     `{"model":"gpt-image-2","prompt_cache_key":"cache","previous_response_id":"resp","input":"draw"}`,
		},
		{
			name:     "compact responses",
			endpoint: "/v1/responses/compact",
			compact:  true,
			body:     `{"model":"gpt-5.5","prompt_cache_key":"cache","previous_response_id":"resp","input":"hello"}`,
		},
		{
			name:     "chat completions",
			endpoint: "/v1/chat/completions",
			body:     `{"model":"gpt-5.5","prompt_cache_key":"cache","previous_response_id":"resp","messages":[]}`,
		},
	}
	headers := http.Header{"Authorization": []string{"Bearer caller"}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			intent := normalizeIntentDocument(RequestIntent{Endpoint: tt.endpoint, Compact: tt.compact, Model: tt.model, OwnerUserID: 42}, newRequestDocument([]byte(tt.body), ""))
			quickDocument := newRequestDocument([]byte(tt.body), "")
			quick := buildExplicitPromptCacheRoutingContext(headers, intent, quickDocument, defaultPromptCacheConfig())
			if quick == nil {
				t.Fatal("expected explicit routing context")
			}

			preparedDocument := newRequestDocument([]byte(tt.body), "")
			preparedIntent, _, err := prepareUpstreamDocument(httptest.NewRequest(http.MethodPost, tt.endpoint, nil), preparedDocument, intent)
			if err != nil {
				t.Fatal(err)
			}
			full := buildPromptCacheRoutingContext(headers, preparedIntent, preparedDocument, defaultPromptCacheConfig())
			if full == nil {
				t.Fatal("expected full routing context")
			}
			if quick.Model != full.Model || quick.PromptCacheKey != full.PromptCacheKey ||
				quick.PromptCacheKeyHash != full.PromptCacheKeyHash || quick.AffinityKey != full.AffinityKey ||
				quick.PreviousResponseID != full.PreviousResponseID || quick.SessionID != full.SessionID {
				t.Fatalf("quick routing context differs\nquick=%+v\nfull=%+v", quick, full)
			}
		})
	}
}

func TestGatewayIdempotencyPrecomputedBodyDigestMatchesBodyWrapper(t *testing.T) {
	body := []byte(`{"model":"gpt-5.5","input":"same"}`)
	intent := RequestIntent{Endpoint: "/v1/responses", OwnerUserID: 42}
	headers := http.Header{"Authorization": []string{"Bearer credential"}}
	fromBody, err := gatewayIdempotencyRequestHash(intent, body, headers, nil)
	if err != nil {
		t.Fatal(err)
	}
	fromDigest, err := gatewayIdempotencyRequestHashWithBodyDigest(intent, sha256Bytes(body), headers, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fromBody != fromDigest {
		t.Fatalf("precomputed body digest changed idempotency hash: %s != %s", fromBody, fromDigest)
	}
}

func TestSharedRequestDocumentMatchesReparsePreparationSemantics(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		intent   RequestIntent
		body     string
	}{
		{
			name:     "responses explicit cache and duplicate model",
			endpoint: "/v1/responses",
			intent:   RequestIntent{Endpoint: "/v1/responses", OwnerUserID: 42},
			body:     `{"model":"ignored","model":"gpt-5.5","prompt_cache_key":"cache","input":[{"role":"user","content":"hello","reasoning_content":"hidden"}],"future":{"large":9007199254740993}}`,
		},
		{
			name:     "responses derived cache",
			endpoint: "/v1/responses",
			intent:   RequestIntent{Endpoint: "/v1/responses"},
			body:     `{"model":"gpt-5.5","instructions":"system","input":"hello","stream":false}`,
		},
		{
			name:     "compact responses",
			endpoint: "/v1/responses/compact",
			intent:   RequestIntent{Endpoint: "/v1/responses/compact", Compact: true},
			body:     `{"model":"gpt-5.5","prompt_cache_key":"cache","input":[],"stream":false}`,
		},
		{
			name:     "chat fast",
			endpoint: "/v1/chat/completions",
			intent:   RequestIntent{Endpoint: "/v1/chat/completions"},
			body:     `{"model":"gpt-5.5","service_tier":"fast","prompt_cache_key":"cache","messages":[{"role":"user","content":"hello","reasoning_content":"hidden"}]}`,
		},
		{
			name:     "image generation",
			endpoint: "/v1/images/generations",
			intent:   RequestIntent{Endpoint: "/v1/images/generations"},
			body:     `{"model":"gpt-image-2","prompt":"draw","size":"1024x1024","future":"ignored by compatibility"}`,
		},
		{
			name:     "alpha search",
			endpoint: alphaSearchEndpoint,
			intent:   RequestIntent{Endpoint: alphaSearchEndpoint},
			body:     `{"id":"search","model":"gpt-5.5","prompt_cache_key":"remove","commands":{"search_query":[{"q":"news"}]}}`,
		},
		{
			name:     "generic",
			endpoint: "/v1/future",
			intent:   RequestIntent{Endpoint: "/v1/future"},
			body:     `{"model":"future","payload":{"reasoning_content":"remove","keep":true}}`,
		},
	}
	headers := http.Header{"Authorization": []string{"Bearer caller"}}
	ids := &codexFingerprintIDs{InstallationID: "i", SessionID: "s", ThreadID: "t", TurnID: "u", WindowID: "w", TurnStartedAt: 123}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, tt.endpoint, nil)
			request.Header = headers.Clone()

			legacyBody := []byte(tt.body)
			if !isAlphaSearchEndpoint(tt.intent) {
				legacyBody, _ = sanitizeReasoningContentBody(legacyBody)
			}
			legacyIntent := normalizeIntent(tt.intent, legacyBody)
			var status int
			var err error
			legacyBody, legacyIntent, status, err = prepareUpstreamPayload(request, legacyBody, legacyIntent)
			if err != nil {
				t.Fatalf("legacy preparation status=%d err=%v", status, err)
			}
			legacyCache, legacyBody := buildPromptCacheContext(headers, legacyIntent, legacyBody, defaultPromptCacheConfig())
			if codexFingerprintEligible(legacyIntent) {
				legacyBody, err = applyCodexFingerprintBody(legacyBody, ids)
				if err != nil {
					t.Fatal(err)
				}
			}
			completePromptCacheContext(legacyCache, legacyBody)

			document := newRequestDocument([]byte(tt.body), "")
			sharedIntent := normalizeIntentDocument(tt.intent, document)
			sharedCache := (*PromptCacheContext)(nil)
			if hasExplicitPromptCacheKey(document, sharedIntent) {
				sharedCache = buildExplicitPromptCacheRoutingContext(headers, sharedIntent, document, defaultPromptCacheConfig())
			}
			if !isAlphaSearchEndpoint(sharedIntent) && !isResponsesEndpoint(sharedIntent) {
				sanitizeReasoningContentDocument(document)
			}
			sharedIntent, status, err = prepareUpstreamDocument(request, document, sharedIntent)
			if err != nil {
				t.Fatalf("shared preparation status=%d err=%v", status, err)
			}
			if sharedCache == nil {
				sharedCache = buildPromptCacheRoutingContext(headers, sharedIntent, document, defaultPromptCacheConfig())
			}
			if err := finalizePromptCacheContext(sharedCache, document); err != nil {
				t.Fatal(err)
			}
			if codexFingerprintEligible(sharedIntent) {
				if err := applyCodexFingerprintDocument(document, ids); err != nil {
					t.Fatal(err)
				}
			}
			sharedBody, err := document.Bytes()
			if err != nil {
				t.Fatal(err)
			}
			completePromptCacheContext(sharedCache, sharedBody)

			if !bytes.Equal(sharedBody, legacyBody) {
				t.Fatalf("wire body changed\nshared=%s\nlegacy=%s", sharedBody, legacyBody)
			}
			if !reflect.DeepEqual(sharedIntent, legacyIntent) {
				t.Fatalf("intent changed\nshared=%+v\nlegacy=%+v", sharedIntent, legacyIntent)
			}
			if !reflect.DeepEqual(sharedCache, legacyCache) {
				t.Fatalf("prompt cache context changed\nshared=%+v\nlegacy=%+v", sharedCache, legacyCache)
			}
		})
	}
}

func BenchmarkRequestPreparationExplicitPromptCache(b *testing.B) {
	messages := make([]any, 0, 400)
	for index := 0; index < cap(messages); index++ {
		messages = append(messages, map[string]any{
			"role": "user", "content": strings.Repeat("request-content-", 12),
			"future": map[string]any{"index": index, "enabled": true},
		})
	}
	body, err := json.Marshal(map[string]any{
		"model": "gpt-5.5", "prompt_cache_key": "benchmark-cache-key",
		"messages": messages, "stream": true, "future_top_level": json.Number("9007199254740993"),
	})
	if err != nil {
		b.Fatal(err)
	}
	headers := http.Header{"Authorization": []string{"Bearer benchmark"}}
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	ids := &codexFingerprintIDs{InstallationID: "i", SessionID: "s", ThreadID: "t", TurnID: "u", WindowID: "w"}
	cfg := defaultPromptCacheConfig()
	b.Run("routing_only_no_token", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			document := newRequestDocument(body, sha256Bytes(body))
			intent := normalizeIntentDocument(RequestIntent{Endpoint: "/v1/chat/completions"}, document)
			if cache := buildExplicitPromptCacheRoutingContext(headers, intent, document, cfg); cache == nil {
				b.Fatal("missing routing context")
			}
		}
	})

	b.Run("shared_document", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			document := newRequestDocument(body, sha256Bytes(body))
			intent := normalizeIntentDocument(RequestIntent{Endpoint: "/v1/chat/completions"}, document)
			cache := buildPromptCacheRoutingContext(headers, intent, document, cfg)
			sanitizeReasoningContentDocument(document)
			if _, _, err := prepareUpstreamDocument(request, document, intent); err != nil {
				b.Fatal(err)
			}
			if err := finalizePromptCacheContext(cache, document); err != nil {
				b.Fatal(err)
			}
			if err := applyCodexFingerprintDocument(document, ids); err != nil {
				b.Fatal(err)
			}
			encoded, err := document.Bytes()
			if err != nil {
				b.Fatal(err)
			}
			completePromptCacheContext(cache, encoded)
		}
	})

	b.Run("reparse_helpers", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			prepared, _ := sanitizeReasoningContentBody(body)
			intent := normalizeIntent(RequestIntent{Endpoint: "/v1/chat/completions"}, prepared)
			prepared, intent, _, err = prepareUpstreamPayload(request, prepared, intent)
			if err != nil {
				b.Fatal(err)
			}
			cache, prepared := buildPromptCacheContext(headers, intent, prepared, cfg)
			prepared, err = applyCodexFingerprintBody(prepared, ids)
			if err != nil {
				b.Fatal(err)
			}
			completePromptCacheContext(cache, prepared)
		}
	})
}
