package httpapi

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/proxy"
)

func TestHandlerRegistersRoutes(t *testing.T) {
	app := NewApp(config.Config{Auth: config.AuthConfig{ServiceAPIKeys: []string{"service-key"}}}, nil, nil, nil, nil, nil)
	_ = app.Handler()
}

func TestNewAppSharesAgentIdentityTaskCoordinatorWithProxy(t *testing.T) {
	pipeline := proxy.New(config.Config{}, nil, nil, nil, nil, nil, nil)
	app := NewApp(config.Config{}, nil, nil, nil, nil, pipeline)
	if app.agentIdentityTasks == nil || app.agentIdentityTasks != pipeline.AgentIdentityTaskCoordinator() {
		t.Fatal("app and proxy do not share the agent identity task coordinator")
	}
}

func TestAlphaSearchRouteRequiresAuthentication(t *testing.T) {
	app := NewApp(config.Config{Auth: config.AuthConfig{ServiceAPIKeys: []string{"service-key"}}}, nil, nil, nil, nil, nil)
	request := httptest.NewRequest(http.MethodPost, "/v1/alpha/search", strings.NewReader(`{"id":"session-1","model":"gpt-5.4"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	app.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d body=%s", response.Code, response.Body.String())
	}
}

func TestIndexInjectsVersionedAssets(t *testing.T) {
	webDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(webDir, "src"), 0o755); err != nil {
		t.Fatal(err)
	}
	index := `<!doctype html>
<link rel="stylesheet" href="/assets/styles.css">
<div title="资源版本 __OAIX_WEB_VERSION_HASH__">前端版本 __OAIX_WEB_VERSION_TIME__</div>
<script type="module" src="/assets/src/main.js"></script>`
	if err := os.WriteFile(filepath.Join(webDir, "index.html"), []byte(index), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "styles.css"), []byte("body{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "src", "main.js"), []byte("import './api.js';"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(webDir, "src", "api.js"), []byte("export {};"), 0o644); err != nil {
		t.Fatal(err)
	}

	app := &App{webDir: webDir}
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	app.index(response, request)
	body := response.Body.String()

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if !strings.Contains(body, `/assets/styles.css?v=`) {
		t.Fatalf("stylesheet was not versioned: %s", body)
	}
	if !strings.Contains(body, `/assets/src/main.js?v=`) {
		t.Fatalf("module script was not versioned: %s", body)
	}
	if strings.Contains(body, "__OAIX_WEB_VERSION_") {
		t.Fatalf("version placeholders were not replaced: %s", body)
	}
}

func TestAssetsUseRevalidationCachePolicy(t *testing.T) {
	webDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(webDir, "styles.css"), []byte("body{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	app := &App{webDir: webDir}
	request := httptest.NewRequest(http.MethodGet, "/assets/styles.css", nil)
	response := httptest.NewRecorder()
	app.assets().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d", response.Code)
	}
	if got := response.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control = %q", got)
	}
}
