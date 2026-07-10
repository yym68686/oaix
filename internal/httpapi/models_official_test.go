package httpapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
	"github.com/yym68686/oaix/internal/tokens"
)

func TestModelsReturnsAndCachesOfficialCatalog(t *testing.T) {
	const officialBody = `{"models":[{"slug":"gpt-5.6-sol","supported_reasoning_levels":[{"effort":"max","description":"Maximum reasoning depth"}]}]}`
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if got := r.URL.Query().Get("client_version"); got != "0.144.0" {
			t.Errorf("client_version = %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer access-pro" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("ChatGPT-Account-Id"); got != "account-pro" {
			t.Errorf("ChatGPT-Account-Id = %q", got)
		}
		if got := r.Header.Get("Originator"); got != codexOriginator {
			t.Errorf("Originator = %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("ETag", `W/"official"`)
		_, _ = io.WriteString(w, officialBody)
	}))
	defer server.Close()

	ownerUserID := int64(42)
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{modelsCatalogToken(1, ownerUserID, "pro", "access-pro", "account-pro")})
	app.modelCatalog.upstreamURL = server.URL
	app.modelCatalog.waitTimeout = time.Second

	first := serveModelsForOwner(app, ownerUserID)
	if first.Code != http.StatusOK || first.Body.String() != officialBody {
		t.Fatalf("first response = %d %s", first.Code, first.Body.String())
	}
	if first.Header().Get("X-OAIX-Models-Source") != "official" ||
		first.Header().Get("X-OAIX-Models-Cache") != "miss" ||
		first.Header().Get("X-OAIX-Models-Plan") != "pro" ||
		first.Header().Get("ETag") != `W/"official"` {
		t.Fatalf("first headers = %#v", first.Header())
	}

	second := serveModelsForOwner(app, ownerUserID)
	if second.Header().Get("X-OAIX-Models-Cache") != "hit" {
		t.Fatalf("second cache header = %q", second.Header().Get("X-OAIX-Models-Cache"))
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("official requests = %d, want 1", got)
	}
}

func TestOfficialModelsRetriesAnotherAvailableAccount(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.Header.Get("Authorization") {
		case "Bearer access-bad":
			http.Error(w, "expired", http.StatusUnauthorized)
		case "Bearer access-good":
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.6-sol"}]}`)
		default:
			http.Error(w, "unexpected token", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ownerUserID := int64(43)
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{
		modelsCatalogToken(1, ownerUserID, "pro", "access-good", "account-good"),
		modelsCatalogToken(2, ownerUserID, "pro", "access-bad", "account-bad"),
	})
	app.modelCatalog.upstreamURL = server.URL
	app.modelCatalog.maxAttempts = 2

	entry, err := app.fetchOfficialModelsCatalog(context.Background(), ownerUserID, "0.144.0")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(entry.body), "gpt-5.6-sol") {
		t.Fatalf("body = %s", entry.body)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("official requests = %d, want 2", got)
	}
}

func TestModelsFallsBackWhileOfficialFetchContinues(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.6-sol"}]}`)
	}))
	defer server.Close()

	ownerUserID := int64(44)
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{modelsCatalogToken(1, ownerUserID, "plus", "access-plus", "account-plus")})
	app.modelCatalog.upstreamURL = server.URL
	app.modelCatalog.waitTimeout = 5 * time.Millisecond

	first := serveModelsForOwner(app, ownerUserID)
	if first.Header().Get("X-OAIX-Models-Source") != "static-fallback" {
		t.Fatalf("first source = %q", first.Header().Get("X-OAIX-Models-Source"))
	}

	time.Sleep(100 * time.Millisecond)
	second := serveModelsForOwner(app, ownerUserID)
	if second.Header().Get("X-OAIX-Models-Source") != "official" ||
		second.Header().Get("X-OAIX-Models-Cache") != "hit" {
		t.Fatalf("second headers = %#v", second.Header())
	}
}

func TestOfficialModelsCatalogIsIsolatedByOwner(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.Header.Get("Authorization") {
		case "Bearer access-pro":
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.6-sol","default_reasoning_level":"low"}]}`)
		case "Bearer access-k12":
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.6-sol","default_reasoning_level":"xhigh"}]}`)
		default:
			http.Error(w, "unexpected token", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	proOwnerID := int64(45)
	k12OwnerID := int64(46)
	source := &modelsCatalogTokenSource{tokens: []store.Token{
		modelsCatalogToken(1, proOwnerID, "pro", "access-pro", "account-pro"),
		modelsCatalogToken(2, k12OwnerID, "k12", "access-k12", "account-k12"),
	}}
	manager := tokens.NewManager(source, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Minute, time.Minute, 10)
	for _, ownerUserID := range []int64{proOwnerID, k12OwnerID} {
		if err := manager.RefreshOwner(context.Background(), ownerUserID); err != nil {
			t.Fatal(err)
		}
	}
	app := NewApp(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, manager, nil, nil)
	app.modelCatalog.upstreamURL = server.URL

	pro := serveModelsForOwner(app, proOwnerID)
	k12 := serveModelsForOwner(app, k12OwnerID)
	if !strings.Contains(pro.Body.String(), `"default_reasoning_level":"low"`) ||
		pro.Header().Get("X-OAIX-Models-Plan") != "pro" {
		t.Fatalf("pro response = %s, headers = %#v", pro.Body.String(), pro.Header())
	}
	if !strings.Contains(k12.Body.String(), `"default_reasoning_level":"xhigh"`) ||
		k12.Header().Get("X-OAIX-Models-Plan") != "k12" {
		t.Fatalf("k12 response = %s, headers = %#v", k12.Body.String(), k12.Header())
	}

	serveModelsForOwner(app, proOwnerID)
	serveModelsForOwner(app, k12OwnerID)
	if got := requests.Load(); got != 2 {
		t.Fatalf("official requests = %d, want one request per owner", got)
	}
}

func modelsCatalogTestApp(t *testing.T, ownerUserID int64, rows []store.Token) *App {
	t.Helper()
	source := &modelsCatalogTokenSource{tokens: rows}
	manager := tokens.NewManager(source, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Minute, time.Minute, 10)
	if err := manager.RefreshOwner(context.Background(), ownerUserID); err != nil {
		t.Fatal(err)
	}
	return NewApp(config.Config{}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil, manager, nil, nil)
}

func serveModelsForOwner(app *App, ownerUserID int64) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, "/v1/models?client_version=0.144.0", nil)
	request = withAuthContext(request, &AuthContext{UserID: &ownerUserID})
	response := httptest.NewRecorder()
	app.models(response, request)
	return response
}

func modelsCatalogToken(id, ownerUserID int64, plan, accessToken, accountID string) store.Token {
	return store.Token{
		ID:          id,
		OwnerUserID: ownerUserID,
		PlanType:    &plan,
		AccessToken: accessToken,
		AccountID:   &accountID,
		IsActive:    true,
	}
}

type modelsCatalogTokenSource struct {
	tokens []store.Token
}

func (s *modelsCatalogTokenSource) ListAvailableTokens(context.Context) ([]store.Token, error) {
	return append([]store.Token(nil), s.tokens...), nil
}

func (s *modelsCatalogTokenSource) ListAvailableTokensScoped(_ context.Context, scope store.ResourceScope) ([]store.Token, error) {
	items := make([]store.Token, 0, len(s.tokens))
	for _, token := range s.tokens {
		if scope.AllowAll || (scope.OwnerUserID != nil && token.OwnerUserID == *scope.OwnerUserID) {
			items = append(items, token)
		}
	}
	return items, nil
}

func (s *modelsCatalogTokenSource) TouchTokens(context.Context, []int64, time.Time) error {
	return nil
}

func (s *modelsCatalogTokenSource) MarkTokenSuccess(context.Context, int64) error {
	return nil
}

func (s *modelsCatalogTokenSource) MarkTokenError(context.Context, int64, string, bool, *time.Time) error {
	return nil
}
