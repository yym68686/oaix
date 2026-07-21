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

func TestOfficialModelsAgentIdentityUsesAssertionAndWorkspaceHeaders(t *testing.T) {
	credentials := probeAgentIdentityCredentials(t, "task-models")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := decodeProbeAgentAssertionTask(t, r.Header.Get("Authorization")); got != credentials.TaskID {
			t.Errorf("assertion task = %q", got)
		}
		if got := r.Header.Get("ChatGPT-Account-ID"); got != credentials.AccountID {
			t.Errorf("ChatGPT-Account-ID = %q", got)
		}
		if got := r.Header.Get("X-OpenAI-FedRAMP"); got != "true" {
			t.Errorf("X-OpenAI-FedRAMP = %q", got)
		}
		if got := r.Header.Get("Version"); got != "0.144.0" {
			t.Errorf("Version = %q", got)
		}
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.6-sol"}]}`)
	}))
	defer server.Close()

	ownerUserID := int64(421)
	plan := "k12"
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{{
		ID:            91,
		OwnerUserID:   ownerUserID,
		RefreshToken:  credentials.IdentityToken(),
		AgentIdentity: &credentials,
		PlanType:      &plan,
		IsActive:      true,
	}})
	app.probeIdentityStore = &fakeAgentIdentityProbeStore{credentials: credentials}
	app.modelCatalog.upstreamURL = server.URL

	response := serveModelsForOwner(app, ownerUserID)
	if response.Code != http.StatusOK {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
}

func TestOfficialModelsAgentIdentityRecoversInvalidTaskExactlyOnce(t *testing.T) {
	credentials := probeAgentIdentityCredentials(t, "task-models-old")
	credentialStore := &fakeAgentIdentityProbeStore{credentials: credentials}
	var registrationCalls atomic.Int32
	registration := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		registrationCalls.Add(1)
		_, _ = io.WriteString(w, `{"task_id":"task-models-new"}`)
	}))
	defer registration.Close()

	var upstreamCalls atomic.Int32
	var tasks []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls.Add(1)
		tasks = append(tasks, decodeProbeAgentAssertionTask(t, r.Header.Get("Authorization")))
		if upstreamCalls.Load() == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"code":"invalid_task_id"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.6-sol"}]}`)
	}))
	defer server.Close()

	ownerUserID := int64(422)
	plan := "k12"
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{{
		ID:            92,
		OwnerUserID:   ownerUserID,
		RefreshToken:  credentials.IdentityToken(),
		AgentIdentity: &credentials,
		PlanType:      &plan,
		IsActive:      true,
	}})
	app.cfg.Upstream.AgentIdentityAuthAPIURL = registration.URL
	app.probeIdentityStore = credentialStore
	app.modelCatalog.upstreamURL = server.URL

	response := serveModelsForOwner(app, ownerUserID)
	if response.Code != http.StatusOK {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
	if upstreamCalls.Load() != 2 || registrationCalls.Load() != 1 || credentialStore.updateCalls != 1 {
		t.Fatalf("calls upstream=%d registration=%d updates=%d", upstreamCalls.Load(), registrationCalls.Load(), credentialStore.updateCalls)
	}
	if len(tasks) != 2 || tasks[0] != "task-models-old" || tasks[1] != "task-models-new" {
		t.Fatalf("assertion tasks = %v", tasks)
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

	entry, err := app.fetchOfficialModelsPlanCatalog(context.Background(), ownerUserID, "pro", "0.144.0")
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

func TestModelsReturnsRetryableErrorWhileOfficialFetchContinues(t *testing.T) {
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
	if first.Code != http.StatusServiceUnavailable || first.Header().Get("X-OAIX-Models-Source") != "official-unavailable" {
		t.Fatalf("first response = %d %s headers=%#v", first.Code, first.Body.String(), first.Header())
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

func TestOfficialModelsCatalogUsesFastPlanAcrossMixedPool(t *testing.T) {
	const fastBody = `{"models":[{"slug":"gpt-5.5","additional_speed_tiers":["fast"],"service_tiers":[{"id":"priority","name":"Fast","description":"1.5x speed"}]}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Authorization") {
		case "Bearer access-pro":
			_, _ = io.WriteString(w, fastBody)
		case "Bearer access-free", "Bearer access-k12":
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.5","additional_speed_tiers":[],"service_tiers":[]}]}`)
		default:
			http.Error(w, "unexpected token", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ownerUserID := int64(47)
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{
		modelsCatalogToken(1, ownerUserID, "free", "access-free", "account-free"),
		modelsCatalogToken(2, ownerUserID, "k12", "access-k12", "account-k12"),
		modelsCatalogToken(3, ownerUserID, "pro", "access-pro", "account-pro"),
	})
	app.modelCatalog.upstreamURL = server.URL

	response := serveModelsForOwner(app, ownerUserID)
	if response.Code != http.StatusOK || response.Body.String() != fastBody {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
	if response.Header().Get("X-OAIX-Models-Plan") != "pro" {
		t.Fatalf("selected plan = %q", response.Header().Get("X-OAIX-Models-Plan"))
	}
	if response.Header().Get("X-OAIX-Models-Plans") != "free,k12,pro" {
		t.Fatalf("scanned plans = %q", response.Header().Get("X-OAIX-Models-Plans"))
	}

	second := serveModelsForOwner(app, ownerUserID)
	if second.Body.String() != fastBody || second.Header().Get("X-OAIX-Models-Cache") != "hit" {
		t.Fatalf("cached response = %d %s headers=%#v", second.Code, second.Body.String(), second.Header())
	}
}

func TestFastRequestCapabilityProbeWorksBeforeModelsRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Authorization") {
		case "Bearer access-pro":
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.5","service_tiers":[{"id":"priority","name":"Fast"}]}]}`)
		case "Bearer access-k12":
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.5","service_tiers":[]}]}`)
		default:
			http.Error(w, "unexpected token", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ownerUserID := int64(471)
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{
		modelsCatalogToken(1, ownerUserID, "k12", "access-k12", "account-k12"),
		modelsCatalogToken(2, ownerUserID, "pro", "access-pro", "account-pro"),
	})
	app.modelCatalog.upstreamURL = server.URL

	eligible, err := app.tokens.FastEligibleTokenIDs(
		context.Background(),
		tokens.Intent{OwnerUserID: ownerUserID},
		"gpt-5.5",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := eligible[2]; !ok || len(eligible) != 1 {
		t.Fatalf("eligible tokens = %#v, want only Pro token 2", eligible)
	}
}

func TestOfficialModelsCatalogHidesFastOnlyWhenEveryPlanLacksIt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.5","additional_speed_tiers":[],"service_tiers":[]}]}`)
	}))
	defer server.Close()

	ownerUserID := int64(48)
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{
		modelsCatalogToken(1, ownerUserID, "free", "access-free", "account-free"),
		modelsCatalogToken(2, ownerUserID, "k12", "access-k12", "account-k12"),
	})
	app.modelCatalog.upstreamURL = server.URL

	response := serveModelsForOwner(app, ownerUserID)
	if response.Code != http.StatusOK || response.Header().Get("X-OAIX-Models-Source") != "official" {
		t.Fatalf("response = %d %s headers=%#v", response.Code, response.Body.String(), response.Header())
	}
	if strings.Contains(response.Body.String(), `"id":"priority"`) || strings.Contains(response.Body.String(), `"fast"`) {
		t.Fatalf("all-no-Fast response advertised Fast: %s", response.Body.String())
	}
	if response.Header().Get("X-OAIX-Models-Plans") != "free,k12" {
		t.Fatalf("scanned plans = %q", response.Header().Get("X-OAIX-Models-Plans"))
	}
}

func TestOfficialModelsCatalogChoosesMostCompleteFastPlanDeterministically(t *testing.T) {
	const proBody = `{"models":[{"slug":"gpt-5.5","service_tiers":[{"id":"priority","name":"Fast"}]}]}`
	const teamBody = `{"models":[{"slug":"gpt-5.4","service_tiers":[{"id":"priority","name":"Fast"}]},{"slug":"gpt-5.5","service_tiers":[{"id":"priority","name":"Fast"}]}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Authorization") {
		case "Bearer access-pro":
			_, _ = io.WriteString(w, proBody)
		case "Bearer access-team":
			time.Sleep(20 * time.Millisecond)
			_, _ = io.WriteString(w, teamBody)
		default:
			http.Error(w, "unexpected token", http.StatusBadRequest)
		}
	}))
	defer server.Close()

	ownerUserID := int64(481)
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{
		modelsCatalogToken(1, ownerUserID, "pro", "access-pro", "account-pro"),
		modelsCatalogToken(2, ownerUserID, "team", "access-team", "account-team"),
	})
	app.modelCatalog.upstreamURL = server.URL

	response := serveModelsForOwner(app, ownerUserID)
	if response.Code != http.StatusOK || response.Body.String() != teamBody {
		t.Fatalf("response = %d %s", response.Code, response.Body.String())
	}
	if plan := response.Header().Get("X-OAIX-Models-Plan"); plan != "team" {
		t.Fatalf("selected plan = %q, want team", plan)
	}
}

func TestOfficialModelsCatalogDoesNotTreatIncompleteScanAsNoFast(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer access-k12" {
			http.Error(w, "temporary failure", http.StatusBadGateway)
			return
		}
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5.5","additional_speed_tiers":[],"service_tiers":[]}]}`)
	}))
	defer server.Close()

	ownerUserID := int64(49)
	app := modelsCatalogTestApp(t, ownerUserID, []store.Token{
		modelsCatalogToken(1, ownerUserID, "free", "access-free", "account-free"),
		modelsCatalogToken(2, ownerUserID, "k12", "access-k12", "account-k12"),
	})
	app.modelCatalog.upstreamURL = server.URL

	response := serveModelsForOwner(app, ownerUserID)
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("X-OAIX-Models-Source") != "official-unavailable" {
		t.Fatalf("response = %d %s headers=%#v", response.Code, response.Body.String(), response.Header())
	}
	if strings.Contains(response.Body.String(), `"models"`) {
		t.Fatalf("incomplete scan returned a misleading model catalog: %s", response.Body.String())
	}
}

func TestOfficialFastModelsPrefersServiceTiersOverLegacyField(t *testing.T) {
	models, count, err := officialFastModels([]byte(`{"models":[
		{"slug":"gpt-5.4","additional_speed_tiers":["fast"],"service_tiers":[]},
		{"slug":"gpt-5.5","additional_speed_tiers":["fast"]},
		{"slug":"gpt-5.6-sol","service_tiers":[{"id":"priority","name":"Fast"}]}
	]}`))
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 || !equalStrings(models, []string{"gpt-5.5", "gpt-5.6-sol"}) {
		t.Fatalf("models=%#v count=%d", models, count)
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
