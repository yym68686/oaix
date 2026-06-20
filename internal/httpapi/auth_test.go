package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yym68686/oaix/internal/config"
	"github.com/yym68686/oaix/internal/store"
)

func TestAuthContextResourceScope(t *testing.T) {
	userID := int64(11)
	admin := &AuthContext{IsAdmin: true}
	if scope := admin.resourceScope(); !scope.AllowAll {
		t.Fatalf("admin scope = %#v, want all", scope)
	}
	user := &AuthContext{UserID: &userID}
	if scope := user.resourceScope(); scope.OwnerUserID == nil || *scope.OwnerUserID != userID {
		t.Fatalf("user scope = %#v, want owner %d", scope, userID)
	}
	actAsID := int64(12)
	service := &AuthContext{IsService: true, IsAdmin: true, ActAsUserID: &actAsID}
	if scope := service.resourceScope(); scope.OwnerUserID == nil || *scope.OwnerUserID != actAsID {
		t.Fatalf("service act-as scope = %#v, want owner %d", scope, actAsID)
	}
}

func TestUserScopeAllowsServiceActAsOwner(t *testing.T) {
	actAsID := int64(42)
	rec := httptest.NewRecorder()
	scope, ok := userScope(rec, &AuthContext{IsService: true, IsAdmin: true, ActAsUserID: &actAsID})
	if !ok {
		t.Fatalf("userScope rejected service act-as: status=%d body=%s", rec.Code, rec.Body.String())
	}
	if scope.OwnerUserID == nil || *scope.OwnerUserID != actAsID {
		t.Fatalf("scope = %#v, want owner %d", scope, actAsID)
	}
}

func TestUserScopeRejectsServiceWithoutActAs(t *testing.T) {
	rec := httptest.NewRecorder()
	if _, ok := userScope(rec, &AuthContext{IsService: true, IsAdmin: true}); ok {
		t.Fatal("userScope accepted service without act-as")
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestRequireAuthServiceKeyHasAdminContext(t *testing.T) {
	app := NewApp(config.Config{Auth: config.AuthConfig{ServiceAPIKeys: []string{"service-key"}}}, nil, nil, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, "/admin/runtime", nil)
	req.Header.Set("Authorization", "Bearer service-key")
	rec := httptest.NewRecorder()

	app.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		auth := authFromContext(r.Context())
		if auth == nil || !auth.IsService || !auth.IsAdmin || auth.Role != "service" {
			t.Fatalf("auth context = %#v", auth)
		}
		w.WriteHeader(http.StatusNoContent)
	})(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, body=%s", rec.Code, rec.Body.String())
	}
}

func TestAuthCanAccessOwner(t *testing.T) {
	userID := int64(3)
	otherID := int64(4)
	if !authCanAccessOwner(&AuthContext{UserID: &userID}, userID) {
		t.Fatal("user should access own owner scope")
	}
	if authCanAccessOwner(&AuthContext{UserID: &userID}, otherID) {
		t.Fatal("user should not access another owner scope")
	}
	if !authCanAccessOwner(&AuthContext{IsAdmin: true}, otherID) {
		t.Fatal("admin should access owner scope")
	}
	if !blockedStatus(&AuthContext{User: &store.PlatformUser{Status: "disabled"}}) {
		t.Fatal("disabled user should be blocked")
	}
}
