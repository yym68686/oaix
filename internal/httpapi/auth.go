package httpapi

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/store"
)

type authContextKey struct{}

type AuthContext struct {
	PrincipalType string              `json:"principal_type"`
	UserID        *int64              `json:"user_id,omitempty"`
	APIKeyID      *int64              `json:"api_key_id,omitempty"`
	Role          string              `json:"role"`
	Scopes        []string            `json:"scopes,omitempty"`
	ActAsUserID   *int64              `json:"act_as_user_id,omitempty"`
	IsService     bool                `json:"is_service"`
	IsAdmin       bool                `json:"is_admin"`
	ReadOnly      bool                `json:"read_only"`
	User          *store.PlatformUser `json:"user,omitempty"`
}

func authFromContext(ctx context.Context) *AuthContext {
	auth, _ := ctx.Value(authContextKey{}).(*AuthContext)
	return auth
}

func withAuthContext(r *http.Request, auth *AuthContext) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), authContextKey{}, auth))
}

func (auth *AuthContext) resourceScope() store.ResourceScope {
	if auth == nil {
		return store.ResourceScope{}
	}
	if auth.IsService || auth.IsAdmin {
		if auth.ActAsUserID != nil {
			return store.OwnerResources(*auth.ActAsUserID)
		}
		return store.AllResources()
	}
	if auth.UserID != nil {
		return store.OwnerResources(*auth.UserID)
	}
	return store.ResourceScope{}
}

func (auth *AuthContext) proxyOwner(ctx context.Context, st *store.Store) (int64, error) {
	if auth == nil {
		return 0, errors.New("missing auth context")
	}
	if auth.ActAsUserID != nil && *auth.ActAsUserID > 0 {
		return *auth.ActAsUserID, nil
	}
	if auth.UserID != nil && *auth.UserID > 0 && !auth.IsAdmin && !auth.IsService {
		return *auth.UserID, nil
	}
	if auth.IsService {
		return st.BootstrapUserID(ctx)
	}
	return 0, errors.New("admin proxy requests require X-OAIX-Act-As-User")
}

func (a *App) requireAuth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(a.authKeys) == 0 && a.store == nil {
			next(w, r)
			return
		}
		auth, ok := a.authenticateRequest(r)
		if !ok {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Invalid or missing API key"})
			return
		}
		if blockedStatus(auth) {
			writeJSON(w, http.StatusForbidden, map[string]any{"detail": "User is not active"})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/admin/") && !auth.IsService && !auth.IsAdmin {
			writeJSON(w, http.StatusForbidden, map[string]any{"detail": "Admin permission required"})
			return
		}
		if auth.ReadOnly && isMutatingMethod(r.Method) {
			writeJSON(w, http.StatusForbidden, map[string]any{"detail": "Readonly API key cannot mutate resources"})
			return
		}
		if err := a.applyActAsUser(r.Context(), r, auth); err != nil {
			writeJSON(w, http.StatusForbidden, map[string]any{"detail": err.Error()})
			return
		}
		next(w, withAuthContext(r, auth))
	}
}

func (a *App) authenticateRequest(r *http.Request) (*AuthContext, bool) {
	provided := bearerToken(r)
	if provided == "" {
		provided = strings.TrimSpace(r.Header.Get("X-API-Key"))
	}
	if provided == "" {
		return nil, false
	}
	for _, key := range a.authKeys {
		if subtle.ConstantTimeCompare([]byte(provided), []byte(key)) == 1 {
			return &AuthContext{
				PrincipalType: "service",
				Role:          "service",
				IsService:     true,
				IsAdmin:       true,
			}, true
		}
	}
	if a.store == nil {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(r.Context(), 900*time.Millisecond)
	defer cancel()
	if item, ok, err := a.store.ValidateAPIKey(ctx, provided); err == nil && ok {
		userID := item.User.ID
		apiKeyID := item.ID
		role := firstNonEmpty(item.Role, item.User.Role, "user")
		auth := &AuthContext{
			PrincipalType: "user",
			UserID:        &userID,
			APIKeyID:      &apiKeyID,
			Role:          role,
			Scopes:        item.Scopes,
			IsService:     item.Kind == "service" || role == "service",
			IsAdmin:       role == "admin" || role == "readonly_admin" || role == "service",
			ReadOnly:      role == "readonly_admin",
			User:          &item.User,
		}
		if auth.IsService {
			auth.PrincipalType = "service"
		}
		a.touchAPIKeyAsync(apiKeyID)
		return auth, true
	}
	if item, ok, err := a.store.ValidateAdminAPIKey(ctx, provided); err == nil && ok {
		role := firstNonEmpty(item.Role, "admin")
		return &AuthContext{
			PrincipalType: "user",
			Role:          role,
			IsAdmin:       role == "admin" || role == "readonly_admin" || role == "service",
			IsService:     role == "service",
			ReadOnly:      role == "readonly_admin",
		}, true
	}
	return nil, false
}

func (a *App) touchAPIKeyAsync(id int64) {
	if a == nil || a.store == nil || id <= 0 {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
		defer cancel()
		_ = a.store.TouchAPIKey(ctx, id)
	}()
}

func (a *App) applyActAsUser(ctx context.Context, r *http.Request, auth *AuthContext) error {
	value := strings.TrimSpace(r.Header.Get("X-OAIX-Act-As-User"))
	if value == "" {
		return nil
	}
	if auth == nil || (!auth.IsService && !auth.IsAdmin) {
		return errors.New("act-as requires admin or service")
	}
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return errors.New("invalid X-OAIX-Act-As-User")
	}
	if a.store != nil {
		user, err := a.store.GetPlatformUser(ctx, id)
		if err != nil {
			return errors.New("act-as user not found")
		}
		if strings.ToLower(strings.TrimSpace(user.Status)) != "active" {
			return errors.New("act-as user is not active")
		}
	}
	auth.ActAsUserID = &id
	return nil
}

func blockedStatus(auth *AuthContext) bool {
	if auth == nil || auth.User == nil {
		return false
	}
	switch strings.ToLower(strings.TrimSpace(auth.User.Status)) {
	case "", "active":
		return false
	default:
		return true
	}
}

func isMutatingMethod(method string) bool {
	switch strings.ToUpper(method) {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func authCanAccessOwner(auth *AuthContext, ownerUserID int64) bool {
	if ownerUserID <= 0 {
		return auth != nil && (auth.IsAdmin || auth.IsService)
	}
	if auth == nil {
		return false
	}
	if auth.IsAdmin || auth.IsService {
		return true
	}
	return auth.UserID != nil && *auth.UserID == ownerUserID
}
