package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/recovery"
)

func (a *App) recoveryImportOwner(r *http.Request) (int64, error) {
	auth := authFromContext(r.Context())
	if auth != nil {
		if auth.ActAsUserID != nil {
			return *auth.ActAsUserID, nil
		}
		if strings.HasPrefix(r.URL.Path, "/api/") && !strings.HasPrefix(r.URL.Path, "/api/admin/") && auth.UserID != nil && !auth.IsService {
			return *auth.UserID, nil
		}
	}
	if a.store == nil {
		return 0, errors.New("recovery document store unavailable")
	}
	return a.store.BootstrapUserID(r.Context())
}

func signedImportBytes(raw []byte) []byte {
	var envelope map[string]json.RawMessage
	if json.Unmarshal(raw, &envelope) != nil {
		return nil
	}
	if _, ok := envelope["x_revive_manifest"]; ok {
		return raw
	}
	if text, ok := envelope["text"]; ok {
		var value string
		if json.Unmarshal(text, &value) == nil {
			return signedImportBytes([]byte(value))
		}
	}
	for _, key := range []string{"data", "tokens"} {
		if nested, ok := envelope[key]; ok {
			if data := signedImportBytes(nested); data != nil {
				return data
			}
		}
	}
	return nil
}

func (a *App) preserveRecoveryImport(r *http.Request, raw []byte, payloads []map[string]any) error {
	raw = signedImportBytes(raw)
	if raw == nil {
		return nil
	}
	owner, err := a.recoveryImportOwner(r)
	if err != nil {
		return err
	}
	return a.preserveRecoveryImportForOwner(r.Context(), owner, raw, payloads)
}
func (a *App) preserveRecoveryImportForOwner(parent context.Context, owner int64, raw []byte, payloads []map[string]any) error {
	doc, err := recovery.ParseSignedDocument(raw)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	id, err := a.store.SaveRecoveryDocument(ctx, owner, raw)
	if err != nil {
		return err
	}
	// Resolve each account once. Large signed bundles must not repeatedly
	// decode every JWT for every import item; shared RTs are not unique identity.
	byIdentity := make(map[string]int, len(doc.Accounts))
	byRefresh := make(map[string]int, len(doc.Accounts))
	for i, account := range doc.Accounts {
		email, workspace, _ := account.Identity()
		byIdentity[strings.ToLower(email)+"\x00"+workspace] = i
		if rt := account.Credentials.RefreshToken; rt != "" {
			if _, exists := byRefresh[rt]; exists {
				byRefresh[rt] = -1
			} else {
				byRefresh[rt] = i
			}
		}
	}
	for _, p := range payloads {
		email := stringFromImportPayload(p, "email")
		workspace := stringFromImportPayload(p, "account_id", "chatgpt_account_id")
		index, matched := byIdentity[strings.ToLower(email)+"\x00"+workspace]
		if !matched {
			claims, err := recovery.TokenIdentity(stringFromImportPayload(p, "access_token"))
			if err == nil {
				index, matched = byIdentity[strings.ToLower(claims.Email)+"\x00"+claims.AccountID]
			}
		}
		if !matched {
			index, matched = byRefresh[stringFromImportPayload(p, "refresh_token")]
			matched = matched && index >= 0
		}
		if !matched {
			return errors.New("import identity does not match signed recovery document")
		}
		p[recovery.DocumentIDField] = id
		p["name"] = doc.Accounts[index].Name
	}

	return nil
}

// Existing accounts can attach a source file without re-importing or rotating
// credentials. Access is scoped exactly like the native token detail endpoint.
func (a *App) attachRecoveryDocument(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("token_id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, 400, errors.New("invalid token id"))
		return
	}
	auth := authFromContext(r.Context())
	if auth == nil {
		writeError(w, 403, errors.New("authentication required"))
		return
	}
	token, err := a.store.GetTokenScoped(r.Context(), auth.resourceScope(), id)
	if err != nil {
		writeError(w, 404, errors.New("token not found"))
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, recovery.MaxSignedDocumentBytes))
	if err != nil {
		writeError(w, 400, errors.New("signed file too large"))
		return
	}
	doc, err := recovery.ParseSignedDocument(raw)
	if err != nil {
		writeError(w, 400, err)
		return
	}
	if !doc.Contains(stringPtr(token.Email), stringPtr(token.AccountID)) {
		writeError(w, 400, errors.New("signed file does not contain this account"))
		return
	}
	docID, err := a.store.SaveRecoveryDocument(r.Context(), token.OwnerUserID, raw)
	if err == nil {
		err = a.store.BindRecoveryDocument(r.Context(), token.OwnerUserID, token.ID, docID)
	}
	if err != nil {
		writeError(w, 503, errors.New("could not save signed recovery file"))
		return
	}
	_ = a.store.WriteAuditLog(r.Context(), "recovery_document_attached", auth.Role, "token", strconv.FormatInt(id, 10), map[string]any{"document_id": docID})
	writeJSON(w, 200, map[string]any{"id": id, "recovery_document_id": docID, "saved": true})
}
