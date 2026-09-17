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
	for _, p := range payloads {
		matched := false
		for _, account := range doc.Accounts {
			email, workspace, _ := account.Identity()
			if rt := stringFromImportPayload(p, "refresh_token"); rt != "" && rt == account.Credentials.RefreshToken {
				p[recovery.DocumentIDField] = id
				p["name"] = account.Name
				matched = true
				break
			}
			pemail := stringFromImportPayload(p, "email")
			pid := stringFromImportPayload(p, "account_id", "chatgpt_account_id")
			if strings.EqualFold(email, pemail) && workspace == pid {
				p[recovery.DocumentIDField] = id
				p["name"] = account.Name
				matched = true
				break
			}
		}
		if !matched {
			return errors.New("import identity does not match signed recovery document")
		}
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
