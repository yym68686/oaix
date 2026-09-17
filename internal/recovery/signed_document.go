package recovery

import (
	"encoding/json"
	"errors"
	"strings"
)

const SignedDefaultURL = "https://zzledu.kdns.fr"
const MaxSignedDocumentBytes = 20 << 20
const DocumentIDField = "recovery_document_id"

type SignedAccount struct {
	Name        string `json:"name"`
	Platform    string `json:"platform"`
	Type        string `json:"type"`
	Credentials struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		IDToken      string `json:"id_token"`
		AccountID    string `json:"chatgpt_account_id"`
		Email        string `json:"email"`
	} `json:"credentials"`
}

type SignedDocument struct {
	Accounts []SignedAccount `json:"accounts"`
	Manifest struct {
		Issuer    string            `json:"issuer"`
		Signature string            `json:"signature"`
		Records   []json.RawMessage `json:"records"`
	} `json:"x_revive_manifest"`
}

// This checks format/identity only. The provider verifies the signature before
// issuing a task token; an unsigned or locally fabricated file cannot recover.
func ParseSignedDocument(raw []byte) (SignedDocument, error) {
	var doc SignedDocument
	if len(raw) == 0 || len(raw) > MaxSignedDocumentBytes {
		return doc, errors.New("signed recovery document size invalid")
	}
	if json.Unmarshal(raw, &doc) != nil {
		return doc, errors.New("invalid signed recovery JSON")
	}
	if doc.Manifest.Issuer != "signed-recovery" || doc.Manifest.Signature == "" || len(doc.Accounts) == 0 || len(doc.Accounts) > 1000 || len(doc.Manifest.Records) != len(doc.Accounts) {
		return doc, errors.New("signed recovery manifest missing or invalid")
	}
	seen := map[string]bool{}
	for _, a := range doc.Accounts {
		email, account, err := a.Identity()
		if err != nil {
			return doc, err
		}
		key := strings.ToLower(email) + "\x00" + account
		if seen[key] {
			return doc, errors.New("duplicate signed recovery identity")
		}
		seen[key] = true
	}
	return doc, nil
}

func (a SignedAccount) Identity() (string, string, error) {
	email := strings.TrimSpace(a.Credentials.Email)
	account := strings.TrimSpace(a.Credentials.AccountID)
	claims, err := TokenIdentity(a.Credentials.AccessToken)
	if err == nil {
		if email == "" {
			email = claims.Email
		}
		if account == "" {
			account = claims.AccountID
		}
		if (claims.Email != "" && !strings.EqualFold(claims.Email, email)) || (claims.AccountID != "" && claims.AccountID != account) {
			return "", "", errors.New("signed recovery account identity mismatch")
		}
	}
	if email == "" || account == "" || !strings.Contains(email, "@") {
		return "", "", errors.New("signed recovery account identity missing")
	}
	return email, account, nil
}

func (d SignedDocument) Contains(email, account string) bool {
	for _, a := range d.Accounts {
		e, id, err := a.Identity()
		if err == nil && strings.EqualFold(e, email) && id == account {
			return true
		}
	}
	return false
}

func (d SignedDocument) Credential(email, account string) (Result, error) {
	for _, a := range d.Accounts {
		e, id, err := a.Identity()
		if err != nil || !strings.EqualFold(e, email) || id != account {
			continue
		}
		result, err := TokenIdentity(a.Credentials.AccessToken)
		if err != nil {
			return Result{}, err
		}
		if a.Credentials.RefreshToken == "" {
			return Result{}, &APIError{Code: "missing_credentials"}
		}
		result.AccessToken = a.Credentials.AccessToken
		result.RefreshToken = a.Credentials.RefreshToken
		result.IDToken = a.Credentials.IDToken
		if err := Validate(result, email, account); err != nil {
			return Result{}, err
		}
		return result, nil
	}
	return Result{}, &APIError{Code: "target_not_recovered"}
}
