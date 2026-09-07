package store

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
)

func TestAPIKeyCipherRoundTripAndTamperDetection(t *testing.T) {
	cipher := newAPIKeyCipher("unit-test-secret", "")
	plaintext := "oaix_user_0123456789abcdef"
	encoded, err := cipher.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("Encrypt returned error: %v", err)
	}
	if encoded == plaintext || strings.Contains(encoded, plaintext) {
		t.Fatalf("ciphertext exposed plaintext: %q", encoded)
	}
	decoded, err := cipher.Decrypt(encoded)
	if err != nil {
		t.Fatalf("Decrypt returned error: %v", err)
	}
	if decoded != plaintext {
		t.Fatalf("Decrypt = %q, want %q", decoded, plaintext)
	}

	parts := strings.Split(encoded, ":")
	ciphertext, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	ciphertext[0] ^= 1
	parts[2] = base64.RawURLEncoding.EncodeToString(ciphertext)
	tampered := strings.Join(parts, ":")
	if _, err := cipher.Decrypt(tampered); !errors.Is(err, ErrAPIKeyNotRecoverable) {
		t.Fatalf("tampered ciphertext error = %v, want ErrAPIKeyNotRecoverable", err)
	}
}

func TestAPIKeyCipherRejectsMissingLegacyCiphertext(t *testing.T) {
	cipher := newAPIKeyCipher("unit-test-secret", "")
	if _, err := cipher.Decrypt(""); !errors.Is(err, ErrAPIKeyNotRecoverable) {
		t.Fatalf("Decrypt empty error = %v, want ErrAPIKeyNotRecoverable", err)
	}
}
