package store

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"strings"
)

const apiKeyCipherVersion = "v1"

var ErrAPIKeyNotRecoverable = errors.New("api key plaintext is not recoverable")

type apiKeyCipher struct {
	aead cipher.AEAD
}

func newAPIKeyCipher(explicitSecret string, databaseURL string) *apiKeyCipher {
	seed := strings.TrimSpace(explicitSecret)
	if seed == "" {
		seed = strings.TrimSpace(databaseURL)
	}
	key := sha256.Sum256([]byte("oaix-api-key-encryption:v1:" + seed))
	block, err := aes.NewCipher(key[:])
	if err != nil {
		panic(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return &apiKeyCipher{aead: aead}
}

func (c *apiKeyCipher) Encrypt(plaintext string) (string, error) {
	if c == nil || c.aead == nil {
		return "", errors.New("api key encryption is unavailable")
	}
	plaintext = strings.TrimSpace(plaintext)
	if plaintext == "" {
		return "", errors.New("api key plaintext is empty")
	}
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}
	ciphertext := c.aead.Seal(nil, nonce, []byte(plaintext), []byte(apiKeyCipherVersion))
	return strings.Join([]string{
		apiKeyCipherVersion,
		base64.RawURLEncoding.EncodeToString(nonce),
		base64.RawURLEncoding.EncodeToString(ciphertext),
	}, ":"), nil
}

func (c *apiKeyCipher) Decrypt(encoded string) (string, error) {
	if strings.TrimSpace(encoded) == "" {
		return "", ErrAPIKeyNotRecoverable
	}
	if c == nil || c.aead == nil {
		return "", errors.New("api key decryption is unavailable")
	}
	parts := strings.Split(encoded, ":")
	if len(parts) != 3 || parts[0] != apiKeyCipherVersion {
		return "", ErrAPIKeyNotRecoverable
	}
	nonce, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(nonce) != c.aead.NonceSize() {
		return "", fmt.Errorf("decode api key nonce: %w", ErrAPIKeyNotRecoverable)
	}
	ciphertext, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("decode api key ciphertext: %w", ErrAPIKeyNotRecoverable)
	}
	plaintext, err := c.aead.Open(nil, nonce, ciphertext, []byte(apiKeyCipherVersion))
	if err != nil {
		return "", fmt.Errorf("decrypt api key: %w", ErrAPIKeyNotRecoverable)
	}
	if len(plaintext) == 0 {
		return "", ErrAPIKeyNotRecoverable
	}
	return string(plaintext), nil
}
