package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

var errRequestBodyTooLarge = errors.New("request body too large")

func readProxyRequestBody(r *http.Request, maxBytes int64) ([]byte, int, string, error) {
	result, status, message, err := readProxyRequestBodyWithDigest(r, maxBytes)
	return result.Bytes, status, message, err
}

type proxyRequestBody struct {
	Bytes  []byte
	SHA256 string
}

func readProxyRequestBodyWithDigest(r *http.Request, maxBytes int64) (proxyRequestBody, int, string, error) {
	if r.Body == nil {
		return proxyRequestBody{SHA256: sha256Bytes(nil)}, 0, "", nil
	}
	defer func() { _ = r.Body.Close() }()

	encodings := contentEncodings(r.Header.Get("Content-Encoding"))
	if len(encodings) == 0 || isIdentityEncoding(encodings) {
		body, digest, err := readBodyWithLimitAndDigest(r.Body, maxBytes)
		if err != nil {
			status, message, readErr := requestBodyReadError(err, "failed to read request body")
			return proxyRequestBody{}, status, message, readErr
		}
		return proxyRequestBody{Bytes: body, SHA256: digest}, 0, "", nil
	}
	if len(encodings) != 1 || encodings[0] != "zstd" {
		return proxyRequestBody{}, http.StatusUnsupportedMediaType, fmt.Sprintf("unsupported content encoding: %s", strings.Join(encodings, ", ")), errors.New("unsupported content encoding")
	}

	compressed, err := readBodyWithLimit(r.Body, maxBytes)
	if err != nil {
		status, message, readErr := requestBodyReadError(err, "failed to read request body")
		return proxyRequestBody{}, status, message, readErr
	}
	decoder, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return proxyRequestBody{}, http.StatusBadRequest, "invalid zstd body", err
	}
	defer decoder.Close()

	body, digest, err := readBodyWithLimitAndDigest(decoder, maxBytes)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			return proxyRequestBody{}, http.StatusRequestEntityTooLarge, "request body too large", err
		}
		return proxyRequestBody{}, http.StatusBadRequest, "invalid zstd body", err
	}
	return proxyRequestBody{Bytes: body, SHA256: digest}, 0, "", nil
}

func readBodyWithLimitAndDigest(reader io.Reader, maxBytes int64) ([]byte, string, error) {
	hasher := sha256.New()
	body, err := readBodyWithLimit(io.TeeReader(reader, hasher), maxBytes)
	if err != nil {
		return nil, "", err
	}
	return body, hex.EncodeToString(hasher.Sum(nil)), nil
}

func requestBodyReadError(err error, fallback string) (int, string, error) {
	if errors.Is(err, errRequestBodyTooLarge) {
		return http.StatusRequestEntityTooLarge, "request body too large", err
	}
	return http.StatusBadRequest, fallback, err
}

func readBodyWithLimit(reader io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		return nil, errRequestBodyTooLarge
	}
	if maxBytes == 0 {
		return io.ReadAll(reader)
	}
	limit := maxBytes + 1
	if limit < 0 {
		limit = maxBytes
	}
	body, err := io.ReadAll(io.LimitReader(reader, limit))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBytes {
		return nil, errRequestBodyTooLarge
	}
	return body, nil
}

func contentEncodings(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	encodings := make([]string, 0, len(parts))
	for _, part := range parts {
		encoding := strings.ToLower(strings.TrimSpace(part))
		if encoding != "" {
			encodings = append(encodings, encoding)
		}
	}
	return encodings
}

func isIdentityEncoding(encodings []string) bool {
	for _, encoding := range encodings {
		if encoding != "identity" {
			return false
		}
	}
	return true
}
