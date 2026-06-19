package proxy

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/klauspost/compress/zstd"
)

var errRequestBodyTooLarge = errors.New("request body too large")

func readProxyRequestBody(r *http.Request, maxBytes int64) ([]byte, int, string, error) {
	if r.Body == nil {
		return nil, 0, "", nil
	}
	defer func() { _ = r.Body.Close() }()

	encodings := contentEncodings(r.Header.Get("Content-Encoding"))
	if len(encodings) == 0 || isIdentityEncoding(encodings) {
		body, err := readBodyWithLimit(r.Body, maxBytes)
		if err != nil {
			status, message, readErr := requestBodyReadError(err, "failed to read request body")
			return nil, status, message, readErr
		}
		return body, 0, "", nil
	}
	if len(encodings) != 1 || encodings[0] != "zstd" {
		return nil, http.StatusUnsupportedMediaType, fmt.Sprintf("unsupported content encoding: %s", strings.Join(encodings, ", ")), errors.New("unsupported content encoding")
	}

	compressed, err := readBodyWithLimit(r.Body, maxBytes)
	if err != nil {
		status, message, readErr := requestBodyReadError(err, "failed to read request body")
		return nil, status, message, readErr
	}
	decoder, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, http.StatusBadRequest, "invalid zstd body", err
	}
	defer decoder.Close()

	body, err := readBodyWithLimit(decoder, maxBytes)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			return nil, http.StatusRequestEntityTooLarge, "request body too large", err
		}
		return nil, http.StatusBadRequest, "invalid zstd body", err
	}
	return body, 0, "", nil
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
