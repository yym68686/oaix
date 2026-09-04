package proxy

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"

	"github.com/yym68686/oaix/internal/protocol/openai"
)

// RequestDocument owns the one decoded representation of a JSON request. The
// standard decoder preserves json.Number values and its last-key-wins behavior
// for duplicate object fields matches the previous request preparation path.
type RequestDocument struct {
	raw       []byte
	rawSHA256 string
	value     any
	object    map[string]any
	parseErr  error
	strictErr error
	dirty     bool
	encoded   []byte
	encodes   int
}

func newRequestDocument(body []byte, rawSHA256 string) *RequestDocument {
	document := &RequestDocument{
		raw:       body,
		rawSHA256: rawSHA256,
	}
	if document.rawSHA256 == "" {
		document.rawSHA256 = sha256Bytes(body)
	}
	decoder := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&document.value); err != nil {
		document.parseErr = err
		return document
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("request body contains multiple JSON values")
		}
		document.strictErr = err
	}
	document.object, _ = document.value.(map[string]any)
	return document
}

func (d *RequestDocument) StrictObject() (map[string]any, error) {
	object, err := d.Object()
	if err != nil {
		return nil, err
	}
	if d.strictErr != nil {
		return nil, errors.New("request body must be valid JSON")
	}
	return object, nil
}

func (d *RequestDocument) Object() (map[string]any, error) {
	if d == nil || d.parseErr != nil {
		return nil, errors.New("request body must be valid JSON")
	}
	if d.object == nil {
		return nil, errors.New("request body must be a JSON object")
	}
	return d.object, nil
}

func (d *RequestDocument) RawSHA256() string {
	if d == nil {
		return sha256Bytes(nil)
	}
	return d.rawSHA256
}

func (d *RequestDocument) MarkDirty() {
	if d == nil {
		return
	}
	d.dirty = true
	d.encoded = nil
}

func (d *RequestDocument) ReplaceObject(object map[string]any) {
	if d == nil {
		return
	}
	d.value = object
	d.object = object
	d.parseErr = nil
	d.strictErr = nil
	d.MarkDirty()
}

func (d *RequestDocument) Bytes() ([]byte, error) {
	if d == nil {
		return nil, errors.New("request document is unavailable")
	}
	if !d.dirty {
		return d.raw, nil
	}
	if d.encoded != nil {
		return d.encoded, nil
	}
	object, err := d.Object()
	if err != nil {
		return nil, err
	}
	encoded, err := openai.EncodeJSON(object)
	if err != nil {
		return nil, err
	}
	d.encoded = encoded
	d.encodes++
	return d.encoded, nil
}

func (d *RequestDocument) CanonicalHash() string {
	if d == nil || d.value == nil || d.parseErr != nil {
		return ""
	}
	hasher := sha256.New()
	w := &finalNewlineHashWriter{hash: hasher}
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(d.value); err != nil {
		return ""
	}
	w.Close()
	return hex.EncodeToString(hasher.Sum(nil))
}

type finalNewlineHashWriter struct {
	hash hash.Hash
	tail byte
	has  bool
}

func (w *finalNewlineHashWriter) Write(value []byte) (int, error) {
	if len(value) == 0 {
		return 0, nil
	}
	if w.has {
		_, _ = w.hash.Write([]byte{w.tail})
	}
	if len(value) > 1 {
		_, _ = w.hash.Write(value[:len(value)-1])
	}
	w.tail = value[len(value)-1]
	w.has = true
	return len(value), nil
}

func (w *finalNewlineHashWriter) Close() {
	if w.has && w.tail != '\n' {
		_, _ = w.hash.Write([]byte{w.tail})
	}
	w.has = false
}
