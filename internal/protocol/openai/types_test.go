package openai

import (
	"bytes"
	"mime/multipart"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeResponsesRequest(t *testing.T) {
	req, err := DecodeResponsesRequest([]byte(`{"model":"gpt-5","stream":true,"previous_response_id":"resp_1","input":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "gpt-5" || !req.Stream || req.PreviousResponseID != "resp_1" {
		t.Fatalf("unexpected request: %+v", req)
	}
	if req.Raw["model"] != "gpt-5" {
		t.Fatalf("raw payload not preserved: %+v", req.Raw)
	}
}

func TestDecodeChatCompletionRequest(t *testing.T) {
	req, err := DecodeChatCompletionRequest([]byte(`{"model":"gpt-5","messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "gpt-5" || len(req.Messages) != 1 || req.Messages[0].Role != "user" {
		t.Fatalf("unexpected chat request: %+v", req)
	}
}

func TestDecodeImageGenerationRequest(t *testing.T) {
	req, err := DecodeImageGenerationRequest([]byte(`{"model":"gpt-image-1","prompt":"logo","n":2,"size":"1024x1024"}`))
	if err != nil {
		t.Fatal(err)
	}
	if req.Model != "gpt-image-1" || req.Prompt != "logo" || req.N != 2 || req.Size != "1024x1024" {
		t.Fatalf("unexpected image request: %+v", req)
	}
}

func TestDecodeImageEditRequest(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	_ = writer.WriteField("model", "gpt-image-1")
	_ = writer.WriteField("prompt", "edit")
	_ = writer.WriteField("n", "3")
	file, err := writer.CreateFormFile("image", "input.png")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = file.Write([]byte("png"))
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("POST", "/v1/images/edits", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	decoded, err := DecodeImageEditRequest(req, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Model != "gpt-image-1" || decoded.Prompt != "edit" || decoded.N != 3 {
		t.Fatalf("unexpected image edit request: %+v", decoded)
	}
	if len(decoded.Files) != 1 || !strings.Contains(decoded.Files[0], "input.png") {
		t.Fatalf("file metadata missing: %+v", decoded.Files)
	}
}
