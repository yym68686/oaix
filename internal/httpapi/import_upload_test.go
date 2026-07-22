package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestUploadImportCombinesMultipleJSONFiles(t *testing.T) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for index := 0; index < 3; index++ {
		filename := fmt.Sprintf("account-%d@example.com.json", index)
		part, err := writer.CreateFormFile("files", filename)
		if err != nil {
			t.Fatal(err)
		}
		payload := map[string]any{
			"accounts": []any{
				map[string]any{
					"name":     fmt.Sprintf("account-%d@example.com", index),
					"platform": "openai",
					"type":     "oauth",
					"credentials": map[string]any{
						"auth_mode":         "agentIdentity",
						"agent_runtime_id":  fmt.Sprintf("runtime-%d", index),
						"agent_private_key": fmt.Sprintf("private-key-%d", index),
						"task_id":           fmt.Sprintf("task-%d", index),
						"email":             fmt.Sprintf("account-%d@example.com", index),
					},
				},
			},
			"proxies": []any{},
		}
		if err := json.NewEncoder(part).Encode(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	request := httptest.NewRequest(http.MethodPost, "/admin/import/upload", &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	response := httptest.NewRecorder()
	(&App{}).uploadImport(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var result struct {
		Items []map[string]any `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 3 {
		t.Fatalf("item count = %d, want 3: %#v", len(result.Items), result.Items)
	}
	for index, item := range result.Items {
		wantFilename := fmt.Sprintf("account-%d@example.com.json", index)
		if item["source_file"] != wantFilename || item["agent_runtime_id"] != fmt.Sprintf("runtime-%d", index) {
			t.Fatalf("item %d = %#v", index, item)
		}
	}
}
