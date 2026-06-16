package openai

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"strconv"
)

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
}

func NewError(message, typ, code string) ErrorResponse {
	return ErrorResponse{Error: ErrorBody{Message: message, Type: typ, Code: code}}
}

type ResponsesRequest struct {
	Model              string          `json:"model,omitempty"`
	Input              json.RawMessage `json:"input,omitempty"`
	Instructions       string          `json:"instructions,omitempty"`
	Stream             bool            `json:"stream,omitempty"`
	PreviousResponseID string          `json:"previous_response_id,omitempty"`
	Store              *bool           `json:"store,omitempty"`
	Metadata           map[string]any  `json:"metadata,omitempty"`
	Raw                map[string]any  `json:"-"`
}

type ResponsesResponse struct {
	ID     string          `json:"id,omitempty"`
	Object string          `json:"object,omitempty"`
	Model  string          `json:"model,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Usage  map[string]any  `json:"usage,omitempty"`
}

type ChatCompletionRequest struct {
	Model       string          `json:"model,omitempty"`
	Messages    []ChatMessage   `json:"messages,omitempty"`
	Stream      bool            `json:"stream,omitempty"`
	Temperature *float64        `json:"temperature,omitempty"`
	TopP        *float64        `json:"top_p,omitempty"`
	MaxTokens   *int            `json:"max_tokens,omitempty"`
	Tools       json.RawMessage `json:"tools,omitempty"`
	Metadata    map[string]any  `json:"metadata,omitempty"`
	Raw         map[string]any  `json:"-"`
}

type ChatMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content,omitempty"`
	Name    string          `json:"name,omitempty"`
}

type ChatCompletionResponse struct {
	ID      string          `json:"id,omitempty"`
	Object  string          `json:"object,omitempty"`
	Model   string          `json:"model,omitempty"`
	Choices json.RawMessage `json:"choices,omitempty"`
	Usage   map[string]any  `json:"usage,omitempty"`
}

type ImageGenerationRequest struct {
	Model          string         `json:"model,omitempty"`
	Prompt         string         `json:"prompt,omitempty"`
	N              int            `json:"n,omitempty"`
	Size           string         `json:"size,omitempty"`
	Quality        string         `json:"quality,omitempty"`
	ResponseFormat string         `json:"response_format,omitempty"`
	Raw            map[string]any `json:"-"`
}

type ImageEditRequest struct {
	Model  string
	Prompt string
	N      int
	Size   string
	Files  []string
	Fields map[string][]string
}

func DecodeResponsesRequest(body []byte) (ResponsesRequest, error) {
	var req ResponsesRequest
	if err := decodeObject(body, &req, &req.Raw); err != nil {
		return ResponsesRequest{}, err
	}
	return req, nil
}

func DecodeChatCompletionRequest(body []byte) (ChatCompletionRequest, error) {
	var req ChatCompletionRequest
	if err := decodeObject(body, &req, &req.Raw); err != nil {
		return ChatCompletionRequest{}, err
	}
	return req, nil
}

func DecodeImageGenerationRequest(body []byte) (ImageGenerationRequest, error) {
	var req ImageGenerationRequest
	if err := decodeObject(body, &req, &req.Raw); err != nil {
		return ImageGenerationRequest{}, err
	}
	return req, nil
}

func DecodeImageEditRequest(r *http.Request, maxMemory int64) (ImageEditRequest, error) {
	if r == nil {
		return ImageEditRequest{}, errors.New("nil request")
	}
	if maxMemory <= 0 {
		maxMemory = 64 << 20
	}
	if err := r.ParseMultipartForm(maxMemory); err != nil {
		return ImageEditRequest{}, err
	}
	req := ImageEditRequest{Fields: map[string][]string{}}
	if r.MultipartForm != nil {
		for key, values := range r.MultipartForm.Value {
			req.Fields[key] = append([]string(nil), values...)
		}
		if files := r.MultipartForm.File; files != nil {
			for key, headers := range files {
				for _, header := range headers {
					req.Files = append(req.Files, key+":"+header.Filename)
				}
			}
		}
	}
	req.Model = firstFormValue(req.Fields, "model")
	req.Prompt = firstFormValue(req.Fields, "prompt")
	req.Size = firstFormValue(req.Fields, "size")
	if n, err := strconv.Atoi(firstFormValue(req.Fields, "n")); err == nil {
		req.N = n
	}
	return req, nil
}

func EncodeJSON(value any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return bytes.TrimSpace(buf.Bytes()), nil
}

func CloneMultipartBody(form *multipart.Form) ([]byte, string, error) {
	var buf bytes.Buffer
	writer := multipart.NewWriter(&buf)
	if form != nil {
		for key, values := range form.Value {
			for _, value := range values {
				if err := writer.WriteField(key, value); err != nil {
					return nil, "", err
				}
			}
		}
		for key, files := range form.File {
			for _, header := range files {
				src, err := header.Open()
				if err != nil {
					return nil, "", err
				}
				dst, err := writer.CreateFormFile(key, header.Filename)
				if err != nil {
					_ = src.Close()
					return nil, "", err
				}
				if _, err := io.Copy(dst, src); err != nil {
					_ = src.Close()
					return nil, "", err
				}
				_ = src.Close()
			}
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return buf.Bytes(), writer.FormDataContentType(), nil
}

func decodeObject(body []byte, target any, raw *map[string]any) error {
	if len(bytes.TrimSpace(body)) == 0 {
		return errors.New("empty request body")
	}
	if err := json.Unmarshal(body, target); err != nil {
		return err
	}
	if raw != nil {
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			return err
		}
		*raw = payload
	}
	return nil
}

func firstFormValue(values map[string][]string, key string) string {
	if len(values[key]) == 0 {
		return ""
	}
	return values[key][0]
}
