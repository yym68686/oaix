package proxy

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/yym68686/oaix/internal/protocol/openai"
	"github.com/yym68686/oaix/internal/protocol/sse"
)

const (
	defaultImagesMainModel = "gpt-5.5"
	defaultImagesToolModel = "gpt-image-2"
	defaultImageInputMax   = 249
	defaultImageUploadMax  = 25 * 1024 * 1024
)

var (
	imageToolTextFields = map[string]struct{}{
		"size":           {},
		"quality":        {},
		"background":     {},
		"output_format":  {},
		"input_fidelity": {},
		"moderation":     {},
	}
	imageToolIntFields = map[string]struct{}{
		"output_compression": {},
		"partial_images":     {},
	}
)

type imageCallResult struct {
	ResultB64     string
	RevisedPrompt string
	OutputFormat  string
	Size          string
	Background    string
	Quality       string
}

func prepareUpstreamPayload(r *http.Request, body []byte, intent RequestIntent) ([]byte, RequestIntent, int, error) {
	switch intent.Endpoint {
	case "/v1/responses", "/v1/responses/compact":
		next, err := prepareResponsesPayload(body, &intent)
		if err != nil {
			return body, intent, http.StatusBadRequest, err
		}
		return next, intent, http.StatusOK, nil
	case "/v1/images/generations":
		next, err := prepareImageGenerationPayload(body, &intent)
		if err != nil {
			return body, intent, http.StatusBadRequest, err
		}
		return next, intent, http.StatusOK, nil
	case "/v1/images/edits":
		next, err := prepareImageEditPayload(r, body, &intent)
		if err != nil {
			return body, intent, http.StatusBadRequest, err
		}
		return next, intent, http.StatusOK, nil
	default:
		return body, intent, http.StatusOK, nil
	}
}

func prepareResponsesPayload(body []byte, intent *RequestIntent) ([]byte, error) {
	payload, err := decodeJSONObject(body)
	if err != nil {
		return body, err
	}
	model := text(payload["model"])
	preservePreviousResponseID := false
	if model == defaultImagesToolModel {
		translated, err := translateResponsesImageCompatPayload(payload, intent.Compact)
		if err != nil {
			return body, err
		}
		payload = translated
		intent.ResponseModelAlias = model
		preservePreviousResponseID = true
	} else {
		preservePreviousResponseID = false
	}
	sanitizeCodexPayload(payload, intent.Compact, preservePreviousResponseID)
	if !intent.Compact && !intent.Stream {
		payload["stream"] = true
	}
	if intent.Compact && !intent.Stream {
		delete(payload, "stream")
	}
	if intent.Stream || !intent.Compact {
		intent.UpstreamAccept = "text/event-stream"
	} else {
		intent.UpstreamAccept = "application/json"
	}
	return openai.EncodeJSON(payload)
}

func translateResponsesImageCompatPayload(payload map[string]any, compact bool) (map[string]any, error) {
	if compact {
		return nil, errors.New("gpt-image-2 is only supported on /v1/responses")
	}
	if err := enforceImageInputCountLimit(countResponsesInputImages(payload)); err != nil {
		return nil, err
	}
	translated := deepCloneMap(payload)
	requestedModel := text(translated["model"])
	translated["model"] = defaultImagesMainModel
	tool := ensureImageGenerationTool(translated)
	tool["type"] = "image_generation"
	tool["model"] = requestedModel
	if text(tool["action"]) == "" {
		tool["action"] = "auto"
	}
	moveImageToolFields(translated, tool)
	applyImagesResponsesDefaults(translated)
	delete(translated, "tool_choice")
	return translated, nil
}

func prepareImageGenerationPayload(body []byte, intent *RequestIntent) ([]byte, error) {
	payload, err := decodeJSONObject(body)
	if err != nil {
		return body, err
	}
	prompt := text(payload["prompt"])
	if prompt == "" {
		return nil, errors.New("prompt is required")
	}
	imageModel := firstNonEmpty(text(payload["model"]), defaultImagesToolModel)
	intent.Model = imageModel
	intent.UpstreamEndpoint = "/v1/responses"
	intent.UpstreamContentType = "application/json"
	intent.UpstreamAccept = "text/event-stream"
	intent.ImageResponseFormat = normalizeImageResponseFormat(payload["response_format"])
	intent.ImageStreamPrefix = "image_generation"
	if stream, ok := payload["stream"].(bool); ok {
		intent.Stream = stream
	}
	tool := map[string]any{
		"type":   "image_generation",
		"action": "generate",
		"model":  imageModel,
	}
	copyImageToolFields(payload, tool, false)
	upstream := buildImagesResponsesPayload(prompt, nil, tool)
	return openai.EncodeJSON(upstream)
}

func prepareImageEditPayload(r *http.Request, body []byte, intent *RequestIntent) ([]byte, error) {
	contentType := strings.TrimSpace(r.Header.Get("Content-Type"))
	lowerContentType := strings.ToLower(contentType)
	if strings.HasPrefix(lowerContentType, "application/json") {
		return prepareImageEditJSONPayload(body, intent)
	}
	if contentType == "" || strings.HasPrefix(lowerContentType, "multipart/form-data") {
		return prepareImageEditMultipartPayload(contentType, body, intent)
	}
	return nil, fmt.Errorf("unsupported Content-Type %q", contentType)
}

func prepareImageEditJSONPayload(body []byte, intent *RequestIntent) ([]byte, error) {
	payload, err := decodeJSONObject(body)
	if err != nil {
		return body, err
	}
	prompt := text(payload["prompt"])
	if prompt == "" {
		return nil, errors.New("prompt is required")
	}
	images := jsonImageReferences(payload)
	if len(images) == 0 {
		return nil, errors.New("images[].image_url is required")
	}
	mask, err := jsonMaskReference(payload["mask"])
	if err != nil {
		return nil, err
	}
	if err := enforceImageInputCountLimit(len(images) + boolCount(mask != "")); err != nil {
		return nil, err
	}
	imageModel := firstNonEmpty(text(payload["model"]), defaultImagesToolModel)
	intent.Model = imageModel
	intent.UpstreamEndpoint = "/v1/responses"
	intent.UpstreamContentType = "application/json"
	intent.UpstreamAccept = "text/event-stream"
	intent.ImageResponseFormat = normalizeImageResponseFormat(payload["response_format"])
	intent.ImageStreamPrefix = "image_edit"
	if stream, ok := payload["stream"].(bool); ok {
		intent.Stream = stream
	}
	tool := map[string]any{
		"type":   "image_generation",
		"action": "edit",
		"model":  imageModel,
	}
	copyImageToolFields(payload, tool, true)
	if mask != "" {
		tool["input_image_mask"] = map[string]any{"image_url": mask}
	}
	upstream := buildImagesResponsesPayload(prompt, images, tool)
	return openai.EncodeJSON(upstream)
}

func prepareImageEditMultipartPayload(contentType string, body []byte, intent *RequestIntent) ([]byte, error) {
	_, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return nil, fmt.Errorf("invalid multipart Content-Type: %w", err)
	}
	boundary := params["boundary"]
	if boundary == "" {
		return nil, errors.New("multipart boundary is required")
	}
	form, err := multipart.NewReader(bytes.NewReader(body), boundary).ReadForm(64 << 20)
	if err != nil {
		return nil, fmt.Errorf("invalid multipart form: %w", err)
	}
	defer form.RemoveAll()
	prompt := firstFormValueAny(form.Value, "prompt")
	if prompt == "" {
		return nil, errors.New("prompt is required")
	}
	images, err := multipartImageReferences(form, "image[]", "image")
	if err != nil {
		return nil, err
	}
	if len(images) == 0 {
		return nil, errors.New("image is required")
	}
	mask, err := multipartMaskReference(form)
	if err != nil {
		return nil, err
	}
	if err := enforceImageInputCountLimit(len(images) + boolCount(mask != "")); err != nil {
		return nil, err
	}
	imageModel := firstNonEmpty(firstFormValueAny(form.Value, "model"), defaultImagesToolModel)
	intent.Model = imageModel
	intent.UpstreamEndpoint = "/v1/responses"
	intent.UpstreamContentType = "application/json"
	intent.UpstreamAccept = "text/event-stream"
	intent.ImageResponseFormat = normalizeImageResponseFormat(firstFormValueAny(form.Value, "response_format"))
	intent.ImageStreamPrefix = "image_edit"
	if stream, ok := parseBool(firstFormValueAny(form.Value, "stream")); ok {
		intent.Stream = stream
	}
	tool := map[string]any{
		"type":   "image_generation",
		"action": "edit",
		"model":  imageModel,
	}
	copyImageToolFields(formMap(form.Value), tool, true)
	if mask != "" {
		tool["input_image_mask"] = map[string]any{"image_url": mask}
	}
	upstream := buildImagesResponsesPayload(prompt, images, tool)
	return openai.EncodeJSON(upstream)
}

func sanitizeCodexPayload(payload map[string]any, compact bool, preservePreviousResponseID bool) {
	removeReasoningContentFields(payload)
	delete(payload, "max_output_tokens")
	delete(payload, "response_format")
	if !preservePreviousResponseID {
		delete(payload, "previous_response_id")
	}
	delete(payload, "conversation_id")
	delete(payload, "session_id")
	delete(payload, "prompt_cache_retention")
	delete(payload, "safety_identifier")
	if compact {
		delete(payload, "store")
	} else {
		payload["store"] = false
		stripStoreFalseReasoningInputIDs(payload["input"])
	}
	if _, ok := payload["instructions"]; !ok {
		payload["instructions"] = ""
	}
}

func stripStoreFalseReasoningInputIDs(value any) bool {
	switch typed := value.(type) {
	case []any:
		changed := false
		for _, item := range typed {
			if stripStoreFalseReasoningInputIDs(item) {
				changed = true
			}
		}
		return changed
	case map[string]any:
		if text(typed["type"]) != "reasoning" {
			return false
		}
		if _, ok := typed["id"]; !ok {
			return false
		}
		delete(typed, "id")
		return true
	default:
		return false
	}
}

func ensureImageGenerationTool(payload map[string]any) map[string]any {
	var tools []any
	var found map[string]any
	if existing, ok := payload["tools"].([]any); ok {
		tools = make([]any, 0, len(existing)+1)
		for _, item := range existing {
			if mapping, ok := item.(map[string]any); ok {
				copied := deepCloneMap(mapping)
				if text(copied["type"]) == "image_generation" {
					found = copied
				}
				tools = append(tools, copied)
				continue
			}
			tools = append(tools, item)
		}
	}
	if found != nil {
		payload["tools"] = tools
		return found
	}
	tool := map[string]any{"type": "image_generation"}
	tools = append(tools, tool)
	payload["tools"] = tools
	return tool
}

func moveImageToolFields(payload, tool map[string]any) {
	for key := range imageToolTextFields {
		if value := text(payload[key]); value != "" {
			tool[key] = value
		}
		delete(payload, key)
	}
	for key := range imageToolIntFields {
		raw, ok := payload[key]
		if ok {
			if value, valueOK := coerceInt(raw); valueOK {
				tool[key] = value
			}
		}
		delete(payload, key)
	}
}

func copyImageToolFields(source, tool map[string]any, includeInputFidelity bool) {
	for key := range imageToolTextFields {
		if key == "input_fidelity" && !includeInputFidelity {
			continue
		}
		if value := text(source[key]); value != "" {
			tool[key] = value
		}
	}
	for key := range imageToolIntFields {
		if value, ok := coerceInt(source[key]); ok {
			tool[key] = value
		}
	}
}

func buildImagesResponsesPayload(prompt string, images []string, tool map[string]any) map[string]any {
	content := []any{map[string]any{"type": "input_text", "text": prompt}}
	for _, imageURL := range images {
		if normalized := strings.TrimSpace(imageURL); normalized != "" {
			content = append(content, map[string]any{"type": "input_image", "image_url": normalized})
		}
	}
	payload := map[string]any{
		"stream": true,
		"model":  defaultImagesMainModel,
		"input": []any{map[string]any{
			"type":    "message",
			"role":    "user",
			"content": content,
		}},
		"tools": []any{tool},
	}
	applyImagesResponsesDefaults(payload)
	return payload
}

func applyImagesResponsesDefaults(payload map[string]any) {
	if _, ok := payload["instructions"]; !ok {
		payload["instructions"] = ""
	}
	if _, ok := payload["parallel_tool_calls"]; !ok {
		payload["parallel_tool_calls"] = true
	}
	if _, ok := payload["reasoning"].(map[string]any); !ok {
		payload["reasoning"] = map[string]any{"effort": "medium", "summary": "auto"}
	}
	include, _ := payload["include"].([]any)
	found := false
	for _, item := range include {
		if text(item) == "reasoning.encrypted_content" {
			found = true
			break
		}
	}
	if !found {
		include = append(include, "reasoning.encrypted_content")
	}
	payload["include"] = include
	payload["store"] = false
}

func decodeJSONObject(body []byte) (map[string]any, error) {
	var payload any
	decoder := json.NewDecoder(bytes.NewReader(bytes.TrimSpace(body)))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return nil, errors.New("request body must be valid JSON")
	}
	mapping, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("request body must be a JSON object")
	}
	return mapping, nil
}

func jsonImageReferences(payload map[string]any) []string {
	var images []string
	for _, key := range []string{"images", "image", "image[]"} {
		images = append(images, imageReferencesFromValue(payload[key])...)
	}
	return images
}

func imageReferencesFromValue(value any) []string {
	switch typed := value.(type) {
	case []any:
		images := make([]string, 0, len(typed))
		for _, item := range typed {
			images = append(images, imageReferencesFromValue(item)...)
		}
		return images
	default:
		if imageURL := jsonImageReference(value); imageURL != "" {
			return []string{imageURL}
		}
		return nil
	}
}

func jsonImageReference(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		if imageURL := imageURLCandidate(typed["image_url"]); imageURL != "" {
			return imageURL
		}
		return imageURLCandidate(typed["url"])
	default:
		return ""
	}
}

func jsonMaskReference(value any) (string, error) {
	if mapping, ok := value.(map[string]any); ok && text(mapping["file_id"]) != "" {
		return "", errors.New("mask.file_id is not supported (use mask.image_url instead)")
	}
	return jsonImageReference(value), nil
}

func imageURLCandidate(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case map[string]any:
		return strings.TrimSpace(text(typed["url"]))
	default:
		return ""
	}
}

func multipartImageReferences(form *multipart.Form, keys ...string) ([]string, error) {
	var images []string
	for _, key := range keys {
		for _, value := range form.Value[key] {
			if normalized := strings.TrimSpace(value); normalized != "" {
				images = append(images, normalized)
			}
		}
		for _, header := range form.File[key] {
			dataURL, err := multipartFileToDataURL(header)
			if err != nil {
				return nil, err
			}
			images = append(images, dataURL)
		}
	}
	return images, nil
}

func multipartMaskReference(form *multipart.Form) (string, error) {
	if values := form.Value["mask"]; len(values) > 0 {
		return strings.TrimSpace(values[0]), nil
	}
	if files := form.File["mask"]; len(files) > 0 {
		return multipartFileToDataURL(files[0])
	}
	return "", nil
}

func multipartFileToDataURL(header *multipart.FileHeader) (string, error) {
	src, err := header.Open()
	if err != nil {
		return "", err
	}
	defer src.Close()
	maxBytes := imageUploadMaxBytes()
	payload, err := io.ReadAll(io.LimitReader(src, int64(maxBytes)+1))
	if err != nil {
		return "", err
	}
	if len(payload) > maxBytes {
		return "", errors.New("uploaded image is too large")
	}
	mediaType := header.Header.Get("Content-Type")
	if strings.TrimSpace(mediaType) == "" {
		mediaType = "application/octet-stream"
	}
	return "data:" + mediaType + ";base64," + base64.StdEncoding.EncodeToString(payload), nil
}

func formMap(values map[string][]string) map[string]any {
	out := make(map[string]any, len(values))
	for key, list := range values {
		if len(list) > 0 {
			out[key] = list[0]
		}
	}
	return out
}

func firstFormValueAny(values map[string][]string, key string) string {
	if list := values[key]; len(list) > 0 {
		return strings.TrimSpace(list[0])
	}
	return ""
}

func normalizeImageResponseFormat(value any) string {
	if strings.EqualFold(text(value), "url") {
		return "url"
	}
	return "b64_json"
}

func outputMimeType(outputFormat string) string {
	switch strings.ToLower(strings.TrimSpace(outputFormat)) {
	case "jpeg", "jpg":
		return "image/jpeg"
	case "webp":
		return "image/webp"
	default:
		return "image/png"
	}
}

func coerceInt(value any) (int, bool) {
	switch typed := value.(type) {
	case nil:
		return 0, false
	case int:
		return typed, true
	case int64:
		return int(typed), true
	case float64:
		return int(typed), true
	case json.Number:
		next, err := typed.Int64()
		if err != nil {
			return 0, false
		}
		return int(next), true
	case string:
		next, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, false
		}
		return next, true
	default:
		return 0, false
	}
}

func parseBool(value string) (bool, bool) {
	if value == "" {
		return false, false
	}
	next, err := strconv.ParseBool(strings.TrimSpace(value))
	return next, err == nil
}

func deepCloneMap(input map[string]any) map[string]any {
	data, err := json.Marshal(input)
	if err != nil {
		return cloneMap(input)
	}
	var output map[string]any
	if err := json.Unmarshal(data, &output); err != nil {
		return cloneMap(input)
	}
	return output
}

func decodeUpstreamError(reader io.Reader) string {
	raw, _ := io.ReadAll(io.LimitReader(reader, 256*1024))
	if len(bytes.TrimSpace(raw)) == 0 {
		return "upstream request failed"
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err == nil {
		if detail := text(payload["detail"]); detail != "" {
			return detail
		}
		if errObj, ok := payload["error"].(map[string]any); ok {
			if msg := text(errObj["message"]); msg != "" {
				return msg
			}
		}
	}
	return strings.TrimSpace(string(raw))
}

func responseFailureStatus(payload map[string]any) (int, string, bool) {
	var errObj map[string]any
	if text(payload["type"]) == "response.failed" {
		if response, ok := payload["response"].(map[string]any); ok {
			if nested, ok := response["error"].(map[string]any); ok {
				errObj = nested
			}
		}
	}
	if errObj == nil {
		if nested, ok := payload["error"].(map[string]any); ok {
			errObj = nested
		}
	}
	if errObj == nil {
		return 0, "", false
	}
	message := firstNonEmpty(text(errObj["message"]), "Responses upstream returned status=failed")
	status := http.StatusInternalServerError
	code := strings.ToLower(text(errObj["code"]))
	typ := strings.ToLower(text(errObj["type"]))
	if strings.Contains(code, "rate_limit") || strings.Contains(typ, "rate_limit") || strings.Contains(strings.ToLower(message), "concurrency limit") {
		status = http.StatusTooManyRequests
	} else if strings.Contains(code, "invalid") || strings.Contains(typ, "invalid") {
		status = http.StatusBadRequest
	} else if strings.Contains(code, "permission") || strings.Contains(typ, "permission") {
		status = http.StatusForbidden
	}
	return status, message, true
}

func retryableStatus(status int) bool {
	return status == http.StatusUnauthorized ||
		status == http.StatusForbidden ||
		status == http.StatusTooManyRequests ||
		status >= 500
}

func isPreflightResponseEvent(eventType string) bool {
	switch eventType {
	case "response.created", "response.in_progress", "response.queued", "keepalive":
		return true
	default:
		return false
	}
}

func eventType(event sse.Event, payload map[string]any) string {
	if strings.TrimSpace(event.Event) != "" {
		return strings.TrimSpace(event.Event)
	}
	return text(payload["type"])
}

func parseEventPayload(event sse.Event) map[string]any {
	var payload map[string]any
	_ = json.Unmarshal(event.Data, &payload)
	return payload
}

type outputTextKey struct {
	outputIndex  int
	contentIndex int
}

var errStopSSE = errors.New("stop sse parse")

func imageInputMaxPerRequest() int {
	return envIntAtLeast("IMAGE_INPUT_MAX_PER_REQUEST", defaultImageInputMax, 1)
}

func imageUploadMaxBytes() int {
	return envIntAtLeast("IMAGE_UPLOAD_MAX_BYTES", defaultImageUploadMax, 1024)
}

func envIntAtLeast(key string, fallback int, minimum int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum {
		return fallback
	}
	return value
}

func boolCount(value bool) int {
	if value {
		return 1
	}
	return 0
}

func countResponsesInputImages(value any) int {
	switch typed := value.(type) {
	case []any:
		count := 0
		for _, item := range typed {
			count += countResponsesInputImages(item)
		}
		return count
	case map[string]any:
		itemType := text(typed["type"])
		if itemType == "image_url" || itemType == "input_image" {
			if imageURLCandidate(firstPresentCompat(typed["image_url"], typed["url"])) != "" {
				return 1
			}
		}
		count := 0
		if maskValue, ok := typed["input_image_mask"]; ok {
			if imageURLCandidate(maskValue) != "" {
				count++
			} else if maskMap, ok := maskValue.(map[string]any); ok && imageURLCandidate(maskMap["image_url"]) != "" {
				count++
			}
		}
		for key, child := range typed {
			if key == "input_image_mask" {
				continue
			}
			count += countResponsesInputImages(child)
		}
		return count
	default:
		return 0
	}
}

func firstPresentCompat(values ...any) any {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}

func enforceImageInputCountLimit(count int) error {
	maxImages := imageInputMaxPerRequest()
	if count <= maxImages {
		return nil
	}
	return fmt.Errorf("too many input images: %d. at most %d input images are allowed per request", count, maxImages)
}

func responseContext(resp *http.Response) context.Context {
	if resp != nil && resp.Request != nil && resp.Request.Context() != nil {
		return resp.Request.Context()
	}
	return context.Background()
}

func patchResponseModelAlias(value any, alias string) any {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return value
	}
	mapping, ok := deepCloneAny(value).(map[string]any)
	if !ok {
		return value
	}
	if _, hasModel := mapping["model"]; hasModel {
		mapping["model"] = alias
	}
	if _, hasModelName := mapping["model_name"]; hasModelName {
		mapping["model_name"] = alias
		mapping["model"] = alias
	}
	if responsePayload, ok := mapping["response"].(map[string]any); ok {
		responsePayload["model"] = alias
		if _, hasModelName := responsePayload["model_name"]; hasModelName {
			responsePayload["model_name"] = alias
		}
	}
	return mapping
}

func rawEventForDownstream(event sse.Event, alias string) []byte {
	if strings.TrimSpace(alias) == "" {
		return append([]byte(nil), event.Raw...)
	}
	data := strings.TrimSpace(string(event.Data))
	if data == "" || data == "[DONE]" {
		return append([]byte(nil), event.Raw...)
	}
	var payload any
	if err := json.Unmarshal(event.Data, &payload); err != nil {
		return append([]byte(nil), event.Raw...)
	}
	patched := patchResponseModelAlias(payload, alias)
	encoded, err := openai.EncodeJSON(patched)
	if err != nil {
		return append([]byte(nil), event.Raw...)
	}
	eventName := strings.TrimSpace(event.Event)
	if eventName == "" {
		if mapping, ok := patched.(map[string]any); ok {
			eventName = text(mapping["type"])
		}
	}
	return sse.Encode(eventName, encoded)
}

func structuredKeepaliveRaw(sequenceNumber int) []byte {
	payload := []byte(fmt.Sprintf(`{"type":"keepalive","sequence_number":%d}`, sequenceNumber))
	return sse.Encode("keepalive", payload)
}

func deepCloneAny(input any) any {
	data, err := json.Marshal(input)
	if err != nil {
		return input
	}
	var output any
	if err := json.Unmarshal(data, &output); err != nil {
		return input
	}
	return output
}

func mergeMapping(target, patch map[string]any) map[string]any {
	for key, value := range patch {
		if nestedPatch, ok := value.(map[string]any); ok {
			if nestedTarget, ok := target[key].(map[string]any); ok {
				mergeMapping(nestedTarget, nestedPatch)
				continue
			}
		}
		target[key] = value
	}
	return target
}

func appendOutputTextDelta(parts map[outputTextKey][]string, payload map[string]any) {
	delta, ok := payload["delta"]
	if !ok {
		return
	}
	outputIndex, _ := coerceInt(payload["output_index"])
	contentIndex, _ := coerceInt(payload["content_index"])
	key := outputTextKey{outputIndex: outputIndex, contentIndex: contentIndex}
	parts[key] = append(parts[key], fmt.Sprint(delta))
}

func buildResponsesOutputFromTextParts(parts map[outputTextKey][]string) []any {
	if len(parts) == 0 {
		return nil
	}
	grouped := map[int]map[int]string{}
	for key, values := range parts {
		if _, ok := grouped[key.outputIndex]; !ok {
			grouped[key.outputIndex] = map[int]string{}
		}
		grouped[key.outputIndex][key.contentIndex] = strings.Join(values, "")
	}
	outputIndexes := make([]int, 0, len(grouped))
	for index := range grouped {
		outputIndexes = append(outputIndexes, index)
	}
	sortInts(outputIndexes)
	output := make([]any, 0, len(outputIndexes))
	for _, outputIndex := range outputIndexes {
		contentIndexes := make([]int, 0, len(grouped[outputIndex]))
		for contentIndex := range grouped[outputIndex] {
			contentIndexes = append(contentIndexes, contentIndex)
		}
		sortInts(contentIndexes)
		content := make([]any, 0, len(contentIndexes))
		for _, contentIndex := range contentIndexes {
			content = append(content, map[string]any{
				"type": "output_text",
				"text": grouped[outputIndex][contentIndex],
			})
		}
		output = append(output, map[string]any{
			"type":    "message",
			"content": content,
		})
	}
	return output
}

func sortInts(values []int) {
	for i := 1; i < len(values); i++ {
		current := values[i]
		j := i - 1
		for ; j >= 0 && values[j] > current; j-- {
			values[j+1] = values[j]
		}
		values[j+1] = current
	}
}

func finalizeCollectedResponse(snapshot map[string]any, model string, textParts map[outputTextKey][]string) map[string]any {
	responseData := map[string]any{}
	if snapshot != nil {
		responseData = deepCloneMap(snapshot)
	}
	if text(responseData["id"]) == "" {
		responseData["id"] = fmt.Sprintf("resp_%x", time.Now().UnixNano())
	}
	if text(responseData["object"]) == "" {
		responseData["object"] = "response"
	}
	if _, ok := responseData["created_at"]; !ok {
		responseData["created_at"] = time.Now().UTC().Unix()
	}
	if text(responseData["model"]) == "" {
		responseData["model"] = model
	}
	responseData["status"] = "completed"
	if _, ok := responseData["output"]; !ok && len(textParts) > 0 {
		responseData["output"] = buildResponsesOutputFromTextParts(textParts)
	}
	return responseData
}

func collectResponsesOutputItemDone(payload any, outputItemsByIndex map[int]map[string]any, outputItemsFallback *[]map[string]any) {
	mapping, ok := payload.(map[string]any)
	if !ok {
		return
	}
	item, ok := mapping["item"].(map[string]any)
	if !ok {
		return
	}
	itemCopy := deepCloneMap(item)
	if outputIndex, ok := coerceInt(mapping["output_index"]); ok {
		outputItemsByIndex[outputIndex] = itemCopy
		return
	}
	*outputItemsFallback = append(*outputItemsFallback, itemCopy)
}

func patchCompletedOutputFromOutputItems(payload any, outputItemsByIndex map[int]map[string]any, outputItemsFallback []map[string]any) any {
	mapping, ok := payload.(map[string]any)
	if !ok {
		return payload
	}
	responsePayload, ok := mapping["response"].(map[string]any)
	if !ok {
		return payload
	}
	outputItems, hasOutput := responsePayload["output"].([]any)
	if hasOutput && len(outputItems) > 0 {
		return payload
	}
	if len(outputItemsByIndex) == 0 && len(outputItemsFallback) == 0 {
		return payload
	}
	patched, ok := deepCloneAny(payload).(map[string]any)
	if !ok {
		return payload
	}
	patchedResponse, ok := patched["response"].(map[string]any)
	if !ok {
		return patched
	}
	indexes := make([]int, 0, len(outputItemsByIndex))
	for index := range outputItemsByIndex {
		indexes = append(indexes, index)
	}
	sortInts(indexes)
	patchedOutput := make([]any, 0, len(indexes)+len(outputItemsFallback))
	for _, index := range indexes {
		patchedOutput = append(patchedOutput, deepCloneMap(outputItemsByIndex[index]))
	}
	for _, item := range outputItemsFallback {
		patchedOutput = append(patchedOutput, deepCloneMap(item))
	}
	patchedResponse["output"] = patchedOutput
	return patched
}

func syntheticCompletedEvent(responseID, modelName string, createdAt int, outputItemsByIndex map[int]map[string]any, outputItemsFallback []map[string]any) map[string]any {
	if len(outputItemsByIndex) == 0 && len(outputItemsFallback) == 0 {
		return nil
	}
	responsePayload := map[string]any{"status": "completed"}
	if responseID != "" {
		responsePayload["id"] = responseID
	}
	if modelName != "" {
		responsePayload["model"] = modelName
	}
	if createdAt > 0 {
		responsePayload["created_at"] = createdAt
	}
	patched := patchCompletedOutputFromOutputItems(map[string]any{
		"type":     "response.completed",
		"response": responsePayload,
	}, outputItemsByIndex, outputItemsFallback)
	mapping, ok := patched.(map[string]any)
	if !ok {
		return nil
	}
	responseMap, ok := mapping["response"].(map[string]any)
	if !ok {
		return nil
	}
	output, ok := responseMap["output"].([]any)
	if !ok || len(output) == 0 {
		return nil
	}
	return mapping
}

func (p *Pipeline) collectResponsesJSONFromSSE(resp *http.Response, attempt Attempt) (map[string]any, *time.Time, error) {
	var responseSnapshot map[string]any
	outputTextParts := map[outputTextKey][]string{}
	outputItemsByIndex := map[int]map[string]any{}
	outputItemsFallback := []map[string]any{}
	var firstTokenAt *time.Time
	parser := sse.NewParser(int(p.cfg.Upstream.NonStreamMaxResponseBytes))
	err := parser.Parse(responseContext(resp), resp.Body, func(event sse.Event) error {
		if strings.TrimSpace(string(event.Data)) == "[DONE]" {
			return errStopSSE
		}
		payload := parseEventPayload(event)
		typ := eventType(event, payload)
		if status, message, failed := responseFailureStatus(payload); failed {
			return streamPreflightError{status: status, message: message}
		}
		if firstTokenAt == nil && typ != "" && !isPreflightResponseEvent(typ) {
			now := time.Now().UTC()
			firstTokenAt = &now
		}
		if payload == nil {
			return nil
		}
		if typ == "response.output_item.done" {
			collectResponsesOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
			return nil
		}
		if typ == "response.output_text.delta" {
			appendOutputTextDelta(outputTextParts, payload)
		}
		if responseObj, ok := payload["response"].(map[string]any); ok {
			if typ == "response.completed" {
				if patched, ok := patchCompletedOutputFromOutputItems(payload, outputItemsByIndex, outputItemsFallback).(map[string]any); ok {
					payload = patched
					responseObj, _ = payload["response"].(map[string]any)
				}
			}
			if responseSnapshot == nil {
				responseSnapshot = map[string]any{}
			}
			mergeMapping(responseSnapshot, responseObj)
		}
		if typ == "response.completed" {
			return errStopSSE
		}
		return nil
	})
	if err != nil && !errors.Is(err, errStopSSE) {
		return nil, firstTokenAt, err
	}
	if responseSnapshot == nil && len(outputTextParts) == 0 {
		return nil, firstTokenAt, errors.New("upstream closed stream without data")
	}
	if responseSnapshot != nil {
		if patched, ok := patchCompletedOutputFromOutputItems(map[string]any{
			"type":     "response.completed",
			"response": responseSnapshot,
		}, outputItemsByIndex, outputItemsFallback).(map[string]any); ok {
			if responseObj, ok := patched["response"].(map[string]any); ok {
				responseSnapshot = responseObj
			}
		}
	}
	return finalizeCollectedResponse(responseSnapshot, attempt.Intent.Model, outputTextParts), firstTokenAt, nil
}

func (p *Pipeline) writeResponsesJSONFromSSE(w http.ResponseWriter, resp *http.Response, attempt Attempt) (AttemptResult, error) {
	data, firstTokenAt, err := p.collectResponsesJSONFromSSE(resp, attempt)
	result := AttemptResult{Status: resp.StatusCode, FirstTokenAt: firstTokenAt}
	if err != nil {
		var preflight streamPreflightError
		if errors.As(err, &preflight) {
			result.Status = preflight.status
			result.Retry = retryableStatus(preflight.status)
			return result, errors.New(preflight.message)
		}
		result.Status = http.StatusBadGateway
		result.Retry = true
		return result, err
	}
	if attempt.Intent.ResponseModelAlias != "" {
		if patched, ok := patchResponseModelAlias(data, attempt.Intent.ResponseModelAlias).(map[string]any); ok {
			data = patched
		}
	}
	body, err := openai.EncodeJSON(data)
	if err != nil {
		result.Status = http.StatusBadGateway
		result.Retry = true
		return result, err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, writeErr := w.Write(body)
	usage, responseID := extractResponseMetrics(body, firstNonEmpty(attempt.Intent.ResponseModelAlias, attempt.Intent.Model))
	result.Committed = true
	result.Usage = usage
	result.ResponseID = responseID
	if writeErr != nil {
		return result, writeErr
	}
	return result, nil
}

func extractImagesFromCompletedResponse(payload map[string]any) ([]imageCallResult, int, map[string]any, error) {
	if payload == nil || text(payload["type"]) != "response.completed" {
		return nil, 0, nil, errors.New("unexpected image response event type")
	}
	responsePayload, ok := payload["response"].(map[string]any)
	if !ok {
		return nil, 0, nil, errors.New("image response.completed event is missing response payload")
	}
	createdAt, _ := coerceInt(responsePayload["created_at"])
	if createdAt <= 0 {
		createdAt = int(time.Now().UTC().Unix())
	}
	var results []imageCallResult
	if outputItems, ok := responsePayload["output"].([]any); ok {
		for _, item := range outputItems {
			mapping, ok := item.(map[string]any)
			if !ok || text(mapping["type"]) != "image_generation_call" {
				continue
			}
			resultB64 := text(mapping["result"])
			if resultB64 == "" {
				continue
			}
			results = append(results, imageCallResult{
				ResultB64:     resultB64,
				RevisedPrompt: text(mapping["revised_prompt"]),
				OutputFormat:  text(mapping["output_format"]),
				Size:          text(mapping["size"]),
				Background:    text(mapping["background"]),
				Quality:       text(mapping["quality"]),
			})
		}
	}
	if len(results) == 0 {
		return nil, 0, nil, errors.New("upstream did not return image output")
	}
	var usage map[string]any
	if toolUsage, ok := responsePayload["tool_usage"].(map[string]any); ok {
		if imageUsage, ok := toolUsage["image_gen"].(map[string]any); ok {
			usage = deepCloneMap(imageUsage)
		}
	}
	return results, createdAt, usage, nil
}

func buildImagesAPIResponse(results []imageCallResult, createdAt int, usage map[string]any, responseFormat string) map[string]any {
	data := make([]any, 0, len(results))
	normalizedFormat := normalizeImageResponseFormat(responseFormat)
	for _, item := range results {
		resultItem := map[string]any{}
		if normalizedFormat == "url" {
			resultItem["url"] = "data:" + outputMimeType(item.OutputFormat) + ";base64," + item.ResultB64
		} else {
			resultItem["b64_json"] = item.ResultB64
		}
		if item.RevisedPrompt != "" {
			resultItem["revised_prompt"] = item.RevisedPrompt
		}
		data = append(data, resultItem)
	}
	response := map[string]any{
		"created": createdAt,
		"data":    data,
	}
	first := results[0]
	if first.Background != "" {
		response["background"] = first.Background
	}
	if first.OutputFormat != "" {
		response["output_format"] = first.OutputFormat
	}
	if first.Quality != "" {
		response["quality"] = first.Quality
	}
	if first.Size != "" {
		response["size"] = first.Size
	}
	if usage != nil {
		response["usage"] = usage
	}
	return response
}

func trackResponseSnapshot(payload map[string]any, responseID *string, modelName *string, createdAt *int) {
	if payload == nil {
		return
	}
	responsePayload, ok := payload["response"].(map[string]any)
	if !ok {
		return
	}
	if next := text(responsePayload["id"]); next != "" {
		*responseID = next
	}
	if next := text(responsePayload["model"]); next != "" {
		*modelName = next
	}
	if next, ok := coerceInt(firstPresentCompat(responsePayload["created_at"], responsePayload["created"])); ok {
		*createdAt = next
	}
}

func (p *Pipeline) collectImageAPIResponseFromSSE(resp *http.Response, attempt Attempt) (map[string]any, *time.Time, error) {
	outputItemsByIndex := map[int]map[string]any{}
	outputItemsFallback := []map[string]any{}
	var firstTokenAt *time.Time
	var responseID, modelName string
	var createdAt int
	var imageResponse map[string]any
	parser := sse.NewParser(int(p.cfg.Upstream.NonStreamMaxResponseBytes))
	err := parser.Parse(responseContext(resp), resp.Body, func(event sse.Event) error {
		if strings.TrimSpace(string(event.Data)) == "[DONE]" {
			if synthetic := syntheticCompletedEvent(responseID, modelName, createdAt, outputItemsByIndex, outputItemsFallback); synthetic != nil {
				results, completedAt, usage, err := extractImagesFromCompletedResponse(synthetic)
				if err != nil {
					return err
				}
				imageResponse = buildImagesAPIResponse(results, completedAt, usage, attempt.Intent.ImageResponseFormat)
			}
			return errStopSSE
		}
		payload := parseEventPayload(event)
		typ := eventType(event, payload)
		if status, message, failed := responseFailureStatus(payload); failed {
			return streamPreflightError{status: status, message: message}
		}
		if firstTokenAt == nil && typ != "" && !isPreflightResponseEvent(typ) {
			now := time.Now().UTC()
			firstTokenAt = &now
		}
		trackResponseSnapshot(payload, &responseID, &modelName, &createdAt)
		if typ == "response.output_item.done" {
			collectResponsesOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
			return nil
		}
		if typ != "response.completed" {
			return nil
		}
		patched, _ := patchCompletedOutputFromOutputItems(payload, outputItemsByIndex, outputItemsFallback).(map[string]any)
		results, completedAt, usage, err := extractImagesFromCompletedResponse(patched)
		if err != nil {
			return err
		}
		imageResponse = buildImagesAPIResponse(results, completedAt, usage, attempt.Intent.ImageResponseFormat)
		return errStopSSE
	})
	if err != nil && !errors.Is(err, errStopSSE) {
		return nil, firstTokenAt, err
	}
	if imageResponse == nil {
		if synthetic := syntheticCompletedEvent(responseID, modelName, createdAt, outputItemsByIndex, outputItemsFallback); synthetic != nil {
			results, completedAt, usage, err := extractImagesFromCompletedResponse(synthetic)
			if err != nil {
				return nil, firstTokenAt, err
			}
			imageResponse = buildImagesAPIResponse(results, completedAt, usage, attempt.Intent.ImageResponseFormat)
		}
	}
	if imageResponse == nil {
		return nil, firstTokenAt, errors.New("upstream closed image stream before completion")
	}
	return imageResponse, firstTokenAt, nil
}

func (p *Pipeline) writeImageJSONResponse(w http.ResponseWriter, resp *http.Response, attempt Attempt) (AttemptResult, error) {
	result := AttemptResult{Status: resp.StatusCode}
	var response map[string]any
	var firstTokenAt *time.Time
	var err error
	if shouldCollectImageResponseAsSSE(resp.Header.Get("Content-Type"), attempt.Intent) {
		response, firstTokenAt, err = p.collectImageAPIResponseFromSSE(resp, attempt)
	} else {
		var payload map[string]any
		if decodeErr := json.NewDecoder(io.LimitReader(resp.Body, p.cfg.Upstream.NonStreamMaxResponseBytes)).Decode(&payload); decodeErr != nil {
			err = decodeErr
		} else if status, message, failed := responseFailureStatus(payload); failed {
			err = streamPreflightError{status: status, message: message}
		} else {
			if text(payload["type"]) != "response.completed" {
				payload = map[string]any{"type": "response.completed", "response": payload}
			}
			results, createdAt, usage, extractErr := extractImagesFromCompletedResponse(payload)
			if extractErr != nil {
				err = extractErr
			} else {
				response = buildImagesAPIResponse(results, createdAt, usage, attempt.Intent.ImageResponseFormat)
			}
		}
	}
	result.FirstTokenAt = firstTokenAt
	if err != nil {
		var preflight streamPreflightError
		if errors.As(err, &preflight) {
			result.Status = preflight.status
			result.Retry = retryableStatus(preflight.status)
			return result, errors.New(preflight.message)
		}
		result.Status = http.StatusBadGateway
		result.Retry = true
		return result, err
	}
	body, err := openai.EncodeJSON(response)
	if err != nil {
		result.Status = http.StatusBadGateway
		result.Retry = true
		return result, err
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, writeErr := w.Write(body)
	result.Committed = true
	if writeErr != nil {
		return result, writeErr
	}
	return result, nil
}

func shouldCollectImageResponseAsSSE(contentType string, intent RequestIntent) bool {
	normalized := strings.ToLower(strings.TrimSpace(contentType))
	if isSSE(normalized) {
		return true
	}
	if strings.Contains(normalized, "application/json") {
		return false
	}
	return intent.UpstreamAccept == "text/event-stream"
}

func transformImageStreamEvent(eventType string, payload map[string]any, responseFormat, streamPrefix string) ([][]byte, bool, error) {
	if payload == nil {
		return nil, false, nil
	}
	normalizedFormat := normalizeImageResponseFormat(responseFormat)
	if eventType == "response.image_generation_call.partial_image" {
		partialB64 := text(payload["partial_image_b64"])
		if partialB64 == "" {
			return nil, false, nil
		}
		partialIndex, _ := coerceInt(payload["partial_image_index"])
		outputFormat := text(payload["output_format"])
		eventName := streamPrefix + ".partial_image"
		responsePayload := map[string]any{
			"type":                eventName,
			"partial_image_index": partialIndex,
		}
		if normalizedFormat == "url" {
			responsePayload["url"] = "data:" + outputMimeType(outputFormat) + ";base64," + partialB64
		} else {
			responsePayload["b64_json"] = partialB64
		}
		data, _ := openai.EncodeJSON(responsePayload)
		return [][]byte{sse.Encode(eventName, data)}, false, nil
	}
	if eventType != "response.completed" {
		return nil, false, nil
	}
	results, _, usage, err := extractImagesFromCompletedResponse(payload)
	if err != nil {
		return nil, false, err
	}
	eventName := streamPrefix + ".completed"
	events := make([][]byte, 0, len(results))
	for _, item := range results {
		responsePayload := map[string]any{"type": eventName}
		if normalizedFormat == "url" {
			responsePayload["url"] = "data:" + outputMimeType(item.OutputFormat) + ";base64," + item.ResultB64
		} else {
			responsePayload["b64_json"] = item.ResultB64
		}
		if item.RevisedPrompt != "" {
			responsePayload["revised_prompt"] = item.RevisedPrompt
		}
		if usage != nil {
			responsePayload["usage"] = usage
		}
		data, _ := openai.EncodeJSON(responsePayload)
		events = append(events, sse.Encode(eventName, data))
	}
	return events, true, nil
}

func (p *Pipeline) streamImageResponse(w http.ResponseWriter, resp *http.Response, attempt Attempt) (AttemptResult, error) {
	outputItemsByIndex := map[int]map[string]any{}
	outputItemsFallback := []map[string]any{}
	var responseID, modelName string
	var createdAt int
	var firstTokenAt *time.Time
	committed := false
	done := false
	flusher, _ := w.(http.Flusher)
	writeEvents := func(events [][]byte) error {
		if len(events) == 0 {
			return nil
		}
		if !committed {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(resp.StatusCode)
			committed = true
		}
		for _, raw := range events {
			if _, err := w.Write(raw); err != nil {
				return err
			}
			if flusher != nil {
				flusher.Flush()
			}
		}
		return nil
	}
	parser := sse.NewParser(int(p.cfg.Upstream.NonStreamMaxResponseBytes))
	err := parser.Parse(responseContext(resp), resp.Body, func(event sse.Event) error {
		if strings.TrimSpace(string(event.Data)) == "[DONE]" {
			if synthetic := syntheticCompletedEvent(responseID, modelName, createdAt, outputItemsByIndex, outputItemsFallback); synthetic != nil {
				events, completed, err := transformImageStreamEvent("response.completed", synthetic, attempt.Intent.ImageResponseFormat, attempt.Intent.ImageStreamPrefix)
				if err != nil {
					return err
				}
				if err := writeEvents(events); err != nil {
					return err
				}
				done = completed
			}
			return errStopSSE
		}
		payload := parseEventPayload(event)
		typ := eventType(event, payload)
		if status, message, failed := responseFailureStatus(payload); failed {
			if !committed {
				return streamPreflightError{status: status, message: message}
			}
			data, _ := openai.EncodeJSON(map[string]any{"error": map[string]any{"message": message, "status": status}})
			if err := writeEvents([][]byte{sse.Encode("error", data)}); err != nil {
				return err
			}
			done = true
			return errStopSSE
		}
		trackResponseSnapshot(payload, &responseID, &modelName, &createdAt)
		if typ == "response.output_item.done" {
			collectResponsesOutputItemDone(payload, outputItemsByIndex, &outputItemsFallback)
			return nil
		}
		if typ == "response.completed" {
			if patched, ok := patchCompletedOutputFromOutputItems(payload, outputItemsByIndex, outputItemsFallback).(map[string]any); ok {
				payload = patched
			}
		}
		events, completed, err := transformImageStreamEvent(typ, payload, attempt.Intent.ImageResponseFormat, attempt.Intent.ImageStreamPrefix)
		if err != nil {
			return err
		}
		if len(events) > 0 && firstTokenAt == nil {
			now := time.Now().UTC()
			firstTokenAt = &now
		}
		if err := writeEvents(events); err != nil {
			return err
		}
		if completed {
			done = true
			return errStopSSE
		}
		return nil
	})
	result := AttemptResult{
		Status:       resp.StatusCode,
		Committed:    committed,
		FirstTokenAt: firstTokenAt,
	}
	if err != nil && !errors.Is(err, errStopSSE) {
		var preflight streamPreflightError
		if errors.As(err, &preflight) {
			result.Status = preflight.status
			result.Retry = retryableStatus(preflight.status)
			return result, errors.New(preflight.message)
		}
		if committed {
			return result, err
		}
		result.Status = http.StatusBadGateway
		result.Retry = true
		return result, err
	}
	if !done && !committed {
		result.Status = http.StatusBadGateway
		result.Retry = true
		return result, errors.New("upstream closed image stream before completion")
	}
	return result, nil
}

func (p *Pipeline) streamResponsesWithPreflight(w http.ResponseWriter, resp *http.Response, attempt Attempt) (AttemptResult, error) {
	observer := newUsageObserver(attempt.Intent.Model)
	flusher, _ := w.(http.Flusher)
	var buffered [][]byte
	committed := false
	streamState := attempt.StreamState
	firstTokenAt := (*time.Time)(nil)
	writeImmediateKeepalive := func() error {
		if streamState.KeepaliveSent {
			return nil
		}
		if !streamState.DownstreamStarted {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(resp.StatusCode)
			streamState.DownstreamStarted = true
		}
		raw := structuredKeepaliveRaw(0)
		observer.Observe(raw)
		if _, err := w.Write(raw); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		streamState.KeepaliveSent = true
		return nil
	}
	parser := sse.NewParser(int(p.cfg.Upstream.NonStreamMaxResponseBytes))
	err := parser.Parse(responseContext(resp), resp.Body, func(event sse.Event) error {
		payload := parseEventPayload(event)
		typ := eventType(event, payload)
		if status, message, failed := responseFailureStatus(payload); failed && !committed {
			return streamPreflightError{status: status, message: message}
		}
		if !committed && isPreflightResponseEvent(typ) {
			if typ == "response.created" {
				if err := writeImmediateKeepalive(); err != nil {
					return err
				}
			}
			buffered = append(buffered, rawEventForDownstream(event, attempt.Intent.ResponseModelAlias))
			return nil
		}
		if !committed {
			if !streamState.DownstreamStarted {
				w.WriteHeader(resp.StatusCode)
				streamState.DownstreamStarted = true
			}
			for _, raw := range buffered {
				observer.Observe(raw)
				if _, err := w.Write(raw); err != nil {
					return err
				}
			}
			buffered = nil
			committed = true
		}
		if firstTokenAt == nil && typ != "" && !isPreflightResponseEvent(typ) {
			now := time.Now().UTC()
			firstTokenAt = &now
		}
		raw := rawEventForDownstream(event, attempt.Intent.ResponseModelAlias)
		observer.Observe(raw)
		if _, err := w.Write(raw); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	})
	observer.flushEvent()
	result := AttemptResult{
		Status:       resp.StatusCode,
		Committed:    committed,
		StreamState:  streamState,
		Usage:        observer.usage,
		ResponseID:   observer.responseID,
		FirstTokenAt: firstNonNilTime(firstTokenAt, observer.firstTokenAt),
	}
	if err != nil {
		var preflight streamPreflightError
		if errors.As(err, &preflight) {
			result.Status = preflight.status
			result.Retry = retryableStatus(preflight.status)
			return result, errors.New(preflight.message)
		}
		if committed {
			return result, err
		}
		result.Status = http.StatusBadGateway
		result.Retry = true
		return result, err
	}
	if !committed {
		if !streamState.DownstreamStarted {
			w.WriteHeader(resp.StatusCode)
			streamState.DownstreamStarted = true
		}
		for _, raw := range buffered {
			observer.Observe(raw)
			if _, err := w.Write(raw); err != nil {
				result.Committed = true
				result.StreamState = streamState
				return result, err
			}
		}
		result.Committed = true
		result.StreamState = streamState
	}
	return result, nil
}

type streamPreflightError struct {
	status  int
	message string
}

func (e streamPreflightError) Error() string {
	return e.message
}

func firstNonNilTime(values ...*time.Time) *time.Time {
	for _, value := range values {
		if value != nil {
			return value
		}
	}
	return nil
}
