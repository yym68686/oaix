package modelaccess

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

type Model struct {
	ID    string `json:"id"`
	Label string `json:"label"`
}

var catalog = []Model{
	{ID: "gpt-5.4-mini", Label: "GPT-5.4 Mini"},
	{ID: "gpt-5.4", Label: "GPT-5.4"},
	{ID: "gpt-5.5", Label: "GPT-5.5"},
	{ID: "gpt-5.6-sol", Label: "GPT-5.6 Sol"},
	{ID: "gpt-5.6-terra", Label: "GPT-5.6 Terra"},
	{ID: "gpt-5.6-luna", Label: "GPT-5.6 Luna"},
	{ID: "gpt-image-2", Label: "GPT Image 2"},
}

var (
	modelIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
	nonFreeOnly    = []string{"gpt-image-2", "gpt-5.4", "gpt-5.3-codex", "gpt-5.2"}
)

func Models() []Model {
	return append([]Model(nil), catalog...)
}

func ModelIDs() []string {
	ids := make([]string, 0, len(catalog))
	for _, model := range catalog {
		ids = append(ids, model.ID)
	}
	return ids
}

func TextModelIDs() []string {
	ids := make([]string, 0, len(catalog))
	for _, model := range catalog {
		if model.ID != "gpt-image-2" {
			ids = append(ids, model.ID)
		}
	}
	return ids
}

func NormalizeModel(value string) (string, error) {
	model := strings.ToLower(strings.TrimSpace(value))
	if !modelIDPattern.MatchString(model) {
		return "", fmt.Errorf("invalid model %q", value)
	}
	return model, nil
}

func NormalizeModels(values []string) ([]string, error) {
	models := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		model, err := NormalizeModel(value)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[model]; exists {
			continue
		}
		seen[model] = struct{}{}
		models = append(models, model)
	}
	sort.Strings(models)
	return models, nil
}

func Matches(models []string, requested string) bool {
	requested = strings.ToLower(strings.TrimSpace(requested))
	for _, configured := range models {
		model := strings.ToLower(strings.TrimSpace(configured))
		if requested == model || strings.HasPrefix(requested, model+"-") {
			return true
		}
	}
	return false
}

func DefaultAllows(plan, requested string) bool {
	if canonicalPlan(plan) != "free" {
		return true
	}
	return !Matches(nonFreeOnly, requested)
}

func DefaultModels(plan string, available []string) []string {
	models := make([]string, 0, len(available))
	for _, model := range available {
		if DefaultAllows(plan, model) {
			models = append(models, model)
		}
	}
	normalized, _ := NormalizeModels(models)
	return normalized
}

func canonicalPlan(value string) string {
	plan := strings.ToLower(strings.TrimSpace(value))
	plan = strings.TrimPrefix(plan, "chatgpt_")
	if plan == "" {
		return "unknown"
	}
	return plan
}
