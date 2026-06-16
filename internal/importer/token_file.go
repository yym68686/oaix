package importer

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"
)

func ReadAccessTokenFile(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var tokens []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "{") {
			var payload map[string]any
			if err := json.Unmarshal([]byte(line), &payload); err == nil {
				for _, key := range []string{"access_token", "accessToken", "token"} {
					if value, ok := payload[key].(string); ok && strings.TrimSpace(value) != "" {
						tokens = append(tokens, strings.TrimSpace(value))
						break
					}
				}
			}
			continue
		}
		tokens = append(tokens, line)
	}
	return tokens, scanner.Err()
}
