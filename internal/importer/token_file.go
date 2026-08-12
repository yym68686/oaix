package importer

import (
	"bufio"
	"encoding/json"
	"os"
	"strings"

	"github.com/yym68686/oaix/internal/importpayload"
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
				if token := importpayload.String(payload, "access_token", "accessToken", "token"); token != "" {
					tokens = append(tokens, token)
				}
			}
			continue
		}
		if !importpayload.IsRedactedCredential(line) {
			tokens = append(tokens, line)
		}
	}
	return tokens, scanner.Err()
}
