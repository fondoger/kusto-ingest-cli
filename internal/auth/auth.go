package auth

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"
)

const kustoResource = "https://api.kusto.windows.net"

type TokenProvider struct {
	resource string
	mu       sync.Mutex
	token    string
}

func New(clusterURL string) *TokenProvider {
	return &TokenProvider{resource: kustoResource}
}

func (t *TokenProvider) Token() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.token != "" {
		return t.token, nil
	}
	return t.fetchLocked()
}

func (t *TokenProvider) Refresh() (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token = ""
	return t.fetchLocked()
}

func (t *TokenProvider) fetchLocked() (string, error) {
	cmd := exec.Command("az", "account", "get-access-token",
		"--resource", t.resource,
		"--query", "accessToken",
		"-o", "tsv")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("failed to get token from Azure CLI. Please run 'az login' first: %w", err)
	}
	tok := strings.TrimSpace(string(out))
	if tok == "" {
		return "", fmt.Errorf("empty token from Azure CLI. Please run 'az login' first")
	}
	t.token = tok
	return tok, nil
}
