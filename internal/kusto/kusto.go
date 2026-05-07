package kusto

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/fondoger/kusto-ingest-cli/internal/auth"
)

type Client struct {
	cluster  string
	database string
	tok      *auth.TokenProvider
	http     *http.Client
}

func New(cluster, database string, tok *auth.TokenProvider) *Client {
	cluster = strings.TrimSpace(cluster)
	if !strings.HasPrefix(cluster, "https://") && !strings.HasPrefix(cluster, "http://") {
		cluster = "https://" + cluster
	}
	return &Client{
		cluster:  strings.TrimRight(cluster, "/"),
		database: database,
		tok:      tok,
		http:     &http.Client{Timeout: 5 * time.Minute},
	}
}

func (c *Client) Database() string { return c.database }

func (c *Client) Mgmt(csl string) error {
	body, _ := json.Marshal(map[string]string{"db": c.database, "csl": csl})
	endpoint := c.cluster + "/v1/rest/mgmt"
	_, err := c.do("POST", endpoint, "application/json", body, true)
	return err
}

func (c *Client) StreamIngest(table, mappingName, format string, body []byte) error {
	q := url.Values{}
	q.Set("streamFormat", format)
	q.Set("mappingName", mappingName)
	endpoint := fmt.Sprintf("%s/v1/rest/ingest/%s/%s?%s",
		c.cluster, url.PathEscape(c.database), url.PathEscape(table), q.Encode())
	contentType := "text/csv"
	if format == "Tsv" {
		contentType = "text/tab-separated-values"
	}
	_, err := c.do("POST", endpoint, contentType, body, false)
	return err
}

func (c *Client) do(method, endpoint, contentType string, body []byte, allowJSONAccept bool) ([]byte, error) {
	var lastErr error
	delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	refreshed := false
	retried409 := 0
	for attempt := 0; attempt <= len(delays); attempt++ {
		tok, err := c.tok.Token()
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Content-Type", contentType)
		if allowJSONAccept {
			req.Header.Set("Accept", "application/json")
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			if attempt < len(delays) {
				time.Sleep(delays[attempt])
				continue
			}
			return nil, err
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return respBody, nil
		}
		if resp.StatusCode == 401 && !refreshed {
			refreshed = true
			if _, rerr := c.tok.Refresh(); rerr != nil {
				return nil, rerr
			}
			continue
		}
		if resp.StatusCode >= 500 && attempt < len(delays) {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 2000))
			time.Sleep(delays[attempt])
			continue
		}
		// 409 from streaming ingest typically means the table's streaming
		// buffer is congested; one retry after a short wait is usually enough
		// when paired with inter-batch throttling on the caller side.
		if resp.StatusCode == 409 && retried409 < 1 {
			retried409++
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 2000))
			time.Sleep(5 * time.Second)
			continue
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 2000))
	}
	return nil, lastErr
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
