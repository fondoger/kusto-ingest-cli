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

// isTransientStreamingErr reports whether an error response is one of the
// known transient streaming-ingest failures (409 buffer congestion, or
// 400 EntityNotFound from streaming-ingest metadata cache lag right after
// table/mapping creation).
func isTransientStreamingErr(status int, body []byte) bool {
	if status == 409 {
		return true
	}
	if status == 400 && bytes.Contains(body, []byte("EntityNotFound")) {
		return true
	}
	return false
}

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
	_, _, err := c.do("POST", endpoint, "application/json", body, true)
	return err
}

// StreamIngest returns (retries, err) where retries is the number of HTTP
// retries that fired before the call returned (success or final failure).
func (c *Client) StreamIngest(table, mappingName, format string, body []byte) (int, error) {
	q := url.Values{}
	q.Set("streamFormat", format)
	q.Set("mappingName", mappingName)
	endpoint := fmt.Sprintf("%s/v1/rest/ingest/%s/%s?%s",
		c.cluster, url.PathEscape(c.database), url.PathEscape(table), q.Encode())
	contentType := "text/csv"
	if format == "Tsv" {
		contentType = "text/tab-separated-values"
	}
	_, retries, err := c.do("POST", endpoint, contentType, body, false)
	return retries, err
}

// do executes the HTTP request with retries. Returns (body, retries, err)
// where retries is the count of retry attempts that fired before the final
// outcome (0 if first attempt succeeded; up to delays+transientDelays).
func (c *Client) do(method, endpoint, contentType string, body []byte, allowJSONAccept bool) ([]byte, int, error) {
	var lastErr error
	delays := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}
	transientDelays := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second}
	refreshed := false
	retriedTransient := 0
	retries := 0
	for attempt := 0; attempt <= len(delays); attempt++ {
		tok, err := c.tok.Token()
		if err != nil {
			return nil, retries, err
		}
		req, err := http.NewRequest(method, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, retries, err
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
				retries++
				continue
			}
			return nil, retries, err
		}
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return respBody, retries, nil
		}
		if resp.StatusCode == 401 && !refreshed {
			refreshed = true
			if _, rerr := c.tok.Refresh(); rerr != nil {
				return nil, retries, rerr
			}
			retries++
			continue
		}
		if resp.StatusCode >= 500 && attempt < len(delays) {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 2000))
			time.Sleep(delays[attempt])
			retries++
			continue
		}
		// Transient streaming-ingest failures: 409 (buffer congestion) and
		// 400 EntityNotFound (metadata cache lag right after table/mapping
		// creation). Retry up to len(transientDelays) times with longer waits
		// than the 5xx backoff so the streaming-ingest service has time to
		// drain its queue or refresh its cache.
		if isTransientStreamingErr(resp.StatusCode, respBody) && retriedTransient < len(transientDelays) {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 2000))
			time.Sleep(transientDelays[retriedTransient])
			retriedTransient++
			retries++
			continue
		}
		return nil, retries, fmt.Errorf("HTTP %d: %s", resp.StatusCode, truncate(string(respBody), 2000))
	}
	return nil, retries, lastErr
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
