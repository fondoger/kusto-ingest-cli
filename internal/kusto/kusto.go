// Package kusto wraps the official azure-kusto-go SDK with the small surface
// the rest of this CLI needs. Auth is delegated to the Azure CLI via WithAzCli;
// users must `az login` before running.
package kusto

import (
	"context"
	"fmt"
	"strings"

	"github.com/Azure/azure-kusto-go/azkustodata"
	"github.com/Azure/azure-kusto-go/azkustodata/kql"
	"github.com/Azure/azure-kusto-go/azkustoingest"
)

type Client struct {
	data     *azkustodata.Client
	ingest   *azkustoingest.Ingestion
	cluster  string
	database string
}

// New constructs a data-plane client (for mgmt commands) and a queued-only
// ingestion client. We don't use the managed client because its
// streaming-first strategy fails permanently on wide schemas (we hit
// Stream_WrongNumberOfFields when Kusto's streaming CSV parser misinterprets
// the gzipped payload, and managed only falls back to queued on *transient*
// errors). Queued ingest is more tolerant of large/wide files and is what we
// want for bulk loads anyway.
func New(cluster, database string) (*Client, error) {
	cluster = normalizeURL(cluster)

	kcsbData := azkustodata.NewConnectionStringBuilder(cluster).WithAzCli()
	data, err := azkustodata.New(kcsbData)
	if err != nil {
		return nil, fmt.Errorf("kusto data client: %w", err)
	}

	kcsbIngest := azkustodata.NewConnectionStringBuilder(cluster).WithAzCli()
	ingest, err := azkustoingest.New(kcsbIngest,
		azkustoingest.WithDefaultDatabase(database))
	if err != nil {
		_ = data.Close()
		return nil, fmt.Errorf("kusto ingest client: %w", err)
	}

	return &Client{
		data:     data,
		ingest:   ingest,
		cluster:  cluster,
		database: database,
	}, nil
}

func (c *Client) Database() string { return c.database }

func (c *Client) Ingestor() *azkustoingest.Ingestion { return c.ingest }

// Mgmt runs an arbitrary control command. The SDK's kql.Builder requires
// compile-time string constants for safety; we use AddUnsafe because table /
// mapping names in our generated commands come from sanitized, internally-
// trusted sources (see internal/namesafe).
func (c *Client) Mgmt(csl string) error {
	stmt := kql.New("").AddUnsafe(csl)
	_, err := c.data.Mgmt(context.Background(), c.database, stmt)
	return err
}

func (c *Client) Close() error {
	c.ingest.Close()
	return c.data.Close()
}

func normalizeURL(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "https://") && !strings.HasPrefix(s, "http://") {
		s = "https://" + s
	}
	return strings.TrimRight(s, "/")
}
