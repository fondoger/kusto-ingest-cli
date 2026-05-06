package ingest_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fondoger/kusto-ingest-cli/internal/auth"
	"github.com/fondoger/kusto-ingest-cli/internal/ingest"
	"github.com/fondoger/kusto-ingest-cli/internal/kusto"
)

// Integration test: opt-in via env vars.
// Set KUSTO_TEST_CLUSTER and KUSTO_TEST_DB to enable. Requires `az login` first.
func TestIngestEndToEnd(t *testing.T) {
	cluster := os.Getenv("KUSTO_TEST_CLUSTER")
	db := os.Getenv("KUSTO_TEST_DB")
	if cluster == "" || db == "" {
		t.Skip("set KUSTO_TEST_CLUSTER and KUSTO_TEST_DB to run integration test")
	}

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "ingest_test_table.csv")
	content := "id,name,price,active\n" +
		"1,foo,1.5,true\n" +
		"2,bar,2.25,false\n" +
		"3,baz,,true\n"
	if err := os.WriteFile(csvPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	tok := auth.New(cluster)
	if _, err := tok.Token(); err != nil {
		t.Fatalf("token: %v", err)
	}
	client := kusto.New(cluster, db, tok)

	res := ingest.IngestFile(client, csvPath, "ingest_test_table", ingest.Options{
		Force:     true,
		InferRows: 1000,
	})
	if res.Err != nil {
		t.Fatalf("ingest failed: %v", res.Err)
	}
	if res.Rows != 3 {
		t.Errorf("rows = %d, want 3", res.Rows)
	}

	t.Cleanup(func() {
		_ = client.Mgmt(".drop table ingest_test_table ifexists")
	})
}
