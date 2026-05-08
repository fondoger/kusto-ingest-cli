package ingest_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fondoger/kusto-ingest-cli/internal/ingest"
	"github.com/fondoger/kusto-ingest-cli/internal/kusto"
)

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

	client, err := kusto.New(cluster, db)
	if err != nil {
		t.Fatalf("kusto client: %v", err)
	}
	defer client.Close()

	res := ingest.SubmitFile(client, csvPath, "ingest_test_table", ingest.Options{
		Force:     true,
		InferRows: 1000,
	})
	if res.Err != nil {
		t.Fatalf("submit failed: %v", res.Err)
	}
	if res.Rows != 3 {
		t.Errorf("rows = %d, want 3", res.Rows)
	}
	if res.SDKResult != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if waitErr := <-res.SDKResult.Wait(ctx); waitErr != nil {
			t.Fatalf("ingestion failed: %v", waitErr)
		}
	}

	t.Cleanup(func() {
		_ = client.Mgmt(".drop table ingest_test_table ifexists")
	})
}
