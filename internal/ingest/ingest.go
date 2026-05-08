package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingest"

	"github.com/fondoger/kusto-ingest-cli/internal/kusto"
	"github.com/fondoger/kusto-ingest-cli/internal/schema"
)

type Result struct {
	Table    string
	Rows     int64
	Bytes    int64
	Duration time.Duration
	Err      error
}

type Options struct {
	Force     bool
	Append    bool
	InferRows int
	Quiet     bool
	Verbose   bool
	// Optional milestone callbacks for real-time progress reporting in quiet mode.
	OnSchemaReady func(rows int64, cols int)
	OnTableReady  func(table string)
}

func IngestFile(client *kusto.Client, path, table string, opts Options) Result {
	start := time.Now()
	res := Result{Table: table}

	fi, err := os.Stat(path)
	if err != nil {
		res.Err = err
		return res
	}
	res.Bytes = fi.Size()

	sch, err := schema.Infer(path, opts.InferRows)
	if err != nil {
		res.Err = fmt.Errorf("infer schema: %w", err)
		return res
	}
	res.Rows = sch.RowCount
	if opts.OnSchemaReady != nil {
		opts.OnSchemaReady(sch.RowCount, len(sch.Columns))
	}

	if err := ensureTable(client, table, sch, opts); err != nil {
		res.Err = err
		return res
	}
	mappingName := table + "_csv_mapping"
	if err := ensureMapping(client, table, mappingName, sch); err != nil {
		res.Err = err
		return res
	}
	if opts.OnTableReady != nil {
		opts.OnTableReady(table)
	}

	if err := uploadViaSDK(client, path, table, mappingName, opts); err != nil {
		res.Err = err
	}
	res.Duration = time.Since(start)
	return res
}

func ensureTable(c *kusto.Client, table string, sch *schema.Schema, opts Options) error {
	colDefs := make([]string, len(sch.Columns))
	for i, col := range sch.Columns {
		colDefs[i] = fmt.Sprintf("['%s']:%s", escapeIdent(col), sch.Types[i].String())
	}
	colList := strings.Join(colDefs, ", ")
	switch {
	case opts.Force:
		if err := c.Mgmt(fmt.Sprintf(".drop table ['%s'] ifexists", escapeIdent(table))); err != nil {
			return fmt.Errorf("drop table: %w", err)
		}
		if err := c.Mgmt(fmt.Sprintf(".create table ['%s'] (%s)", escapeIdent(table), colList)); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	case opts.Append:
		if err := c.Mgmt(fmt.Sprintf(".create-merge table ['%s'] (%s)", escapeIdent(table), colList)); err != nil {
			return fmt.Errorf("create-merge table: %w", err)
		}
	default:
		if err := c.Mgmt(fmt.Sprintf(".create table ['%s'] (%s)", escapeIdent(table), colList)); err != nil {
			return fmt.Errorf("create table (use --append to add to an existing table or --force to overwrite): %w", err)
		}
	}
	return nil
}

func ensureMapping(c *kusto.Client, table, mappingName string, sch *schema.Schema) error {
	type mapEntry struct {
		Column     string            `json:"column"`
		Properties map[string]string `json:"Properties"`
	}
	entries := make([]mapEntry, len(sch.Columns))
	for i, col := range sch.Columns {
		entries[i] = mapEntry{Column: col, Properties: map[string]string{"Ordinal": strconv.Itoa(i)}}
	}
	mappingJSON, _ := json.Marshal(entries)
	csl := fmt.Sprintf(".create-or-alter table ['%s'] ingestion csv mapping '%s' ```%s```",
		escapeIdent(table), escapeIdent(mappingName), string(mappingJSON))
	if err := c.Mgmt(csl); err != nil {
		return fmt.Errorf("create mapping: %w", err)
	}
	return nil
}

func escapeIdent(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// uploadViaSDK hands the file off to the SDK's managed ingest client. The SDK
// takes care of compression, blob upload (for the queued path), queue
// messaging, and automatic streaming-vs-queued selection based on payload
// size. We wait synchronously on the result so the per-file outcome is known
// before returning.
func uploadViaSDK(c *kusto.Client, path, table, mappingName string, opts Options) error {
	format := azkustoingest.CSV
	if strings.EqualFold(filepath.Ext(path), ".tsv") {
		format = azkustoingest.TSV
	}

	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "  → handing off to SDK (managed) format=%s\n", format)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t0 := time.Now()
	ingestor := c.Ingestor()
	result, err := ingestor.FromFile(ctx, path,
		azkustoingest.Table(table),
		azkustoingest.IngestionMappingRef(mappingName, format),
		azkustoingest.IgnoreFirstRecord(),
		azkustoingest.ReportResultToTable(),
	)
	if err != nil {
		return fmt.Errorf("submit ingestion: %w", err)
	}

	if waitErr := <-result.Wait(ctx); waitErr != nil {
		if opts.Verbose {
			fmt.Fprintf(os.Stderr, "    SDK ingest FAIL:\n%s\n", waitErr.Error())
		}
		return fmt.Errorf("ingestion failed: %s", summarizeIngestErr(waitErr))
	}

	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "    SDK ingest OK in %s\n", time.Since(t0).Round(10*time.Millisecond))
	}
	return nil
}

// summarizeIngestErr extracts the most useful single-line context out of an
// SDK ingestion error. The SDK's error string is multi-line (pretty-printed
// status record), and main.go strips at the first newline for non-verbose
// output, so without this we'd just see "Ingestion Failed".
func summarizeIngestErr(err error) string {
	parts := []string{}
	if status, e := azkustoingest.GetIngestionStatus(err); e == nil {
		parts = append(parts, fmt.Sprintf("status=%s", status))
	}
	if code, e := azkustoingest.GetErrorCode(err); e == nil && code != "" {
		parts = append(parts, fmt.Sprintf("errorCode=%s", code))
	}
	if fs, e := azkustoingest.GetIngestionFailureStatus(err); e == nil {
		parts = append(parts, fmt.Sprintf("failureStatus=%s", fs))
	}
	if len(parts) == 0 {
		// Not a status record — fall back to first line of the raw error.
		s := err.Error()
		if i := strings.IndexAny(s, "\r\n"); i >= 0 {
			return s[:i]
		}
		return s
	}
	return strings.Join(parts, " ")
}
