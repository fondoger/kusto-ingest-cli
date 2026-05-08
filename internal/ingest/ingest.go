package ingest

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingest"
	"github.com/schollz/progressbar/v3"

	"github.com/fondoger/kusto-ingest-cli/internal/convert"
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

	if err := uploadViaSDK(client, path, table, mappingName, sch, opts); err != nil {
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

// uploadViaSDK streams the file through a CSV-normalizing transform into the
// SDK's managed ingest client. The transform handles row-count mismatches
// (pads short rows, truncates long ones to len(sch.Columns)), strips NaN/Inf
// from real columns, and skips the header row server-side via Go's csv.Reader
// — so we don't pass IgnoreFirstRecord(). The SDK still picks streaming vs
// queued automatically based on payload size.
func uploadViaSDK(c *kusto.Client, path, table, mappingName string, sch *schema.Schema, opts Options) error {
	format := azkustoingest.CSV
	if strings.EqualFold(filepath.Ext(path), ".tsv") {
		format = azkustoingest.TSV
	}
	delim := schema.DelimiterFor(path)

	fi, err := os.Stat(path)
	if err != nil {
		return err
	}

	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "  → queued ingest via SDK, format=%s\n", format)
	}

	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Wrap the file in a counting reader for the progress bar.
	var reader io.Reader = f
	var bar *progressbar.ProgressBar
	if !opts.Quiet {
		bar = progressbar.NewOptions64(fi.Size(),
			progressbar.OptionSetDescription(fmt.Sprintf("  %s", baseName(path))),
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(20),
			progressbar.OptionThrottle(100*time.Millisecond),
			progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }),
		)
		reader = io.TeeReader(f, bar)
	}

	pr, pw := io.Pipe()
	go func() {
		err := transformCSV(reader, pw, sch, delim)
		_ = pw.CloseWithError(err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	t0 := time.Now()
	ingestor := c.Ingestor()
	result, err := ingestor.FromReader(ctx, pr,
		azkustoingest.Table(table),
		azkustoingest.IngestionMappingRef(mappingName, format),
		azkustoingest.ReportResultToTable(),
	)
	if bar != nil {
		_ = bar.Finish()
	}
	if err != nil {
		return fmt.Errorf("submit ingestion: %w", err)
	}

	if !opts.Quiet {
		fmt.Fprintf(os.Stderr, "  waiting for Kusto to complete ingestion...\n")
	}
	if waitErr := <-result.Wait(ctx); waitErr != nil {
		fmt.Fprintf(os.Stderr, "    SDK ingest FAIL:\n%s", formatIngestErr(waitErr))
		return fmt.Errorf("ingestion failed: %s", summarizeIngestErr(waitErr))
	}

	if opts.Verbose {
		fmt.Fprintf(os.Stderr, "    SDK ingest OK in %s\n", time.Since(t0).Round(10*time.Millisecond))
	}
	return nil
}

func baseName(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return p[i+1:]
		}
	}
	return p
}

// transformCSV reads CSV/TSV from src using Go's RFC 4180 reader, drops the
// header, normalizes each row to len(sch.Columns) (padding short rows with
// empty fields, truncating long ones), sanitizes NaN/Inf in real columns,
// and writes a clean CSV/TSV stream to dst.
//
// This insulates Kusto from common malformed-CSV errors:
//   - Stream_WrongNumberOfFields (inconsistent column counts)
//   - parse failures on NaN/±Inf for real columns
//   - rows with embedded commas/newlines that aren't quoted properly
func transformCSV(src io.Reader, dst io.Writer, sch *schema.Schema, delim rune) error {
	cr := csv.NewReader(schema.StripBOMReader(src))
	cr.Comma = delim
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.ReuseRecord = true

	// Skip header row.
	if _, err := cr.Read(); err != nil {
		return fmt.Errorf("read header: %w", err)
	}

	var row int64
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read row %d: %w", row+1, err)
		}
		line := convert.RowToCSV(sch.Columns, sch.Types, rec, delim)
		if _, err := dst.Write(line); err != nil {
			return err
		}
		if _, err := dst.Write([]byte{'\n'}); err != nil {
			return err
		}
		row++
	}
}

// formatIngestErr builds a human-readable multi-line block from the SDK's
// statusRecord error. The SDK's default Error() string uses kr/pretty which
// emits raw byte arrays for UUIDs and dumps every field; we pick the useful
// ones and skip the noise.
func formatIngestErr(err error) string {
	var b strings.Builder
	b.WriteString("  Ingestion failed:\n")
	if status, e := azkustoingest.GetIngestionStatus(err); e == nil {
		fmt.Fprintf(&b, "    Status:        %s\n", status)
	}
	if code, e := azkustoingest.GetErrorCode(err); e == nil && code != "" {
		fmt.Fprintf(&b, "    ErrorCode:     %s\n", code)
	}
	if fs, e := azkustoingest.GetIngestionFailureStatus(err); e == nil {
		fmt.Fprintf(&b, "    FailureStatus: %s\n", fs)
	}
	raw := err.Error()
	for _, field := range []string{"Database", "Table", "UpdatedOn", "Details"} {
		if v := extractPrettyField(raw, field); v != "" {
			fmt.Fprintf(&b, "    %-13s %s\n", field+":", v)
		}
	}
	return b.String()
}

// extractPrettyField pulls one field out of a kr/pretty struct dump. Returns
// "" if the field isn't present. Handles quoted string values (with the
// common escape sequences) and bare values; bails on multi-line values.
func extractPrettyField(s, name string) string {
	idx := strings.Index(s, name+":")
	if idx < 0 {
		return ""
	}
	rest := s[idx+len(name)+1:]
	end := strings.IndexAny(rest, "\r\n")
	if end < 0 {
		end = len(rest)
	}
	val := strings.TrimSpace(rest[:end])
	val = strings.TrimRight(val, ",")
	val = strings.TrimSpace(val)
	if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
		val = val[1 : len(val)-1]
		val = strings.ReplaceAll(val, `\"`, `"`)
		val = strings.ReplaceAll(val, `\\`, `\`)
	}
	return val
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
