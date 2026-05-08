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

// SubmitResult is returned by SubmitFile. If Err is set, upload failed (schema /
// table / mapping / network). If Err is nil, SDKResult is non-nil and the
// caller should call SDKResult.Wait to get the final ingestion outcome.
type SubmitResult struct {
	Table     string
	Rows      int64
	Bytes     int64
	Err       error
	SDKResult *azkustoingest.Result
}

type Options struct {
	Force     bool
	Append    bool
	InferRows int
	Quiet     bool
	Verbose   bool
}

// SubmitFile infers the schema, creates the table + mapping, and submits the
// file for ingestion. It does NOT wait for ingestion to complete — the caller
// must call SDKResult.Wait() on the returned result. This lets main.go submit
// all files first (fast), then wait for all in a second phase.
func SubmitFile(client *kusto.Client, path, table string, opts Options) SubmitResult {
	res := SubmitResult{Table: table}

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

	if err := ensureTable(client, table, sch, opts); err != nil {
		res.Err = err
		return res
	}
	mappingName := table + "_csv_mapping"
	if err := ensureMapping(client, table, mappingName, sch); err != nil {
		res.Err = err
		return res
	}

	sdkResult, err := submitViaSDK(client, path, table, mappingName, sch, opts)
	if err != nil {
		res.Err = err
		return res
	}
	res.SDKResult = sdkResult
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

// submitViaSDK streams the file through a CSV-normalizing transform and
// submits it to the SDK's queued ingest client. Returns the SDK result handle
// for async waiting. Does NOT block on ingestion completion.
func submitViaSDK(c *kusto.Client, path, table, mappingName string, sch *schema.Schema, opts Options) (*azkustoingest.Result, error) {
	format := azkustoingest.CSV
	if strings.EqualFold(filepath.Ext(path), ".tsv") {
		format = azkustoingest.TSV
	}
	delim := schema.DelimiterFor(path)

	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var reader io.Reader = f
	var bar *progressbar.ProgressBar
	if !opts.Quiet {
		bar = progressbar.NewOptions64(fi.Size(),
			progressbar.OptionSetDescription(fmt.Sprintf("  %s", baseName(path))),
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(20),
			progressbar.OptionThrottle(100*time.Millisecond),
			progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, " uploaded\n") }),
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

	result, err := c.Ingestor().FromReader(ctx, pr,
		azkustoingest.Table(table),
		azkustoingest.IngestionMappingRef(mappingName, format),
		azkustoingest.ReportResultToTable(),
	)
	if bar != nil {
		_ = bar.Finish()
	}
	if err != nil {
		return nil, fmt.Errorf("submit ingestion: %w", err)
	}
	return result, nil
}

func baseName(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return p[i+1:]
		}
	}
	return p
}

func transformCSV(src io.Reader, dst io.Writer, sch *schema.Schema, delim rune) error {
	cr := csv.NewReader(schema.StripBOMReader(src))
	cr.Comma = delim
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.ReuseRecord = true

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

// FormatIngestErr builds a human-readable multi-line block from an SDK
// ingestion error, picking only the useful fields.
func FormatIngestErr(err error) string {
	var b strings.Builder
	b.WriteString("Ingestion failed:\n")
	if status, e := azkustoingest.GetIngestionStatus(err); e == nil {
		fmt.Fprintf(&b, "  Status:        %s\n", status)
	}
	if code, e := azkustoingest.GetErrorCode(err); e == nil && code != "" {
		fmt.Fprintf(&b, "  ErrorCode:     %s\n", code)
	}
	if fs, e := azkustoingest.GetIngestionFailureStatus(err); e == nil {
		fmt.Fprintf(&b, "  FailureStatus: %s\n", fs)
	}
	raw := err.Error()
	for _, field := range []string{"Details"} {
		if v := extractPrettyField(raw, field); v != "" {
			fmt.Fprintf(&b, "  %-13s %s\n", field+":", v)
		}
	}
	return b.String()
}

// SummarizeIngestErr returns a one-line summary of an SDK ingestion error.
func SummarizeIngestErr(err error) string {
	parts := []string{}
	if code, e := azkustoingest.GetErrorCode(err); e == nil && code != "" {
		parts = append(parts, code)
	}
	if details := extractPrettyField(err.Error(), "Details"); details != "" {
		if len(details) > 120 {
			details = details[:120] + "..."
		}
		parts = append(parts, details)
	}
	if len(parts) > 0 {
		return strings.Join(parts, ": ")
	}
	s := err.Error()
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		return s[:i]
	}
	return s
}

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
