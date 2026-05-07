package ingest

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/schollz/progressbar/v3"

	"github.com/fondoger/kusto-ingest-cli/internal/convert"
	"github.com/fondoger/kusto-ingest-cli/internal/kusto"
	"github.com/fondoger/kusto-ingest-cli/internal/schema"
)

const (
	maxBatchBytes = 2 * 1024 * 1024 // 2 MiB — small batches keep the streaming-ingest commit queue from filling up
	// Min 5s/batch with a hard 2s breather between batches keeps us well
	// under Kusto's 4 GB/h-per-table streaming-ingest guidance (2 MiB / 5s
	// ≈ 1.4 GB/h) so the buffer has time to drain.
	minBatchCycle = 5 * time.Second
	minBatchSleep = 2 * time.Second
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
	Quiet     bool // suppress progress bar (non-interactive environments)
	Verbose   bool // log every batch upload (size, result, duration)
	// Optional milestone callbacks for real-time progress reporting in quiet mode.
	OnSchemaReady func(rows int64, cols int)
	OnTableReady  func(table string)
}

func IngestFile(client *kusto.Client, path, table string, opts Options) Result {
	start := time.Now()
	res := Result{Table: table}

	sch, err := schema.Infer(path, opts.InferRows)
	if err != nil {
		res.Err = fmt.Errorf("infer schema: %w", err)
		return res
	}
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

	rows, bytes, err := streamUpload(client, path, table, mappingName, sch, opts)
	res.Rows = rows
	res.Bytes = bytes
	res.Duration = time.Since(start)
	if err != nil {
		res.Err = err
	}
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

const policyNotEnabledMarker = "BadRequest_StreamingIngestionPolicyNotEnabled"

func isPolicyNotEnabledErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), policyNotEnabledMarker)
}

// ingestBatch sends a batch via streaming ingest. On policy-not-enabled errors
// it enables the policy at the database level (and additionally at the table
// level in append mode if database-level didn't fix it), waits 10 seconds, and
// retries. Each policy-alter is attempted at most once per file via the shared
// state struct.
type policyState struct {
	dbTried, tblTried bool
}

func ingestBatch(c *kusto.Client, table, mappingName, format string, body []byte, append bool, st *policyState) (int, error) {
	totalRetries := 0
	r, err := c.StreamIngest(table, mappingName, format, body)
	totalRetries += r
	if err == nil || !isPolicyNotEnabledErr(err) {
		return totalRetries, err
	}
	if !st.dbTried {
		st.dbTried = true
		fmt.Fprintf(os.Stderr, "  streaming ingestion policy not enabled — enabling at database level...\n")
		if aerr := c.Mgmt(fmt.Sprintf(".alter database ['%s'] policy streamingingestion enable", escapeIdent(c.Database()))); aerr != nil {
			return totalRetries, fmt.Errorf("enable database streaming policy: %w (original: %v)", aerr, err)
		}
		time.Sleep(10 * time.Second)
		r, err = c.StreamIngest(table, mappingName, format, body)
		totalRetries += r
		if err == nil || !isPolicyNotEnabledErr(err) {
			return totalRetries, err
		}
	}
	if append && !st.tblTried {
		st.tblTried = true
		fmt.Fprintf(os.Stderr, "  database-level policy still failing — enabling at table level...\n")
		if aerr := c.Mgmt(fmt.Sprintf(".alter table ['%s'] policy streamingingestion enable", escapeIdent(table))); aerr != nil {
			return totalRetries, fmt.Errorf("enable table streaming policy: %w (original: %v)", aerr, err)
		}
		time.Sleep(10 * time.Second)
		r, err = c.StreamIngest(table, mappingName, format, body)
		totalRetries += r
	}
	return totalRetries, err
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
	// CSV mapping covers both Csv and Tsv streamFormat (column-by-ordinal).
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

func streamUpload(c *kusto.Client, path, table, mappingName string, sch *schema.Schema, opts Options) (int64, int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	totalSize := fi.Size()

	var bar *progressbar.ProgressBar
	if !opts.Quiet {
		bar = progressbar.NewOptions64(totalSize,
			progressbar.OptionSetDescription(fmt.Sprintf("  %s", baseName(path))),
			progressbar.OptionShowBytes(true),
			progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(20),
			progressbar.OptionThrottle(100*time.Millisecond),
			progressbar.OptionOnCompletion(func() { fmt.Fprint(os.Stderr, "\n") }),
		)
	}

	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	delim := schema.DelimiterFor(path)
	cr := csv.NewReader(stripBOM(f))
	cr.Comma = delim
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true

	// Skip header
	if _, err := cr.Read(); err != nil {
		return 0, 0, fmt.Errorf("read header: %w", err)
	}

	format := "Csv"
	if delim == '\t' {
		format = "Tsv"
	}

	var rowCount, byteCount int64
	var lastOffset int64

	policySt := &policyState{}
	batchIdx := 0
	batcher := NewBatcher(maxBatchBytes, 0, func(batch []byte) error {
		batchIdx++
		if opts.Verbose {
			fmt.Fprintf(os.Stderr, "  → batch #%d table=%s size=%d bytes\n", batchIdx, table, len(batch))
		}
		t0 := time.Now()
		retries, err := ingestBatch(c, table, mappingName, format, batch, opts.Append, policySt)
		if opts.Verbose {
			retrySuffix := ""
			if retries > 0 {
				retrySuffix = fmt.Sprintf(" (retry=%d)", retries)
			}
			dur := time.Since(t0).Round(10 * time.Millisecond)
			if err == nil {
				fmt.Fprintf(os.Stderr, "    batch #%d OK in %s%s\n", batchIdx, dur, retrySuffix)
			} else {
				// Print the full error body in verbose mode (multi-line OK).
				fmt.Fprintf(os.Stderr, "    batch #%d FAIL in %s%s:\n%s\n", batchIdx, dur, retrySuffix, err.Error())
			}
		}
		// Throttle between batches: target a minimum 4-second batch cycle
		// (~3.6 GB/h per table, under Kusto's 4 GB/h streaming-ingest guidance)
		// with at least 1 second of breathing room after every batch.
		if err == nil {
			sleep := minBatchCycle - time.Since(t0)
			if sleep < minBatchSleep {
				sleep = minBatchSleep
			}
			time.Sleep(sleep)
		}
		return err
	})

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return rowCount, byteCount, fmt.Errorf("read row %d: %w", rowCount+1, err)
		}
		line := convert.RowToCSV(sch.Columns, sch.Types, rec, delim)
		if err := batcher.Add(line); err != nil {
			return rowCount, byteCount, err
		}
		rowCount++

		if off, err := f.Seek(0, io.SeekCurrent); err == nil {
			delta := off - lastOffset
			if delta > 0 {
				if bar != nil {
					_ = bar.Add64(delta)
				}
				lastOffset = off
			}
		}
	}
	if err := batcher.Flush(); err != nil {
		return rowCount, byteCount, err
	}
	byteCount = lastOffset
	if bar != nil {
		_ = bar.Finish()
	}
	return rowCount, byteCount, nil
}

func baseName(p string) string {
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' || p[i] == '\\' {
			return p[i+1:]
		}
	}
	return p
}

type bomStripper struct {
	r       io.Reader
	checked bool
}

func (b *bomStripper) Read(p []byte) (int, error) {
	if !b.checked {
		b.checked = true
		var pre [3]byte
		n, _ := io.ReadFull(b.r, pre[:])
		if n == 3 && pre[0] == 0xEF && pre[1] == 0xBB && pre[2] == 0xBF {
			// strip
		} else if n > 0 {
			b.r = io.MultiReader(strings.NewReader(string(pre[:n])), b.r)
		}
	}
	return b.r.Read(p)
}

func stripBOM(r io.Reader) io.Reader { return &bomStripper{r: r} }
