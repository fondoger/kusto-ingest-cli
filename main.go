package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/Azure/azure-kusto-go/azkustoingest"
	"github.com/spf13/cobra"

	"github.com/fondoger/kusto-ingest-cli/internal/ingest"
	"github.com/fondoger/kusto-ingest-cli/internal/kusto"
	"github.com/fondoger/kusto-ingest-cli/internal/namesafe"
)

var (
	flagCluster     string
	flagDatabase    string
	flagTable       string
	flagRecursive   bool
	flagForce       bool
	flagAppend      bool
	flagInferRows   int
	flagTablePrefix string
	flagVerbose     bool
	flagQuiet       bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:          "kusto-ingest-cli <path>",
		Short:        "Ingest CSV/TSV files into Azure Data Explorer (Kusto)",
		Args:         cobra.ExactArgs(1),
		RunE:         run,
		SilenceUsage: true,
	}
	f := rootCmd.Flags()
	f.StringVar(&flagCluster, "cluster", "", "Kusto cluster URL (e.g. https://mycluster.kusto.windows.net). Falls back to $KUSTO_INGEST_CLUSTER.")
	f.StringVar(&flagDatabase, "database", "", "Target database. Falls back to $KUSTO_INGEST_DATABASE.")
	f.StringVar(&flagTable, "table", "", "Target table name (single-file mode only). Defaults to sanitized file basename.")
	f.BoolVarP(&flagRecursive, "recursive", "r", false, "Recurse into subdirectories (directory mode)")
	f.BoolVarP(&flagForce, "force", "f", false, "Drop and recreate the target table (data lost)")
	f.BoolVar(&flagAppend, "append", false, "Append to existing table (creates the table if missing). Without this flag, an existing table causes an error.")
	f.IntVar(&flagInferRows, "infer-rows", 10000, "Max rows sampled for type inference (evenly distributed)")
	f.StringVar(&flagTablePrefix, "table-prefix", "", "Prefix prepended to auto-derived table names (e.g. \"raw_\"). When set, digit-leading filenames don't get an extra underscore.")
	f.BoolVarP(&flagVerbose, "verbose", "v", false, "Log detailed ingestion info")
	f.BoolVar(&flagQuiet, "quiet", false, "Force non-interactive output (no progress bar, compact lines)")
	if err := rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
}

// submitted tracks one file that was successfully submitted for ingestion.
type submitted struct {
	index     int
	dn        string // display name
	table     string
	rows      int64
	bytes     int64
	sdkResult *azkustoingest.Result
	start     time.Time
}

func run(cmd *cobra.Command, args []string) error {
	clusterFromEnv, dbFromEnv := false, false
	if flagCluster == "" {
		if v := os.Getenv("KUSTO_INGEST_CLUSTER"); v != "" {
			flagCluster = v
			clusterFromEnv = true
		}
	}
	if flagDatabase == "" {
		if v := os.Getenv("KUSTO_INGEST_DATABASE"); v != "" {
			flagDatabase = v
			dbFromEnv = true
		}
	}
	if flagCluster == "" {
		return fmt.Errorf("--cluster is required (or set KUSTO_INGEST_CLUSTER)")
	}
	if flagDatabase == "" {
		return fmt.Errorf("--database is required (or set KUSTO_INGEST_DATABASE)")
	}
	if clusterFromEnv {
		fmt.Printf("Using KUSTO_INGEST_CLUSTER=%s\n", flagCluster)
	}
	if dbFromEnv {
		fmt.Printf("Using KUSTO_INGEST_DATABASE=%s\n", flagDatabase)
	}

	path := args[0]
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("path: %w", err)
	}
	files, err := collectFiles(path, info, flagRecursive)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no .csv/.tsv files found at %s", path)
	}
	if info.IsDir() && flagTable != "" {
		return fmt.Errorf("--table is not allowed when <path> is a directory")
	}
	if flagAppend && flagForce {
		return fmt.Errorf("--append and --force are mutually exclusive")
	}
	if flagTablePrefix != "" {
		if flagTable != "" {
			return fmt.Errorf("--table-prefix cannot be combined with --table")
		}
		if !namesafe.ValidIdentifier(flagTablePrefix) {
			return fmt.Errorf("--table-prefix %q is not a valid Kusto identifier", flagTablePrefix)
		}
	}

	client, err := kusto.New(flagCluster, flagDatabase)
	if err != nil {
		return err
	}
	defer client.Close()

	interactive := isInteractive() && !flagQuiet
	displayName := makeDisplayName(path, info.IsDir())
	opts := ingest.Options{
		Force:     flagForce,
		Append:    flagAppend,
		InferRows: flagInferRows,
		Quiet:     !interactive,
		Verbose:   flagVerbose,
	}

	// ── Phase 1: submit all files ────────────────────────────────────────
	var uploads []submitted
	uploadErrors := 0

	for i, fp := range files {
		table := flagTable
		if table == "" {
			table = defaultTableName(fp)
		}
		dn := displayName(fp)
		fi, _ := os.Stat(fp)
		sizeStr := ""
		if fi != nil {
			sizeStr = humanBytes(fi.Size())
		}

		if interactive {
			fmt.Fprintf(os.Stderr, "[%d/%d] uploading %s (%s) -> %s\n",
				i+1, len(files), dn, sizeStr, table)
		} else {
			// Quiet: partial line — will be completed with "uploaded" or "FAIL"
			fmt.Fprintf(os.Stderr, "[%d/%d] %s %s -> %s ",
				i+1, len(files), dn, sizeStr, table)
		}

		start := time.Now()
		res := ingest.SubmitFile(client, fp, table, opts)

		if res.Err != nil {
			uploadErrors++
			if interactive {
				fmt.Fprintf(os.Stderr, "  FAIL %s: %s\n", dn, firstLine(res.Err.Error()))
			} else {
				fmt.Fprintf(os.Stderr, "FAIL %s\n", firstLine(res.Err.Error()))
			}
			continue
		}

		if interactive {
			// Progress bar already printed " uploaded\n" via OnCompletion.
		} else {
			fmt.Fprintf(os.Stderr, "uploaded\n")
		}

		uploads = append(uploads, submitted{
			index:     i,
			dn:        dn,
			table:     table,
			rows:      res.Rows,
			bytes:     res.Bytes,
			sdkResult: res.SDKResult,
			start:     start,
		})
	}

	if len(uploads) == 0 {
		fmt.Fprintf(os.Stderr, "\nNo files uploaded successfully.\n")
		os.Exit(1)
	}

	// ── Phase 2: wait for all ingestions to complete ──────────────────────
	fmt.Fprintf(os.Stderr, "\nWaiting for ingestion to complete. "+
		"(Ctrl+C to exit — ingestion continues server-side)\n")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	type doneMsg struct {
		idx int
		err error
	}
	doneCh := make(chan doneMsg, len(uploads))
	for i, u := range uploads {
		i, u := i, u
		go func() {
			waitErr := <-u.sdkResult.Wait(ctx)
			doneCh <- doneMsg{i, waitErr}
		}()
	}

	successes, failures := 0, 0
	type failInfo struct {
		dn  string
		err error
	}
	var failList []failInfo
	idFmt := fmt.Sprintf("#%%0%dd", len(fmt.Sprint(len(files))))

	for range uploads {
		msg := <-doneCh
		u := uploads[msg.idx]
		dur := time.Since(u.start).Round(100 * time.Millisecond)
		id := fmt.Sprintf(idFmt, u.index+1)

		if msg.err != nil {
			failures++
			if interactive {
				fmt.Fprintf(os.Stderr, "  FAIL [%s] %s -> %s\n", id, u.dn, u.table)
				fmt.Fprintf(os.Stderr, "    %s", ingest.FormatIngestErr(msg.err))
			} else {
				fmt.Fprintf(os.Stderr, "  [%s] FAIL %s\n", id, ingest.SummarizeIngestErr(msg.err))
			}
			failList = append(failList, failInfo{u.dn, msg.err})
		} else {
			successes++
			if interactive {
				fmt.Fprintf(os.Stderr, "  OK   [%s] %s -> %s  rows=%d  ingested in %s\n",
					id, u.dn, u.table, u.rows, dur)
			} else {
				fmt.Fprintf(os.Stderr, "  [%s] OK %s\n", id, dur)
			}
		}
	}

	// ── Summary ──────────────────────────────────────────────────────────
	totalFailed := failures + uploadErrors
	fmt.Fprintf(os.Stderr, "\nDone. %d succeeded, %d failed.\n", successes, totalFailed)
	if totalFailed > 0 {
		os.Exit(1)
	}
	return nil
}

func makeDisplayName(inputPath string, isDir bool) func(string) string {
	if !isDir {
		return func(fp string) string { return filepath.Base(fp) }
	}
	return func(fp string) string {
		if rel, err := filepath.Rel(inputPath, fp); err == nil {
			return filepath.ToSlash(rel)
		}
		return filepath.Base(fp)
	}
}

func isInteractive() bool {
	fi, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return (fi.Mode() & os.ModeCharDevice) != 0
}

func collectFiles(path string, info fs.FileInfo, recursive bool) ([]string, error) {
	if !info.IsDir() {
		if !isCSVorTSV(path) {
			return nil, fmt.Errorf("file must have .csv or .tsv extension: %s", path)
		}
		return []string{path}, nil
	}
	var out []string
	if recursive {
		err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}
			if isCSVorTSV(p) {
				out = append(out, p)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			full := filepath.Join(path, e.Name())
			if isCSVorTSV(full) {
				out = append(out, full)
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

func isCSVorTSV(p string) bool {
	ext := strings.ToLower(filepath.Ext(p))
	return ext == ".csv" || ext == ".tsv"
}

func defaultTableName(p string) string {
	base := filepath.Base(p)
	if i := strings.LastIndex(base, "."); i > 0 {
		base = base[:i]
	}
	if flagTablePrefix != "" {
		return flagTablePrefix + namesafe.SanitizeChars(base)
	}
	return namesafe.Sanitize(base)
}

func firstLine(s string) string {
	if i := strings.IndexAny(s, "\r\n"); i >= 0 {
		return s[:i]
	}
	return s
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGTPE"[exp])
}
