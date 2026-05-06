package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/fondoger/kusto-ingest-cli/internal/auth"
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
)

func main() {
	rootCmd := &cobra.Command{
		Use:          "kusto-ingest-cli <path>",
		Short:        "Ingest CSV/TSV files into Azure Data Explorer (Kusto) via Streaming Ingest",
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
	if err := rootCmd.Execute(); err != nil {
		os.Exit(2)
	}
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
			return fmt.Errorf("--table-prefix %q is not a valid Kusto identifier (allowed: letters/digits/underscore, must not start with a digit)", flagTablePrefix)
		}
	}

	tok := auth.New(flagCluster)
	if _, err := tok.Token(); err != nil {
		return err
	}
	client := kusto.New(flagCluster, flagDatabase, tok)

	interactive := isInteractive()
	if !interactive {
		fmt.Fprintln(os.Stderr, "Non-interactive environment detected — using simplified output (no progress bar).")
	}

	displayName := makeDisplayName(path, info.IsDir())

	successes, failed := 0, 0
	type failure struct {
		path string
		err  error
	}
	var failures []failure

	for i, fp := range files {
		table := flagTable
		if table == "" {
			table = defaultTableName(fp)
		}
		dn := displayName(fp)
		if interactive {
			fmt.Fprintf(os.Stderr, "[%d/%d] uploading %s -> %s\n", i+1, len(files), dn, table)
		} else {
			fmt.Fprintf(os.Stderr, "[%d/%d] %s\n", i+1, len(files), dn)
		}

		opts := ingest.Options{
			Force:     flagForce,
			Append:    flagAppend,
			InferRows: flagInferRows,
			Quiet:     !interactive,
		}
		if !interactive {
			opts.OnSchemaReady = func(rows int64, cols int) {
				fmt.Fprintf(os.Stderr, "      %d rows, %d cols\n", rows, cols)
			}
			opts.OnTableReady = func(table string) {
				fmt.Fprintf(os.Stderr, "      → %s\n", table)
			}
		}
		res := ingest.IngestFile(client, fp, table, opts)
		if res.Err != nil {
			failed++
			failures = append(failures, failure{fp, res.Err})
			if interactive {
				fmt.Fprintf(os.Stderr, "  FAIL %s: %s\n", dn, firstLine(res.Err.Error()))
			} else {
				fmt.Fprintf(os.Stderr, "      FAIL: %s\n", firstLine(res.Err.Error()))
			}
			continue
		}
		successes++
		if interactive {
			fmt.Fprintf(os.Stderr, "  OK   %s  table=%s  rows=%d  %s  in %s\n",
				dn, res.Table, res.Rows, humanBytes(res.Bytes), res.Duration.Round(100*time.Millisecond))
		} else {
			fmt.Fprintf(os.Stderr, "      OK in %s\n", res.Duration.Round(100*time.Millisecond))
		}
	}

	fmt.Fprintf(os.Stderr, "\nDone. %d succeeded, %d failed.\n", successes, failed)
	if failed > 0 {
		fmt.Fprintln(os.Stderr, "Failed:")
		for _, f := range failures {
			fmt.Fprintf(os.Stderr, "  %s: %s\n", displayName(f.path), firstLine(f.err.Error()))
		}
		os.Exit(1)
	}
	return nil
}

// makeDisplayName returns a function that formats a file path for output.
// Single-file mode shows just the basename; directory mode shows the path
// relative to the input directory (with forward slashes).
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

// isInteractive reports whether stderr is a terminal (where the progress bar
// renders correctly). In CI, redirected pipes, or background runs it returns
// false, and we use simplified line-oriented output instead.
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
