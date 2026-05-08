# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`kusto-ingest-cli` is a Go CLI that ingests CSV/TSV files into Azure Data Explorer (Kusto). It auto-infers schema, auto-creates the table and ordinal-based CSV ingestion mapping, and hands the file off to the official `azure-kusto-go` SDK's **managed ingest client** — which automatically picks streaming or queued ingest based on payload size. Auth is delegated to the Azure CLI (`az login`).

The Go module is `github.com/fondoger/kusto-ingest-cli`. The binary produced by `go install` is `kusto-ingest-cli` (the last segment of the module path).

## Commands

```bash
go build ./...                          # compile
go install .                            # install ./kusto-ingest-cli into $GOPATH/bin
go test ./...                           # unit tests (fast, no network)
go test -run TestSchemaInfer ./internal/schema   # single test by name
go vet ./...
make integration-test                   # opt-in; requires `az login` and a real cluster
```

The integration test (`internal/ingest/integration_test.go`) is gated on `KUSTO_TEST_CLUSTER` + `KUSTO_TEST_DB` env vars and skipped otherwise. The Makefile sets them via `set VAR=value` (cmd.exe syntax — **no quotes around values**, no space before `&&`, otherwise the value picks up trailing whitespace).

## Architecture

Top-down call flow:

```
main.go                       flag parsing, file collection (single file / dir / -r),
                              table-name derivation, per-file dispatch, displays
                              progress / failures
  └─ ingest.IngestFile         schema.Infer → ensureTable → ensureMapping → uploadViaSDK
       ├─ schema.Infer         two-pass: count rows for sampling step, then sample-infer types
       │                       candidate chain: bool → long → real → datetime → timespan → string
       ├─ kusto.Client.Mgmt    runs control commands (.create/.drop/.create-or-alter table /
       │                       mapping) via azkustodata.Client
       └─ uploadViaSDK         single azkustoingest.Managed.FromFile call with
                               IgnoreFirstRecord(), IngestionMappingRef, ReportResultToTable;
                               waits on result.Wait(ctx)
```

Key invariants and design choices that span files:

- **All ingest goes through `azkustoingest.Managed`.** The SDK auto-picks streaming (small files, low latency) or queued (large files, sustainable throughput); we never make that decision. There's no client-side batching, throttling, or 409/EntityNotFound retry — the SDK handles it. If you find yourself adding a `Batcher` or sleep loop, stop and check whether the SDK already does it.
- **CSV/TSV with ordinal mapping.** `ensureMapping` creates a `csv` mapping (`{"column": X, "Properties": {"Ordinal": "N"}}`) which the SDK references via `IngestionMappingRef`. `streamFormat` is `Csv` for `.csv` and `Tsv` for `.tsv`. `IgnoreFirstRecord()` skips the header row server-side, so we don't pre-process the file.
- **One cluster URL.** `kusto.New` normalizes once (`https://` prefix, trim trailing slash) and constructs both an `azkustodata.Client` (for mgmt) and an `azkustoingest.Managed` (for ingest). The SDK auto-derives the data-management endpoint (`ingest-<cluster>`) for queued operations.
- **Three table modes are mutually exclusive and enforced in `main.go`:** default (error if table exists), `--append` (uses `.create-merge`), `--force` (drops + recreates). `ensureTable` in `internal/ingest/ingest.go` switches on these.
- **Auth is delegated to `az`.** No tokens are managed in our code. `azkustodata.NewConnectionStringBuilder(cluster).WithAzCli()` makes the SDK shell out to the Azure CLI on demand. If a user isn't logged in, the SDK surfaces the error.
- **`namesafe` has two sanitizers**: `Sanitize` (full, prepends `_` for digit-leading names) and `SanitizeChars` (chars only, no digit prefix). When `--table-prefix` is set, `defaultTableName` uses `SanitizeChars` because the prefix already guarantees a non-digit start. `DedupColumns` handles header collisions with `_2`, `_3`, … suffixes.
- **BOM stripping** lives in `internal/schema/schema.go` (the only place that reads the CSV ourselves now — the SDK reads the file independently for upload).

## Don't repeat past mistakes

- Don't reintroduce a hand-rolled streaming-ingest REST client. The SDK exists for a reason; we used to fight HTTP 409 buffer congestion and 400 EntityNotFound metadata races for hours before switching.
- Don't add an `ingest-` cluster prefix to the user-supplied URL. The SDK derives the ingest endpoint internally.
- Don't add a per-batch `Batcher` or inter-batch `time.Sleep`. The SDK manages payload size and rate.
- Don't put quotes around values in the Makefile's `set VAR=...` lines (cmd.exe takes them literally → "unsupported protocol scheme \"\"").
- Don't duplicate URL normalization outside `kusto.New`.
