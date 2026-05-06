# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`kusto-ingest` is a Go CLI that ingests CSV/TSV files into Azure Data Explorer (Kusto) via the **Streaming Ingest** REST API. It auto-infers schema, auto-creates the table and JSON ingestion mapping, and uploads in 4 MiB batches. Auth is delegated to the Azure CLI (`az account get-access-token`).

The Go module is `kusto-ingest` (note: directory is `kusto-ingest-cli`, but the module name controls the binary name produced by `go install .`).

## Commands

```bash
go build ./...                          # compile
go install .                            # install ./kusto-ingest into $GOPATH/bin
go test ./...                           # unit tests (fast, no network)
go test -run TestRowToJSON ./internal/convert    # single test by name
go vet ./...
make integration-test                   # opt-in; requires `az login` and a real cluster
```

The integration test (`internal/ingest/integration_test.go`) is gated on `KUSTO_TEST_CLUSTER` + `KUSTO_TEST_DB` env vars and skipped otherwise. The Makefile sets them via `set VAR=value` (cmd.exe syntax — **no quotes around values**, no space before `&&`, otherwise the value picks up trailing whitespace and HTTP requests fail with `unsupported protocol scheme ""`).

## Architecture

Top-down call flow:

```
main.go                       flag parsing, file collection (single file / dir / -r),
                              table-name derivation, per-file dispatch
  └─ ingest.IngestFile         schema.Infer → ensureTable → ensureMapping → streamUpload
       ├─ schema.Infer         two-pass: count rows for sampling step, then sample-infer types
       │                       candidate chain: bool → long → real → datetime → timespan → string
       ├─ kusto.Client.Mgmt    POST /v1/rest/mgmt for .create/.drop/.create-or-alter/.alter
       └─ streamUpload         CSV reader → convert.RowToJSON → Batcher → ingestBatch
            └─ ingestBatch     calls kusto.Client.StreamIngest; lazy-recovers from
                               BadRequest_StreamingIngestionPolicyNotEnabled
```

Key invariants and design choices that span files:

- **One cluster URL for both Mgmt and StreamIngest.** Earlier iterations tried the `ingest-` prefix; do **not** reintroduce it. `kusto.New` normalizes the URL once (adds `https://`, trims trailing slash) — don't duplicate that logic at call sites.
- **Three table modes are mutually exclusive and enforced in `main.go`:** default (error if table exists), `--append` (uses `.create-merge`), `--force` (drops + recreates). `ensureTable` in `internal/ingest/ingest.go` switches on these.
- **Streaming-ingestion-policy recovery is lazy, not eager.** `ensureTable` does NOT pre-alter the policy. Instead, `ingestBatch` detects `BadRequest_StreamingIngestionPolicyNotEnabled` from the first batch's response and: (1) `.alter database … policy streamingingestion enable`, sleep 10s, retry; (2) only in `--append` mode, escalate to `.alter table …`, sleep 10s, retry. `policyState` is shared per-file via closure so each level is attempted at most once. If you change the error marker string or the policy-handling flow, this is the only place it lives.
- **JSON upload, not CSV upload.** `convert.RowToJSON` produces one JSON object per row, joined by `\n` into MultiJson (note: `streamFormat=MultiJson`, mapping is JSON). Empty cells: string columns get `""`; all other types omit the field (null). The mapping is `{"column": X, "Properties": {"Path": "$.X"}}`.
- **Batcher pre-emptively flushes** before crossing 4 MiB (`maxBatchBytes` in `internal/ingest/ingest.go`). A single oversize row is sent on its own without erroring.
- **`namesafe` has two sanitizers**: `Sanitize` (full, prepends `_` for digit-leading names) and `SanitizeChars` (chars only, no digit prefix). When `--table-prefix` is set, `defaultTableName` uses `SanitizeChars` because the prefix already guarantees a non-digit start. `DedupColumns` handles header collisions with `_2`, `_3`, … suffixes.
- **HTTP retry policy** lives in `kusto.Client.do`: 3 attempts with 1s/2s/4s backoff for 5xx; one transparent token refresh on 401; everything else fails fast.
- **BOM stripping** is duplicated in `schema/schema.go` and `ingest/ingest.go` (each reader needs its own). Both must agree, or row-counting and parsing diverge by one row.

## Don't repeat past mistakes

- Don't add an `ingest-` cluster prefix or a separate ingest URI flag — was tried and reverted.
- Don't eagerly alter streaming policy on table create — was tried and replaced with lazy recovery in `ingestBatch`.
- Don't put quotes around values in the Makefile's `set VAR=...` lines.
- Don't duplicate URL normalization outside `kusto.New`.
