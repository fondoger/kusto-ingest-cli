# kusto-ingest-cli

A small CLI for ingesting CSV/TSV files into Azure Data Explorer (Kusto) via the Streaming Ingest REST API.

It infers the schema, creates the target table and JSON ingestion mapping for you, and uploads the data — no Kusto setup required beyond having a database you can write to.

## Features

- Single file, a directory, or a directory tree (`-r`)
- Schema inference from a sample of rows (default 10,000, evenly distributed)
- Auto-creates table and JSON ingestion mapping
- Auto-derives table names from filenames, sanitized to valid Kusto identifiers
- Three modes for existing tables: error (default), append, or force-recreate
- Auto-enables the streaming ingestion policy on first use (database-level, with table-level fallback in append mode)
- Streaming upload in 4 MiB batches with a progress bar
- Authenticates via your existing `az login` session

## Install

Requires Go 1.26+ and the Azure CLI (`az`).

```bash
go install github.com/fondoger/kusto-ingest-cli@latest
```

This installs the `kusto-ingest-cli` binary into your `$GOPATH/bin` (or `$GOBIN`).

> **Just want to run KQL queries?** This tool only does ingestion. For running queries against Kusto, see [danielsada/go-kusto-cli](https://github.com/danielsada/go-kusto-cli).

## Authentication

```bash
az login
```

The CLI calls `az account get-access-token` under the hood to obtain a token for `https://api.kusto.windows.net`. Make sure your account has ingest permissions on the target database.

## Usage

```bash
kusto-ingest-cli [flags] <path>
```

`<path>` is either a single `.csv`/`.tsv` file or a directory containing them.

### Quick examples

Ingest a single file (table name derived from the filename):

```bash
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB ./events.csv
```

Ingest a single file into a specific table:

```bash
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB \
  --table Events ./events.csv
```

Ingest every CSV/TSV in a directory tree, appending to existing tables:

```bash
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB \
  -r --append ./data/
```

Ingest a directory and prefix every auto-derived table name with `raw_`:

```bash
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB \
  -r --table-prefix raw_ ./data/
```

Drop and recreate a table from scratch (destroys existing data):

```bash
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB \
  --force ./events.csv
```

### Flags

| Flag | Description |
|---|---|
| `--cluster` | Kusto cluster URL. `https://` is added if missing. Falls back to `$KUSTO_INGEST_CLUSTER`. |
| `--database` | Target database. Falls back to `$KUSTO_INGEST_DATABASE`. |
| `--table` | Target table name (single-file mode only). Defaults to a sanitized version of the filename. |
| `-r`, `--recursive` | Recurse into subdirectories when `<path>` is a directory. |
| `--append` | Append to the existing table; create it if missing. Without this flag, an existing table causes an error. |
| `-f`, `--force` | Drop and recreate the target table. **Destroys existing data.** Mutually exclusive with `--append`. |
| `--infer-rows` | Max rows sampled for type inference, evenly distributed across the file (default `10000`). |
| `--table-prefix` | Prefix added to auto-derived table names (e.g. `raw_`). Cannot be combined with `--table`. |

### Environment variables

| Variable | Equivalent flag |
|---|---|
| `KUSTO_INGEST_CLUSTER` | `--cluster` |
| `KUSTO_INGEST_DATABASE` | `--database` |

## Behavior notes

**Table names.** When derived from a filename, characters that aren't ASCII letters, digits, or `_` are replaced with `_`, and a leading underscore is added if the name would otherwise start with a digit. With `--table-prefix`, the leading-underscore rule is skipped (the prefix already provides a valid first character).

**Existing tables.** Without `--append` or `--force`, ingesting into a table that already exists is an error — this protects you from accidentally mixing data with mismatched schemas. Use `--append` when you intend to add to an existing table, or `--force` when you intend to wipe and reload.

**Type inference.** Each column is classified as one of: `bool`, `long`, `real`, `datetime`, `timespan`, or `string`. A column is only inferred as `bool` if it contains actual `true`/`false` literals (not just `0`/`1`). Empty cells become null for non-string columns and `""` for string columns.

**Streaming ingestion policy.** If the database doesn't have streaming ingestion enabled, the CLI will detect the error from the first batch, run `.alter database <db> policy streamingingestion enable`, wait 10 seconds, and retry. In `--append` mode, if the database-level alter still doesn't take effect, it falls back to `.alter table <table> policy streamingingestion enable`. Each policy alter is attempted at most once per file.

**Batching.** Rows are uploaded in batches up to 4 MiB. The CLI flushes before crossing the 4 MiB boundary so individual requests stay safely under the streaming-ingest size limit.

**Header rows.** The first row of each file is treated as the header and used for column names. UTF-8 BOM is stripped automatically.

**CSV/TSV parsing.** RFC 4180-style CSVs are supported, including quoted fields containing commas and newlines. `.tsv` files use tab as the delimiter; everything else uses comma. Lazy quotes are enabled, so unescaped quote characters inside fields don't break parsing.

## Exit codes

- `0` — all files ingested successfully
- `1` — at least one file failed; a summary is printed at the end
- `2` — usage error (bad flags, missing required values, etc.)
