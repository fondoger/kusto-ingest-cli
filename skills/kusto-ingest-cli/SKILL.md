---
name: kusto-ingest-cli
description: Ingest CSV/TSV files into Azure Data Explorer (Kusto).
---

# kusto-ingest-cli

A thin wrapper around `kusto-ingest-cli`, which uploads CSV/TSV files into Azure Data Explorer (Kusto) via the Streaming Ingest REST API. It auto-infers the schema, auto-creates the target table and JSON ingestion mapping, and authenticates via `az account get-access-token`. No SDK dependencies — pure REST.

## Assume it's installed and logged in

By default,
1. **assume `kusto-ingest-cli` is already installed and on PATH**.
2. **assume the user is already logged in** and has access to the target Kusto cluster.
Do not run additional checks before executing the ingest.

**Install if missing**: Only do this after an invocation fails with "not found":

```bash
go install github.com/fondoger/kusto-ingest-cli@latest
```

If ingest fails with 401/403, the user likely needs `az login --tenant <correct-tenant>` or doesn't have ingest permissions on that database — surface the error rather than retrying blindly. Token is obtained automatically via `az account get-access-token` for the Kusto resource. Install [Azure CLI](https://aka.ms/installazurecli) and log in (`az login`).

> **Need to run KQL queries instead of ingesting data?** Use the `kusto-cli` skill (wraps [danielsada/go-kusto-cli](https://github.com/danielsada/go-kusto-cli)).

## Usage

```
kusto-ingest-cli [flags] <path>
```

`<path>` is either a single `.csv`/`.tsv` file or a directory containing them.

### Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--cluster` | Kusto cluster URL. `https://` is added if missing. Falls back to `$KUSTO_INGEST_CLUSTER`. | — |
| `--database` | Target database. Falls back to `$KUSTO_INGEST_DATABASE`. | — |
| `--table` | Target table name (single-file mode only). | sanitized filename |
| `-r`, `--recursive` | Recurse into subdirectories. | `false` |
| `--append` | Append to existing table; create if missing. Without this, an existing table is an error. | `false` |
| `-f`, `--force` | Drop and recreate the target table. **Destroys existing data.** Mutually exclusive with `--append`. | `false` |
| `--infer-rows` | Max rows sampled for type inference (evenly distributed). | `10000` |
| `--table-prefix` | Prefix prepended to auto-derived table names (e.g. `raw_`). Cannot be combined with `--table`. | — |

### Environment variables

| Variable | Equivalent flag |
|---|---|
| `KUSTO_INGEST_CLUSTER` | `--cluster` |
| `KUSTO_INGEST_DATABASE` | `--database` |

### Examples

```bash
# Single file, table name auto-derived from filename
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB ./events.csv

# Single file into a specific table
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB --table Events ./events.csv

# Recurse a directory tree, append to existing tables
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB -r --append ./data/

# Recurse with a name prefix on every auto-derived table
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB -r --table-prefix raw_ ./data/

# Drop and recreate a table from scratch (destroys existing data)
kusto-ingest-cli --cluster mycluster.kusto.windows.net --database MyDB --force ./events.csv

# Use environment variables instead of repeating cluster/db flags
export KUSTO_INGEST_CLUSTER=mycluster.kusto.windows.net
export KUSTO_INGEST_DATABASE=MyDB
kusto-ingest-cli ./events.csv
```

## Behavior notes

- **Existing tables**: without `--append` or `--force`, ingesting into a table that already exists is an error (protects against accidental schema mismatch).
- **Type inference**: each column resolves to one of `bool`, `long`, `real`, `datetime`, `timespan`, or `string`. `bool` is only chosen when the column actually contains `true`/`false` literals (not just `0`/`1`).
- **Empty cells**: become null for non-string columns, `""` for string columns.
- **Streaming policy**: on first use, if the database doesn't have streaming ingestion enabled, the CLI auto-runs `.alter database <db> policy streamingingestion enable`, waits 10s, and retries. In `--append` mode, falls back to a table-level alter if needed.
- **Batching**: rows are uploaded in batches up to 4 MiB; flushes before crossing the boundary.
- **CSV/TSV parsing**: RFC 4180-compliant with lazy quotes; `.tsv` uses tab, everything else uses comma. UTF-8 BOM is stripped automatically.

## Exit codes

- `0` — all files ingested successfully
- `1` — at least one file failed (a summary is printed at the end)
- `2` — usage error (bad flags, missing required values, etc.)
