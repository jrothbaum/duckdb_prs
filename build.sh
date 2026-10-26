#!/usr/bin/env bash
# Build the read_prs DuckDB extension.
# Built against DuckDB v2.0.0 via the duckdb-neo crate (v2 C API).
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
# Build first so compile errors are shown and stop the script; the packaging step below
# does not report them.
cargo build --release
cargo duckdb-ext build --duckdb-version v2.0.0 -- --release
