#!/usr/bin/env bash
# Build the read_prs DuckDB extension.
# The Rust duckdb crate version "1.10502" corresponds to DuckDB v1.5.2.
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
cargo duckdb-ext build --duckdb-version v1.5.2 -- --release
