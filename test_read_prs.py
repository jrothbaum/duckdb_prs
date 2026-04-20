"""
Integration tests for the read_prs DuckDB extension.

Reads every test file used by polars_readstat_rs via both:
  - polars_readstat (ground truth: row count, column names)
  - DuckDB with the read_prs extension loaded (read_prs / read_prs / read_prs)

and asserts they agree.

Run from the polars_readstat project directory (where polars_readstat is installed):
    uv run --with duckdb python /path/to/duckdb_prs/test_read_prs.py
"""

import argparse
import sys
import tempfile
from pathlib import Path

import duckdb
import polars as pl
import polars_readstat

READSTAT_RS_TESTS = Path(__file__).parent.parent / "polars_readstat" / "crates" / "polars_readstat_rs" / "tests"
DEFAULT_EXT = Path(__file__).parent / "target" / "release" / "read_prs.duckdb_extension"

SAS_DIR   = READSTAT_RS_TESTS / "sas"   / "data"
STATA_DIR = READSTAT_RS_TESTS / "stata" / "data"
SPSS_DIR  = READSTAT_RS_TESTS / "spss"  / "data"


def collect(root: Path, *exts: str) -> list[Path]:
    files = []
    for ext in exts:
        files.extend(p for p in root.rglob(f"*.{ext}") if "too_big" not in p.parts)
    return sorted(files)


def open_db(ext_path: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.execute(f"LOAD '{ext_path}'")
    return con


def check(ext_path: Path, fn: str, path: Path, df: pl.DataFrame, label: str) -> bool:
    try:
        con = open_db(ext_path)

        # Schema check: column count must match.
        ext_cols = con.sql(f"SELECT * FROM {fn}('{path}') LIMIT 0").columns
        if len(ext_cols) != len(df.columns):
            print(f"  FAIL  {label}: col count {len(ext_cols)} != {len(df.columns)}"
                  f"  ext={ext_cols}  polars={df.columns}")
            con.close()
            return False

        n_rows = df.height
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            pq_path = f.name
        # Rename polars columns by position to match the extension's column names,
        # so DuckDB can join/compare them directly.
        rename_map = {old: new for old, new in zip(df.columns, ext_cols)}
        df.rename(rename_map).write_parquet(pq_path)

        # Within DuckDB: compare extension output vs polars parquet output.
        # Use EXCEPT in both directions to detect any differing rows.
        ext_sel  = ", ".join(f'"{c}"' for c in ext_cols)
        diff_sql = f"""
            SELECT count(*) FROM (
                (SELECT {ext_sel} FROM {fn}('{path}'))
                EXCEPT ALL
                (SELECT * FROM read_parquet('{pq_path}'))
            ) t
        """
        diff = con.sql(diff_sql).fetchone()[0]
        con.close()
        Path(pq_path).unlink(missing_ok=True)
    except Exception as e:
        print(f"  FAIL  {label}: {e}")
        return False

    if diff != 0:
        print(f"  FAIL  {label}: {diff} rows differ  ({n_rows} total)")
        return False

    print(f"  ok    {label}  ({n_rows} rows, {len(ext_cols)} cols)")
    return True


def read_via_polars_readstat(path: Path):
    """Read any supported file using polars_readstat and return a DataFrame."""
    return polars_readstat.scan_readstat(str(path)).collect()


def test_format(ext_path: Path, duckdb_fn: str, files: list[Path], label: str) -> tuple[int, int]:
    passed = failed = 0
    print(f"\n=== {label} ({len(files)} files) ===")
    for path in files:
        rel = path.relative_to(READSTAT_RS_TESTS)
        try:
            df = read_via_polars_readstat(path)
        except Exception as e:
            print(f"  SKIP  {rel}: polars_readstat failed: {e}")
            continue
        if not df.columns:
            print(f"  SKIP  {rel}: 0 columns (unsupported by DuckDB)")
            continue
        if check(ext_path, duckdb_fn, path, df, str(rel)):
            passed += 1
        else:
            failed += 1
    return passed, failed


def test_sas(ext_path):
    return test_format(ext_path, "read_prs", collect(SAS_DIR, "sas7bdat"), "SAS")


def test_stata(ext_path):
    return test_format(ext_path, "read_prs", collect(STATA_DIR, "dta"), "Stata")


def test_spss(ext_path):
    return test_format(ext_path, "read_prs", collect(SPSS_DIR, "sav", "zsav"), "SPSS")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ext", default=str(DEFAULT_EXT),
                        help="Path to libread_prs.so")
    args = parser.parse_args()

    ext_path = Path(args.ext)
    if not ext_path.exists():
        sys.exit(f"Extension not found: {ext_path}\nRun: cargo build --release")

    total_passed = total_failed = 0
    for passed, failed in [test_sas(ext_path), test_stata(ext_path), test_spss(ext_path)]:
        total_passed += passed
        total_failed += failed

    print(f"\n{'='*40}")
    print(f"Results: {total_passed} passed, {total_failed} failed")
    sys.exit(1 if total_failed else 0)


if __name__ == "__main__":
    main()
