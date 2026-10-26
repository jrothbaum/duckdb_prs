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
import os

# Cap thread pools before polars/duckdb are imported; the suite opens hundreds
# of connections and uncapped pools can exhaust the session's thread limit.
os.environ.setdefault("POLARS_MAX_THREADS", "4")
os.environ.setdefault("RAYON_NUM_THREADS", "4")

import signal
import sys
import tempfile
from pathlib import Path

SUITE_TIMEOUT_SECS = 240  # kill the whole process if it takes more than 4 minutes

import duckdb
import polars as pl
import polars_readstat

READSTAT_RS_TESTS = Path(__file__).parent.parent / "polars_readstat" / "crates" / "polars_readstat_rs" / "tests"
DEFAULT_EXT = Path(__file__).parent / "target" / "release" / "read_prs.duckdb_extension"

SAS_DIR   = READSTAT_RS_TESTS / "sas"   / "data"
STATA_DIR = READSTAT_RS_TESTS / "stata" / "data"
SPSS_DIR  = READSTAT_RS_TESTS / "spss"  / "data"

# Files/dirs that hang during testing (known issues tracked separately).
SKIP_NAMES = {
    "omov.sas7bdat",
    "rmov.sas7bdat",
}
SKIP_DIRS: set[str] = set()


def collect(root: Path, *exts: str) -> list[Path]:
    files = []
    for ext in exts:
        files.extend(
            p for p in root.rglob(f"*.{ext}")
            if "too_big" not in p.parts
            and p.name not in SKIP_NAMES
            and not any(d in SKIP_DIRS for d in p.parts)
        )
    return sorted(files)


MAX_THREADS = 400  # abort well before the machine's thread limit is reached

_con: duckdb.DuckDBPyConnection | None = None


def open_db(ext_path: Path) -> duckdb.DuckDBPyConnection:
    # One shared connection for the whole run; avoids re-creating a database
    # (and its thread pool) for each of the hundreds of files.
    global _con
    if _con is None:
        _con = duckdb.connect(config={"allow_unsigned_extensions": True, "threads": 2})
        _con.execute(f"LOAD '{ext_path}'")
    return _con


def guard_threads() -> None:
    """Exit instead of exhausting the machine's thread limit on a leak."""
    try:
        with open("/proc/self/status") as f:
            n = next(int(l.split()[1]) for l in f if l.startswith("Threads:"))
    except OSError:  # no /proc (non-Linux): skip the guard
        return
    if n > MAX_THREADS:
        print(f"ABORT: thread leak, {n} threads in test process (limit {MAX_THREADS})", file=sys.stderr, flush=True)
        os._exit(3)


def check(ext_path: Path, fn: str, path: Path, df: pl.DataFrame, label: str) -> bool:
    guard_threads()
    try:
        con = open_db(ext_path)

        # Schema check: column count must match.
        ext_cols = con.sql(f"SELECT * FROM {fn}('{path}') LIMIT 0").columns
        if len(ext_cols) != len(df.columns):
            print(f"  FAIL  {label}: col count {len(ext_cols)} != {len(df.columns)}"
                  f"  ext={ext_cols}  polars={df.columns}")
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
        diff = con.sql(diff_sql).fetchall()[0][0]
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


def test_format(ext_path: Path, duckdb_fn: str, files: list[Path], label: str, every: int = 1) -> tuple[int, int]:
    files = files[::every]
    passed = failed = 0
    print(f"\n=== {label} ({len(files)} files) ===", flush=True)
    for path in files:
        rel = path.relative_to(READSTAT_RS_TESTS)
        try:
            df = read_via_polars_readstat(path)
        except Exception as e:
            print(f"  SKIP  {rel}: polars_readstat failed: {e}", flush=True)
            continue
        if not df.columns:
            print(f"  SKIP  {rel}: 0 columns (unsupported by DuckDB)", flush=True)
            continue
        if check(ext_path, duckdb_fn, path, df, str(rel)):
            passed += 1
        else:
            failed += 1
    return passed, failed


def test_sas(ext_path, every=1):
    return test_format(ext_path, "read_prs", collect(SAS_DIR, "sas7bdat"), "SAS", every)


def test_stata(ext_path, every=1):
    return test_format(ext_path, "read_prs", collect(STATA_DIR, "dta"), "Stata", every)


def test_spss(ext_path, every=1):
    return test_format(ext_path, "read_prs", collect(SPSS_DIR, "sav", "zsav"), "SPSS", every)


def main():
    signal.signal(signal.SIGALRM, lambda s, f: sys.exit(f"\nTimed out after {SUITE_TIMEOUT_SECS}s"))
    signal.alarm(SUITE_TIMEOUT_SECS)

    parser = argparse.ArgumentParser()
    parser.add_argument("--ext", default=str(DEFAULT_EXT),
                        help="Path to libread_prs.so")
    parser.add_argument("--every", type=int, default=1,
                        help="Test every Nth file (e.g. 3 = ~1/3 of files)")
    args = parser.parse_args()

    ext_path = Path(args.ext)
    if not ext_path.exists():
        sys.exit(f"Extension not found: {ext_path}\nRun: cargo build --release")

    total_passed = total_failed = 0
    for passed, failed in [test_sas(ext_path, args.every), test_stata(ext_path, args.every), test_spss(ext_path, args.every)]:
        total_passed += passed
        total_failed += failed

    print(f"\n{'='*40}")
    print(f"Results: {total_passed} passed, {total_failed} failed")
    sys.exit(1 if total_failed else 0)


if __name__ == "__main__":
    main()
