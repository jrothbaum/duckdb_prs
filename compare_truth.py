"""
Compare all test files against polars_readstat 0.11.1 as truth.

Usage:
  # Step 1: generate truth parquets (run with polars_readstat==0.11.1)
  uv run --with "polars_readstat==0.11.1" python compare_truth.py --mode truth

  # Step 2: run full comparison (run normally)
  uv run --with duckdb python compare_truth.py --mode compare
"""

import argparse
import tempfile
import shutil
from pathlib import Path

import duckdb
import polars as pl
import polars_readstat

READSTAT_RS_TESTS = Path(__file__).parent.parent / "polars_readstat" / "crates" / "polars_readstat_rs" / "tests"
DEFAULT_EXT = Path(__file__).parent / "target" / "release" / "read_prs.duckdb_extension"
TRUTH_DIR = Path(__file__).parent / "truth_parquets"

SAS_DIR   = READSTAT_RS_TESTS / "sas"   / "data"
STATA_DIR = READSTAT_RS_TESTS / "stata" / "data"
SPSS_DIR  = READSTAT_RS_TESTS / "spss"  / "data"

FORMAT_MAP = {
    "sas7bdat": ("read_prs", SAS_DIR),
    "dta":      ("read_prs",      STATA_DIR),
    "sav":      ("read_prs",      SPSS_DIR),
    "zsav":     ("read_prs",      SPSS_DIR),
}


def collect(root: Path, *exts: str) -> list[Path]:
    files = []
    for ext in exts:
        files.extend(p for p in root.rglob(f"*.{ext}") if "too_big" not in p.parts)
    return sorted(files)


def all_files() -> list[Path]:
    return (
        collect(SAS_DIR, "sas7bdat")
        + collect(STATA_DIR, "dta")
        + collect(SPSS_DIR, "sav", "zsav")
    )


def truth_path(src: Path) -> Path:
    return TRUTH_DIR / (src.name + ".parquet")


def rows_differ(con: duckdb.DuckDBPyConnection, sel_a: str, sel_b: str) -> int:
    return con.sql(f"""
        SELECT count(*) FROM (
            ({sel_a}) EXCEPT ALL ({sel_b})
        ) t
    """).fetchone()[0]


# ── mode: truth ──────────────────────────────────────────────────────────────

def generate_truth():
    TRUTH_DIR.mkdir(exist_ok=True)
    files = all_files()
    print(f"Generating truth parquets for {len(files)} files → {TRUTH_DIR}")
    skipped = 0
    for path in files:
        dest = truth_path(path)
        if dest.exists():
            continue
        try:
            df = polars_readstat.scan_readstat(str(path)).collect()
            if not df.columns:
                skipped += 1
                continue
            df.write_parquet(str(dest))
        except BaseException as e:
            short = str(e).split("\n")[0][:120]
            print(f"  SKIP {path.name}: {short}")
            skipped += 1
            continue
    written = len(list(TRUTH_DIR.glob("*.parquet")))
    print(f"Done: {written} truth files written, {skipped} skipped")


# ── mode: compare ─────────────────────────────────────────────────────────────

def compare(ext_path: Path):
    con_plain = duckdb.connect()  # no extension — for parquet comparisons only

    con_ext = duckdb.connect(config={"allow_unsigned_extensions": True})
    con_ext.execute(f"LOAD '{ext_path}'")

    files = all_files()

    results = []  # (name, truth_vs_current, truth_vs_duck, n_rows)

    for path in files:
        tpath = truth_path(path)
        if not tpath.exists():
            continue  # no truth generated (was skipped)

        ext_suffix = path.suffix.lstrip(".")
        fn = FORMAT_MAP[ext_suffix][0]

        # ── current polars_readstat ──────────────────────────────────────────
        try:
            df_cur = polars_readstat.scan_readstat(str(path)).collect()
            if not df_cur.columns:
                results.append((path.name, "skip:0cols", "skip:0cols", 0))
                continue
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
                cur_pq = f.name
            df_cur.write_parquet(cur_pq)
            cur_ok = rows_differ(con_plain,
                f"SELECT * FROM read_parquet('{tpath}')",
                f"SELECT * FROM read_parquet('{cur_pq}')",
            )
            Path(cur_pq).unlink(missing_ok=True)
        except Exception as e:
            cur_ok = f"err:{e}"

        # ── duckdb / read_prs ───────────────────────────────────────────────
        try:
            ext_cols = con_ext.sql(f"SELECT * FROM {fn}('{path}') LIMIT 0").columns
            truth_cols = con_plain.sql(f"SELECT * FROM read_parquet('{tpath}') LIMIT 0").columns
            if len(ext_cols) != len(truth_cols):
                duck_ok = f"col_mismatch:ext={len(ext_cols)},truth={len(truth_cols)}"
            else:
                # rename truth parquet columns to match ext column names (positional)
                with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
                    renamed_pq = f.name
                df_truth = pl.read_parquet(str(tpath))
                rename_map = {old: new for old, new in zip(df_truth.columns, ext_cols)}
                df_truth.rename(rename_map).write_parquet(renamed_pq)
                ext_sel = ", ".join(f'"{c}"' for c in ext_cols)
                duck_ok = rows_differ(con_ext,
                    f"SELECT {ext_sel} FROM {fn}('{path}')",
                    f"SELECT * FROM read_parquet('{renamed_pq}')",
                )
                Path(renamed_pq).unlink(missing_ok=True)
        except Exception as e:
            duck_ok = f"err:{e}"

        n_rows = con_plain.sql(f"SELECT count(*) FROM read_parquet('{tpath}')").fetchone()[0]
        results.append((path.name, cur_ok, duck_ok, n_rows))

    con_plain.close()
    con_ext.close()

    # ── report ───────────────────────────────────────────────────────────────
    both_ok = cur_fail = duck_fail = both_fail = skip = 0
    print(f"\n{'file':<45} {'cur_diff':>10} {'duck_diff':>10} {'rows':>8}")
    print("-" * 78)
    for name, cur, duck, n in sorted(results, key=lambda x: x[0]):
        cur_str  = str(cur)  if cur  != 0 else "ok"
        duck_str = str(duck) if duck != 0 else "ok"
        cur_bad  = cur  != 0 and not str(cur).startswith("skip")
        duck_bad = duck != 0 and not str(duck).startswith("skip")
        if cur_bad or duck_bad:
            print(f"  {name:<43} {cur_str:>10} {duck_str:>10} {n:>8}")
        if str(cur).startswith("skip"):
            skip += 1
        elif cur_bad and duck_bad:
            both_fail += 1
        elif cur_bad:
            cur_fail += 1
        elif duck_bad:
            duck_fail += 1
        else:
            both_ok += 1

    print(f"\nSummary: {both_ok} both-ok, {cur_fail} current-only failures, "
          f"{duck_fail} duckdb-only failures, {both_fail} both-fail, {skip} skipped")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["truth", "compare"], required=True)
    parser.add_argument("--ext", default=str(DEFAULT_EXT))
    args = parser.parse_args()

    if args.mode == "truth":
        generate_truth()
    else:
        ext_path = Path(args.ext)
        if not ext_path.exists():
            raise SystemExit(f"Extension not found: {ext_path}")
        compare(ext_path)


if __name__ == "__main__":
    main()
