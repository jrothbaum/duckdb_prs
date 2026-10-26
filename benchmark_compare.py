"""Compare read_prs, the community read_stat extension and polars_readstat.

read_stat is built per DuckDB version and has no DuckDB 2.0 build yet, while read_prs needs
DuckDB 2.0, so each runs in its own environment and the results are merged into one table.
polars_readstat runs in both as a control (the two columns should agree).

Every measurement runs in a fresh subprocess: DuckDB holds the whole result and .pl() makes a
second copy, and allocators do not return freed memory, so timing several reads in one process
exceeds memory on the larger files.

Run from the polars_readstat project directory (where polars_readstat is installed):
    uv run python /path/to/duckdb_prs/benchmark_compare.py [--runs N] [--variants]

  --variants   also time read_prs preserve_order, a 3-column select and count(*) against
               polars_readstat doing the same (on the DuckDB 2.0 side only)

read_prs is the baseline: polars_readstat and read_stat are reported relative to it.
  --old/--new  DuckDB versions for read_stat / read_prs (default 1.5.2 / 2.0.0)
"""

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
TESTS = HERE.parent / "polars_readstat" / "crates" / "polars_readstat_rs" / "tests"
EXT = HERE / "target" / "release" / "read_prs.duckdb_extension"

FILES = [
    TESTS / "sas" / "data" / "too_big" / "psam_p17.sas7bdat",
    TESTS / "sas" / "data" / "too_big" / "nls.sas7bdat",
    TESTS / "sas" / "data" / "too_big" / "numeric_1000000_2.sas7bdat",
    TESTS / "sas" / "data" / "too_big" / "topical.sas7bdat",
    TESTS / "stata" / "data" / "too_big" / "usa_00009.dta",
    TESTS / "spss" / "data" / "too_big" / "anes_timeseries_cdf_spss_20260205.sav",
    TESTS / "spss" / "data" / "too_big" / "GSS2024.sav",
]

CELL_TIMEOUT = 240


# ── cell: one measurement, run in its own process ────────────────────────────

def run_cell(engine: str, kind: str, path: str, runs: int) -> None:
    """Print the minimum time in ms of `runs` runs of one measurement as JSON."""
    import duckdb
    import polars_readstat as ps

    def time_it(fn) -> float:
        times = []
        for _ in range(runs):
            t = time.perf_counter()
            fn()
            times.append(time.perf_counter() - t)
        return min(times) * 1000

    if engine == "polars":
        # Names come from a throwaway scan of the schema.
        cols = ps.scan_readstat(path).collect_schema().names()
        sel = list(dict.fromkeys([cols[0], cols[len(cols) // 2], cols[-1]]))  # no repeats on narrow files
        fns = {
            "full": lambda: ps.scan_readstat(path).collect(),
            "ordered": lambda: ps.scan_readstat(path, preserve_order=True).collect(),
            "sel3": lambda: ps.scan_readstat(path).select(sel).collect(),
            "count": lambda: ps.scan_readstat(path).select(cols[0]).collect().height,
        }
        print(json.dumps(time_it(fns[kind])))
        return

    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    if engine == "read_prs":
        con.execute(f"LOAD '{EXT}'")
    else:
        con.execute("SET custom_extension_repository='http://community-extensions.duckdb.org';")
        con.execute("INSTALL read_stat")
        con.execute("LOAD read_stat")
    cols = con.sql(f"SELECT * FROM {engine}('{path}') LIMIT 0").columns
    sel = ", ".join(f'"{c}"' for c in dict.fromkeys((cols[0], cols[len(cols) // 2], cols[-1])))
    sql = {
        "full": f"SELECT * FROM {engine}('{path}')",
        "ordered": f"SELECT * FROM {engine}('{path}', preserve_order=true)",
        "sel3": f"SELECT {sel} FROM {engine}('{path}')",
        "count": f"SELECT count(*) FROM {engine}('{path}')",
    }[kind]
    if kind == "count":
        print(json.dumps(time_it(lambda: con.sql(sql).fetchall())))
    else:
        print(json.dumps(time_it(lambda: con.sql(sql).pl())))


# ── orchestration ────────────────────────────────────────────────────────────

def measure(duckdb_version: str, engine: str, kind: str, path: Path, runs: int):
    cmd = [
        "uv", "run", "--with", f"duckdb=={duckdb_version}", "python", str(Path(__file__).resolve()),
        "--cell", engine, kind, str(path), "--runs", str(runs),
    ]
    try:
        out = subprocess.run(cmd, capture_output=True, text=True, timeout=CELL_TIMEOUT)
    except subprocess.TimeoutExpired:
        print(f"    {engine}/{kind} {path.name}: TIMEOUT", file=sys.stderr)
        return None
    if out.returncode != 0:
        msg = [ln for ln in out.stderr.strip().splitlines() if "VIRTUAL_ENV" not in ln]
        print(f"    {engine}/{kind} {path.name}: FAILED ({(msg[-1] if msg else 'exit ' + str(out.returncode))[:110]})",
              file=sys.stderr)
        return None
    return json.loads(out.stdout.strip().splitlines()[-1])


def version_of(duckdb_version: str) -> str:
    out = subprocess.run(
        ["uv", "run", "--with", f"duckdb=={duckdb_version}", "python", "-c", "import duckdb; print(duckdb.__version__)"],
        capture_output=True, text=True,
    )
    return out.stdout.strip().splitlines()[-1] if out.returncode == 0 else duckdb_version


def cell(v) -> str:
    return f"{v:8.0f}" if v is not None else f"{'-':>8}"


def ratio(a, b) -> str:
    return f"{a / b:5.2f}x" if a and b else f"{'-':>6}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--cell", nargs=3, metavar=("ENGINE", "KIND", "PATH"), help="internal: run one measurement")
    ap.add_argument("--runs", type=int, default=3)
    ap.add_argument("--variants", action="store_true")
    ap.add_argument("--old", default="1.5.2", help="DuckDB version to run read_stat on")
    ap.add_argument("--new", default="2.0.0", help="DuckDB version to run read_prs on")
    args = ap.parse_args()

    if args.cell:
        run_cell(*args.cell, args.runs)
        return

    results = {}
    for path in FILES:
        if not path.exists():
            continue
        print(f"  {path.name} ...", file=sys.stderr, flush=True)
        r = {
            "prs": measure(args.new, "read_prs", "full", path, args.runs),
            "stat": measure(args.old, "read_stat", "full", path, args.runs),
            "pl_new": measure(args.new, "polars", "full", path, args.runs),
            "pl_old": measure(args.old, "polars", "full", path, args.runs),
        }
        if args.variants:
            for kind in ("ordered", "sel3", "count"):
                r[f"prs_{kind}"] = measure(args.new, "read_prs", kind, path, args.runs)
                r[f"pl_{kind}"] = measure(args.new, "polars", kind, path, args.runs)
        results[path.name] = r

    print(f"\nread_prs (the baseline, 1.00x) on DuckDB {version_of(args.new)}; read_stat on DuckDB {version_of(args.old)}; "
          f"min of {args.runs} run(s), result as a polars DataFrame")
    print("times in ms; the 'x' columns are time relative to read_prs, so >1 means slower than read_prs")
    print(f"\n{'file':<40} {'read_prs':>8} {'polars':>8} {'read_stat':>9}  {'polars':>7} {'read_stat':>9}   {'polars*':>8}")
    print(f"{'':<40} {'ms':>8} {'ms':>8} {'ms':>9}  {'x prs':>7} {'x prs':>9}   {'ms':>8}")
    print("-" * 104)
    tot = dict.fromkeys(("prs", "stat", "pl_new", "pl_old"), 0.0)
    for name, r in results.items():
        print(f"{name:<40} {cell(r['prs'])} {cell(r['pl_new'])} {cell(r['stat']):>9}  "
              f"{ratio(r['pl_new'], r['prs']):>7} {ratio(r['stat'], r['prs']):>9}   {cell(r['pl_old'])}")
        if all(r[k] for k in tot):
            for k in tot:
                tot[k] += r[k]
    print("-" * 104)
    print(f"{'TOTAL (files where all completed)':<40} {cell(tot['prs'])} {cell(tot['pl_new'])} {cell(tot['stat']):>9}  "
          f"{ratio(tot['pl_new'], tot['prs']):>7} {ratio(tot['stat'], tot['prs']):>9}   {cell(tot['pl_old'])}")
    print("\npolars = polars_readstat alongside read_prs (DuckDB 2.x env); polars* = alongside read_stat (1.x env),")
    print("a control that should match polars. Relative times use read_stat's own run against read_prs's run.")

    if args.variants:
        print(f"\nread_prs (baseline) variants vs polars_readstat doing the same (DuckDB {version_of(args.new)} env); ms and polars time / read_prs time")
        print(f"\n{'file':<40} | {'preserve_order':^26} | {'3 columns':^26} | {'count(*) / 1 col':^26}")
        print(f"{'':<40} | {'prs':>8} {'polars':>8} {'x prs':>7} | {'prs':>8} {'polars':>8} {'x prs':>7} | {'prs':>8} {'polars':>8} {'x prs':>7}")
        for name, r in results.items():
            parts = [f"{cell(r[f'prs_{k}'])} {cell(r[f'pl_{k}'])} {ratio(r[f'pl_{k}'], r[f'prs_{k}']):>7}" for k in ("ordered", "sel3", "count")]
            print(f"{name:<40} | " + " | ".join(parts))


if __name__ == "__main__":
    main()
