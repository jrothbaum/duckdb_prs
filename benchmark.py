"""
Benchmark read_prs vs polars_readstat vs read_stat on large files.

Files mirror those used in polars_readstat/test/benchmark.py.

Usage:
    uv run python benchmark.py [--ext path/to/read_prs.duckdb_extension] [--runs N]
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path


import duckdb

TOO_BIG = Path(__file__).parent.parent / "polars_readstat" / "crates" / "polars_readstat_rs" / "tests"

FILES = [
    TOO_BIG / "sas"   / "data" / "too_big" / "psam_p17.sas7bdat",
    TOO_BIG / "sas"   / "data" / "too_big" / "nls.sas7bdat",
    TOO_BIG / "sas"   / "data" / "too_big" / "numeric_1000000_2.sas7bdat",
    TOO_BIG / "sas"   / "data" / "too_big" / "topical.sas7bdat",
    TOO_BIG / "stata" / "data" / "too_big" / "usa_00009.dta",
    TOO_BIG / "spss"  / "data" / "too_big" / "anes_timeseries_cdf_spss_20260205.sav",
    TOO_BIG / "spss"  / "data" / "too_big" / "GSS2024.sav",
]

FORMAT_MAP = {
    "sas7bdat": "read_prs",
    "dta":      "read_prs",
    "sav":      "read_prs",
}

DEFAULT_EXT = Path(__file__).parent / "target" / "release" / "read_prs.duckdb_extension"


def median(times: list[float]) -> float:
    s = sorted(times)
    return s[len(s) // 2]


def _run_script(script: str, timeout: float) -> float:
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip().split("\n")[-1])
    return json.loads(result.stdout.strip().split("\n")[-1])


def time_read_prs(ext_path: str, fn: str, path_str: str, runs: int, timeout: float) -> float:
    script = f"""
import duckdb, time, json
con = duckdb.connect(config={{"allow_unsigned_extensions": True}})
con.execute("LOAD '{ext_path}'")
times = []
for _ in range({runs}):
    t0 = time.perf_counter()
    con.execute("SELECT * FROM {fn}('{path_str}')").pl().lazy().collect()
    times.append(time.perf_counter() - t0)
print(json.dumps(min(times)))
"""
    return _run_script(script, timeout)


def time_read_stat(path_str: str, runs: int, timeout: float) -> float:
    script = f"""
import duckdb, time, json
con = duckdb.connect()
con.execute("SET custom_extension_repository='http://community-extensions.duckdb.org';")
con.execute("LOAD read_stat")
times = []
for _ in range({runs}):
    t0 = time.perf_counter()
    con.execute("SELECT * FROM read_stat('{path_str}')").pl().lazy().collect()
    times.append(time.perf_counter() - t0)
print(json.dumps(min(times)))
"""
    return _run_script(script, timeout)


def time_polars(path_str: str, runs: int, timeout: float) -> float:
    script = f"""
import polars_readstat, time, json
times = []
for _ in range({runs}):
    t0 = time.perf_counter()
    polars_readstat.scan_readstat('{path_str}').collect()
    times.append(time.perf_counter() - t0)
print(json.dumps(min(times)))
"""
    return _run_script(script, timeout)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ext", default=str(DEFAULT_EXT))
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=120.0, help="Per-query timeout per engine (seconds)")
    args = parser.parse_args()

    ext_path = Path(args.ext)
    if not ext_path.exists():
        raise SystemExit(f"Extension not found: {ext_path}")

    # Use a single in-process connection just for row counts (fast, no hang risk)
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.execute(f"LOAD '{ext_path}'")

    print(f"\n{'file':<40} {'rows':>8}  {'read_prs':>10}  {'read_stat':>10}  {'polars':>10}")
    print("-" * 88)

    quack_total = rs_total = polars_total = 0.0
    n = 0

    for path in FILES:
        if not path.exists():
            print(f"  {path.name:<38}  (not found)")
            continue

        ext = path.suffix.lstrip(".")
        fn = FORMAT_MAP[ext]
        path_str = str(path)

        try:
            n_rows = con.execute(f"SELECT count(*) FROM {fn}('{path_str}')").fetchone()[0]
        except Exception as e:
            print(f"  {path.name:<38}  error: {e}")
            continue

        print(f"  {path.name:<38} {n_rows:>8}", end="  ", flush=True)

        try:
            t_q = time_read_prs(str(ext_path), fn, path_str, args.runs, args.timeout)
            q_str = f"{t_q*1000:8.0f}ms"
        except subprocess.TimeoutExpired:
            t_q = None
            q_str = f"{'TIMEOUT':>8}"
        except Exception as e:
            t_q = None
            q_str = f"{'ERR':>8}"
        print(f"{q_str:>10}", end="  ", flush=True)

        try:
            t_r = time_read_stat(path_str, args.runs, args.timeout)
            r_str = f"{t_r*1000:8.0f}ms"
        except subprocess.TimeoutExpired:
            t_r = None
            r_str = f"{'TIMEOUT':>8}"
        except Exception as e:
            t_r = None
            r_str = f"{'ERR':>8}"
        print(f"{r_str:>10}", end="  ", flush=True)

        try:
            t_p = time_polars(path_str, args.runs, args.timeout)
            p_str = f"{t_p*1000:8.0f}ms"
        except subprocess.TimeoutExpired:
            t_p = None
            p_str = f"{'TIMEOUT':>8}"
        except Exception as e:
            t_p = None
            p_str = f"{'ERR':>8}"
        print(f"{p_str:>10}")

        if t_q: quack_total += t_q
        if t_r: rs_total += t_r
        if t_p: polars_total += t_p
        n += 1

    if n:
        print(f"\n{'TOTAL':<40} {'':>8}  {quack_total*1000:8.0f}ms  {rs_total*1000:8.0f}ms  {polars_total*1000:8.0f}ms")
        print(f"({n} files, {args.runs} run(s) each; DuckDB outputs .arrow(), polars outputs DataFrame)")


if __name__ == "__main__":
    main()
