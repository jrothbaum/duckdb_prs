"""Column projection through read_prs.

Selecting a subset of columns, in any order, must return exactly what polars_readstat
returns for the same select, for SAS, Stata and SPSS; count(*), a filter on an unselected
column and the row-index column must also work alongside a subset.

Run from the polars_readstat project directory:
    uv run --with "duckdb>=2.0.0" python /path/to/duckdb_prs/test_projection.py
"""
import sys
from pathlib import Path

import duckdb
import polars_readstat as ps

HERE = Path(__file__).parent
T = str(HERE.parent / "polars_readstat" / "crates" / "polars_readstat_rs" / "tests") + "/"
EXT = HERE / "target" / "release" / "read_prs.duckdb_extension"
con = duckdb.connect(config={"allow_unsigned_extensions": True})
con.execute(f"LOAD '{EXT}'")
files = ["sas/data/too_big/topical.sas7bdat", "sas/data/data_pandas/airline.sas7bdat", "stata/data/too_big/usa_00009.dta", "stata/data/stata14_118.dta", "spss/data/too_big/GSS2024.sav", "spss/data/sample.sav",
         "sas/data/xpt/ACQ_G.xpt", "sas/data/xpt/dates_xpt_v8.xpt", "spss/data/sample.por"]
ok = bad = 0
for f in files:
    p = T + f
    cols = con.sql(f"SELECT * FROM read_prs('{p}') LIMIT 0").columns
    n = len(cols)
    picks = {"reversed-3": [cols[-1], cols[0], cols[n // 2]] if n >= 3 else cols[::-1], "single-last": [cols[-1]], "single-first": [cols[0]], "all-reversed": cols[::-1] if n <= 40 else cols[::-1][:40]}
    for label, sel in picks.items():
        q = ", ".join(f'"{c}"' for c in sel)
        got = con.sql(f"SELECT {q} FROM read_prs('{p}', preserve_order=true)").pl()
        ref = ps.scan_readstat(p, preserve_order=True).select(sel).collect()
        same = got.columns == ref.columns and got.equals(ref)
        ok += same; bad += (not same)
        if not same: print("MISMATCH", f, label, got.shape, ref.shape, flush=True)
    # count(*) and a filter on a column that is not selected
    cnt = con.sql(f"SELECT count(*) FROM read_prs('{p}')").fetchall()[0][0]
    ref_n = ps.scan_readstat(p, preserve_order=True).select(cols[0]).collect().height
    c2 = con.sql(f'SELECT count(*) FROM read_prs(\'{p}\') WHERE "{cols[0]}" IS NOT NULL').fetchall()[0][0]
    c2r = ps.scan_readstat(p, preserve_order=True).select(cols[0]).collect()[cols[0]].is_not_null().sum()
    good = cnt == ref_n and c2 == c2r
    ok += good; bad += (not good)
    if not good: print("COUNT MISMATCH", f, cnt, ref_n, c2, c2r, flush=True)
    # row index mixed with a column subset
    try:
        r = con.sql(f"SELECT ri, \"{cols[-1]}\" FROM read_prs('{p}', preserve_order=true, row_index_name='ri') LIMIT 5").pl()
        good = r["ri"].to_list() == list(range(r.height))
    except Exception as e:
        good = False; print("ROW INDEX ERR", f, str(e)[:120])
    ok += good; bad += (not good)
    print("%-42s cols=%4d  checks done" % (f, n), flush=True)
print("PASS", ok, "FAIL", bad)
sys.exit(1 if bad else 0)
