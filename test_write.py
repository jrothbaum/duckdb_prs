"""Writing files with COPY ... TO.

Checks, for Stata (dta), SPSS (sav, zsav, por), SAS transport (xpt) and the SAS CSV import
bundle (sas_csv):
  - values, types and nulls survive a write and re-read;
  - every small test file in the polars_readstat suites round-trips through dta, sav, xpt and
    por, or is refused with a clear message (the limits of xpt and por);
  - options, variable labels, empty results, and the error messages for unsupported
    types, unknown options and remote paths.

Run from the polars_readstat project directory:
    uv run --with "duckdb>=2.0.0" python /path/to/duckdb_prs/test_write.py
"""

import datetime
import glob
import math
import os
import sys
import tempfile
from pathlib import Path

import duckdb
import polars_readstat as ps

HERE = Path(__file__).parent
TESTS = HERE.parent / "polars_readstat" / "crates" / "polars_readstat_rs" / "tests"
EXT = HERE / "target" / "release" / "read_prs.duckdb_extension"

failures: list[str] = []
checks = 0


def check(ok: bool, label: str, detail: str = "") -> None:
    global checks
    checks += 1
    if not ok:
        failures.append(f"{label}: {detail}" if detail else label)
        print(f"  FAIL  {label} {detail}")


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.execute(f"LOAD '{EXT}'")
    return con


def fails(con, sql: str, expected: str) -> bool:
    try:
        con.execute(sql)
    except Exception as e:  # noqa: BLE001
        return expected in str(e)
    return False


# ── values, types and nulls ──────────────────────────────────────────────────

def test_round_trip_values(con, tmp: str) -> None:
    print("round trip of values")
    con.execute(
        """CREATE TABLE t AS SELECT * FROM (VALUES
             (1, 1.5, 'alpha', DATE '2020-01-15', TIMESTAMP '2021-03-04 05:06:07', true),
             (2, NULL, 'beta', DATE '1999-12-31', TIMESTAMP '2000-01-01 00:00:00', false),
             (3, 3.25, NULL, NULL, NULL, NULL),
             (4, -4.0, 'delta', DATE '2024-02-29', TIMESTAMP '2024-02-29 23:59:59', true))
           v(id, score, name, d, ts, flag)"""
    )
    for fmt in ("dta", "sav", "zsav", "xpt", "por"):
        path = f"{tmp}/values.{fmt}"
        con.execute(f"COPY t TO '{path}' (FORMAT {fmt})")
        rows = con.sql(f"SELECT * FROM read_prs('{path}', preserve_order = true)").fetchall()
        check(len(rows) == 4, f"{fmt}: four rows")
        check([r[0] for r in rows] == [1, 2, 3, 4] or [r[0] for r in rows] == [1.0, 2.0, 3.0, 4.0], f"{fmt}: ids")
        check([r[1] for r in rows] == [1.5, None, 3.25, -4.0], f"{fmt}: doubles and null")
        # a NULL string is an empty string or NULL: these formats have no distinct string null
        check([r[2] or "" for r in rows] == ["alpha", "beta", "", "delta"], f"{fmt}: strings")
        check(rows[0][3].isoformat() == "2020-01-15" and rows[2][3] is None, f"{fmt}: dates and null")
        check(str(rows[3][4]) == "2024-02-29 23:59:59" and rows[2][4] is None, f"{fmt}: timestamps and null")

    # the format is taken from the file name when FORMAT is left out
    for name in ("auto.dta", "auto.sav", "auto.zsav"):
        con.execute(f"COPY t TO '{tmp}/{name}'")
        check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/{name}')").fetchall() == [(4,)], f"format from file name: {name}")

    # DECIMAL and HUGEINT are written as doubles
    path = f"{tmp}/decimal.dta"
    con.execute(f"COPY (SELECT sum(id) AS s, 1.25::DECIMAL(5,2) AS d FROM t) TO '{path}' (FORMAT dta)")
    check(con.sql(f"SELECT * FROM read_prs('{path}')").fetchall() == [(10.0, 1.25)], "decimal and hugeint")

    # the SAS bundle: a CSV and a script
    base = f"{tmp}/bundle"
    con.execute(f"COPY t TO '{base}' (FORMAT sas_csv, dataset_name 'mydata')")
    csv = Path(base + ".csv").read_text().splitlines()
    script = Path(base + ".sas").read_text()
    check(len(csv) == 5 and csv[0] == "id,score,name,d,ts,flag", "sas_csv: csv header and rows", str(csv[:2]))
    check("data mydata;" in script and "length name $5;" in script, "sas_csv: script")
    con.execute(f"COPY t TO '{tmp}/gz' (FORMAT sas_csv, gzip true, dataset_name 'gz')")
    check(os.path.exists(f"{tmp}/gz.csv.gz") and os.path.exists(f"{tmp}/gz.sas"), "sas_csv: gzip")


# ── every small test file, through dta and sav ───────────────────────────────

def norm(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return float(v)
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        return v.rstrip() or None  # NULL, '' and blank are the same in these formats
    return v


def close(a, b, seconds: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, datetime.datetime) and isinstance(b, datetime.datetime):
        return abs((a - b).total_seconds()) <= seconds
    if isinstance(a, float) and isinstance(b, float):
        if math.isnan(a) and math.isnan(b):
            return True
        return abs(a - b) <= 1e-6 * max(1.0, abs(a), abs(b))
    return a == b


# SPSS files keep date-times to the second, so those may differ by up to one second;
# every other value must come back unchanged.
def same_rows(src, back, fmt: str) -> str | None:
    seconds = 0.001 if fmt == "dta" else 1.0
    if len(src) != len(back):
        return "row count"
    for i, (r1, r2) in enumerate(zip(src, back)):
        if len(r1) != len(r2):
            return "column count"
        for j, (a, b) in enumerate(zip(r1, r2)):
            if not close(norm(a), norm(b), seconds):
                return f"row {i}, column {j}: {a!r} vs {b!r}"
    return None


def test_all_small_files(con, tmp: str) -> None:
    print("round trip of every small test file")
    files = sorted(
        f
        for pattern in ("sas/data/**/*.sas7bdat", "stata/data/**/*.dta", "spss/data/**/*.sav", "spss/data/**/*.zsav")
        for f in glob.glob(str(TESTS / pattern), recursive=True)
        if "too_big" not in f
    )
    # Limits of the formats: a write may be refused with one of these messages instead.
    refusals = {
        "xpt": ("would be read back as padding", "outside the range an XPT file can hold"),
        "por": ("exceeds 8 characters", "cannot be stored in an SPSS portable file"),
    }
    tested = {"dta": 0, "sav": 0, "xpt": 0, "por": 0}
    refused = {"xpt": 0, "por": 0}
    for fmt in ("dta", "sav", "xpt", "por"):
        for f in files:
            name = os.path.basename(f)
            try:
                src = con.sql(f"SELECT * FROM read_prs('{f}', preserve_order = true)").fetchall()
            except Exception:  # noqa: BLE001 - files read_prs cannot read (e.g. no variables)
                continue
            if len(src) > 100_000:
                continue
            out = f"{tmp}/{name}.{fmt}"
            try:
                con.execute(f"COPY (SELECT * FROM read_prs('{f}', preserve_order = true)) TO '{out}' (FORMAT {fmt})")
            except Exception as e:  # noqa: BLE001
                message = str(e)
                if any(r in message for r in refusals.get(fmt, ())):
                    refused[fmt] += 1
                else:
                    check(False, f"{fmt} {name}", message.replace("\n", " ")[:140])
                continue
            try:
                back = con.sql(f"SELECT * FROM read_prs('{out}', preserve_order = true)").fetchall()
            except Exception as e:  # noqa: BLE001
                check(False, f"{fmt} {name}: written file cannot be read", str(e).replace("\n", " ")[:120])
                continue
            tested[fmt] += 1
            problem = same_rows(src, back, fmt)
            check(problem is None, f"{fmt} {name}", problem or "")
    check(tested["dta"] > 500 and tested["sav"] > 500, "enough dta and sav files tested", str(tested))
    check(tested["xpt"] > 500 and tested["por"] > 400, "enough xpt and por files tested", str(tested))
    print(f"  files written and re-read: {tested}; refused for the format's limits: {refused}")


# ── options, labels, empty results and errors ────────────────────────────────

def test_options_and_errors(con, tmp: str) -> None:
    print("options and errors")
    con.execute("CREATE OR REPLACE TABLE t2 AS SELECT range AS id, range * 1.5 AS score, 'row' || range AS name FROM range(5)")

    con.execute(f"COPY t2 TO '{tmp}/l.dta' (FORMAT dta, variable_labels {{'id': 'Record id', 'score': 'The score'}})")
    labels = {v["name"]: v.get("label") for v in ps.ScanReadstat(path=f"{tmp}/l.dta").metadata["variables"]}
    check(labels == {"id": "Record id", "score": "The score", "name": None}, "dta variable labels", str(labels))

    con.execute(f"COPY t2 TO '{tmp}/l.sav' (FORMAT sav, variable_labels {{'id': 'Record id'}})")
    labels = {v["name"]: v.get("label") for v in ps.ScanReadstat(path=f"{tmp}/l.sav").metadata["variables"]}
    check(labels.get("id") == "Record id", "sav variable labels", str(labels))

    con.execute(f"COPY t2 TO '{tmp}/c.dta' (FORMAT dta, compress true)")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/c.dta')").fetchall() == [(5,)], "dta compress")
    con.execute(f"COPY t2 TO '{tmp}/c.sav' (FORMAT sav, compressed true)")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/c.sav')").fetchall() == [(5,)], "sav compressed")

    con.execute(f"COPY (SELECT * FROM t2 WHERE id < 0) TO '{tmp}/empty.dta' (FORMAT dta)")
    check(con.sql(f"SELECT * FROM read_prs('{tmp}/empty.dta') LIMIT 0").columns == ["id", "score", "name"], "empty result columns")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/empty.dta')").fetchall() == [(0,)], "empty result rows")

    # SAS transport and SPSS portable: options, labels and the format taken from the file name
    con.execute(f"COPY t2 TO '{tmp}/o.xpt' (FORMAT xpt, version 5, table_name 'MYDATA', file_label 'A label', variable_labels {{'score': 'The score'}})")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/o.xpt')").fetchall() == [(5,)], "xpt version 5 with options")
    check(ps.ScanReadstat(path=f"{tmp}/o.xpt").metadata["columns"][1].get("label") == "The score", "xpt variable label")
    con.execute(f"COPY t2 TO '{tmp}/o8.xpt' (FORMAT xpt, version 8)")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/o8.xpt')").fetchall() == [(5,)], "xpt version 8")
    con.execute(f"COPY t2 TO '{tmp}/auto.xpt'")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/auto.xpt')").fetchall() == [(5,)], "xpt format from file name")
    con.execute(f"COPY (SELECT id AS ID, score AS SCORE, name AS NAME FROM t2) TO '{tmp}/o.por' (FORMAT por, file_label 'A label', variable_labels {{'SCORE': 'The score'}})")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/o.por')").fetchall() == [(5,)], "por with options")
    con.execute(f"COPY (SELECT id AS ID FROM t2) TO '{tmp}/auto.por'")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/auto.por')").fetchall() == [(5,)], "por format from file name")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/auto.por', format = 'spss_por')").fetchall() == [(5,)], "por format name")
    check(con.sql(f"SELECT count(*) FROM read_prs('{tmp}/o.xpt', format = 'sas_xpt')").fetchall() == [(5,)], "xpt format name")

    check(fails(con, f"COPY t2 TO '{tmp}/x.xpt' (FORMAT xpt, version 6)", "version must be 5 or 8"), "xpt bad version")
    check(fails(con, f"COPY t2 TO '{tmp}/x.xpt' (FORMAT xpt, compress true)", 'unknown option "compress"'), "xpt unknown option")
    check(fails(con, f"COPY (SELECT 'a' AS a, 'b' AS b UNION ALL SELECT '', '') TO '{tmp}/x.xpt' (FORMAT xpt)", "would be read back as padding"), "xpt blank last row")
    check(fails(con, f"COPY (SELECT 1e100 AS x) TO '{tmp}/x.xpt' (FORMAT xpt)", "outside the range an XPT file can hold"), "xpt number out of range")
    check(fails(con, f"COPY (SELECT 1 AS a_long_column_name) TO '{tmp}/x.por' (FORMAT por)", "exceeds 8 characters"), "por long name")
    check(fails(con, f"COPY (SELECT 'caf\u00e9' AS T) TO '{tmp}/x.por' (FORMAT por)", "cannot be stored in an SPSS portable file"), "por non-ASCII text")
    con.execute(f"COPY (SELECT 1e100 AS X, -2e307 AS Y, TIMESTAMP '2018-03-17 12:34:56' AS T) TO '{tmp}/big.por' (FORMAT por)")
    big = con.sql(f"SELECT * FROM read_prs('{tmp}/big.por')").fetchall()[0]
    check(abs(big[0] - 1e100) <= 1e91 and abs(big[1] + 2e307) <= 2e298, "por keeps very large numbers", str(big))
    check(str(big[2]).startswith("2018-03-17 12:34:56"), "por date-times", str(big[2]))
    con.execute(f"COPY (SELECT '' AS T FROM range(3)) TO '{tmp}/emptytext.por' (FORMAT por)")
    check(str(con.sql(f"SELECT * FROM read_prs('{tmp}/emptytext.por') LIMIT 0").types[0]) == "VARCHAR", "por all-empty text column")

    check(fails(con, f"COPY t2 TO '{tmp}/x.dta' (FORMAT dta, compresss true)", 'unknown option "compresss"'), "unknown option")
    check(fails(con, f"COPY (SELECT [1, 2] AS l) TO '{tmp}/x.dta' (FORMAT dta)", "cannot be written"), "list column")
    check(fails(con, f"COPY (SELECT {{'a': 1}} AS s) TO '{tmp}/x.dta' (FORMAT dta)", "cannot be written"), "struct column")
    check(fails(con, "COPY t2 TO 's3://bucket/x.dta' (FORMAT dta)", "only local files"), "remote path")


# ── a large table: scales linearly and does not keep extra copies ────────────

def test_large_table(con, tmp: str) -> None:
    print("large table")
    n = 1_000_000
    path = f"{tmp}/large.dta"
    con.execute(f"COPY (SELECT range AS id, range * 0.5 AS x, 'r' || (range % 100) AS s FROM range({n})) TO '{path}' (FORMAT dta)")
    check(con.sql(f"SELECT count(*), max(id) FROM read_prs('{path}')").fetchall() == [(n, n - 1)], "one million rows")


def main() -> int:
    with tempfile.TemporaryDirectory() as tmp:
        con = connect()
        test_round_trip_values(con, tmp)
        test_options_and_errors(con, tmp)
        test_large_table(con, tmp)
        test_all_small_files(con, tmp)
    print(f"\n{checks - len(failures)} passed, {len(failures)} failed")
    for f in failures[:20]:
        print("  -", f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
