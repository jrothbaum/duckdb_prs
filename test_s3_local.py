"""S3 reads through DuckDB's file system, against a local moto S3 server (no AWS account).

Checks that read_prs picks up DuckDB's own S3 configuration (CREATE SECRET and SET s3_*)
and that httpfs is autoloaded. Mirrors polars_readstat's test/aws/test_local_s3_endpoint.py.
Needs network access once, to install DuckDB's httpfs extension.

Run from the polars_readstat project directory:
    uv run --with "duckdb>=2.0.0" --with "moto[server]" --with boto3 python /path/to/duckdb_prs/test_s3_local.py
"""
import socket, subprocess, sys, time, urllib.request
from pathlib import Path

import boto3
import duckdb
import polars_readstat as ps

HERE = Path(__file__).parent
T = str(HERE.parent / "polars_readstat" / "crates" / "polars_readstat_rs" / "tests") + "/"
EXT = HERE / "target" / "release" / "read_prs.duckdb_extension"
FILES = {"topical.sas7bdat": T+"sas/data/too_big/topical.sas7bdat", "usa.dta": T+"stata/data/too_big/usa_00009.dta", "gss.sav": T+"spss/data/too_big/GSS2024.sav"}
with socket.socket() as s:
    s.bind(("127.0.0.1", 0)); port = s.getsockname()[1]
proc = subprocess.Popen([sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
url = f"http://127.0.0.1:{port}"
try:
    for _ in range(100):
        try: urllib.request.urlopen(url, timeout=1); break
        except OSError: time.sleep(0.1)
    c = boto3.client("s3", endpoint_url=url, region_name="us-east-1", aws_access_key_id="testing", aws_secret_access_key="testing")
    c.create_bucket(Bucket="local-bkt")
    for k, p in FILES.items(): c.upload_file(p, "local-bkt", k)
    print("uploaded", list(FILES), flush=True)
    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.execute(f"LOAD '{EXT}'")
    con.execute("INSTALL httpfs")  # LOADed automatically on first s3:// use
    # 1) no credentials at all: must fail cleanly
    try:
        con.sql("SELECT count(*) FROM read_prs('s3://local-bkt/topical.sas7bdat')").fetchall(); print("no-secret read: UNEXPECTEDLY OK")
    except Exception as e: print("no-secret read fails as expected:", str(e).replace("\n"," ")[:110], flush=True)
    # 2) DuckDB's own S3 secret, picked up by read_prs
    con.execute(f"CREATE SECRET (TYPE s3, KEY_ID 'testing', SECRET 'testing', REGION 'us-east-1', ENDPOINT '127.0.0.1:{port}', USE_SSL false, URL_STYLE 'path')")
    for k, p in FILES.items():
        got = con.sql(f"SELECT * FROM read_prs('s3://local-bkt/{k}', preserve_order=true)").pl()
        ref = ps.scan_readstat(p, preserve_order=True).collect()
        print("s3://local-bkt/%-18s rows %7d | matches local polars read: %s" % (k, got.height, got.equals(ref)), flush=True)
    # 3) options still work over S3
    n = con.sql("SELECT count(*) FROM read_prs('s3://local-bkt/topical.sas7bdat', n_rows=100, row_offset=10)").fetchall()[0][0]
    print("n_rows=100,row_offset=10 over s3 ->", n, "rows", flush=True)
    # 4) settings-based config instead of a secret
    con.execute("DROP SECRET IF EXISTS __default_s3")
    con.execute("SET s3_region='us-east-1'; SET s3_access_key_id='testing'; SET s3_secret_access_key='testing'; SET s3_use_ssl=false; SET s3_url_style='path'")
    con.execute(f"SET s3_endpoint='127.0.0.1:{port}'")
    print("settings-based s3 config ->", con.sql("SELECT count(*) FROM read_prs('s3://local-bkt/topical.sas7bdat')").fetchall()[0][0], "rows", flush=True)
finally:
    proc.terminate(); proc.wait(timeout=10)
print("S3_DONE")
