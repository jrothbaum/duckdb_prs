# duckdb_prs

A DuckDB extension for reading SAS (`.sas7bdat`), Stata (`.dta`) and SPSS (`.sav`/`.zsav`) files directly in SQL, and for writing query results to Stata, SPSS and SAS (CSV plus import script) files. It also reads and writes the less common SAS transport (`.xpt`) and SPSS portable (`.por`) formats.

```sql
SELECT * FROM read_prs('survey.sav') LIMIT 10;

COPY (SELECT * FROM read_prs('survey.sav') WHERE age >= 18) TO 'adults.dta' (FORMAT dta);
```

It reads files with the same Rust core as [polars_readstat](https://github.com/jrothbaum/polars_readstat).

## Why use this?

- **Query statistical files without converting them first.** Select, filter, join and aggregate them like any other DuckDB table, or load them into a table or Parquet with one statement.
- **Write as well as read.** `COPY ... TO` writes any query result to Stata or SPSS, or to a CSV plus a SAS script that loads it. See [Writing files](write.md).
- **Fast.** Much faster than the community `read_stat` extension, and only the columns your query uses are read. See [Benchmarks](benchmarks.md).
- **Works with DuckDB's file access.** Read from S3 using your DuckDB secrets and settings. See [Remote files](read.md#remote-files-s3).
- **Familiar options.** `preserve_order`, `row_index_name`, `value_labels_as_strings` and the other options follow polars_readstat. See [Reading files](read.md).

## Install

The extension is built for **DuckDB 2.0.0**.

```bash
cargo install cargo-duckdb-ext-tools    # once
./build.sh                              # produces target/release/read_prs.duckdb_extension
```

You need a recent Rust toolchain (1.95 or newer). The extension is not signed, so allow unsigned extensions when you load it.

=== "DuckDB command line"

    ```bash
    duckdb -unsigned
    ```
    ```sql
    LOAD '/path/to/read_prs.duckdb_extension';
    ```

=== "Python"

    ```python
    import duckdb

    con = duckdb.connect(config={"allow_unsigned_extensions": True})
    con.execute("LOAD '/path/to/read_prs.duckdb_extension'")
    ```

## Quick start

```sql
SELECT * FROM read_prs('survey.dta');

SELECT age, income FROM read_prs('survey.sav') WHERE age >= 18;

CREATE TABLE survey AS SELECT * FROM read_prs('survey.sas7bdat');

COPY (SELECT * FROM read_prs('survey.sav')) TO 'survey.parquet' (FORMAT parquet);

-- Write a query result as a Stata file
COPY (SELECT * FROM survey WHERE age >= 18) TO 'adults.dta' (FORMAT dta);
```

Next: [Reading files](read.md) and [Writing files](write.md).
