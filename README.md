# duckdb_prs

A DuckDB extension for reading SAS (`.sas7bdat`), Stata (`.dta`) and SPSS (`.sav`/`.zsav`) files directly in SQL, and for writing query results to Stata, SPSS and SAS (CSV plus import script) files.

```sql
SELECT * FROM read_prs('survey.sav') LIMIT 10;

COPY (SELECT * FROM read_prs('survey.sav') WHERE age >= 18) TO 'adults.dta' (FORMAT dta);
```

It reads files with the same Rust core as [polars_readstat](https://github.com/jrothbaum/polars_readstat).

## Why use this?

- **Query statistical files without converting them first.** Select, filter, join and aggregate SAS, Stata and SPSS files like any other DuckDB table, or load them into a table or Parquet with one statement.
- **Fast.** Much faster than the community `read_stat` extension (about 17x faster overall in the [benchmarks](docs/benchmarks.md)), and only the columns your query uses are read, so a few columns out of hundreds is quick.
- **Write as well as read.** `COPY ... TO` writes any query result to Stata or SPSS, or to a CSV plus SAS script that loads it. See [Writing files](docs/write.md).
- **Works with DuckDB's file access.** Read from S3 the same way DuckDB's other readers do, using your DuckDB secrets and settings.
- **Familiar options.** `preserve_order`, `row_index_name`, `value_labels_as_strings`, `missing_string_as_null`, `threads` and `batch_size` follow polars_readstat.

## Install

The extension is built for **DuckDB 2.0.0**.

```bash
cargo install cargo-duckdb-ext-tools    # once
./build.sh                              # produces target/release/read_prs.duckdb_extension
```

You need a recent Rust toolchain (1.95 or newer). The extension is not signed, so DuckDB has to be told to allow unsigned extensions when you load it.

**DuckDB command line**

```bash
duckdb -unsigned
```
```sql
LOAD '/path/to/read_prs.duckdb_extension';
```

**Python**

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": True})
con.execute("LOAD '/path/to/read_prs.duckdb_extension'")
```

## Usage

```sql
-- Read a file; the format comes from the extension (.sas7bdat, .dta, .sav, .zsav)
SELECT * FROM read_prs('survey.dta');

-- Only the columns you select are read from the file
SELECT age, income FROM read_prs('survey.sav') WHERE age >= 18;

-- Load into a table, or convert to Parquet
CREATE TABLE survey AS SELECT * FROM read_prs('survey.sas7bdat');
COPY (SELECT * FROM read_prs('survey.sav')) TO 'survey.parquet' (FORMAT parquet);

-- Part of a file
SELECT * FROM read_prs('survey.dta', row_offset = 1000, n_rows = 500);

-- Combine several files
SELECT * FROM read_prs('wave1.dta') UNION ALL SELECT * FROM read_prs('wave2.dta');
```

### Options

| Option | Default | What it does |
|---|---|---|
| `format` | from file extension | `'sas7bdat'` (or `'sas'`), `'dta'` (or `'stata'`), `'sav'` (or `'zsav'`, `'spss'`). Needed only when the file name has no recognised extension. |
| `preserve_order` | `false` | Return rows in file order. Without it, rows can arrive in any order, which is faster and uses less memory. |
| `row_index_name` | none | Add a 0-based row number column with this name, as the last column. Use it to `ORDER BY` the original order without `preserve_order`. |
| `value_labels_as_strings` | `false` | Stata and SPSS: return the text of value labels instead of the numeric codes. |
| `missing_string_as_null` | `false` | Return empty strings as `NULL`. |
| `row_offset` | `0` | Skip this many rows from the start. |
| `n_rows` | all rows | Read at most this many rows. |
| `threads` | DuckDB's `threads` setting | Number of threads used to decode the file. |
| `batch_size` | automatic | Rows decoded per batch. Rarely needs changing. |

See [Reading files](docs/read.md) for details, [remote files](docs/read.md#remote-files-s3) and column types.

### Writing

```sql
COPY (SELECT * FROM survey) TO 'survey.dta' (FORMAT dta);                  -- Stata
COPY (SELECT * FROM survey) TO 'survey.sav' (FORMAT sav);                  -- SPSS
COPY (SELECT * FROM survey) TO 'survey' (FORMAT sas_csv);                  -- SAS: survey.csv + survey.sas

COPY (SELECT * FROM survey) TO 'survey.dta' (
    FORMAT dta,
    variable_labels {'age': 'Age in years'}
);
```

| Format | Writes | Options |
|---|---|---|
| `dta` | Stata | `compress`, `variable_labels` |
| `sav`, `zsav` | SPSS (`zsav` compresses) | `compressed`, `variable_labels` |
| `sas_csv` | a `.csv` and a `.sas` script that loads it in SAS | `dataset_name`, `library`, `gzip`, `delete_csv_on_import`, `variable_labels` |

Files are written to local paths. See [Writing files](docs/write.md) for column types, labels, what to expect, and size and speed.

## Benchmarks

`read_prs` compared with polars_readstat and the community `read_stat` extension, reading whole files, with `read_prs` as the baseline (lower is faster):

| File | Rows | read_prs | polars_readstat | read_stat |
|---|---|---|---|---|
| SAS | 623,757 | 1.00x (1003 ms) | 0.44x | 19.7x |
| SAS | 84,355 | 1.00x (96 ms) | 0.56x | 9.9x |
| Stata | 9,988,394 | 1.00x (389 ms) | 0.40x | 44.9x |
| SPSS | 73,745 | 1.00x (1078 ms) | 1.18x | 7.7x |
| **All 7 files** | | **1.00x (2735 ms)** | **0.74x** | **17.5x** |

See [Benchmarks](docs/benchmarks.md) for every file, the cost of options such as `preserve_order`, and how to run it yourself.

## Not supported

- One file per `read_prs` call; for several files use `UNION ALL`. File globs such as `'*.dta'` are not supported.
- Writing: local files only, no binary `.sas7bdat` (SAS gets a CSV plus a script), and value labels cannot be written (variable labels can).
- SAS transport (`.xpt`) and SPSS portable (`.por`) files cannot be read or written.
- SAS format catalogs (`.sas7bcat`) and per-column metadata such as variable labels are not available.
