# duckdb_prs

A DuckDB extension for reading SAS (`.sas7bdat`), Stata (`.dta`) and SPSS (`.sav`/`.zsav`) files directly in SQL, and for writing query results to Stata, SPSS and SAS (CSV plus import script) files. It also reads and writes the less common SAS transport (`.xpt`) and SPSS portable (`.por`) formats.

```sql
SELECT * FROM read_prs('survey.sav') LIMIT 10;

COPY (SELECT * FROM read_prs('survey.sav') WHERE age >= 18) TO 'adults.dta' (FORMAT dta);
```

It reads files with the same Rust core as [polars_readstat](https://github.com/jrothbaum/polars_readstat).

## Why use this?

- **Query statistical files without converting them first.** Select, filter, join and aggregate SAS, Stata and SPSS files like any other DuckDB table, or load them into a table or Parquet with one statement.
- **Fast.** It uses polars_readstat's Rust engine, which is faster than ReadStat-based tools such as pyreadstat and the `read_stat` DuckDB extension (see polars_readstat's [benchmarks](https://github.com/jrothbaum/polars_readstat/blob/master/BENCHMARKS.md) and the [timings below](#benchmarks)). Only the columns your query uses are read, so a few columns out of hundreds is quick.
- **Write as well as read.** `COPY ... TO` writes any query result to Stata or SPSS, or to a CSV plus SAS script that loads it. See [Writing files](docs/write.md).
- **Works with DuckDB's file access.** Read from S3 the same way DuckDB's other readers do, using your DuckDB secrets and settings.
- **Familiar options.** `preserve_order`, `row_index_name`, `value_labels_as_strings`, `missing_string_as_null`, `threads` and `batch_size` follow polars_readstat.

## Install

The extension is built for **DuckDB 2.0.0**.

```bash
git clone --recurse-submodules https://github.com/jrothbaum/duckdb_prs
cd duckdb_prs
make configure
make release          # produces build/release/read_prs.duckdb_extension
```

You need a recent Rust toolchain (1.95 or newer), Python 3, `make` and `git`. The extension is not signed, so DuckDB has to be told to allow unsigned extensions when you load it.

**DuckDB command line**

```bash
duckdb -unsigned
```
```sql
LOAD '/path/to/duckdb_prs/build/release/read_prs.duckdb_extension';
```

**Python**

```python
import duckdb

con = duckdb.connect(config={"allow_unsigned_extensions": True})
con.execute("LOAD '/path/to/duckdb_prs/build/release/read_prs.duckdb_extension'")
```

## Usage

```sql
-- Read a file; the format comes from the extension (.sas7bdat, .dta, .sav, .zsav, .xpt, .por)
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
| `format` | from file extension | `'sas7bdat'` (or `'sas'`), `'dta'` (or `'stata'`), `'sav'` (or `'zsav'`, `'spss'`), `'xpt'` (or `'sas_xpt'`), `'por'` (or `'spss_por'`). Needed only when the file name has no recognised extension. |
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
| `xpt` | SAS transport | `version`, `table_name`, `file_label`, `variable_labels` |
| `por` | SPSS portable | `file_label`, `variable_labels` |

Files are written to local paths. See [Writing files](docs/write.md) for column types, labels, what to expect, and size and speed. The SAS transport and SPSS portable formats have limits of their own (for example, `por` allows names of 8 characters and ASCII text only), described on that page.

## Converting files to Parquet

```sql
COPY (SELECT * FROM read_prs('big.sas7bdat')) TO 'big.parquet' (FORMAT parquet);
```

The file is streamed: rows are decoded in batches and written as they arrive, so memory depends on how wide the table is, not on how many rows it has. By default rows can be written in any order, which is the fastest. To keep the order of the original file, add `preserve_order = true`.

**Lowering memory.** Tables with many text columns use the most. Smaller Parquet row groups and fewer DuckDB threads lower it:

```sql
SET threads = 4;
COPY (SELECT * FROM read_prs('big.sas7bdat'))
TO 'big.parquet' (FORMAT parquet, ROW_GROUP_SIZE 10000);
```

To keep the file order and use the least memory, also decode on one thread:

```sql
COPY (SELECT * FROM read_prs('big.sas7bdat', preserve_order = true, threads = 1))
TO 'big.parquet' (FORMAT parquet, ROW_GROUP_SIZE 10000);
```

Time and peak memory on one machine (peak memory is the highest resident memory of the process):

| File | Settings | Time | Peak memory |
|---|---|---|---|
| Stata, 9,988,394 rows × 16 columns | default | 0.7 s | 0.9 GB |
| | `ROW_GROUP_SIZE 10000` | 0.7 s | 0.6 GB |
| | `preserve_order`, `threads = 1`, `ROW_GROUP_SIZE 10000` | 2.7 s | 0.5 GB |
| SAS, 623,757 rows × 286 columns (185 text) | default | 6.8 s | 3.0 GB |
| | `SET threads = 4`, `ROW_GROUP_SIZE 10000` | 1.8 s | 0.9 GB |
| | `SET threads = 2`, `ROW_GROUP_SIZE 10000` | 2.9 s | 0.6 GB |
| | `preserve_order`, `threads = 1`, `ROW_GROUP_SIZE 10000` | 8.4 s | 0.5 GB |

## Benchmarks

Time to read whole files into a Polars DataFrame, in milliseconds, for `read_prs`, polars_readstat itself, and the `read_stat` DuckDB extension (which is built on the ReadStat library). The figure in brackets is each tool's time relative to `read_prs`.

| File | Format | Rows | read_prs | polars_readstat | read_stat |
|---|---|---|---|---|---|
| psam_p17 | SAS | 623,757 | 1,003 ms | 438 ms (0.44x) | 19,800 ms (19.7x) |
| nls | SAS | 13,548 | 18 ms | 3 ms (0.17x) | 15 ms (0.83x) |
| numeric_1000000_2 | SAS | 1,000,000 | 34 ms | 8 ms (0.24x) | 699 ms (20.6x) |
| topical | SAS | 84,355 | 96 ms | 54 ms (0.56x) | 950 ms (9.9x) |
| usa_00009 | Stata | 9,988,394 | 389 ms | 156 ms (0.40x) | 17,484 ms (44.9x) |
| anes_timeseries_cdf | SPSS | 73,745 | 1,078 ms | 1,277 ms (1.18x) | 8,311 ms (7.7x) |
| GSS2024 | SPSS | 3,309 | 118 ms | 88 ms (0.75x) | 676 ms (5.7x) |
| **All files** | | | **2,735 ms** | **2,024 ms (0.74x)** | **47,936 ms (17.5x)** |

These come from one machine with the files stored locally, so look at the relative sizes rather than the exact times. `read_stat` has no DuckDB 2.0 build yet, so it was run on DuckDB 1.5.2. The comparison of the engine itself with pyreadstat, another ReadStat-based tool, is in polars_readstat's [benchmarks](https://github.com/jrothbaum/polars_readstat/blob/master/BENCHMARKS.md). See [Benchmarks](docs/benchmarks.md) here for the cost of options such as `preserve_order` and of selecting only a few columns, and for how to run these timings yourself.

## Not supported

- One file per `read_prs` call; for several files use `UNION ALL`. File globs such as `'*.dta'` are not supported.
- Writing: local files only, no binary `.sas7bdat` (SAS gets a CSV plus a script), and value labels cannot be written (variable labels can).
- SAS format catalogs (`.sas7bcat`) and per-column metadata such as variable labels are not available.

## License

Apache License 2.0. See [LICENSE](LICENSE).
