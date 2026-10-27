# Reading files

```sql
SELECT * FROM read_prs('path/to/file.sas7bdat');
```

`read_prs` is a table function. Use it anywhere you would use a table: in `SELECT`, joins, `CREATE TABLE ... AS`, views, and `COPY ... TO`.

## Formats

The format is taken from the file extension:

| Extension | Format |
|---|---|
| `.sas7bdat` | SAS |
| `.dta` | Stata |
| `.sav`, `.zsav` | SPSS |
| `.xpt`, `.xpt5`, `.xpt8` | SAS transport (version 5 or 8) |
| `.por` | SPSS portable |

If a file has a different or no extension, say what it is with `format`:

```sql
SELECT * FROM read_prs('survey_export', format = 'dta');
```

`format` accepts `'sas7bdat'` (or `'sas'`), `'dta'` (or `'stata'`), `'sav'` (or `'zsav'`, `'spss'`), `'xpt'` (or `'sas_xpt'`) and `'por'` (or `'spss_por'`).

## Reading only what you need

Only the columns your query uses are read from the file, so selecting a few columns out of many is much faster than `SELECT *`.

```sql
SELECT age, income FROM read_prs('survey.sav');   -- reads two columns
SELECT count(*) FROM read_prs('survey.sav');      -- reads very little
```

To read part of a file by rows, use `row_offset` and `n_rows`:

```sql
SELECT * FROM read_prs('survey.dta', row_offset = 1000, n_rows = 500);   -- rows 1001 to 1500
```

## Several files

Each call reads one file. Combine files with `UNION ALL`:

```sql
SELECT * FROM read_prs('wave1.dta')
UNION ALL
SELECT * FROM read_prs('wave2.dta');
```

Use `UNION ALL BY NAME` if the files do not have exactly the same columns in the same order.

## Options

Options are passed by name: `read_prs('file.dta', preserve_order = true)`.

| Option | Default | Applies to | What it does |
|---|---|---|---|
| `format` | from the file extension | all | Force the format. See [Formats](#formats). |
| `preserve_order` | `false` | all except `.por` | Return rows in file order. See [Row order](#row-order). |
| `row_index_name` | none | all | Add a 0-based row number column with this name, as the last column. See [Row order](#row-order). |
| `value_labels_as_strings` | `false` | Stata, SPSS (`.sav`) | Return the text of value labels instead of the numeric codes. See [Value labels](#value-labels). |
| `missing_string_as_null` | `false` | all | Return empty strings as `NULL`. |
| `row_offset` | `0` | all | Skip this many rows from the start of the file. |
| `n_rows` | all rows | all | Read at most this many rows (after `row_offset`). |
| `threads` | DuckDB's `threads` setting | all except `.por` | Number of threads used to decode the file. |
| `batch_size` | automatic | all | Rows decoded per batch (for `.por`, rows returned per batch). Rarely needs changing. |

## Row order

By default, rows can come back in any order. This is faster and uses less memory, and it is the same default as polars_readstat. If your query has an `ORDER BY`, or you do not care about order, nothing more is needed.

To get rows in the order they are stored in the file, set `preserve_order`; that is the option that controls order (`threads` only changes how the file is decoded).

```sql
SELECT * FROM read_prs('survey.dta', preserve_order = true);
```

This costs some speed, most noticeably on files with many rows.

Alternatively, add a row number column and sort on it only when you need to:

```sql
SELECT * EXCLUDE (row)
FROM read_prs('survey.dta', row_index_name = 'row')
ORDER BY row;
```

The row number starts at 0 and counts rows in the file. It is the last column of the result (type `UINTEGER`), and you can select it like any other column.

## Value labels

Stata and SPSS files can attach text labels to numeric codes (for example `1 = Male`, `2 = Female`). By default `read_prs` returns the codes. To get the label text instead:

```sql
SELECT sex FROM read_prs('survey.dta');                                      -- 1, 2, ...
SELECT sex FROM read_prs('survey.dta', value_labels_as_strings = true);      -- 'Male', 'Female', ...
```

Only columns that have a label set in the file change. Other columns are unaffected.

## Missing values

Missing values in SAS, Stata and SPSS files are returned as `NULL`, including SAS special missing values (`.A` to `.Z`).

To also treat empty strings as `NULL`, use `missing_string_as_null = true`.

## Column types

Columns keep the type stored in the file:

| In the file | In DuckDB |
|---|---|
| SAS and SPSS numbers | `DOUBLE` |
| Stata byte, int, long | `TINYINT`, `SMALLINT`, `INTEGER` |
| Stata float, double | `FLOAT`, `DOUBLE` |
| Text | `VARCHAR` |
| Dates | `DATE` |
| Date-times | `TIMESTAMP` (SPSS: `TIMESTAMP_MS`) |
| Times | `TIME_NS` |

Use `DESCRIBE SELECT * FROM read_prs('file.dta')` to see the types of a particular file.

## Remote files (S3)

`read_prs` opens files through DuckDB's own file system, so anything DuckDB's other readers can reach with your credentials works the same way. For S3, the `httpfs` extension is loaded for you the first time it is needed.

Set up credentials the way you would for `read_parquet` or `read_csv`, for example with a secret:

```sql
CREATE SECRET (
    TYPE s3,
    KEY_ID 'my-key-id',
    SECRET 'my-secret',
    REGION 'us-east-1'
);

SELECT * FROM read_prs('s3://my-bucket/path/survey.dta');
```

or with settings:

```sql
SET s3_region = 'us-east-1';
SET s3_access_key_id = 'my-key-id';
SET s3_secret_access_key = 'my-secret';

SELECT * FROM read_prs('s3://my-bucket/path/survey.dta');
```

See DuckDB's documentation for its `httpfs` extension for all the ways to supply credentials. The options on this page work with remote files too.

If credentials are missing or wrong, the query fails with the error from DuckDB's file system.

## Converting to Parquet

`COPY` streams a file into Parquet without holding the whole file in memory:

```sql
COPY (SELECT * FROM read_prs('big.sas7bdat')) TO 'big.parquet' (FORMAT parquet);
```

Memory depends on the number of columns (and how much text they hold), not the number of rows. Rows can be written in any order unless you set `preserve_order = true`. To lower memory on wide tables, use smaller Parquet row groups and fewer DuckDB threads (`SET threads = 4;` and `ROW_GROUP_SIZE 10000` in the `COPY` options); add `preserve_order = true, threads = 1` to keep the order with the least memory. See the [README](https://github.com/jrothbaum/duckdb_prs#converting-files-to-parquet) for the commands and for timings and memory use on large files.

## Errors

| Message | What to do |
|---|---|
| `cannot infer format from extension ...` | The file name does not end in `.sas7bdat`, `.dta`, `.sav`, `.zsav`, `.xpt` or `.por`. Pass `format = '...'`. |
| `file has no columns` | The file contains no variables. |
| `DuckDB VFS open failed for '...'` | The file could not be opened: check the path, and for remote files, your credentials. |

## SAS transport and SPSS portable files

`.xpt` and `.por` files read like the other formats, with a few differences:

- **`.por` is read in one pass.** The whole file is read into memory before any rows are returned, so `threads` and `preserve_order` have no effect (rows are always in file order), and memory is the size of the table. Other formats are read in batches.
- **Names.** Variable names in `.por` files are always upper case.
- **No value labels.** `value_labels_as_strings` does nothing for `.xpt` and `.por` files.
- `missing_string_as_null`, `row_offset`, `n_rows`, `row_index_name` and column selection work as for the other formats.
