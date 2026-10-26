# Writing files

Write the result of any query to a Stata, SPSS or SAS file with `COPY ... TO`:

```sql
COPY (SELECT * FROM survey) TO 'survey.dta' (FORMAT dta);
COPY (SELECT * FROM survey) TO 'survey.sav' (FORMAT sav);
COPY (SELECT * FROM survey) TO 'survey' (FORMAT sas_csv);
```

The source can be a table, a view, or a query, including one that reads another file:

```sql
COPY (SELECT id, age, income FROM read_prs('survey.sas7bdat') WHERE age >= 18)
TO 'adults.dta' (FORMAT dta);
```

## Formats

| `FORMAT` | Writes | Notes |
|---|---|---|
| `dta` | Stata `.dta` | |
| `sav` | SPSS `.sav` | |
| `zsav` | SPSS `.sav` with compression turned on | |
| `sas_csv` | A SAS import bundle | A data file (`.csv`) and a `.sas` script that loads it. See [SAS](#sas). |

The format is chosen from the file name when it ends in `.dta`, `.sav` or `.zsav`, so `COPY survey TO 'survey.dta'` works without `FORMAT`. For `sas_csv`, and for any other file name, give the format. Files can only be written to a local path, not to S3 or other remote locations.

## Options

Options go in the parentheses after the format: `(FORMAT dta, compress true)`.

| Option | Applies to | Default | What it does |
|---|---|---|---|
| `variable_labels` | all | none | Descriptive labels for columns. See [Labels](#labels). |
| `compress` | `dta` | `false` | Write a compressed Stata file. |
| `compressed` | `sav`, `zsav` | `false` for `sav`, `true` for `zsav` | Use SPSS compression. |
| `dataset_name` | `sas_csv` | the file name | Name of the SAS dataset the script creates. |
| `library` | `sas_csv` | none | SAS library to save the dataset in. If not set, the dataset goes to `WORK`. |
| `gzip` | `sas_csv` | `false` | Write `<name>.csv.gz` instead of `<name>.csv`. Needs SAS 9.4 Maintenance 5 or later to read. |
| `delete_csv_on_import` | `sas_csv` | `false` | The script deletes the data file after SAS has loaded it. |

An option that does not exist for the format is an error that lists the options you can use.

## Labels

Give columns descriptive labels with `variable_labels`, a struct of column name to label:

```sql
COPY (SELECT * FROM survey) TO 'survey.dta' (
    FORMAT dta,
    variable_labels {'age': 'Age in years', 'income': 'Household income'}
);
```

Value labels (the text for numeric codes) cannot be written yet.

## Column types

| In DuckDB | In the file |
|---|---|
| `BOOLEAN` | `0` / `1` |
| `TINYINT`, `SMALLINT`, `INTEGER`, `UTINYINT`, `USMALLINT` | integers (`dta` uses the smallest type that holds the values; `sav` stores numbers) |
| `BIGINT`, `UINTEGER`, `UBIGINT`, `HUGEINT` | numbers (doubles) |
| `FLOAT`, `DOUBLE` | numbers |
| `DECIMAL` | numbers (doubles) |
| `VARCHAR` | text |
| `DATE` | dates |
| `TIMESTAMP`, `TIMESTAMP_S`, `TIMESTAMP_MS`, `TIMESTAMP_NS`, `TIMESTAMPTZ` | date-times |
| `TIME` | times |

Other types (`BLOB`, `UUID`, `INTERVAL`, `ENUM`, lists, structs, maps and so on) cannot be written. The error names the column and its type; cast it to something supported in the query:

```sql
COPY (SELECT id, kind::VARCHAR AS kind FROM events) TO 'events.dta' (FORMAT dta);
```

### What to expect

- **Missing values.** `NULL` in a number, date or time column is written as the format's missing value and read back as `NULL`. A `NULL` text value is written as an empty string, because these formats have no missing text; read it back with `missing_string_as_null = true` to get `NULL` again.
- **Date-times.** Stata keeps date-times to the millisecond, SPSS and SAS to the second.
- **Text.** SPSS drops trailing spaces, so a value that is only spaces comes back as an empty string.
- **Column names.** Stata allows only letters, digits and underscores, at most 32 characters, and not starting with a digit. Names that do not fit are changed (`my col` becomes `my_col`, `1abc` becomes `_1abc`, longer names are cut). SPSS keeps names as they are. `sas_csv` changes names to SAS rules, as described [below](#sas).

## SAS

Binary `.sas7bdat` files cannot be written, so `sas_csv` writes two files that you use together:

```sql
COPY (SELECT * FROM survey) TO 'out/survey' (FORMAT sas_csv, dataset_name 'survey');
```

writes `out/survey.csv` with the data and `out/survey.sas` with a SAS program that reads it. In SAS, run the program to create the dataset with the right types, lengths, formats and labels:

```sas
%include "out/survey.sas";
```

The program refers to the data file by its full path, so keep the two files together and run it where that path can be read. With `gzip true`, the data file is `survey.csv.gz`.

Dates are written as SAS dates, and date-times and times as SAS date-times and times. Column and dataset names are changed to valid SAS names (letters, digits and underscores, starting with a letter, at most 32 characters): invalid characters are dropped and a name starting with a digit gets a `d` in front, so `my col` becomes `mycol` and `1abc` becomes `d1abc`. The data file's header uses the changed names.

## Empty results

A query that returns no rows still writes a valid file with all the columns.

## Size and speed

The formats are written as a whole, so the result is held in memory while the file is written. Plan for memory of at least the size of the data, and more for tables with many text columns. Some measurements:

| Table | Rows | Columns | Stata | SPSS | Memory used |
|---|---|---|---|---|---|
| numeric | 9,988,394 | 16 | 7 s | 8 s | about 1.0 to 1.5 GB |
| mostly text | 623,757 | 286 (185 text) | 8 s | 11 s | about 5 GB |

If a result is too large to write in one go, write it as several files, each from part of the source (for example with `row_offset` and `n_rows` on `read_prs`, or a `WHERE` on a key).

## Round trips

A file written with `COPY` can be read back with [`read_prs`](read.md):

```sql
COPY (SELECT * FROM read_prs('survey.sas7bdat')) TO 'survey.dta' (FORMAT dta);
SELECT * FROM read_prs('survey.dta');
```

Value labels and other metadata in the original file are not carried over; only the data and any `variable_labels` you give.
