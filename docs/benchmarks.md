# Benchmarks

`read_prs` is compared with two alternatives that read the same files:

- **polars_readstat**: the Python/Polars library built on the same Rust core.
- **read_stat**: the community DuckDB extension for statistical files.

Everything is relative to `read_prs`, which is 1.00x. A number above 1.00x means that tool took longer than `read_prs` to do the same thing; below 1.00x means it was faster.

## Reading whole files

Each file is read completely (`SELECT *`) into a Polars DataFrame. Times are the best of 2 runs.

| File | Format | Rows | read_prs | polars_readstat | read_stat |
|---|---|---|---|---|---|
| psam_p17 | SAS | 623,757 | 1.00x (1003 ms) | 0.44x | 19.7x |
| nls | SAS | 13,548 | 1.00x (18 ms) | 0.17x | 0.83x |
| numeric_1000000_2 | SAS | 1,000,000 | 1.00x (34 ms) | 0.24x | 20.6x |
| topical | SAS | 84,355 | 1.00x (96 ms) | 0.56x | 9.9x |
| usa_00009 | Stata | 9,988,394 | 1.00x (389 ms) | 0.40x | 44.9x |
| anes_timeseries_cdf | SPSS | 73,745 | 1.00x (1078 ms) | 1.18x | 7.7x |
| GSS2024 | SPSS | 3,309 | 1.00x (118 ms) | 0.75x | 5.7x |
| **All files** | | | **1.00x (2735 ms)** | **0.74x** | **17.5x** |

- `read_prs` is faster than `read_stat` on every file except the smallest one, where both take a few milliseconds.
- polars_readstat reads straight into a DataFrame, so it is usually faster than going through DuckDB. The gap is smallest on the wide SPSS files, where `read_prs` is on par or faster.

## Options and column selection

Some `read_prs` options change the cost of a read. For the three largest files (times in milliseconds):

| File | Whole file | `preserve_order = true` | 3 columns |
|---|---|---|---|
| psam_p17 (SAS, 286 columns) | 1003 | 1321 | 65 |
| usa_00009 (Stata, 10 million rows) | 389 | 627 | 241 |
| anes_timeseries_cdf (SPSS, 1,030 columns) | 1078 | 1211 | 210 |

- Selecting a few columns is much faster than reading a whole file, because only those columns are read.
- Preserving file order costs extra time. If you do not need it, leave `preserve_order` off. See [Row order](read.md#row-order).

## About these numbers

- They come from one machine, with files stored locally. Your numbers will differ; the relative sizes are what to look at.
- `read_stat` has no build for DuckDB 2.0 yet, so it was run on DuckDB 1.5.2 and `read_prs` on DuckDB 2.0.0. polars_readstat was run in both environments and gave the same times, so the two setups are comparable.
- Each measurement ran in its own process, with the file already in the operating system's cache.

## Run it yourself

From the polars_readstat project directory (where `polars_readstat` is installed):

```bash
uv run python /path/to/duckdb_prs/benchmark_compare.py --runs 3 --variants
```

`--variants` adds the `preserve_order` and column-selection measurements. The script runs `read_prs` and `read_stat` in separate DuckDB environments and prints one table.
