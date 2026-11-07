//! `COPY ... TO` formats: write query results as Stata, SPSS or SAS files.
//!
//! ```sql
//! COPY (SELECT ...) TO 'out.dta' (FORMAT dta);
//! COPY (SELECT ...) TO 'out.sav' (FORMAT sav);
//! COPY (SELECT ...) TO 'out/survey' (FORMAT sas_csv, dataset_name 'survey');
//! ```
//!
//! The format writers need the whole table, so the query result is collected as
//! Arrow batches while DuckDB flushes it and written when the copy finishes.

use duckdb_neo::{
    Error, Result,
    arrow::{ArrowArray as DuckArray, ArrowExporter, ArrowSchema as DuckSchema},
    column_data_collection::ColumnDataCollection,
    connection::Context,
    copy_function::{CopyFunctionBuilder, CopyToBindInfo, CopyToFunctionCallbacks},
    extension::Extension,
    logical_type::{LogicalType, LogicalTypeID},
    value::Value,
};
use polars_readstat_rs::arrow_write::{
    ArrowArray, ArrowSchema, ArrowTableBuilder, ArrowWriteFormat, ArrowWriteOptions,
};
use std::{collections::HashMap, path::PathBuf, sync::Mutex};

/// Which file format a registered `COPY` format writes.
#[derive(Clone, Copy)]
enum Kind {
    Stata,
    Spss,
    /// SPSS with standard compression on by default.
    SpssCompressed,
    SasCsv,
    /// SAS transport (`.xpt`).
    Xpt,
    /// SPSS portable (`.por`).
    Por,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Kind::Stata => "dta",
            Kind::Spss => "sav",
            Kind::SpssCompressed => "zsav",
            Kind::SasCsv => "sas_csv",
            Kind::Xpt => "xpt",
            Kind::Por => "por",
        }
    }

    /// Option names this format accepts, lowercase.
    fn options(self) -> &'static [&'static str] {
        match self {
            Kind::Stata => &["compress", "variable_labels"],
            Kind::Spss | Kind::SpssCompressed => &["compressed", "variable_labels"],
            Kind::SasCsv => &["dataset_name", "library", "gzip", "delete_csv_on_import", "variable_labels"],
            Kind::Xpt => &["version", "table_name", "file_label", "variable_labels"],
            Kind::Por => &["file_label", "variable_labels"],
        }
    }
}

fn err(message: impl Into<String>) -> Error {
    Error::invalid_input(message.into())
}

/// Whether a column of this type can be written, and what to tell the user if not.
fn check_column_type(kind: Kind, name: &str, id: LogicalTypeID, text: &str) -> Result<()> {
    use LogicalTypeID as T;
    let supported = matches!(
        id,
        T::Boolean
            | T::TinyInt
            | T::SmallInt
            | T::Integer
            | T::BigInt
            | T::UTinyInt
            | T::USmallInt
            | T::UInteger
            | T::UBigInt
            | T::Float
            | T::Double
            | T::Decimal
            | T::HugeInt
            | T::Varchar
            | T::Date
            | T::Time
            | T::TimeNs
            | T::TimestampSec
            | T::TimestampMs
            | T::Timestamp
            | T::TimestampNs
            | T::TimestampTz
    );
    if supported {
        Ok(())
    } else {
        Err(err(format!(
            "FORMAT {}: column \"{name}\" has type {text}, which cannot be written. Cast it to a supported type \
             (for example DOUBLE for numbers or VARCHAR for text).",
            kind.name()
        )))
    }
}

// ── option parsing ────────────────────────────────────────────────────────────

fn as_bool(name: &str, value: &Value) -> Result<bool> {
    value
        .get::<bool>()
        .ok()
        .flatten()
        .ok_or_else(|| err(format!("option {name} must be true or false")))
}

fn as_int(name: &str, value: &Value) -> Result<i64> {
    value
        .get::<i64>()
        .ok()
        .flatten()
        .ok_or_else(|| err(format!("option {name} must be a whole number")))
}

fn as_string(name: &str, value: &Value) -> Result<String> {
    value
        .get::<String>()
        .ok()
        .flatten()
        .ok_or_else(|| err(format!("option {name} must be a string")))
}

/// `variable_labels` is a struct of column name to label text.
fn as_labels(value: &Value) -> Result<HashMap<String, String>> {
    let names: Vec<String> = value
        .logical_type()?
        .get_params()?
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    let children = value.children()?;
    if names.len() != children.len() {
        return Err(err("option variable_labels must be a struct such as {'age': 'Age in years'}"));
    }
    names
        .into_iter()
        .zip(children)
        .map(|(name, child)| {
            let label = child
                .get::<String>()
                .ok()
                .flatten()
                .ok_or_else(|| err(format!("the label for column \"{name}\" must be a string")))?;
            Ok((name, label))
        })
        .collect()
}

struct Parsed {
    format: ArrowWriteFormat,
    options: ArrowWriteOptions,
}

fn parse_options(kind: Kind, raw: HashMap<String, Value>) -> Result<Parsed> {
    let known = kind.options();
    let mut options: HashMap<String, Value> = HashMap::new();
    for (name, value) in raw {
        let lower = name.to_lowercase();
        if !known.contains(&lower.as_str()) {
            return Err(err(format!(
                "FORMAT {}: unknown option \"{name}\". Options for this format: {}.",
                kind.name(),
                known.join(", ")
            )));
        }
        options.insert(lower, value);
    }
    let get_bool = |name: &str, default: bool| -> Result<bool> {
        options.get(name).map_or(Ok(default), |v| as_bool(name, v))
    };
    let get_string = |name: &str| -> Result<Option<String>> {
        options.get(name).map(|v| as_string(name, v)).transpose()
    };

    let variable_labels = match options.get("variable_labels") {
        Some(v) => as_labels(v)?,
        None => HashMap::new(),
    };
    let format = match kind {
        Kind::Stata => ArrowWriteFormat::Stata { compress: get_bool("compress", false)? },
        Kind::Spss => ArrowWriteFormat::Spss { compressed: get_bool("compressed", false)? },
        Kind::SpssCompressed => ArrowWriteFormat::Spss { compressed: get_bool("compressed", true)? },
        Kind::Xpt => {
            let version = options.get("version").map_or(Ok(8), |v| as_int("version", v))?;
            if version != 5 && version != 8 {
                return Err(err(format!("FORMAT xpt: version must be 5 or 8, not {version}")));
            }
            ArrowWriteFormat::Xpt {
                version: version as u8,
                table_name: get_string("table_name")?,
                file_label: get_string("file_label")?,
            }
        }
        Kind::Por => ArrowWriteFormat::Por { file_label: get_string("file_label")? },
        Kind::SasCsv => ArrowWriteFormat::SasCsvImport {
            dataset_name: get_string("dataset_name")?,
            library: get_string("library")?,
            gzip: get_bool("gzip", false)?,
            delete_csv_on_import: get_bool("delete_csv_on_import", false)?,
        },
    };
    Ok(Parsed { format, options: ArrowWriteOptions { variable_labels } })
}

// ── COPY TO ───────────────────────────────────────────────────────────────────

pub struct WriteFormat {
    kind: Kind,
}

pub struct BindData {
    /// The result's columns, kept to describe an empty result.
    columns: Vec<(String, LogicalType)>,
    format: ArrowWriteFormat,
    options: ArrowWriteOptions,
}

pub struct InitData {
    path: PathBuf,
    /// The table being built from the flushed batches; created with the first batch.
    table: Mutex<Option<ArrowTableBuilder>>,
}

impl CopyToFunctionCallbacks for WriteFormat {
    type InitData = InitData;
    type BindData = BindData;
    type BatchData = ();

    fn bind(&self, _context: &Context, bind_info: &CopyToBindInfo) -> Result<Self::BindData> {
        let mut columns = Vec::new();
        for i in 0..bind_info.column_count()? {
            let (name, logical_type) = bind_info.get_column(i)?;
            check_column_type(self.kind, name, logical_type.id()?, &logical_type.to_text()?)?;
            columns.push((name.to_string(), logical_type.clone()));
        }
        let Parsed { format, options } = parse_options(self.kind, bind_info.options()?)?;
        Ok(BindData { columns, format, options })
    }

    fn init(&self, _context: &Context, _bind: &Self::BindData, file_path: &str) -> Result<Self::InitData> {
        if file_path.contains("://") {
            return Err(err(format!(
                "FORMAT {}: only local files can be written, not '{file_path}'",
                self.kind.name()
            )));
        }
        Ok(InitData { path: PathBuf::from(file_path), table: Mutex::new(None) })
    }

    fn batch(
        &self,
        _context: &Context,
        _bind: &Self::BindData,
        _init: &Self::InitData,
        _input: &ColumnDataCollection<'_>,
    ) -> Result<Self::BatchData> {
        Ok(())
    }

    fn flush(
        &self,
        context: &Context,
        bind: &Self::BindData,
        init: &Self::InitData,
        input: &ColumnDataCollection<'_>,
        _batch: &mut Self::BatchData,
    ) -> Result<()> {
        let columns: Vec<(&str, _)> = bind.columns.iter().map(|(n, t)| (n.as_str(), t.clone())).collect();

        let mut exporter = ArrowExporter::new(context, &columns, None)?;
        let mut scan = input.to_scan(context)?;
        while let Some(chunk) = scan.next_chunk()? {
            exporter.append(&chunk, false)?;
        }
        exporter.flush()?;

        // Convert each batch into the table as it arrives, so the Arrow copy of earlier
        // batches is released instead of being held until the end.
        let mut table = init.table.lock().map_err(|_| err("write state poisoned"))?;
        if table.is_none() {
            *table = Some(builder_for(exporter.schema()?)?);
        }
        let builder = table.as_mut().expect("just created");
        while let Some(array) = exporter.next_array()? {
            builder.push(into_polars_array(array)).map_err(|e| err(e.to_string()))?;
        }
        Ok(())
    }

    fn finalize(&self, context: &Context, bind: &Self::BindData, init: &Self::InitData) -> Result<()> {
        let table = init.table.lock().map_err(|_| err("write state poisoned"))?.take();
        let builder = match table {
            Some(builder) => builder,
            // No rows were flushed: describe the empty result from the bound columns.
            None => {
                let columns: Vec<(&str, _)> = bind.columns.iter().map(|(n, t)| (n.as_str(), t.clone())).collect();
                builder_for(ArrowExporter::new(context, &columns, None)?.schema()?)?
            }
        };
        builder
            .write(&init.path, bind.format.clone(), bind.options.clone())
            .map(|_| ())
            .map_err(|e| err(format!("FORMAT {}: {e}", self.kind.name())))
    }
}

fn builder_for(schema: DuckSchema) -> Result<ArrowTableBuilder> {
    ArrowTableBuilder::new(into_polars_schema(schema)).map_err(|e| err(e.to_string()))
}

// The DuckDB and polars Arrow structs follow the same C Data Interface layout.
fn into_polars_schema(schema: DuckSchema) -> ArrowSchema {
    unsafe { std::mem::transmute(schema.into_raw()) }
}

fn into_polars_array(array: DuckArray) -> ArrowArray {
    unsafe { std::mem::transmute(array.into_raw()) }
}

pub fn register(extension: &Extension) -> Result<()> {
    for kind in [Kind::Stata, Kind::Spss, Kind::SpssCompressed, Kind::SasCsv, Kind::Xpt, Kind::Por] {
        let mut builder = CopyFunctionBuilder::new(kind.name(), WriteFormat { kind });
        if !matches!(kind, Kind::SasCsv) {
            builder = builder.with_fallback_extension(kind.name());
        }
        builder.register(extension)?;
    }
    Ok(())
}
