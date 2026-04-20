use arrow::{
    array::RecordBatch,
    datatypes::DataType,
    ffi::FFI_ArrowSchema,
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use duckdb::{
    Connection, Result,
    core::{DataChunkHandle, LogicalTypeHandle, LogicalTypeId},
    duckdb_entrypoint_c_api,
    vtab::{
        BindInfo, InitInfo, TableFunctionInfo, VTab,
        arrow::{record_batch_to_duckdb_data_chunk, to_duckdb_logical_type},
    },
};
use polars_readstat_rs::{sas_arrow_output, spss_arrow_output, stata_arrow_output};
use std::{path::Path, sync::Mutex};

// ── shared helpers ────────────────────────────────────────────────────────────

unsafe fn schema_fields_from_ptr<T>(
    ptr: *mut T,
) -> Result<Vec<arrow::datatypes::FieldRef>, Box<dyn std::error::Error>> {
    let ffi: FFI_ArrowSchema = unsafe { *Box::from_raw(ptr as *mut FFI_ArrowSchema) };
    let field = arrow::datatypes::Field::try_from(&ffi)?;
    match field.data_type() {
        DataType::Struct(fields) => Ok(fields.iter().cloned().collect()),
        other => Err(format!("expected Struct, got {other}").into()),
    }
}

fn register_columns(
    bind: &BindInfo,
    fields: &[arrow::datatypes::FieldRef],
) -> Result<(), Box<dyn std::error::Error>> {
    if fields.is_empty() {
        return Err("file has no columns".into());
    }
    for f in fields {
        let logical_type = to_duckdb_logical_type(f.data_type())?;
        bind.add_result_column(f.name(), logical_type);
    }
    Ok(())
}

unsafe fn stream_reader_from_ptr<T>(
    ptr: *mut T,
) -> Result<ArrowArrayStreamReader, Box<dyn std::error::Error>> {
    let ffi = unsafe { FFI_ArrowArrayStream::from_raw(ptr as *mut FFI_ArrowArrayStream) };
    Ok(ArrowArrayStreamReader::try_new(ffi)?)
}

fn get_bool_param(bind: &BindInfo, name: &str, default: bool) -> bool {
    bind.get_named_parameter(name)
        .map(|v| v.to_string() == "true")
        .unwrap_or(default)
}

// ── file format detection ─────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum FileFormat {
    Sas,
    Dta,
    Sav,
}

fn detect_format(path: &str, override_fmt: Option<&str>) -> Result<FileFormat, Box<dyn std::error::Error>> {
    if let Some(fmt) = override_fmt {
        return match fmt.to_lowercase().as_str() {
            "sas7bdat" | "sas" => Ok(FileFormat::Sas),
            "dta" | "stata" => Ok(FileFormat::Dta),
            "sav" | "zsav" | "spss" => Ok(FileFormat::Sav),
            other => Err(format!("unknown format '{other}'; expected sas7bdat, dta, or sav").into()),
        };
    }
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_lowercase();
    match ext.as_str() {
        "sas7bdat" => Ok(FileFormat::Sas),
        "dta" => Ok(FileFormat::Dta),
        "sav" | "zsav" => Ok(FileFormat::Sav),
        other => Err(format!(
            "cannot infer format from extension '.{other}'; use format='sas7bdat', 'dta', or 'sav'"
        ).into()),
    }
}

// ── shared VTab state ─────────────────────────────────────────────────────────

struct FileBindData {
    path: String,
    missing_string_as_null: bool,
    format: FileFormat,
}

unsafe impl Send for FileBindData {}
unsafe impl Sync for FileBindData {}

struct StreamInitData {
    reader: Mutex<Option<ArrowArrayStreamReader>>,
    pending: Mutex<Option<(RecordBatch, usize)>>,
}

unsafe impl Send for StreamInitData {}
unsafe impl Sync for StreamInitData {}

impl StreamInitData {
    fn new(reader: ArrowArrayStreamReader) -> Self {
        Self {
            reader: Mutex::new(Some(reader)),
            pending: Mutex::new(None),
        }
    }
}

fn vtab_func<T: VTab<BindData = FileBindData, InitData = StreamInitData>>(
    func: &TableFunctionInfo<T>,
    output: &mut DataChunkHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let init = func.get_init_data();
    let max_rows = unsafe { duckdb::ffi::duckdb_vector_size() as usize };

    // Drain any pending slice from a previous oversized batch.
    let mut pending = init.pending.lock().unwrap();
    if let Some((batch, offset)) = pending.take() {
        let remaining = batch.num_rows() - offset;
        let take = remaining.min(max_rows);
        let slice = batch.slice(offset, take);
        if offset + take < batch.num_rows() {
            *pending = Some((batch, offset + take));
        }
        record_batch_to_duckdb_data_chunk(&slice, output)?;
        return Ok(());
    }
    drop(pending);

    let mut reader = init.reader.lock().unwrap();
    match reader.as_mut().and_then(|r| r.next()) {
        Some(Ok(batch)) => {
            if batch.num_rows() <= max_rows {
                record_batch_to_duckdb_data_chunk(&batch, output)?;
            } else {
                let slice = batch.slice(0, max_rows);
                *init.pending.lock().unwrap() = Some((batch, max_rows));
                record_batch_to_duckdb_data_chunk(&slice, output)?;
            }
        }
        None => {
            output.set_len(0);
        }
        Some(Err(e)) => return Err(e.into()),
    }
    Ok(())
}

fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
    Some(vec![
        ("format".to_string(), LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("missing_string_as_null".to_string(), LogicalTypeHandle::from(LogicalTypeId::Boolean)),
    ])
}

// ── read_prs ──────────────────────────────────────────────────────────────────

struct ReadPrsVTab;

impl VTab for ReadPrsVTab {
    type BindData = FileBindData;
    type InitData = StreamInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn std::error::Error>> {
        let path = bind.get_parameter(0).to_string();
        let fmt_override = bind.get_named_parameter("format").map(|v| v.to_string());
        let format = detect_format(&path, fmt_override.as_deref())?;
        let missing_string_as_null = get_bool_param(bind, "missing_string_as_null", false);

        let schema_ptr = match format {
            FileFormat::Sas => sas_arrow_output::read_to_arrow_schema_ffi(
                Path::new(&path), None, missing_string_as_null, None,
            )?,
            FileFormat::Dta => stata_arrow_output::read_to_arrow_schema_ffi(
                Path::new(&path), None, missing_string_as_null, Some(false), None,
            )?,
            FileFormat::Sav => spss_arrow_output::read_to_arrow_schema_ffi(
                Path::new(&path), None, missing_string_as_null, Some(false), None,
            )?,
        };
        let fields = unsafe { schema_fields_from_ptr(schema_ptr) }?;
        register_columns(bind, &fields)?;
        Ok(FileBindData { path, missing_string_as_null, format })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind = unsafe { &*(init.get_bind_data::<FileBindData>()) };
        let stream_ptr = match bind.format {
            FileFormat::Sas => sas_arrow_output::read_to_arrow_stream_ffi(
                Path::new(&bind.path), None, bind.missing_string_as_null, None,
            )?,
            FileFormat::Dta => stata_arrow_output::read_to_arrow_stream_ffi(
                Path::new(&bind.path), None, bind.missing_string_as_null, Some(false), None,
            )?,
            FileFormat::Sav => spss_arrow_output::read_to_arrow_stream_ffi(
                Path::new(&bind.path), None, bind.missing_string_as_null, Some(false), None,
            )?,
        };
        let reader = unsafe { stream_reader_from_ptr(stream_ptr) }?;
        Ok(StreamInitData::new(reader))
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        vtab_func(func, output)
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![LogicalTypeHandle::from(LogicalTypeId::Varchar)])
    }

    fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
        named_parameters()
    }
}

// ── extension entrypoint ──────────────────────────────────────────────────────

#[duckdb_entrypoint_c_api(ext_name = "read_prs", min_duckdb_version = "v1.0.0")]
pub fn extension_entrypoint(con: Connection) -> Result<(), Box<dyn std::error::Error>> {
    con.register_table_function::<ReadPrsVTab>("read_prs")?;
    Ok(())
}
