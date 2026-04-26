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
use std::{
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

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

fn get_u64_param(bind: &BindInfo, name: &str) -> Option<u64> {
    bind.get_named_parameter(name)
        .and_then(|v| v.to_string().parse::<u64>().ok())
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

// ── VTab state ────────────────────────────────────────────────────────────────

struct FileBindData {
    path: String,
    missing_string_as_null: bool,
    format: FileFormat,
    offset: u64,
    limit: Option<u64>,
}

unsafe impl Send for FileBindData {}
unsafe impl Sync for FileBindData {}

struct StreamInitData {
    reader: Mutex<Option<ArrowArrayStreamReader>>,
    pending: Mutex<Option<RecordBatch>>,
    projected: Vec<usize>,
    read_ns: AtomicU64,
    copy_ns: AtomicU64,
    idle_ns: AtomicU64,
    n_calls: AtomicU64,
    last_end: Mutex<Option<Instant>>,
}

unsafe impl Send for StreamInitData {}
unsafe impl Sync for StreamInitData {}

impl StreamInitData {
    fn new(reader: ArrowArrayStreamReader, projected: Vec<usize>) -> Self {
        Self {
            reader: Mutex::new(Some(reader)),
            pending: Mutex::new(None),
            projected,
            read_ns: AtomicU64::new(0),
            copy_ns: AtomicU64::new(0),
            idle_ns: AtomicU64::new(0),
            n_calls: AtomicU64::new(0),
            last_end: Mutex::new(None),
        }
    }
}

fn vtab_func<T: VTab>(
    _func: &TableFunctionInfo<T>,
    init: &StreamInitData,
    output: &mut DataChunkHandle,
) -> Result<(), Box<dyn std::error::Error>> {
    let call_start = Instant::now();

    {
        let mut last = init.last_end.lock().unwrap();
        if let Some(t) = last.take() {
            init.idle_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
    }
    init.n_calls.fetch_add(1, Ordering::Relaxed);

    let max_rows = unsafe { duckdb::ffi::duckdb_vector_size() as usize };

    let mut pending = init.pending.lock().unwrap();
    if let Some(batch) = pending.take() {
        let take = batch.num_rows().min(max_rows);
        if batch.num_rows() > max_rows {
            let rest = batch.slice(max_rows, batch.num_rows() - max_rows);
            *pending = Some(rest);
        }
        let slice = batch.slice(0, take);
        drop(pending);
        let t = Instant::now();
        record_batch_to_duckdb_data_chunk(&slice, output)?;
        init.copy_ns.fetch_add(t.elapsed().as_nanos() as u64, Ordering::Relaxed);
        *init.last_end.lock().unwrap() = Some(Instant::now());
        return Ok(());
    }
    drop(pending);

    let mut reader = init.reader.lock().unwrap();
    let t_read = Instant::now();
    let next = reader.as_mut().and_then(|r| r.next());
    init.read_ns.fetch_add(t_read.elapsed().as_nanos() as u64, Ordering::Relaxed);

    match next {
        Some(Ok(batch)) => {
            let batch = batch.project(&init.projected)?;
            let t_copy = Instant::now();
            if batch.num_rows() <= max_rows {
                record_batch_to_duckdb_data_chunk(&batch, output)?;
            } else {
                let rest = batch.slice(max_rows, batch.num_rows() - max_rows);
                *init.pending.lock().unwrap() = Some(rest);
                let slice = batch.slice(0, max_rows);
                record_batch_to_duckdb_data_chunk(&slice, output)?;
            }
            init.copy_ns.fetch_add(t_copy.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }
        None => {
            let read_ms = init.read_ns.load(Ordering::Relaxed) / 1_000_000;
            let copy_ms = init.copy_ns.load(Ordering::Relaxed) / 1_000_000;
            let idle_ms = init.idle_ns.load(Ordering::Relaxed) / 1_000_000;
            let calls   = init.n_calls.load(Ordering::Relaxed);
            let total_ms = call_start.elapsed().as_millis() as u64 + read_ms + copy_ms + idle_ms;
            eprintln!(
                "[read_prs] calls={calls}  read={read_ms}ms  copy={copy_ms}ms  \
                 idle(duckdb)={idle_ms}ms  (total tracked ≈{total_ms}ms)"
            );
            output.set_len(0);
            return Ok(());
        }
        Some(Err(e)) => return Err(e.into()),
    }

    *init.last_end.lock().unwrap() = Some(Instant::now());
    Ok(())
}

fn named_parameters() -> Option<Vec<(String, LogicalTypeHandle)>> {
    Some(vec![
        ("format".to_string(),               LogicalTypeHandle::from(LogicalTypeId::Varchar)),
        ("missing_string_as_null".to_string(), LogicalTypeHandle::from(LogicalTypeId::Boolean)),
        ("row_offset".to_string(),           LogicalTypeHandle::from(LogicalTypeId::UBigint)),
        ("n_rows".to_string(),               LogicalTypeHandle::from(LogicalTypeId::UBigint)),
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

        let offset = get_u64_param(bind, "row_offset").unwrap_or(0);
        let limit  = get_u64_param(bind, "n_rows");
        Ok(FileBindData { path, missing_string_as_null, format, offset, limit })
    }

    fn init(init: &InitInfo) -> Result<Self::InitData, Box<dyn std::error::Error>> {
        let bind = unsafe { &*(init.get_bind_data::<FileBindData>()) };
        let projected: Vec<usize> = init.get_column_indices()
            .into_iter()
            .map(|i| i as usize)
            .collect();

        let offset = bind.offset as usize;
        let n_rows = bind.limit.map(|l| l as usize);

        let stream_ptr = match bind.format {
            FileFormat::Sas => sas_arrow_output::read_to_arrow_stream_ffi(
                Path::new(&bind.path), None, bind.missing_string_as_null, None,
                offset, n_rows,
            )?,
            FileFormat::Dta => stata_arrow_output::read_to_arrow_stream_ffi(
                Path::new(&bind.path), None, bind.missing_string_as_null, Some(false), None,
                offset, n_rows,
            )?,
            FileFormat::Sav => spss_arrow_output::read_to_arrow_stream_ffi(
                Path::new(&bind.path), None, bind.missing_string_as_null, Some(false), None,
                offset, n_rows,
            )?,
        };
        let reader = unsafe { stream_reader_from_ptr(stream_ptr) }?;
        Ok(StreamInitData::new(reader, projected))
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn std::error::Error>> {
        vtab_func(func, func.get_init_data(), output)
    }

    fn supports_pushdown() -> bool { true }

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
