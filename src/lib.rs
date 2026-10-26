mod duckdb_file_source;
mod write;

use arrow::{
    array::{Array, RecordBatch, StructArray},
    datatypes::DataType,
    ffi::{FFI_ArrowSchema, to_ffi},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use duckdb_file_source::DuckDbFileSource;
use duckdb_loadable_macros::duckdb_entrypoint_c_api_v2;
use duckdb_neo::{
    Error, Result,
    arrow::{ArrowArray, ArrowImporter, ArrowSchema},
    bind_arguments::BindArgument,
    connection::Context,
    data_chunk::DataChunkRef,
    extension::Extension,
    signature::{FunctionParameter, FunctionParameterKind, SignatureBuilder},
    table_function::{
        BindFunctionHandle, OrderPreservation, ExecColumnInfo, InitColumnData, PartitionData, TableFunctionBuilder,
        TableFunctionCallbacks, TablePartitioningCallbacks,
    },
    vector::Unknown,
};
use polars_readstat_rs::{ReadSource, sas_arrow_output, spss_arrow_output, stata_arrow_output};
use std::{path::Path, sync::{Arc, Mutex}};


/// DuckDB's standard vector size; no chunk handed to DuckDB may exceed it.
const VECTOR_SIZE: usize = 2048;

// ── shared helpers ────────────────────────────────────────────────────────────

fn err<E: std::fmt::Display>(e: E) -> Error {
    Error::invalid_input(e.to_string())
}

/// Takes ownership of the schema polars-readstat allocated, returning the
/// column count and the same schema as a DuckDB-importable Arrow schema.
unsafe fn take_schema<T>(ptr: *mut T) -> Result<(usize, ArrowSchema)> {
    let ffi: FFI_ArrowSchema = unsafe { *Box::from_raw(ptr as *mut FFI_ArrowSchema) };
    let field = arrow::datatypes::Field::try_from(&ffi).map_err(err)?;
    let n = match field.data_type() {
        DataType::Struct(fields) => fields.len(),
        other => return Err(err(format!("expected Struct, got {other}"))),
    };
    // Same C Data Interface layout; ownership (and the release callback) moves.
    Ok((n, unsafe { ArrowSchema::from_raw(std::mem::transmute(ffi)) }))
}

unsafe fn stream_reader_from_ptr<T>(ptr: *mut T) -> Result<ArrowArrayStreamReader> {
    let ffi = unsafe { FFI_ArrowArrayStream::from_raw(ptr as *mut FFI_ArrowArrayStream) };
    ArrowArrayStreamReader::try_new(ffi).map_err(err)
}

// ── file format detection ─────────────────────────────────────────────────────

#[derive(Clone, Copy)]
enum FileFormat {
    Sas,
    Dta,
    Sav,
}

fn detect_format(path: &str, override_fmt: Option<&str>) -> Result<FileFormat> {
    if let Some(fmt) = override_fmt {
        return match fmt.to_lowercase().as_str() {
            "sas7bdat" | "sas" => Ok(FileFormat::Sas),
            "dta" | "stata" => Ok(FileFormat::Dta),
            "sav" | "zsav" | "spss" => Ok(FileFormat::Sav),
            other => Err(err(format!("unknown format '{other}'; expected sas7bdat, dta, or sav"))),
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
        other => Err(err(format!(
            "cannot infer format from extension '.{other}'; use format='sas7bdat', 'dta', or 'sav'"
        ))),
    }
}

// ── scan state ────────────────────────────────────────────────────────────────

struct FileBindData {
    source: Arc<dyn ReadSource>,
    missing_string_as_null: bool,
    format: FileFormat,
    offset: u64,
    limit: Option<u64>,
    preserve_order: bool,
    row_index_name: Option<String>,
    threads: Option<usize>,
    batch_size: Option<usize>,
    value_labels_as_strings: bool,
}

struct StreamState {
    reader: Option<ArrowArrayStreamReader>,
    pending: Option<RecordBatch>,
    /// Sequence number of the next slice handed out; DuckDB uses it to put
    /// slices produced by different workers back in file order.
    next_index: usize,
}

/// Per-worker state: the sequence number of the slice `exec` last produced.
#[derive(Default)]
struct WorkerState {
    batch_index: usize,
}

struct ScanState {
    stream: Mutex<StreamState>,
}

impl ScanState {
    /// Returns the next batch of at most `VECTOR_SIZE` rows, or
    /// `None` once the stream is exhausted.
    fn next_slice(&self) -> Result<Option<(usize, RecordBatch)>> {
        let mut st = self.stream.lock().map_err(|_| err("scan state poisoned"))?;
        let batch = match st.pending.take() {
            Some(b) => b,
            None => match st.reader.as_mut().and_then(|r| r.next()) {
                Some(Ok(b)) => b,
                Some(Err(e)) => return Err(err(e)),
                None => return Ok(None),
            },
        };
        let index = st.next_index;
        st.next_index += 1;
        if batch.num_rows() > VECTOR_SIZE {
            st.pending = Some(batch.slice(VECTOR_SIZE, batch.num_rows() - VECTOR_SIZE));
            Ok(Some((index, batch.slice(0, VECTOR_SIZE))))
        } else {
            Ok(Some((index, batch)))
        }
    }
}

// ── read_prs ──────────────────────────────────────────────────────────────────

struct ReadPrs;

/// Positions of the declared parameters in `bind`'s argument list.
const ARG_PATH: usize = 0;
const ARG_FORMAT: usize = 1;
const ARG_MISSING_STRING_AS_NULL: usize = 2;
const ARG_ROW_OFFSET: usize = 3;
const ARG_N_ROWS: usize = 4;
const ARG_PRESERVE_ORDER: usize = 5;
const ARG_ROW_INDEX_NAME: usize = 6;
const ARG_THREADS: usize = 7;
const ARG_BATCH_SIZE: usize = 8;
const ARG_VALUE_LABELS_AS_STRINGS: usize = 9;

/// DuckDB's own `threads` setting for this session, used when the caller does
/// not pass `threads` explicitly.
fn duckdb_threads(context: &Context) -> Option<usize> {
    let (value, _scope) = context.config().ok()?.get_value("threads").ok()?;
    value.get::<i64>().ok().flatten().filter(|n| *n > 0).map(|n| n as usize)
}

fn arg<T: duckdb_neo::FromValue>(args: &[BindArgument], index: usize) -> Option<T> {
    args.get(index)?.value.as_ref()?.get::<T>().ok().flatten()
}

impl TableFunctionCallbacks for ReadPrs {
    type BindData = FileBindData;
    type LocalState = WorkerState;
    type GlobalState = ScanState;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind_handle: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        let path: String = arg(&arguments, ARG_PATH).ok_or_else(|| err("read_prs requires a path"))?;
        let fmt_override: Option<String> = arg(&arguments, ARG_FORMAT);
        let format = detect_format(&path, fmt_override.as_deref())?;
        let missing_string_as_null = arg::<bool>(&arguments, ARG_MISSING_STRING_AS_NULL).unwrap_or(false);
        let offset = arg::<u64>(&arguments, ARG_ROW_OFFSET).unwrap_or(0);
        let limit = arg::<u64>(&arguments, ARG_N_ROWS);
        let preserve_order = arg::<bool>(&arguments, ARG_PRESERVE_ORDER).unwrap_or(false);
        let row_index_name: Option<String> = arg(&arguments, ARG_ROW_INDEX_NAME);
        let row_index = row_index_name.as_deref();
        let threads = arg::<u64>(&arguments, ARG_THREADS)
            .map(|n| n as usize)
            .or_else(|| duckdb_threads(context));
        let batch_size = arg::<u64>(&arguments, ARG_BATCH_SIZE).map(|n| n as usize);
        let value_labels_as_strings = arg::<bool>(&arguments, ARG_VALUE_LABELS_AS_STRINGS).unwrap_or(false);

        let source: Arc<dyn ReadSource> = Arc::new(DuckDbFileSource::open(context, &path).map_err(err)?);

        let schema_ptr = match format {
            FileFormat::Sas => sas_arrow_output::read_to_arrow_schema_ffi_from_source(source.clone(), row_index),
            FileFormat::Dta => stata_arrow_output::read_to_arrow_schema_ffi_from_source(source.clone(), Some(value_labels_as_strings), row_index),
            FileFormat::Sav => spss_arrow_output::read_to_arrow_schema_ffi_from_source(source.clone(), Some(value_labels_as_strings), row_index),
        }
        .map_err(err)?;
        let (n_columns, schema) = unsafe { take_schema(schema_ptr) }?;
        if n_columns == 0 {
            return Err(err("file has no columns"));
        }

        // DuckDB resolves the Arrow types itself, so the column types always
        // match what the importer produces at scan time.
        // Like polars_readstat: unordered batches by default (streaming, parallel);
        // `preserve_order` buffers in the decoder and has DuckDB keep file order.
        bind_handle.set_order(if preserve_order { OrderPreservation::Insertion } else { OrderPreservation::None })?;
        let importer = ArrowImporter::new(context, schema, None)?;
        for (name, logical_type) in importer.schema()?.get_all()? {
            bind_handle.add_result_column(&name, &logical_type)?;
        }

        Ok(FileBindData {
            source, missing_string_as_null, format, offset, limit, preserve_order, row_index_name,
            threads, batch_size, value_labels_as_strings,
        })
    }

    fn init_global_state(
        &self,
        bind: &Self::BindData,
        _context: &Context,
        column_data: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        let projected = (0..column_data.count()?)
            .map(|i| column_data.get_column_index(i))
            .collect::<Result<Vec<_>>>()?;

        let offset = bind.offset as usize;
        let n_rows = bind.limit.map(|l| l as usize);
        let stream_ptr = match bind.format {
            FileFormat::Sas => sas_arrow_output::read_to_arrow_stream_ffi_from_source(
                bind.source.clone(), bind.threads, bind.missing_string_as_null, bind.batch_size, offset, n_rows,
                bind.preserve_order, bind.row_index_name.clone(), Some(projected.clone()),
            ),
            FileFormat::Dta => stata_arrow_output::read_to_arrow_stream_ffi_from_source(
                bind.source.clone(), bind.threads, bind.missing_string_as_null, Some(bind.value_labels_as_strings), bind.batch_size, offset, n_rows,
                bind.preserve_order, bind.row_index_name.clone(), Some(projected.clone()),
            ),
            FileFormat::Sav => spss_arrow_output::read_to_arrow_stream_ffi_from_source(
                bind.source.clone(), bind.threads, bind.missing_string_as_null, Some(bind.value_labels_as_strings), bind.batch_size, offset, n_rows,
                bind.preserve_order, bind.row_index_name.clone(), Some(projected),
            ),
        }
        .map_err(err)?;
        let reader = unsafe { stream_reader_from_ptr(stream_ptr) }?;

        let state = ScanState {
            stream: Mutex::new(StreamState { reader: Some(reader), pending: None, next_index: 0 }),
        };
        // One worker: the decoder already parallelises internally.
        Ok((Some(state), Some(std::thread::available_parallelism().map_or(1, |n| n.get()))))
    }

    fn init_local_state(
        &self,
        _bind: &Self::BindData,
        _global: Option<&Self::GlobalState>,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(Some(WorkerState::default()))
    }

    fn exec(
        &self,
        _bind: &Self::BindData,
        global: Option<&Self::GlobalState>,
        local: Option<&mut Self::LocalState>,
        context: &Context,
        mut output: DataChunkRef<'_>,
        _column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        let global = global.ok_or_else(|| err("missing global scan state"))?;

        let next = global.next_slice()?;
        let Some((index, batch)) = next else {
            output.get_vector_at_mut::<Unknown>(0)?.set_size(0)?;
            return Ok(());
        };
        if let Some(local) = local {
            local.batch_index = index;
        }
        let n_rows = batch.num_rows();

        // Zero-copy hand-off: the Arrow buffers are moved into DuckDB, which
        // references them from the resulting chunk instead of copying.
        let (array, schema) = to_ffi(&StructArray::from(batch).to_data()).map_err(err)?;
        let schema: ArrowSchema = unsafe { ArrowSchema::from_raw(std::mem::transmute(schema)) };
        let array: ArrowArray = unsafe { ArrowArray::from_raw(std::mem::transmute(array)) };

        let mut importer = ArrowImporter::new(context, schema, Some(VECTOR_SIZE))?;
        importer.append_consumed(array, true)?;
        let chunk = importer
            .next_chunk()?
            .ok_or_else(|| err("Arrow importer produced no chunk"))?;

        for i in 0..chunk.col_count() {
            let source = chunk.get_vector_at::<Unknown>(i)?;
            // SAFETY: `chunk` stays alive for the rest of this call, and DuckDB's
            // reference shares the buffers rather than borrowing them.
            unsafe { output.get_vector_at_mut::<Unknown>(i)?.reference(source)? };
        }
        output.get_vector_at_mut::<Unknown>(0)?.set_size(n_rows)?;
        Ok(())
    }
}

impl TablePartitioningCallbacks for ReadPrs {
    fn partition_data(
        &self,
        _bind: &Self::BindData,
        _global: Option<&Self::GlobalState>,
        local: Option<&mut Self::LocalState>,
        _context: &Context,
        _partition_data: PartitionData<'_>,
    ) -> Result<usize> {
        Ok(local.map_or(0, |l| l.batch_index))
    }
}

// ── extension entrypoint ──────────────────────────────────────────────────────

#[duckdb_entrypoint_c_api_v2(extension_name = "read_prs", api_version = "v2.0.0")]
pub fn entrypoint(extension: &Extension) -> Result<()> {
    TableFunctionBuilder::new(
        "read_prs",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("path").kind(FunctionParameterKind::PositionalOnly))
            .param(FunctionParameter::new::<String>("format").default_value(None::<String>).kind(FunctionParameterKind::Named))
            .param(FunctionParameter::with_default("missing_string_as_null", false).kind(FunctionParameterKind::Named))
            .param(FunctionParameter::with_default("row_offset", 0u64).kind(FunctionParameterKind::Named))
            .param(FunctionParameter::new::<u64>("n_rows").default_value(None::<u64>).kind(FunctionParameterKind::Named))
            .param(FunctionParameter::with_default("preserve_order", false).kind(FunctionParameterKind::Named))
            .param(FunctionParameter::new::<String>("row_index_name").default_value(None::<String>).kind(FunctionParameterKind::Named))
            .param(FunctionParameter::new::<u64>("threads").default_value(None::<u64>).kind(FunctionParameterKind::Named))
            .param(FunctionParameter::new::<u64>("batch_size").default_value(None::<u64>).kind(FunctionParameterKind::Named))
            .param(FunctionParameter::with_default("value_labels_as_strings", false).kind(FunctionParameterKind::Named)),
        ReadPrs,
    )
    .with_projection_pushdown(true)
    .with_partitioning()
    .register(extension)?;
    write::register(extension)?;
    Ok(())
}
