//! Reads files through DuckDB's own file-system abstraction (its VFS) rather
//! than opening them directly with `std::fs::File`. This means `read_prs`
//! picks up whatever DuckDB itself already knows how to reach — its own
//! `httpfs` extension for `s3://`/`https://` paths, credentials configured
//! via `CREATE SECRET`, and any caching DuckDB's own I/O layer does — instead
//! of requiring separate credential/cloud configuration of its own.
//!
//! Implemented entirely against `polars_readstat_rs`'s existing, generic
//! `ReadSource`/`ReadSeek` traits (see `source.rs` there) — no changes to
//! that crate's decode internals were needed.
//!
//! # Lifetime of the file
//!
//! The file is opened once, during bind, through the query's own [`Context`]
//! (so the query's secrets and settings apply). It is owned by the bind data
//! and dropped with the query plan. Nothing is registered in the database
//! itself, so a database can shut down normally once its connections close.
//!
//! The decoder opens several independent readers over one source, possibly
//! from different threads, so the file is opened with parallel access and
//! every reader tracks its own position and uses positional `read_at`.

use duckdb_neo::{
    connection::Context,
    file::{File, FileBuilder, FileSystem},
};
use polars_readstat_rs::{ReadSeek, ReadSource};
use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult, Seek, SeekFrom};
use std::sync::Arc;

/// A file opened with `FILE_FLAG_PARALLEL_ACCESS`, which is what makes
/// concurrent positional reads from several threads safe.
struct ParallelFile(File);

// `File` is `Send` but not `Sync` because concurrent reads are only safe on
// files opened with parallel access, which `DuckDbFileSource::open` always does.
unsafe impl Sync for ParallelFile {}

/// A [`ReadSource`] backed by DuckDB's own VFS instead of `std::fs::File`.
#[derive(Clone)]
pub struct DuckDbFileSource {
    file: Arc<ParallelFile>,
    size: u64,
    path: String,
}

impl DuckDbFileSource {
    /// Opens `path` for reading through the file system of `context`.
    pub fn open(context: &Context, path: &str) -> Result<Self, String> {
        let fs = FileSystem::new(context).map_err(|e| format!("DuckDB file system: {e}"))?;
        let file = FileBuilder::new(&fs, path)
            .and_then(|b| b.read().parallel_access().open())
            .map_err(|e| format!("DuckDB VFS open failed for '{path}': {e}"))?;
        let size = file
            .size()
            .map_err(|e| format!("DuckDB VFS size failed for '{path}': {e}"))?;
        Ok(Self {
            file: Arc::new(ParallelFile(file)),
            size,
            path: path.to_string(),
        })
    }
}

impl ReadSource for DuckDbFileSource {
    fn open_reader(&self) -> IoResult<Box<dyn ReadSeek>> {
        Ok(Box::new(DuckDbReader {
            file: self.file.clone(),
            size: self.size,
            pos: 0,
            path: self.path.clone(),
        }))
    }
}

/// One independent reader over a shared file. Each `open_reader()` call
/// produces its own, matching `ReadSource`'s contract.
struct DuckDbReader {
    file: Arc<ParallelFile>,
    size: u64,
    pos: u64,
    path: String,
}

impl Read for DuckDbReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if buf.is_empty() || self.pos >= self.size {
            return Ok(0);
        }
        let len = buf.len().min((self.size - self.pos) as usize);
        let data = self
            .file
            .0
            .read_at(self.pos as usize, len)
            .map_err(|e| IoError::new(ErrorKind::Other, format!("{}: {e}", self.path)))?;
        buf[..data.len()].copy_from_slice(&data);
        self.pos += data.len() as u64;
        Ok(data.len())
    }
}

impl Seek for DuckDbReader {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        let target = match pos {
            SeekFrom::Start(n) => n as i128,
            SeekFrom::End(n) => self.size as i128 + n as i128,
            SeekFrom::Current(n) => self.pos as i128 + n as i128,
        };
        if target < 0 {
            return Err(IoError::new(ErrorKind::InvalidInput, "negative seek position"));
        }
        self.pos = target as u64;
        Ok(self.pos)
    }
}

// Each reader is used by the single worker thread that opened it, like
// `std::fs::File`; `ReadSeek` requires `Send`, which `Arc<ParallelFile>`
// provides via the `Sync` impl above.
