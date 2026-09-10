//! Opening a dump, whether it is a local file or an object in cloud storage.
//!
//! Executes on the reader thread. The object-store API is async, so a remote source is
//! driven by blocking on the load's Tokio runtime, one chunk at a time. That keeps the
//! rest of the pipeline synchronous: everything downstream still sees a plain
//! [`std::io::Read`].
//!
//! # Why this exists
//!
//! Without it the dump has to be on local disk before the load can start, which for a
//! feed that lands in cloud storage means a staging hop:
//!
//! ```text
//! object storage -> Python memory -> another store -> local file -> this library
//! ```
//!
//! Each step costs its own copy of a 48 GB file and its own failure mode. Reading the
//! object directly removes all of them.
//!
//! # Resuming
//!
//! A multi-hour download over a connection that drops is not an unusual event, it is an
//! expected one. When the byte stream fails part way, the reader re-issues a ranged GET
//! from the offset already delivered rather than failing the load or, worse, returning
//! short. A stream that ends early against a known content length is treated as a
//! truncated transfer and fails, because a short read here would look exactly like a
//! truncated dump and be blamed on the sender.

use std::collections::HashMap;
use std::io::{self, Read};

use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{GetOptions, GetRange, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use tokio::runtime::Handle;
use url::Url;

use crate::error::{Error, Result};

/// URL schemes read through the object store rather than the filesystem.
///
/// Matched explicitly rather than by "does it parse as a URL", because a Windows path
/// such as `C:\dumps\day.sql` parses as a URL with the scheme `c`.
const REMOTE_SCHEMES: &[&str] = &[
    "abfs", "abfss", // Azure Data Lake Storage Gen2
    "az", "adl", "azure", // other spellings object_store accepts
    "gs",    // Google Cloud Storage
    "s3", "s3a", // S3 and S3-compatible
    "http", "https",
];

/// How many times a broken byte stream is resumed before the load gives up.
const MAX_RESUMES: u32 = 8;

/// True if `uri` names an object in cloud storage rather than a local file.
///
/// # Panics
///
/// Does not panic.
///
/// # Examples
///
/// ```
/// use pgdelta::source::is_remote;
///
/// assert!(is_remote("abfss://c@a.dfs.core.windows.net/pg/day.sql.gz"));
/// assert!(is_remote("gs://bucket/day.sql"));
/// assert!(!is_remote("/Volumes/main/landing/day.sql"));
/// // A Windows path parses as a URL with scheme "c", and is not remote.
/// assert!(!is_remote("C:/dumps/day.sql"));
/// ```
pub fn is_remote(uri: &str) -> bool {
    match Url::parse(uri) {
        Ok(url) => REMOTE_SCHEMES.contains(&url.scheme()),
        Err(_) => false,
    }
}

/// Translates an object-store error, which is not `Clone`, into this crate's error type.
fn store_error(context: &str, e: impl std::fmt::Display) -> Error {
    Error::Io {
        message: format!("{context}: {e}"),
    }
}

/// Opens `uri` for reading, whichever kind of location it names.
///
/// Returns the reader and, when the store reported one, the total size in bytes. The size
/// is what lets a caller turn `bytes_read` into a percentage, and what makes a truncated
/// download detectable.
///
/// `options` is passed to the object store for credentials and endpoint configuration. It
/// is ignored for a local path.
///
/// # Errors
///
/// [`Error::Io`] if the location cannot be parsed, the object cannot be opened, or a local
/// file cannot be read.
///
/// # Panics
///
/// Does not panic.
///
/// # Blocking
///
/// Blocks. A remote source drives the object store by blocking on `handle`, so this must
/// not be called from inside that runtime.
pub fn open(
    uri: &str,
    options: &HashMap<String, String>,
    handle: &Handle,
) -> Result<(Box<dyn Read>, Option<u64>)> {
    if !is_remote(uri) {
        let path = local_path(uri);
        let file = std::fs::File::open(&path)
            .map_err(|e| store_error(&format!("opening {}", path.display()), e))?;
        let size = file.metadata().ok().map(|m| m.len());
        return Ok((Box::new(std::io::BufReader::new(file)), size));
    }

    let url = Url::parse(uri).map_err(|e| store_error(&format!("parsing {uri}"), e))?;
    let (store, path) = object_store::parse_url_opts(&url, options.iter())
        .map_err(|e| store_error(&format!("resolving {uri}"), e))?;
    let store: std::sync::Arc<dyn ObjectStore> = store.into();

    let meta = handle
        .block_on(store.head(&path))
        .map_err(|e| store_error(&format!("reading metadata for {uri}"), e))?;
    let size = meta.size;

    let reader = ObjectSource::start(handle.clone(), store, path, size, uri.to_string())?;
    Ok((Box::new(reader), Some(size)))
}

/// Resolves a local location written either as a bare path or as a `file://` URL.
///
/// Delegated to `Url::to_file_path`, which knows that `file:///tmp/x` is absolute on Unix
/// while `file:///C:/x` carries a drive letter on Windows. Stripping the scheme by hand
/// and trimming leading slashes gets Windows right and turns every absolute Unix path into
/// a relative one, which then fails to open from whatever the working directory happens to
/// be.
///
/// `to_file_path` refuses a URL valid only for the other platform, so the fallback uses
/// the URL's own path component rather than handing the whole URI to the filesystem.
fn local_path(uri: &str) -> std::path::PathBuf {
    if uri.starts_with("file://")
        && let Ok(url) = Url::parse(uri)
    {
        return url
            .to_file_path()
            .unwrap_or_else(|()| std::path::PathBuf::from(url.path()));
    }
    std::path::PathBuf::from(uri)
}

/// A blocking [`Read`] over an object-store GET, which resumes a broken stream.
struct ObjectSource {
    handle: Handle,
    store: std::sync::Arc<dyn ObjectStore>,
    path: ObjectPath,
    /// For diagnostics only. Never carries credentials, because it is the URL the caller
    /// supplied and options are passed separately.
    uri: String,
    total: u64,
    delivered: u64,
    resumes: u32,
    stream: Option<BoxStream<'static, object_store::Result<Bytes>>>,
    pending: Bytes,
}

impl ObjectSource {
    fn start(
        handle: Handle,
        store: std::sync::Arc<dyn ObjectStore>,
        path: ObjectPath,
        total: u64,
        uri: String,
    ) -> Result<Self> {
        let mut source = Self {
            handle,
            store,
            path,
            uri,
            total,
            delivered: 0,
            resumes: 0,
            stream: None,
            pending: Bytes::new(),
        };
        source.open_stream()?;
        Ok(source)
    }

    /// Issues a GET from the current offset, so the same call both starts and resumes.
    fn open_stream(&mut self) -> Result<()> {
        let options = GetOptions {
            range: (self.delivered > 0).then_some(GetRange::Offset(self.delivered)),
            ..Default::default()
        };
        let result = self
            .handle
            .block_on(self.store.get_opts(&self.path, options))
            .map_err(|e| {
                store_error(
                    &format!("reading {} from byte {}", self.uri, self.delivered),
                    e,
                )
            })?;
        self.stream = Some(result.into_stream());
        Ok(())
    }

    /// Pulls the next chunk, resuming a broken stream rather than failing the load.
    fn next_chunk(&mut self) -> io::Result<Option<Bytes>> {
        loop {
            let Some(stream) = self.stream.as_mut() else {
                return Ok(None);
            };
            match self.handle.block_on(stream.next()) {
                Some(Ok(bytes)) => {
                    self.delivered += bytes.len() as u64;
                    return Ok(Some(bytes));
                }
                Some(Err(err)) => {
                    if self.resumes >= MAX_RESUMES {
                        return Err(io::Error::other(format!(
                            "reading {} failed after {} resumes at byte {} of {}: {err}",
                            self.uri, self.resumes, self.delivered, self.total
                        )));
                    }
                    self.resumes += 1;
                    self.stream = None;
                    self.open_stream().map_err(io::Error::other)?;
                }
                None => {
                    self.stream = None;
                    // A stream that simply stops is indistinguishable from a truncated
                    // dump downstream, so it is caught here where the size is known.
                    if self.delivered < self.total {
                        return Err(io::Error::other(format!(
                            "reading {} ended at byte {} of {}: the transfer was truncated",
                            self.uri, self.delivered, self.total
                        )));
                    }
                    return Ok(None);
                }
            }
        }
    }
}

impl Read for ObjectSource {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        while self.pending.is_empty() {
            match self.next_chunk()? {
                Some(bytes) => self.pending = bytes,
                None => return Ok(0),
            }
        }
        let take = self.pending.len().min(buf.len());
        buf[..take].copy_from_slice(&self.pending[..take]);
        self.pending = self.pending.slice(take..);
        Ok(take)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_schemes_are_recognised() {
        assert!(is_remote(
            "abfss://container@account.dfs.core.windows.net/pg/day.sql"
        ));
        assert!(is_remote("abfs://container@account/pg/day.sql"));
        assert!(is_remote("gs://bucket/day.sql.gz"));
        assert!(is_remote("s3://bucket/day.sql"));
        assert!(is_remote("https://example.com/day.sql"));
    }

    /// A Windows path parses as a URL whose scheme is its drive letter, which is why the
    /// scheme list is explicit rather than "anything with a scheme".
    #[test]
    fn local_paths_are_not_mistaken_for_urls() {
        assert!(!is_remote("/Volumes/main/landing/day.sql"));
        assert!(!is_remote("C:/dumps/day.sql"));
        assert!(!is_remote(r"C:\dumps\day.sql"));
        assert!(!is_remote("day.sql"));
        assert!(!is_remote("./relative/day.sql"));
        assert!(!is_remote("file:///tmp/day.sql"));
    }

    #[test]
    fn file_urls_and_bare_paths_resolve_alike() {
        let bare = std::env::temp_dir().join("pgdelta-localpath-probe.sql");
        assert_eq!(local_path(&bare.to_string_lossy()), bare);

        // Asserted independently of the host platform, because the bug this guards was
        // invisible on Windows and fatal on Unix: trimming the leading slash by hand
        // turned every absolute Unix path into a relative one, and only a Linux run could
        // see it. Checking the Unix spelling here means a Windows-only run catches it too.
        assert_eq!(
            local_path("file:///tmp/day.sql"),
            std::path::PathBuf::from("/tmp/day.sql"),
            "the leading slash of a Unix path must survive"
        );

        let as_url = format!("file://{}", bare.to_string_lossy().replace('\\', "/"));
        let from_url = local_path(&as_url);
        assert!(
            from_url.is_absolute(),
            "{as_url} resolved to the relative {}",
            from_url.display()
        );
        assert_eq!(
            from_url.file_name(),
            bare.file_name(),
            "{as_url} resolved to {}",
            from_url.display()
        );
    }
}
