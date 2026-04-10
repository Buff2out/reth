use super::{
    fetch::{parallel_segmented_download, DownloadedArchive},
    progress::{
        DownloadProgress, DownloadRequestLimiter, ProgressReader, SharedProgress,
        SharedProgressReader,
    },
    MAX_DOWNLOAD_RETRIES, RETRY_BACKOFF_SECS,
};
use eyre::{Result, WrapErr};
use lz4::Decoder;
use reqwest::blocking::Client as BlockingClient;
use reth_cli_util::cancellation::CancellationToken;
use reth_fs_util as fs;
use std::{
    io::Read,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tar::Archive;
use tokio::task;
use tracing::{info, warn};
use url::Url;
use zstd::stream::read::Decoder as ZstdDecoder;

const EXTENSION_TAR_LZ4: &str = ".tar.lz4";
const EXTENSION_TAR_ZSTD: &str = ".tar.zst";

/// Supported compression formats for snapshots
#[derive(Debug, Clone, Copy)]
pub(crate) enum CompressionFormat {
    Lz4,
    Zstd,
}

impl CompressionFormat {
    /// Detect compression format from file extension
    pub(crate) fn from_url(url: &str) -> Result<Self> {
        let path =
            Url::parse(url).map(|u| u.path().to_string()).unwrap_or_else(|_| url.to_string());

        if path.ends_with(EXTENSION_TAR_LZ4) {
            Ok(Self::Lz4)
        } else if path.ends_with(EXTENSION_TAR_ZSTD) {
            Ok(Self::Zstd)
        } else {
            Err(eyre::eyre!(
                "Unsupported file format. Expected .tar.lz4 or .tar.zst, got: {}",
                path
            ))
        }
    }
}

/// Extracts a compressed tar archive to the target directory with progress tracking.
fn extract_archive<R: Read>(
    reader: R,
    total_size: u64,
    format: CompressionFormat,
    target_dir: &Path,
    cancel_token: CancellationToken,
) -> Result<()> {
    let progress_reader = ProgressReader::new(reader, total_size, cancel_token);

    match format {
        CompressionFormat::Lz4 => {
            let decoder = Decoder::new(progress_reader)?;
            Archive::new(decoder).unpack(target_dir)?;
        }
        CompressionFormat::Zstd => {
            let decoder = ZstdDecoder::new(progress_reader)?;
            Archive::new(decoder).unpack(target_dir)?;
        }
    }

    println!();
    Ok(())
}

/// Extracts a compressed tar archive without progress tracking.
pub(crate) fn extract_archive_raw<R: Read>(
    reader: R,
    format: CompressionFormat,
    target_dir: &Path,
) -> Result<()> {
    match format {
        CompressionFormat::Lz4 => {
            Archive::new(Decoder::new(reader)?).unpack(target_dir).wrap_err_with(|| {
                format!("failed to extract archive into `{}`", target_dir.display())
            })?;
        }
        CompressionFormat::Zstd => {
            Archive::new(ZstdDecoder::new(reader)?).unpack(target_dir).wrap_err_with(|| {
                format!("failed to extract archive into `{}`", target_dir.display())
            })?;
        }
    }
    Ok(())
}

/// Extracts a snapshot from a local file.
fn extract_from_file(path: &Path, format: CompressionFormat, target_dir: &Path) -> Result<()> {
    let file = std::fs::File::open(path)?;
    let total_size = file.metadata()?.len();
    info!(target: "reth::cli",
        file = %path.display(),
        size = %DownloadProgress::format_size(total_size),
        "Extracting local archive"
    );
    let start = Instant::now();
    extract_archive(file, total_size, format, target_dir, CancellationToken::new())?;
    info!(target: "reth::cli",
        file = %path.display(),
        elapsed = %DownloadProgress::format_duration(start.elapsed()),
        "Local extraction complete"
    );
    Ok(())
}

/// Streams a remote archive directly into the extractor without writing to disk.
///
/// On failure, retries from scratch up to [`MAX_DOWNLOAD_RETRIES`] times.
pub(crate) fn streaming_download_and_extract(
    url: &str,
    format: CompressionFormat,
    target_dir: &Path,
    shared: Option<&Arc<SharedProgress>>,
    request_limiter: Option<&Arc<DownloadRequestLimiter>>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let quiet = shared.is_some();
    let mut last_error: Option<eyre::Error> = None;

    for attempt in 1..=MAX_DOWNLOAD_RETRIES {
        if attempt > 1 {
            info!(target: "reth::cli",
                url = %url,
                attempt,
                max = MAX_DOWNLOAD_RETRIES,
                "Retrying streaming download from scratch"
            );
        }

        let client = BlockingClient::builder().connect_timeout(Duration::from_secs(30)).build()?;
        let _request_permit =
            request_limiter.map(|limiter| limiter.acquire(shared, &cancel_token)).transpose()?;

        let response = match client.get(url).send().and_then(|r| r.error_for_status()) {
            Ok(r) => r,
            Err(error) => {
                let err = eyre::Error::from(error);
                if attempt < MAX_DOWNLOAD_RETRIES {
                    warn!(target: "reth::cli",
                        url = %url,
                        attempt,
                        max = MAX_DOWNLOAD_RETRIES,
                        err = %err,
                        "Streaming request failed, retrying"
                    );
                }
                last_error = Some(err);
                if attempt < MAX_DOWNLOAD_RETRIES {
                    std::thread::sleep(Duration::from_secs(RETRY_BACKOFF_SECS));
                }
                continue;
            }
        };

        if !quiet && let Some(size) = response.content_length() {
            info!(target: "reth::cli",
                url = %url,
                size = %DownloadProgress::format_size(size),
                "Streaming archive"
            );
        }

        let result = if let Some(progress) = shared {
            let reader = SharedProgressReader { inner: response, progress: Arc::clone(progress) };
            extract_archive_raw(reader, format, target_dir)
        } else {
            let total_size = response.content_length().unwrap_or(0);
            extract_archive(response, total_size, format, target_dir, cancel_token.clone())
        };

        match result {
            Ok(()) => return Ok(()),
            Err(error) => {
                if attempt < MAX_DOWNLOAD_RETRIES {
                    warn!(target: "reth::cli",
                        url = %url,
                        attempt,
                        max = MAX_DOWNLOAD_RETRIES,
                        err = %error,
                        "Streaming extraction failed, retrying"
                    );
                }
                last_error = Some(error);
                if attempt < MAX_DOWNLOAD_RETRIES {
                    std::thread::sleep(Duration::from_secs(RETRY_BACKOFF_SECS));
                }
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        eyre::eyre!("Streaming download failed after {MAX_DOWNLOAD_RETRIES} attempts")
    }))
}

/// Fetches the snapshot from a remote URL with resume support, then extracts it.
fn download_and_extract(
    url: &str,
    format: CompressionFormat,
    target_dir: &Path,
    shared: Option<&Arc<SharedProgress>>,
    request_limiter: &Arc<DownloadRequestLimiter>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let quiet = shared.is_some();
    let DownloadedArchive { path: downloaded_path, size: total_size } =
        parallel_segmented_download(
            url,
            target_dir,
            shared,
            request_limiter,
            cancel_token.clone(),
        )?;

    let file_name =
        downloaded_path.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default();

    if !quiet {
        info!(target: "reth::cli",
            file = %file_name,
            size = %DownloadProgress::format_size(total_size),
            "Extracting archive"
        );
    }
    let file = fs::open(&downloaded_path)?;

    if quiet {
        extract_archive_raw(file, format, target_dir)?;
    } else {
        extract_archive(file, total_size, format, target_dir, cancel_token)?;
        info!(target: "reth::cli",
            file = %file_name,
            "Extraction complete"
        );
    }

    fs::remove_file(&downloaded_path)?;

    if let Some(progress) = shared {
        progress.record_archive_complete(total_size);
    }

    Ok(())
}

/// Downloads and extracts a snapshot, blocking until finished.
///
/// Supports `file://` URLs for local files and HTTP(S) URLs for remote downloads.
/// When `resumable` is true, downloads to a `.part` file first with HTTP Range resume
/// support. Otherwise streams directly into the extractor.
fn blocking_download_and_extract(
    url: &str,
    target_dir: &Path,
    shared: Option<Arc<SharedProgress>>,
    resumable: bool,
    request_limiter: Option<Arc<DownloadRequestLimiter>>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let format = CompressionFormat::from_url(url)?;

    if let Ok(parsed_url) = Url::parse(url) &&
        parsed_url.scheme() == "file"
    {
        let file_path = parsed_url
            .to_file_path()
            .map_err(|_| eyre::eyre!("Invalid file:// URL path: {}", url))?;
        let result = extract_from_file(&file_path, format, target_dir);
        if result.is_ok() &&
            let Some(progress) = shared
        {
            progress.record_archive_complete(file_path.metadata()?.len());
        }
        result
    } else if let Some(request_limiter) = request_limiter.as_ref() {
        download_and_extract(
            url,
            format,
            target_dir,
            shared.as_ref(),
            request_limiter,
            cancel_token,
        )
    } else if resumable {
        let request_limiter = DownloadRequestLimiter::new(1);
        download_and_extract(
            url,
            format,
            target_dir,
            shared.as_ref(),
            &request_limiter,
            cancel_token,
        )
    } else {
        let result = streaming_download_and_extract(
            url,
            format,
            target_dir,
            shared.as_ref(),
            None,
            cancel_token,
        );
        if result.is_ok() &&
            let Some(progress) = shared
        {
            progress.record_archive_finished();
        }
        result
    }
}

/// Downloads and extracts a snapshot archive asynchronously.
///
/// When `shared` is provided, download progress is reported to the shared
/// counter for aggregated display. Otherwise uses a local progress bar.
/// When `resumable` is true, uses two-phase download with `.part` files.
pub(crate) async fn stream_and_extract(
    url: &str,
    target_dir: &Path,
    shared: Option<Arc<SharedProgress>>,
    resumable: bool,
    request_limiter: Option<Arc<DownloadRequestLimiter>>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let target_dir = target_dir.to_path_buf();
    let url = url.to_string();
    task::spawn_blocking(move || {
        blocking_download_and_extract(
            &url,
            &target_dir,
            shared,
            resumable,
            request_limiter,
            cancel_token,
        )
    })
    .await??;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_format_detection() {
        assert!(matches!(
            CompressionFormat::from_url("https://example.com/snapshot.tar.lz4"),
            Ok(CompressionFormat::Lz4)
        ));
        assert!(matches!(
            CompressionFormat::from_url("https://example.com/snapshot.tar.zst"),
            Ok(CompressionFormat::Zstd)
        ));
        assert!(matches!(
            CompressionFormat::from_url("file:///path/to/snapshot.tar.lz4"),
            Ok(CompressionFormat::Lz4)
        ));
        assert!(matches!(
            CompressionFormat::from_url("file:///path/to/snapshot.tar.zst"),
            Ok(CompressionFormat::Zstd)
        ));
        assert!(CompressionFormat::from_url("https://example.com/snapshot.tar.gz").is_err());
    }
}
