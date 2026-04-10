use super::{
    extract::{extract_archive_raw, streaming_download_and_extract, CompressionFormat},
    fetch::parallel_segmented_download,
    planning::{PlannedArchive, PlannedDownloads},
    progress::{
        spawn_progress_display, DownloadActivityGuard, DownloadRequestLimiter, ExtractionGuard,
        SharedProgress,
    },
    MAX_DOWNLOAD_RETRIES, RETRY_BACKOFF_SECS,
};
use blake3::Hasher;
use eyre::Result;
use futures::stream::{self, StreamExt};
use reth_cli_util::cancellation::CancellationToken;
use reth_fs_util as fs;
use std::{
    io::Read,
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::task;
use tracing::{info, warn};

const DOWNLOAD_CACHE_DIR: &str = ".download-cache";

pub(crate) async fn run_modular_downloads(
    planned_downloads: PlannedDownloads,
    target_dir: &Path,
    download_concurrency: usize,
    cancel_token: CancellationToken,
) -> Result<()> {
    let download_cache_dir = target_dir.join(DOWNLOAD_CACHE_DIR);
    fs::create_dir_all(&download_cache_dir)?;

    let shared = SharedProgress::new(
        planned_downloads.total_size,
        planned_downloads.total_archives() as u64,
        cancel_token.clone(),
    );
    let request_limiter = DownloadRequestLimiter::new(download_concurrency);
    let progress_handle = spawn_progress_display(Arc::clone(&shared));

    let target = target_dir.to_path_buf();
    let cache_dir = Some(download_cache_dir);
    let results: Vec<Result<()>> = stream::iter(planned_downloads.archives)
        .map(|planned| {
            let dir = target.clone();
            let cache = cache_dir.clone();
            let shared_progress = Arc::clone(&shared);
            let limiter = Arc::clone(&request_limiter);
            let cancellation = cancel_token.clone();
            async move {
                process_modular_archive(
                    planned,
                    &dir,
                    cache.as_deref(),
                    Some(shared_progress),
                    Some(limiter),
                    cancellation,
                )
                .await
            }
        })
        .buffer_unordered(download_concurrency)
        .collect()
        .await;

    shared.done.store(true, Ordering::Relaxed);
    let _ = progress_handle.await;

    for result in results {
        result?;
    }

    Ok(())
}

async fn process_modular_archive(
    planned: PlannedArchive,
    target_dir: &Path,
    cache_dir: Option<&Path>,
    shared: Option<Arc<SharedProgress>>,
    request_limiter: Option<Arc<DownloadRequestLimiter>>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let target_dir = target_dir.to_path_buf();
    let cache_dir = cache_dir.map(Path::to_path_buf);

    task::spawn_blocking(move || {
        blocking_process_modular_archive(
            &planned,
            &target_dir,
            cache_dir.as_deref(),
            shared,
            request_limiter,
            cancel_token,
        )
    })
    .await??;

    Ok(())
}

enum ArchiveAttemptMode<'a> {
    Cached { cache_dir: &'a Path, request_limiter: &'a Arc<DownloadRequestLimiter> },
    Streaming { request_limiter: Option<&'a Arc<DownloadRequestLimiter>> },
}

impl<'a> ArchiveAttemptMode<'a> {
    fn new(
        cache_dir: Option<&'a Path>,
        request_limiter: Option<&'a Arc<DownloadRequestLimiter>>,
    ) -> Result<Self> {
        if let Some(cache_dir) = cache_dir {
            let request_limiter =
                request_limiter.ok_or_else(|| eyre::eyre!("Missing download request limiter"))?;
            return Ok(Self::Cached { cache_dir, request_limiter });
        }

        Ok(Self::Streaming { request_limiter })
    }

    const fn retries_attempt_errors(&self) -> bool {
        matches!(self, Self::Cached { .. })
    }

    fn run_attempt(
        &self,
        planned: &PlannedArchive,
        target_dir: &Path,
        format: CompressionFormat,
        shared: Option<&Arc<SharedProgress>>,
        cancel_token: &CancellationToken,
    ) -> Result<()> {
        match self {
            Self::Cached { cache_dir, request_limiter } => run_cached_archive_attempt(
                planned,
                target_dir,
                cache_dir,
                format,
                shared,
                request_limiter,
                cancel_token,
            ),
            Self::Streaming { request_limiter } => run_streaming_archive_attempt(
                &planned.archive,
                format,
                target_dir,
                shared,
                *request_limiter,
                cancel_token,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArchiveAttemptState {
    RunAttempt,
    VerifyOutputs,
    RetryAttempt,
    Complete,
    Fail,
}

fn blocking_process_modular_archive(
    planned: &PlannedArchive,
    target_dir: &Path,
    cache_dir: Option<&Path>,
    shared: Option<Arc<SharedProgress>>,
    request_limiter: Option<Arc<DownloadRequestLimiter>>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let archive = &planned.archive;
    if reuse_verified_outputs(planned, target_dir, shared.as_ref())? {
        info!(target: "reth::cli", file = %archive.file_name, component = %planned.component, "Skipping already verified plain files");
        return Ok(());
    }

    let mode = ArchiveAttemptMode::new(cache_dir, request_limiter.as_ref())?;
    let format = CompressionFormat::from_url(&archive.file_name)?;
    let mut attempt = 1;
    let mut last_error: Option<eyre::Error> = None;
    let mut state = ArchiveAttemptState::RunAttempt;

    loop {
        match state {
            ArchiveAttemptState::RunAttempt => {
                cleanup_output_files(target_dir, &archive.output_files);

                if attempt > 1 {
                    info!(target: "reth::cli",
                        file = %archive.file_name,
                        component = %planned.component,
                        attempt,
                        max = MAX_DOWNLOAD_RETRIES,
                        "Retrying archive from scratch"
                    );
                }

                match mode.run_attempt(planned, target_dir, format, shared.as_ref(), &cancel_token)
                {
                    Ok(()) => state = ArchiveAttemptState::VerifyOutputs,
                    Err(error) if mode.retries_attempt_errors() => {
                        warn!(target: "reth::cli",
                            file = %archive.file_name,
                            component = %planned.component,
                            attempt,
                            err = %format_args!("{error:#}"),
                            "Archive attempt failed, retrying from scratch"
                        );
                        last_error = Some(error);
                        state = ArchiveAttemptState::RetryAttempt;
                    }
                    Err(error) => return Err(error),
                }
            }
            ArchiveAttemptState::VerifyOutputs => {
                if verify_output_files(target_dir, &archive.output_files)? {
                    mark_archive_complete(shared.as_ref(), archive.size);
                    state = ArchiveAttemptState::Complete;
                } else {
                    warn!(target: "reth::cli", file = %archive.file_name, component = %planned.component, attempt, "Archive extracted, but output verification failed, retrying");
                    state = ArchiveAttemptState::RetryAttempt;
                }
            }
            ArchiveAttemptState::RetryAttempt => {
                if attempt >= MAX_DOWNLOAD_RETRIES {
                    state = ArchiveAttemptState::Fail;
                } else {
                    std::thread::sleep(Duration::from_secs(RETRY_BACKOFF_SECS));
                    attempt += 1;
                    state = ArchiveAttemptState::RunAttempt;
                }
            }
            ArchiveAttemptState::Complete => return Ok(()),
            ArchiveAttemptState::Fail => {
                if let Some(error) = last_error {
                    return Err(error.wrap_err(format!(
                        "Failed after {} attempts for {}",
                        MAX_DOWNLOAD_RETRIES, archive.file_name
                    )));
                }

                eyre::bail!(
                    "Failed integrity validation after {} attempts for {}",
                    MAX_DOWNLOAD_RETRIES,
                    archive.file_name
                )
            }
        }
    }
}

fn mark_archive_complete(shared: Option<&Arc<SharedProgress>>, bytes: u64) {
    if let Some(progress) = shared {
        progress.record_archive_complete(bytes);
    }
}

fn reuse_verified_outputs(
    planned: &PlannedArchive,
    target_dir: &Path,
    shared: Option<&Arc<SharedProgress>>,
) -> Result<bool> {
    if verify_output_files(target_dir, &planned.archive.output_files)? {
        mark_archive_complete(shared, planned.archive.size);
        return Ok(true);
    }

    Ok(false)
}

fn extract_cached_archive(
    archive_path: &Path,
    format: CompressionFormat,
    target_dir: &Path,
    shared: Option<&Arc<SharedProgress>>,
) -> Result<()> {
    let _extraction_guard = ExtractionGuard::new(shared);
    let file = fs::open(archive_path)?;
    extract_archive_raw(file, format, target_dir)
}

fn cleanup_cached_archive_files(archive_path: &Path, part_path: &Path) {
    let _ = fs::remove_file(archive_path);
    let _ = fs::remove_file(part_path);
}

fn run_cached_archive_attempt(
    planned: &PlannedArchive,
    target_dir: &Path,
    cache_dir: &Path,
    format: CompressionFormat,
    shared: Option<&Arc<SharedProgress>>,
    request_limiter: &Arc<DownloadRequestLimiter>,
    cancel_token: &CancellationToken,
) -> Result<()> {
    let archive = &planned.archive;
    let archive_path = cache_dir.join(&archive.file_name);
    let part_path = cache_dir.join(format!("{}.part", archive.file_name));

    let download_result = {
        let _download_guard = DownloadActivityGuard::new(shared);
        parallel_segmented_download(
            &archive.url,
            cache_dir,
            shared,
            request_limiter,
            cancel_token.clone(),
        )
    };

    let downloaded = match download_result {
        Ok(downloaded) => downloaded,
        Err(error) => {
            cleanup_cached_archive_files(&archive_path, &part_path);
            return Err(error);
        }
    };

    info!(target: "reth::cli",
        file = %archive.file_name,
        component = %planned.component,
        size = %super::progress::DownloadProgress::format_size(downloaded.size),
        "Archive download complete"
    );

    let extract_result = extract_cached_archive(&downloaded.path, format, target_dir, shared);
    cleanup_cached_archive_files(&archive_path, &part_path);
    extract_result
}

fn run_streaming_archive_attempt(
    archive: &super::manifest::ArchiveDescriptor,
    format: CompressionFormat,
    target_dir: &Path,
    shared: Option<&Arc<SharedProgress>>,
    request_limiter: Option<&Arc<DownloadRequestLimiter>>,
    cancel_token: &CancellationToken,
) -> Result<()> {
    let _download_guard = DownloadActivityGuard::new(shared);
    streaming_download_and_extract(
        &archive.url,
        format,
        target_dir,
        shared,
        request_limiter,
        cancel_token.clone(),
    )
}

pub(crate) fn verify_output_files(
    target_dir: &Path,
    output_files: &[super::manifest::OutputFileChecksum],
) -> Result<bool> {
    if output_files.is_empty() {
        return Ok(false);
    }

    for expected in output_files {
        let output_path = target_dir.join(&expected.path);
        let meta = match fs::metadata(&output_path) {
            Ok(meta) => meta,
            Err(_) => return Ok(false),
        };
        if meta.len() != expected.size {
            return Ok(false);
        }

        let actual = file_blake3_hex(&output_path)?;
        if !actual.eq_ignore_ascii_case(&expected.blake3) {
            return Ok(false);
        }
    }

    Ok(true)
}

fn cleanup_output_files(target_dir: &Path, output_files: &[super::manifest::OutputFileChecksum]) {
    for output in output_files {
        let _ = fs::remove_file(target_dir.join(&output.path));
    }
}

pub(crate) fn file_blake3_hex(path: &Path) -> Result<String> {
    let mut file = fs::open(path)?;
    let mut hasher = Hasher::new();
    let mut buf = [0_u8; 64 * 1024];

    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    Ok(hasher.finalize().to_hex().to_string())
}
