use super::{progress::*, RETRY_BACKOFF_SECS};
use eyre::Result;
use reqwest::{blocking::Client as BlockingClient, header::RANGE, StatusCode};
use reth_cli_util::cancellation::CancellationToken;
use reth_fs_util as fs;
use std::{
    any::Any,
    collections::VecDeque,
    fs::OpenOptions,
    io::{self, BufWriter, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tracing::info;
use url::Url;

/// Maximum retry attempts for a single download segment.
pub(crate) const SEGMENT_RETRY_ATTEMPTS: u32 = 3;

/// Minimum archive size that benefits from segmented downloads.
pub(crate) const SEGMENTED_DOWNLOAD_MIN_FILE_SIZE: u64 = 128 * 1024 * 1024;

/// Piece sizes are intentionally large to reduce request fanout while still
/// leaving enough queue depth to saturate fast links.
pub(crate) const SEGMENTED_DOWNLOAD_SMALL_PIECE_SIZE: u64 = 32 * 1024 * 1024;
const SEGMENTED_DOWNLOAD_LARGE_PIECE_SIZE: u64 = 64 * 1024 * 1024;

/// Cap exponential piece retry backoff to avoid overly long stalls.
const SEGMENTED_DOWNLOAD_MAX_BACKOFF_SECS: u64 = 30;

/// Segmented piece requests should fail fast enough to recover from hung tails.
const SEGMENTED_DOWNLOAD_REQUEST_TIMEOUT_SECS: u64 = 120;

#[derive(Debug, Clone)]
pub(crate) struct DownloadPaths {
    file_name: String,
    final_path: PathBuf,
    part_path: PathBuf,
}

#[derive(Debug, Clone)]
pub(crate) struct DownloadedArchive {
    pub(crate) path: PathBuf,
    pub(crate) size: u64,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RemoteArchiveProbe {
    pub(crate) total_size: u64,
    pub(crate) supports_ranges: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SequentialDownloadFallback {
    NoRangeSupport,
    EmptyFile,
    TooSmall,
}

#[derive(Debug)]
pub(crate) enum FetchStrategy {
    Sequential(SequentialDownloadFallback),
    Segmented(SegmentedDownloadPlan),
}

fn download_paths(url: &str, target_dir: &Path) -> DownloadPaths {
    let file_name = Url::parse(url)
        .ok()
        .and_then(|u| u.path_segments()?.next_back().map(|s| s.to_string()))
        .unwrap_or_else(|| "snapshot.tar".to_string());

    DownloadPaths {
        final_path: target_dir.join(&file_name),
        part_path: target_dir.join(format!("{file_name}.part")),
        file_name,
    }
}

fn probe_remote_archive(client: &BlockingClient, url: &str) -> Result<RemoteArchiveProbe> {
    let probe =
        client.get(url).header(RANGE, "bytes=0-0").send().and_then(|r| r.error_for_status());

    let (supports_ranges, total_size) = match probe {
        Ok(resp) if resp.status() == StatusCode::PARTIAL_CONTENT => {
            let total = resp
                .headers()
                .get("Content-Range")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split('/').next_back())
                .and_then(|v| v.parse::<u64>().ok());
            (true, total)
        }
        _ => {
            let head = client.head(url).send()?.error_for_status()?;
            (false, head.content_length())
        }
    };

    Ok(RemoteArchiveProbe {
        total_size: total_size.ok_or_else(|| eyre::eyre!("Server did not return file size"))?,
        supports_ranges,
    })
}

pub(crate) fn choose_fetch_strategy(
    probe: RemoteArchiveProbe,
    max_workers: usize,
) -> FetchStrategy {
    if !probe.supports_ranges {
        return FetchStrategy::Sequential(SequentialDownloadFallback::NoRangeSupport);
    }

    if probe.total_size == 0 {
        return FetchStrategy::Sequential(SequentialDownloadFallback::EmptyFile);
    }

    plan_segmented_download(probe.total_size, max_workers)
        .map(FetchStrategy::Segmented)
        .unwrap_or(FetchStrategy::Sequential(SequentialDownloadFallback::TooSmall))
}

/// Wrapper that tracks download progress while writing data.
/// Used with [`io::copy`] to display progress during downloads.
struct ProgressWriter<W> {
    inner: W,
    progress: DownloadProgress,
    cancel_token: CancellationToken,
}

impl<W: Write> Write for ProgressWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.cancel_token.is_cancelled() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "download cancelled"));
        }
        let n = self.inner.write(buf)?;
        let _ = self.progress.update(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Downloads a file with resume support using HTTP Range requests.
/// Automatically retries on failure, resuming from where it left off.
/// Returns the path to the downloaded file and its total size.
///
/// When `shared` is provided, progress is reported to the shared counter
/// (for parallel downloads). Otherwise uses a local progress bar.
fn resumable_download(
    url: &str,
    target_dir: &Path,
    shared: Option<&Arc<SharedProgress>>,
    request_limiter: Option<&Arc<DownloadRequestLimiter>>,
    cancel_token: CancellationToken,
    max_download_retries: u32,
) -> Result<DownloadedArchive> {
    let paths = download_paths(url, target_dir);

    let quiet = shared.is_some();

    if !quiet {
        info!(target: "reth::cli", file = %paths.file_name, "Connecting to download server");
    }
    let client = BlockingClient::builder().timeout(Duration::from_secs(30)).build()?;

    let mut total_size: Option<u64> = None;
    let mut last_error: Option<eyre::Error> = None;

    let finalize_download = |size: u64| -> Result<DownloadedArchive> {
        fs::rename(&paths.part_path, &paths.final_path)?;
        if !quiet {
            info!(target: "reth::cli", file = %paths.file_name, "Download complete");
        }
        Ok(DownloadedArchive { path: paths.final_path.clone(), size })
    };

    for attempt in 1..=max_download_retries {
        let existing_size = fs::metadata(&paths.part_path).map(|m| m.len()).unwrap_or(0);

        if let Some(total) = total_size &&
            existing_size >= total
        {
            return finalize_download(total);
        }

        if attempt > 1 {
            info!(target: "reth::cli",
                file = %paths.file_name,
                "Retry attempt {}/{} - resuming from {} bytes",
                attempt, max_download_retries, existing_size
            );
        }

        let mut request = client.get(url);
        if existing_size > 0 {
            request = request.header(RANGE, format!("bytes={existing_size}-"));
            if !quiet && attempt == 1 {
                info!(target: "reth::cli", file = %paths.file_name, "Resuming from {} bytes", existing_size);
            }
        }

        let _request_permit =
            request_limiter.map(|limiter| limiter.acquire(shared, &cancel_token)).transpose()?;

        let response = match request.send().and_then(|r| r.error_for_status()) {
            Ok(r) => r,
            Err(error) => {
                last_error = Some(error.into());
                if attempt < max_download_retries {
                    info!(target: "reth::cli",
                        file = %paths.file_name,
                        "Download failed, retrying in {RETRY_BACKOFF_SECS}s..."
                    );
                    std::thread::sleep(Duration::from_secs(RETRY_BACKOFF_SECS));
                }
                continue;
            }
        };

        let is_partial = response.status() == StatusCode::PARTIAL_CONTENT;
        let size = if is_partial {
            response
                .headers()
                .get("Content-Range")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.split('/').next_back())
                .and_then(|v| v.parse().ok())
        } else {
            response.content_length()
        };

        if total_size.is_none() {
            total_size = size;
            if !quiet && let Some(s) = size {
                info!(target: "reth::cli",
                    file = %paths.file_name,
                    size = %DownloadProgress::format_size(s),
                    "Downloading"
                );
            }
        }

        let current_total = total_size.ok_or_else(|| {
            eyre::eyre!("Server did not provide Content-Length or Content-Range header")
        })?;

        let file = if is_partial && existing_size > 0 {
            OpenOptions::new()
                .append(true)
                .open(&paths.part_path)
                .map_err(|e| fs::FsPathError::open(e, &paths.part_path))?
        } else {
            fs::create_file(&paths.part_path)?
        };

        let start_offset = if is_partial { existing_size } else { 0 };
        let mut reader = response;

        let copy_result;
        let flush_result;

        if let Some(progress) = shared {
            let mut writer = SharedProgressWriter {
                inner: BufWriter::new(file),
                progress: Arc::clone(progress),
            };
            copy_result = io::copy(&mut reader, &mut writer);
            flush_result = writer.inner.flush();
        } else {
            let mut progress = DownloadProgress::new(current_total);
            progress.downloaded = start_offset;
            let mut writer = ProgressWriter {
                inner: BufWriter::new(file),
                progress,
                cancel_token: cancel_token.clone(),
            };
            copy_result = io::copy(&mut reader, &mut writer);
            flush_result = writer.inner.flush();
            println!();
        }

        if let Err(error) = copy_result.and(flush_result) {
            last_error = Some(error.into());
            if attempt < max_download_retries {
                info!(target: "reth::cli",
                    file = %paths.file_name,
                    "Download interrupted, retrying in {RETRY_BACKOFF_SECS}s..."
                );
                std::thread::sleep(Duration::from_secs(RETRY_BACKOFF_SECS));
            }
            continue;
        }

        return finalize_download(current_total);
    }

    Err(last_error
        .unwrap_or_else(|| eyre::eyre!("Download failed after {} attempts", max_download_retries)))
}

/// One queued byte range for a segmented archive download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DownloadPiece {
    pub(crate) start: u64,
    pub(crate) end: u64,
}

/// Static plan for a segmented archive: piece size, queue depth, and worker fanout.
#[derive(Debug)]
pub(crate) struct SegmentedDownloadPlan {
    pub(crate) piece_size: u64,
    pub(crate) piece_count: usize,
    pub(crate) worker_count: usize,
    pieces: VecDeque<DownloadPiece>,
}

/// Shared queue state for one segmented archive download.
///
/// Workers pop pieces until the queue is drained or one worker marks the whole
/// archive attempt as failed.
struct SegmentedDownloadState {
    pieces: Mutex<VecDeque<DownloadPiece>>,
    failed: AtomicBool,
}

impl SegmentedDownloadState {
    fn new(pieces: VecDeque<DownloadPiece>) -> Self {
        Self { pieces: Mutex::new(pieces), failed: AtomicBool::new(false) }
    }

    fn next_piece(&self, cancel_token: &CancellationToken) -> Option<DownloadPiece> {
        if cancel_token.is_cancelled() || self.failed.load(Ordering::Relaxed) {
            return None;
        }

        self.pieces.lock().unwrap().pop_front()
    }

    fn note_terminal_failure(&self) {
        self.failed.store(true, Ordering::Relaxed);
    }
}

#[derive(Default)]
struct TerminalFailure {
    error: Mutex<Option<eyre::Error>>,
}

impl TerminalFailure {
    fn record(&self, error: eyre::Error) {
        let mut slot = self.error.lock().unwrap();
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    fn take(&self) -> Option<eyre::Error> {
        self.error.lock().unwrap().take()
    }
}

pub(crate) fn build_download_pieces(total_size: u64, piece_size: u64) -> VecDeque<DownloadPiece> {
    let mut pieces = VecDeque::new();
    let mut start = 0;

    while start < total_size {
        let end = (start + piece_size).min(total_size) - 1;
        pieces.push_back(DownloadPiece { start, end });
        start = end + 1;
    }

    pieces
}

/// Chooses the fixed piece size for a large archive.
///
/// Smaller large files use 32 MiB pieces to keep enough queue depth. Very large
/// files use 64 MiB pieces to reduce HTTP request overhead.
fn segmented_piece_size(total_size: u64) -> u64 {
    if total_size < 2 * 1024 * 1024 * 1024 {
        SEGMENTED_DOWNLOAD_SMALL_PIECE_SIZE
    } else {
        SEGMENTED_DOWNLOAD_LARGE_PIECE_SIZE
    }
}

/// Builds the segmented download plan for one archive.
///
/// Small files stay single-stream; otherwise the archive is split into fixed
/// pieces and allowed to use up to the global request limit.
pub(crate) fn plan_segmented_download(
    total_size: u64,
    max_workers: usize,
) -> Option<SegmentedDownloadPlan> {
    if max_workers == 0 || total_size < SEGMENTED_DOWNLOAD_MIN_FILE_SIZE {
        return None;
    }

    let piece_size = segmented_piece_size(total_size);
    if total_size <= piece_size {
        return None;
    }

    let pieces = build_download_pieces(total_size, piece_size);
    let piece_count = pieces.len();
    let worker_count = max_workers.min(piece_count).max(1);

    Some(SegmentedDownloadPlan { piece_size, piece_count, worker_count, pieces })
}

fn piece_retry_backoff(attempt: u32, throttled: bool) -> Duration {
    let base = if throttled { 2 } else { RETRY_BACKOFF_SECS };
    let multiplier = 1u64 << attempt.saturating_sub(1).min(3);
    Duration::from_secs(base.saturating_mul(multiplier).min(SEGMENTED_DOWNLOAD_MAX_BACKOFF_SECS))
}

fn is_retryable_piece_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT |
            StatusCode::TOO_MANY_REQUESTS |
            StatusCode::INTERNAL_SERVER_ERROR |
            StatusCode::BAD_GATEWAY |
            StatusCode::SERVICE_UNAVAILABLE |
            StatusCode::GATEWAY_TIMEOUT
    )
}

pub(crate) fn should_retry_piece_status(status: StatusCode) -> bool {
    status == StatusCode::OK || is_retryable_piece_status(status)
}

fn is_throttle_piece_status(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::REQUEST_TIMEOUT |
            StatusCode::TOO_MANY_REQUESTS |
            StatusCode::SERVICE_UNAVAILABLE |
            StatusCode::GATEWAY_TIMEOUT
    )
}

fn is_throttle_piece_error(error: &reqwest::Error) -> bool {
    error.is_timeout() || matches!(error.status(), Some(status) if is_throttle_piece_status(status))
}

enum PieceAttemptFailure {
    Retryable { error: eyre::Error, throttled: bool },
    Terminal(eyre::Error),
}

fn panic_payload_message(payload: Box<dyn Any + Send + 'static>) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Downloads one queued piece once.
fn download_piece_once(
    client: &BlockingClient,
    url: &str,
    file: &std::fs::File,
    piece: DownloadPiece,
    shared: Option<&Arc<SharedProgress>>,
    cancel_token: &CancellationToken,
) -> std::result::Result<(), PieceAttemptFailure> {
    use std::os::unix::fs::FileExt;

    let expected_len = piece.end - piece.start + 1;

    let response = match client
        .get(url)
        .header(RANGE, format!("bytes={}-{}", piece.start, piece.end))
        .send()
    {
        Ok(response) if response.status() == StatusCode::PARTIAL_CONTENT => response,
        Ok(response) if should_retry_piece_status(response.status()) => {
            return Err(PieceAttemptFailure::Retryable {
                error: eyre::eyre!(
                    "Server returned {} for piece {}-{}",
                    response.status(),
                    piece.start,
                    piece.end
                ),
                throttled: is_throttle_piece_status(response.status()),
            });
        }
        Ok(response) => {
            return Err(PieceAttemptFailure::Terminal(eyre::eyre!(
                "Server returned {} instead of 206 for Range request",
                response.status()
            )));
        }
        Err(error) => {
            return Err(PieceAttemptFailure::Retryable {
                throttled: is_throttle_piece_error(&error),
                error: error.into(),
            });
        }
    };

    let mut buf = [0u8; 64 * 1024];
    let mut reader = response.take(expected_len);
    let mut offset = piece.start;

    loop {
        if cancel_token.is_cancelled() {
            return Err(PieceAttemptFailure::Terminal(eyre::eyre!("Download cancelled")));
        }

        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                file.write_all_at(&buf[..n], offset)
                    .map_err(|error| PieceAttemptFailure::Terminal(error.into()))?;
                offset += n as u64;
                if let Some(progress) = shared {
                    progress.record_fetched_bytes(n as u64);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                return Err(PieceAttemptFailure::Retryable {
                    throttled: error.kind() == io::ErrorKind::TimedOut,
                    error: error.into(),
                });
            }
        }
    }

    let downloaded_len = offset - piece.start;
    if downloaded_len == expected_len {
        return Ok(());
    }

    Err(PieceAttemptFailure::Retryable {
        error: eyre::eyre!(
            "Piece {}-{} ended early: expected {} bytes, downloaded {}",
            piece.start,
            piece.end,
            expected_len,
            downloaded_len
        ),
        throttled: false,
    })
}

/// Downloads one queued piece with per-piece retry/backoff.
///
/// Each attempt acquires a permit from the global request pool so whole-file and
/// piece downloads compete for the same fixed number of HTTP request slots.
fn download_piece_with_retries(
    client: &BlockingClient,
    url: &str,
    file: &std::fs::File,
    piece: DownloadPiece,
    shared: Option<&Arc<SharedProgress>>,
    request_limiter: &DownloadRequestLimiter,
    cancel_token: &CancellationToken,
) -> Result<()> {
    for attempt in 1..=SEGMENT_RETRY_ATTEMPTS {
        if cancel_token.is_cancelled() {
            return Err(eyre::eyre!("Download cancelled"));
        }

        let _request_permit = request_limiter.acquire(shared, cancel_token)?;
        match download_piece_once(client, url, file, piece, shared, cancel_token) {
            Ok(()) => return Ok(()),
            Err(PieceAttemptFailure::Retryable { error: _, throttled })
                if attempt < SEGMENT_RETRY_ATTEMPTS =>
            {
                std::thread::sleep(piece_retry_backoff(attempt, throttled));
            }
            Err(PieceAttemptFailure::Retryable { error, .. }) => return Err(error),
            Err(PieceAttemptFailure::Terminal(error)) => return Err(error),
        }
    }

    Err(eyre::eyre!("Piece download failed after {SEGMENT_RETRY_ATTEMPTS} attempts"))
}

fn execute_segmented_download(
    url: &str,
    paths: &DownloadPaths,
    total_size: u64,
    plan: SegmentedDownloadPlan,
    shared: Option<&Arc<SharedProgress>>,
    request_limiter: &Arc<DownloadRequestLimiter>,
    cancel_token: CancellationToken,
) -> Result<DownloadedArchive> {
    {
        let file = fs::create_file(&paths.part_path)?;
        file.set_len(total_size)?;
    }

    let state = Arc::new(SegmentedDownloadState::new(plan.pieces));
    let terminal_failure = Arc::new(TerminalFailure::default());
    let worker_client = BlockingClient::builder()
        .connect_timeout(Duration::from_secs(30))
        .timeout(Duration::from_secs(SEGMENTED_DOWNLOAD_REQUEST_TIMEOUT_SECS))
        .build()?;

    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(plan.worker_count);

        for _ in 0..plan.worker_count {
            let part_path = &paths.part_path;
            let state = Arc::clone(&state);
            let terminal_failure = Arc::clone(&terminal_failure);
            let client = worker_client.clone();
            let request_limiter = Arc::clone(request_limiter);
            let ct = &cancel_token;

            handles.push(scope.spawn(move || {
                let file = match OpenOptions::new().write(true).open(part_path) {
                    Ok(file) => file,
                    Err(error) => {
                        state.note_terminal_failure();
                        terminal_failure.record(error.into());
                        return;
                    }
                };

                while let Some(piece) = state.next_piece(ct) {
                    if let Err(error) = download_piece_with_retries(
                        &client,
                        url,
                        &file,
                        piece,
                        shared,
                        &request_limiter,
                        ct,
                    ) {
                        state.note_terminal_failure();
                        terminal_failure.record(error);
                        return;
                    }
                }
            }));
        }

        for handle in handles {
            if let Err(payload) = handle.join() {
                state.note_terminal_failure();
                terminal_failure.record(eyre::eyre!(
                    "Segmented download worker panicked: {}",
                    panic_payload_message(payload)
                ));
            }
        }
    });

    if let Some(error) = terminal_failure.take() {
        let _ = std::fs::remove_file(&paths.part_path);
        return Err(error.wrap_err("Parallel download failed"));
    }

    fs::rename(&paths.part_path, &paths.final_path)?;
    info!(target: "reth::cli", file = %paths.file_name, "Download complete");
    Ok(DownloadedArchive { path: paths.final_path.clone(), size: total_size })
}

/// Downloads a file using parallel HTTP Range requests.
///
/// The file is split into large fixed-size pieces and a bounded worker pool pulls
/// from the per-file queue. Each piece request acquires a slot from the global
/// request pool, so small whole-file downloads and large-file pieces all share
/// the same backpressure mechanism. Falls back to [`resumable_download`] when
/// the server does not support Range requests or the file is too small.
pub(crate) fn parallel_segmented_download(
    url: &str,
    target_dir: &Path,
    shared: Option<&Arc<SharedProgress>>,
    request_limiter: &Arc<DownloadRequestLimiter>,
    cancel_token: CancellationToken,
) -> Result<DownloadedArchive> {
    let paths = download_paths(url, target_dir);

    let client = BlockingClient::builder().connect_timeout(Duration::from_secs(30)).build()?;

    let probe = probe_remote_archive(&client, url)?;
    let total_size = probe.total_size;

    let strategy = choose_fetch_strategy(probe, request_limiter.max_concurrency());
    let plan = match strategy {
        FetchStrategy::Sequential(SequentialDownloadFallback::NoRangeSupport) => {
            info!(target: "reth::cli",
                "Server does not support Range requests, falling back to sequential download"
            );
            return resumable_download(
                url,
                target_dir,
                shared,
                Some(request_limiter),
                cancel_token,
                super::MAX_DOWNLOAD_RETRIES,
            );
        }
        FetchStrategy::Sequential(SequentialDownloadFallback::EmptyFile) => {
            info!(target: "reth::cli",
                file = %paths.file_name,
                "Remote archive is empty, falling back to sequential download"
            );
            return resumable_download(
                url,
                target_dir,
                shared,
                Some(request_limiter),
                cancel_token,
                super::MAX_DOWNLOAD_RETRIES,
            );
        }
        FetchStrategy::Sequential(SequentialDownloadFallback::TooSmall) => {
            info!(target: "reth::cli",
                total_size = %DownloadProgress::format_size(total_size),
                "Archive too small for segmented download, falling back to single-stream download"
            );
            return resumable_download(
                url,
                target_dir,
                shared,
                Some(request_limiter),
                cancel_token,
                super::MAX_DOWNLOAD_RETRIES,
            );
        }
        FetchStrategy::Segmented(plan) => plan,
    };

    info!(target: "reth::cli",
        total_size = %DownloadProgress::format_size(total_size),
        piece_size = %DownloadProgress::format_size(plan.piece_size),
        pieces = plan.piece_count,
        workers = plan.worker_count,
        max_concurrent_requests = request_limiter.max_concurrency(),
        "Starting queued segmented download"
    );

    execute_segmented_download(url, &paths, total_size, plan, shared, request_limiter, cancel_token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn segmented_plan_skips_small_files() {
        assert!(plan_segmented_download(SEGMENTED_DOWNLOAD_MIN_FILE_SIZE - 1, 16).is_none());
    }

    #[test]
    fn segmented_plan_uses_large_pieces_and_adaptive_workers() {
        let total_size = 512 * 1024 * 1024;
        let plan = plan_segmented_download(total_size, 32).unwrap();

        assert_eq!(plan.piece_size, SEGMENTED_DOWNLOAD_SMALL_PIECE_SIZE);
        assert_eq!(plan.piece_count, 16);
        assert_eq!(plan.worker_count, 16);
    }

    #[test]
    fn build_download_pieces_covers_entire_file() {
        let pieces = build_download_pieces(10, 4).into_iter().collect::<Vec<_>>();

        assert_eq!(
            pieces,
            vec![
                DownloadPiece { start: 0, end: 3 },
                DownloadPiece { start: 4, end: 7 },
                DownloadPiece { start: 8, end: 9 },
            ]
        );
    }

    #[test]
    fn piece_status_retry_policy_retries_200_ok() {
        assert!(should_retry_piece_status(StatusCode::OK));
        assert!(should_retry_piece_status(StatusCode::TOO_MANY_REQUESTS));
        assert!(!should_retry_piece_status(StatusCode::NOT_FOUND));
    }

    #[test]
    fn choose_fetch_strategy_uses_segmented_when_ranges_are_supported() {
        let strategy = choose_fetch_strategy(
            RemoteArchiveProbe { total_size: 512 * 1024 * 1024, supports_ranges: true },
            16,
        );

        assert!(matches!(strategy, FetchStrategy::Segmented(_)));
    }

    #[test]
    fn choose_fetch_strategy_falls_back_without_ranges() {
        let strategy = choose_fetch_strategy(
            RemoteArchiveProbe { total_size: 512 * 1024 * 1024, supports_ranges: false },
            16,
        );

        assert!(matches!(
            strategy,
            FetchStrategy::Sequential(SequentialDownloadFallback::NoRangeSupport)
        ));
    }
}
