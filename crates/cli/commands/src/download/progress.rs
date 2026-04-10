use eyre::Result;
use reth_cli_util::cancellation::CancellationToken;
use std::{
    io::{self, Read, Write},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};
use tracing::info;

const BYTE_UNITS: [&str; 4] = ["B", "KB", "MB", "GB"];

/// Tracks download progress and throttles display updates to every 100ms.
pub(crate) struct DownloadProgress {
    /// Bytes copied so far for this single download.
    pub(crate) downloaded: u64,
    /// Total bytes expected for this single download.
    total_size: u64,
    /// Time when the progress line was last printed.
    last_displayed: Instant,
    /// Time when this progress tracker started.
    started_at: Instant,
}

impl DownloadProgress {
    /// Creates new progress tracker with given total size
    pub(crate) fn new(total_size: u64) -> Self {
        let now = Instant::now();
        Self { downloaded: 0, total_size, last_displayed: now, started_at: now }
    }

    /// Converts bytes to human readable format (B, KB, MB, GB)
    pub(crate) fn format_size(size: u64) -> String {
        let mut size = size as f64;
        let mut unit_index = 0;

        while size >= 1024.0 && unit_index < BYTE_UNITS.len() - 1 {
            size /= 1024.0;
            unit_index += 1;
        }

        format!("{:.2} {}", size, BYTE_UNITS[unit_index])
    }

    /// Format duration as human readable string
    pub(crate) fn format_duration(duration: Duration) -> String {
        let secs = duration.as_secs();
        if secs < 60 {
            format!("{secs}s")
        } else if secs < 3600 {
            format!("{}m {}s", secs / 60, secs % 60)
        } else {
            format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
        }
    }

    /// Updates progress bar (for single-archive legacy downloads)
    pub(crate) fn update(&mut self, chunk_size: u64) -> Result<()> {
        self.downloaded += chunk_size;

        if self.last_displayed.elapsed() >= Duration::from_millis(100) {
            let formatted_downloaded = Self::format_size(self.downloaded);
            let formatted_total = Self::format_size(self.total_size);
            let progress = (self.downloaded as f64 / self.total_size as f64) * 100.0;

            let elapsed = self.started_at.elapsed();
            let eta = if self.downloaded > 0 {
                let remaining = self.total_size.saturating_sub(self.downloaded);
                let speed = self.downloaded as f64 / elapsed.as_secs_f64();
                if speed > 0.0 {
                    Duration::from_secs_f64(remaining as f64 / speed)
                } else {
                    Duration::ZERO
                }
            } else {
                Duration::ZERO
            };
            let eta_str = Self::format_duration(eta);

            print!(
                "\rDownloading and extracting... {progress:.2}% ({formatted_downloaded} / {formatted_total}) ETA: {eta_str}     ",
            );
            io::stdout().flush()?;
            self.last_displayed = Instant::now();
        }

        Ok(())
    }
}

/// Shared progress counters for parallel downloads.
///
/// Download threads increment `fetched_bytes`. Verified archives increment
/// `completed_bytes`. A background task reads both counters and prints one
/// progress line for the whole job.
pub(crate) struct SharedProgress {
    /// Bytes fetched so far across all archive attempts.
    pub(crate) fetched_bytes: AtomicU64,
    /// Bytes whose archives have fully verified.
    pub(crate) completed_bytes: AtomicU64,
    /// Total bytes expected across all planned archives.
    pub(crate) total_size: u64,
    /// Total number of planned archives.
    pub(crate) total_archives: u64,
    /// Time when the modular download job started.
    pub(crate) started_at: Instant,
    /// Number of archives that have fully finished.
    pub(crate) archives_done: AtomicU64,
    /// Number of archives currently in the fetch phase.
    pub(crate) active_downloads: AtomicU64,
    /// Number of in-flight HTTP requests.
    pub(crate) active_download_requests: AtomicU64,
    /// Number of archives currently extracting.
    pub(crate) active_extractions: AtomicU64,
    /// Signals the background progress task to exit.
    pub(crate) done: AtomicBool,
    /// Cancellation token shared by the whole command.
    cancel_token: CancellationToken,
}

impl SharedProgress {
    /// Creates the shared progress state for a modular download job.
    pub(crate) fn new(
        total_size: u64,
        total_archives: u64,
        cancel_token: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            fetched_bytes: AtomicU64::new(0),
            completed_bytes: AtomicU64::new(0),
            total_size,
            total_archives,
            started_at: Instant::now(),
            archives_done: AtomicU64::new(0),
            active_downloads: AtomicU64::new(0),
            active_download_requests: AtomicU64::new(0),
            active_extractions: AtomicU64::new(0),
            done: AtomicBool::new(false),
            cancel_token,
        })
    }

    /// Returns whether the whole command has been cancelled.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// Adds fetched bytes without marking any archive complete.
    pub(crate) fn record_fetched_bytes(&self, bytes: u64) {
        self.fetched_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Adds bytes whose archive outputs have fully verified.
    pub(crate) fn record_completed_bytes(&self, bytes: u64) {
        self.completed_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Increments the count of finished archives.
    pub(crate) fn record_archive_finished(&self) {
        self.archives_done.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks one archive as actively downloading.
    pub(crate) fn download_started(&self) {
        self.active_downloads.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks one archive download as finished.
    pub(crate) fn download_finished(&self) {
        let _ = self
            .active_downloads
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
    }

    /// Marks one HTTP request as in flight.
    pub(crate) fn request_started(&self) {
        self.active_download_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks one HTTP request as finished.
    pub(crate) fn request_finished(&self) {
        let _ =
            self.active_download_requests
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
    }

    /// Marks one archive as actively extracting.
    pub(crate) fn extraction_started(&self) {
        self.active_extractions.fetch_add(1, Ordering::Relaxed);
    }

    /// Marks one archive extraction as finished.
    pub(crate) fn extraction_finished(&self) {
        let _ = self
            .active_extractions
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1));
    }

    /// Records a fully verified archive and its completed byte count.
    pub(crate) fn record_archive_complete(&self, bytes: u64) {
        self.record_completed_bytes(bytes);
        self.record_archive_finished();
    }
}

/// Global request limit for the blocking downloader.
///
/// This uses `Mutex + Condvar` because the segmented path runs blocking reqwest
/// clients on OS threads.
pub(crate) struct DownloadRequestLimiter {
    /// Maximum number of in-flight HTTP requests.
    limit: usize,
    /// Current number of acquired request slots.
    active: Mutex<usize>,
    /// Wakes blocked threads when a slot is released.
    notify: Condvar,
}

impl DownloadRequestLimiter {
    /// Creates the shared request limiter.
    pub(crate) fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self { limit: limit.max(1), active: Mutex::new(0), notify: Condvar::new() })
    }

    /// Returns the configured request limit.
    pub(crate) fn max_concurrency(&self) -> usize {
        self.limit
    }

    pub(crate) fn acquire<'a>(
        &'a self,
        progress: Option<&'a Arc<SharedProgress>>,
        cancel_token: &CancellationToken,
    ) -> Result<DownloadRequestPermit<'a>> {
        let mut active = self.active.lock().unwrap();
        loop {
            if cancel_token.is_cancelled() {
                return Err(eyre::eyre!("Download cancelled"));
            }

            if *active < self.limit {
                *active += 1;
                if let Some(progress) = progress {
                    progress.request_started();
                }
                return Ok(DownloadRequestPermit { limiter: self, progress });
            }

            // Wake periodically so cancellation can interrupt waiters even if
            // no request finishes.
            let (next_active, _) =
                self.notify.wait_timeout(active, Duration::from_millis(100)).unwrap();
            active = next_active;
        }
    }
}

/// RAII permit for one in-flight HTTP request.
///
/// Dropping the permit releases a slot in the shared request limit and updates
/// the live progress counters.
pub(crate) struct DownloadRequestPermit<'a> {
    /// Limiter that owns the request slot.
    limiter: &'a DownloadRequestLimiter,
    /// Shared progress counters updated when the permit drops.
    progress: Option<&'a Arc<SharedProgress>>,
}

impl Drop for DownloadRequestPermit<'_> {
    /// Releases the request slot and updates shared progress counters.
    fn drop(&mut self) {
        let mut active = self.limiter.active.lock().unwrap();
        *active = active.saturating_sub(1);
        drop(active);
        self.limiter.notify.notify_one();

        if let Some(progress) = self.progress {
            progress.request_finished();
        }
    }
}

/// Marks one archive download as active for aggregate progress logging.
pub(crate) struct DownloadActivityGuard<'a> {
    /// Shared progress counters updated when the guard drops.
    progress: Option<&'a Arc<SharedProgress>>,
}

impl<'a> DownloadActivityGuard<'a> {
    /// Marks one archive download as active until the guard drops.
    pub(crate) fn new(progress: Option<&'a Arc<SharedProgress>>) -> Self {
        if let Some(progress) = progress {
            progress.download_started();
        }
        Self { progress }
    }
}

impl Drop for DownloadActivityGuard<'_> {
    /// Clears the active download count for this guard.
    fn drop(&mut self) {
        if let Some(progress) = self.progress {
            progress.download_finished();
        }
    }
}

/// Marks one archive extraction as active for aggregate progress logging.
pub(crate) struct ExtractionGuard<'a> {
    /// Shared progress counters updated when the guard drops.
    progress: Option<&'a Arc<SharedProgress>>,
}

impl<'a> ExtractionGuard<'a> {
    /// Marks one archive extraction as active until the guard drops.
    pub(crate) fn new(progress: Option<&'a Arc<SharedProgress>>) -> Self {
        if let Some(progress) = progress {
            progress.extraction_started();
        }
        Self { progress }
    }
}

impl Drop for ExtractionGuard<'_> {
    /// Clears the active extraction count for this guard.
    fn drop(&mut self) {
        if let Some(progress) = self.progress {
            progress.extraction_finished();
        }
    }
}

/// Adapter to track progress while reading (used for extraction in legacy path)
pub(crate) struct ProgressReader<R> {
    /// Wrapped reader that provides archive bytes.
    reader: R,
    /// Per-download progress tracker for legacy paths.
    progress: DownloadProgress,
    /// Cancellation token checked between reads.
    cancel_token: CancellationToken,
}

impl<R: Read> ProgressReader<R> {
    /// Wraps a reader with per-download progress tracking.
    pub(crate) fn new(reader: R, total_size: u64, cancel_token: CancellationToken) -> Self {
        Self { reader, progress: DownloadProgress::new(total_size), cancel_token }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    /// Reads bytes, checks cancellation, and updates the local progress bar.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.cancel_token.is_cancelled() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "download cancelled"));
        }
        let bytes = self.reader.read(buf)?;
        if bytes > 0 &&
            let Err(error) = self.progress.update(bytes as u64)
        {
            return Err(io::Error::other(error));
        }
        Ok(bytes)
    }
}

/// Wrapper that bumps a shared atomic counter while writing data.
/// Used for parallel downloads where a single display task shows aggregated progress.
pub(crate) struct SharedProgressWriter<W> {
    /// Wrapped writer receiving downloaded bytes.
    pub(crate) inner: W,
    /// Shared counters updated as bytes are written.
    pub(crate) progress: Arc<SharedProgress>,
}

impl<W: Write> Write for SharedProgressWriter<W> {
    /// Writes bytes and records them in shared progress.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.progress.is_cancelled() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "download cancelled"));
        }
        let n = self.inner.write(buf)?;
        self.progress.record_fetched_bytes(n as u64);
        Ok(n)
    }

    /// Flushes the wrapped writer.
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Wrapper that bumps a shared atomic counter while reading data.
/// Used for streaming downloads where a single display task shows aggregated progress.
pub(crate) struct SharedProgressReader<R> {
    /// Wrapped reader producing streamed bytes.
    pub(crate) inner: R,
    /// Shared counters updated as bytes are read.
    pub(crate) progress: Arc<SharedProgress>,
}

impl<R: Read> Read for SharedProgressReader<R> {
    /// Reads bytes and records them in shared progress.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.progress.is_cancelled() {
            return Err(io::Error::new(io::ErrorKind::Interrupted, "download cancelled"));
        }
        let n = self.inner.read(buf)?;
        self.progress.record_fetched_bytes(n as u64);
        Ok(n)
    }
}

/// Spawns a background task that prints aggregated download progress.
/// Returns a handle; drop it (or call `.abort()`) to stop.
pub(crate) fn spawn_progress_display(progress: Arc<SharedProgress>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(3));
        let mut in_extraction_phase = false;
        interval.tick().await;
        loop {
            interval.tick().await;

            if progress.done.load(Ordering::Relaxed) {
                break;
            }

            let fetched = progress.fetched_bytes.load(Ordering::Relaxed);
            let completed = progress.completed_bytes.load(Ordering::Relaxed);
            let total = progress.total_size;
            if total == 0 {
                continue;
            }

            let done = progress.archives_done.load(Ordering::Relaxed);
            let all = progress.total_archives;
            let active_downloads = progress.active_downloads.load(Ordering::Relaxed);
            let active_requests = progress.active_download_requests.load(Ordering::Relaxed);
            let active_extractions = progress.active_extractions.load(Ordering::Relaxed);
            let progress_bytes = fetched.max(completed).min(total);
            let pct = (progress_bytes as f64 / total as f64) * 100.0;
            let completed_display = DownloadProgress::format_size(completed);
            let elapsed = DownloadProgress::format_duration(progress.started_at.elapsed());
            let fetched_display = DownloadProgress::format_size(fetched);
            let tot = DownloadProgress::format_size(total);
            let extraction_phase =
                active_downloads == 0 && active_requests == 0 && active_extractions > 0;

            if extraction_phase && !in_extraction_phase {
                info!(target: "reth::cli",
                    elapsed = %elapsed,
                    active_extractions,
                    "All snapshot archive downloads complete, starting extraction"
                );
            }

            in_extraction_phase = extraction_phase;

            if extraction_phase {
                info!(target: "reth::cli",
                    archives = format_args!("{done}/{all}"),
                    progress = format_args!("{pct:.1}%"),
                    elapsed = %elapsed,
                    active_extractions,
                    completed = %completed_display,
                    total = %tot,
                    fetched_this_session = %fetched_display,
                    "Extracting snapshot archives"
                );
            } else {
                info!(target: "reth::cli",
                    archives = format_args!("{done}/{all}"),
                    progress = format_args!("{pct:.1}%"),
                    elapsed = %elapsed,
                    active_archive_downloads = active_downloads,
                    active_download_requests = active_requests,
                    completed = %completed_display,
                    total = %tot,
                    fetched_this_session = %fetched_display,
                    "Processing snapshot archives"
                );
            }
        }

        let fetched = progress.fetched_bytes.load(Ordering::Relaxed);
        let completed = progress.completed_bytes.load(Ordering::Relaxed);
        let fetched_display = DownloadProgress::format_size(fetched);
        let completed_display = DownloadProgress::format_size(completed);
        let tot = DownloadProgress::format_size(progress.total_size);
        info!(target: "reth::cli",
            completed = %completed_display,
            total = %tot,
            fetched_this_session = %fetched_display,
            "Snapshot archive processing complete"
        );
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn shared_progress_keeps_retry_bytes_out_of_completion_progress() {
        let progress = SharedProgress::new(10, 1, CancellationToken::new());

        progress.record_fetched_bytes(10);
        progress.record_fetched_bytes(10);
        progress.record_archive_complete(10);

        assert_eq!(progress.fetched_bytes.load(Ordering::Relaxed), 20);
        assert_eq!(progress.completed_bytes.load(Ordering::Relaxed), 10);
        assert_eq!(progress.archives_done.load(Ordering::Relaxed), 1);
    }
}
