//! This crate defines a set of commonly used cli utils.

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "tracy-allocator")]
use reth_tracing as _;
#[cfg(feature = "tracy-allocator")]
use tracy_client as _;

pub mod allocator;
pub mod cancellation;

/// Helper function to load a secret key from a file.
pub mod load_secret_key;
pub use load_secret_key::{get_secret_key, parse_secret_key_from_hex};

/// Cli parsers functions.
pub mod parsers;
pub use parsers::{
    hash_or_num_value_parser, parse_duration_from_secs, parse_duration_from_secs_or_ms,
    parse_ether_value, parse_socket_address,
};

#[cfg(all(unix, any(target_env = "gnu", target_os = "macos")))]
pub mod sigsegv_handler;

/// Signal handler to extract a backtrace from stack overflow.
///
/// This is a no-op because this platform doesn't support our signal handler's requirements.
#[cfg(not(all(unix, any(target_env = "gnu", target_os = "macos"))))]
pub mod sigsegv_handler {
    /// No-op function.
    pub fn install() {}
}

/// Installs process-wide signal handlers and other one-time setup.
///
/// Currently this:
/// - Installs a SIGSEGV handler that prints a backtrace on stack overflow.
/// - Ignores SIGPIPE so broken pipes (e.g. `reth | tee`) result in `EPIPE` errors instead of
///   killing the process, allowing graceful shutdown to run.
pub fn init() {
    sigsegv_handler::install();

    #[cfg(unix)]
    ignore_sigpipe();
}

/// Ignores SIGPIPE so that writes to broken pipes return `EPIPE` instead of killing the process.
///
/// Without this, piping reth output through another program (e.g. `reth node | tee log`) can
/// cause an ungraceful shutdown: when the pipe reader is killed, the next write to stdout
/// delivers SIGPIPE which terminates reth immediately, skipping cleanup and leaving static files
/// inconsistent with database checkpoints.
#[cfg(unix)]
fn ignore_sigpipe() {
    // SAFETY: `signal()` with `SIG_IGN` is async-signal-safe and has no preconditions.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}
