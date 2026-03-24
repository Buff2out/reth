//! clap [Args](clap::Args) for revmc JIT configuration.

use clap::Args;

/// Parameters for JIT compilation of EVM bytecode via revmc.
#[derive(Debug, Clone, Args, PartialEq, Eq)]
#[command(next_help_heading = "JIT")]
pub struct JitArgs {
    /// Enable JIT compilation of EVM bytecode.
    #[arg(long = "jit", default_value_t = false, help_heading = "JIT")]
    pub enabled: bool,

    /// Number of observed misses before a bytecode is promoted to JIT compilation.
    #[arg(long = "jit.hot-threshold", default_value_t = Self::DEFAULT_HOT_THRESHOLD, help_heading = "JIT")]
    pub hot_threshold: u32,

    /// Number of JIT compilation worker threads.
    #[arg(long = "jit.worker-count", help_heading = "JIT")]
    pub worker_count: Option<usize>,

    /// Capacity of the lookup-observed event channel.
    /// Events are silently dropped when the channel is full.
    #[arg(long = "jit.channel-capacity", default_value_t = Self::DEFAULT_CHANNEL_CAPACITY, help_heading = "JIT")]
    pub channel_capacity: usize,

    /// Maximum number of pending JIT compilation jobs.
    #[arg(long = "jit.max-pending-jobs", default_value_t = Self::DEFAULT_MAX_PENDING_JOBS, help_heading = "JIT")]
    pub max_pending_jobs: usize,

    /// Enable compiler debug dumps. IR, assembly, and bytecode are written to
    /// `<datadir>/jit/<spec_id>/<code_hash>/` for each compiled contract.
    #[arg(long = "jit.debug", default_value_t = false, help_heading = "JIT")]
    pub debug: bool,

    /// Blocking mode: synchronously JIT-compile every contract on first encounter.
    /// Implies --jit. Intended for debugging only.
    #[arg(long = "jit.blocking", default_value_t = false, help_heading = "JIT")]
    pub blocking: bool,
}

impl JitArgs {
    const DEFAULT_HOT_THRESHOLD: u32 = 8;
    const DEFAULT_CHANNEL_CAPACITY: usize = 4096;
    const DEFAULT_MAX_PENDING_JOBS: usize = 2048;
}

impl Default for JitArgs {
    fn default() -> Self {
        Self {
            enabled: false,
            hot_threshold: Self::DEFAULT_HOT_THRESHOLD,
            worker_count: None,
            channel_capacity: Self::DEFAULT_CHANNEL_CAPACITY,
            max_pending_jobs: Self::DEFAULT_MAX_PENDING_JOBS,
            debug: false,
            blocking: false,
        }
    }
}
