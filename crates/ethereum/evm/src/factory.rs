//! revmc JIT compiler integration for EVM execution.
//!
//! Re-exports types from [`revmc::alloy_evm`] and provides [`RethEvmFactory`], a newtype that
//! implements [`Debug`].

use alloy_evm::{Database, EvmEnv, EvmFactory};
use revm::{
    context::BlockEnv,
    context_interface::result::{EVMError, HaltReason},
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
    Inspector,
};
use revmc::alloy_evm as jit;

pub use jit::JitEvm;
pub use revmc::runtime::{JitBackend, RuntimeConfig, RuntimeStatsSnapshot, RuntimeTuning};

/// Newtype around [`revmc::alloy_evm::JitEvmFactory`] that implements [`Debug`].
///
/// Owns the [`JitBackend`] to keep it alive for the factory's lifetime.
#[derive(Clone)]
pub struct RethEvmFactory {
    inner: jit::JitEvmFactory,
    /// Keeps the backend alive.
    _backend: JitBackend,
}

impl core::fmt::Debug for RethEvmFactory {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RethEvmFactory").finish_non_exhaustive()
    }
}

impl RethEvmFactory {
    /// Creates a new factory that owns the backend.
    pub fn new(backend: JitBackend) -> Self {
        Self { inner: jit::JitEvmFactory::new(backend.clone()), _backend: backend }
    }

    /// Creates a [`RethEvmFactory`] with JIT disabled.
    ///
    /// Starts a backend with `enabled: false` so lookups always return `Interpret`.
    pub fn disabled() -> Self {
        let backend = JitBackend::start(RuntimeConfig::default())
            .expect("failed to start disabled revmc runtime");
        Self::new(backend)
    }
}

impl EvmFactory for RethEvmFactory {
    type Evm<DB: Database, I: Inspector<alloy_evm::eth::EthEvmContext<DB>>> =
        <jit::JitEvmFactory as EvmFactory>::Evm<DB, I>;
    type Context<DB: Database> = <jit::JitEvmFactory as EvmFactory>::Context<DB>;
    type Tx = <jit::JitEvmFactory as EvmFactory>::Tx;
    type Error<DBError: core::error::Error + Send + Sync + 'static> = EVMError<DBError>;
    type HaltReason = HaltReason;
    type Spec = SpecId;
    type BlockEnv = BlockEnv;
    type Precompiles = <jit::JitEvmFactory as EvmFactory>::Precompiles;

    fn create_evm<DB: Database>(&self, db: DB, input: EvmEnv) -> Self::Evm<DB, NoOpInspector> {
        self.inner.create_evm(db, input)
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        self.inner.create_evm_with_inspector(db, input, inspector)
    }
}

/// Prometheus metrics for revmc JIT runtime stats.
#[derive(reth_metrics::Metrics, Clone)]
#[metrics(scope = "revmc.jit")]
pub struct RevmcMetrics {
    /// Total lookups that returned a compiled function.
    pub lookup_hits: metrics::Gauge,
    /// Total lookups that returned interpret (not ready).
    pub lookup_misses: metrics::Gauge,
    /// Lookup-observed events successfully enqueued.
    pub events_sent: metrics::Gauge,
    /// Lookup-observed events dropped (channel full).
    pub events_dropped: metrics::Gauge,
    /// Number of entries in the resident compiled map.
    pub resident_entries: metrics::Gauge,
    /// Approximate total bytes of compiled machine code in the resident map.
    pub jit_code_bytes: metrics::Gauge,
    /// Approximate total bytes of JIT-related data (relocations, metadata, etc.).
    pub jit_data_bytes: metrics::Gauge,
    /// Number of pending JIT compilation jobs in the queue.
    pub jit_queue_len: metrics::Gauge,
    /// Total number of entries evicted (idle + budget).
    pub evictions: metrics::Gauge,
    /// Total number of JIT promotions (hot threshold reached).
    pub jit_promotions: metrics::Gauge,
    /// Total number of successful JIT compilations.
    pub jit_successes: metrics::Gauge,
    /// Total number of failed JIT compilations.
    pub jit_failures: metrics::Gauge,
}

impl RevmcMetrics {
    /// Records a [`RuntimeStatsSnapshot`] into the metrics.
    pub fn record(&self, stats: &RuntimeStatsSnapshot) {
        let RuntimeStatsSnapshot {
            lookup_hits,
            lookup_misses,
            events_sent,
            events_dropped,
            resident_entries,
            jit_code_bytes,
            jit_data_bytes,
            jit_queue_len,
            evictions,
            jit_promotions,
            jit_successes,
            jit_failures,
        } = *stats;
        self.lookup_hits.set(lookup_hits as f64);
        self.lookup_misses.set(lookup_misses as f64);
        self.events_sent.set(events_sent as f64);
        self.events_dropped.set(events_dropped as f64);
        self.resident_entries.set(resident_entries as f64);
        self.jit_code_bytes.set(jit_code_bytes as f64);
        self.jit_data_bytes.set(jit_data_bytes as f64);
        self.jit_queue_len.set(jit_queue_len as f64);
        self.evictions.set(evictions as f64);
        self.jit_promotions.set(jit_promotions as f64);
        self.jit_successes.set(jit_successes as f64);
        self.jit_failures.set(jit_failures as f64);
    }
}
