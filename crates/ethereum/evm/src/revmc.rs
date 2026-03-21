//! revmc JIT compiler integration for EVM execution.
//!
//! Re-exports types from [`revmc::alloy_evm`] and provides [`RevmcRuntime`] for managing the JIT
//! coordinator lifetime, plus a [`RevmcEvmFactory`] newtype that implements [`Debug`].

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
pub use revmc::runtime::{
    JitCoordinator, JitCoordinatorHandle, RuntimeConfig, RuntimeStatsSnapshot,
};

/// Newtype around [`revmc::alloy_evm::JitEvmFactory`] that implements [`Debug`].
#[derive(Clone)]
pub struct RevmcEvmFactory(jit::JitEvmFactory);

impl core::fmt::Debug for RevmcEvmFactory {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RevmcEvmFactory").finish_non_exhaustive()
    }
}

impl RevmcEvmFactory {
    /// Creates a new factory from a coordinator handle.
    pub fn new(handle: JitCoordinatorHandle) -> Self {
        Self(jit::JitEvmFactory::new(handle))
    }
}

impl EvmFactory for RevmcEvmFactory {
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
        self.0.create_evm(db, input)
    }

    fn create_evm_with_inspector<DB: Database, I: Inspector<Self::Context<DB>>>(
        &self,
        db: DB,
        input: EvmEnv,
        inspector: I,
    ) -> Self::Evm<DB, I> {
        self.0.create_evm_with_inspector(db, input, inspector)
    }
}

/// Owns the [`JitCoordinator`] for the node's lifetime and provides handles.
///
/// The coordinator is not `Sync` (it owns `mpsc::Receiver`), so this type should be held in a
/// non-shared context (e.g. main thread). Use [`RevmcRuntime::handle`] or [`RevmcRuntime::factory`]
/// to get `Send + Sync` types for passing into the EVM pipeline.
#[expect(missing_debug_implementations)]
pub struct RevmcRuntime {
    coordinator: JitCoordinator,
}

impl RevmcRuntime {
    /// Starts the revmc runtime with the given configuration.
    pub fn start(config: RuntimeConfig) -> eyre::Result<Self> {
        let coordinator = JitCoordinator::start(config)?;
        Ok(Self { coordinator })
    }

    /// Returns a clonable handle for performing lookups.
    pub fn handle(&self) -> JitCoordinatorHandle {
        self.coordinator.handle()
    }

    /// Returns a [`RevmcEvmFactory`] that can be used with [`EthEvmConfig`].
    ///
    /// [`EthEvmConfig`]: crate::EthEvmConfig
    pub fn factory(&self) -> RevmcEvmFactory {
        RevmcEvmFactory::new(self.handle())
    }

    /// Creates a [`RevmcEvmFactory`] with JIT disabled (no coordinator running).
    ///
    /// Starts a coordinator with `enabled: false` so lookups always return `Interpret`.
    pub fn disabled_factory() -> RevmcEvmFactory {
        let runtime =
            Self::start(RuntimeConfig::default()).expect("failed to start disabled revmc runtime");
        runtime.factory()
    }

    /// Shuts down the coordinator.
    pub fn shutdown(self) -> eyre::Result<()> {
        self.coordinator.shutdown()
    }
}

/// Records revmc JIT runtime stats as Prometheus metrics.
pub fn record_revmc_metrics(stats: &RuntimeStatsSnapshot) {
    metrics::gauge!("revmc_jit_lookup_hits").set(stats.lookup_hits as f64);
    metrics::gauge!("revmc_jit_lookup_misses").set(stats.lookup_misses as f64);
    metrics::gauge!("revmc_jit_events_sent").set(stats.events_sent as f64);
    metrics::gauge!("revmc_jit_events_dropped").set(stats.events_dropped as f64);
    metrics::gauge!("revmc_jit_resident_entries").set(stats.resident_entries as f64);
}
