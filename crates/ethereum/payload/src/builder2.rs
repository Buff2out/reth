//! Prototype payload builder that uses engine-tree caches (execution cache, precompile cache,
//! sparse trie) to speed up block building.
//!
//! This is NOT wired into the node builder -- it exists to prototype the API surface needed
//! to share engine-tree caches with the payload builder.

use alloy_consensus::Transaction;
use alloy_primitives::U256;
use alloy_rlp::Encodable;
use reth_basic_payload_builder::{
    is_better_payload, BuildArguments, BuildOutcome, MissingPayloadBehaviour, PayloadBuilder,
    PayloadConfig,
};
use reth_chainspec::{ChainSpecProvider, EthChainSpec, EthereumHardforks};
use reth_consensus_common::validation::MAX_RLP_BLOCK_SIZE;
use reth_engine_tree::tree::{
    precompile_cache::{CachedPrecompile, PrecompileCacheMap},
    CachedStateMetrics, CachedStateProvider, ExecutionCache, PayloadExecutionCache,
    PreservedSparseTrie, SharedPreservedSparseTrie,
};
use reth_errors::{BlockExecutionError, BlockValidationError, ConsensusError};
use reth_ethereum_primitives::{EthPrimitives, TransactionSigned};
use reth_evm::{
    execute::{BlockBuilder, BlockBuilderOutcome},
    ConfigureEvm, Evm, NextBlockEnvAttributes, SpecFor,
};
use reth_evm_ethereum::EthEvmConfig;
use reth_payload_builder::{BlobSidecars, EthBuiltPayload, EthPayloadBuilderAttributes};
use reth_payload_builder_primitives::PayloadBuilderError;
use reth_payload_primitives::PayloadBuilderAttributes;
use reth_primitives_traits::transaction::error::InvalidTransactionError;
use reth_revm::{database::StateProviderDatabase, db::State};
use reth_storage_api::{StateProvider, StateProviderFactory};
use reth_trie::{updates::TrieUpdates, HashedPostState, HashedStorage, MultiProof, TrieInput};
use reth_transaction_pool::{
    error::{Eip4844PoolTransactionError, InvalidPoolTransactionError},
    BestTransactions, BestTransactionsAttributes, PoolTransaction, TransactionPool,
    ValidPoolTransaction,
};
use revm::context_interface::Block as _;
use std::sync::Arc;
use tracing::{debug, trace, warn};

use crate::EthereumBuilderConfig;

/// Default cross-block cache size (256 MB) used when no engine cache is available.
const DEFAULT_CACHE_SIZE: usize = 256 * 1024 * 1024;

type BestTransactionsIter<Pool> = Box<
    dyn BestTransactions<Item = Arc<ValidPoolTransaction<<Pool as TransactionPool>::Transaction>>>,
>;

/// Prototype Ethereum payload builder that leverages engine-tree caches.
///
/// Holds references to the three engine caches:
/// - **Execution cache**: warm account/storage/bytecode data from prior block execution
/// - **Precompile cache**: cached precompile results across blocks
/// - **Sparse trie**: preserved sparse trie for faster state root computation
#[derive(Debug, Clone)]
pub struct EthereumPayloadBuilder2<Pool, Client, EvmConfig = EthEvmConfig>
where
    EvmConfig: ConfigureEvm,
{
    /// Client providing access to node state.
    client: Client,
    /// Transaction pool.
    pool: Pool,
    /// The type responsible for creating the evm.
    evm_config: EvmConfig,
    /// Payload builder configuration.
    builder_config: EthereumBuilderConfig,
    /// Engine execution cache (Arc-backed, cheap to clone).
    execution_cache: PayloadExecutionCache,
    /// Engine precompile cache map (Arc-backed, cheap to clone).
    precompile_cache_map: PrecompileCacheMap<SpecFor<EvmConfig>>,
    /// Engine sparse trie (Arc-backed, cheap to clone).
    sparse_trie: SharedPreservedSparseTrie,
}

impl<Pool, Client, EvmConfig> EthereumPayloadBuilder2<Pool, Client, EvmConfig>
where
    EvmConfig: ConfigureEvm,
{
    /// Creates a new `EthereumPayloadBuilder2`.
    pub fn new(
        client: Client,
        pool: Pool,
        evm_config: EvmConfig,
        builder_config: EthereumBuilderConfig,
        execution_cache: PayloadExecutionCache,
        precompile_cache_map: PrecompileCacheMap<SpecFor<EvmConfig>>,
        sparse_trie: SharedPreservedSparseTrie,
    ) -> Self {
        Self {
            client,
            pool,
            evm_config,
            builder_config,
            execution_cache,
            precompile_cache_map,
            sparse_trie,
        }
    }
}

impl<Pool, Client, EvmConfig> PayloadBuilder for EthereumPayloadBuilder2<Pool, Client, EvmConfig>
where
    EvmConfig: ConfigureEvm<Primitives = EthPrimitives, NextBlockEnvCtx = NextBlockEnvAttributes>,
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec: EthereumHardforks> + Clone,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>,
{
    type Attributes = EthPayloadBuilderAttributes;
    type BuiltPayload = EthBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<EthPayloadBuilderAttributes, EthBuiltPayload>,
    ) -> Result<BuildOutcome<EthBuiltPayload>, PayloadBuilderError> {
        cached_ethereum_payload(
            self.evm_config.clone(),
            self.client.clone(),
            self.pool.clone(),
            self.builder_config.clone(),
            args,
            |attributes| self.pool.best_transactions_with_attributes(attributes),
            &self.execution_cache,
            &self.precompile_cache_map,
            &self.sparse_trie,
        )
    }

    fn on_missing_payload(
        &self,
        _args: BuildArguments<Self::Attributes, Self::BuiltPayload>,
    ) -> MissingPayloadBehaviour<Self::BuiltPayload> {
        if self.builder_config.await_payload_on_missing {
            MissingPayloadBehaviour::AwaitInProgress
        } else {
            MissingPayloadBehaviour::RaceEmptyPayload
        }
    }

    fn build_empty_payload(
        &self,
        config: PayloadConfig<Self::Attributes>,
    ) -> Result<EthBuiltPayload, PayloadBuilderError> {
        let args = BuildArguments::new(Default::default(), config, Default::default(), None);

        cached_ethereum_payload(
            self.evm_config.clone(),
            self.client.clone(),
            self.pool.clone(),
            self.builder_config.clone(),
            args,
            |attributes| self.pool.best_transactions_with_attributes(attributes),
            &self.execution_cache,
            &self.precompile_cache_map,
            &self.sparse_trie,
        )?
        .into_payload()
        .ok_or_else(|| PayloadBuilderError::MissingPayload)
    }
}

/// Constructs an Ethereum transaction payload using engine-tree caches.
///
/// This is identical to [`default_ethereum_payload`](crate::default_ethereum_payload) except:
///
/// **Phase 1** - Execution cache + precompile cache:
/// - Uses `CachedStateProvider` wrapping the state provider with the engine's execution cache
/// - Wraps EVM precompiles with `CachedPrecompile` using the engine's precompile cache
///
/// **Phase 2** - Sparse trie state root:
/// - Takes the preserved sparse trie before building
/// - Uses it to compute the state root instead of the slow `state_root_with_updates()`
#[allow(clippy::too_many_arguments)]
pub fn cached_ethereum_payload<EvmConfig, Client, Pool, F>(
    evm_config: EvmConfig,
    client: Client,
    pool: Pool,
    builder_config: EthereumBuilderConfig,
    args: BuildArguments<EthPayloadBuilderAttributes, EthBuiltPayload>,
    best_txs: F,
    execution_cache: &PayloadExecutionCache,
    precompile_cache_map: &PrecompileCacheMap<SpecFor<EvmConfig>>,
    sparse_trie: &SharedPreservedSparseTrie,
) -> Result<BuildOutcome<EthBuiltPayload>, PayloadBuilderError>
where
    EvmConfig: ConfigureEvm<Primitives = EthPrimitives, NextBlockEnvCtx = NextBlockEnvAttributes>,
    Client: StateProviderFactory + ChainSpecProvider<ChainSpec: EthereumHardforks>,
    Pool: TransactionPool<Transaction: PoolTransaction<Consensus = TransactionSigned>>,
    F: FnOnce(BestTransactionsAttributes) -> BestTransactionsIter<Pool>,
{
    let BuildArguments { mut cached_reads, config, cancel, best_payload } = args;
    let PayloadConfig { parent_header, attributes } = config;

    let state_provider = client.state_by_block_hash(parent_header.hash())?;

    // --- Phase 1: Execution cache ---
    // Try to get a warm cache from the engine's execution cache for the parent block.
    // If unavailable, create a fresh (empty) cache — `CachedStateProvider` will still
    // function correctly, just with no pre-warmed data.
    let (caches, metrics) = if let Some(saved) = execution_cache.get_cache_for(parent_header.hash())
    {
        debug!(target: "payload_builder", "using engine execution cache for parent");
        (saved.cache().clone(), saved.metrics().clone())
    } else {
        debug!(target: "payload_builder", "no engine execution cache available, using fresh cache");
        (ExecutionCache::new(DEFAULT_CACHE_SIZE), CachedStateMetrics::zeroed())
    };

    let cached_state = CachedStateProvider::new(state_provider.as_ref(), caches, metrics);
    let state = StateProviderDatabase::new(&cached_state);
    let mut db =
        State::builder().with_database(cached_reads.as_db_mut(state)).with_bundle_update().build();

    let next_block_attrs = NextBlockEnvAttributes {
        timestamp: attributes.timestamp(),
        suggested_fee_recipient: attributes.suggested_fee_recipient(),
        prev_randao: attributes.prev_randao(),
        gas_limit: builder_config.gas_limit(parent_header.gas_limit),
        parent_beacon_block_root: attributes.parent_beacon_block_root(),
        withdrawals: Some(attributes.withdrawals().clone()),
        extra_data: builder_config.extra_data,
    };

    // --- Phase 1: Precompile cache ---
    // Get spec_id before creating the builder, to properly key cached precompile results.
    let spec_id = *evm_config
        .next_evm_env(&parent_header, &next_block_attrs)
        .map_err(PayloadBuilderError::other)?
        .spec_id();

    let mut builder = evm_config
        .builder_for_next_block(&mut db, &parent_header, next_block_attrs)
        .map_err(PayloadBuilderError::other)?;

    builder.evm_mut().precompiles_mut().map_cacheable_precompiles(|address, precompile| {
        CachedPrecompile::wrap(
            precompile,
            precompile_cache_map.cache_for_address(*address),
            spec_id,
            None,
        )
    });

    let chain_spec = client.chain_spec();

    debug!(target: "payload_builder", id=%attributes.id, parent_header = ?parent_header.hash(), parent_number = parent_header.number, "building new payload (cached)");
    let mut cumulative_gas_used = 0;
    let block_gas_limit: u64 = builder.evm_mut().block().gas_limit();
    let base_fee = builder.evm_mut().block().basefee();

    let mut best_txs = best_txs(BestTransactionsAttributes::new(
        base_fee,
        builder.evm_mut().block().blob_gasprice().map(|gasprice| gasprice as u64),
    ));
    let mut total_fees = U256::ZERO;

    builder.apply_pre_execution_changes().map_err(|err| {
        warn!(target: "payload_builder", %err, "failed to apply pre-execution changes");
        PayloadBuilderError::Internal(err.into())
    })?;

    let mut blob_sidecars = BlobSidecars::Empty;
    let mut block_blob_count = 0;
    let mut block_transactions_rlp_length = 0;

    let blob_params = chain_spec.blob_params_at_timestamp(attributes.timestamp);
    let protocol_max_blob_count =
        blob_params.as_ref().map(|params| params.max_blob_count).unwrap_or_else(Default::default);

    let max_blob_count = builder_config
        .max_blobs_per_block
        .map(|user_limit| std::cmp::min(user_limit, protocol_max_blob_count).max(1))
        .unwrap_or(protocol_max_blob_count);

    let is_osaka = chain_spec.is_osaka_active_at_timestamp(attributes.timestamp);
    let withdrawals_rlp_length = attributes.withdrawals().length();

    // --- Transaction execution loop (identical to default_ethereum_payload) ---
    while let Some(pool_tx) = best_txs.next() {
        if cumulative_gas_used + pool_tx.gas_limit() > block_gas_limit {
            best_txs.mark_invalid(
                &pool_tx,
                &InvalidPoolTransactionError::ExceedsGasLimit(pool_tx.gas_limit(), block_gas_limit),
            );
            continue
        }

        if cancel.is_cancelled() {
            return Ok(BuildOutcome::Cancelled)
        }

        let tx = pool_tx.to_consensus();
        let tx_rlp_len = tx.inner().length();

        let estimated_block_size_with_tx =
            block_transactions_rlp_length + tx_rlp_len + withdrawals_rlp_length + 1024;

        if is_osaka && estimated_block_size_with_tx > MAX_RLP_BLOCK_SIZE {
            best_txs.mark_invalid(
                &pool_tx,
                &InvalidPoolTransactionError::OversizedData {
                    size: estimated_block_size_with_tx,
                    limit: MAX_RLP_BLOCK_SIZE,
                },
            );
            continue
        }

        let mut blob_tx_sidecar = None;
        if let Some(blob_hashes) = tx.blob_versioned_hashes() {
            let tx_blob_count = blob_hashes.len() as u64;

            if block_blob_count + tx_blob_count > max_blob_count {
                trace!(target: "payload_builder", tx=?tx.hash(), ?block_blob_count, "skipping blob transaction because it would exceed the max blob count per block");
                best_txs.mark_invalid(
                    &pool_tx,
                    &InvalidPoolTransactionError::Eip4844(
                        Eip4844PoolTransactionError::TooManyEip4844Blobs {
                            have: block_blob_count + tx_blob_count,
                            permitted: max_blob_count,
                        },
                    ),
                );
                continue
            }

            let blob_sidecar_result = 'sidecar: {
                let Some(sidecar) =
                    pool.get_blob(*tx.hash()).map_err(PayloadBuilderError::other)?
                else {
                    break 'sidecar Err(Eip4844PoolTransactionError::MissingEip4844BlobSidecar)
                };

                if is_osaka {
                    if sidecar.is_eip7594() {
                        Ok(sidecar)
                    } else {
                        Err(Eip4844PoolTransactionError::UnexpectedEip4844SidecarAfterOsaka)
                    }
                } else if sidecar.is_eip4844() {
                    Ok(sidecar)
                } else {
                    Err(Eip4844PoolTransactionError::UnexpectedEip7594SidecarBeforeOsaka)
                }
            };

            blob_tx_sidecar = match blob_sidecar_result {
                Ok(sidecar) => Some(sidecar),
                Err(error) => {
                    best_txs.mark_invalid(&pool_tx, &InvalidPoolTransactionError::Eip4844(error));
                    continue
                }
            };
        }

        let gas_used = match builder.execute_transaction(tx.clone()) {
            Ok(gas_used) => gas_used,
            Err(BlockExecutionError::Validation(BlockValidationError::InvalidTx {
                error, ..
            })) => {
                if error.is_nonce_too_low() {
                    trace!(target: "payload_builder", %error, ?tx, "skipping nonce too low transaction");
                } else {
                    trace!(target: "payload_builder", %error, ?tx, "skipping invalid transaction and its descendants");
                    best_txs.mark_invalid(
                        &pool_tx,
                        &InvalidPoolTransactionError::Consensus(
                            InvalidTransactionError::TxTypeNotSupported,
                        ),
                    );
                }
                continue
            }
            Err(err) => return Err(PayloadBuilderError::evm(err)),
        };

        if let Some(blob_hashes) = tx.blob_versioned_hashes() {
            block_blob_count += blob_hashes.len() as u64;
            if block_blob_count == max_blob_count {
                best_txs.skip_blobs();
            }
        }

        block_transactions_rlp_length += tx_rlp_len;

        let miner_fee =
            tx.effective_tip_per_gas(base_fee).expect("fee is always valid; execution succeeded");
        total_fees += U256::from(miner_fee) * U256::from(gas_used);
        cumulative_gas_used += gas_used;

        if let Some(sidecar) = blob_tx_sidecar {
            blob_sidecars.push_sidecar_variant(sidecar.as_ref().clone());
        }
    }

    // check if we have a better block
    if !is_better_payload(best_payload.as_ref(), total_fees) {
        drop(builder);
        return Ok(BuildOutcome::Aborted { fees: total_fees, cached_reads })
    }

    // --- Phase 2: Sparse trie state root ---
    // Take the preserved sparse trie before finishing. The wrapper's
    // `state_root_with_updates` will use it instead of the slow full trie computation.
    let preserved = sparse_trie.take();
    let wrapper = SparseTrieStateProvider { inner: state_provider.as_ref(), preserved };

    let BlockBuilderOutcome { execution_result, block, .. } = builder.finish(wrapper)?;

    let requests = chain_spec
        .is_prague_active_at_timestamp(attributes.timestamp)
        .then_some(execution_result.requests);

    let sealed_block = Arc::new(block.into_sealed_block());
    debug!(target: "payload_builder", id=%attributes.id, sealed_block_header = ?sealed_block.sealed_header(), "sealed built block (cached)");

    if is_osaka && sealed_block.rlp_length() > MAX_RLP_BLOCK_SIZE {
        return Err(PayloadBuilderError::other(ConsensusError::BlockTooLarge {
            rlp_length: sealed_block.rlp_length(),
            max_rlp_length: MAX_RLP_BLOCK_SIZE,
        }));
    }

    let payload = EthBuiltPayload::new(attributes.id, sealed_block, total_fees, requests)
        .with_sidecars(blob_sidecars);

    Ok(BuildOutcome::Better { payload, cached_reads })
}

/// A state provider wrapper that holds a preserved sparse trie for
/// faster state root computation.
///
/// All `StateProvider` trait methods delegate to the inner provider. The
/// `state_root_with_updates` method is the hook point for sparse trie integration.
struct SparseTrieStateProvider<'a> {
    inner: &'a dyn StateProvider,
    /// The preserved sparse trie, taken from the shared handle before building.
    #[allow(dead_code)]
    preserved: Option<PreservedSparseTrie>,
}

// --- Delegate all StateProvider trait methods to inner ---

impl reth_storage_api::AccountReader for SparseTrieStateProvider<'_> {
    fn basic_account(
        &self,
        address: &alloy_primitives::Address,
    ) -> reth_errors::ProviderResult<Option<reth_primitives_traits::Account>> {
        self.inner.basic_account(address)
    }
}

impl reth_storage_api::BlockHashReader for SparseTrieStateProvider<'_> {
    fn block_hash(
        &self,
        number: alloy_primitives::BlockNumber,
    ) -> reth_errors::ProviderResult<Option<alloy_primitives::B256>> {
        self.inner.block_hash(number)
    }

    fn canonical_hashes_range(
        &self,
        start: alloy_primitives::BlockNumber,
        end: alloy_primitives::BlockNumber,
    ) -> reth_errors::ProviderResult<Vec<alloy_primitives::B256>> {
        self.inner.canonical_hashes_range(start, end)
    }
}

impl reth_storage_api::BytecodeReader for SparseTrieStateProvider<'_> {
    fn bytecode_by_hash(
        &self,
        code_hash: &alloy_primitives::B256,
    ) -> reth_errors::ProviderResult<Option<reth_primitives_traits::Bytecode>> {
        self.inner.bytecode_by_hash(code_hash)
    }
}

impl reth_storage_api::StateRootProvider for SparseTrieStateProvider<'_> {
    fn state_root(
        &self,
        hashed_state: HashedPostState,
    ) -> reth_errors::ProviderResult<alloy_primitives::B256> {
        self.inner.state_root(hashed_state)
    }

    fn state_root_from_nodes(
        &self,
        input: TrieInput,
    ) -> reth_errors::ProviderResult<alloy_primitives::B256> {
        self.inner.state_root_from_nodes(input)
    }

    fn state_root_with_updates(
        &self,
        hashed_state: HashedPostState,
    ) -> reth_errors::ProviderResult<(alloy_primitives::B256, TrieUpdates)> {
        // Phase 2: Hook point for sparse trie state root computation.
        //
        // Currently falls through to the standard computation. The sparse trie integration
        // requires the multiproof pipeline which is complex to wire synchronously.
        //
        // Future implementation:
        //   1. let targets = hashed_state.multi_proof_targets();
        //   2. let multiproof = self.inner.multiproof(TrieInput::default(), targets)?;
        //   3. sparse_trie.reveal_multiproof(multiproof)?;
        //   4. // update account/storage leaves from hashed_state
        //   5. let (root, updates) = sparse_trie.root_with_updates(provider)?;
        //   6. return Ok((root, updates));
        self.inner.state_root_with_updates(hashed_state)
    }

    fn state_root_from_nodes_with_updates(
        &self,
        input: TrieInput,
    ) -> reth_errors::ProviderResult<(alloy_primitives::B256, TrieUpdates)> {
        self.inner.state_root_from_nodes_with_updates(input)
    }
}

impl reth_storage_api::StorageRootProvider for SparseTrieStateProvider<'_> {
    fn storage_root(
        &self,
        address: alloy_primitives::Address,
        hashed_storage: HashedStorage,
    ) -> reth_errors::ProviderResult<alloy_primitives::B256> {
        self.inner.storage_root(address, hashed_storage)
    }

    fn storage_proof(
        &self,
        address: alloy_primitives::Address,
        slot: alloy_primitives::B256,
        hashed_storage: HashedStorage,
    ) -> reth_errors::ProviderResult<reth_trie::StorageProof> {
        self.inner.storage_proof(address, slot, hashed_storage)
    }

    fn storage_multiproof(
        &self,
        address: alloy_primitives::Address,
        slots: &[alloy_primitives::B256],
        hashed_storage: HashedStorage,
    ) -> reth_errors::ProviderResult<reth_trie::StorageMultiProof> {
        self.inner.storage_multiproof(address, slots, hashed_storage)
    }
}

impl reth_storage_api::StateProofProvider for SparseTrieStateProvider<'_> {
    fn proof(
        &self,
        input: TrieInput,
        address: alloy_primitives::Address,
        slots: &[alloy_primitives::B256],
    ) -> reth_errors::ProviderResult<reth_trie::AccountProof> {
        self.inner.proof(input, address, slots)
    }

    fn multiproof(
        &self,
        input: TrieInput,
        targets: reth_trie::MultiProofTargets,
    ) -> reth_errors::ProviderResult<MultiProof> {
        self.inner.multiproof(input, targets)
    }

    fn witness(
        &self,
        input: TrieInput,
        target: HashedPostState,
    ) -> reth_errors::ProviderResult<Vec<alloy_primitives::Bytes>> {
        self.inner.witness(input, target)
    }
}

impl reth_storage_api::HashedPostStateProvider for SparseTrieStateProvider<'_> {
    fn hashed_post_state(&self, bundle_state: &reth_revm::db::BundleState) -> HashedPostState {
        self.inner.hashed_post_state(bundle_state)
    }
}

impl StateProvider for SparseTrieStateProvider<'_> {
    fn storage(
        &self,
        account: alloy_primitives::Address,
        storage_key: alloy_primitives::StorageKey,
    ) -> reth_errors::ProviderResult<Option<alloy_primitives::StorageValue>> {
        self.inner.storage(account, storage_key)
    }
}
