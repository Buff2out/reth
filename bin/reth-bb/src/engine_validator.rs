//! Big-block engine validator.
//!
//! Wraps [`BasicEngineValidator`] and intercepts `validate_payload` to perform
//! multi-segment execution when [`BigBlockData`] is present for a payload hash.

pub(crate) use reth_engine_primitives::BigBlockData;

use crate::BigBlockMap;
use alloy_evm::Evm as _;
use alloy_primitives::{Bloom, B256};
use alloy_rpc_types::engine::ExecutionData;
use reth_chain_state::ExecutedBlock;
use reth_engine_primitives::{ConfigureEngineEvm, ExecutionPayload, PayloadValidator};
use reth_engine_tree::tree::{
    error::{InsertBlockError, InsertBlockErrorKind},
    payload_processor::receipt_root_task::{IndexedReceipt, ReceiptRootTaskHandle},
    payload_validator::{
        BasicEngineValidator, BlockOrPayload, EngineValidator, InsertPayloadResult,
        LazyHashedPostState, StateRootStrategy, TreeCtx, ValidationOutcome,
    },
    precompile_cache::{CachedPrecompile, CachedPrecompileMetrics},
    CacheWaitDurations, ExecutionEnv, WaitForCaches,
};
use reth_ethereum_primitives::EthPrimitives;
use reth_evm::{block::BlockExecutor, execute::ExecutableTxFor, ConfigureEvm, OnStateHook};
use reth_node_api::{AddOnsContext, FullNodeComponents, NodeTypes, TreeConfig};
use reth_node_builder::rpc::{
    BasicEngineValidatorBuilder, ChangesetCache, EngineValidatorBuilder, PayloadValidatorBuilder,
};
use reth_node_ethereum::EthEngineTypes;
use reth_payload_primitives::{InvalidPayloadAttributesError, NewPayloadError, PayloadTypes};
use reth_primitives_traits::{AlloyBlockHeader, BlockBody, FastInstant as Instant, SealedBlock};
use reth_provider::{
    providers::OverlayStateProviderFactory, BlockExecutionOutput, BlockNumReader, BlockReader,
    ChangeSetReader, DatabaseProviderFactory, DatabaseProviderROFactory, HashedPostStateProvider,
    PruneCheckpointReader, StageCheckpointReader, StateProvider, StateProviderFactory, StateReader,
    StorageChangeSetReader, StorageSettingsCache,
};
use reth_revm::{
    database::StateProviderDatabase,
    db::{states::bundle_state::BundleRetention, State},
};
use revm_primitives::Address;
use std::sync::{atomic::Ordering, Arc};
use tracing::{debug, debug_span, info, trace, warn, Span};

// ---------------------------------------------------------------------------
// Multi-segment data types
// ---------------------------------------------------------------------------

/// Execution plan derived from [`BigBlockData`] for multi-segment execution.
///
/// Each segment covers a range of transactions executed under a single EVM environment.
/// The first segment always starts at transaction index 0.
struct BlockExecutionPlan {
    /// Block hashes from prior big blocks for the BLOCKHASH opcode.
    seed_block_hashes: Vec<(u64, B256)>,
    /// Ordered segments. The first segment's `execution_data` provides the initial EVM
    /// environment; subsequent segments switch the environment at their `stop_before_tx`
    /// boundary. There is always at least one segment.
    segments: Vec<ExecutionSegment>,
}

/// A single execution segment within a big block.
struct ExecutionSegment {
    /// Transactions `[prev_stop..stop_before_tx)` are executed under this segment's
    /// environment. For the first segment, `prev_stop` is implicitly 0.
    stop_before_tx: usize,
    /// The execution data defining the EVM environment for this segment.
    execution_data: ExecutionData,
}

/// Trait for adjusting cumulative gas on receipts across segment boundaries.
trait AdjustCumulativeGas: Clone {
    fn with_gas_offset(&self, gas_offset: u64) -> Self;
}

impl<T: alloy_eips::eip2718::Typed2718 + Clone> AdjustCumulativeGas
    for alloy_consensus::EthereumReceipt<T>
{
    fn with_gas_offset(&self, gas_offset: u64) -> Self {
        let mut receipt = self.clone();
        receipt.cumulative_gas_used += gas_offset;
        receipt
    }
}

// ---------------------------------------------------------------------------
// Provider bounds (type alias for readability)
// ---------------------------------------------------------------------------

/// Provider bounds required by [`BasicEngineValidator`].
pub(crate) trait BbProvider:
    DatabaseProviderFactory<
        Provider: BlockReader
                      + StageCheckpointReader
                      + PruneCheckpointReader
                      + ChangeSetReader
                      + StorageChangeSetReader
                      + BlockNumReader
                      + StorageSettingsCache,
    > + BlockReader<Header = alloy_consensus::Header>
    + ChangeSetReader
    + BlockNumReader
    + StateProviderFactory
    + StateReader
    + HashedPostStateProvider
    + Clone
    + 'static
{
}

impl<P> BbProvider for P where
    P: DatabaseProviderFactory<
            Provider: BlockReader
                          + StageCheckpointReader
                          + PruneCheckpointReader
                          + ChangeSetReader
                          + StorageChangeSetReader
                          + BlockNumReader
                          + StorageSettingsCache,
        > + BlockReader<Header = alloy_consensus::Header>
        + ChangeSetReader
        + BlockNumReader
        + StateProviderFactory
        + StateReader
        + HashedPostStateProvider
        + Clone
        + 'static
{
}

// ---------------------------------------------------------------------------
// BbEngineValidator
// ---------------------------------------------------------------------------

/// Engine validator that supports multi-segment big-block execution.
#[derive(derive_more::Debug)]
pub struct BbEngineValidator<P, Evm, V>
where
    Evm: ConfigureEvm,
{
    /// The upstream engine validator.
    pub inner: BasicEngineValidator<P, Evm, V>,
    /// Shared map of pending big-block metadata, keyed by payload hash.
    #[debug(skip)]
    pub pending: BigBlockMap,
}

impl<P, Evm, V> WaitForCaches for BbEngineValidator<P, Evm, V>
where
    Evm: ConfigureEvm,
{
    fn wait_for_caches(&self) -> CacheWaitDurations {
        self.inner.wait_for_caches()
    }
}

#[allow(private_bounds)]
impl<P, Evm, V> BbEngineValidator<P, Evm, V>
where
    P: BbProvider,
    Evm: ConfigureEvm<Primitives = EthPrimitives>
        + ConfigureEngineEvm<ExecutionData, Primitives = EthPrimitives>
        + 'static,
    V: PayloadValidator<EthEngineTypes, Block = reth_ethereum_primitives::Block> + Clone,
{
    /// Extracts and removes big-block metadata for the given payload hash.
    ///
    /// # Panics
    ///
    /// If the first `env_switch` does not start at transaction index 0.
    fn take_execution_plan(&self, payload_hash: B256) -> Option<BlockExecutionPlan> {
        let bb = self.pending.lock().unwrap().remove(&payload_hash)?;

        assert!(
            bb.env_switches.first().is_some_and(|(idx, _)| *idx == 0),
            "first env_switch must be at transaction index 0"
        );

        let segments = bb
            .env_switches
            .into_iter()
            .map(|(cumulative_tx_count, execution_data)| ExecutionSegment {
                stop_before_tx: cumulative_tx_count,
                execution_data,
            })
            .collect();

        Some(BlockExecutionPlan { seed_block_hashes: bb.prior_block_hashes, segments })
    }

    /// Multi-segment validation path.
    ///
    /// NOTE: Mirrors `BasicEngineValidator::validate_block_with_state`.
    /// Only difference: `execute_block` is replaced with `execute_block_multiseg`.
    fn validate_payload_multiseg(
        &mut self,
        payload: ExecutionData,
        mut ctx: TreeCtx<'_, EthPrimitives>,
        plan: BlockExecutionPlan,
    ) -> InsertPayloadResult<EthPrimitives> {
        let input = BlockOrPayload::<EthEngineTypes>::Payload(payload);

        // Background payload conversion
        let convert_handle = {
            let payload_clone = input.clone();
            let validator = self.inner.validator.clone();
            self.inner.payload_processor.executor().spawn_blocking_named(
                "payload-convert",
                move || {
                    let BlockOrPayload::Payload(payload) = payload_clone else { unreachable!() };
                    validator.convert_payload_to_block(payload)
                },
            )
        };

        let convert_to_block = move |_input: BlockOrPayload<EthEngineTypes>| -> Result<
            SealedBlock<reth_ethereum_primitives::Block>,
            NewPayloadError,
        > { convert_handle.try_into_inner().expect("sole handle") };

        macro_rules! ensure_ok {
            ($expr:expr) => {
                match $expr {
                    Ok(val) => val,
                    Err(e) => {
                        let block = convert_to_block(input)?;
                        return Err(InsertBlockError::new(block, e.into()).into())
                    }
                }
            };
        }

        macro_rules! ensure_ok_post_block {
            ($expr:expr, $block:expr) => {
                match $expr {
                    Ok(val) => val,
                    Err(e) => {
                        return Err(
                            InsertBlockError::new($block.into_sealed_block(), e.into()).into()
                        )
                    }
                }
            };
        }

        let parent_hash = input.parent_hash();

        let Some(provider_builder) =
            ensure_ok!(self.inner.state_provider_builder(parent_hash, ctx.state()))
        else {
            return Err(InsertBlockError::new(
                convert_to_block(input)?,
                reth_provider::ProviderError::HeaderNotFound(parent_hash.into()).into(),
            )
            .into())
        };
        let mut state_provider = ensure_ok!(provider_builder.build());

        let Some(parent_block) =
            ensure_ok!(self.inner.sealed_header_by_hash(parent_hash, ctx.state()))
        else {
            return Err(InsertBlockError::new(
                convert_to_block(input)?,
                reth_provider::ProviderError::HeaderNotFound(parent_hash.into()).into(),
            )
            .into())
        };

        // Build initial EVM env from the first segment's execution data
        let evm_env = self
            .inner
            .evm_config
            .evm_env_for_payload(&plan.segments[0].execution_data)
            .map_err(NewPayloadError::other)?;

        let env = ExecutionEnv {
            evm_env,
            hash: input.hash(),
            parent_hash: input.parent_hash(),
            parent_state_root: parent_block.state_root(),
            transaction_count: input.transaction_count(),
            gas_used: input.gas_used(),
            withdrawals: input.withdrawals().map(|w| w.to_vec()),
        };

        let strategy = self.inner.plan_state_root_computation();
        debug!(target: "engine::bb", ?strategy, "Decided state root algorithm");

        let txs = self.inner.tx_iterator_for(&input)?;

        let block_access_list = ensure_ok!(input
            .block_access_list()
            .transpose()
            .map_err(Box::<dyn std::error::Error + Send + Sync>::from))
        .map(Arc::new);

        let (lazy_overlay, anchor_hash) =
            BasicEngineValidator::<P, Evm, V>::get_parent_lazy_overlay(parent_hash, ctx.state());

        let overlay_factory = OverlayStateProviderFactory::new(
            self.inner.provider.clone(),
            self.inner.changeset_cache.clone(),
        )
        .with_block_hash(Some(anchor_hash))
        .with_lazy_overlay(lazy_overlay);

        let mut handle = ensure_ok!(self.inner.spawn_payload_processor(
            env.clone(),
            txs,
            provider_builder,
            overlay_factory.clone(),
            strategy,
            block_access_list,
        ));

        // Apply cached state provider (skip detailed stats for multiseg path)
        if let Some((caches, cache_metrics)) = handle.caches().zip(handle.cache_metrics()) {
            state_provider = Box::new(reth_engine_tree::tree::CachedStateProvider::new(
                state_provider,
                caches,
                cache_metrics,
            ));
        };

        // ---- Multi-segment execution ----
        let execute_block_start = Instant::now();
        let (output, senders, receipt_root_rx) =
            match self.execute_block_multiseg(state_provider, env, &input, &mut handle, &plan) {
                Ok(output) => output,
                Err(err) => return self.inner.handle_execution_error(input, err, &parent_block),
            };
        let _execution_duration = execute_block_start.elapsed();

        handle.stop_prewarming_execution();

        let output = Arc::new(output);
        let valid_block_tx = handle.terminate_caching(Some(output.clone()));

        let hashed_state_output = output.clone();
        let hashed_state_provider = self.inner.provider.clone();
        let hashed_state: LazyHashedPostState = self
            .inner
            .payload_processor
            .executor()
            .spawn_blocking_named("hash-post-state", move || {
                let _span = debug_span!(target: "engine::bb", "hashed_post_state").entered();
                hashed_state_provider.hashed_post_state(&hashed_state_output.state)
            });

        let block = convert_to_block(input)?;
        let transaction_root = {
            let body = block.body().clone();
            let parent_span = Span::current();
            let num_hash = block.num_hash();
            Some(self.inner.payload_processor.executor().spawn_blocking_named(
                "payload-tx-root",
                move || {
                    let _span = debug_span!(
                        target: "engine::bb",
                        parent: parent_span,
                        "payload_tx_root",
                        block = ?num_hash
                    )
                    .entered();
                    body.calculate_tx_root()
                },
            ))
        };
        let block = block.with_senders(senders);

        let receipt_root_bloom = {
            let _enter = debug_span!(target: "engine::bb", "wait_receipt_root").entered();
            receipt_root_rx
                .blocking_recv()
                .inspect_err(|_| {
                    tracing::error!(target: "engine::bb", "Receipt root task dropped sender");
                })
                .ok()
        };
        let transaction_root = transaction_root.map(|handle| {
            let _span = debug_span!(target: "engine::bb", "wait_payload_tx_root").entered();
            handle.try_into_inner().expect("sole handle")
        });

        let hashed_state = ensure_ok_post_block!(
            self.inner.validate_post_execution(
                &block,
                &parent_block,
                &output,
                &mut ctx,
                transaction_root,
                receipt_root_bloom,
                hashed_state,
            ),
            block
        );

        // --- State root computation ---
        let root_time = Instant::now();
        let mut maybe_state_root = None;
        let mut state_root_task_failed = false;

        match strategy {
            StateRootStrategy::StateRootTask => {
                debug!(target: "engine::bb", "Using sparse trie state root algorithm");
                let task_result = ensure_ok_post_block!(
                    self.inner.await_state_root_with_timeout(
                        &mut handle,
                        overlay_factory.clone(),
                        &hashed_state,
                    ),
                    block
                );

                match task_result {
                    Ok(outcome) => {
                        let elapsed = root_time.elapsed();
                        info!(target: "engine::bb", state_root = ?outcome.state_root, ?elapsed, "State root task finished");

                        if self.inner.config.always_compare_trie_updates() {
                            self.inner.compare_trie_updates_with_serial(
                                overlay_factory.clone(),
                                &hashed_state,
                                outcome.trie_updates.as_ref().clone(),
                            );
                        }

                        if outcome.state_root == block.header().state_root() {
                            maybe_state_root =
                                Some((outcome.state_root, outcome.trie_updates, elapsed))
                        } else {
                            warn!(
                                target: "engine::bb",
                                state_root = ?outcome.state_root,
                                block_state_root = ?block.header().state_root(),
                                "State root task returned incorrect state root"
                            );
                            state_root_task_failed = true;
                        }
                    }
                    Err(error) => {
                        debug!(target: "engine::bb", %error, "State root task failed");
                        state_root_task_failed = true;
                    }
                }
            }
            StateRootStrategy::Parallel => {
                debug!(target: "engine::bb", "Using parallel state root algorithm");
                match self.inner.compute_state_root_parallel(overlay_factory.clone(), &hashed_state)
                {
                    Ok(result) => {
                        let elapsed = root_time.elapsed();
                        info!(target: "engine::bb", state_root = ?result.0, ?elapsed, "Parallel root finished");
                        maybe_state_root = Some((result.0, Arc::new(result.1), elapsed));
                    }
                    Err(error) => {
                        debug!(target: "engine::bb", %error, "Parallel state root failed");
                    }
                }
            }
            StateRootStrategy::Synchronous => {}
        }

        let (state_root, trie_output, root_elapsed) = if let Some(sr) = maybe_state_root {
            sr
        } else {
            if self.inner.config.state_root_fallback() {
                debug!(target: "engine::bb", "Using state root fallback");
            } else {
                warn!(target: "engine::bb", "Failed to compute state root in parallel");
                self.inner.metrics.block_validation.state_root_parallel_fallback_total.increment(1);
            }

            let (root, updates) = ensure_ok_post_block!(
                BasicEngineValidator::<P, Evm, V>::compute_state_root_serial(
                    overlay_factory.clone(),
                    &hashed_state,
                ),
                block
            );

            if state_root_task_failed {
                self.inner
                    .metrics
                    .block_validation
                    .state_root_task_fallback_success_total
                    .increment(1);
            }

            (root, Arc::new(updates), root_time.elapsed())
        };

        self.inner
            .metrics
            .block_validation
            .record_state_root(&trie_output, root_elapsed.as_secs_f64());
        self.inner
            .metrics
            .record_state_root_gas_bucket(block.header().gas_used(), root_elapsed.as_secs_f64());
        debug!(target: "engine::bb", ?root_elapsed, "Calculated state root");

        if state_root != block.header().state_root() {
            self.inner.on_invalid_block(
                &parent_block,
                &block,
                &output,
                Some((&trie_output, state_root)),
                ctx.state_mut(),
            );
            let block_state_root = block.header().state_root();
            return Err(InsertBlockError::new(
                block.into_sealed_block(),
                reth_consensus::ConsensusError::BodyStateRootDiff(
                    reth_primitives_traits::GotExpected {
                        got: state_root,
                        expected: block_state_root,
                    }
                    .into(),
                )
                .into(),
            )
            .into())
        }

        if let Some(valid_block_tx) = valid_block_tx {
            let _ = valid_block_tx.send(());
        }

        let changeset_provider =
            ensure_ok_post_block!(overlay_factory.database_provider_ro(), block);

        let executed_block = self.inner.spawn_deferred_trie_task(
            block,
            output,
            &ctx,
            hashed_state,
            trie_output,
            changeset_provider,
        );
        Ok((executed_block, None))
    }

    /// Multi-segment block execution.
    ///
    /// Consumes transactions from the handle's iterator, splitting them across
    /// segment boundaries defined in the execution plan.
    #[expect(clippy::type_complexity)]
    fn execute_block_multiseg<S, Err, Tx>(
        &mut self,
        state_provider: S,
        env: ExecutionEnv<Evm>,
        input: &BlockOrPayload<EthEngineTypes>,
        handle: &mut reth_engine_tree::tree::PayloadHandle<
            Tx,
            Err,
            reth_ethereum_primitives::Receipt,
        >,
        plan: &BlockExecutionPlan,
    ) -> Result<
        (
            BlockExecutionOutput<reth_ethereum_primitives::Receipt>,
            Vec<Address>,
            tokio::sync::oneshot::Receiver<(B256, Bloom)>,
        ),
        InsertBlockErrorKind,
    >
    where
        S: StateProvider + Send,
        Tx: ExecutableTxFor<Evm>,
        Err: core::error::Error + Send + Sync + 'static,
    {
        debug!(
            target: "engine::bb",
            num_segments = plan.segments.len(),
            seed_hashes = plan.seed_block_hashes.len(),
            "Starting multi-segment execution"
        );

        let mut db = State::builder()
            .with_database(StateProviderDatabase::new(state_provider))
            .with_bundle_update()
            .build();

        // Seed prior block hashes for BLOCKHASH opcode
        for &(block_number, block_hash) in &plan.seed_block_hashes {
            db.block_hashes.insert(block_number, block_hash);
            trace!(target: "engine::bb", block_number, ?block_hash, "Seeded block hash");
        }

        let spec_id = *env.evm_env.spec_id();

        // Spawn receipt root background task
        let receipts_len = input.transaction_count();
        let (receipt_tx, receipt_rx) = crossbeam_channel::unbounded();
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();
        let task_handle = ReceiptRootTaskHandle::new(receipt_rx, result_tx);
        self.inner
            .payload_processor
            .executor()
            .spawn_blocking_named("receipt-root", move || task_handle.run(receipts_len));

        let transaction_count = input.transaction_count();
        let executed_tx_index = Arc::clone(handle.executed_tx_index());
        let execution_start = Instant::now();

        // Collect all transactions up front so the mutable borrow on `handle`
        // is released before the segment loop calls state_hook/state_hook_no_finish.
        let all_txs: Vec<Result<Tx, Err>> = handle.iter_transactions().collect();
        let mut tx_drain = all_txs.into_iter();
        let mut all_senders = Vec::with_capacity(transaction_count);
        let mut all_receipts: Vec<reth_ethereum_primitives::Receipt> =
            Vec::with_capacity(transaction_count);
        let mut all_requests = alloy_eips::eip7685::Requests::default();
        let mut gas_offset: u64 = 0;
        let mut blob_gas_used: u64 = 0;
        let mut global_tx_idx = 0usize;
        let num_segments = plan.segments.len();

        // Each iteration executes transactions in [global_tx_idx..stop_before)
        // under the segment's EVM environment. The first segment (idx 0) has
        // stop_before_tx == 0, meaning it is used for environment only — real
        // boundaries start from segments[1]. After the last segment boundary,
        // remaining transactions form the "tail".
        for (seg_idx, segment) in plan.segments.iter().enumerate() {
            // The tx boundary for this segment is the *next* segment's
            // stop_before_tx, or transaction_count for the last segment.
            let stop_before = if seg_idx + 1 < num_segments {
                plan.segments[seg_idx + 1].stop_before_tx
            } else {
                transaction_count
            };

            // Skip segments that would execute 0 transactions (e.g. a trailing
            // env_switch at exactly transaction_count).
            if stop_before <= global_tx_idx {
                continue;
            }

            let is_final = stop_before >= transaction_count;

            debug!(
                target: "engine::bb",
                seg_idx, global_tx_idx, stop_before, gas_offset, is_final,
                "Executing segment"
            );

            // Create EVM and executor in a block scope so the &mut db borrow
            // is released when the scope ends, allowing the next iteration to
            // borrow db again.
            let result = {
                let seg_evm_env = self
                    .inner
                    .evm_config
                    .evm_env_for_payload(&segment.execution_data)
                    .map_err(|e| InsertBlockErrorKind::Other(Box::new(e)))?;
                let seg_ctx = self
                    .inner
                    .evm_config
                    .context_for_payload(&segment.execution_data)
                    .map_err(|e| InsertBlockErrorKind::Other(Box::new(e)))?;

                let evm = self.inner.evm_config.evm_with_env(&mut db, seg_evm_env);
                let mut executor = self.inner.evm_config.create_executor(evm, seg_ctx);

                if !self.inner.config.precompile_cache_disabled() {
                    executor.evm_mut().precompiles_mut().map_cacheable_precompiles(
                        |address, precompile| {
                            let metrics = self
                                .inner
                                .precompile_cache_metrics
                                .entry(*address)
                                .or_insert_with(|| {
                                    CachedPrecompileMetrics::new_with_address(*address)
                                })
                                .clone();
                            CachedPrecompile::wrap(
                                precompile,
                                self.inner.precompile_cache_map.cache_for_address(*address),
                                spec_id,
                                Some(metrics),
                            )
                        },
                    );
                }

                let state_hook: Option<Box<dyn OnStateHook>> = if is_final {
                    handle.state_hook().map(|h| Box::new(h) as Box<dyn OnStateHook>)
                } else {
                    handle.state_hook_no_finish().map(|h| Box::new(h) as Box<dyn OnStateHook>)
                };
                let mut executor = executor.with_state_hook(state_hook);

                executor.apply_pre_execution_changes()?;

                let mut last_sent_len = 0usize;
                while global_tx_idx < stop_before {
                    let tx_result = tx_drain.next().ok_or_else(|| {
                        InsertBlockErrorKind::Other(
                            "ran out of transactions before segment boundary".into(),
                        )
                    })?;
                    let tx = tx_result.map_err(reth_errors::BlockExecutionError::other)?;

                    let tx_signer = *<Tx as alloy_evm::RecoveredTx<_>>::signer(&tx);
                    all_senders.push(tx_signer);

                    executor.execute_transaction(tx)?;
                    global_tx_idx += 1;
                    executed_tx_index.store(global_tx_idx, Ordering::Relaxed);

                    let current_len = executor.receipts().len();
                    if current_len > last_sent_len {
                        last_sent_len = current_len;
                        if let Some(receipt) = executor.receipts().last() {
                            let adjusted = if gas_offset > 0 {
                                receipt.with_gas_offset(gas_offset)
                            } else {
                                receipt.clone()
                            };
                            let _ =
                                receipt_tx.send(IndexedReceipt::new(global_tx_idx - 1, adjusted));
                        }
                    }
                }

                executor.finish()?.1
            };

            // Accumulate results with gas offset adjustment
            for receipt in &result.receipts {
                all_receipts.push(if gas_offset > 0 {
                    receipt.with_gas_offset(gas_offset)
                } else {
                    receipt.clone()
                });
            }
            all_requests.extend(result.requests);
            gas_offset += result.gas_used;
            blob_gas_used += result.blob_gas_used;
            let expected_seg_gas = segment.execution_data.gas_used();
            debug!(
                target: "engine::bb",
                seg_idx,
                block_number = segment.execution_data.block_number(),
                expected_seg_gas,
                actual_seg_gas = result.gas_used,
                gas_offset,
                receipts = result.receipts.len(),
                "Segment finished"
            );

            // Seed this segment's block hash for BLOCKHASH in subsequent segments.
            // The next segment needs to look up the current segment's block hash via
            // BLOCKHASH(current_block_number). We derive this from the next segment's
            // parent_hash (which is the current segment's block hash) and block_number.
            if seg_idx + 1 < num_segments {
                let next_seg = &plan.segments[seg_idx + 1];
                let finished_block_number = next_seg.execution_data.block_number() - 1;
                let finished_block_hash = next_seg.execution_data.parent_hash();
                db.block_hashes.insert(finished_block_number, finished_block_hash);
                trace!(
                    target: "engine::bb",
                    finished_block_number, ?finished_block_hash,
                    "Seeded inter-segment block hash"
                );
            }
        }

        drop(receipt_tx);

        // Merge all transitions into bundle state
        db.merge_transitions(BundleRetention::Reverts);

        let output = BlockExecutionOutput {
            result: reth_evm::block::BlockExecutionResult {
                receipts: all_receipts,
                requests: all_requests,
                gas_used: gas_offset,
                blob_gas_used,
            },
            state: db.take_bundle(),
        };

        self.inner.metrics.record_block_execution(&output, execution_start.elapsed());
        debug!(target: "engine::bb", total_gas = gas_offset, "Multi-segment execution complete");

        Ok((output, all_senders, result_rx))
    }
}

// ---------------------------------------------------------------------------
// EngineValidator trait impl
// ---------------------------------------------------------------------------

impl<P, Evm, V> EngineValidator<EthEngineTypes> for BbEngineValidator<P, Evm, V>
where
    P: BbProvider,
    V: PayloadValidator<EthEngineTypes, Block = reth_ethereum_primitives::Block> + Clone,
    Evm: ConfigureEngineEvm<ExecutionData, Primitives = EthPrimitives> + 'static,
{
    fn validate_payload_attributes_against_header(
        &self,
        attr: &<EthEngineTypes as PayloadTypes>::PayloadAttributes,
        header: &alloy_consensus::Header,
    ) -> Result<(), InvalidPayloadAttributesError> {
        self.inner.validate_payload_attributes_against_header(attr, header)
    }

    fn convert_payload_to_block(
        &self,
        payload: ExecutionData,
    ) -> Result<SealedBlock<reth_ethereum_primitives::Block>, NewPayloadError> {
        self.inner.convert_payload_to_block(payload)
    }

    fn validate_payload(
        &mut self,
        payload: ExecutionData,
        ctx: TreeCtx<'_, EthPrimitives>,
    ) -> ValidationOutcome<EthPrimitives> {
        let payload_hash = ExecutionPayload::block_hash(&payload);

        if let Some(plan) = self.take_execution_plan(payload_hash) {
            info!(
                target: "engine::bb",
                ?payload_hash,
                segments = plan.segments.len(),
                "Multi-segment payload detected"
            );
            self.validate_payload_multiseg(payload, ctx, plan)
        } else {
            self.inner.validate_block_with_state(BlockOrPayload::Payload(payload), ctx)
        }
    }

    fn validate_block(
        &mut self,
        block: SealedBlock<reth_ethereum_primitives::Block>,
        ctx: TreeCtx<'_, EthPrimitives>,
    ) -> ValidationOutcome<EthPrimitives> {
        self.inner.validate_block_with_state(BlockOrPayload::Block(block), ctx)
    }

    fn on_inserted_executed_block(&self, block: ExecutedBlock<EthPrimitives>) {
        self.inner.payload_processor.on_inserted_executed_block(
            block.recovered_block.block_with_parent(),
            &block.execution_output.state,
        );
    }
}

// ---------------------------------------------------------------------------
// BbEngineValidatorBuilder
// ---------------------------------------------------------------------------

/// Builder that creates a [`BbEngineValidator`] by wrapping the upstream
/// [`BasicEngineValidatorBuilder`].
#[derive(Debug, Clone)]
pub struct BbEngineValidatorBuilder<EV> {
    inner: BasicEngineValidatorBuilder<EV>,
    pending: BigBlockMap,
}

impl<EV: Default> BbEngineValidatorBuilder<EV> {
    /// Creates a new builder with default inner builder and the given pending map.
    pub fn new(pending: BigBlockMap) -> Self {
        Self { inner: BasicEngineValidatorBuilder::default(), pending }
    }
}

impl<Node, EV> EngineValidatorBuilder<Node> for BbEngineValidatorBuilder<EV>
where
    Node: FullNodeComponents<Evm: ConfigureEngineEvm<ExecutionData>>,
    EV: PayloadValidatorBuilder<Node>,
    EV::Validator: PayloadValidator<
            <Node::Types as NodeTypes>::Payload,
            Block = reth_node_api::BlockTy<Node::Types>,
        > + Clone,
    BasicEngineValidatorBuilder<EV>: EngineValidatorBuilder<
        Node,
        EngineValidator = BasicEngineValidator<Node::Provider, Node::Evm, EV::Validator>,
    >,
    BbEngineValidator<Node::Provider, Node::Evm, EV::Validator>: EngineValidator<<Node::Types as NodeTypes>::Payload, <Node::Types as NodeTypes>::Primitives>
        + WaitForCaches,
{
    type EngineValidator = BbEngineValidator<Node::Provider, Node::Evm, EV::Validator>;

    async fn build_tree_validator(
        self,
        ctx: &AddOnsContext<'_, Node>,
        tree_config: TreeConfig,
        changeset_cache: ChangesetCache,
    ) -> eyre::Result<Self::EngineValidator> {
        let inner = self.inner.build_tree_validator(ctx, tree_config, changeset_cache).await?;
        Ok(BbEngineValidator { inner, pending: self.pending })
    }
}
