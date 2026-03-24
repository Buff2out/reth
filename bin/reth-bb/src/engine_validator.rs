//! Big-block engine validator.
//!
//! Provides [`BbBlockExecutor`], an implementation of [`EngineValidatorBlockExecutor`]
//! that performs multi-segment execution when [`BigBlockData`] is present for a
//! payload hash, and [`BbEngineValidatorBuilder`] to wire it into
//! [`BasicEngineValidator`].

pub(crate) use reth_engine_primitives::BigBlockData;

use crate::BigBlockMap;
use alloy_evm::Evm as _;
use alloy_primitives::{Bloom, B256};
use alloy_rpc_types::engine::ExecutionData;
use reth_chainspec::EthChainSpec;
use reth_engine_primitives::{ConfigureEngineEvm, ExecutionPayload, PayloadValidator};
use reth_engine_tree::tree::{
    error::InsertBlockErrorKind,
    payload_processor::receipt_root_task::{IndexedReceipt, ReceiptRootTaskHandle},
    payload_validator::{
        BasicEngineValidator, BasicEngineValidatorBlockExecutor, BlockOrPayload,
        EngineValidatorBlockExecutor, ExecuteBlockCtx, ExecuteBlockResult,
    },
    precompile_cache::{CachedPrecompile, CachedPrecompileMetrics},
    ExecutionEnv, PayloadHandle,
};
use reth_ethereum_primitives::EthPrimitives;
use reth_evm::{block::BlockExecutor, execute::ExecutableTxFor, ConfigureEvm, OnStateHook};
use reth_node_api::{AddOnsContext, FullNodeComponents, NodeTypes, TreeConfig};
use reth_node_builder::{
    invalid_block_hook::InvalidBlockHookExt,
    rpc::{ChangesetCache, EngineValidatorBuilder, PayloadValidatorBuilder},
};
use reth_payload_primitives::PayloadTypes;
use reth_provider::{BlockExecutionOutput, StateProvider};
use reth_revm::{
    database::StateProviderDatabase,
    db::{states::bundle_state::BundleRetention, State},
};
use revm_primitives::Address;
use std::sync::{atomic::Ordering, Arc};
use tracing::{debug, info, trace};

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
// BbBlockExecutor
// ---------------------------------------------------------------------------

/// Block executor strategy that supports multi-segment big-block execution.
///
/// When [`BigBlockData`] has been stashed for a payload hash, the block is split
/// into multiple EVM execution segments. Otherwise execution is delegated to
/// [`BasicEngineValidatorBlockExecutor`].
#[derive(Debug, Clone)]
pub struct BbBlockExecutor {
    /// Shared map of pending big-block metadata, keyed by payload hash.
    pub pending: BigBlockMap,
}

impl BbBlockExecutor {
    /// Builds the execution plan for the given input.
    ///
    /// If [`BigBlockData`] was stashed for this payload hash, it is consumed and
    /// converted into a multi-segment plan. Otherwise `None` is returned and the
    /// caller should delegate to [`BasicEngineValidatorBlockExecutor`].
    fn take_execution_plan<T: PayloadTypes>(
        &self,
        input: &BlockOrPayload<T>,
    ) -> Option<BlockExecutionPlan> {
        let payload_hash = input.hash();

        let bb = self.pending.lock().unwrap().remove(&payload_hash)?;

        assert!(
            bb.env_switches.first().is_some_and(|(idx, _)| *idx == 0),
            "first env_switch must be at transaction index 0"
        );

        let segments: Vec<_> = bb
            .env_switches
            .into_iter()
            .map(|(cumulative_tx_count, execution_data)| ExecutionSegment {
                stop_before_tx: cumulative_tx_count,
                execution_data,
            })
            .collect();

        info!(
            target: "engine::bb",
            ?payload_hash,
            segments = segments.len(),
            "Multi-segment payload detected"
        );

        Some(BlockExecutionPlan { seed_block_hashes: bb.prior_block_hashes, segments })
    }

    /// Multi-segment block execution.
    ///
    /// Consumes transactions from the handle's iterator, splitting them across
    /// segment boundaries defined in the execution plan.
    fn execute_block_multiseg<Evm, S, Err, Tx, T>(
        &mut self,
        ctx: &mut ExecuteBlockCtx<'_, Evm>,
        state_provider: S,
        env: ExecutionEnv<Evm>,
        input: &BlockOrPayload<T>,
        handle: &mut PayloadHandle<Tx, Err, reth_ethereum_primitives::Receipt>,
        plan: &BlockExecutionPlan,
    ) -> ExecuteBlockResult<reth_ethereum_primitives::Receipt>
    where
        Evm: ConfigureEvm<Primitives = EthPrimitives>
            + ConfigureEngineEvm<ExecutionData, Primitives = EthPrimitives>
            + 'static,
        S: StateProvider + Send,
        Tx: ExecutableTxFor<Evm>,
        Err: core::error::Error + Send + Sync + 'static,
        T: PayloadTypes,
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
        ctx.payload_processor
            .executor()
            .spawn_blocking_named("receipt-root", move || task_handle.run(receipts_len));

        let transaction_count = input.transaction_count();
        let executed_tx_index = Arc::clone(handle.executed_tx_index());
        let execution_start = reth_primitives_traits::FastInstant::now();

        // Collect all transactions up front so the mutable borrow on `handle`
        // is released before the segment loop calls state_hook.
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

        for (seg_idx, segment) in plan.segments.iter().enumerate() {
            let stop_before = if seg_idx + 1 < num_segments {
                plan.segments[seg_idx + 1].stop_before_tx
            } else {
                transaction_count
            };

            if stop_before <= global_tx_idx {
                continue;
            }

            let is_final = stop_before >= transaction_count;

            debug!(
                target: "engine::bb",
                seg_idx, global_tx_idx, stop_before, gas_offset, is_final,
                "Executing segment"
            );

            let result = {
                let seg_evm_env = ctx
                    .evm_config
                    .evm_env_for_payload(&segment.execution_data)
                    .map_err(|e| InsertBlockErrorKind::Other(Box::new(e)))?;
                let seg_ctx = ctx
                    .evm_config
                    .context_for_payload(&segment.execution_data)
                    .map_err(|e| InsertBlockErrorKind::Other(Box::new(e)))?;

                let evm = ctx.evm_config.evm_with_env(&mut db, seg_evm_env);
                let mut executor = ctx.evm_config.create_executor(evm, seg_ctx);

                if !ctx.config.precompile_cache_disabled() {
                    executor.evm_mut().precompiles_mut().map_cacheable_precompiles(
                        |address, precompile| {
                            let metrics = ctx
                                .precompile_cache_metrics
                                .entry(*address)
                                .or_insert_with(|| {
                                    CachedPrecompileMetrics::new_with_address(*address)
                                })
                                .clone();
                            CachedPrecompile::wrap(
                                precompile,
                                ctx.precompile_cache_map.cache_for_address(*address),
                                spec_id,
                                Some(metrics),
                            )
                        },
                    );
                }

                let state_hook = handle.state_hook().map(|h| Box::new(h) as Box<dyn OnStateHook>);
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
        handle.finish_state_updates();

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

        ctx.metrics.record_block_execution(&output, execution_start.elapsed());
        debug!(target: "engine::bb", total_gas = gas_offset, "Multi-segment execution complete");

        Ok((output, all_senders, result_rx))
    }
}

impl<Evm> EngineValidatorBlockExecutor<EthPrimitives, Evm> for BbBlockExecutor
where
    Evm: ConfigureEvm<Primitives = EthPrimitives>
        + ConfigureEngineEvm<ExecutionData, Primitives = EthPrimitives>
        + 'static,
{
    fn execute_block<S, Tx, Err, T>(
        &mut self,
        ctx: &mut ExecuteBlockCtx<'_, Evm>,
        state_provider: S,
        env: ExecutionEnv<Evm>,
        input: &BlockOrPayload<T>,
        handle: &mut PayloadHandle<Tx, Err, reth_ethereum_primitives::Receipt>,
    ) -> ExecuteBlockResult<reth_ethereum_primitives::Receipt>
    where
        S: StateProvider + Send,
        Tx: ExecutableTxFor<Evm>,
        Err: core::error::Error + Send + Sync + 'static,
        T: PayloadTypes<
            BuiltPayload: reth_payload_primitives::BuiltPayload<Primitives = EthPrimitives>,
        >,
        Evm: ConfigureEngineEvm<T::ExecutionData, Primitives = EthPrimitives>,
    {
        // Check for pending big-block data
        let plan = self.take_execution_plan(input);

        match plan {
            Some(plan) => {
                // Multi-segment execution
                self.execute_block_multiseg(ctx, state_provider, env, input, handle, &plan)
            }
            None => {
                // No big-block data — delegate to standard single-segment execution
                BasicEngineValidatorBlockExecutor.execute_block(
                    ctx,
                    state_provider,
                    env,
                    input,
                    handle,
                )
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BbEngineValidatorBuilder
// ---------------------------------------------------------------------------

/// Builder that creates a [`BasicEngineValidator`] with [`BbBlockExecutor`].
#[derive(Debug, Clone)]
pub struct BbEngineValidatorBuilder<EV> {
    payload_validator_builder: EV,
    pending: BigBlockMap,
}

impl<EV: Default> BbEngineValidatorBuilder<EV> {
    /// Creates a new builder with default payload validator builder and the given pending map.
    pub fn new(pending: BigBlockMap) -> Self {
        Self { payload_validator_builder: EV::default(), pending }
    }
}

impl<Node, EV> EngineValidatorBuilder<Node> for BbEngineValidatorBuilder<EV>
where
    Node: FullNodeComponents<
        Types: NodeTypes<Primitives = EthPrimitives>,
        Evm: ConfigureEngineEvm<
            <<Node::Types as NodeTypes>::Payload as PayloadTypes>::ExecutionData,
            Primitives = EthPrimitives,
        > + ConfigureEngineEvm<ExecutionData, Primitives = EthPrimitives>,
    >,
    EV: PayloadValidatorBuilder<Node>,
    EV::Validator: PayloadValidator<
            <Node::Types as NodeTypes>::Payload,
            Block = reth_node_api::BlockTy<Node::Types>,
        > + Clone,
{
    type EngineValidator =
        BasicEngineValidator<Node::Provider, Node::Evm, EV::Validator, BbBlockExecutor>;

    async fn build_tree_validator(
        self,
        ctx: &AddOnsContext<'_, Node>,
        tree_config: TreeConfig,
        changeset_cache: ChangesetCache,
    ) -> eyre::Result<Self::EngineValidator> {
        let validator = self.payload_validator_builder.build(ctx).await?;
        let data_dir = ctx.config.datadir.clone().resolve_datadir(ctx.config.chain.chain());
        let invalid_block_hook = ctx.create_invalid_block_hook(&data_dir).await?;

        Ok(BasicEngineValidator::new_with_executor(
            ctx.node.provider().clone(),
            Arc::new(ctx.node.consensus().clone()),
            ctx.node.evm_config().clone(),
            validator,
            tree_config,
            invalid_block_hook,
            changeset_cache,
            ctx.node.task_executor().clone(),
            BbBlockExecutor { pending: self.pending },
        ))
    }
}
