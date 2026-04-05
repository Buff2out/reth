use std::{future::Future, sync::Arc};

use alloy_consensus::{BlockHeader, Eip2718EncodableReceipt, TxReceipt};
use alloy_eips::{eip2718::Encodable2718, BlockId};
use alloy_primitives::{map::AddressMap, Bytes, B256, U256, U64};
use alloy_trie::{
    proof::{ProofNodes, ProofRetainer},
    root::adjust_index_for_rlp,
    HashBuilder,
};
use async_trait::async_trait;
use futures::{Stream, StreamExt};
use jsonrpsee::{core::RpcResult, PendingSubscriptionSink, SubscriptionMessage, SubscriptionSink};
use parking_lot::Mutex;
use reth_chain_state::{
    CanonStateNotification, CanonStateSubscriptions, ForkChoiceSubscriptions,
    PersistedBlockSubscriptions,
};
use reth_errors::RethResult;
use reth_evm::{execute::Executor, ConfigureEvm};
use reth_execution_types::ExecutionOutcome;
use reth_metrics::metrics::Counter;
use reth_primitives_traits::{NodePrimitives, SealedHeader};
use reth_rpc_api::{ReceiptWithProofResponse, RethApiServer};
use reth_rpc_eth_types::{EthApiError, EthResult};
use reth_storage_api::{
    BlockReader, BlockReaderIdExt, ChangeSetReader, HeaderProvider, ReceiptProvider,
    StateProviderFactory, TransactionVariant, TransactionsProvider,
};
use reth_tasks::{pool::BlockingTaskGuard, Runtime};
use reth_trie_common::Nibbles;
use schnellru::{ByLength, LruMap};
use serde::Serialize;
use tokio::sync::oneshot;

/// `reth` API implementation.
///
/// This type provides the functionality for handling `reth` prototype RPC requests.
pub struct RethApi<Provider, EvmConfig> {
    inner: Arc<RethApiInner<Provider, EvmConfig>>,
}

// === impl RethApi ===

impl<Provider, EvmConfig> RethApi<Provider, EvmConfig> {
    /// The provider that can interact with the chain.
    pub fn provider(&self) -> &Provider {
        &self.inner.provider
    }

    /// The evm config.
    pub fn evm_config(&self) -> &EvmConfig {
        &self.inner.evm_config
    }

    /// Create a new instance of the [`RethApi`]
    pub fn new(
        provider: Provider,
        evm_config: EvmConfig,
        blocking_task_guard: BlockingTaskGuard,
        task_spawner: Runtime,
    ) -> Self {
        let inner = Arc::new(RethApiInner {
            provider,
            evm_config,
            blocking_task_guard,
            task_spawner,
            receipt_proof_cache: Mutex::new(LruMap::new(ByLength::new(
                DEFAULT_RECEIPT_PROOF_CACHE_SIZE,
            ))),
            receipt_proof_metrics: ReceiptProofCacheMetrics::default(),
        });
        Self { inner }
    }
}

impl<Provider, EvmConfig> RethApi<Provider, EvmConfig>
where
    Provider: BlockReaderIdExt + ChangeSetReader + StateProviderFactory + 'static,
    EvmConfig: Send + Sync + 'static,
{
    /// Executes the future on a new blocking task.
    async fn on_blocking_task<C, F, R>(&self, c: C) -> EthResult<R>
    where
        C: FnOnce(Self) -> F,
        F: Future<Output = EthResult<R>> + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let this = self.clone();
        let f = c(this);
        self.inner.task_spawner.spawn_blocking_task(async move {
            let res = f.await;
            let _ = tx.send(res);
        });
        rx.await.map_err(|_| EthApiError::InternalEthError)?
    }

    /// Returns a map of addresses to changed account balanced for a particular block.
    pub async fn balance_changes_in_block(&self, block_id: BlockId) -> EthResult<AddressMap<U256>> {
        self.on_blocking_task(async move |this| this.try_balance_changes_in_block(block_id)).await
    }

    fn try_balance_changes_in_block(&self, block_id: BlockId) -> EthResult<AddressMap<U256>> {
        let Some(block_number) = self.provider().block_number_for_id(block_id)? else {
            return Err(EthApiError::HeaderNotFound(block_id))
        };

        let state = self.provider().state_by_block_id(block_id)?;
        let accounts_before = self.provider().account_block_changeset(block_number)?;
        let hash_map = accounts_before.iter().try_fold(
            AddressMap::default(),
            |mut hash_map, account_before| -> RethResult<_> {
                let current_balance = state.account_balance(&account_before.address)?;
                let prev_balance = account_before.info.map(|info| info.balance);
                if current_balance != prev_balance {
                    hash_map.insert(account_before.address, current_balance.unwrap_or_default());
                }
                Ok(hash_map)
            },
        )?;
        Ok(hash_map)
    }

    /// Returns the receipt for a transaction along with its Merkle proof against the receipts root.
    pub async fn receipt_with_proof(
        &self,
        tx_hash: B256,
    ) -> EthResult<Option<ReceiptWithProofResponse>>
    where
        Provider: TransactionsProvider + ReceiptProvider + HeaderProvider,
    {
        let permit = self
            .inner
            .blocking_task_guard
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EthApiError::InternalEthError)?;
        self.on_blocking_task(async move |this| {
            let _permit = permit;
            this.try_receipt_with_proof(tx_hash)
        })
        .await
    }

    fn try_receipt_with_proof(&self, tx_hash: B256) -> EthResult<Option<ReceiptWithProofResponse>>
    where
        Provider: TransactionsProvider + ReceiptProvider + HeaderProvider,
    {
        let Some((_, meta)) = self.provider().transaction_by_hash_with_meta(tx_hash)? else {
            return Ok(None);
        };

        let block_hash = meta.block_hash;
        let tx_index = meta.index as usize;

        let header = self
            .provider()
            .header(block_hash)?
            .ok_or(EthApiError::HeaderNotFound(block_hash.into()))?;
        let receipts_root = header.receipts_root();

        // Check the cache first.
        {
            let mut cache = self.inner.receipt_proof_cache.lock();
            if let Some(cached) = cache.get(&block_hash) {
                if tx_index >= cached.encoded_receipts.len() {
                    return Err(EthApiError::InvalidParams(format!(
                        "tx index {tx_index} out of bounds for block with {} receipts",
                        cached.encoded_receipts.len()
                    )));
                }

                self.inner.receipt_proof_metrics.hits.increment(1);

                let target_nibbles = Nibbles::unpack(alloy_rlp::encode_fixed_size(&tx_index));
                let proof = cached
                    .proof_nodes
                    .matching_nodes_sorted(&target_nibbles)
                    .into_iter()
                    .map(|(_, node)| node)
                    .collect();

                return Ok(Some(ReceiptWithProofResponse {
                    receipt: cached.encoded_receipts[tx_index].clone(),
                    proof,
                    receipts_root,
                    block_hash,
                    tx_index: meta.index,
                }));
            }
        }

        self.inner.receipt_proof_metrics.misses.increment(1);
        let receipts = self
            .provider()
            .receipts_by_block(block_hash.into())?
            .ok_or(EthApiError::HeaderNotFound(block_hash.into()))?;

        let len = receipts.len();
        if tx_index >= len {
            return Err(EthApiError::InvalidParams(format!(
                "tx index {tx_index} out of bounds for block with {len} receipts"
            )));
        }

        let cached = build_receipts_trie(&receipts);
        if cached.root != receipts_root {
            return Err(EthApiError::InternalEthError);
        }

        let target_nibbles = Nibbles::unpack(alloy_rlp::encode_fixed_size(&tx_index));
        let proof = cached
            .proof_nodes
            .matching_nodes_sorted(&target_nibbles)
            .into_iter()
            .map(|(_, node)| node)
            .collect();
        let receipt = cached.encoded_receipts[tx_index].clone();

        // Insert into cache.
        self.inner.receipt_proof_cache.lock().insert(block_hash, cached);

        Ok(Some(ReceiptWithProofResponse {
            receipt,
            proof,
            receipts_root,
            block_hash,
            tx_index: meta.index,
        }))
    }
}

impl<N, Provider, EvmConfig> RethApi<Provider, EvmConfig>
where
    N: NodePrimitives,
    Provider: BlockReaderIdExt
        + ChangeSetReader
        + StateProviderFactory
        + BlockReader<Block = N::Block>
        + CanonStateSubscriptions<Primitives = N>
        + 'static,
    EvmConfig: ConfigureEvm<Primitives = N> + 'static,
{
    /// Re-executes one or more consecutive blocks and returns the execution outcome.
    pub async fn block_execution_outcome(
        &self,
        block_id: BlockId,
        count: Option<U64>,
    ) -> EthResult<Option<ExecutionOutcome<N::Receipt>>> {
        const MAX_BLOCK_COUNT: u64 = 128;

        let block_count = count.map(|c| c.to::<u64>()).unwrap_or(1);
        if block_count == 0 || block_count > MAX_BLOCK_COUNT {
            return Err(EthApiError::InvalidParams(format!(
                "block count must be between 1 and {MAX_BLOCK_COUNT}, got {block_count}"
            )))
        }

        let permit = self
            .inner
            .blocking_task_guard
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| EthApiError::InternalEthError)?;
        self.on_blocking_task(async move |this| {
            let _permit = permit;
            this.try_block_execution_outcome(block_id, block_count)
        })
        .await
    }

    fn try_block_execution_outcome(
        &self,
        block_id: BlockId,
        block_count: u64,
    ) -> EthResult<Option<ExecutionOutcome<N::Receipt>>> {
        let Some(start_block) = self.provider().block_number_for_id(block_id)? else {
            return Ok(None)
        };

        if start_block == 0 {
            return Ok(Some(ExecutionOutcome::default()))
        }

        let state_provider = self.provider().history_by_block_number(start_block - 1)?;
        let db = reth_revm::database::StateProviderDatabase::new(&state_provider);

        let mut blocks = Vec::with_capacity(block_count as usize);
        for block_number in start_block..start_block + block_count {
            let Some(block) = self
                .provider()
                .recovered_block(block_number.into(), TransactionVariant::WithHash)?
            else {
                if block_number == start_block {
                    return Ok(None)
                }
                break;
            };
            blocks.push(block);
        }

        let outcome = self.evm_config().executor(db).execute_batch(&blocks).map_err(
            |e: reth_evm::execute::BlockExecutionError| {
                EthApiError::Internal(reth_errors::RethError::Other(e.into()))
            },
        )?;

        Ok(Some(outcome))
    }
}

#[async_trait]
impl<Provider, EvmConfig> RethApiServer for RethApi<Provider, EvmConfig>
where
    Provider: BlockReaderIdExt
        + ChangeSetReader
        + StateProviderFactory
        + BlockReader<Block = <Provider::Primitives as NodePrimitives>::Block>
        + CanonStateSubscriptions
        + ForkChoiceSubscriptions<Header = <Provider::Primitives as NodePrimitives>::BlockHeader>
        + PersistedBlockSubscriptions
        + 'static,
    EvmConfig: ConfigureEvm<Primitives = Provider::Primitives> + 'static,
{
    /// Handler for `reth_getReceiptWithProof`
    async fn reth_get_receipt_with_proof(
        &self,
        tx_hash: B256,
    ) -> RpcResult<Option<ReceiptWithProofResponse>> {
        Ok(Self::receipt_with_proof(self, tx_hash).await?)
    }

    /// Handler for `reth_getBalanceChangesInBlock`
    async fn reth_get_balance_changes_in_block(
        &self,
        block_id: BlockId,
    ) -> RpcResult<AddressMap<U256>> {
        Ok(Self::balance_changes_in_block(self, block_id).await?)
    }

    /// Handler for `reth_getBlockExecutionOutcome`
    async fn reth_get_block_execution_outcome(
        &self,
        block_id: BlockId,
        count: Option<U64>,
    ) -> RpcResult<Option<serde_json::Value>> {
        let outcome = Self::block_execution_outcome(self, block_id, count).await?;
        match outcome {
            Some(outcome) => {
                let value = serde_json::to_value(&outcome).map_err(|e| {
                    EthApiError::Internal(reth_errors::RethError::msg(e.to_string()))
                })?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Handler for `reth_subscribeChainNotifications`
    async fn reth_subscribe_chain_notifications(
        &self,
        pending: PendingSubscriptionSink,
    ) -> jsonrpsee::core::SubscriptionResult {
        let sink = pending.accept().await?;
        let stream = self.provider().canonical_state_stream();
        self.inner.task_spawner.spawn_task(pipe_from_stream(sink, stream));

        Ok(())
    }

    /// Handler for `reth_subscribePersistedBlock`
    async fn reth_subscribe_persisted_block(
        &self,
        pending: PendingSubscriptionSink,
    ) -> jsonrpsee::core::SubscriptionResult {
        let sink = pending.accept().await?;
        let stream = self.provider().persisted_block_stream();
        self.inner.task_spawner.spawn_task(pipe_from_stream(sink, stream));

        Ok(())
    }

    /// Handler for `reth_subscribeFinalizedChainNotifications`
    async fn reth_subscribe_finalized_chain_notifications(
        &self,
        pending: PendingSubscriptionSink,
    ) -> jsonrpsee::core::SubscriptionResult {
        let sink = pending.accept().await?;
        let canon_stream = self.provider().canonical_state_stream();
        let finalized_stream = self.provider().finalized_block_stream();
        self.inner.task_spawner.spawn_task(finalized_chain_notifications(
            sink,
            canon_stream,
            finalized_stream,
        ));

        Ok(())
    }
}

/// Pipes all stream items to the subscription sink.
async fn pipe_from_stream<S, T>(sink: SubscriptionSink, mut stream: S)
where
    S: Stream<Item = T> + Unpin,
    T: Serialize,
{
    loop {
        tokio::select! {
            _ = sink.closed() => {
                break
            }
            maybe_item = stream.next() => {
                let Some(item) = maybe_item else {
                    break
                };
                let msg = match SubscriptionMessage::new(sink.method_name(), sink.subscription_id(), &item) {
                    Ok(msg) => msg,
                    Err(err) => {
                        tracing::error!(target: "rpc::reth", %err, "Failed to serialize subscription message");
                        break
                    }
                };
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
        }
    }
}

/// Buffers committed chain notifications and emits them when a new finalized block is received.
async fn finalized_chain_notifications<N>(
    sink: SubscriptionSink,
    mut canon_stream: reth_chain_state::CanonStateNotificationStream<N>,
    mut finalized_stream: reth_chain_state::ForkChoiceStream<SealedHeader<N::BlockHeader>>,
) where
    N: NodePrimitives,
{
    let mut buffered: Vec<CanonStateNotification<N>> = Vec::new();

    loop {
        tokio::select! {
            _ = sink.closed() => {
                break
            }
            maybe_canon = canon_stream.next() => {
                let Some(notification) = maybe_canon else { break };
                match &notification {
                    CanonStateNotification::Commit { .. } => {
                        buffered.push(notification);
                    }
                    CanonStateNotification::Reorg { .. } => {
                        buffered.clear();
                    }
                }
            }
            maybe_finalized = finalized_stream.next() => {
                let Some(finalized_header) = maybe_finalized else { break };
                let finalized_num = finalized_header.number();

                let mut committed = Vec::new();
                buffered.retain(|n| {
                    if *n.committed().range().end() <= finalized_num {
                        committed.push(n.clone());
                        false
                    } else {
                        true
                    }
                });

                if committed.is_empty() {
                    continue;
                }

                committed.sort_by_key(|n| *n.committed().range().start());

                let msg = match SubscriptionMessage::new(
                    sink.method_name(),
                    sink.subscription_id(),
                    &committed,
                ) {
                    Ok(msg) => msg,
                    Err(err) => {
                        tracing::error!(target: "rpc::reth", %err, "Failed to serialize finalized chain notification");
                        break
                    }
                };
                if sink.send(msg).await.is_err() {
                    break;
                }
            }
        }
    }
}

impl<Provider, EvmConfig> std::fmt::Debug for RethApi<Provider, EvmConfig> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RethApi").finish_non_exhaustive()
    }
}

impl<Provider, EvmConfig> Clone for RethApi<Provider, EvmConfig> {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

/// Default number of blocks to cache receipt proof tries for.
const DEFAULT_RECEIPT_PROOF_CACHE_SIZE: u32 = 64;

struct RethApiInner<Provider, EvmConfig> {
    /// The provider that can interact with the chain.
    provider: Provider,
    /// The EVM configuration used to create block executors.
    evm_config: EvmConfig,
    /// Guard to restrict the number of concurrent block re-execution requests.
    blocking_task_guard: BlockingTaskGuard,
    /// The type that can spawn tasks which would otherwise block.
    task_spawner: Runtime,
    /// LRU cache of receipts trie proof nodes, keyed by block hash.
    /// Avoids rebuilding the trie when multiple users query proofs from the same block.
    receipt_proof_cache: Mutex<LruMap<B256, CachedReceiptsTrie>>,
    /// Metrics for receipt proof cache.
    receipt_proof_metrics: ReceiptProofCacheMetrics,
}

/// Metrics for the receipt proof cache.
#[derive(Clone)]
struct ReceiptProofCacheMetrics {
    /// Number of receipt proof cache hits.
    hits: Counter,
    /// Number of receipt proof cache misses.
    misses: Counter,
}

impl Default for ReceiptProofCacheMetrics {
    fn default() -> Self {
        Self {
            hits: reth_metrics::metrics::counter!("rpc.reth.receipt_proof_cache.hits_total"),
            misses: reth_metrics::metrics::counter!("rpc.reth.receipt_proof_cache.misses_total"),
        }
    }
}

/// Cached receipts trie data for a single block.
///
/// Built once on first proof request for a block, then reused for all subsequent
/// requests targeting any tx index in the same block.
struct CachedReceiptsTrie {
    /// EIP-2718 encoded receipts, indexed by tx position.
    encoded_receipts: Vec<Bytes>,
    /// All trie proof nodes retained during trie construction.
    proof_nodes: ProofNodes,
    /// The computed receipts root (must match the block header).
    root: B256,
}

/// Builds the full receipts trie with all proof nodes retained.
///
/// This is used to build a [`CachedReceiptsTrie`] that can serve proofs for any tx index
/// without rebuilding the trie.
fn build_receipts_trie<R: TxReceipt + Eip2718EncodableReceipt + Send + Sync>(
    receipts: &[R],
) -> CachedReceiptsTrie {
    let len = receipts.len();

    // Retain proof nodes for ALL tx indices.
    let all_targets: Vec<Nibbles> =
        (0..len).map(|i| Nibbles::unpack(alloy_rlp::encode_fixed_size(&i))).collect();
    let retainer = ProofRetainer::from_iter(all_targets);
    let mut hb = HashBuilder::default().with_proof_retainer(retainer);

    let mut encoded_receipts = vec![Bytes::new(); len];
    let mut buf = Vec::new();

    for i in 0..len {
        let exec_index = adjust_index_for_rlp(i, len);
        buf.clear();
        receipts[exec_index].with_bloom_ref().encode_2718(&mut buf);
        encoded_receipts[exec_index] = Bytes::copy_from_slice(&buf);

        let index_buffer = alloy_rlp::encode_fixed_size(&exec_index);
        hb.add_leaf(Nibbles::unpack(&index_buffer), &buf);
    }

    let root = hb.root();
    let proof_nodes = hb.take_proof_nodes();

    CachedReceiptsTrie { encoded_receipts, proof_nodes, root }
}

/// Builds a Merkle proof for a specific receipt within a list of receipts.
///
/// Returns the EIP-2718 encoded target receipt and the Merkle proof nodes. The proof can be
/// verified against the block's `receipts_root` using `alloy_trie::proof::verify_proof`.
///
/// Encodes receipts on-the-fly during trie construction to avoid allocating a separate buffer
/// for every receipt in the block.
fn build_receipt_proof<R: TxReceipt + Eip2718EncodableReceipt + Send + Sync>(
    receipts: &[R],
    tx_index: usize,
) -> Result<(Bytes, Vec<Bytes>), ()> {
    let len = receipts.len();
    if tx_index >= len {
        return Err(());
    }

    let target_key_buf = alloy_rlp::encode_fixed_size(&tx_index);
    let target_nibbles = Nibbles::unpack(&target_key_buf);

    let retainer = ProofRetainer::from_iter([target_nibbles.clone()]);
    let mut hb = HashBuilder::default().with_proof_retainer(retainer);

    let mut target_encoded = None;
    let mut buf = Vec::new();

    for i in 0..len {
        let exec_index = adjust_index_for_rlp(i, len);
        buf.clear();
        receipts[exec_index].with_bloom_ref().encode_2718(&mut buf);

        if exec_index == tx_index {
            target_encoded = Some(Bytes::copy_from_slice(&buf));
        }

        let index_buffer = alloy_rlp::encode_fixed_size(&exec_index);
        hb.add_leaf(Nibbles::unpack(&index_buffer), &buf);
    }

    let _root = hb.root();
    let proof_nodes = hb.take_proof_nodes();
    let proof = proof_nodes
        .matching_nodes_sorted(&target_nibbles)
        .into_iter()
        .map(|(_, node)| node)
        .collect();

    Ok((target_encoded.expect("target index was within bounds"), proof))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{EthereumReceipt, TxType};
    use alloy_primitives::{Address, Log, LogData};
    use alloy_trie::proof::verify_proof;

    fn make_receipt(status: bool, gas: u64, logs: Vec<Log>) -> EthereumReceipt {
        EthereumReceipt { tx_type: TxType::Legacy, success: status, cumulative_gas_used: gas, logs }
    }

    fn make_log(addr: Address, data: &[u8]) -> Log {
        Log { address: addr, data: LogData::new_unchecked(Vec::new(), data.to_vec().into()) }
    }

    fn encode_receipt_2718(receipt: &EthereumReceipt) -> Vec<u8> {
        let with_bloom = receipt.with_bloom_ref();
        let mut buf = Vec::new();
        with_bloom.encode_2718(&mut buf);
        buf
    }

    fn compute_receipts_root(receipts: &[EthereumReceipt]) -> B256 {
        let len = receipts.len();
        let mut hb = HashBuilder::default();
        let mut buf = Vec::new();
        for i in 0..len {
            let exec_index = adjust_index_for_rlp(i, len);
            buf.clear();
            receipts[exec_index].with_bloom_ref().encode_2718(&mut buf);
            let index_buffer = alloy_rlp::encode_fixed_size(&exec_index);
            hb.add_leaf(Nibbles::unpack(&index_buffer), &buf);
        }
        hb.root()
    }

    fn build_and_verify(receipts: &[EthereumReceipt], idx: usize) {
        let root = compute_receipts_root(receipts);
        let (receipt_bytes, proof) = build_receipt_proof(receipts, idx).unwrap();

        assert_eq!(receipt_bytes.as_ref(), encode_receipt_2718(&receipts[idx]));

        let key = alloy_rlp::encode_fixed_size(&idx);
        let result =
            verify_proof(root, Nibbles::unpack(&key), Some(receipt_bytes.to_vec()), &proof);
        assert!(result.is_ok(), "proof verification failed for index {idx}: {result:?}");

        // Also verify via the ReceiptWithProofResponse::verify utility.
        let response = ReceiptWithProofResponse {
            receipt: receipt_bytes,
            proof,
            receipts_root: root,
            block_hash: B256::ZERO,
            tx_index: idx as u64,
        };
        assert!(response.verify().is_ok(), "ReceiptWithProofResponse::verify failed for idx {idx}");
    }

    #[test]
    fn receipt_proof_single_tx() {
        build_and_verify(&[make_receipt(true, 21000, Vec::new())], 0);
    }

    #[test]
    fn receipt_proof_multiple_txs() {
        let receipts: Vec<EthereumReceipt> = (0..10)
            .map(|i| {
                make_receipt(
                    true,
                    21000 * (i + 1) as u64,
                    vec![make_log(
                        Address::with_last_byte(i as u8),
                        format!("data_{i}").as_bytes(),
                    )],
                )
            })
            .collect();

        for idx in [0, 1, 5, 9] {
            build_and_verify(&receipts, idx);
        }
    }

    #[test]
    fn receipt_proof_across_rlp_boundary() {
        let receipts: Vec<EthereumReceipt> =
            (0..130).map(|i| make_receipt(true, 21000 * (i + 1) as u64, Vec::new())).collect();

        for idx in [0, 1, 127, 128, 129] {
            build_and_verify(&receipts, idx);
        }
    }

    #[test]
    fn receipt_proof_with_logs() {
        let logs = vec![
            make_log(Address::with_last_byte(0xaa), b"Transfer(from,to,value)"),
            make_log(Address::with_last_byte(0xbb), b"Approval(owner,spender,value)"),
        ];
        let receipts = vec![
            make_receipt(true, 21000, Vec::new()),
            make_receipt(true, 50000, logs),
            make_receipt(false, 100000, Vec::new()),
        ];

        // Verify the receipt with logs (index 1).
        build_and_verify(&receipts, 1);

        // Verify the failed receipt (index 2).
        build_and_verify(&receipts, 2);
    }

    #[test]
    fn receipt_proof_out_of_bounds() {
        let receipts = vec![make_receipt(true, 21000, Vec::new())];
        assert!(build_receipt_proof(&receipts, 1).is_err());
        assert!(build_receipt_proof(&receipts, 100).is_err());
    }

    #[test]
    fn receipt_proof_empty_block() {
        let receipts: Vec<EthereumReceipt> = Vec::new();
        assert!(build_receipt_proof(&receipts, 0).is_err());
    }

    #[test]
    fn receipt_proof_tampered_receipt_fails_verification() {
        let receipts =
            vec![make_receipt(true, 21000, Vec::new()), make_receipt(true, 42000, Vec::new())];
        let root = compute_receipts_root(&receipts);
        let (receipt_bytes, proof) = build_receipt_proof(&receipts, 0).unwrap();

        // Tamper with the receipt bytes.
        let mut tampered = receipt_bytes.to_vec();
        if let Some(last) = tampered.last_mut() {
            *last ^= 0xff;
        }

        let key = alloy_rlp::encode_fixed_size(&0usize);
        let result = verify_proof(root, Nibbles::unpack(&key), Some(tampered), &proof);
        assert!(result.is_err(), "tampered proof should fail verification");
    }
}
