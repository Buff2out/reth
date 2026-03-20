//! Command for generating large blocks by merging transactions from consecutive real blocks.
//!
//! This command fetches N consecutive blocks from an RPC, takes block 0 as the "base" payload,
//! concatenates transactions from blocks 1..N-1, and saves the result to disk as a
//! [`BigBlockPayload`] JSON file containing the merged [`ExecutionData`] and environment switches
//! at each block boundary.

use alloy_eips::Typed2718;
use alloy_primitives::{Bytes, B256};
use alloy_provider::{network::AnyNetwork, Provider, RootProvider};
use alloy_rpc_client::ClientBuilder;
use alloy_rpc_types_engine::{
    CancunPayloadFields, ExecutionData, ExecutionPayload, ExecutionPayloadSidecar,
};
use clap::Parser;
use eyre::Context;
use reth_cli_runner::CliContext;
use serde::{Deserialize, Serialize};
use std::future::Future;
use tracing::info;

/// A single transaction with its gas used and raw encoded bytes.
#[derive(Debug, Clone)]
pub struct RawTransaction {
    /// The actual gas used by the transaction (from receipt).
    pub gas_used: u64,
    /// The transaction type (e.g., 3 for EIP-4844 blob txs).
    pub tx_type: u8,
    /// The raw RLP-encoded transaction bytes.
    pub raw: Bytes,
}

/// Abstraction over sources of transactions for big block generation.
///
/// Implementors provide transactions from different sources (RPC, database, files, etc.)
pub trait TransactionSource {
    /// Fetch transactions from a specific block number.
    ///
    /// Returns `Ok(None)` if the block doesn't exist.
    /// Returns `Ok(Some((transactions, gas_used)))` with the block's transactions and total gas.
    fn fetch_block_transactions(
        &self,
        block_number: u64,
    ) -> impl Future<Output = eyre::Result<Option<(Vec<RawTransaction>, u64)>>> + Send;
}

/// RPC-based transaction source that fetches from a remote node.
#[derive(Debug)]
pub struct RpcTransactionSource {
    provider: RootProvider<AnyNetwork>,
}

impl RpcTransactionSource {
    /// Create a new RPC transaction source.
    pub const fn new(provider: RootProvider<AnyNetwork>) -> Self {
        Self { provider }
    }

    /// Create from an RPC URL with retry backoff.
    pub fn from_url(rpc_url: &str) -> eyre::Result<Self> {
        let client = ClientBuilder::default()
            .layer(alloy_transport::layers::RetryBackoffLayer::new(10, 800, u64::MAX))
            .http(rpc_url.parse()?);
        let provider = RootProvider::<AnyNetwork>::new(client);
        Ok(Self { provider })
    }
}

impl TransactionSource for RpcTransactionSource {
    async fn fetch_block_transactions(
        &self,
        block_number: u64,
    ) -> eyre::Result<Option<(Vec<RawTransaction>, u64)>> {
        // Fetch block and receipts in parallel
        let (block, receipts) = tokio::try_join!(
            self.provider.get_block_by_number(block_number.into()).full(),
            self.provider.get_block_receipts(block_number.into())
        )?;

        let Some(block) = block else {
            return Ok(None);
        };

        let Some(receipts) = receipts else {
            return Err(eyre::eyre!("Receipts not found for block {}", block_number));
        };

        let block_gas_used = block.header.gas_used;

        // Convert cumulative gas from receipts to per-tx gas_used
        let mut prev_cumulative = 0u64;
        let transactions: Vec<RawTransaction> = block
            .transactions
            .txns()
            .zip(receipts.iter())
            .map(|(tx, receipt)| {
                let cumulative = receipt.inner.inner.inner.receipt.cumulative_gas_used;
                let gas_used = cumulative - prev_cumulative;
                prev_cumulative = cumulative;

                let with_encoded = tx.inner.inner.clone().into_encoded();
                RawTransaction {
                    gas_used,
                    tx_type: tx.inner.ty(),
                    raw: with_encoded.encoded_bytes().clone(),
                }
            })
            .collect();

        Ok(Some((transactions, block_gas_used)))
    }
}

/// Collects transactions from a source up to a target gas usage.
#[derive(Debug)]
pub struct TransactionCollector<S> {
    source: S,
    target_gas: u64,
}

impl<S: TransactionSource> TransactionCollector<S> {
    /// Create a new transaction collector.
    pub const fn new(source: S, target_gas: u64) -> Self {
        Self { source, target_gas }
    }

    /// Collect transactions starting from the given block number.
    ///
    /// Skips blob transactions (type 3) and collects until target gas is reached.
    /// Returns a `CollectionResult` with transactions, gas info, and next block.
    pub async fn collect(&self, start_block: u64) -> eyre::Result<CollectionResult> {
        self.collect_gas(start_block, self.target_gas).await
    }

    /// Collect transactions up to a specific gas target.
    ///
    /// This is used both for initial collection and for retry top-ups.
    pub async fn collect_gas(
        &self,
        start_block: u64,
        gas_target: u64,
    ) -> eyre::Result<CollectionResult> {
        let mut transactions: Vec<RawTransaction> = Vec::new();
        let mut total_gas: u64 = 0;
        let mut current_block = start_block;

        while total_gas < gas_target {
            let Some((block_txs, _)) = self.source.fetch_block_transactions(current_block).await?
            else {
                tracing::warn!(target: "reth-bench", block = current_block, "Block not found, stopping");
                break;
            };

            for tx in block_txs {
                // Skip blob transactions (EIP-4844, type 3)
                if tx.tx_type == 3 {
                    continue;
                }

                if total_gas + tx.gas_used <= gas_target {
                    total_gas += tx.gas_used;
                    transactions.push(tx);
                }

                if total_gas >= gas_target {
                    break;
                }
            }

            current_block += 1;

            // Stop early if remaining gas is under 1M (close enough to target)
            let remaining_gas = gas_target.saturating_sub(total_gas);
            if remaining_gas < 1_000_000 {
                break;
            }
        }

        info!(
            target: "reth-bench",
            total_txs = transactions.len(),
            gas_sent = total_gas,
            next_block = current_block,
            "Finished collecting transactions"
        );

        Ok(CollectionResult { transactions, gas_sent: total_gas, next_block: current_block })
    }
}

/// Result of collecting transactions from blocks.
#[derive(Debug)]
pub struct CollectionResult {
    /// Collected transactions with their gas info.
    pub transactions: Vec<RawTransaction>,
    /// Total gas sent (sum of historical `gas_used` for all collected txs).
    pub gas_sent: u64,
    /// Next block number to continue collecting from.
    pub next_block: u64,
}

/// Strips blob transactions (EIP-4844, type 3) from an [`ExecutionData`] and clears the
/// sidecar's versioned hashes. Blob transactions are not supported in merged big blocks
/// because the versioned hashes across multiple blocks cannot be reconciled.
fn strip_blob_transactions(data: &mut ExecutionData) {
    let txs = data.payload.transactions_mut();
    let before = txs.len();
    txs.retain(|tx| tx.first().copied() != Some(3));
    let removed = before - txs.len();

    if removed > 0 {
        info!(target: "reth-bench", removed, "Stripped blob transactions from payload");
    }

    // Zero out blob gas fields since blob txs were removed
    if let Some(v3) = data.payload.as_v3_mut() {
        v3.blob_gas_used = 0;
        v3.excess_blob_gas = 0;
    }

    // Rebuild sidecar with empty versioned_hashes to pass validation
    let cancun = data.sidecar.cancun().map(|c| CancunPayloadFields {
        versioned_hashes: Vec::new(),
        parent_beacon_block_root: c.parent_beacon_block_root,
    });
    let prague = data.sidecar.prague().cloned();
    data.sidecar = match (cancun, prague) {
        (Some(c), Some(p)) => ExecutionPayloadSidecar::v4(c, p),
        (Some(c), None) => ExecutionPayloadSidecar::v3(c),
        _ => ExecutionPayloadSidecar::none(),
    };
}

/// A merged big block payload with environment switches at block boundaries.
#[derive(Debug, Serialize, Deserialize)]
pub struct BigBlockPayload {
    /// The primary execution data with all concatenated transactions.
    pub execution_data: ExecutionData,
    /// Environment switches at block boundaries.
    /// Each entry is `(cumulative_tx_count, execution_data_of_next_block)`.
    pub env_switches: Vec<(usize, ExecutionData)>,
}

/// `reth bench generate-big-block` command
///
/// Generates a large block by fetching consecutive blocks from an RPC, merging their
/// transactions into a single payload, and saving the result to disk.
#[derive(Debug, Parser)]
pub struct Command {
    /// The RPC URL to use for fetching blocks.
    #[arg(long, value_name = "RPC_URL")]
    rpc_url: String,

    /// Block number to start from.
    #[arg(long, value_name = "FROM_BLOCK")]
    from_block: u64,

    /// Number of blocks to merge into a single big block.
    #[arg(long, value_name = "COUNT", default_value = "1")]
    count: u64,

    /// Output directory for generated payloads.
    #[arg(long, value_name = "OUTPUT_DIR")]
    output_dir: std::path::PathBuf,
}

impl Command {
    /// Execute the `generate-big-block` command.
    pub async fn execute(self, _ctx: CliContext) -> eyre::Result<()> {
        if self.count == 0 {
            return Err(eyre::eyre!("--count must be at least 1"));
        }

        info!(
            target: "reth-bench",
            from_block = self.from_block,
            count = self.count,
            output_dir = %self.output_dir.display(),
            "Generating big block payload"
        );

        // Create output directory
        std::fs::create_dir_all(&self.output_dir).wrap_err_with(|| {
            format!("Failed to create output directory: {:?}", self.output_dir)
        })?;

        // Set up RPC provider
        let client = ClientBuilder::default()
            .layer(alloy_transport::layers::RetryBackoffLayer::new(10, 800, u64::MAX))
            .http(self.rpc_url.parse()?);
        let provider = RootProvider::<AnyNetwork>::new(client);

        // Fetch all blocks with full transactions
        let mut blocks = Vec::with_capacity(self.count as usize);
        for i in 0..self.count {
            let block_number = self.from_block + i;
            info!(target: "reth-bench", block_number, "Fetching block");

            let rpc_block = provider
                .get_block_by_number(block_number.into())
                .full()
                .await?
                .ok_or_else(|| eyre::eyre!("Block {} not found", block_number))?;

            // Convert to consensus block
            let block = rpc_block
                .into_inner()
                .map_header(|header| header.map(|h| h.into_header_with_defaults()))
                .try_map_transactions(|tx| {
                    tx.try_into_either::<op_alloy_consensus::OpTxEnvelope>()
                })?
                .into_consensus();

            // Convert to ExecutionData
            let (payload, sidecar) = ExecutionPayload::from_block_slow(&block);
            let execution_data = ExecutionData { payload, sidecar };

            info!(
                target: "reth-bench",
                block_number,
                gas_used = execution_data.payload.as_v1().gas_used,
                tx_count = execution_data.payload.transactions().len(),
                "Fetched block"
            );

            blocks.push(execution_data);
        }

        // Block 0 is the base
        let mut base = blocks.remove(0);
        let mut env_switches = Vec::new();

        if !blocks.is_empty() {
            let mut cumulative_tx_count = base.payload.transactions().len();

            // Collect state from the last block for header fields
            let last = blocks.last().unwrap();
            let last_v1 = last.payload.as_v1();
            let final_state_root = last_v1.state_root;
            let final_receipts_root = last_v1.receipts_root;
            let final_logs_bloom = last_v1.logs_bloom;

            let mut total_gas_used = base.payload.as_v1().gas_used;
            let mut total_gas_limit = base.payload.as_v1().gas_limit;

            // Concatenate transactions from subsequent blocks and build env_switches
            for block_data in blocks {
                let block_v1 = block_data.payload.as_v1();
                total_gas_used += block_v1.gas_used;
                total_gas_limit += block_v1.gas_limit;

                // Record environment switch at this block boundary
                env_switches.push((cumulative_tx_count, block_data.clone()));

                // Append this block's transactions to the base payload
                let txs = block_data.payload.transactions().clone();
                cumulative_tx_count += txs.len();
                base.payload.transactions_mut().extend(txs);
            }

            // Mutate the base payload header
            let base_v1 = base.payload.as_v1_mut();
            base_v1.state_root = final_state_root;
            base_v1.gas_used = total_gas_used;
            base_v1.gas_limit = total_gas_limit;
            base_v1.receipts_root = final_receipts_root;
            base_v1.logs_bloom = final_logs_bloom;
            base_v1.block_hash = B256::ZERO;
        }

        // Strip blob transactions from the merged payload and all env_switch payloads
        strip_blob_transactions(&mut base);
        for (_, switch_data) in &mut env_switches {
            strip_blob_transactions(switch_data);
        }

        let big_block = BigBlockPayload { execution_data: base, env_switches };

        // Save to disk
        let filename =
            format!("big_block_{}_to_{}.json", self.from_block, self.from_block + self.count - 1);
        let filepath = self.output_dir.join(&filename);
        let json = serde_json::to_string_pretty(&big_block)?;
        std::fs::write(&filepath, &json)
            .wrap_err_with(|| format!("Failed to write payload to {:?}", filepath))?;

        info!(
            target: "reth-bench",
            path = %filepath.display(),
            total_txs = big_block.execution_data.payload.transactions().len(),
            total_gas_used = big_block.execution_data.payload.as_v1().gas_used,
            env_switches = big_block.env_switches.len(),
            "Big block payload saved"
        );

        Ok(())
    }
}
