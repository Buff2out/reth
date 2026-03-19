use crate::PruneLimiter;
use alloy_primitives::BlockNumber;
use itertools::Itertools;
use reth_db_api::{
    history::{plan_shard_prune, PrunedShardStats as PrunedIndices, ShardOp},
    models::ShardedKey,
    table::Table,
    transaction::DbTxMut,
    BlockNumberList, DatabaseError, RawKey, RawTable, RawValue,
};
use reth_provider::DBProvider;
use reth_prune_types::{SegmentOutput, SegmentOutputCheckpoint};
use rustc_hash::FxHashMap;

/// Result of pruning history changesets, used to build the final output.
pub(crate) struct HistoryPruneResult<K> {
    /// Map of the highest deleted changeset keys to their block numbers.
    pub(crate) highest_deleted: FxHashMap<K, BlockNumber>,
    /// The last block number that had changesets pruned.
    pub(crate) last_pruned_block: Option<BlockNumber>,
    /// Number of changesets pruned.
    pub(crate) pruned_count: usize,
    /// Whether pruning is complete.
    pub(crate) done: bool,
}

/// Finalizes history pruning by sorting sharded keys, pruning history indices, and building output.
///
/// This is shared between static file and database pruning for both account and storage history.
pub(crate) fn finalize_history_prune<Provider, T, K, SK>(
    provider: &Provider,
    result: HistoryPruneResult<K>,
    range_end: BlockNumber,
    limiter: &PruneLimiter,
    to_sharded_key: impl Fn(K, BlockNumber) -> T::Key,
    key_matches: impl Fn(&T::Key, &T::Key) -> bool,
    make_sentinel: impl Fn(&T::Key) -> T::Key,
) -> Result<SegmentOutput, DatabaseError>
where
    Provider: DBProvider<Tx: DbTxMut>,
    T: Table<Value = BlockNumberList>,
    T::Key: AsRef<ShardedKey<SK>> + Clone,
    K: Ord,
{
    let HistoryPruneResult { highest_deleted, last_pruned_block, pruned_count, done } = result;

    // If there's more changesets to prune, set the checkpoint block number to previous,
    // so we could finish pruning its changesets on the next run.
    let last_changeset_pruned_block = last_pruned_block
        .map(|block_number| if done { block_number } else { block_number.saturating_sub(1) })
        .unwrap_or(range_end);

    // Sort highest deleted block numbers and turn them into sharded keys.
    // We use `sorted_unstable` because no equal keys exist in the map.
    let highest_sharded_keys =
        highest_deleted.into_iter().sorted_unstable().map(|(key, block_number)| {
            to_sharded_key(key, block_number.min(last_changeset_pruned_block))
        });

    let outcomes = prune_history_indices::<Provider, T, _>(
        provider,
        highest_sharded_keys,
        key_matches,
        &make_sentinel,
    )?;

    let progress = limiter.progress(done);

    Ok(SegmentOutput {
        progress,
        pruned: pruned_count + outcomes.deleted,
        checkpoint: Some(SegmentOutputCheckpoint {
            block_number: Some(last_changeset_pruned_block),
            tx_number: None,
        }),
    })
}

/// Prune history indices according to the provided list of highest sharded keys.
///
/// For each logical key, collects all matching shards, runs the shared
/// [`plan_shard_prune`] planner, and applies the resulting operations via MDBX
/// cursor.
///
/// Returns total number of deleted, updated and unchanged entities.
pub(crate) fn prune_history_indices<Provider, T, SK>(
    provider: &Provider,
    highest_sharded_keys: impl IntoIterator<Item = T::Key>,
    key_matches: impl Fn(&T::Key, &T::Key) -> bool,
    make_sentinel: impl Fn(&T::Key) -> T::Key,
) -> Result<PrunedIndices, DatabaseError>
where
    Provider: DBProvider<Tx: DbTxMut>,
    T: Table<Value = BlockNumberList>,
    T::Key: AsRef<ShardedKey<SK>> + Clone,
{
    let mut outcomes = PrunedIndices::default();
    let mut cursor = provider.tx_ref().cursor_write::<RawTable<T>>()?;

    for sharded_key in highest_sharded_keys {
        let to_block = sharded_key.as_ref().highest_block_number;

        // Collect all shards for this logical key.
        let mut shards: Vec<(T::Key, BlockNumberList)> = Vec::new();
        let mut shard = cursor.seek(RawKey::new(sharded_key.clone()))?;

        loop {
            let Some((key, raw_value)) =
                shard.map(|(k, v)| Result::<_, DatabaseError>::Ok((k.key()?, v))).transpose()?
            else {
                break;
            };

            if !key_matches(&key, &sharded_key) {
                break;
            }

            shards.push((key, raw_value.value()?));
            shard = cursor.next()?;
        }

        let sentinel_key = make_sentinel(&sharded_key);
        let plan = plan_shard_prune(
            shards,
            to_block,
            |key| key.as_ref().highest_block_number,
            |key| key.as_ref().highest_block_number == u64::MAX,
            || sentinel_key.clone(),
        );

        outcomes.record(plan.outcome);

        // Apply operations via cursor
        for op in plan.ops {
            match op {
                ShardOp::Delete(key) => {
                    cursor.seek_exact(RawKey::new(key))?;
                    cursor.delete_current()?;
                }
                ShardOp::Put(key, list) => {
                    cursor.upsert(RawKey::new(key), &RawValue::new(list))?;
                }
            }
        }
    }

    Ok(outcomes)
}
