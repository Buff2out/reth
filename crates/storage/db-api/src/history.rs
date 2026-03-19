//! Shared history shard pruning logic.
//!
//! Both MDBX and RocksDB backends implement the same algorithm for pruning
//! history shards (account history, storage history). This module extracts
//! the decision logic into a pure planner that returns a [`ShardPrunePlan`]
//! — a list of delete/put operations that backends apply with their own I/O.

use crate::BlockNumberList;
use alloy_primitives::BlockNumber;

/// A planned mutation to a history shard.
#[derive(Debug, Clone)]
pub enum ShardOp<K> {
    /// Delete the shard at this key.
    Delete(K),
    /// Write (upsert) the shard at this key with the given block list.
    Put(K, BlockNumberList),
}

/// Outcome of planning a prune for one logical key's shard group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PruneShardOutcome {
    /// At least one shard was deleted.
    Deleted,
    /// At least one shard was updated (but none deleted).
    Updated,
    /// All shards were unchanged.
    Unchanged,
}

/// Result of planning a prune across one logical key's shard group.
#[derive(Debug, Clone)]
pub struct ShardPrunePlan<K> {
    /// Operations to apply.
    pub ops: Vec<ShardOp<K>>,
    /// Summary outcome.
    pub outcome: PruneShardOutcome,
}

/// Aggregated stats across multiple shard groups.
#[derive(Debug, Default, Clone, Copy)]
pub struct PrunedShardStats {
    /// Number of shard groups where at least one shard was deleted.
    pub deleted: usize,
    /// Number of shard groups where shards were updated but not deleted.
    pub updated: usize,
    /// Number of shard groups that were unchanged.
    pub unchanged: usize,
}

impl PrunedShardStats {
    /// Record the outcome of one shard group.
    pub fn record(&mut self, outcome: PruneShardOutcome) {
        match outcome {
            PruneShardOutcome::Deleted => self.deleted += 1,
            PruneShardOutcome::Updated => self.updated += 1,
            PruneShardOutcome::Unchanged => self.unchanged += 1,
        }
    }
}

/// Plans which shards to delete/update for one logical key's shard group.
///
/// This is backend-agnostic: it only inspects the shard data and returns
/// a plan of operations. The caller applies the plan using backend-specific
/// I/O (MDBX cursors, RocksDB batch, etc.).
///
/// # Arguments
///
/// * `shards` — All shards for one logical key, in DB order (ascending by `highest_block_number`).
///   The last shard should have the sentinel key (`u64::MAX`).
/// * `to_block` — Prune all block numbers `<= to_block`.
/// * `highest_block` — Extract the `highest_block_number` from a key.
/// * `is_sentinel` — Returns `true` if the key is the sentinel (`u64::MAX`).
/// * `make_sentinel` — Creates a sentinel key for this logical key.
pub fn plan_shard_prune<K: Clone>(
    shards: Vec<(K, BlockNumberList)>,
    to_block: BlockNumber,
    highest_block: impl Fn(&K) -> BlockNumber,
    is_sentinel: impl Fn(&K) -> bool,
    make_sentinel: impl Fn() -> K,
) -> ShardPrunePlan<K> {
    if shards.is_empty() {
        return ShardPrunePlan { ops: Vec::new(), outcome: PruneShardOutcome::Unchanged };
    }

    let mut ops = Vec::new();
    let mut deleted = false;
    let mut updated = false;
    let mut last_remaining: Option<(K, BlockNumberList)> = None;

    for (key, block_list) in shards {
        // Non-sentinel shard whose highest block is fully within the prune range —
        // delete it outright without inspecting individual block numbers.
        if !is_sentinel(&key) && highest_block(&key) <= to_block {
            ops.push(ShardOp::Delete(key));
            deleted = true;
            continue;
        }

        let original_len = block_list.len();
        let filtered = BlockNumberList::new_pre_sorted(block_list.iter().filter(|&b| b > to_block));

        if filtered.is_empty() {
            ops.push(ShardOp::Delete(key));
            deleted = true;
        } else if filtered.len() < original_len {
            ops.push(ShardOp::Put(key.clone(), filtered.clone()));
            last_remaining = Some((key, filtered));
            updated = true;
        } else {
            // Unchanged — no op needed, but track as potential last surviving shard.
            last_remaining = Some((key, block_list));
        }
    }

    // If the last surviving shard is not the sentinel, promote it:
    // delete the old key and re-insert under the sentinel key.
    if let Some((last_key, last_value)) = last_remaining {
        if !is_sentinel(&last_key) {
            ops.push(ShardOp::Delete(last_key));
            ops.push(ShardOp::Put(make_sentinel(), last_value));
            updated = true;
        }
    }

    let outcome = if deleted {
        PruneShardOutcome::Deleted
    } else if updated {
        PruneShardOutcome::Updated
    } else {
        PruneShardOutcome::Unchanged
    };

    ShardPrunePlan { ops, outcome }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_list(blocks: &[u64]) -> BlockNumberList {
        BlockNumberList::new_pre_sorted(blocks.iter().copied())
    }

    #[test]
    fn empty_shards_unchanged() {
        let plan = plan_shard_prune::<u64>(vec![], 10, |k| *k, |k| *k == u64::MAX, || u64::MAX);
        assert_eq!(plan.outcome, PruneShardOutcome::Unchanged);
        assert!(plan.ops.is_empty());
    }

    #[test]
    fn single_sentinel_shard_fully_pruned() {
        // Single shard with sentinel key, all blocks <= to_block
        let plan = plan_shard_prune(
            vec![(u64::MAX, make_list(&[1, 2, 3]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Deleted);
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0], ShardOp::Delete(u64::MAX)));
    }

    #[test]
    fn single_sentinel_shard_partially_pruned() {
        let plan = plan_shard_prune(
            vec![(u64::MAX, make_list(&[5, 10, 15, 20]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Updated);
        // Should put the filtered list under the sentinel key
        assert_eq!(plan.ops.len(), 1);
        match &plan.ops[0] {
            ShardOp::Put(k, list) => {
                assert_eq!(*k, u64::MAX);
                assert_eq!(list.iter().collect::<Vec<_>>(), vec![15, 20]);
            }
            _ => panic!("expected Put"),
        }
    }

    #[test]
    fn single_sentinel_shard_unchanged() {
        let plan = plan_shard_prune(
            vec![(u64::MAX, make_list(&[15, 20]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Unchanged);
        assert!(plan.ops.is_empty());
    }

    #[test]
    fn non_sentinel_fully_below_to_block_deleted() {
        // Non-sentinel shard with highest_block_number <= to_block
        let plan = plan_shard_prune(
            vec![(5, make_list(&[1, 2, 3, 4, 5])), (u64::MAX, make_list(&[15, 20]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Deleted);
        // Should delete the first shard; sentinel unchanged
        assert_eq!(plan.ops.len(), 1);
        assert!(matches!(plan.ops[0], ShardOp::Delete(5)));
    }

    #[test]
    fn sentinel_empties_promotes_previous_shard() {
        // Two shards: non-sentinel survives partially, sentinel empties
        // After pruning to_block=10:
        // - shard at key=8: blocks [5,8] → both <= 10, but key's highest is 8 <= 10 → delete
        // - sentinel: blocks [7,9] → both <= 10 → delete (empties)
        // No remaining shards → both deleted
        let plan = plan_shard_prune(
            vec![(8, make_list(&[5, 8])), (u64::MAX, make_list(&[7, 9]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Deleted);
    }

    #[test]
    fn non_sentinel_survives_gets_promoted_to_sentinel() {
        // Non-sentinel shard partially survives, sentinel is empty
        // shard at key=15: blocks [5, 12, 15] → filter > 10 → [12, 15]
        // No sentinel shard present, so last_remaining=(15, [12,15]) is not sentinel → promote
        let plan = plan_shard_prune(
            vec![(15, make_list(&[5, 12, 15]))],
            10,
            |k| *k,
            |k| *k == u64::MAX,
            || u64::MAX,
        );
        assert_eq!(plan.outcome, PruneShardOutcome::Updated);
        // Should: put filtered at key=15, then delete key=15 and put at sentinel
        // The ops are: Put(15, [12,15]), Delete(15), Put(MAX, [12,15])
        assert_eq!(plan.ops.len(), 3);
    }
}
