use rand::{SeedableRng, rngs::StdRng};
use reth_trie::test_utils::TrieTestHarness;
use reth_trie_common::Nibbles;
use reth_trie_sparse::{
    ArenaParallelSparseTrie, ArenaParallelismThresholds, ParallelSparseTrie,
    ParallelismThresholds, SparseTrie,
};

use crate::input::{BlockSpec, FuzzInput, RoundOp, ThresholdProfile};
use crate::model::{
    apply_changeset_to_live_state, build_initial_storage, choose_retained_keys,
    collect_proof_requests, merge_requests, realize_block,
};
use crate::pools::KeyPools;

/// Maximum number of reveal-update retry iterations before we give up.
const MAX_RETRY_ITERS: usize = 64;
/// Per-round operation count bounds.
const MIN_ROUND_OPS: usize = 3;
const MAX_ROUND_OPS: usize = 10;

/// Main fuzzer entry point. Drives both SparseTrie implementations through
/// the same multi-block lifecycle and asserts they agree on roots.
pub fn run(input: FuzzInput) {
    let round_count = 20 + (input.round_count as usize % 81);

    // Early exit if no rounds specified (libfuzzer can generate empty vecs).
    if input.rounds.is_empty() {
        return;
    }

    // 1. Build large initial state.
    let initial_storage = build_initial_storage(&input.initial);
    if initial_storage.is_empty() {
        return;
    }

    let mut live_state = initial_storage.clone();
    let mut harness = TrieTestHarness::new(initial_storage);

    // 2. Root-only reveal — maximally blinded start.
    let root_node = harness.root_node();

    let (arena_thresholds, map_thresholds) = materialize_thresholds(input.profile);

    let mut arena = ArenaParallelSparseTrie::default()
        .with_parallelism_thresholds(arena_thresholds);
    let mut map_trie = ParallelSparseTrie::default()
        .with_parallelism_thresholds(map_thresholds);

    arena
        .set_root(root_node.node.clone(), root_node.masks, false)
        .expect("arena set_root should succeed");
    map_trie
        .set_root(root_node.node, root_node.masks, false)
        .expect("map set_root should succeed");

    let mut pools = KeyPools::from_storage(&live_state);

    for round_idx in 0..round_count {
        // Cycle through the block specs if fewer than round_count.
        let spec = &input.rounds[round_idx % input.rounds.len()];

        // Build one production-like small block.
        let crate::model::RealizedBlock { leaf_updates, changeset, touched_keys } =
            realize_block(spec, &live_state, &mut pools);
        let mut pending_arena = leaf_updates.clone();
        let mut pending_map = leaf_updates;
        let mut pending_changeset = Some(changeset);

        let mut updates_applied = false;
        let mut pruned_this_round = false;

        let ops = materialize_round_ops(spec);
        for (op_idx, op) in ops.into_iter().enumerate() {
            match op {
                RoundOp::ApplyUpdates => {
                    apply_pending_updates(
                        &mut arena,
                        &mut map_trie,
                        &mut harness,
                        &mut pending_arena,
                        &mut pending_map,
                        round_idx,
                        op_idx,
                    );

                    if !updates_applied {
                        let changeset = pending_changeset
                            .take()
                            .expect("changeset should be available before first apply");
                        apply_changeset_to_live_state(&mut live_state, &changeset);
                        harness.apply_changeset(changeset);
                        updates_applied = true;
                    }
                }
                RoundOp::Prune => {
                    let mut prune_rng = StdRng::seed_from_u64(
                        spec.key_seed
                            .wrapping_add((round_idx as u64) << 16)
                            .wrapping_add(op_idx as u64),
                    );
                    let retained = choose_retained_keys(
                        spec,
                        &touched_keys,
                        &live_state,
                        &mut prune_rng,
                    );

                    let mut retained_paths: Vec<Nibbles> =
                        retained.iter().map(|k| Nibbles::unpack(*k)).collect();
                    retained_paths.sort_unstable();
                    retained_paths.dedup();

                    arena.prune(&retained_paths);
                    map_trie.prune(&retained_paths);

                    pools.observe_prune(&touched_keys, &retained, &live_state);
                    pruned_this_round = true;
                }
                RoundOp::CheckpointRoot => {
                    checkpoint_roots(&mut arena, &mut map_trie, &harness, round_idx, Some(op_idx));
                }
            }
        }

        // Ensure rounds eventually apply updates even if the op schedule omitted ApplyUpdates.
        if !updates_applied {
            apply_pending_updates(
                &mut arena,
                &mut map_trie,
                &mut harness,
                &mut pending_arena,
                &mut pending_map,
                round_idx,
                usize::MAX,
            );

            let changeset =
                pending_changeset.take().expect("changeset should exist when applying at round end");
            apply_changeset_to_live_state(&mut live_state, &changeset);
            harness.apply_changeset(changeset);
            updates_applied = true;
        }

        // Always checkpoint at end of round to keep strong invariants regardless of op schedule.
        if updates_applied {
            checkpoint_roots(&mut arena, &mut map_trie, &harness, round_idx, None);
        }

        if !pruned_this_round {
            pools.observe_no_prune(&touched_keys, &live_state);
        }
    }
}

fn materialize_round_ops(spec: &BlockSpec) -> Vec<RoundOp> {
    if spec.ops.is_empty() {
        return vec![
            RoundOp::ApplyUpdates,
            RoundOp::CheckpointRoot,
            RoundOp::Prune,
            RoundOp::CheckpointRoot,
        ];
    }

    let op_count = MIN_ROUND_OPS + (spec.op_count as usize % (MAX_ROUND_OPS - MIN_ROUND_OPS + 1));
    spec.ops.iter().copied().cycle().take(op_count).collect()
}

fn apply_pending_updates(
    arena: &mut ArenaParallelSparseTrie,
    map_trie: &mut ParallelSparseTrie,
    harness: &mut TrieTestHarness,
    pending_arena: &mut alloy_primitives::map::B256Map<reth_trie_sparse::LeafUpdate>,
    pending_map: &mut alloy_primitives::map::B256Map<reth_trie_sparse::LeafUpdate>,
    round_idx: usize,
    op_idx: usize,
) {
    for _ in 0..MAX_RETRY_ITERS {
        let arena_requests = collect_proof_requests(arena, pending_arena);
        let map_requests = collect_proof_requests(map_trie, pending_map);

        let mut targets = merge_requests(arena_requests, map_requests);
        if targets.is_empty() {
            break;
        }

        let (mut proof_nodes, _) = harness.proof_v2(&mut targets);

        // reveal_nodes mutates the slice, so clone for the second implementation.
        let mut proof_nodes_for_map = proof_nodes.clone();

        arena.reveal_nodes(&mut proof_nodes).expect("arena reveal_nodes should succeed");
        map_trie
            .reveal_nodes(&mut proof_nodes_for_map)
            .expect("map reveal_nodes should succeed");
    }

    assert!(
        pending_arena.is_empty(),
        "arena has pending updates after retry budget (round {round_idx}, op {op_idx})"
    );
    assert!(
        pending_map.is_empty(),
        "map has pending updates after retry budget (round {round_idx}, op {op_idx})"
    );
}

fn checkpoint_roots(
    arena: &mut ArenaParallelSparseTrie,
    map_trie: &mut ParallelSparseTrie,
    harness: &TrieTestHarness,
    round_idx: usize,
    op_idx: Option<usize>,
) {
    let arena_root = arena.root();
    let map_root = map_trie.root();
    let expected_root = harness.original_root();

    let phase = op_idx.map_or_else(|| "end".to_string(), |idx| format!("op {idx}"));
    assert_eq!(
        arena_root, map_root,
        "impl divergence at round {round_idx} ({phase}): arena={arena_root} map={map_root}"
    );
    assert_eq!(
        arena_root, expected_root,
        "oracle mismatch at round {round_idx} ({phase}): arena={arena_root} expected={expected_root}"
    );
}

/// Convert a threshold profile into concrete thresholds for both implementations.
fn materialize_thresholds(
    profile: ThresholdProfile,
) -> (ArenaParallelismThresholds, ParallelismThresholds) {
    let val = match profile {
        ThresholdProfile::Serial256 => 256,
        ThresholdProfile::Low1 => 1,
        ThresholdProfile::Boundary4 => 4,
        ThresholdProfile::Boundary8 => 8,
    };

    let arena = ArenaParallelismThresholds {
        min_dirty_leaves: val as u64,
        min_revealed_nodes: val,
        min_updates: val,
        min_leaves_for_prune: val as u64,
    };

    let map = ParallelismThresholds {
        min_revealed_nodes: val,
        min_updated_nodes: val,
    };

    (arena, map)
}
