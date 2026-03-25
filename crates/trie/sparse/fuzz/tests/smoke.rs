use reth_trie_sparse_fuzz::driver::run;
use reth_trie_sparse_fuzz::input::*;

#[test]
fn smoke_low1_two_prefixes() {
    let input = FuzzInput {
        profile: ThresholdProfile::Low1,
        initial: InitialStateSpec {
            key_seed: 42,
            value_seed: 99,
            key_count: 0,
            size_mode: InitialSizeMode::Large,
            layout: InitialLayout::Uniform,
        },
        round_count: 0,
        rounds: vec![BlockSpec {
            subtrie_mode: SubtrieMode::TwoPrefixes,
            touch_count: 0,
            retain_count: 2,
            delete_pct: 10,
            touched_pct: 5,
            hot_weight: 3,
            recent_pruned_weight: 3,
            cold_weight: 3,
            new_weight: 3,
            key_seed: 123,
            value_seed: 456,
            op_count: 4,
            ops: vec![
                RoundOp::ApplyUpdates,
                RoundOp::CheckpointRoot,
                RoundOp::Prune,
                RoundOp::CheckpointRoot,
            ],
        }],
    };
    run(input);
}

#[test]
fn smoke_serial256_scattered() {
    let input = FuzzInput {
        profile: ThresholdProfile::Serial256,
        initial: InitialStateSpec {
            key_seed: 7,
            value_seed: 13,
            key_count: 100,
            size_mode: InitialSizeMode::Large,
            layout: InitialLayout::Clustered,
        },
        round_count: 5,
        rounds: vec![BlockSpec {
            subtrie_mode: SubtrieMode::Scattered,
            touch_count: 10,
            retain_count: 4,
            delete_pct: 20,
            touched_pct: 10,
            hot_weight: 5,
            recent_pruned_weight: 5,
            cold_weight: 2,
            new_weight: 2,
            key_seed: 999,
            value_seed: 888,
            op_count: 4,
            ops: vec![
                RoundOp::ApplyUpdates,
                RoundOp::CheckpointRoot,
                RoundOp::Prune,
                RoundOp::CheckpointRoot,
            ],
        }],
    };
    run(input);
}

#[test]
fn smoke_boundary4_single_prefix() {
    let input = FuzzInput {
        profile: ThresholdProfile::Boundary4,
        initial: InitialStateSpec {
            key_seed: 55,
            value_seed: 66,
            key_count: 200,
            size_mode: InitialSizeMode::Mixed,
            layout: InitialLayout::Mixed,
        },
        round_count: 3,
        rounds: vec![BlockSpec {
            subtrie_mode: SubtrieMode::SinglePrefix,
            touch_count: 15,
            retain_count: 0,
            delete_pct: 30,
            touched_pct: 0,
            hot_weight: 1,
            recent_pruned_weight: 1,
            cold_weight: 4,
            new_weight: 4,
            key_seed: 111,
            value_seed: 222,
            op_count: 4,
            ops: vec![
                RoundOp::ApplyUpdates,
                RoundOp::CheckpointRoot,
                RoundOp::Prune,
                RoundOp::CheckpointRoot,
            ],
        }],
    };
    run(input);
}

#[test]
fn smoke_small_sticky_with_mid_round_checkpoint() {
    let input = FuzzInput {
        profile: ThresholdProfile::Low1,
        initial: InitialStateSpec {
            key_seed: 1337,
            value_seed: 4242,
            key_count: 17,
            size_mode: InitialSizeMode::Small,
            layout: InitialLayout::Clustered,
        },
        round_count: 2,
        rounds: vec![BlockSpec {
            subtrie_mode: SubtrieMode::StickyPrefix,
            touch_count: 21,
            retain_count: 2,
            delete_pct: 25,
            touched_pct: 10,
            hot_weight: 6,
            recent_pruned_weight: 6,
            cold_weight: 1,
            new_weight: 2,
            key_seed: 987,
            value_seed: 654,
            op_count: 3,
            ops: vec![
                RoundOp::CheckpointRoot,
                RoundOp::ApplyUpdates,
                RoundOp::CheckpointRoot,
            ],
        }],
    };
    run(input);
}
