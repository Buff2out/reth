use alloy_primitives::{B256, U256, map::B256Map};
use rand::{Rng, SeedableRng, rngs::StdRng};
use reth_trie_common::ProofV2Target;
use reth_trie_sparse::{LeafUpdate, SparseTrie};
use std::collections::{BTreeMap, BTreeSet};

/// Generate a random B256 from an RNG.
fn random_b256(rng: &mut StdRng) -> B256 {
    B256::from(rng.random::<[u8; 32]>())
}

use crate::input::{BlockSpec, InitialLayout, InitialSizeMode, InitialStateSpec};
use crate::pools::{KeyPools, PoolWeights};

/// Build the initial storage map from the fuzz input spec.
pub fn build_initial_storage(spec: &InitialStateSpec) -> BTreeMap<B256, U256> {
    let count = normalize_initial_count(spec);
    let mut key_rng = StdRng::seed_from_u64(spec.key_seed);
    let mut val_rng = StdRng::seed_from_u64(spec.value_seed);

    let mut storage = BTreeMap::new();

    match spec.layout {
        InitialLayout::Uniform => {
            for _ in 0..count {
                let key = random_b256(&mut key_rng);
                let value = U256::from(val_rng.random::<u64>() | 1);
                storage.insert(key, value);
            }
        }
        InitialLayout::Clustered => {
            // Pick 4-8 prefix bytes, cluster keys under them.
            let num_clusters = key_rng.random_range(4..=8u8);
            let prefixes: Vec<u8> = (0..num_clusters).map(|_| key_rng.random::<u8>()).collect();
            for _ in 0..count {
                let mut key = random_b256(&mut key_rng);
                key[0] = prefixes[key_rng.random_range(0..prefixes.len())];
                let value = U256::from(val_rng.random::<u64>() | 1);
                storage.insert(key, value);
            }
        }
        InitialLayout::Mixed => {
            let num_clusters = key_rng.random_range(2..=4u8);
            let prefixes: Vec<u8> = (0..num_clusters).map(|_| key_rng.random::<u8>()).collect();
            for i in 0..count {
                let mut key = random_b256(&mut key_rng);
                // Half clustered, half uniform.
                if i % 2 == 0 {
                    key[0] = prefixes[key_rng.random_range(0..prefixes.len())];
                }
                let value = U256::from(val_rng.random::<u64>() | 1);
                storage.insert(key, value);
            }
        }
    }

    storage
}

fn normalize_initial_count(spec: &InitialStateSpec) -> usize {
    let large = 500 + (spec.key_count % 1501) as usize;
    let small = 8 + (spec.key_count % 57) as usize;

    match spec.size_mode {
        InitialSizeMode::Large => large,
        InitialSizeMode::Small => small,
        InitialSizeMode::Mixed => {
            if spec.key_seed & 1 == 0 {
                small
            } else {
                large
            }
        }
    }
}

/// A realized block: the leaf updates to apply and the model changeset.
pub struct RealizedBlock {
    /// Updates to feed into `update_leaves`.
    pub leaf_updates: B256Map<LeafUpdate>,
    /// State changes for the model oracle (zero = delete). Does NOT include Touched.
    pub changeset: BTreeMap<B256, U256>,
    /// All keys touched this round (including Touched-only).
    pub touched_keys: BTreeSet<B256>,
}

/// Build a block's updates from a spec and the current live state.
pub fn realize_block(
    spec: &BlockSpec,
    live_state: &BTreeMap<B256, U256>,
    pools: &mut KeyPools,
) -> RealizedBlock {
    let touch_count = 5 + (spec.touch_count % 46) as usize;
    let delete_pct = (spec.delete_pct % 41) as usize;
    let touched_pct = (spec.touched_pct % 21) as usize;

    let mut rng = StdRng::seed_from_u64(spec.key_seed);
    let mut val_rng = StdRng::seed_from_u64(spec.value_seed);

    let weights = PoolWeights::normalize(
        spec.hot_weight,
        spec.recent_pruned_weight,
        spec.cold_weight,
        spec.new_weight,
    );

    let keys = pools.select_keys(touch_count, weights, spec.subtrie_mode, live_state, &mut rng);

    let mut leaf_updates = B256Map::default();
    let mut changeset = BTreeMap::new();
    let mut touched_keys = BTreeSet::new();

    for (i, key) in keys.into_iter().enumerate() {
        touched_keys.insert(key);

        let pct_position = (i * 100) / touch_count.max(1);

        if pct_position < touched_pct {
            // LeafUpdate::Touched — no state change.
            leaf_updates.insert(key, LeafUpdate::Touched);
        } else if pct_position < touched_pct + delete_pct && live_state.contains_key(&key) {
            // Delete an existing key.
            leaf_updates.insert(key, LeafUpdate::Changed(Vec::new()));
            changeset.insert(key, U256::ZERO);
        } else {
            // Insert or update with a non-zero value.
            let value = U256::from(val_rng.random::<u64>() | 1);
            let encoded = alloy_rlp::encode_fixed_size(&value).to_vec();
            leaf_updates.insert(key, LeafUpdate::Changed(encoded));
            changeset.insert(key, value);
        }
    }

    RealizedBlock { leaf_updates, changeset, touched_keys }
}

/// Apply changeset to the live state model.
pub fn apply_changeset_to_live_state(
    live_state: &mut BTreeMap<B256, U256>,
    changeset: &BTreeMap<B256, U256>,
) {
    for (&k, &v) in changeset {
        if v == U256::ZERO {
            live_state.remove(&k);
        } else {
            live_state.insert(k, v);
        }
    }
}

/// Choose which keys to retain after pruning.
pub fn choose_retained_keys(
    spec: &BlockSpec,
    touched_keys: &BTreeSet<B256>,
    live_state: &BTreeMap<B256, U256>,
    rng: &mut StdRng,
) -> BTreeSet<B256> {
    let retain_count = (spec.retain_count % 17) as usize;

    // Prefer retaining keys we just touched that still exist.
    let mut candidates: Vec<B256> =
        touched_keys.iter().filter(|k| live_state.contains_key(*k)).copied().collect();

    // If we don't have enough, add some random live keys.
    if candidates.len() < retain_count {
        let extra: Vec<B256> = live_state
            .keys()
            .filter(|k| !touched_keys.contains(*k))
            .take(retain_count)
            .copied()
            .collect();
        candidates.extend(extra);
    }

    use rand::seq::SliceRandom;
    candidates.shuffle(rng);
    candidates.truncate(retain_count);
    candidates.into_iter().collect()
}

/// Run the reveal-update retry loop for a single SparseTrie implementation.
/// Returns the set of proof targets requested (keys).
pub fn collect_proof_requests<T: SparseTrie>(
    trie: &mut T,
    pending: &mut B256Map<LeafUpdate>,
) -> Vec<(B256, u8)> {
    let mut requests = Vec::new();
    trie.update_leaves(pending, |key, min_len| {
        requests.push((key, min_len));
    })
    .expect("update_leaves should succeed");
    requests
}

/// Merge proof requests from both implementations, keeping the smaller min_len per key.
pub fn merge_requests(a: Vec<(B256, u8)>, b: Vec<(B256, u8)>) -> Vec<ProofV2Target> {
    let mut min_len_by_key = B256Map::<u8>::default();

    for (key, min_len) in a.into_iter().chain(b) {
        min_len_by_key.entry(key).and_modify(|v| *v = (*v).min(min_len)).or_insert(min_len);
    }

    min_len_by_key
        .into_iter()
        .map(|(key, min_len)| ProofV2Target::new(key).with_min_len(min_len))
        .collect()
}
