use alloy_primitives::B256;
use rand::{Rng, rngs::StdRng, seq::SliceRandom};
use std::collections::{BTreeMap, BTreeSet};

use crate::input::SubtrieMode;

/// Generate a random B256 from an RNG.
fn random_b256(rng: &mut StdRng) -> B256 {
    B256::from(rng.random::<[u8; 32]>())
}

/// Tracks key pools across the multi-block lifecycle.
///
/// In production, the sparse trie is mostly blinded. Only keys that were recently
/// revealed (and not yet pruned) are "hot". After pruning, they become "recently
/// pruned" — re-touching them requires new proofs and exercises the re-reveal path.
#[derive(Debug, Default)]
pub struct KeyPools {
    /// Keys retained after the last prune — still revealed in the trie.
    pub hot: BTreeSet<B256>,
    /// Keys that were revealed in a recent round but pruned back to blinded.
    pub recently_pruned: BTreeSet<B256>,
    /// All keys ever generated (to avoid accidental collisions with "new" keys).
    pub known_ever: BTreeSet<B256>,
}

impl KeyPools {
    /// Initialize from the initial storage state.
    pub fn from_storage(storage: &BTreeMap<B256, alloy_primitives::U256>) -> Self {
        Self {
            hot: BTreeSet::new(),
            recently_pruned: BTreeSet::new(),
            known_ever: storage.keys().copied().collect(),
        }
    }

    /// Update pools after a round with pruning.
    pub fn observe_prune(
        &mut self,
        touched_keys: &BTreeSet<B256>,
        retained: &BTreeSet<B256>,
        live_state: &BTreeMap<B256, alloy_primitives::U256>,
    ) {
        // Recently pruned = keys we touched this round that exist but weren't retained.
        self.recently_pruned =
            touched_keys.iter().filter(|k| live_state.contains_key(*k) && !retained.contains(*k)).copied().collect();
        // Hot = retained keys that still exist.
        self.hot = retained.iter().filter(|k| live_state.contains_key(*k)).copied().collect();
    }

    /// Update pools after a round without pruning.
    pub fn observe_no_prune(
        &mut self,
        touched_keys: &BTreeSet<B256>,
        live_state: &BTreeMap<B256, alloy_primitives::U256>,
    ) {
        // Everything touched and still alive joins the hot set.
        for k in touched_keys {
            if live_state.contains_key(k) {
                self.hot.insert(*k);
            }
        }
        // Nothing newly pruned.
        self.recently_pruned.clear();
    }

    /// Select keys for a block from the available pools.
    ///
    /// Returns `(keys, touched_set)` where `touched_set` includes all selected keys.
    pub fn select_keys(
        &mut self,
        count: usize,
        weights: PoolWeights,
        subtrie_mode: SubtrieMode,
        live_state: &BTreeMap<B256, alloy_primitives::U256>,
        rng: &mut StdRng,
    ) -> Vec<B256> {
        let hot: Vec<B256> = self.hot.iter().filter(|k| live_state.contains_key(*k)).copied().collect();
        let pruned: Vec<B256> = self.recently_pruned.iter().filter(|k| live_state.contains_key(*k)).copied().collect();
        let cold: Vec<B256> = live_state
            .keys()
            .filter(|k| !self.hot.contains(*k) && !self.recently_pruned.contains(*k))
            .copied()
            .collect();

        // Determine which 2-nibble prefixes to target.
        let prefix_filter = choose_prefix_filter(subtrie_mode, &hot, &pruned, &cold, live_state, rng);

        let total_weight =
            weights.hot as u32 + weights.recent_pruned as u32 + weights.cold as u32 + weights.new as u32;

        let mut selected = BTreeSet::new();
        let mut result = Vec::with_capacity(count);

        for _ in 0..count {
            let roll = rng.random_range(0..total_weight);
            let pool_choice = if roll < weights.hot as u32 {
                0
            } else if roll < weights.hot as u32 + weights.recent_pruned as u32 {
                1
            } else if roll < weights.hot as u32 + weights.recent_pruned as u32 + weights.cold as u32 {
                2
            } else {
                3
            };

            let key = match pool_choice {
                0 => pick_from_pool(&hot, &selected, &prefix_filter, rng),
                1 => pick_from_pool(&pruned, &selected, &prefix_filter, rng),
                2 => pick_from_pool(&cold, &selected, &prefix_filter, rng),
                _ => None,
            };

            let key = key.unwrap_or_else(|| {
                // Generate a new key.
                loop {
                    let k = generate_key_with_prefix(&prefix_filter, rng);
                    if !selected.contains(&k) && !live_state.contains_key(&k) {
                        self.known_ever.insert(k);
                        break k;
                    }
                    // If collision (extremely rare), try again.
                }
            });

            selected.insert(key);
            result.push(key);
        }

        result
    }
}

/// Pool selection weights, normalized from raw fuzz input.
#[derive(Debug, Clone, Copy)]
pub struct PoolWeights {
    pub hot: u8,
    pub recent_pruned: u8,
    pub cold: u8,
    pub new: u8,
}

impl PoolWeights {
    pub fn normalize(hot: u8, recent_pruned: u8, cold: u8, new: u8) -> Self {
        Self {
            hot: 1 + (hot % 8),
            recent_pruned: 1 + (recent_pruned % 8),
            cold: 1 + (cold % 8),
            new: 1 + (new % 8),
        }
    }
}

#[derive(Debug, Clone)]
struct PrefixFilter {
    prefixes: Vec<u8>,
    /// If set, do not relax prefix constraints when no matching pool key exists.
    strict: bool,
}

/// Pick a key from a pool that matches the prefix filter and hasn't been selected yet.
fn pick_from_pool(
    pool: &[B256],
    already_selected: &BTreeSet<B256>,
    prefix_filter: &Option<PrefixFilter>,
    rng: &mut StdRng,
) -> Option<B256> {
    if pool.is_empty() {
        return None;
    }

    // Try a few random picks before giving up.
    for _ in 0..8 {
        let candidate = pool[rng.random_range(0..pool.len())];
        if already_selected.contains(&candidate) {
            continue;
        }
        if let Some(prefixes) = prefix_filter {
            if !prefixes.prefixes.contains(&candidate[0]) {
                continue;
            }
        }
        return Some(candidate);
    }

    // Linear scan fallback.
    let mut candidates: Vec<_> = pool
        .iter()
        .filter(|k| {
            !already_selected.contains(*k)
                && prefix_filter.as_ref().is_none_or(|prefixes| {
                    prefixes.prefixes.contains(&k[0])
                })
        })
        .copied()
        .collect();

    if candidates.is_empty() && !prefix_filter.as_ref().is_some_and(|f| f.strict) {
        // Relax prefix filter.
        candidates = pool.iter().filter(|k| !already_selected.contains(*k)).copied().collect();
    }

    if candidates.is_empty() {
        return None;
    }

    candidates.shuffle(rng);
    Some(candidates[0])
}

/// Generate a new random key, optionally constrained to a prefix.
fn generate_key_with_prefix(prefix_filter: &Option<PrefixFilter>, rng: &mut StdRng) -> B256 {
    let mut key = random_b256(rng);
    if let Some(prefixes) = prefix_filter {
        if !prefixes.prefixes.is_empty() {
            let chosen = prefixes.prefixes[rng.random_range(0..prefixes.prefixes.len())];
            key[0] = chosen;
        }
    }
    key
}

/// Choose which 2-nibble prefixes (first byte of B256) to target based on SubtrieMode.
fn choose_prefix_filter(
    mode: SubtrieMode,
    hot: &[B256],
    pruned: &[B256],
    cold: &[B256],
    live_state: &BTreeMap<B256, alloy_primitives::U256>,
    rng: &mut StdRng,
) -> Option<PrefixFilter> {
    if live_state.is_empty() {
        return None;
    }

    // Collect all occupied first-bytes (= 2-nibble prefixes = subtrie indices).
    let mut occupied: Vec<u8> = live_state.keys().map(|k| k[0]).collect::<BTreeSet<_>>().into_iter().collect();
    if occupied.is_empty() {
        return None;
    }
    occupied.shuffle(rng);

    match mode {
        SubtrieMode::SinglePrefix => Some(PrefixFilter { prefixes: vec![occupied[0]], strict: false }),
        SubtrieMode::StickyPrefix => {
            let sticky_prefix = dominant_prefix(pruned)
                .or_else(|| dominant_prefix(hot))
                .or_else(|| dominant_prefix(cold))
                .unwrap_or(occupied[0]);
            Some(PrefixFilter { prefixes: vec![sticky_prefix], strict: true })
        }
        SubtrieMode::TwoPrefixes => {
            let n = occupied.len().min(2);
            Some(PrefixFilter { prefixes: occupied[..n].to_vec(), strict: false })
        }
        SubtrieMode::ManyPrefixes => {
            let n = occupied.len().min(rng.random_range(4..=8).min(occupied.len()));
            Some(PrefixFilter { prefixes: occupied[..n].to_vec(), strict: false })
        }
        SubtrieMode::Scattered => None,
    }
}

fn dominant_prefix(keys: &[B256]) -> Option<u8> {
    if keys.is_empty() {
        return None;
    }

    let mut counts = BTreeMap::<u8, usize>::new();
    for key in keys {
        *counts.entry(key[0]).or_default() += 1;
    }

    counts.into_iter().max_by_key(|(_, count)| *count).map(|(prefix, _)| prefix)
}
