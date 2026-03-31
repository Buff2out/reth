//! Preserved state-root assets for reuse across payload validations.

use super::multiproof::MultiProofTaskMetrics;
use alloy_primitives::B256;
use parking_lot::Mutex;
use reth_primitives_traits::{dashmap::DashMap, FastInstant as Instant};
use reth_trie_sparse::{ConfigurableSparseTrie, SparseStateTrie};
use std::sync::Arc;
use tracing::debug;

/// Type alias for the sparse trie type used in preservation.
pub(super) type SparseTrie = SparseStateTrie<ConfigurableSparseTrie, ConfigurableSparseTrie>;

/// Shared storage-root cache type used by proof workers and preserved across continuation blocks.
pub(super) type StorageRootCache = Arc<DashMap<B256, B256>>;

/// Shared handle to preserved state-root assets that can be reused across payload validations.
///
/// This is stored in [`PayloadProcessor`](super::PayloadProcessor) and cloned to pass to
/// [`SparseTrieCacheTask`](super::sparse_trie::SparseTrieCacheTask) for reuse across continuation
/// payloads.
#[derive(Debug, Default, Clone)]
pub(super) struct SharedPreservedStateRootAssets(Arc<Mutex<Option<PreservedStateRootAssets>>>);

impl SharedPreservedStateRootAssets {
    /// Takes the preserved assets if present, leaving `None` in its place.
    pub(super) fn take(&self) -> Option<PreservedStateRootAssets> {
        self.0.lock().take()
    }

    /// Acquires a guard that blocks `take()` until dropped.
    /// Use this before sending the state root result to ensure the next block waits for the assets
    /// to be stored.
    pub(super) fn lock(&self) -> PreservedStateRootAssetsGuard<'_> {
        PreservedStateRootAssetsGuard(self.0.lock())
    }

    /// Waits until the preserved assets lock becomes available.
    ///
    /// This acquires and immediately releases the lock, ensuring that any
    /// ongoing operations complete before returning. Useful for synchronization
    /// before starting payload processing.
    ///
    /// Returns the time spent waiting for the lock.
    pub(super) fn wait_for_availability(&self) -> std::time::Duration {
        let start = Instant::now();
        let _guard = self.0.lock();
        let elapsed = start.elapsed();
        if elapsed.as_millis() > 5 {
            debug!(
                target: "engine::tree::payload_processor",
                blocked_for=?elapsed,
                "Waited for preserved state-root assets to become available"
            );
        }
        elapsed
    }
}

/// Guard that holds the lock on the preserved state-root assets.
/// While held, `take()` will block. Call `store()` to save the assets before dropping.
pub(super) struct PreservedStateRootAssetsGuard<'a>(
    parking_lot::MutexGuard<'a, Option<PreservedStateRootAssets>>,
);

impl PreservedStateRootAssetsGuard<'_> {
    /// Stores preserved assets for later reuse.
    pub(super) fn store(&mut self, assets: PreservedStateRootAssets) {
        self.0.replace(assets);
    }
}

/// Preserved state-root assets that can be reused across payload validations.
///
/// The assets exist in one of two states:
/// - **Anchored**: Have a computed state root and can be reused for payloads whose parent state
///   root matches the anchor.
/// - **Cleared**: Asset data has been cleared but allocations are preserved for reuse.
#[derive(Debug)]
pub(super) enum PreservedStateRootAssets {
    /// Assets with a computed state root that can be reused for continuation payloads.
    Anchored {
        /// The sparse state trie (pruned after root computation).
        trie: SparseTrie,
        /// Shared storage-root cache reused by proof workers.
        storage_root_cache: StorageRootCache,
        /// The state root this trie represents (computed from the previous block).
        /// Used to verify continuity: new payload's `parent_state_root` must match this.
        state_root: B256,
    },
    /// Cleared assets with preserved allocations, ready for fresh use.
    Cleared {
        /// The sparse state trie with cleared data but preserved allocations.
        trie: SparseTrie,
        /// The cleared shared storage-root cache.
        storage_root_cache: StorageRootCache,
    },
}

impl PreservedStateRootAssets {
    /// Creates new anchored preserved assets.
    ///
    /// The `state_root` is the computed state root from the trie, which becomes the
    /// anchor for determining if subsequent payloads can reuse these assets.
    pub(super) const fn anchored(
        trie: SparseTrie,
        storage_root_cache: StorageRootCache,
        state_root: B256,
    ) -> Self {
        Self::Anchored { trie, storage_root_cache, state_root }
    }

    /// Creates cleared preserved assets (allocations preserved, data cleared).
    pub(super) const fn cleared(trie: SparseTrie, storage_root_cache: StorageRootCache) -> Self {
        Self::Cleared { trie, storage_root_cache }
    }

    /// Consumes self and returns the assets for reuse.
    ///
    /// If the preserved assets are anchored and the parent state root matches, the pruned trie and
    /// cached storage roots are reused directly. Otherwise, both are cleared but allocations are
    /// preserved to reduce memory overhead.
    pub(super) fn into_parts_for(
        self,
        parent_state_root: B256,
        metrics: &MultiProofTaskMetrics,
    ) -> (SparseTrie, StorageRootCache) {
        match self {
            Self::Anchored { trie, storage_root_cache, state_root }
                if state_root == parent_state_root =>
            {
                metrics.state_root_assets_anchor_reuse_count.increment(1);
                debug!(
                    target: "engine::tree::payload_processor",
                    %state_root,
                    "Reusing anchored state-root assets for continuation payload"
                );
                (trie, storage_root_cache)
            }
            Self::Anchored { mut trie, storage_root_cache, state_root } => {
                metrics.state_root_assets_anchor_mismatch_count.increment(1);
                debug!(
                    target: "engine::tree::payload_processor",
                    anchor_root = %state_root,
                    %parent_state_root,
                    "Clearing anchored state-root assets - parent state root mismatch"
                );
                trie.clear();
                storage_root_cache.clear();
                (trie, storage_root_cache)
            }
            Self::Cleared { trie, storage_root_cache } => {
                debug!(
                    target: "engine::tree::payload_processor",
                    %parent_state_root,
                    "Using cleared state-root assets with preserved allocations"
                );
                (trie, storage_root_cache)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_trie_sparse::{ArenaParallelSparseTrie, RevealableSparseTrie};

    fn new_sparse_trie() -> SparseTrie {
        let default_trie = RevealableSparseTrie::blind_from(ConfigurableSparseTrie::Arena(
            ArenaParallelSparseTrie::default(),
        ));
        SparseStateTrie::default()
            .with_accounts_trie(default_trie.clone())
            .with_default_storage_trie(default_trie)
            .with_updates(true)
    }

    #[test]
    fn matching_anchor_reuses_storage_root_cache() {
        let state_root = B256::with_last_byte(1);
        let account = B256::with_last_byte(2);
        let root = B256::with_last_byte(3);
        let storage_root_cache = StorageRootCache::default();
        storage_root_cache.insert(account, root);

        let assets = PreservedStateRootAssets::anchored(
            new_sparse_trie(),
            storage_root_cache.clone(),
            state_root,
        );

        let (_, reused_cache) =
            assets.into_parts_for(state_root, &MultiProofTaskMetrics::default());
        assert!(Arc::ptr_eq(&reused_cache, &storage_root_cache));
        assert_eq!(reused_cache.get(&account).as_deref().copied(), Some(root));
    }

    #[test]
    fn mismatched_anchor_clears_storage_root_cache() {
        let storage_root_cache = StorageRootCache::default();
        storage_root_cache.insert(B256::with_last_byte(2), B256::with_last_byte(3));

        let assets = PreservedStateRootAssets::anchored(
            new_sparse_trie(),
            storage_root_cache.clone(),
            B256::with_last_byte(1),
        );

        let (_, reused_cache) =
            assets.into_parts_for(B256::with_last_byte(9), &MultiProofTaskMetrics::default());
        assert!(Arc::ptr_eq(&reused_cache, &storage_root_cache));
        assert!(reused_cache.is_empty());
    }
}
