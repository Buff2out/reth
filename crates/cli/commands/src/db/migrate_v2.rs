//! `reth db migrate-v2` command for migrating v1 storage layout to v2.
//!
//! Migrates data from MDBX-only (v1) storage layout to the hybrid v2 layout:
//! - TransactionSenders → static files
//! - AccountChangeSets → static files
//! - StorageChangeSets → static files
//! - Receipts → static files (if not already there)
//! - TransactionHashNumbers → RocksDB
//! - AccountsHistory → RocksDB
//! - StoragesHistory → RocksDB
//!
//! Then updates `StorageSettings` to v2.

use clap::Parser;
use reth_db::{
    mdbx::{self, ffi},
    models::StorageBeforeTx,
};
use reth_db_api::{
    cursor::DbCursorRO,
    database::Database,
    table::Table,
    tables,
    transaction::{DbTx, DbTxMut},
};
use reth_db_common::DbTool;
use reth_provider::{
    providers::ProviderNodeTypes, DBProvider, DatabaseProviderFactory, MetadataProvider,
    MetadataWriter, RocksDBProviderFactory, StaticFileProviderFactory, StaticFileWriter,
    StorageSettings,
};
use reth_stages_types::StageId;
use reth_static_file_types::StaticFileSegment;
use reth_storage_api::StageCheckpointReader;
use std::path::PathBuf;
use tracing::info;

/// `reth db migrate-v2` command
#[derive(Debug, Parser)]
pub struct Command {
    /// Prune migrated data from MDBX tables after successful migration.
    #[arg(long)]
    prune_mdbx: bool,
}

impl Command {
    /// Execute the migration.
    ///
    /// Returns `true` if MDBX tables were pruned and the database should be
    /// compacted. The caller should run [`Self::compact_mdbx`] and
    /// [`Self::swap_compacted_db`] after dropping the database handle.
    pub fn execute<N: ProviderNodeTypes>(self, tool: &DbTool<N>) -> eyre::Result<bool>
    where
        N::Primitives: reth_primitives_traits::NodePrimitives<
            Receipt: reth_db_api::table::Value + reth_codecs::Compact,
        >,
    {
        // === Phase 0: Preflight ===
        info!(target: "reth::cli", "Starting v1 → v2 storage migration");

        let provider = tool.provider_factory.provider()?;
        let current_settings = provider.storage_settings()?;

        if current_settings.is_some_and(|s| s.is_v2()) {
            info!(target: "reth::cli", "Storage is already v2, nothing to do");
            return Ok(false);
        }

        let tip =
            provider.get_stage_checkpoint(StageId::Execution)?.map(|c| c.block_number).unwrap_or(0);

        info!(target: "reth::cli", tip, "Chain tip block number");

        let sf_provider = tool.provider_factory.static_file_provider();

        // Check that target static file segments are empty
        for segment in [
            StaticFileSegment::TransactionSenders,
            StaticFileSegment::AccountChangeSets,
            StaticFileSegment::StorageChangeSets,
        ] {
            if sf_provider.get_highest_static_file_block(segment).is_some() {
                eyre::bail!(
                    "Static file segment {segment:?} already contains data. \
                     Cannot migrate — target must be empty."
                );
            }
        }

        // Check that RocksDB tables are empty
        let rocksdb = tool.provider_factory.rocksdb_provider();
        if rocksdb.first::<tables::TransactionHashNumbers>()?.is_some() {
            eyre::bail!("RocksDB TransactionHashNumbers already contains data");
        }
        if rocksdb.first::<tables::AccountsHistory>()?.is_some() {
            eyre::bail!("RocksDB AccountsHistory already contains data");
        }
        if rocksdb.first::<tables::StoragesHistory>()?.is_some() {
            eyre::bail!("RocksDB StoragesHistory already contains data");
        }

        drop(provider);
        info!(target: "reth::cli", "Preflight checks passed");

        // === Phase 1: TransactionSenders → static files ===
        self.migrate_transaction_senders(tool, tip)?;

        // === Phase 2: AccountChangeSets → static files ===
        self.migrate_account_changesets(tool, tip)?;

        // === Phase 3: StorageChangeSets → static files ===
        self.migrate_storage_changesets(tool, tip)?;

        // === Phase 4: Receipts → static files ===
        self.migrate_receipts::<N>(tool, tip)?;

        // === Phase 5: TransactionHashNumbers → RocksDB ===
        self.migrate_transaction_hash_numbers(tool)?;

        // === Phase 6: AccountsHistory → RocksDB ===
        self.migrate_accounts_history(tool)?;

        // === Phase 7: StoragesHistory → RocksDB ===
        self.migrate_storages_history(tool)?;

        // === Phase 8: Verify hashed state ===
        self.verify_hashed_state(tool, tip)?;

        // === Phase 9: Update metadata to v2 ===
        info!(target: "reth::cli", "Writing StorageSettings v2 metadata");
        let provider_rw = tool.provider_factory.database_provider_rw()?;
        provider_rw.write_storage_settings(StorageSettings::v2())?;
        provider_rw.commit()?;
        info!(target: "reth::cli", "Storage settings updated to v2");

        // === Phase 10: Optional MDBX pruning ===
        if self.prune_mdbx {
            self.prune_migrated_tables(tool)?;
        }

        info!(target: "reth::cli", "Migration complete!");
        Ok(self.prune_mdbx)
    }

    /// Swaps the original MDBX database with a compacted copy.
    ///
    /// Must be called after the database handle has been dropped.
    pub fn swap_compacted_db(
        db_path: &std::path::Path,
        compact_path: &std::path::Path,
    ) -> eyre::Result<()> {
        let backup_path = db_path.with_file_name("db_pre_compact");

        info!(target: "reth::cli", ?db_path, ?compact_path, "Swapping compacted database");

        // Rename original → backup
        std::fs::rename(db_path, &backup_path)?;

        // Rename compacted → original
        if let Err(e) = std::fs::rename(compact_path, db_path) {
            // Restore backup on failure
            let _ = std::fs::rename(&backup_path, db_path);
            return Err(e.into());
        }

        // Remove backup
        std::fs::remove_dir_all(&backup_path)?;

        info!(target: "reth::cli", "Database compaction swap complete");
        Ok(())
    }

    fn migrate_transaction_senders<N: ProviderNodeTypes>(
        &self,
        tool: &DbTool<N>,
        tip: u64,
    ) -> eyre::Result<()> {
        info!(target: "reth::cli", "Migrating TransactionSenders → static files");
        let provider = tool.provider_factory.provider()?;
        let sf_provider = tool.provider_factory.static_file_provider();
        let mut writer = sf_provider.latest_writer(StaticFileSegment::TransactionSenders)?;

        let mut sender_cursor = provider.tx_ref().cursor_read::<tables::TransactionSenders>()?;
        let mut block_cursor = provider.tx_ref().cursor_read::<tables::BlockBodyIndices>()?;

        let mut count = 0u64;
        let block_walker = block_cursor.walk(Some(0))?;
        for result in block_walker {
            let (block_number, body_indices) = result?;
            if block_number > tip {
                break;
            }
            writer.increment_block(block_number)?;

            let tx_range = body_indices.tx_num_range();
            if tx_range.is_empty() {
                continue;
            }

            let senders_walker = sender_cursor.walk_range(tx_range)?;
            for entry in senders_walker {
                let (tx_num, sender) = entry?;
                writer.append_transaction_sender(tx_num, &sender)?;
                count += 1;
            }
        }

        writer.commit()?;
        drop(provider);

        info!(target: "reth::cli", count, "TransactionSenders migrated");
        Ok(())
    }

    fn migrate_account_changesets<N: ProviderNodeTypes>(
        &self,
        tool: &DbTool<N>,
        tip: u64,
    ) -> eyre::Result<()> {
        info!(target: "reth::cli", "Migrating AccountChangeSets → static files");
        let provider = tool.provider_factory.provider()?;
        let sf_provider = tool.provider_factory.static_file_provider();
        let mut writer = sf_provider.latest_writer(StaticFileSegment::AccountChangeSets)?;

        let mut cursor = provider.tx_ref().cursor_read::<tables::AccountChangeSets>()?;

        let mut count = 0u64;
        // Use a peekable walker so we can look ahead without consuming
        let mut walker = cursor.walk(Some(0))?.peekable();

        // Iterate ALL blocks from 0..=tip, appending empty changesets for blocks with no entries
        for block in 0..=tip {
            let mut entries = Vec::new();

            // Collect all entries for this block
            while let Some(Ok((block_number, _))) = walker.peek() {
                if *block_number != block {
                    break;
                }
                let (_, entry) = walker.next().expect("peeked")?;
                entries.push(entry);
            }

            count += entries.len() as u64;
            writer.append_account_changeset(entries, block)?;
        }

        writer.commit()?;
        drop(provider);

        info!(target: "reth::cli", count, "AccountChangeSets migrated");
        Ok(())
    }

    fn migrate_storage_changesets<N: ProviderNodeTypes>(
        &self,
        tool: &DbTool<N>,
        tip: u64,
    ) -> eyre::Result<()> {
        info!(target: "reth::cli", "Migrating StorageChangeSets → static files");
        let provider = tool.provider_factory.provider()?;
        let sf_provider = tool.provider_factory.static_file_provider();
        let mut writer = sf_provider.latest_writer(StaticFileSegment::StorageChangeSets)?;

        let mut cursor = provider.tx_ref().cursor_read::<tables::StorageChangeSets>()?;

        let mut count = 0u64;
        let mut walker = cursor.walk(Some(Default::default()))?.peekable();

        // Iterate ALL blocks from 0..=tip, appending empty changesets for blocks with no entries
        for block in 0..=tip {
            let mut entries = Vec::new();

            // Collect all entries for this block
            while let Some(Ok((key, _))) = walker.peek() {
                if key.block_number() != block {
                    break;
                }
                let (key, entry) = walker.next().expect("peeked")?;
                entries.push(StorageBeforeTx {
                    address: key.address(),
                    key: entry.key,
                    value: entry.value,
                });
            }

            count += entries.len() as u64;
            writer.append_storage_changeset(entries, block)?;
        }

        writer.commit()?;
        drop(provider);

        info!(target: "reth::cli", count, "StorageChangeSets migrated");
        Ok(())
    }

    fn migrate_receipts<N: ProviderNodeTypes>(&self, tool: &DbTool<N>, tip: u64) -> eyre::Result<()>
    where
        N::Primitives: reth_primitives_traits::NodePrimitives<
            Receipt: reth_db_api::table::Value + reth_codecs::Compact,
        >,
    {
        let sf_provider = tool.provider_factory.static_file_provider();
        let existing = sf_provider.get_highest_static_file_block(StaticFileSegment::Receipts);

        if existing.is_some_and(|b| b >= tip) {
            info!(target: "reth::cli", "Receipts already in static files, skipping");
            return Ok(());
        }

        info!(target: "reth::cli", "Migrating Receipts → static files");

        let start_block = existing.map_or(0, |b| b + 1);
        let block_range = start_block..=tip;

        // Use existing Segment implementation for receipts
        let provider = tool.provider_factory.provider()?.disable_long_read_transaction_safety();

        let segment = reth_static_file::segments::Receipts;
        reth_static_file::segments::Segment::copy_to_static_files(&segment, provider, block_range)?;

        sf_provider.commit()?;

        info!(target: "reth::cli", "Receipts migrated");
        Ok(())
    }

    fn migrate_transaction_hash_numbers<N: ProviderNodeTypes>(
        &self,
        tool: &DbTool<N>,
    ) -> eyre::Result<()> {
        info!(target: "reth::cli", "Migrating TransactionHashNumbers → RocksDB");
        let provider = tool.provider_factory.provider()?;
        let rocksdb = tool.provider_factory.rocksdb_provider();

        let mut cursor = provider.tx_ref().cursor_read::<tables::TransactionHashNumbers>()?;
        let mut batch = rocksdb.batch_with_auto_commit();

        let mut count = 0u64;
        let walker = cursor.walk(None)?;
        for result in walker {
            let (key, value) = result?;
            batch.put::<tables::TransactionHashNumbers>(key, &value)?;
            count += 1;
            if count.is_multiple_of(1_000_000) {
                info!(target: "reth::cli", count, "TransactionHashNumbers progress");
            }
        }

        batch.commit()?;
        drop(provider);

        info!(target: "reth::cli", count, "TransactionHashNumbers migrated");
        Ok(())
    }

    fn migrate_accounts_history<N: ProviderNodeTypes>(&self, tool: &DbTool<N>) -> eyre::Result<()> {
        info!(target: "reth::cli", "Migrating AccountsHistory → RocksDB");
        let provider = tool.provider_factory.provider()?;
        let rocksdb = tool.provider_factory.rocksdb_provider();

        let mut cursor = provider.tx_ref().cursor_read::<tables::AccountsHistory>()?;
        let mut batch = rocksdb.batch_with_auto_commit();

        let mut count = 0u64;
        let walker = cursor.walk(None)?;
        for result in walker {
            let (key, value) = result?;
            batch.put::<tables::AccountsHistory>(key, &value)?;
            count += 1;
            if count.is_multiple_of(1_000_000) {
                info!(target: "reth::cli", count, "AccountsHistory progress");
            }
        }

        batch.commit()?;
        drop(provider);

        info!(target: "reth::cli", count, "AccountsHistory migrated");
        Ok(())
    }

    fn migrate_storages_history<N: ProviderNodeTypes>(&self, tool: &DbTool<N>) -> eyre::Result<()> {
        info!(target: "reth::cli", "Migrating StoragesHistory → RocksDB");
        let provider = tool.provider_factory.provider()?;
        let rocksdb = tool.provider_factory.rocksdb_provider();

        let mut cursor = provider.tx_ref().cursor_read::<tables::StoragesHistory>()?;
        let mut batch = rocksdb.batch_with_auto_commit();

        let mut count = 0u64;
        let walker = cursor.walk(None)?;
        for result in walker {
            let (key, value) = result?;
            batch.put::<tables::StoragesHistory>(key, &value)?;
            count += 1;
            if count.is_multiple_of(1_000_000) {
                info!(target: "reth::cli", count, "StoragesHistory progress");
            }
        }

        batch.commit()?;
        drop(provider);

        info!(target: "reth::cli", count, "StoragesHistory migrated");
        Ok(())
    }

    fn verify_hashed_state<N: ProviderNodeTypes>(
        &self,
        tool: &DbTool<N>,
        tip: u64,
    ) -> eyre::Result<()> {
        if tip == 0 {
            info!(target: "reth::cli", "Empty chain, skipping hashed state verification");
            return Ok(());
        }

        info!(target: "reth::cli", "Verifying HashedAccounts/HashedStorages are populated");
        let provider = tool.provider_factory.provider()?;

        // Check AccountHashing
        let account_hashing = provider
            .get_stage_checkpoint(StageId::AccountHashing)?
            .map(|c| c.block_number)
            .unwrap_or(0);

        eyre::ensure!(
            account_hashing >= tip,
            "AccountHashing stage checkpoint ({account_hashing}) is behind execution tip ({tip}). \
             HashedAccounts may not be fully populated."
        );

        // Check StorageHashing
        let storage_hashing = provider
            .get_stage_checkpoint(StageId::StorageHashing)?
            .map(|c| c.block_number)
            .unwrap_or(0);

        eyre::ensure!(
            storage_hashing >= tip,
            "StorageHashing stage checkpoint ({storage_hashing}) is behind execution tip ({tip}). \
             HashedStorages may not be fully populated."
        );

        // Spot-check that HashedAccounts has at least one entry
        let mut cursor = provider.tx_ref().cursor_read::<tables::HashedAccounts>()?;
        eyre::ensure!(
            cursor.first()?.is_some(),
            "HashedAccounts table is empty but chain has state."
        );

        drop(provider);
        info!(target: "reth::cli", "Hashed state verification passed");
        Ok(())
    }

    fn prune_migrated_tables<N: ProviderNodeTypes>(&self, tool: &DbTool<N>) -> eyre::Result<()> {
        info!(target: "reth::cli", "Pruning migrated MDBX tables");
        let db = tool.provider_factory.db_ref();

        macro_rules! clear_table {
            ($table:ty) => {{
                let tx = db.tx_mut()?;
                tx.clear::<$table>()?;
                tx.commit()?;
                info!(target: "reth::cli", table = <$table as Table>::NAME, "Cleared");
            }};
        }

        // Tables migrated to static files
        clear_table!(tables::TransactionSenders);
        clear_table!(tables::AccountChangeSets);
        clear_table!(tables::StorageChangeSets);

        // Tables migrated to RocksDB
        clear_table!(tables::TransactionHashNumbers);
        clear_table!(tables::AccountsHistory);
        clear_table!(tables::StoragesHistory);

        // Plain state tables superseded by hashed state in v2
        clear_table!(tables::PlainAccountState);
        clear_table!(tables::PlainStorageState);

        info!(target: "reth::cli", "MDBX tables pruned");
        Ok(())
    }

    /// Creates a compacted copy of the MDBX database to `<db_path>/../db_compact/`.
    ///
    /// Returns the path to the compacted copy. The caller must swap it with the
    /// original after dropping the database handle.
    pub fn compact_mdbx(db: &mdbx::DatabaseEnv) -> eyre::Result<PathBuf> {
        let db_path = db.path();
        let compact_path = db_path.with_file_name("db_compact");

        reth_fs_util::create_dir_all(&compact_path)?;

        info!(target: "reth::cli", ?db_path, ?compact_path, "Compacting MDBX database");

        let compact_dest = compact_path.join("mdbx.dat");
        let dest_cstr = std::ffi::CString::new(
            compact_dest.to_str().ok_or_else(|| eyre::eyre!("compact path must be valid UTF-8"))?,
        )?;

        let flags = ffi::MDBX_CP_COMPACT | ffi::MDBX_CP_FORCE_DYNAMIC_SIZE;

        let rc = db.with_raw_env_ptr(|env_ptr| unsafe {
            ffi::mdbx_env_copy(env_ptr, dest_cstr.as_ptr(), flags)
        });

        if rc != 0 {
            eyre::bail!("mdbx_env_copy failed with error code {rc}: {}", unsafe {
                std::ffi::CStr::from_ptr(ffi::mdbx_strerror(rc)).to_string_lossy()
            });
        }

        info!(target: "reth::cli", "MDBX compaction complete");
        Ok(compact_path)
    }
}
