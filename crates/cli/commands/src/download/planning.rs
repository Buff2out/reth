use super::{archive::verify_output_files, manifest::*};
use eyre::Result;
use std::{collections::BTreeMap, path::Path};
use tracing::info;

#[derive(Debug, Clone)]
pub(crate) struct PlannedArchive {
    pub(crate) ty: SnapshotComponentType,
    pub(crate) component: String,
    pub(crate) archive: ArchiveDescriptor,
}

#[derive(Debug)]
pub(crate) struct PlannedDownloads {
    pub(crate) archives: Vec<PlannedArchive>,
    pub(crate) total_size: u64,
}

impl PlannedDownloads {
    pub(crate) const fn total_archives(&self) -> usize {
        self.archives.len()
    }
}

pub(crate) const fn archive_priority_rank(ty: SnapshotComponentType) -> u8 {
    match ty {
        SnapshotComponentType::State => 0,
        SnapshotComponentType::RocksdbIndices => 1,
        _ => 2,
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct DownloadStartupSummary {
    pub(crate) reusable: usize,
    pub(crate) needs_download: usize,
}

pub(crate) fn summarize_download_startup(
    all_downloads: &[PlannedArchive],
    target_dir: &Path,
) -> Result<DownloadStartupSummary> {
    let mut summary = DownloadStartupSummary::default();

    for planned in all_downloads {
        if verify_output_files(target_dir, &planned.archive.output_files)? {
            summary.reusable += 1;
        } else {
            summary.needs_download += 1;
        }
    }

    Ok(summary)
}

fn selection_archive_distance(selection: &ComponentSelection) -> Option<Option<u64>> {
    match selection {
        ComponentSelection::All => Some(None),
        ComponentSelection::Distance(distance) => Some(Some(*distance)),
        ComponentSelection::None => None,
    }
}

fn sort_planned_archives(all_downloads: &mut [PlannedArchive]) {
    all_downloads.sort_by(|a, b| {
        archive_priority_rank(a.ty)
            .cmp(&archive_priority_rank(b.ty))
            .then_with(|| a.component.cmp(&b.component))
            .then_with(|| a.archive.file_name.cmp(&b.archive.file_name))
    });
}

pub(crate) fn collect_planned_archives(
    manifest: &SnapshotManifest,
    selections: &BTreeMap<SnapshotComponentType, ComponentSelection>,
) -> Result<PlannedDownloads> {
    let mut archives = Vec::new();
    let mut total_size = 0;

    for (ty, selection) in selections {
        let Some(distance) = selection_archive_distance(selection) else { continue };
        total_size += manifest.size_for_distance(*ty, distance);

        let descriptors = manifest.archive_descriptors_for_distance(*ty, distance);
        let component = ty.display_name().to_string();
        if !descriptors.is_empty() {
            info!(target: "reth::cli",
                component = %component,
                archives = descriptors.len(),
                selection = %selection,
                "Queued component for download"
            );
        }

        for descriptor in descriptors {
            if descriptor.output_files.is_empty() {
                eyre::bail!(
                    "Invalid modular manifest: {} is missing plain output checksum metadata",
                    descriptor.file_name
                );
            }

            archives.push(PlannedArchive {
                ty: *ty,
                component: component.clone(),
                archive: descriptor,
            });
        }
    }

    sort_planned_archives(&mut archives);
    Ok(PlannedDownloads { archives, total_size })
}

#[cfg(test)]
mod tests {
    use super::{super::archive::file_blake3_hex, *};
    use tempfile::tempdir;

    #[test]
    fn summarize_download_startup_counts_reusable_and_needs_download() {
        let dir = tempdir().unwrap();
        let target_dir = dir.path();
        let ok_file = target_dir.join("ok.bin");
        std::fs::write(&ok_file, vec![1_u8; 4]).unwrap();
        let ok_hash = file_blake3_hex(&ok_file).unwrap();

        let planned = vec![
            PlannedArchive {
                ty: SnapshotComponentType::State,
                component: "State".to_string(),
                archive: ArchiveDescriptor {
                    url: "https://example.com/ok.tar.zst".to_string(),
                    file_name: "ok.tar.zst".to_string(),
                    size: 10,
                    blake3: None,
                    output_files: vec![OutputFileChecksum {
                        path: "ok.bin".to_string(),
                        size: 4,
                        blake3: ok_hash,
                    }],
                },
            },
            PlannedArchive {
                ty: SnapshotComponentType::Headers,
                component: "Headers".to_string(),
                archive: ArchiveDescriptor {
                    url: "https://example.com/missing.tar.zst".to_string(),
                    file_name: "missing.tar.zst".to_string(),
                    size: 10,
                    blake3: None,
                    output_files: vec![OutputFileChecksum {
                        path: "missing.bin".to_string(),
                        size: 1,
                        blake3: "deadbeef".to_string(),
                    }],
                },
            },
            PlannedArchive {
                ty: SnapshotComponentType::Transactions,
                component: "Transactions".to_string(),
                archive: ArchiveDescriptor {
                    url: "https://example.com/bad-size.tar.zst".to_string(),
                    file_name: "bad-size.tar.zst".to_string(),
                    size: 10,
                    blake3: None,
                    output_files: vec![],
                },
            },
        ];

        let summary = summarize_download_startup(&planned, target_dir).unwrap();
        assert_eq!(summary.reusable, 1);
        assert_eq!(summary.needs_download, 2);
    }

    #[test]
    fn archive_priority_prefers_state_then_rocksdb() {
        let mut planned = [
            PlannedArchive {
                ty: SnapshotComponentType::Transactions,
                component: "Transactions".to_string(),
                archive: ArchiveDescriptor {
                    url: "u3".to_string(),
                    file_name: "t.tar.zst".to_string(),
                    size: 1,
                    blake3: None,
                    output_files: vec![OutputFileChecksum {
                        path: "a".to_string(),
                        size: 1,
                        blake3: "x".to_string(),
                    }],
                },
            },
            PlannedArchive {
                ty: SnapshotComponentType::RocksdbIndices,
                component: "RocksDB Indices".to_string(),
                archive: ArchiveDescriptor {
                    url: "u2".to_string(),
                    file_name: "rocksdb_indices.tar.zst".to_string(),
                    size: 1,
                    blake3: None,
                    output_files: vec![OutputFileChecksum {
                        path: "b".to_string(),
                        size: 1,
                        blake3: "y".to_string(),
                    }],
                },
            },
            PlannedArchive {
                ty: SnapshotComponentType::State,
                component: "State (mdbx)".to_string(),
                archive: ArchiveDescriptor {
                    url: "u1".to_string(),
                    file_name: "state.tar.zst".to_string(),
                    size: 1,
                    blake3: None,
                    output_files: vec![OutputFileChecksum {
                        path: "c".to_string(),
                        size: 1,
                        blake3: "z".to_string(),
                    }],
                },
            },
        ];

        planned.sort_by(|a, b| {
            archive_priority_rank(a.ty)
                .cmp(&archive_priority_rank(b.ty))
                .then_with(|| a.component.cmp(&b.component))
                .then_with(|| a.archive.file_name.cmp(&b.archive.file_name))
        });

        assert_eq!(planned[0].ty, SnapshotComponentType::State);
        assert_eq!(planned[1].ty, SnapshotComponentType::RocksdbIndices);
        assert_eq!(planned[2].ty, SnapshotComponentType::Transactions);
    }
}
