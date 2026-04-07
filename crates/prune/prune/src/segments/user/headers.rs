use crate::{
    segments::{self, PruneInput, Segment},
    PrunerError,
};
use reth_provider::StaticFileProviderFactory;
use reth_prune_types::{PruneMode, PrunePurpose, PruneSegment, SegmentOutput};
use reth_static_file_types::StaticFileSegment;
use tracing::instrument;

/// Segment responsible for pruning headers in static files.
#[derive(Debug)]
pub struct Headers {
    mode: PruneMode,
}

impl Headers {
    /// Creates a new [`Headers`] segment with the given prune mode.
    pub const fn new(mode: PruneMode) -> Self {
        Self { mode }
    }
}

impl<Provider> Segment<Provider> for Headers
where
    Provider: StaticFileProviderFactory,
{
    fn segment(&self) -> PruneSegment {
        PruneSegment::Headers
    }

    fn mode(&self) -> Option<PruneMode> {
        Some(self.mode)
    }

    fn purpose(&self) -> PrunePurpose {
        PrunePurpose::User
    }

    #[instrument(
        name = "Headers::prune",
        target = "pruner",
        skip(self, provider),
        ret(level = "trace")
    )]
    fn prune(&self, provider: &Provider, input: PruneInput) -> Result<SegmentOutput, PrunerError> {
        segments::prune_block_based_static_files(provider, input, StaticFileSegment::Headers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{segments::PruneLimiter, SegmentOutput};
    use reth_provider::{
        test_utils::{create_test_provider_factory, MockNodeTypesWithDB},
        ProviderFactory, StaticFileWriter,
    };
    use reth_prune_types::{PruneMode, PruneProgress};
    use reth_static_file_types::{
        SegmentHeader, SegmentRangeInclusive, DEFAULT_BLOCKS_PER_STATIC_FILE,
    };

    fn setup_header_static_file_jars<P: StaticFileProviderFactory>(provider: &P, tip_block: u64) {
        let num_jars = (tip_block + 1) / DEFAULT_BLOCKS_PER_STATIC_FILE;
        let static_file_provider = provider.static_file_provider();

        let mut writer = static_file_provider.latest_writer(StaticFileSegment::Headers).unwrap();

        for jar_idx in 0..num_jars {
            let block_start = jar_idx * DEFAULT_BLOCKS_PER_STATIC_FILE;
            let block_end = ((jar_idx + 1) * DEFAULT_BLOCKS_PER_STATIC_FILE - 1).min(tip_block);

            *writer.user_header_mut() = SegmentHeader::new(
                SegmentRangeInclusive::new(block_start, block_end),
                Some(SegmentRangeInclusive::new(block_start, block_end)),
                None,
                StaticFileSegment::Headers,
            );

            writer.inner().set_dirty();
            writer.commit().expect("commit empty header jar");

            if jar_idx < num_jars - 1 {
                writer.increment_block(block_end + 1).expect("increment header block");
            }
        }

        static_file_provider.initialize_index().expect("initialize index");
    }

    fn setup_transaction_static_file_jars<P: StaticFileProviderFactory>(
        provider: &P,
        tip_block: u64,
    ) {
        let num_jars = (tip_block + 1) / DEFAULT_BLOCKS_PER_STATIC_FILE;
        let static_file_provider = provider.static_file_provider();

        let mut writer =
            static_file_provider.latest_writer(StaticFileSegment::Transactions).unwrap();

        for jar_idx in 0..num_jars {
            let block_start = jar_idx * DEFAULT_BLOCKS_PER_STATIC_FILE;
            let block_end = ((jar_idx + 1) * DEFAULT_BLOCKS_PER_STATIC_FILE - 1).min(tip_block);

            *writer.user_header_mut() = SegmentHeader::new(
                SegmentRangeInclusive::new(block_start, block_end),
                Some(SegmentRangeInclusive::new(block_start, block_end)),
                Some(SegmentRangeInclusive::new(jar_idx, jar_idx)),
                StaticFileSegment::Transactions,
            );

            writer.inner().set_dirty();
            writer.commit().expect("commit empty transaction jar");

            if jar_idx < num_jars - 1 {
                writer.increment_block(block_end + 1).expect("increment transaction block");
            }
        }

        static_file_provider.initialize_index().expect("initialize index");
    }

    fn prune_headers(
        factory: &ProviderFactory<MockNodeTypesWithDB>,
        mode: PruneMode,
        tip: u64,
    ) -> SegmentOutput {
        let (to_block, _) = mode
            .prune_target_block(tip, PruneSegment::Headers, PrunePurpose::User)
            .unwrap()
            .expect("headers should have data to prune");

        Headers::new(mode)
            .prune(
                factory,
                PruneInput {
                    previous_checkpoint: None,
                    to_block,
                    limiter: PruneLimiter::default(),
                },
            )
            .expect("headers prune should succeed")
    }

    #[test]
    fn prune_headers_before_deletes_whole_jars_and_tracks_blocks() {
        let factory = create_test_provider_factory();
        let tip = 2_499_999;
        setup_header_static_file_jars(&factory, tip);

        let output = prune_headers(&factory, PruneMode::Before(900_000), tip);

        assert_eq!(output.progress, PruneProgress::Finished);
        assert_eq!(output.pruned, DEFAULT_BLOCKS_PER_STATIC_FILE as usize);
        assert_eq!(output.checkpoint.unwrap().block_number, Some(499_999));
        assert_eq!(
            factory.static_file_provider().get_lowest_range_start(StaticFileSegment::Headers),
            Some(500_000)
        );
        assert_eq!(
            factory
                .static_file_provider()
                .get_highest_static_file_block(StaticFileSegment::Headers),
            Some(tip)
        );
    }

    #[test]
    fn prune_headers_full_keeps_recent_history_jar() {
        let factory = create_test_provider_factory();
        let tip = 2_499_999;
        setup_header_static_file_jars(&factory, tip);

        let output = prune_headers(&factory, PruneMode::Full, tip);

        assert_eq!(output.progress, PruneProgress::Finished);
        assert_eq!(output.pruned, (DEFAULT_BLOCKS_PER_STATIC_FILE * 4) as usize);
        assert_eq!(output.checkpoint.unwrap().block_number, Some(1_999_999));
        assert_eq!(
            factory.static_file_provider().get_lowest_range_start(StaticFileSegment::Headers),
            Some(2_000_000)
        );
        assert_eq!(
            factory
                .static_file_provider()
                .get_highest_static_file_block(StaticFileSegment::Headers),
            Some(tip)
        );
    }

    #[test]
    fn pruning_headers_updates_earliest_history_height() {
        let factory = create_test_provider_factory();
        let tip = 999_999;

        setup_header_static_file_jars(&factory, tip);
        setup_transaction_static_file_jars(&factory, tip);

        let output = prune_headers(&factory, PruneMode::Before(900_000), tip);

        assert_eq!(output.checkpoint.unwrap().block_number, Some(499_999));
        assert_eq!(factory.static_file_provider().earliest_history_height(), 500_000);
    }
}
