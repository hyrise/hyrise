#include <bit>
#include <cstddef>

#include "base_test.hpp"
#include "operators/aggregate_dyod/aggregate_dyod_config.hpp"
#include "operators/aggregate_dyod/cache_info.hpp"

namespace hyrise {

class AggregateDYODConfigTest : public BaseTest {};

TEST_F(AggregateDYODConfigTest, KeysBudgetMatchesFallbackTuning) {
  EXPECT_EQ(keys_budget_for(FALLBACK_CACHE_SIZES), 32768);
}

TEST_F(AggregateDYODConfigTest, KeysBudgetGrowsWithLastLevelCache) {
  auto small = FALLBACK_CACHE_SIZES;
  auto large = FALLBACK_CACHE_SIZES;
  large.l3_bytes = 2 * small.l3_bytes;

  EXPECT_EQ(keys_budget_for(large), 2 * keys_budget_for(small));
}

TEST_F(AggregateDYODConfigTest, KeysBudgetSplitsTheSliceAmongItsSharers) {
  auto crowded = FALLBACK_CACHE_SIZES;
  crowded.llc_sharing_cpus = 2 * FALLBACK_CACHE_SIZES.llc_sharing_cpus;

  EXPECT_EQ(keys_budget_for(crowded), keys_budget_for(FALLBACK_CACHE_SIZES) / 2);
}

TEST_F(AggregateDYODConfigTest, MergeTileRowsMatchesFallbackTuning) {
  EXPECT_EQ(merge_tile_rows_for(FALLBACK_CACHE_SIZES), 2048);
}

TEST_F(AggregateDYODConfigTest, MergeTileScratchStaysWithinL1) {
  auto sizes = FALLBACK_CACHE_SIZES;
  for (auto l1d = size_t{4} * 1024; l1d <= size_t{1024} * 1024; l1d *= 2) {
    sizes.l1d_bytes = l1d;
    EXPECT_LE(merge_tile_rows_for(sizes) * sizeof(uint32_t), l1d / 4);
    EXPECT_GE(merge_tile_rows_for(sizes), 64);
  }
}

TEST_F(AggregateDYODConfigTest, MergeTileRowsGrowsWithL1) {
  auto small = FALLBACK_CACHE_SIZES;
  auto large = FALLBACK_CACHE_SIZES;
  large.l1d_bytes = 2 * small.l1d_bytes;

  EXPECT_LT(merge_tile_rows_for(small), merge_tile_rows_for(large));
}

TEST_F(AggregateDYODConfigTest, NoMergeSplitForEvenlySizedPartitions) {
  EXPECT_EQ(merge_split_ways_for(4000, 4000, 500, 32, 32), 1);
  EXPECT_EQ(merge_split_ways_for(3 * 4000, 4000, 500, 32, 32), 1);
}

TEST_F(AggregateDYODConfigTest, MergeSplitFollowsTheRowSurplus) {
  EXPECT_EQ(merge_split_ways_for(80'000, 4000, 500, 32, 32), 20);
  EXPECT_EQ(merge_split_ways_for(40'000, 4000, 500, 32, 32), 10);
}

TEST_F(AggregateDYODConfigTest, NoMergeSplitForKeyRichPartitions) {
  EXPECT_EQ(merge_split_ways_for(80'000, 4000, 20'000, 32, 32), 1);
}

TEST_F(AggregateDYODConfigTest, MergeSplitIsCappedByStoresAndWorkers) {
  EXPECT_EQ(merge_split_ways_for(80'000, 4000, 500, 8, 32), 8);
  EXPECT_EQ(merge_split_ways_for(80'000, 4000, 500, 32, 6), 6);
}

TEST_F(AggregateDYODConfigTest, NoMergeSplitForSingleThreadedRuns) {
  EXPECT_EQ(merge_split_ways_for(80'000, 4000, 500, 32, 1), 1);
}

TEST_F(AggregateDYODConfigTest, NoMergeSplitForEmptyInput) {
  EXPECT_EQ(merge_split_ways_for(0, 0, 0, 32, 32), 1);
}

TEST_F(AggregateDYODConfigTest, MaxPartitionCountMatchesFallbackTuning) {
  EXPECT_EQ(max_partition_count_for(FALLBACK_CACHE_SIZES, 2), 2048);
}

TEST_F(AggregateDYODConfigTest, MaxPartitionCountKeepsStagingWithinL2) {
  auto sizes = FALLBACK_CACHE_SIZES;
  for (auto l2 = size_t{64} * 1024; l2 <= size_t{64} * 1024 * 1024; l2 *= 2) {
    sizes.l2_bytes = l2;
    for (auto stream_count = size_t{1}; stream_count <= 16; ++stream_count) {
      const auto partitions = max_partition_count_for(sizes, stream_count);
      EXPECT_LE(partitions * stream_count * (SWWC_LINE_BYTES + sizeof(size_t)), l2);
      EXPECT_GE(partitions, 1);
    }
  }
}

TEST_F(AggregateDYODConfigTest, MaxPartitionCountShrinksWithStreamCount) {
  EXPECT_LT(max_partition_count_for(FALLBACK_CACHE_SIZES, 4), max_partition_count_for(FALLBACK_CACHE_SIZES, 2));
}

TEST_F(AggregateDYODConfigTest, MaxPartitionCountIsPowerOfTwo) {
  for (auto stream_count = size_t{1}; stream_count <= 16; ++stream_count) {
    EXPECT_TRUE(std::has_single_bit(max_partition_count_for(FALLBACK_CACHE_SIZES, stream_count)));
  }
}

TEST_F(AggregateDYODConfigTest, MaxPartitionCountRespectsAbsoluteCeiling) {
  auto sizes = FALLBACK_CACHE_SIZES;
  sizes.l2_bytes = size_t{64} * 1024 * 1024;
  EXPECT_EQ(max_partition_count_for(sizes, 1), MAX_PARTITION_COUNT);
}



TEST_F(AggregateDYODConfigTest, MorselRowsQuartersADefaultChunk) {
  EXPECT_EQ(MORSEL_ROWS, 16384);
}

TEST_F(AggregateDYODConfigTest, MorselCountCoversEveryRow) {
  EXPECT_EQ(morsel_count_for(65535, MORSEL_ROWS), 4);
  EXPECT_EQ(morsel_count_for(4 * MORSEL_ROWS, MORSEL_ROWS), 4);
  EXPECT_EQ(morsel_count_for(4 * MORSEL_ROWS + 1, MORSEL_ROWS), 5);
}

TEST_F(AggregateDYODConfigTest, MorselCountIsAtLeastOne) {
  EXPECT_EQ(morsel_count_for(0, MORSEL_ROWS), 1);
  EXPECT_EQ(morsel_count_for(1, MORSEL_ROWS), 1);
  EXPECT_EQ(morsel_count_for(MORSEL_ROWS, MORSEL_ROWS), 1);
}

TEST_F(AggregateDYODConfigTest, WorkerLimitFollowsScheduler) {
  EXPECT_EQ(worker_limit_for(true, 32), 32);
  EXPECT_EQ(worker_limit_for(false, 32), 1);
}

TEST_F(AggregateDYODConfigTest, WorkerLimitIsAtLeastOne) {
  EXPECT_EQ(worker_limit_for(true, 0), 1);
  EXPECT_EQ(worker_limit_for(false, 0), 1);
}

TEST_F(AggregateDYODConfigTest, EstimateSampleStrideCoversSmallInputs) {
  EXPECT_EQ(estimate_sample_stride(0), 1);
  EXPECT_EQ(estimate_sample_stride(ESTIMATE_SAMPLE_CHUNKS), 1);
  EXPECT_EQ(estimate_sample_stride(4 * ESTIMATE_SAMPLE_CHUNKS), 4);
}

TEST_F(AggregateDYODConfigTest, SampledEstimatePassesThroughWithoutSampling) {
  EXPECT_EQ(scale_sampled_estimate(1234, 1234, 2'000'000, 1), 1234);
}

TEST_F(AggregateDYODConfigTest, SampledEstimateKeepsPlateauedCardinality) {
  EXPECT_EQ(scale_sampled_estimate(4, 4, 60'000'000, 57), 4);
  EXPECT_EQ(scale_sampled_estimate(100'000, 100'000, 60'000'000, 57), 100'000);
}

TEST_F(AggregateDYODConfigTest, SampledEstimateScalesGrowingCardinality) {
  EXPECT_EQ(scale_sampled_estimate(300'000, 150'000, 6'000'000, 5), 1'500'000);
}

TEST_F(AggregateDYODConfigTest, SampledEstimateScalesClusteredDuplicatedKeys) {
  EXPECT_EQ(scale_sampled_estimate(131'072, 65'536, 67'000'000, 64), 8'388'608);
}

TEST_F(AggregateDYODConfigTest, SampledEstimateInterpolatesPartialGrowth) {
  EXPECT_NEAR(static_cast<double>(scale_sampled_estimate(200'000, 141'421, 60'000'000, 16)), 800'000, 2'000);
}

TEST_F(AggregateDYODConfigTest, SampledEstimateNeverExceedsRowCount) {
  EXPECT_EQ(scale_sampled_estimate(900'000, 450'000, 20'000'000, 57), 20'000'000);
}

TEST_F(AggregateDYODConfigTest, SampledEstimateFallsBackOnEmptySamples) {
  EXPECT_EQ(scale_sampled_estimate(0, 0, 20'000'000, 57), 20'000'000);
}

TEST_F(AggregateDYODConfigTest, KeyPieceWidthIsTheWidestAlignedDivisor) {
  EXPECT_EQ(key_piece_width(4), 4);
  EXPECT_EQ(key_piece_width(8), 8);
  EXPECT_EQ(key_piece_width(12), 4);
  EXPECT_EQ(key_piece_width(16), 16);
  EXPECT_EQ(key_piece_width(20), 4);
  EXPECT_EQ(key_piece_width(24), 8);
  EXPECT_EQ(key_piece_width(28), 4);
}

}  // namespace hyrise
