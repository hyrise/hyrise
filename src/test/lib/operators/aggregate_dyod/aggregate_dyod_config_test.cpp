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



}  // namespace hyrise
