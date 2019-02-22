#include "gtest/gtest.h"

#include "statistics/column_statistics.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/histograms/abstract_histogram.hpp"
#include "statistics/horizontal_statistics_slice.hpp"
#include "statistics/table_cardinality_estimation_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/vertical_statistics_slice.hpp"
#include "statistics_test_utils.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class TableCardinalityEstimationStatisticsTest : public ::testing::Test {};

TEST_F(TableCardinalityEstimationStatisticsTest, FromTable) {
  const auto table = load_table("resources/test_data/tbl/int_with_nulls_large.tbl", 20);

  const auto table_statistics = TableCardinalityEstimationStatistics::from_table(*table);

  std::cout << (*table_statistics) << std::endl;

  ASSERT_EQ(table_statistics->row_count(), 200u);
  ASSERT_EQ(table_statistics->horizontal_slices.size(), 1u);
  ASSERT_EQ(table_statistics->horizontal_slices.size(), 1u);

  EXPECT_EQ(table_statistics->horizontal_slices.at(0)->row_count, 200u);

  const auto compact_chunk_statistics = table_statistics->horizontal_slices.at(0);
  ASSERT_EQ(compact_chunk_statistics->vertical_slices.size(), 2u);

  const auto compact_vertical_slices_a =
      std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(compact_chunk_statistics->vertical_slices.at(0));
  ASSERT_TRUE(compact_vertical_slices_a);

  const auto compact_histogram_a =
      std::dynamic_pointer_cast<AbstractHistogram<int32_t>>(compact_vertical_slices_a->histogram);
  ASSERT_TRUE(compact_histogram_a);

  // The 24 nulls values should be represented in the compact statistics as well
  EXPECT_FLOAT_EQ(compact_histogram_a->total_count(), 200 - 27);
  EXPECT_FLOAT_EQ(compact_histogram_a->total_distinct_count(), 10);

  const auto compact_vertical_slices_b =
      std::dynamic_pointer_cast<VerticalStatisticsSlice<int32_t>>(compact_chunk_statistics->vertical_slices.at(1));
  ASSERT_TRUE(compact_vertical_slices_b);

  const auto compact_histogram_b =
      std::dynamic_pointer_cast<AbstractHistogram<int32_t>>(compact_vertical_slices_b->histogram);
  ASSERT_TRUE(compact_histogram_b);

  // The 24 nulls values should be represented in the compact statistics as well
  EXPECT_FLOAT_EQ(compact_histogram_b->total_count(), 200 - 9);
  EXPECT_FLOAT_EQ(compact_histogram_b->total_distinct_count(), 190);
}

}  // namespace opossum
