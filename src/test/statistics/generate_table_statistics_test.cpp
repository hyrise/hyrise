#include "gtest/gtest.h"

#include "statistics/column_statistics.hpp"
#include "statistics/histograms/abstract_histogram.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics_slice.hpp"
#include "statistics/table_statistics2.hpp"
#include "statistics_test_utils.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class GenerateTableStatisticsTest : public ::testing::Test {};

TEST_F(GenerateTableStatisticsTest, GenerateTableStatisticsUnsampled) {
  const auto table = load_table("resources/test_data/tbl/tpch/sf-0.001/customer.tbl");
  const auto table_statistics = generate_table_statistics(*table);

  ASSERT_EQ(table_statistics.column_statistics().size(), 8u);
  EXPECT_EQ(table_statistics.row_count(), 150u);

  EXPECT_INT32_COLUMN_STATISTICS(table_statistics.column_statistics().at(0), 0.0f, 150, 1, 150);
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics.column_statistics().at(1), 0.0f, 150, "Customer#000000001",
                                  "Customer#000000150");
  EXPECT_INT32_COLUMN_STATISTICS(table_statistics.column_statistics().at(3), 0.0f, 25, 0, 24);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics.column_statistics().at(5), 0.0f, 150, -986.96f, 9983.38f);
}

TEST_F(GenerateTableStatisticsTest, GenerateTableStatistics2Unsampled) {
  const auto table = load_table("resources/test_data/tbl/int_with_nulls_large.tbl", 20);

  generate_table_statistics2(table);

  const auto table_statistics = table->table_statistics2();

  std::cout << (*table_statistics) << std::endl;

  ASSERT_EQ(table_statistics->row_count(), 200u);
  ASSERT_EQ(table_statistics->table_statistics_slice_sets.size(), 1u);
  ASSERT_EQ(table_statistics->table_statistics_slice_sets.at(0).size(), 1u);

  EXPECT_EQ(table_statistics->table_statistics_slice_sets.at(0).at(0)->row_count, 200u);

  const auto compact_chunk_statistics = table_statistics->table_statistics_slice_sets.at(0).at(0);
  ASSERT_EQ(compact_chunk_statistics->segment_statistics.size(), 2u);

  const auto compact_segment_statistics_a = std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(compact_chunk_statistics->segment_statistics.at(0));
  ASSERT_TRUE(compact_segment_statistics_a);

  const auto compact_histogram_a = std::dynamic_pointer_cast<AbstractHistogram<int32_t>>(compact_segment_statistics_a->histogram);
  ASSERT_TRUE(compact_histogram_a);

  // The 24 nulls values should be represented in the compact statistics as well
  EXPECT_FLOAT_EQ(compact_histogram_a->total_count(), 200 - 27);
  EXPECT_FLOAT_EQ(compact_histogram_a->total_distinct_count(), 10);

  const auto compact_segment_statistics_b = std::dynamic_pointer_cast<SegmentStatistics2<int32_t>>(compact_chunk_statistics->segment_statistics.at(1));
  ASSERT_TRUE(compact_segment_statistics_b);

  const auto compact_histogram_b = std::dynamic_pointer_cast<AbstractHistogram<int32_t>>(compact_segment_statistics_b->histogram);
  ASSERT_TRUE(compact_histogram_b);

  // The 24 nulls values should be represented in the compact statistics as well
  EXPECT_FLOAT_EQ(compact_histogram_b->total_count(), 200 - 9);
  EXPECT_FLOAT_EQ(compact_histogram_b->total_distinct_count(), 190);

}

}  // namespace opossum
