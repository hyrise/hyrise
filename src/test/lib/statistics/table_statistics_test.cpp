#include "base_test.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "statistics/statistics_objects/abstract_histogram.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/load_table.hpp"

namespace hyrise {

class TableStatisticsTest : public BaseTest {};

TEST_F(TableStatisticsTest, FromTable) {
  const auto table = load_table("resources/test_data/tbl/int_with_nulls_large.tbl", ChunkOffset{20});

  const auto table_statistics = TableStatistics::from_table(*table);

  EXPECT_EQ(table_statistics->row_count, 200);
  ASSERT_EQ(table_statistics->column_statistics.size(), 2);

  const auto column_statistics_a =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(table_statistics->column_statistics.at(0));
  ASSERT_TRUE(column_statistics_a);

  const auto histogram_a = std::dynamic_pointer_cast<const AbstractHistogram<int32_t>>(column_statistics_a->histogram);
  ASSERT_TRUE(histogram_a);

  // The 24 NULL values should be represented in the compact statistics as well.
  EXPECT_DOUBLE_EQ(histogram_a->total_count(), 200 - 27);
  EXPECT_DOUBLE_EQ(histogram_a->total_distinct_count(), 10);

  const auto column_statistics_b =
      std::dynamic_pointer_cast<const AttributeStatistics<int32_t>>(table_statistics->column_statistics.at(1));
  ASSERT_TRUE(column_statistics_b);

  const auto histogram_b = std::dynamic_pointer_cast<const AbstractHistogram<int32_t>>(column_statistics_b->histogram);
  ASSERT_TRUE(histogram_b);

  // The 24 NULL values should be represented in the compact statistics as well.
  EXPECT_DOUBLE_EQ(histogram_b->total_count(), 200 - 9);
  EXPECT_DOUBLE_EQ(histogram_b->total_distinct_count(), 190);
}

}  // namespace hyrise
