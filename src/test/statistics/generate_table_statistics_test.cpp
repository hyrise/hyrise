#include "gtest/gtest.h"

#include "statistics/column_statistics.hpp"
#include "statistics/generate_table_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/load_table.hpp"

namespace opossum {

#define EXPECT_INT_COLUMN_STATISTICS(column_statistics, null_value_ratio_, distinct_count_, min_, max_)  \
  ASSERT_EQ(column_statistics->data_type(), DataType::Int);                                              \
  EXPECT_FLOAT_EQ(column_statistics->null_value_ratio(), null_value_ratio_);                             \
  EXPECT_FLOAT_EQ(column_statistics->distinct_count(), distinct_count_);                                 \
  EXPECT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<int32_t>>(column_statistics)->min(), min_); \
  EXPECT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<int32_t>>(column_statistics)->max(), max_);

#define EXPECT_FLOAT_COLUMN_STATISTICS(column_statistics, null_value_ratio_, distinct_count_, min_, max_)    \
  ASSERT_EQ(column_statistics->data_type(), DataType::Float);                                                \
  EXPECT_FLOAT_EQ(column_statistics->null_value_ratio(), null_value_ratio_);                                 \
  EXPECT_FLOAT_EQ(column_statistics->distinct_count(), distinct_count_);                                     \
  EXPECT_FLOAT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<float>>(column_statistics)->min(), min_); \
  EXPECT_FLOAT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<float>>(column_statistics)->max(), max_);

#define EXPECT_STRING_COLUMN_STATISTICS(column_statistics, null_value_ratio_, distinct_count_, min_, max_)   \
  ASSERT_EQ(column_statistics->data_type(), DataType::String);                                               \
  EXPECT_FLOAT_EQ(column_statistics->null_value_ratio(), null_value_ratio_);                                 \
  EXPECT_FLOAT_EQ(column_statistics->distinct_count(), distinct_count_);                                     \
  EXPECT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<std::string>>(column_statistics)->min(), min_); \
  EXPECT_EQ(std::dynamic_pointer_cast<const ColumnStatistics<std::string>>(column_statistics)->max(), max_);

class GenerateTableStatisticsTest : public ::testing::Test {};

TEST_F(GenerateTableStatisticsTest, GenerateTableStatisticsUnsampled) {
  const auto table = load_table("src/test/tables/tpch/sf-0.001/customer.tbl");
  const auto table_statistics = generate_table_statistics(*table);

  ASSERT_EQ(table_statistics.column_statistics().size(), 8u);
  EXPECT_EQ(table_statistics.row_count(), 150u);

  EXPECT_INT_COLUMN_STATISTICS(table_statistics.column_statistics().at(0), 0.0f, 150, 1, 150);
  EXPECT_STRING_COLUMN_STATISTICS(table_statistics.column_statistics().at(1), 0.0f, 150, "Customer#000000001",
                                  "Customer#000000150");
  EXPECT_INT_COLUMN_STATISTICS(table_statistics.column_statistics().at(3), 0.0f, 25, 0, 24);
  EXPECT_FLOAT_COLUMN_STATISTICS(table_statistics.column_statistics().at(5), 0.0f, 150, -986.96f, 9983.38f);
}

#undef EXPECT_INT_COLUMN_STATISTICS
#undef EXPECT_FLOAT_COLUMN_STATISTICS
#undef EXPECT_STRING_COLUMN_STATISTICS

}  // namespace opossum
