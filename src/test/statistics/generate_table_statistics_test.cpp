#include "gtest/gtest.h" // NEEDEDINCLUDE

#include "statistics/column_statistics.hpp" // NEEDEDINCLUDE
#include "statistics/generate_table_statistics.hpp" // NEEDEDINCLUDE // NEEDEDINCLUDE
#include "statistics/table_statistics.hpp" // NEEDEDINCLUDE
#include "statistics_test_utils.hpp" // NEEDEDINCLUDE
#include "utils/load_table.hpp" // NEEDEDINCLUDE

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

}  // namespace opossum
