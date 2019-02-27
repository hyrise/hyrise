#include <cstdio>
#include <fstream>

#include "gtest/gtest.h"

#include "base_test.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/statistics_import_export.hpp"
#include "statistics/table_statistics.hpp"
#include "statistics_test_utils.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class StatisticsImportExportTest : public ::testing::Test {
  void TearDown() { std::remove((test_data_path + "exported_table_statistics_test.json").c_str()); }
};

TEST_F(StatisticsImportExportTest, EndToEnd) {
  std::vector<std::shared_ptr<const BaseColumnStatistics>> original_column_statistics;

  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<int32_t>>(0.3f, 50.1f, 21, 100));
  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<int64_t>>(0.4f, 51.2f, 22, 101));
  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<float>>(0.5f, 51.3f, 2.2f, 1.01f));
  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<double>>(0.6f, 52.3f, 2.2444, 1.01555));
  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<pmr_string>>(0.7f, 53.3f, "abc", "xyz"));

  TableStatistics original_table_statistics{TableType::Data, 3500, original_column_statistics};

  const auto exported_statistics_file_path = test_data_path + "exported_table_statistics_test.json";

  export_table_statistics(original_table_statistics, exported_statistics_file_path);

  const auto imported_table_statistics = import_table_statistics(exported_statistics_file_path);

  EXPECT_EQ(imported_table_statistics.table_type(), TableType::Data);
  EXPECT_EQ(imported_table_statistics.row_count(), 3500);
  ASSERT_EQ(imported_table_statistics.column_statistics().size(), 5u);

  EXPECT_INT32_COLUMN_STATISTICS(imported_table_statistics.column_statistics().at(0), 0.3f, 50.1f, 21, 100);
  EXPECT_INT64_COLUMN_STATISTICS(imported_table_statistics.column_statistics().at(1), 0.4f, 51.2f, 22, 101);
  EXPECT_FLOAT_COLUMN_STATISTICS(imported_table_statistics.column_statistics().at(2), 0.5f, 51.3f, 2.2f, 1.01f);
  EXPECT_DOUBLE_COLUMN_STATISTICS(imported_table_statistics.column_statistics().at(3), 0.6f, 52.3f, 2.2444, 1.01555);
  EXPECT_STRING_COLUMN_STATISTICS(imported_table_statistics.column_statistics().at(4), 0.7f, 53.3f, "abc", "xyz");
}

}  // namespace opossum
