#include "gtest/gtest.h"

#include <fstream>

#include "statistics/column_statistics.hpp"
#include "statistics/statistics_import_export.hpp"
#include "statistics/table_statistics.hpp"
#include "utils/load_table.hpp"
#include "base_test.hpp"

namespace opossum {

class StatisticsImportExportTest : public ::testing::Test {};

TEST_F(StatisticsImportExportTest, EndToEnd) {
  std::vector<std::shared_ptr<const AbstractColumnStatistics>> original_column_statistics;

  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<int32_t>>(0.3f, 50.1f, 21, 100));
  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<int64_t>>(0.4f, 51.2f, 22, 101));
  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<float>>(0.4f, 51.3f, 2.2f, 1.01f));
  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<double>>(0.4f, 51.3f, 2.2, 1.01));
  original_column_statistics.emplace_back(std::make_shared<ColumnStatistics<std::string>>(0.4f, 51.3f, "abc", "xyz"));

  TableStatistics original_table_statistics{TableType::Data, 3500, original_column_statistics};

  const auto exported_statistics_file_path = test_data_path + "exported_table_statistics.json";

  export_table_statistics(original_table_statistics, exported_statistics_file_path);

  const auto imported_table_statistics = import_table_statistics(exported_statistics_file_path);

  EXPECT_EQ(imported_table_statistics.table_type(), TableType::Data);
  EXPECT_EQ(imported_table_statistics.row_count(), 3500);
  ASSERT_EQ(imported_table_statistics.column_statistics().size(), 5);

  const auto imported_column_0 = imported_table_statistics.column_statistics().at(0);
  ASSERT_EQ(imported_column_0)
}


}  // namespace opossum
