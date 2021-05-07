#include <vector>

#include "base_test.hpp"
#include "gtest/gtest_prod.h"
#include "lib/utils/plugin_test_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

#include "../../plugins/functional_dependency_plugin.hpp"

namespace opossum {

class FunctionalDependencyPluginTest : public BaseTest {
  void SetUp() override {
    const auto& table = load_table("resources/test_data/tbl/functional_dependency_input.tbl", _chunk_size);
    _row_count = table->row_count();
    Hyrise::get().storage_manager.add_table(_table_name, table);
  }

  void TearDown() override { Hyrise::reset(); }

 protected:
  const std::string _table_name{"functionalDependencyTestTable"};
  static constexpr auto _chunk_size = size_t{3};
  uint64_t _row_count = uint64_t{0};

  static bool _check_dependency(const std::string& table_name, std::vector<ColumnID> determinant,
                                std::vector<ColumnID> dependent) {
    return FunctionalDependencyPlugin::_check_dependency(Hyrise::get().storage_manager.get_table(table_name),
                                                         determinant, dependent);
  }
  static void _process_column_data_string(const std::string& table_name, ColumnID column_id, std::vector<long> &process_column ){
    FunctionalDependencyPlugin::_process_column_data_string(Hyrise::get().storage_manager.get_table(table_name), column_id, process_column);
  }
  static void _process_column_data_numeric(const std::string& table_name, ColumnID column_id, std::vector<long> &process_column ){
    FunctionalDependencyPlugin::_process_column_data_numeric(Hyrise::get().storage_manager.get_table(table_name), column_id, process_column);
  }
  static void _process_column_data_numeric_null(const std::string& table_name, ColumnID column_id, std::vector<long> &process_column, std::vector<long> &null_column ){
    FunctionalDependencyPlugin::_process_column_data_numeric_null(Hyrise::get().storage_manager.get_table(table_name), column_id, process_column, null_column);
  }
};

TEST_F(FunctionalDependencyPluginTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseFunctionalDependencyPlugin"));
  pm.unload_plugin("hyriseFunctionalDependencyPlugin");
}

TEST_F(FunctionalDependencyPluginTest, ProcessColumnData) {
  std::vector<long> column_normalized_values;
  std::vector<long> expected_values{1234, 1221, 1234, 1201, 1201, 1301, 1322, 1322};
  column_normalized_values.reserve(_row_count);
  _process_column_data_numeric(_table_name, ColumnID{0}, column_normalized_values);

  for (size_t value_id = 0; value_id < column_normalized_values.size(); value_id++) {
    EXPECT_EQ(column_normalized_values[value_id], expected_values[value_id]);
  }

  column_normalized_values = std::vector<long> {};
  std::vector<long> column_null_values(_row_count, 0);
  std::vector<long> expected_null_values{1, 0, 0, 0, 0, 0, 0, 1};
  expected_values = std::vector<long> {0, 10, 20, 30, 20, 10, 10, 0};
  column_normalized_values.reserve(_row_count);

  _process_column_data_numeric_null(_table_name, ColumnID{4}, column_normalized_values, column_null_values);

  for (size_t value_id = 0; value_id < column_normalized_values.size(); value_id++) {
    EXPECT_EQ(column_normalized_values[value_id], expected_values[value_id]);
  }
  for (size_t value_id = 0; value_id < column_null_values.size(); value_id++) {
    EXPECT_EQ(column_null_values[value_id], expected_null_values[value_id]);
  }

  column_normalized_values = std::vector<long> {};
  expected_values = std::vector<long> {1, 2, 3, 4, 5, 6, 0, 0};
  column_normalized_values.reserve(_row_count);

  _process_column_data_string(_table_name, ColumnID{3}, column_normalized_values);

  for (size_t value_id = 0; value_id < column_normalized_values.size(); value_id++) {
    EXPECT_EQ(column_normalized_values[value_id], expected_values[value_id]);
  }

}

TEST_F(FunctionalDependencyPluginTest, CheckDependency) {
  EXPECT_TRUE(_check_dependency(_table_name, std::vector<ColumnID>{ColumnID{0}}, std::vector<ColumnID>{1}));
  EXPECT_TRUE(_check_dependency(_table_name, std::vector<ColumnID>{ColumnID{0}, ColumnID{2}},
                                std::vector<ColumnID>{ColumnID{3}}));
  EXPECT_TRUE(_check_dependency(_table_name, std::vector<ColumnID>{ColumnID{0}, ColumnID{2}},
                                std::vector<ColumnID>{ColumnID{1}, ColumnID{3}}));
  EXPECT_FALSE(_check_dependency(_table_name, std::vector<ColumnID>{ColumnID{0}}, std::vector<ColumnID>{ColumnID{2}}));
  EXPECT_FALSE(_check_dependency(_table_name, std::vector<ColumnID>{ColumnID{0}, ColumnID{1}},
                                 std::vector<ColumnID>{ColumnID{2}}));
  EXPECT_TRUE(_check_dependency(_table_name, std::vector<ColumnID>{ColumnID{2}, ColumnID{4}, ColumnID{5}},
                                std::vector<ColumnID>{ColumnID{6}}));
}

}  // namespace opossum