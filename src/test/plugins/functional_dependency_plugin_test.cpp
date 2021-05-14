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
};

TEST_F(FunctionalDependencyPluginTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseFunctionalDependencyPlugin"));
  pm.unload_plugin("hyriseFunctionalDependencyPlugin");
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