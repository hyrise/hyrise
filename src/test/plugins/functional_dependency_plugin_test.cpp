#include <vector>

#include "base_test.hpp"
#include "lib/utils/plugin_test_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"
#include "gtest/gtest_prod.h"

#include "../../plugins/functional_dependency_plugin.hpp"

namespace opossum {

class FunctionalDependencyPluginTest : public BaseTest {

  void SetUp() override {
    const auto& table = load_table("resources/test_data/tbl/functional_dependency_input.tbl", _chunk_size);
    Hyrise::get().storage_manager.add_table(_table_name, table);
  }

  void TearDown() override { Hyrise::reset(); }

protected:
  const std::string _table_name{"functionalDependencyTestTable"};
  static constexpr auto _chunk_size = size_t{3};

  static bool _check_dependency(const std::string& table_name, std::vector<ColumnID> determinant, std::vector<ColumnID> dependent) {
    return FunctionalDependencyPlugin::_check_dependency(Hyrise::get().storage_manager.get_table(table_name), determinant, dependent);
  }
};

TEST_F(FunctionalDependencyPluginTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseFunctionalDependencyPlugin"));
  pm.unload_plugin("hyriseFunctionalDependencyPlugin");
}

TEST_F(FunctionalDependencyPluginTest, CheckDependency) {
  auto determinant = std::vector<ColumnID>{0};
  auto dependent = std::vector<ColumnID>{1};
  auto dependency_exists = _check_dependency(_table_name, determinant, dependent);
  EXPECT_TRUE(dependency_exists);
}

}  // namespace opossum