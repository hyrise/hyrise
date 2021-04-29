#include <vector>

#include "base_test.hpp"
#include "lib/utils/plugin_test_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"
#include "utils/plugin_manager.hpp"

#include "../../plugins/functional_dependency_plugin.hpp"

namespace opossum {

class FunctionalDependencyPluginTest : public BaseTest {

  void SetUp() override {
    const auto& table = load_table("resources/test_data/tbl/int3.tbl", _chunk_size);
    Hyrise::get().storage_manager.add_table(_table_name, table);
  }

  void TearDown() override { Hyrise::reset(); }

protected:
  const std::string _table_name{"functionalDependencyTestTable"};
  static constexpr auto _chunk_size = size_t{4};
};

TEST_F(FunctionalDependencyPluginTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libhyriseFunctionalDependencyPlugin"));
  pm.unload_plugin("libhyriseFunctionalDependencyPlugin");
}

TEST_F(FunctionalDependencyPluginTest, CheckDependency) {
  const auto table = Hyrise::get().storage_manager.get_table(_table_name);
  const auto determinant = std::vector<ColumnID>{0};
  const auto dependent = std::vector<ColumnID>{1};
  EXPECT_TRUE(FunctionalDependencyPlugin::_check_dependency(table, determinant, dependent));
}

}  // namespace opossum