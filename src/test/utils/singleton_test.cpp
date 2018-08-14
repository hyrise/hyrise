#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "storage/storage_manager.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/singleton.hpp"

#include "./plugin_test_utils.hpp"
#include "./test_plugin.hpp"

namespace opossum {

class SingletonTest : public BaseTest {
 protected:
  std::unordered_map<PluginName, PluginHandleWrapper>& get_plugins() {
    auto& pm = PluginManager::get();

    return pm._plugins;
  }
};

TEST_F(SingletonTest, SingleInstance) {
  auto& a = Singleton<int>::get();
  auto& b = Singleton<int>::get();

  EXPECT_EQ(a, b);
  EXPECT_EQ(&a, &b);
}

// This test case should validate that there is only a single instance of a singleton when it is accessed from two
// different translation units, i.e., a plugin and the test itself in this case.
TEST_F(SingletonTest, SingleInstanceAcrossTranslationUnits) {
  auto& sm = StorageManager::get();
  auto& pm = PluginManager::get();

  pm.load_plugin(build_dylib_path("libTestPlugin"), "TestPlugin");
  auto& plugins = get_plugins();

  auto test_plugin = static_cast<TestPlugin*>(plugins["TestPlugin"].plugin);

  EXPECT_EQ(&sm, &test_plugin->sm);
}

}  // namespace opossum
