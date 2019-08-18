#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "hyrise.hpp"
#include "utils/singleton.hpp"

#include "../../plugins/test_plugin.hpp"
#include "./plugin_test_utils.hpp"

namespace opossum {

class SingletonTest : public BaseTest {
 protected:
  std::unordered_map<PluginName, PluginHandleWrapper>& get_plugins() {
    auto& pm = Hyrise::get().plugin_manager;

    return pm._plugins;
  }
};

TEST_F(SingletonTest, SingleInstance) {
  auto& a = Singleton<int>::get();
  auto& b = Singleton<int>::get();

  EXPECT_EQ(&a, &b);
}

// ASAN cannot handle the not yet defined (because it's a dynamic library) typeinfo for TestPlugin.
// Therefore, this test is only built if ASAN is not activated.
#if defined(__has_feature)
#if !__has_feature(address_sanitizer)
// This test case should validate that there is only a single instance of a singleton when it is accessed from two
// different translation units, i.e., a plugin and the test itself in this case.
TEST_F(SingletonTest, SingleInstanceAcrossTranslationUnits) {
  auto& sm = Hyrise::get().storage_manager;
  auto& pm = Hyrise::get().plugin_manager;

  // The TestPlugin also holds a reference to the StorageManager.
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  auto& plugins = get_plugins();

  auto test_plugin = static_cast<TestPlugin*>(plugins["hyriseTestPlugin"].plugin);

  EXPECT_EQ(&sm, &test_plugin->sm);
}
#endif
#endif

}  // namespace opossum
