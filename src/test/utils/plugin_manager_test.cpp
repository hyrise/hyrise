#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "storage/storage_manager.hpp"
#include "utils/plugin_manager.hpp"

#include "./plugin_test_utils.hpp"

namespace opossum {

class PluginManagerTest : public BaseTest {
 protected:
  std::unordered_map<PluginName, PluginHandleWrapper>& get_plugins() {
    auto& pm = PluginManager::get();

    return pm._plugins;
  }

  void call_clean_up() {
    auto& pm = PluginManager::get();
    pm._clean_up();
  }
};

TEST_F(PluginManagerTest, LoadStopPlugin) {
  auto& sm = StorageManager::get();
  auto& pm = PluginManager::get();
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libTestPlugin"));

  EXPECT_EQ(plugins.count("TestPlugin"), 1u);
  EXPECT_EQ(plugins["TestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["TestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["TestPlugin"].plugin, nullptr);

  // The test plugin creates a dummy table when it is started
  EXPECT_TRUE(sm.has_table("DummyTable"));

  pm.unload_plugin("TestPlugin");

  // The test plugin removes the dummy table from the storage manager when it is stopped
  EXPECT_FALSE(sm.has_table("DummyTable"));
  EXPECT_EQ(plugins.count("TestPlugin"), 0u);
}

// Plugins are stopped when the PluginManager's destructor is called, this is simulated and tested here.
TEST_F(PluginManagerTest, LoadPluginAutomaticStop) {
  auto& sm = StorageManager::get();
  auto& pm = PluginManager::get();
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libTestPlugin"));

  EXPECT_EQ(plugins.count("TestPlugin"), 1u);
  EXPECT_EQ(plugins["TestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["TestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["TestPlugin"].plugin, nullptr);

  // The test plugin creates a dummy table when it is started
  EXPECT_TRUE(sm.has_table("DummyTable"));

  // The PluginManager's destructor calls _clean_up(), we call it here explicitly to simulate the destructor
  // being called, which in turn should call stop() on all loaded plugins.
  call_clean_up();

  // The test plugin removes the dummy table from the storage manager when it is stopped
  // (implicitly by the destructor of the PluginManager).
  EXPECT_FALSE(sm.has_table("DummyTable"));
}

TEST_F(PluginManagerTest, LoadingSameName) {
  auto& pm = PluginManager::get();
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libTestPlugin"));

  EXPECT_THROW(pm.load_plugin(build_dylib_path("libTestPlugin")), std::exception);
}

TEST_F(PluginManagerTest, LoadingNotExistingLibrary) {
  auto& pm = PluginManager::get();

  EXPECT_THROW(pm.load_plugin(build_dylib_path("libNotExisting")), std::exception);
}

TEST_F(PluginManagerTest, LoadingNonInstantiableLibrary) {
  auto& pm = PluginManager::get();

  EXPECT_THROW(pm.load_plugin(build_dylib_path("libTestNonInstantiablePlugin")), std::exception);
}

TEST_F(PluginManagerTest, LoadingTwoInstancesOfSamePlugin) {
  auto& pm = PluginManager::get();
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libTestPlugin"));
  EXPECT_THROW(pm.load_plugin(build_dylib_path("libTestPlugin")), std::exception);
}

}  // namespace opossum
