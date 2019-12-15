#include "../base_test.hpp"

#include "hyrise.hpp"

#include "./plugin_test_utils.hpp"

namespace opossum {

class PluginManagerTest : public BaseTest {
 protected:
  std::unordered_map<PluginName, PluginHandleWrapper>& get_plugins() {
    auto& pm = Hyrise::get().plugin_manager;

    return pm._plugins;
  }

  void call_clean_up() {
    auto& pm = Hyrise::get().plugin_manager;
    pm._clean_up();
  }
};

TEST_F(PluginManagerTest, LoadUnloadPlugin) {
  auto& sm = Hyrise::get().storage_manager;
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));

  EXPECT_EQ(plugins.count("hyriseTestPlugin"), 1u);
  EXPECT_EQ(plugins["hyriseTestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["hyriseTestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["hyriseTestPlugin"].plugin, nullptr);

  // The test plugin creates a dummy table when it is started
  EXPECT_TRUE(sm.has_table("DummyTable"));

  pm.unload_plugin("hyriseTestPlugin");

  // The test plugin removes the dummy table from the storage manager when it is unloaded
  EXPECT_FALSE(sm.has_table("DummyTable"));
  EXPECT_EQ(plugins.count("hyriseTestPlugin"), 0u);
}

// Plugins are unloaded when the PluginManager's destructor is called, this is simulated and tested here.
TEST_F(PluginManagerTest, LoadPluginAutomaticUnload) {
  auto& sm = Hyrise::get().storage_manager;
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));

  EXPECT_EQ(plugins.count("hyriseTestPlugin"), 1u);
  EXPECT_EQ(plugins["hyriseTestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["hyriseTestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["hyriseTestPlugin"].plugin, nullptr);

  // The test plugin creates a dummy table when it is started
  EXPECT_TRUE(sm.has_table("DummyTable"));

  // The PluginManager's destructor calls _clean_up(), we call it here explicitly to simulate the destructor
  // being called, which in turn should unload all loaded plugins.
  call_clean_up();

  // The test plugin removes the dummy table from the storage manager when it is unloaded
  // (implicitly by the destructor of the PluginManager).
  EXPECT_FALSE(sm.has_table("DummyTable"));
}

TEST_F(PluginManagerTest, LoadingSameName) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));

  EXPECT_THROW(pm.load_plugin(build_dylib_path("libhyriseTestPlugin")), std::exception);
}

TEST_F(PluginManagerTest, LoadingNotExistingLibrary) {
  auto& pm = Hyrise::get().plugin_manager;

  EXPECT_THROW(pm.load_plugin(build_dylib_path("libNotExisting")), std::exception);
}

TEST_F(PluginManagerTest, LoadingNonInstantiableLibrary) {
  auto& pm = Hyrise::get().plugin_manager;

  EXPECT_THROW(pm.load_plugin(build_dylib_path("libhyriseTestNonInstantiablePlugin")), std::exception);
}

TEST_F(PluginManagerTest, LoadingTwoInstancesOfSamePlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  EXPECT_THROW(pm.load_plugin(build_dylib_path("libhyriseTestPlugin")), std::exception);
}

TEST_F(PluginManagerTest, UnloadNotLoadedPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);

  EXPECT_THROW(pm.unload_plugin("NotLoadedPlugin"), std::exception);
}

}  // namespace opossum
