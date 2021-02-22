#include "base_test.hpp"

#include "hyrise.hpp"

#include "./plugin_test_utils.hpp"

namespace opossum {

class PluginManagerTest : public BaseTest {
 protected:
  std::unordered_map<PluginName, PluginHandleWrapper>& get_plugins() { return _hyrise_env->plugin_manager()->_plugins; }

  void call_clean_up() { _hyrise_env->plugin_manager()->_clean_up(); }
};

TEST_F(PluginManagerTest, LoadUnloadPlugin) {
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  _hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseTestPlugin"));

  EXPECT_EQ(plugins.count("hyriseTestPlugin"), 1u);
  EXPECT_EQ(plugins["hyriseTestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["hyriseTestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["hyriseTestPlugin"].plugin, nullptr);

  // The test plugin creates a dummy table when it is started
  EXPECT_TRUE(_hyrise_env->storage_manager()->has_table("DummyTable"));

  _hyrise_env->plugin_manager()->unload_plugin("hyriseTestPlugin");

  // The test plugin removes the dummy table from the storage manager when it is unloaded
  EXPECT_FALSE(_hyrise_env->storage_manager()->has_table("DummyTable"));
  EXPECT_EQ(plugins.count("hyriseTestPlugin"), 0u);
}

// Plugins are unloaded when the PluginManager's destructor is called, this is simulated and tested here.
TEST_F(PluginManagerTest, LoadPluginAutomaticUnload) {
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  _hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseTestPlugin"));

  EXPECT_EQ(plugins.count("hyriseTestPlugin"), 1u);
  EXPECT_EQ(plugins["hyriseTestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["hyriseTestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["hyriseTestPlugin"].plugin, nullptr);

  // The test plugin creates a dummy table when it is started
  EXPECT_TRUE(_hyrise_env->storage_manager()->has_table("DummyTable"));

  // The PluginManager's destructor calls _clean_up(), we call it here explicitly to simulate the destructor
  // being called, which in turn should unload all loaded plugins.
  call_clean_up();

  // The test plugin removes the dummy table from the storage manager when it is unloaded
  // (implicitly by the destructor of the PluginManager).
  EXPECT_FALSE(_hyrise_env->storage_manager()->has_table("DummyTable"));
}

TEST_F(PluginManagerTest, LoadingSameName) {
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  _hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseTestPlugin"));

  EXPECT_THROW(_hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseTestPlugin")), std::exception);
}

TEST_F(PluginManagerTest, LoadingNotExistingLibrary) {
  EXPECT_THROW(_hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libNotExisting")), std::exception);
}

TEST_F(PluginManagerTest, LoadingNonInstantiableLibrary) {
  EXPECT_THROW(_hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseTestNonInstantiablePlugin")),
               std::exception);
}

TEST_F(PluginManagerTest, LoadingDifferentPlugins) {
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  _hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseTestPlugin"));
  _hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseMvccDeletePlugin"));
  EXPECT_EQ(plugins.size(), 2u);
}

TEST_F(PluginManagerTest, LoadingTwoInstancesOfSamePlugin) {
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  _hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseTestPlugin"));
  EXPECT_THROW(_hyrise_env->plugin_manager()->load_plugin(build_dylib_path("libhyriseTestPlugin")), std::exception);
}

TEST_F(PluginManagerTest, UnloadNotLoadedPlugin) {
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);

  EXPECT_THROW(_hyrise_env->plugin_manager()->unload_plugin("NotLoadedPlugin"), std::exception);
}

}  // namespace opossum
