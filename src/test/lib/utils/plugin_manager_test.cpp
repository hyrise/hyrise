#include "nlohmann/json.hpp"

#include "base_test.hpp"
#include "hyrise.hpp"
#include "plugin_test_utils.hpp"
#include "tpch/tpch_benchmark_item_runner.hpp"

namespace hyrise {

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

  EXPECT_TRUE(plugins.contains("hyriseTestPlugin"));
  EXPECT_EQ(plugins["hyriseTestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["hyriseTestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["hyriseTestPlugin"].plugin, nullptr);

  // The test plugin creates a dummy table when it is started
  EXPECT_TRUE(sm.has_table("DummyTable"));

  pm.unload_plugin("hyriseTestPlugin");

  // The test plugin removes the dummy table from the storage manager when it is unloaded
  EXPECT_FALSE(sm.has_table("DummyTable"));
  EXPECT_FALSE(plugins.contains("hyriseTestPlugin"));
}

// Plugins are unloaded when the PluginManager's destructor is called, this is simulated and tested here.
TEST_F(PluginManagerTest, LoadPluginAutomaticUnload) {
  auto& sm = Hyrise::get().storage_manager;
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));

  EXPECT_TRUE(plugins.contains("hyriseTestPlugin"));
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

TEST_F(PluginManagerTest, LoadingUnloadingUserExecutableFunctions) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));
  EXPECT_EQ(plugins.size(), 2u);

  {
    auto user_executable_functions = pm.user_executable_functions();

    EXPECT_EQ(user_executable_functions.size(), 3);
    EXPECT_TRUE(user_executable_functions.contains({"hyriseSecondTestPlugin", "OurFreelyChoosableFunctionName"}));
    EXPECT_TRUE(user_executable_functions.contains({"hyriseTestPlugin", "OurFreelyChoosableFunctionName"}));
    EXPECT_TRUE(user_executable_functions.contains({"hyriseTestPlugin", "SpecialFunction17"}));
  }

  pm.unload_plugin("hyriseTestPlugin");
  EXPECT_EQ(pm.user_executable_functions().size(), 1);

  pm.unload_plugin("hyriseSecondTestPlugin");
  EXPECT_EQ(pm.user_executable_functions().size(), 0);
}

TEST_F(PluginManagerTest, CallUserExecutableFunctions) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& sm = Hyrise::get().storage_manager;
  auto& lm = Hyrise::get().log_manager;

  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));

  pm.exec_user_function("hyriseTestPlugin", "OurFreelyChoosableFunctionName");
  // The test plugin creates the below table when the called function is executed
  EXPECT_TRUE(sm.has_table("TableOfTestPlugin_0"));

  // The PluginManager adds log messages when user executable functions are called
  ASSERT_EQ(lm.log_entries().size(), 1);
  {
    const auto& entry = lm.log_entries()[0];
    EXPECT_EQ(entry.reporter, "PluginManager");
    EXPECT_EQ(
        entry.message,
        "Called user executable function 'OurFreelyChoosableFunctionName' provided by plugin 'hyriseTestPlugin'.");
    EXPECT_EQ(entry.log_level, LogLevel::Info);
  }

  pm.exec_user_function("hyriseSecondTestPlugin", "OurFreelyChoosableFunctionName");
  // The second test plugin creates the below table when the called function is executed
  EXPECT_TRUE(sm.has_table("TableOfSecondTestPlugin"));
  ASSERT_EQ(lm.log_entries().size(), 2);
  {
    const auto& entry = lm.log_entries()[1];
    EXPECT_EQ(entry.reporter, "PluginManager");
    EXPECT_EQ(entry.message,
              "Called user executable function 'OurFreelyChoosableFunctionName' provided by plugin "
              "'hyriseSecondTestPlugin'.");
    EXPECT_EQ(entry.log_level, LogLevel::Info);
  }
}

TEST_F(PluginManagerTest, CallNotCallableUserExecutableFunctions) {
  auto& pm = Hyrise::get().plugin_manager;

  // Call non-existing plugin (with non-existing function)
  EXPECT_THROW(pm.exec_user_function("hyriseUnknownPlugin", "OurFreelyChoosableFunctionName"), std::exception);

  // Call existing, loaded plugin but non-existing function
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));
  EXPECT_THROW(pm.exec_user_function("hyriseSecondTestPlugin", "SpecialFunction17"), std::exception);

  // Call function exposed by plugin but plugin has been unloaded before
  pm.unload_plugin("hyriseSecondTestPlugin");
  EXPECT_THROW(pm.exec_user_function("hyriseSecondTestPlugin", "OurFreelyChoosableFunctionName"), std::exception);
}

TEST_F(PluginManagerTest, LoadingUnloadingBenchmarkHooks) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));
  EXPECT_EQ(plugins.size(), 2u);

  {
    EXPECT_TRUE(pm.has_pre_benchmark_hook("hyriseTestPlugin"));
    EXPECT_TRUE(pm.has_post_benchmark_hook("hyriseTestPlugin"));
    EXPECT_FALSE(pm.has_pre_benchmark_hook("hyriseSecondTestPlugin"));
    EXPECT_FALSE(pm.has_post_benchmark_hook("hyriseSecondTestPlugin"));
  }

  pm.unload_plugin("hyriseTestPlugin");
  EXPECT_FALSE(pm.has_pre_benchmark_hook("hyriseTestPlugin"));
  EXPECT_FALSE(pm.has_post_benchmark_hook("hyriseTestPlugin"));

  pm.unload_plugin("hyriseSecondTestPlugin");
  EXPECT_FALSE(pm.has_pre_benchmark_hook("hyriseSecondTestPlugin"));
  EXPECT_FALSE(pm.has_post_benchmark_hook("hyriseSecondTestPlugin"));
}

TEST_F(PluginManagerTest, CallBenchmarkHooks) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& sm = Hyrise::get().storage_manager;
  auto& lm = Hyrise::get().log_manager;
  const std::unique_ptr<AbstractBenchmarkItemRunner> benchmark_item_runner = std::make_unique<TPCHBenchmarkItemRunner>(
      std::make_shared<BenchmarkConfig>(), false, 1, ClusteringConfiguration::None);

  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));

  pm.exec_pre_benchmark_hook("hyriseTestPlugin", *benchmark_item_runner);
  // The test plugin creates the below table when the hook is executed and adds a row for each benchmark item.
  EXPECT_TRUE(sm.has_table("BenchmarkItems"));
  EXPECT_EQ(sm.get_table("BenchmarkItems")->row_count(), 22);

  // The PluginManager adds log messages when benchmark hooks are called.
  ASSERT_EQ(lm.log_entries().size(), 1);
  {
    const auto& entry = lm.log_entries()[0];
    EXPECT_EQ(entry.reporter, "PluginManager");
    EXPECT_EQ(entry.message, "Called pre-benchmark hook provided by plugin 'hyriseTestPlugin'.");
    EXPECT_EQ(entry.log_level, LogLevel::Info);
  }

  auto report = nlohmann::json{};
  pm.exec_post_benchmark_hook("hyriseTestPlugin", report);
  // The test plugin drops the created table when the hook is executed.
  EXPECT_FALSE(sm.has_table("BenchmarkItems"));
  // Also, it adds a dummy entry to the report.
  EXPECT_EQ(report["dummy"], 1);

  // The PluginManager adds log messages when benchmark hooks are called.
  ASSERT_EQ(lm.log_entries().size(), 2);
  {
    const auto& entry = lm.log_entries()[1];
    EXPECT_EQ(entry.reporter, "PluginManager");
    EXPECT_EQ(entry.message, "Called post-benchmark hook provided by plugin 'hyriseTestPlugin'.");
    EXPECT_EQ(entry.log_level, LogLevel::Info);
  }
}

TEST_F(PluginManagerTest, CallNotExistingBenchmarkHooks) {
  auto& pm = Hyrise::get().plugin_manager;
  const std::unique_ptr<AbstractBenchmarkItemRunner> benchmark_item_runner = std::make_unique<TPCHBenchmarkItemRunner>(
      std::make_shared<BenchmarkConfig>(), false, 1, ClusteringConfiguration::None);
  auto report = nlohmann::json{};

  // Call non-existing plugin (with non-existing hooks).
  EXPECT_THROW(pm.exec_pre_benchmark_hook("hyriseUnknownPlugin", *benchmark_item_runner), std::logic_error);
  EXPECT_THROW(pm.exec_post_benchmark_hook("hyriseUnknownPlugin", report), std::logic_error);

  // Call existing, loaded plugin without hooks.
  pm.load_plugin(build_dylib_path("libhyriseSecondTestPlugin"));
  EXPECT_THROW(pm.exec_pre_benchmark_hook("libhyriseSecondTestPlugin", *benchmark_item_runner), std::logic_error);
  EXPECT_THROW(pm.exec_post_benchmark_hook("libhyriseSecondTestPlugin", report), std::logic_error);
  pm.unload_plugin("hyriseSecondTestPlugin");

  // Call hooks exposed by plugin but plugin has been unloaded before.
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  pm.unload_plugin("hyriseTestPlugin");
  EXPECT_THROW(pm.exec_pre_benchmark_hook("libhyriseTestPlugin", *benchmark_item_runner), std::logic_error);
  EXPECT_THROW(pm.exec_post_benchmark_hook("libhyriseTestPlugin", report), std::logic_error);
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

TEST_F(PluginManagerTest, LoadingDifferentPlugins) {
  auto& pm = Hyrise::get().plugin_manager;
  auto& plugins = get_plugins();

  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(build_dylib_path("libhyriseTestPlugin"));
  pm.load_plugin(build_dylib_path("libhyriseMvccDeletePlugin"));
  EXPECT_EQ(plugins.size(), 2u);
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

}  // namespace hyrise
