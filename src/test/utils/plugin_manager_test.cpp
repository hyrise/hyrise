#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "storage/storage_manager.hpp"
#include "utils/plugin_manager.hpp"

namespace opossum {

class PluginManagerTest : public BaseTest {
 protected:
  std::unordered_map<PluginName, PluginHandleWrapper>& get_plugins() {
    auto &pm = PluginManager::get();
    
    return pm._plugins;
  }

};

TEST_F(PluginManagerTest, LoadStopPlugin) {
  auto &sm = StorageManager::get();
  auto &pm = PluginManager::get();
  auto &plugins = get_plugins();
  
  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(std::string(LIB_DIR) + std::string("libTestPlugin.dylib"), "TestPlugin");
  
  EXPECT_EQ(plugins.count("TestPlugin"), 1u);
  EXPECT_EQ(plugins["TestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["TestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["TestPlugin"].plugin, nullptr);

  // // The test plugin creates a dummy table when it is started
  EXPECT_TRUE(sm.has_table("DummyTable"));

  pm.stop_plugin("TestPlugin");

  // // The test plugin removes the dummy table from the storage manager when it is stopped
  EXPECT_FALSE(sm.has_table("DummyTable"));
  EXPECT_EQ(plugins.count("TestPlugin"), 0u);
}

}  // namespace opossum
