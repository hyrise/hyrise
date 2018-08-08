#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "utils/plugin_manager.hpp"

namespace opossum {

class PluginManagerTest : public BaseTest {
 protected:
  const std::unordered_map<PluginName, PluginHandleWrapper> get_plugins() {
    auto &pm = PluginManager::get();
    
    return pm._plugins;
  }
};

TEST_F(PluginManagerTest, LoadPlugin) {
  auto &pm = PluginManager::get();
  auto plugins = get_plugins();
  
  EXPECT_EQ(plugins.size(), 0u);
  pm.load_plugin(std::string(LIB_DIR) + std::string("libTestPlugin.dylib"), "TestPlugin");
  
  plugins = get_plugins();
  EXPECT_EQ(plugins.count("TestPlugin"), 1u);
  EXPECT_EQ(plugins["TestPlugin"].plugin->description(), "This is the Hyrise TestPlugin");
  EXPECT_NE(plugins["TestPlugin"].handle, nullptr);
  EXPECT_NE(plugins["TestPlugin"].plugin, nullptr);
}

}  // namespace opossum
