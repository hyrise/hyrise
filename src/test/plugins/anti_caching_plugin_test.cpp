#include <chrono>
#include <numeric>
#include <thread>

#include "base_test.hpp"
#include "gtest/gtest.h"

#define private public
#include "../../plugins/anti_caching_plugin.hpp"
#undef private
#include "../utils/plugin_test_utils.hpp"

namespace opossum {

class AntiCachingPluginTest : public BaseTest {
 public:
  static void SetUpTestCase() {}

  void SetUp() override { }

  void TearDown() override { Hyrise::reset(); }

 protected:
};


TEST_F(AntiCachingPluginTest, LoadUnloadPlugin) {
  auto& pm = Hyrise::get().plugin_manager;
  pm.load_plugin(build_dylib_path("libAntiCachingPlugin"));
  pm.unload_plugin("AntiCachingPlugin");
}

TEST_F(AntiCachingPluginTest, ConstructPlugin) {
  auto plugin = AntiCachingPlugin();
  plugin.memory_budget = 5;
  std::cout << plugin.memory_budget << std::endl;
  plugin._evict_segments();
}

}  // namespace opossum
