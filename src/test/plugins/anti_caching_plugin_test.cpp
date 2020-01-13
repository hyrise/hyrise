#include <chrono>
#include <numeric>
#include <thread>

#include "base_test.hpp"
#include "gtest/gtest.h"

#define private public
#include "../../plugins/anti_caching_plugin.hpp"
#undef private
#include "../utils/plugin_test_utils.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "utils/load_table.hpp"

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

TEST_F(AntiCachingPluginTest, FetchCurrentStatisticsEmpty) {
  auto plugin = AntiCachingPlugin();
  auto statistics = plugin._fetch_current_statistics();
  ASSERT_TRUE(statistics.empty());
}

TEST_F(AntiCachingPluginTest, FetchCurrentStatistics) {
  auto plugin = AntiCachingPlugin();
  const auto& table = load_table("resources/test_data/tbl/int_equal_distribution.tbl", 3);
  Hyrise::get().storage_manager.add_table("test_table", table);
  auto statistics = plugin._fetch_current_statistics();
  ASSERT_EQ(statistics.size(), 240);
}

TEST_F(AntiCachingPluginTest, EvaluateStatisticsNoEviction) {
  auto plugin = AntiCachingPlugin();
  const auto& table = load_table("resources/test_data/tbl/int_equal_distribution.tbl", 3);
  Hyrise::get().storage_manager.add_table("test_table", table);
  plugin._evaluate_statistics();
}

TEST_F(AntiCachingPluginTest, EvaluateStatisticsWithEviction) {
  auto plugin = AntiCachingPlugin();
  auto table = load_table("resources/test_data/tbl/int_equal_distribution.tbl", 3);
  Hyrise::get().storage_manager.add_table("int_equal_distribution", table);
  table = load_table("resources/test_data/tbl/int_string_like_not_equals.tbl", 3);
  Hyrise::get().storage_manager.add_table("int_string_like_not_equals", table);
  plugin.memory_budget = 2500;
  plugin._evaluate_statistics();
}

TEST_F(AntiCachingPluginTest, EvictSegments) {
  auto plugin = AntiCachingPlugin();
  plugin._evict_segments();
}

}  // namespace opossum
