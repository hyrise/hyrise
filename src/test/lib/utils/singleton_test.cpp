#include <gtest/gtest.h>

#include <unordered_map>

#include "base_test.hpp"
#include "hyrise.hpp"
#include "utils/plugin_manager.hpp"
#include "utils/singleton.hpp"

namespace hyrise {

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

}  // namespace hyrise
