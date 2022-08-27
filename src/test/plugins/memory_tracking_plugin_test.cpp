#include "base_test.hpp"
#include "hyrise.hpp"
#include "utils/memory_resource_manager.hpp"
#include "plugins/memory_tracking_plugin.hpp"

namespace opossum {

class MemoryTrackingPluginTest : public BaseTest {
 protected:
  void TearDown() override {
    Hyrise::reset();
  }
};

TEST_F(MemoryTrackingPluginTest, EnableAndDisableWorks) {
  // tracking should be disabled by default
  ASSERT_FALSE(MemoryTrackingPlugin::is_enabled());
  MemoryTrackingPlugin::enable();
  ASSERT_TRUE(MemoryTrackingPlugin::is_enabled());
  MemoryTrackingPlugin::disable();
  ASSERT_FALSE(MemoryTrackingPlugin::is_enabled());
  MemoryTrackingPlugin::enable();
  ASSERT_TRUE(MemoryTrackingPlugin::is_enabled());
}

}  // namespace opossum
