#include "base_test.hpp"
#include "hyrise.hpp"
#include "utils/memory_resource_manager.hpp"
#include "../../plugins/memory_tracking_plugin.hpp"

namespace opossum {

class MemoryTrackingPluginTest : public BaseTest {
 protected:
  const MemoryTrackingPlugin _plugin{};
  MemoryResourceManager* _memory_resource_manager;
  void SetUp() override {
    _memory_resource_manager = &Hyrise::get().memory_resource_manager;
  }
};

TEST_F(MemoryTrackingPluginTest, EnableAndDisableNoAllocations) {
  // Tracking should be disabled by default.
  ASSERT_FALSE(_plugin.is_enabled());
  ASSERT_FALSE(_memory_resource_manager->tracking_is_enabled());
  _plugin.enable();
  ASSERT_TRUE(_plugin.is_enabled());
  ASSERT_TRUE(_memory_resource_manager->tracking_is_enabled());
  _plugin.disable();
  ASSERT_FALSE(_plugin.is_enabled());
  ASSERT_FALSE(_memory_resource_manager->tracking_is_enabled());
  _plugin.enable();
  ASSERT_TRUE(_plugin.is_enabled());
  ASSERT_TRUE(_memory_resource_manager->tracking_is_enabled());
}

TEST_F(MemoryTrackingPluginTest, AllocationsTrackedAfterEnable) {
  // Allocations should not be tracked before the enable function is called.
  const auto mem_resource_1 = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "test_data_structure");
  mem_resource_1->allocate(10);
  mem_resource_1->allocate(20);
  ASSERT_EQ(_memory_resource_manager->memory_resources().size(), 0);

  // After tracking is enabled, allocations should be tracked.
  _plugin.enable();
  const auto mem_resource_2 = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "test_data_structure");
  mem_resource_2->allocate(10);
  mem_resource_2->allocate(20);
  ASSERT_EQ(_memory_resource_manager->memory_resources().size(), 1);
}

TEST_F(MemoryTrackingPluginTest, Cleanup) {
  _plugin.enable();
  const auto mem_resource_1 = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "test_data_structure");
  const auto mem_resource_2 = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "test_data_structure");
  mem_resource_1->allocate(10);
  mem_resource_1->allocate(20);
  mem_resource_2->allocate(30);

  ASSERT_EQ(_memory_resource_manager->memory_resources().size(), 2);

  _plugin.cleanup();
  ASSERT_EQ(_memory_resource_manager->memory_resources().size(), 0);
}

TEST_F(MemoryTrackingPluginTest, DisablePerformsCleanup) {
  _plugin.enable();
  const auto mem_resource = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "test_data_structure");
  mem_resource->allocate(10);

  ASSERT_EQ(_memory_resource_manager->memory_resources().size(), 1);

  _plugin.disable();
  ASSERT_EQ(_memory_resource_manager->memory_resources().size(), 0);
  ASSERT_EQ(_memory_resource_manager->memory_resources().capacity(), 0);
}

}  // namespace opossum
