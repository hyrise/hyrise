#include "base_test.hpp"
#include "hyrise.hpp"
#include "lib/utils/plugin_test_utils.hpp"
#include "utils/memory_resource_manager.hpp"
#include "../../plugins/memory_tracking_plugin.hpp"

namespace opossum {

class MemoryTrackingPluginSystemTest : public BaseTest {
 protected:
  MemoryResourceManager* _memory_resource_manager;
  void SetUp() override {
    _memory_resource_manager = &Hyrise::get().memory_resource_manager;

    // Load the memory tracking plugin.
    auto& pm = Hyrise::get().plugin_manager;
    pm.load_plugin(build_dylib_path("libhyriseMemoryTrackingPlugin"));
  }
  void TearDown() override {
    Hyrise::reset();
  }
};

TEST_F(MemoryTrackingPluginSystemTest, NoTrackingWhenDeactivated) {
  // The memory tracking meta table should start off empty.
  const auto table_0 = SQLPipelineBuilder("SELECT * FROM meta_temporary_memory_usage;")
    .create_pipeline().get_result_table().second;
  EXPECT_TRUE(table_0->empty());

  // Now, allocate some memory and verify that it is not tracked in the meta table. 
  const auto mem_resource = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "my_data_structure");
  mem_resource->allocate(20);
  const auto table_1 = SQLPipelineBuilder("SELECT * FROM meta_temporary_memory_usage;")
    .create_pipeline().get_result_table().second;
  EXPECT_TRUE(table_1->empty());
}

TEST_F(MemoryTrackingPluginSystemTest, FullFlowWithDeactivate) {
  // Activate the memory tracking.
  auto pipeline_0 = SQLPipelineBuilder("INSERT INTO meta_exec VALUES ('hyriseMemoryTrackingPlugin', 'enable');").create_pipeline();
  EXPECT_EQ(pipeline_0.get_result_table().first, SQLPipelineStatus::Success);

  // Allocate some memory and verify that it is tracked in the meta table.
  const auto mem_resource_0 = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "my_data_structure");
  mem_resource_0->allocate(30);
  const auto table_0 = SQLPipelineBuilder("SELECT * FROM meta_temporary_memory_usage;")
    .create_pipeline().get_result_table().second;
  EXPECT_FALSE(table_0->empty());

  // Deactivate the tracking. Verify that the meta table is now empty once again and that the memory manager does
  // not track any memory resources.
  auto pipeline_1 = SQLPipelineBuilder("INSERT INTO meta_exec VALUES ('hyriseMemoryTrackingPlugin', 'disable');")
    .create_pipeline();
  EXPECT_EQ(pipeline_1.get_result_table().first, SQLPipelineStatus::Success);
  const auto table_1 = SQLPipelineBuilder("SELECT * FROM meta_temporary_memory_usage;")
    .create_pipeline().get_result_table().second;
  EXPECT_TRUE(table_1->empty());
  EXPECT_TRUE(_memory_resource_manager->memory_resources().empty());

  // Allocate some memory and verify that it IS NOT tracked in the meta table. 
  const auto mem_resource_1 = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "my_data_structure");
  mem_resource_1->allocate(10);
  const auto table_2 = SQLPipelineBuilder("SELECT * FROM meta_temporary_memory_usage;")
    .create_pipeline().get_result_table().second;
  EXPECT_TRUE(table_2->empty());
}

TEST_F(MemoryTrackingPluginSystemTest, FullFlowWithCleanup) {
  // Activate the memory tracking.
  auto pipeline_0 = SQLPipelineBuilder("INSERT INTO meta_exec VALUES ('hyriseMemoryTrackingPlugin', 'enable');").create_pipeline();
  EXPECT_EQ(pipeline_0.get_result_table().first, SQLPipelineStatus::Success);

  // Allocate some memory and verify that it is tracked in the meta table.
  const auto mem_resource_0 = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "my_data_structure");
  mem_resource_0->allocate(30);
  const auto table_0 = SQLPipelineBuilder("SELECT * FROM meta_temporary_memory_usage;")
    .create_pipeline().get_result_table().second;
  EXPECT_FALSE(table_0->empty());

  // Call cleanup. Verify that the meta table is now empty once again and that the memory manager does not track any 
  // memory resources.
  auto pipeline_1 = SQLPipelineBuilder("INSERT INTO meta_exec VALUES ('hyriseMemoryTrackingPlugin', 'cleanup');")
    .create_pipeline();
  EXPECT_EQ(pipeline_1.get_result_table().first, SQLPipelineStatus::Success);
  const auto table_1 = SQLPipelineBuilder("SELECT * FROM meta_temporary_memory_usage;")
    .create_pipeline().get_result_table().second;
  EXPECT_TRUE(table_1->empty());
  EXPECT_TRUE(_memory_resource_manager->memory_resources().empty());

  // Allocate some memory and verify that it IS tracked in the meta table. 
  const auto mem_resource_1 = _memory_resource_manager->get_memory_resource(OperatorType::Mock, "my_data_structure");
  mem_resource_1->allocate(20);
  const auto table_2 = SQLPipelineBuilder("SELECT * FROM meta_temporary_memory_usage;")
    .create_pipeline().get_result_table().second;
  EXPECT_FALSE(table_2->empty());
}

}  // namespace opossum
