#include "base_test.hpp"
#include "hyrise.hpp"
#include "utils/memory_resource_manager.hpp"

namespace opossum {

class MemoryResourceManagerTest : public BaseTest {
 protected:
  MemoryResourceManager memory_resource_manager;
};

TEST_F(MemoryResourceManagerTest, GetMemoryResources) {
    ASSERT_EQ(memory_resource_manager.memory_resources().size(), 0);

    const auto memory_resource_ptr_1 = memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure");
    ASSERT_EQ(memory_resource_manager.memory_resources().size(), 1);
    const auto resource_record_1 = memory_resource_manager.memory_resources()[0];
    EXPECT_EQ(resource_record_1.operator_type, OperatorType::Mock);
    EXPECT_EQ(resource_record_1.operator_data_structure, "my_data_structure");
    EXPECT_EQ(resource_record_1.resource_ptr, memory_resource_ptr_1);

    const auto memory_resource_ptr_2 = memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure_2");
    ASSERT_EQ(memory_resource_manager.memory_resources().size(), 2);
    const auto resource_record_2 = memory_resource_manager.memory_resources()[1];
    EXPECT_EQ(resource_record_2.operator_type, OperatorType::Mock);
    EXPECT_EQ(resource_record_2.operator_data_structure, "my_data_structure_2");
    EXPECT_EQ(resource_record_2.resource_ptr, memory_resource_ptr_2);
}

TEST_F(MemoryResourceManagerTest, GetMemoryResourceForSamePurposeMultipleTimes) {
    const auto memory_resource_ptr_1 = memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure");
    ASSERT_EQ(memory_resource_manager.memory_resources().size(), 1);

    const auto memory_resource_ptr_2 = memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure");
    ASSERT_EQ(memory_resource_manager.memory_resources().size(), 2);

    // we expect 
    ASSERT_NE(memory_resource_ptr_1, memory_resource_ptr_2);
}

}  // namespace opossum