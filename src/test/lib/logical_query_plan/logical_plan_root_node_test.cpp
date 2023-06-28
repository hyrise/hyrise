#include "base_test.hpp"

#include "logical_query_plan/logical_plan_root_node.hpp"

namespace hyrise {

class LogicalPlanRootNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _logical_plan_root_node = LogicalPlanRootNode::make();
  }

  std::shared_ptr<LogicalPlanRootNode> _logical_plan_root_node;
};

TEST_F(LogicalPlanRootNodeTest, UniqueColumnCombinations) {
  EXPECT_THROW(_logical_plan_root_node->unique_column_combinations(), std::logic_error);
}

TEST_F(LogicalPlanRootNodeTest, NonTrivialFunctionalDependencies) {
  EXPECT_THROW(_logical_plan_root_node->non_trivial_functional_dependencies(), std::logic_error);
}

TEST_F(LogicalPlanRootNodeTest, Description) {
  EXPECT_EQ(_logical_plan_root_node->description(), "[LogicalPlanRootNode]");
}

TEST_F(LogicalPlanRootNodeTest, Copy) {
  EXPECT_EQ(*_logical_plan_root_node, *_logical_plan_root_node->deep_copy());
  EXPECT_EQ(_logical_plan_root_node->hash(), _logical_plan_root_node->hash());
}
}  // namespace hyrise
