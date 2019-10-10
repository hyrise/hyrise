#include "gtest/gtest.h"

#include "logical_query_plan/create_prepared_plan_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "storage/prepared_plan.hpp"

namespace opossum {

class CreatePreparedPlanNodeTest : public ::testing::Test {
 public:
  void SetUp() override {
    lqp = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
    prepared_plan = std::make_shared<PreparedPlan>(lqp, std::vector<ParameterID>{});
    create_prepared_plan_node = CreatePreparedPlanNode::make("some_prepared_plan", prepared_plan);
  }

  std::shared_ptr<CreatePreparedPlanNode> create_prepared_plan_node;
  std::shared_ptr<PreparedPlan> prepared_plan;
  std::shared_ptr<MockNode> lqp;
};

TEST_F(CreatePreparedPlanNodeTest, Description) {
  EXPECT_EQ(create_prepared_plan_node->description(),
            R"([CreatePreparedPlan] 'some_prepared_plan' {
ParameterIDs: []
[0] [MockNode 'Unnamed'] Columns: a | pruned: 0/1 columns
})");
}

TEST_F(CreatePreparedPlanNodeTest, HashingAndEqualityCheck) {
  const auto deep_copied_node = create_prepared_plan_node->deep_copy();
  EXPECT_EQ(*create_prepared_plan_node, *deep_copied_node);

  const auto different_prepared_plan_node_a = CreatePreparedPlanNode::make("some_prepared_plan2", prepared_plan);

  const auto different_lqp = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "b"}}));
  const auto different_prepared_plan =
      std::make_shared<PreparedPlan>(different_lqp, std::vector<ParameterID>{ParameterID{1}});
  const auto different_prepared_plan_node_b =
      CreatePreparedPlanNode::make("some_prepared_plan", different_prepared_plan);

  EXPECT_NE(*different_prepared_plan_node_a, *create_prepared_plan_node);
  EXPECT_NE(*different_prepared_plan_node_b, *create_prepared_plan_node);

  EXPECT_NE(different_prepared_plan_node_a->hash(), create_prepared_plan_node->hash());
  EXPECT_NE(different_prepared_plan_node_b->hash(), create_prepared_plan_node->hash());
}

TEST_F(CreatePreparedPlanNodeTest, Copy) {
  EXPECT_EQ(*create_prepared_plan_node, *create_prepared_plan_node->deep_copy());
}

TEST_F(CreatePreparedPlanNodeTest, NodeExpressions) {
  ASSERT_EQ(create_prepared_plan_node->node_expressions.size(), 0u);
}

}  // namespace opossum
