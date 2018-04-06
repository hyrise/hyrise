#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/update_node.hpp"

namespace opossum {

class UpdateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    std::vector<LQPExpressionSPtr> update_expressions;
    _mock_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
    _update_node = UpdateNode::make("table_a", update_expressions, _mock_node);
  }

  UpdateNodeSPtr _update_node;
  MockNodeSPtr _mock_node;
};

TEST_F(UpdateNodeTest, Description) { EXPECT_EQ(_update_node->description(), "[Update] Table: 'table_a'"); }

TEST_F(UpdateNodeTest, TableName) { EXPECT_EQ(_update_node->table_name(), "table_a"); }

TEST_F(UpdateNodeTest, ShallowEquals) {
  EXPECT_TRUE(_update_node->shallow_equals(*_update_node));

  const auto other_update_node_a =
      UpdateNode::make("table_a", std::vector<LQPExpressionSPtr>{}, _mock_node);
  const auto other_update_node_b =
      UpdateNode::make("table_b", std::vector<LQPExpressionSPtr>{}, _mock_node);

  std::vector<LQPExpressionSPtr> update_expressions;
  update_expressions.emplace_back(LQPExpression::create_literal(5));
  const auto other_update_node_c = UpdateNode::make("table_a", update_expressions, _mock_node);

  EXPECT_TRUE(other_update_node_a->shallow_equals(*_update_node));
  EXPECT_FALSE(other_update_node_b->shallow_equals(*_update_node));
  EXPECT_FALSE(other_update_node_c->shallow_equals(*_update_node));
}

}  // namespace opossum
