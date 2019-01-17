#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/update_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class UpdateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
    _update_node = UpdateNode::make("table_a", _mock_node, _mock_node);
  }

  std::shared_ptr<UpdateNode> _update_node;
  std::shared_ptr<MockNode> _mock_node;
};

TEST_F(UpdateNodeTest, Description) { EXPECT_EQ(_update_node->description(), "[Update] Table: 'table_a'"); }

TEST_F(UpdateNodeTest, TableName) { EXPECT_EQ(_update_node->table_name, "table_a"); }

TEST_F(UpdateNodeTest, Equals) {
  EXPECT_EQ(*_update_node, *_update_node);

  const auto other_update_node_a = UpdateNode::make("table_a", _mock_node, _mock_node);
  const auto other_update_node_b = UpdateNode::make("table_b", _mock_node, _mock_node);

  EXPECT_EQ(*_update_node, *other_update_node_a);
  EXPECT_NE(*_update_node, *other_update_node_b);
}

TEST_F(UpdateNodeTest, Copy) { EXPECT_EQ(*_update_node->deep_copy(), *_update_node); }

TEST_F(UpdateNodeTest, NodeExpressions) { ASSERT_TRUE(_update_node->node_expressions.empty()); }

}  // namespace opossum
