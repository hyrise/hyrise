#include <memory>

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
#include "logical_query_plan/show_columns_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ShowColumnsNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { _show_columns_node = ShowColumnsNode::make("table_a"); }

  std::shared_ptr<ShowColumnsNode> _show_columns_node;
};

TEST_F(ShowColumnsNodeTest, Columns) {
  ASSERT_EQ(_show_columns_node->column_expressions().size(), 3u);
  EXPECT_EQ(*_show_columns_node->column_expressions().at(0), *lqp_column_({_show_columns_node, ColumnID{0}}));
  EXPECT_EQ(*_show_columns_node->column_expressions().at(1), *lqp_column_({_show_columns_node, ColumnID{1}}));
  EXPECT_EQ(*_show_columns_node->column_expressions().at(2), *lqp_column_({_show_columns_node, ColumnID{2}}));
}

TEST_F(ShowColumnsNodeTest, Description) {
  EXPECT_EQ(_show_columns_node->description(), "[ShowColumns] Table: 'table_a'");
}

TEST_F(ShowColumnsNodeTest, TableName) { EXPECT_EQ(_show_columns_node->table_name, "table_a"); }

TEST_F(ShowColumnsNodeTest, Equals) {
  EXPECT_EQ(*_show_columns_node, *_show_columns_node);

  const auto other_show_columns_node = ShowColumnsNode::make("table_b");
  EXPECT_NE(*_show_columns_node, *other_show_columns_node);
}

TEST_F(ShowColumnsNodeTest, Copy) { EXPECT_EQ(*_show_columns_node->deep_copy(), *_show_columns_node); }

TEST_F(ShowColumnsNodeTest, NodeExpressions) { ASSERT_EQ(_show_columns_node->node_expressions.size(), 0u); }

}  // namespace opossum
