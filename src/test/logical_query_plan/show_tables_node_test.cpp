#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
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
#include "expression/placeholder_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/show_tables_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ShowTablesNodeTest : public BaseTest {
 protected:
  void SetUp() override { _show_tables_node = ShowTablesNode::make(); }

  std::shared_ptr<ShowTablesNode> _show_tables_node;
};

TEST_F(ShowTablesNodeTest, Columns) {
  ASSERT_EQ(_show_tables_node->column_expressions().size(), 1u);
  EXPECT_EQ(*_show_tables_node->column_expressions().at(0), *lqp_column_({_show_tables_node, ColumnID{0}}));
}

TEST_F(ShowTablesNodeTest, Description) { EXPECT_EQ(_show_tables_node->description(), "[ShowTables]"); }
TEST_F(ShowTablesNodeTest, Equals) { EXPECT_EQ(*_show_tables_node, *_show_tables_node); }
TEST_F(ShowTablesNodeTest, Copy) { EXPECT_EQ(*_show_tables_node->deep_copy(), *_show_tables_node); }

TEST_F(ShowTablesNodeTest, NodeExpressions) { ASSERT_EQ(_show_tables_node->node_expressions.size(), 0u); }

}  // namespace opossum
