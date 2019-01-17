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
#include "logical_query_plan/validate_node.hpp"

namespace opossum {

class ValidateNodeTest : public BaseTest {
 protected:
  void SetUp() override { _validate_node = ValidateNode::make(); }

  std::shared_ptr<ValidateNode> _validate_node;
};

TEST_F(ValidateNodeTest, Description) { EXPECT_EQ(_validate_node->description(), "[Validate]"); }

TEST_F(ValidateNodeTest, Equals) { EXPECT_EQ(*_validate_node, *_validate_node); }

TEST_F(ValidateNodeTest, Copy) { EXPECT_EQ(*_validate_node->deep_copy(), *_validate_node); }

TEST_F(ValidateNodeTest, NodeExpressions) { ASSERT_EQ(_validate_node->node_expressions.size(), 0u); }

}  // namespace opossum
