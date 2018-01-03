#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "expression.hpp"
#include "operators/operator_expression.hpp"
#include "storage/table.hpp"

namespace opossum {

class ExpressionTest : public BaseTest {
 protected:
  std::vector<std::string> _column_names = {"a", "b", "c", "d"};
};

TEST_F(ExpressionTest, ExpressionToStringColumn) {
  const auto expr = OperatorExpression::create_column(ColumnID{1});
  EXPECT_EQ(expr->to_string(_column_names), "b");
}

TEST_F(ExpressionTest, ExpressionToStringSimpleArithmetics) {
  auto expr = OperatorExpression::create_binary_operator(ExpressionType::Subtraction, OperatorExpression::create_column(ColumnID{0}),
                                                 OperatorExpression::create_literal(4));
  EXPECT_EQ(expr->to_string(_column_names), "a - 4");
}

TEST_F(ExpressionTest, ExpressionToStringNestedArithmetics) {
  auto expr = OperatorExpression::create_binary_operator(
      ExpressionType::Multiplication,
      OperatorExpression::create_binary_operator(ExpressionType::Subtraction, OperatorExpression::create_column(ColumnID{2}),
                                         OperatorExpression::create_literal(4)),
      OperatorExpression::create_binary_operator(ExpressionType::Multiplication, OperatorExpression::create_column(ColumnID{3}),
                                         OperatorExpression::create_literal("c")));
  EXPECT_EQ(expr->to_string(_column_names), "(c - 4) * (d * \"c\")");
}

TEST_F(ExpressionTest, ExpressionToStringNestedLogical) {
  auto expr = OperatorExpression::create_unary_operator(
      ExpressionType::Not,
      OperatorExpression::create_binary_operator(
          ExpressionType::And,
          OperatorExpression::create_binary_operator(ExpressionType::GreaterThanEquals, OperatorExpression::create_column(ColumnID{2}),
                                             OperatorExpression::create_literal(4)),
          OperatorExpression::create_binary_operator(ExpressionType::Or, OperatorExpression::create_column(ColumnID{3}),
                                             OperatorExpression::create_literal(true))));
  EXPECT_EQ(expr->to_string(_column_names), "NOT ((c >= 4) AND (d OR 1))");
}

}  // namespace opossum
