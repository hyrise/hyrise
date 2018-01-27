#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "abstract_expression.hpp"
#include "operators/pqp_expression.hpp"
#include "storage/table.hpp"

namespace opossum {

class ExpressionTest : public BaseTest {
 protected:
  std::vector<std::string> _column_names = {"a", "b", "c", "d"};
};

TEST_F(ExpressionTest, ExpressionToStringColumn) {
  const auto expr = PQPExpression::create_column(ColumnID{1});
  EXPECT_EQ(expr->to_string(_column_names), "b");
}

TEST_F(ExpressionTest, ExpressionToStringSimpleArithmetics) {
  auto expr = PQPExpression::create_binary_operator(
      ExpressionType::Subtraction, PQPExpression::create_column(ColumnID{0}), PQPExpression::create_literal(4));
  EXPECT_EQ(expr->to_string(_column_names), "a - 4");
}

TEST_F(ExpressionTest, ExpressionToStringNestedArithmetics) {
  auto expr = PQPExpression::create_binary_operator(
      ExpressionType::Multiplication,
      PQPExpression::create_binary_operator(ExpressionType::Subtraction, PQPExpression::create_column(ColumnID{2}),
                                            PQPExpression::create_literal(4)),
      PQPExpression::create_binary_operator(ExpressionType::Multiplication, PQPExpression::create_column(ColumnID{3}),
                                            PQPExpression::create_literal("c")));
  EXPECT_EQ(expr->to_string(_column_names), "(c - 4) * (d * \"c\")");
}

TEST_F(ExpressionTest, ExpressionToStringNestedLogical) {
  auto expr = PQPExpression::create_unary_operator(
      ExpressionType::Not,
      PQPExpression::create_binary_operator(
          ExpressionType::And, PQPExpression::create_binary_operator(ExpressionType::GreaterThanEquals,
                                                                     PQPExpression::create_column(ColumnID{2}),
                                                                     PQPExpression::create_literal(4)),
          PQPExpression::create_binary_operator(ExpressionType::Or, PQPExpression::create_column(ColumnID{3}),
                                                PQPExpression::create_literal(true))));
  EXPECT_EQ(expr->to_string(_column_names), "NOT ((c >= 4) AND (d OR 1))");
}

}  // namespace opossum
