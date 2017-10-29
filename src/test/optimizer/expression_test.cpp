#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "optimizer/expression.hpp"
#include "storage/table.hpp"

namespace opossum {

class ExpressionTest : public BaseTest {
 protected:
  std::vector<std::string> _column_names = {"a", "b", "c", "d"};
};

TEST_F(ExpressionTest, ExpressionToStringColumn) {
  const auto expr = Expression::create_column(ColumnID{1});
  EXPECT_EQ(expr->to_string(_column_names), "b");
}

TEST_F(ExpressionTest, ExpressionToStringSimpleArithmetics) {
  auto expr = Expression::create_binary_operator(ExpressionType::Subtraction, Expression::create_column(ColumnID{0}),
                                                 Expression::create_literal(4));
  EXPECT_EQ(expr->to_string(_column_names), "a - 4");
}

TEST_F(ExpressionTest, ExpressionToStringNestedArithmetics) {
  auto expr = Expression::create_binary_operator(
      ExpressionType::Multiplication,
      Expression::create_binary_operator(ExpressionType::Subtraction, Expression::create_column(ColumnID{2}),
                                         Expression::create_literal(4)),
      Expression::create_binary_operator(ExpressionType::Multiplication, Expression::create_column(ColumnID{3}),
                                         Expression::create_literal("c")));
  EXPECT_EQ(expr->to_string(_column_names), "(c - 4) * (d * \"c\")");
}

TEST_F(ExpressionTest, ExpressionToStringNestedLogical) {
  auto expr = Expression::create_unary_operator(
      ExpressionType::Not,
      Expression::create_binary_operator(
          ExpressionType::And,
          Expression::create_binary_operator(ExpressionType::GreaterThanEquals, Expression::create_column(ColumnID{2}),
                                             Expression::create_literal(4)),
          Expression::create_binary_operator(ExpressionType::Or, Expression::create_column(ColumnID{3}),
                                             Expression::create_literal(true))));
  EXPECT_EQ(expr->to_string(_column_names), "NOT ((c >= 4) AND (d OR 1))");
}


TEST_F(ExpressionTest, MapColumnIDs) {
  auto a = Expression::create_column(ColumnID{0});
  auto c = Expression::create_column(ColumnID{2});
  auto d = Expression::create_column(ColumnID{3});

  // (c >= (4 - a)) AND (SUM(d) > 1)
  auto expr = Expression::create_binary_operator(
  ExpressionType::And,
  Expression::create_binary_operator(
  ExpressionType::GreaterThanEquals,
  c,
  Expression::create_binary_operator(
  ExpressionType::Subtraction,
  Expression::create_literal(4),
  a
  )
  ),
  Expression::create_binary_operator(
  ExpressionType::GreaterThan,
  Expression::create_aggregate_function(
  AggregateFunction::Sum, {d}
  ),
  Expression::create_literal(1)
  )
  );

  // Previous columns: {a,b,c,d} - New columns: {d, a, b, c}
  ColumnIDMapping column_id_mapping({ColumnID{1}, ColumnID{2}, ColumnID{3}, ColumnID{0}});

  expr->map_column_ids(column_id_mapping);

  EXPECT_EQ(a->column_id(), ColumnID{1});
  EXPECT_EQ(c->column_id(), ColumnID{3});
  EXPECT_EQ(d->column_id(), ColumnID{0});
}

}  // namespace opossum
