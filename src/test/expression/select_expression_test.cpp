#include "gtest/gtest.h"

#include "expression/case_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/expression_factory.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;         // NOLINT
using namespace opossum::expression_factory;  // NOLINT

namespace opossum {

class ExpressionTest : public ::testing::Test {
 public:
  void SetUp() {
    table = load_table("src/test/tables/int_float.tbl");
    StorageManager::get().add_table("int_float", table);

    int_float_node_a = StoredTableNode::make("int_float");
    a = {int_float_node_a, ColumnID{0}};
    b = {int_float_node_a, ColumnID{1}};

    // clang-format off
    const auto lqp_a =
    ProjectionNode::make(expression_vector(parameter(ParameterID{0}, a), add(1, 2)),
                         PredicateNode::make(greater_than(parameter(ParameterID{0}, a), 5),
                                             int_float_node_a));
    const auto lqp_b =
    ProjectionNode::make(expression_vector(parameter(ParameterID{0}, a), add(1, 2)),
                         PredicateNode::make(greater_than(parameter(ParameterID{0}, a), 5),
                                             int_float_node_a));
    const auto lqp_c =
    ProjectionNode::make(expression_vector(parameter(ParameterID{4}), add(1, 2)),
                         PredicateNode::make(greater_than(parameter(ParameterID{0}, a), 5),
                                             int_float_node_a));
    // clang-format on
  }

  void TearDown() { StorageManager::reset(); }

  LQPColumnReference a, b;
  std::shared_ptr<StoredTableNode> int_float_node_a;
  std::shared_ptr<AbstractLQPNode> lqp_a, lqp_b, lqp_c;
  std::shared_ptr<Table> table;
};

TEST_F(ExpressionTest, DeepEquals) {
  EXPECT_TRUE(mul(2, 3)->deep_equals(*mul(2, 3)));
  EXPECT_FALSE(mul(2, 3)->deep_equals(*mul(3, 2)));
  EXPECT_FALSE(mul(2.0, 3)->deep_equals(*mul(2, 3)));
  EXPECT_TRUE(sum(a)->deep_equals(*sum(a)));
  EXPECT_FALSE(sum(a)->deep_equals(*sum(b)));
  EXPECT_TRUE(and_(1, a)->deep_equals(*and_(1, a)));
  EXPECT_TRUE(value(5)->deep_equals(*value(5)));
  EXPECT_FALSE(value(5.0)->deep_equals(*value(5)));
  EXPECT_FALSE(value(5.3)->deep_equals(*value(5)));
  EXPECT_TRUE(between(1, a, 3)->deep_equals(*between(1, a, 3)));
  EXPECT_TRUE(greater_than(1, a)->deep_equals(*greater_than(1, a)));
  EXPECT_FALSE(greater_than(1, a)->deep_equals(*less_than(a, 1)));
  EXPECT_TRUE(is_null(a)->deep_equals(*is_null(a)));
  EXPECT_FALSE(is_null(a)->deep_equals(*is_null(b)));
  EXPECT_TRUE(is_not_null(a)->deep_equals(*is_not_null(a)));
  EXPECT_TRUE(parameter(ParameterID{4})->deep_equals(*parameter(ParameterID{4})));
  EXPECT_FALSE(parameter(ParameterID{4})->deep_equals(*parameter(ParameterID{5})));
  EXPECT_TRUE(extract(DatetimeComponent::Month, "1999-07-30")->deep_equals(*extract(DatetimeComponent::Month, "1999-07-30")));
  EXPECT_TRUE(extract(DatetimeComponent::Day, "1999-07-30")->deep_equals(*extract(DatetimeComponent::Month, "1999-07-30")));
  EXPECT_TRUE(negate(6)->deep_equals(*negate(6)));
  EXPECT_FALSE(negate(6)->deep_equals(*negate(6.5)));

  const auto case_a = case_(equals(add(a, 5), b), add(5, b), a);
  const auto case_b = case_(a, 1, 3);
  const auto case_c = case_(equals(a, 123), b, case_(equals(a, 1234), a, null()));

  EXPECT_TRUE(case_a->deep_equals(*case_a));
  EXPECT_TRUE(case_c->deep_equals(*case_c));
  EXPECT_FALSE(case_a->deep_equals(*case_b));
  EXPECT_FALSE(case_a->deep_equals(*case_c));
}

TEST_F(ExpressionTest, DeepEqualsLQPSelect) {
  const auto select_a = select(lqp_a, std::make_pair(ParameterID{0}, a));
  const auto select_b = select(lqp_b, std::make_pair(ParameterID{0}, a));
  const auto select_c = select(lqp_c, std::make_pair(ParameterID{0}, a));

  EXPECT_TRUE(select_a->deep_equals(*select_b));
  EXPECT_FALSE(select_a->deep_equals(*select_c));

  EXPECT_TRUE(exists(select_a)->deep_equals(*exists(select_a)));
  EXPECT_FALSE(exists(select_a)->deep_equals(*exists(select_c)));
}

TEST_F(ExpressionTest, DeepEqualsPQPSelect) {
  const auto pqp_a = std::make_shared<GetTable>("int_float");
  const auto select_a = std::make_shared<PQPSelectExpression>(pqp_a);

  // Can't compare PQP Select expressions
  EXPECT_ANY_THROW(select_a->deep_equals(*select_a));
}

TEST_F(ExpressionTest, DeepCopy) {
  const auto expr_a = sub(mul(add(1, 5), add(13.3, 14.4)), mod(12, 5.5));
  EXPECT_TRUE(expr_a->deep_equals(*expr_a->deep_copy()));

  const auto expr_b = and_(greater_than_equals(15, 12), or_(greater_than(5, 3), less_than(3, 5)));
  EXPECT_TRUE(expr_b->deep_equals(*expr_b->deep_copy()));

  const auto select_a = select(lqp_a, std::make_pair(ParameterID{0}, a));

  EXPECT_TRUE(select_a->deep_equals(*select_a->deep_copy()));

  // Make sure a new LQP is created on deep_copy()
  EXPECT_NE(std::dynamic_pointer_cast<LQPSelectExpression>(select_a->deep_copy())->lqp, select_a->lqp);
}

TEST_F(ExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(sum(a)->requires_calculation());
  EXPECT_TRUE(between(a, 1, 5)->requires_calculation());
  EXPECT_TRUE(greater_than(a, b)->requires_calculation());
  EXPECT_TRUE(case_(1, a, b)->requires_calculation());
  EXPECT_TRUE(substr("Hello", 1, 2)->requires_calculation());
  EXPECT_TRUE(in(1, list(1, 2, 3))->requires_calculation());
  EXPECT_TRUE(is_null(null())->requires_calculation());
  EXPECT_TRUE(and_(1, 0)->requires_calculation());
  EXPECT_TRUE(negate(5)->requires_calculation());
  EXPECT_FALSE(parameter(ParameterID{5})->requires_calculation());
  EXPECT_FALSE(parameter(ParameterID{5}, a)->requires_calculation());
  EXPECT_FALSE(column(a)->requires_calculation());
  EXPECT_FALSE(PQPColumnExpression::from_table(*table, "a")->requires_calculation());
  EXPECT_FALSE(value(5)->requires_calculation());

  const auto lqp_select_expression = select(int_float_node_a);

  EXPECT_TRUE(lqp_select_expression->requires_calculation());
  EXPECT_TRUE(exists(lqp_select_expression)->requires_calculation());
  EXPECT_TRUE(in(5, lqp_select_expression)->requires_calculation());

  const auto get_table = std::make_shared<GetTable>("int_float");
  const auto pqp_select_expression = std::make_shared<PQPSelectExpression>(get_table);

  EXPECT_TRUE(pqp_select_expression->requires_calculation());
}

TEST_F(ExpressionTest, AsColumnName) {}

TEST_F(ExpressionTest, AsColumnNameNested) {
  /**
   * Test that parentheses are placed correctly when generating column names of nested expressions
   */

  EXPECT_EQ(add(5, 3)->as_column_name(), "5 + 3");
  EXPECT_EQ(add(5, mul(2, 3))->as_column_name(), "5 + 2 * 3");
  EXPECT_EQ(mul(5, add(2, 3))->as_column_name(), "5 * (2 + 3)");
  EXPECT_EQ(div_(5, mul(2, 3))->as_column_name(), "5 / (2 * 3)");
  EXPECT_EQ(case_(greater_than(3, 2), mul(2, 3), 2)->as_column_name(), "CASE WHEN 3 > 2 THEN 2 * 3 ELSE 2 END");
  EXPECT_EQ(case_(1, mul(2, 3), div_(5, mul(2, 3)))->as_column_name(), "CASE WHEN 1 THEN 2 * 3 ELSE 5 / (2 * 3) END");
  EXPECT_EQ(list(1, sum(a))->as_column_name(), "(1, SUM(a))");
  EXPECT_EQ(and_(1, 1)->as_column_name(), "1 AND 1");
  EXPECT_EQ(and_(1, greater_than(add(2, 3), 4))->as_column_name(), "1 AND 2 + 3 > 4");
  EXPECT_EQ(and_(1, or_(greater_than(add(2, 3), 4), 0))->as_column_name(), "1 AND (2 + 3 > 4 OR 0)");
  EXPECT_EQ(or_(1, and_(greater_than(add(2, 3), 4), 0))->as_column_name(), "1 OR (2 + 3 > 4 AND 0)");
  EXPECT_EQ(is_null(1)->as_column_name(), "1 IS NULL");
  EXPECT_EQ(is_null(and_(1, 1))->as_column_name(), "(1 AND 1) IS NULL");
  EXPECT_EQ(is_null(sum(add(a, 2)))->as_column_name(), "SUM(a + 2) IS NULL");
  EXPECT_EQ(less_than(a, b)->as_column_name(), "a < b");
  EXPECT_EQ(less_than(add(a, 5), b)->as_column_name(), "a + 5 < b");
  EXPECT_EQ(between(a, 2, 3)->as_column_name(), "a BETWEEN 2 AND 3");
  EXPECT_EQ(and_(greater_than_equals(b, 5), between(a, 2, 3))->as_column_name(), "b >= 5 AND a BETWEEN 2 AND 3");
  EXPECT_EQ(not_equals(between(a, 2, 3), 0)->as_column_name(), "(a BETWEEN 2 AND 3) != 0");

  EXPECT_EQ(mul(less_than(add(a, 5), b), 3)->as_column_name(), "(a + 5 < b) * 3");
  EXPECT_EQ(add(1, between(a, 2, 3))->as_column_name(), "1 + (a BETWEEN 2 AND 3)");

  // TODO(anybody) Omit redundant parentheses
  EXPECT_EQ(add(5, add(1, 3))->as_column_name(), "5 + (1 + 3)");
  EXPECT_EQ(add(add(2, 5), add(1, 3))->as_column_name(), "(2 + 5) + (1 + 3)");
  EXPECT_EQ(mul(mul(2, 5), mul(1, 3))->as_column_name(), "(2 * 5) * (1 * 3)");
  EXPECT_EQ(and_(and_(1, 0), and_(0, 1))->as_column_name(), "(1 AND 0) AND (0 AND 1)");
  EXPECT_EQ(and_(1, and_(1, or_(0, 1)))->as_column_name(), "1 AND (1 AND (0 OR 1))");
}

}  // namespace opossum
