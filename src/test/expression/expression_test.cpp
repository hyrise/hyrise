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

/**
 * Tests for most expression types, excluding Selects, since they have no complex behaviour that would warrant their own
 * test file.
 */

class ExpressionTest : public ::testing::Test {
 public:
  void SetUp() {
    table_int_float = load_table("src/test/tables/int_float.tbl");
    table_int_float_with_null = load_table("src/test/tables/int_float_with_null.tbl");
    StorageManager::get().add_table("int_float", table_int_float);
    StorageManager::get().add_table("int_float_with_null", table_int_float_with_null);

    int_float_node = StoredTableNode::make("int_float");
    a = {int_float_node, ColumnID{0}};
    b = {int_float_node, ColumnID{1}};

    int_float_node_nullable = StoredTableNode::make("int_float_with_null");
    a_nullable = {int_float_node_nullable, ColumnID{0}};
    b_nullable = {int_float_node_nullable, ColumnID{1}};
  }

  void TearDown() { StorageManager::reset(); }

  LQPColumnReference a, b;
  LQPColumnReference a_nullable, b_nullable;
  std::shared_ptr<StoredTableNode> int_float_node, int_float_node_nullable;
  std::shared_ptr<AbstractLQPNode> lqp_a, lqp_b, lqp_c;
  std::shared_ptr<Table> table_int_float, table_int_float_with_null;
};

TEST_F(ExpressionTest, Equals) {
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
  EXPECT_TRUE(cast(6.5, DataType::Int)->deep_equals(*cast(6.5, DataType::Int)));
  EXPECT_FALSE(cast(6.5, DataType::Int)->deep_equals(*cast(6.5, DataType::Float)));
}

TEST_F(ExpressionTest, DeepEquals) {
  const auto expr_a_a = sub(mul(add(1, 5), add(13.3, 14.4)), mod(12, 5.5));
  const auto expr_a_b = sub(mul(add(1, 5), add(13.3, 14.4)), mod(12, 5.5));
  const auto expr_b = sub(mul(add(1, 5), add(13.3, 14.4)), mod(12, null()));
  EXPECT_TRUE(expr_a_a->deep_equals(*expr_a_b));
  EXPECT_FALSE(expr_a_a->deep_equals(*expr_b));

  const auto case_a = case_(equals(add(a, 5), b), add(5, b), a);
  const auto case_b = case_(a, 1, 3);
  const auto case_c = case_(equals(a, 123), b, case_(equals(a, 1234), a, null()));

  EXPECT_TRUE(case_a->deep_equals(*case_a));
  EXPECT_TRUE(case_c->deep_equals(*case_c));
  EXPECT_FALSE(case_a->deep_equals(*case_b));
  EXPECT_FALSE(case_a->deep_equals(*case_c));
}

TEST_F(ExpressionTest, DeepCopy) {
  const auto expr_a = sub(mul(add(1, 5), add(13.3, 14.4)), mod(12, 5.5));
  EXPECT_TRUE(expr_a->deep_equals(*expr_a->deep_copy()));

  const auto expr_b = and_(greater_than_equals(15, 12), or_(greater_than(5, 3), less_than(3, 5)));
  EXPECT_TRUE(expr_b->deep_equals(*expr_b->deep_copy()));
}

TEST_F(ExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(sum(a)->requires_computation());
  EXPECT_TRUE(between(a, 1, 5)->requires_computation());
  EXPECT_TRUE(greater_than(a, b)->requires_computation());
  EXPECT_TRUE(case_(1, a, b)->requires_computation());
  EXPECT_TRUE(substr("Hello", 1, 2)->requires_computation());
  EXPECT_TRUE(in(1, list(1, 2, 3))->requires_computation());
  EXPECT_TRUE(is_null(null())->requires_computation());
  EXPECT_TRUE(and_(1, 0)->requires_computation());
  EXPECT_TRUE(negate(5)->requires_computation());
  EXPECT_FALSE(parameter(ParameterID{5})->requires_computation());
  EXPECT_FALSE(parameter(ParameterID{5}, a)->requires_computation());
  EXPECT_FALSE(column(a)->requires_computation());
  EXPECT_FALSE(PQPColumnExpression::from_table(*table_int_float, "a")->requires_computation());
  EXPECT_FALSE(value(5)->requires_computation());
  EXPECT_TRUE(cast(5, DataType::Int)->requires_computation());
  EXPECT_TRUE(cast(5.5, DataType::Int)->requires_computation());

  const auto lqp_select_expression = select(int_float_node);

  EXPECT_TRUE(lqp_select_expression->requires_computation());
  EXPECT_TRUE(exists(lqp_select_expression)->requires_computation());
  EXPECT_TRUE(in(5, lqp_select_expression)->requires_computation());

  const auto get_table = std::make_shared<GetTable>("int_float");
  const auto pqp_select_expression = std::make_shared<PQPSelectExpression>(get_table);

  EXPECT_TRUE(pqp_select_expression->requires_computation());
}

TEST_F(ExpressionTest, AsColumnName) {
  EXPECT_EQ(sub(5, 3)->as_column_name(), "5 - 3");
  EXPECT_EQ(add(5, 3)->as_column_name(), "5 + 3");
  EXPECT_EQ(mul(5, 3)->as_column_name(), "5 * 3");
  EXPECT_EQ(mod(5, 3)->as_column_name(), "5 % 3");
  EXPECT_EQ(div_(5, 3)->as_column_name(), "5 / 3");
  EXPECT_EQ(less_than(5, 3)->as_column_name(), "5 < 3");
  EXPECT_EQ(less_than_equals(5, 3)->as_column_name(), "5 <= 3");
  EXPECT_EQ(greater_than_equals(5, 3)->as_column_name(), "5 >= 3");
  EXPECT_EQ(greater_than(5, 3)->as_column_name(), "5 > 3");
  EXPECT_EQ(equals(5, 3)->as_column_name(), "5 = 3");
  EXPECT_EQ(not_equals(5, 3)->as_column_name(), "5 != 3");
  EXPECT_EQ(between(5, 3, 4)->as_column_name(), "5 BETWEEN 3 AND 4");
  EXPECT_EQ(case_(1, 3, case_(0, 2, 1))->as_column_name(), "CASE WHEN 1 THEN 3 ELSE CASE WHEN 0 THEN 2 ELSE 1 END END");
  EXPECT_EQ(extract(DatetimeComponent::Month, "1993-03-04")->as_column_name(), "EXTRACT(MONTH FROM \"1993-03-04\")");
  EXPECT_EQ(substr("Hello", 1, 2)->as_column_name(), "SUBSTR(\"Hello\", 1, 2)");
  EXPECT_EQ(concat("Hello", "World")->as_column_name(), "CONCAT(\"Hello\", \"World\")");
  EXPECT_EQ(and_(1, 0)->as_column_name(), "1 AND 0");
  EXPECT_EQ(or_(1, 0)->as_column_name(), "1 OR 0");
  EXPECT_EQ(is_null(1)->as_column_name(), "1 IS NULL");
  EXPECT_EQ(is_not_null(1)->as_column_name(), "1 IS NOT NULL");
  EXPECT_EQ(list(1)->as_column_name(), "(1)");
  EXPECT_EQ(list(1)->as_column_name(), "(1)");
  EXPECT_EQ(negate(3)->as_column_name(), "-(3)");
  EXPECT_EQ(value(3.25)->as_column_name(), "3.25");
  EXPECT_EQ(null()->as_column_name(), "NULL");
  EXPECT_EQ(cast("36", DataType::Float)->as_column_name(), "CAST(\"36\" AS float)");
  EXPECT_EQ(parameter(ParameterID{0})->as_column_name(), "Parameter[id=0]");
  EXPECT_EQ(parameter(ParameterID{0}, a)->as_column_name(), "Parameter[name=a;id=0]");
}

TEST_F(ExpressionTest, AsColumnNameNested) {
  /**
   * Test that parentheses are placed correctly when generating column names of nested expressions
   */

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

TEST_F(ExpressionTest, DataType) {
  EXPECT_EQ(add(int32_t{1}, int32_t{2})->data_type(), DataType::Int);
  EXPECT_EQ(add(int32_t{1}, int64_t{2})->data_type(), DataType::Long);
  EXPECT_EQ(add(int64_t{1}, int32_t{2})->data_type(), DataType::Long);
  EXPECT_EQ(add(float{1.3}, int32_t{2})->data_type(), DataType::Float);
  EXPECT_EQ(add(float{1.3}, int64_t{2})->data_type(), DataType::Double);
  EXPECT_EQ(add(float{1.3}, float{2})->data_type(), DataType::Float);
  EXPECT_EQ(add(double{1.3}, float{2})->data_type(), DataType::Double);
  EXPECT_EQ(add(double{1.3}, double{2})->data_type(), DataType::Double);
  EXPECT_EQ(add(int32_t{1}, double{2})->data_type(), DataType::Double);
  EXPECT_EQ(negate(float{2})->data_type(), DataType::Float);
  EXPECT_EQ(negate(double{2})->data_type(), DataType::Double);
  EXPECT_EQ(value(double{2})->data_type(), DataType::Double);
  EXPECT_EQ(value("Hello")->data_type(), DataType::String);
  EXPECT_EQ(null()->data_type(), DataType::Null);
  EXPECT_EQ(cast(36.5, DataType::Int)->data_type(), DataType::Int);
  EXPECT_EQ(cast(null(), DataType::Float)->data_type(), DataType::Float);

  EXPECT_EQ(less_than(1, 2)->data_type(), DataType::Int);
  EXPECT_EQ(less_than(1.5, 2)->data_type(), DataType::Int);
  EXPECT_EQ(between(1.5, 2, 3)->data_type(), DataType::Int);
  EXPECT_EQ(and_(1, 1)->data_type(), DataType::Int);
  EXPECT_EQ(or_(1, 1)->data_type(), DataType::Int);
  EXPECT_EQ(is_null(5)->data_type(), DataType::Int);  
  
  EXPECT_EQ(case_(1, int32_t{1}, int32_t{1})->data_type(), DataType::Int);
  EXPECT_EQ(case_(1, double(2.3), int32_t{1})->data_type(), DataType::Double);
  EXPECT_EQ(substr("Hello", 1, 2)->data_type(), DataType::String);
  EXPECT_EQ(concat("Hello", "World")->data_type(), DataType::String);
  EXPECT_EQ(concat("Hello", "World")->data_type(), DataType::String);
}

TEST_F(ExpressionTest, IsNullable) {
  EXPECT_FALSE(add(1, 2)->is_nullable());
  EXPECT_FALSE(between(1, 2, 3)->is_nullable());
  EXPECT_TRUE(between(1, null(), 3)->is_nullable());
  EXPECT_FALSE(list(1, 2)->is_nullable());
  EXPECT_TRUE(list(1, null())->is_nullable());
  EXPECT_FALSE(and_(1, 1)->is_nullable());
  EXPECT_FALSE(case_(1, 1, 2)->is_nullable());
  EXPECT_TRUE(case_(null(), 1, 2)->is_nullable());
  EXPECT_TRUE(case_(1, 1, null())->is_nullable());
  EXPECT_TRUE(add(greater_than(2, null()), 1)->is_nullable());
  EXPECT_TRUE(and_(greater_than(2, null()), 1)->is_nullable());
  EXPECT_FALSE(column(a)->is_nullable());
  EXPECT_TRUE(column(a_nullable)->is_nullable());
  EXPECT_FALSE(cast(12, DataType::String)->is_nullable());
  EXPECT_TRUE(cast(null(), DataType::String)->is_nullable());

  // Division by zero could be nullable, thus division and modulo are always nullable
  EXPECT_TRUE(div_(1, 2)->is_nullable());
  EXPECT_TRUE(mod(1, 2)->is_nullable());
}

}  // namespace opossum
