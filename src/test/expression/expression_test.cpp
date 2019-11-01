#include "base_test.hpp"
#include "expression/case_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/get_table.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;            // NOLINT
using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

/**
 * Tests for most expression types, excluding Subqueries, since they have no complex behaviour that would warrant their
 * own test file.
 */

class ExpressionTest : public BaseTest {
 public:
  void SetUp() {
    table_int_float = load_table("resources/test_data/tbl/int_float.tbl");
    table_int_float_with_null = load_table("resources/test_data/tbl/int_float_with_null.tbl");
    Hyrise::get().storage_manager.add_table("int_float", table_int_float);
    Hyrise::get().storage_manager.add_table("int_float_with_null", table_int_float_with_null);

    int_float_node = StoredTableNode::make("int_float");
    a = {int_float_node, ColumnID{0}};
    b = {int_float_node, ColumnID{1}};

    int_float_node_nullable = StoredTableNode::make("int_float_with_null");
    a_nullable = {int_float_node_nullable, ColumnID{0}};
    b_nullable = {int_float_node_nullable, ColumnID{1}};
  }

  LQPColumnReference a, b;
  LQPColumnReference a_nullable, b_nullable;
  std::shared_ptr<StoredTableNode> int_float_node, int_float_node_nullable;
  std::shared_ptr<AbstractLQPNode> lqp_a, lqp_b, lqp_c;
  std::shared_ptr<Table> table_int_float, table_int_float_with_null;
};

TEST_F(ExpressionTest, Equals) {
  EXPECT_EQ(*mul_(2, 3), *mul_(2, 3));
  EXPECT_NE(*mul_(2, 3), *mul_(3, 2));
  EXPECT_NE(*mul_(2.0, 3), *mul_(2, 3));
  EXPECT_EQ(*sum_(a), *sum_(a));
  EXPECT_NE(*sum_(a), *sum_(b));
  EXPECT_EQ(*and_(1, a), *and_(1, a));
  EXPECT_EQ(*value_(5), *value_(5));
  EXPECT_NE(*value_(5.0), *value_(5));
  EXPECT_NE(*value_(5.3), *value_(5));
  EXPECT_EQ(*between_inclusive_(1, a, 3), *between_inclusive_(1, a, 3));
  EXPECT_NE(*between_inclusive_(1, a, 3), *between_inclusive_(1, a, 4));
  EXPECT_NE(*between_inclusive_(1, a, 3), *between_exclusive_(1, a, 3));
  EXPECT_EQ(*greater_than_(1, a), *greater_than_(1, a));
  EXPECT_NE(*greater_than_(1, a), *less_than_(a, 1));
  EXPECT_EQ(*is_null_(a), *is_null_(a));
  EXPECT_NE(*is_null_(a), *is_null_(b));
  EXPECT_EQ(*is_not_null_(a), *is_not_null_(a));
  EXPECT_EQ(*placeholder_(ParameterID{4}), *placeholder_(ParameterID{4}));
  EXPECT_NE(*placeholder_(ParameterID{4}), *placeholder_(ParameterID{5}));
  EXPECT_EQ(*extract_(DatetimeComponent::Month, "1999-07-30"), *extract_(DatetimeComponent::Month, "1999-07-30"));
  EXPECT_NE(*extract_(DatetimeComponent::Day, "1999-07-30"), *extract_(DatetimeComponent::Month, "1999-07-30"));
  EXPECT_EQ(*unary_minus_(6), *unary_minus_(6));
  EXPECT_NE(*unary_minus_(6), *unary_minus_(6.5));
  EXPECT_EQ(*cast_(6.5, DataType::Int), *cast_(6.5, DataType::Int));
  EXPECT_NE(*cast_(6.5, DataType::Int), *cast_(6.5, DataType::Float));
}

TEST_F(ExpressionTest, DeepEquals) {
  const auto expr_a_a = sub_(mul_(add_(1, 5), add_(13.3, 14.4)), mod_(12, 5.5));
  const auto expr_a_b = sub_(mul_(add_(1, 5), add_(13.3, 14.4)), mod_(12, 5.5));
  const auto expr_b = sub_(mul_(add_(1, 5), add_(13.3, 14.4)), mod_(12, null_()));
  EXPECT_EQ(*expr_a_a, *expr_a_b);
  EXPECT_NE(*expr_a_a, *expr_b);

  const auto case_a = case_(equals_(add_(a, 5), b), add_(5, b), a);
  const auto case_b = case_(a, 1, 3);
  const auto case_c = case_(equals_(a, 123), b, case_(equals_(a, 1234), a, null_()));

  EXPECT_EQ(*case_a, *case_a);
  EXPECT_EQ(*case_c, *case_c);
  EXPECT_NE(*case_a, *case_b);
  EXPECT_NE(*case_a, *case_c);

  const auto parameter_a = correlated_parameter_(ParameterID{5}, a);
  const auto parameter_b = correlated_parameter_(ParameterID{5}, a);
  EXPECT_EQ(*parameter_a, *parameter_b);
  parameter_a->set_value(3);
  parameter_b->set_value(4);
  EXPECT_NE(*parameter_a, *parameter_b);
}

TEST_F(ExpressionTest, DeepCopy) {
  const auto expr_a = sub_(mul_(add_(1, 5), add_(13.3, 14.4)), mod_(12, 5.5));
  EXPECT_EQ(*expr_a, *expr_a->deep_copy());

  const auto expr_b = and_(greater_than_equals_(15, 12), or_(greater_than_(5, 3), less_than_(3, 5)));
  EXPECT_EQ(*expr_b, *expr_b->deep_copy());

  const auto parameter_a = correlated_parameter_(ParameterID{5}, a);
  parameter_a->set_value(3);
  const auto parameter_b = parameter_a->deep_copy();
  EXPECT_EQ(*parameter_a, *parameter_b);
  static_cast<CorrelatedParameterExpression&>(*parameter_b).set_value(4);
  EXPECT_NE(*parameter_a, *parameter_b);
}

TEST_F(ExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(sum_(a)->requires_computation());
  EXPECT_TRUE(between_inclusive_(a, 1, 5)->requires_computation());
  EXPECT_TRUE(greater_than_(a, b)->requires_computation());
  EXPECT_TRUE(case_(1, a, b)->requires_computation());
  EXPECT_TRUE(substr_("Hello", 1, 2)->requires_computation());
  EXPECT_TRUE(in_(1, list_(1, 2, 3))->requires_computation());
  EXPECT_TRUE(is_null_(null_())->requires_computation());
  EXPECT_TRUE(and_(1, 0)->requires_computation());
  EXPECT_TRUE(unary_minus_(5)->requires_computation());
  EXPECT_FALSE(placeholder_(ParameterID{5})->requires_computation());
  EXPECT_FALSE(correlated_parameter_(ParameterID{5}, a)->requires_computation());
  EXPECT_FALSE(lqp_column_(a)->requires_computation());
  EXPECT_FALSE(PQPColumnExpression::from_table(*table_int_float, "a")->requires_computation());
  EXPECT_FALSE(value_(5)->requires_computation());
  EXPECT_TRUE(cast_(5, DataType::Int)->requires_computation());
  EXPECT_TRUE(cast_(5.5, DataType::Int)->requires_computation());

  const auto subquery_expression = lqp_subquery_(int_float_node);

  EXPECT_TRUE(subquery_expression->requires_computation());
  EXPECT_TRUE(exists_(subquery_expression)->requires_computation());
  EXPECT_TRUE(in_(5, subquery_expression)->requires_computation());
  EXPECT_TRUE(not_in_(5, subquery_expression)->requires_computation());

  const auto get_table = std::make_shared<GetTable>("int_float");
  const auto pqp_subquery_expression = std::make_shared<PQPSubqueryExpression>(get_table);

  EXPECT_TRUE(pqp_subquery_expression->requires_computation());
}

TEST_F(ExpressionTest, AsColumnName) {
  EXPECT_EQ(sub_(5, 3)->as_column_name(), "5 - 3");
  EXPECT_EQ(add_(5, 3)->as_column_name(), "5 + 3");
  EXPECT_EQ(mul_(5, 3)->as_column_name(), "5 * 3");
  EXPECT_EQ(mod_(5, 3)->as_column_name(), "5 % 3");
  EXPECT_EQ(div_(5, 3)->as_column_name(), "5 / 3");
  EXPECT_EQ(less_than_(5, 3)->as_column_name(), "5 < 3");
  EXPECT_EQ(less_than_equals_(5, 3)->as_column_name(), "5 <= 3");
  EXPECT_EQ(greater_than_equals_(5, 3)->as_column_name(), "5 >= 3");
  EXPECT_EQ(greater_than_(5, 3)->as_column_name(), "5 > 3");
  EXPECT_EQ(equals_(5, 3)->as_column_name(), "5 = 3");
  EXPECT_EQ(not_equals_(5, 3)->as_column_name(), "5 != 3");
  EXPECT_EQ(between_inclusive_(5, 3, 4)->as_column_name(), "5 BETWEEN INCLUSIVE 3 AND 4");
  EXPECT_EQ(case_(1, 3, case_(0, 2, 1))->as_column_name(), "CASE WHEN 1 THEN 3 ELSE CASE WHEN 0 THEN 2 ELSE 1 END END");
  EXPECT_EQ(extract_(DatetimeComponent::Month, "1993-03-04")->as_column_name(), "EXTRACT(MONTH FROM '1993-03-04')");
  EXPECT_EQ(substr_("Hello", 1, 2)->as_column_name(), "SUBSTR('Hello',1,2)");
  EXPECT_EQ(concat_("Hello", "World")->as_column_name(), "CONCAT('Hello','World')");
  EXPECT_EQ(and_(1, 0)->as_column_name(), "1 AND 0");
  EXPECT_EQ(or_(1, 0)->as_column_name(), "1 OR 0");
  EXPECT_EQ(is_null_(1)->as_column_name(), "1 IS NULL");
  EXPECT_EQ(is_not_null_(1)->as_column_name(), "1 IS NOT NULL");
  EXPECT_EQ(list_(1)->as_column_name(), "(1)");
  EXPECT_EQ(list_(1)->as_column_name(), "(1)");
  EXPECT_EQ(unary_minus_(3)->as_column_name(), "-(3)");
  EXPECT_EQ(value_(3.25)->as_column_name(), "3.25");
  EXPECT_EQ(null_()->as_column_name(), "NULL");
  EXPECT_EQ(cast_("36", DataType::Float)->as_column_name(), "CAST('36' AS float)");
  EXPECT_EQ(placeholder_(ParameterID{0})->as_column_name(), "Placeholder[id=0]");
  EXPECT_EQ(correlated_parameter_(ParameterID{0}, a)->as_column_name(), "Parameter[name=a;id=0]");
  EXPECT_EQ(in_(5, list_(1, 2, 3))->as_column_name(), "(5) IN (1, 2, 3)");
  EXPECT_EQ(not_in_(5, list_(1, 2, 3))->as_column_name(), "(5) NOT IN (1, 2, 3)");
}

TEST_F(ExpressionTest, AsColumnNameNested) {
  /**
   * Test that parentheses are placed correctly when generating column names of nested expressions
   */

  EXPECT_EQ(add_(5, mul_(2, 3))->as_column_name(), "5 + 2 * 3");
  EXPECT_EQ(mul_(5, add_(2, 3))->as_column_name(), "5 * (2 + 3)");
  EXPECT_EQ(div_(5, mul_(2, 3))->as_column_name(), "5 / (2 * 3)");
  EXPECT_EQ(case_(greater_than_(3, 2), mul_(2, 3), 2)->as_column_name(), "CASE WHEN 3 > 2 THEN 2 * 3 ELSE 2 END");
  EXPECT_EQ(case_(1, mul_(2, 3), div_(5, mul_(2, 3)))->as_column_name(), "CASE WHEN 1 THEN 2 * 3 ELSE 5 / (2 * 3) END");
  EXPECT_EQ(list_(1, sum_(a))->as_column_name(), "(1, SUM(a))");
  EXPECT_EQ(and_(1, 1)->as_column_name(), "1 AND 1");
  EXPECT_EQ(and_(1, greater_than_(add_(2, 3), 4))->as_column_name(), "1 AND 2 + 3 > 4");
  EXPECT_EQ(and_(1, or_(greater_than_(add_(2, 3), 4), 0))->as_column_name(), "1 AND (2 + 3 > 4 OR 0)");
  EXPECT_EQ(or_(1, and_(greater_than_(add_(2, 3), 4), 0))->as_column_name(), "1 OR (2 + 3 > 4 AND 0)");
  EXPECT_EQ(is_null_(1)->as_column_name(), "1 IS NULL");
  EXPECT_EQ(is_null_(and_(1, 1))->as_column_name(), "(1 AND 1) IS NULL");
  EXPECT_EQ(is_null_(sum_(add_(a, 2)))->as_column_name(), "SUM(a + 2) IS NULL");
  EXPECT_EQ(less_than_(a, b)->as_column_name(), "a < b");
  EXPECT_EQ(less_than_(add_(a, 5), b)->as_column_name(), "a + 5 < b");
  EXPECT_EQ(between_inclusive_(a, 2, 3)->as_column_name(), "a BETWEEN INCLUSIVE 2 AND 3");
  EXPECT_EQ(and_(greater_than_equals_(b, 5), between_inclusive_(a, 2, 3))->as_column_name(),
            "b >= 5 AND a BETWEEN INCLUSIVE 2 AND 3");
  EXPECT_EQ(not_equals_(between_inclusive_(a, 2, 3), 0)->as_column_name(), "(a BETWEEN INCLUSIVE 2 AND 3) != 0");

  EXPECT_EQ(mul_(less_than_(add_(a, 5), b), 3)->as_column_name(), "(a + 5 < b) * 3");
  EXPECT_EQ(add_(1, between_inclusive_(a, 2, 3))->as_column_name(), "1 + (a BETWEEN INCLUSIVE 2 AND 3)");

  // TODO(anybody) Omit redundant parentheses
  EXPECT_EQ(add_(5, add_(1, 3))->as_column_name(), "5 + (1 + 3)");
  EXPECT_EQ(add_(add_(2, 5), add_(1, 3))->as_column_name(), "(2 + 5) + (1 + 3)");
  EXPECT_EQ(mul_(mul_(2, 5), mul_(1, 3))->as_column_name(), "(2 * 5) * (1 * 3)");
  EXPECT_EQ(and_(and_(1, 0), and_(0, 1))->as_column_name(), "(1 AND 0) AND (0 AND 1)");
  EXPECT_EQ(and_(1, and_(1, or_(0, 1)))->as_column_name(), "1 AND (1 AND (0 OR 1))");
}

TEST_F(ExpressionTest, DataType) {
  EXPECT_EQ(add_(int32_t{1}, int32_t{2})->data_type(), DataType::Int);
  EXPECT_EQ(add_(int32_t{1}, int64_t{2})->data_type(), DataType::Long);
  EXPECT_EQ(add_(int64_t{1}, int32_t{2})->data_type(), DataType::Long);
  EXPECT_EQ(add_(float{1.3f}, int32_t{2})->data_type(), DataType::Float);
  EXPECT_EQ(add_(float{1.3f}, int64_t{2})->data_type(), DataType::Double);
  EXPECT_EQ(add_(float{1.3f}, float{2.f})->data_type(), DataType::Float);
  EXPECT_EQ(add_(double{1.3}, float{2.f})->data_type(), DataType::Double);
  EXPECT_EQ(add_(double{1.3}, double{2})->data_type(), DataType::Double);
  EXPECT_EQ(add_(int32_t{1}, double{2})->data_type(), DataType::Double);
  EXPECT_EQ(unary_minus_(float{2.f})->data_type(), DataType::Float);
  EXPECT_EQ(unary_minus_(double{2})->data_type(), DataType::Double);
  EXPECT_EQ(value_(double{2})->data_type(), DataType::Double);
  EXPECT_EQ(value_("Hello")->data_type(), DataType::String);
  EXPECT_EQ(null_()->data_type(), DataType::Null);
  EXPECT_EQ(cast_(36.5, DataType::Int)->data_type(), DataType::Int);
  EXPECT_EQ(cast_(null_(), DataType::Float)->data_type(), DataType::Float);

  EXPECT_EQ(less_than_(1, 2)->data_type(), DataType::Int);
  EXPECT_EQ(less_than_(1.5, 2)->data_type(), DataType::Int);
  EXPECT_EQ(between_inclusive_(1.5, 2, 3)->data_type(), DataType::Int);
  EXPECT_EQ(and_(1, 1)->data_type(), DataType::Int);
  EXPECT_EQ(or_(1, 1)->data_type(), DataType::Int);
  EXPECT_EQ(in_(1, list_(1, 2, 3))->data_type(), DataType::Int);
  EXPECT_EQ(not_in_(1, list_(1, 2, 3))->data_type(), DataType::Int);
  EXPECT_EQ(is_null_(5)->data_type(), DataType::Int);

  EXPECT_EQ(case_(1, int32_t{1}, int32_t{1})->data_type(), DataType::Int);
  EXPECT_EQ(case_(1, double{2.3}, int32_t{1})->data_type(), DataType::Double);
  EXPECT_EQ(substr_("Hello", 1, 2)->data_type(), DataType::String);
  EXPECT_EQ(concat_("Hello", "World")->data_type(), DataType::String);
  EXPECT_EQ(concat_("Hello", "World")->data_type(), DataType::String);
}

TEST_F(ExpressionTest, IsNullable) {
  const auto dummy_lqp = MockNode::make(MockNode::ColumnDefinitions{});

  EXPECT_FALSE(add_(1, 2)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(between_inclusive_(1, 2, 3)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(between_inclusive_(1, null_(), 3)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(list_(1, 2)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(list_(1, null_())->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(and_(1, 1)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(case_(1, 1, 2)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(case_(null_(), 1, 2)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(case_(1, 1, null_())->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(add_(greater_than_(2, null_()), 1)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(and_(greater_than_(2, null_()), 1)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(cast_(12, DataType::String)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(cast_(null_(), DataType::String)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(sum_(null_())->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(sum_(add_(1, 2))->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(count_star_(dummy_lqp)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(count_(5)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(count_(null_())->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_FALSE(in_(1, list_(1, 2, 3))->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(in_(null_(), list_(1, 2, 3))->is_nullable_on_lqp(*dummy_lqp));

  // Division by zero could be nullable, thus division and modulo are always nullable
  EXPECT_TRUE(div_(1, 2)->is_nullable_on_lqp(*dummy_lqp));
  EXPECT_TRUE(mod_(1, 2)->is_nullable_on_lqp(*dummy_lqp));
}

TEST_F(ExpressionTest, StaticTableNode) {
  {
    const auto static_table_node = StaticTableNode::make(table_int_float);
    const auto col_a = lqp_column_({static_table_node, ColumnID{0}});
    const auto col_b = lqp_column_({static_table_node, ColumnID{1}});
    EXPECT_EQ(col_a->as_column_name(), "a");
    EXPECT_EQ(col_a->data_type(), DataType::Int);
    EXPECT_EQ(col_a->is_nullable_on_lqp(*static_table_node), false);
    EXPECT_EQ(col_b->as_column_name(), "b");
    EXPECT_EQ(col_b->data_type(), DataType::Float);
    EXPECT_EQ(col_b->is_nullable_on_lqp(*static_table_node), false);
  }

  {
    const auto static_table_node = StaticTableNode::make(table_int_float_with_null);
    const auto col_a = lqp_column_({static_table_node, ColumnID{0}});
    EXPECT_EQ(col_a->as_column_name(), "a");
    EXPECT_EQ(col_a->data_type(), DataType::Int);
    EXPECT_EQ(col_a->is_nullable_on_lqp(*static_table_node), true);
  }
}

TEST_F(ExpressionTest, EqualsAndHash) {
  std::vector<std::pair<int, std::shared_ptr<AbstractExpression>>> expressions;

  // AggregateExpression
  expressions.emplace_back(__LINE__, count_(5));
  expressions.emplace_back(__LINE__, count_(null_()));
  expressions.emplace_back(__LINE__, sum_(lqp_column_(a)));

  // ArithmeticExpression
  expressions.emplace_back(__LINE__, sub_(5, 3));
  expressions.emplace_back(__LINE__, add_(5, 3));
  expressions.emplace_back(__LINE__, mul_(5, 3));
  expressions.emplace_back(__LINE__, mod_(5, 3));
  expressions.emplace_back(__LINE__, div_(5, 3));

  // BetweenExpression
  expressions.emplace_back(__LINE__, between_inclusive_(5, 3, 4));
  expressions.emplace_back(__LINE__, between_inclusive_(5, 4, 4));
  expressions.emplace_back(__LINE__, between_exclusive_(5, 3, 4));

  // BinaryPredicateExpression
  expressions.emplace_back(__LINE__, less_than_(5, 3));
  expressions.emplace_back(__LINE__, less_than_equals_(5, 3));
  expressions.emplace_back(__LINE__, greater_than_equals_(5, 3));
  expressions.emplace_back(__LINE__, greater_than_(5, 3));
  expressions.emplace_back(__LINE__, equals_(5, 3));
  expressions.emplace_back(__LINE__, not_equals_(5, 3));

  // CaseExpression
  expressions.emplace_back(__LINE__, case_(1, 3, case_(0, 2, 1)));
  expressions.emplace_back(__LINE__, case_(1, 5, case_(0, 2, 1)));

  // CastExpression
  expressions.emplace_back(__LINE__, cast_("36", DataType::Float));
  expressions.emplace_back(__LINE__, cast_("35", DataType::Float));
  expressions.emplace_back(__LINE__, cast_("35", DataType::Int));

  // CorrelatedParameterExpression
  expressions.emplace_back(__LINE__, correlated_parameter_(ParameterID{0}, a));
  expressions.emplace_back(__LINE__, correlated_parameter_(ParameterID{1}, a));
  expressions.emplace_back(__LINE__, correlated_parameter_(ParameterID{1}, b));

  // ExistsExpression
  expressions.emplace_back(__LINE__, exists_(lqp_subquery_(int_float_node)));
  expressions.emplace_back(__LINE__, not_exists_(lqp_subquery_(int_float_node)));
  expressions.emplace_back(__LINE__, not_exists_(lqp_subquery_(int_float_node_nullable)));

  // ExtractExpression
  expressions.emplace_back(__LINE__, extract_(DatetimeComponent::Month, "1993-03-04"));
  expressions.emplace_back(__LINE__, extract_(DatetimeComponent::Month, "2019-03-04"));
  expressions.emplace_back(__LINE__, extract_(DatetimeComponent::Year, "2019-03-04"));

  // FunctionExpression
  expressions.emplace_back(__LINE__, substr_("Hello", 1, 2));
  expressions.emplace_back(__LINE__, substr_("Hello", 2, 2));
  expressions.emplace_back(__LINE__, concat_("Hello", "World"));

  // InExpression
  expressions.emplace_back(__LINE__, in_(5, list_(1, 2, 3)));
  expressions.emplace_back(__LINE__, in_(6, list_(1, 2, 3)));
  expressions.emplace_back(__LINE__, not_in_(5, list_(1, 2, 3)));
  expressions.emplace_back(__LINE__, in_(5, lqp_subquery_(int_float_node)));

  // IsNullExpression
  expressions.emplace_back(__LINE__, is_null_(1));
  expressions.emplace_back(__LINE__, is_null_(2));
  expressions.emplace_back(__LINE__, is_not_null_(1));

  // ListExpression
  expressions.emplace_back(__LINE__, list_(1));
  expressions.emplace_back(__LINE__, list_(1, 2));
  expressions.emplace_back(__LINE__, list_(2, 3));

  // LogicalExpression
  expressions.emplace_back(__LINE__, and_(1, 0));
  expressions.emplace_back(__LINE__, and_(1, 1));
  expressions.emplace_back(__LINE__, or_(1, 0));

  // LQPColumnExpression
  expressions.emplace_back(__LINE__, lqp_column_(a));
  expressions.emplace_back(__LINE__, lqp_column_(b));

  // LQPSubqueryExpression
  expressions.emplace_back(__LINE__, lqp_subquery_(int_float_node));
  expressions.emplace_back(__LINE__, lqp_subquery_(int_float_node_nullable));
  expressions.emplace_back(__LINE__, lqp_subquery_(int_float_node, std::make_pair(ParameterID{0}, a)));
  expressions.emplace_back(__LINE__, lqp_subquery_(int_float_node, std::make_pair(ParameterID{1}, a)));
  expressions.emplace_back(__LINE__, lqp_subquery_(int_float_node, std::make_pair(ParameterID{1}, b)));

  // PlaceholderExpression
  expressions.emplace_back(__LINE__, placeholder_(ParameterID{0}));
  expressions.emplace_back(__LINE__, placeholder_(ParameterID{1}));

  // PQPColumnExpression
  expressions.emplace_back(__LINE__, pqp_column_(ColumnID{0}, DataType::Float, false, "a + b"));
  expressions.emplace_back(__LINE__, pqp_column_(ColumnID{1}, DataType::Float, false, "a + b"));
  expressions.emplace_back(__LINE__, pqp_column_(ColumnID{1}, DataType::Int, false, "a + b"));
  expressions.emplace_back(__LINE__, pqp_column_(ColumnID{1}, DataType::Int, true, "a + b"));
  expressions.emplace_back(__LINE__, pqp_column_(ColumnID{1}, DataType::Int, true, "alias"));

  // PQPSubqueryExpression
  expressions.emplace_back(__LINE__, pqp_subquery_(std::make_shared<GetTable>("a"), DataType::Int, false));
  expressions.emplace_back(__LINE__, pqp_subquery_(std::make_shared<GetTable>("b"), DataType::Int, false));
  expressions.emplace_back(__LINE__, pqp_subquery_(std::make_shared<GetTable>("b"), DataType::Float, false));
  expressions.emplace_back(__LINE__, pqp_subquery_(std::make_shared<GetTable>("b"), DataType::Float, true));
  const auto parameter = std::make_pair(ParameterID{0}, ColumnID{0});
  expressions.emplace_back(__LINE__, pqp_subquery_(std::make_shared<GetTable>("b"), DataType::Float, true, parameter));

  // UnaryMinusExpression
  expressions.emplace_back(__LINE__, unary_minus_(3));
  expressions.emplace_back(__LINE__, unary_minus_(4));

  // ValueExpression
  expressions.emplace_back(__LINE__, value_(3));
  expressions.emplace_back(__LINE__, value_(3.0f));
  expressions.emplace_back(__LINE__, value_(3.25));
  expressions.emplace_back(__LINE__, null_());

  for (auto first_iter = expressions.begin(); first_iter != expressions.end(); ++first_iter) {
    const auto& [first_line, first_expression] = *first_iter;
    SCOPED_TRACE(std::string{"First expression from line "} + std::to_string(first_line));

    EXPECT_EQ(*first_expression, *first_expression);
    EXPECT_EQ(first_expression->hash(), first_expression->hash());

    std::shared_ptr<AbstractExpression> deep_copy;
    if (!std::dynamic_pointer_cast<PQPSubqueryExpression>(first_expression)) {
      // The deep copy of a PQPSubqueryExpression is not equal to its source, see the comment in _shallow_equals.
      deep_copy = first_expression->deep_copy();
      EXPECT_EQ(*first_expression, *deep_copy);
      EXPECT_EQ(first_expression->hash(), deep_copy->hash());
    }

    for (auto second_iter = first_iter + 1; second_iter != expressions.end(); ++second_iter) {
      const auto& [second_line, second_expression] = *second_iter;
      SCOPED_TRACE(std::string{"Second expression from line "} + std::to_string(second_line));
      EXPECT_NE(*first_expression, *second_expression);
      EXPECT_NE(*second_expression, *first_expression);
      if (deep_copy) {
        EXPECT_NE(*second_expression, *deep_copy);
      }
    }
  }
}

}  // namespace opossum
