#include <regex>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "operators/get_table.hpp"
#include "operators/limit.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;            // NOLINT
using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PQPSubqueryExpressionTest : public BaseTest {
 public:
  void SetUp() override {
    _table = load_table("resources/test_data/tbl/int_float.tbl");
    Hyrise::get().storage_manager.add_table("int_float", _table);
    _a = PQPColumnExpression::from_table(*_table, "a");
    _b = PQPColumnExpression::from_table(*_table, "b");

    // Build a PQP WITHOUT PARAMETERS
    const auto get_table1 = std::make_shared<GetTable>("int_float");
    _pqp = std::make_shared<TableScan>(get_table1, greater_than_(_a, 5));

    // Build a PQP with ONE PARAMETER returning a SINGLE NON-NULLABLE VALUE
    // (1) GetTable
    const auto get_table2 = std::make_shared<GetTable>("int_float");
    // (2) Projection: a + ?
    const auto parameter0 = placeholder_(ParameterID{0});
    const auto projection = std::make_shared<Projection>(get_table2, expression_vector(add_(_a, parameter0)));
    // (3) Limit
    _pqp_with_param = std::make_shared<Limit>(projection, value_(1));

    // Create PQPSubqueryExpression
    _pqp_subquery_expression = std::make_shared<PQPSubqueryExpression>(_pqp);
    // Create PQPSubqueryExpression with Parameter
    _parameters = {std::make_pair(ParameterID{2}, ColumnID{3})};
    _pqp_subquery_expression_with_param =
        std::make_shared<PQPSubqueryExpression>(_pqp_with_param, DataType::Int, false, _parameters);
  }

 protected:
  std::shared_ptr<Table> _table;
  std::shared_ptr<PQPColumnExpression> _a, _b;

  PQPSubqueryExpression::Parameters _parameters;
  std::shared_ptr<AbstractOperator> _pqp;
  std::shared_ptr<AbstractOperator> _pqp_with_param;

  std::shared_ptr<PQPSubqueryExpression> _pqp_subquery_expression;
  std::shared_ptr<PQPSubqueryExpression> _pqp_subquery_expression_with_param;
};

TEST_F(PQPSubqueryExpressionTest, DeepEquals) {
  EXPECT_EQ(*_pqp_subquery_expression_with_param, *_pqp_subquery_expression_with_param);

  // different parameters:
  const auto parameters_b = PQPSubqueryExpression::Parameters{std::make_pair(ParameterID{2}, ColumnID{2})};
  const auto subquery_different_parameter =
      std::make_shared<PQPSubqueryExpression>(_pqp_with_param, DataType::Int, false, parameters_b);
  EXPECT_NE(*_pqp_subquery_expression, *subquery_different_parameter);

  // different PQP:
  const auto pqp_without_limit = _pqp_with_param->mutable_left_input();
  const auto subquery_different_lqp =
      std::make_shared<PQPSubqueryExpression>(pqp_without_limit, DataType::Int, false, _parameters);
  EXPECT_NE(*_pqp_subquery_expression, *subquery_different_lqp);
}

TEST_F(PQPSubqueryExpressionTest, DeepCopy) {
  const auto _pqp_subquery_expression_with_param_copy =
      std::dynamic_pointer_cast<PQPSubqueryExpression>(_pqp_subquery_expression_with_param->deep_copy());
  ASSERT_TRUE(_pqp_subquery_expression_with_param_copy);

  ASSERT_EQ(_pqp_subquery_expression_with_param_copy->parameters.size(), 1u);
  EXPECT_EQ(_pqp_subquery_expression_with_param_copy->parameters[0].first, ParameterID{2});
  EXPECT_EQ(_pqp_subquery_expression_with_param_copy->parameters[0].second, ColumnID{3});
  EXPECT_NE(_pqp_subquery_expression_with_param_copy->pqp, _pqp_subquery_expression_with_param->pqp);
  EXPECT_EQ(_pqp_subquery_expression_with_param_copy->pqp->type(), OperatorType::Limit);

  const auto _pqp_subquery_expression_copy = std::dynamic_pointer_cast<PQPSubqueryExpression>(_pqp_subquery_expression->deep_copy());
  ASSERT_TRUE(_pqp_subquery_expression_copy);

  ASSERT_EQ(_pqp_subquery_expression_copy->parameters.size(), 0u);
  EXPECT_NE(_pqp_subquery_expression_copy->pqp, _pqp_subquery_expression_with_param->pqp);
  EXPECT_EQ(_pqp_subquery_expression_copy->pqp->type(), OperatorType::TableScan);
}

TEST_F(PQPSubqueryExpressionTest, DeepCopyDiamondShape) {
//  auto scan_a = create_table_scan(_table_wrapper_a, ColumnID{0}, PredicateCondition::GreaterThanEquals, 1234);
//  scan_a->execute();
//
//  auto scan_b = create_table_scan(scan_a, ColumnID{1}, PredicateCondition::LessThan, 1000);
//  auto scan_c = create_table_scan(scan_a, ColumnID{1}, PredicateCondition::GreaterThan, 2000);
//  auto union_positions = std::make_shared<UnionPositions>(scan_b, scan_c);
//
//  auto copied_pqp = union_positions->deep_copy();
//
//  EXPECT_EQ(copied_pqp->left_input()->left_input(), copied_pqp->right_input()->left_input());
}

TEST_F(PQPSubqueryExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(_pqp_subquery_expression_with_param->requires_computation());
  EXPECT_TRUE(_pqp_subquery_expression->requires_computation());
}

TEST_F(PQPSubqueryExpressionTest, DataType) {
  // Subquerie returning tables don't have a data type
  EXPECT_ANY_THROW(_pqp_subquery_expression->data_type());
  EXPECT_EQ(_pqp_subquery_expression_with_param->data_type(), DataType::Int);
  const auto subquery_float =
      std::make_shared<PQPSubqueryExpression>(_pqp_with_param, DataType::Float, true, _parameters);
  EXPECT_EQ(subquery_float->data_type(), DataType::Float);
}

TEST_F(PQPSubqueryExpressionTest, IsNullable) {
  // Cannot query nullability of PQP expressions
  EXPECT_ANY_THROW(_pqp_subquery_expression->is_nullable_on_lqp(*DummyTableNode::make()));
}

TEST_F(PQPSubqueryExpressionTest, AsColumnName) {
  EXPECT_TRUE(std::regex_search(_pqp_subquery_expression->as_column_name(), std::regex{"SUBQUERY \\(PQP, 0x[0-9a-f]+\\)"}));
  EXPECT_TRUE(std::regex_search(_pqp_subquery_expression_with_param->as_column_name(),
                                std::regex{"SUBQUERY \\(PQP, 0x[0-9a-f]+\\)"}));
}

}  // namespace opossum
