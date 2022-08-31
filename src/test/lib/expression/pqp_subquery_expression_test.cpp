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
#include "operators/union_positions.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;           // NOLINT
using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class PQPSubqueryExpressionTest : public BaseTest {
 public:
  void SetUp() override {
    _table = load_table("resources/test_data/tbl/int_float.tbl");
    Hyrise::get().storage_manager.add_table("int_float", _table);
    _a = PQPColumnExpression::from_table(*_table, "a");
    _b = PQPColumnExpression::from_table(*_table, "b");

    // Create PQPSubqueryExpression without parameters
    {
      const auto get_table = std::make_shared<GetTable>(_table_name);
      // TableScan (a > 5)
      _pqp = std::make_shared<TableScan>(get_table, greater_than_(_a, 5));

      _pqp_subquery_expression = std::make_shared<PQPSubqueryExpression>(_pqp);
    }

    // Create PQPSubqueryExpression with parameters
    {
      // Build a PQP with one parameter returning a single non-nullable value
      const auto get_table = std::make_shared<GetTable>(_table_name);
      // Projection (a + ?)
      const auto parameter = placeholder_(ParameterID{0});
      const auto projection = std::make_shared<Projection>(get_table, expression_vector(add_(_a, parameter)));
      _pqp_with_param = std::make_shared<Limit>(projection, value_(1));

      _parameters = {std::make_pair(ParameterID{0}, ColumnID{0})};
      _pqp_subquery_expression_with_param =
          std::make_shared<PQPSubqueryExpression>(_pqp_with_param, DataType::Int, false, _parameters);
    }
  }

 protected:
  std::string _table_name = "int_float";
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
  const auto parameters_b = PQPSubqueryExpression::Parameters{std::make_pair(ParameterID{0}, ColumnID{2})};
  const auto subquery_different_parameter =
      std::make_shared<PQPSubqueryExpression>(_pqp_with_param, DataType::Int, false, parameters_b);
  EXPECT_NE(*_pqp_subquery_expression_with_param, *subquery_different_parameter);

  // different PQP:
  const auto pqp_without_limit = _pqp_with_param->mutable_left_input();
  const auto subquery_different_lqp =
      std::make_shared<PQPSubqueryExpression>(pqp_without_limit, DataType::Int, false, _parameters);
  EXPECT_NE(*_pqp_subquery_expression_with_param, *subquery_different_lqp);
}

TEST_F(PQPSubqueryExpressionTest, DeepCopy) {
  // With Parameter
  const auto _pqp_subquery_expression_with_param_copy =
      std::dynamic_pointer_cast<PQPSubqueryExpression>(_pqp_subquery_expression_with_param->deep_copy());
  ASSERT_TRUE(_pqp_subquery_expression_with_param_copy);

  ASSERT_EQ(_pqp_subquery_expression_with_param_copy->parameters.size(), 1u);
  EXPECT_EQ(_pqp_subquery_expression_with_param_copy->parameters[0].first, ParameterID{0});
  EXPECT_EQ(_pqp_subquery_expression_with_param_copy->parameters[0].second, ColumnID{0});
  EXPECT_NE(_pqp_subquery_expression_with_param_copy->pqp, _pqp_subquery_expression_with_param->pqp);
  EXPECT_EQ(_pqp_subquery_expression_with_param_copy->pqp->type(), OperatorType::Limit);

  // Without Parameter
  const auto _pqp_subquery_expression_copy =
      std::dynamic_pointer_cast<PQPSubqueryExpression>(_pqp_subquery_expression->deep_copy());
  ASSERT_TRUE(_pqp_subquery_expression_copy);

  ASSERT_EQ(_pqp_subquery_expression_copy->parameters.size(), 0u);
  EXPECT_NE(_pqp_subquery_expression_copy->pqp, _pqp_subquery_expression_with_param->pqp);
  EXPECT_EQ(_pqp_subquery_expression_copy->pqp->type(), OperatorType::TableScan);
}

TEST_F(PQPSubqueryExpressionTest, DeepCopyPreservesPlanDeduplication) {
  // Prepare DIAMOND-SHAPED PQP
  auto get_table = std::make_shared<GetTable>(_table_name);
  auto scan_a = std::make_shared<TableScan>(get_table, greater_than_(_a, 5));
  auto scan_b = std::make_shared<TableScan>(get_table, greater_than_(_b, 10));
  auto union_positions = std::make_shared<UnionPositions>(scan_a, scan_b);
  auto pqp_subquery_expression = std::make_shared<PQPSubqueryExpression>(union_positions);
  ASSERT_EQ(get_table->consumer_count(), 2);

  const auto copied_pqp_subquery_expression =
      std::dynamic_pointer_cast<PQPSubqueryExpression>(pqp_subquery_expression->deep_copy());
  const auto copied_pqp = copied_pqp_subquery_expression->pqp;

  EXPECT_NE(pqp_subquery_expression->pqp, copied_pqp);
  EXPECT_EQ(get_table->consumer_count(), copied_pqp->left_input()->left_input()->consumer_count());
}

TEST_F(PQPSubqueryExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(_pqp_subquery_expression_with_param->requires_computation());
  EXPECT_TRUE(_pqp_subquery_expression->requires_computation());
}

TEST_F(PQPSubqueryExpressionTest, DataType) {
  // Subqueries returning tables don't have a data type
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
  EXPECT_TRUE(
      std::regex_search(_pqp_subquery_expression->as_column_name(), std::regex{"SUBQUERY \\(PQP, 0x[0-9a-f]+\\)"}));
  EXPECT_TRUE(std::regex_search(_pqp_subquery_expression_with_param->as_column_name(),
                                std::regex{"SUBQUERY \\(PQP, 0x[0-9a-f]+\\)"}));
}

}  // namespace hyrise
