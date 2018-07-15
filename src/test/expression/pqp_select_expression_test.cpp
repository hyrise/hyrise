#include <regex>

#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "operators/get_table.hpp"
#include "operators/limit.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;            // NOLINT
using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PQPSelectExpressionTest : public ::testing::Test {
 public:
  void SetUp() {
    table_a = load_table("src/test/tables/int_float.tbl");
    StorageManager::get().add_table("int_float", table_a);
    a_a = PQPColumnExpression::from_table(*table_a, "a");
    a_b = PQPColumnExpression::from_table(*table_a, "b");

    // Build a Select returning a SINGLE NON-NULLABLE VALUE and taking ONE PARAMETER
    const auto parameter_a = parameter_(ParameterID{2});
    const auto get_table_a = std::make_shared<GetTable>("int_float");
    const auto projection_a = std::make_shared<Projection>(get_table_a, expression_vector(add_(a_a, parameter_a)));
    const auto limit_a = std::make_shared<Limit>(projection_a, value_(1));
    pqp_single_value_one_parameter = limit_a;
    parameters_a = {std::make_pair(ParameterID{2}, ColumnID{3})};
    select_single_value_one_parameter =
        std::make_shared<PQPSelectExpression>(pqp_single_value_one_parameter, DataType::Int, false, parameters_a);

    // Build a Select returning a TABLE and taking NO PARAMETERS
    const auto get_table_b = std::make_shared<GetTable>("int_float");
    const auto table_scan_b = std::make_shared<TableScan>(get_table_b, ColumnID{0}, PredicateCondition::GreaterThan, 5);
    pqp_table = table_scan_b;
    select_table = std::make_shared<PQPSelectExpression>(pqp_table);
  }

  void TearDown() { StorageManager::reset(); }

  std::shared_ptr<Table> table_a;
  std::shared_ptr<PQPColumnExpression> a_a, a_b;
  PQPSelectExpression::Parameters parameters_a;
  std::shared_ptr<PQPSelectExpression> select_single_value_one_parameter;
  std::shared_ptr<PQPSelectExpression> select_table;
  std::shared_ptr<AbstractOperator> pqp_single_value_one_parameter;
  std::shared_ptr<AbstractOperator> pqp_table;
};

TEST_F(PQPSelectExpressionTest, DeepEquals) {
  // Consume the result of an equality check to avoid "equality comparison result unused [-Werror,-Wunused-comparison]"
  // errors
  const auto dummy = [](const auto v) {};

  // Can't compare PQPSelectExpressions (since we can't compare PQPs)
  EXPECT_ANY_THROW(dummy(*select_single_value_one_parameter == *select_single_value_one_parameter));
  EXPECT_ANY_THROW(dummy(*select_table == *select_table));
}

TEST_F(PQPSelectExpressionTest, DeepCopy) {
  const auto select_single_value_one_parameter_copy =
      std::dynamic_pointer_cast<PQPSelectExpression>(select_single_value_one_parameter->deep_copy());
  ASSERT_TRUE(select_single_value_one_parameter_copy);

  ASSERT_EQ(select_single_value_one_parameter_copy->parameters.size(), 1u);
  EXPECT_EQ(select_single_value_one_parameter_copy->parameters[0].first, ParameterID{2});
  EXPECT_EQ(select_single_value_one_parameter_copy->parameters[0].second, ColumnID{3});
  EXPECT_NE(select_single_value_one_parameter_copy->pqp, select_single_value_one_parameter->pqp);
  EXPECT_EQ(select_single_value_one_parameter_copy->pqp->type(), OperatorType::Limit);

  const auto select_table_copy = std::dynamic_pointer_cast<PQPSelectExpression>(select_table->deep_copy());
  ASSERT_TRUE(select_table_copy);

  ASSERT_EQ(select_table_copy->parameters.size(), 0u);
  EXPECT_NE(select_table_copy->pqp, select_single_value_one_parameter->pqp);
  EXPECT_EQ(select_table_copy->pqp->type(), OperatorType::TableScan);
}

TEST_F(PQPSelectExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(select_single_value_one_parameter->requires_computation());
  EXPECT_TRUE(select_table->requires_computation());
}

TEST_F(PQPSelectExpressionTest, DataType) {
  // Selects returning tables don't have a data type
  EXPECT_ANY_THROW(select_table->data_type());
  EXPECT_EQ(select_single_value_one_parameter->data_type(), DataType::Int);
  const auto select_float =
      std::make_shared<PQPSelectExpression>(pqp_single_value_one_parameter, DataType::Float, true, parameters_a);
  EXPECT_EQ(select_float->data_type(), DataType::Float);
}

TEST_F(PQPSelectExpressionTest, IsNullable) {
  /** Nullability of PQPSelectExpressions only depends on the parameter passed */

  EXPECT_ANY_THROW(select_table->is_nullable());
  EXPECT_FALSE(select_single_value_one_parameter->is_nullable());
  const auto select_nullable =
      std::make_shared<PQPSelectExpression>(pqp_single_value_one_parameter, DataType::Int, true, parameters_a);
  EXPECT_TRUE(select_nullable->is_nullable());
}

TEST_F(PQPSelectExpressionTest, AsColumnName) {
  EXPECT_TRUE(std::regex_search(select_table->as_column_name(), std::regex{"SUBSELECT \\(PQP, 0x[0-9a-f]+\\)"}));
  EXPECT_TRUE(std::regex_search(select_single_value_one_parameter->as_column_name(),
                                std::regex{"SUBSELECT \\(PQP, 0x[0-9a-f]+\\)"}));
}

}  // namespace opossum
