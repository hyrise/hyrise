#include <regex>

#include "gtest/gtest.h"

#include "expression/case_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;            // NOLINT
using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LQPSelectExpressionTest : public ::testing::Test {
 public:
  void SetUp() {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));

    int_float_node_a = StoredTableNode::make("int_float");
    a = {int_float_node_a, ColumnID{0}};
    b = {int_float_node_a, ColumnID{1}};

    // clang-format off
    lqp_a =
    AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, uncorrelated_parameter_(ParameterID{0})))),
      ProjectionNode::make(expression_vector(add_(a, uncorrelated_parameter_(ParameterID{0}))),
        int_float_node_a));

    parameter_c = correlated_parameter_(ParameterID{0}, a);
    lqp_c =
    AggregateNode::make(expression_vector(), expression_vector(count_(add_(a, parameter_c))),
      ProjectionNode::make(expression_vector(add_(a, parameter_c)),
        int_float_node_a));
    // clang-format on

    select_a = lqp_select_(lqp_a);
    select_c = lqp_select_(lqp_c, std::make_pair(ParameterID{0}, a));
  }

  void TearDown() { StorageManager::reset(); }

  std::shared_ptr<StoredTableNode> int_float_node_a;
  std::shared_ptr<AbstractLQPNode> lqp_a, lqp_c;
  std::shared_ptr<ParameterExpression> parameter_c;
  std::shared_ptr<LQPSelectExpression> select_a, select_c;
  LQPColumnReference a, b;
};

TEST_F(LQPSelectExpressionTest, DeepEquals) {
  /**
   * Test that when comparing select expressions, the underlying LQPs get compared and so does the Parameter signature
   */

  // clang-format off
  const auto lqp_b =
  AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, uncorrelated_parameter_(ParameterID{0})))),
    ProjectionNode::make(expression_vector(add_(a, uncorrelated_parameter_(ParameterID{0}))),
      int_float_node_a));

  const auto int_float_node_b = StoredTableNode::make("int_float");
  const auto a2 = int_float_node_b->get_column("a");
  const auto parameter_d = correlated_parameter_(ParameterID{0}, a2);
  const auto lqp_d =
  AggregateNode::make(expression_vector(), expression_vector(count_(add_(a, parameter_d))),
    ProjectionNode::make(expression_vector(add_(a, parameter_d)),
      int_float_node_a));

  const auto parameter_e = correlated_parameter_(ParameterID{0}, b);
  const auto lqp_e =
  AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, parameter_d))),
    ProjectionNode::make(expression_vector(add_(a, parameter_d)),
      int_float_node_a));
  // clang-format on

  const auto select_b = lqp_select_(lqp_b);
  const auto select_d = lqp_select_(lqp_d, std::make_pair(ParameterID{0}, a));
  const auto select_e = lqp_select_(lqp_e, std::make_pair(ParameterID{0}, b));

  EXPECT_EQ(*select_a, *select_b);
  EXPECT_NE(*select_a, *select_c);
  EXPECT_EQ(*select_c, *select_d);
  EXPECT_NE(*select_c, *select_e);
}

TEST_F(LQPSelectExpressionTest, DeepCopy) {
  const auto select_a_copy = std::dynamic_pointer_cast<LQPSelectExpression>(select_a->deep_copy());
  EXPECT_EQ(*select_a, *select_a_copy);

  // Check LQP was actually duplicated
  EXPECT_NE(select_a->lqp, select_a_copy->lqp);

  const auto select_c_copy = std::dynamic_pointer_cast<LQPSelectExpression>(select_c->deep_copy());
  EXPECT_EQ(*select_c, *select_c_copy);

  // Check LQP and parameters were actually duplicated
  EXPECT_NE(select_c->lqp, select_c_copy->lqp);
  EXPECT_NE(select_c->arguments[0], select_c_copy->arguments[0]);
}

TEST_F(LQPSelectExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(select_a->requires_computation());
  EXPECT_TRUE(select_c->requires_computation());
}

TEST_F(LQPSelectExpressionTest, DataType) {
  // Can't determine the DataType of this Select, since it depends on a parameter
  EXPECT_ANY_THROW(select_a->data_type());

  EXPECT_EQ(select_c->data_type(), DataType::Long);
}

TEST_F(LQPSelectExpressionTest, IsNullable) {
  EXPECT_TRUE(select_a->is_nullable());
  EXPECT_FALSE(select_c->is_nullable());

  // clang-format off
  const auto lqp_c =
  AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, null_()))),
    ProjectionNode::make(expression_vector(add_(a, null_())),
      int_float_node_a));
  // clang-format off

  EXPECT_TRUE(lqp_select_(lqp_c)->is_nullable());
}

TEST_F(LQPSelectExpressionTest, AsColumnName) {
  EXPECT_TRUE(std::regex_search(select_a->as_column_name(), std::regex{"SUBSELECT \\(LQP, 0x[0-9a-f]+\\)"}));
  EXPECT_TRUE(std::regex_search(select_c->as_column_name(), std::regex{"SUBSELECT \\(LQP, 0x[0-9a-f]+, Parameters: a\\)"}));  // NOLINT

  // Test IN and EXISTS here as well, since they need subselects to function
  EXPECT_TRUE(std::regex_search(exists_(select_c)->as_column_name(), std::regex{"EXISTS\\(SUBSELECT \\(LQP, 0x[0-9a-f]+, Parameters: a\\)\\)"}));  // NOLINT
  EXPECT_TRUE(std::regex_search(in_(5, select_c)->as_column_name(), std::regex{"\\(5\\) IN SUBSELECT \\(LQP, 0x[0-9a-f]+, Parameters: a\\)"}));  // NOLINT
}

}  // namespace opossum
