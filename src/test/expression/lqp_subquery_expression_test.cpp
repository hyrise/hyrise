#include <regex>

#include "base_test.hpp"

#include "expression/case_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "utils/load_table.hpp"

using namespace std::string_literals;            // NOLINT
using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LQPSubqueryExpressionTest : public BaseTest {
 public:
  void SetUp() {
    Hyrise::get().storage_manager.add_table("int_float", load_table("resources/test_data/tbl/int_float.tbl"));

    int_float_node_a = StoredTableNode::make("int_float");
    a = {int_float_node_a, ColumnID{0}};
    b = {int_float_node_a, ColumnID{1}};

    int_float_node_a_2 = StoredTableNode::make("int_float");
    a_2 = {int_float_node_a_2, ColumnID{0}};

    // clang-format off
    lqp_a =
    AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, placeholder_(ParameterID{0})))),
      ProjectionNode::make(expression_vector(add_(a, placeholder_(ParameterID{0}))),
        int_float_node_a));

    parameter_a = correlated_parameter_(ParameterID{0}, a_2);
    lqp_c =
    AggregateNode::make(expression_vector(), expression_vector(count_(add_(a, parameter_a))),
      ProjectionNode::make(expression_vector(add_(a, parameter_a)),
        int_float_node_a));
    // clang-format on

    subquery_a = lqp_subquery_(lqp_a);
    subquery_c = lqp_subquery_(lqp_c, std::make_pair(ParameterID{0}, a_2));
  }

  std::shared_ptr<StoredTableNode> int_float_node_a, int_float_node_a_2;
  std::shared_ptr<AbstractLQPNode> lqp_a, lqp_c;
  std::shared_ptr<CorrelatedParameterExpression> parameter_a;
  std::shared_ptr<LQPSubqueryExpression> subquery_a, subquery_c;
  LQPColumnReference a, b, a_2;
};

TEST_F(LQPSubqueryExpressionTest, DeepEquals) {
  /**
   * Test that when comparing sub query expressions, the underlying LQPs get compared and so does the Parameter
   * signature
   */

  // clang-format off
  const auto lqp_b =
  AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, placeholder_(ParameterID{0})))),
    ProjectionNode::make(expression_vector(add_(a, placeholder_(ParameterID{0}))),
      int_float_node_a));

  const auto parameter_d = correlated_parameter_(ParameterID{0}, a_2);
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

  const auto subquery_b = lqp_subquery_(lqp_b);
  const auto subquery_d = lqp_subquery_(lqp_d, std::make_pair(ParameterID{0}, a_2));
  const auto subquery_e = lqp_subquery_(lqp_e, std::make_pair(ParameterID{0}, b));

  EXPECT_EQ(*subquery_a, *subquery_b);
  EXPECT_NE(*subquery_a, *subquery_c);
  EXPECT_EQ(*subquery_c, *subquery_d);
  EXPECT_NE(*subquery_c, *subquery_e);
}

TEST_F(LQPSubqueryExpressionTest, DeepCopy) {
  const auto subquery_a_copy = std::dynamic_pointer_cast<LQPSubqueryExpression>(subquery_a->deep_copy());
  EXPECT_EQ(*subquery_a, *subquery_a_copy);

  // Check LQP was actually duplicated
  EXPECT_NE(subquery_a->lqp, subquery_a_copy->lqp);

  const auto subquery_c_copy = std::dynamic_pointer_cast<LQPSubqueryExpression>(subquery_c->deep_copy());
  EXPECT_EQ(*subquery_c, *subquery_c_copy);

  // Check LQP and parameters were actually duplicated
  EXPECT_NE(subquery_c->lqp, subquery_c_copy->lqp);
  EXPECT_NE(subquery_c->arguments[0], subquery_c_copy->arguments[0]);
}

TEST_F(LQPSubqueryExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(subquery_a->requires_computation());
  EXPECT_TRUE(subquery_c->requires_computation());
}

TEST_F(LQPSubqueryExpressionTest, DataType) {
  // Can't determine the DataType of this Subquery, since it depends on a parameter
  EXPECT_ANY_THROW(subquery_a->data_type());

  EXPECT_EQ(subquery_c->data_type(), DataType::Long);
}

TEST_F(LQPSubqueryExpressionTest, IsNullable) {
  EXPECT_TRUE(subquery_a->is_nullable_on_lqp(*int_float_node_a_2));
  EXPECT_FALSE(subquery_c->is_nullable_on_lqp(*int_float_node_a_2));

  // clang-format of
  const auto lqp_c = AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, null_()))),
                                         ProjectionNode::make(expression_vector(add_(a, null_())), int_float_node_a));
  // clang-format off

  EXPECT_TRUE(lqp_subquery_(lqp_c)->is_nullable_on_lqp(*int_float_node_a_2));
}

TEST_F(LQPSubqueryExpressionTest, AsColumnName) {
  EXPECT_TRUE(std::regex_search(subquery_a->as_column_name(), std::regex{"SUBQUERY \\(LQP, 0x[0-9a-f]+\\)"}));
  EXPECT_TRUE(std::regex_search(subquery_c->as_column_name(), std::regex{"SUBQUERY \\(LQP, 0x[0-9a-f]+, Parameters: \\[a, id=0\\]\\)"}));  // NOLINT

  // Test IN and EXISTS here as well, since they need subqueries to function
  EXPECT_TRUE(std::regex_search(exists_(subquery_c)->as_column_name(), std::regex{"EXISTS\\(SUBQUERY \\(LQP, 0x[0-9a-f]+, Parameters: \\[a, id=0\\]\\)\\)"}));  // NOLINT
  EXPECT_TRUE(std::regex_search(in_(5, subquery_c)->as_column_name(), std::regex{"\\(5\\) IN SUBQUERY \\(LQP, 0x[0-9a-f]+, Parameters: \\[a, id=0\\]\\)"}));  // NOLINT
}

}  // namespace opossum
