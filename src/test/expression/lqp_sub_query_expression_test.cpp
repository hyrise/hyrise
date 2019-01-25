#include <regex>

#include "base_test.hpp"

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

class LQPSubQueryExpressionTest : public BaseTest {
 public:
  void SetUp() {
    StorageManager::get().add_table("int_float", load_table("resources/test_data/tbl/int_float.tbl"));

    int_float_node_a = StoredTableNode::make("int_float");
    a = {int_float_node_a, ColumnID{0}};
    b = {int_float_node_a, ColumnID{1}};

    // clang-format off
    lqp_a =
    AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, placeholder_(ParameterID{0})))),
      ProjectionNode::make(expression_vector(add_(a, placeholder_(ParameterID{0}))),
        int_float_node_a));

    parameter_c = correlated_parameter_(ParameterID{0}, a);
    lqp_c =
    AggregateNode::make(expression_vector(), expression_vector(count_(add_(a, parameter_c))),
      ProjectionNode::make(expression_vector(add_(a, parameter_c)),
        int_float_node_a));
    // clang-format on

    sub_query_a = lqp_sub_query_(lqp_a);
    sub_query_c = lqp_sub_query_(lqp_c, std::make_pair(ParameterID{0}, a));
  }

  std::shared_ptr<StoredTableNode> int_float_node_a;
  std::shared_ptr<AbstractLQPNode> lqp_a, lqp_c;
  std::shared_ptr<CorrelatedParameterExpression> parameter_c;
  std::shared_ptr<LQPSubQueryExpression> sub_query_a, sub_query_c;
  LQPColumnReference a, b;
};

TEST_F(LQPSubQueryExpressionTest, DeepEquals) {
  /**
   * Test that when comparing sub query expressions, the underlying LQPs get compared and so does the Parameter
   * signature
   */

  // clang-format off
  const auto lqp_b =
  AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, placeholder_(ParameterID{0})))),
    ProjectionNode::make(expression_vector(add_(a, placeholder_(ParameterID{0}))),
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

  const auto sub_query_b = lqp_sub_query_(lqp_b);
  const auto sub_query_d = lqp_sub_query_(lqp_d, std::make_pair(ParameterID{0}, a));
  const auto sub_query_e = lqp_sub_query_(lqp_e, std::make_pair(ParameterID{0}, b));

  EXPECT_EQ(*sub_query_a, *sub_query_b);
  EXPECT_NE(*sub_query_a, *sub_query_c);
  EXPECT_EQ(*sub_query_c, *sub_query_d);
  EXPECT_NE(*sub_query_c, *sub_query_e);
}

TEST_F(LQPSubQueryExpressionTest, DeepCopy) {
  const auto sub_query_a_copy = std::dynamic_pointer_cast<LQPSubQueryExpression>(sub_query_a->deep_copy());
  EXPECT_EQ(*sub_query_a, *sub_query_a_copy);

  // Check LQP was actually duplicated
  EXPECT_NE(sub_query_a->lqp, sub_query_a_copy->lqp);

  const auto sub_query_c_copy = std::dynamic_pointer_cast<LQPSubQueryExpression>(sub_query_c->deep_copy());
  EXPECT_EQ(*sub_query_c, *sub_query_c_copy);

  // Check LQP and parameters were actually duplicated
  EXPECT_NE(sub_query_c->lqp, sub_query_c_copy->lqp);
  EXPECT_NE(sub_query_c->arguments[0], sub_query_c_copy->arguments[0]);
}

TEST_F(LQPSubQueryExpressionTest, RequiresCalculation) {
  EXPECT_TRUE(sub_query_a->requires_computation());
  EXPECT_TRUE(sub_query_c->requires_computation());
}

TEST_F(LQPSubQueryExpressionTest, DataType) {
  // Can't determine the DataType of this Select, since it depends on a parameter
  EXPECT_ANY_THROW(sub_query_a->data_type());

  EXPECT_EQ(sub_query_c->data_type(), DataType::Long);
}

TEST_F(LQPSubQueryExpressionTest, IsNullable) {
  EXPECT_TRUE(sub_query_a->is_nullable());
  EXPECT_FALSE(sub_query_c->is_nullable());

  // clang-format off
  const auto lqp_c =
  AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, null_()))),
    ProjectionNode::make(expression_vector(add_(a, null_())),
      int_float_node_a));
  // clang-format off

  EXPECT_TRUE(lqp_sub_query_(lqp_c)->is_nullable());
}

TEST_F(LQPSubQueryExpressionTest, AsColumnName) {
  EXPECT_TRUE(std::regex_search(sub_query_a->as_column_name(), std::regex{"SUBQUERY \\(LQP, 0x[0-9a-f]+\\)"}));
  EXPECT_TRUE(std::regex_search(sub_query_c->as_column_name(), std::regex{"SUBQUERY \\(LQP, 0x[0-9a-f]+, Parameters: \\[a, id=0\\]\\)"}));  // NOLINT

  // Test IN and EXISTS here as well, since they need sub_querys to function
  EXPECT_TRUE(std::regex_search(exists_(sub_query_c)->as_column_name(), std::regex{"EXISTS\\(SUBQUERY \\(LQP, 0x[0-9a-f]+, Parameters: \\[a, id=0\\]\\)\\)"}));  // NOLINT
  EXPECT_TRUE(std::regex_search(in_(5, sub_query_c)->as_column_name(), std::regex{"\\(5\\) IN SUBQUERY \\(LQP, 0x[0-9a-f]+, Parameters: \\[a, id=0\\]\\)"}));  // NOLINT
}

}  // namespace opossum
