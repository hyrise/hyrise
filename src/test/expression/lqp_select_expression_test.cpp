#include "gtest/gtest.h"

#include "expression/case_expression.hpp"
#include "expression/expression_factory.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "utils/load_table.hpp"
#include "storage/storage_manager.hpp"

using namespace std::string_literals;  // NOLINT
using namespace opossum::expression_factory;  // NOLINT

namespace opossum {

class LQPSelectExpressionTest : public ::testing::Test {
 public:
  void SetUp() {
    StorageManager::get().add_table("int_float", load_table("src/test/tables/int_float.tbl"));

    int_float_node_a = StoredTableNode::make("int_float");
    a = {int_float_node_a, ColumnID{0}};
    b = {int_float_node_a, ColumnID{1}};

    case_a = case_(equals(add(a, 5), b), add(5, b), a);
    case_b = case_(a, 1, 3);
    case_c = case_(equals(a, 123), b, case_(equals(a, 1234), a, null()));
  }

  void TearDown() {
    StorageManager::reset();
  }

  LQPColumnReference a, b;
  std::shared_ptr<AbstractExpression> case_a, case_b, case_c;
  std::shared_ptr<StoredTableNode> int_float_node_a;
};

TEST_F(LQPSelectExpressionTest, Equals) {
  // clang-format off
  const auto lqp_a =
  AggregateNode::make(expression_vector(), expression_vector(max(add(a, parameter(ParameterID{0})))),
    ProjectionNode::make(expression_vector(add(a, parameter(ParameterID{0}))),
      int_float_node_a
  ));
  const auto select_a = select(lqp_a);

  const auto lqp_b =
  AggregateNode::make(expression_vector(), expression_vector(max(add(a, parameter(ParameterID{0})))),
    ProjectionNode::make(expression_vector(add(a, parameter(ParameterID{0}))),
      int_float_node_a
  ));
  const auto select_b = select(lqp_b);

  const auto parameter_c = parameter(ParameterID{0}, a);
  const auto lqp_c =
  AggregateNode::make(expression_vector(), expression_vector(max(add(a, parameter_c))),
    ProjectionNode::make(expression_vector(add(a, parameter_c)),
      int_float_node_a
  ));
  const auto select_c = select(lqp_c, std::make_pair(ParameterID{0}, a));

  const auto int_float_node_b = StoredTableNode::make("int_float");
  const auto a2 = int_float_node_b->get_column("a");
  const auto parameter_d = parameter(ParameterID{0}, a2);
  const auto lqp_d =
  AggregateNode::make(expression_vector(), expression_vector(max(add(a, parameter_d))),
    ProjectionNode::make(expression_vector(add(a, parameter_d)),
      int_float_node_a
  ));
  const auto select_d = select(lqp_d, std::make_pair(ParameterID{0}, a));

  const auto parameter_e = parameter(ParameterID{0}, b);
  const auto lqp_e =
  AggregateNode::make(expression_vector(), expression_vector(max(add(a, parameter_d))),
    ProjectionNode::make(expression_vector(add(a, parameter_d)),
      int_float_node_a
  ));
  const auto select_e = select(lqp_e, std::make_pair(ParameterID{0}, b));
  // clang-format on

  EXPECT_TRUE(select_a->deep_equals(*select_b));
  EXPECT_FALSE(select_a->deep_equals(*select_c));
  EXPECT_TRUE(select_c->deep_equals(*select_d));
  EXPECT_FALSE(select_c->deep_equals(*select_e));
}

}  // namespace opossum
