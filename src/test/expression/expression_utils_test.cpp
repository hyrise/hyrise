#include "gtest/gtest.h"

#include "expression/expression_utils.hpp"
#include "expression/expression_factory.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/mock_node.hpp"

using namespace opossum::expression_factory;

namespace opossum {

class ExpressionUtilsTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}});
    a_a = LQPColumnReference{node_a, ColumnID{0}};
    a_b = LQPColumnReference{node_a, ColumnID{1}};
    a_c = LQPColumnReference{node_a, ColumnID{2}};
  }

  std::shared_ptr<MockNode> node_a;
  LQPColumnReference a_a, a_b, a_c;
};

TEST_F(ExpressionUtilsTest, ExpressionFlattenConjunction) {
  // a > 5 AND b < 6 AND c = 7
  const auto expression = and_(and_(greater_than(a_a, 5), less_than(a_b, 6)), equals(a_c, 7));
  const auto flattened_expressions = expression_flatten_conjunction(expression);

  ASSERT_EQ(flattened_expressions.size(), 3u);
  EXPECT_TRUE(flattened_expressions.at(0)->deep_equals(*equals(a_c, 7)));
  EXPECT_TRUE(flattened_expressions.at(1)->deep_equals(*greater_than(a_a, 5)));
  EXPECT_TRUE(flattened_expressions.at(2)->deep_equals(*less_than(a_b, 6)));
}

}  // namespace opossum
