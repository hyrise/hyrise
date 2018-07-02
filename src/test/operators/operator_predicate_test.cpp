#include "gtest/gtest.h"

#include "expression/expression_factory.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operators/operator_predicate.hpp"

using namespace opossum::expression_factory;  // NOLINT

namespace opossum {

class OperatorPredicateTest : public ::testing::Test {
 public:
  void SetUp() override {
    node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}, {DataType::String, "c"}});
    a = node->get_column("a");
    b = node->get_column("b");
    c = node->get_column("c");
  }

  std::shared_ptr<MockNode> node;
  LQPColumnReference a, b, c;
};

TEST_F(OperatorPredicateTest, FromExpression) {
  const auto operator_predicate_a = OperatorPredicate::from_expression(*greater_than(a, 5), *node);
  ASSERT_TRUE(operator_predicate_a);
  EXPECT_EQ(operator_predicate_a->column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_a->predicate_condition, PredicateCondition::GreaterThan);
  EXPECT_EQ(operator_predicate_a->value, AllParameterVariant{5});
  EXPECT_FALSE(operator_predicate_a->value2);

  const auto operator_predicate_b = OperatorPredicate::from_expression(*and_(0, greater_than(a, 5)), *node);
  EXPECT_FALSE(operator_predicate_b);
}

}  // namespace opossum
