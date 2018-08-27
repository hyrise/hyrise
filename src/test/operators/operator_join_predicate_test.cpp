#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operators/operator_join_predicate.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorJoinPredicateTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::CxlumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    a_a = node_a->get_cxlumn("a");
    a_b = node_a->get_cxlumn("b");

    node_b = MockNode::make(MockNode::CxlumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    b_a = node_b->get_cxlumn("a");
    b_b = node_b->get_cxlumn("b");
  }

  std::shared_ptr<MockNode> node_a, node_b;
  LQPCxlumnReference a_a, a_b, b_a, b_b;
};

TEST_F(OperatorJoinPredicateTest, Conversion) {
  const auto predicate_a = OperatorJoinPredicate::from_expression(*equals_(a_a, b_b), *node_a, *node_b);
  ASSERT_TRUE(predicate_a);
  EXPECT_EQ(predicate_a->cxlumn_ids.first, CxlumnID{0});
  EXPECT_EQ(predicate_a->cxlumn_ids.second, CxlumnID{1});
  EXPECT_EQ(predicate_a->predicate_condition, PredicateCondition::Equals);

  const auto predicate_b = OperatorJoinPredicate::from_expression(*less_than_(b_a, a_b), *node_a, *node_b);
  ASSERT_TRUE(predicate_b);
  EXPECT_EQ(predicate_b->cxlumn_ids.first, CxlumnID{1});
  EXPECT_EQ(predicate_b->cxlumn_ids.second, CxlumnID{0});
  EXPECT_EQ(predicate_b->predicate_condition, PredicateCondition::GreaterThan);
}

TEST_F(OperatorJoinPredicateTest, ConversionImpossible) {
  const auto predicate_a = OperatorJoinPredicate::from_expression(*equals_(a_a, a_b), *node_a, *node_b);
  ASSERT_FALSE(predicate_a);

  const auto predicate_b = OperatorJoinPredicate::from_expression(*less_than_(add_(b_a, 5), a_b), *node_a, *node_b);
  ASSERT_FALSE(predicate_b);
}

}  // namespace opossum
