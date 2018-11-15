#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operators/operator_scan_predicate.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorScanPredicateTest : public ::testing::Test {
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

TEST_F(OperatorScanPredicateTest, FromExpression) {
  const auto operator_predicates_a = OperatorScanPredicate::from_expression(*greater_than_(a, 5), *node);
  ASSERT_TRUE(operator_predicates_a);
  ASSERT_EQ(operator_predicates_a->size(), 1u);
  const auto& operator_predicate_a = operator_predicates_a->at(0);
  EXPECT_EQ(operator_predicate_a.column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_a.predicate_condition, PredicateCondition::GreaterThan);
  EXPECT_EQ(operator_predicate_a.value, AllParameterVariant{5});

  const auto operator_predicates_b = OperatorScanPredicate::from_expression(*less_than_(5, a), *node);
  ASSERT_TRUE(operator_predicates_b);
  ASSERT_EQ(operator_predicates_b->size(), 1u);
  const auto& operator_predicate_b = operator_predicates_b->at(0);
  EXPECT_EQ(operator_predicate_b.column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_b.predicate_condition, PredicateCondition::GreaterThan);
  EXPECT_EQ(operator_predicate_b.value, AllParameterVariant{5});

  const auto operator_predicates_c = OperatorScanPredicate::from_expression(*greater_than_(a, b), *node);
  ASSERT_TRUE(operator_predicates_c);
  ASSERT_EQ(operator_predicates_c->size(), 1u);
  const auto& operator_predicate_c = operator_predicates_c->at(0);
  EXPECT_EQ(operator_predicate_c.column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_c.predicate_condition, PredicateCondition::GreaterThan);
  EXPECT_EQ(operator_predicate_c.value, AllParameterVariant{ColumnID{1}});

  EXPECT_FALSE(OperatorScanPredicate::from_expression(*greater_than_(5, 3), *node));
}

TEST_F(OperatorScanPredicateTest, FromExpressionColumnRight) {
  // `5 > a` becomes `a < 5`
  const auto operator_predicates_a = OperatorScanPredicate::from_expression(*greater_than_(5, a), *node);
  ASSERT_TRUE(operator_predicates_a);
  ASSERT_EQ(operator_predicates_a->size(), 1u);
  const auto& operator_predicate_a = operator_predicates_a->at(0);
  EXPECT_EQ(operator_predicate_a.column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_a.predicate_condition, PredicateCondition::LessThan);
  EXPECT_EQ(operator_predicate_a.value, AllParameterVariant{5});
}

TEST_F(OperatorScanPredicateTest, SimpleBetween) {
  const auto operator_predicates_a = OperatorScanPredicate::from_expression(*between_(a, 5, 7), *node);
  ASSERT_TRUE(operator_predicates_a);
  ASSERT_EQ(operator_predicates_a->size(), 1u);
  const auto& operator_predicate_a = operator_predicates_a->at(0);
  EXPECT_EQ(operator_predicate_a.column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_a.predicate_condition, PredicateCondition::Between);
  EXPECT_EQ(operator_predicate_a.value, AllParameterVariant{5});
  EXPECT_TRUE(operator_predicate_a.value2);
  EXPECT_EQ(*operator_predicate_a.value2, AllParameterVariant{7});
}

TEST_F(OperatorScanPredicateTest, ComplicatedBetween) {
  // `5 BETWEEN a AND b` becomes `a <= 5 AND b >= 5`
  const auto operator_predicates_b = OperatorScanPredicate::from_expression(*between_(5, a, b), *node);
  ASSERT_TRUE(operator_predicates_b);
  ASSERT_EQ(operator_predicates_b->size(), 2u);

  const auto& operator_predicate_b_a = operator_predicates_b->at(0);
  EXPECT_EQ(operator_predicate_b_a.column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_b_a.predicate_condition, PredicateCondition::LessThanEquals);
  EXPECT_EQ(operator_predicate_b_a.value, AllParameterVariant{5});

  const auto& operator_predicate_b_b = operator_predicates_b->at(1);
  EXPECT_EQ(operator_predicate_b_b.column_id, ColumnID{1});
  EXPECT_EQ(operator_predicate_b_b.predicate_condition, PredicateCondition::GreaterThanEquals);
  EXPECT_EQ(operator_predicate_b_b.value, AllParameterVariant{5});
}

TEST_F(OperatorScanPredicateTest, NotConvertible) {
  const auto operator_predicate_a = OperatorScanPredicate::from_expression(*and_(0, greater_than_(a, 5)), *node);
  EXPECT_FALSE(operator_predicate_a);
}

}  // namespace opossum
