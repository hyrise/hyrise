#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "operators/operator_scan_predicate.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OperatorScanPredicateTest : public BaseTest {
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

TEST_F(OperatorScanPredicateTest, FromExpressionPlaceholder) {
  // `a = <correlated_param>`
  const auto operator_predicates_a =
      OperatorScanPredicate::from_expression(*equals_(a, correlated_parameter_(ParameterID{1}, b)), *node);
  ASSERT_TRUE(operator_predicates_a);
  ASSERT_EQ(operator_predicates_a->size(), 1u);
  const auto& operator_predicate_a = operator_predicates_a->at(0);
  EXPECT_EQ(operator_predicate_a.column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_a.predicate_condition, PredicateCondition::Equals);
  EXPECT_EQ(operator_predicate_a.value, AllParameterVariant{ParameterID{1}});

  // `a = ?`
  const auto operator_predicates_b =
      OperatorScanPredicate::from_expression(*equals_(a, placeholder_(ParameterID{2})), *node);
  ASSERT_TRUE(operator_predicates_b);
  ASSERT_EQ(operator_predicates_b->size(), 1u);
  const auto& operator_predicate_b = operator_predicates_b->at(0);
  EXPECT_EQ(operator_predicate_b.column_id, ColumnID{0});
  EXPECT_EQ(operator_predicate_b.predicate_condition, PredicateCondition::Equals);
  EXPECT_EQ(operator_predicate_b.value, AllParameterVariant{ParameterID{2}});
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

TEST_F(OperatorScanPredicateTest, SimpleBetweenInclusive) {
  const auto between_inclusive = OperatorScanPredicate::from_expression(*between_inclusive_(a, 5, 7), *node);
  ASSERT_TRUE(between_inclusive);
  ASSERT_EQ(between_inclusive->size(), 1u);

  EXPECT_EQ(between_inclusive->at(0), OperatorScanPredicate(ColumnID{0}, PredicateCondition::BetweenInclusive, 5, 7));

  const auto between_upper_exclusive =
      OperatorScanPredicate::from_expression(*between_upper_exclusive_(a, 5, 7), *node);
  ASSERT_TRUE(between_upper_exclusive);
  ASSERT_EQ(between_upper_exclusive->size(), 1u);

  EXPECT_EQ(between_upper_exclusive->at(0),
            OperatorScanPredicate(ColumnID{0}, PredicateCondition::BetweenUpperExclusive, 5, 7));

  const auto between_lower_exclusive =
      OperatorScanPredicate::from_expression(*between_lower_exclusive_(a, 5, 7), *node);
  ASSERT_TRUE(between_lower_exclusive);
  ASSERT_EQ(between_lower_exclusive->size(), 1u);

  EXPECT_EQ(between_lower_exclusive->at(0),
            OperatorScanPredicate(ColumnID{0}, PredicateCondition::BetweenLowerExclusive, 5, 7));

  const auto between_exclusive = OperatorScanPredicate::from_expression(*between_exclusive_(a, 5, 7), *node);
  ASSERT_TRUE(between_exclusive);
  ASSERT_EQ(between_exclusive->size(), 1u);

  EXPECT_EQ(between_exclusive->at(0), OperatorScanPredicate(ColumnID{0}, PredicateCondition::BetweenExclusive, 5, 7));
}

TEST_F(OperatorScanPredicateTest, SimpleBetweenUpperExclusive) {}

TEST_F(OperatorScanPredicateTest, SimpleBetweenLowerExclusive) {}

TEST_F(OperatorScanPredicateTest, SimpleBetweenExclusive) {}

TEST_F(OperatorScanPredicateTest, ComplicatedBetween) {
  // `5 BETWEEN INCLUSIVE a AND b` becomes `a <= 5 AND b >= 5`
  const auto operator_predicates_a = OperatorScanPredicate::from_expression(*between_inclusive_(5, a, b), *node);
  ASSERT_TRUE(operator_predicates_a);
  ASSERT_EQ(operator_predicates_a->size(), 2u);

  EXPECT_EQ(operator_predicates_a->at(0), OperatorScanPredicate(ColumnID{0}, PredicateCondition::LessThanEquals, 5));
  EXPECT_EQ(operator_predicates_a->at(1), OperatorScanPredicate(ColumnID{1}, PredicateCondition::GreaterThanEquals, 5));

  // `5 BETWEEN EXCLUSIVE a AND b` becomes `a < 5 AND b > 5`
  const auto operator_predicates_b = OperatorScanPredicate::from_expression(*between_exclusive_(5, a, b), *node);
  ASSERT_TRUE(operator_predicates_b);
  ASSERT_EQ(operator_predicates_b->size(), 2u);

  EXPECT_EQ(operator_predicates_b->at(0), OperatorScanPredicate(ColumnID{0}, PredicateCondition::LessThan, 5));
  EXPECT_EQ(operator_predicates_b->at(1), OperatorScanPredicate(ColumnID{1}, PredicateCondition::GreaterThan, 5));
}

TEST_F(OperatorScanPredicateTest, NotConvertible) {
  const auto operator_predicate_a = OperatorScanPredicate::from_expression(*and_(0, greater_than_(a, 5)), *node);
  EXPECT_FALSE(operator_predicate_a);
}

}  // namespace opossum
