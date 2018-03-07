#include <string>

#include "gtest/gtest.h"

#include "logical_query_plan/mock_node.hpp"
#include "optimizer/join_ordering/join_plan_predicate.hpp"

using namespace std::string_literals;  // NOLINT

namespace opossum {

class JoinPlanPredicateTest : public ::testing::Test {
 public:
  void SetUp() override {
    _mock_node = std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    _column_a = _mock_node->get_column("a"s);
    _column_b = _mock_node->get_column("b"s);

    _predicate_a = std::make_shared<JoinPlanAtomicPredicate>(_column_a, PredicateCondition::Equals, 5);
    _predicate_b = std::make_shared<JoinPlanAtomicPredicate>(_column_a, PredicateCondition::LessThanEquals, _column_b);

    // clang-format off
    _predicate_c =
    std::make_shared<JoinPlanLogicalPredicate>(
      std::make_shared<JoinPlanLogicalPredicate>(
        std::make_shared<JoinPlanAtomicPredicate>(_column_a, PredicateCondition::GreaterThan, _column_b),
        JoinPlanPredicateLogicalOperator::Or,
        std::make_shared<JoinPlanAtomicPredicate>(_column_a, PredicateCondition::GreaterThan, 5)),
      JoinPlanPredicateLogicalOperator::And,
      std::make_shared<JoinPlanAtomicPredicate>(_column_b, PredicateCondition::LessThan, 8));
    // clang-format on
  }

  static std::string to_string(const AbstractJoinPlanPredicate& predicate) {
    std::stringstream stream;
    predicate.print(stream);
    return stream.str();
  }

  std::shared_ptr<MockNode> _mock_node;
  LQPColumnReference _column_a;
  LQPColumnReference _column_b;

  std::shared_ptr<AbstractJoinPlanPredicate> _predicate_a;
  std::shared_ptr<AbstractJoinPlanPredicate> _predicate_b;
  std::shared_ptr<AbstractJoinPlanPredicate> _predicate_c;
};

TEST_F(JoinPlanPredicateTest, Print) {
  EXPECT_EQ(to_string(*_predicate_a), "a = 5");
  EXPECT_EQ(to_string(*_predicate_b), "a <= b");
  EXPECT_EQ(to_string(*_predicate_c), "(a > b OR a > 5) AND b < 8");
}

}  // namespace opossum
