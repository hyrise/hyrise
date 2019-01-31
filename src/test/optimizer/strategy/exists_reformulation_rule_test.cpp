#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/exists_reformulation_rule.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExistsReformulationRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("resources/test_data/tbl/int_int2.tbl"));
    StorageManager::get().add_table("table_b", load_table("resources/test_data/tbl/int_int3.tbl"));

    node_table_a = StoredTableNode::make("table_a");
    node_table_a_col_a = node_table_a->get_column("a");
    node_table_a_col_b = node_table_a->get_column("b");

    node_table_b = StoredTableNode::make("table_b");
    node_table_b_col_a = node_table_b->get_column("a");
    node_table_b_col_b = node_table_b->get_column("b");

    _rule = std::make_shared<ExistsReformulationRule>();
  }

  std::shared_ptr<AbstractLQPNode> apply_exists_rule(const std::shared_ptr<AbstractLQPNode>& lqp) {
    auto copied_lqp = lqp->deep_copy();
    StrategyBaseTest::apply_rule(_rule, copied_lqp);

    return copied_lqp;
  }

  std::shared_ptr<ExistsReformulationRule> _rule;

  std::shared_ptr<StoredTableNode> node_table_a, node_table_b;
  LQPColumnReference node_table_a_col_a, node_table_a_col_b, node_table_b_col_a, node_table_b_col_b;
};

TEST_F(ExistsReformulationRuleTest, SimpleExistsToSemiJoin) {
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);

  // clang-format off
  const auto subquery_lqp =
  PredicateNode::make(equals_(node_table_b_col_a, parameter),
    node_table_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp =
  PredicateNode::make(exists_(subquery),
    node_table_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Semi, equals_(node_table_a_col_a, node_table_b_col_a),
    node_table_a,
    node_table_b);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ExistsReformulationRuleTest, SimpleNotExistsToAntiJoin) {
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);

  // clang-format off
  const auto subquery_lqp =
  PredicateNode::make(equals_(node_table_b_col_a, parameter),
    node_table_b);

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp =
  PredicateNode::make(not_exists_(subquery),
    node_table_a);

  const auto expected_lqp =
  JoinNode::make(JoinMode::Anti, equals_(node_table_a_col_a, node_table_b_col_a),
    node_table_a,
    node_table_b);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ExistsReformulationRuleTest, ComplexSubquery) {
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);

  /**
   * Test that there can be...
   *    - SortNodes in the subquery
   *    - JoinNodes/UnionNodes in the subquery if they are below the PredicateNode that the rule extracts
   *      from.
   *    - PredicateNodes in the subquery
   */
  // clang-format off
  const auto subquery_lqp =
  SortNode::make(expression_vector(node_table_b_col_b), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(greater_than_equals_(node_table_b_col_a, 5),
      PredicateNode::make(equals_(node_table_b_col_a, parameter),
        JoinNode::make(JoinMode::Cross,
          UnionNode::make(UnionMode::Positions,
            node_table_b,
            node_table_b),
          node_table_a))));

  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp =
  PredicateNode::make(not_exists_(subquery),
    node_table_a);

  const auto expected_subquery_lqp =
  SortNode::make(expression_vector(node_table_b_col_b), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(greater_than_equals_(node_table_b_col_a, 5),
      JoinNode::make(JoinMode::Cross,
        UnionNode::make(UnionMode::Positions,
          node_table_b,
          node_table_b),
        node_table_a)));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Anti, equals_(node_table_a_col_a, node_table_b_col_a),
    node_table_a,
    expected_subquery_lqp);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// independent of our current limitations of the rewriting of exists, having a further OR should
// not be reformulated according to "The Complete Story of Joins (in HyPer)" (BTW 2017, pp. 31-50)
TEST_F(ExistsReformulationRuleTest, NoRewriteOfExistsWithOrPredicate) {
  // SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a) OR a < 17
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subquery_lqp = PredicateNode::make(equals_(parameter, node_table_b_col_a), node_table_b);
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          not_equals_(or_(exists_(subquery), less_than_(node_table_a_col_a, 17)), 0),
          ProjectionNode::make(expression_vector(or_(exists_(subquery), less_than_(node_table_a_col_a, 17)),
                                                 node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), input_lqp);
}

/*
  The following cases test whether queries we currently do not rewrite are really not modified by the rule.
  Note, rewriting these cases to joins might be possible but as of now we do not rewrite them.
*/
TEST_F(ExistsReformulationRuleTest, NoRewriteOfInequalityJoinPredicates) {
  // SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b WHERE table_a.a < table_b.a)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subquery_lqp = PredicateNode::make(less_than_(parameter, node_table_b_col_a), node_table_b);
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subquery), 0),
          ProjectionNode::make(expression_vector(exists_(subquery), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), input_lqp);
}

TEST_F(ExistsReformulationRuleTest, NoRewriteOfMultipleJoinPredicates) {
  // SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b
  //    WHERE table_a.a = table_b.a and table_a.a = table_b.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subquery_lqp = PredicateNode::make(equals_(parameter, node_table_b_col_b), node_table_b);
  const auto subquery_lqp2 = PredicateNode::make(equals_(parameter, node_table_b_col_a), subquery_lqp);
  const auto subquery = lqp_subquery_(subquery_lqp2, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subquery), 0),
          ProjectionNode::make(expression_vector(exists_(subquery), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), input_lqp);
}

TEST_F(ExistsReformulationRuleTest, NoRewriteOfExternalJoinPredicatesMoreThanOnce) {
  // SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b
  //    WHERE table_a.a = table_b.a and table_a.a < 17)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subquery_lqp = PredicateNode::make(equals_(parameter, node_table_b_col_a), node_table_b);
  const auto subquery_lqp2 = PredicateNode::make(less_than_(parameter, 17), subquery_lqp);
  const auto subquery = lqp_subquery_(subquery_lqp2, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subquery), 0),
          ProjectionNode::make(expression_vector(exists_(subquery), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), input_lqp);
}

}  // namespace opossum
