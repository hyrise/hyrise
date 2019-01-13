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
#include "optimizer/strategy/in_reformulation_rule.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class InReformulationRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("src/test/tables/int_int2.tbl"));
    StorageManager::get().add_table("table_b", load_table("src/test/tables/int_int3.tbl"));

    node_table_a = StoredTableNode::make("table_a");
    node_table_a_col_a = node_table_a->get_column("a");
    node_table_a_col_b = node_table_a->get_column("b");

    node_table_b = StoredTableNode::make("table_b");
    node_table_b_col_a = node_table_b->get_column("a");
    node_table_b_col_b = node_table_b->get_column("b");

    _rule = std::make_shared<InReformulationRule>();
  }

  std::shared_ptr<AbstractLQPNode> apply_in_rule(const std::shared_ptr<AbstractLQPNode>& lqp) {
    auto copied_lqp = lqp->deep_copy();
    StrategyBaseTest::apply_rule(_rule, copied_lqp);

    return copied_lqp;
  }

  std::shared_ptr<InReformulationRule> _rule;

  std::shared_ptr<StoredTableNode> node_table_a, node_table_b;
  LQPColumnReference node_table_a_col_a, node_table_a_col_b, node_table_b_col_a, node_table_b_col_b;
};

TEST_F(InReformulationRuleTest, SimpleInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b)
  // clang-format off
  const auto subselect_lqp =
      ProjectionNode::make(expression_vector(node_table_b_col_a), node_table_b);

  const auto subselect = lqp_select_(subselect_lqp);

  const auto input_lqp =
      PredicateNode::make(in_(node_table_a_col_a, subselect),
          node_table_a);

  const auto expected_lqp =
      JoinNode::make(JoinMode::Semi, equals_(node_table_a_col_a, node_table_b_col_a),
                     node_table_a,
                     ProjectionNode::make(expression_vector(node_table_b_col_a), node_table_b));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(InReformulationRuleTest, CorrelatedInToSemiJoin) {
  // SELECT * FROM a WHERE a.a IN (SELECT b.a FROM b WHERE b.a = a.a)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);

  // clang-format off
  const auto subselect_lqp =
      ProjectionNode::make(expression_vector(node_table_b_col_a),
          PredicateNode::make(equals_(node_table_b_col_a, parameter),
              node_table_b));

  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp =
      PredicateNode::make(in_(node_table_a_col_a, subselect),
                          node_table_a);

  const auto expected_lqp =
      PredicateNode::make(equals_(node_table_a_col_a, node_table_b_col_a),
                          JoinNode::make(JoinMode::Semi, equals_(node_table_a_col_a, node_table_b_col_a),
                                         node_table_a,
                                         ProjectionNode::make(expression_vector(node_table_b_col_a), node_table_b)));
  // clang-format on
  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  actual_lqp->print();
  std::cout << std::endl;
  expected_lqp->print();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
