#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "optimizer/strategy/exists_reformulation_rule.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExistsReformulationRuleTest : public StrategyBaseTest {
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

    _rule = std::make_shared<ExistsReformulationRule>();
  }

  std::shared_ptr<ExistsReformulationRule> _rule;

  std::shared_ptr<StoredTableNode> node_table_a, node_table_b;
  LQPColumnReference node_table_a_col_a, node_table_a_col_b, node_table_b_col_a, node_table_b_col_b;
};

TEST_F(ExistsReformulationRuleTest, SimpleExistsToSemiJoin) {
  const auto parameter = opossum::expression_functional::parameter_(ParameterID{0}, node_table_a_col_a);

  // clang-format off
  const auto subselect_lqp =
  PredicateNode::make(equals_(node_table_b_col_a, parameter),
    node_table_b);

  const auto subselect = select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp =
  PredicateNode::make(exists_(subselect),
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
  const auto parameter = opossum::expression_functional::parameter_(ParameterID{0}, node_table_a_col_a);

  // clang-format off
  const auto subselect_lqp =
  PredicateNode::make(equals_(node_table_b_col_a, parameter),
    node_table_b);

  const auto subselect = select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp =
  PredicateNode::make(not_exists_(subselect),
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
  const auto parameter = opossum::expression_functional::parameter_(ParameterID{0}, node_table_a_col_a);

  /**
   * Test that there can be...
   *    - SortNodes in the subselect
   *    - JoinNodes/UnionNodes in the subselect if they are below the  PredicateNode that the rule extracts
   *      from.
   *    - PredicateNodes in the subselect
   */
  // clang-format off
  const auto subselect_lqp =
  SortNode::make(expression_vector(node_table_b_col_b), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(greater_than_equals_(node_table_b_col_a, 5),
      PredicateNode::make(equals_(node_table_b_col_a, parameter),
        JoinNode::make(JoinMode::Cross,
          UnionNode::make(UnionMode::Positions,
            node_table_b,
            node_table_b),
          node_table_a))));

  const auto subselect = select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp =
  PredicateNode::make(not_exists_(subselect),
    node_table_a);

  const auto expected_subselect_lqp =
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
    expected_subselect_lqp);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Apply the rule to various queries which should NOT be modified by the reformulation rule.
TEST_F(ExistsReformulationRuleTest, QueryNotRewritten) {
  std::vector<std::string> non_rewritable_tests;

  // As of now, we reformulate to hash joins. Hash joins only support equality join predicates.
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b WHERE table_a.a < table_b.a)");

  // Subquery is not correlated with outer query.
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b WHERE table_b.a < table_b.b)");

  // Multiple correlated predicates in subquery not supported.
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a and table_a.a = "
      "table_b.b)");
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a and table_a.a < 17)");

  // Multiple parameters to subquery (table_a.a, table_a.b).
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a and table_a.b = "
      "table_b.b)");

  for (const auto& query : non_rewritable_tests) {
    auto sql_pipeline = SQLPipelineBuilder{query}.disable_mvcc().create_pipeline_statement();
    auto input_lqp = sql_pipeline.get_unoptimized_logical_plan();

    const auto modified_lqp = apply_rule(_rule, input_lqp);

    // For all the exemplary queries, we expect an unmodified LQP.
    EXPECT_LQP_EQ(input_lqp, modified_lqp);
  }
}

}  // namespace opossum
