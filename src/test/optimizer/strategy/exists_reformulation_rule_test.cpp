#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "operators/print.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/parameter_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/exists_reformulation_rule.hpp"
#include "sql/sql_pipeline.hpp"
#include "sql/sql_pipeline_builder.hpp"
#include "storage/storage_manager.hpp"
#include "tpch/tpch_db_generator.hpp"
#include "tpch/tpch_queries.hpp"
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

// Take an LQP and search for an exists predicate
bool lqp_contains_exists_predicate(const std::shared_ptr<AbstractLQPNode>& lqp, const bool is_non_exists) {
  bool found_node = false;
  visit_lqp(lqp, [&](const auto& deeper_node) {
    if (deeper_node->type == LQPNodeType::Predicate) {
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(deeper_node);
      const auto predicate_expression =
          std::static_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate);
      if (predicate_expression->arguments[0]->type == ExpressionType::Exists) {
        if ((predicate_expression->predicate_condition != PredicateCondition::Equals && !is_non_exists) ||
            (predicate_expression->predicate_condition == PredicateCondition::Equals && is_non_exists)) {
          found_node = true;
          return LQPVisitation::DoNotVisitInputs;
        }
      }
    }
    return LQPVisitation::VisitInputs;
  });
  return found_node;
}

// Take an LQP and search for an exists projection
bool lqp_contains_exists_projection(const std::shared_ptr<AbstractLQPNode>& lqp, const bool is_non_exists) {
  bool found_node = false;
  visit_lqp(lqp, [&](const auto& deeper_node) {
    if (deeper_node->type == LQPNodeType::Projection) {
      const auto projection_node = std::static_pointer_cast<ProjectionNode>(deeper_node);
      for (const auto expression : projection_node->column_expressions()) {
        if (expression->type == ExpressionType::Exists) {
          found_node = true;
          return LQPVisitation::DoNotVisitInputs;
        }
      }
    }
    return LQPVisitation::VisitInputs;
  });
  return found_node;
}

// Take an LQP and search for a join with the specified join mode
bool lqp_contains_join_with_mode(const std::shared_ptr<AbstractLQPNode>& lqp, const JoinMode join_mode) {
  bool found_join = false;
  visit_lqp(lqp, [&](const auto& deeper_node) {
    if (deeper_node->type == LQPNodeType::Join) {
      const auto join_node = std::static_pointer_cast<JoinNode>(deeper_node);
      if (join_node->join_mode == join_mode) {
        found_join = true;
        return LQPVisitation::DoNotVisitInputs;
      }
    }
    return LQPVisitation::VisitInputs;
  });
  return found_join;
}

// Test for a very basic query and check if the query no longer contains
// exists expressions after reformulation but instead the expected join node.
TEST_F(ExistsReformulationRuleTest, QueryWithExists) {
  auto query = "SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_b.a = table_a.a)";
  auto sql_pipeline = SQLPipelineBuilder{query}.disable_mvcc().create_pipeline_statement();

  const auto& unopt_lqp = sql_pipeline.get_unoptimized_logical_plan();
  EXPECT_TRUE(lqp_contains_exists_predicate(unopt_lqp, false));
  EXPECT_FALSE(lqp_contains_join_with_mode(unopt_lqp, JoinMode::Semi));

  auto modified_lqp = unopt_lqp->deep_copy();
  apply_rule(_rule, modified_lqp);
  EXPECT_TRUE(lqp_contains_join_with_mode(modified_lqp, JoinMode::Semi));
  EXPECT_FALSE(lqp_contains_exists_predicate(modified_lqp, false));
  EXPECT_FALSE(lqp_contains_exists_projection(modified_lqp, false));
}

TEST_F(ExistsReformulationRuleTest, QueryWithNotExists) {
  auto query = "SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a)";
  auto sql_pipeline = SQLPipelineBuilder{query}.disable_mvcc().create_pipeline_statement();

  const auto& unopt_lqp = sql_pipeline.get_unoptimized_logical_plan();
  EXPECT_TRUE(lqp_contains_exists_predicate(unopt_lqp, true));
  EXPECT_FALSE(lqp_contains_join_with_mode(unopt_lqp, JoinMode::Anti));

  auto modified_lqp = unopt_lqp->deep_copy();
  apply_rule(_rule, modified_lqp);
  EXPECT_TRUE(lqp_contains_join_with_mode(modified_lqp, JoinMode::Anti));
  EXPECT_FALSE(lqp_contains_exists_predicate(modified_lqp, true));
}

// Manually construct an exists query and apply the reformulation rule.
// Compare with the manually created "optimal" plan and check for equality.
TEST_F(ExistsReformulationRuleTest, ManualSemijoinLQPComparison) {
  const auto parameter = opossum::expression_functional::parameter_(ParameterID{0}, node_table_a_col_a);

  const auto subselect_lqp = PredicateNode::make(equals_(node_table_b_col_a, parameter), node_table_b);
  const auto subselect = select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          not_equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  // apply exists reformulation rule
  auto modified_lqp = input_lqp->deep_copy();
  StrategyBaseTest::apply_rule(_rule, modified_lqp);

  const auto compare_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      JoinNode::make(JoinMode::Semi, equals_(node_table_a_col_a, node_table_b_col_a),
                     ProjectionNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b), node_table_a),
                     node_table_b));

  EXPECT_LQP_EQ(modified_lqp, compare_lqp);
}

TEST_F(ExistsReformulationRuleTest, ManualAntijoinLQPComparison) {
  const auto parameter = opossum::expression_functional::parameter_(ParameterID{0}, node_table_a_col_a);

  const auto subselect_lqp = PredicateNode::make(equals_(node_table_b_col_a, parameter), node_table_b);
  const auto subselect = select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  // apply exists reformulation rule
  auto modified_lqp = input_lqp->deep_copy();
  StrategyBaseTest::apply_rule(_rule, modified_lqp);

  const auto compare_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      JoinNode::make(JoinMode::Anti, equals_(node_table_a_col_a, node_table_b_col_a),
                     ProjectionNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b), node_table_a),
                     node_table_b));

  EXPECT_LQP_EQ(modified_lqp, compare_lqp);
}

// Apply the rule to various queries which should NOT be modified by the reformulation rule.
TEST_F(ExistsReformulationRuleTest, QueryNotRewritten) {
  std::vector<std::string> non_rewritable_tests;

  // As of now, we reformulate to hash joins. Hash joins only support equality join predicates.
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b WHERE table_a.a < table_b.a)");

  // Subquery is not correlated with out query.
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b WHERE table_b.a < table_b.b)");

  // Multiple predicates in correlated subquery not supported.
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a and table_a.a = "
      "table_b.b)");

  // Multiple external predicates.
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a and table_a.b = "
      "table_b.b)");

  // Same external predicate more than once.
  non_rewritable_tests.push_back(
      "SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a and table_a.a < 17)");

  for (const auto& query : non_rewritable_tests) {
    auto sql_pipeline = SQLPipelineBuilder{query}.disable_mvcc().create_pipeline_statement();
    auto input_lqp = sql_pipeline.get_unoptimized_logical_plan();

    auto modified_lqp = input_lqp->deep_copy();
    apply_rule(_rule, modified_lqp);

    // For all the exemplary queries, we expect an unmodified LQP.
    EXPECT_LQP_EQ(input_lqp, modified_lqp);
  }
}

}  // namespace opossum
