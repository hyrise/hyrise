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

  std::shared_ptr<AbstractLQPNode> apply_exists_rule(const std::shared_ptr<AbstractLQPNode>& lqp) {
    auto copied_lqp = lqp->deep_copy();
    StrategyBaseTest::apply_rule(_rule, copied_lqp);

    return copied_lqp;
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
  // SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_b.a = table_a.a)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subselect_lqp = PredicateNode::make(equals_(node_table_b_col_a, parameter), node_table_b);
  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          not_equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_FALSE(lqp_contains_join_with_mode(input_lqp, JoinMode::Semi));
  EXPECT_TRUE(lqp_contains_exists_predicate(input_lqp, false));
  EXPECT_TRUE(lqp_contains_exists_projection(input_lqp, false));

  const auto modified_lqp = this->apply_exists_rule(input_lqp);
  EXPECT_TRUE(lqp_contains_join_with_mode(modified_lqp, JoinMode::Semi));
  EXPECT_FALSE(lqp_contains_exists_predicate(modified_lqp, false));
  EXPECT_FALSE(lqp_contains_exists_projection(modified_lqp, false));
}

TEST_F(ExistsReformulationRuleTest, QueryWithNotExists) {
  // SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subselect_lqp = PredicateNode::make(equals_(node_table_b_col_a, parameter), node_table_b);
  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_FALSE(lqp_contains_join_with_mode(input_lqp, JoinMode::Anti));
  EXPECT_TRUE(lqp_contains_exists_predicate(input_lqp, true));

  const auto modified_lqp = this->apply_exists_rule(input_lqp);
  EXPECT_TRUE(lqp_contains_join_with_mode(modified_lqp, JoinMode::Anti));
  EXPECT_FALSE(lqp_contains_exists_predicate(modified_lqp, true));
  EXPECT_FALSE(lqp_contains_exists_projection(modified_lqp, true));
}

// Manually construct an exists query and apply the reformulation rule.
// Compare with the manually created "optimal" plan and check for equality.
TEST_F(ExistsReformulationRuleTest, ManualSemijoinLQPComparison) {
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);

  const auto subselect_lqp = PredicateNode::make(equals_(node_table_b_col_a, parameter), node_table_b);
  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          not_equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  const auto compare_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      JoinNode::make(JoinMode::Semi, equals_(node_table_a_col_a, node_table_b_col_a),
                     ProjectionNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b), node_table_a),
                     node_table_b));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), compare_lqp);
}

TEST_F(ExistsReformulationRuleTest, ManualAntijoinLQPComparison) {
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);

  const auto subselect_lqp = PredicateNode::make(equals_(node_table_b_col_a, parameter), node_table_b);
  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));

  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  const auto compare_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      JoinNode::make(JoinMode::Anti, equals_(node_table_a_col_a, node_table_b_col_a),
                     ProjectionNode::make(expression_vector(node_table_a_col_a, node_table_a_col_b), node_table_a),
                     node_table_b));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), compare_lqp);
}

// independent of our current limitations of the rewriting of exists, having a further OR should
// not be reformulated according to "The Complete Story of Joins (in HyPer)" (BTW 2017, pp. 31-50)
TEST_F(ExistsReformulationRuleTest, NoRewriteOfExistsWithOrPredicate) {
  // SELECT * FROM table_a WHERE EXISTS (SELECT * FROM table_b WHERE table_a.a = table_b.a) OR a < 17
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subselect_lqp = PredicateNode::make(equals_(parameter, node_table_b_col_a), node_table_b);
  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          not_equals_(or_(exists_(subselect), less_than_(node_table_a_col_a, 17)), 0),
          ProjectionNode::make(expression_vector(or_(exists_(subselect), less_than_(node_table_a_col_a, 17)),
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
  const auto subselect_lqp = PredicateNode::make(less_than_(parameter, node_table_b_col_a), node_table_b);
  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), input_lqp);
}

TEST_F(ExistsReformulationRuleTest, NoRewriteOfMultipleJoinPredicates) {
  // SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b
  //    WHERE table_a.a = table_b.a and table_a.a = table_b.b)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subselect_lqp = PredicateNode::make(equals_(parameter, node_table_b_col_b), node_table_b);
  const auto subselect_lqp2 = PredicateNode::make(equals_(parameter, node_table_b_col_a), subselect_lqp);
  const auto subselect = lqp_select_(subselect_lqp2, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), input_lqp);
}

TEST_F(ExistsReformulationRuleTest, NoRewriteOfExternalJoinPredicatesMoreThanOnce) {
  // SELECT * FROM table_a WHERE NOT EXISTS (SELECT * FROM table_b
  //    WHERE table_a.a = table_b.a and table_a.a < 17)
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);
  const auto subselect_lqp = PredicateNode::make(equals_(parameter, node_table_b_col_a), node_table_b);
  const auto subselect_lqp2 = PredicateNode::make(less_than_(parameter, 17), subselect_lqp);
  const auto subselect = lqp_select_(subselect_lqp2, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = ProjectionNode::make(
      expression_vector(node_table_a_col_a, node_table_a_col_b),
      PredicateNode::make(
          equals_(exists_(subselect), 0),
          ProjectionNode::make(expression_vector(exists_(subselect), node_table_a_col_a, node_table_a_col_b),
                               node_table_a)));

  EXPECT_LQP_EQ(this->apply_exists_rule(input_lqp), input_lqp);
}

}  // namespace opossum
