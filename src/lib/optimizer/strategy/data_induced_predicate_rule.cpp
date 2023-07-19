#include "data_induced_predicate_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/enable_make_for_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"

namespace hyrise {
using namespace expression_functional;

std::string DataInducedPredicateRule::name() const {
  static const auto name = std::string{"DataInducedParameterRule"};
  return name;
}

void DataInducedPredicateRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  Assert(lqp_root->type == LQPNodeType::Root, "Rule needs root to hold onto");

  // Adding dips inside visit_lqp might lead to endless recursions. Thus, we use visit_lqp to identify the
  // reductions that we want to add to the plan, write them into data_induced_predicates and actually add them after
  // visit_lqp.
  std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<PredicateNode>>>
      data_induced_predicates;

  const auto opposite_side = [](const auto side) {
    return side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left;
  };

  const auto estimator = cost_estimator->cardinality_estimator->new_instance();
  estimator->guarantee_bottom_up_construction();

  visit_lqp(lqp_root, [&estimator, &opposite_side, &data_induced_predicates](const auto& node) {
    if (node->type != LQPNodeType::Join) {
      return LQPVisitation::VisitInputs;
    }
    // std::cout << *lqp_root << std::endl;
    const auto join_node = std::static_pointer_cast<JoinNode>(node);

    for (const auto& join_predicate : join_node->join_predicates()) {
      const auto predicate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
      DebugAssert(predicate_expression, "Expected BinaryPredicateExpression");
      if (predicate_expression->predicate_condition != PredicateCondition::Equals) {
        continue;
      }

      const auto reduce_if_beneficial = [&](const auto selection_side) {
        auto reduced_node = join_node->input(selection_side);
        auto reducer_node = join_node->input(opposite_side(selection_side));

        auto reducer_side_expression = predicate_expression->left_operand();
        auto reduced_side_expression = predicate_expression->right_operand();

        auto original_cardinality = estimator->estimate_cardinality(reduced_node);
        // auto reducer_node_cardinality = estimator->estimate_cardinality(reducer_node);

        if (!expression_evaluable_on_lqp(reducer_side_expression, *reducer_node)) {
          std::swap(reduced_side_expression, reducer_side_expression);
        }
        DebugAssert(!expression_evaluable_on_lqp(reducer_side_expression, *join_node->input(selection_side)),
                    "Expected filtered expression to be uniquely evaluable on one side of the join");

        const auto subquery = AggregateNode::make(
            expression_vector(), expression_vector(min_(reducer_side_expression), max_(reducer_side_expression)),
            reducer_node);

        const auto min = ProjectionNode::make(expression_vector(min_(reducer_side_expression)), subquery);
        const auto max = ProjectionNode::make(expression_vector(max_(reducer_side_expression)), subquery);

        auto between_predicate =
            PredicateNode::make(between_inclusive_(reduced_side_expression, lqp_subquery_(min), lqp_subquery_(max)));
        lqp_insert_node(join_node, selection_side, between_predicate);

        // estimate cardinality and decide wether its usefull to do this
        const auto reduced_cardinality = estimator->estimate_cardinality(between_predicate);
        lqp_remove_node(between_predicate);

        if (original_cardinality == 0 || (reduced_cardinality / original_cardinality) > MINIMUM_SELECTIVITY) {
          return;
        }
        data_induced_predicates.emplace_back(join_node, selection_side, between_predicate);
      };

      // Having defined the lambda responsible for conditionally adding a data induced predicate, we now apply it to both
      // inputs of the join. For outer joins, we must not filter the side on which tuples survive even without a join
      // partner.
      if (join_node->join_mode != JoinMode::Right && join_node->join_mode != JoinMode::FullOuter) {
        reduce_if_beneficial(LQPInputSide::Right);
      }

      // On the left side we must not create data induced predicates for anti joins as those rely on the very existence of
      // non-matching values on the right side.
      if (join_node->join_mode != JoinMode::Left && join_node->join_mode != JoinMode::FullOuter &&
          join_node->join_mode != JoinMode::AntiNullAsTrue && join_node->join_mode != JoinMode::AntiNullAsFalse) {
        reduce_if_beneficial(LQPInputSide::Left);
      }
    }
    return LQPVisitation::VisitInputs;
  });

  for (const auto& [join_node, side_of_join, data_induced_predicate_node] : data_induced_predicates) {
    lqp_insert_node(join_node, side_of_join, data_induced_predicate_node, AllowRightInput::Yes);
  }
}
}  // namespace hyrise
