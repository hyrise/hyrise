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

std::string DataInducedPredicateRule::name() const {
  static const auto name = std::string{"DataInducedParameterRule"};
  return name;
}

void DataInducedPredicateRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  Assert(lqp_root->type == LQPNodeType::Root, "Rule needs root to hold onto");

  // Adding semi joins inside visit_lqp might lead to endless recursions. Thus, we use visit_lqp to identify the
  // reductions that we want to add to the plan, write them into semi_join_reductions and actually add them after
  // visit_lqp.
  std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<JoinNode>>> semi_join_reductions;

  const auto opposite_side = [](const auto side) {
    return side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left;
  };

  const auto estimator = cost_estimator->cardinality_estimator->new_instance();
  estimator->guarantee_bottom_up_construction();

  visit_lqp(lqp_root, [&lqp_root, &estimator, &opposite_side, this, &semi_join_reductions](const auto& node) {
    if (node->type != LQPNodeType::Join) {
      return LQPVisitation::VisitInputs;
    }
    std::cout << *lqp_root << std::endl;
    const auto join_node = std::static_pointer_cast<JoinNode>(node);

    DebugAssert(join_node->join_predicates().size() == 1, "We currently only support data induced predicates with only one join predicate");

    const auto predicate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_node->join_predicates()[0]);
    DebugAssert(predicate_expression, "Expected BinaryPredicateExpression");
    DebugAssert(predicate_expression->predicate_condition == PredicateCondition::Equals, "PredicateCondition must be equals");

    // Currently we always use the right side as reducer and left is reduced
    const auto selection_side = LQPInputSide::Left;

    auto reduced_node = join_node->input(selection_side);
    auto reducer_node = join_node->input(opposite_side(selection_side));

    const auto& reducer_side_expression =
        expression_evaluable_on_lqp(predicate_expression->left_operand(), *reducer_node)
            ? predicate_expression->left_operand()
            : predicate_expression->right_operand();
    DebugAssert(!expression_evaluable_on_lqp(reducer_side_expression, *join_node->input(selection_side)),
                "Expected filtered expression to be uniquely evaluable on one side of the join");

    const auto subquery = AggregateNode::make(expression_vector(), expression_vector(min_(reducer_side_expression), max_(reducer_side_expression)), reducer_node);

    const auto min_c_y = ProjectionNode::make(expression_vector(min_(reducer_side_expression)), subquery);
    const auto max_c_y = ProjectionNode::make(expression_vector(max_(reducer_side_expression)), subquery);

    const auto reduce_if_beneficial = [&](const auto side_of_join) {
      auto reduced_node = join_node->input(side_of_join);
      auto original_cardinality = estimator->estimate_cardinality(reduced_node);

      auto reducer_node = join_node->input(opposite_side(side_of_join));
      auto reducer_node_cardinality = estimator->estimate_cardinality(reducer_node);



      const auto semi_join_reduction_node = JoinNode::make(JoinMode::Semi, predicate_expression);
      semi_join_reduction_node->mark_as_semi_reduction(join_node);
      semi_join_reduction_node->comment = name();
      lqp_insert_node(join_node, side_of_join, semi_join_reduction_node);
      semi_join_reduction_node->set_right_input(reducer_node);

      const auto reduced_cardinality = estimator->estimate_cardinality(semi_join_reduction_node);

      semi_join_reduction_node->set_right_input(nullptr);
      lqp_remove_node(semi_join_reduction_node);

      // TODO (team): fix me for dips currently we reduce every time
      // While semi join reductions might not be immediately beneficial if the original cardinality is low, remember
      // that they might be pushed down in the LQP to a point where they are more beneficial (e.g., below an
      // Aggregate).
      /* if (original_cardinality == 0 || (reduced_cardinality / original_cardinality) > MINIMUM_SELECTIVITY) {
        return;
      } */

      // For `t1 JOIN t2 on t1.a = t2.a` where a semi join reduction is supposed to be added to t1, `t1.a = t2.a`
      // is the predicate_expression. The values from t1 are supposed to be filtered by looking at t1.a, which is
      // called the reducer_side_expression.
      const auto& reducer_side_expression =
          expression_evaluable_on_lqp(predicate_expression->left_operand(), *reducer_node)
              ? predicate_expression->left_operand()
              : predicate_expression->right_operand();
      DebugAssert(!expression_evaluable_on_lqp(reducer_side_expression, *join_node->input(side_of_join)),
                  "Expected filtered expression to be uniquely evaluable on one side of the join");

      // Currently, the right input of the semi join reduction (i.e., the reducer node) is the opposite input of the
      // join_node. By walking down that LQP, we might find a suitable reducer node where predicate_expression
      // continues to be evaluable but where the cardinality is lower (mostly, because we moved below a join). Also,
      // by moving further down in the LQP, we (1) allow the semi join reduction to start earlier in multi-threaded
      // environments and (2) have a better chance at finding a common reducer node for multiple semi joins,
      // improving the reusability of sub-LQPs.
      while (true) {
        // Independent of the join type of the reducer_node, we can use either side of the join's input as a candidate
        // for the reducer, as long as the predicate continues to be evaluable. This is because the join in
        // reducer_node might only remove values from the reducer's list of values, but does not add any new values.
        // As such, each value in the output of the join will also be found in the input.
        if (reducer_node->type != LQPNodeType::Join) {
          break;
        }

        const auto try_deeper_reducer_node = [&](const auto side_of_input) {
          const auto& candidate = reducer_node->input(side_of_input);
          if (!expression_evaluable_on_lqp(reducer_side_expression, *candidate)) {
            return false;
          }

          const auto candidate_cardinality = estimator->estimate_cardinality(candidate);

          // Check if the candidate has an equal or lower cardinality. Allow for some tolerance due to float values
          // being used.
          if (candidate_cardinality > reducer_node_cardinality * 1.01) {
            return false;
          }

          reducer_node = candidate;
          reducer_node_cardinality = candidate_cardinality;

          return true;
        };

        if (try_deeper_reducer_node(LQPInputSide::Left) || try_deeper_reducer_node(LQPInputSide::Right)) {
          continue;
        }
        break;
      }

      if (reducer_node_cardinality > original_cardinality) {
        // The JoinHash, which is currently used for semi joins is bad at handling cases where the build side is
        // larger than the probe side. In these cases, do not add a semi join reduction for now.
        return;
      }

      semi_join_reduction_node->set_right_input(reducer_node);
      semi_join_reductions.emplace_back(join_node, side_of_join, semi_join_reduction_node);
    };





    // Having defined the lambda responsible for conditionally adding a semi join reduction, we now apply it to both
    // inputs of the join. For outer joins, we must not filter the side on which tuples survive even without a join
    // partner.
    if (join_node->join_mode != JoinMode::Right && join_node->join_mode != JoinMode::FullOuter) {
      reduce_if_beneficial(LQPInputSide::Right);
    }

    // On the left side we must not create semi join reductions for anti joins as those rely on the very existence of
    // non-matching values on the right side. Also, we should not create semi join reductions for semi joins as those
    // would simply duplicate the original join.
    if (join_node->join_mode != JoinMode::Left && join_node->join_mode != JoinMode::FullOuter &&
        join_node->join_mode != JoinMode::AntiNullAsTrue && join_node->join_mode != JoinMode::AntiNullAsFalse &&
        join_node->join_mode != JoinMode::Semi) {
      reduce_if_beneficial(LQPInputSide::Left);
    }

    return LQPVisitation::VisitInputs;
  });

  for (const auto& [join_node, side_of_join, semi_join_reduction_node] : semi_join_reductions) {
    lqp_insert_node(join_node, side_of_join, semi_join_reduction_node, AllowRightInput::Yes);
  }
}
}  // namespace hyrise
