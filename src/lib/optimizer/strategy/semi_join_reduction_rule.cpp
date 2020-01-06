#include "semi_join_reduction_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"

namespace opossum {
void SemiJoinReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  Assert(root->type == LQPNodeType::Root, "ExpressionReductionRule needs root to hold onto");

  // Adding semi joins inside visit_lqp might lead to endless recursions. Thus, we use visit_lqp to identify the
  // reductions that we want to add to the plan, write them into semi_join_reductions and actually add them after
  // visit_lqp.
  std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<JoinNode>>> semi_join_reductions;

  const auto opposite_side = [](const auto side) {
    return side == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left;
  };

  const auto estimator = cost_estimator->cardinality_estimator->new_instance();
  estimator->guarantee_bottom_up_construction();

  visit_lqp(root, [&](const auto& node) {
    if (node->type != LQPNodeType::Join) return LQPVisitation::VisitInputs;
    const auto join_node = std::static_pointer_cast<JoinNode>(node);

    // As multi-predicate joins are expensive, we do not want to create semi join reductions that use them. Instead, we
    // look at each predicate of the join independently. We can do this as a JoinNode's predicates are conjunctive.
    // Disjunctive predicates are currently not supported and if they were, they would be stored in a single
    // join_predicates entry.
    for (const auto& join_predicate : join_node->join_predicates()) {
      const auto predicate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
      DebugAssert(predicate_expression, "Expected BinaryPredicateExpression");
      if (predicate_expression->predicate_condition != PredicateCondition::Equals) {
        continue;
      }

      // Since semi join reductions might be beneficial for both sides of the join, we create this helper lambda,
      // which can deal with both sides.
      const auto reduce_if_beneficial = [&](const auto side_of_join) {
        auto reduced_node = join_node->input(side_of_join);
        auto original_cardinality = estimator->estimate_cardinality(reduced_node);

        auto reducer_node = join_node->input(opposite_side(side_of_join));
        auto reducer_node_cardinality = estimator->estimate_cardinality(reducer_node);

        const auto semi_join_reduction_node = JoinNode::make(JoinMode::Semi, predicate_expression);
        semi_join_reduction_node->comment = "Semi Reduction";
        lqp_insert_node(join_node, side_of_join, semi_join_reduction_node);
        semi_join_reduction_node->set_right_input(reducer_node);

        const auto reduced_cardinality = estimator->estimate_cardinality(semi_join_reduction_node);

        semi_join_reduction_node->set_right_input(nullptr);
        lqp_remove_node(semi_join_reduction_node);

        // While semi join reductions might not be immediately beneficial if the original cardinality is low, remember
        // that they might be pushed down in the LQP to a point where they are more beneficial (e.g., below an
        // Aggregate).
        if (original_cardinality == 0 || (reduced_cardinality / original_cardinality) > MINIMUM_SELECTIVITY) {
          return;
        }

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

          if (try_deeper_reducer_node(LQPInputSide::Left)) continue;
          if (try_deeper_reducer_node(LQPInputSide::Right)) continue;
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
    }

    return LQPVisitation::VisitInputs;
  });

  for (const auto& [join_node, side_of_join, semi_join_reduction_node] : semi_join_reductions) {
    lqp_insert_node(join_node, side_of_join, semi_join_reduction_node, AllowRightInput::Yes);
  }
}
}  // namespace opossum
