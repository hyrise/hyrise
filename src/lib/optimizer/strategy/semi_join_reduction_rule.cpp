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

    visit_lqp(root, [&](const auto& node) {
      if (node->type != LQPNodeType::Join) return LQPVisitation::VisitInputs;
      const auto join_node = std::static_pointer_cast<JoinNode>(node);

      for (const auto& join_predicate : join_node->join_predicates()) {
        const auto predicate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
        DebugAssert(predicate_expression, "Expected BinaryPredicateExpression");
        if (predicate_expression->predicate_condition != PredicateCondition::Equals) {
          continue;
        }

    // Semi Join Reductions also make sense when they are pushed below, e.g., an expensive aggregate (TPC-H 20?). This can only happen for non-subqueries with WITH or views. Those are not covered yet.

        // Semi/Anti joins are currently handled by the hash join, which performs badly if the right side is much bigger
        // than the left side. For that case, we add a second semi join on the build side, which throws out all values
        // that will not be found by the primary (first) predicate of the later join, anyway. This is the case no matter
        // if the original join is a semi or anti join. In any case, we want the reducing join introduced here to limit
        // the input to the join operator to those values that have a chance of being relevant for the semi/anti join.
        // However, we can only throw away values on the build side if the primary predicate is an equals predicate. For
        // an example, see TPC-H query 21. That query is the main reason this part exists. The current thresholds are
        // somewhat arbitrary. If we start to see more of those cases, we can try and find a more complex heuristic. This
        // is also called "semi join reduction": http://www.db.in.tum.de/research/publications/conferences/semijoin.pdf

        const auto& estimator = cost_estimator->cardinality_estimator;

        const auto reduce_if_beneficial = [&](const auto side_of_join) {
          // right_input holds the right side of the semi join reduction, i.e., the list of values that the left side
          // is filtered on.
          auto right_input = join_node->input(opposite_side(side_of_join));
          auto right_cardinality = estimator->estimate_cardinality(right_input);

          const auto& right_predicate = expression_evaluable_on_lqp(predicate_expression->left_operand(), *right_input) ? predicate_expression->left_operand() : predicate_expression->right_operand();
          DebugAssert(!expression_evaluable_on_lqp(right_predicate, *join_node->input(side_of_join)), "Expected predicate to be uniquely evaluable on one side of the join");

          const auto semi_join_reduction_node = JoinNode::make(JoinMode::Semi, predicate_expression);
          semi_join_reduction_node->comment = "Semi Reduction";
          lqp_insert_node(join_node, side_of_join, semi_join_reduction_node);

          while (true) {
            if (right_input->type != LQPNodeType::Join) break;
            // No matter the join type, we will never see values that were not there before

            const auto try_deeper_right_input = [&](const auto side_of_input) {
              const auto& candidate = right_input->input(side_of_input);
              if (!expression_evaluable_on_lqp(right_predicate, *candidate)) return false;

              const auto candidate_cardinality = estimator->estimate_cardinality(candidate);
              

              if (candidate_cardinality > right_cardinality * 1.01) return false;

              right_input = candidate;
              right_cardinality = candidate_cardinality;

              return true;
            };

            if (try_deeper_right_input(LQPInputSide::Left)) continue;
            if (try_deeper_right_input(LQPInputSide::Right)) continue;
            break;
          }

          semi_join_reduction_node->set_right_input(right_input);

          const auto reduction_input_cardinality = estimator->estimate_cardinality(semi_join_reduction_node->left_input());
          const auto reduction_output_cardinality = estimator->estimate_cardinality(semi_join_reduction_node);
          
          lqp_remove_node(semi_join_reduction_node, AllowRightInput::Yes);

          if (reduction_output_cardinality / reduction_input_cardinality <= .25f) { // TODO move this check up
            semi_join_reductions.emplace_back(join_node, side_of_join, semi_join_reduction_node);
          }
        };

        reduce_if_beneficial(LQPInputSide::Right);
        if (join_node->join_mode != JoinMode::AntiNullAsTrue && join_node->join_mode != JoinMode::AntiNullAsFalse) {
          // TODO Check if doing this for semi makes sense, as it effectively duplicates the join
          reduce_if_beneficial(LQPInputSide::Left);
        }
      }

      return LQPVisitation::VisitInputs;
    });

    for (const auto& [join_node, side_of_join, semi_join_reduction_node] : semi_join_reductions) {
      lqp_insert_node(join_node, side_of_join, semi_join_reduction_node, AllowRightInput::Yes);
    }
  }
}
