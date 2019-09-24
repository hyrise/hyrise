#include "semi_join_reduction_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"

namespace opossum {
  void SemiJoinReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
    Assert(root->type == LQPNodeType::Root, "ExpressionReductionRule needs root to hold onto");

    std::vector<std::tuple<std::shared_ptr<JoinNode>, LQPInputSide, std::shared_ptr<JoinNode>>> semi_join_reductions;

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
        {
          const auto semi_join_reduction_node = JoinNode::make(JoinMode::Semi, join_predicate);
          semi_join_reduction_node->comment = "Semi Reduction";
          lqp_insert_node(join_node, LQPInputSide::Right, semi_join_reduction_node);
          semi_join_reduction_node->set_right_input(join_node->left_input());

          const auto& estimator = cost_estimator->cardinality_estimator;
          const auto reduction_input_cardinality = estimator->estimate_cardinality(semi_join_reduction_node->left_input());
          const auto reduction_output_cardinality = estimator->estimate_cardinality(semi_join_reduction_node);
          
          semi_join_reduction_node->set_right_input(nullptr);
          lqp_remove_node(semi_join_reduction_node);

          if (reduction_output_cardinality / reduction_input_cardinality <= .1f) { // TODO
            semi_join_reductions.emplace_back(join_node, LQPInputSide::Right, semi_join_reduction_node);
          }
        }

        {
          const auto semi_join_reduction_node = JoinNode::make(JoinMode::Semi, join_predicate);
          semi_join_reduction_node->comment = "Semi Reduction";
          lqp_insert_node(join_node, LQPInputSide::Left, semi_join_reduction_node);
          semi_join_reduction_node->set_right_input(join_node->right_input());

          const auto& estimator = cost_estimator->cardinality_estimator;
          const auto reduction_input_cardinality = estimator->estimate_cardinality(semi_join_reduction_node->left_input());
          const auto reduction_output_cardinality = estimator->estimate_cardinality(semi_join_reduction_node);
          
          semi_join_reduction_node->set_right_input(nullptr);
          lqp_remove_node(semi_join_reduction_node);

          if (reduction_output_cardinality / reduction_input_cardinality <= .1f) { // TODO
            semi_join_reductions.emplace_back(join_node, LQPInputSide::Left, semi_join_reduction_node);
          }
        }
      }

      return LQPVisitation::VisitInputs;
    });

    for (const auto& [join_node, side_of_join, semi_join_reduction_node] : semi_join_reductions) {
      // TODO explain why only right side
      lqp_insert_node(join_node, side_of_join, semi_join_reduction_node);
      semi_join_reduction_node->set_right_input(join_node->input(side_of_join == LQPInputSide::Left ? LQPInputSide::Right : LQPInputSide::Left));
    }
  }
}
