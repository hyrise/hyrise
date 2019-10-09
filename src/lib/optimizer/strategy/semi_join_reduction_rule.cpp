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
    // look at each predicate of the join independently.
    for (const auto& join_predicate : join_node->join_predicates()) {
      const auto predicate_expression = std::dynamic_pointer_cast<BinaryPredicateExpression>(join_predicate);
      DebugAssert(predicate_expression, "Expected BinaryPredicateExpression");
      if (predicate_expression->predicate_condition != PredicateCondition::Equals) {
        continue;
      }

      // Since semi join reductions might be beneficial for both sides of the join, we create this helper lambda,
      // which can deal with both sides.
      const auto reduce_if_beneficial = [&](const auto side_of_join) {
        // In this block, "left" refers to the left input of the (potential) semi join reduction, i.e., the table that
        // is getting filtered and "right" refers to the table that the left side is filtered by. As such, right_input
        // holds the right side of the semi join reduction, i.e., the list of values that the left side is filtered on.
        // This is independent on the side of the original join that we are operating on, which is called side_of_join.
        auto left_cardinality = estimator->estimate_cardinality(join_node->input(side_of_join));
        auto right_input = join_node->input(opposite_side(side_of_join));
        auto right_cardinality = estimator->estimate_cardinality(right_input);

        const auto semi_join_reduction_node = JoinNode::make(JoinMode::Semi, predicate_expression);
        semi_join_reduction_node->comment = "Semi Reduction";
        lqp_insert_node(join_node, side_of_join, semi_join_reduction_node);

        semi_join_reduction_node->set_right_input(right_input);

        const auto reduction_input_cardinality =
            estimator->estimate_cardinality(semi_join_reduction_node->left_input());
        const auto reduction_output_cardinality = estimator->estimate_cardinality(semi_join_reduction_node);

        semi_join_reduction_node->set_right_input(nullptr);
        lqp_remove_node(semi_join_reduction_node);

        if (reduction_output_cardinality / reduction_input_cardinality > MINIMUM_SELECTIVITY) {
          return;
        }

        // For `t1 JOIN t2 on t1.a = t2.a` where a semi join reduction is supposed to be added to the t1, `t1.a = t2.a`
        // is the predicate_expression. The values from t1 are supposed to be filtered by looking at t1.a, which is
        // called the left_expression.
        const auto& left_expression = expression_evaluable_on_lqp(predicate_expression->left_operand(), *right_input)
                                          ? predicate_expression->left_operand()
                                          : predicate_expression->right_operand();
        DebugAssert(!expression_evaluable_on_lqp(left_expression, *join_node->input(side_of_join)),
                    "Expected filtered expression to be uniquely evaluable on one side of the join");

        // Currently, the right input of the semi join reduction is opposite_side(side_of_join). However, on that side,
        // multiple joins have been added that increase the cardinality. We walk the LQP starting with right_input
        // taking those paths where lext_expression continues to be evaluable.
        while (true) {
          if (right_input->type != LQPNodeType::Join) {
            break;
          }
          // No matter the join type, we will never see values that were not there before

          const auto try_deeper_right_input = [&](const auto side_of_input) {
            const auto& candidate = right_input->input(side_of_input);
            std::cout << "deeper right candidate: " << candidate->description() << std::endl;
            if (!expression_evaluable_on_lqp(left_expression, *candidate)) {
              break;
            }

            const auto candidate_cardinality = estimator->estimate_cardinality(candidate);

            // Check if the candidate has an equal or lower cardinality. Allow for some tolerance due to float values
            // being used.
            if (candidate_cardinality > right_cardinality * 1.01) {
              return false;
            }

            right_input = candidate;
            right_cardinality = candidate_cardinality;

            return true;
          };

          if (try_deeper_right_input(LQPInputSide::Left)) continue;
          if (try_deeper_right_input(LQPInputSide::Right)) continue;
          break;
        }

        if (right_cardinality > left_cardinality) {
          // The JoinHash, which is currently used for semi joins is bad at handling cases where the build side is
          // larger than the probe side. In these cases, do not add a semi join reduction for now.
          return;
        }

        semi_join_reduction_node->set_right_input(right_input);
        semi_join_reductions.emplace_back(join_node, side_of_join, semi_join_reduction_node);
      };

      // Having defined the lambda responsible for conditionally adding a semi join reduction, we now apply ot to both
      // inputs of the join.
      reduce_if_beneficial(LQPInputSide::Right);

      // On the left side we must not create semi join reductions for anti joins as those rely on the very existence of
      // non-matching values on the right side. Also, we should not create semi join reductions for semi joins as those
      // would simply duplicate the original join.
      if (join_node->join_mode != JoinMode::AntiNullAsTrue && join_node->join_mode != JoinMode::AntiNullAsFalse &&
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
