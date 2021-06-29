#include "semi_join_removal_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/in_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"

namespace {
using namespace opossum;  // NOLINT

bool is_expensive_predicate(const std::shared_ptr<AbstractExpression>& predicate) {
  const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
  if (binary_predicate) {
    /**
     * ColumnVsColumn
     */
    if (binary_predicate->left_operand()->type == ExpressionType::LQPColumn &&
        binary_predicate->right_operand()->type == ExpressionType::LQPColumn) {
      return true;
    }

    /**
     * LIKE
     */
    if (binary_predicate->predicate_condition == PredicateCondition::Like ||
        binary_predicate->predicate_condition == PredicateCondition::NotLike) {
      return true;
    }
  }

  /**
   * IN List(...)
   */
  const auto in_predicate = std::dynamic_pointer_cast<InExpression>(predicate);
  if (in_predicate && std::dynamic_pointer_cast<ListExpression>(in_predicate->set())) return true;

  /**
   * Correlated Subqueries
   */
  auto predicate_contains_correlated_subquery = false;
  visit_expression(predicate, [&](const auto& sub_expression) {
    if (const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression);
        subquery_expression && subquery_expression->is_correlated()) {
      predicate_contains_correlated_subquery = true;
      return ExpressionVisitation::DoNotVisitArguments;
    } else {
      return ExpressionVisitation::VisitArguments;
    }
  });
  if (predicate_contains_correlated_subquery) return true;

  return false;
}

}  // namespace

namespace opossum {

std::string SemiJoinRemovalRule::name() const {
  static const auto name = std::string{"SemiJoinRemovalRule"};
  return name;
}

void SemiJoinRemovalRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  // Approach: Find a semi reduction node and the corresponding original join node and check whether the cardinality is
  // reduced between the semi join reduction and the original join. If it is not, the semi join reduction is likely not
  // helpful and should be removed.

  Assert(lqp_root->type == LQPNodeType::Root, "SemiJoinRemovalRule needs root to hold onto");

  // You might be able to come up with a case where this caches too much (especially when looking at nested semi joins),
  // but the chance of this is slim enough to risk it and benefit from cached results.
  const auto estimator = cost_estimator->cardinality_estimator->new_instance();
  estimator->guarantee_bottom_up_construction();

  // In some cases, semi joins are added on both sides of the join (e.g., TPC-H Q17). In the LQPTranslator, these will
  // be translated into the same operator. If we remove one of these reductions, we block the reuse of the join result.
  // To counter these cases, we track semi join reductions that should not be removed. `removal_candidates` holds the
  // nodes that can be removed unless there is a semantically identical node in `removal_blockers`.
  // This is slightly ugly, as we have to preempt the behavior of the LQPTranslator. This would be better if we had a
  // method of identifying plan reuse in the optimizer. However, when we tried this, we found that reuse was close to
  // impossible to implement correctly in the presence of self-joins.
  auto removal_candidates = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  auto removal_blockers =
      std::unordered_set<std::shared_ptr<AbstractLQPNode>, LQPNodeSharedPtrHash, LQPNodeSharedPtrEqual>{};

  visit_lqp(lqp_root, [&](const auto& node) {
    {
      // Check if the current node is a semi join reduction
      if (node->type != LQPNodeType::Join) return LQPVisitation::VisitInputs;
      const auto& join_node = static_cast<const JoinNode&>(*node);
      if (!join_node.is_reducer()) return LQPVisitation::VisitInputs;
      Assert(join_node.join_predicates().size() == 1, "Did not expect multi-predicate semi join reduction.");
    }

    const auto& semi_reduction_node = static_cast<const JoinNode&>(*node);
    const auto semi_join_predicate = semi_reduction_node.join_predicates().at(0);

    // Since multiple output nodes profit from the reduction, we do not remove it. Compare JOB query 29b, for example.
    if (semi_reduction_node.output_count() > 1) {
      removal_blockers.emplace(node);
      return LQPVisitation::VisitInputs;
    }

    // Find an upper node that corresponds to this node
    visit_lqp_upwards(node, [&](const auto& upper_node) {  // todo maybe pass semi_reduction_node instead of node
      // Skip the semi node found before, i.e., start with its outputs
      if (node == upper_node) return LQPUpwardVisitation::VisitOutputs;

      // The estimation for aggregate and column/column scans is bad, so whenever one of these occurs between the semi
      // join reduction and the original join, do not remove the semi join reduction (i.e., abort the search for an
      // upper node).
      if (upper_node->type == LQPNodeType::Aggregate) {
        removal_blockers.emplace(node);
        return LQPUpwardVisitation::DoNotVisitOutputs;
      }

      if (upper_node->type == LQPNodeType::Predicate) {
        const auto& upper_predicate_node = static_cast<const PredicateNode&>(*upper_node);

        if (is_expensive_predicate(upper_predicate_node.predicate())) {
          removal_blockers.emplace(node);
          return LQPUpwardVisitation::DoNotVisitOutputs;
        }
      }

      if (upper_node->type != LQPNodeType::Join) return LQPUpwardVisitation::VisitOutputs;
      const auto& upper_join_node = static_cast<const JoinNode&>(*upper_node);

      // In most cases, the right side of the semi join reduction is one of the inputs of the original join. In rare
      // cases, this is not true (see try_deeper_reducer_node in semi_join_reduction_rule.cpp). Those cases are not
      // covered by this removal rule yet.
      const auto semi_join_is_left_input = upper_join_node.right_input() == semi_reduction_node.right_input();
      const auto semi_join_is_right_input = upper_join_node.left_input() == semi_reduction_node.right_input();
      if (!semi_join_is_left_input && !semi_join_is_right_input) {
        removal_blockers.emplace(node);
        return LQPUpwardVisitation::DoNotVisitOutputs;
      }
      auto input_side = semi_join_is_left_input ? LQPInputSide::Left : LQPInputSide::Right;

      // Check whether the semi join reduction predicate matches one of the predicates of the upper join
      if (std::none_of(upper_join_node.join_predicates().begin(), upper_join_node.join_predicates().end(),
                       [&](const auto predicate) { return *predicate == *semi_join_predicate; })) {
        return LQPUpwardVisitation::VisitOutputs;
      }

      // Estimate the output cardinality of the semi join reduction and the input cardinality of the corresponding join
      const auto semi_join_input_cardinality = estimator->estimate_cardinality(node->left_input());
      const auto semi_join_output_cardinality = estimator->estimate_cardinality(node);
      const auto join_input_cardinality = estimator->estimate_cardinality(upper_node->input(input_side));

      const auto semi_join_selectivity = semi_join_output_cardinality / semi_join_input_cardinality;
      const auto upper_nodes_selectivity = join_input_cardinality / semi_join_output_cardinality;

      // Remove the semi join reduction if it does not help ...
      if (semi_join_selectivity < upper_nodes_selectivity) {
        removal_blockers.emplace(node);
      } else {
        removal_candidates.emplace(node);
      }

      return LQPUpwardVisitation::DoNotVisitOutputs;
    });

    return LQPVisitation::VisitInputs;
  });

  for (const auto& removal_candidate : removal_candidates) {
    if (removal_blockers.contains(removal_candidate)) continue;
    lqp_remove_node(removal_candidate, AllowRightInput::Yes);
  }
}
}  // namespace opossum
