#include "semi_join_removal_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"

namespace {
using namespace opossum;  // NOLINT

/**
 * TODO.. Doc
 * @param predicate
 * @return
 * TODO: Maybe also consider the predicate multiplier defined by the cost model
 */
bool is_expensive_predicate(const std::shared_ptr<AbstractExpression>& predicate) {
  const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
  if (binary_predicate) {
    // ColumnVsColumn
    if (binary_predicate->left_operand()->type == ExpressionType::LQPColumn &&
        binary_predicate->right_operand()->type == ExpressionType::LQPColumn) {
      return true;
    }

    // LIKE
    if (binary_predicate->predicate_condition == PredicateCondition::Like ||
        binary_predicate->predicate_condition == PredicateCondition::NotLike) {
      return true;
    }
  }

  // IS NULL
  if (std::dynamic_pointer_cast<IsNullExpression>(predicate)) return true;

  // IN List(...)
  const auto in_predicate = std::dynamic_pointer_cast<InExpression>(predicate);
  if (in_predicate && std::dynamic_pointer_cast<ListExpression>(in_predicate->set())) return true;

  // Correlated Subqueries
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
  Assert(lqp_root->type == LQPNodeType::Root, "SemiJoinRemovalRule needs root to hold onto");

  /**
   * APPROACH
   *  1. Find semi join reduction node
   *  2. Find corresponding original join node and check whether there are expensive operators between the original join
   *     and the semi join reduction node and track removal candidates.
   *  3. Remove semi join reductions
   *
   * REMOVAL BLOCKERS TODO Adjust!
   *  In some cases, semi joins are added on both sides of the join (e.g., TPC-H Q17). In the LQPTranslator, these will
   *  be translated into the same operator. If we remove one of these reductions, we block the reuse of the join result.
   *  To counter these cases, we track semi join reductions that should not be removed. `removal_candidates` holds the
   *  nodes that can be removed unless there is a semantically identical node in `removal_blockers`.
   *  This is slightly ugly, as we have to preempt the behavior of the LQPTranslator. This would be better if we had a
   *  method of identifying plan reuse in the optimizer. However, when we tried this, we found that reuse was close to
   *  impossible to implement correctly in the presence of self-joins.
   *
   *  TODO: Is this implemented?
   *    // Semi Reduction is directly followed by the corresponding join.
   *    if (corresponding_join_by_semi_reduction.at(removal_candidate)->input(corresponding_join_input_side) == removal_candidate) {
   */
  auto removal_candidates = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  auto removal_blockers =
      std::unordered_set<std::shared_ptr<AbstractLQPNode>, LQPNodeSharedPtrHash, LQPNodeSharedPtrEqual>{};
  auto corresponding_join_by_semi_reduction =
      std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>{};

  /**
   * Phase 1: Collect semi join reductions
   *            TODO rename rule if we continue to look at semi join reductions only
   */
  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type != LQPNodeType::Join) return LQPVisitation::VisitInputs;
    const auto& join_node = static_cast<const JoinNode&>(*node);
    // Check if the current node is a semi join reduction and ... TODO
    if (join_node.is_reducer() && join_node.output_count() == 1) {
      Assert(join_node.join_predicates().size() == 1, "Did not expect multi-predicate semi join reduction.");
      removal_candidates.emplace(node);
    }
    return LQPVisitation::VisitInputs;
  });

  /**
   * Phase 2: Find corresponding JoinNode & Determine removal blockers.
   */
  for (const auto& removal_candidate : removal_candidates) {
    const auto& semi_reduction_node = static_cast<const JoinNode&>(*removal_candidate);
    visit_lqp_upwards(removal_candidate, [&](const auto& upper_node) {
      if (upper_node->output_count() > 1) {
        std::cout << "Node has several outputs: " << upper_node->description() << std::endl;
      }

      // Start with the output(s) of the removal candidate
      if (upper_node == removal_candidate) return LQPUpwardVisitation::VisitOutputs;

      // Removal Blocker: AggregateNode
      //  The estimation for aggregate and column/column scans is bad, so whenever one of these occurs between the semi
      //  join reduction and the original join, do not remove the semi join reduction (i.e., abort the search for an
      //  upper node).
      if (upper_node->type == LQPNodeType::Aggregate) {
        removal_blockers.emplace(removal_candidate);
        return LQPUpwardVisitation::DoNotVisitOutputs;
      }

      // Removal Blocker: PredicateNode, unless it is not expensive.
      if (upper_node->type == LQPNodeType::Predicate) {
        const auto& upper_predicate_node = static_cast<const PredicateNode&>(*upper_node);
        if (is_expensive_predicate(upper_predicate_node.predicate())) {
          removal_blockers.emplace(removal_candidate);
          return LQPUpwardVisitation::DoNotVisitOutputs;
        }
      }

      // Skip all other nodes, except for joins.
      if (upper_node->type != LQPNodeType::Join) return LQPUpwardVisitation::VisitOutputs;

      // Add Removal Blocker, if it is not the corresponding join.
      const auto& upper_join_node = static_cast<const JoinNode&>(*upper_node);
      if (upper_join_node != *semi_reduction_node.get_corresponding_join_node()) {
        removal_blockers.emplace(removal_candidate);
      } else {
        std::cout << "Found corresponding join" << std::endl;
      }

      return LQPUpwardVisitation::DoNotVisitOutputs;
    });
  }

  /**
   * Phase 3: Remove semi join reduction nodes
   */
  size_t removed_reductions_count = 0;
  for (const auto& removal_candidate : removal_candidates) {
    if (removal_blockers.contains(removal_candidate)) continue;
    lqp_remove_node(removal_candidate, AllowRightInput::Yes);
    removed_reductions_count++;
  }
  std::cout << "Removed semi join reductions: " << removed_reductions_count << std::endl;
}

}  // namespace opossum
