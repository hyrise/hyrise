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

bool is_expensive_predicate(const std::shared_ptr<AbstractExpression>& predicate) {
  // TODO: Maybe also consider the predicate multiplier defined by the cost model

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
   * IS NULL
   */
  if (std::dynamic_pointer_cast<IsNullExpression>(predicate)) return true;

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
   */
  auto removal_candidates = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  auto removal_blockers =
      std::unordered_set<std::shared_ptr<AbstractLQPNode>, LQPNodeSharedPtrHash, LQPNodeSharedPtrEqual>{};
  auto corresponding_join_by_semi_reduction =
      std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<AbstractLQPNode>>{};
  auto corresponding_join_input_side_by_semi_reduction =
      std::unordered_map<std::shared_ptr<AbstractLQPNode>, LQPInputSide>{};

  /**
   * Phase 1: Find semi join reductions & determine removal blockers.
   */
  visit_lqp(lqp_root, [&](const auto& node) {
    // Check if the current node is a semi join reduction
    if (node->type != LQPNodeType::Join) return LQPVisitation::VisitInputs;
    const auto& join_node = static_cast<const JoinNode&>(*node);

    // todo rename rule if we continue to look at semi join reductions only
    if (!join_node.is_reducer()) return LQPVisitation::VisitInputs;
    Assert(join_node.join_predicates().size() == 1, "Did not expect multi-predicate semi join reduction.");

    const auto& semi_reduction_node = static_cast<const JoinNode&>(*node);
    const auto semi_join_predicate = semi_reduction_node.join_predicates().at(0);

    /**
     * Phase 2: Traverse upwards until finding the JoinNode that corresponds to semi_reduction_node.
     *          Add removal blocker in case of expensive nodes and predicates that benefit from semi_reduction_node.
     */
    visit_lqp_upwards(node, [&](const auto& upper_node) {  // todo maybe pass semi_reduction_node instead of node
      /**
       * ToDo Benchmark this:
       */
//      if (upper_node.output_count() > 1) {
//        removal_blockers.emplace(node);
//        return LQPUpwardVisitation::DoNotVisitOutputs;
//      }

      // Skip the semi node found before, i.e., start with its output
      if (node == upper_node) return LQPUpwardVisitation::VisitOutputs;

      /**
       * Removal Blocker: AggregateNode
       *  The estimation for aggregate and column/column scans is bad, so whenever one of these occurs between the semi
       *  join reduction and the original join, do not remove the semi join reduction (i.e., abort the search for an
       *  upper node).
       */
      if (upper_node->type == LQPNodeType::Aggregate) {
        removal_blockers.emplace(node);
        return LQPUpwardVisitation::DoNotVisitOutputs;
      }

      /**
       * Removal Blocker: Expensive PredicateNode
       */
      if (upper_node->type == LQPNodeType::Predicate) {
        const auto& upper_predicate_node = static_cast<const PredicateNode&>(*upper_node);

        if (is_expensive_predicate(upper_predicate_node.predicate())) {
          removal_blockers.emplace(node);
          return LQPUpwardVisitation::DoNotVisitOutputs;
        }
      }

      // Skip all other nodes, except for Joins.
      if (upper_node->type != LQPNodeType::Join) return LQPUpwardVisitation::VisitOutputs;

      /**
       * JoinNode
       *  a) Corresponding Join
       *      In most cases, the right side of the semi join reduction is one of the inputs of the original join. In rare
       *      cases, this is not true (see try_deeper_reducer_node in semi_join_reduction_rule.cpp). Those cases are not
       *      covered by this removal rule yet.
       *  b) Removal Blocker
       */
      const auto& upper_join_node = static_cast<const JoinNode&>(*upper_node);
      const auto semi_join_is_left_input = upper_join_node.right_input() == semi_reduction_node.right_input();
      const auto semi_join_is_right_input = upper_join_node.left_input() == semi_reduction_node.right_input();
      if (semi_join_is_left_input || semi_join_is_right_input) {
        Assert(semi_join_is_left_input != semi_join_is_right_input, "Semi join must be either left or right input.");
        // Check whether the semi join reduction predicate matches one of the predicates of the upper join
        if (std::any_of(upper_join_node.join_predicates().begin(), upper_join_node.join_predicates().end(),
                        [&](const auto predicate) { return *predicate == *semi_join_predicate; })) {
          corresponding_join_by_semi_reduction.emplace(node, upper_node);
          const auto join_input_side = semi_join_is_left_input ? LQPInputSide::Left : LQPInputSide::Right;
          corresponding_join_input_side_by_semi_reduction.emplace(node, join_input_side);
        }
      }
      // If JoinNode does not correspond to semi_reduction_node, add a removal blocker and abort the upwards traversal.
      if (!corresponding_join_by_semi_reduction.contains(node)) {
        removal_blockers.emplace(node);
        return LQPUpwardVisitation::DoNotVisitOutputs;
      }

      removal_candidates.emplace(node);
      return LQPUpwardVisitation::DoNotVisitOutputs;
    });

    return LQPVisitation::VisitInputs;
  });

  if (removal_candidates.empty()) return;

  /**
   * Phase 3: Remove semi join reductions
   */
  for (const auto& removal_candidate : removal_candidates) {
    if (removal_blockers.contains(removal_candidate)) continue;

    // (1) Estimate selectivity of Semi Reduction
    auto semi_reduction_selectivity = 1.0f;
    const auto cardinality_estimator = cost_estimator->cardinality_estimator->new_instance();
    const auto cardinality_in = cardinality_estimator->estimate_cardinality(removal_candidate->left_input());
    const auto semi_reduction_cardinality_out = cardinality_estimator->estimate_cardinality(removal_candidate);
    if (cardinality_in > 100.0f) {
      semi_reduction_selectivity = semi_reduction_cardinality_out / cardinality_in;
    }

    // (2) Remove Semi Reduction node, but store information to revert this change.
    const auto outputs = removal_candidate->outputs();
    const auto input_sides = removal_candidate->get_input_sides();
    const auto left_input = removal_candidate->left_input();
    const auto right_input = removal_candidate->right_input();
    lqp_remove_node(removal_candidate, AllowRightInput::Yes);
    if (semi_reduction_selectivity >= 1.0f) continue;

    // (3) Estimate ...
    const auto& corresponding_join_node = corresponding_join_by_semi_reduction.at(removal_candidate);
    Assert(corresponding_join_node, "Expected corresponding join node for given Semi Reduction.");
    const auto corresponding_join_input_side = corresponding_join_input_side_by_semi_reduction.at(removal_candidate);

    const auto cardinality_estimator2 = cost_estimator->cardinality_estimator->new_instance();
    const auto cardinality_out =
        cardinality_estimator2->estimate_cardinality(corresponding_join_node->input(corresponding_join_input_side));
    const auto other_predicates_selectivity = cardinality_out / cardinality_in;

    // (4) Re-add semi join reduction, if ...
    if (semi_reduction_selectivity < other_predicates_selectivity) {
      std::cout << "  -> Re-added semi-join reduction.";
      removal_candidate->set_left_input(left_input);
      removal_candidate->set_right_input(right_input);
      for (size_t output_idx = 0; output_idx < outputs.size(); ++output_idx) {
        outputs[output_idx]->set_input(input_sides[output_idx], removal_candidate);
      }
    }
  }
}

}  // namespace opossum
