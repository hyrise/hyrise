#include "dependent_join_rewrite_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

void DependentJoinRewriteRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type != LQPNodeType::Aggregate) {
      return LQPVisitation::VisitInputs;
    }
    auto& aggregate_node = static_cast<AggregateNode&>(*node);

    // Early exit: If there are no functional dependencies, we can skip this rule.
    auto fds = aggregate_node.functional_dependencies();
    if (fds.empty()) return LQPVisitation::VisitInputs;

    // --- Preparation ---
    // Store a copy of the root's output expressions before applying the rule
    const auto root_output_expressions = lqp_root->output_expressions();
    // Also store a copy of the aggregate's output expressions to verify the output column order later on.
    const auto initial_aggregate_output_expressions = aggregate_node.output_expressions();

    // Gather group-by columns
    const auto fetch_group_by_columns = [&aggregate_node]() {
      ExpressionUnorderedSet group_by_columns(aggregate_node.aggregate_expressions_begin_idx + 1);
      auto node_expressions_iter = aggregate_node.node_expressions.cbegin();
      std::copy(node_expressions_iter, node_expressions_iter + aggregate_node.aggregate_expressions_begin_idx,
                std::inserter(group_by_columns, group_by_columns.end()));
      return group_by_columns;
    };
    auto group_by_columns = fetch_group_by_columns();

    // Sort the FDs by their left set's column count in hope that the shortest will later form the group-by clause.
    std::sort(fds.begin(), fds.end(), [](const auto& fd_left, const auto& fd_right) {
      return fd_left.determinants.size() < fd_right.determinants.size();
    });

    // --- Main: Reduction phase ---
    // Try to reduce the group-by list one constraint at a time, starting with the shortest constraint.
    auto group_by_list_changed = false;
    for (const auto& fd : fds) {
      // Early exit: The FD's left column set has to be a subset of the group-by columns
      if (group_by_columns.size() < fd.determinants.size()) continue;

      bool success = remove_dependent_group_by_columns(fd, aggregate_node, group_by_columns);
      if (success) {
        // Refresh data structures correspondingly
        group_by_list_changed = true;
        group_by_columns = fetch_group_by_columns();
      }
    }

    // --- Finish: Ensure correct column order ---
    // In case the initial query plan root returned the same columns in the same column order and was not a projection,
    // it is likely that the result of the current aggregate was either the root itself or only operators followed that
    // do not modify the column order (e.g., sort or limit). In this case, we need to restore the initial column order
    // by adding a projection with the initial output expressions since we changed the column order by moving columns
    // from the group-by list to the aggregations.
    if (group_by_list_changed && initial_aggregate_output_expressions == root_output_expressions &&
        lqp_root->type != LQPNodeType::Projection) {
      const auto projection_node = std::make_shared<ProjectionNode>(root_output_expressions);
      lqp_insert_node(lqp_root, LQPInputSide::Left, projection_node);
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
