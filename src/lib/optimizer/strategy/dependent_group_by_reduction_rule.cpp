#include "dependent_group_by_reduction_rule.hpp"

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

namespace {
using namespace opossum;  // NOLINT

/**
 * This function reduces the group-by columns of @param aggregate_node for a given functional dependency (@param fd).
 *
 * @returns Boolean value, denoting whether the group-by list of @param aggregate_node has changed.
 */
bool remove_dependent_group_by_columns(const FunctionalDependency& fd, AggregateNode& aggregate_node,
                                       const ExpressionUnorderedSet& group_by_columns) {
  auto group_by_list_changed = false;

  // To benefit from this rule, the FD's columns have to be part of the group-by list
  if (!std::all_of(fd.determinants.cbegin(), fd.determinants.cend(),
                   [&group_by_columns](const std::shared_ptr<AbstractExpression>& constraint_expression) {
                     return group_by_columns.contains(constraint_expression);
                   })) {
    return false;
  }

  // Every column that is functionally dependent gets moved from the group-by list to the aggregate list. For this
  // purpose it is wrapped in an ANY() expression.
  for (const auto& group_by_column : group_by_columns) {
    if (fd.dependents.contains(group_by_column)) {
      // Remove column from group-by list.
      // Further, decrement the aggregate's index which denotes the end of group-by expressions.
      const auto begin_idx_before = aggregate_node.aggregate_expressions_begin_idx;
      aggregate_node.node_expressions.erase(
          std::remove_if(aggregate_node.node_expressions.begin(), aggregate_node.node_expressions.end(),
                         [&](const auto node_expression) {
                           if (*node_expression == *group_by_column) {
                             // Adjust the number of group by expressions.
                             --aggregate_node.aggregate_expressions_begin_idx;
                             group_by_list_changed = true;
                             return true;
                           }
                           return false;
                         }),
          aggregate_node.node_expressions.end());
      Assert(aggregate_node.aggregate_expressions_begin_idx < begin_idx_before,
             "Failed to remove column from group-by list.");
      // Add the ANY() aggregate to the list of aggregate columns.
      const auto aggregate_any_expression = any_(group_by_column);
      aggregate_node.node_expressions.emplace_back(aggregate_any_expression);
    }
  }

  return group_by_list_changed;
}

}  // namespace

namespace opossum {

void DependentGroupByReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root_lqp) const {
  visit_lqp(root_lqp, [&](const auto& node) {
    if (node->type != LQPNodeType::Aggregate) {
      return LQPVisitation::VisitInputs;
    }
    auto& aggregate_node = static_cast<AggregateNode&>(*node);

    // Early exit: If there are no functional dependencies, we can skip this rule.
    auto fds = aggregate_node.functional_dependencies();
    if (fds.empty()) return LQPVisitation::VisitInputs;

    // --- Preparation ---
    // Store a copy of the root's output expressions before applying the rule
    const auto root_output_expressions = root_lqp->output_expressions();
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
        root_lqp->type != LQPNodeType::Projection) {
      const auto projection_node = std::make_shared<ProjectionNode>(root_output_expressions);
      lqp_insert_node(root_lqp, LQPInputSide::Left, projection_node);
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
