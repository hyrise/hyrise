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
 * This function reduces the group-by columns of @param aggregate_node for the inputs @param stored_table_node and
 * @param table_constraint. @param group_by_columns is passed as it already stores the ColumnID's which are used for
 * intersection instead of retrieving them from @param aggregate_node again.
 *
 * @returns   Boolean value denoting whether at the group-by list of @param aggregate_node changed.
 */
bool reduce_group_by_columns_for_fd(const FunctionalDependency& fd, ExpressionUnorderedSet& group_by_columns, AggregateNode& aggregate_node) {
  auto group_by_list_changed = false;

  // To benefit from this rule, the FD's  columns have to be part of the group-by list
  if(!std::all_of(fd.first.cbegin(), fd.first.cend(), [&group_by_columns](std::shared_ptr<AbstractExpression> constraint_col_expr) {
    return group_by_columns.contains(constraint_col_expr);
  })) {
    return false;
  }

  // Every column that is functionally dependent gets moved from
  // the group-by list to the aggregate list. For this purpose it is wrapped in an ANY() expression.
  for (const auto& group_by_column : group_by_columns) {

    if (fd.second.contains(group_by_column)) {
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
      Assert(aggregate_node.aggregate_expressions_begin_idx
             < begin_idx_before, "Failed to remove column from group-by list.");

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
    if(fds.empty()) return LQPVisitation::VisitInputs;

    // --- Preparation ---
    // Store a copy of the root's column expressions before applying the rule
    const auto root_column_expressions = root_lqp->column_expressions();
    // Also store a copy of the aggregate's column expressions to verify the output column order later on.
    const auto initial_aggregate_column_expressions = aggregate_node.column_expressions();
    auto group_by_list_changed = false;

    // Collect non-nullable group-by columns
    ExpressionUnorderedSet group_by_columns_non_nullable;
    group_by_columns_non_nullable.reserve(aggregate_node.aggregate_expressions_begin_idx + 1);
    for (auto expression_idx = size_t{0}; expression_idx < aggregate_node.aggregate_expressions_begin_idx;
         ++expression_idx) {
      const auto& expression = aggregate_node.node_expressions[expression_idx];
      // Check that group by columns are not nullable. Unique columns can generally store NULLs while previous
      // operators (e.g., outer joins) might have added NULLs to a primary key column. For now, we take the safe route
      // and ignore all cases where group-by columns are nullable.
      if (expression->is_nullable_on_lqp(aggregate_node)) {
        continue;
      }
      group_by_columns_non_nullable.insert(expression);
    }

    // Sort the FDs by their left set's column count in hope that the shortest will later form the group-by clause.
    std::sort(fds.begin(), fds.end(),
              [](const auto& fd_left, const auto& fd_right) {
                return fd_left.first.size() < fd_right.first.size();
    });

    // --- Main ---
    // Try to reduce the group-by list one constraint at a time, starting with the shortest constraint. Multiple
    // constraints might result in different group-by lists (depending on the number of columns of the constraint).
    // We stop as soon as one constraint successfully reduced the group-by list. There is no advantage in a second reduction if the first reduction was successful, because no further columns will be removed. Hence, as soon as one reduction took
    // place, we can ignore the remaining constraints.
    for(const auto& fd : fds) {
      group_by_list_changed |= reduce_group_by_columns_for_fd(fd, group_by_columns_non_nullable,
          aggregate_node);
      if (group_by_list_changed) break;
    }

    // --- Finish ---
    // In case the initial query plan root returned the same columns in the same column order and was not a projection,
    // it is likely that the result of the current aggregate was either the root itself or only operators followed that
    // do not modify the column order (e.g., sort or limit). In this case, we need to restore the initial column order
    // by adding a projection with the initial column_references since we changed the column order by moving columns
    // from the group-by list to the aggregations.
    if (group_by_list_changed && initial_aggregate_column_expressions == root_column_expressions &&
        root_lqp->type != LQPNodeType::Projection) {
      const auto projection_node = std::make_shared<ProjectionNode>(root_column_expressions);
      lqp_insert_node(root_lqp, LQPInputSide::Left, projection_node);
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
