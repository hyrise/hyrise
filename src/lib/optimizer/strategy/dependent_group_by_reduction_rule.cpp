#include "dependent_group_by_reduction_rule.hpp"

#include <unordered_map>

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"

namespace {

using namespace hyrise;                 // NOLINT(build/namespaces)
using namespace expression_functional;  // NOLINT(build/namespaces)

/**
 * This function reduces the group-by columns of @param aggregate_node for a given functional dependency (@param fd).
 *
 * @returns Boolean value, denoting whether the group-by list of @param aggregate_node has changed.
 */
bool remove_dependent_group_by_columns(const FunctionalDependency& fd, AggregateNode& aggregate_node,
                                       const ExpressionUnorderedSet& group_by_columns) {
  auto group_by_list_changed = false;

  // To benefit from this rule, the FD's columns have to be part of the group-by list.
  if (!std::all_of(fd.determinants.cbegin(), fd.determinants.cend(),
                   [&group_by_columns](const std::shared_ptr<AbstractExpression>& expression) {
                     return group_by_columns.contains(expression);
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

namespace hyrise {

std::string DependentGroupByReductionRule::name() const {
  static const auto name = std::string{"DependentGroupByReductionRule"};
  return name;
}

void DependentGroupByReductionRule::_apply_to_plan_without_subqueries(
    const std::shared_ptr<AbstractLQPNode>& lqp_root) const {
  visit_lqp(lqp_root, [&](const auto& node) {
    if (node->type != LQPNodeType::Aggregate) {
      return LQPVisitation::VisitInputs;
    }
    auto& aggregate_node = static_cast<AggregateNode&>(*node);

    // --- Preparation --
    // Gather group-by columns.
    const auto fetch_group_by_columns = [&aggregate_node]() {
      auto group_by_columns = ExpressionUnorderedSet{aggregate_node.aggregate_expressions_begin_idx + 1};
      auto node_expressions_iter = aggregate_node.node_expressions.cbegin();
      std::copy(node_expressions_iter, node_expressions_iter + aggregate_node.aggregate_expressions_begin_idx,
                std::inserter(group_by_columns, group_by_columns.end()));
      return group_by_columns;
    };
    auto group_by_columns = fetch_group_by_columns();

    // Early exit (i): If this is an AggregateNode for SELECT DISTINCT (i.e., it has no aggregates) and the requested
    // columns are already distinct, remove the whole node.
    if (group_by_columns.size() == node->node_expressions.size() &&
        node->left_input()->has_matching_ucc(group_by_columns)) {
      const auto& output_expressions = aggregate_node.output_expressions();
      // Remove the AggregateNode if does not change the output expressions.
      if (expressions_equal(output_expressions, node->left_input()->output_expressions())) {
        lqp_remove_node(node);
        return LQPVisitation::VisitInputs;
      }

      // Else, add a ProjectionNode.
      const auto projection_node = ProjectionNode::make(output_expressions);
      lqp_replace_node(node, projection_node);
      return LQPVisitation::VisitInputs;
    }

    // Early exit (ii): If there are no functional dependencies, we can skip this rule.
    const auto& fds = aggregate_node.functional_dependencies();
    if (fds.empty()) {
      return LQPVisitation::VisitInputs;
    }

    // Store a copy of the root's output expressions before applying the rule.
    const auto& root_output_expressions = lqp_root->output_expressions();
    // Also store a copy of the aggregate's output expressions to verify the output column order later on.
    const auto& initial_aggregate_output_expressions = aggregate_node.output_expressions();

    // Get a sorted list of ColumnIDs from an FD's set of determinants.
    const auto get_column_ids = [&](const auto& determinants) {
      auto column_ids = std::vector<ColumnID>{};
      column_ids.reserve(determinants.size());
      for (const auto& expression : determinants) {
        const auto column_id = find_expression_idx(*expression, initial_aggregate_output_expressions);
        Assert(column_id, "Could not find column " + expression->as_column_name());
        column_ids.emplace_back(*column_id);
      }
      std::sort(column_ids.begin(), column_ids.end());
      return column_ids;
    };

    // Sort the FDs by their left set's column count in hope that the shortest will later form the group-by clause.
    auto ordered_fds = std::vector<FunctionalDependency>{fds.cbegin(), fds.cend()};
    std::sort(ordered_fds.begin(), ordered_fds.end(), [&](const auto& fd_left, const auto& fd_right) {
      const auto left_determinant_size = fd_left.determinants.size();
      const auto right_determinant_size = fd_right.determinants.size();
      if (left_determinant_size != right_determinant_size) {
        return left_determinant_size < right_determinant_size;
      }

      // The FDs are expected to be equally useful for the rewrite. However, we have to decide on semantics here to make
      // the order independent of the position of the FDs in the original set (which might differ due to standard
      // library implementation details). Thus, we compare the ColumnIDs of the determinants.
      const auto& left_column_ids = get_column_ids(fd_left.determinants);
      const auto& right_column_ids = get_column_ids(fd_right.determinants);
      return left_column_ids < right_column_ids;
    });

    // --- Main: Reduction phase ---
    // Try to reduce the group-by list by one dependency at a time, starting with the dependency with the fewest
    // determinants.
    auto group_by_list_changed = false;
    for (const auto& fd : ordered_fds) {
      // Early exit: The FD's determinants have to be a subset of the group-by columns.
      if (group_by_columns.size() < fd.determinants.size()) {
        continue;
      }

      const auto success = remove_dependent_group_by_columns(fd, aggregate_node, group_by_columns);
      if (success) {
        // Refresh data structures correspondingly.
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

}  // namespace hyrise
