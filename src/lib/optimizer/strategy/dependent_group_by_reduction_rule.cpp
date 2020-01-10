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
bool reduce_group_by_columns_for_constraint(const TableConstraintDefinition& table_constraint,
                                            const std::set<ColumnID>& group_by_columns,
                                            const std::shared_ptr<const StoredTableNode>& stored_table_node,
                                            AggregateNode& aggregate_node) {
  auto group_by_list_changed = false;
  const auto& constraint_columns = table_constraint.columns;

  // Intersect primary key/unique columns and group-by columns. In case a primary key/unique constraint covers
  // multiple columns, we need to check that all columns are present in order to later remove dependent columns.
  std::vector<ColumnID> intersection;
  std::set_intersection(constraint_columns.begin(), constraint_columns.end(), group_by_columns.begin(),
                        group_by_columns.end(), std::back_inserter(intersection));

  // Skip the current constraint as the primary key/unique constraint is not completely present.
  if (intersection.size() != constraint_columns.size()) {
    return false;
  }

  // Every column that is part of the table but not of the primary key/unique constraint is going to be moved from
  // the group-by list to the list of aggregates wrapped in an ANY().
  for (const auto& group_by_column : group_by_columns) {
    if (std::find(constraint_columns.begin(), constraint_columns.end(), group_by_column) != constraint_columns.end()) {
      // Do not touch primary key/unique constraint columns.
      continue;
    }

    // Remove node expression if it is a column reference and references the given stored table node.
    // Further, decrement the aggregate's index which denotes the end of group-by expressions.
    aggregate_node.node_expressions.erase(
        std::remove_if(aggregate_node.node_expressions.begin(), aggregate_node.node_expressions.end(),
                       [&, stored_table_node = stored_table_node](const auto expression) {
                         const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
                         if (!column_expression) return false;

                         const auto& expression_stored_table_node = std::dynamic_pointer_cast<const StoredTableNode>(
                             column_expression->column_reference.original_node());
                         if (!expression_stored_table_node) return false;

                         const auto column_id = column_expression->column_reference.original_column_id();
                         if (stored_table_node == expression_stored_table_node && group_by_column == column_id) {
                           // Adjust the number of group by expressions.
                           --aggregate_node.aggregate_expressions_begin_idx;
                           group_by_list_changed = true;
                           return true;
                         }
                         return false;
                       }),
        aggregate_node.node_expressions.end());

    // Add the ANY() aggregate to the list of aggregate columns.
    const auto aggregate_any_expression = any_(lqp_column_({stored_table_node, group_by_column}));
    aggregate_node.node_expressions.emplace_back(aggregate_any_expression);
  }

  return group_by_list_changed;
}
}  // namespace

namespace opossum {

void DependentGroupByReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  // Store a copy of the root's column expressions.
  const auto root_column_expressions = lqp->column_expressions();
  visit_lqp(lqp, [&](const auto& node) {
    if (node->type != LQPNodeType::Aggregate) {
      return LQPVisitation::VisitInputs;
    }
    auto& aggregate_node = static_cast<AggregateNode&>(*node);

    std::unordered_map<std::shared_ptr<const StoredTableNode>, std::set<ColumnID>> group_by_columns_per_table;
    // Collect the group-by columns for each table in the aggregate node
    for (auto expression_idx = size_t{0}; expression_idx < aggregate_node.aggregate_expressions_begin_idx;
         ++expression_idx) {
      const auto& expression = aggregate_node.node_expressions[expression_idx];

      // Check that group by columns are not nullable. Unique columns can generally store NULLs while previous
      // operators (e.g., outer joins) might have added NULLs to a primary key column. For now, we take the safe route
      // and ignore all cases where group-by columns are nullable.
      if (expression->is_nullable_on_lqp(aggregate_node)) {
        continue;
      }

      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      if (!column_expression) {
        // In case the group-by column is not a column expression (e.g., grouping by `a+1`), we take the safe route and
        // continue even though constraints often hold for such simple expression.
        continue;
      }

      const auto& stored_table_node =
          std::dynamic_pointer_cast<const StoredTableNode>(column_expression->column_reference.original_node());
      // If column is not a physical column skip
      if (!stored_table_node) continue;

      const auto column_id = column_expression->column_reference.original_column_id();
      group_by_columns_per_table[stored_table_node].insert(column_id);
    }

    // Store copy of the aggregate's column expressions to later check if this order is the query's final column order
    const auto initial_aggregate_column_expressions = aggregate_node.column_expressions();
    auto group_by_list_changed = false;

    // Main loop. Iterate over the tables and its group-by columns, get table constraints, and try to reduce for each
    // constraint.
    for (const auto& [stored_table_node, group_by_columns] : group_by_columns_per_table) {
      auto unique_columns = std::set<ColumnID>();

      const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      const auto& table_constraints = table->get_soft_unique_constraints();
      if (table_constraints.empty()) {
        // early exit for current table if no constraints are set
        continue;
      }

      // Gather sizes of table constraints (i.e., the number of columns) to start with the shortest constraint (either
      // unique or primary key) in hope that the shorter one will later form the group-by clause.
      std::vector<std::pair<size_t, size_t>> constraints_position_and_size;
      constraints_position_and_size.reserve(table_constraints.size());
      auto constraint_id = size_t{0};
      for (const auto& table_constraint : table_constraints) {
        constraints_position_and_size.emplace_back(constraint_id, table_constraint.columns.size());
        ++constraint_id;
      }
      std::sort(constraints_position_and_size.begin(), constraints_position_and_size.end(),
                [](const auto& left, const auto& right) { return left.second < right.second; });

      // Try to reduce the group-by list one constraint at a time, starting with the shortest constraint. Multiple
      // constraints (e.g., a primary key and a unique column constraint) might result in different group-by lists
      // (depending on the number of columns of the constraint), but we stop as soon as one constraint successfully
      // reduced the group-by list. The reason is that there is no advantage in a second reduction if the first
      // reduction was successful, because no further columns will be removed. Hence, as soon as one reduction took
      // place, we can ignore the remaining constraints.
      for (const auto& [position, size] : constraints_position_and_size) {
        const auto& table_constraint = table_constraints[position];
        group_by_list_changed |= reduce_group_by_columns_for_constraint(table_constraint, group_by_columns,
                                                                        stored_table_node, aggregate_node);
        if (group_by_list_changed) break;
      }
    }

    // In case the initial query plan root returned the same columns in the same column order and was not a projection,
    // it is likely that the result of the current aggregate was either the root itself or only operators followed that
    // do not modify the column order (e.g., sort or limit). In this case, we need to restore the initial column order
    // by adding a projection with the initial column_references since we changed the column order by moving columns
    // from the group-by list to the aggregations.
    if (group_by_list_changed && initial_aggregate_column_expressions == root_column_expressions &&
        lqp->type != LQPNodeType::Projection) {
      const auto projection_node = std::make_shared<ProjectionNode>(root_column_expressions);
      lqp_insert_node(lqp, LQPInputSide::Left, projection_node);
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
