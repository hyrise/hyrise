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

    bool group_by_list_changed = false;

    // Main loop. Iterate over the tables and its group-by columns, gather primary keys/unique columns and check if we can reduce.
    for (const auto& [stored_table_node, group_by_columns] : group_by_columns_per_table) {
      auto unique_columns = std::set<ColumnID>();

      const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      if (table->get_soft_unique_constraints().empty()) {
        // early exit for current table if no constraints are set
        continue;
      }

      for (const auto& table_constraint : table->get_soft_unique_constraints()) {
        // Check that non of the unique/primary key columns is nullable. Unique columns can generally store NULLs while
        // previous operators (e.g., outer joins) might have added NULLs to a primary key column.
        auto columns_not_nullable = std::none_of(table_constraint.columns.begin(), table_constraint.columns.end(),
                                                 [&, stored_table_node = stored_table_node](const auto& column_id) {
                                                   const auto column_reference = std::make_shared<LQPColumnExpression>(
                                                       LQPColumnReference{stored_table_node, column_id});
                                                   return column_reference->is_nullable_on_lqp(aggregate_node);
                                                 });

        if (columns_not_nullable) {
          unique_columns.insert(table_constraint.columns.begin(), table_constraint.columns.end());
          break;
        }
      }

      // Intersect primary key/unique columns and group-by columns. In case a primary key/unique constraint covers
      // multiple columns, we need to check that all columns are present in order to later remove dependent columns.
      std::vector<ColumnID> intersection;
      std::set_intersection(unique_columns.begin(), unique_columns.end(), group_by_columns.begin(),
                            group_by_columns.end(), std::back_inserter(intersection));

      // Skip the current table as the primary key/unique constraint is not completely present.
      if (intersection.size() != unique_columns.size()) {
        continue;
      }

      for (const auto& group_by_column : group_by_columns) {
        // Every column that is not part of the primary key/unique constraint is going to be removed.s
        if (unique_columns.find(group_by_column) != unique_columns.end()) {
          continue;
        }

        // Remove node if it is a column reference and references the correct stored table node. Further, decrement
        // the aggregate's index which denotes the end of group-by expressions.
        aggregate_node.node_expressions.erase(
            std::remove_if(aggregate_node.node_expressions.begin(), aggregate_node.node_expressions.end(),
                           [&, stored_table_node = stored_table_node](const auto expression) {
                             const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
                             if (!column_expression) return false;

                             const auto& expression_stored_table_node =
                                 std::dynamic_pointer_cast<const StoredTableNode>(
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
    }

    // In case the initial query plan root returned the same columns in the same column order and was not a projection,
    // it is likely that the result of the current aggregate was either the root itself or only operators followed that
    // do not modify the column order (e.g., sort or limit). In this case, we need to restore the initial column order
    // by adding a projection with the initial column_references since we changed the column order by moving columns
    // from the group-by list to the aggregations.
    if (group_by_list_changed && aggregate_node.column_expressions() == root_column_expressions &&
        lqp->type != LQPNodeType::Projection) {
      const auto projection_node = std::make_shared<ProjectionNode>(root_column_expressions);
      lqp_insert_node(lqp, LQPInputSide::Left, projection_node);
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
