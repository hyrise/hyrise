#include "dependent_group_by_reduction_rule.hpp"

#include <unordered_map>

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/dummy_table_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/update_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

void DependentGroupByReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  visit_lqp(lqp, [&](const auto& node) {
    if (node->type != LQPNodeType::Aggregate) {
      return LQPVisitation::VisitInputs;
    }

    auto& aggregate_node = static_cast<AggregateNode&>(*node);

    std::vector<ColumnID> group_by_column_ids;
    group_by_column_ids.reserve(aggregate_node.node_expressions.size() -
                                aggregate_node.aggregate_expressions_begin_idx);

    std::unordered_map<std::shared_ptr<const StoredTableNode>, std::set<ColumnID>> group_by_columns_per_table;

    // Collect the group-by columns for each table in the aggregate node
    for (auto expression_idx = size_t{0}; expression_idx < aggregate_node.aggregate_expressions_begin_idx;
         ++expression_idx) {
      const auto& expression = aggregate_node.node_expressions[expression_idx];
      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      if (!column_expression) continue;

      const auto& stored_table_node =
          std::dynamic_pointer_cast<const StoredTableNode>(column_expression->column_reference.original_node());
      // If column is not a physical column skip
      if (!stored_table_node) continue;

      const auto column_id = column_expression->column_reference.original_column_id();

      auto inserted = group_by_columns_per_table.try_emplace(stored_table_node, std::set<ColumnID>{column_id});
      if (!inserted.second) {
        group_by_columns_per_table[stored_table_node].insert(column_id);
      }
    }

    // Main loop. Iterate over the tables and its group-by columns, gather primary keys and see if we can reduce.
    for (const auto& [stored_table_node, group_by_columns] : group_by_columns_per_table) {
      // Obtain column IDs of the primary key
      auto unique_columns = std::set<ColumnID>();

      const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      if (table->get_soft_unique_constraints().empty()) {
        // early exit for current table if no constraints are set
        continue;
      }

      for (const auto& table_constraint : table->get_soft_unique_constraints()) {
        if (table_constraint.is_primary_key == IsPrimaryKey::Yes) {
          unique_columns.insert(table_constraint.columns.begin(), table_constraint.columns.end());
          break;
        }
      }

      // Intersect primary key and group-by columns. Only if full primary key is part of the group-by columns, the
      // remaining columns can be removed.
      std::vector<ColumnID> intersection;
      std::set_intersection(unique_columns.begin(), unique_columns.end(), group_by_columns.begin(),
                            group_by_columns.end(), std::back_inserter(intersection));

      // Skip the current table as the primary key is not completely present.
      if (intersection.size() != unique_columns.size()) {
        continue;
      }

      for (const auto& group_by_column : group_by_columns) {
        // Every column that is not part of the primary key is going to be removed.
        if (unique_columns.find(group_by_column) == unique_columns.end()) {
          // Remove nodes if they are column references and reference the correct stored table node. Further, decrement
          // the aggregate's index signaling the end of group-by expressions.
          aggregate_node.node_expressions.erase(
              std::remove_if(aggregate_node.node_expressions.begin(), aggregate_node.node_expressions.end(),
                             [&, stored_table_node = stored_table_node](const auto expression) {
                               const auto& column_expression =
                                   std::dynamic_pointer_cast<LQPColumnExpression>(expression);
                               if (!column_expression) return false;

                               const auto& expression_stored_table_node =
                                   std::dynamic_pointer_cast<const StoredTableNode>(
                                       column_expression->column_reference.original_node());
                               if (!expression_stored_table_node) return false;

                               const auto column_id = column_expression->column_reference.original_column_id();
                               if (stored_table_node == expression_stored_table_node && group_by_column == column_id) {
                                 // Adjust the number of group by expressions.
                                 --aggregate_node.aggregate_expressions_begin_idx;
                                 return true;
                               }
                               return false;
                             }),
              aggregate_node.node_expressions.end());

          const auto node_to_replace = lqp_column_({stored_table_node, group_by_column});
          bool node_is_later_referenced = false;

          // Check if the removed column is later referenced somewhere upwards in the plan. If so, we need to add it
          // back via the ANY() aggregate function.
          visit_lqp_upwards(node, [&](const auto& upwards_node) {
            if (*node == *upwards_node) {
              // We are only interested in nodes upwards from node, not node itself.
              return LQPUpwardVisitation::VisitOutputs;
            }

            for (auto& expression : upwards_node->node_expressions) {
              visit_expression(expression, [&](auto& sub_expression) {
                if (sub_expression->type == ExpressionType::LQPColumn && *node_to_replace == *sub_expression) {
                  const auto aggregate_expression = std::dynamic_pointer_cast<AggregateExpression>(sub_expression);
                  node_is_later_referenced = true;
                  return ExpressionVisitation::DoNotVisitArguments;
                } else {
                }
                return ExpressionVisitation::VisitArguments;
              });

              if (node_is_later_referenced) {
                return LQPUpwardVisitation::DoNotVisitOutputs;
              }
            }
            return LQPUpwardVisitation::VisitOutputs;
          });

          // Check for the scenario in which a removed group-by column is later used. This can be in form of an
          // aggregate in the same aggregation node, in which case we do not need to do anything. Or it can be in form
          // of a later operator accessing it. For such cases, attributes are usually put into the grouping just to be
          // able to access them. In this case, we need to add them in form of an ANY() to the aggregation list.
          if (!node_is_later_referenced) {
            continue;
          }

          // Before adapting upwards node, add the ANY() aggregate to the list of aggregates.
          const auto aggregate_any_expression = any_(node_to_replace);
          aggregate_node.node_expressions.emplace_back(aggregate_any_expression);

          // Adapt upwards to no longer reference the original column, but the column wrapped in ANY().
          // Not using `expression_deep_replace` here, to avoid wrapping multiple ANY()s within each other.
          visit_lqp_upwards(node, [&, stored_table_node = stored_table_node](const auto& upwards_node) {
            for (auto& expression : upwards_node->node_expressions) {
              visit_expression(expression, [&](auto& sub_expression) {
                // Do not get into ANY()
                if (sub_expression->type == ExpressionType::Aggregate) {
                  const auto aggregate_expression = std::dynamic_pointer_cast<AggregateExpression>(sub_expression);
                  if (aggregate_expression->aggregate_function == AggregateFunction::Any) {
                    return ExpressionVisitation::DoNotVisitArguments;
                  }
                }
                // Replace with ANY() wrap.
                if (*sub_expression == *node_to_replace) {
                  sub_expression = aggregate_any_expression;
                  return ExpressionVisitation::DoNotVisitArguments;
                } else {
                  return ExpressionVisitation::VisitArguments;
                }
              });
            }
            return LQPUpwardVisitation::VisitOutputs;
          });
        }
      }
    }
    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
