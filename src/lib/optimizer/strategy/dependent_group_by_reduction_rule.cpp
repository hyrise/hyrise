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

// void DependentGroupByReductionRule::wrap_node_in_any(std::vector<std::shared_ptr<AbstractExpression>>& expressions, const std::shared_ptr<LQPColumnExpression>& node_to_wrap) const {
//   for (auto& expression : expressions) {
//     visit_expression(expression, [&](auto& sub_expression) {
//       // Do not traverse into ANY()
//       if (sub_expression->type == ExpressionType::Aggregate) {
//         const auto aggregate_expression = std::dynamic_pointer_cast<AggregateExpression>(sub_expression);
//         if (aggregate_expression->aggregate_function == AggregateFunction::Any) {
//           return ExpressionVisitation::DoNotVisitArguments;
//         }
//       }

//       // Replace node_to_wrap with ANY(node_to_wrap).
//       if (*sub_expression == *node_to_wrap) {
//         sub_expression = any_(node_to_wrap);
//         return ExpressionVisitation::DoNotVisitArguments;
//       } else {
//         return ExpressionVisitation::VisitArguments;
//       }
//     });
//   }
// }

// void DependentGroupByReductionRule::restore_column_names(const std::shared_ptr<AbstractLQPNode>& node, const std::shared_ptr<AbstractLQPNode>& root_node) const {
//   bool found_succeeding_alias_node = false;

//   // visit_lqp_upwards(node, [&](const auto& upwards_node) {
//   //   if (aggregation_is_last_column_mention_operator && column_mentioning_node_types.contains(upwards_node->type) && *node != *upwards_node) {
//   //     // See if certain operators follow (hence, ignore starting agg node)
//   //     aggregation_is_last_column_mention_operator = false;
//   //   }
//   //   if (!alias_node_succeeding && upwards_node->type == LQPNodeType::Alias) {
//   //     alias_node_succeeding = true;
//   //   }
//   //   return LQPUpwardVisitation::VisitOutputs;
//   // });

//   // TODO: right now, we always add the alias ... because fuck you
//   std::vector<std::string> aliases;
//   column_names.reserve(root_node->column_expressions()->size());
//   if (!found_succeeding_alias_node) {
//     for (auto& column_expression : root_node->column_expressions()) {
//       auto column_name = column_expression->as_column_name();

//       // TODO(anyone): this should be better solved ... out of ideas right now
//       const auto any_position = column_name.find("ANY(");
//       if (any_position != std::string::npos) {
//         column_name = column_name.erase(any_position, 4);
//         column_name.pop_back();  // remove trailing ')'
//       }
//       aliases.emplace_back(column_name);
//     }
//   }

//   const auto alias_node = std::make_shared<AliasNode>(aggregate_node.column_expressions(), aliases);
//   lqp_insert_node(aggregate_node.outputs()[0], LQPInputSide::Left, alias_node);
// }

void DependentGroupByReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  const auto initial_column_expressions = lqp->column_expressions();

  visit_lqp(lqp, [&](const auto& node) {
    if (node->type != LQPNodeType::Aggregate) {
      return LQPVisitation::VisitInputs;
    }

    bool group_by_list_changed = false;
    // bool aggregation_is_last_column_mention_operator = true;
    // bool alias_node_succeeding = false;
    // I caannot find a good name. Problem is that we need to find nodes that do not explicitley list all outputted colums
    // (e.g., a sort thata does sort on another column). We would not see that column X remaains and will be outputted.
    // Hence, we remove it even though is should be pprat of sort. Same for limit.
    const std::unordered_set<LQPNodeType> column_mentioning_node_types = {LQPNodeType::Aggregate, LQPNodeType::Projection};

    // Check for the scenario in which a removed group-by column is later used. This can be in form of an
    // aggregate in the same aggregation node, in which case we do not need to do anything. Or it can be in form
    // of a later operator accessing it. For such cases, attributes are usually put into the grouping just to be
    // able to access them. In this case, we need to add them in form of an ANY() to the aggregation list.
    // TODO(anyone): explain aggregation_is_last_column_mention_operator
    // if (!node_is_later_referenced && !aggregation_is_last_column_mention_operator) {
    //   continue;
    // }

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

    auto no_table_has_primary_key = true;
    auto unique_columns_per_table = std::unordered_map<std::string, std::set<ColumnID>>();
    // Attempt early exit for aggregations where all tables lack primary keys. Collect unique keys to avoid retrieving
    // it again when early exit failed.
    for (const auto& [stored_table_node, group_by_columns] : group_by_columns_per_table) {
      const auto table_name = stored_table_node->table_name;
      const auto& table = Hyrise::get().storage_manager.get_table(table_name);
      const auto& soft_constraints = table->get_soft_unique_constraints();
      if (soft_constraints.empty()) {
        continue;
      }

      for (const auto& constraint : soft_constraints) {
        if (constraint.is_primary_key == IsPrimaryKey::Yes) {
          unique_columns_per_table[table_name].insert(constraint.columns.begin(), constraint.columns.end());
          break;
        }
      }

      no_table_has_primary_key = false;
    }

    if (no_table_has_primary_key) {
      return LQPVisitation::VisitInputs;
    }

    // Store copy of aggregate expression to enable restoring the original order of columns via a projection
    // auto projection_expressions(aggregate_node.column_expressions());

    const auto initial_aggregate_column_expressions = aggregate_node.column_expressions();

    // Store initial names for later restoring in case a column has been wrapped in an ANY()
    // std::vector<std::string> initial_column_names;
    // initial_column_names.reserve(projection_expressions.size());
    // for (const auto& original_aggregate_expression : projection_expressions) {
    //   initial_column_names.emplace_back(original_aggregate_expression->as_column_name());
    // }

    // Main loop. Iterate over the tables and its group-by columns, gather primary keys and see if we can reduce.
    for (const auto& [stored_table_node, group_by_columns] : group_by_columns_per_table) {
      // Obtain column IDs of the primary key
      // auto unique_columns = std::set<ColumnID>();
      auto unique_columns = unique_columns_per_table[stored_table_node->table_name];

      // const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      // if (table->get_soft_unique_constraints().empty()) {
      //   // early exit for current table if no constraints are set
      //   continue;
      // }

      // for (const auto& table_constraint : table->get_soft_unique_constraints()) {
      //   if (table_constraint.is_primary_key == IsPrimaryKey::Yes) {
      //     unique_columns.insert(table_constraint.columns.begin(), table_constraint.columns.end());
      //     break;
      //   }
      // }

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
                                 group_by_list_changed = true;
                                 return true;
                               }
                               return false;
                             }),
              aggregate_node.node_expressions.end());

          const auto node_to_wrap = lqp_column_({stored_table_node, group_by_column});

          // Before adapting upwards node, add the ANY() aggregate to the list of aggregates.
          const auto aggregate_any_expression = any_(node_to_wrap);
          aggregate_node.node_expressions.emplace_back(aggregate_any_expression);

          // Adapt upwards to no longer reference the original column, but the column wrapped in ANY().
          // Not using `expression_deep_replace` here, to avoid wrapping multiple ANY()s within each other.
          // visit_lqp_upwards(node, [&](const auto& upwards_node) {
          //   if (aggregation_is_last_column_mention_operator && column_mentioning_node_types.contains(upwards_node->type) && *node != *upwards_node) {
          //     // See if certain operators follow (hence, ignore starting agg node)
          //     aggregation_is_last_column_mention_operator = false;
          //   }

          //   if (!alias_node_succeeding && upwards_node->type == LQPNodeType::Alias) {
          //     alias_node_succeeding = true;
          //   }

          //   wrap_node_in_any(upwards_node->node_expressions, node_to_wrap);

          //   return LQPUpwardVisitation::VisitOutputs;
          // });

          // The projection list (which restores the original column order) needs to be adapted, too.
          // wrap_node_in_any(projection_expressions, node_to_wrap);
        }
      }
    }

    // TODO: wrong place here (should be done once?!?!?)
    if (group_by_list_changed && initial_aggregate_column_expressions == initial_column_expressions) {
      const auto projection_node = std::make_shared<ProjectionNode>(initial_column_expressions);
      lqp_insert_node(lqp, LQPInputSide::Left, projection_node);
    }

    // restore_column_names(node, lqp);

    // // If an alias succeeds, we are fine already, because the ANY() wrapping does not touch the aliases.
    // if (group_by_list_changed && !alias_node_succeeding) {
    //   std::vector<std::string> aliases;
    //   aliases.reserve(aggregate_node.column_expressions().size());
    //   for (const auto& column_expression : aggregate_node.column_expressions()) {
    //     auto column_alias = column_expression->as_column_name();

    //     // TODO(anyone): this should be better solved ... out of ideas right now
    //     const auto any_position = column_alias.find("ANY(");
    //     if (any_position != std::string::npos) {
    //       column_alias = column_alias.erase(any_position, 4);
    //       column_alias.pop_back();  // remove trailing ')'
    //     }
    //     aliases.emplace_back(column_alias);
    //   }
    //   const auto alias_node = std::make_shared<AliasNode>(aggregate_node.column_expressions(), aliases);
    //   lqp_insert_node(aggregate_node.outputs()[0], LQPInputSide::Left, alias_node);

    //   if (aggregation_is_last_column_mention_operator) {
    //   // If a column has been moved out of the group by set, it's position in the resulting output table potentially changes.
    //   // In case the aggregation is the last operator in the plan (or operators follow that do not modifiy the order such as sort or limit), the query output stores columns in a wrong order.
    //   // In this case, a projection is added which reproduces the order of the initial aggregation.
    //   // In case other operators such 
    //     const auto projection_node = std::make_shared<ProjectionNode>(projection_expressions);
    //     lqp_insert_node(alias_node->outputs()[0], LQPInputSide::Left, projection_node);
    //   }
    // }

    return LQPVisitation::VisitInputs;
  });

  // std::cout << "final plan:" << *lqp << std::endl;
}

}  // namespace opossum
