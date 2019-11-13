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
	// std::cout << "Before _apply: " << *lqp << std::endl;	
  visit_lqp(lqp, [&](const auto& node) {
    if (node->type != LQPNodeType::Aggregate) {
      return LQPVisitation::VisitInputs;
    }

    auto& aggregate_node = static_cast<AggregateNode&>(*node);

    std::vector<ColumnID> group_by_column_ids;
	  group_by_column_ids.reserve(aggregate_node.node_expressions.size() -
	                              aggregate_node.aggregate_expressions_begin_idx);

	  std::unordered_map<std::shared_ptr<const StoredTableNode>, std::set<ColumnID>> group_by_columns_per_table;

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

    std::set<ColumnID> removable_columns;
    for (const auto& [stored_table_node, group_by_columns] : group_by_columns_per_table) {
      // Obtain column IDs of the primary key
      auto unique_columns = std::set<ColumnID>();

      const auto& table = Hyrise::get().storage_manager.get_table(stored_table_node->table_name);
      if (table->get_soft_unique_constraints().empty()){
      	// early exit for current table if no constraints are set
      	continue;
      }

      for (const auto& table_constraint : table->get_soft_unique_constraints()) {
      	if (table_constraint.is_primary_key) {
      		unique_columns.insert(table_constraint.columns.begin(), table_constraint.columns.end());
      		break;
      	}
      }

      // Intersect primary key and group-by columns. Only if full primary key is part of the group-by columns, the
      // remaining columns can be removed.
      std::vector<ColumnID> intersection;
	    std::set_intersection(unique_columns.begin(), unique_columns.end(),
	                          group_by_columns.begin(), group_by_columns.end(),
	                          std::back_inserter(intersection));

	    // Skip the current table as not the full primary key is present.
	    if (intersection.size() != unique_columns.size()) {
	    	continue;
	    }

			// std::vector<ColumnID> difference;
	  //   std::set_intersection(unique_columns.begin(), unique_columns.end(),
	  //                         group_by_columns.begin(), group_by_columns.end(),
	  //                         std::back_inserter(difference));

	  //   for (const auto& el : difference) {
	  //   	std::cout << "% " << el << std::endl;
	  //   }

	    // std::cout << "Before: " << aggregate_node.node_expressions.size() << " and first non-groupby-index is " << aggregate_node.aggregate_expressions_begin_idx << "." << std::endl;

      for (const auto& group_by_column : group_by_columns) {
      	// Every column that is not part of the primary key is going to be removed.
      	// Not taking the pair<ColumnID, PositionInAgg> approach since it destroys some set-usage nicecities
      	if (unique_columns.find(group_by_column) == unique_columns.end()) {
      		// std::cout << "ColumnID " << group_by_column << " of table " << stored_table_node->table_name << " is dependent." << std::endl;

    			// const auto expression_to_delete = std::make_shared<LQPColumnExpression>(LQPColumnReference{std::make_shared<StoredTableNode>(table_name), group_by_column});
    			// aggregate_node.node_expressions.erase(std::remove(aggregate_node.node_expressions.begin(), aggregate_node.node_expressions.end(), expression_to_delete), aggregate_node.node_expressions.end());

    			aggregate_node.node_expressions.erase(std::remove_if(aggregate_node.node_expressions.begin(), aggregate_node.node_expressions.end(), [&, stored_table_node = stored_table_node](const auto expression){
						const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
						if (!column_expression) return false;

				  	const auto& expression_stored_table_node =
			          std::dynamic_pointer_cast<const StoredTableNode>(column_expression->column_reference.original_node());
			      if (!expression_stored_table_node) return false;

			      const auto column_id = column_expression->column_reference.original_column_id();
			      if (stored_table_node == expression_stored_table_node && group_by_column == column_id) {
    					return true;
			      }
    				return false;
    			}),aggregate_node.node_expressions.end());

					const auto node_to_replace = lqp_column_({stored_table_node, group_by_column});

    			const auto aggregate_any_expression = any_(node_to_replace);
    			bool node_is_later_referenced = false;

    			visit_lqp_upwards(node, [&, stored_table_node = stored_table_node](const auto& upwards_node) {
			      for (auto& expression : upwards_node->node_expressions) {
			      	visit_expression(expression, [&](auto& sub_expression) {
			      		if (sub_expression->type == ExpressionType::LQPColumn && node_to_replace == sub_expression) {
			      			const auto aggregate_expression = std::dynamic_pointer_cast<AggregateExpression>(sub_expression);
			      			node_is_later_referenced = true;
			      			return ExpressionVisitation::DoNotVisitArguments;
			      		}
						    return ExpressionVisitation::VisitArguments;
						  });
						  if (node_is_later_referenced) {
						  	return LQPUpwardVisitation::DoNotVisitOutputs;
						  }
			      }
			      return LQPUpwardVisitation::VisitOutputs;
			    });

    			// TODO: emplace only if expression is aactually required
    			if (node_is_later_referenced) {
    				aggregate_node.node_expressions.emplace_back(aggregate_any_expression);
    			}

    			// modified_aggregates.insert(std::dynamic_pointer_cast<AggregateNode>(node));

    			// not using expression_deep_replace here, since we do not want to wrap ANYs inside of ANYs    			
    			visit_lqp_upwards(node, [&, stored_table_node = stored_table_node](const auto& upwards_node) {
    				// std::cout << "upward search reaaching " << *upwards_node << std::endl;
			      for (auto& expression : upwards_node->node_expressions) {
			      	visit_expression(expression, [&](auto& sub_expression) {
			      		// Do not get into ANY()
			      		if (sub_expression->type == ExpressionType::Aggregate) {
			      			const auto aggregate_expression = std::dynamic_pointer_cast<AggregateExpression>(sub_expression);
			      			if (aggregate_expression->aggregate_function == AggregateFunction::Any) {
			      				// std::cout << "skipped " << *aggregate_expression << std::endl;
			      				return ExpressionVisitation::DoNotVisitArguments;
			      			}
			      		}
			      		// std::cout << "continue with " << *sub_expression << std::endl;

						    if (*sub_expression == *node_to_replace) {
						      sub_expression = aggregate_any_expression;
						      // std::cout << "replaced " << *sub_expression << std::endl;
						      return ExpressionVisitation::DoNotVisitArguments;
						    } else {
						    	// std::cout << *sub_expression << " does not match " << *node_to_replace << std::endl;
						    	// std::cout << sub_expression->hash() << " does not match " << node_to_replace->hash() << std::endl;
						      return ExpressionVisitation::VisitArguments;
						    }
						  });
			      }
			      return LQPUpwardVisitation::VisitOutputs;
			    });

    			--aggregate_node.aggregate_expressions_begin_idx;
      	}
      }

      // std::cout << "After: " << aggregate_node.node_expressions.size() << " and first non-groupby-index is " << aggregate_node.aggregate_expressions_begin_idx << std::endl;
    }
    return LQPVisitation::VisitInputs;
  });
	// std::cout << "After _apply: " << *lqp << std::endl;	
}

}  // namespace opossum
