#include "pqp_analyzer.hpp"

#include "expression/lqp_column_expression.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "operators/pqp_utils.hpp"

namespace opossum {

void PQPAnalyzer::set_queue(const DependencyCandidateQueue& queue) { _queue = queue; };

void PQPAnalyzer::run() {
  const auto& pqp_cache = Hyrise::get().default_pqp_cache;
  if (!pqp_cache) {
    std::cout << "NO PQPCache. Stopping" << std::endl;
    return;
  }
  const auto cache_snapshot = pqp_cache->snapshot();

  if (cache_snapshot.empty()) {
    std::cout << "PQPCache empty. Stopping" << std::endl;
    return;
  }

  for (const auto& [_, entry] : cache_snapshot) {
    const auto pqp_root = entry.value;
    visit_pqp(pqp_root, [&](const auto& op) {
      const auto lqp_node = op->lqp_node;
      if (!lqp_node) {
        return PQPVisitation::VisitInputs;
      }

      switch (op->type()) {
        case OperatorType::JoinHash:
        case OperatorType::JoinNestedLoop:
        case OperatorType::JoinSortMerge: {
          // To DO: Join to SemiJoin rewrite; Join to Scan OD
        } break;
        case OperatorType::Aggregate: {
          const auto aggregate_node = static_pointer_cast<const AggregateNode>(lqp_node);
          const auto num_group_by_columns = aggregate_node->aggregate_expressions_begin_idx;
          if (num_group_by_columns < 2) {
            return PQPVisitation::VisitInputs;
          }
          const auto& node_expressions = aggregate_node->node_expressions;
          std::vector<TableColumnID> table_column_ids;
          for (auto expression_idx = size_t{0}; expression_idx < num_group_by_columns; ++expression_idx) {
            const auto lqp_column_expression =
                dynamic_pointer_cast<LQPColumnExpression>(node_expressions[expression_idx]);
            if (!lqp_column_expression) {
              continue;
            }
            const auto orig_node = lqp_column_expression->original_node.lock();
            if (orig_node->type != LQPNodeType::StoredTable) {
              continue;
            }
            const auto original_column_id = lqp_column_expression->original_column_id;
            if (original_column_id == INVALID_COLUMN_ID) {
              continue;
            }
            const auto stored_table_node = static_pointer_cast<const StoredTableNode>(orig_node);
            const auto table_name = stored_table_node->table_name;
            table_column_ids.emplace_back(table_name, original_column_id);
          }
          if (table_column_ids.empty()) {
            return PQPVisitation::VisitInputs;
          }
          std::cout << "Dependent Group-by Reduction candidates" << std::endl;
          for (const auto& [table_name, column_id] : table_column_ids) {
            const auto orig_table = Hyrise::get().storage_manager.get_table(table_name);
            const auto column_name = orig_table->column_name(column_id);
            std::cout << "\t" << table_name << "." << column_name << std::endl;
          }

        } break;
        default:
          break;
      }

      return PQPVisitation::VisitInputs;
    });
  }
  if (_queue) {
    _queue->emplace(std::vector<TableColumnID>{}, std::vector<TableColumnID>{}, 1);
    _queue->emplace(std::vector<TableColumnID>{}, std::vector<TableColumnID>{}, 2);
  }
}

}  // namespace opossum
